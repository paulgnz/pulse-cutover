// node --test control/test/*.test.mjs
// rc.32: F2 the abort veto stays refused (409) for an event the relay once saw go forward, after the beacons go stale;
// F3 the QUORUM UNREACHABLE alarm is an alarm, not raised when the fleet already aborted, resolved by a signed abort.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';

const ADMIN_TOKEN = 'test-admin-token-0123456789';
const HERE = dirname(fileURLToPath(import.meta.url));
const CHAIN = '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd';
const sha = (s) => createHash('sha256').update(s).digest('hex');
const BPS = ['bpa', 'bpb', 'bpc', 'bpd', 'bpe'];
const TOK = Object.fromEntries(BPS.map((p, i) => [p, String(i + 1).repeat(64)]));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const PUBHEX = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');
const signed = (payload) => { const p = JSON.stringify(payload); return { payload: p, key: PUBHEX, sig: sign(null, Buffer.from(p), privateKey).toString('hex') }; };
const procs = [];
after(() => procs.forEach((p) => p.kill('SIGKILL')));
const CUT = 408462434;

function setup() {
  const dir = mkdtempSync(join(tmpdir(), 'mc-rc32-'));
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN,
    rpc: [], coordinators: [PUBHEX], static_producers: true }] }));
  writeFileSync(join(dir, 'tokens.json'), JSON.stringify(Object.fromEntries(BPS.map((p) => [sha(TOK[p]), { network: 'testnet', producer: p }]))));
  mkdirSync(join(dir, 'state'));
  writeFileSync(join(dir, 'state', 'admin.token'), ADMIN_TOKEN);
  return dir;
}
function start(dir) {
  const proc = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, ADMIN_TOKEN_FILE: join(dir, 'state', 'admin.token'), PORT: '0', MC_OFFLINE: '1',
    NETWORKS: join(dir, 'networks.json'), TOKENS: join(dir, 'tokens.json'), COORD_FILE: join(dir, 'state', 'coord.json') } });
  procs.push(proc);
  return new Promise((ok, ko) => {
    let out = '';
    proc.stdout.on('data', (d) => { out += d; const m = out.match(/on 127\.0\.0\.1:(\d+)/); if (m) ok({ base: `http://127.0.0.1:${m[1]}`, proc }); });
    proc.stderr.on('data', (d) => { out += d; });
    proc.on('exit', (c) => ko(new Error(`server exited ${c}: ${out}`)));
    setTimeout(() => ko(new Error('server did not start: ' + out)), 8000);
  });
}
const kill = (proc) => new Promise((ok) => { proc.once('exit', ok); proc.kill('SIGKILL'); });
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
const net = async (base) => (await (await fetch(`${base}/api/status`)).json()).networks.find((n) => n.id === 'testnet');
const post = (base, payload) => fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ network: 'testnet', ...payload })) });
const evHash = async (base) => sha((await (await fetch(`${base}/api/coord/testnet`)).json()).event.payload);
function reporter(base, startTs = Date.now() - 25e3) {
  let t = startTs;
  return async (producer, ceremony) => {
    for (;;) {
      const r = await fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[producer]}` },
        body: JSON.stringify({ ...fixture(), producer, network: 'testnet', role: 'producer', instance_id: 'a'.repeat(32), ts: new Date(t += 3100).toISOString(),
          source: { head: CUT, lib: CUT, chain_id: CHAIN }, coord: { event_id: 'ev-1', h: CUT, accepted: true, armed: true },
          ceremony: { ...fixture().ceremony, evidence: { h: CUT }, ...ceremony } }) });
      if (r.status !== 429) return r;
      await new Promise((ok) => setTimeout(ok, 3100));
    }
  };
}
async function armedEvent(base, extra = {}) {
  assert.equal((await post(base, { type: 'event', event_id: 'ev-1', h: CUT, chain_id: CHAIN, roster: BPS.map((producer) => ({ producer })), quorum: 5, ...extra })).status, 200);
  assert.equal((await post(base, { type: 'arm', event_id: 'ev-1', event_hash: await evHash(base), issued_at_ms: Date.now() })).status, 200);
}

const exitRep = (state = 'ABORTED', extra = {}) => ({ state, evidence: { h: CUT }, transitions: [{ state: 'ARMED', ts: '2026-10-16T00:00:00Z' }, { state, ts: '2026-10-16T00:01:00Z' }], ...extra });
const sleep = (ms) => new Promise((ok) => setTimeout(ok, ms));
const TARGET = { blockchain_id: 'TargetChainAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', head: CUT + 50, head_id: null, after_cut_id: 'a1'.repeat(32) };
// Reports that are fresh for only ~4 s (the silence window is 45 s for the fixture's interval): the s1c "beacons stopped" case.
// Each producer reports once, 41 s in the past, 50 ms apart.
function shortLived(base) {
  let t = Date.now() - 41e3;
  return (producer, ceremony) => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[producer]}` },
    body: JSON.stringify({ ...fixture(), producer, network: 'testnet', role: 'producer', instance_id: 'a'.repeat(32), ts: new Date(t += 50).toISOString(),
      source: { head: CUT, lib: CUT, chain_id: CHAIN }, coord: { event_id: 'ev-1', h: CUT, accepted: true, armed: true },
      ceremony: { ...fixture().ceremony, evidence: { h: CUT }, ...ceremony } }) });
}

test('rc.32 F2: a LIVE verdict is recorded durably at report time; after the beacons go stale and a restart, an abort is still refused 409', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  await armedEvent(base);
  const send = shortLived(base);
  for (const p of BPS) assert.equal((await send(p, { state: 'LIVE', ignition_started: true, create_started: true, target: TARGET })).status, 200);
  // No status read in between: the latch must come from the reports themselves.
  const c0 = (await net(base)).coordination;
  assert.equal(c0.forward?.event_id, 'ev-1', JSON.stringify(c0));
  assert.ok(c0.history.some((h) => h.type === 'forward_latched'));
  await sleep(6000); // every report is now silent
  await kill(proc);
  ({ base, proc } = await start(dir));
  const n = await net(base);
  assert.notEqual(n.fleet.verdict, 'LIVE', 'the verdict decayed: stale reports');
  assert.ok(n.fleet.forward_latched, JSON.stringify(n.fleet));
  const r = await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) });
  assert.equal(r.status, 409);
  const body = await r.json();
  assert.match(body.error, /NOT relayed/);
  assert.match(body.error, /recorded forward at/);
  assert.ok(body.forward_latched);
  assert.equal((await (await fetch(`${base}/api/coord/testnet`)).json()).abort, null, 'nothing relayed');
  const n2 = await net(base);
  assert.ok(n2.coordination.history.some((h) => h.type === 'abort_refused' && h.forward_latched_at), JSON.stringify(n2.coordination.history));
  // The acknowledgement still relays it (rc.31 behaviour), with the ALARM.
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base), acknowledge_past_creation: true })).status, 200);
  assert.ok((await net(base)).events.some((e) => /acknowledged by the signer/.test(e.text)));
  await kill(proc);
});

test('rc.32 F2: a quorum of fresh past-creation reports is latched too; fewer is not (the veto stays relayable)', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  await armedEvent(base);
  const send = shortLived(base);
  assert.equal((await send('bpa', { state: 'VERIFIED', create_started: true })).status, 200);
  assert.equal((await net(base)).coordination.forward, null, 'one member past creation is not forward');
  for (const p of BPS.slice(1)) assert.equal((await send(p, { state: 'VERIFIED', create_started: true })).status, 200);
  assert.equal((await net(base)).coordination.forward?.event_id, 'ev-1');
  await sleep(6000);
  await kill(proc);
  ({ base, proc } = await start(dir));
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) })).status, 409);
  await kill(proc);
});

test('rc.32 F2: before the fleet goes forward nothing is latched and an abort is relayed as before; the latch is per event', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base);
  const send = reporter(base);
  for (const p of BPS) assert.equal((await send(p, { state: 'FROZEN' })).status, 200);
  assert.equal((await net(base)).coordination.forward, null);
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) })).status, 200);
  await kill(proc);
});

test('rc.32 F3: QUORUM UNREACHABLE is an alarm (not a warning, not a SPLIT), cleared by a signed abort', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  await armedEvent(base); // quorum 5 of 5
  const send = reporter(base);
  for (const p of BPS.slice(0, 4)) assert.equal((await send(p, { state: 'FROZEN' })).status, 200);
  assert.equal((await send('bpe', exitRep('ABORTED'))).status, 200);
  let n = await net(base);
  assert.ok(n.fleet.alarms.some((a) => a.startsWith('ALARM QUORUM UNREACHABLE')), JSON.stringify(n.fleet));
  assert.ok(!n.fleet.warnings.some((w) => w.startsWith('ALARM QUORUM UNREACHABLE')));
  assert.notEqual(n.fleet.verdict, 'SPLIT');
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) })).status, 200);
  n = await net(base);
  assert.equal(n.fleet.quorum_unreachable, null);
  assert.ok(!n.fleet.alarms.some((a) => a.includes('QUORUM UNREACHABLE')), JSON.stringify(n.fleet.alarms));
  // durable: still resolved after a restart
  await kill(proc);
  ({ base, proc } = await start(dir));
  assert.ok(!(await net(base)).fleet.alarms.some((a) => a.includes('QUORUM UNREACHABLE')));
  await kill(proc);
});

test('rc.32 F3: QUORUM UNREACHABLE is not raised when every member already aborted (the s1 board)', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base);
  const send = reporter(base);
  for (const p of BPS) assert.equal((await send(p, exitRep('ABORTED'))).status, 200);
  const n = await net(base);
  assert.equal(n.fleet.verdict, 'ABORTED', JSON.stringify(n.fleet));
  assert.equal(n.fleet.quorum_unreachable, null);
  assert.ok(![...n.fleet.alarms, ...n.fleet.warnings].some((a) => a.includes('QUORUM UNREACHABLE')));
  await kill(proc);
});
