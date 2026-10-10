// node --test control/test/*.test.mjs
// rc.27 (2nd review): the old-chain SPLIT alarm without a guessed burn-off bound. Right after rc.23 mission control was deployed,
// a 5-BP rehearsal on rc.22 beacons (no head_at_pause) paused every source correctly at cut + 377 (DPoS finality
// lag + freeze lead + quiescence), past rc.23's fixed fallback of cut + 360: a false, latched SPLIT. The fallback is
// gone; the relay now watches the old chain's head MOVE after every member is paused and someone is past creation.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { fleetVerdict, movementArmed, nextEventMark, stateRank, PAUSED_RANK } from '../lib.mjs';

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
  const dir = mkdtempSync(join(tmpdir(), 'mc-rc24-'));
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN,
    rpc: [], coordinators: [PUBHEX], static_producers: true }] }));
  writeFileSync(join(dir, 'tokens.json'), JSON.stringify(Object.fromEntries(BPS.map((p) => [sha(TOK[p]), { network: 'testnet', producer: p }]))));
  mkdirSync(join(dir, 'state'));
  return dir;
}
function start(dir) {
  const proc = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', MC_OFFLINE: '1',
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


test('a past-creation report for another H is kept durably: an instance replacement does not erase it; only the operator clears it', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  assert.equal((await fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ type: 'event', network: 'testnet', chain_id: CHAIN,
    event_id: 'ev-1', h: CUT, roster: BPS.map((producer) => ({ producer })), quorum: 5 })) })).status, 200);
  const send = (producer, instance, ts, ceremony) => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[producer]}` },
    body: JSON.stringify({ ...fixture(), producer, network: 'testnet', role: 'producer', instance_id: instance.repeat(32), ts: new Date(ts).toISOString(),
      source: { head: CUT, lib: CUT, chain_id: CHAIN }, coord: { event_id: 'ev-1', h: CUT, accepted: true, armed: true },
      ceremony: { ...fixture().ceremony, ...ceremony } }) });
  // The old instance reports LIVE for ANOTHER H (3 min ago: silent now), then a new instance of the same token reports ABORTED.
  assert.equal((await send('bpa', 'a', Date.now() - 180e3, { state: 'LIVE', ignition_started: true, evidence: { h: CUT + 999 } })).status, 200);
  for (const p of BPS.slice(1)) assert.equal((await send(p, 'c', Date.now() - 1000, { state: 'ABORTED', evidence: { h: CUT } })).status, 200);
  assert.equal((await send('bpa', 'b', Date.now(), { state: 'ABORTED', evidence: { h: CUT } })).status, 200);
  let n = await net(base);
  const bpa = n.producers.find((p) => p.name === 'bpa');
  assert.equal(bpa.event_max['ev-1'].foreign_past[0].state, 'LIVE', 'durable after the replacement');
  assert.equal(bpa.event_max['ev-1'].foreign_past[0].h, CUT + 999);
  assert.notEqual(n.fleet.verdict, 'ABORTED');
  assert.ok(n.fleet.warnings.some((w) => w.startsWith('bpa reports a ceremony for ANOTHER H')), JSON.stringify(n.fleet));
  // Survives a restart.
  await kill(proc);
  ({ base, proc } = await start(dir));
  assert.equal((await net(base)).producers.find((p) => p.name === 'bpa').event_max['ev-1'].foreign_past[0].state, 'LIVE');
  // A forwarded (proxied) request may not clear it; the local operator can.
  assert.equal((await fetch(`${base}/api/admin/clear-foreign?net=testnet&producer=bpa&event=ev-1&h=${CUT + 999}`, { method: 'POST', headers: { 'x-real-ip': '1.2.3.4' } })).status, 403);
  assert.equal((await fetch(`${base}/api/admin/clear-foreign?net=testnet&producer=bpa&event=ev-1`, { method: 'POST' })).status, 400, 'the observation must be named');
  assert.equal((await fetch(`${base}/api/admin/clear-foreign?net=testnet&producer=bpa&event=ev-1&h=7`, { method: 'POST' })).status, 404);
  assert.equal((await fetch(`${base}/api/admin/clear-foreign?net=testnet&producer=bpa&event=ev-1&h=${CUT + 999}`, { method: 'POST' })).status, 200);
  n = await net(base);
  assert.equal(n.producers.find((p) => p.name === 'bpa').event_max['ev-1'].foreign_past, undefined);
  assert.equal(n.fleet.verdict, 'ABORTED');
  await kill(proc);
});

test('2nd review: unsigned event ids cannot evict a mark; every foreign observation is kept; a clear retires only the one named', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  assert.equal((await fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ type: 'event', network: 'testnet', chain_id: CHAIN,
    event_id: 'ev-1', h: CUT, roster: BPS.map((producer) => ({ producer })), quorum: 5 })) })).status, 200);
  let t = Date.now() - 200e3;
  const send = (event, ceremony, instance = 'a') => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK.bpa}` },
    body: JSON.stringify({ ...fixture(), producer: 'bpa', network: 'testnet', role: 'producer', instance_id: instance.repeat(32), ts: new Date(t += 3100).toISOString(),
      source: { head: CUT, lib: CUT, chain_id: CHAIN }, coord: { event_id: event, h: CUT, accepted: true, armed: true },
      ceremony: { ...fixture().ceremony, ...ceremony } }) });
  // Two foreign observations (H+999, then H+888), then an ABORTED for this H.
  assert.equal((await send('ev-1', { state: 'LIVE', ignition_started: true, evidence: { h: CUT + 999 } })).status, 200);
  assert.equal((await send('ev-1', { state: 'LIVE', ignition_started: true, evidence: { h: CUT + 888 } })).status, 200);
  assert.equal((await send('ev-1', { state: 'ABORTED', evidence: { h: CUT } })).status, 200);
  // 25 made-up event ids from the same token (rate limit: one report / 3 s per token; timestamps step 3.1 s).
  for (let i = 0; i < 25; i++) {
    const r = await send(`junk-${i}`, { state: 'ABORTED', evidence: { h: CUT } });
    if (r.status === 429) { await new Promise((ok) => setTimeout(ok, 3100)); i--; continue; }
    assert.equal(r.status, 200);
  }
  let m = (await net(base)).producers.find((p) => p.name === 'bpa').event_max;
  assert.deepEqual(m['ev-1'].foreign_past.map((o) => o.h), [CUT + 999, CUT + 888], 'both observations kept, the event mark not evicted');
  assert.equal(Object.keys(m).filter((k) => k.startsWith('junk-')).length, 0, 'unsigned event ids get no marks');
  // Restart, then retire one: the other still blocks.
  await kill(proc);
  ({ base, proc } = await start(dir));
  const clear = (h) => fetch(`${base}/api/admin/clear-foreign?net=testnet&producer=bpa&event=ev-1&h=${h}`, { method: 'POST' });
  const r1 = await (await clear(CUT + 999)).json();
  assert.deepEqual(r1.remaining.map((o) => o.h), [CUT + 888]);
  m = (await net(base)).producers.find((p) => p.name === 'bpa').event_max;
  assert.deepEqual(m['ev-1'].foreign_past.map((o) => o.h), [CUT + 888]);
  assert.equal((await clear(CUT + 888)).status, 200);
  assert.equal((await net(base)).producers.find((p) => p.name === 'bpa').event_max['ev-1'].foreign_past, undefined);
  await kill(proc);
});
