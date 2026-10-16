// node --test control/test/*.test.mjs
// rc.31 (fleet run h2): mission control accepted a signed abort for an event already 5/5 LIVE (then refused `complete`,
// the verdict stayed LIVE, no alarm). The relay now refuses an abort once any member is past chain creation, records
// the attempt and raises an alarm; a signer may acknowledge it explicitly (`acknowledge_past_creation`).
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
  const dir = mkdtempSync(join(tmpdir(), 'mc-rc31-'));
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

test('rc.31: an abort for an event whose members are LIVE is refused (a veto nobody received), recorded and alarmed; complete still closes it', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  await armedEvent(base);
  const send = reporter(base);
  const target = { blockchain_id: 'TargetChainAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', head: CUT + 50, head_id: null, after_cut_id: 'a1'.repeat(32) };
  for (const p of BPS) assert.equal((await send(p, { state: 'LIVE', ignition_started: true, create_started: true, target })).status, 200);
  const r = await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) });
  assert.equal(r.status, 409);
  const body = await r.json();
  assert.match(body.error, /NOT relayed/);
  assert.match(body.error, /veto no member has received/);
  assert.equal(body.past_create.length, 5);
  const c = await (await fetch(`${base}/api/coord/testnet`)).json();
  assert.equal(c.abort, null, 'nothing relayed: agents never see it');
  const n = await net(base);
  assert.notEqual(n.fleet.verdict, 'ABORTED');
  assert.ok(n.fleet.warnings.some((w) => w.includes('REFUSED') && w.includes('NOT relayed')), JSON.stringify(n.fleet.warnings));
  assert.ok(n.events.some((e) => /ALARM: signed abort for ev-1 REFUSED/.test(e.text)), JSON.stringify(n.events));
  // The refusal is durable (history persisted across a restart).
  await kill(proc);
  ({ base, proc } = await start(dir));
  assert.ok((await net(base)).fleet.warnings.some((w) => w.includes('REFUSED')));
  // `complete` still closes the event that ran.
  assert.equal((await post(base, { type: 'complete', event_id: 'ev-1', event_hash: await evHash(base) })).status, 200);
  await kill(proc);
});

test('rc.31 review M2: a quorum of fresh past-creation reports refuses; fewer (one buggy or spoofed create_started) is relayed with an ALARM', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base);
  const send = reporter(base);
  // One member claims creation: the abort is still the veto the other four need: relayed, alarmed.
  assert.equal((await send('bpa', { state: 'VERIFIED', create_started: true })).status, 200);
  for (const p of BPS.slice(1)) assert.equal((await send(p, { state: 'VERIFIED' })).status, 200);
  const r = await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) });
  assert.equal(r.status, 200);
  const c = await (await fetch(`${base}/api/coord/testnet`)).json();
  assert.ok(c.abort, 'relayed to the agents');
  const n = await net(base);
  assert.ok(n.fleet.warnings.some((w) => w.includes('relayed although members showed signs of chain creation')), JSON.stringify(n.fleet.warnings));
  assert.ok(n.events.some((e) => /ALARM: signed abort for ev-1 relayed although/.test(e.text)));
  await kill(proc);
});

test('rc.31 review M2: with quorum 3, three fresh past-creation reports refuse; an acknowledged abort is relayed', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  assert.equal((await post(base, { type: 'event', event_id: 'ev-1', h: CUT, chain_id: CHAIN, roster: BPS.map((producer) => ({ producer })), quorum: 3 })).status, 200);
  assert.equal((await post(base, { type: 'arm', event_id: 'ev-1', event_hash: await evHash(base), issued_at_ms: Date.now() })).status, 200);
  const send = reporter(base);
  for (const p of BPS.slice(0, 3)) assert.equal((await send(p, { state: 'VERIFIED', create_started: true })).status, 200);
  for (const p of BPS.slice(3)) assert.equal((await send(p, { state: 'VERIFIED' })).status, 200);
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) })).status, 409);
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base), acknowledge_past_creation: true })).status, 200);
  assert.ok((await net(base)).events.some((e) => /acknowledged by the signer/.test(e.text)));
  await kill(proc);
});

test('rc.31 review M2: creation evidence of an EARLIER aborted event at the same H never blocks this event\'s veto', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  assert.equal((await post(base, { type: 'event', event_id: 'ev-0', h: CUT, chain_id: CHAIN, roster: BPS.map((producer) => ({ producer })), quorum: 5 })).status, 200);
  let t = Date.now() - 200e3;
  const old = await fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK.bpa}` },
    body: JSON.stringify({ ...fixture(), producer: 'bpa', network: 'testnet', role: 'producer', instance_id: 'a'.repeat(32), ts: new Date(t += 3100).toISOString(),
      source: { head: CUT, lib: CUT, chain_id: CHAIN }, coord: { event_id: 'ev-0', h: CUT, accepted: true, armed: true },
      ceremony: { ...fixture().ceremony, state: 'LIVE', ignition_started: true, evidence: { h: CUT } } }) });
  assert.equal(old.status, 200);
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-0', acknowledge_past_creation: true })).status, 200);
  await armedEvent(base);
  const send = reporter(base);
  for (const p of BPS) assert.equal((await send(p, { state: 'FROZEN' })).status, 200);
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) })).status, 200);
  await kill(proc);
});

test('rc.31: before anyone is past creation an abort is accepted as before', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base);
  const send = reporter(base);
  for (const p of BPS) assert.equal((await send(p, { state: 'FROZEN' })).status, 200);
  assert.equal((await post(base, { type: 'abort', event_id: 'ev-1', event_hash: await evHash(base) })).status, 200);
  const n = await net(base);
  assert.ok(!n.fleet.warnings.some((w) => /abort was (REFUSED|relayed although)/.test(w)));
  await kill(proc);
});

// ---- review M1: the durable exit mark --------------------------------------------------------------------------
const exitRep = (state = 'ABORTED', extra = {}) => ({ state, evidence: { h: CUT }, transitions: [{ state: 'ARMED', ts: '2026-10-16T00:00:00Z' }, { state, ts: '2026-10-16T00:01:00Z' }], ...extra });
const markOf = async (base, p) => (await net(base)).producers.find((x) => x.name === p).event_max?.['ev-1'];

test('rc.31 review M1: an exit without verifying is a durable mark; a later report in another state (fresh journal) is a re-entry ALARM', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  await armedEvent(base, { early_abort: true });
  const send = reporter(base);
  assert.equal((await send('bpc', exitRep('ABORTED'))).status, 200);
  assert.equal((await markOf(base, 'bpc')).exited.state, 'ABORTED');
  // The operator fixes the config and re-runs bpc on a fresh journal under the same event: ARMED again.
  assert.equal((await send('bpc', { state: 'ARMED', evidence: { h: CUT }, transitions: [{ state: 'ARMED', ts: '2026-10-16T00:05:00Z' }] })).status, 200);
  let m = await markOf(base, 'bpc');
  assert.ok(m.exited, 'the mark never lowers');
  assert.equal(m.reentered, true);
  let n = await net(base);
  assert.ok(n.fleet.warnings.some((w) => w.startsWith('ALARM: bpc re-entered this event')), JSON.stringify(n.fleet.warnings));
  assert.ok(n.events.some((e) => /ALARM: re-entered event ev-1/.test(e.text)));
  assert.equal(n.fleet.members.find((x) => x.producer === 'bpc').exited, true);
  // Durable across a restart.
  await kill(proc);
  ({ base, proc } = await start(dir));
  m = await markOf(base, 'bpc');
  assert.ok(m.exited && m.reentered, JSON.stringify(m));
  await kill(proc);
});

test('rc.31 round 3 (HIGH-1): a member the relay saw verify can never get an exit mark (an honest crash + fresh journal, or a forged report)', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  await armedEvent(base, { early_abort: true });
  const send = reporter(base);
  assert.equal((await send('bpa', { state: 'VERIFIED', evidence: { h: CUT, snapshot_sha256: 'aa'.repeat(32) } })).status, 200);
  let m = await markOf(base, 'bpa');
  assert.equal(m.verified_seen, true);
  // A later never-verified exit (fresh journal) must not mark it, before or after a restart.
  assert.equal((await send('bpa', exitRep('ABORTED'))).status, 200);
  m = await markOf(base, 'bpa');
  assert.ok(!m.exited, JSON.stringify(m));
  await kill(proc);
  ({ base, proc } = await start(dir));
  assert.equal((await markOf(base, 'bpa')).verified_seen, true, 'persisted');
  const send2 = reporter(base, Date.now() + 5e3);
  assert.equal((await send2('bpa', exitRep('STRANDED'))).status, 200);
  assert.ok(!(await markOf(base, 'bpa')).exited);
  // A member that never verified gets an explicit verified_seen: false with its exit mark.
  assert.equal((await send2('bpb', exitRep('ABORTED'))).status, 200);
  m = await markOf(base, 'bpb');
  assert.equal(m.verified_seen, false);
  assert.ok(m.exited);
  await kill(proc);
});

test('rc.31 round 3 (M-c): only a producer-role report can set an exit mark', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base);
  const r = await fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK.bpc}` },
    body: JSON.stringify({ ...fixture(), producer: 'bpc', network: 'testnet', role: 'api', instance_id: 'a'.repeat(32), ts: new Date().toISOString(),
      source: { head: CUT, lib: CUT, chain_id: CHAIN }, coord: { event_id: 'ev-1', h: CUT, accepted: true, armed: true },
      ceremony: { ...fixture().ceremony, ...exitRep('ABORTED') } }) });
  assert.equal(r.status, 200);
  assert.ok(!(await markOf(base, 'bpc'))?.exited);
  await kill(proc);
});

test('rc.31 review M1: no exit mark for an abort after VERIFIED, for another H, without transitions, or with creation evidence', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base);
  const send = reporter(base);
  assert.equal((await send('bpa', exitRep('ABORTED', { transitions: [{ state: 'ARMED', ts: '2026-10-16T00:00:00Z' }, { state: 'VERIFIED', ts: '2026-10-16T00:01:00Z' }, { state: 'ABORTED', ts: '2026-10-16T00:02:00Z' }] }))).status, 200);
  assert.equal((await send('bpb', exitRep('ABORTED', { evidence: { h: CUT + 1 } }))).status, 200);
  assert.equal((await send('bpc', { state: 'ABORTED', evidence: { h: CUT } })).status, 200);
  assert.equal((await send('bpd', exitRep('ABORTED', { create_started: true }))).status, 200);
  assert.equal((await send('bpe', exitRep('STRANDED', { aborting: true }))).status, 200);
  for (const p of BPS) assert.ok(!(await markOf(base, p))?.exited, p);
  await kill(proc);
});

// ---- round 4 (review MEDIUM-1 / MEDIUM-2) -----------------------------------------------------------------------
import { copyFileSync, rmSync } from 'node:fs';
const marksState = async (base) => (await net(base)).marks_state;

test('rc.31 round 4 (MEDIUM-1): a lost or older server state makes the event\'s marks INCOMPLETE for good', async () => {
  for (const mode of ['lost', 'older']) {
    const dir = setup();
    let { base, proc } = await start(dir);
    await armedEvent(base);
    let send = reporter(base);
    assert.equal((await send('bpa', { state: 'FROZEN', evidence: { h: CUT } })).status, 200);
    const first = await marksState(base);
    assert.equal(first.complete, true);
    assert.equal(first.event_id, 'ev-1');
    // A backup taken now (an older copy for the restore case).
    const stateFile = join(dir, 'state', 'servers.json');
    const backup = join(dir, 'servers.backup.json');
    copyFileSync(stateFile, backup);
    assert.equal((await send('bpa', { state: 'VERIFIED', evidence: { h: CUT, snapshot_sha256: 'aa'.repeat(32) } })).status, 200);
    assert.ok((await marksState(base)).generation > first.generation);
    await kill(proc);
    if (mode === 'lost') rmSync(stateFile); else copyFileSync(backup, stateFile);
    ({ base, proc } = await start(dir));
    let ms = await marksState(base);
    assert.equal(ms.complete, false, `${mode}: ${JSON.stringify(ms)}`);
    // Sticky: new reports (and the generation growing past the old one) never make it complete again.
    send = reporter(base, Date.now() + 5e3);
    for (let i = 0; i < 3; i++) assert.equal((await send('bpb', { state: i ? 'FROZEN' : 'ARMED', evidence: { h: CUT } })).status, 200);
    await kill(proc);
    ({ base, proc } = await start(dir));
    ms = await marksState(base);
    assert.equal(ms.complete, false, `${mode} after restart: ${JSON.stringify(ms)}`);
    await kill(proc);
  }
});

test('rc.31 round 4 (MEDIUM-2): a re-entered member\'s later reports for the event are served as identity conflicts', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base, { early_abort: true });
  const send = reporter(base);
  assert.equal((await send('bpc', exitRep('ABORTED'))).status, 200);
  let p = (await net(base)).producers.find((x) => x.name === 'bpc');
  assert.equal(p.beacons[0].conflict, false, 'the exit report itself is not a conflict');
  // An rc.30 agent re-runs on a fresh journal under the same event and reaches VERIFIED.
  assert.equal((await send('bpc', { state: 'VERIFIED', evidence: { h: CUT, snapshot_sha256: 'aa'.repeat(32) },
    transitions: [{ state: 'ARMED', ts: '2026-10-16T00:05:00Z' }, { state: 'VERIFIED', ts: '2026-10-16T00:06:00Z' }] })).status, 200);
  p = (await net(base)).producers.find((x) => x.name === 'bpc');
  assert.equal(p.beacons[0].conflict, true, 'served as a conflict: no agent version counts it at a gate');
  assert.equal(p.beacons[0].exit_reentry, true);
  await kill(proc);
});

test('rc.31 (review fix 4): mission control raises QUORUM UNREACHABLE with the abort one-liner when its own view proves it', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base); // quorum 5 of 5
  const send = reporter(base);
  for (const p of BPS.slice(0, 4)) assert.equal((await send(p, { state: 'FROZEN' })).status, 200);
  let n = await net(base);
  assert.equal(n.fleet.quorum_unreachable, null);
  assert.equal((await send('bpe', exitRep('ABORTED'))).status, 200);
  n = await net(base);
  assert.deepEqual(n.fleet.quorum_unreachable?.out, ['bpe'], JSON.stringify(n.fleet));
  const w = n.fleet.alarms.find((x) => x.startsWith('ALARM QUORUM UNREACHABLE'));
  assert.ok(w && w.includes('control/coord.mjs abort') && w.includes('--event-id ev-1'), w);
  assert.ok(n.events.some((e) => e.text.startsWith('ALARM QUORUM UNREACHABLE')));
  // Someone past creation: not this alarm's call.
  assert.equal((await send('bpa', { state: 'VERIFIED', create_started: true })).status, 200);
  assert.equal((await net(base)).fleet.quorum_unreachable, null);
  await kill(proc);
});

test('rc.31 round 5 (MEDIUM-1): with the event\'s early abort OFF, an h5-style refuse → fix → re-ARM is counted as in rc.30', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base); // no early_abort in the signed event
  const send = reporter(base);
  // bp5 refuses ARM (never verified), the operator fixes it and re-runs on a fresh journal under the same event.
  assert.equal((await send('bpe', exitRep('ABORTED'))).status, 200);
  let m = await markOf(base, 'bpe');
  assert.ok(!m.exited, 'no exit mark with the flag off');
  assert.ok(m.exit_seen, 'remembered for the relay\'s own alarm only');
  assert.equal((await send('bpe', { state: 'VERIFIED', evidence: { h: CUT, snapshot_sha256: 'aa'.repeat(32) },
    transitions: [{ state: 'ARMED', ts: '2026-10-16T00:05:00Z' }, { state: 'VERIFIED', ts: '2026-10-16T00:06:00Z' }] })).status, 200);
  const n = await net(base);
  const p = n.producers.find((x) => x.name === 'bpe');
  assert.equal(p.beacons[0].conflict, false, 'not a conflict: peers count it at their gates as rc.30 did');
  m = await markOf(base, 'bpe');
  assert.ok(!m.reentered && !m.exited);
  assert.ok(!n.fleet.warnings.some((w) => w.includes('re-entered')));
  await kill(proc);
});

test('rc.31 round 5 (LOW-1): a joined report after an exit is neither a re-entry nor a conflict (flag on)', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  await armedEvent(base, { early_abort: true });
  const send = reporter(base);
  assert.equal((await send('bpc', exitRep('ABORTED'))).status, 200);
  assert.ok((await markOf(base, 'bpc')).exited);
  assert.equal((await send('bpc', { state: 'VERIFIED', joined: true, evidence: { h: CUT } })).status, 200);
  const p = (await net(base)).producers.find((x) => x.name === 'bpc');
  assert.equal(p.beacons[0].conflict, false);
  assert.ok(!(await markOf(base, 'bpc')).reentered);
  await kill(proc);
});
