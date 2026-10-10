// node --test control/test/*.test.mjs
// rc.23: findings of the 5-BP fleet rehearsal on the upstream stack (2026-10-06): a beacon instance that is replaced
// (new run directory, reinstall) must not leave its token conflicted forever; mission control computes a fleet
// verdict per event (LIVE / DEGRADED / SPLIT) and raises a red alarm when one roster member resumed the old chain
// while others ignited, or when members report different target chains; private Metal networks are not judged
// against Tahoe.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';

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

function setup(netExtra = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'mc-rc23-'));
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN,
    rpc: [], coordinators: [PUBHEX], static_producers: true, ...netExtra }] }));
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
const postR = (base, body, producer = 'bpa') => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[producer]}` }, body: JSON.stringify(body) });
const postC = (base, m) => fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(m) });
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
const inst = (c) => c.repeat(32);
const net = async (base) => (await (await fetch(`${base}/api/status`)).json()).networks.find((n) => n.id === 'testnet');

test('a new instance for a token whose old instance went silent replaces it (no permanent conflict)', async () => {
  const { base, proc } = await start(setup());
  const old = new Date(Date.now() - 3 * 60e3).toISOString();      // accepted (< 5 min), silent (> 45 s)
  assert.equal((await postR(base, { ...fixture(), producer: 'bpa', network: 'testnet', instance_id: inst('a'), ts: old })).status, 200);
  assert.equal((await postR(base, { ...fixture(), producer: 'bpa', network: 'testnet', instance_id: inst('b'), ts: new Date().toISOString() })).status, 200);
  let n = await net(base);
  let p = n.producers.find((x) => x.name === 'bpa');
  assert.equal(p.beacons.length, 1, 'the silent instance was replaced');
  assert.equal(p.beacons[0].conflict, false);
  assert.ok(n.events.some((e) => /replaced 1 silent instance/.test(e.text)));
  // The old machine comes back and reports while the new one is fresh: that IS one token on two machines.
  assert.equal((await postR(base, { ...fixture(), producer: 'bpa', network: 'testnet', instance_id: inst('a'), ts: new Date(Date.now() + 5).toISOString() })).status, 200);
  p = (await net(base)).producers.find((x) => x.name === 'bpa');
  assert.equal(p.beacons.length, 2);
  assert.ok(p.beacons.every((b) => b.conflict), 'two concurrently reporting instances stay a real conflict');
  await kill(proc);
});


// ---- fleet verdict --------------------------------------------------------------------------------------
import { fleetVerdict, projectReport } from '../lib.mjs';
const AFTER = 'a1'.repeat(32), OTHER = 'b2'.repeat(32), BID = 'TargetChainAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', BID2 = 'TargetChainBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB';
const ce = (state, over = {}) => ({ state, since: null, seq: 1, transitions: [], evidence: {}, ...over });
const tgt = (head, after = AFTER, bid = BID) => ({ blockchain_id: bid, subnet_id: null, chain_id: null, head, head_id: null, after_cut_id: after });
const rep = (ceremony, event = 'ev-1') => ({ coord: { event_id: event }, ceremony });
const ev = (over = {}) => ({ event_id: 'ev-1', h: 1000, roster: BPS.map((producer) => ({ producer })), quorum: 4, ...over });
const fleetOf = (states) => Object.fromEntries(Object.entries(states).map(([p, c]) => [p, [{ report: rep(c), silent: false, conflict: false }]]));

test('fleet verdict: LIVE needs a quorum of roster members LIVE on one chain with a common block after H', () => {
  const live = (h) => ce('LIVE', { target: tgt(h) });
  let v = fleetVerdict(ev(), fleetOf({ bpa: live(1005), bpb: live(1006), bpc: live(1004), bpd: live(1006), bpe: ce('HALTED', { target: tgt(1003) }) }));
  assert.equal(v.verdict, 'LIVE');
  assert.deepEqual(v.alarms, []);
  assert.equal(v.live_chain.members.length, 4);
  assert.deepEqual(v.not_live, ['bpe: HALTED']);
  // r2 shape: 2 LIVE / 3 HALTED on one chain → not a quorum: DEGRADED (not SPLIT: one chain)
  v = fleetVerdict(ev(), fleetOf({ bpa: live(1005), bpb: ce('HALTED', { target: tgt(1004) }), bpc: live(1004), bpd: ce('HALTED', { target: tgt(1004) }), bpe: ce('HALTED', { target: tgt(1004) }) }));
  assert.equal(v.verdict, 'DEGRADED');
  // no common block after the cut yet: not LIVE
  v = fleetVerdict(ev(), fleetOf({ bpa: ce('LIVE', { target: tgt(1000, null) }), bpb: ce('LIVE', { target: tgt(1000, null) }), bpc: ce('LIVE', { target: tgt(1000, null) }), bpd: ce('LIVE', { target: tgt(1000, null) }) }));
  assert.notEqual(v.verdict, 'LIVE');
  // pre-creation and symmetric abort
  assert.equal(fleetVerdict(ev(), fleetOf({ bpa: ce('VERIFIED'), bpb: ce('FROZEN') })).verdict, 'PENDING');
  assert.equal(fleetVerdict(ev(), fleetOf({ bpa: ce('ABORTED', { rollback_complete: true }), bpb: ce('ABORTED', { rollback_complete: true }) })).verdict, 'ABORTED');
  // no roster: never LIVE
  assert.equal(fleetVerdict(ev({ roster: undefined, quorum: undefined }), fleetOf({ bpa: live(1005) })).verdict, 'DEGRADED');
});

test('fleet verdict: r4 — a member that resumed the old chain while peers ignited is a RED split', () => {
  const halted = ce('HALTED', { ignition_started: true, target: tgt(1002) });
  const v = fleetVerdict(ev(), fleetOf({ bpa: halted, bpb: halted, bpc: halted, bpd: halted,
    bpe: ce('ABORTED', { rollback_complete: true, source_resumed: true }) }));
  assert.equal(v.verdict, 'SPLIT');
  assert.match(v.alarms[0], /^split: bpe resumed the old chain after peers ignited \(bpa HALTED/);
  // a silent member's last report still counts for the alarm (r4: the isolated BP could not report)
  const byP = fleetOf({ bpa: halted, bpb: halted });
  byP.bpe = [{ report: rep(ce('ABORTED', { rollback_complete: true })), silent: true, conflict: false }];
  assert.equal(fleetVerdict(ev(), byP).verdict, 'SPLIT');
  // a member that only reached chain creation (create_started) counts as past it
  assert.equal(fleetVerdict(ev(), fleetOf({ bpa: ce('VERIFIED', { create_started: true }), bpe: ce('ABORTED', { rollback_complete: true }) })).verdict, 'SPLIT');
  // a STRANDED member never resumed: no alarm
  assert.equal(fleetVerdict(ev(), fleetOf({ bpa: halted, bpe: ce('STRANDED') })).verdict, 'DEGRADED');
});

test('fleet verdict: members on different target chains, after-cut blocks or blocks at one height are a split', () => {
  const live = (t) => ce('LIVE', { target: t });
  assert.match(fleetVerdict(ev(), fleetOf({ bpa: live(tgt(1005)), bpb: live(tgt(1005, AFTER, BID2)) })).alarms[0], /different target chains/);
  assert.match(fleetVerdict(ev(), fleetOf({ bpa: live(tgt(1005)), bpb: live(tgt(1005, OTHER)) })).alarms[0], /different blocks after the cut/);
  const at = (id) => ({ ...tgt(1009), head_id: id });
  assert.match(fleetVerdict(ev(), fleetOf({ bpa: live(at(AFTER)), bpb: live(at(OTHER)) })).alarms[0], /different blocks at height 1009/);
});

test('the relay keeps the rc.23 ceremony fields and STRANDED; mission control raises the split on /api/status', async () => {
  const r = { ...fixture(), producer: 'bpa', network: 'testnet', ts: new Date().toISOString() };
  r.ceremony = { ...r.ceremony, state: 'STRANDED', create_started: false, source_resumed: false, degraded: true, degraded_reason: 'gap 31 s at /opt/x', joined: false,
    target: tgt(1003) };
  const p = projectReport(r);
  assert.equal(p.ceremony.state, 'STRANDED');
  assert.equal(p.ceremony.target.after_cut_id, AFTER);
  assert.ok(!p.ceremony.degraded_reason.includes('/opt'));
  assert.throws(() => projectReport({ ...r, ceremony: { ...r.ceremony, target: { blockchain_id: '<script>' } } }), /blockchain_id/);

  const { base, proc } = await start(setup());
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-1', h: 1000, roster: BPS.map((producer) => ({ producer })), quorum: 4 }))).status, 200);
  const send = (producer, ceremony) => postR(base, { ...fixture(), producer, network: 'testnet', role: 'producer', ts: new Date().toISOString(),
    source: { head: 1100, lib: 1000, chain_id: CHAIN },
    coord: { event_id: 'ev-1', h: 1000, accepted: true, armed: true }, ceremony: { ...fixture().ceremony, ...ceremony } }, producer);
  for (const b of ['bpa', 'bpb', 'bpc', 'bpd']) assert.equal((await send(b, { state: 'HALTED', ignition_started: true, target: tgt(1002) })).status, 200);
  let n = await net(base);
  assert.equal(n.fleet.verdict, 'DEGRADED');
  assert.equal((await send('bpe', { state: 'ABORTED', rollback_complete: true, source_resumed: true, ignition_started: false, target: null })).status, 200);
  n = await net(base);
  assert.equal(n.fleet.verdict, 'SPLIT');
  assert.match(n.fleet.alarms[0], /bpe resumed the old chain after peers ignited/);
  assert.ok(n.events.some((e) => /FLEET SPLIT \(ev-1\)/.test(e.text)), 'the split is logged');
  const html = await (await fetch(`${base}/testnet`)).text();
  assert.match(html, /id="fleet"/);
  await kill(proc);
});

test('r4 with the resumer SILENT before it aborted: the old chain advancing past the burn-off is the split; it latches', async () => {
  // Pure rule first: no member reports a resume, but source heads moved past every pause head while peers are past creation.
  const halted = (head) => ({ report: { ...rep(ce('HALTED', { ignition_started: true, evidence: { cut_height: 1000, head_at_pause: 1024 }, target: tgt(1002) })),
    source: { head } }, silent: false, conflict: false });
  const byP = { bpa: [halted(1024)], bpb: [halted(1024)], bpe: [{ report: { ...rep(ce('VERIFIED')), source: { head: 1024 } }, silent: true, conflict: false }] };
  assert.equal(fleetVerdict(ev(), byP).verdict, 'DEGRADED', 'paused at the pause head: no split');
  byP.bpb = [halted(1090)];
  const v = fleetVerdict(ev(), byP);
  assert.equal(v.verdict, 'SPLIT');
  assert.match(v.alarms[0], /old chain advanced past the cut \(bpb source head 1090 > 1036\)/);
  // The relay's high-water mark keeps a replaced member past creation.
  assert.equal(fleetVerdict(ev(), fleetOf({ bpa: ce('VERIFIED'), bpe: ce('ABORTED', { rollback_complete: true }) }),
    { bpa: { past_create: true, state: 'IGNITED' } }).verdict, 'SPLIT');

  // End to end: the relay latches the split until the operator clears it.
  const dir = setup();
  const { base, proc } = await start(dir);
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-1', h: 1000, roster: BPS.map((producer) => ({ producer })), quorum: 4 }))).status, 200);
  const send = (producer, ceremony, head) => postR(base, { ...fixture(), producer, network: 'testnet', role: 'producer', ts: new Date().toISOString(),
    source: { head, lib: 1000, chain_id: CHAIN }, coord: { event_id: 'ev-1', h: 1000, accepted: true, armed: true },
    ceremony: { ...fixture().ceremony, evidence: { cut_height: 1000, head_at_pause: 1024 }, ...ceremony } }, producer);
  // bpe reported VERIFIED, then lost the relay (silent) and resumed the old chain; the others ignited.
  assert.equal((await send('bpe', { state: 'VERIFIED' }, 1024)).status, 200);
  for (const b of ['bpa', 'bpb', 'bpc', 'bpd']) assert.equal((await send(b, { state: 'HALTED', ignition_started: true, target: tgt(1002) }, 1060)).status, 200);
  let n = await net(base);
  assert.equal(n.fleet.verdict, 'SPLIT', JSON.stringify(n.fleet));
  assert.match(n.fleet.alarms.join(' '), /old chain advanced past the cut/);
  // The heads look normal again (e.g. a nodeos restored from a snapshot): still SPLIT, latched.
  for (const b of ['bpa', 'bpb', 'bpc', 'bpd']) await send(b, { state: 'HALTED', ignition_started: true, target: tgt(1002) }, 1020);
  n = await net(base);
  assert.equal(n.fleet.verdict, 'SPLIT');
  assert.equal(n.fleet.latched, true);
  assert.equal((await fetch(`${base}/api/admin/clear-split?net=testnet`, { method: 'POST', headers: { 'x-real-ip': '203.0.113.9' } })).status, 403);
  assert.equal((await fetch(`${base}/api/admin/clear-split?net=testnet`, { method: 'POST' })).status, 200);
  n = await net(base);
  assert.notEqual(n.fleet.verdict, 'SPLIT');
  // The high-water mark is published for the agents.
  const m = n.producers.find((p) => p.name === 'bpa').event_max['ev-1'];
  assert.deepEqual([m.past_create, m.state], [true, 'HALTED']);
  await kill(proc);
});

test('re-check #1: the per-event high-water mark cannot be reset by an event-id bounce, and survives a restart', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  // rc.27: marks exist only for events the coordinator signed.
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-1', h: 1000,
    roster: BPS.map((producer) => ({ producer })), quorum: 4 }))).status, 200);
  let t = Date.now() - 60e3;
  const send = (event, state, extra = {}) => postR(base, { ...fixture(), producer: 'bpa', network: 'testnet', role: 'producer', ts: new Date(t += 1000).toISOString(),
    source: { head: 1001, lib: 1000, chain_id: CHAIN }, coord: { event_id: event, h: 1000, accepted: true, armed: true },
    ceremony: { ...fixture().ceremony, state, ...extra } }, 'bpa');
  const mark = async () => (await net(base)).producers.find((p) => p.name === 'bpa').event_max;
  assert.equal((await send('ev-1', 'HALTED', { ignition_started: true })).status, 200);
  // The same token reports another event id, then ev-1 again from a pre-creation state.
  assert.equal((await send('ev-x', 'VERIFIED', { ignition_started: false })).status, 200);
  assert.equal((await send('ev-1', 'VERIFIED', { ignition_started: false })).status, 200);
  let m = await mark();
  assert.equal(m['ev-1'].past_create, true, 'ev-1 stays past creation');
  assert.equal(m['ev-1'].state, 'HALTED');
  assert.equal(m['ev-x'], undefined, 'rc.27: an event id the coordinator never signed gets no mark (no flooding)');
  await kill(proc);
  ({ base, proc } = await start(dir));
  m = await mark();
  assert.equal(m['ev-1'].past_create, true, 'persisted across a restart');
  await kill(proc);
});

test('re-check #4: an incomplete pause-head bound (a beacon without head_at_pause) never latches a false split', () => {
  const m = (state, head, ev, extra = {}) => [{ report: { ...rep(ce(state, { evidence: ev, ...extra })), source: { head } }, silent: false, conflict: false }];
  const past = { ignition_started: true, target: tgt(1002) };
  // bpc reached SNAPSHOTTED (cut height) on an older beacon without head_at_pause and paused at 1080: incomplete
  // bound → cut + 360; 1080 is within it.
  const byP = { bpa: m('HALTED', 1024, { cut_height: 1000, head_at_pause: 1024 }, past), bpc: m('VERIFIED', 1080, { cut_height: 1000 }) };
  assert.notEqual(fleetVerdict(ev(), byP).verdict, 'SPLIT');
  // Pause skew within the tolerance is not a split either.
  assert.notEqual(fleetVerdict(ev(), { ...byP, bpc: m('VERIFIED', 1030, { cut_height: 1000, head_at_pause: 1024 }) }).verdict, 'SPLIT');
  // Complete bound, a head well past it: split.
  assert.equal(fleetVerdict(ev(), { ...byP, bpc: m('VERIFIED', 1080, { cut_height: 1000, head_at_pause: 1024 }) }).verdict, 'SPLIT');
  // rc.24: an incomplete bound is no bound at all (the fixed cut + 360 fallback latched a false split on a real
  // finality lag of 377); a pause head far past the cut is judged by the movement rule instead (rc24.test.mjs).
  assert.notEqual(fleetVerdict(ev(), { ...byP, bpc: m('VERIFIED', 1400, { cut_height: 1000 }) }).verdict, 'SPLIT');
});
