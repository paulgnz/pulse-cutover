// node --test control/test/*.test.mjs
// rc.24: the old-chain SPLIT alarm without a guessed burn-off bound. Right after rc.23 mission control was deployed,
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

const CUT = 408462434, PAUSE = 408462811; // cut + 377, the live rehearsal
const AFTER = 'a1'.repeat(32), BID = 'TargetChainAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
const ce = (state, over = {}) => ({ state, since: null, seq: 1, transitions: [], evidence: {}, ...over });
const tgt = (head) => ({ blockchain_id: BID, subnet_id: null, chain_id: null, head, head_id: null, after_cut_id: AFTER });
const ev = (over = {}) => ({ event_id: 'ev-1', h: CUT, roster: BPS.map((producer) => ({ producer })), quorum: 4, ...over });
const PASTCE = (state = 'HALTED') => ce(state, { ignition_started: true, evidence: { cut_height: CUT }, target: tgt(CUT + 2) });
const PAUSEDCE = () => ce('VERIFIED', { evidence: { cut_height: CUT } });
const one = (c, head, silent = false) => [{ report: { coord: { event_id: 'ev-1' }, ceremony: c, source: { head } }, silent, conflict: false }];

/** Feed reports through the relay's mark logic (what server.js does per report) and return the marks. */
function relay(seq, e = ev()) {
  const marks = {}; let now = 1;
  for (const [p, c, head, freshOverride] of seq) {
    const m = { ...marks, [p]: nextEventMark(marks[p], c, null, false, now) || marks[p] };
    const fresh = freshOverride || new Set(Object.keys(marks).concat(p));
    const nm = nextEventMark(marks[p], c, head, movementArmed(e, m, fresh), now++);
    if (nm) marks[p] = nm;
  }
  return marks;
}

test('state ranks: paused from SNAPSHOTTED; ABORTED ranks 0 and never lowers a mark', () => {
  assert.ok(stateRank('FROZEN') < PAUSED_RANK && stateRank('SNAPSHOTTED') === PAUSED_RANK && stateRank('VERIFIED') > PAUSED_RANK);
  assert.ok(stateRank('IGNITED') > stateRank('VERIFIED') && stateRank('LIVE') > stateRank('FLIPPED'));
  assert.equal(stateRank('ABORTED'), 0); assert.equal(stateRank('bogus'), 0); assert.equal(stateRank('__proto__'), 0);
  const first = nextEventMark(undefined, ce('SNAPSHOTTED'), null, false, 1);
  assert.equal(nextEventMark(first, ce('ABORTED'), null, false, 2), null, 'an abort after the pause changes nothing');
  assert.equal(nextEventMark(first, ce('VERIFIED'), null, false, 2).rank, stateRank('VERIFIED'));
});

test('(a) the live false positive: five sources paused at cut + 377, no head_at_pause, past creation: no split', () => {
  const seq = [];
  for (const p of BPS) seq.push([p, ce('SNAPSHOTTED', { evidence: { cut_height: CUT } }), PAUSE]);
  for (let i = 0; i < 4; i++) for (const p of BPS) seq.push([p, PASTCE(i % 2 ? 'LIVE' : 'IGNITED'), PAUSE]);
  const marks = relay(seq);
  for (const p of BPS) assert.deepEqual([marks[p].src_first, marks[p].src_max], [PAUSE, PAUSE], p);
  const byP = Object.fromEntries(BPS.map((p) => [p, one(PASTCE('IGNITED'), PAUSE)]));
  const v = fleetVerdict(ev(), byP, marks);
  assert.notEqual(v.verdict, 'SPLIT', v.alarms.join('; '));
  assert.equal(v.alarms.length, 0);
});

test('(b) r4: four past creation, one resumed silently, the heads advance across reports: split', () => {
  const seq = [];
  for (const p of BPS) seq.push([p, ce('SNAPSHOTTED', { evidence: { cut_height: CUT } }), PAUSE]);
  // bpe goes silent after SNAPSHOTTED and resumes its producer; the others' nodeos follow its blocks.
  const live4 = new Set(['bpa', 'bpb', 'bpc', 'bpd']);
  let head = PAUSE;
  for (let i = 0; i < 4; i++) { for (const p of live4) seq.push([p, PASTCE('IGNITED'), head, live4]); head += 6; }
  const final = head - 6; // 408462829
  const marks = relay(seq);
  assert.equal(marks.bpa.src_first, PAUSE); assert.equal(marks.bpa.src_max, final);
  const byP = { ...Object.fromEntries([...live4].map((p) => [p, one(PASTCE('IGNITED'), final)])), bpe: one(ce('SNAPSHOTTED'), PAUSE, true) };
  const v = fleetVerdict(ev(), byP, marks);
  assert.equal(v.verdict, 'SPLIT');
  assert.match(v.alarms.join(' '), new RegExp(`old chain is still advancing after chain creation \\(bpa source head ${PAUSE}→${final}`));
  // Movement within the tolerance (late blocks absorbed after the pause) is not a split.
  const near = relay([...BPS.map((p) => [p, ce('SNAPSHOTTED'), PAUSE]), ...BPS.map((p) => [p, PASTCE(), PAUSE]), ...BPS.map((p) => [p, PASTCE(), PAUSE + 12])]);
  assert.notEqual(fleetVerdict(ev(), byP, near).verdict, 'SPLIT');
});

test('(b2) a member silent before SNAPSHOTTED does not block the rule once a quorum is past creation', () => {
  const live4 = new Set(['bpa', 'bpb', 'bpc', 'bpd']);
  const seq = [['bpe', ce('FROZEN'), CUT + 100, new Set(BPS)]];
  for (const p of live4) seq.push([p, ce('SNAPSHOTTED'), PAUSE, live4]);
  // Three past creation (below quorum 4): not armed yet; the fourth makes the quorum.
  for (const p of ['bpa', 'bpb', 'bpc']) seq.push([p, PASTCE(), PAUSE, live4]);
  let marks = relay(seq);
  assert.equal(marks.bpa.src_first, undefined, 'not armed below the quorum while bpe is not paused');
  seq.push(['bpd', PASTCE(), PAUSE, live4]);
  for (const p of live4) seq.push([p, PASTCE(), PAUSE + 30, live4]);
  marks = relay(seq);
  const byP = Object.fromEntries([...live4].map((p) => [p, one(PASTCE(), PAUSE + 30)]));
  assert.equal(fleetVerdict(ev(), { ...byP, bpe: one(ce('FROZEN'), CUT + 100, true) }, marks).verdict, 'SPLIT');
});

test('(c) a slow BP still FROZEN making burn-off while others created: no split until every member is ≥ SNAPSHOTTED', () => {
  const four = ['bpa', 'bpb', 'bpc', 'bpd'];
  const seq = [];
  for (const p of four) seq.push([p, ce('SNAPSHOTTED'), CUT + 300]);
  seq.push(['bpe', ce('FROZEN'), CUT + 300]);
  // Others ignite while bpe (reporting, fresh) is still FROZEN and its producer keeps the head moving.
  for (let h = CUT + 300; h <= PAUSE; h += 20) { for (const p of four) seq.push([p, PASTCE(), h]); seq.push(['bpe', ce('FROZEN'), h]); }
  let marks = relay(seq);
  for (const p of BPS) assert.equal(marks[p].src_first, undefined, `${p} not tracked while bpe is in burn-off`);
  const byP = (h) => ({ ...Object.fromEntries(four.map((p) => [p, one(PASTCE(), h)])), bpe: one(ce('FROZEN'), h) });
  assert.notEqual(fleetVerdict(ev(), byP(PAUSE), marks).verdict, 'SPLIT');
  // bpe pauses at PAUSE: now armed; the heads stay: still no split.
  seq.push(['bpe', ce('SNAPSHOTTED'), PAUSE]);
  for (const p of BPS) seq.push([p, p === 'bpe' ? ce('VERIFIED') : PASTCE(), PAUSE]);
  marks = relay(seq);
  assert.equal(marks.bpa.src_first, PAUSE);
  assert.notEqual(fleetVerdict(ev(), byP(PAUSE), marks).verdict, 'SPLIT');
  // ...and a resume after that is caught.
  for (const p of four) seq.push([p, PASTCE(), PAUSE + 40]);
  marks = relay(seq);
  assert.equal(fleetVerdict(ev(), byP(PAUSE + 40), marks).verdict, 'SPLIT');
});

test('(d) the complete pause-head bound rule is unchanged', () => {
  const m = (state, head, evd, extra = {}) => one(ce(state, { evidence: evd, ...extra }), head);
  const past = { ignition_started: true, target: tgt(CUT + 2) };
  const byP = { bpa: m('HALTED', CUT + 24, { cut_height: CUT, head_at_pause: CUT + 24 }, past) };
  assert.notEqual(fleetVerdict(ev(), { ...byP, bpc: m('VERIFIED', CUT + 36, { cut_height: CUT, head_at_pause: CUT + 24 }) }).verdict, 'SPLIT');
  const v = fleetVerdict(ev(), { ...byP, bpc: m('VERIFIED', CUT + 37, { cut_height: CUT, head_at_pause: CUT + 24 }) });
  assert.equal(v.verdict, 'SPLIT');
  assert.match(v.alarms[0], new RegExp(`old chain advanced past the cut \\(bpc source head ${CUT + 37} > ${CUT + 36}\\)`));
  // Incomplete (bpc without head_at_pause): no fixed bound, whatever the pause head.
  assert.notEqual(fleetVerdict(ev(), { ...byP, bpc: m('VERIFIED', PAUSE, { cut_height: CUT }) }).verdict, 'SPLIT');
});

// ---- end to end: the relay tracks movement and persists it ----------------------------------------------
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

test('(a)+(b)+(e) relay: no split while paused at cut + 377; movement after creation latches; marks survive a restart', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  assert.equal((await fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ type: 'event', network: 'testnet', chain_id: CHAIN,
    event_id: 'ev-1', h: CUT, roster: BPS.map((producer) => ({ producer })), quorum: 4 })) })).status, 200);
  let t = Date.now() - 20e3;
  const send = (producer, ceremony, head) => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[producer]}` },
    body: JSON.stringify({ ...fixture(), producer, network: 'testnet', role: 'producer', ts: new Date(t += 10).toISOString(),
      source: { head, lib: CUT, chain_id: CHAIN }, coord: { event_id: 'ev-1', h: CUT, accepted: true, armed: true },
      ceremony: { ...fixture().ceremony, evidence: { cut_height: CUT }, ...ceremony } }) });
  for (const p of BPS) assert.equal((await send(p, { state: 'SNAPSHOTTED' }, PAUSE)).status, 200);
  for (let i = 0; i < 2; i++) for (const p of BPS) assert.equal((await send(p, { state: 'IGNITED', ignition_started: true, target: tgt(CUT + 2) }, PAUSE)).status, 200);
  let n = await net(base);
  assert.notEqual(n.fleet.verdict, 'SPLIT', JSON.stringify(n.fleet.alarms));
  let m = n.producers.find((p) => p.name === 'bpa').event_max['ev-1'];
  assert.deepEqual([m.src_first, m.src_max, m.rank], [PAUSE, PAUSE, 5]);
  // Restart: the marks (rank, src_first, src_max) are persisted.
  await kill(proc);
  ({ base, proc } = await start(dir));
  m = (await net(base)).producers.find((p) => p.name === 'bpa').event_max['ev-1'];
  assert.deepEqual([m.src_first, m.src_max, m.rank, m.past_create], [PAUSE, PAUSE, 5, true]);
  // r4: bpe resumed silently; the others' source heads advance.
  for (const head of [PAUSE + 7, PAUSE + 19]) for (const p of ['bpa', 'bpb', 'bpc', 'bpd']) assert.equal((await send(p, { state: 'IGNITED', ignition_started: true, target: tgt(CUT + 2) }, head)).status, 200);
  n = await net(base);
  assert.equal(n.fleet.verdict, 'SPLIT');
  assert.match(n.fleet.alarms.join(' '), /old chain is still advancing after chain creation/);
  assert.equal(n.fleet.latched, true);
  m = n.producers.find((p) => p.name === 'bpb').event_max['ev-1'];
  assert.deepEqual([m.src_first, m.src_max], [PAUSE, PAUSE + 19]);
  await kill(proc);
});

test('(e) an rc.23 state file (marks without rank/src fields, and the first single-event shape) still loads; bad fields are dropped', async () => {
  const dir = setup();
  writeFileSync(join(dir, 'state', 'servers.json'), JSON.stringify({ v: 1, lastTs: {}, nodes: {}, event_max: { testnet: {
    bpa: { 'ev-1': { past_create: true, state: 'IGNITED', at: 5 } },
    bpb: { event_id: 'ev-1', past_create: false, state: 'VERIFIED' },
    bpc: { 'ev-1': { past_create: true, state: 'LIVE', at: 6, rank: 'x', src_first: 10, src_max: 5 } } } } }));
  const { base, proc } = await start(dir);
  // Producers are listed once they report (any event).
  let t = Date.now() - 10e3;
  for (const p of ['bpa', 'bpb', 'bpc']) assert.equal((await fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[p]}` },
    body: JSON.stringify({ ...fixture(), producer: p, network: 'testnet', ts: new Date(t += 10).toISOString() }) })).status, 200);
  const n = await net(base);
  const mk = (p) => n.producers.find((x) => x.name === p)?.event_max?.['ev-1'];
  assert.equal(mk('bpa').past_create, true);
  assert.equal(mk('bpb').state, 'VERIFIED');
  assert.equal(mk('bpc').rank, undefined); assert.equal(mk('bpc').src_first, undefined);
  await kill(proc);
});

test('rc.28 verification: an ABORTED report after creation started arms the old-chain movement rule', () => {
  const seq = [];
  for (const p of BPS) seq.push([p, ce('SNAPSHOTTED', { evidence: { cut_height: CUT } }), 500]);
  seq.push(['bpa', ce('ABORTED', { create_started: true, evidence: { cut_height: CUT } }), 500]);
  for (const p of BPS) seq.push([p, ce('ABORTED', { evidence: { cut_height: CUT } }), 520]);
  const marks = relay(seq);
  assert.equal(marks.bpa.past_create, true, 'creation evidence on an ABORTED report');
  const moved = Object.values(marks).filter((m) => Number.isInteger(m.src_first) && m.src_max - m.src_first > 12);
  assert.ok(moved.length > 0, JSON.stringify(marks));
});
