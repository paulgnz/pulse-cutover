// node --test control/test/*.test.mjs
// rc.33: the all-producers gate. The coordinator binds every producer that could produce before the pause (active,
// pending and proposed schedule, plus standbys) into the signed event; mission control refuses to relay the arm until
// each one's agent is ready (accepted under this gate, will start on the arm, new enough, admission lock on and probed),
// names the ones that are not, and marks the fleet NOT READY when a member drops out after the arm.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { producerGate, producerGateCanonical, producerGateSha, gateScheduleProblems, eventPayload, compareVersions, versionAtLeast,
  parseCover, PRODUCER_GATE_MIN_AGENT_VERSION } from '../coordlib.mjs';
import { producerGateView, producerGatePhase, projectReport, SETUP_CHECKS } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CHAIN = '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd';
const sha = (s) => createHash('sha256').update(s).digest('hex');
const BPS = ['bpa', 'bpb', 'bpc'];
const TOK = Object.fromEntries(BPS.map((p, i) => [p, String(i + 7).repeat(64)]));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const PUBHEX = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');
const signed = (payload) => { const p = JSON.stringify(payload); return { payload: p, key: PUBHEX, sig: sign(null, Buffer.from(p), privateKey).toString('hex') }; };
const procs = [];
after(() => procs.forEach((p) => p.kill('SIGKILL')));
const CUT = 408500000;
const V = PRODUCER_GATE_MIN_AGENT_VERSION;
const sched = (v, names) => ({ version: v, producers: names.map((producer_name) => ({ producer_name, authority: [0, {}] })) });
// bpa and bpb are scheduled, bpc is a standby.
const SCHEDULE = { active: sched(5, ['bpa', 'bpb']), pending: null, proposed: null };
const roster = BPS.map((producer) => ({ producer }));
const GATE = producerGate(SCHEDULE, { roster, standby: ['bpc'], eventId: 'ev-g', h: CUT });
const G = { eventId: 'ev-g', h: CUT };

test('rc.33 the canonical text and hash match the shared vector the agent checks (src/producer_gate.rs)', () => {
  const v = JSON.parse(readFileSync(join(HERE, 'fixtures', 'producer-gate-vector.json'), 'utf8'));
  assert.equal(producerGateCanonical(v.gate), v.canonical);
  assert.equal(producerGateSha(v.gate), v.gate.sha256);
});

test('rc.33 producerGate binds active ∪ pending ∪ proposed ∪ standby and refuses an uncovered producer', () => {
  const g = producerGate({ active: sched(5, ['bpb', 'bpa']), pending: sched(6, ['bpa', 'bpd']), proposed: sched(7, ['bpe']) },
    { roster: ['bpa', 'bpb', 'bpd', 'bpe', 'bpf'].map((producer) => ({ producer })), standby: ['bpf'], ...G });
  assert.deepEqual(g.required, ['bpa', 'bpb', 'bpd', 'bpe', 'bpf']);
  assert.deepEqual(g.active.producers, ['bpb', 'bpa'], 'schedule order is kept (it is part of the binding)');
  assert.equal(g.sha256, producerGateSha(g));
  assert.throws(() => producerGate(SCHEDULE, { roster: [{ producer: 'bpa' }], standby: [], ...G }), /no --roster member runs their agent: bpb/);
  // one server signing for several names (rehearsals): --cover
  const c = producerGate(SCHEDULE, { roster: [{ producer: 'box1' }], standby: ['bpc'], cover: parseCover('bpa:box1,bpb:box1,bpc:box1'), ...G });
  assert.deepEqual(c.cover, { bpa: 'box1', bpb: 'box1', bpc: 'box1' });
  assert.throws(() => parseCover('bpa'), /name:member/);
  assert.throws(() => producerGate(SCHEDULE, { roster, cover: { zzz: 'bpa' }, ...G }), /not in the schedule/);
  assert.throws(() => producerGate({ active: null }, { roster, ...G }), /no active schedule/);
  assert.throws(() => producerGate(SCHEDULE, { roster, standby: ['bpc'] }), /event id/, 'review fix 1: bound to an event');
  assert.throws(() => producerGate(SCHEDULE, { roster, standby: ['bpc'], cover: { bpa: 'Bad-Name' }, ...G }), /not an account name/);
  const other = producerGate(SCHEDULE, { roster, standby: ['bpc'], eventId: 'ev-h', h: CUT + 600 });
  assert.notEqual(other.sha256, GATE.sha256, 'a re-issued event with the same schedule hashes differently');
  assert.throws(() => producerGate(SCHEDULE, { roster, standby: ['bpc'], minAgentVersion: '0.5.0-rc.32', ...G }), /at least/);
});

test('rc.33 eventPayload: --producer-gate needs the roster and the source schedule, and puts the gate in the signed payload', () => {
  const a = { net: 'testnet', 'chain-id': CHAIN, h: String(CUT), roster: 'bpa,bpb,bpc', 'producer-gate': 'yes', standby: 'bpc', 'event-id': 'ev-g' };
  assert.throws(() => eventPayload(a, 1), /source schedule/);
  assert.throws(() => eventPayload({ ...a, roster: undefined }, 1, { schedule: SCHEDULE }), /needs --roster/);
  const p = eventPayload(a, 1, { schedule: SCHEDULE });
  assert.deepEqual(p.producer_gate, GATE);
  assert.equal(eventPayload({ ...a, 'producer-gate': undefined }, 1).producer_gate, undefined);
  // Review fix 4: XPR mainnet refuses a cover mapping unless the event is a rehearsal (or shadow).
  const MAIN = '384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0';
  // rc.33 decision: XPR mainnet pins a NEW target chain_id in the signed event.
  assert.throws(() => eventPayload({ ...a, 'chain-id': MAIN }, 1, { schedule: SCHEDULE }), /must pin the new chain_id/);
  assert.throws(() => eventPayload({ ...a, 'chain-id': MAIN, 'target-chain-id': MAIN }, 1, { schedule: SCHEDULE }), /NEW chain_id/);
  assert.throws(() => eventPayload({ ...a, 'target-chain-id': 'xyz' }, 1, { schedule: SCHEDULE }), /64 hex/);
  assert.equal(eventPayload({ ...a, 'chain-id': MAIN, 'target-chain-id': 'C0'.repeat(32) }, 1, { schedule: SCHEDULE }).target_chain_id, 'c0'.repeat(32));
  const m = { ...a, 'chain-id': MAIN, 'target-chain-id': 'c0'.repeat(32), roster: 'bpa,box1', cover: 'bpb:box1,bpc:box1' };
  assert.throws(() => eventPayload(m, 1, { schedule: SCHEDULE }), /XPR MAINNET.*bpb→box1/);
  assert.ok(eventPayload({ ...m, rehearsal: 'yes' }, 1, { schedule: SCHEDULE }).producer_gate);
  assert.ok(eventPayload({ ...a, roster: 'bpa,box1', cover: 'bpb:box1,bpc:box1' }, 1, { schedule: SCHEDULE }).producer_gate, 'testnet may map');
});

test('rc.33 versions compare by semver precedence (same rules as the agent)', () => {
  assert.equal(compareVersions('0.5.0-rc.33', '0.5.0-rc.33'), 0);
  assert.ok(versionAtLeast('0.5.0-rc.100', '0.5.0-rc.33'));
  assert.ok(!versionAtLeast('0.5.0-rc.32', '0.5.0-rc.33'));
  assert.ok(versionAtLeast('0.5.0', '0.5.0-rc.33'));
  assert.ok(!versionAtLeast(null, '0.5.0-rc.33'));
  assert.ok(!versionAtLeast('x', '0.5.0-rc.33'));
});

test('rc.33 gateScheduleProblems: a producer outside the set, or a changed active / pending schedule', () => {
  assert.deepEqual(gateScheduleProblems(GATE, SCHEDULE), []);
  assert.deepEqual(gateScheduleProblems(GATE, { ...SCHEDULE, proposed: sched(6, ['bpa', 'bpc']) }), [], 'a proposal among armed producers');
  assert.match(gateScheduleProblems(GATE, { ...SCHEDULE, proposed: sched(6, ['bpa', 'bpx']) })[0], /bpx \(proposed\)/);
  assert.match(gateScheduleProblems(GATE, { ...SCHEDULE, pending: sched(6, ['bpa', 'bpc']) }).join(), /pending schedule changed/);
  assert.match(gateScheduleProblems(GATE, { active: sched(6, ['bpa', 'bpb']) }).join(), /active schedule changed/);
  assert.match(gateScheduleProblems(GATE, {}).join(), /no active schedule/);
});

const EV = { event_id: 'ev-g', h: CUT, roster, producer_gate: GATE };
const rep = (over = {}) => {
  const { coord = {}, ceremony = null, checks = null, agent_version = V } = over;
  return { role: 'producer', agent_version, coord: { event_id: 'ev-g', accepted: true, producer_gate_sha256: GATE.sha256, arm_ready: true, ...coord },
    checks: checks || [{ name: 'admission_lock', ok: true }, { name: 'admission_lock_probe', ok: true }], ceremony };
};
const armed = (over = {}) => rep({ ...over, ceremony: { state: 'ARMED', evidence: { h: CUT, producer_gate_sha256: GATE.sha256, agent_version: V } } });
const armedE0 = () => rep({ ceremony: { state: 'ARMED', evidence: { h: CUT - 600, producer_gate_sha256: producerGate(SCHEDULE, { roster, standby: ['bpc'], eventId: 'ev-0', h: CUT - 600 }).sha256, agent_version: V } } });
const by = (m) => Object.fromEntries(Object.entries(m).map(([p, r]) => [p, r ? [{ report: r, silent: false, conflict: false, age_ms: 1000 }] : []]));

test('rc.33 producerGateView (arm phase): ready only when every bound producer is ready; each gap is named', () => {
  const all = by({ bpa: rep(), bpb: rep(), bpc: rep() });
  assert.equal(producerGateView(EV, all).ready, true);
  const cases = [
    ['missing', { bpc: null }, /no beacon report/],
    ['old agent', { bpc: rep({ agent_version: '0.5.0-rc.32' }) }, /older than/],
    ['lock off', { bpc: rep({ checks: [{ name: 'admission_lock', ok: false }, { name: 'admission_lock_probe', ok: true }] }) }, /admission_lock is not green/],
    ['probe red', { bpc: rep({ checks: [{ name: 'admission_lock', ok: true }, { name: 'admission_lock_probe', ok: false }] }) }, /admission_lock_probe/],
    ['other gate', { bpc: rep({ coord: { producer_gate_sha256: 'ab'.repeat(32) } }) }, /without this producer gate/],
    ['will not start', { bpc: rep({ coord: { arm_ready: false } }) }, /will not start on the arm/],
    ['not accepted', { bpc: rep({ coord: { accepted: false } }) }, /has not accepted/],
  ];
  for (const [name, change, re] of cases) {
    const v = producerGateView(EV, by({ bpa: rep(), bpb: rep(), bpc: rep(), ...change }));
    assert.equal(v.ready, false, name);
    assert.deepEqual(v.missing, ['bpc'], name);
    assert.match(v.members.find((m) => m.producer === 'bpc').why, re, name);
  }
  const stale = { ...all, bpb: [{ report: rep(), silent: false, conflict: false, age_ms: 90_000 }] };
  assert.match(producerGateView(EV, stale).members.find((m) => m.producer === 'bpb').why, /stale/);
  // A schedule mission control sees that is not the bound one blocks too; an unknown one does not (the agents check it).
  assert.equal(producerGateView(EV, all, { live: { ...SCHEDULE, proposed: sched(6, ['bpx']) } }).ready, false);
  assert.equal(producerGateView(EV, all, { live: null }).schedule_known, false);
  assert.equal(producerGateView({ event_id: 'x' }, all), null, 'no gate, no view');
});

test('rc.33 producerGateView (freeze phase): every member ARMED or FROZEN under this gate; a lying report cannot be told apart', () => {
  const all = by({ bpa: armed(), bpb: armed(), bpc: armed() });
  assert.equal(producerGatePhase(EV, true, all), 'freeze');
  assert.equal(producerGateView(EV, all, { phase: 'freeze' }).ready, true);
  const idle = by({ bpa: armed(), bpb: armed(), bpc: rep() });
  assert.match(producerGateView(EV, idle, { phase: 'freeze' }).members.find((m) => m.producer === 'bpc').why, /ceremony state none/);
  const aborted = by({ bpa: armed(), bpb: armed(), bpc: rep({ ceremony: { state: 'ABORTED', evidence: { h: CUT } } }) });
  assert.equal(producerGateView(EV, aborted, { phase: 'freeze' }).ready, false);
  // A member whose agent says ARMED + lock green but whose nodeos is not locked looks exactly like an honest one: the relay
  // (and every agent) can only read what the producer reports. The burn-off audit stays the backstop (DESIGN §2b).
  assert.equal(producerGateView(EV, by({ bpa: armed(), bpb: armed(), bpc: armed() }), { phase: 'freeze' }).ready, true);
  assert.equal(producerGatePhase(EV, true, by({ bpa: rep({ ceremony: { state: 'SNAPSHOTTED', evidence: { h: CUT } } }) })), 'past');
  // Review fix 1: a member still ARMED from an aborted event (same schedule) is not ready for this one.
  const e0 = producerGateView(EV, by({ bpa: armed(), bpb: armedE0(), bpc: armed() }), { phase: 'freeze' });
  assert.deepEqual(e0.missing, ['bpb']);
  // Review fix 3: the window is 45 s.
  const old = producerGateView(EV, { ...all, bpc: [{ report: armed(), silent: false, conflict: false, age_ms: 46_000 }] }, { phase: 'freeze' });
  assert.match(old.members.find((m) => m.producer === 'bpc').why, /older than 45 s/);
  assert.equal(producerGatePhase(EV, false, all), 'arm');
});

test('rc.33 projectReport keeps the gate hash, arm_ready and the ARMED evidence; the probe is a setup check', () => {
  const fx = JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
  const r = projectReport({ ...fx, coord: { event_id: 'ev-g', accepted: true, producer_gate_sha256: GATE.sha256, arm_ready: true },
    ceremony: { ...fx.ceremony, evidence: { h: CUT, producer_gate_sha256: GATE.sha256 } } });
  assert.equal(r.coord.producer_gate_sha256, GATE.sha256);
  assert.equal(r.coord.arm_ready, true);
  assert.equal(r.ceremony.evidence.producer_gate_sha256, GATE.sha256);
  assert.throws(() => projectReport({ ...fx, coord: { producer_gate_sha256: 'nothex' } }), /producer_gate_sha256/);
  assert.ok(SETUP_CHECKS.includes('admission_lock_probe'));
});

// ---- the relay -------------------------------------------------------------------------------------------------
function setup() {
  const dir = mkdtempSync(join(tmpdir(), 'mc-rc33-'));
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
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
const net = async (base) => (await (await fetch(`${base}/api/status`)).json()).networks.find((n) => n.id === 'testnet');
const post = (base, payload) => fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ network: 'testnet', ...payload })) });
const evHash = async (base) => sha((await (await fetch(`${base}/api/coord/testnet`)).json()).event.payload);
const GATE_CHECKS = [{ name: 'admission_lock', ok: true, detail: 'on' }, { name: 'admission_lock_probe', ok: true, detail: 'answers' }];
function reporter(base) {
  const t = Object.fromEntries(BPS.map((p) => [p, Date.now() - 20e3]));
  return async (producer, { coord = {}, ceremony = null, checks = GATE_CHECKS, agent_version = V } = {}) => {
    for (;;) {
      t[producer] += 3100;
      const r = await fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[producer]}` },
        body: JSON.stringify({ ...fixture(), producer, network: 'testnet', role: 'producer', instance_id: 'c'.repeat(32), agent_version,
          ts: new Date(t[producer]).toISOString(), source: { head: CUT - 400, lib: CUT - 700, chain_id: CHAIN },
          checks: [...fixture().checks, ...checks],
          coord: { event_id: 'ev-g', h: CUT, accepted: true, armed: false, producer_gate_sha256: GATE.sha256, arm_ready: true, ...coord },
          ceremony: ceremony ?? { ...fixture().ceremony, state: null, evidence: {}, transitions: [] } }) });
      if (r.status !== 429) return r;
      await new Promise((ok) => setTimeout(ok, 3100));
    }
  };
}
const armedCeremony = { state: 'ARMED', evidence: { h: CUT, producer_gate_sha256: GATE.sha256, agent_version: V }, transitions: [{ state: 'ARMED', ts: '2026-10-19T00:00:00Z' }] };

test('rc.33 relay: the arm is refused (409, not relayed) until every bound producer is ready, and the gaps are named', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  const pub = await post(base, { type: 'event', event_id: 'ev-g', h: CUT, chain_id: CHAIN, roster, quorum: 3, producer_gate: GATE });
  assert.equal(pub.status, 200, await pub.text());
  const send = reporter(base);
  assert.equal((await send('bpa')).status, 200);
  assert.equal((await send('bpb', { agent_version: '0.5.0-rc.32' })).status, 200);
  // bpc (the standby) has no agent at all
  let n = await net(base);
  assert.equal(n.fleet.producer_gate.ready, false);
  assert.deepEqual(n.fleet.producer_gate.missing, ['bpb', 'bpc']);
  assert.ok(n.fleet.warnings.some((w) => /PRODUCER GATE NOT READY \(the arm is refused\): not armed: bpb, bpc/.test(w)), JSON.stringify(n.fleet.warnings));
  const h = await evHash(base);
  let r = await post(base, { type: 'arm', event_id: 'ev-g', event_hash: h, issued_at_ms: Date.now() });
  assert.equal(r.status, 409);
  const body = await r.json();
  assert.match(body.error, /2 of 3 producers not ready.*NOT relayed/);
  assert.deepEqual(body.missing.map((m) => m.producer), ['bpb', 'bpc']);
  assert.match(body.missing[0].why, /older than/);
  assert.equal((await (await fetch(`${base}/api/coord/testnet`)).json()).arm, null, 'nothing relayed');
  n = await net(base);
  assert.ok(n.coordination.history.some((x) => x.type === 'arm_refused' && x.missing.includes('bpc')));
  assert.ok(n.events.some((e) => /arm for ev-g REFUSED/.test(e.text)));
  // bpb upgrades, bpc starts its agent but its lock probe is red: still refused
  assert.equal((await send('bpb')).status, 200);
  assert.equal((await send('bpc', { checks: [GATE_CHECKS[0], { name: 'admission_lock_probe', ok: false, detail: 'no' }] })).status, 200);
  r = await post(base, { type: 'arm', event_id: 'ev-g', event_hash: h, issued_at_ms: Date.now() });
  assert.equal(r.status, 409);
  assert.deepEqual((await r.json()).missing.map((m) => m.producer), ['bpc']);
  // everyone ready: relayed
  assert.equal((await send('bpc')).status, 200);
  n = await net(base);
  assert.equal(n.fleet.producer_gate.ready, true, JSON.stringify(n.fleet.producer_gate));
  r = await post(base, { type: 'arm', event_id: 'ev-g', event_hash: h, issued_at_ms: Date.now() });
  assert.equal(r.status, 200, await r.text());
  assert.ok((await (await fetch(`${base}/api/coord/testnet`)).json()).arm);
  proc.kill('SIGKILL');
});

test('rc.33 relay: after the arm, a member that is not ARMED under the gate (disarmed or aborted) marks the fleet NOT READY', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  assert.equal((await post(base, { type: 'event', event_id: 'ev-g', h: CUT, chain_id: CHAIN, roster, quorum: 3, producer_gate: GATE })).status, 200);
  const send = reporter(base);
  for (const p of BPS) assert.equal((await send(p)).status, 200);
  assert.equal((await post(base, { type: 'arm', event_id: 'ev-g', event_hash: await evHash(base), issued_at_ms: Date.now() })).status, 200);
  for (const p of BPS) assert.equal((await send(p, { coord: { armed: true }, ceremony: armedCeremony })).status, 200);
  let n = await net(base);
  assert.equal(n.fleet.producer_gate.phase, 'freeze');
  assert.equal(n.fleet.producer_gate.ready, true, JSON.stringify(n.fleet.producer_gate));
  // bpb's ceremony aborts (or its operator disarms): every agent will abort before the freeze; the board says so.
  assert.equal((await send('bpb', { coord: { armed: true }, ceremony: { ...armedCeremony, state: 'ABORTED' } })).status, 200);
  n = await net(base);
  assert.equal(n.fleet.producer_gate.ready, false);
  assert.ok(n.fleet.warnings.some((w) => /PRODUCER GATE NOT READY \(agents abort before the freeze\): not armed: bpb/.test(w)), JSON.stringify(n.fleet.warnings));
  assert.deepEqual(n.fleet.producer_gate.missing, ['bpb']);
  // (A stale report, older than 60 s, does not count either: producerGateView unit test above.)
  proc.kill('SIGKILL');
});

test('rc.33 coord.mjs: event --producer-gate reads the source schedule; arm --source-rpc refuses a changed schedule', async () => {
  const { createServer } = await import('node:http');
  const { execFile } = await import('node:child_process');
  let live = SCHEDULE;
  const src = createServer((q, r) => { r.writeHead(200, { 'content-type': 'application/json' }); r.end(JSON.stringify(q.url.endsWith('/get_producer_schedule') ? live : {})); });
  await new Promise((ok) => src.listen(0, '127.0.0.1', ok));
  const rpc = `http://127.0.0.1:${src.address().port}`;
  const dir = setup();
  const COORD = join(HERE, '..', 'coord.mjs');
  const run = (args) => new Promise((ok) => execFile(process.execPath, [COORD, ...args], { cwd: dir }, (e, stdout, stderr) => ok({ code: e ? e.code : 0, stdout, stderr })));
  const kg = await run(['keygen', '--out', join(dir, 'k.pem')]);
  const pub = kg.stdout.trim();
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN,
    rpc: [], coordinators: [pub], static_producers: true }] }));
  const { base, proc } = await start(dir);
  const common = ['--url', base, '--net', 'testnet', '--key', join(dir, 'k.pem')];
  let r = await run(['event', ...common, '--chain-id', CHAIN, '--h', String(CUT), '--roster', 'bpa,bpb,bpc', '--producer-gate', 'yes', '--event-id', 'ev-g']);
  assert.notEqual(r.code, 0, 'no --source-rpc');
  assert.match(r.stderr, /needs --source-rpc/);
  r = await run(['event', ...common, '--chain-id', CHAIN, '--h', String(CUT), '--roster', 'bpa,bpb', '--producer-gate', 'yes', '--source-rpc', rpc,
    '--standby', 'bpc', '--event-id', 'ev-g']);
  assert.notEqual(r.code, 0);
  assert.match(r.stderr, /no --roster member runs their agent: bpc/);
  r = await run(['event', ...common, '--chain-id', CHAIN, '--h', String(CUT), '--roster', 'bpa,bpb,bpc', '--producer-gate', 'yes', '--source-rpc', rpc,
    '--standby', 'bpc', '--event-id', 'ev-g']);
  assert.equal(r.code, 0, r.stderr);
  const ev = JSON.parse(JSON.parse(readFileSync(join(dir, 'ev-g.event.json'), 'utf8')).payload);
  assert.deepEqual(ev.producer_gate, GATE);
  const send = reporter(base);
  for (const p of BPS) assert.equal((await send(p)).status, 200);
  live = { ...SCHEDULE, proposed: sched(6, ['bpa', 'bpz']) };
  r = await run(['arm', ...common, '--event-id', 'ev-g', '--event-file', join(dir, 'ev-g.event.json'), '--source-rpc', rpc]);
  assert.notEqual(r.code, 0);
  assert.match(r.stderr, /bpz \(proposed\).*Refusing to sign/);
  live = SCHEDULE;
  r = await run(['arm', ...common, '--event-id', 'ev-g', '--event-file', join(dir, 'ev-g.event.json'), '--source-rpc', rpc]);
  assert.equal(r.code, 0, r.stderr + r.stdout);
  proc.kill('SIGKILL');
  src.close();
});

test('rc.33 review fix 2: coord.mjs arm reads /api/status itself and refuses a member below min_agent_version even if the relay would relay', async () => {
  const { createServer } = await import('node:http');
  const { execFile } = await import('node:child_process');
  const dir = setup();
  const COORD = join(HERE, '..', 'coord.mjs');
  const run = (args) => new Promise((ok) => execFile(process.execPath, [COORD, ...args], { cwd: dir }, (e, stdout, stderr) => ok({ code: e ? e.code : 0, stdout, stderr })));
  await run(['keygen', '--out', join(dir, 'k.pem')]);
  // A permissive (or compromised, or pre-rc.33) relay: it serves the event and accepts any arm.
  const posts = [];
  let event = null, beacons = {};
  const relay = createServer(async (q, r) => {
    let body = ''; for await (const c of q) body += c;
    r.writeHead(200, { 'content-type': 'application/json' });
    if (q.method === 'POST') { posts.push(JSON.parse(body)); if (!event) event = JSON.parse(body); return r.end(JSON.stringify({ ok: true, event_hash: sha(JSON.parse(body).payload) })); }
    if (q.url.startsWith('/api/coord/')) return r.end(JSON.stringify({ event }));
    return r.end(JSON.stringify({ networks: [{ id: 'testnet', producers: Object.entries(beacons).map(([name, report]) => ({ name, beacons: [{ role: 'producer', age_ms: 1000, silent: false, conflict: false, report }] })) }] }));
  });
  await new Promise((ok) => relay.listen(0, '127.0.0.1', ok));
  const base = `http://127.0.0.1:${relay.address().port}`;
  const src = createServer((q, r) => { r.writeHead(200); r.end(JSON.stringify(SCHEDULE)); });
  await new Promise((ok) => src.listen(0, '127.0.0.1', ok));
  const rpc = `http://127.0.0.1:${src.address().port}`;
  const common = ['--url', base, '--net', 'testnet', '--key', join(dir, 'k.pem')];
  let r = await run(['event', ...common, '--chain-id', CHAIN, '--h', String(CUT), '--roster', 'bpa,bpb,bpc', '--producer-gate', 'yes', '--source-rpc', rpc,
    '--standby', 'bpc', '--event-id', 'ev-g']);
  assert.equal(r.code, 0, r.stderr);
  beacons = { bpa: rep(), bpb: rep({ agent_version: '0.5.0-rc.32' }), bpc: rep() };
  r = await run(['arm', ...common, '--event-id', 'ev-g', '--event-file', join(dir, 'ev-g.event.json')]);
  assert.notEqual(r.code, 0);
  assert.match(r.stderr, /NOT READY.*\n.*not ready: bpb: .*older than 0\.5\.0-rc\.33/);
  assert.equal(posts.length, 1, 'only the event was posted: the arm was never signed or sent');
  beacons.bpb = rep();
  r = await run(['arm', ...common, '--event-id', 'ev-g', '--event-file', join(dir, 'ev-g.event.json')]);
  assert.equal(r.code, 0, r.stderr);
  assert.equal(posts.length, 2);
  assert.equal(JSON.parse(posts[1].payload).type, 'arm');
  relay.close(); src.close();
});
