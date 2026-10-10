// node --test control/test/*.test.mjs
// rc.25 F4 (rc.24 fleet run c1): with quorum = N the verdict flapped LIVE↔DEGRADED whenever one beacon's target read
// timed out (head null, and the beacon then dropped its block after the cut too). The beacon now keeps the cached
// block after the cut and reports how long the head has been unreadable; a short gap keeps the member in the LIVE
// group, a long one drops it.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fleetVerdict, UNREAD_GRACE_MS } from '../lib.mjs';

const H = 1000, BPS = ['bp1', 'bp2', 'bp3', 'bp4', 'bp5'], FIRST = 'a'.repeat(64);
const ev = { event_id: 'e1', h: H, roster: BPS.map((producer) => ({ producer })), quorum: 5 };
const live = (target) => ({ state: 'LIVE', ignition_started: true, evidence: { h: H, cut_height: H },
  target: { blockchain_id: 'X'.repeat(30), head: H + 20, head_id: null, after_cut_id: FIRST, ...target } });
const by = (last) => Object.fromEntries(BPS.map((p, i) => [p, [{ report: { coord: { event_id: 'e1' },
  ceremony: i === 4 ? last : live({}) }, silent: false, conflict: false }]]));

test('one timed-out target read keeps a LIVE quorum = N verdict LIVE', () => {
  assert.equal(fleetVerdict(ev, by(live({}))).verdict, 'LIVE');
  assert.equal(fleetVerdict(ev, by(live({ head: null, unread_for_ms: 4000 }))).verdict, 'LIVE');
});

test('a target unreadable past the grace drops out (DEGRADED), and an older beacon without a block after the cut too', () => {
  const v = fleetVerdict(ev, by(live({ head: null, unread_for_ms: UNREAD_GRACE_MS + 1 })));
  assert.equal(v.verdict, 'DEGRADED');
  assert.ok(v.not_live.some((s) => s.startsWith('bp5')));
  assert.equal(fleetVerdict(ev, by(live({ head: null, after_cut_id: null }))).verdict, 'DEGRADED');
  assert.equal(fleetVerdict(ev, by(live({ head: null }))).verdict, 'DEGRADED', 'no head and no read age: not counted (as the agent)');
});

test('rc.26: an all-sealed fleet (STRANDED / ABORTED, nobody past creation) reads STRANDED, not PENDING', () => {
  const sealed = (st) => ({ state: st, evidence: { h: H } });
  const byS = (states) => Object.fromEntries(BPS.map((p, i) => [p, [{ report: { coord: { event_id: 'e1' }, ceremony: sealed(states[i]) }, silent: false, conflict: false }]]));
  assert.equal(fleetVerdict(ev, byS(['STRANDED', 'STRANDED', 'STRANDED', 'STRANDED', 'STRANDED'])).verdict, 'STRANDED');
  assert.equal(fleetVerdict(ev, byS(['STRANDED', 'ABORTED', 'ABORTED', 'ABORTED', 'ABORTED'])).verdict, 'STRANDED');
  assert.equal(fleetVerdict(ev, byS(['ABORTED', 'ABORTED', 'ABORTED', 'ABORTED', 'ABORTED'])).verdict, 'ABORTED');
  assert.equal(fleetVerdict(ev, byS(['STRANDED', 'FROZEN', 'ABORTED', 'ABORTED', 'ABORTED'])).verdict, 'PENDING', 'someone still in the ceremony');
});

test('rc.26 review M3: STRANDED needs every member present and fresh; otherwise PENDING listing who is unaccounted for', () => {
  const sealed = { state: 'STRANDED', evidence: { h: H } };
  const byP = Object.fromEntries(BPS.slice(0, 4).map((p) => [p, [{ report: { coord: { event_id: 'e1' }, ceremony: sealed }, silent: false, conflict: false }]]));
  const v = fleetVerdict(ev, byP);
  assert.equal(v.verdict, 'PENDING');
  assert.ok(v.not_live.includes('bp5: no report'), JSON.stringify(v.not_live));
  byP.bp5 = [{ report: { coord: { event_id: 'e1' }, ceremony: sealed }, silent: true, conflict: false }];
  assert.equal(fleetVerdict(ev, byP).verdict, 'PENDING', 'a silent member is not proof');
  byP.bp5[0].silent = false;
  assert.equal(fleetVerdict(ev, byP).verdict, 'STRANDED');
});

test('rc.27: STALLED when a quorum of LIVE members report target_live failing; clears when it passes', () => {
  const by2 = (failing, extra = {}) => Object.fromEntries(BPS.map((p, i) => [p, [{ report: { coord: { event_id: 'e1' }, ceremony: live(extra[p] || {}),
    checks: [{ name: 'target_live', ok: i >= failing, detail: i < failing ? 'no new block for 300 s; probe failed: tx not included' : 'ok' }] }, silent: false, conflict: false }]]));
  const v = fleetVerdict(ev, by2(5));
  assert.equal(v.verdict, 'STALLED');
  assert.ok(v.warnings.some((w) => w.startsWith('target chain stalled')));
  assert.equal(fleetVerdict(ev, by2(4)).verdict, 'LIVE', 'below quorum (5): still LIVE');
  assert.equal(fleetVerdict(ev, by2(0)).verdict, 'LIVE');
  const local = Object.fromEntries(BPS.map((p) => [p, [{ report: { coord: { event_id: 'e1' }, ceremony: live({}),
    checks: [{ name: 'target_live', ok: false, detail: 'skipped (collection time budget exhausted)' }] }, silent: false, conflict: false }]]));
  assert.equal(fleetVerdict(ev, local).verdict, 'LIVE', 'a beacon-local skip is not a stalled chain');
  const probeOnly = Object.fromEntries(BPS.map((p) => [p, [{ report: { coord: { event_id: 'e1' }, ceremony: live({}),
    checks: [{ name: 'target_live', ok: false, detail: 'producing · head 408832470 · last block 0 s ago · probe failed: not included within 3500 ms' }] }, silent: false, conflict: false }]]));
  assert.equal(fleetVerdict(ev, probeOnly).verdict, 'LIVE', 'fleet run g1: a probe failure on a producing chain is not STALLED');
  const backwards = Object.fromEntries(BPS.map((p) => [p, [{ report: { coord: { event_id: 'e1' }, ceremony: live({}),
    checks: [{ name: 'target_live', ok: false, detail: 'head went BACKWARDS: 121 after 122 (rollback, re-import or another chain)' }] }, silent: false, conflict: false }]]));
  assert.equal(fleetVerdict(ev, backwards).verdict, 'STALLED', 'a head going backwards is a chain symptom');
});

test('rc.27: different protocol upgrade schedules or an unsupported next version are warned about', () => {
  const A = 'a1'.repeat(32), B = 'b2'.repeat(32);
  const byS = (hashOf, sup = 2) => Object.fromEntries(BPS.map((p) => [p, [{ report: { coord: { event_id: 'e1' },
    ceremony: live({ protocol_upgrade_schedule_hash: hashOf(p), supported_protocol_version: p === 'bp5' ? sup : 2, next_protocol_upgrade: { protocol_version: 2, activation_height: 5000 } }) }, silent: false, conflict: false }]]));
  const same = fleetVerdict(ev, byS(() => A));
  assert.equal(same.verdict, 'LIVE');
  assert.deepEqual(same.warnings, []);
  const split = fleetVerdict(ev, byS((p) => (p === 'bp2' ? B : A)));
  assert.ok(split.warnings.some((w) => w.includes('different protocol upgrade schedules') && w.includes('bp2')), JSON.stringify(split.warnings));
  const old = fleetVerdict(ev, byS(() => A, 1));
  assert.ok(old.warnings.some((w) => w.startsWith("bp5's PulseVM supports protocol 1")));
});

test('rc.27 review: a past-creation report for another H (any instance) is warned about and blocks a sealed verdict', () => {
  const sealed = { state: 'ABORTED', evidence: { h: H }, source_resumed: true };
  const foreign = { state: 'LIVE', ignition_started: true, evidence: { h: H + 999 } };
  const byP = Object.fromEntries(BPS.map((p) => [p, [{ report: { coord: { event_id: 'e1' }, ceremony: sealed }, silent: false, conflict: false }]]));
  assert.equal(fleetVerdict(ev, byP).verdict, 'ABORTED');
  byP.bp2 = [{ report: { coord: { event_id: 'e1' }, ceremony: foreign }, silent: false, conflict: false }, ...byP.bp2];
  const v = fleetVerdict(ev, byP);
  assert.notEqual(v.verdict, 'ABORTED');
  assert.ok(v.warnings.some((w) => w.startsWith('bp2 has creation evidence for ANOTHER H')), JSON.stringify(v.warnings));
});
