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
