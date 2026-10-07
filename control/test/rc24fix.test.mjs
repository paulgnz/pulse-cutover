// node --test control/test/*.test.mjs
// rc.24 fleet rehearsal (2026-10-07): a beacon restarted on a reused run directory paired the relay's current
// (completed) event id with an OLD journal that had ABORTED and resumed the source. Mission control read that as
// "resumed the old chain after peers ignited" for every member and latched a false SPLIT on the completed event.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fleetVerdict, ceremonyFor, nextEventMark } from '../lib.mjs';

const H = 408462434, OLD_H = 408463830, BPS = ['bp1', 'bp2', 'bp3', 'bp4', 'bp5'];
const ev = { event_id: 'fleet-r6', h: H, roster: BPS.map((producer) => ({ producer })), quorum: 5 };
const staleAborted = { state: 'ABORTED', evidence: { h: OLD_H, chain_id: 'x' }, source_resumed: true, rollback_complete: true };
const by = (ce) => Object.fromEntries(BPS.map((p) => [p, [{ report: { coord: { event_id: 'fleet-r6' }, ceremony: ce, source: { head: H + 377 } }, silent: false, conflict: false }]]));
const marksPastLive = Object.fromEntries(BPS.map((p) => [p, { past_create: true, state: 'LIVE', rank: 7, src_first: H + 377, src_max: H + 377 }]));

test('a journal armed for another H is not this event\'s evidence (no false SPLIT)', () => {
  assert.equal(ceremonyFor({ ceremony: staleAborted }, ev), null);
  assert.equal(ceremonyFor({ ceremony: { ...staleAborted, evidence: { h: H } } }, ev).state, 'ABORTED');
  assert.equal(ceremonyFor({ ceremony: { state: 'VERIFIED', evidence: { cut_height: OLD_H } } }, ev), null, 'cut_height when h is absent');
  assert.equal(ceremonyFor({ ceremony: { state: 'ARMED', evidence: {} } }, ev).state, 'ARMED', 'no height: kept');
  assert.equal(ceremonyFor({ ceremony: staleAborted }, {}).state, 'ABORTED', 'no event H known: kept');
  const v = fleetVerdict(ev, by(staleAborted), marksPastLive);
  assert.notEqual(v.verdict, 'SPLIT', JSON.stringify(v.alarms));
  assert.ok(v.members.every((m) => m.resumed === false && m.state === null));
});

test('the same ABORTED journal for THIS event still raises the split (positive control unchanged)', () => {
  const real = { ...staleAborted, evidence: { h: H } };
  const v = fleetVerdict(ev, by(real), marksPastLive);
  assert.equal(v.verdict, 'SPLIT');
  assert.ok(v.alarms.some((a) => a.includes('resumed the old chain after peers ignited')));
});

test('nextEventMark is never fed a foreign journal by the relay (server.js uses ceremonyFor first)', () => {
  // What server.js does: ce = ceremonyFor(report, currentEvent); no ce, no mark change.
  const ce = ceremonyFor({ ceremony: { state: 'LIVE', ignition_started: true, evidence: { h: OLD_H } } }, ev);
  assert.equal(ce, null);
  assert.ok(nextEventMark(undefined, { state: 'LIVE', evidence: { h: H } }, null, false, 1).past_create);
});
