// node --test control/test/*.test.mjs
// Fleet rehearsal (5 BPs, upstream stack): the agent's fleet gate counts only an event's signed roster, but
// coord.mjs could not publish one, so every coordinated event fell back to "quorum of whoever reports".
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { eventPayload, parseRoster } from '../coordlib.mjs';

const base = { net: 'rehearsal', 'event-id': 'ev-x', 'chain-id': 'ab'.repeat(32), h: '1000', lead: '24', 'cpu-scale': 'none' };

test('an event without fleet flags is unchanged (no roster, no quorum, no cpu scale with none)', () => {
  const p = eventPayload(base, 5);
  assert.deepEqual(p, { v: 1, type: 'event', network: 'rehearsal', event_id: 'ev-x', chain_id: 'ab'.repeat(32), h: 1000, freeze_lead_blocks: 24, issued_at_ms: 5 });
});

test('--roster/--quorum/--release-sha256 land in the signed payload in the shape src/coord.rs reads', () => {
  const p = eventPayload({ ...base, roster: `bp1,bp2,bp3:${'C'.repeat(32)}`, quorum: '3', 'release-sha256': 'AA'.repeat(32) }, 5);
  assert.deepEqual(p.roster, [{ producer: 'bp1' }, { producer: 'bp2' }, { producer: 'bp3', instance_id: 'c'.repeat(32) }]);
  assert.equal(p.quorum, 3);
  assert.equal(p.release_sha256, 'aa'.repeat(32));
  assert.equal(p.snapshot_sha256, undefined);
});

test('a roster may not count a member twice', () => {
  assert.throws(() => parseRoster('bp1,bp2,bp1'), /more than once/);
  const [i1, i2] = ['a'.repeat(32), 'b'.repeat(32)];
  assert.throws(() => parseRoster(`bp1,bp1:${i1}`), /more than once/);
  assert.deepEqual(parseRoster(`bp1:${i1},bp1:${i2}`), [{ producer: 'bp1', instance_id: i1 }, { producer: 'bp1', instance_id: i2 }]);
  assert.throws(() => parseRoster('bp1:inst-3'), /32-hex/);
});

test('a quorum must be reachable and needs a roster; hashes must be sha256 hex', () => {
  assert.throws(() => eventPayload({ ...base, quorum: '2' }), /needs --roster/);
  assert.throws(() => eventPayload({ ...base, roster: 'bp1,bp2', quorum: '3' }), /not within 1..=2/);
  assert.throws(() => eventPayload({ ...base, roster: 'bp1,bp2', quorum: '0' }), /not within/);
  assert.throws(() => eventPayload({ ...base, 'release-sha256': 'xyz' }), /64 hex/);
  assert.throws(() => parseRoster('BP1'), /not an account name/);
});

test('rc.23: --metal-network-id pins a (private) Metal network in the signed event', () => {
  assert.equal(eventPayload({ ...base, 'metal-network-id': '12345' }, 5).metal_network_id, 12345);
  assert.equal(eventPayload(base, 5).metal_network_id, undefined);
  assert.throws(() => eventPayload({ ...base, 'metal-network-id': 'tahoe' }), /positive integer/);
});
