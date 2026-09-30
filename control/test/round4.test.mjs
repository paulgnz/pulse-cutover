// Round 4 (external review of rc.7): the beacon now reports whether ignition started on the box and
// whether its last ABORTED was a forced rollback after ignition. An ABORTED with ignition_started
// must stay distinguishable from a pre-ceremony box on the public dashboard.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { projectReport } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));

test('forced rollback after ignition survives the public projection', () => {
  const r = fixture();
  r.ceremony = { ...(r.ceremony || {}), state: 'ABORTED', ignition_started: true, forced_rollback: true };
  const p = projectReport(r);
  assert.equal(p.ceremony.state, 'ABORTED');
  assert.equal(p.ceremony.ignition_started, true);
  assert.equal(p.ceremony.forced_rollback, true);
});

test('older beacons without the fields project as null, and non-booleans are rejected', () => {
  const old = fixture();
  const p = projectReport(old);
  assert.equal(p.ceremony.ignition_started ?? null, null);
  const bad = fixture();
  bad.ceremony = { ...(bad.ceremony || {}), ignition_started: 'yes' };
  assert.throws(() => projectReport(bad));
});
