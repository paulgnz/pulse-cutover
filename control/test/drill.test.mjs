// node --test control/test/*.test.mjs
// r30: readiness drills. A beacon reports its latest drill as `drill`, never as the ceremony; mission control
// validates it, compares producers per drill height, and keeps it out of every ceremony decision.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { projectReport, projectDrill, drillComparison, fleetVerdict } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
const ROOT = 'ab'.repeat(32), OTHER = 'cd'.repeat(32), BID = `0185699b${'ab'.repeat(28)}`;
const drill = (over = {}) => ({
  kind: 'readiness_drill', h: 408328634, state: 'DONE', stage: 'done', block_id_at_h: BID, snapshot_sha256: 'ee'.repeat(32),
  state_root: '42b0a70fa12998bd', fingerprints_digest: ROOT, compare: 'MISMATCH',
  compare_failing_tables: ['contract_index_double', 'global_property', 'generated_transaction'],
  timings_ms: { export: 168900, import: 191600, compare: 140400, fingerprint: 71600, conversion_total: 572500 },
  error: null, finished_ts_ms: 1791900000000, ...over,
});

test('r30: a drill is projected into its own field and does not touch the ceremony', () => {
  const r = projectReport({ ...fixture(), producer: 'bpa', network: 'mainnet', drill: drill() });
  assert.equal(r.drill.kind, 'readiness_drill');
  assert.equal(r.drill.compare, 'MISMATCH');
  assert.deepEqual(r.drill.compare_failing_tables, ['contract_index_double', 'global_property', 'generated_transaction']);
  assert.equal(r.drill.timings_ms.conversion_total, 572500);
  assert.deepEqual(r.ceremony, projectReport({ ...fixture(), producer: 'bpa', network: 'mainnet' }).ceremony);
  // Absent on an older beacon.
  assert.equal(projectReport({ ...fixture(), producer: 'bpa', network: 'mainnet' }).drill, null);
});

test('r30: a malformed drill is refused, free text is redacted', () => {
  assert.throws(() => projectDrill({ ...drill(), kind: 'ceremony' }), /drill.kind/);
  assert.throws(() => projectDrill({ ...drill(), state: 'LIVE' }), /drill.state/);
  assert.throws(() => projectDrill({ ...drill(), state_root: '<script>' }), /drill.state_root/);
  assert.throws(() => projectDrill({ ...drill(), compare_failing_tables: ['DROP TABLE'] }), /compare_failing_tables/);
  assert.throws(() => projectDrill({ ...drill(), timings_ms: { 'rm -rf': 1 } }), /timings_ms/);
  const e = projectDrill({ ...drill(), state: 'FAILED', error: 'export failed at /root/secret/path http://10.0.0.1:8888' });
  assert.ok(!e.error.includes('10.0.0.1'));
});

test('r30: drills are compared per height; disagreement and failures are visible', () => {
  const agree = drillComparison([
    { producer: 'bpa', report: { drill: drill() } },
    { producer: 'bpb', report: { drill: drill() } },
    { producer: 'bpc', report: { drill: drill({ state: 'RUNNING', fingerprints_digest: null }) } },
    { producer: 'bpd', report: { drill: null } },
  ]);
  assert.equal(agree.length, 1);
  assert.equal(agree[0].agree, true);
  assert.equal(agree[0].done, 2);
  assert.deepEqual(agree[0].running, ['bpc']);

  const split = drillComparison([
    { producer: 'bpa', report: { drill: drill() } },
    { producer: 'bpb', report: { drill: drill({ fingerprints_digest: OTHER }) } },
    { producer: 'bpc', report: { drill: drill({ state: 'FAILED', error: 'oom' }) } },
  ]);
  assert.equal(split[0].agree, false);
  assert.equal(split[0].fingerprints_digest.length, 2);
  assert.deepEqual(split[0].failed, [{ producer: 'bpc', error: 'oom' }]);
});

test('r30: a drill never feeds the fleet verdict', () => {
  const BPS = ['bpa', 'bpb', 'bpc'];
  const ev = { event_id: 'ev-1', h: 100, roster: BPS.map((producer) => ({ producer })), quorum: 2 };
  const base = (extra) => Object.fromEntries(BPS.map((p) => [p, [{ report: { coord: { event_id: 'ev-1' }, ceremony: { state: 'ARMED', evidence: { h: 100 } }, interval_secs: 10, ...extra }, silent: false, conflict: false }]]));
  const without = fleetVerdict(ev, base({}));
  const withDrill = fleetVerdict(ev, base({ drill: drill({ h: 100 }) }));
  assert.deepEqual(withDrill, without);
});

test('r30 review (d): an invalid drill drops only the drill, never the beacon report', () => {
  const good = projectReport({ ...fixture(), producer: 'bpa', network: 'mainnet' });
  for (const bad of [drill({ h: null }), drill({ kind: 'ceremony' }), drill({ state_root: '<script>' }), 'not-an-object']) {
    const r = projectReport({ ...fixture(), producer: 'bpa', network: 'mainnet', drill: bad });
    assert.equal(r.drill, null);
    assert.match(r.drill_invalid, /drill/);
    assert.deepEqual(r.ceremony, good.ceremony);
    assert.equal(r.producer, good.producer);
  }
});

test('r30 review (e): a DONE drill without complete evidence never counts toward agreement', () => {
  for (const missing of ['fingerprints_digest', 'block_id_at_h', 'snapshot_sha256']) {
    const c = drillComparison([
      { producer: 'bpa', report: { drill: drill() } },
      { producer: 'bpb', report: { drill: drill({ [missing]: null }) } },
    ]);
    assert.equal(c[0].agree, false, missing);
    assert.deepEqual(c[0].incomplete, ['bpb'], missing);
  }
  const ok = drillComparison([
    { producer: 'bpa', report: { drill: drill() } },
    { producer: 'bpb', report: { drill: drill() } },
  ]);
  assert.equal(ok[0].agree, true);
  assert.deepEqual(ok[0].incomplete, []);
});
