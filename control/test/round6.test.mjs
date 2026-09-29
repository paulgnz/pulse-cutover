// node --test control/test/*.test.mjs
// Round 6 (Fable final check of rc.8, N-3): an unfinished operator rollback must be visible centrally.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { projectReport } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));

test('N-3 the projection keeps rollback_pending (bool or null) and rejects non-bools', () => {
  const r = fixture();
  r.ceremony = { ...(r.ceremony || {}), state: 'ARMED', rollback_pending: true };
  assert.equal(projectReport(r).ceremony.rollback_pending, true);
  assert.equal(projectReport(fixture()).ceremony.rollback_pending ?? null, null);
  const bad = fixture();
  bad.ceremony = { ...(bad.ceremony || {}), rollback_pending: 'yes' };
  assert.throws(() => projectReport(bad));
});
