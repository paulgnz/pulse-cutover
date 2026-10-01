// node --test control/test/*.test.mjs
// Stage-2 rehearsal: after LIVE the new chain stopped including transactions within seconds while mission
// control kept showing LIVE. The agent now reports a post-LIVE HEALTH check, `target_live`; it must count as
// health here (red), never as a setup step.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SETUP_CHECKS, hasFailingHealth, projectReport } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const failing = { name: 'target_live', ok: false, detail: 'no new block for 88 s (head 408462226; LIVE 90 s ago)' };

test('target_live is a health check: not in check-kinds.json, labeled health on the dashboard', () => {
  assert.ok(!SETUP_CHECKS.includes('target_live'));
  const html = readFileSync(join(HERE, '..', 'public', 'index.html'), 'utf8');
  assert.match(html, /^\s*target_live: \['health', '[^']+'\]/m);
});

test('a failing target_live makes the report unhealthy; a passing one does not', () => {
  assert.equal(hasFailingHealth({ checks: [failing] }), true);
  assert.equal(hasFailingHealth({ checks: [{ ...failing, ok: true, detail: 'producing · head 150 · last block 5 s ago' }] }), false);
});

test('the relay keeps the target_live check and its detail on a LIVE report', () => {
  const r = JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
  r.ceremony = { ...(r.ceremony || {}), state: 'LIVE' };
  r.checks = [...(r.checks || []), failing];
  const c = projectReport(r).checks.find((x) => x.name === 'target_live');
  assert.equal(c.ok, false);
  assert.match(c.detail, /no new block for 88 s/);
});
