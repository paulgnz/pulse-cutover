// node --test control/test/*.test.mjs
// r30 shadow cutovers (docs/DESIGN-shadow.md): the relay keeps the shadow label and refuses a shadow report on mainnet.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { projectReport, hasFailingHealth } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));

test('r30 the shadow label survives projection; null outside a shadow', () => {
  const r = fixture();
  assert.equal(projectReport(r).shadow, null);
  r.network = 'rehearsal';
  r.shadow = { label: 'shadow-test-a', metal_network_id: 4242 };
  r.checks = [...r.checks, { name: 'shadow', ok: false, detail: 'SHADOW ceremony shadow-test-a' }];
  const out = projectReport(r);
  assert.deepEqual(out.shadow, { label: 'shadow-test-a', metal_network_id: 4242 });
  assert.equal(hasFailingHealth(out), hasFailingHealth({ ...out, checks: out.checks.filter((c) => c.name !== 'shadow') }),
    'the shadow check is a setup check: it never changes the health verdict');
});

test('r30 a shadow report on the mainnet network is refused', () => {
  const r = fixture();
  r.network = 'mainnet';
  r.shadow = { label: 'x', metal_network_id: 4242 };
  assert.throws(() => projectReport(r), /SHADOW ceremony never reports on the mainnet network/);
  r.shadow = 'yes';
  r.network = 'rehearsal';
  assert.throws(() => projectReport(r), /shadow/);
});

test('r30 shadow-fix: a shadow report lands only on a shadow board, judged by its chain_id too', async () => {
  const { shadowBoardProblem } = await import('../lib.mjs');
  const MAIN = '384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0';
  const nets = [{ id: 'mainnet', chain_id: MAIN }, { id: 'xpr-main', chain_id: MAIN }, { id: 'shadow-sneaky', chain_id: MAIN },
    { id: 'rehearsal', chain_id: 'aa'.repeat(32) }, { id: 'shadow-a', chain_id: null }, { id: 'shadow-b' }];
  const rep = (network) => ({ network, shadow: { label: 'x', metal_network_id: 4242 } });
  assert.match(shadowBoardProblem(rep('mainnet'), nets), /XPR MAINNET chain_id/);
  assert.match(shadowBoardProblem(rep('xpr-main'), nets), /XPR MAINNET chain_id/, 'a mainnet board under another name');
  assert.match(shadowBoardProblem(rep('shadow-sneaky'), nets), /XPR MAINNET chain_id/, 'a "shadow" name on a mainnet board');
  assert.match(shadowBoardProblem(rep('rehearsal'), nets), /not a shadow board/);
  assert.match(shadowBoardProblem(rep('nowhere'), nets), /configured shadow board/);
  assert.equal(shadowBoardProblem(rep('shadow-a'), nets), null);
  assert.equal(shadowBoardProblem(rep('shadow-b'), nets), null);
  assert.equal(shadowBoardProblem({ network: 'mainnet', shadow: null }, nets), null, 'a normal report is not judged here');
});
