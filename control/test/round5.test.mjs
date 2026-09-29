// node --test control/test/*.test.mjs
// Round 5 (Fable re-check of rc.7, N8/N9): mission control must judge health the way the agent's fleet gate
// does (one shared setup-check list), and an ABORTED must say what actually happened to the old chain.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { SETUP_CHECKS, hasFailingHealth, abortKind, projectReport } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const shared = JSON.parse(readFileSync(join(HERE, '..', 'check-kinds.json'), 'utf8')).setup;
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));

test('N9 lib SETUP_CHECKS is the shared check-kinds.json list', () => {
  assert.deepEqual([...SETUP_CHECKS].sort(), [...shared].sort());
});

test('N9 the dashboard CHECKS map marks exactly the shared list as setup', () => {
  const html = readFileSync(join(HERE, '..', 'public', 'index.html'), 'utf8');
  const setup = [...html.matchAll(/^\s*([a-z_]+): \['setup'/gm)].map((m) => m[1]).sort();
  assert.deepEqual(setup, [...shared].sort());
});

test('N9 hasFailingHealth: failing setup checks are ignored, anything else (incl. unknown names) is health', () => {
  assert.equal(hasFailingHealth({ checks: [{ name: 'hook_on_live', ok: false }] }), false);
  assert.equal(hasFailingHealth({ checks: [{ name: 'disk_free', ok: false }] }), true);
  assert.equal(hasFailingHealth({ checks: [{ name: 'something_new', ok: false }] }), true);
  assert.equal(hasFailingHealth({ checks: [] }), false);
});

test('N8 abortKind says what happened to the old chain', () => {
  assert.equal(abortKind({ state: 'LIVE' }), null);
  assert.equal(abortKind({ state: 'ABORTED', rollback_complete: true, ignition_started: false }), 'resumed');
  assert.equal(abortKind({ state: 'ABORTED', rollback_complete: false }), 'incomplete');
  assert.equal(abortKind({ state: 'ABORTED', forced_rollback: true, ignition_started: true, rollback_complete: true }), 'forced');
  assert.equal(abortKind({ state: 'ABORTED' }), 'unknown');
});

test('N8 the projection keeps rollback_complete (bool or null)', () => {
  const r = fixture();
  r.ceremony = { ...(r.ceremony || {}), state: 'ABORTED', rollback_complete: false };
  assert.equal(projectReport(r).ceremony.rollback_complete, false);
  assert.equal(projectReport(fixture()).ceremony.rollback_complete ?? null, null);
  const bad = fixture();
  bad.ceremony = { ...(bad.ceremony || {}), rollback_complete: 'yes' };
  assert.throws(() => projectReport(bad));
});

// --- relay: agreement applies the same health rule as the agent's fleet gate ---------------------------------
const CHAIN = '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd';
const sha = (s) => createHash('sha256').update(s).digest('hex');
const TOKS = ['1'.repeat(64), '2'.repeat(64)];
const procs = [];
after(() => procs.forEach((p) => p.kill('SIGKILL')));
function start() {
  const dir = mkdtempSync(join(tmpdir(), 'mc-r5-'));
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN, rpc: [],
    coordinators: [], geo: { bp1: { name: 'BP 1' }, bp2: { name: 'BP 2' } } }] }));
  writeFileSync(join(dir, 'tokens.json'), JSON.stringify({ [sha(TOKS[0])]: { network: 'testnet', producer: 'bp1' }, [sha(TOKS[1])]: { network: 'testnet', producer: 'bp2' } }));
  mkdirSync(join(dir, 'state'));
  const proc = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', MC_OFFLINE: '1',
    NETWORKS: join(dir, 'networks.json'), TOKENS: join(dir, 'tokens.json'), COORD_FILE: join(dir, 'state', 'coord.json') } });
  procs.push(proc);
  return new Promise((ok, ko) => {
    let out = '';
    proc.stdout.on('data', (d) => { out += d; const m = out.match(/on 127\.0\.0\.1:(\d+)/); if (m) ok(`http://127.0.0.1:${m[1]}`); });
    proc.stderr.on('data', (d) => { out += d; });
    setTimeout(() => ko(new Error('server did not start: ' + out)), 8000);
  });
}

test('N9 agreement excludes a report with a failing HEALTH check (as the agent gate does)', async () => {
  const base = await start();
  const ev = { snapshot_sha256: 'a'.repeat(64), cut_height: 100 };
  const rep = (producer, checks, inst) => ({ ...fixture(), producer, network: 'testnet', role: 'producer', instance_id: inst.repeat(32),
    ts: new Date().toISOString(), checks, ceremony: { state: 'VERIFIED', transitions: [], evidence: ev } });
  const post = (tok, body) => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${tok}` }, body: JSON.stringify(body) });
  assert.equal((await post(TOKS[0], rep('bp1', [{ name: 'disk_free', ok: false, detail: '2 GB free' }], 'a'))).status, 200);
  assert.equal((await post(TOKS[1], rep('bp2', [{ name: 'hook_on_live', ok: false, detail: 'not configured' }], 'b'))).status, 200);
  const net = (await (await fetch(`${base}/api/status`)).json()).networks[0];
  const row = net.agreement.find((r) => r.key === 'snapshot_sha256');
  assert.ok(row, 'snapshot row present');
  assert.deepEqual(row.unhealthy, ['bp1'], 'bp1 (failing disk_free) is excluded and named');
  assert.equal(row.agree, false, 'a roster member excluded for health means no agreement');
  assert.equal(row.reporting, 1, 'only bp2 (a failing SETUP check) counts');
});
