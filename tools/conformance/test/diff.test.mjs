// node --test tools/conformance/test
// diff.mjs end to end against two in-process mock endpoints (no network): the sample corpus, run-time request
// resolution (head-3, failed-transaction templates), the report, the exit code and the allow-list.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const DIFF = join(HERE, '..', 'diff.mjs');
const CORPUS = join(HERE, 'fixtures', 'sample-corpus.jsonl');
const T = mkdtempSync(join(tmpdir(), 'conformance-'));
const LIB_ID = '000003de' + '11223344' + 'ab'.repeat(24);

// A Leap-like node. flavour 'edge' answers get_code with the edge's 501; 'drift' also returns a different balance.
function node(flavour, seen) {
  return http.createServer((req, res) => {
    let body = ''; req.on('data', (c) => { body += c; });
    req.on('end', () => {
      seen.push({ method: req.method, path: req.url, body });
      const json = (s, o, h = {}) => { res.writeHead(s, { 'content-type': 'application/json', ...h }); res.end(JSON.stringify(o)); };
      if (req.method === 'OPTIONS') { res.writeHead(flavour === 'leap' ? 200 : 204, { 'access-control-allow-origin': '*', 'access-control-allow-headers': flavour === 'leap' ? 'Content-Type, Origin' : 'content-type' }); return res.end(); }
      const p = req.url;
      if (p === '/v1/chain/get_info') return json(200, { chain_id: 'c'.repeat(64), head_block_num: flavour === 'leap' ? 1000 : 1001, last_irreversible_block_num: 990, last_irreversible_block_id: LIB_ID, head_block_time: '2026-10-01T00:00:00.000', server_version: flavour });
      if (p === '/v1/chain/get_account') {
        const q = JSON.parse(body);
        if (q.account_name !== 'alice') return json(500, { code: 500, message: 'Internal Service Error', error: { code: 0, name: 'exception', what: 'unspecified', details: [{ message: `unknown key (eosio::chain::name): ${q.account_name}`, file: flavour + '.cpp', line_number: 1, method: 'x' }] } });
        return json(200, { account_name: 'alice', head_block_num: flavour === 'leap' ? 1000 : 1001, ram_usage: flavour === 'leap' ? 3000 : 2990, permissions: [] });
      }
      if (p === '/v1/chain/get_table_rows') return json(200, { rows: [{ balance: flavour === 'drift' ? '9.0000 XPR' : '1.0000 XPR' }], more: false, next_key: '' });
      if (p === '/v1/chain/get_code') {
        if (flavour === 'leap') return json(200, { account_name: 'eosio.token', code_hash: 'aa', wasm: '0061736d', abi: {} });
        return json(501, { code: 501, message: 'get_code not available on PulseVM yet (upstream)', error: { code: 0, name: 'unsupported_feature', what: 'x', details: [] } }, { 'x-pulse-edge': 'unavailable' });
      }
      if (p === '/v1/chain/get_block_info') { const q = JSON.parse(body); return json(200, { block_num: q.block_num, id: 'ff' }); }
      if (p === '/v1/chain/push_transaction') return json(500, { code: 500, message: 'Internal Service Error', error: { code: 3090003, name: 'unsatisfied_authorization', what: 'Provided keys, permissions, and delays do not satisfy declared authorizations', details: [{ message: 'transaction declares authority', file: 'authorization_manager.cpp', line_number: flavour === 'leap' ? 10 : 20, method: 'check' }] } });
      return json(404, { code: 404, message: 'Not Found', error: { code: 0, name: 'exception', what: 'unspecified', details: [{ message: 'Unknown Endpoint' }] } });
    });
  });
}
const servers = {}, seen = { leap: [], edge: [], drift: [] };
before(async () => {
  for (const f of ['leap', 'edge', 'drift']) { servers[f] = node(f, seen[f]); await new Promise((r) => servers[f].listen(0, '127.0.0.1', r)); }
});
after(() => { for (const s of Object.values(servers)) s.close(); });
const url = (f) => `http://127.0.0.1:${servers[f].address().port}`;
function run(args) {
  return new Promise((resolve) => {
    const p = spawn(process.execPath, [DIFF, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = ''; p.stdout.on('data', (d) => { out += d; }); p.stderr.on('data', (d) => { err += d; });
    p.on('close', (code) => resolve({ code, out, err }));
  });
}

test('a conforming B passes: normalization, documented 501, CORS semantics, failed-tx error shape', async () => {
  const j = join(T, 'ok.json'), md = join(T, 'ok.md');
  const r = await run(['--a', url('leap'), '--b', url('edge'), '--corpus', CORPUS, '--rps', '100', '--json', j, '--report', md]);
  assert.equal(r.code, 0, r.out + r.err);
  const out = JSON.parse(readFileSync(j, 'utf8'));
  const cls = Object.fromEntries(out.results.map((x) => [x.id, x.cls]));
  assert.equal(cls['get_info#001-post-empty-object'], 'equal-after-normalization');
  assert.equal(cls['get_account#001-name'], 'equal-after-normalization', 'ram_usage/head are volatile');
  assert.equal(cls['get_account#002-err-unknown-account'], 'equal-after-normalization', 'file/line differ, error shape does not');
  assert.equal(cls['get_code#001-code_as_wasm-true'], 'B-501-expected');
  assert.equal(cls['push_transaction#001-failed-missing-signature'], 'equal-after-normalization');
  assert.equal(cls['get_info#002-cors-preflight'], 'equal-after-normalization');
  const report = readFileSync(md, 'utf8');
  assert.match(report, /no unexpected differences/);
  assert.match(report, /\| traffic \|/);
  assert.match(report, /\| `get_info` \| medium \|/);
});

test('run-time resolution: head-3 from A, one unsigned transaction shared by A and B', async () => {
  const bi = (s) => s.filter((x) => x.path === '/v1/chain/get_block_info').map((x) => JSON.parse(x.body).block_num);
  assert.deepEqual(bi(seen.leap), [997]);
  assert.deepEqual(bi(seen.edge), [997], 'B is asked for the same block as A');
  const tx = (s) => s.filter((x) => x.path === '/v1/chain/push_transaction').map((x) => JSON.parse(x.body));
  assert.equal(tx(seen.leap)[0].packed_trx, tx(seen.edge)[0].packed_trx);
  assert.deepEqual(tx(seen.leap)[0].signatures, []);
  const exp = Buffer.from(tx(seen.leap)[0].packed_trx, 'hex').readUInt32LE(0);
  assert.ok(Math.abs(exp - (Date.now() / 1000 + 1800)) < 120, 'expiration is live, not frozen in the corpus');
});

test('a value difference fails the run (exit 1) and is shown with its path; an allow-list entry excuses it', async () => {
  const j = join(T, 'bad.json'), md = join(T, 'bad.md');
  const r = await run(['--a', url('leap'), '--b', url('drift'), '--corpus', CORPUS, '--rps', '100', '--retries', '0', '--json', j, '--report', md]);
  assert.equal(r.code, 1);
  const out = JSON.parse(readFileSync(j, 'utf8'));
  const row = out.results.find((x) => x.id === 'get_table_rows#001-json-true');
  assert.equal(row.cls, 'DIFF');
  assert.equal(row.diff[0].path, 'rows[0].balance');
  assert.match(readFileSync(md, 'utf8'), /rows\[0\]\.balance/);
  const allow = join(T, 'allow.json');
  writeFileSync(allow, JSON.stringify([{ endpoint: 'get_table_rows', class: 'DIFF', reason: 'test' }]));
  const r2 = await run(['--a', url('leap'), '--b', url('drift'), '--corpus', CORPUS, '--rps', '100', '--retries', '0', '--allow', allow]);
  assert.equal(r2.code, 0, r2.out);
});

test('frozen mode applies no retries and still passes a conforming B', async () => {
  const r = await run(['--a', url('leap'), '--b', url('edge'), '--corpus', CORPUS, '--rps', '100', '--frozen']);
  assert.equal(r.code, 0, r.out + r.err);
});

test('usage error exits 2', async () => {
  assert.equal((await run(['--a', url('leap')])).code, 2);
});
