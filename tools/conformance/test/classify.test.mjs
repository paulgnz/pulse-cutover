// node --test tools/conformance/test
// The classifier, the docs/V1-COVERAGE.md parser, CORS verdicts, the allow-list and run-time transactions.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { classify, parseCoverage, allowedBy, corsVerdict, buildTx, FAILING, CLASSES } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const coverage = parseCoverage(readFileSync(join(HERE, '../../../docs/V1-COVERAGE.md'), 'utf8'));
const res = (status, body, headers = {}) => ({ status, text: typeof body === 'string' ? body : JSON.stringify(body), json: typeof body === 'string' ? (() => { try { return JSON.parse(body); } catch { return undefined; } })() : body, headers });
const req = (endpoint, extra = {}) => ({ id: `${endpoint}#001-x`, method: 'POST', path: `/v1/chain/${endpoint}`, headers: {}, body: '{}', tags: [endpoint, 'x', 'chain'], volatile: [], ...extra });
const nodeosErr = (status, code, name, what, msg) => ({ code: status, message: 'x', error: { code, name, what, details: [{ message: msg, file: 'f.cpp', line_number: 1, method: 'm' }] } });

test('coverage parser reads every /v1/chain and /v1/history row of docs/V1-COVERAGE.md', () => {
  assert.equal([...coverage.values()].filter((c) => c.group === 'chain').length, 31);
  assert.deepEqual([...coverage].filter(([, c]) => c.group === 'history').map(([e]) => e).sort(), ['get_actions', 'get_controlled_accounts', 'get_key_accounts', 'get_transaction']);
  for (const e of ['get_code', 'compute_transaction', 'send_read_only_transaction', 'get_scheduled_transactions']) assert.ok(coverage.get(e).b501, `${e} is a documented 501`);
  assert.ok(!coverage.get('get_info').b501);
  assert.ok(coverage.get('push_block').b404);
  assert.ok(coverage.get('get_block_header_state').partial && coverage.get('get_producer_schedule').partial && coverage.get('get_raw_code_and_abi').partial);
  assert.ok(coverage.get('get_activated_protocol_features').staticAtCut);
  assert.ok(!coverage.has('get_tokens'), '/v2 rows are not part of the /v1 corpus');
});

test('identical and equal-after-normalization', () => {
  const r = req('get_info', { volatile: ['head_block_num'] });
  assert.equal(classify(r, res(200, { chain_id: 'c', head_block_num: 1 }), res(200, { chain_id: 'c', head_block_num: 1 })).cls, 'identical');
  assert.equal(classify(r, res(200, { chain_id: 'c', head_block_num: 1 }), res(200, { head_block_num: 5, chain_id: 'c' })).cls, 'equal-after-normalization');
});

test('B-501-expected only where the docs say 501 (or the request says so)', () => {
  const b501 = res(501, nodeosErr(501, 0, 'unsupported_feature', 'get_code is not available on PulseVM yet (upstream)', 'x'), { 'x-pulse-edge': 'unavailable' });
  assert.equal(classify(req('get_code'), res(200, { account_name: 'eosio.token', wasm: '00' }), b501, coverage.get('get_code')).cls, 'B-501-expected');
  assert.equal(classify(req('get_account'), res(200, { account_name: 'a' }), b501, coverage.get('get_account')).cls, 'DIFF');
  assert.equal(classify(req('get_transaction_id', { expect_b: '501' }), res(200, 'abc'), b501, coverage.get('get_transaction_id')).cls, 'B-501-expected');
  assert.equal(classify(req('push_block'), res(400, nodeosErr(400, 3200006, 'invalid_http_request', 'x', 'y')), res(404, nodeosErr(404, 0, 'exception', 'unspecified', 'Unknown Endpoint')), coverage.get('push_block')).cls, 'B-501-expected');
});

test('static-at-cut endpoints answering static-missing are a DIFF (the capture step was skipped)', () => {
  const b = res(501, nodeosErr(501, 0, 'unsupported_feature', 'captured file missing', 'x'), { 'x-pulse-edge': 'static-missing' });
  const r = classify(req('get_consensus_parameters'), res(200, { chain_config: {} }), b, coverage.get('get_consensus_parameters'));
  assert.equal(r.cls, 'DIFF');
  assert.match(r.note, /capture-static/);
});

test('documented partial polyfills are B-partial-expected, with the diff kept', () => {
  const a = res(200, { account_name: 'eosio.token', wasm: '0061736d', abi: 'AAA' });
  const b = res(200, { account_name: 'eosio.token', wasm: '', abi: 'AAA' }, { 'x-pulse-edge': 'wasm-unavailable' });
  const r = classify(req('get_raw_code_and_abi'), a, b, coverage.get('get_raw_code_and_abi'));
  assert.equal(r.cls, 'B-partial-expected');
  assert.equal(r.diff[0].path, 'wasm');
  // the same difference without the documented header is a DIFF
  assert.equal(classify(req('get_raw_code_and_abi'), a, res(200, { account_name: 'eosio.token', wasm: '', abi: 'AAA' }), coverage.get('get_raw_code_and_abi')).cls, 'DIFF');
});

test('HTTP status parity: 202 vs 200, 500 vs 400 and 200 vs error are never equal', () => {
  const r = req('push_transactions');
  assert.equal(classify(r, res(202, [{ transaction_id: '00' }]), res(200, [{ transaction_id: '00' }]), coverage.get('push_transactions')).cls, 'DIFF');
  const e = nodeosErr(500, 3090003, 'unsatisfied_authorization', 'Provided keys, permissions, and delays do not satisfy declared authorizations', 'transaction declares authority');
  assert.equal(classify(req('push_transaction'), res(500, e), res(400, { ...e, code: 400 }), coverage.get('push_transaction')).cls, 'error-shape-diff');
  assert.equal(classify(req('push_transaction'), res(500, e), res(500, e), coverage.get('push_transaction')).cls, 'identical');
  const renamed = JSON.parse(JSON.stringify(e)); renamed.error.name = 'tx_no_auths'; renamed.error.details[0].line_number = 77;
  assert.equal(classify(req('push_transaction'), res(500, e), res(500, renamed), coverage.get('push_transaction')).cls, 'error-shape-diff');
  const relined = JSON.parse(JSON.stringify(e)); relined.error.details[0].line_number = 77; relined.error.details[0].file = 'x.cpp';
  assert.equal(classify(req('push_transaction'), res(500, e), res(500, relined), coverage.get('push_transaction')).cls, 'equal-after-normalization');
  assert.equal(classify(req('get_info'), res(200, {}), res(500, nodeosErr(500, 0, 'x', 'y', 'z'))).cls, 'DIFF');
});

test('transport errors are failing, and every failing class is in CLASSES', () => {
  assert.equal(classify(req('get_info'), { error: 'ECONNREFUSED' }, res(200, {})).cls, 'transport-error');
  for (const c of FAILING) assert.ok(CLASSES.includes(c));
});

test('CORS preflight: what a browser concludes, not how the proxy spells it', () => {
  const r = req('get_info', { method: 'OPTIONS', cors: 'preflight', headers: { origin: 'https://dapp.example.org', 'access-control-request-method': 'POST', 'access-control-request-headers': 'content-type' } });
  const nginx = res(200, '', { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'Origin, X-Requested-With, Content-Type, Accept' });
  const edge = res(204, '', { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, POST, OPTIONS', 'access-control-allow-headers': 'content-type' });
  assert.deepEqual(corsVerdict(r, nginx), { preflight_ok: true, origin_allowed: true, method_allowed: true, headers_allowed: true });
  assert.equal(classify(r, nginx, edge).cls, 'equal-after-normalization');
  const noHeaders = res(204, '', { 'access-control-allow-origin': '*' });
  const c = classify(r, nginx, noHeaders);
  assert.equal(c.cls, 'DIFF');
  assert.equal(c.diff[0].path, 'headers_allowed');
  assert.equal(classify(r, nginx, res(404, '', {})).cls, 'DIFF');
  // simple requests: body compared as usual AND the origin verdict must agree
  const s = req('get_info', { cors: 'simple', headers: { origin: 'https://dapp.example.org' } });
  assert.equal(classify(s, res(200, { a: 1 }, { 'access-control-allow-origin': '*' }), res(200, { a: 1 }, {})).cls, 'DIFF');
  assert.equal(classify(s, res(200, { a: 1 }, { 'access-control-allow-origin': '*' }), res(200, { a: 1 }, { 'access-control-allow-origin': 'https://dapp.example.org' })).cls, 'identical');
});

test('allow-list entries need every given field to match', () => {
  const r = req('get_block', { id: 'get_block#010-early-block-2', tags: ['get_block', 'early-block-2', 'chain'] });
  const diff = { cls: 'DIFF', diff: [{ path: '(status)' }] };
  assert.ok(allowedBy([{ id_prefix: 'get_block#', tag: 'early-block-2', reason: 'r' }], r, diff));
  assert.equal(allowedBy([{ id_prefix: 'get_block#', tag: 'other', reason: 'r' }], r, diff), null);
  assert.equal(allowedBy([{ endpoint: 'get_block', class: 'error-shape-diff', reason: 'r' }], r, diff), null);
  assert.ok(allowedBy([{ group: 'chain', class: 'DIFF', reason: 'r' }], r, diff));
  assert.ok(allowedBy([{ endpoint: 'get_block', path: '(status)', reason: 'r' }], r, diff));
  assert.equal(allowedBy([{ endpoint: 'get_block', path: 'rows', reason: 'r' }], r, diff), null);
});

test('run-time failed transactions: unsigned, live expiration, TAPOS from LIB, the requested wrapper', () => {
  const st = { lib: 0x1234abcd, libId: '1234abcd' + 'deadbeef' + '11223344' + 'aa'.repeat(20) }; // prefix = id bytes 8..12
  const t = { actions: [{ account: 'eosio.token', name: 'transfer', authorization: [{ actor: 'alice', permission: 'active' }], data: '' }], expire_in: 1800 };
  const now = Date.UTC(2026, 9, 1, 0, 0, 0);
  const p = buildTx({ ...t, wrap: 'packed' }, st, now);
  assert.deepEqual(p.signatures, []);
  const b = Buffer.from(p.packed_trx, 'hex');
  assert.equal(b.readUInt32LE(0), now / 1000 + 1800, 'expiration = now + expire_in');
  assert.equal(b.readUInt16LE(4), 0xabcd, 'ref_block_num = LIB & 0xffff');
  assert.equal(b.readUInt32LE(6), Buffer.from('11223344', 'hex').readUInt32LE(0), 'ref_block_prefix from the LIB id');
  const bad = Buffer.from(buildTx({ ...t, ref: 'bad-prefix', wrap: 'packed' }, st, now).packed_trx, 'hex');
  assert.notEqual(bad.readUInt32LE(6), b.readUInt32LE(6));
  assert.equal(buildTx({ ...t, wrap: 'send_transaction2' }, st, now).return_failure_trace, false);
  assert.equal(buildTx({ ...t, wrap: 'send_transaction2-trace' }, st, now).return_failure_trace, true);
  assert.ok(Array.isArray(buildTx({ ...t, wrap: 'push_transactions' }, st, now)));
});
