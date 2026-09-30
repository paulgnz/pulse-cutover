// node --test tools/conformance/test
// The response normalizer: volatile (shape-only), live (frozen = strict), ignore, unordered, mask, error bodies.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { splitPath, shapeOf, normalize, normalizeError, maskMessage, equal, jsonDiff } from '../lib.mjs';

test('splitPath understands dots, [*], [n] and the root', () => {
  assert.deepEqual(splitPath('rows.*.unpaid_blocks'), ['rows', '*', 'unpaid_blocks']);
  assert.deepEqual(splitPath('rows[*].count'), ['rows', '*', 'count']);
  assert.deepEqual(splitPath('actions[0].block_num'), ['actions', '0', 'block_num']);
  assert.deepEqual(splitPath('$'), []);
  assert.deepEqual(splitPath(''), []);
});

test('shapeOf keeps field presence and types, not values or array length', () => {
  assert.deepEqual(shapeOf({ b: 1, a: 'x', c: null }), { a: '<string>', b: '<number>', c: '<null>' });
  assert.deepEqual(shapeOf([1, 2, 3]), shapeOf([7]));
  assert.notDeepEqual(shapeOf([1]), shapeOf(['1']));
  assert.deepEqual(shapeOf({ used: 5, max: 9 }), shapeOf({ max: 1, used: 2 }));
});

test('volatile paths compare by shape: two get_info answers at different heads agree', () => {
  const spec = { volatile: ['head_block_num', 'head_block_id', 'server_version'] };
  const a = { chain_id: 'c1', head_block_num: 100, head_block_id: 'aa', server_version: 'd133c641' };
  const b = { chain_id: 'c1', head_block_num: 107, head_block_id: 'bb', server_version: '04774eb7' };
  assert.ok(equal(normalize(a, spec), normalize(b, spec)));
  // …but a real value difference and a type change are still caught
  assert.ok(!equal(normalize({ ...a, chain_id: 'c2' }, spec), normalize(b, spec)));
  assert.ok(!equal(normalize({ ...a, head_block_num: '100' }, spec), normalize(b, spec)));
  // …and so is a missing volatile field
  const { head_block_id, ...noId } = b; // eslint-disable-line no-unused-vars
  assert.ok(!equal(normalize(a, spec), normalize(noId, spec)));
});

test('volatile object subtrees keep their structure (cpu_limit fields must still be present)', () => {
  const spec = { volatile: ['cpu_limit'] };
  const a = { cpu_limit: { used: 1, available: 2, max: 3 } }, b = { cpu_limit: { used: 9, available: 8, max: 7 } };
  assert.ok(equal(normalize(a, spec), normalize(b, spec)));
  assert.ok(!equal(normalize(a, spec), normalize({ cpu_limit: { used: 9, max: 7 } }, spec)));
});

test('wildcards and ** reach into arrays and subtrees', () => {
  const spec = { volatile: ['rows.*.unpaid_blocks', 'processed.**'] };
  const a = { rows: [{ owner: 'x', unpaid_blocks: 5 }, { owner: 'y', unpaid_blocks: 0 }], processed: { elapsed: 10, action_traces: [{ elapsed: 3 }] } };
  const b = { rows: [{ owner: 'x', unpaid_blocks: 9 }, { owner: 'y', unpaid_blocks: 1 }], processed: { elapsed: 99, action_traces: [{ elapsed: 1 }, { elapsed: 2 }] } };
  assert.ok(equal(normalize(a, spec), normalize(b, spec)));
  assert.ok(!equal(normalize(a, spec), normalize({ ...b, rows: [{ owner: 'z', unpaid_blocks: 1 }, b.rows[1]] }, spec)));
});

test('the root can be volatile (get_currency_balance on a moving chain)', () => {
  const spec = { live: ['$'] };
  assert.ok(equal(normalize(['1.0000 XPR'], spec), normalize(['2.5000 XPR', '3.0000 XPR'], spec)));
  assert.ok(!equal(normalize(['1.0000 XPR'], spec, { frozen: true }), normalize(['2.5000 XPR'], spec, { frozen: true })));
});

test('live paths are masked on a live chain and strict when frozen (the pre-flip rig)', () => {
  const spec = { live: ['rows.*.count'] };
  const a = { rows: [{ scope: 'alice', count: 1 }] }, b = { rows: [{ scope: 'alice', count: 2 }] };
  assert.ok(equal(normalize(a, spec), normalize(b, spec)));
  assert.ok(!equal(normalize(a, spec, { frozen: true }), normalize(b, spec, { frozen: true })));
});

test('ignore drops fields one side adds by design', () => {
  const spec = { ignore: ['pulsevm_head_block_time', 'query_time_ms'] };
  assert.ok(equal(normalize({ a: 1, pulsevm_head_block_time: 'x' }, spec), normalize({ a: 1 }, spec)));
  assert.ok(equal(normalize({ a: 1, query_time_ms: 3.2 }, spec), normalize({ a: 1, query_time_ms: 9 }, spec)));
});

test('unordered arrays are sorted before comparison; others keep their order', () => {
  const spec = { unordered: ['account_names'] };
  assert.ok(equal(normalize({ account_names: ['b', 'a'] }, spec), normalize({ account_names: ['a', 'b'] }, spec)));
  assert.ok(!equal(normalize({ rows: ['b', 'a'] }, spec), normalize({ rows: ['a', 'b'] }, spec)));
});

test('mask: error texts inside 2xx bodies compare without timestamps and source lines', () => {
  const spec = { mask: ['*.processed.error'] };
  const mk = (t, line) => [{ transaction_id: '0'.repeat(64), processed: { error: `3040005 expired_tx_exception: Expired Transaction\nexpiration 2020-01-01T00:00:00.000, block time ${t}\n nodeos producer_plugin.cpp:${line} process_incoming` } }];
  assert.ok(equal(normalize(mk('2026-09-30T12:15:35.500', 877), spec), normalize(mk('2026-09-30T12:15:36.000', 876), spec)));
  assert.ok(!equal(normalize(mk('2026-09-30T12:15:35.500', 877), spec), normalize([{ transaction_id: '0'.repeat(64), processed: { error: '3090003 unsatisfied_authorization' } }], spec)));
});

test('maskMessage masks ISO times, source file:line and numbers of 6+ digits, keeps short numbers and names', () => {
  // (7-digit exception codes are masked too; the exception name next to them and error.code are still compared)
  assert.equal(maskMessage('block 408549801 at 2026-09-30T12:15:36.000 (producer_plugin.cpp:877) weight 12345'), 'block <n> at <time> (<src>) weight 12345');
  assert.equal(maskMessage('unknown key (eosio::chain::name): conform.none'), 'unknown key (eosio::chain::name): conform.none');
});

test('error bodies are reduced to code/name/what + detail messages (file/line/method dropped)', () => {
  const a = { code: 500, message: 'Internal Service Error', error: { code: 3050003, name: 'eosio_assert_message_exception', what: 'eosio_assert_message assertion failure',
    details: [{ message: 'assertion failure with message: overdrawn balance', file: 'cf_system.cpp', line_number: 14, method: 'eosio_assert' }] } };
  const b = JSON.parse(JSON.stringify(a)); b.error.details[0].file = 'other.cpp'; b.error.details[0].line_number = 99; b.error.details.method = 'x';
  assert.ok(equal(normalize(a, {}, { status: 500 }), normalize(b, {}, { status: 500 })));
  const c = JSON.parse(JSON.stringify(a)); c.error.name = 'tx_cpu_usage_exceeded';
  assert.ok(!equal(normalize(a, {}, { status: 500 }), normalize(c, {}, { status: 500 })));
  assert.deepEqual(normalizeError({ statusCode: 400, error: 'Bad Request', message: 'x' }), { statusCode: 400, error: 'Bad Request', message: 'x' });
});

test('jsonDiff reports minimal paths with kinds', () => {
  const d = jsonDiff({ a: 1, b: [1, 2], c: { d: 'x' } }, { a: 2, b: [1], c: { e: 'y' } });
  const byPath = Object.fromEntries(d.map((x) => [x.path, x.kind]));
  assert.equal(byPath.a, 'value');
  assert.equal(byPath.b, 'length');
  assert.equal(byPath['c.d'], 'missing-in-b');
  assert.equal(byPath['c.e'], 'extra-in-b');
  assert.equal(jsonDiff({ x: 'y'.repeat(500) }, { x: 'z'.repeat(500) })[0].a.startsWith('<string len=500'), true);
});
