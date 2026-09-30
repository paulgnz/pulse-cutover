// node --test gateway/test
// Starts the /v1 edge (gateway/server.js) against in-process mock upstreams — a PulseVM node (native /v1/chain
// + JSON-RPC) and a federator — and checks every polyfill's shape, the native pass-through and the errors.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { createHash, randomBytes } from 'node:crypto';

const HERE = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const edge = require('../server.js');

const BID = 'TESTBID';
const DEV_EOS = 'EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV';
const DEV_PUB = 'PUB_K1_6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5BoDq63';
const OTHER = edge.pubSpelling({ type: 'K1', data: Buffer.concat([Buffer.from([2]), randomBytes(32)]) });
const BLOCK_ID = '00000064' + 'a1b2c3d4' + 'e5f6'.repeat(12);
const blockJson = (n) => ({ timestamp: '2026-09-30T12:00:00.500', producer: 'eosio', confirmed: 0, previous: '00000063' + '0'.repeat(56),
  transaction_mroot: '0'.repeat(64), action_mroot: '1'.repeat(64), transactions: [], id: BLOCK_ID, block_num: n });

const seen = [];   // every request the mock node received: {path, host, body}
let nativeInfo = { head_block_num: 100, last_irreversible_block_num: 100, head_block_time: '2026-01-01T00:00:00.500', chain_id: 'c'.repeat(64) };
const producers = [
  { owner: 'alpha', total_votes: '100.0', producer_key: DEV_EOS, is_active: 1 },
  { owner: 'bravo', total_votes: '300.5', producer_key: DEV_EOS, is_active: 1 },
  { owner: 'charlie', total_votes: '200.0', producer_key: DEV_EOS, is_active: 1 },
  { owner: 'delta', total_votes: '900.0', producer_key: DEV_EOS, is_active: 0 },   // inactive: sorts after every active one
  { owner: 'echo', total_votes: '50.0', producer_key: DEV_EOS, is_active: 0 },
];
const accounts = {
  alice: { account_name: 'alice', permissions: [
    { perm_name: 'owner', parent: '', required_auth: { threshold: 1, keys: [{ key: DEV_PUB, weight: 1 }], accounts: [], waits: [] } },
    { perm_name: 'active', parent: 'owner', required_auth: { threshold: 2, keys: [{ key: DEV_PUB, weight: 1 }], accounts: [{ permission: { actor: 'bob', permission: 'active' }, weight: 1 }], waits: [] } }] },
  carol: { account_name: 'carol', permissions: [   // key rotated away after the cut
    { perm_name: 'active', parent: 'owner', required_auth: { threshold: 1, keys: [{ key: OTHER, weight: 1 }], accounts: [], waits: [] } }] },
};

function json(res, status, body) { res.writeHead(status, { 'content-type': 'application/json', 'x-mock': 'native' }); res.end(typeof body === 'string' ? body : JSON.stringify(body)); }
const nodeosErr = (what) => ({ code: 500, message: what, error: { code: 0, name: 'internal_error', what, details: [{ message: what, file: '', line_number: 0, method: '' }] } });

function mockNode(req, res, body) {
  const url = new URL(req.url, 'http://x');
  seen.push({ path: url.pathname, host: req.headers.host, body });
  if (url.pathname === `/ext/bc/${BID}/rpc`) {
    const { id, method, params } = JSON.parse(body);
    if (method === 'pulsevm.getRawBlock') {
      if (params.block_num_or_id === '999') return json(res, 200, { jsonrpc: '2.0', id, error: { code: 404, message: 'block_not_found', data: 'block 999 not found' } });
      return json(res, 200, { jsonrpc: '2.0', id, result: blockJson(Number(params.block_num_or_id)) });
    }
    if (method === 'pulsevm.getProducers') return json(res, 200, { jsonrpc: '2.0', id, result: { schedule_version: 3, active_producers: ['alpha', 'bravo'] } });
    return json(res, 200, { jsonrpc: '2.0', id, error: { code: -32601, message: 'method not found' } });
  }
  const m = url.pathname.match(new RegExp(`^/ext/bc/${BID}/v1/chain/(\\w+)$`));
  if (!m) return json(res, 404, nodeosErr('Not found'));
  let p; try { p = JSON.parse(body || '{}'); } catch { return json(res, 400, nodeosErr('Invalid JSON')); }
  switch (m[1]) {
    case 'get_info': return json(res, 200, nativeInfo);
    case 'get_block':
      if (typeof p.block_num_or_id !== 'string') return json(res, 400, nodeosErr('Invalid JSON'));
      if (p.block_num_or_id === '999') return json(res, 500, nodeosErr('block 999 not found'));
      return json(res, 200, blockJson(Number(p.block_num_or_id) || 100));
    case 'get_block_info':
      if (typeof p.block_num !== 'number') return json(res, 400, nodeosErr('Invalid JSON'));
      return json(res, 200, { block_num: p.block_num, id: BLOCK_ID, timestamp: 'TimePoint { elapsed: Microseconds { count: 1790769600500000 } }',
        producer: 'eosio', confirmed: 0, previous: '00000063' + '0'.repeat(56), schedule_version: 0, producer_signature: 'SIG_K1_', header_extensions: [], new_producers: null, ref_block_prefix: 1 });
    case 'get_account':
      return accounts[p.account_name] ? json(res, 200, accounts[p.account_name]) : json(res, 500, nodeosErr(`unknown account ${p.account_name}`));
    case 'get_raw_abi': return json(res, 200, { account_name: p.account_name, code_hash: 'a'.repeat(64), abi_hash: 'b'.repeat(64), abi: 'DmVvc2lvOjphYmkvMS4yAA==' });
    case 'get_table_rows': {
      if (p.index_position !== undefined && typeof p.index_position !== 'number') return json(res, 400, nodeosErr('Invalid JSON'));
      if (p.limit !== undefined && typeof p.limit !== 'number') return json(res, 400, nodeosErr('Invalid JSON'));
      if (p.table === 'producers') {
        const rows = p.json === false ? producers.map((r) => Buffer.from(r.owner).toString('hex')) : producers;
        return json(res, 200, { rows, more: false, next_key: '' });
      }
      if (p.table === 'global') return json(res, 200, { rows: [{ total_producer_vote_weight: '600.50000000000000000' }], more: false, next_key: '' });
      return json(res, 200, { rows: [{ echo: p }], more: false, next_key: '' });
    }
    case 'get_required_keys':
      if ((p.available_keys || []).some((k) => k.startsWith('EOS'))) return json(res, 400, nodeosErr('Invalid JSON'));
      return json(res, 200, { required_keys: (p.available_keys || []).filter((k) => k === DEV_PUB) });
    case 'push_transaction': case 'send_transaction':
      if (p.packed_trx === 'bad') return json(res, 500, nodeosErr('transaction declares authority that was not provided'));
      return json(res, 200, { transaction_id: createHash('sha256').update(Buffer.from(p.packed_trx, 'hex')).digest('hex') });
    case 'get_currency_balance': return json(res, 202, '["1.0000 XPR"]'); // odd status on purpose: pass-through must keep it
    default: return json(res, 404, nodeosErr('Not found'));
  }
}
const fedSeen = [];
function mockFederator(req, res, body) {
  const url = new URL(req.url, 'http://x');
  fedSeen.push({ path: url.pathname, body });
  const p = JSON.parse(body || '{}');
  if (url.pathname === '/v1/history/get_key_accounts') {
    // Discovery deliberately includes a stale candidate (carol no longer holds the key) and a missing account.
    return json(res, 200, { account_names: ['alice', 'carol', 'ghost'] });
  }
  if (url.pathname === '/v1/history/get_controlled_accounts') return json(res, 200, { controlled_accounts: p.controlling_account === 'bob' ? ['alice', 'carol'] : [] });
  if (url.pathname === '/v1/history/get_actions') return json(res, 200, { actions: [{ ok: 1 }], from: 'federator' });
  return json(res, 404, { error: 'nope' });
}
function listen(handler) {
  return new Promise((ok) => {
    const s = http.createServer((req, res) => { let b = ''; req.on('data', (c) => { b += c; }); req.on('end', () => handler(req, res, b)); });
    s.listen(0, '127.0.0.1', () => ok(s));
  });
}

let node, fed, proc, base, staticDir;
async function startEdge(env) {
  const p = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', ...env } });
  const url = await new Promise((ok, ko) => {
    let out = '';
    p.stdout.on('data', (d) => { out += d; const m = out.match(/on 127\.0\.0\.1:(\d+)/); if (m) ok(`http://127.0.0.1:${m[1]}`); });
    p.stderr.on('data', (d) => { out += d; });
    p.on('exit', (c) => ko(new Error(`edge exited ${c}: ${out}`)));
    setTimeout(() => ko(new Error('edge did not start: ' + out)), 8000);
  });
  return { p, url };
}
before(async () => {
  node = await listen(mockNode);
  fed = await listen(mockFederator);
  staticDir = mkdtempSync(join(tmpdir(), 'edge-static-'));
  writeFileSync(join(staticDir, 'activated_protocol_features.json'), JSON.stringify({ activated_protocol_features: Array.from({ length: 13 }, (_, i) => ({
    feature_digest: i.toString(16).padStart(64, '0'), activation_ordinal: i, activation_block_num: 10 + i * 2, specification: [{ name: 'builtin_feature_codename', value: `F${i}` }] })) }));
  writeFileSync(join(staticDir, 'consensus_parameters.json'), JSON.stringify({ chain_config: { max_block_cpu_usage: 200000 }, wasm_config: { max_pages: 528 } }));
  ({ p: proc, url: base } = await startEdge({
    NATIVE_BASE: `http://127.0.0.1:${node.address().port}/ext/bc/${BID}`,
    FEDERATOR_URL: `http://127.0.0.1:${fed.address().port}`, STATIC_DIR: staticDir,
  }));
});
after(() => { proc?.kill(); node?.close(); fed?.close(); });

const call = async (name, body, method = 'POST', b = base) => {
  const r = await fetch(`${b}/v1/chain/${name}`, { method, body: method === 'GET' ? undefined : (typeof body === 'string' ? body : JSON.stringify(body ?? {})) });
  const text = await r.text(); let j; try { j = JSON.parse(text); } catch { j = undefined; }
  return { status: r.status, json: j, text, headers: r.headers };
};
const lastSeen = (suffix) => [...seen].reverse().find((s) => s.path.endsWith(suffix));
function isNodeosError(r, status) {
  assert.equal(r.status, status);
  assert.equal(r.json.code, status);
  assert.equal(typeof r.json.message, 'string');
  assert.equal(typeof r.json.error.name, 'string');
  assert.equal(typeof r.json.error.what, 'string');
  assert.ok(Array.isArray(r.json.error.details));
}

// ---- pure helpers ----------------------------------------------------------------------------------------------
test('key spellings: EOS… and PUB_K1_… decode to the same key; JS RIPEMD-160 matches', () => {
  assert.equal(edge.keyCanon(DEV_EOS), edge.keyCanon(DEV_PUB));
  assert.equal(edge.toPubKey(DEV_EOS), DEV_PUB);
  assert.equal(edge.legacySpelling(edge.keyInfo(DEV_PUB)), DEV_EOS);
  assert.equal(edge.keyInfo(DEV_EOS.slice(0, -1) + 'X'), null, 'bad checksum rejected');
  for (const n of [0, 1, 55, 56, 64, 200]) {
    const b = randomBytes(n); let want;
    try { want = createHash('ripemd160').update(b).digest('hex'); } catch { continue; }
    assert.equal(edge.ripemd160js(b).toString('hex'), want);
  }
});
test('names and packed transactions serialize like nodeos', () => {
  assert.equal(edge.nameToU64('eosio').toString(16), '5530ea0000000000');
  assert.equal(edge.nameToU64('eosio.token').toString(16), '5530ea033482a600');
  const tx = { expiration: '2026-09-30T12:34:56', ref_block_num: 12345, ref_block_prefix: 3141592653, max_net_usage_words: 0, max_cpu_usage_ms: 0, delay_sec: 0,
    context_free_actions: [], actions: [{ account: 'eosio.token', name: 'transfer', authorization: [{ actor: 'alice', permission: 'active' }, { actor: 'bob.x1', permission: 'owner' }], data: '00a6823403ea3055ff' }], transaction_extensions: [] };
  // Reference bytes: eosjs 22 serializeTransaction of the same transaction.
  const ref = 'f001bd6a39304de640bb000000000100a6823403ea3055000000572d3ccdcd020000000000855c3400000000a8ed323200000000840e0e3d0000000080ab26a70900a6823403ea3055ff00';
  assert.equal(edge.packTransaction(tx).toString('hex'), ref);
  const id = edge.transactionId(tx);
  assert.equal(id, createHash('sha256').update(Buffer.from(ref, 'hex')).digest('hex'));
  assert.equal(id, edge.transactionId({ packed_trx: ref }));
  assert.equal(id, edge.transactionId({ transaction: tx }));
});

// ---- native pass-through ----------------------------------------------------------------------------------------
test('pass-through preserves status and body byte-for-byte, and sets Host: localhost', async () => {
  const r = await call('get_currency_balance', { code: 'eosio.token', account: 'alice' });
  assert.equal(r.status, 202);
  assert.equal(r.text, '["1.0000 XPR"]');
  assert.equal(lastSeen('/get_currency_balance').host, 'localhost');
  const e = await call('get_account', { account_name: 'nobody' });
  assert.equal(e.status, 500);
  assert.equal(e.json.error.what, 'unknown account nobody');
  const bad = await call('get_account', '{not json');
  assert.equal(bad.status, 400, 'invalid JSON forwarded as-is so the node answers');
});
test('get_block: numeric block_num_or_id is sent as a string; get_block_info: string block_num as a number, timestamp repaired', async () => {
  const b = await call('get_block', { block_num_or_id: 100 });
  assert.equal(b.status, 200);
  assert.equal(JSON.parse(lastSeen('/get_block').body).block_num_or_id, '100');
  const bi = await call('get_block_info', { block_num: '100' });
  assert.equal(bi.status, 200);
  assert.equal(bi.json.timestamp, '2026-09-30T12:00:00.500');
  assert.equal(bi.headers.get('x-pulse-edge'), 'timestamp-repaired');
  // eosjs transactionHeader: Date.parse(timestamp + 'Z') must be finite
  assert.ok(Number.isFinite(Date.parse(bi.json.timestamp + 'Z')));
});
test('get_table_rows: index names, numeric strings and numeric bounds are normalized', async () => {
  const r = await call('get_table_rows', { code: 'eosio', scope: 'eosio', table: 'x', index_position: 'secondary', key_type: 'name', limit: '5', lower_bound: 7, json: true });
  assert.equal(r.status, 200);
  const sent = JSON.parse(lastSeen('/get_table_rows').body);
  assert.equal(sent.index_position, 2); assert.equal(sent.limit, 5); assert.equal(sent.lower_bound, '7');
});
test('get_required_keys: EOS… keys accepted and answered in the client spelling', async () => {
  const r = await call('get_required_keys', { transaction: { actions: [] }, available_keys: [DEV_EOS, OTHER] });
  assert.equal(r.status, 200);
  assert.deepEqual(r.json.required_keys, [DEV_EOS]);
  assert.ok(JSON.parse(lastSeen('/get_required_keys').body).available_keys.includes(DEV_PUB));
});
test('get_info: stale head time on an idle chain is refreshed, true value kept', async () => {
  const r = await call('get_info', undefined, 'GET');
  assert.equal(r.status, 200);
  assert.equal(r.json.pulsevm_head_block_time, '2026-01-01T00:00:00.500');
  assert.ok(Date.now() - Date.parse(r.json.head_block_time + 'Z') < 5000);
  assert.equal(r.json.head_block_num, 100);
});

// ---- polyfills ------------------------------------------------------------------------------------------------
test('send_transaction2 unwraps the Leap 5 envelope into send_transaction', async () => {
  const r = await call('send_transaction2', { return_failure_trace: true, retry_trx: false, transaction: { signatures: ['SIG_K1_x'], compression: 0, packed_context_free_data: null, packed_trx: 'abcd' } });
  assert.equal(r.status, 200);
  assert.equal(r.json.transaction_id, createHash('sha256').update(Buffer.from('abcd', 'hex')).digest('hex'));
  const sent = JSON.parse(lastSeen('/send_transaction').body);
  assert.equal(sent.packed_trx, 'abcd'); assert.equal(sent.packed_context_free_data, '');
  isNodeosError(await call('send_transaction2', { retry_trx: false }), 400);
});
test('push_transactions: sequential, one result per transaction, failures in nodeos shape', async () => {
  const r = await call('push_transactions', [{ signatures: [], compression: 0, packed_trx: '01' }, { signatures: [], compression: 0, packed_trx: 'bad' }, { signatures: [], compression: 0, packed_trx: '02' }]);
  assert.equal(r.status, 200);
  assert.equal(r.json.length, 3);
  assert.match(r.json[0].transaction_id, /^[0-9a-f]{64}$/);
  assert.equal(r.json[1].transaction_id, '0'.repeat(64));
  assert.match(r.json[1].processed.error, /not provided/);
  assert.match(r.json[2].transaction_id, /^[0-9a-f]{64}$/);
  isNodeosError(await call('push_transactions', { not: 'an array' }), 400);
});
test('get_raw_block via pulsevm.getRawBlock; unknown block is a nodeos-shaped 400', async () => {
  const r = await call('get_raw_block', { block_num_or_id: 100 });
  assert.equal(r.status, 200);
  assert.equal(r.json.producer, 'eosio');
  assert.equal(JSON.parse(lastSeen('/rpc').body).params.block_num_or_id, '100');
  const e = await call('get_raw_block', { block_num_or_id: 999 });
  isNodeosError(e, 400);
  assert.equal(e.json.error.name, 'unknown_block_exception');
});
test('get_block_header / get_block_header_state carry what eosjs TAPOS reads', async () => {
  const h = await call('get_block_header', { block_num_or_id: 100 });
  assert.equal(h.status, 200);
  assert.equal(h.json.id, BLOCK_ID);
  assert.equal(h.json.signed_block_header.timestamp, '2026-09-30T12:00:00.500');
  assert.equal(h.json.signed_block_header.previous, '00000063' + '0'.repeat(56));
  assert.equal(h.json.signed_block_header.producer, 'eosio');
  assert.equal('new_producers' in h.json.signed_block_header, false, 'omitted when null, like nodeos');
  const s = await call('get_block_header_state', { block_num_or_id: 100 });
  assert.equal(s.status, 200);
  // eosjs transactionHeader(refBlock): refBlock.header.timestamp, refBlock.id, refBlock.block_num
  assert.equal(s.json.block_num, 100);
  assert.equal(s.json.id, BLOCK_ID);
  assert.ok(Number.isFinite(Date.parse(s.json.header.timestamp + 'Z')));
  assert.equal(s.json.dpos_irreversible_blocknum, 100);
  assert.equal(s.json.ref_block_prefix, Buffer.from(BLOCK_ID.slice(16, 24), 'hex').readUInt32LE(0));
  isNodeosError(await call('get_block_header', { block_num_or_id: 999 }), 500);
});
test('get_producers: Leap shape, vote order, limit/lower_bound/more, json=false rows', async () => {
  const r = await call('get_producers', { json: true, limit: 2, lower_bound: '' });
  assert.equal(r.status, 200);
  assert.deepEqual(r.json.rows.map((x) => x.owner), ['bravo', 'charlie']);
  assert.equal(r.json.more, 'alpha');
  assert.equal(r.json.total_producer_vote_weight, '600.50000000000000000');
  const r2 = await call('get_producers', { json: true, limit: 10, lower_bound: 'charlie' });
  // eosio.system by_votes: active by votes desc, then inactive by votes asc
  assert.deepEqual(r2.json.rows.map((x) => x.owner), ['charlie', 'alpha', 'echo', 'delta']);
  assert.equal(r2.json.more, '');
  const r3 = await call('get_producers', { json: false, limit: 1 });
  assert.equal(r3.json.rows[0], Buffer.from('bravo').toString('hex'));
});
test('get_producer_schedule: active version + names only, marked partial', async () => {
  const r = await call('get_producer_schedule', {});
  assert.equal(r.status, 200);
  assert.equal(r.json.active.version, 3);
  assert.deepEqual(r.json.active.producers.map((p) => p.producer_name), ['alpha', 'bravo']);
  assert.match(r.headers.get('x-pulse-edge'), /partial/);
});
test('get_raw_code_and_abi: abi from get_raw_abi, empty wasm + header', async () => {
  const r = await call('get_raw_code_and_abi', { account_name: 'eosio.token' });
  assert.equal(r.status, 200);
  assert.deepEqual(Object.keys(r.json).sort(), ['abi', 'account_name', 'wasm']);
  assert.equal(r.json.wasm, '');
  assert.equal(r.json.abi, 'DmVvc2lvOjphYmkvMS4yAA==');
  assert.equal(r.headers.get('x-pulse-edge'), 'wasm-unavailable');
});
test('get_activated_protocol_features: static-at-cut, paged like Leap', async () => {
  const r = await call('get_activated_protocol_features', { limit: 5 });
  assert.equal(r.status, 200);
  assert.equal(r.json.activated_protocol_features.length, 5);
  assert.equal(r.json.more, 5);
  const r2 = await call('get_activated_protocol_features', { lower_bound: 10, limit: 10 });
  assert.deepEqual(r2.json.activated_protocol_features.map((f) => f.activation_ordinal), [10, 11, 12]);
  assert.equal(r2.json.more, undefined);
  const r3 = await call('get_activated_protocol_features', { search_by_block_num: true, lower_bound: 30, reverse: true, limit: 2 });
  assert.deepEqual(r3.json.activated_protocol_features.map((f) => f.activation_block_num), [34, 32]);
  assert.equal(r.headers.get('x-pulse-edge'), 'static-at-cut');
  const c = await call('get_consensus_parameters', {});
  assert.equal(c.json.chain_config.max_block_cpu_usage, 200000);
});
test('static endpoints without a capture answer a nodeos-shaped 501', async () => {
  const empty = mkdtempSync(join(tmpdir(), 'edge-empty-'));
  const { p, url } = await startEdge({ NATIVE_BASE: `http://127.0.0.1:${node.address().port}/ext/bc/${BID}`, STATIC_DIR: empty });
  try {
    for (const n of ['get_activated_protocol_features', 'get_consensus_parameters']) {
      const r = await call(n, {}, 'POST', url);
      isNodeosError(r, 501);
      assert.match(r.json.error.what, /capture-static/);
    }
    // No capture -> cannot know deferred transactions are disabled -> 501, never a fabricated empty list
    isNodeosError(await call('get_scheduled_transactions', {}, 'POST', url), 501);
  } finally { p.kill(); }
});
test('get_scheduled_transactions: 501 while deferred transactions exist; [] once DISABLE_DEFERRED_TRXS_STAGE_1 is active', async () => {
  isNodeosError(await call('get_scheduled_transactions', {}), 501);
  const d = mkdtempSync(join(tmpdir(), 'edge-stage1-'));
  writeFileSync(join(d, 'activated_protocol_features.json'), JSON.stringify({ activated_protocol_features: [
    { feature_digest: 'fce57d2331667353a0eac6b4209b67b843a7262a848af0a49a6e2fa9f6584eb4', activation_ordinal: 0, activation_block_num: 1 }] }));
  const { p, url } = await startEdge({ NATIVE_BASE: `http://127.0.0.1:${node.address().port}/ext/bc/${BID}`, STATIC_DIR: d });
  try {
    const r = await call('get_scheduled_transactions', {}, 'POST', url);
    assert.equal(r.status, 200);
    assert.deepEqual(r.json, { transactions: [], more: '' });
  } finally { p.kill(); }
});
test('get_accounts_by_authorizers (keys): discovery from the federator, truth from the chain', async () => {
  const r = await call('get_accounts_by_authorizers', { keys: [DEV_EOS] });
  assert.equal(r.status, 200);
  // carol was a discovery candidate but no longer holds the key; ghost does not exist
  assert.deepEqual(r.json.accounts.map((a) => `${a.account_name}@${a.permission_name}`), ['alice@active', 'alice@owner']);
  const active = r.json.accounts.find((a) => a.permission_name === 'active');
  assert.deepEqual(active, { account_name: 'alice', permission_name: 'active', authorizing_key: DEV_EOS, weight: 1, threshold: 2 });
});
test('get_accounts_by_authorizers (accounts): wildcard and exact permission', async () => {
  const r = await call('get_accounts_by_authorizers', { accounts: ['bob'] });
  assert.equal(r.status, 200);
  assert.deepEqual(r.json.accounts, [{ account_name: 'alice', permission_name: 'active', authorizing_account: { actor: 'bob', permission: 'active' }, weight: 1, threshold: 2 }]);
  const none = await call('get_accounts_by_authorizers', { accounts: [{ actor: 'bob', permission: 'owner' }] });
  assert.deepEqual(none.json.accounts, []);
});
test('get_accounts_by_authorizers: discovery down is a 502, never an empty answer', async () => {
  const { p, url } = await startEdge({ NATIVE_BASE: `http://127.0.0.1:${node.address().port}/ext/bc/${BID}`, FEDERATOR_URL: 'http://127.0.0.1:1' });
  try { isNodeosError(await call('get_accounts_by_authorizers', { keys: [DEV_EOS] }, 'POST', url), 502); } finally { p.kill(); }
});
test('get_transaction_id: from packed_trx and from hex action data; JSON data is a 501', async () => {
  const tx = { expiration: '2026-09-30T12:34:56', ref_block_num: 1, ref_block_prefix: 2, max_net_usage_words: 0, max_cpu_usage_ms: 0, delay_sec: 0,
    context_free_actions: [], actions: [{ account: 'eosio.token', name: 'transfer', authorization: [{ actor: 'alice', permission: 'active' }], data: 'ff00' }], transaction_extensions: [] };
  const packed = edge.packTransaction(tx).toString('hex');
  const want = createHash('sha256').update(Buffer.from(packed, 'hex')).digest('hex');
  const a = await call('get_transaction_id', tx);
  assert.equal(a.status, 200); assert.equal(a.json, want);
  const b = await call('get_transaction_id', { signatures: [], compression: 'none', packed_trx: packed });
  assert.equal(b.json, want);
  const j = await call('get_transaction_id', { ...tx, actions: [{ ...tx.actions[0], data: { from: 'alice' } }] });
  isNodeosError(j, 501);
});
test('upstream-only endpoints: nodeos-shaped 501 naming upstream', async () => {
  for (const n of ['get_code', 'compute_transaction', 'send_read_only_transaction']) {
    const r = await call(n, { account_name: 'eosio' });
    isNodeosError(r, 501);
    assert.match(r.json.message, /not available on PulseVM yet \(upstream\)/);
  }
});
test('push_block and unknown endpoints: nodeos-style 404', async () => {
  for (const n of ['push_block', 'get_transaction_status', 'abi_json_to_bin', 'nope']) isNodeosError(await call(n, {}), 404);
  const r = await fetch(`${base}/v2/health`);
  assert.equal(r.status, 404);
});
test('/v1/history/* is proxied to the federator', async () => {
  const r = await fetch(`${base}/v1/history/get_actions`, { method: 'POST', body: JSON.stringify({ account_name: 'alice', pos: -1, offset: -1 }) });
  assert.equal(r.status, 200);
  assert.equal((await r.json()).from, 'federator');
  assert.equal(JSON.parse(fedSeen.at(-1).body).account_name, 'alice');
});
test('OPTIONS preflight is answered locally, CORS header on every response', async () => {
  const n = seen.length;
  const r = await fetch(`${base}/v1/chain/get_info`, { method: 'OPTIONS' });
  assert.equal(r.status, 204);
  assert.equal(seen.length, n);
  assert.equal((await call('get_code', {})).headers.get('access-control-allow-origin'), '*');
});
test('get_supported_apis lists every served /v1/chain endpoint', async () => {
  const r = await fetch(`${base}/v1/node/get_supported_apis`);
  const { apis } = await r.json();
  for (const n of ['get_info', 'send_transaction2', 'get_producers', 'get_accounts_by_authorizers']) assert.ok(apis.includes(`/v1/chain/${n}`));
  assert.ok(!apis.includes('/v1/chain/get_code'));
});
