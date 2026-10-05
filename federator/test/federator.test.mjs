// node --test federator/test
// Starts the federator against in-process mocks — legacy Hyperion (pre-cut index + history), local hyperion-rs
// (post-cut index + history) and the chain's /v1/chain — and checks the chain-truth rule for state and the
// history merge across the cut.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { randomBytes } from 'node:crypto';

const HERE = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const fed = require('../server.js');
const edge = require('../../gateway/server.js');

const CUT = 1000;
const CHAIN_ID = 'c'.repeat(64);
const CUT_ID = '000003e8' + 'ab'.repeat(28);   // block ids carry their height in the first 4 bytes
const DEV_EOS = 'EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV';
const DEV_PUB = 'PUB_K1_6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5BoDq63';
const OTHER = edge.pubSpelling({ type: 'K1', data: Buffer.concat([Buffer.from([3]), randomBytes(32)]) });
const perm = (name, keys, accounts = [], threshold = 1) => ({ perm_name: name, parent: name === 'owner' ? '' : 'owner',
  required_auth: { threshold, keys: keys.map((key) => ({ key, weight: 1 })), accounts: accounts.map((a) => ({ permission: a, weight: 1 })), waits: [] }, linked_actions: [] });

// ---- the chain (truth) --------------------------------------------------------------------------------------
let chainUp = true;
const chainAccounts = {
  alice: { account_name: 'alice', permissions: [perm('owner', [DEV_PUB]), perm('active', [DEV_PUB], [{ actor: 'bob', permission: 'active' }])] },
  carol: { account_name: 'carol', permissions: [perm('owner', [OTHER]), perm('active', [OTHER])] }, // DEV key removed after the cut
  dave: { account_name: 'dave', permissions: [perm('owner', [DEV_PUB]), perm('active', [DEV_PUB])] }, // DEV key added after the cut
  erin: { account_name: 'erin', permissions: [perm('owner', [OTHER]), perm('active', [OTHER])] },   // bob removed after the cut
};
const balances = { // code -> account -> rows
  'eosio.token': { alice: ['12.3400 XPR'] },
  'newtoken': { alice: ['1.00 NEW'] },
  'oldtoken': { alice: [] }, // balance row erased after the cut
};
const chainCalls = [];
function chain(req, res, body) {
  const name = req.url.split('/').pop();
  const p = JSON.parse(body || '{}');
  chainCalls.push({ name, p, host: req.headers.host });
  if (!chainUp) return send(res, 502, { code: 502, message: 'node unreachable' });
  if (name === 'get_info') return send(res, 200, { chain_id: CHAIN_ID, head_block_num: 1010, head_block_id: '000003f2' + 'cd'.repeat(28), last_irreversible_block_num: 1010 });
  if (name === 'get_block') return p.block_num_or_id === String(CUT) ? send(res, 200, { id: CUT_ID, block_num: CUT }) : send(res, 500, { code: 500, message: 'unknown block' });
  if (name === 'get_account') return chainAccounts[p.account_name] ? send(res, 200, chainAccounts[p.account_name]) : send(res, 500, { code: 500, message: 'unknown account' });
  if (name === 'get_currency_balance') {
    const c = balances[p.code];
    return c ? send(res, 200, c[p.account] || []) : send(res, 500, { code: 500, message: 'no abi' });
  }
  return send(res, 404, { code: 404 });
}

// ---- indexes -----------------------------------------------------------------------------------------------
let legacyUp = true;
const act = (block, name) => ({ block_num: block, '@timestamp': `2026-09-30T00:00:${String(block % 60).padStart(2, '0')}.000`, trx_id: `t${block}`, block_id: `b${block}`,
  global_sequence: block * 10, act: { account: 'eosio.token', name, authorization: [], data: {} }, receipts: [{ receiver: 'alice', global_sequence: block * 10, recv_sequence: block }] });
const legacyActs = [act(999, 'pre3'), act(998, 'pre2'), act(997, 'pre1')];
const localActs = [act(1005, 'post2'), act(1003, 'post1')];
function page(list, q) {
  const skip = Number(q.get('skip') || 0), limit = Number(q.get('limit') || 10);
  return { actions: list.slice(skip, skip + limit), total: { value: list.length, relation: 'eq' }, lib: 1010 };
}
function legacy(req, res, body) {
  const u = new URL(req.url, 'http://x'), q = u.searchParams;
  if (!legacyUp) return send(res, 429, { error: 'rate limited' });
  switch (u.pathname) {
    // Stale pre-cut index: amounts frozen at the cut, includes a token whose row is gone now.
    case '/v2/state/get_tokens': return send(res, 200, { account: q.get('account'), tokens: q.get('account') === 'alice'
      ? [{ symbol: 'XPR', precision: 4, amount: 5, contract: 'eosio.token' }, { symbol: 'OLD', precision: 0, amount: 9, contract: 'oldtoken' }] : [] });
    // Legacy index keyed by the EOS… spelling only.
    case '/v2/state/get_key_accounts': return send(res, 200, { account_names: q.get('public_key') === DEV_EOS ? ['alice', 'carol', 'zombie'] : [] });
    case '/v1/history/get_controlled_accounts': return send(res, 200, { controlled_accounts: JSON.parse(body).controlling_account === 'bob' ? ['alice', 'erin'] : [] });
    case '/v2/history/get_actions': return send(res, 200, page(legacyActs, q));
    case '/v2/history/get_transaction': return send(res, 200, { actions: q.get('id') === 'pre' ? [act(998, 'x')] : [] });
    case '/v1/history/get_transaction': return JSON.parse(body).id === 'pre' ? send(res, 200, { id: 'pre', block_num: 998, from: 'legacy' }) : send(res, 404, { error: 'not found' });
    case '/v2/health': return send(res, 200, { health: [] });
    default: return send(res, 404, { error: 'no' });
  }
}
function local(req, res, body) {
  const u = new URL(req.url, 'http://x'), q = u.searchParams;
  switch (u.pathname) {
    // Post-cut deltas only: alice received newtoken after the cut; eosio.token untouched -> absent here.
    case '/v2/state/get_tokens': return send(res, 200, { account: q.get('account'), tokens: q.get('account') === 'alice' ? [{ symbol: 'NEW', precision: 2, amount: 999, contract: 'newtoken' }] : [] });
    case '/v2/state/get_key_accounts': return send(res, 200, { account_names: q.get('public_key') === DEV_PUB ? ['dave'] : [] });
    case '/v1/history/get_controlled_accounts': return send(res, 200, { controlled_accounts: [] });
    case '/v2/history/get_actions': return send(res, 200, page(localActs, q));
    case '/v2/history/get_transaction': return send(res, 200, { actions: q.get('id') === 'post' ? [act(1003, 'x')] : [] });
    case '/v1/history/get_transaction': return JSON.parse(body).id === 'post' ? send(res, 200, { id: 'post', block_num: 1003, from: 'local' }) : send(res, 404, { error: 'not found' });
    case '/v2/state/get_links': return send(res, 200, { links: [] });
    case '/v2/health': return send(res, 200, { health: [{ service: 'PulseVM-RPC', status: 'OK', service_data: { head_block_num: 1010 } }, { service: 'Indexer', status: 'OK', service_data: { last_indexed_block: 1010 } }] });
    default: return send(res, 404, { error: 'no' });
  }
}

function send(res, status, body) { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); }
function listen(handler) {
  return new Promise((ok) => {
    const s = http.createServer((req, res) => { let b = ''; req.on('data', (c) => { b += c; }); req.on('end', () => handler(req, res, b)); });
    s.listen(0, '127.0.0.1', () => ok(s));
  });
}
async function startFederator(env) {
  const p = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', PASSTHROUGH_PORT: '0', ...env } });
  const url = await new Promise((ok, ko) => {
    let out = '';
    p.stdout.on('data', (d) => { out += d; const m = out.match(/federating router\) on 127\.0\.0\.1:(\d+)/); if (m) ok(`http://127.0.0.1:${m[1]}`); });
    p.stderr.on('data', (d) => { out += d; });
    p.on('exit', (c) => ko(new Error(`federator exited ${c}: ${out}`)));
    setTimeout(() => ko(new Error('federator did not start: ' + out)), 8000);
  });
  return { p, url };
}

let chainSrv, legacySrv, localSrv, proc, base, env;
before(async () => {
  [chainSrv, legacySrv, localSrv] = await Promise.all([listen(chain), listen(legacy), listen(local)]);
  const dir = mkdtempSync(join(tmpdir(), 'fed-test-'));
  writeFileSync(join(dir, 'boundary.json'), JSON.stringify({ cut_block: CUT, cut_time: '2026-09-30T00:00:00.000', cut_block_id: CUT_ID, chain_id: CHAIN_ID }));
  env = { LOCAL: `http://127.0.0.1:${localSrv.address().port}`, LEGACY: `http://127.0.0.1:${legacySrv.address().port}`, CHAIN_URL: `http://127.0.0.1:${chainSrv.address().port}` };
  ({ p: proc, url: base } = await startFederator({ ...env, BOUNDARY_FILE: join(dir, 'boundary.json') }));
});
after(() => { proc?.kill(); chainSrv?.close(); legacySrv?.close(); localSrv?.close(); });
const get = async (path, b = base) => { const r = await fetch(b + path); return { status: r.status, json: await r.json(), headers: r.headers }; };
const post = async (path, body, b = base) => { const r = await fetch(b + path, { method: 'POST', body: JSON.stringify(body) }); return { status: r.status, json: await r.json() }; };

test('key helpers agree with the edge', () => {
  assert.equal(fed.keyCanon(DEV_EOS), fed.keyCanon(DEV_PUB));
  assert.equal(fed.keyCanon(DEV_EOS), edge.keyCanon(DEV_EOS));
  assert.deepEqual(fed.keySpellings(DEV_EOS).sort(), [DEV_EOS, DEV_PUB].sort());
  assert.deepEqual(fed.tokenRow('eosio.token', '12.3400 XPR'), { symbol: 'XPR', precision: 4, amount: 12.34, contract: 'eosio.token' });
  assert.deepEqual(fed.tokenRow('x', '7 ABC'), { symbol: 'ABC', precision: 0, amount: 7, contract: 'x' });
});

test('get_tokens: an account untouched since the cut gets CHAIN amounts (not an empty list)', async () => {
  const r = await get('/v2/state/get_tokens?account=alice');
  assert.equal(r.status, 200);
  const xpr = r.json.tokens.find((t) => t.contract === 'eosio.token');
  assert.deepEqual(xpr, { symbol: 'XPR', precision: 4, amount: 12.34, contract: 'eosio.token' }, 'legacy said 5 (stale); the chain says 12.34');
});
test('get_tokens: a token first received after the cut is found, amount from the chain', async () => {
  const r = await get('/v2/state/get_tokens?account=alice');
  assert.deepEqual(r.json.tokens.find((t) => t.contract === 'newtoken'), { symbol: 'NEW', precision: 2, amount: 1, contract: 'newtoken' }, 'local index said 999');
});
test('get_tokens: a contract with no balance row now is omitted; shape is Hyperion', async () => {
  const r = await get('/v2/state/get_tokens?account=alice');
  assert.equal(r.json.account, 'alice');
  assert.deepEqual(r.json.tokens.map((t) => t.contract).sort(), ['eosio.token', 'newtoken']);
  for (const t of r.json.tokens) assert.deepEqual(Object.keys(t).sort(), ['amount', 'contract', 'precision', 'symbol']);
  assert.equal(r.json.partial, undefined);
});
test('get_tokens: chain down is a 502 — never index amounts', async () => {
  chainUp = false;
  try { assert.equal((await get('/v2/state/get_tokens?account=alice')).status, 502); } finally { chainUp = true; }
});
test('get_tokens: one discovery source down is answered but flagged partial', async () => {
  legacyUp = false;
  try {
    const r = await get('/v2/state/get_tokens?account=alice');
    assert.equal(r.status, 200);
    assert.equal(r.json.partial, true);
    assert.ok(r.json.source_errors.legacy);
    assert.deepEqual(r.json.tokens.map((t) => t.contract), ['newtoken']);
  } finally { legacyUp = true; }
});
test('get_key_accounts: a key removed after the cut is NOT returned; one added after the cut is', async () => {
  for (const key of [DEV_EOS, DEV_PUB]) {
    const r = await get(`/v2/state/get_key_accounts?public_key=${key}`);
    assert.equal(r.status, 200);
    assert.deepEqual(r.json.account_names, ['alice', 'dave'], `spelling ${key.slice(0, 7)}: carol rotated away, zombie does not exist`);
  }
  const v1 = await post('/v1/history/get_key_accounts', { public_key: DEV_EOS });
  assert.equal(v1.status, 200);
  assert.deepEqual(v1.json.account_names, ['alice', 'dave']);
});
test('get_controlled_accounts: discovery verified against current permissions', async () => {
  const r = await post('/v1/history/get_controlled_accounts', { controlling_account: 'bob' });
  assert.equal(r.status, 200);
  assert.deepEqual(r.json.controlled_accounts, ['alice'], 'erin no longer lists bob');
});
test('get_account: permissions from the chain, chain token amounts, federated actions', async () => {
  const r = await get('/v2/state/get_account?account=alice');
  assert.equal(r.status, 200);
  assert.equal(r.json.account, 'alice');
  assert.deepEqual(r.json.permissions, chainAccounts.alice.permissions);
  assert.equal(r.json.tokens.find((t) => t.contract === 'eosio.token').amount, 12.34);
  assert.deepEqual(r.json.actions.map((a) => a.block_num), [1005, 1003, 999, 998, 997]);
  assert.equal(r.json.total_actions, 5);
  assert.equal(r.json.lib, 1010);
  assert.equal((await get('/v2/state/get_account?account=nobody')).status, 404);
});
test('/v1/history/get_actions pos=-1: latest N across the cut, ascending, v1 shape', async () => {
  const r = await post('/v1/history/get_actions', { account_name: 'alice', pos: -1, offset: -3 });
  assert.equal(r.status, 200);
  assert.deepEqual(r.json.actions.map((a) => a.block_num), [999, 1003, 1005]);
  assert.deepEqual(r.json.actions.map((a) => a.account_action_seq), [2, 3, 4]);
  const a = r.json.actions[2];
  assert.equal(a.global_action_seq, 10050);
  assert.equal(a.action_trace.act.name, 'post2');
  assert.equal(a.action_trace.trx_id, 't1005');
  assert.equal(r.json.federation.positional, 'exact-order');
});
test('/v1/history/get_actions pos>=0: positions over the combined timeline, flagged approximate', async () => {
  const r = await post('/v1/history/get_actions', { account_name: 'alice', pos: 0, offset: 1 });
  assert.deepEqual(r.json.actions.map((a) => a.block_num), [997, 998]);
  assert.equal(r.json.federation.positional, 'approximate');
  const tail = await post('/v1/history/get_actions', { account_name: 'alice', pos: 3, offset: 10 });
  assert.deepEqual(tail.json.actions.map((a) => a.block_num), [1003, 1005]);
});
test('/v1/history/get_transaction and /v2 get_transaction: new-then-legacy', async () => {
  assert.equal((await post('/v1/history/get_transaction', { id: 'post' })).json.from, 'local');
  assert.equal((await post('/v1/history/get_transaction', { id: 'pre' })).json.from, 'legacy');
  assert.equal((await post('/v1/history/get_transaction', { id: 'none' })).status, 404);
  const v2 = await get('/v2/history/get_transaction?id=pre');
  assert.equal(v2.json._premigration, true);
});
test('/v2/history/get_actions merge is unchanged', async () => {
  const r = await get('/v2/history/get_actions?account=alice&limit=3');
  assert.deepEqual(r.json.actions.map((a) => a.block_num), [1005, 1003, 999]);
  assert.equal(r.json.total.value, 5);
  assert.equal(r.json.actions[2]._premigration, true);
});
test('other /v2/state/* are tagged index-only', async () => {
  const r = await get('/v2/state/get_links?account=alice');
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('x-pulse-federation'), 'index-only');
});
test('/v2/health keeps its shape (the flip gate reads federation.local.ok)', async () => {
  const r = await get('/v2/health');
  assert.equal(r.json.federation.local.ok, true);
  assert.equal(r.json.federation.ok, true);
  assert.equal(r.json.federation.boundary.cut_block, CUT);
  assert.equal(r.json.federation.boundary.status, 'valid');
  assert.equal(r.json.federation.boundary.identity.cut_block_id, 'verified');
});
test('chain calls to a loopback CHAIN_URL carry Host: localhost', () => {
  assert.ok(chainCalls.length > 0);
  assert.ok(chainCalls.every((c) => c.host === 'localhost'));
});
test('before any boundary file exists: history and state FAIL CLOSED (503), never unbounded legacy', async () => {
  const { p, url } = await startFederator({ ...env, BOUNDARY_FILE: join(tmpdir(), `absent-${Date.now()}.json`) });
  try {
    for (const path of ['/v2/state/get_tokens?account=alice', '/v2/history/get_actions?account=alice', '/v2/history/get_transaction?id=pre']) {
      const r = await get(path, url);
      assert.equal(r.status, 503, path);
      assert.equal(r.headers.get('x-pulse-federation'), 'boundary-absent');
    }
    assert.equal((await post('/v1/history/get_transaction', { id: 'pre' }, url)).status, 503);
    const h = await get('/v2/health', url);
    assert.equal(h.json.federation.boundary.staged, false);
    assert.equal(h.json.federation.ok, false);
  } finally { p.kill(); }
});
test('ALLOW_NO_BOUNDARY=1 (pre-ceremony staging only): legacy-only until a boundary file is first seen', async () => {
  const { p, url } = await startFederator({ ...env, ALLOW_NO_BOUNDARY: '1', BOUNDARY_FILE: join(tmpdir(), `absent-${Date.now()}.json`) });
  try {
    const t = await get('/v2/state/get_tokens?account=alice', url);
    assert.equal(t.json.tokens.find((x) => x.contract === 'eosio.token').amount, 5, 'legacy passthrough');
    const k = await get(`/v2/state/get_key_accounts?public_key=${DEV_EOS}`, url);
    assert.deepEqual(k.json.account_names, ['alice', 'carol', 'zombie']);
    const tx = await post('/v1/history/get_transaction', { id: 'pre' }, url);
    assert.equal(tx.json.from, 'legacy');
  } finally { p.kill(); }
});
