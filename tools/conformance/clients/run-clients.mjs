#!/usr/bin/env node
// run-clients.mjs — run REAL Antelope client libraries against one /v1 endpoint and record what they see.
//
//   npm ci --prefix tools/conformance/clients          (once; node_modules stays in this directory)
//   node tools/conformance/clients/run-clients.mjs --url <endpoint> [--history <url>] [--out record.json]
//        [--account <name>] [--only eosjs22,protonjs30,...] [--cleos-image leap5=<image> ...]
//
// Libraries: eosjs 16/20/21/22, @proton/js (oldest 22.x, mid 26.x, latest 30.x), @wharfkit/antelope 1.x and 4.x,
// @wharfkit/session 4.x (headless, throwaway key), and cleos in Docker for any --cleos-image given (skipped with a
// reason when Docker does not answer). Flows per library: get_info, get_account, get_currency_balance,
// get_table_rows, get_abi/get_raw_abi, a transfer built with blocksBehind:3 and with useLastIrreversible (TAPOS +
// ABI fetch + serialization; recorded as the serialized transaction minus expiration/TAPOS, plus a TAPOS check
// against the same endpoint), get_required_keys, and key -> accounts the way that library does it.
//
// NOTHING IS BROADCAST. eosjs/@proton/js build with broadcast:false, sign:false and a signature provider that holds
// no key; wharfkit Session signs with a freshly generated throwaway key and broadcast:false; the transfer is a
// 0.0001 self-transfer (rejected by eosio.token even if it were ever pushed). Every HTTP call goes through a
// logger that refuses push_transaction/send_transaction*/push_transactions outright.
import { createRequire } from 'node:module';
import { writeFileSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';

const require = createRequire(import.meta.url);
const argv = process.argv.slice(2);
const opt = (k, d) => { const i = argv.indexOf(`--${k}`); return i >= 0 ? argv[i + 1] : d; };
const opts = (k) => argv.flatMap((a, i) => (a === `--${k}` ? [argv[i + 1]] : []));
const URL_ = (opt('url') || '').replace(/\/+$/, '');
if (!URL_) { console.error('usage: run-clients.mjs --url <endpoint> [--history <url>] [--out record.json] [--account name] [--only libs]'); process.exit(2); }
const HIST = (opt('history') || URL_).replace(/\/+$/, '');
const OUT = opt('out', 'clients.json');
const ONLY = opt('only') ? new Set(opt('only').split(',')) : null;
const RPS = Number(opt('rps', 4));
const log = (...a) => console.error('[clients]', ...a);

// ---- HTTP logging + write guard + rate limit (every library's fetch goes through here) -------------------------------------
const realFetch = globalThis.fetch;
let calls = [];
let nextAt = 0;
const WRITES = /\/v1\/chain\/(push_transaction|push_transactions|send_transaction|send_transaction2|push_block)\b/;
async function loggedFetch(input, init = {}) {
  const url = typeof input === 'string' ? input : input.url;
  const path = new URL(url).pathname;
  if (WRITES.test(path)) { calls.push({ path, refused: true }); throw new Error(`conformance guard: refused ${path} (nothing is ever broadcast)`); }
  const now = Date.now(), at = Math.max(now, nextAt); nextAt = at + 1000 / RPS;
  if (at > now) await new Promise((r) => setTimeout(r, at - now));
  try {
    const r = await realFetch(input, init);
    calls.push({ path, status: r.status });
    return r;
  } catch (e) { calls.push({ path, error: String(e.message || e) }); throw e; }
}
globalThis.fetch = loggedFetch; // eosjs16 (isomorphic-fetch keeps an existing global fetch) and anything else global

async function raw(path, body) { const r = await loggedFetch(URL_ + path, { method: 'POST', body: JSON.stringify(body || {}) }); return r.json(); }

// ---- helpers ------------------------------------------------------------------------------------------------------------------
const hex = (u8) => Buffer.from(u8).toString('hex');
const sha = (v) => createHash('sha256').update(typeof v === 'string' ? v : JSON.stringify(canon(v))).digest('hex');
function canon(v) { if (Array.isArray(v)) return v.map(canon); if (v && typeof v === 'object') { const o = {}; for (const k of Object.keys(v).sort()) o[k] = canon(v[k]); return o; } return v; }
const plain = (v) => JSON.parse(JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? String(x) : x))); // wharfkit objects -> JSON
// Serialized transaction minus expiration (4 bytes) + ref_block_num (2) + ref_block_prefix (4): identical on any
// endpoint that serves the same ABI, whatever head it is at.
const bodyHex = (serialized) => hex(serialized).slice(20);
const taposOf = (serialized) => { const b = Buffer.from(serialized); return { expiration: b.readUInt32LE(0), ref_block_num: b.readUInt16LE(4), ref_block_prefix: b.readUInt32LE(6) }; };
// TAPOS sanity against the same endpoint: the referenced block's id must yield ref_block_prefix.
async function taposCheck(t) {
  const info = await raw('/v1/chain/get_info');
  const head = info.head_block_num;
  const num = head - ((head - t.ref_block_num) & 0xffff);
  const bi = await raw('/v1/chain/get_block_info', { block_num: num });
  const id = bi.id || '';
  const prefix = id ? Buffer.from(id.slice(16, 24), 'hex').readUInt32LE(0) : null;
  return { ref_block: num, behind_head: head - num, prefix_matches: prefix === t.ref_block_prefix, expires_in_s: t.expiration - Math.floor(Date.now() / 1000) };
}

// ---- a context shared by all libraries: the account, key and token every flow uses --------------------------------------
async function context() {
  let account = opt('account');
  if (!account) { // first eosio.token holder from 'e' on with an active K1 key (deterministic for a given state)
    const s = await raw('/v1/chain/get_table_by_scope', { code: 'eosio.token', table: 'accounts', lower_bound: 'e', limit: 20 });
    for (const row of s.rows || []) {
      const a = await raw('/v1/chain/get_account', { account_name: row.scope });
      const act = (a.permissions || []).find((p) => p.perm_name === 'active');
      if (act && act.required_auth.keys.length) { account = row.scope; break; }
    }
  }
  const acct = await raw('/v1/chain/get_account', { account_name: account });
  const key = acct.permissions.find((p) => p.perm_name === 'active').required_auth.keys[0].key;
  const bal = await raw('/v1/chain/get_currency_balance', { code: 'eosio.token', account });
  const b0 = (Array.isArray(bal) && bal[0]) || '0.0000 XPR';
  const [amt, symbol] = b0.split(' ');
  const precision = (amt.split('.')[1] || '').length;
  const quantity = `${(10 ** -precision).toFixed(precision)} ${symbol}`;
  return { account, key, symbol, precision, quantity, chain_id: (await raw('/v1/chain/get_info')).chain_id };
}
const transferAction = (c) => ({ account: 'eosio.token', name: 'transfer', authorization: [{ actor: c.account, permission: 'active' }],
  data: { from: c.account, to: c.account, quantity: c.quantity, memo: 'conformance: never broadcast' } });

// ---- flows ------------------------------------------------------------------------------------------------------------------
// run(fn) -> {ok, value|error, calls:[paths hit, in order]}
async function run(fn) {
  calls = [];
  try { const value = await fn(); return { ok: true, value: plain(value), calls: calls.map((c) => `${c.path} ${c.status || c.error || 'refused'}`) }; } catch (e) {
    return { ok: false, error: String((e && (e.json ? JSON.stringify(e.json).slice(0, 300) : e.message)) || e).slice(0, 400), calls: calls.map((c) => `${c.path} ${c.status || c.error || 'refused'}`) };
  }
}
const pickInfo = (i) => ({ chain_id: i.chain_id, fields: Object.keys(plain(i)).sort() });
const pickAccount = (a) => ({ account_name: String(a.account_name), permissions: plain(a.permissions).map((p) => ({ perm_name: p.perm_name, parent: p.parent, threshold: p.required_auth.threshold,
  keys: p.required_auth.keys.map((k) => k.key), accounts: p.required_auth.accounts })), fields: Object.keys(plain(a)).sort() });

// eosjs 20/21/22 and @proton/js share the Api/JsonRpc surface
function eosjsLike(mod, name, { useLastIrreversible = false, protonRpc = false } = {}) {
  const { Api, JsonRpc } = mod;
  const { TextEncoder, TextDecoder } = require('util');
  return async (c) => {
    const rpc = protonRpc ? new JsonRpc([URL_], { fetch: loggedFetch }) : new JsonRpc(URL_, { fetch: loggedFetch });
    const noKeys = { getAvailableKeys: async () => [], sign: async () => { throw new Error('no keys (by design)'); } };
    const api = new Api({ rpc, signatureProvider: noKeys, chainId: c.chain_id, textEncoder: new TextEncoder(), textDecoder: new TextDecoder() });
    const f = {};
    f.get_info = await run(async () => pickInfo(await rpc.get_info()));
    f.get_account = await run(async () => pickAccount(await rpc.get_account(c.account)));
    f.get_currency_balance = await run(() => rpc.get_currency_balance('eosio.token', c.account, c.symbol));
    f.get_table_rows = await run(async () => (await rpc.get_table_rows({ code: 'eosio.token', scope: c.account, table: 'accounts', json: true, limit: 10 })).rows);
    f.get_abi = await run(async () => ({ sha256: sha((await rpc.get_abi('eosio.token')).abi) }));
    f.get_raw_abi = await run(async () => {
      const r = typeof rpc.get_raw_abi === 'function' ? await rpc.get_raw_abi('eosio.token') : await rpc.fetch('/v1/chain/get_raw_abi', { account_name: 'eosio.token' });
      return { abi_hash: r.abi_hash, bytes: typeof r.abi === 'string' ? r.abi.length : r.abi && r.abi.length, via: typeof rpc.get_raw_abi === 'function' ? 'get_raw_abi' : 'rpc.fetch' };
    });
    const build = async (opts) => {
      const r = await api.transact({ actions: [transferAction(c)] }, { broadcast: false, sign: false, expireSeconds: 120, ...opts });
      const ser = r.serializedTransaction;
      return { body: bodyHex(ser), tapos: await taposCheck(taposOf(ser)) };
    };
    f.transfer_blocksBehind3 = await run(() => build({ blocksBehind: 3 }));
    f.transfer_useLastIrreversible = useLastIrreversible ? await run(() => build({ useLastIrreversible: true })) : { ok: false, error: 'not supported by this version', skipped: true };
    f.get_required_keys = await run(async () => {
      const t = await api.transact({ actions: [transferAction(c)] }, { broadcast: false, sign: false, blocksBehind: 3, expireSeconds: 120 });
      const trx = api.deserializeTransaction(t.serializedTransaction);
      return rpc.getRequiredKeys({ transaction: trx, availableKeys: [c.key] });
    });
    f.key_accounts = await run(async () => {
      const r = await (typeof rpc.history_get_key_accounts === 'function'
        ? new JsonRpc(protonRpc ? [HIST] : HIST, { fetch: loggedFetch }).history_get_key_accounts(c.key)
        : raw('/v1/history/get_key_accounts', { public_key: c.key }));
      return { via: 'history_get_key_accounts', account_names: (r.account_names || []).slice().sort() };
    });
    if (typeof rpc.get_accounts_by_authorizers === 'function') {
      f.accounts_by_authorizers = await run(async () => (await rpc.get_accounts_by_authorizers([], [c.key])).accounts);
    }
    return f;
  };
}

function eosjs16() {
  const Eos = require('eosjs16');
  return async (c) => {
    const eos = Eos({ httpEndpoint: URL_, chainId: c.chain_id, broadcast: false, sign: false, expireInSeconds: 120, verbose: false, logger: { log: null, error: null } });
    const f = {};
    f.get_info = await run(async () => pickInfo(await eos.getInfo({})));
    f.get_account = await run(async () => pickAccount(await eos.getAccount(c.account)));
    f.get_currency_balance = await run(() => eos.getCurrencyBalance('eosio.token', c.account, c.symbol));
    f.get_table_rows = await run(async () => (await eos.getTableRows({ code: 'eosio.token', scope: c.account, table: 'accounts', json: true, limit: 10 })).rows);
    f.get_abi = await run(async () => ({ sha256: sha((await eos.getAbi('eosio.token')).abi) }));
    f.get_raw_abi = { ok: false, error: 'not in eosjs 16', skipped: true };
    f.transfer_blocksBehind3 = await run(async () => {
      // eosjs 16 derives TAPOS from get_info (last irreversible) + get_block; there is no blocksBehind option.
      const r = await eos.transaction({ actions: [transferAction(c)] }, { broadcast: false, sign: false });
      const t = r.transaction.transaction;
      const { packTransaction } = require('../../../gateway/server.js');
      const ser = packTransaction({ ...t, context_free_actions: t.context_free_actions || [], transaction_extensions: t.transaction_extensions || [] });
      return { body: bodyHex(ser), tapos: await taposCheck(taposOf(ser)), note: 'eosjs16: TAPOS from get_info + get_block' };
    });
    f.transfer_useLastIrreversible = { ok: false, error: 'not in eosjs 16', skipped: true };
    f.get_required_keys = await run(async () => {
      const r = await eos.transaction({ actions: [transferAction(c)] }, { broadcast: false, sign: false });
      return eos.getRequiredKeys({ transaction: r.transaction.transaction, available_keys: [c.key] });
    });
    f.key_accounts = await run(async () => {
      const e2 = Eos({ httpEndpoint: HIST, chainId: c.chain_id, logger: { log: null, error: null } });
      const r = await e2.getKeyAccounts(c.key);
      return { via: 'getKeyAccounts (/v1/history)', account_names: (r.account_names || []).slice().sort() };
    });
    return f;
  };
}

function wharfAntelope(modName, { session = false } = {}) {
  const A = require(modName);
  const { APIClient, FetchProvider, Transaction, Action, Serializer, ABI, PublicKey, Name } = A;
  return async (c) => {
    const client = new APIClient({ provider: new FetchProvider(URL_, { fetch: loggedFetch }) });
    const hclient = new APIClient({ provider: new FetchProvider(HIST, { fetch: loggedFetch }) });
    const f = {};
    f.get_info = await run(async () => pickInfo(await client.v1.chain.get_info()));
    f.get_account = await run(async () => pickAccount(plain(await client.v1.chain.get_account(c.account))));
    f.get_currency_balance = await run(async () => (await client.v1.chain.get_currency_balance('eosio.token', c.account, c.symbol)).map(String));
    f.get_table_rows = await run(async () => (await client.v1.chain.get_table_rows({ code: 'eosio.token', scope: c.account, table: 'accounts', limit: 10 })).rows);
    f.get_abi = await run(async () => ({ sha256: sha(plain((await client.v1.chain.get_abi('eosio.token')).abi)) }));
    f.get_raw_abi = await run(async () => { const r = await client.v1.chain.get_raw_abi('eosio.token'); return { abi_hash: String(r.abi_hash), bytes: plain(r.abi).length }; });
    const build = async (useLib) => {
      const info = await client.v1.chain.get_info();
      // wharfkit's own helper: TAPOS from last_irreversible_block_id (useLib) or from head-3 via get_block_info
      let header;
      if (useLib) header = info.getTransactionHeader(120);
      else {
        const blk = await client.v1.chain.get_block_info(Number(info.head_block_num) - 3);
        header = { expiration: A.TimePointSec.fromMilliseconds(Date.now() + 120000), ref_block_num: Number(blk.block_num) & 0xffff, ref_block_prefix: blk.ref_block_prefix };
      }
      const { abi } = await client.v1.chain.get_abi('eosio.token');
      const tx = Transaction.from({ ...header, actions: [Action.from(transferAction(c), ABI.from(abi))] });
      const ser = Serializer.encode({ object: tx }).array;
      return { body: bodyHex(ser), tapos: await taposCheck(taposOf(ser)) };
    };
    f.transfer_blocksBehind3 = await run(() => build(false));
    f.transfer_useLastIrreversible = await run(() => build(true));
    f.get_required_keys = await run(async () => {
      const info = await client.v1.chain.get_info();
      const { abi } = await client.v1.chain.get_abi('eosio.token');
      const tx = Transaction.from({ ...info.getTransactionHeader(120), actions: [Action.from(transferAction(c), ABI.from(abi))] });
      // no typed helper in wharfkit: the generic call is what wallets built on it use
      const r = await client.call({ path: '/v1/chain/get_required_keys', params: { transaction: tx, available_keys: [PublicKey.from(c.key)] } });
      return (r.required_keys || []).map(String);
    });
    f.accounts_by_authorizers = await run(async () => plain(await client.v1.chain.get_accounts_by_authorizers({ keys: [PublicKey.from(c.key)] })).accounts);
    f.key_accounts = await run(async () => ({ via: 'v1.history.get_key_accounts', account_names: plain((await hclient.v1.history.get_key_accounts(PublicKey.from(c.key))).account_names).map(String).sort() }));
    if (session) {
      f.session_transact = await run(async () => {
        const { Session } = require('@wharfkit/session');
        const { WalletPluginPrivateKey } = require('@wharfkit/wallet-plugin-privatekey');
        const throwaway = A.PrivateKey.generate('K1'); // never funded, never authorized: its signature is worthless
        const s = new Session({ chain: { id: c.chain_id, url: URL_ }, actor: c.account, permission: 'active',
          walletPlugin: new WalletPluginPrivateKey(throwaway), fetch: loggedFetch });
        const r = await s.transact({ action: transferAction(c) }, { broadcast: false, expireSeconds: 120 });
        const ser = Serializer.encode({ object: r.resolved.transaction }).array;
        return { body: bodyHex(ser), tapos: await taposCheck(taposOf(ser)), signatures: r.signatures.length };
      });
    }
    return f;
  };
}

// ---- cleos in Docker (optional) ---------------------------------------------------------------------------------------------
function sh(cmd, args, ms) {
  return new Promise((resolve) => {
    const p = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    const t = setTimeout(() => { p.kill('SIGKILL'); resolve({ code: -1, out, err: err + `\n(timeout after ${ms} ms)` }); }, ms);
    p.stdout.on('data', (d) => { out += d; }); p.stderr.on('data', (d) => { err += d; });
    p.on('error', (e) => { clearTimeout(t); resolve({ code: -1, out, err: String(e.message) }); });
    p.on('close', (code) => { clearTimeout(t); resolve({ code, out, err }); });
  });
}
async function cleosRuns(c) {
  const images = opts('cleos-image'); // name=image, e.g. leap5=<your nodeos 5.0 image>
  if (!images.length) return { skipped: 'no --cleos-image given' };
  const v = await sh('docker', ['version', '--format', '{{.Server.Version}}'], 15000);
  if (v.code !== 0) return { skipped: `docker not available: ${(v.err || v.out).trim().slice(0, 200)}` };
  const res = {};
  for (const spec of images) {
    const [name, image] = spec.split('=');
    const cl = (args) => sh('docker', ['run', '--rm', '--entrypoint', 'cleos', image, '-u', URL_, ...args], 120000);
    const f = {};
    for (const [flow, args] of [['get_info', ['get', 'info']], ['get_account', ['get', 'account', c.account, '-j']],
      ['get_currency_balance', ['get', 'currency', 'balance', 'eosio.token', c.account, c.symbol]],
      ['get_table_rows', ['get', 'table', 'eosio.token', c.account, 'accounts']], ['get_abi', ['get', 'abi', 'eosio.token']],
      ['transfer_skip_sign', ['push', 'action', 'eosio.token', 'transfer', JSON.stringify(transferAction(c).data), '-p', `${c.account}@active`, '-s', '-d', '-j', '--expiration', '120']]]) {
      const r = await cl(args);
      let value = r.out.trim();
      try { value = JSON.parse(value); } catch { /* text output */ }
      if (flow === 'get_info' && value && value.chain_id) value = pickInfo(value);
      if (flow === 'get_account' && value && value.permissions) value = pickAccount(value);
      if (flow === 'get_abi' && value && value.abi) value = { sha256: sha(value.abi) };
      if (flow === 'transfer_skip_sign' && value && value.actions) value = { actions: value.actions.map((a) => a.data), ref_block_num: 'volatile' };
      f[flow] = r.code === 0 ? { ok: true, value } : { ok: false, error: (r.err || r.out).trim().slice(0, 300) };
    }
    res[name] = { image, flows: f };
  }
  return res;
}

// Libraries that import their own fetch (cross-fetch) get it replaced by the logged one, so the write guard and the
// rate limit cover them too.
function shimFetchFor(pkg) {
  const r2 = createRequire(require.resolve(pkg));
  for (const dep of ['cross-fetch', 'node-fetch', 'isomorphic-fetch']) {
    let path; try { path = r2.resolve(dep); } catch { continue; }
    const f = Object.assign((...a) => loggedFetch(...a), { Headers, Request, Response });
    f.default = f; f.fetch = f;
    require.cache[path] = { id: path, filename: path, loaded: true, exports: f };
  }
  return require(pkg);
}

// ---- main ------------------------------------------------------------------------------------------------------------------------
const LIBS = {
  eosjs16: () => eosjs16(),
  eosjs20: () => eosjsLike(shimFetchFor('eosjs20'), 'eosjs20'),
  eosjs21: () => eosjsLike(shimFetchFor('eosjs21'), 'eosjs21', { useLastIrreversible: true }),
  eosjs22: () => eosjsLike(shimFetchFor('eosjs22'), 'eosjs22', { useLastIrreversible: true }),
  protonjs22: () => eosjsLike(shimFetchFor('protonjs22'), 'protonjs22', { useLastIrreversible: true, protonRpc: true }),
  protonjs26: () => eosjsLike(shimFetchFor('protonjs26'), 'protonjs26', { useLastIrreversible: true, protonRpc: true }),
  protonjs30: () => eosjsLike(shimFetchFor('protonjs30'), 'protonjs30', { useLastIrreversible: true, protonRpc: true }),
  wharfantelope1: () => wharfAntelope('wharfantelope1'),
  wharfkit4: () => wharfAntelope('@wharfkit/antelope', { session: true }),
};
const version = (m) => { try { return require(`${m}/package.json`).version; } catch { return '?'; } };
const PKG = { eosjs16: 'eosjs16', eosjs20: 'eosjs20', eosjs21: 'eosjs21', eosjs22: 'eosjs22', protonjs22: 'protonjs22', protonjs26: 'protonjs26', protonjs30: 'protonjs30',
  wharfantelope1: 'wharfantelope1', wharfkit4: '@wharfkit/antelope' };

const record = { url: URL_, history: HIST, started: new Date().toISOString(), libs: {} };
const c = await context();
record.context = { account: c.account, key: c.key, symbol: c.symbol, chain_id: c.chain_id };
log(`endpoint ${URL_}: account ${c.account}, ${c.quantity} self-transfer (never broadcast)`);
for (const [name, mk] of Object.entries(LIBS)) {
  if (ONLY && !ONLY.has(name)) continue;
  let flows;
  try { flows = await (mk())(c); } catch (e) { flows = { load: { ok: false, error: String(e.stack || e).slice(0, 400) } }; }
  record.libs[name] = { version: version(PKG[name]), flows };
  const bad = Object.entries(flows).filter(([, v]) => !v.ok && !v.skipped).map(([k]) => k);
  log(`${name}@${record.libs[name].version}: ${Object.keys(flows).length} flows${bad.length ? `, failed: ${bad.join(', ')}` : ''}`);
}
record.cleos = await cleosRuns(c);
if (record.cleos.skipped) log(`cleos: skipped (${record.cleos.skipped})`);
record.finished = new Date().toISOString();
writeFileSync(OUT, JSON.stringify(record, null, 1));
log(`-> ${OUT}`);
