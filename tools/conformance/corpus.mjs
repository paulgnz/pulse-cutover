#!/usr/bin/env node
// corpus.mjs — build a /v1 request corpus FROM a live Leap chain (read-only).
//
//   node tools/conformance/corpus.mjs <leap-url> [--hyperion <url>] [--out corpus.jsonl] [--rps 4]
//        [--coverage docs/V1-COVERAGE.md] [--holders 8] [--contracts 5]
//
// Samples accounts (system accounts, top token holders, producers, msig proposers), contracts (system +
// dapp contracts discovered from code-bearing accounts), keys (from get_account permissions) and recent
// blocks, then emits requests for EVERY endpoint in docs/V1-COVERAGE.md in the styles real clients use
// (numeric vs string numbers, id vs num, json true/false, named index positions, key spellings, bounds,
// string limits, empty bodies, GET, text/plain, trailing slashes) plus the error cases clients match on.
// Write endpoints only ever get requests that cannot land (empty/malformed bodies, an unsigned transaction
// that expired in 2020).
//
// Each line: {id, method, path, headers, body, tags:[endpoint, style], volatile:[paths], live?, ignore?,
// unordered?, expect_b?}. See README.md for the path syntax and what each field means.
// No dependencies (Node >= 18).
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { post, setRate, parseCoverage, keyInfo, pubSpelling, legacySpelling, nameToU64, packTransaction } from './lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const opt = (k, d) => { const i = argv.indexOf(`--${k}`); return i >= 0 ? argv[i + 1] : d; };
const SRC = argv[0] && !argv[0].startsWith('--') ? argv[0].replace(/\/+$/, '') : null;
if (!SRC) { console.error('usage: corpus.mjs <leap-url> [--hyperion <url>] [--out corpus.jsonl] [--rps 4]'); process.exit(2); }
const HYP = opt('hyperion') ? opt('hyperion').replace(/\/+$/, '') : null;
const OUT = opt('out', 'corpus.jsonl');
const N_HOLDERS = Number(opt('holders', 8)), N_CONTRACTS = Number(opt('contracts', 5));
setRate(opt('rps', 4));
const coverage = parseCoverage(readFileSync(opt('coverage', join(HERE, '../../docs/V1-COVERAGE.md')), 'utf8'));

const log = (...a) => console.error('[corpus]', ...a);
const chain = (ep, body) => post(SRC, `/v1/chain/${ep}`, body);
const tryChain = async (ep, body) => { try { return await chain(ep, body); } catch { return null; } };

// ---- volatile paths per endpoint (legitimately differ between two honest endpoints) -------------------------
const HEAD = ['head_block_num', 'head_block_id', 'head_block_time', 'head_block_producer', 'last_irreversible_block_num',
  'last_irreversible_block_id', 'last_irreversible_block_time', 'fork_db_head_block_num', 'fork_db_head_block_id'];
const V = {
  get_info: { volatile: [...HEAD, 'server_version', 'server_version_string', 'server_full_version_string', 'virtual_block_cpu_limit',
    'virtual_block_net_limit', 'earliest_available_block_num'], live: ['total_cpu_weight', 'total_net_weight'], ignore: ['pulsevm_head_block_time'] },
  // ram_usage / cpu / net: resource billing, not ledger state (differs by design between Leap and PulseVM)
  get_account: { volatile: ['head_block_num', 'head_block_time', 'cpu_limit', 'net_limit', 'subjective_cpu_bill_limit', 'ram_usage'],
    live: ['core_liquid_balance', 'total_resources', 'self_delegated_bandwidth', 'refund_request', 'voter_info', 'rex_info', 'ram_quota', 'net_weight', 'cpu_weight'] },
  get_producers: { live: ['rows.*.unpaid_blocks', 'rows.*.last_claim_time', 'rows.*.total_votes', 'total_producer_vote_weight', 'rows.*.lifetime_produce_blocks_num'] },
  get_producers_hex: { live: ['rows', 'total_producer_vote_weight', 'more'] },
  get_currency_balance: { live: ['$'] },
  get_currency_stats: { live: ['*.supply'] },
  get_table_by_scope: { live: ['rows.*.count', 'more'] },
  get_scheduled_transactions: { live: ['transactions', 'more'] },
  get_block_header_state: { volatile: ['dpos_proposed_irreversible_blocknum', 'dpos_irreversible_blocknum', 'bft_irreversible_blocknum'] },
  get_supported_apis: { unordered: ['apis'] },
  history: { ignore: ['query_time_ms', 'cached', 'lib', 'last_irreversible_block', 'last_indexed_block', 'last_indexed_block_time', 'hot_only', 'total.relation', 'cache_expires_in'] },
};
const MOVING_TABLES = { // tables whose rows change every block on a live chain
  'eosio/global': ['rows'], 'eosio/global2': ['rows'], 'eosio/global3': ['rows'], 'eosio/global4': ['rows'], 'eosio/producers': ['rows', 'more', 'next_key'],
  'eosio/rammarket': ['rows'], 'eosio/voters': ['rows'], 'eosio/userres': ['rows'], 'eosio/delband': ['rows'], 'eosio.token/stat': ['rows'], 'eosio.token/accounts': ['rows'],
};

const lines = [];
const counters = {};
function add(endpoint, style, { method = 'POST', body, headers, path, group = coverage.get(endpoint)?.group || 'chain', extra = {} } = {}) {
  const base = V[endpoint] || {};
  const hist = group === 'history' ? V.history : {};
  const n = (counters[endpoint] = (counters[endpoint] || 0) + 1);
  const rawBody = body === undefined ? '{}' : body === null ? null : typeof body === 'string' ? body : JSON.stringify(body);
  const line = {
    id: `${endpoint}#${String(n).padStart(3, '0')}-${style}`,
    method, path: path || `/v1/${group === 'history' ? 'history' : 'chain'}/${endpoint}`,
    headers: headers || (method === 'GET' || method === 'OPTIONS' ? {} : { 'content-type': 'application/json' }),
    body: method === 'GET' || method === 'OPTIONS' ? null : rawBody,
    tags: [endpoint, style, group],
    volatile: [...(base.volatile || []), ...(hist.volatile || []), ...(extra.volatile || [])],
  };
  for (const k of ['live', 'ignore', 'unordered', 'mask']) { const v = [...(base[k] || []), ...(hist[k] || []), ...(extra[k] || [])]; if (v.length) line[k] = v; }
  for (const k of ['expect_b', 'compare_headers', 'cors', 'dynamic', 'tx_template']) if (extra[k]) line[k] = extra[k];
  if (extra.note) line.note = extra.note;
  lines.push(line);
}
const errorStyles = (endpoint, extra) => {
  add(endpoint, 'err-malformed-json', { body: '{"account_name": ', extra });
  add(endpoint, 'err-json-array', { body: '[1,2,3]', extra });
};

// ---- serialization for synthetic transactions -------------------------------------------------------------------------
const u64le = (v) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt.asUintN(64, BigInt(v))); return b; };
function symbolRaw(precision, code) { const b = Buffer.alloc(8); b[0] = precision; Buffer.from(code, 'ascii').copy(b, 1); return b; }
function assetHex(q) { // "1.0000 XPR"
  const [amt, code] = q.split(' ');
  const [i, f = ''] = amt.split('.');
  return Buffer.concat([u64le(BigInt(i + f)), symbolRaw(f.length, code)]).toString('hex');
}
const strHex = (s) => { const b = Buffer.from(s, 'utf8'); return Buffer.concat([Buffer.from([b.length]), b]).toString('hex'); };
const transferHex = (from, to, qty, memo) => u64le(nameToU64(from)).toString('hex') + u64le(nameToU64(to)).toString('hex') + assetHex(qty) + strHex(memo);
const symCodeRaw = (code) => { let v = 0n; for (let i = code.length - 1; i >= 0; i--) v = (v << 8n) | BigInt(code.charCodeAt(i)); return v; };

function transferTx(ref, from, to, qty, memo, { hex = true, expiration = '2020-01-01T00:00:00' } = {}) {
  const data = hex ? transferHex(from, to, qty, memo) : { from, to, quantity: qty, memo };
  return { expiration, ref_block_num: ref.num & 0xffff, ref_block_prefix: Buffer.from(ref.id.slice(16, 24), 'hex').readUInt32LE(0),
    max_net_usage_words: 0, max_cpu_usage_ms: 0, delay_sec: 0, context_free_actions: [],
    actions: [{ account: 'eosio.token', name: 'transfer', authorization: [{ actor: from, permission: 'active' }], data }], transaction_extensions: [] };
}

// ============================================================================================================================
async function main() {
  const info = await chain('get_info', {});
  log(`source ${SRC}: chain ${info.chain_id.slice(0, 12)}… head ${info.head_block_num} lib ${info.last_irreversible_block_num} (${info.server_version_string || info.server_version})`);
  const lib = info.last_irreversible_block_num, head = info.head_block_num;

  // --- accounts ---------------------------------------------------------------------------------------------------------
  const SYSTEM = ['eosio', 'eosio.token', 'eosio.msig', 'eosio.ram', 'eosio.ramfee', 'eosio.stake', 'eosio.names', 'eosio.bpay', 'eosio.vpay',
    'eosio.saving', 'eosio.rex', 'eosio.wrap', 'eosio.prods', 'eosio.null', 'eosio.proton'];
  const accounts = new Map(); // name -> get_account result (sampled)
  for (const a of SYSTEM) { const r = await tryChain('get_account', { account_name: a }); if (r) accounts.set(a, r); }
  const system = [...accounts.keys()];
  const eosioAcct = accounts.get('eosio');
  const core = (() => { for (const r of accounts.values()) { const w = r.total_resources && r.total_resources.cpu_weight; if (typeof w === 'string' && w.includes(' ')) return w.split(' ')[1]; } return 'XPR'; })();
  log(`system accounts: ${system.length}, core symbol ${core}`);

  const prods = await chain('get_producers', { json: true, limit: 30 });
  const producers = (prods.rows || []).map((r) => r.owner);

  // token holders: sample eosio.token accounts scopes across the name space, rank by core balance
  const scopes = new Set();
  for (const lb of ['', 'a', 'e', 'i', 'm', 'p', 's', 'x']) {
    const r = await tryChain('get_table_by_scope', { code: 'eosio.token', table: 'accounts', lower_bound: lb, limit: 8 });
    (r && r.rows || []).forEach((x) => scopes.add(x.scope));
  }
  const balances = [], symbols = new Set([core]);
  for (const s of [...scopes].slice(0, 64)) {
    const r = await tryChain('get_currency_balance', { code: 'eosio.token', account: s });
    if (!Array.isArray(r)) continue;
    r.forEach((b) => symbols.add(b.split(' ')[1]));
    balances.push({ account: s, bal: r, n: r.length });
  }
  // rank by the most widely held symbol (the liquid token; the resource core symbol may be held by nobody)
  const freq = {}; balances.forEach((b) => b.bal.forEach((x) => { const s = x.split(' ')[1]; freq[s] = (freq[s] || 0) + 1; }));
  const rankSym = Object.keys(freq).sort((x, y) => freq[y] - freq[x])[0] || core;
  balances.forEach((b) => { const c = b.bal.find((x) => x.endsWith(' ' + rankSym)); b.core = c ? parseFloat(c) : 0; });
  balances.sort((x, y) => y.core - x.core || y.n - x.n);
  const holders = balances.slice(0, N_HOLDERS).map((x) => x.account);
  const multiToken = balances.find((x) => x.n > 1);
  if (multiToken && !holders.includes(multiToken.account)) holders.push(multiToken.account);
  const fund = balances.find((x) => x.core >= 1) || balances[0];
  log(`holders (by ${rankSym}): ${holders.join(', ')}; symbols: ${[...symbols].join(', ')}`);
  for (const h of [...holders, ...producers.slice(0, 4)]) if (!accounts.has(h)) { const r = await tryChain('get_account', { account_name: h }); if (r) accounts.set(h, r); }

  // msig proposals
  const msig = [];
  const pscopes = await tryChain('get_table_by_scope', { code: 'eosio.msig', table: 'proposal', limit: 10 });
  for (const s of (pscopes && pscopes.rows || []).slice(0, 3)) {
    const r = await tryChain('get_table_rows', { code: 'eosio.msig', scope: s.scope, table: 'proposal', json: true, limit: 2 });
    for (const row of (r && r.rows || [])) msig.push({ proposer: s.scope, proposal_name: row.proposal_name });
  }
  log(`msig proposals: ${msig.length}`);

  // contracts: system + dapps discovered among code-bearing accounts
  const KNOWN_DAPPS = ['atomicassets', 'atomicmarket', 'xtokens', 'proton.wrap', 'proton.swaps', 'dex', 'oracles', 'eosio.proton', 'loan.token', 'lending.loan',
    'token.proton', 'snipx', 'metalx', 'xpr.oracle', 'otc.p', 'lock.token', 'swap.alcor', 'alcor', 'freeosgov', 'nft.proton'];
  const zero = '0'.repeat(64);
  const codeHash = new Map();
  for (const a of [...new Set([...system, ...KNOWN_DAPPS, ...holders, ...producers.slice(0, 10), ...[...scopes].slice(0, 30)])]) {
    const r = await tryChain('get_code_hash', { account_name: a });
    if (r && r.code_hash && r.code_hash !== zero) codeHash.set(a, r.code_hash);
  }
  const sysContracts = system.filter((a) => codeHash.has(a));
  const dapps = [...codeHash.keys()].filter((a) => !system.includes(a)).sort((x, y) => (KNOWN_DAPPS.includes(y) - KNOWN_DAPPS.includes(x)) || (x < y ? -1 : 1)).slice(0, N_CONTRACTS);
  const contractTables = []; // {code, table, scope}
  for (const c of [...dapps, 'eosio.token', 'eosio']) {
    const abi = await tryChain('get_abi', { account_name: c });
    const tables = (abi && abi.abi && abi.abi.tables || []).map((t) => t.name);
    for (const t of tables.slice(0, c === 'eosio' || c === 'eosio.token' ? 0 : 2)) {
      const s = await tryChain('get_table_by_scope', { code: c, table: t, limit: 1 });
      if (s && s.rows && s.rows[0]) contractTables.push({ code: c, table: t, scope: s.rows[0].scope });
    }
  }
  log(`contracts: system ${sysContracts.join(',')}; dapps ${dapps.join(',') || '(none)'}; dapp tables ${contractTables.length}`);

  // keys from permissions; an account-authorized permission for accounts mode
  const keyAcct = []; let acctAuth = null;
  for (const [name, a] of accounts) {
    for (const p of a.permissions || []) {
      for (const k of p.required_auth.keys || []) if (keyAcct.length < 6 && !keyAcct.find((x) => x.key === k.key)) keyAcct.push({ key: k.key, account: name, perm: p.perm_name });
      if (!acctAuth && (p.required_auth.accounts || []).length) acctAuth = p.required_auth.accounts[0].permission;
    }
  }
  const spell = (k) => { const i = keyInfo(k); return i ? { pub: pubSpelling(i), eos: legacySpelling(i) } : { pub: k, eos: k }; };
  const signer = keyAcct.find((x) => x.perm === 'active' && holders.includes(x.account)) || keyAcct.find((x) => x.perm === 'active') || keyAcct[0];
  log(`keys: ${keyAcct.length}; signer account ${signer && signer.account}`);

  // blocks: irreversible, deep, early (node config: partial block logs), reversible, and one carrying transactions
  const libBlock = await chain('get_block', { block_num_or_id: lib - 10 });
  const deepBlock = await chain('get_block', { block_num_or_id: lib - 1000 });
  let txBlock = null;
  const trxIds = [];
  if (HYP) {
    try {
      const r = await post(HYP, '/v1/history/get_actions', { account_name: 'eosio.token', pos: -1, offset: -5 });
      for (const a of r.actions || []) if (a.block_num <= lib && !trxIds.includes(a.action_trace.trx_id)) { trxIds.push(a.action_trace.trx_id); txBlock = txBlock || a.block_num; }
    } catch (e) { log(`hyperion get_actions: ${e.message}`); }
  }
  if (!txBlock) {
    for (let n = lib - 1; n > lib - 40; n--) {
      const b = await tryChain('get_block', { block_num_or_id: n });
      if (b && b.transactions && b.transactions.length) { txBlock = n; b.transactions.forEach((t) => typeof t.trx === 'object' && trxIds.push(t.trx.id)); break; }
    }
  }
  const tb = txBlock ? await chain('get_block', { block_num_or_id: txBlock }) : null;
  if (tb) for (const t of tb.transactions) if (typeof t.trx === 'object' && !trxIds.includes(t.trx.id)) trxIds.push(t.trx.id);
  const reversible = head - 20 > lib ? head - 20 : head;
  log(`blocks: lib-10=${libBlock.block_num} deep=${deepBlock.block_num} tx=${txBlock} reversible=${reversible}; trx ids ${trxIds.length}`);

  const ref = { num: libBlock.block_num, id: libBlock.id };
  const from = (signer && signer.account) || (fund && fund.account) || 'eosio';
  const coreQty = (() => { const w = eosioAcct && eosioAcct.total_resources && eosioAcct.total_resources.cpu_weight; const p = w ? (w.split(' ')[0].split('.')[1] || '').length : 4; return `${(1).toFixed(p)} ${core}`; })();

  // =========================================================================================================================
  // /v1/chain
  // =========================================================================================================================
  add('get_info', 'post-empty-object', { body: {} });
  add('get_info', 'empty-body', { body: '' });
  add('get_info', 'GET', { method: 'GET' });
  add('get_info', 'text-plain', { body: '{}', headers: { 'content-type': 'text/plain;charset=UTF-8' } });
  add('get_info', 'trailing-slash', { body: '{}', path: '/v1/chain/get_info/' });
  add('get_info', 'GET-trailing-slash', { method: 'GET', path: '/v1/chain/get_info/' });
  add('get_info', 'no-content-type', { body: '{}', headers: {} });

  // get_account
  for (const a of accounts.keys()) add('get_account', 'name', { body: { account_name: a } });
  add('get_account', 'text-plain', { body: { account_name: 'eosio' }, headers: { 'content-type': 'text/plain;charset=UTF-8' } });
  add('get_account', 'trailing-slash', { body: { account_name: 'eosio' }, path: '/v1/chain/get_account/' });
  add('get_account', 'expected-core-symbol', { body: { account_name: holders[0] || 'eosio', expected_core_symbol: `4,${core}` } });
  add('get_account', 'err-unknown-account', { body: { account_name: 'conform.none' } });
  add('get_account', 'err-invalid-name', { body: { account_name: 'Bad.Name!' } });
  add('get_account', 'err-missing-field', { body: {} });
  add('get_account', 'err-empty-body', { body: '' });
  add('get_account', 'err-GET', { method: 'GET' });
  errorStyles('get_account');

  // get_block / get_block_info / get_block_header / get_raw_block
  const blocks = [libBlock, deepBlock, ...(tb ? [tb] : [])];
  for (const b of blocks) {
    add('get_block', 'num-number', { body: { block_num_or_id: b.block_num } });
    add('get_block', 'num-string', { body: { block_num_or_id: String(b.block_num) } });
    add('get_block', 'id', { body: { block_num_or_id: b.id } });
  }
  add('get_block', 'early-block-2', { body: { block_num_or_id: 2 }, extra: { note: 'depends on the node\'s block log (earliest_available_block_num)' } });
  add('get_block', 'id-uppercase', { body: { block_num_or_id: libBlock.id.toUpperCase() } });
  add('get_block', 'err-future', { body: { block_num_or_id: head + 50_000_000 } });
  add('get_block', 'err-bad-id', { body: { block_num_or_id: 'zz' + '0'.repeat(62) } });
  add('get_block', 'err-negative', { body: { block_num_or_id: -1 } });
  add('get_block', 'err-missing-field', { body: {} });
  errorStyles('get_block');
  for (const b of blocks) {
    add('get_block_info', 'num-number', { body: { block_num: b.block_num } });
    add('get_block_info', 'num-string', { body: { block_num: String(b.block_num) } });
  }
  add('get_block_info', 'err-id-instead-of-num', { body: { block_num: libBlock.id } });
  add('get_block_info', 'err-future', { body: { block_num: head + 50_000_000 } });
  add('get_block_info', 'err-missing-field', { body: {} });
  for (const b of blocks.slice(0, 2)) {
    add('get_block_header', 'num-number', { body: { block_num_or_id: b.block_num } });
    add('get_block_header', 'id', { body: { block_num_or_id: b.id } });
    add('get_block_header', 'include-extensions', { body: { block_num_or_id: b.block_num, include_extensions: true } });
  }
  add('get_block_header', 'err-future', { body: { block_num_or_id: head + 50_000_000 } });
  for (const b of blocks.slice(0, 2)) {
    add('get_raw_block', 'num-number', { body: { block_num_or_id: b.block_num } });
    add('get_raw_block', 'num-string', { body: { block_num_or_id: String(b.block_num) } });
    add('get_raw_block', 'id', { body: { block_num_or_id: b.id } });
  }
  add('get_raw_block', 'err-future', { body: { block_num_or_id: head + 50_000_000 } });
  add('get_block_header_state', 'reversible-num', { body: { block_num_or_id: reversible }, extra: { note: 'reversible at corpus time; may be irreversible (and gone from the fork db) by run time on both sides' } });
  add('get_block_header_state', 'reversible-num-string', { body: { block_num_or_id: String(reversible) } });
  add('get_block_header_state', 'irreversible', { body: { block_num_or_id: deepBlock.block_num } });
  add('get_block_header_state', 'err-future', { body: { block_num_or_id: head + 50_000_000 } });

  // abi / code
  const contracts = [...sysContracts, ...dapps];
  for (const c of contracts) {
    add('get_abi', 'name', { body: { account_name: c } });
    add('get_raw_abi', 'name', { body: { account_name: c } });
    add('get_code_hash', 'name', { body: { account_name: c } });
  }
  const noCode = holders.find((h) => !codeHash.has(h)) || 'eosio.null';
  add('get_abi', 'no-contract', { body: { account_name: noCode } });
  add('get_abi', 'err-unknown-account', { body: { account_name: 'conform.none' } });
  add('get_abi', 'text-plain', { body: { account_name: 'eosio.token' }, headers: { 'content-type': 'text/plain;charset=UTF-8' } });
  add('get_raw_abi', 'no-contract', { body: { account_name: noCode } });
  add('get_raw_abi', 'err-unknown-account', { body: { account_name: 'conform.none' } });
  const tokenAbi = await tryChain('get_raw_abi', { account_name: 'eosio.token' });
  if (tokenAbi) add('get_raw_abi', 'abi-hash-matches', { body: { account_name: 'eosio.token', abi_hash: tokenAbi.abi_hash } });
  add('get_code_hash', 'no-contract', { body: { account_name: noCode } });
  add('get_code_hash', 'err-unknown-account', { body: { account_name: 'conform.none' } });
  for (const c of ['eosio.token', dapps[0]].filter(Boolean)) {
    add('get_raw_code_and_abi', 'name', { body: { account_name: c } });
    add('get_code', 'code_as_wasm-true', { body: { account_name: c, code_as_wasm: true } });
  }
  add('get_code', 'code_as_wasm-1', { body: { account_name: 'eosio.token', code_as_wasm: 1 } });
  add('get_code', 'no-contract', { body: { account_name: noCode, code_as_wasm: true } });
  add('get_code', 'err-unknown-account', { body: { account_name: 'conform.none', code_as_wasm: true } });
  add('get_raw_code_and_abi', 'no-contract', { body: { account_name: noCode } });
  add('get_raw_code_and_abi', 'err-unknown-account', { body: { account_name: 'conform.none' } });

  // get_table_rows
  const tr = (style, body, t = `${body.code}/${body.table}`) => add('get_table_rows', style, { body, extra: { live: MOVING_TABLES[t] || [] } });
  const h0 = holders[0] || 'eosio';
  tr('json-true', { code: 'eosio.token', scope: h0, table: 'accounts', json: true });
  tr('json-false', { code: 'eosio.token', scope: h0, table: 'accounts', json: false });
  tr('json-string-true', { code: 'eosio.token', scope: h0, table: 'accounts', json: 'true' });
  tr('json-omitted', { code: 'eosio.token', scope: h0, table: 'accounts' });
  tr('limit-string', { code: 'eosio.token', scope: h0, table: 'accounts', json: true, limit: '10' });
  tr('show-payer', { code: 'eosio.token', scope: h0, table: 'accounts', json: true, show_payer: true });
  tr('show-payer-hex', { code: 'eosio.token', scope: h0, table: 'accounts', json: false, show_payer: true });
  if (multiToken) {
    tr('reverse', { code: 'eosio.token', scope: multiToken.account, table: 'accounts', json: true, reverse: true });
    tr('reverse-string', { code: 'eosio.token', scope: multiToken.account, table: 'accounts', json: true, reverse: 'true' });
    tr('limit-1-more', { code: 'eosio.token', scope: multiToken.account, table: 'accounts', json: true, limit: 1 });
  }
  const coreRaw = symCodeRaw(core);
  tr('lower-bound-number', { code: 'eosio.token', scope: h0, table: 'accounts', json: true, lower_bound: Number(coreRaw) });
  tr('lower-bound-numeric-string', { code: 'eosio.token', scope: h0, table: 'accounts', json: true, lower_bound: String(coreRaw), upper_bound: String(coreRaw) });
  tr('lower-bound-symbol-code', { code: 'eosio.token', scope: h0, table: 'accounts', json: true, lower_bound: core, upper_bound: core });
  tr('stat-scope-symbol', { code: 'eosio.token', scope: core, table: 'stat', json: true });
  tr('stat-scope-numeric', { code: 'eosio.token', scope: String(coreRaw), table: 'stat', json: true });
  tr('global', { code: 'eosio', scope: 'eosio', table: 'global', json: true });
  tr('producers-primary-limit', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, limit: 5 });
  if (producers[2]) tr('producers-name-bounds', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, lower_bound: producers[2], upper_bound: producers[2], key_type: 'name' });
  if (producers[2]) tr('producers-i64-bound', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, lower_bound: nameToU64(producers[2]).toString(), limit: 2, key_type: 'i64' });
  tr('producers-idx-2-number', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: 2, key_type: 'float64', limit: 5 });
  tr('producers-idx-2-string', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: '2', key_type: 'float64', limit: 5 });
  tr('producers-idx-secondary', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: 'secondary', key_type: 'float64', limit: 5 });
  tr('producers-idx-2-reverse', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: 2, key_type: 'float64', limit: 3, reverse: true });
  tr('producers-idx-2-encode-dec', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: 2, key_type: 'float64', encode_type: 'dec', limit: 2 });
  tr('producers-idx-2-hex', { code: 'eosio', scope: 'eosio', table: 'producers', json: false, index_position: 2, key_type: 'float64', limit: 2 });
  tr('userres', { code: 'eosio', scope: h0, table: 'userres', json: true });
  tr('delband', { code: 'eosio', scope: h0, table: 'delband', json: true });
  tr('voters-bound', { code: 'eosio', scope: 'eosio', table: 'voters', json: true, lower_bound: h0, limit: 1 });
  for (const p of msig.slice(0, 2)) {
    tr('msig-proposal', { code: 'eosio.msig', scope: p.proposer, table: 'proposal', json: true, limit: 2 });
    tr('msig-approvals2', { code: 'eosio.msig', scope: p.proposer, table: 'approvals2', json: true, lower_bound: p.proposal_name, limit: 1 });
  }
  for (const t of contractTables) {
    tr(`dapp-${t.code}-${t.table}`, { code: t.code, scope: t.scope, table: t.table, json: true, limit: 3 });
    tr(`dapp-${t.code}-${t.table}-hex`, { code: t.code, scope: t.scope, table: t.table, json: false, limit: 3 });
  }
  tr('err-unknown-table', { code: 'eosio.token', scope: h0, table: 'nosuchtable', json: true });
  tr('err-unknown-code', { code: 'conform.none', scope: 'conform.none', table: 'accounts', json: true });
  tr('err-index-99', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: 99, key_type: 'i64' });
  tr('err-bad-key-type', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: 2, key_type: 'foo' });
  tr('err-bad-index-name', { code: 'eosio', scope: 'eosio', table: 'producers', json: true, index_position: 'nonsense', key_type: 'float64' });
  tr('err-missing-table', { code: 'eosio.token', scope: h0, json: true });
  tr('err-limit-negative', { code: 'eosio.token', scope: h0, table: 'accounts', json: true, limit: -1 });
  add('get_table_rows', 'text-plain', { body: { code: 'eosio.token', scope: h0, table: 'accounts', json: true }, headers: { 'content-type': 'text/plain;charset=UTF-8' } });
  add('get_table_rows', 'trailing-slash', { body: { code: 'eosio.token', scope: h0, table: 'accounts', json: true }, path: '/v1/chain/get_table_rows/' });
  add('get_table_rows', 'err-empty-body', { body: '' });
  errorStyles('get_table_rows');

  // get_table_by_scope
  add('get_table_by_scope', 'limit-number', { body: { code: 'eosio.token', table: 'accounts', limit: 5 } });
  add('get_table_by_scope', 'limit-string', { body: { code: 'eosio.token', table: 'accounts', limit: '5' } });
  add('get_table_by_scope', 'lower-bound', { body: { code: 'eosio.token', table: 'accounts', lower_bound: h0, limit: 3 } });
  add('get_table_by_scope', 'bounds', { body: { code: 'eosio.token', table: 'accounts', lower_bound: 'a', upper_bound: 'b', limit: 5 } });
  add('get_table_by_scope', 'reverse', { body: { code: 'eosio.token', table: 'accounts', limit: 3, reverse: true } });
  add('get_table_by_scope', 'reverse-string', { body: { code: 'eosio.token', table: 'accounts', limit: 3, reverse: 'true' } });
  add('get_table_by_scope', 'no-table-filter', { body: { code: 'eosio.token', limit: 5 } });
  add('get_table_by_scope', 'stat', { body: { code: 'eosio.token', table: 'stat', limit: 10 } });
  add('get_table_by_scope', 'msig', { body: { code: 'eosio.msig', table: 'proposal', limit: 5 } });
  add('get_table_by_scope', 'no-code', { body: { code: noCode, limit: 5 } });
  add('get_table_by_scope', 'err-unknown-code', { body: { code: 'conform.none', limit: 5 } });
  add('get_table_by_scope', 'err-missing-code', { body: { limit: 5 } });
  add('get_table_by_scope', 'text-plain', { body: { code: 'eosio.token', table: 'accounts', limit: 2 }, headers: { 'content-type': 'text/plain;charset=UTF-8' } });
  errorStyles('get_table_by_scope');

  // balances / stats
  for (const h of holders) add('get_currency_balance', 'no-symbol', { body: { code: 'eosio.token', account: h } });
  add('get_currency_balance', 'symbol', { body: { code: 'eosio.token', account: h0, symbol: core } });
  add('get_currency_balance', 'symbol-lowercase', { body: { code: 'eosio.token', account: h0, symbol: core.toLowerCase() } });
  add('get_currency_balance', 'symbol-unknown', { body: { code: 'eosio.token', account: h0, symbol: 'ZZZQ' } });
  add('get_currency_balance', 'no-balance-account', { body: { code: 'eosio.token', account: 'eosio.null' } });
  add('get_currency_balance', 'err-unknown-code', { body: { code: 'conform.none', account: h0 } });
  add('get_currency_balance', 'err-unknown-account', { body: { code: 'eosio.token', account: 'conform.none' } });
  add('get_currency_balance', 'err-missing-account', { body: { code: 'eosio.token' } });
  add('get_currency_balance', 'text-plain', { body: { code: 'eosio.token', account: h0 }, headers: { 'content-type': 'text/plain;charset=UTF-8' } });
  for (const s of [...symbols].slice(0, 6)) add('get_currency_stats', 'symbol', { body: { code: 'eosio.token', symbol: s } });
  add('get_currency_stats', 'symbol-lowercase', { body: { code: 'eosio.token', symbol: core.toLowerCase() } });
  add('get_currency_stats', 'symbol-unknown', { body: { code: 'eosio.token', symbol: 'ZZZQ' } });
  add('get_currency_stats', 'err-unknown-code', { body: { code: 'conform.none', symbol: core } });
  add('get_currency_stats', 'err-missing-symbol', { body: { code: 'eosio.token' } });

  // producers / schedule / features / params / deferred
  const P = { live: V.get_producers.live }, PH = { live: V.get_producers_hex.live };
  add('get_producers', 'json-true-limit-number', { body: { json: true, limit: 5 }, extra: P });
  add('get_producers', 'json-true-limit-string', { body: { json: true, limit: '5' }, extra: P });
  add('get_producers', 'json-string-true', { body: { json: 'true', limit: 5 }, extra: P });
  add('get_producers', 'json-false', { body: { json: false, limit: 3 }, extra: PH });
  add('get_producers', 'limit-100', { body: { json: true, limit: 100 }, extra: P });
  if (producers[3]) add('get_producers', 'lower-bound', { body: { json: true, limit: 3, lower_bound: producers[3] }, extra: P });
  add('get_producers', 'empty-object', { body: {}, extra: PH });
  add('get_producers', 'GET', { method: 'GET', extra: PH });
  add('get_producer_schedule', 'empty-object', { body: {} });
  add('get_producer_schedule', 'GET', { method: 'GET' });
  add('get_producer_schedule', 'empty-body', { body: '' });
  add('get_activated_protocol_features', 'empty-object', { body: {} });
  add('get_activated_protocol_features', 'GET', { method: 'GET' });
  add('get_activated_protocol_features', 'limit-number', { body: { limit: 5 } });
  add('get_activated_protocol_features', 'limit-string', { body: { limit: '5' } });
  add('get_activated_protocol_features', 'bounds-number', { body: { lower_bound: 3, upper_bound: 8, limit: 3 } });
  add('get_activated_protocol_features', 'bounds-string', { body: { lower_bound: '3', upper_bound: '8', limit: 3 } });
  add('get_activated_protocol_features', 'reverse', { body: { reverse: true, limit: 4 } });
  add('get_activated_protocol_features', 'search-by-block-num', { body: { search_by_block_num: true, lower_bound: 1, limit: 3 } });
  add('get_activated_protocol_features', 'limit-100', { body: { limit: 100 } });
  add('get_consensus_parameters', 'empty-object', { body: {} });
  add('get_consensus_parameters', 'GET', { method: 'GET' });
  add('get_scheduled_transactions', 'json-true', { body: { json: true, limit: 10 } });
  add('get_scheduled_transactions', 'json-false', { body: { json: false, limit: 5 } });
  add('get_scheduled_transactions', 'limit-string', { body: { json: true, limit: '5' } });
  add('get_scheduled_transactions', 'lower-bound-time', { body: { json: true, lower_bound: '2020-01-01T00:00:00', limit: 5 } });
  add('get_scheduled_transactions', 'empty-object', { body: {} });

  // keys: get_required_keys / get_accounts_by_authorizers
  if (signer) {
    const k = spell(signer.key);
    const tx = transferTx(ref, signer.account, 'eosio.null', coreQty, 'conformance', { hex: false });
    const txHex = transferTx(ref, signer.account, 'eosio.null', coreQty, 'conformance', { hex: true });
    const other = keyAcct.find((x) => x.key !== signer.key);
    add('get_required_keys', 'json-data-EOS-key', { body: { transaction: tx, available_keys: [k.eos] } });
    add('get_required_keys', 'json-data-PUB_K1-key', { body: { transaction: tx, available_keys: [k.pub] } });
    add('get_required_keys', 'hex-data-PUB_K1-key', { body: { transaction: txHex, available_keys: [k.pub] } });
    add('get_required_keys', 'mixed-spellings', { body: { transaction: txHex, available_keys: [k.eos, other ? spell(other.key).pub : k.pub] } });
    if (other) add('get_required_keys', 'err-wrong-key', { body: { transaction: txHex, available_keys: [spell(other.key).pub] } });
    add('get_required_keys', 'err-bad-key', { body: { transaction: txHex, available_keys: ['EOSnotakey'] } });
    add('get_required_keys', 'err-no-keys', { body: { transaction: txHex, available_keys: [] } });
    add('get_required_keys', 'err-missing-transaction', { body: { available_keys: [k.pub] } });
    errorStyles('get_required_keys');
    // get_transaction_id: packed, hex action data, JSON action data (needs the ABI: B answers 501 by design)
    add('get_transaction_id', 'hex-data', { body: txHex });
    add('get_transaction_id', 'wrapped-transaction', { body: { transaction: txHex } });
    add('get_transaction_id', 'json-data', { body: tx, extra: { expect_b: '501', note: 'JSON action data needs the ABI; the edge answers 501 (docs/V1-COVERAGE.md)' } });
    add('get_transaction_id', 'packed-trx', { body: { packed_trx: packTransaction(txHex).toString('hex'), compression: 0, signatures: [], packed_context_free_data: '' } });
    add('get_transaction_id', 'err-empty-object', { body: {} });
    errorStyles('get_transaction_id');
  }
  for (const x of keyAcct.slice(0, 3)) {
    const k = spell(x.key);
    add('get_accounts_by_authorizers', 'keys-PUB_K1', { body: { keys: [k.pub] } });
    add('get_accounts_by_authorizers', 'keys-EOS', { body: { keys: [k.eos] } });
  }
  add('get_accounts_by_authorizers', 'accounts-name', { body: { accounts: [acctAuth ? acctAuth.actor : 'eosio'] } });
  if (acctAuth) add('get_accounts_by_authorizers', 'accounts-permission-level', { body: { accounts: [{ actor: acctAuth.actor, permission: acctAuth.permission }] } });
  if (keyAcct[0]) add('get_accounts_by_authorizers', 'keys-and-accounts', { body: { keys: [spell(keyAcct[0].key).pub], accounts: ['eosio.prods'] } });
  add('get_accounts_by_authorizers', 'empty-object', { body: {} });
  add('get_accounts_by_authorizers', 'err-bad-key', { body: { keys: ['EOSnotakey'] } });
  add('get_accounts_by_authorizers', 'err-bad-account', { body: { accounts: ['Bad.Name!'] } });

  // writes: only requests that cannot land (unsigned, expired 2020-01-01, actor that does not exist)
  const dead = transferTx(ref, 'conform.none', 'eosio.null', coreQty, 'conformance: never lands', { hex: true });
  const deadPacked = { signatures: [], compression: 0, packed_context_free_data: '', packed_trx: packTransaction(dead).toString('hex') };
  for (const ep of ['push_transaction', 'send_transaction']) {
    add(ep, 'expired-unsigned', { body: deadPacked });
    add(ep, 'err-garbage-packed', { body: { signatures: [], compression: 0, packed_context_free_data: '', packed_trx: '00' } });
    add(ep, 'err-compression-string', { body: { ...deadPacked, compression: 'none' } });
    add(ep, 'err-empty-object', { body: {} });
    add(ep, 'err-empty-body', { body: '' });
    errorStyles(ep);
  }
  // Failed transactions: the most frequent write answer on a busy node (most push_transaction calls on a mainnet
  // API node are failing transactions). Status code, error code/name/what must match on both sides. Built at RUN time by diff.mjs
  // from tx_template (fresh expiration, TAPOS from A's LIB) so the non-expired cases really are non-expired.
  // Every case is unsigned (or carries a malformed signature) and is a 0.0001 self-transfer, which eosio.token
  // rejects even if it were ever executed: none of them can land.
  const selfFrom = from;
  const failTx = { actions: [{ account: 'eosio.token', name: 'transfer', authorization: [{ actor: selfFrom, permission: 'active' }], data: transferHex(selfFrom, selfFrom, `${(0.0001).toFixed(4)} ${core}`, 'conformance: never lands') }] };
  const FAIL_CASES = [
    ['missing-signature', { ...failTx, expire_in: 1800 }],
    ['unknown-actor', { actions: [{ ...failTx.actions[0], authorization: [{ actor: 'conform.none', permission: 'active' }], data: transferHex('conform.none', 'conform.none', `${(0.0001).toFixed(4)} ${core}`, 'conformance: never lands') }], expire_in: 1800 }],
    ['tapos-mismatch', { ...failTx, expire_in: 1800, ref: 'bad-prefix' }],
    ['expiration-too-far', { ...failTx, expire_in: 7200 }],
    ['bad-signature', { ...failTx, expire_in: 1800, signatures: ['SIG_K1_notasignature'] }],
    ['unknown-contract', { actions: [{ account: 'conform.none', name: 'transfer', authorization: [{ actor: selfFrom, permission: 'active' }], data: '' }], expire_in: 1800 }],
  ];
  const TRACE_VOL = ['processed.elapsed', 'processed.block_num', 'processed.block_time', 'processed.id', 'processed.except.stack', 'processed.action_traces', 'processed.net_usage', 'processed.receipt'];
  for (const [name, t] of FAIL_CASES) {
    add('push_transaction', `failed-${name}`, { body: null, extra: { tx_template: { ...t, wrap: 'packed' } } });
    add('send_transaction', `failed-${name}`, { body: null, extra: { tx_template: { ...t, wrap: 'packed' } } });
    add('send_transaction2', `failed-${name}`, { body: null, extra: { tx_template: { ...t, wrap: 'send_transaction2' } } });
    add('send_transaction2', `failed-${name}-failure-trace`, { body: null, extra: { tx_template: { ...t, wrap: 'send_transaction2-trace' }, volatile: TRACE_VOL, mask: ['processed.except.message', 'processed.error_code'] } });
    add('push_transactions', `failed-${name}`, { body: null, extra: { tx_template: { ...t, wrap: 'push_transactions' }, mask: ['*.processed.error'] } });
  }
  add('send_transaction2', 'expired-unsigned', { body: { return_failure_trace: false, retry_trx: false, transaction: deadPacked } });
  add('send_transaction2', 'expired-unsigned-failure-trace', { body: { return_failure_trace: true, retry_trx: false, transaction: deadPacked }, extra: { volatile: ['processed.elapsed', 'processed.block_num', 'processed.block_time', 'processed.id', 'processed.except.stack', 'processed.action_traces'] } });
  add('send_transaction2', 'err-missing-transaction', { body: { return_failure_trace: true } });
  add('send_transaction2', 'err-empty-body', { body: '' });
  add('push_transactions', 'one-expired', { body: [deadPacked], extra: { mask: ['*.processed.error'] } });
  add('push_transactions', 'empty-array', { body: [] });
  add('push_transactions', 'err-object', { body: deadPacked });
  add('push_transactions', 'err-empty-body', { body: '' });
  add('compute_transaction', 'expired-unsigned', { body: { transaction: deadPacked } });
  add('compute_transaction', 'err-empty-object', { body: {} });
  add('send_read_only_transaction', 'expired-unsigned', { body: { transaction: deadPacked } });
  add('send_read_only_transaction', 'err-empty-object', { body: {} });
  add('push_block', 'err-empty-object', { body: {} });

  // TAPOS as clients do it, resolved at run time from A's get_info (diff.mjs "dynamic"): eosjs blocksBehind:3 reads
  // get_block_header_state(head-3) then falls back to get_block_info; useLastIrreversible reads get_block_info(LIB);
  // older eosjs reads get_block. wharfkit/@proton/js read get_info + get_block_info / get_block_header_state.
  for (const [ep, key] of [['get_block_header_state', 'block_num_or_id'], ['get_block_info', 'block_num'], ['get_block', 'block_num_or_id']]) {
    add(ep, 'tapos-head-3', { body: {}, extra: { dynamic: { [key]: 'head-3' } } });
    add(ep, 'tapos-lib', { body: {}, extra: { dynamic: { [key]: 'lib' } } });
  }
  add('get_block_header_state', 'tapos-head-3-string', { body: {}, extra: { dynamic: { block_num_or_id: 'head-3:string' } } });

  // CORS: browser wallets and dapps preflight every POST
  for (const ep of ['push_transaction', 'send_transaction2', 'get_required_keys', 'get_table_rows', 'get_info', 'get_currency_balance', 'get_account', 'get_block_info', 'get_block_header_state']) {
    add(ep, 'cors-preflight', { method: 'OPTIONS', headers: { origin: 'https://dapp.example.org', 'access-control-request-method': 'POST', 'access-control-request-headers': 'content-type' }, extra: { cors: 'preflight' } });
  }
  add('get_info', 'cors-origin', { body: '{}', headers: { origin: 'https://dapp.example.org', 'content-type': 'text/plain;charset=UTF-8' }, extra: { cors: 'simple' } });
  add('get_table_rows', 'cors-origin', { body: { code: 'eosio.token', scope: h0, table: 'accounts', json: true }, headers: { origin: 'https://dapp.example.org', 'content-type': 'application/json' }, extra: { cors: 'simple' } });
  add('get_actions', 'cors-preflight', { method: 'OPTIONS', group: 'history', headers: { origin: 'https://dapp.example.org', 'access-control-request-method': 'POST', 'access-control-request-headers': 'content-type' }, extra: { cors: 'preflight' } });

  // endpoints Leap 5 does not serve (docs: nodeos-style 404 on the edge) + an unknown one
  for (const ep of ['get_transaction_status', 'abi_json_to_bin', 'abi_bin_to_json', 'get_finalizer_info', 'conform_nonexistent']) {
    add(ep, 'not-served', { body: {}, path: `/v1/chain/${ep}` });
  }
  add('get_supported_apis', 'GET', { method: 'GET', path: '/v1/node/get_supported_apis', extra: { unordered: ['apis'] } });
  add('get_supported_apis', 'POST', { body: {}, path: '/v1/node/get_supported_apis', extra: { unordered: ['apis'] } });

  // =========================================================================================================================
  // /v1/history (Hyperion v1 shim; diff.mjs sends these to --a-history/--b-history)
  // =========================================================================================================================
  const H = (ep, style, body, extra) => add(ep, style, { body, group: 'history', extra });
  for (const a of ['eosio.token', h0, ...(producers[0] ? [producers[0]] : [])]) {
    H('get_actions', 'pos0-offset4', { account_name: a, pos: 0, offset: 4 });
    H('get_actions', 'pos0-offset4-strings', { account_name: a, pos: '0', offset: '4' });
  }
  H('get_actions', 'latest-pos-1', { account_name: 'eosio.token', pos: -1, offset: -3 }, { live: ['actions'] });
  H('get_actions', 'no-pos', { account_name: h0 }, { live: ['actions'] });
  H('get_actions', 'err-missing-account', { pos: -1, offset: -2 }, { live: ['actions'] });
  H('get_actions', 'unknown-account', { account_name: 'conform.none', pos: -1, offset: -2 });
  add('get_actions', 'text-plain', { group: 'history', body: { account_name: 'eosio.token', pos: 0, offset: 1 }, headers: { 'content-type': 'text/plain;charset=UTF-8' } });
  for (const id of trxIds.slice(0, 3)) {
    H('get_transaction', 'id', { id });
    H('get_transaction', 'id-with-block-hint', { id, block_num_hint: txBlock });
  }
  if (trxIds[0]) H('get_transaction', 'id-uppercase', { id: trxIds[0].toUpperCase() });
  H('get_transaction', 'err-unknown-id', { id: 'f'.repeat(64) });
  H('get_transaction', 'err-bad-id', { id: 'nothex' });
  H('get_transaction', 'err-missing-id', {});
  for (const x of keyAcct.slice(0, 3)) {
    const k = spell(x.key);
    H('get_key_accounts', 'EOS', { public_key: k.eos }, { unordered: ['account_names'] });
    H('get_key_accounts', 'PUB_K1', { public_key: k.pub }, { unordered: ['account_names'] });
  }
  H('get_key_accounts', 'err-bad-key', { public_key: 'EOSnotakey' });
  H('get_key_accounts', 'err-missing-key', {});
  add('get_key_accounts', 'text-plain', { group: 'history', body: { public_key: keyAcct[0] ? spell(keyAcct[0].key).eos : 'EOSnotakey' }, headers: { 'content-type': 'text/plain;charset=UTF-8' }, extra: { unordered: ['account_names'] } });
  add('get_key_accounts', 'no-content-type', { group: 'history', body: { public_key: keyAcct[0] ? spell(keyAcct[0].key).eos : 'EOSnotakey' }, headers: {}, extra: { unordered: ['account_names'] } });
  for (const a of ['eosio', acctAuth ? acctAuth.actor : 'eosio.prods', h0]) H('get_controlled_accounts', 'name', { controlling_account: a }, { unordered: ['controlled_accounts'] });
  H('get_controlled_accounts', 'err-missing', {});

  // ---- coverage check: every endpoint in docs/V1-COVERAGE.md has at least one request ------------------------------------
  const have = new Set(lines.map((l) => l.tags[0]));
  const missing = [...coverage.keys()].filter((e) => !have.has(e));
  const meta = { source: SRC, hyperion: HYP, chain_id: info.chain_id, head: head, lib, server_version: info.server_version_string || info.server_version,
    generated_at: new Date().toISOString(), requests: lines.length, endpoints: have.size };
  writeFileSync(OUT, [JSON.stringify({ _meta: meta }), ...lines.map((l) => JSON.stringify(l))].join('\n') + '\n');
  log(`${lines.length} requests over ${have.size} endpoints -> ${OUT}`);
  if (missing.length) { log(`MISSING coverage for: ${missing.join(', ')}`); process.exit(1); }
}

main().catch((e) => { console.error('[corpus] failed:', e.stack || e.message); process.exit(2); });

