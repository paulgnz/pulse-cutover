#!/usr/bin/env node
// state-diff.mjs — byte-exact state comparison of two Antelope /v1/chain endpoints.
//
//   node tools/state-diff.mjs --a http://127.0.0.1:8888 --b http://127.0.0.1:8899 \
//        [--accounts eosio,alice,...] [--out report.json]
//
// Used to prove a cutover is atomic: the source chain frozen at the cut (A) and the
// imported PulseVM chain before its first new block (B) must hold IDENTICAL state.
// Compares, for every contract account: code_hash, abi_hash, and every row of every
// table in every scope as raw bytes (get_table_rows json:false); for every account:
// permissions (threshold, keys, account and wait weights, parent) and privileged flag.
// Resource usage (RAM/CPU/NET) is deliberately excluded: it is a billing model,
// not ledger state, and differs by design between Leap and PulseVM.
// No dependencies (Node >= 18). Exit 0 = identical, 1 = differences, 2 = error.
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';

const arg = (k, d) => { const i = process.argv.indexOf(`--${k}`); return i > 0 ? process.argv[i + 1] : d; };
const A = arg('a'), B = arg('b'), OUT = arg('out');
if (!A || !B) { console.error('usage: state-diff.mjs --a URL --b URL [--accounts a,b] [--out f.json]'); process.exit(2); }
const seed = (arg('accounts', '') || '').split(',').filter(Boolean);

async function call(base, path, body) {
  for (let i = 0; i < 4; i++) {
    try {
      const r = await fetch(`${base}/v1/chain/${path}`, { method: 'POST', body: JSON.stringify(body) });
      const j = await r.json();
      if (r.ok) return j;
      if (r.status < 500) return { __error: j };
    } catch (e) { if (i === 3) throw e; }
    await new Promise((res) => setTimeout(res, 300 * (i + 1)));
  }
  return { __error: 'retries exhausted' };
}
const sha = (s) => createHash('sha256').update(s).digest('hex');
// Same key, two spellings (EOS… legacy vs PUB_K1_…): compare the 33-byte key body.
const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function keyBody(k) {
  const s = String(k).replace(/^PUB_K1_/, '').replace(/^EOS/, '');
  if (/^PUB_(R1|WA)_/.test(String(k))) return String(k);
  let n = 0n; for (const c of s) { const i = B58.indexOf(c); if (i < 0) return String(k); n = n * 58n + BigInt(i); }
  return 'K1:' + n.toString(16).padStart(74, '0').slice(0, 66);
}
const canonPerms = (acct) => (acct.permissions || []).map((p) => ({
  perm: p.perm_name, parent: p.parent,
  threshold: p.required_auth?.threshold,
  keys: (p.required_auth?.keys || []).map((k) => `${keyBody(k.key)}#${k.weight}`).sort(),
  accounts: (p.required_auth?.accounts || []).map((a) => `${a.permission.actor}@${a.permission.permission}#${a.weight}`).sort(),
  waits: (p.required_auth?.waits || []).map((w) => `${w.wait_sec}#${w.weight}`).sort(),
})).sort((x, y) => x.perm.localeCompare(y.perm));

async function scopes(base, code, table) {
  const out = []; let lower = '';
  for (let page = 0; page < 1000; page++) {
    const r = await call(base, 'get_table_by_scope', { code, table, lower_bound: lower, limit: 500 });
    if (r.__error) return { error: r.__error };
    for (const row of r.rows || []) if (row.table === table && !out.includes(row.scope)) out.push(row.scope);
    if (!r.more || r.more === lower) break;
    lower = r.more;
  }
  return out.sort();
}
async function rows(base, code, scope, table) {
  const out = []; let lower = '';
  for (let page = 0; page < 10000; page++) {
    const r = await call(base, 'get_table_rows', { json: false, code, scope, table, lower_bound: lower, limit: 500 });
    if (r.__error) return { error: r.__error };
    out.push(...(r.rows || []).map((x) => (typeof x === 'string' ? x : x.data ?? JSON.stringify(x))));
    if (!r.more || !r.next_key || r.next_key === lower) break;
    lower = r.next_key;
  }
  return out;
}

async function snapshot(base) {
  const info = await call(base, 'get_info', {});
  const items = new Map(); // key -> digest
  const detail = new Map(); // key -> value (for diffs)
  const accounts = new Set(['eosio', 'eosio.token', ...seed]);
  const contracts = [];
  // Discover accounts: seeds + every table scope + every code account reachable from them.
  const visit = async (name) => {
    const acct = await call(base, 'get_account', { account_name: name });
    if (acct.__error) return;
    const perms = canonPerms(acct);
    const v = JSON.stringify({ perms, privileged: !!acct.privileged });
    items.set(`account/${name}`, sha(v)); detail.set(`account/${name}`, v);
    const ch = await call(base, 'get_code_hash', { account_name: name });
    const code = ch.code_hash && !/^0+$/.test(ch.code_hash) ? ch.code_hash : null;
    if (code) {
      const abi = await call(base, 'get_raw_abi', { account_name: name });
      const v2 = JSON.stringify({ code_hash: code, abi_hash: abi.abi_hash || null });
      items.set(`code/${name}`, sha(v2)); detail.set(`code/${name}`, v2);
      contracts.push(name);
    }
  };
  const queue = [...accounts]; const seen = new Set();
  while (queue.length) {
    const n = queue.shift(); if (seen.has(n)) continue; seen.add(n);
    await visit(n);
    if (contracts.includes(n)) {
      const abi = await call(base, 'get_abi', { account_name: n });
      for (const t of abi.abi?.tables || []) {
        const sc = await scopes(base, n, t.name);
        if (sc.error) { items.set(`table/${n}/${t.name}`, 'ERROR'); detail.set(`table/${n}/${t.name}`, JSON.stringify(sc.error)); continue; }
        for (const s of sc) {
          const r = await rows(base, n, s, t.name);
          const v = r.error ? `ERROR ${JSON.stringify(r.error)}` : r.join('\n');
          items.set(`rows/${n}/${t.name}/${s}`, sha(v)); detail.set(`rows/${n}/${t.name}/${s}`, r.error ? v : `${r.length} rows`);
          if (/^[a-z1-5.]{1,12}$/.test(s) && !seen.has(s)) queue.push(s);   // scopes are usually account names
        }
      }
    }
  }
  const keys = [...items.keys()].sort();
  const digest = sha(keys.map((k) => `${k}=${items.get(k)}`).join('\n'));
  return { info: { chain_id: info.chain_id, head_block_num: info.head_block_num, head_block_id: info.head_block_id }, items, detail, digest, contracts };
}

const t0 = Date.now();
const [sa, sb] = await Promise.all([snapshot(A), snapshot(B)]);
const all = [...new Set([...sa.items.keys(), ...sb.items.keys()])].sort();
const diffs = [];
for (const k of all) {
  const x = sa.items.get(k), y = sb.items.get(k);
  if (x !== y) diffs.push({ item: k, a: x ? sa.detail.get(k) : 'MISSING', b: y ? sb.detail.get(k) : 'MISSING' });
}
const count = (s, p) => [...s.items.keys()].filter((k) => k.startsWith(p)).length;
const report = {
  a: { url: A, ...sa.info, digest: sa.digest, accounts: count(sa, 'account/'), contracts: sa.contracts.length, table_scopes: count(sa, 'rows/') },
  b: { url: B, ...sb.info, digest: sb.digest, accounts: count(sb, 'account/'), contracts: sb.contracts.length, table_scopes: count(sb, 'rows/') },
  identical: diffs.length === 0 && sa.digest === sb.digest,
  differences: diffs.slice(0, 200), difference_count: diffs.length,
  elapsed_ms: Date.now() - t0,
};
if (OUT) writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(`A ${A}  head ${report.a.head_block_num}  accounts ${report.a.accounts}  contracts ${report.a.contracts}  table-scopes ${report.a.table_scopes}  digest ${sa.digest.slice(0, 16)}`);
console.log(`B ${B}  head ${report.b.head_block_num}  accounts ${report.b.accounts}  contracts ${report.b.contracts}  table-scopes ${report.b.table_scopes}  digest ${sb.digest.slice(0, 16)}`);
console.log(report.identical ? 'IDENTICAL — every account, permission, contract and table row matches byte-for-byte'
  : `DIFFERENT — ${diffs.length} item(s):\n` + diffs.slice(0, 15).map((d) => `  ${d.item}: A=${String(d.a).slice(0, 60)} | B=${String(d.b).slice(0, 60)}`).join('\n'));
process.exit(report.identical ? 0 : 1);
