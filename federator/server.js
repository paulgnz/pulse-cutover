// hyperion-federator — the /v2 history boundary router for a PulseVM cutover.
//
// "Your endpoint keeps its memory": after the cut, one public /v2 URL answers
// with PRE-cut history from the legacy Hyperion (the source chain's archive —
// a public one like https://test.proton.eosusa.io, or the operator's own old
// Hyperion/ES if they kept full local history: same knob, different URL) and
// POST-cut history from the local hyperion-rs indexing the PulseVM chain.
// The merge semantics are a server-side port of the pulse-explorer federation
// (lib/hyperion.ts), which already proved them client-side on the 1:1 chain:
// every PulseVM block number > cut > every legacy pre-cut block number, so a
// single descending timeline paginates cleanly across the seam.
//
// STATE is different from history: an index only knows what it has seen, so
// the local index (filled from post-cut deltas only) has no row for an account
// untouched since the cut, and the legacy index is frozen at the cut. Rule:
// VALUES COME FROM THE CHAIN (CHAIN_URL, the node's /v1/chain); the indexes
// are used only for DISCOVERY (which contracts / accounts to look at). That
// covers get_tokens, get_account, get_key_accounts, get_controlled_accounts.
//
// Why a standalone service (and not part of the /v1 edge): the edge is
// stateless PROTOCOL TRANSLATION (/v1 REST) that every api-mode operator
// needs; this is HISTORY BOUNDARY ROUTING between two same-protocol /v2
// sources, parameterized by ceremony facts (cut block/time), that only
// hyperion-mode operators need — different concern, different config
// surface, different lifetime (this one is the permanent /v2 server after the
// cut). Coupling them would make every /v1 operator carry federation config.
//
// Two listeners:
//   PORT             (default 7010) — the federating router. The ceremony's
//                    /v2 flip points nginx here.
//   PASSTHROUGH_PORT (default 7019) — pure legacy proxy: stands in for "the
//                    /v2 service you already had" so the public /v2 URL works
//                    BEFORE the ceremony too (providers with a real local
//                    Hyperion point the pre-cut upstream at that instead).
//
// The history boundary is NOT baked into env: the cutover agent discovers the
// cut mid-ceremony and writes BOUNDARY_FILE ({cut_block, cut_time, ...});
// this server re-reads it on mtime change. Before the file exists the router
// degrades to legacy-only (cut = +infinity) — never a wrong answer.
//
// Env: LOCAL (http://127.0.0.1:7000), LEGACY (https://test.proton.eosusa.io),
//      CHAIN_URL (http://127.0.0.1:8899 — the /v1 edge, or the node's native
//      base http://127.0.0.1:9650/ext/bc/<BID>), BOUNDARY_FILE
//      (/etc/pulse-cutover/boundary.json), PORT, PASSTHROUGH_PORT, HOST
//      (127.0.0.1), LOCAL_TIMEOUT_MS (8000), LEGACY_TIMEOUT_MS (8000),
//      CHAIN_TIMEOUT_MS (8000), CHAIN_CONCURRENCY (8).
// No dependencies (Node >= 14). PORT=0 / PASSTHROUGH_PORT=0 pick free ports.
'use strict';
const http = require('http');
const https = require('https');
const fs = require('fs');
const crypto = require('crypto');

const LOCAL = (process.env.LOCAL || 'http://127.0.0.1:7000').replace(/\/$/, '');
const LEGACY = (process.env.LEGACY || 'https://test.proton.eosusa.io').replace(/\/$/, '');
const CHAIN_URL = (process.env.CHAIN_URL || 'http://127.0.0.1:8899').replace(/\/$/, '');
const BOUNDARY_FILE = process.env.BOUNDARY_FILE || '/etc/pulse-cutover/boundary.json';
const PORT = Number(process.env.PORT == null ? 7010 : process.env.PORT);
const PASSTHROUGH_PORT = Number(process.env.PASSTHROUGH_PORT == null ? 7019 : process.env.PASSTHROUGH_PORT);
const HOST = process.env.HOST || '127.0.0.1';
const LOCAL_TIMEOUT_MS = Number(process.env.LOCAL_TIMEOUT_MS || 8000);
const LEGACY_TIMEOUT_MS = Number(process.env.LEGACY_TIMEOUT_MS || 8000);
const CHAIN_TIMEOUT_MS = Number(process.env.CHAIN_TIMEOUT_MS || 8000);
const CHAIN_CONCURRENCY = Math.max(1, Number(process.env.CHAIN_CONCURRENCY || 8));

// ---- boundary (re-read on mtime change; degrade to legacy-only when absent)
let _boundary = { cut_block: Number.MAX_SAFE_INTEGER, cut_time: null };
let _boundaryMtime = 0;
function boundary() {
  try {
    const st = fs.statSync(BOUNDARY_FILE);
    if (st.mtimeMs !== _boundaryMtime) {
      const b = JSON.parse(fs.readFileSync(BOUNDARY_FILE, 'utf8'));
      _boundary = {
        cut_block: Number(b.cut_block) || Number.MAX_SAFE_INTEGER,
        cut_time: b.cut_time || null,
        cut_block_id: b.cut_block_id,
        chain_id: b.chain_id,
      };
      _boundaryMtime = st.mtimeMs;
      console.log(`boundary loaded: cut_block=${_boundary.cut_block} cut_time=${_boundary.cut_time}`);
    }
  } catch { /* absent/corrupt: keep previous (default = legacy-only) */ }
  return _boundary;
}
const staged = (b) => b.cut_block !== Number.MAX_SAFE_INTEGER;

// ---- upstream fetch that never throws (http.request: Node 14 has no fetch)
function fetchJson(base, path, timeoutMs, { method = 'GET', body } = {}) {
  return new Promise((resolve) => {
    let u; try { u = new URL(base + path); } catch { return resolve({ ok: false, status: 0, json: null, error: `bad url ${base}${path}` }); }
    const headers = { accept: 'application/json' };
    if (/^(127\.|localhost$|\[::1\]$)/.test(u.hostname)) headers.host = 'localhost'; // metalgo's host guard (CHAIN_URL = native base)
    if (body != null) { headers['content-type'] = 'application/json'; headers['content-length'] = Buffer.byteLength(body); }
    const req = (u.protocol === 'https:' ? https : http).request(u, { method, headers, timeout: timeoutMs }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        const ok = res.statusCode >= 200 && res.statusCode < 300;
        try { resolve({ ok, status: res.statusCode, json: JSON.parse(text) }); }
        catch { resolve({ ok: false, status: res.statusCode, json: null, text: text.slice(0, 300) }); }
      });
      res.on('error', (e) => resolve({ ok: false, status: 0, json: null, error: String((e && e.message) || e) }));
    });
    req.on('timeout', () => req.destroy(new Error(`timeout after ${timeoutMs} ms`)));
    req.on('error', (e) => resolve({ ok: false, status: 0, json: null, error: String((e && e.message) || e) }));
    if (body != null) req.write(body);
    req.end();
  });
}
const localGet = (path) => fetchJson(LOCAL, path, LOCAL_TIMEOUT_MS);
const legacyGet = (path) => fetchJson(LEGACY, path, LEGACY_TIMEOUT_MS);
// Hyperion's /v1/history shim is POST + JSON body (hyperion-rs routes are POST-only).
const localPost = (path, obj) => fetchJson(LOCAL, path, LOCAL_TIMEOUT_MS, { method: 'POST', body: JSON.stringify(obj) });
const legacyPost = (path, obj) => fetchJson(LEGACY, path, LEGACY_TIMEOUT_MS, { method: 'POST', body: JSON.stringify(obj) });
const chain = (name, obj) => fetchJson(CHAIN_URL, `/v1/chain/${name}`, CHAIN_TIMEOUT_MS, { method: 'POST', body: JSON.stringify(obj) });

// Legacy /v2/health cache: the legacy archive is a REMOTE public service that
// rate-limits (observed live: eosusa 429s under a 4 Hz hammer through the
// passthrough). Health checks must not spend its request budget — the
// per-request signal that matters is the LOCAL side anyway.
const HEALTH_CACHE_MS = Number(process.env.LEGACY_HEALTH_CACHE_MS || 5000);
let _legacyHealth = { at: 0, value: null };
async function legacyHealthCached() {
  const now = Date.now();
  if (_legacyHealth.value && now - _legacyHealth.at < HEALTH_CACHE_MS) return _legacyHealth.value;
  const r = await legacyGet('/v2/health');
  _legacyHealth = { at: now, value: r };
  return r;
}

const qs = (params) => new URLSearchParams(params).toString();
async function pool(items, n, fn) {
  const out = new Array(items.length); let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const k = i++; out[k] = await fn(items[k], k); } }));
  return out;
}

// ---- keys: compare EOS… / PUB_K1_… / PUB_R1_… / PUB_WA_… by decoded bytes -------------------------------
// hyperion-rs normalizes to one spelling, the legacy index may hold another, the chain answers PUB_K1_….
const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function b58decode(s) {
  let n = 0n;
  for (const c of s) { const i = B58.indexOf(c); if (i < 0) return null; n = n * 58n + BigInt(i); }
  let hex = n === 0n ? '' : n.toString(16); if (hex.length % 2) hex = '0' + hex;
  let lead = 0; while (lead < s.length && s[lead] === '1') lead++;
  return Buffer.concat([Buffer.alloc(lead), Buffer.from(hex, 'hex')]);
}
function b58encode(buf) {
  let n = BigInt('0x' + (buf.toString('hex') || '0')), s = '';
  while (n > 0n) { s = B58[Number(n % 58n)] + s; n /= 58n; }
  for (const b of buf) { if (b !== 0) break; s = '1' + s; }
  return s;
}
// RIPEMD-160 (key checksums): OpenSSL 3 builds may not expose it, so fall back to JS.
const RL = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,7,4,13,1,10,6,15,3,12,0,9,5,2,14,11,8,3,10,14,4,9,15,8,1,2,7,0,6,13,11,5,12,1,9,11,10,0,8,12,4,13,3,7,15,14,5,6,2,4,0,5,9,7,12,2,10,14,1,3,8,11,6,15,13];
const RR = [5,14,7,0,9,2,11,4,13,6,15,8,1,10,3,12,6,11,3,7,0,13,5,10,14,15,8,12,4,9,1,2,15,5,1,3,7,14,6,9,11,8,12,2,10,0,4,13,8,6,4,1,3,11,15,0,5,12,2,13,9,7,10,14,12,15,10,4,1,5,8,7,6,2,13,14,0,3,9,11];
const SL = [11,14,15,12,5,8,7,9,11,13,14,15,6,7,9,8,7,6,8,13,11,9,7,15,7,12,15,9,11,7,13,12,11,13,6,7,14,9,13,15,14,8,13,6,5,12,7,5,11,12,14,15,14,15,9,8,9,14,5,6,8,6,5,12,9,15,5,11,6,8,13,12,5,12,13,14,11,8,5,6];
const SR = [8,9,9,11,13,15,15,5,7,7,8,11,14,14,12,6,9,13,15,7,12,8,9,11,7,7,12,7,6,15,13,11,9,7,15,11,8,6,6,14,12,13,5,14,13,13,7,5,15,5,8,11,14,14,6,14,6,9,12,9,12,5,15,8,8,5,12,9,12,5,14,6,8,13,6,5,15,13,11,11];
const KL = [0, 0x5a827999, 0x6ed9eba1, 0x8f1bbcdc, 0xa953fd4e], KR = [0x50a28be6, 0x5c4dd124, 0x6d703ef3, 0x7a6d76e9, 0];
function ripemd160js(msg) {
  const f = (j, x, y, z) => (j < 16 ? x ^ y ^ z : j < 32 ? (x & y) | (~x & z) : j < 48 ? (x | ~y) ^ z : j < 64 ? (x & z) | (y & ~z) : x ^ (y | ~z));
  const rotl = (x, n) => (x << n) | (x >>> (32 - n));
  const len = msg.length, total = Math.ceil((len + 9) / 64) * 64, b = Buffer.alloc(total);
  msg.copy(b); b[len] = 0x80;
  b.writeUInt32LE((len * 8) >>> 0, total - 8); b.writeUInt32LE(Math.floor(len / 0x20000000), total - 4);
  let h0 = 0x67452301, h1 = 0xefcdab89, h2 = 0x98badcfe, h3 = 0x10325476, h4 = 0xc3d2e1f0;
  for (let off = 0; off < total; off += 64) {
    const X = []; for (let i = 0; i < 16; i++) X.push(b.readInt32LE(off + 4 * i));
    let al = h0, bl = h1, cl = h2, dl = h3, el = h4, ar = h0, br = h1, cr = h2, dr = h3, er = h4, t;
    for (let j = 0; j < 80; j++) {
      t = (rotl((al + f(j, bl, cl, dl) + X[RL[j]] + KL[j >> 4]) | 0, SL[j]) + el) | 0;
      al = el; el = dl; dl = rotl(cl, 10); cl = bl; bl = t;
      t = (rotl((ar + f(79 - j, br, cr, dr) + X[RR[j]] + KR[j >> 4]) | 0, SR[j]) + er) | 0;
      ar = er; er = dr; dr = rotl(cr, 10); cr = br; br = t;
    }
    t = (h1 + cl + dr) | 0; h1 = (h2 + dl + er) | 0; h2 = (h3 + el + ar) | 0; h3 = (h4 + al + br) | 0; h4 = (h0 + bl + cr) | 0; h0 = t;
  }
  const out = Buffer.alloc(20); [h0, h1, h2, h3, h4].forEach((h, i) => out.writeInt32LE(h, 4 * i)); return out;
}
let _nativeRipemd = true;
function ripemd160(buf) {
  if (_nativeRipemd) { try { return crypto.createHash('ripemd160').update(buf).digest(); } catch { _nativeRipemd = false; } }
  return ripemd160js(buf);
}
function keyInfo(k) {
  const s = String(k == null ? '' : k).trim();
  let type, body, legacy = false;
  const m = s.match(/^PUB_(K1|R1|WA)_(.+)$/);
  if (m) { type = m[1]; body = m[2]; } else if (s.startsWith('EOS')) { type = 'K1'; body = s.slice(3); legacy = true; } else return null;
  const raw = b58decode(body);
  if (!raw || raw.length < 5) return null;
  const data = raw.subarray(0, raw.length - 4), chk = raw.subarray(raw.length - 4);
  if (!ripemd160(legacy ? data : Buffer.concat([data, Buffer.from(type)])).subarray(0, 4).equals(chk)) return null;
  if (type !== 'WA' && data.length !== 33) return null;
  return { type, data: Buffer.from(data) };
}
const keyCanon = (k) => { const i = keyInfo(k); return i ? `${i.type}:${i.data.toString('hex')}` : `raw:${String(k).trim()}`; };
// Every spelling an index might hold for this key (discovery queries each).
function keySpellings(k) {
  const i = keyInfo(k);
  if (!i) return [String(k).trim()];
  const pub = `PUB_${i.type}_` + b58encode(Buffer.concat([i.data, ripemd160(Buffer.concat([i.data, Buffer.from(i.type)])).subarray(0, 4)]));
  const leg = i.type === 'K1' ? 'EOS' + b58encode(Buffer.concat([i.data, ripemd160(i.data).subarray(0, 4)])) : null;
  return [...new Set([String(k).trim(), pub, leg].filter(Boolean))];
}

// ---- chain truth ---------------------------------------------------------------------------------------------
// Per-head cache for chain reads (one get_info per request keeps it exact: an entry is only reused while
// the chain head has not moved).
const CACHE_MAX = 5000;
const _cache = new Map();
async function chainHead() {
  const r = await chain('get_info', {});
  return r.ok && r.json ? r.json : null;
}
async function chainCached(head, name, obj) {
  const key = `${head}|${name}|${JSON.stringify(obj)}`;
  if (head != null && _cache.has(key)) return _cache.get(key);
  const r = await chain(name, obj);
  if (head != null && r.ok) {
    if (_cache.size >= CACHE_MAX) _cache.delete(_cache.keys().next().value);
    _cache.set(key, r);
  }
  return r;
}
// Hyperion token shape from a chain balance string "12.3400 XPR".
function tokenRow(contract, s) {
  const m = String(s).trim().match(/^(-?\d+)(?:\.(\d+))?\s+([A-Z]{1,7})$/);
  if (!m) return null;
  return { symbol: m[3], precision: (m[2] || '').length, amount: parseFloat(`${m[1]}.${m[2] || '0'}`), contract };
}
// Verify discovered accounts against their CURRENT permissions on the chain.
async function verifyAccounts(names, head, predicate) {
  const sorted = [...names].sort();
  const results = await pool(sorted, CHAIN_CONCURRENCY, (n) => chainCached(head, 'get_account', { account_name: n }));
  const out = [], errors = [];
  results.forEach((r, i) => {
    if (r.ok && r.json) { if (predicate(r.json)) out.push(r.json.account_name || sorted[i]); }
    else if (r.status === 0 || r.status >= 502) errors.push({ account: sorted[i], status: r.status, error: r.error || r.text });
    // 4xx/500 from the node for a candidate = the account does not exist now: not an error, not a match.
  });
  return { accounts: out, errors };
}
const permHasKey = (want) => (acct) => (acct.permissions || []).some((p) => ((p.required_auth && p.required_auth.keys) || []).some((k) => keyCanon(k.key) === want));
const permHasActor = (actor) => (acct) => (acct.permissions || []).some((p) => ((p.required_auth && p.required_auth.accounts) || []).some((a) => a.permission && a.permission.actor === actor));

// ---- federated endpoints -------------------------------------------------

// One logical descending timeline: [ local post-cut ] ++ [ legacy pre-cut ].
// Page [skip, skip+limit): local serves indices < localTotal, legacy the rest
// (exactly the explorer's proven pagination; valid because block numbering
// continues across the cut).
async function getActions(params) {
  const b = boundary();
  const account = String(params.account || '');
  const limit = Math.max(1, Number(params.limit || 40));
  const skip = Math.max(0, Number(params.skip || 0));
  // Pass unknown filters (act.name, filter, after/before, ...) through.
  const extra = {};
  for (const [k, v] of Object.entries(params)) {
    if (!['account', 'limit', 'skip', 'sort'].includes(k)) extra[k] = v;
  }

  // Global feed (no account): recent post-cut actions from local only — the
  // cross-source merge is defined per account (stable pagination anchor).
  if (!account) {
    const r = await localGet(`/v2/history/get_actions?${qs({ limit, skip, sort: 'desc', ...extra })}`);
    if (r.ok) return { status: 200, body: { ...r.json, federated: true, boundary: pubBoundary(b) } };
    const l = await legacyGet(`/v2/history/get_actions?${qs({ limit, skip, sort: 'desc', ...extra })}`);
    return { status: l.ok ? 200 : 502, body: l.ok ? { ...l.json, federated: true, legacy_only: true } : errBody('both history sources unreachable', r, l) };
  }

  const local = await localGet(`/v2/history/get_actions?${qs({ account, limit, skip, sort: 'desc', ...extra })}`);
  const localTotal = local.ok ? ((local.json && local.json.total && local.json.total.value) || 0) : 0;
  const localActs = (local.ok ? (local.json && local.json.actions) || [] : [])
    .filter((a) => (a.block_num || 0) > b.cut_block)
    .slice(0, limit);

  const need = limit - localActs.length;
  let legacyActs = [];
  let legacyTotal = 0;
  const legacySkip = Math.max(0, skip - localTotal);
  const legacyParams = { account, limit: Math.max(need, 1), skip: legacySkip, sort: 'desc', ...extra };
  if (b.cut_time) legacyParams.before = b.cut_time;
  const legacy = await legacyGet(`/v2/history/get_actions?${qs(legacyParams)}`);
  if (legacy.ok) {
    legacyTotal = (legacy.json && legacy.json.total && legacy.json.total.value) || 0;
    if (need > 0) {
      legacyActs = ((legacy.json && legacy.json.actions) || [])
        .filter((a) => (a.block_num || 0) <= b.cut_block)
        .map((a) => ({ ...a, _premigration: true }))
        .slice(0, need);
    }
  }

  if (!local.ok && !legacy.ok) return { status: 502, body: errBody('both history sources unreachable', local, legacy) };
  // Partial-answer honesty: if one source failed (observed live: legacy 429
  // under load), the page may be missing that source's rows — say so, and
  // mark the total as a lower bound so clients don't treat it as complete.
  const partial = !local.ok || !legacy.ok;
  return {
    status: 200,
    body: {
      actions: [...localActs, ...legacyActs],
      total: { value: localTotal + legacyTotal, relation: partial ? 'gte' : 'eq' },
      local_total: localTotal,
      legacy_total: legacyTotal,
      lib: local.ok ? local.json && local.json.lib : undefined,
      federated: true,
      ...(partial ? { partial: true, source_errors: sourceErrors({ local, legacy }) } : {}),
      boundary: pubBoundary(b),
    },
  };
}

// A transaction is either post-cut (local) or pre-cut (legacy): new-then-legacy.
async function getTransaction(params) {
  const id = String(params.id || '');
  const local = await localGet(`/v2/history/get_transaction?id=${encodeURIComponent(id)}`);
  if (local.ok && local.json && local.json.actions && local.json.actions.length) {
    return { status: 200, body: { ...local.json, federated: true } };
  }
  const legacy = await legacyGet(`/v2/history/get_transaction?id=${encodeURIComponent(id)}`);
  if (legacy.ok && legacy.json && legacy.json.actions && legacy.json.actions.length) {
    return { status: 200, body: { ...legacy.json, _premigration: true, federated: true } };
  }
  if (local.ok || legacy.ok) {
    return { status: 200, body: { ...(local.ok ? local.json : legacy.json), federated: true } };
  }
  return { status: 502, body: errBody('both history sources unreachable', local, legacy) };
}

// Discovery for a key: both indexes, every spelling. Returns {names, sources:{local, legacy}}.
async function discoverKeyAccounts(key) {
  const spellings = keySpellings(key);
  const calls = [];
  for (const s of spellings) {
    const path = `/v2/state/get_key_accounts?${qs({ public_key: s })}`;
    calls.push(localGet(path).then((r) => ({ src: 'local', r })), legacyGet(path).then((r) => ({ src: 'legacy', r })));
  }
  const res = await Promise.all(calls);
  const names = new Set(), ok = { local: true, legacy: true }, last = {};
  for (const { src, r } of res) {   // a source counts as answered only if every spelling query succeeded
    if (r.ok) ((r.json && r.json.account_names) || []).forEach((n) => names.add(n));
    else { ok[src] = false; last[src] = r; }
  }
  return { names, ok, last };
}
// /v2/state/get_key_accounts + /v1/history/get_key_accounts: accounts whose
// CURRENT chain permissions contain the key (a key removed after the cut is
// still in the legacy index; the chain has the final word).
async function getKeyAccounts(params) {
  const key = String(params.public_key || '').trim();
  if (!key) return { status: 400, body: { error: 'public_key is required' } };
  const d = await discoverKeyAccounts(key);
  if (!d.ok.local && !d.ok.legacy) return { status: 502, body: errBody('both discovery sources unreachable', d.last.local, d.last.legacy) };
  const info = await chainHead();
  if (!info) return { status: 502, body: { error: `chain (${CHAIN_URL}) unreachable: cannot verify discovered accounts` } };
  const v = await verifyAccounts(d.names, info.head_block_num, permHasKey(keyCanon(key)));
  const partial = !d.ok.local || !d.ok.legacy || v.errors.length > 0;
  return { status: 200, body: { account_names: v.accounts, federated: true, verified_at_block: info.head_block_num, ...(partial ? { partial: true, source_errors: { ...sourceErrors({ local: d.last.local, legacy: d.last.legacy }), ...(v.errors.length ? { chain: v.errors } : {}) } } : {}) } };
}

// /v1/history/get_controlled_accounts: discovery legacy + local, verified on chain.
async function getControlledAccounts(params) {
  const actor = String(params.controlling_account || '').trim();
  if (!actor) return { status: 400, body: { error: 'controlling_account is required' } };
  const [local, legacy] = await Promise.all([
    localPost('/v1/history/get_controlled_accounts', { controlling_account: actor }),
    legacyPost('/v1/history/get_controlled_accounts', { controlling_account: actor }),
  ]);
  if (!local.ok && !legacy.ok) return { status: 502, body: errBody('both discovery sources unreachable', local, legacy) };
  const names = new Set([
    ...((local.ok && local.json && local.json.controlled_accounts) || []),
    ...((legacy.ok && legacy.json && legacy.json.controlled_accounts) || []),
  ]);
  const info = await chainHead();
  if (!info) return { status: 502, body: { error: `chain (${CHAIN_URL}) unreachable: cannot verify discovered accounts` } };
  const v = await verifyAccounts(names, info.head_block_num, permHasActor(actor));
  const partial = !local.ok || !legacy.ok || v.errors.length > 0;
  return { status: 200, body: { controlled_accounts: v.accounts, ...(partial ? { partial: true } : {}) } };
}

// Token discovery -> chain balances. Contracts come from BOTH indexes (legacy:
// held before the cut; local: touched after it); amounts come only from the
// chain's get_currency_balance. A contract with no balance row is omitted.
async function tokensFor(account, head) {
  const path = `/v2/state/get_tokens?${qs({ account })}`;
  const [local, legacy] = await Promise.all([localGet(path), legacyGet(path)]);
  if (!local.ok && !legacy.ok) return { error: errBody('both discovery sources unreachable', local, legacy) };
  const contracts = new Set();
  for (const r of [local, legacy]) if (r.ok) ((r.json && r.json.tokens) || []).forEach((t) => t && t.contract && contracts.add(String(t.contract)));
  const list = [...contracts].sort();
  const balances = await pool(list, CHAIN_CONCURRENCY, (code) => chainCached(head, 'get_currency_balance', { code, account }));
  const tokens = [], chainErrors = [];
  balances.forEach((r, i) => {
    if (r.ok && Array.isArray(r.json)) r.json.forEach((s) => { const t = tokenRow(list[i], s); if (t) tokens.push(t); });
    else if (r.status === 0 || r.status >= 502) chainErrors.push({ contract: list[i], status: r.status, error: r.error || r.text });
    // 4xx/500: no such token contract / no balance row now -> omitted
  });
  tokens.sort((a, b) => b.amount - a.amount || (a.contract < b.contract ? -1 : 1));
  const partial = !local.ok || !legacy.ok || chainErrors.length > 0;
  return { tokens, partial, source_errors: partial ? { ...sourceErrors({ local, legacy }), ...(chainErrors.length ? { chain: chainErrors } : {}) } : undefined };
}
async function getTokens(params) {
  const account = String(params.account || '').trim();
  if (!account) return { status: 400, body: { error: 'account is required' } };
  const info = await chainHead();
  if (!info) return { status: 502, body: { error: `chain (${CHAIN_URL}) unreachable: balances come from the chain only` } };
  const t = await tokensFor(account, info.head_block_num);
  if (t.error) return { status: 502, body: t.error };
  return { status: 200, body: { account, tokens: t.tokens, federated: true, ...(t.partial ? { partial: true, source_errors: t.source_errors } : {}) } };
}
// hyperion-rs shape: {account, lib, tokens, permissions, total_actions, actions}
// with permissions from the chain (nodeos get_account permission objects),
// tokens as above and actions from the federated history (limit 20).
async function getAccount(params) {
  const account = String(params.account || '').trim();
  if (!account) return { status: 400, body: { error: 'account is required' } };
  const info = await chainHead();
  if (!info) return { status: 502, body: { error: `chain (${CHAIN_URL}) unreachable: account state comes from the chain only` } };
  const acct = await chainCached(info.head_block_num, 'get_account', { account_name: account });
  if (!acct.ok || !acct.json) {
    if (acct.status === 0 || acct.status >= 502) return { status: 502, body: errBody(`chain get_account failed`, acct) };
    return { status: 404, body: { error: `account ${account} not found on chain`, chain: acct.json || acct.text } };
  }
  const [t, h] = await Promise.all([tokensFor(account, info.head_block_num), getActions({ account, limit: 20 })]);
  const partial = !!t.error || t.partial || h.status !== 200 || !!h.body.partial;
  return {
    status: 200,
    body: {
      account,
      lib: info.last_irreversible_block_num,
      tokens: t.error ? [] : t.tokens,
      permissions: acct.json.permissions || [],
      total_actions: h.status === 200 ? h.body.total && h.body.total.value : undefined,
      actions: h.status === 200 ? h.body.actions : [],
      federated: true,
      ...(partial ? { partial: true } : {}),
    },
  };
}

// /v1/history/get_actions (nodeos history_plugin shape) served from the
// federated /v2 timeline: exact ORDER across the cut, but account_action_seq
// is synthesized from the combined position (see federator/README.md).
function v1Action(a, seq) {
  return {
    global_action_seq: a.global_sequence != null ? a.global_sequence : (a.receipts && a.receipts[0] && a.receipts[0].global_sequence),
    account_action_seq: seq,
    block_num: a.block_num,
    block_time: a['@timestamp'] || a.timestamp,
    action_trace: {
      receipt: a.receipts && a.receipts[0],
      act: a.act, trx_id: a.trx_id, block_num: a.block_num, block_time: a['@timestamp'] || a.timestamp,
      producer_block_id: a.block_id,
    },
  };
}
async function getActionsV1(params) {
  const account = String(params.account_name || '').trim();
  if (!account) return { status: 400, body: { error: 'account_name is required' } };
  const pos = params.pos == null ? -1 : Number(params.pos);
  const offset = params.offset == null ? -20 : Number(params.offset);
  let approximate = false, skip, limit;
  // Probe the combined total (one row) to map absolute positions onto the descending timeline.
  const probe = await getActions({ account, limit: 1, skip: 0 });
  if (probe.status !== 200) return probe;
  const total = (probe.body.total && probe.body.total.value) || 0;
  if (pos < 0) {                       // "latest N": positions total-N .. total-1
    limit = Math.min(Math.abs(offset) || 20, 1000); skip = 0;
  } else {                             // nodeos: [pos, pos+offset] (offset >= 0) or [pos+offset, pos] (offset < 0)
    const lo = offset >= 0 ? pos : Math.max(0, pos + offset), hi = offset >= 0 ? pos + offset : pos;
    const top = Math.min(hi, total - 1);
    if (top < lo) return { status: 200, body: { actions: [], last_irreversible_block: probe.body.lib, federation: { positional: 'approximate' } } };
    skip = total - 1 - top; limit = Math.min(top - lo + 1, 1000);
    approximate = true;
  }
  const page = await getActions({ account, limit, skip });
  if (page.status !== 200) return page;
  const acts = (page.body.actions || []).map((a, i) => v1Action(a, total - 1 - skip - i)).reverse(); // nodeos: ascending
  return {
    status: 200,
    body: {
      actions: acts,
      last_irreversible_block: page.body.lib,
      federation: { positional: approximate ? 'approximate' : 'exact-order', ...(page.body.partial ? { partial: true } : {}), boundary: pubBoundary(boundary()) },
    },
  };
}
// /v1/history/get_transaction: new-then-legacy.
async function getTransactionV1(params) {
  const id = String(params.id || '');
  const local = await localPost('/v1/history/get_transaction', { id });
  if (local.ok && local.json) return { status: 200, body: local.json };
  const legacy = await legacyPost('/v1/history/get_transaction', { id });
  if (legacy.ok && legacy.json) return { status: 200, body: legacy.json };
  if (local.status === 404 || legacy.status === 404) return { status: 404, body: (legacy.json || local.json || { error: `transaction ${id} not found` }) };
  return { status: 502, body: errBody('both history sources unreachable', local, legacy) };
}

// Aggregate health: local is the post-cut source of truth; legacy is checked
// so the boundary's pre-cut half is monitored through the same URL.
async function health() {
  const b = boundary();
  const [local, legacy] = await Promise.all([localGet('/v2/health'), legacyHealthCached()]);
  const localServices = (local.ok && local.json && local.json.health) || [];
  const svc = (name) => localServices.find((s) => s.service === name);
  const lastIndexed = svc('Indexer') && svc('Indexer').service_data && svc('Indexer').service_data.last_indexed_block;
  const rpcHead = svc('PulseVM-RPC') && svc('PulseVM-RPC').service_data && svc('PulseVM-RPC').service_data.head_block_num;
  // local.ok mirrors the agent's hydration predicate: all services OK, with
  // the IDLE-AT-CUT allowance — hyperion-rs reports `Indexer: Warning,
  // last_indexed_block: 0` when zero post-cut blocks exist (observed live;
  // an all-OK requirement wedges the flip gate on an idle chain).
  const nonIndexerOk = localServices.length > 0
    && localServices.filter((s) => s.service !== 'Indexer').every((s) => s.status === 'OK');
  const allOk = localServices.length > 0 && localServices.every((s) => s.status === 'OK');
  const idleAtCut = nonIndexerOk && (lastIndexed || 0) === 0
    && typeof rpcHead === 'number' && rpcHead <= b.cut_block;
  const localOk = local.ok && (allOk || idleAtCut);
  const legacyOk = legacy.ok && Array.isArray(legacy.json && legacy.json.health);
  return {
    status: 200,
    body: {
      version: local.ok ? local.json.version : undefined,
      chain: local.ok ? local.json.chain : undefined,
      health: localServices,
      federation: {
        boundary: pubBoundary(b),
        local: { url: LOCAL, ok: localOk, last_indexed_block: lastIndexed, idle_at_cut: idleAtCut || undefined },
        legacy: { url: LEGACY, ok: legacyOk },
        chain: { url: CHAIN_URL },
      },
    },
  };
}

function pubBoundary(b) {
  return !staged(b)
    ? { staged: false }
    : { cut_block: b.cut_block, cut_time: b.cut_time, cut_block_id: b.cut_block_id };
}
function errBody(msg, ...ups) {
  return { error: msg, upstreams: ups.map((u) => ({ status: u.status, error: u.error || u.text })) };
}
function sourceErrors(srcs) {
  const out = {};
  for (const [k, r] of Object.entries(srcs)) if (r && !r.ok) out[k] = { status: r.status, error: r.error || r.text };
  return out;
}

// ---- request plumbing ------------------------------------------------------
function readBody(req) {
  return new Promise((resolve) => {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 1e6) req.destroy(); });
    req.on('end', () => resolve(body));
  });
}

async function routeFederated(req) {
  const url = new URL(req.url, 'http://x');
  const params = Object.fromEntries(url.searchParams);
  if (req.method === 'POST') {
    try { Object.assign(params, JSON.parse((await readBody(req)) || '{}')); } catch { /* query only */ }
  }
  const p = url.pathname.replace(/\/+$/, '') || '/';
  if (p === '/v2/health') return health();
  if (p === '/v2/history/get_actions') return getActions(params);
  if (p === '/v2/history/get_transaction') return getTransaction(params);
  // State: chain truth, index discovery. Before the cut is staged these still
  // verify against CHAIN_URL — which, pre-flip, is the new chain's edge; so
  // pre-ceremony the router serves them legacy-only, exactly as before.
  const pre = !staged(boundary());
  if (p === '/v2/state/get_tokens') return pre ? legacyPass(`${p}?${qs(params)}`) : getTokens(params);
  if (p === '/v2/state/get_account') return pre ? legacyPass(`${p}?${qs(params)}`) : getAccount(params);
  if (p === '/v2/state/get_key_accounts') return pre ? legacyPass(`${p}?${qs(params)}`) : getKeyAccounts(params);
  if (p === '/v1/history/get_key_accounts') return pre ? legacyPass(p, params) : getKeyAccounts(params);
  if (p === '/v1/history/get_controlled_accounts') return pre ? legacyPass(p, params) : getControlledAccounts(params);
  if (p === '/v1/history/get_actions') return pre ? legacyPass(p, params) : getActionsV1(params);
  if (p === '/v1/history/get_transaction') return pre ? legacyPass(p, params) : getTransactionV1(params);
  if (p === '/' || p === '/v2') {
    return { status: 200, body: { service: 'hyperion-federator', local: LOCAL, legacy: LEGACY, chain: CHAIN_URL, boundary: pubBoundary(boundary()) } };
  }
  // Everything else under /v2 (and /v1/history): local first, legacy
  // fallback. Other /v2/state/* endpoints (get_links, get_proposals,
  // get_voters, ...) are answered from an index, not the chain: they are
  // tagged so clients and operators can tell (docs/V1-COVERAGE.md).
  if (p.startsWith('/v2/') || p.startsWith('/v1/history/')) {
    const v1 = p.startsWith('/v1/');
    const indexOnly = p.startsWith('/v2/state/') ? { 'x-pulse-federation': 'index-only' } : undefined;
    const path = v1 ? p : `${p}?${qs(params)}`;
    const local = v1 ? await localPost(p, params) : await localGet(path);
    if (local.ok) return { status: 200, body: local.json, headers: indexOnly };
    const legacy = v1 ? await legacyPost(p, params) : await legacyGet(path);
    if (legacy.ok) return { status: 200, body: v1 ? legacy.json : { ...legacy.json, _premigration: true }, headers: indexOnly };
    return { status: 502, body: errBody(`no source answered ${p}`, local, legacy) };
  }
  return { status: 404, body: { error: `not a history endpoint: ${p}` } };
}
async function legacyPass(path, v1Body) {
  const r = v1Body ? await legacyPost(path, v1Body) : await legacyGet(path);
  return { status: r.ok ? 200 : (r.status || 502), body: r.json || errBody('legacy unreachable', r) };
}

function serve(port, handler, label) {
  const srv = http.createServer((req, res) => {
    res.setHeader('access-control-allow-origin', '*');
    if (req.method === 'OPTIONS') {
      res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
      res.setHeader('access-control-allow-headers', 'content-type');
      res.statusCode = 204;
      return res.end();
    }
    handler(req)
      .then(({ status, body, headers }) => {
        res.statusCode = status;
        for (const [k, v] of Object.entries(headers || {})) res.setHeader(k, v);
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(body));
      })
      .catch((e) => {
        res.statusCode = 500;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ error: String((e && e.message) || e) }));
      });
  });
  srv.listen(port, HOST, () => console.log(`${label} on ${HOST}:${srv.address().port} (local=${LOCAL}, legacy=${LEGACY}, chain=${CHAIN_URL}, boundary=${BOUNDARY_FILE})`));
  return srv;
}

// Pure legacy proxy: "the /v2 you already had", for the pre-flip upstream.
async function routePassthrough(req) {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'POST' && url.pathname.startsWith('/v1/')) {
    let body = {}; try { body = JSON.parse((await readBody(req)) || '{}'); } catch { /* empty */ }
    return legacyPass(url.pathname, body);
  }
  let path = url.pathname + url.search;
  if (req.method === 'POST') {
    const body = await readBody(req);
    try {
      const params = { ...Object.fromEntries(url.searchParams), ...JSON.parse(body || '{}') };
      path = `${url.pathname}?${qs(params)}`;
    } catch { /* keep as-is */ }
  }
  const r = await legacyGet(path);
  return { status: r.ok ? 200 : (r.status || 502), body: r.json || errBody('legacy unreachable', r) };
}

module.exports = { keyInfo, keyCanon, keySpellings, ripemd160js, tokenRow };

if (require.main === module) {
  serve(PORT, routeFederated, 'hyperion-federator (federating router)');
  serve(PASSTHROUGH_PORT, routePassthrough, 'hyperion-federator (legacy passthrough)');
}
