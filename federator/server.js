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
// cut mid-ceremony and writes BOUNDARY_FILE ({cut_block, cut_block_id,
// cut_time, chain_id, target_chain_id}); this server re-reads it on mtime
// change and VALIDATES it before serving any history (see "boundary" below):
// the block id must encode cut_block, CHAIN_URL must serve that chain_id with
// block cut_block == cut_block_id, and the legacy archive must not report
// another chain_id. Missing, corrupt, stale or mismatched = FAIL CLOSED (503),
// never "unlimited legacy history". ALLOW_NO_BOUNDARY=1 restores the old
// pre-ceremony legacy-only answer while no file has ever been loaded (staging
// only; the public /v2 points at the passthrough port until the flip anyway).
//
// Env: LOCAL (http://127.0.0.1:7000), LEGACY (https://test.proton.eosusa.io),
//      CHAIN_URL (http://127.0.0.1:8899 — the /v1 edge, or the node's native
//      base http://127.0.0.1:9650/ext/bc/<BID>), BOUNDARY_FILE
//      (/etc/pulse-cutover/boundary.json), PORT, PASSTHROUGH_PORT, HOST
//      (127.0.0.1), LOCAL_TIMEOUT_MS (8000), LEGACY_TIMEOUT_MS (8000),
//      CHAIN_TIMEOUT_MS (8000), CHAIN_CONCURRENCY (8), ALLOW_NO_BOUNDARY (0),
//      BOUNDARY_RECHECK_MS (30000), BOUNDARY_BLOCK_CHECK (on; "off" only when
//      the chain cannot serve block cut_block — then identity rests on
//      chain_id + head >= cut, which a same-chain_id source continuation could
//      also satisfy).
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

// ---- boundary --------------------------------------------------------------------------------------------
// The ONLY thing that tells this router where legacy history ends. Rule for every history answer: rows from
// LOCAL must be strictly above cut_block, rows from LEGACY at or below it; anything else is not canonical
// history of the migrated chain (legacy rows above the cut are the source's discarded burn-off blocks, or
// an unrelated continuation of a public archive) and is never returned.
const ALLOW_NO_BOUNDARY = process.env.ALLOW_NO_BOUNDARY === '1';
const BOUNDARY_RECHECK_MS = Number(process.env.BOUNDARY_RECHECK_MS || 30000);
const BOUNDARY_BLOCK_CHECK = process.env.BOUNDARY_BLOCK_CHECK !== 'off';
const HEX64 = /^[0-9a-f]{64}$/;
// ISO-8601 with or without a zone (Antelope/Hyperion times are UTC without one). null = not a time.
function parseTime(v) {
  if (typeof v !== 'string' || !v.trim()) return null;
  const s = v.trim();
  if (!/^\d{4}-\d\d-\d\d(T|$)/.test(s)) return null;
  const ms = Date.parse(/(Z|[+-]\d\d:?\d\d)$/.test(s) || !s.includes('T') ? s : s + 'Z');
  return Number.isFinite(ms) ? ms : null;
}
function parseBoundary(j) {
  if (!j || typeof j !== 'object' || Array.isArray(j)) throw new Error('not a JSON object');
  const cut = Number(j.cut_block);
  if (!Number.isSafeInteger(cut) || cut <= 0) throw new Error(`cut_block ${JSON.stringify(j.cut_block)} is not a positive integer`);
  const id = String(j.cut_block_id == null ? '' : j.cut_block_id).toLowerCase();
  if (!HEX64.test(id)) throw new Error('cut_block_id is not a 64-hex block id');
  // Antelope block ids carry the block number in their first 4 bytes.
  const encoded = parseInt(id.slice(0, 8), 16);
  if (encoded !== cut) throw new Error(`cut_block_id encodes block ${encoded}, not cut_block ${cut}`);
  const chainId = String(j.chain_id == null ? '' : j.chain_id).toLowerCase();
  if (!HEX64.test(chainId)) throw new Error('chain_id is not a 64-hex chain id');
  const target = j.target_chain_id == null ? null : String(j.target_chain_id).toLowerCase();
  if (target != null && !HEX64.test(target)) throw new Error('target_chain_id is not a 64-hex chain id');
  const t = parseTime(j.cut_time);
  if (t == null) throw new Error('cut_time is not an ISO-8601 time');
  return { cut_block: cut, cut_block_id: id, chain_id: chainId, target_chain_id: target, cut_time: String(j.cut_time), cut_time_ms: t };
}
// status: absent (no file ever seen) | missing (file vanished after one was loaded) | invalid (unparseable /
// inconsistent) | unverified (not yet checked against the upstreams) | mismatch (an upstream disagrees) | valid
let B = { status: 'absent', error: 'no boundary file yet', mtime: null, b: null, verifiedAt: 0, identity: null };
let _everLoaded = false;
function loadBoundary() {
  let st;
  try { st = fs.statSync(BOUNDARY_FILE); } catch {
    if (_everLoaded && B.status !== 'missing') {
      B = { status: 'missing', error: `boundary file ${BOUNDARY_FILE} disappeared after it was loaded`, mtime: null, b: null, verifiedAt: 0, identity: null };
      console.error(`boundary: ${B.error}; history is refused until it is restored`);
    }
    return B;
  }
  if (st.mtimeMs === B.mtime) return B;
  _everLoaded = true; // from here on a missing or corrupt file is never "pre-ceremony"
  try {
    const b = parseBoundary(JSON.parse(fs.readFileSync(BOUNDARY_FILE, 'utf8')));
    B = { status: 'unverified', error: 'not yet checked against the chain', mtime: st.mtimeMs, b, verifiedAt: 0, identity: null };
    console.log(`boundary loaded: cut_block=${b.cut_block} cut_block_id=${b.cut_block_id} chain_id=${b.chain_id.slice(0, 12)}…`);
  } catch (e) {
    B = { status: 'invalid', error: `boundary file ${BOUNDARY_FILE} is invalid: ${e.message}`, mtime: st.mtimeMs, b: null, verifiedAt: 0, identity: null };
    console.error(`boundary: ${B.error}; history is refused until it is fixed`);
  }
  return B;
}
function legacyChainId(r) {
  const svc = r && r.ok && r.json && Array.isArray(r.json.health) ? r.json.health : [];
  for (const s of svc) {
    const c = s && s.service_data && s.service_data.chain_id;
    if (typeof c === 'string' && HEX64.test(c.toLowerCase())) return c.toLowerCase();
  }
  const top = r && r.ok && r.json && r.json.chain_id;
  return typeof top === 'string' && HEX64.test(top.toLowerCase()) ? top.toLowerCase() : null;
}
// Check a loaded boundary against the upstreams. A failure to REACH an upstream keeps an earlier successful
// verification of the same file (identity unchanged as far as anyone can tell); a disagreement never does.
async function verifyBoundary(cur) {
  const b = cur.b;
  const keep = (why) => (cur.verifiedAt ? { ...cur, recheck_error: why } : { ...cur, status: 'unverified', error: why });
  const mismatch = (why) => ({ ...cur, status: 'mismatch', error: why, verifiedAt: 0, checkedAt: Date.now() });
  const info = await chain('get_info', {});
  if (!info.ok || !info.json) return keep(`chain ${CHAIN_URL} unreachable (${info.error || `HTTP ${info.status}`}): cannot check the boundary`);
  const cid = String(info.json.chain_id || '').toLowerCase();
  if (cid !== b.chain_id && cid !== b.target_chain_id) {
    return mismatch(`chain ${CHAIN_URL} serves chain_id ${cid || '(none)'}, the boundary names ${b.chain_id}${b.target_chain_id ? ` / target ${b.target_chain_id}` : ''}: stale or foreign boundary`);
  }
  const head = Number(info.json.head_block_num);
  if (!(head >= b.cut_block)) return mismatch(`chain head ${info.json.head_block_num} is below cut_block ${b.cut_block}: CHAIN_URL is not the post-cut chain of this boundary`);
  let blockCheck = 'off (BOUNDARY_BLOCK_CHECK=off)';
  if (BOUNDARY_BLOCK_CHECK) {
    const blk = await chain('get_block', { block_num_or_id: String(b.cut_block) });
    const got = blk.ok && blk.json && typeof blk.json.id === 'string' ? blk.json.id.toLowerCase() : null;
    if (!got) return keep(`cannot read block ${b.cut_block} from ${CHAIN_URL} to check cut_block_id (${blk.error || `HTTP ${blk.status}`})`);
    if (got !== b.cut_block_id) return mismatch(`chain block ${b.cut_block} is ${got}, the boundary says ${b.cut_block_id}: wrong cut or wrong chain`);
    blockCheck = 'verified';
  }
  const lcid = legacyChainId(await legacyHealthCached());
  if (lcid && lcid !== b.chain_id) return mismatch(`legacy history ${LEGACY} reports chain_id ${lcid}, the boundary's source chain is ${b.chain_id}`);
  return { ...cur, status: 'valid', error: null, verifiedAt: Date.now(), recheck_error: undefined,
    identity: { chain_id: cid, head_at_check: head, cut_block_id: blockCheck, legacy_chain_id: lcid ? 'verified' : 'not reported by the legacy archive' } };
}
let _verifying = null, _lastAttempt = 0;
// -> {b} (valid boundary) | {legacyOnly: true} (ALLOW_NO_BOUNDARY and no file ever) | {fail: reply}
async function gate() {
  const cur = loadBoundary();
  if (cur.status === 'absent') return ALLOW_NO_BOUNDARY ? { legacyOnly: true } : { fail: boundaryFail(cur) };
  if (cur.status === 'missing' || cur.status === 'invalid') return { fail: boundaryFail(cur) };
  const now = Date.now();
  const due = cur.status !== 'valid' || now - cur.verifiedAt > BOUNDARY_RECHECK_MS;
  if (due && !_verifying && (cur.status === 'valid' || now - _lastAttempt >= 1000)) {
    _lastAttempt = now;
    _verifying = verifyBoundary(cur).then((next) => {
      if (B.mtime === cur.mtime) {
        if (next.status !== B.status) console.log(`boundary ${next.status}${next.error ? `: ${next.error}` : ''}`);
        B = next;
      }
    }).catch((e) => console.error(`boundary check failed: ${e && e.message}`)).finally(() => { _verifying = null; });
  }
  if (_verifying && due) await _verifying;
  return B.status === 'valid' && B.b ? { b: B.b } : { fail: boundaryFail(B) };
}
function boundaryFail(st) {
  return { status: 503, headers: { 'x-pulse-federation': `boundary-${st.status}`, 'retry-after': '5' },
    body: { error: `history unavailable: boundary ${st.status}: ${st.error}`, federation: { status: 'unavailable', boundary: pubBoundary(st) } } };
}
const num = (v) => (v == null || v === '' ? NaN : Number(v));
// Legacy rows must be at or below the cut; a legacy row AT the cut must carry the cut block id.
const legacyAtCutMismatch = (rows, b) => rows.some((a) => num(a.block_num) === b.cut_block && typeof a.block_id === 'string' && a.block_id.toLowerCase() !== b.cut_block_id);
const legacyIdentityFail = (b) => ({ status: 503, headers: { 'x-pulse-federation': 'legacy-identity-mismatch' },
  body: { error: `the legacy archive (${LEGACY}) holds a different block at the cut ${b.cut_block} than ${b.cut_block_id}: it is not this chain's history`, federation: { status: 'unavailable' } } });

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
// Cache for chain reads, keyed by chain_id + head block ID (not head number: two chains, or two forks, can
// share a height). Reads are not pinned to one block: each answer says which head it started from.
const CACHE_MAX = 5000;
const _cache = new Map();
async function chainHead() {
  const r = await chain('get_info', {});
  return r.ok && r.json && r.json.head_block_id && r.json.chain_id ? r.json : (r.ok && r.json ? { ...r.json, _unkeyed: true } : null);
}
const headKey = (info) => (info && !info._unkeyed ? `${info.chain_id}|${info.head_block_id}` : null);
async function chainCached(info, name, obj) {
  const hk = headKey(info);
  const key = `${hk}|${name}|${JSON.stringify(obj)}`;
  if (hk && _cache.has(key)) return _cache.get(key);
  const r = await chain(name, obj);
  if (hk && r.ok) {
    if (_cache.size >= CACHE_MAX) _cache.delete(_cache.keys().next().value);
    _cache.set(key, r);
  }
  return r;
}
// A chain answer that means "no such account / no such token row": 404, or a 4xx/500 whose nodeos error says so.
// Everything else that is not a success (timeouts, 502-504, 429, an unexplained 500) is UNAVAILABLE, never absence.
const ABSENT_RE = /unknown key|unknown account|account_query_exception|unknown_account|does not exist|not found|no abi|abi .*not found|fail(ed)? to retrieve|account .* not found/i;
function chainAbsent(r) {
  if (r.ok) return false;
  if (r.status === 404) return true;
  if ((r.status === 400 || r.status === 500) && r.json) return ABSENT_RE.test(JSON.stringify(r.json).slice(0, 4000));
  return false;
}
const verifiedAt = (info) => ({ verified_at_block: info.head_block_num, verified_at_block_id: info.head_block_id, chain_id: info.chain_id,
  read_consistency: 'each account read at the chain head current when it was read (at or after verified_at_block)' });
// Hyperion token shape from a chain balance string "12.3400 XPR".
function tokenRow(contract, s) {
  const m = String(s).trim().match(/^(-?\d+)(?:\.(\d+))?\s+([A-Z]{1,7})$/);
  if (!m) return null;
  return { symbol: m[3], precision: (m[2] || '').length, amount: parseFloat(`${m[1]}.${m[2] || '0'}`), contract };
}
// Verify discovered accounts against their CURRENT permissions on the chain.
async function verifyAccounts(names, info, predicate) {
  const sorted = [...names].sort();
  const results = await pool(sorted, CHAIN_CONCURRENCY, (n) => chainCached(info, 'get_account', { account_name: n }));
  const out = [], errors = [];
  results.forEach((r, i) => {
    if (r.ok && r.json) { if (predicate(r.json)) out.push(r.json.account_name || sorted[i]); }
    else if (!chainAbsent(r)) errors.push({ account: sorted[i], status: r.status, error: r.error || r.text || (r.json && (r.json.message || r.json.error)) });
    // absent on the chain now: not an error, not a match.
  });
  return { accounts: out, errors };
}
const permHasKey = (want) => (acct) => (acct.permissions || []).some((p) => ((p.required_auth && p.required_auth.keys) || []).some((k) => keyCanon(k.key) === want));
const permHasActor = (actor) => (acct) => (acct.permissions || []).some((p) => ((p.required_auth && p.required_auth.accounts) || []).some((a) => a.permission && a.permission.actor === actor));

// ---- federated history -----------------------------------------------------------------------------------------
// A source answer is UNAVAILABLE (timeout, 429, 5xx, unparseable) or a definitive answer. Unavailable must
// never be turned into "absent": a deposit poller has to tell "not there" from "could not look".
const unavailable = (r) => !r.ok && !(r.status === 404 || r.status === 410);
// Is the local (post-cut) index behind the chain? -> 'not_indexed_yet' | 'absent' | 'absent_index_state_unknown'
let _localHealth = { at: 0, value: null };
async function absenceStatus() {
  const now = Date.now();
  if (!_localHealth.value || now - _localHealth.at > 2000) _localHealth = { at: now, value: await localGet('/v2/health') };
  const h = _localHealth.value;
  const svc = (name) => ((h.ok && h.json && h.json.health) || []).find((x) => x.service === name);
  const indexed = svc('Indexer') && svc('Indexer').service_data && svc('Indexer').service_data.last_indexed_block;
  const head = svc('PulseVM-RPC') && svc('PulseVM-RPC').service_data && svc('PulseVM-RPC').service_data.head_block_num;
  if (typeof indexed !== 'number' || typeof head !== 'number') return 'absent_index_state_unknown';
  return indexed < head ? 'not_indexed_yet' : 'absent';
}
const statusHeaders = (st, extra) => ({ 'x-pulse-federation-status': st, ...(extra || {}) });

// One logical timeline: [ legacy pre-cut ] ++ [ local post-cut ] in ascending order, reversed for descending.
// The page [skip, skip+limit) is served by the FIRST source of the requested order while its total covers it,
// then by the second at skip - firstTotal. That seam is only exact when the first source's total is exact
// ("eq") and no row had to be dropped by the boundary rule; otherwise the answer says so (partial + gte).
async function getActions(params, b) {
  const account = String(params.account || '');
  const limit = Math.max(1, Math.min(Number(params.limit || 40) || 40, 1000));
  const skip = Math.max(0, Number(params.skip || 0) || 0);
  const sortIn = params.sort == null || params.sort === '' ? 'desc' : String(params.sort).toLowerCase();
  if (!['desc', 'asc', '1', '-1'].includes(sortIn)) return { status: 400, body: { error: `sort must be "asc" or "desc", not ${JSON.stringify(params.sort)}` } };
  const asc = sortIn === 'asc' || sortIn === '1';
  const extra = {};
  for (const [k, v] of Object.entries(params)) if (!['account', 'limit', 'skip', 'sort', 'before', 'after'].includes(k)) extra[k] = v;
  const time = {};
  for (const k of ['before', 'after']) {
    if (params[k] == null || params[k] === '') continue;
    if (parseTime(String(params[k])) == null) return { status: 400, body: { error: `${k} must be an ISO-8601 time (got ${JSON.stringify(params[k])})` } };
    time[k] = String(params[k]);
  }
  // The caller's filters are kept on both sides. Legacy is additionally bounded by the cut time (and its rows by
  // the cut block); a source whose half lies wholly outside the caller's window is not asked at all.
  const beforeMs = time.before ? parseTime(time.before) : null, afterMs = time.after ? parseTime(time.after) : null;
  const legacyBefore = beforeMs != null && beforeMs < b.cut_time_ms ? time.before : b.cut_time;
  const wantLegacy = afterMs == null || afterMs <= b.cut_time_ms;
  const wantLocal = beforeMs == null || beforeMs > b.cut_time_ms;
  const base = { ...(account ? { account } : {}), sort: asc ? 'asc' : 'desc', ...extra };
  const src = {
    local: { want: wantLocal, get: (sk, lim) => localGet(`/v2/history/get_actions?${qs({ ...base, ...time, limit: lim, skip: sk })}`),
      keep: (a) => num(a.block_num) > b.cut_block, tag: (a) => a },
    legacy: { want: wantLegacy, get: (sk, lim) => legacyGet(`/v2/history/get_actions?${qs({ ...base, ...(time.after ? { after: time.after } : {}), before: legacyBefore, limit: lim, skip: sk })}`),
      keep: (a) => num(a.block_num) <= b.cut_block, tag: (a) => ({ ...a, _premigration: true }) },
  };
  const read = async (name, sk, lim) => {
    const s = src[name];
    if (!s.want) return { name, ok: true, rows: [], total: 0, rel: 'eq', dropped: 0, skipped: true };
    const r = await s.get(sk, lim);
    if (!r.ok) return { name, ok: false, r, rows: [], total: 0, rel: 'gte', dropped: 0 };
    const all = (r.json && Array.isArray(r.json.actions)) ? r.json.actions : [];
    if (name === 'legacy' && legacyAtCutMismatch(all, b)) return { name, identity: true };
    const kept = all.filter(s.keep);
    const t = r.json && r.json.total;
    return { name, ok: true, r, rows: kept.map(s.tag), total: (t && Number(t.value)) || 0, rel: t && t.relation === 'eq' ? 'eq' : 'gte', dropped: all.length - kept.length, lib: r.json && r.json.lib };
  };

  // Global feed (no account): post-cut actions from the local index. If it is unavailable, pre-cut rows from
  // the legacy archive (bounded by the cut) are returned, clearly marked partial: the post-cut half is missing.
  if (!account) {
    const l = await read('local', skip, limit);
    if (l.ok) {
      return { status: 200, headers: statusHeaders(l.dropped ? 'partial' : 'ok'), body: { ...l.r.json, actions: l.rows, federated: true, sort: base.sort, boundary: pubBoundary(B),
        ...(l.dropped ? { partial: true, dropped_at_or_below_cut: l.dropped } : {}) } };
    }
    const g = await read('legacy', skip, limit);
    if (g.identity) return legacyIdentityFail(b);
    if (!g.ok) return { status: 503, headers: statusHeaders('unavailable'), body: { ...errBody('both history sources unavailable', l.r, g.r), federation: { status: 'unavailable' } } };
    return { status: 200, headers: statusHeaders('partial'), body: { actions: g.rows, total: { value: g.total, relation: 'gte' }, federated: true, legacy_only: true, partial: true,
      source_errors: sourceErrors({ local: l.r }), note: 'post-cut (local) history unavailable: pre-cut rows only, bounded by the cut', boundary: pubBoundary(B) } };
  }

  const [firstName, secondName] = asc ? ['legacy', 'local'] : ['local', 'legacy'];
  const first = await read(firstName, skip, limit);
  if (first.identity) return legacyIdentityFail(b);
  if (!first.ok) {
    // The first half of the timeline could not be read: its length is unknown, so no row of the second half
    // can be placed. Refuse instead of returning rows at guessed positions.
    return { status: 503, headers: statusHeaders('unavailable'), body: { ...errBody(`${firstName} history unavailable: cannot place the page on the timeline`, first.r),
      federation: { status: 'unavailable', unavailable: [firstName] } } };
  }
  const need = limit - first.rows.length;
  const seamExact = first.rel === 'eq' && first.dropped === 0;
  // The second source is always asked once: for its rows when the page continues into it, else for its total.
  const secondSkip = seamExact ? Math.max(0, skip - first.total) : 0;
  const second = await read(secondName, secondSkip, need > 0 && seamExact ? need : 1);
  if (second.identity) return legacyIdentityFail(b);
  const secondRows = need > 0 && seamExact && second.ok ? second.rows.slice(0, need) : [];
  const local = firstName === 'local' ? first : second, legacy = firstName === 'legacy' ? first : second;
  const pageShort = need > 0 && (!seamExact || !second.ok);
  const exact = first.ok && second.ok && first.rel === 'eq' && second.rel === 'eq' && !first.dropped && !second.dropped;
  const partial = !second.ok || !exact || pageShort;
  return {
    status: 200,
    headers: statusHeaders(partial ? 'partial' : 'ok'),
    body: {
      actions: [...first.rows, ...secondRows],
      total: { value: local.total + legacy.total, relation: exact ? 'eq' : 'gte' },
      local_total: local.total,
      legacy_total: legacy.total,
      lib: local.ok && !local.skipped ? local.lib : undefined,
      sort: base.sort,
      federated: true,
      ...(partial ? { partial: true, source_errors: sourceErrors({ [firstName]: first.r, [secondName]: second.r }),
        ...(pageShort ? { page_may_be_short: true, reason: !second.ok ? `${secondName} history unavailable` : `the ${firstName} total is a lower bound or rows were dropped at the cut: the seam position is unknown` } : {}),
        ...((first.dropped || second.dropped) ? { dropped: { [firstName]: first.dropped, [secondName]: second.dropped } } : {}) } : {}),
      boundary: pubBoundary(B),
    },
  };
}

// A transaction is post-cut (local, every action above the cut) or pre-cut (legacy, every action at or below
// it). A legacy hit above the cut is NOT canonical (discarded burn-off branch or an unrelated archive).
async function getTransaction(params, b) {
  const id = String(params.id || '');
  const local = await localGet(`/v2/history/get_transaction?id=${encodeURIComponent(id)}`);
  const lActs = local.ok && local.json && Array.isArray(local.json.actions) ? local.json.actions : null;
  if (lActs && lActs.length) {
    if (lActs.every((a) => num(a.block_num) > b.cut_block)) return { status: 200, headers: statusHeaders('found'), body: { ...local.json, federated: true } };
    return { status: 503, headers: statusHeaders('unavailable'), body: { error: `the local (post-cut) index returned transaction ${id} at or below the cut ${b.cut_block}: boundary inconsistency, refusing to answer`, federation: { status: 'unavailable' } } };
  }
  const legacy = await legacyGet(`/v2/history/get_transaction?id=${encodeURIComponent(id)}`);
  const gActs = legacy.ok && legacy.json && Array.isArray(legacy.json.actions) ? legacy.json.actions : null;
  let aboveCut = 0;
  if (gActs && gActs.length) {
    if (legacyAtCutMismatch(gActs, b)) return legacyIdentityFail(b);
    if (gActs.every((a) => num(a.block_num) <= b.cut_block)) return { status: 200, headers: statusHeaders('found'), body: { ...legacy.json, _premigration: true, federated: true } };
    aboveCut = gActs.length;
  }
  const down = { ...(unavailable(local) ? { local } : {}), ...(!aboveCut && unavailable(legacy) ? { legacy } : {}) };
  if (Object.keys(down).length) {
    return { status: 503, headers: statusHeaders('unavailable'), body: { error: `transaction ${id} not found in the sources that answered, and ${Object.keys(down).join(' + ')} history is unavailable: not a definitive answer`,
      federation: { status: 'unavailable', source_errors: sourceErrors(down) } } };
  }
  const st = await absenceStatus();
  const shape = (local.ok && local.json) || (legacy.ok && legacy.json) || null;
  const federation = { status: st, ...(aboveCut ? { legacy_above_cut_ignored: true, note: `the legacy archive has ${id} above the cut ${b.cut_block}: not part of this chain's history` } : {}) };
  return shape ? { status: local.ok || legacy.ok ? 200 : 404, headers: statusHeaders(st), body: { ...shape, actions: [], federated: true, federation } }
    : { status: 404, headers: statusHeaders(st), body: { error: `transaction ${id} not found`, federation } };
}

// Discovery for a key: both indexes, every spelling. Returns {names, ok:{local, legacy}, last}.
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
  const v = await verifyAccounts(d.names, info, permHasKey(keyCanon(key)));
  const partial = !d.ok.local || !d.ok.legacy || v.errors.length > 0;
  return { status: 200, headers: statusHeaders(partial ? 'partial' : 'ok'), body: { account_names: v.accounts, federated: true, ...verifiedAt(info), ...(partial ? { partial: true, source_errors: { ...sourceErrors({ local: d.last.local, legacy: d.last.legacy }), ...(v.errors.length ? { chain: v.errors } : {}) } } : {}) } };
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
  const v = await verifyAccounts(names, info, permHasActor(actor));
  const partial = !local.ok || !legacy.ok || v.errors.length > 0;
  return { status: 200, headers: statusHeaders(partial ? 'partial' : 'ok'), body: { controlled_accounts: v.accounts, ...verifiedAt(info),
    ...(partial ? { partial: true, source_errors: { ...sourceErrors({ local, legacy }), ...(v.errors.length ? { chain: v.errors } : {}) } } : {}) } };
}

// Token discovery -> chain balances. Contracts come from BOTH indexes (legacy:
// held before the cut; local: touched after it); amounts come only from the
// chain's get_currency_balance. A contract with no balance row is omitted.
async function tokensFor(account, info) {
  const path = `/v2/state/get_tokens?${qs({ account })}`;
  const [local, legacy] = await Promise.all([localGet(path), legacyGet(path)]);
  if (!local.ok && !legacy.ok) return { error: errBody('both discovery sources unreachable', local, legacy) };
  const contracts = new Set();
  for (const r of [local, legacy]) if (r.ok) ((r.json && r.json.tokens) || []).forEach((t) => t && t.contract && contracts.add(String(t.contract)));
  const list = [...contracts].sort();
  const balances = await pool(list, CHAIN_CONCURRENCY, (code) => chainCached(info, 'get_currency_balance', { code, account }));
  const tokens = [], chainErrors = [];
  balances.forEach((r, i) => {
    if (r.ok && Array.isArray(r.json)) r.json.forEach((s) => { const t = tokenRow(list[i], s); if (t) tokens.push(t); });
    else if (!chainAbsent(r)) chainErrors.push({ contract: list[i], status: r.status, error: r.error || r.text || (r.json && (r.json.message || r.json.error)) });
    // no such token contract / no balance row now -> omitted
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
  const t = await tokensFor(account, info);
  if (t.error) return { status: 502, body: t.error };
  return { status: 200, headers: statusHeaders(t.partial ? 'partial' : 'ok'), body: { account, tokens: t.tokens, federated: true, ...verifiedAt(info), ...(t.partial ? { partial: true, source_errors: t.source_errors } : {}) } };
}
// hyperion-rs shape: {account, lib, tokens, permissions, total_actions, actions}
// with permissions from the chain (nodeos get_account permission objects),
// tokens as above and actions from the federated history (limit 20).
async function getAccount(params, b) {
  const account = String(params.account || '').trim();
  if (!account) return { status: 400, body: { error: 'account is required' } };
  const info = await chainHead();
  if (!info) return { status: 502, body: { error: `chain (${CHAIN_URL}) unreachable: account state comes from the chain only` } };
  const acct = await chainCached(info, 'get_account', { account_name: account });
  if (!acct.ok || !acct.json) {
    if (!chainAbsent(acct)) return { status: 502, body: errBody(`chain get_account failed`, acct) };
    return { status: 404, body: { error: `account ${account} not found on chain`, chain: acct.json || acct.text } };
  }
  const [t, h] = await Promise.all([tokensFor(account, info), getActions({ account, limit: 20 }, b)]);
  const partial = !!t.error || t.partial || h.status !== 200 || !!h.body.partial;
  return {
    status: 200,
    headers: statusHeaders(partial ? 'partial' : 'ok'),
    body: {
      account,
      lib: info.last_irreversible_block_num,
      tokens: t.error ? [] : t.tokens,
      permissions: acct.json.permissions || [],
      total_actions: h.status === 200 ? h.body.total && h.body.total.value : undefined,
      actions: h.status === 200 ? h.body.actions : [],
      federated: true,
      ...verifiedAt(info),
      ...(partial ? { partial: true } : {}),
    },
  };
}

// /v1/history/get_actions (nodeos history_plugin shape) served from the
// federated /v2 timeline: exact ORDER across the cut, but account_action_seq
// is SYNTHESIZED from the combined position — it is not the history_plugin's
// stored per-account sequence (neither index keeps one; Hyperion rows carry a
// per-receiver recv_sequence, passed through inside action_trace.receipt).
// Never use it as a durable cursor across the cut; use global_action_seq.
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
async function getActionsV1(params, b) {
  const account = String(params.account_name || '').trim();
  if (!account) return { status: 400, body: { error: 'account_name is required' } };
  const pos = params.pos == null ? -1 : Number(params.pos);
  const offset = params.offset == null ? -20 : Number(params.offset);
  let approximate = false, skip, limit;
  // Probe the combined total (one row) to map absolute positions onto the descending timeline.
  const probe = await getActions({ account, limit: 1, skip: 0 }, b);
  if (probe.status !== 200) return probe;
  const total = (probe.body.total && probe.body.total.value) || 0;
  const totalExact = probe.body.total && probe.body.total.relation === 'eq';
  if (pos < 0) {                       // "latest N": positions total-N .. total-1
    limit = Math.min(Math.abs(offset) || 20, 1000); skip = 0;
  } else {                             // nodeos: [pos, pos+offset] (offset >= 0) or [pos+offset, pos] (offset < 0)
    const lo = offset >= 0 ? pos : Math.max(0, pos + offset), hi = offset >= 0 ? pos + offset : pos;
    const top = Math.min(hi, total - 1);
    if (top < lo) return { status: 200, body: { actions: [], last_irreversible_block: probe.body.lib, federation: { positional: 'approximate', account_action_seq: 'synthesized' } } };
    skip = total - 1 - top; limit = Math.min(top - lo + 1, 1000);
    approximate = true;
  }
  const page = await getActions({ account, limit, skip }, b);
  if (page.status !== 200) return page;
  const acts = (page.body.actions || []).map((a, i) => v1Action(a, total - 1 - skip - i)).reverse(); // nodeos: ascending
  return {
    status: 200,
    headers: page.headers,
    body: {
      actions: acts,
      last_irreversible_block: page.body.lib,
      federation: { positional: approximate || !totalExact ? 'approximate' : 'exact-order', account_action_seq: 'synthesized',
        ...(page.body.partial ? { partial: true } : {}), boundary: pubBoundary(B) },
    },
  };
}
// /v1/history/get_transaction: post-cut local (block_num above the cut) or pre-cut legacy (at or below it).
async function getTransactionV1(params, b) {
  const id = String(params.id || '');
  const local = await localPost('/v1/history/get_transaction', { id });
  if (local.ok && local.json) {
    const n = num(local.json.block_num);
    if (!(n <= b.cut_block)) return { status: 200, headers: statusHeaders('found'), body: local.json };
    return { status: 503, headers: statusHeaders('unavailable'), body: { error: `the local (post-cut) index returned ${id} in block ${n}, at or below the cut ${b.cut_block}: boundary inconsistency, refusing to answer` } };
  }
  const legacy = await legacyPost('/v1/history/get_transaction', { id });
  let aboveCut = false;
  if (legacy.ok && legacy.json) {
    const n = num(legacy.json.block_num);
    if (n <= b.cut_block) {
      if (n === b.cut_block && typeof legacy.json.block_id === 'string' && legacy.json.block_id.toLowerCase() !== b.cut_block_id) return legacyIdentityFail(b);
      return { status: 200, headers: statusHeaders('found'), body: legacy.json };
    }
    if (!Number.isFinite(n)) return { status: 503, headers: statusHeaders('unavailable'), body: { error: `the legacy answer for ${id} has no block_num: it cannot be placed relative to the cut, refusing to answer` } };
    aboveCut = true;
  }
  const down = { ...(unavailable(local) ? { local } : {}), ...(!aboveCut && unavailable(legacy) ? { legacy } : {}) };
  if (Object.keys(down).length) {
    return { status: 503, headers: statusHeaders('unavailable'), body: { ...errBody(`transaction ${id} not found in the sources that answered, and ${Object.keys(down).join(' + ')} history is unavailable: not a definitive answer`, ...Object.values(down)),
      federation: { status: 'unavailable' } } };
  }
  const st = await absenceStatus();
  return { status: 404, headers: statusHeaders(st), body: { ...((local.json && typeof local.json === 'object') ? local.json : (legacy.json && typeof legacy.json === 'object' && !aboveCut ? legacy.json : { error: `transaction ${id} not found` })),
    federation: { status: st, ...(aboveCut ? { legacy_above_cut_ignored: true } : {}) } } };
}

// Aggregate health: local is the post-cut source of truth; legacy is checked
// so the boundary's pre-cut half is monitored through the same URL. The
// boundary's own state is part of it: history is refused unless it is valid.
async function health() {
  const g = await gate();
  const b = B.b;
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
  const idleAtCut = !!b && nonIndexerOk && (lastIndexed || 0) === 0
    && typeof rpcHead === 'number' && rpcHead <= b.cut_block;
  const localOk = local.ok && (allOk || idleAtCut);
  const legacyOk = legacy.ok && Array.isArray(legacy.json && legacy.json.health);
  const boundaryOk = !!g.b;
  return {
    status: 200,
    body: {
      version: local.ok ? local.json.version : undefined,
      chain: local.ok ? local.json.chain : undefined,
      health: localServices,
      federation: {
        ok: localOk && boundaryOk,
        boundary: pubBoundary(B),
        local: { url: LOCAL, ok: localOk, last_indexed_block: lastIndexed, idle_at_cut: idleAtCut || undefined },
        legacy: { url: LEGACY, ok: legacyOk },
        chain: { url: CHAIN_URL },
      },
    },
  };
}

function pubBoundary(st) {
  const s = st || B;
  if (!s.b) return { staged: false, status: s.status, ...(s.error ? { error: s.error } : {}) };
  return { staged: true, status: s.status, valid: s.status === 'valid', cut_block: s.b.cut_block, cut_time: s.b.cut_time, cut_block_id: s.b.cut_block_id,
    chain_id: s.b.chain_id, ...(s.b.target_chain_id ? { target_chain_id: s.b.target_chain_id } : {}), ...(s.error ? { error: s.error } : {}),
    ...(s.identity ? { identity: s.identity } : {}) };
}
function errBody(msg, ...ups) {
  return { error: msg, upstreams: ups.filter(Boolean).map((u) => ({ status: u.status, error: u.error || u.text || (u.json && (u.json.message || u.json.error)) })) };
}
function sourceErrors(srcs) {
  const out = {};
  for (const [k, r] of Object.entries(srcs)) if (r && !r.ok) out[k] = { status: r.status, error: r.error || r.text || (r.json && (r.json.message || r.json.error)) };
  return out;
}
// Generic pass-through answers from the legacy archive: drop every listed row above the cut (and refuse a row
// at the cut with another block id). Returns {body, dropped} or {identity: true}.
function scrubLegacy(json, b) {
  if (!json || typeof json !== 'object') return { body: json, dropped: 0 };
  let dropped = 0;
  const out = Array.isArray(json) ? json.slice() : { ...json };
  const lists = Array.isArray(out) ? [[null, out]] : Object.entries(out).filter(([, v]) => Array.isArray(v));
  for (const [k, list] of lists) {
    if (legacyAtCutMismatch(list.filter((x) => x && typeof x === 'object'), b)) return { identity: true };
    const kept = list.filter((x) => !(x && typeof x === 'object' && num(x.block_num) > b.cut_block));
    dropped += list.length - kept.length;
    if (k != null) out[k] = kept; else return { body: kept, dropped };
  }
  return { body: out, dropped };
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
  if (p === '/' || p === '/v2') {
    await gate();
    return { status: 200, body: { service: 'hyperion-federator', local: LOCAL, legacy: LEGACY, chain: CHAIN_URL, boundary: pubBoundary(B) } };
  }
  if (!(p.startsWith('/v2/') || p.startsWith('/v1/history/'))) return { status: 404, body: { error: `not a history endpoint: ${p}` } };
  // Every history and state answer needs a VALID boundary (or, before any boundary file was ever seen and with
  // ALLOW_NO_BOUNDARY=1, the pre-ceremony legacy-only behaviour).
  const g = await gate();
  if (g.fail) return g.fail;
  if (g.legacyOnly) return p.startsWith('/v1/') ? legacyPass(p, params) : legacyPass(`${p}?${qs(params)}`);
  const b = g.b;
  if (p === '/v2/history/get_actions') return getActions(params, b);
  if (p === '/v2/history/get_transaction') return getTransaction(params, b);
  // State: chain truth, index discovery.
  if (p === '/v2/state/get_tokens') return getTokens(params);
  if (p === '/v2/state/get_account') return getAccount(params, b);
  if (p === '/v2/state/get_key_accounts' || p === '/v1/history/get_key_accounts') return getKeyAccounts(params);
  if (p === '/v1/history/get_controlled_accounts') return getControlledAccounts(params);
  if (p === '/v1/history/get_actions') return getActionsV1(params, b);
  if (p === '/v1/history/get_transaction') return getTransactionV1(params, b);
  // Everything else under /v2 (and /v1/history): local first, legacy fallback. Legacy rows above the cut are
  // dropped. A fallback because the local index was UNAVAILABLE (not because it had no answer) is missing the
  // post-cut half: tagged partial. Other /v2/state/* endpoints (get_links, get_proposals, get_voters, ...) are
  // answered from an index, not the chain: tagged so clients and operators can tell (docs/V1-COVERAGE.md).
  const v1 = p.startsWith('/v1/');
  const tags = p.startsWith('/v2/state/') ? { 'x-pulse-federation': 'index-only' } : {};
  const path = v1 ? p : `${p}?${qs(params)}`;
  const local = v1 ? await localPost(p, params) : await localGet(path);
  if (local.ok) return { status: 200, body: local.json, headers: tags };
  const legacy = v1 ? await legacyPost(p, params) : await legacyGet(path);
  if (legacy.ok) {
    const sc = scrubLegacy(legacy.json, b);
    if (sc.identity) return legacyIdentityFail(b);
    const localDown = unavailable(local);
    const headers = { ...tags, ...statusHeaders(localDown || sc.dropped ? 'partial' : 'ok') };
    if (localDown) headers['x-pulse-federation-partial'] = 'local (post-cut) index unavailable: legacy (pre-cut) answer only';
    if (sc.dropped) headers['x-pulse-federation-dropped'] = `${sc.dropped} legacy row(s) above the cut`;
    const body = v1 || Array.isArray(sc.body) ? sc.body : { ...sc.body, _premigration: true, ...(localDown ? { partial: true } : {}) };
    return { status: 200, body, headers };
  }
  if (!unavailable(local) && !unavailable(legacy)) return { status: local.status || 404, body: local.json || legacy.json || { error: `not found: ${p}` }, headers: tags };
  return { status: 503, headers: statusHeaders('unavailable'), body: errBody(`no source answered ${p}`, local, legacy) };
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
