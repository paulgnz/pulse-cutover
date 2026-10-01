// pulse-edge — the public /v1 surface of a node after an Antelope (Leap 5) -> PulseVM cutover.
//
// Every /v1 endpoint dapps call on a Leap 5 node keeps answering from the first post-cut block, with no
// seeding step. Rule: VALUES COME FROM THE NEW CHAIN; history indexes are used only for DISCOVERY (which
// accounts to look at), never as the answer to a state question.
//
//   /v1/chain/<14 native>   -> the PulseVM node's own nodeos-compatible API (NATIVE_BASE/v1/chain/<name>),
//                              Host: localhost (metalgo's host guard). Requests are normalized where the
//                              native parser is stricter than nodeos (numbers vs strings, index names,
//                              EOS… key spellings); responses pass through unchanged except get_info
//                              (idle-chain head time, see FRESH_HEAD_TIME) and get_block_info (timestamp repair).
//   /v1/chain/<polyfills>   -> translated from native calls / pulsevm.* JSON-RPC (RPC_URL).
//   static-at-cut           -> get_activated_protocol_features / get_consensus_parameters from files captured
//                              on the SOURCE chain at the cut (tools/capture-static.mjs); 501 if absent.
//   /v1/history/*           -> FEDERATOR_URL (pre-cut legacy + post-cut local, chain-verified state).
//   everything else         -> 501 (known Leap endpoint PulseVM cannot serve yet) or nodeos-style 404.
// Request bodies, paths and error bodies follow Leap 5.0.3 exactly (parse_params rules, exact paths, nodeos error
// texts), checked differentially by tools/conformance against a live nodeos.
// docs/V1-COVERAGE.md is the full table.
//
// Env: NATIVE_BASE (http://127.0.0.1:9650/ext/bc/<BID>), RPC_URL (NATIVE_BASE/rpc),
//      FEDERATOR_URL (http://127.0.0.1:7010), STATIC_DIR (/etc/pulse-cutover/static), PORT (8899),
//      HOST (127.0.0.1), UPSTREAM_TIMEOUT_MS (15000), FRESH_HEAD_TIME (1), MAX_BODY_BYTES (4 MiB).
// No dependencies (Node >= 14). PORT=0 picks a free port (printed on start).
'use strict';
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');

const NATIVE_BASE = (process.env.NATIVE_BASE || '').replace(/\/$/, '');
const RPC_URL = process.env.RPC_URL || (NATIVE_BASE ? NATIVE_BASE + '/rpc' : '');
const FEDERATOR_URL = (process.env.FEDERATOR_URL || 'http://127.0.0.1:7010').replace(/\/$/, '');
const STATIC_DIR = process.env.STATIC_DIR || '/etc/pulse-cutover/static';
const PORT = Number(process.env.PORT == null ? 8899 : process.env.PORT);
const HOST = process.env.HOST || '127.0.0.1';
const TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS || 15000);
const FRESH_HEAD_TIME = (process.env.FRESH_HEAD_TIME || '1') !== '0';
const MAX_BODY = Number(process.env.MAX_BODY_BYTES || 4 * 1024 * 1024);
const VERIFY_CONCURRENCY = 8;

process.on('unhandledRejection', (e) => console.error('unhandledRejection:', (e && e.stack) || e));

// ---- keys: EOS… / PUB_K1_… / PUB_R1_… / PUB_WA_… compared by decoded bytes -----------------------------
// (Chain get_account spells keys PUB_K1_…; clients and legacy indexes often send EOS…; the native
// get_required_keys parser rejects EOS….)
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
// RIPEMD-160: node's OpenSSL build may not expose it (OpenSSL 3 legacy provider), so fall back to JS.
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
// -> {type:'K1'|'R1'|'WA', data:Buffer} (checksum verified) or null
function keyInfo(k) {
  const s = String(k == null ? '' : k).trim();
  let type, body, legacy = false;
  const m = s.match(/^PUB_(K1|R1|WA)_(.+)$/);
  if (m) { type = m[1]; body = m[2]; } else if (s.startsWith('EOS')) { type = 'K1'; body = s.slice(3); legacy = true; } else return null;
  const raw = b58decode(body);
  if (!raw || raw.length < 5) return null;
  const data = raw.subarray(0, raw.length - 4), chk = raw.subarray(raw.length - 4);
  const want = ripemd160(legacy ? data : Buffer.concat([data, Buffer.from(type)])).subarray(0, 4);
  if (!want.equals(chk)) return null;
  if (type !== 'WA' && data.length !== 33) return null;
  return { type, data: Buffer.from(data) };
}
const keyCanon = (k) => { const i = keyInfo(k); return i ? `${i.type}:${i.data.toString('hex')}` : `raw:${String(k).trim()}`; };
const pubSpelling = (i) => `PUB_${i.type}_` + b58encode(Buffer.concat([i.data, ripemd160(Buffer.concat([i.data, Buffer.from(i.type)])).subarray(0, 4)]));
const legacySpelling = (i) => (i.type === 'K1' ? 'EOS' + b58encode(Buffer.concat([i.data, ripemd160(i.data).subarray(0, 4)])) : null);
const toPubKey = (k) => { const i = keyInfo(k); return i ? pubSpelling(i) : k; };

// ---- names + packed transactions (get_transaction_id) -----------------------------------------------------
function nameToU64(s) {
  s = String(s);
  if (s.length > 13 || !/^[.1-5a-z]*$/.test(s)) throw new Error(`invalid name "${s}"`);
  const cv = (c) => (c === '.' ? 0 : c <= '5' ? c.charCodeAt(0) - 48 : c.charCodeAt(0) - 91);
  let v = 0n;
  for (let i = 0; i < 13; i++) {
    const c = i < s.length ? cv(s[i]) : 0;
    if (i < 12) v |= BigInt(c & 0x1f) << BigInt(64 - 5 * (i + 1));
    else { if (c > 0x0f) throw new Error(`invalid name "${s}"`); v |= BigInt(c); }
  }
  return v;
}
function varuint(n) { const out = []; n = Number(n) >>> 0; do { let b = n & 0x7f; n >>>= 7; if (n) b |= 0x80; out.push(b); } while (n); return Buffer.from(out); }
const u8 = (n) => Buffer.from([Number(n) & 0xff]);
const u16 = (n) => { const b = Buffer.alloc(2); b.writeUInt16LE(Number(n) & 0xffff); return b; };
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(Number(n) >>> 0); return b; };
const u64 = (v) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt.asUintN(64, v)); return b; };
function hexBytes(h, what) {
  if (typeof h !== 'string' || !/^([0-9a-fA-F]{2})*$/.test(h)) throw new AbiNeeded(what);
  return Buffer.from(h, 'hex');
}
class AbiNeeded extends Error {}
function packAction(a) {
  const auth = a.authorization || [];
  const data = hexBytes(a.data == null ? '' : a.data, `${a.account}::${a.name}`);
  return Buffer.concat([u64(nameToU64(a.account)), u64(nameToU64(a.name)), varuint(auth.length),
    ...auth.map((p) => Buffer.concat([u64(nameToU64(p.actor)), u64(nameToU64(p.permission))])), varuint(data.length), data]);
}
function packTransaction(t) {
  const exp = String(t.expiration || '');
  const secs = Math.floor(Date.parse(/Z$/.test(exp) ? exp : exp + 'Z') / 1000);
  if (!Number.isFinite(secs)) throw new Error('invalid expiration');
  const cfa = t.context_free_actions || [], acts = t.actions || [], ext = t.transaction_extensions || [];
  return Buffer.concat([u32(secs), u16(t.ref_block_num || 0), u32(t.ref_block_prefix || 0), varuint(t.max_net_usage_words || 0),
    u8(t.max_cpu_usage_ms || 0), varuint(t.delay_sec || 0), varuint(cfa.length), ...cfa.map(packAction), varuint(acts.length), ...acts.map(packAction),
    varuint(ext.length), ...ext.map((e) => { const d = hexBytes(Array.isArray(e) ? e[1] : e.data, 'transaction_extensions'); return Buffer.concat([u16(Array.isArray(e) ? e[0] : e.type), varuint(d.length), d]); })]);
}
// nodeos accepts a transaction, {transaction:…}, or a packed transaction {packed_trx, compression}.
function transactionId(p) {
  let t = p;
  if (t && typeof t.packed_trx === 'string' && t.packed_trx) {
    let bytes = hexBytes(t.packed_trx, 'packed_trx');
    const c = t.compression;
    if (c === 1 || c === '1' || String(c).toLowerCase() === 'zlib') bytes = zlib.inflateSync(bytes);
    return crypto.createHash('sha256').update(bytes).digest('hex');
  }
  if (t && t.transaction && typeof t.transaction === 'object') t = t.transaction;
  return crypto.createHash('sha256').update(packTransaction(t)).digest('hex');
}

// ---- nodeos-shaped errors ----------------------------------------------------------------------------------
function nodeosError(status, name, what, message) {
  return {
    code: status,
    message: message || (status === 404 ? 'Not Found' : status === 501 ? what : status >= 500 ? 'Internal Service Error' : 'Bad Request'),
    error: { code: 0, name, what, details: [{ message: what, file: '', line_number: 0, method: '' }] },
  };
}
const reply = (status, body, headers) => ({ status, body, headers });
const err = (status, name, what, message, headers) => reply(status, nodeosError(status, name, what, message), headers);
// Exact Leap 5.0.3 error bodies (captured from nodeos): {code, message, error:{code, name, what, details:[{message,
// file, line_number, method}]}}. Clients match on the HTTP status, error.code/name/what and the detail messages.
const D = (message, file, line_number, method) => ({ message, file, line_number, method });
const leapErr = (status, code, name, what, message, details, headers) => reply(status, { code: status, message, error: { code, name, what, details } }, headers);
const notFound = () => leapErr(404, 0, 'exception', 'unspecified', 'Not Found', [D('Unknown Endpoint', 'beast_http_session.hpp', 186, 'handle_request')]);
const invalidRequest = (details) => leapErr(400, 3200006, 'invalid_http_request', 'invalid http request', 'Invalid Request', details);
const bodyRequired = (file = 'http_plugin.hpp', line = 249) => invalidRequest([D('A Request body is required', file, line, 'parse_params')]);
const unparsable = (...more) => invalidRequest([D('Unable to parse valid input from POST body', 'http_plugin.hpp', 270, 'parse_params'), ...more]);
const unknownBlock = (msg, line, method) => leapErr(400, 3100002, 'unknown_block_exception', 'Unknown block', 'Unknown Block', [D(msg, 'chain_plugin.cpp', line, method)]);

// ---- request parsing exactly like Leap 5's http_plugin parse_params ---------------------------------------------------
// no_params endpoints accept only an empty body or {}; params_optional accept empty; params_required answer 400
// "A Request body is required" for an empty body or {} (whitespace allowed). Only the first JSON value is read
// (nodeos ignores trailing bytes). Malformed JSON and wrong top-level types get fc's messages.
const NO_PARAMS = new Set(['get_info', 'get_producer_schedule', 'get_consensus_parameters']);
const OPTIONAL_PARAMS = new Set(['get_activated_protocol_features']);
const fcType = (v) => (v === null ? 'null_type' : Array.isArray(v) ? 'array_type' : typeof v === 'boolean' ? 'bool_type' : typeof v === 'string' ? 'string_type'
  : typeof v === 'number' ? (Number.isInteger(v) ? (v < 0 ? 'int64_type' : 'uint64_type') : 'double_type') : 'object_type');
const castToObject = (v) => D(`Invalid cast from type '${fcType(v)}' to Object`, 'variant.cpp', 630, 'get_object');
const JSON_TAIL = D('', 'json.cpp', 461, 'from_string');
// The first complete JSON value of `text` (nodeos stops there), or {error: fc-style details}.
function firstJson(text) {
  try { return { value: JSON.parse(text) }; } catch { /* maybe trailing bytes, maybe malformed */ }
  let depth = 0, inStr = false, esc = false, end = -1;
  const c0 = text[0];
  if (c0 === '{' || c0 === '[') {
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (inStr) { if (esc) esc = false; else if (c === '\\') esc = true; else if (c === '"') inStr = false; continue; }
      if (c === '"') inStr = true; else if (c === '{' || c === '[') depth++; else if (c === '}' || c === ']') { depth--; if (depth === 0) { end = i; break; } }
    }
    if (end > 0) { try { return { value: JSON.parse(text.slice(0, end + 1)) }; } catch { /* malformed inside */ } }
    if (end < 0) return { error: [D('Unexpected EOF: 11 eof_exception: End Of File\nunexpected end of file\n    {}\n    nodeos  json.cpp:435 variant_from_stream\n', 'json.cpp', 213, 'objectFromStream'), JSON_TAIL] };
    const m = text.match(/^\{\s*([^"\s}])/);
    if (m) return { error: [D(`Expected '"' but read '${m[1]}'`, 'json.cpp', 100, 'stringFromStream'), D("while parsing token ''", 'json.cpp', 123, 'stringFromStream'), D('Error parsing object', 'json.cpp', 218, 'objectFromStream'), JSON_TAIL] };
    return { error: [D('Error parsing object', 'json.cpp', 218, 'objectFromStream'), JSON_TAIL] };
  }
  const first = text.match(/^[^\s,:\]}]+/);
  try { if (first) return { value: JSON.parse(first[0]) }; } catch { /* not a scalar */ }
  return { error: [D(`Unexpected char '${text.charCodeAt(0)}' in "${text.slice(0, 32)}"`, 'json.cpp', 438, 'variant_from_stream'), JSON_TAIL] };
}
// -> {params} or {fail: reply}
function leapParse(name, raw) {
  const text = String(raw || '').trim();
  const parsed = text ? firstJson(text) : { value: undefined };
  const v = parsed.value;
  const emptyObj = v && typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0;
  if (name === 'get_transaction_id') { // chain_api_plugin's own parser
    const inv = D('Invalid transaction', 'chain_api_plugin.cpp', 91, 'parse_params');
    if (!text) return { fail: bodyRequired('chain_api_plugin.cpp', 50) };
    if (parsed.error) return { fail: invalidRequest([inv, ...parsed.error]) };
    if (!v || typeof v !== 'object' || Array.isArray(v)) return { fail: invalidRequest([D('Transaction object is missing or invalid', 'chain_api_plugin.cpp', 84, 'parse_params'), inv]) };
    if (!Array.isArray(v.actions)) return { fail: invalidRequest([D('Transaction actions are missing or invalid', 'chain_api_plugin.cpp', 80, 'parse_params'), inv]) };
    return { params: v };
  }
  if (NO_PARAMS.has(name)) {
    if (!text || emptyObj) return { params: {} };
    return { fail: unparsable(D('no parameter should be given', 'http_plugin.hpp', 263, 'parse_params')) };
  }
  if (parsed.error) return { fail: unparsable(...parsed.error) };
  if (!text || emptyObj) return OPTIONAL_PARAMS.has(name) ? { params: {} } : { fail: bodyRequired() };
  if (name === 'push_transactions') {
    if (!Array.isArray(v)) return { fail: unparsable(D(`Invalid cast from ${fcType(v)} to Array`, 'variant.cpp', 561, 'get_array')) };
    const bad = v.find((t) => !t || typeof t !== 'object' || Array.isArray(t));
    if (bad !== undefined) return { fail: unparsable(castToObject(bad)) };
    return { params: v };
  }
  if (!v || typeof v !== 'object' || Array.isArray(v)) return { fail: unparsable(castToObject(v)) };
  return { params: v };
}

// Leap's name parser (fc name.cpp): over-long names and names that do not round-trip are refused with these texts.
function leapNameError(s) {
  s = String(s);
  if (s.length > 13) return D(`Name is longer than 13 characters (${s}) `, 'name.cpp', 11, 'set');
  const sym = (c) => (c >= 'a' && c <= 'z' ? c.charCodeAt(0) - 91 : c >= '1' && c <= '5' ? c.charCodeAt(0) - 48 : 0);
  const chars = '.12345abcdefghijklmnopqrstuvwxyz';
  let norm = '';
  for (let i = 0; i < s.length; i++) norm += chars[i < 12 ? sym(s[i]) & 0x1f : sym(s[i]) & 0x0f];
  norm = norm.replace(/\.+$/, '');
  return norm === s ? null : D(`Name not properly normalized (name: ${s}, normalized: ${norm}) `, 'name.cpp', 15, 'set');
}
// Leap's public key parser (fc public_key.cpp): the texts for the malformed keys clients actually send.
function leapKeyError(k) {
  const s = String(k);
  if (keyInfo(s)) return null;
  if (s.startsWith('EOS')) {
    const raw = b58decode(s.slice(3));
    if (!raw || raw.length !== 37) return D('bin.size() == sizeof(data_type) + sizeof(uint32_t): ', 'public_key.cpp', 40, 'parse_base58');
    return D('wrapper::calculate_checksum(wrapped.data) == wrapped.check: ', 'public_key.cpp', 42, 'parse_base58');
  }
  const m = s.match(/^PUB_([A-Z0-9]+)_(.*)$/);
  if (!m) return D(`pivot != std::string::npos: No delimiter in string, cannot determine data type: ${s}`, 'public_key.cpp', 48, 'parse_base58');
  const raw = b58decode(m[2]);
  if (raw && raw.length >= 37) return D('checksum == wrapped.check: ', 'common.hpp', 50, 'apply');
  const n = raw ? raw.length : 0;
  return [D(`read datastream of length ${n} over by ${n - 5}`, 'datastream.cpp', 6, 'throw_datastream_range_error'), D('fc::array<char,33>', 'raw.hpp', 173, 'unpack'),
    D('Error unpacking field data', 'raw.hpp', 363, 'operator()'), D('error unpacking fc::crypto::checksummed_data<T>', 'raw.hpp', 668, 'unpack')];
}
// Leap prints K1 keys in the legacy EOS… form (get_required_keys, get_accounts_by_authorizers, get_account).
const leapKey = (k) => { const i = keyInfo(k); return (i && legacySpelling(i)) || k; };

// Leap's block_num_or_id parser (get_block_header, get_block_header_state, get_raw_block) -> {num} | {id} | {fail}
function leapBlockRef(v, method, line) {
  const s = v == null ? '' : String(v);
  const invalid = (...d) => ({ fail: leapErr(500, 3010008, 'block_id_type_exception', 'Invalid block ID', 'Internal Service Error', d) });
  if (!s || s.length > 64) return invalid(D('Invalid Block number or ID, must be greater than 0 and less than 65 characters', 'chain_plugin.cpp', line - 14, method));
  if (/^\d+$/.test(s)) return { num: Number(s), text: s };
  if (s.length % 2) return invalid(D(`Invalid block ID: ${s}`, 'chain_plugin.cpp', line - 2, method), D('str.size() % 2 == 0: the length of hex string should be even number', 'variant.cpp', 752, 'from_variant'));
  const bad = s.match(/[^0-9a-fA-F]/);
  if (bad) return invalid(D(`Invalid block ID: ${s}`, 'chain_plugin.cpp', line - 2, method), D(`Invalid hex character '${bad[0]}'`, 'hex.cpp', 13, 'from_hex'));
  return { id: s.toLowerCase(), text: s };
}
const unavailable = (endpoint, why) => err(501, 'unsupported_feature', `${endpoint} is not available on PulseVM yet (upstream)${why ? ': ' + why : ''}`,
  `${endpoint} not available on PulseVM yet (upstream)`, { 'x-pulse-edge': 'unavailable' });

// ---- upstream plumbing (http.request: Node 14 has no fetch; lets us set Host) -------------------------------
function request(url, { method = 'POST', body, headers = {} } = {}) {
  return new Promise((resolve) => {
    let u; try { u = new URL(url); } catch (e) { return resolve({ status: 0, error: `bad upstream url ${url}` }); }
    const mod = u.protocol === 'https:' ? https : http;
    const loopback = /^(127\.|localhost$|\[::1\]$)/.test(u.hostname);
    const h = { accept: 'application/json', ...headers };
    if (body != null) { h['content-type'] = h['content-type'] || 'application/json'; h['content-length'] = Buffer.byteLength(body); }
    if (loopback) h.host = 'localhost'; // metalgo rejects other Host values
    const req = mod.request(u, { method, headers: h, timeout: TIMEOUT_MS }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let json; try { json = JSON.parse(text); } catch { json = undefined; }
        resolve({ status: res.statusCode, headers: res.headers, text, json });
      });
      res.on('error', (e) => resolve({ status: 0, error: String(e.message || e) }));
    });
    req.on('timeout', () => req.destroy(new Error(`upstream timeout after ${TIMEOUT_MS} ms`)));
    req.on('error', (e) => resolve({ status: 0, error: String(e.message || e) }));
    if (body != null) req.write(body);
    req.end();
  });
}
// metalgo 1.14 matches VM routes exactly, so /ext/bc/<BID>/v1/chain/<name> is a metalgo 404. The node picks
// the method out of the full URL (split on "/v1/chain/"), query string included, so the method rides in the
// query; this form also works on metalgo 1.13's prefix routes.
const nativeUrl = (name) => `${NATIVE_BASE}/v1/chain?route=/v1/chain/${encodeURIComponent(name)}`;
const native = (name, params) => request(nativeUrl(name), { body: typeof params === 'string' ? params : JSON.stringify(params || {}) });
let _rpcId = 0;
async function rpc(method, params) {
  const r = await request(RPC_URL, { body: JSON.stringify({ jsonrpc: '2.0', id: ++_rpcId, method, params: params || {} }) });
  if (r.json && r.json.error) return { error: r.json.error };
  if (r.json && 'result' in r.json) return { result: r.json.result };
  return { error: { code: -32099, message: r.error || (r.text || '').slice(0, 300) || `upstream HTTP ${r.status}` } };
}
// Upstream failure -> nodeos-shaped reply (keeps a native nodeos-shaped error body as is).
function upstreamFailure(r, what) {
  if (r.status && r.json && r.json.error) return reply(r.status, r.json);
  return err(502, 'upstream_unavailable', `${what}: ${r.error || `HTTP ${r.status}`}`);
}
function rpcFailure(e, what) {
  const msg = `${what}: ${(e && (e.data || e.message)) || 'error'}`;
  if (e && e.code === 404) return err(400, 'unknown_block_exception', msg, 'Bad Request');
  if (e && e.code === 400) return err(400, 'invalid_params', msg);
  return err(500, (e && e.message) || 'internal_error', msg);
}
async function pool(items, n, fn) {
  const out = new Array(items.length); let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const k = i++; out[k] = await fn(items[k], k); } }));
  return out;
}

// ---- request normalization for the native pass-through --------------------------------------------------------
// The native /v1 parser is typed strictly (u32 vs string); nodeos, eosjs and wharfkit are loose. Normalize only
// what nodeos itself accepts, so a request that works on Leap works here.
const INDEX_NAMES = { primary: 1, secondary: 2, tertiary: 3, fourth: 4, fifth: 5, sixth: 6, seventh: 7, eighth: 8, ninth: 9, tenth: 10 };
const toUint = (v) => (typeof v === 'string' && /^\d+$/.test(v.trim()) ? Number(v.trim()) : v);
const toStr = (v) => (typeof v === 'number' || typeof v === 'bigint' ? String(v) : v);
const NORMALIZE = {
  get_block: (p) => ({ ...p, block_num_or_id: toStr(p.block_num_or_id) }),
  get_block_info: (p) => ({ ...p, block_num: toUint(p.block_num) }),
  get_table_rows(p) {
    const q = { ...p, limit: toUint(p.limit), lower_bound: toStr(p.lower_bound), upper_bound: toStr(p.upper_bound), scope: toStr(p.scope) };
    if (typeof q.index_position === 'string') {
      const s = q.index_position.trim().toLowerCase();
      q.index_position = INDEX_NAMES[s] || (/^\d+$/.test(s) ? Number(s) : q.index_position);
    }
    for (const k of ['json', 'reverse', 'show_payer']) if (q[k] === 'true' || q[k] === 'false') q[k] = q[k] === 'true';
    for (const k of Object.keys(q)) if (q[k] === undefined) delete q[k];
    return q;
  },
  get_table_by_scope(p) {
    const q = { ...p, limit: toUint(p.limit), lower_bound: toStr(p.lower_bound), upper_bound: toStr(p.upper_bound) };
    if (q.reverse === 'true' || q.reverse === 'false') q.reverse = q.reverse === 'true';
    for (const k of Object.keys(q)) if (q[k] === undefined) delete q[k];
    return q;
  },
  get_required_keys: (p) => ({ ...p, available_keys: Array.isArray(p.available_keys) ? p.available_keys.map(toPubKey) : p.available_keys }),
  push_transaction: (p) => ({ ...p, packed_context_free_data: p.packed_context_free_data == null ? '' : p.packed_context_free_data }),
  send_transaction: (p) => ({ ...p, packed_context_free_data: p.packed_context_free_data == null ? '' : p.packed_context_free_data }),
};
const NATIVE = new Set(['get_info', 'get_account', 'get_block', 'get_block_info', 'get_abi', 'get_raw_abi', 'get_table_rows', 'get_table_by_scope',
  'get_currency_balance', 'get_currency_stats', 'get_code_hash', 'get_required_keys', 'push_transaction', 'send_transaction']);

const isoMs = (ms) => new Date(ms).toISOString().replace('Z', '').slice(0, 23);
// Native get_block_info prints the block time with Rust's Debug format ("TimePoint { elapsed: Microseconds
// { count: N } }"), which eosjs cannot parse (its TAPOS path reads .timestamp). Repair to nodeos' spelling.
function repairTimestamp(ts) {
  if (typeof ts !== 'string' || /^\d{4}-\d\d-\d\dT/.test(ts)) return ts;
  const m = ts.match(/count:\s*(-?\d+)/);
  return m ? isoMs(Number(BigInt(m[1]) / 1000n)) : ts;
}

// raw: the request body; params: leapParse()'s result (already validated like nodeos would).
async function passNative(name, raw, params) {
  let body = raw && raw.trim() ? raw : '{}';
  const parsed = params;
  try { JSON.parse(body); } catch { body = JSON.stringify(parsed); } // trailing bytes after the first value: nodeos ignores them
  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed) && NORMALIZE[name]) {
    const norm = NORMALIZE[name](parsed);
    const s = JSON.stringify(norm);
    if (s !== JSON.stringify(parsed)) body = s;
  }
  const r = await native(name, body);
  if (!r.status) return err(502, 'upstream_unavailable', `PulseVM node unreachable: ${r.error}`);
  if (r.status === 200 && r.json) {
    if (name === 'get_info' && FRESH_HEAD_TIME && r.json.head_block_time) {
      // PulseVM builds blocks on demand: on an idle chain head_block_time is old, and clients that set
      // expiration = head_block_time + N would send already-expired transactions. Report a fresh head time
      // when the real one is stale; the true value stays in pulsevm_head_block_time.
      const real = Date.parse(r.json.head_block_time + 'Z'), now = Date.now();
      if (now - real > 3000) {
        return reply(200, { ...r.json, head_block_time: isoMs(Math.floor(now / 500) * 500), pulsevm_head_block_time: r.json.head_block_time },
          { 'x-pulse-edge': 'fresh-head-time' });
      }
    }
    if (name === 'get_block_info' && r.json.timestamp && repairTimestamp(r.json.timestamp) !== r.json.timestamp) {
      return reply(200, { ...r.json, timestamp: repairTimestamp(r.json.timestamp) }, { 'x-pulse-edge': 'timestamp-repaired' });
    }
    if (name === 'get_required_keys' && Array.isArray(r.json.required_keys)) {
      // Leap 5 answers K1 keys in the legacy EOS… form whatever spelling the client sent.
      const keys = r.json.required_keys.map(leapKey);
      if (JSON.stringify(keys) !== JSON.stringify(r.json.required_keys)) return reply(200, { ...r.json, required_keys: keys });
    }
  }
  return { status: r.status, raw: r.text, contentType: (r.headers && r.headers['content-type']) || 'application/json' };
}

// ---- polyfills ---------------------------------------------------------------------------------------------------
async function blockAndHeader(id) {
  const b = await native('get_block', { block_num_or_id: toStr(id) });
  if (b.status !== 200 || !b.json) return { fail: upstreamFailure(b, 'get_block'), notFound: b.status >= 400 && b.status < 600 };
  const blk = b.json;
  // Fields get_block omits come from the node's own get_block_info for the same height (never invented here).
  const bi = await native('get_block_info', { block_num: blk.block_num });
  const info = bi.status === 200 && bi.json ? bi.json : {};
  const header = {
    timestamp: blk.timestamp, producer: blk.producer, confirmed: blk.confirmed != null ? blk.confirmed : info.confirmed,
    previous: blk.previous, transaction_mroot: blk.transaction_mroot, action_mroot: blk.action_mroot,
    schedule_version: info.schedule_version, header_extensions: info.header_extensions || [],
  };
  if (info.new_producers != null) header.new_producers = info.new_producers; // nodeos omits it when absent
  return { blk, header, signature: info.producer_signature };
}
// Errors nodeos raises before a transaction executes: never turned into a failure trace by send_transaction2.
// expired_tx_exception, tx_duplicate, packed_transaction_type_exception, invalid_http_request.
const PRE_EXECUTION = new Set([3040005, 3040008, 3010010, 3200006]);
// fc's to_detail_string, as push_transactions reports a failed transaction: "<code> <name>: <what>\n" then per stack
// entry "<message>\n    nodeos  <file>:<line> <method>\n". The node's error JSON does not carry each entry's format
// arguments, so the "    {…}" argument line nodeos prints between the two is left out.
function detailString(body, status) {
  const e = body && body.error;
  if (!e) return (body && body.message) || `HTTP ${status}`;
  return `${e.code} ${e.name}: ${e.what}\n` + (e.details || []).map((d) => `${d.message}\n    nodeos  ${d.file}:${d.line_number} ${d.method}\n`).join('');
}
const refBlockPrefix = (id) => Buffer.from(String(id).slice(16, 24), 'hex').readUInt32LE(0);

const POLY = {
  async send_transaction2(p) {
    // Leap 5: {return_failure_trace = true, retry_trx, retry_trx_num_blocks, transaction:{signatures, compression, …}}.
    const t = p.transaction;
    if (!t || typeof t !== 'object' || Array.isArray(t)) {
      return leapErr(500, 3010010, 'packed_transaction_type_exception', 'Invalid packed transaction', 'Internal Service Error', [
        D('Invalid packed transaction', 'chain_plugin.cpp', 2201, 'send_transaction_gen'), castToObject(t === undefined ? null : t), D('Failed to deserialize variant', 'abi_serializer.hpp', 1003, 'from_variant')]);
    }
    const r = await passNative('send_transaction', JSON.stringify(t), t);
    if (p.return_failure_trace === false || p.return_failure_trace === 'false') return r;
    const body = r.body || (() => { try { return JSON.parse(r.raw); } catch { return null; } })();
    const e = body && body.error;
    // A transaction that fails while executing comes back as 202 + a failure trace (nodeos' default); errors raised
    // before execution (parsing, expiry, duplicates) stay a 500 like on Leap.
    if (r.status < 400 || !e || PRE_EXECUTION.has(Number(e.code)) || Number(e.code) < 3000000) return r;
    let id = '0'.repeat(64);
    try { id = transactionId({ packed_trx: t.packed_trx, compression: t.compression }); } catch { /* unparsable packed_trx never reaches here */ }
    const info = await native('get_info', {});
    const head = (info.json && info.json.head_block_num) || 0, now = Date.now();
    const blockTime = isoMs(Math.max(now, Date.parse(((info.json && info.json.head_block_time) || '1970-01-01T00:00:00') + 'Z') + 500));
    return reply(202, { transaction_id: id, processed: {
      id, block_num: head + 1, block_time: blockTime, producer_block_id: null, receipt: null, elapsed: 0, net_usage: 0, scheduled: false,
      action_traces: [], account_ram_delta: null,
      except: { code: e.code, name: e.name, message: e.what, stack: (e.details || []).map((d) => ({
        context: { level: 'error', file: d.file || '', line: d.line_number || 0, method: d.method || '', hostname: '', thread_name: 'nodeos', timestamp: isoMs(now) },
        format: d.message, data: {} })) },
      error_code: '10000000000000000000',
    } }, { 'x-pulse-edge': 'failure-trace from the node error (elapsed/net_usage and stack data not available)' });
  },
  async push_transactions(p) {
    if (p.length > 1000) return leapErr(500, 3100001, 'too_many_tx_at_once', 'Pushing too many transactions at once', 'Internal Service Error', [D('Attempt to push more than 1000 transactions at once', 'chain_plugin.cpp', 2185, 'push_transactions')]);
    if (!p.length) return leapErr(500, 13, 'St12out_of_range', 'vector', 'Internal Service Error', [D('rethrow vector: ', 'chain_plugin.cpp', 2190, 'push_transactions')]);
    const out = [];
    for (const t of p) { // sequential, in order, one result per transaction (like nodeos)
      const r = await passNative('push_transaction', JSON.stringify(t), t);
      const body = r.body || (() => { try { return JSON.parse(r.raw); } catch { return null; } })();
      if (r.status < 400 && body) out.push(body);
      else out.push({ transaction_id: '0'.repeat(64), processed: { error: detailString(body, r.status) } });
    }
    return reply(202, out);
  },
  async get_raw_block(p) {
    const ref = leapBlockRef(p.block_num_or_id, 'get_raw_block', 1934);
    if (ref.fail) return ref.fail;
    const r = await rpc('pulsevm.getRawBlock', { block_num_or_id: toStr(p.block_num_or_id) });
    if (r.error && r.error.code === 404) return unknownBlock(`Could not find block: ${ref.text}`, 1935, 'get_raw_block');
    return r.error ? rpcFailure(r.error, 'get_raw_block') : reply(200, r.result, { 'x-pulse-edge': 'polyfill' });
  },
  async get_block_header(p) {
    const ref = leapBlockRef(p.block_num_or_id, 'get_block_header', 1973);
    if (ref.fail) return ref.fail;
    const h = await blockAndHeader(p.block_num_or_id);
    if (h.fail) return h.notFound ? unknownBlock(`Could not find block header: ${ref.text}`, 1975, 'get_block_header') : h.fail;
    const out = { id: h.blk.id, signed_block_header: { ...h.header, producer_signature: h.signature } };
    if (p.include_extensions === true || p.include_extensions === 'true') out.block_extensions = h.blk.block_extensions || [];
    return reply(200, out, { 'x-pulse-edge': 'polyfill' });
  },
  async get_block_header_state(p) {
    // Only the fields TAPOS clients read are meaningful here (eosjs: header.timestamp, id, block_num).
    // PulseVM finalizes every accepted block, so irreversible == head: eosjs never actually asks for this
    // (it uses get_block_info at or below LIB), but it must not fail if it does.
    // Leap serves header state only for REVERSIBLE blocks (in the fork database); anything at or below LIB is
    // "Could not find reversible block". On PulseVM LIB == head, so this is always that error, as on a Leap node
    // with no reversible blocks. eosjs/@proton/js only ask for blocks above LIB and fall back to get_block_info.
    const ref = leapBlockRef(p.block_num_or_id, 'get_block_header_state', 2052);
    if (ref.fail) return ref.fail;
    const gone = () => unknownBlock(`Could not find reversible block: ${ref.text}`, 2054, 'get_block_header_state');
    const info = await native('get_info', {});
    const lib = info.json && info.json.last_irreversible_block_num;
    if (ref.num != null && lib != null && ref.num <= lib) return gone();
    const h = await blockAndHeader(p.block_num_or_id);
    if (h.fail) return h.notFound ? gone() : h.fail;
    if (lib != null && h.blk.block_num <= lib) return gone();
    return reply(200, {
      block_num: h.blk.block_num, id: h.blk.id,
      dpos_proposed_irreversible_blocknum: lib, dpos_irreversible_blocknum: lib,
      header: { ...h.header, producer_signature: h.signature },
      ref_block_prefix: refBlockPrefix(h.blk.id),
    }, { 'x-pulse-edge': 'polyfill; partial header state' });
  },
  async get_producers(p) {
    // Leap reads eosio.producers through its by-votes index; PulseVM's native table reader has no float64
    // secondary index, so read the table in primary order and sort by the same key.
    const wantJson = p.json === true || p.json === 'true';
    const limit = Math.max(1, Math.min(Number(p.limit) || 50, 1000));
    const lower = String(p.lower_bound || '');
    const rows = [], hex = [];
    for (let lb = '', guard = 0; guard < 1000; guard++) {
      const q = { code: 'eosio', scope: 'eosio', table: 'producers', json: true, limit: 500 };
      if (lb) q.lower_bound = lb;
      const r = await native('get_table_rows', q);
      if (r.status !== 200 || !r.json) return upstreamFailure(r, 'get_producers (eosio producers table)');
      rows.push(...(r.json.rows || []));
      if (!wantJson) {
        const rh = await native('get_table_rows', { ...q, json: false });
        if (rh.status !== 200 || !rh.json) return upstreamFailure(rh, 'get_producers (eosio producers table)');
        hex.push(...(rh.json.rows || []));
      }
      if (!r.json.more || !r.json.next_key) break;
      lb = String(r.json.next_key);
    }
    // eosio.system's by_votes key: is_active ? -total_votes : total_votes (active by votes desc, then
    // inactive by votes asc), ties in primary-key (owner) order.
    const idx = rows.map((row, i) => { const v = parseFloat(row.total_votes) || 0; return { row, i, key: Number(row.is_active) ? -v : v }; });
    idx.sort((a, b) => a.key - b.key || (a.row.owner < b.row.owner ? -1 : a.row.owner > b.row.owner ? 1 : 0));
    let start = 0;
    if (lower) { start = idx.findIndex((x) => x.row.owner === lower); if (start < 0) start = idx.length; }
    const page = idx.slice(start, start + limit);
    const g = await native('get_table_rows', { code: 'eosio', scope: 'eosio', table: 'global', json: true, limit: 1 });
    const gr = g.status === 200 && g.json && g.json.rows && g.json.rows[0];
    return reply(200, {
      rows: page.map((x) => (wantJson ? x.row : hex[x.i])),
      total_producer_vote_weight: gr ? String(gr.total_producer_vote_weight) : '0',
      more: start + limit < idx.length ? idx[start + limit].row.owner : '',
    }, { 'x-pulse-edge': 'polyfill' });
  },
  async get_producer_schedule() {
    // Only the ACTIVE schedule's version + names are exposed by the node; signing keys and the
    // pending/proposed schedules are not (upstream ask). Never invent them.
    const r = await rpc('pulsevm.getProducers', {});
    if (r.error) return rpcFailure(r.error, 'get_producer_schedule');
    return reply(200, { active: { version: r.result.schedule_version, producers: (r.result.active_producers || []).map((n) => ({ producer_name: n })) } },
      { 'x-pulse-edge': 'partial: active names only' });
  },
  async get_raw_code_and_abi(p) {
    const ne = leapNameError(p.account_name == null ? '' : p.account_name);
    if (ne) return unparsable(ne);
    const r = await native('get_raw_abi', { account_name: p.account_name });
    if (r.status >= 400 && r.status < 600) return leapErr(400, 3060002, 'account_query_exception', 'Account Query Exception', 'Account lookup',
      [D(`unable to retrieve account code/abi (unknown key (eosio::chain::name): ${p.account_name})`, 'chain_plugin.cpp', 2357, 'get_raw_code_and_abi')]);
    if (r.status !== 200 || !r.json) return upstreamFailure(r, 'get_raw_code_and_abi');
    return reply(200, { account_name: r.json.account_name || p.account_name, wasm: '', abi: r.json.abi || '' }, { 'x-pulse-edge': 'wasm-unavailable' });
  },
  async get_activated_protocol_features(p) {
    const s = loadStatic('activated_protocol_features.json');
    if (!s) return staticMissing('get_activated_protocol_features', 'activated_protocol_features.json');
    let list = (s.activated_protocol_features || []).slice();
    const key = p.search_by_block_num === true || p.search_by_block_num === 'true' ? 'activation_block_num' : 'activation_ordinal';
    list.sort((a, b) => a.activation_ordinal - b.activation_ordinal);
    if (p.lower_bound != null && p.lower_bound !== '') list = list.filter((f) => f[key] >= Number(p.lower_bound));
    if (p.upper_bound != null && p.upper_bound !== '') list = list.filter((f) => f[key] <= Number(p.upper_bound));
    if (p.reverse === true || p.reverse === 'true') list.reverse();
    // Leap 5.0 ignores `limit` here: every matching feature, never a `more` (checked against nodeos 5.0.0 and 5.0.3).
    return reply(200, { activated_protocol_features: list }, { 'x-pulse-edge': 'static-at-cut' });
  },
  async get_consensus_parameters() {
    const s = loadStatic('consensus_parameters.json');
    return s ? reply(200, s, { 'x-pulse-edge': 'static-at-cut' }) : staticMissing('get_consensus_parameters', 'consensus_parameters.json');
  },
  async get_scheduled_transactions() {
    // PulseVM v1.0.0 DOES keep deferred (generated) transactions — migrated from the snapshot and
    // executed/retired per block — but exposes no way to list them. An empty list is only a sound answer
    // once DISABLE_DEFERRED_TRXS_STAGE_1 is active: from then on send_deferred is a no-op and every
    // pending row is retired at the next block. Features never deactivate, so the at-cut list is enough.
    const s = loadStatic('activated_protocol_features.json');
    const stage1 = s && (s.activated_protocol_features || []).some((f) => f.feature_digest === DISABLE_DEFERRED_STAGE_1);
    if (stage1) return reply(200, { transactions: [], more: '' }, { 'x-pulse-edge': 'deferred-disabled' });
    return unavailable('get_scheduled_transactions', 'the chain keeps deferred transactions but PulseVM has no endpoint that lists them');
  },
  async get_accounts_by_authorizers(p) {
    // Validate like nodeos' parser (names and keys), before any discovery.
    const rawAccts = Array.isArray(p.accounts) ? p.accounts : [];
    const keys = Array.isArray(p.keys) ? p.keys : [];
    for (const a of rawAccts) {
      for (const n of typeof a === 'string' ? [a] : [a && a.actor, ...(a && a.permission ? [a.permission] : [])]) { const e = leapNameError(n == null ? '' : n); if (e) return unparsable(e); }
    }
    for (const k of keys) { const e = leapKeyError(k); if (e) return unparsable(...[].concat(e)); }
    const accts = rawAccts.map((a) => (typeof a === 'string' ? { actor: a, permission: '' } : { actor: a.actor, permission: a.permission || '' }));
    if (!keys.length && !accts.length) return reply(200, { accounts: [] });
    // Discovery: the federator's verified key -> accounts and controlling -> controlled lookups (legacy
    // pre-cut index + local post-cut index). Truth: this node's get_account for every candidate.
    const candidates = new Set();
    for (const k of keys) {
      const r = await request(`${FEDERATOR_URL}/v1/history/get_key_accounts`, { body: JSON.stringify({ public_key: String(k) }) });
      if (r.status !== 200 || !r.json || !Array.isArray(r.json.account_names)) return discoveryDown(r);
      r.json.account_names.forEach((n) => candidates.add(n));
    }
    for (const a of accts) {
      const r = await request(`${FEDERATOR_URL}/v1/history/get_controlled_accounts`, { body: JSON.stringify({ controlling_account: String(a.actor) }) });
      if (r.status !== 200 || !r.json || !Array.isArray(r.json.controlled_accounts)) return discoveryDown(r);
      r.json.controlled_accounts.forEach((n) => candidates.add(n));
    }
    const keyCanons = [...new Set(keys.map(keyCanon))];
    const acctRows = [], keyRows = [];
    const results = await pool([...candidates], VERIFY_CONCURRENCY, (name) => native('get_account', { account_name: name }));
    for (const r of results) {
      if (r.status !== 200 || !r.json) continue; // no such account on the chain now: nothing to report
      const perms = r.json.permissions || [];
      for (const perm of perms) {
        const ra = perm.required_auth || {};
        // Leap walks its permission index in permission-creation order. Approximated from what get_account exposes:
        // account creation time, then name (same-block ties), then owner, active, the rest in get_account order.
        const created = String(r.json.created || '');
        const rank = perm.perm_name === 'owner' ? 0 : perm.perm_name === 'active' ? 1 : 2 + perms.indexOf(perm);
        for (const kw of ra.keys || []) {
          const c = keyCanon(kw.key);
          if (keyCanons.includes(c)) keyRows.push({ sort: [c, created, nameToU64(r.json.account_name), rank], row: { account_name: r.json.account_name, permission_name: perm.perm_name, authorizing_key: leapKey(kw.key), weight: kw.weight, threshold: ra.threshold } });
        }
        for (const aw of ra.accounts || []) {
          const pl = aw.permission || {};
          if (accts.some((a) => pl.actor === a.actor && (!a.permission || pl.permission === a.permission))) {
            acctRows.push({ sort: [`${pl.actor}@${pl.permission}`, created, nameToU64(r.json.account_name), rank], row: { account_name: r.json.account_name, permission_name: perm.perm_name, authorizing_account: { actor: pl.actor, permission: pl.permission }, weight: aw.weight, threshold: ra.threshold } });
          }
        }
      }
    }
    // Leap order: account-authorized rows first, then key rows; each by authorizer, then permission-creation order.
    const cmp = (x, y) => { for (let i = 0; i < 4; i++) { if (x.sort[i] < y.sort[i]) return -1; if (x.sort[i] > y.sort[i]) return 1; } return 0; };
    const seen = new Set();
    const rows = [...acctRows.sort(cmp), ...keyRows.sort(cmp)].map((x) => x.row).filter((x) => {
      const k = `${x.authorizing_key || `${x.authorizing_account.actor}@${x.authorizing_account.permission}`}|${x.account_name}|${x.permission_name}`;
      if (seen.has(k)) return false; seen.add(k); return true;
    });
    return reply(200, { accounts: rows }, { 'x-pulse-edge': 'polyfill; chain-verified' });
  },
  async get_transaction_id(p) {
    // sha256 of the packed transaction. Sound without an ABI when the caller sends packed_trx or hex action
    // data (what eosjs/wharfkit serialize anyway); JSON action data needs the contract ABI: 501 for that case.
    // leapParse already required a transaction object with an actions array (nodeos 5.0 rejects {transaction}
    // wrappers and {packed_trx} here), so only plain transactions reach this point.
    try { return reply(200, transactionId(p), { 'x-pulse-edge': 'polyfill' }); } catch (e) {
      if (e instanceof AbiNeeded) return unavailable('get_transaction_id', `action ${e.message} has JSON data; send packed_trx or hex-serialized action data`);
      return err(400, 'invalid_params', `get_transaction_id: ${e.message}`);
    }
  },
  get_code: async () => unavailable('get_code', 'the node exposes no way to read contract code bytes (get_code_hash and get_abi/get_raw_abi work)'),
  compute_transaction: async () => unavailable('compute_transaction', 'no dry-run execution endpoint'),
  send_read_only_transaction: async () => unavailable('send_read_only_transaction', 'no read-only execution endpoint'),
};
const DISABLE_DEFERRED_STAGE_1 = 'fce57d2331667353a0eac6b4209b67b843a7262a848af0a49a6e2fa9f6584eb4';
const discoveryDown = (r) => err(502, 'discovery_unavailable', `account discovery (federator ${FEDERATOR_URL}) unavailable: ${r.error || `HTTP ${r.status}`}; refusing to answer from an incomplete candidate set`);

function loadStatic(file) {
  try { return JSON.parse(fs.readFileSync(path.join(STATIC_DIR, file), 'utf8')); } catch { return null; }
}
const staticMissing = (ep, file) => err(501, 'unsupported_feature',
  `${ep} is served from ${path.join(STATIC_DIR, file)}, captured from the source chain at the cut (tools/capture-static.mjs); the file is missing`,
  `${ep} not available: static capture missing`, { 'x-pulse-edge': 'static-missing' });

// ---- routing -------------------------------------------------------------------------------------------------------
const SUPPORTED = [...NATIVE, 'send_transaction2', 'push_transactions', 'get_raw_block', 'get_block_header', 'get_block_header_state', 'get_producers',
  'get_producer_schedule', 'get_raw_code_and_abi', 'get_activated_protocol_features', 'get_consensus_parameters', 'get_scheduled_transactions',
  'get_accounts_by_authorizers', 'get_transaction_id'].sort();

async function route(req, raw) {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname; // exact, like nodeos: a trailing slash is a different (unknown) endpoint
  if (p.startsWith('/v1/history/')) {
    const r = await request(FEDERATOR_URL + p + url.search, { method: req.method === 'GET' ? 'GET' : 'POST', body: req.method === 'GET' ? undefined : (raw || '{}') });
    if (!r.status) return err(502, 'upstream_unavailable', `history (federator ${FEDERATOR_URL}) unreachable: ${r.error}`);
    return { status: r.status, raw: r.text, contentType: (r.headers && r.headers['content-type']) || 'application/json' };
  }
  if (p === '/v1/node/get_supported_apis') return reply(200, { apis: [...SUPPORTED.map((n) => `/v1/chain/${n}`), '/v1/history/*', '/v1/node/get_supported_apis'] });
  const m = p.match(/^\/v1\/chain\/([a-z0-9_]+)$/);
  if (!m) return notFound();
  const name = m[1];
  if (name === 'push_block') return notFound(); // not a producer node
  if (!NATIVE.has(name) && !POLY[name]) return notFound();
  const parsed = leapParse(name, raw); // nodeos validates the body before any handler runs
  if (parsed.fail) return parsed.fail;
  if (NATIVE.has(name)) {
    if (name === 'get_required_keys' && Array.isArray(parsed.params.available_keys)) {
      for (const k of parsed.params.available_keys) { const e = leapKeyError(k); if (e) return unparsable(...[].concat(e)); }
    }
    return passNative(name, raw, parsed.params);
  }
  return POLY[name](parsed.params);
}

function createEdge() {
  return http.createServer((req, res) => {
    res.setHeader('access-control-allow-origin', '*');
    if (req.method === 'OPTIONS') { // answer preflight here; never forward it
      res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
      res.setHeader('access-control-allow-headers', 'content-type');
      res.statusCode = 204; return res.end();
    }
    const chunks = []; let size = 0, tooBig = false;
    req.on('data', (c) => { size += c.length; if (size > MAX_BODY) tooBig = true; else chunks.push(c); });
    req.on('end', () => {
      const send = (out) => {
        res.statusCode = out.status;
        for (const [k, v] of Object.entries(out.headers || {})) res.setHeader(k, v);
        res.setHeader('content-type', out.contentType || 'application/json');
        res.end(out.raw != null ? out.raw : JSON.stringify(out.body));
      };
      if (tooBig) return send(err(413, 'request_too_large', `request body over ${MAX_BODY} bytes`));
      route(req, Buffer.concat(chunks).toString('utf8'))
        .then(send)
        .catch((e) => send(err(500, 'edge_exception', String((e && e.message) || e))));
    });
  });
}

module.exports = { createEdge, keyInfo, keyCanon, toPubKey, pubSpelling, legacySpelling, ripemd160js, b58decode, b58encode, nameToU64, packTransaction, transactionId, repairTimestamp, SUPPORTED };

if (require.main === module) {
  if (!NATIVE_BASE) { console.error('pulse-edge: NATIVE_BASE is required (http://127.0.0.1:9650/ext/bc/<BID>)'); process.exit(2); }
  const srv = createEdge();
  srv.listen(PORT, HOST, () => console.log(`pulse-edge on ${HOST}:${srv.address().port} (native=${NATIVE_BASE}, rpc=${RPC_URL}, federator=${FEDERATOR_URL}, static=${STATIC_DIR})`));
}
