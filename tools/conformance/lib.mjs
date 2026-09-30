// tools/conformance/lib.mjs — shared pieces of the /v1 differential conformance harness:
// HTTP with a per-host rate limit, the response normalizer, the classifier, a minimal JSON diff and the
// docs/V1-COVERAGE.md parser. Pure functions are exported for tools/conformance/test/*.test.mjs.
// No dependencies (Node >= 18).
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
// Key spellings, names and transaction packing are shared with the edge so both agree byte for byte.
const edge = require('../../gateway/server.js');
export const { keyInfo, pubSpelling, legacySpelling, nameToU64, packTransaction } = edge;

// ---- HTTP ------------------------------------------------------------------------------------------------------
// One limiter per origin: public nodes get at most `rps` requests per second from us, whatever the caller does.
const limiters = new Map();
export function setRate(rps) { limiters.clear(); RATE.rps = Math.max(0.1, Number(rps) || 4); }
const RATE = { rps: 4 };
async function slot(origin) {
  let l = limiters.get(origin);
  if (!l) { l = { next: 0 }; limiters.set(origin, l); }
  const now = Date.now(), at = Math.max(now, l.next);
  l.next = at + 1000 / RATE.rps;
  if (at > now) await new Promise((r) => setTimeout(r, at - now));
}

// req: {method, path, headers, body (string|null)}. Returns {status, headers, text, json, ms} or {error, ms}.
export async function send(base, req, { timeoutMs = 20000 } = {}) {
  const url = base.replace(/\/+$/, '') + req.path;
  await slot(new URL(url).origin);
  const t0 = Date.now();
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const init = { method: req.method || 'POST', headers: { ...(req.headers || {}) }, signal: ctl.signal, redirect: 'manual' };
    if (!['GET', 'HEAD', 'OPTIONS'].includes(init.method) && req.body != null) init.body = req.body;
    const r = await fetch(url, init);
    const text = await r.text();
    let json; try { json = JSON.parse(text); } catch { json = undefined; }
    const headers = {};
    for (const [k, v] of r.headers) if (/^(content-type|x-pulse-edge|x-pulse-federation|access-control-.*)$/.test(k)) headers[k] = v;
    return { status: r.status, headers, text, json, ms: Date.now() - t0 };
  } catch (e) {
    return { error: e.name === 'AbortError' ? `timeout after ${timeoutMs} ms` : String((e.cause && e.cause.code) || e.message || e), ms: Date.now() - t0 };
  } finally { clearTimeout(timer); }
}

// JSON POST for the corpus generator: returns parsed JSON or throws.
export async function post(base, path, body) {
  const r = await send(base, { method: 'POST', path, headers: { 'content-type': 'application/json' }, body: JSON.stringify(body || {}) });
  if (r.error) throw new Error(`${path}: ${r.error}`);
  if (r.status !== 200 || r.json === undefined) throw new Error(`${path}: HTTP ${r.status} ${(r.text || '').slice(0, 200)}`);
  return r.json;
}

// ---- JSON paths ----------------------------------------------------------------------------------------------------
// Patterns are dot-separated: `*` matches one segment (object key or array index), `**` matches the whole
// subtree below (zero or more segments). "rows.*.unpaid_blocks", "processed.**", "head_block_num".
export const splitPath = (p) => (p === '' || p === '$' ? [] : String(p).replace(/\[(\*|\d+)\]/g, '.$1').replace(/^\$\.?/, '').split('.').filter((s) => s !== ''));

// Visit every node addressed by `pattern` in `root`, calling fn(parent, key, value). `**` yields its anchor.
function visit(root, segs, fn, parent = null, key = null) {
  if (!segs.length) { fn(parent, key, parent == null ? root : parent[key], true); return; }
  const [s, ...rest] = segs;
  const cur = parent == null ? root : parent[key];
  if (s === '**') { fn(parent, key, cur, false); return; }
  if (cur == null || typeof cur !== 'object') return;
  const keys = s === '*' ? Object.keys(cur) : Object.prototype.hasOwnProperty.call(cur, s) ? [s] : [];
  for (const k of keys) visit(root, rest, fn, cur, k);
}

// Type token of a value; objects/arrays keep their shape (field presence + element shapes, not length).
export function shapeOf(v) {
  if (v === null) return '<null>';
  if (Array.isArray(v)) {
    const uniq = [...new Set(v.map((x) => JSON.stringify(shapeOf(x))))].sort();
    return uniq.map((s) => JSON.parse(s));
  }
  if (typeof v === 'object') { const o = {}; for (const k of Object.keys(v).sort()) o[k] = shapeOf(v[k]); return o; }
  return `<${typeof v}>`;
}

const clone = (v) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
function canon(v) { // stable key order, for comparison and hashing
  if (Array.isArray(v)) return v.map(canon);
  if (v && typeof v === 'object') { const o = {}; for (const k of Object.keys(v).sort()) o[k] = canon(v[k]); return o; }
  return v;
}

// ---- normalization --------------------------------------------------------------------------------------------------
// spec: {volatile, live, ignore, unordered} (arrays of path patterns)
//   ignore    removed entirely (fields one side adds by design: query_time_ms, pulsevm_head_block_time, …)
//   volatile  compared by shape only (types + field presence): head/LIB, versions, times, resource billing
//   live      like volatile, but only while the chain is moving; `frozen: true` compares them strictly
//             (the pre-flip rig holds the same state at H on both sides, so nothing may move there)
//   unordered arrays sorted before comparison (APIs whose order is not part of the contract)
//   mask      strings compared with timestamps and long numbers masked (error texts inside 2xx bodies)
// Error bodies (status >= 400) are reduced to what clients match on: HTTP code, error.code/name/what and the
// detail messages with timestamps and long numbers masked (file/line/method differ between builds).
const ISO_TS = /\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z?/g;
export const maskMessage = (s) => String(s).replace(ISO_TS, '<time>').replace(/[\w./-]+\.(?:cpp|hpp|cc|h|rs):\d+/g, '<src>').replace(/\b\d{6,}\b/g, '<n>');

export function normalizeError(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return body;
  const out = { ...body };
  if (out.error && typeof out.error === 'object') {
    const e = out.error;
    out.error = { code: e.code, name: e.name, what: e.what == null ? e.what : maskMessage(e.what) };
    if (Array.isArray(e.details)) out.error.details = e.details.map((d) => ({ message: maskMessage(d && d.message) }));
  }
  if (typeof out.message === 'string') out.message = maskMessage(out.message);
  return out;
}

export function normalize(json, spec = {}, { status = 200, frozen = false } = {}) {
  if (json === undefined) return undefined;
  let v = clone(json);
  const wrap = { $: v }; // lets a pattern address the root
  const at = (p) => ['$', ...splitPath(p)];
  for (const p of spec.ignore || []) visit(wrap, at(p), (parent, key) => { if (parent && key !== '$') { if (Array.isArray(parent)) parent[key] = '<ignored>'; else delete parent[key]; } }, null, null);
  const masks = [...(spec.volatile || []), ...(frozen ? [] : spec.live || [])];
  for (const p of masks) visit(wrap, at(p), (parent, key, val) => { if (parent) parent[key] = shapeOf(val); });
  for (const p of spec.mask || []) visit(wrap, at(p), (parent, key, val) => { if (parent && typeof val === 'string') parent[key] = maskMessage(val); });
  for (const p of spec.unordered || []) visit(wrap, at(p), (parent, key, val) => {
    if (parent && Array.isArray(val)) parent[key] = val.map(canon).sort((a, b) => { const x = JSON.stringify(a), y = JSON.stringify(b); return x < y ? -1 : x > y ? 1 : 0; });
  });
  v = wrap.$;
  if (status >= 400) v = normalizeError(v);
  return canon(v);
}

export const equal = (a, b) => JSON.stringify(canon(a)) === JSON.stringify(canon(b));

// ---- minimal JSON diff -------------------------------------------------------------------------------------------------
const brief = (v) => {
  if (typeof v === 'string' && v.length > 120) return `<string len=${v.length} sha256=${createHash('sha256').update(v).digest('hex').slice(0, 16)}>`;
  if (v && typeof v === 'object') { const s = JSON.stringify(v); return s.length > 200 ? `<${Array.isArray(v) ? 'array' : 'object'} ${s.slice(0, 160)}…>` : v; }
  return v;
};
export function jsonDiff(a, b, path = '', out = [], max = 20) {
  if (out.length >= max) return out;
  const ta = a === null ? 'null' : Array.isArray(a) ? 'array' : typeof a, tb = b === null ? 'null' : Array.isArray(b) ? 'array' : typeof b;
  if (ta !== tb) { out.push({ path: path || '$', a: brief(a), b: brief(b), kind: 'type' }); return out; }
  if (ta === 'array') {
    if (a.length !== b.length) out.push({ path: path || '$', a: `length ${a.length}`, b: `length ${b.length}`, kind: 'length' });
    for (let i = 0; i < Math.min(a.length, b.length) && out.length < max; i++) jsonDiff(a[i], b[i], `${path}[${i}]`, out, max);
    return out;
  }
  if (ta === 'object') {
    for (const k of [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()) {
      if (out.length >= max) break;
      const p = path ? `${path}.${k}` : k;
      if (!(k in b)) out.push({ path: p, a: brief(a[k]), b: '<absent>', kind: 'missing-in-b' });
      else if (!(k in a)) out.push({ path: p, a: '<absent>', b: brief(b[k]), kind: 'extra-in-b' });
      else jsonDiff(a[k], b[k], p, out, max);
    }
    return out;
  }
  if (a !== b) out.push({ path: path || '$', a: brief(a), b: brief(b), kind: 'value' });
  return out;
}

// ---- docs/V1-COVERAGE.md --------------------------------------------------------------------------------------------------
// -> Map endpoint -> {group:'chain'|'history', servedBy, b501, b404, partial, staticAtCut}
export function parseCoverage(md) {
  const out = new Map();
  let group = null;
  for (const line of String(md).split('\n')) {
    const h = line.match(/^##\s+(\/v1\/chain|\/v1\/history|\/v2)/);
    if (h) { group = h[1] === '/v1/chain' ? 'chain' : h[1] === '/v1/history' ? 'history' : null; continue; }
    if (/^##\s/.test(line)) { group = null; continue; }
    if (!group) continue;
    const m = line.match(/^\|\s*`([a-z0-9_]+)`\s*\|\s*([^|]+)\|/);
    if (!m) continue;
    const servedBy = m[2].trim();
    out.set(m[1], {
      group, servedBy,
      b501: /\b501\b/.test(servedBy),
      b404: /^404\b/.test(servedBy),
      partial: /partial/.test(servedBy),
      staticAtCut: /static-at-cut/.test(servedBy),
    });
  }
  return out;
}

// ---- classification --------------------------------------------------------------------------------------------------------
// Classes, in the order the report lists them:
export const CLASSES = ['identical', 'equal-after-normalization', 'equal-on-retry', 'B-501-expected', 'B-partial-expected', 'allowed',
  'unstable', 'error-shape-diff', 'DIFF', 'transport-error'];
export const FAILING = new Set(['error-shape-diff', 'DIFF', 'transport-error']);

// Header values compared case-insensitively and as sets for list headers ("POST, GET" == "get,post").
function headerValue(h, name) {
  const v = h && h[name];
  if (v == null) return '<absent>';
  return /allow-(methods|headers)|expose-headers/.test(name) ? String(v).toLowerCase().split(',').map((x) => x.trim()).filter(Boolean).sort().join(',') : String(v);
}
// What a browser concludes from a response (Fetch standard CORS checks), for req.cors = 'preflight' | 'simple'.
export function corsVerdict(req, r) {
  const h = r.headers || {}, origin = (req.headers && (req.headers.origin || req.headers.Origin)) || '';
  const acao = h['access-control-allow-origin'];
  const originOk = acao === '*' || (acao != null && acao === origin);
  if (req.cors !== 'preflight') return { origin_allowed: originOk };
  const list = (v) => String(v || '').toLowerCase().split(',').map((x) => x.trim()).filter(Boolean);
  const wantM = String((req.headers && req.headers['access-control-request-method']) || 'POST').toUpperCase();
  const wantH = list(req.headers && req.headers['access-control-request-headers']);
  const methods = list(h['access-control-allow-methods']), headers = list(h['access-control-allow-headers']);
  const safeMethod = ['GET', 'HEAD', 'POST'].includes(wantM);
  return {
    preflight_ok: r.status >= 200 && r.status < 300,
    origin_allowed: originOk,
    method_allowed: safeMethod || methods.includes('*') || methods.includes(wantM.toLowerCase()),
    headers_allowed: wantH.every((x) => headers.includes('*') || headers.includes(x)),
  };
}
const edgeHeader = (r) => (r && r.headers && r.headers['x-pulse-edge']) || '';
export function specOf(req) { return { volatile: req.volatile || [], live: req.live || [], ignore: req.ignore || [], unordered: req.unordered || [], mask: req.mask || [] }; }

// a, b: send() results. cov: parseCoverage() entry for the request's endpoint (may be undefined).
// -> {cls, diff?, note?}
export function classify(req, a, b, cov, { frozen = false } = {}) {
  if (a.error || b.error) return { cls: 'transport-error', note: [a.error && `A: ${a.error}`, b.error && `B: ${b.error}`].filter(Boolean).join('; ') };
  if (req.cors === 'preflight') { // CORS: compare what a browser would conclude, not header spelling (proxies word these differently)
    const va = corsVerdict(req, a), vb = corsVerdict(req, b);
    if (equal(va, vb)) return { cls: a.status === b.status ? 'identical' : 'equal-after-normalization' };
    return { cls: 'DIFF', diff: jsonDiff(va, vb), note: 'a browser would treat these two answers differently' };
  }
  const sameHeaders = (!req.compare_headers || req.compare_headers.every((h) => headerValue(a.headers, h) === headerValue(b.headers, h)))
    && (!req.cors || equal(corsVerdict(req, a), corsVerdict(req, b)));
  if (a.status === b.status && a.text === b.text && sameHeaders) return { cls: 'identical' };
  const endpointCov = cov || {};
  const expected501 = endpointCov.b501 || req.expect_b === '501';
  if (b.status === 501 && expected501 && a.status !== 501) {
    return { cls: 'B-501-expected', note: `B: ${(b.json && b.json.message) || 'HTTP 501'}` };
  }
  if (b.status === 404 && endpointCov.b404 && a.status !== 404) return { cls: 'B-501-expected', note: 'B answers 404 by design (docs/V1-COVERAGE.md)' };
  const spec = specOf(req);
  let na = a.json === undefined ? { __text: maskMessage(a.text) } : normalize(a.json, spec, { status: a.status, frozen });
  let nb = b.json === undefined ? { __text: maskMessage(b.text) } : normalize(b.json, spec, { status: b.status, frozen });
  if (req.compare_headers) { // CORS and friends: header parity is part of the contract for these requests
    const pick = (r) => Object.fromEntries(req.compare_headers.map((h) => [h, headerValue(r.headers, h)]));
    na = { __body: na, __headers: pick(a) }; nb = { __body: nb, __headers: pick(b) };
  }
  if (req.cors) { na = { __body: na, __cors: corsVerdict(req, a) }; nb = { __body: nb, __cors: corsVerdict(req, b) }; }
  if (a.status === b.status && equal(na, nb)) return { cls: 'equal-after-normalization' };
  const diff = a.status === b.status ? jsonDiff(na, nb) : [{ path: '(status)', a: a.status, b: b.status, kind: 'status' }, ...jsonDiff(na, nb, '', [], 10)];
  const hdr = edgeHeader(b);
  if (endpointCov.partial && /partial|wasm-unavailable/.test(hdr) && a.status === b.status) return { cls: 'B-partial-expected', diff, note: `x-pulse-edge: ${hdr}` };
  if (b.status === 501 && /static-missing/.test(hdr)) return { cls: 'DIFF', diff, note: 'B has no static-at-cut capture (run tools/capture-static.mjs)' };
  if (a.status >= 400 && b.status >= 400) return { cls: 'error-shape-diff', diff };
  return { cls: 'DIFF', diff, note: hdr ? `x-pulse-edge: ${hdr}` : undefined };
}

// Allow-list entries: {id?, id_prefix?, endpoint?, tag?, group?, class?, path?, reason}; every given field must match. A match turns a failing class into "allowed".
export function allowedBy(allow, req, result) {
  for (const e of allow || []) {
    if (e.id && e.id !== req.id) continue;
    if (e.id_prefix && !String(req.id).startsWith(e.id_prefix)) continue;
    if (e.group && e.group !== req.tags[2]) continue;
    if (e.endpoint && e.endpoint !== req.tags[0]) continue;
    if (e.tag && !req.tags.includes(e.tag)) continue;
    if (e.class && e.class !== result.cls) continue;
    if (e.path && !(result.diff || []).every((d) => d.path === e.path || d.path.startsWith(e.path + '.') || d.path.startsWith(e.path + '['))) continue;
    return e;
  }
  return null;
}

export const truncate = (s, n = 300) => { s = String(s == null ? '' : s); return s.length > n ? s.slice(0, n) + `… (${s.length} bytes)` : s; };

// ---- run-time transactions (failed-transaction cases) -----------------------------------------------------------------------
// t: corpus tx_template {actions, expire_in, ref?, signatures?, wrap}; st: {lib, libId}. Unsigned unless the template
// carries a (deliberately malformed) signature: these requests exist to compare how two endpoints REJECT them.
export function buildTx(t, st, now = Date.now()) {
  const expiration = new Date(Math.floor(now / 1000) * 1000 + (t.expire_in || 600) * 1000).toISOString().slice(0, 19);
  let prefix = Buffer.from(st.libId.slice(16, 24), 'hex').readUInt32LE(0);
  if (t.ref === 'bad-prefix') prefix = (prefix ^ 0xffffffff) >>> 0;
  const tx = { expiration, ref_block_num: st.lib & 0xffff, ref_block_prefix: prefix, max_net_usage_words: 0, max_cpu_usage_ms: 0, delay_sec: 0,
    context_free_actions: [], actions: t.actions, transaction_extensions: [] };
  const packed = { signatures: t.signatures || [], compression: 0, packed_context_free_data: '', packed_trx: packTransaction(tx).toString('hex') };
  if (t.wrap === 'send_transaction2') return { return_failure_trace: false, retry_trx: false, transaction: packed };
  if (t.wrap === 'send_transaction2-trace') return { return_failure_trace: true, retry_trx: false, transaction: packed };
  if (t.wrap === 'push_transactions') return [packed];
  return packed;
}
