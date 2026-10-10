// Pure helpers for mission control (no I/O side effects beyond DNS/fetch in the async ones), kept separate so
// they can be unit-tested: address classification (SSRF guard), capped/redirect-checked fetch, beacon report
// validation + public projection, and redaction of free-text fields before they reach the public dashboard.
import net from 'node:net';
import dns from 'node:dns/promises';
import http from 'node:http';
import https from 'node:https';
import { readFileSync } from 'node:fs';

// Beacon check names that are PREPARATION ("setup") checks; every other name, including ones this
// version does not know, is a HEALTH check. One shared list (check-kinds.json) for the agent's fleet
// gate (src/beacon.rs SETUP_CHECKS), this relay and the dashboard; tests on each side assert equality.
export const SETUP_CHECKS = Object.freeze(JSON.parse(readFileSync(new URL('./check-kinds.json', import.meta.url), 'utf8')).setup);
/** A report with any failing HEALTH check (failing setup checks do not count). Same rule as the agent's gate. */
export const hasFailingHealth = (report) => (report?.checks || []).some((c) => c && c.ok === false && !SETUP_CHECKS.includes(c.name));
/**
 * What an ABORTED actually did to the old chain: 'resumed' (rollback proven complete, before ignition),
 * 'forced' (forced rollback after ignition: this box's target was fenced first), 'incomplete' (a rollback
 * step failed: the old chain may NOT be producing), 'unknown' (older beacon: not reported). null if not ABORTED.
 */
export function abortKind(ce) {
  if (!ce || ce.state !== 'ABORTED') return null;
  if (ce.forced_rollback === true) return ce.rollback_complete === false ? 'incomplete' : 'forced';
  if (ce.rollback_complete === false) return 'incomplete';
  if (ce.rollback_complete === true) return 'resumed';
  return 'unknown';
}

// ---- address classification ---------------------------------------------------------------------------
const V4_BLOCKED = [
  ['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8], ['169.254.0.0', 16], ['172.16.0.0', 12],
  ['192.0.0.0', 24], ['192.0.2.0', 24], ['192.88.99.0', 24], ['192.168.0.0', 16], ['198.18.0.0', 15],
  ['198.51.100.0', 24], ['203.0.113.0', 24], ['224.0.0.0', 4], ['240.0.0.0', 4],
];
const v4int = (ip) => ip.split('.').reduce((a, o) => (a << 8n) + BigInt(+o), 0n);
function v4Public(ip) {
  const x = v4int(ip);
  return !V4_BLOCKED.some(([base, bits]) => (x >> BigInt(32 - bits)) === (v4int(base) >> BigInt(32 - bits)));
}
// Expand an IPv6 literal to 8 16-bit groups (handles :: and a trailing dotted IPv4).
function v6groups(ip) {
  let s = ip.split('%')[0].toLowerCase();
  const m = s.match(/^(.*:)(\d+\.\d+\.\d+\.\d+)$/);
  if (m) { const x = Number(v4int(m[2])); s = m[1] + ((x >>> 16) & 0xffff).toString(16) + ':' + (x & 0xffff).toString(16); }
  const [head, tail] = s.split('::');
  const h = head ? head.split(':') : [], t = tail !== undefined && tail ? tail.split(':') : [];
  const fill = tail !== undefined ? Array(8 - h.length - t.length).fill('0') : [];
  return [...h, ...fill, ...t].map((g) => parseInt(g || '0', 16));
}
const embeddedV4 = (g, i) => [g[i] >> 8, g[i] & 255, g[i + 1] >> 8, g[i + 1] & 255].join('.');
function v6Public(ip) {
  const g = v6groups(ip);
  if (g.length !== 8 || g.some((x) => !(x >= 0 && x <= 0xffff))) return false;
  if (g.every((x) => x === 0)) return false;                                   // ::
  if (g.slice(0, 7).every((x) => x === 0) && g[7] === 1) return false;         // ::1
  if (g.slice(0, 5).every((x) => x === 0) && g[5] === 0xffff) return v4Public(embeddedV4(g, 6)); // ::ffff:v4
  if (g.slice(0, 6).every((x) => x === 0)) return false;                       // ::v4 (deprecated compat)
  if (g[0] === 0x64 && g[1] === 0xff9b) return v4Public(embeddedV4(g, 6));     // NAT64
  if (g[0] === 0x2002) return v4Public(embeddedV4(g, 1));                      // 6to4
  if ((g[0] & 0xfe00) === 0xfc00) return false;                                // fc00::/7 ULA
  if ((g[0] & 0xffc0) === 0xfe80) return false;                                // fe80::/10 link-local
  if ((g[0] & 0xffc0) === 0xfec0) return false;                                // fec0::/10 site-local
  if ((g[0] & 0xff00) === 0xff00) return false;                                // multicast
  if (g[0] === 0x2001 && g[1] === 0x0db8) return false;                        // documentation
  if (g[0] === 0x2001 && g[1] < 0x200) return false;                           // 2001::/23 IETF special (Teredo, ORCHID…)
  if (g[0] === 0x0100 && g[1] === 0 && g[2] === 0 && g[3] === 0) return false; // discard-only
  return true;
}
/** true only for a syntactically valid, globally routable unicast IP literal. */
export function isPublicIp(ip) {
  const s = String(ip || '').trim();
  const v = net.isIP(s.split('%')[0]);
  if (v === 4) return v4Public(s);
  if (v === 6) return v6Public(s);
  return false;
}
/** Normalise an IPv4-mapped IPv6 literal (::ffff:1.2.3.4) to plain IPv4. */
export const normIp = (ip) => { const s = String(ip || '').trim(); const m = s.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/i); return m ? m[1] : s; };

/** Resolve a hostname; every address must be public. Returns the first address, or throws 'blocked address'. */
export async function resolvePublic(host, lookup = dns.lookup) {
  const h = String(host || '').replace(/^\[|\]$/g, '');
  if (net.isIP(h)) { if (!isPublicIp(h)) throw Object.assign(new Error('blocked address'), { code: 'EBLOCKED' }); return h; }
  const addrs = await lookup(h, { all: true, verbatim: true });
  if (!addrs.length || addrs.some((a) => !isPublicIp(a.address))) throw Object.assign(new Error('blocked address'), { code: 'EBLOCKED' });
  return addrs[0].address;
}

// ---- outbound concurrency + capped, redirect-validated fetch ------------------------------------------------
export function limiter(max) {
  let active = 0; const q = [];
  const next = () => { if (active >= max || !q.length) return; active++; const [fn, ok, ko] = q.shift(); fn().then(ok, ko).finally(() => { active--; next(); }); };
  return (fn) => new Promise((ok, ko) => { q.push([fn, ok, ko]); next(); });
}
/** Reject with a TimeoutError if `p` hasn't settled within `ms` (≤ 0 → immediately). */
export function withDeadline(p, ms) {
  const to = () => Object.assign(new Error('timeout'), { name: 'TimeoutError' });
  if (!(ms > 0)) { p.catch(() => {}); return Promise.reject(to()); }
  let t; return Promise.race([p, new Promise((_, ko) => { t = setTimeout(() => ko(to()), ms); })]).finally(() => clearTimeout(t));
}
export const UA = 'pulse-cutover-mission-control/1.0 (+https://control-rehearsal.protonnz.com)';

/**
 * One HTTP(S) exchange with the connection PINNED to `ip` (already validated as public): the socket's DNS
 * lookup is replaced so it can only ever reach that address, while TLS SNI and the Host header keep the real
 * hostname. This closes the DNS-rebinding gap between "we checked the name" and "fetch connected somewhere".
 * Resolves { status, headers (Headers), body (Buffer, ≤ maxBytes) }.
 */
function requestPinned(u, ip, { method = 'GET', headers = {}, body = null, timeoutMs = 6000, maxBytes = 1024 * 1024, onConnect } = {}) {
  return new Promise((resolve, reject) => {
    const mod = u.protocol === 'https:' ? https : http;
    const host = u.hostname.replace(/^\[|\]$/g, '');
    const fam = net.isIP(ip);
    const pinned = (hostname, opts, cb) => {
      if (typeof opts === 'function') { cb = opts; opts = {}; }
      onConnect?.(ip);
      if (opts && opts.all) cb(null, [{ address: ip, family: fam }]); else cb(null, ip, fam);
    };
    let done = false;
    const finish = (fn, v) => { if (done) return; done = true; clearTimeout(timer); fn(v); };
    const buf = body == null ? null : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body));
    const req = mod.request({
      protocol: u.protocol, hostname: host, port: u.port || undefined, path: (u.pathname || '/') + (u.search || ''), method,
      headers: { 'user-agent': UA, ...headers, ...(buf ? { 'content-length': buf.length } : {}) },
      lookup: pinned, servername: net.isIP(host) ? undefined : host, agent: false,
    }, (res) => {
      const hdrs = new Headers();
      for (const [k, v] of Object.entries(res.headers)) if (v != null) hdrs.set(k, Array.isArray(v) ? v.join(', ') : String(v));
      const len = +(res.headers['content-length'] || 0);
      if (len > maxBytes) { res.destroy(); return finish(reject, new Error('too large')); }
      const parts = []; let n = 0;
      res.on('data', (c) => { n += c.length; if (n > maxBytes) { res.destroy(); finish(reject, new Error('too large')); } else parts.push(c); });
      res.on('end', () => finish(resolve, { status: res.statusCode, headers: hdrs, body: Buffer.concat(parts) }));
      res.on('error', (e) => finish(reject, e));
    });
    const timer = setTimeout(() => { req.destroy(); finish(reject, Object.assign(new Error('timeout'), { name: 'TimeoutError' })); }, timeoutMs);
    req.on('error', (e) => finish(reject, e));
    if (buf) req.write(buf);
    req.end();
  });
}

/**
 * Request a producer-supplied URL: http(s) only, no credentials, every hop's hostname resolved and validated as
 * public, the connection pinned to that validated address, redirects followed manually (max 3) and re-validated.
 * `lookup` (tests) replaces dns.lookup for validation; `onConnect(ip)` (tests) observes the address actually dialled.
 */
export async function safeRequest(url, { method = 'GET', headers = {}, body = null, timeoutMs = 6000, maxBytes = 1024 * 1024, maxRedirects = 3, lookup, onConnect } = {}) {
  let u = new URL(url), m = method, b = body;
  const deadline = Date.now() + timeoutMs;
  for (let hop = 0; ; hop++) {
    if (!/^https?:$/.test(u.protocol)) throw new Error('blocked scheme');
    if (u.username || u.password) throw new Error('blocked credentials in URL');
    // DNS resolution counts against the same deadline: a stalled resolver must not hold a concurrency slot longer
    // than the request's own timeout (the lookup may finish later in the background; its result is ignored).
    const ip = await withDeadline(resolvePublic(u.hostname, lookup), deadline - Date.now());
    const left = deadline - Date.now(); if (left <= 0) throw Object.assign(new Error('timeout'), { name: 'TimeoutError' });
    const r = await requestPinned(u, ip, { method: m, headers, body: b, timeoutMs: left, maxBytes, onConnect });
    const loc = r.headers.get('location');
    if (r.status >= 300 && r.status < 400 && loc) {
      if (hop >= maxRedirects) throw new Error('too many redirects');
      u = new URL(loc, u);
      if (m !== 'GET' && r.status !== 307 && r.status !== 308) { m = 'GET'; b = null; }
      continue;
    }
    return r;
  }
}
/**
 * Does the producer API (/v1/producer/*) of the server at `ip` answer from the internet? Probes that address on
 * :8888 and :80, plus `urls` (the producer's own bp.json endpoints) only where the hostname resolves to `ip`:
 * every connection is pinned to `ip` and redirects are not followed, so this can never be pointed elsewhere.
 * The probe is the read-only POST /v1/producer/paused; "open" = a 2xx whose body is a JSON boolean.
 */
export async function probeProducerApi(ip, urls = [], { request = safeRequest, lookup = dns.lookup, timeoutMs = 4000 } = {}) {
  const host = net.isIPv6(ip) ? `[${ip}]` : ip;
  const cands = new Set([`http://${host}:8888`, `http://${host}`]);
  for (const u of urls) { const id = endpointId(u); if (id) cands.add(id); }
  const onlyIp = async (h, o) => {
    const a = await lookup(h, o);
    if (!a.some((x) => normIp(x.address) === ip)) throw Object.assign(new Error('not this server'), { code: 'EBLOCKED' });
    return [{ address: ip, family: net.isIP(ip) }];
  };
  const open = []; let checked = 0;
  await Promise.all([...cands].slice(0, 8).map(async (base) => {
    let h; try { h = new URL(base).hostname.replace(/^\[|\]$/g, ''); } catch { return; }
    if (net.isIP(h) && normIp(h) !== ip) return;
    try {
      const r = await request(`${base}/v1/producer/paused`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}',
        timeoutMs, maxBytes: 4096, maxRedirects: 0, lookup: onlyIp });
      checked++;
      if (r.status >= 200 && r.status < 300) { try { if (typeof JSON.parse(r.body.toString('utf8')) === 'boolean') open.push(base); } catch {} }
    } catch (e) { if (e?.code !== 'EBLOCKED') checked++; }
  }));
  return { exposed: open.length > 0, open: open.sort(), checked };
}
export async function safeJson(url, opts = {}, maxBytes = 1024 * 1024) {
  const r = await safeRequest(url, { ...opts, maxBytes });
  if (r.status < 200 || r.status >= 300) throw new Error(`HTTP ${r.status}`);
  return { json: JSON.parse(r.body.toString('utf8')), headers: r.headers, status: r.status };
}

// ---- endpoint identity ------------------------------------------------------------------------------------
/**
 * Canonical endpoint id: scheme://host[:port]/path — scheme and host lower-cased, IPv6 bracketed, default port
 * dropped, path case PRESERVED, trailing slashes trimmed. http vs https and /A vs /a are different endpoints.
 */
export function endpointId(u) {
  try {
    const x = new URL(String(u));
    if (!/^https?:$/.test(x.protocol)) return null;
    const path = x.pathname.replace(/\/+$/, '');
    return `${x.protocol}//${x.host.toLowerCase()}${path}`;
  } catch { return null; }
}
const b64u = (s) => Buffer.from(s, 'utf8').toString('base64url');
const unb64u = (s) => { try { return Buffer.from(s, 'base64url').toString('utf8'); } catch { return null; } };
/** Route segment for an endpoint page (URL-safe base64 of the canonical id). */
export const endpointRef = (u) => { const id = endpointId(u); return id ? b64u(id) : null; };
/** Inverse of endpointRef; null unless it decodes to a canonical http(s) endpoint id. */
export function endpointFromRef(ref) {
  if (!/^[A-Za-z0-9_-]{4,600}$/.test(String(ref || ''))) return null;
  const id = unb64u(ref);
  return id && endpointId(id) === id ? id : null;
}

// ---- safe decoding / routing ---------------------------------------------------------------------------
export const safeDecode = (s) => { try { return decodeURIComponent(s); } catch { return null; } };
export const RE = {
  net: /^[a-z0-9-]{1,32}$/,
  producer: /^[a-z1-5.]{1,12}$/,
  label: /^[\p{L}\p{N} ._()@+-]{1,64}$/u,
  endpoint: /^[A-Za-z0-9.-]{1,253}(:\d{1,5})?(\/[A-Za-z0-9._~\/-]{0,200})?$/,   // legacy host[/path] links
  sid: /^[0-9a-f]{16}$/,
};
const RESERVED = new Set(['__proto__', 'constructor', 'prototype']);
export const reservedKey = (k) => RESERVED.has(String(k));
/** Client route grammar; returns true when `pathname` is a page the SPA can render. */
export function isAppRoute(pathname, networks) {
  if (pathname === '/' || pathname === '/index.html') return true;
  const raw = pathname.replace(/\/+$/, '').split('/').slice(1);
  const seg = raw.map(safeDecode);
  if (seg.some((s) => s === null || reservedKey(s))) return false;
  if (!RE.net.test(seg[0] || '') || !networks.includes(seg[0])) return false;
  if (seg.length === 1) return true;
  if (!RE.producer.test(seg[1])) return false;
  if (seg.length === 2) return true;
  if (seg.length === 3) return RE.label.test(seg[2]);
  if (seg.length === 4) return seg[2] === 'endpoint' && (endpointFromRef(seg[3]) !== null || RE.endpoint.test(seg[3]));
  return false;
}

// ---- redaction ------------------------------------------------------------------------------------------
/** Strip control chars, URLs, IPs, filesystem paths and long hex/base64 blobs from free text; cap length. */
export function redact(s, max = 120) {
  if (s === null || s === undefined) return null;
  let t = String(s).replace(/[\u0000-\u001f\u007f]/g, ' ');
  t = t.replace(/\b[a-z][a-z0-9+.-]*:\/\/[^\s'"<>]+/gi, '<url>');
  t = t.replace(/\b\d{1,3}(\.\d{1,3}){3}(:\d+)?\b/g, '<ip>');
  t = t.replace(/\[?[0-9a-f]{0,4}(:[0-9a-f]{0,4}){2,7}\]?(:\d+)?/gi, (m) => (m.includes('::') || m.split(':').length > 4 ? '<ip>' : m));
  t = t.replace(/(^|[\s'"=(])(~?\/[^\s'"()]+)/g, '$1<path>');
  t = t.replace(/\b(PVT|PUB)_[A-Z0-9]+_[1-9A-HJ-NP-Za-km-z]{20,}\b/g, '<key>');
  t = t.replace(/\b5[HJK][1-9A-HJ-NP-Za-km-z]{49}\b/g, '<key>');
  t = t.replace(/[A-Za-z0-9+/=_-]{48,}/g, '<blob>');
  t = t.replace(/\s+/g, ' ').trim();
  return t.length > max ? t.slice(0, max - 1) + '…' : t;
}

// ---- beacon report schema + public projection -----------------------------------------------------------
export const STATES = ['ARMED', 'FROZEN', 'SNAPSHOTTED', 'VERIFIED', 'IGNITED', 'FLIPPED', 'LIVE', 'ABORTED', 'HALTED', 'STRANDED'];
export const PROFILES = ['readiness', 'ceremony'];
export const ROLES = ['producer', 'history', 'api', 'seed', 'query'];
export const EVIDENCE_ALLOW = ['h', 'chain_id', 'freeze_at', 'cut_height', 'cut_block_id', 'burnoff_transactions', 'snapshot_sha256',
  'fingerprints_digest', 'target_head_id', 'write_gap_ms', 'state_diff_identical', 'state_digest', 'state_diff_b_head', 'lineage_at_cut',
  'boot_genesis_sha256', 'head_at_pause', 'compare_allowed_digest', 'protocol_schedule_hash'];

class Bad extends Error {}
const bad = (path, what) => { throw new Bad(`${path}: ${what}`); };
const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const str = (v, path, { max = 64, re = null, nullable = false } = {}) => {
  if (v === undefined || v === null) { if (nullable) return null; bad(path, 'required string'); }
  if (typeof v !== 'string') bad(path, 'must be a string');
  if (v.length > max) bad(path, `longer than ${max}`);
  if (re && !re.test(v)) bad(path, 'invalid format');
  return v;
};
const int = (v, path, { min = 0, max = Number.MAX_SAFE_INTEGER, nullable = true } = {}) => {
  if (v === undefined || v === null) { if (nullable) return null; bad(path, 'required number'); }
  if (typeof v !== 'number' || !Number.isInteger(v) || v < min || v > max) bad(path, `must be an integer in [${min}, ${max}]`);
  return v;
};
const bool = (v, path, { nullable = true } = {}) => {
  if (v === undefined || v === null) { if (nullable) return null; bad(path, 'required boolean'); }
  if (typeof v !== 'boolean') bad(path, 'must be a boolean');
  return v;
};
const tsStr = (v, path, nullable = true) => {
  const s = str(v, path, { max: 40, nullable, re: /^\d{4}-\d{2}-\d{2}T[\d:.]+(Z|[+-]\d{2}:?\d{2})$/ });
  if (s !== null && Number.isNaN(Date.parse(s))) bad(path, 'not a timestamp');
  return s;
};
const numOrStr = (v, path) => {
  if (v === undefined || v === null) return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && /^[\w.-]{1,24}$/.test(v)) return v;
  bad(path, 'must be a short number or string');
};

/** lineage_at_cut: rc.5 sends a short verdict string ("verified" / "UNVERIFIED (…)"); later beacons may send
 *  {h, source_block_id, target_block_id, match}. Both are accepted and normalised. */
function lineage(v) {
  if (typeof v === 'string') { if (!/^[\w :().=,-]{1,64}$/.test(v)) bad('ceremony.evidence.lineage_at_cut', 'invalid verdict string'); return v; }
  if (!isObj(v)) bad('ceremony.evidence.lineage_at_cut', 'must be a string or an object');
  const hex = (x, p) => x == null ? null : str(x, p, { max: 64, re: /^[0-9a-fA-F]{64}$/ }).toLowerCase();
  return { h: int(v.h, 'ceremony.evidence.lineage_at_cut.h'), source_block_id: hex(v.source_block_id, 'ceremony.evidence.lineage_at_cut.source_block_id'),
    target_block_id: hex(v.target_block_id, 'ceremony.evidence.lineage_at_cut.target_block_id'), match: bool(v.match, 'ceremony.evidence.lineage_at_cut.match') };
}

/** ceremony.target (rc.23): what the fleet verdict compares — the target chain's Metal ids, the chain id it serves,
 *  its head and head block id, and the id of the first block after the cut (a common block above H). */
function targetView(t) {
  if (t == null) return null;
  if (!isObj(t)) bad('ceremony.target', 'must be an object');
  const hex = (x, p) => x == null ? null : str(x, p, { max: 64, re: /^[0-9a-fA-F]{64}$/ }).toLowerCase();
  const cb58 = (x, p) => str(x, p, { max: 64, nullable: true, re: /^[1-9A-HJ-NP-Za-km-z]{20,64}$/ });
  return { blockchain_id: cb58(t.blockchain_id, 'ceremony.target.blockchain_id'), subnet_id: cb58(t.subnet_id, 'ceremony.target.subnet_id'),
    chain_id: hex(t.chain_id, 'ceremony.target.chain_id'), head: int(t.head, 'ceremony.target.head'),
    head_id: hex(t.head_id, 'ceremony.target.head_id'), after_cut_id: hex(t.after_cut_id, 'ceremony.target.after_cut_id'),
    // rc.27: the target's protocol fields (PulseVM getInfo); absent on older beacons.
    protocol_upgrade_schedule_hash: hex(t.protocol_upgrade_schedule_hash, 'ceremony.target.protocol_upgrade_schedule_hash'),
    protocol_version: int(t.protocol_version, 'ceremony.target.protocol_version', { max: 0xffffffff }),
    supported_protocol_version: int(t.supported_protocol_version, 'ceremony.target.supported_protocol_version', { max: 0xffffffff }),
    next_protocol_upgrade: t.next_protocol_upgrade == null ? null : (isObj(t.next_protocol_upgrade) ? {
      protocol_version: int(t.next_protocol_upgrade.protocol_version, 'ceremony.target.next_protocol_upgrade.protocol_version', { max: 0xffffffff }),
      activation_height: int(t.next_protocol_upgrade.activation_height, 'ceremony.target.next_protocol_upgrade.activation_height', { max: 0xffffffff }),
    } : bad('ceremony.target.next_protocol_upgrade', 'must be an object')),
    // rc.25 (F4): how long the head has been unreadable (consecutive failed reads); absent on older beacons.
    unread_for_ms: int(t.unread_for_ms, 'ceremony.target.unread_for_ms') };
}

/**
 * Validate a beacon report. Throws Bad (message = path: problem) on any violation of the declared types;
 * returns ONLY the allow-listed public projection (unknown fields are dropped, free text is redacted).
 */
export function projectReport(r) {
  if (!isObj(r)) bad('report', 'must be an object');
  const out = {
    schema: str(r.schema, 'schema', { max: 40, nullable: true, re: /^[\w.-]+$/ }),
    producer: str(r.producer, 'producer', { max: 12, re: RE.producer }),
    network: str(r.network, 'network', { max: 32, re: RE.net }),
    node: str(r.node ?? 'node', 'node', { re: RE.label }),
    role: r.role == null ? null : (ROLES.includes(r.role) ? r.role : bad('role', `must be one of ${ROLES.join('|')}`)),
    instance_id: r.instance_id == null ? null : str(r.instance_id, 'instance_id', { max: 32, re: /^[0-9a-fA-F]{32}$/ }).toLowerCase(),
    profile: r.profile == null ? null : (PROFILES.includes(r.profile) ? r.profile : bad('profile', `must be one of ${PROFILES.join('|')}`)),
    agent_version: str(r.agent_version, 'agent_version', { max: 32, re: /^[0-9A-Za-z.+-]+$/ }),
    ts: tsStr(r.ts, 'ts', false),
    interval_secs: r.interval_secs === 0 ? null : int(r.interval_secs, 'interval_secs', { min: 1, max: 3600 }),
    mode: str(r.mode, 'mode', { max: 16, nullable: true, re: /^[a-z]+$/ }),
    ready: bool(r.ready, 'ready', { nullable: false }),
  };
  if (!Array.isArray(r.checks)) bad('checks', 'must be an array');
  if (r.checks.length > 40) bad('checks', 'more than 40 entries');
  out.checks = r.checks.map((c, i) => {
    if (!isObj(c)) bad(`checks[${i}]`, 'must be an object');
    return { name: str(c.name, `checks[${i}].name`, { max: 48, re: /^[a-z0-9_]+$/ }), ok: bool(c.ok, `checks[${i}].ok`, { nullable: false }),
      detail: c.detail == null ? '' : (typeof c.detail === 'string' && c.detail.length <= 400 ? redact(c.detail, 120) : bad(`checks[${i}].detail`, 'must be a string ≤ 400')) };
  });
  const s = r.source ?? {};
  if (!isObj(s)) bad('source', 'must be an object');
  out.source = { head: int(s.head, 'source.head'), lib: int(s.lib, 'source.lib'), chain_id: str(s.chain_id, 'source.chain_id', { max: 64, nullable: true, re: /^[0-9a-f]{64}$/ }) };

  const ce = r.ceremony ?? null;
  if (ce === null) out.ceremony = null;
  else {
    if (!isObj(ce)) bad('ceremony', 'must be an object');
    const st = (v, p) => (v == null ? null : STATES.includes(v) ? v : bad(p, `must be one of ${STATES.join('|')}`));
    const tr = ce.transitions ?? [];
    if (!Array.isArray(tr) || tr.length > 50) bad('ceremony.transitions', 'must be an array of ≤ 50');
    const ev = ce.evidence ?? {};
    if (!isObj(ev)) bad('ceremony.evidence', 'must be an object');
    const evOut = {};
    for (const k of EVIDENCE_ALLOW) {
      const v = ev[k];
      if (v === undefined || v === null) continue;
      if (k === 'lineage_at_cut') { evOut[k] = lineage(v); continue; }
      if (typeof v === 'number' && Number.isFinite(v)) evOut[k] = v;
      else if (typeof v === 'boolean') evOut[k] = v;
      else if (typeof v === 'string' && /^[\w:.-]{1,80}$/.test(v)) evOut[k] = v;
      else bad(`ceremony.evidence.${k}`, 'must be a number, boolean or short id string');
    }
    out.ceremony = {
      state: st(ce.state, 'ceremony.state'), since: tsStr(ce.since, 'ceremony.since'), seq: int(ce.seq, 'ceremony.seq'),
      transitions: tr.map((t, i) => { if (!isObj(t)) bad(`ceremony.transitions[${i}]`, 'must be an object'); return { state: st(t.state, `ceremony.transitions[${i}].state`), ts: tsStr(t.ts, `ceremony.transitions[${i}].ts`) }; }),
      evidence: evOut,
      // rc.5+ beacons send a sanitized class; older ones sent the raw journal error. Either way only a
      // re-redacted short hint is published (errors can carry commands, paths, URLs).
      last_error_class: ce.last_error_class == null ? null
        : (typeof ce.last_error_class === 'string' && ce.last_error_class.length <= 400 ? redact(ce.last_error_class, 120) : bad('ceremony.last_error_class', 'must be a string ≤ 400')),
      last_error: ce.last_error == null ? null : (typeof ce.last_error === 'string' ? `error (see local journal): ${redact(ce.last_error, 60)}` : bad('ceremony.last_error', 'must be a string')),
      armed_ts_ms: int(ce.armed_ts_ms, 'ceremony.armed_ts_ms'),
      // rc.7+: ignition may have started on this box / the last ABORTED was a forced rollback after
      // ignition. An ABORTED with ignition_started is not "pre-ceremony": its target may still run.
      ignition_started: bool(ce.ignition_started, 'ceremony.ignition_started'),
      forced_rollback: bool(ce.forced_rollback, 'ceremony.forced_rollback'),
      // rc.8+: the journal proves every rollback step completed (false = a step failed or the rollback
      // died part-way: the old chain may NOT be producing). null for older beacons.
      rollback_complete: bool(ce.rollback_complete, 'ceremony.rollback_complete'),
      // rc.9+: an operator rollback recorded its intent and has not finished (`run` refuses there).
      rollback_pending: bool(ce.rollback_pending, 'ceremony.rollback_pending'),
      // rc.23 (fleet verdict): chain creation / a join started on this box; the last ABORTED resumed the old chain;
      // a post-ignition symptom is being waited out because the fleet is live; this ceremony joined a chain.
      create_started: bool(ce.create_started, 'ceremony.create_started'),
      source_resumed: bool(ce.source_resumed, 'ceremony.source_resumed'),
      degraded: bool(ce.degraded, 'ceremony.degraded'),
      degraded_reason: ce.degraded_reason == null ? null
        : (typeof ce.degraded_reason === 'string' && ce.degraded_reason.length <= 400 ? redact(ce.degraded_reason, 120) : bad('ceremony.degraded_reason', 'must be a string ≤ 400')),
      joined: bool(ce.joined, 'ceremony.joined'),
      // rc.23 review #3: the agent journaled an abort intent; it is withdrawing its VERIFIED report.
      aborting: bool(ce.aborting, 'ceremony.aborting'),
      target: targetView(ce.target),
    };
  }
  const co = r.coord ?? null;
  if (co === null) out.coord = null;
  else {
    if (!isObj(co)) bad('coord', 'must be an object');
    out.coord = { event_id: str(co.event_id, 'coord.event_id', { max: 64, nullable: true, re: /^[\w.:-]+$/ }), h: int(co.h, 'coord.h'),
      accepted: bool(co.accepted, 'coord.accepted'), armed: bool(co.armed, 'coord.armed'), aborted: bool(co.aborted, 'coord.aborted'),
      reason: co.reason == null ? null : (typeof co.reason === 'string' ? redact(co.reason, 120) : bad('coord.reason', 'must be a string')),
      at: tsStr(co.at, 'coord.at') };
  }
  const m = r.metal ?? null;
  if (m === null) out.metal = null;
  else {
    if (!isObj(m)) bad('metal', 'must be an object');
    const b = m.bootstrapped ?? {};
    if (!isObj(b)) bad('metal.bootstrapped', 'must be an object');
    out.metal = {
      node_id: str(m.node_id, 'metal.node_id', { max: 64, nullable: true, re: /^NodeID-[1-9A-HJ-NP-Za-km-z]{20,60}$/ }),
      bls_public_key: str(m.bls_public_key, 'metal.bls_public_key', { max: 98, nullable: true, re: /^0x[0-9a-f]{96}$/ }),
      version: str(m.version, 'metal.version', { max: 40, nullable: true, re: /^[\w./+-]+$/ }),
      rpcchainvm: numOrStr(m.rpcchainvm, 'metal.rpcchainvm'), network_id: numOrStr(m.network_id, 'metal.network_id'),
      peers: int(m.peers, 'metal.peers', { max: 100000 }),
      bootstrapped: { P: bool(b.P, 'metal.bootstrapped.P'), X: bool(b.X, 'metal.bootstrapped.X'), C: bool(b.C, 'metal.bootstrapped.C') },
      healthy: bool(m.healthy, 'metal.healthy'), staking_reachable: bool(m.staking_reachable, 'metal.staking_reachable'),
    };
  }
  return out;
}
export const isBad = (e) => e instanceof Bad;

/** Heartbeat deadline for a report: 3 × its interval, never under 45 s. */
export const silentAfterMs = (report) => Math.max(3 * (report?.interval_secs || 10), 45) * 1000;

// ---- fleet verdict (rc.23) ---------------------------------------------------------------------------------
// The 5-BP rehearsal on the upstream stack ended every run with a LIVE/HALTED mix on ONE target chain, and once
// with a BP that resumed the old chain after its peers ignited — while the board showed only per-server states.
// fleetVerdict() turns the roster's reports for one event into one verdict BPs can read:
//   LIVE      ≥ quorum roster members LIVE on one target chain with a common block above H (same first post-cut id)
//   DEGRADED  a target may be running (ignition/creation started somewhere) but no quorum is LIVE on one chain yet
//   SPLIT     RED: a member resumed the old chain while another is past chain creation/ignition, the old chain moved
//             past the complete pause-head bound or kept advancing once every member was paused (rc.24 movement
//             rule), or members report different target chains / different blocks after the cut / at one height
//   STRANDED  every reporting member is sealed (STRANDED or ABORTED, at least one STRANDED), nobody past creation:
//             the fleet stopped and an operator decides (rollback re-runs the guard, or join a LIVE quorum) (rc.26)
//   STALLED   LIVE, but a quorum of the LIVE members' beacons report the post-LIVE watch failing (rc.27)
//   ABORTED   every reporting member aborted before chain creation (symmetric abort)
//   PENDING   no member is past chain creation yet
// It is display only (relay-reported, unsigned), never an authorization.
const PAST = ['IGNITED', 'FLIPPED', 'LIVE', 'HALTED'];
/** Did this report's last ABORTED resume the old chain? */
export const resumedOldChain = (ce) => !!ce && ce.state === 'ABORTED'
  && (ce.source_resumed === true || ['resumed', 'forced'].includes(abortKind(ce)));
/** Is this member past the point where a target chain may exist (creation/join/ignition started)? */
export const pastCreate = (ce) => !!ce && ce.state !== 'ABORTED'
  && (PAST.includes(ce.state) || ce.create_started === true || ce.ignition_started === true || ce.joined === true);
/**
 * @param ev     the event payload ({event_id, h, roster?, quorum?})
 * @param byProducer  { producer: [{ report, silent, conflict }] } (producer-role servers)
 */
/** Pause skew tolerated above the highest reported pause head, and head movement tolerated once every member is
 *  paused (late blocks absorbed after the pause), in blocks. */
export const BURNOFF_TOLERANCE = 12;
/** How long a LIVE member's target may be unreadable and still count toward the LIVE quorum (ms). */
export const UNREAD_GRACE_MS = 60_000;
// rc.24: there is no fixed burn-off bound any more. rc.23 fell back to cut + 360 when a beacon did not publish
// head_at_pause, and the 5-BP rehearsal (rc.22 beacons) paused correctly at cut + 377 (DPoS finality lag + the
// freeze lead + quiescence): a false latched SPLIT. Real XPR finality lag has no fixed bound, so the old-chain
// rules are (1) the COMPLETE pause-head bound, and (2) MOVEMENT: the old chain's head moving after the point where
// every honest source must be paused (see movementArmed / nextEventMark).
/** Ceremony state rank for the relay's per-event high-water mark. ABORTED / unknown rank 0 (the mark keeps the
 *  highest rank seen, so a member that paused and later aborted keeps its SNAPSHOTTED rank); STRANDED is a sealed
 *  post-verify state; HALTED is judged like post-ignition. */
export const STATE_RANK = Object.freeze({ ARMED: 1, FROZEN: 2, SNAPSHOTTED: 3, VERIFIED: 4, STRANDED: 4, IGNITED: 5, HALTED: 5, FLIPPED: 6, LIVE: 7 });
export const stateRank = (s) => (typeof s === 'string' && Object.hasOwn(STATE_RANK, s) ? STATE_RANK[s] : 0);
/** From SNAPSHOTTED on, a member's producer is paused at the cut. */
export const PAUSED_RANK = STATE_RANK.SNAPSHOTTED;
/**
 * Is the movement rule armed for this event? (the point after which the old chain's head must not move.)
 *   A. every roster member's mark is ≥ SNAPSHOTTED (all paused) and at least one member is past chain creation; or
 *   B. every REPORTING roster member (`fresh`) is ≥ SNAPSHOTTED and at least `quorum` members are past creation.
 * B exists so a member that went silent before reporting SNAPSHOTTED (r4: it lost the relay) cannot block the rule
 * forever. Its cost: a silent member that really is still in burn-off when a quorum is already past creation can
 * raise the alarm. That is accepted: once a quorum has a target chain, an old chain still advancing is the r4 risk.
 * Before either holds, a slow member's producers may legitimately still be making burn-off blocks.
 * @param ev     {roster?, quorum?}
 * @param marks  { producer: {rank, past_create} } for THIS event
 * @param fresh  Set of producers currently reporting this event
 */
export function movementArmed(ev, marks, fresh) {
  const roster = Array.isArray(ev?.roster) && ev.roster.length ? ev.roster.map((m) => m.producer) : null;
  const names = roster || Object.keys(marks || {});
  if (!names.length) return false;
  const quorum = roster ? (Number.isInteger(ev.quorum) && ev.quorum > 0 ? ev.quorum : roster.length) : names.length;
  const rank = (p) => (Number.isInteger(marks?.[p]?.rank) ? marks[p].rank : 0);
  const pastN = names.filter((p) => marks?.[p]?.past_create === true).length;
  if (!pastN) return false;
  if (names.every((p) => rank(p) >= PAUSED_RANK)) return true;
  const rep = names.filter((p) => fresh?.has(p));
  return rep.length > 0 && rep.every((p) => rank(p) >= PAUSED_RANK) && pastN >= quorum;
}
/**
 * The next per-event mark for a producer from one report (pure). Only ever raised: past_create, rank, src_max.
 * Once `armed` (or once src_first exists), the reporting member's source head is tracked: src_first is the first
 * head seen from that point, src_max the highest.
 * @returns the new mark, or null when nothing changed
 */
export function nextEventMark(cur, ce, sourceHead, armed, now) {
  const past = pastCreate(ce);
  const rank = Math.max(Number.isInteger(cur?.rank) ? cur.rank : 0, stateRank(ce?.state));
  const next = { past_create: !!(cur?.past_create || past), state: past ? (ce?.state || null) : (cur?.state ?? ce?.state ?? null),
    at: now, rank };
  // Durable foreign-H past-creation evidence (rc.27) is carried over; only the operator clears it.
  if (cur?.foreign_past) next.foreign_past = cur.foreign_past;
  if (cur?.foreign_overflow === true) next.foreign_overflow = true;
  let srcFirst = Number.isInteger(cur?.src_first) ? cur.src_first : null, srcMax = Number.isInteger(cur?.src_max) ? cur.src_max : null;
  if (Number.isInteger(sourceHead) && (armed || srcFirst != null)) {
    if (srcFirst == null) srcFirst = sourceHead;
    srcMax = Math.max(srcMax ?? sourceHead, sourceHead);
  }
  if (srcFirst != null) { next.src_first = srcFirst; next.src_max = srcMax ?? srcFirst; }
  if (!cur) return next;
  const same = cur.past_create === next.past_create && cur.state === next.state && cur.rank === next.rank
    && (cur.src_first ?? null) === (next.src_first ?? null) && (cur.src_max ?? null) === (next.src_max ?? null);
  return same ? null : next;
}
/**
 * rc.27 review: a report under this event id whose journal is for ANOTHER H but past chain creation/ignition (a stale
 * journal, or this event's config is the wrong one) is recorded DURABLY in the event's mark: `foreign_past` is a list of
 * distinct observations {state, h, instance_id, at} (one per H and instance), so a later report (another instance, a
 * fresh ABORTED, a second foreign H) never erases one. Only an operator retires an observation, by its H, on the
 * mission-control host after fencing that target (POST /api/admin/clear-foreign). Past FOREIGN_PAST_MAX observations the
 * mark keeps `foreign_overflow: true`, which blocks like an observation. Null = no change.
 */
export const FOREIGN_PAST_MAX = 64;
/** A mark's unresolved foreign observations (an rc.27-pre single object is read as a one-entry list). */
export const foreignList = (m) => (Array.isArray(m?.foreign_past) ? m.foreign_past : m?.foreign_past && typeof m.foreign_past === 'object' ? [m.foreign_past] : []);
/** Does this mark carry unresolved foreign-H past-creation evidence? */
export const foreignUnresolved = (m) => foreignList(m).length > 0 || m?.foreign_overflow === true;
export function foreignPastMark(cur, report, ev, now) {
  const raw = report?.ceremony;
  if (!raw || ceremonyFor(report, ev) || !pastCreate(raw)) return null;
  const h = Number.isSafeInteger(raw.evidence?.h) ? raw.evidence.h : null, instance_id = report.instance_id || null;
  const list = foreignList(cur);
  if (list.some((o) => o.h === h && o.instance_id === instance_id)) return null;
  const next = Object.assign({ past_create: false, state: null, at: now }, cur || {});
  if (list.length >= FOREIGN_PAST_MAX) {
    if (cur?.foreign_overflow === true) return null;
    next.foreign_overflow = true;
    return next;
  }
  next.foreign_past = [...list, { state: String(raw.state || 'past creation').slice(0, 16), h, instance_id, at: now }];
  return next;
}
/**
 * The report's ceremony, but only when its journal belongs to this event (rc.24 fleet rehearsal): a beacon pairs the
 * coordination state (`coord.event_id`, written by `await`) with whatever journal its config points at. A reused run
 * directory, or a journal left from an earlier ceremony next to a newer (even completed) event, put an old ABORTED
 * journal under the current event id and latched a false "resumed the old chain after peers ignited" SPLIT. A journal
 * armed for another H (`evidence.h`, else `evidence.cut_height`) is not this event's evidence: treated as no ceremony.
 */
export function ceremonyFor(report, ev) {
  const ce = report?.ceremony || null;
  if (!ce || !ev || !Number.isInteger(ev.h)) return ce;
  // `evidence.h` only (every beacon since rc.10): a cut-height fallback dropped real peers on an inexact cut (rc.25).
  const jh = ce.evidence?.h;
  return Number.isInteger(jh) && jh !== ev.h ? null : ce;
}
/** @param eventMax { producer: {past_create, state, rank?, src_first?, src_max?} } for THIS event — the relay's per-event high-water mark (an
 *  instance replacement cannot lower it). */
export function fleetVerdict(ev, byProducer, eventMax = {}) {
  if (!ev) return null;
  const roster = Array.isArray(ev.roster) && ev.roster.length ? ev.roster : null;
  const names = roster ? roster.map((m) => m.producer) : Object.keys(byProducer).filter((p) => (byProducer[p] || []).some((s) => s.report?.coord?.event_id === ev.event_id));
  const quorum = roster ? (Number.isInteger(ev.quorum) ? ev.quorum : roster.length) : null;
  const members = names.map((producer, i) => {
    const pin = roster?.[i]?.instance_id || null;
    const mine = (byProducer[producer] || []).filter((s) => s.report?.coord?.event_id === ev.event_id && (!pin || s.report.instance_id === pin));
    const usable = mine.filter((s) => !s.conflict);
    const s = usable.find((x) => !x.silent) || usable[0] || null;
    const ce = ceremonyFor(s?.report, ev);
    const em = eventMax?.[producer];
    const markPast = !!em && em.past_create === true && (em.event_id === undefined || em.event_id === ev.event_id);
    // rc.27 review (review blocker): ANY instance past creation counts (not only the freshest report), and a report for
    // another H that is past creation is surfaced: it may be a stale journal, or this event's config may be wrong.
    const anyPast = mine.some((x) => pastCreate(ceremonyFor(x.report, ev)));
    const foreignPast = mine.some((x) => !ceremonyFor(x.report, ev) && pastCreate(x.report?.ceremony))
      || (foreignUnresolved(em) && (em.event_id === undefined || em.event_id === ev.event_id));
    return { producer, state: ce?.state || null, fresh: !!s && !s.silent, conflict: mine.length > 0 && !usable.length,
      missing: !s, target: ce?.target || null, resumed: resumedOldChain(ce), past: pastCreate(ce) || markPast || anyPast, foreign_past: foreignPast,
      degraded: ce?.degraded === true,
      joined: ce?.joined === true, source_head: s?.report?.source?.head ?? null, head_at_pause: ce?.evidence?.head_at_pause ?? null,
      // rc.27: the beacon's post-LIVE watch (idle beyond post_live_max_idle_secs with the workload probe failing).
      target_live: (s?.report?.checks || []).find((c) => c?.name === 'target_live') || null,
      cut: ce?.evidence?.cut_height ?? null };
  });
  const alarms = [];
  // The old chain advanced past the cut + burn-off while a member is past creation (review #6): judged from ANY
  // member's source head, so it fires even when the BP that resumed is silent (r4: it had lost the relay, and the
  // others' nodeos followed its fork).
  const cut = ev.h ?? members.find((m) => m.cut != null)?.cut ?? null;
  if (cut != null) {
    // The bound is the highest reported pause head, and only when it is COMPLETE: every member that reached
    // SNAPSHOTTED (it reports a cut height) also reported its pause head. Plus a small tolerance for pause skew.
    // Incomplete (an older beacon without head_at_pause): no bound at all (rc.24; a fixed cut + N is wrong for
    // real finality lag) and the movement rule below covers it.
    const pauses = members.map((m) => m.head_at_pause).filter((x) => Number.isInteger(x));
    const complete = pauses.length > 0 && members.every((m) => m.cut == null || Number.isInteger(m.head_at_pause));
    const bound = complete ? Math.max(...pauses) + BURNOFF_TOLERANCE : null;
    const pastOnes = members.filter((m) => m.past);
    const ahead = bound == null ? [] : members.filter((m) => Number.isInteger(m.source_head) && m.source_head > bound);
    if (pastOnes.length && ahead.length) {
      alarms.push(`split: the old chain advanced past the cut (${ahead.map((m) => `${m.producer} source head ${m.source_head}`).join(', ')} > ${bound}) while ${pastOnes.map((m) => `${m.producer} ${m.state || 'past creation'}`).join(', ')} is past chain creation`);
    }
  }
  // Movement (rc.24): the relay tracks each member's source head from the point where every honest source must be
  // paused (movementArmed: all members ≥ SNAPSHOTTED and someone past creation). From there the head only moves if
  // a producer resumed the old chain (r4), whatever the finality lag was. Judged from the high-water mark, so it
  // holds when the resumer itself is silent and after the heads look normal again.
  const moved = names.map((p) => [p, eventMax?.[p]]).filter(([, em]) => em && (em.event_id === undefined || em.event_id === ev.event_id)
    && Number.isInteger(em.src_first) && Number.isInteger(em.src_max) && em.src_max - em.src_first > BURNOFF_TOLERANCE);
  if (moved.length) {
    alarms.push(`split: the old chain is still advancing after chain creation (${moved.map(([p, em]) => `${p} source head ${em.src_first}→${em.src_max}`).join(', ')})`);
  }
  const past = members.filter((m) => m.past);
  for (const m of members.filter((x) => x.resumed)) {
    const others = past.filter((x) => x.producer !== m.producer).map((x) => `${x.producer} ${x.state}`);
    if (others.length) alarms.push(`split: ${m.producer} resumed the old chain after peers ignited (${others.join(', ')})`);
  }
  const withTarget = past.filter((m) => m.target);
  const distinct = (f) => [...new Set(withTarget.map(f).filter((x) => x != null))];
  const chains = distinct((m) => m.target.blockchain_id || m.target.chain_id);
  if (chains.length > 1) alarms.push(`split: members report different target chains (${chains.map((c) => `${String(c).slice(0, 12)}…: ${withTarget.filter((m) => (m.target.blockchain_id || m.target.chain_id) === c).map((m) => m.producer).join(', ')}`).join(' | ')})`);
  const firsts = distinct((m) => m.target.after_cut_id);
  if (firsts.length > 1) alarms.push(`split: members report different blocks after the cut (${firsts.map((f) => `${f.slice(0, 12)}…: ${withTarget.filter((m) => m.target.after_cut_id === f).map((m) => m.producer).join(', ')}`).join(' | ')})`);
  const atHeight = {};
  for (const m of withTarget) if (Number.isInteger(m.target.head) && m.target.head_id) (atHeight[m.target.head] ||= new Set()).add(m.target.head_id);
  for (const [h, ids] of Object.entries(atHeight)) if (ids.size > 1) alarms.push(`split: members report different blocks at height ${h}`);
  // LIVE quorum: fresh LIVE members on one chain with one common block after the cut.
  const groups = {};
  // A member whose head read failed keeps its (immutable) block after the cut for UNREAD_GRACE_MS (rc.25 F4: one
  // timed-out read flapped the verdict LIVE↔DEGRADED with quorum = N); a target unreadable for longer drops out.
  // Same rule as the agent (src/fleet.rs live_view): no head and no read age (an older beacon) does not count.
  const readable = (t) => t.head != null || (Number.isInteger(t.unread_for_ms) && t.unread_for_ms <= UNREAD_GRACE_MS);
  for (const m of members.filter((x) => x.fresh && x.state === 'LIVE' && x.target?.after_cut_id && readable(x.target))) {
    const k = `${m.target.blockchain_id || m.target.chain_id || '?'}|${m.target.after_cut_id}`;
    (groups[k] ||= []).push(m.producer);
  }
  const ranked = Object.entries(groups).sort((a, b) => b[1].length - a[1].length);
  const best = ranked[0] || null;
  // Review #9: LIVE only for a UNIQUE group reaching the quorum (two groups each with a quorum is a split).
  const uniqueBest = !ranked[1] || !quorum || ranked[1][1].length < quorum;
  const liveChain = best ? { chain: best[0].split('|')[0], after_cut_id: best[0].split('|')[1], members: best[1] } : null;
  // rc.27: protocol upgrade schedule warnings (not alarms: nothing has split YET, but it will at activation).
  const warnings = [];
  for (const m of members.filter((x) => x.foreign_past)) {
    warnings.push(`${m.producer} reports a ceremony for ANOTHER H that is past chain creation: a stale journal or a wrong event config; agents treat it as blocking (no resume) — check before any rollback`);
  }
  const onTarget = withTarget.filter((m) => m.target.protocol_upgrade_schedule_hash);
  const schedules = [...new Set(onTarget.map((m) => m.target.protocol_upgrade_schedule_hash))];
  if (schedules.length > 1) warnings.push(`members loaded different protocol upgrade schedules: the chain splits at the next activation (${schedules.map((h) => `${h.slice(0, 12)}…: ${onTarget.filter((m) => m.target.protocol_upgrade_schedule_hash === h).map((m) => m.producer).join(', ')}`).join(' | ')})`);
  for (const m of onTarget) {
    const next = m.target.next_protocol_upgrade;
    if (next && Number.isInteger(m.target.supported_protocol_version) && m.target.supported_protocol_version < next.protocol_version) {
      warnings.push(`${m.producer}'s PulseVM supports protocol ${m.target.supported_protocol_version} but version ${next.protocol_version} activates at height ${next.activation_height}: it stops there unless upgraded`);
    }
  }
  let verdict;
  if (alarms.length) verdict = 'SPLIT';
  else if (quorum && liveChain && liveChain.members.length >= quorum && uniqueBest) {
    // rc.27 (fleet run f1): the verdict stayed LIVE on a chain that had stopped building. A quorum of the LIVE
    // members' beacons reporting the post-LIVE watch failing (no block beyond post_live_max_idle_secs AND the
    // workload probe failing) is STALLED. It clears by itself when the watch passes again.
    // A beacon-LOCAL failure (its collection budget ran out, or it cannot find the target RPC) is not evidence the
    // chain stopped (rc.27 review).
    const localOnly = (d) => /^(skipped|target RPC unknown)/.test(d || '');
    const stalled = members.filter((m) => liveChain.members.includes(m.producer) && m.fresh && m.target_live?.ok === false && !localOnly(m.target_live.detail));
    verdict = stalled.length >= quorum ? 'STALLED' : 'LIVE';
    if (verdict === 'STALLED') warnings.push(`target chain stalled: ${stalled.map((m) => `${m.producer}: ${m.target_live.detail || 'target_live failing'}`).join('; ')}`);
  }
  else if (past.length) verdict = 'DEGRADED';
  // rc.26 (fleet run d3): every reporting member sealed (STRANDED, or ABORTED), at least one STRANDED, nobody past
  // creation: the fleet stopped and waits for an operator decision (rollback or join); PENDING read as "nothing yet".
  // Review M3: a fleet-wide claim needs every member present and fresh; otherwise PENDING with who is unaccounted for.
  else if (members.length && !members.some((m) => m.foreign_past) && members.every((m) => !m.missing && m.fresh && ['STRANDED', 'ABORTED'].includes(m.state))
    && members.some((m) => m.state === 'STRANDED')) verdict = 'STRANDED';
  else if (members.length && !members.some((m) => m.foreign_past) && members.filter((m) => !m.missing).every((m) => m.state === 'ABORTED') && members.some((m) => !m.missing)) verdict = 'ABORTED';
  else verdict = 'PENDING';
  const unaccounted = members.filter((m) => m.missing || !m.fresh).map((m) => `${m.producer}: ${m.missing ? 'no report' : `silent (${m.state || '?'})`}`);
  const notLive = !liveChain && verdict !== 'SPLIT' ? unaccounted : liveChain ? members.filter((m) => !liveChain.members.includes(m.producer)).map((m) => `${m.producer}: ${m.missing ? 'no report' : m.conflict ? 'identity conflict' : !m.fresh ? `silent (${m.state || '?'})` : m.state}`) : [];
  return { event_id: ev.event_id, h: ev.h ?? null, roster: roster ? roster.length : null, quorum, verdict, alarms, warnings,
    live_chain: liveChain, not_live: notLive,
    detail: !roster ? 'no roster in the event: LIVE needs a roster and quorum' : null,
    members: members.map(({ producer, state, fresh, missing, conflict, resumed, past: p, degraded, joined, target }) =>
      ({ producer, state, fresh, missing, conflict, resumed, past_create: p, degraded, joined,
        target_chain: target ? (target.blockchain_id || target.chain_id) : null, head: target?.head ?? null, after_cut_id: target?.after_cut_id ?? null })) };
}
