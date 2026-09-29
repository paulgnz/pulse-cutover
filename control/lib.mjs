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
export const STATES = ['ARMED', 'FROZEN', 'SNAPSHOTTED', 'VERIFIED', 'IGNITED', 'FLIPPED', 'LIVE', 'ABORTED', 'HALTED'];
export const PROFILES = ['readiness', 'ceremony'];
export const ROLES = ['producer', 'history', 'api', 'seed', 'query'];
export const EVIDENCE_ALLOW = ['h', 'chain_id', 'freeze_at', 'cut_height', 'cut_block_id', 'burnoff_transactions', 'snapshot_sha256',
  'fingerprints_digest', 'target_head_id', 'write_gap_ms', 'state_diff_identical', 'state_digest', 'state_diff_b_head', 'lineage_at_cut'];

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
