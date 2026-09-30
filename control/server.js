#!/usr/bin/env node
// pulse-cutover mission control — a readiness + ceremony status board for several networks.
//
//   NETWORKS=control/networks.json TOKENS=/etc/pulse-control/tokens.json COORD_FILE=/var/lib/pulse-control/coord.json \
//     PORT=8787 node control/server.js
//
// - Polls every configured network's public RPC (read-only): head, LIB, head producer, producer schedule.
// - Accepts beacon reports (POST /api/report, `Authorization: Bearer <token>`). The tokens file holds
//   sha256(token) → {network, producer}; a report is accepted only for the network/producer its token is bound to,
//   only if it matches the report schema, and only an allow-listed, redacted projection of it is ever published.
// - Serves GET /api/status (JSON) and the dashboard (GET / and the app's page routes).
// No dependencies (Node >= 18). Nothing here can change a chain or a node: it only reads and displays.
// MC_OFFLINE=1 disables all outbound polling (tests). PORT=0 picks a free port (printed on start).
import http from 'node:http';
import net from 'node:net';
import { createHash, createPublicKey, verify as edVerify } from 'node:crypto';
import { readFileSync, existsSync, watchFile, renameSync, mkdirSync, openSync, writeSync, fsyncSync, closeSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isPublicIp, normIp, resolvePublic, limiter, safeRequest, probeProducerApi, safeJson, safeDecode, RE, UA, endpointId, endpointRef, reservedKey,
  isAppRoute, projectReport, isBad, silentAfterMs, redact, hasFailingHealth } from './lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const PORT = +(process.env.PORT ?? 8787);
const HOST = process.env.HOST || '127.0.0.1';
const NETWORKS_FILE = process.env.NETWORKS || join(HERE, 'networks.json');
const TOKENS_FILE = process.env.TOKENS || '';
const COORD_FILE = process.env.COORD_FILE || '/var/lib/pulse-control/coord.json';
const REPLAY_FILE = process.env.REPLAY_FILE || join(dirname(COORD_FILE), 'replay.json'); // rc.6 format, migrated on start
const STATE_FILE = process.env.STATE_FILE || join(dirname(COORD_FILE), 'servers.json');
const OFFLINE = process.env.MC_OFFLINE === '1';
const MAX_BODY = 256 * 1024;
const TS_FUTURE_MS = 2 * 60e3, TS_PAST_MS = 5 * 60e3;

// One bad request, probe or timer must never take the board down.
process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e?.stack || e));
process.on('uncaughtException', (e) => console.error('uncaughtException:', e?.stack || e));

const sha = (s) => createHash('sha256').update(s).digest('hex');
const dict = () => Object.create(null);
// Deep copy into null-prototype maps (keys like "constructor" or "__proto__" can never resolve to Object.prototype).
const clone = (o) => JSON.parse(JSON.stringify(o), (k, v) => (v && typeof v === 'object' && !Array.isArray(v) ? Object.assign(dict(), v) : v));
const own = (o, k) => o != null && Object.prototype.hasOwnProperty.call(o, k);
let cfg = JSON.parse(readFileSync(NETWORKS_FILE, 'utf8'));
const netIds = () => cfg.networks.map((n) => n.id);

// ---- tokens: validated reload; a removed file revokes everything --------------------------------------------
let tokens = dict();
function loadTokens() {
  if (!TOKENS_FILE) return;
  if (!existsSync(TOKENS_FILE)) { if (Object.keys(tokens).length) console.error('tokens: file missing, all tokens revoked'); tokens = dict(); return; }
  try {
    const raw = JSON.parse(readFileSync(TOKENS_FILE, 'utf8'));
    const next = dict();
    for (const [h, b] of Object.entries(raw)) {
      if (!/^[0-9a-f]{64}$/.test(h) || !b || !RE.net.test(b.network) || !RE.producer.test(b.producer)) throw new Error(`bad entry ${h.slice(0, 12)}`);
      next[h] = { network: b.network, producer: b.producer };
    }
    tokens = next;
  } catch (e) { console.error(`tokens: reload rejected (${e.message}); keeping the previous set`); }
}
loadTokens();
if (TOKENS_FILE) watchFile(TOKENS_FILE, { interval: 2000 }, loadTokens);
watchFile(NETWORKS_FILE, { interval: 2000 }, () => { try { cfg = JSON.parse(readFileSync(NETWORKS_FILE, 'utf8')); } catch (e) { console.error('networks: reload rejected', e.message); } });

// ---- state (null-prototype maps: no user-controlled key can reach Object.prototype) ------------------------
const chain = dict();    // network id → { head, lib, head_producer, chain_id, head_time, rpc, ok, ts, schedule[] }
const nodes = dict();    // network id → producer → tokenHash → { report, received, hist, first_seen, instance_id, conflict }
const events = dict();   // network id → [{ ts, producer, text }]
const pushEvent = (n, producer, text) => {
  (events[n] ||= []).unshift({ ts: new Date().toISOString(), producer, text });
  events[n].length = Math.min(events[n].length, 200);
};

// Every outbound request identifies itself (some edges 403 Node's default user-agent). Producer-supplied URLs go
// through safeRequest (DNS-validated and connection-pinned); only our own configured network RPCs use fetch.
const outbound = limiter(16); // global cap on concurrent producer-directed probes

// Network RPCs come from our own config (trusted), not from producers.
async function rpc(url, path, body = {}) {
  const r = await fetch(`${url}/v1/chain/${path}`, { method: 'POST', body: JSON.stringify(body), headers: { 'user-agent': UA }, signal: AbortSignal.timeout(4000) });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

async function pollNetwork(n) {
  const c = (chain[n.id] ||= { schedule: [], scheduleTs: 0 });
  for (const url of n.rpc || []) {
    try {
      const info = await rpc(url, 'get_info');
      Object.assign(c, { head: info.head_block_num, lib: info.last_irreversible_block_num, head_producer: info.head_block_producer,
        chain_id: info.chain_id, head_time: info.pulsevm_head_block_time || info.head_block_time, rpc: url, ok: true, ts: Date.now(),
        chain_id_ok: !n.chain_id || info.chain_id === n.chain_id });
      if (Date.now() - c.scheduleTs > 30000) {
        try {
          const s = await rpc(url, 'get_producer_schedule');
          const list = (s.active?.producers || []).map((p) => p.producer_name).filter((x) => RE.producer.test(x));
          if (list.length) { c.schedule = list; c.scheduleTs = Date.now(); c.schedule_version = s.active?.version; }
        } catch {}
      }
      return;
    } catch (e) { c.ok = false; c.error = String(e.message || e); }
  }
}
if (!OFFLINE) {
  setInterval(() => cfg.networks.forEach((n) => pollNetwork(n).catch(() => {})), 3000);
  cfg.networks.forEach((n) => pollNetwork(n).catch(() => {}));
}

// ---- producer registry: eosio::producers (active) + each producer's chains.json → bp.json -------------
// Everything fetched from producer-controlled URLs goes through safeRequest (public addresses only, the socket
// pinned to the validated address, manual redirects re-validated, size-capped, globally rate-limited).
const registry = dict(); // network id → { ts, producers: { owner → {owner, votes, rank, url, org{…}} } }
const logos = new Map(); // `${net}/${owner}` → { type, buf, ts }
const logoInflight = new Map();
const safeUrl = (u) => { try { const x = new URL(u); return /^https?:$/.test(x.protocol) && !x.username && !x.password ? x.href.replace(/\/$/, '') : null; } catch { return null; } };
const getJson = (url, ms = 5000) => outbound(() => safeJson(url, { timeoutMs: ms, headers: { accept: 'application/json' } }, 1024 * 1024)).then((x) => x.json);
async function bpJson(base, chainId) {
  // chains.json maps chain_id → path of that chain's bp.json (EOSIO standard); fall back to /bp.json.
  try {
    const cj = await getJson(`${base}/chains.json`);
    const path = cj?.chains?.[chainId];
    if (typeof path === 'string') return await getJson(new URL(path, base + '/').href);
  } catch {}
  return getJson(`${base}/bp.json`);
}
async function refreshRegistry(n) {
  const c = chain[n.id]; if (!c?.rpc) return;
  const reg = (registry[n.id] ||= { ts: 0, producers: dict() });
  let rows = [];
  try {
    for (let lower = '', i = 0; i < 20; i++) {
      const r = await rpc(c.rpc, 'get_table_rows', { json: true, code: 'eosio', scope: 'eosio', table: 'producers', limit: 500, lower_bound: lower });
      rows.push(...(r.rows || []));
      if (!r.more) break; lower = r.next_key || r.more;
    }
  } catch { return; }
  const active = rows.filter((p) => (p.is_active === 1 || p.is_active === true) && RE.producer.test(p.owner))
    .sort((a, b) => parseFloat(b.total_votes) - parseFloat(a.total_votes));
  const next = dict();
  active.forEach((p, i) => { next[p.owner] = { ...(reg.producers[p.owner] || {}), owner: p.owner, votes: parseFloat(p.total_votes), rank: i + 1, url: p.url }; });
  reg.producers = next; reg.ts = Date.now();
  // Enrich from bp.json, a few at a time, refreshing entries older than an hour.
  const todo = Object.values(next).filter((p) => !p.org_ts || Date.now() - p.org_ts > 3600e3);
  for (let i = 0; i < todo.length; i += 6) {
    await Promise.all(todo.slice(i, i + 6).map(async (p) => {
      const base = safeUrl(p.url); p.org_ts = Date.now();
      if (!base) return;
      try {
        const j = await bpJson(base, n.chain_id);
        const o = j.org || {};
        const loc = o.location || {};
        const logo = safeUrl(o.branding?.logo_256 || o.branding?.logo_1024 || o.branding?.logo_svg || '');
        p.org = { name: String(o.candidate_name || p.owner).slice(0, 80), website: safeUrl(o.website || base), logo,
          country: String(loc.country || '').slice(0, 8), city: String(loc.name || '').slice(0, 80), lat: Number(loc.latitude), lon: Number(loc.longitude) };
      } catch { p.org = p.org || { name: p.owner, website: base }; }
    }));
  }
}
if (!OFFLINE) {
  setInterval(() => cfg.networks.forEach((n) => { if (!n.static_producers) refreshRegistry(n).catch(() => {}); }), 10 * 60e3);
  setTimeout(() => cfg.networks.forEach((n) => { if (!n.static_producers) refreshRegistry(n).catch(() => {}); }), 4000);
}

async function logo(netId, owner) {
  const key = `${netId}/${owner}`; const hit = logos.get(key);
  if (hit && Date.now() - hit.ts < 6 * 3600e3) return hit;
  if (logoInflight.has(key)) return logoInflight.get(key);
  const src = registry[netId]?.producers?.[owner]?.org?.logo; if (!src) return null;
  const p = outbound(async () => {
    try {
      const r = await safeRequest(src, { timeoutMs: 6000, maxBytes: 512 * 1024 });
      const type = (r.headers.get('content-type') || '').split(';')[0].trim();
      if (r.status < 200 || r.status >= 300 || !/^image\/[\w.+-]+$/.test(type)) return null;
      const v = { type, buf: r.body, ts: Date.now() }; logos.set(key, v); return v;
    } catch { return null; }
  }).finally(() => logoInflight.delete(key));
  logoInflight.set(key, p);
  return p;
}

// ---- infrastructure survey: every node of every active producer (bp.json nodes) ------------------------
// Read-only public probes, bounded concurrency, every INFRA_EVERY_MS: API get_info (version, head, latency, edge),
// Hyperion /v2/health, AtomicAssets /health, and a TCP connect to each advertised p2p endpoint.
const INFRA_EVERY_MS = +(process.env.INFRA_EVERY_MS || 10 * 60e3);
const n_id = (cid) => cfg.networks.find((x) => x.chain_id === cid)?.id;
const infra = dict(); // network id → { ts, running, nodes: [...] }
const edgeOf = (h) => {
  const sv = (h.get('server') || '').toLowerCase(), via = (h.get('via') || '').toLowerCase();
  if (h.get('cf-ray') || sv.includes('cloudflare')) return 'cloudflare';
  for (const k of ['openresty', 'nginx', 'haproxy', 'caddy', 'apache', 'envoy', 'traefik']) if (sv.includes(k) || via.includes(k)) return k;
  return sv.startsWith('nodeos') ? 'direct / hidden' : sv ? sv.replace(/[^\w ./-]/g, '').slice(0, 20) : 'hidden';
};
// Probe failures become a short, human label for the table; the raw message is kept (trimmed) for tooltips.
function shortErr(e) {
  const m = String(e?.message || e), code = e?.cause?.code || e?.code || '';
  if (code === 'EBLOCKED' || /blocked address|blocked scheme/.test(m)) return 'non-public address';
  if (e?.name === 'SyntaxError' || /Unexpected token|not valid JSON|JSON/.test(m)) return /'<'|<!DOCTYPE|<html/i.test(m) ? 'HTML page, not API' : 'not JSON';
  if (e?.name === 'TimeoutError' || e?.name === 'AbortError' || /timeout|aborted/i.test(m)) return 'timeout';
  if (/ECONNREFUSED/.test(code)) return 'refused';
  if (/ENOTFOUND|EAI_AGAIN/.test(code)) return 'DNS failed';
  if (/ECONNRESET|UND_ERR_SOCKET/.test(code)) return 'connection reset';
  if (/CERT|SSL|TLS|self.signed/i.test(code + m)) return 'TLS error';
  if (/^HTTP \d+/.test(m)) return m.slice(0, 12);
  if (/too large/.test(m)) return 'response too large';
  if (/redirect/.test(m)) return 'too many redirects';
  if (/fetch failed/.test(m)) return code ? code.toLowerCase() : 'unreachable';
  return redact(m, 24);
}
async function timed(fn) { const t = Date.now(); try { const r = await fn(); return { ok: true, ms: Date.now() - t, ...r }; } catch (e) { return { ok: false, ms: Date.now() - t, error: shortErr(e), error_detail: redact(String(e?.message || e), 160) }; } }
// TCP connect to a producer-advertised host:port — only after resolving it to a public address.
function tcp(hostport) {
  return outbound(async () => {
    const m = String(hostport).trim().match(/^\[?([^\]\s]+?)\]?:(\d{1,5})$/); if (!m || +m[2] < 1 || +m[2] > 65535) return { ok: false, error: 'bad endpoint' };
    let ip; try { ip = await resolvePublic(m[1]); } catch (e) { return { ok: false, error: e.code === 'EBLOCKED' ? 'non-public address' : 'DNS failed' }; }
    return new Promise((res) => {
      const t = Date.now(); const s = net.connect({ host: ip, port: +m[2], timeout: 4000 });
      s.once('connect', () => { s.destroy(); res({ ok: true, ms: Date.now() - t }); });
      s.once('timeout', () => { s.destroy(); res({ ok: false, error: 'timeout' }); });
      s.once('error', (e) => res({ ok: false, error: e.code || 'error' }));
    });
  });
}
const probeJson = (url, opts) => outbound(() => safeJson(url, { timeoutMs: 6000, ...opts }, 256 * 1024));
async function probeApi(url, chainId) {
  return timed(async () => {
    const { json: b, headers } = await probeJson(`${url}/v1/chain/get_info`, { method: 'POST', body: '{}' });
    const head = Number.isInteger(b.head_block_num) ? b.head_block_num : null;
    return { edge: edgeOf(headers), version: typeof b.server_version_string === 'string' ? b.server_version_string.slice(0, 40) : null, head,
      lag: chain[n_id(chainId)]?.head && head ? Math.max(0, chain[n_id(chainId)].head - head) : null,
      chain_ok: !chainId || b.chain_id === chainId };
  });
}
async function probeHyperion(url, head) {
  return timed(async () => {
    const { json: h } = await probeJson(`${url}/v2/health`);
    if (!Array.isArray(h.health) || !h.version) throw new Error('not Hyperion');
    const idx = h.health.map((x) => x?.service_data?.last_indexed_block).filter((v) => Number.isInteger(v) && v > 0).sort((x, y) => y - x)[0];
    const services = h.health.slice(0, 12).map((x) => ({ s: String(x?.service || '?').slice(0, 24), ok: x?.status === 'OK' }));
    return { version: String(h.version).slice(0, 24), indexed: idx || null, lag: head && idx ? Math.max(0, head - idx) : null,
      services, services_ok: services.every((x) => x.ok) };
  });
}
async function probeAtomic(url) {
  return timed(async () => {
    const { json: h } = await probeJson(`${url}/health`);
    return { version: typeof h.data?.version === 'string' ? h.data.version.slice(0, 24) : null, chain_ok: h.data?.chain?.status === 'OK',
      head: Number.isInteger(h.data?.chain?.head_block) ? h.data.chain.head_block : null };
  });
}
async function pool(items, n, fn) { let i = 0; await Promise.all(Array.from({ length: n }, async () => { while (i < items.length) { const k = i++; await fn(items[k]); } })); }
const clip = (v, n) => (v == null ? null : String(v).slice(0, n));
async function surveyInfra(n) {
  const reg = registry[n.id]?.producers; const c = chain[n.id];
  if (!reg || !c?.head || infra[n.id]?.running) return;
  const run = (infra[n.id] ||= { nodes: [] }); run.running = true;
  const found = [];
  try {
  await pool(Object.values(reg), 4, async (p) => { try {
    const base = safeUrl(p.url); if (!base) return;
    let bp; try { bp = await bpJson(base, n.chain_id); } catch { found.push({ producer: p.owner, rank: p.rank, missing_bp_json: true }); return; }
    for (const nd of (Array.isArray(bp.nodes) ? bp.nodes : []).slice(0, 12)) {
      const types = [].concat(nd?.node_type || []).map((x) => clip(x, 24)).slice(0, 6);
      const features = [].concat(nd?.features || []).map((x) => clip(x, 32)).slice(0, 12);
      const url = safeUrl(nd?.ssl_endpoint || nd?.api_endpoint || '');
      const urls = [...new Set([nd?.ssl_endpoint, nd?.api_endpoint].map((x) => safeUrl(x || '')).filter(Boolean))];
      const p2p = typeof nd?.p2p_endpoint === 'string' ? nd.p2p_endpoint.slice(0, 120) : null;
      const e = { producer: p.owner, rank: p.rank, types, features, url, urls, id: url ? endpointId(url) : null, ref: url ? endpointRef(url) : null, p2p, location: clip(nd?.location?.name || nd?.location?.country || null, 60) };
      const jobs = [];
      // bp.json "query" nodes whose only feature is a non-chain service (e.g. atomic-assets-api) are not chain APIs:
      // probing /v1/chain on them just reports a false 404.
      const serviceOnly = features.length && !features.some((f) => /chain-api|hyperion|history|push-api/.test(f));
      if (url) {
        if (!serviceOnly) jobs.push(probeApi(url, n.chain_id).then((r) => (e.api = r)));
        if (features.includes('hyperion-v2') || types.includes('query')) jobs.push(probeHyperion(url, c.head).then((r) => { if (r.ok || features.includes('hyperion-v2')) e.hyperion = r; }));
        if (features.some((f) => /atomic/i.test(f))) jobs.push(probeAtomic(url).then((r) => (e.atomic = r)));
      }
      if (e.p2p) jobs.push(tcp(e.p2p).then((r) => (e.p2p_probe = r)));
      await Promise.all(jobs);
      found.push(e);
    }
  } catch (err) { console.error(`infra ${n.id} ${p.owner}:`, err?.stack || err); found.push({ producer: p.owner, rank: p.rank, survey_error: redact(String(err?.message || err), 120) }); } });
  // Endpoints apps and wallets point at that no active producer's bp.json lists (e.g. wallet defaults run by
  // non-producers). Probed like any other node, kept only when they serve THIS network's chain.
  const host = (u) => { try { return new URL(u).host.toLowerCase(); } catch { return ''; } };
  const listed = new Set(found.filter((x) => x.url).map((x) => endpointId(x.url)));
  await pool((cfg.known_endpoints || []).filter((k) => safeUrl(k.url) && !listed.has(endpointId(k.url))), 6, async (k) => {
    const url = safeUrl(k.url);
    const api = await probeApi(url, n.chain_id).catch(() => null);
    if (!api?.ok || api.chain_ok !== true) return;
    const e = { producer: null, unregistered: true, operator: k.operator || host(url), note: k.note || null, sources: k.sources || [],
      rank: null, types: ['api'], features: [], url, id: endpointId(url), ref: endpointRef(url), p2p: null, location: null, api };
    const hy = await probeHyperion(url, c.head).catch(() => null); if (hy?.ok) e.hyperion = hy;
    found.push(e);
  });
  found.sort((a, b) => (a.unregistered ? 1 : 0) - (b.unregistered ? 1 : 0) || (a.rank || 999) - (b.rank || 999));
  infra[n.id] = { ts: Date.now(), running: false, head: c.head, nodes: found };
  console.log(`infra ${n.id}: ${found.length} nodes surveyed`);
  } catch (err) { console.error(`infra ${n.id} failed:`, err?.stack || err); }
  finally { if (infra[n.id]) infra[n.id].running = false; }
}
if (!OFFLINE) {
  setInterval(() => cfg.networks.forEach((n) => { if (!n.static_producers) surveyInfra(n).catch(() => {}); }), INFRA_EVERY_MS);
  setTimeout(() => cfg.networks.forEach((n) => { if (!n.static_producers) surveyInfra(n).catch((e) => console.error('infra', e)); }), +(process.env.INFRA_FIRST_MS || 45000));
}

// ---- servers (beacon reports) --------------------------------------------------------------------------
// Storage key = token hash, plus the beacon's instance_id when it sends one (one token = one server; a token
// reporting from a second machine is kept as a separate, conflict-flagged entry — never silently replaced).
// The caller-chosen `node` label is display only; equal labels are shown as "api", "api (2)". Each entry also
// gets a public `sid` (one-way hash of its key) so links/APIs address the exact server, not a label.
const sidOf = (key) => sha(`sid:${key}`).slice(0, 16);
function servers(netId, producer) {
  const byKey = nodes[netId]?.[producer]; if (!byKey) return [];
  const list = Object.entries(byKey).map(([key, v]) => ({ key, sid: sidOf(key), ...v })).sort((a, b) => a.first_seen - b.first_seen || a.key.localeCompare(b.key));
  // Display labels are unique per producer, even when beacons choose colliding names ("api", "api", "api (2)").
  const taken = new Set();
  for (const s of list) { const base = s.report.node; let cand = base, n = 2; while (taken.has(cand)) cand = `${base} (${n++})`; taken.add(cand); s.label = cand; }
  return list;
}
// Freshness is judged by the report's OWN timestamp (not when we received it), so a stale report replayed
// within the acceptance window can't look fresh.
const reportAge = (s, now = Date.now()) => Math.max(0, now - (Date.parse(s.report.ts) || s.received));
const isSilent = (s, now = Date.now()) => reportAge(s, now) > silentAfterMs(s.report);
// Preparation stages per server (display only; none of these is authorization to cut).
const STAGE_CHECKS = {
  metal: ['validator_running', 'metal_synced', 'metal_reachable'],
  beacon: ['source_api', 'chain_id', 'producer_api', 'disk_free', 'staged_snapshot_absent'],
  ceremony: ['hook_on_freeze', 'hook_post_ignite', 'hook_on_live', 'hook_on_abort', 'freeze_height', 'freeze_lead_blocks'],
};
function stages(s, now = Date.now()) {
  const byName = dict(); for (const c of s.report.checks) byName[c.name] = c;
  const judge = (names) => { const hit = names.filter((n) => byName[n]); if (!hit.length) return { ok: null, detail: 'not reported' };
    const bad = hit.filter((n) => !byName[n].ok); return { ok: !bad.length, detail: bad.length ? `${bad.length} of ${hit.length} checks failing` : `${hit.length} checks ok` }; };
  const silent = isSilent(s, now);
  const beacon = judge(STAGE_CHECKS.beacon);
  const ceremony = s.report.profile === 'readiness' ? { ok: false, detail: 'readiness profile: no ceremony configured on this server' } : judge(STAGE_CHECKS.ceremony);
  return {
    metal_prepared: judge(STAGE_CHECKS.metal),
    beacon_healthy: silent ? { ok: false, detail: 'beacon silent' } : beacon,
    ceremony_configured: ceremony,
    admitted: { ok: null, detail: 'unknown: validator admission is not tracked yet' },
    authorized: { ok: null, detail: 'none: no COMMIT certificate exists (authority boundary not implemented)' },
    prepared: !silent && s.report.checks.every((c) => c.ok),
  };
}

// ---- coordination: relay of SIGNED coordinator messages (event / arm / abort), persisted ----------------------
// The server checks signatures against the network's configured coordinator keys so it can't be spammed,
// but agents verify again with keys from their OWN config: this relay cannot forge anything. Readiness numbers
// shown next to it are display data, not signed authorization.
// The relay is an ACCEPTANCE BOUNDARY: a message counts as relayed only once it is on disk. Every change is made
// on a copy, persisted atomically (write tmp + fsync + rename), and only then swapped into memory; a failed write
// answers 503 and changes nothing. A corrupt store stops startup (never "start empty" and forget an event).
// `used` = every event id ever published → sha256 of its payload: ids are never reusable.
let coord = dict(); // network id → { event, arm, abort, history: [{type, event_id, at}], used: {event_id: payloadSha} }
const isSignedMsg = (m) => m && typeof m === 'object' && !Array.isArray(m) && typeof m.payload === 'string' && typeof m.key === 'string' && typeof m.sig === 'string';
/** Validate one network's stored coordination entry; returns a normalized copy or throws. */
function validCoordEntry(k, v) {
  if (!v || typeof v !== 'object' || Array.isArray(v)) throw new Error(`${k}: not an object`);
  const out = dict();
  for (const f of ['event', 'arm', 'abort']) {
    if (v[f] === undefined) continue;
    if (!isSignedMsg(v[f])) throw new Error(`${k}.${f}: not a signed message`);
    let pl; try { pl = JSON.parse(v[f].payload); } catch { throw new Error(`${k}.${f}: payload is not JSON`); }
    if (!pl || typeof pl.event_id !== 'string') throw new Error(`${k}.${f}: payload has no event_id`);
    out[f] = { payload: v[f].payload, key: v[f].key, sig: v[f].sig };
  }
  if ((out.arm || out.abort) && !out.event) throw new Error(`${k}: arm/abort without an event`);
  if (v.history !== undefined && !Array.isArray(v.history)) throw new Error(`${k}.history: not an array`);
  out.history = (v.history || []).map((h, i) => { if (!h || typeof h !== 'object' || typeof h.event_id !== 'string') throw new Error(`${k}.history[${i}]: invalid`); return h; });
  if (v.used !== undefined && (!v.used || typeof v.used !== 'object' || Array.isArray(v.used))) throw new Error(`${k}.used: not an object`);
  out.used = dict();
  for (const [id, h] of Object.entries(v.used || {})) { if (typeof h !== 'string' || reservedKey(id)) throw new Error(`${k}.used.${id}: invalid`); out.used[id] = h; }
  // rc.5 stores had no `used` map: every event they mention (active, aborted or only in history) is used, so an
  // id can never be republished after the upgrade. The hash is the stored payload's when we have it.
  if (out.event) { const id = JSON.parse(out.event.payload).event_id; if (!own(out.used, id)) out.used[id] = sha(out.event.payload); }
  for (const h of out.history) if (h.type === 'event' && !own(out.used, h.event_id)) out.used[h.event_id] = h.event_hash || 'legacy:unknown-payload';
  return out;
}
if (existsSync(COORD_FILE)) {
  try { const j = JSON.parse(readFileSync(COORD_FILE, 'utf8')); if (!j || typeof j !== 'object' || Array.isArray(j)) throw new Error('not an object');
    for (const [k, v] of Object.entries(j)) { if (!RE.net.test(k)) throw new Error(`unknown key ${JSON.stringify(k).slice(0, 40)}`); coord[k] = validCoordEntry(k, v); } }
  catch (e) { console.error(`coord: ${COORD_FILE} is unreadable or corrupt (${e.message}). Refusing to start: fix or restore it (a backup, or delete it ONLY if no event is live).`); process.exit(3); }
}
function atomicWrite(file, obj) {
  mkdirSync(dirname(file), { recursive: true });
  const tmp = `${file}.tmp-${process.pid}`;
  const fd = openSync(tmp, 'w'); try { writeSync(fd, JSON.stringify(obj, null, 1)); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(tmp, file);
  // the rename itself is only durable once the directory entry is: fsync the directory too
  const dfd = openSync(dirname(file), 'r');
  try { fsyncSync(dfd); } catch (e) { if (!['EINVAL', 'ENOTSUP', 'EISDIR'].includes(e.code)) throw e; } finally { closeSync(dfd); }
}
/** Persist `next` as the whole coordination state; returns true only if it is durably on disk. */
function commitCoord(next) {
  try { atomicWrite(COORD_FILE, next); coord = next; return true; }
  catch (e) { console.error(`coord: persist failed (${e.message}); change NOT accepted`); return false; }
}

// Server state = the replay watermark (last accepted report timestamp per server key) + every server entry
// (latest report, first_seen, instance_id, conflict). It is written synchronously (tmp + fsync + rename + dir
// fsync) BEFORE a report or an operator change is acknowledged, so a crash right after a 200 can neither replay a
// report nor forget an identity conflict. Only the per-server chart history is memory-only.
// A corrupt state file stops startup (same rule as the coordination store).
const lastTs = dict();
const KEY_RE = /^[0-9a-f]{64}(:[0-9a-f]{32})?$/;
function loadServerState() {
  if (existsSync(STATE_FILE)) {
    let j; try { j = JSON.parse(readFileSync(STATE_FILE, 'utf8')); } catch (e) { throw new Error(`not JSON (${e.message})`); }
    if (!j || typeof j !== 'object' || j.v !== 1 || typeof j.lastTs !== 'object' || typeof j.nodes !== 'object' || !j.lastTs || !j.nodes) throw new Error('bad shape');
    for (const [k, v] of Object.entries(j.lastTs)) { if (!KEY_RE.test(k) || !Number.isFinite(v)) throw new Error(`lastTs.${k.slice(0, 20)}`); lastTs[k] = v; }
    for (const [netId, prods] of Object.entries(j.nodes)) {
      if (!RE.net.test(netId) || !prods || typeof prods !== 'object') throw new Error(`nodes.${netId}`);
      for (const [prod, byKey] of Object.entries(prods)) {
        if (!RE.producer.test(prod) || !byKey || typeof byKey !== 'object') throw new Error(`nodes.${netId}.${prod}`);
        for (const [key, e] of Object.entries(byKey)) {
          if (!KEY_RE.test(key) || !e || typeof e !== 'object' || !Number.isFinite(e.first_seen) || !Number.isFinite(e.received) || typeof e.conflict !== 'boolean'
            || !(e.instance_id === null || /^[0-9a-f]{32}$/.test(e.instance_id))) throw new Error(`nodes.${netId}.${prod}.${key.slice(0, 12)}`);
          let r; try { r = projectReport(e.report); } catch (x) { throw new Error(`nodes.${netId}.${prod}.${key.slice(0, 12)}.report: ${x.message}`); }
          ((nodes[netId] ||= dict())[prod] ||= dict())[key] = { report: r, received: e.received, hist: [], first_seen: e.first_seen, instance_id: e.instance_id, conflict: e.conflict };
        }
      }
    }
    return;
  }
  // rc.6 kept only a replay watermark (written ~1 s after acknowledging): carry it over.
  if (existsSync(REPLAY_FILE)) {
    try { const j = JSON.parse(readFileSync(REPLAY_FILE, 'utf8')); for (const [k, v] of Object.entries(j)) if (KEY_RE.test(k) && Number.isFinite(v)) lastTs[k] = v; }
    catch (e) { console.error(`replay: ${REPLAY_FILE} unreadable (${e.message}); starting with a fresh replay window (reports older than ${TS_PAST_MS / 60e3} min are still refused)`); }
  }
}
try { loadServerState(); }
catch (e) { console.error(`servers: ${STATE_FILE} is unreadable or corrupt (${e.message}). Refusing to start: fix or restore it (deleting it forgets replay protection and identity conflicts).`); process.exit(3); }
/** The whole server state with `override` = [netId, producer, byKey] swapped in (not yet live), as a plain object. */
function serverSnapshot(override, lastOverride) {
  const out = { v: 1, lastTs: { ...lastTs, ...(lastOverride || {}) }, nodes: {} };
  const put = (netId, prod, byKey) => { const o = ((out.nodes[netId] ||= {})[prod] = {});
    for (const [k, e] of Object.entries(byKey)) o[k] = { report: e.report, received: e.received, first_seen: e.first_seen, instance_id: e.instance_id ?? null, conflict: !!e.conflict }; };
  for (const [netId, prods] of Object.entries(nodes)) for (const [prod, byKey] of Object.entries(prods)) {
    if (override && override[0] === netId && override[1] === prod) continue; put(netId, prod, byKey); }
  if (override) put(override[0], override[1], override[2]);
  return out;
}
/** Durably persist the state with the proposed change; true only if it is on disk. Nothing live changes here. */
function commitServers(override, lastOverride) {
  try { atomicWrite(STATE_FILE, serverSnapshot(override, lastOverride)); return true; }
  catch (e) { console.error(`servers: persist failed (${e.message}); change NOT accepted`); return false; }
}
/** One token = one machine: every entry of a token is in conflict while the token has more than one entry. */
function recomputeConflicts(byKey, tok) {
  const mine = Object.keys(byKey).filter((k) => k.split(':')[0] === tok);
  for (const k of mine) byKey[k] = { ...byKey[k], conflict: mine.length > 1 };
}
function verifySigned(msg, keys) {
  try {
    if (!msg || typeof msg.key !== 'string' || typeof msg.payload !== 'string' || typeof msg.sig !== 'string') return null;
    if (!/^[0-9a-f]{64}$/i.test(msg.key) || !/^[0-9a-f]{128}$/i.test(msg.sig)) return null;
    if (!keys?.map((k) => k.toLowerCase()).includes(msg.key.toLowerCase())) return null;
    const pub = createPublicKey({ key: Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), Buffer.from(msg.key, 'hex')]), format: 'der', type: 'spki' });
    if (!edVerify(null, Buffer.from(msg.payload), pub, Buffer.from(msg.sig, 'hex'))) return null;
    const p = JSON.parse(msg.payload);
    return p && typeof p === 'object' && typeof p.event_id === 'string' && /^[\w.:-]{1,64}$/.test(p.event_id) ? p : null;
  } catch { return null; }
}
function coordView(n) {
  const c = coord[n.id] || {};
  const ev = c.event ? JSON.parse(c.event.payload) : null;
  if (!ev) return null;
  const reps = Object.keys(nodes[n.id] || {}).flatMap((p) => servers(n.id, p).filter((s) => (s.report.role || 'producer') === 'producer').map((s) => [p, s]));
  const mine = (s) => s.report.coord?.event_id === ev.event_id;
  return { event: ev, armed: !!c.arm, aborted: !!c.abort,
    arm_at: c.arm ? JSON.parse(c.arm.payload).issued_at_ms : null,
    accepted: [...new Set(reps.filter(([, s]) => mine(s) && s.report.coord.accepted).map(([p]) => p))],
    rejected: reps.filter(([, s]) => mine(s) && s.report.coord.accepted === false).map(([p, s]) => ({ producer: p, reason: s.report.coord.reason })),
    history: (c.history || []).slice(-20) };
}

// ---- agreement (atomicity A1/A3 across producers) ------------------------------------------------
// Roster-based: the denominator is the producer roster (active schedule, else configured/registered set), not
// "whoever happens to report". Stale reports are excluded and named; roster members with no value are "missing".
const EVIDENCE_KEYS = [
  ['h', 'declared H'], ['cut_block_id', 'cut block id'], ['snapshot_sha256', 'snapshot sha256'],
  ['fingerprints_digest', 'table fingerprints'], ['burnoff_transactions', 'transactions after the cut'],
  ['state_digest', 'state digest (old@H = new@H)'], ['target_head_id', 'new chain anchor at H'],
];
function roster(n) {
  const c = chain[n.id] || {};
  if (c.schedule?.length) return [...c.schedule];
  if (n.geo) return Object.keys(n.geo);
  return Object.keys(registry[n.id]?.producers || {});
}
function agreement(n) {
  const rows = []; const now = Date.now();
  const members = roster(n);
  const all = [...new Set([...members, ...Object.keys(nodes[n.id] || {})])];
  for (const [key, label] of EVIDENCE_KEYS) {
    const vals = [], stale = [], conflicted = [], unhealthy = [];
    for (const p of all) {
      for (const s of servers(n.id, p).filter((x) => (x.report.role || 'producer') === 'producer')) {
        const v = s.report.ceremony?.evidence?.[key];
        if (v === undefined || v === null) continue;
        const who = servers(n.id, p).length > 1 ? `${p}/${s.label}` : p;
        // Evidence from a server whose identity is in conflict never counts toward agreement.
        // A report with a failing HEALTH check is excluded exactly as the agent's fleet gate excludes it
        // (setup checks, e.g. hooks, do not count): the dashboard must not show "agree" where the gate won't.
        if (s.conflict) conflicted.push(who); else if (isSilent(s, now)) stale.push(who);
        else if (hasFailingHealth(s.report)) unhealthy.push(who); else vals.push([who, p, v]);
      }
    }
    if (!vals.length && !stale.length && !conflicted.length && !unhealthy.length) continue;
    const groups = {};
    for (const [who, , v] of vals) (groups[JSON.stringify(v)] ||= []).push(who);
    const distinct = Object.keys(groups);
    const have = new Set(vals.map(([, p]) => p));
    // Absent values are MISSING, never silently dropped: roster members without the value, plus (with no roster)
    // every producer-role reporter that lacks it.
    const reporters = [...new Set(all.filter((p) => servers(n.id, p).some((x) => (x.report.role || 'producer') === 'producer')))];
    const missing = (members.length ? members : reporters).filter((p) => !have.has(p));
    const expectBad = key === 'burnoff_transactions' ? vals.some(([, , v]) => v !== 0) : false;
    // No roster = no agreement verdict: "everyone who happened to report agrees" is not agreement.
    const agree = !members.length ? null : distinct.length === 1 && !missing.length && !stale.length && !conflicted.length && !unhealthy.length && !expectBad;
    rows.push({ key, label, agree, verdict: !members.length ? 'no roster' : agree ? 'agree' : 'disagree', reporting: vals.length, roster: members.length,
      missing, stale, conflicted, unhealthy, bad_value: expectBad, values: distinct.map((v) => ({ value: JSON.parse(v), producers: groups[v] })) });
  }
  return rows;
}

function status() {
  const now = Date.now();
  return {
    generated_at: new Date().toISOString(),
    networks: cfg.networks.map((n) => {
      const c = chain[n.id] || {};
      const reported = nodes[n.id] || {};
      const reg = registry[n.id]?.producers || {};
      const names = [...new Set([...Object.keys(reg), ...(c.schedule || []), ...Object.keys(reported)])];
      const geo = n.geo || {};
      let serversAll = 0, serversLive = 0, serversReady = 0, serversConflict = 0; const states = {};
      const producers = names.map((name) => {
        const beacons = servers(n.id, name).map((s) => ({ node: s.label, sid: s.sid, role: s.report.role || null, age_ms: reportAge(s, now),
          silent: isSilent(s, now), conflict: !!s.conflict, stages: stages(s, now), report: s.report }))
          .sort((x, y) => (x.role === 'producer' ? -1 : 0) - (y.role === 'producer' ? -1 : 0));
        for (const b of beacons) {
          serversAll++; if (b.conflict) serversConflict++;
          if (!b.silent) { serversLive++; if (b.report.ready && !b.conflict) serversReady++; const st = b.report.ceremony?.state || 'IDLE'; states[st] = (states[st] || 0) + 1; }
        }
        const r = beacons.length ? { report: beacons[0].report, received: now - beacons[0].age_ms } : null;
        const age = r ? now - r.received : null;
        const g = reg[name] || {};
        const org = g.org || (geo[name] ? { name: geo[name].name || name, city: geo[name].city, country: geo[name].country, lat: geo[name].lat, lon: geo[name].lon } : { name });
        const liveB = beacons.filter((b) => !b.silent);
        return { name, org: { ...org, logo: org.logo ? `/api/logo/${encodeURIComponent(n.id)}/${encodeURIComponent(name)}` : null },
          rank: g.rank || null, votes: g.votes || null, active: !!reg[name] || !!geo[name],
          scheduled: (c.schedule || []).includes(name), reporting: liveB.length > 0,
          // "prepared" (API field kept as `ready` for compatibility): every one of its servers reports, all checks
          // green, no identity conflict. Preparation only: not admission, not authorization to cut.
          ready: beacons.length > 0 && liveB.length === beacons.length && liveB.every((b) => b.report.ready) && !beacons.some((b) => b.conflict),
          silent: r ? beacons.some((b) => b.silent) : null, age_ms: age, report: r?.report || null, beacons };
      }).sort((a, b) => (b.scheduled - a.scheduled) || ((a.rank || 999) - (b.rank || 999)) || a.name.localeCompare(b.name));
      return { id: n.id, name: n.name, label: n.label, priority: n.priority ?? 9, description: n.description || '',
        expected_chain_id: n.chain_id, metal: n.metal || null, event: n.event || null, chain: { ...c, schedule: undefined }, schedule: c.schedule || [],
        summary: { producers: producers.length, active: producers.filter((p) => p.active).length, scheduled: (c.schedule || []).length,
          roster: roster(n).length,
          reporting: producers.filter((p) => p.reporting).length, ready: producers.filter((p) => p.ready).length,
          servers: serversAll, servers_reporting: serversLive, servers_ready: serversReady, servers_prepared: serversReady, servers_silent: serversAll - serversLive, servers_conflict: serversConflict, states },
        producers, agreement: agreement(n), coordination: coordView(n), events: (events[n.id] || []).slice(0, 40) };
    }).sort((a, b) => a.priority - b.priority),
  };
}

// ---- http ----------------------------------------------------------------------------------------
const send = (res, code, body, type = 'application/json') => {
  res.writeHead(code, { 'content-type': type, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' });
  res.end(typeof body === 'string' ? body : JSON.stringify(body ?? {}));
};
const page = () => readFileSync(join(HERE, 'public', 'index.html'), 'utf8');
const NOT_FOUND_HTML = '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Not found</title>'
  + '<body style="background:#070a16;color:#c9cdf0;font:15px system-ui,sans-serif;display:grid;place-items:center;min-height:100vh;margin:0">'
  + '<div style="text-align:center"><h1 style="font-weight:600">Page not found</h1><p><a href="/" style="color:#f5a524">Back to mission control</a></p></div>';

async function readBody(req, max) {
  let raw = '';
  for await (const chunk of req) { raw += chunk; if (raw.length > max) return null; }
  return raw;
}

// per-token token bucket: burst 5, one report per 3 s sustained
const buckets = new Map();
function allow(tok) {
  const now = Date.now(); const b = buckets.get(tok) || { t: now, n: 5 };
  b.n = Math.min(5, b.n + (now - b.t) / 3000); b.t = now;
  if (b.n < 1) { buckets.set(tok, b); return false; }
  b.n -= 1; buckets.set(tok, b); return true;
}

const reachSeen = new Map(); let reachInflight = 0;
const exposureSeen = new Map(); let exposureInflight = 0;
/** The requesting server's address: X-Real-IP only from the local reverse proxy (which overwrites it). */
function callerIp(req) {
  const sock = normIp(req.socket.remoteAddress);
  const local = sock === '127.0.0.1' || sock === '::1';
  return normIp(local && req.headers['x-real-ip'] ? String(req.headers['x-real-ip']).trim() : sock);
}

async function handle(req, res) {
  let url; try { url = new URL(req.url, 'http://x'); } catch { return send(res, 400, { error: 'bad url' }); }
  const path = url.pathname;
  if (req.method === 'GET' && path === '/api/status') return send(res, 200, status());
  if (req.method === 'GET' && path === '/healthz') return send(res, 200, { ok: true });
  if (req.method === 'GET' && (path === '/favicon.ico' || path === '/favicon.svg')) {
    res.writeHead(200, { 'content-type': 'image/svg+xml', 'cache-control': 'public, max-age=86400' });
    return res.end('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#f5a524"/><stop offset="1" stop-color="#8b5cf6"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="url(#g)"/><rect x="14" y="14" width="36" height="36" rx="9" fill="#070a16"/><rect x="31" y="12" width="3" height="40" rx="1.5" fill="url(#g)"/></svg>');
  }
  const im = path.match(/^\/api\/infra\/([a-z0-9-]{1,32})$/);
  if (im && req.method === 'GET') {
    const x = infra[im[1]]; const reg = registry[im[1]]?.producers || {};
    if (!x) return send(res, 200, { pending: true });
    return send(res, 200, { ts: x.ts, head: x.head, nodes: x.nodes.map((nd) => ({ ...nd, org: reg[nd.producer]?.org ? { name: reg[nd.producer].org.name, country: reg[nd.producer].org.country, logo: reg[nd.producer].org.logo ? `/api/logo/${im[1]}/${nd.producer}` : null } : null })) });
  }
  if (req.method === 'GET' && path.startsWith('/api/logo/')) {
    const parts = path.split('/').slice(3).map(safeDecode);
    if (parts.length !== 2 || parts.some((s) => s === null) || !RE.net.test(parts[0]) || !RE.producer.test(parts[1])) return send(res, 400, { error: 'bad logo path' });
    const [netId, owner] = parts;
    const l = await logo(netId, owner);
    if (!l) {
      const name = registry[netId]?.producers?.[owner]?.org?.name || owner || '?';
      const ini = String(name).replace(/[^A-Za-z0-9 ]/g, '').split(/\s+/).filter(Boolean).slice(0, 2).map((w) => w[0]).join('').toUpperCase() || '?';
      res.writeHead(200, { 'content-type': 'image/svg+xml', 'cache-control': 'public, max-age=3600' });
      return res.end(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="16" fill="#141b38"/><text x="32" y="40" text-anchor="middle" font-family="Helvetica,Arial,sans-serif" font-size="22" font-weight="700" fill="#c9cdf0">${ini}</text></svg>`);
    }
    res.writeHead(200, { 'content-type': l.type, 'cache-control': 'public, max-age=21600', 'x-content-type-options': 'nosniff',
      'content-security-policy': "default-src 'none'; style-src 'unsafe-inline'; sandbox" });
    return res.end(l.buf);
  }
  if (req.method === 'GET' && path === '/api/reach') {
    // Can the internet reach the CALLER's Metal staking port? Only ever dials the requesting IP (X-Real-IP is
    // trusted only from the local reverse proxy, which overwrites it), only a public IP literal, only port 9651,
    // at most once per 5 s per IP and 20 probes in flight overall.
    const ip = callerIp(req);
    if (!isPublicIp(ip)) return send(res, 400, { error: 'caller address is not a public IP', ip: net.isIP(ip) ? ip : null });
    const now = Date.now();
    if (reachSeen.size > 10000) reachSeen.clear();
    reachSeen.forEach((t, k) => { if (now - t > 60000) reachSeen.delete(k); });
    if (now - (reachSeen.get(ip) || 0) < 5000) return send(res, 429, { error: 'slow down' });
    if (reachInflight >= 20) return send(res, 503, { error: 'busy, retry shortly' });
    reachSeen.set(ip, now); reachInflight++;
    const t0 = Date.now();
    try {
      const ok = await new Promise((done) => { const sk = net.connect({ host: ip, port: 9651, timeout: 4000 });
        sk.once('connect', () => { sk.destroy(); done(true); }); sk.once('timeout', () => { sk.destroy(); done(false); }); sk.once('error', () => done(false)); });
      return send(res, 200, { ip, port: 9651, reachable: ok, ms: Date.now() - t0 });
    } finally { reachInflight--; }
  }
  if (req.method === 'GET' && path === '/api/exposure') {
    // Does the CALLER's producer API (/v1/producer/*) answer from the internet? Dials only the requesting IP:
    // its :8888 and :80, plus that producer's bp.json endpoints whose hostname resolves to the same IP
    // (connection pinned, no redirects). Read-only probe (/v1/producer/paused). Once per 30 s per IP.
    const ip = callerIp(req);
    if (!isPublicIp(ip)) return send(res, 400, { error: 'caller address is not a public IP', ip: net.isIP(ip) ? ip : null });
    const netId = url.searchParams.get('net') || ''; const prod = url.searchParams.get('producer') || '';
    if ((netId && !RE.net.test(netId)) || (prod && !RE.producer.test(prod))) return send(res, 400, { error: 'bad net or producer' });
    const now = Date.now();
    if (exposureSeen.size > 10000) exposureSeen.clear();
    exposureSeen.forEach((t, k) => { if (now - t > 120000) exposureSeen.delete(k); });
    if (now - (exposureSeen.get(ip) || 0) < 30000) return send(res, 429, { error: 'slow down' });
    if (exposureInflight >= 10) return send(res, 503, { error: 'busy, retry shortly' });
    exposureSeen.set(ip, now); exposureInflight++;
    try {
      const urls = netId && prod ? (infra[netId]?.nodes || []).filter((n) => n.producer === prod).flatMap((n) => n.urls || [n.url]).filter(Boolean) : [];
      const r = await probeProducerApi(ip, urls.slice(0, 12));
      return send(res, 200, { ip, producer_api: r });
    } finally { exposureInflight--; }
  }
  const nm = path.match(/^\/api\/node\/([^/]+)\/([^/]+)\/([^/]+)$/);
  if (nm && req.method === 'GET') {
    // One server's latest (public projection of its) beacon report + short history, for its page and for agents.
    // The last segment is the server's `sid` (exact) or its display label.
    const [netId, prod, nd] = nm.slice(1).map(safeDecode);
    if ([netId, prod, nd].some((s) => s === null) || !RE.net.test(netId) || !RE.producer.test(prod) || !(RE.label.test(nd) || RE.sid.test(nd))) return send(res, 400, { error: 'bad node path' });
    const list = servers(netId, prod);
    const s = list.find((x) => x.sid === nd) || list.find((x) => x.label === nd);
    if (!s) return send(res, 404, { error: 'no such server' });
    return send(res, 200, { network: netId, producer: prod, node: s.label, sid: s.sid, age_ms: reportAge(s), silent: isSilent(s), conflict: !!s.conflict,
      stages: stages(s), report: s.report, history: s.hist || [] });
  }
  const mm = path.match(/^\/api\/manifest(?:\/([a-z0-9-]{1,32}))?$/);
  if (mm && req.method === 'GET') {
    // Network manifest: which Metal network, subnet, blockchain and VM each XPR network maps to, plus pinned
    // metalgo/plugin versions and checksums. metal-install.sh reads this, so nobody copies IDs by hand.
    // It is preparation metadata only: not signed, not bound to an event, not a release authorization.
    const one = (n) => { const { status: summary, ...m } = n.metal || {}; return { status: 'preparation metadata, not a release authorization', xpr_network: n.id, xpr_chain_id: n.chain_id, ...m, ...(summary ? { summary } : {}) }; };
    if (!mm[1]) return send(res, 200, { status: 'preparation metadata, not a release authorization', updated: cfg.manifest_updated || null, networks: cfg.networks.map(one) });
    const n = cfg.networks.find((x) => x.id === mm[1]);
    return n ? send(res, 200, one(n)) : send(res, 404, { error: 'unknown network' });
  }
  const cm = path.match(/^\/api\/coord\/([a-z0-9-]{1,32})$/);
  if (cm && req.method === 'GET') { const c = own(coord, cm[1]) ? coord[cm[1]] : null; return send(res, 200, c ? { event: c.event || null, arm: c.arm || null, abort: c.abort || null } : {}); }
  if (cm && req.method === 'POST') {
    const n = cfg.networks.find((x) => x.id === cm[1]); if (!n) return send(res, 404, { error: 'unknown network' });
    const raw = await readBody(req, 16384); if (raw === null) return send(res, 413, { error: 'too large' });
    let msg; try { msg = JSON.parse(raw); } catch { return send(res, 400, { error: 'bad json' }); }
    const p = verifySigned(msg, n.coordinators);
    if (!p || p.network !== n.id || !['event', 'arm', 'abort'].includes(p.type)) return send(res, 403, { error: 'not a valid signed coordinator message for this network' });
    const next = clone(coord);
    if (reservedKey(p.event_id)) return send(res, 400, { error: 'reserved event id' });
    const c = own(next, n.id) ? next[n.id] : (next[n.id] = Object.assign(dict(), { history: [], used: dict() })); c.history ||= []; c.used ||= dict();
    const cur = c.event ? JSON.parse(c.event.payload) : null;
    const at = new Date().toISOString();
    let note;
    if (p.type === 'event') {
      if (n.chain_id && p.chain_id !== n.chain_id) return send(res, 409, { error: 'event chain_id does not match this network' });
      const h = sha(msg.payload);
      if (cur && !c.abort) {
        if (cur.event_id !== p.event_id) return send(res, 409, { error: `event ${cur.event_id} is active; abort it before publishing another` });
        if (c.event.payload !== msg.payload) return send(res, 409, { error: 'a different payload for this event_id is already published' });
        return send(res, 200, { ok: true, type: 'event', event_id: p.event_id, event_hash: h, unchanged: true });
      }
      // Event ids are never reusable, not even after an abort: a new attempt needs a new id.
      if (own(c.used, p.event_id)) return send(res, 409, { error: c.used[p.event_id] === h ? `event ${p.event_id} was already used (aborted or superseded); publish a new event id` : `event id ${p.event_id} was already used with a different payload` });
      next[n.id] = Object.assign(dict(), { event: msg, history: [...c.history, { type: 'event', event_id: p.event_id, event_hash: h, at }].slice(-100), used: Object.assign(dict(), c.used, { [p.event_id]: h }) });
      note = `published event ${p.event_id}: cut at H = ${Number(p.h) || '?'}`;
    } else {
      if (!cur || cur.event_id !== p.event_id) return send(res, 409, { error: 'no such event' });
      // arm (required) and abort (optional, but checked when present) must name the exact event payload they refer
      // to, so a message signed for one version of an event can never act on another.
      const evHash = sha(c.event.payload);
      if (p.type === 'arm' && p.event_hash !== evHash) return send(res, 409, { error: 'arm must carry event_hash = sha256 of the published event payload (update control/coord.mjs)', event_hash: evHash });
      if (p.type === 'abort' && p.event_hash != null && p.event_hash !== evHash) return send(res, 409, { error: 'abort event_hash does not match the published event', event_hash: evHash });
      if (c.abort) return send(res, 409, { error: 'event already aborted' });
      if (own(c, p.type) && c[p.type]) return send(res, 200, { ok: true, type: p.type, event_id: p.event_id, unchanged: true });
      c[p.type] = msg; c.history.push({ type: p.type, event_id: p.event_id, at });
      note = p.type === 'arm' ? `ARMED event ${p.event_id}` : `ABORTED event ${p.event_id}`;
    }
    if (!commitCoord(next)) return send(res, 503, { error: 'could not persist the message; nothing was relayed, retry' });
    pushEvent(n.id, 'coordinator', note);
    return send(res, 200, { ok: true, type: p.type, event_id: p.event_id, ...(p.type === 'event' ? { event_hash: sha(msg.payload) } : {}) });
  }
  if (req.method === 'POST' && path === '/api/admin/clear-server') {
    // Operator-only, from the box itself: the reverse proxy always sets X-Real-IP, so a request that arrives on
    // loopback WITHOUT it came from a local shell (curl on the mission-control host), never from the internet.
    const sock = normIp(req.socket.remoteAddress);
    if (!(sock === '127.0.0.1' || sock === '::1') || req.headers['x-real-ip'] || req.headers['x-forwarded-for']) return send(res, 403, { error: 'local operator only' });
    const netId = url.searchParams.get('net'), prod = url.searchParams.get('producer'), sid = url.searchParams.get('sid');
    if (!RE.net.test(netId || '') || !RE.producer.test(prod || '') || !RE.sid.test(sid || '')) return send(res, 400, { error: 'need ?net=&producer=&sid=' });
    const byKey = nodes[netId]?.[prod]; const hit = byKey && Object.keys(byKey).find((k) => sidOf(k) === sid);
    if (!hit) return send(res, 404, { error: 'no such server' });
    const tok = hit.split(':')[0];
    // Remove ONLY the selected instance; the rest of that token stay in conflict while more than one remains.
    const next = dict(); for (const [k, v] of Object.entries(byKey)) if (k !== hit) next[k] = v;
    recomputeConflicts(next, tok);
    if (!commitServers([netId, prod, next])) return send(res, 503, { error: 'could not persist; nothing changed' });
    nodes[netId][prod] = next;
    pushEvent(netId, prod, `operator removed server ${sid}`);
    return send(res, 200, { ok: true, removed: sid });
  }
  if (req.method === 'POST' && path === '/api/report') {
    const tokenHash = sha(String(req.headers.authorization || '').replace(/^Bearer\s+/i, ''));
    const bind = tokens[tokenHash];
    if (!bind) return send(res, 401, { error: 'unknown token' });
    if (!allow(tokenHash)) return send(res, 429, { error: 'too many reports: at most one every 3 s' });
    const raw = await readBody(req, MAX_BODY); if (raw === null) return send(res, 413, { error: 'too large' });
    let body; try { body = JSON.parse(raw); } catch { return send(res, 400, { error: 'bad json' }); }
    let r; try { r = projectReport(body); } catch (e) { if (isBad(e)) return send(res, 400, { error: `invalid report: ${e.message}` }); throw e; }
    if (r.network !== bind.network || r.producer !== bind.producer)
      return send(res, 403, { error: `token is bound to ${bind.producer}@${bind.network}` });
    if (!netIds().includes(r.network)) return send(res, 404, { error: 'unknown network' });
    const ts = Date.parse(r.ts), now = Date.now();
    if (ts > now + TS_FUTURE_MS) return send(res, 400, { error: 'report timestamp is in the future (check the server clock)' });
    if (ts < now - TS_PAST_MS) return send(res, 400, { error: 'report is too old' });
    const live = nodes[r.network]?.[r.producer] || dict();
    const key = r.instance_id ? `${tokenHash}:${r.instance_id}` : tokenHash;
    if (ts <= (lastTs[key] || 0)) return send(res, 409, { error: 'replayed or out-of-order report' });
    // Work on a copy; nothing live changes until the new state is durably on disk.
    const byKey = dict(); for (const [k, v] of Object.entries(live)) byKey[k] = v;
    // Pre-instance_id entry of the same token upgrades in place to the keyed entry (same machine, newer beacon).
    if (r.instance_id && !byKey[key] && byKey[tokenHash]) { byKey[key] = byKey[tokenHash]; delete byKey[tokenHash]; }
    const prevE = byKey[key];
    const prev = prevE?.report;
    // Short in-memory history (about 2 h at a 10 s interval) for the per-server page's charts.
    const hist = (prevE?.hist || []).slice(-719);
    const lagNow = chain[r.network]?.head && r.source?.head ? Math.max(0, chain[r.network].head - r.source.head) : null;
    hist.push({ t: ts, head: r.source?.head ?? null, lag: lagNow, peers: r.metal?.peers ?? null, ok: r.checks.filter((c) => c.ok).length, n: r.checks.length });
    byKey[key] = { report: r, received: now, hist, first_seen: prevE?.first_seen || now, instance_id: r.instance_id || null, conflict: false };
    // One token on two machines shows up as a second instance_id: keep BOTH entries, flag every entry of the token
    // as a conflict, until an operator removes one (POST /api/admin/clear-server) or re-enrolls with separate tokens.
    recomputeConflicts(byKey, tokenHash);
    const conflict = byKey[key].conflict;
    if (!commitServers([r.network, r.producer, byKey], { [key]: ts })) return send(res, 503, { error: 'could not persist the report; not accepted, retry' });
    (nodes[r.network] ||= dict())[r.producer] = byKey;
    lastTs[key] = ts;
    const label = servers(r.network, r.producer).find((s) => s.key === key)?.label || r.node;
    const who = Object.keys(byKey).length > 1 || r.node ? `${r.producer} · ${label}` : r.producer;
    if (conflict && !prevE?.conflict) pushEvent(r.network, who, 'the same beacon token is reporting from two machines (second instance id): both kept and flagged');
    if (prev && prev.node !== r.node) pushEvent(r.network, who, `renamed from ${prev.node}`);
    const was = prev?.ceremony?.state, is = r.ceremony?.state;
    if (!prev) pushEvent(r.network, who, `started reporting (${r.role || 'node'}, agent ${r.agent_version})`);
    if (is && is !== was) pushEvent(r.network, who, `→ ${is}`);
    if (prev && prev.ready !== r.ready) pushEvent(r.network, who, r.ready ? 'READY' : `not ready: ${r.checks.filter((c) => !c.ok).map((c) => c.name).join(', ')}`);
    return send(res, 200, { ok: true, node: label });
  }
  // App routes are client-side: serve the dashboard only for paths in the route grammar, 404 page otherwise.
  if (req.method === 'GET' && !path.startsWith('/api/')) {
    if (isAppRoute(path, netIds())) return send(res, 200, page(), 'text/html; charset=utf-8');
    return send(res, 404, NOT_FOUND_HTML, 'text/html; charset=utf-8');
  }
  send(res, 404, { error: 'not found' });
}

const server = http.createServer((req, res) => {
  handle(req, res).catch((e) => {
    console.error(`request ${req.method} ${String(req.url).slice(0, 200)} failed:`, e?.stack || e);
    if (!res.headersSent) send(res, 500, { error: 'internal error' }); else res.destroy();
  });
});
server.listen(PORT, HOST, () => console.log(`mission control on ${HOST}:${server.address().port} · ${cfg.networks.length} networks · tokens ${Object.keys(tokens).length}`));
