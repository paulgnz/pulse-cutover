#!/usr/bin/env node
// pulse-cutover mission control — a readiness + ceremony status board for several networks.
//
//   NETWORKS=control/networks.json TOKENS=/etc/pulse-control/tokens.json PORT=8787 node control/server.js
//
// - Polls every configured network's public RPC (read-only): head, LIB, head producer, producer schedule.
// - Accepts beacon reports (POST /api/report, `Authorization: Bearer <token>`). The tokens file holds
//   sha256(token) → {network, producer}; a report is accepted only for the network/producer its token is bound to.
// - Serves GET /api/status (JSON) and the dashboard (GET /).
// No dependencies (Node >= 18). Nothing here can change a chain or a node: it only reads and displays.
import http from 'node:http';
import net from 'node:net';
import { createHash, createPublicKey, verify as edVerify } from 'node:crypto';
import { readFileSync, existsSync, watchFile } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const PORT = +(process.env.PORT || 8787);
const NETWORKS_FILE = process.env.NETWORKS || join(HERE, 'networks.json');
const TOKENS_FILE = process.env.TOKENS || '';
const SILENT_AFTER_MS = +(process.env.SILENT_AFTER_MS || 20000);
const MAX_BODY = 256 * 1024;

const sha = (s) => createHash('sha256').update(s).digest('hex');
let cfg = JSON.parse(readFileSync(NETWORKS_FILE, 'utf8'));
let tokens = {};
const loadTokens = () => { if (TOKENS_FILE && existsSync(TOKENS_FILE)) tokens = JSON.parse(readFileSync(TOKENS_FILE, 'utf8')); };
loadTokens();
if (TOKENS_FILE) watchFile(TOKENS_FILE, { interval: 2000 }, loadTokens);
watchFile(NETWORKS_FILE, { interval: 2000 }, () => { try { cfg = JSON.parse(readFileSync(NETWORKS_FILE, 'utf8')); } catch {} });

// ---- state ---------------------------------------------------------------------------------------
const chain = {};    // network id → { head, lib, head_producer, chain_id, head_time, rpc, ok, ts, schedule[] }
const nodes = {};    // network id → producer → { report, received }
const events = {};   // network id → [{ ts, producer, text }]
const pushEvent = (net, producer, text) => {
  (events[net] ||= []).unshift({ ts: new Date().toISOString(), producer, text });
  events[net].length = Math.min(events[net].length, 200);
};

async function rpc(url, path, body = {}) {
  const r = await fetch(`${url}/v1/chain/${path}`, { method: 'POST', body: JSON.stringify(body), signal: AbortSignal.timeout(4000) });
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
          const list = (s.active?.producers || []).map((p) => p.producer_name);
          if (list.length) { c.schedule = list; c.scheduleTs = Date.now(); c.schedule_version = s.active?.version; }
        } catch {}
      }
      return;
    } catch (e) { c.ok = false; c.error = String(e.message || e); }
  }
}
setInterval(() => cfg.networks.forEach((n) => pollNetwork(n).catch(() => {})), 3000);
cfg.networks.forEach((n) => pollNetwork(n).catch(() => {}));

// ---- producer registry: eosio::producers (active) + each producer's chains.json → bp.json -------------
const registry = {}; // network id → { ts, producers: { owner → {owner, votes, rank, url, org{…}} } }
const logos = new Map(); // `${net}/${owner}` → { type, buf, ts }
const UA = 'pulse-cutover-mission-control/1.0 (+https://control-rehearsal.protonnz.com)';
const _fetch = globalThis.fetch;
globalThis.fetch = (url, opts = {}) => _fetch(url, { ...opts, headers: { 'user-agent': UA, ...(opts.headers || {}) } });
const safeUrl = (u) => { try { const x = new URL(u); return /^https?:$/.test(x.protocol) ? x.href.replace(/\/$/, '') : null; } catch { return null; } };
async function getJson(url, ms = 5000) {
  const r = await fetch(url, { signal: AbortSignal.timeout(ms), headers: { accept: 'application/json' } });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}
async function bpJson(base, chainId) {
  // chains.json maps chain_id → path of that chain's bp.json (EOSIO standard); fall back to /bp.json.
  try {
    const cj = await getJson(`${base}/chains.json`);
    const path = cj?.chains?.[chainId];
    if (path) return await getJson(new URL(path, base + '/').href);
  } catch {}
  return getJson(`${base}/bp.json`);
}
async function refreshRegistry(n) {
  const c = chain[n.id]; if (!c?.rpc) return;
  const reg = (registry[n.id] ||= { ts: 0, producers: {} });
  let rows = [];
  try {
    for (let lower = '', i = 0; i < 20; i++) {
      const r = await rpc(c.rpc, 'get_table_rows', { json: true, code: 'eosio', scope: 'eosio', table: 'producers', limit: 500, lower_bound: lower });
      rows.push(...(r.rows || []));
      if (!r.more) break; lower = r.next_key || r.more;
    }
  } catch { return; }
  const active = rows.filter((p) => p.is_active === 1 || p.is_active === true)
    .sort((a, b) => parseFloat(b.total_votes) - parseFloat(a.total_votes));
  const next = {};
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
        p.org = { name: o.candidate_name || p.owner, website: safeUrl(o.website || base), logo,
          country: loc.country || '', city: loc.name || '', lat: Number(loc.latitude), lon: Number(loc.longitude) };
      } catch { p.org = p.org || { name: p.owner, website: base }; }
    }));
  }
}
setInterval(() => cfg.networks.forEach((n) => { if (!n.static_producers) refreshRegistry(n).catch(() => {}); }), 10 * 60e3);
setTimeout(() => cfg.networks.forEach((n) => { if (!n.static_producers) refreshRegistry(n).catch(() => {}); }), 4000);

async function logo(net, owner) {
  const key = `${net}/${owner}`; const hit = logos.get(key);
  if (hit && Date.now() - hit.ts < 6 * 3600e3) return hit;
  const src = registry[net]?.producers?.[owner]?.org?.logo; if (!src) return null;
  try {
    const r = await fetch(src, { signal: AbortSignal.timeout(6000) });
    const type = r.headers.get('content-type') || '';
    if (!r.ok || !/^image\//.test(type)) return null;
    const buf = Buffer.from(await r.arrayBuffer());
    if (buf.length > 512 * 1024) return null;
    const v = { type, buf, ts: Date.now() }; logos.set(key, v); return v;
  } catch { return null; }
}

// ---- infrastructure survey: every node of every active producer (bp.json nodes) ------------------------
// Read-only public probes, bounded concurrency, every INFRA_EVERY_MS: API get_info (version, head, latency, edge),
// Hyperion /v2/health, AtomicAssets /health, and a TCP connect to each advertised p2p endpoint.
const INFRA_EVERY_MS = +(process.env.INFRA_EVERY_MS || 10 * 60e3);
const n_id = (cid) => cfg.networks.find((x) => x.chain_id === cid)?.id;
const infra = {}; // network id → { ts, running, nodes: [...] }
const edgeOf = (h) => {
  const sv = (h.get('server') || '').toLowerCase(), via = (h.get('via') || '').toLowerCase();
  if (h.get('cf-ray') || sv.includes('cloudflare')) return 'cloudflare';
  for (const k of ['openresty', 'nginx', 'haproxy', 'caddy', 'apache', 'envoy', 'traefik']) if (sv.includes(k) || via.includes(k)) return k;
  return sv.startsWith('nodeos') ? 'direct / hidden' : sv ? sv.slice(0, 20) : 'hidden';
};
// Probe failures become a short, human label for the table; the raw message is kept (trimmed) for tooltips.
function shortErr(e) {
  const m = String(e?.message || e), code = e?.cause?.code || e?.code || '';
  if (e?.name === 'SyntaxError' || /Unexpected token|not valid JSON|JSON/.test(m)) return /'<'|<!DOCTYPE|<html/i.test(m) ? 'HTML page, not API' : 'not JSON';
  if (e?.name === 'TimeoutError' || e?.name === 'AbortError' || /timeout|aborted/i.test(m)) return 'timeout';
  if (/ECONNREFUSED/.test(code)) return 'refused';
  if (/ENOTFOUND|EAI_AGAIN/.test(code)) return 'DNS failed';
  if (/ECONNRESET|UND_ERR_SOCKET/.test(code)) return 'connection reset';
  if (/CERT|SSL|TLS|self.signed/i.test(code + m)) return 'TLS error';
  if (/^HTTP \d+/.test(m)) return m.slice(0, 12);
  if (/fetch failed/.test(m)) return code ? code.toLowerCase() : 'unreachable';
  return m.slice(0, 24);
}
async function timed(fn) { const t = Date.now(); try { const r = await fn(); return { ok: true, ms: Date.now() - t, ...r }; } catch (e) { return { ok: false, ms: Date.now() - t, error: shortErr(e), error_detail: String(e?.message || e).slice(0, 160) }; } }
function tcp(hostport) {
  return new Promise((res) => {
    const m = String(hostport).trim().match(/^\[?([^\]]+?)\]?:(\d+)$/); if (!m) return res({ ok: false, error: 'bad endpoint' });
    const t = Date.now(); const s = net.connect({ host: m[1], port: +m[2], timeout: 4000 });
    s.once('connect', () => { s.destroy(); res({ ok: true, ms: Date.now() - t }); });
    s.once('timeout', () => { s.destroy(); res({ ok: false, error: 'timeout' }); });
    s.once('error', (e) => res({ ok: false, error: e.code || 'error' }));
  });
}
async function probeApi(url, chainId, head) {
  return timed(async () => {
    const r = await fetch(`${url}/v1/chain/get_info`, { method: 'POST', body: '{}', signal: AbortSignal.timeout(6000) });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const b = await r.json();
    return { edge: edgeOf(r.headers), version: b.server_version_string || null, head: b.head_block_num, lag: chain[n_id(chainId)]?.head && b.head_block_num ? Math.max(0, chain[n_id(chainId)].head - b.head_block_num) : null,
      chain_ok: !chainId || b.chain_id === chainId };
  });
}
async function probeHyperion(url, head) {
  return timed(async () => {
    const h = await (await fetch(`${url}/v2/health`, { signal: AbortSignal.timeout(6000) })).json();
    if (!Array.isArray(h.health) || !h.version) throw new Error('not a Hyperion health document');
    const idx = (h.health || []).map((x) => x.service_data?.last_indexed_block).filter((v) => v > 0).sort((x, y) => y - x)[0];
    return { version: h.version || null, indexed: idx || null, lag: head && idx ? Math.max(0, head - idx) : null,
      services: (h.health || []).map((x) => ({ s: x.service, ok: x.status === 'OK' })) };
  });
}
async function probeAtomic(url) {
  return timed(async () => {
    const h = await (await fetch(`${url}/health`, { signal: AbortSignal.timeout(6000) })).json();
    return { version: h.data?.version || null, chain_ok: h.data?.chain?.status === 'OK', head: h.data?.chain?.head_block || null };
  });
}
async function pool(items, n, fn) { let i = 0; await Promise.all(Array.from({ length: n }, async () => { while (i < items.length) { const k = i++; await fn(items[k]); } })); }
async function surveyInfra(n) {
  const reg = registry[n.id]?.producers; const c = chain[n.id];
  if (!reg || !c?.head || infra[n.id]?.running) return;
  const run = (infra[n.id] ||= { nodes: [] }); run.running = true;
  const nodes = [];
  try {
  await pool(Object.values(reg), 4, async (p) => { try {
    const base = safeUrl(p.url); if (!base) return;
    let bp; try { bp = await bpJson(base, n.chain_id); } catch { nodes.push({ producer: p.owner, rank: p.rank, missing_bp_json: true }); return; }
    for (const nd of (bp.nodes || []).slice(0, 12)) {
      const types = [].concat(nd.node_type || []).map(String);
      const features = [].concat(nd.features || []).map(String);
      const url = safeUrl(nd.ssl_endpoint || nd.api_endpoint || '');
      const e = { producer: p.owner, rank: p.rank, types, features, url, p2p: nd.p2p_endpoint || null,
        location: nd.location?.name || nd.location?.country || null };
      const jobs = [];
      if (url) {
        jobs.push(probeApi(url, n.chain_id, c.head).then((r) => (e.api = r)));
        if (features.includes('hyperion-v2') || types.includes('query')) jobs.push(probeHyperion(url, c.head).then((r) => { if (r.ok || features.includes('hyperion-v2')) e.hyperion = r; }));
        if (features.some((f) => /atomic/i.test(f))) jobs.push(probeAtomic(url).then((r) => (e.atomic = r)));
      }
      if (e.p2p) jobs.push(tcp(e.p2p).then((r) => (e.p2p_probe = r)));
      await Promise.all(jobs);
      nodes.push(e);
    }
  } catch (err) { console.error(`infra ${n.id} ${p.owner}:`, err?.stack || err); nodes.push({ producer: p.owner, rank: p.rank, survey_error: String(err?.message || err).slice(0, 120) }); } });
  // Endpoints apps and wallets point at that no active producer's bp.json lists (e.g. wallet defaults run by
  // non-producers). Probed like any other node, kept only when they serve THIS network's chain.
  const host = (u) => { try { return new URL(u).host.toLowerCase(); } catch { return ''; } };
  const listed = new Set(nodes.filter((x) => x.url).map((x) => host(x.url)));
  await pool((cfg.known_endpoints || []).filter((k) => safeUrl(k.url) && !listed.has(host(k.url))), 6, async (k) => {
    const url = safeUrl(k.url);
    const api = await probeApi(url, n.chain_id, c.head).catch(() => null);
    if (!api?.ok || api.chain_ok !== true) return;
    const e = { producer: null, unregistered: true, operator: k.operator || host(url), note: k.note || null, sources: k.sources || [],
      rank: null, types: ['api'], features: [], url, p2p: null, location: null, api };
    const hy = await probeHyperion(url, c.head).catch(() => null); if (hy?.ok) e.hyperion = hy;
    nodes.push(e);
  });
  nodes.sort((a, b) => (a.unregistered ? 1 : 0) - (b.unregistered ? 1 : 0) || (a.rank || 999) - (b.rank || 999));
  infra[n.id] = { ts: Date.now(), running: false, head: c.head, nodes };
  console.log(`infra ${n.id}: ${nodes.length} nodes surveyed`);
  } catch (err) { console.error(`infra ${n.id} failed:`, err?.stack || err); }
  finally { if (infra[n.id]) infra[n.id].running = false; }
}
setInterval(() => cfg.networks.forEach((n) => { if (!n.static_producers) surveyInfra(n).catch(() => {}); }), INFRA_EVERY_MS);
setTimeout(() => cfg.networks.forEach((n) => { if (!n.static_producers) surveyInfra(n).catch((e) => console.error('infra', e)); }), +(process.env.INFRA_FIRST_MS || 45000));

// ---- coordination: relay of SIGNED coordinator messages (event / arm / abort) ------------------------
// The server checks signatures against the network's configured coordinator keys so it can't be spammed,
// but agents verify again with keys from their OWN config: this relay cannot forge anything.
const coord = {}; // network id → { event, arm, abort }  (each: { payload, sig, key })
function verifySigned(msg, keys) {
  try {
    if (!keys?.map((k) => k.toLowerCase()).includes(String(msg.key).toLowerCase())) return null;
    const pub = createPublicKey({ key: Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), Buffer.from(msg.key, 'hex')]), format: 'der', type: 'spki' });
    if (!edVerify(null, Buffer.from(msg.payload), pub, Buffer.from(msg.sig, 'hex'))) return null;
    return JSON.parse(msg.payload);
  } catch { return null; }
}
function coordView(n) {
  const c = coord[n.id] || {};
  const ev = c.event ? JSON.parse(c.event.payload) : null;
  if (!ev) return null;
  const reps = Object.entries(nodes[n.id] || {}).map(([p, byNode]) => [p, Object.values(byNode)[0]]);
  return { event: ev, armed: !!c.arm, aborted: !!c.abort,
    arm_at: c.arm ? JSON.parse(c.arm.payload).issued_at_ms : null,
    accepted: reps.filter(([, r]) => r.report?.coord?.event_id === ev.event_id && r.report?.coord?.accepted).map(([p]) => p),
    rejected: reps.filter(([, r]) => r.report?.coord?.event_id === ev.event_id && r.report?.coord?.accepted === false).map(([p, r]) => ({ producer: p, reason: r.report.coord.reason })) };
}

// ---- agreement (atomicity A1/A3 across producers) ------------------------------------------------
const EVIDENCE_KEYS = [
  ['h', 'declared H'], ['cut_block_id', 'cut block id'], ['snapshot_sha256', 'snapshot sha256'],
  ['fingerprints_digest', 'table fingerprints'], ['burnoff_transactions', 'transactions after the cut'],
  ['state_digest', 'state digest (old@H = new@H)'], ['target_head_id', 'new chain anchor at H'],
];
function agreement(netId) {
  const rows = [];
  const list = Object.entries(nodes[netId] || {}).flatMap(([p, byNode]) => Object.entries(byNode).filter(([, v]) => (v.report.role || 'producer') === 'producer').map(([nd, v]) => [Object.keys(byNode).length > 1 ? `${p}/${nd}` : p, v]));
  for (const [key, label] of EVIDENCE_KEYS) {
    const vals = list.map(([p, n]) => [p, n.report?.ceremony?.evidence?.[key]]).filter(([, v]) => v !== undefined && v !== null);
    if (!vals.length) continue;
    const groups = {};
    for (const [p, v] of vals) (groups[JSON.stringify(v)] ||= []).push(p);
    const distinct = Object.keys(groups);
    rows.push({ key, label, agree: distinct.length === 1, reporting: vals.length,
      values: distinct.map((v) => ({ value: JSON.parse(v), producers: groups[v] })) });
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
      const producers = names.map((name) => {
        const beacons = Object.entries(reported[name] || {}).map(([node, v]) => ({ node, role: v.report.role || null, age_ms: now - v.received,
          silent: now - v.received > SILENT_AFTER_MS, report: v.report })).sort((x, y) => (x.role === 'producer' ? -1 : 0) - (y.role === 'producer' ? -1 : 0));
        const r = beacons.length ? { report: beacons[0].report, received: now - beacons[0].age_ms } : null;
        const age = r ? now - r.received : null;
        const g = reg[name] || {};
        const org = g.org || (geo[name] ? { name: geo[name].name || name, city: geo[name].city, country: geo[name].country, lat: geo[name].lat, lon: geo[name].lon } : { name });
        return { name, org: { ...org, logo: org.logo ? `/api/logo/${encodeURIComponent(n.id)}/${encodeURIComponent(name)}` : null },
          rank: g.rank || null, votes: g.votes || null, active: !!reg[name] || !!geo[name],
          scheduled: (c.schedule || []).includes(name), reporting: !!r,
          silent: r ? age > SILENT_AFTER_MS : null, age_ms: age, report: r?.report || null, beacons };
      }).sort((a, b) => (b.scheduled - a.scheduled) || ((a.rank || 999) - (b.rank || 999)) || a.name.localeCompare(b.name));
      const live = producers.filter((p) => p.reporting && !p.silent);
      return { id: n.id, name: n.name, label: n.label, priority: n.priority ?? 9, description: n.description || '',
        expected_chain_id: n.chain_id, metal: n.metal || null, event: n.event || null, chain: { ...c, schedule: undefined }, schedule: c.schedule || [],
        summary: { producers: producers.length, active: producers.filter((p) => p.active).length, scheduled: (c.schedule || []).length, reporting: live.length,
          ready: live.filter((p) => p.report?.ready).length,
          states: live.reduce((m, p) => { const s = p.report?.ceremony?.state || 'IDLE'; m[s] = (m[s] || 0) + 1; return m; }, {}) },
        producers, agreement: agreement(n.id), coordination: coordView(n), events: (events[n.id] || []).slice(0, 40) };
    }).sort((a, b) => a.priority - b.priority),
  };
}

// ---- http ----------------------------------------------------------------------------------------
const send = (res, code, body, type = 'application/json') => {
  res.writeHead(code, { 'content-type': type, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' });
  res.end(typeof body === 'string' ? body : JSON.stringify(body));
};

const reachSeen = new Map();
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html'))
    return send(res, 200, readFileSync(join(HERE, 'public', 'index.html'), 'utf8'), 'text/html; charset=utf-8');
  if (req.method === 'GET' && url.pathname === '/api/status') return send(res, 200, status());
  if (req.method === 'GET' && url.pathname === '/healthz') return send(res, 200, { ok: true });
  if (req.method === 'GET' && (url.pathname === '/favicon.ico' || url.pathname === '/favicon.svg')) {
    res.writeHead(200, { 'content-type': 'image/svg+xml', 'cache-control': 'public, max-age=86400' });
    return res.end('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#f5a524"/><stop offset="1" stop-color="#8b5cf6"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="url(#g)"/><rect x="14" y="14" width="36" height="36" rx="9" fill="#070a16"/><rect x="31" y="12" width="3" height="40" rx="1.5" fill="url(#g)"/></svg>');
  }
  const im = url.pathname.match(/^\/api\/infra\/([a-z0-9-]+)$/);
  if (im && req.method === 'GET') {
    const x = infra[im[1]]; const reg = registry[im[1]]?.producers || {};
    if (!x) return send(res, 200, { pending: true });
    return send(res, 200, { ts: x.ts, head: x.head, nodes: x.nodes.map((nd) => ({ ...nd, org: reg[nd.producer]?.org ? { name: reg[nd.producer].org.name, country: reg[nd.producer].org.country, logo: reg[nd.producer].org.logo ? `/api/logo/${im[1]}/${nd.producer}` : null } : null })) });
  }
  if (req.method === 'GET' && url.pathname.startsWith('/api/logo/')) {
    const [, , , net, owner] = url.pathname.split('/').map(decodeURIComponent);
    const l = await logo(net, owner);
    if (!l) {
      const name = registry[net]?.producers?.[owner]?.org?.name || owner || '?';
      const ini = String(name).replace(/[^A-Za-z0-9 ]/g, '').split(/\s+/).filter(Boolean).slice(0, 2).map((w) => w[0]).join('').toUpperCase() || '?';
      res.writeHead(200, { 'content-type': 'image/svg+xml', 'cache-control': 'public, max-age=3600' });
      return res.end(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="16" fill="#141b38"/><text x="32" y="40" text-anchor="middle" font-family="Helvetica,Arial,sans-serif" font-size="22" font-weight="700" fill="#c9cdf0">${ini.replace(/[<&>]/g, '')}</text></svg>`);
    }
    res.writeHead(200, { 'content-type': l.type, 'cache-control': 'public, max-age=21600', 'x-content-type-options': 'nosniff',
      'content-security-policy': "default-src 'none'; style-src 'unsafe-inline'; sandbox" });
    return res.end(l.buf);
  }
  if (req.method === 'GET' && url.pathname === '/api/reach') {
    // Can the internet reach the CALLER's Metal staking port? Only ever dials the requesting IP, only port 9651,
    // at most once per 5 s per IP. Used by metal-install.sh to tell operators whether peers can connect in.
    const local = ['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress);
    const ip = (local && req.headers['x-real-ip']) || req.socket.remoteAddress;
    const now = Date.now(); reachSeen.forEach((t, k) => { if (now - t > 60000) reachSeen.delete(k); });
    if (now - (reachSeen.get(ip) || 0) < 5000) return send(res, 429, { error: 'slow down' });
    reachSeen.set(ip, now);
    const t0 = Date.now();
    const ok = await new Promise((done) => { const sk = net.connect({ host: ip, port: 9651, timeout: 4000 });
      sk.once('connect', () => { sk.destroy(); done(true); }); sk.once('timeout', () => { sk.destroy(); done(false); }); sk.once('error', () => done(false)); });
    return send(res, 200, { ip, port: 9651, reachable: ok, ms: Date.now() - t0 });
  }
  const nm = url.pathname.match(/^\/api\/node\/([a-z0-9-]+)\/([a-z1-5.]{1,12})\/([^/]{1,64})$/);
  if (nm && req.method === 'GET') {
    // One server's latest beacon report + short history. Same public data as /api/status, per server, for agents.
    const [, netId, prod, nd] = nm.map(decodeURIComponent);
    const e = nodes[netId]?.[prod]?.[nd];
    if (!e) return send(res, 404, { error: 'no such server' });
    const age = Date.now() - e.received;
    return send(res, 200, { network: netId, producer: prod, node: nd, age_ms: age, silent: age > SILENT_AFTER_MS, report: e.report, history: e.hist || [] });
  }
  const mm = url.pathname.match(/^\/api\/manifest(?:\/([a-z0-9-]+))?$/);
  if (mm && req.method === 'GET') {
    // Network manifest: which Metal network, subnet, blockchain and VM each XPR network maps to, plus pinned
    // metalgo/plugin versions and checksums. metal-install.sh reads this, so nobody copies IDs by hand.
    const one = (n) => ({ xpr_network: n.id, xpr_chain_id: n.chain_id, ...(n.metal || {}) });
    if (!mm[1]) return send(res, 200, { updated: cfg.manifest_updated || null, networks: cfg.networks.map(one) });
    const n = cfg.networks.find((x) => x.id === mm[1]);
    return n ? send(res, 200, one(n)) : send(res, 404, { error: 'unknown network' });
  }
  const cm = url.pathname.match(/^\/api\/coord\/([a-z0-9-]+)$/);
  if (cm && req.method === 'GET') return send(res, 200, coord[cm[1]] || {});
  if (cm && req.method === 'POST') {
    const n = cfg.networks.find((x) => x.id === cm[1]); if (!n) return send(res, 404, { error: 'unknown network' });
    let raw = ''; for await (const chunk of req) { raw += chunk; if (raw.length > 16384) return send(res, 413, { error: 'too large' }); }
    let msg; try { msg = JSON.parse(raw); } catch { return send(res, 400, { error: 'bad json' }); }
    const p = verifySigned(msg, n.coordinators);
    if (!p || p.network !== n.id || !['event', 'arm', 'abort'].includes(p.type)) return send(res, 403, { error: 'not a valid signed coordinator message for this network' });
    const c = (coord[n.id] ||= {});
    if (p.type === 'event') {
      if (n.chain_id && p.chain_id !== n.chain_id) return send(res, 409, { error: 'event chain_id does not match this network' });
      coord[n.id] = { event: msg }; pushEvent(n.id, 'coordinator', `published event ${p.event_id}: cut at H = ${p.h}`);
    } else {
      const ev = c.event && JSON.parse(c.event.payload);
      if (!ev || ev.event_id !== p.event_id) return send(res, 409, { error: 'no such event' });
      c[p.type] = msg; pushEvent(n.id, 'coordinator', p.type === 'arm' ? `ARMED event ${p.event_id}` : `ABORTED event ${p.event_id}`);
    }
    return send(res, 200, { ok: true, type: p.type, event_id: p.event_id });
  }
  if (req.method === 'POST' && url.pathname === '/api/report') {
    const auth = req.headers.authorization || '';
    const bind = tokens[sha(auth.replace(/^Bearer\s+/i, ''))];
    if (!bind) return send(res, 401, { error: 'unknown token' });
    let raw = '';
    for await (const chunk of req) { raw += chunk; if (raw.length > MAX_BODY) return send(res, 413, { error: 'too large' }); }
    let r; try { r = JSON.parse(raw); } catch { return send(res, 400, { error: 'bad json' }); }
    if (r.network !== bind.network || r.producer !== bind.producer)
      return send(res, 403, { error: `token is bound to ${bind.producer}@${bind.network}` });
    const nodeName = String(r.node || 'node').slice(0, 64);
    const byNode = ((nodes[r.network] ||= {})[r.producer] ||= {});
    const tokenHash = sha(auth.replace(/^Bearer\s+/i, ''));
    // One token = one box: if this token reported under another name before, that was a rename, not a second node.
    for (const [nd, v] of Object.entries(byNode)) if (nd !== nodeName && v.token === tokenHash) delete byNode[nd];
    const prev = byNode[nodeName]?.report;
    // Short in-memory history (about 2 h at a 10 s interval) for the per-server page's charts.
    const hist = (byNode[nodeName]?.hist || []).slice(-719);
    const lagNow = chain[r.network]?.head && r.source?.head ? Math.max(0, chain[r.network].head - r.source.head) : null;
    hist.push({ t: Date.now(), head: r.source?.head ?? null, lag: lagNow, peers: r.metal?.peers ?? null, ok: (r.checks || []).filter((c) => c.ok).length, n: (r.checks || []).length });
    byNode[nodeName] = { report: r, received: Date.now(), token: tokenHash, hist };
    const who = Object.keys(byNode).length > 1 || r.node ? `${r.producer} · ${nodeName}` : r.producer;
    const was = prev?.ceremony?.state, is = r.ceremony?.state;
    if (!prev) pushEvent(r.network, who, `started reporting (${r.role || 'node'}, agent ${r.agent_version})`);
    if (is && is !== was) pushEvent(r.network, who, `→ ${is}`);
    if (prev && prev.ready !== r.ready) pushEvent(r.network, who, r.ready ? 'READY' : `not ready: ${(r.checks || []).filter((c) => !c.ok).map((c) => c.name).join(', ')}`);
    return send(res, 200, { ok: true });
  }
  // App routes (/<net>/<producer>/…) are client-side: serve the dashboard for any other GET that isn't an API call.
  if (req.method === 'GET' && !url.pathname.startsWith('/api/') && /^\/[A-Za-z0-9._~%\/:-]*$/.test(url.pathname))
    return send(res, 200, readFileSync(join(HERE, 'public', 'index.html'), 'utf8'), 'text/html; charset=utf-8');
  send(res, 404, { error: 'not found' });
}).listen(PORT, '127.0.0.1', () => console.log(`mission control on 127.0.0.1:${PORT} · ${cfg.networks.length} networks · tokens ${Object.keys(tokens).length}`));
