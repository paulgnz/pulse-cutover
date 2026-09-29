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
import { createHash } from 'node:crypto';
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

// ---- agreement (atomicity A1/A3 across producers) ------------------------------------------------
const EVIDENCE_KEYS = [
  ['h', 'declared H'], ['cut_block_id', 'cut block id'], ['snapshot_sha256', 'snapshot sha256'],
  ['fingerprints_digest', 'table fingerprints'], ['burnoff_transactions', 'transactions after the cut'],
  ['state_digest', 'state digest (old@H = new@H)'], ['target_head_id', 'new chain anchor at H'],
];
function agreement(netId) {
  const rows = [];
  const list = Object.entries(nodes[netId] || {});
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
        const r = reported[name];
        const age = r ? now - r.received : null;
        const g = reg[name] || {};
        const org = g.org || (geo[name] ? { name: geo[name].name || name, city: geo[name].city, country: geo[name].country, lat: geo[name].lat, lon: geo[name].lon } : { name });
        return { name, org: { ...org, logo: org.logo ? `/api/logo/${encodeURIComponent(n.id)}/${encodeURIComponent(name)}` : null },
          rank: g.rank || null, votes: g.votes || null, active: !!reg[name] || !!geo[name],
          scheduled: (c.schedule || []).includes(name), reporting: !!r,
          silent: r ? age > SILENT_AFTER_MS : null, age_ms: age, report: r?.report || null };
      }).sort((a, b) => (b.scheduled - a.scheduled) || ((a.rank || 999) - (b.rank || 999)) || a.name.localeCompare(b.name));
      const live = producers.filter((p) => p.reporting && !p.silent);
      return { id: n.id, name: n.name, label: n.label, priority: n.priority ?? 9, description: n.description || '',
        expected_chain_id: n.chain_id, event: n.event || null, chain: { ...c, schedule: undefined }, schedule: c.schedule || [],
        summary: { producers: producers.length, active: producers.filter((p) => p.active).length, scheduled: (c.schedule || []).length, reporting: live.length,
          ready: live.filter((p) => p.report?.ready).length,
          states: live.reduce((m, p) => { const s = p.report?.ceremony?.state || 'IDLE'; m[s] = (m[s] || 0) + 1; return m; }, {}) },
        producers, agreement: agreement(n.id), events: (events[n.id] || []).slice(0, 40) };
    }).sort((a, b) => a.priority - b.priority),
  };
}

// ---- http ----------------------------------------------------------------------------------------
const send = (res, code, body, type = 'application/json') => {
  res.writeHead(code, { 'content-type': type, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' });
  res.end(typeof body === 'string' ? body : JSON.stringify(body));
};

http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html'))
    return send(res, 200, readFileSync(join(HERE, 'public', 'index.html'), 'utf8'), 'text/html; charset=utf-8');
  if (req.method === 'GET' && url.pathname === '/api/status') return send(res, 200, status());
  if (req.method === 'GET' && url.pathname === '/healthz') return send(res, 200, { ok: true });
  if (req.method === 'GET' && url.pathname.startsWith('/api/logo/')) {
    const [, , , net, owner] = url.pathname.split('/').map(decodeURIComponent);
    const l = await logo(net, owner);
    if (!l) return send(res, 404, { error: 'no logo' });
    res.writeHead(200, { 'content-type': l.type, 'cache-control': 'public, max-age=21600', 'x-content-type-options': 'nosniff',
      'content-security-policy': "default-src 'none'; style-src 'unsafe-inline'; sandbox" });
    return res.end(l.buf);
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
    const prev = nodes[r.network]?.[r.producer]?.report;
    (nodes[r.network] ||= {})[r.producer] = { report: r, received: Date.now() };
    const was = prev?.ceremony?.state, is = r.ceremony?.state;
    if (!prev) pushEvent(r.network, r.producer, `started reporting (agent ${r.agent_version})`);
    if (is && is !== was) pushEvent(r.network, r.producer, `→ ${is}`);
    if (prev && prev.ready !== r.ready) pushEvent(r.network, r.producer, r.ready ? 'READY' : `not ready: ${(r.checks || []).filter((c) => !c.ok).map((c) => c.name).join(', ')}`);
    return send(res, 200, { ok: true });
  }
  send(res, 404, { error: 'not found' });
}).listen(PORT, '127.0.0.1', () => console.log(`mission control on 127.0.0.1:${PORT} · ${cfg.networks.length} networks · tokens ${Object.keys(tokens).length}`));
