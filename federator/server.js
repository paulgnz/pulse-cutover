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
// Why a standalone service (and not a pulse-rest-gateway extension): the
// gateway is stateless PROTOCOL TRANSLATION (/v1 REST -> pulsevm.* JSON-RPC)
// that every api-mode operator needs; this is HISTORY BOUNDARY ROUTING
// between two same-protocol /v2 sources, parameterized by ceremony facts
// (cut block/time), that only hyperion-mode operators need — different
// concern, different config surface, different lifetime (this one is the
// permanent /v2 server after the cut). Coupling them would make every /v1
// operator carry federation config.
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
//      BOUNDARY_FILE (/etc/pulse-cutover/boundary.json), PORT,
//      PASSTHROUGH_PORT, LOCAL_TIMEOUT_MS (8000), LEGACY_TIMEOUT_MS (8000).
const http = require('http');
const fs = require('fs');

const LOCAL = (process.env.LOCAL || 'http://127.0.0.1:7000').replace(/\/$/, '');
const LEGACY = (process.env.LEGACY || 'https://test.proton.eosusa.io').replace(/\/$/, '');
const BOUNDARY_FILE = process.env.BOUNDARY_FILE || '/etc/pulse-cutover/boundary.json';
const PORT = Number(process.env.PORT || 7010);
const PASSTHROUGH_PORT = Number(process.env.PASSTHROUGH_PORT || 7019);
const LOCAL_TIMEOUT_MS = Number(process.env.LOCAL_TIMEOUT_MS || 8000);
const LEGACY_TIMEOUT_MS = Number(process.env.LEGACY_TIMEOUT_MS || 8000);

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

// ---- upstream fetch that never throws
async function fetchJson(base, path, timeoutMs) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const r = await fetch(base + path, { signal: ctl.signal, headers: { accept: 'application/json' } });
    const text = await r.text();
    try { return { ok: r.ok, status: r.status, json: JSON.parse(text) }; }
    catch { return { ok: false, status: r.status, json: null, text: text.slice(0, 300) }; }
  } catch (e) {
    return { ok: false, status: 0, json: null, error: String((e && e.message) || e) };
  } finally { clearTimeout(timer); }
}
const localGet = (path) => fetchJson(LOCAL, path, LOCAL_TIMEOUT_MS);
const legacyGet = (path) => fetchJson(LEGACY, path, LEGACY_TIMEOUT_MS);

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
  const localTotal = local.ok ? (local.json?.total?.value ?? 0) : 0;
  const localActs = (local.ok ? local.json?.actions ?? [] : [])
    .filter((a) => (a.block_num ?? 0) > b.cut_block)
    .slice(0, limit);

  const need = limit - localActs.length;
  let legacyActs = [];
  let legacyTotal = 0;
  let legacy = null;
  const legacySkip = Math.max(0, skip - localTotal);
  const legacyParams = { account, limit: Math.max(need, 1), skip: legacySkip, sort: 'desc', ...extra };
  if (b.cut_time) legacyParams.before = b.cut_time;
  legacy = await legacyGet(`/v2/history/get_actions?${qs(legacyParams)}`);
  if (legacy.ok) {
    legacyTotal = legacy.json?.total?.value ?? 0;
    if (need > 0) {
      legacyActs = (legacy.json?.actions ?? [])
        .filter((a) => (a.block_num ?? 0) <= b.cut_block)
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
      lib: local.ok ? local.json?.lib : undefined,
      federated: true,
      ...(partial ? {
        partial: true,
        source_errors: {
          ...(local.ok ? {} : { local: { status: local.status, error: local.error || local.text } }),
          ...(legacy.ok ? {} : { legacy: { status: legacy.status, error: legacy.error || legacy.text } }),
        },
      } : {}),
      boundary: pubBoundary(b),
    },
  };
}

// A transaction is either post-cut (local) or pre-cut (legacy): new-then-legacy.
async function getTransaction(params) {
  const id = String(params.id || '');
  const local = await localGet(`/v2/history/get_transaction?id=${encodeURIComponent(id)}`);
  if (local.ok && local.json?.actions?.length) {
    return { status: 200, body: { ...local.json, federated: true } };
  }
  const legacy = await legacyGet(`/v2/history/get_transaction?id=${encodeURIComponent(id)}`);
  if (legacy.ok && legacy.json?.actions?.length) {
    return { status: 200, body: { ...legacy.json, _premigration: true, federated: true } };
  }
  if (local.ok || legacy.ok) {
    return { status: 200, body: { ...(local.ok ? local.json : legacy.json), federated: true } };
  }
  return { status: 502, body: errBody('both history sources unreachable', local, legacy) };
}

// Keys are 1:1 across the cut; local only knows post-cut signers — union.
async function getKeyAccounts(params, path) {
  const [local, legacy] = await Promise.all([localGet(path), legacyGet(path)]);
  const names = new Set([
    ...((local.ok && local.json?.account_names) || []),
    ...((legacy.ok && legacy.json?.account_names) || []),
  ]);
  if (!local.ok && !legacy.ok) return { status: 502, body: errBody('both sources unreachable', local, legacy) };
  return { status: 200, body: { account_names: [...names].sort(), federated: true } };
}

// Aggregate health: local is the post-cut source of truth; legacy is checked
// so the boundary's pre-cut half is monitored through the same URL.
async function health() {
  const b = boundary();
  const [local, legacy] = await Promise.all([localGet('/v2/health'), legacyHealthCached()]);
  const localServices = (local.ok && local.json?.health) || [];
  const lastIndexed = localServices.find((s) => s.service === 'Indexer')?.service_data?.last_indexed_block;
  const rpcHead = localServices.find((s) => s.service === 'PulseVM-RPC')?.service_data?.head_block_num;
  // local.ok mirrors the agent's hydration predicate: all services OK, with
  // the IDLE-AT-CUT allowance — hyperion-rs reports `Indexer: Warning,
  // last_indexed_block: 0` when zero post-cut blocks exist (observed live;
  // an all-OK requirement wedges the flip gate on an idle chain).
  const nonIndexerOk = localServices.length > 0
    && localServices.filter((s) => s.service !== 'Indexer').every((s) => s.status === 'OK');
  const allOk = localServices.length > 0 && localServices.every((s) => s.status === 'OK');
  const idleAtCut = nonIndexerOk && (lastIndexed ?? 0) === 0
    && typeof rpcHead === 'number' && rpcHead <= b.cut_block;
  const localOk = local.ok && (allOk || idleAtCut);
  const legacyOk = legacy.ok && Array.isArray(legacy.json?.health);
  return {
    status: 200,
    body: {
      version: local.ok ? local.json?.version : undefined,
      chain: local.ok ? local.json?.chain : undefined,
      health: localServices,
      federation: {
        boundary: pubBoundary(b),
        local: { url: LOCAL, ok: localOk, last_indexed_block: lastIndexed, idle_at_cut: idleAtCut || undefined },
        legacy: { url: LEGACY, ok: legacyOk },
      },
    },
  };
}

function pubBoundary(b) {
  return b.cut_block === Number.MAX_SAFE_INTEGER
    ? { staged: false }
    : { cut_block: b.cut_block, cut_time: b.cut_time, cut_block_id: b.cut_block_id };
}
function errBody(msg, ...ups) {
  return { error: msg, upstreams: ups.map((u) => ({ status: u.status, error: u.error || u.text })) };
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
  const p = url.pathname;
  if (p === '/v2/health') return health();
  if (p === '/v2/history/get_actions') return getActions(params);
  if (p === '/v2/history/get_transaction') return getTransaction(params);
  if (p === '/v2/state/get_key_accounts') return getKeyAccounts(params, `${p}?${qs(params)}`);
  if (p === '/' || p === '/v2') {
    return { status: 200, body: { service: 'hyperion-federator', local: LOCAL, legacy: LEGACY, boundary: pubBoundary(boundary()) } };
  }
  // Everything else under /v2: local first (post-cut truth), legacy fallback
  // (pre-cut facts like get_creator live there). /v1/history/*: local only —
  // hyperion-rs ships a post-cut /v1 shim; v1 pos/offset pagination is NOT
  // federated across the boundary (seq positions differ per source; /v2 is
  // the federated surface).
  if (p.startsWith('/v2/') || p.startsWith('/v1/history/')) {
    const path = `${p}?${qs(params)}`;
    const local = await localGet(path);
    if (local.ok) return { status: 200, body: local.json };
    if (p.startsWith('/v2/')) {
      const legacy = await legacyGet(path);
      if (legacy.ok) return { status: 200, body: { ...legacy.json, _premigration: true } };
      return { status: 502, body: errBody(`no source answered ${p}`, local, legacy) };
    }
    return { status: local.status || 502, body: local.json ?? errBody(`local /v1 shim unavailable`, local) };
  }
  return { status: 404, body: { error: `not a history endpoint: ${p}` } };
}

function serve(port, handler, label) {
  http.createServer((req, res) => {
    res.setHeader('access-control-allow-origin', '*');
    if (req.method === 'OPTIONS') {
      res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
      res.setHeader('access-control-allow-headers', 'content-type');
      res.statusCode = 204;
      return res.end();
    }
    handler(req)
      .then(({ status, body }) => {
        res.statusCode = status;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(body));
      })
      .catch((e) => {
        res.statusCode = 500;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ error: String((e && e.message) || e) }));
      });
  }).listen(port, () => console.log(`${label} on :${port} (local=${LOCAL}, legacy=${LEGACY}, boundary=${BOUNDARY_FILE})`));
}

// Pure legacy proxy: "the /v2 you already had", for the pre-flip upstream.
async function routePassthrough(req) {
  const url = new URL(req.url, 'http://x');
  let path = url.pathname + url.search;
  if (req.method === 'POST') {
    const body = await readBody(req);
    try {
      const params = { ...Object.fromEntries(url.searchParams), ...JSON.parse(body || '{}') };
      path = `${url.pathname}?${qs(params)}`;
    } catch { /* keep as-is */ }
  }
  const r = await legacyGet(path);
  return { status: r.ok ? 200 : (r.status || 502), body: r.json ?? errBody('legacy unreachable', r) };
}

serve(PORT, routeFederated, 'hyperion-federator (federating router)');
serve(PASSTHROUGH_PORT, routePassthrough, 'hyperion-federator (legacy passthrough)');
