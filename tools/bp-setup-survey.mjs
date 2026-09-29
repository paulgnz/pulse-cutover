// bp-setup-survey.mjs — read-only survey of active producers' public infrastructure (mainnet + testnet).
// For each active producer: bp.json (via chains.json) → nodes → probe public API endpoints:
// nodeos version (get_info), edge/server headers, Hyperion /v2/health. Public GETs only, bounded concurrency.
const NETS = [
  { id: 'mainnet', chain_id: '384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0', rpc: 'https://proton.eosusa.io' },
  { id: 'testnet', chain_id: '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd', rpc: 'https://tn1.protonnz.com' },
];
const T = (ms) => AbortSignal.timeout(ms);
const j = async (url, opt = {}) => { const r = await fetch(url, { ...opt, signal: T(7000) }); if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); };
async function bpJson(base, chainId) {
  try { const c = await j(`${base}/chains.json`); const p = c?.chains?.[chainId]; if (p) return await j(new URL(p, base + '/').href); } catch {}
  return j(`${base}/bp.json`);
}
function edgeOf(h) {
  const server = (h.get('server') || '').toLowerCase(), via = (h.get('via') || '').toLowerCase();
  if (h.get('cf-ray') || server.includes('cloudflare')) return 'cloudflare';
  if (server.includes('openresty')) return 'openresty';
  if (server.includes('nginx')) return 'nginx';
  if (server.includes('haproxy') || via.includes('haproxy')) return 'haproxy';
  if (server.includes('caddy')) return 'caddy';
  if (server.includes('apache')) return 'apache';
  if (server.includes('envoy')) return 'envoy';
  return server ? 'other:' + server.slice(0, 30) : 'unlabelled (server header hidden, e.g. haproxy/traefik)';
}
async function probe(url) {
  const out = { url };
  try {
    const r = await fetch(`${url}/v1/chain/get_info`, { method: 'POST', body: '{}', signal: T(7000) });
    out.edge = edgeOf(r.headers); out.http = r.status;
    const b = await r.json().catch(() => ({}));
    out.version = b.server_version_string || null; out.head = b.head_block_num || null; out.chain_id = b.chain_id;
  } catch (e) { out.error = String(e.message || e).slice(0, 80); }
  try {
    const h = await j(`${url}/v2/health`);
    out.hyperion = { version: h.version || null, hash: h.version_hash || null, host: h.host || null,
      rs: /rs|rust/i.test(JSON.stringify(h).slice(0, 400)) || (h.health || []).some((x) => /PulseVM/.test(x.service)),
      services: (h.health || []).map((x) => `${x.service}:${x.status}`) };
  } catch {}
  return out;
}
async function pool(items, n, fn) { const res = []; let i = 0; await Promise.all(Array.from({ length: n }, async () => { while (i < items.length) { const k = i++; res[k] = await fn(items[k]); } })); return res; }
const report = { generated_at: new Date().toISOString(), networks: {} };
for (const net of NETS) {
  const rows = (await j(`${net.rpc}/v1/chain/get_table_rows`, { method: 'POST', body: JSON.stringify({ json: true, code: 'eosio', scope: 'eosio', table: 'producers', limit: 500 }) })).rows
    .filter((p) => p.is_active === 1).sort((a, b) => parseFloat(b.total_votes) - parseFloat(a.total_votes));
  const sched = (await j(`${net.rpc}/v1/chain/get_producer_schedule`, { method: 'POST', body: '{}' })).active.producers.map((p) => p.producer_name);
  const prods = await pool(rows, 6, async (p, ) => {
    const base = String(p.url || '').replace(/\/$/, '');
    const e = { owner: p.owner, rank: rows.indexOf(p) + 1, scheduled: sched.includes(p.owner), url: base };
    try {
      const bp = await bpJson(base, net.chain_id);
      e.name = bp.org?.candidate_name; e.country = bp.org?.location?.country;
      const nodes = bp.nodes || [];
      e.node_types = nodes.map((n) => [].concat(n.node_type || []).join('+'));
      e.features = [...new Set(nodes.flatMap((n) => n.features || []))];
      const eps = [...new Set(nodes.flatMap((n) => [n.ssl_endpoint, n.api_endpoint]).filter(Boolean).map((u) => u.replace(/\/$/, '')))].slice(0, 4);
      e.endpoints = await pool(eps, 2, probe);
      e.p2p = nodes.filter((n) => n.p2p_endpoint).length;
    } catch (err) { e.bp_json_error = String(err.message || err).slice(0, 80); }
    return e;
  });
  report.networks[net.id] = prods;
}
const fs = await import('node:fs');
const file = new URL(`./bp-setups-${report.generated_at.slice(0, 10)}.json`, import.meta.url);
fs.writeFileSync(file, JSON.stringify(report, null, 2));
// summary
for (const [id, prods] of Object.entries(report.networks)) {
  const eps = prods.flatMap((p) => p.endpoints || []).filter((e) => !e.error);
  const count = (arr) => Object.entries(arr.reduce((m, k) => (m[k] = (m[k] || 0) + 1, m), {})).sort((a, b) => b[1] - a[1]);
  console.log(`\n== ${id}: ${prods.length} active producers, ${prods.filter((p) => !p.bp_json_error).length} with bp.json, ${eps.length} reachable API endpoints`);
  console.log('nodeos versions  :', JSON.stringify(count(eps.map((e) => (e.version || '?').split('-')[0]))));
  console.log('edges            :', JSON.stringify(count(eps.map((e) => e.edge))));
  const hy = eps.filter((e) => e.hyperion);
  console.log('hyperion         :', hy.length, 'endpoints ·', JSON.stringify(count(hy.map((e) => e.hyperion.version || '?'))));
  console.log('producers w/ hyperion:', new Set(prods.filter((p) => (p.endpoints || []).some((e) => e.hyperion)).map((p) => p.owner)).size);
  console.log('features         :', JSON.stringify(count(prods.flatMap((p) => p.features || [])).slice(0, 10)));
  console.log('no bp.json       :', prods.filter((p) => p.bp_json_error).map((p) => p.owner).join(', ') || '—');
}
