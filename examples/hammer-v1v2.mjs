// External availability hammer for a hyperion-mode cutover rehearsal.
// Runs OFF-BOX (the trace only means something if it crosses the real
// internet). Probes, concurrently and independently:
//   - POST {BASE}/v1/chain/get_info                       every 250 ms
//   - GET  {BASE}/v2/health                               every 250 ms
//   - GET  {BASE}/v2/history/get_actions?account=...&limit=1  every 1 s
// One JSONL line per probe: {ts, ep, ok, status, ms, head, note}.
//
//   node hammer-v1v2.mjs http://<public-ip-or-domain> <account> hammer.jsonl
import fs from 'fs';

const BASE = (process.argv[2] || 'http://127.0.0.1').replace(/\/$/, '');
const ACCOUNT = process.argv[3] || 'protonnz';
const OUT = process.argv[4] || 'hammer-hyp.jsonl';
const out = fs.createWriteStream(OUT, { flags: 'a' });

async function probe(ep, url, opts, extract) {
  const t0 = Date.now();
  const line = { ts: new Date(t0).toISOString(), ep, ok: false, status: 0, ms: 0 };
  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 5000);
    const r = await fetch(url, { ...opts, signal: ctl.signal });
    clearTimeout(timer);
    line.status = r.status;
    const j = await r.json().catch(() => null);
    line.ok = r.ok && j !== null;
    if (j && extract) Object.assign(line, extract(j));
  } catch (e) {
    line.note = String(e && e.message || e).slice(0, 120);
  }
  line.ms = Date.now() - t0;
  out.write(JSON.stringify(line) + '\n');
}

setInterval(() => probe('v1_get_info', `${BASE}/v1/chain/get_info`, { method: 'POST' },
  (j) => ({ head: j.head_block_num, prod: j.head_block_producer || undefined })), 250);

setInterval(() => probe('v2_health', `${BASE}/v2/health`, {},
  (j) => ({
    services: (j.health || []).map((s) => `${s.service}:${s.status}`).join(','),
    fed_local_ok: j.federation?.local?.ok,
    last_indexed: j.federation?.local?.last_indexed_block
      ?? (j.health || []).find((s) => s.service === 'Indexer')?.service_data?.last_indexed_block,
  })), 250);

setInterval(() => probe('v2_get_actions', `${BASE}/v2/history/get_actions?account=${ACCOUNT}&limit=1`, {},
  (j) => ({
    n: j.actions?.length ?? 0,
    top_block: j.actions?.[0]?.block_num,
    total: j.total?.value,
    federated: j.federated,
    premig: j.actions?.[0]?._premigration,
  })), 1000);

console.log(`hammering ${BASE} (v1 250ms, v2 health 250ms, v2 get_actions 1s) -> ${OUT}`);
