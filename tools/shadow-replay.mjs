#!/usr/bin/env node
// shadow-replay.mjs — replay real mainnet traffic after the cut into a SHADOW chain and compare outcomes.
//
//   # dry run (default): fetch the source blocks after H and classify what a replay would meet; no target contact
//   node tools/shadow-replay.mjs --source http://127.0.0.1:8888 --cut-height 408328634 --blocks 1200 --out plan.json
//
//   # send: forward each already-signed transaction, unchanged, to the shadow chain and confirm inclusion
//   node tools/shadow-replay.mjs --source http://127.0.0.1:8888 --cut-height 408328634 --cut-id <id of H> \
//        --target http://10.0.0.5:8888 --target-api v1 --send --shadow-i-understand --rate 20 --out replay.json
//
// Rung 5 of the test ladder (docs/HOW-IT-WORKS.md §7): a shadow cutover runs the full ceremony on a PRIVATE network
// from real mainnet state at H with the real chain_id. This tool then takes the transactions mainnet itself
// executed in H+1…H+n and sends each one, byte for byte as mainnet received it, to the shadow chain, so the new
// chain meets the real workload. It never signs anything and holds no keys: it only forwards transactions mainnet
// has already executed (mainnet's own replay protection rejects them if they ever leak back).
//
// Because a shadow chain carries the mainnet chain_id, a target that IS mainnet would accept nothing new (every
// transaction is a duplicate there) but the tool still refuses it outright: see checkTargetSafety().
// No dependencies (Node >= 18). Exit 0 = dry run done / every outcome matched, 1 = mismatches, 2 = refused or error.
import { createHash } from 'node:crypto';
import { writeFileSync, readFileSync } from 'node:fs';
import { inflateSync } from 'node:zlib';
import { pathToFileURL } from 'node:url';

export const VERSION = '1';
export const XPR_MAINNET_CHAIN_ID = '384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0';
// Public XPR mainnet API hosts a shadow replay must never target, whatever they answer. Not exhaustive: the
// block-after-H check in checkTargetSafety() is what catches any other mainnet node.
export const PUBLIC_MAINNET_HOSTS = [
  'proton.greymass.com', 'proton.cryptolions.io', 'proton.eosusa.io', 'proton.eoscafeblock.com', 'api.protonnz.com',
  'xpr-mainnet-api.bloxprod.io',
];
const STATUS_BY_NUM = ['executed', 'soft_fail', 'hard_fail', 'delayed', 'expired'];

// ---------------------------------------------------------------- pure helpers (tested) --

/** Normalise a receipt status (Leap JSON string or numeric enum) to a name. */
export function statusName(s) {
  if (typeof s === 'number') return STATUS_BY_NUM[s] || `status_${s}`;
  return String(s ?? 'unknown');
}

/** Transaction id = sha256 of the (decompressed) packed transaction. */
export function packedTrxId(packedHex, compression) {
  let bytes = Buffer.from(String(packedHex), 'hex');
  const c = compression;
  if (c === 1 || c === '1' || String(c).toLowerCase() === 'zlib') bytes = inflateSync(bytes);
  return createHash('sha256').update(bytes).digest('hex');
}

/**
 * The block number a ref_block_num most plausibly refers to: the highest block ≤ includedIn whose low 16 bits
 * equal ref (TAPOS references a recent block). Leap only accepts references within the last 65,536 blocks.
 */
export function refBlockNumber(ref, includedIn) {
  const low = includedIn & 0xffff;
  const back = (low - (ref & 0xffff) + 0x10000) & 0xffff;
  return includedIn - back;
}

/**
 * Turn one Leap /v1 get_block response into replay candidates. A receipt whose trx is only an id (a deferred or
 * scheduled transaction mainnet generated itself) cannot be forwarded; it is listed as not replayable.
 */
export function extractTxs(block, cutHeight) {
  const num = Number(block.block_num);
  const out = [];
  (block.transactions || []).forEach((r, i) => {
    const status = statusName(r.status);
    const base = { block_num: num, index: i, status, cpu_usage_us: r.cpu_usage_us ?? null, net_usage_words: r.net_usage_words ?? null };
    let trx = r.trx;
    if (Array.isArray(trx)) trx = trx[1];
    if (typeof trx === 'string') { out.push({ ...base, id: trx, replayable: false, reason: 'deferred' }); return; }
    if (!trx || typeof trx.packed_trx !== 'string') { out.push({ ...base, id: trx?.id ?? null, replayable: false, reason: 'no_packed_trx' }); return; }
    const id = trx.id || packedTrxId(trx.packed_trx, trx.compression);
    const t = trx.transaction || {};
    const notes = [];
    if (t.ref_block_num != null && refBlockNumber(Number(t.ref_block_num), num) > cutHeight) notes.push('tapos_ref_after_cut');
    if ((t.context_free_actions || []).length || (trx.context_free_data || []).length || (trx.packed_context_free_data || '').length) notes.push('context_free');
    if (Number(t.delay_sec || 0) > 0) notes.push('delay_sec');
    if (status !== 'executed') notes.push(`mainnet_${status}`);
    out.push({
      ...base, id, replayable: true, notes, expiration: t.expiration ?? null,
      payload: { signatures: trx.signatures || [], compression: trx.compression ?? 'none', packed_context_free_data: trx.packed_context_free_data || '', packed_trx: trx.packed_trx },
    });
  });
  return out;
}

/** Ids (and statuses) of the transactions in a shadow-chain block, from a /v1 get_block or a pulsevm.getBlock. */
export function extractBlockIds(block) {
  const ids = new Map();
  for (const r of block?.transactions || []) {
    let trx = r.trx ?? r.transaction ?? r;
    if (Array.isArray(trx)) trx = trx[1];
    let id = null;
    if (typeof trx === 'string') id = trx;
    else if (trx && typeof trx.id === 'string') id = trx.id;
    else if (trx && typeof trx.packed_trx === 'string') { try { id = packedTrxId(trx.packed_trx, trx.compression); } catch { id = null; } }
    if (id) ids.set(id.toLowerCase(), statusName(r.status ?? 'executed'));
  }
  return ids;
}

/** Categorise a shadow rejection by its error text. */
export function classifyError(text) {
  const s = String(text || '').toLowerCase();
  if (/expired|expiration|transaction has expired|expired_tx/.test(s)) return 'expired';
  if (/tapos|ref_block|invalid_ref_block|reference block/.test(s)) return 'tapos';
  if (/duplicate|tx_duplicate|already (been )?(executed|included|seen)/.test(s)) return 'duplicate';
  if (/cpu|net usage|ram|resource|billed|exhausted|insufficient|leeway|deadline/.test(s)) return 'resource';
  if (/assert|eosio_assert|assertion/.test(s)) return 'assertion';
  if (/signature|unsatisfied_authorization|missing_auth|irrelevant_auth|authorization/.test(s)) return 'authorization';
  if (/deferred|delay|scheduled/.test(s)) return 'deferred';
  return 'other';
}

/**
 * Decide whether a target may receive replayed mainnet transactions. Returns {ok, reasons[], checks{}}.
 * target: {host, chain_id, head, block_at_cut_id, block_after_cut_id}; source: {chain_id, block_after_cut_id}.
 */
export function checkTargetSafety({ target, source, cutHeight, cutId, iUnderstand, denyHosts = [] }) {
  const reasons = [];
  const host = String(target.host || '').toLowerCase();
  const checks = { host, target_chain_id: target.chain_id ?? null, source_chain_id: source.chain_id ?? null };
  const deny = [...PUBLIC_MAINNET_HOSTS, ...denyHosts.map((h) => String(h).toLowerCase())];
  if (deny.some((h) => host === h || host.endsWith(`.${h}`))) reasons.push(`target host ${host} is a public XPR mainnet API`);
  if (!target.chain_id) reasons.push('target did not report a chain_id');
  if (source.chain_id && target.chain_id && source.chain_id !== target.chain_id) {
    reasons.push(`target chain_id ${target.chain_id} differs from the source's ${source.chain_id}: replayed signatures cannot verify there`);
  }
  const mainnet = String(target.chain_id || '').toLowerCase() === XPR_MAINNET_CHAIN_ID;
  checks.mainnet_chain_id = mainnet;
  if (mainnet) {
    if (!iUnderstand) reasons.push('target reports the XPR mainnet chain_id: pass --shadow-i-understand to confirm it is a private shadow chain');
    if (!cutId) reasons.push('target reports the XPR mainnet chain_id: --cut-id (the block id at H) is required');
    else if (!target.block_at_cut_id) reasons.push(`target cannot show its block at H=${cutHeight}: lineage unverified`);
    else if (target.block_at_cut_id.toLowerCase() !== cutId.toLowerCase()) reasons.push(`target block at H is ${target.block_at_cut_id}, not the cut ${cutId}: not a shadow of this cut`);
  }
  // A real mainnet node also has the cut block at H; what tells it apart is the block AFTER H: a shadow chain
  // produced its own H+1 (or none yet), mainnet's H+1 is the source's.
  if (source.block_after_cut_id && target.block_after_cut_id && target.block_after_cut_id.toLowerCase() === source.block_after_cut_id.toLowerCase()) {
    reasons.push(`target block H+1 equals mainnet's (${source.block_after_cut_id}): the target is following the real chain, not a shadow`);
  }
  checks.block_at_cut_id = target.block_at_cut_id ?? null;
  checks.block_after_cut_id = target.block_after_cut_id ?? null;
  return { ok: reasons.length === 0, reasons, checks };
}

/** Compare one transaction's mainnet outcome with the shadow outcome. */
export function compareOutcome(tx, shadow) {
  if (!tx.replayable) return { result: 'skipped', category: tx.reason === 'deferred' ? 'deferred' : 'not_replayable' };
  if (!shadow) return { result: 'not_sent', category: 'not_sent' };
  if (shadow.rejected) {
    const category = classifyError(shadow.error);
    return { result: tx.status === 'executed' ? 'mismatch' : 'match', category };
  }
  if (!shadow.included) return { result: 'mismatch', category: 'missing' };
  const same = shadow.included_status === 'executed' ? tx.status === 'executed' : shadow.included_status === tx.status;
  return { result: same ? 'match' : 'mismatch', category: same ? 'match' : `status_${tx.status}_vs_${shadow.included_status}` };
}

/** Summarise per-transaction rows into the report's counts. */
export function summarise(rows) {
  const by = (f) => rows.reduce((m, r) => { const k = f(r); if (k != null) m[k] = (m[k] || 0) + 1; return m; }, {});
  return {
    transactions: rows.length,
    by_mainnet_status: by((r) => r.status),
    replayable: rows.filter((r) => r.replayable).length,
    not_replayable: by((r) => (r.replayable ? null : r.reason)),
    predicted: by((r) => (r.notes && r.notes.length ? r.notes.join('+') : null)),
    result: by((r) => r.compare?.result ?? null),
    mismatch_categories: by((r) => (r.compare?.result === 'mismatch' ? r.compare.category : null)),
  };
}

// ---------------------------------------------------------------- I/O --

const arg = (k, d) => { const i = process.argv.indexOf(`--${k}`); return i > 0 ? process.argv[i + 1] : d; };
const flag = (k) => process.argv.includes(`--${k}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function postJson(url, body, timeoutMs = 15000) {
  const ac = new AbortController(); const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const r = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: ac.signal });
    const text = await r.text(); let json; try { json = JSON.parse(text); } catch { json = undefined; }
    return { status: r.status, json, text };
  } catch (e) { return { status: 0, error: String(e.message || e) }; } finally { clearTimeout(t); }
}
function makeClient(base, api) {
  base = base.replace(/\/$/, '');
  if (api === 'native') {
    let id = 0;
    const rpc = async (method, params) => {
      const r = await postJson(base, { jsonrpc: '2.0', id: ++id, method, params });
      if (r.json?.error) return { error: r.json.error.message || JSON.stringify(r.json.error) };
      if (r.json && 'result' in r.json) return { result: r.json.result };
      return { error: r.error || `HTTP ${r.status} ${(r.text || '').slice(0, 200)}` };
    };
    return {
      info: async () => { const r = await rpc('pulsevm.getInfo', {}); return r.result ? { chain_id: r.result.chain_id, head: Number(r.result.head_block_num) } : { error: r.error }; },
      block: async (n) => { const r = await rpc('pulsevm.getBlock', { block_num_or_id: String(n) }); return r.result ? { block: r.result } : { error: r.error }; },
      send: async (p) => { const r = await rpc('pulsevm.issueTx', p); return r.result ? { ok: true, result: r.result } : { ok: false, error: r.error }; },
    };
  }
  const v1 = async (name, body) => postJson(`${base}/v1/chain/${name}`, body);
  return {
    info: async () => { const r = await v1('get_info', {}); return r.json && r.status === 200 ? { chain_id: r.json.chain_id, head: Number(r.json.head_block_num) } : { error: r.error || `HTTP ${r.status}` }; },
    block: async (n) => { const r = await v1('get_block', { block_num_or_id: String(n) }); return r.status === 200 && r.json ? { block: r.json } : { error: r.error || JSON.stringify(r.json ?? r.text).slice(0, 300) }; },
    send: async (p) => { const r = await v1('send_transaction', p); return r.status >= 200 && r.status < 300 ? { ok: true, result: r.json } : { ok: false, error: r.error || JSON.stringify(r.json ?? r.text).slice(0, 600) }; },
  };
}
const blockId = (b) => b?.id ?? b?.block_id ?? null;

async function main() {
  const SOURCE = arg('source'), BLOCKS_FILE = arg('blocks-file'), TARGET = arg('target'), TARGET_API = arg('target-api', 'v1');
  const H = Number(arg('cut-height')), CUT_ID = arg('cut-id'), N = Number(arg('blocks', '100'));
  const RATE = Number(arg('rate', '5')), INCL_TIMEOUT = Number(arg('inclusion-timeout', '120')), OUT = arg('out');
  const SEND = flag('send'), I_UNDERSTAND = flag('shadow-i-understand');
  if ((!SOURCE && !BLOCKS_FILE) || !Number.isInteger(H) || H <= 0 || (SEND && !TARGET) || !['v1', 'native'].includes(TARGET_API)) {
    console.error('usage: shadow-replay.mjs (--source URL | --blocks-file f.json) --cut-height H [--blocks N] [--out f.json]\n' +
      '       [--target URL --target-api v1|native --cut-id ID --send --shadow-i-understand --rate TPS --inclusion-timeout S --deny-host h1,h2]');
    process.exit(2);
  }
  const report = { tool: 'shadow-replay', version: VERSION, generated_at: new Date().toISOString(), mode: SEND ? 'send' : 'dry-run', cut: { height: H, id: CUT_ID || null } };

  // 1. Source blocks H+1 … H+N (read only).
  let blocks;
  let sourceChainId = null;
  if (BLOCKS_FILE) {
    blocks = JSON.parse(readFileSync(BLOCKS_FILE, 'utf8'));
    if (!Array.isArray(blocks)) blocks = blocks.blocks || [];
    sourceChainId = arg('source-chain-id', null);
    report.source = { file: BLOCKS_FILE, chain_id: sourceChainId };
  } else {
    const src = makeClient(SOURCE, 'v1');
    const info = await src.info();
    if (info.error) { console.error(`source get_info failed: ${info.error}`); process.exit(2); }
    sourceChainId = info.chain_id;
    report.source = { url: SOURCE, chain_id: info.chain_id, head: info.head };
    if (CUT_ID) {
      const b = await src.block(H);
      if (b.error || String(blockId(b.block)).toLowerCase() !== CUT_ID.toLowerCase()) { console.error(`source block at H is ${blockId(b.block) ?? b.error}, not --cut-id ${CUT_ID}`); process.exit(2); }
    }
    blocks = [];
    for (let n = H + 1; n <= Math.min(H + N, info.head); n++) {
      const b = await src.block(n);
      if (b.error) { console.error(`source block ${n}: ${b.error}`); process.exit(2); }
      blocks.push(b.block);
    }
  }
  blocks = blocks.filter((b) => Number(b.block_num) > H).sort((a, b) => a.block_num - b.block_num);
  const rows = blocks.flatMap((b) => extractTxs(b, H));
  report.range = { from: blocks[0]?.block_num ?? null, to: blocks.at(-1)?.block_num ?? null, blocks: blocks.length };
  const sourceAfterId = blockId(blocks.find((b) => Number(b.block_num) === H + 1));

  // 2. Dry run stops here: classification only, the target is never contacted.
  if (!SEND) return finish(report, rows, OUT);

  // 3. Target safety, then send in mainnet order at the given rate.
  const tgt = makeClient(TARGET, TARGET_API);
  const ti = await tgt.info();
  if (ti.error) { console.error(`target info failed: ${ti.error}`); process.exit(2); }
  const atH = await tgt.block(H);
  const afterH = ti.head > H ? await tgt.block(H + 1) : {};
  const safety = checkTargetSafety({
    target: { host: new URL(TARGET).hostname, chain_id: ti.chain_id, head: ti.head, block_at_cut_id: blockId(atH.block), block_after_cut_id: blockId(afterH.block) },
    source: { chain_id: sourceChainId, block_after_cut_id: sourceAfterId },
    cutHeight: H, cutId: CUT_ID, iUnderstand: I_UNDERSTAND, denyHosts: (arg('deny-host', '') || '').split(',').filter(Boolean),
  });
  report.target = { url: TARGET, api: TARGET_API, chain_id: ti.chain_id, head_at_start: ti.head };
  report.safety = safety;
  if (!safety.ok) { console.error(`REFUSED:\n- ${safety.reasons.join('\n- ')}`); finish(report, rows, OUT, 2); return; }

  const sent = new Map();
  const gap = RATE > 0 ? 1000 / RATE : 0;
  for (const r of rows) {
    if (!r.replayable) continue;
    const t0 = Date.now();
    const res = await tgt.send(r.payload);
    sent.set(r.id.toLowerCase(), res.ok ? { accepted: true } : { rejected: true, error: res.error });
    const wait = gap - (Date.now() - t0); if (wait > 0) await sleep(wait);
  }

  // 4. Confirm inclusion by reading the shadow chain's blocks after H.
  const want = new Set([...sent].filter(([, v]) => v.accepted).map(([k]) => k));
  const included = new Map();
  let next = H + 1; const deadline = Date.now() + INCL_TIMEOUT * 1000;
  while (want.size && Date.now() < deadline) {
    const info = await tgt.info();
    while (!info.error && next <= info.head) {
      const b = await tgt.block(next);
      if (b.error) break;
      for (const [id, st] of extractBlockIds(b.block)) if (want.delete(id)) included.set(id, { block: next, status: st });
      next++;
    }
    if (want.size) await sleep(1000);
  }
  report.target.scanned_to = next - 1;
  for (const r of rows) {
    const s = sent.get(String(r.id).toLowerCase());
    const inc = included.get(String(r.id).toLowerCase());
    const shadow = s && (s.rejected ? s : { accepted: true, included: !!inc, included_block: inc?.block ?? null, included_status: inc?.status ?? null });
    r.shadow = shadow ?? null;
    r.compare = compareOutcome(r, shadow);
  }
  const mism = rows.filter((r) => r.compare?.result === 'mismatch').length;
  finish(report, rows, OUT, mism ? 1 : 0);
}

function finish(report, rows, out, code = 0) {
  for (const r of rows) delete r.payload; // the report never carries the transactions themselves
  if (report.mode === 'dry-run') for (const r of rows) r.compare = compareOutcome(r, null);
  report.summary = summarise(rows);
  report.transactions = rows;
  const text = JSON.stringify(report, null, 1);
  if (out) writeFileSync(out, text);
  console.log(JSON.stringify({ mode: report.mode, range: report.range, summary: report.summary, safety: report.safety?.ok ?? null }, null, 1));
  process.exitCode = code;
}

if (import.meta.url === pathToFileURL(process.argv[1] || '').href) main().catch((e) => { console.error(e?.stack || e); process.exit(2); });
