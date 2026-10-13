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
//
// What tells the shadow apart from mainnet is the block AFTER the cut (H+1). Mainnet's H+1 is never taken from
// --source or --blocks-file (those could be anything, even the shadow itself): it is AUTHENTICATED independently,
// from at least two reference endpoints on distinct hostnames that agree. For the mainnet chain_id only the built-in
// pinned list of public XPR mainnet APIs can vouch; a --mainnet-endpoint adds evidence that can only REFUSE (any
// disagreement refuses) and never counts toward the quorum (round 5). Any other chain_id: --reference-endpoint.
// No request follows an HTTP redirect: a 3xx on any read is an error, and a 3xx on a send stops the whole run.
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
// The pinned reference endpoints for XPR mainnet: where mainnet's own H+1 is read, independently of --source. Only
// these vouch; --mainnet-endpoint adds answers that can only make the check refuse. Nothing removes from the list.
export const MAINNET_REFERENCE_ENDPOINTS = PUBLIC_MAINNET_HOSTS.map((h) => `https://${h}`);
// A reference H+1 needs this many vouching endpoints on DISTINCT HOSTNAMES that agree (and no valid answer, from any
// endpoint, that disagrees). Two ports on one host are one voice.
export const MIN_REFERENCE_AGREEMENT = 2;
const STATUS_BY_NUM = ['executed', 'soft_fail', 'hard_fail', 'delayed', 'expired'];
// The only receipt statuses Leap has. Anything else (a number outside the enum, an empty or new string) is UNKNOWN:
// two equal unsupported values say nothing about the outcome (r30 round-3 review: `status_9` vs `status_9` matched).
export const KNOWN_STATUSES = new Set(STATUS_BY_NUM);

// ---------------------------------------------------------------- pure helpers (tested) --

/** Normalise a receipt status (Leap JSON string or numeric enum) to a name. */
export function statusName(s) {
  if (typeof s === 'number') return Number.isInteger(s) && STATUS_BY_NUM[s] ? STATUS_BY_NUM[s] : 'unknown';
  const v = String(s ?? '').trim().toLowerCase();
  return KNOWN_STATUSES.has(v) ? v : 'unknown';
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
    // A receipt without a status is UNKNOWN, never assumed executed (r30 round-2 review: a bare id matched).
    if (id) ids.set(id.toLowerCase(), statusName(r.status));
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
 * Authenticate the reference chain's blocks at H and H+1 from independent endpoints, never from --source.
 * answers: [{endpoint, host (hostname), chain_id, block_at_cut_id, block_after_cut_id, error}]. excludeHosts: hostnames
 * that may not vouch (the source's and the target's). authorities: when given (XPR mainnet: the exact pinned https
 * URLs), only answers whose normalized endpoint IS one of those URLs count toward the quorum (one voice per hostname);
 * every other valid answer, including one on a pinned hostname with another scheme, port or path, is still compared
 * and can only refuse. A valid answer reports the expected chain_id and both blocks. Fail-closed:
 *  - fewer than `minAgree` vouching answers on distinct hostnames → refuse;
 *  - any two valid answers that disagree on H or H+1 → refuse (ambiguity is never resolved by majority);
 *  - a valid answer whose block at H is not --cut-id → refuse (the cut is not this chain's H).
 * Returns {ok, block_after_cut_id, agreeing[], reasons[], answers[]}.
 */
export function decideReference({ answers, chainId, cutId, minAgree = MIN_REFERENCE_AGREEMENT, excludeHosts = [], authorities = null }) {
  const lc = (v) => (v == null ? null : String(v).toLowerCase());
  const reasons = [];
  const excluded = new Set(excludeHosts.map(lc).filter(Boolean));
  // Round 6: authority is EXACT provenance, never a hostname. An answer vouches only when its endpoint, strictly
  // normalized, IS one of the authority URLs (XPR mainnet: the built-in pinned https URLs). An added endpoint on a
  // pinned hostname (another scheme, port or path) is compared like any other answer, so it can refuse, but never vouches.
  const vouchers = authorities ? new Set(authorities.map(normalizeUrl).filter(Boolean)) : null;
  const valid = [];
  const notes = [];
  for (const a of answers) {
    // The hostname comes from the endpoint URL itself, never from a field the answer carries.
    const host = hostOf(a.endpoint) ?? lc(a.host);
    if (a.error) { notes.push(`${a.endpoint}: ${a.error}`); continue; }
    if (!host) { notes.push(`${a.endpoint}: not a URL`); continue; }
    if (excluded.has(host)) { notes.push(`${a.endpoint}: is the source or the target, so it cannot vouch independently`); continue; }
    if (lc(a.chain_id) !== lc(chainId)) { notes.push(`${a.endpoint}: reports chain_id ${a.chain_id ?? 'none'}, not ${chainId}`); continue; }
    if (!a.block_at_cut_id || !a.block_after_cut_id) { notes.push(`${a.endpoint}: did not show its blocks at H and H+1`); continue; }
    valid.push({ ...a, host });
  }
  // Every valid answer is compared, vouching or not: any disagreement refuses.
  const afterIds = new Set(valid.map((a) => lc(a.block_after_cut_id)));
  const atIds = new Set(valid.map((a) => lc(a.block_at_cut_id)));
  if (afterIds.size > 1 || atIds.size > 1) {
    reasons.push(`reference endpoints disagree on the blocks at H/H+1 (${valid.map((a) => `${a.endpoint}: ${a.block_at_cut_id}/${a.block_after_cut_id}`).join('; ')}): refusing rather than guessing`);
  }
  if (cutId && valid.some((a) => lc(a.block_at_cut_id) !== lc(cutId))) {
    reasons.push(`the reference chain's block at H is not --cut-id ${cutId}: the cut is not block H of chain ${chainId}`);
  }
  // Count vouching answers: provenance first, then one voice per hostname.
  const seen = new Set();
  const counted = [];
  for (const a of valid) {
    if (vouchers && !vouchers.has(normalizeUrl(a.endpoint))) continue;
    if (seen.has(a.host)) continue;
    seen.add(a.host);
    counted.push(a);
  }
  if (counted.length < minAgree) {
    reasons.push(`only ${counted.length} ${vouchers ? 'pinned ' : ''}reference endpoint(s) on distinct hostnames answered for chain ${chainId} (need ${minAgree} that agree${vouchers ? '; only the exact built-in pinned https URLs vouch, an added --mainnet-endpoint never counts, whatever its hostname' : ''})${notes.length ? `: ${notes.join('; ')}` : ''}`);
  }
  const ok = reasons.length === 0;
  return { ok, block_after_cut_id: ok ? lc(counted[0].block_after_cut_id) : null, agreeing: counted.map((a) => a.host), reasons, notes };
}

/**
 * Decide whether a target may receive replayed mainnet transactions. Returns {ok, reasons[], checks{}}.
 * Lineage must be PROVEN, never inferred from a missing answer: the target must be on the cut (its block at H is
 * --cut-id) and must have produced its OWN block H+1 (it exists and differs from mainnet's H+1, which the source
 * must supply). Any fetch failure, missing block or unknown value refuses.
 * target: {host, chain_id, head, block_at_cut_id, block_after_cut_id, block_at_cut_error, block_after_cut_error};
 * source: {chain_id, block_after_cut_id}.
 */
export function checkTargetSafety({ target, source, reference, cutHeight, cutId, iUnderstand, denyHosts = [] }) {
  const reasons = [];
  const host = String(target.host || '').toLowerCase();
  const lc = (v) => (v == null ? null : String(v).toLowerCase());
  const checks = { host, target_chain_id: target.chain_id ?? null, source_chain_id: source.chain_id ?? null };
  const deny = [...PUBLIC_MAINNET_HOSTS, ...denyHosts.map((h) => String(h).toLowerCase())];
  if (deny.some((h) => host === h || host.endsWith(`.${h}`))) reasons.push(`target host ${host} is a public XPR mainnet API`);
  if (!target.chain_id) reasons.push('target did not report a chain_id');
  if (!source.chain_id) reasons.push('the source chain_id is unknown (with --blocks-file pass --source-chain-id): the target cannot be checked against it');
  if (source.chain_id && target.chain_id && lc(source.chain_id) !== lc(target.chain_id)) {
    reasons.push(`target chain_id ${target.chain_id} differs from the source's ${source.chain_id}: replayed signatures cannot verify there`);
  }
  const mainnet = lc(target.chain_id) === XPR_MAINNET_CHAIN_ID;
  checks.mainnet_chain_id = mainnet;
  if (mainnet && !iUnderstand) reasons.push('target reports the XPR mainnet chain_id: pass --shadow-i-understand to confirm it is a private shadow chain');
  // 1. On the cut: the target's block at H is the cut block.
  if (!cutId) reasons.push('--cut-id (the block id at H) is required to send: lineage cannot be checked without it');
  else if (!target.block_at_cut_id) reasons.push(`target cannot show its block at H=${cutHeight}${target.block_at_cut_error ? ` (${target.block_at_cut_error})` : ''}: lineage unproven`);
  else if (lc(target.block_at_cut_id) !== lc(cutId)) reasons.push(`target block at H is ${target.block_at_cut_id}, not the cut ${cutId}: not a shadow of this cut`);
  // 2. Its own chain after the cut: a real mainnet node also has the cut block at H; what tells it apart is the
  //    block AFTER H. The shadow must already have produced one, and it must not be mainnet's.
  if (!source.block_after_cut_id) reasons.push(`mainnet's block H+1=${cutHeight + 1} is unknown (the source or blocks file must include it): cannot tell the target from mainnet`);
  if (!target.block_after_cut_id) reasons.push(`target cannot show its block H+1=${cutHeight + 1}${target.block_after_cut_error ? ` (${target.block_after_cut_error})` : ''}: lineage unproven (a shadow must have produced its first block before replay)`);
  if (source.block_after_cut_id && target.block_after_cut_id && lc(target.block_after_cut_id) === lc(source.block_after_cut_id)) {
    reasons.push(`target block H+1 equals mainnet's (${source.block_after_cut_id}): the target is following the real chain, not a shadow`);
  }
  // 3. Mainnet's H+1 itself is authenticated independently of the source (round 4: swapping source and target made a
  //    real mainnet target pass, because the "mainnet" H+1 came from the supplied source, i.e. the shadow).
  const refId = reference?.ok ? lc(reference.block_after_cut_id) : null;
  if (!reference || !reference.ok) {
    reasons.push(`mainnet's block H+1 could not be authenticated independently of the source: ${(reference?.reasons || ['no reference endpoints were asked']).join('; ')}`);
  } else {
    if (source.block_after_cut_id && lc(source.block_after_cut_id) !== refId) {
      reasons.push(`the source's block H+1 (${source.block_after_cut_id}) is not the authenticated reference H+1 (${refId}): the source is not the chain it claims to be (a shadow passed as the source?)`);
    }
    if (target.block_after_cut_id && lc(target.block_after_cut_id) === refId) {
      reasons.push(`target block H+1 equals the authenticated reference H+1 (${refId}): the target is following the real chain, not a shadow`);
    }
  }
  checks.reference_block_after_cut_id = refId;
  checks.reference_agreeing = reference?.agreeing ?? [];
  checks.block_at_cut_id = target.block_at_cut_id ?? null;
  checks.block_after_cut_id = target.block_after_cut_id ?? null;
  checks.source_block_after_cut_id = source.block_after_cut_id ?? null;
  return { ok: reasons.length === 0, reasons, checks };
}

// What each mainnet receipt status means, and which shadow outcomes are the SAME outcome.
//   executed  : ran, state changed            → shadow must include it as executed
//   delayed   : accepted and queued (runs later) → shadow must include it as delayed; a rejection is a mismatch
//   soft_fail : failed, onerror handled it     → shadow must include it as soft_fail
//   hard_fail : included, failed, no effects   → shadow included as hard_fail, or rejected for an execution failure
//               (an assertion) — not for admission reasons (auth, TAPOS, expiry, duplicate, resources)
//   expired   : included only to retire it     → shadow included as expired, or rejected as expired
// Only hard_fail and expired mean "not executed and not queued", so only they can match a rejection, and only
// with the compatible reason below. Every other combination is a mismatch with a category.
const REJECTION_MATCHES = { hard_fail: new Set(['assertion']), expired: new Set(['expired']) };

/** Compare one transaction's mainnet outcome with the shadow outcome. */
export function compareOutcome(tx, shadow) {
  if (!tx.replayable) return { result: 'skipped', category: tx.reason === 'deferred' ? 'deferred' : 'not_replayable' };
  if (!shadow) return { result: 'not_sent', category: 'not_sent' };
  const status = String(tx.status);
  // An outcome nobody knows is neither a match nor a mismatch: it is reported as unknown and never counted as a match.
  // Only Leap's own statuses are outcomes; anything else is unknown, on either side.
  if (!KNOWN_STATUSES.has(status)) return { result: 'unknown', category: 'mainnet_status_unknown' };
  if (shadow.rejected) {
    const category = classifyError(shadow.error);
    if (REJECTION_MATCHES[status]?.has(category)) return { result: 'match', category };
    if (status === 'executed') return { result: 'mismatch', category };
    return { result: 'mismatch', category: `mainnet_${status}_vs_rejected_${category}` };
  }
  if (!shadow.included) return { result: 'mismatch', category: 'missing' };
  if (shadow.included_status == null || !KNOWN_STATUSES.has(String(shadow.included_status))) {
    return { result: 'unknown', category: 'shadow_status_unknown' };
  }
  const same = shadow.included_status === status;
  return { result: same ? 'match' : 'mismatch', category: same ? 'match' : `status_${status}_vs_${shadow.included_status}` };
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

const argOf = (argv) => (k, d) => { const i = argv.indexOf(`--${k}`); return i > 0 ? argv[i + 1] : d; };
const flagOf = (argv) => (k) => argv.includes(`--${k}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * POST JSON and NEVER follow a redirect (round 4: a target could pass every read and then 307 the send to a real
 * mainnet API, and fetch would have forwarded the signed body). A 3xx comes back as {redirected: true, error}.
 */
export async function postJson(url, body, timeoutMs = 15000) {
  const ac = new AbortController(); const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const r = await fetch(url, { method: 'POST', redirect: 'manual', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: ac.signal });
    if ((r.status >= 300 && r.status < 400) || r.type === 'opaqueredirect') {
      try { await r.body?.cancel(); } catch { /* nothing to drain */ }
      return { status: r.status, redirected: true, error: `redirect refused: HTTP ${r.status} to ${r.headers.get('location') ?? '(no location)'}` };
    }
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
      if (r.redirected) return { error: r.error, redirected: true };
      if (r.json?.error) return { error: r.json.error.message || JSON.stringify(r.json.error) };
      if (r.json && 'result' in r.json) return { result: r.json.result };
      return { error: r.error || `HTTP ${r.status} ${(r.text || '').slice(0, 200)}` };
    };
    return {
      info: async () => { const r = await rpc('pulsevm.getInfo', {}); return r.result ? { chain_id: r.result.chain_id, head: Number(r.result.head_block_num) } : { error: r.error }; },
      block: async (n) => { const r = await rpc('pulsevm.getBlock', { block_num_or_id: String(n) }); return r.result ? { block: r.result } : { error: r.error }; },
      send: async (p) => { const r = await rpc('pulsevm.issueTx', p); return r.result ? { ok: true, result: r.result } : { ok: false, error: r.error, redirected: !!r.redirected }; },
    };
  }
  const v1 = async (name, body) => postJson(`${base}/v1/chain/${name}`, body);
  return {
    info: async () => { const r = await v1('get_info', {}); return r.json && r.status === 200 ? { chain_id: r.json.chain_id, head: Number(r.json.head_block_num) } : { error: r.error || `HTTP ${r.status}`, redirected: !!r.redirected }; },
    block: async (n) => { const r = await v1('get_block', { block_num_or_id: String(n) }); return r.status === 200 && r.json ? { block: r.json } : { error: r.error || JSON.stringify(r.json ?? r.text).slice(0, 300), redirected: !!r.redirected }; },
    send: async (p) => { const r = await v1('send_transaction', p); return r.status >= 200 && r.status < 300 ? { ok: true, result: r.json } : { ok: false, error: r.error || JSON.stringify(r.json ?? r.text).slice(0, 600), redirected: !!r.redirected }; },
  };
}
const blockId = (b) => b?.id ?? b?.block_id ?? null;
const argAllOf = (argv) => (k) => argv.flatMap((v, i) => (v === `--${k}` && argv[i + 1] ? argv[i + 1].split(',') : [])).map((x) => x.trim()).filter(Boolean);
// Independence is judged per HOSTNAME (round 5): two ports on one host are one voice, never two.
export const hostOf = (u) => { try { return new URL(u).hostname.toLowerCase().replace(/^\[|\]$/g, '') || null; } catch { return null; } };

/**
 * Strict URL normalization for provenance (round 6): lowercase scheme and host, default port elided, trailing slash
 * ignored; a URL with userinfo, a query or a fragment, or that does not parse, normalizes to null (it can never match).
 */
export function normalizeUrl(u) {
  let x;
  try { x = new URL(String(u)); } catch { return null; }
  if (x.username || x.password || x.search || x.hash) return null;
  if (x.protocol !== 'https:' && x.protocol !== 'http:') return null;
  const host = x.hostname.toLowerCase();
  if (!host) return null;
  const path = x.pathname.replace(/\/+$/, '');
  return `${x.protocol}//${host}${x.port ? `:${x.port}` : ''}${path}`;
}

/**
 * Which endpoints are asked about chain `chainId`'s blocks at H and H+1, and which of them may vouch. XPR mainnet: the
 * pinned list vouches (authorities = its exact normalized https URLs, never its hostnames); added --mainnet-endpoint
 * answers are compared but never vouch, even on a pinned hostname.
 * Any other chain_id: the user-given --reference-endpoint list vouches (no authority list). `pinned` exists for tests
 * that call run() in-process; the CLI never sets it, and no environment variable can change it.
 */
export function referenceEndpoints({ chainId, mainnetAdded = [], referenceGiven = [], pinned = MAINNET_REFERENCE_ENDPOINTS }) {
  if (String(chainId || '').toLowerCase() === XPR_MAINNET_CHAIN_ID) {
    // Only https pinned URLs can vouch, by exact (normalized) URL: provenance, not hostname (round 6).
    const authorities = pinned.map(normalizeUrl).filter((u) => u && u.startsWith('https://'));
    return { endpoints: [...pinned, ...mainnetAdded], authorities, pinned: true };
  }
  return { endpoints: referenceGiven, authorities: null, pinned: false };
}

/** Ask every reference endpoint for its chain_id and its blocks at H and H+1 (no redirects followed). */
export async function askReferences(endpoints, H) {
  return Promise.all(endpoints.map(async (endpoint) => {
    const host = hostOf(endpoint);
    if (!host) return { endpoint, host: null, error: 'not a URL' };
    const c = makeClient(endpoint, 'v1');
    const info = await c.info();
    if (info.error) return { endpoint, host, error: `get_info: ${info.error}` };
    if (!(info.head > H)) return { endpoint, host, chain_id: info.chain_id, error: `head ${info.head} is not past H` };
    const [at, after] = [await c.block(H), await c.block(H + 1)];
    if (at.error || after.error) return { endpoint, host, chain_id: info.chain_id, error: `get_block: ${at.error || after.error}` };
    return { endpoint, host, chain_id: info.chain_id, block_at_cut_id: blockId(at.block), block_after_cut_id: blockId(after.block) };
  }));
}

/**
 * The CLI, callable in-process. Returns the exit code. opts (tests only; the CLI passes none):
 * pinnedReferenceEndpoints replaces the pinned mainnet list, referenceFetcher(endpoints, H) replaces the network
 * reads of the references, log/err replace console.log/console.error.
 */
export async function run(argv = process.argv, opts = {}) {
  const arg = argOf(argv), flag = flagOf(argv), argAll = argAllOf(argv);
  const log = opts.log ?? ((m) => console.log(m)), err = opts.err ?? ((m) => console.error(m));
  const SOURCE = arg('source'), BLOCKS_FILE = arg('blocks-file'), TARGET = arg('target'), TARGET_API = arg('target-api', 'v1');
  const H = Number(arg('cut-height')), CUT_ID = arg('cut-id'), N = Number(arg('blocks', '100'));
  const RATE = Number(arg('rate', '5')), INCL_TIMEOUT = Number(arg('inclusion-timeout', '120')), OUT = arg('out');
  const SEND = flag('send'), I_UNDERSTAND = flag('shadow-i-understand'), ALLOW_INCONCLUSIVE = flag('allow-inconclusive');
  if ((!SOURCE && !BLOCKS_FILE) || !Number.isInteger(H) || H <= 0 || (SEND && !TARGET) || !['v1', 'native'].includes(TARGET_API)) {
    err('usage: shadow-replay.mjs (--source URL | --blocks-file f.json) --cut-height H [--blocks N] [--out f.json]\n' +
      '       [--target URL --target-api v1|native --cut-id ID --send --shadow-i-understand --rate TPS --inclusion-timeout S --deny-host h1,h2 --allow-inconclusive]\n' +
      '       [--mainnet-endpoint URL (extra mainnet evidence: can only refuse) | --reference-endpoint URL (≥ 2 hosts, any other chain_id)]');
    return 2;
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
    if (info.error) { err(`source get_info failed: ${info.error}`); return 2; }
    sourceChainId = info.chain_id;
    report.source = { url: SOURCE, chain_id: info.chain_id, head: info.head };
    if (CUT_ID) {
      const b = await src.block(H);
      if (b.error || String(blockId(b.block)).toLowerCase() !== CUT_ID.toLowerCase()) { err(`source block at H is ${blockId(b.block) ?? b.error}, not --cut-id ${CUT_ID}`); return 2; }
    }
    blocks = [];
    for (let n = H + 1; n <= Math.min(H + N, info.head); n++) {
      const b = await src.block(n);
      if (b.error) { err(`source block ${n}: ${b.error}`); return 2; }
      blocks.push(b.block);
    }
  }
  blocks = blocks.filter((b) => Number(b.block_num) > H).sort((a, b) => a.block_num - b.block_num);
  const rows = blocks.flatMap((b) => extractTxs(b, H));
  report.range = { from: blocks[0]?.block_num ?? null, to: blocks.at(-1)?.block_num ?? null, blocks: blocks.length };
  const sourceAfterId = blockId(blocks.find((b) => Number(b.block_num) === H + 1));

  // 2. Dry run stops here: classification only, the target is never contacted.
  if (!SEND) return finish(report, rows, OUT, 0, log);

  // 3. Target safety, then send in mainnet order at the given rate.
  const tgt = makeClient(TARGET, TARGET_API);
  const ti = await tgt.info();
  if (ti.error) { err(`target info failed: ${ti.error}`); return 2; }
  const atH = await tgt.block(H);
  const afterH = ti.head > H ? await tgt.block(H + 1) : { error: `target head ${ti.head} is not past H` };
  // Mainnet's H+1 from independent references, never from the source (round 4).
  const sourceHost = SOURCE ? hostOf(SOURCE) : null;
  const refs = referenceEndpoints({ chainId: sourceChainId, mainnetAdded: argAll('mainnet-endpoint'), referenceGiven: argAll('reference-endpoint'),
    ...(opts.pinnedReferenceEndpoints ? { pinned: opts.pinnedReferenceEndpoints } : {}) });
  const fetchRefs = opts.referenceFetcher ?? askReferences;
  const refAnswers = refs.endpoints.length ? await fetchRefs(refs.endpoints, H) : [];
  const reference = decideReference({ answers: refAnswers, chainId: sourceChainId, cutId: CUT_ID, excludeHosts: [hostOf(TARGET), sourceHost], authorities: refs.authorities });
  if (!refs.endpoints.length) reference.reasons.unshift('no reference endpoints: for a chain_id other than XPR mainnet pass --reference-endpoint (at least 2, on distinct hostnames)');
  report.reference = { pinned: refs.pinned, endpoints: refs.endpoints, authorities: refs.authorities, ok: reference.ok, agreeing: reference.agreeing, block_after_cut_id: reference.block_after_cut_id, reasons: reference.reasons, notes: reference.notes };
  const safety = checkTargetSafety({
    reference,
    target: {
      host: new URL(TARGET).hostname, chain_id: ti.chain_id, head: ti.head,
      block_at_cut_id: atH.error ? null : blockId(atH.block), block_at_cut_error: atH.error ?? null,
      block_after_cut_id: afterH.error ? null : blockId(afterH.block), block_after_cut_error: afterH.error ?? null,
    },
    source: { chain_id: sourceChainId, block_after_cut_id: sourceAfterId },
    cutHeight: H, cutId: CUT_ID, iUnderstand: I_UNDERSTAND, denyHosts: (arg('deny-host', '') || '').split(',').filter(Boolean),
  });
  report.target = { url: TARGET, api: TARGET_API, chain_id: ti.chain_id, head_at_start: ti.head };
  report.safety = safety;
  if (!safety.ok) { err(`REFUSED:\n- ${safety.reasons.join('\n- ')}`); return finish(report, rows, OUT, 2, log); }

  const sent = new Map();
  const gap = RATE > 0 ? 1000 / RATE : 0;
  for (const r of rows) {
    if (!r.replayable) continue;
    const t0 = Date.now();
    const res = await tgt.send(r.payload);
    if (res.redirected) {
      // The target tried to send the signed body elsewhere: stop everything, nothing more is sent anywhere.
      report.aborted = `target redirected a send: ${res.error}`;
      err(`REFUSED: ${report.aborted}; stopping the replay`);
      return finish(report, rows, OUT, 2, log);
    }
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
  // A send whose outcomes are not all known is not a pass (round 3): exit 3 unless --allow-inconclusive. A mismatch
  // always wins (exit 1).
  const inconclusive = rows.filter((r) => ['unknown', 'not_sent'].includes(r.compare?.result)).length;
  report.verdict = mism ? 'mismatch' : inconclusive ? 'inconclusive' : 'match';
  report.inconclusive = inconclusive;
  return finish(report, rows, OUT, mism ? 1 : inconclusive && !ALLOW_INCONCLUSIVE ? 3 : 0, log);
}

function finish(report, rows, out, code = 0, log = (m) => console.log(m)) {
  for (const r of rows) delete r.payload; // the report never carries the transactions themselves
  if (report.mode === 'dry-run') for (const r of rows) r.compare = compareOutcome(r, null);
  report.summary = summarise(rows);
  report.transactions = rows;
  const text = JSON.stringify(report, null, 1);
  if (out) writeFileSync(out, text);
  log(JSON.stringify({ mode: report.mode, range: report.range, summary: report.summary, verdict: report.verdict ?? null,
    safety: report.safety?.ok ?? null }, null, 1));
  return code;
}

if (import.meta.url === pathToFileURL(process.argv[1] || '').href) {
  run(process.argv).then((code) => { process.exitCode = code; }, (e) => { console.error(e?.stack || e); process.exitCode = 2; });
}
