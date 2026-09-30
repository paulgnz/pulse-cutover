#!/usr/bin/env node
// diff.mjs — send every corpus request to two /v1 endpoints and compare the answers.
//
//   node tools/conformance/diff.mjs --a <url> --b <url> --corpus corpus.jsonl
//        [--a-history <url>] [--b-history <url>]   where /v1/history/* goes (default: --a / --b)
//        [--report out.md|out.html] [--json out.json] [--allow allow.json] [--frozen] [--rps 4]
//        [--only endpoint[,endpoint]] [--retries 2] [--coverage docs/V1-COVERAGE.md] [--weights usage-weights.json]
//
// A is the reference (Leap 5 nodeos + Hyperion), B the candidate (the PulseVM edge + federator).
// Each request is sent to both at the same moment, both answers are normalized (see lib.mjs: volatile paths are
// compared by shape, ignored paths dropped, unordered arrays sorted, error bodies reduced to what clients match
// on) and classified. On a live chain a mismatch is retried: equal on retry = "equal-on-retry" (the chain moved
// between the two reads); A disagreeing with itself = "unstable" (cannot be judged). --frozen (the pre-flip rig,
// both sides at the same state at H) compares `live` paths strictly and does not retry.
//
// Exit 0 = no unexpected difference; 1 = DIFF / error-shape-diff / transport-error; 2 = usage error.
// No dependencies (Node >= 18).
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { send, setRate, parseCoverage, classify, allowedBy, normalize, specOf, equal, CLASSES, FAILING, truncate, buildTx } from './lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const opt = (k, d) => { const i = argv.indexOf(`--${k}`); return i >= 0 ? argv[i + 1] : d; };
const flag = (k) => argv.includes(`--${k}`);
const A = opt('a'), B = opt('b'), CORPUS = opt('corpus');
if (!A || !B || !CORPUS) {
  console.error('usage: diff.mjs --a <url> --b <url> --corpus corpus.jsonl [--a-history url] [--b-history url] [--report f.md|f.html] [--json f.json] [--allow f.json] [--frozen] [--rps 4]');
  process.exit(2);
}
const AH = opt('a-history', A), BH = opt('b-history', B);
const FROZEN = flag('frozen'), RETRIES = FROZEN ? 0 : Number(opt('retries', 2));
setRate(opt('rps', 4));
const coverage = parseCoverage(readFileSync(opt('coverage', join(HERE, '../../docs/V1-COVERAGE.md')), 'utf8'));
const allow = opt('allow') ? JSON.parse(readFileSync(opt('allow'), 'utf8')) : [];
const only = opt('only') ? new Set(opt('only').split(',')) : null;
const weights = (() => { try { return JSON.parse(readFileSync(opt('weights', join(HERE, 'usage-weights.json')), 'utf8')); } catch { return {}; } })();

const all = readFileSync(CORPUS, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
const meta = (all.find((l) => l._meta) || {})._meta || {};
const corpus = all.filter((l) => !l._meta && (!only || only.has(l.tags[0])));

const norm = (r, req) => (r.error ? { __error: r.error } : { s: r.status, j: r.json === undefined ? r.text : normalize(r.json, specOf(req), { status: r.status, frozen: FROZEN }) });

// ---- run-time request resolution -------------------------------------------------------------------------------------
// `dynamic` fields (head-N, lib) and `tx_template` bodies are filled from A's get_info right before sending, so
// TAPOS-shaped reads ask for blocks clients would ask for and failed-transaction cases carry a live expiration.
let chainState = { t: 0 };
async function state() {
  if (Date.now() - chainState.t < 3000) return chainState;
  const r = await send(A, { method: 'POST', path: '/v1/chain/get_info', headers: { 'content-type': 'application/json' }, body: '{}' });
  if (!r.json || !r.json.head_block_num) throw new Error(`A get_info failed: ${r.error || r.status}`);
  chainState = { t: Date.now(), head: r.json.head_block_num, lib: r.json.last_irreversible_block_num, libId: r.json.last_irreversible_block_id };
  return chainState;
}
async function resolve(req) {
  if (!req.dynamic && !req.tx_template) return req;
  const st = await state();
  if (req.tx_template) return { ...req, body: JSON.stringify(buildTx(req.tx_template, st)) };
  const body = JSON.parse(req.body || '{}');
  for (const [k, v] of Object.entries(req.dynamic)) {
    const [what, as] = String(v).split(':');
    const m = what.match(/^(head|lib)(?:-(\d+))?$/);
    if (!m) continue;
    const n = (m[1] === 'head' ? st.head : st.lib) - Number(m[2] || 0);
    body[k] = as === 'string' ? String(n) : n;
  }
  return { ...req, body: JSON.stringify(body) };
}

async function one(req0) {
  const req = await resolve(req0);
  const hist = req.tags[2] === 'history';
  const ba = hist ? AH : A, bb = hist ? BH : B;
  let [a, b] = await Promise.all([send(ba, req), send(bb, req)]);
  let res = classify(req, a, b, coverage.get(req.tags[0]), { frozen: FROZEN });
  for (let i = 0; i < RETRIES && (FAILING.has(res.cls)); i++) {
    const [a2, b2] = await Promise.all([send(ba, req), send(bb, req)]);
    const r2 = classify(req, a2, b2, coverage.get(req.tags[0]), { frozen: FROZEN });
    if (!FAILING.has(r2.cls)) { res = { cls: r2.cls === 'identical' || r2.cls === 'equal-after-normalization' ? 'equal-on-retry' : r2.cls, note: r2.note }; a = a2; b = b2; break; }
    if (!a.error && !a2.error && !equal(norm(a, req), norm(a2, req))) { res = { ...r2, cls: 'unstable', note: 'A changed between two reads (moving state); not judged' }; a = a2; b = b2; break; }
    res = r2; a = a2; b = b2;
  }
  const allowed = FAILING.has(res.cls) || res.cls === 'unstable' ? allowedBy(allow, req, res) : null;
  if (allowed) res = { ...res, original: res.cls, cls: 'allowed', note: allowed.reason };
  return {
    id: req.id, endpoint: req.tags[0], style: req.tags[1], group: req.tags[2], method: req.method, path: req.path, body: truncate(req.body, 400),
    cls: res.cls, original: res.original, note: res.note || req.note, diff: res.diff,
    a: { status: a.status, error: a.error, ms: a.ms, edge: a.headers && a.headers['x-pulse-edge'], sample: truncate(a.text, 300) },
    b: { status: b.status, error: b.error, ms: b.ms, edge: b.headers && b.headers['x-pulse-edge'], sample: truncate(b.text, 300) },
  };
}

// ---- reports ---------------------------------------------------------------------------------------------------------------
function summarize(results) {
  const by = new Map();
  for (const r of results) {
    if (!by.has(r.endpoint)) by.set(r.endpoint, Object.fromEntries(CLASSES.map((c) => [c, 0])));
    by.get(r.endpoint)[r.cls]++;
  }
  const total = Object.fromEntries(CLASSES.map((c) => [c, results.filter((r) => r.cls === c).length]));
  return { by, total };
}
const SHORT = { identical: 'ident', 'equal-after-normalization': 'eq-norm', 'equal-on-retry': 'eq-retry', 'B-501-expected': 'B-501', 'B-partial-expected': 'B-partial',
  allowed: 'allowed', unstable: 'unstable', 'error-shape-diff': 'err-shape', DIFF: 'DIFF', 'transport-error': 'transport' };

function markdown(results, sum, hdr) {
  const L = [];
  L.push(`# /v1 conformance: A vs B`, '', `- A: \`${A}\`${AH !== A ? ` (history \`${AH}\`)` : ''}`, `- B: \`${B}\`${BH !== B ? ` (history \`${BH}\`)` : ''}`,
    `- corpus: ${corpus.length} requests from \`${meta.source || '?'}\` at head ${meta.head || '?'} (${meta.generated_at || '?'})`,
    `- mode: ${FROZEN ? 'frozen (live paths strict, no retry)' : `live (retries ${RETRIES})`}; run ${hdr.started} → ${hdr.finished}`,
    `- verdict: **${hdr.failing ? `${hdr.failing} unexpected difference(s)` : 'no unexpected differences'}**`, '');
  const R = (ep) => (weights[ep] && weights[ep].rank) || 999, bucketOf = (ep) => (weights[ep] && weights[ep].bucket) || '';
  const eps = [...sum.by].sort((x, y) => R(x[0]) - R(y[0]) || (x[0] < y[0] ? -1 : 1));
  const failingEps = eps.filter(([, c]) => [...FAILING].some((k) => c[k])).map(([ep]) => ep);
  const byBucket = ['top', 'high', 'medium', 'low', 'rare'].map((b) => [b, failingEps.filter((ep) => bucketOf(ep) === b)]).filter(([, l]) => l.length);
  if (failingEps.length) L.push(`- endpoints with an unexpected difference, by mainnet traffic (usage-weights.json): ${byBucket.map(([b, l]) => `**${b}**: ${l.map((e) => `\`${e}\``).join(', ')}`).join('; ') || '(none ranked)'}`, '');
  L.push('## Summary by endpoint (busiest first)', '', `| endpoint | traffic | ${CLASSES.map((c) => SHORT[c]).join(' | ')} |`, `|---|---|${CLASSES.map(() => '--:').join('|')}|`);
  for (const [ep, c] of eps) L.push(`| \`${ep}\` | ${bucketOf(ep)} | ${CLASSES.map((k) => (c[k] ? (FAILING.has(k) ? `**${c[k]}**` : c[k]) : '')).join(' | ')} |`);
  L.push(`| **total** | | ${CLASSES.map((k) => sum.total[k] || '').join(' | ')} |`, '');
  const detail = results.filter((r) => !['identical', 'equal-after-normalization'].includes(r.cls));
  const order = ['DIFF', 'error-shape-diff', 'transport-error', 'unstable', 'allowed', 'B-partial-expected', 'B-501-expected', 'equal-on-retry'];
  L.push('## Details', '');
  for (const cls of order) {
    const rs = detail.filter((r) => r.cls === cls).sort((x, y) => ((weights[x.endpoint] && weights[x.endpoint].rank) || 999) - ((weights[y.endpoint] && weights[y.endpoint].rank) || 999));
    if (!rs.length) continue;
    L.push(`### ${cls} (${rs.length})`, '');
    for (const r of rs) {
      L.push(`- **\`${r.id}\`** \`${r.method} ${r.path}\` — A ${r.a.status || r.a.error}, B ${r.b.status || r.b.error}${r.b.edge ? ` (x-pulse-edge: ${r.b.edge})` : ''}${r.original ? ` [was ${r.original}]` : ''}${r.note ? ` — ${r.note}` : ''}`);
      if (r.body) L.push(`  - request: \`${r.body.replace(/`/g, "'")}\``);
      for (const d of (r.diff || []).slice(0, 8)) L.push(`  - \`${d.path}\` (${d.kind}): A \`${JSON.stringify(d.a)}\` · B \`${JSON.stringify(d.b)}\``);
      if (FAILING.has(cls) && !(r.diff || []).length) L.push(`  - A: \`${(r.a.sample || '').replace(/`/g, "'")}\``, `  - B: \`${(r.b.sample || '').replace(/`/g, "'")}\``);
    }
    L.push('');
  }
  return L.join('\n');
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
function html(md) { // small self-contained page: the markdown report rendered minimally
  const body = md.split('\n').map((l) => {
    if (l.startsWith('### ')) return `<h3>${esc(l.slice(4))}</h3>`;
    if (l.startsWith('## ')) return `<h2>${esc(l.slice(3))}</h2>`;
    if (l.startsWith('# ')) return `<h1>${esc(l.slice(2))}</h1>`;
    if (/^\|---/.test(l)) return '';
    if (l.startsWith('|')) return `<tr>${l.split('|').slice(1, -1).map((c) => `<td>${esc(c.trim()).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>').replace(/`(.+?)`/g, '<code>$1</code>')}</td>`).join('')}</tr>`;
    const li = l.match(/^(\s*)- (.*)$/);
    if (li) return `<div class="li${li[1] ? ' sub' : ''}">${esc(li[2]).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>').replace(/`(.+?)`/g, '<code>$1</code>')}</div>`;
    return l ? `<p>${esc(l)}</p>` : '';
  }).join('\n').replace(/(<tr>[\s\S]*?<\/tr>\n?)+/g, (t) => `<table>${t}</table>`);
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>/v1 conformance</title>
<style>:root{--bg:#fff;--fg:#1a1a1a;--mut:#666;--line:#ddd;--code:#f3f3f3}@media (prefers-color-scheme:dark){:root{--bg:#111;--fg:#e6e6e6;--mut:#999;--line:#333;--code:#1d1d1d}}
body{background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,sans-serif;margin:0 auto;max-width:1200px;padding:16px}table{border-collapse:collapse;display:block;overflow-x:auto}
td{border:1px solid var(--line);padding:3px 8px;text-align:right}td:first-child{text-align:left}code{background:var(--code);padding:0 3px;word-break:break-all}.li{margin:6px 0}.sub{margin-left:24px;color:var(--mut)}</style>
</head><body>${body}</body></html>`;
}

// ---- main --------------------------------------------------------------------------------------------------------------------
const started = new Date().toISOString();
const results = [];
let i = 0;
for (const req of corpus) {
  const r = await one(req);
  results.push(r);
  i++;
  if (FAILING.has(r.cls) || i % 25 === 0) process.stderr.write(`[diff] ${i}/${corpus.length} ${r.cls === 'identical' ? '' : r.cls + ' '}${r.id}\n`);
}
const sum = summarize(results);
const failing = results.filter((r) => FAILING.has(r.cls)).length;
const hdr = { started, finished: new Date().toISOString(), failing };
const md = markdown(results, sum, hdr);
const REPORT = opt('report');
if (REPORT) writeFileSync(REPORT, REPORT.endsWith('.html') ? html(md) : md);
if (opt('json')) writeFileSync(opt('json'), JSON.stringify({ a: A, b: B, a_history: AH, b_history: BH, corpus: meta, frozen: FROZEN, ...hdr, total: sum.total, results }, null, 1));

// console summary
const w = Math.max(...[...sum.by.keys()].map((k) => k.length), 8);
console.log(`${'endpoint'.padEnd(w)}  ${CLASSES.map((c) => SHORT[c].padStart(9)).join('')}`);
for (const [ep, c] of [...sum.by].sort()) console.log(`${ep.padEnd(w)}  ${CLASSES.map((k) => String(c[k] || '·').padStart(9)).join('')}`);
console.log(`${'TOTAL'.padEnd(w)}  ${CLASSES.map((k) => String(sum.total[k] || '·').padStart(9)).join('')}`);
console.log(failing ? `\n${failing} unexpected difference(s)${REPORT ? ` — see ${REPORT}` : ''}` : '\nno unexpected differences');
process.exit(failing ? 1 : 0);
