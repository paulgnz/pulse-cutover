#!/usr/bin/env node
// diff-clients.mjs — compare two run-clients.mjs records (A = reference endpoint, B = candidate).
//
//   node tools/conformance/clients/diff-clients.mjs a.json b.json [--report out.md]
//
// Per library and flow: same outcome (ok/error), same value (TAPOS distance and expiry are volatile; the TAPOS
// check itself — ref_block_prefix matching the referenced block — must pass on both), and the same sequence of
// endpoints hit (a different sequence means the library fell back to another code path on B, e.g. because
// get_block_header_state failed there). Exit 0 = same, 1 = differences, 2 = usage error.
import { readFileSync, writeFileSync } from 'node:fs';
import { jsonDiff, equal } from '../lib.mjs';

const [fa, fb] = process.argv.slice(2).filter((x) => !x.startsWith('--'));
const ri = process.argv.indexOf('--report');
const REPORT = ri > 0 ? process.argv[ri + 1] : null;
if (!fa || !fb) { console.error('usage: diff-clients.mjs a.json b.json [--report out.md]'); process.exit(2); }
const A = JSON.parse(readFileSync(fa, 'utf8')), B = JSON.parse(readFileSync(fb, 'utf8'));

// Values that legitimately differ between two honest endpoints at different moments.
export function stable(flow, v) {
  if (!v || typeof v !== 'object') return v;
  const o = JSON.parse(JSON.stringify(v));
  if (o.tapos) o.tapos = { prefix_matches: o.tapos.prefix_matches, within_tapos_window: o.tapos.behind_head >= 0 && o.tapos.behind_head < 65536, expiry_in_future: o.tapos.expires_in_s > 0 };
  return o;
}
const seq = (calls) => (calls || []).map((c) => c.replace(/ (\d+)$/, (m, s) => ` ${s[0]}xx`)); // status class only

const rows = [];
let bad = 0;
for (const lib of [...new Set([...Object.keys(A.libs || {}), ...Object.keys(B.libs || {})])].sort()) {
  const la = (A.libs[lib] || {}).flows || {}, lb = (B.libs[lib] || {}).flows || {};
  for (const flow of [...new Set([...Object.keys(la), ...Object.keys(lb)])]) {
    const x = la[flow], y = lb[flow];
    let verdict = 'same', detail = [];
    if (!x || !y) { verdict = 'missing'; detail.push(`${!x ? 'A' : 'B'} has no ${flow}`); } else if (x.skipped && y.skipped) verdict = 'skipped';
    else if (x.ok !== y.ok) { verdict = 'outcome'; detail.push(`A ${x.ok ? 'ok' : `error: ${x.error}`} · B ${y.ok ? 'ok' : `error: ${y.error}`}`); } else if (!x.ok) {
      if (x.error !== y.error) { verdict = 'error-text'; detail.push(`A: ${x.error} · B: ${y.error}`); }
    } else {
      const va = stable(flow, x.value), vb = stable(flow, y.value);
      if (!equal(va, vb)) { verdict = 'value'; detail.push(...jsonDiff(va, vb).slice(0, 6).map((d) => `${d.path}: A ${JSON.stringify(d.a)} · B ${JSON.stringify(d.b)}`)); }
      // A TAPOS problem on B only is a difference; the same problem on both is the library's behaviour on this chain
      // (e.g. useLastIrreversible + a short expireSeconds on a chain whose LIB lags head by minutes) and is noted.
      const tOk = (v) => !(v && v.tapos) || (v.tapos.prefix_matches && v.tapos.within_tapos_window && v.tapos.expiry_in_future);
      if (!tOk(vb) && tOk(va)) { verdict = 'value'; detail.push(`B: TAPOS check failed ${JSON.stringify(vb.tapos)}`); }
      else if (!tOk(va) && !tOk(vb)) detail.push(`note: TAPOS check fails on both endpoints ${JSON.stringify(va.tapos)}`);
    }
    if (x && y && !equal(seq(x.calls), seq(y.calls))) { detail.push(`calls A [${seq(x.calls).join(', ')}] · B [${seq(y.calls).join(', ')}]`); if (verdict === 'same') verdict = 'calls'; }
    if (!['same', 'skipped'].includes(verdict)) bad++;
    rows.push({ lib, version: (A.libs[lib] || B.libs[lib]).version, flow, verdict, detail });
  }
}
const cleos = { a: A.cleos, b: B.cleos };

const L = [`# Client matrix: A vs B`, '', `- A: \`${A.url}\` (${A.started})`, `- B: \`${B.url}\` (${B.started})`,
  `- account ${A.context && A.context.account} / ${B.context && B.context.account}; chain ${(A.context && A.context.chain_id || '').slice(0, 12)}… / ${(B.context && B.context.chain_id || '').slice(0, 12)}…`,
  `- verdict: **${bad ? `${bad} difference(s)` : 'every library saw the same thing on both endpoints'}**`, '',
  '| library | flow | verdict | detail |', '|---|---|---|---|'];
for (const r of rows) L.push(`| ${r.lib}@${r.version} | ${r.flow} | ${r.verdict === 'same' ? 'same' : r.verdict === 'skipped' ? 'skipped' : `**${r.verdict}**`} | ${r.detail.join('<br>').replace(/\|/g, '\\|').slice(0, 900)} |`);
L.push('', `cleos: A ${JSON.stringify(cleos.a && cleos.a.skipped ? { skipped: cleos.a.skipped } : Object.keys(cleos.a || {}))} · B ${JSON.stringify(cleos.b && cleos.b.skipped ? { skipped: cleos.b.skipped } : Object.keys(cleos.b || {}))}`);
const md = L.join('\n');
if (REPORT) writeFileSync(REPORT, md);
for (const r of rows) if (!['same', 'skipped'].includes(r.verdict) || r.detail.length) console.log(`${r.lib} ${r.flow}: ${r.verdict}\n   ${r.detail.join('\n   ')}`);
console.log(bad ? `\n${bad} difference(s)` : `\nno differences (${rows.length} library/flow pairs)`);
process.exit(bad ? 1 : 0);
