#!/usr/bin/env node
// tools/capture-static.mjs — capture the at-cut facts the /v1 edge serves statically.
//
//   node tools/capture-static.mjs <source-rpc> <dir>
//   e.g. node tools/capture-static.mjs http://127.0.0.1:8888 /etc/pulse-cutover/static
//
// Writes, from the SOURCE (Leap) chain:
//   <dir>/activated_protocol_features.json   every page of get_activated_protocol_features, merged
//   <dir>/consensus_parameters.json          get_consensus_parameters
// Run it while the source still answers (before its nodeos is stopped; any time during the freeze is
// right, since protocol features and consensus parameters cannot change while no blocks carry actions).
// Files are written atomically (tmp + rename); a failed capture leaves the previous files untouched.
// No dependencies (Node >= 14).
import http from 'node:http';
import https from 'node:https';
import { mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { join } from 'node:path';

const [src, dir] = process.argv.slice(2);
if (!src || !dir) { console.error('usage: node tools/capture-static.mjs <source-rpc> <dir>'); process.exit(2); }
const base = src.replace(/\/$/, '');

function post(path, obj) {
  return new Promise((resolve, reject) => {
    const u = new URL(base + path), body = JSON.stringify(obj);
    const req = (u.protocol === 'https:' ? https : http).request(u, { method: 'POST', timeout: 20000,
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), accept: 'application/json' } }, (res) => {
      let t = ''; res.setEncoding('utf8'); res.on('data', (c) => { t += c; });
      res.on('end', () => {
        if (res.statusCode !== 200) return reject(new Error(`${path}: HTTP ${res.statusCode} ${t.slice(0, 200)}`));
        try { resolve(JSON.parse(t)); } catch { reject(new Error(`${path}: not JSON`)); }
      });
    });
    req.on('timeout', () => req.destroy(new Error(`${path}: timeout`)));
    req.on('error', reject);
    req.end(body);
  });
}
function writeAtomic(file, obj) {
  const tmp = `${file}.tmp`;
  writeFileSync(tmp, JSON.stringify(obj, null, 1) + '\n');
  renameSync(tmp, file);
}

const info = await post('/v1/chain/get_info', {});
const features = [];
let lower = 0;
for (let page = 0; page < 1000; page++) {
  const r = await post('/v1/chain/get_activated_protocol_features', { lower_bound: lower, limit: 100, search_by_block_num: false, reverse: false });
  const got = r.activated_protocol_features || [];
  features.push(...got);
  if (r.more == null || r.more === '' || !got.length) break;
  const next = Number(r.more);
  if (!(next > lower)) break; // defensive: never loop on a non-advancing cursor
  lower = next;
}
const seen = new Set();
const uniq = features.filter((f) => (seen.has(f.feature_digest) ? false : seen.add(f.feature_digest)))
  .sort((a, b) => a.activation_ordinal - b.activation_ordinal);
const consensus = await post('/v1/chain/get_consensus_parameters', {});

mkdirSync(dir, { recursive: true });
const meta = { chain_id: info.chain_id, captured_at_head: info.head_block_num, captured_at_lib: info.last_irreversible_block_num, captured_time: new Date().toISOString() };
writeAtomic(join(dir, 'activated_protocol_features.json'), { activated_protocol_features: uniq, _capture: meta });
writeAtomic(join(dir, 'consensus_parameters.json'), consensus);
console.log(`captured ${uniq.length} activated protocol features + consensus parameters from chain ${String(info.chain_id).slice(0, 12)}… at head ${info.head_block_num} -> ${dir}`);
