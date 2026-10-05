// node --test federator/test
// The history boundary rule ("local strictly above H, legacy at or below H"), boundary-file validation (fail
// closed), and the pagination / absence semantics deposit pollers depend on. Mocks: the chain (/v1/chain), a
// legacy Hyperion whose archive CONTINUES past the cut (block 101 > H = 100, like a public archive of a chain
// that kept producing burn-off blocks), and a local hyperion-rs.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const H = 100;
const CHAIN = 'a1'.repeat(32);
const OTHER_CHAIN = 'b2'.repeat(32);
const ID_H = '00000064' + 'ee'.repeat(28);
const CUT_TIME = '2026-01-02T00:00:00.000';
const act = (n, extra = {}) => ({ block_num: n, block_id: n === H ? ID_H : `${n.toString(16).padStart(8, '0')}${'00'.repeat(28)}`,
  '@timestamp': `2026-01-0${n <= H ? 1 : 3}T00:00:${String(n % 60).padStart(2, '0')}.000`, trx_id: `t${n}`, act: { account: 'eosio.token', name: 'transfer' }, ...extra });

// ---- mutable mock state -------------------------------------------------------------------------------------
const S = {
  chainId: CHAIN, blockAtH: ID_H, chainUp: true,
  legacyChainId: CHAIN,
  legacyActs: [act(101), act(99), act(98), act(97)],   // desc; 101 is ABOVE the cut
  legacyRel: 'eq',
  localActs: [act(105), act(103)],
  localRel: 'eq', localUp: true, localV1Status: 404,
  legacyV1: { synthetic: { id: 'synthetic', block_num: 101 }, old: { id: 'old', block_num: 99 } },
  legacyTx: { synthetic: [act(101)], old: [act(99)], atcut: [{ ...act(H), block_id: 'ff'.repeat(32) }] },
  legacyUp: true, legacyV1Status: 404,
};
const calls = [];
function send(res, status, body) { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); }
function listen(handler) {
  return new Promise((ok) => {
    const s = http.createServer((req, res) => { let b = ''; req.on('data', (c) => { b += c; }); req.on('end', () => handler(req, res, b)); });
    s.listen(0, '127.0.0.1', () => ok(s));
  });
}
function pageOf(list, q, rel) {
  const asc = q.get('sort') === 'asc';
  const rows = asc ? list.slice().reverse() : list;
  const skip = Number(q.get('skip') || 0), limit = Number(q.get('limit') || 10);
  return { actions: rows.slice(skip, skip + limit), total: { value: list.length, relation: rel }, lib: 110 };
}
function chainMock(req, res, body) {
  const name = req.url.split('/').pop(), p = JSON.parse(body || '{}');
  if (!S.chainUp) return send(res, 503, { code: 503, message: 'down' });
  if (name === 'get_info') return send(res, 200, { chain_id: S.chainId, head_block_num: 110, head_block_id: '0000006e' + '11'.repeat(28), last_irreversible_block_num: 110 });
  if (name === 'get_block') return p.block_num_or_id === String(H) ? send(res, 200, { id: S.blockAtH, block_num: H }) : send(res, 500, { code: 500, message: 'unknown block' });
  return send(res, 404, { code: 404 });
}
function legacyMock(req, res, body) {
  const u = new URL(req.url, 'http://x'), q = u.searchParams;
  calls.push({ src: 'legacy', path: u.pathname, q: Object.fromEntries(q) });
  if (u.pathname === '/v2/health') return send(res, 200, { health: [{ service: 'NodeosRPC', status: 'OK', service_data: { chain_id: S.legacyChainId } }] });
  if (!S.legacyUp) return send(res, 429, { error: 'rate limited' });
  switch (u.pathname) {
    case '/v2/history/get_actions': return send(res, 200, pageOf(S.legacyActs, q, S.legacyRel));
    case '/v2/history/get_transaction': {   // Hyperion's shape: top-level executed / trx_id / lib next to the actions
      const acts = S.legacyTx[q.get('id')] || [];
      return send(res, 200, { query_time_ms: 1, executed: acts.length > 0, trx_id: q.get('id'), lib: 110, actions: acts });
    }
    case '/v1/history/get_transaction': { const t = S.legacyV1[JSON.parse(body).id]; return t ? send(res, 200, t) : send(res, S.legacyV1Status, { error: 'not found' }); }
    case '/v2/history/get_deltas': return send(res, 200, { deltas: [{ block_num: 101, x: 1 }, { block_num: 99, x: 2 }] });
    default: return send(res, 404, { error: 'no' });
  }
}
function localMock(req, res, body) {
  const u = new URL(req.url, 'http://x'), q = u.searchParams;
  calls.push({ src: 'local', path: u.pathname, q: Object.fromEntries(q) });
  if (u.pathname === '/v2/health') return send(res, 200, { health: [{ service: 'PulseVM-RPC', status: 'OK', service_data: { head_block_num: 110 } }, { service: 'Indexer', status: 'OK', service_data: { last_indexed_block: 108 } }] });
  if (!S.localUp) return send(res, 503, { error: 'indexer unavailable' });
  switch (u.pathname) {
    case '/v2/history/get_actions': return send(res, 200, pageOf(S.localActs, q, S.localRel));
    case '/v2/history/get_transaction': return send(res, 200, { actions: [] });
    case '/v1/history/get_transaction': return send(res, S.localV1Status, { error: 'not found' });
    case '/v2/history/get_deltas': return send(res, 503, { error: 'down' });
    default: return send(res, 404, { error: 'no' });
  }
}

let chainSrv, legacySrv, localSrv, env, dir;
const procs = [];
async function federator(boundary, extraEnv = {}) {
  const file = join(dir, `b-${procs.length}.json`);
  if (boundary !== undefined) writeFileSync(file, typeof boundary === 'string' ? boundary : JSON.stringify(boundary));
  const p = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', PASSTHROUGH_PORT: '0', ...env, BOUNDARY_FILE: file, ...extraEnv } });
  procs.push(p);
  const url = await new Promise((ok, ko) => {
    let out = '';
    p.stdout.on('data', (d) => { out += d; const m = out.match(/federating router\) on 127\.0\.0\.1:(\d+)/); if (m) ok(`http://127.0.0.1:${m[1]}`); });
    p.stderr.on('data', (d) => { out += d; });
    p.on('exit', (c) => ko(new Error(`federator exited ${c}: ${out}`)));
    setTimeout(() => ko(new Error('federator did not start: ' + out)), 8000);
  });
  return { url, file };
}
const GOOD = { cut_block: H, cut_block_id: ID_H, cut_time: CUT_TIME, chain_id: CHAIN };
const get = async (base, path) => { const r = await fetch(base + path); return { status: r.status, json: await r.json(), headers: r.headers }; };
const post = async (base, path, body) => { const r = await fetch(base + path, { method: 'POST', body: JSON.stringify(body) }); return { status: r.status, json: await r.json(), headers: r.headers }; };
const reset = () => { S.chainId = CHAIN; S.blockAtH = ID_H; S.chainUp = true; S.legacyChainId = CHAIN; S.localUp = true; S.legacyUp = true; S.localRel = 'eq'; S.legacyRel = 'eq'; S.localV1Status = 404; S.legacyV1Status = 404; calls.length = 0; };

let good;
before(async () => {
  [chainSrv, legacySrv, localSrv] = await Promise.all([listen(chainMock), listen(legacyMock), listen(localMock)]);
  dir = mkdtempSync(join(tmpdir(), 'fed-boundary-'));
  env = { LOCAL: `http://127.0.0.1:${localSrv.address().port}`, LEGACY: `http://127.0.0.1:${legacySrv.address().port}`, CHAIN_URL: `http://127.0.0.1:${chainSrv.address().port}` };
  good = (await federator(GOOD)).url;
});
after(() => { for (const p of procs) p.kill(); chainSrv?.close(); legacySrv?.close(); localSrv?.close(); });

// ---- the review's reproduction (H = 100, legacy item at 101) --------------------------------------------------
test('a legacy transaction ABOVE the cut is never returned (v2 and v1)', async () => {
  reset();
  const v2 = await get(good, '/v2/history/get_transaction?id=synthetic');
  assert.ok(v2.status === 200 || v2.status === 404, `status ${v2.status}`);
  assert.deepEqual(v2.json.actions, [], 'block 101 on the legacy archive is not this chain\'s history');
  assert.notEqual(v2.json._premigration, true);
  assert.equal(v2.json.federation.legacy_above_cut_ignored, true);
  assert.notEqual(v2.json.executed, true, 'the legacy archive\'s executed:true above the cut must not leak into the answer');
  assert.equal(v2.status === 404 || v2.json.executed === false, true);
  const v1 = await post(good, '/v1/history/get_transaction', { id: 'synthetic' });
  assert.equal(v1.status, 404);
  assert.equal(v1.json.federation.legacy_above_cut_ignored, true);
  // at or below the cut it is pre-cut history
  assert.equal((await get(good, '/v2/history/get_transaction?id=old')).json._premigration, true);
  assert.equal((await post(good, '/v1/history/get_transaction', { id: 'old' })).json.block_num, 99);
});
test('the global feed\'s legacy fallback is bounded by the cut and marked partial', async () => {
  reset(); S.localUp = false;
  const r = await get(good, '/v2/history/get_actions?limit=10');
  assert.equal(r.status, 200);
  assert.deepEqual(r.json.actions.map((a) => a.block_num), [99, 98, 97], 'the legacy row at 101 is dropped');
  assert.equal(r.json.partial, true);
  assert.equal(r.json.legacy_only, true);
  assert.equal(r.json.total.relation, 'gte');
  const q = calls.find((c) => c.src === 'legacy' && c.path === '/v2/history/get_actions').q;
  assert.equal(q.before, CUT_TIME, 'legacy is asked only up to the cut');
});
test('account feed: legacy rows above the cut are dropped (and the answer says the seam is inexact)', async () => {
  reset();
  const r = await get(good, '/v2/history/get_actions?account=alice&limit=10');
  assert.deepEqual(r.json.actions.map((a) => a.block_num), [105, 103, 99, 98, 97]);
  assert.ok(r.json.actions.every((a) => (a.block_num > H) !== (a._premigration === true)));
});

// ---- pagination / filter semantics ------------------------------------------------------------------------------
test('a caller\'s `before` is kept (never overwritten by the cut time) and `sort=asc` is honoured', async () => {
  reset();
  const r = await get(good, '/v2/history/get_actions?account=alice&before=2026-01-01T00:00:00Z&sort=asc&limit=5');
  assert.equal(r.status, 200);
  assert.equal(r.json.sort, 'asc');
  const leg = calls.find((c) => c.src === 'legacy' && c.path === '/v2/history/get_actions').q;
  assert.equal(leg.before, '2026-01-01T00:00:00Z', 'the earlier caller bound wins');
  assert.equal(leg.sort, 'asc');
  assert.ok(!calls.some((c) => c.src === 'local' && c.path === '/v2/history/get_actions'), 'a window wholly before the cut never asks the post-cut index');
  assert.equal((await get(good, '/v2/history/get_actions?account=alice&before=yesterday')).status, 400, 'a non-time `before` is refused, not ignored');
});
test('ascending pages cross the seam legacy -> local at the right offset', async () => {
  reset(); S.legacyActs = [act(99), act(98), act(97)];
  try {
    const r = await get(good, '/v2/history/get_actions?account=alice&sort=asc&skip=2&limit=2');
    assert.deepEqual(r.json.actions.map((a) => a.block_num), [99, 103]);
    assert.equal(r.json.total.relation, 'eq');
    const d = await get(good, '/v2/history/get_actions?account=alice&skip=1&limit=2');
    assert.deepEqual(d.json.actions.map((a) => a.block_num), [103, 99]);
  } finally { S.legacyActs = [act(101), act(99), act(98), act(97)]; }
});
test('a lower-bound total ("gte") is never upgraded to "eq", and an unknown seam is not guessed', async () => {
  reset(); S.legacyActs = [act(99), act(98), act(97)]; S.localRel = 'gte';
  try {
    const r = await get(good, '/v2/history/get_actions?account=alice&limit=5');
    assert.equal(r.json.total.relation, 'gte');
    assert.equal(r.json.partial, true);
    assert.equal(r.json.page_may_be_short, true);
    assert.deepEqual(r.json.actions.map((a) => a.block_num), [105, 103], 'no legacy rows at guessed positions');
  } finally { S.legacyActs = [act(101), act(99), act(98), act(97)]; }
});
test('the first half of the timeline unavailable is a 503, not legacy rows at guessed positions', async () => {
  reset(); S.localUp = false;
  const r = await get(good, '/v2/history/get_actions?account=alice&limit=5');
  assert.equal(r.status, 503);
  assert.equal(r.headers.get('x-pulse-federation-status'), 'unavailable');
});

// ---- absent vs not-yet-indexed vs unavailable ---------------------------------------------------------------------
test('local 503 + legacy 404 is UNAVAILABLE (503), not a definitive 404', async () => {
  reset(); S.localUp = false;
  const v1 = await post(good, '/v1/history/get_transaction', { id: 'nope' });
  assert.equal(v1.status, 503);
  const v2 = await get(good, '/v2/history/get_transaction?id=nope');
  assert.equal(v2.status, 503);
  assert.equal(v2.json.federation.status, 'unavailable');
});
test('both sources answering "not found" while the local indexer lags the chain = not_indexed_yet', async () => {
  reset();
  const v1 = await post(good, '/v1/history/get_transaction', { id: 'nope' });
  assert.equal(v1.status, 404);
  assert.equal(v1.json.federation.status, 'not_indexed_yet', 'indexer at 108, chain head 110');
  assert.equal(v1.headers.get('x-pulse-federation-status'), 'not_indexed_yet');
});
test('generic pass-through: legacy rows above the cut are dropped, a local outage is flagged partial', async () => {
  reset();
  const r = await get(good, '/v2/history/get_deltas?code=x');
  assert.equal(r.status, 200);
  assert.deepEqual(r.json.deltas.map((d) => d.block_num), [99]);
  assert.equal(r.json.partial, true);
  assert.match(r.headers.get('x-pulse-federation-partial'), /local/);
});
test('a legacy row AT the cut with another block id means the archive is another chain: refused', async () => {
  reset();
  const r = await get(good, '/v2/history/get_transaction?id=atcut');
  assert.equal(r.status, 503);
  assert.equal(r.headers.get('x-pulse-federation'), 'legacy-identity-mismatch');
});

// ---- boundary validation: fail closed --------------------------------------------------------------------------
const refuses = async (url, status) => {
  const r = await get(url, '/v2/history/get_actions?account=alice');
  assert.equal(r.status, 503);
  assert.equal(r.headers.get('x-pulse-federation'), `boundary-${status}`);
  const t = await post(url, '/v1/history/get_transaction', { id: 'old' });
  assert.equal(t.status, 503);
  const h = await get(url, '/v2/health');
  assert.equal(h.json.federation.ok, false);
  assert.equal(h.json.federation.boundary.status, status);
  return r;
};
test('boundary: a block id that does not encode cut_block is invalid', async () => {
  reset();
  const { url } = await federator({ ...GOOD, cut_block_id: '00000065' + 'ee'.repeat(28) });
  assert.match((await refuses(url, 'invalid')).json.error, /encodes block 101/);
});
test('boundary: corrupt JSON is invalid', async () => {
  reset();
  await refuses((await federator('{"cut_block": 100,')).url, 'invalid');
});
test('boundary: the chain serving another chain_id (stale boundary from another ceremony) is a mismatch', async () => {
  reset();
  const { url } = await federator({ ...GOOD, chain_id: OTHER_CHAIN });
  assert.match((await refuses(url, 'mismatch')).json.error, /stale or foreign/);
});
test('boundary: the chain\'s block at H differs from cut_block_id = mismatch', async () => {
  reset(); S.blockAtH = '00000064' + '99'.repeat(28);
  try { await refuses((await federator(GOOD)).url, 'mismatch'); } finally { S.blockAtH = ID_H; }
});
test('boundary: the legacy archive reporting another chain_id = mismatch', async () => {
  reset(); S.legacyChainId = OTHER_CHAIN;
  try { assert.match((await refuses((await federator(GOOD)).url, 'mismatch')).json.error, /legacy history/); } finally { S.legacyChainId = CHAIN; }
});
test('boundary: unverifiable at start (chain down) fails closed; a target chain_id named by the boundary is accepted', async () => {
  reset(); S.chainUp = false;
  try { await refuses((await federator(GOOD)).url, 'unverified'); } finally { S.chainUp = true; }
  S.chainId = OTHER_CHAIN;
  try {
    const { url } = await federator({ ...GOOD, target_chain_id: OTHER_CHAIN });
    assert.equal((await get(url, '/v2/history/get_actions?account=alice')).status, 200, 'rehearsal: target signs with another chain_id');
  } finally { S.chainId = CHAIN; }
});
test('boundary: a file that disappears after it was loaded fails closed (never falls back to unlimited legacy)', async () => {
  reset();
  const { url, file } = await federator(GOOD);
  assert.equal((await get(url, '/v2/history/get_actions?account=alice')).status, 200);
  rmSync(file);
  await refuses(url, 'missing');
});
