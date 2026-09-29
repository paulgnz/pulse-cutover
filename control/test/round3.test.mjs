// node --test control/test/*.test.mjs
// Round-3 fixes (Astra second verification 2026-09-30). Every test reproduces the failing case Astra reported:
// rc.5-store event-id reuse, invalid nested store accepted, crash right after a 200 losing replay/conflict state,
// conflicted evidence counted in agreement, clearing one conflict un-flagging the rest, colliding display labels
// and label-based navigation, first-wins endpoint health, DNS outliving the request deadline, event-hash contract.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { safeRequest } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CHAIN = '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd';
const sha = (s) => createHash('sha256').update(s).digest('hex');
const TOKS = ['1'.repeat(64), '2'.repeat(64), '3'.repeat(64)];
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const PUBHEX = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');
const signed = (payload) => { const p = typeof payload === 'string' ? payload : JSON.stringify(payload); return { payload: p, key: PUBHEX, sig: sign(null, Buffer.from(p), privateKey).toString('hex') }; };
const procs = [];
after(() => procs.forEach((p) => p.kill('SIGKILL')));

function setup({ roster = false, extraNets = [] } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'mc-r3-'));
  const nets = [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN, rpc: [], coordinators: [PUBHEX],
    ...(roster ? { geo: { protonnz: { name: 'Proton NZ' } } } : {}) }, ...extraNets];
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: nets }));
  writeFileSync(join(dir, 'tokens.json'), JSON.stringify(Object.fromEntries(TOKS.map((t) => [sha(t), { network: 'testnet', producer: 'protonnz' }]))));
  mkdirSync(join(dir, 'state'));
  return dir;
}
function start(dir) {
  const proc = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', MC_OFFLINE: '1',
    NETWORKS: join(dir, 'networks.json'), TOKENS: join(dir, 'tokens.json'), COORD_FILE: join(dir, 'state', 'coord.json') } });
  procs.push(proc);
  return new Promise((ok, ko) => {
    let out = '';
    proc.stdout.on('data', (d) => { out += d; const m = out.match(/on 127\.0\.0\.1:(\d+)/); if (m) ok({ base: `http://127.0.0.1:${m[1]}`, proc }); });
    proc.stderr.on('data', (d) => { out += d; });
    proc.on('exit', (c) => ko(Object.assign(new Error(`server exited ${c}: ${out}`), { code: c, out })));
    setTimeout(() => ko(new Error('server did not start: ' + out)), 8000);
  });
}
// a crash, not a clean shutdown: nothing gets a chance to flush
const crash = (proc) => new Promise((ok) => { proc.once('exit', ok); proc.kill('SIGKILL'); });
const postC = (base, m, net = 'testnet') => fetch(`${base}/api/coord/${net}`, { method: 'POST', body: JSON.stringify(m) });
const postR = (base, body, tok = TOKS[0]) => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${tok}` }, body: JSON.stringify(body) });
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
let tick = 0;
const fresh = (over = {}) => ({ ...fixture(), ts: new Date(Date.now() - 60e3 + (tick++) * 1000).toISOString(), ...over });
const inst = (c) => c.repeat(32);
const status = async (base) => (await (await fetch(`${base}/api/status`)).json()).networks.find((n) => n.id === 'testnet');
const pnz = async (base) => (await status(base)).producers.find((p) => p.name === 'protonnz');

test('#4 an rc.5 store (no `used` map) still makes its event ids single-use after the upgrade', async () => {
  const dir = setup();
  const evA = signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-a', h: 100 });
  const abA = signed({ type: 'abort', network: 'testnet', event_id: 'ev-a' });
  // rc.5 wrote { event, arm?, abort?, history? } only; ev-old appears only in history
  writeFileSync(join(dir, 'state', 'coord.json'), JSON.stringify({ testnet: { event: evA, abort: abA, history: [{ type: 'event', event_id: 'ev-old', at: 'x' }] } }));
  const { base, proc } = await start(dir);
  const changed = signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-a', h: 999 });
  assert.equal((await postC(base, changed)).status, 409, 'aborted rc.5 event id cannot be republished with a changed payload');
  assert.equal((await postC(base, evA)).status, 409, 'nor with the same payload');
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-old', h: 5 }))).status, 409, 'history-only ids are used too');
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-new', h: 5 }))).status, 200);
  await crash(proc);
});

test('#4 arm without event_hash is refused (client omission cannot slip through)', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  const ev = signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-h', h: 100 });
  assert.equal((await postC(base, ev)).status, 200);
  assert.equal((await postC(base, signed({ type: 'arm', network: 'testnet', event_id: 'ev-h' }))).status, 409);
  assert.equal((await postC(base, signed({ type: 'arm', network: 'testnet', event_id: 'ev-h', event_hash: null }))).status, 409);
  await crash(proc);
});

test('#6 a syntactically valid but invalid nested store refuses to start', async () => {
  for (const bad of ['{"testnet":null}', '{"testnet":{"event":"x"}}', '{"testnet":{"arm":{"payload":"{}","key":"a","sig":"b"}}}', '{"testnet":{"used":[]}}', '{"__proto__":{}}']) {
    const dir = setup();
    writeFileSync(join(dir, 'state', 'coord.json'), bad);
    await assert.rejects(start(dir), (e) => e.code === 3 && /Refusing to start/.test(e.out), bad);
  }
  const dir = setup();
  writeFileSync(join(dir, 'state', 'servers.json'), '{"v":1,"lastTs":{"zz":1},"nodes":{}}');
  await assert.rejects(start(dir), (e) => e.code === 3 && /servers: .*Refusing to start/.test(e.out));
});

test('#6 a crash right after a 200 keeps the replay watermark and the identity conflict', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  const a = fresh({ instance_id: inst('a') }), b = fresh({ instance_id: inst('b') });
  assert.equal((await postR(base, a)).status, 200);
  assert.equal((await postR(base, b)).status, 200);
  await crash(proc);                                   // immediately: no 1.3 s grace for deferred writes
  ({ base, proc } = await start(dir));
  assert.equal((await postR(base, b)).status, 409, 'the acknowledged report cannot be replayed after the crash');
  const p = await pnz(base);
  assert.equal(p.beacons.length, 2, 'both instances survived the crash');
  assert.ok(p.beacons.every((x) => x.conflict), 'and both are still in conflict');
  assert.equal(p.ready, false);
  await crash(proc);
});

test('#6 the rc.6 replay.json watermark is migrated', async () => {
  const dir = setup();
  const key = `${sha(TOKS[0])}:${inst('c')}`;
  const r = fresh({ instance_id: inst('c') });
  writeFileSync(join(dir, 'state', 'replay.json'), JSON.stringify({ [key]: Date.parse(r.ts) }));
  const { base, proc } = await start(dir);
  assert.equal((await postR(base, r)).status, 409);
  await crash(proc);
});

test('#5 conflicted evidence never counts toward agreement or prepared servers', async () => {
  const dir = setup({ roster: true });
  const { base, proc } = await start(dir);
  const ev = { h: 100, snapshot_sha256: 'a'.repeat(64) };
  for (const i of ['a', 'b']) {
    const r = fresh({ instance_id: inst(i) }); r.ceremony = { ...r.ceremony, evidence: ev };
    assert.equal((await postR(base, r)).status, 200);
  }
  const n = await status(base);
  const row = n.agreement.find((x) => x.key === 'snapshot_sha256');
  assert.ok(row, 'agreement row present');
  assert.equal(row.agree, false, 'identical values from conflicted servers must not agree');
  assert.equal(row.conflicted.length, 2);
  assert.deepEqual(row.missing, ['protonnz'], 'the roster member counts as missing');
  assert.equal(n.summary.servers_prepared, 0);
  await crash(proc);
});

test('#5 clearing one of three conflicting instances leaves the other two in conflict (and survives a crash)', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  for (const i of ['a', 'b', 'c']) assert.equal((await postR(base, fresh({ instance_id: inst(i) }))).status, 200);
  let p = await pnz(base);
  assert.equal(p.beacons.length, 3);
  const sid = p.beacons[0].sid;
  const r = await fetch(`${base}/api/admin/clear-server?net=testnet&producer=protonnz&sid=${sid}`, { method: 'POST' });
  assert.equal(r.status, 200);
  p = await pnz(base);
  assert.equal(p.beacons.length, 2);
  assert.ok(p.beacons.every((x) => x.conflict), 'the two that remain are still one token on two machines');
  assert.equal(p.ready, false);
  await crash(proc);
  ({ base, proc } = await start(dir));
  p = await pnz(base);
  assert.equal(p.beacons.length, 2, 'the removal was durable');
  assert.ok(p.beacons.every((x) => x.conflict));
  await crash(proc);
});

test('#10 colliding labels ("api", "api", "api (2)") get unique display labels, and each sid reaches its own server', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  const labels = ['api', 'api', 'api (2)'];
  for (let k = 0; k < 3; k++) assert.equal((await postR(base, fresh({ instance_id: inst('abc'[k]), node: labels[k], role: 'api' }), TOKS[k])).status, 200);
  const p = await pnz(base);
  const shown = p.beacons.map((x) => x.node);
  assert.equal(new Set(shown).size, 3, `labels must be unique: ${shown}`);
  for (const b of p.beacons) {
    const d = await (await fetch(`${base}/api/node/testnet/protonnz/${b.sid}`)).json();
    assert.equal(d.sid, b.sid); assert.equal(d.node, b.node);
  }
  await crash(proc);
});

// the dashboard's pure helpers are tested straight from index.html
function pureHelpers() {
  const html = readFileSync(join(HERE, '..', 'public', 'index.html'), 'utf8');
  const m = html.match(/\/\* pure:begin[\s\S]*?\*\/([\s\S]*?)\/\* pure:end \*\//);
  assert.ok(m, 'pure helper block present');
  return new Function(`${m[1]}; return { resolveServer, mergeEndpointRecords };`)();
}

test('#10/#13 navigation resolves servers by sid; an ambiguous label never picks one', () => {
  const { resolveServer } = pureHelpers();
  const bs = [{ node: 'api', sid: '1111111111111111' }, { node: 'api (2)', sid: '2222222222222222' }, { node: 'api (2) (2)', sid: '3333333333333333' }];
  assert.equal(resolveServer(bs, '3333333333333333').node, 'api (2) (2)', 'third tile opens the third server');
  assert.equal(resolveServer(bs, '2222222222222222').sid, '2222222222222222');
  assert.equal(resolveServer([{ node: 'api', sid: 'a' }, { node: 'api', sid: 'b' }], 'api'), null, 'ambiguous legacy label → no guess');
  assert.equal(resolveServer(bs, 'api').sid, '1111111111111111', 'an unambiguous legacy label still works');
});

test('#13 same-URL endpoint aggregation keeps the worst health, not the first record', () => {
  const { mergeEndpointRecords } = pureHelpers();
  const m = mergeEndpointRecords([
    { url: 'https://x', types: ['full'], api: { ok: true, ms: 10 } },
    { url: 'https://x', types: ['query'], api: { ok: false, error: 'timeout' }, hyperion: { ok: true, degraded: true } },
  ]);
  assert.equal(m.api.ok, false, 'a failing listing shows as failing');
  assert.equal(m.hyperion.degraded, true);
  assert.deepEqual(m.types, ['full', 'query']);
  assert.equal(m.listings.length, 2, 'every listing kept for display');
});

test('#9 DNS resolution is bounded by the request deadline', async () => {
  const slow = () => new Promise((ok) => setTimeout(() => ok([{ address: '93.184.216.34', family: 4 }]), 366));
  const t0 = Date.now();
  await assert.rejects(safeRequest('http://slow.example/', { lookup: slow, timeoutMs: 20 }), (e) => e.name === 'TimeoutError');
  assert.ok(Date.now() - t0 < 200, `returned after ${Date.now() - t0} ms (resolver took 366 ms)`);
});

test('event_hash contract: sha256 over the UTF-8 bytes of the exact stored payload (shared vector with the Rust agent)', async () => {
  const v = JSON.parse(readFileSync(join(HERE, 'fixtures', 'event-hash-vector.json'), 'utf8'));
  assert.equal(createHash('sha256').update(Buffer.from(v.payload, 'utf8')).digest('hex'), v.event_hash);
  assert.equal(Buffer.byteLength(v.payload), v.payload_utf8_bytes);
  const pl = JSON.parse(v.payload);
  const dir = setup({ extraNets: [{ id: pl.network, name: 'Rehearsal', label: 'r', chain_id: pl.chain_id, rpc: [], coordinators: [PUBHEX] }] });
  const { base, proc } = await start(dir);
  const r = await (await postC(base, signed(v.payload), pl.network)).json();
  assert.equal(r.event_hash, v.event_hash, 'the relay reports the same hash');
  const got = await (await fetch(`${base}/api/coord/${pl.network}`)).json();
  assert.equal(got.event.payload, v.payload, 'stored byte-for-byte');
  await crash(proc);
});
