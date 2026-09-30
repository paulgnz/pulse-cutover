// node --test control/test/*.test.mjs
// Round-2 fixes (independent verification 2026-09-30): DNS-pinned outbound requests, rc.5 report schema, durable relay
// (restart, write failure, corrupt store, single-use event ids), replay state across restarts, instance-id conflicts,
// endpoint identity, no-roster agreement, freshness from the report's own timestamp.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import http from 'node:http';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { safeRequest, projectReport, endpointId, endpointRef, endpointFromRef, isAppRoute } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CHAIN = '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd';
const sha = (s) => createHash('sha256').update(s).digest('hex');
const TOK = '1'.repeat(64);
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const PUBHEX = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');
const signed = (payload) => { const p = JSON.stringify(payload); return { payload: p, key: PUBHEX, sig: sign(null, Buffer.from(p), privateKey).toString('hex') }; };
const procs = [];
after(() => procs.forEach((p) => p.kill()));

function setup() {
  const dir = mkdtempSync(join(tmpdir(), 'mc-r2-'));
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN, rpc: [], coordinators: [PUBHEX] }] }));
  writeFileSync(join(dir, 'tokens.json'), JSON.stringify({ [sha(TOK)]: { network: 'testnet', producer: 'protonnz' } }));
  return dir;
}
function start(dir, coordFile = join(dir, 'state', 'coord.json')) {
  const proc = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', MC_OFFLINE: '1',
    NETWORKS: join(dir, 'networks.json'), TOKENS: join(dir, 'tokens.json'), COORD_FILE: coordFile } });
  procs.push(proc);
  return new Promise((ok, ko) => {
    let out = '';
    proc.stdout.on('data', (d) => { out += d; const m = out.match(/on 127\.0\.0\.1:(\d+)/); if (m) ok({ base: `http://127.0.0.1:${m[1]}`, proc }); });
    proc.stderr.on('data', (d) => { out += d; });
    proc.on('exit', (c) => ko(Object.assign(new Error(`server exited ${c}: ${out}`), { code: c, out })));
    setTimeout(() => ko(new Error('server did not start: ' + out)), 8000);
  });
}
const stop = (proc) => new Promise((ok) => { proc.once('exit', ok); proc.kill(); });
const postC = (base, m) => fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(m) });
const postR = (base, body, headers = {}) => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK}`, ...headers }, body: JSON.stringify(body) });
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
const fresh = (over = {}) => ({ ...fixture(), ts: new Date().toISOString(), ...over });

test('DNS pinning: the socket goes only to the address that was validated, never to a re-resolved one', async () => {
  let hits = 0;
  const local = http.createServer((q, r) => { hits++; r.end('{}'); });
  await new Promise((ok) => local.listen(0, '127.0.0.1', ok));
  const port = local.address().port;
  let lookups = 0; const dialled = [];
  // rebinding resolver: public the first time (validation), loopback afterwards (what fetch would have used)
  const lookup = async () => (lookups++ === 0 ? [{ address: '93.184.216.34', family: 4 }] : [{ address: '127.0.0.1', family: 4 }]);
  await assert.rejects(safeRequest(`http://rebind.example:${port}/`, { lookup, onConnect: (ip) => dialled.push(ip), timeoutMs: 500 }));
  local.close();
  assert.equal(lookups, 1, 'validation lookup only; no second resolution at connect time');
  assert.deepEqual([...new Set(dialled)], ['93.184.216.34']);
  assert.equal(hits, 0, 'the loopback server was never reached');
  await assert.rejects(safeRequest('http://x.example/', { lookup: async () => [{ address: '127.0.0.1', family: 4 }] }), /blocked address/);
  await assert.rejects(safeRequest('http://user:pw@x.example/'), /credentials/);
});

test('a real rc.5 beacon report keeps last_error_class and lineage_at_cut after projection', () => {
  const r = projectReport(fixture());
  assert.equal(r.profile, 'readiness');
  assert.match(r.instance_id, /^[0-9a-f]{32}$/);
  assert.equal(r.ceremony.evidence.lineage_at_cut, 'verified');
  assert.ok(r.ceremony.last_error_class && r.ceremony.last_error_class.startsWith('HALTED'));
  const txt = JSON.stringify(r);
  for (const leak of ['10.0.0.3', '/etc/pulse']) assert.ok(!txt.includes(leak), leak);
  // object-form lineage (later beacons) and the HALTED state are accepted
  const o = projectReport({ ...fixture(), ceremony: { ...fixture().ceremony, state: 'HALTED',
    evidence: { lineage_at_cut: { h: 1000, source_block_id: 'A'.repeat(64), target_block_id: 'a'.repeat(64), match: true } } } });
  assert.equal(o.ceremony.state, 'HALTED');
  assert.deepEqual(o.ceremony.evidence.lineage_at_cut, { h: 1000, source_block_id: 'a'.repeat(64), target_block_id: 'a'.repeat(64), match: true });
  assert.throws(() => projectReport({ ...fixture(), profile: 'root' }));
  assert.throws(() => projectReport({ ...fixture(), instance_id: 'not-hex-at-all-000000000000000000' }));
});

test('endpoint identity: scheme and path case matter, IPv6 works, refs round-trip', () => {
  assert.notEqual(endpointId('http://api.example.com'), endpointId('https://api.example.com'));
  assert.notEqual(endpointId('https://api.example.com/A'), endpointId('https://api.example.com/a'));
  assert.equal(endpointId('HTTPS://API.Example.com:443/v1/'), 'https://api.example.com/v1');
  assert.equal(endpointId('https://[2001:db8::1]:8443/x'), 'https://[2001:db8::1]:8443/x');
  for (const u of ['https://api.example.com/A', 'http://api.example.com', 'https://[2606:4700::1111]/chain']) {
    const ref = endpointRef(u);
    assert.equal(endpointFromRef(ref), endpointId(u));
    assert.equal(isAppRoute(`/testnet/protonnz/endpoint/${ref}`, ['testnet']), true, u);
  }
  assert.equal(endpointFromRef('aGVsbG8'), null);              // decodes, but not a canonical endpoint id
  assert.equal(isAppRoute('/testnet/protonnz/endpoint/tn1.protonnz.com', ['testnet']), true);   // legacy links still render
});

test('relay: persisted across restart, event ids single-use, arm bound to the event hash; replay state survives restart', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  const ev = signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-a', h: 100 });
  const pr = await (await postC(base, ev)).json();
  assert.equal(pr.event_hash, sha(ev.payload));
  assert.equal((await postC(base, signed({ type: 'arm', network: 'testnet', event_id: 'ev-a', event_hash: 'f'.repeat(64) }))).status, 409);
  assert.equal((await postC(base, signed({ type: 'arm', network: 'testnet', event_id: 'ev-a', event_hash: sha(ev.payload) }))).status, 200);
  const rep = fresh();
  assert.equal((await postR(base, rep)).status, 200);
  // crash immediately after the 200s: acknowledged state must already be durable (no grace for deferred writes)
  await new Promise((ok) => { proc.once('exit', ok); proc.kill('SIGKILL'); });
  ({ base, proc } = await start(dir));
  const c = await (await fetch(`${base}/api/coord/testnet`)).json();
  assert.equal(c.event.payload, ev.payload);
  assert.ok(c.arm);
  assert.equal((await postR(base, rep)).status, 409, 'a report captured before the restart cannot be replayed after it');
  // abort, then try to reuse the id (same or different payload): never allowed
  assert.equal((await postC(base, signed({ type: 'abort', network: 'testnet', event_id: 'ev-a', event_hash: sha(ev.payload) }))).status, 200);
  assert.equal((await postC(base, ev)).status, 409);
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-a', h: 200 }))).status, 409);
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-b', h: 200 }))).status, 200);
  await stop(proc);
});

test('relay: a failed write answers 503 and changes nothing', { skip: process.getuid?.() === 0 ? 'root ignores directory permissions' : false }, async () => {
  const dir = setup();
  const ro = join(dir, 'ro'); mkdirSync(ro); chmodSync(ro, 0o555);
  const { base, proc } = await start(dir, join(ro, 'coord.json'));
  const r = await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-x', h: 100 }));
  assert.equal(r.status, 503);
  assert.deepEqual(await (await fetch(`${base}/api/coord/testnet`)).json(), {});
  chmodSync(ro, 0o755);
  // once the disk is writable again the same event can be published (the failed attempt consumed nothing)
  assert.equal((await postC(base, signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev-x', h: 100 }))).status, 200);
  await stop(proc);
});

test('relay: a corrupt store stops startup instead of silently forgetting events', async () => {
  const dir = setup();
  mkdirSync(join(dir, 'state'));
  writeFileSync(join(dir, 'state', 'coord.json'), '{"testnet": {"event": ');
  await assert.rejects(start(dir), (e) => e.code === 3 && /Refusing to start/.test(e.out));
});

test('one token on two machines: both kept and flagged; loopback-only operator clear', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  const A = 'a'.repeat(32), B = 'b'.repeat(32);
  assert.equal((await postR(base, fresh({ instance_id: A }))).status, 200);
  await new Promise((r) => setTimeout(r, 20));
  assert.equal((await postR(base, fresh({ instance_id: B }))).status, 200);
  let p = (await (await fetch(`${base}/api/status`)).json()).networks[0].producers.find((x) => x.name === 'protonnz');
  assert.equal(p.beacons.length, 2);
  assert.ok(p.beacons.every((b) => b.conflict));
  assert.equal(p.ready, false);
  const sid = p.beacons[1].sid;
  assert.match(sid, /^[0-9a-f]{16}$/);
  assert.equal((await fetch(`${base}/api/node/testnet/protonnz/${sid}`)).status, 200);
  const clear = (h = {}) => fetch(`${base}/api/admin/clear-server?net=testnet&producer=protonnz&sid=${sid}`, { method: 'POST', headers: h });
  assert.equal((await clear({ 'x-real-ip': '203.0.113.9' })).status, 403, 'anything that came through the proxy is refused');
  assert.equal((await clear()).status, 200);
  p = (await (await fetch(`${base}/api/status`)).json()).networks[0].producers.find((x) => x.name === 'protonnz');
  assert.equal(p.beacons.length, 1);
  assert.equal(p.beacons[0].conflict, false);
  await stop(proc);
});

test('no roster → no agreement verdict; freshness uses the report timestamp; stages are reported', async () => {
  const dir = setup();
  const { base, proc } = await start(dir);
  const old = new Date(Date.now() - 4 * 60e3).toISOString();        // accepted (< 5 min) but older than the silence window
  assert.equal((await postR(base, fresh({ ts: old }))).status, 200);
  const n = (await (await fetch(`${base}/api/status`)).json()).networks[0];
  const b = n.producers.find((x) => x.name === 'protonnz').beacons[0];
  assert.equal(b.silent, true);
  assert.ok(b.age_ms >= 4 * 60e3 - 5000);
  assert.equal(b.stages.beacon_healthy.ok, false);
  assert.equal(b.stages.ceremony_configured.ok, false);             // readiness profile
  assert.equal(b.stages.admitted.ok, null);
  assert.equal(b.stages.authorized.ok, null);
  const row = n.agreement.find((a) => a.key === 'h');
  assert.equal(row.agree, null);
  assert.equal(row.verdict, 'no roster');
  await stop(proc);
});
