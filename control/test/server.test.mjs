// node --test control/test
// Starts mission control offline (MC_OFFLINE=1) on a random port with temp config, then exercises the public
// surface: crash-safety, report validation, projection/redaction, identity, freshness, reach, routing, coordination.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { isPublicIp, resolvePublic, redact, isAppRoute, projectReport } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CHAIN = '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd';
const sha = (s) => createHash('sha256').update(s).digest('hex');
const T1 = 'a'.repeat(64), T2 = 'b'.repeat(64), T3 = 'c'.repeat(64), T4 = 'd'.repeat(64), T5 = 'e'.repeat(64), T6 = 'f'.repeat(64);
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const PUBHEX = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');

let proc, base, dir;
before(async () => {
  dir = mkdtempSync(join(tmpdir(), 'mc-test-'));
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [
    { id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN, rpc: [], coordinators: [PUBHEX], metal: { network: 'tahoe', status: 'descriptive text' } }] }));
  writeFileSync(join(dir, 'tokens.json'), JSON.stringify({
    [sha(T1)]: { network: 'testnet', producer: 'protonnz' }, [sha(T2)]: { network: 'testnet', producer: 'protonnz' },
    [sha(T3)]: { network: 'testnet', producer: 'otherbp' }, [sha(T4)]: { network: 'testnet', producer: 'ratebp' },
    [sha(T5)]: { network: 'testnet', producer: 'otherbp' }, [sha(T6)]: { network: 'testnet', producer: 'protonnz' } }));
  proc = spawn(process.execPath, [join(HERE, '..', 'server.js')], { env: { ...process.env, PORT: '0', MC_OFFLINE: '1',
    NETWORKS: join(dir, 'networks.json'), TOKENS: join(dir, 'tokens.json'), COORD_FILE: join(dir, 'coord', 'coord.json') } });
  base = await new Promise((ok, ko) => {
    let out = '';
    proc.stdout.on('data', (d) => { out += d; const m = out.match(/on 127\.0\.0\.1:(\d+)/); if (m) ok(`http://127.0.0.1:${m[1]}`); });
    proc.stderr.on('data', (d) => { out += d; });
    proc.on('exit', (c) => ko(new Error(`server exited ${c}: ${out}`)));
    setTimeout(() => ko(new Error('server did not start: ' + out)), 8000);
  });
});
after(() => proc?.kill());

const report = (over = {}) => ({ schema: 'pulse-cutover-beacon-v1', producer: 'protonnz', network: 'testnet', node: 'api', role: 'api',
  agent_version: '0.5.0-rc.5', ts: new Date().toISOString(), mode: 'api', ready: false,
  checks: [{ name: 'source_api', ok: true, detail: 'head 10 lib 9' }], source: { head: 10, lib: 9, chain_id: CHAIN },
  ceremony: { state: null, since: null, seq: 0, transitions: [], evidence: {}, last_error: null }, coord: null, metal: null, ...over });
const post = (tok, body) => fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${tok}`, 'content-type': 'application/json' },
  body: typeof body === 'string' ? body : JSON.stringify(body) });
const get = (p, h = {}) => fetch(`${base}${p}`, { headers: h });

test('malformed percent-encoding returns 400 and the server stays up', async () => {
  assert.equal((await get('/api/logo/%E0%A4%A/x')).status, 400);
  assert.equal((await get('/api/node/testnet/protonnz/%E0%A4%A')).status, 400);
  assert.equal((await get('/testnet/%E0%A4%A')).status, 404);
  assert.equal((await get('/healthz')).status, 200);
});

test('malformed reports are rejected with 400', async () => {
  assert.equal((await post(T3, 'null')).status, 400);
  assert.equal((await post(T3, '{not json')).status, 400);
  assert.equal((await post(T3, report({ producer: 'otherbp', checks: 'nope' }))).status, 400);
  assert.equal((await post(T3, report({ producer: 'otherbp', ready: 'yes' }))).status, 400);
  assert.equal((await post(T3, report({ producer: 'otherbp', source: { head: 'x' } }))).status, 400);
});

test('XSS / unknown ceremony state is rejected', async () => {
  const r = await post(T5, report({ producer: 'otherbp', ceremony: { state: '<img src=x onerror=alert(1)>', transitions: [], evidence: {} } }));
  assert.equal(r.status, 400);
  assert.match((await r.json()).error, /ceremony\.state/);
  assert.equal((await post(T5, report({ producer: 'otherbp', node: '<b>x</b>' }))).status, 400);
});

test('SSRF helper rejects private / loopback / link-local / ULA addresses', async () => {
  for (const ip of ['127.0.0.1', '10.1.2.3', '172.16.5.5', '192.168.1.1', '169.254.169.254', '100.64.0.1', '0.0.0.0', '224.0.0.1',
    '::1', '::', 'fc00::1', 'fd12:3456::1', 'fe80::1', '::ffff:127.0.0.1', '::ffff:10.0.0.1', '64:ff9b::a00:1', '2002:0a00:0001::1', '2001:db8::1', 'not-an-ip'])
    assert.equal(isPublicIp(ip), false, ip);
  for (const ip of ['8.8.8.8', '1.1.1.1', '103.96.110.52', '2606:4700:4700::1111', '::ffff:8.8.8.8'])
    assert.equal(isPublicIp(ip), true, ip);
  await assert.rejects(resolvePublic('localhost'), /blocked address/);
  await assert.rejects(resolvePublic('evil.example', async () => [{ address: '93.184.216.34' }, { address: '10.0.0.1' }]), /blocked address/);
  assert.equal(await resolvePublic('ok.example', async () => [{ address: '93.184.216.34' }]), '93.184.216.34');
});

test('/api/reach refuses a private X-Real-IP', async () => {
  const r = await get('/api/reach', { 'x-real-ip': '10.0.0.5' });
  assert.equal(r.status, 400);
  assert.equal((await get('/api/reach', { 'x-real-ip': '169.254.169.254' })).status, 400);
  // no header from loopback: caller is 127.0.0.1, also not public
  assert.equal((await get('/api/reach')).status, 400);
});

test('two tokens with the same label are both kept and disambiguated', async () => {
  assert.equal((await post(T1, report())).status, 200);
  const r2 = await post(T2, report());
  assert.equal(r2.status, 200);
  assert.equal((await r2.json()).node, 'api (2)');
  const st = await (await get('/api/status')).json();
  const p = st.networks[0].producers.find((x) => x.name === 'protonnz');
  assert.deepEqual(p.beacons.map((b) => b.node).sort(), ['api', 'api (2)']);
  assert.equal((await get('/api/node/testnet/protonnz/api%20(2)')).status, 200);
  assert.equal(st.networks[0].summary.servers, 2);
});

test('replayed and stale reports are rejected', async () => {
  const body = report({ node: 'fresh', ts: new Date(Date.now() + 1000).toISOString() });
  assert.equal((await post(T1, body)).status, 200);
  assert.equal((await post(T1, body)).status, 409);                                   // same ts again = replay
  assert.equal((await post(T1, report({ node: 'fresh', ts: new Date(Date.now() - 10 * 60e3).toISOString() }))).status, 400); // too old
  assert.equal((await post(T1, report({ node: 'fresh', ts: new Date(Date.now() + 10 * 60e3).toISOString() }))).status, 400); // future
});

test('per-token rate limit', async () => {
  const codes = [];
  for (let i = 0; i < 7; i++) codes.push((await post(T4, report({ producer: 'ratebp', node: 'p', ts: new Date(Date.now() + i * 10).toISOString() }))).status);
  assert.ok(codes.includes(429), codes.join(','));
});

test('only the allow-listed, redacted projection is published', async () => {
  const body = report({ node: 'priv', ts: new Date(Date.now() + 2000).toISOString(), secret_field: 'hunter2', hostname: 'box1.internal',
    checks: [{ name: 'hook_on_freeze', ok: false, detail: 'failed: /etc/pulse/hooks/freeze.sh http://10.0.0.3:8888/v1 PVT_K1_2bfGi9rYsXQSXXTvJbDAPhHLQUojjaNLomdm3cEJ1XTzMqUt3V' }],
    ceremony: { state: 'ABORTED', transitions: [{ state: 'ARMED', ts: new Date().toISOString() }], evidence: { h: 5, junk: 'x', cut_block_id: 'abc' },
      last_error: 'hook /opt/x/run.sh exited 1: curl https://secret.internal/key?token=123' } });
  assert.equal((await post(T6, body)).status, 200);
  const txt = await (await get('/api/node/testnet/protonnz/priv')).text();
  for (const leak of ['hunter2', 'box1.internal', '/etc/pulse', '10.0.0.3', 'PVT_K1', 'secret.internal', '/opt/x', 'junk']) assert.ok(!txt.includes(leak), leak);
  const j = JSON.parse(txt);
  assert.equal(j.report.ceremony.evidence.h, 5);
  assert.match(j.report.ceremony.last_error, /^error \(see local journal\)/);
});

test('unit: redact and projection', () => {
  assert.equal(redact('see /var/lib/x and 1.2.3.4:80'), 'see <path> and <ip>');
  assert.throws(() => projectReport(null));
  assert.throws(() => projectReport(report({ role: 'root' })));
});

test('SPA fallback serves only app routes', async () => {
  assert.equal((await get('/testnet')).status, 200);
  assert.equal((await get('/testnet/protonnz/endpoint/tn1.protonnz.com')).status, 200);
  assert.equal((await get('/junk/path/x/y/z')).status, 404);
  assert.equal((await get('/nope')).status, 404);
  assert.equal((await get('/wp-admin.php')).status, 404);
  assert.equal(isAppRoute('/testnet/protonnz/api%20(2)', ['testnet']), true);
  assert.equal(isAppRoute('/testnet/__proto__', ['testnet']), false);
});

test('manifest is marked as preparation metadata', async () => {
  const m = await (await get('/api/manifest/testnet')).json();
  assert.equal(m.status, 'preparation metadata, not a release authorization');
  assert.equal(m.summary, 'descriptive text');
});

test('coordination: persisted, and a different event cannot replace an active one', async () => {
  const signed = (payload) => { const p = JSON.stringify(payload); return { payload: p, key: PUBHEX, sig: sign(null, Buffer.from(p), privateKey).toString('hex') }; };
  const postC = (m) => fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(m) });
  assert.equal((await postC(signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev1', h: 100 }))).status, 200);
  assert.equal((await postC(signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev2', h: 200 }))).status, 409);
  assert.equal((await postC(signed({ type: 'arm', network: 'testnet', event_id: 'ev1' }))).status, 200);
  assert.equal((await postC({ payload: '{}', key: 'zz', sig: 'zz' })).status, 403);
  const f = join(dir, 'coord', 'coord.json');
  assert.ok(existsSync(f));
  const saved = JSON.parse(readFileSync(f, 'utf8'));
  assert.ok(saved.testnet.event && saved.testnet.arm);
  assert.equal(saved.testnet.history.length, 2);
  assert.equal((await postC(signed({ type: 'abort', network: 'testnet', event_id: 'ev1' }))).status, 200);
  assert.equal((await postC(signed({ type: 'event', network: 'testnet', chain_id: CHAIN, event_id: 'ev2', h: 200 }))).status, 200);
});

test('reserved keys cannot reach Object.prototype', async () => {
  assert.equal((await get('/api/coord/constructor')).status, 200);
  assert.deepEqual(await (await get('/api/coord/constructor')).json(), {});
  assert.equal((await get('/api/infra/constructor')).status, 200);
  assert.deepEqual(await (await get('/api/infra/constructor')).json(), { pending: true });
});
