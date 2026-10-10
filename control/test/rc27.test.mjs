// node --test control/test/*.test.mjs
// rc.27 (2nd review): the old-chain SPLIT alarm without a guessed burn-off bound. Right after rc.23 mission control was deployed,
// a 5-BP rehearsal on rc.22 beacons (no head_at_pause) paused every source correctly at cut + 377 (DPoS finality
// lag + freeze lead + quiescence), past rc.23's fixed fallback of cut + 360: a false, latched SPLIT. The fallback is
// gone; the relay now watches the old chain's head MOVE after every member is paused and someone is past creation.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { fleetVerdict, movementArmed, nextEventMark, stateRank, PAUSED_RANK } from '../lib.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CHAIN = '71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd';
const sha = (s) => createHash('sha256').update(s).digest('hex');
const BPS = ['bpa', 'bpb', 'bpc', 'bpd', 'bpe'];
const TOK = Object.fromEntries(BPS.map((p, i) => [p, String(i + 1).repeat(64)]));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const PUBHEX = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');
const signed = (payload) => { const p = JSON.stringify(payload); return { payload: p, key: PUBHEX, sig: sign(null, Buffer.from(p), privateKey).toString('hex') }; };
const procs = [];
after(() => procs.forEach((p) => p.kill('SIGKILL')));

const CUT = 408462434;
function setup() {
  const dir = mkdtempSync(join(tmpdir(), 'mc-rc24-'));
  writeFileSync(join(dir, 'networks.json'), JSON.stringify({ networks: [{ id: 'testnet', name: 'XPR Network', label: 'Testnet', chain_id: CHAIN,
    rpc: [], coordinators: [PUBHEX], static_producers: true }] }));
  writeFileSync(join(dir, 'tokens.json'), JSON.stringify(Object.fromEntries(BPS.map((p) => [sha(TOK[p]), { network: 'testnet', producer: p }]))));
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
    proc.on('exit', (c) => ko(new Error(`server exited ${c}: ${out}`)));
    setTimeout(() => ko(new Error('server did not start: ' + out)), 8000);
  });
}
const kill = (proc) => new Promise((ok) => { proc.once('exit', ok); proc.kill('SIGKILL'); });
const fixture = () => JSON.parse(readFileSync(join(HERE, 'fixtures', 'rc5-report.json'), 'utf8'));
const net = async (base) => (await (await fetch(`${base}/api/status`)).json()).networks.find((n) => n.id === 'testnet');


// rc.28: the creation-evidence log. Each test reproduces a scenario an independent review used to make the relay lose
// past-creation evidence (rc.27 rounds 1-3), end to end through the real server.
const EVPOST = (base, payload) => fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ type: 'event', network: 'testnet',
  chain_id: CHAIN, roster: BPS.map((producer) => ({ producer })), quorum: 5, ...payload })) });
function reporter(base, startTs) {
  let t = startTs;
  return async (producer, event, ceremony, instance = 'a') => {
    for (;;) {
      const r = await fetch(`${base}/api/report`, { method: 'POST', headers: { authorization: `Bearer ${TOK[producer]}` },
        body: JSON.stringify({ ...fixture(), producer, network: 'testnet', role: 'producer', instance_id: instance.repeat(32), ts: new Date(t += 3100).toISOString(),
          source: { head: CUT, lib: CUT, chain_id: CHAIN }, coord: { event_id: event, h: CUT, accepted: true, armed: true },
          ceremony: { ...fixture().ceremony, ...ceremony } }) });
      if (r.status !== 429) return r;
      await new Promise((ok) => setTimeout(ok, 3100));
    }
  };
}
const evlog = async (base, p = 'bpa') => (await net(base)).producers.find((x) => x.name === p).creation_evidence;
const retire = (base, q) => fetch(`${base}/api/admin/retire-evidence?net=testnet&producer=bpa&${q}`, { method: 'POST' });

test('rc.28: evidence survives instance replacement, a fresh ABORTED and a restart; only a local operator retires it, by id', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  assert.equal((await EVPOST(base, { event_id: 'ev-1', h: CUT })).status, 200);
  // The old instance reported ~4.5 min ago (silent now), the rest report recently: a real replacement.
  assert.equal((await reporter(base, Date.now() - 270e3)('bpa', 'ev-1', { state: 'LIVE', ignition_started: true, evidence: { h: CUT + 999 } }, 'a')).status, 200);
  const send = reporter(base, Date.now() - 60e3);
  for (const p of BPS.slice(1)) assert.equal((await send(p, 'ev-1', { state: 'ABORTED', evidence: { h: CUT } }, 'c')).status, 200);
  assert.equal((await send('bpa', 'ev-1', { state: 'ABORTED', evidence: { h: CUT } }, 'b')).status, 200);
  let n = await net(base);
  assert.deepEqual(n.producers.find((p) => p.name === 'bpa').creation_evidence.obs.map((o) => [o.id, o.h, o.state]), [[1, CUT + 999, 'LIVE']]);
  assert.notEqual(n.fleet.verdict, 'ABORTED');
  assert.ok(n.fleet.warnings.some((w) => w.startsWith('bpa has creation evidence for ANOTHER H')), JSON.stringify(n.fleet));
  await kill(proc);
  ({ base, proc } = await start(dir));
  assert.equal((await evlog(base)).obs.length, 1, 'persisted');
  assert.equal((await retire(base, 'id=1').then((r) => r.status)), 200);
  assert.equal((await fetch(`${base}/api/admin/retire-evidence?net=testnet&producer=bpa&id=1`, { method: 'POST', headers: { 'x-real-ip': '1.2.3.4' } })).status, 403);
  assert.equal((await retire(base, 'h=999')).status, 400, 'by id only');
  assert.equal((await retire(base, 'id=1')).status, 404, 'already retired');
  n = await net(base);
  assert.equal(n.producers.find((p) => p.name === 'bpa').creation_evidence.obs.length, 0);
  assert.equal(n.fleet.verdict, 'ABORTED');
  await kill(proc);
});

test('rc.28: a report accepted BEFORE its event is published (or with the coordinator store lost) is still evidence', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  const send = reporter(base, Date.now() - 200e3);
  assert.equal((await send('bpa', 'ev-1', { state: 'LIVE', ignition_started: true, evidence: { h: CUT } }, 'a')).status, 200);
  assert.equal((await send('bpa', 'ev-1', { state: 'ABORTED', evidence: { h: CUT } }, 'a')).status, 200);
  assert.equal((await EVPOST(base, { event_id: 'ev-1', h: CUT })).status, 200);
  let lg = await evlog(base);
  assert.deepEqual(lg.obs.map((o) => [o.event_id, o.h, o.state]), [['ev-1', CUT, 'LIVE']]);
  const n = await net(base);
  assert.equal(n.producers.find((p) => p.name === 'bpa').creation_evidence.obs[0].event_id, 'ev-1');
  await kill(proc);
});

test('rc.28: an older event\'s ordinary creation evidence is not trimmed by newer signed events; observations are per target', async () => {
  const dir = setup();
  let { base, proc } = await start(dir);
  assert.equal((await EVPOST(base, { event_id: 'ev-1', h: CUT })).status, 200);
  const send = reporter(base, Date.now() - 250e3);
  // Two different target chains at the same H from the same instance: two observations.
  const tgt = (bid) => ({ blockchain_id: bid, subnet_id: null, chain_id: null, head: CUT + 2, head_id: null, after_cut_id: 'a1'.repeat(32) });
  assert.equal((await send('bpa', 'ev-1', { state: 'LIVE', ignition_started: true, evidence: { h: CUT }, target: tgt('TargetChainAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') })).status, 200);
  assert.equal((await send('bpa', 'ev-1', { state: 'LIVE', ignition_started: true, evidence: { h: CUT }, target: tgt('TargetChainBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB') })).status, 200);
  assert.equal((await send('bpa', 'ev-1', { state: 'ABORTED', evidence: { h: CUT } })).status, 200);
  // Close ev-1, then 22 newer signed events with ordinary reports.
  assert.equal((await fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ type: 'abort', network: 'testnet', event_id: 'ev-1' })) })).status, 200);
  for (let i = 0; i < 22; i++) {
    assert.equal((await EVPOST(base, { event_id: `ev-n${i}`, h: CUT + i + 1 })).status, 200);
    assert.equal((await send('bpa', `ev-n${i}`, { state: 'ABORTED', evidence: { h: CUT + i + 1 } })).status, 200);
    assert.equal((await fetch(`${base}/api/coord/testnet`, { method: 'POST', body: JSON.stringify(signed({ type: 'abort', network: 'testnet', event_id: `ev-n${i}` })) })).status, 200);
  }
  await kill(proc);
  ({ base, proc } = await start(dir));
  const lg = await evlog(base);
  assert.deepEqual(lg.obs.filter((o) => o.event_id === 'ev-1').map((o) => o.target_bid.slice(11, 12)), ['A', 'B']);
  await kill(proc);
});
