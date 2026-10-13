// node --test tools/test/shadow-replay.test.mjs
// tools/shadow-replay.mjs: classification, target safety and an end-to-end run against local mock servers
// (127.0.0.1 only; nothing leaves the machine).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { deflateSync } from 'node:zlib';
import { normalizeUrl, joinUrl,
  extractTxs, extractBlockIds, refBlockNumber, classifyError, checkTargetSafety, compareOutcome, summarise,
  packedTrxId, statusName, XPR_MAINNET_CHAIN_ID, decideReference, referenceEndpoints, hostOf,
  MAINNET_REFERENCE_ENDPOINTS, postJson, run as runTool,
} from '../shadow-replay.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const TOOL = join(HERE, '..', 'shadow-replay.mjs');
const BLOCKS = JSON.parse(readFileSync(join(HERE, 'fixtures/shadow-replay/blocks.json'), 'utf8'));
const H = 1000;
const ID1 = 'd0691df4c9f54fb5c5e986529376ebe541982fded830bc975012712f09636140';
const ID2 = 'e6f8a09f627edccfd262bee03b75cf6e4b0c14417b6bbd80eb15f2b62a4c876e';
const ID3 = 'fb84b4e91ad8c50fcac397eac264588713bb4da25b2205c1d1f6353c44b6f97a';

test('packed transaction id is the sha256 of the (decompressed) bytes', () => {
  const hex = Buffer.from('trx-one').toString('hex');
  assert.equal(packedTrxId(hex, 'none'), ID1);
  assert.equal(packedTrxId(deflateSync(Buffer.from('trx-one')).toString('hex'), 'zlib'), ID1);
  assert.equal(statusName(0), 'executed');
  assert.equal(statusName(2), 'hard_fail');
  assert.equal(statusName('soft_fail'), 'soft_fail');
});

test('TAPOS reference resolves to the most recent block with those low 16 bits', () => {
  assert.equal(refBlockNumber(990, 1001), 990);
  assert.equal(refBlockNumber(1001, 1002), 1001);
  assert.equal(refBlockNumber(0xfff0, 0x10005), 0xfff0); // wraps across a 65,536 boundary
  assert.equal(refBlockNumber(408328640 & 0xffff, 408328650), 408328640);
});

test('extractTxs: replayable packed transactions, deferred ids, TAPOS after the cut, mainnet status', () => {
  const rows = BLOCKS.flatMap((b) => extractTxs(b, H));
  assert.equal(rows.length, 4);
  const [a, d, b, c] = rows;
  assert.equal(a.id, ID1); assert.equal(a.replayable, true); assert.deepEqual(a.notes, []);
  assert.equal(a.payload.packed_trx, Buffer.from('trx-one').toString('hex'));
  assert.equal(d.replayable, false); assert.equal(d.reason, 'deferred');
  assert.equal(b.id, ID2); assert.deepEqual(b.notes, ['tapos_ref_after_cut']);
  assert.equal(c.id, ID3); assert.deepEqual(c.notes, ['mainnet_hard_fail']);
  assert.equal(a.cpu_usage_us, 120);
});

test('extractBlockIds reads /v1 and native receipt shapes', () => {
  const hex = Buffer.from('trx-two').toString('hex');
  const v1 = extractBlockIds({ transactions: [{ status: 'executed', trx: { id: ID1.toUpperCase() } }, { status: 'executed', trx: 'ab'.repeat(32) }] });
  assert.equal(v1.get(ID1), 'executed');
  assert.equal(v1.get('ab'.repeat(32)), 'executed');
  const native = extractBlockIds({ transactions: [{ status: 0, trx: [1, { packed_trx: hex, compression: 0 }] }] });
  assert.equal(native.get(ID2), 'executed');
});

test('round 2: a receipt without a status is unknown, never a match', () => {
  // Review (Codex r30-2 #10): a shadow receipt carrying only the transaction id used to default to "executed" and
  // matched an executed mainnet transaction although its outcome was unknown.
  const ids = extractBlockIds({ transactions: [{ trx: 'ab'.repeat(32) }] });
  assert.equal(ids.get('ab'.repeat(32)), 'unknown');
  const tx = { replayable: true, status: 'executed' };
  const r = compareOutcome(tx, { accepted: true, included: true, included_status: 'unknown' });
  assert.equal(r.result, 'unknown');
  assert.equal(r.category, 'shadow_status_unknown');
  assert.equal(compareOutcome(tx, { accepted: true, included: true, included_status: null }).result, 'unknown');
  // A mainnet receipt without a status is unknown too, whatever the shadow did.
  assert.equal(compareOutcome({ replayable: true, status: 'unknown' }, { accepted: true, included: true, included_status: 'executed' }).result, 'unknown');
  const sum = summarise([{ replayable: true, status: 'executed', compare: r }]);
  assert.equal(sum.result.unknown, 1);
  assert.equal(sum.result.match, undefined);
});

test('round 3: only Leap statuses are outcomes; equal unsupported statuses are unknown, never a match', () => {
  for (const s of [9, -1, 1.5, 'status_9', 'new_status', '', null, undefined]) assert.equal(statusName(s), 'unknown', String(s));
  assert.equal(statusName(' Executed '), 'executed');
  for (const s of ['status_9', 'new_status', '']) {
    const r = compareOutcome({ replayable: true, status: s }, { accepted: true, included: true, included_status: s });
    assert.equal(r.result, 'unknown', s);
  }
  assert.equal(compareOutcome({ replayable: true, status: 'executed' }, { accepted: true, included: true, included_status: 'status_9' }).result, 'unknown');
});

test('classifyError buckets the usual rejections', () => {
  assert.equal(classifyError('expired_tx_exception: Expired Transaction'), 'expired');
  assert.equal(classifyError('invalid_ref_block_exception: Transaction\'s reference block did not match'), 'tapos');
  assert.equal(classifyError('tx_duplicate: Duplicate transaction'), 'duplicate');
  assert.equal(classifyError('tx_cpu_usage_exceeded: billed CPU time is greater than the maximum'), 'resource');
  assert.equal(classifyError('eosio_assert_message_exception: assertion failure with message: overdrawn'), 'assertion');
  assert.equal(classifyError('unsatisfied_authorization: transaction declares authority'), 'authorization');
  assert.equal(classifyError('something new'), 'other');
});

const shadow = (over = {}) => ({ host: '10.0.0.5', chain_id: XPR_MAINNET_CHAIN_ID, head: 1001, block_at_cut_id: 'cut', block_after_cut_id: 'shadow-1001', ...over });
const src = { chain_id: XPR_MAINNET_CHAIN_ID, block_after_cut_id: 'mainnet-1001' };
// An authenticated reference that agrees with `src` (mainnet's H+1 = mainnet-1001).
const ref = { ok: true, block_after_cut_id: 'mainnet-1001', agreeing: ['a:443', 'b:443'], reasons: [] };
const safe = (o) => checkTargetSafety({ reference: ref, ...o });

test('safety: a mainnet-chain_id target needs the flag, the cut id and matching lineage', () => {
  assert.ok(safe({ target: shadow(), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true }).ok);
  const noFlag = safe({ target: shadow(), source: src, cutHeight: H, cutId: 'cut', iUnderstand: false });
  assert.ok(!noFlag.ok && noFlag.reasons.some((r) => r.includes('--shadow-i-understand')));
  const noCut = safe({ target: shadow(), source: src, cutHeight: H, cutId: undefined, iUnderstand: true });
  assert.ok(!noCut.ok && noCut.reasons.some((r) => r.includes('--cut-id')));
  const wrongCut = safe({ target: shadow({ block_at_cut_id: 'other' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!wrongCut.ok && wrongCut.reasons.some((r) => r.includes('not a shadow of this cut')));
});

test('safety: refuses public mainnet hosts, the real chain (same H+1) and a chain_id mismatch', () => {
  const pub = safe({ target: shadow({ host: 'proton.greymass.com' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!pub.ok && pub.reasons.some((r) => r.includes('public XPR mainnet API')));
  const extra = safe({ target: shadow({ host: 'rpc.example.org' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true, denyHosts: ['example.org'] });
  assert.ok(!extra.ok);
  const real = safe({ target: shadow({ head: 2000, block_after_cut_id: 'MAINNET-1001' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!real.ok && real.reasons.some((r) => r.includes('following the real chain')));
  const other = safe({ target: shadow({ chain_id: 'aa'.repeat(32) }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!other.ok && other.reasons.some((r) => r.includes('cannot verify')));
});

test('safety: lineage must be proven; every missing or failed answer refuses', () => {
  const ok = (t, s = src, cutId = 'cut') => safe({ target: shadow(t), source: s, cutHeight: H, cutId, iUnderstand: true });
  const refusedWith = (res, text) => assert.ok(!res.ok && res.reasons.some((x) => x.includes(text)), JSON.stringify(res.reasons));
  // the source's H+1 is unknown (fetch failure, or a blocks file without it)
  refusedWith(ok({}, { chain_id: XPR_MAINNET_CHAIN_ID, block_after_cut_id: null }), "mainnet's block H+1");
  // the target has not produced H+1, or its answer failed
  refusedWith(ok({ head: 1000, block_after_cut_id: null, block_after_cut_error: 'target head 1000 is not past H' }), 'cannot show its block H+1');
  refusedWith(ok({ block_after_cut_id: null, block_after_cut_error: 'HTTP 0 timeout' }), 'HTTP 0 timeout');
  // the target cannot show H, for any chain_id
  refusedWith(ok({ block_at_cut_id: null, block_at_cut_error: 'unknown block' }), 'cannot show its block at H');
  refusedWith(ok({ chain_id: 'aa'.repeat(32), block_at_cut_id: null }, { chain_id: 'aa'.repeat(32), block_after_cut_id: 'm-1001' }, 'cut', { ...ref, block_after_cut_id: 'm-1001' }), 'cannot show its block at H');
  // a non-mainnet chain_id still needs the cut id
  refusedWith(ok({ chain_id: 'aa'.repeat(32) }, { chain_id: 'aa'.repeat(32), block_after_cut_id: 'm-1001' }, null, { ...ref, block_after_cut_id: 'm-1001' }), '--cut-id');
  // an unknown source chain_id
  refusedWith(ok({}, { chain_id: null, block_after_cut_id: 'mainnet-1001' }), 'source chain_id is unknown');
  // a fully proven shadow on another chain_id passes without the mainnet flag
  assert.ok(checkTargetSafety({ target: shadow({ chain_id: 'aa'.repeat(32) }), source: { chain_id: 'aa'.repeat(32), block_after_cut_id: 'm-1001' }, reference: { ...ref, block_after_cut_id: 'm-1001' }, cutHeight: H, cutId: 'cut', iUnderstand: false }).ok);
});

test('compareOutcome: only hard_fail and expired may match a rejection, with a compatible reason', () => {
  const rej = (error) => ({ rejected: true, error });
  const inc = (st) => ({ accepted: true, included: true, included_status: st });
  const c = (status, shadow) => compareOutcome({ replayable: true, status }, shadow);
  // delayed = accepted and queued on mainnet: a rejection is a mismatch, whatever the reason
  assert.deepEqual(c('delayed', rej('unsatisfied_authorization: transaction declares authority')), { result: 'mismatch', category: 'mainnet_delayed_vs_rejected_authorization' });
  assert.deepEqual(c('delayed', rej('assertion failure')), { result: 'mismatch', category: 'mainnet_delayed_vs_rejected_assertion' });
  assert.deepEqual(c('delayed', inc('delayed')), { result: 'match', category: 'match' });
  assert.deepEqual(c('delayed', inc('executed')), { result: 'mismatch', category: 'status_delayed_vs_executed' });
  // hard_fail: an execution failure matches, an admission rejection does not
  assert.deepEqual(c('hard_fail', rej('eosio_assert_message_exception: overdrawn')), { result: 'match', category: 'assertion' });
  assert.deepEqual(c('hard_fail', rej('unsatisfied_authorization')), { result: 'mismatch', category: 'mainnet_hard_fail_vs_rejected_authorization' });
  assert.deepEqual(c('hard_fail', rej('billed CPU time exceeded')), { result: 'mismatch', category: 'mainnet_hard_fail_vs_rejected_resource' });
  assert.deepEqual(c('hard_fail', inc('hard_fail')), { result: 'match', category: 'match' });
  // expired: only an expiry rejection matches
  assert.deepEqual(c('expired', rej('expired_tx_exception')), { result: 'match', category: 'expired' });
  assert.deepEqual(c('expired', rej('duplicate transaction')), { result: 'mismatch', category: 'mainnet_expired_vs_rejected_duplicate' });
  // soft_fail: must be included as soft_fail
  assert.deepEqual(c('soft_fail', rej('assertion failure')), { result: 'mismatch', category: 'mainnet_soft_fail_vs_rejected_assertion' });
  assert.deepEqual(c('soft_fail', inc('soft_fail')), { result: 'match', category: 'match' });
  // executed: unchanged
  assert.deepEqual(c('executed', rej('Expired Transaction')), { result: 'mismatch', category: 'expired' });
  // an unknown mainnet status never matches a rejection: it is unknown (inconclusive, round 3), never a match
  assert.equal(c('status_9', rej('assertion failure')).result, 'unknown');
});

test('compareOutcome and summarise', () => {
  const tx = { replayable: true, status: 'executed' };
  assert.deepEqual(compareOutcome(tx, { accepted: true, included: true, included_status: 'executed' }), { result: 'match', category: 'match' });
  assert.deepEqual(compareOutcome(tx, { rejected: true, error: 'Expired Transaction' }), { result: 'mismatch', category: 'expired' });
  assert.deepEqual(compareOutcome(tx, { accepted: true, included: false }), { result: 'mismatch', category: 'missing' });
  assert.deepEqual(compareOutcome({ replayable: true, status: 'hard_fail' }, { rejected: true, error: 'assertion failure' }), { result: 'match', category: 'assertion' });
  assert.equal(compareOutcome({ replayable: false, reason: 'deferred' }, null).category, 'deferred');
  const s = summarise([{ status: 'executed', replayable: true, notes: [], compare: { result: 'match' } }, { status: 'executed', replayable: false, reason: 'deferred', compare: { result: 'skipped' } }]);
  assert.equal(s.transactions, 2); assert.equal(s.replayable, 1); assert.equal(s.not_replayable.deferred, 1); assert.equal(s.result.match, 1);
});

test('round 4: mainnet H+1 is authenticated by >= 2 independent agreeing references, never by the source', () => {
  const A = (host, over = {}) => ({ endpoint: `https://${host}`, host, chain_id: XPR_MAINNET_CHAIN_ID, block_at_cut_id: 'cut', block_after_cut_id: 'mainnet-1001', ...over });
  const d = (answers, o = {}) => decideReference({ answers, chainId: XPR_MAINNET_CHAIN_ID, cutId: 'cut', ...o });
  assert.ok(d([A('a'), A('b')]).ok);
  assert.equal(d([A('a'), A('b')]).block_after_cut_id, 'mainnet-1001');
  // one agreeing host is not enough; two answers from ONE host are one voice
  assert.ok(!d([A('a')]).ok);
  assert.ok(!d([A('a'), { ...A('a'), endpoint: 'https://a/v2' }]).ok);
  // errors, another chain_id, missing blocks do not count
  assert.ok(!d([A('a'), A('b', { error: 'HTTP 0' })]).ok);
  assert.ok(!d([A('a'), A('b', { chain_id: 'aa'.repeat(32) })]).ok);
  assert.ok(!d([A('a'), A('b', { block_after_cut_id: null })]).ok);
  // any disagreement refuses, even with a majority
  const dis = d([A('a'), A('b'), A('c', { block_after_cut_id: 'other' })]);
  assert.ok(!dis.ok && dis.reasons.some((r) => r.includes('disagree')), JSON.stringify(dis));
  // the cut must be the reference chain's H
  const wrongCut = d([A('a', { block_at_cut_id: 'x' }), A('b', { block_at_cut_id: 'x' })]);
  assert.ok(!wrongCut.ok && wrongCut.reasons.some((r) => r.includes('--cut-id')));
  // the source and the target never vouch
  assert.ok(!d([A('a'), A('b')], { excludeHosts: ['b'] }).ok);
});

test('round 5: only pinned mainnet endpoints vouch; added ones can only refuse; no env override exists', () => {
  const m = referenceEndpoints({ chainId: XPR_MAINNET_CHAIN_ID, mainnetAdded: ['https://extra.example'] });
  assert.ok(m.pinned);
  for (const e of MAINNET_REFERENCE_ENDPOINTS) assert.ok(m.endpoints.includes(e), e);
  assert.ok(m.endpoints.includes('https://extra.example'), 'added endpoints are still asked');
  assert.ok(!m.authorities.some((u) => u.includes('extra.example')), 'but never vouch');
  assert.deepEqual(m.authorities, MAINNET_REFERENCE_ENDPOINTS.map(normalizeUrl));
  assert.ok(new Set(m.authorities.map(hostOf)).size >= 2, 'the pinned list spans at least two hostnames');
  // the old test-only environment override is gone: setting it changes nothing
  const before = process.env.PULSE_SHADOW_REPLAY_TEST_ONLY_LOOPBACK_REFERENCES;
  process.env.PULSE_SHADOW_REPLAY_TEST_ONLY_LOOPBACK_REFERENCES = '1';
  try {
    const e = referenceEndpoints({ chainId: XPR_MAINNET_CHAIN_ID, mainnetAdded: ['http://127.0.0.1:8001', 'http://127.0.0.1:8002'] });
    assert.ok(e.pinned && MAINNET_REFERENCE_ENDPOINTS.every((x) => e.endpoints.includes(x)));
    assert.deepEqual(e.authorities, MAINNET_REFERENCE_ENDPOINTS.map(normalizeUrl));
  } finally {
    if (before === undefined) delete process.env.PULSE_SHADOW_REPLAY_TEST_ONLY_LOOPBACK_REFERENCES;
    else process.env.PULSE_SHADOW_REPLAY_TEST_ONLY_LOOPBACK_REFERENCES = before;
  }
  assert.ok(!readFileSync(TOOL, 'utf8').includes('process.env'), 'the tool reads no environment variable');
  // another chain_id: what the user gives vouches
  const o = referenceEndpoints({ chainId: 'aa'.repeat(32), referenceGiven: ['https://r1', 'https://r2'] });
  assert.deepEqual(o.endpoints, ['https://r1', 'https://r2']); assert.equal(o.authorities, null);

  const A = (host, over = {}) => ({ endpoint: `https://${host}`, host, chain_id: XPR_MAINNET_CHAIN_ID, block_at_cut_id: 'cut', block_after_cut_id: 'mainnet-1001', ...over });
  const auth = ['https://pin-a.example', 'https://pin-b.example'];
  const d = (answers) => decideReference({ answers, chainId: XPR_MAINNET_CHAIN_ID, cutId: 'cut', authorities: auth });
  // every pinned endpoint down, two custom ports on one box agreeing: refused
  const custom = d([A('pin-a.example', { error: 'HTTP 0' }), A('pin-b.example', { error: 'HTTP 0' }), A('127.0.0.1'), A('127.0.0.1')]);
  assert.ok(!custom.ok && custom.reasons.some((r) => r.includes('pinned')), JSON.stringify(custom));
  // two custom HOSTS agreeing, pinned down: still refused (they never vouch)
  assert.ok(!d([A('pin-a.example', { error: 'x' }), A('c1.example'), A('c2.example')]).ok);
  // one pinned + one custom: refused
  assert.ok(!d([A('pin-a.example'), A('c1.example')]).ok);
  // two pinned agreeing: ok, and only they are reported as vouching
  const two = d([A('pin-a.example'), A('pin-b.example'), A('c1.example')]);
  assert.ok(two.ok, JSON.stringify(two)); assert.deepEqual(two.agreeing, auth.map(hostOf));
  // any disagreement refuses, including from an added endpoint
  assert.ok(!d([A('pin-a.example'), A('pin-b.example'), A('c1.example', { block_after_cut_id: 'other' })]).ok);
  assert.ok(!d([A('pin-a.example'), A('pin-b.example', { block_after_cut_id: 'other' })]).ok);
  // independence is per hostname: one pinned host on two ports is one voice
  assert.ok(!decideReference({ answers: [A('pin-a.example'), { ...A('pin-a.example'), endpoint: 'https://pin-a.example:8443' }],
    chainId: XPR_MAINNET_CHAIN_ID, cutId: 'cut', authorities: ['https://pin-a.example'] }).ok);
  assert.equal(hostOf('http://127.0.0.1:8001'), hostOf('http://127.0.0.1:8002'));
});

test('round 4: the swap attack (a shadow passed as the source) and a target on the real chain are refused', () => {
  // The supplied "source" is really a shadow (its H+1 is shadow-1001); the target is real mainnet (H+1 = mainnet-1001).
  const swap = checkTargetSafety({
    target: shadow({ block_after_cut_id: 'mainnet-1001' }), source: { chain_id: XPR_MAINNET_CHAIN_ID, block_after_cut_id: 'shadow-1001' },
    reference: ref, cutHeight: H, cutId: 'cut', iUnderstand: true,
  });
  assert.ok(!swap.ok);
  assert.ok(swap.reasons.some((r) => r.includes('not the chain it claims')), JSON.stringify(swap.reasons));
  assert.ok(swap.reasons.some((r) => r.includes('authenticated reference H+1')), JSON.stringify(swap.reasons));
  // no authenticated reference at all: refused
  const none = checkTargetSafety({ target: shadow(), source: src, reference: { ok: false, reasons: ['only 0 independent reference endpoint(s)'] }, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!none.ok && none.reasons.some((r) => r.includes('could not be authenticated')));
  assert.ok(!checkTargetSafety({ target: shadow(), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true }).ok);
});

// ---- end to end against two mock servers on 127.0.0.1 ----
function mock(handler) {
  return new Promise((res) => {
    const srv = createServer((req, rsp) => {
      let body = ''; req.on('data', (c) => (body += c));
      req.on('end', () => {
        const [status, json, headers = {}] = handler(req.url, body ? JSON.parse(body) : {});
        rsp.writeHead(status, { 'content-type': 'application/json', ...headers }); rsp.end(JSON.stringify(json));
      });
    });
    srv.listen(0, '127.0.0.1', () => res(srv));
  });
}
/** The CLI in-process (round 5: references are injected through run()'s options; no environment variable). */
const run = async (args, opts = {}) => {
  let out = '', err = '';
  const code = await runTool([process.execPath, TOOL, ...args], { ...opts, log: (m) => { out += `${m}\n`; }, err: (m) => { err += `${m}\n`; } });
  return { code, out, err };
};
/** The real CLI as a child process (exit code wiring). */
const runCli = (args) => new Promise((res) => {
  const p = spawn(process.execPath, [TOOL, ...args]); let out = '', err = '';
  p.stdout.on('data', (d) => (out += d)); p.stderr.on('data', (d) => (err += d));
  p.on('close', (code) => res({ code, out, err }));
});

/**
 * Injected mainnet references: `pinned` stand-ins for the pinned list (distinct hostnames, answered in memory, no
 * network), serving the cut block and mainnet's real H+1 (fixture id 000003e9aa). `answers` overrides per hostname.
 */
async function references(cutBlock, { pinned = 2, answers = {} } = {}) {
  const pinnedEndpoints = ['https://ref-a.example', 'https://ref-b.example', 'https://ref-c.example'].slice(0, pinned);
  const after = BLOCKS.find((x) => x.block_num === H + 1).id;
  const referenceFetcher = async (endpoints) => endpoints.map((endpoint) => {
    const host = hostOf(endpoint);
    return answers[host] ?? { endpoint, host, chain_id: XPR_MAINNET_CHAIN_ID, block_at_cut_id: cutBlock.id, block_after_cut_id: after };
  });
  return { opts: { pinnedReferenceEndpoints: pinnedEndpoints, referenceFetcher }, close: () => {} };
}

test('end to end: dry run needs no target; send replays, confirms inclusion and reports a mismatch', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  const sent = [];
  let head = H + 1; const shadowBlocks = { [H]: cutBlock, [H + 1]: { block_num: H + 1, id: 'shadow-own-1001', transactions: [] } };
  const target = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: head }];
    if (url.endsWith('/get_block')) { const blk = shadowBlocks[Number(b.block_num_or_id)]; return blk ? [200, blk] : [400, { error: { what: 'unknown block' } }]; }
    if (url.endsWith('/send_transaction')) {
      sent.push(b);
      const id = packedTrxId(b.packed_trx, b.compression);
      if (id === ID2) return [500, { error: { name: 'tx_cpu_usage_exceeded', what: 'billed CPU time exceeded' } }];
      head += 1; shadowBlocks[head] = { block_num: head, id: `shadow-${head}`, transactions: [{ status: 'executed', trx: { id } }] };
      return [202, { transaction_id: id }];
    }
    return [404, {}];
  });
  const S = `http://127.0.0.1:${source.address().port}`, T = `http://127.0.0.1:${target.address().port}`;
  const refs = await references(cutBlock);
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-'));
  try {
    const dry = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--out', join(dir, 'dry.json')]);
    assert.equal(dry.code, 0, dry.err);
    const d = JSON.parse(readFileSync(join(dir, 'dry.json'), 'utf8'));
    assert.equal(d.mode, 'dry-run'); assert.equal(d.summary.transactions, 4); assert.equal(sent.length, 0);

    const refused = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send'], refs.opts);
    assert.equal(refused.code, 2); assert.match(refused.err, /shadow-i-understand/); assert.equal(sent.length, 0);

    const r = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '5', '--out', join(dir, 'send.json')], refs.opts);
    const rep = JSON.parse(readFileSync(join(dir, 'send.json'), 'utf8'));
    assert.equal(r.code, 1, r.err);
    assert.equal(sent.length, 3, 'deferred id is never sent');
    assert.ok(sent.every((p) => p.signatures && p.packed_trx && !('private_key' in p)), 'forwards exactly what mainnet received');
    assert.equal(rep.summary.result.match, 1); // ID1: executed on both
    assert.equal(rep.summary.mismatch_categories.resource, 1); // ID2: CPU rejection on the shadow
    assert.equal(rep.summary.mismatch_categories.status_hard_fail_vs_executed, 1); // ID3: failed on mainnet, executed on the shadow
    assert.equal(rep.summary.predicted.tapos_ref_after_cut, 1);
    assert.ok(!JSON.stringify(rep).includes('SIG_K1_a'), 'the report never carries signatures or payloads');
    assert.equal(rep.reference.ok, true); assert.equal(rep.reference.agreeing.length, 2);
  } finally { source.close(); target.close(); refs.close(); }
});

test('end to end: refuses when lineage cannot be proven (no shadow H+1, failed H+1 read, blocks file without H+1)', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  const sent = [];
  let mode = 'at_cut';
  const target = await mock((url, b) => {
    const n = Number(b.block_num_or_id);
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: mode === 'at_cut' ? H : H + 1 }];
    if (url.endsWith('/get_block')) {
      if (n === H) return [200, cutBlock];
      if (mode === 'h1_error') return [500, { error: { what: 'internal error' } }];
      return [400, { error: { what: 'unknown block' } }];
    }
    if (url.endsWith('/send_transaction')) { sent.push(b); return [202, {}]; }
    return [404, {}];
  });
  const S = `http://127.0.0.1:${source.address().port}`, T = `http://127.0.0.1:${target.address().port}`;
  const refs = await references(cutBlock);
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-'));
  const base = ['--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '1'];
  try {
    // the shadow has not produced its own H+1 yet
    let r = await run(['--source', S, ...base], refs.opts);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /block H\+1/); assert.equal(sent.length, 0);
    // its H+1 read fails
    mode = 'h1_error';
    r = await run(['--source', S, ...base], refs.opts);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /lineage unproven/); assert.equal(sent.length, 0);
    // a blocks file that lacks mainnet's H+1 (only H+2), against an otherwise fine target
    const file = join(dir, 'blocks.json');
    writeFileSync(file, JSON.stringify(BLOCKS.filter((x) => x.block_num !== H + 1)));
    r = await run(['--blocks-file', file, '--source-chain-id', XPR_MAINNET_CHAIN_ID, ...base], refs.opts);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /mainnet's block H\+1/); assert.equal(sent.length, 0);
    // a blocks file without --source-chain-id
    r = await run(['--blocks-file', file, ...base], refs.opts);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /source chain_id is unknown/); assert.equal(sent.length, 0);
  } finally { source.close(); target.close(); refs.close(); }
});

test('end to end (round 3): unknown outcomes are inconclusive: exit 3 unless --allow-inconclusive', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  let head = H + 1; const shadowBlocks = { [H]: cutBlock, [H + 1]: { block_num: H + 1, id: 'shadow-own-1001', transactions: [] } };
  const target = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: head }];
    if (url.endsWith('/get_block')) { const blk = shadowBlocks[Number(b.block_num_or_id)]; return blk ? [200, blk] : [400, { error: { what: 'unknown block' } }]; }
    if (url.endsWith('/send_transaction')) {
      const id = packedTrxId(b.packed_trx, b.compression);
      // Every transaction lands, with a status Leap does not have: nothing about its outcome is known.
      head += 1; shadowBlocks[head] = { block_num: head, id: `shadow-${head}`, transactions: [{ status: 'status_9', trx: { id } }] };
      return [202, { transaction_id: id }];
    }
    return [404, {}];
  });
  const S = `http://127.0.0.1:${source.address().port}`, T = `http://127.0.0.1:${target.address().port}`;
  const refs = await references(cutBlock);
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-'));
  try {
    const base = ['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '5'];
    const r = await run([...base, '--out', join(dir, 'a.json')], refs.opts);
    const rep = JSON.parse(readFileSync(join(dir, 'a.json'), 'utf8'));
    assert.equal(rep.summary.result.match ?? 0, 0, 'no unknown outcome counts as a match');
    // ID3 failed on mainnet: its unknown shadow status is unknown too, so nothing mismatches and nothing matches.
    assert.equal(rep.verdict, 'inconclusive');
    assert.equal(r.code, 3, r.err);
    const allowed = await run([...base, '--allow-inconclusive'], refs.opts);
    assert.equal(allowed.code, 0, allowed.err);
  } finally { source.close(); target.close(); refs.close(); }
});

test('end to end (round 4): the swap attack is refused: a shadow as --source, real mainnet as --target, nothing sent', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  // The "source" is a shadow: its H+1 is its own block, not mainnet's.
  const shadowSrc = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) {
      const n = Number(b.block_num_or_id);
      if (n === H) return [200, cutBlock];
      const real = BLOCKS.find((x) => x.block_num === n);
      return [200, { ...real, id: `shadow-${n}` }];
    }
    return [404, {}];
  });
  // The "target" is real mainnet: same cut, mainnet's own H+1.
  const sent = [];
  const realTarget = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 5000 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : BLOCKS.find((x) => x.block_num === n)]; }
    if (url.endsWith('/send_transaction')) { sent.push(b); return [202, {}]; }
    return [404, {}];
  });
  const refs = await references(cutBlock);
  const S = `http://127.0.0.1:${shadowSrc.address().port}`, T = `http://127.0.0.1:${realTarget.address().port}`;
  try {
    const r = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '1'], refs.opts);
    assert.equal(r.code, 2, r.err);
    assert.match(r.err, /not the chain it claims/);
    assert.match(r.err, /following the real chain/);
    assert.equal(sent.length, 0, 'nothing reached the real chain');
    // with only one reference, nothing is authenticated: refused as well
    const one = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '1'], (await references(cutBlock, { pinned: 1 })).opts);
    assert.equal(one.code, 2, one.err); assert.match(one.err, /could not be authenticated/); assert.equal(sent.length, 0);
  } finally { shadowSrc.close(); realTarget.close(); refs.close(); }
});

test('end to end (round 4): a 307 on send stops the run; nothing reaches the redirect destination', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  const sink = [];
  const destination = await mock((url, b) => { sink.push({ url, b }); return [202, {}]; });
  const D = `http://127.0.0.1:${destination.address().port}`;
  let sends = 0;
  const shadowBlocks = { [H]: cutBlock, [H + 1]: { block_num: H + 1, id: 'shadow-own-1001', transactions: [] } };
  const target = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: H + 1 }];
    if (url.endsWith('/get_block')) { const blk = shadowBlocks[Number(b.block_num_or_id)]; return blk ? [200, blk] : [400, {}]; }
    if (url.endsWith('/send_transaction')) { sends += 1; return [307, {}, { location: `${D}/v1/chain/send_transaction` }]; }
    return [404, {}];
  });
  const refs = await references(cutBlock);
  const S = `http://127.0.0.1:${source.address().port}`, T = `http://127.0.0.1:${target.address().port}`;
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-'));
  try {
    const r = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '1', '--out', join(dir, 'r.json')], refs.opts);
    assert.equal(r.code, 2, r.err);
    assert.match(r.err, /redirect/);
    assert.equal(sends, 1, 'the run stops at the first redirected send');
    assert.equal(sink.length, 0, 'the signed body never reached the redirect destination');
    const rep = JSON.parse(readFileSync(join(dir, 'r.json'), 'utf8'));
    assert.match(rep.aborted, /redirect/);
    // reads never follow redirects either
    const red = await postJson(`${T}/v1/chain/send_transaction`, {});
    assert.equal(red.redirected, true); assert.equal(sink.length, 0);
  } finally { source.close(); target.close(); destination.close(); refs.close(); }
});

test('end to end (round 5): pinned references down and two custom mainnet hosts agreeing: refused, nothing sent', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  const sent = [];
  const shadowBlocks = { [H]: cutBlock, [H + 1]: { block_num: H + 1, id: 'shadow-own-1001', transactions: [] } };
  const target = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: H + 1 }];
    if (url.endsWith('/get_block')) { const blk = shadowBlocks[Number(b.block_num_or_id)]; return blk ? [200, blk] : [400, {}]; }
    if (url.endsWith('/send_transaction')) { sent.push(b); return [202, {}]; }
    return [404, {}];
  });
  const S = `http://127.0.0.1:${source.address().port}`, T = `http://127.0.0.1:${target.address().port}`;
  const down = (host) => ({ endpoint: `https://${host}`, host, error: 'get_info: HTTP 0' });
  const refs = await references(cutBlock, { answers: { 'ref-a.example': down('ref-a.example'), 'ref-b.example': down('ref-b.example') } });
  try {
    const r = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand',
      '--rate', '0', '--inclusion-timeout', '1', '--mainnet-endpoint', 'https://custom-a.example,https://custom-b.example'], refs.opts);
    assert.equal(r.code, 2, r.err);
    assert.match(r.err, /could not be authenticated/);
    assert.equal(sent.length, 0);
    // two pinned agreeing but an added endpoint disagreeing: refused too
    const after = BLOCKS.find((x) => x.block_num === H + 1).id;
    const dis = await references(cutBlock, { pinned: 2, answers: { 'custom.example': { endpoint: 'https://custom.example', host: 'custom.example',
      chain_id: XPR_MAINNET_CHAIN_ID, block_at_cut_id: 'cutid', block_after_cut_id: `${after}-other` } } });
    const r2 = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand',
      '--rate', '0', '--inclusion-timeout', '1', '--mainnet-endpoint', 'https://custom.example'], dis.opts);
    assert.equal(r2.code, 2, r2.err); assert.match(r2.err, /disagree|could not be authenticated/); assert.equal(sent.length, 0);
  } finally { source.close(); target.close(); }
});

test('CLI: the child process keeps its exit codes (usage error, dry run)', async () => {
  const bad = await runCli(['--cut-height', 'x']);
  assert.equal(bad.code, 2); assert.match(bad.err, /usage/);
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  try {
    const dry = await runCli(['--source', `http://127.0.0.1:${source.address().port}`, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2']);
    assert.equal(dry.code, 0, dry.err); assert.match(dry.out, /dry-run/);
  } finally { source.close(); }
});

test('round 6: authority is exact pinned-URL provenance, never a hostname (Codex R6-2)', () => {
  const pinned = ['https://pin-a.example', 'https://pin-b.example'];
  const refs = referenceEndpoints({ chainId: XPR_MAINNET_CHAIN_ID, pinned,
    mainnetAdded: ['http://pin-a.example', 'http://pin-b.example', 'https://pin-a.example:8443', 'https://pin-b.example/other'] });
  assert.deepEqual(refs.authorities, pinned, 'only the exact https pinned URLs are authorities');
  const A = (endpoint, over = {}) => ({ endpoint, host: hostOf(endpoint), chain_id: XPR_MAINNET_CHAIN_ID, block_at_cut_id: 'cut', block_after_cut_id: 'shadow-1001', ...over });
  const d = (answers) => decideReference({ answers, chainId: XPR_MAINNET_CHAIN_ID, cutId: 'cut', authorities: refs.authorities });
  // pinned https down + two added http:// URLs on the pinned hostnames agreeing: refused (the Codex reproduction)
  const attack = d([A('https://pin-a.example', { error: 'HTTP 0' }), A('https://pin-b.example', { error: 'HTTP 0' }),
    A('http://pin-a.example'), A('http://pin-b.example')]);
  assert.ok(!attack.ok && attack.reasons.some((r) => r.includes('exact built-in pinned')), JSON.stringify(attack));
  // added https URLs on a pinned hostname but another port / path never count
  assert.ok(!d([A('https://pin-a.example', { error: 'x' }), A('https://pin-b.example', { error: 'x' }),
    A('https://pin-a.example:8443'), A('https://pin-b.example/other')]).ok);
  // an answer claiming a pinned host in its `host` field does not borrow that host's authority
  assert.ok(!d([A('https://pin-a.example', { error: 'x' }), A('https://pin-b.example', { error: 'x' }),
    { ...A('http://evil.example'), host: 'pin-a.example' }, { ...A('http://evil2.example'), host: 'pin-b.example' }]).ok);
  // two distinct pinned URLs agreeing: ok; normalization tolerates case, a trailing slash and the default port
  const ok = d([A('HTTPS://PIN-A.example/'), A('https://pin-b.example:443')]);
  assert.ok(ok.ok, JSON.stringify(ok));
  // two pinned URLs on ONE hostname count once
  assert.ok(!decideReference({ answers: [A('https://pin-a.example'), A('https://pin-a.example/v1')], chainId: XPR_MAINNET_CHAIN_ID,
    cutId: 'cut', authorities: ['https://pin-a.example', 'https://pin-a.example/v1'] }).ok);
  // an added endpoint on a pinned hostname that disagrees still refuses
  assert.ok(!d([A('https://pin-a.example'), A('https://pin-b.example'), A('http://pin-a.example', { block_after_cut_id: 'other' })]).ok);
  // userinfo / query / fragment never normalize (never match a pinned URL)
  for (const u of ['https://x@pin-a.example', 'https://pin-a.example/?q=1', 'https://pin-a.example/#f', 'ftp://pin-a.example', 'not a url'])
    assert.equal(normalizeUrl(u), null, u);
  // a non-https pinned URL is never an authority
  assert.deepEqual(referenceEndpoints({ chainId: XPR_MAINNET_CHAIN_ID, pinned: ['http://pin-a.example', 'https://pin-b.example'] }).authorities, ['https://pin-b.example']);
});

test('round 6 end to end: the Codex reproduction (pinned down, added http URLs on pinned hostnames vouching for a swap) sends nothing', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const shadowAfter = { block_num: H + 1, id: 'shadow-own-1001', transactions: [] };
  // The supplied source is really a shadow; the target is "real mainnet".
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : n === H + 1 ? shadowAfter : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  const sent = [];
  const realAfter = BLOCKS.find((x) => x.block_num === H + 1);
  const target = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : n === H + 1 ? realAfter : BLOCKS.find((x) => x.block_num === n)]; }
    if (url.endsWith('/send_transaction')) { sent.push(b); return [202, { transaction_id: 'x' }]; }
    return [404, {}];
  });
  const S = `http://127.0.0.1:${source.address().port}`, T = `http://127.0.0.1:${target.address().port}`;
  const pinnedReferenceEndpoints = ['https://ref-a.example', 'https://ref-b.example'];
  // Pinned https down; the attacker's added http:// URLs on the pinned hostnames vouch for the shadow's H+1.
  const referenceFetcher = async (endpoints) => endpoints.map((endpoint) => (endpoint.startsWith('https://')
    ? { endpoint, host: hostOf(endpoint), error: 'HTTP 0' }
    : { endpoint, host: hostOf(endpoint), chain_id: XPR_MAINNET_CHAIN_ID, block_at_cut_id: cutBlock.id, block_after_cut_id: shadowAfter.id }));
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-r6-'));
  try {
    const code = await run(['node', TOOL, '--source', S, '--cut-height', String(H), '--cut-id', cutBlock.id, '--blocks', '2',
      '--target', T, '--send', '--shadow-i-understand', '--rate', '50',
      '--mainnet-endpoint', 'http://ref-a.example', '--mainnet-endpoint', 'http://ref-b.example',
      '--out', join(dir, 'r.json')], { pinnedReferenceEndpoints, referenceFetcher });
    assert.notEqual(code, 0, 'the run is refused');
    assert.equal(sent.length, 0, 'nothing reached the target');
    const rep = JSON.parse(readFileSync(join(dir, 'r.json'), 'utf8'));
    assert.equal(rep.reference.ok, false, JSON.stringify(rep.reference));
  } finally { source.close(); target.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('round 7: an added URL that NORMALIZES to a pinned one (https://<pinned-host>//) never vouches; nothing is sent', async () => {
  const cutBlock = { block_num: H, id: 'cutid', transactions: [] };
  const shadowAfter = { block_num: H + 1, id: 'shadow-own-1001', transactions: [] };
  const source = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : n === H + 1 ? shadowAfter : BLOCKS.find((x) => x.block_num === n)]; }
    return [404, {}];
  });
  const sent = [];
  const realAfter = BLOCKS.find((x) => x.block_num === H + 1);
  const target = await mock((url, b) => {
    if (url.endsWith('/get_info')) return [200, { chain_id: XPR_MAINNET_CHAIN_ID, head_block_num: 1002 }];
    if (url.endsWith('/get_block')) { const n = Number(b.block_num_or_id); return [200, n === H ? cutBlock : n === H + 1 ? realAfter : BLOCKS.find((x) => x.block_num === n)]; }
    if (url.endsWith('/send_transaction')) { sent.push(b); return [202, { transaction_id: 'x' }]; }
    return [404, {}];
  });
  const S = `http://127.0.0.1:${source.address().port}`, T = `http://127.0.0.1:${target.address().port}`;
  const pinnedReferenceEndpoints = ['https://ref-a.example', 'https://ref-b.example'];
  // The exact pinned URLs are down; the attacker's `//` variants (same normalized URL) vouch for the shadow's H+1.
  const referenceFetcher = async (endpoints) => endpoints.map((endpoint) => (pinnedReferenceEndpoints.includes(endpoint)
    ? { endpoint, host: hostOf(endpoint), error: 'HTTP 0' }
    : { endpoint, host: hostOf(endpoint), chain_id: XPR_MAINNET_CHAIN_ID, block_at_cut_id: cutBlock.id, block_after_cut_id: shadowAfter.id }));
  assert.equal(normalizeUrl('https://ref-a.example//'), normalizeUrl('https://ref-a.example'), 'the attack relies on equal normalization');
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-r7-'));
  try {
    const code = await run(['node', TOOL, '--source', S, '--cut-height', String(H), '--cut-id', cutBlock.id, '--blocks', '2',
      '--target', T, '--send', '--shadow-i-understand', '--rate', '50',
      '--mainnet-endpoint', 'https://ref-a.example//', '--mainnet-endpoint', 'https://ref-b.example//',
      '--out', join(dir, 'r.json')], { pinnedReferenceEndpoints, referenceFetcher });
    assert.notEqual(code, 0, 'the run is refused');
    assert.equal(sent.length, 0, 'nothing reached the target');
    const rep = JSON.parse(readFileSync(join(dir, 'r.json'), 'utf8'));
    assert.equal(rep.reference.ok, false, JSON.stringify(rep.reference));
  } finally { source.close(); target.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('round 7: one URL join for every request: the requested URL is the normalized endpoint plus the API path', () => {
  assert.equal(joinUrl('https://a.example//', '/v1/chain/get_info'), 'https://a.example/v1/chain/get_info');
  assert.equal(joinUrl('https://a.example/', '/v1/chain/get_info'), 'https://a.example/v1/chain/get_info');
  assert.equal(joinUrl('https://a.example', '/v1/chain/get_info'), 'https://a.example/v1/chain/get_info');
  // referenceEndpoints marks vouching by position: pinned https entries only.
  const r = referenceEndpoints({ chainId: XPR_MAINNET_CHAIN_ID, pinned: ['https://p1.example', 'http://p2.example'], mainnetAdded: ['https://p1.example'] });
  assert.deepEqual(r.vouches, [true, false, false]);
});
