// node --test tools/test/shadow-replay.test.mjs
// tools/shadow-replay.mjs: classification, target safety and an end-to-end run against local mock servers
// (127.0.0.1 only; nothing leaves the machine).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { deflateSync } from 'node:zlib';
import {
  extractTxs, extractBlockIds, refBlockNumber, classifyError, checkTargetSafety, compareOutcome, summarise,
  packedTrxId, statusName, XPR_MAINNET_CHAIN_ID,
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

test('safety: a mainnet-chain_id target needs the flag, the cut id and matching lineage', () => {
  assert.ok(checkTargetSafety({ target: shadow(), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true }).ok);
  const noFlag = checkTargetSafety({ target: shadow(), source: src, cutHeight: H, cutId: 'cut', iUnderstand: false });
  assert.ok(!noFlag.ok && noFlag.reasons.some((r) => r.includes('--shadow-i-understand')));
  const noCut = checkTargetSafety({ target: shadow(), source: src, cutHeight: H, cutId: undefined, iUnderstand: true });
  assert.ok(!noCut.ok && noCut.reasons.some((r) => r.includes('--cut-id')));
  const wrongCut = checkTargetSafety({ target: shadow({ block_at_cut_id: 'other' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!wrongCut.ok && wrongCut.reasons.some((r) => r.includes('not a shadow of this cut')));
});

test('safety: refuses public mainnet hosts, the real chain (same H+1) and a chain_id mismatch', () => {
  const pub = checkTargetSafety({ target: shadow({ host: 'proton.greymass.com' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!pub.ok && pub.reasons.some((r) => r.includes('public XPR mainnet API')));
  const extra = checkTargetSafety({ target: shadow({ host: 'rpc.example.org' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true, denyHosts: ['example.org'] });
  assert.ok(!extra.ok);
  const real = checkTargetSafety({ target: shadow({ head: 2000, block_after_cut_id: 'MAINNET-1001' }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!real.ok && real.reasons.some((r) => r.includes('following the real chain')));
  const other = checkTargetSafety({ target: shadow({ chain_id: 'aa'.repeat(32) }), source: src, cutHeight: H, cutId: 'cut', iUnderstand: true });
  assert.ok(!other.ok && other.reasons.some((r) => r.includes('cannot verify')));
});

test('safety: lineage must be proven; every missing or failed answer refuses', () => {
  const ok = (t, s = src, cutId = 'cut') => checkTargetSafety({ target: shadow(t), source: s, cutHeight: H, cutId, iUnderstand: true });
  const refusedWith = (res, text) => assert.ok(!res.ok && res.reasons.some((x) => x.includes(text)), JSON.stringify(res.reasons));
  // the source's H+1 is unknown (fetch failure, or a blocks file without it)
  refusedWith(ok({}, { chain_id: XPR_MAINNET_CHAIN_ID, block_after_cut_id: null }), "mainnet's block H+1");
  // the target has not produced H+1, or its answer failed
  refusedWith(ok({ head: 1000, block_after_cut_id: null, block_after_cut_error: 'target head 1000 is not past H' }), 'cannot show its block H+1');
  refusedWith(ok({ block_after_cut_id: null, block_after_cut_error: 'HTTP 0 timeout' }), 'HTTP 0 timeout');
  // the target cannot show H, for any chain_id
  refusedWith(ok({ block_at_cut_id: null, block_at_cut_error: 'unknown block' }), 'cannot show its block at H');
  refusedWith(ok({ chain_id: 'aa'.repeat(32), block_at_cut_id: null }, { chain_id: 'aa'.repeat(32), block_after_cut_id: 'm-1001' }), 'cannot show its block at H');
  // a non-mainnet chain_id still needs the cut id
  refusedWith(ok({ chain_id: 'aa'.repeat(32) }, { chain_id: 'aa'.repeat(32), block_after_cut_id: 'm-1001' }, null), '--cut-id');
  // an unknown source chain_id
  refusedWith(ok({}, { chain_id: null, block_after_cut_id: 'mainnet-1001' }), 'source chain_id is unknown');
  // a fully proven shadow on another chain_id passes without the mainnet flag
  assert.ok(checkTargetSafety({ target: shadow({ chain_id: 'aa'.repeat(32) }), source: { chain_id: 'aa'.repeat(32), block_after_cut_id: 'm-1001' }, cutHeight: H, cutId: 'cut', iUnderstand: false }).ok);
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
  // an unknown mainnet status never matches a rejection
  assert.equal(c('status_9', rej('assertion failure')).result, 'mismatch');
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

// ---- end to end against two mock servers on 127.0.0.1 ----
function mock(handler) {
  return new Promise((res) => {
    const srv = createServer((req, rsp) => {
      let body = ''; req.on('data', (c) => (body += c));
      req.on('end', () => { const [status, json] = handler(req.url, body ? JSON.parse(body) : {}); rsp.writeHead(status, { 'content-type': 'application/json' }); rsp.end(JSON.stringify(json)); });
    });
    srv.listen(0, '127.0.0.1', () => res(srv));
  });
}
const run = (args) => new Promise((res) => {
  const p = spawn(process.execPath, [TOOL, ...args]); let out = '', err = '';
  p.stdout.on('data', (d) => (out += d)); p.stderr.on('data', (d) => (err += d));
  p.on('close', (code) => res({ code, out, err }));
});

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
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-'));
  try {
    const dry = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--out', join(dir, 'dry.json')]);
    assert.equal(dry.code, 0, dry.err);
    const d = JSON.parse(readFileSync(join(dir, 'dry.json'), 'utf8'));
    assert.equal(d.mode, 'dry-run'); assert.equal(d.summary.transactions, 4); assert.equal(sent.length, 0);

    const refused = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send']);
    assert.equal(refused.code, 2); assert.match(refused.err, /shadow-i-understand/); assert.equal(sent.length, 0);

    const r = await run(['--source', S, '--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '5', '--out', join(dir, 'send.json')]);
    const rep = JSON.parse(readFileSync(join(dir, 'send.json'), 'utf8'));
    assert.equal(r.code, 1, r.err);
    assert.equal(sent.length, 3, 'deferred id is never sent');
    assert.ok(sent.every((p) => p.signatures && p.packed_trx && !('private_key' in p)), 'forwards exactly what mainnet received');
    assert.equal(rep.summary.result.match, 1); // ID1: executed on both
    assert.equal(rep.summary.mismatch_categories.resource, 1); // ID2: CPU rejection on the shadow
    assert.equal(rep.summary.mismatch_categories.status_hard_fail_vs_executed, 1); // ID3: failed on mainnet, executed on the shadow
    assert.equal(rep.summary.predicted.tapos_ref_after_cut, 1);
    assert.ok(!JSON.stringify(rep).includes('SIG_K1_a'), 'the report never carries signatures or payloads');
  } finally { source.close(); target.close(); }
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
  const dir = mkdtempSync(join(tmpdir(), 'shadow-replay-'));
  const base = ['--cut-height', String(H), '--cut-id', 'cutid', '--blocks', '2', '--target', T, '--send', '--shadow-i-understand', '--rate', '0', '--inclusion-timeout', '1'];
  try {
    // the shadow has not produced its own H+1 yet
    let r = await run(['--source', S, ...base]);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /block H\+1/); assert.equal(sent.length, 0);
    // its H+1 read fails
    mode = 'h1_error';
    r = await run(['--source', S, ...base]);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /lineage unproven/); assert.equal(sent.length, 0);
    // a blocks file that lacks mainnet's H+1 (only H+2), against an otherwise fine target
    const file = join(dir, 'blocks.json');
    writeFileSync(file, JSON.stringify(BLOCKS.filter((x) => x.block_num !== H + 1)));
    r = await run(['--blocks-file', file, '--source-chain-id', XPR_MAINNET_CHAIN_ID, ...base]);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /mainnet's block H\+1/); assert.equal(sent.length, 0);
    // a blocks file without --source-chain-id
    r = await run(['--blocks-file', file, ...base]);
    assert.equal(r.code, 2, r.err); assert.match(r.err, /source chain_id is unknown/); assert.equal(sent.length, 0);
  } finally { source.close(); target.close(); }
});
