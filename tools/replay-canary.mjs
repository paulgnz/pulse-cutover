#!/usr/bin/env node
// replay-canary.mjs — exactly-once proof across a same-chain_id cutover.
//
// A transaction signed for the old chain is ALSO validly signed for the new one (same
// chain_id). Atomic means: anything the old chain executed before the cut must NOT execute
// again on the new chain, and anything signed but never executed must execute at most once.
//
//   prepare  sign N transfers (from → to, memo canary-<run>-<i>) with a long expiration, DON'T send
//   pre      send the first half to the OLD chain before the freeze (they execute pre-cut)
//   post     after LIVE: re-send ALL N to the NEW chain, twice
//              - pre-cut half  → must be rejected (already executed on the migrated state)
//              - held half     → must execute exactly once (second send rejected as duplicate)
//   check    compare the receiver's balance delta with the expected N (+0 double-applies)
//
// env: ENDPOINT (one /v1/chain URL), KEY (signer), FROM, TO, QTY (default "0.0001 XPR"),
//      TOKEN (default eosio.token), N (default 20), RUN, FILE (default canary-<RUN>.json),
//      EXPIRE (seconds, default 3000 — must outlive the freeze window)
import pkg from '@proton/js';
import { readFileSync, writeFileSync } from 'node:fs';
const { Api, JsonRpc, JsSignatureProvider } = pkg;
const env = (k, d) => process.env[k] ?? d;
const MODE = process.argv[2];
const RUN = env('RUN', 'r'), FILE = env('FILE', `canary-${RUN}.json`), N = +env('N', 20);
const FROM = env('FROM'), TO = env('TO'), QTY = env('QTY', '0.0001 XPR'), TOKEN = env('TOKEN', 'eosio.token');
const rpc = new JsonRpc(env('ENDPOINT'), { fetch });
const api = new Api({ rpc, signatureProvider: new JsSignatureProvider([env('KEY')]) });
const hexOf = (u8) => Buffer.from(u8).toString('hex');
const push = async (t) => {
  const body = { signatures: t.signatures, compression: 0, packed_context_free_data: '', packed_trx: t.packed_trx };
  const r = await fetch(`${env('ENDPOINT')}/v1/chain/push_transaction`, { method: 'POST', body: JSON.stringify(body) });
  const j = await r.json().catch(() => ({}));
  const err = j.error?.details?.[0]?.message || j.error?.what || j.error?.message || j.message;
  return r.ok && j.transaction_id ? { ok: true, id: j.transaction_id, status: j.processed?.receipt?.status } : { ok: false, http: r.status, err: String(err).slice(0, 160) };
};
const balance = async () => {
  const r = await rpc.get_currency_balance(TOKEN, TO, QTY.split(' ')[1]);
  return r[0] || `0 ${QTY.split(' ')[1]}`;
};

if (MODE === 'prepare') {
  const txs = [];
  for (let i = 0; i < N; i++) {
    const r = await api.transact({ actions: [{ account: TOKEN, name: 'transfer', authorization: [{ actor: FROM, permission: 'active' }],
      data: { from: FROM, to: TO, quantity: QTY, memo: `canary-${RUN}-${i}` } }] },
      { blocksBehind: 3, expireSeconds: +env('EXPIRE', 3000), broadcast: false, sign: true });
    txs.push({ i, memo: `canary-${RUN}-${i}`, signatures: r.signatures, packed_trx: hexOf(r.serializedTransaction), pre: i < N / 2 });
  }
  writeFileSync(FILE, JSON.stringify({ run: RUN, to: TO, qty: QTY, balance_before: await balance(), txs }, null, 2));
  console.log(`prepared ${N} signed transfers (${N / 2} to send pre-cut, ${N / 2} held) → ${FILE}; ${TO} balance ${await balance()}`);
} else if (MODE === 'pre') {
  const f = JSON.parse(readFileSync(FILE));
  for (const t of f.txs.filter((x) => x.pre)) { t.pre_result = await push(t); console.log('pre', t.memo, JSON.stringify(t.pre_result)); }
  await new Promise((r) => setTimeout(r, 3000));
  f.balance_after_pre = await balance();
  writeFileSync(FILE, JSON.stringify(f, null, 2)); console.log(`${TO} balance after pre-cut sends: ${f.balance_after_pre}`);
} else if (MODE === 'post') {
  const f = JSON.parse(readFileSync(FILE));
  f.balance_before_post = await balance();
  for (const pass of [1, 2]) for (const t of f.txs) {
    t[`post${pass}`] = await push(t);
    console.log(`post#${pass}`, t.pre ? 'PRE-CUT ' : 'held    ', t.memo, JSON.stringify(t[`post${pass}`]));
  }
  await new Promise((r) => setTimeout(r, 6000));
  f.balance_after_post = await balance();
  const unit = +QTY.split(' ')[0], amt = (s) => +String(s).split(' ')[0];
  const landedPost = Math.round((amt(f.balance_after_post) - amt(f.balance_before_post)) / unit);
  const held = f.txs.filter((t) => !t.pre).length;
  f.verdict = {
    pre_cut_replays_accepted: f.txs.filter((t) => t.pre && (t.post1.ok || t.post2.ok)).length,
    held_first_send_accepted: f.txs.filter((t) => !t.pre && t.post1.ok).length,
    held_second_send_accepted: f.txs.filter((t) => !t.pre && t.post2.ok).length,
    transfers_landed_post_cut: landedPost, expected_post_cut: held,
  };
  f.verdict.exactly_once = f.verdict.transfers_landed_post_cut === held;
  writeFileSync(FILE, JSON.stringify(f, null, 2));
  console.log(JSON.stringify(f.verdict));
  console.log(f.verdict.exactly_once
    ? `EXACTLY-ONCE: ${held} held transfers landed once; ${f.txs.length - held} pre-cut transfers did not re-execute`
    : `VIOLATION: ${landedPost} transfers landed post-cut, expected ${held} (a pre-cut transaction re-executed or a held one was lost/duplicated)`);
  process.exit(f.verdict.exactly_once ? 0 : 1);
}
