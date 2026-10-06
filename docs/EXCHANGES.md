# Exchanges, wallets and indexers: what changes at the cut

Short operational guidance for anyone who credits deposits, sends withdrawals or indexes the chain across a
cutover. The process itself is in [PROCESS.md](PROCESS.md); what is and is not proven is in
[ATOMICITY.md](../ATOMICITY.md).

## 1. The head block number goes BACKWARDS once, at the flip

The cut is block **H**. Everything up to and including H is carried over. To let H become irreversible, the
source producers keep making **empty** blocks after H (with writes frozen) until they pause: in the rehearsals
that was about **330 blocks** (H+1 … about H+330). Those "burn-off" blocks are **discarded**: they are not part
of the new chain.

The new chain continues from H, so its first blocks are again H+1, H+2, … with **different block ids** and
timestamps than the discarded burn-off blocks at the same heights.

What you will see if you follow the head through the cut:

```
old chain:  … H-1  H  H+1' H+2' … H+330'   (paused; H+1'…H+330' are discarded, empty)
new chain:            H+1  H+2  …           (same heights, different ids, real transactions)
```

So `get_info.head_block_num` drops by roughly 330 at the moment your endpoint starts serving the new chain.

**Rules for indexers and deposit pollers:**

- **H is the end of the old chain.** Never use a block above H from the old chain (your own node's, or a
  public archive that kept following it). The federating history router enforces this and refuses to serve
  legacy rows above H.
- **Key everything by block id, not only by height.** A height above H names two different blocks across
  the cut. A poller that stores "last processed height" must also store the block id at that height and,
  on a mismatch, rewind to H.
- **Expect the regression once.** Do not alarm on it, do not treat it as a reorg of real transactions
  (the discarded blocks are empty: the agent aborts the cut if any of them carries a transaction), and do not
  "wait until the head catches up": the new chain's H+N is the block you want.
- Burn-off blocks have no transactions, so no deposit can be lost in them; but an indexer that recorded them
  must drop them.

## 2. Crediting deposits

- Credit only **irreversible** inclusion on the chain your endpoint serves after the cut, identified by
  transaction id **and** block id.
- Through the federated `/v1/history` and `/v2` endpoints, read `x-pulse-federation-status`:
  `found` (credit-eligible once irreversible), `absent` (both sources answered "not there"),
  `not_indexed_yet` (the post-cut index is behind the chain: ask again), `partial` (some rows or the total
  may be missing), `unavailable` (HTTP 503: a source that could hold the answer did not answer). **A 503 or
  `not_indexed_yet` is never "the deposit does not exist".**
- `total.relation: "gte"` is a lower bound. Positional `/v1/history/get_actions` sequence numbers are
  synthesized across the cut: page by `global_action_seq`, not by `account_action_seq`.

## 3. Withdrawals and retries: reconcile before you re-sign

A timeout, a dropped connection or a 5xx after you sent a transaction is an **ambiguous** outcome: it may have
executed. Re-signing creates a **new transaction id**, and transaction-id deduplication cannot stop a second
transfer.

1. Keep an **application-level operation id** for every withdrawal (your own id, also put in the memo or
   contract action where you can), and refuse to execute one operation twice on your side.
2. After an ambiguous outcome, **look up the original transaction id** (history `get_transaction`, or your own
   node) until it is either found (done: do not resend) or provably dead (its expiration has passed by the
   chain's head block time and it is not found on a fully indexed, available source).
3. Only then build and sign a replacement, under the same operation id.
4. An HTTP 503 from the edge **during the write freeze** means the write was not accepted: safe to retry
   later, still under the same operation id.

Do not rely on re-sending the identical signed bytes across the cut: whether the new chain rejects them as
duplicates, or accepts a transaction signed before the cut, depends on the chain-id / TAPOS / dedupe policy
of the target, which is not final yet (ATOMICITY Known limits).

`send_transaction2` with `retry_trx: true` is refused by the edge (it does not track inclusion); a successful
send is an admission, not an execution receipt.

## 4. Every write path is frozen by the operator

The write freeze is the producers' `on_freeze` hook. It must close **every** path that can put a transaction
into a producer's nodeos: public and private API endpoints, relays and p2p entry points the operator controls,
and direct clients (bots, keepers, oracle feeders). A path left open lets transactions land in the burn-off
blocks; the agent then aborts the cut (it audits every block between H and the pause) instead of losing them,
but the cutover does not happen. If you run your own nodes peered to a producer, expect your writes to be
refused for the freeze window.

## 5. Block producers after the cut: no 12-block rotation

On the old chain, the active schedule's 21 producers take turns in alphabetical order, 12 blocks each, every
0.5 s. PulseVM does not work that way:

- **Who builds a block** is decided by Snowman consensus and ProposerVM: validators get stake-weighted proposal
  windows, and a node builds a block **on demand**, when it has transactions. A quiet chain makes no blocks; a busy
  one makes them as fast as they are proposed and accepted (in the 5-validator rehearsal on 4-vCPU hosts: about
  1.35 s per accepted block, gaps quantized to ~5 s windows, and up to tens of seconds under light traffic).
- **A block's `producer` field** is the producer name configured on the node that built it, stamped into the
  block, and the node refuses to build unless its key is that name's key in the active schedule (PulseVM v1.0.0,
  `crates/pulsevm_core/src/chain/controller.rs`, lines 2210–2236). It is not a schedule slot. In rehearsals every
  validator ran the same producer name and key (MetalBlockchain/pulsevm#107), so every block says `protonnz`
  whoever built it.
- **What behaves differently:** missed-block or "round" trackers that expect each scheduled producer to produce
  12 consecutive blocks per round; producer pay or monitoring based on `unpaid_blocks` and block counts per
  producer; dashboards that show "next producer" or a rotation order; anything that infers liveness of one
  producer from its slots. Head block time is the time of the last block, not wall clock minus 0.5 s: on a quiet
  chain it can be minutes old while the chain is healthy (use the head **and** a recent transaction of your own to
  judge liveness, see section 2).

Nothing here changes how you sign, verify or credit transactions; it changes what you can infer from block
headers.
