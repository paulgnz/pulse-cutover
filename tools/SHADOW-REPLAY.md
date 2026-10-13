# shadow-replay: real mainnet traffic against a shadow cutover

Rung 5 of the test ladder ([docs/HOW-IT-WORKS.md](../docs/HOW-IT-WORKS.md#7-how-it-gets-proven-before-mainnet)) runs the
full ceremony on a **private** network from real XPR mainnet state at H, with the real chain_id. `tools/shadow-replay.mjs`
then gives that shadow chain the real workload: every transaction mainnet executed in H+1 … H+n, forwarded **unchanged**
(same signatures, same packed bytes), in mainnet's order, at a controlled rate. It reports, per transaction, mainnet's
outcome next to the shadow chain's, and the differences by category.

It holds no keys and signs nothing. It only forwards transactions mainnet has already executed; if one ever reached
mainnet again, mainnet's own replay protection rejects it as a duplicate.

## Usage

```sh
# 1. Dry run (the default): read H+1 … H+N from a source node, classify, write a plan. The target is never contacted.
node tools/shadow-replay.mjs --source http://127.0.0.1:8888 --cut-height <H> --cut-id <block id of H> \
     --blocks 1200 --out plan.json

# 2. Replay into the shadow chain and confirm inclusion by reading its blocks after H.
node tools/shadow-replay.mjs --source http://127.0.0.1:8888 --cut-height <H> --cut-id <block id of H> \
     --blocks 1200 --target http://<shadow edge>:8888 --target-api v1 \
     --send --shadow-i-understand --rate 20 --inclusion-timeout 180 --out replay.json
```

| Flag | Meaning |
|---|---|
| `--source URL` / `--blocks-file f.json` | where mainnet's blocks after H come from: a Leap `/v1/chain/get_block` endpoint (read only), or a JSON array of `get_block` responses (`--source-chain-id` then names their chain) |
| `--cut-height H`, `--cut-id ID` | the cut. With `--source`, the source's block at H must have this id |
| `--blocks N` | how many blocks after H (default 100) |
| `--target URL`, `--target-api v1\|native` | the shadow chain: a `/v1/chain` edge (`send_transaction`) or PulseVM JSON-RPC (`pulsevm.issueTx`, URL = the chain's `/rpc`) |
| `--send` | actually forward; without it nothing is sent |
| `--shadow-i-understand` | required when the target reports the XPR mainnet chain_id |
| `--rate TPS` | send rate (default 5; 0 = as fast as answers come) |
| `--inclusion-timeout S` | how long to read the shadow chain's blocks for the forwarded ids (default 120) |
| `--deny-host a,b` | extra hostnames to refuse as targets |
| `--out f.json` | the full report |

Exit codes: 0 = dry run done, or every forwarded transaction matched; 1 = mismatches; 2 = refused or error.

## Target safety

Before sending anything the tool refuses a target that:

- is one of the built-in public XPR mainnet API hosts, or a `--deny-host`;
- reports a chain_id different from the source's (replayed signatures could not verify there);
- reports the XPR mainnet chain_id without `--shadow-i-understand` and `--cut-id`, or whose block at H is not the cut;
- has a block H+1 equal to mainnet's H+1. A real mainnet node has the same block at H as a shadow does; the block after
  H is what tells them apart, so this check catches any mainnet node, listed or not.

The report never contains signatures or packed transactions, only ids, statuses and outcomes.

## The report

`summary` holds the counts: `by_mainnet_status`, `replayable`, `not_replayable` (`deferred`: mainnet-generated
transactions whose receipt carries only an id), `predicted` (what the dry run expects to meet: `tapos_ref_after_cut`,
`context_free`, `delay_sec`, `mainnet_<status>`), `result` (`match` / `mismatch` / `skipped` / `not_sent`) and
`mismatch_categories`: `expired`, `tapos`, `duplicate`, `resource`, `assertion`, `authorization`, `deferred`, `other`,
`missing` (accepted but never included before the timeout), and `status_<mainnet>_vs_<shadow>`.

## What it cannot show

- **TAPOS after the cut.** A transaction whose reference block is after H refers to a mainnet block the shadow chain
  does not have. Once PulseVM enforces TAPOS (upstream #108) these are expected rejections (`tapos`); until then they
  pass. The dry run counts them as `tapos_ref_after_cut`.
- **Expiry.** Transactions expire (XPR allows up to an hour). Replaying long after mainnet included them yields
  `expired`; run the replay soon after the shadow cut, or treat `expired` as expected for old blocks.
- **Deferred and scheduled transactions** mainnet generated itself appear in blocks only as ids and cannot be forwarded.
  The shadow chain must produce its own from the imported state (compare those separately).
- **Context-free actions and data** are forwarded as received (`packed_context_free_data`); a `/v1` edge or node that
  does not accept them shows up as a mismatch, flagged `context_free` in the dry run.
- **Ordering and state drift.** Transactions run in mainnet's order, but the shadow chain's blocks are not mainnet's:
  timing-dependent contracts (oracles, auctions, anything reading the block time) can legitimately differ. Read
  `assertion` mismatches with that in mind.
- **Resource usage** (CPU/NET) is recorded from mainnet for reference, not compared: the billing models differ (#106).
