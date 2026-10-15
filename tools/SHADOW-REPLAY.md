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
| `--allow-inconclusive` | exit 0 instead of 3 when nothing mismatched but some outcomes are unknown or not sent |
| `--out f.json` | the full report |

Exit codes: 0 = dry run done, or every forwarded transaction matched; 1 = mismatches; 2 = refused or error.

## Target safety

Before sending anything the tool must **prove** the target is a shadow of this cut. A missing or failed answer is
never taken as proof: it refuses. It refuses a target that:

- is one of the built-in public XPR mainnet API hosts, or a `--deny-host`;
- does not report a chain_id, or reports one different from the source's (with `--blocks-file`, pass
  `--source-chain-id`; an unknown source chain_id refuses);
- reports the XPR mainnet chain_id without `--shadow-i-understand`;
- cannot show its block at H, or whose block at H is not `--cut-id` (required for every send);
- cannot show its own block H+1: a shadow must have produced its first block before replay starts (its head must be
  past H and the read must succeed);
- has a block H+1 equal to mainnet's H+1. A real mainnet node has the same block at H as a shadow; the block after H
  is what tells them apart, so this check catches any mainnet node, listed or not. Mainnet's H+1 must therefore be
  known: from the source, or present in the blocks file; without it the tool refuses.

**Mainnet's H+1 is authenticated independently of the source.** The source (or blocks file) could be anything, even
the shadow itself: swapping `--source` and `--target` used to make a real mainnet target pass. So the reference H+1
is read from **at least two reference endpoints on distinct hostnames that agree** (one vote per hostname: two ports
on one host are one voice; the source's and the target's hostnames never vote), and every reference must also show
`--cut-id` at H:

- for the XPR mainnet chain_id, **only the built-in pinned list of public mainnet APIs vouches** (the same hosts as
  the deny list), and only by **exact URL** (round 6): an answer counts only when its endpoint, normalized (scheme and
  host lowercased, default port and trailing slash ignored; userinfo, query or fragment never match), is one of the
  built-in `https://` URLs. A `--mainnet-endpoint` on a pinned hostname with another scheme, port or path is an added
  endpoint like any other: compared, never vouching. Since round 7 the right to vouch is decided by **position**, not
  by string comparison: only an entry of the built-in pinned list itself vouches, so an added URL that normalizes to a
  pinned one (`https://<pinned-host>//`) still never counts. Every request URL is built by one function (`joinUrl`)
  from the same normalized base, so the URL requested is exactly the endpoint's. `--mainnet-endpoint URL` (repeatable, or comma-separated) adds endpoints that are asked and
  compared, but they **never count toward the two**: an added endpoint can only make the check refuse (any
  disagreement refuses), never make it pass. With every pinned endpoint unreachable the tool refuses, whatever the
  added endpoints say. A shadow replay therefore needs outbound HTTPS to at least two pinned hosts;
- for any other chain_id, `--reference-endpoint URL`: at least two, on distinct hostnames.

The tool refuses when fewer than two vouching endpoints agree, when any two valid answers disagree (never a majority
vote), when the cut is not the reference chain's H, when the source's H+1 is not the reference H+1 (the source is not
the chain it claims), and when the target's H+1 equals it.

**No redirects.** No request follows an HTTP redirect. A 3xx on a read is an error (lineage unproven). A 3xx on a
send stops the whole run (exit 2, `aborted` in the report): the signed body is never forwarded anywhere else.

No environment variable changes any of this. Tests call the tool in-process (`run(argv, opts)`) and inject stand-in
references through `opts`; the command line has no way to do that.

The report never contains signatures or packed transactions, only ids, statuses and outcomes.

## The report

`summary` holds the counts: `by_mainnet_status`, `replayable`, `not_replayable` (`deferred`: mainnet-generated
transactions whose receipt carries only an id), `predicted` (what the dry run expects to meet: `tapos_ref_after_cut`,
`context_free`, `delay_sec`, `mainnet_<status>`), `result` (`match` / `mismatch` / `skipped` / `not_sent`) and
`mismatch_categories`: `expired`, `tapos`, `duplicate`, `resource`, `assertion`, `authorization`, `deferred`, `other`,
`missing` (accepted but never included before the timeout), `status_<mainnet>_vs_<shadow>`, and
`mainnet_<status>_vs_rejected_<reason>`.

How outcomes are matched, by mainnet's receipt status:

| Mainnet | Means | Same outcome on the shadow |
|---|---|---|
| `executed` | ran, state changed | included as `executed` |
| `delayed` | accepted and queued to run later | included as `delayed` (a rejection is a mismatch) |
| `soft_fail` | failed, `onerror` handled it | included as `soft_fail` |
| `hard_fail` | included, failed, no effects | included as `hard_fail`, or rejected for an execution failure (`assertion`); an admission rejection (authorization, TAPOS, expiry, duplicate, resources) is a mismatch |
| `expired` | included only to retire it | included as `expired`, or rejected as `expired` |

Only `hard_fail` and `expired` mean "neither executed nor queued", so only they can match a rejection.

Only Leap's five receipt statuses are outcomes. Any other value on either side (a number outside the enum, an empty
or unfamiliar string) is `unknown`: it is never a match, even when both sides report the same unsupported value.

**Exit codes (send mode):** 0 when every replayed transaction matched; 1 when any mismatched; 3 when nothing
mismatched but some outcomes are `unknown` or `not_sent` (inconclusive: not a pass), unless `--allow-inconclusive`;
2 when the tool refused or failed. `verdict` in the report is `match`, `mismatch` or `inconclusive`.

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
