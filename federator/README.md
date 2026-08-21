# hyperion-federator

The /v2 history boundary router for a PulseVM cutover: after the cut, the
SAME public /v2 URL answers with pre-cut history from the **legacy** Hyperion
and post-cut history from the **local** hyperion-rs indexing the PulseVM
chain. "Your endpoint keeps its memory."

Server-side port of the pulse-explorer federation (`lib/hyperion.ts`), which
proved the merge semantics client-side on the 1:1 chain: PulseVM continues the
source chain's block numbering, so every post-cut block number > cut > every
pre-cut block number — one descending timeline paginates cleanly across the
seam.

## The two legacy patterns (one knob)

- **No local full history** (most providers): `LEGACY=https://test.proton.eosusa.io`
  — pre-cut queries go to the source chain's public archive.
- **Local old ES** (providers who kept full history): `LEGACY=http://127.0.0.1:<old-hyperion>`
  — same code path, pre-cut queries stay on the box.

## Federated surface

| endpoint | behavior |
|---|---|
| `/v2/health` | local health + `federation` block (boundary, local ok + last_indexed, legacy ok) |
| `/v2/history/get_actions` | per-account merge + cross-boundary pagination (desc); no-account = local feed |
| `/v2/history/get_transaction` | new-then-legacy; legacy hits tagged `_premigration` |
| `/v2/state/get_key_accounts` | union (keys are 1:1 across the cut) |
| other `/v2/*` | local first, legacy fallback (`get_creator` etc. live pre-cut) |
| `/v1/history/*` | local hyperion-rs shim only — v1 pos/offset pagination is NOT federated (per-source seq positions differ; /v2 is the federated surface) |

## Boundary

`BOUNDARY_FILE` (default `/etc/pulse-cutover/boundary.json`) is written by the
cutover agent when the cut is pinned (`{cut_block, cut_time, cut_block_id,
chain_id}`) and re-read on change. Absent file = legacy-only degradation
(pre-ceremony state; never a wrong answer).

## Ports

- `PORT` (7010): the federating router — the ceremony's /v2 flip points here.
- `PASSTHROUGH_PORT` (7019): pure legacy proxy, standing in for "the /v2 you
  already had" as the pre-flip nginx upstream.

Run: `LOCAL=http://127.0.0.1:7000 LEGACY=https://test.proton.eosusa.io node server.js`
