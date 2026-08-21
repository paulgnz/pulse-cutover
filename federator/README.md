# hyperion-federator

**What this is, in one paragraph:** when a chain migrates to PulseVM, the
history *before* the migration lives in the old Hyperion archive and the
history *after* it lives in a fresh indexer on the new chain. Users don't
care — they call one `/v2` URL and expect all of it. This small Node server
sits behind that URL and answers with both: old rows from the **legacy**
archive, new rows from the **local** indexer, merged into one seamless
timeline. "Your endpoint keeps its memory." It is staged automatically by
`install.sh --mode hyperion`; you only need this README if you're curious or
running it by hand. (Terms: [README glossary](../README.md#glossary).)

Why the merge is clean: PulseVM continues the source chain's block numbering,
so every post-cut block number > cut > every pre-cut block number — one
descending timeline paginates cleanly across the seam. (This is a server-side
port of the pulse-explorer federation, `lib/hyperion.ts`, which proved the
semantics client-side on the 1:1 chain.)

## Where does the OLD history come from? (one knob: `LEGACY=`)

- **You did not keep full history yourself** (most providers): point
  `LEGACY=` at the source chain's public archive, e.g.
  `LEGACY=https://test.proton.eosusa.io` — pre-cut queries are proxied there.
- **You kept your own full-history Elasticsearch**: point `LEGACY=` at your
  old local Hyperion, e.g. `LEGACY=http://127.0.0.1:<old-hyperion-port>` —
  same code path, pre-cut queries never leave the box.

## Federated surface

| endpoint | behavior |
|---|---|
| `/v2/health` | local health + `federation` block (boundary, local ok + last_indexed, legacy ok) |
| `/v2/history/get_actions` | per-account merge + cross-boundary pagination (desc); no-account = local feed |
| `/v2/history/get_transaction` | new-then-legacy; legacy hits tagged `_premigration` |
| `/v2/state/get_key_accounts` | union (keys are 1:1 across the cut) |
| other `/v2/*` | local first, legacy fallback (`get_creator` etc. live pre-cut) |
| `/v1/history/*` | local hyperion-rs shim only — v1 pos/offset pagination is NOT federated (per-source seq positions differ; /v2 is the federated surface) |

## Boundary — how the router knows where "old" ends and "new" begins

`BOUNDARY_FILE` (default `/etc/pulse-cutover/boundary.json`) is written by the
cutover agent the moment the cut is pinned (`{cut_block, cut_time,
cut_block_id, chain_id}`) and re-read whenever it changes — no restart. If
the file doesn't exist yet (pre-ceremony), the router simply serves
legacy-only: never a wrong answer, just no new rows yet.

## Ports

- `PORT` (7010): the federating router — the ceremony's /v2 flip points here.
- `PASSTHROUGH_PORT` (7019): pure legacy proxy, standing in for "the /v2 you
  already had" as the pre-flip nginx upstream.

Run: `LOCAL=http://127.0.0.1:7000 LEGACY=https://test.proton.eosusa.io node server.js`
