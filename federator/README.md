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

## State comes from the chain

An index only knows what it has seen. The local hyperion-rs is filled from
post-cut deltas, so an account untouched since the cut has no rows there (its
token list would come back empty, with HTTP 200); the legacy archive is frozen
at the cut. So for **state**, the indexes are used only to *discover* which
contracts or accounts to look at, and the values are read from the chain's
`/v1/chain` (`CHAIN_URL`, default `http://127.0.0.1:8899`, the /v1 edge; the
node's native base `http://127.0.0.1:9650/ext/bc/<BID>` works too). A chain
failure is a 502, never an index answer. Full per-endpoint table:
[docs/V1-COVERAGE.md](../docs/V1-COVERAGE.md).

## Federated surface

| endpoint | behavior |
|---|---|
| `/v2/health` | local health + `federation` block (boundary, local ok + last_indexed, legacy ok) |
| `/v2/history/get_actions` | per-account merge + cross-boundary pagination (desc); no-account = local feed |
| `/v2/history/get_transaction` | new-then-legacy; legacy hits tagged `_premigration` |
| `/v2/state/get_tokens` | contracts: union of legacy + local (discovery); amounts: chain `get_currency_balance` per contract; no balance row = omitted |
| `/v2/state/get_account` | permissions from chain `get_account`; tokens as above; actions = federated `get_actions` (limit 20); hyperion-rs shape |
| `/v2/state/get_key_accounts` | discovery legacy + local in every key spelling, kept only if the key is in the account's permissions **now** |
| other `/v2/state/*` | local first, legacy fallback, tagged `x-pulse-federation: index-only` (not chain-verified) |
| other `/v2/*` | local first, legacy fallback (`get_creator` etc. live pre-cut) |
| `/v1/history/get_actions` | from the federated timeline, v1 shape: `pos=-1` exact order; `pos>=0` mapped onto the combined timeline, flagged `federation.positional: "approximate"` (sequence numbers are per-index, see V1-COVERAGE) |
| `/v1/history/get_transaction` | new-then-legacy |
| `/v1/history/get_key_accounts` | as `/v2/state/get_key_accounts` |
| `/v1/history/get_controlled_accounts` | discovery legacy + local, kept only if the account's permissions name the controller now |

Before the boundary file exists every state and `/v1/history` endpoint is
served legacy-only (the pre-ceremony behaviour). A partially failed discovery
(one index unreachable) is answered with `partial: true` + `source_errors`.

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

Run: `LOCAL=http://127.0.0.1:7000 LEGACY=https://test.proton.eosusa.io CHAIN_URL=http://127.0.0.1:8899 node server.js`

Tests: `node --test federator/test/*.test.mjs` (mock legacy, local and chain; no network).
