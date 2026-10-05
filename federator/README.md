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
| `/v2/health` | local health + `federation` block (`ok`, boundary status + identity checks, local ok + last_indexed, legacy ok) |
| `/v2/history/get_actions` | per-account merge across the cut, `sort=desc` (default) or `asc`; caller filters (`before`, `after`, `act.name`, …) kept on both sides; no-account = local feed |
| `/v2/history/get_transaction` | local only if every action is above the cut; legacy only if every action is at or below it (tagged `_premigration`) |
| `/v2/state/get_tokens` | contracts: union of legacy + local (discovery); amounts: chain `get_currency_balance` per contract; no balance row = omitted |
| `/v2/state/get_account` | permissions from chain `get_account`; tokens as above; actions = federated `get_actions` (limit 20); hyperion-rs shape |
| `/v2/state/get_key_accounts` | discovery legacy + local in every key spelling, kept only if the key is in the account's permissions **now** |
| other `/v2/state/*` | local first, legacy fallback, tagged `x-pulse-federation: index-only` (not chain-verified) |
| other `/v2/*` | local first, legacy fallback (`get_creator` etc. live pre-cut); legacy rows above the cut dropped |
| `/v1/history/get_actions` | from the federated timeline, v1 shape: `pos=-1` exact order; `pos>=0` mapped onto the combined timeline, flagged `federation.positional: "approximate"`. `account_action_seq` is **synthesized** from the position (`federation.account_action_seq: "synthesized"`) |
| `/v1/history/get_transaction` | as the v2 lookup, by the answer's `block_num` |
| `/v1/history/get_key_accounts` | as `/v2/state/get_key_accounts` |
| `/v1/history/get_controlled_accounts` | discovery legacy + local, kept only if the account's permissions name the controller now |

### The boundary rule (what a deposit poller can rely on)

- **Local rows strictly above the cut, legacy rows at or below it.** A legacy row above the cut is
  never returned: it is one of the source's discarded burn-off blocks, or a public archive that kept
  following a chain that did not migrate. A legacy row *at* the cut must carry the cut block id, or the
  answer is a 503 (`x-pulse-federation: legacy-identity-mismatch`): that archive is another chain.
- **Absent, not yet indexed, and unavailable are different answers.** Every lookup carries
  `x-pulse-federation-status`:
  `found` / `ok`; `absent` (both sources answered and the local indexer is at the chain head);
  `not_indexed_yet` (both answered, but the local indexer is behind the chain head: retry);
  `absent_index_state_unknown`; `partial` (some rows or the total may be missing; see `source_errors`);
  `unavailable` (HTTP 503: a source that could hold the answer did not answer). A local 503 plus a
  legacy 404 is a 503, never a definitive 404.
- **Totals.** `total.relation` is `"eq"` only when both sources answered with exact totals and no row
  was dropped at the cut; a lower bound from either source stays `"gte"`.
- **Pagination.** One timeline: legacy (pre-cut) then local (post-cut), reversed for `desc`. The page
  crosses the seam at `skip - <first source's total>`, which is only exact when that total is `"eq"`;
  otherwise the page stops at the seam and says `page_may_be_short: true` instead of placing rows at
  guessed offsets. If the first source of the requested order is unavailable the answer is a 503.
- **Time filters.** A caller's `before`/`after` are kept (ISO-8601 only; anything else is a 400).
  Legacy is additionally bounded by the cut time; a source whose half lies wholly outside the
  caller's window is not asked.
- **`account_action_seq` (v1)** is synthesized from the combined position: neither index stores the
  history_plugin's per-account sequence. It is stable only while no new action lands for the account;
  use `global_action_seq` (and the Hyperion `recv_sequence` inside `action_trace.receipt`) as cursors.
- **Global feed** (no `account`): the local post-cut index. If it is unavailable the legacy archive is
  asked up to the cut and the answer is marked `partial` + `legacy_only`.
- **State answers** (`get_tokens`, `get_account`, `get_key_accounts`, `get_controlled_accounts`) carry
  `verified_at_block`, `verified_at_block_id` and `chain_id`: the chain head when the request started.
  Each account is read at the head current when it was read, not at one pinned block. A chain error
  that is not a nodeos "unknown account / no such row" answer counts as unavailable (`partial`), never
  as absence. The chain-read cache is keyed by chain id + head block **id**, not head number.

## Boundary — how the router knows where "old" ends and "new" begins

`BOUNDARY_FILE` (default `/etc/pulse-cutover/boundary.json`) is written by the
cutover agent once the target is up at the cut (`{cut_block, cut_block_id,
cut_time, chain_id, target_chain_id?}`; producer mode writes it too when
`ceremony.boundary_path` is set) and re-read whenever it changes, no restart.

It is **validated, not trusted**, before any history is served:

1. `cut_block_id` must encode `cut_block` (Antelope ids carry the height in their first 4 bytes),
   `chain_id` must be a chain id, `cut_time` a time.
2. `CHAIN_URL` must serve `chain_id` (or the `target_chain_id` a rehearsal names), with head at or
   past the cut, and its block `cut_block` must be `cut_block_id` (`BOUNDARY_BLOCK_CHECK=off` skips
   this one check, only for a chain that cannot serve that block; then a same-chain-id source
   continuation could also pass).
3. The legacy archive must not report another chain id (Hyperion's `/v2/health` `NodeosRPC`).

The checks are repeated every `BOUNDARY_RECHECK_MS` (30 s). **Fail closed:** no file, a file that
disappeared after it was loaded, a corrupt or inconsistent file, a disagreeing upstream, or a boundary
that could never be checked = every history and state endpoint answers **503** with
`x-pulse-federation: boundary-<status>` (`/v2/health` still answers, with `federation.ok: false`).
There is no fallback to unlimited legacy history. `ALLOW_NO_BOUNDARY=1` restores the old pre-ceremony
legacy-only answer **only until the first boundary file is seen** (staging; before the flip the
public `/v2` points at the passthrough port anyway).

## Ports

- `PORT` (7010): the federating router — the ceremony's /v2 flip points here.
- `PASSTHROUGH_PORT` (7019): pure legacy proxy, standing in for "the /v2 you
  already had" as the pre-flip nginx upstream.

Run: `LOCAL=http://127.0.0.1:7000 LEGACY=https://test.proton.eosusa.io CHAIN_URL=http://127.0.0.1:8899 node server.js`

Tests: `node --test federator/test/*.test.mjs` (mock legacy, local and chain; no network).
