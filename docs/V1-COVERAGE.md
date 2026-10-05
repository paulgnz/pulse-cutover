# /v1 coverage after the cutover

What a dapp gets when it keeps calling the same `/v1` URL after an XPR (Leap 5) → PulseVM v1.0.0 cutover,
with `gateway.mode = "edge"` (the recommended mode for PulseVM v1.0.0+; `gateway/server.js`) and, for
hyperion-mode operators, the federator (`federator/server.js`).

**Rule:** values come from the new chain. History indexes (the legacy Hyperion archive for pre-cut, the local
hyperion-rs for post-cut) are used only for **discovery**, to find which accounts or token contracts to look at;
the answer is then read from the chain. No endpoint answers a state question from an index alone, and nothing
needs seeding: every answer is correct from the first post-cut block.

Legend for "served by":

- **native**: forwarded to the PulseVM node's own `/ext/bc/<BID>/v1/chain/<name>` (with `Host: localhost`).
  "(+normalization)" means the edge rewrites a request the node would reject but nodeos accepts, or repairs
  a response field clients cannot parse.
- **polyfill**: answered by the edge from other native calls or the node's `pulsevm.*` JSON-RPC.
- **static-at-cut**: served from a file captured on the source chain at the cut (`tools/capture-static.mjs`).
- **federator**: `/v1/history/*` is proxied to the federator.
- **501 upstream**: a known Leap endpoint PulseVM cannot serve yet; nodeos-shaped JSON error
  `{code:501, message:"… not available on PulseVM yet (upstream)", error:{name,what,details}}`.

[tools/conformance](../tools/conformance/README.md) checks this table differentially: the same requests go to a Leap 5 node and to the edge, and the answers are compared (documented 501s are expected, anything else must match).

Request bodies are validated like Leap 5's `parse_params` before anything reaches the node: an empty body or `{}` on an endpoint that needs parameters is `400 A Request body is required`, malformed JSON and wrong top-level types get fc's messages, and `get_info`/`get_producer_schedule`/`get_consensus_parameters` accept only an empty body or `{}`. Paths match exactly (a trailing slash is an unknown endpoint), and unknown endpoints get nodeos' `404 Unknown Endpoint`.

Responses that are not a plain pass-through carry an `x-pulse-edge` header saying what was done
(`polyfill`, `static-at-cut`, `wasm-unavailable`, `fresh-head-time`, `timestamp-repaired`, `partial: …`).

## /v1/chain (the 31 endpoints a Leap 5 node serves)

| endpoint | served by | notes |
|---|---|---|
| `get_info` | native | The node's real `head_block_time`, unchanged. On an idle chain (PulseVM builds blocks on demand) it can be minutes old. `FRESH_HEAD_TIME=1` is an opt-in **client workaround** for clients that compute expiration from it: a stale value is replaced by the current time (synthesized, saying nothing about block production), the real one kept in `pulsevm_head_block_time`, and the answer carries `x-pulse-synthesized: head_block_time` |
| `get_account` | native | |
| `get_block` | native (+normalization) | numeric `block_num_or_id` sent as a string (the node's parser only takes strings; eosjs sends numbers) |
| `get_block_info` | native (+normalization) | string `block_num` sent as a number; `timestamp` repaired: the node prints it in Rust debug format (`TimePoint { elapsed: … }`), which breaks eosjs TAPOS (`blocksBehind`) |
| `get_abi` | native | |
| `get_raw_abi` | native | |
| `get_table_rows` | native (+normalization) | `index_position` names (`secondary`, …) and numeric strings, string `limit`, numeric bounds, `"true"`/`"false"` flags |
| `get_table_by_scope` | native (+normalization) | same scalar normalization |
| `get_currency_balance` | native | |
| `get_currency_stats` | native | |
| `get_code_hash` | native | |
| `get_required_keys` | native (+normalization) | `EOS…` keys converted to `PUB_K1_…` (the node rejects `EOS…`); the transaction header eosjs 20-22 and @proton/js send (expiration with `.000` milliseconds, no `max_net_usage_words` / `max_cpu_usage_ms` / `delay_sec`) is normalized to what the native parser takes (the node answered 500 "Invalid JSON" and every eosjs transact() failed before signing); K1 keys in the answer are printed `EOS…` whatever the client sent, like Leap 5 |
| `push_transaction` | native | the node returns `{transaction_id}` on **admission**; confirm inclusion before treating a write as final |
| `send_transaction` | native | as `push_transaction` |
| `send_transaction2` | polyfill | Leap 5 envelope `{return_failure_trace, retry_trx, …, transaction}` unwrapped into native `send_transaction`. `retry_trx: true` is **refused** (500 `unsupported_feature`, header `x-pulse-edge: retry-unsupported`): the edge does not re-send or wait for inclusion, and accepting the flag would suggest it does. A transaction that fails while executing is a **202** with a failure trace (`return_failure_trace` defaults to true, as on Leap), synthesized from the node's error: `except` code/name/message/stack as the node reports them; `block_num` and `block_time` are `null` (the node names no block for an error, and none is invented); `elapsed`, `net_usage` and the stack entries' `data` are not available (header `x-pulse-edge: failure-trace synthesized …`). A success is the node's ADMISSION answer, not an execution receipt: reconcile the id through history. Errors raised before execution (parse, expiry, duplicate) stay a 500. `return_failure_trace: false` gives the node's 500. Success: the node's answer |
| `push_transactions` | polyfill | sequential native `push_transaction`, one result per transaction in order; answered **202** like nodeos; a failure is `{transaction_id: "000…0", processed: {error}}` with nodeos' detail string (`<code> <name>: <what>` + each detail and its source line; the per-entry argument line nodeos prints is not in the node's error JSON). `[]` is the same 500 `St12out_of_range` nodeos 5.0.3 gives; max 1000. If the client disconnects mid-batch the edge stops: the remaining transactions are NOT submitted (logged), so a client that retries its operation cannot have the rest of the old batch executing behind it |
| `get_raw_block` | polyfill | `pulsevm.getRawBlock`; an unknown block is a 400 `unknown_block_exception` |
| `get_block_header` | polyfill | `{id, signed_block_header}` from native `get_block` (timestamp, producer, previous, roots) + native `get_block_info` (fields `get_block` omits); `block_extensions` not available. Byte-identical to Leap when pointed at a Leap node. On PulseVM `producer_signature` and `schedule_version` are whatever the node's `get_block_info` reports (currently placeholders) |
| `get_block_header_state` | polyfill (partial) | `block_num`, `id`, `header` (what eosjs TAPOS reads), `dpos_irreversible_blocknum`; other header-state fields (schedules, merkle, signing authority) are not exposed by PulseVM. Like Leap, only **reversible** blocks (above LIB) are served; at or below LIB it is Leap's 400 `Could not find reversible block`. On PulseVM LIB equals head, so that is every block: eosjs and @proton/js only ask for blocks above LIB (otherwise they use `get_block_info`) and fall back to `get_block_info` on error |
| `get_producers` | polyfill | `eosio` `producers` table in eosio.system's `by_votes` order (active by votes desc, then inactive by votes asc; ties by owner) + `global.total_producer_vote_weight`; honours `json`, `limit` (default 50), `lower_bound` (a producer name), `more` = next owner. PulseVM's table reader has no float64 secondary index, so the whole table is read and sorted. Checked against a live XPR mainnet Leap node with the edge pointed at it: identical rows, `more` and vote weight for `json` true/false, limits 3–100 and `lower_bound` |
| `get_producer_schedule` | polyfill (partial) | `pulsevm.getProducers`: `active.version` + producer names only; signing keys and the pending/proposed schedules are not exposed (and never invented). Header `x-pulse-edge: partial: active names only` |
| `get_raw_code_and_abi` | polyfill (partial) | `abi` from `get_raw_abi`; `wasm: ""` because code bytes cannot be read; header `x-pulse-edge: wasm-unavailable` |
| `get_activated_protocol_features` | static-at-cut | captured list, filtered like Leap 5.0 (`lower_bound`, `upper_bound`, `search_by_block_num`, `reverse`); like nodeos 5.0.0/5.0.3, `limit` is ignored and there is never a `more`. Features activated on PulseVM after the cut are not reflected. 501 if no capture |
| `get_consensus_parameters` | static-at-cut | captured `{chain_config, wasm_config}`. Parameter changes after the cut are not reflected. 501 if no capture |
| `get_scheduled_transactions` | 501 upstream, or polyfill | PulseVM v1.0.0 **does** keep deferred transactions (migrated from the snapshot and executed or retired per block) but has no way to list them, so this is a 501. It answers `{transactions:[], more:""}` only when the at-cut feature list shows `DISABLE_DEFERRED_TRXS_STAGE_1` active (then no deferred transaction can exist after the next block). XPR mainnet has **not** activated it |
| `get_accounts_by_authorizers` | polyfill | `keys`: discovery = the federator's `get_key_accounts` per key; `accounts` (name = any permission, or `{actor, permission}`): discovery = the federator's `get_controlled_accounts`. Truth = the chain's `get_account` for every candidate: a row `{account_name, permission_name, authorizing_key \| authorizing_account, weight, threshold}` is emitted only where the key or account is in that permission **now**. Federator unreachable: 502; a discovery the federator marks `partial`, or a candidate whose `get_account` failed with anything but "unknown account": 503 (`discovery_incomplete` / `verification_unavailable`), never an answer that looks complete. Keys are printed `EOS…`; rows come account-authorized first, then by key, in permission-creation order as far as `get_account` shows it (account creation time, then name). Malformed keys and names get nodeos' parser errors. Completeness depends on the indexes having seen every permission change (see limits) |
| `get_transaction_id` | polyfill | sha256 of the packed transaction. Sound without an ABI for a transaction whose action data is hex (what eosjs and wharfkit send); JSON action data needs the contract ABI and is a 501. Like nodeos 5.0, `{transaction: …}` wrappers and `{packed_trx}` are refused with `Transaction actions are missing or invalid` |
| `get_code` | 501 upstream | the node exposes no way to read contract code bytes; `get_code_hash`, `get_abi`, `get_raw_abi` work |
| `compute_transaction` | 501 upstream | no dry-run execution endpoint |
| `send_read_only_transaction` | 501 upstream | no read-only execution endpoint |
| `push_block` | 404 | answered like a non-producer node |

Anything else under `/v1/chain/` (including `get_transaction_status`, `abi_json_to_bin`, `abi_bin_to_json` and
`get_finalizer_info`, which Leap 5 does not serve either) is a nodeos-style 404. `/v1/node/get_supported_apis`
lists what the edge serves.

## /v1/history (Hyperion's v1 shim; nodeos itself does not serve these)

Proxied by the edge to the federator (`FEDERATOR_URL`, default `http://127.0.0.1:7010`). The federator serves
nothing without a VALID boundary file (written by the ceremony, checked against the chain and the legacy archive):
missing, stale or mismatched = 503, never unbounded legacy history. Rows come from the local index only above the
cut and from the legacy archive only at or below it. Every answer carries `x-pulse-federation-status`
(`found`/`ok`, `absent`, `not_indexed_yet`, `partial`, `unavailable`): see `federator/README.md` for what a deposit
poller can rely on.

| endpoint | served by | notes |
|---|---|---|
| `get_actions` | federator | built on the federated `/v2` timeline. `pos = -1` (latest N): exact order across the cut. `pos ≥ 0`: positions are mapped onto the combined timeline and the response carries `federation.positional: "approximate"`. `account_action_seq` is **synthesized**: the position in that combined timeline, not a nodeos history-plugin counter (`federation.account_action_seq: "synthesized"`; see below) |
| `get_transaction` | federator | local only if its `block_num` is above the cut, legacy only at or below it; a source outage is a 503, not a 404 |
| `get_key_accounts` | federator | discovery legacy + local in every key spelling (`EOS…`, `PUB_K1_…`), then only accounts whose **current** chain permissions contain the key |
| `get_controlled_accounts` | federator | discovery legacy + local, then only accounts whose current permissions name the controlling account |

Why `pos ≥ 0` is approximate: PulseVM v1.0.0 continues `global_sequence` and each account's `recv_sequence` across
the import (the migration sidecar carries both), but `account_action_seq` in the v1 history API is an index
counter (history plugin, Hyperion), not chain state. The legacy archive and the local indexer number actions
independently, so an absolute position only means something on the combined timeline the federator builds.

## /v2 state (federator)

| endpoint | behaviour |
|---|---|
| `/v2/state/get_tokens` | contracts = union of legacy + local `get_tokens` (discovery); amounts = chain `get_currency_balance {code, account}` per contract; contracts with no balance row are omitted; Hyperion shape `{account, tokens:[{symbol, precision, amount, contract}]}` |
| `/v2/state/get_account` | hyperion-rs shape; `permissions` from chain `get_account`, `tokens` as above, `actions` from the federated `get_actions` (limit 20) |
| `/v2/state/get_key_accounts` | as `/v1/history/get_key_accounts` |
| other `/v2/state/*` (`get_links`, `get_proposals`, `get_voters`, …) | still answered from an index (local first, then legacy) and tagged `x-pulse-federation: index-only`. Values there may be stale for anything untouched since the cut |

A partially failed discovery (one index unreachable) is answered with `partial: true` and `source_errors`. A
chain failure is a 502: amounts and permissions are never taken from an index.

## Limits

- **Discovery completeness.** The chain-truth rule makes every returned value correct, but an account or token
  contract can only be returned if one of the indexes has seen it: the legacy archive must hold the complete
  pre-cut permission/token index, and hyperion-rs must index post-cut permission and token deltas.
- **Static-at-cut values** (`get_activated_protocol_features`, `get_consensus_parameters`) do not follow changes
  made on PulseVM after the cut. Every such answer carries `x-pulse-edge: static-at-cut` and
  `x-pulse-static-captured` (chain, head and time of the capture, from `capture.json`). `get_consensus_parameters`
  is also flagged `x-pulse-static-stale: …` when its `chain_config` no longer matches the chain's `eosio/global`
  row (`setparams` writes both); protocol activations after the cut cannot be detected and are marked
  `x-pulse-static-freshness`. Re-run the capture just before the source nodeos stops.
- **Head time on an idle chain** is the real (old) time by default (see `get_info`). Clients that derive
  expiration from it, or from an old block (eosjs `blocksBehind`), can fail the first transaction on an idle
  chain: use `expireSeconds ≥ 120`, or enable the `FRESH_HEAD_TIME=1` workaround knowingly.
- **Partial discovery** (one index unreachable, a chain read failed) makes `get_accounts_by_authorizers` a 503
  (`discovery_incomplete` / `verification_unavailable`), never a complete-looking list.

## Upstream asks (PulseVM)

1. **Contract code bytes**: a way to read an account's wasm, for `get_code` and `get_raw_code_and_abi`.
2. **`get_accounts_by_authorizers` natively** (keys and accounts mode), so discovery does not depend on history indexes.
3. **`compute_transaction` / `send_read_only_transaction`**: dry-run and read-only execution endpoints.
4. **`get_transaction_id` natively**, so JSON action data can be hashed with the chain's ABIs.
5. **Producers and schedule on `/v1`**: `get_producers` (with the votes index), `get_producer_schedule` with signing
   authorities and pending/proposed schedules, and `get_block_header_state`.
6. **Deferred transactions**: a `get_scheduled_transactions` listing of the generated-transaction table PulseVM already keeps.
7. **Protocol features and consensus parameters** served by the node (`get_activated_protocol_features`,
   `get_consensus_parameters`), so they follow post-cut changes.
8. **Failure traces and error details**: `send_transaction2` failure traces (`elapsed`, `net_usage`, stack `data`) and the full fc detail string (with each entry's arguments) in error JSON, so the edge does not have to rebuild them from the error body.
9. **`get_block_info` timestamp**: print the block time as `YYYY-MM-DDTHH:MM:SS.mmm` (it currently uses Rust's debug format).
10. **Request parsing like nodeos**: accept numeric `block_num_or_id`, `index_position` names, numeric strings, and
   `EOS…` key spellings in `get_required_keys`, so the edge's normalization can go away.
11. **Action-sequence continuity for history**: expose per-account action sequence numbers in a form history APIs
    can continue across the import, so v1 positional paging can be exact.
