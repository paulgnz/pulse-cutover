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

Responses that are not a plain pass-through carry an `x-pulse-edge` header saying what was done
(`polyfill`, `static-at-cut`, `wasm-unavailable`, `fresh-head-time`, `timestamp-repaired`, `partial: …`).

## /v1/chain (the 31 endpoints a Leap 5 node serves)

| endpoint | served by | notes |
|---|---|---|
| `get_info` | native | On an idle chain (PulseVM builds blocks on demand) a stale `head_block_time` is replaced by the current time, the true value kept in `pulsevm_head_block_time` (same behaviour as the legacy gateway; `FRESH_HEAD_TIME=0` turns it off) |
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
| `get_required_keys` | native (+normalization) | `EOS…` keys converted to `PUB_K1_…` (the node rejects `EOS…`); the answer uses the client's own spelling |
| `push_transaction` | native | the node returns `{transaction_id}` on **admission**; confirm inclusion before treating a write as final |
| `send_transaction` | native | as `push_transaction` |
| `send_transaction2` | polyfill | Leap 5 envelope `{return_failure_trace, retry_trx, …, transaction}` unwrapped into native `send_transaction`; the response is the node's |
| `push_transactions` | polyfill | sequential native `push_transaction`, one result per transaction in order; a failure is `{transaction_id: "000…0", processed: {error}}` like nodeos; max 1000 |
| `get_raw_block` | polyfill | `pulsevm.getRawBlock`; an unknown block is a 400 `unknown_block_exception` |
| `get_block_header` | polyfill | `{id, signed_block_header}` from native `get_block` (timestamp, producer, previous, roots) + native `get_block_info` (fields `get_block` omits); `block_extensions` not available. Byte-identical to Leap when pointed at a Leap node. On PulseVM `producer_signature` and `schedule_version` are whatever the node's `get_block_info` reports (currently placeholders) |
| `get_block_header_state` | polyfill (partial) | `block_num`, `id`, `header` (what eosjs TAPOS reads), `dpos_irreversible_blocknum`; other header-state fields (schedules, merkle, signing authority) are not exposed by PulseVM. eosjs never asks for it in practice: PulseVM's LIB equals head, so eosjs uses `get_block_info` |
| `get_producers` | polyfill | `eosio` `producers` table in eosio.system's `by_votes` order (active by votes desc, then inactive by votes asc; ties by owner) + `global.total_producer_vote_weight`; honours `json`, `limit` (default 50), `lower_bound` (a producer name), `more` = next owner. PulseVM's table reader has no float64 secondary index, so the whole table is read and sorted. Checked against a live XPR mainnet Leap node with the edge pointed at it: identical rows, `more` and vote weight for `json` true/false, limits 3–100 and `lower_bound` |
| `get_producer_schedule` | polyfill (partial) | `pulsevm.getProducers`: `active.version` + producer names only; signing keys and the pending/proposed schedules are not exposed (and never invented). Header `x-pulse-edge: partial: active names only` |
| `get_raw_code_and_abi` | polyfill (partial) | `abi` from `get_raw_abi`; `wasm: ""` because code bytes cannot be read; header `x-pulse-edge: wasm-unavailable` |
| `get_activated_protocol_features` | static-at-cut | captured list, paged like Leap (`lower_bound`, `upper_bound`, `limit`, `search_by_block_num`, `reverse`, `more`). Features activated on PulseVM after the cut are not reflected. 501 if no capture |
| `get_consensus_parameters` | static-at-cut | captured `{chain_config, wasm_config}`. Parameter changes after the cut are not reflected. 501 if no capture |
| `get_scheduled_transactions` | 501 upstream, or polyfill | PulseVM v1.0.0 **does** keep deferred transactions (migrated from the snapshot and executed or retired per block) but has no way to list them, so this is a 501. It answers `{transactions:[], more:""}` only when the at-cut feature list shows `DISABLE_DEFERRED_TRXS_STAGE_1` active (then no deferred transaction can exist after the next block). XPR mainnet has **not** activated it |
| `get_accounts_by_authorizers` | polyfill | `keys`: discovery = the federator's `get_key_accounts` per key; `accounts` (name = any permission, or `{actor, permission}`): discovery = the federator's `get_controlled_accounts`. Truth = the chain's `get_account` for every candidate: a row `{account_name, permission_name, authorizing_key \| authorizing_account, weight, threshold}` is emitted only where the key or account is in that permission **now**. Federator unreachable: 502, never an empty answer. Completeness depends on the indexes having seen every permission change (see limits) |
| `get_transaction_id` | polyfill | sha256 of the packed transaction. Sound without an ABI: accepts `packed_trx` (+`compression`) or a transaction whose action data is hex (what eosjs and wharfkit send); JSON action data needs the contract ABI and is a 501 |
| `get_code` | 501 upstream | the node exposes no way to read contract code bytes; `get_code_hash`, `get_abi`, `get_raw_abi` work |
| `compute_transaction` | 501 upstream | no dry-run execution endpoint |
| `send_read_only_transaction` | 501 upstream | no read-only execution endpoint |
| `push_block` | 404 | answered like a non-producer node |

Anything else under `/v1/chain/` (including `get_transaction_status`, `abi_json_to_bin`, `abi_bin_to_json` and
`get_finalizer_info`, which Leap 5 does not serve either) is a nodeos-style 404. `/v1/node/get_supported_apis`
lists what the edge serves.

## /v1/history (Hyperion's v1 shim; nodeos itself does not serve these)

Proxied by the edge to the federator (`FEDERATOR_URL`, default `http://127.0.0.1:7010`). Before the ceremony
writes the boundary file the federator answers these legacy-only, exactly as before.

| endpoint | served by | notes |
|---|---|---|
| `get_actions` | federator | built on the federated `/v2` timeline. `pos = -1` (latest N): exact order across the cut. `pos ≥ 0`: positions are mapped onto the combined timeline and the response carries `federation.positional: "approximate"`. `account_action_seq` is the position in that combined timeline, not a nodeos history-plugin counter (see below) |
| `get_transaction` | federator | local (post-cut) first, then legacy (pre-cut) |
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
  made on PulseVM after the cut. Re-run the capture just before the source nodeos stops.
- **Head time on an idle chain** is reported as "now" (see `get_info`); clients that derive expiration from an old
  block (eosjs `blocksBehind`) can still fail the first transaction on an idle chain. Use `expireSeconds ≥ 120`.

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
8. **`get_block_info` timestamp**: print the block time as `YYYY-MM-DDTHH:MM:SS.mmm` (it currently uses Rust's debug format).
9. **Request parsing like nodeos**: accept numeric `block_num_or_id`, `index_position` names, numeric strings, and
   `EOS…` key spellings in `get_required_keys`, so the edge's normalization can go away.
10. **Action-sequence continuity for history**: expose per-account action sequence numbers in a form history APIs
    can continue across the import, so v1 positional paging can be exact.
