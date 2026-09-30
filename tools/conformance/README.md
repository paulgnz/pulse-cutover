# /v1 conformance harness

A differential test for the public `/v1` surface. It sends every call real XPR clients make to two endpoints and
reports where the answers differ:

- **A** (reference): a Leap 5 nodeos, plus Hyperion for `/v1/history/*`.
- **B** (candidate): the PulseVM stack, meaning the `/v1` edge (`gateway/server.js`) and the federator
  (`federator/server.js`). [docs/V1-COVERAGE.md](../../docs/V1-COVERAGE.md) says what B serves and how.

When both sides hold the same imported state at H, every difference the harness reports is something a wallet,
exchange or dapp would notice after the flip. Until that rig exists, the harness is validated by pointing it at two
Leap nodes (see [Validation](#validation-2026-10-01)).

| piece | what it does |
|---|---|
| `corpus.mjs` | builds a request corpus from a live Leap chain (read-only) |
| `diff.mjs` | sends the corpus to A and B, normalizes, classifies and reports; exits non-zero on an unexpected difference |
| `clients/run-clients.mjs` | runs real client libraries (eosjs 16–22, @proton/js, wharfkit, cleos) against one endpoint and records what they see |
| `clients/diff-clients.mjs` | compares two of those records |
| `allow/*.json` | accepted differences for a given pair of endpoints, each with a reason |
| `usage-weights.json` | relative traffic per endpoint on a mainnet API node (rank + bucket: top/high/medium/low/rare, no counts); orders the report and names the traffic behind a failing endpoint |

`corpus.mjs`, `diff.mjs` and `lib.mjs` need nothing but Node ≥ 18. They are operator tools and are never installed
on producer boxes. The client matrix installs its libraries only under `clients/node_modules` (gitignored, about 50 MB).

## Run it

```bash
cd tools/conformance
mkdir -p out

# 1. corpus from the reference chain (≈ 180 read-only requests at ≤ 4 req/s, about a minute)
node corpus.mjs https://tn1.protonnz.com --hyperion https://hyperion-testnet.protonnz.com --out out/corpus.jsonl

# 2. differential run (≈ 400 requests per side at ≤ 4 req/s per endpoint)
node diff.mjs --a https://tn1.protonnz.com --a-history https://hyperion-testnet.protonnz.com \
              --b https://<pulsevm-edge> [--b-history https://<federator-or-edge>] \
              --corpus out/corpus.jsonl --report out/report.html --json out/report.json
echo $?   # 0 = no unexpected difference, 1 = at least one, 2 = usage error

# 3. client matrix (install once, then one record per endpoint, then compare)
npm ci --prefix clients
node clients/run-clients.mjs --url https://tn1.protonnz.com --history https://hyperion-testnet.protonnz.com --out out/clients-a.json
node clients/run-clients.mjs --url https://<pulsevm-edge> --out out/clients-b.json
node clients/diff-clients.mjs out/clients-a.json out/clients-b.json --report out/clients.md
```

Useful `diff.mjs` flags:

- `--frozen`: the pre-flip rig. `live` paths are compared strictly and nothing is retried.
- `--allow file.json`: accepted differences.
- `--only get_info,get_account`: run a subset.
- `--rps N`: rate limit per endpoint. The default is 4. Public nodes should stay at 5 or below.
- `--retries N`: live-mode retries. The default is 2.

Generated corpora, reports and records go under `out/`, which is gitignored. Only
`test/fixtures/sample-corpus.jsonl` is committed.

## The corpus

`corpus.mjs` samples the chain it is given:

- **accounts**: system accounts, top `eosio.token` holders, producers and msig proposers
- **contracts**: the system contracts, plus dapp contracts found among code-bearing accounts, with one or two of
  their tables and scopes
- **keys**: taken from those accounts' permissions
- **blocks**: irreversible, deep, very early, reversible, and one that carries transactions
- **transaction ids**: from Hyperion or from recent blocks

For every endpoint in V1-COVERAGE.md it then emits requests in the styles clients actually use. The generator
fails if any endpoint in that document has no request.

- **Scalars**: numeric vs string block numbers, block id vs number, `json` true/false/`"true"`, `index_position`
  as `2`/`"2"`/`"secondary"`, `key_type` names, numeric and string bounds, string `limit`, `reverse`, `show_payer`,
  `encode_type`.
- **Keys**: `EOS…` vs `PUB_K1_…` spellings.
- **Transport**: empty body, GET, `text/plain` (eosjs' fetch default), no content-type, trailing slash.
- **Error cases**: unknown account, table, code or index; bad key or key type; malformed JSON; a JSON array
  instead of an object; missing fields. Clients match on error shape.
- **Failed transactions**: missing signature, unknown actor, TAPOS mismatch, expiration too far, malformed
  signature, unknown contract, expired. Each is sent through `push_transaction`, `send_transaction`,
  `send_transaction2` (with and without `return_failure_trace`) and `push_transactions`.
- **TAPOS reads as clients make them**: `get_block_header_state`, `get_block_info` and `get_block` at head-3
  and at LIB.
- **CORS**: preflights and `Origin` requests on the busiest endpoints.
- **Endpoints Leap 5 does not serve**, plus `/v1/node/get_supported_apis`.

**Nothing written can land.** Every transaction is unsigned or carries a malformed signature. It is a 0.0001
self-transfer, which `eosio.token` rejects even if it were ever executed, or it names an actor or contract that
does not exist. `diff.mjs` builds the failed-transaction bodies at run time from `tx_template`: the expiration is
live and TAPOS comes from A's LIB, so "missing signature" really is non-expired. The same bytes go to A and B.

Each corpus line is:

```json
{"id":"get_account#001-name","method":"POST","path":"/v1/chain/get_account","headers":{"content-type":"application/json"},
 "body":"{\"account_name\":\"eosio\"}","tags":["get_account","name","chain"],
 "volatile":["head_block_num","head_block_time","cpu_limit","net_limit","subjective_cpu_bill_limit","ram_usage"],
 "live":["core_liquid_balance","total_resources","voter_info"]}
```

Optional fields: `live`, `ignore`, `unordered`, `mask`, `expect_b` (`"501"` for a request-specific documented 501),
`dynamic` (`{"block_num":"head-3"}`, resolved from A's `get_info` at run time), `tx_template`, `cors`
(`"preflight"` or `"simple"`), `compare_headers`.

## How answers are compared

Paths are dot-separated. `*` matches one key or index, `**` matches a whole subtree, and `$` is the root.

| field | effect |
|---|---|
| `volatile` | compared by **shape** (types and field presence, not values): head/LIB fields, server versions, times, resource billing (`ram_usage`, `cpu_limit`, `net_limit`). These differ between two honest endpoints, and between Leap and PulseVM by design |
| `live` | like `volatile`, but only while the chain moves (balances, producer counters, row counts). `--frozen` compares them strictly |
| `ignore` | removed: fields one side adds by design (`pulsevm_head_block_time`, Hyperion's `query_time_ms`, `last_indexed_block`) |
| `unordered` | arrays sorted first, for APIs whose order is not part of the contract |
| `mask` | strings compared with timestamps, `file.cpp:line` and numbers of 6+ digits masked (error texts inside 2xx bodies) |

**Error bodies** (HTTP ≥ 400) are reduced to what clients match on: `code`, `error.code`, `error.name`,
`error.what` and the detail messages, with the same masking. `file`, `line_number` and `method` are dropped
because they differ between builds of the same software. **HTTP status is always compared.** A 202 is not a 200,
and a 500 is not a 400: nodeos answers an accepted `push_transaction` with 202 and a failed one with 500, and
clients branch on that.

**CORS preflights** are compared by what a browser would conclude: preflight OK, origin allowed, method allowed,
request headers allowed. Header spelling is not compared, because proxies word these headers differently.

On a live chain a mismatch is retried. If the retry agrees, the class is `equal-on-retry`: the chain moved between
the two reads. If A disagrees with itself, the class is `unstable`: state was moving and the request cannot be
judged.

## Reading the report

`diff.mjs` prints a table per endpoint and writes `--report` (`.md`, or `.html` for a self-contained page) and
`--json`. The report lists endpoints busiest first with their traffic bucket (`usage-weights.json`), then the details. Each detail
gives the request, both status codes, B's `x-pulse-edge` header and a minimal JSON diff (up to 8 paths).

| class | meaning | fails the run |
|---|---|:-:|
| `identical` | same status, byte-identical body | |
| `equal-after-normalization` | same status, same body after the rules above | |
| `equal-on-retry` | differed once, agreed on the next read (moving chain) | |
| `B-501-expected` | B answered a **documented** 501 (or 404 for `push_block`); see below | |
| `B-partial-expected` | a documented partial polyfill (`x-pulse-edge: partial…` / `wasm-unavailable`); the diff is kept in the report | |
| `allowed` | a failing class excused by an `--allow` entry; its reason is shown | |
| `unstable` | A changed between two reads; not judged | |
| `error-shape-diff` | both sides errored, but with a different status, code, name or message | ✔ |
| `DIFF` | anything else: a value, field, type or status difference | ✔ |
| `transport-error` | a side did not answer (timeout, connection refused) | ✔ |

### B-501-expected

V1-COVERAGE.md lists the Leap endpoints PulseVM v1.0.0 cannot serve yet: `get_code`, `compute_transaction`,
`send_read_only_transaction`, and `get_scheduled_transactions` while deferred transactions exist. The edge answers
these with a nodeos-shaped 501. When B answers **one of those endpoints** with 501, the class is `B-501-expected`.
The same applies to a single request the corpus marks `expect_b: "501"`, such as `get_transaction_id` with JSON
action data. The expectation is read from the document at run time, so a row that moves from "501 upstream" to
"native" makes that 501 a failure.

A 501 anywhere else is a `DIFF`. So is a `static-at-cut` endpoint answering `static-missing`: someone skipped
`tools/capture-static.mjs`.

## As the pre-flip gate

The intended ceremony use, not wired into `pulse-cutover` yet:

1. At H, with both sides frozen on the same state, generate the corpus from the source nodeos:
   `corpus.mjs <source> --hyperion <legacy-hyperion>`.
2. Run `diff.mjs --a <source> --a-history <legacy-hyperion> --b <edge> --b-history <edge> --frozen`, with the
   ceremony's allow-list (reviewed in advance, every entry with a reason).
3. Exit 0 lets the flip proceed. Exit 1 blocks it, and the report goes into the journal as evidence.
4. Run the client matrix against both. `diff-clients.mjs` must report no differences for the transaction-building
   flows: TAPOS, ABI fetch and serialization.

`--frozen` matters here: on the rig nothing may move, so balances, counters and row counts are compared exactly.

## Validation (2026-10-01)

Numbers from the runs this harness was built with. None of this is PulseVM evidence yet.

- **Leap vs Leap** (XPR testnet; A = tn1.protonnz.com v5.0.3 + hyperion-testnet.protonnz.com, B = a public v5.0.0
  node that also serves Hyperion). 393 requests over 41 endpoints: 300 identical, 69 equal after normalization,
  1 equal on retry, 23 allowed, **0 unexpected**. The allowed entries (`allow/leap-vs-leap-testnet.json`) are
  genuine differences between the two deployments:
  - node configuration: supported APIs, the transaction-status plugin, read-only threads, and a partial block log
    (no block 2)
  - one libstdc++ error text
  - Hyperion version wording
  - history retention: pos 0 is a different action on each index
  - `get_key_accounts` returns nothing on one of the Hyperion instances for keys the chain holds
- **Client matrix, Leap vs Leap**: 9 libraries, about 100 flows each side. Everything matches except
  `key_accounts` (the Hyperion difference above). Transaction bytes minus TAPOS are identical across libraries and
  endpoints. `useLastIrreversible` with `expireSeconds: 120` builds an **already expired** transaction on XPR
  testnet: LIB trails head by about 165 s, and eosjs/@proton/js set the expiration from the LIB block's time.
- **Edge over Leap, first pass**: the edge run locally, its native base pointed at a v5.0.0 node. Among its
  differences from Leap:
  - `send_transaction2` answered 500 where Leap answers 202 + failure trace; `push_transactions` answered 200,
    not 202, and shortened each error.
  - `get_required_keys` and `get_accounts_by_authorizers` echoed `PUB_K1_…` where Leap prints `EOS…`, and the
    rows came in a different order.
  - `get_activated_protocol_features` paged by 10; Leap 5 ignores `limit`.
  - Error shapes differed for empty and malformed bodies and for unknown endpoints.
  - The edge was more lenient than Leap: trailing slashes, `get_transaction_id` wrappers, `get_producers {}`,
    header state for irreversible blocks.
  - `get_block_header` left out `block_extensions`.
- **Edge over Leap, after the fixes**: native base, A and corpus all tn1 v5.0.3; history on the v5.0.0 node's
  Hyperion. 393 requests: 329 identical, 18 equal after normalization, 13 B-501-expected, 4 B-partial-expected,
  29 unexpected (28 DIFF, 1 error-shape).
  - 10 exist by construction. `get_raw_block` and `get_producer_schedule` need PulseVM's JSON-RPC, and there is
    none in front of a Leap node.
  - 12 are failure-trace detail. Leap's `send_transaction2` trace stack carries each entry's `data`, and its
    `push_transactions` error text carries each entry's argument line. The node's error JSON does not carry
    either, so the edge cannot rebuild them. This is an upstream item.
  - 5 are `get_accounts_by_authorizers`. In three, Leap orders rows by permission-object id, and the edge
    approximates that as account creation time, then name. That differs only for accounts created in the same
    block by different transactions (`eosio.rex`). The other two are the accounts mode: the history index behind
    discovery does not list the controlled account.
  - 2 are `get_supported_apis`: the edge lists what it serves, and nodeos lists its plugins.

## Tests

`node --test tools/conformance/test/*.test.mjs` needs no network and runs in CI with the edge and federator tests.
It covers the normalizer, the classifier, the V1-COVERAGE.md parser, CORS verdicts, the allow-list and run-time
transactions. It also runs `diff.mjs` end to end against in-process mock endpoints.

## Limits

- **Successful writes are never exercised.** Status parity for an accepted transaction (202) needs a funded test
  account on the rig. The same applies to failure modes that need a valid signature: assertion failure, CPU/NET
  exceeded, duplicate transaction.
- **cleos in Docker** runs only for `--cleos-image name=image` entries and is skipped with a reason when Docker
  does not answer. No official Leap 3.2/4.0/5.0 images were verified.
- **@proton/js versions**: the matrix pins 22.0.3 (oldest on npm), 26.1.71 and 30.1.0. No `@proton/*` npm package
  has a 2.1.x release, so user agents reporting 2.1.15–2.1.66 are probably a wallet app's own version, not
  @proton/js.
