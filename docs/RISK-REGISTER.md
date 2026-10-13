# Risk register: XPR → PulseVM mainnet cutover

Every known way the cutover can go wrong, what guards against it today, the evidence that the guard works,
and what closes it. Start with [HOW-IT-WORKS.md](HOW-IT-WORKS.md) if you have not read it.

*Last updated 2026-10-13 (pulse-cutover v0.5.0-rc.29, PulseVM v1.0.0). Add a row whenever something new is
found; never delete one, mark it closed with its evidence.*

**Status**

| Status | Means |
|---|---|
| **CLOSED** | the safeguard exists and has been rehearsed or measured with recorded evidence |
| **MITIGATED** | the safeguard exists and is tested, but not rehearsed on the real stack or at mainnet scale |
| **PARTIAL** | a safeguard narrows the risk; a known case still gets through |
| **OPEN** | no safeguard yet |
| **BLOCKER** | open, and a mainnet cutover cannot happen until it is closed |
| **DECISION** | needs a decision by people, not code |

**Owners:** *agent* = this repo · *upstream* = PulseVM · *Metal* = MetalGo / the Metal network ·
*BPs* = block producers · *coord* = the cutover coordinator and governance · *ecosystem* = exchanges,
wallets, apps, API and history providers.

**Summary (2026-10-13), 57 risks:** 6 BLOCKER · 21 OPEN · 4 PARTIAL · 10 DECISION · 14 MITIGATED · 2 CLOSED.
The six blockers: A-1 split brain (sealed start), A-6 compare fails on mainnet state, A-9 source chain_id
for signing, B-1 CPU units, B-2 per-producer keys, C-1 MetalGo 1.14 on Metal mainnet.

---

## A. Atomicity: every transaction on exactly one side, exactly once

| ID | Risk | Safeguard today | Evidence | Status | Owner | Closes when |
|---|---|---|---|---|---|---|
| A-1 | **Split brain**: some producers resume the old chain while others start the new one; two writable chains with the same chain_id | fleet gate; positive "no abort" before create/ignite; resume guard → STRANDED; `join`; mission control SPLIT alarm | happened in fleet run r4 (2026-10-06, 33 writes after H); rc.23–rc.29 guards rehearsed in fault runs. All rest on unsigned relay reports (ATOMICITY limits #1, #14) | **BLOCKER** | upstream + agent | sealed start (upstream #113) + fleet-signed COMMIT/ABORT certificates the agents verify ([design](DESIGN-authority-boundary.md)) |
| A-2 | A transaction lands on the old chain after H, so it exists there and not on the new chain | writes close 24 blocks before H; burn-off audit aborts on any transaction after H (fail-closed) | caught in multi-BP run 1, every BP aborted; 0 transactions after H in runs 2–6 and the 2026-10-06 fleet runs | CLOSED (for paths the freeze hook closes) | agent | — |
| A-3 | A write path the freeze hook does not close (private RPC, a relay node, a bot pushing p2p) | the burn-off audit catches the result and aborts | audit rehearsed; inventory of write paths is per BP | MITIGATED | BPs | each BP lists and fences every write path in its rehearsal |
| A-4 | Producers cut different blocks | snapshot scheduled at exactly H, pinned by height and block id; fleet gate compares hashes | identical on 5/5 in every rehearsal (ATOMICITY A1) | CLOSED (producer mode) | agent | — |
| A-5 | Exact H in API mode or after an agent restart | scheduled snapshot + restart restore, unit and fault-injection tested | not rehearsed on a real chain (ATOMICITY limit #2) | MITIGATED | agent | rehearsed in rung 4/5 of the test ladder |
| A-6 | Converted state differs from the old chain at H | 21-table compare + whole-state fingerprint; fleet compares fingerprints | **on mainnet state the compare fails on 3 tables** (`contract_index_double`, `global_property`, `generated_transaction`), measured 2026-10-13; rehearsals used an allowlist that is refused for mainnet | **BLOCKER** | upstream | the compare passes on a mainnet snapshot with no allowlist (#109) |
| A-7 | Compare and import share code, so a bug in both is invisible | same-importer limit (ATOMICITY #5) | — | OPEN | upstream | an independent implementation of the state commitment |
| A-8 | A transaction executed before the cut executes again after it (replay) | the recent-transaction dedupe set is carried (2,779 entries on mainnet) | 0 replays in the replay canary (runs 5, 6), sample only | PARTIAL | upstream | TAPOS enforced and the source chain_id used for signing (#108), canary on the shadow cutover |
| A-9 | The new chain does not sign with the source chain_id | rehearsal override only; refused for mainnet | v1.0.0 signs with MetalGo's blockchain id (#108) | **BLOCKER** | upstream | #108 fixed and rehearsed |
| A-10 | The old chain keeps producing after H (a producer that never froze, a standby producer, a node restarted unpaused) | mission control watches source heads; abort before freeze (ATOMICITY #15a) | no protocol guard | OPEN | coord + upstream | an on-chain action before H that retires the old schedule, or a source-side guard; plus monitoring |
| A-11 | API nodes not running the agent keep serving the old chain to readers | none (ATOMICITY #15b) | — | OPEN | ecosystem + coord | every public API provider on the roster, or fenced at the cut |
| A-12 | Deferred transactions pending at H (34 on mainnet 2026-10-13) execute wrongly or twice | carried by the sidecar; checked one-for-one by the importer | importer reports 34 imported; the compare cannot see them (#109); none existed on testnet, so never rehearsed | OPEN | upstream + agent | compare covers them; shadow cutover executes them and matches mainnet's results |
| A-13 | Action sequence numbers reset or gap (exchanges page history by sequence) | agent refuses a sidecar without exact sequences (rc.15) | exact on a real testnet export | MITIGATED | upstream | PulseVM requires the sidecar itself (#101) |
| A-14 | Booting from a cut block that contains transactions | — | tool refuses it, controller accepts it (#112) | OPEN | upstream | supported path documented and rehearsed |

## B. The new chain after the cut

| ID | Risk | Safeguard today | Evidence | Status | Owner | Closes when |
|---|---|---|---|---|---|---|
| B-1 | **CPU billed in the wrong units**: imported accounts run out of CPU within seconds | rehearsal-only patch (not in releases) | #106: one transfer costs ~97,000 "µs" | **BLOCKER** | upstream | #106 fixed in a release |
| B-2 | **Producer keys**: the migrated system contract re-elects producers at the first block; a node whose key differs stops producing | all rehearsals shared one producer name and key | #107; ATOMICITY #4 | **BLOCKER** | upstream | per-producer keys rehearsed |
| B-3 | Transactions the node acknowledged are never included | edge no longer over-claims; apps told to reconcile | 136, 77 and 4 lost in stage-2 runs (#105, ATOMICITY #12) | OPEN | upstream | terminal transaction outcomes; inclusion probe passes under load |
| B-4 | PulseVM computes a different result than Leap for some mainnet transaction | full replay of 401M mainnet blocks (September); live mainnet follower (#118, #119) | follower stalls when its Hyperion lags (seen 2026-10-13); no alerting on mismatch | MITIGATED | upstream | follower runs continuously to the cut with mismatch alerts |
| B-5 | A deployed contract uses an intrinsic PulseVM only stubs | `scan-contracts` preflight | 30 code hashes at risk on testnet state (advisory) | OPEN | upstream + agent | scan run on mainnet state; every at-risk contract cleared or owner told |
| B-6 | Throughput and block cadence on a WAN validator set (slow, gappy blocks; the ~50 s post-LIVE stall) | degraded mode waits instead of halting | ~1.35 s per block, 11–39 s gaps on 5 WAN validators (2026-10-06); stall unexplained (ATOMICITY #8) | OPEN | upstream + Metal | load test at mainnet traffic on the real validator count |
| B-7 | A protocol-upgrade schedule halts the chain | agent checks the schedule hash and supported version (rc.27) | fleet run f1 halted on a schedule change | MITIGATED | upstream | upstream issue filed and fixed; schedule frozen for the event |
| B-8 | `/v1` endpoints dapps rely on are missing or differ | 501 for unsupported calls; polyfills | V1-COVERAGE.md; #110, #111 | PARTIAL | upstream + ecosystem | coverage list signed off by the main dapps and wallets |
| B-9 | Time jump: the chain clock freezes for the pause, so oracles, staking and time-locked contracts see a ~15 min step | apps told | an oracle aged ~72 s in rehearsal | OPEN | ecosystem | oracle and time-sensitive contract owners confirm handling |
| B-10 | Nondeterminism (two validators compute different results) | dual import catches import nondeterminism | history of determinism bugs in earlier PulseVM versions | MITIGATED | upstream | shadow cutover with the real validator count and replayed traffic |

## C. Platform (Metal)

| ID | Risk | Safeguard today | Evidence | Status | Owner | Closes when |
|---|---|---|---|---|---|---|
| C-1 | **Metal mainnet runs MetalGo 1.13.5** (protocol 43); PulseVM v1.0.0 needs 1.14.x (protocol 45) | — | v1.0.0 release notes | **BLOCKER** | Metal | Metal mainnet validators on 1.14.x |
| C-2 | Every producer needs a registered, funded validator before H | — | ATOMICITY #7 | OPEN | Metal + BPs | registration and funding done and checked per BP |
| C-3 | Deployment model: classic subnet or converted (ACP-77) L1; who owns it | — | — | DECISION | coord + Metal | decided and rehearsed on that model |
| C-4 | Who becomes a validator, with what weight; DPoS votes no longer pick producers | — | — | DECISION | coord | governance decision published |
| C-5 | Creating the chain at the cut needs the subnet owner's key on the P-Chain | creation hook | ~11 s in rehearsal | DECISION | coord | key holder and procedure named and rehearsed |

## D. The ceremony tooling

| ID | Risk | Safeguard today | Evidence | Status | Owner | Closes when |
|---|---|---|---|---|---|---|
| D-1 | One coordinator key arms and aborts; producer reports are unsigned | ed25519 signed events; aborts are final tombstones | ATOMICITY #3, #16 | PARTIAL | agent + coord | threshold coordinator keys; producer-signed votes |
| D-2 | Mission control is a single relay; restoring it mid-event can hide an abort | append-only evidence log; refuse unknown answers | ATOMICITY #16, #17 | PARTIAL | agent | certificates from a second channel; rule: never restore during an event |
| D-3 | An agent crashes mid-ceremony | journal, resume, halt-after-ignite, process-group kills | 83/83 and 49/49 fault scenarios with stubs; fleet crash run r5 | MITIGATED | agent | crash runs on the shadow cutover |
| D-4 | A producer's hooks are wrong for its infrastructure (edge freeze, flip) | production profile; doctor; required hooks | run 5 shipped a non-executable hook | MITIGATED | BPs | each BP passes rungs 4 and 5 with its own hooks |
| D-5 | Mismatched agent versions across producers | gate refuses differing evidence; pinned release | rc.27 schedule-hash case | MITIGATED | agent + coord | one pinned release per event, checked at the gate |
| D-6 | Shadow cutovers need the mainnet chain_id, but rehearsal overrides are refused for it | — | — | OPEN | agent | a shadow mode that runs only on a private network and never with real keys |
| D-7 | No zero-impact way for every real producer to prove it can cut and convert mainnet | — | — | OPEN | agent | a drill mode: snapshot at H, convert, fingerprint, report; no freeze, no pause |
| D-8 | Supply chain: a tampered binary or script | pinned releases, sha256 sums, SHA-pinned CI, pins checked at release | rc.29 | MITIGATED | agent | — |

## E. Time and resources

| ID | Risk | Safeguard today | Evidence | Status | Owner | Closes when |
|---|---|---|---|---|---|---|
| E-1 | The write pause is long: ~15 min | — | conversion 9.6 min measured on mainnet state; finality ~3 min rehearsed | DECISION | coord | accepted and announced, or shortened (DESIGN.md §6 mirror) |
| E-2 | Snapshot-at-H write time on a mainnet producer is unknown | — | not measured | OPEN | BPs | measured in the readiness drill (rung 4) |
| E-3 | Not enough RAM: the compare peaks at 13.7 GB on mainnet state | `doctor` requires 32 GB for XPR mainnet | measured 2026-10-13 | MITIGATED | BPs | every roster node passes doctor |
| E-4 | The slowest producer holds everyone (gate waits for quorum) | `fleet_timeout_secs` | — | OPEN | coord | timeouts set from drill measurements |
| E-5 | Upstream recommends keeping writes closed for XPR's max transaction lifetime (3,600 s) after the cut, as a backstop to the dedupe set | dedupe set carried | upstream export README | DECISION | coord | decided: rely on the dedupe set, or add the hour |

## F. History, APIs and the ecosystem

| ID | Risk | Safeguard today | Evidence | Status | Owner | Closes when |
|---|---|---|---|---|---|---|
| F-1 | History federation at mainnet scale | federator refuses rows above H, fails closed | rehearsed on testnet, one node (ATOMICITY #13) | MITIGATED | ecosystem | run against a production Hyperion archive |
| F-2 | Legacy archive rate-limits (429) give partial history pages | federator flags partial answers | R23 | MITIGATED | ecosystem | providers use their own legacy Elasticsearch |
| F-3 | Exchanges mis-handle the pause or the head step back (double credit, missed deposit) | EXCHANGES.md | not tested with a real exchange | OPEN | ecosystem | each listed exchange confirms its plan; test against the testnet migration |
| F-4 | Wallets (WebAuth, Anchor, others) and their default endpoints | R1 and WebAuthn keys carried (demo chain) | partial | OPEN | ecosystem | each wallet tested on the shadow or testnet migration |
| F-5 | Dapps, bots, oracles break after the cut | — | perps DEX and bots ran across the cut in rehearsal | OPEN | ecosystem | top dapps tested on the testnet migration |
| F-6 | Other indexers and APIs (light API, AtomicAssets, explorers) | — | — | OPEN | ecosystem | inventory and owner confirmations |

## G. People, governance and process

| ID | Risk | Safeguard today | Evidence | Status | Owner | Closes when |
|---|---|---|---|---|---|---|
| G-1 | Nobody owns go/no-go and abort | — | — | DECISION | coord | named owner and written criteria |
| G-2 | A producer does not take part, or runs it wrong | roster + quorum; STRANDED | — | OPEN | BPs + coord | every active and standby producer rehearsed and on the roster |
| G-3 | On-chain approval for the migration and for retiring the old schedule | — | — | DECISION | coord + BPs | the msig and its timing agreed |
| G-4 | Communication fails (users, exchanges, apps surprised) | — | — | OPEN | coord | announcement plan, status page and incident channel |
| G-5 | Key custody: coordinator and producer keys | — | — | DECISION | coord + BPs | threshold coordinator keys; per-BP producer keys (B-2) |
| G-6 | Validators' right to run PulseVM under its license | — | PulseVM Business License | DECISION | upstream | written confirmation for every validator |
| G-7 | After LIVE there is no way back without losing writes | halt instead of resume; forward fixes only | inherent | DECISION | coord | forward-fix runbook and HALT criteria agreed |
| G-8 | A shadow chain carries the real chain_id: anything signed there is valid on mainnet | rule in HOW-IT-WORKS §7 | — | OPEN | coord + agent | shadow mode refuses public listeners; written rule; nobody signs with real keys |
| G-9 | A human enters a wrong parameter (H, chain_id, roster) | signed events; `await` writes the config; doctor; production profile | — | MITIGATED | coord | two-person check on every published event |
