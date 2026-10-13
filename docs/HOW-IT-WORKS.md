# How an XPR → PulseVM cutover works

**Read this first.** It explains the whole cutover in plain terms, with the measured numbers, and says
plainly what is proven, what is only implemented and what does not exist yet. Every other document goes
deeper on one part; links are at the end. The risks, one by one, are in [RISK-REGISTER.md](RISK-REGISTER.md).

Status words used on this page, and nowhere loosely:

| Word | Means |
|---|---|
| **Measured** | a number taken from a real run on real data; the run is named |
| **Rehearsed** | done end to end on real services (5 BPs), at least once, with recorded evidence ([EVIDENCE.md](EVIDENCE.md)) |
| **Implemented** | the code exists and has tests; not yet done for real |
| **Estimate** | arithmetic from measured parts; not itself measured |
| **Not built** | a design or a requirement only |

*Last updated 2026-10-13, pulse-cutover v0.5.0-rc.29, PulseVM v1.0.0.*

---

## 1. The idea

XPR runs on Leap (nodeos). The cutover moves the running chain onto PulseVM, which runs on Metal (an
Avalanche-family network), at **one block, H**. What it is designed to achieve:

- everything up to and including block H is carried over: accounts, keys, permissions, balances, contracts
  and table rows;
- block H+1 is the first block PulseVM produces;
- the chain keeps its **chain_id**, so keys and wallets keep the same signing domain (whether a given signed
  transaction is accepted still depends on TAPOS, expiry, permissions and replay protection);
- public URLs keep working; the API edge in front of each node switches its backend.

The goal is an **atomic** cut: every transaction ends up on exactly one side, exactly once, and the network
as a whole either moves or does not. **That is the requirement, not today's state**: section 5 and the
[risk register](RISK-REGISTER.md) say how far the evidence goes. In particular, PulseVM v1.0.0 does not yet
sign with the source chain_id or enforce TAPOS, and the agent refuses to ignite XPR mainnet until it does.

It is **not instant**. Writes pause for the switch (section 3). Reads stay available during the cut: an
external probe measured 99.8% read availability through an API-mode flip on real testnet state, with a
sub-second gap at the switch.

## 2. Who does what

| Who | Runs | At the cut |
|---|---|---|
| **Coordinator** | mission control (a signing relay) and a signing key | publishes the signed event: H, roster, quorum, versions; can sign an abort |
| **Each block producer** | its own nodeos + `pulse-cutover` (producer mode) + a Metal validator | freezes its writes, snapshots exactly H, converts and checks it, joins the new chain |
| **API providers** | `pulse-cutover` (api mode) | the same cut, then flips its public `/v1` URL to the new chain |
| **History (Hyperion) providers** | api mode + `[hyperion]` | keeps old history read-only, indexes the new chain from H+1, one `/v2` URL across both |
| **Exchanges, wallets, apps** | nothing new | hold writes during the pause, reconcile, carry on ([EXCHANGES.md](EXCHANGES.md)) |

No one downloads a snapshot from anyone else. Every producer cuts and checks its **own** copy and the fleet
compares results. The fleet comparisons in this page apply to a **coordinated** ceremony (a signed event with a
roster and quorum, received by `pulse-cutover await`); the agent can also run alone from a config file, and then
it has no fleet gate. A mainnet cut must be coordinated.

## 3. The ceremony, step by step

Each producer's agent walks the same states and records each one in an append-only journal on disk. The steps
with outside effects (staging a file, creating the chain, starting ignition, flips, stopping the source) are
journaled **before** they act; a crashed agent resumes at its last state and re-runs that step, except that an
interrupted chain creation or ignition halts instead of guessing.

Times are for XPR mainnet. Numbers in *italics* are defaults that the config can change.

| # | State | What happens | Gate (must pass) | If it fails | Time |
|---|---|---|---|---|---|
| 0 | ARMED | Signed event received. nodeos is told in advance to write a snapshot at exactly H. | right chain_id, config passes the production profile, nothing stale staged | abort; nothing has changed | — |
| 1 | FROZEN | *24 blocks* (12 s) before H, the edge starts answering writes with **503**. Producers keep making blocks; transactions already admitted may still land up to H. | the freeze hook succeeds | abort, writes reopen | at H − *24* |
| 2 | SNAPSHOTTED | Leap writes the snapshot once H is final. Producers pause. Every block after H up to the pause must be empty. | snapshot is exactly H (height **and** block id); **0 transactions after H** | see "stopping after the freeze" below | finality: 2 min 56 s in fleet run g1b (testnet state); mainnet not measured. Snapshot write on a mainnet producer: not measured |
| 3 | VERIFIED | The snapshot is converted to PulseVM's format and checked (section 4). Then the fleet gate: wait until a quorum of the roster reports the **same** snapshot hash, fingerprints and protocol schedule. | 21-table compare passes; state root computed; quorum agrees; for XPR mainnet, the built-in list of unmet PulseVM prerequisites must be empty (today it is not, so mainnet stops here) | see "stopping after the freeze" | conversion **9.6 min, measured** on mainnet state (6-vCPU host); gate 20 s in g1b |
| — | *point of no return* | The agent journals that it is about to **create the chain** (upstream backend) or **start ignition** (fork backend). From this record on it never resumes the old chain. | the relay positively confirms "no abort" for this signed event; boot artifacts unchanged | halt; a human decides | — |
| 4 | IGNITED | The chain is created on Metal and started from the converted state. | new chain shows the source chain_id and the cut's block id at H | halt | create 11 s + ignite 31 s in g1b |
| 5 | LIVE | The head passes H, keeps advancing with no gap over *20 s* for *60 s*, the `on_live` hook flips the edge and reopens writes. | sustained progress; `on_live` succeeds | coordinated: wait as **degraded** while a quorum reports the same chain, then halt if it does not recover; otherwise halt | 1–2 min in healthy fleet runs, up to ~10 min in degraded ones |

After LIVE the beacon keeps checking the new chain (new blocks, a probe transaction if configured) and reports
problems to mission control; it never rolls anything back.

**Stopping after the freeze.** Once writes are frozen, a coordinated producer that has to stop (a failed check,
a timeout, a signed abort) does not simply resume the old chain: its **resume guard** must first show, twice,
that no peer can still go forward. If it can, the agent resumes the old producer and reopens writes (ABORTED). If
it cannot, the agent ends **STRANDED**: it tries to keep the old producer paused and writes frozen, and pages a
human. STRANDED is reported as UNSEALED when the pause could not be confirmed, so operators must check that flag.

**Estimated total write pause on mainnet: about 15 minutes** (estimate): finality ~3 min + conversion 9.6 min +
gate, creation and ignition ~1 min + LIVE sustain ~1 min, plus the unmeasured snapshot write. Faster hardware
shortens the conversion; slow or degraded validators lengthen the end.

## 4. What "conversion" actually does

nodeos keeps its state in **chainbase**, a memory image only the same nodeos build can read. PulseVM keeps state
in **Arena**. They hold the same tables in different formats, so the state is translated once. No block is
replayed during the cut: it is a copy of the data, plus checks that the copy is exact. (Deferred transactions
pending at H are carried as data and execute later on the new chain.)

Measured 2026-10-13 on a real XPR mainnet snapshot (1.52 GB, block 408,328,634, 885,878 accounts,
7.6 M contract rows), official PulseVM v1.0.0 tools, one 6-vCPU / 16 GB host ([EVIDENCE.md](EVIDENCE.md#mainnet-conversion-2026-10-13)):

| Stage | What it does | Mainnet | Peak RAM |
|---|---|---|---|
| **export** | Starts the same nodeos XPR runs (Leap 5.0.3), offline, loads the snapshot, and has it write its state in SHiP format (605 MB) plus a sidecar with what SHiP leaves out: deferred transactions, account sequence counters, the 2,779 recent transaction ids used for replay protection (245 MB) | 169 s | not measured (inside a container) |
| **import** | Decodes every row and writes it into Arena; result: a 2.1 GB checkpoint the new chain boots from | 192 s | 11.2 GB |
| **compare** | Reads both sides again; row count and hash for each of 21 tables must match | 140 s | **13.7 GB** |
| **state root** | The upstream tool's whole-state root over the converted state: the value every producer must agree on | 72 s | 7.7 GB |
| **total** | | **573 s (9.6 min)** | |

The same run on testnet state (185 MB) took 67 s: time grows roughly in line with state size. We provision
**32 GB RAM** per node for mainnet (a requirement with headroom, not a measured minimum; `pulse-cutover doctor`
flags less when it can read the node's chain_id and memory) and 50 GB free disk.

**Today's result on mainnet: the compare fails on 3 of 21 tables** (`contract_index_double`,
`global_property`, `generated_transaction`; upstream issue #109). The importer reports all their rows imported,
so it may be a gap in the compare tool rather than lost data, but that is not yet verified, and until it passes
the gate refuses, as it should. Rehearsals passed only with an allowlist that is refused for mainnet.

## 5. The point of no return

Before the point of no return, mistakes can be undone: the old chain is only paused, and resuming it loses
nothing. After the new chain has taken real writes, going back would throw those writes away. So the design
makes every check happen before that point, and tries to make the whole network cross it together.

**What exists:**

- per node: the chain-creation (or ignition) intent is journaled before it happens; from then on the agent
  never resumes the old chain; failures halt, or wait as degraded while the fleet vouches for the chain
  (implemented; rehearsed in the October fleet runs; crash-tested with stubs on older releases);
- fleet gate (coordinated ceremonies): no node creates or ignites until a quorum reports identical evidence and
  the relay positively says "no abort" for this exact signed event (rehearsed);
- resume guard: a frozen node resumes the old chain only if the fleet view shows nobody can still go forward;
  otherwise STRANDED (rehearsed in fault runs);
- `join`: a stranded node can follow the chain a LIVE quorum runs, after checking its own evidence (rehearsed);
- a built-in refusal to ignite XPR mainnet while known PulseVM prerequisites are unmet (implemented).

**What does not exist: a fleet-wide commit point.** Everything above reads *unsigned* reports through one relay.
It narrows the split-brain case (some producers resume the old chain while others start the new one; it
happened in fleet run r4, 33 writes after H) but cannot rule it out. Closing it needs, upstream in PulseVM, a
**sealed start**: the new chain loads its state but cannot produce or accept transactions until a fleet-signed
COMMIT exists (issue #113, design in [DESIGN-authority-boundary.md](DESIGN-authority-boundary.md)).
**Until that exists, a public mainnet cut should not be scheduled.**

## 6. History and APIs

- **History is not converted.** The existing Hyperion keeps serving all XPR history up to H. A new
  hyperion-rs indexes the PulseVM chain from H+1. The federating router in this repo sends each `/v2` query to
  the old or new index by block range, so apps keep one URL and see continuous history (rehearsed on real
  testnet state, one node; not at mainnet history scale). PulseVM v1.0.0's SHiP output after the cut is
  incomplete (#109), which every SHiP indexer inherits.
- **`/v1/chain`** is served by the edge in front of PulseVM. Most of Leap's API is covered; some calls return
  501, and some are answered with fields left empty (for example staking and REX details in `get_account`); see
  [V1-COVERAGE.md](V1-COVERAGE.md). Blocks before H are not served by PulseVM.
- **What apps see:** HTTP 503 on writes during the pause, reads throughout, the head block number steps back
  once at the flip (the old chain's empty blocks after H are discarded), and after LIVE the same URL, keys and
  accounts. An accepted transaction on PulseVM means admitted, not executed: apps must confirm inclusion
  ([EXCHANGES.md](EXCHANGES.md)).

## 7. How it gets proven before mainnet

A mainnet cutover cannot be tried and undone. Confidence comes from a ladder of tests, each closer to mainnet,
each repeatable, with mainnet untouched until the second-to-last rung. Other large chain migrations used the same
approach: many rehearsals on real mainnet state (shadow forks), then real public testnets, then mainnet.

| Rung | What | What it proves | Touches mainnet? | Status |
|---|---|---|---|---|
| 1 | **Fleet rehearsals**: September, 5 BPs on 5 continents on a small dev chain (fork plugin); October, 5 BPs on real XPR testnet state with upstream PulseVM v1.0.0, signed events and injected faults | exact-H cut, conversion, fleet gate, ignition, abort, crash, partition, stranding and `join` | no | **rehearsed** (with the shared producer key and rehearsal patches; see EVIDENCE) |
| 2 | **Mainnet conversion** of a real mainnet snapshot | scale, timing, memory; that the tools run on mainnet state | no | **measured** 2026-10-13; found the 3-table compare gap |
| 3 | **Mainnet execution follower** (upstream PulseVM #118, #119): PulseVM verifies and applies every live mainnet block | that PulseVM computes the same results as Leap on real mainnet traffic, continuously | reads only | running upstream; needs steady uptime and alerts on any mismatch |
| 4 | **Readiness drill**: every producer's agent snapshots mainnet at an agreed H **without** freezing anything, converts, fingerprints and reports | that every real producer's infrastructure produces identical state at the same H, and how long each takes | no write impact (a scheduled snapshot does not stop production) | **implemented** (`pulse-cutover drill`), not yet run on a real producer |
| 5 | **Shadow cutover**: the full ceremony on a private Metal network, from real mainnet state at H, with the real chain_id and no allowlist; then mainnet's own transactions from after H replayed into it and the results compared | the new chain on the real workload, end to end | no (private network) | **implemented** (shadow mode, `tools/shadow-replay.mjs`), not yet run; passing needs the upstream fixes |
| 6 | **Soak**: the shadow chain run for days under replayed load, with validator restarts and an upgrade | stability beyond the first minutes | no | not started |
| 7 | **Real XPR testnet migration** with the real testnet producers, apps and exchanges' test setups | the whole thing, with real people, on a public chain | no | not started |
| 8 | **Mainnet freeze drill**: an announced freeze at H on mainnet, then a deliberate signed abort; every producer resumes | the coordinated freeze and resume on mainnet itself, with no new chain | yes: a write pause of a few minutes, announced as maintenance | not started; needs producers' agreement |
| 9 | **Mainnet cutover** | — | yes | after 1–8 are clean |

> [!WARNING]
> A shadow chain carries the **real mainnet chain_id**, so it shares mainnet's signing domain: a transaction
> signed for it could also be accepted on mainnet, subject to TAPOS, expiry, permissions and replay protection.
> Never sign on a shadow chain with real keys and never expose it publicly; only replay transactions mainnet has
> already executed (mainnet's replay protection then rejects them if they leak back). Transactions whose
> reference block is after H cannot pass TAPOS on the shadow once it is enforced; the replay tool reports them
> separately.

## 8. What has to be true before a mainnet date

The full list, with owners and evidence, is the [risk register](RISK-REGISTER.md). The short version:

1. **Upstream PulseVM:** the compare passes on mainnet state (#109); the new chain signs with the source chain_id
   and enforces TAPOS (#108); CPU is billed in the units imported accounts expect (#106); each producer signs
   with its own key (#107); a sealed start exists (#113). The agent's built-in mainnet refusal names these.
2. **Metal:** mainnet validators run a MetalGo that speaks PulseVM v1.0.0's plugin protocol (45; MetalGo
   1.14.x); every producer has a registered, funded validator.
3. **Tests:** rungs 4–8 above done and clean, on the release that will be used.
4. **People:** a named go/no-go owner; coordinator keys held as a threshold, not one key; every producer and
   major API and history provider on the roster and rehearsed; exchanges told and confirmed.

## Further reading

| Document | Covers |
|---|---|
| [RISK-REGISTER.md](RISK-REGISTER.md) | every known risk, its safeguard, its evidence and who closes it |
| [PROCESS.md](PROCESS.md) | the ceremony in more detail, with diagrams |
| [../ATOMICITY.md](../ATOMICITY.md) | the five atomicity properties, the evidence for each, known limits |
| [DESIGN.md](DESIGN.md) | state machine, review findings (R-numbers), failure table |
| [DESIGN-authority-boundary.md](DESIGN-authority-boundary.md) | the fleet-wide commit point (not built) |
| [EVIDENCE.md](EVIDENCE.md) | every recorded run, with what it proves and what it does not |
| [EXCHANGES.md](EXCHANGES.md) | for exchanges, wallets and indexers |
| [OPERATOR-QUICKSTART.md](OPERATOR-QUICKSTART.md) | running it as a producer |
