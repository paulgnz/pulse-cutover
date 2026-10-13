# How an XPR → PulseVM cutover works

**Read this first.** It explains the whole cutover in plain terms, with the measured numbers, and says
plainly what is proven, what is only implemented and what does not exist yet. Every other document goes
deeper on one part; links are at the end. The risks, one by one, are in [RISK-REGISTER.md](RISK-REGISTER.md).

Status words used on this page, and nowhere loosely:

| Word | Means |
|---|---|
| **Measured** | a number taken from a real run on real data; the run is named |
| **Rehearsed** | done end to end on real services (5 BPs or more), at least once, with recorded evidence |
| **Implemented** | the code exists and has tests; not yet done for real |
| **Not built** | a design or a requirement only |

*Last updated 2026-10-13, pulse-cutover v0.5.0-rc.29, PulseVM v1.0.0.*

---

## 1. The idea

XPR runs on Leap (nodeos). The cutover moves the running chain onto PulseVM, which runs on Metal (an
Avalanche-family network), at **one block, H**:

- everything up to and including block H is carried over: every account, key, permission, balance,
  contract and table row;
- block H+1 is the first block PulseVM produces;
- the chain keeps its **chain_id**, so existing keys, wallets and signed transactions keep working;
- public URLs keep working; the API edge in front of each node switches its backend.

It is called **atomic** because it is all-or-nothing at H: every transaction ends up on exactly one side,
exactly once, and the network as a whole either moves or does not. Section 5 says how far that is true today.

It is **not instant**. Writes pause for the switch (section 3). Reads (balances, history, explorers) keep
working throughout.

## 2. Who does what

| Who | Runs | At the cut |
|---|---|---|
| **Coordinator** | mission control (a signing relay) and a signing key | publishes the signed event: H, roster, versions; can sign an abort |
| **Each block producer** | its own nodeos + `pulse-cutover` (producer mode) + a Metal validator | freezes its writes, snapshots exactly H, converts and checks it, joins the new chain |
| **API providers** | `pulse-cutover` (api mode) | the same cut, then flips its public `/v1` URL to the new chain |
| **History (Hyperion) providers** | api mode + `[hyperion]` | keeps old history read-only, indexes the new chain from H+1, one `/v2` URL across both |
| **Exchanges, wallets, apps** | nothing new | hold writes during the pause, reconcile, carry on ([EXCHANGES.md](EXCHANGES.md)) |

No one downloads a snapshot from anyone else. Every producer cuts and checks its **own** copy and the fleet
compares results.

## 3. The ceremony, step by step

Each producer's agent walks the same states. Every step is written to a journal on disk before it acts, so a
crash resumes where it stopped.

| # | State | What happens | Gate (must pass) | If it fails | Time on mainnet |
|---|---|---|---|---|---|
| 0 | ARMED | Signed event received. nodeos is told in advance to write a snapshot at exactly H. | right chain_id, config passes the production profile, nothing stale staged | abort; nothing has changed | — |
| 1 | FROZEN | 24 blocks (12 s) before H, the API edge starts answering writes with **503**. Producers keep making blocks (now empty). | the freeze hook succeeds | abort, writes reopen | 12 s before H |
| 2 | SNAPSHOTTED | Leap writes the snapshot once H is final. Producers pause. Every block after H is checked to be empty. | snapshot is exactly H (height **and** block id); **0 transactions after H** | abort, resume, reopen | **~3 min** finality wait (rehearsed) + snapshot write (**not measured on mainnet**) |
| 3 | VERIFIED | The snapshot is converted to PulseVM's format and checked (section 4). | 21-table compare passes; state fingerprint computed | abort, resume, reopen | **9.6 min** (measured, mainnet) |
| 4 | fleet gate | Each agent waits until a quorum of the roster reports the **same** snapshot hash and fingerprint. | quorum agrees; coordinator has not aborted | abort only if the fleet check allows it, else STRANDED (section 5) | ~20 s (rehearsed) |
| 5 | IGNITED | The new chain is created on Metal and started from the converted state. **Point of no return for this node.** | new chain shows the source chain_id and block id at H | **halt** (never resume the old chain) | ~40 s (rehearsed) |
| 6 | LIVE | New blocks past H, sustained for 60 s; the edge flips to PulseVM and writes reopen. | head keeps advancing, probe transaction lands | halt, a human decides | 1–2 min (rehearsed) |

**Total write pause on mainnet: about 15 minutes**, plus the snapshot write time that has not been measured
on a mainnet producer. The pieces: finality ~3 min (rehearsed on testnet; expected to be similar on mainnet, not measured),
conversion 9.6 min (measured on mainnet state), gate + creation + ignition ~1 min, LIVE sustain ~1 min.

## 4. What "conversion" actually does

nodeos keeps its state in **chainbase**, a memory image that only the same nodeos build can read. PulseVM
keeps state in **Arena**. The two hold the same tables, in different formats, so the state has to be
translated once. Nothing is replayed and no transaction is re-executed: it is a copy of the data, plus
checks that the copy is exact.

Measured 2026-10-13 on a real XPR mainnet snapshot (1.52 GB, block 408,328,634, 885,878 accounts,
7.6 M contract rows), official PulseVM v1.0.0 tools, one 6-vCPU / 16 GB host:

| Stage | What it does | Mainnet | Peak RAM |
|---|---|---|---|
| **export** | Starts the same nodeos XPR runs (Leap 5.0.3), offline, loads the snapshot, and has it write all its state in SHiP format (605 MB) plus a sidecar with what SHiP leaves out: deferred transactions, account sequence counters, the 2,779 recent transaction ids used for replay protection (245 MB) | 169 s | small |
| **import** | Decodes every row and writes it into Arena; result: a 2.1 GB checkpoint the new chain boots from | 192 s | 11.2 GB |
| **compare** | Reads both sides again; row count and hash for each of 21 tables must match | 140 s | **13.7 GB** |
| **fingerprint** | One SHA-256 over the entire converted state: the number every producer must agree on | 72 s | 7.7 GB |
| **total** | | **573 s (9.6 min)** | |

The same run on testnet state (185 MB) took 67 s: time grows roughly in line with state size. Hardware:
**32 GB RAM** per node for mainnet (`pulse-cutover doctor` checks it), 50 GB free disk.

**Today's result on mainnet: the compare fails on 3 of 21 tables** (`contract_index_double`,
`global_property`, `generated_transaction`; upstream issue #109). The import reports all their rows
imported, so this looks like a gap in the compare tool rather than lost data, but until it passes, the gate
refuses, as it should. Rehearsals passed only with an allowlist that is refused for mainnet.

## 5. The point of no return

Before a node ignites, every mistake can be undone: the old chain is only paused, and resuming it loses
nothing. After the new chain has taken real writes, going back would throw those writes away. So the whole
design is about **making every check happen before the point of no return, and making sure the whole
network crosses it together**.

**What exists (implemented and rehearsed):**

- per node: ignition is journaled before it starts; from then on any failure halts and never resumes the
  old chain (rehearsed; crash-tested at every journal record with stubs);
- fleet gate: no node creates or ignites until a quorum reports identical evidence and the relay positively
  says "no abort" for this exact signed event (rehearsed);
- resume guard: a node that froze resumes the old chain only if the fleet view proves nobody can still go
  forward; otherwise it stays **STRANDED** (sealed, paused) and a human decides (rehearsed in fault runs);
- `join`: a stranded node can follow the chain a LIVE quorum runs, after checking its own evidence (rehearsed).

**What does not exist (not built): a fleet-wide commit point.** Everything above reads *unsigned* reports
through one relay. It narrows the split-brain case (some producers resume the old chain while others start
the new one; it happened once in rehearsal, run r4) but cannot rule it out. Closing it needs, upstream in
PulseVM, a **sealed start**: the new chain loads its state but cannot produce or accept transactions until a
fleet-signed COMMIT exists (issue #113, design in [DESIGN-authority-boundary.md](DESIGN-authority-boundary.md)).
**Until that exists, a public mainnet cut should not be scheduled.**

## 6. History and APIs

- **History is not converted.** The existing Hyperion keeps serving all XPR history up to H. A new
  hyperion-rs indexes the PulseVM chain from H+1. The federating router in this repo sends each `/v2`
  query to the old or new index by block range, so apps keep one URL and see continuous history
  (rehearsed on real testnet state, one node; not at mainnet history scale).
- **`/v1/chain`** is served by the edge in front of PulseVM. Most of Leap's API is covered; the gaps are
  listed in [V1-COVERAGE.md](V1-COVERAGE.md) and return a clear 501, not a wrong answer.
- **What apps see:** HTTP 503 on writes during the pause, reads throughout, the head block number steps back
  once at the flip (the old chain's empty blocks after H are discarded), and after LIVE the same URL, keys
  and accounts. An accepted transaction on PulseVM means admitted, not executed: apps must confirm
  inclusion ([EXCHANGES.md](EXCHANGES.md)).

## 7. How it gets proven before mainnet

A mainnet cutover cannot be tried and undone. The way to get confidence is a ladder of tests, each one
closer to mainnet, each one repeatable, with mainnet itself untouched until the last step but one. This is
the same approach other large chain migrations used: many rehearsals on real mainnet state (shadow forks),
then real public testnets, then mainnet.

| Rung | What | What it proves | Touches mainnet? | Status |
|---|---|---|---|---|
| 1 | **Fleet rehearsals** on real testnet state (5 BPs, 5 continents, faults injected) | the mechanics: exact-H cut, conversion, fleet gate, ignition, abort, crash, partition | no | **rehearsed** (rc.21–rc.29) |
| 2 | **Mainnet conversion** of a real mainnet snapshot | scale, timing, RAM; that the tools run on mainnet state | no | **measured** 2026-10-13; found the 3-table compare gap |
| 3 | **Mainnet execution follower** (upstream PulseVM #118, #119): PulseVM verifies and applies every live mainnet block | that PulseVM computes the same results as Leap on real mainnet traffic, continuously | reads only | running upstream; needs stable uptime and mismatch alerts |
| 4 | **Mainnet readiness drill**: every producer's agent snapshots mainnet at an agreed H **without** freezing anything, converts, fingerprints and reports | that every real producer's infrastructure produces identical state at the same H, and how long each takes | no write impact (a scheduled snapshot does not stop production) | **not built**: needs a no-freeze drill mode in this agent |
| 5 | **Mainnet shadow cutover**: the full ceremony on a private Metal network, from real mainnet state at H, with the real chain_id and no allowlist; then real mainnet transactions from after H are replayed into the shadow chain and their results compared | the new chain on the real workload, end to end | no (private network) | **not built**: needs a shadow mode (rehearsal overrides are refused for the mainnet chain_id today) and the upstream fixes |
| 6 | **Real XPR testnet migration** with the real testnet BPs, apps and exchanges' test setups | the whole thing, with real people, on a public chain | no | not started |
| 7 | **Mainnet freeze drill**: a scheduled, announced freeze at H on mainnet, then a deliberate signed abort; every producer resumes | the riskiest human and infrastructure part (coordinated freeze and resume) on mainnet itself, with no new chain | yes: a write pause of a few minutes, announced as maintenance | not started; needs BP agreement |
| 8 | **Mainnet cutover** | — | yes | after 1–7 are clean |

> [!WARNING]
> A shadow chain carries the **real mainnet chain_id**, so anything signed for it is also valid on mainnet.
> Never sign with real keys on a shadow chain and never expose it publicly; only replay transactions mainnet
> has already executed (mainnet's own replay protection then rejects them if they leak back).

## 8. What has to be true before a mainnet date

The full list, with owners and evidence, is the [risk register](RISK-REGISTER.md). The short version:

1. **Upstream PulseVM:** the compare passes on mainnet state (#109); the new chain keeps the source chain_id
   for signing and enforces TAPOS (#108); CPU is billed in the units imported accounts expect (#106); each
   producer signs with its own key (#107); a sealed start exists (#113).
2. **Metal:** mainnet validators run MetalGo 1.14.x (PulseVM v1.0.0 needs rpcchainvm protocol 45;
   MetalGo 1.13.5 is protocol 43); every producer has a registered, funded validator.
3. **Tests:** rungs 4–7 above done and clean, on the release that will be used.
4. **People:** a named go/no-go owner; coordinator keys held as a threshold, not one key; every producer
   and major API and history provider on the roster, rehearsed; exchanges told and confirmed.

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
