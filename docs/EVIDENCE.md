# Recorded evidence

Every rehearsal run behind the numbers in the README, [PROCESS.md](PROCESS.md) and
[ATOMICITY.md](../ATOMICITY.md), with what each one proves **and what it does not**. All
figures are read from the runs' fsynced journals or, for client-side availability, from an
external probe that ran off the box. The journals and logs are kept with the rehearsal notes;
this page is the public record of what they show.

Design context (state machine, R-numbers, failure table): [DESIGN.md](DESIGN.md).

## Read this first: what none of these runs cover

Every run below used a **fork build** of the PulseVM plugin (the `feat/arena-snapshot-import`
branch; the multi-producer runs used the `v0.0.0-arena-mempoolfix.1` lineage) on **metalgo
1.13.5, plugin protocol 43**. None ran upstream PulseVM **v1.0.0** (protocol 45, metalgo 1.14.x).
The multi-producer runs used a **private** Metal network, not Tahoe, and every validator
**shared one producer name and key**. No mainnet event has run. The re-qualification list for
the intended stack is in [ATOMICITY.md](../ATOMICITY.md#upstream-stack-re-qualification).

The agent has also changed since most of these runs. The main differences, noted per run:

- Since rc.6, a failure after ignition starts **halts** (sealed; the source is not resumed). The
  August runs predate this: their post-ignition aborts rolled back automatically.
- Since late September (just before the multi-producer rehearsal), **any** transaction in the
  burn-off window aborts. After run 5 of that rehearsal, `post_ignite` and `on_live` were made required.
- Since rc.5/rc.6, every mode must cut at **exactly H**, API mode schedules its snapshot at H,
  and the target's block id at H is checked. None of this has run against a real chain yet
  (ATOMICITY Known limits #2).
- The LIVE gate now requires `live_sustain_secs` (60 s) of progress with no gap over
  `live_max_gap_secs` (20 s). The August runs and the multi-producer runs passed LIVE on the
  first block past the cut.

## Summary

| Run set | Date | Source chain | Target | Mode | Result |
|---|---|---|---|---|---|
| [Single-producer demo](#single-producer-demo-dev-chain-2026-08-21) | 2026-08-21 | Leap 5.0.3 dev chain, 1 producer | fresh Tahoe subnet | producer (`pause_at_h`) | 1 LIVE (cut = H, write gap 15.0 s), 2 correct aborts |
| [API mode, live XPR testnet](#api-mode-on-the-live-xpr-testnet-2026-08-21) | 2026-08-21 | node synced to the live XPR testnet | Tahoe (headline), local 1-node network (loop) | api (`simulate_freeze`) | LIVE; 99.81% read availability; 12/12 loop runs |
| [Hyperion and bp mode](#hyperion-mode-and-bp-mode-2026-08-21-second-session) | 2026-08-21 | same | Tahoe (hyperion), local 1-node network (bp, loop) | hyperion, producer (`schedule_at_h`) | hyperion: 1 correct abort, 1 LIVE; bp: LIVE at exactly H; loop 22/22 |
| [Multi-producer rehearsal](#multi-producer-rehearsal-5-bps-september-2026) | 2026-09-28/29 | Leap 5.0.3, 5 producers | private 5-validator Metal network | producer, 5 BPs | run 1 aborted on all 5; runs 2–6 LIVE on all 5 |
| [Linux fault injection](#linux-fault-injection-rc9-and-rc10) | 2026-09-29/30 | stubbed nodeos | stubbed metalgo | producer and api | rc.9: 83/83 with a shim, critical BUG-1 found; rc.10: 49/49 with no shim |

---

## Single-producer demo (dev chain, 2026-08-21)

**Setup.** Source: a fresh Leap 5.0.3 dev chain (nodeos in an Ubuntu 22.04 container), single
producer `eosio`, `eosio.token`, three funded test accounts, and a traffic generator sending one
transfer every 2 s. Target: a fresh PulseVM chain on a fresh Tahoe subnet, on a second, isolated
metalgo instance, chain config pre-staged with `snapshot_path` and `import_cpu_scale = 143`. The
agent ran unattended from ARM to a terminal state in all three runs.

**Run 3 (headline): declared H = 3405, cut at exactly H.**

| Time (UTC) | State | Δ | Evidence journaled |
|---|---|---|---|
| 01:49:25.961 | ARMED | — | head 3165, H = 3405, preflight ok (incl. the R12 stage-path check) |
| 01:51:25.445 | FROZEN | +119.5 s (waiting for H) | write-freeze hook ran; last accepted user transaction at 01:51:24.802; production continues (R1) |
| 01:51:28.319 | SNAPSHOTTED | +2.9 s | create_snapshot 380 ms; cut pinned at 3405 = H, id `00000d4daad8…`; pause + 8-poll quiescence; burn-off 2 blocks, 0 transactions |
| 01:51:28.328 | VERIFIED | +9 ms | sha256 `370c0b7b…`; two fresh-arena imports identical; 19 roots captured; 7 accounts, 1 code object, 16 permissions |
| 01:51:39.373 | IGNITED | +11.0 s | metalgo restart; target serves the source chain_id at head 3405 = the cut block id |
| 01:51:40.478 | LIVE | +1.1 s | traffic repointed to PulseVM; block 3406 minted (`00000d4eda87…`) |

- **Write gap 15.0 s** wall clock (`write_gap_ms_wallclock: 15033`). Block timestamps: last source
  block 01:51:25.500, first PulseVM block 01:51:39.500 (14.0 s). Reads never stopped: nodeos kept
  serving them, paused.
- The gap was dominated by the metalgo restart (11 s); snapshot and verification took under 3 s
  at dev-chain scale.
- **Continuity.** Same chain_id served by PulseVM; numbering continued 3405 → 3406; the same keys
  that signed cleos transfers before the cut signed pulsevm-js transfers after it (28+ post-cut
  transfers, head 3434 when sampled). The `eosio` balance matched exactly (997000.0000 SYS) and
  the test accounts' sum was conserved at 3000.0000 SYS.

**Runs 1 and 2: correct aborts.** Both reached IGNITED with a dual-import-verified cut at H
(run 1: 2576), but the target's producer key did not pair with its genesis `initial_key` (R11),
so it presented the cut and never produced. The LIVE gate refused, `quorum_timeout` expired, the
agent aborted and resumed the source producer; nothing public had flipped. Run 2 also exposed
R12 (a stale staged snapshot pre-pinned a fresh chain). Both led to preflight or install checks.

**Proves:** the state machine end to end on one node: exact-H cut, dual-import verification,
ignition from the verified snapshot, same chain_id and block numbering, same keys, and aborts
that left the source chain running.

**Does not prove:** anything at scale (7 accounts) or with more than one producer; the
`schedule_at_h` path; any current behaviour after ignition. Under rc.11 the run 1 and run 2
failures would **halt** (sealed, source not resumed), not roll back, because they happened after
ignition started.

---

## API mode on the live XPR testnet (2026-08-21)

API mode is the ceremony an RPC provider runs: nodeos serving a public `/v1` URL until the new
chain serves the same URL. API nodes do not produce, so this could run against the live XPR
testnet: `simulate_freeze = true` declares H := LIB + margin and proceeds when LIB ≥ H, as if
the producers had frozen. The chain did not actually stop, so the cut lands wherever the next
snapshot finalizes, and the journal records the real cut.

**Setup.** Source: nodeos 5.0.3 (Ubuntu 22.04 container) restored from a standard public XPR
testnet snapshot and synced live to the testnet (chain_id `71ee83bc…`), behind nginx serving
`/v1/chain`. Target: a fresh Tahoe subnet and chain, chain config staged with `snapshot_path`
declared and the file absent (R12). The box was prepared by `./install.sh --mode api` and the
ceremony driven by `./cutover.sh`, unattended.

| Time (UTC) | State | Δ | Evidence |
|---|---|---|---|
| 03:16:19.990 | ARMED | — | head 401579134, LIB 401578805, H = 401579045 (LIB + 240) |
| 03:18:24.162 | FROZEN | +124.2 s | LIB reached H; simulated freeze journaled as such |
| 03:21:18.086 | SNAPSHOTTED | +173.9 s | create_snapshot waits until the snapshot block is irreversible (R17); cut 401579370 = H + 325, id `17ef9d6a…` |
| 03:21:22.151 | VERIFIED | +4.1 s | sha256 `1e56fb1b…`; dual import identical; 19 roots; 32,513 accounts, 636 code objects, 65,804 permissions (180 MB snapshot) |
| 03:21:36.584 | IGNITED | +14.4 s | metalgo restart; target serves chain_id `71ee83bc…` at head = cut id |
| 03:21:37.731 | FLIPPED | +1.1 s | nginx upstream swapped to the gateway; health 4/4: public head = target head = cut |
| 03:21:38.733 | LIVE | +1.0 s | operator's own `systemctl stop nodeos`; public re-check 4/4 |

**Read availability.** An external probe sent `POST /v1/chain/get_info` to the public URL every
250 ms for 13.5 min, from before ARM to 5 min after LIVE: **3,229 requests, 3,223 OK, 99.81%**
(1 non-200, 5 transport errors). At the flip, the last nodeos-served read was 03:21:36.245 and
the first PulseVM-served read 03:21:36.999: a **0.75 s gap** (2 failed probes) during the nginx
reload. The largest gap in the whole trace (1.26 s) happened before the ceremony froze, from
ordinary internet noise between the probe and the box.

**Write path.** After LIVE, a transfer signed on a separate machine (key never exported) and sent
to the public `/v1/chain/push_transaction` (nginx → REST gateway → `pulsevm.issueTx`) executed in
block **401579371** (numbering continues from the cut), tx `c2266af5…`. The receiving test
account was then exactly 0.1000 XPR higher on PulseVM than on the real testnet: the source chain
did not see the transaction.

**Loop harness, N = 12.** `pulse-cutover loop` restored both sides each iteration: the source
nodeos restarted and kept live-syncing the real testnet (every iteration cut at a fresh real
LIB); the target was a fresh subnet and chain on a **local single-node metalgo network** (about
11 s to create). Same ceremony code as on Tahoe; only the consensus substrate differed (run 1:
194.66 s locally vs 194.57 s on Tahoe). Result: **12 LIVE, 0 failures**; ceremony gap
(FROZEN → LIVE) mean 190.0 s, median 187.8 s, p95 194.7 s, max 199.5 s.

**Proves:** zero-read-downtime `/v1` continuity through the same URL on real XPR testnet state
(32,513 accounts); a health-gated flip that can tell two engines apart without the chain_id;
writes through the unchanged public URL landing on PulseVM; repeatability of the API-mode
sequence (12/12).

**Does not prove:** a real freeze. The cut was H + 325, not H, because the testnet kept running;
today's API mode without `simulate_freeze` schedules the snapshot at exactly H, which has not
been rehearsed against a real chain. One API node, not a fleet. The loop ran on a local 1-node
network, not Tahoe. A failure at FLIPPED would now halt rather than revert the flip.

---

## Hyperion mode and bp mode (2026-08-21, second session)

Same box and same live-syncing testnet replica as the API-mode runs.

### Hyperion-mode runs

Hyperion mode is API mode plus `/v2` history: after IGNITED the agent starts hyperion-rs against
the new chain's SHiP, writes the cut boundary for the federating router
([federator/README.md](../federator/README.md)), waits for hydration, and flips `/v1` and `/v2`
in one stage. Setup: an Elasticsearch 8.17.4 container (2 GB heap), hyperion-rs, the federator
and the nginx `/v2` surface, all staged by `install.sh --mode hyperion`. The legacy (pre-cut)
source was a public XPR testnet Hyperion archive. A fresh Tahoe subnet and chain per run.

**Run 1: correct abort.** The ceremony passed IGNITED and hydration, both surfaces flipped, then
the public `/v2` gate refused: hyperion-rs reports `Indexer: Warning, last_indexed_block: 0` on an
idle imported chain, and the federator counted that as unhealthy (R20). The agent aborted, both
flips were reverted automatically, and nodeos was still authoritative. The external probe
measured **98.6% `/v1` availability through the flip, abort and revert**.

**Run 2: LIVE.** Cut 401597934 (id `17efe5ee…`), 32,513 accounts / 636 code objects, dual
import identical.

| Time (UTC) | State | Δ | Evidence |
|---|---|---|---|
| 05:58:52.921 | ARMED | — | head 401597704, LIB 401597369, H = LIB + 240 = 401597609 |
| 06:00:54.090 | FROZEN | +121.2 s | LIB ≥ H (simulated freeze) |
| 06:03:48.153 | SNAPSHOTTED | +174.1 s | snapshot at finality (R17); cut 401597934 = H + 325 |
| 06:03:52.115 | VERIFIED | +4.0 s | sha256 + dual import identical; 19 roots |
| 06:04:02.486 | IGNITED | +10.4 s | target serves the source chain_id at the cut |
| 06:04:02.783 | (hydrated) | +0.25 s | boundary staged; hyperion-rs healthy via the idle-at-cut rule |
| 06:04:04.089 | FLIPPED | +1.6 s | `/v1` → gateway and `/v2` → federator, both gates green |
| 06:04:05.280 | LIVE | +1.2 s | nodeos stopped last; ceremony gap 191.2 s |

**History continuity.** Minutes after the cut, after one post-cut transfer, a single public call
`GET /v2/history/get_actions?account=<test account>&limit=5` returned `total 3207 (= 1 local +
3206 legacy)` with boundary `cut_block 401597934`: the post-cut transfer (block 401597935) from
the local hyperion-rs, then pre-cut actions from the legacy archive. `get_transaction` federated
the same way: a post-cut id answered locally, a pre-cut id answered by the legacy archive with
`_premigration: true`. The post-cut write (tx `9736dced…`, block 401597935) exists only on
PulseVM; a control transaction that went to the real testnet by mistake (R22) exists only there.

**Availability.** External probe over 25.6 min spanning both runs (`/v1` get_info at 4 Hz, `/v2`
health at 4 Hz, `/v2` get_actions at 1 Hz; 9,923 probes):

| Surface | Result |
|---|---|
| `/v1` | 99.43% raw; 99.52% excluding two outages on the probe's own uplink (all endpoints failed together while the box journal shows it healthy). No non-200 from the box across two flips, one abort and revert, and one source shutdown. Run 2 flip: 0.50 s gap on both surfaces (2 probes) |
| `/v2` before the flip | 93.3%; every failure a 429 from the legacy archive rate-limiting the 4 Hz passthrough (R23) |
| `/v2` after the flip, through the federator | 99.76% health, 100% get_actions |

### bp-mode run

Source: the same live-syncing testnet replica (real 32,513-account state). Target: the loop
harness's local single-node network, with this box as the new chain's only producer and
validator. Exercised `freeze_strategy = "schedule_at_h"` and `source.quiesce_cmd` (which severs
p2p so a single node can stand in for "every producer paused"; a real fleet does not need it).

| Time (UTC) | State | Δ | Evidence |
|---|---|---|---|
| 06:20:54.781 | ARMED | — | head 401600323, LIB 401599997, H = LIB + 420 = 401600417; snapshot scheduled at exactly H |
| 06:21:41.661 | FROZEN | +46.9 s | head = H; write-freeze hook ran |
| 06:24:27.790 | SNAPSHOTTED | +166.1 s | LIB reached H; scheduled file picked up (file wait 0 ms); cut pinned to exactly H (id `17efefa1…`); pause; p2p severed; 2 late blocks absorbed; burn-off audit journaled 327 blocks, 2,789 transactions |
| 06:24:32.376 | VERIFIED | +4.6 s | sha256 + dual import identical; 32,513 accounts / 636 code objects |
| 06:24:41.001 | IGNITED | +8.6 s | target presents the source chain_id at head = H |
| 06:24:58.708 | LIVE | +17.7 s | block 401600418 minted (first post-cut block 06:24:56.500) by a signed transfer via the gateway; write gap 197.0 s |

bp mode cut at exactly H (the scheduled snapshot), API mode at H + ~325 (whatever finalizes
next); both waited about the same ~166–175 s for finality.

**Proves (both):** `/v2` history continuity through one URL across the cut; that the ratchet
held when the last gate failed (run 1); that a scheduled snapshot lands at exactly H on real
state.

**Does not prove:** the burn-off transactions in the bp run came from the live testnet, which
did not freeze; under today's code any burn-off transaction **aborts**, so that run would abort
at SNAPSHOTTED. Single node, local network for the bp target. The hyperion run 1 abort reverted
public flips after ignition; today that failure would halt. Federator hardening for R23 shipped
after these numbers were taken.

### API-mode loop statistics (N=22)

Two batches on identical substrate and config (batch A n = 12 above, batch B n = 10; batch
means differ by 0.3%). **22/22 LIVE, zero failures, zero aborts.** Every run cut at a fresh real
LIB of the live testnet; target on the local single-node network.

| Metric (ms) | mean | median | p95 | min | max |
|---|---|---|---|---|---|
| **ceremony gap (FROZEN → LIVE)** | **190,229** | 188,046 | 194,657 | 186,977 | 199,502 |
| total (ARMED → LIVE) | 218,253 | 219,213 | 223,413 | 212,646 | 225,345 |
| ARMED → FROZEN (wait for LIB ≥ H) | 28,023 | 26,600 | 32,877 | 24,585 | 34,873 |
| FROZEN → SNAPSHOTTED (snapshot at finality) | 176,613 | 173,985 | 179,964 | 173,693 | 186,063 |
| SNAPSHOTTED → VERIFIED (sha256 + dual import) | 3,077 | 2,875 | 3,989 | 2,727 | 4,100 |
| VERIFIED → IGNITED | 8,494 | 8,562 | 8,605 | 8,314 | 8,608 |
| IGNITED → FLIPPED | 1,048 | 1,055 | 1,063 | 791 | 1,183 |
| FLIPPED → LIVE (stop source + re-check) | 995 | 992 | 1,074 | 919 | 1,093 |

About 93% of the gap is the source chain's own finality wait (R17), invisible to readers but
part of the write-freeze window; the tooling (verify + ignite + flip + stop) is about 13.6 s.

**Proves:** the API-mode sequence is repeatable on real state (22/22). **Does not prove:** the
loop target was a local single-node network; the loop used `simulate_freeze` (inexact cut) and
the pre-rc.6 single-block LIVE gate.

---

## Multi-producer rehearsal (5 BPs, September 2026)

**Setup.** Five BPs in Sydney, Singapore, Los Angeles, New Jersey and Frankfurt. Each ran a Leap
5.0.3 producer of a small source chain (bios + `eosio.token`, schedule bp1…bp5) **and** a
metalgo 1.13.5 validator on a private Metal network (network id 88888, 5 subnet validators,
weight 100 each), with the fork PulseVM plugin (protocol 43). Producer mode, `schedule_at_h`,
live transfer traffic until each BP's `on_freeze`. From run 3, each BP also had a public TLS API
edge and bots wrote through the edges throughout; run 5 mixed nginx and HAProxy edges; run 6
put all five behind one public URL. Full narrative:
[README, Multi-producer cutover](../README.md#multi-producer-cutover-5-bps-5-continents); atomicity
view: [ATOMICITY.md](../ATOMICITY.md#evidence-from-the-5-bp-rehearsal).

| Run | H | Result | Write gap (s, min–max over 5 BPs) | Unattended? |
|---|---|---|---|---|
| 1 | 1368 | all 5 ABORTED, source resumed | n/a | yes |
| 2 | 2240 | all 5 LIVE | 242.3–243.0 | no: LIVE waited on manual post-ignite traffic |
| 3 | 3086 | all 5 LIVE | 71.8–72.4 | yes, but the plugin's mempool panics dropped admitted transactions |
| 4 | 5299 | all 5 LIVE | 70.5–71.0 | yes; ~50 s post-LIVE stall |
| 5 | 6220 | all 5 LIVE | 552.4–553.0 | no: `post_ignite` hook not executable; heartbeat sent by hand |
| 6 | 8539 | all 5 LIVE | 101.0–114.3 | yes |

- **Run 1.** All 5 wrote a byte-identical snapshot at exactly H (sha `e5e622f5…`). Writes froze
  at head ≥ H, so 3 in-flight transfers landed in block 1369 (H + 1). The burn-off audit caught
  it on all 5, every BP aborted and resumed its producer, and the source chain kept running. →
  `freeze_lead_blocks` (writes close at H − 24; the cut stays H).
- **Run 2.** Freeze at 2216; burn-off 70 blocks, 0 transactions. Identical staged snapshot on all
  5 (sha `9089d4c2…`), identical fingerprints. The target served the source chain_id at 2240 = the
  cut block id; all 5 validators later agreed on head 2249. A test balance continued from the cut
  through 9 post-cut transfers signed with the migrated key. PulseVM builds blocks on demand, so
  LIVE waited for manual traffic. → `post_ignite` heartbeat hook.
- **Run 3.** Freeze at 3062; burn-off 70 blocks, 0 transactions. FROZEN 23:00:13.5 → LIVE
  23:01:25.5 on all 5; client-side write gap 73.5 s; 143 HTTP 503s from the five edges. Only
  108 of 193 transactions admitted on PulseVM landed: the fork plugin panicked in the mempool on
  the block-verify path. Already fixed upstream; the fork was rebuilt with the fix
  (`v0.0.0-arena-mempoolfix.1`).
- **Run 4.** Rebuilt plugin, 0 panics; 79/79 admitted transfers landed. A perps DEX contract
  deployed on the source kept taking orders on PulseVM unchanged: 0 duplicate orders, 16/17
  post-cut orders landed, the oracle's newest price aged to ~72 s (under its 120 s limit).
  **New: a ~50 s stall right after LIVE** — blocks 5300–5303 accepted within 5 s, then nothing
  accepted until 5304 about 50 s later, while the validators were still re-peering after their
  ignite restarts, under four bots' backlog. Not seen in run 3 (lighter load). Unexplained.
- **Run 5.** Cut `0000184cbaf7c1cd…`; burn-off 62 blocks, 0 transactions; IGNITED spread 1.6 s.
  The `post_ignite` hook shipped without its execute bit and the ceremony did not stop on it, so
  every BP held the new chain at exactly H for about 8 minutes until the heartbeat was sent by
  hand. The A3 state diff was taken in that window: identical on all 5 over 19 accounts,
  5 contracts and 32 table scopes. The replay canary passed. → `post_ignite` and `on_live` are
  now required to succeed.
- **Run 6.** Cut `0000215bde3e2263…`; burn-off 73 blocks, 0 transactions. FROZEN 01:07:00 →
  IGNITED 01:08:00.6–01:08:03.4 → LIVE 01:08:41.3–01:08:54.6 (local LIVE spread ~13 s). No
  manual step. The state diff ran automatically on every BP (identical, digest `df0f4017…`, same
  19/5/32 sample); the replay canary through the public URL accepted 0 pre-cut replays and landed
  10/10 held transfers once. The post-LIVE stall reproduced (92 expired + 12 timeouts after the
  flip).

**Proves:** `schedule_at_h` gives the same cut block, byte-identical snapshot and identical
fingerprints on five producers on five continents (A1); zero transactions after H with the write
lead (A2); state equality and exactly-once on the sampled state (A3, A4, runs 5–6); a symmetric
abort that left the source chain running (run 1); contracts and bots carrying on across the cut.

**Does not prove:**

- Fleet-wide all-or-nothing (A5). Only symmetric outcomes occurred (all abort, all LIVE); a
  partial abort or a partition was never exercised.
- Per-validator identity: all validators shared one producer name and key, because the plugin
  seeds its schedule from node config and genesis (R11). Per-BP keys were not run.
- Upstream PulseVM v1.0.0 / protocol 45 / metalgo 1.14.x / Tahoe.
- Whole-state equality: the state diff is a sample, and fingerprints are 64-bit.
- The signed-coordination path (`pulse-cutover await`) and the fleet gate post-date these runs.
- The ~50 s post-LIVE stall is unexplained. Under rc.11 defaults (`live_max_gap_secs = 20`) a
  stall like run 4's would be expected to halt the ceremony during the sustain window.

---

## Linux fault injection (rc.9 and rc.10)

Crash-recovery and rollback logic under injected faults on real Linux and systemd, with the
chain services stubbed.

**Setup.** One Ubuntu 24.04.4 box (procps-ng 4.0.4, systemd). Ubuntu 22.04 checks ran in an
`ubuntu:22.04` container on the same box (procps-ng 3.3.17). The released musl binary,
sha256-verified. A real XPR testnet snapshot (block 400588707, 181 MB), really verified (two
imports, 4.6 s). Stubs as real systemd units: a nodeos stub (chain API + producer API; head
advances 4 blocks/s, stops while paused; a scheduled snapshot lands when H is final), a metalgo
stub (serves PulseVM JSON-RPC only after a restart that finds the staged snapshot, i.e.
ignition by `systemctl restart`), and an edge stub switched by the flip hook. Real shell hooks
with injectable sleeps, background children and failures. Producer and API configs, both with
`schedule_at_h`, exact H and the lineage check on; each reaches LIVE uninterrupted in about 32 s
(16 and 18 journal records).

**Invariants checked after every recovery.** I1: the source producer is never resumed after
`ignite_started`, except by `--force-after-ignite` after the target was proven stopped in that
attempt. I2: a resumed run never proceeds past an unfinished operator rollback. I3: no orphaned
hook process group survives a resume or rollback. I4: no staged snapshot is left after ABORTED.
I5: HALTED survives restarts; `run` refuses; `unhalt --i-understand` journals and resumes. I6:
the journal is always readable, a torn tail is handled, exit codes are 0/3/4 as documented. I7:
the reused-PID guard works with real `ps -o lstart=`.

### rc.9: 83 of 83 scenarios pass (13 of them only with a shim)

| Group | Result |
|---|---|
| Kill matrix, producer mode: SIGKILL at each of 16 journal records, recovered by `run` and by `cutover.sh abort` | 32/32 |
| Kill matrix, API mode: 18 records × 2 | 36/36 |
| Targeted cases T1–T11 (13 results): mid-hook kills, a hook leaving a background child, hook timeout, HALT surviving restart, forced rollback with a real systemd fence (succeeding and failing), rollback killed after its intent record, recycled-PID guard, torn tail, corrupt last record, second agent refused | 13/13, with a shim |
| Baselines | 2/2 |

Expected and observed in the kill matrix: a kill before ignition → resume reaches LIVE, abort
exits 0 with ABORTED; a kill after `ignite_started` but before IGNITED → resume halts by design,
then `unhalt` + `run` reach LIVE, abort exits 3 (refused); a kill after IGNITED → resume reaches
LIVE, abort exits 3. The source was never resumed after ignition started. Three early FAILs were
harness timing errors (a kill aimed at one record landed after the next, written ~2 ms later)
and were re-run.

**BUG-1 (critical), found by this run.** The agent killed process groups by running
`/usr/bin/kill -SIG -<pgid>` without `--`, and procps parses `-<digits>` as options. On Ubuntu
24.04 a routine orphan-hook cleanup sent SIGTERM to **every process on the box** (sshd,
journald, networkd and the services all exited); on 22.04 it made the agent kill itself and its
caller. macOS parses the form correctly, so no unit test caught it. The 13 targeted cases that
reach this path ran with a labelled shim adding the `--`. **LOW-1:** zombie group members were
counted as survivors of SIGKILL, a false refusal (safe direction).

### rc.10 re-run, no shim: 49 of 49 pass

rc.10 signals process groups with `libc::kill` (never the `kill` binary), refuses pgid 0 and 1,
and ignores zombies. Re-run on a fresh Ubuntu 24.04.4 box and a 22.04 container, with the shim
asserted absent and a collateral-kill check around every case (sshd, journald and networkd main
PIDs unchanged; six sentinel processes outside every hook group alive; the nodeos and edge stubs
keep their PIDs). New snapshot (H = 408418371, 185 MB).

| Group | Result |
|---|---|
| Baselines (producer 16 records, API 18) | 2/2 LIVE, no collateral |
| Targeted cases T1–T11 (13 results), no shim | 13/13, no collateral |
| T1 with the process-group id forced to start with `1` (rc.9's `kill(-1)` trigger) | 2/2, no collateral |
| Kill-matrix smoke, producer mode, records 1, 4, 7, 10, 13, 16 × {resume, abort} | 12/12, no collateral |
| Ubuntu 22.04 container: orphan cleanup + rollback, then a stranger process with a wrong recorded start time | pass: orphan killed, rollback journaled `rollback_done`; stranger left alive, second rollback refused (exit 3) |

29 case results + 20 collateral checks = 49.

**Proves:** the journal, resume, halt, rollback and fence logic behaves as designed under
SIGKILL at every journal record, mid-hook kills and timeouts on real Linux with systemd; the
source is never resumed after ignition starts; BUG-1 is fixed.

**Does not prove:** real nodeos, metalgo or PulseVM (all stubbed); a multi-producer fleet; the
upstream import pipeline (`import_backend = "upstream"`, not stubbable); Ubuntu 20.04's procps
3.3.16 directly; the LOW-1 fix (needs a container without init; not re-exercised); faults
between records other than the constructed mid-hook and torn-tail cases. rc.11 has not been
through this suite.

---

## Findings R13–R23

Found in the August rehearsals above. R1–R12 are in [DESIGN.md](DESIGN.md#2-adversarial-review-findings-that-shaped-the-design).

| # | Finding | Fix / consequence |
|---|---|---|
| **R13** | The old Metal JS SDK's `buildCreateChainTx` treats its vmID argument as a VM **name**: the on-chain vmID is cb58 of the ASCII name zero-padded to 32 bytes. | Name the plugin binary after the on-chain vmID, read back from `platform.getBlockchains`; `install.sh` takes `vm_id` from the manifest, so put the on-chain value there. |
| **R14** | The legacy REST gateway returned the bare ABI for `get_abi`; eosjs and @proton/js read `.abi.version`, so it must be wrapped as `{account_name, abi}`. | Fixed in the gateway. `/v1` continuity needs ABI-shape parity tests, not just `get_info`. |
| **R15** | In API mode H is a finality target (wait for LIB ≥ H), so preflight must compare H with LIB, not head. The testnet's head runs ~330 blocks ahead of LIB. | Fixed: mode-aware preflight. The first arm attempt refused cleanly. |
| **R16** | Under `set -euo pipefail`, an optional-key lookup written as `grep \| head \| sed` killed `cutover.sh` silently when the key was absent. | `{ grep … \|\| true; } \| …`. Found only by running the script on a real box. |
| **R17** | On a live multi-producer chain `create_snapshot` blocks until the snapshot block is irreversible: 173.9 s of the 194.6 s API-mode gap. A real producer-side freeze pays the same LIB lag. | This is the ceremony-duration floor in v1; the v2 shadow mirror ([DESIGN.md](DESIGN.md#6-v2-shadow-mirror-sketch-not-implemented)) targets it. |
| **R18** | metalgo 1.13.5 loads `chain-aliases-file` for logs and metrics but does not register the alias on the HTTP router: `/ext/bc/<alias>/rpc` returns 404. | The loop harness routes through a one-line local nginx proxy rewritten with each new blockchain ID; the ceremony config never changes. |
| **R19** | hyperion-rs reports `Indexer: Warning, last_indexed_block: 0` on an idle imported chain, so an all-services-OK hydration gate deadlocks. | Hydration gate has an idle-at-cut arm (non-indexer services OK, head ≤ cut, nothing indexed). It cannot tell "nothing to index" from a broken SHiP stream; the post-LIVE write appearing in `/v2` is the definitive check. |
| **R20** | The same idle-at-cut signature failed the public `/v2` gate one layer up (hyperion run 1). | The federator's `local.ok` now uses the agent's predicate. The abort itself is a positive result (see run 1). |
| **R21** | hyperion-rs on an imported chain must index from cut + 1. `start_block = 0` asks SHiP for block 1, which the imported chain cannot serve, and the stream stays silent while `/v2/health` looks like a healthy idle chain. | The agent substitutes `{first_post_cut_block}` into `[hyperion].start_cmd`; `install.sh` ships a start script that sets `start_block`. |
| **R22** | proton-cli ignores edits to `networks[].endpoints` in its config; only an `endpoints[]` override takes effect. A write proof meant for the rehearsal box landed on the real testnet. | Use the `endpoints` key and check with `proton chain:get` before signing. The misrouted transaction became the divergence control. |
| **R23** | Public legacy Hyperion archives rate-limit: 429s to a 4 Hz probe (pre-flip `/v2` 93.3–93.5% vs 99.8–100% through the federator). Under 429s the federator could return HTTP 200 pages missing pre-cut rows (22 of 310 probes). | Federator hardening, after the recorded run: legacy health cached 5 s; partial answers flagged (`partial: true`, `total.relation: "gte"`, `source_errors`). A provider with its own old Elasticsearch avoids the dependency (same `LEGACY=` knob). |
