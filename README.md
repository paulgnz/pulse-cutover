# pulse-cutover

[![How the cutover works — 106s explainer](https://pulsevm.dev/media/cutover-explainer.png)](https://pulsevm.dev/guide/migrate-antelope-chain)

*106-second explainer + full methodology and recorded numbers: [pulsevm.dev/guide/migrate-antelope-chain](https://pulsevm.dev/guide/migrate-antelope-chain)*

pulse-cutover moves a running Antelope chain (XPR Network) onto the PulseVM
engine without users noticing: **same public URL, same chain_id, same account
state, zero read downtime**. One binary drives the whole ceremony unattended —
freeze, snapshot, verify, ignite, flip — and records every step, with evidence,
in a journal you can hand to anyone. Nothing a user can see changes until the
new chain is verified and serving; aborting at any earlier point leaves your
existing node exactly as it was.

**Want to help test? → [TESTING.md](TESTING.md)** — rehearsals never touch
production, take about 40 minutes, and give you operational familiarity before
any real event (plus credit for every setup you help us support).

**Using an AI agent (Claude Code etc.) to operate this? → [AGENTS.md](AGENTS.md)**
— repo map, machine-readable contracts (`doctor --json`, the journal), and
the safety rails an agent must follow.

---

## Start here — the operator walkthrough

Six steps. Every step is one copy-paste block, what you should see, and what
to do if you don't. Plain English throughout — anything in *italics* on first
use is defined in the [Glossary](#glossary).

### Step 0 — what you need

- **A box**: a spare Ubuntu 22.04 or 24.04 server, or your existing node box.
  The first tool you'll run (`doctor`) is strictly read-only and safe anywhere,
  including production. The later steps (install + rehearsal) belong on a
  spare/test box.
- **Rough specs**: 4+ cores, 16 GB RAM, 50 GB+ free disk. Don't measure by
  hand — `doctor` checks all of it and tells you exactly what's missing.
- **A running nodeos** on that box (native or docker, both fine), synced to
  the chain being migrated. For a rehearsal, a testnet node is perfect.
- **Time**: about 40 minutes end-to-end for a first rehearsal.
- **What this touches on your production infrastructure: nothing.** Steps 1–3
  only install tools and stage files. The only moment anything user-visible
  can change is the *flip* stage of a ceremony you explicitly start in Step 4
  — and a rehearsal ceremony runs entirely on the rehearsal box.

### Step 1 — get the tools

Two git repos side by side (the cutover agent and the PulseVM import library
it links against), then one build:

```sh
sudo apt-get install -y git curl build-essential jq
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && . "$HOME/.cargo/env"
git clone --branch feat/arena-snapshot-import https://github.com/paulgnz/pulsevm pulsevm-arena-import
git clone https://github.com/paulgnz/pulse-cutover
cd pulse-cutover
cargo build --release
sudo install target/release/pulse-cutover /usr/local/bin/
```

You should see the build end with:

```
    Finished `release` profile [optimized + debuginfo] target(s) in 7.15s
```

(In an organized test event the coordinator ships a prebuilt, sha256-pinned
binary in the ceremony bundle, and `install.sh` verifies and installs it for
you — then this step is just the two `git clone` lines, for the scripts.)

**If it didn't:** `failed to load manifest ... pulsevm-arena-import` means the
two repos are not side by side — both `git clone` commands must run in the
same parent directory.

### Step 2 — survey the box: `pulse-cutover doctor`

`doctor` reads your box — how nodeos runs, what nginx serves, disk, ports —
and gives a per-mode verdict. It never writes, restarts, or changes anything.

```sh
pulse-cutover doctor
```

You should see a table like this (real output from our rehearsal box,
trimmed):

```
pulse-cutover doctor v0.2.0 — api-cutover-test (2026-08-21T08:31:45Z)

HOST
  os                     Ubuntu 24.04 (x86_64, Linux 6.8.0-137-generic)
  cpu                    8 x AMD EPYC-Genoa Processor
  ram                    15.2 GB
  disk /                 265 GB free / 300 GB
  systemd / docker       yes / yes

NODEOS (source chain — yours, untouched)
  runtime                docker container `nodeos` (nodeos:5.0.3)
  chain api              http://127.0.0.1:8888
  version                v5.0.3
  chain_id               71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd
  head                   401616025
  producer_api           enabled
  state_history          no

WEB EDGE (nginx)
  version                nginx/1.24.0 (Ubuntu)
  route _                /v1/chain/ -> 127.0.0.1:8888 (upstream pulse_v1_backend)
  ...

VERDICTS
  bp        READY
  api       READY
  hyperion  READY
```

**You should see: `READY` for the mode you plan to run** (`bp` for block
producers, `api` for RPC providers — see [Which mode am I?](#which-mode-am-i)).

**`NEEDS` with a list** is normal on a first run. The common ones:

| doctor says | it means | fix |
|---|---|---|
| `producer_api_plugin (localhost-bound)` | the ceremony takes its snapshot through nodeos' producer API, and yours doesn't have it on | add `plugin = eosio::producer_api_plugin` to your nodeos config, restart nodeos. Keep it bound to 127.0.0.1 — it can pause your chain, so it must never be public |
| `a running, synced nodeos ... serving /v1/chain/get_info` | no nodeos answered on this box | start your node, or run doctor on the box that has one |
| `50GB+ free disk` | the snapshot + new chain need room | free up or mount disk |
| `source.stop_cmd must be declared` | doctor couldn't work out how to stop your nodeos (no systemd unit, no docker container) | you'll add a `stop_cmd` line to the manifest in Step 3 |

**`UNSUPPORTED`** means we genuinely can't drive your setup yet (e.g. apache
or caddy on the public edge, kubernetes-managed nodeos). The verdict says
exactly why. Jump to Step 5 and send us the report bundle — that is literally
how setups get added.

### Step 3 — stage everything: `./install.sh`

One command installs and configures every piece the ceremony needs. It does
**not** touch your running nodeos and does **not** change any public traffic.

It needs a *manifest* — a `ceremony.json` file saying which chain, which
target, and which pinned binaries (its shape is shown in
[the manifest section](#the-ceremonyjson-manifest) below). Where it comes
from:

- **Testing with us**: ask in the
  [Telegram group](https://t.me/+N1mAvoUDbtVmNTBh) for the current rehearsal
  bundle — the manifest plus the prebuilt, sha256-pinned binaries it refers
  to. (Every underlying knob is documented, field by field, in
  `examples/ceremony-api.toml` and friends.)
- **A real event**: the ceremony coordinator publishes one manifest and
  every operator uses the same file.

```sh
sudo ./install.sh --mode api --manifest ceremony.json
```

It re-runs `doctor` first and refuses (politely, with reasons and nothing
half-installed) if the box isn't ready. On success it ends with the
ARMED-READY banner — this one is from our rehearsal box:

```
============================================================
 ARMED-READY (mode: api) — installed, verified, staged.

 Doctor:  survey + per-mode verdicts in /root/api-cutover/doctor.json
          flip scripts templated from the detected nginx layout
          source stop: systemctl stop nodeos
 Source:  nodeos at http://127.0.0.1:8888 serving 71ee83bc... (untouched)
 Target:  metalgo-pulse tracking subnet 2QziKVhwqMh7tE41Pm5d7Wyg4CL9fFPDEsjtq8w9Z9zNfnvpNL
          NodeID NodeID-P5i8jjoZ2yepjXru6GqLVKjgMMtb96V3k
          chain i54aDguQgAHV2PqDbQPUKm9mURqbwJHoofb5ewyYuAf49d3Ng: waiting for the
          verified snapshot at /root/api-cutover/snapshot-cut.bin (absent by design)
 Public:  nginx /v1/chain -> nodeos:8888 (flip staged, NOT flipped)

 What happens at H (run: ./cutover.sh --manifest ceremony.json):
   1. agent watches the source chain to the freeze height
   2. snapshot via your nodeos producer_api at ~finality
   3. sha256 + dual-import 19-table fingerprint verification
   4. PulseVM ignites from the verified snapshot (same chain_id)
   5. /v1 URL flips to PulseVM + health check   <- only visible step
   6. YOUR stop command retires nodeos (reads never gapped)
============================================================
```

Note the last block: it tells you, before anything happens, exactly what the
ceremony will do. Re-running `install.sh` is always safe — it re-verifies and
converges instead of duplicating.

**If it didn't:** every refusal prints the precise reason and the fix (missing
plugin, stale snapshot file, no `/v1` route in nginx pointing at your nodeos,
...). If the reason doesn't make sense, run `pulse-cutover doctor` for the
full survey, or go to Step 5 and send us the bundle.

### Step 4 — run the ceremony: `./cutover.sh`

In a real event you run this when the coordinator says go. In a rehearsal you
run it whenever you like:

```sh
./cutover.sh --manifest ceremony.json
```

It first checks the manifest against the live chain and refuses to start if
anything is off. Then you watch the states scroll by — this is the real output
of a recorded rehearsal against the live XPR testnet:

```
ceremony starting — journal: /root/api-cutover/journal.jsonl
(the source chain stays authoritative until the last step; ^C + './cutover.sh abort' is always safe before FLIPPED)
[ARMED]       watching the source chain; preflight passed.
  [2026-08-21T03:16:19.995Z]    ARMED {"blocks_to_h_final":240,"head":401579134,"lib":401578805}
  [2026-08-21T03:17:50.298Z]    ARMED {"blocks_to_h_final":72,"head":401579302,"lib":401578973}
[FROZEN]      the freeze height is final on the source chain — taking the state snapshot next.
[SNAPSHOTTED] state snapshot cut + pinned to one exact block.
[VERIFIED]    snapshot hash + state fingerprints check out; staged for PulseVM.
[IGNITED]     PulseVM is up, serving the SAME chain_id, continuing at the cut block.
[FLIPPED]     public /v1 now answered by PulseVM — this was the only user-visible change.
[LIVE]        ceremony complete. Source retired. Same URL, same chain, new engine.

LIVE. Evidence journal: /root/api-cutover/journal.jsonl
```

How long each part takes (recorded, testnet-sized state; a bigger chain
mostly stretches the snapshot and verify phases):

- **ARMED** — until the declared freeze height arrives. Minutes to hours;
  the countdown lines show progress.
- **FROZEN → VERIFIED** — snapshot + verification, ~1–3 minutes.
- **IGNITED** — new engine boots from the snapshot, ~15 seconds.
- **FLIPPED → LIVE** — the URL swap itself took **0.75 seconds** across 22
  recorded runs; reads never stopped being answered.

**What LIVE means:** exit code 0, your public URL now serves the same chain
from the new engine, and the journal holds the evidence (hashes, block ids,
timings) for every step.

**What ABORT looks like:** the run stops, prints `[ABORTED]` with the reason,
and exits non-zero. This is safe by design — the ceremony changes nothing
user-visible until FLIPPED, and an abort at FLIPPED swaps the URL straight
back. Your original node was still running the whole time; in bp mode the
agent resumes it automatically (`./cutover.sh abort` does the same for a
stuck/^C'd run). An aborted rehearsal is a *useful* rehearsal: go to Step 5.

### Step 5 — share the evidence: `pulse-cutover report`

Whether it went LIVE or ABORTED, one command packs everything we need to
debug your rehearsal or add support for your setup — with secrets scrubbed:

```sh
pulse-cutover report
```

Real output (trimmed):

```
== pulse-cutover report bundle ==
  pulse-cutover-report-api-cutover-test-20260821-083145.tar.gz (28890 bytes)
  sha256 5f38ea34c7a1908099bdd63e3526a841b2fa6f1773568ff681223cd0252351fe

files in the bundle:
  - doctor.json
  - doctor.txt
  - ceremony.toml
  - journal.jsonl
  - logs/metalgo-pulse.log
  - logs/nodeos.log
  ...

redactions applied (review before sharing — nothing listed leaves the box unredacted):
      1 x private-key -> [REDACTED-private-key]
```

The sanitizer always runs: private keys, tokens and passwords come out as
`[REDACTED-...]`; chain ids, block ids and hashes are kept (they're the
evidence). The command prints the full file list so you can review before
sharing (`tar -tzf` the bundle). Add `--paranoid` to placeholder hostnames
and IPs too.

Share it (with the printed sha256):

- **Telegram**: [the cutover testing group](https://t.me/+N1mAvoUDbtVmNTBh)
- **GitHub**: a [rehearsal-feedback issue](https://github.com/paulgnz/pulse-cutover/issues/new?template=rehearsal-feedback.md)

That's the whole loop. [TESTING.md](TESTING.md) describes the rehearsal
program — what we're trying to cover and what testers get.

---

## Which mode am I?

One agent, one manifest format, three operator roles — pick with
`install.sh --mode bp|api|hyperion`:

| | **bp** | **api** | **hyperion** |
|---|---|---|---|
| Who runs it | block producer | RPC/API provider | API provider w/ Hyperion |
| What freezes | the chain (writes at API edge) | observed only (LIB ≥ H) | observed only |
| Snapshot | at **exactly H** | at ~finality (R17) | at ~finality |
| What flips | nothing public | `/v1` upstream | `/v1` **and** `/v2` together |
| What continues | production, numbering, chain_id | reads — zero gap | reads **and history** |

- **bp** — you help freeze the chain and become a producer/validator of the
  migrated chain. Writes reject at the API edge while empty blocks carry the
  chain to H; the snapshot is scheduled at exactly the declared block.
  Recorded: real XPR state, cut at exactly H, gap 197.0s.
- **api** — your job is `/v1` continuity: nodeos serving the public URL →
  PulseVM serving the *same URL*. Your nodeos outlives ignition and retires
  *last*, so reads never gap; the flip is a one-line health-gated nginx swap.
  Recorded: live XPR testnet, **99.8% read availability, 0.75s flip, 22/22
  loop runs LIVE**.
- **hyperion** — everything api does, plus `/v2` history continuity: post-cut
  history from local hyperion-rs, pre-cut history from the legacy archive,
  merged behind the same URL — **the URL keeps its memory**. Recorded: one
  public `/v2` call returning post-cut + 3,206 pre-cut rows minutes after the
  cut.

(Recorded evidence: wiki/59 Appendices B + C in pulsevm-experimental.)

---

## Glossary

Plain-English versions of every term this repo uses:

- **Ceremony** — one scripted, journaled migration run, from watching the old
  chain to serving traffic from the new one. A rehearsal is a ceremony on a
  test box.
- **Manifest** (`ceremony.json`) — the one file describing a ceremony: which
  chain, the freeze height, which pinned binaries, where things live. In a
  real event everyone gets the same manifest from the coordinator.
- **H / freeze height** — the agreed block number where the old chain stops
  accepting writes. Everything before H migrates; H is the last migrated
  block.
- **The cut** — the exact block (number *and* id) the snapshot was taken at.
  The new chain continues from the cut: same numbering, same chain_id.
- **Snapshot** — nodeos' own binary export of the full chain state (every
  account, balance, contract, table) at one block.
- **Goldens / fingerprints** — checksums of the snapshot's state, computed
  per table. Everyone's snapshot must produce the same fingerprints as the
  published "golden" values — proof we're all migrating the same state.
- **States** — the ceremony's fixed path. `ARMED`: watching the old chain,
  waiting for H. `FROZEN`: H reached/final; writes are over. `SNAPSHOTTED`:
  state exported and pinned to the cut. `VERIFIED`: snapshot hashed,
  imported twice, fingerprints match. `IGNITED`: the new engine is up,
  serving the same chain. `FLIPPED` (api modes): the public URL now points
  at the new engine. `LIVE`: done — new chain producing/serving, old node
  retired. `ABORTED`: stopped safely; the old chain is still the real one.
- **Flip** — the single user-visible action: one nginx upstream line swapped
  so the public URL answers from the new engine. Health-checked, instantly
  revertible.
- **Federator** — a small router that keeps one `/v2` history URL answering
  across the migration: old rows from the old archive, new rows from the new
  indexer.
- **Journal** — an append-only file (`journal.jsonl`) the agent writes every
  step to, with timestamps and evidence. Crash-safe: a restarted agent
  resumes from it. It's also the thing you share when something breaks.
- **R-numbers (R1, R10, R12...)** — findings from the design review
  (wiki/59). When a message cites one, it's pointing at the *reason* a rule
  exists, e.g. R10 = "the producer API can pause your chain, keep it off the
  public internet"; R12 = "a stale staged snapshot must never pin a new chain
  to an old cut".

---

## How it works (the builders' half)

Everything below is reference detail. You do not need it to rehearse.

### The state machine

```
ARMED → FROZEN → SNAPSHOTTED → VERIFIED → IGNITED → LIVE
                    (ABORTED terminal from any state; source chain stays authoritative)
```

Per-mode ceremony:

```
bp        ARMED → FROZEN → SNAPSHOTTED → VERIFIED → IGNITED → LIVE
api       ARMED → FROZEN → SNAPSHOTTED → VERIFIED → IGNITED → FLIPPED → LIVE
hyperion  ARMED → FROZEN → SNAPSHOTTED → VERIFIED → IGNITED* → FLIPPED† → LIVE
          * + hyperion-rs hydration gate   † flips /v1 and /v2 in one stage
```

- **ARMED** — preflight, watch the source chain head until freeze height `H`.
- **FROZEN** — producers paused; the cut pinned by height *and block id* after a
  quiescence window (late blocks are detected and absorbed).
- **SNAPSHOTTED** — nodeos `create_snapshot`, hard-asserted to be *of the pinned cut*.
- **VERIFIED** — streaming sha256 + the 19-table state fingerprints computed by
  importing the snapshot into **two independent fresh arenas** through the exact
  code path a PulseVM node boots with (`pulsevm_snapshot_import`); compared
  against pre-published goldens (multi-BP) or captured with provenance (rehearsal).
- **IGNITED** — verified snapshot staged into the pre-staged PulseVM chain config,
  metalgo (re)started; target must present the **source chain_id at the cut height**.
- **LIVE** — target head advances past the cut (quorum is really producing);
  only now do traffic hooks flip anything user-visible.

Every transition is an fsynced JSONL journal line with timestamps and evidence
(hashes, block ids, fingerprints, durations). A crashed agent resumes from the
journal and re-runs its (idempotent) current step.

### Commands

```sh
pulse-cutover run    --config ceremony.toml
pulse-cutover loop   --config ceremony.toml --runs N
pulse-cutover status --config ceremony.toml
pulse-cutover verify --snapshot snap.bin [--cpu-scale 143] [--golden g.txt | --capture g.txt]
pulse-cutover doctor [--json]                       # read-only environment survey + verdicts
pulse-cutover scan-contracts snap.bin [--json]      # stubbed-intrinsic exposure (advisory)
pulse-cutover report [--paranoid] [--out f.tar.gz]  # sanitized feedback bundle
```

`pulse-cutover <command> --help` prints examples for each.

### API-provider mode (`mode = "api"`)

A producer freezes the chain; an **API provider follows it**. api mode is the
ceremony for the operator whose job is `/v1` continuity:

```
ARMED → FROZEN → SNAPSHOTTED → VERIFIED → IGNITED → FLIPPED → LIVE
```

The state order is deliberately different from producer mode: **the source
nodeos outlives ignition**. Reads must never gap, so nodeos keeps answering
the public URL while PulseVM boots and verifies; the only user-visible step is
the **FLIPPED** transition (nginx upstream swap, health-checked: same
chain_id, head agreeing with the target RPC), and only after that does the
operator's own `source.stop_cmd` retire nodeos. An abort before FLIPPED
touches nothing public; an abort at FLIPPED reverts the swap — nodeos was
still running either way.

- No producer pause: the freeze is observed (`LIB ≥ H`), not caused.
- Snapshot via the node's **own** `producer_api` `create_snapshot` (works
  read-only on non-producers; keep it localhost-bound — R10).
- `simulate_freeze = true` rehearses against a live chain that will *not*
  stop: when LIB ≥ H the agent proceeds as if frozen; the journal records the
  actual cut block. In a real event H is exact because the BPs freeze — the
  agent trusts the declared H either way.

### hyperion mode (`mode = "api"` + `[hyperion]`)

/v2 history continuity rides the api ceremony: after IGNITED the agent stands up
**hyperion-rs** against the new chain's SHiP (`start_cmd` gets the ceremony's
`{first_post_cut_block}` substituted — an imported chain must index from cut+1,
never from 0), writes the **history boundary file** for the federating router,
and holds until `/v2/health` reports the indexer hydrated (with an *idle-at-cut*
allowance: a chain with zero post-cut blocks reports `Indexer: Warning,
last_indexed_block: 0` and is caught up by definition). The FLIP stage then
swaps `/v2` to the **federating router** (`federator/server.js`) in the same
user-visible moment as `/v1`, and both public gates must go green before the
source may be stopped.

The router serves ONE timeline through one URL: pre-cut rows from the legacy
Hyperion (`LEGACY=` the source chain's public archive — or your own old ES if
you kept full history; same knob), post-cut rows from local hyperion-rs, merged
and paginated across the boundary. See `federator/README.md`.

### bp mode (`mode = "producer"`)

The producer-side ceremony from Appendix A: freeze writes at the API edge,
**schedule the snapshot at exactly H** (`freeze_strategy = "schedule_at_h"` —
nodeos writes `snapshot-<block_id(H)>.bin` when H finalizes and the agent picks
it up by that exact name), pause after, quiescence-pin the cut, verify, ignite
as the new chain's producer/validator. `source.quiesce_cmd` exists for
single-node rehearsals against a live-syncing replica (sever p2p to emulate
"every producer paused"); a real multi-BP ceremony does not need it. The
burn-off audit journals every transaction between the cut and the pause head —
in a real freeze those blocks are empty and the audit proves it.

### `pulse-cutover doctor` — detect, don't assume

The 22 recorded ceremonies ran on boxes we built. Your box is not that box —
so the tooling **detects** instead of assuming. `doctor` is a strictly
read-only survey (no restarts, no writes, no flips):

```sh
pulse-cutover doctor          # human table
pulse-cutover doctor --json   # machine JSON (what install.sh consumes)
```

What it detects:

- **host** — OS/version/arch/kernel, RAM, CPU, free disk per relevant mount,
  systemd/docker presence, virtualization;
- **nodeos** — native binary or docker container (both work), the systemd
  unit(s) that exec or docker-wrap it, chain API address, version
  (`server_version_string`), chain_id + head, whether the producer_api
  answers (`/v1/producer/paused` probe: 200 = enabled, 404 = missing plugin,
  401/403 = restricted), state-history plugin on/off;
- **web edge** — nginx, apache or caddy; for nginx the full
  `server_name -> location -> proxy_pass/upstream` map from `nginx -T`
  (this is what the flip templater uses), plus TLS cert paths and expiry;
- **history stack** — legacy Hyperion under pm2 or systemd, its /v2 health;
  Elasticsearch version + heap;
- **PulseVM target** — metalgo binary/node, plugins dir, staged
  pulse-cutover services;
- **ports** — the ceremony's port plan (9650/9651/8899/80/9200/7000/7010/7019)
  vs what is actually listening, and who owns the conflict.

It ends with a per-mode verdict: **READY**, **NEEDS** (a precise list — e.g.
"producer_api_plugin (localhost-bound)"), or **UNSUPPORTED** (a precise
reason — e.g. caddy on the public edge, kube-managed nodeos) plus a pointer
at `pulse-cutover report` so unsupported setups become supported ones.

#### Supported setups matrix

| dimension | detected & handled | detected, NOT yet handled (UNSUPPORTED + explain) |
|---|---|---|
| nodeos runtime | native (systemd unit or bare pid) · docker container | kubernetes-managed |
| nodeos stop/start | manifest `stop_cmd` · derived `systemctl stop <unit>` · derived `docker stop <container>` | bare-pid nodeos in api mode without a manifest `stop_cmd` (NEEDS) |
| public edge | nginx (any layout: named upstreams, direct proxy_pass, TLS server blocks, multiple domains) · no web server (managed layout staged) | apache · caddy |
| nginx flip | templated byte-exact from the detected `server_name -> proxy_pass` map; refuses if no /v1 route points at your nodeos | hand-minified configs may degrade to fewer detected routes — doctor shows what it saw |
| history | legacy Hyperion (pm2 or systemd) noted; hyperion mode flips the detected /v2 route | no detectable /v2 route in hyperion mode (refuses with reason) |
| OS | Ubuntu 22.04 / 24.04 | anything else (UNSUPPORTED — tell us via `report`) |

### `install.sh` internals

`install.sh` runs **doctor first** and consumes its JSON:

- refuses per the doctor verdict — UNSUPPORTED prints the precise reason and
  points at `report`; NEEDS prints the exact missing list;
- templates the flip/revert scripts from the **detected** nginx map
  (domain -> upstream), byte-exact against your own config files — your
  nginx is untouched until the ceremony's flip stage. Only on a box with no
  /v1 routes at all does it stage the managed layout from the recorded runs.
  If nginx has routes but none reach your nodeos, it refuses and says so;
- defaults `source.stop_cmd`/`start_cmd` from the detected systemd unit or
  docker container when the manifest doesn't declare them;
- runs the stubbed-intrinsic scan (advisory) when a prescan snapshot is
  staged.

Beyond that it installs the agent, PulseVM plugin, metalgo, and (api mode)
the /v1 REST gateway — all **pinned + sha256-verified, fail-closed**;
extracts any tarball safely (`--no-same-owner`, staging dir); stages the
metalgo/chain configs with the manifest's values; enforces R12 (no stale
staged snapshot); and ends with an ARMED-READY print of exactly what will
happen at H. Idempotent — re-run it freely. It never touches your running
nodeos and never flips traffic.

`cutover.sh` validates the manifest against the live chain, runs the agent,
and streams each state transition in plain language. Exit 0 = LIVE; anything
else = ABORTED with the journal path (the source chain is still authoritative).
`cutover.sh status` and `cutover.sh abort` do what they say.

### The ceremony.json manifest

```json
{
  "mode": "api",
  "ceremony": { "chain_id": "…", "freeze_height": 0, "freeze_margin": 240,
                "simulate_freeze": true, "import_cpu_scale": 143 },
  "source":   { "rpc_url": "http://127.0.0.1:8888",
                "producer_api_url": "http://127.0.0.1:8888",
                "stop_cmd": "systemctl stop nodeos",
                "start_cmd": "systemctl start nodeos" },
  "target":   { "network_id": "tahoe", "subnet_id": "…", "blockchain_id": "…",
                "vm_id": "…", "producer_name": "eosio", "producer_key": "PVT_K1_…",
                "staking_dir": "/root/api-cutover/staking" },
  "flip":     { "public_host": "<public-ip-or-domain>" },
  "artifacts": { "agent":   {"url": "…", "sha256": "…"},
                 "plugin":  {"url": "…", "sha256": "…"},
                 "metalgo": {"url": "…", "sha256": "…"},
                 "gateway": {"url": "…", "sha256": "…"} },
  "paths": { "work_dir": "/root/api-cutover" }
}
```

In a real multi-operator ceremony this file is generated from the on-chain
msig declaration (freeze height, pinned versions, goldens) — see wiki/59 §A.3.

### `pulse-cutover scan-contracts` — stubbed-intrinsic preflight

Since PulseVM's arena-import branch, a contract importing an unserved host
function **loads** (the import gets a stub) but **traps if it ever calls
it**. The exposure is exactly enumerable from the snapshot:

```sh
pulse-cutover scan-contracts snapshot.bin          # at-risk table
pulse-cutover scan-contracts snapshot.bin --json   # machine-readable
```

Parses every code object's wasm import section (wasmparser) and diffs `env`
function imports against the served host-function table (169 names, embedded;
`--served file` to override). **Advisory, never a gate** — a referenced
import is a real code path but not necessarily a reachable one (the
`send_deferred` cluster on XPR testnet is the canonical example: 20+ legacy
contracts reference it, few can still reach it). The ceremony runs this scan
automatically on the **actual cut snapshot** after verification and journals
the table; declare `snapshot.prescan_path` (manifest `.snapshot.prescan_path`)
to additionally scan a staged rehearsal snapshot at ARM time.

### `pulse-cutover report` — the feedback loop

One command produces a sanitized tar.gz with everything we need to debug a
rehearsal or add support for a setup (see Step 5 above for the operator
view).

Collected: doctor JSON + table, ceremony journal(s) and loop metrics, the
staged manifest/config, the last ~200 lines of every relevant service log it
detects (nodeos — native or docker —, metalgo-pulse, pulse-gateway,
hyperion-rs units, federator), the stubbed-intrinsic scan table, agent
version.

**Sanitization is non-negotiable and always on**: private keys
(`PVT_K1_...`, `PVT_R1_...`, legacy WIF), bearer/authorization tokens,
passwords (config values, `user:pass@` URLs, ES credentials) and labeled hex
secrets are replaced with `[REDACTED-<type>]` before anything is written
into the bundle. Chain ids, block ids and sha256 digests are kept — they are
the evidence. The command ends by printing exactly what was redacted and the
full file list so you can review before sharing (`tar -tzf`). Hostnames/IPs
stay by default (so we can talk about your box); `--paranoid` placeholders
them too. The sanitizer is covered by dedicated unit tests with planted fake
secrets — that test suite is the review gate for this repo.

### Loop harness — "it works, with numbers"

```sh
pulse-cutover loop --config ceremony.toml --runs 100
```

Each iteration: `[loop].reset_cmd` returns both sides to a pre-ARM state
(source restored, fresh target chain, staged snapshot removed — R12 enforced
by preflight), then a full ceremony runs with H re-derived from live LIB
(`freeze_margin`). Failures don't stop the loop — they're data, categorized in
the summary. Output: per-run JSONL metrics (`[loop].metrics_path`) + aggregate
mean/median/p95/max for the ceremony gap and every phase duration.

The reference loop deployment (scripts + gotchas from the recorded 22-run
series) is in `examples/loop/`.

### Build

Sibling checkout convention: this repo and `pulsevm-arena-import` (branch
`feat/arena-snapshot-import` of paulgnz/pulsevm) live side by side; the
fingerprint stack is consumed as path dependencies.

```sh
cargo build --release && cargo test
```

### Design docs & trust model

The reviewed design — trust model, failure/rollback table, R-findings, and
the v2 shadow-mirror sketch — is Appendix A of `wiki/59-cutover-orchestration.md`
(pulsevm-experimental). `examples/ceremony.toml` documents the full agent
config format.

---

## Reproduce our results

The recorded numbers (22/22 api-mode loop runs LIVE, 99.8% read availability,
0.75s flip; bp-mode cut at exactly H, gap 197.0s; hyperion /v2 federation
minutes after the cut) come from rehearsals against the **live XPR testnet**
on a single Ubuntu 24.04 box. To reproduce: walk Steps 0–5 above, then
`pulse-cutover loop --runs N` with `examples/ceremony-api.toml` (the
`examples/loop/` scripts show the exact reset harness we used). Evidence
journals for the recorded runs: wiki/59 Appendices B + C.

## Testing program

We are building the operator-side confidence for a real migration event, one
rehearsal at a time — different nodeos setups, nginx layouts, history stacks.
**[TESTING.md](TESTING.md)** is the whole program: what a test run involves,
the guarantee that it never touches production, what to share and where, and
what testers get out of it.

## Status & caveats

- v0.2.0 — rehearsal-grade. The recorded ceremonies are real, on live-testnet
  state, but no *mainnet* event has run yet.
- Ubuntu 22.04/24.04 + systemd only; nginx-only traffic flips (apache/caddy
  detected and refused with reasons). `report` bundles are how new setups
  get added.
- Depends on the PulseVM arena-import branch (`paulgnz/pulsevm`,
  `feat/arena-snapshot-import`) for snapshot import + fingerprints.
- Guide + video: [pulsevm.dev/guide/migrate-antelope-chain](https://pulsevm.dev/guide/migrate-antelope-chain)
- Questions / test bundles: [Telegram](https://t.me/+N1mAvoUDbtVmNTBh) ·
  [rehearsal-feedback issues](https://github.com/paulgnz/pulse-cutover/issues/new?template=rehearsal-feedback.md)
