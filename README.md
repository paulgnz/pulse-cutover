# pulse-cutover

Programmatic, zero-read-downtime Antelope → PulseVM cutover ceremony agent.

One binary drives the whole ceremony unattended and records everything:

```
ARMED → FROZEN → SNAPSHOTTED → VERIFIED → IGNITED → LIVE
                    (ABORTED terminal from any state; source chain stays authoritative)
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

## Usage

```sh
pulse-cutover run    --config ceremony.toml
pulse-cutover status --config ceremony.toml
pulse-cutover verify --snapshot snap.bin [--cpu-scale 143] [--golden g.txt | --capture g.txt]
```

See `examples/ceremony.toml` for the config format, and Appendix A of
`wiki/59-cutover-orchestration.md` (pulsevm-experimental) for the reviewed
design: trust model, failure/rollback table, and the v2 shadow-mirror sketch.

## Build

Sibling checkout convention: this repo and `pulsevm-arena-import` (branch
`feat/arena-snapshot-import` of paulgnz/pulsevm) live side by side; the
fingerprint stack is consumed as path dependencies.

```sh
cargo build --release && cargo test
```

## API-provider mode (`mode = "api"`)

A producer freezes the chain; an **API provider follows it**. api mode is the
ceremony for the operator whose job is `/v1` continuity — nodeos serving the
public URL → PulseVM serving the *same URL*, with **zero read downtime**:

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

## Two-command operator story

For a BP or API provider who has never seen PulseVM:

```sh
# now (any time before the ceremony):
./install.sh --mode api --manifest ceremony.json     # or --mode bp

# at H (announced freeze):
./cutover.sh --manifest ceremony.json
```

`install.sh` refuses politely (with reasons) if the box isn't ready; installs
the agent, PulseVM plugin, metalgo, and (api mode) the /v1 REST gateway — all
**pinned + sha256-verified, fail-closed**; extracts any tarball safely
(`--no-same-owner`, staging dir); stages the metalgo/chain/nginx configs with
the manifest's values; enforces R12 (no stale staged snapshot); and ends with
an ARMED-READY print of exactly what will happen at H. Idempotent — re-run it
freely. It never touches your running nodeos and never flips traffic.

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

## Loop harness — "it works, with numbers"

```sh
pulse-cutover loop --config ceremony.toml --runs 100
```

Each iteration: `[loop].reset_cmd` returns both sides to a pre-ARM state
(source restored, fresh target chain, staged snapshot removed — R12 enforced
by preflight), then a full ceremony runs with H re-derived from live LIB
(`freeze_margin`). Failures don't stop the loop — they're data, categorized in
the summary. Output: per-run JSONL metrics (`[loop].metrics_path`) + aggregate
mean/median/p95/max for the ceremony gap and every phase duration.
