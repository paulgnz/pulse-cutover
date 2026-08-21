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
