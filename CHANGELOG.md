# Changelog

Release notes per tag. Earlier releases are described in their tag messages and in
[docs/EVIDENCE.md](docs/EVIDENCE.md).

## v0.5.0-rc.32 (2026-10-16)

From the 8-BP rehearsal (fleet-step3, runs s1, s1b, s1c) and its review.

**Agent**

- **Producer admission lock** (`ceremony.admission_lock`, default on; turning it off fails the production profile in
  producer mode). At the freeze the agent journals the producer's actor whitelist and sets it to `pulse.frozen1`, a
  name no account can have, through `/v1/producer/set_whitelist_blacklist`. The producer then drops every queued
  transaction whatever its expiry, refuses new ones at its API and never relays them; deferred transactions stay
  scheduled. Run s1 aborted all 8 BPs because 3 transfers queued before the freeze landed at H+25. The original list is
  restored when a rollback reopens writes. The lock is re-read on every poll tick from the lock to the pause and re-applied
  within one tick (a nodeos restart drops it: `admission_lock_lost`, with the height), again right before the pause, and re-read before the burn-off audit; ARM refuses a producer API without
  `get_whitelist_blacklist`. The burn-off audit stays the backstop and names the producer of each offending block.
  Residuals: producers not running the lock, actions with an empty authorization set, and senders in
  `sender-bypass-whiteblacklist` (docs/DESIGN.md §2a, RISK-REGISTER A-22).
- Source RPCs from the freeze to the pause and through the snapshot wait, the pause itself and the producer-paused
  reads around it, retry transient errors (connection refused
  or reset, timeout, HTTP 5xx) with backoff for `source.rpc_retry_secs` (default 180 s, at most half of
  `snapshot_timeout_secs`), journaled as `source_rpc_retry` / `source_rpc_recovered`. Past the bound the ceremony takes
  the abort path instead of exiting; the lock is never lifted while the source cannot be read, and a strand re-applies
  it. A resumed agent in
  FROZEN re-checks the lock first. Fleet run r3: a 1.8 s nodeos restart killed the agent in FROZEN.
- From the pause until chain creation (quiescence, verification, VERIFIED, the fleet gate) every tick checks that the
  source is still paused and locked and re-pauses / re-locks it at once (`source_unpaused`, `admission_lock_lost`,
  with the height); the burn-off re-check re-applies the lock. A head past the pause head still refuses creation.
  Fleet run r3b: a restart 234 ms after the pause left bp3 producing 14 blocks unpaused and unlocked.
- `await` resumes the ceremony it started when it is restarted with that ceremony unfinished (ARMED through VERIFIED, or
  STRANDED; journaled `await_resume`); before, its own "ran" tombstone made a restarted `await` idle. It forwards
  SIGTERM / SIGINT to the running ceremony and waits for its safe stop.
- `examples/pulse-cutover-await.service`: `Restart=on-failure`, `KillMode=mixed`, `TimeoutStopSec=900`, designed exits
  not restarted.
- One retry deadline per poll tick (`source.rpc_retry_secs`), so several retried calls in one tick cannot overrun the
  quiescence, snapshot and fleet deadlines; the fleet gate checks a signed abort before its seal check. HTTP 500 with a
  nodeos exception body is not retried. A STRANDED re-run or operator-rollback strand re-applies the lock. The upstream
  tools' poll re-pauses with the same episode bookkeeping as the agent's own pause.
- Operator docs: a SPLIT stays after `coord.mjs complete` while the old chain keeps advancing, until the next event.
- `coordination.fleet_timeout_secs` defaults to 900 s (was 300). Run s1b aborted at 300 s while the slowest box was
  still verifying, 5.5 min behind the fastest. A warning (on config load, journaled at ARM) fires when the timeout is
  under 1.5 x this producer's drill-measured conversion time.
- The beacon reports an `admission_lock` setup check.
- `rust-version = "1.89"` is declared (`Option::is_none_or` needs 1.82; the journal lock's `File::try_lock` already needs 1.89).

**Mission control**

- The relay records durably, per event, the first time the fleet went forward (LIVE / STALLED, or a quorum of fresh
  past-creation reports). A later signed abort is refused 409 on that record even after the reports go stale or the
  relay restarts (run s1c relayed one with an ALARM). `--after-creation yes` still relays it explicitly.
- "ALARM QUORUM UNREACHABLE" is in `fleet.alarms` (was `fleet.warnings`), is not raised once every member aborted, and a
  relayed signed abort resolves it. Scripts that read it from `fleet.warnings` must read `fleet.alarms`.

**Tests**: `tests/admission.rs` (mempool and expiry harness), model invariant I8, `control/test/rc32.test.mjs`.
