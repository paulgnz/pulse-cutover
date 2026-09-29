# Design: the authority boundary (commit-or-abort for the whole fleet)

**Status: design, not implemented.** This closes Astra P0 #1 (authority boundary and source fencing) and most of
P0 #3 (coordination model) once built. Parts need upstream PulseVM support (marked **upstream**).

## The problem

Today every agent decides on its own. `abort()` resumes the source chain whenever `auto_rollback` is on
([src/machine.rs:397](../src/machine.rs)), and `cutover.sh abort` resumes it unconditionally. Nothing stops one
producer from resuming the old chain while others have already started producing on PulseVM. That is two
writable histories under the **same chain_id**, which is the one outcome the whole design exists to prevent.

A rehearsal where everyone succeeds or everyone aborts together (runs 1–6) never exercises this. It only shows
up when the fleet disagrees: one BP partitioned, one BP's import fails, the coordinator crashes mid-way.

## The rule

There is exactly one point of no return per event, and it is **global**, not per node:

> A signed **COMMIT certificate** for event *E* exists → nobody may ever resume the source chain for *E*.
> No COMMIT certificate exists → nobody may produce or admit transactions on the target chain for *E*.

Everything before COMMIT is reversible. Everything after it is repair-forward only.

## Phases

```
 PREPARE (reversible)                          COMMIT (one-way)            AFTER (repair forward)
 freeze → snapshot@H → import → verify →  →  certificate published  →  target opens, source fenced for good
 each BP signs PREPARED(vote)                  (or ABORT certificate)      failures: halt + alert, never resume source
```

1. **Prepare.** As today: write-freeze, scheduled snapshot at exactly H, import, verify. The target chain boots
   **sealed**: metalgo tracks it, PulseVM loads the imported state, but builds no blocks and admits no
   transactions until it sees a COMMIT certificate (**upstream**: "sealed start" / activation gate, below).
2. **Vote.** Each required participant signs a `PREPARED` vote binding:
   `event_id, H, block_id@H, snapshot_sha256, state_commitment, plugin_sha256, roster_hash, instance_id`.
   Votes are durable in the participant's journal *before* they are sent.
3. **Decide.** The coordinator(s) sign exactly one of `COMMIT(E, votes…)` or `ABORT(E, reason)`, never both.
   COMMIT needs: votes from ≥ the governed threshold of validator weight **and** every API/history provider on
   the coverage roster reporting its write fence, all binding the same `block_id@H` / snapshot / state
   commitment. Coordinator authority is a **threshold** (e.g. 2-of-3 keys), not any single key.
4. **Act.** Agents act only on a certificate, and persist it before acting:
   - COMMIT → unseal target (**upstream**), keep the source fenced permanently, flip edges, LIVE gate.
   - ABORT → resume source, revert edges. ABORT is only valid if no COMMIT exists for E; an agent that has
     persisted COMMIT ignores any later ABORT.
   - No certificate within the prepare timeout → **stay halted and alert.** A local timeout never resumes the
     source by itself; only a signed ABORT does.

## Source fencing

At COMMIT, "paused" is not enough (a restart un-pauses nodeos). The fence must survive restarts:
- **Producers:** remove the signature provider / set `pause-on-startup` equivalent in config and restart, or
  (preferred) an on-chain action before H that retires the schedule (governance, below). Beacon check:
  `source_fenced` = producer API reports paused **and** config has no active signature provider.
- **API providers:** push/send endpoints closed at the edge and nodeos in read-only mode; beacon check verifies a
  test push is rejected.

## What each piece needs

| Piece | Owner | Notes |
|---|---|---|
| Sealed start / activation gate: chain loads imported state but builds no blocks and admits no txs until a COMMIT certificate (file or Warp message) is presented | **upstream (Glenn)** | Smallest version: a chain-config flag `activation_certificate_path` + signer set; VM refuses `BuildBlock`/`IssueTx` until a valid certificate is present. |
| Deterministic `state_commitment` at H (hash over all tables, not sampled) exposed by the importer and by the node | **upstream** | Replaces the 64-bit fingerprints and the sampled RPC state-diff as the gate (Astra P0 #5). |
| `block_id@H` recorded in imported chain metadata, queryable after boot | **upstream** | Lets every agent check target lineage at H (Astra P0 #2). |
| Vote / certificate formats, signing, persistence, threshold verification | **us** | Extends `src/coord.rs`; per-BP keys (not coordinator keys) sign votes. |
| Agent logic: act only on certificates; timeout = halt; COMMIT blocks any resume path (incl. `cutover.sh abort`) | **us** | Rewrites `abort()` semantics; journal records certificate before action. |
| Roster: required validators + weights, required API/history providers (incl. non-producers such as Greymass) | **Metallicus + us** | Published per event, hash bound into every vote. |
| Coordinator key holders and threshold; who can sign ABORT | **Metallicus governance** | Not one key on one laptop. |
| Durable, replicated relay (certificates must survive mission control restarts; agents also accept certificates from a second channel) | **us** | Mission control is a convenience relay, never the authority. |

## Failure cases this must pass (rehearsal plan)

1. One BP's import fails → no quorum → ABORT certificate → everyone resumes source. ✔ reversible.
2. Coordinator crashes after COMMIT is signed but before most agents receive it → agents stay halted (no
   resume); on recovery they fetch COMMIT from any channel and proceed.
3. One BP partitioned during vote → quorum still reached → COMMIT → partitioned BP later receives COMMIT and
   catches up on the target; it must never resume the source.
4. Operator runs `cutover.sh abort` after COMMIT → refused.
5. Agent crash/reboot at every transition → replays journal, resumes from the persisted certificate.
6. Conflicting certificates (a stolen single key signs ABORT after COMMIT) → rejected: threshold + COMMIT wins.

## Interim rule until this exists

Until sealed start and certificates exist, **a public cut must not be scheduled** (see ATOMICITY Known limits).
For rehearsals, `auto_rollback` after IGNITED is disabled (agent B, rc.5): a failure after ignition halts and
alerts instead of resuming the source.
