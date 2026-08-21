//! The ceremony state machine's states.
//!
//! Producer mode: ARMED -> FROZEN -> SNAPSHOTTED -> VERIFIED -> IGNITED -> LIVE
//! API mode:      ARMED -> FROZEN -> SNAPSHOTTED -> VERIFIED -> IGNITED -> FLIPPED -> LIVE
//! with ABORTED reachable from every non-terminal state. Transitions only
//! move forward; a crash resumes IN the last journaled state and re-runs that
//! state's (idempotent) step.
//!
//! FLIPPED exists only in api mode: the public /v1 URL has been swapped to the
//! PulseVM gateway and health-checked; the source nodeos is stopped AFTER this
//! state (reads must never gap — nodeos outlives ignition, unlike producer
//! mode where the source producer pauses before the snapshot finalizes).

use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Config validated, preflight passed, watching the source chain head.
    Armed,
    /// Writes are stopped and the cut block is pinned (height + block id +
    /// quiescence confirmed).
    Frozen,
    /// nodeos produced the snapshot file for the cut block.
    Snapshotted,
    /// sha256 recorded/compared and the 19-table fingerprints dual-imported
    /// and verified (against goldens, or captured with provenance).
    Verified,
    /// Verified snapshot staged; target metalgo (re)started; target RPC
    /// answered with the source chain_id at the cut height.
    Ignited,
    /// (api mode only) The public /v1 endpoint has been swapped to the
    /// PulseVM side and health-checked; the source nodeos is still serving
    /// nothing (traffic already left it) but has NOT yet been stopped.
    Flipped,
    /// Target head advanced past the cut height (quorum is producing).
    Live,
    /// Terminal failure; source chain remains authoritative (auto-rollback
    /// resumes the source producer if configured).
    Aborted,
}

impl State {
    pub fn as_str(&self) -> &'static str {
        match self {
            State::Armed => "ARMED",
            State::Frozen => "FROZEN",
            State::Snapshotted => "SNAPSHOTTED",
            State::Verified => "VERIFIED",
            State::Ignited => "IGNITED",
            State::Flipped => "FLIPPED",
            State::Live => "LIVE",
            State::Aborted => "ABORTED",
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for State {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "ARMED" => State::Armed,
            "FROZEN" => State::Frozen,
            "SNAPSHOTTED" => State::Snapshotted,
            "VERIFIED" => State::Verified,
            "IGNITED" => State::Ignited,
            "FLIPPED" => State::Flipped,
            "LIVE" => State::Live,
            "ABORTED" => State::Aborted,
            other => return Err(format!("unknown state in journal: {other}")),
        })
    }
}
