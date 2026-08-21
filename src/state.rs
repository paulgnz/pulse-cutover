//! The ceremony state machine's states.
//!
//! ARMED -> FROZEN -> SNAPSHOTTED -> VERIFIED -> IGNITED -> LIVE
//! with ABORTED reachable from every non-terminal state. Transitions only
//! move forward; a crash resumes IN the last journaled state and re-runs that
//! state's (idempotent) step.

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
            "LIVE" => State::Live,
            "ABORTED" => State::Aborted,
            other => return Err(format!("unknown state in journal: {other}")),
        })
    }
}
