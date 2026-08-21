//! Ceremony configuration: everything the agent needs to run unattended.
//!
//! The config file IS the (v1, single-node) ceremony manifest. In multi-BP
//! mode the `[ceremony]` block's contents are what the consortium declares by
//! msig on the source chain; the agent's trust model for each field is
//! documented inline.

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub ceremony: Ceremony,
    pub source: Source,
    pub snapshot: Snapshot,
    pub target: Target,
    #[serde(default)]
    pub hooks: Hooks,
    /// Append-only JSONL journal — the ceremony's evidence log and the
    /// resume-after-crash source of truth.
    pub journal_path: PathBuf,
    /// Poll interval against the source chain while waiting for H.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ceremony {
    /// Freeze height H: the agent freezes as soon as head >= H. The actual
    /// cut height (>= H, exact under `schedule_at_h`, best-effort-pause under
    /// `pause_at_h`) is recorded in the journal and pinned by block id.
    pub freeze_height: u64,
    /// Expected source chain_id (hex). Everything downstream — snapshot,
    /// import, target chain — must present exactly this id. Optional only so
    /// a rehearsal against a freshly booted dev chain can discover it at ARM
    /// time (it is then journaled and enforced from there on).
    #[serde(default)]
    pub chain_id: Option<String>,
    /// Import-time CPU unit conversion (metering points per source µs).
    /// Part of the migrated chain's identity: the staged PulseVM chain config
    /// MUST carry the same value, and fingerprint goldens are only comparable
    /// across nodes using the same value.
    #[serde(default = "default_cpu_scale")]
    pub import_cpu_scale: u64,
    /// How writes stop at H (see Appendix A, review finding R1):
    /// - "pause_at_h": poll head, pause the producer the moment head >= H.
    ///   Single-producer rehearsal mode; cut height may land a block or two
    ///   past H, and finality of the cut block is by quiescence, not DPoS LIB.
    /// - "schedule_at_h": production continues (writes already frozen at the
    ///   API edge) and the snapshot is pinned to exactly H once H is
    ///   irreversible; pause happens after the snapshot lands. Multi-BP mode.
    #[serde(default)]
    pub freeze_strategy: FreezeStrategy,
    /// After pausing, require this many consecutive polls with an unchanged
    /// head before trusting the cut height (quiescence window — catches a
    /// producer that did not actually stop, or late blocks arriving on p2p).
    #[serde(default = "default_quiescence_polls")]
    pub quiescence_polls: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FreezeStrategy {
    #[default]
    PauseAtH,
    ScheduleAtH,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// nodeos chain API (get_info / get_block).
    pub rpc_url: String,
    /// nodeos producer_api (pause / paused / create_snapshot /
    /// schedule_snapshot). MUST NOT be publicly reachable — it can stop the
    /// chain. The agent talks to it over localhost.
    pub producer_api_url: String,
    /// Timeout for the create_snapshot call itself (a mainnet-scale snapshot
    /// takes minutes to write; Leap also defers the write until the block is
    /// final).
    #[serde(default = "default_snapshot_timeout")]
    pub snapshot_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Where the verified snapshot must land: the exact `snapshot_path` the
    /// staged PulseVM chain config declares. The agent copies (never moves —
    /// the original stays for forensics) the nodeos-produced file here after
    /// verification.
    pub staged_path: PathBuf,
    /// nodeos reports the snapshot path in ITS filesystem namespace; when
    /// nodeos runs in a container these two fields remap the reported prefix
    /// to the host path the agent actually reads. Identity when unset.
    #[serde(default)]
    pub path_map_from: Option<String>,
    #[serde(default)]
    pub path_map_to: Option<String>,
    /// Strict file-level golden: sha256 of the snapshot. Only comparable
    /// across nodes running the ceremony-pinned nodeos version (finding R3);
    /// the binding cross-node check is the fingerprints.
    #[serde(default)]
    pub expected_sha256: Option<String>,
    /// Golden 19-table fingerprint file to verify against (multi-BP mode:
    /// pre-published; format = golden_import_roots.txt).
    #[serde(default)]
    pub golden_roots: Option<PathBuf>,
    /// Rehearsal / first-node mode: CAPTURE the fingerprints to this path
    /// (with full provenance header) instead of comparing against a
    /// pre-published set. Mutually exclusive with `golden_roots`.
    #[serde(default)]
    pub capture_roots: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    /// systemd unit that runs the (pre-staged, pre-bootstrapped) metalgo
    /// instance tracking the target subnet. Ignition = restart of this unit
    /// with the verified snapshot in place.
    pub metalgo_unit: String,
    /// Override for ignition (rehearsals/tests); default is
    /// `systemctl restart <metalgo_unit>`.
    #[serde(default)]
    pub ignite_cmd: Option<String>,
    /// The target chain's pulsevm JSON-RPC endpoint (…/ext/bc/<chainID>/rpc).
    pub rpc_url: String,
    /// LIVE gate: require head to advance this many blocks past the cut
    /// height before declaring LIVE (and before any traffic flip hook runs).
    #[serde(default = "default_live_blocks")]
    pub live_blocks: u64,
    /// How long to wait for the target chain to respond and reach the LIVE
    /// gate before declaring the ignition failed (partial-quorum limbo,
    /// finding R5). On expiry the agent ABORTs and (if `auto_rollback`)
    /// resumes the source producer — the source chain stays authoritative.
    #[serde(default = "default_quorum_timeout")]
    pub quorum_timeout_secs: u64,
    /// On abort, automatically resume the source chain producer.
    #[serde(default = "default_true")]
    pub auto_rollback: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Hooks {
    /// Runs at freeze time, BEFORE the snapshot is requested: this is the
    /// write-freeze (review finding R2) — flip tx acceptance off / make the
    /// gateway reject writes. Block production keeps running (finding R1:
    /// the snapshot block can only finalize if blocks keep coming), so the
    /// blocks between the cut and the pause stay empty instead of carrying
    /// writes that would be silently discarded.
    #[serde(default)]
    pub on_freeze: Option<String>,
    /// Runs once after IGNITED (e.g. repoint the traffic generator / warm the
    /// gateway). Failures are journaled but non-fatal: the LIVE gate decides.
    #[serde(default)]
    pub post_ignite: Option<String>,
    /// Runs once after LIVE (e.g. flip gateway upstreams / DNS). This is the
    /// ONLY user-visible commitment in the ceremony (finding R5 ratchet
    /// rule); everything before it is abortable.
    #[serde(default)]
    pub on_live: Option<String>,
    /// Runs on ABORT after auto-rollback (e.g. re-open writes at the
    /// gateway, page the operator).
    #[serde(default)]
    pub on_abort: Option<String>,
}

fn default_poll_ms() -> u64 {
    500
}
fn default_cpu_scale() -> u64 {
    1
}
fn default_quiescence_polls() -> u32 {
    6
}
fn default_snapshot_timeout() -> u64 {
    600
}
fn default_live_blocks() -> u64 {
    1
}
fn default_quorum_timeout() -> u64 {
    600
}
fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read config {}: {e}", path.display()))?;
        let config: Config =
            toml::from_str(&text).map_err(|e| format!("parse config {}: {e}", path.display()))?;
        if config.snapshot.golden_roots.is_some() && config.snapshot.capture_roots.is_some() {
            return Err("snapshot.golden_roots and snapshot.capture_roots are mutually exclusive: \
                        a node either verifies against published goldens or captures its own"
                .into());
        }
        if config.snapshot.path_map_from.is_some() != config.snapshot.path_map_to.is_some() {
            return Err("snapshot.path_map_from and path_map_to must be set together".into());
        }
        Ok(config)
    }

    /// Remap a nodeos-reported snapshot path into the agent's filesystem view.
    pub fn map_snapshot_path(&self, reported: &str) -> PathBuf {
        match (&self.snapshot.path_map_from, &self.snapshot.path_map_to) {
            (Some(from), Some(to)) if reported.starts_with(from.as_str()) => {
                PathBuf::from(format!("{to}{}", &reported[from.len()..]))
            }
            _ => PathBuf::from(reported),
        }
    }
}
