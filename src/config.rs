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
    /// api mode only: how the public /v1 URL is swapped from nodeos to the
    /// PulseVM gateway, and how the swap is health-checked.
    #[serde(default)]
    pub flip: Option<Flip>,
    /// hyperion mode = api mode + this section: /v2 history continuity.
    /// hyperion-rs is stood up against the new chain's SHiP after IGNITED,
    /// hydration is gated on its /v2/health, and the FLIP stage additionally
    /// swaps the public /v2 to the federating history router (pre-cut rows
    /// keep coming from the legacy Hyperion, post-cut from hyperion-rs —
    /// "your endpoint keeps its memory").
    #[serde(default)]
    pub hyperion: Option<Hyperion>,
    /// The official (#61) import pipeline — required when
    /// `ceremony.import_backend = "upstream"`.
    #[serde(default)]
    pub upstream: Option<Upstream>,
    /// loop-harness settings (`pulse-cutover loop`).
    #[serde(default)]
    pub r#loop: Option<LoopCfg>,
    #[serde(default)]
    pub hooks: Hooks,
    /// Mission control reporting (`pulse-cutover beacon`): readiness checks and
    /// ceremony evidence posted to a central status board. Optional; the
    /// ceremony never depends on it.
    #[serde(default)]
    pub beacon: Option<Beacon>,
    /// Coordinated arming (`pulse-cutover await`) and the pre-ignite fleet gate.
    #[serde(default)]
    pub coordination: Option<Coordination>,
    /// Append-only JSONL journal — the ceremony's evidence log and the
    /// resume-after-crash source of truth.
    pub journal_path: PathBuf,
    /// Poll interval against the source chain while waiting for H.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Beacon {
    /// Mission control report endpoint, e.g. `https://control.example/api/report`.
    /// Empty + `--once` prints the report instead of sending it.
    #[serde(default)]
    pub url: String,
    /// This producer's account name as it appears in the producer schedule.
    pub producer: String,
    /// Network id as mission control knows it: "mainnet", "testnet", "rehearsal"…
    pub network: String,
    /// This machine's name on the board (default: its hostname). One producer usually runs several
    /// nodes (producer, API, history); each reports separately so a missing one is visible.
    #[serde(default)]
    pub node: Option<String>,
    /// What this node does for the producer: "producer", "api", "history", "seed" (default: "producer"
    /// in producer mode, otherwise "api").
    #[serde(default)]
    pub role: Option<String>,
    /// File holding this producer's bearer token (one line, mode 600). Never
    /// put the token itself in the config.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    #[serde(default = "default_beacon_interval")]
    pub interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coordination {
    /// Mission control base URL (relays signed coordinator messages; trusts nothing itself).
    pub url: String,
    /// Network id, as in mission control's networks.json.
    pub network: String,
    /// ed25519 public keys (hex) whose signatures this node accepts. Empty = trust nobody.
    #[serde(default)]
    pub coordinator_keys: Vec<String>,
    /// false (default): a signed ARM also needs the local file `confirm-<event_id>` next to the journal.
    #[serde(default)]
    pub auto_arm: bool,
    /// Reject events whose H is fewer than this many blocks ahead of head.
    #[serde(default = "default_min_lead")]
    pub min_lead_blocks: u64,
    /// Before igniting, wait until this many producers (including this one) report VERIFIED with
    /// the same snapshot hash and fingerprints on mission control. 0 = no fleet gate.
    #[serde(default)]
    pub fleet_quorum: usize,
    #[serde(default = "default_fleet_timeout")]
    pub fleet_timeout_secs: u64,
    /// Set by `await` in the derived config: the event this ceremony belongs to (enables signed aborts).
    #[serde(default)]
    pub event_id: Option<String>,
    /// Set by `await` from the signed event: the servers that MUST agree before ignition.
    /// When non-empty the fleet gate counts only these members (matched by beacon
    /// `instance_id` when given, else by producer account), only with FRESH reports, and
    /// ignores everything else the relay says. Empty = legacy quorum-of-anyone gate.
    #[serde(default)]
    pub roster: Vec<RosterMember>,
    /// Max age of a roster member's beacon report for the fleet gate to count it.
    #[serde(default = "default_report_max_age")]
    pub report_max_age_secs: u64,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RosterMember {
    pub producer: String,
    /// The beacon's persisted instance id (`beacon.instance`). Optional: without it any
    /// fresh report from that producer's servers counts.
    #[serde(default)]
    pub instance_id: Option<String>,
}

/// A roster must name each member once: a duplicate would let one report count twice toward
/// the quorum. Two entries for one producer are fine only with distinct instance ids.
pub fn check_roster(roster: &[RosterMember]) -> Result<(), String> {
    for (i, m) in roster.iter().enumerate() {
        for other in &roster[i + 1..] {
            // Same producer twice is a duplicate unless BOTH name distinct instances.
            let dup = m.producer == other.producer
                && match (&m.instance_id, &other.instance_id) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                };
            if dup {
                return Err(format!("roster lists {} more than once (each member may count once toward the quorum)", m.producer));
            }
        }
    }
    Ok(())
}

fn default_report_max_age() -> u64 {
    60
}

fn default_min_lead() -> u64 {
    120
}
fn default_fleet_timeout() -> u64 {
    300
}

fn default_beacon_interval() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ceremony {
    /// What this config file is FOR:
    /// - "ceremony" (default): a real ceremony config; `run`/`loop`/`await` accept it.
    /// - "readiness": a beacon-only config (what `beacon-install.sh` writes). `beacon`,
    ///   `doctor` and `status` work; every ceremony-mutating command (`run`, `loop`,
    ///   `await`) REFUSES it before touching anything. Ceremony-only validation (flip
    ///   section, stop_cmd, H) is skipped, so a readiness file never needs placeholders
    ///   that could accidentally drive a cut.
    #[serde(default)]
    pub profile: Profile,
    /// Who this agent is in the ceremony:
    /// - "producer": the node PRODUCES the source chain — freeze = stop
    ///   writes, pause production (order per R1/R2). The rehearsed v1 path.
    /// - "api": the node is an API provider — it does not produce and cannot
    ///   freeze the chain; it observes the (declared or simulated) freeze,
    ///   snapshots via its own producer_api (read-only create_snapshot works
    ///   on non-producers), and the user-visible commitment is the /v1 URL
    ///   flip (state FLIPPED). The source nodeos keeps serving reads until
    ///   AFTER the flip is healthy — reads never gap — and is stopped via
    ///   `source.stop_cmd` only then.
    #[serde(default)]
    pub mode: Mode,
    /// Freeze height H: the agent freezes as soon as head >= H (producer
    /// mode) or LIB >= H (api mode — snapshot at finality, R1). 0 = derive
    /// from `freeze_margin` at ARM time (journaled as `resolved_h`).
    #[serde(default)]
    pub freeze_height: u64,
    /// If `freeze_height = 0`: H := (source LIB at ARM) + freeze_margin.
    /// This is the loop-harness / simulated-freeze mode of declaring H; in a
    /// real ceremony H is exact because the BPs freeze at it — api mode
    /// trusts the declared H either way.
    #[serde(default)]
    pub freeze_margin: Option<u64>,
    /// Rehearsal/loop only: allow `freeze_height = 0` + `freeze_margin` to DERIVE H at ARM
    /// (H := LIB-at-ARM + margin). Without this explicit opt-in a config with no H is
    /// rejected: a real ceremony's H comes from the signed coordinator event (`await`
    /// writes it into the derived config) or is typed in, never guessed.
    #[serde(default)]
    pub derive_h_at_arm: bool,
    /// Rehearsal only: accept a cut that is NOT exactly H (pause_at_h landing at H+1 on a
    /// single-producer stand-in). Default false: every mode must snapshot exactly H, and a
    /// snapshot at any other height aborts. `simulate_freeze` implies this (a live chain that
    /// will not stop cannot be cut at an exact height by an API node) and is journaled as such.
    #[serde(default)]
    pub allow_inexact_cut: bool,
    /// api mode against a LIVE source chain that will NOT actually freeze
    /// (rehearsals on the real testnet): when LIB >= H the agent PROCEEDS as
    /// if frozen — the snapshot lands at ~finality near H and the journal
    /// records the actual cut block. In a real event the BPs stop at H and
    /// this flag is false (the observed head simply stops moving at H).
    #[serde(default)]
    pub simulate_freeze: bool,
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
    /// How writes stop at H (see docs/DESIGN.md, review finding R1):
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
    /// Upper bound on the quiescence wait. A producer that keeps producing after
    /// the pause (pause ignored, another producer still on our schedule, a
    /// replica still syncing) would otherwise hold the ceremony in FROZEN, with
    /// writes closed, forever. On expiry the ceremony aborts (before ignition:
    /// the on_abort hook reopens writes on the old chain). Must be > 0.
    #[serde(default = "default_quiescence_timeout")]
    pub quiescence_timeout_secs: u64,
    /// `schedule_at_h` only: freeze writes this many blocks BEFORE H. The
    /// cut stays exactly H; the lead lets transactions already accepted at
    /// an API edge or in flight on p2p land at or before H instead of in
    /// H+1.. (where the burn-off audit would abort the ceremony — observed
    /// in the 5-producer rehearsal: freezing at head >= H left 3 in-flight
    /// transfers in H+1 on every producer). 24 blocks = 12s at 0.5s blocks.
    #[serde(default = "default_freeze_lead_blocks")]
    pub freeze_lead_blocks: u64,
    /// Which import stack turns the cut snapshot into PulseVM state:
    /// - "fork": our arena-snapshot-import branch reads the Leap `.bin`
    ///   directly and the target chain boots via `snapshot_path`. This is
    ///   the interim/bridge implementation, and it stays the DEFAULT only
    ///   until MetalBlockchain/pulsevm#61 merges — once the official path
    ///   ships, the default flips to "upstream" and the fork path retires.
    /// - "upstream": the core team's official migration path (#61):
    ///   export.sh replays the `.bin` through a pinned Leap into a SHiP
    ///   full-state log, `xpr_import_check` hydrates it into an Arena
    ///   checkpoint, and verification uses upstream's OWN tools
    ///   (`xpr_19_table_compare` + `xpr_state_fingerprint`). Requires the
    ///   `[upstream]` section. IGNITED from the checkpoint is pending the
    ///   #61 merge (the ceremony stops after VERIFIED with a precise
    ///   explanation of what remains).
    #[serde(default)]
    pub import_backend: ImportBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    #[default]
    Ceremony,
    Readiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImportBackend {
    #[default]
    Fork,
    Upstream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FreezeStrategy {
    #[default]
    PauseAtH,
    ScheduleAtH,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Producer,
    Api,
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
    /// api mode: the operator's own stop script for the source nodeos
    /// (arbitrary command — `./stop.sh`, `docker stop nodeos`, `systemctl
    /// stop nodeos`, …). Runs ONLY after FLIPPED health-checks green: the
    /// nodeos must outlive the flip so reads never gap.
    #[serde(default)]
    pub stop_cmd: Option<String>,
    /// api mode, optional: how to bring the source nodeos BACK if the
    /// ceremony aborts after stop_cmd already ran (defense in depth; the
    /// only abort left after stop is a post-stop public health failure).
    #[serde(default)]
    pub start_cmd: Option<String>,
    /// producer mode, optional: runs immediately after the producer pause,
    /// before the quiescence window. In a REAL multi-BP ceremony every
    /// producer pauses, so no new blocks arrive and quiescence passes on its
    /// own. A single-node stand-in rehearsing the producer ceremony against
    /// a live-syncing replica must emulate "everyone paused" by also
    /// severing p2p — this hook is where that happens. Journaled; a failure
    /// aborts (an un-quiesced head would otherwise poison the cut pin).
    #[serde(default)]
    pub quiesce_cmd: Option<String>,
}

/// api mode: the /v1 URL swap. `cmd` performs the swap (e.g. rewrite the
/// nginx upstream + reload); the agent then polls `public_url` until it
/// serves the TARGET chain (same chain_id — that's the whole point — so the
/// discriminator is head agreement with the target RPC within
/// `head_tolerance` blocks, `health_polls` consecutive times).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Flip {
    pub cmd: String,
    /// The PUBLIC endpoint clients actually use (through nginx), e.g.
    /// "http://<public-ip>" — the agent appends /v1/chain/get_info.
    pub public_url: String,
    /// Optional revert command (swap nginx back to nodeos) run on abort if
    /// the flip already happened.
    #[serde(default)]
    pub revert_cmd: Option<String>,
    #[serde(default = "default_health_polls")]
    pub health_polls: u32,
    #[serde(default = "default_head_tolerance")]
    pub head_tolerance: u64,
    #[serde(default = "default_flip_timeout")]
    pub health_timeout_secs: u64,
}

/// `pulse-cutover loop`: run the ceremony N times against resettable source
/// and target, aggregating per-run metrics. Requires `freeze_margin` (H is
/// re-derived per run) and a `reset_cmd` that returns both sides to a
/// pre-ARM state (fresh target chain dir, staged snapshot removed — R12).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopCfg {
    /// Shell command that resets the world for the next iteration.
    pub reset_cmd: String,
    /// Seconds to wait after reset before ARMing (services settling).
    #[serde(default)]
    pub settle_secs: u64,
    /// Where per-run metrics JSONL lines are appended.
    pub metrics_path: PathBuf,
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
    /// Advisory stubbed-intrinsic preflight: a pre-staged/rehearsal snapshot
    /// (NOT the ceremony's own cut — that one is scanned automatically after
    /// verification) to run `scan-contracts` against at ARM time, so the
    /// at-risk contract table is in the journal before anything freezes.
    /// Never a gate: unserved imports are journaled and the ceremony
    /// continues (referenced != reachable).
    #[serde(default)]
    pub prescan_path: Option<PathBuf>,
    /// Host directory where nodeos writes its snapshots (`schedule_at_h`
    /// only): the scheduled snapshot lands here as
    /// `snapshot-<block_id_at_H>.bin` once H is irreversible, and the agent
    /// picks it up by that exact name — the schedule pins the cut to H, the
    /// filename pins it to H's block id.
    #[serde(default)]
    pub dir: Option<PathBuf>,
    /// Upstream alignment (MetalBlockchain/pulsevm#61): optional path to an
    /// `xpr_state_fingerprint`-compatible binary — upstream's canonical
    /// verification tool (whole-state root + per-table SHA-256 report). When
    /// set and the binary exists, it runs at verify time ALONGSIDE (never
    /// replacing) the 19-table fingerprint check, and its report is journaled
    /// next to ours. When unset — or set but absent on this box — the step is
    /// a journaled no-op: #61 is not merged yet, so absence is the normal
    /// case. Advisory, never a gate; once the tool ships upstream it becomes
    /// the preferred cross-implementation check.
    #[serde(default)]
    pub upstream_fingerprint_bin: Option<PathBuf>,
    /// Arguments for the upstream fingerprint binary. `{snapshot}` expands to
    /// the verified snapshot file, `{staged}` to `staged_path`. Defaults to
    /// `["{snapshot}"]`; set explicitly to match the merged tool's final CLI
    /// (currently `<checkpoint> <arena-directory>` on the #61 branch).
    #[serde(default)]
    pub upstream_fingerprint_args: Option<Vec<String>>,
}

/// The official import pipeline from MetalBlockchain/pulsevm#61 (unmerged;
/// fetch `pull/61/head` and `cargo build --release --examples -p
/// pulsevm_database` for the tools). The ceremony drives it for the
/// SNAPSHOTTED -> VERIFIED stages when `ceremony.import_backend = "upstream"`:
///
///   export_cmd (export.sh: pinned Leap replays the cut `.bin` into a SHiP
///   chain_state_history.log) -> import_bin (xpr_import_check: SHiP ->
///   Arena checkpoint + manifest) -> compare_bin (xpr_19_table_compare:
///   wire-level nodeos-vs-Arena 19-table comparison, THE verification gate)
///   + fingerprint_bin (xpr_state_fingerprint: whole-state root, journaled,
///   golden-comparable across nodes).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// Scratch root for export/import artifacts. The agent creates
    /// `export-<cut_height>/` (the export.sh work dir lands under it),
    /// `checkpoint-<cut_height>.bin`, and per-tool arena directories here.
    pub work_dir: PathBuf,
    /// Runs the official export.sh flow against the cut snapshot.
    /// Placeholders: `{snapshot}` = host path of the cut `.bin`,
    /// `{export_dir}` = a fresh directory the agent names (the command may
    /// nest export.sh's own `--work-dir` under it — the agent finds
    /// `chain_state_history.log` and `manifest.env` anywhere below).
    /// Typical (dockerized Leap, the validated shape):
    ///   docker run --rm -v <work_dir>:/w nodeos:5.0.3 bash /w/export.sh
    ///     --nodeos /usr/local/bin/nodeos --snapshot /w/<cut>.bin
    ///     --work-dir /w/export-<h>/work --chain-state-db-size-mb 4096
    /// (export.sh's `rg` calls need the rg->grep patch on images without
    /// ripgrep — see README "Import backends".)
    pub export_cmd: String,
    /// xpr_import_check binary: `<chain_state_history.log> <arena-dir>
    /// [checkpoint]` — writes the Arena checkpoint + `.manifest.json`.
    pub import_bin: PathBuf,
    /// xpr_state_fingerprint binary: `<checkpoint> <arena-dir>` — prints
    /// `revision` / `state_root` / per-table sha256 lines (journaled; the
    /// cross-node golden once multiple operators run the same cut).
    pub fingerprint_bin: PathBuf,
    /// xpr_19_table_compare binary: `<log> <checkpoint> <arena-dir>
    /// <chain-id-hex> [report.json]`. When set this is a hard gate: a
    /// non-zero exit (any table mismatch between nodeos's SHiP snapshot and
    /// Arena's re-serialization) fails verification.
    #[serde(default)]
    pub compare_bin: Option<PathBuf>,
    /// Multi-node mode: the published golden `state_root` this node must
    /// reproduce (from another operator's journal / the coordinator).
    #[serde(default)]
    pub golden_state_root: Option<String>,
    /// Dev/audit only, default OFF: additionally run our fork importer's
    /// dual-arena 19-table fingerprint over the same cut `.bin` and journal
    /// it. This is a release-validation tool, not an operator step, and it
    /// is NEVER a gate — the one-time published cross-check against #61
    /// already established equivalence; the ceremony verifies with the
    /// official tools above.
    #[serde(default)]
    pub fork_audit: bool,
}

/// hyperion mode (api mode + `[hyperion]`): /v2 history continuity across
/// the cut. Post-IGNITED, hyperion-rs indexes the new chain from its SHiP;
/// once hydrated, the flip stage swaps the public /v2 to the federating
/// router, which merges pre-cut history (legacy Hyperion — either the public
/// source-chain archive, or the operator's own old ES) with post-cut history
/// (local hyperion-rs) at the ceremony's cut block.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hyperion {
    /// Stands up hyperion-rs (indexer + api) against the new chain. Runs
    /// once, right after IGNITED. Optional: leave unset if the services are
    /// already running (they will simply start indexing when SHiP appears).
    #[serde(default)]
    pub start_cmd: Option<String>,
    /// Local hyperion-rs /v2/health. The hydration gate requires every
    /// service OK and the Indexer caught up (last_indexed_block against the
    /// RPC head / the cut) before the flip may proceed.
    pub health_url: String,
    #[serde(default = "default_hydration_timeout")]
    pub hydration_timeout_secs: u64,
    /// Indexer lag (blocks) tolerated by the hydration gate.
    #[serde(default)]
    pub max_lag_blocks: u64,
    /// Where the agent writes the ceremony boundary facts for the federating
    /// router ({cut_block, cut_time, cut_block_id, chain_id}): the router
    /// re-reads this file, so the cut discovered mid-ceremony configures the
    /// history boundary without a restart.
    #[serde(default)]
    pub boundary_path: Option<PathBuf>,
    /// Swaps the public /v2 upstream to the federating router. Runs in the
    /// flip stage, right after the /v1 flip command.
    pub flip_cmd: String,
    /// Reverts the /v2 swap on abort (mirror of flip.revert_cmd).
    #[serde(default)]
    pub revert_cmd: Option<String>,
    /// PUBLIC /v2/health through the flipped edge. The FLIPPED gate requires
    /// it to answer with `federation.local.ok == true` (the router is live
    /// AND its post-cut source is the hydrated hyperion-rs).
    #[serde(default)]
    pub public_health_url: Option<String>,
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
    /// On abort, automatically resume the source chain producer. Only ever applies BEFORE
    /// IGNITED: once this node's target is ignited a local failure seals (halts and alerts)
    /// instead — resuming the source then could create a second writable history.
    #[serde(default = "default_true")]
    pub auto_rollback: bool,
    /// LIVE gate, sustained: after head passes `cut + live_blocks`, keep watching for this
    /// many seconds; the chain must keep producing (no gap longer than `live_max_gap_secs`).
    /// 0 disables (single-block smoke runs).
    #[serde(default = "default_live_sustain")]
    pub live_sustain_secs: u64,
    #[serde(default = "default_live_max_gap")]
    pub live_max_gap_secs: u64,
    /// Require the target's block id AT the cut height to equal the source's (lineage), not
    /// just height + chain_id. Default true; if the target RPC cannot show block H the
    /// ceremony aborts before IGNITED. false = journaled as unverified (rehearsals only).
    #[serde(default = "default_true")]
    pub require_lineage_check: bool,
    /// Optional: the installed PulseVM plugin binary. When set and a coordinated event pins a
    /// `release_sha256`, `await` rejects the event unless this file hashes to it.
    #[serde(default)]
    pub plugin_path: Option<PathBuf>,
    /// Local fence for a forced rollback after ignition (`rollback --force-after-ignite`): must
    /// stop THIS box's target and succeed only if it is no longer running. It runs BEFORE the
    /// source is resumed; if it fails the source is not resumed. Default:
    /// `systemctl stop <metalgo_unit> && ! systemctl is-active --quiet <metalgo_unit>`.
    /// A local fence only: other producers' targets are not affected.
    #[serde(default)]
    pub stop_cmd: Option<String>,
}

impl Target {
    /// The command that fences this box's target (see `stop_cmd`).
    pub fn fence_cmd(&self) -> String {
        self.stop_cmd.clone().unwrap_or_else(|| {
            format!(
                "systemctl stop {u} && ! systemctl is-active --quiet {u}",
                u = self.metalgo_unit
            )
        })
    }
}

fn default_live_sustain() -> u64 {
    60
}
fn default_live_max_gap() -> u64 {
    20
}

#[derive(Debug, Clone, Deserialize)]
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
    /// Runs once after IGNITED (e.g. send the first transactions: PulseVM builds
    /// blocks on demand). Required: a failure halts the ceremony (ignition has started).
    #[serde(default)]
    pub post_ignite: Option<String>,
    /// Runs once the LIVE gate has passed, BEFORE LIVE is journaled (e.g. flip the
    /// edge to PulseVM and reopen writes). In producer mode this is the user-visible
    /// commitment (finding R5 ratchet rule). Required: a failure halts the ceremony.
    #[serde(default)]
    pub on_live: Option<String>,
    /// Runs on ABORT after auto-rollback (e.g. re-open writes at the
    /// gateway, page the operator).
    #[serde(default)]
    pub on_abort: Option<String>,
    /// Runs when the ceremony HALTS after IGNITED (sealed: the source is NOT resumed and
    /// writes are NOT re-opened). Use it to page humans — it must not undo anything.
    #[serde(default)]
    pub on_halt: Option<String>,
    /// Every hook (and flip/stop/start command) is killed after this many seconds and
    /// treated as failed. Required hooks failing abort before IGNITED and halt after it.
    #[serde(default = "default_hook_timeout")]
    pub timeout_secs: u64,
}

fn default_hook_timeout() -> u64 {
    300
}

impl Default for Hooks {
    fn default() -> Self {
        Hooks { on_freeze: None, post_ignite: None, on_live: None, on_abort: None, on_halt: None, timeout_secs: default_hook_timeout() }
    }
}

fn default_poll_ms() -> u64 {
    500
}
fn default_health_polls() -> u32 {
    4
}
fn default_head_tolerance() -> u64 {
    8
}
fn default_flip_timeout() -> u64 {
    120
}
fn default_cpu_scale() -> u64 {
    1
}
fn default_freeze_lead_blocks() -> u64 {
    24
}
fn default_quiescence_polls() -> u32 {
    6
}
fn default_quiescence_timeout() -> u64 {
    120
}
fn default_snapshot_timeout() -> u64 {
    600
}
fn default_hydration_timeout() -> u64 {
    900
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
        if config.ceremony.quiescence_timeout_secs == 0 {
            return Err("ceremony.quiescence_timeout_secs must be > 0 (0 would abort every ceremony at the pause)".into());
        }
        if config.snapshot.golden_roots.is_some() && config.snapshot.capture_roots.is_some() {
            return Err("snapshot.golden_roots and snapshot.capture_roots are mutually exclusive: \
                        a node either verifies against published goldens or captures its own"
                .into());
        }
        if config.snapshot.path_map_from.is_some() != config.snapshot.path_map_to.is_some() {
            return Err("snapshot.path_map_from and path_map_to must be set together".into());
        }
        if let Some(b) = &config.beacon {
            check_beacon_url(&b.url)?;
        }
        if let Some(co) = &config.coordination {
            check_roster(&co.roster)?;
            if !co.roster.is_empty() {
                // A roster with quorum 0 used to skip the fleet gate entirely (no event id): an
                // explicit quorum is required, and it must be reachable.
                if co.fleet_quorum == 0 {
                    return Err(format!(
                        "[coordination] has a roster of {} but fleet_quorum = 0: set fleet_quorum explicitly \
                         (1..={}); a roster without a quorum would skip the fleet gate",
                        co.roster.len(), co.roster.len()));
                }
                if co.fleet_quorum > co.roster.len() {
                    return Err(format!("[coordination] fleet_quorum {} exceeds the roster of {}: it can never be met",
                        co.fleet_quorum, co.roster.len()));
                }
            }
        }
        if config.ceremony.profile == Profile::Readiness {
            // Beacon/doctor/status only: none of the ceremony requirements apply, and every
            // mutating command refuses this file (see `ensure_ceremony_profile`).
            return Ok(config);
        }
        // An `await` config legitimately has no H yet: the signed coordinator event supplies it
        // (await writes the derived config with that H). `run`/`loop` refuse it via ensure_h_known.
        let awaiting_event = config.ceremony.freeze_height == 0
            && config.ceremony.freeze_margin.is_none()
            && config.coordination.is_some();
        if config.ceremony.freeze_height == 0 && !awaiting_event {
            if config.ceremony.freeze_margin.is_none() {
                return Err("ceremony.freeze_height is 0: set the event's H (or use `await`, which \
                            takes H from the signed coordinator event)"
                    .into());
            }
            if !config.ceremony.derive_h_at_arm {
                return Err("ceremony.freeze_height = 0 with freeze_margin would DERIVE H at arm time. \
                            That is a rehearsal/loop feature only: set derive_h_at_arm = true to opt \
                            in explicitly, or set freeze_height to the event's H"
                    .into());
            }
        }
        if config.ceremony.mode == Mode::Api
            && !config.ceremony.simulate_freeze
            && config.snapshot.dir.is_none()
        {
            return Err("api mode (without simulate_freeze) cuts at exactly H via schedule_snapshot and \
                        needs snapshot.dir (where nodeos writes snapshot-<block_id_at_H>.bin)"
                .into());
        }
        if config.ceremony.mode == Mode::Api {
            if config.flip.is_none() {
                return Err("api mode requires a [flip] section (cmd + public_url): the \
                            /v1 URL swap IS the user-visible cutover"
                    .into());
            }
            if config.source.stop_cmd.is_none() {
                return Err("api mode requires source.stop_cmd (the operator's own nodeos \
                            stop script; runs only after FLIPPED is healthy)"
                    .into());
            }
        }
        if config.ceremony.simulate_freeze && config.ceremony.mode != Mode::Api {
            return Err("simulate_freeze is only meaningful in api mode (a producer freezes \
                        the chain for real)"
                .into());
        }
        if config.hyperion.is_some() && config.ceremony.mode != Mode::Api {
            return Err("[hyperion] composes with api mode (mode = \"api\"): /v2 continuity \
                        is an API-provider concern — the flip is the user-visible act"
                .into());
        }
        if config.ceremony.import_backend == ImportBackend::Upstream && config.upstream.is_none() {
            return Err("ceremony.import_backend = \"upstream\" requires an [upstream] section \
                        (work_dir, export_cmd, import_bin, fingerprint_bin — the official #61 \
                        pipeline the ceremony drives)"
                .into());
        }
        if config.ceremony.freeze_strategy == FreezeStrategy::ScheduleAtH
            && config.ceremony.mode == Mode::Producer
            && config.snapshot.dir.is_none()
        {
            return Err("freeze_strategy = \"schedule_at_h\" requires snapshot.dir (the host \
                        directory where the scheduled snapshot-<block_id>.bin lands)"
                .into());
        }
        Ok(config)
    }

    /// Mutating commands (`run`, `loop`, `await`) call this first: a readiness-only config
    /// can never drive a ceremony, whatever else it contains.
    /// `run`/`loop`: the cut height must be known (explicit H, or the rehearsal derive opt-in).
    /// A coordination config with freeze_height = 0 is for `await` only.
    pub fn ensure_h_known(&self) -> Result<(), String> {
        if self.ceremony.freeze_height == 0 && !(self.ceremony.derive_h_at_arm && self.ceremony.freeze_margin.is_some()) {
            return Err("ceremony.freeze_height is 0: this config waits for a coordinator event. Run `pulse-cutover await` with it (H comes from the signed event), or set freeze_height"
                .into());
        }
        Ok(())
    }

    pub fn ensure_ceremony_profile(&self) -> Result<(), String> {
        if self.ceremony.profile == Profile::Readiness {
            return Err("this config is a READINESS-ONLY profile ([ceremony] profile = \"readiness\", \
                        written by beacon-install.sh): it reports readiness to mission control and \
                        cannot run a ceremony. Use the ceremony config staged for the event."
                .into());
        }
        Ok(())
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

/// The beacon sends a bearer token with every report: only over HTTPS, except to this machine.
/// Parsed with a real URL parser: userinfo is rejected outright (`http://localhost:80@example.org`
/// addresses example.org), and http is allowed only when the PARSED host is exactly
/// localhost / 127.0.0.1 / ::1 (not `localhost.example.org`, not `127.0.0.1.nip.io`).
pub fn check_beacon_url(raw: &str) -> Result<(), String> {
    if raw.is_empty() {
        return Ok(());
    }
    let u = url::Url::parse(raw).map_err(|e| format!("beacon.url {raw:?} is not a valid URL: {e}"))?;
    if !u.username().is_empty() || u.password().is_some() {
        return Err(format!("beacon.url {raw:?} contains userinfo (user@host): refused, the real destination would be ambiguous"));
    }
    let host_ok = match u.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback() && ip == std::net::Ipv4Addr::LOCALHOST,
        Some(url::Host::Ipv6(ip)) => ip == std::net::Ipv6Addr::LOCALHOST,
        None => false,
    };
    match u.scheme() {
        "https" if u.host().is_some() => Ok(()),
        "http" if host_ok => Ok(()),
        "http" => Err(format!(
            "beacon.url {raw:?} is plain http to a remote host: the bearer token would travel \
             unencrypted. Use https:// (http is allowed only to localhost)"
        )),
        other => Err(format!("beacon.url {raw:?}: scheme {other:?} is not allowed (https only)")),
    }
}
