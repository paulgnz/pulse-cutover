//! The cutover ceremony state machine.
//!
//! Forward-only: ARMED -> FROZEN -> SNAPSHOTTED -> VERIFIED -> IGNITED ->
//! LIVE, ABORTED terminal from anywhere before ignition may have started, HALTED (sealed,
//! durable) after it. Each state's step is idempotent so a
//! crashed agent resumes from the journal and simply re-runs the step it died
//! in. Every transition carries evidence (heights, block ids, hashes,
//! durations) into the journal.

use serde_json::json;

use crate::{
    config::{
        Config,
        FreezeStrategy,
        ImportBackend,
        Mode,
    },
    journal::{
        Journal,
        Recovered,
    },
    ops::ChainOps,
    scan,
    state::State,
    upstream,
    verify,
};

pub struct Machine<'a, O: ChainOps> {
    pub cfg: &'a Config,
    pub ops: &'a O,
    pub journal: Journal,
    pub state: State,
    resumed: bool,
    // Ceremony facts (journal-recovered on resume).
    pub chain_id: Option<String>,
    /// H as resolved at ARM (explicit freeze_height or LIB + freeze_margin).
    pub resolved_h: Option<u64>,
    pub cut_height: Option<u64>,
    pub cut_block_id: Option<String>,
    pub snapshot_file: Option<String>,
    pub sha256: Option<String>,
    pub frozen_ts_ms: Option<u64>,
    pub last_source_block_time: Option<String>,
    /// api mode rollback bookkeeping: did the flip command already run?
    flip_ran: bool,
    /// hyperion rollback bookkeeping: did the /v2 flip command already run?
    hyperion_flip_ran: bool,
    /// api mode rollback bookkeeping: did source.stop_cmd already run?
    source_stopped: bool,
    /// producer mode: did schedule_snapshot(H) succeed at ARM? (If not, the
    /// FROZEN step falls back to an immediate create_snapshot.)
    scheduled: bool,
    /// Last time the coordinator's abort signal was polled (rate limit).
    last_coord_check_ms: u64,
    /// Ignition may have started on this node (`ignite_started` journaled BEFORE the ignite
    /// command runs, or IGNITED or later, now or in a previous run). From here on a local
    /// failure SEALS (HALTED + alert) instead of resuming the source.
    reached_ignited: bool,
    /// Identity of the snapshot this ceremony staged (journaled `staged_artifact`).
    staged_sha256: Option<String>,
    /// An operator cleared a previous HALT: continue forward instead of halting on resume.
    unhalted: bool,
    /// The journal ends in an ABORTED whose rollback provably finished (`rollback_done`).
    aborted_rollback_complete: bool,
    /// Rollback steps already journaled as done in this abort episode (resumable rollback).
    rollback_steps_done: Vec<String>,
    rollback_pending: bool,
    /// Upstream backend: the Metal blockchain id create_chain_cmd created (journaled at once).
    target_blockchain_id: Option<String>,
    target_subnet_id: Option<String>,
    /// Upstream backend: create_chain_cmd was started (journaled before it ran).
    create_chain_started: bool,
    /// REHEARSAL ONLY: the different target chain_id `rehearsal_allow_chain_id_change` accepted.
    accepted_target_chain_id: Option<String>,
    /// Upstream backend: boot artifact hashes journaled at VERIFIED (manifest, genesis, config).
    boot_hashes: (Option<String>, Option<String>, Option<String>),
    /// rc.23: this ceremony is joining a target chain other roster members created (`join`): no
    /// fleet gate, no chain creation, no abort check before ignition (the fleet already committed);
    /// past the point of no return (a failure halts).
    joining: bool,
    head_at_pause: Option<u64>,
    /// rc.23: the post-ignition degraded wait (`degraded_continue`): when the first symptom was seen
    /// (patience counts from there, across symptoms), the fleet's highest reported target head and
    /// when it last moved, and the last journaled degraded note (journaled on change only).
    degraded_since_ms: Option<u64>,
    fleet_head: (u64, u64),
    /// When the fleet view first stopped showing a quorum during a degraded wait (None = it shows one).
    fleet_unhealthy_since: Option<u64>,
    degraded_note: String,
}

/// What `pulse-cutover rollback` did. `failed` lists every step that did not succeed; the
/// command exits 4 ("rollback attempted, incomplete") unless it is empty.
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackOutcome {
    pub state: State,
    pub failed: Vec<String>,
    pub notes: Vec<String>,
    /// The journal already ended in a complete rollback: nothing was done again.
    pub already: bool,
}

/// Hydration predicate over a hyperion-rs /v2/health document. Two ways in:
///
/// 1. Every service OK and the Indexer caught up (last_indexed_block within
///    `max_lag` of the RPC head, or at/past the cut).
/// 2. **Idle at the cut**: the chain presents head <= cut and nothing has
///    streamed yet — hyperion-rs then reports `Indexer: Warning` with
///    `last_indexed_block: 0` (observed live on an idle imported chain: an
///    all-OK gate deadlocks). With zero post-cut blocks the indexer is
///    caught up by definition; every NON-indexer service must still be OK.
///    Honest caveat: this arm cannot distinguish "nothing to index" from a
///    broken SHiP connection — the post-LIVE continuity proof (first write
///    appearing in /v2) is the definitive check, and a real migration has
///    traffic immediately.
pub fn hyperion_hydrated(health: &serde_json::Value, cut: u64, max_lag: u64) -> Option<serde_json::Value> {
    let services = health.get("health")?.as_array()?;
    if services.is_empty() {
        return None;
    }
    let status_ok = |s: &&serde_json::Value| s.get("status").and_then(|v| v.as_str()) == Some("OK");
    let all_ok = services.iter().all(|s| status_ok(&s));
    let non_indexer_ok = services
        .iter()
        .filter(|s| s.get("service").and_then(|v| v.as_str()) != Some("Indexer"))
        .all(|s| status_ok(&s));
    let field = |service: &str, key: &str| -> Option<u64> {
        services
            .iter()
            .find(|s| s.get("service").and_then(|v| v.as_str()) == Some(service))
            .and_then(|s| s.get("service_data")?.get(key)?.as_u64())
    };
    let last_indexed = field("Indexer", "last_indexed_block")?;
    let rpc_head = field("PulseVM-RPC", "head_block_num").unwrap_or(cut);
    let caught_up = all_ok && (last_indexed + max_lag >= rpc_head || last_indexed >= cut);
    let idle_at_cut = non_indexer_ok && rpc_head <= cut && last_indexed == 0;
    (caught_up || idle_at_cut).then(|| {
        json!({
            "last_indexed_block": last_indexed,
            "rpc_head": rpc_head,
            "cut_height": cut,
            "idle_at_cut": idle_at_cut && !caught_up,
        })
    })
}

/// What one poll of the coordinator's relay says about this event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortPoll {
    /// A valid signed abort for this event.
    Aborted,
    /// The relay answered and carries no abort for this event.
    Clear,
    /// Not asked (rate limit) or the relay did not answer: NOT evidence of "no abort".
    Unknown,
}

/// `Machine::coordinator_aborted` without borrowing the machine (the upstream pipeline polls it
/// while its progress callback holds the journal). `last` is the rate-limit clock.
fn coordinator_abort_poll<O: ChainOps>(cfg: &Config, ops: &O, last: &mut u64, force: bool) -> bool {
    coordinator_abort_state(cfg, ops, last, force) == AbortPoll::Aborted
}

fn coordinator_abort_state<O: ChainOps>(cfg: &Config, ops: &O, last: &mut u64, force: bool) -> AbortPoll {
    let Some(co) = cfg.coordination.as_ref() else { return AbortPoll::Clear };
    let Some(id) = co.event_id.as_deref() else { return AbortPoll::Clear };
    // An abort this node already saw is final, whatever the relay serves now (await tombstones).
    if crate::coord::is_tombstoned(cfg, id) {
        return AbortPoll::Aborted;
    }
    let now = ops.now_ms();
    if !force && now.saturating_sub(*last) < 3000 {
        return AbortPoll::Unknown;
    }
    *last = now;
    let url = format!("{}/api/coord/{}", co.url.trim_end_matches('/'), co.network);
    match ops.get_json(&url) {
        Ok(Some(doc)) if doc.is_object() => {
            if crate::coord::aborted(&doc, &co.coordinator_keys, &co.network, id) {
                crate::coord::record_tombstone(cfg, id, "aborted", "signed abort seen by the ceremony");
                AbortPoll::Aborted
            } else {
                AbortPoll::Clear
            }
        }
        _ => AbortPoll::Unknown,
    }
}

/// The target chain ids from the last journaled `join` record (blockchain_id, subnet_id).
fn journaled_join_ids(path: &std::path::Path) -> Option<(String, Option<String>)> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines().rev().filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find_map(|v| {
            let j = &v["data"]["join"];
            j["blockchain_id"].as_str().map(|b| (b.to_string(), j["subnet_id"].as_str().map(str::to_string)))
        })
}

/// What the upstream verification produced (SNAPSHOTTED→VERIFIED and `join --reverify` share it, rc.25 F5).
struct UpstreamVerified {
    sha256: String,
    file_size: u64,
    outcome: upstream::UpstreamOutcome,
    boot: serde_json::Value,
    boot_hashes: (Option<String>, Option<String>, Option<String>),
    table_compare: String,
    cut_height: u64,
    cut_block_id: String,
    chain_id: String,
    started: u64,
    /// rc.27: the new chain's protocol upgrade schedule (validated against the cut).
    protocol: upstream::ProtocolSchedule,
}

impl UpstreamVerified {
    /// The evidence the fleet compares (the beacon's snapshot_sha256 / fingerprints_digest / boot_genesis_sha256).
    fn fleet_evidence(&self) -> serde_json::Value {
        let fingerprints = self.outcome.state_root.as_ref().map(|r| json!({"upstream_state_root": r}));
        json!({"snapshot_sha256": self.sha256, "boot_genesis_sha256": self.boot_hashes.1,
            "fingerprints_digest": fingerprints.map(|f| crate::beacon::fingerprints_digest(&f)),
            "protocol_schedule_hash": self.protocol.hash})
    }
}

impl<'a, O: ChainOps> Machine<'a, O> {
    pub fn new(cfg: &'a Config, ops: &'a O, journal: Journal, recovered: Recovered) -> Self {
        let resumed = recovered.state.is_some();
        let state = recovered.state.unwrap_or(State::Armed);
        // A resumed agent must assume any side effect it journaled as STARTED may have applied
        // (the flip runs inside IGNITED, before FLIPPED is journaled).
        let started = |name: &str| recovered.side_effects.iter().any(|s| s == name);
        let flip_ran = matches!(state, State::Flipped | State::Live) || started("flip_cmd");
        let hyperion_flip_ran = matches!(state, State::Flipped | State::Live) || started("hyperion_flip_cmd");
        let source_stopped = started("source_stop_cmd");
        let scheduled = recovered.scheduled;
        let reached_ignited = recovered.reached_ignited;
        let staged_sha256 = recovered.staged_sha256.clone();
        let unhalted = recovered.unhalted;
        let aborted_rollback_complete = recovered.aborted_rollback_complete;
        let rollback_steps_done = recovered.rollback_steps_done.clone();
        let rollback_pending = recovered.rollback_pending;
        let joining = recovered.side_effects.iter().any(|s| s == "join");
        let create_chain_started = recovered.side_effects.iter().any(|s| s == "create_chain") || joining;
        Machine {
            cfg,
            ops,
            journal,
            state,
            resumed,
            chain_id: recovered.chain_id.or_else(|| cfg.ceremony.chain_id.clone()),
            resolved_h: recovered.resolved_h.or(if cfg.ceremony.freeze_height > 0 {
                Some(cfg.ceremony.freeze_height)
            } else {
                None
            }),
            cut_height: recovered.cut_height,
            cut_block_id: recovered.cut_block_id,
            snapshot_file: recovered.snapshot_file,
            sha256: recovered.sha256,
            frozen_ts_ms: recovered.frozen_ts_ms,
            last_source_block_time: recovered.last_source_block_time,
            flip_ran,
            hyperion_flip_ran,
            source_stopped,
            scheduled,
            last_coord_check_ms: 0,
            reached_ignited,
            staged_sha256,
            unhalted,
            aborted_rollback_complete,
            rollback_steps_done,
            rollback_pending,
            target_blockchain_id: recovered.target_blockchain_id,
            target_subnet_id: recovered.target_subnet_id,
            create_chain_started,
            accepted_target_chain_id: recovered.accepted_target_chain_id,
            boot_hashes: (recovered.boot_manifest_sha256, recovered.boot_genesis_sha256, recovered.boot_chain_config_sha256),
            joining,
            head_at_pause: recovered.head_at_pause,
            degraded_since_ms: recovered.degraded_since_ms,
            fleet_head: (0, 0),
            fleet_unhealthy_since: None,
            degraded_note: String::new(),
        }
    }

    fn h(&self) -> u64 {
        self.resolved_h
            .expect("H resolved at ARM (freeze_height or LIB + freeze_margin)")
    }

    /// Drive the ceremony to a terminal state. Returns the terminal state.
    pub fn run(&mut self) -> Result<State, String> {
        // Defense in depth (main.rs checks too): a readiness-only config never drives anything.
        self.cfg.ensure_ceremony_profile()?;
        if self.rollback_pending {
            return Err("refusing to run: an operator `pulse-cutover rollback` was started on this journal and \
                 did not finish (it recorded `rollback_requested` but never reached ABORTED). The operator \
                 asked to go back, not to carry on: re-run `pulse-cutover rollback` to finish it."
                .into());
        }
        // A previous agent that died mid-hook (or mid pipeline step) left that process group
        // running: stop it before this run does anything, or it would race the resumed step (e.g.
        // a still-running on_freeze beside on_abort). Refuse to resume if it cannot be stopped.
        match self.ops.kill_orphan_hooks() {
            Ok(None) => {}
            Ok(Some(desc)) => {
                eprintln!("{desc}");
                self.journal.evidence(self.state, json!({"orphan_hook_killed": desc, "at": "run resume"}))?;
            }
            Err(e) => {
                self.journal.evidence(self.state, json!({"orphan_hook_kill_error": e, "resume_blocked": true}))?;
                return Err(format!(
                    "refusing to resume: a hook left running by the previous agent could not be stopped ({e}). \
                     Stop it by hand (see {}), then re-run.",
                    crate::ops::hook_pgid_path(self.journal.path()).display()
                ));
            }
        }
        if self.target_blockchain_id.is_some() {
            // Resumed after create_chain: the target RPC / ignite placeholders need the id again.
            self.bind_target_vars();
        }
        if self.state == State::Halted {
            return Err(format!(
                "HALTED (journaled): this ceremony was sealed after ignition may have started. It will \
                 not continue or roll back on its own. After a fleet-wide decision, an operator clears \
                 it with `pulse-cutover unhalt --config <file> --i-understand` (journaled). Journal: {}",
                self.journal.path().display()
            ));
        }
        if self.state == State::Stranded {
            return Err(self.stranded_message());
        }
        if self.resumed && self.reached_ignited && !self.unhalted && matches!(self.state, State::Armed | State::Frozen | State::Snapshotted | State::Verified) {
            // The previous run journaled `ignite_started` and died before IGNITED: the target may
            // be running. Continuing (re-igniting) or rolling back are both unsafe to decide locally.
            self.halt(
                "resumed after ignition may have started (ignite_started journaled, no IGNITED)",
                json!({"recovered_state": self.state.as_str()}),
            )?;
        }
        if self.resumed && self.create_chain_started && self.target_blockchain_id.is_none() && !self.unhalted
            && matches!(self.state, State::Armed | State::Frozen | State::Snapshotted | State::Verified) {
            // create_chain_cmd started and no blockchain id was journaled: a chain may exist (and be
            // starting on validators that already track the subnet). Neither creating another nor
            // resuming the source can be decided locally.
            self.halt(
                "resumed after create_chain_cmd started without a journaled blockchain id: a target chain may exist",
                json!({"recovered_state": self.state.as_str(),
                       "fix": "find the chain on the P-Chain by its genesis hash; a fleet-wide decision either journals its id and continues, or retires it and rolls back with --force-after-ignite"}),
            )?;
        }
        if self.resumed && !matches!(self.state, State::Live | State::Aborted | State::Stranded) {
            // A recovered run gets the same invariant checks as a fresh one (minus the ones
            // that are legitimately stale mid-ceremony, like "H is in the future").
            let info = self.ops.source_info();
            match info {
                Ok(info) => self.preflight(&info, true)?,
                Err(e) if self.reached_ignited || matches!(self.state, State::Ignited | State::Flipped) => {
                    // The source may be stopped by design after the flip; that is not a failure.
                    self.journal.evidence(self.state, json!({"resume_preflight": "source unreachable (expected after IGNITED)", "error": e}))?;
                }
                Err(e) => {
                    self.abort("resume preflight: source chain unreachable", json!({"error": e}))?;
                    return Ok(self.state);
                }
            }
            if self.state == State::Aborted {
                return Ok(self.state);
            }
        }
        if !self.resumed {
            // Fresh ceremony: journal the arming evidence once.
            let info = self.ops.source_info()?;
            self.chain_id = Some(info.chain_id.clone());
            // Resolve H: explicit, or LIB-at-ARM + margin (simulated freeze /
            // loop harness — in a real event H is exact because BPs freeze).
            let resolved_h = if self.cfg.ceremony.freeze_height > 0 {
                self.cfg.ceremony.freeze_height
            } else {
                info.last_irreversible_block_num
                    + self.cfg.ceremony.freeze_margin.unwrap_or(0)
            };
            self.resolved_h = Some(resolved_h);
            self.journal.transition(
                State::Armed,
                json!({
                    "mode": format!("{:?}", self.cfg.ceremony.mode),
                    "freeze_height": self.cfg.ceremony.freeze_height,
                    "resolved_h": resolved_h,
                    "simulate_freeze": self.cfg.ceremony.simulate_freeze,
                    "chain_id": info.chain_id,
                    "head_at_arm": info.head_block_num,
                    "lib_at_arm": info.last_irreversible_block_num,
                    "freeze_strategy": format!("{:?}", self.cfg.ceremony.freeze_strategy),
                    "import_cpu_scale": self.cfg.ceremony.import_cpu_scale,
                    // The upstream backend has no import CPU scale (PulseVM v1.0.0): the value is
                    // journaled for agreement with the event, never as if it took effect.
                    "import_cpu_scale_effective": self.cfg.ceremony.import_backend != ImportBackend::Upstream,
                    "import_backend": format!("{:?}", self.cfg.ceremony.import_backend),
                    "rehearsal_overrides": self.cfg.rehearsal_overrides(),
                    "rehearsal": self.cfg.ceremony.rehearsal,
                    "production_profile": if self.cfg.ceremony.rehearsal { json!(self.cfg.production_problems()) } else { json!("met") },
                }),
            )?;
            for w in self.cfg.warnings() {
                self.journal.evidence(State::Armed, json!({"config_warning": w}))?;
                eprintln!("WARNING: {w}");
            }
            if self.cfg.ceremony.rehearsal {
                self.journal.evidence(State::Armed, json!({"REHEARSAL": true, "relaxations": self.cfg.production_problems(),
                    "note": "REHEARSAL ceremony (ceremony.rehearsal = true): not a valid cutover"}))?;
                eprintln!("WARNING: REHEARSAL ceremony; relaxations: {}", self.cfg.production_problems().join("; "));
            }
            if !self.cfg.rehearsal_overrides().is_empty() {
                self.journal.evidence(State::Armed, json!({"REHEARSAL_OVERRIDES_ACTIVE": self.cfg.rehearsal_overrides(),
                    "note": "REHEARSAL ONLY: gates a real cut depends on are relaxed; this ceremony is not a valid cutover"}))?;
                eprintln!("WARNING: rehearsal overrides active: {}", self.cfg.rehearsal_overrides().join("; "));
            }
            self.preflight(&info, false)?;
        }
        loop {
            match self.state {
                State::Armed => self.step_armed()?,
                State::Frozen => self.step_frozen()?,
                State::Snapshotted => self.step_snapshotted()?,
                State::Verified => self.step_verified()?,
                State::Ignited => self.step_ignited()?,
                State::Flipped => self.step_flipped()?,
                State::Live | State::Aborted => return Ok(self.state),
                State::Halted => return Err(format!("HALTED at {}", self.state)),
                State::Stranded => return Err(self.stranded_message()),
            }
        }
    }

    fn preflight(&mut self, info: &crate::ops::ChainInfo, resumed: bool) -> Result<(), String> {
        let mut problems = Vec::new();
        let h = self.h();
        let pre_verify = matches!(self.state, State::Armed | State::Frozen | State::Snapshotted);
        // "H is in the future" means different things per mode: a producer
        // freezes at head >= H, an api node proceeds at LIB >= H (H is a
        // FINALITY target there — on a live DPoS chain head runs ~2*21*6
        // blocks ahead of LIB, and head being past H is normal).
        let reference = match self.cfg.ceremony.mode {
            Mode::Producer => info.head_block_num,
            Mode::Api => info.last_irreversible_block_num,
        };
        if !resumed && reference >= h {
            problems.push(format!(
                "freeze height {} is not in the future ({} {})",
                h,
                if self.cfg.ceremony.mode == Mode::Api { "lib" } else { "head" },
                reference
            ));
        }
        if let Some(expected) = &self.cfg.ceremony.chain_id {
            if expected != &info.chain_id {
                problems.push(format!(
                    "source chain_id {} != ceremony chain_id {expected}",
                    info.chain_id
                ));
            }
        }
        // Rehearsal overrides are refused at config load for a configured mainnet chain_id; a
        // config that leaves chain_id to discovery is caught here, before anything freezes.
        if crate::config::is_xpr_mainnet(&info.chain_id) && (!self.cfg.rehearsal_overrides().is_empty() || self.cfg.ceremony.rehearsal) {
            let mut what = self.cfg.rehearsal_overrides();
            if self.cfg.ceremony.rehearsal {
                what.push("ceremony.rehearsal = true".into());
            }
            problems.push(format!("the source is XPR MAINNET and rehearsal overrides are active ({}): refused", what.join("; ")));
        }
        match self.ops.producer_paused() {
            // api mode: this node does not produce; we only need producer_api
            // reachable for create_snapshot — its paused flag is irrelevant.
            Ok(paused) => {
                if !resumed && paused && self.cfg.ceremony.mode == Mode::Producer {
                    problems.push("producer already paused at arm time".into());
                }
            }
            Err(e) if !(resumed && self.reached_ignited) => problems.push(format!("producer_api unreachable: {e}")),
            Err(_) => {}
        }
        // api mode: the public URL must be serving the SOURCE chain now —
        // proves the nginx -> nodeos path the ceremony will flip actually
        // works before anything is committed.
        if let Some(flip) = self.cfg.flip.as_ref().filter(|_| !self.flip_ran && !self.reached_ignited) {
            match self.ops.public_info(&flip.public_url) {
                Ok(Some(pubinfo)) if pubinfo.chain_id == info.chain_id => {}
                Ok(Some(pubinfo)) => problems.push(format!(
                    "public_url serves chain_id {} != source {}",
                    pubinfo.chain_id, info.chain_id
                )),
                Ok(None) | Err(_) => problems.push(format!(
                    "public_url {} not serving get_info at ARM time",
                    flip.public_url
                )),
            }
        }
        // rc.27: the new chain's protocol upgrade schedule must be valid and keep every activation clear of the
        // cut (checked again at verification, where its hash becomes evidence).
        if let Some(up) = self.cfg.upstream.as_ref().filter(|_| pre_verify && self.cfg.ceremony.import_backend == ImportBackend::Upstream) {
            match upstream::ProtocolSchedule::load(up.protocol_upgrades_file.as_deref())
                .and_then(|s| s.check_against_cut(h, up.protocol_upgrade_margin_blocks)) {
                Ok(()) => {}
                Err(e) => problems.push(format!("protocol upgrade schedule: {e}")),
            }
        }
        // Producer signing key vs the key the source registers (stage-2 run 1 halt). Checked
        // again before ignition, against the state at the cut.
        let mut producer_key_ok = None;
        if !(resumed && self.reached_ignited) {
            match self.producer_key_check() {
                Some(Ok(ev)) => producer_key_ok = Some(ev),
                Some(Err(problem)) => problems.push(problem),
                None => {}
            }
        }
        if let Some(dir) = self.cfg.snapshot.staged_path.parent() {
            if !dir.exists() {
                problems.push(format!("staged_path dir {} missing", dir.display()));
            }
        }
        if !(resumed && self.reached_ignited) {
            problems.extend(self.signer_material_problems());
        }
        for p in [self.cfg.ceremony.boundary_path.as_ref(), self.cfg.hyperion.as_ref().and_then(|h| h.boundary_path.as_ref())].into_iter().flatten() {
            if p.parent().is_some_and(|d| !d.as_os_str().is_empty() && !d.is_dir()) {
                problems.push(format!("history boundary_path {}: its directory does not exist", p.display()));
            }
        }
        // Stage-path hygiene (R12): the pre-staged target imports whatever sits
        // at snapshot_path the moment its chain first initializes. A stale file
        // from an earlier ceremony pins the chain to the WRONG cut before this
        // ceremony even freezes — the file must not exist until VERIFIED stages
        // the verified one.
        let staged_is_ours = resumed
            && self.cfg.snapshot.staged_path.exists()
            && self.staged_sha256.as_deref().is_some_and(|want| {
                verify::sha256_file(&self.cfg.snapshot.staged_path)
                    .map(|(got, _)| got.eq_ignore_ascii_case(want))
                    .unwrap_or(false)
            });
        if pre_verify && resumed && self.cfg.snapshot.staged_path.exists() && self.staged_sha256.is_some() && !staged_is_ours {
            // This ceremony journaled a staging intent and died mid-copy: the file is our own
            // partial artifact (the target only imports it at ignition, which has not started).
            // It is re-staged when the SNAPSHOTTED step re-runs.
            self.journal.evidence(self.state, json!({"resume_preflight_staged": "partial copy of this ceremony's artifact; will be re-staged"}))?;
        } else if staged_is_ours {
            self.journal.evidence(self.state, json!({"resume_preflight_staged": "matches this ceremony's journaled staged_artifact"}))?;
        } else if pre_verify && self.cfg.snapshot.staged_path.exists() {
            problems.push(format!(
                "staged_path {} already exists — stale snapshot from a previous ceremony? \
                 the target would import it prematurely; remove it (and re-create the target \
                 chain if it already initialized from it)",
                self.cfg.snapshot.staged_path.display()
            ));
        }
        if let Some(g) = &self.cfg.snapshot.golden_roots {
            if !g.exists() {
                problems.push(format!("golden_roots {} missing", g.display()));
            }
        }
        if let (true, Some(ev)) = (problems.is_empty(), producer_key_ok) {
            self.journal.evidence(self.state, json!({"producer_key_check": ev}))?;
        }
        if problems.is_empty() && resumed {
            self.journal.evidence(self.state, json!({"resume_preflight": "ok"}))?;
            return Ok(());
        }
        if problems.is_empty() {
            self.journal
                .evidence(State::Armed, json!({"preflight": "ok"}))?;
            // Advisory stubbed-intrinsic preflight (never a gate): when the
            // operator staged a rehearsal snapshot, put the at-risk contract
            // table in the journal BEFORE anything freezes.
            if let Some(prescan) = self.cfg.snapshot.prescan_path.clone() {
                if prescan.exists() {
                    self.advisory_scan(&prescan, State::Armed)?;
                } else {
                    self.journal.evidence(
                        State::Armed,
                        json!({"stubbed_intrinsic_prescan_skipped":
                            format!("{} not present (advisory only)", prescan.display())}),
                    )?;
                }
            }
            Ok(())
        } else {
            let what = if resumed { "resume preflight failed" } else { "preflight failed" };
            self.abort(what, json!({"problems": problems}))?;
            Err(format!("{what}: {}", problems.join("; ")))
        }
    }

    /// Run the stubbed-intrinsic contract scan over a snapshot and journal
    /// the result. ADVISORY: prints the at-risk table, persists it beside
    /// the journal for `pulse-cutover report`, and always continues — an
    /// unserved import is a real code path but not necessarily a reachable
    /// one, and gating the ceremony on it would block every chain whose
    /// legacy contracts still reference send_deferred.
    fn advisory_scan(&mut self, snapshot: &std::path::Path, stage: State) -> Result<(), String> {
        let served = scan::parse_served(scan::DEFAULT_SERVED);
        match scan::scan_snapshot_path(snapshot, &served) {
            Ok(report) => {
                let table = scan::format_table(&report);
                eprint!("{table}");
                if let Some(dir) = self.cfg.journal_path.parent() {
                    let _ = std::fs::write(dir.join("scan-contracts.txt"), &table);
                }
                let rows = serde_json::to_value(
                    report.rows.iter().take(100).collect::<Vec<_>>(),
                )
                .unwrap_or(serde_json::Value::Null);
                self.journal.evidence(
                    stage,
                    json!({
                        "stubbed_intrinsic_scan": {
                            "advisory": true,
                            "snapshot": snapshot.display().to_string(),
                            "served_imports": report.served_count,
                            "code_objects": report.code_objects,
                            "clean": report.clean,
                            "at_risk": report.at_risk,
                            "parse_failures": report.parse_failures,
                            "unserved_tally": report.unserved_tally,
                            "at_risk_rows": rows,
                        }
                    }),
                )?;
            }
            Err(e) => {
                // Advisory means advisory: a scan failure is evidence, not
                // an abort.
                self.journal.evidence(
                    stage,
                    json!({"stubbed_intrinsic_scan_error": e, "advisory": true}),
                )?;
            }
        }
        Ok(())
    }

    /// The production profile, re-checked right before the write freeze (the config may have been
    /// edited, or the source discovered to be mainnet, since load). Ok(false) after aborting.
    fn production_gate(&mut self) -> Result<bool, String> {
        let mainnet = self.chain_id.as_deref().is_some_and(crate::config::is_xpr_mainnet);
        let problems = self.cfg.production_problems();
        if self.cfg.ceremony.rehearsal && !mainnet {
            return Ok(true);
        }
        if self.cfg.ceremony.rehearsal || !problems.is_empty() {
            self.abort(
                "production profile not met before the freeze",
                json!({"problems": problems, "rehearsal": self.cfg.ceremony.rehearsal, "xpr_mainnet": mainnet}),
            )?;
            return Ok(false);
        }
        self.journal.evidence(State::Armed, json!({"production_profile": "met (checked before the freeze)"}))?;
        Ok(true)
    }

    /// Coordinated ceremonies: has the coordinator published a SIGNED abort for this event?
    /// Rate-limited to one poll every 3 s; unreachable mission control = no abort (the
    /// ceremony's own gates still apply). Never consulted after IGNITED.
    fn coordinator_aborted(&mut self, force: bool) -> bool {
        let mut last = self.last_coord_check_ms;
        let out = coordinator_abort_poll(self.cfg, self.ops, &mut last, force);
        self.last_coord_check_ms = last;
        out
    }

    /// Positive authorization for a step that cannot be undone (creating the target chain,
    /// ignition): in a coordinated ceremony the relay must ANSWER, and carry no abort. A relay that
    /// cannot be reached is "unknown", never "no abort": retried for up to 30 s, then the step is
    /// refused. Ok(true) = go; Ok(false) = aborted (or halted, past the point of no return) inside.
    fn require_no_abort(&mut self, before: &str) -> Result<bool, String> {
        let event = self.cfg.coordination.as_ref().and_then(|c| c.event_id.clone());
        if event.is_none() {
            return Ok(true);
        }
        let deadline = self.ops.now_ms() + 30_000;
        loop {
            let mut last = self.last_coord_check_ms;
            let st = coordinator_abort_state(self.cfg, self.ops, &mut last, true);
            self.last_coord_check_ms = last;
            match st {
                AbortPoll::Clear => {
                    self.journal.evidence(self.state, json!({"abort_check": "clear", "before": before, "event_id": event}))?;
                    return Ok(true);
                }
                AbortPoll::Aborted => {
                    self.abort(&format!("coordinator aborted the event (signed) before {before}"), json!({"event_id": event}))?;
                    return Ok(false);
                }
                AbortPoll::Unknown if self.ops.now_ms() > deadline => {
                    self.abort(
                        &format!("cannot confirm the event is not aborted before {before}: the coordinator relay did not answer"),
                        json!({"event_id": event, "note": "an unreachable relay is not an authorization"}),
                    )?;
                    return Ok(false);
                }
                AbortPoll::Unknown => self.ops.sleep_ms(2000),
            }
        }
    }

    /// Pre-ignite fleet gate: wait until `fleet_quorum` producers (this one included) report
    /// VERIFIED-or-later with the same snapshot sha256 and fingerprint digest as ours.
    /// Returns Ok(false) after aborting (signed abort or timeout).
    fn fleet_gate(&mut self) -> Result<bool, String> {
        let Some(co) = self.cfg.coordination.clone() else { return Ok(true) };
        if co.fleet_quorum == 0 && co.event_id.is_none() && co.roster.is_empty() {
            return Ok(true);
        }
        let ours = crate::beacon::journal_summary(&self.cfg.journal_path)["evidence"].clone();
        let deadline = self.ops.now_ms() + co.fleet_timeout_secs * 1000;
        let status_url = format!("{}/api/status", co.url.trim_end_matches('/'));
        let mut last_seen = usize::MAX;
        let mut last_excluded: Vec<serde_json::Value> = vec![json!("none yet")];
        loop {
            if self.coordinator_aborted(true) {
                self.abort("coordinator aborted the event (signed) before ignition", json!({"event_id": co.event_id}))?;
                return Ok(false);
            }
            if co.fleet_quorum == 0 && co.roster.is_empty() {
                return Ok(true);
            }
            let event_id = co.event_id.clone();
            let upstream_backend = self.cfg.ceremony.import_backend == ImportBackend::Upstream;
            let agrees = |r: &serde_json::Value| {
                let c = &r["ceremony"];
                // Absent evidence never "agrees" (null == null must not count), and with an event
                // the report must be about THIS event.
                let same = |k: &str| !ours[k].is_null() && !c["evidence"][k].is_null() && c["evidence"][k] == ours[k];
                let event_ok = match &event_id {
                    Some(id) => r["coord"]["event_id"].as_str() == Some(id.as_str()),
                    None => true,
                };
                matches!(c["state"].as_str(), Some("VERIFIED" | "IGNITED" | "FLIPPED" | "LIVE"))
                    // rc.23: a member withdrawing its VERIFIED report before resuming its source never counts.
                    && c["aborting"].as_bool() != Some(true)
                    && same("snapshot_sha256")
                    && same("fingerprints_digest")
                    // rc.27: one protocol upgrade schedule for the new chain (a different one splits it at activation).
                    // Only the fork backend has no schedule; on the upstream backend a missing hash (a journal from an
                    // older agent) never agrees (rc.27 review: missing evidence is not agreement).
                    && (if upstream_backend { same("protocol_schedule_hash") } else { true })
                    && event_ok
            };
            let producers = match self.ops.get_json(&status_url) {
                Ok(Some(st)) => st["networks"].as_array()
                    .and_then(|nets| nets.iter().find(|n| n["id"].as_str() == Some(co.network.as_str())))
                    .and_then(|n| n["producers"].as_array().cloned())
                    .unwrap_or_default(),
                _ => vec![],
            };
            // Every report the gate may count: per-server beacons (with the relay's identity-conflict
            // flag and age), or the producer-level report of an older relay. A conflicted report (one
            // token reporting from two machines), a stale one, or one with a failing HEALTH check
            // (setup checks such as hooks are ignored: the dashboard's health/setup split) never counts.
            let max_age_ms = co.report_max_age_secs * 1000;
            // Why a report does not count (journaled: an excluded producer costs quorum, and the
            // operator must be able to see why).
            let exclusion = |r: &serde_json::Value, age: Option<u64>, conflict: bool| -> Option<String> {
                if conflict {
                    return Some("identity conflict (one token, two machines)".into());
                }
                if !age.map(|a| a <= max_age_ms).unwrap_or(false) {
                    // A reason CLASS, not the changing age: the record is journaled only when the set
                    // of (report, reason) changes, not on every poll of a steadily stale report.
                    return Some(format!("stale report (older than {} s)", co.report_max_age_secs));
                }
                let failing: Vec<String> = r["checks"].as_array().map(|cs| cs.iter()
                    .filter(|c| c["ok"].as_bool() == Some(false)
                        && !c["name"].as_str().map(|n| crate::beacon::SETUP_CHECKS.contains(&n)).unwrap_or(false))
                    .map(|c| c["name"].as_str().unwrap_or("?").to_string())
                    .collect()).unwrap_or_default();
                if !failing.is_empty() {
                    return Some(format!("failing health check(s): {}", failing.join("; ")));
                }
                // rc.27 review HIGH-2: a verified report for this event whose evidence differs names the key (a peer on
                // an older agent has no protocol_schedule_hash: that costs quorum, and the operator must see why).
                let c = &r["ceremony"];
                let this_event = event_id.as_deref().is_none_or(|id| r["coord"]["event_id"].as_str() == Some(id));
                if this_event && matches!(c["state"].as_str(), Some("VERIFIED" | "IGNITED" | "FLIPPED" | "LIVE")) {
                    let diff: Vec<String> = ["snapshot_sha256", "fingerprints_digest", "protocol_schedule_hash"].into_iter()
                        .filter(|k| !ours[*k].is_null() && c["evidence"][*k] != ours[*k])
                        .map(|k| if c["evidence"][k].is_null() { format!("{k} missing (the peer's agent may predate this release)") } else { k.to_string() })
                        .collect();
                    if !diff.is_empty() {
                        return Some(format!("evidence differs from ours: {}", diff.join(", ")));
                    }
                }
                None
            };
            let mut excluded: Vec<serde_json::Value> = vec![];
            for p in &producers {
                // Only the event's roster is named (a non-roster producer is simply not counted).
                if !co.roster.is_empty()
                    && !co.roster.iter().any(|m| p["name"].as_str() == Some(m.producer.as_str())) {
                    continue;
                }
                let mut v: Vec<(serde_json::Value, Option<u64>, bool)> = p["beacons"].as_array().map(|bs| bs.iter()
                    .map(|b| (b["report"].clone(), b["age_ms"].as_u64(), b["conflict"].as_bool().unwrap_or(false)))
                    .collect()).unwrap_or_default();
                if v.is_empty() && !p["report"].is_null() {
                    v.push((p["report"].clone(), p["age_ms"].as_u64(), p["conflict"].as_bool().unwrap_or(false)));
                }
                for (r, age, conflict) in v {
                    if let Some(why) = exclusion(&r, age, conflict) {
                        excluded.push(json!({"producer": p["name"], "instance_id": r["instance_id"], "why": why}));
                    }
                }
            }
            let reports_of = |p: &serde_json::Value| -> Vec<serde_json::Value> {
                let mut v: Vec<(serde_json::Value, Option<u64>, bool)> = p["beacons"].as_array().map(|bs| bs.iter()
                    .map(|b| (b["report"].clone(), b["age_ms"].as_u64(), b["conflict"].as_bool().unwrap_or(false)))
                    .collect()).unwrap_or_default();
                if v.is_empty() && !p["report"].is_null() {
                    v.push((p["report"].clone(), p["age_ms"].as_u64(), p["conflict"].as_bool().unwrap_or(false)));
                }
                v.into_iter()
                    .filter(|(r, age, conflict)| !conflict && age.map(|a| a <= max_age_ms).unwrap_or(false)
                        && !crate::beacon::has_failing_health(r))
                    .map(|(r, _, _)| r)
                    .collect()
            };
            let agreeing = if co.roster.is_empty() {
                // Legacy gate: any producer the relay lists (unbound roster — see coord.rs), one count
                // per producer, only with a fresh, non-conflicted agreeing report.
                producers.iter().filter(|p| reports_of(p).iter().any(|r| agrees(r))).count()
            } else {
                // Roster gate: only the event's required members.
                co.roster.iter().filter(|m| {
                    let Some(p) = producers.iter().find(|p| p["name"].as_str() == Some(m.producer.as_str())) else { return false };
                    reports_of(p).iter().any(|r| {
                        let id_ok = m.instance_id.as_deref().map(|id| r["instance_id"].as_str() == Some(id)).unwrap_or(true);
                        id_ok && agrees(r)
                    })
                }).count()
            };
            let quorum = if co.fleet_quorum > 0 { co.fleet_quorum } else { co.roster.len() };
            if agreeing != last_seen || excluded != last_excluded {
                self.journal.evidence(State::Verified, json!({"fleet_gate": {"agreeing": agreeing, "quorum": quorum,
                    "roster": if co.roster.is_empty() { json!("unbound (any producer)") } else { json!(co.roster.len()) }},
                    "fleet_gate_excluded": excluded.clone()}))?;
                last_seen = agreeing;
                last_excluded = excluded;
            }
            if agreeing >= quorum {
                return Ok(true);
            }
            if self.ops.now_ms() > deadline {
                self.abort("fleet did not reach verified agreement before fleet_timeout",
                    json!({"agreeing": agreeing, "quorum": quorum, "fleet_timeout_secs": co.fleet_timeout_secs}))?;
                return Ok(false);
            }
            self.ops.sleep_ms(2000);
        }
    }

    /// Sealed stop after IGNITED: this node's target is running (and peers' may be producing),
    /// so resuming the source or reverting public routing could create a second writable
    /// history. Journal it, page humans via `on_halt`, change nothing, and return an error
    /// starting with "HALTED" (the run ends; a human decides).
    fn halt(&mut self, reason: &str, detail: serde_json::Value) -> Result<(), String> {
        let from = self.state;
        // ONE durable record: the HALTED transition carries the decision and its reason. (rc.6 wrote
        // an error and then the transition; a crash between the two left a journal that replayed as
        // still-IGNITED and a restart ran on. Replay also still honours that older shape.)
        self.state = State::Halted;
        self.journal.transition(State::Halted, json!({
            "halted_from": from.as_str(), "reason": reason, "message": format!("HALTED: {reason}"),
            "detail": detail, "sealed": true, "source_resumed": false,
            "public_routing_reverted": false, "writes_reopened": false,
            "why": "ignition may have started; a local failure must not resume the source",
            "clear_with": "pulse-cutover unhalt --config <file> --i-understand"}))?;
        eprintln!("HALTED at {from}: {reason}");
        if let Some(hook) = &self.cfg.hooks.on_halt {
            let result = self.ops.run_hook(hook);
            self.journal.evidence(State::Halted, json!({"on_halt_hook": format!("{result:?}")}))?;
        }
        Err(format!("HALTED at {from}: {reason}"))
    }

    /// True once this node's target may be running: IGNITED or later, `ignite_started` journaled,
    /// or (upstream backend) `create_chain` journaled: a creation hook can submit a chain that an
    /// already-tracking validator starts, and its outcome may be unknown (lost answer, crash), so
    /// from the moment it starts a local failure halts instead of resuming the source.
    fn past_point_of_no_return(&self) -> bool {
        self.reached_ignited || self.create_chain_started || matches!(self.state, State::Ignited | State::Flipped | State::Live | State::Halted)
    }

    /// `pulse-cutover rollback` (what `cutover.sh abort` runs), holding the journal lock: undo
    /// this node's changes and resume the source, but ONLY while ignition has provably not started.
    /// After that point it refuses unless `force` (a coordinator-confirmed, fleet-wide rollback),
    /// and even then only after FENCING this box's target (`target.stop_cmd`): a forced rollback
    /// that cannot prove the target stopped does not resume the source. Always performs the
    /// rollback actions (not gated on `target.auto_rollback`: the operator asked for them).
    /// Err = refused/nothing changed (message starts with "refusing") or a journal I/O error.
    pub fn operator_rollback(&mut self, force: bool) -> Result<RollbackOutcome, String> {
        self.operator_rollback_opts(force, false)
    }

    /// `operator_rollback`, plus rc.23's STRANDED rule: a stranded journal is rolled back (the old chain
    /// resumed) only if the resume guard passes now, or with
    /// `force_stranded` (an operator's record of a fleet-wide decision, journaled).
    pub fn operator_rollback_opts(&mut self, force: bool, force_stranded: bool) -> Result<RollbackOutcome, String> {
        if self.state == State::Stranded {
            if force_stranded {
                // The fleet view being overridden is journaled with the override (review #10).
                let co = self.cfg.coordination.clone();
                let status = self.fleet_status();
                let me = self.me();
                let view = co.as_ref().map(|co| {
                    let members: Vec<_> = status.as_ref().map(|st| crate::fleet::members(st, co)).unwrap_or_default().into_iter()
                        .map(|m| json!({"producer": m.producer, "state": m.last.as_ref().map(|r| r["ceremony"]["state"].clone()),
                            "fresh": m.report.is_some(), "missing": m.missing, "conflict": m.conflict, "max_past": m.max_past})).collect();
                    let signed = co.event_id.as_deref().is_some_and(|e| crate::coord::is_tombstoned(self.cfg, e));
                    json!({"relay_answered": status.is_some(), "members": members,
                        "guard": match crate::fleet::resume_guard(status.as_ref(), co, me.as_ref().map(|(p, i)| (p.as_str(), i.as_str())), signed) {
                            Ok(_) => "would pass".to_string(), Err(why) => why }})
                });
                self.journal.evidence(self.state, json!({"resume_guard": "overridden by the operator (--force-stranded --i-understand)",
                    "overridden_fleet_view": view,
                    "by": std::env::var("SUDO_USER").or_else(|_| std::env::var("USER")).unwrap_or_else(|_| "unknown".into())}))?;
            } else if self.cfg.coordination.as_ref().is_some_and(|c| c.event_id.is_some()) {
                if let Err(why) = self.resume_guard()? {
                    return Err(format!(
                        "refusing to roll back a STRANDED ceremony: {why}. Nothing was changed. If the fleet went LIVE, \
                         use `pulse-cutover join`; after a fleet-wide decision to resume the old chain, re-run with --force-stranded."));
                }
            }
        }
        let past = self.past_point_of_no_return();
        let mut notes = vec![];
        // FIRST, on every path (including "already rolled back"): an agent killed mid-hook or mid
        // pipeline step leaves that process group running (they run in their own group). Stop it
        // before anything below, or e.g. a still-running on_freeze could close writes again after
        // on_abort reopened them, or a late import could re-create the staged artifact. If it cannot
        // be stopped nothing is done: that is a refusal (nothing changed), not an incomplete rollback.
        match self.ops.kill_orphan_hooks() {
            Ok(None) => {}
            Ok(Some(desc)) => {
                self.journal.evidence(self.state, json!({"orphan_hook_killed": desc}))?;
                notes.push(desc);
            }
            Err(e) => {
                self.journal.evidence(self.state, json!({"orphan_hook_kill_error": e, "rollback_blocked": true}))?;
                return Err(format!(
                    "refusing to roll back: a hook left running by a killed agent could not be stopped ({e}). \
                     Nothing was rolled back. Stop that process group by hand, then re-run."
                ));
            }
        }
        if self.state == State::Aborted && self.aborted_rollback_complete {
            // Idempotent even here: a staged artifact that appeared after the rollback (e.g. a late
            // pipeline write) is moved aside too.
            let mut failed = vec![];
            self.unstage(true, &mut failed, &mut notes)?;
            notes.push("already rolled back (the journal proves every rollback step completed): nothing done again".into());
            return Ok(RollbackOutcome { state: self.state, failed, notes, already: true });
        }
        if past && !force {
            return Err(format!(
                "refusing to roll back: journal state {} and ignition may have started (the target may \
                 be running). Nothing was changed. A fleet-wide rollback confirmed by the coordinator \
                 uses --force-after-ignite.",
                self.state
            ));
        }
        // Intent first: if this rollback dies before its ABORTED transition, a later `run` must not
        // resume the ceremony (see `rollback_pending`); re-running `rollback` finishes the job.
        self.journal.evidence(self.state, json!({"rollback_requested": true, "force_after_ignite": force && past}))?;
        self.rollback_pending = true;
        // The fence runs on EVERY forced attempt: it is a precondition checked now, not a revert that
        // stays done (the target may have been restarted since an earlier attempt).
        if force && past {
            // Local fence FIRST: resuming the source while this box's target still runs would
            // create two writable histories under one chain_id, by our own hand.
            let fence = self.cfg.target.fence_cmd();
            self.journal.evidence(self.state, json!({"side_effect": "target_fence", "cmd": fence}))?;
            match self.ops.run_hook(&fence) {
                Ok(out) => {
                    self.journal.evidence(self.state, json!({"target_fenced": true, "output": out}))?;
                    self.mark_step("target_fence")?;
                    notes.push("this box's target was stopped first (local fence only: other producers' targets are unaffected)".into());
                }
                Err(e) => {
                    self.journal.evidence(self.state, json!({"target_fenced": false, "error": e,
                        "forced_rollback_blocked": "the target on this box could not be proven stopped; the source was NOT resumed"}))?;
                    return Ok(RollbackOutcome { state: self.state, notes, already: false,
                        failed: vec![format!("target fence failed ({e}): the source was NOT resumed and routing was NOT reverted")] });
                }
            }
        }
        if force && past {
            // Operator decision: every configured flip revert runs (a flip may have half-applied).
            self.flip_ran = self.flip_ran || self.cfg.flip.is_some();
            self.hyperion_flip_ran = self.hyperion_flip_ran || self.cfg.hyperion.is_some();
        }
        let detail = json!({"by": "operator_rollback", "force_after_ignite": force && past,
                            "ignition_started": past, "state_before": self.state.as_str()});
        let (failed, mut more) = self.rollback_steps("operator rollback (pulse-cutover rollback)", detail, true, true)?;
        notes.append(&mut more);
        Ok(RollbackOutcome { state: self.state, failed, notes, already: false })
    }

    fn step_done(&self, name: &str) -> bool {
        self.rollback_steps_done.iter().any(|s| s == name)
    }

    /// Journal that a rollback step completed (a rollback that dies later redoes only the rest).
    fn mark_step(&mut self, name: &str) -> Result<(), String> {
        self.journal.evidence(self.state, json!({"rollback_step": name, "ok": true}))?;
        if !self.step_done(name) {
            self.rollback_steps_done.push(name.to_string());
        }
        Ok(())
    }

    /// Move the staged snapshot aside (never delete it): left in place it makes the next preflight
    /// refuse and, worse, a metalgo restart would import the abandoned cut. `any`: the operator's
    /// rollback moves any staged file; the agent's own abort only the one this ceremony staged.
    fn unstage(&mut self, any: bool, failed: &mut Vec<String>, notes: &mut Vec<String>) -> Result<(), String> {
        let staged = self.cfg.snapshot.staged_path.clone();
        if !staged.exists() || !(any || self.staged_sha256.is_some()) {
            return Ok(());
        }
        let mut to = staged.as_os_str().to_owned();
        to.push(format!(".rolled-back-{}", self.ops.now_ms()));
        let to = std::path::PathBuf::from(to);
        match std::fs::rename(&staged, &to) {
            Ok(()) => {
                self.journal.evidence(self.state, json!({"unstaged": {"from": staged, "to": to}}))?;
                notes.push(format!("staged snapshot moved aside to {}", to.display()));
            }
            Err(e) => {
                let msg = format!("unstage {} failed: {e}", staged.display());
                self.journal.evidence(self.state, json!({"unstage_error": msg}))?;
                failed.push(msg);
            }
        }
        Ok(())
    }

    fn abort(&mut self, reason: &str, detail: serde_json::Value) -> Result<(), String> {
        if self.past_point_of_no_return() {
            return self.halt(reason, detail);
        }
        let perform = self.cfg.target.auto_rollback;
        // rc.23 (fleet run r4): a coordinated producer that froze writes resumes the OLD chain only when the
        // resume guard passes (fleet::resume_guard: nobody past chain creation, and the quorum unreachable
        // without this node, or a signed abort with every member accounted for). Otherwise it is STRANDED:
        // sealed, writes stay frozen. Two phases (review #3): after a first pass, the abort intent is
        // journaled (the beacon then stops presenting this node as VERIFIED), the agent waits until the
        // relay shows that (or until its last VERIFIED report is too old for any peer's gate to count),
        // and only a second pass of the guard lets it resume.
        if perform && self.needs_resume_guard() {
            if let Err(why) = self.resume_guard()? {
                return self.strand(reason, detail, &why);
            }
            self.journal.evidence(self.state, json!({"abort_intent": true, "reason": reason,
                "note": "withdrawing this node's VERIFIED report before resuming the old chain"}))?;
            self.wait_own_report_withdrawn()?;
            if let Err(why) = self.resume_guard()? {
                return self.strand(reason, detail, &why);
            }
        }
        let (failed, _notes) = self.rollback_steps(reason, detail, perform, false)?;
        if !failed.is_empty() {
            eprintln!("ABORTED, but the rollback is INCOMPLETE: {}", failed.join("; "));
        }
        Ok(())
    }

    /// The resume guard applies to a coordinated producer whose source this ceremony froze (FROZEN to
    /// VERIFIED): peers of the same event may be creating or igniting the target.
    fn needs_resume_guard(&self) -> bool {
        self.cfg.ceremony.mode == Mode::Producer
            && self.cfg.coordination.as_ref().is_some_and(|c| c.event_id.is_some())
            && matches!(self.state, State::Frozen | State::Snapshotted | State::Verified)
    }

    /// This node as the relay knows it: (beacon producer, beacon instance id).
    fn me(&self) -> Option<(String, String)> {
        self.cfg.beacon.as_ref().map(|b| (b.producer.clone(), crate::beacon::instance_id(self.cfg)))
    }

    /// The relay's `/api/status`, retried for up to ~10 s (None = it did not answer).
    fn fleet_status(&self) -> Option<serde_json::Value> {
        let co = self.cfg.coordination.as_ref()?;
        let url = format!("{}/api/status", co.url.trim_end_matches('/'));
        for i in 0..5 {
            if let Ok(Some(mut doc)) = self.ops.get_json(&url) {
                if doc.is_object() {
                    // A report whose journal was armed for another H is not evidence for this event (rc.24 fleet
                    // rehearsal: a stale journal under the current event id).
                    // Compared with the event's H as this journal resolved it at ARM (what the beacon reports as
                    // `evidence.h`), not the cut height (an inexact rehearsal cut would drop every peer).
                    if let Some(h) = self.resolved_h {
                        crate::fleet::drop_foreign_ceremonies(&mut doc, &co.network, h);
                    }
                    return Some(doc);
                }
            }
            if i < 4 {
                self.ops.sleep_ms(2000);
            }
        }
        None
    }

    /// Phase 2 of a guarded resume: wait until the relay shows this node's own report as no longer
    /// VERIFIED (state moved on, or `aborting`), at most `report_max_age_secs` (after that no peer's gate
    /// counts the old report anyway: it is stale).
    fn wait_own_report_withdrawn(&mut self) -> Result<(), String> {
        let co = self.cfg.coordination.clone().expect("checked by needs_resume_guard");
        let me = self.me();
        let event = co.event_id.clone().unwrap_or_default();
        let deadline = self.ops.now_ms() + co.report_max_age_secs * 1000;
        let url = format!("{}/api/status", co.url.trim_end_matches('/'));
        loop {
            if let (Some((prod, inst)), Ok(Some(doc))) = (me.as_ref(), self.ops.get_json(&url)) {
                let mine = doc["networks"].as_array().into_iter().flatten()
                    .filter(|n| n["id"].as_str() == Some(co.network.as_str()))
                    .flat_map(|n| n["producers"].as_array().cloned().unwrap_or_default())
                    .filter(|p| p["name"].as_str() == Some(prod.as_str()))
                    .flat_map(|p| p["beacons"].as_array().cloned().unwrap_or_default())
                    .map(|b| b["report"].clone())
                    .filter(|r| r["coord"]["event_id"].as_str() == Some(event.as_str()) && r["instance_id"].as_str().is_none_or(|i| i == inst))
                    .collect::<Vec<_>>();
                if !mine.is_empty() && mine.iter().all(|r| r["ceremony"]["state"].as_str() != Some("VERIFIED")
                    || r["ceremony"]["aborting"].as_bool() == Some(true)) {
                    self.journal.evidence(self.state, json!({"own_report_withdrawn": "the relay shows this node aborting"}))?;
                    // Re-check #2: a peer that journaled create_chain just before may not have REPORTED it yet.
                    // Wait one beacon cycle (its interval, at least 10 s) before the second guard pass.
                    let interval = self.cfg.beacon.as_ref().map(|b| b.interval_secs).unwrap_or(5);
                    let settle_ms = interval.max(10) * 1000;
                    let until = self.ops.now_ms() + settle_ms;
                    while self.ops.now_ms() < until {
                        self.ops.sleep_ms(1000);
                    }
                    self.journal.evidence(self.state, json!({"settle_before_second_guard_ms": settle_ms}))?;
                    return Ok(());
                }
            }
            if self.ops.now_ms() > deadline {
                self.journal.evidence(self.state, json!({"own_report_withdrawn": format!(
                    "not seen on the relay; waited report_max_age_secs ({} s): the last VERIFIED report is now too old for any gate", co.report_max_age_secs)}))?;
                return Ok(());
            }
            self.ops.sleep_ms(2000);
        }
    }

    /// rc.27: compare the target's `getInfo` protocol fields with this event's schedule. Ok(Ok(note)) = matches or
    /// not reported (an older PulseVM; noted), Ok(Err(detail)) = mismatch.
    fn target_protocol_problem(&mut self) -> Result<Result<serde_json::Value, serde_json::Value>, String> {
        let up = match self.cfg.upstream.as_ref() {
            Some(up) if self.cfg.ceremony.import_backend == ImportBackend::Upstream => up,
            _ => return Ok(Ok(json!("not applicable (not the upstream backend)"))),
        };
        let sched = match upstream::ProtocolSchedule::load(up.protocol_upgrades_file.as_deref()) {
            Ok(s) => s,
            Err(e) => return Ok(Err(json!({"error": e}))),
        };
        // Review LOW-2: what the fleet compared (VERIFIED evidence) is authoritative, not the file now on disk.
        let want = crate::beacon::journal_summary(&self.cfg.journal_path)["evidence"]["protocol_schedule_hash"].as_str()
            .map(str::to_string).unwrap_or_else(|| sched.hash.clone());
        // Review HIGH-1: a transport failure is retried, then unknown; a getInfo WITHOUT the protocol fields means a
        // binary older than protocol schedules, which cannot run any scheduled version > 1.
        let mut reported = None;
        let mut last_err = None;
        for i in 0..5 {
            match self.ops.target_protocol() {
                Ok(v) => { reported = Some(v); break; }
                Err(e) => last_err = Some(e),
            }
            if i < 4 {
                self.ops.sleep_ms(2000);
            }
        }
        let t = match reported {
            Some(Some(t)) => t,
            Some(None) if sched.max_version() <= 1 => {
                return Ok(Ok(json!("UNVERIFIED: the target does not report protocol fields (empty schedule: nothing to activate)")));
            }
            Some(None) => return Ok(Err(json!({"max_scheduled_version": sched.max_version(),
                "fix": "the target's PulseVM does not report protocol fields, so it predates protocol schedules and cannot run the scheduled version: upgrade it"}))),
            None if sched.max_version() <= 1 => {
                return Ok(Ok(json!({"UNVERIFIED": "the target's getInfo could not be read", "error": last_err})));
            }
            None => return Ok(Err(json!({"error": last_err, "fix": "the target's getInfo could not be read: its protocol schedule is unknown"}))),
        };
        let theirs = t["protocol_upgrade_schedule_hash"].as_str().unwrap_or("");
        let supported = t["supported_protocol_version"].as_u64().unwrap_or(0);
        if !theirs.eq_ignore_ascii_case(&want) {
            return Ok(Err(json!({"expected_schedule_hash": want, "target": t,
                "fix": format!("the validator loaded another upgrade.json than this event's (expected {} entries)", sched.entries.len())})));
        }
        if supported < sched.max_version() as u64 {
            return Ok(Err(json!({"max_scheduled_version": sched.max_version(), "target": t,
                "fix": "this PulseVM binary cannot execute the scheduled protocol version: upgrade it before the activation height"})));
        }
        Ok(Ok(json!({"schedule_hash": want, "supported_protocol_version": supported, "verified": true})))
    }

    /// rc.27: install the new chain's protocol upgrade schedule as `<chain_config_dir>/<bid>/upgrade.json` (MetalGo
    /// hands it to PulseVM as upgrade_bytes). The file must still hash to what VERIFIED journaled (the fleet
    /// compared that); an empty schedule installs nothing, and refuses a stale upgrade.json already there.
    fn install_protocol_schedule(&mut self, dir: &std::path::Path, bid: &str) -> Result<Result<(), serde_json::Value>, String> {
        let up = match self.cfg.upstream.as_ref() {
            Some(up) if self.cfg.ceremony.import_backend == ImportBackend::Upstream => up,
            _ => return Ok(Ok(())),
        };
        let want = crate::beacon::journal_summary(&self.cfg.journal_path)["evidence"]["protocol_schedule_hash"].as_str().map(str::to_string);
        let sched = match upstream::ProtocolSchedule::load(up.protocol_upgrades_file.as_deref()) {
            Ok(s) => s,
            Err(e) => return Ok(Err(json!({"error": e}))),
        };
        let Some(want) = want else {
            return Ok(Err(json!({"error": "no protocol upgrade schedule hash journaled at VERIFIED (a journal from an agent before rc.27): \
                                  the fleet never compared this node's schedule"})));
        };
        if want != sched.hash {
            return Ok(Err(json!({"error": "upstream.protocol_upgrades_file changed since VERIFIED", "verified": want, "now": sched.hash})));
        }
        let dest = dir.join(bid).join("upgrade.json");
        if sched.entries.is_empty() {
            if dest.exists() {
                return Ok(Err(json!({"error": format!("{} exists but this event's schedule is empty: remove it", dest.display())})));
            }
            self.journal.evidence(self.state, json!({"protocol_schedule_installed": "empty (protocol version 1)", "hash": sched.hash}))?;
            return Ok(Ok(()));
        }
        let bytes = serde_json::to_vec_pretty(&sched.to_json()).expect("json");
        // Atomic (review LOW-3): a crash mid-write must not leave a truncated schedule for metalgo to load.
        let tmp = dest.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, &bytes).and_then(|_| std::fs::rename(&tmp, &dest)) {
            return Ok(Err(json!({"error": format!("write {}: {e}", dest.display())})));
        }
        self.journal.evidence(self.state, json!({"protocol_schedule_installed": dest.display().to_string(), "hash": sched.hash,
            "protocol_upgrades": sched.to_json()["protocol_upgrades"]}))?;
        Ok(Ok(()))
    }

    /// May the old chain be resumed? Ok(Ok(())) after journaling why; Ok(Err(why)) = no (fleet::resume_guard).
    fn resume_guard(&mut self) -> Result<Result<(), String>, String> {
        let co = self.cfg.coordination.clone().expect("checked by needs_resume_guard");
        let event = co.event_id.clone().unwrap_or_default();
        let status = self.fleet_status();
        let signed_abort = crate::coord::is_tombstoned(self.cfg, &event)
            || self.ops.get_json(&format!("{}/api/coord/{}", co.url.trim_end_matches('/'), co.network)).ok().flatten()
                .is_some_and(|d| crate::coord::aborted(&d, &co.coordinator_keys, &co.network, &event));
        let me = self.me();
        let me_ref = me.as_ref().map(|(p, i)| (p.as_str(), i.as_str()));
        match crate::fleet::resume_guard(status.as_ref(), &co, me_ref, signed_abort) {
            Ok(ev) => {
                self.journal.evidence(self.state, json!({"resume_guard": "clear", "signed_abort": signed_abort, "fleet": ev}))?;
                Ok(Ok(()))
            }
            Err(why) => {
                self.journal.evidence(self.state, json!({"resume_guard": "refused", "signed_abort": signed_abort, "why": why}))?;
                Ok(Err(why))
            }
        }
    }

    fn stranded_message(&self) -> String {
        format!(
            "STRANDED (journaled): this producer stopped before its own chain creation but could not prove that no \
             other roster member had started creation or ignition, so the old chain was NOT resumed and writes stay \
             frozen. If the fleet went LIVE: `pulse-cutover join --config <file> --event <id>`. Otherwise: \
             `pulse-cutover rollback --config <file>` (re-runs the resume guard; `--force-stranded --i-understand` only \
             after a fleet-wide decision). Journal: {}",
            self.journal.path().display()
        )
    }

    /// STRANDED: like HALTED, nothing is rolled back (source paused, writes frozen, staged files kept);
    /// `on_halt` pages a human.
    fn strand(&mut self, reason: &str, detail: serde_json::Value, why: &str) -> Result<(), String> {
        let from = self.state;
        self.journal.error(self.state, reason, detail.clone())?;
        self.state = State::Stranded;
        self.journal.transition(State::Stranded, json!({
            "stranded_from": from.as_str(), "reason": reason, "detail": detail,
            "message": format!("STRANDED: {reason}; peers may have ignited ({why}): refusing to resume the source"),
            "guard": why, "sealed": true, "source_resumed": false, "writes_reopened": false,
            "clear_with": "pulse-cutover join --config <file> --event <id> (fleet LIVE) | pulse-cutover rollback --config <file> [--force-stranded]"}))?;
        eprintln!("STRANDED at {from}: {reason}; peers may have ignited ({why}); refusing to resume the source");
        if let Some(hook) = &self.cfg.hooks.on_halt {
            let result = self.ops.run_hook(hook);
            self.journal.evidence(State::Stranded, json!({"on_halt_hook": format!("{result:?}")}))?;
        }
        Ok(())
    }

    /// Journal the abort and (if `perform`) undo this node's changes, step by step: each step that
    /// succeeds is journaled (`rollback_step`) and skipped if the rollback is re-run, and the
    /// rollback is only recorded as finished (`rollback_done`) AFTER every step, on_abort and the
    /// unstage included, succeeded. Returns (failed steps, notes). `unstage_any`: see `unstage`.
    fn rollback_steps(&mut self, reason: &str, detail: serde_json::Value, perform: bool, unstage_any: bool)
        -> Result<(Vec<String>, Vec<String>), String> {
        self.journal.error(self.state, reason, detail.clone())?;
        let mut failed: Vec<String> = vec![];
        let mut notes: Vec<String> = vec![];
        let mut rollback = json!({"reason": reason, "auto_rollback": self.cfg.target.auto_rollback, "rollback_performed": perform});
        if let Some(obj) = detail.as_object() {
            for k in ["by", "force_after_ignite", "ignition_started"] {
                if let Some(v) = obj.get(k) {
                    rollback[k] = v.clone();
                }
            }
        }
        if !self.rollback_steps_done.is_empty() {
            rollback["steps_already_done"] = json!(self.rollback_steps_done.clone());
        }
        if perform {
            match self.cfg.ceremony.mode {
                // Producer mode: un-pausing nodeos IS the entire rollback.
                Mode::Producer => {
                    if !self.step_done("resume") {
                        match self.ops.resume() {
                            Ok(()) => {
                                rollback["source_producer_resumed"] = json!(true);
                                self.mark_step("resume")?;
                            }
                            Err(e) => {
                                failed.push(format!("resume the source producer: {e}"));
                                rollback["source_producer_resume_error"] = json!(e);
                            }
                        }
                    } else {
                        rollback["source_producer_resumed"] = json!("earlier (journaled)");
                    }
                }
                // api mode: nodeos was never paused (it isn't ours to pause).
                // Undo whatever user-visible steps already happened, in
                // reverse order: restart the source if we stopped it, then
                // swap the public URL back to it.
                Mode::Api => {
                    if self.source_stopped && !self.step_done("source_restart") {
                        if let Some(cmd) = self.cfg.source.start_cmd.clone() {
                            match self.ops.run_hook(&cmd) {
                                Ok(o) => {
                                    rollback["source_restarted"] = json!(o);
                                    self.mark_step("source_restart")?;
                                }
                                Err(e) => {
                                    failed.push(format!("restart the source: {e}"));
                                    rollback["source_restart_error"] = json!(e);
                                }
                            }
                        } else {
                            failed.push("the source was stopped and no source.start_cmd is configured: restart it by hand".into());
                            rollback["source_stopped_no_start_cmd"] = json!(true);
                        }
                    }
                    if self.hyperion_flip_ran && !self.step_done("hyperion_revert") {
                        if let Some(cmd) = self.cfg.hyperion.as_ref().and_then(|h| h.revert_cmd.clone()) {
                            match self.ops.run_hook(&cmd) {
                                Ok(o) => {
                                    rollback["hyperion_flip_reverted"] = json!(o);
                                    self.mark_step("hyperion_revert")?;
                                }
                                Err(e) => {
                                    failed.push(format!("revert the /v2 flip: {e}"));
                                    rollback["hyperion_flip_revert_error"] = json!(e);
                                }
                            }
                        }
                    }
                    if self.flip_ran && !self.step_done("flip_revert") {
                        if let Some(cmd) = self.cfg.flip.as_ref().and_then(|f| f.revert_cmd.clone()) {
                            match self.ops.run_hook(&cmd) {
                                Ok(o) => {
                                    rollback["flip_reverted"] = json!(o);
                                    self.mark_step("flip_revert")?;
                                }
                                Err(e) => {
                                    failed.push(format!("revert the public flip: {e}"));
                                    rollback["flip_revert_error"] = json!(e);
                                }
                            }
                        }
                    }
                    rollback["source_chain_untouched"] = json!(!self.source_stopped);
                }
            }
        }
        // The reverts' result only: completion of the WHOLE rollback is `rollback_done`, below.
        rollback["reverts_ok"] = json!(perform && failed.is_empty());
        self.state = State::Aborted;
        self.journal.transition(State::Aborted, rollback)?;
        if let Some(hook) = self.cfg.hooks.on_abort.clone() {
            if !self.step_done("on_abort") {
                let result = self.ops.run_hook(&hook);
                self.journal.evidence(State::Aborted, json!({"on_abort_hook": format!("{result:?}")}))?;
                match result {
                    Ok(_) => self.mark_step("on_abort")?,
                    Err(e) => failed.push(format!("on_abort hook: {e}")),
                }
            }
        }
        self.unstage(unstage_any, &mut failed, &mut notes)?;
        if failed.is_empty() {
            if perform {
                self.journal.evidence(State::Aborted, json!({"rollback_done": true, "rollback_complete": true}))?;
                self.aborted_rollback_complete = true;
            }
        } else {
            self.journal.evidence(State::Aborted, json!({"rollback_incomplete": failed, "rollback_complete": false}))?;
        }
        Ok((failed, notes))
    }

    /// ARMED: watch head until H, then FREEZE WRITES (hook) — production
    /// keeps running so the snapshot block can finalize (finding R1; a
    /// paused chain never finalizes its head, verified empirically on Leap
    /// 5.0.3: create_snapshot hangs). The cut is pinned in the FROZEN step.
    fn step_armed(&mut self) -> Result<(), String> {
        if self.cfg.ceremony.mode == Mode::Api {
            return self.step_armed_api();
        }
        if self.cfg.ceremony.freeze_strategy == FreezeStrategy::ScheduleAtH {
            // Multi-BP mode: pin the snapshot to exactly H up front; nodeos
            // writes it when H becomes irreversible. (Falls back to the
            // immediate create_snapshot path if the scheduler is missing.)
            match self.ops.schedule_snapshot(self.h()) {
                Ok(()) => {
                    self.scheduled = true;
                    self.journal.evidence(
                        State::Armed,
                        json!({"snapshot_scheduled_at": self.h()}),
                    )?;
                }
                Err(e) => {
                    // No fallback: an immediate snapshot would cut at whatever head is current,
                    // not at H, and different producers would cut at different blocks.
                    self.abort(
                        "schedule_snapshot(H) failed: exact-H cut cannot be guaranteed",
                        json!({"error": e, "h": self.h(),
                               "fix": "enable the producer_api snapshot scheduler (Leap 4+) and re-run"}),
                    )?;
                    return Ok(());
                }
            }
        }
        // Multi-BP: close writes `freeze_lead_blocks` before H so in-flight
        // transactions land at or before the cut, not in H+1.. (the cut
        // itself stays pinned to exactly H by the scheduled snapshot).
        let freeze_at = if self.scheduled {
            self.h().saturating_sub(self.cfg.ceremony.freeze_lead_blocks)
        } else {
            self.h()
        };
        let mut last_heartbeat = 0u64;
        let info = loop {
            let info = self.ops.source_info()?;
            if info.head_block_num >= freeze_at {
                break info;
            }
            if self.coordinator_aborted(false) {
                self.abort("coordinator aborted the event (signed) before the freeze", json!({"head": info.head_block_num}))?;
                return Ok(());
            }
            let now = self.ops.now_ms();
            if now.saturating_sub(last_heartbeat) >= 30_000 {
                self.journal.evidence(
                    State::Armed,
                    json!({"head": info.head_block_num, "lib": info.last_irreversible_block_num,
                           "blocks_to_h": self.h().saturating_sub(info.head_block_num),
                           "blocks_to_freeze": freeze_at.saturating_sub(info.head_block_num)}),
                )?;
                last_heartbeat = now;
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        };

        if !self.production_gate()? {
            return Ok(());
        }
        // The write freeze — the moment the write gap starts (R2: an
        // explicit reject at the API edge, never just a producer pause).
        let freeze_hook = if let Some(hook) = &self.cfg.hooks.on_freeze {
            let result = self.ops.run_hook(hook);
            if let Err(e) = &result {
                self.abort("write-freeze hook failed", json!({"error": e}))?;
                return Ok(());
            }
            format!("{result:?}")
        } else {
            "none configured (rehearsal traffic stops via pause visibility)".into()
        };
        self.chain_id = Some(info.chain_id.clone());
        self.frozen_ts_ms = Some(self.ops.now_ms());
        self.state = State::Frozen;
        self.journal.transition(
            State::Frozen,
            json!({
                "declared_h": self.h(),
                "freeze_at": freeze_at,
                "head_at_freeze": info.head_block_num,
                "lib_at_freeze": info.last_irreversible_block_num,
                "chain_id": info.chain_id,
                "on_freeze_hook": freeze_hook,
                "note": "writes frozen; production continues so the cut can finalize (R1)",
            }),
        )?;
        Ok(())
    }

    /// ARMED (api mode): this node cannot freeze the chain — it observes the
    /// declared H. Gate on LIB >= H (snapshot-at-finality, R1): in a real
    /// ceremony the BPs produce empty blocks through H until it finalizes and
    /// LIB reaches H; under `simulate_freeze` the live chain simply advances
    /// past H and we proceed as if frozen, journaling the actual cut later.
    fn step_armed_api(&mut self) -> Result<(), String> {
        // Exact H: unless rehearsing against a live chain (simulate_freeze), the API node pins
        // its snapshot to H with the scheduler — never "whatever head is when LIB passes H".
        if !self.cfg.ceremony.simulate_freeze && !self.scheduled {
            match self.ops.schedule_snapshot(self.h()) {
                Ok(()) => {
                    self.scheduled = true;
                    self.journal.evidence(State::Armed, json!({"snapshot_scheduled_at": self.h()}))?;
                }
                Err(e) => {
                    self.abort(
                        "schedule_snapshot(H) failed: api mode cannot cut at exactly H",
                        json!({"error": e, "h": self.h()}),
                    )?;
                    return Ok(());
                }
            }
        }
        let mut last_heartbeat = 0u64;
        let info = loop {
            let info = self.ops.source_info()?;
            if info.last_irreversible_block_num >= self.h() {
                break info;
            }
            let now = self.ops.now_ms();
            if now.saturating_sub(last_heartbeat) >= 30_000 {
                self.journal.evidence(
                    State::Armed,
                    json!({"head": info.head_block_num, "lib": info.last_irreversible_block_num,
                           "blocks_to_h_final": self.h().saturating_sub(info.last_irreversible_block_num)}),
                )?;
                last_heartbeat = now;
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        };
        if !self.production_gate()? {
            return Ok(());
        }
        // Optional write-freeze hook (e.g. a local gateway starts rejecting
        // writes with a clear "cutover in progress" error — R2). An API node
        // cannot freeze the network's writes; that happened (or is simulated
        // to have happened) at the BP edge.
        let freeze_hook = if let Some(hook) = &self.cfg.hooks.on_freeze {
            // Required hook: a failed write-freeze must not be shrugged off (run-5 lesson).
            match self.ops.run_hook(hook) {
                Ok(o) => o,
                Err(e) => {
                    self.abort("write-freeze hook failed", json!({"error": e}))?;
                    return Ok(());
                }
            }
        } else {
            "none configured".into()
        };
        self.chain_id = Some(info.chain_id.clone());
        self.frozen_ts_ms = Some(self.ops.now_ms());
        self.state = State::Frozen;
        self.journal.transition(
            State::Frozen,
            json!({
                "declared_h": self.h(),
                "simulate_freeze": self.cfg.ceremony.simulate_freeze,
                "head_at_freeze": info.head_block_num,
                "lib_at_freeze": info.last_irreversible_block_num,
                "chain_id": info.chain_id,
                "on_freeze_hook": freeze_hook,
                "note": if self.cfg.ceremony.simulate_freeze {
                    "SIMULATED freeze: live chain keeps advancing; cut lands at ~finality near H"
                } else {
                    "observed freeze: H final on the source chain (BP-side freeze)"
                },
            }),
        )?;
        Ok(())
    }

    /// FROZEN (api mode): snapshot via this node's own producer_api —
    /// create_snapshot works read-only on non-producers; Leap returns once
    /// the snapshot block is irreversible (live chain => LIB advances on its
    /// own). No pause, no quiescence: the chain is not ours to stop. The cut
    /// is pinned by the snapshot's own block id, cross-checked against the
    /// chain's view of that height.
    fn step_frozen_api(&mut self) -> Result<(), String> {
        let started = self.ops.now_ms();
        let snap = if self.scheduled {
            match self.await_scheduled_snapshot()? {
                Some(s) => s,
                None => return Ok(()), // aborted inside, with evidence
            }
        } else {
            // simulate_freeze only (validated): a live chain, an inexact cut, journaled as such.
            match self.ops.create_snapshot() {
                Ok(s) => s,
                Err(e) => {
                    self.abort("create_snapshot failed", json!({"error": e}))?;
                    return Ok(());
                }
            }
        };
        let snapshot_wall_ms = self.ops.now_ms().saturating_sub(started);
        let cut_height = snap.head_block_num;
        if !self.cut_is_exact(cut_height)? {
            return Ok(()); // aborted inside
        }
        let (chain_view_id, cut_block_time) = self.ops.source_block_id(cut_height)?;
        if !snap.head_block_id.eq_ignore_ascii_case(&chain_view_id) {
            self.abort(
                "snapshot block id != chain block id at cut height (fork at the cut?)",
                json!({"snapshot_block_id": snap.head_block_id, "chain_block_id": chain_view_id}),
            )?;
            return Ok(());
        }
        let info = self.ops.source_info()?;
        if let Some(expected) = &self.cfg.ceremony.chain_id {
            if !info.chain_id.eq_ignore_ascii_case(expected) {
                self.abort(
                    "source chain_id changed mid-ceremony",
                    json!({"seen": info.chain_id, "expected": expected}),
                )?;
                return Ok(());
            }
        }
        let host_path = self.cfg.map_snapshot_path(&snap.snapshot_name);
        if !host_path.exists() {
            self.abort(
                "snapshot file not found on host",
                json!({"reported": snap.snapshot_name, "mapped": host_path.display().to_string()}),
            )?;
            return Ok(());
        }
        let size = std::fs::metadata(&host_path).map(|m| m.len()).unwrap_or(0);
        self.cut_height = Some(cut_height);
        self.cut_block_id = Some(snap.head_block_id.clone());
        self.last_source_block_time = Some(cut_block_time.clone());
        self.snapshot_file = Some(host_path.display().to_string());
        self.state = State::Snapshotted;
        self.journal.transition(
            State::Snapshotted,
            json!({
                "snapshot_file": host_path.display().to_string(),
                "reported_name": snap.snapshot_name,
                "size_bytes": size,
                "snapshot_wall_ms": snapshot_wall_ms,
                "cut_height": cut_height,
                "cut_block_id": snap.head_block_id,
                "cut_vs_declared_h": cut_height as i64 - self.h() as i64,
                "last_source_block_time": cut_block_time,
                "head_after_snapshot": info.head_block_num,
                "note": "api mode: cut pinned by the snapshot's own finalized block; source keeps serving reads",
            }),
        )?;
        Ok(())
    }

    /// Exact-H rule: the cut must be H. Only `allow_inexact_cut` (single-producer pause_at_h
    /// rehearsals) or `simulate_freeze` (live-chain API rehearsals) may accept another height,
    /// and then it is journaled loudly. Returns Ok(false) after aborting.
    fn cut_is_exact(&mut self, cut: u64) -> Result<bool, String> {
        let h = self.h();
        if cut == h {
            return Ok(true);
        }
        if self.cfg.ceremony.allow_inexact_cut || self.cfg.ceremony.simulate_freeze {
            self.journal.evidence(
                State::Frozen,
                json!({"inexact_cut": {"h": h, "cut": cut, "offset": cut as i64 - h as i64,
                       "allowed_by": if self.cfg.ceremony.simulate_freeze { "simulate_freeze" } else { "allow_inexact_cut" },
                       "note": "REHEARSAL ONLY: producers cutting at different heights would diverge"}}),
            )?;
            return Ok(true);
        }
        self.abort(
            "snapshot is not at H: the cut must be exactly the event's H",
            json!({"h": h, "snapshot_height": cut}),
        )?;
        Ok(false)
    }

    /// `schedule_at_h` (producer mode): the snapshot was scheduled at ARM to
    /// land at exactly H. Wait for H to finalize (LIB >= H — empty blocks
    /// keep coming per R1), pin H's block id from the chain, and pick up the
    /// file nodeos writes as `snapshot-<block_id_at_H>.bin` in snapshot.dir.
    /// Returns Ok(None) after aborting (deadline expiry).
    fn await_scheduled_snapshot(&mut self) -> Result<Option<crate::ops::SnapshotResult>, String> {
        let h = self.h();
        let deadline = self.ops.now_ms() + self.cfg.source.snapshot_timeout_secs * 1000;
        let mut last_heartbeat = 0u64;
        loop {
            let info = self.ops.source_info()?;
            if info.last_irreversible_block_num >= h {
                break;
            }
            if self.coordinator_aborted(false) {
                self.abort("coordinator aborted the event (signed) while waiting for H to finalize",
                    json!({"h": h, "lib": info.last_irreversible_block_num}))?;
                return Ok(None);
            }
            let now = self.ops.now_ms();
            if now > deadline {
                self.abort(
                    "scheduled snapshot: H did not finalize before snapshot_timeout",
                    json!({"h": h, "lib": info.last_irreversible_block_num}),
                )?;
                return Ok(None);
            }
            if now.saturating_sub(last_heartbeat) >= 30_000 {
                self.journal.evidence(
                    State::Frozen,
                    json!({"awaiting_h_final": h, "lib": info.last_irreversible_block_num}),
                )?;
                last_heartbeat = now;
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        }
        let (id_h, _ts) = self.ops.source_block_id(h)?;
        let dir = self
            .cfg
            .snapshot
            .dir
            .clone()
            .expect("schedule_at_h validated snapshot.dir");
        let expected = dir.join(format!("snapshot-{id_h}.bin"));
        let waited_from = self.ops.now_ms();
        while !expected.exists() {
            if self.ops.now_ms() > deadline {
                self.abort(
                    "scheduled snapshot file did not appear before snapshot_timeout",
                    json!({"expected": expected.display().to_string()}),
                )?;
                return Ok(None);
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        }
        self.journal.evidence(
            State::Frozen,
            json!({
                "scheduled_snapshot_file": expected.display().to_string(),
                "file_wait_ms": self.ops.now_ms().saturating_sub(waited_from),
                "pinned_to_h": h,
            }),
        )?;
        Ok(Some(crate::ops::SnapshotResult {
            snapshot_name: expected.display().to_string(),
            head_block_num: h,
            head_block_id: id_h,
        }))
    }

    /// FROZEN: snapshot while still producing (nodeos writes it when the cut
    /// block finalizes), THEN pause, then quiescence, then pin + audit.
    fn step_frozen(&mut self) -> Result<(), String> {
        if self.cfg.ceremony.mode == Mode::Api {
            return self.step_frozen_api();
        }
        // Idempotent resume: if a crashed run left the producer paused, the
        // chain cannot finalize a snapshot block — unpause first (writes are
        // still frozen by the hook, so re-produced blocks stay empty).
        if self.ops.producer_paused()? {
            self.ops.resume()?;
            self.journal.evidence(
                State::Frozen,
                json!({"resumed_production_for_snapshot": true}),
            )?;
        }
        let started = self.ops.now_ms();
        let snap = if self.scheduled {
            match self.await_scheduled_snapshot()? {
                Some(s) => s,
                None => return Ok(()), // aborted inside, with evidence
            }
        } else {
            match self.ops.create_snapshot() {
                Ok(s) => s,
                Err(e) => {
                    self.abort("create_snapshot failed", json!({"error": e}))?;
                    return Ok(());
                }
            }
        };
        let snapshot_wall_ms = self.ops.now_ms().saturating_sub(started);
        if !self.cut_is_exact(snap.head_block_num)? {
            return Ok(()); // aborted inside (production never paused)
        }

        // Stop production and require quiescence (R4: catches producers that
        // ignored the pause and late blocks arriving over p2p).
        self.ops.pause()?;
        if !self.ops.producer_paused()? {
            self.abort("pause did not take effect", json!({}))?;
            return Ok(());
        }
        // Stand-in rehearsals: emulate "every producer paused" (e.g. sever
        // p2p on a live-syncing replica) so the quiescence window can pass.
        if let Some(cmd) = &self.cfg.source.quiesce_cmd {
            match self.ops.run_hook(cmd) {
                Ok(o) => self
                    .journal
                    .evidence(State::Frozen, json!({"quiesce_cmd": o}))?,
                Err(e) => {
                    self.abort("quiesce_cmd failed", json!({"error": e}))?;
                    return Ok(());
                }
            }
        }
        let mut stable = 0u32;
        let mut head = 0u64;
        let mut late = 0u64;
        let quiesce_from = self.ops.now_ms();
        let quiesce_deadline = quiesce_from.saturating_add(self.cfg.ceremony.quiescence_timeout_secs.saturating_mul(1000));
        let at_pause = loop {
            self.ops.sleep_ms(self.cfg.poll_ms);
            let i = self.ops.source_info()?;
            if i.head_block_num == head {
                stable += 1;
                if stable >= self.cfg.ceremony.quiescence_polls {
                    break i;
                }
            } else {
                if head != 0 {
                    late += 1;
                    // Journal the first few; a producer that never stops would
                    // otherwise write one line per poll until the deadline.
                    if late <= 10 {
                        self.journal.evidence(
                            State::Frozen,
                            json!({"late_block_after_pause": i.head_block_num}),
                        )?;
                    }
                }
                stable = 0;
                head = i.head_block_num;
            }
            if self.ops.now_ms() > quiesce_deadline {
                self.abort(
                    "source did not stop producing after the pause (quiescence_timeout_secs)",
                    json!({"head": i.head_block_num, "late_blocks": late,
                           "waited_ms": self.ops.now_ms().saturating_sub(quiesce_from),
                           "quiescence_timeout_secs": self.cfg.ceremony.quiescence_timeout_secs}),
                )?;
                return Ok(());
            }
        };

        // Pin the cut to the snapshot's block and cross-check it against the
        // chain's view of that height (fork-at-the-cut detection, R4).
        let cut_height = snap.head_block_num;
        let (chain_view_id, cut_block_time) = self.ops.source_block_id(cut_height)?;
        if !snap.head_block_id.eq_ignore_ascii_case(&chain_view_id) {
            self.abort(
                "snapshot block id != chain block id at cut height (fork at the cut?)",
                json!({"snapshot_block_id": snap.head_block_id, "chain_block_id": chain_view_id}),
            )?;
            return Ok(());
        }
        if let Some(expected) = &self.cfg.ceremony.chain_id {
            if !at_pause.chain_id.eq_ignore_ascii_case(expected) {
                self.abort(
                    "source chain_id changed mid-ceremony",
                    json!({"seen": at_pause.chain_id, "expected": expected}),
                )?;
                return Ok(());
            }
        }

        // Burn-off audit (R2): blocks after the cut, up to the pause head,
        // are outside the migrated state. With writes frozen they must be
        // empty. Fail closed: a transaction here would be accepted on the
        // source chain and missing from the migrated state, and a block we
        // cannot read is not evidence that it was empty.
        let mut burnoff_txs = 0u64;
        let mut burnoff_nonempty: Vec<serde_json::Value> = Vec::new();
        for n in (cut_height + 1)..=at_pause.head_block_num {
            match self.ops.source_block_tx_count(n) {
                Ok(0) => {}
                Ok(count) => {
                    burnoff_txs += count;
                    burnoff_nonempty.push(json!({"block": n, "transactions": count}));
                }
                Err(e) => {
                    self.abort(
                        "burn-off audit could not read a post-cut block",
                        json!({"block": n, "cut_height": cut_height, "error": e}),
                    )?;
                    return Ok(());
                }
            }
        }
        if burnoff_txs > 0 {
            self.abort(
                "transactions landed after the cut and would be missing from the migrated state",
                json!({"cut_height": cut_height, "burnoff_transactions": burnoff_txs,
                       "blocks": burnoff_nonempty,
                       "fix": "close every admission path (API edge, p2p, producer) before the cut, then re-run"}),
            )?;
            return Ok(());
        }

        let host_path = self.cfg.map_snapshot_path(&snap.snapshot_name);
        if !host_path.exists() {
            self.abort(
                "snapshot file not found on host",
                json!({"reported": snap.snapshot_name, "mapped": host_path.display().to_string()}),
            )?;
            return Ok(());
        }
        let size = std::fs::metadata(&host_path).map(|m| m.len()).unwrap_or(0);
        self.cut_height = Some(cut_height);
        self.cut_block_id = Some(snap.head_block_id.clone());
        self.last_source_block_time = Some(cut_block_time.clone());
        self.snapshot_file = Some(host_path.display().to_string());
        self.state = State::Snapshotted;
        self.journal.transition(
            State::Snapshotted,
            json!({
                "snapshot_file": host_path.display().to_string(),
                "reported_name": snap.snapshot_name,
                "size_bytes": size,
                "snapshot_wall_ms": snapshot_wall_ms,
                "cut_height": cut_height,
                "cut_block_id": snap.head_block_id,
                "last_source_block_time": cut_block_time,
                "head_at_pause": at_pause.head_block_num,
                "burnoff_blocks": at_pause.head_block_num - cut_height,
                "burnoff_transactions": burnoff_txs,
                "quiescence_polls": self.cfg.ceremony.quiescence_polls,
            }),
        )?;
        Ok(())
    }

    /// SNAPSHOTTED (upstream backend): drive the official #61 pipeline —
    /// export.sh (pinned Leap -> SHiP full-state log) -> xpr_import_check
    /// (-> Arena checkpoint) — and verify with upstream's OWN tools:
    /// xpr_19_table_compare (the gate) + xpr_state_fingerprint (the
    /// journaled, golden-comparable whole-state root). Every artifact is
    /// bound back to the ceremony's pinned cut (snapshot sha256, cut block
    /// id, cut height). The fork importer is not part of verification here;
    /// `[upstream] fork_audit = true` may journal it as a labeled dev extra.
    /// The upstream verification of the journaled cut snapshot: sha256, the #61 pipeline, the advisory scan, the fork
    /// audit and the boot artifacts, journaled as evidence under `stage`. `Ok(Err((reason, detail)))` changed no state:
    /// the caller aborts (SNAPSHOTTED) or refuses (`join --reverify`, rc.25 F5). Outer Err = journal failure.
    fn upstream_verify(&mut self, stage: State) -> Result<Result<UpstreamVerified, (String, serde_json::Value)>, String> {
        let Some(file) = self.snapshot_file.clone() else {
            return Ok(Err(("no cut snapshot journaled".into(), json!({}))));
        };
        let path = std::path::PathBuf::from(file);
        let up = self.cfg.upstream.clone().expect("validated: upstream section");
        let started = self.ops.now_ms();
        let (sha256, file_size) = match verify::sha256_file(&path) {
            Ok(v) => v,
            Err(e) => return Ok(Err(("cannot hash cut snapshot".into(), json!({"error": e})))),
        };
        if let Some(expected) = &self.cfg.snapshot.expected_sha256 {
            if !sha256.eq_ignore_ascii_case(expected) {
                return Ok(Err(("snapshot sha256 mismatch vs ceremony manifest".into(),
                    json!({"computed": sha256, "expected": expected}))));
            }
        }
        let (Some(cut_height), Some(cut_block_id)) = (self.cut_height, self.cut_block_id.clone()) else {
            return Ok(Err(("no cut height / cut block id journaled".into(), json!({}))));
        };
        let chain_id = self.chain_id.clone().unwrap_or_default();
        let outcome = {
            let journal = &mut self.journal;
            let (cfg, ops) = (self.cfg, self.ops);
            let mut last_coord = self.last_coord_check_ms;
            let out = upstream::run_pipeline(
                &up,
                self.ops,
                &path,
                &sha256,
                cut_height,
                &cut_block_id,
                &chain_id,
                |evidence| {
                    let _ = journal.evidence(stage, evidence);
                },
                // A signed abort is honoured between steps and while each tool runs (it kills
                // the tool), not only at the fleet gate after VERIFIED.
                |force| coordinator_abort_poll(cfg, ops, &mut last_coord, force),
            );
            self.last_coord_check_ms = last_coord;
            out
        };
        let outcome = match outcome {
            Ok(o) => o,
            Err(upstream::PipelineError::Aborted { during }) => {
                let event_id = self.cfg.coordination.as_ref().and_then(|c| c.event_id.clone());
                return Ok(Err(("coordinator aborted the event (signed) before ignition".into(),
                    json!({"event_id": event_id, "during": format!("upstream verification ({during})"),
                           "verify_wall_ms": self.ops.now_ms().saturating_sub(started)}))));
            }
            Err(upstream::PipelineError::Failed(e)) => {
                return Ok(Err(("upstream verification failed".into(), json!({"error": e}))));
            }
        };
        // rc.27: the protocol upgrade schedule the new chain will run under (its hash is fleet evidence).
        let protocol = match upstream::ProtocolSchedule::load(up.protocol_upgrades_file.as_deref())
            .and_then(|s| s.check_against_cut(cut_height, up.protocol_upgrade_margin_blocks).map(|_| s)) {
            Ok(p) => p,
            Err(e) => return Ok(Err(("protocol upgrade schedule refused".into(), json!({"error": e})))),
        };
        // Advisory stubbed-intrinsic scan of the actual cut (read-only audit
        // over the deployed code objects; backend-independent).
        self.advisory_scan(&path, stage)?;
        // Dev/audit extra, clearly labeled and NEVER a gate: the fork
        // importer's dual-arena fingerprints over the same cut.
        if up.fork_audit {
            match verify::verify_snapshot(&path, self.cfg.ceremony.import_cpu_scale) {
                Ok(o) => {
                    let roots: serde_json::Map<String, serde_json::Value> = o
                        .roots
                        .iter()
                        .map(|(n, r)| (n.clone(), json!(format!("{r:016x}"))))
                        .collect();
                    self.journal.evidence(
                        stage,
                        json!({"fork_audit": {
                            "note": "release-validation audit only — not an operator step, not a gate",
                            "fingerprints": roots,
                        }}),
                    )?;
                }
                Err(e) => self.journal.evidence(
                    stage,
                    json!({"fork_audit_error": e, "advisory": true}),
                )?,
            }
        }
        // Boot artifacts for ignition from the checkpoint (only when ignition is configured; a
        // verify-only run stops after VERIFIED). Built and bound to the cut BEFORE VERIFIED, while
        // the source still serves the cut block: any failure here aborts and resumes the source.
        let mut boot = serde_json::Value::Null;
        let mut boot_hashes = (None, None, None);
        if up.ignite_configured(&self.cfg.target) {
            let rpc = up.source_block_rpc_url.as_deref();
            let block = match self.ops.source_block(cut_height, rpc) {
                Ok(b) => b,
                Err(e) => {
                    return Ok(Err(("cannot fetch the full source cut block for the boot manifest anchor".into(),
                        json!({"error": e, "cut_height": cut_height,
                               "fix": "the source RPC (or upstream.source_block_rpc_url) must serve get_block at the cut"}))));
                }
            };
            match upstream::build_boot_artifacts(&up, &outcome, &block, cut_height, &cut_block_id, &chain_id) {
                Ok(a) => {
                    boot = json!({
                        "manifest": a.manifest.display().to_string(),
                        "genesis": a.genesis.display().to_string(),
                        "chain_config": a.chain_config.display().to_string(),
                        "source_block_bytes": a.source_block_bytes,
                        "source_block_receipts": a.source_block_receipts,
                        "anchor": "full packed source cut block (all receipts); computed id == cut block id",
                    });
                    boot_hashes = (Some(a.manifest_sha256), Some(a.genesis_sha256), Some(a.chain_config_sha256));
                }
                Err(e) => return Ok(Err(("cannot build the upstream boot artifacts".into(), json!({"error": e})))),
            }
        }
        self.boot_hashes = boot_hashes.clone();
        let table_compare = match (&outcome.compare_stdout, outcome.compare_allowed_mismatch.is_empty()) {
            (None, _) => "not configured".to_string(),
            (Some(_), true) => "MATCH".to_string(),
            (Some(_), false) => format!(
                "MISMATCH ALLOWED BY REHEARSAL OVERRIDE ({}) — not a valid verification for a real cut",
                outcome.compare_allowed_mismatch.join(", ")
            ),
        };
        Ok(Ok(UpstreamVerified { sha256, file_size, outcome, boot, boot_hashes, table_compare, cut_height, cut_block_id, chain_id, started, protocol }))
    }

    fn step_snapshotted_upstream(&mut self) -> Result<(), String> {
        let v = match self.upstream_verify(State::Snapshotted)? {
            Ok(v) => v,
            Err((reason, detail)) => {
                self.abort(&reason, detail)?;
                return Ok(());
            }
        };
        let UpstreamVerified { sha256, file_size, outcome, boot, boot_hashes, table_compare, cut_height, cut_block_id, chain_id, started, protocol } = v;
        if self.coordinator_aborted(true) {
            let event_id = self.cfg.coordination.as_ref().and_then(|c| c.event_id.clone());
            self.abort(
                "coordinator aborted the event (signed) before ignition",
                json!({"event_id": event_id, "during": "upstream verification (boot artifacts)"}),
            )?;
            return Ok(());
        }
        self.sha256 = Some(sha256.clone());
        self.state = State::Verified;
        self.journal.transition(
            State::Verified,
            json!({
                "boot": boot,
                "boot_manifest_sha256": boot_hashes.0,
                "boot_genesis_sha256": boot_hashes.1,
                "boot_chain_config_sha256": boot_hashes.2,
                "compare_allowed_mismatch": outcome.compare_allowed_mismatch,
                "rehearsal_overrides": self.cfg.rehearsal_overrides(),
                // What the fleet gate compares across producers (beacon fingerprints_digest).
                "fingerprints": outcome.state_root.as_ref().map(|r| json!({"upstream_state_root": r})),
                "verify_backend": "upstream (#61: export.sh -> xpr_import_check; gates: \
                                   xpr_19_table_compare + manifest bindings)",
                "sha256": sha256,
                "size_bytes": file_size,
                "chain_id": chain_id,
                "cut_height": cut_height,
                "cut_block_id": cut_block_id,
                "ship_log": outcome.ship_log.display().to_string(),
                "export_manifest": outcome.manifest_env,
                "export_reused": outcome.export_reused,
                "checkpoint": outcome.checkpoint_path.display().to_string(),
                "checkpoint_sha256": outcome.checkpoint_sha256,
                "checkpoint_revision": outcome.checkpoint_revision,
                "table_compare": table_compare,
                "state_root": outcome.state_root,
                "protocol_schedule_hash": protocol.hash,
                "protocol_upgrades": protocol.to_json()["protocol_upgrades"],
                "verify_wall_ms": self.ops.now_ms().saturating_sub(started),
            }),
        )?;
        Ok(())
    }

    /// SNAPSHOTTED: verify — sha256, dual-import fingerprints, goldens.
    fn step_snapshotted(&mut self) -> Result<(), String> {
        if self.cfg.ceremony.import_backend == ImportBackend::Upstream {
            return self.step_snapshotted_upstream();
        }
        let path = std::path::PathBuf::from(
            self.snapshot_file.clone().expect("snapshot file recorded"),
        );
        let started = self.ops.now_ms();
        let outcome = match verify::verify_snapshot(&path, self.cfg.ceremony.import_cpu_scale) {
            Ok(o) => o,
            Err(e) => {
                self.abort("verification failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        // File-level golden (strict; nodeos-version-pinned — finding R3).
        if let Some(expected) = &self.cfg.snapshot.expected_sha256 {
            if !outcome.sha256.eq_ignore_ascii_case(expected) {
                self.abort(
                    "snapshot sha256 mismatch vs ceremony manifest",
                    json!({"computed": outcome.sha256, "expected": expected}),
                )?;
                return Ok(());
            }
        }
        // The snapshot must be OF the pinned cut and OF the source chain.
        let cut_height = self.cut_height.expect("cut pinned");
        if outcome.head_block_num != cut_height {
            self.abort(
                "imported head != pinned cut height",
                json!({"imported": outcome.head_block_num, "cut_height": cut_height}),
            )?;
            return Ok(());
        }
        if let Some(chain_id) = &self.chain_id {
            if !outcome.chain_id.eq_ignore_ascii_case(chain_id) {
                self.abort(
                    "imported chain_id != source chain_id",
                    json!({"imported": outcome.chain_id, "source": chain_id}),
                )?;
                return Ok(());
            }
        }
        if let Some(cut_id) = &self.cut_block_id {
            if !outcome.head_block_id.eq_ignore_ascii_case(cut_id) {
                self.abort(
                    "imported head block id != pinned cut block id",
                    json!({"imported": outcome.head_block_id, "cut_block_id": cut_id}),
                )?;
                return Ok(());
            }
        }
        // State-level goldens: verify (multi-BP) or capture (first node).
        let mut golden_mode = "none";
        if let Some(golden_path) = &self.cfg.snapshot.golden_roots {
            let text = std::fs::read_to_string(golden_path)
                .map_err(|e| format!("read goldens: {e}"))?;
            let goldens = verify::parse_goldens(&text)?;
            if let Err(e) = verify::compare_goldens(&outcome.roots, &goldens) {
                self.abort("fingerprints do not match goldens", json!({"diff": e}))?;
                return Ok(());
            }
            golden_mode = "verified";
        } else if let Some(capture_path) = &self.cfg.snapshot.capture_roots {
            std::fs::write(
                capture_path,
                verify::format_goldens(&outcome, self.cfg.ceremony.import_cpu_scale),
            )
            .map_err(|e| format!("write captured goldens: {e}"))?;
            golden_mode = "captured";
        }
        // Stage the verified file where the pulsevm chain config expects it. The artifact's
        // identity is journaled FIRST, so a crash mid-copy is recognizable as our own file.
        self.journal.evidence(State::Snapshotted, json!({"staged_artifact": {
            "sha256": outcome.sha256, "cut_height": outcome.head_block_num,
            "path": self.cfg.snapshot.staged_path.display().to_string()}}))?;
        self.staged_sha256 = Some(outcome.sha256.clone());
        if path != self.cfg.snapshot.staged_path {
            std::fs::copy(&path, &self.cfg.snapshot.staged_path)
                .map_err(|e| format!("stage snapshot: {e}"))?;
            let (staged_sha, _) = verify::sha256_file(&self.cfg.snapshot.staged_path)?;
            if staged_sha != outcome.sha256 {
                self.abort("staged copy sha256 mismatch", json!({"staged": staged_sha}))?;
                return Ok(());
            }
        }
        // Advisory stubbed-intrinsic scan of the ACTUAL cut snapshot: which
        // contracts reference host functions PulseVM stubs. Journaled table,
        // never a gate.
        self.advisory_scan(&path, State::Snapshotted)?;
        // Upstream alignment (MetalBlockchain/pulsevm#61): if the official
        // `xpr_state_fingerprint` tool is staged on this box, run it alongside
        // our 19-table check and journal its report verbatim next to ours.
        // Advisory, never a gate; a missing binary is a journaled no-op.
        let upstream_fingerprint = self
            .cfg
            .snapshot
            .upstream_fingerprint_bin
            .as_ref()
            .map(|bin| {
                verify::run_upstream_fingerprint(
                    bin,
                    &self.cfg.snapshot.upstream_fingerprint_args,
                    &path,
                    &self.cfg.snapshot.staged_path,
                )
            });
        self.sha256 = Some(outcome.sha256.clone());
        self.state = State::Verified;
        let roots: serde_json::Map<String, serde_json::Value> = outcome
            .roots
            .iter()
            .map(|(n, r)| (n.clone(), json!(format!("{r:016x}"))))
            .collect();
        let mut verified_payload = json!({
                "sha256": outcome.sha256,
                "size_bytes": outcome.file_size,
                "chain_id": outcome.chain_id,
                "cut_height": outcome.head_block_num,
                "cut_block_id": outcome.head_block_id,
                "import_cpu_scale": self.cfg.ceremony.import_cpu_scale,
                "fingerprints": roots,
                "golden_mode": golden_mode,
                "dual_import": "identical",
                "verify_wall_ms": self.ops.now_ms().saturating_sub(started),
                "staged_path": self.cfg.snapshot.staged_path.display().to_string(),
                "accounts": outcome.report.accounts,
                "code_objects": outcome.report.code_objects,
                "permissions": outcome.report.permissions.written,
        });
        if let Some(upstream) = &upstream_fingerprint {
            verified_payload["upstream_fingerprint"] = serde_json::to_value(upstream)
                .unwrap_or_else(|_| json!({"status": "serialize_error"}));
        }
        self.journal.transition(State::Verified, verified_payload)?;
        Ok(())
    }

    /// The `{name}` placeholders the upstream ignition binds once the chain exists.
    fn bind_target_vars(&mut self) {
        let (Some(bid), Some(up), Some(h)) = (self.target_blockchain_id.clone(), self.cfg.upstream.as_ref(), self.cut_height) else {
            return;
        };
        let (manifest, genesis, chain_config) = upstream::boot_paths(up, h);
        let checkpoint = up.work_dir.join(format!("checkpoint-{h}.bin"));
        if let Some(sid) = self.target_subnet_id.clone() {
            self.ops.bind_target(&[("subnet_id".into(), sid)]);
        }
        self.ops.bind_target(&[
            ("blockchain_id".into(), bid),
            ("chain_config".into(), chain_config.display().to_string()),
            ("genesis".into(), genesis.display().to_string()),
            ("manifest".into(), manifest.display().to_string()),
            ("checkpoint".into(), checkpoint.display().to_string()),
            ("cut_height".into(), h.to_string()),
        ]);
    }

    /// Producer mode, upstream ignition: can the key the target will sign with actually produce
    /// once the migrated system contract re-elects the schedule from `eosio/producers`? The
    /// signing key is the chain config base's `producer_key` (its public key; the private key is
    /// never journaled), which must also be the genesis `initial_key` (schedule v0); the producer
    /// is the chain config's `producer_name`. Ok(Ok(evidence)) / Ok(Err(problem)); None = not
    /// applicable (api mode, fork backend, verify-only).
    fn producer_key_check(&self) -> Option<Result<serde_json::Value, String>> {
        let up = self.cfg.upstream.as_ref()?;
        if self.cfg.ceremony.mode != Mode::Producer
            || self.cfg.ceremony.import_backend != ImportBackend::Upstream
            || !up.ignite_configured(&self.cfg.target)
        {
            return None;
        }
        let read = |p: &std::path::Path, what: &str| -> Result<serde_json::Value, String> {
            let t = std::fs::read_to_string(p).map_err(|e| format!("read {what} {}: {e}", p.display()))?;
            serde_json::from_str(&t).map_err(|e| format!("{what} {} is not JSON: {e}", p.display()))
        };
        Some((|| {
            let genesis = read(up.genesis_base.as_ref().expect("ignite configured"), "upstream.genesis_base")?;
            let initial = genesis["initial_key"].as_str().ok_or("upstream.genesis_base has no initial_key")?;
            let initial = crate::keys::parse_public_k1(initial).map_err(|e| format!("upstream.genesis_base initial_key: {e}"))?;
            let cc = match &up.chain_config_base {
                Some(p) => read(p, "upstream.chain_config_base")?,
                None => json!({}),
            };
            let producer = cc["producer_name"].as_str().ok_or(
                "upstream.chain_config_base has no producer_name: cannot tell which producer this node signs as                  (the migrated chain re-elects producers from eosio/producers at the first onblock)")?;
            let signing = match cc["producer_key"].as_str() {
                Some(k) => {
                    let p = crate::keys::public_of_private_k1(k).map_err(|e| format!("upstream.chain_config_base producer_key: {e}"))?;
                    if p != initial {
                        return Err(format!(
                            "the chain config producer_key (public {}) is not the genesis initial_key {}: PulseVM seeds                              schedule v0 with initial_key and signs with producer_key, so it could not produce at all",
                            crate::keys::format_public_k1(&p), crate::keys::format_public_k1(&initial)));
                    }
                    p
                }
                // No producer_key in the base (installed another way): the node signs for
                // schedule v0 with the genesis initial key, so that is what must be registered.
                None => initial,
            };
            let row = self.ops.source_producer(producer).map_err(|e| format!("cannot read eosio/producers for {producer} on the source: {e}"))?;
            crate::keys::check_registered_producer_key(row.as_ref(), producer, &signing)
        })())
    }

    /// Where the upstream ignition will put this node's signing key (the chain config built from
    /// `chain_config_base`) must be private, and the SHARED boot inputs (genesis base) must hold
    /// none. Checked at ARM, before anything freezes.
    fn signer_material_problems(&self) -> Vec<String> {
        let mut out = vec![];
        let Some(up) = self.cfg.upstream.as_ref().filter(|u| u.ignite_configured(&self.cfg.target)) else { return out };
        let read = |p: &std::path::Path| std::fs::read_to_string(p).ok().and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok());
        if let Some(g) = up.genesis_base.as_ref().and_then(|p| read(p)) {
            if crate::secrets::has_private_key(&g) {
                out.push("upstream.genesis_base contains a private key: the migration genesis is shared with every validator and chain creation (initial_key must be a PUBLIC key)".into());
            }
        }
        let holds_key = up.chain_config_base.as_ref().and_then(|p| read(p)).map(|v| crate::secrets::has_private_key(&v)).unwrap_or(false);
        if holds_key {
            for dir in [Some(up.work_dir.clone()), self.cfg.target.chain_config_dir.clone()].into_iter().flatten() {
                if dir.exists() {
                    if let Err(e) = crate::secrets::check_private_dir(&dir) {
                        out.push(format!("the chain config will hold this node's signing key: {e}"));
                    }
                }
            }
        }
        out
    }

    /// Upstream backend, before the fleet gate: refuse what cannot be ignited safely, journal what
    /// stays unsolved for mainnet, and re-check the boot artifacts against their VERIFIED hashes.
    /// Ok(false) after aborting (pre-ignition: the source is resumed).
    fn upstream_ignite_preflight(&mut self) -> Result<bool, String> {
        let up = self.cfg.upstream.clone().expect("validated: upstream section");
        let source_chain = self.chain_id.clone().unwrap_or_default();
        let pending = upstream::ignite_pending_reasons();
        if crate::config::is_xpr_mainnet(&source_chain) && !pending.is_empty() {
            self.abort(
                "upstream ignite refused for XPR MAINNET: a same-chain-id mainnet cutover still has unsolved \
                 prerequisites (verification completed with the official #61 tools)",
                json!({"remaining": pending}),
            )?;
            return Ok(false);
        }
        if !up.ignite_configured(&self.cfg.target) {
            self.abort(
                "upstream ignite not configured — verification completed with the official #61 tools; \
                 set upstream.genesis_base and target.create_chain_cmd to boot the target from the checkpoint",
                json!({"verify_only": true, "remaining_for_mainnet": pending}),
            )?;
            return Ok(false);
        }
        match self.producer_key_check() {
            Some(Ok(ev)) => self.journal.evidence(State::Verified, json!({"producer_key_check": ev}))?,
            Some(Err(problem)) => {
                self.abort("producer signing key check failed (pre-ignition)", json!({"problem": problem}))?;
                return Ok(false);
            }
            None => {}
        }
        self.journal.evidence(State::Verified, json!({
            "upstream_ignite_warnings": pending,
            "note": "unsolved for a same-chain-id MAINNET cutover; acceptable only on a rehearsal/test chain",
            "rehearsal_overrides": self.cfg.rehearsal_overrides(),
        }))?;
        eprintln!("WARNING (upstream ignite, not mainnet-ready): {}", pending.join(" | "));
        // The artifacts the target boots from must be exactly the ones verification produced.
        let h = self.cut_height.expect("cut pinned");
        let (manifest, genesis, chain_config) = upstream::boot_paths(&up, h);
        let mut changed = vec![];
        for (what, path, want) in [
            ("boot manifest", &manifest, &self.boot_hashes.0),
            ("migration genesis", &genesis, &self.boot_hashes.1),
            ("chain config", &chain_config, &self.boot_hashes.2),
        ] {
            let got = verify::sha256_file(path).map(|(h, _)| h).ok();
            if want.is_none() || got.as_deref() != want.as_deref() {
                changed.push(json!({"artifact": what, "path": path.display().to_string(), "verified": want, "now": got}));
            }
        }
        if !changed.is_empty() {
            self.abort("upstream boot artifacts missing or changed since VERIFIED", json!({"artifacts": changed}))?;
            return Ok(false);
        }
        Ok(true)
    }

    /// Upstream backend, after the fleet gate and BEFORE `ignite_started`: create the target chain
    /// on Metal (once: its id is journaled the moment it is known), bind the placeholders and
    /// install the chain config. Ok(false) after aborting (pre-ignition).
    fn upstream_create_chain(&mut self) -> Result<bool, String> {
        let up = self.cfg.upstream.clone().expect("validated: upstream section");
        let h = self.cut_height.expect("cut pinned");
        let (manifest, genesis, chain_config) = upstream::boot_paths(&up, h);
        if let Some(bid) = self.target_blockchain_id.clone() {
            self.journal.evidence(State::Verified, json!({"create_chain": "reused (journaled by a previous run)", "blockchain_id": bid}))?;
        } else if self.create_chain_started {
            // Past the point of no return (abort() halts): never create a second chain.
            self.abort(
                "create_chain_cmd was started by a previous run but no blockchain id was journaled: a chain may \
                 exist on Metal. Refusing to create a second one",
                json!({"fix": "find the chain on the P-Chain (by its genesis hash); retire it or journal its id by hand, then start a new ceremony"}),
            )?;
            return Ok(false);
        } else {
            // Last pre-boundary check: a signed abort (or an unreachable relay) stops here, while
            // resuming the source is still safe.
            if !self.require_no_abort("chain creation")? {
                return Ok(false);
            }
            let cmd = self.cfg.target.create_chain_cmd.clone().expect("validated: ignite configured");
            let cmd = crate::ops::expand_placeholders(&cmd, &[
                ("genesis".into(), genesis.display().to_string()),
                ("genesis_sha256".into(), self.boot_hashes.1.clone().unwrap_or_default()),
                ("chain_config".into(), chain_config.display().to_string()),
                ("manifest".into(), manifest.display().to_string()),
                ("checkpoint".into(), up.work_dir.join(format!("checkpoint-{h}.bin")).display().to_string()),
                ("cut_height".into(), h.to_string()),
            ]);
            // Intent FIRST, and it is the point of no return: from here every outcome (failure, an
            // answer without an id, a lost answer, a crash) halts for an operator decision.
            self.journal.evidence(State::Verified, json!({"side_effect": "create_chain", "cmd": cmd,
                "point_of_no_return": "chain creation may activate a target; any outcome from here halts instead of resuming the source"}))?;
            self.create_chain_started = true;
            let out = match self.ops.run_hook(&cmd) {
                Ok(o) => o,
                Err(e) => {
                    self.abort("create_chain_cmd failed after it started", json!({"error": e,
                        "note": "the chain may nevertheless exist on Metal (and start on validators tracking its subnet): operator decision"}))?;
                    return Ok(false);
                }
            };
            let (bid, subnet) = match upstream::parse_create_chain_output(&out) {
                Ok(v) => v,
                Err(e) => {
                    self.abort("create_chain_cmd output has no blockchain id", json!({"error": e, "output": out}))?;
                    return Ok(false);
                }
            };
            self.journal.evidence(State::Verified, json!({"target_blockchain_id": bid, "target_subnet_id": subnet,
                "create_chain_output": out}))?;
            self.target_blockchain_id = Some(bid);
            self.target_subnet_id = subnet;
        }
        self.bind_target_vars();
        let bid = self.target_blockchain_id.clone().expect("set above");
        if let Some(dir) = self.cfg.target.chain_config_dir.clone() {
            let dest = dir.join(&bid).join("config.json");
            // The chain config may hold this node's signing key: installed 0600 from creation, into
            // a directory nobody else can write (a fresh per-chain dir is created 0700).
            let installed = std::fs::read(&chain_config)
                .map_err(|e| format!("read {}: {e}", chain_config.display()))
                .and_then(|bytes| {
                    let private = serde_json::from_slice::<serde_json::Value>(&bytes)
                        .map(|v| crate::secrets::has_private_key(&v))
                        .unwrap_or(true);
                    let parent = dest.parent().expect("has parent");
                    let mut b = std::fs::DirBuilder::new();
                    b.recursive(true);
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::DirBuilderExt;
                        if private {
                            b.mode(0o700);
                        }
                    }
                    b.create(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
                    crate::secrets::write_file(&dest, &bytes, private)
                })
                .and_then(|_| verify::sha256_file(&dest).map(|(h, _)| h));
            match installed {
                Ok(h) if Some(&h) == self.boot_hashes.2.as_ref() => {
                    self.journal.evidence(State::Verified, json!({"chain_config_installed": dest.display().to_string(), "sha256": h}))?;
                    if let Err(e) = self.install_protocol_schedule(&dir, &bid)? {
                        self.abort("could not install the protocol upgrade schedule for the target", e)?;
                        return Ok(false);
                    }
                }
                other => {
                    self.abort("could not install the chain config for the target", json!({
                        "dest": dest.display().to_string(), "result": format!("{other:?}")}))?;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// rc.23 `pulse-cutover join --event <id>`: a producer that is STRANDED or ABORTED while a quorum of
    /// the event's roster went LIVE on target chain X tracks and ignites X (no chain creation) and goes
    /// through post_ignite / LIVE like any producer. Refused unless: the upstream backend in producer
    /// mode, this event, this node reached VERIFIED with the same snapshot hash, fingerprints and
    /// migration genesis the LIVE quorum reports, its boot artifacts are unchanged, and its source did
    /// not produce past its journaled pause head nor carry any transaction after H. Everything checked
    /// is journaled; from the `join` record on, a failure HALTS (never resumes the old chain).
    pub fn join(&mut self, event_id: &str) -> Result<State, String> {
        self.join_with(event_id, false)
    }

    /// `join`, optionally re-verifying first (rc.25 F5, `join --reverify`): a producer whose own verification failed
    /// or never finished (no VERIFIED evidence) re-runs the upstream verification on its journaled cut snapshot, and
    /// joins only if the result (snapshot sha256, state fingerprints, migration genesis — all three required) equals
    /// the LIVE quorum's. A failed re-verification changes nothing on either chain.
    pub fn join_with(&mut self, event_id: &str, reverify: bool) -> Result<State, String> {
        self.cfg.ensure_ceremony_profile()?;
        let co = self.cfg.coordination.clone().ok_or("join needs the event's [coordination] config (the one `await` derived)")?;
        if co.event_id.as_deref() != Some(event_id) {
            return Err(format!("this config is for event {:?}, not {event_id}", co.event_id));
        }
        if self.cfg.ceremony.import_backend != ImportBackend::Upstream || self.cfg.ceremony.mode != Mode::Producer
            || !self.cfg.upstream.as_ref().is_some_and(|u| u.ignite_configured(&self.cfg.target)) {
            return Err("join supports producer mode on the upstream backend with ignition configured \
                        (upstream.genesis_base + target.create_chain_cmd)".into());
        }
        if self.rollback_pending {
            return Err("an operator rollback is pending on this journal: finish it first".into());
        }
        if self.joining {
            // A join was committed (its `join` record is journaled) and the process died: finish it
            // instead of refusing (review #5). Its checks passed when it committed.
            return self.resume_join(event_id);
        }
        if !matches!(self.state, State::Stranded | State::Aborted) {
            return Err(format!("join is for a STRANDED or ABORTED ceremony; this journal is {} (HALTED: use unhalt)", self.state));
        }
        // rc.26 review M1: the mainnet refusal of rehearsal configs runs at load (configured chain_id) and at ARM; a
        // config edited after ARM must not bring one into a join either. The journal knows the source chain.
        if self.chain_id.as_deref().is_some_and(crate::config::is_xpr_mainnet)
            && (self.cfg.ceremony.rehearsal || !self.cfg.rehearsal_overrides().is_empty()) {
            return Err("join refused: this journal's source is XPR MAINNET and the config is a rehearsal or carries rehearsal \
                        overrides: refused for mainnet. Nothing was changed.".into());
        }
        if self.reached_ignited || self.target_blockchain_id.is_some() {
            return Err("this node already started its own ignition or chain creation: not a join (operator decision)".into());
        }
        let verified = self.sha256.is_some() && self.boot_hashes.0.is_some() && self.boot_hashes.1.is_some() && self.boot_hashes.2.is_some();
        // rc.27 review (review #3): a VERIFIED journal from an agent before rc.27 carries no protocol schedule hash, so it
        // cannot prove the same schedule as the LIVE quorum: it must re-verify (which records one) before joining.
        let legacy = verified && crate::beacon::journal_summary(&self.cfg.journal_path)["evidence"]["protocol_schedule_hash"].is_null();
        if legacy && !reverify {
            return Err("join refused: this ceremony VERIFIED with an agent before rc.27 (no protocol upgrade schedule hash \
                        journaled), so it cannot prove the LIVE quorum's schedule: run `join --reverify`".into());
        }
        let verified = verified && !legacy;
        let (mut ours_override, mut reverified) = (None, None);
        if verified && reverify {
            // rc.25 review #3: a re-verification rewrites the boot artifacts in place; on a node that verified they
            // are what its VERIFIED record binds. Same snapshot, same result: a plain join is the path.
            return Err("join --reverify is for a ceremony WITHOUT VERIFIED evidence; this one verified: use `join` without \
                        --reverify (its artifacts are checked against the LIVE quorum's)".into());
        }
        if !verified {
            if !reverify {
                return Err("this ceremony never reached VERIFIED (no snapshot hash / boot artifacts journaled): nothing to join with. \
                            If it took the cut snapshot (SNAPSHOTTED), `join --reverify` re-runs the verification on it first".into());
            }
            let (ours, record) = self.join_reverify()?;
            ours_override = Some(ours);
            reverified = Some(record);
        }
        // Until the `reverified` record is journaled (only after the LIVE quorum accepts the evidence), a refusal
        // leaves the journal as it was: the in-memory evidence is dropped again (rc.25 review #3).
        let forget = |m: &mut Self, e: String| -> String {
            if reverified.is_some() {
                m.sha256 = None;
                m.boot_hashes = (None, None, None);
            }
            e
        };
        let (Some(h), Some(cut_id), Some(sha)) = (self.cut_height, self.cut_block_id.clone(), self.sha256.clone()) else {
            return Err("this ceremony never reached VERIFIED (no cut / snapshot hash journaled): nothing to join with".into());
        };
        if self.boot_hashes.0.is_none() || self.boot_hashes.1.is_none() || self.boot_hashes.2.is_none() {
            return Err(forget(self, "no boot artifacts journaled at VERIFIED: nothing to join with".into()));
        }
        // 0. Writes frozen again (review #4): an ABORTED ceremony's on_abort reopened them on the old chain.
        //    on_freeze is re-run (it must be idempotent, like every hook) before any other join step;
        //    without it a join from ABORTED is refused. A STRANDED ceremony never reopened them.
        let refrozen = self.state == State::Aborted;
        if refrozen {
            let Some(hook) = self.cfg.hooks.on_freeze.clone() else {
                return Err(forget(self, "join refused: this ceremony ABORTED (on_abort reopened writes) and has no hooks.on_freeze to close \
                            them again first. Nothing was changed on either chain.".into()));
            };
            match self.ops.run_hook(&hook) {
                Ok(o) => self.journal.evidence(self.state, json!({"join_refreeze": o, "note": "writes frozen again before the join"}))?,
                Err(e) => {
                    self.journal.evidence(self.state, json!({"join_refreeze_error": e}))?;
                    let e = forget(self, format!("join refused: re-running on_freeze to close writes failed: {e}"));
                    return Err(self.join_refused_after_refreeze(e)?);
                }
            }
        }
        let (bid, sid, members, info, paused_head, paused, ours) = match self.join_checks(&co, h, ours_override) {
            Ok(x) => x,
            Err(e) if refrozen => {
                let e = forget(self, e);
                return Err(self.join_refused_after_refreeze(e)?);
            }
            Err(e) => return Err(forget(self, e)),
        };
        if let Some(record) = reverified {
            self.journal.evidence(self.state, record)?;
        }
        // 4. Commit to the join (point of no return), then continue as a producer that verified.
        let from = self.state;
        self.journal.evidence(self.state, json!({"join": {"event_id": event_id, "blockchain_id": bid, "subnet_id": sid,
            "live_members": members, "source_head": info.head_block_num, "head_at_pause": paused_head,
            "burnoff_transactions": 0, "source_paused": paused, "evidence": ours}}))?;
        if !paused {
            self.ops.pause()?;
            self.journal.evidence(self.state, json!({"join_paused_source": true}))?;
        }
        self.journal.evidence(self.state, json!({"side_effect": "join",
            "point_of_no_return": "joining an existing target chain: any failure from here halts"}))?;
        self.joining = true;
        self.create_chain_started = true;
        self.journal.evidence(self.state, json!({"target_blockchain_id": bid, "target_subnet_id": sid}))?;
        self.target_blockchain_id = Some(bid);
        self.target_subnet_id = sid;
        let _ = (sha, cut_id);
        self.join_to_verified(event_id, from)?;
        self.bind_target_vars();
        self.resumed = true;
        self.run()
    }

    /// `join --reverify` (rc.25 F5): the upstream verification again, on this node's journaled cut snapshot (only for a
    /// ceremony without VERIFIED evidence). Ok = the fleet evidence it produced (every field present) and the
    /// `reverified` record the caller journals once the LIVE quorum's evidence matches; Err = refused, nothing changed
    /// on either chain (only this node's verification work files were rewritten; no record binds them).
    fn join_reverify(&mut self) -> Result<(serde_json::Value, serde_json::Value), String> {
        if self.cut_height.is_none() || self.cut_block_id.is_none() {
            return Err("join --reverify: this ceremony never took the cut (no cut height / block id journaled): nothing to verify".into());
        }
        // Cheap first (rc.25 review #4): a source that produced past its pause head can never join; say so before
        // minutes of pipeline (and, from ABORTED, before writes are frozen again).
        if let (Some(paused), Ok(info)) = (self.head_at_pause, self.ops.source_info()) {
            if info.head_block_num > paused {
                return Err(format!("join refused: the source produced {} block(s) past the journaled pause head {paused} (head now {}): \
                    this node's old chain continued after H. Operator decision. Nothing was changed.", info.head_block_num - paused, info.head_block_num));
            }
        }
        match self.snapshot_file.as_deref() {
            Some(f) if std::path::Path::new(f).is_file() => {}
            Some(f) => return Err(format!("join --reverify: the journaled cut snapshot {f} is gone: nothing to verify")),
            None => return Err("join --reverify: no cut snapshot journaled (the ceremony stopped before SNAPSHOTTED)".into()),
        }
        self.journal.evidence(self.state, json!({"reverify_started": {"snapshot_file": self.snapshot_file, "cut_height": self.cut_height}}))?;
        let v = match self.upstream_verify(self.state)? {
            Ok(v) => v,
            Err((reason, detail)) => {
                self.journal.evidence(self.state, json!({"reverify_failed": {"reason": reason, "detail": detail}}))?;
                self.boot_hashes = (None, None, None);
                return Err(format!("join refused: re-verification failed: {reason} ({detail}). Nothing was changed on either chain \
                                    (only this node's verification work files were rewritten)."));
            }
        };
        // rc.26 F7 (fleet run d1): on PulseVM v1.0.0 every node's VERIFIED needs the rehearsal compare allowlist (#109),
        // so refusing it outright left a rehearsal with no route onto the LIVE chain. In a REHEARSAL ceremony it is
        // accepted when the LIVE quorum verified under the SAME set (compare_allowed_digest is part of the evidence the
        // join compares); outside one it refuses, and `join_with` refuses rehearsal configs on a mainnet journal.
        let allowed = json!(v.outcome.compare_allowed_mismatch);
        if !v.outcome.compare_allowed_mismatch.is_empty() && !self.cfg.ceremony.rehearsal {
            self.journal.evidence(self.state, json!({"reverify_failed": {"reason": "table compare mismatch allowed by a rehearsal override",
                "allowed": allowed}}))?;
            self.boot_hashes = (None, None, None);
            return Err("join refused: the re-verification's table compare only passed under a rehearsal override \
                        (compare_allowed_mismatch) and this is not a rehearsal ceremony: not a verification to join with".into());
        }
        let mut ours = v.fleet_evidence();
        if let Some(dg) = crate::beacon::compare_allowed_digest(&allowed) {
            ours["compare_allowed_digest"] = json!(dg);
        }
        if ["snapshot_sha256", "fingerprints_digest", "boot_genesis_sha256"].iter().any(|k| ours[*k].is_null()) {
            self.journal.evidence(self.state, json!({"reverify_failed": {"reason": "incomplete evidence", "evidence": ours}}))?;
            self.boot_hashes = (None, None, None);
            return Err(format!("join refused: the re-verification did not produce all of snapshot sha256, state fingerprints and \
                                migration genesis ({ours}): the LIVE quorum's state cannot be matched. Nothing was changed on either chain."));
        }
        self.sha256 = Some(v.sha256.clone());
        // Journaled by the caller only once the LIVE quorum's evidence matched (review #3).
        let record = json!({"reverified": true, "evidence": ours, "compare_allowed_mismatch": allowed,
            "protocol_schedule_hash": v.protocol.hash, "protocol_upgrades": v.protocol.to_json()["protocol_upgrades"],
            "rehearsal_overrides": self.cfg.rehearsal_overrides(),
            "sha256": v.sha256, "size_bytes": v.file_size, "cut_height": v.cut_height, "cut_block_id": v.cut_block_id, "chain_id": v.chain_id,
            "fingerprints": v.outcome.state_root.as_ref().map(|r| json!({"upstream_state_root": r})), "state_root": v.outcome.state_root,
            "table_compare": v.table_compare, "boot": v.boot, "boot_manifest_sha256": v.boot_hashes.0,
            "boot_genesis_sha256": v.boot_hashes.1, "boot_chain_config_sha256": v.boot_hashes.2,
            "checkpoint_sha256": v.outcome.checkpoint_sha256, "verify_wall_ms": self.ops.now_ms().saturating_sub(v.started)});
        Ok((ours, record))
    }

    /// Steps 1-3 of a join: unchanged boot artifacts, a unique LIVE quorum with our evidence, a source
    /// that did not move on after H. Changes nothing.
    #[allow(clippy::type_complexity)]
    fn join_checks(&mut self, co: &crate::config::Coordination, h: u64, ours_override: Option<serde_json::Value>)
        -> Result<(String, Option<String>, Vec<String>, crate::ops::ChainInfo, u64, bool, serde_json::Value), String> {
        let up = self.cfg.upstream.clone().expect("checked");
        let (manifest, genesis, chain_config) = upstream::boot_paths(&up, h);
        for (what, path, want) in [("boot manifest", &manifest, &self.boot_hashes.0), ("migration genesis", &genesis, &self.boot_hashes.1),
                                   ("chain config", &chain_config, &self.boot_hashes.2)] {
            let got = verify::sha256_file(path).map(|(x, _)| x).ok();
            if got.as_deref() != want.as_deref() {
                return Err(format!("join refused: the {what} ({}) changed since VERIFIED", path.display()));
            }
        }
        let ours = ours_override.unwrap_or_else(|| crate::beacon::journal_summary(&self.cfg.journal_path)["evidence"].clone());
        // Review LOW-1: the comparison only checks keys WE have; every one of the three that bind the state must be ours.
        if let Some(k) = ["snapshot_sha256", "fingerprints_digest", "boot_genesis_sha256", "protocol_schedule_hash"].into_iter().find(|k| ours[*k].is_null()) {
            return Err(format!("join refused: this node's verified evidence lacks {k}: the LIVE quorum's state cannot be matched"));
        }
        let status = self.fleet_status();
        let (bid, sid, members) = crate::fleet::join_view(status.as_ref(), co, &ours).map_err(|e| format!("join refused: {e}"))?;
        let info = self.ops.source_info().map_err(|e| format!("join refused: cannot read the source chain: {e}"))?;
        let paused_head = self.head_at_pause.ok_or("join refused: no journaled pause head (the source was never paused at the cut)")?;
        if info.head_block_num > paused_head {
            return Err(format!(
                "join refused: the source produced {} block(s) past the journaled pause head {paused_head} (head now {}): this node's \
                 old chain continued after H (it may carry writes the new chain does not have). Operator decision.",
                info.head_block_num - paused_head, info.head_block_num));
        }
        let mut txs = 0u64;
        for n in (h + 1)..=info.head_block_num {
            match self.ops.source_block_tx_count(n) {
                Ok(c) => txs += c,
                Err(e) => return Err(format!("join refused: the burn-off audit cannot read block {n}: {e}")),
            }
        }
        if txs > 0 {
            return Err(format!("join refused: {txs} transaction(s) on the source after H {h}"));
        }
        let paused = self.ops.producer_paused().map_err(|e| format!("join refused: producer API: {e}"))?;
        Ok((bid, sid, members, info, paused_head, paused, ours))
    }

    /// A join from ABORTED that re-ran on_freeze and was then refused (re-check #3): writes are now FROZEN on
    /// the old chain. Journaled as an incomplete rollback whose on_abort must run again, so `pulse-cutover
    /// rollback` reopens them; the message says so.
    fn join_refused_after_refreeze(&mut self, why: String) -> Result<String, String> {
        self.journal.evidence(self.state, json!({"join_refused_after_refreeze": why, "writes_frozen": true}))?;
        self.journal.evidence(self.state, json!({"rollback_incomplete": [
            "a refused join froze writes on the old chain again (on_freeze): `pulse-cutover rollback` reopens them (re-runs on_abort)"],
            "rollback_complete": false}))?;
        Ok(format!(
            "{why}\nWrites are now FROZEN on the old chain (the join re-ran on_freeze before its checks); the source \
             producer was not touched. Either fix the cause and run `pulse-cutover join --config <file> --event <id>` \
             again, or reopen writes on the old chain with `pulse-cutover rollback --config <file>` (it re-runs on_abort)."))
    }

    /// The VERIFIED transition a join continues from (carrying the verified evidence forward).
    fn join_to_verified(&mut self, event_id: &str, from: State) -> Result<(), String> {
        self.state = State::Verified;
        self.journal.transition(State::Verified, json!({"joined": true, "joined_from": from.as_str(), "event_id": event_id,
            "sha256": self.sha256, "cut_height": self.cut_height, "cut_block_id": self.cut_block_id, "chain_id": self.chain_id,
            "boot_manifest_sha256": self.boot_hashes.0, "boot_genesis_sha256": self.boot_hashes.1,
            "boot_chain_config_sha256": self.boot_hashes.2}))
    }

    /// Finish a join whose `join` record is journaled (crash-resume). If the process died before the
    /// target chain's id was journaled, the id is read again from the fleet (the same LIVE-quorum rule);
    /// then the VERIFIED transition (if missing) and the run continue. Never resumes the old chain.
    fn resume_join(&mut self, event_id: &str) -> Result<State, String> {
        self.journal.evidence(self.state, json!({"join_resumed": true, "state": self.state.as_str()}))?;
        if self.target_blockchain_id.is_none() {
            // The `join` record (journaled before the point of no return) names the chain: use it (re-check #3).
            if let Some((bid, sid)) = journaled_join_ids(self.journal.path()) {
                self.journal.evidence(self.state, json!({"join_resumed_ids_from": "the journaled join record"}))?;
                self.journal.evidence(self.state, json!({"target_blockchain_id": bid, "target_subnet_id": sid}))?;
                self.target_blockchain_id = Some(bid);
                self.target_subnet_id = sid;
            }
        }
        if self.target_blockchain_id.is_none() {
            let co = self.cfg.coordination.clone().expect("checked by join");
            let ours = crate::beacon::journal_summary(&self.cfg.journal_path)["evidence"].clone();
            let status = self.fleet_status();
            let (bid, sid, members) = crate::fleet::join_view(status.as_ref(), &co, &ours)
                .map_err(|e| format!("join resume: the target chain id was not journaled and cannot be re-read from the fleet ({e}); \
                                      this ceremony is past its join (it will not resume the old chain): retry when the relay shows the LIVE quorum"))?;
            self.journal.evidence(self.state, json!({"join_resumed_ids_from": members}))?;
            self.journal.evidence(self.state, json!({"target_blockchain_id": bid, "target_subnet_id": sid}))?;
            self.target_blockchain_id = Some(bid);
            self.target_subnet_id = sid;
        }
        if matches!(self.state, State::Stranded | State::Aborted) {
            let from = self.state;
            self.join_to_verified(event_id, from)?;
        }
        self.bind_target_vars();
        self.resumed = true;
        self.run()
    }

    /// VERIFIED: ignite the target and wait for it to present the source
    /// chain at the cut height.
    fn step_verified(&mut self) -> Result<(), String> {
        let upstream_backend = self.cfg.ceremony.import_backend == ImportBackend::Upstream;
        if upstream_backend && !self.upstream_ignite_preflight()? {
            return Ok(()); // aborted inside, with evidence
        }
        if !upstream_backend {
            // The same-chain-id gaps are PulseVM's, whatever imported the state: the fork backend
            // gets the same hard XPR-mainnet refusal as the upstream one.
            let pending = upstream::ignite_pending_reasons();
            if self.chain_id.as_deref().is_some_and(crate::config::is_xpr_mainnet) && !pending.is_empty() {
                self.abort("ignite refused for XPR MAINNET (fork backend): a same-chain-id mainnet cutover still has unsolved prerequisites",
                    json!({"remaining": pending}))?;
                return Ok(());
            }
        }
        if !self.joining && !self.fleet_gate()? {
            return Ok(()); // aborted inside, with evidence
        }
        if upstream_backend && !self.upstream_create_chain()? {
            return Ok(()); // aborted inside, with evidence
        }
        // Positive authorization right before ignition (fork backend: still pre-boundary, an abort
        // resumes the source; upstream: after chain creation, an abort here halts).
        // A join tracks a chain a quorum already runs LIVE: an abort is not honoured after ignition, and
        // the join checked the fleet itself.
        if !self.joining && !self.require_no_abort("ignition")? {
            return Ok(());
        }
        let started = self.ops.now_ms();
        // Point of no return (locally): journaled BEFORE the command runs. From here any failure
        // (ignite error, timeout, wrong or unverifiable lineage) seals instead of resuming the
        // source, because the target may already be running; recovery reads this record too.
        self.journal.evidence(State::Verified, json!({"side_effect": "ignite_started"}))?;
        self.reached_ignited = true;
        let output = match self.ops.ignite() {
            Ok(o) => o,
            Err(e) => {
                self.abort("ignition command failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        let deadline = started + self.cfg.target.quorum_timeout_secs * 1000;
        let cut_height = self.cut_height.expect("cut pinned");
        let mut observed_at_cut: Option<String> = None;
        let info = loop {
            if self.ops.now_ms() > deadline {
                self.abort(
                    "target chain did not come up before quorum timeout",
                    json!({"quorum_timeout_secs": self.cfg.target.quorum_timeout_secs}),
                )?;
                return Ok(());
            }
            if let Some(info) = self.ops.target_info()? {
                if info.head_block_num == cut_height {
                    observed_at_cut = Some(info.head_block_id.clone());
                }
                if info.head_block_num >= cut_height {
                    break info;
                }
                self.journal.evidence(
                    State::Verified,
                    json!({"target_head_below_cut": info.head_block_num}),
                )?;
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        };
        if let Some(chain_id) = self.chain_id.clone() {
            if !info.chain_id.eq_ignore_ascii_case(&chain_id) {
                if self.cfg.ceremony.rehearsal_allow_chain_id_change {
                    // REHEARSAL ONLY (refused for mainnet at load and at ARM): PulseVM v1.0.0 signs
                    // with metalgo's blockchain id. Both ids are journaled; LIVE carries them too.
                    self.journal.evidence(State::Verified, json!({
                        "accepted_target_chain_id": info.chain_id,
                        "rehearsal_chain_id_change": {
                            "source_chain_id": chain_id, "target_chain_id": info.chain_id,
                            "allowed_by": "ceremony.rehearsal_allow_chain_id_change",
                            "REHEARSAL_ONLY": "transactions must be signed for the TARGET chain_id; a real cut keeps the source chain_id",
                        }}))?;
                    eprintln!("WARNING (rehearsal override): target chain_id {} != source {chain_id}", info.chain_id);
                    self.accepted_target_chain_id = Some(info.chain_id.clone());
                } else {
                    self.abort(
                        "target chain_id != source chain_id",
                        json!({"target": info.chain_id, "source": chain_id,
                               "note": "a rehearsal on PulseVM v1.0.0 (signs with metalgo's blockchain id) needs ceremony.rehearsal_allow_chain_id_change = true"}),
                    )?;
                    return Ok(());
                }
            }
        }
        // Lineage at H: the target's block AT the cut must be the source's block at the cut
        // (height + chain_id alone would accept a target imported from a different fork/cut).
        let target_id_at_cut = match self.ops.target_block_id(cut_height)? {
            Some(id) => Some(id),
            None => observed_at_cut,
        };
        let cut_block_id = self.cut_block_id.clone().unwrap_or_default();
        let lineage = match &target_id_at_cut {
            Some(id) if id.eq_ignore_ascii_case(&cut_block_id) => "verified",
            Some(id) => {
                self.abort(
                    "target block id at the cut != source block id at the cut (wrong lineage)",
                    json!({"cut_height": cut_height, "target_block_id": id, "source_block_id": cut_block_id}),
                )?;
                return Ok(());
            }
            None if self.cfg.target.require_lineage_check => {
                self.abort(
                    "cannot verify the target's block id at the cut height (target RPC did not show block H)",
                    json!({"cut_height": cut_height,
                           "fix": "target RPC must answer pulsevm.getBlock(H), or set target.require_lineage_check = false (rehearsals only)"}),
                )?;
                return Ok(());
            }
            None => "UNVERIFIED (require_lineage_check = false)",
        };
        // rc.27: the running target must have loaded THIS event's protocol schedule and support its highest version
        // (a validator with another schedule, or an older binary, splits or stops the chain at activation).
        let protocol_check = match self.target_protocol_problem()? {
            Ok(v) => v,
            Err(detail) => {
                self.abort("the target's protocol upgrade schedule or supported version does not match this event", detail)?;
                return Ok(());
            }
        };
        self.reached_ignited = true;
        self.state = State::Ignited;
        self.journal.transition(
            State::Ignited,
            json!({
                "ignite_output": output,
                "target_chain_id": info.chain_id,
                "target_head": info.head_block_num,
                "target_head_id": info.head_block_id,
                "cut_height": cut_height,
                "target_block_id_at_cut": target_id_at_cut,
                "lineage_at_cut": lineage,
                "protocol_check": protocol_check,
                "ignite_wall_ms": self.ops.now_ms().saturating_sub(started),
            }),
        )?;
        Ok(())
    }

    /// rc.23, after ignition: a LOCAL liveness symptom (a gap in the sustained-LIVE window, the head
    /// not past the cut in time, a failing post_ignite / on_live hook) halted each BP on its own view
    /// in every fleet rehearsal run, leaving LIVE and HALTED BPs on one target chain. With a coordinated
    /// event this asks the fleet first: Ok(true) = keep waiting ("degraded", journaled and reported by
    /// the beacon) because a quorum of the roster reports the SAME target chain (common block after
    /// the cut, heads past it, the fleet's head still moving within `fleet_stall_secs`); Ok(false) =
    /// halt now: no coordinated event, the fleet view does not show that (or the relay did not
    /// answer), `degraded_patience_secs` since the first symptom ran out, or an operator created
    /// `operator-halt` next to the journal (the explicit decision). Never reopens anything itself.
    fn degraded_continue(&mut self, site: &str, symptom: &str) -> Result<bool, String> {
        let Some(co) = self.cfg.coordination.clone().filter(|c| c.event_id.is_some()) else { return Ok(false) };
        let state = self.state;
        let end = |m: &mut Self, why: String| -> Result<bool, String> {
            m.journal.evidence(state, json!({"degraded_end": why, "site": site, "symptom": symptom}))?;
            Ok(false)
        };
        let halt_file = self.cfg.journal_path.parent().unwrap_or(std::path::Path::new(".")).join("operator-halt");
        if halt_file.exists() {
            return end(self, format!("operator decision ({} exists)", halt_file.display()));
        }
        let now = self.ops.now_ms();
        let since = match self.degraded_since_ms {
            Some(t) => t,
            None => {
                // Journaled: a crash and resume does not restart the patience (review #7).
                self.journal.evidence(state, json!({"degraded_since_ms": now}))?;
                self.degraded_since_ms = Some(now);
                now
            }
        };
        if now.saturating_sub(since) > co.degraded_patience_secs * 1000 {
            return end(self, format!("patience exhausted ({} s since the first symptom)", co.degraded_patience_secs));
        }
        let Some(cut) = self.cut_height else { return Ok(false) };
        // Retried like every other fleet read: one transient relay error is not "the fleet does not vouch".
        let status = self.fleet_status();
        let ours = self.ops.target_block_id(cut + 1).ok().flatten();
        let v = crate::fleet::live_view(status.as_ref(), &co, cut, self.target_blockchain_id.as_deref(), ours.as_deref());
        if !v.healthy {
            // rc.24 fleet rehearsal (c2): one peer's node still at the cut, or one beacon missing a slow target read,
            // ended the wait at once and halted. A view that is merely short of a quorum gets the same grace as a
            // stalled fleet head (`fleet_stall_secs`, at most 120 s); a split or this node on a fork never does.
            let grace = co.fleet_stall_secs.min(120) * 1000;
            if v.is_hard() || grace == 0 {
                return end(self, format!("the fleet view does not vouch for the chain: {}", v.why));
            }
            let short_since = match self.fleet_unhealthy_since {
                Some(t) => t,
                None => {
                    self.fleet_unhealthy_since = Some(now);
                    self.journal.evidence(state, json!({"degraded_fleet_short": v.why, "grace_secs": grace / 1000}))?;
                    now
                }
            };
            if now.saturating_sub(short_since) > grace {
                return end(self, format!("the fleet view does not vouch for the chain ({} s): {}", grace / 1000, v.why));
            }
            return Ok(true);
        }
        self.fleet_unhealthy_since = None;
        if v.max_head > self.fleet_head.0 {
            self.fleet_head = (v.max_head, now);
        } else if self.fleet_head.1 == 0 {
            self.fleet_head.1 = now;
        }
        if now.saturating_sub(self.fleet_head.1) > co.fleet_stall_secs * 1000 {
            return end(self, format!("the fleet's target head has not moved for {} s (head {})", co.fleet_stall_secs, v.max_head));
        }
        let note = format!("{site}: {symptom}");
        if note != self.degraded_note {
            self.journal.evidence(state, json!({"degraded": {"site": site, "symptom": note, "fleet": {"members": v.members,
                "max_head": v.max_head, "after_cut_id": v.after_cut_id, "why": v.why},
                "patience_left_secs": (co.degraded_patience_secs * 1000).saturating_sub(now.saturating_sub(since)) / 1000,
                "note": "a quorum of the roster is on the same target chain: waiting out a local symptom instead of halting"}}))?;
            eprintln!("DEGRADED ({site}): {symptom}; the fleet is live ({}): waiting", v.why);
            self.degraded_note = note;
        }
        Ok(true)
    }

    /// A degraded wait ended well (the symptom cleared): journaled once.
    fn degraded_cleared(&mut self, site: &str) -> Result<(), String> {
        if !self.degraded_note.is_empty() {
            self.journal.evidence(self.state, json!({"degraded_cleared": true, "site": site}))?;
            self.degraded_note.clear();
        }
        Ok(())
    }

    /// Retry delay while degraded: `degraded_retry_secs`, doubling per attempt, at most 120 s.
    fn degraded_backoff_ms(&self, attempt: u32) -> u64 {
        let base = self.cfg.coordination.as_ref().map(|c| c.degraded_retry_secs).unwrap_or(10).max(1);
        base.saturating_mul(1u64 << attempt.min(6)).min(120) * 1000
    }

    /// Run a required post-ignition hook; while degraded it is retried with backoff (hooks must be safe
    /// to re-run: they already are re-run on resume). Ok(Some(output)), or Ok(None) after halting.
    fn run_required_hook(&mut self, site: &str, hook: &str) -> Result<Option<String>, String> {
        let mut attempt = 0u32;
        loop {
            match self.ops.run_hook(hook) {
                Ok(o) => {
                    if attempt > 0 {
                        self.journal.evidence(self.state, json!({"hook_retry_succeeded": site, "attempts": attempt + 1}))?;
                        self.degraded_cleared(site)?;
                    }
                    return Ok(Some(o));
                }
                Err(e) => {
                    if self.degraded_continue(site, &format!("{site} hook failed: {e}"))? {
                        self.ops.sleep_ms(self.degraded_backoff_ms(attempt));
                        attempt += 1;
                        continue;
                    }
                    let reason = if site == "on_live" { "on_live hook failed (writes may not be open on the new chain)" } else { "post_ignite hook failed" };
                    self.abort(reason, json!({"error": e, "attempts": attempt + 1}))?;
                    return Ok(None);
                }
            }
        }
    }

    /// Required post-ignite hook. After IGNITED a failure halts (sealed) — never "journaled and
    /// carried on" (run 5: a non-executable hook let the ceremony continue without its checks).
    /// rc.23: while the fleet is live on this chain, retried with backoff first (degraded).
    fn run_post_ignite(&mut self) -> Result<bool, String> {
        let Some(hook) = self.cfg.hooks.post_ignite.clone() else { return Ok(true) };
        match self.run_required_hook("post_ignite", &hook)? {
            Some(o) => {
                self.journal.evidence(State::Ignited, json!({"post_ignite_hook": o}))?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Sustained LIVE gate: after the head passed `cut + live_blocks`, the target must keep
    /// producing for `live_sustain_secs` with no gap longer than `live_max_gap_secs`. A stall
    /// is journaled (the ~50 s post-LIVE stall of runs 4-6) and halts the ceremony.
    fn sustain_live(&mut self, state: State) -> Result<serde_json::Value, String> {
        let sustain_ms = self.cfg.target.live_sustain_secs * 1000;
        if sustain_ms == 0 {
            return Ok(json!({"sustain_secs": 0}));
        }
        let max_gap_ms = self.cfg.target.live_max_gap_secs * 1000;
        let mut start = self.ops.now_ms();
        let (mut last_head, mut last_change, mut worst_gap) = (0u64, start, 0u64);
        loop {
            // A gap is measured between OBSERVATIONS of progress, including RPC latency: a poll
            // that itself took longer than max_gap is a gap, and progress seen after a long gap
            // is checked against the limit BEFORE it resets the clock.
            let t0 = self.ops.now_ms();
            let info = self.ops.target_info()?;
            let t1 = self.ops.now_ms();
            let poll_ms = t1.saturating_sub(t0);
            let mut gap = t1.saturating_sub(last_change);
            let advanced = info.as_ref().is_some_and(|i| i.head_block_num > last_head);
            if poll_ms > max_gap_ms {
                gap = gap.max(poll_ms);
            }
            if gap > max_gap_ms {
                self.journal.evidence(state, json!({"live_stall": {"head": last_head, "gap_ms": gap,
                    "poll_ms": poll_ms, "max_gap_ms": max_gap_ms, "into_sustain_ms": t1 - start}}))?;
                if self.degraded_continue("sustain", &format!("no new target block for {} s (head {last_head})", gap / 1000))? {
                    // The fleet is live on this chain: start the sustained window over.
                    start = t1;
                    last_change = t1;
                    if advanced {
                        last_head = info.map(|i| i.head_block_num).unwrap_or(last_head);
                    }
                    self.ops.sleep_ms(self.cfg.poll_ms);
                    continue;
                }
                self.abort(
                    "target stalled during the sustained LIVE window",
                    json!({"head": last_head, "gap_ms": gap, "live_max_gap_secs": self.cfg.target.live_max_gap_secs}),
                )?;
                return Ok(serde_json::Value::Null); // unreachable: abort after ignition halts
            }
            if advanced {
                if last_head != 0 {
                    worst_gap = worst_gap.max(gap);
                }
                last_head = info.map(|i| i.head_block_num).unwrap_or(last_head);
                last_change = t1;
            }
            if t1 - start >= sustain_ms {
                self.degraded_cleared("sustain")?;
                return Ok(json!({"sustain_secs": self.cfg.target.live_sustain_secs,
                                 "worst_gap_ms": worst_gap.max(t1.saturating_sub(last_change)), "head_at_end": last_head}));
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        }
    }

    /// on_live is part of going live (producer mode: it re-opens writes on the new chain), so it
    /// runs BEFORE LIVE is journaled; a failure halts (sealed) instead of recording a LIVE that
    /// never reached users.
    fn run_on_live(&mut self, state: State) -> Result<Option<serde_json::Value>, String> {
        let Some(hook) = self.cfg.hooks.on_live.clone() else { return Ok(Some(serde_json::Value::Null)) };
        match self.run_required_hook("on_live", &hook)? {
            Some(o) => {
                self.journal.evidence(state, json!({"on_live_hook": o}))?;
                Ok(Some(json!(o)))
            }
            None => Ok(None),
        }
    }

    /// Poll the PUBLIC URL until it demonstrably serves the TARGET chain, `health_polls`
    /// consecutive times. chain_id cannot discriminate (a same-chain-id migration keeps it) and
    /// a nearby head height cannot either (a leftover source has heights past the cut too), so the
    /// proof is a COMMON BLOCK ID above the cut: at min(target head, public head) > cut, the block
    /// the public endpoint serves must be the target's block (the source's block at that height,
    /// if any, is a discarded burn-off block with another id). Heads within `head_tolerance`.
    fn public_serves_target(&mut self) -> Result<Option<serde_json::Value>, String> {
        let flip = self.cfg.flip.clone().expect("api mode validated flip");
        let cut = self.cut_height.expect("cut pinned");
        let deadline = self.ops.now_ms() + flip.health_timeout_secs * 1000;
        let mut consecutive = 0u32;
        let mut last_why = String::from("no poll yet");
        loop {
            if self.ops.now_ms() > deadline {
                self.journal.evidence(self.state, json!({"public_route_check_last": last_why}))?;
                return Ok(None);
            }
            let target = self.ops.target_info()?;
            let public = self.ops.public_info(&flip.public_url)?;
            let proof = match (&target, &public) {
                (Some(t), Some(p)) => {
                    let chain_ok = self
                        .accepted_target_chain_id
                        .as_ref()
                        .or(self.chain_id.as_ref())
                        .map(|c| p.chain_id.eq_ignore_ascii_case(c))
                        .unwrap_or(false);
                    let common = t.head_block_num.min(p.head_block_num);
                    if !chain_ok {
                        last_why = format!("public chain_id {}", p.chain_id);
                        None
                    } else if t.head_block_num.abs_diff(p.head_block_num) > flip.head_tolerance {
                        last_why = format!("heads apart: target {} public {}", t.head_block_num, p.head_block_num);
                        None
                    } else if common <= cut {
                        last_why = format!("no block above the cut on both yet (target {}, public {})", t.head_block_num, p.head_block_num);
                        None
                    } else {
                        let tid = if common == t.head_block_num { Some(t.head_block_id.clone()) } else { self.ops.target_block_id(common)? };
                        let pid = if common == p.head_block_num { Some(p.head_block_id.clone()) } else { self.ops.public_block_id(&flip.public_url, common)? };
                        match (tid, pid) {
                            (Some(a), Some(b)) if a.eq_ignore_ascii_case(&b) => Some((common, a)),
                            (a, b) => {
                                last_why = format!("block {common}: target {a:?} public {b:?}");
                                None
                            }
                        }
                    }
                }
                _ => {
                    last_why = "target or public endpoint not answering".into();
                    None
                }
            };
            if let Some((height, id)) = proof {
                consecutive += 1;
                if consecutive >= flip.health_polls {
                    let (t, p) = (target.unwrap(), public.unwrap());
                    return Ok(Some(json!({
                        "public_head": p.head_block_num,
                        "public_chain_id": p.chain_id,
                        "target_head": t.head_block_num,
                        "common_block": {"height": height, "id": id, "above_cut": cut},
                        "consecutive_ok_polls": consecutive,
                    })));
                }
            } else {
                consecutive = 0;
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        }
    }

    /// IGNITED (api mode): swap the public /v1 URL from nodeos to the
    /// PulseVM gateway (`flip.cmd`), health-check that the public URL now
    /// serves the target, and only then move to FLIPPED. The source nodeos
    /// is STILL RUNNING — reads never gap; it stops in the FLIPPED step.
    fn step_ignited_api(&mut self) -> Result<(), String> {
        if !self.run_post_ignite()? {
            return Ok(());
        }
        // hyperion mode: stand up hyperion-rs against the new chain's SHiP,
        // stage the history boundary for the federating router, and gate on
        // hydration BEFORE anything user-visible flips. All of this is
        // abortable — the ratchet (nothing public before FLIPPED) holds.
        if self.cfg.hyperion.is_some() && !self.hyperion_hydrate()? {
            return Ok(()); // aborted inside, with evidence
        }
        if let Some(path) = self.cfg.ceremony.boundary_path.clone() {
            if !self.write_boundary(&path, "history_boundary_staged")? {
                return Ok(());
            }
        }
        let started = self.ops.now_ms();
        let flip_cmd = self.cfg.flip.as_ref().expect("validated").cmd.clone();
        self.journal.evidence(State::Ignited, json!({"side_effect": "flip_cmd"}))?;
        self.flip_ran = true; // even a failing cmd may have half-applied
        let flip_out = match self.ops.run_hook(&flip_cmd) {
            Ok(o) => o,
            Err(e) => {
                self.abort("flip command failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        // hyperion mode: the SAME flip stage also swaps /v2 to the
        // federating router — one user-visible moment for both surfaces.
        let mut v2_flip_out = serde_json::Value::Null;
        if let Some(hyp) = self.cfg.hyperion.clone() {
            self.journal.evidence(State::Ignited, json!({"side_effect": "hyperion_flip_cmd"}))?;
            self.hyperion_flip_ran = true;
            match self.ops.run_hook(&hyp.flip_cmd) {
                Ok(o) => v2_flip_out = json!(o),
                Err(e) => {
                    self.abort("hyperion /v2 flip command failed", json!({"error": e}))?;
                    return Ok(());
                }
            }
        }
        match self.public_serves_target()? {
            Some(evidence) => {
                // /v2 public gate: the flipped edge must serve the federating
                // router with a live local (post-cut) source behind it.
                let v2_health = match self.hyperion_public_gate()? {
                    Ok(h) => h,
                    Err(reason) => {
                        self.abort(&reason, json!({}))?;
                        return Ok(());
                    }
                };
                self.state = State::Flipped;
                self.journal.transition(
                    State::Flipped,
                    json!({
                        "flip_cmd_output": flip_out,
                        "hyperion_flip_cmd_output": v2_flip_out,
                        "health": evidence,
                        "v2_health": v2_health,
                        "flip_wall_ms": self.ops.now_ms().saturating_sub(started),
                        "note": "public /v1 now serves PulseVM; source nodeos still running (reads never gapped)",
                    }),
                )?;
            }
            None => {
                self.abort(
                    "public URL did not serve the target after flip (health timeout)",
                    json!({"timeout_secs": self.cfg.flip.as_ref().unwrap().health_timeout_secs}),
                )?;
            }
        }
        Ok(())
    }

    /// Write the history boundary for the federating router (atomically: the router re-reads it
    /// on change and must never see half a file). It names the source chain AND the chain the
    /// target serves (they differ only in a rehearsal), so the router can check both. A failure
    /// after ignition halts (abort() decides). Ok(false) after that.
    fn write_boundary(&mut self, path: &std::path::Path, key: &str) -> Result<bool, String> {
        let boundary = json!({
            "cut_block": self.cut_height,
            "cut_block_id": self.cut_block_id,
            "cut_time": self.last_source_block_time,
            "chain_id": self.chain_id,
            "target_chain_id": self.accepted_target_chain_id.clone().or(self.chain_id.clone()),
            "written_at_ms": self.ops.now_ms(),
        });
        let text = serde_json::to_string_pretty(&boundary).expect("boundary json") + "\n";
        if let Err(e) = crate::secrets::write_file(path, text.as_bytes(), false) {
            self.abort("could not write history boundary file", json!({"path": path.display().to_string(), "error": e}))?;
            return Ok(false);
        }
        self.journal.evidence(State::Ignited, json!({key: path.display().to_string(), "boundary": boundary}))?;
        Ok(true)
    }

    /// hyperion mode, post-IGNITED: start hyperion-rs, write the boundary
    /// file, and hold until the indexer is hydrated against the new chain.
    /// Returns Ok(false) after aborting (start failure / hydration timeout).
    fn hyperion_hydrate(&mut self) -> Result<bool, String> {
        let hyp = self.cfg.hyperion.clone().expect("caller checked");
        let cut = self.cut_height.expect("cut pinned");
        // Boundary FIRST (the router must know the cut before its local
        // source comes alive), then start.
        if let Some(path) = hyp.boundary_path.clone() {
            if !self.write_boundary(&path, "hyperion_boundary_staged")? {
                return Ok(false);
            }
        }
        if let Some(cmd) = &hyp.start_cmd {
            // Placeholder substitution: the ceremony discovers the cut, and
            // hyperion-rs on an imported chain MUST index from the first
            // post-cut block. `start_block = 0` asks SHiP for the stream
            // from block 1 — which this chain cannot serve (no pre-cut
            // blocks exist) — and the stream stays SILENT forever while
            // /v2/health shows the same signature as a healthy idle chain
            // (found live: hyperion rehearsal run 2, R21).
            let cmd = cmd
                .replace("{first_post_cut_block}", &(cut + 1).to_string())
                .replace("{cut_height}", &cut.to_string());
            match self.ops.run_hook(&cmd) {
                Ok(o) => self
                    .journal
                    .evidence(State::Ignited, json!({"hyperion_start": o, "cmd": cmd}))?,
                Err(e) => {
                    self.abort("hyperion start_cmd failed", json!({"error": e}))?;
                    return Ok(false);
                }
            }
        }
        let started = self.ops.now_ms();
        let deadline = started + hyp.hydration_timeout_secs * 1000;
        let mut last_heartbeat = 0u64;
        loop {
            let now = self.ops.now_ms();
            if now > deadline {
                self.abort(
                    "hyperion did not hydrate before hydration_timeout",
                    json!({"health_url": hyp.health_url,
                           "hydration_timeout_secs": hyp.hydration_timeout_secs}),
                )?;
                return Ok(false);
            }
            if let Some(health) = self.ops.get_json(&hyp.health_url)? {
                if let Some(evidence) = hyperion_hydrated(&health, cut, hyp.max_lag_blocks) {
                    self.journal.evidence(
                        State::Ignited,
                        json!({"hyperion_hydrated": evidence, "hydration_wall_ms": now - started}),
                    )?;
                    return Ok(true);
                }
                if now.saturating_sub(last_heartbeat) >= 30_000 {
                    self.journal.evidence(
                        State::Ignited,
                        json!({"hyperion_hydrating":
                            health.get("health").cloned().unwrap_or(serde_json::Value::Null)}),
                    )?;
                    last_heartbeat = now;
                }
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        }
    }

    /// The /v2 side of the FLIPPED health gate: the PUBLIC /v2/health must
    /// answer through the flipped edge with `federation.local.ok == true`.
    /// Ok(Ok(health)) on success; Ok(Err(reason)) when the gate fails.
    #[allow(clippy::type_complexity)]
    fn hyperion_public_gate(&mut self) -> Result<Result<serde_json::Value, String>, String> {
        let Some(hyp) = self.cfg.hyperion.clone() else {
            return Ok(Ok(serde_json::Value::Null));
        };
        let Some(url) = hyp.public_health_url.clone() else {
            return Ok(Ok(serde_json::Value::Null));
        };
        let timeout = self.cfg.flip.as_ref().expect("validated").health_timeout_secs;
        let deadline = self.ops.now_ms() + timeout * 1000;
        loop {
            if self.ops.now_ms() > deadline {
                return Ok(Err(format!(
                    "public /v2 did not serve the federating router with a live \
                     local source after flip (health timeout; url {url})"
                )));
            }
            if let Some(health) = self.ops.get_json(&url)? {
                let flag = |p: &str| health.pointer(p).and_then(|v| v.as_bool()).unwrap_or(false);
                // The router must have a live local source AND a boundary it validated against the
                // chain and the legacy archive (it serves no history otherwise).
                if flag("/federation/local/ok") && flag("/federation/boundary/valid") {
                    return Ok(Ok(health));
                }
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        }
    }

    /// FLIPPED (api mode only): traffic is on PulseVM; NOW stop the source
    /// nodeos via the operator's own stop command, re-verify the public URL
    /// is still healthy, and declare LIVE.
    fn step_flipped(&mut self) -> Result<(), String> {
        if self.cfg.ceremony.mode != Mode::Api {
            return Err("FLIPPED state reached in producer mode (journal corrupt?)".into());
        }
        let stop_cmd = self.cfg.source.stop_cmd.clone().expect("validated");
        let started = self.ops.now_ms();
        self.journal.evidence(State::Flipped, json!({"side_effect": "source_stop_cmd"}))?;
        self.source_stopped = true; // even a failing stop may have half-applied
        let stop_out = match self.ops.run_hook(&stop_cmd) {
            Ok(o) => o,
            Err(e) => {
                self.abort("source stop command failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        // The reads must have survived the source's death: re-run the same
        // public health gate before declaring LIVE.
        let health = match self.public_serves_target()? {
            Some(h) => h,
            None => {
                self.abort(
                    "public URL unhealthy after source stop",
                    json!({"source_stop_output": stop_out}),
                )?;
                return Ok(());
            }
        };
        // hyperion mode: /v2 must also have survived the source's death (the
        // federator's legacy upstream is remote — nodeos going away must not
        // matter — but the gate PROVES it rather than assuming).
        let v2_health = match self.hyperion_public_gate()? {
            Ok(h) => h,
            Err(reason) => {
                self.abort(&reason, json!({"source_stop_output": stop_out}))?;
                return Ok(());
            }
        };
        let sustained = self.sustain_live(State::Flipped)?;
        let Some(on_live) = self.run_on_live(State::Flipped)? else { return Ok(()) };
        let live_ts = self.ops.now_ms();
        let write_gap_ms = self.frozen_ts_ms.map(|f| live_ts.saturating_sub(f));
        self.state = State::Live;
        self.journal.transition(
            State::Live,
            json!({
                "sustained": sustained,
                "source_stop_output": stop_out,
                "stop_wall_ms": self.ops.now_ms().saturating_sub(started),
                "public_health": health,
                "v2_health": v2_health,
                "cut_height": self.cut_height,
                "last_source_block_time": self.last_source_block_time,
                "ceremony_gap_ms_wallclock": write_gap_ms,
                "on_live_hook": on_live,
                "rehearsal_overrides": self.cfg.rehearsal_overrides(),
                "target_chain_id": self.accepted_target_chain_id.clone().or(self.chain_id.clone()),
                "source_chain_id_changed": self.accepted_target_chain_id.is_some(),
                "note": "api-node cutover complete: same URL, same chain_id, PulseVM serving; nodeos stopped LAST; the gap includes the sustain window and on_live",
            }),
        )?;
        Ok(())
    }

    /// IGNITED: run the traffic hook, then hold at the LIVE gate until the
    /// target head advances past the cut (quorum is actually producing).
    fn step_ignited(&mut self) -> Result<(), String> {
        if self.cfg.ceremony.mode == Mode::Api {
            return self.step_ignited_api();
        }
        if !self.run_post_ignite()? {
            return Ok(());
        }
        // Producer mode: the federating router on this operator's API/history boxes needs the cut
        // too (dapp rehearsal: only hyperion mode wrote it). Before LIVE, so the router is valid the
        // moment on_live reopens writes.
        if let Some(path) = self.cfg.ceremony.boundary_path.clone() {
            if !self.write_boundary(&path, "history_boundary_staged")? {
                return Ok(());
            }
        }
        let started = self.ops.now_ms();
        let mut deadline = started + self.cfg.target.quorum_timeout_secs * 1000;
        let cut_height = self.cut_height.expect("cut pinned");
        let goal = cut_height + self.cfg.target.live_blocks;
        let mut waits = 0u32;
        let info = loop {
            if self.ops.now_ms() > deadline {
                if self.degraded_continue("live_gate", &format!("head not past {goal} within {} s", self.cfg.target.quorum_timeout_secs))? {
                    deadline = self.ops.now_ms() + self.degraded_backoff_ms(waits);
                    waits += 1;
                    continue;
                }
                self.abort(
                    "target head did not advance past the cut (no quorum / no activity)",
                    json!({"goal": goal, "quorum_timeout_secs": self.cfg.target.quorum_timeout_secs}),
                )?;
                return Ok(());
            }
            if let Some(info) = self.ops.target_info()? {
                if info.head_block_num >= goal {
                    break info;
                }
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        };
        self.degraded_cleared("live_gate")?;
        let reached_goal_ts = self.ops.now_ms();
        let sustained = self.sustain_live(State::Ignited)?;
        let Some(on_live) = self.run_on_live(State::Ignited)? else { return Ok(()) };
        let live_ts = self.ops.now_ms();
        // Outage = freeze to LIVE declared (after the sustain window and on_live). The older
        // "first progress" figure is kept for comparison with runs 1-6.
        let write_gap_ms = self.frozen_ts_ms.map(|f| live_ts.saturating_sub(f));
        let first_progress_gap_ms = self.frozen_ts_ms.map(|f| reached_goal_ts.saturating_sub(f));
        self.state = State::Live;
        self.journal.transition(
            State::Live,
            json!({
                "sustained": sustained,
                "live_declared_after_sustain_ms": live_ts.saturating_sub(reached_goal_ts),
                "target_head": info.head_block_num,
                "target_head_id": info.head_block_id,
                "first_post_cut_block_time": info.head_block_time,
                "last_source_block_time": self.last_source_block_time,
                "cut_height": cut_height,
                "write_gap_ms_wallclock": write_gap_ms,
                "first_progress_gap_ms_wallclock": first_progress_gap_ms,
                "on_live_hook": on_live,
                "rehearsal_overrides": self.cfg.rehearsal_overrides(),
                "target_chain_id": self.accepted_target_chain_id.clone().or(self.chain_id.clone()),
                "source_chain_id_changed": self.accepted_target_chain_id.is_some(),
            }),
        )?;
        Ok(())
    }
}
