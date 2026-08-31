//! The cutover ceremony state machine.
//!
//! Forward-only: ARMED -> FROZEN -> SNAPSHOTTED -> VERIFIED -> IGNITED ->
//! LIVE, ABORTED terminal from anywhere. Each state's step is idempotent so a
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

impl<'a, O: ChainOps> Machine<'a, O> {
    pub fn new(cfg: &'a Config, ops: &'a O, journal: Journal, recovered: Recovered) -> Self {
        let resumed = recovered.state.is_some();
        let state = recovered.state.unwrap_or(State::Armed);
        // A resumed agent in/past FLIPPED must assume the flip command ran.
        let flip_ran = matches!(state, State::Flipped | State::Live);
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
            hyperion_flip_ran: flip_ran,
            source_stopped: false,
            scheduled: false,
        }
    }

    fn h(&self) -> u64 {
        self.resolved_h
            .expect("H resolved at ARM (freeze_height or LIB + freeze_margin)")
    }

    /// Drive the ceremony to a terminal state. Returns the terminal state.
    pub fn run(&mut self) -> Result<State, String> {
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
                }),
            )?;
            self.preflight(&info)?;
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
            }
        }
    }

    fn preflight(&mut self, info: &crate::ops::ChainInfo) -> Result<(), String> {
        let mut problems = Vec::new();
        let h = self.h();
        // "H is in the future" means different things per mode: a producer
        // freezes at head >= H, an api node proceeds at LIB >= H (H is a
        // FINALITY target there — on a live DPoS chain head runs ~2*21*6
        // blocks ahead of LIB, and head being past H is normal).
        let reference = match self.cfg.ceremony.mode {
            Mode::Producer => info.head_block_num,
            Mode::Api => info.last_irreversible_block_num,
        };
        if reference >= h {
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
        match self.ops.producer_paused() {
            // api mode: this node does not produce; we only need producer_api
            // reachable for create_snapshot — its paused flag is irrelevant.
            Ok(paused) => {
                if paused && self.cfg.ceremony.mode == Mode::Producer {
                    problems.push("producer already paused at arm time".into());
                }
            }
            Err(e) => problems.push(format!("producer_api unreachable: {e}")),
        }
        // api mode: the public URL must be serving the SOURCE chain now —
        // proves the nginx -> nodeos path the ceremony will flip actually
        // works before anything is committed.
        if let Some(flip) = &self.cfg.flip {
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
        if let Some(dir) = self.cfg.snapshot.staged_path.parent() {
            if !dir.exists() {
                problems.push(format!("staged_path dir {} missing", dir.display()));
            }
        }
        // Stage-path hygiene (R12): the pre-staged target imports whatever sits
        // at snapshot_path the moment its chain first initializes. A stale file
        // from an earlier ceremony pins the chain to the WRONG cut before this
        // ceremony even freezes — the file must not exist until VERIFIED stages
        // the verified one.
        if self.cfg.snapshot.staged_path.exists() {
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
            self.abort("preflight failed", json!({"problems": problems}))?;
            Err(format!("preflight failed: {}", problems.join("; ")))
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

    fn abort(&mut self, reason: &str, detail: serde_json::Value) -> Result<(), String> {
        self.journal.error(self.state, reason, detail)?;
        let mut rollback = json!({"reason": reason, "auto_rollback": self.cfg.target.auto_rollback});
        if self.cfg.target.auto_rollback {
            match self.cfg.ceremony.mode {
                // Producer mode: un-pausing nodeos IS the entire rollback.
                Mode::Producer => match self.ops.resume() {
                    Ok(()) => rollback["source_producer_resumed"] = json!(true),
                    Err(e) => rollback["source_producer_resume_error"] = json!(e),
                },
                // api mode: nodeos was never paused (it isn't ours to pause).
                // Undo whatever user-visible steps already happened, in
                // reverse order: restart the source if we stopped it, then
                // swap the public URL back to it.
                Mode::Api => {
                    if self.source_stopped {
                        if let Some(cmd) = &self.cfg.source.start_cmd {
                            match self.ops.run_hook(cmd) {
                                Ok(o) => rollback["source_restarted"] = json!(o),
                                Err(e) => rollback["source_restart_error"] = json!(e),
                            }
                        } else {
                            rollback["source_stopped_no_start_cmd"] = json!(true);
                        }
                    }
                    if self.hyperion_flip_ran {
                        if let Some(cmd) =
                            self.cfg.hyperion.as_ref().and_then(|h| h.revert_cmd.clone())
                        {
                            match self.ops.run_hook(&cmd) {
                                Ok(o) => rollback["hyperion_flip_reverted"] = json!(o),
                                Err(e) => rollback["hyperion_flip_revert_error"] = json!(e),
                            }
                        }
                    }
                    if self.flip_ran {
                        if let Some(cmd) =
                            self.cfg.flip.as_ref().and_then(|f| f.revert_cmd.clone())
                        {
                            match self.ops.run_hook(&cmd) {
                                Ok(o) => rollback["flip_reverted"] = json!(o),
                                Err(e) => rollback["flip_revert_error"] = json!(e),
                            }
                        }
                    }
                    rollback["source_chain_untouched"] = json!(!self.source_stopped);
                }
            }
        }
        self.state = State::Aborted;
        self.journal.transition(State::Aborted, rollback)?;
        if let Some(hook) = &self.cfg.hooks.on_abort {
            let result = self.ops.run_hook(hook);
            self.journal.evidence(
                State::Aborted,
                json!({"on_abort_hook": format!("{result:?}")}),
            )?;
        }
        Ok(())
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
                    self.journal.evidence(
                        State::Armed,
                        json!({"schedule_snapshot_unavailable": e, "fallback": "pause_at_h"}),
                    )?;
                }
            }
        }
        let mut last_heartbeat = 0u64;
        let info = loop {
            let info = self.ops.source_info()?;
            if info.head_block_num >= self.h() {
                break info;
            }
            let now = self.ops.now_ms();
            if now.saturating_sub(last_heartbeat) >= 30_000 {
                self.journal.evidence(
                    State::Armed,
                    json!({"head": info.head_block_num, "lib": info.last_irreversible_block_num,
                           "blocks_to_h": self.h() - info.head_block_num}),
                )?;
                last_heartbeat = now;
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        };

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
        // Optional write-freeze hook (e.g. a local gateway starts rejecting
        // writes with a clear "cutover in progress" error — R2). An API node
        // cannot freeze the network's writes; that happened (or is simulated
        // to have happened) at the BP edge.
        let freeze_hook = if let Some(hook) = &self.cfg.hooks.on_freeze {
            format!("{:?}", self.ops.run_hook(hook))
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
        let snap = match self.ops.create_snapshot() {
            Ok(s) => s,
            Err(e) => {
                self.abort("create_snapshot failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        let snapshot_wall_ms = self.ops.now_ms() - started;
        let cut_height = snap.head_block_num;
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
                "file_wait_ms": self.ops.now_ms() - waited_from,
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
        let snapshot_wall_ms = self.ops.now_ms() - started;

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
                    self.journal.evidence(
                        State::Frozen,
                        json!({"late_block_after_pause": i.head_block_num}),
                    )?;
                }
                stable = 0;
                head = i.head_block_num;
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

        // Burn-off audit (R2 evidence): blocks after the cut, up to the
        // pause head, are outside the migrated state. With writes frozen
        // they must be empty; any transactions here are journaled loudly.
        let mut burnoff_txs = 0u64;
        for n in (cut_height + 1)..=at_pause.head_block_num {
            burnoff_txs += self.ops.source_block_tx_count(n).unwrap_or(0);
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
    fn step_snapshotted_upstream(&mut self) -> Result<(), String> {
        let path = std::path::PathBuf::from(
            self.snapshot_file.clone().expect("snapshot file recorded"),
        );
        let up = self.cfg.upstream.clone().expect("validated: upstream section");
        let started = self.ops.now_ms();
        let (sha256, file_size) = match verify::sha256_file(&path) {
            Ok(v) => v,
            Err(e) => {
                self.abort("cannot hash cut snapshot", json!({"error": e}))?;
                return Ok(());
            }
        };
        if let Some(expected) = &self.cfg.snapshot.expected_sha256 {
            if !sha256.eq_ignore_ascii_case(expected) {
                self.abort(
                    "snapshot sha256 mismatch vs ceremony manifest",
                    json!({"computed": sha256, "expected": expected}),
                )?;
                return Ok(());
            }
        }
        let cut_height = self.cut_height.expect("cut pinned");
        let cut_block_id = self.cut_block_id.clone().expect("cut pinned");
        let chain_id = self.chain_id.clone().unwrap_or_default();
        let outcome = {
            let journal = &mut self.journal;
            upstream::run_pipeline(
                &up,
                self.ops,
                &path,
                &sha256,
                cut_height,
                &cut_block_id,
                &chain_id,
                |evidence| {
                    let _ = journal.evidence(State::Snapshotted, evidence);
                },
            )
        };
        let outcome = match outcome {
            Ok(o) => o,
            Err(e) => {
                self.abort("upstream verification failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        // Advisory stubbed-intrinsic scan of the actual cut (read-only audit
        // over the deployed code objects; backend-independent).
        self.advisory_scan(&path, State::Snapshotted)?;
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
                        State::Snapshotted,
                        json!({"fork_audit": {
                            "note": "release-validation audit only — not an operator step, not a gate",
                            "fingerprints": roots,
                        }}),
                    )?;
                }
                Err(e) => self.journal.evidence(
                    State::Snapshotted,
                    json!({"fork_audit_error": e, "advisory": true}),
                )?,
            }
        }
        self.sha256 = Some(sha256.clone());
        self.state = State::Verified;
        self.journal.transition(
            State::Verified,
            json!({
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
                "table_compare": outcome
                    .compare_stdout
                    .as_deref()
                    .map(|_| "MATCH")
                    .unwrap_or("not configured"),
                "state_root": outcome.state_root,
                "verify_wall_ms": self.ops.now_ms() - started,
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
        // Stage the verified file where the pulsevm chain config expects it.
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
                "verify_wall_ms": self.ops.now_ms() - started,
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

    /// VERIFIED: ignite the target and wait for it to present the source
    /// chain at the cut height.
    fn step_verified(&mut self) -> Result<(), String> {
        // Upstream backend: igniting FROM the #61 checkpoint needs the
        // checkpoint-consuming node, which only exists on the unmerged PR
        // branch (migration genesis committing the checkpoint sha256 +
        // node-config migration_checkpoint knobs). Verification is done and
        // journaled; stop here with the precise remaining list rather than
        // pretending the fork plugin could boot his checkpoint.
        if self.cfg.ceremony.import_backend == ImportBackend::Upstream {
            self.abort(
                "upstream ignite pending MetalBlockchain/pulsevm#61 merge — verification \
                 completed with the official tools; ignition from the checkpoint is not \
                 yet available",
                json!({"remaining": upstream::ignite_pending_reasons()}),
            )?;
            return Ok(());
        }
        let started = self.ops.now_ms();
        let output = match self.ops.ignite() {
            Ok(o) => o,
            Err(e) => {
                self.abort("ignition command failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        let deadline = started + self.cfg.target.quorum_timeout_secs * 1000;
        let cut_height = self.cut_height.expect("cut pinned");
        let info = loop {
            if self.ops.now_ms() > deadline {
                self.abort(
                    "target chain did not come up before quorum timeout",
                    json!({"quorum_timeout_secs": self.cfg.target.quorum_timeout_secs}),
                )?;
                return Ok(());
            }
            if let Some(info) = self.ops.target_info()? {
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
        if let Some(chain_id) = &self.chain_id {
            if !info.chain_id.eq_ignore_ascii_case(chain_id) {
                self.abort(
                    "target chain_id != source chain_id",
                    json!({"target": info.chain_id, "source": chain_id}),
                )?;
                return Ok(());
            }
        }
        self.state = State::Ignited;
        self.journal.transition(
            State::Ignited,
            json!({
                "ignite_output": output,
                "target_chain_id": info.chain_id,
                "target_head": info.head_block_num,
                "target_head_id": info.head_block_id,
                "cut_height": cut_height,
                "ignite_wall_ms": self.ops.now_ms() - started,
            }),
        )?;
        Ok(())
    }

    /// Poll the PUBLIC URL until it demonstrably serves the TARGET chain:
    /// same chain_id (that is the migration's whole point, so it cannot
    /// discriminate) AND head agreeing with the target RPC within
    /// `head_tolerance`, `health_polls` consecutive times. Under
    /// simulate_freeze the still-running nodeos head is far past the cut, so
    /// head-agreement-with-target is the discriminator that proves the swap.
    fn public_serves_target(&mut self) -> Result<Option<serde_json::Value>, String> {
        let flip = self.cfg.flip.clone().expect("api mode validated flip");
        let deadline = self.ops.now_ms() + flip.health_timeout_secs * 1000;
        let mut consecutive = 0u32;
        loop {
            if self.ops.now_ms() > deadline {
                return Ok(None);
            }
            let target = self.ops.target_info()?;
            let public = self.ops.public_info(&flip.public_url)?;
            let ok = match (&target, &public) {
                (Some(t), Some(p)) => {
                    let chain_ok = self
                        .chain_id
                        .as_ref()
                        .map(|c| p.chain_id.eq_ignore_ascii_case(c))
                        .unwrap_or(false);
                    let diff = t.head_block_num.abs_diff(p.head_block_num);
                    chain_ok && diff <= flip.head_tolerance
                }
                _ => false,
            };
            if ok {
                consecutive += 1;
                if consecutive >= flip.health_polls {
                    let (t, p) = (target.unwrap(), public.unwrap());
                    return Ok(Some(json!({
                        "public_head": p.head_block_num,
                        "public_chain_id": p.chain_id,
                        "target_head": t.head_block_num,
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
        if let Some(hook) = &self.cfg.hooks.post_ignite {
            let result = self.ops.run_hook(hook);
            self.journal.evidence(
                State::Ignited,
                json!({"post_ignite_hook": format!("{result:?}")}),
            )?;
        }
        // hyperion mode: stand up hyperion-rs against the new chain's SHiP,
        // stage the history boundary for the federating router, and gate on
        // hydration BEFORE anything user-visible flips. All of this is
        // abortable — the ratchet (nothing public before FLIPPED) holds.
        if self.cfg.hyperion.is_some() && !self.hyperion_hydrate()? {
            return Ok(()); // aborted inside, with evidence
        }
        let started = self.ops.now_ms();
        let flip_cmd = self.cfg.flip.as_ref().expect("validated").cmd.clone();
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
                        "flip_wall_ms": self.ops.now_ms() - started,
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

    /// hyperion mode, post-IGNITED: start hyperion-rs, write the boundary
    /// file, and hold until the indexer is hydrated against the new chain.
    /// Returns Ok(false) after aborting (start failure / hydration timeout).
    fn hyperion_hydrate(&mut self) -> Result<bool, String> {
        let hyp = self.cfg.hyperion.clone().expect("caller checked");
        let cut = self.cut_height.expect("cut pinned");
        // Boundary FIRST (the router must know the cut before its local
        // source comes alive), then start.
        if let Some(path) = &hyp.boundary_path {
            let boundary = json!({
                "cut_block": self.cut_height,
                "cut_block_id": self.cut_block_id,
                "cut_time": self.last_source_block_time,
                "chain_id": self.chain_id,
                "written_at_ms": self.ops.now_ms(),
            });
            if let Err(e) = std::fs::write(
                path,
                serde_json::to_string_pretty(&boundary).expect("boundary json"),
            ) {
                self.abort(
                    "could not write history boundary file",
                    json!({"path": path.display().to_string(), "error": e.to_string()}),
                )?;
                return Ok(false);
            }
            self.journal.evidence(
                State::Ignited,
                json!({"hyperion_boundary_staged": path.display().to_string(), "boundary": boundary}),
            )?;
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
                let local_ok = health
                    .pointer("/federation/local/ok")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if local_ok {
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
        let live_ts = self.ops.now_ms();
        let write_gap_ms = self.frozen_ts_ms.map(|f| live_ts - f);
        self.state = State::Live;
        self.journal.transition(
            State::Live,
            json!({
                "source_stop_output": stop_out,
                "stop_wall_ms": self.ops.now_ms() - started,
                "public_health": health,
                "v2_health": v2_health,
                "cut_height": self.cut_height,
                "last_source_block_time": self.last_source_block_time,
                "ceremony_gap_ms_wallclock": write_gap_ms,
                "note": "api-node cutover complete: same URL, same chain_id, PulseVM serving; nodeos stopped LAST",
            }),
        )?;
        if let Some(hook) = &self.cfg.hooks.on_live {
            let result = self.ops.run_hook(hook);
            self.journal
                .evidence(State::Live, json!({"on_live_hook": format!("{result:?}")}))?;
        }
        Ok(())
    }

    /// IGNITED: run the traffic hook, then hold at the LIVE gate until the
    /// target head advances past the cut (quorum is actually producing).
    fn step_ignited(&mut self) -> Result<(), String> {
        if self.cfg.ceremony.mode == Mode::Api {
            return self.step_ignited_api();
        }
        if let Some(hook) = &self.cfg.hooks.post_ignite {
            let result = self.ops.run_hook(hook);
            self.journal.evidence(
                State::Ignited,
                json!({"post_ignite_hook": format!("{result:?}")}),
            )?;
        }
        let started = self.ops.now_ms();
        let deadline = started + self.cfg.target.quorum_timeout_secs * 1000;
        let cut_height = self.cut_height.expect("cut pinned");
        let goal = cut_height + self.cfg.target.live_blocks;
        let info = loop {
            if self.ops.now_ms() > deadline {
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
        let live_ts = self.ops.now_ms();
        let write_gap_ms = self.frozen_ts_ms.map(|f| live_ts - f);
        self.state = State::Live;
        self.journal.transition(
            State::Live,
            json!({
                "target_head": info.head_block_num,
                "target_head_id": info.head_block_id,
                "first_post_cut_block_time": info.head_block_time,
                "last_source_block_time": self.last_source_block_time,
                "cut_height": cut_height,
                "write_gap_ms_wallclock": write_gap_ms,
            }),
        )?;
        if let Some(hook) = &self.cfg.hooks.on_live {
            let result = self.ops.run_hook(hook);
            self.journal
                .evidence(State::Live, json!({"on_live_hook": format!("{result:?}")}))?;
        }
        Ok(())
    }
}
