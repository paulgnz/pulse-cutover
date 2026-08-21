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
    },
    journal::{
        Journal,
        Recovered,
    },
    ops::ChainOps,
    state::State,
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
    pub cut_height: Option<u64>,
    pub cut_block_id: Option<String>,
    pub snapshot_file: Option<String>,
    pub sha256: Option<String>,
    pub frozen_ts_ms: Option<u64>,
    pub last_source_block_time: Option<String>,
}

impl<'a, O: ChainOps> Machine<'a, O> {
    pub fn new(cfg: &'a Config, ops: &'a O, journal: Journal, recovered: Recovered) -> Self {
        let resumed = recovered.state.is_some();
        let state = recovered.state.unwrap_or(State::Armed);
        Machine {
            cfg,
            ops,
            journal,
            state,
            resumed,
            chain_id: recovered.chain_id.or_else(|| cfg.ceremony.chain_id.clone()),
            cut_height: recovered.cut_height,
            cut_block_id: recovered.cut_block_id,
            snapshot_file: recovered.snapshot_file,
            sha256: recovered.sha256,
            frozen_ts_ms: recovered.frozen_ts_ms,
            last_source_block_time: recovered.last_source_block_time,
        }
    }

    /// Drive the ceremony to a terminal state. Returns the terminal state.
    pub fn run(&mut self) -> Result<State, String> {
        if !self.resumed {
            // Fresh ceremony: journal the arming evidence once.
            let info = self.ops.source_info()?;
            self.chain_id = Some(info.chain_id.clone());
            self.journal.transition(
                State::Armed,
                json!({
                    "freeze_height": self.cfg.ceremony.freeze_height,
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
                State::Live | State::Aborted => return Ok(self.state),
            }
        }
    }

    fn preflight(&mut self, info: &crate::ops::ChainInfo) -> Result<(), String> {
        let mut problems = Vec::new();
        if info.head_block_num >= self.cfg.ceremony.freeze_height {
            problems.push(format!(
                "freeze_height {} is not in the future (head {})",
                self.cfg.ceremony.freeze_height, info.head_block_num
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
            Ok(false) => {}
            Ok(true) => problems.push("producer already paused at arm time".into()),
            Err(e) => problems.push(format!("producer_api unreachable: {e}")),
        }
        if let Some(dir) = self.cfg.snapshot.staged_path.parent() {
            if !dir.exists() {
                problems.push(format!("staged_path dir {} missing", dir.display()));
            }
        }
        if let Some(g) = &self.cfg.snapshot.golden_roots {
            if !g.exists() {
                problems.push(format!("golden_roots {} missing", g.display()));
            }
        }
        if problems.is_empty() {
            self.journal
                .evidence(State::Armed, json!({"preflight": "ok"}))?;
            Ok(())
        } else {
            self.abort("preflight failed", json!({"problems": problems}))?;
            Err(format!("preflight failed: {}", problems.join("; ")))
        }
    }

    fn abort(&mut self, reason: &str, detail: serde_json::Value) -> Result<(), String> {
        self.journal.error(self.state, reason, detail)?;
        let mut rollback = json!({"reason": reason, "auto_rollback": self.cfg.target.auto_rollback});
        if self.cfg.target.auto_rollback {
            match self.ops.resume() {
                Ok(()) => rollback["source_producer_resumed"] = json!(true),
                Err(e) => rollback["source_producer_resume_error"] = json!(e),
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

    /// ARMED: watch head; freeze at H per strategy; pin the cut by block id
    /// after a quiescence window.
    fn step_armed(&mut self) -> Result<(), String> {
        if self.cfg.ceremony.freeze_strategy == FreezeStrategy::ScheduleAtH {
            // Multi-BP mode: pin the snapshot to exactly H up front; nodeos
            // writes it when H becomes irreversible. (Falls back to
            // pause_at_h if the scheduler API is unavailable.)
            if let Err(e) = self.ops.schedule_snapshot(self.cfg.ceremony.freeze_height) {
                self.journal.evidence(
                    State::Armed,
                    json!({"schedule_snapshot_unavailable": e, "fallback": "pause_at_h"}),
                )?;
            }
        }
        let mut last_heartbeat = 0u64;
        let info = loop {
            let info = self.ops.source_info()?;
            if info.head_block_num >= self.cfg.ceremony.freeze_height {
                break info;
            }
            let now = self.ops.now_ms();
            if now.saturating_sub(last_heartbeat) >= 30_000 {
                self.journal.evidence(
                    State::Armed,
                    json!({"head": info.head_block_num, "lib": info.last_irreversible_block_num,
                           "blocks_to_h": self.cfg.ceremony.freeze_height - info.head_block_num}),
                )?;
                last_heartbeat = now;
            }
            self.ops.sleep_ms(self.cfg.poll_ms);
        };

        // Freeze: stop production. (In multi-BP schedule_at_h mode this
        // happens only after LIB >= H; production of empty blocks past H is
        // what finalizes H — review finding R1.)
        if self.cfg.ceremony.freeze_strategy == FreezeStrategy::ScheduleAtH {
            loop {
                let i = self.ops.source_info()?;
                if i.last_irreversible_block_num >= self.cfg.ceremony.freeze_height {
                    break;
                }
                self.ops.sleep_ms(self.cfg.poll_ms);
            }
        }
        let pause_requested_ms = self.ops.now_ms();
        self.ops.pause()?;
        if !self.ops.producer_paused()? {
            self.abort("pause did not take effect", json!({}))?;
            return Ok(());
        }

        // Quiescence window: head must stop moving (catches a second
        // producer that didn't pause, or in-flight blocks — finding R4).
        let mut stable = 0u32;
        let mut head = info.head_block_num;
        let cut = loop {
            self.ops.sleep_ms(self.cfg.poll_ms);
            let i = self.ops.source_info()?;
            if i.head_block_num == head {
                stable += 1;
                if stable >= self.cfg.ceremony.quiescence_polls {
                    break i;
                }
            } else {
                stable = 0;
                head = i.head_block_num;
                self.journal.evidence(
                    State::Armed,
                    json!({"late_block_after_pause": i.head_block_num}),
                )?;
            }
        };

        // Pin the cut: in schedule_at_h mode the cut is H itself; in
        // pause_at_h mode it is wherever the head stopped (>= H).
        let cut_height = match self.cfg.ceremony.freeze_strategy {
            FreezeStrategy::ScheduleAtH => self.cfg.ceremony.freeze_height,
            FreezeStrategy::PauseAtH => cut.head_block_num,
        };
        let (cut_block_id, cut_block_time) = self.ops.source_block_id(cut_height)?;
        self.chain_id = Some(cut.chain_id.clone());
        self.cut_height = Some(cut_height);
        self.cut_block_id = Some(cut_block_id.clone());
        self.frozen_ts_ms = Some(self.ops.now_ms());
        self.last_source_block_time = Some(cut_block_time.clone());
        self.state = State::Frozen;
        self.journal.transition(
            State::Frozen,
            json!({
                "declared_h": self.cfg.ceremony.freeze_height,
                "cut_height": cut_height,
                "cut_block_id": cut_block_id,
                "last_source_block_time": cut_block_time,
                "head_at_pause": cut.head_block_num,
                "lib_at_pause": cut.last_irreversible_block_num,
                "chain_id": cut.chain_id,
                "pause_requested_ms": pause_requested_ms,
                "quiescence_polls": self.cfg.ceremony.quiescence_polls,
            }),
        )?;
        Ok(())
    }

    /// FROZEN: produce the snapshot for the cut block and stage the file.
    fn step_frozen(&mut self) -> Result<(), String> {
        // Idempotent on resume: pause again (no-op if already paused).
        if !self.ops.producer_paused()? {
            self.ops.pause()?;
        }
        let started = self.ops.now_ms();
        let snap = match self.ops.create_snapshot() {
            Ok(s) => s,
            Err(e) => {
                self.abort("create_snapshot failed", json!({"error": e}))?;
                return Ok(());
            }
        };
        let cut_height = self.cut_height.expect("cut pinned in FROZEN");
        if snap.head_block_num != cut_height {
            self.abort(
                "snapshot height != pinned cut height",
                json!({"snapshot_height": snap.head_block_num, "cut_height": cut_height,
                       "snapshot_block_id": snap.head_block_id}),
            )?;
            return Ok(());
        }
        if let Some(cut_id) = &self.cut_block_id {
            if !snap.head_block_id.eq_ignore_ascii_case(cut_id) {
                self.abort(
                    "snapshot block id != pinned cut block id (fork at the cut?)",
                    json!({"snapshot_block_id": snap.head_block_id, "cut_block_id": cut_id}),
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
        self.snapshot_file = Some(host_path.display().to_string());
        self.state = State::Snapshotted;
        self.journal.transition(
            State::Snapshotted,
            json!({
                "snapshot_file": host_path.display().to_string(),
                "reported_name": snap.snapshot_name,
                "size_bytes": size,
                "snapshot_wall_ms": self.ops.now_ms() - started,
                "cut_height": cut_height,
                "cut_block_id": snap.head_block_id,
            }),
        )?;
        Ok(())
    }

    /// SNAPSHOTTED: verify — sha256, dual-import fingerprints, goldens.
    fn step_snapshotted(&mut self) -> Result<(), String> {
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
        self.sha256 = Some(outcome.sha256.clone());
        self.state = State::Verified;
        let roots: serde_json::Map<String, serde_json::Value> = outcome
            .roots
            .iter()
            .map(|(n, r)| (n.clone(), json!(format!("{r:016x}"))))
            .collect();
        self.journal.transition(
            State::Verified,
            json!({
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
            }),
        )?;
        Ok(())
    }

    /// VERIFIED: ignite the target and wait for it to present the source
    /// chain at the cut height.
    fn step_verified(&mut self) -> Result<(), String> {
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

    /// IGNITED: run the traffic hook, then hold at the LIVE gate until the
    /// target head advances past the cut (quorum is actually producing).
    fn step_ignited(&mut self) -> Result<(), String> {
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
