//! State-machine tests driven by a mock `ChainOps` and a REAL snapshot: the
//! mock's create_snapshot writes a `MiniSnapshot` (pulsevm_snapshot's fixture
//! builder), so the VERIFIED step exercises the genuine import + fingerprint
//! path end to end without the 176 MB fixture.

use std::{
    cell::{
        Cell,
        RefCell,
    },
    path::PathBuf,
};

use pulse_cutover::{
    config::Config,
    journal::Journal,
    machine::Machine,
    ops::{
        ChainInfo,
        ChainOps,
        SnapshotResult,
    },
    state::State,
    verify,
};
use pulsevm_snapshot::testing::{
    MiniSnapshot,
    TestAccount,
};

const CHAIN_ID: [u8; 32] = [0xAB; 32];

fn mini(head: u32) -> MiniSnapshot {
    MiniSnapshot {
        chain_id: CHAIN_ID,
        head_block_num: head,
        head_slot: 1_514_764_800,
        head_producer: "eosio".parse().unwrap(),
        accounts: vec![TestAccount {
            name: "cutdemo1".parse().unwrap(),
            key: [2u8; 33],
        }],
    }
}

/// Scripted chain world. Source head advances one block per poll until
/// paused; the target appears after ignition and advances once traffic
/// resumes (post_ignite hook).
struct MockOps {
    dir: PathBuf,
    head: Cell<u64>,
    paused: Cell<bool>,
    resumes: Cell<u32>,
    ignited: Cell<bool>,
    target_polls: Cell<u64>,
    /// Head of the (mock) imported chain: the cut height, set by
    /// create_snapshot; advances only once traffic resumes.
    target_head: Cell<u64>,
    /// If true, produce one extra block on the SECOND poll after pause (a
    /// late block arriving over p2p — quiescence must absorb + journal it).
    late_block: Cell<bool>,
    paused_polls: Cell<u32>,
    /// Fault injection for the burn-off audit: transactions in each
    /// post-cut block, or an unreadable post-cut block.
    burnoff_tx_per_block: Cell<u64>,
    burnoff_read_fails: Cell<bool>,
    hooks: RefCell<Vec<String>>,
    now: Cell<u64>,
    // --- api-mode world ---
    /// Blocks the source head advances per source_info poll (a live chain
    /// that will NOT freeze — simulate_freeze rehearsals). Default 1.
    drift: Cell<u64>,
    /// Set when the "flip-nginx" hook runs: public URL now routes to target.
    flipped: Cell<bool>,
    /// Set when the "stop-nodeos" hook runs: source nodeos is down.
    stopped: Cell<bool>,
    /// Fault injection: flip cmd succeeds but the public URL never serves
    /// the target (nginx swap to a dead upstream).
    flip_breaks_public: Cell<bool>,
    // --- hyperion-mode world ---
    /// Set when the "hyperion-start" hook runs (start_cmd).
    hyperion_started: Cell<bool>,
    /// Local /v2/health polls seen so far.
    hyperion_polls: Cell<u32>,
    /// Indexer catches up after this many health polls (u32::MAX = never).
    hyperion_ready_after: Cell<u32>,
    /// Set when the "flip-v2" hook runs: public /v2 routes to the federator.
    flipped_v2: Cell<bool>,
    // --- producer schedule_at_h world ---
    /// schedule_snapshot succeeds and stages the scheduled file.
    schedule_ok: Cell<bool>,
    scheduled_h: Cell<u64>,
    /// Source head at the moment the write-freeze hook ran.
    freeze_head: Cell<u64>,
    // --- coordination world ---
    /// GET <url>/api/coord/<net> answer (signed messages), if any.
    coord_doc: RefCell<Option<serde_json::Value>>,
    /// /api/status: producers agreeing with our evidence = `fleet_agree` once
    /// `fleet_agree_after` status polls have happened (1 before that: just us).
    fleet_agree: Cell<usize>,
    fleet_agree_after: Cell<u32>,
    fleet_polls: Cell<u32>,
    /// Override for GET /api/status (roster fleet-gate tests).
    status_doc: RefCell<Option<serde_json::Value>>,
    // --- fault injection ---
    /// The ignited target presents a DIFFERENT block id at the cut (wrong lineage).
    target_fork: Cell<bool>,
    /// Target head stops advancing after this many target polls (post-LIVE stall).
    target_stall_after: Cell<u64>,
    /// Source info calls (a readiness refusal must make none).
    source_calls: Cell<u32>,
    /// The ignite command fails (after possibly starting the target).
    ignite_fails: Cell<bool>,
    /// Extra mock-clock time each target_info poll takes (RPC latency).
    target_latency_ms: Cell<u64>,
    /// The producer resume call fails (rollback must report it).
    resume_fails: Cell<bool>,
    /// Ordered log of resume / hooks / orphan-kill calls (rollback ordering tests).
    events: RefCell<Vec<String>>,
    /// kill_orphan_hooks answer (None = default Ok(None)).
    orphan: RefCell<Option<Result<Option<String>, String>>>,
    /// A hook command that makes the mock panic (simulates the agent dying mid-hook).
    panic_on_hook: RefCell<Option<String>>,
}

impl MockOps {
    fn new(dir: &std::path::Path, start_head: u64) -> Self {
        MockOps {
            dir: dir.to_path_buf(),
            head: Cell::new(start_head),
            paused: Cell::new(false),
            resumes: Cell::new(0),
            ignited: Cell::new(false),
            target_polls: Cell::new(0),
            target_head: Cell::new(0),
            late_block: Cell::new(false),
            paused_polls: Cell::new(0),
            burnoff_tx_per_block: Cell::new(0),
            burnoff_read_fails: Cell::new(false),
            hooks: RefCell::new(Vec::new()),
            now: Cell::new(1_000_000),
            drift: Cell::new(1),
            flipped: Cell::new(false),
            stopped: Cell::new(false),
            flip_breaks_public: Cell::new(false),
            hyperion_started: Cell::new(false),
            hyperion_polls: Cell::new(0),
            hyperion_ready_after: Cell::new(0),
            flipped_v2: Cell::new(false),
            schedule_ok: Cell::new(false),
            scheduled_h: Cell::new(0),
            freeze_head: Cell::new(0),
            coord_doc: RefCell::new(None),
            fleet_agree: Cell::new(0),
            fleet_agree_after: Cell::new(0),
            fleet_polls: Cell::new(0),
            status_doc: RefCell::new(None),
            target_fork: Cell::new(false),
            target_stall_after: Cell::new(u64::MAX),
            source_calls: Cell::new(0),
            ignite_fails: Cell::new(false),
            target_latency_ms: Cell::new(0),
            resume_fails: Cell::new(false),
            events: RefCell::new(Vec::new()),
            orphan: RefCell::new(None),
            panic_on_hook: RefCell::new(None),
        }
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("snapshot-cut.bin")
    }

    fn info(&self, head: u64) -> ChainInfo {
        let m = mini(head as u32);
        ChainInfo {
            chain_id: hex::encode(CHAIN_ID),
            head_block_num: head,
            head_block_id: hex::encode(m.head_id()),
            head_block_time: "2024-01-01T00:00:00.000".into(),
            last_irreversible_block_num: head.saturating_sub(1),
        }
    }
}

impl ChainOps for MockOps {
    fn source_info(&self) -> Result<ChainInfo, String> {
        self.source_calls.set(self.source_calls.get() + 1);
        if self.stopped.get() {
            return Err("connection refused (nodeos stopped)".into());
        }
        if !self.paused.get() {
            self.head.set(self.head.get() + self.drift.get());
        } else {
            let polls = self.paused_polls.get() + 1;
            self.paused_polls.set(polls);
            if polls == 2 && self.late_block.replace(false) {
                self.head.set(self.head.get() + 1);
            }
        }
        Ok(self.info(self.head.get()))
    }

    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        let m = mini(block_num as u32);
        Ok((hex::encode(m.head_id()), "2024-01-01T00:00:00.000".into()))
    }

    fn source_block_tx_count(&self, _block_num: u64) -> Result<u64, String> {
        if self.burnoff_read_fails.get() {
            return Err("get_block timed out".into());
        }
        Ok(self.burnoff_tx_per_block.get()) // default 0: writes are frozen
    }

    fn producer_paused(&self) -> Result<bool, String> {
        Ok(self.paused.get())
    }

    fn pause(&self) -> Result<(), String> {
        self.paused.set(true);
        Ok(())
    }

    fn resume(&self) -> Result<(), String> {
        self.events.borrow_mut().push("resume".into());
        if self.resume_fails.get() {
            return Err("http://mock/v1/producer/resume: connection refused".into());
        }
        self.resumes.set(self.resumes.get() + 1);
        self.paused.set(false);
        Ok(())
    }

    fn kill_orphan_hooks(&self) -> Result<Option<String>, String> {
        self.events.borrow_mut().push("kill-orphans".into());
        self.orphan.borrow().clone().unwrap_or(Ok(None))
    }

    fn create_snapshot(&self) -> Result<SnapshotResult, String> {
        // R1, as verified live on Leap 5.0.3: a paused chain cannot finalize
        // its head, so the snapshot write never completes.
        if self.paused.get() {
            return Err("create_snapshot timed out: paused chain never finalizes head (R1)".into());
        }
        let cut = self.head.get();
        let m = mini(cut as u32);
        std::fs::write(self.snapshot_path(), m.build()).map_err(|e| e.to_string())?;
        // Cut facts for the fake upstream tools (the real xpr_import derives
        // these from the SHiP log; the fakes read them from here).
        let _ = std::fs::write(
            self.dir.join("cut-facts.env"),
            format!("CUT_HEIGHT={cut}\nCUT_BLOCK_ID={}\n", hex::encode(m.head_id())),
        );
        // Production continues; the block that finalized the cut arrives.
        self.head.set(cut + 1);
        self.target_head.set(cut);
        Ok(SnapshotResult {
            snapshot_name: self.snapshot_path().display().to_string(),
            head_block_num: cut,
            head_block_id: hex::encode(m.head_id()),
        })
    }

    fn schedule_snapshot(&self, height: u64) -> Result<(), String> {
        if !self.schedule_ok.get() {
            return Err("snapshot scheduler unavailable".into());
        }
        // The scheduler will write snapshot-<block_id_at_H>.bin once H is
        // irreversible; the mock stages it up front (the machine only looks
        // after LIB >= H) and the staged target chain presents the cut.
        let m = mini(height as u32);
        let file = self.dir.join(format!("snapshot-{}.bin", hex::encode(m.head_id())));
        std::fs::write(&file, m.build()).map_err(|e| e.to_string())?;
        self.scheduled_h.set(height);
        self.target_head.set(height);
        Ok(())
    }

    fn target_info(&self) -> Result<Option<ChainInfo>, String> {
        self.now.set(self.now.get() + self.target_latency_ms.get());
        if !self.ignited.get() {
            return Ok(None);
        }
        let polls = self.target_polls.get() + 1;
        self.target_polls.set(polls);
        let traffic_resumed = self.flipped.get()
            || self
                .hooks
                .borrow()
                .iter()
                .any(|h| h.contains("resume-traffic"));
        // The imported chain presents the cut height; it only mints past it
        // once transactions flow again (post_ignite traffic hook / public flip),
        // and keeps producing unless a stall is injected.
        let head = if traffic_resumed {
            self.target_head.get() + polls.min(self.target_stall_after.get())
        } else {
            self.target_head.get()
        };
        let mut info = self.info(head);
        if self.target_fork.get() {
            info.head_block_id = format!("ff{}", &info.head_block_id[2..]);
        }
        Ok(Some(info))
    }

    fn public_info(&self, _public_url: &str) -> Result<Option<ChainInfo>, String> {
        if self.flipped.get() {
            // Public URL routes to the gateway -> pulsevm target.
            if self.flip_breaks_public.get() {
                return Ok(None); // swapped to a dead upstream
            }
            return self.target_info();
        }
        // Public URL routes to nodeos (which may be live-drifting way past
        // the cut under simulate_freeze, or dead if stopped prematurely).
        if self.stopped.get() {
            return Ok(None);
        }
        Ok(Some(self.info(self.head.get())))
    }

    fn ignite(&self) -> Result<String, String> {
        self.ignited.set(true);
        if self.ignite_fails.get() {
            return Err("systemctl restart exited 1 (partial start)".into());
        }
        Ok("mock metalgo restarted".into())
    }

    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        self.hooks.borrow_mut().push(cmd.to_string());
        self.events.borrow_mut().push(format!("hook:{cmd}"));
        if self.panic_on_hook.borrow().as_deref() == Some(cmd) {
            panic!("simulated crash inside hook `{cmd}`");
        }
        if cmd.starts_with("fail-") {
            return Err(format!("`{cmd}` exited 1"));
        }
        if cmd == "freeze-writes" {
            self.freeze_head.set(self.head.get());
        }
        // Upstream-pipeline commands reference generated fake tools in the
        // test dir — execute them for real: the pipeline verifies their
        // file outputs (SHiP log, manifest.env, checkpoint + manifest).
        if cmd.contains("fake-") {
            return pulse_cutover::ops::run_shell(cmd);
        }
        if cmd.contains("flip-nginx") {
            self.flipped.set(true);
        }
        if cmd.contains("revert-nginx") {
            self.flipped.set(false);
        }
        if cmd.contains("stop-nodeos") {
            self.stopped.set(true);
        }
        if cmd.contains("hyperion-start") {
            self.hyperion_started.set(true);
        }
        if cmd.contains("flip-v2") {
            self.flipped_v2.set(true);
        }
        if cmd.contains("revert-v2") {
            self.flipped_v2.set(false);
        }
        Ok(format!("ran: {cmd}"))
    }

    fn get_json(&self, url: &str) -> Result<Option<serde_json::Value>, String> {
        if url.contains("/api/coord/") {
            return Ok(self.coord_doc.borrow().clone());
        }
        if url.ends_with("/api/status") {
            let polls = self.fleet_polls.get() + 1;
            self.fleet_polls.set(polls);
            if let Some(doc) = self.status_doc.borrow().clone() {
                return Ok(Some(doc));
            }
            let ours = pulse_cutover::beacon::journal_summary(&self.dir.join("journal.jsonl"))["evidence"].clone();
            let n = if polls >= self.fleet_agree_after.get() { self.fleet_agree.get() } else { 1 };
            // The relay always reports each report's age (the gate requires fresh reports).
            let producers: Vec<_> = (0..n).map(|i| serde_json::json!({"name": format!("bp{i}"), "age_ms": 1000,
                "report": {"coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}}})).collect();
            return Ok(Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": producers}]})));
        }
        // Local hyperion-rs /v2/health (the .95-observed shape).
        if url.contains("hyperion-local") {
            if !self.hyperion_started.get() {
                return Ok(None); // service not yet started
            }
            let polls = self.hyperion_polls.get() + 1;
            self.hyperion_polls.set(polls);
            let cut = self.target_head.get();
            let ready = polls >= self.hyperion_ready_after.get();
            // Until hydrated: services OK but the indexer visibly behind a
            // SHiP head that is past the cut — the predicate must WAIT.
            let (last_indexed, rpc_head) =
                if ready { (cut + 5, cut + 5) } else { (0, cut + 5) };
            return Ok(Some(serde_json::json!({
                "chain": "xpr",
                "version": "0.1.0",
                "health": [
                    {"service": "Elasticsearch", "status": "OK"},
                    {"service": "PulseVM-RPC", "status": "OK",
                     "service_data": {"chain_id": hex::encode(CHAIN_ID), "head_block_num": rpc_head}},
                    {"service": "Indexer", "status": "OK",
                     "service_data": {"head_block_num": rpc_head, "last_indexed_block": last_indexed}}
                ]
            })));
        }
        // Public /v2/health through the flipped edge (federating router).
        if url.contains("v2-public") {
            let local_ok = self.flipped_v2.get()
                && self.hyperion_polls.get() >= self.hyperion_ready_after.get();
            return Ok(Some(serde_json::json!({
                "federation": {
                    "boundary": {"cut_block": self.target_head.get()},
                    "local": {"ok": local_ok},
                    "legacy": {"ok": true}
                }
            })));
        }
        Ok(None)
    }

    fn now_ms(&self) -> u64 {
        self.now.set(self.now.get() + 25);
        self.now.get()
    }

    fn sleep_ms(&self, _ms: u64) {}
}

fn test_config(dir: &std::path::Path, freeze_height: u64) -> Config {
    let toml_text = format!(
        r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1

[ceremony]
freeze_height = {freeze_height}
quiescence_polls = 3

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"

[snapshot]
staged_path = "{dir}/staged.bin"
capture_roots = "{dir}/captured-roots.txt"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 60

[hooks]
on_freeze = "freeze-writes"
post_ignite = "resume-traffic"
on_live = "flip-gateway"
"#,
        dir = dir.display(),
    );
    let path = dir.join("ceremony.toml");
    std::fs::write(&path, toml_text).unwrap();
    Config::load(&path).unwrap()
}

/// api-node ceremony config: no producer pause; the /v1 flip + source stop
/// replace the producer-mode traffic hooks. simulate_freeze because the mock
/// source is a live chain that will not stop at H.
fn api_test_config(dir: &std::path::Path, freeze_height: u64) -> Config {
    let toml_text = format!(
        r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1

[ceremony]
mode = "api"
freeze_height = {freeze_height}
simulate_freeze = true

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
stop_cmd = "stop-nodeos"

[snapshot]
staged_path = "{dir}/staged.bin"
capture_roots = "{dir}/captured-roots.txt"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 60

[flip]
cmd = "flip-nginx"
public_url = "http://mock-public"
revert_cmd = "revert-nginx"
health_polls = 2
head_tolerance = 2
health_timeout_secs = 5

[hooks]
on_freeze = "freeze-writes"
on_live = "announce-live"
"#,
        dir = dir.display(),
    );
    let path = dir.join("ceremony-api.toml");
    std::fs::write(&path, toml_text).unwrap();
    Config::load(&path).unwrap()
}

fn run_machine(cfg: &Config, ops: &MockOps) -> State {
    run_machine_result(cfg, ops).unwrap()
}

fn run_machine_result(cfg: &Config, ops: &MockOps) -> Result<State, String> {
    let (journal, recovered) = Journal::open(&cfg.journal_path).unwrap();
    let mut machine = Machine::new(cfg, ops, journal, recovered);
    machine.run()
}

fn load_toml(dir: &std::path::Path, name: &str, text: &str) -> Result<Config, String> {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    Config::load(&path)
}

#[test]
fn happy_path_reaches_live_with_full_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);

    // The staged snapshot is the verified one, byte for byte.
    let staged = std::fs::read(dir.path().join("staged.bin")).unwrap();
    let original = std::fs::read(ops.snapshot_path()).unwrap();
    assert_eq!(staged, original);

    // Captured goldens carry provenance + all 19 tables.
    let roots = std::fs::read_to_string(dir.path().join("captured-roots.txt")).unwrap();
    for table in verify::TABLE_NAMES {
        assert!(roots.contains(table), "missing {table} in captured goldens");
    }
    assert!(roots.contains(&hex::encode(CHAIN_ID)));

    // Hooks ran in ceremony order: write freeze BEFORE the snapshot (R1/R2),
    // traffic after ignition, gateway flip only at LIVE.
    let hooks = ops.hooks.borrow();
    assert_eq!(
        *hooks,
        vec![
            "freeze-writes".to_string(),
            "resume-traffic".to_string(),
            "flip-gateway".to_string()
        ]
    );

    // Journal walked the full state sequence.
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let states: Vec<String> = text
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .filter(|v| v["kind"] == "transition")
        .map(|v| v["state"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        states,
        ["ARMED", "FROZEN", "SNAPSHOTTED", "VERIFIED", "IGNITED", "LIVE"]
    );

    // The cut was pinned at/after H and the write gap was measured.
    let live: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .filter(|v: &serde_json::Value| v["state"] == "LIVE" && v["kind"] == "transition")
        .next_back()
        .unwrap();
    assert!(live["data"]["cut_height"].as_u64().unwrap() >= 120);
    assert!(live["data"]["write_gap_ms_wallclock"].as_u64().unwrap() > 0);
}

#[test]
fn late_block_after_pause_is_absorbed_and_cut_repinned() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.late_block.set(true); // one straggler lands after pause

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("late_block_after_pause"));
    // The cut stays pinned to the snapshot block; the straggler became an
    // (empty) burn-off block instead of moving the cut.
    let snapped: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "SNAPSHOTTED" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(snapped["data"]["cut_height"].as_u64().unwrap(), 120);
    assert_eq!(snapped["data"]["burnoff_blocks"].as_u64().unwrap(), 2);
    assert_eq!(snapped["data"]["burnoff_transactions"].as_u64().unwrap(), 0);
}

#[test]
fn post_cut_transactions_abort_and_roll_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.late_block.set(true); // guarantees at least one post-cut block
    ops.burnoff_tx_per_block.set(1); // and it carries a transaction

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Aborted);
    assert!(!ops.ignited.get());
    assert_eq!(ops.resumes.get(), 1);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("transactions landed after the cut"));
}

#[test]
fn unreadable_post_cut_block_aborts_instead_of_counting_zero() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.late_block.set(true);
    ops.burnoff_read_fails.set(true);

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Aborted);
    assert!(!ops.ignited.get());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("burn-off audit could not read a post-cut block"));
}

#[test]
fn golden_mismatch_aborts_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path(), 120);
    // Verify mode with WRONG goldens (right tables, wrong roots).
    let mut bad = String::new();
    for table in verify::TABLE_NAMES {
        bad.push_str(&format!("{table} 0000000000000000\n"));
    }
    let golden_path = dir.path().join("bad-goldens.txt");
    std::fs::write(&golden_path, bad).unwrap();
    cfg.snapshot.capture_roots = None;
    cfg.snapshot.golden_roots = Some(golden_path);

    let ops = MockOps::new(dir.path(), 110);
    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Aborted);

    // Rollback ratchet: the source producer was resumed, the target never
    // ignited, and no user-visible hook ran (only the write freeze did).
    assert_eq!(ops.resumes.get(), 1);
    assert!(!ops.ignited.get());
    assert_eq!(*ops.hooks.borrow(), vec!["freeze-writes".to_string()]);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("fingerprints do not match goldens"));
    assert!(text.contains("source_producer_resumed"));
}

#[test]
fn sha256_manifest_mismatch_aborts_before_ignition() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path(), 120);
    cfg.snapshot.expected_sha256 = Some("00".repeat(32));

    let ops = MockOps::new(dir.path(), 110);
    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Aborted);
    assert!(!ops.ignited.get());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("sha256 mismatch"));
}

#[test]
fn resume_from_snapshotted_journal_skips_freeze_and_completes() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);

    // A previous agent run got to SNAPSHOTTED and crashed: seed the journal
    // and the snapshot file exactly as it would have left them.
    let cut = 121u64;
    let m = mini(cut as u32);
    let snapshot_path = dir.path().join("snapshot-cut.bin");
    std::fs::write(&snapshot_path, m.build()).unwrap();
    let entries = [
        serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "ARMED",
            "data": {"chain_id": hex::encode(CHAIN_ID), "freeze_height": 120}}),
        serde_json::json!({"seq": 1, "ts_ms": 2, "ts": "t", "kind": "transition", "state": "FROZEN",
            "data": {"chain_id": hex::encode(CHAIN_ID), "head_at_freeze": 120}}),
        serde_json::json!({"seq": 2, "ts_ms": 3, "ts": "t", "kind": "transition", "state": "SNAPSHOTTED",
            "data": {"snapshot_file": snapshot_path.display().to_string(), "cut_height": cut,
                     "cut_block_id": hex::encode(m.head_id()),
                     "last_source_block_time": "2024-01-01T00:00:00.000"}}),
    ];
    let lines: Vec<String> = entries.iter().map(|e| e.to_string()).collect();
    std::fs::write(&cfg.journal_path, lines.join("\n") + "\n").unwrap();

    let ops = MockOps::new(dir.path(), 121);
    ops.paused.set(true); // world state: producer is paused, as at crash time
    ops.target_head.set(cut); // the staged chain will present the cut height
    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);

    // The resumed run never re-froze or re-snapshotted: head never moved.
    assert_eq!(ops.head.get(), 121);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let frozen_count = text
        .lines()
        .filter(|l| l.contains("\"kind\":\"transition\"") && l.contains("\"state\":\"FROZEN\""))
        .count();
    assert_eq!(frozen_count, 1, "resume must not re-run FROZEN");
}

#[test]
fn verify_snapshot_is_deterministic_and_tamper_evident() {
    let dir = tempfile::tempdir().unwrap();
    let m = mini(500);
    let path = dir.path().join("mini.bin");
    std::fs::write(&path, m.build()).unwrap();

    let a = verify::verify_snapshot(&path, 1).unwrap();
    let b = verify::verify_snapshot(&path, 1).unwrap();
    assert_eq!(a.roots, b.roots);
    assert_eq!(a.sha256, b.sha256);
    assert_eq!(a.head_block_num, 500);
    assert_eq!(a.chain_id, hex::encode(CHAIN_ID));
    assert_eq!(a.roots.len(), verify::TABLE_NAMES.len());

    // cpu_scale is part of chain identity: scaled import fingerprints differ
    // exactly where CPU-denominated config lives.
    let scaled = verify::verify_snapshot(&path, 143).unwrap();
    assert_ne!(
        a.roots.iter().find(|(n, _)| n == "global_property").unwrap(),
        scaled.roots.iter().find(|(n, _)| n == "global_property").unwrap(),
    );

    // Golden round trip.
    let goldens = verify::parse_goldens(&verify::format_goldens(&a, 1)).unwrap();
    verify::compare_goldens(&a.roots, &goldens).unwrap();

    // Tampering flips the sha and (for a state byte) the fingerprints.
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    bytes[n / 2] ^= 0xFF;
    let tampered = dir.path().join("tampered.bin");
    std::fs::write(&tampered, bytes).unwrap();
    match verify::verify_snapshot(&tampered, 1) {
        Ok(t) => {
            assert_ne!(t.sha256, a.sha256);
            assert_ne!(t.roots, a.roots);
        }
        Err(_) => {} // structural corruption fails the parse — also detected
    }
}

#[test]
fn api_mode_flips_before_stopping_source_and_reads_never_gap() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = api_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    // A live chain that keeps drifting well past H (simulate_freeze): the
    // public URL pre-flip shows nodeos far ahead of the cut, so the flip
    // health gate's head-agreement check is genuinely discriminating.
    ops.drift.set(5);

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);

    // State order is the api-mode order: FLIPPED sits between IGNITED and
    // LIVE — nodeos outlives ignition, reads never gap.
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let states: Vec<String> = text
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .filter(|v| v["kind"] == "transition")
        .map(|v| v["state"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        states,
        ["ARMED", "FROZEN", "SNAPSHOTTED", "VERIFIED", "IGNITED", "FLIPPED", "LIVE"]
    );

    // Ratchet order: flip strictly BEFORE the source stop; stop strictly
    // BEFORE the on_live announcement. The producer pause was never touched.
    let hooks = ops.hooks.borrow();
    assert_eq!(
        *hooks,
        vec![
            "freeze-writes".to_string(),
            "flip-nginx".to_string(),
            "stop-nodeos".to_string(),
            "announce-live".to_string()
        ]
    );
    assert!(!ops.paused.get(), "api mode must never pause the source producer");
    assert_eq!(ops.resumes.get(), 0);
    assert!(ops.stopped.get(), "source nodeos stopped only at the end");

    // The journal's LIVE entry carries the api-mode evidence.
    let live: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .filter(|v: &serde_json::Value| v["state"] == "LIVE" && v["kind"] == "transition")
        .next_back()
        .unwrap();
    assert!(live["data"]["public_health"]["consecutive_ok_polls"].as_u64().unwrap() >= 2);
    assert!(live["data"]["ceremony_gap_ms_wallclock"].as_u64().unwrap() > 0);

    // Under simulate_freeze the cut lands at/after H — journaled honestly.
    let snapped: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "SNAPSHOTTED" && v["kind"] == "transition")
        .unwrap();
    assert!(snapped["data"]["cut_height"].as_u64().unwrap() >= 120);
    assert!(snapped["data"]["cut_vs_declared_h"].as_i64().unwrap() >= 0);
}

#[test]
fn api_mode_flip_health_failure_after_ignition_seals_without_stopping_source() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = api_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.drift.set(5);
    ops.flip_breaks_public.set(true); // nginx swaps to a dead upstream

    // The target was already ignited: a local failure SEALS (HALTED) instead of rolling back.
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED"), "{err}");

    // The source nodeos was never stopped, and nothing was undone automatically: reverting
    // public routing after ignition is a human decision (cutover.sh abort --force-after-ignite).
    assert!(!ops.stopped.get(), "source must NOT be stopped on a failed flip");
    let hooks = ops.hooks.borrow();
    assert!(!hooks.iter().any(|h| h == "revert-nginx"), "no automatic revert after IGNITED");
    assert!(!hooks.iter().any(|h| h == "stop-nodeos"));
    assert!(!hooks.iter().any(|h| h == "announce-live"));
    assert_eq!(ops.resumes.get(), 0);

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("HALTED: public URL did not serve the target"));
    assert!(text.contains(r#""sealed":true"#));
    assert!(!text.contains(r#""state":"ABORTED""#), "sealed, not rolled back");
}

/// hyperion-mode ceremony config: api mode + [hyperion] — /v2 history
/// continuity rides the same ceremony, one flip stage for both surfaces.
fn hyperion_test_config(dir: &std::path::Path, freeze_height: u64) -> Config {
    let toml_text = format!(
        r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1

[ceremony]
mode = "api"
freeze_height = {freeze_height}
simulate_freeze = true

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
stop_cmd = "stop-nodeos"

[snapshot]
staged_path = "{dir}/staged.bin"
capture_roots = "{dir}/captured-roots.txt"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 60

[flip]
cmd = "flip-nginx"
public_url = "http://mock-public"
revert_cmd = "revert-nginx"
health_polls = 2
head_tolerance = 2
health_timeout_secs = 5

[hyperion]
start_cmd = "hyperion-start"
health_url = "http://hyperion-local/v2/health"
hydration_timeout_secs = 30
boundary_path = "{dir}/boundary.json"
flip_cmd = "flip-v2"
revert_cmd = "revert-v2"
public_health_url = "http://v2-public/v2/health"

[hooks]
on_freeze = "freeze-writes"
on_live = "announce-live"
"#,
        dir = dir.display(),
    );
    let path = dir.join("ceremony-hyperion.toml");
    std::fs::write(&path, toml_text).unwrap();
    Config::load(&path).unwrap()
}

#[test]
fn hyperion_mode_hydrates_then_flips_both_surfaces() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = hyperion_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.drift.set(5);
    ops.hyperion_ready_after.set(3); // hydration takes a few health polls

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);

    // Order of user-visible acts: hyperion stood up + hydrated BEFORE any
    // flip; /v1 flip then /v2 flip in the same stage; source stop last.
    let hooks = ops.hooks.borrow();
    let pos = |name: &str| hooks.iter().position(|h| h == name).unwrap();
    assert!(pos("hyperion-start") < pos("flip-nginx"));
    assert!(pos("flip-nginx") < pos("flip-v2"));
    assert!(pos("flip-v2") < pos("stop-nodeos"));
    assert!(pos("stop-nodeos") < pos("announce-live"));

    // The boundary file carries the ceremony's cut for the federator.
    let boundary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("boundary.json")).unwrap())
            .unwrap();
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let snapped: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "SNAPSHOTTED" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(boundary["cut_block"], snapped["data"]["cut_height"]);
    assert_eq!(boundary["chain_id"], serde_json::json!(hex::encode(CHAIN_ID)));

    // Hydration is journaled evidence; FLIPPED and LIVE both carry v2 health.
    assert!(text.contains("hyperion_hydrated"));
    let flipped: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "FLIPPED" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(flipped["data"]["v2_health"]["federation"]["local"]["ok"], serde_json::json!(true));
    let live: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "LIVE" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(live["data"]["v2_health"]["federation"]["local"]["ok"], serde_json::json!(true));
}

#[test]
fn hyperion_hydration_timeout_aborts_before_any_flip() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = hyperion_test_config(dir.path(), 120);
    if let Some(h) = cfg.hyperion.as_mut() {
        h.hydration_timeout_secs = 1; // mock clock ticks 25ms per now_ms()
    }
    let ops = MockOps::new(dir.path(), 110);
    ops.drift.set(5);
    ops.hyperion_ready_after.set(u32::MAX); // indexer never catches up

    // After IGNITED a failure seals (HALTED) — and nothing user-visible was done.
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED"), "{err}");

    // The ratchet held: hyperion was started, but NOTHING user-visible
    // happened — no /v1 flip, no /v2 flip, source still serving.
    let hooks = ops.hooks.borrow();
    assert!(hooks.iter().any(|h| h == "hyperion-start"));
    assert!(!hooks.iter().any(|h| h == "flip-nginx"));
    assert!(!hooks.iter().any(|h| h == "flip-v2"));
    assert!(!hooks.iter().any(|h| h == "stop-nodeos"));
    assert!(!ops.stopped.get());
    assert!(ops.public_info("http://mock-public").unwrap().is_some());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("hyperion did not hydrate"));
}

#[test]
fn producer_schedule_at_h_pins_cut_to_exactly_h() {
    let dir = tempfile::tempdir().unwrap();
    // Rebuild the producer config with schedule_at_h + snapshot.dir + a
    // quiesce hook (the stand-in for "every producer paused").
    let toml_text = format!(
        r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1

[ceremony]
freeze_height = 120
freeze_strategy = "schedule_at_h"
quiescence_polls = 3

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
quiesce_cmd = "quiesce-p2p"

[snapshot]
staged_path = "{dir}/staged.bin"
capture_roots = "{dir}/captured-roots.txt"
dir = "{dir}"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 60

[hooks]
on_freeze = "freeze-writes"
post_ignite = "resume-traffic"
on_live = "flip-gateway"
"#,
        dir = dir.path().display(),
    );
    let path = dir.path().join("ceremony-sched.toml");
    std::fs::write(&path, toml_text).unwrap();
    let cfg = Config::load(&path).unwrap();

    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);
    assert_eq!(ops.scheduled_h.get(), 120, "snapshot scheduled at exactly H");

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("snapshot_scheduled_at"));
    assert!(text.contains("scheduled_snapshot_file"));
    // The cut is EXACTLY H (not "whenever create_snapshot ran"), and the
    // quiesce hook ran after the pause.
    let snapped: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "SNAPSHOTTED" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(snapped["data"]["cut_height"].as_u64().unwrap(), 120);
    assert!(text.contains("quiesce_cmd"));
    let hooks = ops.hooks.borrow();
    assert!(hooks.iter().any(|h| h == "quiesce-p2p"));
    // Head ran past H while waiting for finality: burn-off blocks audited.
    assert!(snapped["data"]["burnoff_blocks"].as_u64().unwrap() >= 1);
}

#[test]
fn producer_schedule_at_h_freezes_writes_before_h() {
    // Multi-BP rehearsal finding: freezing at head >= H left in-flight
    // transfers in H+1 on all five producers. With a lead, the write freeze
    // runs `freeze_lead_blocks` before H while the cut stays exactly H.
    let dir = tempfile::tempdir().unwrap();
    let toml_text = format!(
        r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1

[ceremony]
freeze_height = 120
freeze_strategy = "schedule_at_h"
freeze_lead_blocks = 20
quiescence_polls = 3

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
quiesce_cmd = "quiesce-p2p"

[snapshot]
staged_path = "{dir}/staged.bin"
capture_roots = "{dir}/captured-roots.txt"
dir = "{dir}"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 60

[hooks]
on_freeze = "freeze-writes"
post_ignite = "resume-traffic"
"#,
        dir = dir.path().display(),
    );
    let path = dir.path().join("ceremony-lead.toml");
    std::fs::write(&path, toml_text).unwrap();
    let cfg = Config::load(&path).unwrap();

    let ops = MockOps::new(dir.path(), 50);
    ops.schedule_ok.set(true);

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);
    assert_eq!(ops.freeze_head.get(), 100, "writes froze at H - lead");

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let lines: Vec<serde_json::Value> = text.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    let frozen = lines
        .iter()
        .find(|v| v["state"] == "FROZEN" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(frozen["data"]["freeze_at"].as_u64().unwrap(), 100);
    assert_eq!(frozen["data"]["declared_h"].as_u64().unwrap(), 120);
    let snapped = lines
        .iter()
        .find(|v| v["state"] == "SNAPSHOTTED" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(snapped["data"]["cut_height"].as_u64().unwrap(), 120, "cut is still exactly H");
}

#[test]
fn ceremony_journals_advisory_stubbed_intrinsic_scan() {
    // The VERIFIED step scans the ACTUAL cut snapshot for unserved env
    // imports and journals the result — advisory evidence, never a gate.
    // MiniSnapshot carries zero code objects, so the scan must report clean
    // and the ceremony must still reach LIVE.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let scan_line: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["data"]["stubbed_intrinsic_scan"].is_object())
        .expect("scan evidence journaled");
    let scan = &scan_line["data"]["stubbed_intrinsic_scan"];
    assert_eq!(scan["advisory"], serde_json::json!(true));
    assert_eq!(scan["code_objects"], serde_json::json!(0));
    assert_eq!(scan["at_risk"], serde_json::json!(0));
    assert_eq!(scan["served_imports"], serde_json::json!(169));

    // The table is persisted beside the journal for `pulse-cutover report`.
    let table = std::fs::read_to_string(dir.path().join("scan-contracts.txt")).unwrap();
    assert!(table.contains("no stub-trap exposure"));
}

#[test]
fn armed_prescan_journals_at_risk_table_before_freeze() {
    // snapshot.prescan_path: a rehearsal snapshot staged before the ceremony
    // gets scanned during ARMED preflight; the ceremony continues regardless.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path(), 120);
    let prescan = dir.path().join("rehearsal-snapshot.bin");
    std::fs::write(&prescan, mini(50).build()).unwrap();
    cfg.snapshot.prescan_path = Some(prescan);

    let ops = MockOps::new(dir.path(), 110);
    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Live);

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    // Two scans journaled: the ARMED prescan and the VERIFIED-step scan of
    // the actual cut.
    let scans = text
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .filter(|v| v["data"]["stubbed_intrinsic_scan"].is_object())
        .count();
    assert_eq!(scans, 2);
    let first: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["data"]["stubbed_intrinsic_scan"].is_object())
        .unwrap();
    assert_eq!(first["state"], "ARMED", "prescan runs during ARMED preflight");
}

#[test]
fn hydration_predicate_accepts_idle_at_cut_indexer_warning() {
    // Observed live (idle imported chain): Indexer reports Warning with
    // last_indexed_block: 0 because zero post-cut blocks exist — hydration
    // must pass (idle_at_cut), an all-OK gate would deadlock.
    use pulse_cutover::machine::hyperion_hydrated;
    let idle = serde_json::json!({"health": [
        {"service": "Elasticsearch", "status": "OK"},
        {"service": "PulseVM-RPC", "status": "OK",
         "service_data": {"head_block_num": 401579371, "last_irreversible_block": 401579371}},
        {"service": "Indexer", "status": "Warning",
         "service_data": {"head_block_num": 401579371, "last_indexed_block": 0}}
    ]});
    let ev = hyperion_hydrated(&idle, 401579371, 0).expect("idle-at-cut hydrates");
    assert_eq!(ev["idle_at_cut"], serde_json::json!(true));

    // But a Warning indexer BEHIND a moving head must NOT pass...
    let behind = serde_json::json!({"health": [
        {"service": "Elasticsearch", "status": "OK"},
        {"service": "PulseVM-RPC", "status": "OK", "service_data": {"head_block_num": 401579400}},
        {"service": "Indexer", "status": "Warning",
         "service_data": {"head_block_num": 401579400, "last_indexed_block": 0}}
    ]});
    assert!(hyperion_hydrated(&behind, 401579371, 0).is_none());

    // ...and a broken Elasticsearch blocks hydration even when idle.
    let es_down = serde_json::json!({"health": [
        {"service": "Elasticsearch", "status": "DOWN"},
        {"service": "PulseVM-RPC", "status": "OK", "service_data": {"head_block_num": 401579371}},
        {"service": "Indexer", "status": "Warning",
         "service_data": {"head_block_num": 401579371, "last_indexed_block": 0}}
    ]});
    assert!(hyperion_hydrated(&es_down, 401579371, 0).is_none());
}

/// Upstream alignment knob (MetalBlockchain/pulsevm#61): when an
/// `xpr_state_fingerprint`-compatible binary is configured and present, the
/// VERIFIED journal entry carries its report next to our fingerprints; when
/// the binary is missing the ceremony proceeds and the skip is journaled.
#[test]
fn upstream_fingerprint_runs_alongside_ours_and_noops_when_missing() {
    // Present: a fake tool emitting the #61 report format.
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("xpr_state_fingerprint");
    std::fs::write(
        &script,
        "#!/bin/sh\necho revision 9\necho state_root cafef00d\necho \"table account bytes=1 sha256=ab\"\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut cfg = test_config(dir.path(), 120);
    cfg.snapshot.upstream_fingerprint_bin = Some(script.clone());
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let verified: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "VERIFIED" && v["kind"] == "transition")
        .unwrap();
    let upstream = &verified["data"]["upstream_fingerprint"];
    assert_eq!(upstream["status"], "ran");
    assert_eq!(upstream["state_root"], "cafef00d");
    // Ours still ran too — never replaced.
    assert!(verified["data"]["fingerprints"]["account"].is_string());

    // Missing: same ceremony, binary absent — clean journaled no-op.
    let dir2 = tempfile::tempdir().unwrap();
    let mut cfg2 = test_config(dir2.path(), 120);
    cfg2.snapshot.upstream_fingerprint_bin = Some(dir2.path().join("not-built-yet"));
    let ops2 = MockOps::new(dir2.path(), 110);
    assert_eq!(run_machine(&cfg2, &ops2), State::Live);
    let text2 = std::fs::read_to_string(&cfg2.journal_path).unwrap();
    let verified2: serde_json::Value = text2
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "VERIFIED" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(
        verified2["data"]["upstream_fingerprint"]["status"],
        "skipped_missing_binary"
    );
}

// ---------------------------------------------------------------------------
// import_backend = "upstream": the ceremony drives the official #61 pipeline
// (export.sh -> xpr_import_check) and verifies with upstream's own tools
// (xpr_19_table_compare gate + xpr_state_fingerprint). Fake tools stand in
// for the real binaries; the pipeline's file-output contract is exercised
// for real (SHiP log, manifest.env, checkpoint + .manifest.json).
// ---------------------------------------------------------------------------

fn write_script(path: &std::path::Path, body: &str) {
    std::fs::write(path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn stage_fake_upstream_tools(dir: &std::path::Path, compare_exit: i32) {
    let d = dir.display();
    // export.sh stand-in: nests its own work dir (the docker-mount shape),
    // emits the SHiP log + a manifest.env whose INPUT_SNAPSHOT_SHA256 is the
    // REAL sha256 of the snapshot it was handed.
    write_script(
        &dir.join("fake-export.sh"),
        &format!(
            "#!/bin/sh\nset -e\nsnap=\"$1\"; out=\"$2\"\n\
             sha() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\"; else shasum -a 256 \"$1\"; fi | awk '{{print $1}}'; }}\n\
             mkdir -p \"$out/work/state-history\"\n\
             printf SHIPLOG > \"$out/work/state-history/chain_state_history.log\"\n\
             if [ -z \"$NO_SIDECAR\" ]; then printf '{{}}' > \"$out/work/deferred-transactions.json\"; fi\n\
             {{ echo \"XPR_CORE_REVISION=d133c641\"; echo \"INPUT_SNAPSHOT_SHA256=$(sha \"$snap\")\"; \
                echo \"CHAIN_STATE_HISTORY_SHA256=$(sha \"$out/work/state-history/chain_state_history.log\")\"; }} > \"$out/work/manifest.env\"\n\
             echo \"exported full XPR chain-state history to $out/work\"\n"
        ),
    );
    // xpr_import_check stand-in: writes the checkpoint + the manifest that
    // binds it to the cut (facts recorded by the mock's create_snapshot).
    write_script(
        &dir.join("fake-import.sh"),
        &format!(
            "#!/bin/sh\nset -e\n. {d}/cut-facts.env\n\
             sha() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\"; else shasum -a 256 \"$1\"; fi | awk '{{print $1}}'; }}\n\
             [ -f \"$4\" ] || {{ echo 'usage: sidecar (4th arg) missing' >&2; exit 2; }}\n\
             printf CKPT > \"$3\"\n\
             printf '{{\"checkpoint_sha256\":\"%s\",\"checkpoint_revision\":%s,\"source_block_id\":\"%s\"}}' \
               \"$(sha \"$3\")\" \"$CUT_HEIGHT\" \"$CUT_BLOCK_ID\" > \"$3.manifest.json\"\n\
             echo 'XPR state imported successfully: ImportSummary {{ accounts: 1 }}'\n"
        ),
    );
    write_script(
        &dir.join("fake-compare.sh"),
        &format!(
            "#!/bin/sh\n\
             [ -f \"$5\" ] || {{ echo 'usage: sidecar (5th arg) missing' >&2; exit 2; }}\n\
             if [ {compare_exit} -ne 0 ]; then echo 'table permission: nodeos=1 arena=2' >&2; exit {compare_exit}; fi\n\
             echo 'table account: rows=1 sha256=aa55'\necho 'table permission: rows=2 sha256=bb66'\nexit 0\n"
        ),
    );
    write_script(
        &dir.join("fake-fingerprint.sh"),
        "#!/bin/sh\necho 'revision 999'\necho 'state_root feedfacecafebeef'\necho 'table account bytes=10 sha256=aa55'\n",
    );
}

fn upstream_test_config(dir: &std::path::Path, freeze_height: u64) -> Config {
    let toml_text = format!(
        r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1

[ceremony]
freeze_height = {freeze_height}
quiescence_polls = 3
import_backend = "upstream"

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"

[snapshot]
staged_path = "{dir}/staged.bin"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 60

[upstream]
work_dir = "{dir}/upstream-work"
export_cmd = "sh {dir}/fake-export.sh {{snapshot}} {{export_dir}}"
import_bin = "{dir}/fake-import.sh"
fingerprint_bin = "{dir}/fake-fingerprint.sh"
compare_bin = "{dir}/fake-compare.sh"
"#,
        dir = dir.display(),
    );
    let path = dir.join("ceremony-upstream.toml");
    std::fs::write(&path, toml_text).unwrap();
    Config::load(&path).unwrap()
}

#[test]
fn upstream_backend_verifies_with_official_tools_and_stubs_ignite() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    // VERIFIED is reached with the official tools; ignition from the
    // checkpoint is pending #61, so the ceremony stops there — safely.
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let entries: Vec<serde_json::Value> =
        text.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    let verified = entries
        .iter()
        .find(|v| v["state"] == "VERIFIED" && v["kind"] == "transition")
        .expect("VERIFIED reached");
    let data = &verified["data"];
    assert!(data["verify_backend"].as_str().unwrap().contains("upstream"));
    assert_eq!(data["table_compare"], "MATCH");
    assert_eq!(data["state_root"], "feedfacecafebeef");
    assert!(data["checkpoint_sha256"].as_str().unwrap().len() == 64);
    // The export manifest's input hash was verified against the cut.
    assert!(data["export_manifest"]["INPUT_SNAPSHOT_SHA256"].is_string());
    // The 19-table compare ran and its per-table rows are journaled.
    let compare = entries
        .iter()
        .find(|v| v["data"].get("upstream_19_table_compare").is_some()
            && v["data"]["upstream_19_table_compare"].is_object())
        .expect("compare evidence");
    assert_eq!(compare["data"]["upstream_19_table_compare"]["result"], "MATCH");
    // No fork-importer artifacts: staged .bin never written, no captured roots.
    assert!(!dir.path().join("staged.bin").exists());
    // The stop is the documented #61 stub, with the remaining work listed.
    let abort_err = entries
        .iter()
        .find(|v| v["kind"] == "error")
        .expect("journaled abort reason");
    assert!(abort_err["data"]["message"].as_str().unwrap().contains("#61"));
    let remaining = abort_err["data"]["detail"]["remaining"].to_string();
    assert!(remaining.contains("TAPOS") && remaining.contains("chain_id"));
    assert!(abort_err["data"]["detail"]["remaining"].is_array());
}

#[test]
fn upstream_export_without_sidecar_fails_verification() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    // An export that does not write deferred-transactions.json (no
    // --deferred-sidecar): the dedupe set would be missing on the target.
    let export = dir.path().join("fake-export.sh");
    let body = std::fs::read_to_string(&export).unwrap();
    let body: String = body.lines().filter(|l| !l.contains("deferred-transactions.json")).collect::<Vec<_>>().join("\n");
    write_script(&export, &format!("{body}\n"));
    let cfg = upstream_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("no deferred-transactions.json sidecar"));
    assert!(!text.lines().any(|l| {
        let v: serde_json::Value = serde_json::from_str(l).unwrap();
        v["state"] == "VERIFIED" && v["kind"] == "transition"
    }));
}

#[test]
fn upstream_table_compare_mismatch_fails_verification() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 1); // compare exits non-zero
    let cfg = upstream_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    // Never VERIFIED; the error names the official tool and the mismatch.
    assert!(!text.lines().any(|l| {
        let v: serde_json::Value = serde_json::from_str(l).unwrap();
        v["state"] == "VERIFIED" && v["kind"] == "transition"
    }));
    assert!(text.contains("xpr_19_table_compare FAILED"));
    // Producer-mode rollback ran: the source producer was resumed.
    assert_eq!(ops.resumes.get(), 1);
}

#[test]
fn upstream_backend_requires_upstream_section() {
    let dir = tempfile::tempdir().unwrap();
    let toml_text = format!(
        "journal_path = \"{d}/j.jsonl\"\n[ceremony]\nfreeze_height = 5\nimport_backend = \"upstream\"\n\
         [source]\nrpc_url = \"http://m\"\nproducer_api_url = \"http://m\"\n\
         [snapshot]\nstaged_path = \"{d}/s.bin\"\n\
         [target]\nmetalgo_unit = \"m\"\nrpc_url = \"http://m\"\n",
        d = dir.path().display()
    );
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, toml_text).unwrap();
    let err = Config::load(&path).unwrap_err();
    assert!(err.contains("[upstream]"));
}

// ---- coordinated arming ---------------------------------------------------------------------------

fn coord_key() -> ed25519_dalek::SigningKey {
    ed25519_dalek::SigningKey::from_bytes(&[42u8; 32])
}

fn signed(payload: serde_json::Value) -> serde_json::Value {
    use ed25519_dalek::Signer;
    let sk = coord_key();
    let p = payload.to_string();
    serde_json::json!({"payload": p, "sig": hex::encode(sk.sign(p.as_bytes()).to_bytes()),
        "key": hex::encode(sk.verifying_key().to_bytes())})
}

fn coord_config(dir: &std::path::Path, freeze_height: u64, quorum: usize, timeout: u64) -> Config {
    let base = test_config(dir, freeze_height);
    let _ = base;
    let text = std::fs::read_to_string(dir.join("ceremony.toml")).unwrap();
    let text = format!("{text}\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{}\"]\nevent_id = \"e1\"\nfleet_quorum = {quorum}\nfleet_timeout_secs = {timeout}\n",
        hex::encode(coord_key().verifying_key().to_bytes()));
    let path = dir.join("ceremony-coord.toml");
    std::fs::write(&path, text).unwrap();
    Config::load(&path).unwrap()
}

#[test]
fn signed_coordinator_abort_while_armed_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = coord_config(dir.path(), 120, 0, 60);
    let ops = MockOps::new(dir.path(), 50);
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({
        "abort": signed(serde_json::json!({"type": "abort", "network": "rehearsal", "event_id": "e1"}))}));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("coordinator aborted the event (signed) before the freeze"));
    assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"), "nothing was frozen");
}

#[test]
fn abort_for_another_event_or_unsigned_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = coord_config(dir.path(), 120, 0, 60);
    let ops = MockOps::new(dir.path(), 110);
    let mut forged = signed(serde_json::json!({"type": "abort", "network": "rehearsal", "event_id": "e1"}));
    forged["sig"] = serde_json::json!("00".repeat(64));
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"abort": forged}));
    assert_eq!(run_machine(&cfg, &ops), State::Live, "a forged abort must not stop the ceremony");
}

#[test]
fn fleet_gate_holds_ignite_until_quorum_agrees() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = coord_config(dir.path(), 120, 3, 60);
    let ops = MockOps::new(dir.path(), 110);
    ops.fleet_agree.set(3);
    ops.fleet_agree_after.set(4);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""fleet_gate":{"agreeing":3,"quorum":3,"#), "gate journaled the agreeing count");
    assert!(ops.fleet_polls.get() >= 4, "ignite waited for the fleet");
}

#[test]
fn fleet_gate_times_out_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = coord_config(dir.path(), 120, 3, 5);
    let ops = MockOps::new(dir.path(), 110);
    ops.fleet_agree.set(1);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("fleet did not reach verified agreement"));
    assert!(ops.resumes.get() >= 1, "source producer resumed");
}


// ---- Astra review fixes (2026-09-29) ---------------------------------------------------------------

#[test]
fn readiness_profile_refuses_to_run_before_any_side_effect() {
    let dir = tempfile::tempdir().unwrap();
    // What beacon-install.sh writes: api mode, no flip/stop_cmd, no H — valid only as readiness.
    let text = format!(r#"
journal_path = "{dir}/journal.jsonl"
[ceremony]
profile = "readiness"
mode = "api"
[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
[snapshot]
staged_path = "{dir}/staged.bin"
[target]
metalgo_unit = "metalgo"
rpc_url = "http://127.0.0.1:9650/ext/bc/NOT-CONFIGURED/rpc"
[beacon]
url = "https://mc.example/api/report"
producer = "bp1"
network = "testnet"
"#, dir = dir.path().display());
    let cfg = load_toml(dir.path(), "beacon.toml", &text).expect("readiness config loads for the beacon");
    assert!(cfg.ensure_ceremony_profile().is_err());
    let ops = MockOps::new(dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("READINESS-ONLY"), "{err}");
    assert_eq!(ops.source_calls.get(), 0, "refused before touching the chain");
    assert!(ops.hooks.borrow().is_empty(), "no hook ran");
    assert!(!ops.paused.get());
    // The same file WITHOUT the readiness profile is not a valid ceremony config.
    let as_ceremony = text.replace("profile = \"readiness\"\n", "");
    assert!(load_toml(dir.path(), "beacon2.toml", &as_ceremony).is_err());
}

#[test]
fn derived_h_requires_explicit_opt_in() {
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ test_config(dir.path(), 120); dir.path().join("ceremony.toml") }).unwrap();
    let margin = base.replace("freeze_height = 120", "freeze_height = 0\nfreeze_margin = 10");
    let err = load_toml(dir.path(), "m.toml", &margin).unwrap_err();
    assert!(err.contains("derive_h_at_arm"), "{err}");
    let opted = margin.replace("freeze_margin = 10", "freeze_margin = 10\nderive_h_at_arm = true");
    assert!(load_toml(dir.path(), "m2.toml", &opted).is_ok());
}

#[test]
fn beacon_url_must_be_https_unless_local() {
    use pulse_cutover::config::check_beacon_url;
    assert!(check_beacon_url("https://mc.example/api/report").is_ok());
    assert!(check_beacon_url("http://127.0.0.1:8787/api/report").is_ok());
    assert!(check_beacon_url("http://localhost/api/report").is_ok());
    assert!(check_beacon_url("http://[::1]:8787/api/report").is_ok());
    assert!(check_beacon_url("http://mc.example/api/report").is_err());
    assert!(check_beacon_url("").is_ok(), "empty = print-only --once");
    // Astra 2026-09-30 #5: authority tricks that hand-parsing accepted.
    assert!(check_beacon_url("http://localhost:80@example.org/api/report").is_err(), "userinfo: real host is example.org");
    assert!(check_beacon_url("http://localhost.example.org/api/report").is_err());
    assert!(check_beacon_url("http://127.0.0.1.nip.io/api/report").is_err());
    assert!(check_beacon_url("https://user:pw@mc.example/api/report").is_err(), "no userinfo on https either");
    assert!(check_beacon_url("ftp://localhost/x").is_err());
}

#[test]
fn pause_at_h_snapshot_past_h_aborts_unless_rehearsal_allows_it() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.drift.set(3); // head jumps past H: create_snapshot lands at 121, not 120
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("snapshot is not at H"));
    assert!(!text.contains(r#""state":"SNAPSHOTTED""#));
    assert!(ops.resumes.get() == 0 || !ops.paused.get(), "source left producing");

    let dir2 = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ test_config(dir2.path(), 120); dir2.path().join("ceremony.toml") }).unwrap();
    let cfg2 = load_toml(dir2.path(), "i.toml", &base.replace("quiescence_polls = 3", "quiescence_polls = 3\nallow_inexact_cut = true")).unwrap();
    let ops2 = MockOps::new(dir2.path(), 110);
    ops2.drift.set(3);
    assert_eq!(run_machine(&cfg2, &ops2), State::Live);
    assert!(std::fs::read_to_string(&cfg2.journal_path).unwrap().contains("inexact_cut"));
}

fn sched_config(dir: &std::path::Path) -> Config {
    let text = format!(r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1
[ceremony]
freeze_height = 120
freeze_strategy = "schedule_at_h"
quiescence_polls = 3
[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
[snapshot]
staged_path = "{dir}/staged.bin"
capture_roots = "{dir}/captured-roots.txt"
dir = "{dir}"
[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 60
[hooks]
on_freeze = "freeze-writes"
post_ignite = "resume-traffic"
"#, dir = dir.display());
    load_toml(dir, "sched.toml", &text).unwrap()
}

#[test]
fn schedule_at_h_without_scheduler_aborts_instead_of_falling_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = sched_config(dir.path());
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(false);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("exact-H cut cannot be guaranteed"));
    assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"), "aborted before freezing");
}

#[test]
fn resumed_frozen_run_keeps_waiting_for_the_scheduled_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = sched_config(dir.path());
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.schedule_snapshot(120).unwrap(); // the crashed run scheduled it and staged the file
    let entries = [
        serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "ARMED",
            "data": {"chain_id": hex::encode(CHAIN_ID), "resolved_h": 120}}),
        serde_json::json!({"seq": 1, "ts_ms": 2, "ts": "t", "kind": "evidence", "state": "ARMED",
            "data": {"snapshot_scheduled_at": 120}}),
        serde_json::json!({"seq": 2, "ts_ms": 3, "ts": "t", "kind": "transition", "state": "FROZEN",
            "data": {"chain_id": hex::encode(CHAIN_ID)}}),
    ];
    std::fs::write(&cfg.journal_path, entries.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("\n") + "\n").unwrap();
    ops.head.set(125);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("scheduled_snapshot_file"), "resume used the scheduled snapshot");
    let snapped: serde_json::Value = text.lines().map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "SNAPSHOTTED" && v["kind"] == "transition").unwrap();
    assert_eq!(snapped["data"]["cut_height"], 120, "cut stays exactly H after a crash");
}

#[test]
fn api_mode_without_simulate_cuts_exactly_h_via_scheduler() {
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ api_test_config(dir.path(), 120); dir.path().join("ceremony-api.toml") }).unwrap();
    let exact = base.replace("simulate_freeze = true", "")
        .replace("capture_roots", &format!("dir = \"{}\"\ncapture_roots", dir.path().display()));
    let cfg = load_toml(dir.path(), "api-exact.toml", &exact).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.drift.set(5);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let snapped: serde_json::Value = text.lines().map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "SNAPSHOTTED" && v["kind"] == "transition").unwrap();
    assert_eq!(snapped["data"]["cut_height"], 120);
    assert_eq!(snapped["data"]["cut_vs_declared_h"], 0);
    // ...and without the scheduler it refuses rather than cutting at "whatever LIB is".
    let dir2 = tempfile::tempdir().unwrap();
    let exact2 = exact.replace(&dir.path().display().to_string(), &dir2.path().display().to_string());
    let cfg2 = load_toml(dir2.path(), "api-exact.toml", &exact2).unwrap();
    let ops2 = MockOps::new(dir2.path(), 110);
    assert_eq!(run_machine(&cfg2, &ops2), State::Aborted);
    assert!(std::fs::read_to_string(&cfg2.journal_path).unwrap().contains("api mode cannot cut at exactly H"));
    // api without simulate_freeze and without snapshot.dir is a config error.
    assert!(load_toml(dir2.path(), "bad.toml", &base.replace("simulate_freeze = true", "")).is_err());
}

#[test]
fn wrong_lineage_after_ignite_started_halts_without_resuming_source() {
    // Astra 2026-09-30 #1: the lineage check runs AFTER the ignite command; the target may be
    // running, so a mismatch must seal, never resume the source.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED"), "{err}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("wrong lineage"));
    assert!(text.contains(r#""side_effect":"ignite_started""#));
    assert!(text.contains(r#""kind":"transition","state":"HALTED""#), "HALTED is a durable journaled state");
    assert!(!text.contains(r#""state":"IGNITED""#));
    assert!(!text.contains(r#""state":"ABORTED""#));
    assert_eq!(ops.resumes.get(), 0, "the source producer must NOT be resumed once ignition started");
    assert!(ops.paused.get());
}

#[test]
fn failing_ignite_command_halts_without_resuming_source() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.ignite_fails.set(true);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED") && err.contains("ignition command failed"), "{err}");
    assert_eq!(ops.resumes.get(), 0);
    assert!(!ops.hooks.borrow().iter().any(|h| h.contains("abort")), "on_abort must not run");
}

#[test]
fn halted_journal_is_durable_and_only_unhalt_clears_it() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    // A restarted run refuses to continue OR roll back.
    let ops2 = MockOps::new(dir.path(), 200);
    let err = run_machine_result(&cfg, &ops2).unwrap_err();
    assert!(err.contains("HALTED (journaled)"), "{err}");
    assert_eq!(ops2.resumes.get(), 0);
    assert_eq!(ops2.source_calls.get(), 0, "refused before touching anything");
    // The operator clears it with the real binary (journaled), returning to the halted-from state.
    let bin = env!("CARGO_BIN_EXE_pulse-cutover");
    let cfgp = dir.path().join("ceremony.toml");
    let out = std::process::Command::new(bin).args(["unhalt", "--config"]).arg(&cfgp).output().unwrap();
    assert!(!out.status.success(), "unhalt without --i-understand is refused");
    let out = std::process::Command::new(bin).args(["unhalt", "--config"]).arg(&cfgp).arg("--i-understand").output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.state, Some(State::Verified));
    assert!(rec.unhalted);
    assert!(rec.reached_ignited, "ignition-started stays recorded after unhalt");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("unhalted_by"));
}

#[test]
fn happy_path_journals_verified_lineage_and_sustained_live() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""lineage_at_cut":"verified""#));
    let live: serde_json::Value = text.lines().map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "LIVE" && v["kind"] == "transition").unwrap();
    assert_eq!(live["data"]["sustained"]["sustain_secs"], 60);
    assert!(text.contains(r#""side_effect":"ignite_started""#));
}

#[test]
fn post_live_stall_halts_sealed_without_resuming_source() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_stall_after.set(8); // produces a few blocks past the goal, then stops
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED"), "{err}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("live_stall"));
    assert!(text.contains("target stalled during the sustained LIVE window"));
    assert!(!text.contains(r#""state":"LIVE""#));
    assert_eq!(ops.resumes.get(), 0, "sealed: the paused source producer was NOT resumed");
    assert!(ops.paused.get());
}

#[test]
fn failing_on_live_hook_halts_and_never_journals_live() {
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ test_config(dir.path(), 120); dir.path().join("ceremony.toml") }).unwrap();
    let cfg = load_toml(dir.path(), "l.toml", &base.replace("on_live = \"flip-gateway\"", "on_live = \"fail-live\"")).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED") && err.contains("on_live hook failed"), "{err}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(!text.contains(r#""kind":"transition","state":"LIVE""#), "LIVE only after on_live succeeds");
    assert_eq!(ops.resumes.get(), 0);
}

#[test]
fn slow_target_polls_count_as_gaps_in_the_sustained_live_window() {
    // Each target poll takes 25 s of wall clock (RPC latency) > live_max_gap_secs (20): even
    // though every poll shows progress, the observed interval is a stall.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_latency_ms.set(25_000);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED"), "{err}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("live_stall"), "{text}");
    assert!(!text.contains(r#""kind":"transition","state":"LIVE""#));
}

#[test]
fn live_outage_metric_includes_sustain_window_and_on_live() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let live: serde_json::Value = text.lines().map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "LIVE" && v["kind"] == "transition").unwrap();
    let gap = live["data"]["write_gap_ms_wallclock"].as_u64().unwrap();
    let first = live["data"]["first_progress_gap_ms_wallclock"].as_u64().unwrap();
    assert!(gap >= first + 60_000, "gap {gap} must include the 60 s sustain window (first progress {first})");
    let on_live = text.lines().position(|l| l.contains("on_live_hook")).unwrap();
    let live_line = text.lines().position(|l| l.contains(r#""kind":"transition","state":"LIVE""#)).unwrap();
    assert!(on_live < live_line, "on_live ran before LIVE was journaled");
}

#[test]
fn quorum_timeout_after_ignited_halts_instead_of_resuming_source() {
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ test_config(dir.path(), 120); dir.path().join("ceremony.toml") }).unwrap();
    // No traffic hook: the ignited target never mints past the cut.
    let cfg = load_toml(dir.path(), "q.toml", &base.replace("post_ignite = \"resume-traffic\"\n", "")
        .replace("quorum_timeout_secs = 60", "quorum_timeout_secs = 2")).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED"), "{err}");
    assert_eq!(ops.resumes.get(), 0);
    assert!(!ops.hooks.borrow().iter().any(|h| h.contains("abort")), "on_abort (re-open writes) must not run");
}

#[test]
fn failing_required_hooks_abort_before_ignition_and_halt_after() {
    // on_freeze failing in api mode used to be journaled and ignored.
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ api_test_config(dir.path(), 120); dir.path().join("ceremony-api.toml") }).unwrap();
    let cfg = load_toml(dir.path(), "f.toml", &base.replace("on_freeze = \"freeze-writes\"", "on_freeze = \"fail-freeze\"")).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("write-freeze hook failed"));

    // post_ignite failing (the run-5 non-executable hook) halts, sealed.
    let dir2 = tempfile::tempdir().unwrap();
    let base2 = std::fs::read_to_string({ test_config(dir2.path(), 120); dir2.path().join("ceremony.toml") }).unwrap();
    let cfg2 = load_toml(dir2.path(), "p.toml", &base2.replace("post_ignite = \"resume-traffic\"", "post_ignite = \"fail-post-ignite\"")).unwrap();
    let ops2 = MockOps::new(dir2.path(), 110);
    let err = run_machine_result(&cfg2, &ops2).unwrap_err();
    assert!(err.contains("post_ignite hook failed"), "{err}");
    assert_eq!(ops2.resumes.get(), 0);
}

#[test]
fn second_process_on_the_same_journal_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let (held, _) = Journal::open(&cfg.journal_path).unwrap();
    // A genuinely separate process (the real binary) must be refused while this one holds it.
    let bin = env!("CARGO_BIN_EXE_pulse-cutover");
    let out = std::process::Command::new(bin).args(["run", "--config"]).arg(dir.path().join("ceremony.toml")).output().unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("already running"), "{stderr}");
    drop(held);
    assert!(Journal::open(&cfg.journal_path).is_ok(), "lock released on drop");
}

#[test]
fn torn_last_line_is_set_aside_but_a_corrupt_middle_line_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.jsonl");
    let good = serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "ARMED",
        "data": {"chain_id": "ab", "resolved_h": 120}}).to_string();
    std::fs::write(&path, format!("{good}\n{{\"seq\":1,\"ts_ms\":2,\"kin")).unwrap();
    let (mut j, rec) = Journal::open(&path).unwrap();
    assert!(rec.torn_tail);
    assert_eq!(rec.state, Some(State::Armed));
    assert_eq!(rec.resolved_h, Some(120));
    j.evidence(State::Armed, serde_json::json!({"after": "repair"})).unwrap();
    drop(j);
    // The next append landed on a clean line: the whole journal parses again.
    assert!(Journal::replay(&path).is_ok());
    assert!(std::fs::read_dir(dir.path()).unwrap().any(|e| e.unwrap().file_name().to_string_lossy().contains(".torn-")));

    let bad = dir.path().join("bad.jsonl");
    std::fs::write(&bad, format!("{good}\nnot json at all\n{good}\n")).unwrap();
    let err = Journal::replay(&bad).unwrap_err();
    assert!(err.contains("corrupt journal line 2"), "{err}");

    // A COMPLETE (newline-terminated) last line that does not parse is corruption, not a torn
    // write: refuse instead of silently dropping what may be an authority-relevant record.
    let done = dir.path().join("done.jsonl");
    std::fs::write(&done, format!("{good}\n{{\"seq\":1,\"garbled\n")).unwrap();
    let err = Journal::open(&done).err().expect("must refuse");
    assert!(err.contains("corruption, not a torn"), "{err}");
    assert!(Journal::replay(&done).is_err());
}

#[test]
fn crash_after_ignite_started_before_ignited_resumes_into_halt() {
    // Drive a real ceremony to completion, then cut the journal back to the moment right after
    // `ignite_started` was journaled (the process "died" inside the ignite command).
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let keep: Vec<&str> = text.lines().take_while(|l| !l.contains(r#""state":"IGNITED""#)).collect();
    assert!(keep.last().unwrap().contains("ignite_started") || keep.iter().any(|l| l.contains("ignite_started")));
    let cut = keep.iter().position(|l| l.contains("ignite_started")).unwrap();
    std::fs::write(&cfg.journal_path, keep[..=cut].join("\n") + "\n").unwrap();
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.state, Some(State::Verified));
    assert!(rec.reached_ignited, "ignite_started alone marks the boundary");
    // Recovery: a fresh agent on the same journal must seal, not re-ignite and not roll back.
    let ops = MockOps::new(dir.path(), 200);
    ops.paused.set(true);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED") && err.contains("ignition may have started"), "{err}");
    assert_eq!(ops.resumes.get(), 0);
    assert!(!ops.ignited.get(), "did not re-run the ignite command on its own");
}

#[test]
fn crash_mid_staging_resumes_and_accepts_its_own_staged_snapshot() {
    // Astra 2026-09-30 #2: a crash after staging but before VERIFIED left the staged file; the
    // resumed preflight used to call it stale and abort.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Live);
    let rec_full = Journal::replay(&cfg.journal_path).unwrap();
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    let staged = lines.iter().position(|l| l.contains("staged_artifact")).expect("staging identity journaled");
    std::fs::write(&cfg.journal_path, lines[..=staged].join("\n") + "\n").unwrap();
    assert!(cfg.snapshot.staged_path.exists());
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.state, Some(State::Snapshotted));
    assert!(rec.staged_sha256.is_some());
    let ops = MockOps::new(dir.path(), rec_full.cut_height.unwrap() + 3);
    ops.paused.set(true);
    ops.target_head.set(rec_full.cut_height.unwrap());
    let st = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("matches this ceremony's journaled staged_artifact"), "{text}");
    assert!(!text.contains("stale snapshot from a previous ceremony"));
    assert_eq!(st.unwrap(), State::Live);
}

#[test]
fn side_effects_started_before_a_crash_are_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.jsonl");
    let lines = [
        serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "IGNITED", "data": {}}),
        serde_json::json!({"seq": 1, "ts_ms": 2, "ts": "t", "kind": "evidence", "state": "IGNITED", "data": {"side_effect": "flip_cmd"}}),
    ];
    std::fs::write(&path, lines.iter().map(|l| l.to_string()).collect::<Vec<_>>().join("\n") + "\n").unwrap();
    let rec = Journal::replay(&path).unwrap();
    assert!(rec.reached_ignited);
    assert_eq!(rec.side_effects, vec!["flip_cmd".to_string()]);
}

#[test]
fn hook_completion_is_bounded_when_a_descendant_holds_the_pipe() {
    // Astra 2026-09-30 #4: the shell exits but a background child keeps stdout open.
    let t0 = std::time::Instant::now();
    let out = pulse_cutover::ops::run_shell_timeout("sleep 30 & echo hi", std::time::Duration::from_secs(20)).unwrap();
    let took = t0.elapsed();
    assert!(took < pulse_cutover::ops::PIPE_GRACE + std::time::Duration::from_secs(4), "took {took:?}");
    assert!(out.starts_with("hi"), "{out}");
    assert!(out.contains("were killed"), "{out}");
    // Output is capped (head + tail kept).
    let big = pulse_cutover::ops::run_shell_timeout("head -c 500000 /dev/zero | tr '\\0' a", std::time::Duration::from_secs(20)).unwrap();
    assert!(big.len() < pulse_cutover::ops::OUTPUT_CAP + 200, "{}", big.len());
    assert!(big.contains("bytes of output dropped"));
}

#[test]
fn hook_commands_are_killed_at_the_timeout() {
    let t0 = std::time::Instant::now();
    let err = pulse_cutover::ops::run_shell_timeout("sleep 30", std::time::Duration::from_secs(1)).unwrap_err();
    assert!(err.contains("timed out"), "{err}");
    assert!(t0.elapsed() < std::time::Duration::from_secs(10));
    assert_eq!(pulse_cutover::ops::run_shell_timeout("echo hi", std::time::Duration::from_secs(5)).unwrap(), "hi");
}

fn roster_config(dir: &std::path::Path) -> Config {
    let base = std::fs::read_to_string({ coord_config(dir, 120, 0, 5); dir.join("ceremony-coord.toml") }).unwrap();
    let text = base.replace("fleet_quorum = 0", "fleet_quorum = 2\nreport_max_age_secs = 60")
        + "\n[[coordination.roster]]\nproducer = \"bp1\"\ninstance_id = \"aa\"\n\n[[coordination.roster]]\nproducer = \"bp2\"\n";
    load_toml(dir, "roster.toml", &text).unwrap()
}

#[test]
fn roster_fleet_gate_counts_only_fresh_roster_members() {
    // The mock's snapshot is deterministic, so a probe run yields exactly the evidence the gated
    // run will journal at VERIFIED (same sha256 + fingerprints digest).
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    assert!(!ours["snapshot_sha256"].is_null());
    let rep = |id: &str| serde_json::json!({"instance_id": id, "coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    let run = |good: bool| -> (State, String) {
        let d = tempfile::tempdir().unwrap();
        let cfg = roster_config(d.path());
        let ops = MockOps::new(d.path(), 110);
        // Plenty of agreeing NON-members; roster members are only good when `good`.
        *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
            {"name": "outsider1", "beacons": [{"age_ms": 1000, "report": rep("x1")}]},
            {"name": "outsider2", "beacons": [{"age_ms": 1000, "report": rep("x2")}]},
            {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep(if good { "aa" } else { "zz" })}]},
            {"name": "bp2", "beacons": [{"age_ms": if good { 1000 } else { 999_000 }, "report": rep("bb")}]},
        ]}]}));
        let st = run_machine(&cfg, &ops);
        (st, std::fs::read_to_string(d.path().join("journal.jsonl")).unwrap())
    };
    let (st, text) = run(false);
    assert_eq!(st, State::Aborted, "outsiders + a stale member + a wrong instance must not satisfy the roster");
    assert!(text.contains("fleet did not reach verified agreement"));
    let (st, text) = run(true);
    assert_eq!(st, State::Live, "{text}");
}


#[test]
fn roster_members_count_once_and_only_for_this_event() {
    // Duplicate roster entries are rejected at load (one report must not count twice).
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ roster_config(dir.path()); dir.path().join("roster.toml") }).unwrap();
    let dup = base.clone() + "\n[[coordination.roster]]\nproducer = \"bp2\"\n";
    let err = load_toml(dir.path(), "dup.toml", &dup).unwrap_err();
    assert!(err.contains("more than once"), "{err}");

    // Reports about a DIFFERENT event, or with no evidence at all, never agree.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": {"instance_id": "aa", "coord": {"event_id": "OTHER"},
            "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}}}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": {"instance_id": "bb", "coord": {"event_id": "e1"},
            "ceremony": {"state": "VERIFIED", "evidence": {}}}}]},
    ]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted, "wrong event + empty evidence must not satisfy the gate");
}

#[test]
fn event_quorum_is_never_inherited_and_release_pin_needs_a_plugin() {
    use pulse_cutover::coord::{derived_config, validate_event};
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("c.toml");
    std::fs::write(&src, "journal_path = \"/j\"\n[ceremony]\nfreeze_height = 0\n[snapshot]\nstaged_path = \"/s\"\n[coordination]\nurl = \"https://mc\"\nnetwork = \"testnet\"\nfleet_quorum = 1\n").unwrap();
    let ev = serde_json::json!({"type": "event", "event_id": "e9", "h": 900, "roster": [{"producer": "bp1"}, {"producer": "bp2"}, {"producer": "bp3"}]});
    let out = dir.path().join("d.toml");
    derived_config(&src, &ev, &out).unwrap();
    let d: toml::Value = toml::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(d["coordination"]["fleet_quorum"].as_integer(), Some(3), "no event quorum = all roster members, not the local 1");

    let cfg = test_config(dir.path(), 120);
    let dup = serde_json::json!({"type": "event", "network": "rehearsal", "h": 100_000, "roster": [{"producer": "bp1"}, {"producer": "bp1"}]});
    assert!(validate_event(&dup, &cfg, "rehearsal", Some(10), 10).unwrap_err().contains("more than once"));
    let pinned = serde_json::json!({"type": "event", "network": "rehearsal", "h": 100_000, "release_sha256": "ab".repeat(32)});
    assert!(validate_event(&pinned, &cfg, "rehearsal", Some(10), 10).unwrap_err().contains("plugin_path"));
}

#[test]
fn beacon_preview_writes_nothing() {
    // Astra 2026-09-30 #8: `beacon --once` with no url (installer dry-run) created the journal
    // dir and beacon.instance.
    let dir = tempfile::tempdir().unwrap();
    let jdir = dir.path().join("not-created");
    let text = format!(r#"
journal_path = "{j}/journal.jsonl"
[ceremony]
profile = "readiness"
[source]
rpc_url = "http://127.0.0.1:9"
producer_api_url = "http://127.0.0.1:9"
[snapshot]
staged_path = "{j}/staged.bin"
[target]
metalgo_unit = "metalgo-none"
rpc_url = "http://127.0.0.1:9/ext/bc/X/rpc"
[beacon]
url = ""
producer = "bp1"
network = "testnet"
"#, j = jdir.display());
    let cfgp = dir.path().join("b.toml");
    std::fs::write(&cfgp, text).unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
        .args(["beacon", "--config"]).arg(&cfgp).arg("--once").output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["instance_id"].as_str().map(str::len), Some(32));
    assert!(!jdir.exists(), "a preview must not create the journal dir or beacon.instance");
}

// ---- Astra second verification (2026-09-30): round 3 regressions ------------------------------------
// Each test below reproduces a failing case from ~/dev/pulse-migration/study/ASTRA-verify2-2026-09-30.md.

fn fail_live_config(dir: &std::path::Path) -> Config {
    let base = std::fs::read_to_string({ test_config(dir, 120); dir.join("ceremony.toml") }).unwrap();
    load_toml(dir, "fail-live.toml", &base.replace("on_live = \"flip-gateway\"", "on_live = \"fail-live\"")).unwrap()
}

#[test]
fn r3_rc6_halt_error_without_its_transition_is_still_halted() {
    // Astra #2: rc.6 wrote the HALTED error, then the HALTED transition. A crash between the two
    // left a journal whose replayed state was still IGNITED, and a restart ran on to LIVE.
    let dir = tempfile::tempdir().unwrap();
    let cfg = fail_live_config(dir.path());
    assert!(run_machine_result(&cfg, &MockOps::new(dir.path(), 110)).unwrap_err().starts_with("HALTED"));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let keep: Vec<&str> = text.lines().take_while(|l| !l.contains("HALTED")).collect();
    let seq = keep.len();
    let rc6_error = serde_json::json!({"seq": seq, "ts_ms": 1, "ts": "t", "kind": "error", "state": "IGNITED",
        "data": {"message": "HALTED: on_live hook failed", "detail": {"detail": {}, "sealed": true}}});
    std::fs::write(&cfg.journal_path, keep.join("\n") + "\n" + &rc6_error.to_string() + "\n").unwrap();
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.state, Some(State::Halted), "a halt-intent error record is HALTED even without its transition");
    assert_eq!(rec.halted_from, Some(State::Ignited));
    // Restart with a config whose on_live now succeeds: it must still refuse.
    let good = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 200);
    let err = run_machine_result(&good, &ops).unwrap_err();
    assert!(err.contains("HALTED (journaled)"), "{err}");
    assert!(!std::fs::read_to_string(&cfg.journal_path).unwrap().contains(r#""kind":"transition","state":"LIVE""#));
}

#[test]
fn r3_halt_is_one_durable_record_carrying_the_reason() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = fail_live_config(dir.path());
    assert!(run_machine_result(&cfg, &MockOps::new(dir.path(), 110)).unwrap_err().starts_with("HALTED"));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let halted: Vec<&str> = text.lines().filter(|l| l.contains("HALTED")).collect();
    let first = halted.first().expect("a HALTED record");
    assert!(first.contains(r#""kind":"transition","state":"HALTED""#), "the FIRST HALTED record is the durable transition: {first}");
    assert!(first.contains("on_live hook failed"), "the transition carries the reason: {first}");
    let s = pulse_cutover::beacon::journal_summary(&cfg.journal_path);
    assert_eq!(s["state"], "HALTED");
    assert!(s["last_error_class"].as_str().map(|x| x.contains("HALTED")).unwrap_or(false), "{s}");
}

#[test]
fn r3_unhalt_keeps_the_verified_snapshot_evidence() {
    // Astra #3: unhalt wrote a VERIFIED transition without `sha256`; the beacon summary then
    // reported snapshot_sha256 = null and any fleet-gated retry could never agree.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    let before = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    assert!(!before["snapshot_sha256"].is_null());
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
        .args(["unhalt", "--config"]).arg(dir.path().join("ceremony.toml")).arg("--i-understand").output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let after = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    assert_eq!(after["snapshot_sha256"], before["snapshot_sha256"], "unhalt must not erase the snapshot hash");
    assert_eq!(after["fingerprints_digest"], before["fingerprints_digest"]);
    assert!(Journal::replay(&cfg.journal_path).unwrap().sha256.is_some());
}

#[test]
fn r3_unhalted_fleet_gated_retry_can_still_agree_and_reach_live() {
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let rep = |id: &str| serde_json::json!({"instance_id": id, "coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    let doc = serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aa")}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": rep("bb")}]}]}]});
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    *ops.status_doc.borrow_mut() = Some(doc.clone());
    ops.target_fork.set(true);
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
        .args(["unhalt", "--config"]).arg(d.path().join("roster.toml")).arg("--i-understand").output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    let ops2 = MockOps::new(d.path(), rec.cut_height.unwrap() + 3);
    ops2.paused.set(true);
    ops2.target_head.set(rec.cut_height.unwrap());
    *ops2.status_doc.borrow_mut() = Some(doc);
    let st = run_machine_result(&cfg, &ops2);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert_eq!(st, Ok(State::Live), "{text}");
}

#[test]
fn r3_roster_with_zero_quorum_is_a_config_error() {
    // Astra (#9 table): roster configured, fleet_quorum left at 0 and no event id → the gate was
    // skipped entirely and the ceremony reached LIVE with no fleet check.
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ coord_config(dir.path(), 120, 0, 5); dir.path().join("ceremony-coord.toml") }).unwrap();
    let text = base.replace("event_id = \"e1\"\n", "") + "\n[[coordination.roster]]\nproducer = \"bp1\"\n";
    let err = load_toml(dir.path(), "zero.toml", &text).err().expect("roster + quorum 0 must not load");
    assert!(err.contains("fleet_quorum"), "{err}");
    let over = base.replace("fleet_quorum = 0", "fleet_quorum = 3") + "\n[[coordination.roster]]\nproducer = \"bp1\"\n";
    assert!(load_toml(dir.path(), "over.toml", &over).is_err(), "quorum larger than the roster can never be met");
}

#[test]
fn r3_conflicted_reports_never_count_toward_the_fleet_gate() {
    // Astra #5: every roster report carried conflict=true (one token on two machines) and the
    // machine still reached LIVE.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let rep = |id: &str| serde_json::json!({"instance_id": id, "ready": false, "coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "conflict": true, "report": rep("aa")}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "conflict": true, "report": rep("bb")}]}]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted, "conflicted evidence must not satisfy the roster");

    // Legacy (no roster) gate: the same exclusion, and reports must be FRESH too.
    let d2 = tempfile::tempdir().unwrap();
    let cfg2 = coord_config(d2.path(), 120, 2, 5);
    let ops2 = MockOps::new(d2.path(), 110);
    *ops2.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "age_ms": 999_000, "report": rep("aa")},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "conflict": true, "report": rep("bb")}]}]}]}));
    assert_eq!(run_machine(&cfg2, &ops2), State::Aborted, "a stale legacy report and a conflicted one are not a quorum of 2");
}

#[test]
fn r3_completed_corrupt_record_before_a_cr_only_tail_is_fatal() {
    // Astra #7: `valid\ncorrupt\n\r` — the tail after the last LF is whitespace, so the corrupt
    // record is COMPLETE; it used to be treated as torn and silently dropped.
    let dir = tempfile::tempdir().unwrap();
    let good = serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "ARMED",
        "data": {"chain_id": "ab", "resolved_h": 120}}).to_string();
    let path = dir.path().join("cr.jsonl");
    std::fs::write(&path, format!("{good}\n{{\"seq\":1,\"garbled\n\r")).unwrap();
    assert!(Journal::replay(&path).is_err(), "replay must not skip a completed corrupt record");
    let err = Journal::open(&path).err().expect("open must refuse");
    assert!(err.contains("corrupt"), "{err}");
    assert!(std::fs::read_to_string(&path).unwrap().contains("garbled"), "nothing was truncated");
    // A genuinely torn tail (no LF after it) is still set aside.
    let torn = dir.path().join("torn.jsonl");
    std::fs::write(&torn, format!("{good}\n\r\n{{\"seq\":1,\"ts_ms\":2,\"kin")).unwrap();
    let (_j, rec) = Journal::open(&torn).unwrap();
    assert!(rec.torn_tail);
    assert_eq!(rec.state, Some(State::Armed));
}

fn run_bin(args: &[&str], cfg: &std::path::Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover")).args(&args[..1]).arg("--config").arg(cfg).args(&args[1..]).output().unwrap()
}

#[test]
fn r3_rollback_refuses_without_affirmative_evidence() {
    // Astra #1: `cutover.sh abort` treated a MISSING journal as safe and resumed the source.
    let dir = tempfile::tempdir().unwrap();
    let _ = test_config(dir.path(), 120);
    let cfgp = dir.path().join("ceremony.toml");
    let out = run_bin(&["rollback", "--wait", "1"], &cfgp);
    assert_eq!(out.status.code(), Some(3), "missing journal is not proof that ignition never started: {}", String::from_utf8_lossy(&out.stderr));
    assert!(!dir.path().join("journal.jsonl").exists());
    let (url, _hits) = stub_http(|_m, _p| (200, "{}".into()));
    point_producer_at(dir.path(), &url);
    let out = run_bin(&["rollback", "--wait", "1", "--no-journal-i-know"], &cfgp);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn r3_rollback_refuses_after_ignition_started_unless_forced() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    let cfgp = dir.path().join("ceremony.toml");
    let before = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let out = run_bin(&["rollback", "--wait", "1"], &cfgp);
    assert_eq!(out.status.code(), Some(3), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(std::fs::read_to_string(&cfg.journal_path).unwrap(), before, "a refused rollback changes nothing");
    let (url, _hits) = stub_http(|_m, _p| (200, "{}".into()));
    point_producer_at(dir.path(), &url);
    set_target_line(dir.path(), "stop_cmd = \"true\"");
    let out = run_bin(&["rollback", "--wait", "1", "--force-after-ignite"], &cfgp);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""kind":"transition","state":"ABORTED""#) && text.contains("force_after_ignite"), "{text}");
}

#[test]
fn r3_rollback_before_ignition_is_journaled_and_waits_for_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let keep: Vec<&str> = text.lines().take_while(|l| !l.contains(r#""kind":"transition","state":"SNAPSHOTTED""#)).collect();
    std::fs::write(&cfg.journal_path, keep.join("\n") + "\n").unwrap();
    let cfgp = dir.path().join("ceremony.toml");
    // Another process (a still-running agent) holds the journal: rollback must not decide.
    let held = Journal::open(&cfg.journal_path).unwrap();
    let out = run_bin(&["rollback", "--wait", "1"], &cfgp);
    assert_eq!(out.status.code(), Some(3), "a lock still held after --wait is a refusal (exit 3)");
    assert!(String::from_utf8_lossy(&out.stderr).contains("holds"), "{}", String::from_utf8_lossy(&out.stderr));
    drop(held);
    // The binary talks to a real producer API: serve one that accepts the resume.
    let (url, _hits) = stub_http(|_m, _p| (200, "{}".into()));
    point_producer_at(dir.path(), &url);
    let out = run_bin(&["rollback", "--wait", "1"], &cfgp);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""kind":"transition","state":"ABORTED""#) && text.contains("operator_rollback"), "{text}");
}

#[test]
fn r3_cutover_sh_abort_with_missing_journal_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let _ = test_config(dir.path(), 120);
    let bindir = dir.path().join("bin");
    std::fs::create_dir_all(&bindir).unwrap();
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_pulse-cutover"), bindir.join("pulse-cutover")).unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("cutover.sh");
    let out = std::process::Command::new("bash").arg(&script).arg("abort")
        .env("PULSE_CUTOVER_CONFIG", dir.path().join("ceremony.toml"))
        .env("PULSE_CUTOVER_ABORT_WAIT", "1")
        .env("PATH", format!("{}:{}", bindir.display(), std::env::var("PATH").unwrap()))
        .output().unwrap();
    assert_eq!(out.status.code(), Some(3), "stdout: {}\nstderr: {}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
}

#[test]
fn r3_recovered_flip_side_effect_is_reverted_by_a_forced_rollback() {
    // Test-assurance: drive the machine from a crash right after the flip side effect started.
    let dir = tempfile::tempdir().unwrap();
    let cfg = api_test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    let at = lines.iter().position(|l| l.contains(r#""side_effect":"flip_cmd""#)).expect("flip side effect journaled");
    std::fs::write(&cfg.journal_path, lines[..=at].join("\n") + "\n").unwrap();
    let ops = MockOps::new(dir.path(), 200);
    let (journal, rec) = Journal::open(&cfg.journal_path).unwrap();
    let mut m = Machine::new(&cfg, &ops, journal, rec);
    assert!(m.operator_rollback(false).is_err(), "ignition started: refused without force");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "revert-nginx"));
    assert_eq!(m.operator_rollback(true).unwrap().state, State::Aborted);
    assert!(ops.hooks.borrow().iter().any(|h| h == "revert-nginx"), "the recovered flip was reverted: {:?}", ops.hooks.borrow());
}

#[test]
fn r3_crash_mid_copy_restages_a_truncated_staged_snapshot() {
    // Test-assurance: interrupt the staging COPY itself (half a file on disk), then resume.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Live);
    let rec_full = Journal::replay(&cfg.journal_path).unwrap();
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    let staged = lines.iter().position(|l| l.contains("staged_artifact")).unwrap();
    std::fs::write(&cfg.journal_path, lines[..=staged].join("\n") + "\n").unwrap();
    let full = std::fs::read(&cfg.snapshot.staged_path).unwrap();
    std::fs::write(&cfg.snapshot.staged_path, &full[..full.len() / 2]).unwrap();
    let ops = MockOps::new(dir.path(), rec_full.cut_height.unwrap() + 3);
    ops.paused.set(true);
    ops.target_head.set(rec_full.cut_height.unwrap());
    let st = run_machine_result(&cfg, &ops);
    assert_eq!(st, Ok(State::Live), "{}", std::fs::read_to_string(&cfg.journal_path).unwrap());
    assert_eq!(std::fs::read(&cfg.snapshot.staged_path).unwrap(), full, "the partial copy was replaced by a full restage");
}


// ---------------------------------------------------------------------------------------------
// Round 4 (Fable review of rc.7): rollback must never claim success it did not achieve.
// ---------------------------------------------------------------------------------------------

/// Minimal HTTP stub on 127.0.0.1: `handler(method, path) -> (status, body)`. Returns the base
/// URL and a hit counter per request.
fn stub_http(handler: impl Fn(&str, &str) -> (u16, String) + Send + Sync + 'static)
    -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let h2 = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = vec![0u8; 65536];
            let mut got = 0;
            // Read headers (+ a small body): enough for these tests.
            loop {
                let n = match stream.read(&mut buf[got..]) { Ok(0) | Err(_) => break, Ok(n) => n };
                got += n;
                let text = String::from_utf8_lossy(&buf[..got]);
                if let Some(end) = text.find("\r\n\r\n") {
                    let len = text[..end].lines().find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0))).unwrap_or(0);
                    if got >= end + 4 + len { break; }
                }
            }
            let text = String::from_utf8_lossy(&buf[..got]).to_string();
            let mut first = text.lines().next().unwrap_or("").split_whitespace();
            let (m, p) = (first.next().unwrap_or("").to_string(), first.next().unwrap_or("").to_string());
            h2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (code, body) = handler(&m, &p);
            let _ = write!(stream, "HTTP/1.1 {code} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
        }
    });
    (format!("http://{addr}"), hits)
}

/// Point ceremony.toml's source APIs at `url` (the binary tests use real HTTP).
fn point_producer_at(dir: &std::path::Path, url: &str) {
    let p = dir.join("ceremony.toml");
    let t = std::fs::read_to_string(&p).unwrap()
        .replace("producer_api_url = \"http://mock\"", &format!("producer_api_url = \"{url}\""));
    std::fs::write(&p, t).unwrap();
}

/// Add a line to ceremony.toml's [target] section.
fn set_target_line(dir: &std::path::Path, line: &str) {
    let p = dir.join("ceremony.toml");
    let t = std::fs::read_to_string(&p).unwrap().replacen("[target]\n", &format!("[target]\n{line}\n"), 1);
    std::fs::write(&p, t).unwrap();
}

/// A producer journal cut right before SNAPSHOTTED (ignition provably not started) with the
/// staged snapshot still on disk from the full run.
fn pre_ignite_journal(dir: &std::path::Path) -> Config {
    let cfg = test_config(dir, 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir, 110)), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let keep: Vec<&str> = text.lines().take_while(|l| !l.contains(r#""kind":"transition","state":"SNAPSHOTTED""#)).collect();
    std::fs::write(&cfg.journal_path, keep.join("\n") + "\n").unwrap();
    cfg
}

#[test]
fn r4_rollback_with_a_failed_resume_exits_4_and_names_the_step() {
    // Fable §2.1: `rollback` printed "rolled back" and exited 0 when the producer resume FAILED.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    point_producer_at(dir.path(), "http://127.0.0.1:1"); // closed port
    let out = run_bin(&["rollback", "--wait", "1"], &dir.path().join("ceremony.toml"));
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(4), "a failed resume is an INCOMPLETE rollback: {err}");
    assert!(err.contains("resume the source producer"), "names the failed step: {err}");
    assert!(!String::from_utf8_lossy(&out.stdout).contains("rolled back:"), "never claims success");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("rollback_incomplete") && text.contains(r#""rollback_complete":false"#), "{text}");
}

#[test]
fn r4_a_second_rollback_after_a_complete_one_does_nothing() {
    // Fable §2.7: repeat rollbacks re-resumed and re-ran on_abort.
    let dir = tempfile::tempdir().unwrap();
    let _cfg = pre_ignite_journal(dir.path());
    let (url, hits) = stub_http(|_m, _p| (200, "{}".into()));
    point_producer_at(dir.path(), &url);
    let cfgp = dir.path().join("ceremony.toml");
    let out = run_bin(&["rollback", "--wait", "1"], &cfgp);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let after_first = hits.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(after_first, 1, "one resume call");
    let out = run_bin(&["rollback", "--wait", "1"], &cfgp);
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("already rolled back"), "{}", String::from_utf8_lossy(&out.stdout));
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1, "no second resume");
}

#[test]
fn r4_forced_rollback_fences_the_target_before_resuming_the_source() {
    // Fable §2.2: --force-after-ignite resumed the source while this box's target kept running.
    let dir = tempfile::tempdir().unwrap();
    let base = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&base, &ops).unwrap_err().starts_with("HALTED"));
    // Fence that fails: the source must NOT be resumed and the journal stays HALTED.
    set_target_line(dir.path(), "stop_cmd = \"fail-stop-target\"");
    let cfg = Config::load(&dir.path().join("ceremony.toml")).unwrap();
    let ops = MockOps::new(dir.path(), 200);
    let (j, rec) = Journal::open(&cfg.journal_path).unwrap();
    let mut m = Machine::new(&cfg, &ops, j, rec);
    let out = m.operator_rollback(true).unwrap();
    assert!(!out.failed.is_empty() && out.failed[0].contains("target fence failed"), "{out:?}");
    assert_eq!(ops.resumes.get(), 0, "never resume the source while the target may run");
    assert_eq!(out.state, State::Halted);
    drop(m);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("forced_rollback_blocked"), "{text}");
    // Fence that succeeds: it runs FIRST, then the source is resumed.
    let t = std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap().replace("fail-stop-target", "stop-target");
    std::fs::write(dir.path().join("ceremony.toml"), t).unwrap();
    let cfg = Config::load(&dir.path().join("ceremony.toml")).unwrap();
    let ops = MockOps::new(dir.path(), 200);
    let (j, rec) = Journal::open(&cfg.journal_path).unwrap();
    let mut m = Machine::new(&cfg, &ops, j, rec);
    let out = m.operator_rollback(true).unwrap();
    assert!(out.failed.is_empty(), "{out:?}");
    assert_eq!(out.state, State::Aborted);
    let ev = ops.events.borrow().clone();
    let fence = ev.iter().position(|e| e == "hook:stop-target").expect("fence ran");
    let resume = ev.iter().position(|e| e == "resume").expect("source resumed");
    assert!(fence < resume, "fence before resume: {ev:?}");
}

#[test]
fn r4_every_rollback_refusal_exits_3() {
    // Fable §2.3: lock timeout and corrupt journal exited 1, so cutover.sh skipped its guidance.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    let cfgp = dir.path().join("ceremony.toml");
    let held = Journal::open(&cfg.journal_path).unwrap();
    assert_eq!(run_bin(&["rollback", "--wait", "1"], &cfgp).status.code(), Some(3), "lock held past --wait");
    drop(held);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let mut lines: Vec<&str> = text.lines().collect();
    lines.insert(1, "{not json");
    std::fs::write(&cfg.journal_path, lines.join("\n") + "\n").unwrap();
    let out = run_bin(&["rollback", "--wait", "1"], &cfgp);
    assert_eq!(out.status.code(), Some(3), "corrupt journal: {}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn r4_rollback_moves_the_staged_snapshot_aside() {
    // Fable §2.4: the staged snapshot stayed; the next preflight refused and a metalgo restart
    // would have imported the abandoned cut.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    assert!(cfg.snapshot.staged_path.exists(), "fixture: staged file present");
    let ops = MockOps::new(dir.path(), 200);
    let (j, rec) = Journal::open(&cfg.journal_path).unwrap();
    let mut m = Machine::new(&cfg, &ops, j, rec);
    let out = m.operator_rollback(false).unwrap();
    assert!(out.failed.is_empty(), "{out:?}");
    assert!(!cfg.snapshot.staged_path.exists(), "unstaged");
    let aside = std::fs::read_dir(dir.path()).unwrap().filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("staged.bin.rolled-back-"));
    assert!(aside, "moved aside, not deleted");
    drop(m);
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("\"unstaged\""));
}

#[test]
fn r4_no_journal_rollback_leaves_no_ceremony_journal_behind() {
    // Fable §2.5: --no-journal-i-know created a terminal ABORTED journal, so the next `run` on
    // that box did nothing.
    let dir = tempfile::tempdir().unwrap();
    let _ = test_config(dir.path(), 120);
    let (url, _hits) = stub_http(|_m, _p| (200, "{}".into()));
    point_producer_at(dir.path(), &url);
    let cfgp = dir.path().join("ceremony.toml");
    let out = run_bin(&["rollback", "--wait", "1", "--no-journal-i-know"], &cfgp);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(!dir.path().join("journal.jsonl").exists(), "the ceremony journal is not created");
    let audit = std::fs::read_dir(dir.path()).unwrap().filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("journal.jsonl.rollback-"));
    assert!(audit, "a separate audit record is written");
    let st = run_bin(&["status"], &cfgp);
    assert!(String::from_utf8_lossy(&st.stdout).contains("no journal"), "next run starts fresh");
}

#[test]
fn r4_rollback_stops_an_orphaned_hook_before_anything_else() {
    // Fable §2.6: an on_freeze left running by a killed agent could close writes again after
    // on_abort reopened them.
    let dir = tempfile::tempdir().unwrap();
    let _ = pre_ignite_journal(dir.path());
    let t = std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap()
        .replace("on_live = \"flip-gateway\"", "on_live = \"flip-gateway\"\non_abort = \"reopen-writes\"");
    std::fs::write(dir.path().join("ceremony.toml"), t).unwrap();
    let cfg = Config::load(&dir.path().join("ceremony.toml")).unwrap();
    let ops = MockOps::new(dir.path(), 200);
    *ops.orphan.borrow_mut() = Some(Ok(Some("killed orphaned hook process group 4242 (SIGTERM): `freeze-writes`".into())));
    let (j, rec) = Journal::open(&cfg.journal_path).unwrap();
    let mut m = Machine::new(&cfg, &ops, j, rec);
    let out = m.operator_rollback(false).unwrap();
    assert!(out.failed.is_empty(), "{out:?}");
    let ev = ops.events.borrow().clone();
    assert_eq!(ev.first().map(String::as_str), Some("kill-orphans"), "{ev:?}");
    let resume = ev.iter().position(|e| e == "resume").unwrap();
    let on_abort = ev.iter().position(|e| e == "hook:reopen-writes").unwrap();
    assert!(resume < on_abort, "{ev:?}");
    drop(m);
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("orphan_hook_killed"));
    // An orphan that cannot be stopped blocks the whole rollback.
    let dir2 = tempfile::tempdir().unwrap();
    let cfg2 = pre_ignite_journal(dir2.path());
    let ops2 = MockOps::new(dir2.path(), 200);
    *ops2.orphan.borrow_mut() = Some(Err("survived SIGKILL".into()));
    let (j, rec) = Journal::open(&cfg2.journal_path).unwrap();
    // Round 5 (Fable N5): nothing changed, so this is a refusal (exit 3), not an incomplete rollback.
    let err = Machine::new(&cfg2, &ops2, j, rec).operator_rollback(false).expect_err("refused");
    assert!(err.starts_with("refusing"), "{err}");
    assert_eq!(ops2.resumes.get(), 0, "nothing rolled back while an orphaned hook still runs");
}

#[test]
fn r4_recorded_hook_group_is_killed_for_real() {
    // The real mechanism behind kill_orphan_hooks: a tracked hook records its process group and a
    // separate caller can kill it (as `rollback` does after `pkill` stopped the agent).
    let dir = tempfile::tempdir().unwrap();
    let pg = dir.path().join("journal.jsonl.hook.pgid");
    let pg2 = pg.clone();
    let started = std::time::Instant::now();
    let t = std::thread::spawn(move || pulse_cutover::ops::run_shell_timeout_tracked("sleep 30", std::time::Duration::from_secs(60), Some(&pg2)));
    while !pg.exists() && started.elapsed().as_secs() < 5 {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(pg.exists(), "the running hook's group is recorded");
    let killed = pulse_cutover::ops::kill_recorded_hook_group(&pg, std::time::Duration::from_secs(2)).unwrap();
    assert!(killed.unwrap_or_default().contains("killed orphaned hook process group"));
    let res = t.join().unwrap();
    assert!(res.is_err(), "the hook died: {res:?}");
    assert!(started.elapsed().as_secs() < 15, "killed, not waited out");
    assert!(!pg.exists(), "record removed");
    assert_eq!(pulse_cutover::ops::kill_recorded_hook_group(&pg, std::time::Duration::from_secs(1)).unwrap(), None);
}

#[test]
fn r4_fleet_gate_ignores_reports_with_failing_health_but_not_setup_checks() {
    // Fable #5 residual: the gate counted reports whose health checks were failing.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let rep = |id: &str, checks: serde_json::Value| serde_json::json!({"instance_id": id, "checks": checks,
        "coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    let sick = serde_json::json!([{"name": "source_api", "ok": false, "detail": "unreachable"}]);
    let setup_only = serde_json::json!([{"name": "hook_on_live", "ok": false, "detail": "not configured"},
                                        {"name": "source_api", "ok": true, "detail": "ok"}]);
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aa", sick.clone())}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": rep("bb", setup_only.clone())}]}]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted, "bp1's failing health check excludes it: 1 of 2");
    let d2 = tempfile::tempdir().unwrap();
    let cfg2 = roster_config(d2.path());
    let ops2 = MockOps::new(d2.path(), 110);
    *ops2.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aa", setup_only.clone())}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": rep("bb", setup_only)}]}]}]}));
    assert_eq!(run_machine(&cfg2, &ops2), State::Live, "failing SETUP checks (hooks) do not exclude a report");
}

#[test]
fn r4_await_never_launches_the_ceremony_for_an_arm_with_the_wrong_event_hash() {
    // Fable #4 residual: the arm-hash check was only unit-tested through check_arm.
    fn run_await_against(dir: &std::path::Path, wrong_hash: bool) -> String {
        let _ = coord_config(dir, 120, 0, 60);
        let ev = signed(serde_json::json!({"type": "event", "event_id": "e1", "network": "rehearsal", "h": 5000u64}));
        let good = pulse_cutover::coord::payload_hash(&ev).unwrap();
        let hash = if wrong_hash { "00".repeat(32) } else { good };
        let arm = signed(serde_json::json!({"type": "arm", "event_id": "e1", "network": "rehearsal", "event_hash": hash,
            "issued_at_ms": chrono::Utc::now().timestamp_millis()}));
        let doc = serde_json::json!({"event": ev, "arm": arm}).to_string();
        let (url, _hits) = stub_http(move |_m, p| {
            if p.starts_with("/api/coord/") { (200, doc.clone()) }
            else { (200, r#"{"head_block_num": 100}"#.into()) }
        });
        let t = std::fs::read_to_string(dir.join("ceremony-coord.toml")).unwrap()
            .replace("url = \"http://mc\"", &format!("url = \"{url}\"\nauto_arm = true"))
            .replace("rpc_url = \"http://mock\"\nproducer_api_url", &format!("rpc_url = \"{url}\"\nproducer_api_url"));
        std::fs::write(dir.join("ceremony-coord.toml"), t).unwrap();
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
            .args(["await", "--config"]).arg(dir.join("ceremony-coord.toml"))
            .stderr(std::process::Stdio::piped()).stdout(std::process::Stdio::null()).spawn().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(5));
        let _ = child.kill();
        let out = child.wait_with_output().unwrap();
        String::from_utf8_lossy(&out.stderr).to_string()
    }
    let good = tempfile::tempdir().unwrap();
    let err = run_await_against(good.path(), false);
    assert!(err.contains("ARMED by signed coordinator message"), "control: the right hash arms: {err}");
    let bad = tempfile::tempdir().unwrap();
    let err = run_await_against(bad.path(), true);
    assert!(!err.contains("ARMED by signed"), "a wrong event_hash must never launch the ceremony: {err}");
    assert!(err.contains("event_hash does not match"), "{err}");
    assert!(!bad.path().join("ceremony-e1.toml").exists(), "no derived ceremony config was written");
}

#[test]
fn r4_beacon_summary_marks_a_forced_rollback_after_ignition() {
    // Fable §2.9: after a forced rollback the beacon judged the box as pre-ceremony.
    let dir = tempfile::tempdir().unwrap();
    let base = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&base, &ops).unwrap_err().starts_with("HALTED"));
    let ops = MockOps::new(dir.path(), 200);
    let (j, rec) = Journal::open(&base.journal_path).unwrap();
    Machine::new(&base, &ops, j, rec).operator_rollback(true).unwrap();
    let s = pulse_cutover::beacon::journal_summary(&base.journal_path);
    assert_eq!(s["state"], "ABORTED");
    assert_eq!(s["ignition_started"], true);
    assert_eq!(s["forced_rollback"], true);
}


// ---------------------------------------------------------------------------------------------
// Round 5 (Fable re-check of rc.7, N1–N10): rollback completion, orphans on every path.
// ---------------------------------------------------------------------------------------------

/// pre_ignite_journal + an on_abort hook.
fn pre_ignite_with_on_abort(dir: &std::path::Path) -> Config {
    let _ = pre_ignite_journal(dir);
    let t = std::fs::read_to_string(dir.join("ceremony.toml")).unwrap()
        .replace("on_live = \"flip-gateway\"", "on_live = \"flip-gateway\"\non_abort = \"reopen-writes\"");
    std::fs::write(dir.join("ceremony.toml"), t).unwrap();
    Config::load(&dir.join("ceremony.toml")).unwrap()
}

fn rollback_with(cfg: &Config, ops: &MockOps, force: bool) -> Result<pulse_cutover::machine::RollbackOutcome, String> {
    let (j, rec) = Journal::open(&cfg.journal_path).unwrap();
    Machine::new(cfg, ops, j, rec).operator_rollback(force)
}

#[test]
fn r5_rollback_killed_inside_on_abort_reruns_only_the_missing_steps() {
    // Fable N1 (P9): rollback_complete was journaled BEFORE on_abort; a rollback killed inside
    // on_abort replayed as complete and every later rollback was a no-op with writes still closed.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_with_on_abort(dir.path());
    let ops = MockOps::new(dir.path(), 200);
    *ops.panic_on_hook.borrow_mut() = Some("reopen-writes".into());
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rollback_with(&cfg, &ops, false)));
    assert!(crashed.is_err(), "fixture: the rollback died inside on_abort");
    assert_eq!(ops.resumes.get(), 1, "the resume step completed before the crash");
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["rollback_complete"], serde_json::json!(false),
        "mission control is told the rollback is unfinished");
    // Second rollback: not "already rolled back"; it re-runs on_abort and does NOT resume again.
    let ops2 = MockOps::new(dir.path(), 200);
    let out = rollback_with(&cfg, &ops2, false).unwrap();
    assert!(!out.already, "a rollback that died inside on_abort is not complete: {out:?}");
    assert!(out.failed.is_empty(), "{out:?}");
    assert!(ops2.events.borrow().iter().any(|e| e == "hook:reopen-writes"), "on_abort re-run: {:?}", ops2.events.borrow());
    assert_eq!(ops2.resumes.get(), 0, "the resume step already succeeded: not repeated");
    // Now the journal proves every step: a third rollback is a no-op.
    let ops3 = MockOps::new(dir.path(), 200);
    let out = rollback_with(&cfg, &ops3, false).unwrap();
    assert!(out.already, "{out:?}");
    assert!(!ops3.events.borrow().iter().any(|e| e.starts_with("hook:") || e == "resume"), "{:?}", ops3.events.borrow());
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["rollback_complete"], serde_json::json!(true));
}

#[test]
fn r5_already_rolled_back_still_unstages_and_kills_orphans() {
    // Fable N2 (P4, P8d): the "already rolled back" no-op returned before the orphan kill and the
    // unstage, so a late staged file and an orphaned hook both survived.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    assert!(rollback_with(&cfg, &MockOps::new(dir.path(), 200), false).unwrap().failed.is_empty());
    std::fs::write(&cfg.snapshot.staged_path, b"late artifact").unwrap(); // e.g. a late import write
    let ops = MockOps::new(dir.path(), 200);
    *ops.orphan.borrow_mut() = Some(Ok(Some("killed orphaned hook process group 4242 (SIGTERM): `freeze-writes`".into())));
    let out = rollback_with(&cfg, &ops, false).unwrap();
    assert!(out.already, "{out:?}");
    assert_eq!(ops.events.borrow().first().map(String::as_str), Some("kill-orphans"), "{:?}", ops.events.borrow());
    assert!(!cfg.snapshot.staged_path.exists(), "the late staged file is moved aside even on the no-op path");
    assert_eq!(ops.resumes.get(), 0, "no second resume");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("orphan_hook_killed"), "{text}");
}

#[test]
fn r5_automatic_abort_after_staging_moves_its_own_snapshot_aside() {
    // Fable N2 (P4): the agent's own abort (here: fleet gate short of quorum, after staging)
    // left staged.bin; the next preflight refused and a metalgo restart would import it.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    let rep = serde_json::json!({"instance_id": "aa", "checks": [], "coord": {"event_id": "e1"},
        "ceremony": {"state": "VERIFIED", "evidence": ours}});
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep}]}]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert!(!cfg.snapshot.staged_path.exists(), "the agent's own abort unstages the snapshot it staged");
    let aside = std::fs::read_dir(d.path()).unwrap().filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("staged.bin.rolled-back-"));
    assert!(aside, "moved aside, not deleted");
}

#[test]
fn r5_orphan_that_cannot_be_stopped_is_a_refusal() {
    // Fable N5: an orphan that could not be killed exited 4 ("incomplete") although nothing changed.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    let ops = MockOps::new(dir.path(), 200);
    *ops.orphan.borrow_mut() = Some(Err("survived SIGKILL".into()));
    let err = rollback_with(&cfg, &ops, false).expect_err("a refusal, not an incomplete rollback");
    assert!(err.starts_with("refusing"), "{err}");
    assert_eq!(ops.resumes.get(), 0);
}

/// Spawn a real long-running command in its own process group (like a hook a dead agent left).
/// A background thread reaps it, as init reaps a dead agent's orphans (an unreaped zombie would
/// still show in its process group). Returns (pid, exited flag).
#[cfg(unix)]
fn spawn_group(cmd: &str) -> (u32, std::sync::Arc<std::sync::atomic::AtomicBool>) {
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new("sh").arg("-c").arg(cmd).process_group(0)
        .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
    let pid = child.id();
    let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let e2 = exited.clone();
    std::thread::spawn(move || { let _ = child.wait(); e2.store(true, std::sync::atomic::Ordering::SeqCst); });
    (pid, exited)
}

#[cfg(unix)]
fn wait_exited(exited: &std::sync::atomic::AtomicBool, pid: u32) -> bool {
    let dead = (0..60).any(|_| { std::thread::sleep(std::time::Duration::from_millis(100)); exited.load(std::sync::atomic::Ordering::SeqCst) });
    if !dead {
        let _ = std::process::Command::new("kill").arg("-KILL").arg(format!("-{pid}")).status();
    }
    dead
}

#[cfg(unix)]
fn alive(pid: u32) -> bool {
    std::process::Command::new("kill").arg("-0").arg(pid.to_string()).status().map(|s| s.success()).unwrap_or(false)
}

#[test]
#[cfg(unix)]
fn r5_run_resume_kills_a_recorded_orphan_before_anything_else() {
    // Fable N3 (P10): a resumed `run` neither killed the orphan left by a crashed agent nor kept
    // its record (the next hook overwrote it and deleted it on completion).
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    let (pid, exited) = spawn_group("sleep 300");
    let pg = pulse_cutover::ops::hook_pgid_path(&cfg.journal_path);
    std::fs::write(&pg, format!("{pid}\nsleep 300\n")).unwrap();
    let out = run_bin(&["run"], &dir.path().join("ceremony.toml")); // source is unreachable: it aborts
    let dead = wait_exited(&exited, pid);
    assert!(dead, "the resumed run killed the orphaned hook group {pid}: {}", String::from_utf8_lossy(&out.stderr));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("orphan_hook_killed"), "journaled: {text}");
}

#[test]
#[cfg(unix)]
fn r5_run_long_pipeline_steps_are_tracked_and_killable() {
    // Fable N4: run_long (upstream export/import) ran untracked, outside its own process group:
    // it survived `pkill` and could re-create the staged artifact after "rolled back".
    use pulse_cutover::ops::ChainOps;
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("journal.jsonl");
    let pg = pulse_cutover::ops::hook_pgid_path(&journal);
    let j2 = journal.clone();
    let started = std::time::Instant::now();
    let t = std::thread::spawn(move || {
        let ops = pulse_cutover::ops::HttpOps::new("http://127.0.0.1:1", "http://127.0.0.1:1", "http://127.0.0.1:1", "true", 5)
            .with_pgid_file_for(&j2);
        ops.run_long("sleep 30")
    });
    while !pg.exists() && started.elapsed().as_secs() < 5 {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(pg.exists(), "a running pipeline step records its process group");
    let killed = pulse_cutover::ops::kill_recorded_hook_group(&pg, std::time::Duration::from_secs(2)).unwrap();
    assert!(killed.unwrap_or_default().contains("killed"), "killable by rollback/resume");
    assert!(t.join().unwrap().is_err());
    assert!(started.elapsed().as_secs() < 15);
}

#[test]
#[cfg(unix)]
fn r5_recycled_pid_guard_compares_the_process_start_time() {
    // Fable N10: the guard accepted any `sh -c …` leader, so a compound hook's record could kill an
    // unrelated `sh -c` group that inherited the pid. The record now carries the start time.
    let dir = tempfile::tempdir().unwrap();
    let pg = dir.path().join("journal.jsonl.hook.pgid");
    let (pid, exited) = spawn_group("sleep 30; true"); // compound: sh stays the group leader
    std::thread::sleep(std::time::Duration::from_millis(200));
    std::fs::write(&pg, format!("{pid}\nstart=Mon Jan  1 00:00:00 2001\nsleep 30; true\n")).unwrap();
    let res = pulse_cutover::ops::kill_recorded_hook_group(&pg, std::time::Duration::from_secs(1));
    assert!(res.is_err(), "a start-time mismatch means a different process: {res:?}");
    assert!(alive(pid), "the unrelated group was not touched");
    let start = pulse_cutover::ops::process_start(pid).expect("start time of a live pid");
    std::fs::write(&pg, format!("{pid}\nstart={start}\nsleep 30; true\n")).unwrap();
    let res = pulse_cutover::ops::kill_recorded_hook_group(&pg, std::time::Duration::from_secs(2)).unwrap();
    assert!(res.unwrap_or_default().contains("killed"));
    assert!(wait_exited(&exited, pid));
}

#[test]
fn r5_no_journal_rollback_leaves_no_lock_file_litter() {
    // Fable N7: --no-journal-i-know left <journal>.rollback-<ms>.jsonl.lock behind.
    let dir = tempfile::tempdir().unwrap();
    let _ = test_config(dir.path(), 120);
    let (url, _hits) = stub_http(|_m, _p| (200, "{}".into()));
    point_producer_at(dir.path(), &url);
    let out = run_bin(&["rollback", "--wait", "1", "--no-journal-i-know"], &dir.path().join("ceremony.toml"));
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let litter: Vec<String> = std::fs::read_dir(dir.path()).unwrap().filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string()).filter(|n| n.ends_with(".lock")).collect();
    assert!(litter.is_empty(), "no lock files left: {litter:?}");
}

#[test]
fn r5_fleet_gate_journals_why_each_report_was_excluded() {
    // Fable N9: excluded reports left no per-producer reason in the journal.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    let rep = |id: &str, checks: serde_json::Value| serde_json::json!({"instance_id": id, "checks": checks,
        "coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aa", serde_json::json!([{"name": "disk_free", "ok": false, "detail": "2 GB free"}]))}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": rep("bb", serde_json::json!([]))}]}]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("fleet_gate_excluded") && text.contains("disk_free"), "per-report reason journaled: {text}");
}

#[test]
fn r5_setup_check_names_are_shared_with_mission_control() {
    // Fable N9: the agent's health/setup split must be the dashboard's, from one shared list.
    let v: serde_json::Value = serde_json::from_str(include_str!("../control/check-kinds.json")).unwrap();
    let mut shared: Vec<String> = v["setup"].as_array().unwrap().iter().map(|x| x.as_str().unwrap().to_string()).collect();
    let mut ours: Vec<String> = pulse_cutover::beacon::SETUP_CHECKS.iter().map(|s| s.to_string()).collect();
    shared.sort();
    ours.sort();
    assert_eq!(ours, shared);
}
