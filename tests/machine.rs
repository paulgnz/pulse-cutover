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
        self.resumes.set(self.resumes.get() + 1);
        self.paused.set(false);
        Ok(())
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
            let producers: Vec<_> = (0..n).map(|i| serde_json::json!({"name": format!("bp{i}"),
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
