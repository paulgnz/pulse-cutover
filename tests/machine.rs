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
        Ok(0) // writes are frozen: burn-off blocks are empty
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
        if !self.ignited.get() {
            return Ok(None);
        }
        let polls = self.target_polls.get() + 1;
        self.target_polls.set(polls);
        let traffic_resumed = self
            .hooks
            .borrow()
            .iter()
            .any(|h| h.contains("resume-traffic"));
        // The imported chain presents the cut height; it only mints past it
        // once transactions flow again (post_ignite traffic hook).
        let head = if traffic_resumed {
            self.target_head.get() + polls.min(5)
        } else {
            self.target_head.get()
        };
        Ok(Some(self.info(head)))
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
        Ok("mock metalgo restarted".into())
    }

    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        self.hooks.borrow_mut().push(cmd.to_string());
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
    let (journal, recovered) = Journal::open(&cfg.journal_path).unwrap();
    let mut machine = Machine::new(cfg, ops, journal, recovered);
    machine.run().unwrap()
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
fn api_mode_flip_health_failure_aborts_without_stopping_source() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = api_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.drift.set(5);
    ops.flip_breaks_public.set(true); // nginx swaps to a dead upstream

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Aborted);

    // THE invariant: the source nodeos was never stopped — reads survive the
    // failed flip; and the flip was reverted so the public URL points back
    // at nodeos.
    assert!(!ops.stopped.get(), "source must NOT be stopped on a failed flip");
    let hooks = ops.hooks.borrow();
    assert!(hooks.iter().any(|h| h == "revert-nginx"), "flip must be reverted");
    assert!(!hooks.iter().any(|h| h == "stop-nodeos"));
    assert!(!hooks.iter().any(|h| h == "announce-live"));
    // Producer-mode rollback (resume) must not fire in api mode.
    assert_eq!(ops.resumes.get(), 0);

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("public URL did not serve the target"));
    assert!(text.contains("flip_reverted"));
    // Reads still served by nodeos after the abort.
    assert!(ops.public_info("http://mock-public").unwrap().is_some());
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

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Aborted);

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
