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
    /// If true, produce one extra block AFTER pause was requested (a "late
    /// block" — the quiescence window must absorb it).
    late_block: Cell<bool>,
    hooks: RefCell<Vec<String>>,
    now: Cell<u64>,
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
            late_block: Cell::new(false),
            hooks: RefCell::new(Vec::new()),
            now: Cell::new(1_000_000),
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
        if !self.paused.get() {
            self.head.set(self.head.get() + 1);
        } else if self.late_block.replace(false) {
            self.head.set(self.head.get() + 1);
        }
        Ok(self.info(self.head.get()))
    }

    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        let m = mini(block_num as u32);
        Ok((hex::encode(m.head_id()), "2024-01-01T00:00:00.000".into()))
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
        let head = self.head.get();
        let m = mini(head as u32);
        std::fs::write(self.snapshot_path(), m.build()).map_err(|e| e.to_string())?;
        Ok(SnapshotResult {
            snapshot_name: self.snapshot_path().display().to_string(),
            head_block_num: head,
            head_block_id: hex::encode(m.head_id()),
        })
    }

    fn schedule_snapshot(&self, _height: u64) -> Result<(), String> {
        Err("snapshot scheduler unavailable".into())
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
        let head = if traffic_resumed {
            self.head.get() + polls.min(5)
        } else {
            self.head.get()
        };
        Ok(Some(self.info(head)))
    }

    fn ignite(&self) -> Result<String, String> {
        self.ignited.set(true);
        Ok("mock metalgo restarted".into())
    }

    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        self.hooks.borrow_mut().push(cmd.to_string());
        Ok(format!("ran: {cmd}"))
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
post_ignite = "resume-traffic"
on_live = "flip-gateway"
"#,
        dir = dir.display(),
    );
    let path = dir.join("ceremony.toml");
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

    // Hooks ran in ceremony order.
    let hooks = ops.hooks.borrow();
    assert_eq!(*hooks, vec!["resume-traffic".to_string(), "flip-gateway".to_string()]);

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
    // The cut settled one past the pause head and the snapshot matches it.
    let frozen: serde_json::Value = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .find(|v: &serde_json::Value| v["state"] == "FROZEN" && v["kind"] == "transition")
        .unwrap();
    assert_eq!(frozen["data"]["cut_height"].as_u64().unwrap(), 121);
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
    // ignited, no traffic hook ran.
    assert_eq!(ops.resumes.get(), 1);
    assert!(!ops.ignited.get());
    assert!(ops.hooks.borrow().is_empty());
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
            "data": {"cut_height": cut, "cut_block_id": hex::encode(m.head_id()),
                     "last_source_block_time": "2024-01-01T00:00:00.000",
                     "chain_id": hex::encode(CHAIN_ID)}}),
        serde_json::json!({"seq": 2, "ts_ms": 3, "ts": "t", "kind": "transition", "state": "SNAPSHOTTED",
            "data": {"snapshot_file": snapshot_path.display().to_string(), "cut_height": cut}}),
    ];
    let lines: Vec<String> = entries.iter().map(|e| e.to_string()).collect();
    std::fs::write(&cfg.journal_path, lines.join("\n") + "\n").unwrap();

    let ops = MockOps::new(dir.path(), 121);
    ops.paused.set(true); // world state: producer is paused, as at crash time
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
