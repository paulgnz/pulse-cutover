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
    /// rc.29: `pause()` answers Ok but the producer keeps producing.
    pause_ignored: Cell<bool>,
    /// r30 round 7: while THIS producer is paused, the fleet's other producers keep producing up to this head (then
    /// they pause too): H can finalize without this node.
    peers_produce_to: Cell<u64>,
    /// r30 round 8: someone else pauses the producer once its head reaches this height (0 = never): e.g. during the
    /// scheduled-snapshot wait, after this ceremony's last ownership check.
    external_pause_at: Cell<u64>,
    /// r30 round 8: the external party resumes its pause once the (paused) head reaches this height (0 = never).
    external_resume_at: Cell<u64>,
    /// r30 round 8: `producer_paused()` cannot be read (the producer API errors).
    paused_read_fails: Cell<bool>,
    /// r30 round 9: the next N producer-paused reads fail (a transient error), then reads answer again.
    paused_read_fail_count: Cell<u32>,
    /// r30 round 9: a resume arms `paused_read_fail_count` with this many failing reads (transient errors right after).
    fail_reads_after_resume: Cell<u32>,
    /// r30 round 9: once the freeze hook ran, this many producer-paused reads fail (then reads answer again).
    fail_reads_after_freeze: Cell<u32>,
    /// r30 round 8: `resume()` answers Ok but the producer stays paused (someone else's pause on top, a stuck node).
    resume_ignored: Cell<bool>,
    /// rc.29: the paused source gains one block once the snapshot was taken (after the quiescence audit).
    move_after_snapshot: Cell<bool>,
    resumes: Cell<u32>,
    /// Round-3 review: every pause() call (the shadow gate must keep a retargeted source untouched).
    pauses: Cell<u32>,
    ignited: Cell<bool>,
    target_polls: Cell<u64>,
    /// Head of the (mock) imported chain: the cut height, set by
    /// create_snapshot; advances only once traffic resumes.
    target_head: Cell<u64>,
    /// If true, produce one extra block on the SECOND poll after pause (a
    /// late block arriving over p2p — quiescence must absorb + journal it).
    late_block: Cell<bool>,
    /// If true, the source keeps producing after the pause (a producer that
    /// ignored it): quiescence never passes and must time out.
    never_quiesce: Cell<bool>,
    paused_polls: Cell<u32>,
    /// Fault injection for the burn-off audit: transactions in each
    /// post-cut block, or an unreadable post-cut block.
    burnoff_tx_per_block: Cell<u64>,
    burnoff_read_fails: Cell<bool>,
    hooks: RefCell<Vec<String>>,
    // --- r30 round 10: model-checker instrumentation (join model; inert unless `model_on`) ---
    /// Ground-truth checks at the action point (I2 reopen into paused, I3 external resume, I4 resume past the point
    /// of no return) and a crash injected at the N-th instrumented operation.
    model_on: Cell<bool>,
    model_journal: RefCell<Option<PathBuf>>,
    model_calls: Cell<u64>,
    model_crash_at: Cell<u64>,
    /// The current pause is someone else's (the model's external actor), not this agent's.
    model_external_pause: Cell<bool>,
    /// r30 round 11 (I3 per pause episode): `external_pause_release` records in the journal when the current external
    /// pause began, or when the last release was consumed by a resume. A resume of that pause is legal only once a
    /// newer release exists, and each release authorizes one resume.
    model_release_baseline: Cell<usize>,
    /// r30 round 11 (I6): operations allowed in the current step (0 = unbounded); exceeding it = no progress.
    model_budget: Cell<u64>,
    /// r30 round 11: the next pause() takes effect and the agent is killed before it can journal the ack.
    model_crash_after_pause: Cell<bool>,
    model_violations: RefCell<Vec<String>>,
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
    coord_down: Cell<bool>,
    public_stale_source: Cell<bool>,
    /// /api/status: producers agreeing with our evidence = `fleet_agree` once
    /// `fleet_agree_after` status polls have happened (1 before that: just us).
    fleet_agree: Cell<usize>,
    fleet_agree_after: Cell<u32>,
    fleet_polls: Cell<u32>,
    /// Override for GET /api/status (roster fleet-gate tests).
    status_doc: RefCell<Option<serde_json::Value>>,
    /// rc.23: after this many status polls, serve this document instead.
    status_doc_after: RefCell<Option<(u32, serde_json::Value)>>,
    /// rc.23: the next N status polls fail (a transient relay error).
    status_fail_times: Cell<u32>,
    /// rc.23: when this hook command first fails (flaky), arm `status_fail_times` with N.
    fail_status_on_hook: RefCell<Option<(String, u32)>>,
    status_failures_served: Cell<u32>,
    /// Stale reports age like the real relay's: `age_ms` values >= 60 000 grow 2 s per poll.
    age_stale_per_poll: Cell<bool>,
    // --- fault injection ---
    /// The ignited target presents a DIFFERENT block id at the cut (wrong lineage).
    target_fork: Cell<bool>,
    /// Target head stops advancing after this many target polls (post-LIVE stall).
    target_stall_after: Cell<u64>,
    /// rc.23: ... and resumes at this poll (0 = never): a temporary stall.
    target_stall_until: Cell<u64>,
    /// rc.23: a hook command that fails this many times, then succeeds.
    flaky_hook: RefCell<Option<(String, u32)>>,
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
    // --- upstream ignition world ---
    /// Source chain_id (default hex(CHAIN_ID)).
    chain_id: RefCell<String>,
    /// The ignited target presents this chain_id instead of the source's (PulseVM v1.0.0 signs
    /// with metalgo's blockchain id).
    target_chain_id: RefCell<Option<String>>,
    /// Block ids are REAL Antelope ids of packable blocks (built from the stage-2 fixture block
    /// re-numbered to each height), so the upstream full-block anchor can be checked end to end.
    real_blocks: Cell<bool>,
    /// rc.27: what the target's getInfo reports as protocol fields (None = an older PulseVM without them).
    target_protocol: RefCell<Option<serde_json::Value>>,
    /// rc.27 review: write (path, contents) right before answering target_protocol (an edit after install).
    target_protocol_edit: RefCell<Option<(std::path::PathBuf, String)>>,
    /// source_block (full get_block) calls fail.
    source_block_fails: Cell<bool>,
    /// r30 shadow: what the target metalgo reports for info.getNetworkID (None = no answer).
    shadow_network_id: Cell<Option<u64>>,
    /// r30 shadow: the source's get_producer_schedule answer (None = no answer).
    shadow_schedule: RefCell<Option<serde_json::Value>>,
    /// r30 shadow-fix: what PUBLIC XPR mainnet answers ({"keys": [...], "same_blocks": bool, "head": n}); None = unreachable.
    public_mainnet: RefCell<Option<serde_json::Value>>,
    /// round-4 (R4-1): what the PRODUCER API reports as its head block id. None = the source node's own head block
    /// (one node behind both APIs); Some(answer) = that answer (e.g. a real mainnet block behind a routing proxy).
    producer_head: RefCell<Option<Option<String>>>,
    /// round-4 (R4-1): once any hook has run (the write freeze), the producer API answers with this head block id instead.
    producer_head_after_hook: RefCell<Option<String>>,
    /// r30 shadow-fix: metal_network_id calls so far, and from which call on it answers this instead.
    metal_calls: Cell<u32>,
    metal_network_from_call: Cell<Option<(u32, Option<u64>)>>,
    /// round-2 N5: what the proven metalgo's P-Chain says about a blockchain id (None = no answer); default "Validating".
    metal_chain_status: RefCell<Option<String>>,
    metal_chain_asked: RefCell<Vec<String>>,
    /// Placeholder values bound via bind_target, and the ones in force when ignite ran.
    bound: RefCell<Vec<(String, String)>>,
    ignite_vars: RefCell<Vec<(String, String)>>,
    /// GET /api/coord/<net> answers this doc once `<dir>/abort-now` exists (a fake pipeline tool
    /// creates it: the coordinator's abort lands while that tool runs).
    coord_doc_when_abort_file: RefCell<Option<serde_json::Value>>,
    /// Scripted eosio/producers answers, one per read (then the default: bp1, active, the test
    /// producer key).
    producer_rows: RefCell<std::collections::VecDeque<Option<serde_json::Value>>>,
    producer_reads: Cell<u32>,
}

/// An eosio/producers row as nodeos prints it (legacy EOS key format).
fn producer_row(owner: &str, key: &[u8; 33]) -> serde_json::Value {
    serde_json::json!({"owner": owner, "is_active": 1, "total_votes": "1.0",
        "producer_key": pulse_cutover::keys::format_legacy_public(key), "url": "", "location": 0})
}

/// The test producer's secret (fixture only) and its public key.
const TEST_PRODUCER_SECRET: [u8; 32] = [5u8; 32];
fn test_producer_pub() -> [u8; 33] {
    pulse_cutover::keys::public_of_private_k1(&pulse_cutover::keys::format_private_k1(&TEST_PRODUCER_SECRET)).unwrap()
}

/// The stage-2 fixture block (XPR testnet 408461570, 6 receipts) re-numbered to `h`: its id is
/// computed by the real packer, so it is a consistent, packable anchor at any height.
fn fixture_block(h: u64) -> serde_json::Value {
    let mut b: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/xpr-testnet-block-408461570.json")).unwrap();
    let prev = b["previous"].as_str().unwrap().to_string();
    b["previous"] = serde_json::json!(format!("{:08x}{}", h - 1, &prev[8..]));
    let id = pulse_cutover::upstream::pack_signed_block(&b).unwrap().id;
    b["id"] = serde_json::json!(id);
    b["block_num"] = serde_json::json!(h);
    b
}

impl MockOps {
    /// r30 round 10 model: count an instrumented operation; panic (a simulated kill) at the scripted one.
    fn model_op(&self, name: &str) {
        if !self.model_on.get() {
            return;
        }
        let n = self.model_calls.get() + 1;
        self.model_calls.set(n);
        if self.model_budget.get() > 0 && n > self.model_budget.get() {
            self.model_budget.set(0);
            panic!("MODEL-NO-PROGRESS after {n} operations (last: {name})");
        }
        if self.model_crash_at.get() == n {
            self.model_crash_at.set(0);
            panic!("MODEL-CRASH-INJECTED at op {n} ({name})");
        }
    }

    fn model_journal_text(&self) -> String {
        self.model_journal.borrow().as_ref().and_then(|p| std::fs::read_to_string(p).ok()).unwrap_or_default()
    }

    fn model_releases(&self) -> usize {
        self.model_journal_text().matches(r#""side_effect":"external_pause_release""#).count()
    }

    fn model_violate(&self, inv: &str, why: String) {
        self.model_violations.borrow_mut().push(format!("{inv}: {why}"));
    }

    fn new(dir: &std::path::Path, start_head: u64) -> Self {
        MockOps {
            dir: dir.to_path_buf(),
            head: Cell::new(start_head),
            paused: Cell::new(false),
            pause_ignored: Cell::new(false),
            peers_produce_to: Cell::new(0),
            move_after_snapshot: Cell::new(false),
            resumes: Cell::new(0),
            pauses: Cell::new(0),
            ignited: Cell::new(false),
            target_polls: Cell::new(0),
            target_head: Cell::new(0),
            late_block: Cell::new(false),
            never_quiesce: Cell::new(false),
            paused_polls: Cell::new(0),
            burnoff_tx_per_block: Cell::new(0),
            burnoff_read_fails: Cell::new(false),
            hooks: RefCell::new(Vec::new()),
            model_on: Cell::new(false),
            model_journal: RefCell::new(None),
            model_calls: Cell::new(0),
            model_crash_at: Cell::new(0),
            model_external_pause: Cell::new(false),
            model_release_baseline: Cell::new(0),
            model_budget: Cell::new(0),
            model_crash_after_pause: Cell::new(false),
            model_violations: RefCell::new(Vec::new()),
            now: Cell::new(1_000_000),
            drift: Cell::new(1),
            external_pause_at: Cell::new(0),
            external_resume_at: Cell::new(0),
            paused_read_fails: Cell::new(false),
            paused_read_fail_count: Cell::new(0),
            fail_reads_after_resume: Cell::new(0),
            fail_reads_after_freeze: Cell::new(0),
            resume_ignored: Cell::new(false),
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
            coord_down: Cell::new(false),
            public_stale_source: Cell::new(false),
            fleet_agree: Cell::new(0),
            fleet_agree_after: Cell::new(0),
            fleet_polls: Cell::new(0),
            status_doc: RefCell::new(None),
            status_doc_after: RefCell::new(None),
            status_fail_times: Cell::new(0),
            fail_status_on_hook: RefCell::new(None),
            status_failures_served: Cell::new(0),
            age_stale_per_poll: Cell::new(false),
            target_fork: Cell::new(false),
            target_stall_after: Cell::new(u64::MAX),
            target_stall_until: Cell::new(0),
            flaky_hook: RefCell::new(None),
            source_calls: Cell::new(0),
            ignite_fails: Cell::new(false),
            target_latency_ms: Cell::new(0),
            resume_fails: Cell::new(false),
            events: RefCell::new(Vec::new()),
            orphan: RefCell::new(None),
            panic_on_hook: RefCell::new(None),
            chain_id: RefCell::new(hex::encode(CHAIN_ID)),
            target_chain_id: RefCell::new(None),
            real_blocks: Cell::new(false),
            target_protocol: RefCell::new(None),
            target_protocol_edit: RefCell::new(None),
            source_block_fails: Cell::new(false),
            shadow_network_id: Cell::new(None),
            shadow_schedule: RefCell::new(None),
            public_mainnet: RefCell::new(None),
            producer_head: RefCell::new(None),
            producer_head_after_hook: RefCell::new(None),
            metal_calls: Cell::new(0),
            metal_network_from_call: Cell::new(None),
            metal_chain_status: RefCell::new(Some("Validating".into())),
            metal_chain_asked: RefCell::new(Vec::new()),
            bound: RefCell::new(Vec::new()),
            ignite_vars: RefCell::new(Vec::new()),
            coord_doc_when_abort_file: RefCell::new(None),
            producer_rows: RefCell::new(std::collections::VecDeque::new()),
            producer_reads: Cell::new(0),
        }
    }

    fn block_id(&self, h: u64) -> String {
        if self.real_blocks.get() {
            fixture_block(h)["id"].as_str().unwrap().to_string()
        } else {
            hex::encode(mini(h as u32).head_id())
        }
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("snapshot-cut.bin")
    }

    fn info(&self, head: u64) -> ChainInfo {
        ChainInfo {
            chain_id: self.chain_id.borrow().clone(),
            head_block_num: head,
            head_block_id: self.block_id(head),
            head_block_time: "2024-01-01T00:00:00.000".into(),
            last_irreversible_block_num: head.saturating_sub(1),
        }
    }
}

impl ChainOps for MockOps {
    fn metal_network_id(&self, base: &str) -> Result<Option<u64>, String> {
        assert_eq!(base, "http://127.0.0.1:9650", "the shadow network proof must ask the target's own metalgo");
        let n = self.metal_calls.get() + 1;
        self.metal_calls.set(n);
        if let Some((from, answer)) = self.metal_network_from_call.get().filter(|(from, _)| n >= *from) {
            let _ = from;
            return Ok(answer);
        }
        Ok(self.shadow_network_id.get())
    }
    fn metal_blockchain_status(&self, base: &str, blockchain_id: &str) -> Result<Option<String>, String> {
        assert_eq!(base, "http://127.0.0.1:9650", "the chain check must ask the PROVEN metalgo (target.rpc_url's)");
        self.metal_chain_asked.borrow_mut().push(blockchain_id.to_string());
        Ok(self.metal_chain_status.borrow().clone())
    }
    fn public_chain_post(&self, base: &str, path: &str, body: serde_json::Value) -> Result<serde_json::Value, String> {
        assert!(pulse_cutover::shadow::PUBLIC_XPR_MAINNET_APIS.contains(&base), "only the pinned public endpoints: {base}");
        let pm = self.public_mainnet.borrow().clone().ok_or_else(|| format!("{base}: unreachable (mock)"))?;
        match path {
            // round-2 N2: "head": null = an endpoint whose get_info carries no head_block_num.
            // round-3 F2: "heads": {base: head} gives each pinned endpoint its own head (null = no head_block_num).
            "/v1/chain/get_info" if pm.get("heads").is_some() => Ok(match pm["heads"].get(base) {
                Some(serde_json::Value::Null) | None => serde_json::json!({"chain_id": MAINNET}),
                Some(h) => serde_json::json!({"chain_id": MAINNET, "head_block_num": h.as_u64().unwrap()}),
            }),
            "/v1/chain/get_info" => Ok(match pm.get("head") {
                Some(serde_json::Value::Null) => serde_json::json!({"chain_id": MAINNET}),
                _ => serde_json::json!({"chain_id": MAINNET, "head_block_num": pm["head"].as_u64().unwrap_or(1u64 << 40)}),
            }),
            "/v1/chain/get_producer_schedule" => {
                let producers: Vec<serde_json::Value> = pm["keys"].as_array().cloned().unwrap_or_default().iter().enumerate()
                    .map(|(i, k)| serde_json::json!({"producer_name": format!("real{i}"),
                        "authority": [0, {"threshold": 1, "keys": [{"key": k, "weight": 1}]}]})).collect();
                Ok(serde_json::json!({"active": {"version": 7, "producers": producers}, "pending": null, "proposed": null}))
            }
            "/v1/chain/get_block" => {
                let n = body["block_num_or_id"].as_u64().unwrap();
                let id = if pm["same_blocks"].as_bool() == Some(true) { self.block_id(n) } else { format!("{n:08x}{}", "ee".repeat(28)) };
                Ok(serde_json::json!({"id": id, "block_num": n}))
            }
            other => Err(format!("unexpected public path {other}")),
        }
    }
    fn source_producer_schedule(&self) -> Result<Option<serde_json::Value>, String> {
        Ok(self.shadow_schedule.borrow().clone())
    }
    fn source_producer_head(&self) -> Result<Option<String>, String> {
        if let Some(id) = self.producer_head_after_hook.borrow().clone().filter(|_| !self.hooks.borrow().is_empty()) {
            return Ok(Some(id));
        }
        match self.producer_head.borrow().clone() {
            Some(answer) => Ok(answer),
            None => Ok(Some(self.block_id(self.head.get()))),
        }
    }
    fn source_info(&self) -> Result<ChainInfo, String> {
        self.model_op("source_info");
        self.source_calls.set(self.source_calls.get() + 1);
        if self.stopped.get() {
            return Err("connection refused (nodeos stopped)".into());
        }
        let ext = self.external_pause_at.get();
        if ext > 0 && !self.paused.get() && self.head.get() >= ext {
            self.paused.set(true); // someone else's pause: not counted in `pauses`
        }
        let rel = self.external_resume_at.get();
        if rel > 0 && self.paused.get() && self.head.get() >= rel {
            self.paused.set(false); // its owner resumes it (not counted in `resumes`)
            self.external_resume_at.set(0);
        }
        if !self.paused.get() {
            self.head.set(self.head.get() + self.drift.get());
        } else {
            let polls = self.paused_polls.get() + 1;
            self.paused_polls.set(polls);
            if self.never_quiesce.get() {
                self.head.set(self.head.get() + 1);
            }
            if self.head.get() < self.peers_produce_to.get() {
                self.head.set(self.head.get() + 1);
            }
            if polls == 2 && self.late_block.replace(false) {
                self.head.set(self.head.get() + 1);
            }
        }
        Ok(self.info(self.head.get()))
    }

    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        // Called once the quiescence window passed: a late block from a peer lands after the audit.
        if self.move_after_snapshot.replace(false) {
            self.head.set(self.head.get() + 1);
        }
        Ok((self.block_id(block_num), "2024-01-01T00:00:00.000".into()))
    }

    fn target_protocol(&self) -> Result<Option<serde_json::Value>, String> {
        if let Some((p, text)) = self.target_protocol_edit.borrow().as_ref() {
            std::fs::write(p, text).unwrap();
        }
        Ok(self.target_protocol.borrow().clone())
    }

    fn source_block(&self, block_num: u64, _rpc_url: Option<&str>) -> Result<serde_json::Value, String> {
        if self.source_block_fails.get() {
            return Err("get_block: unknown block (snapshot-started nodeos)".into());
        }
        Ok(fixture_block(block_num))
    }

    fn bind_target(&self, vars: &[(String, String)]) {
        let mut b = self.bound.borrow_mut();
        for (k, v) in vars {
            b.retain(|(k2, _)| k2 != k);
            b.push((k.clone(), v.clone()));
        }
    }

    fn source_block_tx_count(&self, _block_num: u64) -> Result<u64, String> {
        if self.burnoff_read_fails.get() {
            return Err("get_block timed out".into());
        }
        Ok(self.burnoff_tx_per_block.get()) // default 0: writes are frozen
    }

    fn producer_paused(&self) -> Result<bool, String> {
        self.model_op("producer_paused");
        if self.paused_read_fails.get() {
            return Err("http://mock/v1/producer/paused: connection refused".into());
        }
        if self.freeze_head.get() > 0 && self.fail_reads_after_freeze.get() > 0 {
            self.fail_reads_after_freeze.set(self.fail_reads_after_freeze.get() - 1);
            return Err("http://mock/v1/producer/paused: timed out".into());
        }
        if self.paused_read_fail_count.get() > 0 {
            self.paused_read_fail_count.set(self.paused_read_fail_count.get() - 1);
            return Err("http://mock/v1/producer/paused: timed out".into());
        }
        Ok(self.paused.get())
    }

    fn pause(&self) -> Result<(), String> {
        self.model_op("pause");
        self.pauses.set(self.pauses.get() + 1);
        if !self.pause_ignored.get() {
            if !self.paused.get() {
                self.model_external_pause.set(false);
            }
            self.paused.set(true);
        }
        if self.model_on.get() && self.model_crash_after_pause.replace(false) {
            panic!("MODEL-CRASH-INJECTED after the pause took effect");
        }
        Ok(())
    }

    fn resume(&self) -> Result<(), String> {
        self.model_op("resume");
        if self.model_on.get() {
            let j = self.model_journal_text();
            if self.paused.get() && self.model_external_pause.get() && !self.resume_fails.get() && !self.resume_ignored.get() {
                let now = self.model_releases();
                if now <= self.model_release_baseline.get() {
                    self.model_violate("I3", format!("the agent resumed an EXTERNAL pause with no release journaled since \
                        that pause began (releases then {}, now {now})", self.model_release_baseline.get()));
                } else {
                    self.model_release_baseline.set(now); // consumed by this resume
                }
            }
            if j.contains(r#""side_effect":"create_chain""#) || j.contains(r#""side_effect":"ignite_started""#)
                || j.contains(r#""side_effect":"join""#) {
                self.model_violate("I4", "the agent resumed the source after this journal's point of no return".into());
            }
        }
        self.events.borrow_mut().push("resume".into());
        if self.resume_fails.get() {
            return Err("http://mock/v1/producer/resume: connection refused".into());
        }
        self.resumes.set(self.resumes.get() + 1);
        if !self.resume_ignored.get() {
            self.paused.set(false);
        }
        if self.fail_reads_after_resume.get() > 0 {
            self.paused_read_fail_count.set(self.fail_reads_after_resume.get());
        }
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
            format!("CUT_HEIGHT={cut}\nCUT_BLOCK_ID={}\nCHAIN_ID={}\n", self.block_id(cut), self.chain_id.borrow()),
        );
        // Production continues; the block that finalized the cut arrives.
        self.head.set(cut + 1);
        self.target_head.set(cut);
        Ok(SnapshotResult {
            snapshot_name: self.snapshot_path().display().to_string(),
            head_block_num: cut,
            head_block_id: self.block_id(cut),
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
        let file = self.dir.join(format!("snapshot-{}.bin", self.block_id(height)));
        std::fs::write(&file, m.build()).map_err(|e| e.to_string())?;
        let _ = std::fs::write(
            self.dir.join("cut-facts.env"),
            format!("CUT_HEIGHT={height}\nCUT_BLOCK_ID={}\nCHAIN_ID={}\n", self.block_id(height), self.chain_id.borrow()),
        );
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
        let (a, b) = (self.target_stall_after.get(), self.target_stall_until.get());
        let progress = if b > a && polls >= b { a + (polls - b) } else { polls.min(a) };
        let head = if traffic_resumed {
            self.target_head.get() + progress
        } else {
            self.target_head.get()
        };
        let mut info = self.info(head);
        if let Some(c) = self.target_chain_id.borrow().clone() {
            info.chain_id = c;
        }
        if self.target_fork.get() {
            info.head_block_id = format!("ff{}", &info.head_block_id[2..]);
        }
        Ok(Some(info))
    }

    fn public_info(&self, _public_url: &str) -> Result<Option<ChainInfo>, String> {
        if self.public_stale_source.get() && self.flipped.get() {
            // The flip silently failed: the public route still reaches a leftover SOURCE that keeps
            // producing (same chain_id) at heights right next to the target's.
            let t = self.target_info()?;
            return Ok(t.map(|t| self.info(t.head_block_num)).map(|mut i| { i.head_block_id = format!("5e{}", &i.head_block_id[2..]); i }));
        }
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

    fn public_block_id(&self, _public_url: &str, height: u64) -> Result<Option<String>, String> {
        if self.public_stale_source.get() {
            let id = self.block_id(height);
            return Ok(Some(format!("5e{}", &id[2..])));
        }
        if self.flipped.get() {
            if self.flip_breaks_public.get() || !self.ignited.get() {
                return Ok(None);
            }
            // Routed to the target: the target's block (a forked target presents its own ids).
            let id = self.block_id(height);
            return Ok(Some(if self.target_fork.get() { format!("ff{}", &id[2..]) } else { id }));
        }
        if self.stopped.get() {
            return Ok(None);
        }
        // Still the SOURCE: above the cut its blocks are burn-off blocks the target never had.
        let id = self.block_id(height);
        Ok(Some(if height > self.target_head.get() { format!("5e{}", &id[2..]) } else { id }))
    }

    fn ignite(&self) -> Result<String, String> {
        *self.ignite_vars.borrow_mut() = self.bound.borrow().clone();
        self.ignited.set(true);
        if self.ignite_fails.get() {
            return Err("systemctl restart exited 1 (partial start)".into());
        }
        Ok("mock metalgo restarted".into())
    }

    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        self.model_op(&format!("hook {cmd}"));
        if self.model_on.get() && cmd == "reopen-writes" && self.paused.get() {
            self.model_violate("I2", format!("writes reopened while the producer is paused (external: {})",
                self.model_external_pause.get()));
        }
        self.hooks.borrow_mut().push(cmd.to_string());
        self.events.borrow_mut().push(format!("hook:{cmd}"));
        if self.panic_on_hook.borrow().as_deref() == Some(cmd) {
            panic!("simulated crash inside hook `{cmd}`");
        }
        if cmd.starts_with("fail-") {
            return Err(format!("`{cmd}` exited 1"));
        }
        if let Some((name, left)) = self.flaky_hook.borrow_mut().as_mut() {
            if name == cmd && *left > 0 {
                *left -= 1;
                if let Some((h, n)) = self.fail_status_on_hook.borrow_mut().take() {
                    if h == cmd { self.status_fail_times.set(n); }
                }
                return Err(format!("`{cmd}` exited 1 (flaky)"));
            }
        }
        if cmd == "freeze-writes" {
            self.freeze_head.set(self.head.get());
        }
        // r30 round 7: the write freeze, after which an operator (or the hook itself) pauses the producer by hand.
        if cmd == "freeze-writes-operator-pauses" {
            self.freeze_head.set(self.head.get());
            self.paused.set(true);
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

    fn source_producer(&self, producer: &str) -> Result<Option<serde_json::Value>, String> {
        self.producer_reads.set(self.producer_reads.get() + 1);
        if let Some(row) = self.producer_rows.borrow_mut().pop_front() {
            return Ok(row);
        }
        Ok((producer == "bp1").then(|| producer_row("bp1", &test_producer_pub())))
    }

    fn run_long_cancellable(&self, cmd: &str, cancel: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        self.model_op("run_long");
        if !cmd.contains("fake-") {
            return if cancel() { Ok(None) } else { self.run_long(cmd).map(Some) };
        }
        // Fake pipeline tools run for real and can be cut short; every cancel poll advances the
        // mock clock by a second (the coordinator check rate-limits itself on that clock).
        self.hooks.borrow_mut().push(cmd.to_string());
        let now = &self.now;
        pulse_cutover::ops::run_shell_cancellable(cmd, std::time::Duration::from_secs(120), None, &mut || {
            now.set(now.get() + 1000);
            cancel()
        })
    }

    fn get_json(&self, url: &str) -> Result<Option<serde_json::Value>, String> {
        self.model_op("get_json");
        if url.contains("/api/coord/") {
            if self.dir.join("abort-now").exists() {
                if let Some(doc) = self.coord_doc_when_abort_file.borrow().clone() {
                    return Ok(Some(doc));
                }
            }
            if self.coord_down.get() {
                return Err("mission control unreachable".into());
            }
            // The relay is up and serves the signed event e1 (no abort) unless a test sets another document.
            return Ok(Some(self.coord_doc.borrow().clone().unwrap_or_else(|| serde_json::json!({"event": mock_event()}))));
        }
        if url.ends_with("/api/status") {
            if self.coord_down.get() {
                return Err("mission control unreachable".into());
            }
            if self.status_fail_times.get() > 0 {
                self.status_fail_times.set(self.status_fail_times.get() - 1);
                self.status_failures_served.set(self.status_failures_served.get() + 1);
                return Err("HTTP 502 (transient)".into());
            }
            let polls = self.fleet_polls.get() + 1;
            self.fleet_polls.set(polls);
            let after = self.status_doc_after.borrow().clone().filter(|(n, _)| polls > *n).map(|(_, d)| d);
            if let Some(mut doc) = after.or_else(|| self.status_doc.borrow().clone()) {
                if self.age_stale_per_poll.get() {
                    fn bump(v: &mut serde_json::Value, polls: u64) {
                        match v {
                            serde_json::Value::Object(m) => {
                                for (k, x) in m.iter_mut() {
                                    if k == "age_ms" && x.as_u64().map(|a| a >= 60_000).unwrap_or(false) {
                                        *x = serde_json::json!(x.as_u64().unwrap() + polls * 2000);
                                    } else {
                                        bump(x, polls);
                                    }
                                }
                            }
                            serde_json::Value::Array(a) => a.iter_mut().for_each(|x| bump(x, polls)),
                            _ => {}
                        }
                    }
                    bump(&mut doc, polls as u64);
                }
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
                    "boundary": {"cut_block": self.target_head.get(), "status": "valid", "valid": true},
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
rehearsal = true
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
rehearsal = true
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

/// Journal::open for tests. Other tests in this binary spawn real child processes; a fork taken
/// while this test holds its journal lock briefly shares the lock until the child execs (the fd is
/// close-on-exec), so a reopen right after a simulated crash can see "lock busy" for a moment.
/// Retry briefly on exactly that error; anything else fails the test.
fn open_journal(path: &std::path::Path) -> (Journal, pulse_cutover::journal::Recovered) {
    for _ in 0..100 {
        match Journal::open(path) {
            Ok(x) => return x,
            Err(e) if e.contains("another pulse-cutover process holds") => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => panic!("{e}"),
        }
    }
    panic!("journal lock still busy after 2 s: {}", path.display())
}

/// Runs to a terminal state. rc.23: STRANDED ends the run with an error (sealed, like HALTED); it is
/// returned here as the state so tests can assert it.
fn run_machine(cfg: &Config, ops: &MockOps) -> State {
    match run_machine_result(cfg, ops) {
        Ok(s) => s,
        Err(e) if e.starts_with("STRANDED") => State::Stranded,
        Err(e) => panic!("{e}"),
    }
}

fn run_machine_result(cfg: &Config, ops: &MockOps) -> Result<State, String> {
    let (journal, recovered) = open_journal(&cfg.journal_path);
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
fn producer_that_ignores_the_pause_aborts_at_the_quiescence_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path(), 120);
    cfg.ceremony.quiescence_timeout_secs = 2;
    let ops = MockOps::new(dir.path(), 110);
    ops.never_quiesce.set(true);

    let terminal = run_machine(&cfg, &ops);
    assert_eq!(terminal, State::Aborted, "must not wait forever or ignite on an un-quiesced head");
    assert!(!ops.ignited.get());

    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("quiescence_timeout_secs"));
    // Late blocks are journaled, but capped (a runaway producer must not flood the journal).
    assert_eq!(text.matches("late_block_after_pause").count(), 10);
}

#[test]
fn zero_quiescence_timeout_is_rejected_at_load() {
    let dir = tempfile::tempdir().unwrap();
    test_config(dir.path(), 120);
    let base = std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap();
    let bad = base.replace("quiescence_polls = 3", "quiescence_polls = 3\nquiescence_timeout_secs = 0");
    assert_ne!(bad, base);
    let err = load_toml(dir.path(), "bad.toml", &bad).unwrap_err();
    assert!(err.contains("quiescence_timeout_secs"), "{err}");
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
                     "cut_block_id": hex::encode(m.head_id()), "head_at_pause": cut,
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
rehearsal = true
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
rehearsal = true
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
rehearsal = true
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
    assert_eq!(scan["served_imports"], serde_json::json!(183));

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

/// The fake export's sidecar (printf template: %s = chain id, %s = cut block id). recv
/// sequences 3 + 2 = global 5, as on a chain that started at genesis.
const SIDECAR: &str = r#"{"version":1,"source_chain_id":"%s","source_block_id":"%s","account_metadata":[{"name":1,"recv_sequence":3,"auth_sequence":1,"code_sequence":0,"abi_sequence":0},{"name":2,"recv_sequence":2,"auth_sequence":0,"code_sequence":0,"abi_sequence":0}],"global_action_sequence":5,"input_transactions":[]}"#;

/// Rewrite the fake export so it writes `sidecar_printf` (same %s %s arguments) instead.
fn fake_export_with_sidecar(dir: &std::path::Path, sidecar_printf: &str) {
    let export = dir.join("fake-export.sh");
    let body = std::fs::read_to_string(&export).unwrap().replace(SIDECAR, sidecar_printf);
    write_script(&export, &body);
}

fn upstream_journal_has_verified(text: &str) -> bool {
    text.lines().any(|l| {
        let v: serde_json::Value = serde_json::from_str(l).unwrap();
        v["state"] == "VERIFIED" && v["kind"] == "transition"
    })
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
             . {d}/cut-facts.env\n\
             if [ -z \"$NO_SIDECAR\" ]; then printf '{SIDECAR}' \"$CHAIN_ID\" \"$CUT_BLOCK_ID\" > \"$out/work/deferred-transactions.json\"; fi\n\
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
             printf '{{\"version\":1,\"checkpoint_sha256\":\"%s\",\"checkpoint_revision\":%s,\"source_block_id\":\"%s\",\"source_chain_id\":\"%s\"}}' \
               \"$(sha \"$3\")\" \"$CUT_HEIGHT\" \"$CUT_BLOCK_ID\" \"$CHAIN_ID\" > \"$3.manifest.json\"\n\
             echo 'XPR state imported successfully: ImportSummary {{ accounts: 1 }}'\n"
        ),
    );
    write_script(
        &dir.join("fake-compare.sh"),
        &format!(
            "#!/bin/sh\n\
             [ -f \"$5\" ] || {{ echo 'usage: sidecar (5th arg) missing' >&2; exit 2; }}\n\
             if [ {compare_exit} -ne 0 ]; then echo 'table permission: nodeos=1 arena=2' >&2; exit {compare_exit}; fi\n\
             for t in {tables}; do echo \"table $t: rows=1 sha256=aa55\"; done\nexit 0\n",
            tables = pulse_cutover::upstream::REQUIRED_COMPARE_TABLES.join(" ")
        ),
    );
    // create_chain stand-in (the rig's Go helper): checks it was handed the migration genesis
    // and a chain config pointing at a boot manifest that carries the source block, counts its
    // invocations, and prints the ids.
    write_script(
        &dir.join("fake-create-chain.sh"),
        &format!(
            "#!/bin/sh\nset -e\necho run >> {d}/create-chain.calls\n\
             grep -q migration_checkpoint_sha256 \"$1\" || {{ echo 'genesis without migration_checkpoint_sha256' >&2; exit 3; }}\n\
             grep -q migration_manifest \"$2\" || {{ echo 'chain config without migration_manifest' >&2; exit 3; }}\n\
             grep -q source_block \"$3\" || {{ echo 'boot manifest without source_block' >&2; exit 3; }}\n\
             echo 'issuing CreateSubnetTx + CreateChainTx'\n\
             echo SUBNET_ID=2qFyanyVDk2LKrUsUdh3JjtYJYZGqUg9y9QAvMaWdQZZGaLiXY\n\
             echo BLOCKCHAIN_ID=2YXVy2NWHZJphNuvWry8So8TpQodhSb7ZVJPxhwhtKS6Z6JjpV\n"
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
rehearsal = true
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
fn upstream_backend_verifies_with_official_tools_and_stops_when_ignite_is_not_configured() {
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
    // Verify-only (no genesis_base / create_chain_cmd): a clean stop after VERIFIED, with what
    // still stands between an upstream ignite and a mainnet cut listed.
    let abort_err = entries
        .iter()
        .find(|v| v["kind"] == "error")
        .expect("journaled abort reason");
    let msg = abort_err["data"]["message"].as_str().unwrap();
    assert!(msg.contains("#61") && msg.contains("not configured"), "{msg}");
    let remaining = abort_err["data"]["detail"]["remaining_for_mainnet"].to_string();
    assert!(remaining.contains("TAPOS") && remaining.contains("chain_id"));
    assert!(abort_err["data"]["detail"]["remaining_for_mainnet"].is_array());
    assert_eq!(ops.resumes.get(), 1, "verify-only stop resumes the source");
    assert!(!text.contains("ignite_started"));
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
fn upstream_sidecar_sequences_are_journaled() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    run_machine_result(&cfg, &ops).ok();
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let ev = text.lines().map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .find(|v| !v["data"]["upstream_sidecar_sequences"].is_null()).expect("sequence evidence journaled");
    let seq = &ev["data"]["upstream_sidecar_sequences"];
    assert_eq!(seq["accounts"], 2);
    assert_eq!(seq["global_action_sequence"], 5);
    assert_eq!(seq["recv_sum_matches_global"], true);
    assert!(upstream_journal_has_verified(&text));
}

#[test]
fn upstream_sidecar_without_source_chain_id_fails_verification() {
    // A pre-full-state sidecar: upstream would restore the global sequence but leave every
    // per-account counter at 0 (issue #101 case 2).
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    fake_export_with_sidecar(dir.path(), r#"{"version":1,"x":"%s","source_block_id":"%s","global_action_sequence":5,"input_transactions":[]}"#);
    let cfg = upstream_test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("no source_chain_id"), "{text}");
    assert!(!upstream_journal_has_verified(&text));
}

#[test]
fn upstream_sidecar_from_another_block_or_without_accounts_fails_verification() {
    for (sidecar, want) in [
        (r#"{"version":1,"source_chain_id":"%s","source_block_id":"%s00","account_metadata":[{"name":1,"recv_sequence":5}],"global_action_sequence":5,"input_transactions":[]}"#, "is not the cut block"),
        (r#"{"version":1,"source_chain_id":"%s","source_block_id":"%s","account_metadata":[],"global_action_sequence":5,"input_transactions":[]}"#, "no account_metadata rows"),
        (r#"{"version":1,"source_chain_id":"%s","source_block_id":"%s","account_metadata":[{"name":1,"recv_sequence":0}],"global_action_sequence":0,"input_transactions":[]}"#, "global_action_sequence is 0"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        fake_export_with_sidecar(dir.path(), sidecar);
        let cfg = upstream_test_config(dir.path(), 120);
        assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Aborted, "{want}");
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        assert!(text.contains(want), "{want}: {text}");
    }
}

#[test]
fn upstream_sidecar_recv_sum_mismatch_fails_verification() {
    // Exact on a real XPR testnet export, so a mismatch means lost or altered counters.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    fake_export_with_sidecar(dir.path(), &SIDECAR.replace("\"global_action_sequence\":5", "\"global_action_sequence\":9"));
    let cfg = upstream_test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("sequence counters are inconsistent"), "{text}");
    assert!(!upstream_journal_has_verified(&text));
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

/// upstream_test_config + a coordinated event (e1) whose signed abort the mock relays once a
/// fake tool touches `<dir>/abort-now`; `tool` (export/import/compare/fingerprint) does that first
/// and then runs `sleep_secs` before doing its work.
fn upstream_abort_rig(dir: &std::path::Path, tool: &str, sleep_secs: u64) -> (Config, MockOps) {
    stage_fake_upstream_tools(dir, 0);
    let script = dir.join(format!("fake-{tool}.sh"));
    let body = std::fs::read_to_string(&script).unwrap();
    let (shebang, rest) = body.split_once('\n').unwrap();
    write_script(&script, &format!("{shebang}\ntouch {}/abort-now\nsleep {sleep_secs}\n{rest}", dir.display()));
    let _ = upstream_test_config(dir, 120);
    let path = dir.join("ceremony-upstream.toml");
    let text = format!("{}\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{}\"]\nevent_id = \"e1\"\nevent_hash = \"{}\"\n",
        std::fs::read_to_string(&path).unwrap(), hex::encode(coord_key().verifying_key().to_bytes()), mock_event_hash());
    std::fs::write(&path, text).unwrap();
    let cfg = Config::load(&path).unwrap();
    let ops = MockOps::new(dir, 110);
    *ops.coord_doc_when_abort_file.borrow_mut() = Some(serde_json::json!({
        "abort": signed(serde_json::json!({"type": "abort", "event_id": "e1", "network": "rehearsal"}))}));
    (cfg, ops)
}

#[test]
fn stage2_signed_abort_kills_a_running_upstream_step_promptly() {
    // Stage-2 run 3: ABORT posted 7 s after SNAPSHOTTED was honoured only after VERIFIED (23.7 s
    // later): export, import, compare and fingerprint all ran first. Now the running tool is killed.
    let dir = tempfile::tempdir().unwrap();
    let (cfg, ops) = upstream_abort_rig(dir.path(), "export", 30);
    let t = std::time::Instant::now();
    // rc.23 review #8: this rig's event has no roster, so the resume guard cannot judge the fleet: STRANDED.
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert!(t.elapsed() < std::time::Duration::from_secs(10), "the 30 s export was cut short: {:?}", t.elapsed());
    let e = journal_entries(&cfg);
    assert!(transition(&e, "VERIFIED").is_none());
    let err = e.iter().find(|v| v["kind"] == "error").expect("abort reason journaled");
    assert_eq!(err["state"], "SNAPSHOTTED");
    assert_eq!(err["data"]["message"], "coordinator aborted the event (signed) before ignition");
    assert_eq!(err["data"]["detail"]["during"], "upstream verification (export)");
    assert_eq!(err["data"]["detail"]["event_id"], "e1");
    assert!(pulse_cutover::upstream::find_file(&dir.path().join("upstream-work"), "manifest.env").is_none(), "the export never finished");
    assert!(!ops.hooks.borrow().iter().any(|h| h.contains("fake-import")), "nothing after the aborted step ran");
    assert_eq!(ops.resumes.get(), 0, "no roster: the source is NOT resumed (rc.23 review #8)");
}

#[test]
fn stage2_signed_abort_between_upstream_steps_stops_before_the_next_one() {
    for (tool, next, during) in [
        ("import", "fake-compare", "upstream verification (after import)"),
        ("compare", "fake-fingerprint", "upstream verification (after table compare)"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, ops) = upstream_abort_rig(dir.path(), tool, 0);
        assert_eq!(run_machine(&cfg, &ops), State::Stranded, "{tool} (no roster: rc.23 review #8)");
        let e = journal_entries(&cfg);
        assert!(transition(&e, "VERIFIED").is_none(), "{tool}");
        let err = e.iter().find(|v| v["kind"] == "error").expect("abort reason journaled");
        assert_eq!(err["data"]["message"], "coordinator aborted the event (signed) before ignition", "{tool}");
        let got = err["data"]["detail"]["during"].as_str().unwrap();
        // A fast tool may finish before the in-step poll sees the abort; then the boundary check does.
        assert!(got == during || got == format!("upstream verification ({tool})").replace("compare", "table compare"), "{tool}: {got}");
        assert!(!ops.hooks.borrow().iter().any(|h| h.contains(next)), "{tool}: {next} never ran");
        assert_eq!(ops.resumes.get(), 0, "{tool}");
    }
}

#[test]
fn stage2_import_cpu_scale_is_reported_as_inert_on_the_upstream_backend() {
    // Stage-2 runs 2/2b/2c: import_cpu_scale = 143 was set (and signed into the event) believing it
    // fixed CPU billing on the target; on the upstream backend PulseVM v1.0.0 has no such setting.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let _ = upstream_test_config(dir.path(), 120);
    let path = dir.path().join("ceremony-upstream.toml");
    let text = std::fs::read_to_string(&path).unwrap().replace("import_backend = \"upstream\"", "import_backend = \"upstream\"\nimport_cpu_scale = 143");
    std::fs::write(&path, text).unwrap();
    let cfg = Config::load(&path).unwrap();
    let w = cfg.warnings();
    assert_eq!(w.len(), 1);
    assert!(w[0].contains("import_cpu_scale = 143 is IGNORED"), "{w:?}");
    // Still part of the agreement with a signed event (payload unchanged, hash-compatible).
    let ev = |scale: u64| serde_json::json!({"type": "event", "network": "rehearsal", "h": 5000, "import_cpu_scale": scale, "roster": [{"producer": "bp1"}]});
    assert!(pulse_cutover::coord::validate_event(&ev(143), &cfg, "rehearsal", None, 0).is_ok());
    assert!(pulse_cutover::coord::validate_event(&ev(1), &cfg, "rehearsal", None, 0).unwrap_err().contains("import_cpu_scale"));
    // Journaled at ARM as not effective, with the warning.
    let ops = MockOps::new(dir.path(), 110);
    let _ = run_machine_result(&cfg, &ops);
    let e = journal_entries(&cfg);
    let armed = &transition(&e, "ARMED").unwrap()["data"];
    assert_eq!((armed["import_cpu_scale"].as_u64(), armed["import_cpu_scale_effective"].as_bool()), (Some(143), Some(false)));
    assert!(e.iter().any(|v| v["state"] == "ARMED" && v["data"]["config_warning"].as_str().is_some_and(|x| x.contains("IGNORED"))));
    // Default value on upstream, or any value on the fork backend: no warning.
    let _ = test_config(dir.path(), 120);
    let fork_text = std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap().replacen("[ceremony]\nrehearsal = true\n", "[ceremony]\nrehearsal = true\nimport_cpu_scale = 143\n", 1);
    let fork = load_toml(dir.path(), "fork-143.toml", &fork_text).unwrap();
    assert_eq!(fork.ceremony.import_cpu_scale, 143);
    assert!(fork.warnings().is_empty());
    assert!(upstream_test_config(dir.path(), 120).warnings().is_empty());
}

#[test]
fn upstream_backend_requires_upstream_section() {
    let dir = tempfile::tempdir().unwrap();
    let toml_text = format!(
        "journal_path = \"{d}/j.jsonl\"\n[ceremony]\nrehearsal = true\nfreeze_height = 5\nimport_backend = \"upstream\"\n\
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

/// The event the mock relay serves by default, and its payload hash (rc.29: a derived config carries it).
fn mock_event() -> serde_json::Value {
    signed(serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e1"}))
}
fn mock_event_hash() -> String {
    pulse_cutover::coord::payload_hash(&mock_event()).unwrap()
}

fn coord_config(dir: &std::path::Path, freeze_height: u64, quorum: usize, timeout: u64) -> Config {
    let base = test_config(dir, freeze_height);
    let _ = base;
    let text = std::fs::read_to_string(dir.join("ceremony.toml")).unwrap();
    let text = format!("{text}\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{}\"]\nevent_id = \"e1\"\nevent_hash = \"{}\"\nfleet_quorum = {quorum}\nfleet_timeout_secs = {timeout}\n",
        hex::encode(coord_key().verifying_key().to_bytes()), mock_event_hash());
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
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"abort": forged,
        "event": signed(serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e1"}))}));
    assert_eq!(run_machine(&cfg, &ops), State::Live, "a forged abort must not stop the ceremony");
}

#[test]
fn rc29_an_empty_relay_answer_is_not_a_positive_no_abort() {
    // A relay that lost its store answers 200 `{}`: before chain creation that is "unknown", never "no abort".
    for doc in [serde_json::json!({}),
                serde_json::json!({"event": signed(serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e0"}))}),
                serde_json::json!({"event": signed(serde_json::json!({"type": "event", "network": "other", "event_id": "e1"}))})] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = coord_config(dir.path(), 120, 0, 60);
        let ops = MockOps::new(dir.path(), 110);
        *ops.coord_doc.borrow_mut() = Some(doc.clone());
        // Sealed: STRANDED here (the guard has no fleet view to prove nobody created the chain), never LIVE.
        assert_eq!(run_machine(&cfg, &ops), State::Stranded, "{doc}");
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        assert!(text.contains("cannot confirm the event is not aborted"), "{doc}");
        assert!(!text.contains("\"create_chain\"") && !text.contains("ignite_started"), "nothing irreversible: {doc}");
    }
}

#[test]
fn rc29_no_abort_needs_the_exact_accepted_event_payload() {
    let dir = tempfile::tempdir().unwrap();
    let base = coord_config(dir.path(), 120, 0, 60);
    let _ = base;
    let text = std::fs::read_to_string(dir.path().join("ceremony-coord.toml")).unwrap();
    let ev = signed(serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e1", "h": 120}));
    let other = signed(serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e1", "h": 121}));
    let hash = pulse_cutover::coord::payload_hash(&ev).unwrap();
    let path = dir.path().join("ceremony-hash.toml");
    std::fs::write(&path, text.replace(&format!("event_hash = \"{}\"\n", mock_event_hash()), &format!("event_hash = \"{hash}\"\n"))).unwrap();
    let cfg = Config::load(&path).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"event": other}));
    assert_eq!(run_machine(&cfg, &ops), State::Stranded, "a re-signed event with another payload is not ours");
    assert!(!std::fs::read_to_string(&cfg.journal_path).unwrap().contains("ignite_started"));

    let dir = tempfile::tempdir().unwrap();
    let _ = coord_config(dir.path(), 120, 0, 60);
    let text = std::fs::read_to_string(dir.path().join("ceremony-coord.toml")).unwrap();
    let path = dir.path().join("ceremony-hash.toml");
    std::fs::write(&path, text.replace(&format!("event_hash = \"{}\"\n", mock_event_hash()), &format!("event_hash = \"{hash}\"\n"))).unwrap();
    let cfg = Config::load(&path).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"event": ev}));
    assert_eq!(run_machine(&cfg, &ops), State::Live);
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
    // rc.23 review #8: without a roster the resume guard cannot judge reachability: STRANDED, not resumed.
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("fleet did not reach verified agreement") && text.contains("no roster or quorum"));
    assert_eq!(ops.resumes.get(), 0);
}


// ---- Independent review fixes (2026-09-29) ---------------------------------------------------------------

#[test]
fn readiness_profile_refuses_to_run_before_any_side_effect() {
    let dir = tempfile::tempdir().unwrap();
    // What beacon-install.sh writes: api mode, no flip/stop_cmd, no H — valid only as readiness.
    let text = format!(r#"
journal_path = "{dir}/journal.jsonl"
[ceremony]
rehearsal = true
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
    // Review 2026-09-30 #5: authority tricks that hand-parsing accepted.
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
rehearsal = true
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
fn rc29_a_resumed_frozen_run_keeps_the_producer_paused_once_h_is_final() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = sched_config(dir.path());
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.schedule_snapshot(120).unwrap();
    let entries = [
        serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "ARMED",
            "data": {"chain_id": hex::encode(CHAIN_ID), "resolved_h": 120}}),
        serde_json::json!({"seq": 1, "ts_ms": 2, "ts": "t", "kind": "evidence", "state": "ARMED",
            "data": {"snapshot_scheduled_at": 120}}),
        serde_json::json!({"seq": 2, "ts_ms": 3, "ts": "t", "kind": "transition", "state": "FROZEN",
            "data": {"chain_id": hex::encode(CHAIN_ID)}}),
        // r30 round 7: the crashed agent's own pause is on its journal (written before the pause RPC); without it the
        // pause would be someone else's and is adopted instead (r30r7_* tests).
        serde_json::json!({"seq": 3, "ts_ms": 4, "ts": "t", "kind": "evidence", "state": "FROZEN",
            "data": {"side_effect": "source_pause"}}),
        // r30 round 10: ...and its confirmed pause (the ack): without it the pause's ownership is unknown and it is
        // adopted (never resumed) instead.
        serde_json::json!({"seq": 4, "ts_ms": 5, "ts": "t", "kind": "evidence", "state": "FROZEN",
            "data": {"source_pause_ack": 1}}),
    ];
    std::fs::write(&cfg.journal_path, entries.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("\n") + "\n").unwrap();
    // The crashed agent had paused the producer after H became final.
    ops.head.set(125);
    ops.paused.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("kept_paused_on_resume") && !text.contains("resumed_production_for_snapshot"), "{text}");
    assert_eq!(ops.resumes.get(), 0, "no old-chain blocks re-produced after H");
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
    // Review 2026-09-30 #1: the lineage check runs AFTER the ignite command; the target may be
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
    let (held, _) = open_journal(&cfg.journal_path);
    // A genuinely separate process (the real binary) must be refused while this one holds it.
    let bin = env!("CARGO_BIN_EXE_pulse-cutover");
    let out = std::process::Command::new(bin).args(["run", "--config"]).arg(dir.path().join("ceremony.toml")).output().unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("already running"), "{stderr}");
    drop(held);
    let _ = open_journal(&cfg.journal_path); // lock released on drop (retrying past a sibling test's fork)
}

#[test]
fn torn_last_line_is_set_aside_but_a_corrupt_middle_line_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.jsonl");
    let good = serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "ARMED",
        "data": {"chain_id": "ab", "resolved_h": 120}}).to_string();
    std::fs::write(&path, format!("{good}\n{{\"seq\":1,\"ts_ms\":2,\"kin")).unwrap();
    let (mut j, rec) = open_journal(&path);
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
    // Review 2026-09-30 #2: a crash after staging but before VERIFIED left the staged file; the
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
    let ops = MockOps::new(dir.path(), rec_full.head_at_pause.unwrap_or(rec_full.cut_height.unwrap()));
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
    // Review 2026-09-30 #4: the shell exits but a background child keeps stdout open.
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
        + "\n[[coordination.roster]]\nproducer = \"bp1\"\ninstance_id = \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n\n[[coordination.roster]]\nproducer = \"bp2\"\n";
    // A beacon as bp1 whose persisted instance is the pinned "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" (rc.25 F6: the guard matches its own report).
    std::fs::write(dir.join("beacon.token"), "tok\n").unwrap();
    std::fs::write(dir.join("beacon.instance"), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n").unwrap();
    let text = text + &format!("\n[beacon]\nurl = \"https://mc.example/api/report\"\nproducer = \"bp1\"\nnetwork = \"rehearsal\"\ntoken_file = \"{}/beacon.token\"\n", dir.display());
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
            {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep(if good { "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } else { "zz" })}]},
            {"name": "bp2", "beacons": [{"age_ms": if good { 1000 } else { 999_000 }, "report": rep("bb")}]},
        ]}]}));
        let st = run_machine(&cfg, &ops);
        (st, std::fs::read_to_string(d.path().join("journal.jsonl")).unwrap())
    };
    let (st, text) = run(false);
    // rc.23: a stale member and a missing pinned instance are unknown states: STRANDED, never a resume.
    assert_eq!(st, State::Stranded, "outsiders + a stale member + a wrong instance must not satisfy the roster");
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
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": {"instance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "coord": {"event_id": "OTHER"},
            "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}}}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": {"instance_id": "bb", "coord": {"event_id": "e1"},
            "ceremony": {"state": "VERIFIED", "evidence": {}}}}]},
    ]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Stranded, "wrong event + empty evidence must not satisfy the gate (rc.23: bp1 has no report for this event)");
}

#[test]
fn event_quorum_is_never_inherited_and_release_pin_needs_a_plugin() {
    use pulse_cutover::coord::{derived_config, validate_event};
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("c.toml");
    std::fs::write(&src, "journal_path = \"/j\"\n[ceremony]\nrehearsal = true\nfreeze_height = 0\n[snapshot]\nstaged_path = \"/s\"\n[coordination]\nurl = \"https://mc\"\nnetwork = \"testnet\"\nfleet_quorum = 1\n").unwrap();
    let ev = serde_json::json!({"type": "event", "event_id": "e9", "h": 900, "roster": [{"producer": "bp1"}, {"producer": "bp2"}, {"producer": "bp3"}]});
    let out = dir.path().join("d.toml");
    derived_config(&src, &ev, None, &out).unwrap();
    let d: toml::Value = toml::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(d["coordination"]["fleet_quorum"].as_integer(), Some(3), "no event quorum = all roster members, not the local 1");

    let cfg = test_config(dir.path(), 120);
    let dup = serde_json::json!({"type": "event", "network": "rehearsal", "h": 100_000, "roster": [{"producer": "bp1"}, {"producer": "bp1"}]});
    assert!(validate_event(&dup, &cfg, "rehearsal", Some(10), 10).unwrap_err().contains("more than once"));
    let pinned = serde_json::json!({"type": "event", "network": "rehearsal", "h": 100_000, "release_sha256": "ab".repeat(32), "roster": [{"producer": "bp1"}]});
    // rc.23 review #8: a producer-mode event without a roster is refused.
    let bare = serde_json::json!({"type": "event", "network": "rehearsal", "h": 100_000});
    assert!(validate_event(&bare, &cfg, "rehearsal", Some(10), 10).unwrap_err().contains("no roster"));
    assert!(validate_event(&pinned, &cfg, "rehearsal", Some(10), 10).unwrap_err().contains("plugin_path"));
}

#[test]
fn beacon_preview_writes_nothing() {
    // Review 2026-09-30 #8: `beacon --once` with no url (installer dry-run) created the journal
    // dir and beacon.instance.
    let dir = tempfile::tempdir().unwrap();
    let jdir = dir.path().join("not-created");
    let text = format!(r#"
journal_path = "{j}/journal.jsonl"
[ceremony]
rehearsal = true
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

// ---- second independent verification (2026-09-30): round 3 regressions ------------------------------------
// Each test below reproduces a failing case from an independent review of rc.6.

fn fail_live_config(dir: &std::path::Path) -> Config {
    let base = std::fs::read_to_string({ test_config(dir, 120); dir.join("ceremony.toml") }).unwrap();
    load_toml(dir, "fail-live.toml", &base.replace("on_live = \"flip-gateway\"", "on_live = \"fail-live\"")).unwrap()
}

#[test]
fn r3_rc6_halt_error_without_its_transition_is_still_halted() {
    // Review #2: rc.6 wrote the HALTED error, then the HALTED transition. A crash between the two
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
    // Review #3: unhalt wrote a VERIFIED transition without `sha256`; the beacon summary then
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
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}]},
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
    let ops2 = MockOps::new(d.path(), rec.head_at_pause.unwrap_or(rec.cut_height.unwrap()));
    ops2.paused.set(true);
    ops2.target_head.set(rec.cut_height.unwrap());
    *ops2.status_doc.borrow_mut() = Some(doc);
    let st = run_machine_result(&cfg, &ops2);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert_eq!(st, Ok(State::Live), "{text}");
}

#[test]
fn r3_roster_with_zero_quorum_is_a_config_error() {
    // Review (#9 table): roster configured, fleet_quorum left at 0 and no event id → the gate was
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
    // Review #5: every roster report carried conflict=true (one token on two machines) and the
    // machine still reached LIVE.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let rep = |id: &str| serde_json::json!({"instance_id": id, "ready": false, "coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "conflict": true, "report": rep("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "conflict": true, "report": rep("bb")}]}]}]}));
    // rc.23: and with conflicted members the fleet's state is unknown: STRANDED, the source is not resumed.
    assert_eq!(run_machine(&cfg, &ops), State::Stranded, "conflicted evidence must not satisfy the roster");
    assert_eq!(ops.resumes.get(), 0);

    // Legacy (no roster) gate: the same exclusion, and reports must be FRESH too.
    let d2 = tempfile::tempdir().unwrap();
    let cfg2 = coord_config(d2.path(), 120, 2, 5);
    let ops2 = MockOps::new(d2.path(), 110);
    *ops2.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "age_ms": 999_000, "report": rep("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "conflict": true, "report": rep("bb")}]}]}]}));
    assert_eq!(run_machine(&cfg2, &ops2), State::Stranded, "a stale legacy report and a conflicted one are not a quorum of 2 (and not proof)");
}

#[test]
fn r3_completed_corrupt_record_before_a_cr_only_tail_is_fatal() {
    // Review #7: `valid\ncorrupt\n\r` — the tail after the last LF is whitespace, so the corrupt
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
    let (_j, rec) = open_journal(&torn);
    assert!(rec.torn_tail);
    assert_eq!(rec.state, Some(State::Armed));
}

fn run_bin(args: &[&str], cfg: &std::path::Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover")).args(&args[..1]).arg("--config").arg(cfg).args(&args[1..]).output().unwrap()
}

#[test]
fn r3_rollback_refuses_without_affirmative_evidence() {
    // Review #1: `cutover.sh abort` treated a MISSING journal as safe and resumed the source.
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
    let held = open_journal(&cfg.journal_path);
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
    let (journal, rec) = open_journal(&cfg.journal_path);
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
    let ops = MockOps::new(dir.path(), rec_full.head_at_pause.unwrap_or(rec_full.cut_height.unwrap()));
    ops.paused.set(true);
    ops.target_head.set(rec_full.cut_height.unwrap());
    let st = run_machine_result(&cfg, &ops);
    assert_eq!(st, Ok(State::Live), "{}", std::fs::read_to_string(&cfg.journal_path).unwrap());
    assert_eq!(std::fs::read(&cfg.snapshot.staged_path).unwrap(), full, "the partial copy was replaced by a full restage");
}


// ---------------------------------------------------------------------------------------------
// Round 4 (external review of rc.7): rollback must never claim success it did not achieve.
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
    // Review §2.1: `rollback` printed "rolled back" and exited 0 when the producer resume FAILED.
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
    // Review §2.7: repeat rollbacks re-resumed and re-ran on_abort.
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
    // Review §2.2: --force-after-ignite resumed the source while this box's target kept running.
    let dir = tempfile::tempdir().unwrap();
    let base = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&base, &ops).unwrap_err().starts_with("HALTED"));
    // Fence that fails: the source must NOT be resumed and the journal stays HALTED.
    set_target_line(dir.path(), "stop_cmd = \"fail-stop-target\"");
    let cfg = Config::load(&dir.path().join("ceremony.toml")).unwrap();
    let ops = MockOps::new(dir.path(), 200);
    let (j, rec) = open_journal(&cfg.journal_path);
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
    let (j, rec) = open_journal(&cfg.journal_path);
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
    // Review §2.3: lock timeout and corrupt journal exited 1, so cutover.sh skipped its guidance.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    let cfgp = dir.path().join("ceremony.toml");
    let held = open_journal(&cfg.journal_path);
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
    // Review §2.4: the staged snapshot stayed; the next preflight refused and a metalgo restart
    // would have imported the abandoned cut.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    assert!(cfg.snapshot.staged_path.exists(), "fixture: staged file present");
    let ops = MockOps::new(dir.path(), 200);
    let (j, rec) = open_journal(&cfg.journal_path);
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
    // Review §2.5: --no-journal-i-know created a terminal ABORTED journal, so the next `run` on
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
    // Review §2.6: an on_freeze left running by a killed agent could close writes again after
    // on_abort reopened them.
    let dir = tempfile::tempdir().unwrap();
    let _ = pre_ignite_journal(dir.path());
    let t = std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap()
        .replace("on_live = \"flip-gateway\"", "on_live = \"flip-gateway\"\non_abort = \"reopen-writes\"");
    std::fs::write(dir.path().join("ceremony.toml"), t).unwrap();
    let cfg = Config::load(&dir.path().join("ceremony.toml")).unwrap();
    let ops = MockOps::new(dir.path(), 200);
    *ops.orphan.borrow_mut() = Some(Ok(Some("killed orphaned hook process group 4242 (SIGTERM): `freeze-writes`".into())));
    let (j, rec) = open_journal(&cfg.journal_path);
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
    let (j, rec) = open_journal(&cfg2.journal_path);
    // Round 5 (Review N5): nothing changed, so this is a refusal (exit 3), not an incomplete rollback.
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
fn rc25_a_roster_without_this_nodes_beacon_is_warned_about() {
    // F6: the resume guard needs this node's own report on the relay; without a beacon (or under a name outside
    // the roster) every guarded abort strands. The config says so up front.
    let d = tempfile::tempdir().unwrap();
    let base = std::fs::read_to_string({ coord_config(d.path(), 120, 0, 5); d.path().join("ceremony-coord.toml") }).unwrap();
    let text = base.replace("fleet_quorum = 0", "fleet_quorum = 2") + "\n[[coordination.roster]]\nproducer = \"bp1\"\n\n[[coordination.roster]]\nproducer = \"bp2\"\n";
    let no_beacon = load_toml(d.path(), "nb.toml", &text).unwrap();
    assert!(no_beacon.warnings().iter().any(|w| w.contains("roster but no [beacon]")), "{:?}", no_beacon.warnings());
    std::fs::write(d.path().join("beacon.token"), "tok\n").unwrap();
    let beacon = |p: &str| format!("\n[beacon]\nurl = \"https://mc.example/api/report\"\nproducer = \"{p}\"\nnetwork = \"rehearsal\"\ntoken_file = \"{}/beacon.token\"\n", d.path().display());
    let outsider = load_toml(d.path(), "out.toml", &(text.clone() + &beacon("bp9"))).unwrap();
    assert!(outsider.warnings().iter().any(|w| w.contains("bp9 is not in the event roster")));
    let member = load_toml(d.path(), "in.toml", &(text + &beacon("bp1"))).unwrap();
    assert!(!member.warnings().iter().any(|w| w.contains("[beacon]")), "{:?}", member.warnings());
}

#[test]
fn r4_fleet_gate_ignores_reports_with_failing_health_but_not_setup_checks() {
    // Review #5 residual: the gate counted reports whose health checks were failing.
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
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", sick.clone())}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": rep("bb", setup_only.clone())}]}]}]}));
    // The gate fails (bp1, this node, excluded: 1 of 2). rc.25 F6: the node now recognizes its own report (the
    // fixture has a real beacon), so the guard sees bp2 alone still in the ceremony, below quorum 2: bp2 cannot
    // create the chain without this node, and the abort resumes the old chain.
    assert_eq!(run_machine(&cfg, &ops), State::Aborted, "bp1's failing health check excludes it: 1 of 2");
    let d2 = tempfile::tempdir().unwrap();
    let cfg2 = roster_config(d2.path());
    let ops2 = MockOps::new(d2.path(), 110);
    *ops2.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", setup_only.clone())}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": rep("bb", setup_only)}]}]}]}));
    assert_eq!(run_machine(&cfg2, &ops2), State::Live, "failing SETUP checks (hooks) do not exclude a report");
}

#[test]
fn r4_await_never_launches_the_ceremony_for_an_arm_with_the_wrong_event_hash() {
    // Review #4 residual: the arm-hash check was only unit-tested through check_arm.
    fn run_await_against(dir: &std::path::Path, wrong_hash: bool) -> String {
        let _ = coord_config(dir, 120, 0, 60);
        let ev = signed(serde_json::json!({"type": "event", "event_id": "e1", "network": "rehearsal", "h": 5000u64, "roster": [{"producer": "bp1"}]}));
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
fn stage2_await_never_accepts_an_event_the_coordinator_aborted() {
    // Stage-2 rig, 2026-10-01: after a signed ABORT the await loop dropped the event, re-accepted it on
    // the next poll, dropped it again, … (accepted/aborted flip every 3 s), and a fresh `await` started
    // after the abort accepted the aborted event. A signed abort is final for its event id.
    let dir = tempfile::tempdir().unwrap();
    let _ = coord_config(dir.path(), 120, 0, 60);
    let ev = signed(serde_json::json!({"type": "event", "event_id": "e1", "network": "rehearsal", "h": 5000u64}));
    let hash = pulse_cutover::coord::payload_hash(&ev).unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    let arm = signed(serde_json::json!({"type": "arm", "event_id": "e1", "network": "rehearsal", "event_hash": hash, "issued_at_ms": now}));
    let abort = signed(serde_json::json!({"type": "abort", "event_id": "e1", "network": "rehearsal", "event_hash": hash, "issued_at_ms": now}));
    let doc = serde_json::json!({"event": ev, "arm": arm, "abort": abort}).to_string();
    let (url, _hits) = stub_http(move |_m, p| {
        if p.starts_with("/api/coord/") { (200, doc.clone()) } else { (200, r#"{"head_block_num": 100}"#.into()) }
    });
    let t = std::fs::read_to_string(dir.path().join("ceremony-coord.toml")).unwrap()
        .replace("url = \"http://mc\"", &format!("url = \"{url}\"\nauto_arm = true"))
        .replace("rpc_url = \"http://mock\"\nproducer_api_url", &format!("rpc_url = \"{url}\"\nproducer_api_url"));
    std::fs::write(dir.path().join("ceremony-coord.toml"), t).unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
        .args(["await", "--config"]).arg(dir.path().join("ceremony-coord.toml"))
        .stderr(std::process::Stdio::piped()).stdout(std::process::Stdio::null()).spawn().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(8));
    let _ = child.kill();
    let err = String::from_utf8_lossy(&child.wait_with_output().unwrap().stderr).to_string();
    assert!(err.contains("event e1 is aborted by the coordinator"), "{err}");
    assert!(!err.contains("accepted event e1"), "an aborted event must never be accepted: {err}");
    assert!(!err.contains("ARMED by signed"), "{err}");
    assert_eq!(err.matches("aborted by the coordinator").count(), 1, "no accepted/aborted oscillation: {err}");
    assert!(!dir.path().join("ceremony-e1.toml").exists(), "no derived ceremony config was written");
    // The beacon reports the abort (written once by this fresh await, never "accepted").
    let st: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.path().join("coord-state.json")).unwrap()).unwrap();
    assert_eq!((st["event_id"].as_str(), st["aborted"].as_bool(), st["accepted"].as_bool()), (Some("e1"), Some(true), Some(false)), "{st}");
}

#[test]
fn r4_beacon_summary_marks_a_forced_rollback_after_ignition() {
    // Review §2.9: after a forced rollback the beacon judged the box as pre-ceremony.
    let dir = tempfile::tempdir().unwrap();
    let base = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&base, &ops).unwrap_err().starts_with("HALTED"));
    let ops = MockOps::new(dir.path(), 200);
    let (j, rec) = open_journal(&base.journal_path);
    Machine::new(&base, &ops, j, rec).operator_rollback(true).unwrap();
    let s = pulse_cutover::beacon::journal_summary(&base.journal_path);
    assert_eq!(s["state"], "ABORTED");
    assert_eq!(s["ignition_started"], true);
    assert_eq!(s["forced_rollback"], true);
}


// ---------------------------------------------------------------------------------------------
// Round 5 (external re-check of rc.7, N1–N10): rollback completion, orphans on every path.
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
    let (j, rec) = open_journal(&cfg.journal_path);
    Machine::new(cfg, ops, j, rec).operator_rollback(force)
}

#[test]
fn r5_rollback_killed_inside_on_abort_reruns_only_the_missing_steps() {
    // Review N1 (P9): rollback_complete was journaled BEFORE on_abort; a rollback killed inside
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
    // Review N2 (P4, P8d): the "already rolled back" no-op returned before the orphan kill and the
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
    // Review N2 (P4): the agent's own abort (here: fleet gate short of quorum, after staging)
    // left staged.bin; the next preflight refused and a metalgo restart would import it.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    let rep = serde_json::json!({"instance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "checks": [], "coord": {"event_id": "e1"},
        "ceremony": {"state": "VERIFIED", "evidence": ours}});
    // rc.23: bp2 already aborted, so the quorum (2) is unreachable without this node: the guard passes, a real abort.
    let behind = serde_json::json!({"instance_id": "bb", "checks": [], "coord": {"event_id": "e1"}, "ceremony": {"state": "ABORTED"}});
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": behind}]}]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert!(!cfg.snapshot.staged_path.exists(), "the agent's own abort unstages the snapshot it staged");
    let aside = std::fs::read_dir(d.path()).unwrap().filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("staged.bin.rolled-back-"));
    assert!(aside, "moved aside, not deleted");
}

#[test]
fn r5_orphan_that_cannot_be_stopped_is_a_refusal() {
    // Review N5: an orphan that could not be killed exited 4 ("incomplete") although nothing changed.
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
    // Review N3 (P10): a resumed `run` neither killed the orphan left by a crashed agent nor kept
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
    // Review N4: run_long (upstream export/import) ran untracked, outside its own process group:
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
    // Review N10: the guard accepted any `sh -c …` leader, so a compound hook's record could kill an
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
    // Review N7: --no-journal-i-know left <journal>.rollback-<ms>.jsonl.lock behind.
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
    // Review N9: excluded reports left no per-producer reason in the journal.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    let rep = |id: &str, checks: serde_json::Value| serde_json::json!({"instance_id": id, "checks": checks,
        "coord": {"event_id": "e1"}, "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", serde_json::json!([{"name": "disk_free", "ok": false, "detail": "2 GB free"}]))}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": rep("bb", serde_json::json!([]))}]}]}]}));
    // rc.25 F6: this node knows its own report, so bp2 alone (quorum 2) cannot commit without it: a real abort.
    assert_eq!(run_machine(&cfg, &ops), State::Aborted, "bp2 alone is below quorum 2");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("fleet_gate_excluded") && text.contains("disk_free"), "per-report reason journaled: {text}");
}

#[test]
fn r5_setup_check_names_are_shared_with_mission_control() {
    // Review N9: the agent's health/setup split must be the dashboard's, from one shared list.
    let v: serde_json::Value = serde_json::from_str(include_str!("../control/check-kinds.json")).unwrap();
    let mut shared: Vec<String> = v["setup"].as_array().unwrap().iter().map(|x| x.as_str().unwrap().to_string()).collect();
    let mut ours: Vec<String> = pulse_cutover::beacon::SETUP_CHECKS.iter().map(|s| s.to_string()).collect();
    shared.sort();
    ours.sort();
    assert_eq!(ours, shared);
}

#[test]
fn r5b_run_refuses_a_journal_whose_operator_rollback_died_before_aborted() {
    // A `rollback` killed after recording its intent but before its ABORTED transition left a
    // pre-ignition journal: a later `run` resumed the ceremony the operator had asked to abandon.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    {
        let (mut j, rec) = open_journal(&cfg.journal_path);
        let st = rec.state.unwrap();
        j.evidence(st, serde_json::json!({"rollback_requested": true, "force_after_ignite": false})).unwrap();
    }
    let ops = MockOps::new(dir.path(), 200);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("rollback") && err.contains("did not finish"), "{err}");
    assert!(ops.events.borrow().iter().all(|e| e == "kill-orphans"), "run did nothing: {:?}", ops.events.borrow());
    // Re-running rollback finishes it; after that the journal is a normal ABORTED.
    let out = rollback_with(&cfg, &MockOps::new(dir.path(), 200), false).unwrap();
    assert!(out.failed.is_empty() && !out.already, "{out:?}");
    let (_, rec) = open_journal(&cfg.journal_path);
    assert!(!rec.rollback_pending && rec.state == Some(State::Aborted), "{rec:?}");
}

#[test]
fn r5b_operator_rollback_records_its_intent_before_any_step() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    assert!(rollback_with(&cfg, &MockOps::new(dir.path(), 200), false).unwrap().failed.is_empty());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let intent = text.find("rollback_requested").expect("intent recorded");
    let first_step = text.find("rollback_step").expect("a step recorded");
    assert!(intent < first_step, "intent must precede every step");
}


#[test]
fn r6_steady_stale_report_is_journaled_once_and_non_roster_producers_are_not_named() {
    // Final review N-1: the exclusion reason embedded the relay's changing age_ms, so a steadily
    // stale report wrote a fleet_gate line on EVERY 2 s poll, and non-roster producers were named.
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let d = tempfile::tempdir().unwrap();
    let cfg = roster_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    ops.age_stale_per_poll.set(true);
    let rep = |id: &str| serde_json::json!({"instance_id": id, "checks": [], "coord": {"event_id": "e1"},
        "ceremony": {"state": "VERIFIED", "evidence": ours.clone()}});
    *ops.status_doc.borrow_mut() = Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}]},
        {"name": "bp2", "beacons": [{"age_ms": 100000, "report": rep("bb")}]},
        {"name": "outsider", "beacons": [{"age_ms": 100000, "report": rep("zz")}]}]}]}));
    assert_eq!(run_machine(&cfg, &ops), State::Stranded, "quorum 2 never reached; bp2 stale = unknown (rc.23)");
    assert!(ops.fleet_polls.get() >= 50, "fixture: many polls ({})", ops.fleet_polls.get());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let lines = text.lines().filter(|l| l.contains("\"fleet_gate\"")).count();
    assert_eq!(lines, 1, "a constant situation is journaled once, not per poll");
    assert!(!text.contains("outsider"), "non-roster producers are not named");
    assert!(text.contains("stale report (older than 60 s)"), "{text}");
}

#[test]
fn r6_forced_rollback_fences_on_every_attempt() {
    // Final review N-2 (P7e): after a forced attempt whose fence succeeded and whose resume failed, the
    // re-run skipped the fence and resumed on the strength of the earlier "target stopped" proof.
    let dir = tempfile::tempdir().unwrap();
    let base = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.target_fork.set(true);
    assert!(run_machine_result(&base, &ops).unwrap_err().starts_with("HALTED"));
    set_target_line(dir.path(), "stop_cmd = \"stop-target\"");
    let cfg = Config::load(&dir.path().join("ceremony.toml")).unwrap();
    let ops = MockOps::new(dir.path(), 200);
    ops.resume_fails.set(true);
    ops.paused.set(true); // round 11: the failed resume is settled by a read; the producer is still paused
    let out = rollback_with(&cfg, &ops, true).unwrap();
    assert!(!out.failed.is_empty(), "fixture: the resume failed: {out:?}");
    assert!(ops.events.borrow().iter().any(|e| e == "hook:stop-target"));
    let ops2 = MockOps::new(dir.path(), 200);
    let out = rollback_with(&cfg, &ops2, true).unwrap();
    assert!(out.failed.is_empty(), "{out:?}");
    let ev = ops2.events.borrow().clone();
    let fence = ev.iter().position(|e| e == "hook:stop-target").expect("fence re-run in THIS attempt");
    let resume = ev.iter().position(|e| e == "resume").expect("resume");
    assert!(fence < resume, "fence proven in this attempt before the resume: {ev:?}");
}

#[test]
fn r6_status_shows_a_pending_rollback_and_cancel_intent_works_only_before_any_step() {
    // Final review N-3: a recorded intent was invisible in `status`/the beacon and could not be withdrawn.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    {
        let (mut j, rec) = open_journal(&cfg.journal_path);
        j.evidence(rec.state.unwrap(), serde_json::json!({"rollback_requested": true, "force_after_ignite": false})).unwrap();
    }
    let toml = dir.path().join("ceremony.toml");
    let st = String::from_utf8_lossy(&run_bin(&["status"], &toml).stdout).to_string();
    assert!(st.contains("rollback_pending: yes") && st.contains("rollback: pending"), "{st}");
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["rollback_pending"], serde_json::json!(true));
    // Without --i-understand: refused, nothing changes.
    let out = run_bin(&["rollback", "--cancel-intent"], &toml);
    assert_eq!(out.status.code(), Some(3), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(Journal::replay(&cfg.journal_path).unwrap().rollback_pending);
    // No step completed yet: cancellation is journaled and clears it.
    let out = run_bin(&["rollback", "--cancel-intent", "--i-understand"], &toml);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(!Journal::replay(&cfg.journal_path).unwrap().rollback_pending);
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("rollback_intent_cancelled"));
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["rollback_pending"], serde_json::json!(false));
    // Once a step has completed, the rollback can only be finished, not cancelled.
    {
        let (mut j, rec) = open_journal(&cfg.journal_path);
        let s = rec.state.unwrap();
        j.evidence(s, serde_json::json!({"rollback_requested": true})).unwrap();
        j.evidence(s, serde_json::json!({"rollback_step": "resume", "ok": true})).unwrap();
    }
    let out = run_bin(&["rollback", "--cancel-intent", "--i-understand"], &toml);
    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot be cancelled"));
    assert!(Journal::replay(&cfg.journal_path).unwrap().rollback_pending);
}

#[test]
fn r6_run_on_an_aborted_journal_with_an_unfinished_rollback_says_so_and_exits_4() {
    // Final review N-4: `run` said "it stopped safely and rolled back" for an ABORTED whose rollback
    // never finished (writes still closed).
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    let ops = MockOps::new(dir.path(), 200);
    ops.resume_fails.set(true);
    ops.paused.set(true); // round 11: the failed resume is settled by a read; the producer is still paused
    assert!(!rollback_with(&cfg, &ops, false).unwrap().failed.is_empty(), "fixture: incomplete rollback");
    let out = run_bin(&["run"], &dir.path().join("ceremony.toml"));
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert_eq!(out.status.code(), Some(4), "{err}");
    assert!(err.contains("rollback INCOMPLETE") && !err.contains("stopped safely and rolled back"), "{err}");
}

#[test]
fn r6_pre_rc8_aborted_reads_as_unknown_not_incomplete() {
    // Final review N-8: every rc.7-written ABORTED (no step records) read as an incomplete rollback.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("journal.jsonl");
    {
        let (mut j, _) = open_journal(&p);
        j.transition(State::Armed, serde_json::json!({})).unwrap();
        j.transition(State::Aborted, serde_json::json!({"reason": "rc.7 style", "rollback_complete": true})).unwrap();
    }
    assert_eq!(pulse_cutover::beacon::journal_summary(&p)["rollback_complete"], serde_json::Value::Null);
    let d2 = tempfile::tempdir().unwrap();
    let p2 = d2.path().join("journal.jsonl");
    {
        let (mut j, _) = open_journal(&p2);
        j.transition(State::Armed, serde_json::json!({})).unwrap();
        j.transition(State::Aborted, serde_json::json!({"reason": "rc.8", "reverts_ok": true})).unwrap();
    }
    assert_eq!(pulse_cutover::beacon::journal_summary(&p2)["rollback_complete"], serde_json::json!(false));
}

#[cfg(unix)]
#[test]
fn r6_legacy_record_of_an_execd_script_hook_is_recognised() {
    // Final review N-6: an rc.7 record (no start time) of a script hook, which `sh -c` execs in place
    // so the leader shows `<interpreter> <script>`, was refused as "pid reused?" and left running.
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("myhook.sh");
    std::fs::write(&script, "#!/bin/sh\nsleep 30\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (pid, exited) = spawn_group(&script.display().to_string());
    std::thread::sleep(std::time::Duration::from_millis(300));
    let pg = dir.path().join("journal.jsonl.hook.pgid");
    std::fs::write(&pg, format!("{pid}\n{}\n", script.display())).unwrap();
    let res = pulse_cutover::ops::kill_recorded_hook_group(&pg, std::time::Duration::from_secs(2));
    assert!(res.as_ref().map(|r| r.as_deref().unwrap_or("").contains("killed")).unwrap_or(false), "{res:?}");
    assert!(wait_exited(&exited, pid));
}

#[test]
fn await_config_without_h_loads_but_run_refuses_it() {
    // A production await config: freeze_height = 0, no rehearsal derive flags, a [coordination] section.
    let dir = tempfile::tempdir().unwrap();
    test_config(dir.path(), 120);
    let base = std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap();
    let text = base.replace("freeze_height = 120", "freeze_height = 0")
        + "\n[coordination]\nurl = \"https://mc.example\"\nnetwork = \"testnet\"\ncoordinator_keys = [\"00\"]\n";
    let cfg = load_toml(dir.path(), "await.toml", &text).expect("an await config needs no H");
    assert!(cfg.ensure_h_known().unwrap_err().contains("await"));
    // Without [coordination] the old rule stands: H is required.
    let plain = base.replace("freeze_height = 120", "freeze_height = 0");
    assert!(load_toml(dir.path(), "plain.toml", &plain).unwrap_err().contains("freeze_height is 0"));
}

// ---------------------------------------------------------------------------------------------
// Upstream ignition from the #61 checkpoint (PulseVM v1.0.0 boot: migration genesis + chain
// config + boot manifest anchored on the FULL source cut block; chain created on Metal by a hook).
// ---------------------------------------------------------------------------------------------

const BLOCKCHAIN_ID: &str = "2YXVy2NWHZJphNuvWry8So8TpQodhSb7ZVJPxhwhtKS6Z6JjpV";
const TARGET_CHAIN_ID: &str = "cb4786adfd7ddff2c1ae87db4e406a13d208fd3015e56ccddaf105ac9ba85c01";
const MAINNET: &str = "384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0";

/// Make the fake compare fail the way v1.0.0 does: real-format lines, the named tables failing.
fn fake_compare_failing(dir: &std::path::Path, failing: &[&str]) {
    let mut body = String::from("#!/bin/sh\n");
    for t in pulse_cutover::upstream::REQUIRED_COMPARE_TABLES {
        if !failing.contains(t) {
            body.push_str(&format!("echo 'table {t}: rows=1 sha256=aa55'\n"));
        }
    }
    for t in failing {
        // The v1.0.0 signatures (tests/fixtures/compare-v1.0.0-408461570.log).
        if *t == "global_property" {
            body.push_str(&format!("echo 'table {t}: nodeos=Some(TableReport {{ rows: 1, sha256: \"aa\" }}) arena=Some(TableReport {{ rows: 1, sha256: \"bb\" }})'\n"));
            body.push_str(&format!("echo 'table {t}: first differing row index 0'\necho '  nodeos present=true bytes=157 hex=01'\necho '  arena present=true bytes=143 hex=01'\n"));
        } else {
            body.push_str(&format!("echo 'table {t}: nodeos=Some(TableReport {{ rows: 1, sha256: \"aa\" }}) arena=None'\n"));
            body.push_str(&format!("echo 'table {t}: first differing row index 0'\n"));
        }
    }
    body.push_str("echo '21-table nodeos/Arena comparison FAILED' >&2\nexit 1\n");
    write_script(&dir.join("fake-compare.sh"), &body);
}

/// upstream_test_config + ignition: genesis/chain-config bases, create_chain hook, chain config
/// dir, traffic hooks. `extra_ceremony` / `extra_upstream` are appended to those sections.
fn upstream_ignite_config(dir: &std::path::Path, extra_ceremony: &str, extra_upstream: &str) -> Result<Config, String> {
    std::fs::write(dir.join("genesis-base.json"), serde_json::json!({"initial_timestamp": "2020-04-22T17:00:00",
        "initial_key": pulse_cutover::keys::format_public_k1(&test_producer_pub()),
        "initial_configuration": {"max_block_cpu_usage": 200000}}).to_string()).unwrap();
    std::fs::write(dir.join("chain-config-base.json"), serde_json::json!({"system_account": "eosio", "native_system_contract": false,
        "producer_name": "bp1", "producer_key": pulse_cutover::keys::format_private_k1(&TEST_PRODUCER_SECRET)}).to_string()).unwrap();
    let toml_text = format!(
        r#"
journal_path = "{d}/journal.jsonl"
poll_ms = 1

[ceremony]
rehearsal = true
freeze_height = 120
quiescence_polls = 3
import_backend = "upstream"
lock_dir = "{d}/locks"
{extra_ceremony}

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"

[snapshot]
staged_path = "{d}/staged.bin"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://127.0.0.1:9650/ext/bc/{{blockchain_id}}/rpc"
quorum_timeout_secs = 60
create_chain_cmd = "sh {d}/fake-create-chain.sh {{genesis}} {{chain_config}} {{manifest}}"
chain_config_dir = "{d}/chain-configs"

[upstream]
work_dir = "{d}/upstream-work"
export_cmd = "sh {d}/fake-export.sh {{snapshot}} {{export_dir}}"
import_bin = "{d}/fake-import.sh"
fingerprint_bin = "{d}/fake-fingerprint.sh"
compare_bin = "{d}/fake-compare.sh"
genesis_base = "{d}/genesis-base.json"
chain_config_base = "{d}/chain-config-base.json"
{extra_upstream}

[hooks]
on_freeze = "freeze-writes"
post_ignite = "resume-traffic"
on_live = "flip-gateway"
on_abort = "reopen-writes"
"#,
        d = dir.display(),
    );
    load_toml(dir, "ceremony-upstream-ignite.toml", &toml_text)
}

fn upstream_ops(dir: &std::path::Path) -> MockOps {
    let ops = MockOps::new(dir, 110);
    ops.real_blocks.set(true);
    ops
}

fn journal_entries(cfg: &Config) -> Vec<serde_json::Value> {
    std::fs::read_to_string(&cfg.journal_path).unwrap().lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn transition<'a>(entries: &'a [serde_json::Value], state: &str) -> Option<&'a serde_json::Value> {
    entries.iter().find(|v| v["kind"] == "transition" && v["state"] == state)
}

/// rc.27: the new chain's protocol upgrade schedule. Validated and kept clear of the cut at ARM, hashed into the VERIFIED
/// evidence the fleet compares, installed as upgrade.json next to the chain config, and checked against what the running
/// target reports; a target with another schedule (or one too old for it) halts before IGNITED.
#[test]
fn rc27_protocol_upgrade_schedule_is_validated_installed_and_checked_on_the_target() {
    const FAR: &str = r#"{"protocol_upgrades":[{"protocol_version":2,"activation_height":1000000}]}"#;
    let sched = pulse_cutover::upstream::ProtocolSchedule::parse(FAR.as_bytes()).unwrap();
    let setup = |json: &str| {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        std::fs::write(dir.path().join("upgrade.json"), json).unwrap();
        let cfg = upstream_ignite_config(dir.path(), "", &format!("protocol_upgrades_file = \"{}/upgrade.json\"", dir.path().display())).unwrap();
        (dir, cfg)
    };
    // Matching target: LIVE, schedule installed, hash in VERIFIED evidence, target check verified.
    let (dir, cfg) = setup(FAR);
    let ops = upstream_ops(dir.path());
    *ops.target_protocol.borrow_mut() = Some(serde_json::json!({"protocol_version": 1, "supported_protocol_version": 2,
        "protocol_upgrade_schedule_hash": sched.hash, "next_protocol_upgrade": {"protocol_version": 2, "activation_height": 1000000}}));
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    assert_eq!(transition(&e, "VERIFIED").unwrap()["data"]["protocol_schedule_hash"], serde_json::json!(sched.hash));
    assert_eq!(transition(&e, "IGNITED").unwrap()["data"]["protocol_check"]["verified"], serde_json::json!(true));
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"]["protocol_schedule_hash"], serde_json::json!(sched.hash));
    let installed = std::fs::read_dir(dir.path().join("chain-configs")).unwrap().filter_map(|d| d.ok())
        .map(|d| d.path().join("upgrade.json")).find(|p| p.exists()).expect("upgrade.json installed next to config.json");
    assert_eq!(pulse_cutover::upstream::ProtocolSchedule::load(Some(&installed)).unwrap(), sched);

    // A target that loaded another schedule: halted after ignition started, never LIVE.
    let (dir, cfg) = setup(FAR);
    let ops = upstream_ops(dir.path());
    *ops.target_protocol.borrow_mut() = Some(serde_json::json!({"protocol_version": 1, "supported_protocol_version": 2,
        "protocol_upgrade_schedule_hash": pulse_cutover::upstream::ProtocolSchedule::empty().hash, "next_protocol_upgrade": null}));
    assert!(run_machine_result(&cfg, &ops).unwrap_err().contains("protocol upgrade schedule or supported version does not match"));
    assert!(transition(&journal_entries(&cfg), "IGNITED").is_none() && transition(&journal_entries(&cfg), "HALTED").is_some());

    // A target binary too old for the scheduled version: halted too.
    let (dir, cfg) = setup(FAR);
    let ops = upstream_ops(dir.path());
    *ops.target_protocol.borrow_mut() = Some(serde_json::json!({"protocol_version": 1, "supported_protocol_version": 1,
        "protocol_upgrade_schedule_hash": sched.hash, "next_protocol_upgrade": {"protocol_version": 2, "activation_height": 1000000}}));
    assert!(run_machine_result(&cfg, &ops).unwrap_err().contains("does not match"));
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("cannot execute the scheduled protocol version"));

    // 2nd review: the schedule is what VERIFIED journaled. Emptying the file after install neither lowers the required
    // version (a target supporting only 1 still halts) nor enables the empty-schedule exception for missing fields.
    for (supported, report) in [(1, true), (2, false)] {
        let (dir, cfg) = setup(FAR);
        let ops = upstream_ops(dir.path());
        *ops.target_protocol_edit.borrow_mut() = Some((dir.path().join("upgrade.json"), "{}".into()));
        if report {
            *ops.target_protocol.borrow_mut() = Some(serde_json::json!({"protocol_version": 1, "supported_protocol_version": supported,
                "protocol_upgrade_schedule_hash": sched.hash, "next_protocol_upgrade": {"protocol_version": 2, "activation_height": 1000000}}));
        }
        assert!(run_machine_result(&cfg, &ops).is_err(), "supported {supported}, fields reported {report}: must halt");
        assert!(transition(&journal_entries(&cfg), "IGNITED").is_none());
    }

    // Review HIGH-1: a target that does not report protocol fields (an older PulseVM) cannot run a scheduled v2: halt.
    let (dir, cfg) = setup(FAR);
    let ops = upstream_ops(dir.path());
    assert!(run_machine_result(&cfg, &ops).unwrap_err().contains("does not match"));
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("predates protocol schedules"));

    // An activation too close to the cut (H = 120, default margin 100,000): refused before anything freezes.
    let (dir, cfg) = setup(r#"{"protocol_upgrades":[{"protocol_version":2,"activation_height":5000}]}"#);
    let ops = upstream_ops(dir.path());
    let st = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("protocol upgrade schedule") && text.contains("within 100000 blocks"), "{st:?} {text}");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"), "refused before the freeze");
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_none());

    // An invalid schedule (typo) is refused the same way.
    let (dir, cfg) = setup(r#"{"protocol_upgrades":[{"protocol_version":2,"activation_heigth":1000000}]}"#);
    let ops = upstream_ops(dir.path());
    let _ = run_machine_result(&cfg, &ops);
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("unknown field"));
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_none());
}

/// rc.27 review (review #3): a VERIFIED journal written before rc.27 has no protocol schedule hash; a plain join must
/// not treat that as agreement with the LIVE quorum's schedule.
#[test]
fn rc27_join_refuses_a_legacy_verified_journal_without_a_schedule_hash() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    let good = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    assert!(good["protocol_schedule_hash"].is_string());
    // Rewrite the journal as an rc.26 agent would have written it: same records, no protocol_schedule_hash.
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let legacy: Vec<String> = text.lines().map(|l| {
        let mut v: serde_json::Value = serde_json::from_str(l).unwrap();
        if let Some(o) = v["data"].as_object_mut() { o.remove("protocol_schedule_hash"); }
        serde_json::to_string(&v).unwrap()
    }).collect();
    std::fs::write(&cfg.journal_path, legacy.join("\n") + "\n").unwrap();
    assert!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"]["protocol_schedule_hash"].is_null());
    ops.coord_down.set(false);
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&good, &["bp2", "bp3", "bp4", "bp5"]));
    let (j, rec) = open_journal(&cfg.journal_path);
    let e = Machine::new(&cfg, &ops, j, rec).join_with("e1", false).unwrap_err();
    assert!(e.contains("before rc.27") && e.contains("--reverify"), "{e}");
    // --reverify records the schedule hash (after the quorum matched) and joins.
    let (j, rec) = open_journal(&cfg.journal_path);
    assert_eq!(Machine::new(&cfg, &ops, j, rec).join_with("e1", true).unwrap(), State::Live);
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"]["protocol_schedule_hash"], good["protocol_schedule_hash"]);
}

/// rc.27 2nd review: a JSON-RPC error from getInfo is a failed read (retried), not "a binary without protocol fields";
/// a 64-hex hash is required to count as reported.
#[test]
fn rc27_target_protocol_read_distinguishes_rpc_errors_from_missing_fields() {
    let n = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let n2 = n.clone();
    let (url, _) = stub_http(move |_, _| {
        let i = n2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match i {
            0 => (200, r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"temporary backend unavailable"}}"#.into()),
            1 => (200, r#"{"jsonrpc":"2.0","id":1,"result":{"head_block_num":5,"protocol_upgrade_schedule_hash":""}}"#.into()),
            _ => (200, format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"protocol_version":1,"supported_protocol_version":2,"protocol_upgrade_schedule_hash":"{}","next_protocol_upgrade":null}}}}"#, "AB".repeat(32))),
        }
    });
    let ops = pulse_cutover::ops::HttpOps::new("http://127.0.0.1:1", "http://127.0.0.1:1", &url, "true", 5);
    assert!(ops.target_protocol().unwrap_err().contains("temporary backend unavailable"), "an RPC error is an Err (retried)");
    assert_eq!(ops.target_protocol().unwrap(), None, "a non-hash sentinel is 'not reported'");
    let v = ops.target_protocol().unwrap().unwrap();
    assert_eq!(v["protocol_upgrade_schedule_hash"], serde_json::json!("ab".repeat(32)));
    assert_eq!(v["supported_protocol_version"], serde_json::json!(2));
}

#[test]
fn upstream_ignites_from_the_checkpoint_and_reaches_live() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    for st in ["ARMED", "FROZEN", "SNAPSHOTTED", "VERIFIED", "IGNITED", "LIVE"] {
        assert!(transition(&e, st).is_some(), "{st} journaled");
    }
    let verified = &transition(&e, "VERIFIED").unwrap()["data"];
    let cut = verified["cut_height"].as_u64().unwrap();
    // Boot manifest = checkpoint manifest + the FULL packed source cut block.
    let manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(verified["boot"]["manifest"].as_str().unwrap()).unwrap()).unwrap();
    let packed = pulse_cutover::upstream::pack_signed_block(&fixture_block(cut)).unwrap();
    assert_eq!(manifest["source_block"].as_str().unwrap(), hex::encode(&packed.bytes));
    assert_eq!(manifest["source_block_id"].as_str().unwrap(), packed.id);
    assert_eq!(verified["boot"]["source_block_receipts"], 6);
    // Genesis commits the verified checkpoint; the chain config points at checkpoint + manifest.
    let genesis: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(verified["boot"]["genesis"].as_str().unwrap()).unwrap()).unwrap();
    assert_eq!(genesis["migration_checkpoint_sha256"], verified["checkpoint_sha256"]);
    assert_eq!(genesis["initial_key"], pulse_cutover::keys::format_public_k1(&test_producer_pub()));
    let cc: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(verified["boot"]["chain_config"].as_str().unwrap()).unwrap()).unwrap();
    assert_eq!(cc["migration_checkpoint"], verified["checkpoint"]);
    assert_eq!(cc["migration_manifest"], verified["boot"]["manifest"]);
    assert_eq!(cc["producer_name"], "bp1");
    // The chain was created once, its id journaled before ignite_started, the config installed
    // under it, and the placeholders bound when the ignite command ran.
    assert_eq!(std::fs::read_to_string(dir.path().join("create-chain.calls")).unwrap().lines().count(), 1);
    let pos = |needle: &str| e.iter().position(|v| v.to_string().contains(needle)).unwrap_or_else(|| panic!("{needle} journaled"));
    assert!(pos("\"target_blockchain_id\"") < pos("ignite_started"));
    let installed = dir.path().join("chain-configs").join(BLOCKCHAIN_ID).join("config.json");
    assert_eq!(std::fs::read_to_string(installed).unwrap(), std::fs::read_to_string(verified["boot"]["chain_config"].as_str().unwrap()).unwrap());
    assert!(ops.ignite_vars.borrow().iter().any(|(k, v)| k == "blockchain_id" && v == BLOCKCHAIN_ID));
    assert!(ops.ignite_vars.borrow().iter().any(|(k, v)| k == "subnet_id" && v == "2qFyanyVDk2LKrUsUdh3JjtYJYZGqUg9y9QAvMaWdQZZGaLiXY"));
    // Mainnet gaps journaled as warnings; no overrides; same chain_id end to end.
    assert!(e.iter().any(|v| v["data"]["upstream_ignite_warnings"].to_string().contains("TAPOS")));
    let live = &transition(&e, "LIVE").unwrap()["data"];
    assert_eq!(live["source_chain_id_changed"], false);
    assert_eq!(live["rehearsal_overrides"], serde_json::json!([]));
    assert_eq!(transition(&e, "IGNITED").unwrap()["data"]["lineage_at_cut"], "verified");
    assert_eq!(ops.resumes.get(), 0);
}

fn other_key() -> [u8; 33] {
    pulse_cutover::keys::public_of_private_k1(&pulse_cutover::keys::format_private_k1(&[6u8; 32])).unwrap()
}

#[test]
fn stage2_producer_key_is_checked_against_eosio_producers_at_arm_and_before_ignition() {
    // Happy path: the check passes at ARM and again before ignition, both journaled (public key only).
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    let checks: Vec<_> = e.iter().filter(|v| !v["data"]["producer_key_check"].is_null()).collect();
    assert_eq!(checks.iter().map(|v| v["state"].as_str().unwrap()).collect::<Vec<_>>(), vec!["ARMED", "VERIFIED"]);
    assert_eq!(checks[0]["data"]["producer_key_check"]["signing_key"], pulse_cutover::keys::format_public_k1(&test_producer_pub()));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(!text.contains(&pulse_cutover::keys::format_private_k1(&TEST_PRODUCER_SECRET)), "the private key is never journaled");
    assert_eq!(ops.producer_reads.get(), 2);
}

#[test]
fn stage2_producer_key_mismatch_aborts_at_arm_before_anything_freezes() {
    // Stage-2 run 1: the node signed with a key eosio/producers did not register for it; the first
    // onblock re-election put the registered key on the schedule and the chain halted at H+4.
    for (row, want) in [
        (Some(producer_row("bp1", &other_key())), "producer signing key mismatch"),
        (None, "not registered in eosio/producers"),
        (Some({ let mut r = producer_row("bp1", &test_producer_pub()); r["is_active"] = serde_json::json!(0); r }), "NOT active"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
        let ops = upstream_ops(dir.path());
        ops.producer_rows.borrow_mut().push_back(row);
        let err = run_machine_result(&cfg, &ops).unwrap_err();
        assert!(err.contains(want), "{want}: {err}");
        let e = journal_entries(&cfg);
        assert_eq!(e.last().unwrap()["state"], "ABORTED", "{want}");
        assert!(transition(&e, "FROZEN").is_none(), "{want}: nothing froze");
        assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"), "{want}");
    }
}

#[test]
fn stage2_producer_key_changed_by_the_cut_aborts_before_create_chain() {
    // Registered key changes between ARM and the cut (a regproducer in the window): the check at
    // VERIFIED reads the state the target will inherit and aborts pre-ignition (source resumed).
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    ops.producer_rows.borrow_mut().extend([Some(producer_row("bp1", &test_producer_pub())), Some(producer_row("bp1", &other_key()))]);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let e = journal_entries(&cfg);
    assert!(transition(&e, "VERIFIED").is_some());
    let err = e.iter().find(|v| v["kind"] == "error").unwrap();
    assert_eq!(err["data"]["message"], "producer signing key check failed (pre-ignition)");
    assert!(err["data"]["detail"]["problem"].as_str().unwrap().contains(&pulse_cutover::keys::format_public_k1(&other_key())));
    assert!(!dir.path().join("create-chain.calls").exists(), "no chain created");
    assert!(!std::fs::read_to_string(&cfg.journal_path).unwrap().contains("ignite_started"));
    assert_eq!(ops.resumes.get(), 1);
}

#[test]
fn stage2_chain_config_producer_key_must_be_the_genesis_initial_key() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    std::fs::write(dir.path().join("chain-config-base.json"), serde_json::json!({"producer_name": "bp1",
        "producer_key": pulse_cutover::keys::format_private_k1(&[6u8; 32])}).to_string()).unwrap();
    let err = run_machine_result(&cfg, &upstream_ops(dir.path())).unwrap_err();
    assert!(err.contains("is not the genesis initial_key"), "{err}");
    assert!(!err.contains(&pulse_cutover::keys::format_private_k1(&[6u8; 32])), "{err}");
    // No producer_name: cannot tell which producer to check.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    std::fs::write(dir.path().join("chain-config-base.json"), r#"{"system_account":"eosio"}"#).unwrap();
    assert!(run_machine_result(&cfg, &upstream_ops(dir.path())).unwrap_err().contains("no producer_name"));
}

#[test]
fn upstream_target_chain_id_change_halts_without_the_override_and_passes_with_it() {
    // Without the override: the chain_id check runs after ignite_started, so it SEALS.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    *ops.target_chain_id.borrow_mut() = Some(TARGET_CHAIN_ID.into());
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED") && err.contains("target chain_id != source chain_id"), "{err}");
    assert_eq!(ops.resumes.get(), 0, "never resume the source after ignition started");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("rehearsal_allow_chain_id_change"), "the HALT names the rehearsal override");

    // With it: LIVE, both ids journaled.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "rehearsal_allow_chain_id_change = true", "").unwrap();
    let ops = upstream_ops(dir.path());
    *ops.target_chain_id.borrow_mut() = Some(TARGET_CHAIN_ID.into());
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    let change = e.iter().find(|v| !v["data"]["rehearsal_chain_id_change"].is_null()).expect("chain id change journaled");
    assert_eq!(change["data"]["rehearsal_chain_id_change"]["source_chain_id"], hex::encode(CHAIN_ID));
    assert_eq!(change["data"]["rehearsal_chain_id_change"]["target_chain_id"], TARGET_CHAIN_ID);
    let live = &transition(&e, "LIVE").unwrap()["data"];
    assert_eq!(live["target_chain_id"], TARGET_CHAIN_ID);
    assert_eq!(live["source_chain_id_changed"], true);
    assert!(live["rehearsal_overrides"].to_string().contains("rehearsal_allow_chain_id_change"));
    assert!(transition(&e, "ARMED").unwrap()["data"]["rehearsal_overrides"].to_string().contains("chain_id"));
    // Resume keeps the accepted id (not re-derived from the source).
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.accepted_target_chain_id.as_deref(), Some(TARGET_CHAIN_ID));
    assert_eq!(rec.chain_id.as_deref(), Some(hex::encode(CHAIN_ID).as_str()), "the source chain_id is not overwritten");
    assert_eq!(rec.target_blockchain_id.as_deref(), Some(BLOCKCHAIN_ID));
}

#[test]
fn upstream_compare_failure_on_allowed_tables_passes_only_with_the_override() {
    let failing = ["contract_index_double", "global_property"];
    // No override: the v1.0.0 compare failure aborts verification (source resumed).
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    fake_compare_failing(dir.path(), &failing);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("xpr_19_table_compare FAILED") && !upstream_journal_has_verified(&text));
    assert_eq!(ops.resumes.get(), 1);

    // Override listing exactly those tables: verification passes LOUDLY, full output journaled.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    fake_compare_failing(dir.path(), &failing);
    let cfg = upstream_ignite_config(dir.path(), "", r#"rehearsal_allow_compare_mismatch = ["contract_index_double", "global_property"]"#).unwrap();
    let ops = upstream_ops(dir.path());
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    let cmp = e.iter().find(|v| v["data"]["upstream_19_table_compare"]["result"].as_str().is_some_and(|r| r.contains("ALLOWED"))).expect("override journaled");
    assert_eq!(cmp["data"]["upstream_19_table_compare"]["failing_tables"], serde_json::json!(failing));
    assert!(cmp["data"]["upstream_19_table_compare"]["output"].as_str().unwrap().contains("first differing row"), "full output journaled");
    assert!(cmp["data"]["upstream_19_table_compare"]["REHEARSAL_ONLY"].is_string());
    let verified = &transition(&e, "VERIFIED").unwrap()["data"];
    assert!(verified["table_compare"].as_str().unwrap().starts_with("MISMATCH ALLOWED BY REHEARSAL OVERRIDE"));
    assert_eq!(verified["compare_allowed_mismatch"], serde_json::json!(failing));
}

#[test]
fn upstream_compare_failure_on_a_table_not_allowed_still_aborts() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    fake_compare_failing(dir.path(), &["global_property", "permission_link"]);
    let cfg = upstream_ignite_config(dir.path(), "", r#"rehearsal_allow_compare_mismatch = ["contract_index_double", "global_property"]"#).unwrap();
    let ops = upstream_ops(dir.path());
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("not the known difference: permission_link"), "{text}");
    assert!(!upstream_journal_has_verified(&text));
    assert!(!text.contains("ignite_started"));
    // A failure that names no table cannot be allowlisted either.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    write_script(&dir.path().join("fake-compare.sh"), "#!/bin/sh\necho 'panicked: arena open failed' >&2\nexit 101\n");
    let cfg = upstream_ignite_config(dir.path(), "", r#"rehearsal_allow_compare_mismatch = ["global_property"]"#).unwrap();
    assert_eq!(run_machine(&cfg, &upstream_ops(dir.path())), State::Aborted);
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("names no table"));
}

#[test]
fn rehearsal_overrides_are_refused_for_mainnet() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let chain = format!("chain_id = \"{MAINNET}\"");
    for (c, u) in [
        (format!("{chain}\nrehearsal_allow_chain_id_change = true"), String::new()),
        (chain.clone(), r#"rehearsal_allow_compare_mismatch = ["global_property"]"#.to_string()),
    ] {
        let err = upstream_ignite_config(dir.path(), &c, &u).unwrap_err();
        assert!(err.contains("MAINNET"), "{err}");
    }
    assert!(upstream_ignite_config(dir.path(), "", r#"rehearsal_allow_compare_mismatch = ["*"]"#).unwrap_err().contains("not a table name"));
    assert!(upstream_ignite_config(dir.path(), "", r#"rehearsal_allow_compare_mismatch = ["permission"]"#).unwrap_err().contains("no known difference"));
    // Mainnet discovered at ARM (no chain_id configured): refused before anything freezes.
    let cfg = upstream_ignite_config(dir.path(), "rehearsal_allow_chain_id_change = true", "").unwrap();
    let ops = upstream_ops(dir.path());
    *ops.chain_id.borrow_mut() = MAINNET.into();
    assert!(run_machine_result(&cfg, &ops).is_err());
    let e = journal_entries(&cfg);
    assert!(transition(&e, "FROZEN").is_none());
    assert!(e.iter().any(|v| v.to_string().contains("XPR MAINNET and rehearsal overrides are active")));
    assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"));
}

/// upstream_ignite_config turned into a PRODUCTION-profile config (no `rehearsal`): exact-H
/// scheduled snapshot, every required hook, a post-LIVE probe.
fn upstream_production_config(dir: &std::path::Path) -> Result<Config, String> {
    let _ = upstream_ignite_config(dir, "", "")?;
    let path = dir.join("ceremony-upstream-ignite.toml");
    let text = std::fs::read_to_string(&path).unwrap()
        .replace("rehearsal = true\n", "freeze_strategy = \"schedule_at_h\"\n")
        .replace("[snapshot]\n", &format!("[snapshot]\ndir = \"{}\"\n", dir.display()))
        .replace("[target]\n", "[target]\npost_live_probe_cmd = \"probe-workload\"\n");
    std::fs::write(&path, text).unwrap();
    Config::load(&path)
}

/// The production upstream config, coordinated the way a mainnet ceremony must be (RISK-REGISTER D-10): a signed
/// event (id + hash), a roster of bp1 (this node, with its beacon) and bp2, quorum 2.
/// A mainnet-shaped signed event (D-10): it binds the chain, H, roster and quorum the ceremony runs with.
fn mainnet_event(h: u64) -> serde_json::Value {
    signed(serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e1", "chain_id": MAINNET, "h": h,
        "roster": [{"producer": "bp1"}, {"producer": "bp2"}], "quorum": 2}))
}

/// The coordinated mainnet config and the signed event the mock relay must serve for it (set it on the ops).
fn upstream_mainnet_coordinated_config(dir: &std::path::Path) -> Config {
    let h = upstream_production_config(dir).unwrap().ceremony.freeze_height;
    std::fs::write(dir.join("beacon.token"), "tok\n").unwrap();
    let ev_hash = pulse_cutover::coord::payload_hash(&mainnet_event(h)).unwrap();
    let text = std::fs::read_to_string(dir.join("ceremony-upstream-ignite.toml")).unwrap()
        + &format!("\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{}\"]\nevent_id = \"e1\"\nevent_hash = \"{}\"\nfleet_quorum = 2\nfleet_timeout_secs = 5\nreport_max_age_secs = 60\n",
            hex::encode(coord_key().verifying_key().to_bytes()), ev_hash)
        + "\n[[coordination.roster]]\nproducer = \"bp1\"\n\n[[coordination.roster]]\nproducer = \"bp2\"\n"
        + &format!("\n[beacon]\nurl = \"https://mc.example/api/report\"\nproducer = \"bp1\"\nnetwork = \"rehearsal\"\ntoken_file = \"{}/beacon.token\"\n", dir.display());
    load_toml(dir, "mainnet-coord.toml", &text).unwrap()
}

#[test]
fn upstream_ignite_is_refused_for_mainnet_while_gaps_remain() {
    // No overrides, production profile, coordinated, mainnet chain: verification runs, ignition is refused
    // (pre-ignition abort). The only peer has aborted, so the resume guard lets this node resume the old chain.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_mainnet_coordinated_config(dir.path());
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"event": mainnet_event(cfg.ceremony.freeze_height)}));
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED", "aborting": true})),
        ("bp2", serde_json::json!({"state": "ABORTED"}))]));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(upstream_journal_has_verified(&text));
    assert!(text.contains("refused for XPR MAINNET") && text.contains("TAPOS"));
    assert!(text.contains("#106") && text.contains("#107") && text.contains("#113"), "every open mainnet blocker is named");
    assert!(!dir.path().join("create-chain.calls").exists(), "no chain created");
    assert_eq!(ops.resumes.get(), 1);
    assert!(text.contains("mainnet_event_verified"), "the signed event was authenticated and bound at ARM");
}

/// RISK-REGISTER D-10: an XPR MAINNET ceremony without signed coordination is refused, at load when chain_id is
/// configured and at ARM (before anything freezes) when it is discovered; `await`'s base config still loads.
#[test]
fn d10_mainnet_producer_ceremony_must_be_coordinated() {
    // Load: a configured mainnet chain_id without a [coordination] section.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let _ = upstream_production_config(dir.path()).unwrap();
    let base = std::fs::read_to_string(dir.path().join("ceremony-upstream-ignite.toml")).unwrap();
    let with_chain = base.replacen("[ceremony]\n", &format!("[ceremony]\nchain_id = \"{MAINNET}\"\n"), 1);
    let err = load_toml(dir.path(), "m.toml", &with_chain).unwrap_err();
    assert!(err.contains("must be coordinated") && err.contains("[coordination]"), "{err}");
    // ...the same config for the testnet loads: the rule is mainnet-only.
    assert!(load_toml(dir.path(), "t.toml", &base).is_ok());
    // await's base config: [coordination] without an event or roster yet loads for mainnet.
    let await_base = with_chain.clone() + &format!("\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{}\"]\n",
        hex::encode(coord_key().verifying_key().to_bytes()));
    let cfg = load_toml(dir.path(), "await-base.toml", &await_base).unwrap();
    // ...but it cannot ARM: no signed event, no roster, quorum below 2. Nothing freezes.
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    assert!(run_machine_result(&cfg, &ops).is_err());
    let e = journal_entries(&cfg);
    assert!(transition(&e, "FROZEN").is_none(), "nothing froze");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("must run from a signed event") && text.contains("roster is empty") && text.contains("quorum of at least 2"), "{text}");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"), "writes never froze: {:?}", ops.hooks.borrow());

    // ARM: a config that leaves chain_id to discovery has no [coordination]; the source turns out to be mainnet.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_production_config(dir.path()).unwrap();
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    assert!(run_machine_result(&cfg, &ops).is_err());
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_none(), "nothing froze");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("must be coordinated"), "{text}");
    // ...and the same config runs on the testnet as before.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_production_config(dir.path()).unwrap();
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    let _ = run_machine_result(&cfg, &ops);
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_some(), "testnet: the rule does not apply");
}

/// RISK-REGISTER D-10 (Codex review): field presence is not authorization. At a fresh mainnet ARM the relay must
/// serve the signed event the config was derived from, and the payload must bind this chain, H, roster and quorum;
/// every mismatch, a bad signature, another payload under the same id, and a relay that does not answer refuse
/// before anything freezes.
#[test]
fn d10_mainnet_arm_authenticates_and_binds_the_signed_event() {
    let other_key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
    let forged = |payload: serde_json::Value| {
        use ed25519_dalek::Signer;
        let p = payload.to_string();
        serde_json::json!({"payload": p, "sig": hex::encode(other_key.sign(p.as_bytes()).to_bytes()),
            "key": hex::encode(other_key.verifying_key().to_bytes())})
    };
    // (what the relay serves, the expected refusal) — None = the relay is down.
    let cases: Vec<(&str, Option<serde_json::Value>, &str)> = vec![
        ("wrong chain", Some(serde_json::json!({"event": signed(serde_json::json!({"type": "event", "network": "rehearsal",
            "event_id": "e1", "chain_id": "71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd", "h": 0,
            "roster": [{"producer": "bp1"}, {"producer": "bp2"}], "quorum": 2}))})), "event_hash differs"),
        ("relay down", None, "did not answer"),
        ("no event", Some(serde_json::json!({})), "does not serve a signed event"),
        ("bad signature", Some(serde_json::json!({"event": forged(serde_json::json!({"type": "event"}))})), "does not verify"),
    ];
    for (name, doc, needle) in cases {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let cfg = upstream_mainnet_coordinated_config(dir.path());
        let ops = upstream_ops(dir.path());
        ops.schedule_ok.set(true);
        *ops.chain_id.borrow_mut() = MAINNET.into();
        match doc {
            Some(d) => *ops.coord_doc.borrow_mut() = Some(d),
            None => ops.coord_down.set(true),
        }
        assert!(run_machine_result(&cfg, &ops).is_err(), "{name}");
        let e = journal_entries(&cfg);
        assert!(transition(&e, "FROZEN").is_none(), "{name}: nothing froze");
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        assert!(text.contains("XPR MAINNET signed event") && text.contains(needle), "{name}: {text}");
        assert!(!text.contains("mainnet_event_verified"), "{name}");
    }

    // Binding mismatches on an authentic payload whose hash the config carries: the config is re-derived per case so
    // the hash check passes and the binding check is what refuses.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_mainnet_coordinated_config(dir.path());
    let h = cfg.ceremony.freeze_height;
    let ev = |chain: Option<&str>, hh: Option<u64>, roster: serde_json::Value, quorum: u64| {
        let mut p = serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e1", "roster": roster, "quorum": quorum});
        if let Some(c) = chain { p["chain_id"] = serde_json::json!(c); }
        if let Some(x) = hh { p["h"] = serde_json::json!(x); }
        signed(p)
    };
    let two = serde_json::json!([{"producer": "bp1"}, {"producer": "bp2"}]);
    for (name, msg, needle) in [
        ("good", ev(Some(MAINNET), Some(h), two.clone(), 2), ""),
        ("other chain", ev(Some("71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd"), Some(h), two.clone(), 2), "is for chain_id"),
        ("no chain", ev(None, Some(h), two.clone(), 2), "names no chain_id"),
        ("other H", ev(Some(MAINNET), Some(h + 1), two.clone(), 2), "is not ceremony.freeze_height"),
        ("no H", ev(Some(MAINNET), None, two.clone(), 2), "names no H"),
        ("other roster", ev(Some(MAINNET), Some(h), serde_json::json!([{"producer": "bp1"}, {"producer": "bp9"}]), 2), "roster differs"),
        ("other quorum", ev(Some(MAINNET), Some(h), two.clone(), 1), "quorum 1 is not"),
    ] {
        let mut c = cfg.clone();
        c.coordination.as_mut().unwrap().event_hash = pulse_cutover::coord::payload_hash(&msg);
        let doc = serde_json::json!({"event": msg});
        let p = pulse_cutover::coord::mainnet_event_problems(Some(&doc), &c, MAINNET);
        if needle.is_empty() {
            assert!(p.is_empty(), "{name}: {p:?}");
        } else {
            assert!(p.iter().any(|x| x.contains(needle)), "{name}: {p:?}");
        }
    }
    // The network the event was signed for must be this node's coordination network.
    let mut c = cfg.clone();
    let msg = signed(serde_json::json!({"type": "event", "network": "testnet", "event_id": "e1", "chain_id": MAINNET, "h": h,
        "roster": two, "quorum": 2}));
    c.coordination.as_mut().unwrap().event_hash = pulse_cutover::coord::payload_hash(&msg);
    let p = pulse_cutover::coord::mainnet_event_problems(Some(&serde_json::json!({"event": msg})), &c, MAINNET);
    assert!(p.iter().any(|x| x.contains("network")), "{p:?}");
}

/// RISK-REGISTER D-10, api mode: a mainnet api ceremony needs the signed event too (no roster: API nodes are not
/// members of the producers' quorum).
#[test]
fn d10_mainnet_api_ceremony_needs_the_signed_event_but_no_roster() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = api_test_config(dir.path(), 120);
    let mut c = cfg.clone();
    assert!(c.mainnet_coordination_problems(true).iter().any(|p| p.contains("must be coordinated")));
    c.coordination = coord_config(dir.path(), 120, 0, 5).coordination;
    assert!(c.mainnet_coordination_problems(true).is_empty(), "api: event id + hash suffice, no roster or quorum");
    c.coordination.as_mut().unwrap().event_hash = None;
    assert!(c.mainnet_coordination_problems(true).iter().any(|p| p.contains("signed event")));
    // Producer mode with the same coordination still needs a roster and a quorum.
    let mut p = coord_config(dir.path(), 120, 0, 5);
    assert!(p.mainnet_coordination_problems(true).iter().any(|x| x.contains("roster is empty")));
    p.coordination.as_mut().unwrap().fleet_quorum = 1;
    assert!(p.mainnet_coordination_problems(true).iter().any(|x| x.contains("quorum of at least 2")));
    // A readiness config is not a ceremony.
    p.ceremony.profile = pulse_cutover::config::Profile::Readiness;
    assert!(p.mainnet_coordination_problems(true).is_empty());
}

#[test]
fn upstream_anchor_must_be_the_cut_block() {
    // The source cannot serve the cut block: abort before VERIFIED.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    ops.source_block_fails.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("cannot fetch the full source cut block") && !upstream_journal_has_verified(&text));
    // A block that is not the cut (its computed id differs from the pinned cut id): abort.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = MockOps::new(dir.path(), 110); // mini-snapshot ids: not the fixture block's id
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("is not the cut"), "{text}");
    assert_eq!(ops.resumes.get(), 1);
}

#[test]
fn upstream_create_chain_failure_after_it_started_halts_never_resumes_the_source() {
    // Review: a creation hook can submit a chain that validators already tracking the subnet start;
    // its failure (or an answer without an id, or a lost answer) is not proof that no chain exists.
    for (script, needle) in [
        ("#!/bin/sh\necho 'insufficient funds' >&2\nexit 1\n", "create_chain_cmd failed after it started"),
        ("#!/bin/sh\necho created\n", "no blockchain id"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        write_script(&dir.path().join("fake-create-chain.sh"), script);
        let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
        let ops = upstream_ops(dir.path());
        let err = run_machine_result(&cfg, &ops).unwrap_err();
        assert!(err.starts_with("HALTED") && err.contains(needle), "{err}");
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        assert!(text.contains("point_of_no_return"), "the creation intent is the boundary");
        assert!(!text.contains("ignite_started"));
        assert_eq!(ops.resumes.get(), 0, "the source is NOT resumed");
        assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "on_abort must not run");
        // Going back needs the fleet-wide, fenced operator decision.
        assert!(rollback_with(&cfg, &ops, false).unwrap_err().starts_with("refusing"));
    }
}

#[test]
fn upstream_resume_after_create_chain_started_without_a_journaled_id_halts() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    let cmd = format!("sh {d}/fake-create-chain.sh {d}/upstream-work/migration-genesis-120.json {d}/upstream-work/chain-config-120.json {d}/upstream-work/boot-120.manifest.json", d = dir.path().display());
    *ops.panic_on_hook.borrow_mut() = Some(cmd);
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_machine_result(&cfg, &ops)));
    assert!(crashed.is_err(), "agent died inside create_chain");
    let ops2 = upstream_ops(dir.path());
    ops2.head.set(130);
    let err = run_machine_result(&cfg, &ops2).unwrap_err();
    assert!(err.starts_with("HALTED") && err.contains("a target chain may exist"), "{err}");
    assert!(!dir.path().join("create-chain.calls").exists(), "never a second create");
    assert_eq!(ops2.resumes.get(), 0);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(!text.contains("ignite_started"));
}

#[test]
fn rc21_signed_abort_or_unreachable_relay_right_before_create_stops_before_the_boundary() {
    let rig = |dir: &std::path::Path| {
        stage_fake_upstream_tools(dir, 0);
        let _ = upstream_ignite_config(dir, "", "").unwrap();
        let path = dir.join("ceremony-upstream-ignite.toml");
        let text = format!("{}\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{}\"]\nevent_id = \"e1\"\nevent_hash = \"{}\"\n",
            std::fs::read_to_string(&path).unwrap(), hex::encode(coord_key().verifying_key().to_bytes()), mock_event_hash());
        std::fs::write(&path, text).unwrap();
        Config::load(&path).unwrap()
    };
    // The abort arrives as the fleet gate passes (the last fake tool touches abort-now).
    let dir = tempfile::tempdir().unwrap();
    let cfg = rig(dir.path());
    let body = std::fs::read_to_string(dir.path().join("fake-fingerprint.sh")).unwrap();
    write_script(&dir.path().join("fake-fingerprint.sh"), &body.replacen("\n", &format!("\ntouch {}/abort-now\n", dir.path().display()), 1));
    let ops = upstream_ops(dir.path());
    *ops.coord_doc_when_abort_file.borrow_mut() = Some(serde_json::json!({
        "abort": signed(serde_json::json!({"type": "abort", "event_id": "e1", "network": "rehearsal"}))}));
    // rc.23 review #8: no roster in this rig's event, so even a signed abort cannot show the fleet is accounted for.
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert!(!dir.path().join("create-chain.calls").exists(), "no chain created");
    assert_eq!(ops.resumes.get(), 0, "no roster: not resumed");
    assert!(pulse_cutover::coord::is_tombstoned(&cfg, "e1"), "the abort is persisted");

    // The relay unreachable from the start: "unknown" is not an authorization to create.
    let dir = tempfile::tempdir().unwrap();
    let cfg = rig(dir.path());
    let ops = upstream_ops(dir.path());
    ops.coord_down.set(true);
    // rc.23: and the same unreachable relay cannot show that no peer created the chain: STRANDED (sealed), the
    // source is NOT resumed.
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("cannot confirm the event is not aborted before chain creation"), "{text}");
    assert!(!dir.path().join("create-chain.calls").exists());
    assert_eq!(ops.resumes.get(), 0);

    // A tombstoned abort is final even when the relay no longer serves it.
    let dir = tempfile::tempdir().unwrap();
    let cfg = rig(dir.path());
    pulse_cutover::coord::record_tombstone(&cfg, "e1", "aborted", "seen earlier");
    let ops = upstream_ops(dir.path());
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert!(!dir.path().join("create-chain.calls").exists());
}

#[test]
fn upstream_ignite_failure_halts_after_ignition_started() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    ops.ignite_fails.set(true);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED") && err.contains("ignition command failed"), "{err}");
    assert_eq!(ops.resumes.get(), 0);
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "on_abort must not run");
    // A forced-less rollback is refused after ignition started.
    assert!(rollback_with(&cfg, &ops, false).unwrap_err().starts_with("refusing"));
}

#[test]
fn upstream_boot_artifact_changed_after_verified_aborts_before_ignition() {
    // The agent dies right after VERIFIED (before chain creation); someone edits the migration genesis;
    // the resumed run re-hashes the boot artifacts before anything else and aborts pre-ignition.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    let d = dir.path().display();
    *ops.panic_on_hook.borrow_mut() = Some(format!(
        "sh {d}/fake-create-chain.sh {d}/upstream-work/migration-genesis-120.json {d}/upstream-work/chain-config-120.json {d}/upstream-work/boot-120.manifest.json"));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_machine_result(&cfg, &ops))).is_err());
    // The crash happened right after VERIFIED, before the creation intent: keep the journal up to
    // and including the VERIFIED transition only.
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let mut kept = String::new();
    for l in text.lines() {
        kept.push_str(l);
        kept.push('\n');
        if l.contains("\"kind\":\"transition\"") && l.contains("\"state\":\"VERIFIED\"") {
            break;
        }
    }
    std::fs::write(&cfg.journal_path, kept).unwrap();
    assert!(upstream_journal_has_verified(&std::fs::read_to_string(&cfg.journal_path).unwrap()));
    let genesis = dir.path().join("upstream-work/migration-genesis-120.json");
    let tampered = std::fs::read_to_string(&genesis).unwrap().replace("2020-04-22T17:00:00", "2020-04-22T17:00:01");
    std::fs::write(&genesis, tampered).unwrap();
    let ops2 = upstream_ops(dir.path());
    ops2.head.set(130);
    assert_eq!(run_machine(&cfg, &ops2), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("missing or changed since VERIFIED") && text.contains("migration genesis"), "{text}");
    assert!(!text.contains("ignite_started"));
}

#[test]
fn upstream_api_mode_rehearsal_flips_to_a_target_with_a_different_chain_id() {
    // api mode: the public /v1 health gate must accept the ACCEPTED target chain_id (it compared
    // against the source id before, which a v1.0.0 target never presents).
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    std::fs::write(dir.path().join("genesis-base.json"), r#"{"initial_timestamp":"2020-04-22T17:00:00","initial_key":"PUB_K1_test","initial_configuration":{}}"#).unwrap();
    let toml_text = format!(
        r#"
journal_path = "{d}/journal.jsonl"
poll_ms = 1

[ceremony]
rehearsal = true
mode = "api"
freeze_height = 120
simulate_freeze = true
import_backend = "upstream"
rehearsal_allow_chain_id_change = true

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
stop_cmd = "stop-nodeos"

[snapshot]
staged_path = "{d}/staged.bin"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://127.0.0.1:9650/ext/bc/{{blockchain_id}}/rpc"
quorum_timeout_secs = 60
create_chain_cmd = "sh {d}/fake-create-chain.sh {{genesis}} {{chain_config}} {{manifest}}"

[upstream]
work_dir = "{d}/upstream-work"
export_cmd = "sh {d}/fake-export.sh {{snapshot}} {{export_dir}}"
import_bin = "{d}/fake-import.sh"
fingerprint_bin = "{d}/fake-fingerprint.sh"
compare_bin = "{d}/fake-compare.sh"
genesis_base = "{d}/genesis-base.json"

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
        d = dir.path().display()
    );
    let cfg = load_toml(dir.path(), "api-upstream.toml", &toml_text).unwrap();
    let ops = upstream_ops(dir.path());
    *ops.target_chain_id.borrow_mut() = Some(TARGET_CHAIN_ID.into());
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    assert_eq!(transition(&e, "FLIPPED").unwrap()["data"]["health"]["public_chain_id"], TARGET_CHAIN_ID);
    assert_eq!(transition(&e, "LIVE").unwrap()["data"]["source_chain_id_changed"], true);
}

#[test]
fn rehearsal_overrides_show_in_status_and_fail_a_beacon_setup_check() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    let base = format!(r#"
journal_path = "{d}/journal.jsonl"
[ceremony]
profile = "readiness"
REHEARSAL
[source]
rpc_url = "http://127.0.0.1:9"
producer_api_url = "http://127.0.0.1:9"
[snapshot]
staged_path = "{d}/staged.bin"
[target]
metalgo_unit = "metalgo-none"
rpc_url = "http://127.0.0.1:9/ext/bc/X/rpc"
[beacon]
url = ""
producer = "bp1"
network = "rehearsal"
"#);
    let run = |cfg_text: &str, args: &[&str]| -> (bool, String) {
        let p = dir.path().join("b.toml");
        std::fs::write(&p, cfg_text).unwrap();
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover")).args(args).arg("--config").arg(&p).args(if args[0] == "beacon" { &["--once"][..] } else { &[][..] }).output().unwrap();
        (out.status.success(), format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr)))
    };
    let with = base.replace("REHEARSAL", "rehearsal_allow_chain_id_change = true");
    let (ok, out) = run(&with, &["status"]);
    assert!(ok && out.contains("rehearsal_overrides: ACTIVE"), "{out}");
    let (ok, out) = run(&with, &["beacon"]);
    assert!(ok, "{out}");
    let report: serde_json::Value = serde_json::from_str(out.lines().find(|l| l.starts_with('{')).map(|_| &out[out.find('{').unwrap()..out.rfind('}').unwrap() + 1]).unwrap()).unwrap();
    let c = report["checks"].as_array().unwrap().iter().find(|c| c["name"] == "rehearsal_overrides").expect("check present");
    assert_eq!(c["ok"], false);
    assert!(c["detail"].as_str().unwrap().contains("rehearsal overrides active"));
    assert!(!pulse_cutover::beacon::has_failing_health(&serde_json::json!({"checks": [c]})), "a SETUP check, not health");
    // Without overrides: no such check, and status says none.
    let without = base.replace("REHEARSAL", "");
    let (_, out) = run(&without, &["status"]);
    assert!(out.contains("rehearsal_overrides: none"), "{out}");
    let (_, out) = run(&without, &["beacon"]);
    assert!(!out.contains("\"rehearsal_overrides\""), "{out}");
    // An explicit rehearsal ceremony is shown the same way (rc.21).
    let marked = base.replace("REHEARSAL", "rehearsal = true");
    let (_, out) = run(&marked, &["status"]);
    assert!(out.contains("ceremony: REHEARSAL"), "{out}");
    let (_, out) = run(&marked, &["beacon"]);
    assert!(out.contains("REHEARSAL ceremony (ceremony.rehearsal = true)"), "{out}");
}

#[test]
fn stage2_post_live_watch_reports_a_stalled_target_red_in_beacon_and_status() {
    // Stage-2 run 2: LIVE at 18:20:19.8, last block two seconds later (CPU billing), every later
    // transaction accepted over HTTP and never included, and everything kept saying LIVE.
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    let now = chrono::Utc::now();
    let ms = |t: chrono::DateTime<chrono::Utc>| t.timestamp_millis();
    let live_at = now - chrono::Duration::seconds(90);
    let block_time = std::sync::Arc::new(std::sync::Mutex::new(live_at + chrono::Duration::seconds(2)));
    let bt = block_time.clone();
    let (url, _) = stub_http(move |_m, p| {
        if p.contains("/ext/bc/2YXVy2NWHZJphNuvWry8So8TpQodhSb7ZVJPxhwhtKS6Z6JjpV/rpc") {
            let t = bt.lock().unwrap().format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
            (200, serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {"head_block_num": 408462226u64, "head_block_time": t}}).to_string())
        } else {
            (200, r#"{"head_block_num": 500, "last_irreversible_block_num": 400, "chain_id": "ab"}"#.into())
        }
    });
    let line = |seq: u64, kind: &str, state: &str, ts: i64, data: serde_json::Value| {
        format!("{}\n", serde_json::json!({"seq": seq, "ts_ms": ts, "ts": "t", "kind": kind, "state": state, "data": data}))
    };
    let t0 = ms(live_at) - 600_000;
    let journal = [
        line(0, "transition", "ARMED", t0, serde_json::json!({"resolved_h": 100})),
        line(1, "evidence", "VERIFIED", t0 + 1000, serde_json::json!({"target_blockchain_id": BLOCKCHAIN_ID, "target_subnet_id": "2qFyanyVDk2LKrUsUdh3JjtYJYZGqUg9y9QAvMaWdQZZGaLiXY"})),
        line(2, "transition", "IGNITED", t0 + 2000, serde_json::json!({})),
        line(3, "transition", "LIVE", ms(live_at), serde_json::json!({"write_gap_ms_wallclock": 1000})),
    ].concat();
    std::fs::write(dir.path().join("journal.jsonl"), journal).unwrap();
    let cfg_text = |probe: &str| format!(r#"
journal_path = "{d}/journal.jsonl"
[ceremony]
rehearsal = true
profile = "readiness"
[source]
rpc_url = "{url}"
producer_api_url = "{url}"
[snapshot]
staged_path = "{d}/staged.bin"
[target]
metalgo_unit = "metalgo-none"
rpc_url = "{url}/ext/bc/{{blockchain_id}}/rpc"
{probe}
[beacon]
url = ""
producer = "bp1"
network = "rehearsal"
"#);
    let run = |text: &str, cmd: &str| -> String {
        let p = dir.path().join("b.toml");
        std::fs::write(&p, text).unwrap();
        let mut c = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"));
        c.arg(cmd).arg("--config").arg(&p);
        if cmd == "beacon" {
            c.arg("--once");
        }
        let out = c.output().unwrap();
        format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr))
    };
    let report = |out: &str| -> serde_json::Value { serde_json::from_str(&out[out.find('{').unwrap()..out.rfind('}').unwrap() + 1]).unwrap() };
    let target_live = |r: &serde_json::Value| r["checks"].as_array().unwrap().iter().find(|c| c["name"] == "target_live").cloned();

    let r = report(&run(&cfg_text(""), "beacon"));
    let c = target_live(&r).expect("target_live reported after LIVE");
    assert_eq!(c["ok"], false, "{c}");
    assert!(c["detail"].as_str().unwrap().starts_with("no new block for 8"), "{c}");
    assert!(pulse_cutover::beacon::has_failing_health(&r), "a HEALTH check: mission control shows it red");
    assert_eq!(r["ceremony"]["state"], "LIVE", "the journal state is reported as is (no auto-rollback)");
    for k in ["live_ts_ms", "target_blockchain_id", "target_subnet_id"] {
        assert!(r["ceremony"].get(k).is_none(), "{k} stays local");
    }
    let out = run(&cfg_text(""), "status");
    assert!(out.contains("target_live: FAILING (no new block for 8") && out.contains("operator decision"), "{out}");
    assert!(out.contains("state: LIVE"), "{out}");

    // Producing again: green. A failing operator probe turns it red with its own reason.
    *block_time.lock().unwrap() = chrono::Utc::now() - chrono::Duration::seconds(3);
    let c = target_live(&report(&run(&cfg_text(""), "beacon"))).unwrap();
    assert_eq!(c["ok"], true, "{c}");
    assert!(run(&cfg_text(""), "status").contains("target_live: ok (producing"));
    let c = target_live(&report(&run(&cfg_text("post_live_probe_cmd = \"echo no inclusion for {blockchain_id} >&2; exit 3\""), "beacon"))).unwrap();
    assert_eq!(c["ok"], false, "{c}");
    assert!(c["detail"].as_str().unwrap().contains("probe failed: no inclusion for"), "{c}");
    // rc.21: idle check off (rehearsal) no longer switches the probe off.
    let c = target_live(&report(&run(&cfg_text("post_live_max_idle_secs = 0\npost_live_probe_cmd = \"exit 4\""), "beacon"))).expect("still checked");
    assert_eq!(c["ok"], false, "{c}");
    assert!(c["detail"].as_str().unwrap().contains("idle check off") && c["detail"].as_str().unwrap().contains("probe failed"), "{c}");
    // A quiet chain with a PASSING probe is ok (no traffic = no blocks on PulseVM).
    *block_time.lock().unwrap() = live_at + chrono::Duration::seconds(2);
    let c = target_live(&report(&run(&cfg_text("post_live_probe_cmd = \"true\""), "beacon"))).unwrap();
    assert_eq!(c["ok"], true, "{c}");
    assert!(c["detail"].as_str().unwrap().starts_with("idle, probe passing"), "{c}");
}

#[test]
fn upstream_example_configs_load_and_the_rehearsal_one_refuses_mainnet() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["ceremony-upstream.toml", "ceremony-upstream-rehearsal.toml"] {
        let text = std::fs::read_to_string(format!("{}/examples/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap();
        let cfg = load_toml(dir.path(), name, &text).unwrap_or_else(|e| panic!("{name}: {e}"));
        let rehearsal = name.contains("rehearsal");
        assert_eq!(!cfg.rehearsal_overrides().is_empty(), rehearsal, "{name}");
        if rehearsal {
            assert!(cfg.upstream.as_ref().unwrap().ignite_configured(&cfg.target));
            let mainnet = text.replace("71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd", MAINNET);
            assert!(load_toml(dir.path(), "m.toml", &mainnet).unwrap_err().contains("MAINNET"));
        }
    }
}

#[cfg(unix)]
#[test]
fn rc21_signer_material_is_written_0600_and_a_shared_directory_is_refused_at_arm() {
    use std::os::unix::fs::PermissionsExt;
    // Review: the generated chain config copied producer_key from the base and was written with
    // the umask's mode (0644), then installed by copy.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let ops = upstream_ops(dir.path());
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    let verified = &transition(&e, "VERIFIED").unwrap()["data"];
    let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(std::path::Path::new(verified["boot"]["chain_config"].as_str().unwrap())), 0o600, "built 0600");
    let bid = e.iter().find_map(|v| v["data"]["target_blockchain_id"].as_str().map(str::to_string)).unwrap();
    let installed = dir.path().join("chain-configs").join(&bid).join("config.json");
    assert_eq!(mode(&installed), 0o600, "installed 0600");
    assert_eq!(mode(installed.parent().unwrap()), 0o700, "its fresh directory 0700");
    assert_eq!(mode(std::path::Path::new(verified["boot"]["genesis"].as_str().unwrap())), 0o644, "the shared genesis is not secret");

    // A group/world-writable work dir: refused at ARM, before anything freezes.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    std::fs::create_dir_all(dir.path().join("upstream-work")).unwrap();
    std::fs::set_permissions(dir.path().join("upstream-work"), std::fs::Permissions::from_mode(0o777)).unwrap();
    let ops = upstream_ops(dir.path());
    let _ = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("writable by group/others"), "{text}");
    assert!(!text.contains("\"FROZEN\""), "nothing froze");

    // Signer material in the SHARED genesis base: refused.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    let mut g: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.path().join("genesis-base.json")).unwrap()).unwrap();
    g["note"] = serde_json::json!(pulse_cutover::keys::format_private_k1(&TEST_PRODUCER_SECRET));
    std::fs::write(dir.path().join("genesis-base.json"), g.to_string()).unwrap();
    let ops = upstream_ops(dir.path());
    let _ = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("genesis_base contains a private key"), "{text}");
    assert!(!text.contains("\"FROZEN\""));
}

#[test]
fn rc21_verified_needs_a_compare_a_state_root_and_the_exact_known_difference() {
    // No compare_bin: never VERIFIED (it used to be "skipped").
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let text = std::fs::read_to_string(dir.path().join("fake-compare.sh")).unwrap();
    assert!(text.contains("dynamic_global_property"));
    let _ = upstream_test_config(dir.path(), 120); // writes ceremony-upstream.toml
    let toml = std::fs::read_to_string(dir.path().join("ceremony-upstream.toml")).unwrap().replace("compare_bin =", "# compare_bin =");
    std::fs::write(dir.path().join("ceremony-upstream.toml"), toml).unwrap();
    let cfg2 = Config::load(&dir.path().join("ceremony-upstream.toml")).unwrap();
    assert_eq!(run_machine(&cfg2, &MockOps::new(dir.path(), 110)), State::Aborted);
    let j = std::fs::read_to_string(&cfg2.journal_path).unwrap();
    assert!(j.contains("compare_bin is not configured") && !upstream_journal_has_verified(&j), "{j}");

    // A compare that exits 0 but covers only some tables: not a verification.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    write_script(&dir.path().join("fake-compare.sh"), "#!/bin/sh\necho 'table account: rows=1 sha256=aa'\nexit 0\n");
    let cfg = upstream_test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Aborted);
    let j = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(j.contains("did not report a matching line for every required table") && !upstream_journal_has_verified(&j));

    // No state root: not a verification.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    write_script(&dir.path().join("fake-fingerprint.sh"), "#!/bin/sh\necho 'revision 999'\necho 'table account bytes=10 sha256=aa55'\n");
    let cfg = upstream_test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Aborted);
    let j = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(j.contains("no usable state_root") && !upstream_journal_has_verified(&j));

    // global_property on the allowlist, but failing differently than the known 14-byte gap: refused.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    fake_compare_failing(dir.path(), &["global_property"]);
    let body = std::fs::read_to_string(dir.path().join("fake-compare.sh")).unwrap().replace("bytes=143", "bytes=150");
    write_script(&dir.path().join("fake-compare.sh"), &body);
    let cfg = upstream_ignite_config(dir.path(), "", r#"rehearsal_allow_compare_mismatch = ["global_property"]"#).unwrap();
    assert_eq!(run_machine(&cfg, &upstream_ops(dir.path())), State::Aborted);
    let j = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(j.contains("not the known difference") && !upstream_journal_has_verified(&j), "{j}");
}

#[test]
fn rc21_production_profile_is_mandatory_unless_the_ceremony_is_an_explicit_rehearsal() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    // A production-complete config loads without `rehearsal` and its ARM journals "met".
    let cfg = upstream_production_config(dir.path()).unwrap();
    assert!(cfg.production_problems().is_empty(), "{:?}", cfg.production_problems());
    // Each relaxation alone is refused at load without `rehearsal = true`.
    let path = dir.path().join("ceremony-upstream-ignite.toml");
    let good = std::fs::read_to_string(&path).unwrap();
    for (edit, needle) in [
        (good.replace("freeze_strategy = \"schedule_at_h\"\n", "allow_inexact_cut = true\nfreeze_strategy = \"schedule_at_h\"\n"), "allow_inexact_cut"),
        (good.replace("[target]\n", "[target]\nlive_sustain_secs = 0\n"), "live_sustain_secs = 0"),
        (good.replace("[target]\n", "[target]\npost_live_max_idle_secs = 0\n"), "post_live_max_idle_secs = 0"),
        (good.replace("[target]\n", "[target]\nrequire_lineage_check = false\n"), "require_lineage_check"),
        (good.replace("post_live_probe_cmd = \"probe-workload\"\n", ""), "post_live_probe_cmd"),
        (good.replace("on_abort = \"reopen-writes\"\n", ""), "hooks.on_abort"),
        (good.replace("on_freeze = \"freeze-writes\"\n", ""), "hooks.on_freeze"),
        (good.replace("freeze_strategy = \"schedule_at_h\"\n", ""), "schedule_at_h"),
        (good.replace("compare_bin = ", "# compare_bin = "), "compare_bin"),
        (good.replace("[source]\n", "[source]\nquiesce_cmd = \"q\"\n"), "quiesce_cmd"),
        (good.replace("[upstream]\n", "[upstream]\nrehearsal_allow_compare_mismatch = [\"global_property\"]\n"), "rehearsal_allow_compare_mismatch"),
    ] {
        let err = load_toml(dir.path(), "p.toml", &edit).unwrap_err();
        assert!(err.contains("PRODUCTION PROFILE") && err.contains(needle), "{needle}: {err}");
        // The same relaxation with an explicit rehearsal flag loads (and is listed as a relaxation).
        let r = load_toml(dir.path(), "r.toml", &edit.replacen("[ceremony]\n", "[ceremony]\nrehearsal = true\n", 1)).unwrap();
        assert!(r.production_problems().iter().any(|p| p.contains(needle)), "{needle}");
    }
    // The fork backend is held to the same profile (goldens, not a capture).
    let fork = good.replace("import_backend = \"upstream\"\n", "");
    assert!(load_toml(dir.path(), "f.toml", &fork).unwrap_err().contains("golden_roots"));
    // An explicit rehearsal is refused for a configured XPR mainnet chain_id.
    let m = good.replacen("[ceremony]\n", &format!("[ceremony]\nrehearsal = true\nchain_id = \"{MAINNET}\"\n"), 1);
    assert!(load_toml(dir.path(), "m.toml", &m).unwrap_err().contains("MAINNET"));
    // ...and at ARM when the source turns out to be mainnet: nothing freezes.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_ignite_config(dir.path(), "", "").unwrap();
    assert!(cfg.ceremony.rehearsal);
    let ops = upstream_ops(dir.path());
    *ops.chain_id.borrow_mut() = MAINNET.into();
    assert!(run_machine_result(&cfg, &ops).is_err());
    let e = journal_entries(&cfg);
    assert!(transition(&e, "FROZEN").is_none());
    assert!(e.iter().any(|v| v.to_string().contains("ceremony.rehearsal = true")));
}

#[test]
fn rc21_public_route_must_serve_the_targets_block_not_a_leftover_source_at_a_nearby_height() {
    // Review: the public-route check compared chain_id + nearby heights; a leftover source with the
    // same chain_id producing next to the target's height passed it.
    let dir = tempfile::tempdir().unwrap();
    let cfg = api_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.drift.set(5);
    ops.public_stale_source.set(true);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED") && err.contains("public URL did not serve the target"), "{err}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("public_route_check_last") && text.contains("5e"), "the differing block ids are journaled");
    // The healthy path journals the common block it proved.
    let dir = tempfile::tempdir().unwrap();
    let cfg = api_test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    ops.drift.set(5);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let e = journal_entries(&cfg);
    let flipped = &transition(&e, "FLIPPED").unwrap()["data"]["health"];
    assert!(flipped["common_block"]["height"].as_u64().unwrap() > flipped["common_block"]["above_cut"].as_u64().unwrap());
}

#[test]
fn rc21_producer_mode_writes_the_history_boundary_before_live() {
    // Dapp rehearsal: only hyperion mode wrote the boundary; a federator next to a producer had none.
    let dir = tempfile::tempdir().unwrap();
    let _ = test_config(dir.path(), 120);
    let path = dir.path().join("ceremony.toml");
    let text = std::fs::read_to_string(&path).unwrap().replacen("[ceremony]\n", &format!("[ceremony]\nboundary_path = \"{}/boundary.json\"\n", dir.path().display()), 1);
    std::fs::write(&path, text).unwrap();
    let cfg = Config::load(&path).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let b: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.path().join("boundary.json")).unwrap()).unwrap();
    let e = journal_entries(&cfg);
    let snap = &transition(&e, "SNAPSHOTTED").unwrap()["data"];
    assert_eq!(b["cut_block"], snap["cut_height"]);
    assert_eq!(b["cut_block_id"], snap["cut_block_id"]);
    assert_eq!(b["cut_time"], snap["last_source_block_time"]);
    assert!(b["chain_id"].is_string() && b["target_chain_id"] == b["chain_id"]);
    let pos = |needle: &str| e.iter().position(|v| v.to_string().contains(needle)).unwrap();
    assert!(pos("history_boundary_staged") < pos("\"state\":\"LIVE\""), "written before LIVE");
    // A boundary_path whose directory does not exist is refused at ARM.
    let dir = tempfile::tempdir().unwrap();
    let _ = test_config(dir.path(), 120);
    let path = dir.path().join("ceremony.toml");
    let text = std::fs::read_to_string(&path).unwrap().replacen("[ceremony]\n", &format!("[ceremony]\nboundary_path = \"{}/nope/boundary.json\"\n", dir.path().display()), 1);
    std::fs::write(&path, text).unwrap();
    let cfg = Config::load(&path).unwrap();
    let _ = run_machine_result(&cfg, &MockOps::new(dir.path(), 110));
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("its directory does not exist"));
}

#[test]
fn beacon_reports_a_config_short_of_the_production_profile_instead_of_refusing_it() {
    // rc.21 field finding: a beacon pointed at an rc.20-era ceremony config crash-looped because the
    // production profile refused the config at load. The beacon is read-only: it must keep reporting and
    // show the problems as a failing setup check; mutating commands still refuse the same file.
    let dir = tempfile::tempdir().unwrap();
    test_config(dir.path(), 120);
    let path = dir.path().join("ceremony.toml");
    let text = std::fs::read_to_string(&path).unwrap().replace("rehearsal = true", "rehearsal = false");
    std::fs::write(&path, &text).unwrap();
    let strict = Config::load(&path).err().expect("the test fixture must fall short of the production profile");
    assert!(strict.contains("PRODUCTION PROFILE"), "{strict}");
    let cfg = Config::load_for_report(&path).expect("the beacon loads it for reporting");
    let report = pulse_cutover::beacon::build_report(&cfg, "protonnz", "testnet");
    let check = report["checks"].as_array().unwrap().iter()
        .find(|c| c["name"] == "rehearsal_overrides").expect("a failing setup check names the problems");
    assert_eq!(check["ok"], false);
    assert!(check["detail"].as_str().unwrap().contains("NOT production-ready"), "{check}");
}

// ---- rc.23: fleet rehearsal on the upstream stack (2026-10-06) ------------------------------------------------

/// §4.5: the beacon judged every validator against Tahoe (network 5, public staking port), so a private
/// rehearsal network was permanently red. The network now comes from config (or the event); a private one on
/// an XPR chain is a labelled rehearsal override and refused for XPR mainnet.
#[test]
fn rc23_private_metal_network_is_configurable_labelled_and_refused_for_mainnet() {
    let dir = tempfile::tempdir().unwrap();
    test_config(dir.path(), 120);
    let base = std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap();
    let testnet = "71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd";
    let with = |chain: &str, net: &str| base.replace("freeze_height = 120", &format!("freeze_height = 120\nchain_id = \"{chain}\""))
        .replace("quorum_timeout_secs = 60", &format!("quorum_timeout_secs = 60\nmetal_network_id = {net}"));
    let cfg = load_toml(dir.path(), "private.toml", &with(testnet, "12345")).unwrap();
    assert_eq!(cfg.target.expected_metal_network(Some(testnet)), Some(12345));
    assert!(cfg.rehearsal_overrides().iter().any(|o| o.contains("metal_network_id = 12345")), "{:?}", cfg.rehearsal_overrides());
    // Tahoe for the testnet chain is no override at all.
    let cfg = load_toml(dir.path(), "tahoe.toml", &with(testnet, "5")).unwrap();
    assert!(cfg.rehearsal_overrides().is_empty());
    let mainnet = "384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0";
    let err = load_toml(dir.path(), "mainnet.toml", &with(mainnet, "12345")).unwrap_err();
    assert!(err.contains("MAINNET") && err.contains("metal_network_id"), "{err}");

    // An event pinning the network must match this node's.
    use pulse_cutover::coord::validate_event;
    let cfg = load_toml(dir.path(), "private2.toml", &with(testnet, "12345")).unwrap();
    let ev = |n: u64| serde_json::json!({"type": "event", "network": "rehearsal", "chain_id": testnet, "h": 100_000, "metal_network_id": n, "roster": [{"producer": "bp1"}]});
    assert!(validate_event(&ev(12345), &cfg, "rehearsal", Some(10), 10).is_ok());
    assert!(validate_event(&ev(5), &cfg, "rehearsal", Some(10), 10).unwrap_err().contains("metal_network_id"));
}

/// A 5-member roster (quorum 4, as in run r4) with on_abort / on_halt hooks; this node is bp1 (its beacon
/// identity is in the ceremony config, so the resume guard can leave it out).
fn rc23_fleet_config(dir: &std::path::Path) -> Config {
    let base = std::fs::read_to_string({ coord_config(dir, 120, 0, 5); dir.join("ceremony-coord.toml") }).unwrap();
    let mut text = base.replace("fleet_quorum = 0", "fleet_quorum = 4\nreport_max_age_secs = 60")
        .replace("on_live = \"flip-gateway\"", "on_live = \"flip-gateway\"\non_abort = \"reopen-writes\"\non_halt = \"page-human\"");
    for p in ["bp1", "bp2", "bp3", "bp4", "bp5"] {
        text += &format!("\n[[coordination.roster]]\nproducer = \"{p}\"\n");
    }
    // A real beacon identity (url + token): its instance id is persisted next to the token, so the ceremony and
    // the beacon agree on it (the self-match in the resume guard).
    std::fs::write(dir.join("beacon.token"), "tok\n").unwrap();
    text += &format!("\n[beacon]\nurl = \"https://mc.example/api/report\"\nproducer = \"bp1\"\nnetwork = \"rehearsal\"\ntoken_file = \"{}/beacon.token\"\n", dir.display());
    load_toml(dir, "fleet.toml", &text).unwrap()
}

/// The relay view the node itself gets back: its own beacon (bp1) is always there unless a test lists bp1 itself,
/// as a real beacon reports for the event (rc.25 F6: a node whose own report is not visible may not resume).
fn rc23_status(members: &[(&str, serde_json::Value)]) -> serde_json::Value {
    let mut all: Vec<(&str, serde_json::Value)> = members.to_vec();
    if !all.iter().any(|(n, _)| *n == "bp1") {
        all.insert(0, ("bp1", serde_json::json!({"state": "ARMED"})));
    }
    serde_json::json!({"networks": [{"id": "rehearsal", "producers": all.iter().map(|(n, c)| serde_json::json!({"name": n,
        "beacons": [{"age_ms": 1000, "report": {"coord": {"event_id": "e1"}, "ceremony": c}}]})).collect::<Vec<_>>()}]})
}

/// Run r4: one BP lost the relay before chain creation; the other four ignited. At its fleet timeout it ABORTED and
/// resumed the old chain, which took 33 writes after H. rc.23: unknown is not "no": it is STRANDED, the source stays
/// paused, writes stay frozen (no on_abort), on_halt pages a human, and `run` / `rollback` refuse until the fleet
/// view proves no commit or an operator records a fleet-wide decision.
#[test]
fn rc23_r4_isolated_producer_is_stranded_not_resumed() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    ops.coord_down.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert_eq!(ops.resumes.get(), 0, "the old chain is NOT resumed");
    assert!(ops.paused.get(), "the source producer stays paused");
    let hooks = ops.hooks.borrow().clone();
    assert!(!hooks.iter().any(|h| h == "reopen-writes"), "writes stay frozen: {hooks:?}");
    assert!(hooks.iter().any(|h| h == "page-human"), "on_halt pages a human");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("fleet did not reach verified agreement") && text.contains("refusing to resume the source"), "{text}");
    // Sealed: a re-run refuses; the beacon reports STRANDED.
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("STRANDED") && err.contains("join"), "{err}");
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["state"], "STRANDED");
    // The operator's rollback re-checks the fleet: the relay is still down → refused, nothing changed.
    let (j, rec) = open_journal(&cfg.journal_path);
    let err = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap_err();
    assert!(err.starts_with("refusing") && err.contains("STRANDED"), "{err}");
    assert_eq!(ops.resumes.get(), 0);
    // Once every peer reports a state before chain creation, the same rollback resumes the old chain.
    ops.coord_down.set(false);
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "FROZEN"})),
        ("bp4", serde_json::json!({"state": "SNAPSHOTTED"})), ("bp5", serde_json::json!({"state": "ARMED"}))]));
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap();
    assert_eq!(out.state, State::Aborted);
    assert!(out.failed.is_empty(), "{:?}", out.failed);
    assert_eq!(ops.resumes.get(), 1);
}

#[test]
fn rc23_a_peer_past_creation_strands_and_force_stranded_is_an_operator_override() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    // The relay answers, but the gate is short (only bp2 past VERIFIED, other evidence) and bp2 has ignited.
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "IGNITED", "ignition_started": true})),
        ("bp3", serde_json::json!({"state": "VERIFIED"})), ("bp4", serde_json::json!({"state": "VERIFIED"})), ("bp5", serde_json::json!({"state": "VERIFIED"}))]));
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("bp2 was reported past chain creation or ignition for this event (IGNITED; a beacon report)"), "{text}");
    assert_eq!(ops.resumes.get(), 0);
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap_err().contains("bp2 was reported past chain creation"));
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true).unwrap();
    assert_eq!(out.state, State::Aborted);
    assert_eq!(ops.resumes.get(), 1, "--force-stranded: the operator's fleet-wide decision resumes the old chain");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("overridden by the operator (--force-stranded --i-understand)") && text.contains("overridden_fleet_view")
        && text.contains("bp2 was reported past chain creation"), "review #10: the overridden view is journaled");
}

#[test]
fn rc23_signed_abort_resumes_only_with_every_member_accounted_for() {
    // Review #2: the old version of this test resumed with 3 of 4 peers missing from the relay (and its comment
    // claimed the relay was unreachable). A signed abort now needs every roster member to have some report for
    // the event, none past creation (including the relay's high-water mark).
    let abort = serde_json::json!({"abort": signed(serde_json::json!({"type": "abort", "network": "rehearsal", "event_id": "e1"}))});
    let peers = |states: &[(&str, &str)]| rc23_status(&states.iter().map(|(p, st)| (*p, serde_json::json!({"state": st}))).collect::<Vec<_>>());
    let run = |doc: serde_json::Value| {
        let d = tempfile::tempdir().unwrap();
        let cfg = rc23_fleet_config(d.path());
        let ops = MockOps::new(d.path(), 118); // freezes at the first ARMED poll: the abort is seen after the freeze
        *ops.coord_doc.borrow_mut() = Some(abort.clone());
        *ops.status_doc.borrow_mut() = Some(doc);
        let st = run_machine(&cfg, &ops);
        assert!(ops.hooks.borrow().iter().any(|h| h == "freeze-writes"), "the abort came after the freeze");
        (st, ops.resumes.get(), std::fs::read_to_string(&cfg.journal_path).unwrap())
    };
    // Every member accounted for (a stale one is accepted under a signed abort), nobody past creation: resumed.
    let (st, resumes, text) = run(peers(&[("bp2", "VERIFIED"), ("bp3", "VERIFIED"), ("bp4", "FROZEN"), ("bp5", "VERIFIED")]));
    assert_eq!(st, State::Aborted, "{text}");
    assert!(resumes >= 1);
    assert!(text.contains("signed coordinator abort, every member accounted for"));
    // Three of four peers missing: unknown → STRANDED.
    let (st, resumes, text) = run(peers(&[("bp2", "VERIFIED")]));
    assert_eq!((st, resumes), (State::Stranded, 0));
    assert!(text.contains("bp3 has no report"), "{text}");
    // A peer past creation.
    let (st, _, text) = run(peers(&[("bp2", "HALTED"), ("bp3", "VERIFIED"), ("bp4", "VERIFIED"), ("bp5", "VERIFIED")]));
    assert_eq!(st, State::Stranded);
    assert!(text.contains("bp2 was reported past chain creation"), "{text}");
    // A peer whose IGNITED entry was replaced by a new silent-instance report: the relay's high-water mark still
    // shows it past creation.
    let mut doc = peers(&[("bp2", "VERIFIED"), ("bp3", "VERIFIED"), ("bp4", "VERIFIED"), ("bp5", "VERIFIED")]);
    doc["networks"][0]["producers"][1]["event_max"] = serde_json::json!({"e1": {"past_create": true, "state": "IGNITED"}});
    let (st, _, text) = run(doc);
    assert_eq!(st, State::Stranded);
    assert!(text.contains("high-water mark"), "{text}");
}

/// Review #1/§6: a LOCAL abort after the freeze (here: burn-off transactions) while the peers are still before
/// VERIFIED. With quorum 4 of 5, the four peers can reach it without this node: STRANDED (the old code resumed,
/// and the peers ignited without it). Once enough peers aborted too, the same abort resumes.
#[test]
fn rc23_local_abort_after_the_freeze_strands_while_the_rest_can_reach_quorum() {
    let run = |states: &[&str]| {
        let d = tempfile::tempdir().unwrap();
        let cfg = rc23_fleet_config(d.path());
        let ops = MockOps::new(d.path(), 110);
        ops.burnoff_tx_per_block.set(1);
        *ops.status_doc.borrow_mut() = Some(rc23_status(&["bp2", "bp3", "bp4", "bp5"].iter().zip(states)
            .map(|(p, st)| (*p, serde_json::json!({"state": st}))).collect::<Vec<_>>()));
        let st = run_machine(&cfg, &ops);
        (st, ops.resumes.get(), std::fs::read_to_string(&cfg.journal_path).unwrap())
    };
    let (st, resumes, text) = run(&["FROZEN", "FROZEN", "SNAPSHOTTED", "FROZEN"]);
    assert_eq!((st, resumes), (State::Stranded, 0), "{text}");
    assert!(text.contains("transactions landed after the cut") && text.contains("can reach it without this node"));
    let (st, resumes, text) = run(&["ABORTED", "ABORTED", "FROZEN", "FROZEN"]);
    assert_eq!(st, State::Aborted, "{text}");
    assert!(resumes >= 1);
}

/// Review #3/§6 stale-self race: before resuming, the agent journals its abort intent and waits until the relay
/// shows its own report no longer VERIFIED (or the report would be too old for any gate), then re-runs the guard.
#[test]
fn rc23_resume_waits_for_its_own_verified_report_to_be_withdrawn_and_re_checks() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    ops.burnoff_tx_per_block.set(1);
    let me = pulse_cutover::beacon::instance_id(&cfg);
    let with_self = |self_state: serde_json::Value| {
        let mut doc = rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
            ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]);
        doc["networks"][0]["producers"].as_array_mut().unwrap().push(serde_json::json!({"name": "bp1", "beacons": [{"age_ms": 1000,
            "report": {"instance_id": me, "coord": {"event_id": "e1"}, "ceremony": self_state}}]}));
        doc
    };
    // The relay shows this node aborting: the wait ends at once, the guard passes again, resumed.
    *ops.status_doc.borrow_mut() = Some(with_self(serde_json::json!({"state": "FROZEN", "aborting": true})));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let intent = text.find(r#""abort_intent":true"#).expect("intent journaled");
    let withdrawn = text.find("the relay shows this node aborting").expect("withdrawal seen");
    let resumed = text.find(r#""rollback_step":"resume""#).expect("resumed");
    assert!(intent < withdrawn && withdrawn < resumed, "intent → withdrawn → second guard → resume");
    assert_eq!(text.matches(r#""resume_guard":"clear""#).count(), 2, "the guard ran twice");
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["aborting"], false, "cleared by the ABORTED transition");

    // While it waits, peers move on (two of them back to VERIFIED with the quorum reachable): the second guard strands.
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    ops.burnoff_tx_per_block.set(1);
    // Self never shows as withdrawn (still VERIFIED): the wait runs the whole report_max_age_secs; the fleet view
    // the second pass sees is the scripted one after the first relay read.
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED"})),
        ("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
    *ops.status_doc_after.borrow_mut() = Some((3, rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED"})), ("bp2", serde_json::json!({"state": "VERIFIED"})), ("bp3", serde_json::json!({"state": "VERIFIED"})),
        ("bp4", serde_json::json!({"state": "VERIFIED"})), ("bp5", serde_json::json!({"state": "VERIFIED"}))])));
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert_eq!(ops.resumes.get(), 0);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("too old for any gate") && text.contains("can reach it without this node"), "{text}");
}

/// An upstream (ignition-configured) ceremony in a 5-member roster (quorum 4); this node is bp1.
fn rc23_upstream_fleet(dir: &std::path::Path) -> Config {
    rc23_upstream_fleet_with(dir, "")
}

fn rc23_upstream_fleet_with(dir: &std::path::Path, extra_upstream: &str) -> Config {
    stage_fake_upstream_tools(dir, 0);
    let _ = upstream_ignite_config(dir, "", extra_upstream).unwrap();
    let path = dir.join("ceremony-upstream-ignite.toml");
    let mut text = format!("{}\non_halt = \"page-human\"\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{}\"]\nevent_id = \"e1\"\nevent_hash = \"{}\"\nfleet_quorum = 4\nfleet_timeout_secs = 5\n",
        std::fs::read_to_string(&path).unwrap(), hex::encode(coord_key().verifying_key().to_bytes()), mock_event_hash());
    for p in ["bp1", "bp2", "bp3", "bp4", "bp5"] {
        text += &format!("\n[[coordination.roster]]\nproducer = \"{p}\"\n");
    }
    text += "\n[beacon]\nurl = \"\"\nproducer = \"bp1\"\nnetwork = \"rehearsal\"\n";
    std::fs::write(&path, text).unwrap();
    Config::load(&path).unwrap()
}

const JOIN_BID: &str = "TargetChainAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn rc23_live_peers(evidence: &serde_json::Value, peers: &[&str]) -> serde_json::Value {
    let t = serde_json::json!({"blockchain_id": JOIN_BID, "subnet_id": "TargetSubnetBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "head": 140, "after_cut_id": "a1".repeat(32)});
    rc23_status(&peers.iter().map(|p| (*p, serde_json::json!({"state": "LIVE", "evidence": evidence, "target": t}))).collect::<Vec<_>>())
}

/// Run r4's missing path: the isolated BP (here: STRANDED, as rc.23 leaves it) joins the chain its peers run LIVE,
/// without creating one, after checking its own artifacts, the fleet's evidence and its source.
#[test]
fn rc23_join_tracks_the_live_quorums_chain_without_creating_one() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert!(!d.path().join("create-chain.calls").exists());
    let ours = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    assert!(ours["boot_genesis_sha256"].is_string() && ours["fingerprints_digest"].is_string());
    let join = |ops: &MockOps| {
        let (j, rec) = open_journal(&cfg.journal_path);
        Machine::new(&cfg, ops, j, rec).join("e1")
    };
    ops.coord_down.set(false);
    // Refused: wrong event; a quorum not LIVE; evidence that differs from ours.
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).join("e2").unwrap_err().contains("not e2"));
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&ours, &["bp2", "bp3", "bp4"]));
    assert!(join(&ops).unwrap_err().contains("quorum 4"));
    let mut other = ours.clone();
    other["boot_genesis_sha256"] = serde_json::json!("cd".repeat(32));
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&other, &["bp2", "bp3", "bp4", "bp5"]));
    assert!(join(&ops).unwrap_err().contains("boot_genesis_sha256"));
    assert_eq!(pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().state, Some(State::Stranded), "refusals change nothing");
    // Joined: tracks the fleet's chain, installs the chain config under ITS id, ignites, reaches LIVE.
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&ours, &["bp2", "bp3", "bp4", "bp5"]));
    assert_eq!(join(&ops).unwrap(), State::Live);
    assert!(!d.path().join("create-chain.calls").exists(), "a join never creates a chain");
    assert!(d.path().join("chain-configs").join(JOIN_BID).join("config.json").exists());
    assert!(ops.ignite_vars.borrow().iter().any(|(k, v)| k == "blockchain_id" && v == JOIN_BID));
    assert_eq!(ops.resumes.get(), 0, "the old chain was never resumed");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""side_effect":"join""#) && text.contains(r#""joined":true"#));
    let s = pulse_cutover::beacon::journal_summary(&cfg.journal_path);
    assert_eq!((s["state"].as_str(), s["joined"].as_bool()), (Some("LIVE"), Some(true)));
}

/// rc.25 F5 (rc.24 fleet run c3a): bp3's table compare broke, so its ceremony stopped before VERIFIED while the
/// others went LIVE, and `join` refused ("never reached VERIFIED"): no supported route onto the LIVE chain. `join
/// --reverify` re-runs the verification on its journaled cut snapshot and joins only on equal evidence.
#[test]
fn rc25_join_reverify_lets_a_node_whose_verification_failed_join_the_live_chain() {
    // The evidence a node that verified this cut journals (the LIVE quorum's): a probe in its own directory.
    let p = tempfile::tempdir().unwrap();
    let pcfg = rc23_upstream_fleet(p.path());
    let pops = upstream_ops(p.path());
    pops.coord_down.set(true);
    assert_eq!(run_machine(&pcfg, &pops), State::Stranded);
    let good = pulse_cutover::beacon::journal_summary(&pcfg.journal_path)["evidence"].clone();

    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    stage_fake_upstream_tools(d.path(), 1); // the compare fails: no VERIFIED
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    let st = run_machine(&cfg, &ops);
    assert!(matches!(st, State::Stranded | State::Aborted), "{st:?}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("upstream verification failed") && !text.contains(r#""to":"VERIFIED""#), "{text}");
    ops.coord_down.set(false);
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&good, &["bp2", "bp3", "bp4", "bp5"]));
    let join = |reverify: bool| {
        let (j, rec) = open_journal(&cfg.journal_path);
        Machine::new(&cfg, &ops, j, rec).join_with("e1", reverify)
    };
    // Without --reverify: refused, with the way out named.
    assert!(join(false).unwrap_err().contains("--reverify"));
    // --reverify while the compare is still broken: refused, nothing changed on either chain.
    let e = join(true).unwrap_err();
    assert!(e.contains("re-verification failed") && e.contains("Nothing was changed"), "{e}");
    assert_eq!(pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().state, Some(st));
    // --reverify against a LIVE quorum whose state differs: refused.
    stage_fake_upstream_tools(d.path(), 0);
    let mut other = good.clone();
    other["fingerprints_digest"] = serde_json::json!("cd".repeat(32));
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&other, &["bp2", "bp3", "bp4", "bp5"]));
    assert!(join(true).unwrap_err().contains("fingerprints_digest"));
    assert_eq!(pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().state, Some(st), "refusals change nothing");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(!text.contains(r#""side_effect":"join""#));
    // Review #3: a refused re-verification journals no evidence (replay recovers none, the beacon reports none), so
    // a plain join still refuses instead of treating the unaccepted result as VERIFIED.
    assert!(!text.contains(r#""reverified":true"#));
    assert!(pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().sha256.is_none());
    assert!(join(false).unwrap_err().contains("--reverify"));
    // A node that verified uses a plain join: --reverify would rewrite the artifacts its VERIFIED record binds.
    let (j, rec) = open_journal(&pcfg.journal_path);
    assert!(Machine::new(&pcfg, &pops, j, rec).join_with("e1", true).unwrap_err().contains("WITHOUT VERIFIED"));
    // Fixed compare, equal evidence: re-verified, joined, LIVE, never created a chain or resumed the old one.
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&good, &["bp2", "bp3", "bp4", "bp5"]));
    assert_eq!(join(true).unwrap(), State::Live);
    assert!(!d.path().join("create-chain.calls").exists(), "a join never creates a chain");
    assert!(ops.ignite_vars.borrow().iter().any(|(k, v)| k == "blockchain_id" && v == JOIN_BID));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""reverified":true"#) && text.contains(r#""side_effect":"join""#));
    let s = pulse_cutover::beacon::journal_summary(&cfg.journal_path);
    assert_eq!(s["evidence"]["fingerprints_digest"], good["fingerprints_digest"], "the beacon reports the re-verified evidence");
    assert_eq!(s["evidence"]["snapshot_sha256"], good["snapshot_sha256"]);
}

/// rc.26 F7 (fleet run d1): on PulseVM v1.0.0 every node verifies under the rehearsal compare allowlist (#109), so a
/// `--reverify` that refused the allowlist could never join a rehearsal. In a rehearsal it now joins when the LIVE
/// quorum verified under the SAME allowed set, and refuses when the sets differ.
#[test]
fn rc26_join_reverify_accepts_the_same_rehearsal_compare_allowlist_as_the_live_quorum() {
    const ALLOW: &str = r#"rehearsal_allow_compare_mismatch = ["contract_index_double", "global_property"]"#;
    let failing = ["contract_index_double", "global_property"];
    let p = tempfile::tempdir().unwrap();
    let pcfg = rc23_upstream_fleet_with(p.path(), ALLOW);
    fake_compare_failing(p.path(), &failing);
    let pops = upstream_ops(p.path());
    pops.coord_down.set(true);
    assert_eq!(run_machine(&pcfg, &pops), State::Stranded);
    let good = pulse_cutover::beacon::journal_summary(&pcfg.journal_path)["evidence"].clone();
    assert!(good["compare_allowed_digest"].is_string(), "the LIVE quorum's evidence carries the allowed set: {good}");

    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet_with(d.path(), ALLOW);
    stage_fake_upstream_tools(d.path(), 1); // broken (not an allowed table): no VERIFIED
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    let st = run_machine(&cfg, &ops);
    assert!(matches!(st, State::Stranded | State::Aborted), "{st:?}");
    ops.coord_down.set(false);
    stage_fake_upstream_tools(d.path(), 0);
    fake_compare_failing(d.path(), &failing); // fixed: now only the known, allowed differences
    let join = || {
        let (j, rec) = open_journal(&cfg.journal_path);
        Machine::new(&cfg, &ops, j, rec).join_with("e1", true)
    };
    // A quorum that verified WITHOUT the override (no allowed set in its evidence): a different verification, refused.
    let mut clean = good.clone();
    clean.as_object_mut().unwrap().remove("compare_allowed_digest");
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&clean, &["bp2", "bp3", "bp4", "bp5"]));
    assert!(join().unwrap_err().contains("compare_allowed_digest"));
    assert!(!std::fs::read_to_string(&cfg.journal_path).unwrap().contains(r#""reverified":true"#));
    // The same allowed set: joined, and the record says the override was used.
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&good, &["bp2", "bp3", "bp4", "bp5"]));
    assert_eq!(join().unwrap(), State::Live);
    let e = journal_entries(&cfg);
    let rec = e.iter().find(|v| v["data"]["reverified"].as_bool() == Some(true)).expect("reverified journaled");
    assert!(rec["data"]["table_compare"].as_str().unwrap().starts_with("MISMATCH ALLOWED BY REHEARSAL OVERRIDE"));
    assert_eq!(rec["data"]["compare_allowed_mismatch"], serde_json::json!(failing));
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"]["compare_allowed_digest"], good["compare_allowed_digest"]);
}

/// rc.26 review M1: a rehearsal config (here: rehearsal = true) is refused for a join whose journal recorded XPR
/// mainnet as the source chain, even when the config leaves chain_id to discovery (the load-time check cannot see it).
#[test]
fn rc26_join_refuses_a_rehearsal_config_on_a_mainnet_journal() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    assert!(cfg.ceremony.rehearsal, "the fleet fixture is a rehearsal config");
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    let (mut j, _) = open_journal(&cfg.journal_path);
    j.evidence(State::Stranded, serde_json::json!({"chain_id": pulse_cutover::config::XPR_MAINNET_CHAIN_ID})).unwrap();
    drop(j);
    ops.coord_down.set(false);
    let (j, rec) = open_journal(&cfg.journal_path);
    let e = Machine::new(&cfg, &ops, j, rec).join_with("e1", false).unwrap_err();
    assert!(e.contains("XPR MAINNET"), "{e}");
}

#[test]
fn rc23_join_refuses_a_source_that_moved_on_after_h() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    ops.coord_down.set(false);
    let ours = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&ours, &["bp2", "bp3", "bp4", "bp5"]));
    // r4: the isolated BP resumed and produced (with writes) after the cut.
    ops.head.set(ops.head.get() + 64);
    let (j, rec) = open_journal(&cfg.journal_path);
    let err = Machine::new(&cfg, &ops, j, rec).join("e1").unwrap_err();
    assert!(err.contains("past the journaled pause head"), "{err}");
    ops.head.set(ops.head.get() - 64);
    ops.burnoff_tx_per_block.set(1);
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).join("e1").unwrap_err().contains("transaction(s) on the source after H"));
    assert!(!ops.ignited.get());
}

/// Fleet runs r2–r6: every run ended with LIVE and HALTED BPs on ONE target chain, each halting on its own view
/// of a slow, gappy target. rc.23: while a quorum of the roster reports the same target chain (common block
/// after the cut, heads moving), a local symptom is waited out as DEGRADED; it halts when the fleet view stops
/// vouching, patience runs out, or the operator decides.
fn rc23_degraded_rig(d: &std::path::Path, peers_state: &str, patience: u64) -> (Config, MockOps) {
    rc23_degraded_rig_head(d, peers_state, patience, 200)
}

fn rc23_degraded_rig_head(d: &std::path::Path, peers_state: &str, patience: u64, peers_head: u64) -> (Config, MockOps) {
    let probe = tempfile::tempdir().unwrap();
    let _ = run_machine_result(&test_config(probe.path(), 120), &MockOps::new(probe.path(), 110));
    let ours = pulse_cutover::beacon::journal_summary(&probe.path().join("journal.jsonl"))["evidence"].clone();
    let base = std::fs::read_to_string({ rc23_fleet_config(d); d.join("fleet.toml") }).unwrap();
    let cfg = load_toml(d, "fleet-degraded.toml", &base.replace("report_max_age_secs = 60",
        &format!("report_max_age_secs = 60\ndegraded_patience_secs = {patience}\ndegraded_retry_secs = 1"))).unwrap();
    let ops = MockOps::new(d, 110);
    let t = serde_json::json!({"head": peers_head, "after_cut_id": "a1".repeat(32)});
    *ops.status_doc.borrow_mut() = Some(rc23_status(&["bp2", "bp3", "bp4", "bp5"].iter()
        .map(|p| (*p, serde_json::json!({"state": peers_state, "evidence": ours.clone(), "target": t.clone()}))).collect::<Vec<_>>()));
    (cfg, ops)
}

#[test]
fn rc23_a_gap_is_waited_out_while_the_fleet_is_live_on_the_same_chain() {
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig(d.path(), "LIVE", 900);
    ops.target_latency_ms.set(1000); // ~1 s per target poll
    ops.target_stall_after.set(5);
    ops.target_stall_until.set(40); // a ~35 s gap (live_max_gap 20 s), then blocks again
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""degraded":{"#) && text.contains("sustain: no new target block"), "{text}");
    assert!(text.contains(r#""degraded_cleared":true"#));
    assert!(!text.contains("HALTED"));
}

#[test]
fn rc23_a_failing_post_ignite_hook_is_retried_while_the_fleet_is_live() {
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig(d.path(), "IGNITED", 900);
    *ops.flaky_hook.borrow_mut() = Some(("resume-traffic".into(), 2));
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""attempts":3,"hook_retry_succeeded":"post_ignite""#), "{text}");
    // on_live too
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig(d.path(), "LIVE", 900);
    *ops.flaky_hook.borrow_mut() = Some(("flip-gateway".into(), 1));
    assert_eq!(run_machine(&cfg, &ops), State::Live);
}

#[test]
fn rc23_degraded_ends_in_a_halt_when_the_fleet_does_not_vouch_patience_runs_out_or_the_operator_decides() {
    // Peers ignited but none shows a block after the cut: the fleet does not vouch for the chain → halt, after the
    // short-of-quorum grace (rc.24 rehearsal fix: min(fleet_stall_secs, 120) s, journaled as degraded_fleet_short).
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig_head(d.path(), "IGNITED", 900, 120);
    *ops.flaky_hook.borrow_mut() = Some(("resume-traffic".into(), u32::MAX));
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("degraded_fleet_short") && text.contains("the fleet view does not vouch for the chain (120 s)"), "{text}");
    // A permanent stall with a live fleet: patience (30 s) runs out → halt.
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig(d.path(), "LIVE", 30);
    ops.target_latency_ms.set(1000);
    ops.target_stall_after.set(5);
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""degraded":{"#) && text.contains("patience exhausted"), "{text}");
    assert_eq!(ops.resumes.get(), 0, "never resumes the source after ignition");
    // The operator's explicit decision.
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig(d.path(), "LIVE", 900);
    *ops.flaky_hook.borrow_mut() = Some(("resume-traffic".into(), 99));
    std::fs::write(d.path().join("operator-halt"), "").unwrap();
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("operator decision"));
    // Without a coordinated event nothing changes: a gap halts at once.
    let d = tempfile::tempdir().unwrap();
    let cfg = test_config(d.path(), 120);
    let ops = MockOps::new(d.path(), 110);
    ops.target_latency_ms.set(1000);
    ops.target_stall_after.set(5);
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("HALTED"));
    assert!(!std::fs::read_to_string(&cfg.journal_path).unwrap().contains("degraded"));
}

/// Review #4/§6: a join from ABORTED (on_abort reopened writes on the old chain) re-runs on_freeze first.
#[test]
fn rc23_join_from_aborted_freezes_writes_again_before_anything_else() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    // The gate fails; two peers already aborted, so the quorum (4) is unreachable without this node: ABORTED.
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let reopened = ops.hooks.borrow().iter().rposition(|h| h == "reopen-writes").expect("on_abort reopened writes");
    // Later the others recovered and went LIVE (e.g. by join); this node's producer is not producing.
    ops.paused.set(true);
    let ours = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&ours, &["bp2", "bp3", "bp4", "bp5"]));
    let (j, rec) = open_journal(&cfg.journal_path);
    assert_eq!(Machine::new(&cfg, &ops, j, rec).join("e1").unwrap(), State::Live);
    let hooks = ops.hooks.borrow().clone();
    let refrozen = hooks.iter().rposition(|h| h == "freeze-writes").unwrap();
    assert!(refrozen > reopened, "on_freeze re-ran after on_abort: {hooks:?}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let refreeze = text.find(r#""join_refreeze""#).expect("journaled");
    assert!(refreeze < text.find(r#""join":{"#).unwrap(), "the re-freeze comes before the join checks are journaled");
    assert!(!d.path().join("create-chain.calls").exists());
}

/// Review #5/§6: a crash between the `join` record and the chain-id record: `join` resumes (re-reading the id from
/// the fleet) instead of refusing; a crash after the ids: `join` resumes from there.
#[test]
fn rc23_join_resumes_after_a_crash_between_its_journal_records() {
    for crash_after_ids in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let cfg = rc23_upstream_fleet(d.path());
        let ops = upstream_ops(d.path());
        ops.coord_down.set(true);
        assert_eq!(run_machine(&cfg, &ops), State::Stranded);
        ops.coord_down.set(false);
        let ours = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
        *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&ours, &["bp2", "bp3", "bp4", "bp5"]));
        {
            // What a join that died right after its point-of-no-return record left behind.
            let (mut j, _) = open_journal(&cfg.journal_path);
            j.evidence(State::Stranded, serde_json::json!({"side_effect": "join"})).unwrap();
            if crash_after_ids {
                j.evidence(State::Stranded, serde_json::json!({"target_blockchain_id": JOIN_BID, "target_subnet_id": "TargetSubnetBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"})).unwrap();
            }
        }
        // `run` stays sealed; `join` finishes the join.
        assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("STRANDED"));
        let (j, rec) = open_journal(&cfg.journal_path);
        assert_eq!(Machine::new(&cfg, &ops, j, rec).join("e1").unwrap(), State::Live, "crash_after_ids={crash_after_ids}");
        assert!(ops.ignite_vars.borrow().iter().any(|(k, v)| k == "blockchain_id" && v == JOIN_BID));
        assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains(r#""join_resumed":true"#));
        assert_eq!(ops.resumes.get(), 0);
        assert!(!d.path().join("create-chain.calls").exists());
    }
}

/// Review #7/§6: one transient relay error does not end a degraded wait (fleet reads are retried), and the
/// start of the wait is journaled so a resumed run keeps the same patience.
#[test]
fn rc23_degraded_survives_a_transient_relay_error_and_journals_its_start() {
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig(d.path(), "IGNITED", 900);
    *ops.flaky_hook.borrow_mut() = Some(("resume-traffic".into(), 1));
    // The hook's failure arms two failing relay reads: exactly the degraded check's reads.
    ops.fail_status_on_hook.borrow_mut().replace(("resume-traffic".into(), 2));
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""degraded_since_ms""#) && text.contains(r#""hook_retry_succeeded":"post_ignite""#), "{text}");
    assert!(ops.status_failures_served.get() >= 2, "the transient errors were really served");
    let rec = pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap();
    assert!(rec.degraded_since_ms.is_some(), "recovered on resume");
}

/// Review #10: `--force-stranded` alone is refused (exit 3, nothing changed); it needs `--i-understand`.
#[test]
fn rc23_force_stranded_needs_i_understand() {
    let d = tempfile::tempdir().unwrap();
    let _ = rc23_fleet_config(d.path());
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
        .args(["rollback", "--config"]).arg(d.path().join("fleet.toml")).arg("--force-stranded").output().unwrap();
    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--force-stranded --i-understand"));
    assert!(!d.path().join("journal.jsonl").exists(), "nothing was touched");
}

/// Re-check #2: after its own withdrawal is visible, the agent waits a beacon cycle (≥ 10 s) before the second
/// guard pass, so a peer that started chain creation just before (and reports it a cycle later) is seen.
#[test]
fn rc23_second_guard_pass_waits_a_beacon_cycle_and_sees_a_late_create_started() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    ops.burnoff_tx_per_block.set(1);
    let me = pulse_cutover::beacon::instance_id(&cfg);
    let doc = |bp4: serde_json::Value| {
        let mut doc = rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
            ("bp4", bp4), ("bp5", serde_json::json!({"state": "FROZEN"}))]);
        doc["networks"][0]["producers"].as_array_mut().unwrap().push(serde_json::json!({"name": "bp1", "beacons": [{"age_ms": 1000,
            "report": {"instance_id": me, "coord": {"event_id": "e1"}, "ceremony": {"state": "FROZEN", "aborting": true}}}]}));
        doc
    };
    *ops.status_doc.borrow_mut() = Some(doc(serde_json::json!({"state": "VERIFIED"})));
    // Polls: 1 = first guard, 2 = withdrawal seen; from poll 3 (after the settle) bp4 reports create_started.
    *ops.status_doc_after.borrow_mut() = Some((2, doc(serde_json::json!({"state": "VERIFIED", "create_started": true}))));
    let t0 = ops.now.get();
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert_eq!(ops.resumes.get(), 0);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""settle_before_second_guard_ms":10000"#), "{text}");
    assert!(text.contains("bp4 was reported past chain creation"), "{text}");
    assert!(ops.now.get() - t0 >= 10_000, "the settle really waited (mock clock)");
}

/// Re-check #3: a join from ABORTED that is refused AFTER re-running on_freeze says writes are frozen and what to
/// do; `rollback` then reopens them (re-runs on_abort) instead of reporting "already rolled back".
#[test]
fn rc23_join_refused_after_refreeze_says_writes_are_frozen_and_rollback_reopens_them() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    // The resumed old chain moved on: the join must refuse, after re-freezing writes.
    let ours = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&ours, &["bp2", "bp3", "bp4", "bp5"]));
    let (j, rec) = open_journal(&cfg.journal_path);
    let err = Machine::new(&cfg, &ops, j, rec).join("e1").unwrap_err();
    assert!(err.contains("past the journaled pause head") && err.contains("Writes are now FROZEN on the old chain")
        && err.contains("pulse-cutover rollback"), "{err}");
    assert_eq!(ops.hooks.borrow().last().map(String::as_str), Some("freeze-writes"));
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["rollback_complete"], false, "mission control sees it");
    // r30 round 9: the join started a new journal segment, so the earlier abort's completed rollback no longer covers
    // it; with the fleet LIVE, reopening writes on the old chain is an operator's fleet-wide decision: a plain rollback
    // refuses (nothing changed), --force-stranded reopens them.
    let (j, rec) = open_journal(&cfg.journal_path);
    let err = Machine::new(&cfg, &ops, j, rec).operator_rollback(false).err().unwrap_or_default();
    assert!(err.contains("refusing to roll back") && err.contains("--force-stranded"), "{err}");
    assert_eq!(ops.hooks.borrow().last().map(String::as_str), Some("freeze-writes"), "nothing changed");
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true).unwrap();
    assert!(!out.already && out.failed.is_empty(), "{out:?}");
    assert_eq!(ops.hooks.borrow().last().map(String::as_str), Some("reopen-writes"), "on_abort reopened writes");
    assert!(pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().aborted_rollback_complete);
}

/// Re-check #3: a join resumed after a crash uses the chain id in its journaled `join` record (the relay may be down).
#[test]
fn rc23_join_resume_uses_the_journaled_join_record_without_the_relay() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.evidence(State::Stranded, serde_json::json!({"join": {"event_id": "e1", "blockchain_id": JOIN_BID, "subnet_id": "TargetSubnetBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"}})).unwrap();
        j.evidence(State::Stranded, serde_json::json!({"side_effect": "join"})).unwrap();
    }
    // The relay stays down: the id comes from the journal.
    let (j, rec) = open_journal(&cfg.journal_path);
    assert_eq!(Machine::new(&cfg, &ops, j, rec).join("e1").unwrap(), State::Live);
    assert!(ops.ignite_vars.borrow().iter().any(|(k, v)| k == "blockchain_id" && v == JOIN_BID));
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("the journaled join record"));
}

/// Re-check #5: a roster that pins another instance id for this producer is warned about at load (and journaled at ARM).
#[test]
fn rc23_a_roster_pin_that_is_not_this_node_is_warned_about() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let mine = pulse_cutover::beacon::instance_id_readonly(&cfg).unwrap();
    assert_eq!(mine, pulse_cutover::beacon::instance_id(&cfg), "read-only and persisting agree");
    assert!(cfg.warnings().iter().all(|w| !w.contains("pins instance")));
    let text = std::fs::read_to_string(d.path().join("fleet.toml")).unwrap()
        .replacen("producer = \"bp1\"\n", &format!("producer = \"bp1\"\ninstance_id = \"{}\"\n", "0".repeat(32)), 1);
    let bad = load_toml(d.path(), "fleet-pin.toml", &text).unwrap();
    assert!(bad.warnings().iter().any(|w| w.contains("pins instance") && w.contains(&mine)), "{:?}", bad.warnings());
    let ok = load_toml(d.path(), "fleet-pin-ok.toml", &std::fs::read_to_string(d.path().join("fleet.toml")).unwrap()
        .replacen("producer = \"bp1\"\n", &format!("producer = \"bp1\"\ninstance_id = \"{mine}\"\n"), 1)).unwrap();
    assert!(ok.warnings().iter().all(|w| !w.contains("pins instance")));
}

/// rc.24 fleet rehearsal (c2): after `unhalt`, a re-run starts a NEW degraded patience; it must not inherit the
/// halted episode's start (the re-run halted at its first symptom with "patience exhausted").
#[test]
fn rc24_unhalt_restarts_degraded_patience() {
    let d = tempfile::tempdir().unwrap();
    let j = d.path().join("journal.jsonl");
    let lines = [
        r#"{"seq":0,"ts_ms":1,"ts":"t","kind":"transition","state":"IGNITED","data":{}}"#,
        r#"{"seq":1,"ts_ms":2,"ts":"t","kind":"evidence","state":"IGNITED","data":{"degraded_since_ms":1000}}"#,
        r#"{"seq":2,"ts_ms":3,"ts":"t","kind":"transition","state":"HALTED","data":{"halted_from":"IGNITED"}}"#,
    ];
    std::fs::write(&j, lines.join("\n") + "\n").unwrap();
    assert_eq!(pulse_cutover::journal::Journal::replay(&j).unwrap().degraded_since_ms, Some(1000));
    let mut more = lines.join("\n") + "\n";
    more.push_str(r#"{"seq":3,"ts_ms":4,"ts":"t","kind":"evidence","state":"HALTED","data":{"unhalted_by":"root","returning_to":"IGNITED"}}"#);
    more.push('\n');
    std::fs::write(&j, &more).unwrap();
    assert_eq!(pulse_cutover::journal::Journal::replay(&j).unwrap().degraded_since_ms, None, "patience restarts after unhalt");
    more.push_str(r#"{"seq":4,"ts_ms":5,"ts":"t","kind":"evidence","state":"IGNITED","data":{"degraded_since_ms":9000}}"#);
    more.push('\n');
    std::fs::write(&j, &more).unwrap();
    assert_eq!(pulse_cutover::journal::Journal::replay(&j).unwrap().degraded_since_ms, Some(9000));
}

/// rc.24 fleet rehearsal (c2/c3a, F2): a fleet view that is merely SHORT of a quorum (peers' nodes still at the cut)
/// is given a grace instead of ending the degraded wait at once; when the peers catch up the ceremony goes LIVE.
#[test]
fn rc24_a_short_of_quorum_fleet_view_is_waited_out_within_the_grace() {
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = rc23_degraded_rig_head(d.path(), "IGNITED", 900, 120);
    *ops.flaky_hook.borrow_mut() = Some(("resume-traffic".into(), 2));
    // Read 1 is the fleet gate; read 2 (the first degraded check) shows the peers at the cut (short of a quorum);
    // from read 3 they show blocks past it.
    let ours = ops.status_doc.borrow().clone().unwrap();
    let mut caught_up = ours.clone();
    for p in caught_up["networks"][0]["producers"].as_array_mut().unwrap() {
        p["beacons"][0]["report"]["ceremony"]["target"]["head"] = serde_json::json!(200);
    }
    *ops.status_doc_after.borrow_mut() = Some((2, caught_up));
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("degraded_fleet_short") && !text.contains("HALTED"), "{text}");
}

/// rc.29: the journal of an agent killed at VERIFIED (e.g. by `cutover.sh abort`): a stranded run's journal cut
/// before its first error record. The mock keeps the source paused, as the killed agent left it.
fn rc29_killed_at_verified(d: &std::path::Path, cfg: &Config, ops: &MockOps) {
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "IGNITED", "ignition_started": true})),
        ("bp3", serde_json::json!({"state": "VERIFIED"})), ("bp4", serde_json::json!({"state": "VERIFIED"})), ("bp5", serde_json::json!({"state": "VERIFIED"}))]));
    assert_eq!(run_machine(cfg, ops), State::Stranded);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let kept: Vec<&str> = text.lines().take_while(|l| !l.contains("\"kind\":\"error\"")).collect();
    assert!(kept.last().unwrap().contains("\"state\":\"VERIFIED\""), "{kept:?}");
    std::fs::write(&cfg.journal_path, kept.join("\n") + "\n").unwrap();
    let _ = d;
    assert!(ops.paused.get());
}

#[test]
fn rc29_operator_rollback_from_verified_runs_the_resume_guard() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    rc29_killed_at_verified(d.path(), &cfg, &ops);
    let (j, rec) = open_journal(&cfg.journal_path);
    assert_eq!(rec.state, Some(State::Verified));
    // bp2 ignited: a plain rollback must not resume the old chain. The node is stranded instead (sealed).
    let err = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap_err();
    assert!(err.starts_with("refusing") && err.contains("bp2 was reported past chain creation") && err.contains("STRANDED"), "{err}");
    assert_eq!(ops.resumes.get(), 0, "the old chain is NOT resumed");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes stay frozen");
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["state"], "STRANDED");
    // --force-stranded remains the operator's journaled override.
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true).unwrap();
    assert_eq!((out.state, ops.resumes.get()), (State::Aborted, 1));
}

#[test]
fn rc29_operator_rollback_from_verified_resumes_when_the_guard_passes() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    rc29_killed_at_verified(d.path(), &cfg, &ops);
    // Every peer before chain creation and the quorum unreachable without this node.
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED", "aborting": true})),
        ("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "VERIFIED"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap();
    assert_eq!((out.state, ops.resumes.get()), (State::Aborted, 1), "{:?}", out.notes);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("\"abort_intent\":true") && text.contains("own_report_withdrawn"), "two-pass guard: {text}");
}

#[test]
fn rc29_auto_rollback_off_still_runs_the_guard_and_keeps_writes_frozen() {
    let d = tempfile::tempdir().unwrap();
    let base = rc23_fleet_config(d.path());
    let _ = base;
    let text = std::fs::read_to_string(d.path().join("fleet.toml")).unwrap();
    let text = text.replacen("[target]\n", "[target]\nauto_rollback = false\n", 1);
    let cfg = load_toml(d.path(), "fleet-noauto.toml", &text).unwrap();
    assert!(!cfg.target.auto_rollback);
    let ops = MockOps::new(d.path(), 110);
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "IGNITED", "ignition_started": true})),
        ("bp3", serde_json::json!({"state": "VERIFIED"})), ("bp4", serde_json::json!({"state": "VERIFIED"})), ("bp5", serde_json::json!({"state": "VERIFIED"}))]));
    assert_eq!(run_machine(&cfg, &ops), State::Stranded, "auto_rollback = false no longer skips the guard");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "on_abort did not reopen writes");
    assert_eq!(ops.resumes.get(), 0);
}

#[test]
fn rc29_an_agent_abort_records_its_intent_before_resuming() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    // Peers aborted: the agent's own abort (fleet gate short) passes the guard and resumes.
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let intent = text.find("\"by\":\"agent_abort\"").expect("intent journaled");
    let resumed = text.find("\"rollback_step\":\"resume\"").expect("resume step journaled");
    assert!(intent < resumed, "intent before the first rollback step");
    // A crash right after the resume step: replay must refuse to carry on the ceremony.
    let kept: Vec<&str> = text.lines().take_while(|l| !l.contains("\"rollback_step\":\"resume\"")).chain(
        text.lines().filter(|l| l.contains("\"rollback_step\":\"resume\""))).collect();
    std::fs::write(&cfg.journal_path, kept.join("\n") + "\n").unwrap();
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("refusing to run") && err.contains("rollback_requested"), "{err}");
    // And `rollback` finishes it without re-running the guard (the source was already resumed).
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap();
    assert_eq!(out.state, State::Aborted);
}

#[test]
fn rc29_fork_backend_rehashes_the_staged_snapshot_before_ignition() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    // Crash at VERIFIED (before ignite_started), then another file lands at staged_path.
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let kept: Vec<&str> = text.lines().take_while(|l| !l.contains("ignite_started")).collect();
    assert!(kept.iter().any(|l| l.contains("\"kind\":\"transition\"") && l.contains("\"state\":\"VERIFIED\"")));
    std::fs::write(&cfg.journal_path, kept.join("\n") + "\n").unwrap();
    std::fs::write(&cfg.snapshot.staged_path, b"another ceremony's snapshot").unwrap();
    let pause_head = open_journal(&cfg.journal_path).1.head_at_pause.unwrap();
    let ops = MockOps::new(dir.path(), pause_head);
    ops.paused.set(true);
    let st = run_machine(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("staged snapshot does not match the artifact this ceremony verified"), "{st:?} {text}");
    assert!(!text.contains("ignite_started"), "never ignited");
}

fn rc29_noauto_config(d: &std::path::Path) -> Config {
    let _ = rc23_fleet_config(d);
    let text = std::fs::read_to_string(d.join("fleet.toml")).unwrap().replacen("[target]\n", "[target]\nauto_rollback = false\n", 1);
    load_toml(d, "fleet-noauto.toml", &text).unwrap()
}

#[test]
fn rc29_an_abort_before_snapshotted_keeps_writes_frozen_and_rollback_stays_guarded() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc29_noauto_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    ops.burnoff_tx_per_block.set(1); // aborts in the burn-off audit: after the pause, before SNAPSHOTTED
    let clean = rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]);
    *ops.status_doc.borrow_mut() = Some(clean.clone());
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(!text.contains("head_at_pause"), "no pause head journaled before SNAPSHOTTED");
    assert!(text.contains("on_abort_deferred"), "{text}");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes stay frozen: the source was not resumed");
    assert_eq!(ops.resumes.get(), 0);
    // A peer ignites later: the operator's plain rollback must not resume the old chain.
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "IGNITED", "ignition_started": true})),
        ("bp3", serde_json::json!({"state": "ABORTED"})), ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
    let (j, rec) = open_journal(&cfg.journal_path);
    let err = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap_err();
    assert!(err.contains("bp2 was reported past chain creation"), "{err}");
    assert_eq!(ops.resumes.get(), 0);
    // With every peer before creation it resumes, and only then reopens writes.
    *ops.status_doc.borrow_mut() = Some(clean);
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap();
    assert_eq!((out.state, ops.resumes.get()), (State::Aborted, 1), "{:?}", out.failed);
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"));
}

#[test]
fn rc29_a_crash_after_abort_intent_does_not_replay_as_a_ceremony_in_progress() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let kept: Vec<&str> = text.lines().take_while(|l| !l.contains("own_report_withdrawn")).collect();
    assert!(kept.last().unwrap().contains("abort_intent"), "{kept:?}");
    std::fs::write(&cfg.journal_path, kept.join("\n") + "\n").unwrap();
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("refusing to run") && err.contains("rollback_requested"), "{err}");
}

#[test]
fn rc29_a_strand_pauses_a_producer_that_is_still_producing() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    rc29_killed_at_verified(d.path(), &cfg, &ops);
    ops.paused.set(false); // e.g. an operator resumed it by hand, or the agent died before the pause
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).is_err());
    assert!(ops.paused.get(), "a STRANDED producer is paused");
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("\"source_pause_enforced\":true"));
}

#[test]
fn rc29_a_config_without_the_event_hash_cannot_run_but_can_roll_back() {
    // A config derived by rc.28 has no event_hash: it cannot tell this event from another payload signed under the
    // same id. `run` refuses before anything freezes; the config still loads, so `rollback`/`status` keep working.
    let d = tempfile::tempdir().unwrap();
    let _ = coord_config(d.path(), 120, 0, 60);
    let text = std::fs::read_to_string(d.path().join("ceremony-coord.toml")).unwrap()
        .replace(&format!("event_hash = \"{}\"\n", mock_event_hash()), "");
    let cfg = load_toml(d.path(), "nohash.toml", &text).unwrap();
    let ops = MockOps::new(d.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("has no event_hash") && err.contains("await"), "{err}");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"), "nothing frozen");
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).is_ok(), "rollback still works");
}

#[test]
fn rc29_a_verified_restart_with_the_producer_running_never_ignites() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    assert_eq!(run_machine(&cfg, &MockOps::new(dir.path(), 110)), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let kept: Vec<&str> = text.lines().take_while(|l| !l.contains("ignite_started")).collect();
    std::fs::write(&cfg.journal_path, kept.join("\n") + "\n").unwrap();
    // nodeos restarted while the agent was down: the producer is no longer paused.
    let pause_head = open_journal(&cfg.journal_path).1.head_at_pause.unwrap();
    let ops = MockOps::new(dir.path(), pause_head);
    let st = run_machine(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("refusing to create or ignite the target: the source producer is not paused"), "{st:?}");
    assert!(!text.contains("ignite_started"));
}

#[test]
fn rc29_a_strand_whose_pause_does_not_take_is_reported_unsealed() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    rc29_killed_at_verified(d.path(), &cfg, &ops);
    ops.paused.set(false);
    ops.pause_ignored.set(true);
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).is_err());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("STRANDED but UNSEALED") && text.contains("\"sealed\":false") && text.contains("\"source_pause_enforced\":false"), "{text}");
    // Every later run retries the pause.
    ops.pause_ignored.set(false);
    assert!(run_machine_result(&cfg, &ops).unwrap_err().starts_with("STRANDED"));
    assert!(ops.paused.get(), "the retry took");
}

#[test]
fn rc29_cancelling_a_rollback_intent_ends_the_withdrawal_on_the_beacon() {
    let d = tempfile::tempdir().unwrap();
    let cfg = test_config(d.path(), 120);
    let lines = [
        serde_json::json!({"seq": 0, "ts_ms": 1, "ts": "t", "kind": "transition", "state": "VERIFIED", "data": {}}),
        serde_json::json!({"seq": 1, "ts_ms": 2, "ts": "t", "kind": "evidence", "state": "VERIFIED", "data": {"rollback_requested": true, "by": "agent_abort"}}),
        serde_json::json!({"seq": 2, "ts_ms": 3, "ts": "t", "kind": "evidence", "state": "VERIFIED", "data": {"abort_intent": true}}),
    ];
    std::fs::write(&cfg.journal_path, lines.iter().map(|l| l.to_string()).collect::<Vec<_>>().join("\n") + "\n").unwrap();
    let s = pulse_cutover::beacon::journal_summary(&cfg.journal_path);
    assert_eq!((s["aborting"].as_bool(), s["rollback_pending"].as_bool()), (Some(true), Some(true)), "{s}");
    let mut text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    text += &(serde_json::json!({"seq": 3, "ts_ms": 4, "ts": "t", "kind": "evidence", "state": "VERIFIED", "data": {"rollback_intent_cancelled": true}}).to_string() + "\n");
    std::fs::write(&cfg.journal_path, text).unwrap();
    let s = pulse_cutover::beacon::journal_summary(&cfg.journal_path);
    assert_ne!(s["aborting"].as_bool(), Some(true), "{s}");
    assert_ne!(s["rollback_pending"].as_bool(), Some(true), "{s}");
}

#[test]
fn rc29_an_uninterrupted_run_refuses_creation_when_the_source_moved_past_the_pause_head() {
    let d = tempfile::tempdir().unwrap();
    let cfg = test_config(d.path(), 120);
    let ops = MockOps::new(d.path(), 110);
    // One block lands after the audited quiescence window (the mock's paused head moves once, late).
    ops.move_after_snapshot.set(true);
    let st = run_machine(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("the source head moved past the pause head"), "{st:?} {text}");
    assert!(!text.contains("ignite_started"));
}

#[test]
fn rc29_the_beacon_reports_an_unsealed_strand_until_a_pause_is_confirmed() {
    let d = tempfile::tempdir().unwrap();
    let cfg = test_config(d.path(), 120);
    let line = |seq: u64, kind: &str, state: &str, data: serde_json::Value| serde_json::json!({"seq": seq, "ts_ms": seq + 1, "ts": "t", "kind": kind, "state": state, "data": data}).to_string();
    let mut text = [line(0, "transition", "VERIFIED", serde_json::json!({})),
        line(1, "transition", "STRANDED", serde_json::json!({"sealed": false, "source_paused": false}))].join("\n") + "\n";
    std::fs::write(&cfg.journal_path, &text).unwrap();
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["sealed"], serde_json::json!(false));
    text += &(line(2, "evidence", "STRANDED", serde_json::json!({"source_pause_enforced": true})) + "\n");
    std::fs::write(&cfg.journal_path, &text).unwrap();
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["sealed"], serde_json::json!(true));
}

#[test]
fn rc29_a_hashless_stranded_run_still_retries_the_pause_and_the_latest_check_wins_on_the_beacon() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_fleet_config(d.path());
    let ops = MockOps::new(d.path(), 110);
    rc29_killed_at_verified(d.path(), &cfg, &ops);
    ops.paused.set(false);
    ops.pause_ignored.set(true);
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).is_err());
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["sealed"], serde_json::json!(false));
    // An older derived config (no event_hash) on that STRANDED journal: run refuses, but retries the pause first.
    let text = std::fs::read_to_string(d.path().join("fleet.toml")).unwrap()
        .replace(&format!("event_hash = \"{}\"\n", mock_event_hash()), "");
    let old = load_toml(d.path(), "fleet-old.toml", &text).unwrap();
    ops.pause_ignored.set(false);
    assert!(run_machine_result(&old, &ops).unwrap_err().starts_with("STRANDED"));
    assert!(ops.paused.get());
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["sealed"], serde_json::json!(true));
    // A later failed check unseals again.
    ops.paused.set(false);
    ops.pause_ignored.set(true);
    assert!(run_machine_result(&cfg, &ops).is_err());
    assert_eq!(pulse_cutover::beacon::journal_summary(&cfg.journal_path)["sealed"], serde_json::json!(false));
}


// ---------------------------------------------------------------- r30: shadow cutovers --

const SHADOW_KEY: &str = "PUB_K1_shadowkey1";

/// The production upstream config turned into a SHADOW of XPR mainnet on private Metal network 4242, coordinated on a
/// "shadow-*" network (a producer-mode shadow keeps the multi-party controls: signed event, roster, quorum 2).
fn shadow_config(dir: &std::path::Path, edit: impl Fn(String) -> String) -> Result<Config, String> {
    let _ = upstream_production_config(dir)?;
    let path = dir.join("ceremony-upstream-ignite.toml");
    let text = std::fs::read_to_string(&path).unwrap()
        .replace("[ceremony]\n", &format!("[ceremony]\nchain_id = \"{MAINNET}\"\nshadow = true\n"))
        .replace("[target]\n", "[target]\nmetal_network_id = 4242\n")
        + &format!("\n[shadow]\nlabel = \"shadow-test-a\"\nproducer_keys = [\"{SHADOW_KEY}\"]\n\
                    i_understand_signatures_are_valid_on_mainnet = true\n")
        + &shadow_coordination("shadow-net")
        + &format!("\n[beacon]\nurl = \"https://mc.example/api/report\"\nproducer = \"bp1\"\nnetwork = \"shadow-net\"\ntoken_file = \"{}/beacon.token\"\n",
                   dir.display());
    std::fs::write(dir.join("beacon.token"), "tok\n").unwrap();
    std::fs::write(dir.join("beacon.instance"), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n").unwrap();
    std::fs::write(&path, edit(text)).unwrap();
    Config::load(&path)
}

/// The signed event a shadow fleet runs from (on its own "shadow-net" board).
/// Mainnet-shaped (rc.30 D-10): it binds the source chain_id, H (the upstream fixtures cut at 120), and the config's
/// roster and quorum, because a coordinated shadow carries the mainnet chain_id and ARM verifies that binding.
fn shadow_event() -> serde_json::Value {
    signed(serde_json::json!({"type": "event", "network": "shadow-net", "event_id": "e1", "chain_id": MAINNET, "h": 120,
        "roster": [{"producer": "bp1", "instance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, {"producer": "bp2"}], "quorum": 2}))
}

fn shadow_coordination(network: &str) -> String {
    format!("\n[coordination]\nurl = \"http://mc\"\nnetwork = \"{network}\"\ncoordinator_keys = [\"{}\"]\nevent_id = \"e1\"\nevent_hash = \"{}\"\n\
             fleet_quorum = 2\nfleet_timeout_secs = 5\nreport_max_age_secs = 60\n\n[[coordination.roster]]\nproducer = \"bp1\"\ninstance_id = \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n\n\
             [[coordination.roster]]\nproducer = \"bp2\"\n",
        hex::encode(coord_key().verifying_key().to_bytes()), pulse_cutover::coord::payload_hash(&shadow_event()).unwrap())
}

/// A status doc on the shadow board in which bp1 (us) and bp2 report `ceremony`.
fn shadow_status(bp2: serde_json::Value) -> serde_json::Value {
    shadow_status2(serde_json::json!({"state": "ARMED"}), bp2)
}

fn shadow_status2(bp1: serde_json::Value, bp2: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"networks": [{"id": "shadow-net", "producers": [
        {"name": "bp1", "beacons": [{"age_ms": 1000, "report": {"instance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "coord": {"event_id": "e1"}, "ceremony": bp1}}]},
        {"name": "bp2", "beacons": [{"age_ms": 1000, "report": {"instance_id": "bb", "coord": {"event_id": "e1"}, "ceremony": bp2}}]}]}]})
}

fn shadow_ops(dir: &std::path::Path, keys: &[&str], network: Option<u64>) -> MockOps {
    let ops = upstream_ops(dir);
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    ops.shadow_network_id.set(network);
    let producers: Vec<serde_json::Value> = keys.iter().enumerate().map(|(i, k)| serde_json::json!({
        "producer_name": format!("bp{}", i + 1),
        "authority": [0, {"threshold": 1, "keys": [{"key": k, "weight": 1}]}]})).collect();
    *ops.shadow_schedule.borrow_mut() = Some(serde_json::json!({"active": {"version": 1, "producers": producers}}));
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"event": shadow_event()}));
    // Public mainnet answers, with its own (real) keys and its own blocks.
    *ops.public_mainnet.borrow_mut() = Some(serde_json::json!({"keys": ["PUB_K1_realproducerkey"], "same_blocks": false}));
    // The peer has aborted: a failed shadow may legitimately end ABORTED; a refused one must not resume anything.
    *ops.status_doc.borrow_mut() = Some(shadow_status(serde_json::json!({"state": "ABORTED"})));
    ops
}

#[test]
fn r30_shadow_config_is_explicit_and_private() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    assert!(shadow_config(dir.path(), |t| t).is_ok(), "{:?}", shadow_config(dir.path(), |t| t).err());
    for (edit, needle) in [
        (Box::new(|t: String| t.replace("shadow = true\n", "")) as Box<dyn Fn(String) -> String>, "ceremony.shadow is not true"),
        (Box::new(|t: String| t.split("\n[shadow]").next().unwrap().to_string()), "needs a [shadow] section"),
        (Box::new(|t: String| t.replace("metal_network_id = 4242", "metal_network_id = 1")), "public Metal network"),
        (Box::new(|t: String| t.replace("metal_network_id = 4242\n", "")), "PRIVATE Metal network"),
        (Box::new(|t: String| t.replace("valid_on_mainnet = true", "valid_on_mainnet = false")), "must be true"),
        (Box::new(|t: String| t.replace(&format!("[\"{SHADOW_KEY}\"]"), "[]")), "producer_keys is empty"),
        (Box::new(|t: String| t.replace("label = \"shadow-test-a\"", "label = \" \"")), "label is empty"),
        // shadow-fix #1: the network proof is bound to the target the ceremony ignites; no free-text endpoint.
        (Box::new(|t: String| t.replace("i_understand_signatures_are_valid_on_mainnet = true\n",
            "i_understand_signatures_are_valid_on_mainnet = true\nmetal_info_url = \"http://127.0.0.1:9999\"\n")), "metal_info_url"),
        (Box::new(|t: String| t.replace("rpc_url = \"http://127.0.0.1:9650/ext/bc/{blockchain_id}/rpc\"", "rpc_url = \"http://127.0.0.1:9650/rpc\"")),
         "not a metalgo /ext/ URL"),
        // shadow-fix (b): identity is read on the node whose producer gets paused.
        (Box::new(|t: String| t.replace("producer_api_url = \"http://mock\"", "producer_api_url = \"http://other-node\"")), "must be the same API base"),
        // shadow-fix #3: a producer-mode shadow keeps the multi-party controls...
        (Box::new(|t: String| t.split("\n[coordination]").next().unwrap().to_string()), "must be coordinated"),
        // ...on a shadow network (#4), never mainnet or a real board.
        (Box::new(|t: String| t.replace("network = \"shadow-net\"", "network = \"mainnet\"")), "starts with \"shadow\""),
        (Box::new(|t: String| t.replace("network = \"shadow-net\"", "network = \"xpr-mainnet\"")), "starts with \"shadow\""),
    ] {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let err = shadow_config(dir.path(), edit).unwrap_err();
        assert!(err.contains(needle), "{needle}: {err}");
    }
    // Never on a non-shadow board, never served publicly by the agent's flip.
    for net in ["mainnet", "production", "rehearsal"] {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let err = shadow_config(dir.path(), |t| t.replace("network = \"shadow-net\"\ntoken_file", &format!("network = \"{net}\"\ntoken_file"))).unwrap_err();
        assert!(err.contains("beacon.network") && err.contains("starts with \"shadow\""), "{net}: {err}");
    }
    assert!(pulse_cutover::shadow::is_private_url("http://10.0.0.5") && !pulse_cutover::shadow::is_private_url("https://api.example.org"));
    assert!(!pulse_cutover::shadow::is_private_url("http://public.example\\@127.0.0.1:8888/"), "ambiguous authority is public");
}

#[test]
fn r30_without_shadow_mainnet_refusals_are_unchanged() {
    // The same overrides the shadow may use stay refused for mainnet in a normal config.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let err = upstream_ignite_config(dir.path(), &format!("chain_id = \"{MAINNET}\""), r#"rehearsal_allow_compare_mismatch = ["global_property"]"#).unwrap_err();
    assert!(err.contains("MAINNET"), "{err}");
    // ...and with shadow = true they load (labelled), because the shadow identity is checked at run time.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t.replace("shadow = true\n", "shadow = true\nrehearsal = true\nrehearsal_allow_chain_id_change = true\n")).unwrap();
    assert!(cfg.is_shadow() && !cfg.mainnet_refusals_apply(Some(MAINNET)));
}

/// A refused shadow touches nothing: no resume, no hook, nothing frozen or created; the journal's rollback is complete
/// (an operator `rollback` afterwards resumes nothing either).
fn assert_shadow_refused_untouched(dir: &std::path::Path, cfg: &Config, ops: &MockOps, needle: &str) {
    let e = journal_entries(cfg);
    assert!(e.iter().any(|v| v.to_string().contains(needle)), "{needle}: {e:?}");
    let aborted = transition(&e, "ABORTED").expect("refused = ABORTED");
    assert_eq!(aborted["data"]["shadow_refused"], true, "{needle}");
    assert_eq!(ops.resumes.get(), 0, "{needle}: the source must not be resumed");
    assert!(!ops.hooks.borrow().iter().any(|h| h.contains("reopen-writes")), "{needle}: on_abort must not run: {:?}", ops.hooks.borrow());
    assert!(!dir.join("create-chain.calls").exists(), "{needle}: nothing created");
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(rec.aborted_rollback_complete, "{needle}: nothing left to roll back");
    let mut m = Machine::new(cfg, ops, j, rec);
    let _ = m.operator_rollback(false);
    assert_eq!(ops.resumes.get(), 0, "{needle}: a later operator rollback resumes nothing");
}

#[test]
fn r30_shadow_on_a_real_mainnet_node_refuses_before_anything_freezes() {
    type Setup = Box<dyn Fn(&MockOps)>;
    let cases: Vec<(&[&str], Option<u64>, Setup, &str)> = vec![
        (&["PUB_K1_realproducerkey"], Some(4242), Box::new(|_| {}), "not shadow keys"),
        (&[SHADOW_KEY], Some(1), Box::new(|_| {}), "reports network id 1"),
        (&[SHADOW_KEY], None, Box::new(|_| {}), "did not report a network id"),
        // shadow-fix #1: the declared keys ARE real mainnet keys (the source schedule "matches" them)...
        (&["PUB_K1_realproducerkey"], Some(4242), Box::new(|o: &MockOps| {
            *o.public_mainnet.borrow_mut() = Some(serde_json::json!({"keys": ["PUB_K1_realproducerkey"], "same_blocks": false}));
        }), "REAL XPR mainnet producer keys"),
        // ...the source's block at LIB is mainnet's own block (a real node)...
        (&[SHADOW_KEY], Some(4242), Box::new(|o: &MockOps| {
            *o.public_mainnet.borrow_mut() = Some(serde_json::json!({"keys": ["PUB_K1_realproducerkey"], "same_blocks": true}));
        }), "is XPR MAINNET's own block"),
        // ...and public mainnet cannot be consulted: fail closed.
        (&[SHADOW_KEY], Some(4242), Box::new(|o: &MockOps| { *o.public_mainnet.borrow_mut() = None; }), "could not be consulted"),
    ];
    for (keys, net, setup, needle) in cases {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let declared = if needle.starts_with("REAL") { keys[0] } else { SHADOW_KEY };
        let cfg = shadow_config(dir.path(), |t| t.replace(&format!("[\"{SHADOW_KEY}\"]"), &format!("[\"{declared}\"]"))).unwrap();
        let ops = shadow_ops(dir.path(), keys, net);
        setup(&ops);
        assert!(run_machine_result(&cfg, &ops).is_err(), "{needle}");
        let e = journal_entries(&cfg);
        assert!(transition(&e, "FROZEN").is_none(), "{needle}: nothing froze");
        assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes"));
        assert_shadow_refused_untouched(dir.path(), &cfg, &ops, needle);
    }
    // A schedule that cannot be read is not "fine".
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    *ops.shadow_schedule.borrow_mut() = None;
    assert!(run_machine_result(&cfg, &ops).is_err());
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_none());
    assert_shadow_refused_untouched(dir.path(), &cfg, &ops, "schedule");
}

/// shadow-fix (a): an identity failure AFTER the freeze (here: before chain creation) leaves the paused source paused
/// and runs no hook; one after ignition halts (sealed), never resumes.
#[test]
fn r30_shadow_identity_failure_after_the_freeze_touches_nothing_and_halts_after_ignition() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    // ARM (1) and the pre-freeze gate (2) see the private network; the re-proof before creation (3) does not.
    ops.metal_network_from_call.set(Some((3, Some(1))));
    let _ = run_machine_result(&cfg, &ops);
    let e = journal_entries(&cfg);
    assert!(transition(&e, "FROZEN").is_some(), "it froze before the network changed");
    assert!(e.iter().any(|v| v.to_string().contains("before chain creation / ignition")), "{e:?}");
    assert_shadow_refused_untouched(dir.path(), &cfg, &ops, "reports network id 1");

    // After ignition the target metalgo stops reporting the private network: HALTED, sealed, never resumed.
    let probe = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(probe.path(), 0);
    let pcfg = shadow_config(probe.path(), |t| t).unwrap();
    let pops = shadow_ops(probe.path(), &[SHADOW_KEY], Some(4242));
    *pops.status_doc.borrow_mut() = Some(shadow_status(serde_json::json!({"state": "ARMED"})));
    let _ = run_machine_result(&pcfg, &pops);
    let ours = pulse_cutover::beacon::journal_summary(&pcfg.journal_path)["evidence"].clone();
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    *ops.status_doc.borrow_mut() = Some(shadow_status2(serde_json::json!({"state": "VERIFIED", "evidence": ours.clone()}),
        serde_json::json!({"state": "VERIFIED", "evidence": ours})));
    ops.metal_network_from_call.set(Some((4, Some(1))));
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.starts_with("HALTED"), "{err}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("after ignition the target metalgo is not the configured private network"), "{text}");
    assert!(dir.path().join("create-chain.calls").exists());
    assert_eq!(ops.resumes.get(), 0, "never resumed after ignition");
}

#[test]
fn r30_shadow_goes_past_the_mainnet_ignite_refusal_labelled_everywhere() {
    // The peer's VERIFIED evidence must equal ours: take it from a probe run of the same shadow.
    let probe = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(probe.path(), 0);
    let pcfg = shadow_config(probe.path(), |t| t).unwrap();
    let pops = shadow_ops(probe.path(), &[SHADOW_KEY, SHADOW_KEY], Some(4242));
    *pops.status_doc.borrow_mut() = Some(shadow_status(serde_json::json!({"state": "ARMED"})));
    let _ = run_machine_result(&pcfg, &pops);
    let ours = pulse_cutover::beacon::journal_summary(&pcfg.journal_path)["evidence"].clone();
    assert!(ours.is_object(), "probe reached VERIFIED: {ours}");

    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY, SHADOW_KEY], Some(4242));
    *ops.status_doc.borrow_mut() = Some(shadow_status2(serde_json::json!({"state": "VERIFIED", "evidence": ours.clone()}),
        serde_json::json!({"state": "VERIFIED", "evidence": ours})));
    let st = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert_eq!(st, Ok(State::Live), "the shadow runs the full ceremony to LIVE: {text}");
    let e = journal_entries(&cfg);
    let armed = transition(&e, "ARMED").expect("armed");
    assert_eq!(armed["data"]["shadow"]["label"], "shadow-test-a");
    assert_eq!(armed["data"]["shadow"]["metal_network_id"], 4242);
    assert!(armed["data"]["shadow"]["producer_keys_sha256"].as_str().is_some_and(|h| h.len() == 64));
    assert!(text.contains("\"SHADOW\"") && text.contains("verified before chain creation / ignition"));
    assert!(upstream_journal_has_verified(&text));
    // The real-mainnet refusal became a journaled warning, only after this run's re-proof; the shadow created its target.
    assert!(text.find("verified before chain creation / ignition").unwrap() < text.find("shadow_ignite_pending").unwrap());
    assert!(text.contains("shadow_ignite_pending") && text.contains("TAPOS"));
    assert!(!text.contains("refused for XPR MAINNET"), "{text}");
    assert!(dir.path().join("create-chain.calls").exists(), "the shadow created its target chain");
    // The network was re-proven after ignition too (ARM, freeze, before creation, after ignition).
    assert!(ops.metal_calls.get() >= 4, "{}", ops.metal_calls.get());
    // The beacon shows it red and labelled.
    let report = pulse_cutover::beacon::build_report(&cfg, "bp1", "shadow-net");
    assert_eq!(report["shadow"]["label"], "shadow-test-a");
    assert!(report["checks"].as_array().unwrap().iter().any(|c| c["name"] == "shadow" && c["ok"] == false));
    // ...as a SETUP check: a shadow node's own label never excludes it from the shadow fleet's gate.
    assert!(!pulse_cutover::beacon::has_failing_health(&serde_json::json!({"checks": [{"name": "shadow", "ok": false}]})));
}

#[test]
fn r30_a_fresh_ceremony_refuses_to_arm_while_a_readiness_drill_is_running() {
    // Review: drill/ceremony exclusion ran one way only. A drill RUNNING on this box (its own process, same
    // start time) makes a fresh ceremony's ARM preflight refuse before anything freezes.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let drill_root = pulse_cutover::drill::drill_root(&cfg);
    std::fs::create_dir_all(&drill_root).unwrap();
    let me = std::process::id();
    std::fs::write(pulse_cutover::drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 99,
        "state": "RUNNING", "pid": me, "pid_start": pulse_cutover::ops::process_start(me)}).to_string()).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("readiness drill at H = 99 is RUNNING"), "{err}");
    let e = journal_entries(&cfg);
    assert!(transition(&e, "FROZEN").is_none(), "nothing froze");
    assert!(!ops.paused.get(), "the producer was never paused");

    // A stale record (the drill process is gone) is journaled, never blocking.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    std::fs::create_dir_all(pulse_cutover::drill::drill_root(&cfg)).unwrap();
    std::fs::write(pulse_cutover::drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 99,
        "state": "RUNNING", "pid": me, "pid_start": "another process"}).to_string()).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("readiness_drill_stale"));
}

#[test]
fn r30r3_a_fresh_ceremony_refuses_while_a_drill_under_another_journal_name_holds_the_drill_root() {
    // Round 3: configs sharing a journal directory lock different journal files. A drill under ANOTHER journal name
    // that has taken the drill root's lock but not yet published RUNNING (latest.json absent) used to be invisible to
    // the ceremony: now a fresh ARM needs that lock too, and refuses before anything freezes.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    assert!(!pulse_cutover::drill::latest_path(&cfg).exists());
    let held = pulse_cutover::journal::Journal::lock(&pulse_cutover::drill::root_lock_base(&cfg)).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("drill lock"), "{err}");
    let e = journal_entries(&cfg);
    assert!(transition(&e, "FROZEN").is_none(), "nothing froze");
    assert!(!ops.paused.get(), "the producer was never paused");
    drop(held);
    // Free: the ceremony takes the lock and runs.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
}

/// shadow-fix #2: the journal binds the shadow identity at ARM. A real journal can never take the shadow path (run, join,
/// rollback, unhalt), a shadow journal never the real one, and a shadow's identity never changes mid-ceremony.
#[test]
fn r30_shadow_identity_is_bound_to_the_journal() {
    // A REAL (non-shadow) mainnet ceremony armed and refused at the ignite gate; then the config is flipped to shadow.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let real = upstream_mainnet_coordinated_config(dir.path());
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED", "aborting": true})),
        ("bp2", serde_json::json!({"state": "ABORTED"}))]));
    let _ = run_machine_result(&real, &ops);
    let rec = Journal::replay(&real.journal_path).unwrap();
    assert_eq!(rec.armed_shadow, Some(serde_json::Value::Null), "a real ceremony's ARMED record binds 'not a shadow'");
    let shadow = shadow_config(dir.path(), |t| t.replace("journal_path", "journal_path_unused_marker")
        .replace("journal_path_unused_marker", "journal_path")).unwrap();
    // Point the shadow config at the real journal.
    let shadow_text = std::fs::read_to_string(dir.path().join("ceremony-upstream-ignite.toml")).unwrap();
    let real_journal = real.journal_path.display().to_string();
    let shadow = if shadow.journal_path == real.journal_path { shadow } else {
        load_toml(dir.path(), "shadow-on-real.toml", &shadow_text.lines().map(|l| if l.starts_with("journal_path") {
            format!("journal_path = \"{real_journal}\"") } else { l.to_string() }).collect::<Vec<_>>().join("\n")).unwrap()
    };
    assert_eq!(shadow.journal_path, real.journal_path);
    let sops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    let resumes = sops.resumes.get();
    let err = run_machine_result(&shadow, &sops).unwrap_err();
    assert!(err.contains("armed as a REAL ceremony"), "{err}");
    let (j, rec) = open_journal(&shadow.journal_path);
    assert!(Machine::new(&shadow, &sops, j, rec).join_with("e1", false).unwrap_err().contains("armed as a REAL ceremony"));
    let (j, rec) = open_journal(&shadow.journal_path);
    assert!(Machine::new(&shadow, &sops, j, rec).operator_rollback(false).err().unwrap_or_default().contains("armed as a REAL ceremony"));
    assert_eq!(sops.resumes.get(), resumes, "nothing resumed by the refused shadow config");
    // unhalt (the binary): seal the journal, then try to clear it with the shadow config.
    {
        let (mut j, _) = open_journal(&real.journal_path);
        j.transition(State::Halted, serde_json::json!({"halted_from": "VERIFIED", "reason": "test seal"})).unwrap();
    }
    let shadow_path = if dir.path().join("shadow-on-real.toml").exists() { dir.path().join("shadow-on-real.toml") }
        else { dir.path().join("ceremony-upstream-ignite.toml") };
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
        .args(["unhalt", "--config"]).arg(&shadow_path).arg("--i-understand").output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("armed as a REAL ceremony"), "{}", String::from_utf8_lossy(&out.stderr));

    // A SHADOW journal: the config dropped to a real one, or its label changed, is refused.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    ops.metal_network_from_call.set(Some((3, Some(1)))); // stops after the freeze, before creation
    let _ = run_machine_result(&cfg, &ops);
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.armed_shadow.as_ref().unwrap()["label"], "shadow-test-a");
    let relabelled = shadow_config(dir.path(), |t| t.replace("label = \"shadow-test-a\"", "label = \"shadow-test-b\"")).unwrap();
    assert!(run_machine_result(&relabelled, &ops).unwrap_err().contains("fixed at ARM"));
    let rekeyed = shadow_config(dir.path(), |t| t.replace(&format!("[\"{SHADOW_KEY}\"]"), "[\"PUB_K1_otherkey\"]")).unwrap();
    assert!(run_machine_result(&rekeyed, &ops).unwrap_err().contains("fixed at ARM"));
    // Round-3 F3: a coordinated shadow's signed-event binding is compared on resume like a real cut's: a quorum
    // lowered between a crash and the resume is refused (offline, nothing changed).
    assert_ne!(rec.state, Some(State::Armed), "the shadow got past ARM, so its binding was journaled");
    let lowered = shadow_config(dir.path(), |t| t.replace("fleet_quorum = 2", "fleet_quorum = 1")).unwrap();
    let err = run_machine_result(&lowered, &ops).unwrap_err();
    assert!(err.contains("authenticated at ARM") && err.contains("quorum"), "{err}");
    assert!(pulse_cutover::shadow::journal_mismatch(&real, rec.armed_shadow.as_ref()).unwrap().contains("no longer a shadow"));
}

// ------------------------------------------------------------------ round-2 review (r30) --

/// Keep the journal up to and including the first record `stop` matches (a crash right after it).
fn truncate_journal_after(cfg: &Config, stop: impl Fn(&serde_json::Value) -> bool) {
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let mut kept = vec![];
    for l in text.lines() {
        kept.push(l);
        if stop(&serde_json::from_str(l).unwrap()) {
            std::fs::write(&cfg.journal_path, kept.join("\n") + "\n").unwrap();
            return;
        }
    }
    panic!("no matching record to truncate after");
}

/// A shadow journal paused at VERIFIED: the full happy shadow run, cut right after its VERIFIED transition.
fn shadow_journal_at_verified(dir: &std::path::Path) -> Config {
    let probe = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(probe.path(), 0);
    let pcfg = shadow_config(probe.path(), |t| t).unwrap();
    let pops = shadow_ops(probe.path(), &[SHADOW_KEY], Some(4242));
    *pops.status_doc.borrow_mut() = Some(shadow_status(serde_json::json!({"state": "ARMED"})));
    let _ = run_machine_result(&pcfg, &pops);
    let ours = pulse_cutover::beacon::journal_summary(&pcfg.journal_path)["evidence"].clone();
    stage_fake_upstream_tools(dir, 0);
    let cfg = shadow_config(dir, |t| t).unwrap();
    let ops = shadow_ops(dir, &[SHADOW_KEY], Some(4242));
    *ops.status_doc.borrow_mut() = Some(shadow_status2(serde_json::json!({"state": "VERIFIED", "evidence": ours.clone()}),
        serde_json::json!({"state": "VERIFIED", "evidence": ours})));
    assert_eq!(run_machine_result(&cfg, &ops), Ok(State::Live));
    truncate_journal_after(&cfg, |v| v["kind"] == "transition" && v["state"] == "VERIFIED");
    cfg
}

/// Codex round 2, HIGH 1: the refusal is authoritative from its FIRST durable record. A crash right after it (before the
/// ABORTED and rollback_done records) must not reopen a rollback that resumes the source or runs on_abort, even if the
/// identity would pass now.
#[test]
fn r30r2_shadow_refusal_is_authoritative_from_its_first_record() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    ops.metal_network_from_call.set(Some((3, Some(1)))); // refused after the freeze, before creation
    let _ = run_machine_result(&cfg, &ops);
    assert_eq!(ops.resumes.get(), 0);
    truncate_journal_after(&cfg, |v| v["kind"] == "evidence" && v["data"]["shadow_refused"] == true);
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert!(rec.shadow_refused, "replay recovers the refusal from its first record");
    assert_ne!(rec.state, Some(State::Aborted), "the crash cut the ABORTED record off");
    // The identity would pass now: the refusal still stands.
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback(false).unwrap();
    assert!(out.notes.iter().any(|n| n.contains("nothing was resumed")), "{:?}", out.notes);
    assert_eq!(ops.resumes.get(), 0, "rollback resumes nothing");
    assert!(!ops.hooks.borrow().iter().any(|h| h.contains("reopen-writes")), "no on_abort: {:?}", ops.hooks.borrow());
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.state, Some(State::Aborted));
    assert!(rec.aborted_rollback_complete, "the refusal's records were finished, nothing rolled back");
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("identity refusal"), "{err}");
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).join_with("e1", false).unwrap_err().contains("identity refusal"));
    assert_eq!(ops.resumes.get(), 0);
    // A crash before even the ABORTED record, then `run`: same.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    ops.metal_network_from_call.set(Some((3, Some(1))));
    let _ = run_machine_result(&cfg, &ops);
    truncate_journal_after(&cfg, |v| v["kind"] == "error" && v["data"]["detail"]["shadow_refused"] == true);
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    assert!(run_machine_result(&cfg, &ops).is_err());
    assert_eq!(ops.resumes.get(), 0);
    assert!(!ops.hooks.borrow().iter().any(|h| h.contains("reopen-writes")));
    assert_eq!(Journal::replay(&cfg.journal_path).unwrap().state, Some(State::Aborted));
}

/// Codex round 2, HIGH 2: a shadow resumed at VERIFIED whose resume preflight fails for an ordinary reason reaches
/// abort() before step_verified re-proves the identity. With the source now looking real (retargeted), the abort must
/// refuse without resuming; with the identity intact it re-proves and carries on with the normal abort.
#[test]
fn r30r2_shadow_abort_at_verified_reproves_identity_before_resuming() {
    let poison = |dir: &std::path::Path| {
        let mut g: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.join("genesis-base.json")).unwrap()).unwrap();
        g["note"] = serde_json::json!(pulse_cutover::keys::format_private_k1(&TEST_PRODUCER_SECRET));
        std::fs::write(dir.join("genesis-base.json"), g.to_string()).unwrap();
    };
    // The source now signs with a REAL key (both URLs retargeted to a real producer): refused, untouched.
    let dir = tempfile::tempdir().unwrap();
    let cfg = shadow_journal_at_verified(dir.path());
    poison(dir.path());
    let ops = shadow_ops(dir.path(), &["PUB_K1_realproducerkey"], Some(4242));
    ops.paused.set(true);
    let _ = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("genesis_base contains a private key"), "the ordinary preflight failure: {text}");
    assert!(text.contains("SHADOW identity not re-proven"), "{text}");
    assert_eq!(ops.resumes.get(), 0, "an unproven shadow never resumes its source");
    assert!(!ops.hooks.borrow().iter().any(|h| h.contains("reopen-writes")));
    assert!(Journal::replay(&cfg.journal_path).unwrap().shadow_refused);
    // Identity intact: the abort re-proves it in this run, then takes the ordinary (guarded) path.
    let dir = tempfile::tempdir().unwrap();
    let cfg = shadow_journal_at_verified(dir.path());
    poison(dir.path());
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    ops.paused.set(true);
    let _ = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("re-proven before an abort"), "{text}");
    assert!(!Journal::replay(&cfg.journal_path).unwrap().shadow_refused);
}

/// Codex round 2, HIGH 4: the identity reads and the producer mutations must hit the same API base, path included.
#[test]
fn r30r2_shadow_source_urls_must_share_the_whole_api_base() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let err = shadow_config(dir.path(), |t| t
        .replace("rpc_url = \"http://mock\"", "rpc_url = \"http://mock/shadow\"")
        .replace("producer_api_url = \"http://mock\"", "producer_api_url = \"http://mock/mainnet\"")).unwrap_err();
    assert!(err.contains("must be the same API base"), "{err}");
}

/// Reviewer N5: the chain about to be ignited must exist on the proven private metalgo.
#[test]
fn r30r2_shadow_target_chain_must_exist_on_the_proven_metalgo() {
    let probe = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(probe.path(), 0);
    let pcfg = shadow_config(probe.path(), |t| t).unwrap();
    let pops = shadow_ops(probe.path(), &[SHADOW_KEY], Some(4242));
    *pops.status_doc.borrow_mut() = Some(shadow_status(serde_json::json!({"state": "ARMED"})));
    let _ = run_machine_result(&pcfg, &pops);
    let ours = pulse_cutover::beacon::journal_summary(&pcfg.journal_path)["evidence"].clone();
    for status in [Some("Unknown"), None] {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let cfg = shadow_config(dir.path(), |t| t).unwrap();
        let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
        *ops.status_doc.borrow_mut() = Some(shadow_status2(serde_json::json!({"state": "VERIFIED", "evidence": ours.clone()}),
            serde_json::json!({"state": "VERIFIED", "evidence": ours.clone()})));
        *ops.metal_chain_status.borrow_mut() = status.map(str::to_string);
        let err = run_machine_result(&cfg, &ops).unwrap_err();
        assert!(err.starts_with("HALTED"), "after chain creation the refusal halts: {err}");
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        assert!(text.contains("not on the proven private network"), "{text}");
        assert!(!text.contains("\"ignite_started\""), "never ignited: {text}");
        assert_eq!(ops.metal_chain_asked.borrow().len(), 1, "asked about the journaled chain");
        assert_eq!(ops.resumes.get(), 0);
    }
}

/// Reviewer N2: a lagging or malformed public endpoint never counts as a block comparison.
#[test]
fn r30r2_a_lagging_public_endpoint_proves_nothing() {
    for head in [serde_json::json!(105u64), serde_json::Value::Null] {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let cfg = shadow_config(dir.path(), |t| t).unwrap();
        let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
        // Mainnet's head just below the source's LIB (lagging), or no head at all: nothing compared, fail closed.
        *ops.public_mainnet.borrow_mut() = Some(serde_json::json!({"keys": ["PUB_K1_realproducerkey"], "same_blocks": false,
            "head": head}));
        let probs = pulse_cutover::shadow::public_mainnet_problems(&[SHADOW_KEY.to_string()], &ops);
        assert!(probs.iter().any(|p| p.contains("could not be consulted")), "{head}: {probs:?}");
        // ...and the whole proof refuses before anything freezes.
        assert!(run_machine_result(&cfg, &ops).is_err());
        assert!(transition(&journal_entries(&cfg), "FROZEN").is_none());
        assert_eq!(ops.resumes.get(), 0);
    }
}

/// Codex round 2, HIGH 3 + reviewer N1: the D-10 authentication cannot be skipped by a crash after ARMED, and a resumed
/// mainnet journal continues only with the event that was authenticated (offline comparison).
#[test]
fn r30r2_mainnet_event_binding_survives_crashes_and_config_edits() {
    // 1. Crash right after ARMED, before the authentication: the next run authenticates (a wrong event refuses).
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_mainnet_coordinated_config(dir.path());
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"event": mainnet_event(cfg.ceremony.freeze_height)}));
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED", "aborting": true})),
        ("bp2", serde_json::json!({"state": "ABORTED"}))]));
    let _ = run_machine_result(&cfg, &ops);
    let full = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(full.contains("\"mainnet_event_verified\":{") && full.contains("roster_sha256"), "binding journaled");
    truncate_journal_after(&cfg, |v| v["kind"] == "transition" && v["state"] == "ARMED");
    let ops2 = upstream_ops(dir.path());
    ops2.schedule_ok.set(true);
    *ops2.chain_id.borrow_mut() = MAINNET.into();
    *ops2.coord_doc.borrow_mut() = Some(serde_json::json!({"event": mainnet_event(cfg.ceremony.freeze_height + 1)}));
    assert!(run_machine_result(&cfg, &ops2).is_err());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("XPR MAINNET signed event"), "re-authenticated on resume: {text}");
    assert!(!text.contains("\"FROZEN\""), "nothing froze");

    // 2. Past ARMED without the binding (the record was never written): refused everywhere, nothing touched.
    std::fs::write(&cfg.journal_path, &full).unwrap();
    let no_binding: Vec<&str> = full.lines().filter(|l| !l.contains("mainnet_event_verified")).collect();
    let frozen_at = no_binding.iter().position(|l| l.contains("\"kind\":\"transition\"") && l.contains("\"FROZEN\"")).unwrap();
    std::fs::write(&cfg.journal_path, no_binding[..=frozen_at].join("\n") + "\n").unwrap();
    let ops3 = upstream_ops(dir.path());
    *ops3.chain_id.borrow_mut() = MAINNET.into();
    let err = run_machine_result(&cfg, &ops3).unwrap_err();
    assert!(err.contains("no `mainnet_event_verified`"), "{err}");
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops3, j, rec).operator_rollback(false).err().unwrap_or_default().contains("mainnet_event_verified"));
    assert_eq!(ops3.resumes.get(), 0);

    // 3. With the binding, a roster edited between the crash and the resume: refused (run, rollback, join, unhalt).
    let with_binding: Vec<&str> = full.lines().collect();
    let frozen_at = with_binding.iter().position(|l| l.contains("\"kind\":\"transition\"") && l.contains("\"FROZEN\"")).unwrap();
    std::fs::write(&cfg.journal_path, with_binding[..=frozen_at].join("\n") + "\n").unwrap();
    let text = std::fs::read_to_string(dir.path().join("mainnet-coord.toml")).unwrap()
        .replace("[[coordination.roster]]\nproducer = \"bp2\"\n", "[[coordination.roster]]\nproducer = \"bp2\"\n\n[[coordination.roster]]\nproducer = \"bp3\"\n");
    let edited = load_toml(dir.path(), "mainnet-coord-edited.toml", &text).unwrap();
    let ops4 = upstream_ops(dir.path());
    *ops4.chain_id.borrow_mut() = MAINNET.into();
    let err = run_machine_result(&edited, &ops4).unwrap_err();
    assert!(err.contains("not the XPR MAINNET event that was authenticated") && err.contains("roster_sha256"), "{err}");
    let (j, rec) = open_journal(&edited.journal_path);
    assert!(Machine::new(&edited, &ops4, j, rec).operator_rollback(false).err().unwrap_or_default().contains("authenticated"));
    let (j, rec) = open_journal(&edited.journal_path);
    assert!(Machine::new(&edited, &ops4, j, rec).join_with("e1", false).unwrap_err().contains("authenticated"));
    assert_eq!(ops4.resumes.get(), 0);
    // The unchanged config continues past the check (whatever it does next, it is not refused for the binding).
    let ops5 = upstream_ops(dir.path());
    *ops5.chain_id.borrow_mut() = MAINNET.into();
    let r = run_machine_result(&cfg, &ops5);
    assert!(!r.as_ref().err().is_some_and(|e| e.contains("authenticated") || e.contains("mainnet_event_verified")), "{r:?}");
}

/// A valid STRANDED shadow journal (VERIFIED, then stranded by its resume guard), for the round-3 recovery tests.
fn shadow_journal_stranded(dir: &std::path::Path) -> Config {
    let cfg = shadow_journal_at_verified(dir);
    let (mut j, _) = open_journal(&cfg.journal_path);
    j.transition(State::Stranded, serde_json::json!({"reason": "test: resume guard refused", "source_paused": true})).unwrap();
    cfg
}

/// Codex round 3, HIGH 1: a valid STRANDED shadow journal whose source URLs now point at a REAL producer (label, network
/// and keys unchanged) must not be touched by `run` (its pause retry), `rollback` (strand's pause / the resume) or `join`
/// (on_freeze, the pause): every source mutation waits for a fresh identity proof in this run.
#[test]
fn r30r3_shadow_recovery_never_touches_a_retargeted_source_before_the_proof() {
    type Entry = Box<dyn Fn(&Config, &MockOps)>;
    let entries: Vec<(&str, Entry)> = vec![
        ("run", Box::new(|cfg, ops| { let _ = run_machine_result(cfg, ops); })),
        ("rollback", Box::new(|cfg, ops| { let (j, rec) = open_journal(&cfg.journal_path); let _ = Machine::new(cfg, ops, j, rec).operator_rollback(false); })),
        ("rollback --force-stranded", Box::new(|cfg, ops| { let (j, rec) = open_journal(&cfg.journal_path); let _ = Machine::new(cfg, ops, j, rec).operator_rollback(true); })),
        ("join from ABORTED", Box::new(|cfg, ops| { let (j, rec) = open_journal(&cfg.journal_path); let _ = Machine::new(cfg, ops, j, rec).join_with("e1", false); })),
    ];
    for (name, entry) in entries {
        let dir = tempfile::tempdir().unwrap();
        let cfg = shadow_journal_stranded(dir.path());
        if name.starts_with("join") {
            // join's first mutation is re-running on_freeze, which happens from ABORTED (on_abort reopened writes).
            let (mut j, _) = open_journal(&cfg.journal_path);
            j.transition(State::Aborted, serde_json::json!({"reason": "test: aborted after the guard passed"})).unwrap();
            j.evidence(State::Aborted, serde_json::json!({"rollback_done": true})).unwrap();
        }
        // Both URLs retargeted to a real producer: it signs with a real key and is running (not paused).
        let ops = shadow_ops(dir.path(), &["PUB_K1_realproducerkey"], Some(4242));
        ops.paused.set(false);
        entry(&cfg, &ops);
        assert_eq!(ops.pauses.get(), 0, "{name}: the real producer was paused");
        assert_eq!(ops.resumes.get(), 0, "{name}: the real producer was resumed");
        assert!(ops.hooks.borrow().is_empty(), "{name}: a hook ran on an unproven shadow: {:?}", ops.hooks.borrow());
        let rec = Journal::replay(&cfg.journal_path).unwrap();
        assert!(rec.shadow_refused, "{name}: refused through shadow_refuse");
    }
    // A STRANDED join with the retargeted source: refused somewhere, but never after touching the source.
    let dir = tempfile::tempdir().unwrap();
    let cfg = shadow_journal_stranded(dir.path());
    let ops = shadow_ops(dir.path(), &["PUB_K1_realproducerkey"], Some(4242));
    ops.paused.set(false);
    let (j, rec) = open_journal(&cfg.journal_path);
    assert!(Machine::new(&cfg, &ops, j, rec).join_with("e1", false).is_err());
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0));
    assert!(ops.hooks.borrow().is_empty(), "{:?}", ops.hooks.borrow());
    // Identity intact: `run` on the STRANDED shadow proves it, then retries the pause as before.
    let dir = tempfile::tempdir().unwrap();
    let cfg = shadow_journal_stranded(dir.path());
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("STRANDED"), "{err}");
    assert!(ops.pauses.get() >= 1, "a proven shadow retries the pause");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("proven in this run before pausing the source producer"), "{text}");
    assert!(!Journal::replay(&cfg.journal_path).unwrap().shadow_refused);
}

/// Codex round 3, HIGH 2: a crash right after ARMED (before the D-10 authentication), then a resume with a config AND a
/// valid signed event for another H: the journal executes the ARMED record's H, so the authorization for the other H
/// must refuse before anything is scheduled or frozen.
#[test]
fn r30r3_mainnet_resume_cannot_authorize_another_h_than_the_journal_executes() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_mainnet_coordinated_config(dir.path());
    let h1 = cfg.ceremony.freeze_height;
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"event": mainnet_event(h1)}));
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED", "aborting": true})),
        ("bp2", serde_json::json!({"state": "ABORTED"}))]));
    let _ = run_machine_result(&cfg, &ops);
    truncate_journal_after(&cfg, |v| v["kind"] == "transition" && v["state"] == "ARMED");
    assert_eq!(Journal::replay(&cfg.journal_path).unwrap().resolved_h, Some(h1));
    // A second config (H2) with its own valid signed event.
    let h2 = h1 + 50;
    let hash1 = pulse_cutover::coord::payload_hash(&mainnet_event(h1)).unwrap();
    let hash2 = pulse_cutover::coord::payload_hash(&mainnet_event(h2)).unwrap();
    let text = std::fs::read_to_string(dir.path().join("mainnet-coord.toml")).unwrap()
        .replace(&format!("freeze_height = {h1}"), &format!("freeze_height = {h2}"))
        .replace(&hash1, &hash2);
    let other = load_toml(dir.path(), "mainnet-coord-h2.toml", &text).unwrap();
    assert_eq!(other.ceremony.freeze_height, h2);
    let ops2 = upstream_ops(dir.path());
    ops2.schedule_ok.set(true);
    *ops2.chain_id.borrow_mut() = MAINNET.into();
    *ops2.coord_doc.borrow_mut() = Some(serde_json::json!({"event": mainnet_event(h2)}));
    let err = run_machine_result(&other, &ops2).unwrap_err();
    assert!(err.contains("executes H = ") , "{err}");
    let e = journal_entries(&cfg);
    assert!(transition(&e, "FROZEN").is_none(), "nothing froze");
    assert!(!e.iter().any(|v| v["data"]["mainnet_event_verified"].is_object()), "no binding for H2 was journaled");
    assert!(!e.iter().any(|v| v["data"]["snapshot_scheduled_at"].is_number() || v.to_string().contains("snapshot_scheduled")),
        "nothing was scheduled");
    assert_eq!(ops2.pauses.get(), 0);
}

/// Reviewer round 3, F2: the "far ahead" rule is judged on the HIGHEST head among the answering public endpoints. One
/// stuck endpoint (a day behind) next to a healthy one within a day of the source proves nothing; only when every
/// answering endpoint is more than a day behind does the gap count.
#[test]
fn r30r3_far_ahead_is_judged_on_the_highest_public_head() {
    let apis = pulse_cutover::shadow::PUBLIC_XPR_MAINNET_APIS;
    let far = pulse_cutover::shadow::FAR_AHEAD_BLOCKS;
    let probe = |heads: serde_json::Value| {
        let dir = tempfile::tempdir().unwrap();
        stage_fake_upstream_tools(dir.path(), 0);
        let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
        ops.head.set(10_000_000);
        *ops.public_mainnet.borrow_mut() = Some(serde_json::json!({"keys": ["PUB_K1_realproducerkey"], "same_blocks": false,
            "heads": heads}));
        pulse_cutover::shadow::public_mainnet_problems(&[SHADOW_KEY.to_string()], &ops)
    };
    let src_lib = 10_000_000u64 - 1;
    // One stuck endpoint more than a day behind, the rest within a day: refused (nothing really compared).
    let mut heads = serde_json::Map::new();
    heads.insert(apis[0].into(), serde_json::json!(src_lib - far - 10));
    for a in &apis[1..] {
        heads.insert((*a).into(), serde_json::json!(src_lib - 100));
    }
    let probs = probe(serde_json::Value::Object(heads));
    assert!(probs.iter().any(|p| p.contains("could not be consulted")), "{probs:?}");
    // Every answering endpoint more than a day behind: the gap itself counts, the proof passes.
    let mut heads = serde_json::Map::new();
    for a in apis {
        heads.insert((*a).into(), serde_json::json!(src_lib - far - 10));
    }
    let probs = probe(serde_json::Value::Object(heads));
    assert!(probs.is_empty(), "{probs:?}");
    // One endpoint at or past the source with its own block, one stuck: the real comparison counts.
    let mut heads = serde_json::Map::new();
    heads.insert(apis[0].into(), serde_json::json!(src_lib - far - 10));
    heads.insert(apis[1].into(), serde_json::json!(src_lib + 5));
    let probs = probe(serde_json::Value::Object(heads));
    assert!(probs.is_empty(), "{probs:?}");
}

/// Round-3 review: every source-side mutation in the state machine goes through the shadow gate (`src_pause`,
/// `src_resume`, `hook`). A raw `self.ops.pause()` / `resume()` / `run_hook(` is allowed only on a line marked `ungated`
/// (the gate itself and the paging-only on_halt), so a new call site cannot skip the shadow identity proof.
/// Round 5 (Codex R5-7): the code of a Rust source with every comment and string/char literal blanked (newlines kept, so
/// line numbers stay), and every `#[cfg(test)] mod … { … }` block removed. Other `#[cfg(test)]` items stay scanned.
fn rust_code_mask(text: &str) -> String {
    let c: Vec<char> = text.chars().collect();
    let mut out: Vec<char> = Vec::with_capacity(c.len());
    let blank = |ch: char| if ch == '\n' { '\n' } else { ' ' };
    let mut i = 0;
    while i < c.len() {
        if c[i] == '/' && c.get(i + 1) == Some(&'/') {
            while i < c.len() && c[i] != '\n' { out.push(' '); i += 1; }
        } else if c[i] == '/' && c.get(i + 1) == Some(&'*') {
            let mut depth = 0;
            while i < c.len() {
                if c[i] == '/' && c.get(i + 1) == Some(&'*') { depth += 1; out.push(' '); out.push(' '); i += 2; continue; }
                if c[i] == '*' && c.get(i + 1) == Some(&'/') { depth -= 1; out.push(' '); out.push(' '); i += 2; if depth == 0 { break; } continue; }
                out.push(blank(c[i])); i += 1;
            }
        } else if c[i] == 'r' && (c.get(i + 1) == Some(&'"') || c.get(i + 1) == Some(&'#'))
            && (i == 0 || !(c[i - 1].is_alphanumeric() || c[i - 1] == '_')) {
            let mut j = i + 1;
            let mut hashes = 0;
            while c.get(j) == Some(&'#') { hashes += 1; j += 1; }
            if c.get(j) != Some(&'"') { out.push(c[i]); i += 1; continue; }
            out.push('r'); for _ in 0..hashes { out.push('#'); } out.push('"');
            j += 1;
            loop {
                if j >= c.len() { break; }
                if c[j] == '"' && (0..hashes).all(|k| c.get(j + 1 + k) == Some(&'#')) {
                    out.push('"'); for _ in 0..hashes { out.push('#'); } j += 1 + hashes; break;
                }
                out.push(blank(c[j])); j += 1;
            }
            i = j;
        } else if c[i] == '"' {
            out.push('"'); i += 1;
            while i < c.len() && c[i] != '"' {
                if c[i] == '\\' { out.push(' '); i += 1; if i < c.len() { out.push(blank(c[i])); i += 1; } continue; }
                out.push(blank(c[i])); i += 1;
            }
            if i < c.len() { out.push('"'); i += 1; }
        } else if c[i] == '\'' && (c.get(i + 1) == Some(&'\\') || c.get(i + 2) == Some(&'\'')) {
            out.push('\''); i += 1;
            while i < c.len() && c[i] != '\'' { if c[i] == '\\' { out.push(' '); i += 1; } if i < c.len() { out.push(' '); i += 1; } }
            if i < c.len() { out.push('\''); i += 1; }
        } else { out.push(c[i]); i += 1; }
    }
    let masked: String = out.into_iter().collect();
    // Remove #[cfg(test)] mod NAME { ... } blocks (braces are balanced: strings and comments are already blank).
    let re = regex::Regex::new(r"#\[cfg\(test\)\](\s*#\[[^\]]*\])*\s*(pub(\([^)]*\))?\s+)?mod\s+\w+\s*\{").unwrap();
    let mut b: Vec<char> = masked.chars().collect();
    let starts: Vec<(usize, usize)> = re.find_iter(&masked).map(|m| (masked[..m.start()].chars().count(), masked[..m.end()].chars().count())).collect();
    for (st, open_end) in starts.into_iter().rev() {
        let mut depth = 1;
        let mut k = open_end;
        while k < b.len() && depth > 0 { if b[k] == '{' { depth += 1 } else if b[k] == '}' { depth -= 1 } k += 1; }
        for x in b.iter_mut().take(k).skip(st) { if *x != '\n' { *x = ' ' } }
    }
    b.into_iter().collect()
}

/// Every mutating call outside test modules, as (line number, original line), unless that line carries
/// `// ungated: <reason of at least 10 chars>`. Mutating: the ChainOps calls that change the source, the producer or the
/// target, and `run_shell` (loop's reset_cmd and every other free-form shell command).
fn mutation_scan(text: &str) -> Vec<(usize, String)> {
    let code = rust_code_mask(text);
    let lines: Vec<&str> = text.lines().collect();
    let re = regex::Regex::new(r"(?:\.|::|\b)\s*(pause|resume|run_hook|ignite|create_snapshot|schedule_snapshot\w*|unschedule_snapshot\w*|run_shell)\s*\(").unwrap();
    let mut out = vec![];
    for m in re.captures_iter(&code) {
        let name = m.get(1).unwrap();
        let before = code[..m.get(0).unwrap().start()].trim_end();
        if before.ends_with("fn") { continue; } // a definition, not a call
        let n = code[..name.start()].matches('\n').count();
        let line = lines.get(n).copied().unwrap_or("");
        let marked = line.split("// ungated:").nth(1).is_some_and(|r| r.trim().len() >= 10);
        if !marked { out.push((n + 1, line.to_string())); }
    }
    out
}

#[test]
fn r30r3_every_source_mutation_goes_through_the_shadow_gate() {
    // Round-4 review (F3): scan EVERY src/*.rs (outside its #[cfg(test)] module) for EVERY mutating ChainOps call,
    // through any receiver (`self.ops.x(`, `ops.x(`, `o.x(` after a rebinding, `ChainOps::x(`). Each one must carry an
    // explicit `// ungated: <reason>` marker, or (src/drill.rs, owned by the drill's own guard) be listed exactly below.
    let files: [(&str, &str); 22] = [
        ("beacon.rs", include_str!("../src/beacon.rs")), ("config.rs", include_str!("../src/config.rs")),
        ("coord.rs", include_str!("../src/coord.rs")), ("doctor.rs", include_str!("../src/doctor.rs")),
        ("drill.rs", include_str!("../src/drill.rs")), ("fleet.rs", include_str!("../src/fleet.rs")),
        ("journal.rs", include_str!("../src/journal.rs")), ("keys.rs", include_str!("../src/keys.rs")),
        ("lib.rs", include_str!("../src/lib.rs")), ("live_watch.rs", include_str!("../src/live_watch.rs")),
        ("looper.rs", include_str!("../src/looper.rs")), ("machine.rs", include_str!("../src/machine.rs")),
        ("main.rs", include_str!("../src/main.rs")), ("ops.rs", include_str!("../src/ops.rs")),
        ("report.rs", include_str!("../src/report.rs")), ("sanitize.rs", include_str!("../src/sanitize.rs")),
        ("scan.rs", include_str!("../src/scan.rs")), ("secrets.rs", include_str!("../src/secrets.rs")),
        ("shadow.rs", include_str!("../src/shadow.rs")), ("state.rs", include_str!("../src/state.rs")),
        ("upstream.rs", include_str!("../src/upstream.rs")), ("verify.rs", include_str!("../src/verify.rs")),
    ];
    // The drill's own guarded calls (DrillGuard forwards only the request it scheduled; tests/drill.rs proves it).
    let drill_allowed = [
        "self.schedule_snapshot_request(height).map(|_| ())",
        "let id = self.inner.schedule_snapshot_request(height)?;",
        "self.inner.unschedule_snapshot_request(id, height)",
        "let rec = match ops.unschedule_snapshot_request(id, h) {",
        "let request_id = ops.schedule_snapshot_request(h).map_err(|e| format!(\"schedule_snapshot({h}) failed: {e}\"))?;",
    ];
    let scan = |name: &str, text: &str| -> Vec<String> {
        mutation_scan(text).into_iter()
            .filter(|(_, line)| !(name == "drill.rs" && drill_allowed.contains(&line.trim())))
            .map(|(n, line)| format!("src/{name}:{n}: {}", line.trim()))
            .collect()
    };
    let bad: Vec<String> = files.iter().flat_map(|(n, t)| scan(n, t)).collect();
    assert!(bad.is_empty(), "source mutation(s) without the shadow gate or an `// ungated: <reason>` marker:\n{}", bad.join("\n"));
    // Every src file is in the list (a new module cannot escape the scan).
    let on_disk = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/src")).unwrap()
        .filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).filter(|n| n.ends_with(".rs")).count();
    assert_eq!(on_disk, files.len(), "add the new src/*.rs file to this scan");
    // The scanner itself flags the shapes that used to escape it.
    for sneaky in ["let o = self.ops; o.resume();", "ChainOps::pause(self.ops)?;", "self.ops.unschedule_snapshot_request(1, 2)?;",
                   "ops.ignite()?;", "self.ops.run_hook(\"x\") // ungated: no",
                   // round 5 (Codex R5-7): a call split across lines, and production code after a NON-module
                   // #[cfg(test)] item (the old scan stopped at the first #[cfg(test)] it saw).
                   "fn f() {\n    self.ops.pause\n        ()?;\n}",
                   "#[cfg(test)]\nthread_local! { static X: u8 = 0; }\nfn f() { self.ops.resume().ok(); }",
                   "let out = crate::ops::run_shell(&cmd)?;"] {
        assert_eq!(scan("looper.rs", sneaky).len(), 1, "the scan must flag `{sneaky}`");
    }
    // ...while a #[cfg(test)] MODULE, comments and string literals are not code.
    for benign in ["#[cfg(test)]\nmod tests {\n    fn t() { ops.pause().unwrap(); }\n}",
                   "// self.ops.pause() in a comment\n/* ops.resume() */ let s = \"ops.ignite()\";",
                   "fn pause(&self) -> Result<(), String>;",
                   "self.ops.resume() // ungated: a reason long enough"] {
        assert!(scan("looper.rs", benign).is_empty(), "the scan must not flag `{benign}`");
    }
    // The gate bodies themselves are the only raw pause/resume/run_hook outside the on_halt pages.
    let m = files.iter().find(|(n, _)| *n == "machine.rs").unwrap().1;
    let raw = m.lines().filter(|l| (l.contains("self.ops.pause()") || l.contains("self.ops.resume()") || l.contains("self.ops.run_hook("))).count();
    assert_eq!(raw, 5, "exactly the three gate bodies and the two on_halt pages call pause/resume/run_hook directly");
}

/// Round-4 review (Codex R4-1): a proxy can route `/v1/chain/*` to the shadow node and `/v1/producer/*` to a real
/// producer behind an identical base URL. The identity proof is therefore also made THROUGH the producer API (its head
/// block must be the chain API's block at that height and not public mainnet's), with the rest of the proof and again
/// right before every pause/resume. A producer API that answers with a real mainnet block is refused; nothing is paused,
/// resumed or (before the freeze) hooked.
#[test]
fn r30r4_a_producer_api_routed_to_a_real_producer_is_refused_before_any_pause() {
    let mainnet_id = |n: u64| format!("{n:08x}{}", "ee".repeat(28)); // the mock's public mainnet block at n
    // Fresh run: the chain API is the proven shadow, the producer API is real mainnet's node.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    let head = ops.head.get();
    *ops.producer_head.borrow_mut() = Some(Some(mainnet_id(head)));
    let _ = run_machine_result(&cfg, &ops);
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0), "the real producer was touched");
    assert!(ops.hooks.borrow().is_empty(), "a hook ran with an unproven producer API: {:?}", ops.hooks.borrow());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("PRODUCER API serves XPR MAINNET's own block"), "{text}");
    assert!(text.contains("reach DIFFERENT nodes"), "{text}");
    assert!(Journal::replay(&cfg.journal_path).unwrap().shadow_refused);

    // The producer API cannot be read at all: fail closed, nothing touched.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    *ops.producer_head.borrow_mut() = Some(None);
    let _ = run_machine_result(&cfg, &ops);
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0));
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("did not report its head block id"));

    // Proven at ARM, then retargeted between the freeze and the pause: the per-pause re-check refuses the pause.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = shadow_config(dir.path(), |t| t).unwrap();
    let ops = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    *ops.producer_head_after_hook.borrow_mut() = Some(mainnet_id(130));
    let _ = run_machine_result(&cfg, &ops);
    assert!(!ops.hooks.borrow().is_empty(), "the freeze hook ran on the proven shadow");
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0), "the retargeted producer was paused or resumed");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("SHADOW producer API not bound to the proven shadow node, so pausing the source producer was refused"), "{text}");
    assert!(Journal::replay(&cfg.journal_path).unwrap().shadow_refused);
}

/// Round-4 review (Codex R4-7): a resumed ceremony skipped the drill check and only journaled a busy drill lock.
/// Resumed while still ARMED (nothing frozen yet) it now refuses next to a live drill like a fresh ARM; resumed AFTER
/// the freeze it is never refused (the cut continues) but stops the drill on this box first, and journals a loud
/// warning when the drill cannot be verified stopped.
#[cfg(unix)]
#[test]
fn r30r4_a_resumed_ceremony_handles_a_live_readiness_drill() {
    let write_latest = |cfg: &Config, v: serde_json::Value| {
        std::fs::create_dir_all(pulse_cutover::drill::drill_root(cfg)).unwrap();
        std::fs::write(pulse_cutover::drill::latest_path(cfg), v.to_string()).unwrap();
    };
    // A hand-built FROZEN journal has no scheduled-snapshot record, so the mock cut may end on its own (snapshot not at
    // H); what matters here is that nothing is refused or aborted FOR THE DRILL.
    let not_for_the_drill = |cfg: &Config| {
        for e in journal_entries(cfg) {
            if e["kind"] == "error" || (e["kind"] == "transition" && (e["state"] == "ABORTED" || e["state"] == "STRANDED")) {
                let s = e.to_string();
                assert!(!s.contains("readiness drill") && !s.contains("drill lock"), "refused for a drill: {s}");
            }
        }
    };
    let me = std::process::id();
    let live_me = serde_json::json!({"kind": "readiness_drill", "h": 99, "state": "RUNNING", "pid": me,
        "pid_start": pulse_cutover::ops::process_start(me)});

    // 1. Resumed at ARMED next to a live drill: refused before anything freezes.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"resolved_h": 120, "freeze_height": 120})).unwrap();
    }
    write_latest(&cfg, live_me.clone());
    let ops = MockOps::new(dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("readiness drill at H = 99 is RUNNING"), "{err}");
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_none(), "nothing froze");
    assert!(!ops.paused.get());

    // 2. Resumed AFTER the freeze with a drill whose only live part is its pipeline group: the group is stopped, the
    //    cut continues to LIVE, and the stop is journaled.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"resolved_h": 120, "freeze_height": 120})).unwrap();
        j.transition(State::Frozen, serde_json::json!({"freeze_at": 96, "declared_h": 120})).unwrap();
    }
    write_latest(&cfg, serde_json::json!({"kind": "readiness_drill", "h": 99, "state": "RUNNING", "pid": me,
        "pid_start": "another process"}));
    let script = dir.path().join("conv.sh");
    std::fs::write(&script, "#!/bin/sh\nsleep 30\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (pid, exited) = spawn_group(&script.display().to_string());
    std::thread::sleep(std::time::Duration::from_millis(300));
    std::fs::create_dir_all(pulse_cutover::drill::drill_dir(&cfg, 99)).unwrap();
    std::fs::write(pulse_cutover::drill::pgid_file(&cfg, 99), format!("{pid}\n{}\n", script.display())).unwrap();
    assert!(matches!(pulse_cutover::drill::activity(&cfg), pulse_cutover::drill::DrillActivity::Running { .. }));
    let ops = MockOps::new(dir.path(), 110);
    let _ = run_machine_result(&cfg, &ops);
    assert!(wait_exited(&exited, pid), "the drill's pipeline group was stopped for the cut");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("readiness_drill_stopped_for_the_cut"), "{text}");
    assert!(!text.contains("readiness_drill_not_stopped"), "{text}");
    assert!(pulse_cutover::drill::cancel_path(&cfg, 99).exists(), "the drill was asked to stop");
    not_for_the_drill(&cfg);

    // 3. Resumed AFTER the freeze next to a drill that cannot be stopped (its process stays alive): a loud warning,
    //    and the cut still continues (never stranded for a drill).
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"resolved_h": 120, "freeze_height": 120})).unwrap();
        j.transition(State::Frozen, serde_json::json!({"freeze_at": 96, "declared_h": 120})).unwrap();
    }
    write_latest(&cfg, live_me);
    let ops = MockOps::new(dir.path(), 110);
    let _ = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("readiness_drill_not_stopped"), "{text}");
    not_for_the_drill(&cfg);
}

/// Round 5 (Codex R5-4): `loop` ran `[loop] reset_cmd` (service commands on the source) before any machine existed or
/// any shadow identity was proven. Now every reset is gated: the chain must be pinned in the config, never real XPR
/// mainnet (unless a shadow), a source that answers must report that chain, a shadow's identity is proven (parts 1-4)
/// right before EVERY reset, and no ceremony or drill may hold this box's locks. A refused gate runs nothing.
#[test]
fn r30r5_loop_never_runs_reset_cmd_before_its_gate() {
    let loop_cfg = |dir: &std::path::Path, base: String, marker: &std::path::Path| -> String {
        base.replace("freeze_height = 120\n", "freeze_height = 0\nfreeze_margin = 20\nderive_h_at_arm = true\n")
            .replace("freeze_height = 120", "freeze_height = 0\nfreeze_margin = 20\nderive_h_at_arm = true")
            + &format!("\n[loop]\nreset_cmd = \"touch '{}'\"\nmetrics_path = \"{}/loop-metrics.jsonl\"\n",
                       marker.display(), dir.display())
    };
    // 1. No pinned chain: refused, nothing ran.
    let dir = tempfile::tempdir().unwrap();
    let _ = test_config(dir.path(), 120);
    let marker = dir.path().join("reset-ran");
    let text = loop_cfg(dir.path(), std::fs::read_to_string(dir.path().join("ceremony.toml")).unwrap(), &marker);
    let cfg = load_toml(dir.path(), "loop.toml", &text).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let err = pulse_cutover::looper::run_loop(&cfg, &ops, 1).unwrap_err();
    assert!(err.contains("set ceremony.chain_id"), "{err}");
    assert!(!marker.exists(), "reset_cmd ran before its gate");

    // 2. Pinned testnet chain, but the source answering is real XPR mainnet: refused, nothing ran.
    let chain = hex::encode(CHAIN_ID);
    let pinned = text.replacen("[ceremony]\n", &format!("[ceremony]\nchain_id = \"{chain}\"\n"), 1);
    let cfg = load_toml(dir.path(), "loop-pinned.toml", &pinned).unwrap();
    *ops.chain_id.borrow_mut() = MAINNET.into();
    let err = pulse_cutover::looper::run_loop(&cfg, &ops, 1).unwrap_err();
    assert!(err.contains("source reports chain_id"), "{err}");
    assert!(!marker.exists(), "reset_cmd ran against a real mainnet source");

    // 3. A ceremony holds this box's journal lock: refused, nothing ran.
    *ops.chain_id.borrow_mut() = chain.clone();
    let held = pulse_cutover::journal::Journal::lock(&cfg.journal_path).unwrap();
    let err = pulse_cutover::looper::run_loop(&cfg, &ops, 1).unwrap_err();
    assert!(err.contains("journal lock"), "{err}");
    assert!(!marker.exists());
    drop(held);

    // 4. A SHADOW loop whose source signs with a real (non-shadow) key: the identity proof fails BEFORE the reset.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let _ = shadow_config(dir.path(), |t| t).unwrap();
    let marker = dir.path().join("reset-ran");
    let base = std::fs::read_to_string(dir.path().join("ceremony-upstream-ignite.toml")).unwrap()
        .replacen("[ceremony]\n", "[ceremony]\nrehearsal = true\n", 1);
    let text = loop_cfg(dir.path(), base, &marker);
    let shadow = load_toml(dir.path(), "shadow-loop.toml", &text).unwrap();
    assert!(shadow.is_shadow());
    let real = shadow_ops(dir.path(), &["PUB_K1_aRealMainnetProducerKeyThatIsNotAShadowKey"], Some(4242));
    let err = pulse_cutover::looper::run_loop(&shadow, &real, 1).unwrap_err();
    assert!(err.contains("SHADOW identity not proven before reset_cmd"), "{err}");
    assert!(!marker.exists(), "reset_cmd ran on an unproven shadow source");
    assert_eq!((real.pauses.get(), real.resumes.get()), (0, 0));
    assert!(real.hooks.borrow().is_empty());

    // 5. The same shadow loop with its identity intact passes the gate (the reset runs; the run itself is not the
    //    point here).
    let proven = shadow_ops(dir.path(), &[SHADOW_KEY], Some(4242));
    assert!(pulse_cutover::looper::loop_gate(&shadow, &proven).is_ok());
    let _ = pulse_cutover::looper::run_loop(&shadow, &proven, 1);
    assert!(marker.exists(), "a proven shadow loop runs its reset");
}

/// Round 5 (Codex R5-5): a ceremony resumed after its freeze asks a live drill to stop by writing the drill's cancel
/// file. That write opened the name with truncate and only O_NOFOLLOW, so a hard link planted at the cancel path
/// truncated whatever it named: here, the ceremony's own journal. It now unlinks the name and creates a fresh inode;
/// the journal keeps every record and the ceremony continues (a loud warning: the drill could not be stopped).
#[test]
fn r30r5_drill_cancel_write_never_truncates_through_a_hard_link() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"resolved_h": 120, "freeze_height": 120})).unwrap();
        j.transition(State::Frozen, serde_json::json!({"freeze_at": 96, "declared_h": 120})).unwrap();
    }
    let me = std::process::id();
    std::fs::create_dir_all(pulse_cutover::drill::drill_dir(&cfg, 99)).unwrap();
    std::fs::write(pulse_cutover::drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 99,
        "state": "RUNNING", "pid": me, "pid_start": pulse_cutover::ops::process_start(me)}).to_string()).unwrap();
    // The planted hard link: the drill's cancel name IS the ceremony's journal.
    std::fs::hard_link(&cfg.journal_path, pulse_cutover::drill::cancel_path(&cfg, 99)).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let _ = run_machine_result(&cfg, &ops);
    let e = journal_entries(&cfg);
    assert_eq!(e.first().map(|v| v["state"].clone()), Some(serde_json::json!("ARMED")), "the journal was truncated: {e:?}");
    assert!(transition(&e, "FROZEN").is_some(), "the FROZEN record survived");
    use std::os::unix::fs::MetadataExt;
    assert_eq!(std::fs::metadata(&cfg.journal_path).unwrap().nlink(), 1, "the cancel file is a separate inode now");
    let cancel = std::fs::read_to_string(pulse_cutover::drill::cancel_path(&cfg, 99)).unwrap();
    assert!(!cancel.contains("ARMED"), "the cancel file is not the journal");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("readiness_drill_not_stopped"), "{text}");
}

/// R5-6 (Codex, ceremony side): a readiness drill on the SAME producer under ANOTHER journal directory holds the
/// producer's lock; before round 5 the ceremony only looked at its own journal directory and armed next to it. Now a
/// fresh ARM refuses before anything freezes.
#[test]
fn r30r5_a_fresh_ceremony_refuses_next_to_a_drill_on_the_same_producer_in_another_directory() {
    let shared = tempfile::tempdir().unwrap();
    let snapdir = shared.path().join("snapshots");
    std::fs::create_dir_all(&snapdir).unwrap();
    let with_snapdir = |dir: &std::path::Path| {
        let cfg = test_config(dir, 120);
        let p = dir.join("ceremony.toml");
        let t = std::fs::read_to_string(&p).unwrap()
            .replacen("[snapshot]\n", &format!("[snapshot]\ndir = \"{}\"\n", snapdir.display()), 1);
        std::fs::write(&p, t).unwrap();
        let _ = cfg;
        Config::load(&p).unwrap()
    };
    let drill_dir = tempfile::tempdir().unwrap();
    let ceremony_dir = tempfile::tempdir().unwrap();
    let drill_cfg = with_snapdir(drill_dir.path());
    let cfg = with_snapdir(ceremony_dir.path());
    assert_eq!(pulse_cutover::drill::producer_lock_base(&drill_cfg), pulse_cutover::drill::producer_lock_base(&cfg));
    // The other directory's drill holds the producer lock for its whole run.
    let held = pulse_cutover::journal::Journal::lock(&pulse_cutover::drill::producer_lock_base(&drill_cfg)).unwrap();
    let ops = MockOps::new(ceremony_dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("producer's lock"), "{err}");
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_none(), "nothing froze");
    assert!(!ops.paused.get());
    drop(held);
}


/// r30 round 6 (Codex R6-1): undo only what this ceremony did. A preflight refusal (a producer someone else paused,
/// a busy producer lock) must never resume that producer or reopen writes another process closed; a normal abort
/// after this ceremony froze and paused still resumes and reopens (regression check).
#[test]
fn r30r6_a_refused_arm_undoes_nothing_a_real_abort_still_undoes_what_it_did() {
    let with_on_abort = |dir: &std::path::Path, extra_snapshot: Option<&std::path::Path>| {
        let _ = test_config(dir, 120);
        let p = dir.join("ceremony.toml");
        let mut t = std::fs::read_to_string(&p).unwrap().replacen("[hooks]\n", "[hooks]\non_abort = \"reopen-writes\"\n", 1);
        if let Some(sd) = extra_snapshot {
            t = t.replacen("[snapshot]\n", &format!("[snapshot]\ndir = \"{}\"\n", sd.display()), 1);
        }
        std::fs::write(&p, t).unwrap();
        Config::load(&p).unwrap()
    };
    // (a) The producer is already paused by another process at ARM: refused, ABORTED, nothing resumed or reopened.
    let dir = tempfile::tempdir().unwrap();
    let cfg = with_on_abort(dir.path(), None);
    let ops = MockOps::new(dir.path(), 110);
    ops.paused.set(true);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("producer already paused at arm time"), "{err}");
    assert_eq!(ops.resumes.get(), 0, "another process's pause was undone");
    assert!(ops.paused.get(), "the producer someone else paused stays paused");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes another process closed were reopened: {:?}", ops.hooks.borrow());
    let e = journal_entries(&cfg);
    assert!(transition(&e, "ABORTED").is_some());
    assert!(e.iter().any(|v| v["data"]["nothing_to_undo"] == serde_json::json!(true)));
    // A later operator rollback is a no-op: the journal proves the rollback complete.
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback(false).unwrap();
    assert!(out.already, "{:?}", out.notes);
    assert_eq!(ops.resumes.get(), 0);

    // (b) A busy producer lock at a fresh ARM: refused, nothing paused, resumed or reopened.
    let shared = tempfile::tempdir().unwrap();
    let snapdir = shared.path().join("snapshots");
    std::fs::create_dir_all(&snapdir).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = with_on_abort(dir.path(), Some(&snapdir));
    let held = pulse_cutover::journal::Journal::lock(&pulse_cutover::drill::producer_lock_base(&cfg)).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("producer's lock"), "{err}");
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0));
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes" || h == "freeze-writes"), "{:?}", ops.hooks.borrow());
    drop(held);

    // (c) Regression: a real abort after this ceremony froze writes and paused the producer still resumes it and
    //     reopens writes (a burn-off transaction after H aborts at SNAPSHOTTED).
    let dir = tempfile::tempdir().unwrap();
    let cfg = with_on_abort(dir.path(), None);
    let ops = MockOps::new(dir.path(), 110);
    ops.late_block.set(true);
    ops.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert!(ops.pauses.get() >= 1);
    assert_eq!(ops.resumes.get(), 1, "the ceremony's own pause is undone");
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "the ceremony's own freeze is undone: {:?}", ops.hooks.borrow());
}

/// r30 round 6 (Codex R6-6/R6-7, reviewer R6-F1/F2): the producer lock lives in an agent-owned `lock_dir`, keyed on
/// nodeos's canonical snapshot directory. Nothing is written into the snapshot directory (a read-only one still
/// arms), spelling aliases of the producer API share one lock, and a lock_dir the agent cannot use is a journaled
/// warning, never a refusal.
#[test]
fn r30r6_producer_lock_lives_in_lock_dir_keyed_on_the_snapshot_dir() {
    use std::os::unix::fs::PermissionsExt;
    let shared = tempfile::tempdir().unwrap();
    let snapdir = shared.path().join("snapshots");
    std::fs::create_dir_all(&snapdir).unwrap();
    let locks = shared.path().join("locks");
    let cfg_for = |dir: &std::path::Path, producer_api: &str, lock_dir: &std::path::Path| {
        let _ = test_config(dir, 120);
        let p = dir.join("ceremony.toml");
        let t = std::fs::read_to_string(&p).unwrap()
            .replacen("[snapshot]\n", &format!("[snapshot]\ndir = \"{}\"\n", snapdir.display()), 1)
            .replacen("[ceremony]\n", &format!("[ceremony]\nlock_dir = \"{}\"\n", lock_dir.display()), 1)
            .replace("producer_api_url = \"http://mock\"", &format!("producer_api_url = \"{producer_api}\""));
        std::fs::write(&p, t).unwrap();
        Config::load(&p).unwrap()
    };
    // Aliases of one producer API (and different journal directories) share one lock, in lock_dir.
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let ca = cfg_for(a.path(), "http://localhost:8888", &locks);
    let cb = cfg_for(b.path(), "http://127.0.0.1:8888/", &locks);
    assert_eq!(pulse_cutover::drill::producer_lock_base(&ca), pulse_cutover::drill::producer_lock_base(&cb));
    assert!(pulse_cutover::drill::producer_lock_base(&ca).starts_with(&locks));

    // A read-only snapshot directory still arms, and nothing is created in it.
    std::fs::set_permissions(&snapdir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let ops = MockOps::new(a.path(), 110);
    let _ = run_machine_result(&ca, &ops);
    std::fs::set_permissions(&snapdir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let e = journal_entries(&ca);
    assert!(transition(&e, "FROZEN").is_some(), "a read-only snapshot dir must not stop the ceremony arming");
    assert!(!e.iter().any(|v| v.to_string().contains("producer's lock")));
    let created: Vec<_> = std::fs::read_dir(&snapdir).unwrap().filter_map(|x| x.ok())
        .map(|x| x.file_name().to_string_lossy().to_string()).filter(|n| n.starts_with(".pulse-cutover")).collect();
    assert!(created.is_empty(), "the agent wrote into nodeos's snapshot dir: {created:?}");

    // A lock_dir the agent cannot use (under a regular file): a journaled warning, the ceremony arms.
    let blocker = shared.path().join("not-a-dir");
    std::fs::write(&blocker, "x").unwrap();
    let c = tempfile::tempdir().unwrap();
    let cc = cfg_for(c.path(), "http://mock", &blocker.join("locks"));
    let ops = MockOps::new(c.path(), 110);
    let _ = run_machine_result(&cc, &ops);
    let e = journal_entries(&cc);
    assert!(e.iter().any(|v| v["data"]["producer_lock_unavailable"].is_string()), "the unusable lock_dir is journaled");
    assert!(transition(&e, "FROZEN").is_some(), "an unusable lock_dir must not refuse the cut");
}

/// r30 round 6 (Codex R6-5): a drill's work that survives its process (export container, pipeline group) is named by
/// the producer's work marker, so a ceremony under ANOTHER journal directory still refuses to arm next to it, and
/// arms once the work is verified gone.
#[test]
fn r30r6_orphan_drill_work_blocks_a_ceremony_in_another_directory() {
    let shared = tempfile::tempdir().unwrap();
    let snapdir = shared.path().join("snapshots");
    std::fs::create_dir_all(&snapdir).unwrap();
    let locks = shared.path().join("locks");
    let cfg_for = |dir: &std::path::Path| {
        let _ = test_config(dir, 120);
        let p = dir.join("ceremony.toml");
        let t = std::fs::read_to_string(&p).unwrap()
            .replacen("[snapshot]\n", &format!("[snapshot]\ndir = \"{}\"\n", snapdir.display()), 1)
            .replacen("[ceremony]\n", &format!("[ceremony]\nlock_dir = \"{}\"\n", locks.display()), 1);
        std::fs::write(&p, t).unwrap();
        Config::load(&p).unwrap()
    };
    let drill_dir = tempfile::tempdir().unwrap();
    let ceremony_dir = tempfile::tempdir().unwrap();
    let drill_cfg = cfg_for(drill_dir.path());
    let cfg = cfg_for(ceremony_dir.path());
    // The drill process is gone (no lock held), but its export container was never verified gone.
    let root = pulse_cutover::drill::drill_root(&drill_cfg);
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("latest.json"), serde_json::json!({"kind": "readiness_drill", "h": 99, "state": "FAILED",
        "export_container": "pulse-drill-99-abc"}).to_string()).unwrap();
    std::fs::create_dir_all(&locks).unwrap();
    std::fs::write(pulse_cutover::drill::work_marker_path(&drill_cfg), serde_json::json!({"kind": "readiness_drill_work",
        "journal_dir": drill_dir.path().canonicalize().unwrap(), "h": 99}).to_string()).unwrap();
    let ops = MockOps::new(ceremony_dir.path(), 110);
    let err = run_machine_result(&cfg, &ops).unwrap_err();
    assert!(err.contains("still running on this producer"), "{err}");
    assert!(transition(&journal_entries(&cfg), "FROZEN").is_none(), "nothing froze");
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0));

    // Once the container is verified gone, a fresh ceremony (new journal) arms.
    std::fs::write(root.join("latest.json"), serde_json::json!({"kind": "readiness_drill", "h": 99, "state": "FAILED",
        "export_container": "pulse-drill-99-abc", "export_container_gone": true}).to_string()).unwrap();
    let ceremony_dir2 = tempfile::tempdir().unwrap();
    let cfg2 = cfg_for(ceremony_dir2.path());
    let ops = MockOps::new(ceremony_dir2.path(), 110);
    let _ = run_machine_result(&cfg2, &ops);
    assert!(transition(&journal_entries(&cfg2), "FROZEN").is_some());
}

/// r30 round 6 (Codex R6-3/R6-4): `loop` fails closed when the source does not answer (its chain cannot be checked
/// before reset_cmd), unless `[loop] reset_when_source_down = true` (refused for a mainnet chain_id), and it takes the
/// producer's lock: a ceremony or drill on the same producer under another journal directory blocks the reset.
#[test]
fn r30r6_loop_gate_fails_closed_and_takes_the_producer_lock() {
    let shared = tempfile::tempdir().unwrap();
    let snapdir = shared.path().join("snapshots");
    std::fs::create_dir_all(&snapdir).unwrap();
    let locks = shared.path().join("locks");
    let chain = hex::encode(CHAIN_ID);
    let loop_text = |dir: &std::path::Path, extra: &str| {
        let _ = test_config(dir, 120);
        let marker = dir.join("reset-ran");
        std::fs::read_to_string(dir.join("ceremony.toml")).unwrap()
            .replace("freeze_height = 120\n", "freeze_height = 0\nfreeze_margin = 20\nderive_h_at_arm = true\n")
            .replacen("[ceremony]\n", &format!("[ceremony]\nchain_id = \"{chain}\"\nlock_dir = \"{}\"\n", locks.display()), 1)
            .replacen("[snapshot]\n", &format!("[snapshot]\ndir = \"{}\"\n", snapdir.display()), 1)
            + &format!("\n[loop]\nreset_cmd = \"touch '{}'\"\nmetrics_path = \"{}/m.jsonl\"\n{extra}", marker.display(), dir.display())
    };
    // 1. The source does not answer: refused, nothing ran.
    let dir = tempfile::tempdir().unwrap();
    let cfg = load_toml(dir.path(), "loop.toml", &loop_text(dir.path(), "")).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    ops.stopped.set(true);
    let err = pulse_cutover::looper::loop_gate(&cfg, &ops).unwrap_err();
    assert!(err.contains("does not answer"), "{err}");
    let err = pulse_cutover::looper::run_loop(&cfg, &ops, 1).unwrap_err();
    assert!(err.contains("does not answer"), "{err}");
    assert!(!dir.path().join("reset-ran").exists(), "reset_cmd ran against an unidentified source");
    // 2. Explicitly allowed for a non-mainnet chain: the gate passes.
    let dir = tempfile::tempdir().unwrap();
    let cfg = load_toml(dir.path(), "loop.toml", &loop_text(dir.path(), "reset_when_source_down = true\n")).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    ops.stopped.set(true);
    assert!(pulse_cutover::looper::loop_gate(&cfg, &ops).is_ok());
    // ... and refused at load for a mainnet chain_id.
    let mainnet = loop_text(dir.path(), "reset_when_source_down = true\n").replace(&chain, MAINNET);
    assert!(load_toml(dir.path(), "m.toml", &mainnet).unwrap_err().contains("reset_when_source_down"));
    // 3. Another directory's ceremony or drill holds this producer's lock: refused, nothing ran.
    let dir = tempfile::tempdir().unwrap();
    let cfg = load_toml(dir.path(), "loop.toml", &loop_text(dir.path(), "")).unwrap();
    let held = pulse_cutover::journal::Journal::lock(&pulse_cutover::drill::producer_lock_base(&cfg)).unwrap();
    let ops = MockOps::new(dir.path(), 110);
    let err = pulse_cutover::looper::run_loop(&cfg, &ops, 1).unwrap_err();
    assert!(err.contains("holds its lock"), "{err}");
    assert!(!dir.path().join("reset-ran").exists());
    drop(held);
}

/// r30 round 7: a schedule_at_h producer config with abort/halt hooks (writes reopened by `reopen-writes`).
fn r7_sched_config(dir: &std::path::Path, on_freeze: &str) -> Config {
    let toml_text = format!(
        r#"
journal_path = "{dir}/journal.jsonl"
poll_ms = 1

[ceremony]
rehearsal = true
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
on_freeze = "{on_freeze}"
post_ignite = "resume-traffic"
on_live = "flip-gateway"
on_abort = "reopen-writes"
on_halt = "page-oncall"
"#,
        dir = dir.display(),
    );
    load_toml(dir, "r7.toml", &toml_text).unwrap()
}

fn r7_journal_has_adopted(cfg: &Config) -> bool {
    journal_entries(cfg).iter().any(|v| v["data"]["side_effect"] == "source_pause" && v["data"]["adopted"] == true)
}

fn r7_journal_has_own_pause(cfg: &Config) -> bool {
    journal_entries(cfg).iter().any(|v| v["data"]["side_effect"] == "source_pause" && v["data"]["adopted"] != true)
}

/// r30 round 7 (reviewer F1): an operator pausing the producer by hand right after the write freeze must not drop
/// this producer out of the cut: the pause is adopted (journaled), never re-paused or resumed, and the cut goes LIVE.
#[test]
fn r30r7_an_operator_pause_after_the_freeze_is_adopted_and_the_cut_goes_live() {
    // Fleet: the other producers finalize H while this one is paused (then pause too, at 125).
    let dir = tempfile::tempdir().unwrap();
    let cfg2 = r7_sched_config(dir.path(), "freeze-writes-operator-pauses");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.peers_produce_to.set(125);
    assert_eq!(run_machine(&cfg2, &ops), State::Live);
    assert!(r7_journal_has_adopted(&cfg2), "the external pause is journaled as adopted");
    assert!(!r7_journal_has_own_pause(&cfg2), "never recorded as this ceremony's pause");
    assert_eq!(ops.pauses.get(), 0, "an adopted pause is never paused again (that would make it ours)");
    assert_eq!(ops.resumes.get(), 0, "never resumed");
}

/// r30 round 7 (Codex R7-1): the external pause is found after a crash with H already final. Ownership is decided
/// BEFORE the H-final branch: adopted, never turned into this ceremony's pause, and the cut goes LIVE.
#[test]
fn r30r7_an_external_pause_with_h_already_final_is_adopted_not_converted() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    *ops.panic_on_hook.borrow_mut() = Some("freeze-writes".into());
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_machine_result(&cfg, &ops)));
    assert!(crashed.is_err(), "agent died inside the freeze hook");
    // Meanwhile the fleet finalized H and someone paused this producer by hand.
    let ops2 = MockOps::new(dir.path(), 130);
    ops2.schedule_ok.set(true);
    ops2.paused.set(true);
    assert_eq!(run_machine(&cfg, &ops2), State::Live);
    assert!(r7_journal_has_adopted(&cfg));
    assert!(!r7_journal_has_own_pause(&cfg), "the H-final path must not record the external pause as ours");
    assert_eq!((ops2.pauses.get(), ops2.resumes.get()), (0, 0));
}

/// r30 round 7: an abort of a ceremony whose producer someone else paused seals (STRANDED, on_halt pages): no
/// resume, no on_abort (writes are never reopened into a paused chain). A forced operator rollback then releases it:
/// resume + reopen, journaled. Our own pause keeps round 6's behaviour (resume + reopen on abort).
#[test]
fn r30r7_an_abort_never_resumes_an_adopted_pause_until_the_operator_releases_it() {
    // Operator pause, then an abort (transactions in the burn-off window).
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    *ops.panic_on_hook.borrow_mut() = Some("freeze-writes".into());
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_machine_result(&cfg, &ops)));
    let ops2 = MockOps::new(dir.path(), 130);
    ops2.schedule_ok.set(true);
    ops2.paused.set(true);
    ops2.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops2), State::Stranded);
    assert!(r7_journal_has_adopted(&cfg));
    assert_eq!(ops2.resumes.get(), 0, "an adopted external pause is never resumed by an abort");
    assert!(!ops2.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes never reopened into a paused chain");
    assert!(ops2.hooks.borrow().iter().any(|h| h == "page-oncall"), "a human is paged");
    let e = journal_entries(&cfg);
    assert!(e.iter().any(|v| v["kind"] == "transition" && v["state"] == "STRANDED" && v["data"]["external_pause_held"] == true));

    // A plain operator rollback refuses (nothing changed); the forced one releases it: resume + reopen, journaled.
    let (j, rec) = open_journal(&cfg.journal_path);
    let err = Machine::new(&cfg, &ops2, j, rec).operator_rollback_opts(false, false).err().unwrap_or_default();
    assert!(err.contains("paused by someone else"), "{err}");
    assert_eq!(ops2.resumes.get(), 0);
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops2, j, rec).operator_rollback_opts(false, true).unwrap();
    assert!(out.failed.is_empty(), "{:?}", out.failed);
    assert_eq!(ops2.resumes.get(), 1, "the operator's forced rollback resumes the released pause");
    assert!(ops2.hooks.borrow().iter().any(|h| h == "reopen-writes"), "and reopens writes");
    assert!(journal_entries(&cfg).iter().any(|v| v["data"]["side_effect"] == "external_pause_release"));

    // Regression (round 6): our own pause, then the same abort: resumed and writes reopened.
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert!(r7_journal_has_own_pause(&cfg) && !r7_journal_has_adopted(&cfg));
    assert_eq!(ops.resumes.get(), 1, "our own pause is resumed on abort");
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "and writes reopen");
}


/// r30 round 7 (review F2): the default lock_dir depends on which user runs, so XPR mainnet configs must name it: a
/// configured mainnet chain_id without one is refused at load, a discovered mainnet source refuses at ARM before anything
/// freezes, and elsewhere the default is journaled loudly.
#[test]
fn r30r7_mainnet_requires_an_explicit_lock_dir_and_the_default_is_loud() {
    // Load time: a configured mainnet chain_id.
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let _ = upstream_mainnet_coordinated_config(dir.path());
    let base = std::fs::read_to_string(dir.path().join("mainnet-coord.toml")).unwrap();
    let with_chain = base.replacen("[ceremony]\n", &format!("[ceremony]\nchain_id = \"{MAINNET}\"\n"), 1);
    assert!(load_toml(dir.path(), "m-ok.toml", &with_chain).is_ok(), "with lock_dir it loads");
    let no_dir = with_chain.lines().filter(|l| !l.starts_with("lock_dir")).collect::<Vec<_>>().join("\n");
    // r30 round 8 (review R8-F1): never refused at load (the beacon, status, rollback and unhalt of an existing
    // installer-generated config keep working); only the entry points that start something new refuse it.
    let loaded = load_toml(dir.path(), "m.toml", &no_dir).expect("a mainnet config without lock_dir still loads");
    assert!(loaded.mainnet_lock_dir_problem().is_some_and(|w| w.contains("must set [ceremony] lock_dir")));
    assert!(Config::load_for_report(&dir.path().join("m.toml")).is_ok(), "the beacon loads it");
    for cmd in ["loop", "await"] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
            .args([cmd, "--config"]).arg(dir.path().join("m.toml")).output().unwrap();
        assert!(!out.status.success(), "{cmd} must refuse");
        assert!(String::from_utf8_lossy(&out.stderr).contains("must set [ceremony] lock_dir"),
            "{cmd}: {}", String::from_utf8_lossy(&out.stderr));
    }

    // ARM: a mainnet source discovered at runtime (chain_id not in the config).
    let cfg = test_config(dir.path(), 120);
    assert!(cfg.ceremony.lock_dir.is_none() && cfg.ceremony.chain_id.is_none());
    let ops = MockOps::new(dir.path(), 110);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    let _ = run_machine_result(&cfg, &ops);
    let e = journal_entries(&cfg);
    assert!(e.iter().any(|v| v.to_string().contains("must set [ceremony] lock_dir")), "refused at ARM");
    assert!(transition(&e, "FROZEN").is_none(), "nothing froze");
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0));

    // Any other chain: the default is used, journaled loudly with its path.
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path(), 120);
    let ops = MockOps::new(dir.path(), 110);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    assert!(journal_entries(&cfg).iter().any(|v| v["data"]["producer_lock_dir_default"].is_string()),
        "the default lock_dir is journaled");
}


/// r30 round 8 (Codex R8-1): someone else pauses the producer DURING the scheduled-snapshot wait (after the step's
/// first ownership check). The pause is never recorded as ours (no own `source_pause`, no pause RPC); the cut carries on
/// with it adopted, and an abort then never resumes it and keeps writes frozen: STRANDED, on_halt pages.
#[test]
fn r30r8_an_external_pause_during_the_snapshot_wait_is_never_ours() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.external_pause_at.set(117); // during the wait for H = 120 to become final
    ops.peers_produce_to.set(126);
    ops.burnoff_tx_per_block.set(1); // the burn-off audit then aborts the cut
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert!(r7_journal_has_adopted(&cfg), "the external pause is journaled as adopted");
    assert!(!r7_journal_has_own_pause(&cfg), "never recorded as this ceremony's pause");
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0), "never paused again, never resumed");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes never reopened into a paused chain");
    assert!(ops.hooks.borrow().iter().any(|h| h == "page-oncall"), "a human is paged");
    assert!(journal_entries(&cfg).iter().any(|v| v["data"]["writes_kept_frozen"] == "producer paused"));
}

/// r30 round 8 (Codex R8-2): the agent dies right after freezing writes; someone else pauses the producer; a plain
/// operator rollback must not reopen writes into the paused chain (nor resume someone else's pause).
#[test]
fn r30r8_rollback_before_adoption_never_reopens_into_a_paused_producer() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    *ops.panic_on_hook.borrow_mut() = Some("freeze-writes".into());
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_machine_result(&cfg, &ops)));
    let ops2 = MockOps::new(dir.path(), 112);
    ops2.paused.set(true);
    let (j, rec) = open_journal(&cfg.journal_path);
    let res = Machine::new(&cfg, &ops2, j, rec).operator_rollback_opts(false, false);
    assert!(res.is_err(), "a plain rollback refuses: the producer is paused by someone else");
    assert_eq!(ops2.resumes.get(), 0);
    assert!(!ops2.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes not reopened into a paused chain");
    // Unreadable producer state: the same (unknown is never "producing").
    let ops3 = MockOps::new(dir.path(), 112);
    ops3.paused_read_fails.set(true);
    let (j, rec) = open_journal(&cfg.journal_path);
    let _ = Machine::new(&cfg, &ops3, j, rec).operator_rollback_opts(false, false);
    assert!(!ops3.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes not reopened into an unknown state");
}

/// r30 round 8 (Codex R8-3): an older agent's journal never fabricates ownership. ARMED only (it never paused):
/// a rollback resumes nothing. Past its pause (SNAPSHOTTED with a pause head): the rollback resumes, as before.
#[test]
fn r30r8_old_journals_resume_only_a_pause_the_old_agent_made() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"mode": "Producer", "freeze_height": 120})).unwrap();
    }
    let ops = MockOps::new(dir.path(), 110);
    ops.paused.set(true); // someone else's pause
    let (j, rec) = open_journal(&cfg.journal_path);
    assert_eq!(rec.undo_tracking, Some(false), "an older agent's arming record");
    let _ = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false);
    assert_eq!(ops.resumes.get(), 0, "an old ARMED journal never paused: nothing is resumed");

    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"mode": "Producer", "freeze_height": 120})).unwrap();
        j.transition(State::Frozen, serde_json::json!({})).unwrap();
        j.transition(State::Snapshotted, serde_json::json!({"cut_height": 120, "head_at_pause": 125})).unwrap();
    }
    let ops = MockOps::new(dir.path(), 125);
    ops.paused.set(true);
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false).unwrap();
    assert!(out.failed.is_empty(), "{:?}", out.failed);
    assert_eq!(ops.resumes.get(), 1, "the old agent paused it (it reached SNAPSHOTTED): resumed as before");
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "and writes reopen into the producing chain");
}

/// r30 round 8 (Codex R8-4): a run resumed while still ARMED that never froze writes is a fresh ARM: a producer
/// someone else paused is refused, nothing undone.
#[test]
fn r30r8_a_resumed_armed_run_refuses_an_externally_paused_producer() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"mode": "Producer", "freeze_height": 120, "undo_tracking": true})).unwrap();
    }
    let ops = MockOps::new(dir.path(), 90);
    ops.schedule_ok.set(true);
    ops.paused.set(true);
    let _ = run_machine_result(&cfg, &ops);
    let e = journal_entries(&cfg);
    assert!(e.iter().any(|v| v.to_string().contains("producer already paused at arm time")), "refused like a fresh ARM");
    assert!(transition(&e, "FROZEN").is_none(), "nothing froze");
    assert_eq!((ops.pauses.get(), ops.resumes.get()), (0, 0));
    assert!(!ops.hooks.borrow().iter().any(|h| h == "freeze-writes" || h == "reopen-writes"));
}

/// r30 round 8 (Codex R8-6): an abort journaled that it keeps writes frozen (strand intent), then the agent died
/// before the STRANDED transition. The next run completes the strand: nothing resumed, nothing reopened.
#[test]
fn r30r8_a_journaled_strand_intent_is_completed_on_the_next_run() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    {
        let (mut j, _) = open_journal(&cfg.journal_path);
        j.transition(State::Armed, serde_json::json!({"mode": "Producer", "freeze_height": 120, "undo_tracking": true})).unwrap();
        j.evidence(State::Armed, serde_json::json!({"side_effect": "write_freeze"})).unwrap();
        j.transition(State::Frozen, serde_json::json!({})).unwrap();
        j.evidence(State::Frozen, serde_json::json!({"strand_intent": true, "writes_kept_frozen": "producer paused"})).unwrap();
    }
    // The producer is producing again when the next run starts (someone resumed it): without the recovered intent the
    // run would carry the cut on; with it, the strand is completed (sealing pauses the source), nothing reopened.
    let ops = MockOps::new(dir.path(), 125);
    ops.schedule_ok.set(true);
    assert!(run_machine_result(&cfg, &ops).is_err());
    let rec = pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.state, Some(State::Stranded), "the strand is completed");
    assert!(transition(&journal_entries(&cfg), "SNAPSHOTTED").is_none(), "the cut did not carry on");
    assert_eq!(ops.resumes.get(), 0);
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"));
}

/// r30 round 8: the two invariants together. (6) Our own pause, then an abort: resumed, and writes reopen because the
/// producer now produces. (7) Adopted, then the external party resumes it, then we pause it ourselves: our pause is
/// resumed on abort and writes reopen. (8) The operator's forced release resumes an external pause, and writes reopen
/// only if the producer then produces (a resume that does not take effect seals instead).
#[test]
fn r30r8_resume_ours_reopen_only_into_a_producing_chain() {
    // (6)
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert!(r7_journal_has_own_pause(&cfg));
    assert_eq!(ops.resumes.get(), 1);
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"));

    // (6b) our own pause resumed, but a fresh read still shows it paused (someone else paused on top): sealed.
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.burnoff_tx_per_block.set(1);
    ops.resume_ignored.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    assert_eq!(ops.resumes.get(), 1, "our own pause was resumed");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "but writes stay frozen: not producing");

    // (7) adopted at the top of FROZEN (after a crash in the freeze hook), then its owner resumes it during the wait;
    // this ceremony then pauses it itself (a fresh read sees it producing) and owns THAT pause.
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    *ops.panic_on_hook.borrow_mut() = Some("freeze-writes".into());
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_machine_result(&cfg, &ops)));
    let ops2 = MockOps::new(dir.path(), 112);
    ops2.schedule_ok.set(true);
    ops2.paused.set(true);
    ops2.peers_produce_to.set(115);
    ops2.external_resume_at.set(114);
    ops2.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops2), State::Aborted);
    assert!(r7_journal_has_adopted(&cfg) && r7_journal_has_own_pause(&cfg), "adopted earlier, then paused by us");
    assert_eq!(ops2.pauses.get(), 1, "this ceremony made the second pause");
    assert_eq!(ops2.resumes.get(), 1, "our own pause is resumed on abort");
    assert!(ops2.hooks.borrow().iter().any(|h| h == "reopen-writes"), "and writes reopen");

    // (8) forced release: resume + reopen only if producing.
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.external_pause_at.set(117);
    ops.peers_produce_to.set(126);
    ops.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    ops.resume_ignored.set(true);
    let (j, rec) = open_journal(&cfg.journal_path);
    let _ = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true);
    assert_eq!(ops.resumes.get(), 1, "the operator's forced release resumes it");
    assert!(!ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "but writes reopen only into a producing chain");
    ops.resume_ignored.set(false);
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true).unwrap();
    assert!(out.failed.is_empty(), "{:?}", out.failed);
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "once it produces, writes reopen");
}


/// r30 round 9 (Codex R9-1): an R2 strand (the agent kept writes frozen because the source was not producing) journals a
/// strand intent before its STRANDED transition. The transition satisfies it: a later, valid `join` reaches LIVE instead
/// of being stranded again by the old intent (also for round 7's `external_pause_held` record).
#[test]
fn r30r9_a_join_after_an_r2_strand_reaches_live() {
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    ops.coord_down.set(true);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    // Make it an R2 strand: the strand intent (and round 7's record) precede the STRANDED transition.
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let at = lines.iter().rposition(|l| l.contains(r#""kind":"transition""#) && l.contains(r#""state":"STRANDED""#)).unwrap();
    for rec in [serde_json::json!({"strand_intent": true, "writes_kept_frozen": "producer paused"}),
                serde_json::json!({"external_pause_held": true})] {
        let line = serde_json::json!({"seq": 0, "ts_ms": 0, "ts": "2026-10-13T00:00:00.000Z", "kind": "evidence",
            "state": "VERIFIED", "data": rec}).to_string();
        lines.insert(at, line);
    }
    std::fs::write(&cfg.journal_path, lines.join("\n") + "\n").unwrap();
    assert!(!pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().strand_intent,
        "the STRANDED transition satisfied the intent");
    ops.coord_down.set(false);
    let ours = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    *ops.status_doc.borrow_mut() = Some(rc23_live_peers(&ours, &["bp2", "bp3", "bp4", "bp5"]));
    let (j, rec) = open_journal(&cfg.journal_path);
    assert_eq!(Machine::new(&cfg, &ops, j, rec).join("e1").unwrap(), State::Live, "the join is not re-stranded");
}

/// r30 round 9 (Codex R9-3): a join from ABORTED starts a new journal segment before its first side effect, so the
/// earlier, completed rollback never covers what the join did. (a) Killed inside the re-freeze hook: `rollback` reopens
/// writes (never "already rolled back"). (b) Killed after the join's own pause took effect but before its `join` side
/// effect was journaled: `rollback` resumes that pause and reopens writes.
#[test]
fn r30r9_a_crashed_join_is_rolled_back_by_its_own_segment() {
    let aborted = |d: &std::path::Path| {
        let cfg = rc23_upstream_fleet(d);
        let ops = upstream_ops(d);
        *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
            ("bp4", serde_json::json!({"state": "FROZEN"})), ("bp5", serde_json::json!({"state": "FROZEN"}))]));
        assert_eq!(run_machine(&cfg, &ops), State::Aborted);
        assert!(pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().aborted_rollback_complete);
        (cfg, ops)
    };
    let all_aborted = || rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "ABORTED"})), ("bp5", serde_json::json!({"state": "ABORTED"}))]);

    // (a)
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = aborted(d.path());
    *ops.panic_on_hook.borrow_mut() = Some("freeze-writes".into());
    let (j, rec) = open_journal(&cfg.journal_path);
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Machine::new(&cfg, &ops, j, rec).join("e1")));
    assert!(crashed.is_err(), "the join died inside the re-freeze hook");
    *ops.panic_on_hook.borrow_mut() = None;
    let rec = pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap();
    assert!(!rec.aborted_rollback_complete && rec.segment == 1, "the join's segment is not covered by the old rollback");
    *ops.status_doc.borrow_mut() = Some(all_aborted());
    let resumes = ops.resumes.get();
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback(false).unwrap();
    assert!(!out.already && out.failed.is_empty(), "{out:?}");
    assert_eq!(ops.hooks.borrow().last().map(String::as_str), Some("reopen-writes"), "writes reopened");
    assert_eq!(ops.resumes.get(), resumes, "nothing of ours to resume: the join never paused");

    // (b): the records a join writes up to its pause, then the crash.
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = aborted(d.path());
    {
        let (mut j, rec) = open_journal(&cfg.journal_path);
        let st = rec.state.unwrap();
        j.evidence(st, serde_json::json!({"segment_start": 1, "by": "join"})).unwrap();
        j.evidence(st, serde_json::json!({"side_effect": "write_freeze"})).unwrap();
        j.evidence(st, serde_json::json!({"join_refreeze": "ran: freeze-writes"})).unwrap();
        j.evidence(st, serde_json::json!({"join": {"event_id": "e1", "source_paused": false}})).unwrap();
        j.evidence(st, serde_json::json!({"side_effect": "source_pause", "episode": 2})).unwrap();
        // r30 round 10: the pause took effect and was confirmed (its ack), so it is the join's own.
        j.evidence(st, serde_json::json!({"source_pause_ack": 2})).unwrap();
    }
    ops.paused.set(true); // the join's pause RPC took effect before the crash
    *ops.status_doc.borrow_mut() = Some(all_aborted());
    let resumes = ops.resumes.get();
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback(false).unwrap();
    assert!(!out.already && out.failed.is_empty(), "{out:?}");
    assert_eq!(ops.resumes.get(), resumes + 1, "the join's own pause is resumed");
    assert_eq!(ops.hooks.borrow().last().map(String::as_str), Some("reopen-writes"), "and writes reopen");

    // (c) r30 round 10: killed after the join's pause INTENT but before its ack, with the producer paused: ownership is
    // unknown (it may be someone else's), so nothing resumes it and writes stay frozen (sealed), never reopened into a
    // paused producer.
    let d = tempfile::tempdir().unwrap();
    let (cfg, ops) = aborted(d.path());
    {
        let (mut j, rec) = open_journal(&cfg.journal_path);
        let st = rec.state.unwrap();
        j.evidence(st, serde_json::json!({"segment_start": 1, "by": "join"})).unwrap();
        j.evidence(st, serde_json::json!({"side_effect": "write_freeze"})).unwrap();
        j.evidence(st, serde_json::json!({"join_refreeze": "ran: freeze-writes"})).unwrap();
        j.evidence(st, serde_json::json!({"join": {"event_id": "e1", "source_paused": false}})).unwrap();
        j.evidence(st, serde_json::json!({"side_effect": "source_pause", "episode": 2})).unwrap();
    }
    ops.paused.set(true);
    *ops.status_doc.borrow_mut() = Some(all_aborted());
    let resumes = ops.resumes.get();
    let hooks_before = ops.hooks.borrow().len();
    let (j, rec) = open_journal(&cfg.journal_path);
    let _ = Machine::new(&cfg, &ops, j, rec).operator_rollback(false);
    assert_eq!(ops.resumes.get(), resumes, "an unacknowledged pause is never resumed");
    assert!(!ops.hooks.borrow()[hooks_before..].iter().any(|h| h == "reopen-writes"), "writes stay frozen");
    assert!(ops.paused.get());
}

/// r30 round 9 (Codex R9-2): this ceremony resumed its own pause, then on_abort failed, then someone else paused the
/// producer. A completed resume never authorizes resuming a later pause: plain rollbacks never resume it.
#[test]
fn r30r9_a_resumed_then_externally_repaused_producer_is_not_resumed_by_a_plain_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.burnoff_tx_per_block.set(1);
    *ops.flaky_hook.borrow_mut() = Some(("reopen-writes".into(), 1));
    assert_eq!(run_machine(&cfg, &ops), State::Aborted);
    assert_eq!(ops.resumes.get(), 1, "our own pause was resumed");
    assert!(!pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap().aborted_rollback_complete, "on_abort failed");
    ops.paused.set(true); // someone else pauses it now
    for _ in 0..2 {
        let (j, rec) = open_journal(&cfg.journal_path);
        let _ = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, false);
    }
    assert_eq!(ops.resumes.get(), 1, "the later external pause is never resumed by a plain rollback");
    assert_eq!(ops.hooks.borrow().iter().filter(|h| *h == "reopen-writes").count(), 1, "writes never reopened into a paused chain");
}

/// r30 round 9 (Codex R9-4): an operator rollback that cannot finish (the released pause's resume did not take effect,
/// so writes stay frozen and the journal is STRANDED) reports a failure with the truth: never "rolled back".
#[test]
fn r30r9_a_stranded_operator_rollback_reports_failure() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.external_pause_at.set(117);
    ops.peers_produce_to.set(126);
    ops.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    ops.resume_ignored.set(true);
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true).unwrap();
    assert_eq!(out.state, State::Stranded);
    assert!(out.failed.iter().any(|f| f.contains("did NOT finish") && f.contains("writes stay FROZEN") && f.contains("STRANDED")),
        "{:?}", out.failed);
}

/// r30 round 9 (Codex R9-5): an XPR mainnet ceremony that died inside its freeze hook (a write-freeze intent journaled,
/// still ARMED) resumes past the freeze even when the config has no lock_dir: a warning, never a refusal.
#[test]
fn r30r9_a_frozen_armed_mainnet_journal_without_lock_dir_resumes() {
    let dir = tempfile::tempdir().unwrap();
    stage_fake_upstream_tools(dir.path(), 0);
    let cfg = upstream_mainnet_coordinated_config(dir.path());
    let ops = upstream_ops(dir.path());
    ops.schedule_ok.set(true);
    *ops.chain_id.borrow_mut() = MAINNET.into();
    *ops.coord_doc.borrow_mut() = Some(serde_json::json!({"event": mainnet_event(cfg.ceremony.freeze_height)}));
    *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp1", serde_json::json!({"state": "VERIFIED", "aborting": true})),
        ("bp2", serde_json::json!({"state": "ABORTED"}))]));
    let freeze = cfg.hooks.on_freeze.clone().unwrap();
    *ops.panic_on_hook.borrow_mut() = Some(freeze.clone());
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_machine_result(&cfg, &ops)));
    assert!(crashed.is_err(), "the agent died inside the freeze hook");
    let rec = pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap();
    assert_eq!(rec.state, Some(State::Armed));
    assert!(rec.writes_closed, "the write-freeze intent is journaled");
    *ops.panic_on_hook.borrow_mut() = None;
    let text = std::fs::read_to_string(dir.path().join("mainnet-coord.toml")).unwrap();
    let no_dir = text.lines().filter(|l| !l.starts_with("lock_dir")).collect::<Vec<_>>().join("\n");
    let cfg2 = load_toml(dir.path(), "mainnet-coord-nodir.toml", &no_dir).unwrap();
    assert!(cfg2.ceremony.lock_dir.is_none());
    let _ = run_machine_result(&cfg2, &ops);
    let e = journal_entries(&cfg2);
    assert!(transition(&e, "FROZEN").is_some(), "the cut continued past the freeze");
    assert!(e.iter().any(|v| v["data"]["mainnet_lock_dir_missing"].is_string()), "warned");
    assert!(!e.iter().any(|v| v["kind"] == "error" && v.to_string().contains("must set [ceremony] lock_dir")),
        "never refused for it");
}

/// r30 round 9 (Codex R9-6): a forced rollback that releases an external pause journals its rollback intent FIRST: a
/// crash between the two records always recovers a pending rollback (run refuses, rollback finishes), never a released
/// pause with nothing behind it.
#[test]
fn r30r9_the_forced_release_follows_the_rollback_intent() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.external_pause_at.set(117);
    ops.peers_produce_to.set(126);
    ops.burnoff_tx_per_block.set(1);
    assert_eq!(run_machine(&cfg, &ops), State::Stranded);
    let before = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true).unwrap();
    assert!(out.failed.is_empty(), "{:?}", out.failed);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let added: Vec<&str> = text.lines().skip(before.lines().count()).collect();
    let intent = added.iter().position(|l| l.contains(r#""rollback_requested":true"#)).expect("intent journaled");
    let release = added.iter().position(|l| l.contains(r#""side_effect":"external_pause_release""#)).expect("release journaled");
    assert!(intent < release, "the rollback intent precedes the release");
    // The crash between them: the journal ends right after the intent.
    let cut: Vec<&str> = before.lines().chain(added[..=intent].iter().copied()).collect();
    std::fs::write(&cfg.journal_path, cut.join("\n") + "\n").unwrap();
    let rec = pulse_cutover::journal::Journal::replay(&cfg.journal_path).unwrap();
    assert!(rec.rollback_pending, "a pending rollback is recovered");
    ops.paused.set(true);
    ops.hooks.borrow_mut().clear();
    assert!(run_machine_result(&cfg, &ops).unwrap_err().contains("did not finish"), "run refuses to carry on");
    let (j, rec) = open_journal(&cfg.journal_path);
    let out = Machine::new(&cfg, &ops, j, rec).operator_rollback_opts(false, true).unwrap();
    assert!(out.failed.is_empty(), "{:?}", out.failed);
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "the re-run finishes the rollback");
}

/// r30 round 9 (review R9-F1): right after this rollback resumed its own pause, a transient read error must not strand a
/// healthy producer: the read is retried (bounded) and writes reopen once it answers "producing".
#[test]
fn r30r9_a_transient_read_after_our_resume_is_retried() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.burnoff_tx_per_block.set(1);
    ops.fail_reads_after_resume.set(3);
    assert_eq!(run_machine(&cfg, &ops), State::Aborted, "not STRANDED: the read recovered");
    assert_eq!(ops.resumes.get(), 1);
    assert!(ops.hooks.borrow().iter().any(|h| h == "reopen-writes"), "writes reopened into the producing chain");
    let e = journal_entries(&cfg);
    assert!(e.iter().any(|v| v["data"]["producer_state_read_failed"].is_string()), "the failed reads are journaled");
    assert!(e.iter().any(|v| v["data"]["producer_state_read_recovered"].is_string()));
}

/// r30 round 9 (review R9-F2): a transient read error while deciding who paused the producer (after the freeze) is
/// retried instead of ending the run mid-FROZEN.
#[test]
fn r30r9_a_transient_read_during_adoption_is_retried() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = r7_sched_config(dir.path(), "freeze-writes");
    let ops = MockOps::new(dir.path(), 110);
    ops.schedule_ok.set(true);
    ops.fail_reads_after_freeze.set(3);
    assert_eq!(run_machine(&cfg, &ops), State::Live);
    assert!(journal_entries(&cfg).iter().any(|v| v["data"]["producer_state_read_failed"].is_string()));
}

// ------------------------------------------------------------ r30 round 10: join model --
//
// The randomized model checker (tests/model.rs) covers the fork backend's run / rollback / unhalt paths. `join` and the
// upstream backend's chain-creation point of no return need the upstream fixtures that live here, so their model runs
// here on an instrumented MockOps: a seeded scenario starts from an ABORTED or STRANDED coordinated ceremony, lets an
// external actor pause or resume the producer, injects a kill at a random instrumented operation inside `join`, and
// follows up with joins, plain and forced rollbacks and re-runs. Invariants are checked at the action point (I2, I3, I4
// inside MockOps) and after every step (I5 journal replays). `JOIN_MODEL_SCENARIOS` (default 60), `JOIN_MODEL_SEED`.

struct JoinRng(u64);
impl JoinRng {
    fn new(seed: u64) -> Self {
        JoinRng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

fn join_model_quiet_panics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload().downcast_ref::<String>().cloned()
                .or_else(|| info.payload().downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default();
            if payload.starts_with("MODEL-CRASH-INJECTED") {
                return;
            }
            prev(info)
        }));
    });
}

/// One seeded join scenario. Returns (violations, trace).
fn join_scenario(seed: u64) -> (Vec<String>, Vec<String>) {
    let mut rng = JoinRng::new(seed);
    let mut trace = vec![format!("seed {seed}")];
    let d = tempfile::tempdir().unwrap();
    let cfg = rc23_upstream_fleet(d.path());
    let ops = upstream_ops(d.path());
    let stranded = rng.chance(50);
    let start = if stranded {
        ops.coord_down.set(true);
        run_machine(&cfg, &ops)
    } else {
        *ops.status_doc.borrow_mut() = Some(rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})),
            ("bp3", serde_json::json!({"state": "ABORTED"})), ("bp4", serde_json::json!({"state": "FROZEN"})),
            ("bp5", serde_json::json!({"state": "FROZEN"}))]));
        run_machine(&cfg, &ops)
    };
    trace.push(format!("start: {start:?}"));
    ops.coord_down.set(false);
    let ours = pulse_cutover::beacon::journal_summary(&cfg.journal_path)["evidence"].clone();
    let live = rc23_live_peers(&ours, &["bp2", "bp3", "bp4", "bp5"]);
    let all_aborted = rc23_status(&[("bp2", serde_json::json!({"state": "ABORTED"})), ("bp3", serde_json::json!({"state": "ABORTED"})),
        ("bp4", serde_json::json!({"state": "ABORTED"})), ("bp5", serde_json::json!({"state": "ABORTED"}))]);
    // r30 round 11 (reach): in half the scenarios the source does not drift: an unpaused source whose head stays at
    // the journaled pause head (its peers paused), so `join_checks` pass and the join's own re-freeze, pause intent,
    // ack and unknown-ownership windows are reached (with drift a resumed source always fails the pause-head check).
    if rng.chance(50) {
        ops.drift.set(0);
        if let Some(hp) = Journal::replay(&cfg.journal_path).ok().and_then(|r| r.head_at_pause) {
            ops.head.set(hp);
        }
        trace.push(format!("stationary source at {}", ops.head.get()));
    }
    // From here the model watches.
    *ops.model_journal.borrow_mut() = Some(cfg.journal_path.clone());
    ops.model_on.set(true);
    let external = |ops: &MockOps, pause: bool, trace: &mut Vec<String>| {
        if pause && !ops.paused.get() {
            ops.paused.set(true);
            ops.model_external_pause.set(true);
            ops.model_release_baseline.set(ops.model_releases());
            trace.push("external pause".into());
        } else if !pause && ops.paused.get() && ops.model_external_pause.get() {
            ops.paused.set(false);
            ops.model_external_pause.set(false);
            trace.push("external resume".into());
        }
    };
    let steps = 2 + rng.below(4);
    for i in 0..steps {
        match rng.below(4) {
            0 => external(&ops, true, &mut trace),
            1 => external(&ops, false, &mut trace),
            _ => {}
        }
        *ops.status_doc.borrow_mut() = Some(if rng.chance(70) { live.clone() } else { all_aborted.clone() });
        if rng.chance(60) {
            ops.model_crash_at.set(ops.model_calls.get() + 1 + rng.below(60));
        }
        ops.model_crash_after_pause.set(rng.chance(35));
        let pick = if i == 0 { 0 } else { rng.below(10) };
        let label = match pick { 0..=4 => "join", 5..=6 => "rollback", 7 => "rollback --force-stranded", _ => "run" };
        ops.model_budget.set(ops.model_calls.get() + 200_000);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<String, String> {
            let (j, rec) = Journal::open(&cfg.journal_path)?;
            let mut m = Machine::new(&cfg, &ops, j, rec);
            match label {
                "join" => m.join("e1").map(|s| s.as_str().to_string()),
                "rollback" => m.operator_rollback_opts(false, false).map(|o| format!("{} failed={:?}", o.state.as_str(), o.failed)),
                "rollback --force-stranded" => m.operator_rollback_opts(false, true).map(|o| format!("{} failed={:?}", o.state.as_str(), o.failed)),
                _ => m.run().map(|s| s.as_str().to_string()),
            }
        }));
        ops.model_crash_at.set(0);
        ops.model_budget.set(0);
        let shown = match &r {
            Ok(x) => format!("{x:?}"),
            Err(p) => {
                let msg = p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default();
                if msg.starts_with("MODEL-NO-PROGRESS") {
                    ops.model_violate("I6", format!("{label} made no progress: {msg}"));
                } else if !msg.starts_with("MODEL-CRASH-INJECTED") {
                    ops.model_violate("PANIC", format!("unexpected panic in {label}: {msg}"));
                }
                msg
            }
        };
        trace.push(format!("{label}: {shown} (paused={} external={})", ops.paused.get(), ops.model_external_pause.get()));
        match Journal::replay(&cfg.journal_path) {
            Err(e) => ops.model_violate("I5", format!("journal does not replay after {label}: {e}")),
            // I1: a rollback that provably finished leaves no pause of the AGENT's in place.
            Ok(rec) => if rec.state == Some(State::Aborted) && rec.aborted_rollback_complete && ops.paused.get()
                && !ops.model_external_pause.get() {
                ops.model_violate("I1", format!("journal ABORTED (rolled back) after {label}, but the agent's pause is still in place"));
            },
        }
    }
    let state = Journal::replay(&cfg.journal_path).ok().and_then(|r| r.state).map(|s| s.as_str().to_string());
    // Coverage tags (r30 round 11): which join windows this scenario reached, from the journal after a segment start.
    let jt = std::fs::read_to_string(&cfg.journal_path).unwrap_or_default();
    let after_segment = jt.find("segment_start").map(|i| &jt[i..]).unwrap_or("");
    let mut tags = Vec::new();
    for (k, needle) in [("segment", "segment_start"), ("join_refreeze", r#""side_effect":"write_freeze""#),
                        ("join_pause_ack", "source_pause_ack"), ("join_unknown", r#""pause_ownership_unknown":true"#),
                        ("join_record", r#""side_effect":"join""#)] {
        if after_segment.contains(needle) {
            tags.push(k);
        }
    }
    // An own pause intent (not an adoption record): `{"episode":n,"side_effect":"source_pause"}`; one whose episode
    // never got its `source_pause_ack` is the unknown-ownership window (a kill between the pause and its ack).
    let mut intents = Vec::new();
    let mut acks = Vec::new();
    for l in after_segment.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(l) else { continue };
        let d = &v["data"];
        if d["side_effect"] == "source_pause" && d["adopted"] != true {
            intents.push(d["episode"].as_u64().unwrap_or(0));
        }
        if let Some(a) = d["source_pause_ack"].as_u64() {
            acks.push(a);
        }
    }
    if !intents.is_empty() {
        tags.push("join_pause_intent");
    }
    if intents.iter().any(|e| !acks.contains(e)) {
        tags.push("join_intent_unacked");
    }
    trace.push(format!("COVER {}", tags.join(",")));
    trace.push(format!("final: {state:?}"));
    let v = ops.model_violations.borrow().clone();
    (v, trace)
}

#[test]
fn r30r10_join_model_finds_no_invariant_violation() {
    join_model_quiet_panics();
    let (start, count) = match std::env::var("JOIN_MODEL_SEED").ok().and_then(|s| s.parse::<u64>().ok()) {
        Some(s) => (s, 1),
        None => (1, std::env::var("JOIN_MODEL_SCENARIOS").ok().and_then(|s| s.parse().ok()).unwrap_or(60)),
    };
    let mut bad = Vec::new();
    let mut finals: std::collections::BTreeMap<String, u64> = Default::default();
    let mut cover: std::collections::BTreeMap<String, u64> = Default::default();
    for seed in start..start + count {
        let (v, t) = join_scenario(seed);
        *finals.entry(t.last().cloned().unwrap_or_default()).or_default() += 1;
        if let Some(c) = t.iter().find_map(|l| l.strip_prefix("COVER ")) {
            for k in c.split(',').filter(|k| !k.is_empty()) {
                *cover.entry(k.to_string()).or_default() += 1;
            }
        }
        if !v.is_empty() {
            bad.push((seed, v, t));
        }
    }
    eprintln!("JOIN MODEL over {count} scenarios: {finals:?}; coverage {cover:?}");
    // r30 round 11: the default run must actually reach the join's own pause windows (a model that never gets there
    // proves nothing about them).
    if count >= 60 {
        for k in ["segment", "join_pause_intent", "join_pause_ack", "join_intent_unacked"] {
            assert!(cover.get(k).copied().unwrap_or(0) > 0, "join model never reached `{k}` in {count} scenarios: {cover:?}");
        }
    }
    let report: String = bad.iter().take(10).map(|(s, v, t)| format!("\n=== seed {s}: {}\n    {}", v.join(" | "), t.join("\n    "))).collect();
    assert!(bad.is_empty(), "{} of {count} join scenarios violated an invariant:{report}", bad.len());
}

// ------------------------------------------------------------------------------------------------------------------
// r30 round 11 (Codex R10-1, R10-2): a resume gives up ownership BEFORE its RPC (`side_effect: source_resume`); an
// unsettled resume leaves the episode's ownership unknown (never resumed again on this ceremony's own authority), and the
// pause_at_h snapshot-recovery resume closes its episode like any other.
// ------------------------------------------------------------------------------------------------------------------

/// Append records to a test journal, as a crashed agent would have left them.
fn r30r11_append(cfg: &Config, records: &[serde_json::Value]) {
    let (mut j, rec) = open_journal(&cfg.journal_path);
    let s = rec.state.unwrap();
    for r in records {
        j.evidence(s, r.clone()).unwrap();
    }
}

#[test]
fn r30r11_an_unsettled_resume_never_resumes_a_later_pause() {
    // A rollback journaled its resume intent, then died (before the RPC, or after it with the response lost). Since
    // then the producer is paused again: by someone else after our resume took effect, or still ours if the RPC never
    // left. The agent cannot tell, so it never resumes it on its own authority and never reopens writes into it.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_with_on_abort(dir.path());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains("source_pause_ack"), "fixture: the ceremony paused and acknowledged its pause");
    r30r11_append(&cfg, &[serde_json::json!({"rollback_requested": true, "force_after_ignite": false}),
        serde_json::json!({"side_effect": "source_resume", "episode": 1, "during": "rollback"})]);
    let ops = MockOps::new(dir.path(), 200);
    ops.paused.set(true);
    let res = rollback_with(&cfg, &ops, false);
    let ev = ops.events.borrow().clone();
    assert!(!ev.iter().any(|e| e == "resume"), "an unsettled resume's episode is never resumed again: {ev:?} {res:?}");
    assert!(!ev.iter().any(|e| e == "hook:reopen-writes"), "writes never reopen into a paused producer: {ev:?}");
    match res {
        Err(e) => assert!(e.contains("refusing to roll back"), "{e}"),
        Ok(out) => assert_eq!(out.state, State::Stranded, "{out:?}"),
    }
    // The operator's explicit release resumes it and reopens writes once the producer produces.
    let ops2 = MockOps::new(dir.path(), 200);
    ops2.paused.set(true);
    let out = rollback_with_stranded(&cfg, &ops2);
    let ev2 = ops2.events.borrow().clone();
    assert!(ev2.iter().any(|e| e == "resume") && ev2.iter().any(|e| e == "hook:reopen-writes"), "{ev2:?} {out:?}");
    assert!(std::fs::read_to_string(&cfg.journal_path).unwrap().contains("external_pause_release"));
}

#[test]
fn r30r11_an_unsettled_resume_with_the_producer_producing_reopens_writes_without_resuming() {
    // Crash after the resume RPC took effect, before its closing record: the producer produces. Nothing to resume (and
    // nothing is resumed); the reopen invariant's fresh read lets writes reopen.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_with_on_abort(dir.path());
    r30r11_append(&cfg, &[serde_json::json!({"rollback_requested": true, "force_after_ignite": false}),
        serde_json::json!({"side_effect": "source_resume", "episode": 1, "during": "rollback"})]);
    let ops = MockOps::new(dir.path(), 200);
    ops.paused.set(false);
    let out = rollback_with(&cfg, &ops, false).unwrap();
    let ev = ops.events.borrow().clone();
    assert!(!ev.iter().any(|e| e == "resume"), "{ev:?}");
    assert!(ev.iter().any(|e| e == "hook:reopen-writes"), "writes reopen into a producing chain: {ev:?}");
    assert!(out.failed.is_empty() && out.state == State::Aborted, "{out:?}");
}

#[test]
fn r30r11_a_crash_inside_the_snapshot_recovery_resume_adopts_instead_of_resuming() {
    // pause_at_h recovery: a crash after the resume-for-snapshot intent, before its RPC; the producer is paused when
    // the agent comes back. Ownership is unknown: the pause is adopted for the cut, never resumed by this ceremony.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    r30r11_append(&cfg, &[serde_json::json!({"side_effect": "source_resume", "episode": 1,
        "during": "resuming production for the snapshot"})]);
    let _ = std::fs::remove_file(dir.path().join("staged.bin")); // the fixture's earlier run staged it
    let ops = MockOps::new(dir.path(), 200);
    ops.paused.set(true);
    let _ = run_machine_result(&cfg, &ops);
    let ev = ops.events.borrow().clone();
    assert!(!ev.iter().any(|e| e == "resume"), "an unsettled resume is never retried on our own authority: {ev:?}");
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""pause_ownership_unknown":true"#), "adopted with ownership unknown: {text}");
}

#[test]
fn r30r11_the_snapshot_recovery_resume_closes_its_episode() {
    // A crashed pause_at_h run left the producer paused by this ceremony (intent + ack). The re-run resumes it for the
    // snapshot: that closes the episode, so the next pause needs a fresh intent and ack, and a pause someone else makes
    // afterwards is never taken for ours.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    let before = std::fs::read_to_string(&cfg.journal_path).unwrap();
    let intents_before = before.matches(r#""side_effect":"source_pause""#).count();
    let _ = std::fs::remove_file(dir.path().join("staged.bin")); // the fixture's earlier run staged it
    let _ = std::fs::remove_file(dir.path().join("staged.bin"));
    let ops = MockOps::new(dir.path(), 120); // the crashed run froze at H = 120 (pause_at_h snapshots at H)
    ops.paused.set(true);
    let _ = run_machine_result(&cfg, &ops);
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""resumed_production_for_snapshot":true"#), "{text}");
    let resumed_at = text.find(r#""resumed_production_for_snapshot":true"#).unwrap();
    let after = &text[resumed_at..];
    assert!(after.contains(r#""side_effect":"source_pause""#) && after.contains("source_pause_ack"),
        "the next pause opened a NEW episode with its own intent and ack: {after}");
    assert!(text.matches(r#""side_effect":"source_pause""#).count() > intents_before);

    // Replay: once the recovery resume is journaled, the old episode is closed; a pause seen afterwards is not ours.
    let dir = tempfile::tempdir().unwrap();
    let cfg = pre_ignite_journal(dir.path());
    r30r11_append(&cfg, &[serde_json::json!({"side_effect": "source_resume", "episode": 1,
        "during": "resuming production for the snapshot"}),
        serde_json::json!({"resumed_production_for_snapshot": true, "episode": 1})]);
    let rec = Journal::replay(&cfg.journal_path).unwrap();
    let eps = pulse_cutover::journal::pause_episodes(&rec.pause_events, false, true);
    assert!(!eps.ours && !eps.unknown, "the recovery resume closed the episode: {:?}", rec.pause_events);
    let _ = std::fs::remove_file(dir.path().join("staged.bin"));
    let ops = MockOps::new(dir.path(), 200);
    ops.paused.set(true); // someone else paused it after our resume
    let _ = run_machine_result(&cfg, &ops);
    assert!(!ops.events.borrow().iter().any(|e| e == "resume"), "{:?}", ops.events.borrow());
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
    assert!(text.contains(r#""adopted":true"#), "the later pause is adopted, not ours: {text}");
}

fn rollback_with_stranded(cfg: &Config, ops: &MockOps) -> Result<pulse_cutover::machine::RollbackOutcome, String> {
    let (j, rec) = open_journal(&cfg.journal_path);
    Machine::new(cfg, ops, j, rec).operator_rollback_opts(false, true)
}
