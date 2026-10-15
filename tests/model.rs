//! Randomized model checker for the ceremony's journal / pause / rollback state machine (r30).
//!
//! Review rounds 6–9 found crash-and-interleaving bugs one at a time: a pause the agent made left in place, writes
//! reopened into a paused producer, an external pause resumed, a strand that blocks progress. This test drives the
//! REAL `Machine` through seeded random scenarios against a world model that knows the ground truth (who paused the
//! producer, whether the old chain's write edge is open, whether a target was ignited) and checks invariants at the
//! moment of every action and after every step:
//!
//! - I1 no pause the AGENT made is left in place once the journal ends ABORTED (rolled back);
//! - I2 writes are never reopened on the old chain while its producer is paused;
//! - I3 an EXTERNAL pause is never resumed except after a journaled explicit release (`external_pause_release`)
//!   written AFTER that pause began, and each release authorizes one resume (per pause episode, round 11);
//! - I4 the source is never resumed after this journal's chain-creation / ignition intent;
//! - I5 the journal always replays;
//! - I6 every step terminates (no loop), and a plain re-run of a STRANDED journal with nothing changed in the world
//!   never appends another STRANDED transition (no strand loop);
//! - I7 a journal that ends ABORTED with its rollback provably done (`rollback_done`) leaves the old chain's writes
//!   open (or never froze them): a rollback that cannot reopen writes must end STRANDED (sealed, paged) instead.
//!
//! About half the scenarios use `schedule_at_h` (the mainnet freeze strategy: the snapshot is scheduled at ARM and
//! the world's scheduler writes `snapshot-<id of H>.bin` once H is final), the rest the `pause_at_h` fallback.
//!
//! Crashes are injected faithfully: the world panics on the N-th operation the agent performs, so everything the
//! agent journaled (fsynced) before that call survives and nothing after it happened. Then the scenario continues
//! with a restart, external actions, operator rollbacks or `unhalt`, like a real operator would.
//!
//! Default run: `MODEL_SCENARIOS` scenarios (default 300, a few seconds in debug). Long run:
//! `MODEL_SCENARIOS=20000 cargo test --test model -- --nocapture`. Replay one: `MODEL_SEED=<seed>`.

use std::{
    cell::{
        Cell,
        RefCell,
    },
    path::{
        Path,
        PathBuf,
    },
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
};
use pulsevm_snapshot::testing::{
    MiniSnapshot,
    TestAccount,
};

const CHAIN_ID: [u8; 32] = [0xAB; 32];
const CRASH: &str = "MODEL-CRASH-INJECTED";
const H: u64 = 120;

fn mini(head: u32) -> MiniSnapshot {
    MiniSnapshot {
        chain_id: CHAIN_ID,
        head_block_num: head,
        head_slot: 1_514_764_800,
        head_producer: "eosio".parse().unwrap(),
        accounts: vec![TestAccount { name: "cutdemo1".parse().unwrap(), key: [2u8; 33] }],
    }
}

// ------------------------------------------------------------------ PRNG --

/// xorshift64*: seeded, reproducible, no dependency.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
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

// ----------------------------------------------------------------- world --

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    Agent,
    External,
}

/// The world: one producer, its old-chain write edge, the target. Ground truth only; the agent sees it through
/// `ChainOps`.
struct World {
    dir: PathBuf,
    journal: PathBuf,
    head: Cell<u64>,
    paused: Cell<bool>,
    owner: Cell<Option<Owner>>,
    /// The agent's pause stands for the fleet pausing at H together: the chain head stops. An external pause of THIS
    /// producer alone does not stop the chain (its peers keep producing).
    agent_paused: Cell<bool>,
    /// The old chain's public write edge (hooks freeze-writes / reopen-writes). Starts open.
    writes_open: Cell<bool>,
    /// on_live flipped the edge to the new chain: the old edge no longer matters.
    edge_new: Cell<bool>,
    ignited: Cell<bool>,
    target_head: Cell<u64>,
    traffic: Cell<bool>,
    // --- scripted faults ---
    /// Panic (simulated kill) on this agent operation number (0 = never).
    crash_at: Cell<u64>,
    calls: Cell<u64>,
    /// Hard bound on operations in one step: exceeding it is an I6 violation (no progress).
    call_budget: Cell<u64>,
    /// source_info polls so far (the clock external actions are scripted on).
    ticks: Cell<u64>,
    ext_pause_tick: Cell<u64>,
    ext_resume_tick: Cell<u64>,
    /// The next N producer_paused() reads fail (a transient producer-API error).
    paused_read_fails: Cell<u32>,
    burnoff_tx: Cell<u64>,
    /// The next N on_abort (reopen-writes) hooks fail.
    reopen_fails: Cell<u32>,
    /// The next N resume() calls fail (the producer API refuses).
    resume_fails: Cell<u32>,
    /// The next N resume() calls answer Ok but the producer stays paused (a stuck node).
    resume_ignored: Cell<u32>,
    /// r30 round 11 (Codex R10-1): the next N resume() calls TAKE EFFECT but the answer is lost (an error to the agent).
    resume_lost: Cell<u32>,
    /// r30 round 11 (Codex R10-1): the next resume() takes effect, then the agent is killed before it hears back.
    crash_after_resume: Cell<bool>,
    /// r30 round 11: the next pause() takes effect, then the agent is killed (before its ack / next record).
    crash_after_pause: Cell<bool>,
    /// r30 round 11: the agent is killed on its first source poll while its own pause holds (after the pause ack).
    crash_on_poll_while_paused: Cell<bool>,
    /// r30 round 11: right after any agent resume, someone else pauses the producer on the next source poll.
    ext_pause_after_resume: Cell<bool>,
    ext_pause_armed: Cell<bool>,
    /// r30 round 11: someone else pauses the producer the instant an agent resume returns (no poll in between).
    ext_pause_right_after_resume: Cell<bool>,
    /// The new chain never produces past the cut (LIVE gate fails → HALT).
    target_stuck: Cell<bool>,
    /// r30 round 11: `schedule_at_h` scenarios. The scheduler accepts `schedule_snapshot(H)` and writes
    /// `snapshot-<id of H>.bin` into `snap_dir` once LIB >= H (like nodeos), never earlier.
    scheduler: Cell<bool>,
    scheduled_h: Cell<u64>,
    snap_dir: PathBuf,
    /// r30 round 13 (I3, run-local release authorization): the number of `external_pause_release` records that can no
    /// longer authorize anything. It is reset to the journal's count at the start of every step (a release journaled
    /// by an earlier run authorizes nothing in this one) and when an external pause begins (a release for an earlier
    /// pause authorizes nothing for this one), and it advances by one on EVERY resume attempt of an external pause
    /// (successful, refused or ignored: each release authorizes exactly one attempt by the run that journaled it).
    release_baseline: Cell<usize>,
    /// r30 round 13: someone else resumes the AGENT's own pause on this source poll (an operator un-pausing by hand).
    ext_resume_agent_tick: Cell<u64>,
    /// r30 round 13: a later external pause on this source poll (after an external resume of the agent's pause).
    ext_pause2_tick: Cell<u64>,
    /// r30 round 14: the agent's own pause was un-paused by someone else and the agent has not read the producer state
    /// since. The later external pause (`ext_pause2_tick`) waits for one agent read in between unless
    /// `r13_gap_free` (the documented inherent residual: an un-pause and re-pause entirely between two reads).
    ext_resume_agent_unread: Cell<bool>,
    r13_gap_free: Cell<bool>,
    /// r30 round 13: after the next resume that takes effect, this many producer_paused() reads fail (an outage of the
    /// producer API right after a resume; a large value outlasts the agent's 30 s retry window).
    read_outage_after_resume: Cell<u32>,
    /// r30 round 13 (I7): the journal was ABORTED with its rollback complete when the current step began.
    complete_before: Cell<bool>,
    /// r30 round 13: the config has no `on_abort` hook (nothing can reopen writes; that is the operator's job).
    no_abort_hook: Cell<bool>,
    /// r30 round 13: a legacy fixture whose (older) agent froze writes.
    legacy_froze: Cell<bool>,
    /// Signed coordinator abort is served from this tick on (0 = never; coordinated configs only).
    coord_abort_tick: Cell<u64>,
    now: Cell<u64>,
    // --- observations ---
    violations: RefCell<Vec<String>>,
    trace: RefCell<Vec<String>>,
    /// r30 round 11: a resume took effect without the agent learning it (answer lost, or killed right after): the
    /// precondition of class R10-1.
    r10_1_fired: Cell<bool>,
}

impl World {
    fn new(dir: &Path) -> Self {
        World {
            dir: dir.to_path_buf(),
            journal: dir.join("journal.jsonl"),
            head: Cell::new(110),
            paused: Cell::new(false),
            owner: Cell::new(None),
            agent_paused: Cell::new(false),
            writes_open: Cell::new(true),
            edge_new: Cell::new(false),
            ignited: Cell::new(false),
            target_head: Cell::new(0),
            traffic: Cell::new(false),
            crash_at: Cell::new(0),
            calls: Cell::new(0),
            call_budget: Cell::new(0),
            ticks: Cell::new(0),
            ext_pause_tick: Cell::new(0),
            ext_resume_tick: Cell::new(0),
            paused_read_fails: Cell::new(0),
            burnoff_tx: Cell::new(0),
            target_stuck: Cell::new(false),
            crash_after_pause: Cell::new(false),
            crash_on_poll_while_paused: Cell::new(false),
            ext_pause_after_resume: Cell::new(false),
            ext_pause_armed: Cell::new(false),
            ext_pause_right_after_resume: Cell::new(false),
            resume_lost: Cell::new(0),
            crash_after_resume: Cell::new(false),
            scheduler: Cell::new(false),
            scheduled_h: Cell::new(0),
            snap_dir: dir.join("snapshots"),
            release_baseline: Cell::new(0),
            ext_resume_agent_tick: Cell::new(0),
            ext_resume_agent_unread: Cell::new(false),
            r13_gap_free: Cell::new(R13_GAP_FREE.with(|g| g.get())),
            ext_pause2_tick: Cell::new(0),
            read_outage_after_resume: Cell::new(0),
            complete_before: Cell::new(false),
            no_abort_hook: Cell::new(false),
            legacy_froze: Cell::new(false),
            reopen_fails: Cell::new(0),
            resume_fails: Cell::new(0),
            resume_ignored: Cell::new(0),
            coord_abort_tick: Cell::new(0),
            now: Cell::new(1_000_000),
            violations: RefCell::new(Vec::new()),
            trace: RefCell::new(Vec::new()),
            r10_1_fired: Cell::new(false),
        }
    }

    fn note(&self, s: impl Into<String>) {
        self.trace.borrow_mut().push(s.into());
    }

    fn violate(&self, inv: &str, why: String) {
        let msg = format!("{inv}: {why}");
        // Round 12: classes R10-1, R10-2 and R11-A are fixed, so every violation fails (no known-open escape).
        if inv == "I3" && self.r10_1_fired.get() {
            self.note("class R10-1 shape (a resume took effect unseen)".to_string());
        }
        self.note(format!("VIOLATION {msg}"));
        self.violations.borrow_mut().push(msg);
    }

    fn journal_text(&self) -> String {
        std::fs::read_to_string(&self.journal).unwrap_or_default()
    }

    fn releases(&self) -> usize {
        self.journal_text().matches("\"side_effect\":\"external_pause_release\"").count()
    }

    /// Every agent operation passes through here: the crash point and the progress budget.
    fn op(&self, name: &str) {
        let n = self.calls.get() + 1;
        self.calls.set(n);
        if self.crash_at.get() == n {
            self.crash_at.set(0);
            self.note(format!("crash at op {n} ({name})"));
            panic!("{CRASH}");
        }
        if self.call_budget.get() > 0 && n > self.call_budget.get() {
            self.call_budget.set(0);
            panic!("MODEL-NO-PROGRESS after {n} operations (last: {name})");
        }
    }

    fn external_pause(&self) {
        if !self.paused.get() {
            self.paused.set(true);
            self.owner.set(Some(Owner::External));
            self.release_baseline.set(self.releases());
            self.note(format!("external pause at head {}", self.head.get()));
        }
    }

    fn external_resume(&self) {
        if self.paused.get() && self.owner.get() == Some(Owner::External) {
            self.paused.set(false);
            self.owner.set(None);
            self.note(format!("external resume at head {}", self.head.get()));
        }
    }

    /// r30 round 13: someone else un-pauses the AGENT's own pause (the chain moves again; ownership: nobody).
    fn external_resume_of_agent_pause(&self) {
        if self.paused.get() && self.owner.get() == Some(Owner::Agent) {
            self.paused.set(false);
            self.owner.set(None);
            self.agent_paused.set(false);
            self.ext_resume_agent_unread.set(true);
            self.note(format!("external resume of the AGENT's pause at head {}", self.head.get()));
        }
    }

    /// r30 round 13: every step (run, rollback, unhalt) starts here. Releases journaled by earlier runs authorize
    /// nothing in this one (I3), and I7 remembers whether the rollback was already complete.
    fn begin_step(&self) {
        self.release_baseline.set(self.releases());
        let complete = Journal::replay(&self.journal)
            .map(|r| r.state == Some(State::Aborted) && r.aborted_rollback_complete).unwrap_or(false);
        self.complete_before.set(complete);
    }

    /// The scheduler writes the snapshot of H once H is final (LIB = head - 1 in this model).
    fn scheduler_tick(&self) {
        let h = self.scheduled_h.get();
        if h == 0 || self.head.get().saturating_sub(1) < h {
            return;
        }
        let file = self.snap_dir.join(format!("snapshot-{}.bin", self.block_id(h)));
        if !file.exists() {
            let _ = std::fs::create_dir_all(&self.snap_dir);
            let _ = std::fs::write(&file, mini(h as u32).build());
            self.target_head.set(h);
            self.note(format!("scheduler wrote the snapshot of H={h} (head {})", self.head.get()));
        }
    }

    fn block_id(&self, h: u64) -> String {
        hex::encode(mini(h as u32).head_id())
    }

    fn info(&self, head: u64) -> ChainInfo {
        ChainInfo {
            chain_id: hex::encode(CHAIN_ID),
            head_block_num: head,
            head_block_id: self.block_id(head),
            head_block_time: "2024-01-01T00:00:00.000".into(),
            last_irreversible_block_num: head.saturating_sub(1),
        }
    }
}

fn mock_signed(payload: serde_json::Value) -> serde_json::Value {
    use ed25519_dalek::Signer;
    let sk = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
    let p = payload.to_string();
    serde_json::json!({"payload": p, "sig": hex::encode(sk.sign(p.as_bytes()).to_bytes()),
        "key": hex::encode(sk.verifying_key().to_bytes())})
}

fn mock_event() -> serde_json::Value {
    mock_signed(serde_json::json!({"type": "event", "network": "rehearsal", "event_id": "e1"}))
}

impl ChainOps for World {
    fn source_info(&self) -> Result<ChainInfo, String> {
        self.op("source_info");
        if self.crash_on_poll_while_paused.get() && self.agent_paused.get() {
            self.crash_on_poll_while_paused.set(false);
            self.note("crash on a source poll while the agent's pause holds");
            panic!("{CRASH}");
        }
        let t = self.ticks.get() + 1;
        self.ticks.set(t);
        if self.ext_pause_tick.get() == t {
            self.external_pause();
        }
        if self.ext_resume_tick.get() == t {
            self.external_resume();
        }
        if self.ext_resume_agent_tick.get() == t {
            self.external_resume_of_agent_pause();
        }
        if self.ext_pause2_tick.get() == t {
            if self.ext_resume_agent_unread.get() && !self.r13_gap_free.get() {
                // Round 14: the claim tested is "seen by the next read"; the re-pause waits for one.
                self.ext_pause2_tick.set(t + 1);
            } else {
                self.external_pause();
            }
        }
        if self.ext_pause_armed.get() {
            self.ext_pause_armed.set(false);
            self.external_pause();
        }
        if !self.agent_paused.get() {
            self.head.set(self.head.get() + 1);
        }
        self.scheduler_tick();
        Ok(self.info(self.head.get()))
    }

    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        self.op("source_block_id");
        Ok((self.block_id(block_num), "2024-01-01T00:00:00.000".into()))
    }

    fn source_block_tx_count(&self, _block_num: u64) -> Result<u64, String> {
        self.op("source_block_tx_count");
        Ok(self.burnoff_tx.get())
    }

    fn producer_paused(&self) -> Result<bool, String> {
        self.op("producer_paused");
        self.ext_resume_agent_unread.set(false);
        if self.paused_read_fails.get() > 0 {
            self.paused_read_fails.set(self.paused_read_fails.get() - 1);
            return Err("producer API: connection reset (model)".into());
        }
        Ok(self.paused.get())
    }

    fn pause(&self) -> Result<(), String> {
        self.op("pause");
        self.agent_paused.set(true);
        if !self.paused.get() {
            self.paused.set(true);
            self.owner.set(Some(Owner::Agent));
            self.note(format!("agent pause at head {}", self.head.get()));
        } else {
            self.note(format!("agent pause on an already paused producer (owner {:?})", self.owner.get()));
        }
        if self.crash_after_pause.get() {
            self.crash_after_pause.set(false);
            self.note("crash right after the pause took effect");
            panic!("{CRASH}");
        }
        Ok(())
    }

    fn resume(&self) -> Result<(), String> {
        self.op("resume");
        let j = self.journal_text();
        // I3 (round 13, run-local): every attempt to resume an EXTERNAL pause (whatever its outcome) needs a release
        // journaled by THIS run after that pause began and not yet consumed; the attempt consumes it.
        if self.paused.get() && self.owner.get() == Some(Owner::External) {
            let now = self.releases();
            let used = self.release_baseline.get();
            if now <= used {
                self.violate("I3", format!("agent attempted to resume an EXTERNAL pause with no unconsumed release journaled \
                    by this run since that pause began (releases {now}, already spent or stale {used}; head {})", self.head.get()));
            } else {
                self.release_baseline.set(used + 1); // consumed by this attempt
            }
        }
        if j.contains("\"ignite_started\"") || j.contains("\"side_effect\":\"create_chain\"") {
            self.violate("I4", "agent resumed the source after this journal's ignition / creation intent".into());
        }
        if self.resume_fails.get() > 0 {
            self.resume_fails.set(self.resume_fails.get() - 1);
            self.note("agent resume FAILED (producer API refused)");
            return Err("producer API: resume refused (model)".into());
        }
        if self.resume_ignored.get() > 0 {
            self.resume_ignored.set(self.resume_ignored.get() - 1);
            self.note(format!("agent resume answered Ok but had no effect (owner {:?})", self.owner.get()));
            return Ok(());
        }
        if self.paused.get() {
            self.note(format!("agent resume (owner was {:?})", self.owner.get()));
        }
        self.paused.set(false);
        self.owner.set(None);
        self.agent_paused.set(false);
        let outage = self.read_outage_after_resume.replace(0);
        if outage > 0 {
            self.paused_read_fails.set(self.paused_read_fails.get() + outage);
            self.note(format!("producer API outage right after the resume: the next {outage} reads fail"));
        }
        if self.ext_pause_after_resume.get() {
            self.ext_pause_armed.set(true);
        }
        if self.ext_pause_right_after_resume.replace(false) {
            self.external_pause();
        }
        if self.crash_after_resume.get() {
            self.crash_after_resume.set(false);
            self.r10_1_fired.set(true);
            self.note("crash right after the resume took effect (the agent never hears back)");
            panic!("{CRASH}");
        }
        if self.resume_lost.get() > 0 {
            self.resume_lost.set(self.resume_lost.get() - 1);
            self.r10_1_fired.set(true);
            self.note("resume took effect but the answer was lost");
            return Err("producer API: connection reset after the request was sent (model)".into());
        }
        Ok(())
    }

    fn create_snapshot(&self) -> Result<SnapshotResult, String> {
        self.op("create_snapshot");
        if self.agent_paused.get() {
            return Err("create_snapshot timed out: paused chain never finalizes head (R1)".into());
        }
        let cut = self.head.get();
        let path = self.dir.join("snapshot-cut.bin");
        std::fs::write(&path, mini(cut as u32).build()).map_err(|e| e.to_string())?;
        self.head.set(cut + 1);
        self.target_head.set(cut);
        Ok(SnapshotResult { snapshot_name: path.display().to_string(), head_block_num: cut, head_block_id: self.block_id(cut) })
    }

    fn schedule_snapshot(&self, height: u64) -> Result<(), String> {
        self.op("schedule_snapshot");
        if !self.scheduler.get() {
            return Err("snapshot scheduler unavailable (model: pause_at_h)".into());
        }
        self.scheduled_h.set(height);
        self.note(format!("snapshot scheduled at H={height}"));
        Ok(())
    }

    fn target_info(&self) -> Result<Option<ChainInfo>, String> {
        self.op("target_info");
        if !self.ignited.get() {
            return Ok(None);
        }
        let head = if self.traffic.get() && !self.target_stuck.get() {
            self.target_head.set(self.target_head.get() + 1);
            self.target_head.get()
        } else {
            self.target_head.get()
        };
        Ok(Some(self.info(head)))
    }

    fn public_info(&self, _public_url: &str) -> Result<Option<ChainInfo>, String> {
        self.op("public_info");
        Ok(Some(self.info(self.head.get())))
    }

    fn ignite(&self) -> Result<String, String> {
        self.op("ignite");
        self.ignited.set(true);
        self.note("ignite");
        Ok("model ignite".into())
    }

    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        self.op(&format!("hook {cmd}"));
        match cmd {
            "freeze-writes" => {
                self.writes_open.set(false);
                self.note(format!("hook freeze-writes at head {}", self.head.get()));
            }
            "fail-freeze" => return Err("`fail-freeze` exited 1 (model)".into()),
            "reopen-writes" => {
                if self.reopen_fails.get() > 0 {
                    self.reopen_fails.set(self.reopen_fails.get() - 1);
                    self.note("hook reopen-writes FAILED");
                    return Err("`reopen-writes` exited 1 (model)".into());
                }
                if self.paused.get() && !self.edge_new.get() {
                    self.violate("I2", format!("writes reopened on the old chain while its producer is paused (owner {:?})", self.owner.get()));
                }
                self.writes_open.set(true);
                self.note("hook reopen-writes");
            }
            "resume-traffic" => self.traffic.set(true),
            "flip-gateway" => {
                self.edge_new.set(true);
                self.note("hook flip-gateway (LIVE)");
            }
            _ => {}
        }
        Ok(format!("ran {cmd}"))
    }

    fn get_json(&self, url: &str) -> Result<Option<serde_json::Value>, String> {
        self.op("get_json");
        if url.contains("/api/coord/") {
            let t = self.coord_abort_tick.get();
            if t > 0 && self.ticks.get() >= t {
                return Ok(Some(serde_json::json!({"event": mock_event(),
                    "abort": mock_signed(serde_json::json!({"type": "abort", "network": "rehearsal", "event_id": "e1"}))})));
            }
            return Ok(Some(serde_json::json!({"event": mock_event()})));
        }
        if url.ends_with("/api/status") {
            return Ok(Some(serde_json::json!({"networks": [{"id": "rehearsal", "producers": []}]})));
        }
        Ok(None)
    }

    fn now_ms(&self) -> u64 {
        self.now.set(self.now.get() + 25);
        self.now.get()
    }

    fn sleep_ms(&self, ms: u64) {
        self.now.set(self.now.get() + ms);
    }
}

// ---------------------------------------------------------------- config --

fn model_config(dir: &Path, coordinated: bool, fail_freeze: bool) -> Config {
    model_config_with(dir, coordinated, fail_freeze, false)
}

fn model_config_with(dir: &Path, coordinated: bool, fail_freeze: bool, scheduled: bool) -> Config {
    model_config_full(dir, coordinated, fail_freeze, scheduled, false)
}

/// `no_abort_hook` (round 13): a rehearsal-valid config without `on_abort` (nothing reopens writes on a rollback).
fn model_config_full(dir: &Path, coordinated: bool, fail_freeze: bool, scheduled: bool, no_abort_hook: bool) -> Config {
    let mut text = format!(
        r#"
journal_path = "{d}/journal.jsonl"
poll_ms = 1

[ceremony]
rehearsal = true
freeze_height = {H}
{strategy}
quiescence_polls = 3
quiescence_timeout_secs = 5

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
snapshot_timeout_secs = 20

[snapshot]
staged_path = "{d}/staged.bin"
capture_roots = "{d}/captured-roots.txt"
{snapdir}
[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 10
live_sustain_secs = 1

[hooks]
on_freeze = "{freeze}"
post_ignite = "resume-traffic"
on_live = "flip-gateway"
{on_abort}on_halt = "page"
"#,
        d = dir.display(),
        freeze = if fail_freeze { "fail-freeze" } else { "freeze-writes" },
        strategy = if scheduled { "freeze_strategy = \"schedule_at_h\"" } else { "" },
        on_abort = if no_abort_hook { "" } else { "on_abort = \"reopen-writes\"\n" },
        snapdir = if scheduled { format!("dir = \"{}/snapshots\"\n", dir.display()) } else { String::new() },
    );
    if scheduled {
        std::fs::create_dir_all(dir.join("snapshots")).unwrap();
    }
    if coordinated {
        use ed25519_dalek::SigningKey;
        let key = hex::encode(SigningKey::from_bytes(&[42u8; 32]).verifying_key().to_bytes());
        let hash = pulse_cutover::coord::payload_hash(&mock_event()).unwrap();
        text.push_str(&format!(
            "\n[coordination]\nurl = \"http://mc\"\nnetwork = \"rehearsal\"\ncoordinator_keys = [\"{key}\"]\nevent_id = \"e1\"\nevent_hash = \"{hash}\"\nfleet_quorum = 0\nfleet_timeout_secs = 5\n"
        ));
    }
    let path = dir.join("ceremony.toml");
    std::fs::write(&path, text).unwrap();
    Config::load(&path).unwrap_or_else(|e| panic!("model config does not load: {e}"))
}

// --------------------------------------------------------------- driving --

#[derive(Debug)]
enum StepResult {
    #[allow(dead_code)]
    Done(Result<String, String>),
    Crashed,
    Panicked(String),
}

fn guarded<F: FnOnce() -> Result<String, String>>(world: &World, f: F) -> StepResult {
    world.call_budget.set(world.calls.get() + 200_000);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    world.call_budget.set(0);
    match r {
        Ok(x) => StepResult::Done(x),
        Err(p) => {
            let msg = p.downcast_ref::<String>().cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string panic".into());
            if msg.contains(CRASH) { StepResult::Crashed } else { StepResult::Panicked(msg) }
        }
    }
}

fn open_journal(path: &Path) -> Result<(Journal, pulse_cutover::journal::Recovered), String> {
    for _ in 0..100 {
        match Journal::open(path) {
            Ok(x) => return Ok(x),
            Err(e) if e.contains("another pulse-cutover process holds") => std::thread::sleep(std::time::Duration::from_millis(10)),
            Err(e) => return Err(e),
        }
    }
    Err("journal lock busy".into())
}

fn step_run(cfg: &Config, world: &World) -> StepResult {
    world.begin_step();
    guarded(world, || {
        let (j, rec) = open_journal(&cfg.journal_path)?;
        let mut m = Machine::new(cfg, world, j, rec);
        m.run().map(|s| s.as_str().to_string())
    })
}

fn step_rollback(cfg: &Config, world: &World, force_stranded: bool) -> StepResult {
    world.begin_step();
    guarded(world, || {
        let (j, rec) = open_journal(&cfg.journal_path)?;
        let mut m = Machine::new(cfg, world, j, rec);
        m.operator_rollback_opts(false, force_stranded).map(|o| format!("{} failed={:?} already={}", o.state.as_str(), o.failed, o.already))
    })
}

fn step_unhalt(cfg_path: &Path) -> Result<String, String> {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pulse-cutover"))
        .args(["unhalt", "--config"]).arg(cfg_path).arg("--i-understand")
        .output().map_err(|e| e.to_string())?;
    if out.status.success() { Ok("unhalted".into()) } else { Err(String::from_utf8_lossy(&out.stderr).trim().to_string()) }
}

fn strand_transitions(world: &World) -> usize {
    world.journal_text().lines().filter(|l| l.contains("\"kind\":\"transition\"") && l.contains("\"state\":\"STRANDED\"")).count()
}

fn journal_state(world: &World) -> Result<Option<State>, String> {
    Journal::replay(&world.journal).map(|r| r.state)
}

/// Invariants checked after every step (the at-the-moment ones fire inside the world).
fn check_after_step(world: &World, label: &str) {
    match journal_state(world) {
        Err(e) => world.violate("I5", format!("journal does not replay after {label}: {e}")),
        Ok(state) => {
            if state == Some(State::Aborted) && world.journal_text().contains("rollback_done")
                && world.paused.get() && world.owner.get() == Some(Owner::Agent) {
                world.violate("I1", format!("journal ABORTED (rolled back) after {label}, but the AGENT's pause is still in place"));
            }
            // I7: a rollback that provably finished leaves the old chain writable (unless it never froze writes).
            let complete = Journal::replay(&world.journal).map(|r| r.aborted_rollback_complete).unwrap_or(false);
            if state == Some(State::Aborted) && complete && !world.writes_open.get() && !world.edge_new.get()
                && !world.no_abort_hook.get() {
                world.violate("I7", format!("journal ABORTED with rollback_done after {label}, but the old chain's writes are \
                    still frozen (a rollback that cannot reopen writes must strand and page)"));
            }
            // I7 (round 13): a rollback that completed in THIS step never leaves the producer paused, whoever paused it
            // and whatever hooks are configured (an unresolved or unknown pause must strand and page instead).
            // Scope: a ceremony that froze writes (this journal, or the older agent of a legacy fixture). A refused ARM
            // or an abort before the freeze has nothing to undo and correctly leaves someone else's pause alone.
            let froze = world.journal_text().contains("\"side_effect\":\"write_freeze\"") || world.legacy_froze.get();
            if state == Some(State::Aborted) && complete && !world.complete_before.get() && world.paused.get() && froze {
                world.violate("I7", format!("journal ABORTED with rollback_done after {label} while the producer is still \
                    paused (owner {:?}): a rollback must not report completion over a paused producer", world.owner.get()));
            }
        }
    }
}

/// One seeded scenario. Returns the violations (empty = clean).
fn scenario(seed: u64) -> (Vec<String>, Vec<String>) {
    scenario_full(seed, true, true)
}

/// Round-12 entry (the r10/r11 regression seeds): no round-13 additions, so they replay exactly as found.
fn scenario_with(seed: u64, r11: bool) -> (Vec<String>, Vec<String>) {
    scenario_full(seed, r11, false)
}

/// A journal written by an agent from before round 6 (no `undo_tracking` / `pause_ack` markers on its arming record),
/// with the world left as that agent would have left it. Kind 0: ARMED only (it never froze or paused). Kind 1:
/// FROZEN (its write freeze holds, it never paused). Kind 2: SNAPSHOTTED with a pause head (it froze writes and
/// paused the producer).
fn write_legacy_journal(world: &World, kind: u64) {
    let (mut j, _) = open_journal(&world.journal).expect("legacy journal");
    j.transition(State::Armed, serde_json::json!({"mode": "Producer", "freeze_height": H})).unwrap();
    if kind >= 1 {
        world.legacy_froze.set(true);
        j.transition(State::Frozen, serde_json::json!({"frozen_ts_ms": 1_000_000u64, "head_at_freeze": H - 24})).unwrap();
        world.writes_open.set(false);
    }
    if kind >= 2 {
        let hp = world.head.get() + 5;
        world.head.set(hp);
        let snap = world.dir.join(format!("snapshot-{}.bin", world.block_id(H)));
        std::fs::write(&snap, mini(H as u32).build()).unwrap();
        world.target_head.set(H);
        j.transition(State::Snapshotted, serde_json::json!({"cut_height": H, "head_at_pause": hp,
            "snapshot_file": snap.display().to_string(), "cut_block_id": world.block_id(H)})).unwrap();
        world.paused.set(true);
        world.owner.set(Some(Owner::Agent));
        world.agent_paused.set(true);
    }
    world.note(format!("legacy journal fixture kind {kind} (written by an agent from before round 6)"));
}

/// `r11 = false` replays the round-10 generator exactly (the class A/B regression seeds were found with it): every
/// round-11 addition (schedule_at_h, lost/crashed resumes, crashes after a pause, pauses right after a resume) draws
/// from a second, independent PRNG, so the original draw sequence is unchanged.
fn scenario_full(seed: u64, r11: bool, r13: bool) -> (Vec<String>, Vec<String>) {
    let mut rng = Rng::new(seed);
    let mut rng2 = Rng::new(seed ^ 0x5EED_0011_D00D_F00D);
    // Round 13 draws come from a third stream so the round-10/11 draw sequences (and their regression seeds) are kept.
    let mut rng3 = Rng::new(seed ^ 0x0D15_EA5E_1313_C0DE);
    let dir = tempfile::tempdir().unwrap();
    let coordinated = rng.chance(40);
    let fail_freeze = rng.chance(8);
    let scheduled = r11 && rng2.chance(50);
    let no_abort_hook = r13 && rng3.chance(20);
    let legacy = if r13 && rng3.chance(10) { Some(rng3.below(3)) } else { None };
    let cfg = model_config_full(dir.path(), coordinated, fail_freeze, scheduled, no_abort_hook);
    let world = World::new(dir.path());
    world.scheduler.set(scheduled);
    world.no_abort_hook.set(no_abort_hook);
    world.note(format!("seed {seed}: coordinated={coordinated} fail_freeze={fail_freeze} scheduled={scheduled} \
        no_abort_hook={no_abort_hook} legacy={legacy:?}"));

    // Faults.
    if rng.chance(45) {
        world.ext_pause_tick.set(1 + rng.below(40));
        if rng.chance(50) {
            world.ext_resume_tick.set(world.ext_pause_tick.get() + 1 + rng.below(30));
        }
    }
    if rng.chance(20) {
        world.paused_read_fails.set(1 + rng.below(3) as u32);
    }
    if rng.chance(15) {
        world.burnoff_tx.set(1);
    }
    if rng.chance(15) {
        world.target_stuck.set(true);
    }
    if rng.chance(20) {
        world.reopen_fails.set(1 + rng.below(2) as u32);
    }
    if rng.chance(12) {
        world.resume_fails.set(1 + rng.below(2) as u32);
    }
    if rng.chance(10) {
        world.resume_ignored.set(1);
    }
    if r11 {
        if rng2.chance(10) {
            world.resume_lost.set(1);
        }
        if rng2.chance(8) {
            world.crash_after_resume.set(true);
        }
        if rng2.chance(10) {
            world.crash_after_pause.set(true);
        }
        if rng2.chance(10) {
            world.ext_pause_after_resume.set(true);
        }
        if rng2.chance(5) {
            world.ext_pause_right_after_resume.set(true);
        }
    }
    if r13 {
        if rng3.chance(6) {
            // An operator un-pauses the agent's own pause by hand, and someone pauses again later.
            world.ext_resume_agent_tick.set(1 + rng3.below(60));
            if rng3.chance(70) {
                world.ext_pause2_tick.set(world.ext_resume_agent_tick.get() + 1 + rng3.below(30));
            }
        }
        if rng3.chance(12) {
            // A producer-API outage right after a resume: short (inside the 30 s retry) or long (outlasting it).
            world.read_outage_after_resume.set(if rng3.chance(50) { 1 + rng3.below(4) as u32 } else { 5_000 });
        }
    }
    if coordinated && rng.chance(35) {
        world.coord_abort_tick.set(1 + rng.below(40));
    }
    if rng.chance(60) {
        world.crash_at.set(1 + rng.below(250));
    }
    world.note(format!("faults: ext_pause@{} ext_resume@{} read_fails={} burnoff_tx={} target_stuck={} coord_abort@{} crash@{} reopen_fails={} resume_fails={} resume_ignored={}",
        world.ext_pause_tick.get(), world.ext_resume_tick.get(), world.paused_read_fails.get(), world.burnoff_tx.get(),
        world.target_stuck.get(), world.coord_abort_tick.get(), world.crash_at.get(), world.reopen_fails.get(),
        world.resume_fails.get(), world.resume_ignored.get()));
    world.note(format!("faults(r11): resume_lost={} crash_after_resume={} crash_after_pause={} ext_pause_after_resume={}",
        world.resume_lost.get(), world.crash_after_resume.get(), world.crash_after_pause.get(), world.ext_pause_after_resume.get()));

    // Step 1: the ceremony runs (maybe crashing), or a legacy journal is found on disk (round 13).
    let first = match legacy {
        Some(kind) => {
            write_legacy_journal(&world, kind);
            StepResult::Done(Ok("legacy fixture".into()))
        }
        None => step_run(&cfg, &world),
    };
    world.note(format!("run: {first:?}"));
    // Round 13: the operator-abort sequence (`cutover.sh abort`: the agent is killed at an arbitrary operation, then a
    // plain rollback runs straight away).
    let mut op_abort = r13 && matches!(first, StepResult::Crashed) && rng3.chance(50);
    if let StepResult::Panicked(m) = &first {
        if m.contains("MODEL-NO-PROGRESS") {
            world.violate("I6", format!("run made no progress: {m}"));
        } else {
            world.violate("PANIC", format!("unexpected panic in run: {m}"));
        }
    }
    check_after_step(&world, "run");

    // Steps 2..: what an operator / the world might do next.
    let mut strand_runs = 0;
    let steps = 2 + rng.below(4);
    for _ in 0..steps {
        let state = journal_state(&world).ok().flatten();
        if state == Some(State::Live) && rng.chance(70) {
            break; // a LIVE cut is done; sometimes an operator still pokes at it
        }
        // Between steps the outside world may act (not inside the operator-abort sequence).
        // Same draw sequence as rounds 10-12 (regression seeds replay exactly).
        let ext = rng.below(4);
        let crash_at = if rng.chance(25) { Some(world.calls.get() + 1 + rng.below(120)) } else { None };
        if !op_abort {
            match ext {
                0 => world.external_pause(),
                1 => world.external_resume(),
                _ => {}
            }
            if r13 && rng3.chance(5) {
                world.external_resume_of_agent_pause();
            }
            if let Some(c) = crash_at {
                world.crash_at.set(c);
            }
        }
        let pick = if std::mem::replace(&mut op_abort, false) { 5 } else { rng.below(10) };
        let stranded_before = state == Some(State::Stranded);
        let strands_before = strand_transitions(&world);
        let world_before = (world.paused.get(), world.owner.get(), world.writes_open.get());
        let (label, res) = match (state, pick) {
            (Some(State::Halted), 0..=4) => {
                world.begin_step();
                let r = step_unhalt(&dir.path().join("ceremony.toml"));
                world.note(format!("unhalt: {r:?}"));
                ("unhalt", StepResult::Done(r))
            }
            (_, 0..=4) => ("run", step_run(&cfg, &world)),
            (_, 5..=7) => ("rollback", step_rollback(&cfg, &world, false)),
            _ => ("rollback --force-stranded", step_rollback(&cfg, &world, true)),
        };
        world.note(format!("{label}: {res:?}"));
        if let StepResult::Panicked(m) = &res {
            if m.contains("MODEL-NO-PROGRESS") {
                world.violate("I6", format!("{label} made no progress: {m}"));
            } else {
                world.violate("PANIC", format!("unexpected panic in {label}: {m}"));
            }
        }
        if label == "run" && journal_state(&world).ok().flatten() == Some(State::Stranded) {
            strand_runs += 1;
            // I6 (strand loop): a plain re-run of a STRANDED journal, with nothing changed in the world since the step
            // began and no crash in it, stays sealed without writing another STRANDED transition.
            let unchanged = world_before == (world.paused.get(), world.owner.get(), world.writes_open.get());
            if stranded_before && unchanged && !matches!(res, StepResult::Crashed) && strand_transitions(&world) > strands_before {
                world.violate("I6", "a plain re-run of a STRANDED journal (nothing changed) wrote another STRANDED transition".into());
            }
        }
        check_after_step(&world, label);
    }
    if strand_runs > 0 {
        world.note(format!("plain runs that ended STRANDED: {strand_runs}"));
    }
    let fin = journal_state(&world).ok().flatten().map(|s| s.as_str().to_string()).unwrap_or_else(|| "none".into());
    let j = world.journal_text();
    let mut tags = vec![format!("final:{fin}")];
    for (k, needle) in [("crashed", "crash at op"), ("ext_pause", "external pause"), ("adopted", "\"adopted\":true"),
                        ("forced_release", "external_pause_release"), ("writes_kept_frozen", "writes_kept_frozen"),
                        ("nothing_to_undo", "nothing_to_undo"), ("scheduled", "snapshot_scheduled_at"),
                        ("sched_file_wait", "scheduled_snapshot_file"), ("kept_paused_on_resume", "kept_paused_on_resume"),
                        ("resumed_for_snapshot", "resumed_production_for_snapshot"), ("ownership_unknown", "\"pause_ownership_unknown\":true"),
                        ("pause_ack", "source_pause_ack"), ("resume_lost", "resume took effect but the answer"),
                        ("crash_after_resume", "crash right after the resume"),
                        ("no_abort_hook", "no_abort_hook=true"), ("legacy", "legacy journal fixture"),
                        ("ext_resume_of_agent_pause", "external resume of the AGENT's pause"),
                        ("read_outage_after_resume", "outage right after the resume"),
                        ("resume_outcome_unknown", "source_resume_outcome_unknown")] {
        let hay = format!("{j}\n{}", world.trace.borrow().join("\n"));
        if hay.contains(needle) { tags.push(k.to_string()); }
    }
    let v = world.violations.borrow().clone();
    let mut t = world.trace.borrow().clone();
    t.push(format!("TAGS {}", tags.join(",")));
    (v, t)
}

fn quiet_panics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload().downcast_ref::<String>().cloned()
                .or_else(|| info.payload().downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default();
            if payload == CRASH || payload.starts_with("MODEL-NO-PROGRESS") {
                return;
            }
            prev(info)
        }));
    });
}

/// Regression seeds of class R10-1 (Codex round 10, fixed in round 11): a resume that TOOK EFFECT but whose answer was
/// lost, or after which the agent was killed before journaling it, used to leave the agent's pause episode open, so a
/// later external pause was resumed as ours. Kept as a regression list (`model_known_open_seeds_all_clean`).
const KNOWN_OPEN: &[u64] = &[287, 319, 675, 1447, 1959, 2067, 2225, 2553, 2558, 2659, 2866];

/// Class R11-A (found by the round-11 checker, fixed in round 12): a STALE forced release used to authorize resuming a
/// LATER external pause. Regression test: `model_i3_class_r11_a_stale_forced_release_seed_13295`.
fn run_many(start: u64, count: u64) -> Vec<(u64, Vec<String>, Vec<String>)> {
    quiet_panics();
    let mut bad = Vec::new();
    let mut stats: std::collections::BTreeMap<String, u64> = Default::default();
    let mut open: Vec<u64> = Vec::new();
    for seed in start..start + count {
        let (v, t) = scenario(seed);
        if t.iter().any(|l| l.starts_with("class R10-1 shape")) {
            open.push(seed);
        }
        if let Some(tags) = t.last().and_then(|l| l.strip_prefix("TAGS ")) {
            for tag in tags.split(',') {
                *stats.entry(tag.to_string()).or_default() += 1;
            }
        }
        if !v.is_empty() {
            bad.push((seed, v, t));
        }
    }
    eprintln!("MODEL STATS over {count} scenarios: {stats:?}");
    let mut classes: std::collections::BTreeMap<&str, Vec<u64>> = Default::default();
    for (seed, v, t) in &bad {
        classes.entry(classify(v, t)).or_default().push(*seed);
    }
    for (c, seeds) in &classes {
        eprintln!("MODEL CLASS {c}: {} seed(s): {:?}", seeds.len(), &seeds[..seeds.len().min(40)]);
    }
    if !open.is_empty() {
        eprintln!("MODEL: {} scenario(s) reached the class R10-1 shape (a resume that took effect unseen)", open.len());
    }
    bad
}

/// Round 13: the class of a violating scenario (for the long-run report and the regression tests).
/// - F1-A (Codex final round, F1): a rollback completes ABORTED over a paused producer in a config without `on_abort`
///   (the producer-state confirmation only runs when a reopen hook is pending);
/// - R13-A: the agent's acknowledged pause is un-paused by someone else and paused again, unseen; the agent later
///   resumes that external pause as its own;
/// - R13-B: an older agent's journal is stranded by this agent over someone else's pause, and the legacy rule
///   ("a STRANDED state means the older agent paused") makes that pause look like ours;
/// - other: anything else (a new class).
fn classify(v: &[String], t: &[String]) -> &'static str {
    let tr = t.join("\n");
    let i3 = v.iter().any(|x| x.starts_with("I3"));
    let i1_i7 = v.iter().any(|x| x.starts_with("I1") || x.starts_with("I7"));
    if i3 && tr.contains("external resume of the AGENT's pause") {
        "R13-A"
    } else if i3 && tr.contains("legacy journal fixture") {
        "R13-B"
    } else if i1_i7 && tr.contains("no_abort_hook=true") {
        "F1-A"
    } else {
        "other"
    }
}

fn report(bad: &[(u64, Vec<String>, Vec<String>)]) -> String {
    let mut out = String::new();
    for (seed, v, t) in bad.iter().take(20) {
        out.push_str(&format!("\n=== seed {seed}: {}\n", v.join(" | ")));
        for line in t {
            out.push_str(&format!("    {line}\n"));
        }
    }
    out
}

/// The CI run: seeds 1..=MODEL_SCENARIOS (default 300), or one MODEL_SEED.
#[test]
fn model_checker_finds_no_invariant_violation() {
    let (start, count) = match std::env::var("MODEL_SEED").ok().and_then(|s| s.parse::<u64>().ok()) {
        Some(s) => (s, 1),
        None => (std::env::var("MODEL_START").ok().and_then(|s| s.parse().ok()).unwrap_or(1),
                 std::env::var("MODEL_SCENARIOS").ok().and_then(|s| s.parse().ok()).unwrap_or(300)),
    };
    let bad = run_many(start, count);
    // Round 13: the classes the round-13 checker found stay visible (reported, and each has an #[ignore]d failing
    // regression test) until the source fixes land; anything else fails the run. `MODEL_STRICT=1` fails on every
    // violation: use it once OPEN_CLASSES is emptied.
    let strict = std::env::var("MODEL_STRICT").is_ok();
    let unexpected: Vec<_> = bad.iter().filter(|(_, v, t)| strict || !OPEN_CLASSES.contains(&classify(v, t))).cloned().collect();
    if !bad.is_empty() && unexpected.is_empty() {
        eprintln!("MODEL: {} scenario(s) in the open classes {OPEN_CLASSES:?} (see the #[ignore]d class tests)", bad.len());
    }
    assert!(unexpected.is_empty(), "{} of {count} scenarios violated an invariant outside the open classes:{}",
        unexpected.len(), report(&unexpected));
}

/// Classes found by the round-13 checker and still open in the source (F1-A, R13-A, R13-B; see `classify`). Empty this
/// list as each fix lands (and un-ignore its class test).
const OPEN_CLASSES: &[&str] = &[];

/// Directed: Codex round 9, R9-2. The ceremony pauses (ours), a coordinator abort makes it resume its own pause, the
/// write-reopen hook fails, someone else then pauses the producer, and an operator runs plain rollbacks. An external
/// pause must never be resumed without a journaled release.
fn directed_r9_2(dir: &Path, force_last: bool) -> Vec<String> {
    quiet_panics();
    let cfg = model_config(dir, false, false);
    let world = World::new(dir);
    world.burnoff_tx.set(1); // the burn-off audit aborts after OUR pause (uncoordinated: the abort resumes it)
    world.reopen_fails.set(1);
    let r = step_run(&cfg, &world);
    world.note(format!("run: {r:?}"));
    check_after_step(&world, "run");
    world.external_pause();
    for i in 0..3 {
        let force = force_last && i == 2;
        let r = step_rollback(&cfg, &world, force);
        world.note(format!("rollback(force_stranded={force}): {r:?}"));
        check_after_step(&world, "rollback");
    }
    let v = world.violations.borrow().clone();
    if !v.is_empty() {
        eprintln!("{}", world.trace.borrow().join("\n"));
    }
    v
}

/// FAILS on r30/integrate 5518752 (I3): un-ignore once round 9's pause-episode fix lands.
#[test]
fn directed_i3_codex_r9_2_external_pause_after_a_completed_resume() {
    let dir = tempfile::tempdir().unwrap();
    let v = directed_r9_2(dir.path(), false);
    assert!(v.is_empty(), "{v:?}");
}

fn assert_seed_clean(seed: u64) {
    assert_seed_clean_with(seed, false);
}

fn assert_seed_clean_with(seed: u64, r11: bool) {
    quiet_panics();
    let (v, t) = scenario_with(seed, r11);
    let open: Vec<&String> = t.iter().filter(|l| l.starts_with("KNOWN-OPEN")).collect();
    assert!(v.is_empty() && open.is_empty(), "seed {seed}: {} {open:?}\n{}", v.join(" | "), t.join("\n"));
}

/// Class A (= Codex R9-2), found by the checker: own pause resumed, on_abort fails, someone else pauses, a later
/// rollback (plain or --force-stranded) resumes that external pause as if it were ours (no release journaled).
#[test]
fn model_i3_class_a_seed_1300() {
    assert_seed_clean(1300);
}

#[test]
fn model_i3_class_a_seed_5955() {
    assert_seed_clean(5955);
}

/// Class B (new): the agent crashes after journaling its pause intent and before the pause RPC; someone else then
/// pauses the producer; the recovered run trusts the intent and resumes that external pause.
#[test]
fn model_i3_class_b_seed_3470() {
    assert_seed_clean(3470);
}

#[test]
fn model_i3_class_b_seed_4771() {
    assert_seed_clean(4771);
}

/// Directed, Codex round 10 (R10-2): `pause_at_h` recovery. The agent's own pause is acknowledged, the agent is killed
/// during quiescence, the re-run finds its own pause at the start of FROZEN and resumes production for the snapshot
/// (`resumed_production_for_snapshot`); someone else then pauses the producer; the burn-off audit aborts. The episode
/// the snapshot resume closed must not make that external pause "ours" (I3).
fn directed_r10_2(dir: &Path) -> Vec<String> {
    quiet_panics();
    let cfg = model_config(dir, false, false);
    let world = World::new(dir);
    world.crash_on_poll_while_paused.set(true);
    let r = step_run(&cfg, &world);
    world.note(format!("run: {r:?}"));
    check_after_step(&world, "run");
    world.ext_pause_right_after_resume.set(true);
    let r = step_run(&cfg, &world);
    world.note(format!("re-run: {r:?}"));
    check_after_step(&world, "re-run");
    for _ in 0..2 {
        let r = step_rollback(&cfg, &world, false);
        world.note(format!("rollback: {r:?}"));
        check_after_step(&world, "rollback");
    }
    let j = world.journal_text();
    // Round 12: the scenario must reach the snapshot resume ATTEMPT (its intent); with the external pause right after
    // it, the outcome is ownership unknown, so there is no `resumed_production_for_snapshot` record any more.
    assert!(j.contains("\"during\":\"resuming production for the snapshot\""), "the scenario must reach the snapshot resume:\n{}",
        world.trace.borrow().join("\n"));
    if std::env::var("MODEL_TRACE").is_ok() { eprintln!("{}", world.trace.borrow().join("\n")); }
    let v = world.violations.borrow().clone();
    if !v.is_empty() {
        eprintln!("{}", world.trace.borrow().join("\n"));
    }
    v
}

/// FAILS on r30/integrate 972d37b (I3, Codex R10-2): `resumed_production_for_snapshot` does not close the agent's
/// pause episode, so a pause someone else makes right after it is resumed by the abort as if it were ours.
#[test]
fn directed_i3_codex_r10_2_pause_at_h_snapshot_resume() {
    let dir = tempfile::tempdir().unwrap();
    let v = directed_r10_2(dir.path());
    assert!(v.is_empty(), "{v:?}");
}

// ---- class R10-1 (Codex round 10): lost or crashed resume leaves the episode open. FAIL on 972d37b. ----

fn assert_r11_seed(seed: u64) {
    assert_seed_clean_with(seed, true);
}

#[test]
fn model_i3_class_r10_1_lost_resume_answer_seed_319() {
    assert_r11_seed(319);
}

#[test]
fn model_i3_class_r10_1_crash_after_resume_seed_1959() {
    assert_r11_seed(1959);
}

#[test]
fn model_i3_class_r10_1_crash_after_pause_and_resume_seed_675() {
    assert_r11_seed(675);
}

#[test]
fn model_i3_class_r10_1_lost_resume_after_release_seed_2067() {
    assert_r11_seed(2067);
}

/// Every KNOWN_OPEN seed, in one test: un-ignore with the fix and empty KNOWN_OPEN.
#[test]
fn model_known_open_seeds_all_clean() {
    for &seed in KNOWN_OPEN {
        assert_r11_seed(seed);
    }
}

/// Class R11-A: a stale forced release authorizes resuming a LATER external pause. FAILS on 972d37b.
#[test]
fn model_i3_class_r11_a_stale_forced_release_seed_13295() {
    assert_r11_seed(13295);
}

// ---- round 13: the oracles themselves, and the classes the round-13 checker found (open until the source fixes). ----

thread_local! {
    /// Round 14: set by the residual test only (see `model_residual_r13_a_unpause_and_repause_between_two_reads`).
    static R13_GAP_FREE: Cell<bool> = const { Cell::new(false) };
}

fn assert_seed_clean_r13(seed: u64) {
    quiet_panics();
    let (v, t) = scenario(seed);
    assert!(v.is_empty(), "seed {seed} [{}]:{}", classify(&v, &t), report(&[(seed, v.clone(), t.clone())]));
}

/// I3 is run-local and consumed per attempt (round 13). A release journaled by an EARLIER run authorizes nothing (the
/// R11-A shape: a stale forced release must not license resuming a later external pause), and a release journaled by
/// this run authorizes exactly one attempt, even one the producer refuses.
#[test]
fn oracle_i3_release_is_run_local_and_consumed_per_attempt() {
    quiet_panics();
    let release = "{\"kind\":\"evidence\",\"state\":\"STRANDED\",\"data\":{\"side_effect\":\"external_pause_release\"}}\n";
    // 1. A release from an earlier run; this run resumes a new external pause: flagged.
    let dir = tempfile::tempdir().unwrap();
    let world = World::new(dir.path());
    std::fs::write(&world.journal, release).unwrap();
    world.external_pause();
    world.begin_step();
    let _ = world.resume();
    assert!(world.violations.borrow().iter().any(|v| v.starts_with("I3")), "a stale release must not authorize a resume");
    // 2. A release journaled by this run: one attempt (here refused by the producer) consumes it; a second attempt in
    //    the same run without a new release is flagged.
    let dir = tempfile::tempdir().unwrap();
    let world = World::new(dir.path());
    std::fs::write(&world.journal, "").unwrap();
    world.external_pause();
    world.begin_step();
    std::fs::write(&world.journal, release).unwrap(); // journaled by this run, after the pause began
    world.resume_fails.set(1);
    assert!(world.resume().is_err());
    assert!(world.violations.borrow().is_empty(), "a live release authorizes one attempt: {:?}", world.violations.borrow());
    let _ = world.resume();
    assert!(world.violations.borrow().iter().any(|v| v.starts_with("I3")), "the refused attempt consumed the release");
    // 3. A release journaled before the external pause began authorizes nothing for that pause.
    let dir = tempfile::tempdir().unwrap();
    let world = World::new(dir.path());
    std::fs::write(&world.journal, "").unwrap();
    world.begin_step();
    std::fs::write(&world.journal, release).unwrap();
    world.external_pause();
    let _ = world.resume();
    assert!(world.violations.borrow().iter().any(|v| v.starts_with("I3")), "a release for an earlier pause authorizes nothing");
}

/// I7 scope (round 13): completion over a paused producer is flagged only for a ceremony that froze writes; a refused
/// ARM (someone else's pause before the ceremony started) has nothing to undo and is clean (seed 46 shape).
#[test]
fn oracle_i7_ignores_a_refused_arm_over_someone_elses_pause() {
    assert_seed_clean_r13(46);
}

/// Class F1-A (Codex final round, F1), found by the round-13 checker: in a config without `on_abort`, a rollback
/// completes ABORTED over a paused producer, because the producer-state confirmation only runs when a reopen hook is
/// pending. Seed 52: someone else's pause, the agent's freeze hook failed. Seed 233: an older agent's own pause whose
/// resume answered Ok without effect.
#[test]
fn model_i7_class_f1_a_no_abort_hook_external_pause_seed_52() {
    assert_seed_clean_r13(52);
}

#[test]
fn model_i1_class_f1_a_no_abort_hook_ignored_resume_seed_233() {
    assert_seed_clean_r13(233);
}

/// Class R13-A, found by the round-13 checker: the agent's acknowledged pause is un-paused by someone else and paused
/// again later without the agent observing the gap; the agent then resumes that external pause as its own (seed 16).
/// The gap is only observable through a producer read while the agent believes its pause holds.
#[test]
fn model_i3_class_r13_a_external_resume_of_agent_pause_seed_16() {
    // Round 14: closed for the case the agent can see: a read between the un-pause and the re-pause closes its episode.
    assert_seed_clean_r13(16);
}

/// The INHERENT residual of R13-A (round 14, docs/RISK-REGISTER.md A-21): someone un-pauses this ceremony's pause and
/// pauses the producer again entirely between two of the agent's reads. Leap's producer API does not say who paused,
/// so no read can tell the two pauses apart, and a later rollback resumes the second pause as the agent's own. This
/// test fails by design and stays ignored: it documents the limit. The mitigation is operational (never pause or
/// resume a producer by hand during a ceremony) and the agent closes its episode whenever it sees the producer running.
#[test]
#[ignore = "documents an inherent residual (A-21): an un-pause and re-pause between two agent reads cannot be detected"]
fn model_residual_r13_a_unpause_and_repause_between_two_reads() {
    R13_GAP_FREE.with(|g| g.set(true));
    assert_seed_clean_r13(16);
}

/// Class R13-B, found by the round-13 checker (a source bug): a journal from an agent older than round 6 is stranded by
/// THIS agent over someone else's pause ("already paused"); the legacy rule "a STRANDED state means the older agent
/// paused" then makes that external pause look like ours, and a plain rollback resumes it with no release (seed 242).
#[test]
fn model_i3_class_r13_b_legacy_strand_fabricates_ownership_seed_242() {
    assert_seed_clean_r13(242);
}

/// Class R13-B, second shape (round 14, found by the 20,000-scenario run): this agent strands an older agent's journal
/// over someone else's pause (preflight fails on an unreadable producer), and a later rollback re-derived the older
/// agent's ownership from the STRANDED state this agent wrote. Round 14 fixes the older agent's ownership once, in a
/// `legacy_takeover` record, the first time this agent opens the journal.
#[test]
fn model_i3_class_r13_b_legacy_takeover_seed_913() {
    assert_seed_clean_r13(913);
}
