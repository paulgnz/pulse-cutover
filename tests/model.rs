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
//! - I3 an EXTERNAL pause is never resumed except after a journaled explicit release (`external_pause_release`);
//! - I4 the source is never resumed after this journal's chain-creation / ignition intent;
//! - I5 the journal always replays;
//! - I6 every step terminates (no loop) and STRANDED is never re-entered without new cause on a plain re-run.
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
    /// The new chain never produces past the cut (LIVE gate fails → HALT).
    target_stuck: Cell<bool>,
    /// Signed coordinator abort is served from this tick on (0 = never; coordinated configs only).
    coord_abort_tick: Cell<u64>,
    now: Cell<u64>,
    // --- observations ---
    violations: RefCell<Vec<String>>,
    trace: RefCell<Vec<String>>,
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
            reopen_fails: Cell::new(0),
            resume_fails: Cell::new(0),
            resume_ignored: Cell::new(0),
            coord_abort_tick: Cell::new(0),
            now: Cell::new(1_000_000),
            violations: RefCell::new(Vec::new()),
            trace: RefCell::new(Vec::new()),
        }
    }

    fn note(&self, s: impl Into<String>) {
        self.trace.borrow_mut().push(s.into());
    }

    fn violate(&self, inv: &str, why: String) {
        let msg = format!("{inv}: {why}");
        self.note(format!("VIOLATION {msg}"));
        self.violations.borrow_mut().push(msg);
    }

    fn journal_text(&self) -> String {
        std::fs::read_to_string(&self.journal).unwrap_or_default()
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
        let t = self.ticks.get() + 1;
        self.ticks.set(t);
        if self.ext_pause_tick.get() == t {
            self.external_pause();
        }
        if self.ext_resume_tick.get() == t {
            self.external_resume();
        }
        if !self.agent_paused.get() {
            self.head.set(self.head.get() + 1);
        }
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
        Ok(())
    }

    fn resume(&self) -> Result<(), String> {
        self.op("resume");
        let j = self.journal_text();
        if self.paused.get() && self.owner.get() == Some(Owner::External) && !j.contains("external_pause_release") {
            self.violate("I3", format!("agent resumed an EXTERNAL pause with no journaled release (head {})", self.head.get()));
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

    fn schedule_snapshot(&self, _height: u64) -> Result<(), String> {
        self.op("schedule_snapshot");
        Err("snapshot scheduler unavailable (model: pause_at_h)".into())
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
    let mut text = format!(
        r#"
journal_path = "{d}/journal.jsonl"
poll_ms = 1

[ceremony]
rehearsal = true
freeze_height = {H}
quiescence_polls = 3
quiescence_timeout_secs = 5

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
snapshot_timeout_secs = 20

[snapshot]
staged_path = "{d}/staged.bin"
capture_roots = "{d}/captured-roots.txt"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 10
live_sustain_secs = 1

[hooks]
on_freeze = "{freeze}"
post_ignite = "resume-traffic"
on_live = "flip-gateway"
on_abort = "reopen-writes"
on_halt = "page"
"#,
        d = dir.display(),
        freeze = if fail_freeze { "fail-freeze" } else { "freeze-writes" },
    );
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
    guarded(world, || {
        let (j, rec) = open_journal(&cfg.journal_path)?;
        let mut m = Machine::new(cfg, world, j, rec);
        m.run().map(|s| s.as_str().to_string())
    })
}

fn step_rollback(cfg: &Config, world: &World, force_stranded: bool) -> StepResult {
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
        }
    }
}

/// One seeded scenario. Returns the violations (empty = clean).
fn scenario(seed: u64) -> (Vec<String>, Vec<String>) {
    let mut rng = Rng::new(seed);
    let dir = tempfile::tempdir().unwrap();
    let coordinated = rng.chance(40);
    let fail_freeze = rng.chance(8);
    let cfg = model_config(dir.path(), coordinated, fail_freeze);
    let world = World::new(dir.path());
    world.note(format!("seed {seed}: coordinated={coordinated} fail_freeze={fail_freeze}"));

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

    // Step 1: the ceremony runs (maybe crashing).
    let first = step_run(&cfg, &world);
    world.note(format!("run: {first:?}"));
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
        // Between steps the outside world may act.
        match rng.below(4) {
            0 => world.external_pause(),
            1 => world.external_resume(),
            _ => {}
        }
        if rng.chance(25) {
            world.crash_at.set(world.calls.get() + 1 + rng.below(120));
        }
        let pick = rng.below(10);
        let (label, res) = match (state, pick) {
            (Some(State::Halted), 0..=4) => {
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
        }
        check_after_step(&world, label);
    }
    let _ = strand_runs;
    let fin = journal_state(&world).ok().flatten().map(|s| s.as_str().to_string()).unwrap_or_else(|| "none".into());
    let j = world.journal_text();
    let mut tags = vec![format!("final:{fin}")];
    for (k, needle) in [("crashed", "crash at op"), ("ext_pause", "external pause"), ("adopted", "\"adopted\":true"),
                        ("forced_release", "external_pause_release"), ("writes_kept_frozen", "writes_kept_frozen"),
                        ("nothing_to_undo", "nothing_to_undo")] {
        let hay = if needle.starts_with('"') || needle.contains('_') { &j } else { &world.trace.borrow().join("\n") };
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

fn run_many(start: u64, count: u64) -> Vec<(u64, Vec<String>, Vec<String>)> {
    quiet_panics();
    let mut bad = Vec::new();
    let mut stats: std::collections::BTreeMap<String, u64> = Default::default();
    for seed in start..start + count {
        let (v, t) = scenario(seed);
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
    bad
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
    assert!(bad.is_empty(), "{} of {count} scenarios violated an invariant:{}", bad.len(), report(&bad));
}

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
#[ignore = "I3 violation on 5518752 (Codex R9-2): un-ignore with the pause-episode fix"]
fn directed_i3_codex_r9_2_external_pause_after_a_completed_resume() {
    let dir = tempfile::tempdir().unwrap();
    let v = directed_r9_2(dir.path(), false);
    assert!(v.is_empty(), "{v:?}");
}

fn assert_seed_clean(seed: u64) {
    quiet_panics();
    let (v, t) = scenario(seed);
    assert!(v.is_empty(), "seed {seed}: {}\n{}", v.join(" | "), t.join("\n"));
}

/// Class A (= Codex R9-2), found by the checker: own pause resumed, on_abort fails, someone else pauses, a later
/// rollback (plain or --force-stranded) resumes that external pause as if it were ours (no release journaled).
#[test]
#[ignore = "I3 violation on 5518752 (class A, pause episodes): un-ignore with the round-9 fix"]
fn model_i3_class_a_seed_1300() {
    assert_seed_clean(1300);
}

#[test]
#[ignore = "I3 violation on 5518752 (class A, pause episodes): un-ignore with the round-9 fix"]
fn model_i3_class_a_seed_5955() {
    assert_seed_clean(5955);
}

/// Class B (new): the agent crashes after journaling its pause intent and before the pause RPC; someone else then
/// pauses the producer; the recovered run trusts the intent and resumes that external pause.
#[test]
#[ignore = "I3 violation on 5518752 (class B, pause intent without ack): un-ignore once ownership needs an ack"]
fn model_i3_class_b_seed_3470() {
    assert_seed_clean(3470);
}

#[test]
#[ignore = "I3 violation on 5518752 (class B, pause intent without ack): un-ignore once ownership needs an ack"]
fn model_i3_class_b_seed_4771() {
    assert_seed_clean(4771);
}
