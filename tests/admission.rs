//! rc.32 (F1): rehearsal-style mempool / expiry harness for the producer admission lock.
//!
//! The 8-BP rehearsal (fleet-step3 run s1) aborted the whole fleet: three transfers with a 120 s expiry, admitted
//! before the freeze, sat in one producer's unapplied queue (nodeos 5.0.3 keeps a transaction it could not apply
//! and retries it at the start of every block it builds, until it expires) and landed in the first block of that
//! producer's next slot, H+25. The burn-off audit aborted, correctly. The harness's workaround (a 10 s expiry for
//! test traffic) does not exist on mainnet, where an expiry can be up to `max_transaction_lifetime` (3,600 s on XPR).
//!
//! This world models what matters, in nodeos 5.0.3 terms:
//! - a DPoS schedule of `N` producers with 12-block slots and 0.5 s blocks;
//! - per-producer unapplied queues: a queued transaction lands in the next block its holder PRODUCES after it was
//!   admitted, if the block time is not past its expiry (expiry is objective: no producer can include it later);
//! - the subjective actor whitelist: a producer whose whitelist is the admission-lock sentinel drops every queued
//!   transaction instead of including it (`actor_whitelist_exception` is a failure, not an "exhausted" retry);
//! - "participating" producers run an agent: when this node's agent sets its lock, theirs do too (the stand-in for
//!   their own agents reaching the same freeze height); a non-participating producer never locks.
//!
//! The REAL `Machine` drives this world from ARM to LIVE or ABORTED.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};

use pulse_cutover::{
    config::{Config, ADMISSION_LOCK_SENTINEL},
    journal::Journal,
    machine::Machine,
    ops::{ChainInfo, ChainOps, SnapshotResult},
    state::State,
};
use pulsevm_snapshot::testing::{MiniSnapshot, TestAccount};

const CHAIN_ID: [u8; 32] = [0xAB; 32];
const H: u64 = 120;
const LEAD: u64 = 24;
const SLOT: u64 = 12;
/// DPoS finality lag: production must continue this far past H before H is final (s1: LIB reached H at H+325).
const LIB_LAG: u64 = 40;

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

#[derive(Clone, Debug)]
struct QueuedTrx {
    holder: usize,
    admitted_block: u64,
    expiry_secs: u64,
}

struct Mempool {
    snap_dir: PathBuf,
    producers: usize,
    /// Producers that run an agent (index 0 is this node).
    participating: Vec<bool>,
    head: Cell<u64>,
    paused: Cell<bool>,
    scheduled_h: Cell<u64>,
    queue: RefCell<Vec<QueuedTrx>>,
    /// Transactions per produced block, and who produced it.
    included: RefCell<std::collections::BTreeMap<u64, u64>>,
    /// This node's actor whitelist (nodeos answer to get_whitelist_blacklist).
    whitelist: RefCell<Vec<String>>,
    /// Whitelist set calls, in order.
    whitelist_sets: RefCell<Vec<Vec<String>>>,
    locked: RefCell<Vec<bool>>,
    dropped: Cell<u64>,
    writes_open: Cell<bool>,
    ignited: Cell<bool>,
    traffic: Cell<bool>,
    target_head: Cell<u64>,
    now: Cell<u64>,
    /// The next set_actor_whitelist fails (producer API refuses).
    set_fails: Cell<bool>,
    /// The next set_actor_whitelist panics before it applies (the agent is killed after the intent record).
    crash_on_set: Cell<bool>,
    /// rc.32 review: nodeos on this node restarts when the chain reaches this block (0 = never): the runtime whitelist
    /// falls back to the configured list (empty) and its lock is gone.
    restart_at: Cell<u64>,
    /// Test hook: nodeos restarts (lock gone) on the agent's first source_block_id read once the scheduled snapshot
    /// file exists: after the last freeze-window tick, before the pause.
    restart_after_snapshot: Cell<bool>,
    /// rc.32 (fleet run r3): nodeos goes down when the chain is about to produce this block (0 = never) for
    /// `outage_ms` of agent time, and comes back with its configured (empty) whitelist.
    outage_at: Cell<u64>,
    outage_ms: Cell<u64>,
    /// nodeos restarts at the moment of the pause call, unreachable for this long (0 = never): the pause RPC fails,
    /// the restarted node comes back unpaused with its configured (empty) whitelist.
    outage_on_pause_ms: Cell<u64>,
    /// rc.32 (fleet run r3b): nodeos restarts right after the pause (here: when the burn-off audit reads its first
    /// block, after quiescence passed): unpaused, its configured (empty) whitelist back.
    restart_on_audit: Cell<bool>,
    /// The agent process dies (panic) on this source poll while this node is paused (0 = never).
    crash_on_paused_poll: Cell<u32>,
    /// The agent process dies (panic) on the source poll that would produce this block (0 = never).
    crash_at: Cell<u64>,
    down_until: Cell<u64>,
    /// get_whitelist_blacklist is not reachable (a proxy exposing only part of /v1/producer).
    whitelist_read_fails: Cell<bool>,
    /// The whitelist is emptied on the first block-id read while this node is paused (the cut pin, after the last
    /// quiescence tick and before the burn-off audit's re-check).
    lock_drop_when_paused: Cell<bool>,
    /// When the burn-off audit reads its first block (after every re-check), someone else sets the whitelist to this.
    external_whitelist_on_audit: RefCell<Option<Vec<String>>>,
    /// Once this node is paused, someone else sets the whitelist to this list.
    external_whitelist_on_pause: RefCell<Option<Vec<String>>>,
}

impl Mempool {
    fn new(dir: &Path, producers: usize, participating: Vec<bool>, start_head: u64) -> Self {
        let snap_dir = dir.join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();
        Mempool {
            snap_dir,
            producers,
            participating,
            head: Cell::new(start_head),
            paused: Cell::new(false),
            scheduled_h: Cell::new(0),
            queue: RefCell::new(vec![]),
            included: RefCell::new(Default::default()),
            whitelist: RefCell::new(vec![]),
            whitelist_sets: RefCell::new(vec![]),
            locked: RefCell::new(vec![false; producers]),
            dropped: Cell::new(0),
            writes_open: Cell::new(true),
            ignited: Cell::new(false),
            traffic: Cell::new(false),
            target_head: Cell::new(0),
            now: Cell::new(0),
            set_fails: Cell::new(false),
            restart_at: Cell::new(0),
            whitelist_read_fails: Cell::new(false),
            restart_after_snapshot: Cell::new(false),
            outage_at: Cell::new(0),
            crash_at: Cell::new(0),
            crash_on_paused_poll: Cell::new(0),
            restart_on_audit: Cell::new(false),
            outage_on_pause_ms: Cell::new(0),
            outage_ms: Cell::new(0),
            down_until: Cell::new(0),
            crash_on_set: Cell::new(false),
            external_whitelist_on_pause: RefCell::new(None),
            external_whitelist_on_audit: RefCell::new(None),
            lock_drop_when_paused: Cell::new(false),
        }
    }

    fn producer_of(&self, block: u64) -> usize {
        ((block / SLOT) as usize) % self.producers
    }

    fn block_time_secs_x2(block: u64) -> u64 {
        block // 0.5 s blocks: time in half-seconds == block number
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
            last_irreversible_block_num: head.saturating_sub(LIB_LAG),
        }
    }

    /// Produce block `b`: its producer includes (or, when locked, drops) every transaction it holds that was
    /// admitted before `b` and has not expired by `b`'s time.
    fn produce(&self, b: u64) {
        if self.restart_at.get() == b {
            self.whitelist.borrow_mut().clear();
            self.locked.borrow_mut()[0] = false;
        }
        let p = self.producer_of(b);
        let locked = self.locked.borrow()[p];
        let mut q = self.queue.borrow_mut();
        let mut n = 0u64;
        q.retain(|t| {
            if t.holder != p || t.admitted_block >= b {
                return true;
            }
            let expires_x2 = Self::block_time_secs_x2(t.admitted_block) + 2 * t.expiry_secs;
            if Self::block_time_secs_x2(b) > expires_x2 {
                return false; // expired: objective, nobody can include it any more
            }
            if locked {
                self.dropped.set(self.dropped.get() + 1);
                return false; // actor_whitelist_exception: a failure, erased from the unapplied queue
            }
            n += 1;
            false
        });
        if n > 0 {
            self.included.borrow_mut().insert(b, n);
        }
        // The scheduler writes the snapshot of H once H is final.
        let h = self.scheduled_h.get();
        if h > 0 && b.saturating_sub(LIB_LAG) >= h {
            let file = self
                .snap_dir
                .join(format!("snapshot-{}.bin", self.block_id(h)));
            if !file.exists() {
                std::fs::write(&file, mini(h as u32).build()).unwrap();
                self.target_head.set(h);
            }
        }
    }

    fn admit(&self, holder: usize, admitted_block: u64, expiry_secs: u64, count: usize) {
        for _ in 0..count {
            self.queue.borrow_mut().push(QueuedTrx {
                holder,
                admitted_block,
                expiry_secs,
            });
        }
    }

    fn landed_after(&self, h: u64) -> u64 {
        self.included.borrow().range(h + 1..).map(|(_, n)| n).sum()
    }
}

impl Mempool {
    /// The outage: every source RPC fails with the error the r3 crash showed while nodeos is down.
    fn down(&self) -> Result<(), String> {
        if self.now.get() < self.down_until.get() {
            return Err("http://127.0.0.1:8890/v1/chain/get_info: Network Error: Connection reset by peer (os error 104)".into());
        }
        Ok(())
    }
}

impl ChainOps for Mempool {
    fn source_info(&self) -> Result<ChainInfo, String> {
        if self.outage_at.get() > 0 && self.head.get() + 1 == self.outage_at.get() {
            self.outage_at.set(0);
            self.down_until.set(self.now.get() + self.outage_ms.get());
            self.whitelist.borrow_mut().clear(); // the restart reloads the configured whitelist
            self.locked.borrow_mut()[0] = false;
        }
        self.down()?;
        if self.paused.get() && self.crash_on_paused_poll.get() > 0 {
            let n = self.crash_on_paused_poll.get() - 1;
            self.crash_on_paused_poll.set(n);
            if n == 0 {
                panic!("CRASH-INJECTED: the agent died after the pause");
            }
        }
        if self.crash_at.get() > 0 && self.head.get() + 1 == self.crash_at.get() {
            self.crash_at.set(0);
            panic!("CRASH-INJECTED: the agent died in the freeze window");
        }
        if !self.paused.get() {
            let b = self.head.get() + 1;
            self.produce(b);
            self.head.set(b);
        }
        Ok(self.info(self.head.get()))
    }
    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        self.down()?;
        if self.paused.get() && self.lock_drop_when_paused.replace(false) {
            self.whitelist.borrow_mut().clear();
            self.locked.borrow_mut()[0] = false;
        }
        let h = self.scheduled_h.get();
        if h > 0
            && self.restart_after_snapshot.get()
            && self
                .snap_dir
                .join(format!("snapshot-{}.bin", self.block_id(h)))
                .exists()
        {
            self.restart_after_snapshot.set(false);
            self.whitelist.borrow_mut().clear();
            self.locked.borrow_mut()[0] = false;
        }
        Ok((self.block_id(block_num), "2024-01-01T00:00:00.000".into()))
    }
    fn source_block_tx_count(&self, block_num: u64) -> Result<u64, String> {
        self.down()?;
        if let Some(l) = self.external_whitelist_on_audit.borrow_mut().take() {
            *self.whitelist.borrow_mut() = l;
        }
        if self.restart_on_audit.replace(false) {
            self.paused.set(false);
            self.whitelist.borrow_mut().clear();
            self.locked.borrow_mut()[0] = false;
            // The restarted producer makes a block before the agent's next tick.
            let b = self.head.get() + 1;
            self.produce(b);
            self.head.set(b);
        }
        Ok(*self.included.borrow().get(&block_num).unwrap_or(&0))
    }
    fn source_block(
        &self,
        block_num: u64,
        _rpc: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        Ok(
            serde_json::json!({"block_num": block_num, "producer": format!("prod{}", self.producer_of(block_num))}),
        )
    }
    fn producer_paused(&self) -> Result<bool, String> {
        self.down()?;
        Ok(self.paused.get())
    }
    fn pause(&self) -> Result<(), String> {
        let ms = self.outage_on_pause_ms.replace(0);
        if ms > 0 {
            self.down_until.set(self.now.get() + ms);
            self.whitelist.borrow_mut().clear();
            self.locked.borrow_mut()[0] = false;
            self.paused.set(false);
        }
        self.down()?;
        self.paused.set(true);
        if let Some(l) = self.external_whitelist_on_pause.borrow_mut().take() {
            *self.whitelist.borrow_mut() = l;
        }
        Ok(())
    }
    fn resume(&self) -> Result<(), String> {
        self.paused.set(false);
        Ok(())
    }
    fn create_snapshot(&self) -> Result<SnapshotResult, String> {
        Err("this world only schedules snapshots".into())
    }
    fn schedule_snapshot(&self, height: u64) -> Result<(), String> {
        self.scheduled_h.set(height);
        Ok(())
    }
    fn actor_whitelist(&self) -> Result<Option<Vec<String>>, String> {
        self.down()?;
        if self.whitelist_read_fails.get() {
            return Err("http://mock/v1/producer/get_whitelist_blacklist: HTTP 404".into());
        }
        Ok(Some(self.whitelist.borrow().clone()))
    }
    fn set_actor_whitelist(&self, actors: &[String]) -> Result<(), String> {
        self.down()?;
        if self.crash_on_set.replace(false) {
            panic!("CRASH-INJECTED before set_actor_whitelist applied");
        }
        if self.set_fails.replace(false) {
            return Err("http://mock/v1/producer/set_whitelist_blacklist: HTTP 404".into());
        }
        let lock = actors == [ADMISSION_LOCK_SENTINEL.to_string()];
        let first = self.whitelist_sets.borrow().is_empty();
        *self.whitelist.borrow_mut() = actors.to_vec();
        self.whitelist_sets.borrow_mut().push(actors.to_vec());
        let mut locked = self.locked.borrow_mut();
        if first || !lock {
            for (i, p) in self.participating.iter().enumerate() {
                if *p {
                    locked[i] = lock; // every participating agent reaches the same freeze height
                }
            }
        } else {
            locked[0] = true; // a re-apply on this node only
        }
        Ok(())
    }
    fn unapplied_transactions(&self) -> Result<Option<serde_json::Value>, String> {
        let mine = self.queue.borrow().iter().filter(|t| t.holder == 0).count();
        Ok(Some(serde_json::json!({"size": mine})))
    }
    fn target_info(&self) -> Result<Option<ChainInfo>, String> {
        if !self.ignited.get() {
            return Ok(None);
        }
        if self.traffic.get() {
            self.target_head.set(self.target_head.get() + 1);
        }
        Ok(Some(self.info(self.target_head.get())))
    }
    fn public_info(&self, _public_url: &str) -> Result<Option<ChainInfo>, String> {
        Ok(Some(self.info(self.head.get())))
    }
    fn ignite(&self) -> Result<String, String> {
        self.ignited.set(true);
        Ok("ignited".into())
    }
    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        match cmd {
            "freeze-writes" => self.writes_open.set(false),
            "reopen-writes" => self.writes_open.set(true),
            "resume-traffic" => self.traffic.set(true),
            _ => {}
        }
        Ok(format!("ran {cmd}"))
    }
    fn get_json(&self, _url: &str) -> Result<Option<serde_json::Value>, String> {
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

fn config(dir: &Path, admission_lock: bool) -> Config {
    let text = format!(
        r#"
journal_path = "{d}/journal.jsonl"
poll_ms = 1

[ceremony]
rehearsal = true
freeze_height = {H}
freeze_strategy = "schedule_at_h"
freeze_lead_blocks = {LEAD}
admission_lock = {admission_lock}
quiescence_polls = 3
quiescence_timeout_secs = 5

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
snapshot_timeout_secs = 60

[snapshot]
staged_path = "{d}/staged.bin"
capture_roots = "{d}/captured-roots.txt"
dir = "{d}/snapshots"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
quorum_timeout_secs = 10
live_sustain_secs = 1

[hooks]
on_freeze = "freeze-writes"
post_ignite = "resume-traffic"
on_abort = "reopen-writes"
"#,
        d = dir.display(),
    );
    let path = dir.join("ceremony.toml");
    std::fs::write(&path, text).unwrap();
    Config::load(&path).unwrap()
}

fn run(cfg: &Config, world: &Mempool) -> State {
    let (journal, recovered) = Journal::open(&cfg.journal_path).unwrap();
    let mut m = Machine::new(cfg, world, journal, recovered);
    match m.run() {
        Ok(s) => s,
        Err(e) if e.starts_with("STRANDED") => State::Stranded,
        Err(e) => panic!("{e}"),
    }
}

fn entries(cfg: &Config) -> Vec<serde_json::Value> {
    std::fs::read_to_string(&cfg.journal_path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn snapshotted(cfg: &Config) -> Option<serde_json::Value> {
    entries(cfg)
        .into_iter()
        .find(|v| v["kind"] == "transition" && v["state"] == "SNAPSHOTTED")
}

fn burnoff_error(cfg: &Config) -> Option<serde_json::Value> {
    entries(cfg).into_iter().find(|v| {
        v["kind"] == "error"
            && v["data"]["message"]
                .as_str()
                .is_some_and(|m| m.starts_with("transactions landed after the cut"))
    })
}

/// The s1 shape: four producers, all running the agent. Producer 3 holds transfers admitted at block 95, the last
/// block of its slot before the freeze (H - 24 = 96); its next slot starts at 132 = H + 12.
fn s1_world(dir: &Path, expiry_secs: u64) -> Mempool {
    let w = Mempool::new(dir, 4, vec![true; 4], 60);
    w.admit(3, 95, expiry_secs, 3);
    w
}

/// Reproduces fleet-step3 run s1 (lock off = the rc.31 behaviour): a 120 s expiry lands after H and the burn-off
/// audit aborts the ceremony, which is safe but a failed cut.
#[test]
fn rc32_f1_without_the_lock_a_pre_freeze_transaction_lands_after_h_and_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), false);
    let w = s1_world(dir.path(), 120);
    assert_eq!(run(&cfg, &w), State::Aborted);
    assert_eq!(w.landed_after(H), 3, "the s1 shape: 3 transfers after H");
    let err = burnoff_error(&cfg).expect("burn-off abort");
    assert_eq!(err["data"]["detail"]["burnoff_transactions"], 3);
    assert_eq!(
        err["data"]["detail"]["blocks"][0]["producer"], "prod3",
        "the abort names the producer that signed the offending block"
    );
    assert!(
        w.whitelist_sets.borrow().is_empty(),
        "lock off: never touched"
    );
}

/// The harness workaround (10 s expiry) only hides it: the same transactions expire before the holder's slot.
#[test]
fn rc32_f1_a_short_expiry_hides_the_problem_without_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), false);
    let w = s1_world(dir.path(), 10);
    assert_eq!(run(&cfg, &w), State::Live);
    assert_eq!(w.landed_after(H), 0);
}

/// The fix: with the admission lock on every participating producer, NO pre-freeze transaction lands after H,
/// whatever its expiry, up to XPR's max_transaction_lifetime (3,600 s) and beyond, and the ceremony goes LIVE.
#[test]
fn rc32_f1_with_the_lock_no_transaction_lands_after_h_whatever_its_expiry() {
    for expiry in [10u64, 120, 600, 3_600, 86_400] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path(), true);
        let w = s1_world(dir.path(), expiry);
        // Queued on this node too, and on a producer whose slot straddles H.
        w.admit(0, 95, expiry, 2);
        w.admit(2, 95, expiry, 4);
        assert_eq!(run(&cfg, &w), State::Live, "expiry {expiry}");
        assert_eq!(w.landed_after(H), 0, "expiry {expiry}: nothing after H");
        let s = snapshotted(&cfg).unwrap();
        assert_eq!(s["data"]["burnoff_transactions"], 0);
        assert_eq!(s["data"]["cut_height"], H);
        if expiry >= 120 {
            assert!(
                w.dropped.get() >= 3,
                "expiry {expiry}: the held transactions were dropped, not delayed"
            );
        }
        // The original (empty) whitelist was journaled before the lock, and the lock was confirmed.
        let es = entries(&cfg);
        let intent = es
            .iter()
            .position(|v| v["data"]["side_effect"] == "admission_lock")
            .expect("lock intent");
        let ack = es
            .iter()
            .position(|v| v["data"]["admission_lock_ack"] == true)
            .expect("lock confirmed");
        let frozen = es
            .iter()
            .position(|v| v["kind"] == "transition" && v["state"] == "FROZEN")
            .unwrap();
        assert!(intent < ack && ack < frozen);
        assert_eq!(
            es[intent]["data"]["original_actor_whitelist"],
            serde_json::json!([])
        );
        assert_eq!(es[frozen]["data"]["admission_lock"], true);
        // LIVE: the old chain stays locked (it is retired), the lock is never lifted.
        assert_eq!(
            *w.whitelist.borrow(),
            vec![ADMISSION_LOCK_SENTINEL.to_string()]
        );
    }
}

/// The residual: a producer outside the ceremony (no agent, or `admission_lock = false`) still includes what it
/// holds. The lock cannot reach it; the burn-off audit is the backstop and names the producer.
#[test]
fn rc32_f1_a_non_participating_producer_is_caught_by_the_burn_off_backstop() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true, true, true, false], 60);
    w.admit(3, 95, 3_600, 3);
    assert_eq!(run(&cfg, &w), State::Aborted);
    let err = burnoff_error(&cfg).expect("burn-off abort");
    assert_eq!(err["data"]["detail"]["blocks"][0]["producer"], "prod3");
    assert_eq!(err["data"]["detail"]["admission_lock"], true);
    // The abort restored this node's admission: the original list is back, after the lock.
    let sets = w.whitelist_sets.borrow().clone();
    assert_eq!(
        sets,
        vec![vec![ADMISSION_LOCK_SENTINEL.to_string()], vec![]],
        "lock, then restore"
    );
    assert!(entries(&cfg)
        .iter()
        .any(|v| v["data"]["rollback_step"] == "admission_unlock" && v["data"]["ok"] == true));
    assert!(w.writes_open.get());
    assert!(!w.paused.get());
}

/// A configured whitelist on the producer is restored exactly, and a ceremony restart never mistakes its own lock
/// for the original.
#[test]
fn rc32_f1_a_configured_whitelist_is_restored_exactly_after_an_abort() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true, true, true, false], 60);
    *w.whitelist.borrow_mut() = vec!["alice".into(), "bob".into()];
    w.admit(3, 95, 3_600, 1);
    assert_eq!(run(&cfg, &w), State::Aborted);
    assert_eq!(
        *w.whitelist.borrow(),
        vec!["alice".to_string(), "bob".to_string()]
    );
}

/// A producer API that refuses the lock: the ceremony aborts before FROZEN (nothing is frozen at a node that would
/// still admit transactions), and the rollback finds nothing applied to restore.
#[test]
fn rc32_f1_a_lock_that_cannot_be_set_aborts_before_the_freeze() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 120);
    w.set_fails.set(true);
    assert_eq!(run(&cfg, &w), State::Aborted);
    let es = entries(&cfg);
    assert!(!es
        .iter()
        .any(|v| v["kind"] == "transition" && v["state"] == "FROZEN"));
    assert!(es.iter().any(|v| v["kind"] == "error"
        && v["data"]["message"]
            .as_str()
            .is_some_and(|m| m.starts_with("admission lock failed"))));
    assert!(es
        .iter()
        .any(|v| v["data"]["rollback_step"] == "admission_unlock"
            && v["data"]["result"]
                .as_str()
                .is_some_and(|r| r.starts_with("the lock was never applied"))));
    assert!(es.iter().any(|v| v["data"]["nothing_to_undo"] == true
        && v["data"]["note"]
            .as_str()
            .is_some_and(|n| n.contains("admission lock was never applied"))));
    assert!(w.writes_open.get(), "on_freeze never ran: writes stay open");
    assert_eq!(*w.whitelist.borrow(), Vec::<String>::new());
}

/// Killed after the lock intent was journaled: the re-run keeps the ORIGINAL list the first attempt recorded (never
/// the sentinel it may find on the node), and a later abort restores that original.
#[test]
fn rc32_f1_a_crash_after_the_lock_intent_keeps_the_original_list() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true, true, true, false], 60);
    *w.whitelist.borrow_mut() = vec!["carol".into()];
    w.admit(3, 95, 3_600, 1);
    w.crash_on_set.set(true);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&cfg, &w)));
    assert!(r.is_err(), "the injected crash");
    // The crash happened before the set applied; simulate the worst case (it applied) before the re-run.
    *w.whitelist.borrow_mut() = vec![ADMISSION_LOCK_SENTINEL.to_string()];
    assert_eq!(run(&cfg, &w), State::Aborted, "non-participant burn-off");
    let intents: Vec<_> = entries(&cfg)
        .into_iter()
        .filter(|v| v["data"]["side_effect"] == "admission_lock")
        .collect();
    assert_eq!(intents.len(), 1, "one intent, from the first attempt");
    assert_eq!(
        intents[0]["data"]["original_actor_whitelist"],
        serde_json::json!(["carol"])
    );
    assert_eq!(*w.whitelist.borrow(), vec!["carol".to_string()]);
}

/// Someone else changes the whitelist while the ceremony holds its lock: the rollback leaves their list alone (it
/// only ever restores from its own sentinel) and journals it.
#[test]
fn rc32_f1_a_whitelist_changed_by_someone_else_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true, true, true, false], 60);
    w.admit(3, 95, 3_600, 1);
    *w.external_whitelist_on_audit.borrow_mut() = Some(vec!["dave".into()]);
    assert_eq!(run(&cfg, &w), State::Aborted);
    assert_eq!(*w.whitelist.borrow(), vec!["dave".to_string()]);
    assert!(entries(&cfg)
        .iter()
        .any(|v| v["data"]["admission_lock_changed_externally"] == serde_json::json!(["dave"])));
}

/// `admission_lock = false` is a rehearsal-only escape: in producer mode it fails the production profile.
#[test]
fn rc32_f1_admission_lock_off_is_a_production_problem() {
    let dir = tempfile::tempdir().unwrap();
    let off = config(dir.path(), false);
    assert!(off
        .production_problems()
        .iter()
        .any(|p| p.starts_with("ceremony.admission_lock = false")));
    let on = config(dir.path(), true);
    assert!(!on
        .production_problems()
        .iter()
        .any(|p| p.contains("admission_lock")));
    // The sentinel can never be an account: native newaccount refuses names over 12 characters.
    assert_eq!(ADMISSION_LOCK_SENTINEL.len(), 13);
    let last = ADMISSION_LOCK_SENTINEL.chars().last().unwrap();
    assert!(
        ".12345abcdefghij".contains(last),
        "a valid 13th name character"
    );
    assert!(ADMISSION_LOCK_SENTINEL[..12]
        .chars()
        .all(|c| ".12345abcdefghijklmnopqrstuvwxyz".contains(c)));
}

/// rc.32 (F4): the fleet gate must outlast the verify lag; the drill's measured conversion time sets the floor.
#[test]
fn rc32_f4_fleet_timeout_under_one_and_a_half_drill_conversions_warns() {
    use pulse_cutover::drill::fleet_timeout_problem;
    // fleet-step3 slowest box: ~439 s from SNAPSHOTTED to VERIFIED.
    let drill = serde_json::json!({"state": "DONE", "h": 408480162u64, "timings_ms": {"conversion_total": 439_000u64}});
    let w = fleet_timeout_problem(300, &drill).expect("300 s < 1.5 x 439 s");
    assert!(w.contains("at least 659"), "{w}");
    assert!(fleet_timeout_problem(659, &drill).is_none());
    assert!(
        fleet_timeout_problem(900, &drill).is_none(),
        "the rc.32 default covers it"
    );
    // No finished drill, no warning.
    assert!(fleet_timeout_problem(1, &serde_json::json!({"state": "RUNNING"})).is_none());
    assert!(
        fleet_timeout_problem(1, &serde_json::json!({"state": "DONE", "timings_ms": {}})).is_none()
    );
}

/// rc.32 review: nodeos restarts mid-window (between the freeze and the pause) and comes back with its configured
/// (empty) whitelist while it holds a transaction it would include in its next slot. The agent re-reads the lock on
/// every poll tick, re-applies it within one tick (journaled with the height), and nothing from that producer lands
/// after H.
#[test]
fn rc32_f1_a_restart_mid_window_gets_the_lock_back_within_one_tick() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true; 4], 60);
    let restart = H + 21; // 141: this node's slot starts at 144
    w.admit(0, restart, 3_600, 2); // reaches this node right after the restart
    w.restart_at.set(restart);
    assert_eq!(run(&cfg, &w), State::Live);
    let es = entries(&cfg);
    let lost: Vec<&serde_json::Value> = es
        .iter()
        .filter(|v| v["data"]["admission_lock_lost"].is_object())
        .collect();
    assert_eq!(lost.len(), 1, "one loss, caught once");
    let rec = &lost[0]["data"]["admission_lock_lost"];
    assert_eq!(rec["at"], "freeze window");
    assert_eq!(rec["reapplied"], true);
    assert_eq!(rec["found"], serde_json::json!([]));
    assert_eq!(
        rec["head"], restart,
        "re-applied in the tick that saw the restart"
    );
    assert_eq!(w.landed_after(restart), 0, "nothing lands after that tick");
    assert_eq!(w.landed_after(H), 0);
    assert_eq!(
        w.dropped.get(),
        2,
        "the held transfers were dropped at the producer's next block"
    );
    assert_eq!(
        *w.whitelist.borrow(),
        vec![ADMISSION_LOCK_SENTINEL.to_string()]
    );
}

/// A restart soon after H (in another producer's slot) is caught by the window watch at the height it happened, so the
/// held transfers never reach this producer's next block.
#[test]
fn rc32_f1_a_restart_soon_after_h_is_caught_at_its_height() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 3_600);
    w.restart_at.set(H + 5);
    assert_eq!(run(&cfg, &w), State::Live);
    let es = entries(&cfg);
    assert!(es.iter().any(
        |v| v["data"]["admission_lock_lost"]["at"] == "freeze window"
            && v["data"]["admission_lock_lost"]["head"] == H + 5
    ));
    assert_eq!(w.landed_after(H), 0);
}

/// rc.32 review: ARM reads get_whitelist_blacklist, so a producer API that does not expose it refuses at ARM, before
/// anything is frozen.
#[test]
fn rc32_f1_arm_refuses_when_get_whitelist_blacklist_is_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 120);
    w.whitelist_read_fails.set(true);
    let (journal, recovered) = Journal::open(&cfg.journal_path).unwrap();
    let mut m = Machine::new(&cfg, &w, journal, recovered);
    let r = m.run();
    let text = std::fs::read_to_string(&cfg.journal_path).unwrap_or_default();
    assert!(
        text.contains("get_whitelist_blacklist")
            || r.as_ref()
                .err()
                .is_some_and(|e| e.contains("get_whitelist_blacklist")),
        "{r:?}\n{text}"
    );
    assert!(!text.contains("\"state\":\"FROZEN\""), "nothing frozen");
    assert!(w.whitelist_sets.borrow().is_empty());
    assert!(w.writes_open.get());
}

/// Coverage of the re-check right before the pause: the lock goes missing after the snapshot file appeared and after
/// the last freeze-window tick. The pre-pause check re-applies it.
#[test]
fn rc32_f1_a_lock_lost_after_the_snapshot_is_reapplied_right_before_the_pause() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 3_600);
    w.restart_after_snapshot.set(true);
    assert_eq!(run(&cfg, &w), State::Live);
    let es = entries(&cfg);
    let lost: Vec<&serde_json::Value> = es
        .iter()
        .filter(|v| v["data"]["admission_lock_lost"].is_object())
        .collect();
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!(
        lost[0]["data"]["admission_lock_lost"]["at"],
        "before the pause"
    );
    assert_eq!(lost[0]["data"]["admission_lock_lost"]["reapplied"], true);
    assert!(lost[0]["data"]["admission_lock_lost"]["head"]
        .as_u64()
        .is_some());
    let li = es
        .iter()
        .position(|v| v["data"]["admission_lock_lost"].is_object())
        .unwrap();
    let pause = es
        .iter()
        .position(|v| v["data"]["side_effect"] == "source_pause")
        .unwrap();
    assert!(li < pause);
    assert_eq!(
        *w.whitelist.borrow(),
        vec![ADMISSION_LOCK_SENTINEL.to_string()]
    );
    assert_eq!(w.landed_after(H), 0);
}

/// Coverage of the burn-off detail: nodeos restarts on the first block of this producer's own slot (144), so no
/// freeze-window tick runs between the restart and that block, and the held transfers land in it. The next tick
/// re-applies the lock; the burn-off audit aborts and its detail carries the loss and the producer.
#[test]
fn rc32_f1_a_lock_lost_at_the_start_of_this_producers_slot_is_in_the_burn_off_detail() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true; 4], 60);
    w.admit(0, 143, 3_600, 2); // reaches this node right before its slot
    w.restart_at.set(144);
    assert_eq!(run(&cfg, &w), State::Aborted);
    assert_eq!(w.landed_after(H), 2);
    let err = burnoff_error(&cfg).expect("burn-off abort");
    let d = &err["data"]["detail"];
    assert_eq!(d["blocks"][0]["block"], 144);
    assert_eq!(d["blocks"][0]["producer"], "prod0");
    assert_eq!(d["admission_lock_lost"][0]["at"], "freeze window");
    assert_eq!(d["admission_lock_lost"][0]["head"], 144);
    assert_eq!(d["admission_lock_lost"][0]["reapplied"], true);
}

/// Coverage of the re-read at the burn-off audit itself: the lock disappears after the last quiescence tick, while the
/// producer is paused. The audit's re-check journals it and (rc.32, fleet run r3b) re-applies it.
#[test]
fn rc32_f1_a_lock_lost_after_the_pause_is_journaled_at_the_burn_off_audit() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 120);
    w.lock_drop_when_paused.set(true);
    assert_eq!(run(&cfg, &w), State::Live);
    let rec = entries(&cfg)
        .into_iter()
        .find(|v| v["data"]["admission_lock_lost"]["at"] == "before the burn-off audit")
        .expect("journaled at the audit");
    // rc.32 (fleet run r3b): the audit's re-check re-applies it too.
    assert_eq!(rec["data"]["admission_lock_lost"]["reapplied"], true);
    assert!(rec["data"]["admission_lock_lost"]["head"]
        .as_u64()
        .is_some());
    assert_eq!(
        *w.whitelist.borrow(),
        vec![ADMISSION_LOCK_SENTINEL.to_string()]
    );
}

/// rc.32 (fleet run r3): nodeos is unreachable for 20 s mid-window (a restart: connection reset) and comes back with the
/// lock gone, holding transfers for its next slot. The agent retries, survives, re-locks on the next tick and goes LIVE
/// with nothing after H.
#[test]
fn rc32_f1_an_outage_mid_window_is_survived_and_the_lock_comes_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true; 4], 60);
    w.admit(0, 130, 3_600, 2);
    w.outage_at.set(H + 10);
    w.outage_ms.set(20_000);
    assert_eq!(run(&cfg, &w), State::Live);
    let es = entries(&cfg);
    assert!(es
        .iter()
        .any(|v| v["data"]["source_rpc_retry"]["what"] == "get_info"));
    assert!(es
        .iter()
        .any(|v| v["data"]["source_rpc_recovered"]["what"] == "get_info"));
    let lost = es
        .iter()
        .find(|v| v["data"]["admission_lock_lost"]["at"] == "freeze window")
        .expect("re-locked on the tick after the outage");
    assert_eq!(lost["data"]["admission_lock_lost"]["reapplied"], true);
    assert_eq!(lost["data"]["admission_lock_lost"]["head"], H + 10);
    assert_eq!(w.landed_after(H), 0);
    assert_eq!(
        *w.whitelist.borrow(),
        vec![ADMISSION_LOCK_SENTINEL.to_string()]
    );
}

/// An outage longer than the retry bound takes the abort path (never a bare exit), and the agent never lifts the lock:
/// nothing is restored into a source it cannot see.
#[test]
fn rc32_f1_an_outage_longer_than_the_bound_takes_the_abort_path_with_the_lock_kept() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = Mempool::new(dir.path(), 4, vec![true; 4], 60);
    w.outage_at.set(H + 10);
    w.outage_ms.set(u64::MAX / 4);
    let end = run(&cfg, &w);
    assert!(matches!(end, State::Aborted | State::Stranded), "{end:?}");
    let es = entries(&cfg);
    assert!(es
        .iter()
        .any(|v| v["data"]["source_rpc_unreachable"]["what"] == "get_info"));
    assert!(es.iter().any(|v| v["kind"] == "error"
        && v["data"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("stayed unreachable longer than source.rpc_retry_secs"))));
    assert!(!es
        .iter()
        .any(|v| v["data"]["rollback_step"] == "admission_unlock"));
    assert_eq!(
        *w.whitelist_sets.borrow(),
        vec![vec![ADMISSION_LOCK_SENTINEL.to_string()]],
        "locked once, never restored"
    );
    assert!(!w.writes_open.get(), "writes stay frozen");
}

/// rc.32 (fleet run r3, item 3): the agent dies in FROZEN (as on bp3) and nodeos restarts; the unit restarts the agent,
/// which resumes from the journal and checks the admission lock before anything else.
#[test]
fn rc32_f1_a_resume_in_frozen_rechecks_the_lock_first() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 3_600);
    w.crash_at.set(H + 3);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&cfg, &w)));
    assert!(r.is_err(), "the injected crash");
    w.whitelist.borrow_mut().clear(); // nodeos restarted too
    w.locked.borrow_mut()[0] = false;
    let before = entries(&cfg).len();
    assert_eq!(run(&cfg, &w), State::Live);
    let es = entries(&cfg);
    let first_after = es[before..]
        .iter()
        .find(|v| v["kind"] != "transition")
        .unwrap()
        .clone();
    let lost = es[before..]
        .iter()
        .position(|v| v["data"]["admission_lock_lost"]["at"] == "resumed in FROZEN")
        .expect("re-checked on resume");
    assert!(
        es[before..][..lost]
            .iter()
            .all(|v| v["data"]["admission_lock_lost"].is_null()
                && v["data"]["side_effect"].is_null()),
        "nothing acted before the re-check: {first_after}"
    );
    assert_eq!(
        es[before + lost]["data"]["admission_lock_lost"]["reapplied"],
        true
    );
    assert_eq!(w.landed_after(H), 0);
}

/// rc.32 (fleet run r3 follow-up): nodeos restarts at the pause call (20 s unreachable, back unpaused and unlocked).
/// The pause and the paused reads are retried under source.rpc_retry_secs; the agent pauses it, re-applies the lock and
/// goes LIVE with nothing after H.
#[test]
fn rc32_f1_an_outage_spanning_the_pause_call_is_survived() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 3_600);
    w.outage_on_pause_ms.set(20_000);
    assert_eq!(run(&cfg, &w), State::Live);
    let es = entries(&cfg);
    assert!(es
        .iter()
        .any(|v| v["data"]["source_rpc_retry"]["what"] == "pause"));
    assert!(es
        .iter()
        .any(|v| v["data"]["source_rpc_recovered"]["what"] == "pause"));
    assert!(es.iter().any(
        |v| v["data"]["admission_lock_lost"]["at"] == "after the pause"
            && v["data"]["admission_lock_lost"]["reapplied"] == true
    ));
    assert!(w.paused.get());
    assert_eq!(
        *w.whitelist.borrow(),
        vec![ADMISSION_LOCK_SENTINEL.to_string()]
    );
    assert_eq!(w.landed_after(H), 0);
}

/// The same restart, but nodeos stays unreachable past the bound: no exit, the abort path strands (the source cannot be
/// confirmed paused), and the agent never lifts the lock (it is never restored to the original list).
#[test]
fn rc32_f1_an_outage_at_the_pause_past_the_bound_strands_with_the_lock_kept() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 3_600);
    w.outage_on_pause_ms.set(u64::MAX / 4);
    assert_eq!(run(&cfg, &w), State::Stranded);
    let es = entries(&cfg);
    assert!(es
        .iter()
        .any(|v| v["data"]["source_rpc_unreachable"]["what"] == "pause"));
    assert!(es
        .iter()
        .any(|v| v["kind"] == "error" && v["data"]["detail"]["during"] == "the pause"));
    assert!(es
        .iter()
        .any(|v| v["data"]["admission_lock_lost"]["at"] == "stranded"));
    assert!(!es
        .iter()
        .any(|v| v["data"]["rollback_step"] == "admission_unlock"));
    assert_eq!(
        *w.whitelist_sets.borrow(),
        vec![vec![ADMISSION_LOCK_SENTINEL.to_string()]],
        "locked once, never restored"
    );
    assert!(!w.writes_open.get());
}

/// When nodeos comes back while the strand is re-checking the lock, the strand re-applies it.
#[test]
fn rc32_f1_a_strand_reapplies_the_lock_when_nodeos_comes_back() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 3_600);
    // Unreachable past the pause bound and the rollback's reads, back during the strand's own lock re-check.
    w.outage_on_pause_ms.set(75_000);
    let end = run(&cfg, &w);
    let es = entries(&cfg);
    if end == State::Stranded {
        let rec = es
            .iter()
            .find(|v| v["data"]["admission_lock_lost"]["at"] == "stranded")
            .expect("re-checked in the strand");
        assert_eq!(
            rec["data"]["admission_lock_lost"]["reapplied"], true,
            "{rec}"
        );
        assert_eq!(
            *w.whitelist.borrow(),
            vec![ADMISSION_LOCK_SENTINEL.to_string()]
        );
    } else {
        panic!("expected STRANDED, got {end:?}");
    }
}

/// rc.32 (fleet run r3b): nodeos restarts just after the pause (bp3: 234 ms), clearing the pause and the lock. The
/// agent re-pauses and re-locks on its next tick (`source_unpaused`, `admission_lock_lost`, with the height); the
/// block the restarted producer made in between moves the head past the pause head, so the ceremony still refuses to
/// create or ignite and aborts; no transaction lands.
#[test]
fn rc32_f1_a_restart_just_after_the_pause_is_repaused_relocked_and_the_cut_refused() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    let w = s1_world(dir.path(), 3_600);
    w.admit(0, 150, 3_600, 2); // the restarted producer holds transfers too
    w.restart_on_audit.set(true);
    assert_eq!(run(&cfg, &w), State::Aborted);
    let es = entries(&cfg);
    let snap = es
        .iter()
        .position(|v| v["kind"] == "transition" && v["state"] == "SNAPSHOTTED")
        .expect("the cut was audited before the restart showed");
    let pause_head = es[snap]["data"]["head_at_pause"].as_u64().unwrap();
    let unpaused = es
        .iter()
        .position(|v| v["data"]["source_unpaused"].is_object())
        .expect("re-paused");
    assert!(unpaused > snap);
    let u = &es[unpaused]["data"]["source_unpaused"];
    assert_eq!(u["at"], "verification", "the first tick after the restart");
    assert_eq!(u["repaused"], true);
    assert!(u["head"].as_u64().unwrap() > pause_head);
    let lost = es
        .iter()
        .position(|v| v["data"]["admission_lock_lost"]["at"] == "verification")
        .expect("re-locked");
    assert_eq!(es[lost]["data"]["admission_lock_lost"]["reapplied"], true);
    assert!(lost > snap);
    assert!(es.iter().any(|v| v["kind"] == "error"
        && v["data"]["message"].as_str().is_some_and(
            |m| m.starts_with("refusing to") && m.contains("moved past the pause head")
        )));
    assert_eq!(w.landed_after(H), 0, "no transactions land");
    assert!(
        w.head.get() <= pause_head + 1,
        "re-paused within one block (head {}, pause head {pause_head})",
        w.head.get()
    );
}

/// rc.32 (review): the agent unit's restart really resumes. `run` dies in FROZEN after its pause and nodeos restarts
/// (pause and lock gone). A restarted `await` finds the ceremony it started (tombstone + derived config + a non-terminal
/// journal) and runs it again from the journal; the resumed run re-locks and re-pauses before anything else and goes
/// LIVE with nothing after H. Terminal journals are never resumed.
#[test]
fn rc32_await_resumes_a_ceremony_killed_in_frozen() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), true);
    // What `await` leaves behind when it spawns `run`: the derived config and a "ran" tombstone.
    let derived = dir.path().join("ceremony-e1.toml");
    std::fs::copy(dir.path().join("ceremony.toml"), &derived).unwrap();
    std::fs::write(
        dir.path().join("coord-tombstones.json"),
        r#"{"e1": {"kind": "ran", "at": "2026-10-16T00:00:00Z", "reason": "this node started the ceremony"}}"#,
    )
    .unwrap();
    assert!(
        pulse_cutover::coord::resumable_ceremony(&cfg).is_none(),
        "no journal yet: nothing to resume"
    );
    let w = s1_world(dir.path(), 3_600);
    w.crash_on_paused_poll.set(1);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&cfg, &w)));
    assert!(r.is_err(), "the injected crash");
    assert!(w.paused.get());
    // nodeos restarts too: the runtime pause and the lock are gone.
    w.paused.set(false);
    w.whitelist.borrow_mut().clear();
    w.locked.borrow_mut()[0] = false;
    let (id, path, state) =
        pulse_cutover::coord::resumable_ceremony(&cfg).expect("the restarted await resumes it");
    assert_eq!(id, "e1");
    assert_eq!(path, derived);
    assert_eq!(state, State::Frozen);
    let before = entries(&cfg).len();
    let dcfg = Config::load(&path).unwrap();
    assert_eq!(run(&dcfg, &w), State::Live);
    let es = entries(&cfg);
    let seg = &es[before..];
    let lock = seg
        .iter()
        .position(|v| v["data"]["admission_lock_lost"]["at"] == "resumed in FROZEN")
        .expect("re-locked on resume");
    let repause = seg
        .iter()
        .position(|v| v["data"]["source_unpaused"]["at"] == "resumed in FROZEN")
        .expect("re-paused on resume");
    assert_eq!(seg[repause]["data"]["source_unpaused"]["repaused"], true);
    // Before the resumed step acts on anything else (only the read-only resume preflight runs earlier).
    let first_other = seg
        .iter()
        .position(|v| {
            v["kind"] == "transition"
                || v["data"]["kept_paused_on_resume"].is_boolean()
                || v["data"]["resumed_production_for_snapshot"].is_boolean()
                || v["data"]["scheduled_snapshot_file"].is_string()
        })
        .unwrap();
    assert!(
        lock < first_other && repause < first_other,
        "re-lock and re-pause come first: {:?}",
        &seg[..first_other.min(seg.len())]
    );
    assert_eq!(w.landed_after(H), 0);
    // LIVE is terminal: a later restart of await stays idle.
    assert!(pulse_cutover::coord::resumable_ceremony(&cfg).is_none());
}

/// rc.32 (review): nodeos answers HTTP 500 for deterministic chain exceptions; those are never retried as transient.
#[test]
fn rc32_a_nodeos_exception_500_is_not_transient() {
    use pulse_cutover::machine::transient_rpc_error as t;
    assert!(t(
        "http://x/v1/chain/get_info: Network Error: Connection reset by peer (os error 104)"
    ));
    assert!(t(
        "http://x/v1/producer/pause: HTTP 503: <html>bad gateway</html>"
    ));
    assert!(t("http://x/v1/producer/pause: HTTP 500: upstream gone"));
    assert!(!t(
        r#"http://x/v1/producer/set_whitelist_blacklist: HTTP 500: {"code":500,"message":"Internal Service Error","error":{"code":3010001,"name":"name_type_exception","what":"Invalid name","details":[]}}"#
    ));
    assert!(!t("http://x/v1/chain/get_block: HTTP 400: bad request"));
}
