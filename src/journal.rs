//! Append-only JSONL ceremony journal.
//!
//! Every state transition and every piece of evidence (hashes, block ids,
//! timings) is one line, written with fsync before the machine acts on it —
//! the journal is both the audit record and the crash-resume source of truth.

use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::state::State;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub seq: u64,
    /// Unix epoch milliseconds.
    pub ts_ms: u64,
    /// Human-readable UTC timestamp of ts_ms.
    pub ts: String,
    /// "transition" | "evidence" | "error"
    pub kind: String,
    /// State the machine is in (after the transition, for transitions).
    pub state: String,
    pub data: Value,
}

/// Why a lock could not be taken (r30 round 6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockError {
    /// Another process holds it: a live competitor.
    Held(String),
    /// Anything else (directory missing or unwritable, read-only mount): not evidence of a competitor.
    Io(String),
}

pub struct Journal {
    path: PathBuf,
    file: File,
    seq: u64,
    /// Test support: the next N writes fail as a full disk would (nothing is written).
    fail_writes: u32,
    /// Held for the Journal's lifetime: an exclusive lock on `<journal>.lock`, so two ceremony
    /// processes can never drive the same journal (released when the Journal is dropped).
    _lock: File,
}

/// Ceremony facts recovered from a journal on resume: everything a restarted
/// agent must not re-derive differently than the first run did.
#[derive(Debug, Clone, Default)]
pub struct Recovered {
    pub state: Option<State>,
    pub chain_id: Option<String>,
    /// H as resolved at ARM time (explicit freeze_height, or LIB + margin).
    pub resolved_h: Option<u64>,
    pub cut_height: Option<u64>,
    pub cut_block_id: Option<String>,
    pub snapshot_file: Option<String>,
    pub sha256: Option<String>,
    pub frozen_ts_ms: Option<u64>,
    pub last_source_block_time: Option<String>,
    /// schedule_snapshot(H) succeeded at ARM (evidence `snapshot_scheduled_at`): a resumed
    /// FROZEN step must wait for THAT snapshot, never fall back to an immediate one.
    pub scheduled: bool,
    /// Side effects the machine journaled as STARTED (`{"side_effect": name}` evidence is
    /// written before each external action). A resumed agent treats started as possibly
    /// applied — e.g. a flip that began before a crash must be considered live.
    pub side_effects: Vec<String>,
    /// The journal showed IGNITED (or later), OR recorded `ignite_started` (the side effect is
    /// journaled BEFORE the ignite command runs): this node's target may be running, so a local
    /// failure must seal instead of resuming the source.
    pub reached_ignited: bool,
    /// Identity of the snapshot this ceremony staged at `staged_path` (evidence
    /// `staged_artifact`): a resumed preflight accepts a staged file that matches it.
    pub staged_sha256: Option<String>,
    pub staged_cut_height: Option<u64>,
    /// The state HALTED was entered from (so `unhalt` can return there).
    pub halted_from: Option<State>,
    /// An operator cleared the last HALT (`unhalt --i-understand`): a resumed run may continue
    /// forward (failures still halt, since ignition-started stays recorded).
    pub unhalted: bool,
    /// A trailing partial line (crash mid-write) was found and set aside on open.
    pub torn_tail: bool,
    /// The journal ends in an ABORTED whose rollback PROVABLY finished: a `rollback_done`
    /// record, written only after every step (reverts, on_abort, unstage) succeeded, and no later
    /// ABORTED or `rollback_incomplete`. A repeated `rollback` is then a no-op instead of a second
    /// resume. (rc.7 put `rollback_complete: true` on the ABORTED transition BEFORE on_abort ran;
    /// that key is no longer trusted, so an rc.7 journal repeats its rollback: the safe default.)
    pub aborted_rollback_complete: bool,
    /// Rollback steps journaled as done (`rollback_step` records) in the current abort episode
    /// (since the last non-ABORTED transition): a rollback that died part-way redoes only the rest.
    pub rollback_steps_done: Vec<String>,
    /// An operator `rollback` recorded its intent (`rollback_requested`) and no ABORTED transition
    /// followed: it died before finishing. `run` must refuse (the operator asked to go back, not to
    /// carry on); re-running `rollback` finishes it.
    pub rollback_pending: bool,
    /// Upstream backend: the Metal blockchain id `create_chain_cmd` created (evidence
    /// `target_blockchain_id`, journaled the moment it is known): a resumed agent reuses it and
    /// never creates a second chain.
    pub target_blockchain_id: Option<String>,
    /// Upstream backend: the subnet create_chain_cmd reported (`target_subnet_id`), if any.
    pub target_subnet_id: Option<String>,
    /// REHEARSAL ONLY: a target chain_id different from the source's that
    /// `rehearsal_allow_chain_id_change` accepted at IGNITED (evidence `accepted_target_chain_id`).
    pub accepted_target_chain_id: Option<String>,
    /// Upstream backend: hashes of the boot artifacts as journaled at VERIFIED (re-checked
    /// before ignition: a file changed since verification aborts).
    pub boot_manifest_sha256: Option<String>,
    pub boot_genesis_sha256: Option<String>,
    pub boot_chain_config_sha256: Option<String>,
    /// The source head when the producer pause took effect (SNAPSHOTTED `head_at_pause`): `join`
    /// refuses a source that produced past it.
    pub head_at_pause: Option<u64>,
    /// rc.23: when the first post-ignition degraded symptom was seen (patience counts from there,
    /// across a crash and resume).
    pub degraded_since_ms: Option<u64>,
    /// The `shadow` value of the ARMED (arming) record: Null for a real ceremony, the shadow identity for a shadow
    /// (docs/DESIGN-shadow.md). None = no arming record yet. Journals from before shadow mode read as Null.
    pub armed_shadow: Option<Value>,
    /// r30 round 6: the arming record's `undo_tracking` flag. Some(true) = this journal records every source pause
    /// (`side_effect: source_pause`) and write freeze (`side_effect: write_freeze`) before doing it, so a rollback
    /// undoes only those. Some(false) = an arming record from an older agent (rollback keeps its old behaviour).
    /// None = no arming record yet (nothing was done).
    pub undo_tracking: Option<bool>,
    /// r30 round 14 (model R13-B): the ownership an agent of round 14+ derived ONCE, the first time it opened an older
    /// agent's journal (`legacy_takeover` evidence: `{paused, froze}`). Later runs use it instead of re-deriving it from
    /// states this agent wrote itself (a strand this agent wrote over someone else's pause is not the older agent's).
    pub legacy_takeover: Option<(bool, bool)>,
    /// r30 round 10 (model checker, class B): the arming record's `pause_ack` flag. Some(true) = this journal writes a
    /// `source_pause_ack` record once its own pause RPC succeeded and a read confirmed the producer paused, and an own
    /// pause INTENT without its ack is ownership UNKNOWN (never resumed). Otherwise (older journals) an own intent
    /// counts as ours, the previous behaviour.
    pub pause_ack_tracking: Option<bool>,
    /// Round-2 review: a shadow identity refusal was journaled (its first record). Sticky: from then on nothing may
    /// resume the source or run on_abort on this journal, whatever records follow (a crash between the refusal's
    /// records must not reopen a rollback that resumes a node which may be real).
    pub shadow_refused: bool,
    /// r30 round 8 (review R8-6): an abort decided to keep writes frozen and seal (STRANDED) because the producer was
    /// paused (or unreadable) when writes would have reopened (`strand_intent`, or round 7's `external_pause_held`). A
    /// crash before the STRANDED transition must not let a later run carry on: the next run completes the strand.
    pub strand_intent: bool,
    /// Round-2 review (D-10): the bindings a fresh XPR mainnet ARM authenticated (`mainnet_event_verified`), compared
    /// offline with the config on every later run, join, rollback and unhalt (coord::mainnet_binding_problem).
    pub mainnet_event_binding: Option<Value>,
    /// r30 round 9 (review R9-2): every pause, resume and operator release, in journal order (pause EPISODES). A pause by
    /// this ceremony (`side_effect: source_pause`, written before the RPC) or an adopted external one opens an episode;
    /// a journaled resume (`rollback_step: resume`) closes the latest open one; an operator's `external_pause_release`
    /// makes the latest open external episode ours. Never cleared: an old, completed resume can never authorize a later one.
    pub pause_events: Vec<PauseEvent>,
    /// r30 round 9 (review R9-3): writes this journal froze in its CURRENT segment (a `write_freeze` intent in it, or a
    /// freeze carried over from the previous segment whose on_abort never reopened them).
    pub writes_closed: bool,
    /// rc.32 (F1): this journal set the producer's admission lock (`side_effect: admission_lock`, written before the
    /// RPC) and no rollback has restored it since (`rollback_step: admission_unlock`): the actor whitelist to restore.
    pub admission_lock: Option<Vec<String>>,
    /// rc.32: the latest admission lock was confirmed on the producer (`admission_lock_ack` or
    /// `admission_lock_reapplied` after its intent). False: its set failed or never ran.
    pub admission_lock_applied: bool,
    /// r30 round 9 (review R9-1/R9-3): the journal segment (0 = the original ceremony). A `join` that re-enters an
    /// ABORTED or STRANDED ceremony journals `segment_start` before its first side effect: undo bookkeeping
    /// (rollback steps, rollback_done, strand intent) belongs to one segment only.
    pub segment: u64,
}

/// r30 round 9: one entry of the pause-episode log (`Recovered::pause_events`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PauseEvent {
    /// A pause: `ours` = this ceremony INTENDED it (intent before the RPC); false = someone else's, adopted as evidence.
    Pause { ours: bool },
    /// r30 round 10: this ceremony's pause RPC succeeded and a read confirmed the producer paused (`source_pause_ack`):
    /// the latest open own intent becomes ours. Without it (a crash between the intent and the RPC, then someone else
    /// pausing) the episode's ownership is UNKNOWN and nothing resumes it.
    Ack,
    /// A journaled resume (a rollback's `resume` step, or `resumed_production_for_snapshot`): closes the latest open
    /// episode.
    Resume,
    /// The operator's journaled release of an external pause (`rollback --force-stranded --i-understand`).
    /// r30 round 12 (model class R11-A): a release authorizes ONE resume, by the run that journaled it (`live`). A
    /// release recovered from the journal (`live: false`, a forced rollback that died or whose resume did not settle)
    /// authorizes nothing: the episode it named is ownership unknown, and a later forced rollback journals its own.
    Release { live: bool },
    /// r30 round 11 (review R10-1): this ceremony is ABOUT to resume its own pause (`side_effect: source_resume`, written
    /// before the resume RPC). It gives up ownership at once: the episode is CLOSING. If the resume's outcome is never
    /// journaled (a crash, or a lost response that a read could not settle), the episode stays closing = ownership
    /// unknown, never resumed again on this ceremony's own authority (the producer may have resumed and someone else
    /// paused it since).
    ResumeIntent,
}

/// r30 round 9/10: the folded pause-episode log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Episodes {
    /// The latest open episode is this ceremony's own, confirmed pause (intent + ack, or legacy, or released).
    pub ours: bool,
    /// The latest open episode is someone else's (adopted) pause.
    pub external: bool,
    /// The latest open episode is an own pause INTENT without its ack (round 10), or an own pause whose resume was
    /// started but never settled (round 11, closing): ownership unknown, never resumed.
    pub unknown: bool,
    /// This journal ever intended a pause of its own (open or closed, acked or not).
    pub ever_ours: bool,
}

/// r30 round 9: fold the pause-episode log. `legacy_paused`: an older agent's journal whose agent demonstrably paused
/// (an open episode of ours before every journaled event). `acks_tracked` (round 10): own intents need their
/// `source_pause_ack` to count as ours; false for journals armed before round 10 (an intent counts as ours, as before).
pub fn pause_episodes(events: &[PauseEvent], legacy_paused: bool, acks_tracked: bool) -> Episodes {
    #[derive(Clone, Copy, PartialEq)]
    enum Owner {
        Ours,
        External,
        Unknown,
        /// r30 round 11: an own pause whose resume was started (intent journaled) and not settled.
        Closing,
    }
    // (owner, open)
    let mut eps: Vec<(Owner, bool)> = Vec::new();
    if legacy_paused {
        eps.push((Owner::Ours, true));
    }
    let mut ever_ours = legacy_paused;
    for e in events {
        match e {
            PauseEvent::Pause { ours: true } => {
                ever_ours = true;
                eps.push((
                    if acks_tracked {
                        Owner::Unknown
                    } else {
                        Owner::Ours
                    },
                    true,
                ));
            }
            PauseEvent::Pause { ours: false } => eps.push((Owner::External, true)),
            PauseEvent::Ack => {
                if let Some(ep) = eps.iter_mut().rev().find(|(_, open)| *open) {
                    if ep.0 == Owner::Unknown {
                        ep.0 = Owner::Ours;
                    }
                }
            }
            PauseEvent::Resume => {
                if let Some(ep) = eps.iter_mut().rev().find(|(_, open)| *open) {
                    ep.1 = false;
                }
            }
            PauseEvent::Release { live } => {
                let owner = if *live { Owner::Ours } else { Owner::Unknown };
                match eps.iter_mut().rev().find(|(_, open)| *open) {
                    Some(ep) => ep.0 = owner,
                    None => eps.push((owner, true)),
                }
            }
            PauseEvent::ResumeIntent => {
                if let Some(ep) = eps.iter_mut().rev().find(|(_, open)| *open) {
                    if ep.0 == Owner::Ours {
                        ep.0 = Owner::Closing;
                    }
                }
            }
        }
    }
    let latest_open = eps.iter().rev().find(|(_, open)| *open).map(|(o, _)| *o);
    Episodes {
        ours: latest_open == Some(Owner::Ours),
        external: latest_open == Some(Owner::External),
        unknown: matches!(latest_open, Some(Owner::Unknown) | Some(Owner::Closing)),
        ever_ours,
    }
}

impl Journal {
    /// The exclusive lock on `<journal>.lock`, held by a ceremony agent for its whole run and by a readiness
    /// drill for its whole run (r30), so a ceremony and a drill (or two of either) on one journal are mutually
    /// exclusive atomically. Creates the journal's directory if needed; never touches the journal itself.
    pub fn lock(path: &Path) -> Result<File, String> {
        Self::try_lock(path).map_err(|e| match e {
            LockError::Held(m) | LockError::Io(m) => m,
        })
    }

    /// r30 round 6: like `lock`, but tells "another process holds it" (`Held`, the only answer that means a live
    /// competitor) apart from every other failure (`Io`: an unwritable or missing directory, a read-only mount), so
    /// a hygiene lock that cannot be taken is reported rather than mistaken for a competitor.
    pub fn try_lock(path: &Path) -> Result<File, LockError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    LockError::Io(format!("create lock dir {}: {e}", parent.display()))
                })?;
            }
        }
        let lock_path = PathBuf::from(format!("{}.lock", path.display()));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| LockError::Io(format!("open lock {}: {e}", lock_path.display())))?;
        // A lock that is busy for a moment is not a second agent: a process that forked while this
        // lock's previous holder had it open shares the lock until it execs (the fd is close-on-exec).
        // Wait up to 2 s; a real concurrent agent holds the lock for the whole ceremony.
        let mut busy = lock.try_lock();
        for _ in 0..100 {
            if !matches!(busy, Err(std::fs::TryLockError::WouldBlock)) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
            busy = lock.try_lock();
        }
        match busy {
            Ok(()) => Ok(lock),
            Err(std::fs::TryLockError::WouldBlock) => Err(LockError::Held(format!(
                "another pulse-cutover process holds {} — a ceremony (or a readiness drill) is already running on \
                 this journal. Refusing to start a second one.",
                lock_path.display()
            ))),
            Err(std::fs::TryLockError::Error(e)) => Err(LockError::Io(format!("lock {}: {e}", lock_path.display()))),
        }
    }

    pub fn open(path: &Path) -> Result<(Self, Recovered), String> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| format!("create journal dir: {e}"))?;
            }
        }
        // Exclusive lock FIRST: nothing below (tail repair, appends) may race another agent.
        let lock = Self::lock(path)?;
        let torn = if path.exists() {
            Self::repair_torn_tail(path)?
        } else {
            false
        };
        let recovered = if path.exists() {
            let mut r = Self::replay(path)?;
            r.torn_tail = torn;
            r
        } else {
            Recovered::default()
        };
        let seq = if path.exists() {
            BufReader::new(File::open(path).map_err(|e| e.to_string())?)
                .lines()
                .count() as u64
        } else {
            0
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open journal {}: {e}", path.display()))?;
        Ok((
            Journal {
                path: path.to_path_buf(),
                file,
                seq,
                fail_writes: 0,
                _lock: lock,
            },
            recovered,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Test support: make the next `n` writes fail (as ENOSPC would), writing nothing.
    #[doc(hidden)]
    pub fn inject_write_failures(&mut self, n: u32) {
        self.fail_writes = n;
    }

    fn write(&mut self, kind: &str, state: &str, data: Value) -> Result<Entry, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let ts_ms = now.as_millis() as u64;
        let entry = Entry {
            seq: self.seq,
            ts_ms,
            ts: chrono::DateTime::from_timestamp_millis(ts_ms as i64)
                .unwrap()
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string(),
            kind: kind.to_string(),
            state: state.to_string(),
            data,
        };
        let line = serde_json::to_string(&entry).map_err(|e| e.to_string())?;
        if self.fail_writes > 0 {
            self.fail_writes -= 1;
            return Err("journal write: injected failure (No space left on device)".into());
        }
        writeln!(self.file, "{line}").map_err(|e| format!("journal write: {e}"))?;
        self.file
            .sync_data()
            .map_err(|e| format!("journal fsync: {e}"))?;
        self.seq += 1;
        Ok(entry)
    }

    pub fn transition(&mut self, state: State, data: Value) -> Result<(), String> {
        let entry = self.write("transition", state.as_str(), data)?;
        eprintln!("[{}] -> {} {}", entry.ts, entry.state, entry.data);
        Ok(())
    }

    pub fn evidence(&mut self, state: State, data: Value) -> Result<(), String> {
        let entry = self.write("evidence", state.as_str(), data)?;
        eprintln!("[{}]    {} {}", entry.ts, entry.state, entry.data);
        Ok(())
    }

    pub fn error(&mut self, state: State, message: &str, data: Value) -> Result<(), String> {
        let entry = self.write(
            "error",
            state.as_str(),
            serde_json::json!({"message": message, "detail": data}),
        )?;
        eprintln!("[{}] !! {} {}", entry.ts, entry.state, entry.data);
        Ok(())
    }

    /// A crash mid-write can leave a partial last line. Appending after it would glue the next
    /// entry onto garbage and turn a harmless torn TAIL into a corrupt MIDDLE line.
    ///
    /// The physical boundary is the LAST LF. Everything before it is complete records (each was
    /// finished by its writer); the bytes after it are the only candidate for a torn write:
    /// - only whitespace/CR after the last LF: not a record, trimmed;
    /// - a non-whitespace fragment after the last LF that parses: a complete entry missing its
    ///   newline, which is added;
    /// - a non-whitespace fragment that does not parse: torn, moved to `<journal>.torn-<ms>`
    ///   (fsynced, including its directory, BEFORE the journal is truncated).
    ///
    /// The last LF-terminated record must parse: a complete record that does not is corruption,
    /// never silently dropped (that could roll recovery backward). Returns whether a torn tail
    /// was set aside.
    fn repair_torn_tail(path: &Path) -> Result<bool, String> {
        let bytes =
            std::fs::read(path).map_err(|e| format!("read journal {}: {e}", path.display()))?;
        if bytes.is_empty() {
            return Ok(false);
        }
        let last_lf = bytes.iter().rposition(|b| *b == b'\n');
        let tail_start = last_lf.map(|i| i + 1).unwrap_or(0);
        let tail = &bytes[tail_start..];
        let is_ws = |b: &u8| b.is_ascii_whitespace();
        // The last COMPLETE (LF-terminated) non-blank record must parse.
        if let Some(lf) = last_lf {
            let complete = &bytes[..lf];
            let mut end = complete.len();
            loop {
                let start = complete[..end]
                    .iter()
                    .rposition(|b| *b == b'\n')
                    .map(|i| i + 1)
                    .unwrap_or(0);
                let rec = &complete[start..end];
                if rec.iter().all(is_ws) {
                    if start == 0 {
                        break;
                    }
                    end = start - 1;
                    continue;
                }
                let rec = rec.strip_suffix(b"\r").unwrap_or(rec);
                if serde_json::from_slice::<Entry>(rec).is_err() {
                    return Err(format!(
                        "journal {} has a corrupt last complete (LF-terminated) record: corruption, \
                         not a torn write. Refusing to resume; inspect the journal by hand.",
                        path.display()
                    ));
                }
                break;
            }
        }
        if tail.is_empty() {
            return Ok(false);
        }
        if tail.iter().all(is_ws) {
            // Stray CR/blank bytes after the last LF: not a record. Trim so appends start clean.
            let f = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|e| e.to_string())?;
            f.set_len(tail_start as u64)
                .map_err(|e| format!("trim journal tail: {e}"))?;
            f.sync_data().map_err(|e| e.to_string())?;
            return Ok(false);
        }
        let frag = tail.strip_suffix(b"\r").unwrap_or(tail);
        if serde_json::from_slice::<Entry>(frag).is_ok() {
            // Complete entry, only its newline missing: add it so the next append starts clean.
            let mut f = OpenOptions::new()
                .append(true)
                .open(path)
                .map_err(|e| e.to_string())?;
            f.write_all(b"\n").map_err(|e| e.to_string())?;
            f.sync_data().map_err(|e| e.to_string())?;
            return Ok(false);
        }
        let aside = PathBuf::from(format!(
            "{}.torn-{}",
            path.display(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        ));
        {
            // The forensic copy (and its directory entry) must be durable BEFORE truncation.
            let mut f = File::create(&aside).map_err(|e| format!("save torn tail: {e}"))?;
            f.write_all(tail)
                .map_err(|e| format!("save torn tail: {e}"))?;
            f.sync_all()
                .map_err(|e| format!("fsync torn tail copy: {e}"))?;
            let dir = aside
                .parent()
                .filter(|d| !d.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| format!("fsync directory of torn tail copy {}: {e}", dir.display()))?;
        }
        let f = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        f.set_len(tail_start as u64)
            .map_err(|e| format!("truncate torn tail: {e}"))?;
        f.sync_data().map_err(|e| e.to_string())?;
        eprintln!(
            "journal: WARNING a partial last line (crash mid-write) was set aside to {} and \
             the journal truncated to its last complete entry",
            aside.display()
        );
        Ok(true)
    }

    /// Rebuild the machine-relevant facts from an existing journal. A partial LAST line (crash
    /// mid-write) is ignored with a warning; an unparsable line anywhere else is corruption and
    /// an error — the ceremony must not resume from a journal it cannot fully read.
    pub fn replay(path: &Path) -> Result<Recovered, String> {
        let file = File::open(path).map_err(|e| format!("open journal {}: {e}", path.display()))?;
        let mut out = Recovered::default();
        let lines: Vec<String> = BufReader::new(file)
            .lines()
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        // Only the bytes after the LAST LF can be a torn write (see repair_torn_tail): with
        // `lines()`, that fragment is the final element exactly when the file does not end in LF.
        let ends_with_newline = std::fs::read(path)
            .map(|b| b.last() == Some(&b'\n'))
            .unwrap_or(true);
        let tail_index = if ends_with_newline {
            None
        } else {
            lines.len().checked_sub(1)
        };
        for (i, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: Entry = match serde_json::from_str(line.trim_end_matches('\r')) {
                Ok(e) => e,
                Err(e) if Some(i) == tail_index => {
                    eprintln!(
                        "journal: WARNING ignoring a partial last line (crash mid-write): {e}"
                    );
                    out.torn_tail = true;
                    continue;
                }
                Err(e) => {
                    return Err(format!(
                    "corrupt journal line {} of {} (not the last line, so not a torn write): {e}",
                    i + 1,
                    path.display()
                ))
                }
            };
            // Fail closed on a journal from a NEWER agent: a record kind this agent does not know (or, below, a state it
            // does not know) means it cannot fold the journal faithfully. Never run an older agent on a newer journal.
            if !matches!(entry.kind.as_str(), "transition" | "evidence" | "error") {
                return Err(format!(
                    "journal line {} of {} has a record kind {:?} this agent does not know: the journal was written by a \
                     newer pulse-cutover. Never run an older agent on a journal a newer one wrote: use that version",
                    i + 1,
                    path.display(),
                    entry.kind
                ));
            }
            if entry.kind == "transition" {
                let st: State = entry.state.parse()?;
                if matches!(
                    st,
                    State::Ignited | State::Flipped | State::Live | State::Halted
                ) {
                    out.reached_ignited = true;
                }
                if st == State::Halted {
                    out.unhalted = false;
                    out.halted_from = entry
                        .data
                        .get("halted_from")
                        .and_then(|v| v.as_str())
                        .and_then(|v| v.parse().ok());
                }
                // Every ABORTED transition (re)opens the rollback; only a later `rollback_done` closes
                // it. Leaving ABORTED (a new ceremony on this journal) forgets the episode's steps.
                out.aborted_rollback_complete = false;
                // A STRANDED node is sealed (rc.29: its rollback intent may precede the strand); `join` and the
                // guarded `rollback` take it from there.
                if st == State::Aborted || st == State::Stranded {
                    out.rollback_pending = false;
                }
                if st != State::Aborted {
                    out.rollback_steps_done.clear();
                }
                out.state = Some(st);
                // The arming record (it carries "mode"; an unhalt returning to ARMED does not): bind the shadow identity.
                if st == State::Armed
                    && out.armed_shadow.is_none()
                    && entry.data.get("mode").is_some()
                {
                    out.armed_shadow =
                        Some(entry.data.get("shadow").cloned().unwrap_or(Value::Null));
                }
                if st == State::Armed
                    && out.undo_tracking.is_none()
                    && entry.data.get("mode").is_some()
                {
                    out.undo_tracking = Some(
                        entry.data.get("undo_tracking").and_then(|v| v.as_bool()) == Some(true),
                    );
                }
                if st == State::Armed
                    && out.pause_ack_tracking.is_none()
                    && entry.data.get("mode").is_some()
                {
                    out.pause_ack_tracking =
                        Some(entry.data.get("pause_ack").and_then(|v| v.as_bool()) == Some(true));
                }
                if entry.state == State::Frozen.as_str() {
                    out.frozen_ts_ms = Some(entry.ts_ms);
                }
            }
            // rc.6 wrote a halt as an error whose detail was `sealed: true` BEFORE its HALTED
            // transition: that error is HALTED even if the process died before the transition.
            // (rc.7+ writes one HALTED transition carrying the reason.)
            if entry.kind == "error"
                && entry
                    .data
                    .get("detail")
                    .and_then(|d| d.get("sealed"))
                    .and_then(|v| v.as_bool())
                    == Some(true)
            {
                out.halted_from = entry
                    .state
                    .parse()
                    .ok()
                    .filter(|s: &State| *s != State::Halted)
                    .or(out.halted_from);
                out.state = Some(State::Halted);
                out.reached_ignited = true;
                out.unhalted = false;
            }
            if entry.data.get("shadow_refused").and_then(|v| v.as_bool()) == Some(true)
                || entry
                    .data
                    .get("detail")
                    .and_then(|d| d.get("shadow_refused"))
                    .and_then(|v| v.as_bool())
                    == Some(true)
            {
                out.shadow_refused = true;
            }
            // r30 round 9 (review R9-1): a strand intent is satisfied by the STRANDED transition it caused (the transition
            // itself carries `external_pause_held` as a display field and is not an intent).
            if entry.kind != "transition"
                && (entry.data.get("strand_intent").and_then(|v| v.as_bool()) == Some(true)
                    || entry
                        .data
                        .get("external_pause_held")
                        .and_then(|v| v.as_bool())
                        == Some(true))
            {
                out.strand_intent = true;
            }
            if entry.kind == "transition" && entry.state == State::Stranded.as_str() {
                out.strand_intent = false;
            }
            // r30 round 9 (review R9-3): a new segment (a join re-entering this ceremony) starts with no undo of its own
            // done; writes the previous segment froze and never reopened stay ours to reopen.
            if let Some(seg) = entry.data.get("segment_start").and_then(|v| v.as_u64()) {
                let legacy_froze = out.undo_tracking == Some(false) && out.frozen_ts_ms.is_some();
                let reopened = out.rollback_steps_done.iter().any(|x| x == "on_abort");
                out.writes_closed = (out.writes_closed || legacy_froze) && !reopened;
                out.rollback_steps_done.clear();
                out.aborted_rollback_complete = false;
                out.strand_intent = false;
                out.segment = seg;
            }
            if let Some(b) = entry
                .data
                .get("mainnet_event_verified")
                .filter(|b| b.is_object())
            {
                out.mainnet_event_binding = Some(b.clone());
            }
            if out.legacy_takeover.is_none() {
                if let Some(t) = entry.data.get("legacy_takeover").filter(|t| t.is_object()) {
                    out.legacy_takeover = Some((
                        t.get("paused").and_then(|v| v.as_bool()) == Some(true),
                        t.get("froze").and_then(|v| v.as_bool()) == Some(true),
                    ));
                }
            }
            if entry
                .data
                .get("snapshot_scheduled_at")
                .and_then(|v| v.as_u64())
                .is_some()
            {
                out.scheduled = true;
            }
            if entry.data.get("source_pause_ack").is_some() {
                out.pause_events.push(PauseEvent::Ack);
            }
            // r30 round 12: a resume attempt followed by a paused (or unreadable) read is never settled back to "ours"
            // (`source_resume_outcome_unknown`): the episode stays closing = ownership unknown. A round-11
            // `source_resume_cancelled` record is read the same way.
            // r30 round 11 (review R10-2): the pause_at_h recovery resume closes its episode like a rollback's resume.
            if entry
                .data
                .get("resumed_production_for_snapshot")
                .and_then(|v| v.as_bool())
                == Some(true)
            {
                out.pause_events.push(PauseEvent::Resume);
            }
            // r30 round 14 (model R13-A): someone else un-paused this ceremony's own pause (a read saw it producing).
            if entry
                .data
                .get("own_pause_released_externally")
                .and_then(|v| v.as_bool())
                == Some(true)
            {
                out.pause_events.push(PauseEvent::Resume);
            }
            if let Some(se) = entry.data.get("side_effect").and_then(|v| v.as_str()) {
                match se {
                    "source_pause" => out.pause_events.push(PauseEvent::Pause {
                        ours: entry.data.get("adopted").and_then(|v| v.as_bool()) != Some(true),
                    }),
                    "external_pause_release" => {
                        out.pause_events.push(PauseEvent::Release { live: false })
                    }
                    "source_resume" => out.pause_events.push(PauseEvent::ResumeIntent),
                    "write_freeze" => out.writes_closed = true,
                    "admission_lock" => {
                        out.admission_lock_applied = false;
                        out.admission_lock = Some(
                            entry
                                .data
                                .get("original_actor_whitelist")
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    a.iter()
                                        .filter_map(|x| x.as_str().map(str::to_string))
                                        .collect()
                                })
                                .unwrap_or_default(),
                        )
                    }
                    _ => {}
                }
                // r30 round 7: a pause found after the freeze and ADOPTED for the cut (someone else paused the
                // producer) is journaled as `side_effect: source_pause` with `adopted: true`; it is recovered as
                // `source_pause_adopted`, never as this ceremony's own pause.
                let se = if se == "source_pause"
                    && entry.data.get("adopted").and_then(|v| v.as_bool()) == Some(true)
                {
                    "source_pause_adopted"
                } else {
                    se
                };
                if !out.side_effects.iter().any(|x| x == se) {
                    out.side_effects.push(se.to_string());
                }
                // Ignition may have started: from here a local failure seals (never resumes the
                // source), whatever state the journal was in when the process died.
                if se == "ignite_started" || se == "ignite" {
                    out.reached_ignited = true;
                }
            }
            if entry
                .data
                .get("rollback_requested")
                .and_then(|v| v.as_bool())
                == Some(true)
            {
                out.rollback_pending = true;
            }
            if entry
                .data
                .get("rollback_intent_cancelled")
                .and_then(|v| v.as_bool())
                == Some(true)
            {
                out.rollback_pending = false;
            }
            if entry.data.get("rollback_incomplete").is_some() {
                out.aborted_rollback_complete = false;
            }
            // A refused join re-froze writes (re-check #3): on_abort must run again on the next rollback.
            if entry.data.get("join_refused_after_refreeze").is_some() {
                out.aborted_rollback_complete = false;
                out.rollback_steps_done.retain(|x| x != "on_abort");
            }
            if entry.data.get("admission_lock_ack").is_some()
                || entry
                    .data
                    .get("admission_lock_reapplied")
                    .and_then(|v| v.as_bool())
                    == Some(true)
            {
                out.admission_lock_applied = true;
            }
            if let Some(step) = entry.data.get("rollback_step").and_then(|v| v.as_str()) {
                if entry.data.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                    if step == "resume" {
                        out.pause_events.push(PauseEvent::Resume);
                    }
                    if step == "admission_unlock" {
                        out.admission_lock = None;
                    }
                    if !out.rollback_steps_done.iter().any(|x| x == step) {
                        out.rollback_steps_done.push(step.to_string());
                    }
                }
            }
            if entry.data.get("rollback_done").and_then(|v| v.as_bool()) == Some(true)
                && out.state == Some(State::Aborted)
            {
                out.aborted_rollback_complete = true;
            }
            if entry.data.get("unhalted_by").is_some() {
                out.unhalted = true;
                // rc.24 fleet rehearsal (c2): the patience of a degraded wait belongs to the episode that halted. After an
                // operator's `unhalt` the re-run starts a new one; keeping the old start halted the re-run at its first
                // symptom with "patience exhausted (900 s since the first symptom)".
                out.degraded_since_ms = None;
            }
            if let Some(a) = entry.data.get("staged_artifact") {
                out.staged_sha256 = a.get("sha256").and_then(|v| v.as_str()).map(str::to_string);
                out.staged_cut_height = a.get("cut_height").and_then(|v| v.as_u64());
            }
            for (key, slot) in [
                ("target_blockchain_id", &mut out.target_blockchain_id),
                ("target_subnet_id", &mut out.target_subnet_id),
                (
                    "accepted_target_chain_id",
                    &mut out.accepted_target_chain_id,
                ),
                ("boot_manifest_sha256", &mut out.boot_manifest_sha256),
                ("boot_genesis_sha256", &mut out.boot_genesis_sha256),
                (
                    "boot_chain_config_sha256",
                    &mut out.boot_chain_config_sha256,
                ),
                ("chain_id", &mut out.chain_id),
                ("cut_block_id", &mut out.cut_block_id),
                ("snapshot_file", &mut out.snapshot_file),
                ("sha256", &mut out.sha256),
                ("last_source_block_time", &mut out.last_source_block_time),
            ] {
                if let Some(v) = entry.data.get(key).and_then(|v| v.as_str()) {
                    *slot = Some(v.to_string());
                }
            }
            if let Some(v) = entry.data.get("cut_height").and_then(|v| v.as_u64()) {
                out.cut_height = Some(v);
            }
            if let Some(v) = entry.data.get("degraded_since_ms").and_then(|v| v.as_u64()) {
                out.degraded_since_ms.get_or_insert(v);
            }
            if let Some(v) = entry.data.get("head_at_pause").and_then(|v| v.as_u64()) {
                out.head_at_pause = Some(v);
            }
            if let Some(v) = entry.data.get("resolved_h").and_then(|v| v.as_u64()) {
                out.resolved_h = Some(v);
            }
        }
        Ok(out)
    }
}
