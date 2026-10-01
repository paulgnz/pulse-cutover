//! Append-only JSONL ceremony journal.
//!
//! Every state transition and every piece of evidence (hashes, block ids,
//! timings) is one line, written with fsync before the machine acts on it —
//! the journal is both the audit record and the crash-resume source of truth.

use std::{
    fs::{
        File,
        OpenOptions,
    },
    io::{
        BufRead,
        BufReader,
        Write,
    },
    path::{
        Path,
        PathBuf,
    },
};

use serde::{
    Deserialize,
    Serialize,
};
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

pub struct Journal {
    path: PathBuf,
    file: File,
    seq: u64,
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
}

impl Journal {
    pub fn open(path: &Path) -> Result<(Self, Recovered), String> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| format!("create journal dir: {e}"))?;
            }
        }
        // Exclusive lock FIRST: nothing below (tail repair, appends) may race another agent.
        let lock_path = PathBuf::from(format!("{}.lock", path.display()));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| format!("open journal lock {}: {e}", lock_path.display()))?;
        // A lock that is busy for a moment is not a second agent: a process that forked while this
        // lock's previous holder had it open shares the lock until it execs (the fd is close-on-exec).
        // Wait up to 2 s; a real concurrent agent holds the lock for the whole ceremony.
        let mut busy = lock.try_lock();
        for _ in 0..100 {
            if !matches!(busy, Err(std::fs::TryLockError::WouldBlock)) { break; }
            std::thread::sleep(std::time::Duration::from_millis(20));
            busy = lock.try_lock();
        }
        match busy {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(format!(
                    "another pulse-cutover process holds {} — a ceremony is already running on \
                     this journal. Refusing to start a second one.",
                    lock_path.display()
                ))
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(format!("lock {}: {e}", lock_path.display()))
            }
        }
        let torn = if path.exists() { Self::repair_torn_tail(path)? } else { false };
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
                _lock: lock,
            },
            recovered,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
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
        writeln!(self.file, "{line}").map_err(|e| format!("journal write: {e}"))?;
        self.file.sync_data().map_err(|e| format!("journal fsync: {e}"))?;
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
    /// The last LF-terminated record must parse: a complete record that does not is corruption,
    /// never silently dropped (that could roll recovery backward). Returns whether a torn tail
    /// was set aside.
    fn repair_torn_tail(path: &Path) -> Result<bool, String> {
        let bytes = std::fs::read(path).map_err(|e| format!("read journal {}: {e}", path.display()))?;
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
                let start = complete[..end].iter().rposition(|b| *b == b'\n').map(|i| i + 1).unwrap_or(0);
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
            let f = OpenOptions::new().write(true).open(path).map_err(|e| e.to_string())?;
            f.set_len(tail_start as u64).map_err(|e| format!("trim journal tail: {e}"))?;
            f.sync_data().map_err(|e| e.to_string())?;
            return Ok(false);
        }
        let frag = tail.strip_suffix(b"\r").unwrap_or(tail);
        if serde_json::from_slice::<Entry>(frag).is_ok() {
            // Complete entry, only its newline missing: add it so the next append starts clean.
            let mut f = OpenOptions::new().append(true).open(path).map_err(|e| e.to_string())?;
            f.write_all(b"\n").map_err(|e| e.to_string())?;
            f.sync_data().map_err(|e| e.to_string())?;
            return Ok(false);
        }
        let aside = PathBuf::from(format!(
            "{}.torn-{}",
            path.display(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
        ));
        {
            // The forensic copy (and its directory entry) must be durable BEFORE truncation.
            let mut f = File::create(&aside).map_err(|e| format!("save torn tail: {e}"))?;
            f.write_all(tail).map_err(|e| format!("save torn tail: {e}"))?;
            f.sync_all().map_err(|e| format!("fsync torn tail copy: {e}"))?;
            let dir = aside.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
            File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| format!("fsync directory of torn tail copy {}: {e}", dir.display()))?;
        }
        let f = OpenOptions::new().write(true).open(path).map_err(|e| e.to_string())?;
        f.set_len(tail_start as u64).map_err(|e| format!("truncate torn tail: {e}"))?;
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
        let ends_with_newline = std::fs::read(path).map(|b| b.last() == Some(&b'\n')).unwrap_or(true);
        let tail_index = if ends_with_newline { None } else { lines.len().checked_sub(1) };
        for (i, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: Entry = match serde_json::from_str(line.trim_end_matches('\r')) {
                Ok(e) => e,
                Err(e) if Some(i) == tail_index => {
                    eprintln!("journal: WARNING ignoring a partial last line (crash mid-write): {e}");
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
            if entry.kind == "transition" {
                let st: State = entry.state.parse()?;
                if matches!(st, State::Ignited | State::Flipped | State::Live | State::Halted) {
                    out.reached_ignited = true;
                }
                if st == State::Halted {
                    out.unhalted = false;
                    out.halted_from = entry.data.get("halted_from").and_then(|v| v.as_str()).and_then(|v| v.parse().ok());
                }
                // Every ABORTED transition (re)opens the rollback; only a later `rollback_done` closes
                // it. Leaving ABORTED (a new ceremony on this journal) forgets the episode's steps.
                out.aborted_rollback_complete = false;
                if st == State::Aborted {
                    out.rollback_pending = false;
                }
                if st != State::Aborted {
                    out.rollback_steps_done.clear();
                }
                out.state = Some(st);
                if entry.state == State::Frozen.as_str() {
                    out.frozen_ts_ms = Some(entry.ts_ms);
                }
            }
            // rc.6 wrote a halt as an error whose detail was `sealed: true` BEFORE its HALTED
            // transition: that error is HALTED even if the process died before the transition.
            // (rc.7+ writes one HALTED transition carrying the reason.)
            if entry.kind == "error"
                && entry.data.get("detail").and_then(|d| d.get("sealed")).and_then(|v| v.as_bool()) == Some(true)
            {
                out.halted_from = entry.state.parse().ok().filter(|s: &State| *s != State::Halted).or(out.halted_from);
                out.state = Some(State::Halted);
                out.reached_ignited = true;
                out.unhalted = false;
            }
            if entry.data.get("snapshot_scheduled_at").and_then(|v| v.as_u64()).is_some() {
                out.scheduled = true;
            }
            if let Some(se) = entry.data.get("side_effect").and_then(|v| v.as_str()) {
                if !out.side_effects.iter().any(|x| x == se) {
                    out.side_effects.push(se.to_string());
                }
                // Ignition may have started: from here a local failure seals (never resumes the
                // source), whatever state the journal was in when the process died.
                if se == "ignite_started" || se == "ignite" {
                    out.reached_ignited = true;
                }
            }
            if entry.data.get("rollback_requested").and_then(|v| v.as_bool()) == Some(true) {
                out.rollback_pending = true;
            }
            if entry.data.get("rollback_intent_cancelled").and_then(|v| v.as_bool()) == Some(true) {
                out.rollback_pending = false;
            }
            if entry.data.get("rollback_incomplete").is_some() {
                out.aborted_rollback_complete = false;
            }
            if let Some(step) = entry.data.get("rollback_step").and_then(|v| v.as_str()) {
                if entry.data.get("ok").and_then(|v| v.as_bool()) == Some(true)
                    && !out.rollback_steps_done.iter().any(|x| x == step)
                {
                    out.rollback_steps_done.push(step.to_string());
                }
            }
            if entry.data.get("rollback_done").and_then(|v| v.as_bool()) == Some(true)
                && out.state == Some(State::Aborted)
            {
                out.aborted_rollback_complete = true;
            }
            if entry.data.get("unhalted_by").is_some() {
                out.unhalted = true;
            }
            if let Some(a) = entry.data.get("staged_artifact") {
                out.staged_sha256 = a.get("sha256").and_then(|v| v.as_str()).map(str::to_string);
                out.staged_cut_height = a.get("cut_height").and_then(|v| v.as_u64());
            }
            for (key, slot) in [
                ("target_blockchain_id", &mut out.target_blockchain_id),
                ("target_subnet_id", &mut out.target_subnet_id),
                ("accepted_target_chain_id", &mut out.accepted_target_chain_id),
                ("boot_manifest_sha256", &mut out.boot_manifest_sha256),
                ("boot_genesis_sha256", &mut out.boot_genesis_sha256),
                ("boot_chain_config_sha256", &mut out.boot_chain_config_sha256),
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
            if let Some(v) = entry.data.get("resolved_h").and_then(|v| v.as_u64()) {
                out.resolved_h = Some(v);
            }
        }
        Ok(out)
    }
}
