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
        match lock.try_lock() {
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
    /// entry onto garbage and turn a harmless torn TAIL into a corrupt MIDDLE line. So: if the
    /// last line does not parse, move it to `<journal>.torn-<ms>` (forensics) and truncate the
    /// journal back to the last complete line. Returns whether a tail was set aside.
    fn repair_torn_tail(path: &Path) -> Result<bool, String> {
        let bytes = std::fs::read(path).map_err(|e| format!("read journal {}: {e}", path.display()))?;
        if bytes.is_empty() {
            return Ok(false);
        }
        // Start of the last non-empty line.
        let trimmed_end = bytes.iter().rposition(|b| *b != b'\n' && *b != b'\r').map(|i| i + 1).unwrap_or(0);
        if trimmed_end == 0 {
            return Ok(false);
        }
        let start = bytes[..trimmed_end].iter().rposition(|b| *b == b'\n').map(|i| i + 1).unwrap_or(0);
        let last = &bytes[start..trimmed_end];
        let ends_with_newline = bytes.last() == Some(&b'\n');
        let parses = serde_json::from_slice::<Entry>(last).is_ok();
        if !parses && ends_with_newline {
            // A complete (newline-terminated) line that does not parse is not a torn write: the
            // writer finished it. That is corruption, and recovering past it could roll the
            // ceremony backward (e.g. lose an authority-relevant last record). Refuse.
            return Err(format!(
                "journal {} ends with a complete but unparsable line: corruption, not a torn \
                 write. Refusing to resume; inspect the journal by hand.",
                path.display()
            ));
        }
        if parses {
            if !ends_with_newline {
                // Complete entry but missing its newline: add it so the next append starts clean.
                let mut f = OpenOptions::new().append(true).open(path).map_err(|e| e.to_string())?;
                f.write_all(b"\n").map_err(|e| e.to_string())?;
                f.sync_data().map_err(|e| e.to_string())?;
            }
            return Ok(false);
        }
        let aside = PathBuf::from(format!(
            "{}.torn-{}",
            path.display(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
        ));
        {
            // The forensic copy must be durable BEFORE the journal is truncated.
            let mut f = File::create(&aside).map_err(|e| format!("save torn tail: {e}"))?;
            f.write_all(last).map_err(|e| format!("save torn tail: {e}"))?;
            f.sync_all().map_err(|e| format!("fsync torn tail copy: {e}"))?;
            if let Some(dir) = aside.parent() {
                if let Ok(d) = File::open(dir) {
                    let _ = d.sync_all();
                }
            }
        }
        let f = OpenOptions::new().write(true).open(path).map_err(|e| e.to_string())?;
        f.set_len(start as u64).map_err(|e| format!("truncate torn tail: {e}"))?;
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
        let ends_with_newline = std::fs::read(path).map(|b| b.last() == Some(&b'\n')).unwrap_or(true);
        let last_nonempty = lines.iter().rposition(|l| !l.trim().is_empty());
        for (i, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: Entry = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(e) if Some(i) == last_nonempty && !ends_with_newline => {
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
                out.state = Some(st);
                if entry.state == State::Frozen.as_str() {
                    out.frozen_ts_ms = Some(entry.ts_ms);
                }
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
            if entry.data.get("unhalted_by").is_some() {
                out.unhalted = true;
            }
            if let Some(a) = entry.data.get("staged_artifact") {
                out.staged_sha256 = a.get("sha256").and_then(|v| v.as_str()).map(str::to_string);
                out.staged_cut_height = a.get("cut_height").and_then(|v| v.as_u64());
            }
            for (key, slot) in [
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
