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
}

/// Ceremony facts recovered from a journal on resume: everything a restarted
/// agent must not re-derive differently than the first run did.
#[derive(Debug, Clone, Default)]
pub struct Recovered {
    pub state: Option<State>,
    pub chain_id: Option<String>,
    pub cut_height: Option<u64>,
    pub cut_block_id: Option<String>,
    pub snapshot_file: Option<String>,
    pub sha256: Option<String>,
    pub frozen_ts_ms: Option<u64>,
    pub last_source_block_time: Option<String>,
}

impl Journal {
    pub fn open(path: &Path) -> Result<(Self, Recovered), String> {
        let recovered = if path.exists() {
            Self::replay(path)?
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create journal dir: {e}"))?;
            }
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

    /// Rebuild the machine-relevant facts from an existing journal.
    pub fn replay(path: &Path) -> Result<Recovered, String> {
        let file = File::open(path).map_err(|e| format!("open journal {}: {e}", path.display()))?;
        let mut out = Recovered::default();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|e| e.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: Entry =
                serde_json::from_str(&line).map_err(|e| format!("corrupt journal line: {e}"))?;
            if entry.kind == "transition" {
                out.state = Some(entry.state.parse()?);
                if entry.state == State::Frozen.as_str() {
                    out.frozen_ts_ms = Some(entry.ts_ms);
                }
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
        }
        Ok(out)
    }
}
