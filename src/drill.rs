//! Readiness drill (risk register D-7, test-ladder rung 4): every producer proves, on the LIVE chain and
//! with zero impact on it, that its own infrastructure can cut exactly H and convert it to the same state as
//! everyone else, and how long each step takes.
//!
//! A drill schedules the nodeos snapshot at exactly H (the ceremony's `schedule_at_h` path), waits for H to
//! become final while the chain keeps producing, picks up `snapshot-<id of H>.bin`, and runs the upstream
//! verification pipeline (export → import → compare → fingerprint) on it. It reports the true compare result
//! (never allow-listed) and the same `fingerprints_digest` a real ceremony's VERIFIED record carries, so mission
//! control can compare producers.
//!
//! What it never does, by construction:
//! - it never touches the ceremony journal (it refuses to start while one is in progress and only reads it);
//! - it never holds the raw [`ChainOps`]: every call goes through [`DrillGuard`], which refuses (and records)
//!   pause, resume, create_snapshot, ignite, hooks, orphan-hook kills and target binding;
//! - it never runs a hook (`on_freeze`, `post_ignite`, `on_live`, flips, stop/start commands), never creates a
//!   chain and never reads or writes `snapshot.staged_path`;
//! - its pipeline runs in its own work directory (`<journal dir>/drill/<H>/work`) with its own process-group
//!   file, so a real ceremony's orphan-hook cleanup and the drill can never kill each other's processes.
//!
//! Post-H blocks are expected (the chain keeps running) and are recorded as information only.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::config::{Config, ImportBackend};
use crate::journal::Journal;
use crate::ops::{ChainInfo, ChainOps, SnapshotResult};
use crate::state::State;
use crate::upstream::{self, PipelineError};

/// The marker every drill record and the beacon view carry, so nothing reads a drill as a ceremony.
pub const DRILL_KIND: &str = "readiness_drill";

/// Minimum distance between the current head and H when the drill schedules its snapshot: nodeos only writes a
/// scheduled snapshot for a block it has not produced/applied yet.
pub const MIN_LEAD_BLOCKS: u64 = 20;

/// Where drills live: `<journal dir>/drill/`. Fixed (no flag), so the beacon always finds the latest result.
pub fn drill_root(cfg: &Config) -> PathBuf {
    cfg.journal_path.parent().unwrap_or_else(|| Path::new(".")).join("drill")
}

pub fn latest_path(cfg: &Config) -> PathBuf {
    drill_root(cfg).join("latest.json")
}

#[derive(Debug, Clone)]
pub struct DrillOptions {
    /// The agreed drill height (same on every producer).
    pub height: u64,
    /// Keep the multi-GB work files (export, arenas, checkpoint). Default: delete them, keep the evidence.
    pub keep_work: bool,
}

// ---------------------------------------------------------------------------------------------- guard ----

/// The only handle a drill has on the chain. Read-only calls, the snapshot schedule and the pipeline's long
/// commands pass through; everything that could change the chain, the producer or the target is refused and
/// recorded in `violations` (the drill then fails loudly; tests assert it stays empty).
pub struct DrillGuard<'a, O: ChainOps> {
    inner: &'a O,
    pub violations: RefCell<Vec<String>>,
}

impl<'a, O: ChainOps> DrillGuard<'a, O> {
    pub fn new(inner: &'a O) -> Self {
        DrillGuard { inner, violations: RefCell::new(vec![]) }
    }

    fn refuse<T>(&self, what: &str) -> Result<T, String> {
        self.violations.borrow_mut().push(what.to_string());
        Err(format!("readiness drill: {what} is forbidden (a drill never changes the chain, the producer or a target)"))
    }
}

impl<O: ChainOps> ChainOps for DrillGuard<'_, O> {
    fn source_info(&self) -> Result<ChainInfo, String> {
        self.inner.source_info()
    }
    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        self.inner.source_block_id(block_num)
    }
    fn source_block_tx_count(&self, block_num: u64) -> Result<u64, String> {
        self.inner.source_block_tx_count(block_num)
    }
    fn producer_paused(&self) -> Result<bool, String> {
        self.inner.producer_paused()
    }
    fn pause(&self) -> Result<(), String> {
        self.refuse("pause")
    }
    fn resume(&self) -> Result<(), String> {
        self.refuse("resume")
    }
    fn create_snapshot(&self) -> Result<SnapshotResult, String> {
        self.refuse("create_snapshot (a drill only uses the snapshot scheduled at exactly H)")
    }
    fn schedule_snapshot(&self, height: u64) -> Result<(), String> {
        self.inner.schedule_snapshot(height)
    }
    fn target_info(&self) -> Result<Option<ChainInfo>, String> {
        self.refuse("target_info")
    }
    fn public_info(&self, _public_url: &str) -> Result<Option<ChainInfo>, String> {
        self.refuse("public_info")
    }
    fn target_block_id(&self, _height: u64) -> Result<Option<String>, String> {
        self.refuse("target_block_id")
    }
    fn target_protocol(&self) -> Result<Option<Value>, String> {
        self.refuse("target_protocol")
    }
    fn public_block_id(&self, _public_url: &str, _height: u64) -> Result<Option<String>, String> {
        self.refuse("public_block_id")
    }
    fn source_block(&self, block_num: u64, rpc_url: Option<&str>) -> Result<Value, String> {
        self.inner.source_block(block_num, rpc_url)
    }
    fn source_producer(&self, producer: &str) -> Result<Option<Value>, String> {
        self.inner.source_producer(producer)
    }
    fn bind_target(&self, _vars: &[(String, String)]) {
        let _ = self.refuse::<()>("bind_target");
    }
    fn ignite(&self) -> Result<String, String> {
        self.refuse("ignite")
    }
    fn run_hook(&self, _cmd: &str) -> Result<String, String> {
        self.refuse("run_hook")
    }
    fn kill_orphan_hooks(&self) -> Result<Option<String>, String> {
        self.refuse("kill_orphan_hooks")
    }
    fn run_long(&self, cmd: &str) -> Result<String, String> {
        self.inner.run_long(cmd)
    }
    fn run_long_cancellable(&self, cmd: &str, cancel: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        self.inner.run_long_cancellable(cmd, cancel)
    }
    fn get_json(&self, url: &str) -> Result<Option<Value>, String> {
        self.inner.get_json(url)
    }
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }
    fn sleep_ms(&self, ms: u64) {
        self.inner.sleep_ms(ms)
    }
}

// --------------------------------------------------------------------------------------- preconditions ----

/// Why this config cannot run a drill (empty = it can). Checked before anything is scheduled.
pub fn config_problems(cfg: &Config) -> Vec<String> {
    let mut p = vec![];
    if cfg.ceremony.import_backend != ImportBackend::Upstream {
        p.push("a drill runs the official upstream pipeline: set [ceremony] import_backend = \"upstream\"".into());
    }
    match &cfg.upstream {
        None => p.push("a drill needs an [upstream] section (export_cmd, import_bin, compare_bin, fingerprint_bin)".into()),
        Some(u) if u.compare_bin.is_none() => {
            p.push("a drill needs [upstream] compare_bin: the table comparison is half of what it measures".into())
        }
        _ => {}
    }
    if cfg.snapshot.dir.is_none() {
        p.push("a drill picks up nodeos's scheduled snapshot: set [snapshot] dir".into());
    }
    p
}

/// A drill must not run next to a ceremony in progress (it would compete for CPU, RAM and disk at the worst
/// moment, and a sealed ceremony's producer is paused). Allowed: no journal, an empty one, or a finished ABORTED.
pub fn ceremony_conflict(cfg: &Config) -> Result<Option<String>, String> {
    if !cfg.journal_path.exists() {
        return Ok(None);
    }
    let r = Journal::replay(&cfg.journal_path)?;
    Ok(match r.state {
        None => None,
        Some(State::Aborted) if r.aborted_rollback_complete && !r.rollback_pending => None,
        Some(s) => Some(format!(
            "the ceremony journal {} is in {s}: a drill never runs next to a ceremony in progress, sealed, \
             unfinished or LIVE (use a config whose journal_path is not a live ceremony's)",
            cfg.journal_path.display()
        )),
    })
}

// ------------------------------------------------------------------------------------------------ run ----

/// The drill's own append-only record (`<drill dir>/<H>/drill.jsonl`): one JSON line per step, fsynced.
struct DrillLog {
    path: PathBuf,
    latest: PathBuf,
    view: Value,
}

impl DrillLog {
    fn record(&mut self, now_ms: u64, stage: &str, data: Value) -> Result<(), String> {
        use std::io::Write;
        let line = json!({"kind": DRILL_KIND, "ts_ms": now_ms, "stage": stage, "data": data});
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("open drill log {}: {e}", self.path.display()))?;
        writeln!(f, "{line}").map_err(|e| format!("write drill log: {e}"))?;
        f.sync_all().map_err(|e| format!("fsync drill log: {e}"))?;
        Ok(())
    }

    /// Rewrite `latest.json` (the beacon's view) atomically.
    fn publish(&mut self, patch: Value) -> Result<(), String> {
        if let (Some(v), Some(p)) = (self.view.as_object_mut(), patch.as_object()) {
            for (k, val) in p {
                v.insert(k.clone(), val.clone());
            }
        }
        let tmp = self.latest.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&self.view).unwrap_or_default())
            .map_err(|e| format!("write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &self.latest).map_err(|e| format!("rename {}: {e}", self.latest.display()))
    }
}

/// The outcome the CLI prints and maps to an exit code.
#[derive(Debug, Clone)]
pub struct DrillOutcome {
    pub view: Value,
    /// The evidence is complete (fingerprint computed).
    pub evidence_complete: bool,
    /// The compare matched outright.
    pub compare_match: bool,
}

/// Run one readiness drill at `opts.height`. `ops` is wrapped in a [`DrillGuard`]; nothing else is used.
pub fn run<O: ChainOps>(cfg: &Config, ops: &O, opts: &DrillOptions) -> Result<DrillOutcome, String> {
    let guard = DrillGuard::new(ops);
    let res = run_guarded(cfg, &guard, opts);
    let v = guard.violations.borrow().clone();
    if !v.is_empty() {
        // Defense in depth: the guard already refused each call; a drill that even tried is a bug to report.
        return Err(format!("readiness drill tried forbidden operations (all refused): {}", v.join(", ")));
    }
    res
}

fn run_guarded<O: ChainOps>(cfg: &Config, ops: &DrillGuard<'_, O>, opts: &DrillOptions) -> Result<DrillOutcome, String> {
    let problems = config_problems(cfg);
    if !problems.is_empty() {
        return Err(format!("this config cannot run a readiness drill: {}", problems.join("; ")));
    }
    if let Some(why) = ceremony_conflict(cfg)? {
        return Err(why);
    }
    let h = opts.height;
    let info = ops.source_info()?;
    if let Some(pinned) = &cfg.ceremony.chain_id {
        if !pinned.eq_ignore_ascii_case(&info.chain_id) {
            return Err(format!("source chain_id {} != configured ceremony.chain_id {pinned}", info.chain_id));
        }
    }
    let dir = drill_root(cfg).join(h.to_string());
    if dir.join("drill.jsonl").exists() {
        return Err(format!(
            "a drill at H = {h} already ran here ({}); every drill height is used once",
            dir.display()
        ));
    }
    if h < info.head_block_num + MIN_LEAD_BLOCKS {
        return Err(format!(
            "drill height {h} is too close: head is {} (need H ≥ head + {MIN_LEAD_BLOCKS} so nodeos can still \
             schedule the snapshot); pick a later H, the same on every producer",
            info.head_block_num
        ));
    }

    let work = dir.join("work");
    std::fs::create_dir_all(&work).map_err(|e| format!("create {}: {e}", work.display()))?;
    let started = ops.now_ms();
    let mut log = DrillLog {
        path: dir.join("drill.jsonl"),
        latest: drill_root(cfg).join("latest.json"),
        view: json!({
            "kind": DRILL_KIND,
            "drill": true,
            "h": h,
            "state": "RUNNING",
            "stage": "scheduled",
            "chain_id": info.chain_id,
            "agent_version": env!("CARGO_PKG_VERSION"),
            "started_ts_ms": started,
            "timings_ms": {},
        }),
    };
    log.record(started, "start", json!({"h": h, "head": info.head_block_num, "lib": info.last_irreversible_block_num,
        "chain_id": info.chain_id, "note": "readiness drill: no freeze, no pause, no chain creation"}))?;

    let outcome = drive(cfg, ops, opts, h, &work, started, &mut log);
    match outcome {
        Ok(o) => Ok(o),
        Err(e) => {
            let now = ops.now_ms();
            let _ = log.record(now, "failed", json!({"error": e}));
            let _ = log.publish(json!({"state": "FAILED", "error": crate::beacon::sanitize_short(&e), "finished_ts_ms": now}));
            if !opts.keep_work {
                let _ = std::fs::remove_dir_all(&work);
            }
            Err(e)
        }
    }
}

fn drive<O: ChainOps>(
    cfg: &Config,
    ops: &DrillGuard<'_, O>,
    opts: &DrillOptions,
    h: u64,
    work: &Path,
    started: u64,
    log: &mut DrillLog,
) -> Result<DrillOutcome, String> {
    let mut timings = serde_json::Map::new();

    // 1. Schedule the snapshot at exactly H (the same producer-API call the ceremony makes at ARM).
    ops.schedule_snapshot(h).map_err(|e| format!("schedule_snapshot({h}) failed: {e}"))?;
    log.record(ops.now_ms(), "snapshot_scheduled", json!({"h": h}))?;
    log.publish(json!({"stage": "waiting_for_h"}))?;

    // 2. Wait for H to be produced and then final. The chain keeps running: nothing is frozen.
    let head_now = ops.source_info()?.head_block_num;
    // Generous: 1 s per block still to come (blocks are 0.5 s), plus the configured snapshot timeout for finality.
    let deadline = ops.now_ms() + (h.saturating_sub(head_now) * 1000) + cfg.source.snapshot_timeout_secs * 1000;
    let mut produced_ms: Option<u64> = None;
    let mut last_beat = 0u64;
    loop {
        let i = ops.source_info()?;
        let now = ops.now_ms();
        if produced_ms.is_none() && i.head_block_num >= h {
            produced_ms = Some(now);
        }
        if i.last_irreversible_block_num >= h {
            break;
        }
        if now > deadline {
            return Err(format!("H = {h} did not become final in time (lib {})", i.last_irreversible_block_num));
        }
        if now.saturating_sub(last_beat) >= 30_000 {
            log.record(now, "awaiting_h_final", json!({"head": i.head_block_num, "lib": i.last_irreversible_block_num}))?;
            last_beat = now;
        }
        ops.sleep_ms(cfg.poll_ms);
    }
    let final_ms = ops.now_ms();
    if let Some(p) = produced_ms {
        timings.insert("finality_wait".into(), json!(final_ms.saturating_sub(p)));
    }

    // 3. Pin H's block id and pick up the file nodeos writes as `snapshot-<id of H>.bin`.
    let (id_h, _ts) = ops.source_block_id(h)?;
    let snap_dir = cfg.snapshot.dir.clone().ok_or("snapshot.dir is required")?;
    let snapshot = snap_dir.join(format!("snapshot-{id_h}.bin"));
    while !snapshot.exists() {
        if ops.now_ms() > deadline {
            return Err(format!("scheduled snapshot {} did not appear in time", snapshot.display()));
        }
        ops.sleep_ms(cfg.poll_ms);
    }
    timings.insert("snapshot_file_wait".into(), json!(ops.now_ms().saturating_sub(final_ms)));
    let t = ops.now_ms();
    let (sha256, size) = crate::verify::sha256_file(&snapshot)?;
    timings.insert("snapshot_sha256".into(), json!(ops.now_ms().saturating_sub(t)));
    log.record(ops.now_ms(), "snapshot", json!({"file": snapshot.display().to_string(), "block_id_at_h": id_h,
        "sha256": sha256, "bytes": size}))?;
    log.publish(json!({"stage": "converting", "block_id_at_h": id_h, "snapshot_sha256": sha256, "snapshot_bytes": size,
        "timings_ms": Value::Object(timings.clone())}))?;

    // 4. The upstream pipeline, unchanged, in the drill's own work dir. The rehearsal allowlist and a golden root
    //    pinned for a real event are cleared: a drill reports the true compare result for its own H.
    let mut up = cfg.upstream.clone().ok_or("no [upstream] section")?;
    up.work_dir = work.to_path_buf();
    up.rehearsal_allow_compare_mismatch.clear();
    up.golden_state_root = None;
    let chain_id = ops.source_info()?.chain_id;
    let mut blobs: Vec<Value> = vec![];
    let conv_started = ops.now_ms();
    let res = upstream::run_pipeline(&up, ops, &snapshot, &sha256, h, &id_h, &chain_id,
        |v| blobs.push(v), |_| false);
    for b in &blobs {
        log.record(ops.now_ms(), "pipeline", b.clone())?;
    }
    let find = |key: &str| blobs.iter().rev().find_map(|b| b.get(key).cloned());
    if let Some(ms) = blobs.iter().find_map(|b| b["export_wall_ms"].as_u64()) {
        timings.insert("export".into(), json!(ms));
    }
    if let Some(ms) = find("upstream_import").and_then(|v| v["import_wall_ms"].as_u64()) {
        timings.insert("import".into(), json!(ms));
    }
    let compare = find("upstream_19_table_compare");
    if let Some(ms) = compare.as_ref().and_then(|v| v["compare_wall_ms"].as_u64()) {
        timings.insert("compare".into(), json!(ms));
    }
    let compare_result = compare.as_ref().and_then(|v| v["result"].as_str().map(str::to_string));
    let failing: Vec<Value> = compare.as_ref().and_then(|v| v["failing_tables"].as_array().cloned()).unwrap_or_default();

    let (state_root, compare_match) = match res {
        Ok(out) => {
            // The pipeline does not time its last step: conversion total minus the timed steps.
            let timed: u64 = ["export", "import", "compare"].iter()
                .map(|k| timings.get(*k).and_then(Value::as_u64).unwrap_or(0)).sum();
            timings.insert("fingerprint_approx".into(), json!(ops.now_ms().saturating_sub(conv_started).saturating_sub(timed)));
            (out.state_root, true)
        }
        Err(PipelineError::Failed(e)) if compare_result.as_deref() == Some("MISMATCH") => {
            // The comparison disagreed: that IS the drill's finding. The fingerprint is still computed (the same
            // tool and arguments the pipeline uses) so producers can be compared with each other.
            log.record(ops.now_ms(), "compare_mismatch", json!({"error": e}))?;
            let checkpoint = work.join(format!("checkpoint-{h}.bin"));
            if !checkpoint.exists() {
                return Err(format!("compare failed and no checkpoint exists to fingerprint: {e}"));
            }
            let arena = work.join(format!("arena-fingerprint-{h}"));
            let _ = std::fs::remove_dir_all(&arena);
            std::fs::create_dir_all(&arena).map_err(|e| format!("create {}: {e}", arena.display()))?;
            let cmd = format!("'{}' '{}' '{}'", up.fingerprint_bin.display(), checkpoint.display(), arena.display());
            let t = ops.now_ms();
            let out = ops.run_long(&cmd).map_err(|e| format!("xpr_state_fingerprint failed: {e}"))?;
            timings.insert("fingerprint".into(), json!(ops.now_ms().saturating_sub(t)));
            let (root, _tables) = crate::verify::parse_upstream_report(&out);
            (root, false)
        }
        Err(PipelineError::Failed(e)) => return Err(format!("conversion failed: {e}")),
        Err(PipelineError::Aborted { during }) => return Err(format!("conversion stopped during {during}")),
    };
    let end = ops.now_ms();
    timings.insert("conversion_total".into(), json!(end.saturating_sub(conv_started)));
    timings.insert("h_final_to_evidence".into(), json!(end.saturating_sub(final_ms)));
    timings.insert("total".into(), json!(end.saturating_sub(started)));

    let root_ok = state_root.as_deref().is_some_and(|r| r.len() >= 16 && r.chars().all(|c| c.is_ascii_hexdigit()));
    if !root_ok {
        return Err(format!("xpr_state_fingerprint printed no usable state_root ({state_root:?})"));
    }
    // The same digest a real ceremony's VERIFIED record carries (machine.rs: {"upstream_state_root": root}).
    let fingerprints_digest = crate::beacon::fingerprints_digest(&json!({"upstream_state_root": state_root}));
    let head_end = ops.source_info().map(|i| i.head_block_num).unwrap_or(0);

    // Keep the small evidence files, drop the multi-GB work files unless asked to keep them.
    let evidence_dir = work.parent().unwrap_or(work).to_path_buf();
    for name in [format!("compare-report-{h}.json"), format!("checkpoint-{h}.bin.manifest.json")] {
        let src = work.join(&name);
        if src.exists() {
            let _ = std::fs::copy(&src, evidence_dir.join(&name));
        }
    }
    if !opts.keep_work {
        let _ = std::fs::remove_dir_all(work);
    }

    let compare_word = if compare_match { "MATCH" } else { "MISMATCH" };
    let result = json!({
        "state": "DONE",
        "stage": "done",
        "block_id_at_h": id_h,
        "snapshot_sha256": sha256,
        "state_root": state_root,
        "fingerprints_digest": fingerprints_digest,
        "compare": compare_word,
        "compare_failing_tables": failing,
        "timings_ms": Value::Object(timings),
        "source_head_at_end": head_end,
        "post_h_blocks_informational": head_end.saturating_sub(h),
        "finished_ts_ms": end,
    });
    log.record(end, "done", result.clone())?;
    log.publish(result)?;
    Ok(DrillOutcome { view: log.view.clone(), evidence_complete: true, compare_match })
}

// ----------------------------------------------------------------------------------------------- views ----

/// What the beacon publishes about the latest drill (None when this box never ran one). Only short, typed
/// values: hashes, heights, table names, millisecond timings, a sanitized error.
pub fn beacon_view(cfg: &Config) -> Option<Value> {
    let text = std::fs::read_to_string(latest_path(cfg)).ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    if v["kind"].as_str() != Some(DRILL_KIND) {
        return None;
    }
    let hex = |k: &str| v[k].as_str().filter(|s| (16..=64).contains(&s.len()) && s.chars().all(|c| c.is_ascii_hexdigit()))
        .map(str::to_ascii_lowercase);
    let tables: Vec<Value> = v["compare_failing_tables"].as_array().map(|a| {
        a.iter().filter_map(|t| t.as_str()).filter(|t| t.len() <= 48 && t.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .take(30).map(|t| json!(t)).collect()
    }).unwrap_or_default();
    let mut timings = serde_json::Map::new();
    if let Some(t) = v["timings_ms"].as_object() {
        for (k, val) in t {
            if let Some(n) = val.as_u64() {
                if k.len() <= 32 && k.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                    timings.insert(k.clone(), json!(n));
                }
            }
        }
    }
    Some(json!({
        "kind": DRILL_KIND,
        "h": v["h"].as_u64(),
        "state": v["state"].as_str().filter(|s| matches!(*s, "RUNNING" | "DONE" | "FAILED")),
        "stage": v["stage"].as_str().filter(|s| s.len() <= 24 && s.chars().all(|c| c.is_ascii_lowercase() || c == '_')),
        "block_id_at_h": hex("block_id_at_h"),
        "snapshot_sha256": hex("snapshot_sha256"),
        "state_root": hex("state_root"),
        "fingerprints_digest": hex("fingerprints_digest"),
        "compare": v["compare"].as_str().filter(|s| matches!(*s, "MATCH" | "MISMATCH")),
        "compare_failing_tables": tables,
        "timings_ms": Value::Object(timings),
        "error": v["error"].as_str().map(crate::beacon::sanitize_short),
        "finished_ts_ms": v["finished_ts_ms"].as_u64(),
    }))
}
