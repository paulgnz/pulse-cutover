//! Repeatable-rehearsal harness: run the ceremony N times against
//! resettable source/target, collect per-run metrics (JSONL), aggregate
//! summary stats. "It worked once" -> "it works, with numbers".
//!
//! Requirements on the config:
//! - `ceremony.freeze_margin` set (H is re-derived per run from live LIB);
//! - `[loop] reset_cmd` returns BOTH sides to a pre-ARM state: source
//!   restored/serving, target chain directory fresh (an ignited PulseVM
//!   chain cannot be re-ignited), staged snapshot REMOVED (R12 — the
//!   preflight enforces this and a stale file fails the run, which is data).
//!
//! Failures do not stop the loop: each run's terminal state and abort cause
//! are recorded and categorized in the summary.

use std::io::Write as _;

use serde_json::json;

use crate::{
    config::Config,
    journal::Journal,
    machine::Machine,
    ops::ChainOps,
    state::State,
};

#[derive(Debug, Clone)]
pub struct RunMetrics {
    pub run: u32,
    pub terminal: String,
    pub abort_reason: Option<String>,
    pub resolved_h: Option<u64>,
    pub cut_height: Option<u64>,
    /// Wall-clock durations between journaled transitions, milliseconds.
    pub phase_ms: Vec<(String, u64)>,
    pub gap_ms: Option<u64>,
    pub total_ms: u64,
}

/// Extract metrics from a completed run's journal.
pub fn metrics_from_journal(path: &std::path::Path, run: u32) -> Result<RunMetrics, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read journal: {e}"))?;
    let mut transitions: Vec<(String, u64)> = Vec::new(); // (state, ts_ms)
    let mut abort_reason = None;
    let mut resolved_h = None;
    let mut cut_height = None;
    let mut gap_ms = None;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| format!("journal line: {e}"))?;
        let ts = v["ts_ms"].as_u64().unwrap_or(0);
        match v["kind"].as_str() {
            Some("transition") => {
                transitions.push((v["state"].as_str().unwrap_or("?").to_string(), ts));
            }
            Some("error") => {
                if abort_reason.is_none() {
                    abort_reason = v["data"]["message"].as_str().map(str::to_string);
                }
            }
            _ => {}
        }
        if let Some(h) = v["data"]["resolved_h"].as_u64() {
            resolved_h = Some(h);
        }
        if let Some(c) = v["data"]["cut_height"].as_u64() {
            cut_height = Some(c);
        }
        for key in ["write_gap_ms_wallclock", "ceremony_gap_ms_wallclock"] {
            if let Some(g) = v["data"][key].as_u64() {
                gap_ms = Some(g);
            }
        }
    }
    let mut phase_ms = Vec::new();
    for pair in transitions.windows(2) {
        phase_ms.push((
            format!("{}->{}", pair[0].0, pair[1].0),
            pair[1].1.saturating_sub(pair[0].1),
        ));
    }
    let terminal = transitions
        .last()
        .map(|(s, _)| s.clone())
        .unwrap_or_else(|| "NONE".into());
    let total_ms = transitions
        .last()
        .map(|l| l.1)
        .unwrap_or(0)
        .saturating_sub(transitions.first().map(|f| f.1).unwrap_or(0));
    Ok(RunMetrics {
        run,
        terminal,
        abort_reason,
        resolved_h,
        cut_height,
        phase_ms,
        gap_ms,
        total_ms,
    })
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn stats_line(label: &str, mut values: Vec<u64>) -> String {
    if values.is_empty() {
        return format!("{label:>28}: (no data)");
    }
    values.sort_unstable();
    let n = values.len() as u64;
    let mean = values.iter().sum::<u64>() / n;
    format!(
        "{label:>28}: n={n} mean={mean}ms median={}ms p95={}ms max={}ms",
        percentile(&values, 0.5),
        percentile(&values, 0.95),
        percentile(&values, 1.0),
    )
}

/// Drive `runs` ceremonies. Per-run journals land beside the configured
/// journal_path with a `-loop-N` suffix; metrics are appended to
/// `loop.metrics_path` as JSONL; a summary table is printed at the end.
pub fn run_loop<O: ChainOps>(cfg: &Config, ops: &O, runs: u32) -> Result<(), String> {
    let loop_cfg = cfg
        .r#loop
        .as_ref()
        .ok_or("loop mode requires a [loop] section (reset_cmd + metrics_path)")?;
    if cfg.ceremony.freeze_margin.is_none() {
        return Err("loop mode requires ceremony.freeze_margin (H re-derived per run)".into());
    }
    let mut all: Vec<RunMetrics> = Vec::new();
    let mut metrics_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&loop_cfg.metrics_path)
        .map_err(|e| format!("open metrics {}: {e}", loop_cfg.metrics_path.display()))?;

    for run in 1..=runs {
        eprintln!("=== loop run {run}/{runs}: reset ===");
        let reset = crate::ops::run_shell(&loop_cfg.reset_cmd);
        if let Err(e) = &reset {
            eprintln!("reset failed: {e} — recording and continuing");
            let m = RunMetrics {
                run,
                terminal: "RESET_FAILED".into(),
                abort_reason: Some(e.clone()),
                resolved_h: None,
                cut_height: None,
                phase_ms: vec![],
                gap_ms: None,
                total_ms: 0,
            };
            write_metrics(&mut metrics_file, &m)?;
            all.push(m);
            continue;
        }
        if loop_cfg.settle_secs > 0 {
            ops.sleep_ms(loop_cfg.settle_secs * 1000);
        }
        // Per-run journal: forward-only resume semantics stay intact within
        // a run while each iteration starts a FRESH ceremony.
        let mut run_cfg = cfg.clone();
        let base = cfg.journal_path.display().to_string();
        let stem = base.trim_end_matches(".jsonl");
        run_cfg.journal_path = std::path::PathBuf::from(format!("{stem}-loop-{run}.jsonl"));
        let _ = std::fs::remove_file(&run_cfg.journal_path);

        let outcome = (|| -> Result<State, String> {
            let (journal, recovered) = Journal::open(&run_cfg.journal_path)?;
            let mut machine = Machine::new(&run_cfg, ops, journal, recovered);
            machine.run()
        })();
        let mut m = metrics_from_journal(&run_cfg.journal_path, run).unwrap_or(RunMetrics {
            run,
            terminal: "NO_JOURNAL".into(),
            abort_reason: outcome.clone().err(),
            resolved_h: None,
            cut_height: None,
            phase_ms: vec![],
            gap_ms: None,
            total_ms: 0,
        });
        if let Err(e) = outcome {
            m.abort_reason.get_or_insert(e);
        }
        eprintln!(
            "=== loop run {run}/{runs}: {} gap={}ms total={}ms ===",
            m.terminal,
            m.gap_ms.unwrap_or(0),
            m.total_ms
        );
        write_metrics(&mut metrics_file, &m)?;
        all.push(m);
    }
    print_summary(&all);
    Ok(())
}

fn write_metrics(file: &mut std::fs::File, m: &RunMetrics) -> Result<(), String> {
    let phases: serde_json::Map<String, serde_json::Value> = m
        .phase_ms
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    let line = json!({
        "run": m.run,
        "terminal": m.terminal,
        "abort_reason": m.abort_reason,
        "resolved_h": m.resolved_h,
        "cut_height": m.cut_height,
        "gap_ms": m.gap_ms,
        "total_ms": m.total_ms,
        "phases_ms": phases,
    });
    writeln!(file, "{line}").map_err(|e| format!("write metrics: {e}"))?;
    file.sync_data().ok();
    Ok(())
}

pub fn print_summary(all: &[RunMetrics]) {
    let live = all.iter().filter(|m| m.terminal == "LIVE").count();
    println!("\n================ LOOP SUMMARY ================");
    println!("runs: {}   LIVE: {live}   non-LIVE: {}", all.len(), all.len() - live);
    for m in all {
        println!(
            "  run {:>3}: {:<12} gap={:>7}ms total={:>7}ms cut={} {}",
            m.run,
            m.terminal,
            m.gap_ms.map(|g| g.to_string()).unwrap_or_else(|| "-".into()),
            m.total_ms,
            m.cut_height.map(|c| c.to_string()).unwrap_or_else(|| "-".into()),
            m.abort_reason.clone().unwrap_or_default(),
        );
    }
    println!("----------------------------------------------");
    println!(
        "{}",
        stats_line(
            "ceremony gap (FROZEN->LIVE)",
            all.iter().filter_map(|m| m.gap_ms).collect()
        )
    );
    // Aggregate each phase present in any run.
    let mut phase_names: Vec<String> = Vec::new();
    for m in all {
        for (name, _) in &m.phase_ms {
            if !phase_names.contains(name) {
                phase_names.push(name.clone());
            }
        }
    }
    for name in phase_names {
        let vals: Vec<u64> = all
            .iter()
            .flat_map(|m| {
                m.phase_ms
                    .iter()
                    .filter(|(n, _)| n == &name)
                    .map(|(_, v)| *v)
            })
            .collect();
        println!("{}", stats_line(&name, vals));
    }
    // Failure taxonomy.
    let mut causes: Vec<(String, usize)> = Vec::new();
    for m in all.iter().filter(|m| m.terminal != "LIVE") {
        let cause = m.abort_reason.clone().unwrap_or_else(|| m.terminal.clone());
        match causes.iter_mut().find(|(c, _)| *c == cause) {
            Some((_, n)) => *n += 1,
            None => causes.push((cause, 1)),
        }
    }
    if !causes.is_empty() {
        println!("failures by cause:");
        for (cause, n) in causes {
            println!("  {n}x {cause}");
        }
    }
    println!("==============================================");
}
