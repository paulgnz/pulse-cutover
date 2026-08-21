//! pulse-cutover — programmatic, zero-read-downtime Antelope -> PulseVM
//! cutover ceremony agent.
//!
//!   pulse-cutover run    --config ceremony.toml   # drive the ceremony
//!   pulse-cutover status --config ceremony.toml   # print journal state
//!   pulse-cutover verify --snapshot file.bin [--cpu-scale N]
//!                        [--golden roots.txt | --capture roots.txt]
//!   pulse-cutover doctor [--json]                 # read-only environment survey
//!   pulse-cutover scan-contracts snap.bin [--served f] [--json]
//!   pulse-cutover report [--config f] [--out f.tar.gz] [--paranoid]
//!
//! See Appendix A of wiki/59-cutover-orchestration.md for the reviewed design.

use std::path::PathBuf;

use pulse_cutover::{
    config::Config,
    doctor,
    journal::Journal,
    machine::Machine,
    ops::HttpOps,
    report,
    scan,
    state,
    verify,
};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("");
    let result = match command {
        "run" => cmd_run(&args),
        "loop" => cmd_loop(&args),
        "status" => cmd_status(&args),
        "verify" => cmd_verify(&args),
        "doctor" => cmd_doctor(&args),
        "scan-contracts" => cmd_scan(&args),
        "report" => cmd_report(&args),
        _ => {
            eprintln!(
                "usage: pulse-cutover <run|loop|status|verify> --config ceremony.toml\n       \
                 pulse-cutover loop --config ceremony.toml --runs N\n       \
                 pulse-cutover verify --snapshot file.bin [--cpu-scale N] [--golden g.txt | --capture g.txt]\n       \
                 pulse-cutover doctor [--json]\n       \
                 pulse-cutover scan-contracts snapshot.bin [--served served.txt] [--json]\n       \
                 pulse-cutover report [--config ceremony.toml] [--out bundle.tar.gz] [--paranoid]"
            );
            std::process::exit(2);
        }
    };
    if let Err(e) = result {
        eprintln!("pulse-cutover: {e}");
        std::process::exit(1);
    }
}

/// Read-only environment survey: human table by default, machine JSON with
/// --json (stdout carries ONLY the JSON so install.sh can consume it).
fn cmd_doctor(args: &[String]) -> Result<(), String> {
    let survey = doctor::survey();
    if flag(args, "--json") {
        println!(
            "{}",
            serde_json::to_string_pretty(&survey).map_err(|e| e.to_string())?
        );
    } else {
        print!("{}", doctor::render_human(&survey));
    }
    // Doctor informs; it does not gate. Exit 0 unless a requested mode's
    // verdict is UNSUPPORTED/NEEDS AND --mode was given (install.sh path).
    if let Some(mode) = arg(args, "--mode") {
        let verdict = survey
            .verdicts
            .get(&mode)
            .ok_or(format!("unknown mode {mode} (bp|api|hyperion)"))?;
        if verdict.status != "READY" {
            std::process::exit(3);
        }
    }
    Ok(())
}

/// Stubbed-intrinsic exposure scan over a portable snapshot. Advisory:
/// exit 0 even with at-risk rows (referenced != reachable).
fn cmd_scan(args: &[String]) -> Result<(), String> {
    let snapshot = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .cloned()
        .or_else(|| arg(args, "--snapshot"))
        .ok_or("usage: pulse-cutover scan-contracts <snapshot.bin> [--served f] [--json]")?;
    let served = match arg(args, "--served") {
        Some(path) => scan::parse_served(
            &std::fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?,
        ),
        None => scan::parse_served(scan::DEFAULT_SERVED),
    };
    let report = scan::scan_snapshot_path(&PathBuf::from(&snapshot), &served)?;
    if flag(args, "--json") {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
        );
    } else {
        print!("{}", scan::format_table(&report));
    }
    Ok(())
}

fn cmd_report(args: &[String]) -> Result<(), String> {
    report::run(&report::ReportOptions {
        config_path: arg(args, "--config").map(PathBuf::from),
        out: arg(args, "--out").map(PathBuf::from),
        paranoid: flag(args, "--paranoid"),
    })
}

fn load_config(args: &[String]) -> Result<Config, String> {
    let path = arg(args, "--config").ok_or("missing --config")?;
    Config::load(&PathBuf::from(path))
}

fn cmd_run(args: &[String]) -> Result<(), String> {
    let cfg = load_config(args)?;
    let ignite_cmd = cfg
        .target
        .ignite_cmd
        .clone()
        .unwrap_or_else(|| format!("systemctl restart {}", cfg.target.metalgo_unit));
    let ops = HttpOps::new(
        &cfg.source.rpc_url,
        &cfg.source.producer_api_url,
        &cfg.target.rpc_url,
        &ignite_cmd,
        cfg.source.snapshot_timeout_secs,
    );
    let (journal, recovered) = Journal::open(&cfg.journal_path)?;
    if let Some(state) = recovered.state {
        eprintln!("resuming ceremony from journaled state {state}");
    }
    let mut machine = Machine::new(&cfg, &ops, journal, recovered);
    let terminal = machine.run()?;
    println!("{terminal}");
    if terminal == state::State::Live {
        Ok(())
    } else {
        Err(format!("ceremony ended in {terminal} (see journal)"))
    }
}

fn cmd_loop(args: &[String]) -> Result<(), String> {
    let cfg = load_config(args)?;
    let runs: u32 = arg(args, "--runs")
        .ok_or("missing --runs")?
        .parse()
        .map_err(|e| format!("bad --runs: {e}"))?;
    let ignite_cmd = cfg
        .target
        .ignite_cmd
        .clone()
        .unwrap_or_else(|| format!("systemctl restart {}", cfg.target.metalgo_unit));
    let ops = HttpOps::new(
        &cfg.source.rpc_url,
        &cfg.source.producer_api_url,
        &cfg.target.rpc_url,
        &ignite_cmd,
        cfg.source.snapshot_timeout_secs,
    );
    pulse_cutover::looper::run_loop(&cfg, &ops, runs)
}

fn cmd_status(args: &[String]) -> Result<(), String> {
    let cfg = load_config(args)?;
    if !cfg.journal_path.exists() {
        println!("no journal at {} — ceremony not started", cfg.journal_path.display());
        return Ok(());
    }
    let recovered = Journal::replay(&cfg.journal_path)?;
    println!(
        "state: {}",
        recovered
            .state
            .map(|s| s.to_string())
            .unwrap_or_else(|| "(no transitions)".into())
    );
    for (k, v) in [
        ("chain_id", recovered.chain_id),
        ("cut_block_id", recovered.cut_block_id),
        ("snapshot_file", recovered.snapshot_file),
        ("sha256", recovered.sha256),
        ("last_source_block_time", recovered.last_source_block_time),
    ] {
        if let Some(v) = v {
            println!("{k}: {v}");
        }
    }
    if let Some(h) = recovered.cut_height {
        println!("cut_height: {h}");
    }
    Ok(())
}

fn cmd_verify(args: &[String]) -> Result<(), String> {
    let snapshot = PathBuf::from(arg(args, "--snapshot").ok_or("missing --snapshot")?);
    let cpu_scale: u64 = arg(args, "--cpu-scale")
        .map(|s| s.parse().map_err(|e| format!("bad --cpu-scale: {e}")))
        .transpose()?
        .unwrap_or(1);
    let outcome = verify::verify_snapshot(&snapshot, cpu_scale)?;
    println!(
        "sha256 {}\nsize {}\nchain_id {}\ncut_height {}\ncut_block_id {}\ndual_import identical",
        outcome.sha256,
        outcome.file_size,
        outcome.chain_id,
        outcome.head_block_num,
        outcome.head_block_id
    );
    for (name, root) in &outcome.roots {
        println!("{name} {root:016x}");
    }
    if let Some(golden) = arg(args, "--golden") {
        let text = std::fs::read_to_string(&golden).map_err(|e| format!("read {golden}: {e}"))?;
        verify::compare_goldens(&outcome.roots, &verify::parse_goldens(&text)?)?;
        println!("goldens: MATCH ({golden})");
    }
    if let Some(capture) = arg(args, "--capture") {
        std::fs::write(&capture, verify::format_goldens(&outcome, cpu_scale))
            .map_err(|e| format!("write {capture}: {e}"))?;
        println!("goldens: captured -> {capture}");
    }
    Ok(())
}
