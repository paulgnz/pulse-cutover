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

const USAGE: &str = "\
pulse-cutover — drives an Antelope -> PulseVM cutover ceremony: same URL,
same chain_id, same state, zero read downtime. Operator walkthrough: README.md.

usage: pulse-cutover <command> [options]   (pulse-cutover <command> --help for examples)

read-only, safe anywhere (including production):
  doctor          survey this box (nodeos, nginx/haproxy, disk, ports) + per-mode verdicts
  status          how far the ceremony got, from the journal
  scan-contracts  list contracts that reference host functions PulseVM stubs (advisory)
  report          build a sanitized tar.gz to share (keys/tokens auto-redacted)
  verify          hash + dual-import fingerprint a snapshot (heavy but touches nothing)

mutating (normally driven by install.sh / cutover.sh):
  run             run the ceremony to LIVE (exit 0) or ABORTED (exit 1)
  loop            run N ceremonies back to back with a reset between (rehearsal boxes)";

const HELP_RUN: &str = "\
pulse-cutover run --config ceremony.toml

Runs the cutover ceremony described by the config, journaling every step:
ARMED -> FROZEN -> SNAPSHOTTED -> VERIFIED -> IGNITED [-> FLIPPED] -> LIVE.
Nothing user-visible changes before FLIPPED; an abort at any point is safe
and rolls back automatically. Most operators run ./cutover.sh instead, which
wraps this with preflight checks and plain-language output.

Exit codes: 0 = LIVE, 1 = did not reach LIVE (journal has the reason).
If the process dies, re-run the same command — it resumes from the journal.

EXAMPLES
  pulse-cutover run --config /etc/pulse-cutover/ceremony.toml
  ./cutover.sh --manifest ceremony.json     # the friendly wrapper";

const HELP_LOOP: &str = "\
pulse-cutover loop --config ceremony.toml --runs N

Runs N full ceremonies back to back on a rehearsal box: [loop].reset_cmd
returns both sides to a pre-arm state between runs. Failures don't stop the
loop — they are counted and categorized in the final summary. Per-run
metrics go to [loop].metrics_path (JSONL).

EXAMPLES
  pulse-cutover loop --config ceremony.toml --runs 22
  # reference reset harness: examples/loop/ in the repo";

const HELP_STATUS: &str = "\
pulse-cutover status --config ceremony.toml

Read-only. Replays the journal and prints the current state plus the pinned
facts (cut block, snapshot hash...). 'no journal' means the ceremony was
never started on this config.

EXAMPLES
  pulse-cutover status --config /etc/pulse-cutover/ceremony.toml";

const HELP_VERIFY: &str = "\
pulse-cutover verify --snapshot file.bin [--cpu-scale N]
                     [--golden roots.txt | --capture roots.txt]

Read-only for the box (CPU/RAM heavy): hashes the snapshot and imports it
TWICE through the exact code path a PulseVM node boots with, printing the
per-table state fingerprints. --golden checks them against a published
golden file (exit non-zero on any difference); --capture writes them out
for others to check against. --cpu-scale must match the ceremony's
import_cpu_scale (fingerprints depend on it).

EXAMPLES
  pulse-cutover verify --snapshot snapshot-cut.bin --cpu-scale 143 --capture roots.txt
  pulse-cutover verify --snapshot snapshot-cut.bin --cpu-scale 143 --golden golden-roots.txt";

const HELP_DOCTOR: &str = "\
pulse-cutover doctor [--json] [--mode bp|api|hyperion]

Strictly read-only survey of this box: how nodeos runs (native/docker),
what nginx and/or haproxy serve, history stack, metalgo, disk, ports. Ends with a
verdict per mode: READY, NEEDS (precise list of what's missing and how to
fix it), or UNSUPPORTED (precise reason — run 'pulse-cutover report' and
share the bundle so we can add support). Safe to run anywhere, including
production.

--json prints the machine-readable survey (schema: AGENTS.md).
--mode makes the exit code reflect that mode's verdict (0 = READY, 3 =
not ready) — this is what install.sh uses.

EXAMPLES
  pulse-cutover doctor
  pulse-cutover doctor --json | jq '.verdicts.api'";

const HELP_SCAN: &str = "\
pulse-cutover scan-contracts <snapshot.bin> [--served served.txt] [--json]

Read-only. Lists deployed contracts that reference host functions PulseVM
stubs: such a contract still loads, but traps if it ever CALLS the missing
function. Advisory, never a gate — a referenced function is not necessarily
a reachable one. The ceremony runs this automatically on the actual cut
snapshot and journals the table.

EXAMPLES
  pulse-cutover scan-contracts snapshot-cut.bin
  pulse-cutover scan-contracts snapshot-cut.bin --json | jq '.at_risk'";

const HELP_REPORT: &str = "\
pulse-cutover report [--config ceremony.toml] [--out bundle.tar.gz] [--paranoid]

Read-only survey + one sanitized tar.gz: doctor output, ceremony journal,
staged config, recent service logs. Private keys, tokens and passwords are
ALWAYS redacted ([REDACTED-<type>]); chain/block ids and hashes are kept
(they're the evidence). Prints the redaction summary and full file list so
you can review before sharing. --paranoid also placeholders hostnames/IPs.

Share the bundle (plus its printed sha256) in the testing Telegram group or
a rehearsal-feedback GitHub issue — see TESTING.md.

EXAMPLES
  pulse-cutover report
  pulse-cutover report --paranoid --out /tmp/bundle.tar.gz";

fn help_for(cmd: &str) -> Option<&'static str> {
    match cmd {
        "run" => Some(HELP_RUN),
        "loop" => Some(HELP_LOOP),
        "status" => Some(HELP_STATUS),
        "verify" => Some(HELP_VERIFY),
        "doctor" => Some(HELP_DOCTOR),
        "scan-contracts" => Some(HELP_SCAN),
        "report" => Some(HELP_REPORT),
        _ => None,
    }
}

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
    // `pulse-cutover help [command]` and `pulse-cutover <command> --help|-h`.
    if command == "help" || command == "--help" || command == "-h" {
        match args.get(1).and_then(|c| help_for(c)) {
            Some(h) => println!("{h}"),
            None => println!("{USAGE}"),
        }
        return;
    }
    if flag(&args, "--help") || flag(&args, "-h") {
        match help_for(command) {
            Some(h) => println!("{h}"),
            None => println!("{USAGE}"),
        }
        return;
    }
    let result = match command {
        "run" => cmd_run(&args),
        "loop" => cmd_loop(&args),
        "status" => cmd_status(&args),
        "verify" => cmd_verify(&args),
        "doctor" => cmd_doctor(&args),
        "scan-contracts" => cmd_scan(&args),
        "report" => cmd_report(&args),
        _ => {
            eprintln!("{USAGE}");
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
        Err(format!(
            "ceremony ended in {terminal} — it stopped safely and rolled back; the source \
             chain is still authoritative. The reason is the last 'error' line in {}. \
             'pulse-cutover report' packs it (keys redacted) for sharing.",
            cfg.journal_path.display()
        ))
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
