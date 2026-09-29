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
  beacon          report readiness + ceremony evidence to mission control
  await           arm from a signed coordinator event (same H on every producer)
  verify          hash + dual-import fingerprint a snapshot (heavy but touches nothing)

mutating (normally driven by install.sh / cutover.sh):
  run             run the ceremony to LIVE (exit 0), ABORTED or HALTED (exit 1)
  loop            run N ceremonies back to back with a reset between (rehearsal boxes)
  unhalt          clear a HALTED (sealed) journal after a fleet-wide decision (--i-understand)
  rollback        roll this node back, only if ignition provably has not started (what cutover.sh abort runs)";

const HELP_RUN: &str = "\
pulse-cutover run --config ceremony.toml

Runs the cutover ceremony described by the config, journaling every step:
ARMED -> FROZEN -> SNAPSHOTTED -> VERIFIED -> IGNITED [-> FLIPPED] -> LIVE.
Before ignition starts, a failure ABORTS and rolls this node back. Once ignition
has started (journaled before the ignite command runs), a failure HALTS: nothing
is rolled back and the journal is sealed until an operator runs `unhalt`. Most operators run ./cutover.sh instead, which
wraps this with preflight checks and plain-language output.

Exit codes: 0 = LIVE, 1 = did not reach LIVE (journal has the reason).
If the process dies, re-run the same command — it resumes from the journal.

EXAMPLES
  pulse-cutover run --config /etc/pulse-cutover/ceremony.toml
  ./cutover.sh --manifest ceremony.json     # the friendly wrapper";

const HELP_UNHALT: &str = "\
pulse-cutover unhalt --config ceremony.toml --i-understand

A HALTED journal is sealed: ignition may have started on this node, so the agent
refused to continue or roll back on its own. After the coordinator has made the
fleet-wide decision, this clears the seal: it journals who/when and returns the
journal to the state it halted from, so the next `run` resumes from there (a
resumed run with ignition started halts again unless the cause is fixed).
It never resumes the source chain or reverts routing itself.";

const HELP_ROLLBACK: &str = "\
pulse-cutover rollback --config ceremony.toml [--wait SECS] [--force-after-ignite] [--no-journal-i-know]
pulse-cutover rollback --config ceremony.toml --cancel-intent --i-understand

What `./cutover.sh abort` runs. It takes the journal's exclusive lock (waiting up to --wait
seconds, default 30, for a stopping agent to let go; it never decides while another process
holds the journal), replays it, and rolls this node back (resume the paused source producer;
api mode: revert the flips, restart the source if the agent stopped it) ONLY when the journal
positively shows ignition has not started.

Exit codes:
  0  rolled back (every step succeeded), or already rolled back: the journal proves every step
     completed, on_abort and the unstage included, so a repeat does nothing again (it still stops
     an orphaned hook and moves a late staged file aside)
  3  REFUSED, nothing changed: the config cannot be loaded; there is no journal (absence is not
     proof: pass --no-journal-i-know only on a box that never ran a ceremony); the journal is
     locked past --wait, corrupt or unreadable; or ignition may have started (ignite_started
     journaled, IGNITED or later, HALTED) and --force-after-ignite was not given
  4  INCOMPLETE: a rollback step failed (resume, flip revert, source restart, on_abort, unstage,
     or the target fence of a forced rollback); the failed steps are printed. The source may NOT
     be producing: check by hand. Re-running redoes only the steps not yet journaled as done.
  An orphaned hook that cannot be stopped is a refusal (3): nothing is rolled back.

Before anything else it kills a hook left running by a killed agent. --force-after-ignite first
FENCES this box's target (target.stop_cmd, default `systemctl stop <unit> && ! systemctl
is-active --quiet <unit>`); if that fails the source is not resumed (exit 4). This is a local
fence only: other producers' targets are not affected. A staged snapshot is moved aside
(<staged>.rolled-back-<ms>) on every path (the agent's own abort moves the one it staged). Each step
is journaled as it completes (`rollback_step`); the rollback counts as finished only when a final
`rollback_done` record follows on_abort and the unstage. Every rollback is journaled (ABORTED, `operator_rollback`,
`force_after_ignite`); with --no-journal-i-know the record goes to <journal>.rollback-<ms>.jsonl
and the ceremony journal is not created.

A rollback records its intent (`rollback_requested`) before doing anything. If it dies before
reaching ABORTED, `run` refuses to continue (`status` shows `rollback_pending: yes`); re-run
`rollback` to finish it. `unhalt` does not clear it. Only if NO rollback step has completed yet,
`--cancel-intent --i-understand` withdraws it (journaled with who ran it) so `run` may continue.
The forced target fence runs on every forced attempt (never reused from an earlier attempt).";

const HELP_BEACON: &str = "\
pulse-cutover beacon --config ceremony.toml [--once]

Reports this node's readiness and ceremony evidence to mission control (the
[beacon] section: url, producer, network, token_file, interval_secs). Every
interval it checks the source API, producer API, chain_id, H, freeze lead,
hooks (exist + executable), staged snapshot absent, validator running and
disk, then summarizes the journal (state, cut id, snapshot sha256,
fingerprint digest, state-diff digest) and POSTs one JSON report.
Read-only on the box; runs beside the ceremony, never inside it.

EXAMPLES
  pulse-cutover beacon --config /etc/pulse-cutover/ceremony.toml
  pulse-cutover beacon --config ceremony.toml --once     # print one report";

const HELP_AWAIT: &str = "\
pulse-cutover await --config ceremony.toml

Coordinated arming. Waits for the coordinator's SIGNED event (network, chain_id, H,
freeze lead, cpu scale) relayed by mission control ([coordination] url/network),
verifies it against [coordination] coordinator_keys and this node's own config and
head, then waits for a signed ARM. With auto_arm = false (default) the operator must
also `touch <journal dir>/confirm-<event_id>`. It then runs the ceremony at exactly
that H. A signed ABORT is honoured before arming and, during the ceremony, up to
VERIFIED (never after IGNITED).

EXAMPLES
  pulse-cutover await --config /etc/pulse-cutover/ceremony.toml";

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
        "beacon" => Some(HELP_BEACON),
        "await" => Some(HELP_AWAIT),
        "unhalt" => Some(HELP_UNHALT),
        "rollback" => Some(HELP_ROLLBACK),
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
        "beacon" => cmd_beacon(&args),
        "await" => cmd_await(&args),
        "unhalt" => cmd_unhalt(&args),
        "rollback" => cmd_rollback(&args),
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
    cfg.ensure_ceremony_profile()?;
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
    )
    .with_hook_timeout(cfg.hooks.timeout_secs)
    .with_pgid_file_for(&cfg.journal_path);
    let (journal, recovered) = Journal::open(&cfg.journal_path)?;
    if let Some(state) = recovered.state {
        eprintln!("resuming ceremony from journaled state {state}");
    }
    let mut machine = Machine::new(&cfg, &ops, journal, recovered);
    let terminal = match machine.run() {
        Ok(t) => t,
        Err(e) if e.starts_with("HALTED") || e.contains("HALTED (journaled)") => {
            return Err(format!(
                "{e}\nThe ceremony is SEALED: this node's target was already ignited, so the source was \
                 NOT resumed and writes were NOT re-opened (that could create a second writable \
                 history). A human decides what happens next; the journal {} has the evidence.",
                cfg.journal_path.display()
            ))
        }
        Err(e) => return Err(e),
    };
    println!("{terminal}");
    if terminal == state::State::Aborted {
        // Only a rollback the journal PROVES finished (`rollback_done`) may be called safe.
        let rec = Journal::replay(&cfg.journal_path)?;
        if !rec.aborted_rollback_complete {
            eprintln!(
                "ceremony ended in ABORTED but rollback INCOMPLETE: the journal does not prove every rollback \
                 step finished (the source may NOT be producing, writes may still be closed). Run \
                 `pulse-cutover rollback --config …` to finish it. Journal: {}",
                cfg.journal_path.display()
            );
            std::process::exit(4);
        }
    }
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
    cfg.ensure_ceremony_profile()?;
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
    )
    .with_hook_timeout(cfg.hooks.timeout_secs)
    .with_pgid_file_for(&cfg.journal_path);
    pulse_cutover::looper::run_loop(&cfg, &ops, runs)
}

fn cmd_beacon(args: &[String]) -> Result<(), String> {
    let path = arg(args, "--config").ok_or("--config <ceremony.toml> is required")?;
    let cfg = Config::load(&PathBuf::from(path))?;
    pulse_cutover::beacon::run(&cfg, flag(args, "--once"))
}

fn cmd_await(args: &[String]) -> Result<(), String> {
    let path = PathBuf::from(arg(args, "--config").ok_or("--config <ceremony.toml> is required")?);
    let cfg = Config::load(&path)?;
    cfg.ensure_ceremony_profile()?;
    let code = pulse_cutover::coord::run_await(&cfg, &path)?;
    std::process::exit(code);
}

fn cmd_unhalt(args: &[String]) -> Result<(), String> {
    if !flag(args, "--i-understand") {
        return Err("unhalt clears a SEALED ceremony (ignition may have started). Only after a \
                    fleet-wide decision: re-run with --i-understand"
            .into());
    }
    let cfg = load_config(args)?;
    cfg.ensure_ceremony_profile()?;
    let (mut journal, recovered) = Journal::open(&cfg.journal_path)?;
    if recovered.state != Some(state::State::Halted) {
        return Err(format!("journal state is {:?}, not HALTED: nothing to clear", recovered.state.map(|s| s.to_string())));
    }
    let back = recovered.halted_from.unwrap_or(state::State::Verified);
    let who = std::env::var("SUDO_USER").or_else(|_| std::env::var("USER")).unwrap_or_else(|_| "unknown".into());
    journal.evidence(state::State::Halted, serde_json::json!({"unhalted_by": who, "returning_to": back.as_str()}))?;
    // Carry the established evidence forward on the transition, so nothing that reads "the
    // latest <state> record" (beacon summary, fleet gate) sees it as erased.
    let mut data = serde_json::json!({"unhalted": true, "by": who,
        "note": "seal cleared by operator; ignition-started is still recorded, so a failure halts again"});
    for (k, v) in [("sha256", &recovered.sha256), ("chain_id", &recovered.chain_id), ("cut_block_id", &recovered.cut_block_id),
                   ("snapshot_file", &recovered.snapshot_file)] {
        if let Some(v) = v {
            data[k] = serde_json::json!(v);
        }
    }
    if let Some(h) = recovered.cut_height {
        data["cut_height"] = serde_json::json!(h);
    }
    journal.transition(back, data)?;
    println!("unhalted: journal returned to {back}");
    Ok(())
}

/// Exit codes: 0 = rolled back (or already rolled back); 3 = REFUSED, nothing changed;
/// 4 = rollback attempted but INCOMPLETE (some step failed: the source may NOT be producing).
fn cmd_rollback(args: &[String]) -> Result<(), String> {
    let refuse = |msg: String| -> ! {
        eprintln!("pulse-cutover rollback: REFUSED — {msg}");
        eprintln!("Nothing was changed: the source was NOT resumed and public routing was NOT reverted.");
        std::process::exit(3);
    };
    let incomplete = |failed: &[String]| -> ! {
        eprintln!("pulse-cutover rollback: INCOMPLETE — {} step(s) failed:", failed.len());
        for f in failed {
            eprintln!("  - {f}");
        }
        eprintln!("The source may NOT be producing and routing may NOT be reverted: check by hand.");
        std::process::exit(4);
    };
    // Every failure before any rollback action runs is a refusal (exit 3): nothing changed.
    let cfg = load_config(args).unwrap_or_else(|e| refuse(format!("cannot load the config: {e}")));
    cfg.ensure_ceremony_profile().unwrap_or_else(|e| refuse(e));
    if flag(args, "--cancel-intent") {
        return cancel_rollback_intent(&cfg, flag(args, "--i-understand")).map_err(|e| refuse(e));
    }
    let force = flag(args, "--force-after-ignite");
    let wait: u64 = match arg(args, "--wait").map(|s| s.parse::<u64>()) {
        None => 30,
        Some(Ok(w)) => w,
        Some(Err(e)) => refuse(format!("bad --wait: {e}")),
    };
    // --no-journal-i-know on a box without a journal: do the reverts, but never create the
    // ceremony journal (a terminal ABORTED there would make the next `run` a no-op). The record
    // goes to a separate audit file instead.
    let mut journal_path = cfg.journal_path.clone();
    let mut audit_only = false;
    if !cfg.journal_path.exists() {
        if !flag(args, "--no-journal-i-know") {
            refuse(format!(
                "no journal at {}. A missing journal is not proof that ignition never started (wrong path, \
                 deleted, unreadable). If this box never ran a ceremony, re-run with --no-journal-i-know.",
                cfg.journal_path.display()));
        }
        let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        let mut p = cfg.journal_path.as_os_str().to_owned();
        p.push(format!(".rollback-{ms}.jsonl"));
        journal_path = std::path::PathBuf::from(p);
        audit_only = true;
    }
    // Hold the journal's exclusive lock while deciding: a still-running agent must not race us.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait);
    let (journal, recovered) = loop {
        match Journal::open(&journal_path) {
            Ok(x) => break x,
            Err(e) if e.contains("holds") && std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
            Err(e) => refuse(format!("cannot take and read the journal to decide safely: {e}")),
        }
    };
    let ignite_cmd = cfg.target.ignite_cmd.clone().unwrap_or_else(|| format!("systemctl restart {}", cfg.target.metalgo_unit));
    let ops = HttpOps::new(&cfg.source.rpc_url, &cfg.source.producer_api_url, &cfg.target.rpc_url, &ignite_cmd,
        cfg.source.snapshot_timeout_secs).with_hook_timeout(cfg.hooks.timeout_secs)
        .with_pgid_file_for(&cfg.journal_path);
    let mut machine = Machine::new(&cfg, &ops, journal, recovered);
    let result = machine.operator_rollback(force);
    drop(machine); // releases the journal lock
    if audit_only {
        // The audit record's lock file is not needed once the record is written: leave no litter.
        let _ = std::fs::remove_file(format!("{}.lock", journal_path.display()));
    }
    match result {
        Ok(out) => {
            for n in &out.notes {
                println!("  {n}");
            }
            if !out.failed.is_empty() {
                incomplete(&out.failed);
            }
            if out.already {
                println!("already rolled back: journal {} (nothing done again)", cfg.journal_path.display());
            } else if audit_only {
                println!("rolled back (no ceremony journal existed; audit record: {})", journal_path.display());
            } else {
                println!("rolled back: journal now {} (see {})", out.state, cfg.journal_path.display());
            }
            Ok(())
        }
        Err(e) if e.starts_with("refusing") => refuse(e),
        // A failure after rollback actions may have started (e.g. the journal could not be written).
        Err(e) => incomplete(&[e]),
    }
}

/// `rollback --cancel-intent --i-understand`: withdraw a recorded operator rollback intent so `run`
/// may continue, ONLY while no rollback step has completed in this episode (after a step ran, the
/// only way forward is to finish the rollback). Journaled with who and when.
fn cancel_rollback_intent(cfg: &Config, understood: bool) -> Result<(), String> {
    if !understood {
        return Err("--cancel-intent withdraws a recorded rollback so the ceremony may continue: pass \
                    --i-understand to confirm".into());
    }
    let (mut journal, rec) = Journal::open(&cfg.journal_path)
        .map_err(|e| format!("cannot take and read the journal: {e}"))?;
    if !rec.rollback_pending {
        return Err("no pending rollback intent in this journal: nothing to cancel".into());
    }
    if !rec.rollback_steps_done.is_empty() {
        return Err(format!(
            "rollback step(s) already completed ({}): the rollback cannot be cancelled, only finished \
             (re-run `pulse-cutover rollback`)",
            rec.rollback_steps_done.join(", ")
        ));
    }
    let by = std::env::var("SUDO_USER").or_else(|_| std::env::var("USER")).unwrap_or_else(|_| "unknown".into());
    let st = rec.state.unwrap_or(state::State::Armed);
    journal.evidence(st, serde_json::json!({"rollback_intent_cancelled": true, "cancelled_by": by}))?;
    println!("rollback intent cancelled (journaled, by {by}); `run` may continue from state {st}");
    Ok(())
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
    // cutover.sh reads this: once ignition may have started, a local rollback is refused.
    println!("ignition_started: {}", if recovered.reached_ignited { "yes" } else { "no" });
    // An operator rollback that started and did not finish blocks `run` until it is finished
    // (re-run `rollback`) or, if no step ran yet, cancelled (`rollback --cancel-intent`).
    let rollback = if recovered.rollback_pending {
        "pending (an operator rollback started and did not finish: re-run `pulse-cutover rollback`)"
    } else if recovered.state == Some(state::State::Aborted) {
        if recovered.aborted_rollback_complete { "complete" } else { "INCOMPLETE (run `pulse-cutover rollback`)" }
    } else {
        "none"
    };
    println!("rollback_pending: {}", if recovered.rollback_pending { "yes" } else { "no" });
    println!("rollback: {rollback}");
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
