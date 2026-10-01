//! Side effects behind a trait so the state machine is unit-testable.
//!
//! `HttpOps` is the real implementation: nodeos chain/producer APIs on the
//! source side, pulsevm JSON-RPC on the target side, systemctl/shell for
//! ignition and hooks.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{
    Value,
    json,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ChainInfo {
    pub chain_id: String,
    pub head_block_num: u64,
    pub head_block_id: String,
    pub head_block_time: String,
    pub last_irreversible_block_num: u64,
}

#[derive(Debug, Clone)]
pub struct SnapshotResult {
    /// Path as reported by nodeos (its own filesystem namespace).
    pub snapshot_name: String,
    pub head_block_num: u64,
    pub head_block_id: String,
}

pub trait ChainOps {
    fn source_info(&self) -> Result<ChainInfo, String>;
    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String>; // (id, timestamp)
    /// Number of transactions in a source block (burn-off audit: blocks
    /// between the cut and the pause must be empty once writes are frozen).
    fn source_block_tx_count(&self, block_num: u64) -> Result<u64, String>;
    fn producer_paused(&self) -> Result<bool, String>;
    fn pause(&self) -> Result<(), String>;
    fn resume(&self) -> Result<(), String>;
    fn create_snapshot(&self) -> Result<SnapshotResult, String>;
    /// Best-effort schedule_snapshot at a specific height (Leap 4+ snapshot
    /// scheduler). Err if unsupported — callers fall back to create_snapshot.
    fn schedule_snapshot(&self, height: u64) -> Result<(), String>;
    /// getInfo against the target pulsevm chain; Ok(None) while unreachable
    /// (still bootstrapping / chain not yet initialized).
    fn target_info(&self) -> Result<Option<ChainInfo>, String>;
    /// get_info via the PUBLIC /v1 endpoint (api mode: through nginx + the
    /// REST gateway). Ok(None) while unreachable/mid-reload — the flip
    /// health check treats that as a transient, bounded by its timeout.
    fn public_info(&self, public_url: &str) -> Result<Option<ChainInfo>, String>;
    /// The target chain's block id at `height` (lineage check at the cut). Ok(None) when the
    /// target cannot show that block (not implemented / pruned / still booting).
    fn target_block_id(&self, _height: u64) -> Result<Option<String>, String> {
        Ok(None)
    }
    /// The FULL source block at `block_num` as nodeos `get_block` JSON (all receipts), from
    /// `rpc_url` (a nodeos /v1 base) or, when None, the source RPC. Upstream ignition packs it
    /// into the boot manifest's anchor.
    fn source_block(&self, block_num: u64, _rpc_url: Option<&str>) -> Result<Value, String> {
        Err(format!("source_block({block_num}) is not supported by this ChainOps"))
    }
    /// Bind values for `{name}` placeholders in the target RPC URL and the ignite command (the
    /// upstream backend learns `blockchain_id` only when it creates the chain). Default: no-op.
    fn bind_target(&self, _vars: &[(String, String)]) {}
    fn ignite(&self) -> Result<String, String>;
    /// Hooks and operator commands: killed after the configured hook timeout.
    fn run_hook(&self, cmd: &str) -> Result<String, String>;
    /// Kill a hook process group left running by an agent that died mid-hook (recorded while
    /// hooks run). Ok(Some(desc)) = killed one; Ok(None) = nothing to do. Default: nothing.
    fn kill_orphan_hooks(&self) -> Result<Option<String>, String> {
        Ok(None)
    }
    /// Long-running pipeline steps (upstream export/import over a mainnet snapshot take far
    /// longer than any hook timeout). Default: same as run_hook.
    fn run_long(&self, cmd: &str) -> Result<String, String> {
        self.run_hook(cmd)
    }
    /// GET a JSON document (hyperion /v2/health, local or public).
    /// Ok(None) while unreachable / non-JSON — health gates treat that as a
    /// transient bounded by their own timeout.
    fn get_json(&self, url: &str) -> Result<Option<Value>, String>;
    fn now_ms(&self) -> u64;
    fn sleep_ms(&self, ms: u64);
}

/// Captured output is capped per stream: the first and last `OUTPUT_CAP / 2` bytes are kept.
pub const OUTPUT_CAP: usize = 64 * 1024;
/// Deadline for long pipeline steps (`run_long`): effectively none.
pub const LONG_STEP_DEADLINE: Duration = Duration::from_secs(30 * 24 * 3600);
/// After the shell itself exits, how long its output pipes may stay open (a background
/// descendant holding them) before the process group is killed.
pub const PIPE_GRACE: Duration = Duration::from_secs(5);

/// Bounded capture of one pipe: head + tail, with a count of what was dropped in between.
fn capture(mut r: impl std::io::Read) -> Vec<u8> {
    let half = OUTPUT_CAP / 2;
    let (mut head, mut tail) = (Vec::new(), std::collections::VecDeque::new());
    let mut dropped = 0usize;
    let mut buf = [0u8; 8192];
    loop {
        match r.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                for &b in &buf[..n] {
                    if head.len() < half {
                        head.push(b);
                    } else {
                        tail.push_back(b);
                        if tail.len() > half {
                            tail.pop_front();
                            dropped += 1;
                        }
                    }
                }
            }
        }
    }
    if dropped > 0 {
        head.extend_from_slice(format!("\n…[{dropped} bytes of output dropped]…\n").as_bytes());
    }
    head.extend(tail);
    head
}

/// Send `sig` to every process in process group `pgid`, via the kernel directly.
///
/// Never shell out to `kill -SIG -<pgid>`: procps `kill` parses `-<pgid>` as more options, which on
/// Ubuntu 24.04 became kill(-<first digit>) (SIGTERM to every process on the box) and on 22.04
/// kill(0) (the agent killed itself). Found by the Linux fault-injection run, 2026-09-30.
/// pgid 0 and 1 are refused: kill(0, …) is our own group and kill(-1, …) is every process.
#[cfg(unix)]
pub(crate) fn signal_group(pgid: u32, sig: i32) -> bool {
    if pgid <= 1 || pgid > i32::MAX as u32 {
        return false;
    }
    // SAFETY: plain syscall; a negative pid addresses exactly one process group.
    unsafe { libc::kill(-(pgid as i32), sig) == 0 }
}

#[cfg(unix)]
fn kill_group(pid: u32) {
    let _ = signal_group(pid, libc::SIGKILL);
}
#[cfg(not(unix))]
fn kill_group(_pid: u32) {}

/// Process ids currently in process group `pgid` (empty if none / cannot tell).
#[cfg(unix)]
fn group_members(pgid: u32) -> Vec<u32> {
    let out = match std::process::Command::new("ps").args(["-A", "-o", "pid=,pgid=,stat="]).output() {
        Ok(o) => o,
        Err(_) => return vec![],
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let pid: u32 = it.next()?.parse().ok()?;
            let g: u32 = it.next()?.parse().ok()?;
            // A zombie is already dead (only its exit status is left): it is not a survivor.
            let zombie = it.next().is_some_and(|st| st.starts_with('Z'));
            (g == pgid && !zombie).then_some(pid)
        })
        .collect()
}

/// The pgid recorded in `pgid_file` if that group still has live members.
#[cfg(not(unix))]
fn recorded_live_group(_pgid_file: &Path) -> Option<u32> {
    None
}
#[cfg(unix)]
fn recorded_live_group(pgid_file: &Path) -> Option<u32> {
    let text = std::fs::read_to_string(pgid_file).ok()?;
    let pgid: u32 = text.lines().next()?.trim().parse().ok()?;
    (pgid > 1 && !group_members(pgid).is_empty()).then_some(pgid)
}

/// A process's start time as `ps -o lstart=` prints it (same on Linux procps and macOS), used
/// to tell a recorded hook group leader from an unrelated process that later got the same pid.
pub fn process_start(pid: u32) -> Option<String> {
    let o = std::process::Command::new("ps").args(["-o", "lstart=", "-p", &pid.to_string()]).output().ok()?;
    let s = String::from_utf8_lossy(&o.stdout).split_whitespace().collect::<Vec<_>>().join(" ");
    (!s.is_empty()).then_some(s)
}

/// Kill a hook process group recorded by `run_shell_timeout_tracked` (an agent that died
/// mid-hook leaves it running): SIGTERM the group, wait up to `grace`, then SIGKILL. Returns
/// Ok(None) when nothing was recorded or the group is already gone, Ok(Some(description)) when
/// a group was killed, Err when it could not be killed. The record is removed afterwards.
#[cfg(unix)]
pub fn kill_recorded_hook_group(pgid_file: &Path, grace: Duration) -> Result<Option<String>, String> {
    let text = match std::fs::read_to_string(pgid_file) {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    let mut lines = text.lines();
    let pgid: u32 = match lines.next().and_then(|l| l.trim().parse().ok()) {
        Some(p) if p > 1 => p,
        _ => {
            let _ = std::fs::remove_file(pgid_file);
            return Ok(None);
        }
    };
    // Record format: pgid, then (rc.8+) `start=<ps lstart of the leader>`, then the command.
    let rest: Vec<&str> = lines.collect();
    let (start, cmd) = match rest.first() {
        Some(l) if l.starts_with("start=") => (Some(l.trim_start_matches("start=").trim().to_string()), rest[1..].join("\n")),
        _ => (None, rest.join("\n")),
    };
    if group_members(pgid).is_empty() {
        let _ = std::fs::remove_file(pgid_file);
        return Ok(None);
    }
    // Guard against a recycled pid. While any member of the group lives, the kernel does not hand
    // the group id out again, so members without a living leader are still ours. A living leader
    // must be the process we recorded: same start time (rc.8+ records carry it). Older records
    // fall back to the command-name check (weaker: any `sh -c …` leader passed it).
    if let Ok(o) = std::process::Command::new("ps").args(["-o", "command=", "-p", &pgid.to_string()]).output() {
        let leader = String::from_utf8_lossy(&o.stdout).trim().to_string();
        if !leader.is_empty() {
            let ours = match &start {
                // The leader can exit between the two `ps` calls: then only members remain, and
                // they are still ours (the group id is not reused while any member lives).
                Some(want) => match process_start(pgid) {
                    None => true,
                    Some(now) => now == *want,
                },
                None => {
                    let base = |w: &str| w.rsplit('/').next().unwrap_or(w).to_string();
                    let leader_prog = leader.split_whitespace().next().map(base).unwrap_or_default();
                    // rc.7 records: `sh -c <hook>` usually execs a simple hook in place, so the
                    // leader shows the hook's own program: compare against that, not `sh`.
                    let hook = cmd.trim_start().strip_prefix("sh -c ").unwrap_or(&cmd);
                    let cmd_prog = hook.split_whitespace().next().map(base).unwrap_or_default();
                    // A script hook exec'd in place shows as `<interpreter> <script>`: accept the
                    // hook's program as the first OR second word of the leader's command line.
                    let second = leader.split_whitespace().nth(1).map(base).unwrap_or_default();
                    let leader_prog = if !cmd_prog.is_empty() && second == cmd_prog { second } else { leader_prog };
                    leader.starts_with("sh -c") || (!cmd_prog.is_empty() && leader_prog == cmd_prog)
                }
            };
            if !ours {
                let _ = std::fs::remove_file(pgid_file);
                return Err(format!(
                    "recorded hook process group {pgid} now belongs to `{leader}` (a different process: pid \
                     reused?): not killed. The record was removed; re-running proceeds without it."
                ));
            }
        }
    }
    let _ = signal_group(pgid, libc::SIGTERM);
    let start = std::time::Instant::now();
    while start.elapsed() < grace && !group_members(pgid).is_empty() {
        std::thread::sleep(Duration::from_millis(100));
    }
    let mut how = "SIGTERM";
    if !group_members(pgid).is_empty() {
        kill_group(pgid);
        how = "SIGKILL";
        std::thread::sleep(Duration::from_millis(200));
    }
    let _ = std::fs::remove_file(pgid_file);
    if group_members(pgid).is_empty() {
        Ok(Some(format!("killed orphaned hook process group {pgid} ({how}): `{cmd}`")))
    } else {
        Err(format!("orphaned hook process group {pgid} (`{cmd}`) survived SIGKILL"))
    }
}
#[cfg(not(unix))]
pub fn kill_recorded_hook_group(_pgid_file: &Path, _grace: Duration) -> Result<Option<String>, String> {
    Ok(None)
}

/// Run a shell command with a deadline. The command runs in its own process group so a
/// timeout kills the whole tree (sh + whatever it started), and a timeout is a FAILURE.
/// Completion is bounded too: after the shell exits, output pipes still held open by a
/// descendant get `PIPE_GRACE`, then the group is killed. Output is capped (`OUTPUT_CAP`).
/// Hooks that must leave work running should detach it (`setsid … >log 2>&1 &`).
/// `<journal>.hook.pgid`: the process group of the hook currently running for this journal.
pub fn hook_pgid_path(journal_path: &Path) -> PathBuf {
    let mut s = journal_path.as_os_str().to_owned();
    s.push(".hook.pgid");
    PathBuf::from(s)
}

pub fn run_shell_timeout(cmd: &str, timeout: Duration) -> Result<String, String> {
    run_shell_timeout_tracked(cmd, timeout, None)
}

/// Removes the recorded hook process-group file when the hook finishes (any path out).
struct PgidRecord(Option<PathBuf>);
impl Drop for PgidRecord {
    fn drop(&mut self) {
        if let Some(p) = &self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// `run_shell_timeout`, additionally recording the hook's process-group id in `pgid_file` while
/// it runs (removed on completion). If the agent is killed mid-hook (`cutover.sh abort`
/// SIGTERMs it; hooks live in their own process group and survive), `rollback` reads this file
/// and kills the orphaned group before it runs `on_abort` or any revert.
pub fn run_shell_timeout_tracked(cmd: &str, timeout: Duration, pgid_file: Option<&Path>) -> Result<String, String> {
    #[cfg(unix)]
    use std::os::unix::process::CommandExt;
    use std::sync::mpsc;
    let mut command = std::process::Command::new("sh");
    command
        .arg("-c")
        .arg(cmd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    // Never overwrite (and later delete) the record of a group that is still running: that is a
    // hook left by a dead agent, and its record is the only way to find and stop it.
    if let Some(p) = pgid_file {
        if let Some(live) = recorded_live_group(p) {
            return Err(format!(
                "refusing to start `{cmd}`: process group {live} recorded in {} is still running (left by \
                 a previous agent). Stop it first (`pulse-cutover rollback` or re-running does).",
                p.display()
            ));
        }
    }
    let mut child = command.spawn().map_err(|e| format!("spawn `{cmd}`: {e}"))?;
    let pid = child.id();
    let _record = PgidRecord(pgid_file.and_then(|p| {
        // pgid (= the shell's pid: process_group(0)), the leader's start time (recycled-pid guard)
        // and the command, for the operator.
        let start = process_start(pid).map(|s| format!("start={s}\n")).unwrap_or_default();
        std::fs::write(p, format!("{pid}\n{start}{cmd}\n")).ok().map(|_| p.to_path_buf())
    }));
    // Drain pipes on threads (a chatty command can't block on a full pipe); results arrive on a
    // channel so waiting for them can be bounded instead of an unconditional join.
    let so = child.stdout.take().expect("piped");
    let se = child.stderr.take().expect("piped");
    let (tx_out, rx_out) = mpsc::channel();
    let (tx_err, rx_err) = mpsc::channel();
    std::thread::spawn(move || { let _ = tx_out.send(capture(so)); });
    std::thread::spawn(move || { let _ = tx_err.send(capture(se)); });
    let started = std::time::Instant::now();
    let status = loop {
        match child.try_wait().map_err(|e| format!("wait `{cmd}`: {e}"))? {
            Some(st) => break Some(st),
            None if started.elapsed() >= timeout => break None,
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    let Some(status) = status else {
        kill_group(pid);
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("`{cmd}` timed out after {}s and was killed", timeout.as_secs()));
    };
    // The shell exited. Its pipes close unless a descendant still holds them.
    let deadline = std::time::Instant::now() + PIPE_GRACE;
    let recv = |rx: &mpsc::Receiver<Vec<u8>>| rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()));
    let (mut out, mut err) = (recv(&rx_out).ok(), recv(&rx_err).ok());
    let mut note = String::new();
    if out.is_none() || err.is_none() {
        kill_group(pid);
        note = format!(" [descendant processes still held the output after {}s and were killed]", PIPE_GRACE.as_secs());
        let grace2 = Duration::from_secs(1);
        if out.is_none() { out = rx_out.recv_timeout(grace2).ok(); }
        if err.is_none() { err = rx_err.recv_timeout(grace2).ok(); }
    }
    let stdout = String::from_utf8_lossy(&out.unwrap_or_default()).trim().to_string();
    let stderr = String::from_utf8_lossy(&err.unwrap_or_default()).trim().to_string();
    if status.success() {
        Ok(format!("{}{note}", if stdout.is_empty() { stderr } else { stdout }))
    } else {
        Err(format!("`{cmd}` exited {status}: {stderr} {stdout}{note}"))
    }
}

/// Run a shell command, returning trimmed stdout (or stderr if stdout is
/// empty) on success. No deadline: loop-harness resets and long pipeline steps.
pub fn run_shell(cmd: &str) -> Result<String, String> {
    let out = std::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .output()
        .map_err(|e| format!("spawn `{cmd}`: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if out.status.success() {
        Ok(if stdout.is_empty() { stderr } else { stdout })
    } else {
        Err(format!("`{cmd}` exited {}: {stderr} {stdout}", out.status))
    }
}

/// Expand `{name}` placeholders from `vars` (unknown placeholders are left as they are).
pub fn expand_placeholders(template: &str, vars: &[(String, String)]) -> String {
    vars.iter().fold(template.to_string(), |acc, (k, v)| acc.replace(&format!("{{{k}}}"), v))
}

pub struct HttpOps {
    /// Values for `{name}` placeholders in `target_rpc` / `ignite_cmd` (see `bind_target`).
    pub target_vars: std::sync::Mutex<Vec<(String, String)>>,
    pub source_rpc: String,
    pub producer_api: String,
    pub target_rpc: String,
    pub ignite_cmd: String,
    pub snapshot_timeout: Duration,
    /// Deadline for hooks / flip / stop / start / ignite commands.
    pub hook_timeout: Duration,
    /// Where the running hook's process-group id is recorded (`<journal>.hook.pgid`).
    pub pgid_file: Option<PathBuf>,
    agent: ureq::Agent,
}

impl HttpOps {
    pub fn new(
        source_rpc: &str,
        producer_api: &str,
        target_rpc: &str,
        ignite_cmd: &str,
        snapshot_timeout_secs: u64,
    ) -> Self {
        HttpOps {
            target_vars: std::sync::Mutex::new(vec![]),
            source_rpc: source_rpc.trim_end_matches('/').to_string(),
            producer_api: producer_api.trim_end_matches('/').to_string(),
            target_rpc: target_rpc.to_string(),
            ignite_cmd: ignite_cmd.to_string(),
            snapshot_timeout: Duration::from_secs(snapshot_timeout_secs),
            hook_timeout: Duration::from_secs(300),
            pgid_file: None,
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(15))
                .build(),
        }
    }

    pub fn with_hook_timeout(mut self, secs: u64) -> Self {
        self.hook_timeout = Duration::from_secs(secs.max(1));
        self
    }

    /// Record running hooks' process groups in `<journal>.hook.pgid` (see `kill_orphan_hooks`).
    pub fn with_pgid_file_for(mut self, journal_path: &Path) -> Self {
        self.pgid_file = Some(hook_pgid_path(journal_path));
        self
    }

    fn post(&self, url: &str, body: Option<Value>, timeout: Option<Duration>) -> Result<Value, String> {
        let mut req = self.agent.post(url);
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        let resp = match body {
            Some(b) => req.send_json(b),
            None => req.send_string(""),
        };
        match resp {
            Ok(r) => r
                .into_json::<Value>()
                .map_err(|e| format!("{url}: bad json: {e}")),
            Err(ureq::Error::Status(code, r)) => {
                let text = r.into_string().unwrap_or_default();
                Err(format!("{url}: HTTP {code}: {text}"))
            }
            Err(e) => Err(format!("{url}: {e}")),
        }
    }

    /// The target RPC URL with bound placeholders expanded; None while it still holds an
    /// unbound `{blockchain_id}` (the chain does not exist yet: "unreachable").
    pub fn target_url(&self) -> Option<String> {
        let vars = self.target_vars.lock().map(|v| v.clone()).unwrap_or_default();
        let url = expand_placeholders(&self.target_rpc, &vars);
        (!url.contains("{blockchain_id}")).then_some(url)
    }

    fn parse_info(v: &Value) -> Result<ChainInfo, String> {
        let s = |k: &str| -> Result<String, String> {
            v.get(k)
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .ok_or_else(|| format!("get_info missing {k}: {v}"))
        };
        let n = |k: &str| -> Result<u64, String> {
            let field = v.get(k).ok_or_else(|| format!("get_info missing {k}: {v}"))?;
            field
                .as_u64()
                .or_else(|| field.as_str().and_then(|x| x.parse().ok()))
                .ok_or_else(|| format!("get_info bad {k}: {v}"))
        };
        Ok(ChainInfo {
            chain_id: s("chain_id")?,
            head_block_num: n("head_block_num")?,
            head_block_id: s("head_block_id")?,
            head_block_time: s("head_block_time").unwrap_or_default(),
            last_irreversible_block_num: n("last_irreversible_block_num").unwrap_or(0),
        })
    }
}

impl ChainOps for HttpOps {
    fn source_info(&self) -> Result<ChainInfo, String> {
        let v = self.post(&format!("{}/v1/chain/get_info", self.source_rpc), None, None)?;
        Self::parse_info(&v)
    }

    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        let v = self.post(
            &format!("{}/v1/chain/get_block", self.source_rpc),
            Some(json!({"block_num_or_id": block_num})),
            None,
        )?;
        let id = v
            .get("id")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("get_block({block_num}) missing id"))?;
        let ts = v
            .get("timestamp")
            .and_then(|x| x.as_str())
            .unwrap_or_default();
        Ok((id.to_string(), ts.to_string()))
    }

    fn source_block_tx_count(&self, block_num: u64) -> Result<u64, String> {
        let v = self.post(
            &format!("{}/v1/chain/get_block", self.source_rpc),
            Some(json!({"block_num_or_id": block_num})),
            None,
        )?;
        Ok(v.get("transactions")
            .and_then(|t| t.as_array())
            .map(|a| a.len() as u64)
            .unwrap_or(0))
    }

    fn producer_paused(&self) -> Result<bool, String> {
        let v = self.post(&format!("{}/v1/producer/paused", self.producer_api), None, None)?;
        v.as_bool().ok_or_else(|| format!("paused: bad reply {v}"))
    }

    fn pause(&self) -> Result<(), String> {
        self.post(&format!("{}/v1/producer/pause", self.producer_api), None, None)
            .map(|_| ())
    }

    fn resume(&self) -> Result<(), String> {
        self.post(&format!("{}/v1/producer/resume", self.producer_api), None, None)
            .map(|_| ())
    }

    fn create_snapshot(&self) -> Result<SnapshotResult, String> {
        let v = self.post(
            &format!("{}/v1/producer/create_snapshot", self.producer_api),
            None,
            Some(self.snapshot_timeout),
        )?;
        let name = v
            .get("snapshot_name")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("create_snapshot: no snapshot_name in {v}"))?;
        let id = v
            .get("head_block_id")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("create_snapshot: no head_block_id in {v}"))?;
        // Antelope block ids carry the height in the first 4 bytes (BE).
        let num = v
            .get("head_block_num")
            .and_then(|x| x.as_u64())
            .or_else(|| u64::from_str_radix(id.get(0..8)?, 16).ok())
            .ok_or_else(|| format!("create_snapshot: cannot derive height from {v}"))?;
        Ok(SnapshotResult {
            snapshot_name: name.to_string(),
            head_block_num: num,
            head_block_id: id.to_string(),
        })
    }

    fn schedule_snapshot(&self, height: u64) -> Result<(), String> {
        self.post(
            &format!("{}/v1/producer/schedule_snapshot", self.producer_api),
            Some(json!({"start_block_num": height, "end_block_num": height})),
            None,
        )
        .map(|_| ())
    }

    fn target_info(&self) -> Result<Option<ChainInfo>, String> {
        let body = json!({"jsonrpc": "2.0", "method": "pulsevm.getInfo", "params": {}, "id": 1});
        let Some(url) = self.target_url() else { return Ok(None) };
        match self.post(&url, Some(body), None) {
            Ok(v) => {
                let result = v.get("result").cloned().unwrap_or(v);
                match Self::parse_info(&result) {
                    Ok(info) => Ok(Some(info)),
                    // RPC answered but the chain isn't serving state yet.
                    Err(_) => Ok(None),
                }
            }
            // Unreachable/booting is a normal transient, not an error.
            Err(_) => Ok(None),
        }
    }

    fn public_info(&self, public_url: &str) -> Result<Option<ChainInfo>, String> {
        let url = format!("{}/v1/chain/get_info", public_url.trim_end_matches('/'));
        match self.post(&url, None, None) {
            Ok(v) => Ok(Self::parse_info(&v).ok()),
            Err(_) => Ok(None),
        }
    }

    fn target_block_id(&self, height: u64) -> Result<Option<String>, String> {
        // PulseVM's getBlock takes block_num_or_id as a STRING.
        let body = json!({"jsonrpc": "2.0", "method": "pulsevm.getBlock",
                          "params": {"block_num_or_id": height.to_string()}, "id": 1});
        let Some(url) = self.target_url() else { return Ok(None) };
        match self.post(&url, Some(body), None) {
            Ok(v) => {
                let r = v.get("result").cloned().unwrap_or(v);
                Ok(["id", "block_id"].iter().find_map(|k| r.get(*k).and_then(|x| x.as_str()).map(str::to_string)))
            }
            Err(_) => Ok(None),
        }
    }

    fn source_block(&self, block_num: u64, rpc_url: Option<&str>) -> Result<Value, String> {
        let base = rpc_url.map(|u| u.trim_end_matches('/').to_string()).unwrap_or_else(|| self.source_rpc.clone());
        self.post(&format!("{base}/v1/chain/get_block"), Some(json!({"block_num_or_id": block_num})), None)
    }

    fn bind_target(&self, vars: &[(String, String)]) {
        if let Ok(mut v) = self.target_vars.lock() {
            for (k, val) in vars {
                v.retain(|(k2, _)| k2 != k);
                v.push((k.clone(), val.clone()));
            }
        }
    }

    fn ignite(&self) -> Result<String, String> {
        let vars = self.target_vars.lock().map(|v| v.clone()).unwrap_or_default();
        let cmd = expand_placeholders(&self.ignite_cmd, &vars);
        if cmd.contains("{blockchain_id}") {
            return Err(format!("ignite command still has an unbound {{blockchain_id}}: `{cmd}`"));
        }
        self.run_hook(&cmd)
    }

    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        // Once the upstream backend has created the target chain, `{blockchain_id}`,
        // `{subnet_id}`, `{chain_config}`, … expand in every hook (e.g. a flip script that points
        // the edge at /ext/bc/<id>). Before that they are left as written.
        let vars = self.target_vars.lock().map(|v| v.clone()).unwrap_or_default();
        let cmd = expand_placeholders(cmd, &vars);
        run_shell_timeout_tracked(&cmd, self.hook_timeout, self.pgid_file.as_deref())
    }

    fn kill_orphan_hooks(&self) -> Result<Option<String>, String> {
        match &self.pgid_file {
            Some(p) => kill_recorded_hook_group(p, Duration::from_secs(5)),
            None => Ok(None),
        }
    }

    fn run_long(&self, cmd: &str) -> Result<String, String> {
        // Pipeline steps (upstream export/import over a mainnet snapshot) take far longer than any
        // hook timeout, but they run exactly like hooks otherwise: their own process group, recorded
        // for this journal, so `pkill` of the agent cannot leave an untracked import running that
        // re-creates the staged artifact after a rollback. No practical deadline (30 days).
        run_shell_timeout_tracked(cmd, LONG_STEP_DEADLINE, self.pgid_file.as_deref())
    }

    fn get_json(&self, url: &str) -> Result<Option<Value>, String> {
        match self.agent.get(url).call() {
            Ok(r) => Ok(r.into_json::<Value>().ok()),
            Err(_) => Ok(None),
        }
    }

    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn sleep_ms(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

#[cfg(test)]
mod placeholder_tests {
    use super::*;

    #[test]
    fn target_url_and_ignite_cmd_expand_bound_placeholders() {
        let ops = HttpOps::new("http://s", "http://p", "http://127.0.0.1:9650/ext/bc/{blockchain_id}/rpc",
            "install {chain_config} {blockchain_id} && systemctl restart metalgo", 5);
        assert_eq!(ops.target_url(), None, "unbound blockchain id = target not reachable yet");
        ops.bind_target(&[("blockchain_id".into(), "2YXV".into()), ("chain_config".into(), "/w/cc.json".into())]);
        assert_eq!(ops.target_url().unwrap(), "http://127.0.0.1:9650/ext/bc/2YXV/rpc");
        let vars = ops.target_vars.lock().unwrap().clone();
        assert_eq!(expand_placeholders(&ops.ignite_cmd, &vars), "install /w/cc.json 2YXV && systemctl restart metalgo");
        // Every hook expands them too (a flip script repointing the edge at the new chain).
        assert_eq!(ops.run_hook("echo /ext/bc/{blockchain_id}").unwrap(), "/ext/bc/2YXV");
        // Re-binding replaces, never duplicates.
        ops.bind_target(&[("blockchain_id".into(), "3ABC".into())]);
        assert_eq!(ops.target_url().unwrap(), "http://127.0.0.1:9650/ext/bc/3ABC/rpc");
    }
}

#[cfg(all(test, unix))]
mod group_kill_tests {
    use super::*;
    use std::os::unix::process::CommandExt;

    /// Regression (Linux fault injection, 2026-09-30): killing a hook's process group must reach
    /// exactly that group, on every platform, and never this process or anything else.
    #[test]
    fn signal_group_kills_exactly_the_hook_group_and_nothing_else() {
        let mut bystander = std::process::Command::new("sleep").arg("60").spawn().unwrap();
        let mut hook = std::process::Command::new("sh")
            .args(["-c", "sleep 60 & sleep 60"])
            .process_group(0)
            .spawn()
            .unwrap();
        let pgid = hook.id();
        std::thread::sleep(Duration::from_millis(300));
        assert!(group_members(pgid).len() >= 2, "fixture: the hook and its background child share the group");
        assert!(signal_group(pgid, libc::SIGTERM));
        let status = hook.wait().unwrap();
        assert!(!status.success(), "the hook leader was killed");
        let t = std::time::Instant::now();
        while !group_members(pgid).is_empty() && t.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(group_members(pgid).is_empty(), "the background child in the group is gone too");
        assert!(bystander.try_wait().unwrap().is_none(), "a process outside the group was NOT signalled");
        let _ = bystander.kill();
        let _ = bystander.wait();
    }

    #[test]
    fn signal_group_refuses_pgid_0_and_1() {
        // kill(0, sig) would hit our own group; kill(-1, sig) every process we may signal.
        assert!(!signal_group(0, 0));
        assert!(!signal_group(1, 0));
    }
}
