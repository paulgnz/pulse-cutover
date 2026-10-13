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
    /// Remove a pending single-block snapshot request at exactly `height` (Leap's producer API
    /// `get_snapshot_requests` + `unschedule_snapshot`). Ok(true) when one was removed, Ok(false) when none
    /// was pending (already written, or never scheduled) or this ChainOps cannot unschedule.
    fn unschedule_snapshot(&self, _height: u64) -> Result<bool, String> {
        Ok(false)
    }
    /// r30 round 2: schedule a single-block snapshot at `height` and return the producer API's
    /// `snapshot_request_id` when it reports one, so the caller can later remove exactly that request.
    fn schedule_snapshot_request(&self, height: u64) -> Result<Option<u64>, String> {
        self.schedule_snapshot(height).map(|_| None)
    }
    /// Remove the pending snapshot request `id`, and only it, and only while it is still the single-block request at
    /// `height` it was recorded as (r30 round 3: a producer-local id proves nothing on its own). Ok(true) when it was
    /// pending at that height and removed, Ok(false) when no such request is pending (or this ChainOps cannot
    /// unschedule).
    fn unschedule_snapshot_request(&self, _id: u64, _height: u64) -> Result<bool, String> {
        Ok(false)
    }
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
    /// rc.27: the target's protocol fields from `pulsevm.getInfo` (`protocol_version`,
    /// `supported_protocol_version`, `protocol_upgrade_schedule_hash`, `next_protocol_upgrade`). Ok(None) when
    /// the target does not report them (or cannot be read).
    fn target_protocol(&self) -> Result<Option<Value>, String> {
        Ok(None)
    }
    /// The block id at `height` as the PUBLIC endpoint serves it (`/v1/chain/get_block`). Ok(None)
    /// when it cannot show it.
    fn public_block_id(&self, _public_url: &str, _height: u64) -> Result<Option<String>, String> {
        Ok(None)
    }
    /// The FULL source block at `block_num` as nodeos `get_block` JSON (all receipts), from
    /// `rpc_url` (a nodeos /v1 base) or, when None, the source RPC. Upstream ignition packs it
    /// into the boot manifest's anchor.
    fn source_block(&self, block_num: u64, _rpc_url: Option<&str>) -> Result<Value, String> {
        Err(format!("source_block({block_num}) is not supported by this ChainOps"))
    }
    /// The source chain's `eosio/producers` row for `producer` (nodeos get_table_rows, JSON);
    /// Ok(None) when no such producer is registered.
    fn source_producer(&self, producer: &str) -> Result<Option<Value>, String> {
        Err(format!("source_producer({producer}) is not supported by this ChainOps"))
    }
    /// Shadow mode (docs/DESIGN-shadow.md): the network id the target metalgo reports
    /// (`info.getNetworkID` under `base`/ext/info). None = not answered (the shadow check fails closed).
    fn metal_network_id(&self, _base: &str) -> Result<Option<u64>, String> {
        Ok(None)
    }
    /// Shadow mode (round-2 review N5): the P-Chain status the target metalgo under `base` reports for `blockchain_id`
    /// (`platform.getBlockchainStatus`: "Validating", "Created", "Preferred", "Syncing" or "Unknown"). None = not
    /// answered. Default: not supported (the shadow check fails closed).
    fn metal_blockchain_status(&self, base: &str, blockchain_id: &str) -> Result<Option<String>, String> {
        Err(format!("metal_blockchain_status({base}, {blockchain_id}) is not supported by this ChainOps"))
    }
    /// Shadow mode: the source's `get_producer_schedule` answer. None = not answered (fails closed).
    fn source_producer_schedule(&self) -> Result<Option<Value>, String> {
        Ok(None)
    }
    /// Shadow mode: POST `body` to `path` on a PUBLIC chain API `base` (the pinned public XPR mainnet endpoints in
    /// `shadow::PUBLIC_XPR_MAINNET_APIS`). Default: not supported (the shadow proof fails closed).
    fn public_chain_post(&self, base: &str, path: &str, _body: Value) -> Result<Value, String> {
        Err(format!("public_chain_post({base}{path}) is not supported by this ChainOps"))
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
    /// `run_long` that can be cut short: `cancel` is polled while the step runs (and once before
    /// it starts); when it returns true the step's process group is killed and Ok(None) is
    /// returned. Ok(Some(output)) = the step finished. Used for the upstream pipeline so a signed
    /// coordinator abort is honoured mid-export/import, not only after VERIFIED. Default: checks
    /// `cancel` before and after an uninterruptible `run_long`.
    fn run_long_cancellable(&self, cmd: &str, cancel: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        if cancel() {
            return Ok(None);
        }
        let out = self.run_long(cmd)?;
        if cancel() {
            return Ok(None);
        }
        Ok(Some(out))
    }
    /// Run a program by ARGUMENT VECTOR, never through a shell (the readiness drill's only way to run anything,
    /// r30 round 4): `argv[0]` is the program, the rest its arguments, passed as they are. Same contract as
    /// `run_long_cancellable` otherwise. Default: refused (an implementation must opt in).
    fn run_argv_cancellable(&self, argv: &[String], _cancel: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        Err(format!("run_argv_cancellable is not supported here ({})", argv.first().map(String::as_str).unwrap_or("")))
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

/// Process ids currently in process group `pgid` (empty if none / cannot tell). The ceremony's historical view:
/// an inspection that fails reads as "no members". Strict callers (the readiness drill) use [`group_status`].
#[cfg(unix)]
fn group_members(pgid: u32) -> Vec<u32> {
    match group_status(pgid) {
        GroupStatus::Live(m) => m,
        GroupStatus::Gone | GroupStatus::Unknown(_) => vec![],
    }
}

/// What process inspection says about a process group (r30 round 4: "cannot tell" is not "gone").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupStatus {
    /// `ps` listed the processes and none (except zombies) is in the group.
    Gone,
    /// Live members (pids).
    Live(Vec<u32>),
    /// `ps` is missing, refused, exited non-zero or printed nothing parseable: unknown, never "stopped".
    Unknown(String),
}

#[cfg(test)]
thread_local! {
    /// Tests only: the program used as `ps` on this thread (to simulate a failing process inspection).
    pub(crate) static PS_PROGRAM: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

fn ps_program() -> String {
    #[cfg(test)]
    if let Some(p) = PS_PROGRAM.with(|p| p.borrow().clone()) {
        return p;
    }
    "ps".to_string()
}

/// [`GroupStatus`] of process group `pgid`.
#[cfg(unix)]
pub fn group_status(pgid: u32) -> GroupStatus {
    let out = match std::process::Command::new(ps_program()).args(["-A", "-o", "pid=,pgid=,stat="]).output() {
        Ok(o) => o,
        Err(e) => return GroupStatus::Unknown(format!("ps could not run: {e}")),
    };
    if !out.status.success() {
        return GroupStatus::Unknown(format!("ps exited {}", out.status));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parsed = 0usize;
    let mut members = vec![];
    for l in text.lines() {
        let mut it = l.split_whitespace();
        let (Some(pid), Some(g)) = (it.next().and_then(|v| v.parse::<u32>().ok()), it.next().and_then(|v| v.parse::<u32>().ok()))
        else {
            continue;
        };
        parsed += 1;
        // A zombie is already dead (only its exit status is left): it is not a survivor.
        let zombie = it.next().is_some_and(|st| st.starts_with('Z'));
        if g == pgid && !zombie {
            members.push(pid);
        }
    }
    // `ps -A` always lists at least this process: nothing parseable means the inspection failed.
    if parsed == 0 {
        return GroupStatus::Unknown("ps printed no process list".into());
    }
    if members.is_empty() { GroupStatus::Gone } else { GroupStatus::Live(members) }
}
#[cfg(not(unix))]
pub fn group_status(_pgid: u32) -> GroupStatus {
    GroupStatus::Unknown("process groups are not inspected on this platform".into())
}

/// The pgid recorded in `pgid_file` (first line), if the file holds one.
pub fn recorded_pgid(pgid_file: &Path) -> Option<u32> {
    let text = std::fs::read_to_string(pgid_file).ok()?;
    text.lines().next()?.trim().parse().ok().filter(|p: &u32| *p > 1)
}

/// The strict view of a recorded group (readiness drill): None when nothing is recorded, else its status.
pub fn recorded_group_status(pgid_file: &Path) -> Option<(u32, GroupStatus)> {
    recorded_pgid(pgid_file).map(|g| (g, group_status(g)))
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

/// The live process group recorded in `pgid_file` (first line), if any member is still running.
pub fn live_recorded_group(pgid_file: &Path) -> Option<u32> {
    recorded_live_group(pgid_file)
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
    kill_recorded_group(pgid_file, grace, false)
}

/// [`kill_recorded_hook_group`] for a readiness drill's `--cancel` (r30 round 3): the record is removed only when the
/// group is verified gone. On a refusal (the leader is not the recorded process) or a group that survived SIGKILL
/// the record stays, so the drill keeps showing as RUNNING and a ceremony's ARM keeps refusing.
#[cfg(unix)]
pub fn kill_recorded_hook_group_keep_evidence(pgid_file: &Path, grace: Duration) -> Result<Option<String>, String> {
    kill_recorded_group(pgid_file, grace, true)
}
#[cfg(not(unix))]
pub fn kill_recorded_hook_group_keep_evidence(_pgid_file: &Path, _grace: Duration) -> Result<Option<String>, String> {
    Ok(None)
}

#[cfg(unix)]
fn kill_recorded_group(pgid_file: &Path, grace: Duration, keep_on_failure: bool) -> Result<Option<String>, String> {
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
    if keep_on_failure {
        // Strict (readiness drill): "cannot tell" keeps the record and is a failure, never "already gone".
        match group_status(pgid) {
            GroupStatus::Gone => {
                let _ = std::fs::remove_file(pgid_file);
                return Ok(None);
            }
            GroupStatus::Unknown(why) => {
                return Err(format!(
                    "cannot tell whether process group {pgid} is still running ({why}): not killed, record kept"
                ))
            }
            GroupStatus::Live(_) => {}
        }
    } else if group_members(pgid).is_empty() {
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
                if keep_on_failure {
                    return Err(format!(
                        "recorded process group {pgid} now has the leader `{leader}`, not the recorded process: not \
                         killed. The record is kept (live work may remain); check it by hand"
                    ));
                }
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
    // Strict: only a group `ps` positively lists as gone counts as stopped.
    let gone = if keep_on_failure { group_status(pgid) == GroupStatus::Gone } else { group_members(pgid).is_empty() };
    if gone || !keep_on_failure {
        let _ = std::fs::remove_file(pgid_file);
    }
    if gone {
        Ok(Some(format!("killed orphaned hook process group {pgid} ({how}): `{cmd}`")))
    } else {
        Err(format!("orphaned hook process group {pgid} (`{cmd}`) survived SIGKILL (or cannot be verified gone)"))
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

/// Removes the recorded hook process-group file when the hook finishes (any path out), unless [`PgidRecord::keep`]
/// was called (strict runs keep the record while the group cannot be verified gone).
struct PgidRecord(Option<PathBuf>, std::cell::Cell<bool>);
impl PgidRecord {
    fn keep(&self) {
        self.1.set(true);
    }
}
impl Drop for PgidRecord {
    fn drop(&mut self) {
        if let (Some(p), false) = (&self.0, self.1.get()) {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Write a process-group record as a NEW inode (r30 round 4): a name already at `path` (a stale record, a symlink,
/// a hard link planted after a preflight check) is unlinked first, which never touches the file it pointed at; the
/// record is created with O_CREAT|O_EXCL|O_NOFOLLOW and must have exactly one name.
pub fn write_pgid_record(path: &Path, contents: &str) -> Result<(), String> {
    use std::io::Write;
    if std::fs::symlink_metadata(path).is_ok() {
        std::fs::remove_file(path).map_err(|e| format!("replace {}: {e}", path.display()))?;
    }
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.custom_flags(libc::O_NOFOLLOW);
    }
    let mut f = o.open(path).map_err(|e| format!("create {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let m = f.metadata().map_err(|e| format!("stat {}: {e}", path.display()))?;
        if m.nlink() != 1 {
            let _ = std::fs::remove_file(path);
            return Err(format!("{} has {} names right after creation: refused", path.display(), m.nlink()));
        }
    }
    f.write_all(contents.as_bytes()).map_err(|e| format!("write {}: {e}", path.display()))?;
    f.sync_all().map_err(|e| format!("fsync {}: {e}", path.display()))
}

/// `run_shell_timeout`, additionally recording the hook's process-group id in `pgid_file` while
/// it runs (removed on completion). If the agent is killed mid-hook (`cutover.sh abort`
/// SIGTERMs it; hooks live in their own process group and survive), `rollback` reads this file
/// and kills the orphaned group before it runs `on_abort` or any revert.
pub fn run_shell_timeout_tracked(cmd: &str, timeout: Duration, pgid_file: Option<&Path>) -> Result<String, String> {
    run_shell_inner(cmd, timeout, pgid_file, None).map(|o| o.expect("no cancel: always completes"))
}

/// How often a cancellable step polls its `cancel` callback (the callback rate-limits any
/// network check itself).
pub const CANCEL_POLL: Duration = Duration::from_millis(250);

/// `run_shell_timeout_tracked` that kills the command's process group and returns Ok(None) as
/// soon as `cancel` returns true (polled every `CANCEL_POLL` while the command runs).
pub fn run_shell_cancellable(cmd: &str, timeout: Duration, pgid_file: Option<&Path>, cancel: &mut dyn FnMut() -> bool)
    -> Result<Option<String>, String> {
    if cancel() {
        return Ok(None);
    }
    run_shell_inner(cmd, timeout, pgid_file, Some(cancel))
}

fn run_shell_inner(cmd: &str, timeout: Duration, pgid_file: Option<&Path>, cancel: Option<&mut dyn FnMut() -> bool>)
    -> Result<Option<String>, String> {
    let mut command = std::process::Command::new("sh");
    command.arg("-c").arg(cmd);
    run_command_inner(command, cmd, timeout, pgid_file, cancel, false)
}

/// Run a program by argument vector (no shell) in its own process group, STRICT (the readiness drill, r30 round 4):
/// its record is written as a fresh inode and is only removed once `ps` positively shows the whole group gone; a
/// group that survives the kill, or cannot be inspected, keeps its record and makes the run fail as INCOMPLETE, so
/// the drill keeps showing as running and a ceremony keeps refusing to ARM.
pub fn run_argv_cancellable_strict(argv: &[String], timeout: Duration, pgid_file: Option<&Path>,
                                   cancel: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
    let Some((prog, args)) = argv.split_first() else { return Err("empty argument vector".into()) };
    if cancel() {
        return Ok(None);
    }
    let mut command = std::process::Command::new(prog);
    command.args(args);
    let display = argv.iter().map(|a| format!("'{a}'")).collect::<Vec<_>>().join(" ");
    run_command_inner(command, &display, timeout, pgid_file, Some(cancel), true)
}

/// After a strict run's group was signalled: wait briefly until `ps` shows it gone (re-sending SIGKILL), then
/// Ok(()) or the reason it cannot be verified gone.
#[cfg(unix)]
fn verify_group_gone(pgid: u32) -> Result<(), String> {
    let end = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match group_status(pgid) {
            GroupStatus::Gone => return Ok(()),
            GroupStatus::Live(m) if std::time::Instant::now() >= end => {
                return Err(format!("process group {pgid} still has members {m:?} after SIGKILL"))
            }
            GroupStatus::Unknown(why) if std::time::Instant::now() >= end => {
                return Err(format!("cannot verify process group {pgid} is gone: {why}"))
            }
            _ => {
                kill_group(pgid);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}
#[cfg(not(unix))]
fn verify_group_gone(_pgid: u32) -> Result<(), String> {
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_command_inner(mut command: std::process::Command, cmd: &str, timeout: Duration, pgid_file: Option<&Path>,
                     mut cancel: Option<&mut dyn FnMut() -> bool>, strict: bool) -> Result<Option<String>, String> {
    #[cfg(unix)]
    use std::os::unix::process::CommandExt;
    use std::sync::mpsc;
    command
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
        // Strict: a recorded group whose state cannot be inspected is not "gone" either.
        if strict {
            if let Some((g, GroupStatus::Unknown(why))) = recorded_group_status(p) {
                return Err(format!(
                    "refusing to start `{cmd}`: cannot tell whether process group {g} recorded in {} is still running \
                     ({why})",
                    p.display()
                ));
            }
        }
    }
    let mut child = command.spawn().map_err(|e| format!("spawn `{cmd}`: {e}"))?;
    let pid = child.id();
    // pgid (= the leader's pid: process_group(0)), the leader's start time (recycled-pid guard) and the command, for
    // the operator. Written as a fresh inode, never through a link (r30 round 4).
    let written = pgid_file.map(|p| {
        let start = process_start(pid).map(|s| format!("start={s}\n")).unwrap_or_default();
        write_pgid_record(p, &format!("{pid}\n{start}{cmd}\n")).map(|_| p.to_path_buf())
    });
    let record = match written {
        None => PgidRecord(None, std::cell::Cell::new(false)),
        Some(Ok(p)) => PgidRecord(Some(p), std::cell::Cell::new(false)),
        Some(Err(e)) if strict => {
            // A strict run without its record could not be found again if the agent died: stop it now.
            kill_group(pid);
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("refusing to run `{cmd}` without its process-group record: {e}"));
        }
        Some(Err(_)) => PgidRecord(None, std::cell::Cell::new(false)),
    };
    // Strict runs end only once the whole group is verified gone; otherwise the record stays and the run fails.
    let finish = |res: Result<Option<String>, String>| -> Result<Option<String>, String> {
        if !strict {
            return res;
        }
        #[cfg(unix)]
        {
            match group_status(pid) {
                GroupStatus::Gone => return res,
                _ => kill_group(pid),
            }
        }
        match verify_group_gone(pid) {
            Ok(()) => res,
            Err(why) => {
                record.keep();
                Err(format!("INCOMPLETE: `{cmd}`: {why}; its process-group record is kept (work may still be running)"))
            }
        }
    };
    // Drain pipes on threads (a chatty command can't block on a full pipe); results arrive on a
    // channel so waiting for them can be bounded instead of an unconditional join.
    let so = child.stdout.take().expect("piped");
    let se = child.stderr.take().expect("piped");
    let (tx_out, rx_out) = mpsc::channel();
    let (tx_err, rx_err) = mpsc::channel();
    std::thread::spawn(move || { let _ = tx_out.send(capture(so)); });
    std::thread::spawn(move || { let _ = tx_err.send(capture(se)); });
    let started = std::time::Instant::now();
    let mut last_cancel_poll = std::time::Instant::now();
    let status = loop {
        match child.try_wait().map_err(|e| format!("wait `{cmd}`: {e}"))? {
            Some(st) => break Some(st),
            None if started.elapsed() >= timeout => break None,
            None => {
                if let Some(c) = cancel.as_mut() {
                    if last_cancel_poll.elapsed() >= CANCEL_POLL {
                        last_cancel_poll = std::time::Instant::now();
                        if c() {
                            kill_group(pid);
                            let _ = child.kill();
                            let _ = child.wait();
                            return finish(Ok(None));
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(50))
            }
        }
    };
    let Some(status) = status else {
        kill_group(pid);
        let _ = child.kill();
        let _ = child.wait();
        return finish(Err(format!("`{cmd}` timed out after {}s and was killed", timeout.as_secs())));
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
    let res = if status.success() {
        Ok(Some(format!("{}{note}", if stdout.is_empty() { stderr } else { stdout })))
    } else {
        Err(format!("`{cmd}` exited {status}: {stderr} {stdout}{note}"))
    };
    finish(res)
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
            // A short connect timeout (rc.26, fleet run d2): against a silently dropping firewall each relay read
            // otherwise sat out the whole 15 s, so the resume guard's retries stranded ~3.4 min after the fleet timeout.
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(5))
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
        block_tx_count(&v, block_num)
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

    fn schedule_snapshot_request(&self, height: u64) -> Result<Option<u64>, String> {
        let r = self.post(
            &format!("{}/v1/producer/schedule_snapshot", self.producer_api),
            Some(json!({"start_block_num": height, "end_block_num": height})),
            None,
        )?;
        Ok(r["snapshot_request_id"].as_u64())
    }

    fn unschedule_snapshot_request(&self, id: u64, height: u64) -> Result<bool, String> {
        let reqs = self.post(&format!("{}/v1/producer/get_snapshot_requests", self.producer_api), Some(json!({})), None)?;
        let pending = reqs["snapshot_requests"].as_array().is_some_and(|a| {
            a.iter().any(|r| {
                r["snapshot_request_id"].as_u64() == Some(id)
                    && r["start_block_num"].as_u64() == Some(height)
                    && r["end_block_num"].as_u64() == Some(height)
                    && r["block_spacing"].as_u64().unwrap_or(0) == 0
            })
        });
        if !pending {
            return Ok(false);
        }
        self.post(
            &format!("{}/v1/producer/unschedule_snapshot", self.producer_api),
            Some(json!({"snapshot_request_id": id})),
            None,
        )?;
        Ok(true)
    }

    fn unschedule_snapshot(&self, height: u64) -> Result<bool, String> {
        let reqs = self.post(&format!("{}/v1/producer/get_snapshot_requests", self.producer_api), Some(json!({})), None)?;
        let ids: Vec<u64> = reqs["snapshot_requests"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|r| {
                        r["start_block_num"].as_u64() == Some(height)
                            && r["end_block_num"].as_u64() == Some(height)
                            && r["block_spacing"].as_u64().unwrap_or(0) == 0
                    })
                    .filter_map(|r| r["snapshot_request_id"].as_u64())
                    .collect()
            })
            .unwrap_or_default();
        for id in &ids {
            self.post(
                &format!("{}/v1/producer/unschedule_snapshot", self.producer_api),
                Some(json!({"snapshot_request_id": id})),
                None,
            )?;
        }
        Ok(!ids.is_empty())
    }

    fn target_protocol(&self) -> Result<Option<Value>, String> {
        let body = json!({"jsonrpc": "2.0", "method": "pulsevm.getInfo", "params": {}, "id": 1});
        let Some(url) = self.target_url() else { return Err("no target RPC url".into()) };
        let v = self.post(&url, Some(body), None)?;
        // A JSON-RPC error is a failed read (retried by the caller), not "a binary without protocol fields" (rc.27 review).
        if let Some(e) = v.get("error").filter(|e| !e.is_null()) {
            return Err(format!("pulsevm.getInfo returned an error: {e}"));
        }
        let r = v.get("result").cloned().unwrap_or(v);
        // Only a 64-hex hash counts as reported (a "" / 0 sentinel from a default response is "not reported").
        let hash = r["protocol_upgrade_schedule_hash"].as_str()
            .filter(|h| h.len() == 64 && h.chars().all(|c| c.is_ascii_hexdigit()));
        Ok(hash.map(|h| json!({"protocol_version": r["protocol_version"], "supported_protocol_version": r["supported_protocol_version"],
            "protocol_upgrade_schedule_hash": h.to_ascii_lowercase(), "next_protocol_upgrade": r["next_protocol_upgrade"]})))
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

    fn public_block_id(&self, public_url: &str, height: u64) -> Result<Option<String>, String> {
        let url = format!("{}/v1/chain/get_block", public_url.trim_end_matches('/'));
        match self.post(&url, Some(json!({"block_num_or_id": height.to_string()})), None) {
            Ok(v) => Ok(v.get("id").and_then(|x| x.as_str()).map(str::to_string)),
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

    fn source_producer(&self, producer: &str) -> Result<Option<Value>, String> {
        let v = self.post(
            &format!("{}/v1/chain/get_table_rows", self.source_rpc),
            Some(json!({"json": true, "code": "eosio", "scope": "eosio", "table": "producers",
                        "lower_bound": producer, "upper_bound": producer, "limit": 1})),
            None,
        )?;
        let rows = v.get("rows").and_then(|r| r.as_array()).ok_or_else(|| format!("get_table_rows eosio/producers: no rows in {v}"))?;
        Ok(rows.iter().find(|r| r["owner"].as_str() == Some(producer)).cloned())
    }

    fn metal_network_id(&self, base: &str) -> Result<Option<u64>, String> {
        let v = self.post(
            &format!("{}/ext/info", base.trim_end_matches('/')),
            Some(json!({"jsonrpc": "2.0", "id": 1, "method": "info.getNetworkID", "params": {}})),
            None,
        )?;
        let n = &v["result"]["networkID"];
        Ok(n.as_u64().or_else(|| n.as_str().and_then(|s| s.parse().ok())))
    }

    fn metal_blockchain_status(&self, base: &str, blockchain_id: &str) -> Result<Option<String>, String> {
        let v = self.post(
            &format!("{}/ext/P", base.trim_end_matches('/')),
            Some(json!({"jsonrpc": "2.0", "id": 1, "method": "platform.getBlockchainStatus",
                        "params": {"blockchainID": blockchain_id}})),
            None,
        )?;
        Ok(v["result"]["status"].as_str().map(str::to_string))
    }

    fn source_producer_schedule(&self) -> Result<Option<Value>, String> {
        let v = self.post(&format!("{}/v1/chain/get_producer_schedule", self.source_rpc), Some(json!({})), None)?;
        Ok(v.get("active").is_some().then_some(v))
    }

    fn public_chain_post(&self, base: &str, path: &str, body: Value) -> Result<Value, String> {
        self.post(&format!("{}{path}", base.trim_end_matches('/')), Some(body), Some(Duration::from_secs(10)))
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

    fn run_long_cancellable(&self, cmd: &str, cancel: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        run_shell_cancellable(cmd, LONG_STEP_DEADLINE, self.pgid_file.as_deref(), cancel)
    }

    fn run_argv_cancellable(&self, argv: &[String], cancel: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        run_argv_cancellable_strict(argv, LONG_STEP_DEADLINE, self.pgid_file.as_deref(), cancel)
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

/// Transactions in a nodeos `get_block` answer for `block_num`, FAIL CLOSED: the answer must be the
/// block asked for (its `block_num` and the height encoded in its `id`) and carry a `transactions`
/// array. An error object, another schema or a truncated proxy answer is an error (unknown), never
/// "0 transactions": the burn-off audit certifies empty blocks with this.
pub fn block_tx_count(v: &Value, block_num: u64) -> Result<u64, String> {
    let short = || {
        let t = v.to_string();
        if t.len() > 200 { format!("{}…", &t[..200]) } else { t }
    };
    let num = v.get("block_num").and_then(|x| x.as_u64().or_else(|| x.as_str().and_then(|s| s.parse().ok())));
    if num != Some(block_num) {
        return Err(format!("get_block({block_num}) answered block_num {num:?}, not the block asked for: {}", short()));
    }
    let id = v.get("id").and_then(|x| x.as_str()).unwrap_or_default();
    let encoded = (id.len() == 64 && id.chars().all(|c| c.is_ascii_hexdigit()))
        .then(|| u32::from_str_radix(&id[..8], 16).ok())
        .flatten();
    if encoded.map(u64::from) != Some(block_num) {
        return Err(format!("get_block({block_num}) has no block id for that height (id {id:?}): {}", short()));
    }
    v.get("transactions")
        .and_then(|t| t.as_array())
        .map(|a| a.len() as u64)
        .ok_or_else(|| format!("get_block({block_num}) has no transactions array: {}", short()))
}

#[cfg(test)]
mod block_tx_count_tests {
    use super::*;

    fn id(n: u64) -> String {
        format!("{n:08x}{}", "ab".repeat(28))
    }

    #[test]
    fn counts_only_a_well_formed_answer_for_the_block_asked_for() {
        assert_eq!(block_tx_count(&json!({"block_num": 7, "id": id(7), "transactions": []}), 7), Ok(0));
        assert_eq!(block_tx_count(&json!({"block_num": "7", "id": id(7), "transactions": [{}, {}]}), 7), Ok(2));
    }

    /// Review: a 200 carrying an error object, another schema or a truncated answer certified an
    /// empty burn-off block.
    #[test]
    fn malformed_or_foreign_answers_are_errors_never_zero() {
        for (v, why) in [
            (json!({"code": 500, "message": "Internal Service Error", "error": {}}), "error object"),
            (json!({"block_num": 7, "id": id(7)}), "no transactions"),
            (json!({"block_num": 7, "id": id(7), "transactions": {}}), "transactions not an array"),
            (json!({"block_num": 8, "id": id(8), "transactions": []}), "another block"),
            (json!({"id": id(7), "transactions": []}), "no block_num"),
            (json!({"block_num": 7, "transactions": []}), "no id"),
            (json!({"block_num": 7, "id": id(8), "transactions": []}), "id of another height"),
            (json!({"block_num": 7, "id": "zz", "transactions": []}), "malformed id"),
            (json!(null), "null"),
        ] {
            assert!(block_tx_count(&v, 7).is_err(), "{why} must not count as an empty block");
        }
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

    /// Stage-2 run 3: a signed abort waited 23.7 s for the upstream pipeline. A cancellable long
    /// step must stop promptly, take its whole process group with it (a background child would
    /// otherwise finish the import after the rollback), and drop its pgid record.
    #[test]
    fn cancellable_step_stops_promptly_and_kills_its_group() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("finished");
        let pgid_file = dir.path().join("j.hook.pgid");
        let cmd = format!("(sleep 2; touch {m}) & sleep 2; touch {m}", m = marker.display());
        let mut polls = 0;
        let t = std::time::Instant::now();
        let out = run_shell_cancellable(&cmd, Duration::from_secs(60), Some(&pgid_file), &mut || {
            polls += 1;
            polls >= 3
        })
        .unwrap();
        assert_eq!(out, None, "cancelled");
        assert!(t.elapsed() < Duration::from_millis(1500), "stopped promptly: {:?}", t.elapsed());
        std::thread::sleep(Duration::from_millis(2500));
        assert!(!marker.exists(), "neither the step nor its background child kept running");
        assert!(!pgid_file.exists(), "the pgid record is removed with the step");
        // Not cancelled: the output comes back like run_long's.
        assert_eq!(run_shell_cancellable("echo ok", Duration::from_secs(5), None, &mut || false).unwrap().as_deref(), Some("ok"));
        // Already cancelled: the command never starts.
        let m2 = dir.path().join("never");
        assert_eq!(run_shell_cancellable(&format!("touch {}", m2.display()), Duration::from_secs(5), None, &mut || true).unwrap(), None);
        assert!(!m2.exists());
    }

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

    /// R4-10: a failing process inspection is UNKNOWN, never "gone".
    #[cfg(unix)]
    #[test]
    fn r4_unknown_process_inspection_is_never_gone_and_keeps_the_record() {
        PS_PROGRAM.with(|p| *p.borrow_mut() = Some("false".into()));
        assert!(matches!(group_status(4242), GroupStatus::Unknown(_)));
        PS_PROGRAM.with(|p| *p.borrow_mut() = Some("/nonexistent/ps".into()));
        assert!(matches!(group_status(4242), GroupStatus::Unknown(_)));
        let dir = tempfile::tempdir().unwrap();
        let rec = dir.path().join("x.hook.pgid");
        std::fs::write(&rec, "4242\nstart=x\nexport\n").unwrap();
        let res = kill_recorded_hook_group_keep_evidence(&rec, Duration::from_millis(50));
        PS_PROGRAM.with(|p| *p.borrow_mut() = None);
        assert!(res.unwrap_err().contains("cannot tell"));
        assert!(rec.exists(), "the record is kept while the group cannot be inspected");
        // With a working ps, an absent group is Gone.
        assert_eq!(group_status(u32::MAX - 7), GroupStatus::Gone);
    }

    /// R4-8: a strict (drill) run that is cancelled ends only once the group is verified gone; when that cannot be
    /// verified the record is KEPT and the run fails as INCOMPLETE instead of returning a clean cancel.
    #[cfg(unix)]
    #[test]
    fn r4_strict_runs_keep_their_record_unless_the_group_is_verified_gone() {
        let dir = tempfile::tempdir().unwrap();
        let rec = dir.path().join("d.hook.pgid");
        let argv: Vec<String> = ["sh", "-c", "sleep 30"].iter().map(|s| s.to_string()).collect();
        // Verified gone: a clean cancel, record removed.
        let mut n = 0;
        let r = run_argv_cancellable_strict(&argv, Duration::from_secs(60), Some(&rec), &mut || { n += 1; n > 3 });
        assert_eq!(r.unwrap(), None);
        assert!(!rec.exists());
        // Cannot be verified (ps fails from the moment the run is cancelled): INCOMPLETE, record kept.
        let mut n = 0;
        let r = run_argv_cancellable_strict(&argv, Duration::from_secs(60), Some(&rec), &mut || {
            n += 1;
            if n > 3 {
                PS_PROGRAM.with(|p| *p.borrow_mut() = Some("false".into()));
                return true;
            }
            false
        });
        PS_PROGRAM.with(|p| *p.borrow_mut() = None);
        let pgid = recorded_pgid(&rec);
        if let Some(g) = pgid {
            let _ = signal_group(g, libc::SIGKILL);
        }
        assert!(r.unwrap_err().contains("INCOMPLETE"));
        assert!(pgid.is_some(), "the record is kept");
        // The ceremony's shell runner keeps its old behaviour: a cancel returns Ok(None) and removes the record.
        let crec = dir.path().join("c.hook.pgid");
        let mut n = 0;
        let r = run_shell_cancellable("sleep 30", Duration::from_secs(60), Some(&crec), &mut || { n += 1; n > 3 });
        assert_eq!(r.unwrap(), None);
        assert!(!crec.exists());
    }

    /// R4-9: a process-group record is written as a fresh inode, never through a link planted at its path (the
    /// ceremony's runner and the drill's).
    #[cfg(unix)]
    #[test]
    fn r4_pgid_records_never_write_through_a_link() {
        let dir = tempfile::tempdir().unwrap();
        let protected = dir.path().join("journal.jsonl");
        std::fs::write(&protected, "JOURNAL").unwrap();
        let rec = dir.path().join("j.hook.pgid");
        std::os::unix::fs::symlink(&protected, &rec).unwrap();
        run_shell_timeout_tracked("true", Duration::from_secs(10), Some(&rec)).unwrap();
        assert_eq!(std::fs::read_to_string(&protected).unwrap(), "JOURNAL");
        std::fs::hard_link(&protected, &rec).unwrap();
        let argv: Vec<String> = ["true"].iter().map(|s| s.to_string()).collect();
        run_argv_cancellable_strict(&argv, Duration::from_secs(10), Some(&rec), &mut || false).unwrap();
        assert_eq!(std::fs::read_to_string(&protected).unwrap(), "JOURNAL");
    }

    #[test]
    fn signal_group_refuses_pgid_0_and_1() {
        // kill(0, sig) would hit our own group; kill(-1, sig) every process we may signal.
        assert!(!signal_group(0, 0));
        assert!(!signal_group(1, 0));
    }
}
