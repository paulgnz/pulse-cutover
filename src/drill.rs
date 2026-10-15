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
//!
//! What it does cost: CPU, RAM (the mainnet conversion peaked at 13.7 GB) and disk, plus nodeos writing the
//! snapshot at H synchronously, which on a producing node can cost it blocks in its own round. It has no
//! state or write impact; prefer a non-producing node with the same snapshot directory, or an H outside your
//! production round. A drill that fails or is cancelled before H is final unschedules its snapshot request.
//!
//! Mutual exclusion with a real ceremony runs both ways: a drill refuses next to a ceremony in progress, and a
//! fresh ceremony's ARM preflight refuses while a drill is RUNNING ([`activity`]). `pulse-cutover drill
//! --cancel` stops one ([`cancel`]).

use std::cell::RefCell;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

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
    cfg.journal_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("drill")
}

pub fn latest_path(cfg: &Config) -> PathBuf {
    drill_root(cfg).join("latest.json")
}

/// One drill's directory: `<journal dir>/drill/<H>/`.
pub fn drill_dir(cfg: &Config, h: u64) -> PathBuf {
    drill_root(cfg).join(h.to_string())
}

/// The path `HttpOps::with_pgid_file_for` is given for a drill: its process-group file is
/// `<journal dir>/drill/<H>/drill.jsonl.hook.pgid`, never the ceremony's `<journal>.hook.pgid`.
pub fn pgid_journal_for(cfg: &Config, h: u64) -> PathBuf {
    drill_dir(cfg, h).join("drill.jsonl")
}

/// The drill's process-group file (the pipeline tool running right now, if any).
pub fn pgid_file(cfg: &Config, h: u64) -> PathBuf {
    crate::ops::hook_pgid_path(&pgid_journal_for(cfg, h))
}

/// `pulse-cutover drill --cancel` writes this; the running drill polls it.
pub fn cancel_path(cfg: &Config, h: u64) -> PathBuf {
    drill_dir(cfg, h).join("cancel")
}

/// What this box's latest drill is doing right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrillActivity {
    /// No drill recorded, or the latest one finished.
    Idle,
    /// `latest.json` says RUNNING and its process (same pid and start time) or its pipeline process group is alive.
    Running {
        h: u64,
        pid: Option<u32>,
        group: Option<u32>,
    },
    /// `latest.json` says RUNNING but nothing of it is alive (the drill process died): reported, never blocking.
    Stale { h: u64 },
}

/// The drill's recorded pipeline group, strictly: Some(pgid) when it is alive OR cannot be inspected (r30 round 4:
/// "cannot tell" is never "stopped").
fn group_not_proven_gone(cfg: &Config, h: u64) -> Option<u32> {
    group_not_proven_gone_at(&journal_dir_of(cfg), h)
}

fn journal_dir_of(cfg: &Config) -> PathBuf {
    cfg.journal_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .to_path_buf()
}

fn group_not_proven_gone_at(journal_dir: &Path, h: u64) -> Option<u32> {
    let pgid = crate::ops::hook_pgid_path(
        &journal_dir
            .join("drill")
            .join(h.to_string())
            .join("drill.jsonl"),
    );
    match crate::ops::recorded_group_status(&pgid) {
        Some((g, crate::ops::GroupStatus::Live(_) | crate::ops::GroupStatus::Unknown(_))) => {
            Some(g)
        }
        _ => None,
    }
}

/// Read `latest.json` and tell a live drill from a stale record. A recorded pipeline group that is alive, or that
/// cannot be inspected, makes the drill RUNNING whatever the record says (r30 round 4: work that survived a failed
/// or incomplete stop keeps blocking a ceremony).
pub fn activity(cfg: &Config) -> DrillActivity {
    activity_at(&journal_dir_of(cfg))
}

/// [`activity`] for the drill whose config keeps its journal in `journal_dir` (r30 round 6: a ceremony under
/// another journal directory evaluates a drill named by the producer's work marker).
pub fn activity_at(journal_dir: &Path) -> DrillActivity {
    let Ok(text) = std::fs::read_to_string(journal_dir.join("drill").join("latest.json")) else {
        return DrillActivity::Idle;
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return DrillActivity::Idle;
    };
    if v["kind"].as_str() != Some(DRILL_KIND) {
        return DrillActivity::Idle;
    }
    let Some(h) = v["h"].as_u64() else {
        return DrillActivity::Idle;
    };
    // r30 round 5 (review R5-3): an export container that was started and not verified gone may still be running
    // (killing the docker CLI does not stop a daemon-managed container): the drill counts as running.
    let container_live = container_not_verified_gone(&v);
    if v["state"].as_str() != Some("RUNNING") {
        return match group_not_proven_gone_at(journal_dir, h) {
            Some(g) => DrillActivity::Running {
                h,
                pid: None,
                group: Some(g),
            },
            None if container_live => DrillActivity::Running {
                h,
                pid: None,
                group: None,
            },
            None => DrillActivity::Idle,
        };
    }
    let pid = v["pid"]
        .as_u64()
        .and_then(|p| u32::try_from(p).ok())
        .filter(|p| *p > 1)
        .filter(
            |p| match (crate::ops::process_start(*p), v["pid_start"].as_str()) {
                (Some(now), Some(want)) => now == want,
                _ => false,
            },
        );
    let group = group_not_proven_gone_at(journal_dir, h);
    if pid.is_some() || group.is_some() || container_live {
        DrillActivity::Running { h, pid, group }
    } else {
        DrillActivity::Stale { h }
    }
}

/// The drill's view records an export container (`export_container`) that has not been verified gone.
fn container_not_verified_gone(v: &Value) -> bool {
    v["export_container"].as_str().is_some() && v["export_container_gone"].as_bool() != Some(true)
}

/// Why a fresh ceremony must not ARM now (a drill is running on this box), if it must not.
pub fn ceremony_blocker(cfg: &Config) -> Option<String> {
    match activity(cfg) {
        DrillActivity::Running { h, pid, group } => Some(format!(
            "a readiness drill at H = {h} is RUNNING on this box (pid {}, pipeline process group {}): it competes for \
             CPU, RAM (up to ~14 GB on mainnet) and disk at the worst moment. Wait for it, or stop it with \
             `pulse-cutover drill --cancel`",
            pid.map_or("-".into(), |p| p.to_string()),
            group.map_or("-".into(), |g| g.to_string())
        )),
        _ => None,
    }
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
    /// The one snapshot request this drill scheduled: its height and the producer API's request id (or, for
    /// `--cancel` of a dead drill, the recorded request it was allowed to clean up). The guard only ever removes a
    /// request by this id, never by height (r30 round 2: a ceremony may hold a request at the same height).
    scheduled: RefCell<Option<(u64, Option<u64>)>>,
    /// r30 round 4/5: the only programs a drill may run (by argument vector), each with the sha256 its content must
    /// have at the moment it is run: the pinned docker CLI and the three pinned tools. Anything else, and any shell
    /// command, is refused.
    programs: RefCell<Vec<PinnedProgram>>,
    /// r30 round 5: every docker run must talk to this local daemon (`--host unix://<socket>` right after argv[0]).
    docker_host: RefCell<Option<String>>,
}

/// A program a drill may run: its absolute path and the sha256 its content must have when it is run.
#[derive(Debug, Clone)]
pub struct PinnedProgram {
    pub path: String,
    pub sha256: String,
    /// The docker CLI (its argument vectors must name the local daemon).
    pub docker: bool,
}

impl<'a, O: ChainOps> DrillGuard<'a, O> {
    pub fn new(inner: &'a O) -> Self {
        DrillGuard {
            inner,
            violations: RefCell::new(vec![]),
            scheduled: RefCell::new(None),
            programs: RefCell::new(vec![]),
            docker_host: RefCell::new(None),
        }
    }

    /// Allow exactly these pinned programs (`argv[0]`) for [`ChainOps::run_argv_cancellable`]; docker argument
    /// vectors must then name `docker_host` (`unix://<socket>`).
    pub fn allow_programs(&self, programs: Vec<PinnedProgram>, docker_host: Option<String>) {
        *self.programs.borrow_mut() = programs;
        *self.docker_host.borrow_mut() = docker_host;
    }

    /// `--cancel` of a drill whose process died: allow removing exactly the request that drill recorded.
    pub fn allow_unschedule_request(&self, h: u64, id: u64) {
        *self.scheduled.borrow_mut() = Some((h, Some(id)));
    }

    /// The request this guard scheduled (or was allowed to clean up): (height, request id).
    pub fn scheduled_request(&self) -> Option<(u64, Option<u64>)> {
        *self.scheduled.borrow()
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
        self.schedule_snapshot_request(height).map(|_| ())
    }
    fn schedule_snapshot_request(&self, height: u64) -> Result<Option<u64>, String> {
        let id = self.inner.schedule_snapshot_request(height)?;
        *self.scheduled.borrow_mut() = Some((height, id));
        Ok(id)
    }
    fn unschedule_snapshot(&self, height: u64) -> Result<bool, String> {
        // By height it could remove someone else's request at the same height (a ceremony's): never.
        self.refuse(&format!(
            "unschedule_snapshot({height}) by height (a drill only removes its own request, by id)"
        ))
    }
    fn unschedule_snapshot_request(&self, id: u64, height: u64) -> Result<bool, String> {
        if !matches!(*self.scheduled.borrow(), Some((mh, Some(mine))) if mine == id && mh == height)
        {
            return self.refuse(&format!(
                "unschedule_snapshot_request({id} at {height}) of a request this drill did not schedule"
            ));
        }
        self.inner.unschedule_snapshot_request(id, height)
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
    fn run_long(&self, _cmd: &str) -> Result<String, String> {
        self.refuse("run_long (a drill never runs a shell command)")
    }
    fn run_long_cancellable(
        &self,
        _cmd: &str,
        _cancel: &mut dyn FnMut() -> bool,
    ) -> Result<Option<String>, String> {
        self.refuse("run_long_cancellable (a drill never runs a shell command)")
    }
    fn run_argv_cancellable(
        &self,
        argv: &[String],
        cancel: &mut dyn FnMut() -> bool,
    ) -> Result<Option<String>, String> {
        let prog = argv.first().cloned().unwrap_or_default();
        let Some(pin) = self
            .programs
            .borrow()
            .iter()
            .find(|p| p.path == prog)
            .cloned()
        else {
            return self.refuse(&format!(
                "running {prog:?} (not one of the drill's validated programs)"
            ));
        };
        if pin.docker {
            let want = self.docker_host.borrow().clone();
            let named = argv.get(1).map(String::as_str) == Some("--host")
                && want.is_some()
                && argv.get(2) == want.as_ref();
            if !named {
                return self.refuse(&format!(
                    "running docker without `--host {}` (the local daemon)",
                    want.unwrap_or_else(|| "?".into())
                ));
            }
        }
        // r30 round 5: the content is checked again right before the run (the config-time check could be stale).
        // Residual: the file could change between this hash and the exec; it must be a single-named file not
        // writable by group or others (checked again here), so only its owner could, and the owner already could
        // run anything. Exec-through-the-hashed-fd is not portable (no fexecve on macOS).
        if let Some(why) = pinned_program_problem(&prog, Path::new(&prog), Some(&pin.sha256)) {
            return Err(format!("readiness drill: refusing to run {prog}: {why}"));
        }
        self.inner.run_argv_cancellable(argv, cancel)
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
        None => p.push("a drill needs an [upstream] section (export_image@sha256, export_tools_dir, chain_state_db_size_mb, \
            docker_bin + docker_bin_sha256, import_bin/compare_bin/fingerprint_bin + their _sha256 pins)".into()),
        Some(u) if u.compare_bin.is_none() => {
            p.push("a drill needs [upstream] compare_bin: the table comparison is half of what it measures".into())
        }
        _ => {}
    }
    if cfg.snapshot.dir.is_none() {
        p.push("a drill picks up nodeos's scheduled snapshot: set [snapshot] dir".into());
    }
    p.extend(exec_problems(cfg));
    if let Some(why) = producer_api_problem(&cfg.source.producer_api_url) {
        p.push(why);
    }
    p
}

/// r30 round 4: a drill never runs free-form shell. Rounds 1–3 tried to scan the configured conversion commands and
/// any wrapper scripts for references to a hook or a stop/create/ignite command; every round found another way past
/// the scan (quotes, `cd`, `..` through a symlink, hard links, variables, PATH). So a drill does not run configured
/// text at all: the export is a fixed `docker run` argument vector built from explicit, validated fields, and the
/// three tools must be native executables run by argument vector. Nothing a config says can add a command.
fn exec_problems(cfg: &Config) -> Vec<String> {
    let mut p = vec![];
    let Some(up) = &cfg.upstream else { return p };
    match &up.export_image {
        None => p.push(
            "a drill runs the export as a fixed `docker run` (never `export_cmd`, which is shell): set [upstream] \
             export_image to the Leap 5.0.3 sidecar image pinned by digest (`name@sha256:<64 hex>`)"
                .into(),
        ),
        Some(img) if !valid_image(img) => p.push(format!(
            "upstream.export_image {img:?} is not a plain image reference (letters, digits, `._/:@-`, not starting \
             with `-`, at most 255 characters)"
        )),
        Some(img) if image_digest(img).is_none() => p.push(format!(
            "upstream.export_image {img:?} is not pinned by digest: a drill runs only `name@sha256:<64 lowercase hex>` \
             (a tag can be moved to other content)"
        )),
        _ => {}
    }
    match &up.docker_bin {
        None => p.push("a drill runs docker by absolute path only: set [upstream] docker_bin and docker_bin_sha256".into()),
        Some(d) => p.extend(pinned_program_problem("upstream.docker_bin", d, up.docker_bin_sha256.as_ref())),
    }
    p.extend(socket_problem(&docker_socket(up)));
    match &up.export_tools_dir {
        None => p.push(
            "a drill mounts the export tools read-only: set [upstream] export_tools_dir (the absolute directory holding \
             export.sh)"
                .into(),
        ),
        Some(d) => p.extend(tools_dir_problems(d)),
    }
    match up.chain_state_db_size_mb {
        None => p.push("a drill passes the chain-state size to export.sh: set [upstream] chain_state_db_size_mb".into()),
        Some(n) if !(1..=1_048_576).contains(&n) => {
            p.push(format!("upstream.chain_state_db_size_mb = {n} is out of range (1..=1048576)"))
        }
        _ => {}
    }
    p.extend(pinned_program_problem(
        "upstream.import_bin",
        &up.import_bin,
        up.import_bin_sha256.as_ref(),
    ));
    p.extend(pinned_program_problem(
        "upstream.fingerprint_bin",
        &up.fingerprint_bin,
        up.fingerprint_bin_sha256.as_ref(),
    ));
    if let Some(c) = &up.compare_bin {
        p.extend(pinned_program_problem(
            "upstream.compare_bin",
            c,
            up.compare_bin_sha256.as_ref(),
        ));
    }
    p
}

/// The docker daemon socket a drill talks to (`/var/run/docker.sock` unless configured).
pub fn docker_socket(up: &crate::config::Upstream) -> PathBuf {
    up.docker_socket
        .clone()
        .unwrap_or_else(|| PathBuf::from("/var/run/docker.sock"))
}

/// `name@sha256:<64 lowercase hex>` → the digest; anything else (a tag, a short or upper-case digest) → None.
pub fn image_digest(img: &str) -> Option<String> {
    let (name, d) = img.rsplit_once("@sha256:")?;
    (!name.is_empty()
        && d.len() == 64
        && d.chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)))
    .then(|| d.to_string())
}

/// A lowercase 64-hex sha256 pin.
pub fn valid_pin(s: &str) -> bool {
    s.len() == 64
        && s.chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
}

/// r30 round 5 (review R5-2): a native-executable check alone admits `/bin/bash` as a "tool" (it would then run the
/// export-produced log as shell). A drill runs a program only when its CONTENT matches a configured pin: the plain
/// file and native-executable checks, plus sha256(file) == pin. Checked at config time and right before each run.
pub fn pinned_program_problem(key: &str, path: &Path, pin: Option<&String>) -> Option<String> {
    if let Some(why) = native_executable_problems(key, path).into_iter().next() {
        return Some(why);
    }
    let Some(pin) = pin else {
        return Some(format!(
            "{key} {} has no sha256 pin ({key}_sha256): a drill runs only pinned content",
            path.display()
        ));
    };
    if !valid_pin(pin) {
        return Some(format!(
            "{key}_sha256 {pin:?} is not 64 lowercase hex characters"
        ));
    }
    match crate::verify::sha256_file(path) {
        Ok((sha, _)) if sha.eq_ignore_ascii_case(pin) => None,
        Ok((sha, _)) => Some(format!(
            "{key} {} has sha256 {sha}, not its pin {pin}: refused",
            path.display()
        )),
        Err(e) => Some(format!("{key} {}: cannot hash it: {e}", path.display())),
    }
}

/// The docker daemon socket must be an absolute path to a unix socket (not a link): the drill only ever talks to
/// the local daemon.
fn socket_problem(p: &Path) -> Vec<String> {
    if !p.is_absolute() {
        return vec![format!(
            "upstream.docker_socket {} is not an absolute path",
            p.display()
        )];
    }
    match std::fs::symlink_metadata(p) {
        Err(e) => vec![format!("upstream.docker_socket {}: {e}", p.display())],
        Ok(m) if m.file_type().is_symlink() => vec![format!(
            "upstream.docker_socket {} is a symlink: refused",
            p.display()
        )],
        Ok(m) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                if !m.file_type().is_socket() {
                    return vec![format!(
                        "upstream.docker_socket {} is not a unix socket",
                        p.display()
                    )];
                }
            }
            let _ = m;
            vec![]
        }
    }
}

/// A plain container image reference: no shell syntax, no option-looking first character.
pub fn valid_image(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && !s.starts_with('-')
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | ':' | '@' | '-'))
}

/// `name` = a single-named regular file (not a symlink, not hard-linked), not writable by group or others.
fn plain_file_problem(key: &str, path: &Path) -> Option<String> {
    let m = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) => return Some(format!("{key} {}: {e}", path.display())),
    };
    if m.file_type().is_symlink() {
        return Some(format!(
            "{key} {} is a symlink: a drill only runs a file by its own name",
            path.display()
        ));
    }
    if !m.is_file() {
        return Some(format!("{key} {} is not a regular file", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if m.nlink() != 1 {
            return Some(format!(
                "{key} {} has {} names (a hard link): refused",
                path.display(),
                m.nlink()
            ));
        }
        if m.mode() & 0o022 != 0 {
            return Some(format!(
                "{key} {} is writable by group or others: refused",
                path.display()
            ));
        }
    }
    None
}

/// A drill tool must be an absolute path to a native executable (ELF, or Mach-O on macOS), never a script.
fn native_executable_problems(key: &str, path: &Path) -> Vec<String> {
    if !path.is_absolute() {
        return vec![format!(
            "{key} {} is not an absolute path: a drill runs its tools by absolute path only",
            path.display()
        )];
    }
    if let Some(why) = plain_file_problem(key, path) {
        return vec![why];
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path)
            .map(|m| m.permissions().mode() & 0o111 == 0)
            .unwrap_or(true)
        {
            return vec![format!("{key} {} is not executable", path.display())];
        }
    }
    let mut magic = [0u8; 4];
    let read = std::fs::File::open(path).and_then(|mut f| {
        use std::io::Read;
        f.read_exact(&mut magic)
    });
    if read.is_err() {
        return vec![format!("{key} {}: cannot read its header", path.display())];
    }
    if !is_native_executable_magic(&magic) {
        let what = if magic.starts_with(b"#!") {
            "a script"
        } else {
            "not a native executable"
        };
        return vec![format!(
            "{key} {} is {what}: a drill runs only native executables (ELF or Mach-O) by argument vector, never a \
             script or shell",
            path.display()
        )];
    }
    vec![]
}

/// ELF, or Mach-O (32/64-bit, either byte order, universal).
pub fn is_native_executable_magic(m: &[u8; 4]) -> bool {
    m == b"\x7fELF"
        || matches!(
            u32::from_be_bytes(*m),
            0xfeed_face | 0xfeed_facf | 0xcefa_edfe | 0xcffa_edfe | 0xcafe_babe
        )
}

/// The export tools directory: absolute, a real directory (not a symlink), holding `export.sh` as a plain file.
fn tools_dir_problems(d: &Path) -> Vec<String> {
    if !d.is_absolute() {
        return vec![format!(
            "upstream.export_tools_dir {} is not an absolute path",
            d.display()
        )];
    }
    match std::fs::symlink_metadata(d) {
        Err(e) => return vec![format!("upstream.export_tools_dir {}: {e}", d.display())],
        Ok(m) if m.file_type().is_symlink() => {
            return vec![format!(
                "upstream.export_tools_dir {} is a symlink: refused",
                d.display()
            )]
        }
        Ok(m) if !m.is_dir() => {
            return vec![format!(
                "upstream.export_tools_dir {} is not a directory",
                d.display()
            )]
        }
        _ => {}
    }
    plain_file_problem(
        "export.sh in upstream.export_tools_dir",
        &d.join("export.sh"),
    )
    .into_iter()
    .collect()
}

/// r30 round 4: the producer API a snapshot request is bound to must be ONE unambiguous endpoint: a query,
/// fragment or userinfo could route the same normalised base to different nodes behind a proxy.
fn producer_api_problem(url: &str) -> Option<String> {
    match url::Url::parse(url.trim()) {
        Err(e) => Some(format!("source.producer_api_url {url:?} is not a URL: {e}")),
        Ok(u) if u.query().is_some() || u.fragment().is_some() || !u.username().is_empty() || u.password().is_some() => {
            Some(format!(
                "source.producer_api_url {url:?} carries a query, fragment or credentials: a drill binds its snapshot \
                 request to scheme, host, port and path only, so the endpoint must be exactly that"
            ))
        }
        Ok(_) => None,
    }
}

/// `p` made absolute with symlinks resolved, also when it (or its tail) does not exist yet: the longest existing
/// ancestor is canonicalized and the rest appended.
fn canonical(p: &Path) -> PathBuf {
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(p)
    };
    let mut tail = vec![];
    let mut cur = abs.as_path();
    loop {
        if let Ok(c) = std::fs::canonicalize(cur) {
            let mut out = c;
            for t in tail.iter().rev() {
                out.push(t);
            }
            return out;
        }
        match (cur.parent(), cur.file_name()) {
            (Some(parent), Some(name)) => {
                tail.push(name.to_os_string());
                cur = parent;
            }
            _ => return abs,
        }
    }
}

/// r30 round 6: the directory holding the per-producer lock and the drill work markers. Agent-owned and never
/// nodeos's `snapshot.dir` (round 5 put the lock there: an agent without write access to nodeos's directory, a
/// read-only mount, or an operator clearing old snapshots broke it, and an agent creating that directory as root
/// could stop nodeos writing the cut). `[ceremony] lock_dir` when set; else `/var/lib/pulse-cutover/locks` when it
/// is or can be made writable; else `locks/` beside the journal directory (its parent's `locks`). Every agent, drill and loop on one box must resolve
/// the same directory (set `lock_dir` explicitly when they run as different users).
pub fn lock_dir(cfg: &Config) -> PathBuf {
    if let Some(d) = &cfg.ceremony.lock_dir {
        return d.clone();
    }
    let default = PathBuf::from("/var/lib/pulse-cutover/locks");
    if std::fs::create_dir_all(&default).is_ok() && dir_writable(&default) {
        return default;
    }
    let journal_dir = canonical(&journal_dir_of(cfg));
    journal_dir
        .parent()
        .map(|p| p.join("locks"))
        .unwrap_or_else(|| journal_dir.join("locks"))
}

fn dir_writable(d: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(d.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: a valid NUL-terminated path; access(2) only reads it.
    unsafe { libc::access(c.as_ptr(), libc::W_OK | libc::X_OK) == 0 }
}

/// The shared resource a ceremony, a drill and a loop on one producer have in common: nodeos's snapshot directory,
/// canonicalised (so `localhost` / `127.0.0.1` / trailing-slash spellings of the producer API, or a symlinked path,
/// collapse to one key). A config without a snapshot directory keys on its journal directory (same-directory
/// exclusion only).
pub fn producer_resource_key(cfg: &Config) -> String {
    match &cfg.snapshot.dir {
        Some(d) => format!("snapshot-dir:{}", canonical(d).display()),
        None => format!(
            "journal-dir:{}",
            canonical(cfg.journal_path.parent().unwrap_or(Path::new("."))).display()
        ),
    }
}

/// The per-producer lock (base path; the lock file is `<base>.lock`) in `lock_dir`. Lock order everywhere: journal
/// lock, then drill-root lock, then this one.
pub fn producer_lock_base(cfg: &Config) -> PathBuf {
    use sha2::{Digest, Sha256};
    let key = hex::encode(Sha256::digest(producer_resource_key(cfg).as_bytes()));
    lock_dir(cfg).join(format!("producer-{}", &key[..16]))
}

/// r30 round 6 (review R6-5): the drill work marker beside the producer lock (`<producer lock base>.work.json`).
/// Written (fresh, never through a link) before a drill's work starts, naming its journal directory and H; removed
/// only once that work is verified gone. The producer lock dies with the drill process, but a surviving export
/// container or pipeline group does not: through the marker a ceremony under ANY journal directory still sees it.
pub fn work_marker_path(cfg: &Config) -> PathBuf {
    PathBuf::from(format!("{}.work.json", producer_lock_base(cfg).display()))
}

/// What the producer's work marker says, for a ceremony on that producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerWork {
    /// No marker.
    None,
    /// A drill (under `journal_dir`) whose work is alive or cannot be shown stopped.
    Live {
        journal_dir: PathBuf,
        h: u64,
        pid: Option<u32>,
        group: Option<u32>,
    },
    /// A marker that cannot be read or parsed, or whose drill cannot be shown finished: unknown is never "stopped".
    Unknown(String),
    /// The marker's drill is verified finished (its work is gone): a leftover marker. `drill_id`: the drill that
    /// wrote it (r30 round 7), None for a marker from before round 7.
    Done {
        journal_dir: PathBuf,
        drill_id: Option<String>,
    },
}

/// The work marker's raw fields (r30 round 7): who wrote it and what work it may have left.
#[derive(Debug, Clone, Default)]
pub struct MarkerRecord {
    pub journal_dir: Option<PathBuf>,
    pub h: Option<u64>,
    pub drill_id: Option<String>,
    pub pid: Option<u32>,
    pub pid_start: Option<String>,
    pub container: Option<String>,
}

/// Read the producer's work marker. Ok(None): no marker. Err: present but unreadable.
pub fn read_marker(cfg: &Config) -> Result<Option<MarkerRecord>, String> {
    if !lock_dir(cfg).is_dir() {
        return Ok(None);
    }
    let path = work_marker_path(cfg);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let v: Value =
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))?;
    Ok(Some(MarkerRecord {
        journal_dir: v["journal_dir"].as_str().map(PathBuf::from),
        h: v["h"].as_u64(),
        drill_id: v["drill_id"].as_str().map(str::to_string),
        pid: v["pid"].as_u64().and_then(|p| u32::try_from(p).ok()),
        pid_start: v["pid_start"].as_str().map(str::to_string),
        container: v["export_container"].as_str().map(str::to_string),
    }))
}

/// The marker's own drill process is alive (same pid AND the same start time).
fn marker_process_alive(m: &MarkerRecord) -> bool {
    match (m.pid.filter(|p| *p > 1), m.pid_start.as_deref()) {
        (Some(p), Some(want)) => crate::ops::process_start(p).as_deref() == Some(want),
        _ => false,
    }
}

/// The drill id `latest.json` under `journal_dir` names, if any.
fn latest_drill_id_at(journal_dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(journal_dir.join("drill").join("latest.json")).ok()?;
    serde_json::from_str::<Value>(&text).ok()?["drill_id"]
        .as_str()
        .map(str::to_string)
}

pub fn marker_work(cfg: &Config) -> MarkerWork {
    // No usable lock directory: no drill can have written a marker there (an unusable lock_dir is the agent's
    // problem, journaled by the ceremony, never evidence of a drill).
    if !lock_dir(cfg).is_dir() {
        return MarkerWork::None;
    }
    let path = work_marker_path(cfg);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return MarkerWork::None,
        Err(e) => return MarkerWork::Unknown(format!("read {}: {e}", path.display())),
    };
    let v: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => return MarkerWork::Unknown(format!("parse {}: {e}", path.display())),
    };
    let (Some(dir), Some(mh)) = (v["journal_dir"].as_str(), v["h"].as_u64()) else {
        return MarkerWork::Unknown(format!("{} names no journal_dir / h", path.display()));
    };
    let journal_dir = PathBuf::from(dir);
    let marker_id = v["drill_id"].as_str().map(str::to_string);
    // r30 round 7 (review R7-3): the marker is judged by ITS drill. Only when `latest.json` names the same drill id
    // does its state speak for the marker; a marker whose drill has not published (or published under another id)
    // is live while that drill's process or pipeline group is, and UNKNOWN otherwise (never "done": its container may
    // exist). A marker from before round 7 (no id) keeps the old reading.
    if let Some(id) = &marker_id {
        if latest_drill_id_at(&journal_dir).as_deref() != Some(id.as_str()) {
            let rec = MarkerRecord {
                pid: v["pid"].as_u64().and_then(|p| u32::try_from(p).ok()),
                pid_start: v["pid_start"].as_str().map(str::to_string),
                ..Default::default()
            };
            let group = group_not_proven_gone_at(&journal_dir, mh);
            if marker_process_alive(&rec) || group.is_some() {
                return MarkerWork::Live {
                    journal_dir,
                    h: mh,
                    pid: rec.pid.filter(|_| marker_process_alive(&rec)),
                    group,
                };
            }
            return MarkerWork::Unknown(format!(
                "the work marker's drill {id} at H = {mh} under {} is not running and never published its result: its                  export may have started; verify and remove it with `pulse-cutover drill --clear-marker`",
                journal_dir.display()));
        }
    }
    match activity_at(&journal_dir) {
        DrillActivity::Running { h, pid, group } => MarkerWork::Live {
            journal_dir,
            h,
            pid,
            group,
        },
        _ => MarkerWork::Done {
            journal_dir,
            drill_id: marker_id,
        },
    }
}

fn write_work_marker(cfg: &Config, h: u64, drill_id: &str, container: &str) -> Result<(), String> {
    let me = std::process::id();
    // r30 round 7: the marker names its drill (`drill_id`, also in that drill's latest.json) and the export container
    // it will start, so it is cleaned up only by its own drill and can be verified gone without that drill's config.
    let body = json!({"kind": "readiness_drill_work", "journal_dir": canonical(&journal_dir_of(cfg)), "h": h,
        "drill_id": drill_id, "export_container": container,
        "pid": me, "pid_start": crate::ops::process_start(me), "producer_resource": producer_resource_key(cfg)});
    let path = work_marker_path(cfg);
    // Replace a leftover marker of THIS producer only after checking it is done (the caller holds the producer lock).
    match marker_work(cfg) {
        MarkerWork::None | MarkerWork::Done { .. } => {}
        MarkerWork::Live { journal_dir, h, .. } => return Err(format!(
            "a readiness drill's work at H = {h} under {} is still running on this producer: stop it first \
             (`pulse-cutover drill --cancel` with that config)", journal_dir.display())),
        MarkerWork::Unknown(why) => return Err(format!("this producer's drill work marker is unreadable or its drill \
             cannot be shown finished ({why}): verify and remove it with `pulse-cutover drill --clear-marker` before \
             another drill")),
    }
    crate::ops::write_fresh_in(&lock_dir(cfg), &path, body.to_string().as_bytes())
        .map_err(|e| format!("cannot write the drill work marker {}: {e}", path.display()))
}

/// Remove the work marker once ITS drill's work is verified gone (r30 round 7: only the marker whose `drill_id` is
/// `drill_id`; never a newer drill's). The caller holds the producer lock, so no other drill can write a marker
/// between the check and the removal. Called by the drill itself (or by `--cancel` for it): when its latest.json
/// never named this id, the drill failed before any work started and the marker is removed; otherwise only once
/// that record is not running (its pipeline group and export container verified gone).
pub fn clear_work_marker_if_done(cfg: &Config, drill_id: &str) {
    let Ok(Some(m)) = read_marker(cfg) else {
        return;
    };
    if m.drill_id.as_deref() != Some(drill_id) {
        return; // someone else's marker (or one from before round 7): never ours to remove
    }
    let jd = journal_dir_of(cfg);
    if m.journal_dir.as_ref().map(|d| canonical(d)) != Some(canonical(&jd)) {
        return;
    }
    let published = latest_drill_id_at(&jd).as_deref() == Some(drill_id);
    if !published || !matches!(activity_at(&jd), DrillActivity::Running { .. }) {
        let _ = std::fs::remove_file(work_marker_path(cfg));
    }
}

/// A fresh, random drill id (32 hex).
fn new_drill_id() -> String {
    let mut b = [0u8; 16];
    if getrandom_fill(&mut b).is_err() {
        use sha2::{Digest, Sha256};
        let d = Sha256::digest(
            format!("{}:{:?}", std::process::id(), std::time::SystemTime::now()).as_bytes(),
        );
        b.copy_from_slice(&d[..16]);
    }
    hex::encode(b)
}

/// A ceremony's view: Some(why it must not ARM) while another directory's drill work on this producer is live or
/// unknown. A marker naming the ceremony's own journal directory is left to the ceremony's own activity check.
pub fn foreign_marker_blocker(cfg: &Config) -> Option<String> {
    let mine = canonical(&journal_dir_of(cfg));
    match marker_work(cfg) {
        MarkerWork::Live { journal_dir, .. } if canonical(&journal_dir) == mine => None,
        MarkerWork::Live { journal_dir, h, pid, group } => Some(format!(
            "a readiness drill's work at H = {h} (under {}, pid {}, process group {}) is still running on this producer: \
             wait for it, or stop it with `pulse-cutover drill --cancel` using that drill's config",
            journal_dir.display(), pid.map_or("-".into(), |p| p.to_string()), group.map_or("-".into(), |g| g.to_string()))),
        MarkerWork::Unknown(why) => Some(format!(
            "this producer's drill work marker cannot be read ({why}): a drill's work may be running; check {} by hand",
            work_marker_path(cfg).display())),
        _ => None,
    }
}

/// The drill root's own lock file (`<journal dir>/drill/drill.lock`): one drill per drill root, whatever journal
/// file name a config uses (r30 round 2: two configs sharing a journal directory share `drill/latest.json`).
pub fn root_lock_base(cfg: &Config) -> PathBuf {
    drill_root(cfg).join("drill")
}

/// r30 review (c), round 2: EVERY path a drill writes must not overlap, even through a symlink, the ceremony's
/// staged snapshot, journal, work dir or nodeos's snapshot dir: the whole drill root (each drill's work files,
/// which are rewritten and deleted, `latest.json` and its temp file, the cancel and lock files) and the journal's
/// lock file the drill takes. Checked before the drill creates anything.
pub fn path_overlap_problems(cfg: &Config, h: u64) -> Vec<String> {
    let drill = canonical(&drill_root(cfg));
    let journal_lock = canonical(Path::new(&format!("{}.lock", cfg.journal_path.display())));
    let overlaps = |other: &Path| {
        let o = canonical(other);
        o.starts_with(&drill) || drill.starts_with(&o) || o == journal_lock
    };
    let mut p = vec![];
    let mut check = |name: &str, path: &Path| {
        if overlaps(path) {
            p.push(format!(
                "{name} {} overlaps the drill's files under {} (work files rewritten and deleted, latest.json, lock and \
                 cancel files) or the journal lock it takes: refused",
                path.display(),
                drill.display()
            ));
        }
    };
    check("snapshot.staged_path", &cfg.snapshot.staged_path);
    if canonical(&cfg.journal_path).starts_with(&drill) {
        check("journal_path", &cfg.journal_path);
    }
    if let Some(up) = &cfg.upstream {
        check("upstream.work_dir", &up.work_dir);
    }
    if let Some(d) = &cfg.snapshot.dir {
        check("snapshot.dir", d);
    }
    p.extend(linked_output_problems(cfg, h));
    p
}

/// r30 round 3: resolving the drill root is not enough: a file BENEATH it (`latest.json.tmp`, the log, the cancel
/// or lock file) can itself be a symlink (or a hard link) to a protected file and be written through. Every path a
/// drill or `--cancel` writes or opens for writing must be absent, or a plain single-named file / directory.
fn linked_output_problems(cfg: &Config, h: u64) -> Vec<String> {
    let root = drill_root(cfg);
    let dir = drill_dir(cfg, h);
    let mut paths = vec![
        root.join("latest.json"),
        root.join("latest.json.tmp"),
        PathBuf::from(format!("{}.lock", root_lock_base(cfg).display())),
        PathBuf::from(format!("{}.lock", cfg.journal_path.display())),
        dir.clone(),
        dir.join("drill.jsonl"),
        dir.join("cancel"),
        dir.join("work"),
        dir.join("export.cid"),
        pgid_file(cfg, h),
        PathBuf::from(format!("{}.lock", producer_lock_base(cfg).display())),
    ];
    for name in [
        format!("compare-report-{h}.json"),
        format!("checkpoint-{h}.bin.manifest.json"),
    ] {
        paths.push(dir.join(name));
    }
    let mut p = vec![];
    for path in paths {
        let Ok(m) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if m.file_type().is_symlink() {
            p.push(format!(
                "{} is a symlink (to {}): a drill never writes through a link (it could overwrite a protected file); \
                 remove it",
                path.display(),
                std::fs::read_link(&path).map(|t| t.display().to_string()).unwrap_or_else(|_| "?".into())
            ));
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if m.is_file() && m.nlink() > 1 {
                p.push(format!(
                    "{} has {} names (a hard link): a drill never writes a file that may be another path's; remove it",
                    path.display(),
                    m.nlink()
                ));
            }
        }
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

// ------------------------------------------------------------------------------------------- safe writes ----

/// r30 round 3: a drill never writes THROUGH a link. A path a drill writes may already exist as a symlink (or a
/// hard link) to a protected file (the staged snapshot, the journal); following it would overwrite that file.
#[cfg(unix)]
fn nofollow(o: &mut std::fs::OpenOptions) -> &mut std::fs::OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    o.custom_flags(libc::O_NOFOLLOW)
}
#[cfg(not(unix))]
fn nofollow(o: &mut std::fs::OpenOptions) -> &mut std::fs::OpenOptions {
    o
}

/// A fresh file at `path` with `bytes`: whatever name was there (a stale file, a symlink, a hard link) is unlinked
/// first, which never touches the file it pointed at, then a NEW inode is created (O_EXCL, O_NOFOLLOW) and fsynced.
fn write_fresh(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    if std::fs::symlink_metadata(path).is_ok() {
        std::fs::remove_file(path).map_err(|e| format!("replace {}: {e}", path.display()))?;
    }
    let mut f = nofollow(std::fs::OpenOptions::new().write(true).create_new(true))
        .open(path)
        .map_err(|e| format!("create {}: {e}", path.display()))?;
    f.write_all(bytes)
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    f.sync_all()
        .map_err(|e| format!("fsync {}: {e}", path.display()))
}

/// Append to the drill's own log without following a link, and refuse a file with other names (a hard link).
fn append_nofollow(path: &Path, line: &str) -> Result<(), String> {
    use std::io::Write;
    let mut f = nofollow(std::fs::OpenOptions::new().create(true).append(true))
        .open(path)
        .map_err(|e| format!("open drill log {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let m = f
            .metadata()
            .map_err(|e| format!("stat drill log {}: {e}", path.display()))?;
        if m.nlink() > 1 {
            return Err(format!(
                "drill log {} has other names (a hard link): refused",
                path.display()
            ));
        }
    }
    writeln!(f, "{line}").map_err(|e| format!("write drill log: {e}"))?;
    f.sync_all().map_err(|e| format!("fsync drill log: {e}"))
}

/// The producer API a snapshot request was made on, normalised for comparison (scheme, host, port, path; no
/// trailing slash). A request id is only meaningful on the node that issued it (r30 round 3).
pub fn normalized_endpoint(url: &str) -> String {
    match url::Url::parse(url.trim()) {
        Ok(u) => format!(
            "{}://{}{}{}",
            u.scheme(),
            u.host_str().unwrap_or("").to_ascii_lowercase(),
            u.port_or_known_default()
                .map(|p| format!(":{p}"))
                .unwrap_or_default(),
            u.path().trim_end_matches('/')
        ),
        Err(_) => url.trim().trim_end_matches('/').to_string(),
    }
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
        let line = json!({"kind": DRILL_KIND, "ts_ms": now_ms, "stage": stage, "data": data});
        append_nofollow(&self.path, &line.to_string())
    }

    /// Rewrite `latest.json` (the beacon's view) atomically.
    fn publish(&mut self, patch: Value) -> Result<(), String> {
        if let (Some(v), Some(p)) = (self.view.as_object_mut(), patch.as_object()) {
            for (k, val) in p {
                v.insert(k.clone(), val.clone());
            }
        }
        // A fresh temp inode (never written through a link), then rename: rename replaces the NAME `latest.json`,
        // never a file a symlink there pointed at.
        let tmp = self.latest.with_extension("json.tmp");
        write_fresh(
            &tmp,
            &serde_json::to_vec_pretty(&self.view).unwrap_or_default(),
        )?;
        std::fs::rename(&tmp, &self.latest)
            .map_err(|e| format!("rename {}: {e}", self.latest.display()))
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
    /// rc.31: the compare did not match, and EVERY failing table is one of the known PulseVM v1.0.0 compare gaps
    /// (`upstream::KNOWN_COMPARE_DIFFERENCES`) with exactly its recorded signature, and every other required table
    /// matched. Still a mismatch (`compare` stays "MISMATCH"); the CLI exits 7 instead of 5.
    pub compare_known_gaps_only: bool,
}

/// rc.31: classify a compare that did not match. Some(list of matched known differences) only when every failing
/// table is a known v1.0.0 gap whose failure is exactly that gap's signature (`upstream::known_compare_difference`),
/// and no required table is unreported. Anything else (a table outside the list, the right table with another
/// failure, missing output) is None: a real mismatch.
pub fn known_gaps_only(failing: &[String], output: &str, chain_id: &str) -> Option<Vec<Value>> {
    if failing.is_empty() || output.is_empty() {
        return None;
    }
    let mut known = vec![];
    for t in failing {
        if !upstream::KNOWN_COMPARE_DIFFERENCES.contains(&t.as_str()) {
            return None;
        }
        let sig = upstream::known_compare_difference(t, output, chain_id).ok()?;
        known.push(json!({"table": t, "matched": sig}));
    }
    if !upstream::compare_missing_tables(output, failing).is_empty() {
        return None;
    }
    Some(known)
}

/// Run one readiness drill at `opts.height`. `ops` is wrapped in a [`DrillGuard`]; nothing else is used.
pub fn run<O: ChainOps>(
    cfg: &Config,
    ops: &O,
    opts: &DrillOptions,
) -> Result<DrillOutcome, String> {
    run_with_guard(cfg, &DrillGuard::new(ops), opts)
}

/// [`run`] with a caller-made guard. Any violation the guard recorded during the run (or before it) fails the
/// drill, even when the run itself returned Ok.
pub fn run_with_guard<O: ChainOps>(
    cfg: &Config,
    guard: &DrillGuard<'_, O>,
    opts: &DrillOptions,
) -> Result<DrillOutcome, String> {
    // r30 round 7: the work marker is cleaned up inside run_guarded, while the drill still holds the producer lock.
    let res = run_guarded(cfg, guard, opts);
    let v = guard.violations.borrow().clone();
    if !v.is_empty() {
        // Defense in depth: the guard already refused each call; a drill that even tried is a bug to report.
        return Err(format!(
            "readiness drill tried forbidden operations (all refused): {}",
            v.join(", ")
        ));
    }
    res
}

fn run_guarded<O: ChainOps>(
    cfg: &Config,
    ops: &DrillGuard<'_, O>,
    opts: &DrillOptions,
) -> Result<DrillOutcome, String> {
    let problems = config_problems(cfg);
    if !problems.is_empty() {
        return Err(format!(
            "this config cannot run a readiness drill: {}",
            problems.join("; ")
        ));
    }
    // Every path the drill will write is checked before it creates anything (lock files included).
    let overlap = path_overlap_problems(cfg, opts.height);
    if !overlap.is_empty() {
        return Err(overlap.join("; "));
    }
    // r30 review (b): the ceremony's own exclusive journal lock, held for the whole drill: a ceremony (or another
    // drill) cannot start on this journal while it runs, and the state check below cannot race one starting.
    let _lock = Journal::lock(&cfg.journal_path)
        .map_err(|e| format!("a readiness drill needs this journal's lock: {e}"))?;
    // Round 2: and the drill root's own lock, so configs that share a journal directory (and so `latest.json`) but
    // name different journal files cannot run two drills at once.
    let _root_lock = Journal::lock(&root_lock_base(cfg)).map_err(|e| {
        format!(
            "a readiness drill needs the drill directory's lock (another drill is running): {e}"
        )
    })?;
    // Round 5: and the producer's own lock, shared with a ceremony (or drill) on the same producer under ANY journal
    // directory.
    let _producer_lock = Journal::lock(&producer_lock_base(cfg)).map_err(|e| format!(
        "a readiness drill needs this producer's lock (a ceremony or another drill on the same producer API holds it, \
         possibly from another journal directory): {e}"))?;
    if let Some(why) = ceremony_conflict(cfg)? {
        return Err(why);
    }
    if let DrillActivity::Running { h, pid, .. } = activity(cfg) {
        return Err(format!(
            "a readiness drill at H = {h} is already running on this box (pid {}): one drill at a time",
            pid.map_or("-".into(), |p| p.to_string())
        ));
    }
    let h = opts.height;
    // Round 7 (F2): XPR mainnet needs an explicit `[ceremony] lock_dir`, also when the config leaves the chain to
    // discovery: a default that depends on who runs the drill could split it from the ceremony's lock. Checked before
    // the work marker exists, so a refusal leaves nothing behind.
    // r30 round 8 (review R8-7): the discovery must succeed (an unreadable source is refused, never read as "not
    // mainnet"), and the drill reuses this checked answer instead of asking again.
    let info = ops.source_info().map_err(|e| {
        format!("cannot read the source chain before the drill ({e}): refused, nothing started")
    })?;
    if cfg.ceremony.lock_dir.is_none() && crate::config::is_xpr_mainnet(&info.chain_id) {
        return Err("the source is XPR MAINNET: set an explicit [ceremony] lock_dir (the same one the ceremony uses) \
                    before running a drill".into());
    }
    // Round 6: the work marker, before any work starts (the producer lock is held). Round 7: bound to this drill.
    let drill_id = new_drill_id();
    let container = format!("pulse-drill-{h}-{}", container_suffix(h, ops.now_ms()));
    write_work_marker(cfg, h, &drill_id, &container)?;
    let res = run_marked(cfg, ops, opts, &drill_id, &container, info);
    // Still under the journal, drill-root and producer locks (they drop when this function returns).
    clear_work_marker_if_done(cfg, &drill_id);
    res
}

fn run_marked<O: ChainOps>(
    cfg: &Config,
    ops: &DrillGuard<'_, O>,
    opts: &DrillOptions,
    drill_id: &str,
    container: &str,
    info: crate::ops::ChainInfo,
) -> Result<DrillOutcome, String> {
    let h = opts.height;
    if let Some(pinned) = &cfg.ceremony.chain_id {
        if !pinned.eq_ignore_ascii_case(&info.chain_id) {
            return Err(format!(
                "source chain_id {} != configured ceremony.chain_id {pinned}",
                info.chain_id
            ));
        }
    }
    let dir = drill_dir(cfg, h);
    if dir.exists() {
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

    // Claim H atomically: exactly one drill creates this directory.
    std::fs::create_dir_all(drill_root(cfg))
        .map_err(|e| format!("create {}: {e}", drill_root(cfg).display()))?;
    match std::fs::create_dir(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(format!(
                "a drill at H = {h} already ran here ({}); every drill height is used once",
                dir.display()
            ))
        }
        Err(e) => return Err(format!("create {}: {e}", dir.display())),
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
            "drill_id": drill_id,
            "planned_export_container": container,
            "h": h,
            "state": "RUNNING",
            "stage": "scheduled",
            "chain_id": info.chain_id,
            "agent_version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            "pid_start": crate::ops::process_start(std::process::id()),
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
            unschedule_if_pending(ops, h, &mut log);
            // Round 5: a started export container must be stopped and verified gone, or the drill stays RUNNING.
            if let Err(why) = settle_export_container(cfg, ops, &mut log) {
                let now = ops.now_ms();
                let _ = log.publish(json!({"state": "RUNNING", "stage": "stop_incomplete",
                    "error": crate::beacon::sanitize_short(&format!("FAILED-INCOMPLETE: {why}: {e}")), "finished_ts_ms": now}));
                return Err(format!("FAILED-INCOMPLETE: {e} ({why}; the drill stays RUNNING until `pulse-cutover drill \
                    --cancel` stops it)"));
            }
            let now = ops.now_ms();
            // r30 round 4: a pipeline tool whose process group could not be verified gone (its record is kept)
            // leaves the drill RUNNING (stage `stop_incomplete`), so a ceremony's ARM keeps refusing until the work is
            // stopped (`pulse-cutover drill --cancel`). Its work files stay too (something may still write them).
            if let Some(g) = group_not_proven_gone(cfg, h) {
                let _ = log.publish(json!({"state": "RUNNING", "stage": "stop_incomplete",
                    "error": crate::beacon::sanitize_short(&format!("FAILED-INCOMPLETE: process group {g} not proven gone: {e}")),
                    "finished_ts_ms": now}));
                return Err(format!("FAILED-INCOMPLETE: {e} (pipeline process group {g} is not proven gone; the drill stays \
                    RUNNING until `pulse-cutover drill --cancel` stops it)"));
            }
            let _ = log.publish(json!({"state": "FAILED", "error": crate::beacon::sanitize_short(&e), "finished_ts_ms": now}));
            if !opts.keep_work {
                let _ = std::fs::remove_dir_all(&work);
            }
            Err(e)
        }
    }
}

/// After a failure or cancel: if this drill's snapshot request at H may still be pending (H not final yet, or the
/// chain cannot be read), remove it so nodeos does not write a snapshot nobody will use. Recorded either way.
fn unschedule_if_pending<O: ChainOps>(ops: &DrillGuard<'_, O>, h: u64, log: &mut DrillLog) {
    let Some((sh, id)) = ops.scheduled_request() else {
        return;
    }; // never scheduled (failed before)
    if sh != h {
        return;
    }
    let lib = ops
        .source_info()
        .map(|i| i.last_irreversible_block_num)
        .ok();
    if lib.is_some_and(|l| l >= h) {
        let _ = log.record(
            ops.now_ms(),
            "snapshot_request",
            json!({"unscheduled": false, "why": "H is final: nodeos already wrote it"}),
        );
        return;
    }
    let Some(id) = id else {
        let _ = log.record(ops.now_ms(), "snapshot_request", json!({"unscheduled": false,
            "why": "the producer API reported no request id: a drill never removes a request by height (a ceremony may hold one at the same height); remove it by hand if it is still pending"}));
        return;
    };
    let rec = match ops.unschedule_snapshot_request(id, h) {
        Ok(true) => json!({"unscheduled": true, "snapshot_request_id": id}),
        Ok(false) => {
            json!({"unscheduled": false, "snapshot_request_id": id, "why": "that request is no longer pending (or the producer API cannot unschedule)"})
        }
        Err(e) => json!({"unscheduled": false, "snapshot_request_id": id, "error": e}),
    };
    let _ = log.record(ops.now_ms(), "snapshot_request", rec);
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
    let cancel = cancel_path(cfg, h);
    let cancelled = |c: &Path| c.exists();
    const CANCELLED: &str = "cancelled by `pulse-cutover drill --cancel`";

    // 1. Schedule the snapshot at exactly H (the same producer-API call the ceremony makes at ARM).
    let request_id = ops
        .schedule_snapshot_request(h)
        .map_err(|e| format!("schedule_snapshot({h}) failed: {e}"))?;
    // The request id is persisted so `--cancel` of a dead drill removes exactly this request, never by height.
    // ...bound to the producer API it was made on and its height (round 3: an id means nothing on another node).
    let endpoint = normalized_endpoint(&cfg.source.producer_api_url);
    log.record(
        ops.now_ms(),
        "snapshot_scheduled",
        json!({"h": h, "snapshot_request_id": request_id,
        "snapshot_request_endpoint": endpoint}),
    )?;
    log.publish(
        json!({"stage": "waiting_for_h", "snapshot_request_id": request_id, "snapshot_request_h": h,
        "snapshot_request_endpoint": endpoint}),
    )?;

    // 2. Wait for H to be produced and then final. The chain keeps running: nothing is frozen.
    let head_now = ops.source_info()?.head_block_num;
    // Generous: 1 s per block still to come (blocks are 0.5 s), plus the configured snapshot timeout for finality.
    let deadline = ops.now_ms()
        + (h.saturating_sub(head_now) * 1000)
        + cfg.source.snapshot_timeout_secs * 1000;
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
        if cancelled(&cancel) {
            return Err(CANCELLED.into());
        }
        if now > deadline {
            return Err(format!(
                "H = {h} did not become final in time (lib {})",
                i.last_irreversible_block_num
            ));
        }
        if now.saturating_sub(last_beat) >= 30_000 {
            log.record(
                now,
                "awaiting_h_final",
                json!({"head": i.head_block_num, "lib": i.last_irreversible_block_num}),
            )?;
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
        if cancelled(&cancel) {
            return Err(CANCELLED.into());
        }
        if ops.now_ms() > deadline {
            return Err(format!(
                "scheduled snapshot {} did not appear in time",
                snapshot.display()
            ));
        }
        ops.sleep_ms(cfg.poll_ms);
    }
    timings.insert(
        "snapshot_file_wait".into(),
        json!(ops.now_ms().saturating_sub(final_ms)),
    );
    let t = ops.now_ms();
    let (sha256, size) = crate::verify::sha256_file(&snapshot)?;
    timings.insert(
        "snapshot_sha256".into(),
        json!(ops.now_ms().saturating_sub(t)),
    );
    log.record(
        ops.now_ms(),
        "snapshot",
        json!({"file": snapshot.display().to_string(), "block_id_at_h": id_h,
        "sha256": sha256, "bytes": size}),
    )?;
    log.publish(json!({"stage": "converting", "block_id_at_h": id_h, "snapshot_sha256": sha256, "snapshot_bytes": size,
        "timings_ms": Value::Object(timings.clone())}))?;

    // 4. The upstream pipeline, unchanged, in the drill's own work dir. The rehearsal allowlist and a golden root
    //    pinned for a real event are cleared: a drill reports the true compare result for its own H.
    let mut up = cfg.upstream.clone().ok_or("no [upstream] section")?;
    up.work_dir = work.to_path_buf();
    up.rehearsal_allow_compare_mismatch.clear();
    up.golden_state_root = None;
    // r30 round 4: no shell anywhere. The export is a fixed `docker run` vector from the validated explicit fields;
    // the tools run by argument vector; the guard allows exactly these four programs.
    // r30 round 7: the name the work marker already recorded (so the marker alone can verify it gone).
    let container_name = log.view["planned_export_container"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| format!("pulse-drill-{h}-{}", container_suffix(h, started)));
    let export = upstream::DrillExport {
        image: up
            .export_image
            .clone()
            .ok_or("upstream.export_image is required for a drill")?,
        tools_dir: up
            .export_tools_dir
            .clone()
            .ok_or("upstream.export_tools_dir is required for a drill")?,
        chain_state_db_size_mb: up
            .chain_state_db_size_mb
            .ok_or("upstream.chain_state_db_size_mb is required for a drill")?,
        docker_bin: up
            .docker_bin
            .clone()
            .ok_or("upstream.docker_bin is required for a drill")?,
        docker_socket: docker_socket(&up),
        container_name: container_name.clone(),
        cidfile: work.parent().unwrap_or(work).join("export.cid"),
    };
    allow_pinned_programs(ops, &up)?;
    // The container's name is recorded BEFORE it can exist, so a cancel (or the next run of anything that reads
    // activity) knows which container to stop and verify gone.
    log.record(
        ops.now_ms(),
        "export_container",
        json!({"name": container_name}),
    )?;
    log.publish(json!({"export_container": container_name, "export_container_gone": false}))?;
    let chain_id = ops.source_info()?.chain_id;
    let mut blobs: Vec<Value> = vec![];
    let conv_started = ops.now_ms();
    let res = upstream::run_pipeline_exec(
        &up,
        ops,
        &upstream::PipelineExec::Argv(export),
        &snapshot,
        &sha256,
        h,
        &id_h,
        &chain_id,
        |v| blobs.push(v),
        |_| cancel.exists(),
    );
    for b in &blobs {
        log.record(ops.now_ms(), "pipeline", b.clone())?;
    }
    // Round 5: whatever the pipeline did (finished, failed, cancelled mid-export), the export container must be gone
    // (verified with the daemon) before the drill reports anything else.
    settle_export_container(cfg, ops, log)?;
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
    let compare_result = compare
        .as_ref()
        .and_then(|v| v["result"].as_str().map(str::to_string));
    let failing: Vec<Value> = compare
        .as_ref()
        .and_then(|v| v["failing_tables"].as_array().cloned())
        .unwrap_or_default();

    let (state_root, compare_match) = match res {
        Ok(out) => {
            // The pipeline does not time its last step: conversion total minus the timed steps.
            let timed: u64 = ["export", "import", "compare"]
                .iter()
                .map(|k| timings.get(*k).and_then(Value::as_u64).unwrap_or(0))
                .sum();
            timings.insert(
                "fingerprint_approx".into(),
                json!(ops
                    .now_ms()
                    .saturating_sub(conv_started)
                    .saturating_sub(timed)),
            );
            (out.state_root, true)
        }
        Err(PipelineError::Failed(e)) if compare_result.as_deref() == Some("MISMATCH") => {
            // The comparison disagreed: that IS the drill's finding. The fingerprint is still computed (the same
            // tool and arguments the pipeline uses) so producers can be compared with each other.
            log.record(ops.now_ms(), "compare_mismatch", json!({"error": e}))?;
            let checkpoint = work.join(format!("checkpoint-{h}.bin"));
            if !checkpoint.exists() {
                return Err(format!(
                    "compare failed and no checkpoint exists to fingerprint: {e}"
                ));
            }
            let arena = work.join(format!("arena-fingerprint-{h}"));
            let _ = std::fs::remove_dir_all(&arena);
            std::fs::create_dir_all(&arena)
                .map_err(|e| format!("create {}: {e}", arena.display()))?;
            let argv = vec![
                up.fingerprint_bin.display().to_string(),
                checkpoint.display().to_string(),
                arena.display().to_string(),
            ];
            let t = ops.now_ms();
            let out = ops
                .run_argv_cancellable(&argv, &mut || cancel.exists())
                .map_err(|e| format!("xpr_state_fingerprint failed: {e}"))?
                .ok_or_else(|| CANCELLED.to_string())?;
            timings.insert("fingerprint".into(), json!(ops.now_ms().saturating_sub(t)));
            let (root, _tables) = crate::verify::parse_upstream_report(&out);
            (root, false)
        }
        Err(PipelineError::Failed(e)) => return Err(format!("conversion failed: {e}")),
        Err(PipelineError::Aborted { during }) if cancelled(&cancel) => {
            return Err(format!("{CANCELLED} (during {during})"))
        }
        Err(PipelineError::Aborted { during }) => {
            return Err(format!("conversion stopped during {during}"))
        }
    };
    let end = ops.now_ms();
    timings.insert(
        "conversion_total".into(),
        json!(end.saturating_sub(conv_started)),
    );
    timings.insert(
        "h_final_to_evidence".into(),
        json!(end.saturating_sub(final_ms)),
    );
    timings.insert("total".into(), json!(end.saturating_sub(started)));

    let root_ok = state_root
        .as_deref()
        .is_some_and(|r| r.len() >= 16 && r.chars().all(|c| c.is_ascii_hexdigit()));
    if !root_ok {
        return Err(format!(
            "xpr_state_fingerprint printed no usable state_root ({state_root:?})"
        ));
    }
    // The same digest a real ceremony's VERIFIED record carries (machine.rs: {"upstream_state_root": root}).
    let fingerprints_digest =
        crate::beacon::fingerprints_digest(&json!({"upstream_state_root": state_root}));
    let head_end = ops.source_info().map(|i| i.head_block_num).unwrap_or(0);

    // Keep the small evidence files, drop the multi-GB work files unless asked to keep them.
    let evidence_dir = work.parent().unwrap_or(work).to_path_buf();
    for name in [
        format!("compare-report-{h}.json"),
        format!("checkpoint-{h}.bin.manifest.json"),
    ] {
        let src = work.join(&name);
        if let Ok(bytes) = std::fs::read(&src) {
            let _ = write_fresh(&evidence_dir.join(&name), &bytes);
        }
    }
    if !opts.keep_work {
        let _ = std::fs::remove_dir_all(work);
    }

    let compare_word = if compare_match { "MATCH" } else { "MISMATCH" };
    // rc.31 (fleet run: every drill "failed" on the two known v1.0.0 gaps, indistinguishable from a real mismatch).
    let failing_names: Vec<String> = failing
        .iter()
        .filter_map(|t| t.as_str().map(str::to_string))
        .collect();
    let compare_output = compare
        .as_ref()
        .and_then(|v| v["output"].as_str())
        .unwrap_or("");
    let known = if compare_match {
        None
    } else {
        known_gaps_only(&failing_names, compare_output, &chain_id)
    };
    let compare_class = match (&known, compare_match) {
        (_, true) => "match",
        (Some(_), false) => "known_gaps_only",
        (None, false) => "mismatch",
    };
    let result = json!({
        "state": "DONE",
        "stage": "done",
        "block_id_at_h": id_h,
        "snapshot_sha256": sha256,
        "state_root": state_root,
        "fingerprints_digest": fingerprints_digest,
        "compare": compare_word,
        "compare_failing_tables": failing,
        "compare_class": compare_class,
        "compare_known_gaps": known.clone().unwrap_or_default(),
        "tools": tools_evidence(&up),
        "timings_ms": Value::Object(timings),
        "source_head_at_end": head_end,
        "post_h_blocks_informational": head_end.saturating_sub(h),
        "finished_ts_ms": end,
    });
    log.record(end, "done", result.clone())?;
    log.publish(result)?;
    Ok(DrillOutcome {
        view: log.view.clone(),
        evidence_complete: true,
        compare_match,
        compare_known_gaps_only: known.is_some(),
    })
}

/// The pinned programs a drill may run, allowed on its guard: the docker CLI (bound to the local socket) and the tools.
fn allow_pinned_programs<O: ChainOps>(
    ops: &DrillGuard<'_, O>,
    up: &crate::config::Upstream,
) -> Result<(), String> {
    let pin = |p: &Option<String>, key: &str| {
        p.clone()
            .ok_or_else(|| format!("upstream.{key}_sha256 is required for a drill"))
    };
    let docker = up
        .docker_bin
        .clone()
        .ok_or("upstream.docker_bin is required for a drill")?;
    let mut programs = vec![
        PinnedProgram {
            path: docker.display().to_string(),
            sha256: pin(&up.docker_bin_sha256, "docker_bin")?,
            docker: true,
        },
        PinnedProgram {
            path: up.import_bin.display().to_string(),
            sha256: pin(&up.import_bin_sha256, "import_bin")?,
            docker: false,
        },
        PinnedProgram {
            path: up.fingerprint_bin.display().to_string(),
            sha256: pin(&up.fingerprint_bin_sha256, "fingerprint_bin")?,
            docker: false,
        },
    ];
    if let Some(c) = &up.compare_bin {
        programs.push(PinnedProgram {
            path: c.display().to_string(),
            sha256: pin(&up.compare_bin_sha256, "compare_bin")?,
            docker: false,
        });
    }
    ops.allow_programs(
        programs,
        Some(format!("unix://{}", docker_socket(up).display())),
    );
    Ok(())
}

/// 12 hex characters that make the container name unique per drill run.
fn container_suffix(h: u64, now_ms: u64) -> String {
    use sha2::{Digest, Sha256};
    let mut seed = [0u8; 16];
    let _ = getrandom_fill(&mut seed);
    let d = Sha256::digest(
        format!("{h}:{now_ms}:{}:{}", std::process::id(), hex::encode(seed)).as_bytes(),
    );
    hex::encode(&d[..6])
}

/// Best-effort randomness from the OS (the name only has to be unique on this host; time and pid are mixed in).
fn getrandom_fill(buf: &mut [u8]) -> std::io::Result<()> {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")?.read_exact(buf)
}

/// What the drill's evidence says about the exact programs it ran (mission control compares producers on these).
pub fn tools_evidence(up: &crate::config::Upstream) -> Value {
    let pins = json!({
        "docker_bin_sha256": up.docker_bin_sha256,
        "import_bin_sha256": up.import_bin_sha256,
        "compare_bin_sha256": up.compare_bin_sha256,
        "fingerprint_bin_sha256": up.fingerprint_bin_sha256,
        "export_image_digest": up.export_image.as_deref().and_then(image_digest),
    });
    let canon = [
        "docker_bin_sha256",
        "import_bin_sha256",
        "compare_bin_sha256",
        "fingerprint_bin_sha256",
        "export_image_digest",
    ]
    .iter()
    .map(|k| format!("{k}={}", pins[*k].as_str().unwrap_or("")))
    .collect::<Vec<_>>()
    .join("\n");
    use sha2::{Digest, Sha256};
    let mut v = pins;
    v["tools_digest"] = json!(hex::encode(Sha256::digest(canon.as_bytes())));
    v
}

/// What the daemon says about a container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerStatus {
    Gone,
    Present(String),
    Unknown(String),
}

/// `docker inspect` of `name` through the pinned CLI and the local socket. "No such object/container" = Gone; any
/// other failure is Unknown (never counted as gone).
pub fn container_status<O: ChainOps>(ops: &O, prefix: &[String], name: &str) -> ContainerStatus {
    let mut argv = prefix.to_vec();
    argv.extend([
        "inspect".into(),
        "--type".into(),
        "container".into(),
        "--format".into(),
        "{{.State.Status}}".into(),
        name.into(),
    ]);
    match ops.run_argv_cancellable(&argv, &mut || false) {
        Ok(Some(out)) => ContainerStatus::Present(out.trim().chars().take(40).collect()),
        Ok(None) => ContainerStatus::Unknown("inspect was cancelled".into()),
        Err(e) if e.contains("No such object") || e.contains("No such container") => {
            ContainerStatus::Gone
        }
        Err(e) => ContainerStatus::Unknown(e.chars().take(200).collect()),
    }
}

/// Stop (kill, then remove) and verify gone the export container this drill recorded. Ok(()) when none is recorded,
/// it is already verified gone, or the daemon now says it does not exist; Err(why) otherwise (the view keeps
/// `export_container_gone: false`, so the drill keeps counting as running).
fn settle_export_container<O: ChainOps>(
    cfg: &Config,
    ops: &O,
    log: &mut DrillLog,
) -> Result<(), String> {
    let Some(name) = log.view["export_container"].as_str().map(str::to_string) else {
        return Ok(());
    };
    if log.view["export_container_gone"].as_bool() == Some(true) {
        return Ok(());
    }
    let up = cfg
        .upstream
        .as_ref()
        .ok_or("no [upstream] section to reach docker")?;
    let docker = up
        .docker_bin
        .as_ref()
        .ok_or("upstream.docker_bin is not configured")?;
    let prefix = upstream::drill_docker_prefix(docker, &docker_socket(up));
    let mut last = container_status(ops, &prefix, &name);
    if last != ContainerStatus::Gone {
        for verb in [&["kill"][..], &["rm", "-f"][..]] {
            let mut argv = prefix.clone();
            argv.extend(verb.iter().map(|s| s.to_string()));
            argv.push(name.clone());
            let _ = ops.run_argv_cancellable(&argv, &mut || false);
        }
        for _ in 0..10 {
            last = container_status(ops, &prefix, &name);
            if last == ContainerStatus::Gone {
                break;
            }
            ops.sleep_ms(500);
        }
    }
    let now = ops.now_ms();
    if last == ContainerStatus::Gone {
        log.record(now, "export_container_gone", json!({"name": name}))?;
        log.publish(json!({"export_container_gone": true}))?;
        return Ok(());
    }
    let why = format!("export container {name} not verified gone ({last:?})");
    let _ = log.record(
        now,
        "export_container_not_gone",
        json!({"name": name, "status": format!("{last:?}")}),
    );
    Err(why)
}

// ---------------------------------------------------------------------------------------------- cancel ----

fn wait_until_not_running(cfg: &Config, wait: std::time::Duration) -> bool {
    let end = std::time::Instant::now() + wait;
    loop {
        if !matches!(activity(cfg), DrillActivity::Running { .. }) {
            return true;
        }
        if std::time::Instant::now() >= end {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// `pulse-cutover drill --cancel`: stop this box's running drill and record why.
///
/// Round 3: every check comes BEFORE anything is written: the drill's output paths (overlap with protected paths,
/// and no symlink or hard link at any of them), and, when the drill process is not alive to finish its own record,
/// the same locks a drill takes (the journal's and the drill root's; a ceremony or another drill holding them
/// refuses), a ceremony in progress, and the snapshot request's binding (the producer API it was made on must be
/// this config's). A refused cancel writes nothing.
///
/// 1. A live drill: write the cancel file it polls (while waiting for H, and as the pipeline's cancel check, which
///    stops the running tool); it then fails itself, removes its own pending snapshot request and publishes FAILED.
/// 2. If it has not stopped within `wait`, or its process is gone, stop the pipeline tool's recorded process
///    group (start-time checked; pgid 0 and 1 are never signalled), and verify the group is gone. If it is not,
///    the cancel is INCOMPLETE: the process-group record is KEPT and nothing is marked finished (the drill keeps
///    showing as RUNNING, so a ceremony's ARM keeps refusing) and the command fails.
/// 3. If no drill process is left to finish its own record, remove ONLY the snapshot request id that drill
///    recorded, on the producer API it recorded, and only while that request is still the single-block request at
///    the recorded H (never by height alone), and mark it FAILED.
pub fn cancel<O: ChainOps>(
    cfg: &Config,
    ops: &O,
    wait: std::time::Duration,
) -> Result<String, String> {
    // r30 round 7: the marker is cleared inside cancel_inner, under the locks, and only for the cancelled drill's id.
    cancel_inner(cfg, ops, wait)
}

fn cancel_inner<O: ChainOps>(
    cfg: &Config,
    ops: &O,
    wait: std::time::Duration,
) -> Result<String, String> {
    let (h, live_pid) = match activity(cfg) {
        DrillActivity::Idle => return Ok("no readiness drill is running on this box".into()),
        DrillActivity::Running { h, pid, .. } => (h, pid),
        DrillActivity::Stale { h } => (h, None),
    };
    let overlap = path_overlap_problems(cfg, h);
    if !overlap.is_empty() {
        return Err(format!(
            "cancel refused, nothing changed: {}",
            overlap.join("; ")
        ));
    }
    if let Some(why) = producer_api_problem(&cfg.source.producer_api_url) {
        return Err(format!("cancel refused, nothing changed: {why}"));
    }
    let read_view = || {
        std::fs::read_to_string(latest_path(cfg))
            .ok()
            .and_then(|t| serde_json::from_str::<Value>(&t).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({"kind": DRILL_KIND, "drill": true, "h": h}))
    };
    // The drill process is not alive (dead, or only its pipeline group is): nothing holds the locks, so take them
    // now, before writing anything.
    let mut locks: Option<(std::fs::File, std::fs::File, std::fs::File)> = None;
    let take_locks = || -> Result<(std::fs::File, std::fs::File, std::fs::File), String> {
        let j = Journal::lock(&cfg.journal_path).map_err(|e| {
            format!(
                "cancel refused, nothing changed: a ceremony or a drill holds this journal ({e})"
            )
        })?;
        let r = Journal::lock(&root_lock_base(cfg))
            .map_err(|e| format!("cancel refused, nothing changed: another readiness drill holds the drill directory ({e})"))?;
        let pl = Journal::lock(&producer_lock_base(cfg)).map_err(|e| format!(
            "cancel refused, nothing changed: a ceremony or another drill on this producer holds its lock ({e})"))?;
        if let Some(why) = ceremony_conflict(cfg)? {
            return Err(format!("cancel refused, nothing changed: {why}"));
        }
        binding_problem(cfg, &read_view(), h).map_or(Ok((j, r, pl)), Err)
    };
    if live_pid.is_none() {
        locks = Some(take_locks()?);
    }
    let dir = drill_dir(cfg, h);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let mut log = DrillLog {
        path: dir.join("drill.jsonl"),
        latest: latest_path(cfg),
        view: read_view(),
    };
    if live_pid.is_some() {
        write_fresh(
            &cancel_path(cfg, h),
            format!("{}\n", ops.now_ms()).as_bytes(),
        )?;
    }
    log.record(ops.now_ms(), "cancel_requested", json!({"pid": live_pid}))?;

    if live_pid.is_some() && wait_until_not_running(cfg, wait) {
        return Ok(format!(
            "readiness drill at H = {h} cancelled: it stopped itself (see {})",
            dir.join("drill.jsonl").display()
        ));
    }
    let killed = crate::ops::kill_recorded_hook_group_keep_evidence(
        &pgid_file(cfg, h),
        std::time::Duration::from_secs(10),
    );
    let killed_s = match &killed {
        Ok(Some(d)) => d.clone(),
        Ok(None) => "no live pipeline process group".to_string(),
        Err(e) => format!("pipeline process group not stopped: {e}"),
    };
    log.record(ops.now_ms(), "cancel_group", json!({"result": killed_s}))?;
    if live_pid.is_some() && wait_until_not_running(cfg, wait) {
        return Ok(format!(
            "readiness drill at H = {h} cancelled after stopping its pipeline ({killed_s})"
        ));
    }
    // Success only when the pipeline group is verified gone. A failed kill, or a group still alive, is an
    // incomplete cancel: the record stays RUNNING (a ceremony's ARM keeps refusing), the group record is kept, and
    // the command fails.
    let group_left = group_not_proven_gone(cfg, h);
    if killed.is_err() || group_left.is_some() {
        log.record(
            ops.now_ms(),
            "cancel_incomplete",
            json!({"result": killed_s, "live_group": group_left}),
        )?;
        return Err(format!(
            "cancel INCOMPLETE for the readiness drill at H = {h}: its pipeline process group is still alive or could \
             not be proven gone ({killed_s}{}); it was not marked cancelled. Stop it by hand, then run \
             `pulse-cutover drill --cancel` again",
            group_left.map_or(String::new(), |g| format!(", group {g}"))
        ));
    }
    if let DrillActivity::Running { pid: Some(p), .. } = activity(cfg) {
        return Err(format!(
            "the drill process {p} is still running after the cancel request ({killed_s}); check it by hand"
        ));
    }
    // Nobody is left to finish the drill's record. A drill that was alive at the start has died since: take the
    // locks now (a ceremony or another drill holding them refuses this cleanup).
    if locks.is_none() {
        locks = Some(take_locks()?);
    }
    let _locks = locks;
    log.view = read_view();
    let guard = DrillGuard::new(ops);
    // Round 5 (review R5-3): the export container the dead drill started is stopped and verified gone through the
    // pinned docker CLI and the local socket; until then the cancel is INCOMPLETE and the drill keeps counting as
    // running.
    if container_not_verified_gone(&log.view) {
        let settled = match &cfg.upstream {
            Some(up) => allow_pinned_programs(&guard, up)
                .and_then(|()| settle_export_container(cfg, &guard, &mut log)),
            None => Err("no [upstream] section to reach docker".into()),
        };
        if let Err(why) = settled {
            log.record(
                ops.now_ms(),
                "cancel_incomplete",
                json!({"export_container": why}),
            )?;
            return Err(format!(
                "cancel INCOMPLETE for the readiness drill at H = {h}: {why}. It was not marked cancelled; stop the \
                 container by hand (`docker rm -f <name>` on the local daemon), then run `pulse-cutover drill --cancel` again"
            ));
        }
    }
    match log.view["snapshot_request_id"].as_u64() {
        Some(id) => {
            guard.allow_unschedule_request(h, id);
            unschedule_if_pending(&guard, h, &mut log);
        }
        None => {
            log.record(ops.now_ms(), "snapshot_request", json!({"unscheduled": false,
                "why": "the drill recorded no snapshot request id: a request is never removed by height alone"}))?;
        }
    }
    let now = ops.now_ms();
    log.record(
        now,
        "cancelled",
        json!({"by": "pulse-cutover drill --cancel", "drill_process": "not running"}),
    )?;
    log.publish(json!({"state": "FAILED",
        "error": "cancelled by `pulse-cutover drill --cancel` (the drill process was not running)",
        "finished_ts_ms": now}))?;
    if let Some(id) = log.view["drill_id"].as_str() {
        clear_work_marker_if_done(cfg, id); // the locks are held (`_locks`)
    }
    Ok(format!(
        "readiness drill at H = {h} marked cancelled; its process was not running ({killed_s})"
    ))
}

/// Round 3: a recorded snapshot request may only be cleaned up through the producer API it was made on, at the
/// height it was made for. Some(why) refuses the cancel (nothing changed).
fn binding_problem(cfg: &Config, view: &Value, h: u64) -> Option<String> {
    view["snapshot_request_id"].as_u64()?;
    let here = normalized_endpoint(&cfg.source.producer_api_url);
    match view["snapshot_request_endpoint"].as_str() {
        Some(ep) if ep == here => {}
        Some(ep) => {
            return Some(format!(
                "cancel refused, nothing changed: the drill's snapshot request was made on {ep}, but this config's \
                 producer API is {here}; a request id only means something on the node that issued it. Run \
                 `drill --cancel` with the config the drill used"
            ))
        }
        None => {
            return Some(
                "cancel refused, nothing changed: the drill recorded a snapshot request id without the producer API it \
                 was made on; remove the request by hand (get_snapshot_requests / unschedule_snapshot on that node)"
                    .into(),
            )
        }
    }
    match view["snapshot_request_h"].as_u64() {
        Some(rh) if rh == h => None,
        other => Some(format!(
            "cancel refused, nothing changed: the drill's snapshot request is recorded for height {other:?}, not this \
             drill's H = {h}"
        )),
    }
}

/// `pulse-cutover drill --clear-marker` (r30 round 7): remove an orphaned work marker (its drill died, or the docker
/// daemon was down when it ended) WITHOUT that drill's config: any config on the same producer (same lock_dir and
/// snapshot.dir) will do. It takes the journal, drill-root and producer locks, refuses next to a ceremony in progress,
/// and removes the marker only when it can PROVE the marker's work is gone: the drill process is not running, its
/// recorded pipeline process group has no members, and the pinned docker CLI on the local socket says the recorded
/// export container does not exist. Anything unknown refuses (nothing changed). The removal is recorded in this
/// config's drill directory.
pub fn clear_marker<O: ChainOps>(cfg: &Config, ops: &O) -> Result<String, String> {
    let marker = read_marker(cfg).map_err(|e| {
        format!(
            "clear-marker refused, nothing changed: the work marker cannot be \
        read ({e}); inspect {} and remove it by hand only after checking no drill work runs",
            work_marker_path(cfg).display()
        )
    })?;
    let Some(m) = marker else {
        return Ok("no drill work marker on this producer".into());
    };
    let (Some(jd), Some(h), Some(id)) = (m.journal_dir.clone(), m.h, m.drill_id.clone()) else {
        return Err("clear-marker refused, nothing changed: the marker is from before round 7 (no drill id) or names no \
            journal_dir / h: run `pulse-cutover drill --cancel` with that drill's config".into());
    };
    let overlap = path_overlap_problems(cfg, h);
    if !overlap.is_empty() {
        return Err(format!(
            "clear-marker refused, nothing changed: {}",
            overlap.join("; ")
        ));
    }
    let _j = Journal::lock(&cfg.journal_path).map_err(|e| {
        format!(
            "clear-marker refused, nothing changed: a ceremony or a drill holds this journal ({e})"
        )
    })?;
    let _r = Journal::lock(&root_lock_base(cfg))
        .map_err(|e| format!("clear-marker refused, nothing changed: a readiness drill holds the drill directory ({e})"))?;
    let _p = Journal::lock(&producer_lock_base(cfg)).map_err(|e| format!(
        "clear-marker refused, nothing changed: a ceremony or a drill on this producer holds its lock ({e})"))?;
    if let Some(why) = ceremony_conflict(cfg)? {
        return Err(format!("clear-marker refused, nothing changed: {why}"));
    }
    // Re-read under the producer lock: the marker cannot change from here on.
    let Some(m2) =
        read_marker(cfg).map_err(|e| format!("clear-marker refused, nothing changed: {e}"))?
    else {
        return Ok("no drill work marker on this producer".into());
    };
    if m2.drill_id.as_deref() != Some(id.as_str()) {
        return Err("clear-marker refused, nothing changed: the marker changed while the locks were taken; run it again".into());
    }
    if marker_process_alive(&m2) {
        return Err(format!("clear-marker refused, nothing changed: the marker's drill {id} (pid {}) is still running: \
            stop it with `pulse-cutover drill --cancel` using its config", m2.pid.unwrap_or(0)));
    }
    if let Some(g) = group_not_proven_gone_at(&jd, h) {
        return Err(format!("clear-marker refused, nothing changed: the drill's pipeline process group {g} is alive or \
            cannot be inspected"));
    }
    let Some(container) = m2.container.clone() else {
        return Err(
            "clear-marker refused, nothing changed: the marker records no export container".into(),
        );
    };
    let up = cfg
        .upstream
        .as_ref()
        .ok_or("clear-marker refused, nothing changed: no [upstream] section to reach docker")?;
    let docker = up
        .docker_bin
        .as_ref()
        .ok_or("clear-marker refused, nothing changed: upstream.docker_bin is not configured")?;
    let guard = DrillGuard::new(ops);
    allow_pinned_programs(&guard, up)
        .map_err(|e| format!("clear-marker refused, nothing changed: {e}"))?;
    let prefix = upstream::drill_docker_prefix(docker, &docker_socket(up));
    match container_status(&guard, &prefix, &container) {
        ContainerStatus::Gone => {}
        other => return Err(format!("clear-marker refused, nothing changed: the export container {container} is not \
            verified gone ({other:?}); stop it (`docker rm -f {container}` on the local daemon) and run this again")),
    }
    let v = guard.violations.borrow().clone();
    if !v.is_empty() {
        return Err(format!(
            "clear-marker refused, nothing changed: forbidden operations attempted: {}",
            v.join(", ")
        ));
    }
    std::fs::create_dir_all(drill_root(cfg))
        .map_err(|e| format!("create {}: {e}", drill_root(cfg).display()))?;
    let record = json!({"kind": DRILL_KIND, "event": "work_marker_cleared", "ts_ms": ops.now_ms(), "drill_id": id, "h": h,
        "journal_dir": jd, "export_container": container, "verified": {"process": "not running", "pipeline_group": "none",
        "export_container": "gone (docker inspect on the local socket)"}});
    write_fresh(
        &drill_root(cfg).join(format!("marker-cleared-{id}.json")),
        record.to_string().as_bytes(),
    )?;
    std::fs::remove_file(work_marker_path(cfg))
        .map_err(|e| format!("remove {}: {e}", work_marker_path(cfg).display()))?;
    Ok(format!("drill work marker of drill {id} (H = {h}) removed: its process, pipeline group and export container are \
        verified gone"))
}

/// The producer's work marker as the beacon and `status` show it (r30 round 7), before any event.
pub fn marker_summary(cfg: &Config) -> Option<Value> {
    let m = match read_marker(cfg) {
        Ok(Some(m)) => Some(m),
        Ok(None) => return None,
        Err(_) => None,
    };
    let state = match marker_work(cfg) {
        MarkerWork::None => return None,
        MarkerWork::Live { .. } => "live",
        MarkerWork::Unknown(_) => "unknown",
        MarkerWork::Done { .. } => "done",
    };
    let id = m
        .as_ref()
        .and_then(|m| m.drill_id.clone())
        .filter(|s| s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit()));
    Some(json!({"state": state, "h": m.as_ref().and_then(|m| m.h), "drill_id": id}))
}

// ----------------------------------------------------------------------------------------------- views ----

/// What the beacon publishes about the latest drill (None when this box never ran one). Only short, typed
/// values: hashes, heights, table names, millisecond timings, a sanitized error.
pub fn beacon_view(cfg: &Config) -> Option<Value> {
    // r30 round 7: an orphaned or live work marker on this producer is shown even when this box has no drill record.
    let marker = marker_summary(cfg);
    let latest = std::fs::read_to_string(latest_path(cfg))
        .ok()
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .filter(|v| v["kind"].as_str() == Some(DRILL_KIND));
    let Some(v) = latest else {
        let m = marker?;
        return Some(
            json!({"kind": DRILL_KIND, "h": m["h"].as_u64(), "state": null, "work_marker": m}),
        );
    };
    let hex = |k: &str| {
        v[k].as_str()
            .filter(|s| (16..=64).contains(&s.len()) && s.chars().all(|c| c.is_ascii_hexdigit()))
            .map(str::to_ascii_lowercase)
    };
    let tables: Vec<Value> = v["compare_failing_tables"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|t| t.as_str())
                .filter(|t| {
                    t.len() <= 48
                        && t.chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                })
                .take(30)
                .map(|t| json!(t))
                .collect()
        })
        .unwrap_or_default();
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
        "compare_class": v["compare_class"].as_str().filter(|s| matches!(*s, "match" | "known_gaps_only" | "mismatch")),
        "timings_ms": Value::Object(timings),
        "error": v["error"].as_str().map(crate::beacon::sanitize_short),
        "finished_ts_ms": v["finished_ts_ms"].as_u64(),
        // Round 5: the exact programs the drill ran, so mission control only calls producers in agreement when they
        // ran identical tools on identical state.
        "tools": tools_view(&v["tools"]),
        "work_marker": marker,
    }))
}

/// The `tools` pins of a drill view: only 64-hex values under the known keys.
fn tools_view(t: &Value) -> Value {
    let keys = [
        "docker_bin_sha256",
        "import_bin_sha256",
        "compare_bin_sha256",
        "fingerprint_bin_sha256",
        "export_image_digest",
        "tools_digest",
    ];
    let mut out = serde_json::Map::new();
    for k in keys {
        if let Some(h) = t[k].as_str().filter(|s| valid_pin(s)) {
            out.insert(k.into(), json!(h));
        }
    }
    if out.is_empty() {
        Value::Null
    } else {
        Value::Object(out)
    }
}
