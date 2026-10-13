//! Readiness drill (risk D-7): it measures and reports on the LIVE chain and never changes it.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

use pulse_cutover::config::Config;
use pulse_cutover::drill::{self, DrillGuard, DrillOptions};
use pulse_cutover::ops::{ChainInfo, ChainOps, SnapshotResult};
use serde_json::Value;

const MAINNET: &str = "384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0";
/// The export image, pinned by digest (r30 round 5: a tag is refused).
const IMG: &str = "leap-sidecar@sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn block_id(h: u64) -> String {
    format!("{h:08x}{}", "ab".repeat(28))
}

/// A live chain that never stops: head +1 per read, LIB 3 behind. A scheduled snapshot lands once LIB ≥ H, as
/// nodeos does. Every call is recorded by name.
struct LiveChain {
    dir: PathBuf,
    snap_dir: PathBuf,
    chain_id: String,
    head: RefCell<u64>,
    scheduled: RefCell<Option<u64>>,
    /// The producer API's id for the drill-side request (`scheduled`), and the next id it hands out.
    request_id: RefCell<Option<u64>>,
    next_id: RefCell<u64>,
    /// Requests someone else (a ceremony) holds: (id, height). Never the drill's to remove.
    foreign: RefCell<Vec<(u64, u64)>>,
    calls: RefCell<Vec<String>>,
    /// Every argument vector the drill ran (r30 round 4: no shell ever).
    argvs: RefCell<Vec<Vec<String>>>,
    clock: RefCell<u64>,
    /// LIB never reaches this height (a chain whose finality is slow): the drill waits.
    lib_cap: RefCell<Option<u64>>,
    /// Real milliseconds per sleep_ms (0 = no real sleep, the default).
    real_sleep_ms: u64,
    /// r30 round 5: containers the fake docker daemon runs (by name). Killing the docker CLI (a cancelled `run`)
    /// leaves its container running, as with a real daemon.
    containers: RefCell<std::collections::BTreeSet<String>>,
    /// `docker kill` / `docker rm -f` do not remove the container (a daemon that cannot stop it).
    container_sticky: RefCell<bool>,
    /// `docker run` is "killed" mid-export: the CLI returns as cancelled, the container stays.
    cancel_export: RefCell<bool>,
    /// r30 round 7: the docker daemon cannot be reached (every docker call fails, not "no such object").
    docker_down: RefCell<bool>,
}

impl LiveChain {
    fn new(dir: &Path, chain_id: &str, head: u64) -> Self {
        let snap_dir = dir.join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();
        LiveChain {
            dir: dir.to_path_buf(),
            snap_dir,
            chain_id: chain_id.into(),
            head: RefCell::new(head),
            scheduled: RefCell::new(None),
            request_id: RefCell::new(None),
            next_id: RefCell::new(7),
            foreign: RefCell::new(vec![]),
            calls: RefCell::new(vec![]),
            argvs: RefCell::new(vec![]),
            clock: RefCell::new(1_000_000),
            lib_cap: RefCell::new(None),
            real_sleep_ms: 0,
            containers: RefCell::new(Default::default()),
            container_sticky: RefCell::new(false),
            docker_down: RefCell::new(false),
            cancel_export: RefCell::new(false),
        }
    }
    fn stalled(dir: &Path, chain_id: &str, head: u64, lib_cap: u64, real_sleep_ms: u64) -> Self {
        let c = LiveChain::new(dir, chain_id, head);
        *c.lib_cap.borrow_mut() = Some(lib_cap);
        LiveChain { real_sleep_ms, ..c }
    }
    fn call(&self, n: &str) {
        self.calls.borrow_mut().push(n.into());
    }
    fn called(&self, n: &str) -> bool {
        self.calls.borrow().iter().any(|c| c == n)
    }
}

impl ChainOps for LiveChain {
    fn source_info(&self) -> Result<ChainInfo, String> {
        self.call("source_info");
        let head = {
            let mut h = self.head.borrow_mut();
            *h += 1;
            *h
        };
        let lib = head.saturating_sub(3).min(self.lib_cap.borrow().unwrap_or(u64::MAX));
        if let Some(h) = *self.scheduled.borrow() {
            let f = self.snap_dir.join(format!("snapshot-{}.bin", block_id(h)));
            if lib >= h && !f.exists() {
                std::fs::write(&f, format!("snapshot at {h}")).unwrap();
                std::fs::write(
                    self.dir.join("cut-facts.env"),
                    format!("CHAIN_ID={}\nCUT_BLOCK_ID={}\nCUT_HEIGHT={h}\n", self.chain_id, block_id(h)),
                )
                .unwrap();
            }
        }
        Ok(ChainInfo {
            chain_id: self.chain_id.clone(),
            head_block_num: head,
            head_block_id: block_id(head),
            head_block_time: "2026-10-13T00:00:00.000".into(),
            last_irreversible_block_num: lib,
        })
    }
    fn source_block_id(&self, n: u64) -> Result<(String, String), String> {
        self.call("source_block_id");
        Ok((block_id(n), "2026-10-13T00:00:00.000".into()))
    }
    fn source_block_tx_count(&self, _n: u64) -> Result<u64, String> {
        self.call("source_block_tx_count");
        Ok(7)
    }
    fn producer_paused(&self) -> Result<bool, String> {
        self.call("producer_paused");
        Ok(false)
    }
    fn pause(&self) -> Result<(), String> {
        self.call("pause");
        Ok(())
    }
    fn resume(&self) -> Result<(), String> {
        self.call("resume");
        Ok(())
    }
    fn create_snapshot(&self) -> Result<SnapshotResult, String> {
        self.call("create_snapshot");
        Err("not in a drill".into())
    }
    fn schedule_snapshot(&self, h: u64) -> Result<(), String> {
        self.schedule_snapshot_request(h).map(|_| ())
    }
    fn schedule_snapshot_request(&self, h: u64) -> Result<Option<u64>, String> {
        self.call("schedule_snapshot");
        *self.scheduled.borrow_mut() = Some(h);
        let id = {
            let mut n = self.next_id.borrow_mut();
            *n += 1;
            *n
        };
        *self.request_id.borrow_mut() = Some(id);
        Ok(Some(id))
    }
    /// By height, as the old code did: removes EVERY request at that height, the ceremony's too.
    fn unschedule_snapshot(&self, h: u64) -> Result<bool, String> {
        self.call("unschedule_by_height");
        let had = *self.scheduled.borrow() == Some(h);
        if had {
            *self.scheduled.borrow_mut() = None;
        }
        self.foreign.borrow_mut().retain(|(_, fh)| *fh != h);
        Ok(had)
    }
    /// As the producer API's get_snapshot_requests + unschedule_snapshot: only a pending request with this id AT this
    /// height is removed.
    fn unschedule_snapshot_request(&self, id: u64, height: u64) -> Result<bool, String> {
        self.call("unschedule_snapshot");
        if *self.request_id.borrow() == Some(id) && *self.scheduled.borrow() == Some(height) {
            *self.request_id.borrow_mut() = None;
            *self.scheduled.borrow_mut() = None;
            return Ok(true);
        }
        let before = self.foreign.borrow().len();
        self.foreign.borrow_mut().retain(|(fid, fh)| !(*fid == id && *fh == height));
        Ok(self.foreign.borrow().len() != before)
    }
    fn target_info(&self) -> Result<Option<ChainInfo>, String> {
        self.call("target_info");
        Ok(None)
    }
    fn public_info(&self, _u: &str) -> Result<Option<ChainInfo>, String> {
        self.call("public_info");
        Ok(None)
    }
    fn ignite(&self) -> Result<String, String> {
        self.call("ignite");
        Ok(String::new())
    }
    fn run_hook(&self, _cmd: &str) -> Result<String, String> {
        self.call("run_hook");
        Ok(String::new())
    }
    fn run_long(&self, cmd: &str) -> Result<String, String> {
        self.call("run_long");
        pulse_cutover::ops::run_shell(cmd)
    }
    fn run_long_cancellable(&self, cmd: &str, _c: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        self.call("run_long");
        pulse_cutover::ops::run_shell(cmd).map(Some)
    }
    /// The test stand-in for running by argument vector: `docker run … <image> /xce/export.sh …` runs the staged
    /// export emulation with the mounted host paths; a fake native tool `<bin>` runs its emulation `<bin>.run`.
    /// Neither goes through a configured command string.
    fn run_argv_cancellable(&self, argv: &[String], c: &mut dyn FnMut() -> bool) -> Result<Option<String>, String> {
        self.call("run_argv");
        self.argvs.borrow_mut().push(argv.to_vec());
        if c() {
            return Ok(None);
        }
        let q = |a: &str| format!("'{a}'");
        if argv[0].ends_with("/docker") {
            assert_eq!(argv[1], "--host", "every docker vector names the local daemon");
            assert!(argv[2].starts_with("unix://"));
            let sub = argv[3].as_str();
            let last = argv.last().cloned().unwrap_or_default();
            if *self.docker_down.borrow() {
                return Err("`docker` exited 1: Cannot connect to the Docker daemon at unix:///x/docker.sock".into());
            }
            match sub {
                "inspect" => {
                    return if self.containers.borrow().contains(&last) {
                        Ok(Some("running".into()))
                    } else {
                        Err(format!("`docker inspect` exited 1: Error: No such object: {last}"))
                    };
                }
                "kill" | "rm" => {
                    if !*self.container_sticky.borrow() {
                        self.containers.borrow_mut().remove(&last);
                    }
                    return Ok(Some(String::new()));
                }
                "run" => {}
                other => return Err(format!("unexpected docker subcommand {other}")),
            }
            let name = argv.iter().position(|a| a == "--name").map(|i| argv[i + 1].clone()).expect("--name");
            self.containers.borrow_mut().insert(name.clone());
            if *self.cancel_export.borrow() {
                return Ok(None); // the CLI was killed; the daemon keeps the container
            }
            let mount = |dst: &str| argv.iter().find_map(|a| {
                let mut it = a.splitn(3, ':');
                let (src, d) = (it.next()?, it.next()?);
                (d == dst).then(|| src.to_string())
            });
            let out = mount("/out").ok_or("no /out mount")?;
            let snap = mount("/in/cut.bin").ok_or("no snapshot mount")?;
            let tools = mount("/xce").ok_or("no /xce mount")?;
            let r = pulse_cutover::ops::run_shell(&format!("sh {} {} {}", q(&format!("{tools}/export.sh")), q(&snap), q(&out)))
                .map(Some);
            self.containers.borrow_mut().remove(&name); // --rm
            return r;
        }
        let rest: Vec<String> = argv[1..].iter().map(|a| q(a)).collect();
        pulse_cutover::ops::run_shell(&format!("sh {} {}", q(&format!("{}.run", argv[0])), rest.join(" "))).map(Some)
    }
    fn get_json(&self, _url: &str) -> Result<Option<Value>, String> {
        self.call("get_json");
        Ok(None)
    }
    fn now_ms(&self) -> u64 {
        let mut c = self.clock.borrow_mut();
        *c += 100;
        *c
    }
    fn sleep_ms(&self, _ms: u64) {
        if self.real_sleep_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.real_sleep_ms));
        }
    }
}

fn script(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A fake native executable: an ELF header, mode 0755, one name, content unique to its name (so each pin is
/// distinct). The mock runs `<path>.run` in its place.
fn native(path: &Path) {
    let mut bytes = b"\x7fELF".to_vec();
    bytes.extend_from_slice(&[0u8; 60]);
    bytes.extend_from_slice(path.file_name().unwrap().to_string_lossy().as_bytes());
    std::fs::write(path, bytes).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Fake upstream tools with the official argument shapes: `xce/export.sh` (mounted as the export tools dir) and the
/// three tools as fake native executables under `bin/` with their emulations beside them. `compare_ok = false`
/// makes the compare fail on the two tables v1.0.0 reports for every XPR snapshot (issue #109).
fn stage_tools(dir: &Path, compare_ok: bool) {
    let d = dir.display();
    std::fs::create_dir_all(dir.join("xce")).unwrap();
    std::fs::create_dir_all(dir.join("bin")).unwrap();
    let sidecar = r#"{"version":2,"source_chain_id":"%s","source_block_id":"%s","account_metadata":[{"name":1,"recv_sequence":3,"auth_sequence":1,"code_sequence":0,"abi_sequence":0},{"name":2,"recv_sequence":2,"auth_sequence":0,"code_sequence":0,"abi_sequence":0}],"global_action_sequence":5,"input_transactions":[]}"#;
    let export = format!(
        "#!/bin/sh\nset -e\nsnap=\"$1\"; out=\"$2\"\n\
         sha() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\"; else shasum -a 256 \"$1\"; fi | awk '{{print $1}}'; }}\n\
         . {d}/cut-facts.env\nmkdir -p \"$out/work/state-history\"\n\
         printf SHIPLOG > \"$out/work/state-history/chain_state_history.log\"\n\
         printf '{sidecar}' \"$CHAIN_ID\" \"$CUT_BLOCK_ID\" > \"$out/work/deferred-transactions.json\"\n\
         echo \"INPUT_SNAPSHOT_SHA256=$(sha \"$snap\")\" > \"$out/work/manifest.env\"\necho exported\n"
    );
    std::fs::write(dir.join("xce/export.sh"), &export).unwrap();
    // The ceremony's shell form (export_cmd) keeps a copy outside the tools dir.
    script(&dir.join("export.sh"), &export);
    let import = format!(
        "#!/bin/sh\nset -e\n. {d}/cut-facts.env\nprintf CKPT > \"$3\"\n\
         printf '{{\"checkpoint_sha256\":\"cc\",\"checkpoint_revision\":%s,\"source_block_id\":\"%s\"}}' \"$CUT_HEIGHT\" \"$CUT_BLOCK_ID\" > \"$3.manifest.json\"\n\
         echo imported\n"
    );
    let tables = pulse_cutover::upstream::REQUIRED_COMPARE_TABLES.join(" ");
    let compare = if compare_ok {
        format!("#!/bin/sh\nfor t in {tables}; do echo \"table $t: rows=1 sha256=aa\"; done\nexit 0\n")
    } else {
        "#!/bin/sh\necho 'table contract_index_double: nodeos=Some(TableReport { rows: 104 }) arena=None' >&2\n\
         echo 'table global_property: first differing row index 0' >&2\necho '21-table nodeos/Arena comparison FAILED' >&2\nexit 1\n"
            .to_string()
    };
    let fingerprint = "#!/bin/sh\necho 'revision 1'\necho 'state_root 42b0a70fa12998bdfeedfacecafebeef'\n".to_string();
    for (name, body) in [("import", import), ("compare", compare), ("fingerprint", fingerprint)] {
        let bin = dir.join("bin").join(name);
        if !bin.exists() {
            native(&bin);
        }
        script(&dir.join("bin").join(format!("{name}.run")), &body);
    }
    // r30 round 5: the docker CLI by absolute path (pinned) and the local daemon's socket (a real unix socket).
    if !dir.join("bin/docker").exists() {
        native(&dir.join("bin/docker"));
    }
    let sock = dir.join("docker.sock");
    if !sock.exists() {
        let l = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        std::mem::forget(l);
    }
}

/// sha256 of a staged file (its pin), or 64 zeros when it does not exist.
fn pin(path: &Path) -> String {
    pulse_cutover::verify::sha256_file(path).map(|(h, _)| h).unwrap_or_else(|_| "0".repeat(64))
}

fn drill_config(dir: &Path, chain_id: &str, extra_upstream: &str) -> Result<Config, String> {
    drill_config_edit(dir, chain_id, extra_upstream, |t| t)
}

fn drill_config_edit(dir: &Path, chain_id: &str, extra_upstream: &str, edit: impl Fn(String) -> String) -> Result<Config, String> {
    let d = dir.display();
    let text = format!(
        r#"
journal_path = "{d}/journal.jsonl"
poll_ms = 1

[ceremony]
profile = "readiness"
chain_id = "{chain_id}"
import_backend = "upstream"

[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"

[snapshot]
staged_path = "{d}/staged.bin"
dir = "{d}/snapshots"

[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"

[upstream]
work_dir = "{d}/ceremony-work"
export_cmd = "sh {d}/export.sh {{snapshot}} {{export_dir}}"
export_image = "{IMG}"
export_tools_dir = "{d}/xce"
chain_state_db_size_mb = 12288
docker_bin = "{d}/bin/docker"
docker_bin_sha256 = "{docker_pin}"
docker_socket = "{d}/docker.sock"
import_bin = "{d}/bin/import"
import_bin_sha256 = "{import_pin}"
fingerprint_bin = "{d}/bin/fingerprint"
fingerprint_bin_sha256 = "{fingerprint_pin}"
compare_bin = "{d}/bin/compare"
compare_bin_sha256 = "{compare_pin}"
{extra_upstream}
"#,
        docker_pin = pin(&dir.join("bin/docker")),
        import_pin = pin(&dir.join("bin/import")),
        fingerprint_pin = pin(&dir.join("bin/fingerprint")),
        compare_pin = pin(&dir.join("bin/compare")),
    );
    let p = dir.join("drill.toml");
    std::fs::write(&p, edit(text)).unwrap();
    Config::load(&p)
}

const FORBIDDEN: &[&str] = &["pause", "resume", "create_snapshot", "ignite", "run_hook", "target_info", "public_info"];

#[test]
fn drill_on_mainnet_measures_and_reports_without_touching_the_chain_or_the_ceremony() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").expect("a mainnet drill config loads without any override");
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    let out = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap();

    for f in FORBIDDEN {
        assert!(!chain.called(f), "a drill called {f}");
    }
    assert!(chain.called("schedule_snapshot"));
    assert!(out.evidence_complete && out.compare_match);
    let v = &out.view;
    assert_eq!(v["kind"], "readiness_drill");
    assert_eq!(v["state"], "DONE");
    assert_eq!(v["compare"], "MATCH");
    assert_eq!(v["block_id_at_h"], block_id(1_050));
    assert_eq!(v["state_root"], "42b0a70fa12998bdfeedfacecafebeef");
    // The same digest a real ceremony's VERIFIED record carries, so drills and ceremonies compare directly.
    let expect = pulse_cutover::beacon::fingerprints_digest(&serde_json::json!({"upstream_state_root": "42b0a70fa12998bdfeedfacecafebeef"}));
    assert_eq!(v["fingerprints_digest"], expect);
    for k in ["export", "import", "compare", "conversion_total", "h_final_to_evidence", "snapshot_sha256", "total"] {
        assert!(v["timings_ms"][k].is_u64(), "timing {k} missing: {}", v["timings_ms"]);
    }
    // The chain kept running past H (informational only).
    assert!(v["post_h_blocks_informational"].as_u64().unwrap() > 0);

    // Nothing of the ceremony was touched: no journal, no staged snapshot, not its work dir.
    assert!(!cfg.journal_path.exists());
    assert!(!dir.path().join("staged.bin").exists());
    assert!(!dir.path().join("ceremony-work").exists());
    // Evidence kept, multi-GB work files removed.
    let root = drill::drill_root(&cfg);
    assert!(root.join("1050/drill.jsonl").exists());
    assert!(root.join("1050/checkpoint-1050.bin.manifest.json").exists());
    assert!(!root.join("1050/work").exists());
    let log = std::fs::read_to_string(root.join("1050/drill.jsonl")).unwrap();
    assert!(log.lines().all(|l| serde_json::from_str::<Value>(l).unwrap()["kind"] == "readiness_drill"));

    // The beacon reports it as `drill`, never as the ceremony.
    let view = drill::beacon_view(&cfg).unwrap();
    assert_eq!(view["state"], "DONE");
    assert_eq!(view["fingerprints_digest"], expect);
    let report = pulse_cutover::beacon::build_report(&cfg, "bp1", "mainnet");
    assert_eq!(report["drill"]["h"], 1_050);
    assert!(report["ceremony"]["state"].is_null() || report["ceremony"]["state"] == "");

    // Every drill height is used once.
    let again = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false });
    assert!(again.unwrap_err().contains("already ran"));
}

#[test]
fn drill_reports_the_true_compare_failure_and_still_fingerprints() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), false);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 500);
    let out = drill::run(&cfg, &chain, &DrillOptions { height: 540, keep_work: false }).unwrap();
    assert!(out.evidence_complete);
    assert!(!out.compare_match);
    assert_eq!(out.view["compare"], "MISMATCH");
    let failing: Vec<&str> = out.view["compare_failing_tables"].as_array().unwrap().iter().filter_map(Value::as_str).collect();
    assert!(failing.contains(&"contract_index_double") && failing.contains(&"global_property"), "{failing:?}");
    assert_eq!(out.view["state_root"], "42b0a70fa12998bdfeedfacecafebeef");
    assert!(out.view["timings_ms"]["fingerprint"].is_u64());
    for f in FORBIDDEN {
        assert!(!chain.called(f), "a drill called {f}");
    }
}

#[test]
fn a_rehearsal_allowlist_never_reaches_a_drill_and_stays_refused_on_mainnet() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), false);
    // On mainnet the override is refused at load, drill or not.
    let err = drill_config(dir.path(), MAINNET, r#"rehearsal_allow_compare_mismatch = ["contract_index_double", "global_property"]"#)
        .unwrap_err();
    assert!(err.contains("refused for XPR MAINNET"), "{err}");
    // On another chain the config loads, but the drill ignores the allowlist: it reports the true mismatch.
    let testnet = "71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd";
    let cfg = drill_config(dir.path(), testnet, r#"rehearsal_allow_compare_mismatch = ["contract_index_double", "global_property"]"#)
        .unwrap();
    let chain = LiveChain::new(dir.path(), testnet, 100);
    let out = drill::run(&cfg, &chain, &DrillOptions { height: 140, keep_work: false }).unwrap();
    assert_eq!(out.view["compare"], "MISMATCH");
}

#[test]
fn the_guard_refuses_and_records_every_mutating_call() {
    let dir = tempfile::tempdir().unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1);
    let g = DrillGuard::new(&chain);
    assert!(g.pause().is_err());
    assert!(g.resume().is_err());
    assert!(g.create_snapshot().is_err());
    assert!(g.ignite().is_err());
    assert!(g.run_hook("echo flip").is_err());
    assert!(g.kill_orphan_hooks().is_err());
    assert!(g.target_info().is_err());
    assert!(g.public_info("https://x").is_err());
    g.bind_target(&[("a".into(), "b".into())]);
    assert_eq!(g.violations.borrow().len(), 9);
    for f in FORBIDDEN {
        assert!(!chain.called(f), "the guard passed {f} through");
    }
    // Read-only calls and the snapshot schedule pass.
    assert!(g.source_info().is_ok());
    assert!(g.schedule_snapshot(10).is_ok());
}

#[test]
fn drill_refuses_next_to_a_ceremony_in_progress_and_too_close_heights() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    let near = drill::run(&cfg, &chain, &DrillOptions { height: 1_005, keep_work: false }).unwrap_err();
    assert!(near.contains("too close"), "{near}");
    assert!(!chain.called("schedule_snapshot"));

    // A ceremony journal in FROZEN: refused before anything is scheduled.
    {
        let (mut j, _) = pulse_cutover::journal::Journal::open(&cfg.journal_path).unwrap();
        j.transition(pulse_cutover::state::State::Armed, serde_json::json!({})).unwrap();
        j.transition(pulse_cutover::state::State::Frozen, serde_json::json!({})).unwrap();
    }
    let busy = drill::run(&cfg, &chain, &DrillOptions { height: 2_000, keep_work: false }).unwrap_err();
    assert!(busy.contains("in progress") && busy.contains("FROZEN"), "{busy}");
    assert!(!chain.called("schedule_snapshot"));
}

#[test]
fn drill_needs_the_upstream_pipeline_with_a_compare() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    let text = format!(
        r#"
journal_path = "{d}/journal.jsonl"
[ceremony]
profile = "readiness"
[source]
rpc_url = "http://mock"
producer_api_url = "http://mock"
[snapshot]
staged_path = "{d}/staged.bin"
[target]
metalgo_unit = "mock.service"
rpc_url = "http://mock"
"#
    );
    let p = dir.path().join("c.toml");
    std::fs::write(&p, text).unwrap();
    let cfg = Config::load(&p).unwrap();
    let problems = drill::config_problems(&cfg);
    assert_eq!(problems.len(), 3, "{problems:?}");
}

// ------------------------------------------------------------------------- r30 review: exclusion, cancel ----

fn latest(cfg: &Config) -> Value {
    serde_json::from_str(&std::fs::read_to_string(drill::latest_path(cfg)).unwrap()).unwrap()
}

#[test]
fn a_running_drill_blocks_a_ceremony_and_drill_cancel_stops_it_and_unschedules() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let p = dir.path().to_path_buf();
    // The drill runs in its own thread on a chain whose LIB stays below H: it is waiting for finality.
    let worker = std::thread::spawn(move || {
        let cfg = Config::load(&p.join("drill.toml")).unwrap();
        let chain = LiveChain::stalled(&p, MAINNET, 1_000, 1_030, 5);
        let res = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false });
        let calls = chain.calls.borrow().clone();
        (res.map(|o| o.view), calls)
    });
    let t0 = std::time::Instant::now();
    while !matches!(drill::activity(&cfg), drill::DrillActivity::Running { .. }) {
        assert!(t0.elapsed() < std::time::Duration::from_secs(20), "the drill never showed as RUNNING");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    // While it runs: a fresh ceremony is refused (the ARM preflight reason) and cannot even take the journal.
    let why = drill::ceremony_blocker(&cfg).expect("a running drill blocks a ceremony");
    assert!(why.contains("H = 1050") && why.contains("drill --cancel"), "{why}");
    let held = pulse_cutover::journal::Journal::open(&cfg.journal_path).err().expect("the drill holds the journal lock");
    assert!(held.contains("holds"), "{held}");
    // ...and a second drill is refused.
    let other = LiveChain::new(dir.path(), MAINNET, 1_000);
    let second = drill::run(&cfg, &other, &DrillOptions { height: 1_200, keep_work: false }).unwrap_err();
    assert!(second.contains("lock"), "{second}");
    assert!(!other.called("schedule_snapshot"));

    let msg = drill::cancel(&cfg, &other, std::time::Duration::from_secs(20)).unwrap();
    assert!(msg.contains("cancelled"), "{msg}");
    let (res, calls) = worker.join().unwrap();
    let err = res.unwrap_err();
    assert!(err.contains("cancelled by `pulse-cutover drill --cancel`"), "{err}");
    assert!(calls.iter().any(|c| c == "unschedule_snapshot"), "the drill removed its pending snapshot request");
    for f in FORBIDDEN {
        assert!(!calls.iter().any(|c| c == f), "a cancelled drill called {f}");
    }
    assert_eq!(latest(&cfg)["state"], "FAILED");
    let log = std::fs::read_to_string(drill::drill_dir(&cfg, 1_050).join("drill.jsonl")).unwrap();
    assert!(log.contains("cancel_requested") && log.contains("\"unscheduled\":true"), "{log}");
    assert_eq!(drill::activity(&cfg), drill::DrillActivity::Idle);
    assert!(drill::ceremony_blocker(&cfg).is_none());
    // The lock is released with the drill.
    assert!(pulse_cutover::journal::Journal::open(&cfg.journal_path).is_ok());
}

#[test]
fn a_stale_running_record_does_not_block_and_cancel_closes_it() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 777)).unwrap();
    // A drill process that died mid-wait: RUNNING, but neither its pid (start time differs) nor a group is alive.
    let chain = LiveChain::stalled(dir.path(), MAINNET, 700, 760, 0);
    let id = chain.schedule_snapshot_request(777).unwrap().unwrap();
    // A ceremony holds its own request at the same height: a drill cancel must never remove it.
    chain.foreign.borrow_mut().push((99, 777));
    std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "drill": true, "h": 777,
        "state": "RUNNING", "pid": std::process::id(), "pid_start": "not this process", "snapshot_request_id": id,
        "snapshot_request_h": 777, "snapshot_request_endpoint": drill::normalized_endpoint("http://mock")}).to_string()).unwrap();
    assert_eq!(drill::activity(&cfg), drill::DrillActivity::Stale { h: 777 });
    assert!(drill::ceremony_blocker(&cfg).is_none(), "a stale record is reported, never blocking");

    let msg = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(200)).unwrap();
    assert!(msg.contains("marked cancelled"), "{msg}");
    assert!(chain.called("unschedule_snapshot"));
    assert!(!chain.called("unschedule_by_height"), "never by height");
    assert!(chain.scheduled.borrow().is_none());
    assert_eq!(*chain.foreign.borrow(), vec![(99, 777)], "the ceremony's request at the same height survives");
    let v = latest(&cfg);
    assert_eq!(v["state"], "FAILED");
    assert!(v["error"].as_str().unwrap().contains("cancelled"));
    let log = std::fs::read_to_string(drill::drill_dir(&cfg, 777).join("drill.jsonl")).unwrap();
    assert!(log.contains("\"stage\":\"cancelled\""), "{log}");
    // Nothing to cancel any more.
    assert!(drill::cancel(&cfg, &chain, std::time::Duration::from_millis(10)).unwrap().contains("no readiness drill"));
}

#[test]
fn a_drill_that_fails_before_h_is_final_unschedules_its_snapshot_request() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| t.replace("[source]\n", "[source]\nsnapshot_timeout_secs = 1\n")).unwrap();
    let chain = LiveChain::stalled(dir.path(), MAINNET, 1_000, 1_030, 0);
    let err = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
    assert!(err.contains("did not become final"), "{err}");
    assert!(chain.called("unschedule_snapshot"));
    assert!(chain.scheduled.borrow().is_none());
    assert_eq!(latest(&cfg)["state"], "FAILED");
}

#[test]
fn the_guard_only_unschedules_the_request_this_drill_scheduled_by_id_never_by_height() {
    let dir = tempfile::tempdir().unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1);
    chain.foreign.borrow_mut().push((99, 600)); // a ceremony's request at the same height
    let g = DrillGuard::new(&chain);
    assert!(g.unschedule_snapshot_request(99, 600).is_err(), "a request the drill never scheduled (a ceremony's)");
    assert_eq!(g.violations.borrow().len(), 1);
    assert!(!chain.called("unschedule_snapshot"));
    let id = g.schedule_snapshot_request(600).unwrap().unwrap();
    // By height is refused outright, even for the drill's own height.
    assert!(g.unschedule_snapshot(600).is_err());
    assert!(!chain.called("unschedule_by_height"));
    // Its own id at another height is refused too (round 3: an id is bound to its height).
    assert!(g.unschedule_snapshot_request(id, 601).is_err());
    assert!(g.unschedule_snapshot_request(id, 600).unwrap());
    assert_eq!(*chain.foreign.borrow(), vec![(99, 600)]);
    assert!(g.unschedule_snapshot_request(99, 600).is_err());
}

#[test]
fn any_guard_violation_fails_the_drill_even_when_the_pipeline_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    let g = DrillGuard::new(&chain);
    let _ = g.pause(); // a forbidden call recorded by the guard during the drill's lifetime
    let err = drill::run_with_guard(&cfg, &g, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
    assert!(err.contains("forbidden operations") && err.contains("pause"), "{err}");
    assert!(!chain.called("pause"));
}

#[test]
fn the_drill_pgid_file_is_its_own_never_the_ceremonys() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let ceremony = pulse_cutover::ops::hook_pgid_path(&cfg.journal_path);
    for h in [1, 1_050, 408_328_634] {
        let f = drill::pgid_file(&cfg, h);
        assert_ne!(f, ceremony);
        assert!(f.starts_with(drill::drill_dir(&cfg, h)), "{}", f.display());
        assert_eq!(f, pulse_cutover::ops::hook_pgid_path(&drill::pgid_journal_for(&cfg, h)));
    }
    // A live group recorded in the drill's file makes it RUNNING (so a ceremony refuses) even without its pid.
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new("sh").arg("-c").arg("sleep 30").process_group(0).spawn().unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 9)).unwrap();
    std::fs::write(drill::pgid_file(&cfg, 9), format!("{}\n", child.id())).unwrap();
    std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 9, "state": "RUNNING"}).to_string()).unwrap();
    let running = drill::activity(&cfg);
    let _ = child.kill();
    let _ = child.wait();
    assert!(matches!(running, drill::DrillActivity::Running { h: 9, pid: None, group: Some(_) }), "{running:?}");
    // The ceremony's own pgid file is a different path: its orphan cleanup never sees the drill's group.
    assert!(!ceremony.exists());
}


#[test]
fn a_drill_refuses_paths_that_overlap_the_ceremony_even_through_a_symlink() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    // staged_path inside the drill's work directory (it would be rewritten and deleted).
    let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| t.replace(&format!("staged_path = \"{d}/staged.bin\""),
        &format!("staged_path = \"{d}/drill/1050/work/checkpoint-1050.bin\""))).unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    let err = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
    assert!(err.contains("snapshot.staged_path") && err.contains("overlaps"), "{err}");
    assert!(!chain.called("schedule_snapshot"));
    assert!(!drill::drill_dir(&cfg, 1_050).exists());

    // The drill directory is a symlink into the ceremony's work dir.
    let dir2 = tempfile::tempdir().unwrap();
    stage_tools(dir2.path(), true);
    let cfg = drill_config(dir2.path(), MAINNET, "").unwrap();
    let work = cfg.upstream.as_ref().unwrap().work_dir.clone();
    std::fs::create_dir_all(&work).unwrap();
    std::os::unix::fs::symlink(&work, drill::drill_root(&cfg)).unwrap();
    let chain = LiveChain::new(dir2.path(), MAINNET, 1_000);
    let err = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
    assert!(err.contains("upstream.work_dir") && err.contains("overlaps"), "{err}");
    assert!(!chain.called("schedule_snapshot"));
}


// ------------------------------------------------------------------------------- r30 round 2 review ----



#[test]
fn r2_every_drill_output_path_is_protected_including_shared_telemetry_and_locks() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    for staged in [format!("{d}/drill/latest.json"), format!("{d}/drill/latest.json.tmp"), format!("{d}/drill/drill.lock"),
                   format!("{d}/journal.jsonl.lock"), format!("{d}/drill/1050/cancel")] {
        let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| t.replace(&format!("staged_path = \"{d}/staged.bin\""),
            &format!("staged_path = \"{staged}\""))).unwrap();
        let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
        let err = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
        assert!(err.contains("snapshot.staged_path") && err.contains("overlaps"), "{staged}: {err}");
        assert!(!chain.called("schedule_snapshot"));
        assert!(!drill::latest_path(&cfg).exists(), "nothing was written before the check");
        assert!(!dir.path().join("journal.jsonl.lock").exists(), "no lock file was created before the check");
    }
}

#[test]
fn r2_cancel_takes_the_locks_refuses_next_to_a_ceremony_and_never_unschedules_by_height() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 777)).unwrap();
    let chain = LiveChain::stalled(dir.path(), MAINNET, 700, 760, 0);
    let id = chain.schedule_snapshot_request(777).unwrap().unwrap();
    chain.foreign.borrow_mut().push((99, 777));
    let stale = serde_json::json!({"kind": "readiness_drill", "drill": true, "h": 777, "state": "RUNNING",
        "pid": std::process::id(), "pid_start": "not this process", "snapshot_request_id": id,
        "snapshot_request_h": 777, "snapshot_request_endpoint": drill::normalized_endpoint("http://mock")});
    std::fs::write(drill::latest_path(&cfg), stale.to_string()).unwrap();

    // 1. A ceremony in progress on this journal (its agent not running): refused, nothing changed.
    {
        let (mut j, _) = pulse_cutover::journal::Journal::open(&cfg.journal_path).unwrap();
        j.transition(pulse_cutover::state::State::Armed, serde_json::json!({})).unwrap();
    }
    let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50)).unwrap_err();
    assert!(err.contains("cancel refused") && err.contains("ARMED"), "{err}");
    assert!(!chain.called("unschedule_snapshot") && !chain.called("unschedule_by_height"));
    assert_eq!(latest(&cfg)["state"], "RUNNING");
    std::fs::remove_file(&cfg.journal_path).unwrap();

    // 2. A live ceremony agent holding the journal lock: refused.
    let held = pulse_cutover::journal::Journal::lock(&cfg.journal_path).unwrap();
    let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50)).unwrap_err();
    assert!(err.contains("cancel refused") && err.contains("holds"), "{err}");
    assert!(!chain.called("unschedule_snapshot"));
    drop(held);

    // 3. Another drill holding the drill root's lock (a config with another journal name): refused.
    let held = pulse_cutover::journal::Journal::lock(&drill::root_lock_base(&cfg)).unwrap();
    let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50)).unwrap_err();
    assert!(err.contains("drill directory"), "{err}");
    drop(held);

    // 4. Free: only the recorded request is removed; the ceremony's request at the same height survives.
    let msg = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50)).unwrap();
    assert!(msg.contains("marked cancelled"), "{msg}");
    assert!(!chain.called("unschedule_by_height"));
    assert_eq!(*chain.foreign.borrow(), vec![(99, 777)]);
    assert_eq!(latest(&cfg)["state"], "FAILED");

    // 5. A stale drill that recorded no request id: nothing is unscheduled (never by height).
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 888)).unwrap();
    let chain = LiveChain::stalled(dir.path(), MAINNET, 800, 860, 0);
    chain.foreign.borrow_mut().push((99, 888));
    std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 888, "state": "RUNNING",
        "pid": std::process::id(), "pid_start": "x"}).to_string()).unwrap();
    drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50)).unwrap();
    assert!(!chain.called("unschedule_snapshot") && !chain.called("unschedule_by_height"));
    assert_eq!(*chain.foreign.borrow(), vec![(99, 888)]);
    let log = std::fs::read_to_string(drill::drill_dir(&cfg, 888).join("drill.jsonl")).unwrap();
    assert!(log.contains("never removed by height"), "{log}");
}

#[test]
fn r2_configs_sharing_a_journal_directory_cannot_run_two_drills_at_once() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let p = dir.path().to_path_buf();
    let worker = std::thread::spawn(move || {
        let cfg = Config::load(&p.join("drill.toml")).unwrap();
        let chain = LiveChain::stalled(&p, MAINNET, 1_000, 1_030, 5);
        drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).map(|o| o.view)
    });
    let t0 = std::time::Instant::now();
    while !matches!(drill::activity(&cfg), drill::DrillActivity::Running { .. }) {
        assert!(t0.elapsed() < std::time::Duration::from_secs(20), "the drill never showed as RUNNING");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    // Same directory, another journal file name: it shares drill/latest.json, so it must be refused.
    let other = drill_config_edit(dir.path(), MAINNET, "", |t| t.replace("journal.jsonl", "other-journal.jsonl")).unwrap();
    assert_eq!(drill::drill_root(&other), drill::drill_root(&cfg));
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    let err = drill::run(&other, &chain, &DrillOptions { height: 1_300, keep_work: false }).unwrap_err();
    assert!(err.contains("drill directory"), "{err}");
    assert!(!chain.called("schedule_snapshot"));
    assert!(!drill::drill_dir(&other, 1_300).exists());
    drill::cancel(&cfg, &chain, std::time::Duration::from_secs(20)).unwrap();
    assert!(worker.join().unwrap().is_err());
}

#[test]
fn r2_a_failed_kill_of_the_pipeline_group_is_an_incomplete_cancel_never_success() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 9)).unwrap();
    // The drill process is gone; its recorded pipeline group is alive but its record no longer proves ownership
    // (a different leader start time), so the kill refuses: the cancel must report INCOMPLETE, not FAILED/success.
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new("sh").arg("-c").arg("sleep 30").process_group(0).spawn().unwrap();
    std::fs::write(drill::pgid_file(&cfg, 9), format!("{}\nstart=not the leader\nexport\n", child.id())).unwrap();
    std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 9, "state": "RUNNING",
        "pid": 999_999, "pid_start": "gone", "snapshot_request_id": 3, "snapshot_request_h": 9,
        "snapshot_request_endpoint": drill::normalized_endpoint("http://mock")}).to_string()).unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1);
    let res = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50));
    let _ = child.kill();
    let _ = child.wait();
    let err = res.unwrap_err();
    assert!(err.contains("INCOMPLETE"), "{err}");
    assert_eq!(latest(&cfg)["state"], "RUNNING", "never marked cancelled while work may still run");
    assert!(!chain.called("unschedule_snapshot"));
    let log = std::fs::read_to_string(drill::drill_dir(&cfg, 9).join("drill.jsonl")).unwrap();
    assert!(log.contains("cancel_incomplete") && !log.contains("\"stage\":\"cancelled\""), "{log}");
}

// ------------------------------------------------------------------------------- r30 round 3 review ----


#[test]
fn r3_a_drill_never_writes_through_a_symlink_or_hard_link_beneath_its_root() {
    for kind in ["symlink", "hardlink"] {
        for name in ["latest.json.tmp", "latest.json"] {
            let dir = tempfile::tempdir().unwrap();
            stage_tools(dir.path(), true);
            let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
            // The staged snapshot (a protected file) lives outside the drill root, so the root's overlap check passes.
            std::fs::write(&cfg.snapshot.staged_path, "STAGED").unwrap();
            std::fs::create_dir_all(drill::drill_root(&cfg)).unwrap();
            let link = drill::drill_root(&cfg).join(name);
            if kind == "symlink" {
                std::os::unix::fs::symlink(&cfg.snapshot.staged_path, &link).unwrap();
            } else {
                std::fs::hard_link(&cfg.snapshot.staged_path, &link).unwrap();
            }
            let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
            let err = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
            assert!(err.contains(name) && (err.contains("symlink") || err.contains("hard link")), "{kind} {name}: {err}");
            assert!(!chain.called("schedule_snapshot"), "{kind} {name}");
            assert_eq!(std::fs::read_to_string(&cfg.snapshot.staged_path).unwrap(), "STAGED", "{kind} {name}: untouched");
        }
    }
}

#[test]
fn r3_cancel_checks_paths_locks_and_binding_before_writing_anything() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    let stale = |cfg: &Config, endpoint: &str| {
        std::fs::create_dir_all(drill::drill_dir(cfg, 777)).unwrap();
        std::fs::write(drill::latest_path(cfg), serde_json::json!({"kind": "readiness_drill", "drill": true, "h": 777,
            "state": "RUNNING", "pid": std::process::id(), "pid_start": "not this process", "snapshot_request_id": 8,
            "snapshot_request_h": 777, "snapshot_request_endpoint": endpoint}).to_string()).unwrap();
    };
    // 1. An overlapping config (staged_path = the drill's own log): refused, nothing written.
    let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| t.replace(&format!("staged_path = \"{d}/staged.bin\""),
        &format!("staged_path = \"{d}/drill/777/drill.jsonl\""))).unwrap();
    stale(&cfg, &drill::normalized_endpoint("http://mock"));
    let chain = LiveChain::new(dir.path(), MAINNET, 700);
    let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(20)).unwrap_err();
    assert!(err.contains("cancel refused, nothing changed") && err.contains("overlaps"), "{err}");
    assert!(!drill::drill_dir(&cfg, 777).join("drill.jsonl").exists(), "no log written");
    assert!(!drill::cancel_path(&cfg, 777).exists(), "no cancel file written");
    assert_eq!(latest(&cfg)["state"], "RUNNING");

    // 2. The drill's log is a symlink to a protected file: refused, the target untouched.
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    stale(&cfg, &drill::normalized_endpoint("http://mock"));
    std::fs::write(&cfg.snapshot.staged_path, "STAGED").unwrap();
    std::os::unix::fs::symlink(&cfg.snapshot.staged_path, drill::drill_dir(&cfg, 777).join("drill.jsonl")).unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 700);
    let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(20)).unwrap_err();
    assert!(err.contains("symlink"), "{err}");
    assert_eq!(std::fs::read_to_string(&cfg.snapshot.staged_path).unwrap(), "STAGED");
    assert!(!chain.called("unschedule_snapshot"));

    // 3. The request was made on another producer API (the config now points elsewhere): refused, nothing written,
    //    nothing unscheduled.
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    stale(&cfg, "http://other-node:8888");
    let chain = LiveChain::stalled(dir.path(), MAINNET, 700, 760, 0);
    chain.foreign.borrow_mut().push((8, 777)); // on THIS node, id 8 is a ceremony's request
    let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(20)).unwrap_err();
    assert!(err.contains("other-node") && err.contains("nothing changed"), "{err}");
    assert!(!chain.called("unschedule_snapshot"));
    assert_eq!(*chain.foreign.borrow(), vec![(8, 777)]);
    assert!(!drill::drill_dir(&cfg, 777).join("drill.jsonl").exists(), "no log written");
    assert_eq!(latest(&cfg)["state"], "RUNNING");
}

#[test]
fn r3_a_recorded_request_is_only_removed_at_its_recorded_height() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 777)).unwrap();
    let chain = LiveChain::stalled(dir.path(), MAINNET, 700, 760, 0);
    // The id the drill recorded now names a request at ANOTHER height on this node: never removed.
    let id = chain.schedule_snapshot_request(778).unwrap().unwrap();
    std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "drill": true, "h": 777,
        "state": "RUNNING", "pid": std::process::id(), "pid_start": "x", "snapshot_request_id": id,
        "snapshot_request_h": 777, "snapshot_request_endpoint": drill::normalized_endpoint("http://mock")}).to_string()).unwrap();
    drill::cancel(&cfg, &chain, std::time::Duration::from_millis(20)).unwrap();
    assert_eq!(*chain.scheduled.borrow(), Some(778), "the request at another height survives");
}

#[test]
fn r3_a_failed_cancel_keeps_the_group_record_so_the_drill_keeps_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 9)).unwrap();
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new("sh").arg("-c").arg("sleep 30").process_group(0).spawn().unwrap();
    std::fs::write(drill::pgid_file(&cfg, 9), format!("{}\nstart=not the leader\nexport\n", child.id())).unwrap();
    std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 9, "state": "RUNNING",
        "pid": 999_999, "pid_start": "gone", "snapshot_request_id": 3, "snapshot_request_h": 9,
        "snapshot_request_endpoint": drill::normalized_endpoint("http://mock")}).to_string()).unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1);
    let first = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50));
    let record_kept = drill::pgid_file(&cfg, 9).exists();
    let activity = drill::activity(&cfg);
    let blocker = drill::ceremony_blocker(&cfg);
    let second = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(50));
    let _ = child.kill();
    let _ = child.wait();
    assert!(first.unwrap_err().contains("INCOMPLETE"));
    assert!(record_kept, "the group record is kept while the group may still run");
    assert!(matches!(activity, drill::DrillActivity::Running { h: 9, .. }), "{activity:?}");
    assert!(blocker.is_some(), "a fresh ceremony's ARM is still refused");
    assert!(second.unwrap_err().contains("INCOMPLETE"), "a second cancel does not report success");
    assert_eq!(latest(&cfg)["state"], "RUNNING");
}

// ------------------------------------------------------------------------------- r30 round 4 review ----

fn set_toml(t: &str, key: &str, value: &str) -> String {
    let start = t.find(&format!("{key} = ")).unwrap();
    let end = start + t[start..].find('\n').unwrap();
    format!("{}{key} = {}{}", &t[..start], serde_json::to_string(value).unwrap(), &t[end..])
}

/// R4-4 / R4-5 and every scanner bypass before them: a drill runs no configured text, so an export_cmd (or a relative
/// wrapper, or a `..` path through a symlink) that would run a hook is simply never run. Before round 4 the scanner
/// accepted these and the drill ran export_cmd, firing the hook.
#[test]
fn r4_a_drill_never_runs_export_cmd_or_any_shell_whatever_it_says() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    let marker = dir.path().join("HOOK-RAN");
    std::fs::create_dir_all(dir.path().join("hooks/sub")).unwrap();
    std::fs::create_dir_all(dir.path().join("conv")).unwrap();
    script(&dir.path().join("hooks/on-freeze.sh"), &format!("#!/bin/sh\ntouch {}\n", marker.display()));
    script(&dir.path().join("conv/wrapper.sh"), &format!("#!/bin/sh\n{d}/hooks/on-freeze.sh\nsh {d}/export.sh \"$1\" \"$2\"\n"));
    std::os::unix::fs::symlink(dir.path().join("hooks/sub"), dir.path().join("link")).unwrap();
    let hooks = format!("\n[hooks]\non_freeze = \"{d}/hooks/on-freeze.sh\"\n");
    for (i, cmd) in [
        format!("cd {d}/conv && sh wrapper.sh {{snapshot}} {{export_dir}}"),
        format!("sh {d}/link/../on-freeze.sh && sh {d}/export.sh {{snapshot}} {{export_dir}}"),
        format!("H={d}/hooks; \"$H/on-freeze.sh\"; sh {d}/export.sh {{snapshot}} {{export_dir}}"),
    ].iter().enumerate() {
        let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| set_toml(&t, "export_cmd", cmd) + &hooks).unwrap();
        assert!(drill::config_problems(&cfg).is_empty(), "{cmd}: {:?}", drill::config_problems(&cfg));
        let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
        let h = 1_050 + 100 * i as u64;
        let out = drill::run(&cfg, &chain, &DrillOptions { height: h, keep_work: false }).unwrap();
        assert_eq!(out.view["state"], "DONE");
        assert!(!marker.exists(), "{cmd}: the hook ran");
        assert!(!chain.called("run_long"), "{cmd}: a shell command reached the chain ops");
        // The export is the fixed vector, built only from the explicit fields.
        let argvs = chain.argvs.borrow();
        let ex = argvs.iter().find(|a| a[0].ends_with("/docker") && a[3] == "run").expect("the export ran by argument vector");
        assert_eq!(ex[..5], [format!("{d}/bin/docker"), "--host".into(), format!("unix://{d}/docker.sock"), "run".into(),
            "--rm".into()]);
        assert!(ex.contains(&"--name".to_string()) && ex.contains(&"--cidfile".to_string()) && ex.contains(&"none".to_string()));
        assert!(ex.contains(&IMG.to_string()) && ex.contains(&format!("{d}/xce:/xce:ro")));
        assert!(ex.contains(&"--chain-state-db-size-mb".to_string()) && ex.contains(&"12288".to_string()));
        assert!(ex.iter().all(|a| !a.contains("on-freeze") && !a.contains("wrapper")), "{ex:?}");
        for tool in ["import", "compare", "fingerprint"] {
            assert!(argvs.iter().any(|a| a[0] == format!("{d}/bin/{tool}")), "{tool} not run by vector");
        }
    }
}

#[test]
fn r4_the_guard_refuses_shell_and_every_program_but_the_validated_four() {
    let dir = tempfile::tempdir().unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1);
    let g = DrillGuard::new(&chain);
    assert!(g.run_long("echo hi").is_err());
    assert!(g.run_long_cancellable("echo hi", &mut || false).is_err());
    g.allow_programs(vec![drill::PinnedProgram { path: "/opt/bin/import".into(), sha256: "0".repeat(64), docker: false }], None);
    assert!(g.run_argv_cancellable(&["/bin/sh".into(), "-c".into(), "touch x".into()], &mut || false).is_err());
    assert!(g.run_argv_cancellable(&["systemctl".into(), "stop".into(), "nodeos".into()], &mut || false).is_err());
    assert_eq!(g.violations.borrow().len(), 4);
    assert!(!chain.called("run_long") && !chain.called("run_argv"));
}

#[test]
fn r4_drill_tools_must_be_plain_native_executables_and_the_export_fields_explicit() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    let base = drill_config(dir.path(), MAINNET, "").unwrap();
    assert!(drill::config_problems(&base).is_empty(), "{:?}", drill::config_problems(&base));
    // A script, a symlink, a hard link, a relative path, a group-writable file: each refused.
    script(&dir.path().join("bin/script-tool"), "#!/bin/sh\necho hi\n");
    std::os::unix::fs::symlink(dir.path().join("bin/import"), dir.path().join("bin/import-link")).unwrap();
    native(&dir.path().join("bin/hard"));
    std::fs::hard_link(dir.path().join("bin/hard"), dir.path().join("bin/hard2")).unwrap();
    native(&dir.path().join("bin/gw"));
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.path().join("bin/gw"), std::fs::Permissions::from_mode(0o775)).unwrap();
    for (tool, want) in [("bin/script-tool", "a script"), ("bin/import-link", "symlink"), ("bin/hard", "hard link"),
                         ("bin/gw", "writable")] {
        let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| set_toml(&t, "import_bin", &format!("{d}/{tool}"))).unwrap();
        let p = drill::config_problems(&cfg).join("; ");
        assert!(p.contains("upstream.import_bin") && p.contains(want), "{tool}: {p}");
    }
    let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| set_toml(&t, "compare_bin", "bin/compare")).unwrap();
    assert!(drill::config_problems(&cfg).join("; ").contains("not an absolute path"));
    // Missing or invalid export fields.
    for (edit, want) in [
        (Box::new(|t: String| t.replace(&format!("export_image = \"{IMG}\"\n"), "")) as Box<dyn Fn(String) -> String>, "export_image"),
        (Box::new(|t: String| t.replace(IMG, "img; rm -rf /")), "plain image reference"),
        (Box::new(|t: String| t.replace(IMG, "--privileged")), "plain image reference"),
        (Box::new(|t: String| t.replace("chain_state_db_size_mb = 12288\n", "")), "chain_state_db_size_mb"),
        (Box::new(move |t: String| t.replace(&format!("{d}/xce"), "/nonexistent-xce")), "export_tools_dir"),
    ] {
        let cfg = drill_config_edit(dir.path(), MAINNET, "", edit).unwrap();
        let p = drill::config_problems(&cfg).join("; ");
        assert!(p.contains(want), "{want}: {p}");
        let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
        assert!(drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).is_err());
        assert!(!chain.called("schedule_snapshot"));
    }
}

/// R4-6: an endpoint with a query, fragment or credentials is refused for a drill and for its cancel.
#[test]
fn r4_a_drill_refuses_an_ambiguous_producer_api_and_cancel_writes_nothing() {
    for url in ["http://proxy/api?node=shadow", "http://proxy/api#x", "http://user:pw@proxy/api"] {
        let dir = tempfile::tempdir().unwrap();
        stage_tools(dir.path(), true);
        let cfg = drill_config_edit(dir.path(), MAINNET, "", |t| t.replacen("producer_api_url = \"http://mock\"",
            &format!("producer_api_url = \"{url}\""), 1)).unwrap();
        let p = drill::config_problems(&cfg).join("; ");
        assert!(p.contains("source.producer_api_url"), "{url}: {p}");
        let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
        assert!(drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).is_err());
        assert!(!chain.called("schedule_snapshot"));
        // A stale record: cancel refuses before writing anything.
        std::fs::create_dir_all(drill::drill_dir(&cfg, 777)).unwrap();
        std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 777, "state": "RUNNING",
            "pid": std::process::id(), "pid_start": "x", "snapshot_request_id": 8, "snapshot_request_h": 777,
            "snapshot_request_endpoint": drill::normalized_endpoint(url)}).to_string()).unwrap();
        let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(20)).unwrap_err();
        assert!(err.contains("nothing changed") && err.contains("producer_api_url"), "{url}: {err}");
        assert!(!drill::drill_dir(&cfg, 777).join("drill.jsonl").exists());
        assert!(!chain.called("unschedule_snapshot"));
    }
}

/// R4-8 (drill side): a finished record does not end the drill while its recorded process group is still alive.
#[test]
fn r4_a_recorded_live_group_keeps_a_finished_drill_running() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    std::fs::create_dir_all(drill::drill_dir(&cfg, 9)).unwrap();
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new("sh").arg("-c").arg("sleep 30").process_group(0).spawn().unwrap();
    std::fs::write(drill::pgid_file(&cfg, 9), format!("{}\n", child.id())).unwrap();
    std::fs::write(drill::latest_path(&cfg), serde_json::json!({"kind": "readiness_drill", "h": 9, "state": "FAILED"}).to_string()).unwrap();
    let a = drill::activity(&cfg);
    let blocker = drill::ceremony_blocker(&cfg);
    let _ = child.kill();
    let _ = child.wait();
    assert!(matches!(a, drill::DrillActivity::Running { h: 9, .. }), "{a:?}");
    assert!(blocker.is_some());
    // Once the group is gone the finished record is idle again.
    assert_eq!(drill::activity(&cfg), drill::DrillActivity::Idle);
}


/// R5-2 (Codex): a native-executable check alone admitted `/bin/bash` as a tool (the import step would then run the
/// export-produced log as shell). A drill runs a program only when its CONTENT matches its configured pin, and the
/// export image only by digest; docker only by absolute path, pinned, against an explicit local socket.
#[test]
fn r5_a_drill_runs_only_pinned_content_never_a_host_shell() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    let base = drill_config(dir.path(), MAINNET, "").unwrap();
    assert!(drill::config_problems(&base).is_empty(), "{:?}", drill::config_problems(&base));
    let bash = ["/bin/bash", "/usr/bin/bash", "/bin/sh"].into_iter().find(|p| Path::new(p).exists()).unwrap();
    let remove = |key: &'static str| move |t: String| {
        let start = t.find(&format!("{key} = ")).unwrap();
        let end = start + t[start..].find('\n').unwrap() + 1;
        format!("{}{}", &t[..start], &t[end..])
    };
    let regular = dir.path().join("not-a-socket");
    std::fs::write(&regular, "x").unwrap();
    let cases: Vec<(Box<dyn Fn(String) -> String>, &str)> = vec![
        (Box::new(move |t: String| set_toml(&t, "import_bin", bash)), "not its pin"),
        (Box::new(remove("import_bin_sha256")), "no sha256 pin"),
        (Box::new(|t: String| set_toml(&t, "compare_bin_sha256", "ABC")), "64 lowercase hex"),
        (Box::new(|t: String| t.replace(IMG, "leap-sidecar:5.0.3")), "not pinned by digest"),
        (Box::new(remove("docker_bin")), "docker_bin"),
        (Box::new(remove("docker_bin_sha256")), "no sha256 pin"),
        (Box::new(move |t: String| set_toml(&t, "docker_socket", &regular.display().to_string())), "not a unix socket"),
        (Box::new(|t: String| set_toml(&t, "docker_socket", "run/docker.sock")), "not an absolute path"),
    ];
    for (edit, want) in cases {
        let cfg = drill_config_edit(dir.path(), MAINNET, "", edit).unwrap();
        let p = drill::config_problems(&cfg).join("; ");
        assert!(p.contains(want), "{want}: {p}");
        let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
        assert!(drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).is_err());
        assert!(!chain.called("schedule_snapshot") && !chain.called("run_argv"), "{want}: the drill started");
    }

    // At USE time: a tool whose content changed after the config was checked is not run.
    let chain = LiveChain::new(dir.path(), MAINNET, 1);
    let g = DrillGuard::new(&chain);
    let import = format!("{d}/bin/import");
    g.allow_programs(vec![drill::PinnedProgram { path: import.clone(), sha256: pin(&dir.path().join("bin/import")), docker: false }], None);
    let mut bytes = std::fs::read(&import).unwrap();
    bytes.extend_from_slice(b"tampered");
    std::fs::write(&import, bytes).unwrap();
    let err = g.run_argv_cancellable(&[import.clone(), "x".into()], &mut || false).unwrap_err();
    assert!(err.contains("not its pin"), "{err}");
    assert!(!chain.called("run_argv"), "the changed tool reached the runner");

    // Docker must name the local daemon: a vector without `--host unix://<socket>` is refused.
    let docker = format!("{d}/bin/docker");
    g.allow_programs(vec![drill::PinnedProgram { path: docker.clone(), sha256: pin(&dir.path().join("bin/docker")), docker: true }],
        Some(format!("unix://{d}/docker.sock")));
    assert!(g.run_argv_cancellable(&[docker.clone(), "run".into(), "x".into()], &mut || false).is_err());
    assert!(g.run_argv_cancellable(&[docker, "--host".into(), "tcp://remote:2375".into(), "run".into()], &mut || false).is_err());
    assert!(!chain.called("run_argv"));
}

/// R5-3 (Codex): killing the docker CLI does not stop the daemon-managed export container. The drill names the
/// container before it exists, stops it (kill, rm -f) and verifies it gone through the pinned CLI; until then the drill
/// counts as running (a ceremony keeps refusing) and a cancel is INCOMPLETE.
#[test]
fn r5_an_export_container_that_outlives_the_docker_cli_keeps_the_drill_running_until_verified_gone() {
    // 1. The CLI is killed mid-export; the daemon CAN stop the container: the drill stops it, verifies it gone, FAILED.
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    *chain.cancel_export.borrow_mut() = true;
    assert!(drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).is_err());
    assert!(chain.containers.borrow().is_empty(), "the container was left running");
    let v = latest(&cfg);
    assert_eq!(v["state"], "FAILED", "{v}");
    assert_eq!(v["export_container_gone"], true);
    assert_eq!(drill::activity(&cfg), drill::DrillActivity::Idle);
    let argvs = chain.argvs.borrow();
    for verb in ["kill", "rm", "inspect"] {
        assert!(argvs.iter().any(|a| a[0].ends_with("/docker") && a[3] == verb), "no docker {verb}");
    }
    drop(argvs);

    // 2. The container cannot be stopped: the drill stays RUNNING, a ceremony is refused, cancel is INCOMPLETE.
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let cfg = drill_config(dir.path(), MAINNET, "").unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    *chain.cancel_export.borrow_mut() = true;
    *chain.container_sticky.borrow_mut() = true;
    let err = drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
    assert!(err.contains("FAILED-INCOMPLETE") && err.contains("not verified gone"), "{err}");
    assert_eq!(chain.containers.borrow().len(), 1);
    let v = latest(&cfg);
    assert_eq!(v["state"], "RUNNING", "{v}");
    assert!(matches!(drill::activity(&cfg), drill::DrillActivity::Running { h: 1_050, .. }));
    assert!(drill::ceremony_blocker(&cfg).is_some(), "a ceremony must keep refusing while the container may run");
    // The drill process exits after returning (here: this test process): its record now names a dead process. The
    // recorded container alone keeps it running.
    let mut v = latest(&cfg);
    v["pid_start"] = serde_json::json!("a process that is gone");
    std::fs::write(drill::latest_path(&cfg), v.to_string()).unwrap();
    assert!(matches!(drill::activity(&cfg), drill::DrillActivity::Running { pid: None, group: None, .. }));
    let err = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(20)).unwrap_err();
    assert!(err.contains("INCOMPLETE") && err.contains("not verified gone"), "{err}");
    assert!(matches!(drill::activity(&cfg), drill::DrillActivity::Running { .. }));

    // 3. Once the daemon can stop it, cancel stops it, verifies it gone and closes the drill.
    *chain.container_sticky.borrow_mut() = false;
    let ok = drill::cancel(&cfg, &chain, std::time::Duration::from_millis(20)).unwrap();
    assert!(ok.contains("marked cancelled"), "{ok}");
    assert!(chain.containers.borrow().is_empty());
    assert_eq!(drill::activity(&cfg), drill::DrillActivity::Idle);
}

/// R5-6 (Codex): exclusion was keyed on the journal directory. Two configs on the SAME producer (same producer API,
/// same nodeos snapshot dir) under different journal directories now share one producer lock: a second drill (or a
/// ceremony) is refused while the first holds it.
#[test]
fn r5_drills_on_the_same_producer_exclude_each_other_across_journal_directories() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    stage_tools(a.path(), true);
    stage_tools(b.path(), true);
    let shared_snapshots = a.path().join("snapshots");
    let cfg_a = drill_config(a.path(), MAINNET, "").unwrap();
    let cfg_b = drill_config_edit(b.path(), MAINNET, "", |t| t.replace(&format!("{}/snapshots", b.path().display()),
        &shared_snapshots.display().to_string())).unwrap();
    assert_ne!(drill::drill_root(&cfg_a), drill::drill_root(&cfg_b));
    assert_eq!(drill::producer_lock_base(&cfg_a), drill::producer_lock_base(&cfg_b));
    // A's drill holds the producer lock (as it does for its whole run).
    let held = pulse_cutover::journal::Journal::lock(&drill::producer_lock_base(&cfg_a)).unwrap();
    let chain = LiveChain::new(b.path(), MAINNET, 1_000);
    let err = drill::run(&cfg_b, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap_err();
    assert!(err.contains("producer's lock"), "{err}");
    assert!(!chain.called("schedule_snapshot"));
    drop(held);
    // A producer with its own snapshot directory (another nodeos on the box) has its own lock (round 6: keyed on
    // the canonical snapshot dir, not the producer API spelling).
    let cfg_c = drill_config_edit(b.path(), MAINNET, "", |t| t.replacen("producer_api_url = \"http://mock\"",
        "producer_api_url = \"http://other-node:8888\"", 1)).unwrap();
    assert_ne!(drill::producer_lock_base(&cfg_a), drill::producer_lock_base(&cfg_c));
}

/// R7-3 (Codex): a drill released its locks and then cleaned up the producer's work marker by reading `latest.json`;
/// a newer drill could have written ITS marker in between (latest.json still naming the old, finished drill), and the
/// old drill's cleanup deleted it. Now every marker names its drill (`drill_id`, also in that drill's latest.json),
/// only that drill removes it, under its locks; and a marker whose drill never published and is not running is
/// UNKNOWN (it blocks a ceremony anywhere until `drill --clear-marker` verifies its work gone), never "done".
#[test]
fn r7_a_drill_cleans_up_only_its_own_work_marker() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    let with_lock_dir = |t: String| t.replacen("[ceremony]\n", &format!("[ceremony]\nlock_dir = \"{d}/locks\"\n"), 1);
    let cfg = drill_config_edit(dir.path(), MAINNET, "", with_lock_dir).unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    drill::run(&cfg, &chain, &DrillOptions { height: 1_050, keep_work: false }).unwrap();
    assert!(!drill::work_marker_path(&cfg).exists(), "drill A removed its own marker");
    let a_id = latest(&cfg)["drill_id"].as_str().unwrap().to_string();
    assert_eq!(a_id.len(), 32);

    // Drill B (alive: this process) has written its marker; latest.json still names the finished drill A.
    let me = std::process::id();
    let jd = std::fs::canonicalize(dir.path()).unwrap();
    let b_marker = |pid_start: &str| serde_json::json!({"kind": "readiness_drill_work", "journal_dir": jd, "h": 1_100,
        "drill_id": "b".repeat(32), "export_container": "pulse-drill-1100-bbbbbbbbbbbb", "pid": me, "pid_start": pid_start});
    let start = pulse_cutover::ops::process_start(me).unwrap();
    std::fs::write(drill::work_marker_path(&cfg), b_marker(&start).to_string()).unwrap();
    // A's (late) cleanup must not touch B's marker.
    drill::clear_work_marker_if_done(&cfg, &a_id);
    assert!(drill::work_marker_path(&cfg).exists(), "drill A's cleanup deleted drill B's marker");
    assert!(matches!(drill::marker_work(&cfg), drill::MarkerWork::Live { h: 1_100, .. }), "{:?}", drill::marker_work(&cfg));

    // B died without publishing: its export may exist. The marker is UNKNOWN, never done, and blocks a ceremony on the
    // same producer under ANOTHER journal directory.
    std::fs::write(drill::work_marker_path(&cfg), b_marker("a process that is gone").to_string()).unwrap();
    assert!(matches!(drill::marker_work(&cfg), drill::MarkerWork::Unknown(_)), "{:?}", drill::marker_work(&cfg));
    let other = tempfile::tempdir().unwrap();
    let other_cfg_text = std::fs::read_to_string(dir.path().join("drill.toml")).unwrap()
        .replace(&format!("journal_path = \"{d}/journal.jsonl\""), &format!("journal_path = \"{}/journal.jsonl\"", other.path().display()));
    let p = other.path().join("c.toml");
    std::fs::write(&p, other_cfg_text).unwrap();
    let other_cfg = Config::load(&p).unwrap();
    assert!(drill::foreign_marker_blocker(&other_cfg).unwrap().contains("clear-marker"));
    // And a new drill on this producer refuses while it is unknown.
    assert!(drill::run(&cfg, &chain, &DrillOptions { height: 1_200, keep_work: false }).unwrap_err().contains("unreadable"));
}

/// R7-F3 (reviewer): an orphaned work marker (the drill died, or docker was down when it ended) blocked a fresh mainnet
/// ARM forever, and only that drill's own config could clear it. `drill --clear-marker` works from ANY config on the
/// producer: under the locks, it removes the marker only when the drill process, its pipeline group and its export
/// container are all proven gone; anything unknown refuses with nothing changed. `status`/the beacon show the marker.
#[test]
fn r7_clear_marker_removes_an_orphan_only_when_its_work_is_proven_gone() {
    let dir = tempfile::tempdir().unwrap();
    stage_tools(dir.path(), true);
    let d = dir.path().display().to_string();
    let cfg = drill_config_edit(dir.path(), MAINNET, "",
        |t: String| t.replacen("[ceremony]\n", &format!("[ceremony]\nlock_dir = \"{d}/locks\"\n"), 1)).unwrap();
    let chain = LiveChain::new(dir.path(), MAINNET, 1_000);
    std::fs::create_dir_all(format!("{d}/locks")).unwrap();
    // An orphan: drill X died before publishing (dead pid), container name recorded, journal dir elsewhere.
    let elsewhere = tempfile::tempdir().unwrap();
    let name = "pulse-drill-1100-cccccccccccc";
    let orphan = serde_json::json!({"kind": "readiness_drill_work", "journal_dir": elsewhere.path(), "h": 1_100,
        "drill_id": "c".repeat(32), "export_container": name, "pid": std::process::id(), "pid_start": "gone"});
    std::fs::write(drill::work_marker_path(&cfg), orphan.to_string()).unwrap();
    assert!(matches!(drill::marker_work(&cfg), drill::MarkerWork::Unknown(_)));
    // Shown before the event.
    let view = drill::beacon_view(&cfg).expect("the marker is shown even without a drill record here");
    assert_eq!(view["work_marker"]["state"], "unknown");
    assert_eq!(view["work_marker"]["h"], 1_100);

    // 1. The container still exists: refused, marker kept.
    chain.containers.borrow_mut().insert(name.into());
    let e = drill::clear_marker(&cfg, &chain).unwrap_err();
    assert!(e.contains("not verified gone") && e.contains("nothing changed"), "{e}");
    assert!(drill::work_marker_path(&cfg).exists());
    // 2. The daemon cannot be reached: unknown, refused, marker kept.
    chain.containers.borrow_mut().clear();
    *chain.docker_down.borrow_mut() = true;
    let e = drill::clear_marker(&cfg, &chain).unwrap_err();
    assert!(e.contains("Unknown"), "{e}");
    assert!(drill::work_marker_path(&cfg).exists());
    *chain.docker_down.borrow_mut() = false;
    // 3. Its drill process is alive: refused (that is --cancel's job).
    let mut live = orphan.clone();
    live["pid_start"] = serde_json::json!(pulse_cutover::ops::process_start(std::process::id()).unwrap());
    std::fs::write(drill::work_marker_path(&cfg), live.to_string()).unwrap();
    assert!(drill::clear_marker(&cfg, &chain).unwrap_err().contains("still running"));
    std::fs::write(drill::work_marker_path(&cfg), orphan.to_string()).unwrap();
    // 4. A ceremony holds this journal: refused.
    let held = pulse_cutover::journal::Journal::lock(&cfg.journal_path).unwrap();
    assert!(drill::clear_marker(&cfg, &chain).unwrap_err().contains("holds this journal"));
    drop(held);
    // 5. Everything proven gone: removed, recorded, and a ceremony is no longer blocked.
    let msg = drill::clear_marker(&cfg, &chain).unwrap();
    assert!(msg.contains("removed"), "{msg}");
    assert!(!drill::work_marker_path(&cfg).exists());
    assert!(drill::drill_root(&cfg).join(format!("marker-cleared-{}.json", "c".repeat(32))).exists());
    assert!(drill::foreign_marker_blocker(&cfg).is_none());
    assert!(chain.argvs.borrow().iter().all(|a| a[0].ends_with("/docker") && a[3] == "inspect"), "only `docker inspect` ran");
    for f in FORBIDDEN {
        assert!(!chain.called(f), "clear-marker called {f}");
    }
    // Nothing to clear: a no-op.
    assert!(drill::clear_marker(&cfg, &chain).unwrap().contains("no drill work marker"));
}
