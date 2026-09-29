//! `pulse-cutover beacon`: readiness + ceremony status reporter for mission control.
//!
//! Runs next to (never inside) the ceremony: every `interval_secs` it checks the
//! box's readiness, summarizes the journal into comparable evidence (cut id,
//! snapshot sha256, fingerprint digest, state-diff digest) and POSTs one JSON
//! report to the `[beacon] url` with a bearer token. Fire-and-forget: if mission
//! control is down the beacon logs and retries; the ceremony never waits on it.
//! Read-only on the box: it calls nodeos' chain API, reads files, and asks
//! systemd whether a unit is active. The only thing it ever writes is its own
//! `beacon.instance` id (once).
//!
//! Public by design: mission control's dashboard is public, so every string in a
//! report is either a fixed verdict, a number, a public chain/Metal identifier, or
//! passed through `sanitize_short` (no paths, URLs, IPs, commands).
//!
//! "Ready" here is PREPARATION telemetry, not cutover eligibility: validator membership,
//! funding, the approved release and the event roster are decided elsewhere.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::Config;

pub const REPORT_SCHEMA: &str = "pulse-cutover-beacon-v1";

/// Whole-cycle collection budget: a slow nodeos/metalgo must not make a report late enough
/// to look "silent" on mission control (its threshold is 20 s).
const CYCLE_BUDGET: Duration = Duration::from_secs(8);

fn check(name: &str, ok: bool, detail: impl Into<String>) -> Value {
    json!({"name": name, "ok": ok, "detail": detail.into()})
}

/// Short, public-safe error class: first line, no URLs, paths, IP addresses, backticked
/// commands or long hex/base64 blobs, at most 80 chars.
pub fn sanitize_short(s: &str) -> String {
    static RES: std::sync::OnceLock<Vec<(regex::Regex, &'static str)>> = std::sync::OnceLock::new();
    let res = RES.get_or_init(|| {
        [
            (r"`[^`]*`", "<cmd>"),
            (r"[a-zA-Z][a-zA-Z0-9+.-]*://\S+", "<url>"),
            (r"\b\d{1,3}(\.\d{1,3}){3}(:\d+)?\b", "<ip>"),
            (r"\[?[0-9a-fA-F]{0,4}(:[0-9a-fA-F]{0,4}){2,7}\]?(:\d+)?", "<ip>"),
            (r"(~|\.{1,2})?/[^\s:;,'\x22)]+", "<path>"),
            (r"[A-Za-z0-9+/=_-]{40,}", "<blob>"),
        ]
        .iter()
        .map(|(p, r)| (regex::Regex::new(p).expect("static regex"), *r))
        .collect()
    });
    let mut out = s.lines().next().unwrap_or("").to_string();
    for (re, rep) in res {
        out = re.replace_all(&out, *rep).into_owned();
    }
    let out = out.trim();
    if out.chars().count() > 80 {
        format!("{}…", out.chars().take(79).collect::<String>())
    } else {
        out.to_string()
    }
}

/// Per-cycle time budget; each probe gets min(per-probe cap, what is left).
struct Budget(Instant);

impl Budget {
    fn new() -> Self {
        Budget(Instant::now() + CYCLE_BUDGET)
    }
    fn agent(&self, cap: Duration) -> Option<ureq::Agent> {
        let left = self.0.saturating_duration_since(Instant::now());
        if left < Duration::from_millis(200) {
            return None;
        }
        Some(ureq::AgentBuilder::new().timeout(cap.min(left)).build())
    }
}

fn post_json(a: &ureq::Agent, url: &str, body: Value) -> Result<Value, String> {
    a.post(url)
        .send_json(body)
        .map_err(|e| e.to_string())?
        .into_json::<Value>()
        .map_err(|e| e.to_string())
}

/// Base URL of the local metalgo HTTP API, derived from the target rpc_url (…:9650/ext/bc/<id>/rpc).
fn metal_base(cfg: &Config) -> String {
    let u = &cfg.target.rpc_url;
    match u.find("/ext/") { Some(i) => u[..i].to_string(), None => "http://127.0.0.1:9650".into() }
}

fn metal_rpc(b: &Budget, base: &str, path: &str, method: &str, params: Value) -> Option<Value> {
    let a = b.agent(Duration::from_secs(2))?;
    post_json(&a, &format!("{base}{path}"), json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .ok()
        .and_then(|v| v.get("result").cloned())
}

/// The Metal network a source chain is expected to move onto (XPR mainnet → Metal mainnet 1,
/// XPR testnet → Tahoe 5). Unknown chains → None (reported, not judged).
pub fn expected_metal_network(chain_id: Option<&str>) -> Option<u64> {
    match chain_id? {
        "384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0" => Some(1),
        "71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd" => Some(5),
        _ => None,
    }
}

/// Last port-reachability verdict from mission control (it dials this server's IP on 9651 only).
/// Asked at most every 10 minutes; `None` until the first answer.
static REACH: Mutex<Option<(Instant, bool)>> = Mutex::new(None);

fn staking_reachable(cfg: &Config, b: &Budget) -> Option<bool> {
    let url = cfg.beacon.as_ref().map(|b| b.url.clone()).unwrap_or_default();
    let base = url.trim_end_matches("/api/report");
    if base.is_empty() || base == url {
        return None;
    }
    let mut g = REACH.lock().ok()?;
    if let Some((t, ok)) = *g {
        if t.elapsed() < Duration::from_secs(600) {
            return Some(ok);
        }
    }
    let a = b.agent(Duration::from_secs(5))?;
    let ok = a.get(&format!("{base}/api/reach")).call().ok()?.into_json::<Value>().ok()?["reachable"].as_bool()?;
    *g = Some((Instant::now(), ok));
    Some(ok)
}

/// metalgo answers /ext/health with 200 when healthy and 503 (same JSON shape) when not.
fn metal_healthy(b: &Budget, base: &str) -> Option<bool> {
    let a = b.agent(Duration::from_secs(2))?;
    let doc = match a.get(&format!("{base}/ext/health")).call() {
        Ok(r) => r.into_json::<Value>().ok()?,
        Err(ureq::Error::Status(_, r)) => r.into_json::<Value>().ok()?,
        Err(_) => return None,
    };
    doc["healthy"].as_bool()
}

/// Public facts about the local Metal node. NodeID, BLS key and version are public on the P-Chain anyway;
/// never the IP address (mission control's page is public).
fn metal_info(cfg: &Config, b: &Budget) -> Value {
    let base = metal_base(cfg);
    let Some(id) = metal_rpc(b, &base, "/ext/info", "info.getNodeID", json!({})) else { return Value::Null };
    let ver = metal_rpc(b, &base, "/ext/info", "info.getNodeVersion", json!({}));
    let net = metal_rpc(b, &base, "/ext/info", "info.getNetworkID", json!({}));
    let peers = metal_rpc(b, &base, "/ext/info", "info.peers", json!({}));
    let boot = |c: &str| metal_rpc(b, &base, "/ext/info", "info.isBootstrapped", json!({"chain": c})).and_then(|v| v["isBootstrapped"].as_bool());
    let peer_n = peers.as_ref().and_then(|p| p["numPeers"].as_str().and_then(|n| n.parse::<u64>().ok()).or_else(|| p["peers"].as_array().map(|x| x.len() as u64)));
    let network_id = net.as_ref().and_then(|v| v["networkID"].as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| v["networkID"].as_u64()));
    json!({
        "node_id": id["nodeID"],
        "bls_public_key": id["nodePOP"]["publicKey"],
        "version": ver.as_ref().map(|v| v["version"].clone()).unwrap_or(Value::Null),
        "rpcchainvm": ver.as_ref().map(|v| v["rpcProtocolVersion"].clone()).unwrap_or(Value::Null),
        "network_id": network_id,
        "expected_network_id": expected_metal_network(cfg.ceremony.chain_id.as_deref()),
        "peers": peer_n,
        "bootstrapped": {"P": boot("P"), "X": boot("X"), "C": boot("C")},
        "healthy": metal_healthy(b, &base),
        "staking_reachable": staking_reachable(cfg, b),
    })
}

/// First word of a hook command is the program; a hook is ready when it resolves to an
/// executable file (the run-5 rehearsal stalled on a hook without +x). Bare names are looked
/// up on PATH like the shell would.
fn hook_ready(cmd: &str) -> (bool, String) {
    // Details are public on mission control: never include paths, only the verdict.
    let prog = cmd.split_whitespace().next().unwrap_or("");
    if prog.is_empty() {
        return (false, "configured but empty".into());
    }
    let candidates: Vec<PathBuf> = if prog.contains('/') {
        vec![PathBuf::from(prog)]
    } else {
        std::env::var_os("PATH")
            .map(|p| std::env::split_paths(&p).map(|d| d.join(prog)).collect())
            .unwrap_or_default()
    };
    for c in &candidates {
        if let Ok(m) = std::fs::metadata(c) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if !m.is_file() || m.permissions().mode() & 0o111 == 0 {
                    return (false, "configured but not executable (chmod +x)".into());
                }
            }
            let _ = m;
            return (true, "configured and executable".into());
        }
    }
    (false, if prog.contains('/') { "configured but the script does not exist" } else { "configured but the command is not on PATH" }.into())
}

fn free_gb(dir: &Path) -> Option<f64> {
    // The snapshots dir may not exist yet (nodeos creates it on the first snapshot): measure the
    // filesystem it will live on, i.e. the nearest existing ancestor.
    let dir = dir.ancestors().find(|p| p.exists()).unwrap_or(dir);
    let out = std::process::Command::new("df").arg("-Pk").arg(dir).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: f64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb / 1024.0 / 1024.0)
}

/// systemd ActiveState of a unit ("active", "activating", "inactive", "failed", …).
fn unit_state(unit: &str) -> String {
    std::process::Command::new("systemctl")
        .args(["is-active", unit])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

/// Stable per-server identity for mission control (so two servers can never overwrite each
/// other, whatever their display labels). 16 random bytes, generated once. Looked up next to
/// the token file first (installers may create it), then in the journal directory (the one
/// place the sandboxed beacon may write); if neither can be written, derived from the token.
pub fn instance_id(cfg: &Config) -> String {
    let token_dir = cfg.beacon.as_ref().and_then(|b| b.token_file.as_ref()).and_then(|p| p.parent().map(Path::to_path_buf));
    let journal_dir = cfg.journal_path.parent().map(Path::to_path_buf);
    let valid = |s: &str| s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit());
    for dir in [token_dir.as_ref(), journal_dir.as_ref()].into_iter().flatten() {
        if let Ok(t) = std::fs::read_to_string(dir.join("beacon.instance")) {
            let t = t.trim().to_lowercase();
            if valid(&t) {
                return t;
            }
        }
    }
    let mut buf = [0u8; 16];
    let random = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut buf)).is_ok();
    // A preview (`beacon --once` with no url: installer dry-runs) must not write anything: it
    // gets an ephemeral id and persists nothing.
    let persist = cfg.beacon.as_ref().map(|b| !b.url.is_empty()).unwrap_or(false);
    if random && !persist {
        return hex::encode(buf);
    }
    if random {
        let id = hex::encode(buf);
        if let Some(dir) = journal_dir.as_ref() {
            let _ = std::fs::create_dir_all(dir);
            if std::fs::write(dir.join("beacon.instance"), format!("{id}\n")).is_ok() {
                return id;
            }
        }
    }
    let token = cfg.beacon.as_ref().and_then(|b| b.token_file.as_ref()).and_then(|p| std::fs::read_to_string(p).ok()).unwrap_or_default();
    hex::encode(&Sha256::digest(format!("pulse-cutover-instance:{}", token.trim()).as_bytes())[..16])
}

/// Incremental journal reader state: the beacon runs every few seconds against a journal that
/// only grows, so each cycle reads just the NEW bytes (never the whole file again).
#[derive(Default, Clone)]
struct Acc {
    ident: (u64, u64),
    offset: u64,
    state: Value,
    last_ts: Value,
    seq: u64,
    transitions: Vec<Value>,
    ev: serde_json::Map<String, Value>,
    last_error: Option<String>,
    armed_ts_ms: Option<u64>,
    /// Ignition may have started on this box (ignite_started journaled, or IGNITED or later).
    ignition_started: bool,
    /// The journal ends in an operator rollback forced after ignition (`--force-after-ignite`).
    forced_rollback: bool,
}

static JOURNALS: Mutex<Option<HashMap<PathBuf, Acc>>> = Mutex::new(None);

/// Preparation ("setup") checks: not health. Same split as the dashboard's CHECKS map
/// (control/public/index.html); every other check is a HEALTH check.
pub const SETUP_CHECKS: &[&str] = &[
    "hook_on_freeze", "hook_post_ignite", "hook_on_live", "hook_on_abort",
    "validator_running", "metal_synced", "metal_reachable",
];

/// A report with any failing HEALTH check (a failing setup check does not count).
pub fn has_failing_health(report: &Value) -> bool {
    report["checks"].as_array().map(|cs| cs.iter().any(|c| {
        c["ok"].as_bool() == Some(false)
            && !c["name"].as_str().map(|n| SETUP_CHECKS.contains(&n)).unwrap_or(false)
    })).unwrap_or(false)
}

fn file_ident(m: &std::fs::Metadata) -> (u64, u64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (m.dev(), m.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = m;
        (0, 0)
    }
}

fn apply_line(acc: &mut Acc, v: &Value) {
    acc.seq = v["seq"].as_u64().unwrap_or(acc.seq);
    let d = &v["data"];
    // Evidence only ever ACCUMULATES: a record that lacks a field (e.g. the VERIFIED transition
    // written by `unhalt`) must not erase what an earlier record established.
    fn put(ev: &mut serde_json::Map<String, Value>, k: &str, val: &Value) {
        if !val.is_null() {
            ev.insert(k.into(), val.clone());
        }
    }
    if matches!(d["side_effect"].as_str(), Some("ignite_started" | "ignite")) {
        acc.ignition_started = true;
    }
    match v["kind"].as_str() {
        Some("transition") => {
            acc.state = v["state"].clone();
            if matches!(v["state"].as_str(), Some("IGNITED" | "FLIPPED" | "LIVE" | "HALTED")) {
                acc.ignition_started = true;
            }
            acc.forced_rollback = v["state"].as_str() == Some("ABORTED") && d["force_after_ignite"].as_bool() == Some(true);
            acc.last_ts = v["ts"].clone();
            acc.transitions.push(json!({"state": v["state"], "ts": v["ts"]}));
            let ev = &mut acc.ev;
            match v["state"].as_str() {
                Some("ARMED") => {
                    acc.armed_ts_ms = v["ts_ms"].as_u64();
                    put(ev, "h", &d["resolved_h"]);
                    put(ev, "chain_id", &d["chain_id"]);
                }
                Some("FROZEN") => put(ev, "freeze_at", &d["freeze_at"]),
                Some("SNAPSHOTTED") => {
                    put(ev, "cut_height", &d["cut_height"]);
                    put(ev, "cut_block_id", &d["cut_block_id"]);
                    put(ev, "burnoff_transactions", &d["burnoff_transactions"]);
                }
                Some("VERIFIED") => {
                    put(ev, "snapshot_sha256", &d["sha256"]);
                    if d["fingerprints"].is_object() {
                        let canon = serde_json::to_string(&d["fingerprints"]).unwrap_or_default();
                        // Full 256-bit digest: a shortened one would let different state collide.
                        ev.insert("fingerprints_digest".into(), json!(hex::encode(Sha256::digest(canon.as_bytes()))));
                    }
                }
                Some("IGNITED") => {
                    put(ev, "target_head_id", &d["target_head_id"]);
                    put(ev, "lineage_at_cut", &d["lineage_at_cut"]);
                }
                Some("LIVE") => put(ev, "write_gap_ms", &d["write_gap_ms_wallclock"]),
                // rc.7: the HALTED transition itself carries the reason (one durable record).
                Some("HALTED") => {
                    if let Some(m) = d["message"].as_str() {
                        acc.last_error = Some(m.to_string());
                    }
                }
                _ => {}
            }
        }
        Some("error") => acc.last_error = d["message"].as_str().map(String::from),
        _ => {}
    }
}

/// Condense the journal into what mission control compares across producers.
pub fn journal_summary(path: &Path) -> Value {
    let mut guard = JOURNALS.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    let acc = match std::fs::metadata(path) {
        Err(_) => {
            map.remove(path);
            Acc::default()
        }
        Ok(m) => {
            let ident = file_ident(&m);
            let mut acc = map.remove(path).unwrap_or_default();
            if acc.ident != ident || m.len() < acc.offset {
                acc = Acc { ident, ..Default::default() }; // replaced or truncated: start over
            }
            if m.len() > acc.offset {
                if let Ok(mut f) = std::fs::File::open(path) {
                    if f.seek(SeekFrom::Start(acc.offset)).is_ok() {
                        let mut buf = Vec::new();
                        if f.take(m.len() - acc.offset).read_to_end(&mut buf).is_ok() {
                            // Only complete lines; a partial last line is re-read next cycle.
                            if let Some(end) = buf.iter().rposition(|b| *b == b'\n') {
                                for line in buf[..end].split(|b| *b == b'\n') {
                                    if let Ok(v) = serde_json::from_slice::<Value>(line) {
                                        apply_line(&mut acc, &v);
                                    }
                                }
                                acc.offset += end as u64 + 1;
                            }
                        }
                    }
                }
            }
            map.insert(path.to_path_buf(), acc.clone());
            acc
        }
    };
    json!({"state": acc.state, "since": acc.last_ts, "seq": acc.seq, "transitions": acc.transitions,
           "evidence": Value::Object(acc.ev),
           // Never the raw message: errors can carry commands, paths and hosts.
           "last_error_class": acc.last_error.as_deref().map(sanitize_short),
           "armed_ts_ms": acc.armed_ts_ms,
           "ignition_started": acc.ignition_started,
           "forced_rollback": acc.forced_rollback})
}

/// Coordination status for the report, with free-text fields sanitized.
fn coord_public(cfg: &Config) -> Value {
    let mut c = crate::coord::read_state(cfg);
    if let Some(r) = c.get("reason").and_then(|r| r.as_str()).map(sanitize_short) {
        c["reason"] = json!(r);
    }
    c
}

pub fn build_report(cfg: &Config, producer: &str, network: &str) -> Value {
    let budget = Budget::new();
    let mut checks = vec![];
    let journal = journal_summary(&cfg.journal_path);
    let state = journal["state"].as_str().unwrap_or("").to_string();
    // HALTED: ignition may have started (the target may be running), so judge like post-ignite.
    // So is an ABORTED that followed ignition (a forced rollback): never judged as pre-ceremony.
    let past_ignite = matches!(state.as_str(), "IGNITED" | "FLIPPED" | "LIVE" | "HALTED")
        || (state == "ABORTED" && journal["ignition_started"].as_bool() == Some(true));
    // From VERIFIED on, the staged snapshot is supposed to exist, and ignition restarts the
    // validator: judge those checks by phase, not by the pre-ceremony rule.
    let past_verify = past_ignite || state == "VERIFIED";
    let in_ignite = matches!(state.as_str(), "VERIFIED" | "IGNITED");
    let skipped = || "skipped (collection time budget exhausted)".to_string();

    // Source chain.
    let info = match budget.agent(Duration::from_secs(3)) {
        Some(a) => post_json(&a, &format!("{}/v1/chain/get_info", cfg.source.rpc_url.trim_end_matches('/')), json!({})),
        None => Err(skipped()),
    };
    let (head, lib, chain_id) = match &info {
        Ok(v) => (v["head_block_num"].as_u64(), v["last_irreversible_block_num"].as_u64(), v["chain_id"].as_str().map(String::from)),
        Err(_) => (None, None, None),
    };
    checks.push(check("source_api", info.is_ok(), match &info { Ok(_) => format!("head {} lib {}", head.unwrap_or(0), lib.unwrap_or(0)), Err(_) => "not reachable".into() }));
    if let Some(want) = &cfg.ceremony.chain_id {
        let ok = chain_id.as_deref() == Some(want.as_str());
        checks.push(check("chain_id", ok, if ok { format!("{}…", &want[..16.min(want.len())]) } else { format!("node reports {}, config expects {}…", chain_id.as_deref().map(|c| format!("{}…", &c[..16.min(c.len())])).unwrap_or_else(|| "nothing".into()), &want[..16.min(want.len())]) }));
    }
    if cfg.ceremony.mode == crate::config::Mode::Producer {
        let p = match budget.agent(Duration::from_secs(2)) {
            Some(a) => post_json(&a, &format!("{}/v1/producer/paused", cfg.source.producer_api_url.trim_end_matches('/')), json!({})),
            None => Err(skipped()),
        };
        checks.push(check("producer_api", p.is_ok(), match &p { Ok(_) => "reachable locally".to_string(), Err(_) => "not reachable locally".into() }));
    }

    // Declared H (only meaningful for a ceremony config; readiness configs have none).
    if cfg.ceremony.profile == crate::config::Profile::Ceremony {
        let h = cfg.ceremony.freeze_height;
        if h > 0 {
            let ok = past_ignite || state == "LIVE" || head.map(|x| x < h || !state.is_empty()).unwrap_or(false);
            checks.push(check("freeze_height", ok, format!("H = {h}{}", head.map(|x| if x < h { format!(", {} blocks away", h - x) } else { String::new() }).unwrap_or_default())));
        } else {
            checks.push(check("freeze_height", cfg.ceremony.derive_h_at_arm, "H derived at ARM from LIB + freeze_margin (rehearsal)"));
        }
        if cfg.ceremony.freeze_strategy == crate::config::FreezeStrategy::ScheduleAtH {
            checks.push(check("freeze_lead_blocks", cfg.ceremony.freeze_lead_blocks > 0, format!("writes close {} blocks before H", cfg.ceremony.freeze_lead_blocks)));
        }
    }

    // Hooks.
    let hooks = [("on_freeze", &cfg.hooks.on_freeze), ("post_ignite", &cfg.hooks.post_ignite), ("on_live", &cfg.hooks.on_live), ("on_abort", &cfg.hooks.on_abort)];
    for (name, h) in hooks {
        if let Some(cmd) = h {
            let (ok, d) = hook_ready(cmd);
            checks.push(check(&format!("hook_{name}"), ok, d));
        } else if cfg.ceremony.mode == crate::config::Mode::Producer && matches!(name, "on_freeze" | "post_ignite") {
            checks.push(check(&format!("hook_{name}"), false, "not configured (needed for a multi-producer ceremony)"));
        }
    }

    // Target side.
    let staged = &cfg.snapshot.staged_path;
    let staged_ok = past_verify || !staged.exists();
    checks.push(check("staged_snapshot_absent", staged_ok, if past_verify { "staged by the ceremony (expected)".to_string() } else if staged_ok { "not pre-staged".to_string() } else { "a snapshot is already staged (would boot the target from a stale cut)".to_string() }));
    let unit = &cfg.target.metalgo_unit;
    let ustate = unit_state(unit);
    let active = ustate == "active";
    let metal = if active { metal_info(cfg, &budget) } else { Value::Null };
    // Mid-ignition the unit legitimately restarts, but only "activating" counts — a journal
    // saying "ignition" never makes a stopped or failed validator look healthy.
    let restarting = in_ignite && matches!(ustate.as_str(), "activating" | "reloading");
    let vdetail = match metal["version"].as_str() {
        Some(v) if active => format!("running · {}", v.trim_start_matches("metalgo/")),
        _ if active => "running".to_string(),
        _ if restarting => "restarting for ignition".to_string(),
        _ => format!("not running ({ustate})"),
    };
    checks.push(check("validator_running", active || restarting, vdetail));
    if active {
        if metal.is_null() {
            checks.push(check("metal_synced", false, "Metal API not answering"));
        } else {
            let p = metal["bootstrapped"]["P"].as_bool().unwrap_or(false);
            let peers = metal["peers"].as_u64().unwrap_or(0);
            let (net, want) = (metal["network_id"].as_u64(), metal["expected_network_id"].as_u64());
            let net_ok = match (net, want) { (Some(n), Some(w)) => n == w, _ => true };
            let detail = if !net_ok {
                format!("on Metal network {}, this chain moves to network {}", net.unwrap_or(0), want.unwrap_or(0))
            } else if p {
                format!("P-Chain synced · {peers} peers")
            } else {
                format!("P-Chain syncing · {peers} peers")
            };
            checks.push(check("metal_synced", p && net_ok, detail));
            // Visible even when unknown: an untested staking port is not a ready one.
            match metal["staking_reachable"].as_bool() {
                Some(r) => checks.push(check("metal_reachable", r, if r { "port 9651 reachable from the internet" } else { "port 9651 not reachable from the internet" })),
                None => checks.push(check("metal_reachable", false, "not tested yet")),
            }
        }
    }
    if let Some(dir) = cfg.snapshot.dir.as_ref().or(staged.parent().map(|p| p.to_path_buf()).as_ref()) {
        let gb = free_gb(dir);
        checks.push(check("disk_free", gb.map(|g| g >= 5.0).unwrap_or(false), gb.map(|g| format!("{:.0} GB free", g.floor())).unwrap_or_else(|| "unknown".into())));
    }

    // State diff (A3), if the post_ignite hook produced one next to the journal — but only if it
    // belongs to THIS ceremony: written after ARM, and about a head at/after the cut.
    let mut journal = journal;
    if let Some(dir) = cfg.journal_path.parent() {
        let p = dir.join("state-diff.json");
        if let (Ok(t), Ok(m)) = (std::fs::read_to_string(&p), std::fs::metadata(&p)) {
            if let Ok(v) = serde_json::from_str::<Value>(&t) {
                let mtime_ms = m.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64);
                let after_arm = match (mtime_ms, journal["armed_ts_ms"].as_u64()) { (Some(mt), Some(a)) => mt >= a, _ => false };
                let cut = journal["evidence"]["cut_height"].as_u64();
                let covers_cut = match (v["b"]["head_block_num"].as_u64(), cut) { (Some(b), Some(c)) => b >= c, (_, None) => false, _ => false };
                if after_arm && covers_cut {
                    journal["evidence"]["state_diff_identical"] = v["identical"].clone();
                    journal["evidence"]["state_digest"] = v["b"]["digest"].clone();
                    journal["evidence"]["state_diff_b_head"] = v["b"]["head_block_num"].clone();
                } else {
                    journal["evidence"]["state_diff_ignored"] = json!("state-diff.json predates this ceremony or does not cover the cut");
                }
            }
        }
    }
    if let Some(o) = journal.as_object_mut() {
        o.remove("armed_ts_ms");
    }

    let ready = checks.iter().all(|c| c["ok"].as_bool().unwrap_or(false));
    let b = cfg.beacon.as_ref();
    json!({
        "schema": REPORT_SCHEMA,
        "producer": producer,
        "network": network,
        // Stable identity of THIS server (labels are editable, ids are not).
        "instance_id": instance_id(cfg),
        // Public on mission control: the operator's chosen label, never the machine's hostname.
        "node": b.and_then(|b| b.node.clone()).unwrap_or_else(|| {
            b.and_then(|b| b.role.clone()).unwrap_or_else(|| "node".into())
        }),
        "role": b.and_then(|b| b.role.clone()).unwrap_or_else(|| {
            if cfg.ceremony.mode == crate::config::Mode::Producer { "producer".into() } else { "api".into() }
        }),
        "interval_secs": b.map(|b| b.interval_secs).unwrap_or(0),
        "agent_version": env!("CARGO_PKG_VERSION"),
        "ts": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "mode": format!("{:?}", cfg.ceremony.mode).to_lowercase(),
        "profile": format!("{:?}", cfg.ceremony.profile).to_lowercase(),
        // Preparation telemetry only — see module docs.
        "ready": ready,
        "checks": checks,
        "source": {"head": head, "lib": lib, "chain_id": chain_id},
        "ceremony": journal,
        "coord": coord_public(cfg),
        "metal": metal,
    })
}

/// Loop forever (or once), posting reports. Never exits on a delivery failure.
pub fn run(cfg: &Config, once: bool) -> Result<(), String> {
    let b = cfg.beacon.as_ref().ok_or("config has no [beacon] section")?;
    crate::config::check_beacon_url(&b.url)?;
    let token = match &b.token_file {
        Some(p) => std::fs::read_to_string(p).map_err(|e| format!("token_file {}: {e}", p.display()))?.trim().to_string(),
        None => String::new(),
    };
    let a = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();
    loop {
        let started = Instant::now();
        let report = build_report(cfg, &b.producer, &b.network);
        if once && b.url.is_empty() {
            println!("{}", serde_json::to_string_pretty(&report).unwrap_or_default());
            return Ok(());
        }
        let mut req = a.post(&b.url);
        if !token.is_empty() {
            req = req.set("Authorization", &format!("Bearer {token}"));
        }
        match req.send_json(report.clone()) {
            Ok(_) => {
                if once {
                    println!("{}", serde_json::to_string_pretty(&report).unwrap_or_default());
                    return Ok(());
                }
            }
            Err(e) => {
                eprintln!("beacon: delivery failed ({}); will retry", sanitize_short(&e.to_string()));
                if once {
                    return Err(format!("delivery failed: {e}"));
                }
            }
        }
        // Keep the cadence: the interval counts from the start of the cycle, not its end.
        let interval = Duration::from_secs(b.interval_secs.max(1));
        std::thread::sleep(interval.saturating_sub(started.elapsed()).max(Duration::from_millis(500)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_short_strips_paths_urls_ips_and_commands() {
        let s = sanitize_short("`/opt/hooks/flip.sh --key abc` exited 1: connect to http://10.0.0.5:8888/v1 failed; see /var/log/x.log");
        assert!(!s.contains("/opt"), "{s}");
        assert!(!s.contains("10.0.0.5"), "{s}");
        assert!(!s.contains("http"), "{s}");
        assert!(!s.contains("/var/log"), "{s}");
        assert!(s.chars().count() <= 80, "{s}");
        assert_eq!(sanitize_short("line one\nsecret second line"), "line one");
    }

    #[test]
    fn journal_summary_is_incremental_full_digest_and_sanitized() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("j.jsonl");
        let line = |seq: u64, kind: &str, state: &str, data: Value| {
            format!("{}\n", json!({"seq": seq, "ts_ms": 1000 + seq, "ts": "t", "kind": kind, "state": state, "data": data}))
        };
        std::fs::write(&p, line(0, "transition", "ARMED", json!({"resolved_h": 100, "chain_id": "ab"}))).unwrap();
        let s1 = journal_summary(&p);
        assert_eq!(s1["state"], "ARMED");
        assert_eq!(s1["evidence"]["h"], 100);
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(line(1, "transition", "VERIFIED", json!({"sha256": "ff", "fingerprints": {"a": "1"}})).as_bytes()).unwrap();
        f.write_all(line(2, "error", "VERIFIED", json!({"message": "`/opt/x.sh` failed at /var/lib/y"})).as_bytes()).unwrap();
        f.write_all(b"{\"partial").unwrap(); // torn tail: not consumed
        let s2 = journal_summary(&p);
        assert_eq!(s2["state"], "VERIFIED");
        assert_eq!(s2["evidence"]["h"], 100, "earlier facts survive incremental reads");
        assert_eq!(s2["evidence"]["fingerprints_digest"].as_str().unwrap().len(), 64);
        let e = s2["last_error_class"].as_str().unwrap();
        assert!(!e.contains("/opt") && !e.contains("/var"), "{e}");
        assert!(s2.get("last_error").is_none());
    }

    #[test]
    fn hook_ready_resolves_bare_names_on_path_and_rejects_missing() {
        assert!(hook_ready("sh -c true").0, "sh is on PATH");
        assert!(!hook_ready("definitely-not-a-real-command-xyz").0);
        assert!(!hook_ready("/nonexistent/hook.sh").0);
    }

    #[test]
    fn expected_metal_network_maps_xpr_chains() {
        assert_eq!(expected_metal_network(Some("71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd")), Some(5));
        assert_eq!(expected_metal_network(Some("384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0")), Some(1));
        assert_eq!(expected_metal_network(Some("00")), None);
    }
}
