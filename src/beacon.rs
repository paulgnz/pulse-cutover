//! `pulse-cutover beacon`: readiness + ceremony status reporter for mission control.
//!
//! Runs next to (never inside) the ceremony: every `interval_secs` it checks the
//! box's readiness, summarizes the journal into comparable evidence (cut id,
//! snapshot sha256, fingerprint digest, state-diff digest) and POSTs one JSON
//! report to the `[beacon] url` with a bearer token. Fire-and-forget: if mission
//! control is down the beacon logs and retries; the ceremony never waits on it.
//! Read-only on the box: it calls nodeos' chain API, reads files, and asks
//! systemd whether a unit is active. It never changes anything.

use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::Config;

pub const REPORT_SCHEMA: &str = "pulse-cutover-beacon-v1";

fn check(name: &str, ok: bool, detail: impl Into<String>) -> Value {
    json!({"name": name, "ok": ok, "detail": detail.into()})
}

fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new().timeout(Duration::from_secs(3)).build()
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

fn metal_rpc(a: &ureq::Agent, base: &str, path: &str, method: &str, params: Value) -> Option<Value> {
    post_json(a, &format!("{base}{path}"), json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .ok()
        .and_then(|v| v.get("result").cloned())
}

/// Last port-reachability verdict from mission control (it dials this server's IP on 9651 only).
/// Asked at most every 10 minutes; `None` until the first answer.
static REACH: std::sync::Mutex<Option<(std::time::Instant, bool)>> = std::sync::Mutex::new(None);

fn staking_reachable(cfg: &Config) -> Option<bool> {
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
    let a = ureq::AgentBuilder::new().timeout(Duration::from_secs(8)).build();
    let ok = a.get(&format!("{base}/api/reach")).call().ok()?.into_json::<Value>().ok()?["reachable"].as_bool()?;
    *g = Some((std::time::Instant::now(), ok));
    Some(ok)
}

/// Public facts about the local Metal node. NodeID, BLS key and version are public on the P-Chain anyway;
/// never the IP address (mission control's page is public).
fn metal_info(cfg: &Config) -> Value {
    let a = agent();
    let base = metal_base(cfg);
    let id = metal_rpc(&a, &base, "/ext/info", "info.getNodeID", json!({}));
    let Some(id) = id else { return Value::Null };
    let ver = metal_rpc(&a, &base, "/ext/info", "info.getNodeVersion", json!({}));
    let net = metal_rpc(&a, &base, "/ext/info", "info.getNetworkID", json!({}));
    let peers = metal_rpc(&a, &base, "/ext/info", "info.peers", json!({}));
    let boot = |c: &str| metal_rpc(&a, &base, "/ext/info", "info.isBootstrapped", json!({"chain": c})).and_then(|v| v["isBootstrapped"].as_bool());
    let health = a.get(&format!("{base}/ext/health")).call().ok().and_then(|r| r.into_json::<Value>().ok()).and_then(|v| v["healthy"].as_bool());
    let peer_n = peers.as_ref().and_then(|p| p["numPeers"].as_str().and_then(|n| n.parse::<u64>().ok()).or_else(|| p["peers"].as_array().map(|x| x.len() as u64)));
    json!({
        "node_id": id["nodeID"],
        "bls_public_key": id["nodePOP"]["publicKey"],
        "version": ver.as_ref().map(|v| v["version"].clone()).unwrap_or(Value::Null),
        "rpcchainvm": ver.as_ref().map(|v| v["rpcProtocolVersion"].clone()).unwrap_or(Value::Null),
        "network_id": net.as_ref().map(|v| v["networkID"].clone()).unwrap_or(Value::Null),
        "peers": peer_n,
        "bootstrapped": {"P": boot("P"), "X": boot("X"), "C": boot("C")},
        "healthy": health,
        "staking_reachable": staking_reachable(cfg),
    })
}

/// First word of a hook command is the program; a hook is ready when it exists
/// and is executable (the run-5 rehearsal stalled on a hook without +x).
fn hook_ready(cmd: &str) -> (bool, String) {
    // Details are public on mission control: never include paths, only the verdict.
    let prog = cmd.split_whitespace().next().unwrap_or("");
    if !prog.starts_with('/') {
        return (true, "configured (command on PATH)".into());
    }
    match std::fs::metadata(prog) {
        Ok(m) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if m.permissions().mode() & 0o111 == 0 {
                    return (false, "configured but not executable (chmod +x)".into());
                }
            }
            let _ = m;
            (true, "configured and executable".into())
        }
        Err(_) => (false, "configured but the script does not exist".into()),
    }
}

fn free_gb(dir: &Path) -> Option<f64> {
    let out = std::process::Command::new("df").arg("-Pk").arg(dir).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: f64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb / 1024.0 / 1024.0)
}

fn unit_active(unit: &str) -> bool {
    std::process::Command::new("systemctl")
        .args(["is-active", "--quiet", unit])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Condense the journal into what mission control compares across producers.
pub fn journal_summary(path: &Path) -> Value {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let mut state = Value::Null;
    let mut last_ts = Value::Null;
    let mut seq = 0u64;
    let mut transitions = vec![];
    let mut ev = serde_json::Map::new();
    let mut last_error = Value::Null;
    for line in text.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else { continue };
        seq = v["seq"].as_u64().unwrap_or(seq);
        let d = &v["data"];
        match v["kind"].as_str() {
            Some("transition") => {
                state = v["state"].clone();
                last_ts = v["ts"].clone();
                transitions.push(json!({"state": v["state"], "ts": v["ts"]}));
                match v["state"].as_str() {
                    Some("ARMED") => {
                        ev.insert("h".into(), d["resolved_h"].clone());
                        ev.insert("chain_id".into(), d["chain_id"].clone());
                    }
                    Some("FROZEN") => {
                        ev.insert("freeze_at".into(), d["freeze_at"].clone());
                    }
                    Some("SNAPSHOTTED") => {
                        ev.insert("cut_height".into(), d["cut_height"].clone());
                        ev.insert("cut_block_id".into(), d["cut_block_id"].clone());
                        ev.insert("burnoff_transactions".into(), d["burnoff_transactions"].clone());
                    }
                    Some("VERIFIED") => {
                        ev.insert("snapshot_sha256".into(), d["sha256"].clone());
                        if d["fingerprints"].is_object() {
                            let canon = serde_json::to_string(&d["fingerprints"]).unwrap_or_default();
                            let dig = hex::encode(Sha256::digest(canon.as_bytes()));
                            ev.insert("fingerprints_digest".into(), json!(&dig[..16]));
                        }
                    }
                    Some("IGNITED") => {
                        ev.insert("target_head_id".into(), d["target_head_id"].clone());
                    }
                    Some("LIVE") => {
                        ev.insert("write_gap_ms".into(), d["write_gap_ms_wallclock"].clone());
                    }
                    _ => {}
                }
            }
            Some("error") => last_error = d["message"].clone(),
            _ => {}
        }
    }
    json!({"state": state, "since": last_ts, "seq": seq, "transitions": transitions,
           "evidence": Value::Object(ev), "last_error": last_error})
}

pub fn build_report(cfg: &Config, producer: &str, network: &str) -> Value {
    let a = agent();
    let mut checks = vec![];
    let journal = journal_summary(&cfg.journal_path);
    let state = journal["state"].as_str().unwrap_or("").to_string();
    let past_ignite = matches!(state.as_str(), "IGNITED" | "FLIPPED" | "LIVE");
    // From VERIFIED on, the staged snapshot is supposed to exist, and ignition restarts the
    // validator: judge those checks by phase, not by the pre-ceremony rule.
    let past_verify = past_ignite || state == "VERIFIED";
    let in_ignite = matches!(state.as_str(), "VERIFIED" | "IGNITED");

    // Source chain.
    let info = post_json(&a, &format!("{}/v1/chain/get_info", cfg.source.rpc_url.trim_end_matches('/')), json!({}));
    let (head, lib, chain_id) = match &info {
        Ok(v) => (v["head_block_num"].as_u64(), v["last_irreversible_block_num"].as_u64(), v["chain_id"].as_str().map(String::from)),
        Err(_) => (None, None, None),
    };
    checks.push(check("source_api", info.is_ok(), match &info { Ok(_) => format!("head {} lib {}", head.unwrap_or(0), lib.unwrap_or(0)), Err(_) => "not reachable".into() }));
    if let Some(want) = &cfg.ceremony.chain_id {
        let ok = chain_id.as_deref() == Some(want.as_str());
        checks.push(check("chain_id", ok, if ok { format!("{}…", &want[..16.min(want.len())]) } else { format!("node reports {:?}, config expects {}…", chain_id, &want[..16.min(want.len())]) }));
    }
    if cfg.ceremony.mode == crate::config::Mode::Producer {
        let p = post_json(&a, &format!("{}/v1/producer/paused", cfg.source.producer_api_url.trim_end_matches('/')), json!({}));
        checks.push(check("producer_api", p.is_ok(), match &p { Ok(_) => "reachable locally".to_string(), Err(_) => "not reachable locally".into() }));
    }

    // Declared H.
    let h = cfg.ceremony.freeze_height;
    if h > 0 {
        let ok = past_ignite || state == "LIVE" || head.map(|x| x < h || !state.is_empty()).unwrap_or(false);
        checks.push(check("freeze_height", ok, format!("H = {h}{}", head.map(|x| if x < h { format!(", {} blocks away", h - x) } else { String::new() }).unwrap_or_default())));
    } else {
        checks.push(check("freeze_height", cfg.ceremony.freeze_margin.is_some(), "H derived at ARM from LIB + freeze_margin"));
    }
    if cfg.ceremony.freeze_strategy == crate::config::FreezeStrategy::ScheduleAtH {
        checks.push(check("freeze_lead_blocks", cfg.ceremony.freeze_lead_blocks > 0, format!("writes close {} blocks before H", cfg.ceremony.freeze_lead_blocks)));
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
    let active = unit_active(unit);
    let metal = if active { metal_info(cfg) } else { Value::Null };
    let vdetail = match metal["version"].as_str() {
        Some(v) if active => format!("running · {}", v.trim_start_matches("metalgo/")),
        _ if active => "running".to_string(),
        _ if in_ignite => "restarting for ignition".to_string(),
        _ => "not running".to_string(),
    };
    checks.push(check("validator_running", active || in_ignite, vdetail));
    if active && !metal.is_null() {
        let p = metal["bootstrapped"]["P"].as_bool().unwrap_or(false);
        let peers = metal["peers"].as_u64().unwrap_or(0);
        checks.push(check("metal_synced", p, if p { format!("P-Chain synced · {peers} peers") } else { format!("P-Chain syncing · {peers} peers") }));
        match metal["staking_reachable"].as_bool() {
            Some(r) => checks.push(check("metal_reachable", r, if r { "port 9651 reachable from the internet" } else { "port 9651 not reachable from the internet" })),
            None => {}
        }
    }
    if let Some(dir) = cfg.snapshot.dir.as_ref().or(staged.parent().map(|p| p.to_path_buf()).as_ref()) {
        let gb = free_gb(dir);
        checks.push(check("disk_free", gb.map(|g| g >= 5.0).unwrap_or(false), gb.map(|g| format!("{:.0} GB free", g.floor())).unwrap_or_else(|| "unknown".into())));
    }

    // State diff (A3), if the post_ignite hook produced one next to the journal.
    let mut journal = journal;
    if let Some(dir) = cfg.journal_path.parent() {
        if let Ok(t) = std::fs::read_to_string(dir.join("state-diff.json")) {
            if let Ok(v) = serde_json::from_str::<Value>(&t) {
                journal["evidence"]["state_diff_identical"] = v["identical"].clone();
                journal["evidence"]["state_digest"] = json!(v["b"]["digest"].as_str().map(|d| &d[..16.min(d.len())]));
                journal["evidence"]["state_diff_b_head"] = v["b"]["head_block_num"].clone();
            }
        }
    }

    let ready = checks.iter().all(|c| c["ok"].as_bool().unwrap_or(false));
    json!({
        "schema": REPORT_SCHEMA,
        "producer": producer,
        "network": network,
        // Public on mission control: the operator's chosen label, never the machine's hostname.
        "node": cfg.beacon.as_ref().and_then(|b| b.node.clone()).unwrap_or_else(|| {
            cfg.beacon.as_ref().and_then(|b| b.role.clone()).unwrap_or_else(|| "node".into())
        }),
        "role": cfg.beacon.as_ref().and_then(|b| b.role.clone()).unwrap_or_else(|| {
            if cfg.ceremony.mode == crate::config::Mode::Producer { "producer".into() } else { "api".into() }
        }),
        "agent_version": env!("CARGO_PKG_VERSION"),
        "ts": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "mode": format!("{:?}", cfg.ceremony.mode).to_lowercase(),
        "ready": ready,
        "checks": checks,
        "source": {"head": head, "lib": lib, "chain_id": chain_id},
        "ceremony": journal,
        "coord": crate::coord::read_state(cfg),
        "metal": metal,
    })
}

/// Loop forever (or once), posting reports. Never exits on a delivery failure.
pub fn run(cfg: &Config, once: bool) -> Result<(), String> {
    let b = cfg.beacon.as_ref().ok_or("config has no [beacon] section")?;
    let token = match &b.token_file {
        Some(p) => std::fs::read_to_string(p).map_err(|e| format!("token_file {}: {e}", p.display()))?.trim().to_string(),
        None => String::new(),
    };
    let a = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();
    loop {
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
                eprintln!("beacon: delivery failed ({e}); will retry");
                if once {
                    return Err(format!("delivery failed: {e}"));
                }
            }
        }
        std::thread::sleep(Duration::from_secs(b.interval_secs.max(1)));
    }
}
