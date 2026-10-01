//! Coordinated arming: every producer cuts at the same H because a coordinator publishes it once,
//! signed, instead of N operators typing it by hand.
//!
//! Trust model
//! - Messages are ed25519-signed by coordinator keys that each producer lists in its OWN config
//!   (`[coordination] coordinator_keys`). Mission control only relays them; it cannot forge one.
//! - Three message types, all for one `event_id`:
//!     event  — "the cut will be at H on this chain with these parameters" (published ahead of time)
//!     arm    — "go": the agent may start the ceremony for that event
//!     abort  — "stop": honoured only before IGNITED (after that, falling back would drop
//!              transactions already accepted by the new chain; that is a human decision)
//! - Every agent re-checks an event against its local config (chain_id, freeze lead, cpu scale)
//!   and the live head (H must be at least `min_lead_blocks` ahead) before accepting it.
//! - `auto_arm = false` (default): a signed arm ALSO needs a local confirmation file, so no remote
//!   party can start a ceremony on a box whose operator has not said yes.
//!
//! Wire format: `{"payload": "<json string>", "sig": "<128 hex>", "key": "<64 hex>"}`. The
//! signature covers the payload string's exact bytes (no canonicalization ambiguity).
//!
//! Event binding (optional fields, all covered by the signature):
//!   roster          [{"producer", "instance_id"?}] — the servers that must agree before ignition;
//!                   `await` writes it into the derived config and the fleet gate counts ONLY
//!                   these members, with fresh reports (see machine.rs `fleet_gate`)
//!   quorum          how many roster members must agree (default: all; never the local config's)
//!   release_sha256  the PulseVM plugin build; rejected if `target.plugin_path` hashes differently
//!                   or is not set (an unverifiable plugin refuses the event)
//!   snapshot_sha256 the expected cut snapshot hash, once known (becomes snapshot.expected_sha256)
//! Arm binding: an arm must carry `event_hash` = sha256 of the accepted event's exact signed payload
//! string (the relay enforces this too; the agent checks it itself in `check_arm`). An event is
//! identified by id AND payload hash: a different body under the same id is validated from scratch.
//! Still NOT done (next step): per-BP SIGNED acknowledgements. Today the fleet gate reads beacon
//! reports through the relay; a roster bounds WHO counts and freshness bounds WHEN, but a
//! compromised relay could still misreport them.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde_json::{json, Value};

use crate::config::Config;

/// Verify one signed message against the allowed keys. Returns the parsed payload.
pub fn verify(msg: &Value, allowed_keys: &[String]) -> Result<Value, String> {
    let payload = msg["payload"].as_str().ok_or("message has no payload")?;
    let key_hex = msg["key"].as_str().ok_or("message has no key")?.to_lowercase();
    if !allowed_keys.iter().any(|k| k.to_lowercase() == key_hex) {
        return Err(format!("key {}… is not a configured coordinator key", &key_hex[..12.min(key_hex.len())]));
    }
    let key: [u8; 32] = hex::decode(&key_hex).map_err(|e| e.to_string())?.try_into().map_err(|_| "key must be 32 bytes")?;
    let sig: [u8; 64] = hex::decode(msg["sig"].as_str().ok_or("message has no sig")?)
        .map_err(|e| e.to_string())?.try_into().map_err(|_| "sig must be 64 bytes")?;
    VerifyingKey::from_bytes(&key).map_err(|e| e.to_string())?
        .verify(payload.as_bytes(), &Signature::from_bytes(&sig))
        .map_err(|_| "signature does not verify".to_string())?;
    serde_json::from_str(payload).map_err(|e| format!("payload is not JSON: {e}"))
}

/// Check an `event` payload against this node's config and the live head.
pub fn validate_event(ev: &Value, cfg: &Config, network: &str, head: Option<u64>, min_lead: u64) -> Result<u64, String> {
    if ev["type"] != "event" { return Err("not an event message".into()); }
    if ev["network"].as_str() != Some(network) { return Err(format!("event is for network {}, this node is on {network}", ev["network"])); }
    if let Some(want) = &cfg.ceremony.chain_id {
        if ev["chain_id"].as_str() != Some(want.as_str()) { return Err("event chain_id differs from this node's config".into()); }
    }
    let h = ev["h"].as_u64().ok_or("event has no H")?;
    if let Some(v) = ev["freeze_lead_blocks"].as_u64() {
        if v != cfg.ceremony.freeze_lead_blocks { return Err(format!("event freeze_lead_blocks {v} ≠ local {}", cfg.ceremony.freeze_lead_blocks)); }
    }
    if let Some(v) = ev["import_cpu_scale"].as_u64() {
        if v != cfg.ceremony.import_cpu_scale { return Err(format!("event import_cpu_scale {v} ≠ local {}", cfg.ceremony.import_cpu_scale)); }
    }
    if let Some(head) = head {
        if h < head + min_lead { return Err(format!("H {h} is only {} blocks ahead of head {head} (minimum {min_lead})", h.saturating_sub(head))); }
    }
    if let Some(roster) = ev.get("roster").filter(|r| !r.is_null()) {
        let members: Vec<crate::config::RosterMember> =
            serde_json::from_value(roster.clone()).map_err(|e| format!("event roster is malformed: {e}"))?;
        if members.is_empty() { return Err("event roster is empty".into()); }
        crate::config::check_roster(&members).map_err(|e| format!("event {e}"))?;
        if let Some(q) = ev["quorum"].as_u64() {
            if q == 0 || q as usize > members.len() { return Err(format!("event quorum {q} is not within 1..={}", members.len())); }
        }
    }
    if let Some(want) = ev["release_sha256"].as_str() {
        if let Some(path) = &cfg.target.plugin_path {
            let (got, _) = crate::verify::sha256_file(path).map_err(|e| format!("cannot hash plugin {}: {e}", path.display()))?;
            if !got.eq_ignore_ascii_case(want) {
                return Err(format!("event release_sha256 {}… ≠ installed plugin {}…", &want[..12.min(want.len())], &got[..12]));
            }
        } else {
            return Err("event pins release_sha256 but this node has no target.plugin_path: the installed \
                        plugin cannot be verified, so the event is refused (set target.plugin_path)".into());
        }
    }
    Ok(h)
}

/// sha256 (hex) of a signed message's exact payload string: the relay's `event_hash`.
pub fn payload_hash(msg: &Value) -> Option<String> {
    use sha2::{Digest, Sha256};
    msg["payload"].as_str().map(|p| hex::encode(Sha256::digest(p.as_bytes())))
}

/// An (already signature-verified) arm payload applies to THIS accepted event.
pub fn check_arm(arm: &Value, event_id: &str, network: &str, event_hash: &str) -> Result<(), String> {
    if arm["type"] != "arm" { return Err("not an arm message".into()); }
    if arm["event_id"].as_str() != Some(event_id) { return Err("arm is for another event".into()); }
    if arm["network"].as_str() != Some(network) { return Err("arm is for another network".into()); }
    // The arm must name the exact event payload it authorizes (sha256 of the signed payload
    // string, as the relay computes it): an arm can never be replayed onto a different event body
    // published under the same id.
    match arm["event_hash"].as_str() {
        Some(h) if h.eq_ignore_ascii_case(event_hash) => Ok(()),
        Some(_) => Err("arm event_hash does not match the accepted event's payload".into()),
        None => Err("arm carries no event_hash: it is not bound to an event payload".into()),
    }
}

/// True when `doc` (GET /api/coord/<network>) carries a valid signed abort for `event_id`.
pub fn aborted(doc: &Value, keys: &[String], network: &str, event_id: &str) -> bool {
    doc.get("abort").filter(|m| !m.is_null())
        .and_then(|m| verify(m, keys).ok())
        .map(|p| p["type"] == "abort" && p["network"].as_str() == Some(network) && p["event_id"].as_str() == Some(event_id))
        .unwrap_or(false)
}

fn state_file(cfg: &Config) -> PathBuf {
    cfg.journal_path.parent().unwrap_or(Path::new(".")).join("coord-state.json")
}

/// Coordination status the beacon reports (written by `await`).
pub fn read_state(cfg: &Config) -> Value {
    std::fs::read_to_string(state_file(cfg)).ok().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or(Value::Null)
}

/// Write the coordination status the beacon reports. Err names the file and the OS error.
pub fn try_write_state(cfg: &Config, v: &Value) -> Result<(), String> {
    let mut v = v.clone();
    if let Some(r) = v.get("reason").and_then(|r| r.as_str()).map(crate::beacon::sanitize_short) {
        v["reason"] = json!(r);
    }
    let path = state_file(cfg);
    std::fs::write(&path, serde_json::to_string_pretty(&v).unwrap_or_default())
        .map_err(|e| format!("cannot write coordination state {}: {e}", path.display()))
}

/// `try_write_state`, logging a failure on stderr (once per distinct error) instead of dropping
/// it: a journal directory the service cannot write (e.g. created by root) otherwise left mission
/// control without this node's acceptance/abort status and nobody knew why. Never fatal: the
/// ceremony itself does not depend on this file.
fn write_state(cfg: &Config, v: &Value) {
    static LAST: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());
    match try_write_state(cfg, v) {
        Ok(()) => {
            if let Ok(mut l) = LAST.lock() {
                l.clear();
            }
        }
        Err(e) => {
            let mut l = LAST.lock().unwrap_or_else(|p| p.into_inner());
            if *l != e {
                eprintln!("await: ERROR: {e}; mission control will not see this node's coordination status \
                           (fix the directory's ownership/permissions)");
                *l = e;
            }
        }
    }
}

/// Derived ceremony config for one event: H, event_id and the event's bindings (roster, quorum,
/// expected snapshot hash) from the signed event. H derivation is switched OFF: H is the event's.
pub fn derived_config(src: &Path, ev: &Value, out: &Path) -> Result<(), String> {
    let h = ev["h"].as_u64().ok_or("event has no H")?;
    let event_id = ev["event_id"].as_str().ok_or("event has no event_id")?;
    let text = std::fs::read_to_string(src).map_err(|e| e.to_string())?;
    let mut doc: toml::Value = toml::from_str(&text).map_err(|e| e.to_string())?;
    let cer = doc["ceremony"].as_table_mut().ok_or("config has no [ceremony]")?;
    cer.insert("freeze_height".into(), toml::Value::Integer(h as i64));
    cer.remove("derive_h_at_arm");
    let co = doc["coordination"].as_table_mut().ok_or("config has no [coordination]")?;
    co.insert("event_id".into(), toml::Value::String(event_id.into()));
    if let Some(roster) = ev.get("roster").filter(|r| r.is_array()) {
        let t: toml::Value = toml::Value::try_from(roster).map_err(|e| format!("roster: {e}"))?;
        co.insert("roster".into(), t);
        // The event decides the quorum; a local fleet_quorum is never inherited. No quorum in the
        // event means every roster member must agree.
        let q = ev["quorum"].as_u64().unwrap_or(roster.as_array().map(|a| a.len()).unwrap_or(0) as u64);
        co.insert("fleet_quorum".into(), toml::Value::Integer(q as i64));
    }
    if let Some(sha) = ev["snapshot_sha256"].as_str() {
        if let Some(snap) = doc.get_mut("snapshot").and_then(|v| v.as_table_mut()) {
            snap.insert("expected_sha256".into(), toml::Value::String(sha.into()));
        }
    }
    std::fs::write(out, toml::to_string(&doc).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}

/// `pulse-cutover await`: wait for a signed event + arm, then run the ceremony with that exact H.
pub fn run_await(cfg: &Config, config_path: &Path) -> Result<i32, String> {
    let co = cfg.coordination.as_ref().ok_or("config has no [coordination] section")?;
    if co.coordinator_keys.is_empty() { return Err("[coordination] coordinator_keys is empty: refusing to trust anyone".into()); }
    let a = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();
    let base = co.url.trim_end_matches('/');
    // (event_id, H, sha256 of the event's signed payload): an event is identified by BOTH id and
    // body; a changed body under the same id is a different event and is validated again.
    let mut accepted: Option<(String, u64, String)> = None;
    let mut accepted_ev: Value = Value::Null;
    // The event id whose signed abort was last recorded in the state file (written once, not every poll).
    let mut aborted_seen: Option<String> = None;
    let mut last_note = String::new();
    let note = |s: &str, last: &mut String| { if s != last { eprintln!("await: {s}"); *last = s.to_string(); } };
    loop {
        let doc: Value = match a.get(&format!("{base}/api/coord/{}", co.network)).call() {
            Ok(r) => r.into_json().unwrap_or(Value::Null),
            Err(e) => { note(&format!("mission control unreachable ({e}); retrying"), &mut last_note); std::thread::sleep(Duration::from_secs(3)); continue; }
        };
        let head = a.post(&format!("{}/v1/chain/get_info", cfg.source.rpc_url.trim_end_matches('/'))).send_json(json!({}))
            .ok().and_then(|r| r.into_json::<Value>().ok()).and_then(|v| v["head_block_num"].as_u64());
        // 1. event
        match doc.get("event").filter(|m| !m.is_null()).map(|m| verify(m, &co.coordinator_keys)) {
            Some(Ok(ev)) => {
                let id = ev["event_id"].as_str().unwrap_or("").to_string();
                let hash = doc["event"].get("payload").and_then(|_| payload_hash(&doc["event"])).unwrap_or_default();
                if aborted(&doc, &co.coordinator_keys, &co.network, &id) {
                    // A signed abort is final for its event id, whatever the event body (a new
                    // attempt is a new event id): never (re-)accept it. Without this the loop
                    // dropped the event, re-accepted it on the next poll, dropped it again
                    // (accepted/aborted flipping every 3 s), and an `await` started after the
                    // abort accepted the aborted event.
                    if aborted_seen.as_deref() != Some(id.as_str()) {
                        let was_accepted = accepted.as_ref().is_some_and(|(x, _, _)| x == &id);
                        write_state(cfg, &json!({"event_id": id, "accepted": was_accepted, "armed": false, "aborted": true,
                            "at": chrono::Utc::now().to_rfc3339()}));
                        aborted_seen = Some(id.clone());
                    }
                    accepted = None;
                    note(&format!("event {id} is aborted by the coordinator (signed); waiting for a new event"), &mut last_note);
                } else if accepted.as_ref().map(|(x, _, hx)| x != &id || hx != &hash).unwrap_or(true) {
                    accepted = None;
                    match validate_event(&ev, cfg, &co.network, head, co.min_lead_blocks) {
                        Ok(h) => {
                            accepted = Some((id.clone(), h, hash.clone()));
                            accepted_ev = ev.clone();
                            write_state(cfg, &json!({"event_id": id, "h": h, "accepted": true, "armed": false, "at": chrono::Utc::now().to_rfc3339()}));
                            note(&format!("accepted event {id}: cut at H = {h}"), &mut last_note);
                        }
                        Err(e) => {
                            write_state(cfg, &json!({"event_id": id, "accepted": false, "reason": e}));
                            note(&format!("REJECTED event {id}: {e}"), &mut last_note);
                        }
                    }
                }
            }
            Some(Err(e)) => note(&format!("ignoring event: {e}"), &mut last_note),
            None => note("no event published; waiting", &mut last_note),
        }
        // 2. arm (an aborted event never reaches here: step 1 dropped it)
        if let (Some((id, h, ev_hash)), Some(Ok(arm))) = (&accepted, doc.get("arm").filter(|m| !m.is_null()).map(|m| verify(m, &co.coordinator_keys))) {
            let bound = check_arm(&arm, id, &co.network, ev_hash);
            if let Err(e) = &bound {
                if arm["event_id"].as_str() == Some(id.as_str()) {
                    note(&format!("ignoring arm for {id}: {e}"), &mut last_note);
                }
            }
            if bound.is_ok() {
                let fresh = arm["issued_at_ms"].as_u64().map(|t| (chrono::Utc::now().timestamp_millis() as u64).saturating_sub(t) < 15 * 60_000).unwrap_or(false);
                let confirm = cfg.journal_path.parent().unwrap_or(Path::new(".")).join(format!("confirm-{id}"));
                if !fresh {
                    note(&format!("arm for {id} is older than 15 min; ignoring"), &mut last_note);
                } else if !co.auto_arm && !confirm.exists() {
                    note(&format!("ARM received for {id} (H = {h}). auto_arm is off: to start, the operator runs `touch {}`", confirm.display()), &mut last_note);
                } else if head.map(|x| *h < x + co.min_lead_blocks / 2).unwrap_or(true) {
                    note(&format!("ARM received but H {h} is too close to head {head:?}; not starting"), &mut last_note);
                } else {
                    let derived = cfg.journal_path.parent().unwrap_or(Path::new(".")).join(format!("ceremony-{id}.toml"));
                    derived_config(config_path, &accepted_ev, &derived)?;
                    write_state(cfg, &json!({"event_id": id, "h": h, "accepted": true, "armed": true, "at": chrono::Utc::now().to_rfc3339()}));
                    eprintln!("await: ARMED by signed coordinator message for {id}: running the ceremony at H = {h}");
                    let status = std::process::Command::new(std::env::current_exe().map_err(|e| e.to_string())?)
                        .args(["run", "--config"]).arg(&derived).status().map_err(|e| e.to_string())?;
                    return Ok(status.code().unwrap_or(1));
                }
            }
        }
        std::thread::sleep(Duration::from_secs(3));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// The agent and mission control must compute the same `event_hash` for the same signed
    /// payload. The vector is shared with control/test (control/test/fixtures/event-hash-vector.json).
    #[test]
    fn event_hash_matches_the_shared_control_vector() {
        let v: Value = serde_json::from_str(include_str!("../control/test/fixtures/event-hash-vector.json")).unwrap();
        let msg = serde_json::json!({ "payload": v["payload"].as_str().unwrap() });
        assert_eq!(payload_hash(&msg).as_deref(), v["event_hash"].as_str());
    }

    fn signed(sk: &SigningKey, payload: &Value) -> Value {
        let p = payload.to_string();
        json!({"payload": p, "sig": hex::encode(sk.sign(p.as_bytes()).to_bytes()), "key": hex::encode(sk.verifying_key().to_bytes())})
    }

    #[test]
    fn verifies_only_configured_keys_and_untampered_payloads() {
        let sk = SigningKey::from_bytes(&[7u8; 32]);
        let other = SigningKey::from_bytes(&[9u8; 32]);
        let keys = vec![hex::encode(sk.verifying_key().to_bytes())];
        let m = signed(&sk, &json!({"type": "event", "network": "rehearsal", "event_id": "e1", "h": 100}));
        assert_eq!(verify(&m, &keys).unwrap()["h"], 100);
        // wrong key
        assert!(verify(&signed(&other, &json!({"type": "event"})), &keys).is_err());
        // tampered payload
        let mut t = m.clone();
        t["payload"] = json!(m["payload"].as_str().unwrap().replace("100", "999"));
        assert!(verify(&t, &keys).unwrap_err().contains("does not verify"));
    }

    #[test]
    fn derived_config_binds_h_roster_quorum_and_snapshot_and_drops_derivation() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("c.toml");
        std::fs::write(&src, "journal_path = \"/j\"\n[ceremony]\nfreeze_height = 0\nfreeze_margin = 10\nderive_h_at_arm = true\n[snapshot]\nstaged_path = \"/s\"\n[coordination]\nurl = \"https://mc\"\nnetwork = \"testnet\"\n").unwrap();
        let ev = json!({"type": "event", "event_id": "e7", "h": 900,
            "roster": [{"producer": "bp1", "instance_id": "aa"}, {"producer": "bp2"}], "quorum": 2,
            "snapshot_sha256": "ff00"});
        let out = dir.path().join("d.toml");
        derived_config(&src, &ev, &out).unwrap();
        let d: toml::Value = toml::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
        assert_eq!(d["ceremony"]["freeze_height"].as_integer(), Some(900));
        assert!(d["ceremony"].get("derive_h_at_arm").is_none(), "H must come from the event, not be derived");
        assert_eq!(d["coordination"]["event_id"].as_str(), Some("e7"));
        assert_eq!(d["coordination"]["fleet_quorum"].as_integer(), Some(2));
        assert_eq!(d["coordination"]["roster"].as_array().unwrap().len(), 2);
        assert_eq!(d["snapshot"]["expected_sha256"].as_str(), Some("ff00"));
    }

    #[test]
    fn arm_must_bind_the_exact_event_payload() {
        // Review #4: a correctly signed arm carrying the WRONG event_hash started the ceremony.
        let arm = |h: &str| json!({"type": "arm", "network": "testnet", "event_id": "e1", "event_hash": h});
        assert!(check_arm(&arm("aa"), "e1", "testnet", "aa").is_ok());
        assert!(check_arm(&arm("bb"), "e1", "testnet", "aa").unwrap_err().contains("event_hash"));
        assert!(check_arm(&json!({"type": "arm", "network": "testnet", "event_id": "e1"}), "e1", "testnet", "aa").is_err(),
            "an arm without event_hash is not bound to any payload");
        let sk = SigningKey::from_bytes(&[7u8; 32]);
        let ev = signed(&sk, &json!({"type": "event", "event_id": "e1"}));
        use sha2::{Digest, Sha256};
        assert_eq!(payload_hash(&ev).unwrap(), hex::encode(Sha256::digest(ev["payload"].as_str().unwrap().as_bytes())));
    }

    #[test]
    fn state_write_failure_is_an_error_not_silence() {
        // Stage-2 rig: a root-owned journal dir made every write fail silently.
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("c.toml");
        std::fs::write(&cfg_path, format!(
            "journal_path = \"{}/missing-dir/journal.jsonl\"\n[ceremony]\nprofile = \"readiness\"\n[source]\nrpc_url = \"http://m\"\nproducer_api_url = \"http://m\"\n\
             [snapshot]\nstaged_path = \"/s\"\n[target]\nmetalgo_unit = \"m\"\nrpc_url = \"http://m\"\n", dir.path().display())).unwrap();
        let cfg = Config::load(&cfg_path).unwrap();
        let err = try_write_state(&cfg, &json!({"event_id": "e1"})).unwrap_err();
        assert!(err.contains("cannot write coordination state") && err.contains("coord-state.json"), "{err}");
        write_state(&cfg, &json!({"event_id": "e1"})); // logs, never panics
        std::fs::create_dir_all(dir.path().join("missing-dir")).unwrap();
        try_write_state(&cfg, &json!({"event_id": "e1"})).unwrap();
        assert_eq!(read_state(&cfg)["event_id"], "e1");
    }

    #[test]
    fn abort_must_match_network_and_event() {
        let sk = SigningKey::from_bytes(&[7u8; 32]);
        let keys = vec![hex::encode(sk.verifying_key().to_bytes())];
        let doc = json!({"abort": signed(&sk, &json!({"type": "abort", "network": "rehearsal", "event_id": "e1"}))});
        assert!(aborted(&doc, &keys, "rehearsal", "e1"));
        assert!(!aborted(&doc, &keys, "rehearsal", "e2"));
        assert!(!aborted(&doc, &keys, "mainnet", "e1"));
        assert!(!aborted(&json!({"abort": null}), &keys, "rehearsal", "e1"));
    }
}
