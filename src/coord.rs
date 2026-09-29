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
    Ok(h)
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

fn write_state(cfg: &Config, v: &Value) {
    let _ = std::fs::write(state_file(cfg), serde_json::to_string_pretty(v).unwrap_or_default());
}

/// Derived ceremony config for one event: H from the signed event, event_id recorded.
fn derived_config(src: &Path, h: u64, event_id: &str, out: &Path) -> Result<(), String> {
    let text = std::fs::read_to_string(src).map_err(|e| e.to_string())?;
    let mut doc: toml::Value = toml::from_str(&text).map_err(|e| e.to_string())?;
    doc["ceremony"].as_table_mut().ok_or("config has no [ceremony]")?.insert("freeze_height".into(), toml::Value::Integer(h as i64));
    doc["coordination"].as_table_mut().ok_or("config has no [coordination]")?.insert("event_id".into(), toml::Value::String(event_id.into()));
    std::fs::write(out, toml::to_string(&doc).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}

/// `pulse-cutover await`: wait for a signed event + arm, then run the ceremony with that exact H.
pub fn run_await(cfg: &Config, config_path: &Path) -> Result<i32, String> {
    let co = cfg.coordination.as_ref().ok_or("config has no [coordination] section")?;
    if co.coordinator_keys.is_empty() { return Err("[coordination] coordinator_keys is empty: refusing to trust anyone".into()); }
    let a = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();
    let base = co.url.trim_end_matches('/');
    let mut accepted: Option<(String, u64)> = None;
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
                if accepted.as_ref().map(|(x, _)| x != &id).unwrap_or(true) {
                    match validate_event(&ev, cfg, &co.network, head, co.min_lead_blocks) {
                        Ok(h) => {
                            accepted = Some((id.clone(), h));
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
        // 2. abort before arming
        if let Some((id, _)) = &accepted {
            if aborted(&doc, &co.coordinator_keys, &co.network, id) {
                write_state(cfg, &json!({"event_id": id, "accepted": true, "armed": false, "aborted": true}));
                note(&format!("event {id} aborted by the coordinator (signed) before arming"), &mut last_note);
                accepted = None;
            }
        }
        // 3. arm
        if let (Some((id, h)), Some(Ok(arm))) = (&accepted, doc.get("arm").filter(|m| !m.is_null()).map(|m| verify(m, &co.coordinator_keys))) {
            if arm["type"] == "arm" && arm["event_id"].as_str() == Some(id.as_str()) && arm["network"].as_str() == Some(co.network.as_str()) {
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
                    derived_config(config_path, *h, id, &derived)?;
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
