//! SHADOW cutovers (docs/DESIGN-shadow.md): the full ceremony on a PRIVATE network from real mainnet
//! state, mainnet chain_id kept. The real-cut refusals stay as they are; this module holds the separate,
//! explicit path: its config section, the load-time rules, the runtime identity checks (target network
//! id, source producer schedule) and the labels.

use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::Config;
use crate::ops::ChainOps;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowCfg {
    /// Shown on every journal record, report and the mission control board.
    pub label: String,
    /// The shadow source's block-signing keys (the re-keyed producer schedule). The source's active
    /// schedule must sign with these only: real mainnet's never does.
    #[serde(default)]
    pub producer_keys: Vec<String>,
    /// Must be true: a shadow chain carries the real chain_id, so anything signed for it is also
    /// valid on mainnet.
    #[serde(default)]
    pub i_understand_signatures_are_valid_on_mainnet: bool,
    /// Base URL of the target metalgo for `info.getNetworkID` (default: target.rpc_url's origin).
    #[serde(default)]
    pub metal_info_url: Option<String>,
}

/// Load-time rules. Errors refuse the config (every loader, including status and the beacon).
pub fn check_config(cfg: &Config) -> Result<(), String> {
    match (cfg.ceremony.shadow, &cfg.shadow) {
        (false, None) => return Ok(()),
        (false, Some(_)) => {
            return Err("[shadow] is set but ceremony.shadow is not true: a shadow section never applies silently; \
                        set ceremony.shadow = true for a shadow cutover, or remove [shadow]"
                .into())
        }
        (true, None) => {
            return Err("ceremony.shadow = true needs a [shadow] section (label, producer_keys, \
                        i_understand_signatures_are_valid_on_mainnet = true): see docs/DESIGN-shadow.md"
                .into())
        }
        (true, Some(_)) => {}
    }
    let sh = cfg.shadow.as_ref().expect("checked above");
    let mut problems = vec![];
    if sh.label.trim().is_empty() {
        problems.push("shadow.label is empty (it labels every record so a shadow is never mistaken for a cut)".to_string());
    }
    if sh.producer_keys.iter().all(|k| k.trim().is_empty()) {
        problems.push("shadow.producer_keys is empty (the shadow source's signing keys; checked against its active schedule)".into());
    }
    if !sh.i_understand_signatures_are_valid_on_mainnet {
        problems.push("shadow.i_understand_signatures_are_valid_on_mainnet must be true: the shadow chain keeps the real \
                       chain_id, so anything signed for it is valid on mainnet (never sign there with a real key)"
            .into());
    }
    if cfg.ceremony.chain_id.as_deref().map_or(true, |c| c.trim().is_empty()) {
        problems.push("ceremony.chain_id must be set in a shadow ceremony (the chain it imitates)".into());
    }
    match cfg.target.metal_network_id {
        None => problems.push("target.metal_network_id must be set: a shadow runs on a PRIVATE Metal network".into()),
        Some(n) if crate::config::is_public_metal_network(n) => problems.push(format!(
            "target.metal_network_id = {n} is a public Metal network: a shadow runs on a PRIVATE one only"
        )),
        Some(_) => {}
    }
    if cfg.beacon.as_ref().is_some_and(|b| b.network.trim().eq_ignore_ascii_case("mainnet")) {
        problems.push("beacon.network is \"mainnet\": shadow evidence must never land on the mainnet board".into());
    }
    if cfg.coordination.as_ref().is_some_and(|c| c.network.trim().eq_ignore_ascii_case("mainnet")) {
        problems.push("coordination.network is \"mainnet\": a shadow must never take part in a mainnet event".into());
    }
    if let Some(f) = &cfg.flip {
        if !is_private_url(&f.public_url) {
            problems.push(format!(
                "flip.public_url {} is not a private address: a shadow chain (mainnet chain_id) must never be served publicly",
                f.public_url
            ));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!("shadow ceremony config refused:\n  - {}", problems.join("\n  - ")))
    }
}

/// Loopback, RFC 1918, link-local, CGNAT-free private ranges, IPv6 loopback / unique-local / link-local,
/// or a reserved private name suffix. Anything else (including a hostname that cannot be classified) is public.
pub fn is_private_url(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = if let Some(h) = authority.strip_prefix('[') {
        h.split(']').next().unwrap_or("")
    } else {
        authority.split(':').next().unwrap_or("")
    };
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() {
        return false;
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return match ip {
            std::net::IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
            std::net::IpAddr::V6(v6) => {
                let seg0 = v6.segments()[0];
                v6.is_loopback() || (seg0 & 0xfe00) == 0xfc00 || (seg0 & 0xffc0) == 0xfe80
            }
        };
    }
    host == "localhost"
        || [".localhost", ".local", ".internal", ".test", ".lan", ".home.arpa"]
            .iter()
            .any(|s| host.ends_with(s))
}

/// Base URL for the target metalgo's info API.
fn metal_info_base(cfg: &Config) -> String {
    if let Some(u) = cfg.shadow.as_ref().and_then(|s| s.metal_info_url.as_deref()) {
        return u.trim_end_matches('/').to_string();
    }
    let u = &cfg.target.rpc_url;
    match u.find("/ext/") {
        Some(i) => u[..i].to_string(),
        None => "http://127.0.0.1:9650".into(),
    }
}

/// Every block-signing key in a `get_producer_schedule` answer's ACTIVE schedule: Leap 2.x+ authority form
/// (`authority: [0, {keys: [{key}]}]`) and the legacy `block_signing_key`.
pub fn schedule_keys(schedule: &Value) -> Option<Vec<(String, String)>> {
    let producers = schedule.get("active")?.get("producers")?.as_array()?;
    let mut out = vec![];
    for p in producers {
        let name = p["producer_name"].as_str().unwrap_or("?").to_string();
        let mut found = false;
        if let Some(k) = p["block_signing_key"].as_str() {
            out.push((name.clone(), k.to_string()));
            found = true;
        }
        if let Some(keys) = p["authority"].get(1).and_then(|a| a["keys"].as_array()) {
            for k in keys {
                if let Some(k) = k["key"].as_str() {
                    out.push((name.clone(), k.to_string()));
                    found = true;
                }
            }
        }
        if !found {
            return None; // a producer whose key cannot be read: unknown, never "fine"
        }
    }
    (!out.is_empty()).then_some(out)
}

/// The runtime identity checks (ARM and right before the freeze). Fail-closed: an unreadable answer is a
/// problem. Empty = this really is the private shadow it claims to be.
pub fn runtime_problems<O: ChainOps + ?Sized>(cfg: &Config, ops: &O) -> Vec<String> {
    let Some(sh) = cfg.shadow.as_ref().filter(|_| cfg.ceremony.shadow) else { return vec![] };
    let mut out = vec![];
    let want = cfg.target.metal_network_id;
    let base = metal_info_base(cfg);
    match ops.metal_network_id(&base) {
        Ok(Some(n)) if Some(n) == want && !crate::config::is_public_metal_network(n) => {}
        Ok(Some(n)) => out.push(format!(
            "the target metalgo at {base} reports network id {n}, not the configured private shadow network {}",
            want.map_or("(none)".to_string(), |w| w.to_string())
        )),
        Ok(None) => out.push(format!("the target metalgo at {base} did not report a network id (shadow identity unverified)")),
        Err(e) => out.push(format!("cannot read the target network id at {base}: {e} (shadow identity unverified)")),
    }
    let allowed: Vec<String> = sh.producer_keys.iter().map(|k| k.trim().to_string()).filter(|k| !k.is_empty()).collect();
    match ops.source_producer_schedule() {
        Ok(Some(v)) => match schedule_keys(&v) {
            Some(keys) => {
                let foreign: Vec<String> = keys
                    .iter()
                    .filter(|(_, k)| !allowed.iter().any(|a| a == k))
                    .map(|(p, k)| format!("{p}={k}"))
                    .collect();
                if !foreign.is_empty() {
                    out.push(format!(
                        "the source's active producer schedule signs with keys that are not shadow keys ({}): this \
                         looks like a real chain, not a re-keyed shadow source",
                        foreign.join(", ")
                    ));
                }
            }
            None => out.push("the source's producer schedule could not be read as a list of signing keys (shadow source unverified)".into()),
        },
        Ok(None) => out.push("the source did not return its producer schedule (shadow source unverified)".into()),
        Err(e) => out.push(format!("cannot read the source's producer schedule: {e} (shadow source unverified)")),
    }
    out
}

/// The label for journal records and the beacon summary (Null outside a shadow).
pub fn label(cfg: &Config) -> Value {
    match cfg.shadow.as_ref().filter(|_| cfg.ceremony.shadow) {
        Some(sh) => json!({"label": sh.label, "metal_network_id": cfg.target.metal_network_id}),
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_urls() {
        for u in ["http://127.0.0.1:8080", "http://10.1.2.3", "https://192.168.1.5/x", "http://172.16.0.9:80",
                  "http://[::1]:8888", "http://[fd00::1]/", "http://shadow.internal", "http://edge.local:80",
                  "http://localhost", "http://a.b.test", "http://169.254.1.1"] {
            assert!(is_private_url(u), "{u}");
        }
        for u in ["https://api.example.com", "http://8.8.8.8", "http://172.32.0.1", "https://[2001:db8::1]/",
                  "http://user@1.2.3.4", "", "http://", "https://localhost.example.com"] {
            assert!(!is_private_url(u), "{u}");
        }
    }

    #[test]
    fn schedule_keys_both_forms_and_unknown() {
        let v = json!({"active": {"producers": [
            {"producer_name": "a", "authority": [0, {"threshold": 1, "keys": [{"key": "PUB_K1_a", "weight": 1}]}]},
            {"producer_name": "b", "block_signing_key": "PUB_K1_b"}]}});
        let k = schedule_keys(&v).unwrap();
        assert_eq!(k, vec![("a".into(), "PUB_K1_a".into()), ("b".into(), "PUB_K1_b".into())]);
        assert!(schedule_keys(&json!({"active": {"producers": [{"producer_name": "x"}]}})).is_none());
        assert!(schedule_keys(&json!({"active": {"producers": []}})).is_none());
        assert!(schedule_keys(&json!({})).is_none());
    }
}
