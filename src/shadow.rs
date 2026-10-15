//! SHADOW cutovers (docs/DESIGN-shadow.md): the full ceremony on a PRIVATE network from real mainnet
//! state, mainnet chain_id kept. The real-cut refusals stay as they are; this module holds the separate,
//! explicit path: its config section, the load-time rules, the runtime identity checks and the labels.
//!
//! The identity proof (`identity_problems`) has three independent parts, every one fail-closed:
//!  1. the TARGET metalgo, found only through `target.rpc_url` (the endpoint the ceremony creates and ignites on),
//!     reports the configured private Metal network id;
//!  2. the SOURCE's active producer schedule signs only with the declared shadow keys;
//!  3. PUBLIC XPR mainnet (a pinned list of public API endpoints, never configurable) contradicts the source:
//!     none of the declared keys is in mainnet's producer schedule, and mainnet's block at the source's LIB is
//!     not the source's block (or mainnet has not reached that height). A real mainnet node can pass 2 only by
//!     declaring the real keys, which 3 refuses; it can never pass the block-id half of 3.
//!
//! It runs at ARM, before the freeze, right before chain creation / ignition (the result gates the mainnet
//! bypass), and after ignition (target network only). The journal binds the shadow identity at ARM
//! (`identity`), so a config flipped to or from shadow later is refused on run, join, rollback and unhalt.

use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

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
}

/// Public XPR mainnet API endpoints the identity proof asks (pinned on purpose: an operator-chosen endpoint could
/// be a stub that says whatever makes a real node look like a shadow). A shadow run needs outbound HTTPS to them;
/// if none answers as XPR mainnet the proof fails closed.
pub const PUBLIC_XPR_MAINNET_APIS: &[&str] = &[
    "https://proton.greymass.com",
    "https://proton.eosusa.io",
    "https://proton.cryptolions.io",
    "https://proton.eoscafeblock.com",
    "https://api.protonnz.com",
];

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
    if cfg
        .ceremony
        .chain_id
        .as_deref()
        .is_none_or(|c| c.trim().is_empty())
    {
        problems.push(
            "ceremony.chain_id must be set in a shadow ceremony (the chain it imitates)".into(),
        );
    }
    match cfg.target.metal_network_id {
        None => problems.push("target.metal_network_id must be set: a shadow runs on a PRIVATE Metal network".into()),
        Some(n) if crate::config::is_public_metal_network(n) => problems.push(format!(
            "target.metal_network_id = {n} is a public Metal network: a shadow runs on a PRIVATE one only"
        )),
        Some(_) => {}
    }
    // Round-2 review: the WHOLE API base must match (scheme, host, port AND path): a path-routing proxy could otherwise
    // serve the identity reads for `/shadow` and forward producer mutations for `/mainnet` on the same origin.
    match (api_base(&cfg.source.rpc_url), api_base(&cfg.source.producer_api_url)) {
        (Some(a), Some(b)) if a == b => {}
        _ => problems.push(format!(
            "source.rpc_url {} and source.producer_api_url {} must be the same API base (same scheme, host, port and \
             path, no query) in a shadow: the identity is proven on the node whose producer the ceremony pauses",
            cfg.source.rpc_url, cfg.source.producer_api_url
        )),
    }
    if metal_info_base(cfg).is_none() {
        problems.push(format!(
            "target.rpc_url {} is not a metalgo /ext/ URL: a shadow proves its private network on the metalgo that \
             serves the target, so the target RPC must be that metalgo's /ext/bc/... endpoint",
            cfg.target.rpc_url
        ));
    }
    if let Some(b) = &cfg.beacon {
        if !is_shadow_network_name(&b.network) {
            problems.push(format!(
                "beacon.network is {:?}: a shadow reports only to a board whose name starts with \"shadow\" (never a \
                 mainnet or testnet board)",
                b.network
            ));
        }
    }
    if let Some(c) = &cfg.coordination {
        if !is_shadow_network_name(&c.network) {
            problems.push(format!(
                "coordination.network is {:?}: a shadow takes part only in events on a network whose name starts with \
                 \"shadow\" (never a mainnet event)",
                c.network
            ));
        }
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
        Err(format!(
            "shadow ceremony config refused:\n  - {}",
            problems.join("\n  - ")
        ))
    }
}

/// Parse a URL strictly for the shadow checks: refuses backslashes and userinfo (ambiguous between parsers: a hand
/// split and an HTTP client can disagree about the host), and anything without a host.
fn strict_url(url: &str) -> Option<url::Url> {
    if url.contains('\\') || url.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return None;
    }
    let u = url::Url::parse(url.trim()).ok()?;
    if !u.username().is_empty() || u.password().is_some() || u.host().is_none() {
        return None;
    }
    Some(u)
}

/// (scheme, host, port) of a URL, parsed strictly (None when ambiguous or unparsable).
pub fn origin(url: &str) -> Option<(String, String, u16)> {
    let u = strict_url(url)?;
    Some((
        u.scheme().to_string(),
        u.host_str()?.to_ascii_lowercase(),
        u.port_or_known_default()?,
    ))
}

/// The complete API base the HTTP client calls (scheme, host, port, path without trailing slashes), parsed strictly;
/// None when ambiguous, unparsable, or carrying a query or fragment (the client appends paths to the raw string).
pub fn api_base(url: &str) -> Option<(String, String, u16, String)> {
    let u = strict_url(url)?;
    if u.query().is_some() || u.fragment().is_some() {
        return None;
    }
    let path = u.path().trim_end_matches('/').to_string();
    Some((
        u.scheme().to_string(),
        u.host_str()?.to_ascii_lowercase(),
        u.port_or_known_default()?,
        path,
    ))
}

/// Loopback, RFC 1918, link-local, IPv6 loopback / unique-local / link-local, or a reserved private name suffix.
/// Parsed with the URL parser the HTTP client uses; ambiguous syntax (backslashes, userinfo) counts as public.
pub fn is_private_url(url: &str) -> bool {
    let Some(u) = strict_url(url) else {
        return false;
    };
    match u.host() {
        Some(url::Host::Ipv4(v4)) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        Some(url::Host::Ipv6(v6)) => {
            let seg0 = v6.segments()[0];
            v6.is_loopback() || (seg0 & 0xfe00) == 0xfc00 || (seg0 & 0xffc0) == 0xfe80
        }
        Some(url::Host::Domain(d)) => {
            let host = d.trim_end_matches('.').to_ascii_lowercase();
            host == "localhost"
                || [
                    ".localhost",
                    ".local",
                    ".internal",
                    ".test",
                    ".lan",
                    ".home.arpa",
                ]
                .iter()
                .any(|sfx| host.ends_with(sfx))
        }
        None => false,
    }
}

/// A board or coordination network a shadow may use: its name starts with "shadow" (case-insensitive).
pub fn is_shadow_network_name(name: &str) -> bool {
    name.trim().to_ascii_lowercase().starts_with("shadow")
}

/// The target metalgo's base URL, derived ONLY from `target.rpc_url` (the endpoint the ceremony ignites on and reads
/// the target through): everything before `/ext/`. None when the target RPC is not a metalgo /ext/ URL.
pub fn metal_info_base(cfg: &Config) -> Option<String> {
    let u = &cfg.target.rpc_url;
    u.find("/ext/")
        .map(|i| u[..i].to_string())
        .filter(|b| b.contains("://"))
}

/// The shadow identity journaled on the ARMED record (Null outside a shadow): label, private network id, and a hash
/// of the declared key set. A later run, join, rollback or unhalt must present the same identity.
pub fn identity(cfg: &Config) -> Value {
    match cfg.shadow.as_ref().filter(|_| cfg.ceremony.shadow) {
        Some(sh) => {
            let mut keys: Vec<&str> = sh
                .producer_keys
                .iter()
                .map(|k| k.trim())
                .filter(|k| !k.is_empty())
                .collect();
            keys.sort_unstable();
            keys.dedup();
            json!({"label": sh.label, "metal_network_id": cfg.target.metal_network_id,
                   "producer_keys_sha256": hex::encode(Sha256::digest(keys.join("\n").as_bytes()))})
        }
        None => Value::Null,
    }
}

/// `armed` = the `shadow` value of this journal's ARMED record (None: the journal has no ARMED record yet). Some(why)
/// when the current config is not the ceremony that was armed: a real journal can never become a shadow, a shadow
/// never a real one, and a shadow's label, network or keys never change mid-ceremony.
pub fn journal_mismatch(cfg: &Config, armed: Option<&Value>) -> Option<String> {
    let armed = armed?;
    let now = identity(cfg);
    if *armed == now {
        return None;
    }
    Some(match (armed.is_null(), now.is_null()) {
        (true, false) => "this journal was armed as a REAL ceremony and the config is now a SHADOW: refused (a shadow is \
                          armed as one from the start; a real ceremony's journal can never take the shadow path)"
            .into(),
        (false, true) => format!("this journal was armed as SHADOW {armed} and the config is no longer a shadow: refused"),
        _ => format!("this journal was armed as SHADOW {armed} and the config now says {now}: refused (a shadow's label, \
                      network and keys are fixed at ARM)"),
    })
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

/// The target half alone (after ignition): the metalgo behind `target.rpc_url` still reports the private network.
pub fn target_network_problems<O: ChainOps + ?Sized>(cfg: &Config, ops: &O) -> Vec<String> {
    let want = cfg.target.metal_network_id;
    let Some(base) = metal_info_base(cfg) else {
        return vec![format!(
            "target.rpc_url {} is not a metalgo /ext/ URL (shadow identity unverified)",
            cfg.target.rpc_url
        )];
    };
    match ops.metal_network_id(&base) {
        Ok(Some(n)) if Some(n) == want && !crate::config::is_public_metal_network(n) => vec![],
        Ok(Some(n)) => vec![format!(
            "the target metalgo at {base} reports network id {n}, not the configured private shadow network {}",
            want.map_or("(none)".to_string(), |w| w.to_string())
        )],
        Ok(None) => vec![format!("the target metalgo at {base} did not report a network id (shadow identity unverified)")],
        Err(e) => vec![format!("cannot read the target network id at {base}: {e} (shadow identity unverified)")],
    }
}

/// One day of 0.5 s blocks (see `public_mainnet_problems`).
pub const FAR_AHEAD_BLOCKS: u64 = 172_800;

/// Round-2 review (N5): the chain the ceremony is about to ignite must exist on the PROVEN private metalgo (the one
/// behind `target.rpc_url` whose network id the identity proof read), not merely on whatever `create_chain_cmd`
/// talked to. `blockchain_id` = the journaled id (upstream backend); otherwise the id in `target.rpc_url`
/// (`/ext/bc/<id>/...`). Fail-closed.
pub fn target_chain_problems<O: ChainOps + ?Sized>(
    cfg: &Config,
    ops: &O,
    blockchain_id: Option<&str>,
) -> Vec<String> {
    let Some(base) = metal_info_base(cfg) else {
        return vec![format!(
            "target.rpc_url {} is not a metalgo /ext/ URL (target chain unverified)",
            cfg.target.rpc_url
        )];
    };
    let from_url = cfg
        .target
        .rpc_url
        .split("/ext/bc/")
        .nth(1)
        .and_then(|r| r.split('/').next())
        .filter(|id| !id.is_empty() && !id.contains('{'))
        .map(str::to_string);
    let Some(id) = blockchain_id.map(str::to_string).or(from_url) else {
        return vec!["no blockchain id is known for the target (neither journaled nor in target.rpc_url): target chain unverified".into()];
    };
    match ops.metal_blockchain_status(&base, &id) {
        Ok(Some(st)) if !st.is_empty() && !st.eq_ignore_ascii_case("unknown") => vec![],
        Ok(Some(st)) => vec![format!("the proven private metalgo at {base} reports chain {id} as {st:?}: the chain about to be \
                                      ignited is not on the shadow network")],
        Ok(None) => vec![format!("the proven private metalgo at {base} did not report a status for chain {id} (target chain unverified)")],
        Err(e) => vec![format!("cannot ask the proven private metalgo at {base} about chain {id}: {e} (target chain unverified)")],
    }
}

/// The full identity proof (module doc). Fail-closed: an unreadable answer is a problem. Empty = this really is a
/// private shadow, independently of anything the operator declared.
pub fn identity_problems<O: ChainOps + ?Sized>(cfg: &Config, ops: &O) -> Vec<String> {
    let Some(sh) = cfg.shadow.as_ref().filter(|_| cfg.ceremony.shadow) else {
        return vec![];
    };
    let mut out = target_network_problems(cfg, ops);
    let allowed: Vec<String> = sh
        .producer_keys
        .iter()
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty())
        .collect();
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
    out.extend(public_mainnet_problems(&allowed, ops));
    out.extend(producer_binding_problems(ops));
    out
}

/// Part 4 of the proof (round-4 review): the PRODUCER API is the shadow node. Parts 1-3 read the chain API, but pause
/// and resume go to `/v1/producer/*`; a path-routing proxy could serve one from the shadow and the other from a real
/// producer behind an identical base URL. So the producer API's own head block id (`get_integrity_hash`) must (a) be
/// the block the chain API serves at that height (one node behind both) and (b) NOT be public XPR mainnet's block at
/// that height, judged like part 3 (every answering pinned endpoint; "far ahead" only on the highest head). Fail-closed:
/// any unreadable answer is a problem. Checked with the rest of the proof, and again right before every pause/resume.
pub fn producer_binding_problems<O: ChainOps + ?Sized>(ops: &O) -> Vec<String> {
    let head_id = match ops.source_producer_head() {
        Ok(Some(id)) if id.len() >= 8 => id,
        Ok(Some(id)) => return vec![format!("the producer API's head block id {id:?} is malformed (producer node unverified)")],
        Ok(None) => return vec!["the producer API did not report its head block id (get_integrity_hash): the node that \
                                 would be paused is unverified".into()],
        Err(e) => return vec![format!("cannot read the producer API's head block (get_integrity_hash): {e} (producer node \
                                       unverified)")],
    };
    let Ok(num) = u64::from_str_radix(&head_id[..8], 16) else {
        return vec![format!("the producer API's head block id {head_id} has no block number (producer node unverified)")];
    };
    let mut out = vec![];
    // (a) one node behind both APIs.
    match ops.source_block_id(num) {
        Ok((id, _)) if id.eq_ignore_ascii_case(&head_id) => {}
        Ok((id, _)) => out.push(format!(
            "the producer API's head block {num} is {head_id} but the chain API serves {id} at that height: the two \
             source URLs reach DIFFERENT nodes. Refused (pause/resume would not touch the node the proof checked)"
        )),
        Err(e) => out.push(format!("the chain API cannot serve the producer API's head block {num} ({e}): the two source \
                                    URLs may reach different nodes (producer node unverified)")),
    }
    // (b) not public mainnet's block.
    let (mut compared, mut max_head, mut unreachable) = (0usize, None::<u64>, vec![]);
    for base in PUBLIC_XPR_MAINNET_APIS {
        let info = match ops.public_chain_post(base, "/v1/chain/get_info", json!({})) {
            Ok(v)
                if v["chain_id"]
                    .as_str()
                    .is_some_and(crate::config::is_xpr_mainnet) =>
            {
                v
            }
            Ok(v) => {
                unreachable.push(format!(
                    "{base}: not XPR mainnet (chain_id {})",
                    v["chain_id"]
                ));
                continue;
            }
            Err(e) => {
                unreachable.push(format!("{base}: {e}"));
                continue;
            }
        };
        let Some(mainnet_head) = info["head_block_num"].as_u64() else {
            unreachable.push(format!("{base}: get_info without head_block_num"));
            continue;
        };
        max_head = max_head.max(Some(mainnet_head));
        if mainnet_head < num {
            unreachable.push(format!(
                "{base}: mainnet head {mainnet_head} is behind the producer's head {num}"
            ));
            continue;
        }
        match ops.public_chain_post(base, "/v1/chain/get_block", json!({"block_num_or_id": num})) {
            Ok(b) => match b["id"].as_str() {
                Some(id) => {
                    compared += 1;
                    if id.eq_ignore_ascii_case(&head_id) {
                        out.push(format!(
                            "the PRODUCER API serves XPR MAINNET's own block {id} at {num} (per {base}): the node pause/resume \
                             would reach is a real mainnet producer. Refused"
                        ));
                    }
                }
                None => unreachable.push(format!("{base}: get_block {num}: no id")),
            },
            Err(e) => unreachable.push(format!("{base}: get_block {num}: {e}")),
        }
    }
    if compared == 0 {
        match max_head {
            Some(h) if h < num && num - h > FAR_AHEAD_BLOCKS => {}
            _ => out.push(format!(
                "the producer API's head block {num} could not be compared with public XPR mainnet ({}): producer node \
                 unverified",
                if unreachable.is_empty() { "no answer".to_string() } else { unreachable.join("; ") }
            )),
        }
    }
    out
}

/// Part 3 of the proof: ask PUBLIC XPR mainnet. Every pinned endpoint that answers as XPR mainnet is consulted, and
/// any one that recognises the declared keys or the source's block refuses the shadow; at least one must answer
/// each question, else the proof fails closed.
pub fn public_mainnet_problems<O: ChainOps + ?Sized>(
    declared_keys: &[String],
    ops: &O,
) -> Vec<String> {
    let mut out = vec![];
    let (src_lib, src_id) = match ops.source_info() {
        Ok(info) => match ops.source_block_id(info.last_irreversible_block_num) {
            Ok((id, _)) => (info.last_irreversible_block_num, id),
            Err(e) => {
                return vec![format!(
                    "cannot read the source's block at its LIB {}: {e} (shadow source unverified)",
                    info.last_irreversible_block_num
                )]
            }
        },
        Err(e) => {
            return vec![format!(
                "cannot read the source chain: {e} (shadow source unverified)"
            )]
        }
    };
    let (mut schedules_read, mut blocks_compared, mut unreachable) = (0usize, 0usize, vec![]);
    let (mut max_head, mut behind): (Option<u64>, Vec<String>) = (None, vec![]);
    for base in PUBLIC_XPR_MAINNET_APIS {
        let info = match ops.public_chain_post(base, "/v1/chain/get_info", json!({})) {
            Ok(v)
                if v["chain_id"]
                    .as_str()
                    .is_some_and(crate::config::is_xpr_mainnet) =>
            {
                v
            }
            Ok(v) => {
                unreachable.push(format!(
                    "{base}: not XPR mainnet (chain_id {})",
                    v["chain_id"]
                ));
                continue;
            }
            Err(e) => {
                unreachable.push(format!("{base}: {e}"));
                continue;
            }
        };
        match ops.public_chain_post(base, "/v1/chain/get_producer_schedule", json!({})) {
            Ok(sched) => {
                let mut mainnet_keys = vec![];
                for part in ["active", "pending", "proposed"] {
                    if let Some(k) = sched
                        .get(part)
                        .filter(|v| !v.is_null())
                        .and_then(|v| schedule_keys(&json!({"active": v})))
                    {
                        mainnet_keys.extend(k);
                    }
                }
                if mainnet_keys.is_empty() {
                    unreachable.push(format!("{base}: producer schedule unreadable"));
                } else {
                    schedules_read += 1;
                    let real: Vec<String> = declared_keys
                        .iter()
                        .filter(|k| mainnet_keys.iter().any(|(_, m)| m == *k))
                        .cloned()
                        .collect();
                    if !real.is_empty() {
                        out.push(format!(
                            "declared shadow keys are REAL XPR mainnet producer keys (per {base}): {}. A shadow source is \
                             re-keyed; refused",
                            real.join(", ")
                        ));
                    }
                }
            }
            Err(e) => unreachable.push(format!("{base}: get_producer_schedule: {e}")),
        }
        // Round-2 review (N2): a missing head is unreadable, never a comparison. Round-3 review (F2): an endpoint BEHIND
        // the source is never judged on its own: the "far ahead" rule below applies to the HIGHEST head any answering
        // mainnet endpoint reports, so one stuck endpoint cannot vouch for a source that a healthy one would expose.
        let Some(mainnet_head) = info["head_block_num"].as_u64() else {
            unreachable.push(format!(
                "{base}: get_info without head_block_num (block not compared)"
            ));
            continue;
        };
        max_head = max_head.max(Some(mainnet_head));
        if mainnet_head < src_lib {
            behind.push(format!(
                "{base}: mainnet head {mainnet_head} is behind the source's LIB {src_lib}"
            ));
            continue;
        }
        match ops.public_chain_post(
            base,
            "/v1/chain/get_block",
            json!({"block_num_or_id": src_lib}),
        ) {
            Ok(b) => match b["id"].as_str() {
                Some(id) => {
                    blocks_compared += 1;
                    if id.eq_ignore_ascii_case(&src_id) {
                        out.push(format!(
                            "the source's block at its LIB {src_lib} is XPR MAINNET's own block {id} (per {base}): this \
                             is a real mainnet node, not a shadow source. Refused"
                        ));
                    }
                }
                None => unreachable.push(format!("{base}: get_block {src_lib}: no id")),
            },
            Err(e) => unreachable.push(format!("{base}: get_block {src_lib}: {e}")),
        }
    }
    // Only when EVERY answering endpoint (so the highest head among them) is more than FAR_AHEAD_BLOCKS (one day of 0.5 s
    // blocks) behind the source's LIB does the gap itself count as "not mainnet": no healthy public endpoint lags by a
    // day, and a restored snapshot never runs ahead of the network it came from by accident. Anything closer is
    // unknown: a real get_block comparison is required.
    if blocks_compared == 0 {
        match max_head {
            Some(h) if h < src_lib && src_lib - h > FAR_AHEAD_BLOCKS => blocks_compared = 1,
            Some(_) => unreachable.extend(behind.into_iter().map(|b| format!(
                "{b}; the highest public mainnet head is within a day of the source, so only a block comparison \
                 counts (none possible: block not compared)"))),
            None => {}
        }
    }
    if schedules_read == 0 || blocks_compared == 0 {
        out.push(format!(
            "public XPR mainnet could not be consulted ({}): a shadow must prove it is not mainnet against public mainnet, \
             so it needs outbound HTTPS to one of {}. Shadow identity unverified",
            if unreachable.is_empty() { "no answer".to_string() } else { unreachable.join("; ") },
            PUBLIC_XPR_MAINNET_APIS.join(", ")
        ));
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
        for u in [
            "http://127.0.0.1:8080",
            "http://10.1.2.3",
            "https://192.168.1.5/x",
            "http://172.16.0.9:80",
            "http://[::1]:8888",
            "http://[fd00::1]/",
            "http://shadow.internal",
            "http://edge.local:80",
            "http://localhost",
            "http://a.b.test",
            "http://169.254.1.1",
        ] {
            assert!(is_private_url(u), "{u}");
        }
        for u in [
            "https://api.example.com",
            "http://8.8.8.8",
            "http://172.32.0.1",
            "https://[2001:db8::1]/",
            "http://user@1.2.3.4",
            "",
            "http://",
            "https://localhost.example.com",
            "http://public.example\\@127.0.0.1:8888/",
            "http://public.example@127.0.0.1:8888/",
            "http://user:pw@10.0.0.1/",
            "http://10.0.0.1\\.evil.example/",
        ] {
            assert!(!is_private_url(u), "{u}");
        }
    }

    #[test]
    fn schedule_keys_both_forms_and_unknown() {
        let v = json!({"active": {"producers": [
            {"producer_name": "a", "authority": [0, {"threshold": 1, "keys": [{"key": "PUB_K1_a", "weight": 1}]}]},
            {"producer_name": "b", "block_signing_key": "PUB_K1_b"}]}});
        let k = schedule_keys(&v).unwrap();
        assert_eq!(
            k,
            vec![
                ("a".into(), "PUB_K1_a".into()),
                ("b".into(), "PUB_K1_b".into())
            ]
        );
        assert!(
            schedule_keys(&json!({"active": {"producers": [{"producer_name": "x"}]}})).is_none()
        );
        assert!(schedule_keys(&json!({"active": {"producers": []}})).is_none());
        assert!(schedule_keys(&json!({})).is_none());
    }

    #[test]
    fn origins_are_parsed_strictly() {
        assert_eq!(
            origin("http://127.0.0.1:8888/v1"),
            Some(("http".into(), "127.0.0.1".into(), 8888))
        );
        assert_eq!(
            origin("http://LOCALHOST:8888"),
            origin("http://localhost:8888/")
        );
        assert_ne!(
            origin("http://127.0.0.1:8888"),
            origin("http://127.0.0.1:8889")
        );
        assert_eq!(
            origin("https://node.internal"),
            Some(("https".into(), "node.internal".into(), 443))
        );
        assert!(origin("http://a@127.0.0.1:8888").is_none());
        assert!(origin("http://x\\@127.0.0.1:8888").is_none());
        // Round-2 review: the path is part of the identity (a path-routing proxy must not split reads and mutations).
        assert_eq!(api_base("http://gw:8888"), api_base("http://gw:8888/"));
        assert_eq!(
            api_base("http://GW:8888/node/"),
            api_base("http://gw:8888/node")
        );
        assert_ne!(
            api_base("http://gw:8888/shadow"),
            api_base("http://gw:8888/mainnet")
        );
        assert_ne!(
            api_base("http://gw:8888"),
            api_base("http://gw:8888/mainnet")
        );
        assert!(api_base("http://gw:8888/x?route=mainnet").is_none());
        assert!(api_base("http://gw:8888/x#frag").is_none());
    }

    #[test]
    fn shadow_network_names() {
        for n in ["shadow", "shadow-mainnet-1", "Shadow_X"] {
            assert!(is_shadow_network_name(n), "{n}");
        }
        for n in [
            "mainnet",
            "xpr-mainnet",
            "production",
            "testnet",
            "rehearsal",
            " ",
        ] {
            assert!(!is_shadow_network_name(n), "{n}");
        }
    }
}
