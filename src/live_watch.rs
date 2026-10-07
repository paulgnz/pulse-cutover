//! Post-LIVE liveness watch (reporting only, never an action).
//!
//! Stage-2 run 2: the new chain reached LIVE, then stopped including transactions two seconds
//! later (every transfer exhausted its CPU budget), while the journal, `status` and mission
//! control all kept saying LIVE. After LIVE the beacon (and `pulse-cutover status`) keep asking
//! the target for its head and report the HEALTH check `target_live`: failing once no new block
//! has appeared for `target.post_live_max_idle_secs`, or when the operator's optional
//! `target.post_live_probe_cmd` fails. PulseVM builds blocks only when there are transactions,
//! so on a chain with traffic (any production network) a still head means admitted
//! transactions are not being included. Nothing is rolled back after LIVE: that is an operator
//! decision; this only makes it visible.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::config::Config;

pub const CHECK: &str = "target_live";

/// How often the beacon runs `post_live_probe_cmd` (the last verdict is reported in between),
/// and how long one run may take.
const PROBE_EVERY: Duration = Duration::from_secs(30);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// What the target said: its head, the head block's RAW timestamp (ms since the epoch, as the
/// node reports it; never an edge's synthesized value) and the chain it serves.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetHead {
    pub head: u64,
    pub head_block_ms: Option<u64>,
    pub chain_id: Option<String>,
}

/// Clock skew tolerated before a head block time "in the future" is ignored.
pub const FUTURE_SKEW_MS: u64 = 5_000;

/// `YYYY-MM-DDTHH:MM:SS[.fff][Z]` (UTC) -> ms since the epoch.
pub fn parse_block_time_ms(s: &str) -> Option<u64> {
    let t = chrono::NaiveDateTime::parse_from_str(s.trim_end_matches('Z'), "%Y-%m-%dT%H:%M:%S%.f").ok()?;
    u64::try_from(t.and_utc().timestamp_millis()).ok()
}

/// The verdict. `last_progress_ms`: when this observer last SAW the head move (None = never
/// watched it move); the newer of that and the head block's own timestamp is the last block.
pub fn judge(now_ms: u64, live_ts_ms: u64, target: Option<&TargetHead>, last_progress_ms: Option<u64>, max_idle_secs: u64) -> (bool, String) {
    let Some(t) = target else {
        let since = now_ms.saturating_sub(live_ts_ms) / 1000;
        return (false, format!("target RPC not answering ({since} s after LIVE)"));
    };
    // A head block time in the future cannot vouch for recent production (it would hide any idle
    // time through the saturating subtraction): ignored, and said so.
    let future = t.head_block_ms.is_some_and(|b| b > now_ms + FUTURE_SKEW_MS);
    let last_block = t.head_block_ms.filter(|_| !future).max(last_progress_ms);
    // Idle time counts from LIVE at the earliest: the sustained-LIVE gate proved progress up to it.
    let quiet_since_live = last_block.is_none_or(|b| b <= live_ts_ms);
    let idle = now_ms.saturating_sub(last_block.filter(|_| !quiet_since_live).unwrap_or(live_ts_ms));
    if idle > max_idle_secs * 1000 {
        let detail = if quiet_since_live {
            format!("no new block for {} s since LIVE (head {})", idle / 1000, t.head)
        } else {
            format!("no new block for {} s (head {}; LIVE {} s ago)", idle / 1000, t.head, now_ms.saturating_sub(live_ts_ms) / 1000)
        };
        return (false, detail);
    }
    let note = if future { " · head block time is in the future (ignored)" } else { "" };
    if quiet_since_live {
        return (true, format!("head {} · no block yet since LIVE {} s ago{note}", t.head, idle / 1000));
    }
    (true, format!("producing · head {} · last block {} s ago{note}", t.head, idle / 1000))
}

/// The target RPC URL with the journaled chain ids expanded; None while `{blockchain_id}` is
/// still unbound.
pub fn target_url(cfg: &Config, blockchain_id: Option<&str>, subnet_id: Option<&str>) -> Option<String> {
    let mut vars = vec![];
    if let Some(b) = blockchain_id {
        vars.push(("blockchain_id".to_string(), b.to_string()));
    }
    if let Some(s) = subnet_id {
        vars.push(("subnet_id".to_string(), s.to_string()));
    }
    let url = crate::ops::expand_placeholders(&cfg.target.rpc_url, &vars);
    (!url.contains("{blockchain_id}")).then_some(url)
}

/// One `pulsevm.getInfo` against the target. None = no answer.
pub fn fetch_head(agent: &ureq::Agent, url: &str) -> Option<TargetHead> {
    let v: Value = agent
        .post(url)
        .send_json(json!({"jsonrpc": "2.0", "method": "pulsevm.getInfo", "params": {}, "id": 1}))
        .ok()?
        .into_json()
        .ok()?;
    let r = v.get("result").unwrap_or(&v);
    let head = r["head_block_num"].as_u64().or_else(|| r["head_block_num"].as_str()?.parse().ok())?;
    Some(TargetHead {
        head,
        head_block_ms: r["head_block_time"].as_str().and_then(parse_block_time_ms),
        chain_id: r["chain_id"].as_str().map(|s| s.to_ascii_lowercase()),
    })
}

/// One `pulsevm.getBlock(height)` against the target: the block's id, if it has one.
pub fn fetch_block_id(agent: &ureq::Agent, url: &str, height: u64) -> Option<String> {
    let v: Value = agent
        .post(url)
        .send_json(json!({"jsonrpc": "2.0", "method": "pulsevm.getBlock", "params": {"block_num_or_id": height.to_string()}, "id": 1}))
        .ok()?
        .into_json()
        .ok()?;
    let r = v.get("result").unwrap_or(&v);
    ["id", "block_id"].iter().find_map(|k| r.get(*k)?.as_str()).map(|s| s.to_ascii_lowercase())
        .filter(|s| s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()))
}

/// The id of the first block after the cut, once seen (it never changes on one chain).
static AFTER_CUT: Mutex<Option<(String, u64, String)>> = Mutex::new(None);

/// What the fleet compares across producers once a target may be running (rc.23): the target's
/// head and head block id, the chain id it serves and the id of the first block after the cut (a
/// COMMON block above H: equal on every member of one chain, different on any fork or other
/// chain). Public values only (heights, hex ids, the chain's Metal ids from the journal).
pub fn target_view(cfg: &Config, summary: &Value, agent: Option<&ureq::Agent>) -> Value {
    let (bid, sid) = (summary["target_blockchain_id"].as_str(), summary["target_subnet_id"].as_str());
    let mut out = json!({"blockchain_id": bid, "subnet_id": sid, "chain_id": summary["target_chain_id"],
        "head": null, "head_id": null, "after_cut_id": null});
    let (Some(url), Some(agent)) = (target_url(cfg, bid, sid), agent) else { return out };
    let v: Option<Value> = agent.post(&url)
        .send_json(json!({"jsonrpc": "2.0", "method": "pulsevm.getInfo", "params": {}, "id": 1}))
        .ok().and_then(|r| r.into_json().ok());
    if let Some(v) = v {
        let r = v.get("result").unwrap_or(&v);
        out["head"] = json!(r["head_block_num"].as_u64().or_else(|| r["head_block_num"].as_str()?.parse().ok()));
        let hex64 = |x: &Value| x.as_str().map(|s| s.to_ascii_lowercase()).filter(|s| s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()));
        out["head_id"] = json!(hex64(&r["head_block_id"]));
        if out["chain_id"].is_null() {
            out["chain_id"] = json!(hex64(&r["chain_id"]));
        }
    }
    let cut = summary["evidence"]["cut_height"].as_u64();
    if let Some(cut) = cut {
        let mut g = AFTER_CUT.lock().unwrap_or_else(|p| p.into_inner());
        let cached = g.as_ref().filter(|(u, c, _)| u == &url && *c == cut).map(|(_, _, id)| id.clone());
        // rc.25 F4: the first block after the cut never changes on one chain, so a failed head read still reports
        // the cached id (with `unread_since_ms` below): one timed-out read no longer drops this member from the
        // fleet's LIVE group (the verdict flapped LIVE↔DEGRADED with quorum = N).
        let id = cached.or_else(|| out["head"].as_u64().filter(|h| *h > cut).and_then(|_| fetch_block_id(agent, &url, cut + 1)));
        if let Some(id) = &id {
            *g = Some((url.clone(), cut, id.clone()));
        }
        out["after_cut_id"] = json!(id);
    }
    // How long the target's head has been unreadable (consecutive failures, ms since the first), so readers can
    // tell a single timeout from a target that is down.
    let mut u = UNREAD_SINCE.lock().unwrap_or_else(|p| p.into_inner());
    if out["head"].is_null() {
        let now = now_ms();
        let since = *u.get_or_insert(now);
        out["unread_since_ms"] = json!(since);
        out["unread_for_ms"] = json!(now.saturating_sub(since));
    } else {
        *u = None;
    }
    out
}

/// When the target's head first failed to read (consecutive failures), None once it answers.
static UNREAD_SINCE: Mutex<Option<u64>> = Mutex::new(None);

/// What one observer has seen of one target, across beacon cycles.
#[derive(Debug, Clone, PartialEq)]
pub struct Seen {
    pub url: String,
    /// Highest head seen.
    pub max_head: u64,
    /// When the head last moved up (ms).
    pub moved_ms: u64,
    /// The chain the target served the first time it answered.
    pub chain_id: Option<String>,
}

/// Identity and monotonicity, judged against what was seen before (and the chain id the
/// ceremony journaled). Err = the target is not the same chain any more, or went backwards: a
/// failing check at once, whatever the idle time says. Returns the updated record.
pub fn observe(prev: Option<&Seen>, url: &str, h: &TargetHead, now_ms: u64, live_ts_ms: u64, journaled_chain: Option<&str>) -> Result<Seen, String> {
    let want = journaled_chain.map(|c| c.to_ascii_lowercase()).or_else(|| prev.filter(|p| p.url == url).and_then(|p| p.chain_id.clone()));
    if let (Some(w), Some(got)) = (&want, &h.chain_id) {
        if w != got {
            return Err(format!("the target now serves chain_id {got}, not {w}: another chain behind the target RPC"));
        }
    }
    match prev.filter(|p| p.url == url) {
        Some(p) if h.head < p.max_head => Err(format!("head went BACKWARDS: {} after {} (rollback, re-import or another chain)", h.head, p.max_head)),
        Some(p) if h.head > p.max_head => Ok(Seen { url: url.into(), max_head: h.head, moved_ms: now_ms, chain_id: p.chain_id.clone().or(h.chain_id.clone()) }),
        Some(p) => Ok(p.clone()),
        None => Ok(Seen { url: url.into(), max_head: h.head, moved_ms: live_ts_ms, chain_id: want.or(h.chain_id.clone()) }),
    }
}

/// Observed head progress across beacon cycles.
static SEEN: Mutex<Option<Seen>> = Mutex::new(None);
/// Last probe verdict: (when it ran, ok, sanitized detail).
static PROBE: Mutex<Option<(Instant, bool, String)>> = Mutex::new(None);

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// The probe verdict, re-running `post_live_probe_cmd` at most every `PROBE_EVERY`.
fn probe(cfg: &Config, vars: &[(String, String)]) -> Option<(bool, String)> {
    let cmd = cfg.target.post_live_probe_cmd.as_ref()?;
    let mut g = PROBE.lock().unwrap_or_else(|p| p.into_inner());
    if let Some((t, ok, d)) = g.as_ref() {
        if t.elapsed() < PROBE_EVERY {
            return Some((*ok, d.clone()));
        }
    }
    let cmd = crate::ops::expand_placeholders(cmd, vars);
    let (ok, d) = match crate::ops::run_shell_timeout(&cmd, PROBE_TIMEOUT) {
        Ok(_) => (true, "probe ok".to_string()),
        Err(e) => {
            // "`cmd` exited exit status: N: <stderr> <stdout>": report the probe's own words.
            let re = regex::Regex::new(r"^`[\s\S]*?` exited [^:]*: \d+: ").expect("static regex");
            let why = re.replace(&e, "");
            (false, format!("probe failed: {}", crate::beacon::sanitize_short(why.trim())))
        }
    };
    *g = Some((Instant::now(), ok, d.clone()));
    Some((ok, d))
}

/// The `target_live` check for a journal whose state is LIVE (`summary` = beacon journal
/// summary). `with_probe`: run the operator probe (the beacon does; `status` does not).
pub fn check(cfg: &Config, summary: &Value, agent: Option<&ureq::Agent>, with_probe: bool) -> Option<Value> {
    if summary["state"].as_str() != Some("LIVE") {
        return None;
    }
    let live_ts = summary["live_ts_ms"].as_u64()?;
    let (bid, sid) = (summary["target_blockchain_id"].as_str(), summary["target_subnet_id"].as_str());
    let now = now_ms();
    let Some(url) = target_url(cfg, bid, sid) else {
        return Some(json!({"name": CHECK, "ok": false, "detail": "target RPC unknown (no blockchain id journaled)"}));
    };
    let Some(agent) = agent else {
        return Some(json!({"name": CHECK, "ok": false, "detail": "skipped (collection time budget exhausted)"}));
    };
    let head = fetch_head(agent, &url);
    let journaled_chain = summary["target_chain_id"].as_str();
    let mut identity: Option<String> = None;
    let progress = {
        let mut g = SEEN.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(h) = &head {
            match observe(g.as_ref(), &url, h, now, live_ts, journaled_chain) {
                Ok(next) => *g = Some(next),
                // The record keeps the highest head and first chain: a regression stays red until
                // the target is back at or above what was seen.
                Err(e) => identity = Some(e),
            }
        }
        g.as_ref().filter(|s| s.url == url).map(|s| s.moved_ms)
    };
    let max_idle = cfg.target.post_live_max_idle_secs;
    let (mut ok, mut detail) = if let Some(e) = identity {
        (false, e)
    } else if max_idle == 0 {
        // Idle judgment off (rehearsal only); the probe below still runs.
        (head.is_some(), match &head { Some(h) => format!("head {} (idle check off)", h.head), None => "target RPC not answering".into() })
    } else {
        judge(now, live_ts, head.as_ref(), progress, max_idle)
    };
    let idle_only = !ok && head.is_some() && detail.starts_with("no new block");
    if with_probe {
        let mut vars = vec![];
        if let Some(b) = bid {
            vars.push(("blockchain_id".to_string(), b.to_string()));
        }
        if let Some(s) = sid {
            vars.push(("subnet_id".to_string(), s.to_string()));
        }
        if let Some((pok, pd)) = probe(cfg, &vars) {
            if idle_only && pok {
                // A quiet chain is not a stuck one when the operator's workload probe passes: PulseVM
                // builds blocks only for transactions, so no traffic = no blocks.
                ok = true;
                detail = format!("idle, probe passing ({detail}) · {pd}");
            } else {
                ok &= pok;
                detail = format!("{detail} · {pd}");
            }
        }
    }
    Some(json!({"name": CHECK, "ok": ok, "detail": detail}))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIVE: u64 = 1_790_000_000_000;

    #[test]
    fn judges_progress_against_the_idle_limit() {
        let t = |head, ms: Option<u64>| TargetHead { head, head_block_ms: ms, chain_id: None };
        // Stage-2 run 2: last block 2 s after LIVE, nothing since.
        let (ok, d) = judge(LIVE + 90_000, LIVE, Some(&t(408462226, Some(LIVE + 2_000))), None, 60);
        assert!(!ok);
        assert_eq!(d, "no new block for 88 s (head 408462226; LIVE 90 s ago)");
        // Nothing at all since LIVE.
        let (ok, d) = judge(LIVE + 90_000, LIVE, Some(&t(100, Some(LIVE - 5_000))), None, 60);
        assert!(!ok);
        assert_eq!(d, "no new block for 90 s since LIVE (head 100)");
        // Producing: a recent block.
        let (ok, d) = judge(LIVE + 90_000, LIVE, Some(&t(150, Some(LIVE + 85_000))), None, 60);
        assert!(ok, "{d}");
        assert!(d.contains("head 150") && d.contains("5 s ago"), "{d}");
        // Within the grace window right after LIVE.
        assert!(judge(LIVE + 30_000, LIVE, Some(&t(100, None)), None, 60).0);
        // No timestamp from the node: the observer's own progress record decides.
        assert!(judge(LIVE + 90_000, LIVE, Some(&t(150, None)), Some(LIVE + 80_000), 60).0);
        assert!(!judge(LIVE + 90_000, LIVE, Some(&t(150, None)), Some(LIVE), 60).0);
        // A skewed-old node clock does not hide a head the observer saw move.
        assert!(judge(LIVE + 90_000, LIVE, Some(&t(150, Some(LIVE - 1))), Some(LIVE + 70_000), 60).0);
        // Unreachable target.
        let (ok, d) = judge(LIVE + 10_000, LIVE, None, None, 60);
        assert!(!ok && d.contains("not answering"), "{d}");
    }

    /// Review: a head block time in the future suppressed the idle calculation.
    #[test]
    fn a_future_head_block_time_does_not_hide_an_idle_target() {
        let t = TargetHead { head: 150, head_block_ms: Some(LIVE + 3_600_000), chain_id: None };
        let (ok, d) = judge(LIVE + 90_000, LIVE, Some(&t), None, 60);
        assert!(!ok, "{d}");
        let (ok, d) = judge(LIVE + 30_000, LIVE, Some(&t), None, 60);
        assert!(ok && d.contains("in the future (ignored)"), "{d}");
    }

    #[test]
    fn head_regression_and_a_chain_change_fail_at_once() {
        let h = |head, c: &str| TargetHead { head, head_block_ms: None, chain_id: Some(c.into()) };
        let s = observe(None, "u", &h(100, "aa"), LIVE + 1, LIVE, Some("AA")).unwrap();
        assert_eq!((s.max_head, s.moved_ms), (100, LIVE));
        let s = observe(Some(&s), "u", &h(105, "aa"), LIVE + 5_000, LIVE, Some("aa")).unwrap();
        assert_eq!((s.max_head, s.moved_ms), (105, LIVE + 5_000));
        assert!(observe(Some(&s), "u", &h(104, "aa"), LIVE + 6_000, LIVE, Some("aa")).unwrap_err().contains("BACKWARDS"));
        assert!(observe(Some(&s), "u", &h(200, "bb"), LIVE + 6_000, LIVE, Some("aa")).unwrap_err().contains("chain_id"));
        // No journaled chain: the first one seen is the reference.
        assert!(observe(Some(&s), "u", &h(200, "bb"), LIVE + 6_000, LIVE, None).unwrap_err().contains("chain_id"));
        // A new URL (the chain id got bound) starts a fresh record.
        assert_eq!(observe(Some(&s), "v", &h(7, "aa"), LIVE + 6_000, LIVE, None).unwrap().max_head, 7);
    }

    #[test]
    fn block_times_parse_like_pulsevm_prints_them() {
        assert_eq!(parse_block_time_ms("2000-01-01T00:00:00.500"), Some(946_684_800_500));
        assert_eq!(parse_block_time_ms("2000-01-01T00:00:01Z"), Some(946_684_801_000));
        assert_eq!(parse_block_time_ms("nonsense"), None);
    }
}
