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

/// What the target said: its head and the head block's timestamp (ms since the epoch), if any.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetHead {
    pub head: u64,
    pub head_block_ms: Option<u64>,
}

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
    let last_block = t.head_block_ms.max(last_progress_ms);
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
    if quiet_since_live {
        return (true, format!("head {} · no block yet since LIVE {} s ago", t.head, idle / 1000));
    }
    (true, format!("producing · head {} · last block {} s ago", t.head, idle / 1000))
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
    Some(TargetHead { head, head_block_ms: r["head_block_time"].as_str().and_then(parse_block_time_ms) })
}

/// Observed head progress, per target URL, across beacon cycles: (url, head, when it last moved).
static SEEN: Mutex<Option<(String, u64, u64)>> = Mutex::new(None);
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
    if summary["state"].as_str() != Some("LIVE") || cfg.target.post_live_max_idle_secs == 0 {
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
    let progress = {
        let mut g = SEEN.lock().unwrap_or_else(|p| p.into_inner());
        match (&head, g.as_ref()) {
            (Some(h), Some((u, last, _))) if u == &url && h.head > *last => *g = Some((url.clone(), h.head, now)),
            (Some(h), Some((u, _, _))) if u != &url => *g = Some((url.clone(), h.head, live_ts)),
            (Some(h), None) => *g = Some((url.clone(), h.head, live_ts)),
            _ => {}
        }
        g.as_ref().filter(|(u, _, _)| u == &url).map(|(_, _, t)| *t)
    };
    let (mut ok, mut detail) = judge(now, live_ts, head.as_ref(), progress, cfg.target.post_live_max_idle_secs);
    if with_probe {
        let mut vars = vec![];
        if let Some(b) = bid {
            vars.push(("blockchain_id".to_string(), b.to_string()));
        }
        if let Some(s) = sid {
            vars.push(("subnet_id".to_string(), s.to_string()));
        }
        if let Some((pok, pd)) = probe(cfg, &vars) {
            ok &= pok;
            detail = format!("{detail} · {pd}");
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
        let t = |head, ms: Option<u64>| TargetHead { head, head_block_ms: ms };
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

    #[test]
    fn block_times_parse_like_pulsevm_prints_them() {
        assert_eq!(parse_block_time_ms("2000-01-01T00:00:00.500"), Some(946_684_800_500));
        assert_eq!(parse_block_time_ms("2000-01-01T00:00:01Z"), Some(946_684_801_000));
        assert_eq!(parse_block_time_ms("nonsense"), None);
    }
}
