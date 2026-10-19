//! rc.33: the all-producers gate. Closes the last "transaction after H" gap the admission lock (rc.32, docs/DESIGN.md
//! §2a) leaves open: a producer that is NOT running an armed agent with the lock on can still put transactions in its
//! blocks between the freeze and the pause. The burn-off audit catches that and aborts (safe, but the cut fails). This
//! gate makes the ceremony unable to reach the freeze unless every producer that could produce a block before the
//! pause runs an rc.33+ agent that is ARMED for this event, with `admission_lock = true` and a working lock probe.
//!
//! The set of producers ("required") is derived by the coordinator from the source chain's `get_producer_schedule`
//! (active, pending and proposed) plus the configured standbys, when the event is made (`coord.mjs event
//! --producer-gate yes`), and bound into the SIGNED event (`producer_gate`, with a sha256 over a canonical text, see
//! [`ProducerGate::canonical`]; `control/coordlib.mjs` computes the same text, and a shared vector checks both). Each
//! required name maps to the roster member (the beacon's producer account) that runs its agent: itself, or the
//! member named in `cover` (rehearsals where one server signs for several schedule names).
//!
//! Checks, all fail-closed ("unknown is never yes"):
//! - `await` refuses an event whose gate is malformed, whose hash does not match, whose members are not on the roster,
//!   or whose `min_agent_version` is newer than this agent;
//! - ARM (preflight): the lock must be on, and the live schedule must still be the bound one (active and pending
//!   unchanged, every name in active, pending and proposed inside the required set);
//! - right before the freeze: the same schedule check, AND every required name's member must have a FRESH beacon
//!   report on the relay for this event, in ARMED or FROZEN, journaled with this gate's hash, run by an agent at least
//!   `min_agent_version`, with the `admission_lock` and `admission_lock_probe` checks green. Otherwise the agent
//!   aborts before it freezes anything.
//!
//! Limits (docs/DESIGN.md §2b, RISK-REGISTER A-24): mission control is an unsigned relay and a producer can lie in its
//! own report (reports are authenticated per producer by a bearer token, not signed); the burn-off audit stays the
//! final backstop.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The first agent version that runs this gate. An event's `min_agent_version` is at least this.
pub const MIN_AGENT_VERSION: &str = "0.5.0-rc.33";

/// The states a member may report at the freeze tick (a member a block ahead may already be FROZEN).
pub const FREEZE_STATES: &[&str] = &["ARMED", "FROZEN"];

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Sched {
    pub version: u64,
    pub producers: Vec<String>,
}

/// The signed event's `producer_gate` (also written by `await` into the derived config as
/// `[coordination.producer_gate]`).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProducerGate {
    #[serde(default = "one")]
    pub v: u64,
    /// Every schedule account that could produce before the pause: active ∪ pending ∪ proposed ∪ standby (sorted).
    pub required: Vec<String>,
    /// schedule name → roster member running its agent, where they differ (rehearsals with shared producer identity).
    #[serde(default)]
    pub cover: BTreeMap<String, String>,
    pub active: Sched,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<Sched>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed: Option<Sched>,
    #[serde(default)]
    pub standby: Vec<String>,
    pub min_agent_version: String,
    /// sha256 (hex) of [`ProducerGate::canonical`].
    pub sha256: String,
}

fn one() -> u64 {
    1
}

fn valid_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 12
        && n.bytes()
            .all(|b| b.is_ascii_lowercase() || (b'1'..=b'5').contains(&b) || b == b'.')
}

impl ProducerGate {
    /// The roster member that runs `name`'s agent.
    pub fn member_of<'a>(&'a self, name: &'a str) -> &'a str {
        self.cover.get(name).map(String::as_str).unwrap_or(name)
    }

    /// The text the sha256 covers. Kept byte-for-byte equal to `producerGateCanonical` in control/coordlib.mjs.
    pub fn canonical(&self) -> String {
        let mut req: Vec<&String> = self.required.iter().collect();
        req.sort();
        let sched = |s: &Option<Sched>| match s {
            None => "-".to_string(),
            Some(s) => format!("{}:{}", s.version, s.producers.join(",")),
        };
        let mut standby = self.standby.clone();
        standby.sort();
        [
            "pulse-cutover producer gate v1".to_string(),
            format!(
                "required={}",
                req.iter()
                    .map(|n| format!("{n}:{}", self.member_of(n)))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            format!("active={}", sched(&Some(self.active.clone()))),
            format!("pending={}", sched(&self.pending)),
            format!("proposed={}", sched(&self.proposed)),
            format!("standby={}", standby.join(",")),
            format!("min_agent_version={}", self.min_agent_version),
        ]
        .join("\n")
    }

    pub fn compute_sha256(&self) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(self.canonical().as_bytes()))
    }

    /// The gate is well formed and covered by `roster` (the event's members). Checked by `await` on the signed event
    /// and at config load on the derived config.
    pub fn check(&self, roster: &[crate::config::RosterMember]) -> Result<(), String> {
        if self.v != 1 {
            return Err(format!(
                "producer_gate v{} is not understood by this agent",
                self.v
            ));
        }
        if !self.sha256.eq_ignore_ascii_case(&self.compute_sha256()) {
            return Err("producer_gate sha256 does not match its contents".into());
        }
        if parse_version(&self.min_agent_version).is_none() {
            return Err(format!(
                "producer_gate min_agent_version {:?} is not a version",
                self.min_agent_version
            ));
        }
        if !version_at_least(&self.min_agent_version, MIN_AGENT_VERSION) {
            return Err(format!(
                "producer_gate min_agent_version {} is older than {MIN_AGENT_VERSION}, the first agent that runs the gate",
                self.min_agent_version
            ));
        }
        let mut union: BTreeSet<&str> = BTreeSet::new();
        for s in [
            Some(&self.active),
            self.pending.as_ref(),
            self.proposed.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            union.extend(s.producers.iter().map(String::as_str));
        }
        union.extend(self.standby.iter().map(String::as_str));
        let req: BTreeSet<&str> = self.required.iter().map(String::as_str).collect();
        if req.len() != self.required.len() {
            return Err("producer_gate required lists a producer twice".into());
        }
        if req != union {
            return Err(
                "producer_gate required is not exactly active ∪ pending ∪ proposed ∪ standby"
                    .into(),
            );
        }
        if self.active.producers.is_empty() {
            return Err("producer_gate active schedule is empty".into());
        }
        if let Some(bad) = req.iter().find(|n| !valid_name(n)) {
            return Err(format!(
                "producer_gate names {bad:?}, which is not an account name"
            ));
        }
        if let Some(extra) = self.cover.keys().find(|k| !req.contains(k.as_str())) {
            return Err(format!(
                "producer_gate cover maps {extra}, which is not a required producer"
            ));
        }
        let members: BTreeSet<&str> = roster.iter().map(|m| m.producer.as_str()).collect();
        let uncovered: Vec<String> = self
            .required
            .iter()
            .filter(|n| !members.contains(self.member_of(n)))
            .map(|n| {
                if self.member_of(n) == n {
                    n.clone()
                } else {
                    format!("{n} (covered by {})", self.member_of(n))
                }
            })
            .collect();
        if !uncovered.is_empty() {
            return Err(format!(
                "producer_gate: these producers can produce before the pause but no roster member runs their agent: {}",
                uncovered.join(", ")
            ));
        }
        Ok(())
    }

    /// What is wrong with the live schedule (`get_producer_schedule` JSON) for this gate. Empty = it is still the one
    /// the event bound: active and pending unchanged, and every name in active, pending and proposed is required.
    pub fn schedule_problems(&self, live: &Value) -> Vec<String> {
        let mut out = vec![];
        let (Some(active), pending, proposed) = (
            sched_of(&live["active"]),
            sched_of(&live["pending"]),
            sched_of(&live["proposed"]),
        ) else {
            return vec!["get_producer_schedule has no active schedule".into()];
        };
        let req: BTreeSet<&str> = self.required.iter().map(String::as_str).collect();
        let mut outside = BTreeSet::new();
        for (what, s) in [
            ("active", Some(&active)),
            ("pending", pending.as_ref()),
            ("proposed", proposed.as_ref()),
        ] {
            for p in s.into_iter().flat_map(|s| s.producers.iter()) {
                if !req.contains(p.as_str()) {
                    outside.insert(format!("{p} ({what})"));
                }
            }
        }
        if !outside.is_empty() {
            out.push(format!(
                "producers in the live schedule that are not in the event's armed set: {}",
                outside.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
        if active != self.active {
            out.push(format!(
                "the active schedule changed since the event (bound v{}, now v{})",
                self.active.version, active.version
            ));
        }
        if pending != self.pending {
            out.push(format!(
                "the pending schedule changed since the event (bound {}, now {})",
                self.pending
                    .as_ref()
                    .map(|s| format!("v{}", s.version))
                    .unwrap_or_else(|| "none".into()),
                pending
                    .as_ref()
                    .map(|s| format!("v{}", s.version))
                    .unwrap_or_else(|| "none".into())
            ));
        }
        out
    }

    /// The members whose reports on the relay's `/api/status` do not show them ready, one entry per required name:
    /// `{producer, member, why}`. `states`: the ceremony states that count ([`FREEZE_STATES`] before the freeze).
    pub fn relay_problems(
        &self,
        status: &Value,
        network: &str,
        event_id: &str,
        max_age_ms: u64,
        states: &[&str],
    ) -> Vec<Value> {
        let producers: Vec<Value> = status["networks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|n| n["id"].as_str() == Some(network))
            .flat_map(|n| n["producers"].as_array().cloned().unwrap_or_default())
            .collect();
        let mut verdict: BTreeMap<String, Option<String>> = BTreeMap::new();
        let mut out = vec![];
        for name in &self.required {
            let member = self.member_of(name).to_string();
            let why = verdict
                .entry(member.clone())
                .or_insert_with(|| {
                    member_problem(&producers, &member, self, event_id, max_age_ms, states)
                })
                .clone();
            if let Some(why) = why {
                out.push(json!({"producer": name, "member": member, "why": why}));
            }
        }
        out
    }
}

/// None = `member` has at least one report that satisfies the gate; Some(why) otherwise (the best candidate's
/// reasons, or "no report").
fn member_problem(
    producers: &[Value],
    member: &str,
    gate: &ProducerGate,
    event_id: &str,
    max_age_ms: u64,
    states: &[&str],
) -> Option<String> {
    let Some(p) = producers
        .iter()
        .find(|p| p["name"].as_str() == Some(member))
    else {
        return Some("no beacon report on mission control".into());
    };
    let mut reports: Vec<(Value, Option<u64>, bool, bool)> = p["beacons"]
        .as_array()
        .map(|bs| {
            bs.iter()
                .map(|b| {
                    (
                        b["report"].clone(),
                        b["age_ms"].as_u64(),
                        b["conflict"].as_bool().unwrap_or(false),
                        b["silent"].as_bool().unwrap_or(false),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    if reports.is_empty() && !p["report"].is_null() {
        reports.push((
            p["report"].clone(),
            p["age_ms"].as_u64(),
            p["conflict"].as_bool().unwrap_or(false),
            p["silent"].as_bool().unwrap_or(false),
        ));
    }
    if reports.is_empty() {
        return Some("no beacon report on mission control".into());
    }
    let mut best: Option<Vec<String>> = None;
    for (r, age, conflict, silent) in reports {
        if r["role"].as_str().is_some_and(|x| x != "producer") {
            continue;
        }
        let mut why = vec![];
        if conflict {
            why.push("identity conflict (one token, two machines)".to_string());
        }
        if silent || age.is_none_or(|a| a > max_age_ms) {
            why.push(format!("stale report (older than {} s)", max_age_ms / 1000));
        }
        if r["coord"]["event_id"].as_str() != Some(event_id) {
            why.push("not reporting this event".into());
        }
        let ce = &r["ceremony"];
        let st = ce["state"].as_str().unwrap_or("none");
        if !states.contains(&st) {
            why.push(format!(
                "ceremony state {st} (needs {})",
                states.join(" or ")
            ));
        }
        if !ce["evidence"]["producer_gate_sha256"]
            .as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case(&gate.sha256))
        {
            why.push("its journal does not carry this event's producer gate".into());
        }
        let ver = ce["evidence"]["agent_version"]
            .as_str()
            .or(r["agent_version"].as_str());
        if !ver.is_some_and(|v| version_at_least(v, &gate.min_agent_version)) {
            why.push(format!(
                "agent {} is older than {}",
                ver.unwrap_or("unknown"),
                gate.min_agent_version
            ));
        }
        for c in ["admission_lock", "admission_lock_probe"] {
            let ok = r["checks"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|x| x["name"].as_str() == Some(c) && x["ok"].as_bool() == Some(true));
            if !ok {
                why.push(format!("check {c} is not green"));
            }
        }
        if why.is_empty() {
            return None;
        }
        if best.as_ref().is_none_or(|b| why.len() < b.len()) {
            best = Some(why);
        }
    }
    Some(
        best.map(|b| b.join("; "))
            .unwrap_or_else(|| "no producer-role beacon report".into()),
    )
}

/// One schedule of a `get_producer_schedule` answer (Leap: `{version, producers: [{producer_name, authority}]}`, or
/// null).
pub fn sched_of(v: &Value) -> Option<Sched> {
    let version = v.get("version")?.as_u64()?;
    let producers = v["producers"]
        .as_array()?
        .iter()
        .filter_map(|p| p["producer_name"].as_str().or(p.as_str()).map(String::from))
        .collect();
    Some(Sched { version, producers })
}

type Version = ([u64; 3], Vec<String>);

/// semver precedence for the versions this project tags (`0.5.0-rc.33`); build metadata is ignored.
pub fn parse_version(v: &str) -> Option<Version> {
    let v = v.trim().trim_start_matches('v');
    let v = v.split('+').next()?;
    let (core, pre) = match v.split_once('-') {
        Some((c, p)) => (c, Some(p)),
        None => (v, None),
    };
    let nums: Vec<u64> = core
        .split('.')
        .map(|x| x.parse().ok())
        .collect::<Option<_>>()?;
    let [a, b, c] = nums.as_slice() else {
        return None;
    };
    let pre: Vec<String> = match pre {
        Some("") => return None,
        Some(p) => p.split('.').map(String::from).collect(),
        None => vec![],
    };
    Some(([*a, *b, *c], pre))
}

pub fn cmp_version(a: &str, b: &str) -> Option<Ordering> {
    let (ca, pa) = parse_version(a)?;
    let (cb, pb) = parse_version(b)?;
    Some(
        ca.cmp(&cb)
            .then_with(|| match (pa.is_empty(), pb.is_empty()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => {
                    for (x, y) in pa.iter().zip(pb.iter()) {
                        let o = match (x.parse::<u64>(), y.parse::<u64>()) {
                            (Ok(i), Ok(j)) => i.cmp(&j),
                            (Ok(_), Err(_)) => Ordering::Less,
                            (Err(_), Ok(_)) => Ordering::Greater,
                            (Err(_), Err(_)) => x.cmp(y),
                        };
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                    pa.len().cmp(&pb.len())
                }
            }),
    )
}

/// `v` ≥ `min`; an unparsable version is never at least anything.
pub fn version_at_least(v: &str, min: &str) -> bool {
    matches!(
        cmp_version(v, min),
        Some(Ordering::Greater | Ordering::Equal)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RosterMember;

    fn gate() -> ProducerGate {
        let mut g = ProducerGate {
            v: 1,
            required: vec!["bpa".into(), "bpb".into(), "bpc".into(), "sby".into()],
            cover: BTreeMap::new(),
            active: Sched {
                version: 7,
                producers: vec!["bpa".into(), "bpb".into()],
            },
            pending: None,
            proposed: Some(Sched {
                version: 8,
                producers: vec!["bpa".into(), "bpc".into()],
            }),
            standby: vec!["sby".into()],
            min_agent_version: MIN_AGENT_VERSION.into(),
            sha256: String::new(),
        };
        g.sha256 = g.compute_sha256();
        g
    }
    fn roster(names: &[&str]) -> Vec<RosterMember> {
        names
            .iter()
            .map(|p| RosterMember {
                producer: p.to_string(),
                instance_id: None,
            })
            .collect()
    }

    #[test]
    fn canonical_text_and_hash_match_the_shared_control_vector() {
        let v: Value = serde_json::from_str(include_str!(
            "../control/test/fixtures/producer-gate-vector.json"
        ))
        .unwrap();
        let g: ProducerGate = serde_json::from_value(v["gate"].clone()).unwrap();
        assert_eq!(g.canonical(), v["canonical"].as_str().unwrap());
        assert_eq!(g.compute_sha256(), v["gate"]["sha256"].as_str().unwrap());
        g.check(&roster(&["bp1", "bp2", "bp3"])).unwrap();
    }

    #[test]
    fn check_requires_the_union_a_matching_hash_and_a_covering_roster() {
        let g = gate();
        g.check(&roster(&["bpa", "bpb", "bpc", "sby"])).unwrap();
        let e = g.check(&roster(&["bpa", "bpb", "bpc"])).unwrap_err();
        assert!(e.contains("sby"), "{e}");
        let mut t = g.clone();
        t.required.pop();
        assert!(t.check(&roster(&["bpa", "bpb", "bpc", "sby"])).is_err());
        let mut t = g.clone();
        t.active.producers.push("bpz".into());
        assert!(t
            .check(&roster(&["bpa", "bpb", "bpc", "sby"]))
            .unwrap_err()
            .contains("sha256"));
        let mut t = g.clone();
        t.min_agent_version = "0.5.0-rc.32".into();
        t.sha256 = t.compute_sha256();
        assert!(t
            .check(&roster(&["bpa", "bpb", "bpc", "sby"]))
            .unwrap_err()
            .contains("older"));
        // cover: one member runs several names
        let mut t = g.clone();
        t.cover = [
            ("bpa", "box1"),
            ("bpb", "box1"),
            ("bpc", "box2"),
            ("sby", "box2"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
        t.sha256 = t.compute_sha256();
        t.check(&roster(&["box1", "box2"])).unwrap();
        assert!(t
            .check(&roster(&["box1"]))
            .unwrap_err()
            .contains("covered by box2"));
    }

    #[test]
    fn schedule_problems_flag_outsiders_and_active_or_pending_changes() {
        let g = gate();
        let live = |active: Value, pending: Value, proposed: Value| json!({"active": active, "pending": pending, "proposed": proposed});
        let s = |v: u64, p: &[&str]| json!({"version": v, "producers": p.iter().map(|n| json!({"producer_name": n, "authority": [0, {}]})).collect::<Vec<_>>()});
        assert!(g
            .schedule_problems(&live(
                s(7, &["bpa", "bpb"]),
                Value::Null,
                s(8, &["bpa", "bpc"])
            ))
            .is_empty());
        // a proposal among armed producers only is tolerated (it cannot bring in anyone outside the set)
        assert!(g
            .schedule_problems(&live(s(7, &["bpa", "bpb"]), Value::Null, Value::Null))
            .is_empty());
        let p = g.schedule_problems(&live(
            s(7, &["bpa", "bpb"]),
            Value::Null,
            s(8, &["bpa", "evil"]),
        ));
        assert!(p[0].contains("evil (proposed)"), "{p:?}");
        let p = g.schedule_problems(&live(s(8, &["bpa", "bpc"]), Value::Null, Value::Null));
        assert!(
            p.iter().any(|x| x.contains("active schedule changed")),
            "{p:?}"
        );
        let p = g.schedule_problems(&live(
            s(7, &["bpa", "bpb"]),
            s(8, &["bpa", "bpc"]),
            Value::Null,
        ));
        assert!(
            p.iter().any(|x| x.contains("pending schedule changed")),
            "{p:?}"
        );
        assert!(
            !g.schedule_problems(&json!({})).is_empty(),
            "unknown is never yes"
        );
    }

    #[test]
    fn versions_compare_by_semver_precedence() {
        assert!(version_at_least("0.5.0-rc.33", "0.5.0-rc.33"));
        assert!(version_at_least("0.5.0-rc.100", "0.5.0-rc.33"));
        assert!(!version_at_least("0.5.0-rc.32", "0.5.0-rc.33"));
        assert!(version_at_least("0.5.0", "0.5.0-rc.33"));
        assert!(version_at_least("0.6.0-rc.1", "0.5.0-rc.33"));
        assert!(!version_at_least("0.4.9", "0.5.0-rc.33"));
        assert!(!version_at_least("garbage", "0.5.0-rc.33"));
        assert!(version_at_least(
            env!("CARGO_PKG_VERSION"),
            MIN_AGENT_VERSION
        ));
    }

    fn report(member: &str, state: &str, sha: &str, ver: &str, probe: bool, age: u64) -> Value {
        json!({"name": member, "beacons": [{"role": "producer", "age_ms": age, "silent": false, "conflict": false, "report": {
            "role": "producer", "agent_version": ver, "coord": {"event_id": "ev-1"},
            "checks": [{"name": "admission_lock", "ok": true}, {"name": "admission_lock_probe", "ok": probe}],
            "ceremony": {"state": state, "evidence": {"producer_gate_sha256": sha, "agent_version": ver}}}}]})
    }

    #[test]
    fn relay_problems_name_each_missing_stale_old_or_unprobed_member() {
        let g = gate();
        let doc = |ps: Vec<Value>| json!({"networks": [{"id": "testnet", "producers": ps}]});
        let ok = |m: &str| report(m, "ARMED", &g.sha256, MIN_AGENT_VERSION, true, 1000);
        let all = doc(vec![ok("bpa"), ok("bpb"), ok("bpc"), ok("sby")]);
        assert!(g
            .relay_problems(&all, "testnet", "ev-1", 60_000, FREEZE_STATES)
            .is_empty());
        let d = doc(vec![
            ok("bpa"),
            report("bpb", "ARMED", &g.sha256, MIN_AGENT_VERSION, true, 120_000),
            report("bpc", "ARMED", &g.sha256, "0.5.0-rc.32", true, 1000),
            report("sby", "ARMED", &g.sha256, MIN_AGENT_VERSION, false, 1000),
        ]);
        let p = g.relay_problems(&d, "testnet", "ev-1", 60_000, FREEZE_STATES);
        let why = |n: &str| {
            p.iter()
                .find(|x| x["producer"] == n)
                .map(|x| x["why"].as_str().unwrap().to_string())
                .unwrap_or_default()
        };
        assert!(why("bpb").contains("stale"));
        assert!(why("bpc").contains("older"));
        assert!(why("sby").contains("admission_lock_probe"));
        assert!(why("bpa").is_empty());
        let p = g.relay_problems(
            &doc(vec![ok("bpa")]),
            "testnet",
            "ev-1",
            60_000,
            FREEZE_STATES,
        );
        assert_eq!(p.len(), 3);
        assert!(p[0]["why"].as_str().unwrap().contains("no beacon report"));
        let d = doc(vec![
            ok("bpa"),
            ok("bpb"),
            ok("bpc"),
            report("sby", "IDLE", &g.sha256, MIN_AGENT_VERSION, true, 1000),
        ]);
        assert!(
            g.relay_problems(&d, "testnet", "ev-1", 60_000, FREEZE_STATES)[0]["why"]
                .as_str()
                .unwrap()
                .contains("ceremony state IDLE")
        );
    }
}
