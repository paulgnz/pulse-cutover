//! The agent's view of the fleet, from mission control's relay (`GET /api/status`), for the decisions
//! a local observation cannot make alone (rc.23, after the 5-BP rehearsal on the upstream stack):
//!
//! - **resume guard** (`resume_guard`): before an agent that froze writes resumes the OLD chain on an
//!   abort, it must positively see that no roster member has started chain creation or ignition for
//!   this event AND that the event's quorum is unreachable without it (fewer than `quorum` other
//!   members are still in the ceremony), or a signed coordinator abort with every member accounted for.
//!   In run r4 an isolated BP aborted on its fleet timeout and resumed the old chain while its four
//!   peers had ignited: 33 user writes landed on the old chain after H.
//! - **live view** (`live_view`): after ignition, is a quorum of roster members on the SAME target
//!   chain (same first block after the cut, head past it)? Then a local liveness symptom (a block gap, a
//!   slow first block, a failing post-ignition hook) is waited out as "degraded" instead of halting.
//! - **join view** (`join_view`): is a quorum LIVE on one target chain whose evidence (snapshot hash,
//!   fingerprints, migration genesis) equals ours? Then `pulse-cutover join` may track that chain.
//!
//! All of it reads unsigned beacon reports through the relay: a roster bounds WHO counts, freshness
//! bounds WHEN; a compromised relay could still misreport (ATOMICITY Known limits #1, #3). Every rule
//! here therefore only ever makes the agent MORE conservative than its local view, except `live_view`,
//! which delays a halt by at most the configured patience and never reopens anything by itself.

use serde_json::{json, Value};

use crate::config::Coordination;

/// One roster member as the relay shows it for one event.
#[derive(Debug, Clone)]
pub struct MemberView {
    pub producer: String,
    pub instance_id: Option<String>,
    /// The freshest usable report for this event (fresh, not identity-conflicted).
    pub report: Option<Value>,
    /// The freshest unconflicted report for this event, however old (the signed-abort rule).
    pub last: Option<Value>,
    pub missing: bool,
    pub stale: bool,
    pub conflict: bool,
    /// The relay's per-event high-water mark for this producer showed it past chain creation (state),
    /// even if the report that showed it is gone (an instance replaced since). rc.23 review #2.
    pub max_past: Option<String>,
}

/// The beacons the relay lists for one producer: (report, age_ms, conflict).
fn beacons(p: &Value) -> Vec<(Value, Option<u64>, bool)> {
    let mut v: Vec<(Value, Option<u64>, bool)> = p["beacons"].as_array().map(|bs| bs.iter()
        .map(|b| (b["report"].clone(), b["age_ms"].as_u64(), b["conflict"].as_bool().unwrap_or(false)))
        .collect()).unwrap_or_default();
    if v.is_empty() && !p["report"].is_null() {
        v.push((p["report"].clone(), p["age_ms"].as_u64(), p["conflict"].as_bool().unwrap_or(false)));
    }
    v
}

/// The network's producer list from a `/api/status` document.
fn producers<'a>(status: &'a Value, network: &str) -> Vec<&'a Value> {
    status["networks"].as_array()
        .and_then(|nets| nets.iter().find(|n| n["id"].as_str() == Some(network)))
        .and_then(|n| n["producers"].as_array())
        .map(|a| a.iter().collect())
        .unwrap_or_default()
}

/// Every member of the event's roster (or, without a roster, every producer the relay lists with a
/// report for this event), with its freshest usable report.
pub fn members(status: &Value, co: &Coordination) -> Vec<MemberView> {
    let event = co.event_id.as_deref();
    let max_age = co.report_max_age_secs * 1000;
    let prods = producers(status, &co.network);
    let for_event = |r: &Value| event.is_none_or(|e| r["coord"]["event_id"].as_str() == Some(e));
    let wanted: Vec<(String, Option<String>)> = if co.roster.is_empty() {
        prods.iter().filter(|p| beacons(p).iter().any(|(r, _, _)| for_event(r)))
            .filter_map(|p| p["name"].as_str().map(|n| (n.to_string(), None))).collect()
    } else {
        co.roster.iter().map(|m| (m.producer.clone(), m.instance_id.clone())).collect()
    };
    wanted.into_iter().map(|(producer, instance_id)| {
        let entry = prods.iter().find(|p| p["name"].as_str() == Some(producer.as_str()));
        // The relay's mark for THIS event: {event_id: {past_create, state}} (rc.23 first shape: {event_id, past_create, state}).
        let mark = entry.map(|p| &p["event_max"]).and_then(|m| match (m["event_id"].as_str(), event) {
            (Some(id), Some(e)) => (id == e).then_some(m),
            (None, Some(e)) => m.get(e),
            _ => None,
        });
        let max_past = mark.filter(|m| m["past_create"].as_bool() == Some(true))
            .map(|m| format!("{}; relay high-water mark", m["state"].as_str().unwrap_or("past creation")));
        let bs: Vec<_> = entry.map(|p| beacons(p)).unwrap_or_default().into_iter()
            .filter(|(r, _, _)| for_event(r))
            .filter(|(r, _, _)| instance_id.as_deref().is_none_or(|id| r["instance_id"].as_str() == Some(id)))
            .collect();
        // rc.27 review (review blocker): a blocking fact from ANY of this producer's instances counts, not only from
        // the freshest report: another instance's newer ABORTED must not hide a past-creation report (same H, or a
        // `foreign_past_create` kept from a report for another H).
        let max_past = max_past.or_else(|| bs.iter().find_map(|(r, _, _)| {
            if let Some(st) = r["foreign_past_create"].as_str() {
                return Some(format!("{st}; a report for another H"));
            }
            past_create(r).then(|| format!("{}; a beacon report", r["ceremony"]["state"].as_str().unwrap_or("past creation")))
        }));
        let usable: Vec<_> = bs.iter().filter(|(_, _, c)| !c).collect();
        let fresh = usable.iter().filter(|(_, age, _)| age.is_some_and(|a| a <= max_age)).min_by_key(|(_, age, _)| age.unwrap_or(u64::MAX));
        let last = usable.iter().min_by_key(|(_, age, _)| age.unwrap_or(u64::MAX)).map(|(r, _, _)| r.clone());
        MemberView {
            producer, instance_id, max_past, last,
            report: fresh.map(|(r, _, _)| r.clone()),
            missing: bs.is_empty(),
            stale: !bs.is_empty() && !usable.is_empty() && fresh.is_none(),
            conflict: !bs.is_empty() && usable.is_empty(),
        }
    }).collect()
}

/// rc.24 fleet rehearsal: a beacon pairs `await`'s coordination state (the event id) with whatever journal its
/// config points at, so a reused run directory or a stale journal next to a newer event reported an OLD
/// ceremony (e.g. ABORTED with the source resumed) under the current event id. A report whose journal was
/// armed for another H (`ceremony.evidence.h`, else `evidence.cut_height`) is not evidence for this event:
/// its ceremony is replaced by `null`, so the member reads as "no state" — still in the ceremony for the
/// resume guard (more conservative), never LIVE / ABORTED / past creation. Returns how many were dropped.
pub fn drop_foreign_ceremonies(status: &mut Value, network: &str, h: u64) -> usize {
    let mut dropped = 0;
    let Some(nets) = status["networks"].as_array_mut() else { return 0 };
    for net in nets.iter_mut().filter(|n| n["id"].as_str() == Some(network)) {
        let Some(prods) = net["producers"].as_array_mut() else { continue };
        for p in prods.iter_mut() {
            let mut fix = |r: &mut Value| {
                // `evidence.h` only (every beacon since rc.10 reports it): a cut-height fallback made a correctly
                // reporting older peer "foreign" on an inexact rehearsal cut (rc.25 review #5).
                let jh = r["ceremony"]["evidence"]["h"].as_u64();
                if jh.is_some_and(|j| j != h) {
                    // Asymmetric (rc.25 review #1): a foreign report loses its PERMISSIVE facts (ABORTED, a resumed
                    // source, LIVE) but never a blocking one. If it says past creation / ignition, that is kept as
                    // `foreign_past_create`, which `past_create` honours: a peer whose H differs from ours (our
                    // config may be the wrong one) and that ignited must still stop a resume.
                    if past_create(r) {
                        r["foreign_past_create"] = json!(r["ceremony"]["state"].as_str().unwrap_or("past creation"));
                    }
                    r["ceremony"] = Value::Null;
                    dropped += 1;
                }
            };
            if let Some(bs) = p["beacons"].as_array_mut() {
                for b in bs.iter_mut() {
                    fix(&mut b["report"]);
                }
            }
            if p["report"].is_object() {
                fix(&mut p["report"]);
            }
        }
    }
    dropped
}

/// A report is past the point where resuming the old chain is safe: creation, a join or ignition
/// started, or a state after it.
pub fn past_create(report: &Value) -> bool {
    if report["foreign_past_create"].is_string() {
        return true;
    }
    let c = &report["ceremony"];
    matches!(c["state"].as_str(), Some("IGNITED" | "FLIPPED" | "LIVE" | "HALTED"))
        || c["create_started"].as_bool() == Some(true)
        || c["ignition_started"].as_bool() == Some(true)
        || c["joined"].as_bool() == Some(true)
}

fn quorum_of(co: &Coordination) -> usize {
    if co.fleet_quorum > 0 { co.fleet_quorum } else { co.roster.len() }
}

/// Is `m` this node? (Matched by beacon producer and, when the roster pins one, instance id.)
fn is_self(m: &MemberView, me: Option<(&str, &str)>) -> bool {
    me.is_some_and(|(prod, inst)| m.producer == prod && m.instance_id.as_deref().is_none_or(|i| i == inst))
}

/// This node's OWN fresh report for the event, from THIS instance (rc.25 review #2): another box reporting under the
/// same producer name with its own token (a standby, an old box) must not stand in for a silent beacon. A report
/// without an instance id (an older beacon) matches by producer.
fn own_report_visible(status: &Value, co: &Coordination, me: (&str, &str)) -> bool {
    let max_age = co.report_max_age_secs * 1000;
    producers(status, &co.network).iter().filter(|p| p["name"].as_str() == Some(me.0)).any(|p| {
        beacons(p).into_iter().any(|(r, age, conflict)| !conflict && age.is_some_and(|a| a <= max_age)
            && co.event_id.as_deref().is_none_or(|e| r["coord"]["event_id"].as_str() == Some(e))
            && r["instance_id"].as_str().is_none_or(|i| i.eq_ignore_ascii_case(me.1)))
    })
}

/// May an agent that froze writes resume the old chain? Err(why) unless (unknown is never "no"):
///
/// - the event has a roster and a quorum (without them reachability cannot be judged), the relay
///   answered, and no OTHER roster member is missing, identity-conflicted or past chain creation
///   (its fresh report, or the relay's per-event high-water mark for it, which an instance
///   replacement cannot lower); and then either
/// - `signed_abort`: the coordinator signed an abort for this event: every other member has SOME
///   report for it (stale is accepted: the abort is the authority), none past creation; or
/// - the event's quorum is unreachable without this node (as far as fresh reports show): every other member has a FRESH
///   report, and fewer than `quorum` of them are still in the ceremony (not ABORTED / STRANDED).
///   With quorum = N any peer's abort makes it unreachable; with quorum < N a minority that aborts
///   alone strands (the rest can still reach quorum and ignite: the r4 split).
///
/// `me` = (beacon producer, instance id) of this node, left out (its own state is known locally).
pub fn resume_guard(status: Option<&Value>, co: &Coordination, me: Option<(&str, &str)>, signed_abort: bool) -> Result<Value, String> {
    let q = quorum_of(co);
    if co.roster.is_empty() || q == 0 {
        return Err("the event has no roster or quorum: whether the fleet can still commit without this node cannot be judged".into());
    }
    let Some(status) = status else {
        return Err("the coordinator relay did not answer: whether peers started chain creation or ignition is unknown".into());
    };
    let all = members(status, co);
    // rc.24 fleet run c5 (F6): this node's OWN report for the event must be visible and fresh on the relay before
    // it may resume. Otherwise its peers cannot account for it (they strand, since it "may have ignited unseen")
    // while it alone resumes: the one silent member deciding for everyone, backwards.
    let mine: Vec<&MemberView> = all.iter().filter(|m| is_self(m, me)).collect();
    if me.is_none() || mine.is_empty() || mine.iter().any(|m| m.missing || m.conflict || m.report.is_none())
        || !me.is_some_and(|me| own_report_visible(status, co, me)) {
        return Err("own beacon not visible on the relay (this node's report for the event is missing, stale or identity-conflicted): \
                    peers cannot account for this node, so it must not resume the old chain".into());
    }
    let (mut seen, mut in_ceremony) = (vec![], vec![]);
    if let Some((m, st)) = all.iter().filter(|m| !is_self(m, me))
        .find_map(|m| m.report.as_ref().or(m.last.as_ref())?["foreign_past_create"].as_str().map(|st| (m, st))) {
        return Err(format!("{} reports a ceremony for another H that is past chain creation or ignition ({st}): this node's \
            event config may be the wrong one; unknown, so no resume", m.producer));
    }
    // First the strongest evidence: anyone ever reported past creation for this event.
    if let Some(m) = all.iter().filter(|m| !is_self(m, me)).find(|m| m.max_past.is_some()) {
        return Err(format!("{} was reported past chain creation or ignition for this event ({})",
            m.producer, m.max_past.as_deref().unwrap_or("?")));
    }
    for m in all.iter().filter(|m| !is_self(m, me)) {
        if m.missing {
            return Err(format!("{} has no report for this event on the relay (missing or replaced instance): it may have ignited unseen", m.producer));
        }
        if m.conflict {
            return Err(format!("{}'s report is identity-conflicted: its state is unknown", m.producer));
        }
        let r = if signed_abort { m.last.as_ref() } else { m.report.as_ref() };
        let Some(r) = r else {
            return Err(format!("{}'s last report is stale (older than {} s): it may have ignited since", m.producer, co.report_max_age_secs));
        };
        let st = r["ceremony"]["state"].as_str().unwrap_or("none");
        if past_create(r) {
            return Err(format!("{} is past chain creation or ignition ({st})", m.producer));
        }
        if !matches!(st, "ABORTED" | "STRANDED") {
            in_ceremony.push(m.producer.clone());
        }
        seen.push(json!({"producer": m.producer, "state": st, "fresh": m.report.is_some()}));
    }
    if !signed_abort && in_ceremony.len() >= q {
        return Err(format!("{} other members ({}) are still in the ceremony, quorum {q}: they can reach it without this node and ignite",
            in_ceremony.len(), in_ceremony.join(", ")));
    }
    Ok(json!({"members": seen, "still_in_ceremony": in_ceremony, "quorum": q,
        "by": if signed_abort { "signed coordinator abort, every member accounted for" } else { "quorum unreachable without this node" }}))
}

/// The largest group and whether it is the ONLY one that reaches `q` (rc.23 review #9: two groups on
/// different chains each with a quorum is a split, never a decision).
fn unique_best<K: Clone, V: Clone>(groups: Vec<(K, Vec<String>, V)>, q: usize) -> Option<(K, Vec<String>, V, bool)> {
    let mut g = groups;
    g.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
    let second = g.get(1).map(|x| x.1.len()).unwrap_or(0);
    g.into_iter().next().map(|(k, m, v)| (k, m, v, second < q))
}

/// What the fleet shows about the target chain after ignition.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveView {
    /// A quorum of fresh members is on one target chain with a common block after the cut.
    pub healthy: bool,
    pub why: String,
    pub members: Vec<String>,
    pub after_cut_id: Option<String>,
    pub max_head: u64,
    /// rc.25: the view rules the chain out (more than one chain/fork with a quorum, or this node on another
    /// chain / a fork), as opposed to one that merely does not show a quorum yet (a peer still catching up, a
    /// beacon that missed a slow target read, the relay not answering). A hard view ends a degraded wait at once;
    /// a soft one gets a bounded grace (machine.rs degraded_continue).
    pub hard: bool,
}

impl LiveView {
    pub fn is_hard(&self) -> bool {
        self.hard
    }
}

/// How long a member's target may be unreadable and still count on its chain (ms); mission control uses the same.
pub const UNREAD_GRACE_MS: u64 = 60_000;

/// After ignition: is a quorum of roster members (fresh reports for this event, state IGNITED,
/// FLIPPED or LIVE) on the same target chain as this node, sharing one block after the cut, with a
/// head past it? `our_bid` = this node's Metal blockchain id (members on another chain do not count);
/// `our_after_cut` = this node's own block at cut + 1, if it has one (a different one = this node is
/// on a fork: never healthy).
pub fn live_view(status: Option<&Value>, co: &Coordination, cut: u64, our_bid: Option<&str>, our_after_cut: Option<&str>) -> LiveView {
    let none = |why: &str| LiveView { healthy: false, why: why.into(), members: vec![], after_cut_id: None, max_head: 0, hard: false };
    let Some(status) = status else { return none("the coordinator relay did not answer") };
    let q = quorum_of(co);
    if q == 0 {
        return none("the event has no roster or quorum: the fleet cannot vouch for the chain");
    }
    let mut groups: std::collections::BTreeMap<String, (Vec<String>, u64)> = Default::default();
    for m in members(status, co) {
        let Some(r) = m.report else { continue };
        let c = &r["ceremony"];
        // HALTED counts too (rc.24 fleet rehearsal c3a): a halted agent's validator still runs the target chain, and
        // excluding it made one peer's halt shrink every other peer's view below quorum, so the halts cascaded.
        if !matches!(c["state"].as_str(), Some("IGNITED" | "FLIPPED" | "LIVE" | "HALTED")) {
            continue;
        }
        let t = &c["target"];
        let Some(first) = t["after_cut_id"].as_str() else { continue };
        // rc.25 F4: a failed head read keeps the member's (immutable) block after the cut for UNREAD_GRACE_MS: one
        // timed-out read is not a member leaving the chain. Older beacons report no block after the cut then.
        let head = match t["head"].as_u64() {
            Some(h) if h <= cut => continue,
            Some(h) => h,
            None if t["unread_for_ms"].as_u64().is_some_and(|ms| ms <= UNREAD_GRACE_MS) => 0,
            None => continue,
        };
        if let (Some(ours), Some(theirs)) = (our_bid, t["blockchain_id"].as_str()) {
            if ours != theirs {
                continue;
            }
        }
        let e = groups.entry(first.to_ascii_lowercase()).or_default();
        e.0.push(m.producer.clone());
        e.1 = e.1.max(head);
    }
    let Some((first, members, max_head, unique)) = unique_best(groups.into_iter().map(|(k, (m, h))| (k, m, h)).collect(), q) else {
        return none("no roster member reports a target block after the cut");
    };
    if !unique {
        return LiveView { healthy: false, members, after_cut_id: Some(first), max_head, hard: true,
            why: "more than one target chain or fork has a quorum: split, not a decision".into() };
    }
    if let Some(ours) = our_after_cut {
        if !ours.eq_ignore_ascii_case(&first) {
            return LiveView { healthy: false, members, after_cut_id: Some(first.clone()), max_head, hard: true,
                why: format!("this node's block after the cut ({}…) differs from the fleet's ({}…): another chain or a fork",
                    &ours[..12.min(ours.len())], &first[..12.min(first.len())]) };
        }
    }
    let healthy = members.len() >= q;
    let why = if healthy {
        format!("{} of quorum {q} members on the same target chain (common block after the cut, head {max_head})", members.len())
    } else {
        format!("only {} member(s) on one target chain, quorum {q}", members.len())
    };
    LiveView { healthy, why, members, after_cut_id: Some(first), max_head, hard: false }
}

/// The chain a `join` may track: a quorum of fresh members LIVE on one target chain (Metal blockchain
/// id + one block after the cut), every one of them reporting OUR snapshot hash, fingerprints and
/// migration genesis hash. Ok((blockchain_id, subnet_id, members)); Err(why).
pub fn join_view(status: Option<&Value>, co: &Coordination, ours: &Value) -> Result<(String, Option<String>, Vec<String>), String> {
    let Some(status) = status else { return Err("the coordinator relay did not answer".into()) };
    let q = quorum_of(co);
    if q == 0 {
        return Err("the event has no roster or quorum: there is no fleet decision to join".into());
    }
    let mut groups: std::collections::BTreeMap<(String, String), (Vec<String>, Option<String>, Vec<String>)> = Default::default();
    for m in members(status, co) {
        let Some(r) = m.report else { continue };
        let c = &r["ceremony"];
        if c["state"].as_str() != Some("LIVE") {
            continue;
        }
        let t = &c["target"];
        let (Some(bid), Some(first)) = (t["blockchain_id"].as_str(), t["after_cut_id"].as_str()) else { continue };
        let e = groups.entry((bid.to_string(), first.to_ascii_lowercase())).or_default();
        e.0.push(m.producer.clone());
        if e.1.is_none() {
            e.1 = t["subnet_id"].as_str().map(str::to_string);
        }
        for k in ["snapshot_sha256", "fingerprints_digest", "boot_genesis_sha256", "compare_allowed_digest", "protocol_schedule_hash"] {
            let (mine, theirs) = (&ours[k], &c["evidence"][k]);
            if !mine.is_null() && mine != theirs {
                e.2.push(format!("{}: {k} {} ≠ ours", m.producer, theirs.as_str().map(|s| format!("{}…", &s[..12.min(s.len())])).unwrap_or_else(|| "missing".into())));
            }
        }
    }
    let Some(((bid, _), members, (subnet, mismatches), unique)) = unique_best(groups.into_iter().map(|(k, (m, s, x))| (k, m, (s, x))).collect(), q) else {
        return Err("no roster member reports LIVE on a target chain".into());
    };
    if !unique {
        return Err("more than one target chain or fork has a LIVE quorum: split, not a decision to join".into());
    }
    if members.len() < q {
        return Err(format!("only {} member(s) LIVE on one target chain ({}), quorum {q}", members.len(), members.join(", ")));
    }
    if !mismatches.is_empty() {
        let hint = if mismatches.iter().any(|m| m.contains("compare_allowed_digest missing")) {
            " (compare_allowed_digest missing: the LIVE members' beacons or the relay may predate rc.26, which a rehearsal \
             join under the compare allowlist needs)"
        } else if mismatches.iter().any(|m| m.contains("protocol_schedule_hash missing")) {
            " (protocol_schedule_hash missing: the LIVE members' beacons or the relay may predate rc.27)"
        } else { "" };
        return Err(format!("the LIVE members' evidence differs from this node's: {}{hint}", mismatches.join("; ")));
    }
    Ok((bid, subnet, members))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RosterMember;

    fn co(roster: &[&str], quorum: usize) -> Coordination {
        Coordination {
            url: "http://mc".into(), network: "rehearsal".into(), coordinator_keys: vec![], auto_arm: false, min_lead_blocks: 1,
            fleet_quorum: quorum, fleet_timeout_secs: 1, event_id: Some("e1".into()),
            roster: roster.iter().map(|p| RosterMember { producer: p.to_string(), instance_id: None }).collect(),
            report_max_age_secs: 60, degraded_patience_secs: 900, degraded_retry_secs: 10, fleet_stall_secs: 300,
        }
    }
    fn rep(state: &str, extra: Value) -> Value {
        let mut c = json!({"state": state, "evidence": {"snapshot_sha256": "aa", "fingerprints_digest": "ff"}});
        if let (Some(o), Some(e)) = (c.as_object_mut(), extra.as_object()) {
            for (k, v) in e { o.insert(k.clone(), v.clone()); }
        }
        json!({"coord": {"event_id": "e1"}, "ceremony": c})
    }
    /// `status` plus this node's own (bp1) fresh report, which the resume guard requires (F6).
    fn status_me(ps: &[(&str, Value, u64)]) -> Value {
        let mut v: Vec<(&str, Value, u64)> = vec![("bp1", rep("ABORTED", json!({})), 1000)];
        v.extend(ps.iter().cloned());
        status(&v)
    }
    fn status(ps: &[(&str, Value, u64)]) -> Value {
        json!({"networks": [{"id": "rehearsal", "producers": ps.iter().map(|(n, r, age)| json!({"name": n, "beacons": [{"age_ms": age, "report": r}]})).collect::<Vec<_>>()}]})
    }

    #[test]
    fn a_timed_out_head_read_keeps_a_member_on_its_chain_for_a_bounded_time() {
        // rc.25 F4: quorum = N, one member's head read just failed (head null, cached block after the cut).
        let c = co(&["bp1", "bp2", "bp3"], 3);
        let live = |head: Value, unread: Option<u64>| {
            let mut t = json!({"blockchain_id": "X", "head": head, "after_cut_id": "a".repeat(64)});
            if let Some(ms) = unread { t["unread_for_ms"] = json!(ms); }
            rep("LIVE", json!({"target": t}))
        };
        let view = |third: Value| live_view(Some(&status(&[("bp1", live(json!(130), None), 1000),
            ("bp2", live(json!(131), None), 1000), ("bp3", third, 1000)])), &c, 120, Some("X"), None);
        assert!(view(live(json!(null), Some(5_000))).healthy, "a single timeout keeps the quorum");
        assert!(!view(live(json!(null), Some(UNREAD_GRACE_MS + 1))).healthy, "a target down for longer drops out");
        assert!(!view(live(json!(null), None)).healthy, "no read age (older beacon): not counted");
    }

    #[test]
    fn resume_guard_needs_every_member_accounted_for_and_the_quorum_unreachable_without_me() {
        let me = Some(("bp1", "i"));
        // q = N (3 of 3): one peer's abort makes the quorum unreachable without me.
        let c = co(&["bp1", "bp2", "bp3"], 3);
        let one_aborted = status_me(&[("bp2", rep("ABORTED", json!({})), 1000), ("bp3", rep("VERIFIED", json!({})), 1000)]);
        assert!(resume_guard(Some(&one_aborted), &c, me, false).is_ok());
        // Review #1: a LOCAL abort while peers are still before VERIFIED. With q < N (2 of 3) the two peers can
        // still reach quorum without me and ignite: the old behaviour resumed here (the common split).
        let c2 = co(&["bp1", "bp2", "bp3"], 2);
        let early = status_me(&[("bp2", rep("FROZEN", json!({})), 1000), ("bp3", rep("ARMED", json!({})), 1000)]);
        assert!(resume_guard(Some(&early), &c2, me, false).unwrap_err().contains("can reach it without this node"));
        // ... and with q = N the same peers cannot (they need me): resume.
        assert!(resume_guard(Some(&early), &c, me, false).is_ok());
        // A minority that aborts alone strands; once enough peers aborted too, it may resume.
        let c5 = co(&["bp1", "bp2", "bp3", "bp4", "bp5"], 4);
        let four_on = status_me(&[("bp2", rep("VERIFIED", json!({})), 1000), ("bp3", rep("VERIFIED", json!({})), 1000),
            ("bp4", rep("SNAPSHOTTED", json!({})), 1000), ("bp5", rep("FROZEN", json!({})), 1000)]);
        assert!(resume_guard(Some(&four_on), &c5, me, false).is_err());
        let two_off = status_me(&[("bp2", rep("VERIFIED", json!({})), 1000), ("bp3", rep("ABORTED", json!({})), 1000),
            ("bp4", rep("STRANDED", json!({})), 1000), ("bp5", rep("FROZEN", json!({})), 1000)]);
        assert!(resume_guard(Some(&two_off), &c5, me, false).is_ok());
        // Unknown is never "no": relay down, a peer past creation, stale, missing, no roster.
        assert!(resume_guard(None, &c, me, false).unwrap_err().contains("did not answer"));
        let past = status_me(&[("bp2", rep("HALTED", json!({})), 1000), ("bp3", rep("ABORTED", json!({})), 1000)]);
        assert!(resume_guard(Some(&past), &c, me, false).unwrap_err().contains("bp2 was reported past chain creation"), "any-instance rule (rc.27 review)");
        let creating = status_me(&[("bp2", rep("VERIFIED", json!({"create_started": true})), 1000), ("bp3", rep("ABORTED", json!({})), 1000)]);
        assert!(resume_guard(Some(&creating), &c, me, false).unwrap_err().contains("bp2 was reported past chain creation"), "any-instance rule (rc.27 review)");
        let stale = status_me(&[("bp2", rep("ABORTED", json!({})), 999_000), ("bp3", rep("ABORTED", json!({})), 1000)]);
        assert!(resume_guard(Some(&stale), &c, me, false).unwrap_err().contains("stale"));
        let missing = status_me(&[("bp2", rep("ABORTED", json!({})), 1000)]);
        assert!(resume_guard(Some(&missing), &c, me, false).unwrap_err().contains("bp3 has no report"));
        assert!(resume_guard(Some(&missing), &c, None, false).is_err(), "without knowing who we are, our own (missing) report blocks");
        assert!(resume_guard(Some(&one_aborted), &co(&[], 0), me, false).unwrap_err().contains("no roster"), "review #8");
    }

    /// Review #2: the signed-abort path trusted omission. Every member must have SOME report for the event (stale
    /// accepted: the abort is the authority); missing or replaced = unknown; the relay's per-event high-water
    /// mark keeps a past-creation member past creation even after its entry was replaced.
    #[test]
    fn signed_abort_needs_every_member_accounted_for_and_honours_the_high_water_mark() {
        let me = Some(("bp1", "i"));
        let c = co(&["bp1", "bp2", "bp3", "bp4"], 3);
        let three_missing = status_me(&[("bp2", rep("VERIFIED", json!({})), 1000)]);
        assert!(resume_guard(Some(&three_missing), &c, me, true).unwrap_err().contains("no report"));
        assert!(resume_guard(None, &c, me, true).unwrap_err().contains("did not answer"));
        let all = status_me(&[("bp2", rep("VERIFIED", json!({})), 1000), ("bp3", rep("FROZEN", json!({})), 999_000),
            ("bp4", rep("VERIFIED", json!({})), 1000)]);
        assert!(resume_guard(Some(&all), &c, me, true).is_ok(), "stale accepted under a signed abort; quorum rule not applied");
        let mut replaced = all.clone();
        replaced["networks"][0]["producers"][2]["event_max"] = json!({"e1": {"past_create": true, "state": "IGNITED"}, "e0": {"past_create": false}});
        assert!(resume_guard(Some(&replaced), &c, me, true).unwrap_err().contains("high-water mark"));
        assert!(resume_guard(Some(&replaced), &c, me, false).unwrap_err().contains("high-water mark"));
        // A mark for another event does not count.
        replaced["networks"][0]["producers"][2]["event_max"] = json!({"e0": {"past_create": true, "state": "LIVE"}});
        assert!(resume_guard(Some(&replaced), &c, me, true).is_ok());
        // The rc.23 first shape is still read.
        replaced["networks"][0]["producers"][2]["event_max"] = json!({"event_id": "e1", "past_create": true, "state": "IGNITED"});
        assert!(resume_guard(Some(&replaced), &c, me, true).is_err());
    }

    #[test]
    fn live_view_counts_a_quorum_on_one_chain_with_a_common_block_after_the_cut() {
        let c = co(&["bp1", "bp2", "bp3", "bp4"], 3);
        let t = |head: u64, first: &str| json!({"target": {"blockchain_id": "X", "head": head, "after_cut_id": first}});
        let a = "a".repeat(64);
        let st = status(&[("bp1", rep("LIVE", t(105, &a)), 1000), ("bp2", rep("LIVE", t(107, &a)), 1000),
            ("bp3", rep("IGNITED", t(103, &a)), 1000), ("bp4", rep("HALTED", t(103, &a)), 1000)]);
        let v = live_view(Some(&st), &c, 100, Some("X"), None);
        assert!(v.healthy, "{}", v.why);
        assert_eq!(v.max_head, 107);
        assert!(!live_view(Some(&st), &c, 100, Some("Y"), None).healthy, "members on another chain do not count");
        assert!(!live_view(Some(&st), &c, 100, Some("X"), Some(&"b".repeat(64))).healthy, "this node on a fork");
        // rc.24 rehearsal fix: the HALTED bp4 still runs the chain and counts toward 4; a STRANDED one does not.
        assert!(live_view(Some(&st), &co(&["bp1", "bp2", "bp3", "bp4"], 4), 100, Some("X"), None).healthy);
        let mut stranded = st.clone();
        stranded["networks"][0]["producers"][3]["beacons"][0]["report"]["ceremony"]["state"] = json!("STRANDED");
        assert!(!live_view(Some(&stranded), &co(&["bp1", "bp2", "bp3", "bp4"], 4), 100, Some("X"), None).healthy);
        assert!(!live_view(None, &c, 100, Some("X"), None).healthy);
        assert!(!live_view(Some(&st), &c, 107, Some("X"), None).healthy, "heads must be past the cut");
    }

    #[test]
    fn join_view_needs_a_live_quorum_with_our_evidence() {
        let c = co(&["bp1", "bp2", "bp3", "bp4", "bp5"], 4);
        let a = "a".repeat(64);
        let t = json!({"target": {"blockchain_id": "X", "subnet_id": "S", "head": 120, "after_cut_id": a}});
        let ours = json!({"snapshot_sha256": "aa", "fingerprints_digest": "ff"});
        let live: Vec<(&str, Value, u64)> = ["bp1", "bp2", "bp3", "bp4"].iter().map(|p| (*p, rep("LIVE", t.clone()), 1000)).collect();
        let (bid, sid, m) = join_view(Some(&status(&live)), &c, &ours).unwrap();
        assert_eq!((bid.as_str(), sid.as_deref(), m.len()), ("X", Some("S"), 4));
        assert!(join_view(Some(&status(&live[..3])), &c, &ours).unwrap_err().contains("quorum 4"));
        let other = json!({"snapshot_sha256": "bb", "fingerprints_digest": "ff"});
        assert!(join_view(Some(&status(&live)), &c, &other).unwrap_err().contains("differs"));
    }

    /// rc.24 fleet run c5 (F6): under a signed abort, the one member whose own beacon was silent resumed while
    /// every visible member stranded waiting for it. Its own report must be visible and fresh first.
    #[test]
    fn a_node_whose_own_beacon_is_not_visible_strands_instead_of_resuming() {
        let me = Some(("bp1", "i"));
        let c = co(&["bp1", "bp2", "bp3"], 3);
        let peers = [("bp2", rep("STRANDED", json!({})), 1000), ("bp3", rep("STRANDED", json!({})), 1000)];
        // Own report missing (c5): strand, on both paths.
        let without_me = status(&peers);
        assert!(resume_guard(Some(&without_me), &c, me, true).unwrap_err().contains("own beacon not visible"));
        assert!(resume_guard(Some(&without_me), &c, me, false).unwrap_err().contains("own beacon not visible"));
        // Own report stale: strand.
        let mut v = vec![("bp1", rep("ABORTED", json!({})), 999_000)]; v.extend(peers.iter().cloned());
        assert!(resume_guard(Some(&status(&v)), &c, me, true).unwrap_err().contains("own beacon not visible"));
        // Own report visible and fresh: the normal rules apply (here: signed abort, everyone accounted for).
        assert!(resume_guard(Some(&status_me(&peers)), &c, me, true).is_ok());
    }

    /// rc.24 fleet rehearsal: a stale journal (armed for another H) under the current event id is not evidence.
    #[test]
    fn a_journal_armed_for_another_h_is_not_this_events_evidence() {
        let me = Some(("bp1", "i"));
        let c = co(&["bp1", "bp2", "bp3"], 2);
        let foreign = |st: &str| { let mut r = rep(st, json!({"source_resumed": true})); r["ceremony"]["evidence"]["h"] = json!(999); r };
        let mut ours = rep("VERIFIED", json!({})); ours["ceremony"]["evidence"]["h"] = json!(100);
        let mut st = status_me(&[("bp2", foreign("ABORTED"), 1000), ("bp3", ours.clone(), 1000)]);
        // Before: bp2's stale ABORTED journal counts it out of the ceremony and the guard lets bp1 resume.
        assert!(resume_guard(Some(&st), &c, me, false).is_ok());
        assert_eq!(drop_foreign_ceremonies(&mut st, "rehearsal", 100), 1);
        assert!(st["networks"][0]["producers"][1]["beacons"][0]["report"]["ceremony"].is_null());
        assert!(st["networks"][0]["producers"][2]["beacons"][0]["report"]["ceremony"].is_object(), "a journal for this H stays");
        // After: bp2 has a report for the event but no known state: still in the ceremony, the quorum is reachable.
        assert!(resume_guard(Some(&st), &c, me, false).unwrap_err().contains("still in the ceremony"));
        // Review #5: no cut_height fallback (an inexact cut is not another event); another network is untouched.
        let mut r = rep("LIVE", json!({})); r["ceremony"]["evidence"]["h"] = json!(7);
        let mut st2 = status_me(&[("bp2", r, 1000)]);
        assert_eq!(drop_foreign_ceremonies(&mut st2, "other", 100), 0);
        assert_eq!(drop_foreign_ceremonies(&mut st2, "rehearsal", 7), 0);
        assert_eq!(drop_foreign_ceremonies(&mut st2, "rehearsal", 100), 1);
        let mut inexact = rep("VERIFIED", json!({})); inexact["ceremony"]["evidence"]["cut_height"] = json!(103);
        assert_eq!(drop_foreign_ceremonies(&mut status_me(&[("bp2", inexact, 1000)]), "rehearsal", 100), 0);
    }

    /// rc.26 review M2: the rehearsal compare allowlist digest is part of the joined evidence: a different allowed
    /// set (different tables failed) refuses; a LIVE quorum without one (older beacons/relay) refuses with a hint.
    #[test]
    fn join_view_compares_the_allowed_compare_set() {
        let c = co(&["bp1", "bp2", "bp3", "bp4", "bp5"], 4);
        let t = |dg: Option<&str>| {
            let mut x = json!({"target": {"blockchain_id": "X", "subnet_id": "S", "head": 120, "after_cut_id": "a".repeat(64)}});
            if let Some(dg) = dg { x["evidence"] = json!({"snapshot_sha256": "aa", "fingerprints_digest": "ff", "compare_allowed_digest": dg}); }
            rep("LIVE", x)
        };
        let ours = json!({"snapshot_sha256": "aa", "fingerprints_digest": "ff", "compare_allowed_digest": "d1"});
        let live = |dg: Option<&str>| status(&["bp2", "bp3", "bp4", "bp5"].iter().map(|p| (*p, t(dg), 1000)).collect::<Vec<_>>());
        assert!(join_view(Some(&live(Some("d1"))), &c, &ours).is_ok());
        assert!(join_view(Some(&live(Some("d2"))), &c, &ours).unwrap_err().contains("compare_allowed_digest"));
        assert!(join_view(Some(&live(None)), &c, &ours).unwrap_err().contains("may predate rc.26"));
    }

    /// Review #1: with quorum = N the quorum rule always passes, so a peer's past-creation state is the one fact that
    /// blocks a resume. A report for another H (our config may be the wrong one) loses permissive facts, never that.
    #[test]
    fn a_foreign_report_past_creation_still_blocks_a_resume() {
        let me = Some(("bp1", "i"));
        let c = co(&["bp1", "bp2", "bp3"], 3);
        let mut ignited = rep("IGNITED", json!({"ignition_started": true})); ignited["ceremony"]["evidence"]["h"] = json!(999);
        let mut st = status_me(&[("bp2", ignited, 1000), ("bp3", rep("ABORTED", json!({})), 1000)]);
        assert_eq!(drop_foreign_ceremonies(&mut st, "rehearsal", 100), 1);
        assert!(st["networks"][0]["producers"][1]["beacons"][0]["report"]["ceremony"].is_null());
        let e = resume_guard(Some(&st), &c, me, false).unwrap_err();
        assert!(e.contains("bp2 reports a ceremony for another H that is past chain creation"), "{e}");
        assert!(resume_guard(Some(&st), &c, me, true).is_err(), "a signed abort does not override it either");
    }

    /// rc.27 review (review blocker): an unpinned member with two instances, one LIVE for another H (kept as
    /// foreign_past_create) and one fresher ABORTED for this H: the fresher report must not hide the blocker.
    #[test]
    fn a_fresher_instance_does_not_hide_another_instances_past_creation() {
        let me = Some(("bp1", "i"));
        let c = co(&["bp1", "bp2", "bp3"], 3);
        let mut foreign_live = rep("LIVE", json!({"ignition_started": true})); foreign_live["ceremony"]["evidence"]["h"] = json!(999);
        let mut aborted = rep("ABORTED", json!({})); aborted["ceremony"]["evidence"]["h"] = json!(100);
        let mut st = json!({"networks": [{"id": "rehearsal", "producers": [
            {"name": "bp1", "beacons": [{"age_ms": 1000, "report": rep("ABORTED", json!({}))}]},
            {"name": "bp2", "beacons": [{"age_ms": 5000, "report": foreign_live}, {"age_ms": 1000, "report": aborted}]},
            {"name": "bp3", "beacons": [{"age_ms": 1000, "report": rep("ABORTED", json!({}))}]}]}]});
        assert_eq!(drop_foreign_ceremonies(&mut st, "rehearsal", 100), 1);
        let e = resume_guard(Some(&st), &c, me, true).unwrap_err();
        assert!(e.contains("bp2") && e.contains("another H"), "{e}");
        // Same H, older instance past creation, newer instance ABORTED: also blocks.
        let st2 = status_me(&[("bp2", rep("IGNITED", json!({})), 9000), ("bp3", rep("ABORTED", json!({})), 1000)]);
        let mut st2 = st2;
        st2["networks"][0]["producers"][1]["beacons"].as_array_mut().unwrap().push(json!({"age_ms": 1000, "report": rep("ABORTED", json!({}))}));
        assert!(resume_guard(Some(&st2), &c, me, true).unwrap_err().contains("bp2 was reported past chain creation"));
    }

    /// Review #2: another box reporting under this producer name (its own instance) does not stand in for this
    /// node's own silent beacon.
    #[test]
    fn another_instance_under_my_name_is_not_my_own_report() {
        let c = co(&["bp1", "bp2"], 2);
        let mut other_box = rep("ABORTED", json!({})); other_box["instance_id"] = json!("b".repeat(32));
        let st = status(&[("bp1", other_box.clone(), 1000), ("bp2", rep("ABORTED", json!({})), 1000)]);
        let me = Some(("bp1", "a".repeat(32)));
        let me = me.as_ref().map(|(p, i)| (*p, i.as_str()));
        assert!(resume_guard(Some(&st), &c, me, false).unwrap_err().contains("own beacon not visible"));
        let mut mine = other_box; mine["instance_id"] = json!("a".repeat(32));
        let st = status(&[("bp1", mine, 1000), ("bp2", rep("ABORTED", json!({})), 1000)]);
        assert!(resume_guard(Some(&st), &c, me, false).is_ok());
    }

    /// rc.24 fleet rehearsal (c3a): a HALTED peer whose validator runs the chain still counts; a split is "hard".
    #[test]
    fn halted_members_on_the_chain_count_and_splits_are_hard() {
        let c = co(&["bp1", "bp2", "bp3", "bp4"], 4);
        let t = |head: u64, first: &str| json!({"target": {"blockchain_id": "X", "head": head, "after_cut_id": first}});
        let a = "a".repeat(64);
        let st = status(&[("bp1", rep("IGNITED", t(105, &a)), 1000), ("bp2", rep("HALTED", t(104, &a)), 1000),
            ("bp3", rep("HALTED", t(103, &a)), 1000), ("bp4", rep("LIVE", t(105, &a)), 1000)]);
        let v = live_view(Some(&st), &c, 100, Some("X"), None);
        assert!(v.healthy, "{}", v.why);
        let lag = status(&[("bp1", rep("IGNITED", t(105, &a)), 1000), ("bp2", rep("IGNITED", t(100, &a)), 1000)]);
        let v = live_view(Some(&lag), &c, 100, Some("X"), None);
        assert!(!v.healthy && !v.is_hard(), "a peer at the cut is not yet a quorum, but not a split: {}", v.why);
        let v = live_view(Some(&st), &c, 100, Some("X"), Some(&"b".repeat(64)));
        assert!(!v.healthy && v.is_hard(), "{}", v.why);
    }

    /// Review #9: two groups that each reach the quorum (different chains or forks) are a split, not a decision.
    #[test]
    fn two_groups_with_a_quorum_are_never_a_decision() {
        let c = co(&["bp1", "bp2", "bp3", "bp4"], 2);
        let ours = json!({"snapshot_sha256": "aa", "fingerprints_digest": "ff"});
        let t = |bid: &str, first: &str| json!({"target": {"blockchain_id": bid, "subnet_id": "S", "head": 120, "after_cut_id": first}});
        let (a, b) = ("a".repeat(64), "b".repeat(64));
        let st = status(&[("bp1", rep("LIVE", t("X", &a)), 1000), ("bp2", rep("LIVE", t("X", &a)), 1000),
            ("bp3", rep("LIVE", t("X", &b)), 1000), ("bp4", rep("LIVE", t("X", &b)), 1000)]);
        assert!(join_view(Some(&st), &c, &ours).unwrap_err().contains("split"));
        let v = live_view(Some(&st), &c, 100, Some("X"), None);
        assert!(!v.healthy && v.why.contains("split"), "{}", v.why);
    }
}
