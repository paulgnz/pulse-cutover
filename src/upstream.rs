//! The official (#61) import pipeline, driven by the ceremony.
//!
//! MetalBlockchain/pulsevm#61 is the core team's migration path: a pinned
//! Leap nodeos replays the cut snapshot into a SHiP full-state log
//! (`tools/xpr-chainbase-export/export.sh`), `xpr_import_check` hydrates that
//! log into an Arena checkpoint (+ a manifest binding checkpoint bytes to the
//! source block id), and verification is upstream's own:
//! `xpr_19_table_compare` (wire-level nodeos-vs-Arena table comparison — the
//! gate) and `xpr_state_fingerprint` (whole-state root — the cross-node
//! golden). This module orchestrates those tools for the SNAPSHOTTED ->
//! VERIFIED stages when `ceremony.import_backend = "upstream"`, binding every
//! artifact back to the ceremony's pinned cut:
//!
//!   manifest.env INPUT_SNAPSHOT_SHA256  == the cut snapshot's sha256
//!   manifest.json source_block_id       == the pinned cut block id
//!   manifest.json checkpoint_revision   == the pinned cut height
//!
//! The fork importer plays NO verification role here (its equivalence to #61
//! was established once, by the published cross-check — see the README's
//! "Import backends"); `[upstream] fork_audit = true` can journal its
//! fingerprints as a clearly-labeled dev/audit extra, never a gate.

use std::{
    collections::BTreeMap,
    path::{
        Path,
        PathBuf,
    },
};

use serde_json::{
    Value,
    json,
};

use crate::{
    config::Upstream,
    ops::ChainOps,
    verify,
};

#[derive(Debug, Clone)]
pub struct UpstreamOutcome {
    pub export_dir: PathBuf,
    pub ship_log: PathBuf,
    /// export.sh's manifest.env, parsed (pinned source revision, input and
    /// output hashes).
    pub manifest_env: BTreeMap<String, String>,
    /// Whether the export was re-used from a completed previous attempt
    /// (crash-resume idempotency) instead of re-run.
    pub export_reused: bool,
    pub checkpoint_path: PathBuf,
    /// xpr_import_check's `<checkpoint>.manifest.json`, verbatim.
    pub checkpoint_manifest: Value,
    pub checkpoint_sha256: String,
    pub checkpoint_revision: u64,
    pub source_block_id: String,
    /// Per-table `table <name>: rows=N sha256=...` stdout of the 19-table
    /// compare (None when no compare_bin is configured).
    pub compare_stdout: Option<String>,
    pub compare_report_path: Option<PathBuf>,
    /// REHEARSAL ONLY: the tables xpr_19_table_compare reported as differing that
    /// `rehearsal_allow_compare_mismatch` allowed (empty = the compare matched outright).
    pub compare_allowed_mismatch: Vec<String>,
    /// xpr_state_fingerprint's whole-state root.
    pub state_root: Option<String>,
    /// xpr_state_fingerprint's per-table (name, sha256) lines.
    pub fingerprint_tables: Vec<(String, String)>,
}

/// Parse export.sh's `manifest.env` (KEY=value lines).
pub fn parse_manifest_env(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|l| {
            let l = l.trim();
            if l.is_empty() || l.starts_with('#') {
                return None;
            }
            l.split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect()
}

/// Find the first file named `name` at or below `dir` (export.sh may nest
/// its own --work-dir under the agent's export dir; depth-limited).
pub fn find_file(dir: &Path, name: &str) -> Option<PathBuf> {
    fn walk(dir: &Path, name: &str, depth: u32) -> Option<PathBuf> {
        if depth > 6 {
            return None;
        }
        let mut subdirs = Vec::new();
        for entry in std::fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && entry.file_name().to_string_lossy() == name {
                return Some(path);
            }
            if path.is_dir() {
                subdirs.push(path);
            }
        }
        subdirs.into_iter().find_map(|d| walk(&d, name, depth + 1))
    }
    walk(dir, name, 0)
}

fn fresh_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        std::fs::remove_dir_all(path)
            .map_err(|e| format!("clear {}: {e}", path.display()))?;
    }
    std::fs::create_dir_all(path).map_err(|e| format!("create {}: {e}", path.display()))
}

#[derive(serde::Deserialize)]
struct SidecarSeq {
    #[serde(default)]
    source_chain_id: Option<String>,
    source_block_id: String,
    global_action_sequence: u64,
    #[serde(default)]
    account_metadata: Vec<SidecarAccountSeq>,
}

#[derive(serde::Deserialize)]
struct SidecarAccountSeq {
    recv_sequence: u64,
}

/// Gate the sidecar's sequence counters before anything is imported. Hard failures: no
/// `source_chain_id` (upstream then skips its exact-coverage check), a chain id or block id
/// that is not the cut's, no per-account rows, a zero global sequence, and recv_sequences that do
/// not sum to global_action_sequence (every receipt bumps both once).
pub fn check_sidecar(path: &Path, chain_id: &str, cut_block_id: &str) -> Result<Value, String> {
    let f = std::fs::File::open(path).map_err(|e| format!("open sidecar {}: {e}", path.display()))?;
    let sc: SidecarSeq = serde_json::from_reader(std::io::BufReader::new(f))
        .map_err(|e| format!("sidecar {} is not valid JSON of the expected shape: {e}", path.display()))?;
    let src = sc.source_chain_id.as_deref().ok_or(
        "sidecar has no source_chain_id: it predates full-state export, so per-account action \
         sequence counters would be missing or unchecked (history paging would reset at the cut). \
         Re-export with the current deferred-sidecar plugin",
    )?;
    if !chain_id.is_empty() && !src.eq_ignore_ascii_case(chain_id) {
        return Err(format!("sidecar source_chain_id {src} is not this chain ({chain_id})"));
    }
    if !sc.source_block_id.eq_ignore_ascii_case(cut_block_id) {
        return Err(format!(
            "sidecar source_block_id {} is not the cut block {cut_block_id}",
            sc.source_block_id
        ));
    }
    if sc.account_metadata.is_empty() {
        return Err("sidecar has no account_metadata rows: every account's recv/auth/code/abi \
                    sequence would restart at 0"
            .into());
    }
    if sc.global_action_sequence == 0 {
        return Err("sidecar global_action_sequence is 0: the global action sequence would restart".into());
    }
    let sum_recv: u128 = sc.account_metadata.iter().map(|a| a.recv_sequence as u128).sum();
    // Every receipt bumps the global sequence once and exactly one receiver's recv_sequence once, so on a
    // chain that started at genesis these are equal. Proven exact on a real XPR testnet export (2026-10-01:
    // 1,983,236,481 both): a mismatch means counters were lost or altered, and history paging would break.
    if sum_recv != sc.global_action_sequence as u128 {
        return Err(format!(
            "sidecar sequence counters are inconsistent: sum of recv_sequence {sum_recv} != global_action_sequence {} \
             (counters lost or altered in export; history paging would break at the cut)",
            sc.global_action_sequence
        ));
    }
    Ok(json!({
        "accounts": sc.account_metadata.len(),
        "global_action_sequence": sc.global_action_sequence,
        "sum_recv_sequence": sum_recv.to_string(),
        "recv_sum_matches_global": sum_recv == sc.global_action_sequence as u128,
    }))
}

/// Drive the whole #61 pipeline over the cut snapshot. `progress` receives
/// journal-ready evidence blobs between steps; Err is a verification failure
/// (the machine aborts with it). Idempotent on resume: a completed export
/// (manifest.env + chain_state_history.log present) is re-used; import,
/// compare and fingerprint re-run into fresh arena directories every time.
pub fn run_pipeline<O: ChainOps>(
    up: &Upstream,
    ops: &O,
    snapshot: &Path,
    snapshot_sha256: &str,
    cut_height: u64,
    cut_block_id: &str,
    chain_id: &str,
    mut progress: impl FnMut(Value),
) -> Result<UpstreamOutcome, String> {
    std::fs::create_dir_all(&up.work_dir)
        .map_err(|e| format!("create upstream work_dir {}: {e}", up.work_dir.display()))?;
    let export_dir = up.work_dir.join(format!("export-{cut_height}"));

    // ---- 1. export.sh: pinned Leap replays the .bin into a SHiP log ----
    let complete = find_file(&export_dir, "chain_state_history.log")
        .zip(find_file(&export_dir, "manifest.env"));
    let (ship_log, manifest_path, export_reused) = match complete {
        Some((log, env)) => {
            progress(json!({
                "upstream_export": "reused completed export from a previous attempt",
                "export_dir": export_dir.display().to_string(),
            }));
            (log, env, true)
        }
        None => {
            fresh_dir(&export_dir)?;
            let cmd = up
                .export_cmd
                .replace("{snapshot}", &snapshot.display().to_string())
                .replace("{export_dir}", &export_dir.display().to_string());
            progress(json!({"upstream_export_cmd": cmd}));
            let started = ops.now_ms();
            let out = ops
                .run_long(&cmd)
                .map_err(|e| format!("upstream export_cmd failed: {e}"))?;
            let log = find_file(&export_dir, "chain_state_history.log").ok_or(
                "export_cmd succeeded but no chain_state_history.log found under the export dir",
            )?;
            let env = find_file(&export_dir, "manifest.env")
                .ok_or("export_cmd succeeded but no manifest.env found under the export dir")?;
            progress(json!({
                "upstream_export": "ok",
                "output_tail": out.chars().rev().take(400).collect::<String>().chars().rev().collect::<String>(),
                "ship_log": log.display().to_string(),
                "export_wall_ms": ops.now_ms().saturating_sub(started),
            }));
            (log, env, false)
        }
    };
    // The deferred/input-transaction sidecar carries what SHiP cannot: the
    // input-transaction dedupe set (replay protection across the cut) and
    // deferred transactions. The importer restores it atomically with the
    // SHiP rows and the compare gate checks it, so it is mandatory.
    let sidecar = find_file(&export_dir, "deferred-transactions.json").ok_or(
        "export produced no deferred-transactions.json sidecar — run export.sh with \
         --deferred-sidecar /out/deferred-transactions.json on a nodeos build with the \
         deferred-sidecar plugin; without it the migrated chain has no transaction dedupe \
         set (post-cut replay protection) and deferred transactions are lost",
    )?;
    progress(json!({"upstream_sidecar": sidecar.display().to_string()}));
    // Action sequence counters (global_action_sequence, per-account recv/auth/code/abi) come ONLY
    // from the sidecar: SHiP carries none of them. A missing or partial sidecar silently restarts
    // them, which breaks every client that pages history by sequence (upstream issue #101).
    let seq = check_sidecar(&sidecar, chain_id, cut_block_id)?;
    progress(json!({"upstream_sidecar_sequences": seq}));
    let manifest_env = parse_manifest_env(
        &std::fs::read_to_string(&manifest_path)
            .map_err(|e| format!("read {}: {e}", manifest_path.display()))?,
    );
    // Gate: the export consumed exactly the ceremony's cut snapshot.
    match manifest_env.get("INPUT_SNAPSHOT_SHA256") {
        Some(sha) if sha.eq_ignore_ascii_case(snapshot_sha256) => {}
        Some(sha) => {
            return Err(format!(
                "export manifest INPUT_SNAPSHOT_SHA256 {sha} != cut snapshot sha256 \
                 {snapshot_sha256} — the export did not consume this ceremony's cut"
            ));
        }
        None => return Err("export manifest.env has no INPUT_SNAPSHOT_SHA256".into()),
    }
    progress(json!({"upstream_export_manifest": manifest_env}));

    // ---- 2. xpr_import_check: SHiP log -> Arena checkpoint + manifest ----
    let checkpoint_path = up.work_dir.join(format!("checkpoint-{cut_height}.bin"));
    let arena_import = up.work_dir.join(format!("arena-import-{cut_height}"));
    fresh_dir(&arena_import)?;
    let _ = std::fs::remove_file(&checkpoint_path);
    let _ = std::fs::remove_file(manifest_json_path(&checkpoint_path));
    let started = ops.now_ms();
    let import_out = ops
        .run_long(&format!(
            "'{}' '{}' '{}' '{}' '{}'",
            up.import_bin.display(),
            ship_log.display(),
            arena_import.display(),
            checkpoint_path.display(),
            sidecar.display()
        ))
        .map_err(|e| format!("xpr_import_check failed: {e}"))?;
    let checkpoint_manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(manifest_json_path(&checkpoint_path))
            .map_err(|e| format!("read checkpoint manifest: {e}"))?,
    )
    .map_err(|e| format!("parse checkpoint manifest: {e}"))?;
    let checkpoint_sha256 = checkpoint_manifest
        .get("checkpoint_sha256")
        .and_then(|v| v.as_str())
        .ok_or("checkpoint manifest missing checkpoint_sha256")?
        .to_string();
    let checkpoint_revision = checkpoint_manifest
        .get("checkpoint_revision")
        .and_then(|v| v.as_u64())
        .ok_or("checkpoint manifest missing checkpoint_revision")?;
    let source_block_id = checkpoint_manifest
        .get("source_block_id")
        .and_then(|v| v.as_str())
        .ok_or("checkpoint manifest missing source_block_id")?
        .to_string();
    progress(json!({
        "upstream_import": {
            "checkpoint": checkpoint_path.display().to_string(),
            "checkpoint_sha256": checkpoint_sha256,
            "checkpoint_revision": checkpoint_revision,
            "source_block_id": source_block_id,
            "summary_tail": import_out.lines().last().unwrap_or(""),
            "import_wall_ms": ops.now_ms().saturating_sub(started),
        }
    }));
    // Gates: the checkpoint is OF the pinned cut.
    if checkpoint_revision != cut_height {
        return Err(format!(
            "upstream checkpoint revision {checkpoint_revision} != pinned cut height {cut_height}"
        ));
    }
    if !source_block_id.eq_ignore_ascii_case(cut_block_id) {
        return Err(format!(
            "upstream checkpoint source_block_id {source_block_id} != pinned cut block id \
             {cut_block_id}"
        ));
    }

    // ---- 3. xpr_19_table_compare: THE verification gate (when staged) ----
    let (compare_stdout, compare_report_path, compare_allowed_mismatch) = match &up.compare_bin {
        Some(bin) => {
            let arena_cmp = up.work_dir.join(format!("arena-compare-{cut_height}"));
            fresh_dir(&arena_cmp)?;
            let report = up.work_dir.join(format!("compare-report-{cut_height}.json"));
            let _ = std::fs::remove_file(&report);
            let started = ops.now_ms();
            // A non-zero exit (any nodeos-vs-Arena table difference) is a
            // verification FAILURE, unless (rehearsal only) every failing table it names is on
            // the explicit allowlist.
            let res = ops.run_long(&format!(
                "'{}' '{}' '{}' '{}' '{}' '{}' '{}'",
                bin.display(),
                ship_log.display(),
                checkpoint_path.display(),
                arena_cmp.display(),
                chain_id,
                sidecar.display(),
                report.display()
            ));
            let wall = ops.now_ms().saturating_sub(started);
            match res {
                Ok(out) => {
                    progress(json!({
                        "upstream_19_table_compare": {
                            "result": "MATCH",
                            "tables": out.lines().collect::<Vec<_>>(),
                            "report": report.display().to_string(),
                            "compare_wall_ms": wall,
                        }
                    }));
                    (Some(out), Some(report), vec![])
                }
                Err(e) => {
                    let failing = compare_failing_tables(&e);
                    let allow = &up.rehearsal_allow_compare_mismatch;
                    let not_allowed: Vec<&String> = failing.iter().filter(|t| !allow.contains(t)).collect();
                    if allow.is_empty() || failing.is_empty() || !not_allowed.is_empty() {
                        progress(json!({
                            "upstream_19_table_compare": {
                                "result": "MISMATCH",
                                "failing_tables": failing,
                                "not_allowed": not_allowed,
                                "rehearsal_allow_compare_mismatch": allow,
                                "output": e,
                                "report": report.display().to_string(),
                                "compare_wall_ms": wall,
                            }
                        }));
                        let why = if allow.is_empty() {
                            String::new()
                        } else if failing.is_empty() {
                            " (the failure names no table, so the rehearsal allowlist cannot apply)".to_string()
                        } else {
                            format!(
                                " (not on the rehearsal allowlist: {})",
                                not_allowed.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                            )
                        };
                        return Err(format!(
                            "xpr_19_table_compare FAILED — the nodeos SHiP snapshot and the Arena \
                             re-serialization disagree (verification failure){why}: {e}"
                        ));
                    }
                    progress(json!({
                        "upstream_19_table_compare": {
                            "result": "MISMATCH ALLOWED BY REHEARSAL OVERRIDE",
                            "REHEARSAL_ONLY": "upstream.rehearsal_allow_compare_mismatch is set: this is NOT a valid verification for a real cut",
                            "failing_tables": failing,
                            "rehearsal_allow_compare_mismatch": allow,
                            "output": e,
                            "report": report.display().to_string(),
                            "compare_wall_ms": wall,
                        }
                    }));
                    (Some(e), Some(report), failing)
                }
            }
        }
        None => {
            progress(json!({
                "upstream_19_table_compare": "skipped (no compare_bin configured)"
            }));
            (None, None, vec![])
        }
    };

    // ---- 4. xpr_state_fingerprint: whole-state root (cross-node golden) ----
    let arena_fp = up.work_dir.join(format!("arena-fingerprint-{cut_height}"));
    fresh_dir(&arena_fp)?;
    let fp_out = ops
        .run_long(&format!(
            "'{}' '{}' '{}'",
            up.fingerprint_bin.display(),
            checkpoint_path.display(),
            arena_fp.display()
        ))
        .map_err(|e| format!("xpr_state_fingerprint failed: {e}"))?;
    let (state_root, fingerprint_tables) = verify::parse_upstream_report(&fp_out);
    progress(json!({
        "upstream_state_fingerprint": {
            "state_root": state_root,
            "tables": fingerprint_tables
                .iter()
                .map(|(n, s)| format!("{n} {s}"))
                .collect::<Vec<_>>(),
        }
    }));
    if let Some(golden) = &up.golden_state_root {
        match &state_root {
            Some(root) if root.eq_ignore_ascii_case(golden) => {
                progress(json!({"upstream_golden_state_root": "MATCH"}));
            }
            Some(root) => {
                return Err(format!(
                    "upstream state_root {root} != published golden {golden}"
                ));
            }
            None => {
                return Err("golden_state_root configured but xpr_state_fingerprint printed \
                            no state_root"
                    .into());
            }
        }
    }

    Ok(UpstreamOutcome {
        export_dir,
        ship_log,
        manifest_env,
        export_reused,
        checkpoint_path,
        checkpoint_manifest,
        checkpoint_sha256,
        checkpoint_revision,
        source_block_id,
        compare_stdout,
        compare_report_path,
        compare_allowed_mismatch,
        state_root,
        fingerprint_tables,
    })
}

fn manifest_json_path(checkpoint: &Path) -> PathBuf {
    PathBuf::from(format!("{}.manifest.json", checkpoint.display()))
}

/// Failing table names in `xpr_19_table_compare` output (stdout and stderr, as the failed
/// command reports them). Every table line is `table <name>: ...`; a matching table prints
/// `rows=N sha256=...`, anything else (`nodeos=... arena=...`, `first differing row ...`) marks
/// the table as failing. Exact names, de-duplicated, in output order.
pub fn compare_failing_tables(output: &str) -> Vec<String> {
    let re = regex::Regex::new(r"(?m)(?:^|[\s:])table ([a-z0-9_]+): (.*)$").expect("static regex");
    let mut failing: Vec<String> = vec![];
    for c in re.captures_iter(output) {
        let (name, rest) = (&c[1], c[2].trim_start());
        if rest.starts_with("rows=") {
            continue;
        }
        if !failing.iter().any(|f| f == name) {
            failing.push(name.to_string());
        }
    }
    failing
}

/// What still stands between an upstream ignition and a SAME-CHAIN-ID MAINNET cutover. Kept as
/// data: an upstream ignite is refused for XPR mainnet while this is non-empty, and a rehearsal
/// ignite journals it as warnings every time.
pub fn ignite_pending_reasons() -> Vec<&'static str> {
    vec![
        "same-chain-id cutover: the signing chain_id must be pinned to the source \
         chain_id and enforced at node startup (PulseVM v1.0.0 signs with the blockchain id \
         metalgo passes at initialize and only warns on a mismatch)",
        "same-chain-id cutover: PulseVM must enforce TAPOS (block-summary ring seeded \
         at the cut, checked at admission and block application) so transactions \
         signed on a leftover source chain cannot replay onto the target",
    ]
}

// ------------------------------------------------- boot from the checkpoint --

/// The anchor block the migrated chain starts from: the FULL packed `signed_block` at the cut,
/// every transaction receipt included. Upstream's `xpr_attach_source_block` refuses a boundary
/// block with transactions and a block emptied of them fails the controller's transaction_mroot
/// check; the controller itself accepts the complete block (proven on the stage-2 rig, PulseVM
/// v1.0.0). Built here from nodeos `get_block` JSON and bound to the cut by its computed id.
#[derive(Debug, Clone)]
pub struct PackedBlock {
    pub bytes: Vec<u8>,
    /// sha256(header) with the first 4 bytes replaced by the block number (Antelope block id).
    pub id: String,
    pub block_num: u32,
    pub receipts: usize,
}

fn varuint32(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut b = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
}

fn hex_field(v: &Value, k: &str) -> Result<Vec<u8>, String> {
    let s = v.get(k).and_then(|x| x.as_str()).ok_or_else(|| format!("block JSON missing {k}"))?;
    hex::decode(s).map_err(|e| format!("block {k} is not hex: {e}"))
}

fn u64_field(v: &Value, k: &str) -> Result<u64, String> {
    let f = v.get(k).ok_or_else(|| format!("block JSON missing {k}"))?;
    f.as_u64().or_else(|| f.as_str().and_then(|s| s.parse().ok())).ok_or_else(|| format!("block {k} is not a number: {f}"))
}

fn bytes_field(out: &mut Vec<u8>, b: &[u8]) {
    varuint32(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// Antelope `name` -> u64 (the standard 5-bit packing, 13th char 4 bits).
pub fn name_to_u64(s: &str) -> Result<u64, String> {
    if s.len() > 13 {
        return Err(format!("name {s:?} is longer than 13 characters"));
    }
    let sym = |c: u8| -> Result<u64, String> {
        match c {
            b'a'..=b'z' => Ok((c - b'a') as u64 + 6),
            b'1'..=b'5' => Ok((c - b'1') as u64 + 1),
            b'.' => Ok(0),
            _ => Err(format!("name {s:?} has an invalid character {:?}", c as char)),
        }
    };
    let mut v = 0u64;
    for (i, c) in s.bytes().enumerate() {
        let x = sym(c)?;
        if i < 12 {
            v |= (x & 0x1f) << (64 - 5 * (i + 1));
        } else {
            if x > 0x0f {
                return Err(format!("name {s:?}: 13th character must be in [.1-5a-j]"));
            }
            v |= x & 0x0f;
        }
    }
    Ok(v)
}

/// `YYYY-MM-DDTHH:MM:SS[.mmm]` (UTC) -> Antelope block_timestamp slot (500 ms since 2000-01-01).
pub fn block_timestamp_slot(ts: &str) -> Result<u32, String> {
    let bad = || format!("block timestamp {ts:?} is not YYYY-MM-DDTHH:MM:SS[.mmm]");
    let ts = ts.trim_end_matches('Z');
    let (date, time) = ts.split_once('T').ok_or_else(bad)?;
    let d: Vec<i64> = date.split('-').map(|x| x.parse().map_err(|_| bad())).collect::<Result<_, _>>()?;
    let (hms, frac) = time.split_once('.').unwrap_or((time, "0"));
    let t: Vec<i64> = hms.split(':').map(|x| x.parse().map_err(|_| bad())).collect::<Result<_, _>>()?;
    if d.len() != 3 || t.len() != 3 || frac.is_empty() || frac.len() > 3 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let ms_frac: i64 = format!("{frac:0<3}").parse().map_err(|_| bad())?;
    // days_from_civil (H. Hinnant), proleptic Gregorian.
    let (y, m, day) = (d[0] - if d[1] <= 2 { 1 } else { 0 }, d[1], d[2]);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let ms = ((days * 24 + t[0]) * 60 + t[1]) * 60_000 + t[2] * 1000 + ms_frac;
    let since = ms - 946_684_800_000;
    if since < 0 || since % 500 != 0 {
        return Err(format!("block timestamp {ts:?} is not on a 500 ms slot after 2000-01-01"));
    }
    u32::try_from(since / 500).map_err(|_| bad())
}

/// `SIG_K1_…` / `SIG_R1_…` -> variant index + 65 bytes, checksum verified
/// (ripemd160(sig ‖ "K1"/"R1")[..4]).
pub fn pack_signature(sig: &str) -> Result<Vec<u8>, String> {
    use ripemd::Digest as _;
    let (idx, kind, b58) = if let Some(r) = sig.strip_prefix("SIG_K1_") {
        (0u8, "K1", r)
    } else if let Some(r) = sig.strip_prefix("SIG_R1_") {
        (1u8, "R1", r)
    } else {
        return Err(format!("unsupported signature {sig:?} (only SIG_K1_ / SIG_R1_)"));
    };
    let raw = bs58::decode(b58).into_vec().map_err(|e| format!("signature base58: {e}"))?;
    if raw.len() != 69 {
        return Err(format!("signature decodes to {} bytes, expected 69", raw.len()));
    }
    let mut h = ripemd::Ripemd160::new();
    h.update(&raw[..65]);
    h.update(kind.as_bytes());
    if h.finalize()[..4] != raw[65..] {
        return Err(format!("signature {sig:.24}… has a bad checksum"));
    }
    let mut out = vec![idx];
    out.extend_from_slice(&raw[..65]);
    Ok(out)
}

/// `[[type, "hex"], …]` (header/block extensions) -> vector<pair<uint16, bytes>>.
fn pack_extensions(out: &mut Vec<u8>, v: Option<&Value>, what: &str) -> Result<(), String> {
    let exts = match v {
        None | Some(Value::Null) => vec![],
        Some(Value::Array(a)) => a.clone(),
        Some(other) => return Err(format!("block {what} is not an array: {other}")),
    };
    varuint32(out, exts.len() as u64);
    for e in exts {
        let (t, data) = match &e {
            Value::Array(p) if p.len() == 2 => (p[0].as_u64(), p[1].as_str()),
            Value::Object(o) => (o.get("type").and_then(|x| x.as_u64()), o.get("data").and_then(|x| x.as_str())),
            _ => (None, None),
        };
        let (t, data) = t.zip(data).ok_or_else(|| format!("block {what} entry {e} is not [type, hex]"))?;
        out.extend_from_slice(&u16::try_from(t).map_err(|_| format!("{what} type {t} > u16"))?.to_le_bytes());
        bytes_field(out, &hex::decode(data).map_err(|e| format!("{what} data: {e}"))?);
    }
    Ok(())
}

/// Pack a nodeos `get_block` JSON (Leap 5 legacy block) into its wire `signed_block`, and check it
/// is self-consistent: every packed transaction id must equal sha256(packed_trx) (uncompressed).
/// The caller binds the result to the cut by comparing `id`.
pub fn pack_signed_block(block: &Value) -> Result<PackedBlock, String> {
    use sha2::Digest as _;
    let mut out = Vec::new();
    let ts = block.get("timestamp").and_then(|x| x.as_str()).ok_or("block JSON missing timestamp")?;
    out.extend_from_slice(&block_timestamp_slot(ts)?.to_le_bytes());
    let producer = block.get("producer").and_then(|x| x.as_str()).ok_or("block JSON missing producer")?;
    out.extend_from_slice(&name_to_u64(producer)?.to_le_bytes());
    out.extend_from_slice(&u16::try_from(u64_field(block, "confirmed")?).map_err(|_| "confirmed > u16")?.to_le_bytes());
    let previous = hex_field(block, "previous")?;
    for (k, b) in [("previous", &previous), ("transaction_mroot", &hex_field(block, "transaction_mroot")?), ("action_mroot", &hex_field(block, "action_mroot")?)] {
        if b.len() != 32 {
            return Err(format!("block {k} is {} bytes, expected 32", b.len()));
        }
        out.extend_from_slice(b);
    }
    out.extend_from_slice(&u32::try_from(u64_field(block, "schedule_version")?).map_err(|_| "schedule_version > u32")?.to_le_bytes());
    match block.get("new_producers") {
        None | Some(Value::Null) => out.push(0),
        Some(_) => {
            return Err("the cut block carries a producer schedule change (new_producers): not supported \
                        as a migration anchor — choose a cut without a schedule change"
                .into())
        }
    }
    pack_extensions(&mut out, block.get("header_extensions"), "header_extensions")?;
    let header_len = out.len();
    let sig = block.get("producer_signature").and_then(|x| x.as_str()).ok_or("block JSON missing producer_signature")?;
    out.extend_from_slice(&pack_signature(sig)?);
    let txs = block.get("transactions").and_then(|x| x.as_array()).ok_or("block JSON missing transactions[] (need the full block with receipts)")?;
    varuint32(&mut out, txs.len() as u64);
    for (i, t) in txs.iter().enumerate() {
        let status = match t.get("status").and_then(|x| x.as_str()) {
            Some("executed") => 0u8,
            Some("soft_fail") => 1,
            Some("hard_fail") => 2,
            Some("delayed") => 3,
            Some("expired") => 4,
            other => return Err(format!("receipt {i}: unknown status {other:?}")),
        };
        out.push(status);
        out.extend_from_slice(&u32::try_from(u64_field(t, "cpu_usage_us")?).map_err(|_| "cpu_usage_us > u32")?.to_le_bytes());
        varuint32(&mut out, u64_field(t, "net_usage_words")?);
        match t.get("trx") {
            Some(Value::String(id)) => {
                let id = hex::decode(id).map_err(|e| format!("receipt {i}: trx id: {e}"))?;
                if id.len() != 32 {
                    return Err(format!("receipt {i}: trx id is {} bytes", id.len()));
                }
                out.push(0);
                out.extend_from_slice(&id);
            }
            Some(trx @ Value::Object(_)) => {
                out.push(1);
                let sigs = trx.get("signatures").and_then(|x| x.as_array()).ok_or_else(|| format!("receipt {i}: no signatures"))?;
                varuint32(&mut out, sigs.len() as u64);
                for s in sigs {
                    out.extend_from_slice(&pack_signature(s.as_str().ok_or_else(|| format!("receipt {i}: signature not a string"))?)?);
                }
                let comp = match trx.get("compression").and_then(|x| x.as_str()) {
                    Some("none") | None => 0u8,
                    Some("zlib") => 1,
                    Some(o) => return Err(format!("receipt {i}: unknown compression {o:?}")),
                };
                out.push(comp);
                let cfd = hex::decode(trx.get("packed_context_free_data").and_then(|x| x.as_str()).unwrap_or(""))
                    .map_err(|e| format!("receipt {i}: packed_context_free_data: {e}"))?;
                bytes_field(&mut out, &cfd);
                let packed = hex_field(trx, "packed_trx").map_err(|e| format!("receipt {i}: {e}"))?;
                if comp == 0 {
                    if let Some(want) = trx.get("id").and_then(|x| x.as_str()) {
                        let got = hex::encode(sha2::Sha256::digest(&packed));
                        if !got.eq_ignore_ascii_case(want) {
                            return Err(format!("receipt {i}: sha256(packed_trx) {got} != transaction id {want}"));
                        }
                    }
                }
                bytes_field(&mut out, &packed);
            }
            other => return Err(format!("receipt {i}: unsupported trx {other:?}")),
        }
    }
    pack_extensions(&mut out, block.get("block_extensions"), "block_extensions")?;
    let prev_num = u32::from_be_bytes(previous[..4].try_into().expect("32 bytes"));
    let block_num = prev_num.checked_add(1).ok_or("previous block number overflows")?;
    let mut digest: [u8; 32] = sha2::Sha256::digest(&out[..header_len]).into();
    digest[..4].copy_from_slice(&block_num.to_be_bytes());
    Ok(PackedBlock { bytes: out, id: hex::encode(digest), block_num, receipts: txs.len() })
}

/// The three files a PulseVM v1.0.0 node boots a migrated chain from, written into the
/// upstream work dir and journaled by hash.
#[derive(Debug, Clone)]
pub struct BootArtifacts {
    pub manifest: PathBuf,
    pub manifest_sha256: String,
    pub genesis: PathBuf,
    pub genesis_sha256: String,
    pub chain_config: PathBuf,
    pub chain_config_sha256: String,
    pub source_block_bytes: usize,
    pub source_block_receipts: usize,
}

/// Deterministic artifact paths for a cut (a resumed agent re-derives them).
pub fn boot_paths(up: &Upstream, cut_height: u64) -> (PathBuf, PathBuf, PathBuf) {
    (
        up.work_dir.join(format!("boot-{cut_height}.manifest.json")),
        up.work_dir.join(format!("migration-genesis-{cut_height}.json")),
        up.work_dir.join(format!("chain-config-{cut_height}.json")),
    )
}

fn read_json_object(path: &Path, what: &str) -> Result<serde_json::Map<String, Value>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {what} {}: {e}", path.display()))?;
    match serde_json::from_str::<Value>(&text).map_err(|e| format!("{what} {} is not JSON: {e}", path.display()))? {
        Value::Object(m) => Ok(m),
        _ => Err(format!("{what} {} is not a JSON object", path.display())),
    }
}

fn write_json(path: &Path, v: &Value) -> Result<String, String> {
    use sha2::Digest as _;
    let text = serde_json::to_string_pretty(v).expect("json") + "\n";
    std::fs::write(path, &text).map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(hex::encode(sha2::Sha256::digest(text.as_bytes())))
}

/// Build the boot manifest (checkpoint manifest + the FULL source cut block), the migration
/// genesis (base + `migration_checkpoint_sha256`) and the chain config (base +
/// `migration_checkpoint` + `migration_manifest`). `source_block` is nodeos `get_block` JSON
/// for the cut; its packed id must equal `cut_block_id`.
pub fn build_boot_artifacts(
    up: &Upstream,
    outcome: &UpstreamOutcome,
    source_block: &Value,
    cut_height: u64,
    cut_block_id: &str,
    chain_id: &str,
) -> Result<BootArtifacts, String> {
    let packed = pack_signed_block(source_block).map_err(|e| format!("cannot pack the source cut block: {e}"))?;
    if packed.block_num as u64 != cut_height || !packed.id.eq_ignore_ascii_case(cut_block_id) {
        return Err(format!(
            "the packed source block is not the cut: computed id {} (block {}) != cut block id {cut_block_id} \
             (block {cut_height}) — wrong block, or the JSON is not a faithful get_block",
            packed.id, packed.block_num
        ));
    }
    let mut manifest = outcome.checkpoint_manifest.clone();
    let m = manifest.as_object_mut().ok_or("checkpoint manifest is not a JSON object")?;
    if let Some(src) = m.get("source_chain_id").and_then(|v| v.as_str()) {
        if !chain_id.is_empty() && !src.eq_ignore_ascii_case(chain_id) {
            return Err(format!("checkpoint manifest source_chain_id {src} is not this chain ({chain_id})"));
        }
    }
    m.insert("source_block".into(), json!(hex::encode(&packed.bytes)));
    let (manifest_path, genesis_path, config_path) = boot_paths(up, cut_height);
    let manifest_sha256 = write_json(&manifest_path, &manifest)?;

    let base = up.genesis_base.as_ref().ok_or("upstream.genesis_base is not set")?;
    let mut genesis = read_json_object(base, "upstream.genesis_base")?;
    for k in ["initial_timestamp", "initial_key", "initial_configuration"] {
        if !genesis.contains_key(k) {
            return Err(format!("upstream.genesis_base {} has no {k}", base.display()));
        }
    }
    genesis.insert("migration_checkpoint_sha256".into(), json!(outcome.checkpoint_sha256));
    let genesis_sha256 = write_json(&genesis_path, &Value::Object(genesis))?;

    let mut cc = match &up.chain_config_base {
        Some(p) => read_json_object(p, "upstream.chain_config_base")?,
        None => serde_json::Map::new(),
    };
    cc.insert("migration_checkpoint".into(), json!(outcome.checkpoint_path.display().to_string()));
    cc.insert("migration_manifest".into(), json!(manifest_path.display().to_string()));
    let chain_config_sha256 = write_json(&config_path, &Value::Object(cc))?;
    Ok(BootArtifacts {
        manifest: manifest_path,
        manifest_sha256,
        genesis: genesis_path,
        genesis_sha256,
        chain_config: config_path,
        chain_config_sha256,
        source_block_bytes: packed.bytes.len(),
        source_block_receipts: packed.receipts,
    })
}

/// `BLOCKCHAIN_ID=<id>` (and optionally `SUBNET_ID=<id>`) from create_chain_cmd output; the last
/// occurrence wins. Ids are Metal CB58 strings.
pub fn parse_create_chain_output(out: &str) -> Result<(String, Option<String>), String> {
    let re = regex::Regex::new(r"(?m)^\s*(BLOCKCHAIN_ID|SUBNET_ID)=([1-9A-HJ-NP-Za-km-z]{32,64})\s*$").expect("static regex");
    let (mut bid, mut sid) = (None, None);
    for c in re.captures_iter(out) {
        match &c[1] {
            "BLOCKCHAIN_ID" => bid = Some(c[2].to_string()),
            _ => sid = Some(c[2].to_string()),
        }
    }
    let bid = bid.ok_or("create_chain_cmd printed no BLOCKCHAIN_ID=<cb58 id> line")?;
    Ok((bid, sid))
}

// --------------------------------------------------------------- tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_env_parses_key_values() {
        let env = parse_manifest_env(
            "XPR_CORE_REVISION=d133c6413ce8ce2e96096a0513ec25b4a8dbe837\n\
             INPUT_SNAPSHOT_SHA256=d2e3e6071edfa93ec4777aeb817ff63c76ae40a76ec6e1ca755f252c54b026d7\n\
             CHAIN_STATE_HISTORY_SHA256=579e9d21\n\
             # comment\n\
             CHAIN_STATE_HISTORY_LOG=chain_state_history.log\n",
        );
        assert_eq!(env["XPR_CORE_REVISION"], "d133c6413ce8ce2e96096a0513ec25b4a8dbe837");
        assert_eq!(
            env["INPUT_SNAPSHOT_SHA256"],
            "d2e3e6071edfa93ec4777aeb817ff63c76ae40a76ec6e1ca755f252c54b026d7"
        );
        assert_eq!(env.len(), 4);
    }

    #[test]
    fn find_file_walks_nested_export_layout() {
        // export.sh nests its own --work-dir under the agent's export dir
        // (docker mounts make this the natural shape).
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("work/state-history");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("chain_state_history.log"), b"x").unwrap();
        std::fs::write(dir.path().join("work").join("manifest.env"), b"A=1").unwrap();
        assert_eq!(
            find_file(dir.path(), "chain_state_history.log").unwrap(),
            nested.join("chain_state_history.log")
        );
        assert!(find_file(dir.path(), "manifest.env").is_some());
        assert!(find_file(dir.path(), "nope.log").is_none());
    }

    #[test]
    fn ignite_pending_reasons_name_only_the_mainnet_gaps() {
        let reasons = ignite_pending_reasons().join(" ");
        assert!(reasons.contains("chain_id") && reasons.contains("TAPOS"));
        assert_eq!(ignite_pending_reasons().len(), 2);
    }

    /// The stage-2 rig's real cut block (XPR testnet 408461570, 6 receipts) and the anchor the
    /// PulseVM v1.0.0 controller accepted, built there by a one-off script: packing in Rust must
    /// reproduce it byte for byte, and its computed id must be the cut block id.
    #[test]
    fn full_source_block_reproduces_the_rig_anchor_exactly() {
        let block: Value = serde_json::from_str(include_str!("../tests/fixtures/xpr-testnet-block-408461570.json")).unwrap();
        let want: Value = serde_json::from_str(include_str!("../tests/fixtures/boot-manifest-408461570.json")).unwrap();
        let p = pack_signed_block(&block).unwrap();
        assert_eq!(p.id, "1858a10265a2714f8d77797b02b164ca83e1d73f3776576e9f055cda4d979fed");
        assert_eq!(p.block_num, 408461570);
        assert_eq!(p.receipts, 6);
        assert_eq!(hex::encode(&p.bytes), want["source_block"].as_str().unwrap());
    }

    #[test]
    fn tampered_block_json_changes_the_id_or_fails() {
        let block: Value = serde_json::from_str(include_str!("../tests/fixtures/xpr-testnet-block-408461570.json")).unwrap();
        let mut b = block.clone();
        b["action_mroot"] = json!("00".repeat(32));
        assert_ne!(pack_signed_block(&b).unwrap().id, pack_signed_block(&block).unwrap().id);
        let mut b = block.clone();
        b["transactions"][0]["trx"]["packed_trx"] = json!("00");
        assert!(pack_signed_block(&b).unwrap_err().contains("sha256(packed_trx)"));
        let mut b = block.clone();
        let sig = b["producer_signature"].as_str().unwrap().to_string();
        b["producer_signature"] = json!(format!("{}{}", &sig[..sig.len() - 1], if sig.ends_with('6') { '7' } else { '6' }));
        assert!(pack_signed_block(&b).is_err());
        let mut b = block;
        b.as_object_mut().unwrap().remove("transactions");
        assert!(pack_signed_block(&b).unwrap_err().contains("transactions"));
    }

    #[test]
    fn names_and_timestamps_pack_like_antelope() {
        assert_eq!(name_to_u64("eosio").unwrap(), 0x5530ea0000000000);
        assert_eq!(name_to_u64("eosio.token").unwrap(), 0x5530ea033482a600);
        assert!(name_to_u64("EOSIO").is_err());
        assert_eq!(block_timestamp_slot("2000-01-01T00:00:00.000").unwrap(), 0);
        assert_eq!(block_timestamp_slot("2000-01-01T00:00:00.500").unwrap(), 1);
        assert_eq!(block_timestamp_slot("2026-09-30T00:00:01.000").unwrap(), 0x649e1b02);
        assert!(block_timestamp_slot("2026-09-30T00:00:01.250").is_err());
    }

    #[test]
    fn compare_output_names_exactly_the_failing_tables() {
        let out = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/compare-v1.0.0-408461570.log")).unwrap();
        assert_eq!(compare_failing_tables(&out), vec!["contract_index_double", "global_property"]);
        // As the failed command reports it: "`cmd` exited ...: <stderr> <stdout>" — the first
        // stdout line shares a line with the prefix.
        let wrapped = format!("`x` exited exit status: 1:  table permission: nodeos=1 arena=2\ntable account: rows=1 sha256=aa");
        assert_eq!(compare_failing_tables(&wrapped), vec!["permission"]);
        assert!(compare_failing_tables("21-table nodeos/Arena comparison FAILED").is_empty());
    }

    #[test]
    fn create_chain_output_yields_the_blockchain_id() {
        let (b, s) = parse_create_chain_output(
            "issuing...\nSUBNET_ID=2qFyanyVDk2LKrUsUdh3JjtYJYZGqUg9y9QAvMaWdQZZGaLiXY\nBLOCKCHAIN_ID=2YXVy2NWHZJphNuvWry8So8TpQodhSb7ZVJPxhwhtKS6Z6JjpV\n",
        )
        .unwrap();
        assert_eq!(b, "2YXVy2NWHZJphNuvWry8So8TpQodhSb7ZVJPxhwhtKS6Z6JjpV");
        assert_eq!(s.as_deref(), Some("2qFyanyVDk2LKrUsUdh3JjtYJYZGqUg9y9QAvMaWdQZZGaLiXY"));
        assert!(parse_create_chain_output("ok").is_err());
        assert!(parse_create_chain_output("BLOCKCHAIN_ID=0OIl").is_err());
    }
}
