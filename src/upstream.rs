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
                .run_hook(&cmd)
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
                "export_wall_ms": ops.now_ms() - started,
            }));
            (log, env, false)
        }
    };
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
        .run_hook(&format!(
            "'{}' '{}' '{}' '{}'",
            up.import_bin.display(),
            ship_log.display(),
            arena_import.display(),
            checkpoint_path.display()
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
            "import_wall_ms": ops.now_ms() - started,
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
    let (compare_stdout, compare_report_path) = match &up.compare_bin {
        Some(bin) => {
            let arena_cmp = up.work_dir.join(format!("arena-compare-{cut_height}"));
            fresh_dir(&arena_cmp)?;
            let report = up.work_dir.join(format!("compare-report-{cut_height}.json"));
            let _ = std::fs::remove_file(&report);
            let started = ops.now_ms();
            // A non-zero exit (any nodeos-vs-Arena table difference) is a
            // verification FAILURE — run_hook errors and we propagate.
            let out = ops
                .run_hook(&format!(
                    "'{}' '{}' '{}' '{}' '{}' '{}'",
                    bin.display(),
                    ship_log.display(),
                    checkpoint_path.display(),
                    arena_cmp.display(),
                    chain_id,
                    report.display()
                ))
                .map_err(|e| {
                    format!(
                        "xpr_19_table_compare FAILED — the nodeos SHiP snapshot and the Arena \
                         re-serialization disagree (verification failure): {e}"
                    )
                })?;
            progress(json!({
                "upstream_19_table_compare": {
                    "result": "MATCH",
                    "tables": out.lines().collect::<Vec<_>>(),
                    "report": report.display().to_string(),
                    "compare_wall_ms": ops.now_ms() - started,
                }
            }));
            (Some(out), Some(report))
        }
        None => {
            progress(json!({
                "upstream_19_table_compare": "skipped (no compare_bin configured)"
            }));
            (None, None)
        }
    };

    // ---- 4. xpr_state_fingerprint: whole-state root (cross-node golden) ----
    let arena_fp = up.work_dir.join(format!("arena-fingerprint-{cut_height}"));
    fresh_dir(&arena_fp)?;
    let fp_out = ops
        .run_hook(&format!(
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
        state_root,
        fingerprint_tables,
    })
}

fn manifest_json_path(checkpoint: &Path) -> PathBuf {
    PathBuf::from(format!("{}.manifest.json", checkpoint.display()))
}

/// What still stands between VERIFIED-with-the-official-tools and an ignition
/// from the upstream checkpoint. Kept as data so the journal documents it
/// verbatim every time the stub fires.
pub fn ignite_pending_reasons() -> Vec<&'static str> {
    vec![
        "MetalBlockchain/pulsevm#61 must merge and ship a released node build \
         (the checkpoint-consuming node is only on the PR branch; building it \
         needs LLVM 22 for the Wasmer backend)",
        "a migration genesis committing migration_checkpoint_sha256 must be \
         created for the target chain (the node rejects a checkpoint its \
         genesis does not commit to)",
        "the target chain config must carry the #61 knobs: migration_checkpoint \
         + its .manifest.json path (this branch's node_config), replacing the \
         fork plugin's snapshot_path boot",
        "the released VM id / plugin binary must be staged in place of the fork \
         plugin, and the subnet's validators must run it",
    ]
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
    fn ignite_pending_reasons_name_the_gaps() {
        let reasons = ignite_pending_reasons();
        assert!(reasons.iter().any(|r| r.contains("#61")));
        assert!(reasons.iter().any(|r| r.contains("migration_checkpoint_sha256")));
        assert!(reasons.iter().any(|r| r.contains("snapshot_path")));
    }
}
