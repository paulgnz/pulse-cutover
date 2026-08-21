//! `pulse-cutover scan-contracts` — stubbed-intrinsic exposure scan.
//!
//! Since PulseVM's arena-import branch, unknown `env` imports LOAD (the
//! module gets a stub) but TRAP on call ("unimplemented intrinsic called").
//! The migration exposure is therefore exactly: contracts whose wasm import
//! section references a host function outside the served table. This module
//! reimplements the 2026-08-15 audit (wiki/assets/stubbed-intrinsic-audit)
//! inside the agent: parse every code object in a portable snapshot with
//! `wasmparser`, diff `env` function imports against the served set, report
//! per-contract.
//!
//! ADVISORY, never a gate: a referenced import is a real code path but not
//! necessarily a reachable one (the deferred-tx cluster is the canonical
//! example). The ceremony journals the table and continues.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use pulsevm_snapshot::SnapshotReader;
use wasmparser::{
    Imports,
    Parser,
    Payload,
    TypeRef,
};

/// The served host-function table: the 169 registered `env` import names
/// extracted from pulsevm_core wasm_runtime.rs (feat/arena-snapshot-import
/// @4e95f2dd, incl. pulse_*/eosio_* alias pairs). Override with --served
/// when auditing against a different node build.
pub const DEFAULT_SERVED: &str = include_str!("served_imports.txt");

pub fn parse_served(text: &str) -> BTreeSet<String> {
    text.lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanRow {
    pub code_hash: String,
    pub accounts: Vec<String>,
    pub env_imports_total: usize,
    pub unserved: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ScanReport {
    pub served_count: usize,
    pub code_objects: u64,
    pub clean: u64,
    pub at_risk: u64,
    pub parse_failures: u64,
    /// Code objects importing outside `env` entirely (would fail
    /// instantiation, not merely stub) — should be zero on real snapshots.
    pub non_env_importers: u64,
    pub rows: Vec<ScanRow>,
    /// unserved import name -> number of code objects referencing it.
    pub unserved_tally: BTreeMap<String, u64>,
}

/// Function imports of one wasm module, split into `env` and everything else.
pub fn wasm_env_imports(
    wasm: &[u8],
) -> Result<(BTreeSet<String>, BTreeSet<String>), String> {
    let mut env = BTreeSet::new();
    let mut non_env = BTreeSet::new();
    for payload in Parser::new(0).parse_all(wasm) {
        match payload.map_err(|e| e.to_string())? {
            Payload::ImportSection(reader) => {
                let mut record = |module: &str, name: &str, is_func: bool| {
                    if !is_func {
                        return; // only function imports can be called/stubbed
                    }
                    if module == "env" {
                        env.insert(name.to_string());
                    } else {
                        non_env.insert(format!("{module}::{name}"));
                    }
                };
                let is_func =
                    |ty: &TypeRef| matches!(ty, TypeRef::Func(_) | TypeRef::FuncExact(_));
                for group in reader {
                    match group.map_err(|e| e.to_string())? {
                        Imports::Single(_, imp) => {
                            record(imp.module, imp.name, is_func(&imp.ty))
                        }
                        Imports::Compact1 { module, items } => {
                            for item in items {
                                let item = item.map_err(|e| e.to_string())?;
                                record(module, item.name, is_func(&item.ty));
                            }
                        }
                        Imports::Compact2 { module, ty, names } => {
                            let f = is_func(&ty);
                            for name in names {
                                record(module, name.map_err(|e| e.to_string())?, f);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok((env, non_env))
}

/// Scan every code object in a snapshot against the served import set.
pub fn scan_snapshot_bytes(
    bytes: &[u8],
    served: &BTreeSet<String>,
) -> Result<ScanReport, String> {
    let snapshot = SnapshotReader::new(bytes).map_err(|e| format!("parse snapshot: {e:?}"))?;

    // code_hash -> accounts running that code.
    let mut accounts_by_hash: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for meta in snapshot
        .account_metadata()
        .map_err(|e| format!("account_metadata: {e:?}"))?
    {
        let meta = meta.map_err(|e| format!("account_metadata row: {e:?}"))?;
        if meta.has_code() {
            accounts_by_hash
                .entry(meta.code_hash.to_string())
                .or_default()
                .push(meta.name.to_string());
        }
    }

    let mut report = ScanReport {
        served_count: served.len(),
        ..Default::default()
    };
    for code in snapshot.code().map_err(|e| format!("code section: {e:?}"))? {
        let code = code.map_err(|e| format!("code row: {e:?}"))?;
        report.code_objects += 1;
        let hash = code.code_hash.to_string();
        let accounts = accounts_by_hash.get(&hash).cloned().unwrap_or_default();
        let (env, non_env) = match wasm_env_imports(&code.code.0) {
            Ok(x) => x,
            Err(_) => {
                report.parse_failures += 1;
                continue;
            }
        };
        if !non_env.is_empty() {
            report.non_env_importers += 1;
        }
        let unserved: Vec<String> = env
            .iter()
            .filter(|n| !served.contains(*n))
            .cloned()
            .collect();
        if unserved.is_empty() {
            report.clean += 1;
        } else {
            report.at_risk += 1;
            for u in &unserved {
                *report.unserved_tally.entry(u.clone()).or_default() += 1;
            }
            report.rows.push(ScanRow {
                code_hash: hash,
                accounts,
                env_imports_total: env.len(),
                unserved,
            });
        }
    }
    // Widest exposure first: most unserved imports, then most accounts.
    report
        .rows
        .sort_by(|a, b| {
            b.unserved
                .len()
                .cmp(&a.unserved.len())
                .then(b.accounts.len().cmp(&a.accounts.len()))
                .then(a.code_hash.cmp(&b.code_hash))
        });
    Ok(report)
}

pub fn scan_snapshot_path(
    path: &std::path::Path,
    served: &BTreeSet<String>,
) -> Result<ScanReport, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    scan_snapshot_bytes(&bytes, served)
}

/// The human at-risk table (also what the ceremony prints during preflight).
pub fn format_table(report: &ScanReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "stubbed-intrinsic scan: {} code objects, {} clean, {} at-risk, {} parse failures (served table: {} imports)\n",
        report.code_objects, report.clean, report.at_risk, report.parse_failures, report.served_count
    ));
    if report.at_risk == 0 {
        out.push_str("  every env import of every contract is served — no stub-trap exposure.\n");
        return out;
    }
    out.push_str(
        "  ADVISORY: these contracts reference host functions PulseVM stubs — they load,\n  but TRAP if the import is ever CALLED (referenced != reachable).\n\n",
    );
    out.push_str(&format!(
        "  {:<10} {:<28} {:>4}  {}\n",
        "code_hash", "accounts", "env", "unserved imports"
    ));
    for row in &report.rows {
        let hash8 = row.code_hash.get(0..8).unwrap_or(&row.code_hash);
        let accounts = if row.accounts.is_empty() {
            "(no-account)".to_string()
        } else {
            row.accounts.join(" ")
        };
        out.push_str(&format!(
            "  {:<10} {:<28} {:>4}  {}\n",
            hash8,
            accounts,
            row.env_imports_total,
            row.unserved.join(" ")
        ));
    }
    out.push_str("\n  unserved import frequency:\n");
    let mut tally: Vec<_> = report.unserved_tally.iter().collect();
    tally.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
    for (name, n) in tally {
        out.push_str(&format!("  {n:>5}  {name}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-assemble a minimal wasm module whose import section pulls the
    /// given function names from `env` (plus optionally one non-env import).
    fn wasm_with_env_imports(names: &[&str], non_env: Option<(&str, &str)>) -> Vec<u8> {
        fn leb(out: &mut Vec<u8>, mut n: u32) {
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
        fn name(out: &mut Vec<u8>, s: &str) {
            leb(out, s.len() as u32);
            out.extend(s.as_bytes());
        }
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]; // \0asm v1
        // Type section: one type, () -> ().
        let mut types = Vec::new();
        leb(&mut types, 1);
        types.push(0x60);
        leb(&mut types, 0);
        leb(&mut types, 0);
        module.push(1);
        leb(&mut module, types.len() as u32);
        module.extend(&types);
        // Import section: each name as a func import of type 0.
        let mut imports = Vec::new();
        let total = names.len() + usize::from(non_env.is_some());
        leb(&mut imports, total as u32);
        for n in names {
            name(&mut imports, "env");
            name(&mut imports, n);
            imports.push(0x00); // import kind: func
            leb(&mut imports, 0); // type index
        }
        if let Some((module_name, field)) = non_env {
            name(&mut imports, module_name);
            name(&mut imports, field);
            imports.push(0x00);
            leb(&mut imports, 0);
        }
        module.push(2);
        leb(&mut module, imports.len() as u32);
        module.extend(&imports);
        module
    }

    #[test]
    fn extracts_env_function_imports() {
        let wasm = wasm_with_env_imports(
            &["eosio_assert", "send_deferred", "get_sender"],
            Some(("wasi", "fd_write")),
        );
        let (env, non_env) = wasm_env_imports(&wasm).unwrap();
        assert_eq!(
            env,
            ["eosio_assert", "send_deferred", "get_sender"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
        assert_eq!(non_env, ["wasi::fd_write".to_string()].into_iter().collect());
    }

    #[test]
    fn classification_splits_served_from_unserved() {
        let served = parse_served(DEFAULT_SERVED);
        assert_eq!(served.len(), 169, "embedded served table drifted");
        // Known-served names from the audit method...
        for name in ["eosio_assert", "db_find_i64", "sha256", "__fixtfdi"] {
            assert!(served.contains(name), "{name} should be served");
        }
        // ...and the audit's headline unserved names must NOT be in it.
        for name in [
            "send_deferred",
            "cancel_deferred",
            "get_sender",
            "__fixunstfdi",
            "get_code_hash",
            "preactivate_feature",
            "set_proposed_producers_ex",
        ] {
            assert!(!served.contains(name), "{name} should be unserved");
        }
    }

    #[test]
    fn mini_snapshot_scans_clean_with_zero_code_objects() {
        // MiniSnapshot carries an empty code section: the scan must parse the
        // real container format and report zero exposure (not error out).
        use pulsevm_snapshot::testing::{
            MiniSnapshot,
            TestAccount,
        };
        let mini = MiniSnapshot {
            chain_id: [0xCD; 32],
            head_block_num: 42,
            head_slot: 1_514_764_800,
            head_producer: "eosio".parse().unwrap(),
            accounts: vec![TestAccount {
                name: "scandemo".parse().unwrap(),
                key: [2u8; 33],
            }],
        };
        let served = parse_served(DEFAULT_SERVED);
        let report = scan_snapshot_bytes(&mini.build(), &served).unwrap();
        assert_eq!(report.code_objects, 0);
        assert_eq!(report.at_risk, 0);
        assert_eq!(report.parse_failures, 0);
        let table = format_table(&report);
        assert!(table.contains("no stub-trap exposure"));
    }

    #[test]
    fn at_risk_module_is_reported_with_unserved_names() {
        let served = parse_served(DEFAULT_SERVED);
        let wasm = wasm_with_env_imports(&["eosio_assert", "send_deferred"], None);
        let (env, _) = wasm_env_imports(&wasm).unwrap();
        let unserved: Vec<&String> = env.iter().filter(|n| !served.contains(*n)).collect();
        assert_eq!(unserved, ["send_deferred"]);
    }
}
