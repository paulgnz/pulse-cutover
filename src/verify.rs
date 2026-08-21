//! Snapshot verification: file-level sha256 + state-level 19-table
//! fingerprints, computed by importing the snapshot into TWO independent
//! fresh arenas (determinism cross-check) through the exact code path the
//! node itself boots with (`pulsevm_snapshot_import::import_chainstate_scaled`).
//!
//! Fingerprint construction matches the upstream import-regression harness
//! byte for byte: per logical table, the `DefaultHasher` (SipHash) of the
//! arena's canonical state bytes — platform-independent because the input is
//! a little-endian, key-sorted byte stream. Golden files use the
//! `golden_import_roots.txt` format: `# comments`, then `<table> <hex u64>`.

use std::{
    collections::BTreeMap,
    io::Read,
    path::Path,
};

use pulsevm_chaindb::ChainDatabase;
use pulsevm_snapshot::SnapshotReader;
use pulsevm_snapshot_import::{
    ImportReport,
    import_chainstate_scaled,
};
use sha2::{
    Digest as _,
    Sha256,
};

/// The 19 logical state tables, in golden-file order.
pub const TABLE_NAMES: [&str; 19] = [
    "account_metadata",
    "account",
    "permission",
    "permission_link",
    "code",
    "transaction",
    "resource_usage",
    "resource_limits",
    "resource_state",
    "dynamic_global_property",
    "global_property",
    "resource_limits_config",
    "contract_table",
    "contract_key_value",
    "contract_idx64",
    "contract_idx128",
    "contract_idx256",
    "contract_idx_double",
    "contract_idx_long_double",
];

#[derive(Debug, Clone)]
pub struct VerifyOutcome {
    pub sha256: String,
    pub file_size: u64,
    pub chain_id: String,
    pub head_block_num: u64,
    pub head_block_id: String,
    pub roots: Vec<(String, u64)>,
    pub report: ImportReport,
}

fn table_root(bytes: &[u8]) -> u64 {
    use std::hash::{
        Hash,
        Hasher,
    };
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

fn arena_roots(db: &ChainDatabase) -> Vec<(String, u64)> {
    let tables: Vec<(&str, Vec<u8>)> = vec![
        ("account_metadata", db.account_metadata_state_bytes()),
        ("account", db.account_state_bytes()),
        ("permission", db.permission_state_bytes()),
        ("permission_link", db.permission_link_state_bytes()),
        ("code", db.code_state_bytes()),
        ("transaction", db.transaction_state_bytes()),
        ("resource_usage", db.resource_usage_state_bytes()),
        ("resource_limits", db.account_limits_state_bytes()),
        ("resource_state", db.resource_state_bytes()),
        (
            "dynamic_global_property",
            db.global_action_sequence()
                .unwrap_or(0)
                .to_le_bytes()
                .to_vec(),
        ),
        ("global_property", db.global_property_state_bytes()),
        ("resource_limits_config", db.resource_config_state_bytes()),
        ("contract_table", db.contract_table_state_bytes()),
        ("contract_key_value", db.contract_kv_state_bytes()),
        ("contract_idx64", db.contract_idx64_state_bytes()),
        ("contract_idx128", db.contract_idx128_state_bytes()),
        ("contract_idx256", db.contract_idx256_state_bytes()),
        ("contract_idx_double", db.contract_idx_double_state_bytes()),
        (
            "contract_idx_long_double",
            db.contract_idx_long_double_state_bytes(),
        ),
    ];
    tables
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), table_root(&bytes)))
        .collect()
}

/// Streaming sha256 of a file (mainnet snapshots are GBs; do not slurp twice).
pub fn sha256_file(path: &Path) -> Result<(String, u64), String> {
    let mut file =
        std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        total += n as u64;
        hasher.update(&buf[..n]);
    }
    Ok((hex::encode(hasher.finalize()), total))
}

/// Import the snapshot into two independent fresh arenas and fingerprint
/// both; any divergence means a nondeterministic writer and fails the
/// ceremony. Returns the (identical) roots plus provenance.
pub fn verify_snapshot(path: &Path, cpu_scale: u64) -> Result<VerifyOutcome, String> {
    let (sha256, file_size) = sha256_file(path)?;
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let snapshot = SnapshotReader::new(&bytes).map_err(|e| format!("parse snapshot: {e:?}"))?;

    let db1 = ChainDatabase::new().map_err(|e| format!("fresh arena 1: {e:?}"))?;
    let report = import_chainstate_scaled(&db1, &snapshot, cpu_scale)
        .map_err(|e| format!("import 1: {e:?}"))?;
    let roots1 = arena_roots(&db1);

    let db2 = ChainDatabase::new().map_err(|e| format!("fresh arena 2: {e:?}"))?;
    import_chainstate_scaled(&db2, &snapshot, cpu_scale)
        .map_err(|e| format!("import 2: {e:?}"))?;
    let roots2 = arena_roots(&db2);

    if roots1 != roots2 {
        let diff: Vec<String> = roots1
            .iter()
            .zip(&roots2)
            .filter(|(a, b)| a != b)
            .map(|(a, b)| format!("{}: {:016x} != {:016x}", a.0, a.1, b.1))
            .collect();
        return Err(format!(
            "NONDETERMINISTIC IMPORT: two fresh-arena imports disagree: {}",
            diff.join(", ")
        ));
    }

    Ok(VerifyOutcome {
        sha256,
        file_size,
        chain_id: hex::encode(report.chain_id.0),
        head_block_num: report.head_block_num as u64,
        head_block_id: hex::encode(report.head_block_id.0),
        roots: roots1,
        report,
    })
}

/// Parse a golden roots file (`golden_import_roots.txt` format).
pub fn parse_goldens(text: &str) -> Result<BTreeMap<String, u64>, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let name = parts.next().ok_or("golden line missing table name")?;
        let value = parts
            .next()
            .ok_or_else(|| format!("golden line for {name} missing value"))?;
        let root = u64::from_str_radix(value, 16)
            .map_err(|e| format!("golden {name}: bad hex {value}: {e}"))?;
        out.insert(name.to_string(), root);
    }
    Ok(out)
}

/// Compare computed roots against goldens; Err lists every mismatch.
pub fn compare_goldens(
    roots: &[(String, u64)],
    goldens: &BTreeMap<String, u64>,
) -> Result<(), String> {
    let mut failures = Vec::new();
    for (name, root) in roots {
        match goldens.get(name) {
            Some(expected) if expected == root => {}
            Some(expected) => failures.push(format!(
                "{name}: computed {root:016x}, golden {expected:016x}"
            )),
            None => failures.push(format!("{name}: no golden entry")),
        }
    }
    for name in goldens.keys() {
        if !roots.iter().any(|(n, _)| n == name) {
            failures.push(format!("{name}: golden entry has no computed root"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

/// Serialize roots in golden-file format with full provenance.
pub fn format_goldens(outcome: &VerifyOutcome, cpu_scale: u64) -> String {
    let mut out = String::new();
    out.push_str("# pulse-cutover captured fingerprints (dual fresh-arena import, verified equal)\n");
    out.push_str(&format!(
        "# chain_id {} cut_height {} cut_block_id {}\n",
        outcome.chain_id, outcome.head_block_num, outcome.head_block_id
    ));
    out.push_str(&format!(
        "# snapshot sha256 {} size {} import_cpu_scale {}\n",
        outcome.sha256, outcome.file_size, cpu_scale
    ));
    for (name, root) in &outcome.roots {
        out.push_str(&format!("{name} {root:016x}\n"));
    }
    out
}
