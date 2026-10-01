//! Antelope K1 key strings, and the producer signing-key preflight.
//!
//! Stage-2 run 1: the migrated eosio.system re-elected the schedule from `eosio/producers` at the
//! first onblock; the node signed with the genesis initial key, not the producer's REGISTERED
//! key, so PulseVM refused to build and the chain halted silently. Before ignition the agent now
//! compares the key the target will sign with against what the source registers for it.

use secp256k1::{PublicKey, Secp256k1, SecretKey};
use serde_json::{json, Value};
use sha2::Digest as _;

fn ripemd_check(data: &[u8], suffix: &[u8]) -> [u8; 4] {
    use ripemd::Digest as _;
    let mut h = ripemd::Ripemd160::new();
    h.update(data);
    h.update(suffix);
    h.finalize()[..4].try_into().expect("4 bytes")
}

/// `PUB_K1_…` or legacy `EOS…` -> 33-byte compressed secp256k1 key (checksum verified).
pub fn parse_public_k1(s: &str) -> Result<[u8; 33], String> {
    let (b58, suffix): (&str, &[u8]) = if let Some(r) = s.strip_prefix("PUB_K1_") {
        (r, b"K1")
    } else if let Some(r) = s.strip_prefix("EOS") {
        (r, b"")
    } else {
        return Err(format!("{s:.16}… is not a K1 public key (PUB_K1_… or EOS…)"));
    };
    let raw = bs58::decode(b58).into_vec().map_err(|e| format!("public key base58: {e}"))?;
    if raw.len() != 37 || ripemd_check(&raw[..33], suffix) != raw[33..] {
        return Err(format!("{s:.16}… is not a valid K1 public key (length or checksum)"));
    }
    Ok(raw[..33].try_into().expect("33 bytes"))
}

/// 33-byte compressed key -> `PUB_K1_…`.
pub fn format_public_k1(key: &[u8; 33]) -> String {
    let mut raw = key.to_vec();
    raw.extend_from_slice(&ripemd_check(key, b"K1"));
    format!("PUB_K1_{}", bs58::encode(raw).into_string())
}

/// 33-byte compressed key -> legacy `EOS…` (what nodeos prints in eosio/producers).
pub fn format_legacy_public(key: &[u8; 33]) -> String {
    let mut raw = key.to_vec();
    raw.extend_from_slice(&ripemd_check(key, b""));
    format!("EOS{}", bs58::encode(raw).into_string())
}

/// 32-byte secret -> `PVT_K1_…` (fixtures and tooling).
pub fn format_private_k1(secret: &[u8; 32]) -> String {
    let mut raw = secret.to_vec();
    raw.extend_from_slice(&ripemd_check(secret, b"K1"));
    format!("PVT_K1_{}", bs58::encode(raw).into_string())
}

/// The public key of a `PVT_K1_…` or legacy WIF (`5…`) private key. The error never echoes
/// the private key.
pub fn public_of_private_k1(s: &str) -> Result<[u8; 33], String> {
    let secret: [u8; 32] = if let Some(r) = s.strip_prefix("PVT_K1_") {
        let raw = bs58::decode(r).into_vec().map_err(|_| "private key is not valid base58".to_string())?;
        if raw.len() != 36 || ripemd_check(&raw[..32], b"K1") != raw[32..] {
            return Err("PVT_K1_ private key has a bad length or checksum".into());
        }
        raw[..32].try_into().expect("32 bytes")
    } else {
        let raw = bs58::decode(s).into_vec().map_err(|_| "private key is not PVT_K1_… or a valid WIF".to_string())?;
        let ok = raw.len() == 37
            && raw[0] == 0x80
            && sha2::Sha256::digest(sha2::Sha256::digest(&raw[..33]))[..4] == raw[33..];
        if !ok {
            return Err("private key is not PVT_K1_… or a valid WIF (length, version or checksum)".into());
        }
        raw[1..33].try_into().expect("32 bytes")
    };
    let sk = SecretKey::from_byte_array(&secret).map_err(|_| "private key is not a valid secp256k1 secret".to_string())?;
    Ok(PublicKey::from_secret_key(&Secp256k1::signing_only(), &sk).serialize())
}

/// The keys that can sign for a producer per its `eosio/producers` row: `producer_authority`
/// (block_signing_authority_v0) when present, else `producer_key`. Returns (threshold,
/// [(key string, weight)]).
fn registered_signers(row: &Value) -> (u64, Vec<(String, u64)>) {
    // nodeos prints the binary_extension as ["block_signing_authority_v0", {threshold, keys}].
    let auth = match &row["producer_authority"] {
        Value::Array(a) if a.len() == 2 => Some(a[1].clone()),
        Value::Object(o) => Some(Value::Object(o.clone())),
        _ => None,
    };
    if let Some(a) = auth.filter(|a| a["keys"].is_array()) {
        let keys = a["keys"].as_array().expect("checked").iter()
            .filter_map(|k| Some((k["key"].as_str()?.to_string(), k["weight"].as_u64().unwrap_or(0))))
            .collect();
        return (a["threshold"].as_u64().unwrap_or(1), keys);
    }
    (1, row["producer_key"].as_str().map(|k| vec![(k.to_string(), 1)]).unwrap_or_default())
}

/// Can `signing` alone produce for `producer` according to its source `eosio/producers` row?
/// Ok(evidence) or Err(why the migrated chain would halt at the first re-election).
pub fn check_registered_producer_key(row: Option<&Value>, producer: &str, signing: &[u8; 33]) -> Result<Value, String> {
    let ours = format_public_k1(signing);
    let row = row.ok_or_else(|| format!(
        "producer {producer} (this node's chain-config producer_name) is not registered in eosio/producers on the \
         source: after the first onblock the migrated system contract elects the schedule from that table, this \
         node would not be on it and PulseVM would refuse to build blocks"))?;
    if row["is_active"].as_u64() == Some(0) || row["is_active"].as_bool() == Some(false) {
        return Err(format!(
            "producer {producer} is registered but NOT active in eosio/producers: the first re-election after \
             the cut drops it from the schedule and this node would stop producing"));
    }
    let (threshold, keys) = registered_signers(row);
    let matches = keys.iter().any(|(k, w)| *w >= threshold && parse_public_k1(k).map(|p| &p == signing).unwrap_or(false));
    let registered: Vec<String> = keys.iter().map(|(k, _)| parse_public_k1(k).map(|p| format_public_k1(&p)).unwrap_or_else(|_| k.clone())).collect();
    if !matches {
        return Err(format!(
            "producer signing key mismatch: the target will sign as {producer} with {ours}, but eosio/producers \
             registers {} (threshold {threshold}). After the first onblock re-election PulseVM would refuse to \
             build blocks and the chain would halt (stage-2 run 1). Set the genesis initial_key and the chain \
             config producer_key to the registered key (or regproducer with the node's key) before the cut",
            if registered.is_empty() { "no key".to_string() } else { registered.join(", ") }
        ));
    }
    Ok(json!({"producer": producer, "signing_key": ours, "registered": registered, "threshold": threshold, "result": "match"}))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: [u8; 32] = [7u8; 32];

    #[test]
    fn key_formats_round_trip_and_derive() {
        let public = public_of_private_k1(&format_private_k1(&SECRET)).unwrap();
        assert_eq!(parse_public_k1(&format_public_k1(&public)).unwrap(), public);
        assert_eq!(parse_public_k1(&format_legacy_public(&public)).unwrap(), public);
        // Derivation against a mathematical constant: secret 1 is the generator point G.
        let mut one = [0u8; 32];
        one[31] = 1;
        let g = public_of_private_k1(&format_private_k1(&one)).unwrap();
        assert_eq!(hex::encode(g), "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798");
        // Legacy WIF (0x80 ‖ secret ‖ sha256d checksum) derives the same key.
        let mut wif = vec![0x80u8];
        wif.extend_from_slice(&SECRET);
        let check = sha2::Sha256::digest(sha2::Sha256::digest(&wif));
        wif.extend_from_slice(&check[..4]);
        assert_eq!(public_of_private_k1(&bs58::encode(wif).into_string()).unwrap(), public);
        let mut bad = format_public_k1(&public);
        bad.pop();
        bad.push('1');
        assert!(parse_public_k1(&bad).is_err());
        assert!(parse_public_k1("PUB_K1_test").is_err());
        let e = public_of_private_k1("PVT_K1_notakey").unwrap_err();
        assert!(!e.contains("notakey"), "errors never echo the private key: {e}");
    }

    #[test]
    fn registered_key_check_follows_authority_then_producer_key() {
        let ours = public_of_private_k1(&format_private_k1(&SECRET)).unwrap();
        let other = public_of_private_k1(&format_private_k1(&[9u8; 32])).unwrap();
        let row = |key: &[u8; 33]| json!({"owner": "bp1", "is_active": 1, "producer_key": format_legacy_public(key)});
        assert_eq!(check_registered_producer_key(Some(&row(&ours)), "bp1", &ours).unwrap()["result"], "match");
        let e = check_registered_producer_key(Some(&row(&other)), "bp1", &ours).unwrap_err();
        assert!(e.contains("mismatch") && e.contains(&format_public_k1(&other)) && e.contains(&format_public_k1(&ours)), "{e}");
        assert!(check_registered_producer_key(None, "bp1", &ours).unwrap_err().contains("not registered"));
        let mut inactive = row(&ours);
        inactive["is_active"] = json!(0);
        assert!(check_registered_producer_key(Some(&inactive), "bp1", &ours).unwrap_err().contains("NOT active"));
        // producer_authority wins over producer_key when present.
        let mut auth = row(&other);
        auth["producer_authority"] = json!(["block_signing_authority_v0", {"threshold": 1, "keys": [{"key": format_legacy_public(&ours), "weight": 1}]}]);
        assert!(check_registered_producer_key(Some(&auth), "bp1", &ours).is_ok());
        // A multi-key authority this single key cannot satisfy alone.
        auth["producer_authority"] = json!(["block_signing_authority_v0", {"threshold": 2, "keys": [
            {"key": format_legacy_public(&ours), "weight": 1}, {"key": format_legacy_public(&other), "weight": 1}]}]);
        assert!(check_registered_producer_key(Some(&auth), "bp1", &ours).is_err());
    }
}
