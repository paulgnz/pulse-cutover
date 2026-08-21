//! Redaction engine for `pulse-cutover report` — the security-critical path.
//!
//! Everything that leaves an operator's box through a feedback bundle goes
//! through [`sanitize`]. The rules are deliberately over-eager on anything
//! key-shaped and deliberately conservative about ordinary chain facts:
//! chain ids, block ids and sha256 digests are 64-hex too, and they are the
//! *evidence* — a bundle with those redacted is useless. So bare 64-hex is
//! kept unless the same line says it is a secret (`private`, `secret`,
//! `signing`, `seed`, `wif`, …).
//!
//! `--paranoid` additionally placeholders public IPv4 addresses and
//! domain-looking hostnames; by default those stay (they are how we tell one
//! rehearsal box from another).

use std::collections::BTreeMap;

use regex::Regex;

/// One redaction rule: pattern -> replacement, tallied under `kind`.
struct Rule {
    kind: &'static str,
    re: Regex,
    replacement: &'static str,
}

fn base_rules() -> Vec<Rule> {
    let rule = |kind, pattern: &str, replacement| Rule {
        kind,
        re: Regex::new(pattern).expect("static redaction regex"),
        replacement,
    };
    vec![
        // Antelope/EOSIO private keys, any curve: PVT_K1_..., PVT_R1_...
        rule(
            "private-key",
            r"PVT_[A-Za-z0-9]{2}_[1-9A-HJ-NP-Za-km-z]{20,}",
            "[REDACTED-private-key]",
        ),
        // Legacy WIF (51 base58 chars starting with 5) — the old eosio key format.
        rule(
            "private-key-wif",
            r"\b5[1-9A-HJ-NP-Za-km-z]{50}\b",
            "[REDACTED-private-key-wif]",
        ),
        // 64-hex ONLY when the line labels it a secret. Bare 64-hex (chain_id,
        // block ids, sha256) is ceremony evidence and stays.
        rule(
            "hex-secret",
            r#"(?i)(?P<k>(secret|private|priv|signing|seed|wif)[a-z0-9_\-]*\s*["']?\s*[:=]\s*["']?)[0-9a-fA-F]{64}"#,
            "${k}[REDACTED-hex-secret]",
        ),
        // Bearer tokens (curl transcripts, nginx/gateway logs).
        rule(
            "bearer-token",
            r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]{8,}=*",
            "Bearer [REDACTED-token]",
        ),
        rule(
            "authorization-header",
            r"(?i)^(?P<k>\s*authorization\s*:\s*).+$",
            "${k}[REDACTED-token]",
        ),
        // key=value / key: value passwords & tokens (toml, ini, env, logs).
        // Matches: password, passwd, pass, api_key, apikey, access_key,
        // secret_key, auth_token, token. Values keep nothing.
        rule(
            "password",
            r#"(?i)(?P<k>\b(pass(word|wd)?|es_pass)\s*["']?\s*[:=]\s*)("[^"]*"|'[^']*'|\S+)"#,
            "${k}[REDACTED-password]",
        ),
        rule(
            "api-token",
            r#"(?i)(?P<k>\b(api[_-]?key|access[_-]?key|secret[_-]?key|auth[_-]?token|token)\s*["']?\s*[:=]\s*)("[^"]*"|'[^']*'|\S+)"#,
            "${k}[REDACTED-token]",
        ),
        // Credentials embedded in URLs: scheme://user:pass@host
        rule(
            "url-credentials",
            r"://(?P<u>[^/:@\s]+):[^@\s/]+@",
            "://${u}:[REDACTED-password]@",
        ),
    ]
}

fn paranoid_rules() -> Vec<Rule> {
    let rule = |kind, pattern: &str, replacement| Rule {
        kind,
        re: Regex::new(pattern).expect("static redaction regex"),
        replacement,
    };
    vec![
        // Public IPv4. The regex crate has no lookahead, so the loopback /
        // wildcard exclusions (127.0.0.1, 0.0.0.0 — they carry no identity
        // and losing them breaks reading nginx/unit configs) are handled by
        // `apply_ip_rule`'s protect-replace-restore pass, not the pattern.
        rule(
            "ip-address",
            r"\b((25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\b",
            "[REDACTED-ip]",
        ),
        // Domain-looking hostnames on common TLDs. Deliberately a TLD
        // allowlist, not a generic dotted-token match: file names
        // (server.js, config.toml) must survive.
        rule(
            "hostname",
            r"\b[A-Za-z0-9][A-Za-z0-9\-.]*\.(com|net|org|io|dev|nz|cloud|xyz|app|tech|network|finance)\b",
            "[REDACTED-host]",
        ),
    ]
}

/// Regex crate has no lookahead; emulate the IPv4 exclusion by a two-pass:
/// protect the allowed literals, run the rule, restore.
fn apply_ip_rule(text: &str, rule: &Rule, tally: &mut BTreeMap<&'static str, usize>) -> String {
    const KEEP: [&str; 2] = ["127.0.0.1", "0.0.0.0"];
    let mut protected = text.to_string();
    for (i, k) in KEEP.iter().enumerate() {
        protected = protected.replace(k, &format!("\u{1}KEEP{i}\u{1}"));
    }
    let n = rule.re.find_iter(&protected).count();
    if n > 0 {
        *tally.entry(rule.kind).or_default() += n;
    }
    let mut out = rule.re.replace_all(&protected, rule.replacement).to_string();
    for (i, k) in KEEP.iter().enumerate() {
        out = out.replace(&format!("\u{1}KEEP{i}\u{1}"), k);
    }
    out
}

/// Sanitize one text document. Returns the redacted text plus a tally of
/// redactions by kind (empty tally == nothing sensitive found).
pub fn sanitize(text: &str, paranoid: bool) -> (String, BTreeMap<&'static str, usize>) {
    let mut tally: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut out = String::with_capacity(text.len());
    // Line-oriented so ^/$ anchored rules (authorization-header) work and a
    // pathological line cannot make a rule eat the whole document.
    for (i, line) in text.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        let mut cur = line.to_string();
        for rule in base_rules() {
            let n = rule.re.find_iter(&cur).count();
            if n > 0 {
                *tally.entry(rule.kind).or_default() += n;
                cur = rule.re.replace_all(&cur, rule.replacement).to_string();
            }
        }
        if paranoid {
            for rule in paranoid_rules() {
                if rule.kind == "ip-address" {
                    cur = apply_ip_rule(&cur, &rule, &mut tally);
                } else {
                    let n = rule.re.find_iter(&cur).count();
                    if n > 0 {
                        *tally.entry(rule.kind).or_default() += n;
                        cur = rule.re.replace_all(&cur, rule.replacement).to_string();
                    }
                }
            }
        }
        out.push_str(&cur);
    }
    (out, tally)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE gate: every planted fake secret must be gone from the output and
    /// replaced with its typed placeholder.
    #[test]
    fn planted_private_keys_are_stripped() {
        let fake_k1 = "PVT_K1_2bfGi9rYsXQSXXTvJbDAPhHLQUojjaNLomdm3cEJ1XTzMqUt3V";
        let fake_r1 = "PVT_R1_vFqhewbYRHwSySmvGzCcXPfjEwFHfDcW6d9dDCONE9DZatt6u";
        let fake_wif = "5KQwrPbi5N4gyDKn3jVrtVXLPujQAKgPKoJRAY5og93cV1DsQwt";
        let input = format!(
            "producer_key = \"{fake_k1}\"\nlegacy: {fake_wif}\nother_key={fake_r1}\n"
        );
        let (out, tally) = sanitize(&input, false);
        assert!(!out.contains(fake_k1), "K1 key leaked:\n{out}");
        assert!(!out.contains(fake_r1), "R1 key leaked:\n{out}");
        assert!(!out.contains(fake_wif), "WIF key leaked:\n{out}");
        assert!(out.contains("producer_key = \"[REDACTED-private-key]\""));
        assert!(out.contains("[REDACTED-private-key-wif]"));
        assert_eq!(tally["private-key"], 2);
        assert_eq!(tally["private-key-wif"], 1);
    }

    #[test]
    fn labeled_hex_secret_stripped_but_chain_evidence_kept() {
        let chain_id = "71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8bfd67e64cb5ac884689652b";
        let block_id = "174858fc00000000000000000000000000000000000000000000000000000000";
        let secret = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        let input = format!(
            "chain_id: {chain_id}\ncut_block_id: {block_id}\nsigning_key = {secret}\nsha256 {chain_id}\n"
        );
        let (out, tally) = sanitize(&input, false);
        assert!(out.contains(chain_id), "chain_id must be KEPT (evidence)");
        assert!(out.contains(block_id), "block id must be KEPT (evidence)");
        assert!(!out.contains(secret), "labeled hex secret leaked:\n{out}");
        assert!(out.contains("signing_key = [REDACTED-hex-secret]"));
        assert_eq!(tally["hex-secret"], 1);
        assert_eq!(out.matches(chain_id).count(), 2);
    }

    #[test]
    fn bearer_tokens_and_auth_headers_stripped() {
        let input = "curl -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig'\n\
                     authorization: Basic cGF1bDpodW50ZXIy\n";
        let (out, tally) = sanitize(input, false);
        assert!(!out.contains("eyJhbGciOiJIUzI1NiJ9"), "bearer leaked:\n{out}");
        assert!(!out.contains("cGF1bDpodW50ZXIy"), "basic auth leaked:\n{out}");
        assert!(tally["bearer-token"] >= 1);
        assert!(tally["authorization-header"] >= 1);
    }

    #[test]
    fn es_passwords_and_url_credentials_stripped() {
        let input = "pass = \"hunter2secret\"\n\
                     password: sup3rs3cret\n\
                     url = \"http://elastic:changeme@127.0.0.1:9200\"\n\
                     api_key = abcdef123456\n";
        let (out, tally) = sanitize(input, false);
        assert!(!out.contains("hunter2secret"), "toml pass leaked:\n{out}");
        assert!(!out.contains("sup3rs3cret"), "password leaked:\n{out}");
        assert!(!out.contains("changeme"), "url credential leaked:\n{out}");
        assert!(!out.contains("abcdef123456"), "api key leaked:\n{out}");
        assert!(out.contains("elastic:[REDACTED-password]@"));
        assert!(tally["password"] >= 2);
        assert!(tally["url-credentials"] >= 1);
        assert!(tally["api-token"] >= 1);
    }

    #[test]
    fn empty_quoted_password_is_redacted_not_panicked() {
        // hyperion config.toml ships `pass = ""` — must not break parsing.
        let (out, _) = sanitize("user = \"\"\npass = \"\"\n", false);
        assert!(out.contains("pass = [REDACTED-password]"));
    }

    #[test]
    fn default_mode_keeps_hostnames_and_ips() {
        let input = "public_url = http://178.105.197.65\nhost rpc.testnet.protonnz.com\n";
        let (out, tally) = sanitize(input, false);
        assert!(out.contains("178.105.197.65"));
        assert!(out.contains("rpc.testnet.protonnz.com"));
        assert!(tally.is_empty());
    }

    #[test]
    fn paranoid_mode_placeholders_ips_and_hostnames_but_keeps_loopback() {
        let input = "upstream 178.105.197.65; server 127.0.0.1:8888; listen 0.0.0.0:80;\n\
                     server_name rpc.testnet.protonnz.com;\nfile: server.js config.toml\n";
        let (out, tally) = sanitize(input, true);
        assert!(!out.contains("178.105.197.65"), "public IP leaked:\n{out}");
        assert!(out.contains("[REDACTED-ip]"));
        assert!(out.contains("127.0.0.1:8888"), "loopback must survive");
        assert!(out.contains("0.0.0.0:80"), "wildcard bind must survive");
        assert!(!out.contains("protonnz.com"), "hostname leaked:\n{out}");
        assert!(out.contains("server.js"), "file names must survive paranoid mode");
        assert!(out.contains("config.toml"));
        assert!(tally["ip-address"] >= 1);
        assert!(tally["hostname"] >= 1);
    }

    #[test]
    fn journal_lines_survive_sanitization_structurally() {
        // A realistic journal line: JSON with hashes and ids — must come out
        // byte-identical (no secrets present).
        let line = r#"{"seq":3,"ts_ms":1755763200000,"kind":"transition","state":"VERIFIED","data":{"sha256":"9f4c3e64c4708a0bd104f9d266d3f9de9ab4bce17ffe1c8ba434f9930a1a4dcb","chain_id":"71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8bfd67e64cb5ac884689652b","cut_height":390536704}}"#;
        let (out, tally) = sanitize(line, false);
        assert_eq!(out, line);
        assert!(tally.is_empty());
    }

    #[test]
    fn multiline_document_tally_accumulates() {
        let doc = "PVT_K1_2bfGi9rYsXQSXXTvJbDAPhHLQUojjaNLomdm3cEJ1XTzMqUt3V\n\
                   PVT_K1_2Zn9zzHK3XG8Vv7t4H6bkKKHmRUuBV3akUpU4dHK1BJTkQxbSN\n";
        let (out, tally) = sanitize(doc, false);
        assert_eq!(tally["private-key"], 2);
        assert_eq!(out.matches("[REDACTED-private-key]").count(), 2);
    }
}
