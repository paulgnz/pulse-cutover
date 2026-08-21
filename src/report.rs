//! `pulse-cutover report` — one command, one sanitized shareable bundle.
//!
//! Collects: fresh doctor JSON, ceremony journal(s) + loop metrics, the
//! staged manifest/config (SANITIZED), the last ~200 lines of every relevant
//! service log doctor detects (nodeos / metalgo-pulse / pulse-gateway /
//! hyperion-rs units / federator, docker logs for a containerized nodeos),
//! the stubbed-intrinsic scan output if present, and the agent version.
//!
//! EVERY text file passes through `sanitize::sanitize` before it is written
//! into the bundle — private keys, tokens and passwords come out as
//! `[REDACTED-<type>]`. The command ends by printing what was redacted and
//! the full file list, so the operator can review before sharing.

use std::collections::BTreeMap;
use std::path::{
    Path,
    PathBuf,
};

use crate::{
    doctor,
    ops::run_shell,
    sanitize,
    verify,
};

pub struct ReportOptions {
    /// Ceremony config to mine for journal/work paths; default
    /// /etc/pulse-cutover/ceremony.toml when present.
    pub config_path: Option<PathBuf>,
    pub out: Option<PathBuf>,
    pub paranoid: bool,
}

struct Collected {
    /// (name inside bundle, sanitized content, per-kind redaction tally)
    files: Vec<(String, String, BTreeMap<&'static str, usize>)>,
    notes: Vec<String>,
    paranoid: bool,
}

impl Collected {
    fn add(&mut self, name: &str, raw: &str) {
        let (clean, tally) = sanitize::sanitize(raw, self.paranoid);
        self.files.push((name.to_string(), clean, tally));
    }

    fn add_file(&mut self, name: &str, path: &Path, tail_lines: Option<usize>) {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let text = match tail_lines {
                    Some(n) => tail(&text, n),
                    None => text,
                };
                self.add(name, &text);
            }
            Err(e) => self.notes.push(format!("skipped {}: {e}", path.display())),
        }
    }

    fn add_cmd(&mut self, name: &str, cmd: &str) {
        match run_shell(cmd) {
            Ok(out) if !out.is_empty() => self.add(name, &out),
            Ok(_) => self.notes.push(format!("{name}: empty output from `{cmd}`")),
            Err(e) => self.notes.push(format!("{name}: {e}")),
        }
    }
}

fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

/// Lenient TOML value lookup — the report must work even when the staged
/// config would fail Config::load's ceremony validation.
fn toml_str(value: &toml::Value, path: &[&str]) -> Option<String> {
    let mut cur = value;
    for key in path {
        cur = cur.get(key)?;
    }
    cur.as_str().map(str::to_string)
}

pub fn run(opts: &ReportOptions) -> Result<(), String> {
    eprintln!("pulse-cutover report: surveying (read-only)...");
    let mut c = Collected {
        files: Vec::new(),
        notes: Vec::new(),
        paranoid: opts.paranoid,
    };

    // 1. Doctor survey (fresh) — both forms.
    let survey = doctor::survey();
    c.add(
        "doctor.json",
        &serde_json::to_string_pretty(&survey).map_err(|e| e.to_string())?,
    );
    c.add("doctor.txt", &doctor::render_human(&survey));

    // 2. Ceremony config + manifest (sanitized; the manifest carries
    //    producer_key — the private-key rule strips it).
    let config_path = opts
        .config_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("/etc/pulse-cutover/ceremony.toml"));
    let mut journal_path: Option<PathBuf> = None;
    let mut work_dir: Option<PathBuf> = None;
    if config_path.exists() {
        c.add_file("ceremony.toml", &config_path, None);
        if let Ok(text) = std::fs::read_to_string(&config_path) {
            if let Ok(value) = text.parse::<toml::Value>() {
                journal_path = toml_str(&value, &["journal_path"]).map(PathBuf::from);
                work_dir = journal_path.as_ref().and_then(|j| j.parent().map(Path::to_path_buf));
            }
        }
    } else {
        c.notes.push(format!("no ceremony config at {}", config_path.display()));
    }
    for manifest in ["/etc/pulse-cutover/ceremony.json", "ceremony.json"] {
        let p = Path::new(manifest);
        if p.exists() {
            c.add_file("ceremony.json", p, None);
            break;
        }
    }

    // 3. Journals + loop metrics + scan output + captured goldens.
    if let Some(journal) = &journal_path {
        if journal.exists() {
            c.add_file("journal.jsonl", journal, Some(2000));
        }
        // Loop-run siblings: journal-loop-N.jsonl beside the main journal.
        if let (Some(dir), Some(stem)) = (
            journal.parent(),
            journal.file_stem().and_then(|s| s.to_str()),
        ) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                let mut loops: Vec<PathBuf> = entries
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| {
                        p.file_name()
                            .and_then(|n| n.to_str())
                            .map(|n| n.starts_with(&format!("{stem}-loop-")))
                            .unwrap_or(false)
                    })
                    .collect();
                loops.sort();
                for p in loops.iter().rev().take(5) {
                    // Most recent 5 loop journals keep the bundle bounded.
                    let name = p.file_name().unwrap().to_string_lossy();
                    c.add_file(&format!("loops/{name}"), p, Some(2000));
                }
            }
        }
    }
    if let Some(work) = &work_dir {
        for (bundle_name, file) in [
            ("scan-contracts.txt", "scan-contracts.txt"),
            ("captured-roots.txt", "captured-roots.txt"),
            ("loop-metrics.jsonl", "loop-metrics.jsonl"),
        ] {
            let p = work.join(file);
            if p.exists() {
                c.add_file(bundle_name, &p, Some(2000));
            }
        }
    }

    // 4. Service logs — only units/containers the survey actually saw.
    let mut units: Vec<String> = survey.pulse_services.keys().cloned().collect();
    for u in &survey.nodeos.systemd_units {
        if !units.contains(u) {
            units.push(u.clone());
        }
    }
    if survey.systemd_present {
        for unit in &units {
            c.add_cmd(
                &format!("logs/{unit}.log"),
                &format!("journalctl -u {unit} -n 200 --no-pager 2>/dev/null"),
            );
        }
    }
    if let Some(container) = &survey.nodeos.container_name {
        c.add_cmd(
            &format!("logs/docker-{container}.log"),
            &format!("docker logs --tail 200 {container} 2>&1"),
        );
    }

    // 5. Meta: version + what this bundle is.
    let hostname = if opts.paranoid {
        "[REDACTED-host]".to_string()
    } else {
        survey.hostname.clone()
    };
    let stamp = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();

    // ---- assemble ----
    let staging_root = std::env::temp_dir().join(format!("pulse-cutover-report-{stamp}"));
    let staging = staging_root.join("pulse-cutover-report");
    std::fs::create_dir_all(&staging).map_err(|e| format!("staging dir: {e}"))?;

    let mut total_by_kind: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut file_list: Vec<String> = Vec::new();
    for (name, content, tally) in &c.files {
        let dest = staging.join(name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&dest, content).map_err(|e| format!("write {name}: {e}"))?;
        file_list.push(name.clone());
        for (k, v) in tally {
            *total_by_kind.entry(k).or_default() += v;
        }
    }
    let meta = serde_json::json!({
        "tool": "pulse-cutover report",
        "agent_version": env!("CARGO_PKG_VERSION"),
        "generated_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "hostname": hostname,
        "paranoid": opts.paranoid,
        "files": file_list,
        "redactions": total_by_kind.iter().map(|(k, v)| (k.to_string(), v)).collect::<BTreeMap<_,_>>(),
        "collection_notes": c.notes,
    });
    std::fs::write(
        staging.join("meta.json"),
        serde_json::to_string_pretty(&meta).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    file_list.push("meta.json".into());

    let out = opts.out.clone().unwrap_or_else(|| {
        PathBuf::from(format!(
            "pulse-cutover-report-{}-{stamp}.tar.gz",
            if opts.paranoid { "host".into() } else { survey.hostname.replace('/', "-") }
        ))
    });
    run_shell(&format!(
        "tar -czf '{}' -C '{}' pulse-cutover-report",
        out.display(),
        staging_root.display()
    ))?;
    let (sha, size) = verify::sha256_file(&out)?;
    let _ = std::fs::remove_dir_all(&staging_root);

    // ---- operator-facing summary ----
    println!("\n== pulse-cutover report bundle ==");
    println!("  {} ({} bytes)", out.display(), size);
    println!("  sha256 {sha}\n");
    println!("files in the bundle:");
    for f in &file_list {
        println!("  - {f}");
    }
    println!("\nredactions applied (review before sharing — nothing listed leaves the box unredacted):");
    if total_by_kind.is_empty() {
        println!("  (none needed — no key/token/password-shaped content found)");
    } else {
        for (kind, n) in &total_by_kind {
            println!("  {n:>5} x {kind} -> [REDACTED-{kind}]");
        }
    }
    if !c.notes.is_empty() {
        println!("\nnot collected:");
        for n in &c.notes {
            println!("  - {n}");
        }
    }
    println!(
        "\nHow to share: this bundle is what we need to make pulse-cutover work on YOUR \
         setup. Review the files (tar -tzf / tar -xzf), then attach it to a GitHub issue \
         using the \"rehearsal feedback\" template at \
         https://github.com/paulgnz/pulse-cutover/issues/new?template=rehearsal-feedback.md \
         (include the sha256 above so we know the bundle arrived intact), or post it in \
         the community Telegram thread. Hostnames/IPs are kept by default so we can talk \
         about your box specifically — re-run with --paranoid to placeholder those too."
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_keeps_last_n_lines() {
        let text = (1..=10).map(|i| i.to_string()).collect::<Vec<_>>().join("\n");
        assert_eq!(tail(&text, 3), "8\n9\n10");
        assert_eq!(tail(&text, 100), text);
    }

    /// End-to-end sanitization through the collector: a planted key in a
    /// collected "manifest" must be redacted in the staged content.
    #[test]
    fn collected_files_are_sanitized() {
        let mut c = Collected {
            files: Vec::new(),
            notes: Vec::new(),
            paranoid: false,
        };
        let fake = "PVT_K1_2bfGi9rYsXQSXXTvJbDAPhHLQUojjaNLomdm3cEJ1XTzMqUt3V";
        c.add(
            "ceremony.json",
            &format!("{{\"producer_key\": \"{fake}\"}}"),
        );
        let (_, content, tally) = &c.files[0];
        assert!(!content.contains(fake));
        assert!(content.contains("[REDACTED-private-key]"));
        assert_eq!(tally["private-key"], 1);
    }
}
