//! Files that may hold signer material (a producer's private key in the generated PulseVM chain
//! config): created with mode 0600 from the first byte (never written 0644 and chmod-ed later),
//! owned by this process's user, and only in a directory no other local user can write to.
//! Shared boot artifacts (the migration genesis and the boot manifest, identical on every
//! validator) must never contain one.

use std::path::Path;

use serde_json::Value;

/// A private key spelling: `PVT_K1_…` / `PVT_R1_…`, or a legacy WIF (`5…`, 51 base58 characters).
pub fn looks_private(s: &str) -> bool {
    let s = s.trim();
    if s.starts_with("PVT_") {
        return true;
    }
    s.len() == 51
        && s.starts_with('5')
        && s.bytes().all(|b| b.is_ascii_alphanumeric() && !matches!(b, b'0' | b'O' | b'I' | b'l'))
}

/// Does any string anywhere in `v` look like a private key?
pub fn has_private_key(v: &Value) -> bool {
    match v {
        Value::String(s) => looks_private(s),
        Value::Array(a) => a.iter().any(has_private_key),
        Value::Object(m) => m.values().any(has_private_key),
        _ => false,
    }
}

/// A directory that may receive a private file: it exists, is not writable by group or others
/// (another user could swap the file or plant a link), and is owned by this user or root.
pub fn check_private_dir(dir: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let m = std::fs::metadata(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        if !m.is_dir() {
            return Err(format!("{} is not a directory", dir.display()));
        }
        let mode = m.permissions().mode() & 0o7777;
        if mode & 0o022 != 0 {
            return Err(format!(
                "{} is writable by group/others (mode {mode:04o}): refusing to put signer material there \
                 (chmod go-w it, or use a private directory)",
                dir.display()
            ));
        }
        // SAFETY: geteuid has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        if m.uid() != me && m.uid() != 0 {
            return Err(format!(
                "{} is owned by uid {} (not this user, uid {me}, nor root): refusing to put signer material there",
                dir.display(),
                m.uid()
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Write `bytes` to `path` atomically (temp file in the same directory + rename). `private`: the
/// file is created 0600 (O_CREAT|O_EXCL with that mode, so it is never readable by others, not
/// even for an instant), its directory must pass `check_private_dir`, and the result is checked
/// to be owned by this user with no group/other bits. Otherwise 0644.
pub fn write_file(path: &Path, bytes: &[u8], private: bool) -> Result<(), String> {
    use std::io::Write;
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    if private {
        check_private_dir(dir)?;
    }
    let name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
    let tmp = dir.join(format!(".{name}.tmp-{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(if private { 0o600 } else { 0o644 });
    }
    let written = (|| {
        let mut f = opts.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("write {}: {e}", path.display()));
    }
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let m = std::fs::symlink_metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
        // SAFETY: geteuid has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        if !m.is_file() || m.permissions().mode() & 0o077 != 0 || m.uid() != me {
            return Err(format!(
                "{} is not a private file after writing (mode {:04o}, uid {}): refusing to leave signer material there",
                path.display(),
                m.permissions().mode() & 0o7777,
                m.uid()
            ));
        }
    }
    Ok(())
}

/// Is a file group/other-readable? (A private-key file that is, is reported, not rewritten.)
pub fn readable_by_others(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        return std::fs::metadata(path).map(|m| m.permissions().mode() & 0o044 != 0).unwrap_or(false);
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn private_key_spellings_are_recognized() {
        // Synthetic spellings (no real key material in the repository).
        assert!(looks_private(&format!("PVT_K1_{}", "Ab3".repeat(17))));
        assert!(looks_private(&format!("5{}", "Kq7".repeat(16) + "Kq")));
        assert!(!looks_private(&format!("5{}", "K0".repeat(25))), "0 is not base58");
        assert!(!looks_private("EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV"));
        assert!(!looks_private("PUB_K1_6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5BoDq63"));
        assert!(has_private_key(&serde_json::json!({"a": [{"producer_key": "PVT_K1_x"}]})));
        assert!(!has_private_key(&serde_json::json!({"initial_key": "PUB_K1_x"})));
    }

    #[test]
    fn a_private_file_is_0600_from_creation_and_a_shared_dir_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let p = dir.path().join("chain-config.json");
        // An existing world-readable file is replaced, not reused with its old mode.
        std::fs::write(&p, b"old").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_file(&p, b"{\"producer_key\":\"PVT_K1_x\"}", true).unwrap();
        assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::read(&p).unwrap(), b"{\"producer_key\":\"PVT_K1_x\"}");
        write_file(&dir.path().join("genesis.json"), b"{}", false).unwrap();
        assert_eq!(std::fs::metadata(dir.path().join("genesis.json")).unwrap().permissions().mode() & 0o777, 0o644);

        let shared = dir.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).unwrap();
        let err = write_file(&shared.join("c.json"), b"{}", true).unwrap_err();
        assert!(err.contains("writable by group/others"), "{err}");
        assert!(!shared.join("c.json").exists());
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o775)).unwrap();
        assert!(write_file(&shared.join("c.json"), b"{}", true).is_err(), "group-writable is refused too");
        // A non-private file may go anywhere the user can write.
        write_file(&shared.join("public.json"), b"{}", false).unwrap();
    }
}
