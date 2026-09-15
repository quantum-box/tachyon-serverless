//! Host prerequisite checks and file digests.
//!
//! Every check is reported, none panics, and the report is meaningful on a
//! host that cannot run Firecracker at all (e.g. macOS): each failing check
//! carries a human-readable reason.

use std::collections::HashMap;
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tachyon_serverless_domain::Architecture;
use tachyon_serverless_provider_port::{PreflightCheck, PreflightReport};

use crate::config::FirecrackerConfig;
use crate::vmm::{EnvPaths, MAX_UNIX_SOCKET_PATH};

/// Hex SHA-256 of a file, streamed.
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex::encode(h.finalize()))
}

#[derive(Debug, Clone)]
struct CachedDigest {
    len: u64,
    mtime: Option<SystemTime>,
    hex: String,
}

/// Digest cache keyed by path, invalidated when size or mtime change.
#[derive(Debug, Default)]
pub struct DigestCache {
    inner: parking_lot::Mutex<HashMap<PathBuf, CachedDigest>>,
}

impl DigestCache {
    /// Digest of `path`, computed off the async runtime when not cached.
    pub async fn digest(&self, path: &Path) -> std::io::Result<String> {
        let meta = tokio::fs::metadata(path).await?;
        let len = meta.len();
        let mtime = meta.modified().ok();
        if let Some(c) = self.inner.lock().get(path)
            && c.len == len
            && c.mtime == mtime
        {
            return Ok(c.hex.clone());
        }
        let p = path.to_path_buf();
        let hex = tokio::task::spawn_blocking(move || sha256_file(&p))
            .await
            .map_err(|e| std::io::Error::other(format!("digest task failed: {e}")))??;
        self.inner.lock().insert(
            path.to_path_buf(),
            CachedDigest {
                len,
                mtime,
                hex: hex.clone(),
            },
        );
        Ok(hex)
    }
}

/// `access(2)` wrapper; `mode` is a bitmask of `libc::R_OK | W_OK | X_OK`.
pub fn access(path: &Path, mode: libc::c_int) -> bool {
    let Ok(c) = CString::new(path.as_os_str().as_encoded_bytes()) else {
        return false;
    };
    // SAFETY: `c` is a valid NUL-terminated string that outlives the call.
    unsafe { libc::access(c.as_ptr(), mode) == 0 }
}

/// Resolve a command: an explicit path is returned as-is when it exists; a
/// bare name is looked up in `PATH`.
pub fn resolve_command(cmd: &Path) -> Option<PathBuf> {
    if cmd.components().count() > 1 || cmd.is_absolute() {
        return cmd.is_file().then(|| cmd.to_path_buf());
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join(cmd))
        .find(|p| p.is_file() && access(p, libc::X_OK))
}

/// Parse `firecracker --version` output (`Firecracker v1.17.0` on the first
/// line; later lines list snapshot format versions). The first line starting
/// with `Firecracker` wins; otherwise the first non-empty line is used.
pub fn parse_firecracker_version(output: &str) -> Option<String> {
    let lines = || output.lines().map(str::trim);
    let line = lines()
        .find_map(|l| l.find("Firecracker").map(|i| &l[i..]))
        .or_else(|| lines().find(|l| !l.is_empty()))?;
    let v = line.strip_prefix("Firecracker").unwrap_or(line).trim();
    (!v.is_empty()).then(|| v.to_owned())
}

/// Run `firecracker --version` synchronously (best effort, short).
pub fn probe_firecracker_version(binary: &Path) -> Option<String> {
    let out = std::process::Command::new(binary)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_firecracker_version(&String::from_utf8_lossy(&out.stdout))
}

fn check(name: &str, ok: bool, detail: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        name: name.to_owned(),
        ok,
        detail: detail.into(),
    }
}

/// Run all host checks. Never fails; the report says what is wrong.
pub async fn run_preflight(
    cfg: &FirecrackerConfig,
    version: Option<&str>,
    digests: &DigestCache,
) -> PreflightReport {
    let mut checks = Vec::new();

    // 1. Operating system.
    let os = std::env::consts::OS;
    checks.push(check(
        "os",
        os == "linux",
        format!("host os is {os}; Firecracker requires linux"),
    ));

    // 2. Host architecture.
    match Architecture::host() {
        Some(a) => checks.push(check(
            "arch",
            true,
            format!("host architecture {}", a.as_str()),
        )),
        None => checks.push(check(
            "arch",
            false,
            format!(
                "host architecture {} is not supported (x86_64 or aarch64 required)",
                std::env::consts::ARCH
            ),
        )),
    }

    // 3. /dev/kvm.
    let kvm = Path::new("/dev/kvm");
    if !kvm.exists() {
        checks.push(check(
            "kvm",
            false,
            "/dev/kvm does not exist (no KVM on this host)",
        ));
    } else if access(kvm, libc::R_OK | libc::W_OK) {
        checks.push(check("kvm", true, "/dev/kvm is readable and writable"));
    } else {
        checks.push(check(
            "kvm",
            false,
            "/dev/kvm exists but is not rw for this uid (add the user to the kvm group or chmod 660)",
        ));
    }

    // 4. firecracker binary.
    match resolve_command(&cfg.firecracker_binary) {
        Some(p) if access(&p, libc::X_OK) => {
            let ver = version
                .map(str::to_owned)
                .or_else(|| probe_firecracker_version(&p));
            checks.push(check(
                "firecracker_binary",
                true,
                format!(
                    "{} (version {})",
                    p.display(),
                    ver.as_deref().unwrap_or("unknown")
                ),
            ));
        }
        Some(p) => checks.push(check(
            "firecracker_binary",
            false,
            format!("{} is not executable", p.display()),
        )),
        None => checks.push(check(
            "firecracker_binary",
            false,
            format!("{} not found", cfg.firecracker_binary.display()),
        )),
    }

    // 5. kernel.
    checks.push(file_check("kernel", &cfg.kernel, digests).await);
    // 6. rootfs.
    checks.push(file_check("rootfs", &cfg.rootfs, digests).await);

    // 7. mkfs.ext4.
    match resolve_command(&cfg.mkfs_ext4) {
        Some(p) => checks.push(check("mkfs_ext4", true, p.display().to_string())),
        None => checks.push(check(
            "mkfs_ext4",
            false,
            format!("{} not found (install e2fsprogs)", cfg.mkfs_ext4.display()),
        )),
    }

    // 8. workdir writable.
    match probe_workdir(&cfg.workdir).await {
        Ok(()) => checks.push(check("workdir", true, cfg.workdir.display().to_string())),
        Err(e) => checks.push(check(
            "workdir",
            false,
            format!("{} not writable: {e}", cfg.workdir.display()),
        )),
    }

    // 9. Unix socket path length for a representative environment.
    let sample = EnvPaths::new(
        &cfg.workdir,
        "env_00000000000000000000000000",
        cfg.vsock_port,
    );
    let longest = sample.longest_socket_path_len();
    checks.push(check(
        "socket_path_length",
        longest <= MAX_UNIX_SOCKET_PATH,
        format!(
            "longest socket path would be {longest} bytes (max {MAX_UNIX_SOCKET_PATH}); {}",
            if longest <= MAX_UNIX_SOCKET_PATH {
                "ok"
            } else {
                "use a shorter workdir"
            }
        ),
    ));

    let ok = checks.iter().all(|c| c.ok);
    PreflightReport {
        provider: "firecracker".to_owned(),
        ok,
        checks,
    }
}

async fn file_check(name: &str, path: &Path, digests: &DigestCache) -> PreflightCheck {
    if !path.is_file() {
        return check(name, false, format!("{} does not exist", path.display()));
    }
    match digests.digest(path).await {
        Ok(hex) => check(name, true, format!("{} sha256:{hex}", path.display())),
        Err(e) => check(name, false, format!("{} unreadable: {e}", path.display())),
    }
}

async fn probe_workdir(workdir: &Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(workdir).await?;
    let probe = workdir.join(format!(".preflight-{}", std::process::id()));
    tokio::fs::write(&probe, b"ok").await?;
    tokio::fs::remove_file(&probe).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_parsing() {
        assert_eq!(
            parse_firecracker_version(
                "Firecracker v1.17.0\nSupported snapshot data format versions: 5.0.0\n"
            ),
            Some("v1.17.0".to_owned())
        );
        assert_eq!(parse_firecracker_version(""), None);
        assert_eq!(
            parse_firecracker_version("\nrunning 1 test\ntest x ... Firecracker v9.9.9-fake\n"),
            Some("v9.9.9-fake".to_owned())
        );
        assert_eq!(
            parse_firecracker_version("v1.0.0"),
            Some("v1.0.0".to_owned())
        );
    }

    #[test]
    fn resolve_command_finds_sh_and_rejects_missing() {
        assert!(resolve_command(Path::new("sh")).is_some());
        assert!(resolve_command(Path::new("definitely-not-a-command-xyz")).is_none());
        assert!(resolve_command(Path::new("/definitely/not/here")).is_none());
    }

    #[tokio::test]
    async fn digest_cache_hits_until_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("k");
        std::fs::write(&f, b"hello").unwrap();
        let cache = DigestCache::default();
        let a = cache.digest(&f).await.unwrap();
        assert_eq!(
            a,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(cache.digest(&f).await.unwrap(), a);
        std::fs::write(&f, b"hello!").unwrap();
        assert_ne!(cache.digest(&f).await.unwrap(), a);
    }

    #[tokio::test]
    async fn preflight_reports_structured_failures_on_any_host() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = FirecrackerConfig {
            firecracker_binary: dir.path().join("no-firecracker"),
            kernel: dir.path().join("no-vmlinux"),
            rootfs: dir.path().join("no-rootfs"),
            workdir: dir.path().join("run"),
            vsock_port: 5000,
            ..Default::default()
        };
        let report = run_preflight(&cfg, None, &DigestCache::default()).await;
        assert_eq!(report.provider, "firecracker");
        let names: Vec<&str> = report.checks.iter().map(|c| c.name.as_str()).collect();
        for expected in [
            "os",
            "arch",
            "kvm",
            "firecracker_binary",
            "kernel",
            "rootfs",
            "mkfs_ext4",
            "workdir",
            "socket_path_length",
        ] {
            assert!(names.contains(&expected), "missing check {expected}");
        }
        // Missing files must be reported as failures with a reason.
        for n in ["firecracker_binary", "kernel", "rootfs"] {
            let c = report.checks.iter().find(|c| c.name == n).unwrap();
            assert!(!c.ok, "{n} should fail");
            assert!(!c.detail.is_empty());
        }
        assert!(!report.ok);
        assert_eq!(report.ok, report.checks.iter().all(|c| c.ok));
        // The workdir probe creates the directory and cleans its probe file.
        let workdir = report.checks.iter().find(|c| c.name == "workdir").unwrap();
        assert!(workdir.ok, "{}", workdir.detail);
        assert!(
            std::fs::read_dir(dir.path().join("run"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}
