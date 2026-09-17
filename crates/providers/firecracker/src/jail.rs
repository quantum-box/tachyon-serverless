//! Firecracker `jailer` launch (PLT-4622).
//!
//! With `[provider.firecracker.jailer]` the VMM is not started directly:
//!
//! ```text
//! jailer --id <instance id> --exec-file <firecracker> --uid U --gid G
//!        --chroot-base-dir <base> [--new-pid-ns]
//!        -- --api-sock /fc.sock --log-path /fc.log --level Warning
//! ```
//!
//! The jailer (running as root) creates `<base>/<exec name>/<id>/root`,
//! copies the Firecracker binary into it, creates `/dev/kvm`,
//! `/dev/net/tun`, `/dev/urandom` and `/dev/userfaultfd` owned by U:G,
//! unshares the mount namespace and `pivot_root`s into the chroot, optionally
//! clones into a new PID namespace, drops to U:G with no capabilities and
//! execs Firecracker, which then installs its seccomp filters.
//!
//! Before that, the provider hard-links everything the VMM opens into the
//! chroot ([`prepare`]): kernel and rootfs (read-only for U), the function
//! drive (read-only), the scratch drive (owned by U, 0600) and `fc.log`
//! (owned by U). API and vsock sockets live in the chroot; the host-side
//! vsock listener is `chown`ed to U so the VMM may connect to it.
//!
//! Network: no `--netns` is used. The tap and its nftables chain stay in the
//! host namespace (crate::network) and the tap is created with owner U:G so
//! the unprivileged VMM may attach to it.

use std::ffi::OsString;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::config::{FirecrackerConfig, JailerConfig};

/// Paths of one environment's jail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JailLayout {
    /// `<base>/<exec name>/<instance id>`: removed on terminate.
    pub jail_dir: PathBuf,
    /// `<jail_dir>/root`: `/` of the VMM.
    pub root: PathBuf,
    /// File name of the Firecracker binary (the jailer's directory level).
    pub exec_name: String,
}

/// In-chroot names (the VMM sees them as `/<name>`).
pub const KERNEL: &str = "vmlinux";
pub const ROOTFS: &str = "rootfs.ext4";
pub const FUNCTION_DRIVE: &str = "function.ext4";
pub const SCRATCH_DRIVE: &str = "scratch.ext4";
pub const FC_LOG: &str = "fc.log";
pub const API_SOCK: &str = "fc.sock";
pub const VSOCK_UDS: &str = "v.sock";

/// File name the jailer uses for the chroot level and the in-jail binary.
pub fn exec_name(firecracker_binary: &Path) -> Result<String, String> {
    let name = firecracker_binary
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            format!(
                "firecracker_binary {} has no file name",
                firecracker_binary.display()
            )
        })?;
    // Enforced by the jailer itself; checked here to fail early and clearly.
    if !name.contains("firecracker") {
        return Err(format!(
            "the jailer only execs a file whose name contains `firecracker` (got `{name}`)"
        ));
    }
    Ok(name.to_owned())
}

pub fn layout(jailer: &JailerConfig, exec_name: &str, instance_id: &str) -> JailLayout {
    let jail_dir = jailer.chroot_base.join(exec_name).join(instance_id);
    JailLayout {
        root: jail_dir.join("root"),
        jail_dir,
        exec_name: exec_name.to_owned(),
    }
}

impl JailLayout {
    /// Path of an in-chroot file as seen from the host.
    pub fn host(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    /// Path of an in-chroot file as the VMM sees it.
    pub fn guest(name: &str) -> PathBuf {
        Path::new("/").join(name)
    }

    /// Where the jailer records the VMM pid when it clones into a new PID
    /// namespace (`<root>/<exec name>.pid`).
    pub fn pid_file(&self) -> PathBuf {
        self.root.join(format!("{}.pid", self.exec_name))
    }
}

/// Arguments of the jailer; the Firecracker arguments follow `--`.
pub fn jailer_args(
    jailer: &JailerConfig,
    firecracker_binary: &Path,
    instance_id: &str,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "--id".into(),
        instance_id.into(),
        "--exec-file".into(),
        firecracker_binary.into(),
        "--uid".into(),
        jailer.uid.to_string().into(),
        "--gid".into(),
        jailer.gid.to_string().into(),
        "--chroot-base-dir".into(),
        jailer.chroot_base.clone().into(),
    ];
    if jailer.new_pid_ns {
        args.push("--new-pid-ns".into());
    }
    args.extend(
        [
            "--",
            "--api-sock",
            "/fc.sock",
            "--log-path",
            "/fc.log",
            "--level",
            "Warning",
        ]
        .map(OsString::from),
    );
    args
}

/// Files hard-linked into the chroot.
#[derive(Debug, Clone)]
pub struct JailInputs<'a> {
    pub kernel: &'a Path,
    pub rootfs: &'a Path,
    pub function_drive: &'a Path,
    pub scratch_drive: &'a Path,
    pub fc_log: &'a Path,
}

/// Create the chroot and hard-link the VMM's files into it. Hard links (not
/// copies) keep the host disk budget unchanged; a chroot on another file
/// system is refused.
pub fn prepare(
    jailer: &JailerConfig,
    l: &JailLayout,
    inputs: &JailInputs<'_>,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(&l.root).map_err(|e| format!("mkdir {}: {e}", l.root.display()))?;
    for (src, name) in [
        (inputs.kernel, KERNEL),
        (inputs.rootfs, ROOTFS),
        (inputs.function_drive, FUNCTION_DRIVE),
        (inputs.scratch_drive, SCRATCH_DRIVE),
        (inputs.fc_log, FC_LOG),
    ] {
        let dst = l.host(name);
        std::fs::hard_link(src, &dst).map_err(|e| {
            if e.raw_os_error() == Some(libc::EXDEV) {
                format!(
                    "cannot hard-link {} into {}: chroot_base must be on the same file system \
                     as workdir, kernel and rootfs",
                    src.display(),
                    l.root.display()
                )
            } else {
                format!("hard-link {} -> {}: {e}", src.display(), dst.display())
            }
        })?;
    }
    // Read-only inputs must be readable by the jailed uid; they are never
    // chowned (the kernel and rootfs inodes are shared with every environment).
    for name in [KERNEL, ROOTFS, FUNCTION_DRIVE] {
        let p = l.host(name);
        let mode = std::fs::metadata(&p)
            .map_err(|e| format!("stat {}: {e}", p.display()))?
            .mode();
        if mode & 0o004 == 0 {
            return Err(format!(
                "{} (mode {:o}) is not world-readable; the jailed VMM (uid {}) cannot open it",
                p.display(),
                mode & 0o777,
                jailer.uid
            ));
        }
    }
    // Writable, per-environment files belong to the jailed uid only.
    for name in [SCRATCH_DRIVE, FC_LOG] {
        let p = l.host(name);
        chown(&p, jailer.uid, jailer.gid)?;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("chmod {}: {e}", p.display()))?;
    }
    Ok(())
}

/// `chown` a path (not following symlinks).
pub fn chown(path: &Path, uid: u32, gid: u32) -> Result<(), String> {
    std::os::unix::fs::lchown(path, Some(uid), Some(gid))
        .map_err(|e| format!("chown {uid}:{gid} {}: {e}", path.display()))
}

/// Pid of the VMM recorded by a jailer that cloned into a new PID namespace.
pub fn read_vmm_pid(l: &JailLayout) -> Option<u32> {
    std::fs::read_to_string(l.pid_file())
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Remove a jail directory. Refuses anything that is not
/// `<chroot_base>/<exec name>/<id>`; removes the `<exec name>` level once it
/// is empty.
pub fn remove(jailer: &JailerConfig, l: &JailLayout) -> Result<bool, String> {
    let expected_parent = jailer.chroot_base.join(&l.exec_name);
    if l.jail_dir.parent() != Some(expected_parent.as_path()) {
        return Err(format!(
            "refusing to remove {}: not under {}",
            l.jail_dir.display(),
            expected_parent.display()
        ));
    }
    let removed = match std::fs::remove_dir_all(&l.jail_dir) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(format!("remove {}: {e}", l.jail_dir.display())),
    };
    // ENOTEMPTY while other environments run.
    let _ = std::fs::remove_dir(&expected_parent);
    Ok(removed)
}

/// Remove jails whose instance id is not in `live`.
pub fn sweep(jailer: &JailerConfig, exec_name: &str, live: &[String]) -> Vec<String> {
    let mut removed = Vec::new();
    let Ok(rd) = std::fs::read_dir(jailer.chroot_base.join(exec_name)) else {
        return removed;
    };
    let orphans: Vec<String> = rd
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|name| !live.contains(name))
        .collect();
    for id in orphans {
        let l = layout(jailer, exec_name, &id);
        match remove(jailer, &l) {
            Ok(true) => removed.push(format!("jail:{}", l.jail_dir.display())),
            Ok(false) => {}
            Err(e) => {
                tracing::error!(jail = %l.jail_dir.display(), error = %e, "orphan jail not removed")
            }
        }
    }
    let _ = std::fs::remove_dir(jailer.chroot_base.join(exec_name));
    removed
}

/// Whether the jailer can be used with this configuration. `Err` carries the
/// reason.
pub fn host_support(cfg: &FirecrackerConfig, jailer: &JailerConfig) -> Result<String, String> {
    if !cfg!(target_os = "linux") {
        return Err(format!(
            "the jailer needs Linux (host os is {})",
            std::env::consts::OS
        ));
    }
    // SAFETY: geteuid has no preconditions.
    let euid = unsafe { libc::geteuid() };
    if euid != 0 {
        return Err(format!(
            "the jailer must be started as root (gateway euid is {euid})"
        ));
    }
    if jailer.uid == 0 || jailer.gid == 0 {
        return Err("jailer uid / gid must not be 0".into());
    }
    let binary = crate::preflight::resolve_command(&jailer.binary)
        .filter(|p| crate::preflight::access(p, libc::X_OK))
        .ok_or_else(|| {
            format!(
                "jailer binary {} not found or not executable",
                jailer.binary.display()
            )
        })?;
    if !cfg.firecracker_binary.is_absolute() {
        return Err(format!(
            "firecracker_binary {} must resolve to an absolute path for the jailer",
            cfg.firecracker_binary.display()
        ));
    }
    let exec = exec_name(&cfg.firecracker_binary)?;
    std::fs::create_dir_all(&jailer.chroot_base)
        .map_err(|e| format!("mkdir {}: {e}", jailer.chroot_base.display()))?;
    let dev = |p: &Path| {
        std::fs::metadata(p)
            .map(|m| m.dev())
            .map_err(|e| format!("stat {}: {e}", p.display()))
    };
    let base_dev = dev(&jailer.chroot_base)?;
    for p in [&cfg.workdir, &cfg.kernel, &cfg.rootfs] {
        if p.exists() && dev(p)? != base_dev {
            return Err(format!(
                "{} is on another file system than chroot_base {} (files are hard-linked into the chroot)",
                p.display(),
                jailer.chroot_base.display()
            ));
        }
    }
    Ok(format!(
        "{} -> {}/{exec}/<id>/root as uid {} gid {}{}",
        binary.display(),
        jailer.chroot_base.display(),
        jailer.uid,
        jailer.gid,
        if jailer.new_pid_ns {
            ", new PID namespace"
        } else {
            ""
        }
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jailer(base: &Path) -> JailerConfig {
        JailerConfig {
            binary: PathBuf::from("/usr/bin/jailer"),
            uid: 64000,
            gid: 64001,
            chroot_base: base.to_path_buf(),
            new_pid_ns: true,
        }
    }

    #[test]
    fn args_put_firecracker_flags_after_the_separator() {
        let j = jailer(Path::new("/srv/jailer"));
        let args: Vec<String> = jailer_args(&j, Path::new("/opt/fc/firecracker"), "env-01")
            .into_iter()
            .map(|a| a.into_string().unwrap())
            .collect();
        assert_eq!(
            args,
            [
                "--id",
                "env-01",
                "--exec-file",
                "/opt/fc/firecracker",
                "--uid",
                "64000",
                "--gid",
                "64001",
                "--chroot-base-dir",
                "/srv/jailer",
                "--new-pid-ns",
                "--",
                "--api-sock",
                "/fc.sock",
                "--log-path",
                "/fc.log",
                "--level",
                "Warning"
            ]
        );
        let no_ns = JailerConfig {
            new_pid_ns: false,
            ..j
        };
        assert!(
            !jailer_args(&no_ns, Path::new("/x/firecracker"), "e")
                .iter()
                .any(|a| a == "--new-pid-ns")
        );
    }

    #[test]
    fn layout_and_exec_name() {
        assert_eq!(
            exec_name(Path::new("/a/firecracker-v1.17.0-aarch64")).unwrap(),
            "firecracker-v1.17.0-aarch64"
        );
        assert!(exec_name(Path::new("/a/fc")).is_err());
        let l = layout(&jailer(Path::new("/srv/jailer")), "firecracker", "env-1");
        assert_eq!(l.jail_dir, PathBuf::from("/srv/jailer/firecracker/env-1"));
        assert_eq!(l.root, PathBuf::from("/srv/jailer/firecracker/env-1/root"));
        assert_eq!(
            l.pid_file(),
            PathBuf::from("/srv/jailer/firecracker/env-1/root/firecracker.pid")
        );
        assert_eq!(JailLayout::guest(API_SOCK), PathBuf::from("/fc.sock"));
    }

    #[test]
    fn prepare_links_and_remove_stays_inside_the_base() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let mk = |n: &str| {
            let p = src.join(n);
            std::fs::write(&p, n).unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
            p
        };
        let (k, r, f, s, log) = (mk("k"), mk("r"), mk("f"), mk("s"), mk("l"));
        let base = dir.path().join("jail");
        // Use the current ids: chown to yourself is always allowed.
        let j = JailerConfig {
            // SAFETY: getuid/getgid have no preconditions.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            ..jailer(&base)
        };
        let l = layout(&j, "firecracker", "env-1");
        prepare(
            &j,
            &l,
            &JailInputs {
                kernel: &k,
                rootfs: &r,
                function_drive: &f,
                scratch_drive: &s,
                fc_log: &log,
            },
        )
        .unwrap();
        assert_eq!(std::fs::read_to_string(l.host(ROOTFS)).unwrap(), "r");
        assert_eq!(std::fs::metadata(l.host(SCRATCH_DRIVE)).unwrap().nlink(), 2);
        assert_eq!(std::fs::metadata(&s).unwrap().mode() & 0o777, 0o600);

        let escaped = JailLayout {
            jail_dir: dir.path().join("src"),
            ..l.clone()
        };
        assert!(remove(&j, &escaped).is_err());
        assert!(src.exists());

        assert_eq!(
            sweep(&j, "firecracker", &["env-1".into()]),
            Vec::<String>::new()
        );
        assert!(l.root.exists());
        assert_eq!(sweep(&j, "firecracker", &[]).len(), 1);
        assert!(!l.jail_dir.exists());
        assert!(!base.join("firecracker").exists());
        assert!(k.exists(), "the source of a hard link survives");
        assert!(!remove(&j, &l).unwrap());
    }
}
