//! Provider configuration.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::network::NetworkConfig;

/// Default [`FirecrackerConfig::console_log_max_bytes`]: 4 MiB.
pub const DEFAULT_CONSOLE_LOG_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// Default [`FirecrackerConfig::fc_log_max_bytes`]: 4 MiB.
pub const DEFAULT_FC_LOG_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// Default [`FirecrackerConfig::min_host_free_bytes`]: 512 MiB.
pub const DEFAULT_MIN_HOST_FREE_BYTES: u64 = 512 * 1024 * 1024;

/// Configuration of the Firecracker provider. Mirrors the
/// `[provider.firecracker]` section of the gateway config (docs/architecture.md §4).
///
/// The gateway constructs it with the five mandatory fields and
/// `..Default::default()` for the rest.
#[derive(Debug, Clone)]
pub struct FirecrackerConfig {
    /// Path of the `firecracker` binary (absolute, relative to the cwd, or a
    /// bare name looked up in `PATH`).
    pub firecracker_binary: PathBuf,
    /// Guest kernel (uncompressed `vmlinux`).
    pub kernel: PathBuf,
    /// Read-only base rootfs (ext4) containing `/sbin/tachyon-init`.
    pub rootfs: PathBuf,
    /// Per-environment working directory. Each environment gets
    /// `<workdir>/<env_id>/`; archived logs live under `<workdir>/_archive/`.
    pub workdir: PathBuf,
    /// vsock port the guest bridge connects to (host listens on
    /// `<env_dir>/v.sock_<port>`).
    pub vsock_port: u32,
    /// Extra kernel command line appended after the mandatory arguments.
    pub boot_args_extra: Option<String>,
    /// `mkfs.ext4` binary used to build the function drive.
    pub mkfs_ext4: PathBuf,
    /// Grace period given to a VM that is expected to power off by itself
    /// (`TerminateReason::Completed` / `Shutdown`) before SIGKILL.
    pub kill_grace: Duration,
    /// Cap on `console.log` (the guest serial console) per environment. The
    /// console is read through a pipe; bytes past the cap are drained and
    /// discarded, so a guest that floods its serial port cannot fill the host
    /// disk (PLT-4622). A one-line marker is written when the cap is reached.
    pub console_log_max_bytes: u64,
    /// Cap on the allocated size of `fc.log` per environment. Firecracker
    /// writes it itself, so it is enforced by a watchdog that truncates the
    /// file once it holds more than this (checked every second).
    pub fc_log_max_bytes: u64,
    /// Free space that must remain in `workdir`'s file system after an
    /// environment's host-side budget (staged artifact, function drive,
    /// scratch drive, log caps) is set aside. Below it `create_environment`
    /// fails closed with `Unavailable` before anything is written.
    pub min_host_free_bytes: u64,
    /// Host network used by the `restricted` / `public-web` egress profiles
    /// (PLT-4622, docs/adr/0005-egress-profiles.md). Only touched for
    /// environments that ask for egress; `none` never creates a device.
    pub network: NetworkConfig,
    /// Host-side cgroup v2 limits per VMM (PLT-4622, [`crate::cgroup`]).
    pub cgroup: CgroupConfig,
    /// Launch the VMM through Firecracker's `jailer` (PLT-4622,
    /// [`crate::jail`]). `None` runs `firecracker` directly as the gateway's
    /// user, which is meant for development.
    pub jailer: Option<JailerConfig>,
}

/// What to do when host cgroup v2 limits cannot be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CgroupMode {
    /// Every environment gets its cgroup, or it is not created
    /// (`ProviderError::Unavailable`); preflight fails without delegation.
    Required,
    /// Apply the limits when the host allows it, otherwise boot without them
    /// (logged, and reported in the capabilities).
    #[default]
    BestEffort,
    /// Never touch cgroups.
    Off,
}

impl CgroupMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::BestEffort => "best-effort",
            Self::Off => "off",
        }
    }
}

/// Default [`CgroupConfig::root`].
pub const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup";
/// Default [`CgroupConfig::parent`].
pub const DEFAULT_CGROUP_PARENT: &str = "tachyon";
/// Default [`CgroupConfig::cpu_period_us`]: the CFS default of 100 ms.
pub const DEFAULT_CPU_PERIOD_US: u64 = 100_000;
/// Default [`CgroupConfig::memory_overhead_mib`] (docs/kvm.md §3.6 records
/// the VMM overhead measured against it).
pub const DEFAULT_MEMORY_OVERHEAD_MIB: u64 = 64;
/// Default [`CgroupConfig::pids_max`].
pub const DEFAULT_PIDS_MAX: u64 = 64;

/// Host-side cgroup v2 limits of each VMM:
/// `<root>/<parent>/<env_id>/{cpu.max, memory.max, pids.max}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupConfig {
    pub mode: CgroupMode,
    /// A cgroup v2 hierarchy this process may create children in and move
    /// processes into (the mount root, or a delegated subtree).
    pub root: PathBuf,
    /// Directory under `root` that holds one cgroup per environment.
    pub parent: String,
    /// CFS period written to `cpu.max`; the quota is
    /// `cpu_millis * period / 1000`, shared by all vCPUs and VMM threads.
    pub cpu_period_us: u64,
    /// Added to the guest memory for `memory.max` (VMM heap, device
    /// emulation, page cache of the drives).
    pub memory_overhead_mib: u64,
    /// `pids.max` (VMM threads plus the jailer).
    pub pids_max: u64,
}

impl Default for CgroupConfig {
    fn default() -> Self {
        Self {
            mode: CgroupMode::default(),
            root: PathBuf::from(DEFAULT_CGROUP_ROOT),
            parent: DEFAULT_CGROUP_PARENT.to_owned(),
            cpu_period_us: DEFAULT_CPU_PERIOD_US,
            memory_overhead_mib: DEFAULT_MEMORY_OVERHEAD_MIB,
            pids_max: DEFAULT_PIDS_MAX,
        }
    }
}

/// Default [`JailerConfig::chroot_base`] (the jailer's own default).
pub const DEFAULT_CHROOT_BASE: &str = "/srv/jailer";

/// Firecracker `jailer` settings. The gateway must run as root: the jailer
/// creates the chroot and device nodes, then drops to `uid` / `gid`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JailerConfig {
    /// Path of the `jailer` binary (same Firecracker release as the VMM).
    pub binary: PathBuf,
    /// Unprivileged uid / gid the VMM runs as. Must not be 0.
    pub uid: u32,
    pub gid: u32,
    /// `<chroot_base>/<firecracker file name>/<instance id>/root` is the
    /// chroot of one environment. It must be on the same file system as
    /// `workdir`, `kernel` and `rootfs`: files are hard-linked into it.
    pub chroot_base: PathBuf,
    /// `--new-pid-ns`: the VMM runs as pid 1 of its own PID namespace.
    pub new_pid_ns: bool,
}

impl Default for JailerConfig {
    fn default() -> Self {
        Self {
            binary: PathBuf::from("jailer"),
            uid: 64000,
            gid: 64000,
            chroot_base: PathBuf::from(DEFAULT_CHROOT_BASE),
            new_pid_ns: true,
        }
    }
}

impl Default for FirecrackerConfig {
    fn default() -> Self {
        Self {
            firecracker_binary: PathBuf::from("firecracker"),
            kernel: PathBuf::from(".kvm/vmlinux"),
            rootfs: PathBuf::from(".kvm/rootfs.ext4"),
            workdir: PathBuf::from(".kvm/run"),
            vsock_port: tachyon_serverless_protocol::DEFAULT_VSOCK_PORT,
            boot_args_extra: None,
            mkfs_ext4: PathBuf::from("mkfs.ext4"),
            kill_grace: Duration::from_secs(2),
            console_log_max_bytes: DEFAULT_CONSOLE_LOG_MAX_BYTES,
            fc_log_max_bytes: DEFAULT_FC_LOG_MAX_BYTES,
            min_host_free_bytes: DEFAULT_MIN_HOST_FREE_BYTES,
            network: NetworkConfig::default(),
            cgroup: CgroupConfig::default(),
            jailer: None,
        }
    }
}

impl FirecrackerConfig {
    /// Make every path absolute (relative to the current directory) so that
    /// later `chdir`s or child processes with a different cwd cannot change
    /// their meaning. Bare command names (no path separator) are left alone
    /// so they are still resolved through `PATH`.
    pub(crate) fn absolutized(mut self) -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let abs = |p: &Path| -> PathBuf {
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                cwd.join(p)
            }
        };
        let is_bare = |p: &Path| p.components().count() == 1 && !p.is_absolute();
        if !is_bare(&self.firecracker_binary) {
            self.firecracker_binary = abs(&self.firecracker_binary);
        }
        if !is_bare(&self.mkfs_ext4) {
            self.mkfs_ext4 = abs(&self.mkfs_ext4);
        }
        if !is_bare(&self.network.nft_binary) {
            self.network.nft_binary = abs(&self.network.nft_binary);
        }
        if !is_bare(&self.network.ip_binary) {
            self.network.ip_binary = abs(&self.network.ip_binary);
        }
        if let Some(j) = self.jailer.as_mut() {
            // The jailer is given absolute paths for both binaries.
            if let Some(resolved) = crate::preflight::resolve_command(&j.binary) {
                j.binary = resolved;
            } else if !is_bare(&j.binary) {
                j.binary = abs(&j.binary);
            }
            j.chroot_base = abs(&j.chroot_base);
            if let Some(resolved) = crate::preflight::resolve_command(&self.firecracker_binary) {
                self.firecracker_binary = resolved;
            }
        }
        self.cgroup.root = abs(&self.cgroup.root);
        self.kernel = abs(&self.kernel);
        self.rootfs = abs(&self.rootfs);
        self.workdir = abs(&self.workdir);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_docs() {
        let c = FirecrackerConfig::default();
        assert_eq!(c.vsock_port, 5000);
        assert_eq!(c.kill_grace, Duration::from_secs(2));
        assert_eq!(c.mkfs_ext4, PathBuf::from("mkfs.ext4"));
        assert!(c.boot_args_extra.is_none());
        assert_eq!(c.console_log_max_bytes, 4 * 1024 * 1024);
        assert_eq!(c.fc_log_max_bytes, 4 * 1024 * 1024);
        assert_eq!(c.min_host_free_bytes, 512 * 1024 * 1024);
        assert_eq!(c.cgroup.mode, CgroupMode::BestEffort);
        assert_eq!(c.cgroup.root, PathBuf::from("/sys/fs/cgroup"));
        assert_eq!(c.cgroup.parent, "tachyon");
        assert_eq!(c.cgroup.cpu_period_us, 100_000);
        assert!(c.jailer.is_none());
        let j = JailerConfig::default();
        assert!(j.new_pid_ns && j.uid != 0 && j.gid != 0);
    }

    #[test]
    fn absolutize_keeps_bare_names_and_absolute_paths() {
        let c = FirecrackerConfig {
            firecracker_binary: PathBuf::from("firecracker"),
            kernel: PathBuf::from("/abs/vmlinux"),
            rootfs: PathBuf::from("rel/rootfs.ext4"),
            workdir: PathBuf::from(".kvm/run"),
            mkfs_ext4: PathBuf::from("/sbin/mkfs.ext4"),
            ..Default::default()
        }
        .absolutized();
        assert_eq!(c.firecracker_binary, PathBuf::from("firecracker"));
        assert_eq!(c.kernel, PathBuf::from("/abs/vmlinux"));
        assert!(c.rootfs.is_absolute());
        assert!(c.rootfs.ends_with("rel/rootfs.ext4"));
        assert!(c.workdir.is_absolute());
        assert_eq!(c.mkfs_ext4, PathBuf::from("/sbin/mkfs.ext4"));
    }
}
