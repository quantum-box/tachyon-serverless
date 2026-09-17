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
