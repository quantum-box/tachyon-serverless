//! Kernel command line composition (docs/protocol.md §C).

use tachyon_serverless_domain::Architecture;

/// Path of the guest init (the runtime bridge in `--init` mode).
pub const GUEST_INIT: &str = "/sbin/tachyon-init";
/// Guest block device the function drive appears as (second virtio-blk).
pub const FUNCTION_DEV: &str = "/dev/vdb";
/// Entrypoint of the user function inside the guest.
pub const GUEST_ENTRYPOINT: &str = "/function/app";

/// Compose the kernel command line for one environment.
///
/// `console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init
/// tachyon.env_id=<id> tachyon.vsock_port=<port> tachyon.function_dev=/dev/vdb`
/// plus ` keep_bootcon` on aarch64, plus `boot_args_extra` (trimmed) if any.
pub fn compose_boot_args(
    arch: Architecture,
    env_id: &str,
    vsock_port: u32,
    extra: Option<&str>,
) -> String {
    let mut s = format!(
        "console=ttyS0 reboot=k panic=1 pci=off init={GUEST_INIT} \
         tachyon.env_id={env_id} tachyon.vsock_port={vsock_port} tachyon.function_dev={FUNCTION_DEV}"
    );
    if arch == Architecture::Aarch64 {
        s.push_str(" keep_bootcon");
    }
    if let Some(extra) = extra.map(str::trim).filter(|e| !e.is_empty()) {
        s.push(' ');
        s.push_str(extra);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x86_64_matches_protocol_doc() {
        let s = compose_boot_args(Architecture::X86_64, "env_01abc", 5000, None);
        assert_eq!(
            s,
            "console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init \
             tachyon.env_id=env_01abc tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb"
        );
    }

    #[test]
    fn aarch64_adds_keep_bootcon_and_extra_is_appended() {
        let s = compose_boot_args(Architecture::Aarch64, "env_x", 5001, Some("  loglevel=8 "));
        assert!(s.ends_with(" keep_bootcon loglevel=8"));
        assert!(s.contains("tachyon.vsock_port=5001"));
        assert!(s.starts_with("console=ttyS0 reboot=k panic=1 pci=off"));
    }

    #[test]
    fn blank_extra_is_ignored() {
        let a = compose_boot_args(Architecture::X86_64, "e", 5000, Some("   "));
        let b = compose_boot_args(Architecture::X86_64, "e", 5000, None);
        assert_eq!(a, b);
    }
}
