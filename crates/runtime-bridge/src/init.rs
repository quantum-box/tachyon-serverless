//! PID 1 duties inside a Firecracker guest (docs/protocol.md section C).
//!
//! The kernel cmdline parser is platform independent so it can be unit
//! tested anywhere; the mount / reboot calls are Linux only.

/// Values read from the kernel cmdline (`tachyon.*` keys).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootParams {
    pub env_id: Option<String>,
    pub vsock_port: Option<u32>,
    pub function_dev: Option<String>,
    /// Writable scratch drive mounted at `/tmp` (PLT-4622). Its size is the
    /// revision's `ephemeral_storage_mib`; without it `/tmp` is a tmpfs.
    pub scratch_dev: Option<String>,
}

/// Parse `key=value` tokens from `/proc/cmdline`.
pub fn parse_cmdline(cmdline: &str) -> BootParams {
    let mut params = BootParams::default();
    for token in cmdline.split_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        match key {
            "tachyon.env_id" => params.env_id = Some(value.to_string()),
            "tachyon.vsock_port" => params.vsock_port = value.parse().ok(),
            "tachyon.function_dev" => params.function_dev = Some(value.to_string()),
            "tachyon.scratch_dev" => params.scratch_dev = Some(value.to_string()),
            _ => {}
        }
    }
    params
}

/// `/proc/sys/kernel/random/boot_id` when readable (Linux).
pub fn read_boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Mount point of the function drive inside the guest.
pub const FUNCTION_MOUNT: &str = "/function";
/// Mount point of the scratch drive (or the tmpfs fallback) inside the guest.
pub const SCRATCH_MOUNT: &str = "/tmp";

#[cfg(target_os = "linux")]
pub use linux::{power_off, setup};

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::CString;

    use super::{BootParams, FUNCTION_MOUNT, SCRATCH_MOUNT, parse_cmdline};

    /// Mount the pseudo file systems, read the cmdline, mount the function
    /// drive read-only and the scratch drive read-write at `/tmp`.
    ///
    /// The root file system and the function drive are attached read-only by
    /// the host, so the scratch drive is the only storage a guest can write
    /// that is backed by the host disk, and its size is fixed by the host
    /// (PLT-4622). A scratch drive that the host announced but that cannot be
    /// mounted is an init error: falling back to a tmpfs would silently change
    /// the storage limit the revision asked for.
    pub fn setup() -> std::io::Result<BootParams> {
        mount("proc", "/proc", "proc", 0)?;
        mount("sysfs", "/sys", "sysfs", 0)?;
        mount("devtmpfs", "/dev", "devtmpfs", 0)?;
        // The Runtime API is served on 127.0.0.1; a fresh kernel leaves `lo`
        // down, so the user process would get ENETUNREACH.
        bring_up_loopback()?;
        let cmdline = std::fs::read_to_string("/proc/cmdline")?;
        let params = parse_cmdline(&cmdline);
        if let Some(dev) = &params.function_dev {
            mount(
                dev,
                FUNCTION_MOUNT,
                "ext4",
                libc::MS_RDONLY | libc::MS_NOSUID,
            )?;
        }
        match &params.scratch_dev {
            Some(dev) => {
                mount(dev, SCRATCH_MOUNT, "ext4", libc::MS_NOSUID | libc::MS_NODEV)?;
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(SCRATCH_MOUNT, std::fs::Permissions::from_mode(0o1777))?;
            }
            // Older hosts: a tmpfs, bounded by the guest memory.
            None => mount("tmpfs", SCRATCH_MOUNT, "tmpfs", 0)?,
        }
        Ok(params)
    }

    fn mount(
        source: &str,
        target: &str,
        fstype: &str,
        flags: libc::c_ulong,
    ) -> std::io::Result<()> {
        let _ = std::fs::create_dir_all(target);
        let source_c = CString::new(source)?;
        let target_c = CString::new(target)?;
        let fstype_c = CString::new(fstype)?;
        // SAFETY: valid NUL-terminated strings; no data argument.
        let rc = unsafe {
            libc::mount(
                source_c.as_ptr(),
                target_c.as_ptr(),
                fstype_c.as_ptr(),
                flags,
                std::ptr::null(),
            )
        };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            // Already mounted (e.g. the kernel pre-mounted devtmpfs).
            if err.raw_os_error() == Some(libc::EBUSY) {
                return Ok(());
            }
            return Err(std::io::Error::new(
                err.kind(),
                format!("mount {fstype} on {target}: {err}"),
            ));
        }
        Ok(())
    }

    /// Set IFF_UP on `lo`. The kernel assigns 127.0.0.1/8 automatically once
    /// the loopback device comes up.
    fn bring_up_loopback() -> std::io::Result<()> {
        let ctx = |e: std::io::Error| std::io::Error::new(e.kind(), format!("bring up lo: {e}"));
        // SAFETY: plain socket/ioctl calls on a zeroed, correctly sized ifreq.
        unsafe {
            let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
            if fd < 0 {
                return Err(ctx(std::io::Error::last_os_error()));
            }
            let mut ifr: libc::ifreq = std::mem::zeroed();
            for (dst, src) in ifr.ifr_name.iter_mut().zip(b"lo\0".iter()) {
                *dst = *src as libc::c_char;
            }
            if libc::ioctl(fd, libc::SIOCGIFFLAGS as _, &mut ifr) < 0 {
                let e = std::io::Error::last_os_error();
                libc::close(fd);
                return Err(ctx(e));
            }
            ifr.ifr_ifru.ifru_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
            if libc::ioctl(fd, libc::SIOCSIFFLAGS as _, &ifr) < 0 {
                let e = std::io::Error::last_os_error();
                libc::close(fd);
                return Err(ctx(e));
            }
            libc::close(fd);
        }
        Ok(())
    }

    /// Flush and power the guest off. Only returns if `reboot` fails, in
    /// which case the process exits with `code` (as PID 1 this panics the
    /// kernel, which Firecracker treats as a stop).
    pub fn power_off(code: i32) -> ! {
        // SAFETY: plain syscalls with constant arguments.
        unsafe {
            libc::sync();
            libc::reboot(libc::RB_POWER_OFF);
        }
        std::process::exit(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tachyon_keys() {
        let p = parse_cmdline(
            "console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init tachyon.env_id=env_01abc tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb tachyon.scratch_dev=/dev/vdc\n",
        );
        assert_eq!(
            p,
            BootParams {
                env_id: Some("env_01abc".into()),
                vsock_port: Some(5000),
                function_dev: Some("/dev/vdb".into()),
                scratch_dev: Some("/dev/vdc".into()),
            }
        );
        assert_eq!(parse_cmdline("quiet"), BootParams::default());
        assert_eq!(parse_cmdline("tachyon.vsock_port=x").vsock_port, None);
    }
}
