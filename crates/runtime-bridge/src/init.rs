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

#[cfg(target_os = "linux")]
pub use linux::{power_off, setup};

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::CString;

    use super::{BootParams, FUNCTION_MOUNT, parse_cmdline};

    /// Mount the pseudo file systems, read the cmdline and mount the
    /// function drive read-only.
    pub fn setup() -> std::io::Result<BootParams> {
        mount("proc", "/proc", "proc", 0)?;
        mount("sysfs", "/sys", "sysfs", 0)?;
        mount("devtmpfs", "/dev", "devtmpfs", 0)?;
        mount("tmpfs", "/tmp", "tmpfs", 0)?;
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
        Ok(params)
    }

    fn mount(
        source: &str,
        target: &str,
        fstype: &str,
        flags: libc::c_ulong,
    ) -> std::io::Result<()> {
        let _ = std::fs::create_dir_all(target);
        let source = CString::new(source)?;
        let target_c = CString::new(target)?;
        let fstype = CString::new(fstype)?;
        // SAFETY: valid NUL-terminated strings; no data argument.
        let rc = unsafe {
            libc::mount(
                source.as_ptr(),
                target_c.as_ptr(),
                fstype.as_ptr(),
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
            "console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init tachyon.env_id=env_01abc tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb\n",
        );
        assert_eq!(
            p,
            BootParams {
                env_id: Some("env_01abc".into()),
                vsock_port: Some(5000),
                function_dev: Some("/dev/vdb".into()),
            }
        );
        assert_eq!(parse_cmdline("quiet"), BootParams::default());
        assert_eq!(parse_cmdline("tachyon.vsock_port=x").vsock_port, None);
    }
}
