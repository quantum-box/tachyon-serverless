//! VM generation change notifications from the guest kernel.
//!
//! The Linux `vmgenid` driver reseeds the CRNG when the VMM changes the
//! generation id (a restore) and emits a `KOBJ_CHANGE` uevent carrying
//! `NEW_VMGENID=1`. Listening for it lets a restored guest react at once
//! instead of when its next vsock write fails.

/// Whether a raw uevent datagram (NUL-separated `KEY=VALUE` fields) reports
/// a VM generation change.
pub fn is_vmgenid_change(datagram: &[u8]) -> bool {
    datagram
        .split(|b| *b == 0)
        .any(|field| field.starts_with(b"NEW_VMGENID="))
}

/// Block on the kernel uevent netlink socket and call `on_change` for every
/// VM generation change. Returns only if the socket fails.
#[cfg(target_os = "linux")]
pub fn watch_vmgenid(mut on_change: impl FnMut()) -> std::io::Error {
    use std::mem::{size_of, zeroed};
    // SAFETY: plain socket / bind / recv on a zeroed, correctly sized
    // sockaddr_nl and a stack buffer whose length is passed along.
    unsafe {
        let fd = libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
            libc::NETLINK_KOBJECT_UEVENT,
        );
        if fd < 0 {
            return std::io::Error::last_os_error();
        }
        let mut addr: libc::sockaddr_nl = zeroed();
        addr.nl_family = libc::AF_NETLINK as libc::sa_family_t;
        addr.nl_groups = 1;
        if libc::bind(
            fd,
            (&addr as *const libc::sockaddr_nl).cast::<libc::sockaddr>(),
            size_of::<libc::sockaddr_nl>() as libc::socklen_t,
        ) != 0
        {
            let e = std::io::Error::last_os_error();
            libc::close(fd);
            return e;
        }
        let mut buf = [0u8; 8192];
        loop {
            let n = libc::recv(fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len(), 0);
            if n < 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                libc::close(fd);
                return e;
            }
            if is_vmgenid_change(&buf[..n as usize]) {
                on_change();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_the_vmgenid_uevent_only() {
        let change = b"change@/devices/platform/vmgenid\0ACTION=change\0DEVPATH=/devices/platform/vmgenid\0SUBSYSTEM=platform\0NEW_VMGENID=1\0SEQNUM=812\0";
        assert!(is_vmgenid_change(change));
        let other = b"add@/devices/virtual/block/loop0\0ACTION=add\0SUBSYSTEM=block\0";
        assert!(!is_vmgenid_change(other));
        assert!(!is_vmgenid_change(b""));
    }
}
