//! Clock readings reported by the guest, so the evidence shows how each
//! clock behaves across a snapshot / restore.

use serde::{Deserialize, Serialize};

/// Three clocks read at the same moment, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Clocks {
    /// `CLOCK_REALTIME` (wall clock), ms since the Unix epoch.
    pub wall_ms: u64,
    /// `CLOCK_MONOTONIC` (stops while the guest is not running).
    pub monotonic_ms: u64,
    /// `CLOCK_BOOTTIME` on Linux (`CLOCK_MONOTONIC` elsewhere).
    pub boottime_ms: u64,
}

fn read(clock: libc::clockid_t) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec.
    let rc = unsafe { libc::clock_gettime(clock, &mut ts) };
    if rc != 0 {
        return 0;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1000)
        .saturating_add((ts.tv_nsec as u64) / 1_000_000)
}

impl Clocks {
    pub fn now() -> Self {
        #[cfg(target_os = "linux")]
        let boot = libc::CLOCK_BOOTTIME;
        #[cfg(not(target_os = "linux"))]
        let boot = libc::CLOCK_MONOTONIC;
        Self {
            wall_ms: read(libc::CLOCK_REALTIME),
            monotonic_ms: read(libc::CLOCK_MONOTONIC),
            boottime_ms: read(boot),
        }
    }
}

/// 16 bytes from `/dev/urandom` as hex (empty when unreadable). Two copies of
/// one snapshot that print the same value share their kernel CRNG state.
pub fn urandom_hex() -> String {
    use std::io::Read;
    let mut buf = [0u8; 16];
    match std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut buf)) {
        Ok(()) => buf.iter().map(|b| format!("{b:02x}")).collect(),
        Err(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clocks_are_populated_and_random_differs() {
        let c = Clocks::now();
        assert!(c.wall_ms > 1_600_000_000_000);
        assert!(c.monotonic_ms > 0);
        let a = urandom_hex();
        assert_eq!(a.len(), 32);
        assert_ne!(a, urandom_hex());
    }
}
