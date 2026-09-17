//! Host-side bounds of one environment (PLT-4622).
//!
//! A guest must not be able to exhaust the host through the artefacts the
//! provider keeps for it. Everything under `<workdir>/<env_id>/` is bounded:
//!
//! | artefact        | bound                                                        |
//! |-----------------|--------------------------------------------------------------|
//! | `function.ext4` | artifact size + 8 MiB, read-only in the guest                |
//! | `scratch.ext4`  | exactly `ephemeral_storage_mib`, reserved up front            |
//! | `console.log`   | [`copy_capped`] through a pipe: `console_log_max_bytes` + marker |
//! | `fc.log`        | [`enforce_file_cap`] watchdog: `fc_log_max_bytes` (+1 s of writes) |
//! | `stage/`        | removed as soon as the function drive is built               |
//!
//! [`check_host_budget`] refuses to create an environment whose budget would
//! leave less than `min_host_free_bytes` free, before anything is written.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// How often the `fc.log` watchdog looks at the file.
pub const LOG_WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);

/// Byte counts of a finished (or running) console capture.
#[derive(Debug, Default)]
pub struct ConsoleCounters {
    pub written: AtomicU64,
    pub dropped: AtomicU64,
    pub finished: AtomicBool,
}

/// Handle on the thread that drains the Firecracker stdout/stderr pipe into
/// `console.log`.
#[derive(Debug, Clone)]
pub struct ConsoleCapture {
    counters: Arc<ConsoleCounters>,
}

impl ConsoleCapture {
    /// Start draining `reader` into `sink` with a cap of `cap` bytes. The
    /// thread ends when every write end of the pipe is closed (the VMM exited).
    pub fn spawn(reader: impl Read + Send + 'static, sink: std::fs::File, cap: u64) -> Self {
        let counters = Arc::new(ConsoleCounters::default());
        let c = counters.clone();
        std::thread::Builder::new()
            .name("fc-console".into())
            .spawn(move || {
                copy_capped(reader, sink, cap, &c);
                c.finished.store(true, Ordering::SeqCst);
            })
            .map(|_| ())
            .unwrap_or_else(|e| {
                // Without a reader the VMM would block on a full pipe; this
                // only happens when the host cannot create threads at all.
                tracing::error!(error = %e, "cannot start the console capture thread");
                counters.finished.store(true, Ordering::SeqCst);
            });
        Self { counters }
    }

    /// Wait (polling) until the pipe was drained to EOF, at most `max`.
    pub async fn wait_finished(&self, max: Duration) -> bool {
        let deadline = std::time::Instant::now() + max;
        while !self.counters.finished.load(Ordering::SeqCst) {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }

    pub fn dropped_bytes(&self) -> u64 {
        self.counters.dropped.load(Ordering::SeqCst)
    }
}

/// Marker written once when the console reaches its cap.
pub fn cap_marker(cap: u64) -> String {
    format!(
        "\n[tachyon] console.log reached its cap of {cap} bytes; further console output is discarded\n"
    )
}

/// Copy `reader` into `sink` until EOF, keeping at most `cap` bytes (plus the
/// marker lines). Past the cap the reader is still drained, so the VMM never
/// blocks on a full pipe; a write error is treated like reaching the cap.
pub fn copy_capped(
    mut reader: impl Read,
    mut sink: impl Write,
    cap: u64,
    counters: &ConsoleCounters,
) {
    let mut buf = [0u8; 64 * 1024];
    let mut written = 0u64;
    let mut dropped = 0u64;
    let mut capped = false;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        let mut chunk = &buf[..n];
        if !capped {
            let room = cap.saturating_sub(written);
            let take = (chunk.len() as u64).min(room) as usize;
            if take > 0 {
                if sink.write_all(&chunk[..take]).is_ok() {
                    written += take as u64;
                    chunk = &chunk[take..];
                } else {
                    capped = true;
                }
            }
            if !chunk.is_empty() && !capped {
                capped = true;
                let _ = sink.write_all(cap_marker(cap).as_bytes());
            }
        }
        dropped += chunk.len() as u64;
        counters.written.store(written, Ordering::SeqCst);
        counters.dropped.store(dropped, Ordering::SeqCst);
    }
    if dropped > 0 {
        let _ = sink.write_all(
            format!("[tachyon] discarded {dropped} bytes of console output\n").as_bytes(),
        );
    }
    let _ = sink.flush();
}

/// Bytes a file really occupies on disk (`st_blocks * 512`), 0 when missing.
pub fn allocated_bytes(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path)
        .map(|m| m.blocks().saturating_mul(512))
        .unwrap_or(0)
}

/// Truncate `path` to zero length when it occupies more than `cap` bytes.
/// Returns whether it was truncated.
///
/// Firecracker opens `--log-path` without `O_APPEND`, so its next write lands
/// at its old offset and the file becomes sparse: the *allocated* size stays
/// bounded, which is what protects the host disk, while the apparent length
/// keeps growing. That is why the check uses allocated blocks.
pub fn enforce_file_cap(path: &Path, cap: u64) -> std::io::Result<bool> {
    if allocated_bytes(path) <= cap {
        return Ok(false);
    }
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)?
        .set_len(0)?;
    Ok(true)
}

/// Keep `path` under `cap` until `dir` disappears (the environment was
/// terminated and its directory removed).
pub fn spawn_log_watchdog(
    path: PathBuf,
    dir: PathBuf,
    cap: u64,
    env_id: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut truncations = 0u64;
        loop {
            tokio::time::sleep(LOG_WATCHDOG_INTERVAL).await;
            if !dir.exists() {
                break;
            }
            match enforce_file_cap(&path, cap) {
                Ok(true) => {
                    truncations += 1;
                    tracing::warn!(
                        env_id,
                        path = %path.display(),
                        cap,
                        truncations,
                        "log exceeded its cap and was truncated"
                    );
                }
                Ok(false) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(env_id, path = %path.display(), error = %e, "log cap check failed")
                }
            }
        }
    })
}

/// Host disk an environment may consume at most.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostBudget {
    pub staged_artifact_bytes: u64,
    pub function_drive_bytes: u64,
    pub scratch_drive_bytes: u64,
    pub console_log_max_bytes: u64,
    pub fc_log_max_bytes: u64,
}

impl HostBudget {
    pub fn total(&self) -> u64 {
        self.staged_artifact_bytes
            .saturating_add(self.function_drive_bytes)
            .saturating_add(self.scratch_drive_bytes)
            .saturating_add(self.console_log_max_bytes)
            .saturating_add(self.fc_log_max_bytes)
    }
}

/// Bytes available to an unprivileged user in the file system holding `path`.
pub fn available_bytes(path: &Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: zeroed statvfs is a valid out-parameter; `c` is NUL-terminated.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    #[allow(clippy::unnecessary_cast)] // the field widths differ per platform
    Ok((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
}

/// Fail when `available` cannot hold `budget` while keeping `reserve` free.
pub fn check_host_budget(available: u64, budget: &HostBudget, reserve: u64) -> Result<(), String> {
    let needed = budget.total().saturating_add(reserve);
    if available < needed {
        return Err(format!(
            "host disk too full for a new environment: {available} bytes available, \
             {} bytes needed for its budget ({budget:?}) plus {reserve} bytes kept free",
            budget.total()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_copy_keeps_the_head_and_drains_the_rest() {
        let input = vec![b'x'; 10_000];
        let mut out = Vec::new();
        let c = ConsoleCounters::default();
        copy_capped(&input[..], &mut out, 1_000, &c);
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with(&"x".repeat(1_000)));
        assert!(text.contains("reached its cap of 1000 bytes"));
        assert!(text.ends_with("[tachyon] discarded 9000 bytes of console output\n"));
        assert_eq!(c.written.load(Ordering::SeqCst), 1_000);
        assert_eq!(c.dropped.load(Ordering::SeqCst), 9_000);
        assert!(
            text.len() < 1_000 + 200,
            "cap plus markers only: {}",
            text.len()
        );
    }

    #[test]
    fn console_copy_under_the_cap_is_verbatim() {
        let mut out = Vec::new();
        let c = ConsoleCounters::default();
        copy_capped(&b"boot ok\n"[..], &mut out, 1_000, &c);
        assert_eq!(out, b"boot ok\n");
        assert_eq!(c.dropped.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_failing_sink_does_not_stop_the_drain() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk full"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let input = vec![0u8; 300_000];
        let c = ConsoleCounters::default();
        copy_capped(&input[..], Broken, 1_000_000, &c);
        assert_eq!(c.dropped.load(Ordering::SeqCst), 300_000);
    }

    #[tokio::test]
    async fn capture_thread_finishes_at_eof() {
        let dir = tempfile::tempdir().unwrap();
        let (r, mut w) = std::io::pipe().unwrap();
        let sink = std::fs::File::create(dir.path().join("console.log")).unwrap();
        let cap = ConsoleCapture::spawn(r, sink, 4);
        w.write_all(b"0123456789").unwrap();
        drop(w);
        assert!(cap.wait_finished(Duration::from_secs(5)).await);
        assert_eq!(cap.dropped_bytes(), 6);
        let text = std::fs::read_to_string(dir.path().join("console.log")).unwrap();
        assert!(text.starts_with("0123\n[tachyon]"), "{text}");
    }

    #[test]
    fn file_cap_truncates_only_above_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("fc.log");
        std::fs::write(&f, vec![b'a'; 64 * 1024]).unwrap();
        assert!(!enforce_file_cap(&f, 1024 * 1024).unwrap());
        assert!(enforce_file_cap(&f, 4096).unwrap());
        assert_eq!(std::fs::metadata(&f).unwrap().len(), 0);
        assert!(!enforce_file_cap(&dir.path().join("missing"), 0).unwrap());
    }

    #[test]
    fn budget_check_keeps_the_reserve() {
        let budget = HostBudget {
            staged_artifact_bytes: 10,
            function_drive_bytes: 20,
            scratch_drive_bytes: 100,
            console_log_max_bytes: 5,
            fc_log_max_bytes: 5,
        };
        assert_eq!(budget.total(), 140);
        assert!(check_host_budget(240, &budget, 100).is_ok());
        let err = check_host_budget(239, &budget, 100).unwrap_err();
        assert!(err.contains("host disk too full"), "{err}");
        assert!(available_bytes(Path::new("/")).unwrap() > 0);
    }
}
