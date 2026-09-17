//! Host-side process helpers: spawning Firecracker in its own process
//! group, killing that group, liveness checks and log tails.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use crate::host_guard::ConsoleCapture;

/// Firecracker's `--id` must match `[A-Za-z0-9-]{1,64}`; environment ids
/// contain an underscore (`env_...`), so it is replaced by a dash.
pub fn instance_id_for(env_id: &str) -> String {
    let mut s: String = env_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    s.truncate(64);
    if s.is_empty() {
        s.push_str("tachyon");
    }
    s
}

/// Files of one environment directory.
#[derive(Debug, Clone)]
pub struct EnvPaths {
    pub dir: PathBuf,
    pub stage: PathBuf,
    pub stage_app: PathBuf,
    pub function_drive: PathBuf,
    pub scratch_drive: PathBuf,
    pub api_sock: PathBuf,
    pub vsock_uds: PathBuf,
    pub vsock_listener: PathBuf,
    pub fc_log: PathBuf,
    pub console_log: PathBuf,
    pub pid_file: PathBuf,
}

impl EnvPaths {
    pub fn new(workdir: &Path, env_id: &str, vsock_port: u32) -> Self {
        let dir = workdir.join(env_id);
        Self {
            stage: dir.join("stage"),
            stage_app: dir.join("stage").join("app"),
            function_drive: dir.join("function.ext4"),
            scratch_drive: dir.join("scratch.ext4"),
            api_sock: dir.join("fc.sock"),
            vsock_uds: dir.join("v.sock"),
            vsock_listener: dir.join(format!("v.sock_{vsock_port}")),
            fc_log: dir.join("fc.log"),
            console_log: dir.join("console.log"),
            pid_file: dir.join("fc.pid"),
            dir,
        }
    }

    /// Longest Unix socket path Firecracker or we will bind/connect.
    pub fn longest_socket_path_len(&self) -> usize {
        [&self.api_sock, &self.vsock_uds, &self.vsock_listener]
            .into_iter()
            .map(|p| p.as_os_str().len())
            .max()
            .unwrap_or(0)
    }
}

/// `sun_path` is 108 bytes on Linux including the terminating NUL.
pub const MAX_UNIX_SOCKET_PATH: usize = 107;

/// Spawn `firecracker --api-sock ... --id ... --log-path ... --level Warning`
/// in a new process group with stdin closed and stdout/stderr (the guest
/// serial console) sent through a pipe that a [`ConsoleCapture`] drains into
/// `console_log`, keeping at most `console_cap` bytes (PLT-4622).
pub fn spawn_firecracker(
    binary: &Path,
    paths: &EnvPaths,
    instance_id: &str,
    console_cap: u64,
) -> std::io::Result<(Child, ConsoleCapture)> {
    // Firecracker opens --log-path without O_CREAT; the file must exist.
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.fc_log)?;
    let console = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.console_log)?;
    // Both ends are close-on-exec; only the dup2'ed stdout/stderr of the
    // child survive the exec, so the pipe reaches EOF when the VMM exits.
    let (console_rx, console_tx) = std::io::pipe()?;
    let console_err = console_tx.try_clone()?;
    let mut cmd = Command::new(binary);
    cmd.arg("--api-sock")
        .arg(&paths.api_sock)
        .arg("--id")
        .arg(instance_id)
        .arg("--log-path")
        .arg(&paths.fc_log)
        .arg("--level")
        .arg("Warning")
        // A terminal on stdin would be switched to raw mode by Firecracker.
        .stdin(Stdio::null())
        .stdout(Stdio::from(console_tx))
        .stderr(Stdio::from(console_err))
        .current_dir(&paths.dir)
        .kill_on_drop(false);
    cmd.process_group(0);
    let child = cmd.spawn()?;
    // Drop our copies of the write end now, or the capture never sees EOF.
    drop(cmd);
    Ok((
        child,
        ConsoleCapture::spawn(console_rx, console, console_cap),
    ))
}

/// SIGKILL the whole process group led by `pid`. Errors (e.g. ESRCH when the
/// group is already gone) are ignored.
pub fn kill_process_group(pid: u32) {
    // SAFETY: plain syscall with a negative pid (process group); no memory involved.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

/// True when a process with this pid exists (signal 0). EPERM also counts as
/// alive: the process exists but belongs to another user.
pub fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 performs only the permission/existence check.
    let rc = unsafe { libc::kill(pid as i32, 0) };
    if rc == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Whether `pid` is a Firecracker process started for `dir`. Uses
/// `/proc/<pid>/cmdline`; returns `None` when that cannot be determined
/// (non-Linux hosts, process gone), in which case callers must not kill.
pub fn pid_belongs_to_env(pid: u32, dir: &Path) -> Option<bool> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let raw = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let needle = dir.as_os_str().as_encoded_bytes();
    Some(
        raw.split(|b| *b == 0)
            .any(|arg| arg.windows(needle.len()).any(|w| w == needle)),
    )
}

/// Wait until `pid` is gone or `grace` elapses. Returns true when it exited.
pub async fn wait_pid_gone(pid: u32, grace: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < grace {
        if !pid_alive(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    !pid_alive(pid)
}

/// Last `max_bytes` of a text file (lossy UTF-8), or a short marker.
pub fn tail_of_file(path: &Path, max_bytes: usize) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return format!("<{} not readable>", path.display());
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let start = len.saturating_sub(max_bytes as u64);
    if f.seek(SeekFrom::Start(start)).is_err() {
        return format!("<{} not seekable>", path.display());
    }
    let mut buf = Vec::with_capacity((len - start) as usize);
    if f.read_to_end(&mut buf).is_err() {
        return format!("<{} not readable>", path.display());
    }
    let s = String::from_utf8_lossy(&buf).into_owned();
    if s.trim().is_empty() {
        "<empty>".to_owned()
    } else if start > 0 {
        format!("...{s}")
    } else {
        s
    }
}

pub fn write_pid_file(path: &Path, pid: u32) -> std::io::Result<()> {
    std::fs::write(path, format!("{pid}\n"))
}

pub fn read_pid_file(path: &Path) -> Option<u32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_id_replaces_underscore() {
        assert_eq!(
            instance_id_for("env_01hzzzzzzzzzzzzzzzzzzzzzzz"),
            "env-01hzzzzzzzzzzzzzzzzzzzzzzz"
        );
        assert!(
            instance_id_for("env_x")
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
        );
        assert_eq!(instance_id_for(""), "tachyon");
        assert_eq!(instance_id_for(&"a".repeat(100)).len(), 64);
    }

    #[test]
    fn env_paths_layout() {
        let p = EnvPaths::new(Path::new("/w"), "env_1", 5000);
        assert_eq!(p.dir, PathBuf::from("/w/env_1"));
        assert_eq!(p.stage_app, PathBuf::from("/w/env_1/stage/app"));
        assert_eq!(p.function_drive, PathBuf::from("/w/env_1/function.ext4"));
        assert_eq!(p.scratch_drive, PathBuf::from("/w/env_1/scratch.ext4"));
        assert_eq!(p.api_sock, PathBuf::from("/w/env_1/fc.sock"));
        assert_eq!(p.vsock_uds, PathBuf::from("/w/env_1/v.sock"));
        assert_eq!(p.vsock_listener, PathBuf::from("/w/env_1/v.sock_5000"));
        assert_eq!(p.longest_socket_path_len(), "/w/env_1/v.sock_5000".len());
    }

    #[test]
    fn pid_liveness_and_kill_of_missing_group_are_safe() {
        assert!(pid_alive(std::process::id()));
        // Very unlikely to exist; kill must not panic.
        kill_process_group(u32::MAX / 2);
        assert!(!pid_alive(u32::MAX / 2));
    }

    #[test]
    fn tail_is_bounded_and_marks_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("console.log");
        std::fs::write(&f, "a".repeat(100) + "END").unwrap();
        let t = tail_of_file(&f, 10);
        assert!(t.starts_with("..."));
        assert!(t.ends_with("END"));
        assert_eq!(t.len(), 13);
        assert!(tail_of_file(&dir.path().join("nope"), 10).contains("not readable"));
        std::fs::write(&f, "").unwrap();
        assert_eq!(tail_of_file(&f, 10), "<empty>");
    }

    #[test]
    fn pid_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("fc.pid");
        write_pid_file(&f, 4242).unwrap();
        assert_eq!(read_pid_file(&f), Some(4242));
        assert_eq!(read_pid_file(&dir.path().join("none")), None);
    }
}
