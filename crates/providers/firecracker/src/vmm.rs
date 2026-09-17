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
    /// Jail of the environment when the VMM runs under the jailer. The
    /// socket paths above then point into its chroot.
    pub jail: Option<crate::jail::JailLayout>,
}

impl EnvPaths {
    /// Move the sockets into a jail's chroot (see [`crate::jail`]).
    pub fn jailed(mut self, jail: crate::jail::JailLayout, vsock_port: u32) -> Self {
        use crate::jail::{API_SOCK, VSOCK_UDS};
        self.api_sock = jail.host(API_SOCK);
        self.vsock_uds = jail.host(VSOCK_UDS);
        self.vsock_listener = jail.host(&format!("{VSOCK_UDS}_{vsock_port}"));
        self.jail = Some(jail);
        self
    }

    /// The path the VMM is given for a host file: unchanged without a jail,
    /// `/<in-chroot name>` with one.
    pub fn vmm_path(&self, host: &Path, jail_name: &str) -> PathBuf {
        match &self.jail {
            Some(_) => crate::jail::JailLayout::guest(jail_name),
            None => host.to_path_buf(),
        }
    }

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
            jail: None,
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

/// How the VMM process is started.
#[derive(Debug, Clone, Copy)]
pub enum Launcher<'a> {
    /// `firecracker` directly, as the gateway's user.
    Direct { binary: &'a Path },
    /// `jailer ... -- <firecracker args>` (see [`crate::jail`]).
    Jailer {
        jailer: &'a crate::config::JailerConfig,
        firecracker: &'a Path,
    },
}

/// Create the (empty) `fc.log`: Firecracker opens `--log-path` without
/// `O_CREAT`, so the file must exist (and, for a jail, be linked into it).
pub fn create_fc_log(paths: &EnvPaths) -> std::io::Result<()> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.fc_log)
        .map(drop)
}

/// Spawn the VMM (`firecracker --api-sock ... --id ... --log-path ... --level
/// Warning`, directly or through the jailer) in a new process group with
/// stdin closed and stdout/stderr (the guest serial console) sent through a
/// pipe that a [`ConsoleCapture`] drains into `console_log`, keeping at most
/// `console_cap` bytes (PLT-4622).
///
/// With `cgroup_procs` (an fd of a cgroup's `cgroup.procs` opened for
/// writing) the child moves itself into that cgroup between `fork` and
/// `exec`, so the VMM and every thread it creates start inside the limits.
pub fn spawn_vmm(
    launcher: Launcher<'_>,
    paths: &EnvPaths,
    instance_id: &str,
    console_cap: u64,
    cgroup_procs: Option<std::os::fd::RawFd>,
) -> std::io::Result<(Child, ConsoleCapture)> {
    create_fc_log(paths)?;
    let console = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.console_log)?;
    // Both ends are close-on-exec; only the dup2'ed stdout/stderr of the
    // child survive the exec, so the pipe reaches EOF when the VMM exits.
    let (console_rx, console_tx) = std::io::pipe()?;
    let console_err = console_tx.try_clone()?;
    let mut cmd = match launcher {
        Launcher::Direct { binary } => {
            let mut cmd = Command::new(binary);
            cmd.arg("--api-sock")
                .arg(&paths.api_sock)
                .arg("--id")
                .arg(instance_id)
                .arg("--log-path")
                .arg(&paths.fc_log)
                .arg("--level")
                .arg("Warning");
            cmd
        }
        Launcher::Jailer {
            jailer,
            firecracker,
        } => {
            let mut cmd = Command::new(&jailer.binary);
            cmd.args(crate::jail::jailer_args(jailer, firecracker, instance_id));
            cmd
        }
    };
    if let Some(fd) = cgroup_procs {
        // SAFETY: the closure only calls write(2) on an fd that stays open in
        // the parent for the duration of spawn, which is async-signal-safe
        // and allocates nothing.
        unsafe {
            cmd.pre_exec(move || {
                // "0" moves the writing process (the child) into the cgroup.
                if libc::write(fd, b"0".as_ptr().cast(), 1) == 1 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
    }
    // A terminal on stdin would be switched to raw mode by Firecracker.
    cmd.stdin(Stdio::null())
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

/// SIGKILL a VMM that may not lead its process group (a jailer cloned it):
/// its group, unless that is the caller's own, and the process itself.
pub fn kill_vmm(pid: u32) {
    // SAFETY: plain syscalls on pids; no memory involved.
    unsafe {
        let pgid = libc::getpgid(pid as i32);
        if pgid > 1 && pgid != libc::getpgid(0) {
            libc::kill(-pgid, libc::SIGKILL);
        }
        libc::kill(pid as i32, libc::SIGKILL);
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
    pid_cmdline_contains(pid, dir.as_os_str().as_encoded_bytes())
}

/// Whether any argument of `pid`'s command line contains `needle` (`None`
/// when unknown). A jailed VMM's command line holds no host path, only
/// `--id <instance id>`, so the provider also matches the instance id.
pub fn pid_cmdline_contains(pid: u32, needle: &[u8]) -> Option<bool> {
    if !cfg!(target_os = "linux") || needle.is_empty() {
        return None;
    }
    let raw = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    cmdline_contains(&raw, needle)
}

/// Whether a raw `/proc/<pid>/cmdline` (NUL-separated) has an argument
/// containing `needle`. An **empty** command line proves nothing either way
/// (`None`): the kernel returns it for a live process in the middle of
/// `execve` (the new image's argv is not set up yet), for a zombie and for a
/// kernel thread. The jailer turns into Firecracker with an `execve` while the
/// provider polls for the API socket; reading that window as "not ours" made a
/// VMM that was still starting look gone ("firecracker exited before creating
/// the API socket" with empty logs, seen under host load, PLT-4647).
fn cmdline_contains(raw: &[u8], needle: &[u8]) -> Option<bool> {
    if raw.is_empty() {
        return None;
    }
    Some(
        raw.split(|b| *b == 0)
            .any(|arg| arg.windows(needle.len()).any(|w| w == needle)),
    )
}

/// True when `pid` is a zombie (or dead) according to `/proc/<pid>/stat`: it
/// still answers signal 0 but will never run again. `false` when unknown.
pub fn pid_is_zombie(pid: u32) -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    std::fs::read(format!("/proc/{pid}/stat"))
        .ok()
        .is_some_and(|stat| stat_is_zombie(&stat))
}

/// The state field of `/proc/<pid>/stat` follows the last `)` (the command
/// name may itself contain parentheses and spaces).
fn stat_is_zombie(stat: &[u8]) -> bool {
    let Some(close) = stat.iter().rposition(|b| *b == b')') else {
        return false;
    };
    matches!(stat.get(close + 2), Some(b'Z' | b'X' | b'x'))
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
    fn an_empty_cmdline_is_unknown_not_foreign() {
        // What /proc/<pid>/cmdline reads back while the jailer execs Firecracker.
        assert_eq!(cmdline_contains(b"", b"env-01"), None);
        assert_eq!(
            cmdline_contains(b"firecracker\0--id\0env-01\0", b"env-01"),
            Some(true)
        );
        assert_eq!(
            cmdline_contains(b"firecracker\0--id\0env-02\0", b"env-01"),
            Some(false)
        );
    }

    #[test]
    fn zombie_state_is_read_after_the_last_parenthesis() {
        assert!(stat_is_zombie(b"42 (firecracker) Z 1 42 42 0 -1"));
        assert!(!stat_is_zombie(b"42 (fc (vcpu) Z) S 1 42 42 0 -1"));
        assert!(stat_is_zombie(b"42 (a) b) X 1"));
        assert!(!stat_is_zombie(b"42 (firecracker) R 1 42"));
        assert!(!stat_is_zombie(b"garbage"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_live_process_is_not_a_zombie_and_matches_its_cmdline() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        assert!(!pid_is_zombie(pid));
        // Right after spawn the child may still be inside exec, when
        // /proc/<pid>/cmdline reads back empty (`None`): the very race the
        // provider now tolerates. Wait for exec to finish before asserting.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut matched = pid_cmdline_contains(pid, b"30");
        while matched.is_none() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
            matched = pid_cmdline_contains(pid, b"30");
        }
        assert_eq!(matched, Some(true));
        child.kill().expect("kill");
        // Killed but not reaped: a zombie, which must never count as a running VMM.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !pid_is_zombie(pid) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(pid_is_zombie(pid));
        child.wait().expect("reap");
    }

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
        assert_eq!(
            p.vmm_path(&p.scratch_drive, crate::jail::SCRATCH_DRIVE),
            p.scratch_drive
        );

        let jailer = crate::config::JailerConfig {
            chroot_base: PathBuf::from("/srv/jailer"),
            ..Default::default()
        };
        let j = p.jailed(crate::jail::layout(&jailer, "firecracker", "env-1"), 5000);
        let root = PathBuf::from("/srv/jailer/firecracker/env-1/root");
        assert_eq!(j.api_sock, root.join("fc.sock"));
        assert_eq!(j.vsock_uds, root.join("v.sock"));
        assert_eq!(j.vsock_listener, root.join("v.sock_5000"));
        assert_eq!(
            j.dir,
            PathBuf::from("/w/env_1"),
            "host artefacts stay in the env dir"
        );
        assert_eq!(
            j.vmm_path(&j.scratch_drive, crate::jail::SCRATCH_DRIVE),
            PathBuf::from("/scratch.ext4")
        );
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
