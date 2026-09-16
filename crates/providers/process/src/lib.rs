//! Process provider: runs the runtime bridge as a local child process and
//! talks to it over a unix socket. **No isolation** (`Capabilities.dev_only`
//! is true); it exists so the whole invoke path can be exercised on
//! developer machines without KVM. Success here says nothing about microVMs.
//!
//! Per environment the provider keeps `<workdir>/<env_id>/` with
//! `bridge.sock` (listener bound before spawning), `bridge.pid`,
//! `bridge.stdout` and `bridge.stderr`. The bridge runs in its own process
//! group so terminate can signal it and everything it started; the bridge
//! itself kills the user process, which it puts in a separate process group
//! that the provider cannot reach.
//!
//! Terminate order: for `Completed` / `Shutdown` the host has just sent the
//! bridge `Shutdown`, so the provider first waits `GRACEFUL_EXIT_WAIT` for the
//! bridge to wind down on its own. After that (and immediately for every other
//! reason) it sends `SIGTERM` to the bridge group, waits `TERMINATE_GRACE` and
//! sends `SIGKILL`. Both windows are longer than the bridge's own
//! SIGTERM -> SIGKILL grace for the user process, so the bridge is never
//! killed while it still owns a user process that ignores `SIGTERM`.
//!
//! A pid read from a `bridge.pid` this instance did not spawn may be stale
//! (reboot, pid wrap); it is only signalled after its argv is shown to contain
//! the environment id.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tachyon_serverless_domain::{Architecture, BootEvidence, EnvironmentId, ProviderKind};
use tachyon_serverless_provider_port::{
    ArtifactLocation, Capabilities, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec,
    ExecutionProvider, IsolationLevel, PreflightCheck, PreflightReport, ProviderError, Support,
    TerminateReason, TerminateReport,
};
use tokio::net::UnixListener;
use tokio::process::{Child, Command};
use tracing::{info, warn};

/// Mirrors `tachyon_serverless_protocol::env::UNISOLATED` (rule 5.6): the
/// bridge forwards it to the user process.
const UNISOLATED_ENV: &str = "TACHYON_UNISOLATED";
const SOCKET_FILE: &str = "bridge.sock";
const PID_FILE: &str = "bridge.pid";
const STDOUT_FILE: &str = "bridge.stdout";
const STDERR_FILE: &str = "bridge.stderr";
/// Mirrors the runtime bridge's `SHUTDOWN_GRACE` (docs/protocol.md: on
/// `Shutdown` the bridge sends the user process SIGTERM, waits 2 s, then
/// SIGKILL). The bridge's SIGTERM path uses a shorter grace (1 s). Only the
/// bridge can kill the user process, so the provider's timers must outlast it.
const BRIDGE_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
/// `Completed` / `Shutdown`: how long to wait for the bridge to exit on its own
/// after the host's `Shutdown` frame before signalling it.
const GRACEFUL_EXIT_WAIT: Duration = Duration::from_millis(2500);
/// SIGTERM -> SIGKILL grace for the bridge group. The host sends `Shutdown`
/// before every terminate, so the bridge may already be winding down with the
/// full `BRIDGE_SHUTDOWN_GRACE` and ignore our SIGTERM. The extra second lets
/// it deliver SIGKILL to the user process before we SIGKILL the bridge.
const TERMINATE_GRACE: Duration = Duration::from_secs(3);
const KILL_WAIT: Duration = Duration::from_secs(5);

// No orphaned user process: the bridge must reach its own SIGKILL of the user
// process before the provider can SIGKILL the bridge.
const _: () = assert!(TERMINATE_GRACE.as_millis() > BRIDGE_SHUTDOWN_GRACE.as_millis());
const _: () = assert!(GRACEFUL_EXIT_WAIT.as_millis() > BRIDGE_SHUTDOWN_GRACE.as_millis());

#[derive(Debug, Clone)]
pub struct ProcessProviderConfig {
    /// Path of the `tachyon-serverless-runtime-bridge` binary.
    pub bridge_binary: PathBuf,
    /// Directory holding one sub-directory per environment.
    pub workdir: PathBuf,
}

struct Tracked {
    pid: u32,
    dir: PathBuf,
    child: tokio::sync::Mutex<Option<Child>>,
}

pub struct ProcessProvider {
    config: ProcessProviderConfig,
    environments: Mutex<HashMap<EnvironmentId, Arc<Tracked>>>,
}

impl std::fmt::Debug for ProcessProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ProcessProvider {
    pub fn new(config: ProcessProviderConfig) -> Self {
        Self {
            config,
            environments: Mutex::new(HashMap::new()),
        }
    }

    pub fn config(&self) -> &ProcessProviderConfig {
        &self.config
    }

    fn env_dir(&self, id: &EnvironmentId) -> PathBuf {
        self.config.workdir.join(id.as_str())
    }

    fn tracked(&self, id: &EnvironmentId) -> Option<Arc<Tracked>> {
        self.environments
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .cloned()
    }

    fn untrack(&self, id: &EnvironmentId) -> Option<Arc<Tracked>> {
        self.environments
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id)
    }

    /// Bind the bridge socket and open the log files inside `dir`.
    fn prepare_dir(
        dir: &Path,
        id: &EnvironmentId,
    ) -> Result<(UnixListener, PathBuf, File, File), ProviderError> {
        let sock = socket_bind_path(dir, id);
        let listener = UnixListener::bind(&sock)
            .map_err(|e| ProviderError::Boot(format!("cannot bind {}: {e}", sock.display())))?;
        let canonical_sock = dir.join(SOCKET_FILE);
        if sock != canonical_sock {
            // Long workdir: the socket lives at a short path; leave a symlink
            // in the environment directory so the layout stays discoverable
            // and cleanup can find the real socket.
            #[cfg(unix)]
            std::os::unix::fs::symlink(&sock, &canonical_sock)?;
        }
        let stdout = File::create(dir.join(STDOUT_FILE))?;
        let stderr = File::create(dir.join(STDERR_FILE))?;
        Ok((listener, sock, stdout, stderr))
    }

    /// Remove the environment directory (and a relocated socket, if any),
    /// listing what was removed.
    fn cleanup_dir(dir: &Path, cleaned: &mut Vec<String>) -> Result<(), ProviderError> {
        if !dir.exists() && std::fs::symlink_metadata(dir).is_err() {
            return Ok(());
        }
        let sock = dir.join(SOCKET_FILE);
        // A symlink means the socket was bound at a short path (see
        // `socket_bind_path`); remove the real socket as well.
        if let Ok(meta) = std::fs::symlink_metadata(&sock)
            && meta.file_type().is_symlink()
            && let Ok(target) = std::fs::read_link(&sock)
            && std::fs::remove_file(&target).is_ok()
        {
            cleaned.push(target.display().to_string());
        }
        for name in [SOCKET_FILE, PID_FILE, STDOUT_FILE, STDERR_FILE] {
            let p = dir.join(name);
            if std::fs::symlink_metadata(&p).is_ok() {
                cleaned.push(p.display().to_string());
            }
        }
        std::fs::remove_dir_all(dir)?;
        cleaned.push(dir.display().to_string());
        Ok(())
    }
}

/// Longest socket path accepted on every supported host (`sun_path` is 104
/// bytes on macOS and 108 on Linux, including the terminating NUL).
const MAX_SOCKET_PATH_BYTES: usize = 100;

/// Where to bind the bridge socket. Normally `<dir>/bridge.sock`; when that
/// exceeds the `sun_path` limit the socket goes to `/tmp/tsls-<ulid>.sock`.
fn socket_bind_path(dir: &Path, id: &EnvironmentId) -> PathBuf {
    let preferred = dir.join(SOCKET_FILE);
    if preferred.as_os_str().len() < MAX_SOCKET_PATH_BYTES {
        return preferred;
    }
    let ulid = id.as_str().rsplit('_').next().unwrap_or(id.as_str());
    PathBuf::from(format!("/tmp/tsls-{ulid}.sock"))
}

/// A pid that names exactly one process. `0` and values that turn negative as
/// `pid_t` would address process groups (or every process) in `kill(2)`.
fn valid_pid(pid: u32) -> bool {
    pid > 0 && pid <= i32::MAX as u32
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: i32) {
    if !valid_pid(pid) {
        return;
    }
    // SAFETY: plain signal delivery to a pid we spawned; ESRCH is ignored.
    unsafe {
        if libc::killpg(pid as libc::pid_t, signal) != 0 {
            libc::kill(pid as libc::pid_t, signal);
        }
    }
}

#[cfg(not(unix))]
fn signal_group(_pid: u32, _signal: i32) {}

/// `killpg` without the `kill(pid)` fallback, for sweeps after the leader may
/// already be gone: a process group id is not reused while any member lives,
/// whereas a bare pid can be.
#[cfg(unix)]
fn signal_group_only(pgid: u32, signal: i32) {
    if !valid_pid(pgid) {
        return;
    }
    // SAFETY: plain signal delivery to a process group; ESRCH is ignored.
    unsafe {
        libc::killpg(pgid as libc::pid_t, signal);
    }
}

#[cfg(not(unix))]
fn signal_group_only(_pgid: u32, _signal: i32) {}

/// `kill(pid, 0)`: true while a process with this pid exists.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 performs no delivery.
    valid_pid(pid) && unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    false
}

#[cfg(unix)]
const SIGTERM: i32 = libc::SIGTERM;
#[cfg(unix)]
const SIGKILL: i32 = libc::SIGKILL;
#[cfg(not(unix))]
const SIGTERM: i32 = 15;
#[cfg(not(unix))]
const SIGKILL: i32 = 9;

fn exit_parts(status: &std::process::ExitStatus) -> (Option<i32>, Option<i32>) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        (status.code(), status.signal())
    }
    #[cfg(not(unix))]
    {
        (status.code(), None)
    }
}

fn read_pid_file(dir: &Path) -> Option<u32> {
    std::fs::read_to_string(dir.join(PID_FILE))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .filter(|pid| valid_pid(*pid))
}

/// Whether `pid` is the bridge spawned for `environment_id`: one argv element
/// must equal the environment id, which is always passed as
/// `--environment-id <id>` and is unique. (The socket path is not used: a long
/// workdir relocates it to `/tmp`.) Returns `None` when argv cannot be read
/// (unsupported host, process gone, no permission); callers must not signal
/// the pid then.
fn pid_belongs_to_env(pid: u32, environment_id: &EnvironmentId) -> Option<bool> {
    if !valid_pid(pid) {
        return None;
    }
    let argv = process_argv(pid)?;
    Some(argv_contains(&argv, environment_id.as_str()))
}

/// True when one argv element is exactly `needle` (a substring is not enough).
fn argv_contains<A: AsRef<[u8]>>(argv: &[A], needle: &str) -> bool {
    argv.iter().any(|arg| arg.as_ref() == needle.as_bytes())
}

/// Split `/proc/<pid>/cmdline` (NUL-terminated arguments) into argv.
#[cfg(any(target_os = "linux", test))]
fn parse_proc_cmdline(raw: &[u8]) -> Vec<Vec<u8>> {
    let raw = raw.strip_suffix(&[0]).unwrap_or(raw);
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split(|b| *b == 0).map(<[u8]>::to_vec).collect()
}

/// Parse a `KERN_PROCARGS2` buffer: native-endian `int argc`, the executable
/// path, NUL padding, then `argc` NUL-terminated arguments. The environment
/// strings that follow are not argv and are ignored.
#[cfg(any(target_os = "macos", test))]
fn parse_procargs2(buf: &[u8]) -> Option<Vec<Vec<u8>>> {
    let argc = i32::from_ne_bytes(buf.get(..4)?.try_into().ok()?);
    let argc = usize::try_from(argc).ok()?;
    let mut argv = Vec::new();
    if argc == 0 {
        return Some(argv);
    }
    let rest = &buf[4..];
    let exec_path_end = rest.iter().position(|b| *b == 0)?;
    let rest = &rest[exec_path_end..];
    let args_start = rest.iter().position(|b| *b != 0)?;
    let mut rest = &rest[args_start..];
    for _ in 0..argc {
        let end = rest.iter().position(|b| *b == 0)?;
        argv.push(rest[..end].to_vec());
        rest = &rest[end + 1..];
    }
    Some(argv)
}

#[cfg(target_os = "linux")]
fn process_argv(pid: u32) -> Option<Vec<Vec<u8>>> {
    std::fs::read(format!("/proc/{pid}/cmdline"))
        .ok()
        .map(|raw| parse_proc_cmdline(&raw))
}

/// The kernel's copy of the target's argv via `sysctl(KERN_PROCARGS2)`
/// (readable for processes of the same user).
#[cfg(target_os = "macos")]
fn process_argv(pid: u32) -> Option<Vec<Vec<u8>>> {
    let mut argmax: libc::c_int = 0;
    let mut len: libc::size_t = std::mem::size_of::<libc::c_int>();
    let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
    // SAFETY: `argmax` is a writable buffer of `len` bytes; no new value is set.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as libc::c_uint,
            (&mut argmax as *mut libc::c_int).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 || argmax <= 0 {
        return None;
    }
    let mut buf = vec![0u8; argmax as usize];
    let mut len: libc::size_t = buf.len();
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid as libc::c_int];
    // SAFETY: `buf` is a writable buffer of `len` bytes; the kernel writes at
    // most `len` bytes and stores the length it wrote back into `len`.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as libc::c_uint,
            buf.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return None;
    }
    buf.truncate(len);
    parse_procargs2(&buf)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_argv(_pid: u32) -> Option<Vec<Vec<u8>>> {
    None
}

/// Poll `kill(pid, 0)` until the process is gone or `limit` elapses. Returns
/// true when it is gone.
async fn wait_pid_gone(pid: u32, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while pid_alive(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    true
}

/// Stop a bridge we spawned. When `graceful` (the host has just sent
/// `Shutdown`), first give it `GRACEFUL_EXIT_WAIT` to exit on its own, so the
/// bridge finishes its user process itself. Then SIGTERM, `TERMINATE_GRACE`,
/// SIGKILL.
async fn stop_child(pid: u32, child: &mut Child, graceful: bool) {
    if graceful
        && tokio::time::timeout(GRACEFUL_EXIT_WAIT, child.wait())
            .await
            .is_ok()
    {
        return;
    }
    signal_group(pid, SIGTERM);
    if tokio::time::timeout(TERMINATE_GRACE, child.wait())
        .await
        .is_ok()
    {
        return;
    }
    warn!(pid, "bridge ignored SIGTERM; sending SIGKILL");
    signal_group(pid, SIGKILL);
    let _ = tokio::time::timeout(KILL_WAIT, child.wait()).await;
}

/// Stop a bridge left by a previous run whose ownership the caller verified.
/// We cannot reap it, so its pid may be recycled while we wait; ownership is
/// checked again before every signal. Returns true when the bridge was ours
/// and is gone.
async fn stop_orphan(pid: u32, environment_id: &EnvironmentId, graceful: bool) -> bool {
    if graceful && wait_pid_gone(pid, GRACEFUL_EXIT_WAIT).await {
        return true;
    }
    if pid_belongs_to_env(pid, environment_id) != Some(true) {
        return false;
    }
    signal_group(pid, SIGTERM);
    if wait_pid_gone(pid, TERMINATE_GRACE).await {
        return true;
    }
    if pid_belongs_to_env(pid, environment_id) != Some(true) {
        return false;
    }
    warn!(pid, "orphaned bridge ignored SIGTERM; sending SIGKILL");
    signal_group(pid, SIGKILL);
    wait_pid_gone(pid, KILL_WAIT).await
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.is_file()
    }
}

/// Check the executable header matches the host format and architecture.
fn check_magic(path: &Path, architecture: Architecture) -> Result<(), ProviderError> {
    use std::io::Read;
    let mut head = [0u8; 8];
    let n = File::open(path)?.read(&mut head)?;
    if n < 8 {
        return Err(ProviderError::ArtifactRejected(
            "file too short to be an executable".into(),
        ));
    }
    if cfg!(target_os = "linux") {
        if head[..4] != [0x7f, b'E', b'L', b'F'] {
            return Err(ProviderError::ArtifactRejected(
                "not an ELF executable (expected on Linux hosts)".into(),
            ));
        }
        // e_machine lives at offset 18 (2 bytes, endianness from EI_DATA).
        let mut hdr = [0u8; 20];
        let n = File::open(path)?.read(&mut hdr)?;
        if n >= 20 {
            let little = hdr[5] == 1;
            let machine = if little {
                u16::from_le_bytes([hdr[18], hdr[19]])
            } else {
                u16::from_be_bytes([hdr[18], hdr[19]])
            };
            let expected = match architecture {
                Architecture::X86_64 => 0x3e,
                Architecture::Aarch64 => 0xb7,
            };
            if machine != expected {
                return Err(ProviderError::ArtifactRejected(format!(
                    "ELF machine 0x{machine:x} does not match {}",
                    architecture.as_str()
                )));
            }
        }
        Ok(())
    } else if cfg!(target_os = "macos") {
        let magic_le = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
        let magic_be = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
        const MH_MAGIC_64: u32 = 0xfeed_facf;
        const FAT_MAGIC: u32 = 0xcafe_babe;
        if magic_le == MH_MAGIC_64 || magic_be == MH_MAGIC_64 {
            let cputype = if magic_le == MH_MAGIC_64 {
                u32::from_le_bytes([head[4], head[5], head[6], head[7]])
            } else {
                u32::from_be_bytes([head[4], head[5], head[6], head[7]])
            };
            let expected = match architecture {
                Architecture::X86_64 => 0x0100_0007,
                Architecture::Aarch64 => 0x0100_000c,
            };
            if cputype != expected {
                return Err(ProviderError::ArtifactRejected(format!(
                    "Mach-O cputype 0x{cputype:x} does not match {}",
                    architecture.as_str()
                )));
            }
            Ok(())
        } else if magic_be == FAT_MAGIC {
            // Universal binary: trust the loader to pick the host slice.
            Ok(())
        } else {
            Err(ProviderError::ArtifactRejected(
                "not a Mach-O executable (expected on macOS hosts)".into(),
            ))
        }
    } else {
        Err(ProviderError::Unavailable(
            "process provider supports Linux and macOS hosts only".into(),
        ))
    }
}

#[async_trait]
impl ExecutionProvider for ProcessProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            isolation: IsolationLevel::Process,
            create_terminate: Support::Supported,
            observe: Support::Supported,
            enforce_deadline: Support::Supported,
            enforce_resource_limits: Support::unsupported("no cgroups in process provider"),
            egress_none: Support::unsupported("shares host network"),
            egress_restricted: Support::unsupported("shares host network"),
            egress_public_web: Support::unsupported("shares host network"),
            host_metering: Support::unsupported("process provider does not meter"),
            idle_quiesce: Support::unsupported("destroy-after-invoke only"),
            idle_resume: Support::unsupported("destroy-after-invoke only"),
            snapshot_create: Support::unsupported("no snapshots for host processes"),
            snapshot_clone: Support::unsupported("no snapshots for host processes"),
            dev_only: true,
        }
    }

    async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
        let mut checks = Vec::new();
        let bridge = &self.config.bridge_binary;
        checks.push(PreflightCheck {
            name: "bridge_binary".into(),
            ok: is_executable(bridge),
            detail: if is_executable(bridge) {
                format!("{} is executable", bridge.display())
            } else {
                format!("{} is missing or not executable", bridge.display())
            },
        });
        let workdir = &self.config.workdir;
        let writable = std::fs::create_dir_all(workdir).is_ok() && {
            let probe = workdir.join(format!(".preflight-{}", std::process::id()));
            let ok = std::fs::write(&probe, b"ok").is_ok();
            let _ = std::fs::remove_file(&probe);
            ok
        };
        checks.push(PreflightCheck {
            name: "workdir_writable".into(),
            ok: writable,
            detail: format!("{}", workdir.display()),
        });
        let arch = Architecture::host();
        checks.push(PreflightCheck {
            name: "host_architecture".into(),
            ok: arch.is_some(),
            detail: arch
                .map(|a| a.as_str().to_string())
                .unwrap_or_else(|| std::env::consts::ARCH.to_string()),
        });
        Ok(PreflightReport {
            provider: "process".into(),
            ok: checks.iter().all(|c| c.ok),
            checks,
        })
    }

    async fn validate_artifact(
        &self,
        artifact: &ArtifactLocation,
        architecture: Architecture,
    ) -> Result<(), ProviderError> {
        let host = Architecture::host().ok_or_else(|| {
            ProviderError::Unavailable(format!(
                "unsupported host architecture {}",
                std::env::consts::ARCH
            ))
        })?;
        if architecture != host {
            return Err(ProviderError::ArtifactRejected(format!(
                "artifact architecture {} does not match host {}",
                architecture.as_str(),
                host.as_str()
            )));
        }
        if !artifact.path.is_file() {
            return Err(ProviderError::ArtifactRejected(format!(
                "artifact {} does not exist",
                artifact.path.display()
            )));
        }
        if !is_executable(&artifact.path) {
            return Err(ProviderError::ArtifactRejected(format!(
                "artifact {} is not executable",
                artifact.path.display()
            )));
        }
        check_magic(&artifact.path, architecture)
    }

    async fn create_environment(
        &self,
        spec: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError> {
        let created_at = Instant::now();
        let id = spec.environment_id.clone();
        let dir = self.env_dir(&id);
        {
            let envs = self.environments.lock().unwrap_or_else(|p| p.into_inner());
            if envs.contains_key(&id) {
                return Err(ProviderError::InvalidSpec(format!(
                    "environment {id} already exists"
                )));
            }
        }
        if dir.exists() {
            return Err(ProviderError::InvalidSpec(format!(
                "environment directory {} already exists",
                dir.display()
            )));
        }
        std::fs::create_dir_all(&dir)?;
        let (listener, sock, stdout, stderr) = match Self::prepare_dir(&dir, &id) {
            Ok(v) => v,
            Err(e) => {
                let _ = Self::cleanup_dir(&dir, &mut Vec::new());
                return Err(e);
            }
        };

        let mut cmd = Command::new(&self.config.bridge_binary);
        cmd.arg("--transport")
            .arg("unix")
            .arg("--unix-path")
            .arg(&sock)
            .arg("--environment-id")
            .arg(id.as_str())
            .arg("--runtime-api-addr")
            .arg("127.0.0.1:0")
            // Rule 5.6: the guest learns it runs without isolation.
            .env(UNISOLATED_ENV, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(false);
        #[cfg(unix)]
        cmd.process_group(0);
        let child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                drop(listener);
                let _ = Self::cleanup_dir(&dir, &mut Vec::new());
                return Err(ProviderError::Boot(format!(
                    "cannot spawn bridge {}: {e}",
                    self.config.bridge_binary.display()
                )));
            }
        };
        let pid = child.id().unwrap_or(0);
        let _ = std::fs::write(dir.join(PID_FILE), pid.to_string());
        let tracked = Arc::new(Tracked {
            pid,
            dir: dir.clone(),
            child: tokio::sync::Mutex::new(Some(child)),
        });
        self.environments
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id.clone(), tracked.clone());
        info!(environment_id = %id, pid, "bridge spawned");

        // Wait for the bridge to connect, racing against an early exit.
        let exited = async {
            loop {
                {
                    let mut guard = tracked.child.lock().await;
                    if let Some(child) = guard.as_mut()
                        && let Ok(Some(status)) = child.try_wait()
                    {
                        return status;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        let accepted = tokio::select! {
            r = tokio::time::timeout(spec.connect_timeout, listener.accept()) => match r {
                Ok(Ok((stream, _))) => Ok(stream),
                Ok(Err(e)) => Err(ProviderError::Boot(format!("accept failed: {e}"))),
                Err(_) => Err(ProviderError::Timeout { stage: "bridge_connect" }),
            },
            status = exited => {
                let (code, signal) = exit_parts(&status);
                Err(ProviderError::Boot(format!(
                    "bridge exited before connecting (exit_code={code:?}, signal={signal:?})"
                )))
            }
        };
        let stream = match accepted {
            Ok(s) => s,
            Err(e) => {
                let _ = self
                    .terminate_environment(&id, TerminateReason::InitFailed)
                    .await;
                return Err(e);
            }
        };
        let connected_at = Instant::now();
        drop(listener);

        let mut details = serde_json::Map::new();
        details.insert("provider".into(), "process".into());
        details.insert("isolation".into(), "none".into());
        details.insert(
            "bridge_binary".into(),
            self.config.bridge_binary.display().to_string().into(),
        );
        details.insert(
            "workdir".into(),
            self.config.workdir.display().to_string().into(),
        );
        Ok(EnvironmentHandle {
            environment_id: id,
            evidence: BootEvidence {
                guest_boot_id: None,
                host_pid: Some(pid),
                details,
            },
            stream: Box::new(stream),
            created_at,
            connected_at,
        })
    }

    async fn terminate_environment(
        &self,
        environment_id: &EnvironmentId,
        reason: TerminateReason,
    ) -> Result<TerminateReport, ProviderError> {
        let mut report = TerminateReport::default();
        let dir = self.env_dir(environment_id);
        // The host sends `Shutdown` before these, so the bridge is expected to
        // exit by itself (mirrors the Firecracker provider).
        // Only a guest that can still act on the `Shutdown` frame is waited
        // for; a quiesced environment cannot (PLT-4633 review F3).
        let graceful = reason.waits_for_the_guest();
        match self.untrack(environment_id) {
            Some(tracked) => {
                let mut guard = tracked.child.lock().await;
                if let Some(child) = guard.as_mut() {
                    let running = matches!(child.try_wait(), Ok(None));
                    report.was_running = running;
                    if running {
                        info!(environment_id = %environment_id, pid = tracked.pid, ?reason, graceful, "terminating bridge");
                        stop_child(tracked.pid, child, graceful).await;
                    }
                    // Sweep anything left in the bridge's process group. The
                    // bridge may already be reaped, so no `kill(pid)` fallback.
                    signal_group_only(tracked.pid, SIGKILL);
                }
                *guard = None;
                Self::cleanup_dir(&tracked.dir, &mut report.cleaned)?;
            }
            None => {
                // Not tracked by this instance: maybe an orphan from a
                // previous run whose directory still exists. Its pid file may
                // be stale, so only a pid whose argv proves it is this
                // environment's bridge is signalled.
                if dir.exists() {
                    if let Some(pid) = read_pid_file(&dir)
                        && pid_alive(pid)
                    {
                        match pid_belongs_to_env(pid, environment_id) {
                            Some(true) => {
                                warn!(environment_id = %environment_id, pid, "terminating orphaned bridge");
                                report.was_running = true;
                                if stop_orphan(pid, environment_id, graceful).await {
                                    signal_group_only(pid, SIGKILL);
                                }
                            }
                            Some(false) => warn!(
                                environment_id = %environment_id,
                                pid,
                                "pid file points at an unrelated process; not killed"
                            ),
                            None => warn!(
                                environment_id = %environment_id,
                                pid,
                                "cannot verify that the pid is this environment's bridge; not killed, removing files only"
                            ),
                        }
                    }
                    Self::cleanup_dir(&dir, &mut report.cleaned)?;
                }
            }
        }
        Ok(report)
    }

    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        if let Some(tracked) = self.tracked(environment_id) {
            let mut guard = tracked.child.lock().await;
            return Ok(match guard.as_mut() {
                Some(child) => match child.try_wait()? {
                    None => EnvironmentObservation::Running {
                        host_pid: Some(tracked.pid),
                    },
                    Some(status) => {
                        let (exit_code, signal) = exit_parts(&status);
                        EnvironmentObservation::Exited { exit_code, signal }
                    }
                },
                None if pid_alive(tracked.pid) => EnvironmentObservation::Running {
                    host_pid: Some(tracked.pid),
                },
                None => EnvironmentObservation::Exited {
                    exit_code: None,
                    signal: None,
                },
            });
        }
        let dir = self.env_dir(environment_id);
        if !dir.exists() {
            return Ok(EnvironmentObservation::NotFound);
        }
        Ok(match read_pid_file(&dir) {
            // A recycled pid must not keep a stale environment "running".
            Some(pid)
                if pid_alive(pid) && pid_belongs_to_env(pid, environment_id) != Some(false) =>
            {
                EnvironmentObservation::Running {
                    host_pid: Some(pid),
                }
            }
            Some(_) => EnvironmentObservation::Exited {
                exit_code: None,
                signal: None,
            },
            None => EnvironmentObservation::NotFound,
        })
    }

    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
        let mut ids: Vec<EnvironmentId> = Vec::new();
        let entries = match std::fs::read_dir(&self.config.workdir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Ok(id) = EnvironmentId::parse(&entry.file_name().to_string_lossy()) {
                ids.push(id);
            }
        }
        ids.sort();
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_serverless_domain::Sha256Digest;

    fn provider(dir: &Path) -> ProcessProvider {
        ProcessProvider::new(ProcessProviderConfig {
            bridge_binary: dir.join("missing-bridge"),
            workdir: dir.join("work"),
        })
    }

    #[test]
    fn capabilities_are_dev_only_and_explicit() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let c = p.capabilities();
        assert!(c.dev_only);
        assert_eq!(c.isolation, IsolationLevel::Process);
        assert!(c.create_terminate.is_supported());
        assert!(c.observe.is_supported());
        assert!(c.enforce_deadline.is_supported());
        assert!(!c.enforce_resource_limits.is_supported());
        assert!(!c.egress_none.is_supported());
        assert!(!c.snapshot_create.is_supported());
        // destroy-after-invoke: the environment pool only ever hands out an
        // environment for a provider that reports both idle capabilities as
        // `Supported` (docs/architecture.md §4), so this provider stays on the
        // P1 behaviour whatever `[pool] enabled` says.
        for s in [&c.idle_quiesce, &c.idle_resume] {
            assert!(matches!(s, Support::Unsupported { .. }), "{s:?}");
        }
        assert_eq!(p.kind(), ProviderKind::Process);
    }

    #[tokio::test]
    async fn preflight_reports_missing_bridge() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let r = p.preflight().await.unwrap();
        assert!(!r.ok);
        let bridge = r.checks.iter().find(|c| c.name == "bridge_binary").unwrap();
        assert!(!bridge.ok);
        let work = r
            .checks
            .iter()
            .find(|c| c.name == "workdir_writable")
            .unwrap();
        assert!(work.ok);
        assert!(dir.path().join("work").is_dir());
    }

    #[tokio::test]
    async fn validate_artifact_checks_arch_existence_mode_and_magic() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let host = host_arch();
        let other = match host {
            Architecture::X86_64 => Architecture::Aarch64,
            Architecture::Aarch64 => Architecture::X86_64,
        };
        let path = dir.path().join("app");
        let art = |path: &Path| ArtifactLocation {
            path: path.to_path_buf(),
            digest: Sha256Digest::of_bytes(b"x"),
            size_bytes: 1,
        };

        // Wrong architecture.
        assert!(matches!(
            p.validate_artifact(&art(&path), other).await,
            Err(ProviderError::ArtifactRejected(_))
        ));
        // Missing file.
        assert!(matches!(
            p.validate_artifact(&art(&path), host).await,
            Err(ProviderError::ArtifactRejected(_))
        ));
        // Not executable.
        std::fs::write(&path, b"#!/bin/sh\necho hi\n").unwrap();
        assert!(matches!(
            p.validate_artifact(&art(&path), host).await,
            Err(ProviderError::ArtifactRejected(_))
        ));
        // Executable but wrong magic (a shell script).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        assert!(matches!(
            p.validate_artifact(&art(&path), host).await,
            Err(ProviderError::ArtifactRejected(_))
        ));
        // The test binary itself is a valid host executable.
        let me = std::env::current_exe().unwrap();
        p.validate_artifact(&art(&me), host).await.unwrap();
    }

    fn host_arch() -> Architecture {
        Architecture::host().expect("supported host")
    }

    #[test]
    fn socket_path_falls_back_to_tmp_when_too_long() {
        let id = EnvironmentId::generate();
        let short = socket_bind_path(Path::new("/srv/x"), &id);
        assert_eq!(short, Path::new("/srv/x").join(SOCKET_FILE));
        let long_dir = PathBuf::from(format!("/{}", "d".repeat(120)));
        let relocated = socket_bind_path(&long_dir, &id);
        assert!(relocated.as_os_str().len() < MAX_SOCKET_PATH_BYTES);
        assert!(relocated.starts_with("/tmp"));
        assert!(
            relocated
                .to_string_lossy()
                .contains(id.as_str().trim_start_matches("env_"))
        );
    }

    #[tokio::test]
    async fn observe_and_terminate_unknown_environment() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        assert_eq!(
            p.observe_environment(&id).await.unwrap(),
            EnvironmentObservation::NotFound
        );
        let r = p
            .terminate_environment(&id, TerminateReason::Reconcile)
            .await
            .unwrap();
        assert!(!r.was_running);
        assert!(r.cleaned.is_empty());
        assert!(p.list_environments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn orphan_directory_is_listed_and_cleaned() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        let env_dir = dir.path().join("work").join(id.as_str());
        std::fs::create_dir_all(&env_dir).unwrap();
        std::fs::write(env_dir.join(PID_FILE), "999999999").unwrap();
        std::fs::create_dir_all(dir.path().join("work").join("not-an-id")).unwrap();
        assert_eq!(p.list_environments().await.unwrap(), vec![id.clone()]);
        assert_eq!(
            p.observe_environment(&id).await.unwrap(),
            EnvironmentObservation::Exited {
                exit_code: None,
                signal: None
            }
        );
        let r = p
            .terminate_environment(&id, TerminateReason::Reconcile)
            .await
            .unwrap();
        assert!(!r.was_running);
        assert!(r.cleaned.iter().any(|c| c.ends_with(PID_FILE)));
        assert!(!env_dir.exists());
        assert!(p.list_environments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_with_missing_bridge_fails_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        let spec = EnvironmentSpec {
            environment_id: id.clone(),
            tenant_id: tachyon_serverless_domain::TenantId::generate(),
            revision_id: tachyon_serverless_domain::RevisionId::generate(),
            artifact: ArtifactLocation {
                path: dir.path().join("app"),
                digest: Sha256Digest::of_bytes(b"x"),
                size_bytes: 1,
            },
            architecture: host_arch(),
            resources: Default::default(),
            egress: Default::default(),
            connect_timeout: Duration::from_secs(2),
        };
        let err = p.create_environment(spec).await.unwrap_err();
        assert!(matches!(err, ProviderError::Boot(_)), "{err}");
        assert!(!dir.path().join("work").join(id.as_str()).exists());
        assert!(p.list_environments().await.unwrap().is_empty());
    }

    #[test]
    fn pid_file_rejects_pids_that_address_groups() {
        let dir = tempfile::tempdir().unwrap();
        for bad in ["0", "2147483648", "4294967295", "-1", "abc"] {
            std::fs::write(dir.path().join(PID_FILE), bad).unwrap();
            assert_eq!(read_pid_file(dir.path()), None, "{bad}");
        }
        std::fs::write(dir.path().join(PID_FILE), "4242\n").unwrap();
        assert_eq!(read_pid_file(dir.path()), Some(4242));
    }

    #[test]
    fn argv_matcher_requires_an_exact_element() {
        let id = "env_01hzzzzzzzzzzzzzzzzzzzzzz1";
        let bridge = parse_proc_cmdline(
            b"/opt/tsls/bridge\0--transport\0unix\0--environment-id\0env_01hzzzzzzzzzzzzzzzzzzzzzz1\0",
        );
        assert_eq!(bridge.len(), 5);
        assert!(argv_contains(&bridge, id));
        // A path or a longer token that merely contains the id is not ownership.
        let editor =
            parse_proc_cmdline(b"vim\0/work/env_01hzzzzzzzzzzzzzzzzzzzzzz1/bridge.stderr\0");
        assert!(!argv_contains(&editor, id));
        let longer = parse_proc_cmdline(b"x\0env_01hzzzzzzzzzzzzzzzzzzzzzz12\0");
        assert!(!argv_contains(&longer, id));
        // Zombies and kernel threads have an empty cmdline.
        assert!(parse_proc_cmdline(b"").is_empty());
        assert!(!argv_contains(&parse_proc_cmdline(b""), id));
    }

    #[test]
    fn procargs2_buffer_yields_argv_only() {
        let mut buf = 3i32.to_ne_bytes().to_vec();
        buf.extend_from_slice(b"/opt/tsls/bridge\0\0\0\0");
        buf.extend_from_slice(b"bridge\0--environment-id\0env_a\0");
        buf.extend_from_slice(b"TSLS_ENV=env_b\0env_b\0");
        let argv = parse_procargs2(&buf).unwrap();
        assert_eq!(
            argv,
            vec![
                b"bridge".to_vec(),
                b"--environment-id".to_vec(),
                b"env_a".to_vec()
            ]
        );
        assert!(argv_contains(&argv, "env_a"));
        assert!(
            !argv_contains(&argv, "env_b"),
            "environment strings are not argv"
        );
        // Truncated or malformed buffers are not guessed at.
        assert_eq!(parse_procargs2(&[1, 0]), None);
        assert_eq!(parse_procargs2(&(-1i32).to_ne_bytes()), None);
        let mut short = 4i32.to_ne_bytes().to_vec();
        short.extend_from_slice(b"/bin/x\0a\0b\0");
        assert_eq!(parse_procargs2(&short), None);
    }

    /// Spawn `cmd` and reap it on a thread so `kill(pid, 0)` sees it vanish.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn spawn_reaped(
        cmd: &mut std::process::Command,
    ) -> (u32, std::sync::mpsc::Receiver<std::process::ExitStatus>) {
        let mut child = cmd.spawn().unwrap();
        let pid = child.id();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(child.wait().unwrap());
        });
        (pid, rx)
    }

    /// Wait until `pid` has exec'd and shows `needle` in its argv.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    async fn wait_for_argv(pid: u32, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !process_argv(pid).is_some_and(|argv| argv_contains(&argv, needle)) {
            assert!(
                Instant::now() < deadline,
                "pid {pid} never showed `{needle}` in its argv"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// An environment directory left by a previous run, pointing at `pid`.
    fn write_orphan_dir(p: &ProcessProvider, id: &EnvironmentId, pid: u32) -> PathBuf {
        let env_dir = p.env_dir(id);
        std::fs::create_dir_all(&env_dir).unwrap();
        std::fs::write(env_dir.join(PID_FILE), pid.to_string()).unwrap();
        env_dir
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn stale_pid_file_never_kills_an_unrelated_process() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        // The pid was recycled: it now names some other live process of ours.
        let (pid, exited) = spawn_reaped(std::process::Command::new("sleep").arg("30"));
        wait_for_argv(pid, "30").await;
        let env_dir = write_orphan_dir(&p, &id, pid);

        assert_eq!(pid_belongs_to_env(pid, &id), Some(false));
        assert!(!matches!(
            p.observe_environment(&id).await.unwrap(),
            EnvironmentObservation::Running { .. }
        ));
        let r = p
            .terminate_environment(&id, TerminateReason::Reconcile)
            .await
            .unwrap();
        assert!(!r.was_running);
        assert!(!env_dir.exists(), "files are still cleaned up");
        assert!(
            exited.recv_timeout(Duration::from_millis(500)).is_err(),
            "an unrelated process was killed"
        );
        assert!(pid_alive(pid));

        // SAFETY: signalling our own child.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGKILL);
        }
        exited.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn orphaned_bridge_with_matching_argv_is_terminated() {
        use std::os::unix::process::CommandExt;
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        // Stands in for a bridge from a previous run: the id is in its argv
        // (`$0` of the shell). Two commands keep the shell from exec'ing
        // `sleep` in its place.
        let mut cmd = std::process::Command::new("/bin/sh");
        cmd.arg("-c")
            .arg("sleep 30; :")
            .arg(id.as_str())
            .process_group(0);
        let (pid, exited) = spawn_reaped(&mut cmd);
        wait_for_argv(pid, id.as_str()).await;
        let env_dir = write_orphan_dir(&p, &id, pid);

        assert_eq!(pid_belongs_to_env(pid, &id), Some(true));
        assert_eq!(
            p.observe_environment(&id).await.unwrap(),
            EnvironmentObservation::Running {
                host_pid: Some(pid)
            }
        );
        let r = p
            .terminate_environment(&id, TerminateReason::Reconcile)
            .await
            .unwrap();
        assert!(r.was_running);
        assert!(!env_dir.exists());
        exited
            .recv_timeout(Duration::from_secs(5))
            .expect("orphaned bridge is still running");
    }

    /// Track `/bin/sh -c <script>` as if `create_environment` had spawned it as
    /// the bridge (own process group, pid file).
    #[cfg(unix)]
    fn track_stub(
        p: &ProcessProvider,
        script: &str,
        env: &[(&str, &Path)],
    ) -> (EnvironmentId, u32) {
        let id = EnvironmentId::generate();
        let env_dir = p.env_dir(&id);
        std::fs::create_dir_all(&env_dir).unwrap();
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c")
            .arg(script)
            .kill_on_drop(true)
            .process_group(0);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().unwrap();
        let pid = child.id().unwrap();
        std::fs::write(env_dir.join(PID_FILE), pid.to_string()).unwrap();
        p.environments.lock().unwrap().insert(
            id.clone(),
            Arc::new(Tracked {
                pid,
                dir: env_dir,
                child: tokio::sync::Mutex::new(Some(child)),
            }),
        );
        (id, pid)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn completed_terminate_lets_a_winding_down_bridge_exit_unsignalled() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let term_mark = dir.path().join("got-sigterm");
        let done_mark = dir.path().join("exited-by-itself");
        // Like a bridge that received `Shutdown` and needs 1.5 s to finish its
        // user process. A SIGTERM would be recorded; a SIGKILL would skip the
        // final marker.
        let (id, _) = track_stub(
            &p,
            r#"trap 'echo x > "$TSLS_TERM_MARK"' TERM; sleep 1.5; echo x > "$TSLS_DONE_MARK"; exit 0"#,
            &[
                ("TSLS_TERM_MARK", term_mark.as_path()),
                ("TSLS_DONE_MARK", done_mark.as_path()),
            ],
        );
        let r = p
            .terminate_environment(&id, TerminateReason::Completed)
            .await
            .unwrap();
        assert!(r.was_running);
        assert!(
            !term_mark.exists(),
            "SIGTERM was sent to a bridge that was exiting on its own"
        );
        assert!(
            done_mark.exists(),
            "the bridge was killed before it could exit on its own"
        );
        assert!(!p.env_dir(&id).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn completed_terminate_kills_a_bridge_that_never_exits_after_the_grace() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let term_mark = dir.path().join("got-sigterm");
        // Records SIGTERM and keeps running: only SIGKILL ends it.
        let (id, pid) = track_stub(
            &p,
            r#"trap 'echo x > "$TSLS_TERM_MARK"' TERM; while :; do sleep 0.1; done"#,
            &[("TSLS_TERM_MARK", term_mark.as_path())],
        );
        let started = Instant::now();
        let r = p
            .terminate_environment(&id, TerminateReason::Completed)
            .await
            .unwrap();
        let took = started.elapsed();
        assert!(r.was_running);
        assert!(term_mark.exists(), "SIGTERM follows the graceful wait");
        assert!(
            took >= GRACEFUL_EXIT_WAIT + TERMINATE_GRACE,
            "SIGKILL came before the graceful wait and the grace: {took:?}"
        );
        assert!(
            took < GRACEFUL_EXIT_WAIT + TERMINATE_GRACE + KILL_WAIT,
            "{took:?}"
        );
        assert!(!pid_alive(pid), "bridge {pid} survived");
        assert!(!p.env_dir(&id).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_graceful_terminate_signals_without_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let (id, pid) = track_stub(&p, "while :; do sleep 0.1; done", &[]);
        let started = Instant::now();
        let r = p
            .terminate_environment(&id, TerminateReason::Timeout)
            .await
            .unwrap();
        assert!(r.was_running);
        assert!(
            started.elapsed() < GRACEFUL_EXIT_WAIT,
            "{:?}",
            started.elapsed()
        );
        assert!(!pid_alive(pid));
    }
}
