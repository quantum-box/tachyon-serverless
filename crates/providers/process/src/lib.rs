//! Process provider: runs the runtime bridge as a local child process and
//! talks to it over a unix socket. **No isolation** (`Capabilities.dev_only`
//! is true); it exists so the whole invoke path can be exercised on
//! developer machines without KVM. Success here says nothing about microVMs.
//!
//! Per environment the provider keeps `<workdir>/<env_id>/` with
//! `bridge.sock` (listener bound before spawning), `bridge.pid`,
//! `bridge.stdout` and `bridge.stderr`. The bridge runs in its own process
//! group so terminate can signal it and everything it started; the bridge
//! itself kills the user process on `SIGTERM`.

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
const TERMINATE_GRACE: Duration = Duration::from_secs(2);
const KILL_WAIT: Duration = Duration::from_secs(5);

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

#[cfg(unix)]
fn signal_group(pid: u32, signal: i32) {
    // SAFETY: plain signal delivery to a pid we spawned; ESRCH is ignored.
    unsafe {
        if libc::killpg(pid as libc::pid_t, signal) != 0 {
            libc::kill(pid as libc::pid_t, signal);
        }
    }
}

#[cfg(not(unix))]
fn signal_group(_pid: u32, _signal: i32) {}

/// `kill(pid, 0)`: true while a process with this pid exists.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 performs no delivery.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
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
}

/// Wait for a child we own: SIGTERM, grace, SIGKILL.
async fn stop_child(pid: u32, child: &mut Child) {
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

/// Wait for a process we do not own (orphan from a previous run).
async fn stop_orphan(pid: u32) {
    signal_group(pid, SIGTERM);
    let deadline = Instant::now() + TERMINATE_GRACE;
    while pid_alive(pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if pid_alive(pid) {
        signal_group(pid, SIGKILL);
        let deadline = Instant::now() + KILL_WAIT;
        while pid_alive(pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
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
        match self.untrack(environment_id) {
            Some(tracked) => {
                let mut guard = tracked.child.lock().await;
                if let Some(child) = guard.as_mut() {
                    let running = matches!(child.try_wait(), Ok(None));
                    report.was_running = running;
                    if running {
                        info!(environment_id = %environment_id, pid = tracked.pid, ?reason, "terminating bridge");
                        stop_child(tracked.pid, child).await;
                    }
                    // Sweep anything left in the process group.
                    signal_group(tracked.pid, SIGKILL);
                }
                *guard = None;
                Self::cleanup_dir(&tracked.dir, &mut report.cleaned)?;
            }
            None => {
                // Not tracked by this instance: maybe an orphan from a
                // previous run whose directory still exists.
                if dir.exists() {
                    if let Some(pid) = read_pid_file(&dir)
                        && pid_alive(pid)
                    {
                        warn!(environment_id = %environment_id, pid, "terminating orphaned bridge");
                        report.was_running = true;
                        stop_orphan(pid).await;
                        signal_group(pid, SIGKILL);
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
            Some(pid) if pid_alive(pid) => EnvironmentObservation::Running {
                host_pid: Some(pid),
            },
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
}
