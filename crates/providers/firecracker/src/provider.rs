//! `ExecutionProvider` implementation for Firecracker.
//!
//! Lifecycle of one environment (docs/protocol.md §C):
//!
//! ```text
//! <workdir>/<env_id>/
//!   stage/app        artifact copy (0755)         -> mkfs.ext4 -d stage function.ext4
//!   function.ext4    read-only function drive     -> PUT /drives/function
//!   v.sock_<port>    host listener (bound BEFORE InstanceStart)
//!   fc.sock          Firecracker API socket
//!   v.sock           vsock UDS bound by Firecracker (host-initiated side, unused)
//!   fc.log           Firecracker log (--log-path)
//!   console.log      guest serial console (Firecracker stdout/stderr)
//!   fc.pid           pid of the Firecracker process (process-group leader)
//! ```
//!
//! Guest-initiated vsock connections (guest -> host CID 2, port N) are
//! forwarded by Firecracker to `<uds_path>_<N>` with no `CONNECT` handshake,
//! so the accepted Unix stream carries bridge frames from the first byte.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::net::UnixListener;
use tokio::process::Child;

use tachyon_serverless_domain::{
    Architecture, BootEvidence, EgressProfile, EnvironmentId, ProviderKind,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, Capabilities, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec,
    ExecutionProvider, IsolationLevel, PreflightReport, ProviderError, Support, TerminateReason,
    TerminateReport,
};

use crate::api::ApiClient;
use crate::boot_args::compose_boot_args;
use crate::config::FirecrackerConfig;
use crate::drive::{create_sparse_image, function_drive_size_bytes, mkfs_args};
use crate::elf::{ElfInfo, inspect_elf_file};
use crate::preflight::{DigestCache, probe_firecracker_version, run_preflight};
use crate::vmm::{
    EnvPaths, MAX_UNIX_SOCKET_PATH, instance_id_for, kill_process_group, pid_alive,
    pid_belongs_to_env, read_pid_file, spawn_firecracker, tail_of_file, wait_pid_gone,
    write_pid_file,
};

/// Name of the directory under `workdir` that keeps logs of terminated environments.
pub const ARCHIVE_DIR: &str = "_archive";
/// Number of archived environments kept (oldest are pruned).
pub const ARCHIVE_KEEP: usize = 50;
/// How long to wait for `fc.sock` after spawning Firecracker.
pub const API_SOCKET_WAIT: Duration = Duration::from_secs(2);
/// Per-request timeout for Firecracker API calls.
pub const API_TIMEOUT: Duration = Duration::from_secs(10);
/// Firecracker vsock guest CID (host is always 2).
pub const GUEST_CID: u32 = 3;
const CONSOLE_TAIL_BYTES: usize = 4096;
const FC_LOG_TAIL_BYTES: usize = 2048;

struct Tracked {
    pid: u32,
    child: Child,
}

/// Firecracker execution provider (Linux/KVM only; compiles everywhere).
pub struct FirecrackerProvider {
    cfg: FirecrackerConfig,
    version: Option<String>,
    running: tokio::sync::Mutex<HashMap<EnvironmentId, Tracked>>,
    digests: DigestCache,
}

impl std::fmt::Debug for FirecrackerProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirecrackerProvider")
            .field("cfg", &self.cfg)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl FirecrackerProvider {
    /// Build a provider. Paths are made absolute; the Firecracker version is
    /// probed once (`firecracker --version`, best effort, may be `None`).
    pub fn new(cfg: FirecrackerConfig) -> Self {
        let cfg = cfg.absolutized();
        let version = crate::preflight::resolve_command(&cfg.firecracker_binary)
            .and_then(|p| probe_firecracker_version(&p));
        Self {
            cfg,
            version,
            running: tokio::sync::Mutex::new(HashMap::new()),
            digests: DigestCache::default(),
        }
    }

    pub fn config(&self) -> &FirecrackerConfig {
        &self.cfg
    }

    /// Firecracker version as reported by the binary at construction.
    pub fn firecracker_version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Capability table advertised by this provider (RFC §6.3).
    pub fn capability_table() -> Capabilities {
        Capabilities {
            isolation: IsolationLevel::MicroVm,
            create_terminate: Support::Supported,
            observe: Support::Supported,
            enforce_deadline: Support::Supported,
            enforce_resource_limits: Support::unverified(
                "vcpu/mem via machine-config; ephemeral storage not enforced",
            ),
            egress_none: Support::Supported,
            egress_restricted: Support::unsupported("no network device is configured in P1"),
            egress_public_web: Support::unsupported("no network device is configured in P1"),
            host_metering: Support::unverified("host-side timings only; no cgroup/KVM stats"),
            idle_quiesce: Support::unsupported("not implemented in P1"),
            idle_resume: Support::unsupported("not implemented in P1"),
            snapshot_create: Support::unsupported("not implemented in P1"),
            snapshot_clone: Support::unsupported("not implemented in P1"),
            dev_only: false,
        }
    }

    fn paths_for(&self, env_id: &EnvironmentId) -> EnvPaths {
        EnvPaths::new(&self.cfg.workdir, env_id.as_str(), self.cfg.vsock_port)
    }

    fn archive_root(&self) -> std::path::PathBuf {
        self.cfg.workdir.join(ARCHIVE_DIR)
    }

    /// `ProviderError::Boot` carrying the tails of the guest console and the
    /// Firecracker log so that a failed boot can be diagnosed from the error.
    fn boot_error(paths: &EnvPaths, msg: impl std::fmt::Display) -> ProviderError {
        ProviderError::Boot(format!(
            "{msg}\n--- console.log (tail) ---\n{}\n--- fc.log (tail) ---\n{}",
            tail_of_file(&paths.console_log, CONSOLE_TAIL_BYTES),
            tail_of_file(&paths.fc_log, FC_LOG_TAIL_BYTES)
        ))
    }

    /// Everything after the environment directory exists. On error the caller
    /// kills whatever was spawned (left in `slot`) and removes the directory.
    async fn boot(
        &self,
        spec: &EnvironmentSpec,
        paths: &EnvPaths,
        created_at: Instant,
        slot: &mut Option<Child>,
    ) -> Result<EnvironmentHandle, ProviderError> {
        let env_id = spec.environment_id.as_str();

        // 1. Stage the artifact as /app (0755).
        tokio::fs::copy(&spec.artifact.path, &paths.stage_app)
            .await
            .map_err(|e| {
                ProviderError::Boot(format!(
                    "copy artifact {} -> {}: {e}",
                    spec.artifact.path.display(),
                    paths.stage_app.display()
                ))
            })?;
        tokio::fs::set_permissions(&paths.stage_app, std::fs::Permissions::from_mode(0o755))
            .await?;

        // 2. Function drive.
        let drive_bytes = function_drive_size_bytes(spec.artifact.size_bytes);
        create_sparse_image(&paths.function_drive, drive_bytes)?;
        let mkfs = tokio::process::Command::new(&self.cfg.mkfs_ext4)
            .args(mkfs_args(&paths.stage, &paths.function_drive, drive_bytes))
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .map_err(|e| {
                ProviderError::Boot(format!(
                    "cannot run {}: {e} (install e2fsprogs >= 1.43)",
                    self.cfg.mkfs_ext4.display()
                ))
            })?;
        if !mkfs.status.success() {
            return Err(ProviderError::Boot(format!(
                "mkfs.ext4 failed ({}): {}",
                mkfs.status,
                String::from_utf8_lossy(&mkfs.stderr).trim()
            )));
        }

        // 3. Listen for the guest-initiated vsock connection BEFORE the VM starts.
        let listener = UnixListener::bind(&paths.vsock_listener).map_err(|e| {
            ProviderError::Boot(format!("bind {}: {e}", paths.vsock_listener.display()))
        })?;

        // 4. Spawn Firecracker in its own process group.
        let instance_id = instance_id_for(env_id);
        let child =
            spawn_firecracker(&self.cfg.firecracker_binary, paths, &instance_id).map_err(|e| {
                ProviderError::Boot(format!(
                    "spawn {}: {e}",
                    self.cfg.firecracker_binary.display()
                ))
            })?;
        let pid = child
            .id()
            .ok_or_else(|| ProviderError::Internal("spawned child has no pid".into()))?;
        write_pid_file(&paths.pid_file, pid)?;
        *slot = Some(child);
        let child = slot.as_mut().expect("child stored above");
        tracing::info!(env_id, pid, instance_id, "firecracker spawned");

        // 5. Wait for the API socket.
        let start = Instant::now();
        loop {
            if paths.api_sock.exists() {
                break;
            }
            if let Ok(Some(status)) = child.try_wait() {
                return Err(Self::boot_error(
                    paths,
                    format!("firecracker exited before creating the API socket ({status})"),
                ));
            }
            if start.elapsed() > API_SOCKET_WAIT {
                return Err(Self::boot_error(
                    paths,
                    format!(
                        "API socket {} did not appear within {API_SOCKET_WAIT:?}",
                        paths.api_sock.display()
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // 6. Configure and start the microVM (order fixed by docs/protocol.md §C).
        let api = ApiClient::new(&paths.api_sock, API_TIMEOUT);
        let vcpus = spec.resources.vcpus();
        let mem_mib = spec.resources.memory_mib;
        let boot_args = compose_boot_args(
            spec.architecture,
            env_id,
            self.cfg.vsock_port,
            self.cfg.boot_args_extra.as_deref(),
        );
        let calls: [(&str, serde_json::Value); 6] = [
            (
                "/machine-config",
                serde_json::json!({"vcpu_count": vcpus, "mem_size_mib": mem_mib, "smt": false}),
            ),
            (
                "/boot-source",
                serde_json::json!({"kernel_image_path": self.cfg.kernel, "boot_args": boot_args}),
            ),
            (
                "/drives/rootfs",
                serde_json::json!({
                    "drive_id": "rootfs",
                    "path_on_host": self.cfg.rootfs,
                    "is_root_device": true,
                    "is_read_only": true
                }),
            ),
            (
                "/drives/function",
                serde_json::json!({
                    "drive_id": "function",
                    "path_on_host": paths.function_drive,
                    "is_root_device": false,
                    "is_read_only": true
                }),
            ),
            (
                "/vsock",
                serde_json::json!({"guest_cid": GUEST_CID, "uds_path": paths.vsock_uds}),
            ),
            (
                "/actions",
                serde_json::json!({"action_type": "InstanceStart"}),
            ),
        ];
        for (path, body) in &calls {
            if let Err(e) = api.put(path, body).await {
                let msg = match child.try_wait() {
                    Ok(Some(status)) => format!(
                        "firecracker exited before configuration completed ({status}); last API error: {e}"
                    ),
                    _ => format!("firecracker API: {e}"),
                };
                return Err(Self::boot_error(paths, msg));
            }
        }
        tracing::info!(env_id, vcpus, mem_mib, "InstanceStart accepted");

        // 7. Wait for the guest bridge (or an early VMM exit).
        let stream = tokio::select! {
            accepted = tokio::time::timeout(spec.connect_timeout, listener.accept()) => match accepted {
                Ok(Ok((stream, _))) => stream,
                Ok(Err(e)) => {
                    return Err(Self::boot_error(paths, format!("accept on {}: {e}", paths.vsock_listener.display())));
                }
                Err(_) => {
                    return Err(Self::boot_error(
                        paths,
                        format!(
                            "timeout waiting for guest bridge on {} after {:?}",
                            paths.vsock_listener.display(),
                            spec.connect_timeout
                        ),
                    ));
                }
            },
            status = child.wait() => {
                let status = status.map(|s| s.to_string()).unwrap_or_else(|e| e.to_string());
                return Err(Self::boot_error(
                    paths,
                    format!("firecracker exited before the guest bridge connected ({status})"),
                ));
            }
        };
        let connected_at = Instant::now();
        drop(listener);

        // 8. Evidence (never secrets).
        let kernel_sha256 = self.digests.digest(&self.cfg.kernel).await.ok();
        let rootfs_sha256 = self.digests.digest(&self.cfg.rootfs).await.ok();
        let mut details = serde_json::Map::new();
        details.insert("provider".into(), "firecracker".into());
        details.insert(
            "firecracker_version".into(),
            self.version
                .clone()
                .map(Into::into)
                .unwrap_or(serde_json::Value::Null),
        );
        details.insert("instance_id".into(), instance_id.into());
        details.insert(
            "kernel_path".into(),
            self.cfg.kernel.display().to_string().into(),
        );
        details.insert(
            "kernel_sha256".into(),
            kernel_sha256
                .map(Into::into)
                .unwrap_or(serde_json::Value::Null),
        );
        details.insert(
            "rootfs_path".into(),
            self.cfg.rootfs.display().to_string().into(),
        );
        details.insert(
            "rootfs_sha256".into(),
            rootfs_sha256
                .map(Into::into)
                .unwrap_or(serde_json::Value::Null),
        );
        details.insert("vcpus".into(), vcpus.into());
        details.insert("mem_mib".into(), mem_mib.into());
        details.insert("vsock_port".into(), self.cfg.vsock_port.into());
        details.insert("function_drive_bytes".into(), drive_bytes.into());
        details.insert("env_dir".into(), paths.dir.display().to_string().into());
        details.insert(
            "console_log".into(),
            paths.console_log.display().to_string().into(),
        );
        details.insert(
            "boot_ms".into(),
            (connected_at.duration_since(created_at).as_millis() as u64).into(),
        );

        Ok(EnvironmentHandle {
            environment_id: spec.environment_id.clone(),
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

    /// Move console.log / fc.log to `<workdir>/_archive/<env_id>/` and prune
    /// the archive to [`ARCHIVE_KEEP`] entries. Returns archived paths.
    async fn archive_logs(&self, env_id: &EnvironmentId, paths: &EnvPaths) -> Vec<String> {
        let mut archived = Vec::new();
        let dest = self.archive_root().join(env_id.as_str());
        if tokio::fs::create_dir_all(&dest).await.is_err() {
            return archived;
        }
        for src in [&paths.console_log, &paths.fc_log] {
            if !src.exists() {
                continue;
            }
            let Some(name) = src.file_name() else {
                continue;
            };
            let target = dest.join(name);
            let moved = match tokio::fs::rename(src, &target).await {
                Ok(()) => true,
                Err(_) => tokio::fs::copy(src, &target).await.is_ok(),
            };
            if moved {
                archived.push(target.display().to_string());
            }
        }
        self.prune_archive().await;
        archived
    }

    async fn prune_archive(&self) {
        let root = self.archive_root();
        let Ok(mut rd) = tokio::fs::read_dir(&root).await else {
            return;
        };
        let mut names = Vec::new();
        while let Ok(Some(e)) = rd.next_entry().await {
            if e.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                names.push(e.file_name());
            }
        }
        if names.len() <= ARCHIVE_KEEP {
            return;
        }
        // ULID-based ids sort chronologically; drop the oldest.
        names.sort();
        for name in names.iter().take(names.len() - ARCHIVE_KEEP) {
            let _ = tokio::fs::remove_dir_all(root.join(name)).await;
        }
    }

    /// Remove every host artefact of an environment directory (best effort).
    async fn remove_env_files(paths: &EnvPaths, cleaned: &mut Vec<String>) {
        for p in [
            &paths.api_sock,
            &paths.vsock_uds,
            &paths.vsock_listener,
            &paths.function_drive,
            &paths.pid_file,
            &paths.stage_app,
        ] {
            if tokio::fs::remove_file(p).await.is_ok() {
                cleaned.push(p.display().to_string());
            }
        }
        if tokio::fs::remove_dir_all(&paths.stage).await.is_ok() {
            cleaned.push(paths.stage.display().to_string());
        }
        match tokio::fs::remove_dir_all(&paths.dir).await {
            Ok(()) => cleaned.push(paths.dir.display().to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(dir = %paths.dir.display(), error = %e, "env dir not removed"),
        }
    }

    /// Kill a tracked child, waiting for a self-initiated poweroff first when
    /// `graceful`. Returns whether it was still running when we started.
    async fn stop_tracked(&self, t: &mut Tracked, graceful: bool) -> bool {
        let was_running = matches!(t.child.try_wait(), Ok(None));
        if was_running && graceful {
            let deadline = Instant::now() + self.cfg.kill_grace;
            while Instant::now() < deadline {
                if !matches!(t.child.try_wait(), Ok(None)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        kill_process_group(t.pid);
        let _ = t.child.wait().await;
        was_running
    }
}

/// Validate an inspected ELF against the revision's and the host's architecture.
pub fn check_elf(
    info: &ElfInfo,
    requested: Architecture,
    host: Option<Architecture>,
) -> Result<(), ProviderError> {
    let Some(arch) = info.architecture() else {
        return Err(ProviderError::ArtifactRejected(format!(
            "unsupported ELF machine {:#x} (expected x86_64 or aarch64)",
            info.machine
        )));
    };
    if arch != requested {
        return Err(ProviderError::ArtifactRejected(format!(
            "artifact is {} but the revision declares {}",
            arch.as_str(),
            requested.as_str()
        )));
    }
    match host {
        Some(h) if h != arch => {
            return Err(ProviderError::ArtifactRejected(format!(
                "artifact is {} but this host is {}; Firecracker runs same-architecture guests only",
                arch.as_str(),
                h.as_str()
            )));
        }
        Some(_) => {}
        None => {
            return Err(ProviderError::Unavailable(format!(
                "host architecture {} is not supported",
                std::env::consts::ARCH
            )));
        }
    }
    if !info.is_static() {
        return Err(ProviderError::ArtifactRejected(format!(
            "dynamically linked (PT_INTERP present); the guest rootfs has no libc. \
             Build with --target {}-unknown-linux-musl",
            arch.as_str()
        )));
    }
    Ok(())
}

#[async_trait]
impl ExecutionProvider for FirecrackerProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Firecracker
    }

    fn capabilities(&self) -> Capabilities {
        Self::capability_table()
    }

    async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
        Ok(run_preflight(&self.cfg, self.version.as_deref(), &self.digests).await)
    }

    async fn validate_artifact(
        &self,
        artifact: &ArtifactLocation,
        architecture: Architecture,
    ) -> Result<(), ProviderError> {
        let path = artifact.path.clone();
        let info = tokio::task::spawn_blocking(move || inspect_elf_file(&path))
            .await
            .map_err(|e| ProviderError::Internal(format!("elf inspection task: {e}")))?
            .map_err(|e| ProviderError::ArtifactRejected(e.to_string()))?;
        check_elf(&info, architecture, Architecture::host())
    }

    async fn create_environment(
        &self,
        spec: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError> {
        let created_at = Instant::now();
        let paths = self.paths_for(&spec.environment_id);
        match Architecture::host() {
            Some(h) if h == spec.architecture => {}
            Some(h) => {
                return Err(ProviderError::InvalidSpec(format!(
                    "spec architecture {} does not match host {}",
                    spec.architecture.as_str(),
                    h.as_str()
                )));
            }
            None => {
                return Err(ProviderError::Unavailable(format!(
                    "host architecture {} is not supported",
                    std::env::consts::ARCH
                )));
            }
        }
        if spec.egress != EgressProfile::None {
            return Err(ProviderError::InvalidSpec(format!(
                "egress profile {:?} is not supported (P1 configures no network device)",
                spec.egress
            )));
        }
        let longest = paths.longest_socket_path_len();
        if longest > MAX_UNIX_SOCKET_PATH {
            return Err(ProviderError::InvalidSpec(format!(
                "unix socket path would be {longest} bytes (max {MAX_UNIX_SOCKET_PATH}); use a shorter workdir than {}",
                self.cfg.workdir.display()
            )));
        }
        if paths.dir.exists() || self.running.lock().await.contains_key(&spec.environment_id) {
            return Err(ProviderError::InvalidSpec(format!(
                "environment {} already exists at {}",
                spec.environment_id,
                paths.dir.display()
            )));
        }
        tokio::fs::create_dir_all(&paths.stage).await?;

        let mut slot: Option<Child> = None;
        match self.boot(&spec, &paths, created_at, &mut slot).await {
            Ok(handle) => {
                let child = slot.take().expect("boot succeeded with a child");
                let pid = handle.evidence.host_pid.unwrap_or_default();
                self.running
                    .lock()
                    .await
                    .insert(spec.environment_id.clone(), Tracked { pid, child });
                Ok(handle)
            }
            Err(err) => {
                tracing::warn!(env_id = %spec.environment_id, error = %err, "boot failed; cleaning up");
                if let Some(mut child) = slot.take() {
                    if let Some(pid) = child.id() {
                        kill_process_group(pid);
                    }
                    let _ = child.wait().await;
                }
                let mut cleaned = Vec::new();
                let _ = self.archive_logs(&spec.environment_id, &paths).await;
                Self::remove_env_files(&paths, &mut cleaned).await;
                Err(err)
            }
        }
    }

    async fn terminate_environment(
        &self,
        environment_id: &EnvironmentId,
        reason: TerminateReason,
    ) -> Result<TerminateReport, ProviderError> {
        let paths = self.paths_for(environment_id);
        let tracked = self.running.lock().await.remove(environment_id);
        if tracked.is_none() && !paths.dir.exists() {
            return Ok(TerminateReport::default());
        }
        let graceful = matches!(
            reason,
            TerminateReason::Completed | TerminateReason::Shutdown
        );
        let mut cleaned = Vec::new();
        let mut was_running = false;

        if let Some(mut t) = tracked {
            was_running = self.stop_tracked(&mut t, graceful).await;
            cleaned.push(format!("process-group:{}", t.pid));
        } else if let Some(pid) = read_pid_file(&paths.pid_file)
            && pid_alive(pid)
        {
            // Not spawned by this process (e.g. a previous gateway run). Only
            // kill when the pid can be proven to be *our* Firecracker.
            match pid_belongs_to_env(pid, &paths.dir) {
                Some(true) => {
                    was_running = true;
                    if graceful {
                        wait_pid_gone(pid, self.cfg.kill_grace).await;
                    }
                    kill_process_group(pid);
                    wait_pid_gone(pid, Duration::from_secs(1)).await;
                    cleaned.push(format!("process-group:{pid}"));
                }
                Some(false) => {
                    tracing::warn!(pid, "pid file points at an unrelated process; not killed")
                }
                None => tracing::warn!(pid, "cannot verify pid ownership on this host; not killed"),
            }
        }

        let archived = self.archive_logs(environment_id, &paths).await;
        cleaned.extend(archived.into_iter().map(|p| format!("archived:{p}")));
        Self::remove_env_files(&paths, &mut cleaned).await;
        tracing::info!(env_id = %environment_id, ?reason, was_running, "environment terminated");
        Ok(TerminateReport {
            was_running,
            cleaned,
        })
    }

    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        if let Some(t) = self.running.lock().await.get_mut(environment_id) {
            return match t.child.try_wait() {
                Ok(None) => Ok(EnvironmentObservation::Running {
                    host_pid: Some(t.pid),
                }),
                Ok(Some(status)) => {
                    use std::os::unix::process::ExitStatusExt;
                    Ok(EnvironmentObservation::Exited {
                        exit_code: status.code(),
                        signal: status.signal(),
                    })
                }
                Err(e) => Err(ProviderError::Internal(format!("try_wait: {e}"))),
            };
        }
        let paths = self.paths_for(environment_id);
        if !paths.dir.exists() {
            return Ok(EnvironmentObservation::NotFound);
        }
        match read_pid_file(&paths.pid_file) {
            Some(pid) if pid_alive(pid) && pid_belongs_to_env(pid, &paths.dir) != Some(false) => {
                Ok(EnvironmentObservation::Running {
                    host_pid: Some(pid),
                })
            }
            _ => Ok(EnvironmentObservation::Exited {
                exit_code: None,
                signal: None,
            }),
        }
    }

    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
        let mut ids = Vec::new();
        let mut rd = match tokio::fs::read_dir(&self.cfg.workdir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name == ARCHIVE_DIR {
                continue;
            }
            if let Ok(id) = EnvironmentId::parse(name) {
                ids.push(id);
            }
        }
        ids.sort();
        Ok(ids)
    }
}

/// Helper for tests and tools: an artifact location for a file on disk.
pub fn artifact_location_for(path: &Path) -> std::io::Result<ArtifactLocation> {
    let bytes = std::fs::read(path)?;
    Ok(ArtifactLocation {
        path: path.to_path_buf(),
        digest: tachyon_serverless_domain::Sha256Digest::of_bytes(&bytes),
        size_bytes: bytes.len() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elf::{EM_AARCH64, EM_X86_64, ET_DYN, ET_EXEC, PT_DYNAMIC, PT_INTERP, craft_elf};
    use tachyon_serverless_domain::{RevisionId, Sha256Digest, TenantId};

    fn provider(dir: &Path) -> FirecrackerProvider {
        FirecrackerProvider::new(FirecrackerConfig {
            firecracker_binary: dir.join("no-firecracker"),
            kernel: dir.join("vmlinux"),
            rootfs: dir.join("rootfs.ext4"),
            workdir: dir.join("run"),
            vsock_port: 5000,
            ..Default::default()
        })
    }

    #[test]
    fn capabilities_are_explicit() {
        let c = FirecrackerProvider::capability_table();
        assert_eq!(c.isolation, IsolationLevel::MicroVm);
        assert!(!c.dev_only);
        assert!(c.create_terminate.is_supported());
        assert!(c.observe.is_supported());
        assert!(c.enforce_deadline.is_supported());
        assert!(c.egress_none.is_supported());
        assert!(matches!(
            c.enforce_resource_limits,
            Support::Unverified { .. }
        ));
        assert!(matches!(c.egress_restricted, Support::Unsupported { .. }));
        assert!(matches!(c.egress_public_web, Support::Unsupported { .. }));
        assert!(matches!(c.host_metering, Support::Unverified { .. }));
        for s in [
            &c.idle_quiesce,
            &c.idle_resume,
            &c.snapshot_create,
            &c.snapshot_clone,
        ] {
            assert!(matches!(s, Support::Unsupported { .. }));
        }
    }

    #[test]
    fn elf_checks_cover_arch_and_linkage() {
        let x = crate::elf::inspect_elf_bytes(&craft_elf(EM_X86_64, ET_EXEC, &[1])).unwrap();
        let a = crate::elf::inspect_elf_bytes(&craft_elf(EM_AARCH64, ET_DYN, &[1, PT_DYNAMIC]))
            .unwrap();
        let dynamic = crate::elf::inspect_elf_bytes(&craft_elf(
            EM_X86_64,
            ET_DYN,
            &[1, PT_INTERP, PT_DYNAMIC],
        ))
        .unwrap();
        assert!(check_elf(&x, Architecture::X86_64, Some(Architecture::X86_64)).is_ok());
        assert!(check_elf(&a, Architecture::Aarch64, Some(Architecture::Aarch64)).is_ok());
        assert!(matches!(
            check_elf(&x, Architecture::Aarch64, Some(Architecture::Aarch64)),
            Err(ProviderError::ArtifactRejected(m)) if m.contains("revision declares")
        ));
        assert!(matches!(
            check_elf(&x, Architecture::X86_64, Some(Architecture::Aarch64)),
            Err(ProviderError::ArtifactRejected(m)) if m.contains("same-architecture")
        ));
        assert!(matches!(
            check_elf(&dynamic, Architecture::X86_64, Some(Architecture::X86_64)),
            Err(ProviderError::ArtifactRejected(m)) if m.contains("x86_64-unknown-linux-musl")
        ));
        assert!(matches!(
            check_elf(&x, Architecture::X86_64, None),
            Err(ProviderError::Unavailable(_))
        ));
        let unknown = crate::elf::inspect_elf_bytes(&craft_elf(0x28, ET_EXEC, &[])).unwrap();
        assert!(matches!(
            check_elf(&unknown, Architecture::X86_64, Some(Architecture::X86_64)),
            Err(ProviderError::ArtifactRejected(_))
        ));
    }

    #[tokio::test]
    async fn validate_artifact_reads_file_and_rejects_non_elf() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let host = Architecture::host().unwrap();
        let good = dir.path().join("good");
        std::fs::write(
            &good,
            craft_elf(crate::elf::machine_for(host), ET_DYN, &[1, PT_DYNAMIC]),
        )
        .unwrap();
        p.validate_artifact(&artifact_location_for(&good).unwrap(), host)
            .await
            .unwrap();
        let bad = dir.path().join("bad");
        std::fs::write(&bad, b"#!/bin/sh\necho hi\n").unwrap();
        assert!(matches!(
            p.validate_artifact(&artifact_location_for(&bad).unwrap(), host)
                .await,
            Err(ProviderError::ArtifactRejected(_))
        ));
    }

    #[tokio::test]
    async fn terminate_missing_env_is_idempotent_noop() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        let r = p
            .terminate_environment(&id, TerminateReason::Completed)
            .await
            .unwrap();
        assert!(!r.was_running);
        assert!(r.cleaned.is_empty());
        let r2 = p
            .terminate_environment(&id, TerminateReason::Timeout)
            .await
            .unwrap();
        assert_eq!(r2, TerminateReport::default());
        assert_eq!(
            p.observe_environment(&id).await.unwrap(),
            EnvironmentObservation::NotFound
        );
    }

    #[tokio::test]
    async fn terminate_cleans_a_stale_env_dir_and_archives_logs() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        let paths = p.paths_for(&id);
        std::fs::create_dir_all(&paths.stage).unwrap();
        std::fs::write(&paths.stage_app, b"x").unwrap();
        std::fs::write(&paths.function_drive, b"img").unwrap();
        std::fs::write(&paths.console_log, b"guest console").unwrap();
        std::fs::write(&paths.fc_log, b"fc log").unwrap();
        // A pid that certainly does not exist: observe reports Exited, terminate does not kill.
        write_pid_file(&paths.pid_file, u32::MAX / 2).unwrap();
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
        assert!(!paths.dir.exists());
        assert!(r.cleaned.iter().any(|c| c.ends_with("function.ext4")));
        assert!(
            r.cleaned
                .iter()
                .any(|c| c == &paths.dir.display().to_string())
        );
        let archive = dir.path().join("run").join(ARCHIVE_DIR).join(id.as_str());
        assert_eq!(
            std::fs::read(archive.join("console.log")).unwrap(),
            b"guest console"
        );
        assert!(r.cleaned.iter().any(|c| c.starts_with("archived:")));
        assert!(
            p.list_environments().await.unwrap().is_empty(),
            "_archive is excluded"
        );
    }

    #[tokio::test]
    async fn archive_is_pruned_to_keep_limit() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        for _ in 0..(ARCHIVE_KEEP + 3) {
            std::fs::create_dir_all(p.archive_root().join(EnvironmentId::generate().as_str()))
                .unwrap();
        }
        p.prune_archive().await;
        let n = std::fs::read_dir(p.archive_root()).unwrap().count();
        assert_eq!(n, ARCHIVE_KEEP);
    }

    #[tokio::test]
    async fn create_rejects_existing_dir_before_touching_anything() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        std::fs::create_dir_all(p.paths_for(&id).dir).unwrap();
        let spec = EnvironmentSpec {
            environment_id: id,
            tenant_id: TenantId::generate(),
            revision_id: RevisionId::generate(),
            artifact: ArtifactLocation {
                path: dir.path().join("nope"),
                digest: Sha256Digest::of_bytes(b""),
                size_bytes: 0,
            },
            architecture: Architecture::host().unwrap(),
            resources: Default::default(),
            egress: EgressProfile::None,
            connect_timeout: Duration::from_secs(1),
        };
        assert!(matches!(
            p.create_environment(spec).await,
            Err(ProviderError::InvalidSpec(m)) if m.contains("already exists")
        ));
    }

    #[tokio::test]
    async fn create_rejects_non_none_egress() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let spec = EnvironmentSpec {
            environment_id: EnvironmentId::generate(),
            tenant_id: TenantId::generate(),
            revision_id: RevisionId::generate(),
            artifact: ArtifactLocation {
                path: dir.path().join("nope"),
                digest: Sha256Digest::of_bytes(b""),
                size_bytes: 0,
            },
            architecture: Architecture::host().unwrap(),
            resources: Default::default(),
            egress: EgressProfile::PublicWeb,
            connect_timeout: Duration::from_secs(1),
        };
        assert!(matches!(
            p.create_environment(spec).await,
            Err(ProviderError::InvalidSpec(m)) if m.contains("egress")
        ));
        assert!(p.list_environments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn boot_failure_cleans_up_and_reports_reason() {
        // The artifact copy step fails (missing file) after the dir was
        // created; the directory must be gone afterwards.
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        let spec = EnvironmentSpec {
            environment_id: id.clone(),
            tenant_id: TenantId::generate(),
            revision_id: RevisionId::generate(),
            artifact: ArtifactLocation {
                path: dir.path().join("missing-artifact"),
                digest: Sha256Digest::of_bytes(b""),
                size_bytes: 0,
            },
            architecture: Architecture::host().unwrap(),
            resources: Default::default(),
            egress: EgressProfile::None,
            connect_timeout: Duration::from_secs(1),
        };
        let err = p.create_environment(spec).await.unwrap_err();
        assert!(
            matches!(err, ProviderError::Boot(ref m) if m.contains("copy artifact")),
            "{err}"
        );
        assert!(!p.paths_for(&id).dir.exists());
        assert_eq!(
            p.observe_environment(&id).await.unwrap(),
            EnvironmentObservation::NotFound
        );
    }

    #[tokio::test]
    async fn preflight_never_fails_and_kind_is_firecracker() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        assert_eq!(p.kind(), ProviderKind::Firecracker);
        let r = p.preflight().await.unwrap();
        assert_eq!(r.provider, "firecracker");
        assert!(!r.checks.is_empty());
    }
}
