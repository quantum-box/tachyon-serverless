//! `ExecutionProvider` implementation for Firecracker.
//!
//! Lifecycle of one environment (docs/protocol.md §C):
//!
//! ```text
//! <workdir>/<env_id>/
//!   stage/app        artifact copy (0755)         -> mkfs.ext4 -d stage function.ext4 (removed after)
//!   function.ext4    read-only function drive     -> PUT /drives/function
//!   scratch.ext4     writable /tmp, ephemeral_storage_mib, reserved -> PUT /drives/scratch
//!   v.sock_<port>    host listener (bound BEFORE InstanceStart)
//!   fc.sock          Firecracker API socket
//!   v.sock           vsock UDS bound by Firecracker (host-initiated side, unused)
//!   fc.log           Firecracker log (--log-path)
//!   console.log      guest serial console (Firecracker stdout/stderr, capped)
//!   fc.pid           pid of the Firecracker process (process-group leader)
//!   net.json         network lease (egress restricted / public-web only)
//! ```
//!
//! Egress `restricted` / `public-web` (PLT-4622) additionally get a host tap
//! and an nftables chain, installed and read back before the network device
//! is configured and before `InstanceStart` (crate::network, egress_gate).
//!
//! Host isolation of the VMM (PLT-4622):
//!
//! - every VMM starts inside `<cgroup root>/<parent>/<env_id>` with `cpu.max`
//!   from `cpu_millis`, `memory.max` = guest memory + overhead and `pids.max`
//!   (crate::cgroup), proven by reading `cgroup.procs` before configuration;
//! - with `[provider.firecracker.jailer]` the VMM is started by the jailer in
//!   `<chroot_base>/<exec name>/<instance id>/root` as an unprivileged uid in
//!   new mount / PID namespaces (crate::jail). The sockets above then live in
//!   that chroot and the drives, kernel, rootfs and `fc.log` are hard-linked
//!   into it; the files in `<workdir>/<env_id>/` stay the host's copies.
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
    EnvironmentStats, ExecutionProvider, IsolationLevel, PreflightReport, ProviderError, Support,
    TerminateReason, TerminateReport,
};

use crate::api::ApiClient;
use crate::boot_args::compose_boot_args;
use crate::cgroup::{CgroupLimits, EnvCgroup, HostCgroups, verify_member};
use crate::config::{CgroupMode, FirecrackerConfig};
use crate::drive::{
    create_reserved_image, create_sparse_image, function_drive_size_bytes, mkfs_args,
    scratch_drive_size_bytes, scratch_mkfs_args,
};
use crate::egress_gate::{ExpectedNic, check_planned_calls, check_vm_config};
use crate::elf::{ElfInfo, inspect_elf_file};
use crate::host_guard::{
    ConsoleCapture, HostBudget, available_bytes, check_host_budget, spawn_log_watchdog,
};
use crate::jail::JailInputs;
use crate::network::{HostNetwork, VerifiedPolicy, host_support};
use crate::preflight::{DigestCache, probe_firecracker_version, run_preflight};
use crate::vmm::{
    EnvPaths, Launcher, MAX_UNIX_SOCKET_PATH, create_fc_log, instance_id_for, kill_process_group,
    kill_vmm, pid_alive, pid_belongs_to_env, pid_cmdline_contains, pid_is_zombie, read_pid_file,
    spawn_vmm, tail_of_file, wait_pid_gone, write_pid_file,
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
/// microVM states set through `PATCH /vm` (docs/protocol.md §C; the API is the
/// one listed for Firecracker in `docs/adr/0001` under "pause / resume").
pub const VM_STATE_PAUSED: &str = "Paused";
/// See [`VM_STATE_PAUSED`].
pub const VM_STATE_RESUMED: &str = "Resumed";
const CONSOLE_TAIL_BYTES: usize = 4096;
const FC_LOG_TAIL_BYTES: usize = 2048;

/// A running VMM.
///
/// Started directly (or by a jailer that execs in place), the spawned child
/// *is* the VMM. A jailer with `--new-pid-ns` clones the VMM and exits; then
/// `child` is `None`, `pid` is the VMM recorded by the jailer and `pgid` the
/// process group the jailer led (the VMM stays in it).
struct Tracked {
    pid: u32,
    pgid: u32,
    child: Option<Child>,
    instance_id: String,
}

impl Tracked {
    /// `Some(description)` once the VMM has exited.
    fn exited(&mut self) -> Option<String> {
        match &mut self.child {
            Some(child) => match child.try_wait() {
                Ok(None) => None,
                Ok(Some(status)) => Some(status.to_string()),
                Err(e) => Some(format!("try_wait failed: {e}")),
            },
            None => {
                // An empty command line (the jailer is still exec'ing
                // Firecracker) is no verdict; a zombie is gone.
                let ours = pid_alive(self.pid)
                    && !pid_is_zombie(self.pid)
                    && pid_cmdline_contains(self.pid, self.instance_id.as_bytes()) != Some(false);
                (!ours).then(|| format!("vmm pid {} is gone", self.pid))
            }
        }
    }

    async fn wait_exit(&mut self) -> String {
        match &mut self.child {
            Some(child) => child
                .wait()
                .await
                .map(|s| s.to_string())
                .unwrap_or_else(|e| e.to_string()),
            None => loop {
                if let Some(status) = self.exited() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
        }
    }

    /// SIGKILL the VMM's process group and reap / wait for it.
    async fn kill(&mut self) {
        kill_process_group(self.pgid);
        match &mut self.child {
            Some(child) => {
                let _ = child.wait().await;
            }
            None => {
                kill_vmm(self.pid);
                wait_pid_gone(self.pid, Duration::from_secs(2)).await;
            }
        }
    }
}

/// Firecracker execution provider (Linux/KVM only; compiles everywhere).
pub struct FirecrackerProvider {
    cfg: FirecrackerConfig,
    version: Option<String>,
    running: tokio::sync::Mutex<HashMap<EnvironmentId, Tracked>>,
    digests: DigestCache,
    net: HostNetwork,
    /// [`host_support`] at construction: whether this process can enforce
    /// `restricted` / `public-web` on this host, or why not.
    net_support: Result<String, String>,
    /// Environment cgroups; `None` when `mode = "off"`, or `"best-effort"` on
    /// a host without cgroup delegation.
    cgroups: Option<HostCgroups>,
    /// [`crate::cgroup::host_support`] at construction.
    cgroup_support: Result<String, String>,
    /// File name of the Firecracker binary when the jailer is configured
    /// (the jail's directory level), or why it cannot be used.
    jail_exec: Option<Result<String, String>>,
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
        let net_support = host_support(&cfg.network);
        if let Err(reason) = &net_support {
            tracing::info!(
                reason,
                "egress restricted / public-web are unavailable on this host (egress none is unaffected)"
            );
        }
        let cgroup_support = crate::cgroup::host_support(&cfg.cgroup);
        let cgroups = match (cfg.cgroup.mode, &cgroup_support) {
            (CgroupMode::Off, _) => None,
            (CgroupMode::BestEffort, Err(reason)) => {
                tracing::warn!(
                    reason,
                    "host cgroup v2 limits are unavailable; VMMs run without cpu.max / memory.max \
                     (cgroup mode best-effort; production requires mode = \"required\")"
                );
                None
            }
            (CgroupMode::Required, Err(reason)) => {
                tracing::error!(
                    reason,
                    "host cgroup v2 limits are required but unavailable; every environment will be refused"
                );
                Some(HostCgroups::new(cfg.cgroup.clone()))
            }
            (_, Ok(_)) => Some(HostCgroups::new(cfg.cgroup.clone())),
        };
        let jail_exec = cfg
            .jailer
            .as_ref()
            .map(|_| crate::jail::exec_name(&cfg.firecracker_binary));
        if cfg.jailer.is_none() {
            tracing::info!(
                "the VMM runs without the jailer (no chroot, no dedicated uid); use \
                 [provider.firecracker.jailer] outside development"
            );
        }
        Self {
            net: HostNetwork::new(cfg.network.clone()),
            cfg,
            version,
            running: tokio::sync::Mutex::new(HashMap::new()),
            digests: DigestCache::default(),
            net_support,
            cgroups,
            cgroup_support,
            jail_exec,
        }
    }

    pub fn config(&self) -> &FirecrackerConfig {
        &self.cfg
    }

    /// Firecracker version as reported by the binary at construction.
    pub fn firecracker_version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Capability table of this provider on a host that can enforce every
    /// profile (RFC §6.3). [`ExecutionProvider::capabilities`] downgrades the
    /// network profiles to `Unsupported` with the reason when this host
    /// cannot (no `CAP_NET_ADMIN`, no nftables, not Linux, ...).
    pub fn capability_table() -> Capabilities {
        Capabilities {
            isolation: IsolationLevel::MicroVm,
            create_terminate: Support::Supported,
            observe: Support::Supported,
            enforce_deadline: Support::Supported,
            // PLT-4622, measured on real KVM and promoted from `Unverified`
            // (docs/evidence/isolation-20260917T011555Z, scripts/kvm/measure-isolation.sh):
            // - vCPU / memory (ADR-0001 M9): the guest sees 1 vCPU for 500 m and
            //   MemTotal 232 MiB for 256 MiB; allocating 512 MiB in a 128 MiB
            //   environment ends as `crash` / `Runtime.Crash`;
            // - ephemeral storage: `/tmp` is a 64 MiB scratch drive for
            //   `ephemeral_storage_mib = 64`, a fill stops with ENOSPC after
            //   58 MiB (ext4 metadata), `/` and `/function` answer EROFS, and the
            //   host lost at most 70 MiB of free space (the reserved drive).
            // - host cgroup v2 on the VMM (docs/evidence/isolation-20260917T041930Z,
            //   HOST / NOISY): every VMM ran in its own cgroup with cpu.max from
            //   cpu_millis; a 500 m tenant spinning 4 threads used 0.517 cores at
            //   most over any 5 s window while a neighbour's CPU work slowed by
            //   x1.245 and its fsync'd writes by x1.051; memory.peak stayed under
            //   guest + 64 MiB with no host oom_kill.
            // Without host cgroups the claim is withdrawn per host
            // (`resource_limits_support`). aarch64 under nested virtualization only.
            enforce_resource_limits: Support::Supported,
            // ADR-0001 M8 measured on aarch64 (docs/evidence/isolation-20260916T020934Z):
            // no network device is configured, the guest lists loopback only and
            // every connect attempt failed with NetworkUnreachable.
            egress_none: Support::Supported,
            egress_restricted: EGRESS_NETWORK_SUPPORT,
            egress_public_web: EGRESS_NETWORK_SUPPORT,
            host_metering: Support::unverified("host-side timings only; no cgroup/KVM stats"),
            // PLT-4633, measured on real KVM and promoted from `Unverified`
            // (docs/evidence/warm-20260916T162532Z, taken with
            // scripts/kvm/measure-warm.sh): 5 of 6 invocations were served warm
            // on one environment reused across 6 epochs, resume 9 ms and
            // readiness 9 ms (median), while the paused VMM used 0 CPU ticks
            // over 3 s and held its 38 MiB RSS - pausing stops the vCPUs, it
            // does not return the memory. The run is aarch64 under nested
            // virtualization; x86_64, bare metal, and the failure paths (a
            // refused resume, a guest that stops answering) are covered by
            // tests rather than by that measurement (docs/adr/0001 §5).
            idle_quiesce: Support::Supported,
            idle_resume: Support::Supported,
            snapshot_create: Support::unsupported("not implemented in P1"),
            snapshot_clone: Support::unsupported("not implemented in P1"),
            dev_only: false,
        }
    }

    fn paths_for(&self, env_id: &EnvironmentId) -> EnvPaths {
        let paths = EnvPaths::new(&self.cfg.workdir, env_id.as_str(), self.cfg.vsock_port);
        match (&self.cfg.jailer, &self.jail_exec) {
            (Some(jailer), Some(Ok(exec))) => paths.jailed(
                crate::jail::layout(jailer, exec, &instance_id_for(env_id.as_str())),
                self.cfg.vsock_port,
            ),
            _ => paths,
        }
    }

    /// Whether `pid` (read from a pid file) is this environment's VMM: its
    /// command line names the environment directory (direct launch) or the
    /// instance id (jailed launch). `None` when that cannot be determined.
    fn pid_is_ours(pid: u32, env_id: &EnvironmentId, paths: &EnvPaths) -> Option<bool> {
        let by_dir = pid_belongs_to_env(pid, &paths.dir);
        let by_id = pid_cmdline_contains(pid, instance_id_for(env_id.as_str()).as_bytes());
        match (by_dir, by_id) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (None, None) => None,
            _ => Some(false),
        }
    }

    /// Remove the cgroup and the jail of an environment (whatever of them
    /// exists). The cgroup is killed first, so nothing survives in it.
    async fn release_host(
        &self,
        environment_id: &EnvironmentId,
        paths: &EnvPaths,
        cleaned: &mut Vec<String>,
    ) {
        if let Some(cgroups) = &self.cgroups {
            let path = cgroups.env_dir(environment_id.as_str());
            match cgroups.remove(environment_id.as_str()).await {
                Ok(Some(stats)) => {
                    tracing::info!(
                        env_id = %environment_id,
                        cgroup = %path.display(),
                        stats = %stats,
                        "cgroup stats at teardown"
                    );
                    cleaned.push(format!("cgroup:{}", path.display()));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(env_id = %environment_id, error = %e, "cgroup not removed");
                    cleaned.push(format!("cgroup-remove-failed:{e}"));
                }
            }
        }
        if let (Some(jailer), Some(jail)) = (&self.cfg.jailer, &paths.jail) {
            match crate::jail::remove(jailer, jail) {
                Ok(true) => cleaned.push(format!("jail:{}", jail.jail_dir.display())),
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(env_id = %environment_id, error = %e, "jail not removed");
                    cleaned.push(format!("jail-remove-failed:{e}"));
                }
            }
        }
    }

    /// Whether host isolation state (cgroup, jail) of an environment exists.
    fn host_state_exists(&self, environment_id: &EnvironmentId, paths: &EnvPaths) -> bool {
        self.cgroups
            .as_ref()
            .is_some_and(|c| c.env_dir(environment_id.as_str()).exists())
            || paths.jail.as_ref().is_some_and(|j| j.jail_dir.exists())
    }

    fn archive_root(&self) -> std::path::PathBuf {
        self.cfg.workdir.join(ARCHIVE_DIR)
    }

    /// [`Self::boot_error`] for a VMM that has exited: the console pipe is
    /// drained to EOF first (bounded), so the tail contains its last words.
    async fn boot_error_after_exit(
        paths: &EnvPaths,
        console: &ConsoleCapture,
        msg: impl std::fmt::Display,
    ) -> ProviderError {
        console.wait_finished(Duration::from_secs(1)).await;
        Self::boot_error(paths, msg)
    }

    /// Run `mkfs.ext4` with `args`; `what` names the drive in errors.
    async fn run_mkfs(
        &self,
        args: Vec<std::ffi::OsString>,
        what: &str,
    ) -> Result<(), ProviderError> {
        let mkfs = tokio::process::Command::new(&self.cfg.mkfs_ext4)
            .args(args)
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
                "mkfs.ext4 ({what}) failed ({}): {}",
                mkfs.status,
                String::from_utf8_lossy(&mkfs.stderr).trim()
            )));
        }
        Ok(())
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
        slot: &mut Option<Tracked>,
    ) -> Result<EnvironmentHandle, ProviderError> {
        let env_id = spec.environment_id.as_str();

        // 0. Host disk budget (PLT-4622): refuse before anything is written
        //    when this environment's worst case would eat into the reserve.
        let drive_bytes = function_drive_size_bytes(spec.artifact.size_bytes);
        let scratch_bytes = scratch_drive_size_bytes(spec.resources.ephemeral_storage_mib);
        let budget = HostBudget {
            staged_artifact_bytes: spec.artifact.size_bytes,
            function_drive_bytes: drive_bytes,
            scratch_drive_bytes: scratch_bytes,
            console_log_max_bytes: self.cfg.console_log_max_bytes,
            fc_log_max_bytes: self.cfg.fc_log_max_bytes,
            // The jailer copies the Firecracker binary into every chroot.
            vmm_binary_copy_bytes: match &self.cfg.jailer {
                Some(_) => std::fs::metadata(&self.cfg.firecracker_binary)
                    .map(|m| m.len())
                    .unwrap_or(0),
                None => 0,
            },
        };
        let available = available_bytes(&paths.dir).map_err(|e| {
            ProviderError::Unavailable(format!(
                "cannot read free space of {}: {e}",
                paths.dir.display()
            ))
        })?;
        check_host_budget(available, &budget, self.cfg.min_host_free_bytes)
            .map_err(ProviderError::Unavailable)?;

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

        // 2. Function drive. The staged copy is not needed once it is built,
        //    so it does not count twice against the host disk.
        create_sparse_image(&paths.function_drive, drive_bytes)?;
        self.run_mkfs(
            mkfs_args(&paths.stage, &paths.function_drive, drive_bytes),
            "function drive",
        )
        .await?;
        tokio::fs::remove_dir_all(&paths.stage).await?;

        // 2b. Scratch drive (PLT-4622): the guest's only writable, host-disk
        //     backed storage, exactly `ephemeral_storage_mib` and reserved now.
        let scratch_started = Instant::now();
        let scratch_reserved =
            create_reserved_image(&paths.scratch_drive, scratch_bytes).map_err(|e| {
                ProviderError::Unavailable(format!(
                    "cannot reserve {scratch_bytes} bytes for the scratch drive {}: {e}",
                    paths.scratch_drive.display()
                ))
            })?;
        self.run_mkfs(
            scratch_mkfs_args(&paths.scratch_drive, scratch_bytes),
            "scratch drive",
        )
        .await?;
        let scratch_ms = scratch_started.elapsed().as_millis() as u64;
        if !scratch_reserved {
            tracing::warn!(
                env_id,
                path = %paths.scratch_drive.display(),
                "fallocate is not supported here; the scratch drive is sparse and its space is not reserved"
            );
        }

        // 2c. Egress policy (PLT-4622): tap + nftables chain, read back. Nothing
        //     that could carry a packet exists in the VM before this passed.
        let policy: Option<VerifiedPolicy> = if spec.egress == EgressProfile::None {
            None
        } else {
            let policy = self
                .net
                .setup(
                    &self.cfg.workdir,
                    &paths.dir,
                    env_id,
                    spec.egress,
                    &spec.egress_allow,
                    self.cfg.jailer.as_ref().map(|j| (j.uid, j.gid)),
                )
                .await
                .map_err(|e| {
                    ProviderError::Boot(format!(
                        "egress gate: the {} policy could not be installed and verified: {e}",
                        spec.egress.as_str()
                    ))
                })?;
            tracing::info!(
                env_id,
                egress = spec.egress.as_str(),
                tap = %policy.lease().tap,
                guest_ip = %policy.lease().guest_ip,
                rules = policy.rule_count(),
                ms = policy.verified_ms(),
                "egress policy installed and verified"
            );
            Some(policy)
        };
        let expected_nic: Option<ExpectedNic> = policy.as_ref().map(|p| p.lease().expected_nic());

        // 2d. Host cgroup (PLT-4622): cpu.max / memory.max / pids.max, read
        //     back, before the VMM exists. Required mode refuses to go on.
        let limits = CgroupLimits::for_resources(
            spec.resources.cpu_millis,
            spec.resources.memory_mib,
            &self.cfg.cgroup,
        );
        let cgroup: Option<EnvCgroup> = match &self.cgroups {
            None => None,
            Some(cgroups) => match cgroups.create(env_id, limits).await {
                Ok(cg) => Some(cg),
                Err(e) if self.cfg.cgroup.mode == CgroupMode::Required => {
                    return Err(ProviderError::Unavailable(format!(
                        "host cgroup limits are required and could not be applied: {e}"
                    )));
                }
                Err(e) => {
                    tracing::warn!(env_id, error = %e, "host cgroup limits not applied (best-effort)");
                    None
                }
            },
        };

        // 2e. Jail (PLT-4622): hard-link what the VMM opens into its chroot.
        if let (Some(jailer), Some(jail)) = (&self.cfg.jailer, &paths.jail) {
            create_fc_log(paths)?;
            crate::jail::prepare(
                jailer,
                jail,
                &JailInputs {
                    kernel: &self.cfg.kernel,
                    rootfs: &self.cfg.rootfs,
                    function_drive: &paths.function_drive,
                    scratch_drive: &paths.scratch_drive,
                    fc_log: &paths.fc_log,
                },
            )
            .map_err(|e| ProviderError::Boot(format!("jail: {e}")))?;
        }

        // 3. Listen for the guest-initiated vsock connection BEFORE the VM starts.
        let listener = UnixListener::bind(&paths.vsock_listener).map_err(|e| {
            ProviderError::Boot(format!("bind {}: {e}", paths.vsock_listener.display()))
        })?;
        if let Some(jailer) = &self.cfg.jailer {
            // The unprivileged VMM connects to it.
            crate::jail::chown(&paths.vsock_listener, jailer.uid, jailer.gid)
                .map_err(ProviderError::Boot)?;
        }

        // 4. Spawn the VMM in its own process group (and cgroup).
        let instance_id = instance_id_for(env_id);
        let launcher = match &self.cfg.jailer {
            Some(jailer) => Launcher::Jailer {
                jailer,
                firecracker: &self.cfg.firecracker_binary,
            },
            None => Launcher::Direct {
                binary: &self.cfg.firecracker_binary,
            },
        };
        let (child, console) = spawn_vmm(
            launcher,
            paths,
            &instance_id,
            self.cfg.console_log_max_bytes,
            cgroup.as_ref().map(EnvCgroup::procs_fd),
        )
        .map_err(|e| {
            let what = match &self.cfg.jailer {
                Some(j) => j.binary.display().to_string(),
                None => self.cfg.firecracker_binary.display().to_string(),
            };
            ProviderError::Boot(format!("spawn {what}: {e}"))
        })?;
        // fc.log is written by Firecracker itself; a watchdog keeps it bounded
        // until the environment directory is removed.
        spawn_log_watchdog(
            paths.fc_log.clone(),
            paths.dir.clone(),
            self.cfg.fc_log_max_bytes,
            env_id.to_owned(),
        );
        let spawned = child
            .id()
            .ok_or_else(|| ProviderError::Internal("spawned child has no pid".into()))?;
        *slot = Some(Tracked {
            pid: spawned,
            pgid: spawned,
            child: Some(child),
            instance_id: instance_id.clone(),
        });
        let vmm = slot.as_mut().expect("tracked stored above");
        if let (Some(jailer), Some(jail)) = (&self.cfg.jailer, &paths.jail)
            && jailer.new_pid_ns
        {
            // The jailer clones the VMM into a new PID namespace, records its
            // pid in the chroot and exits.
            let start = Instant::now();
            loop {
                let exited = vmm.child.as_mut().and_then(|c| c.try_wait().ok().flatten());
                if let Some(status) = exited {
                    if !status.success() {
                        return Err(Self::boot_error_after_exit(
                            paths,
                            &console,
                            format!("jailer exited with {status} before starting the VMM"),
                        )
                        .await);
                    }
                    break;
                }
                if start.elapsed() > API_SOCKET_WAIT {
                    return Err(Self::boot_error(
                        paths,
                        format!(
                            "the jailer did not hand over to the VMM within {API_SOCKET_WAIT:?}"
                        ),
                    ));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            vmm.child = None;
            vmm.pid = crate::jail::read_vmm_pid(jail).ok_or_else(|| {
                Self::boot_error(
                    paths,
                    format!(
                        "the jailer exited without recording the VMM pid in {}",
                        jail.pid_file().display()
                    ),
                )
            })?;
        }
        let pid = vmm.pid;
        write_pid_file(&paths.pid_file, pid)?;
        // Placement proof: the VMM (not only its launcher) is in the cgroup.
        if let Some(cg) = &cgroup {
            match verify_member(&self.cfg.cgroup.root, &cg.path, pid) {
                Ok(()) => {}
                Err(e) if self.cfg.cgroup.mode == CgroupMode::Required => {
                    return Err(Self::boot_error(paths, format!("host cgroup: {e}")));
                }
                Err(e) => tracing::warn!(env_id, error = %e, "VMM cgroup placement not verified"),
            }
        }
        tracing::info!(
            env_id,
            pid,
            instance_id,
            jailed = self.cfg.jailer.is_some(),
            cgroup = cgroup.as_ref().map(|c| c.path.display().to_string()),
            "firecracker spawned"
        );

        // 5. Wait for the API socket.
        let start = Instant::now();
        loop {
            // The socket file exists as soon as Firecracker binds it, a moment
            // before it listens; a connect in that window is refused (seen on
            // KVM with two environments booting at once). Wait for a connect.
            if paths.api_sock.exists()
                && tokio::net::UnixStream::connect(&paths.api_sock)
                    .await
                    .is_ok()
            {
                break;
            }
            if let Some(status) = vmm.exited() {
                return Err(Self::boot_error_after_exit(
                    paths,
                    &console,
                    format!("firecracker exited before creating the API socket ({status})"),
                )
                .await);
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
        let ip_arg = policy.as_ref().map(|p| p.lease().kernel_net_args());
        let boot_args = compose_boot_args(
            spec.architecture,
            env_id,
            self.cfg.vsock_port,
            ip_arg.as_deref(),
            self.cfg.boot_args_extra.as_deref(),
        );
        let mut calls: Vec<(String, serde_json::Value)> = [
            (
                "/machine-config",
                serde_json::json!({"vcpu_count": vcpus, "mem_size_mib": mem_mib, "smt": false}),
            ),
            (
                "/boot-source",
                serde_json::json!({
                    "kernel_image_path": paths.vmm_path(&self.cfg.kernel, crate::jail::KERNEL),
                    "boot_args": boot_args
                }),
            ),
            (
                "/drives/rootfs",
                serde_json::json!({
                    "drive_id": "rootfs",
                    "path_on_host": paths.vmm_path(&self.cfg.rootfs, crate::jail::ROOTFS),
                    "is_root_device": true,
                    "is_read_only": true
                }),
            ),
            (
                "/drives/function",
                serde_json::json!({
                    "drive_id": "function",
                    "path_on_host": paths.vmm_path(&paths.function_drive, crate::jail::FUNCTION_DRIVE),
                    "is_root_device": false,
                    "is_read_only": true
                }),
            ),
            (
                "/drives/scratch",
                serde_json::json!({
                    "drive_id": "scratch",
                    "path_on_host": paths.vmm_path(&paths.scratch_drive, crate::jail::SCRATCH_DRIVE),
                    "is_root_device": false,
                    "is_read_only": false
                }),
            ),
            (
                "/vsock",
                serde_json::json!({
                    "guest_cid": GUEST_CID,
                    "uds_path": paths.vmm_path(&paths.vsock_uds, crate::jail::VSOCK_UDS)
                }),
            ),
        ]
        .into_iter()
        .map(|(path, body)| (path.to_owned(), body))
        .collect();
        if let (Some(policy), Some(nic)) = (&policy, &expected_nic) {
            calls.push((nic.api_path(), policy.lease().firecracker_body()));
        }
        // Egress gate, part 1: the plan configures no network path, or
        // exactly the one interface whose policy was verified.
        check_planned_calls(
            calls.iter().map(|(path, _)| path.as_str()),
            expected_nic.as_ref(),
        )
        .map_err(|e| Self::boot_error(paths, e))?;
        for (path, body) in &calls {
            if let Err(e) = api.put(path, body).await {
                return Err(match vmm.exited() {
                    Some(status) => {
                        Self::boot_error_after_exit(
                            paths,
                            &console,
                            format!(
                                "firecracker exited before configuration completed ({status}); last API error: {e}"
                            ),
                        )
                        .await
                    }
                    None => Self::boot_error(paths, format!("firecracker API: {e}")),
                });
            }
        }
        // Egress gate, part 2 (PLT-4622): ask the VMM what it is about to boot
        // and refuse unless it has no network interface. Nothing in the guest
        // - and so no user code - runs before this passes.
        let vm_config = api.get_json("/vm/config").await.map_err(|e| {
            Self::boot_error(
                paths,
                format!("egress gate: cannot read the VM configuration before InstanceStart: {e}"),
            )
        })?;
        check_vm_config(&vm_config, expected_nic.as_ref())
            .map_err(|e| Self::boot_error(paths, e))?;
        // Egress gate, part 3: the policy is still in force at the moment the
        // guest is started (nobody flushed the chain since it was installed).
        if let Some(policy) = &policy {
            self.net.verify(policy).await.map_err(|e| {
                Self::boot_error(
                    paths,
                    format!("egress gate: the policy no longer verifies before InstanceStart: {e}"),
                )
            })?;
        }
        if let Err(e) = api
            .put(
                "/actions",
                &serde_json::json!({"action_type": "InstanceStart"}),
            )
            .await
        {
            return Err(Self::boot_error(paths, format!("firecracker API: {e}")));
        }
        tracing::info!(
            env_id,
            vcpus,
            mem_mib,
            scratch_bytes,
            "InstanceStart accepted"
        );

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
            status = vmm.wait_exit() => {
                return Err(Self::boot_error_after_exit(
                    paths,
                    &console,
                    format!("firecracker exited before the guest bridge connected ({status})"),
                ).await);
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
        details.insert("scratch_drive_bytes".into(), scratch_bytes.into());
        details.insert("scratch_drive_reserved".into(), scratch_reserved.into());
        details.insert("scratch_drive_ms".into(), scratch_ms.into());
        details.insert(
            "console_log_max_bytes".into(),
            self.cfg.console_log_max_bytes.into(),
        );
        details.insert("fc_log_max_bytes".into(), self.cfg.fc_log_max_bytes.into());
        details.insert("host_disk_budget_bytes".into(), budget.total().into());
        details.insert("egress_profile".into(), spec.egress.as_str().into());
        match &policy {
            None => {
                details.insert("network_interfaces".into(), 0.into());
            }
            Some(policy) => {
                let lease = policy.lease();
                details.insert("network_interfaces".into(), 1.into());
                details.insert("egress_tap".into(), lease.tap.clone().into());
                details.insert("egress_chain".into(), lease.chain().into());
                details.insert("guest_ip".into(), lease.guest_ip.to_string().into());
                details.insert("host_tap_ip".into(), lease.host_ip.to_string().into());
                details.insert(
                    "dns_resolver".into(),
                    lease
                        .dns_resolver
                        .map(|d| d.to_string().into())
                        .unwrap_or(serde_json::Value::Null),
                );
                details.insert("egress_policy_rules".into(), policy.rule_count().into());
                details.insert(
                    "egress_policy_verified_ms".into(),
                    policy.verified_ms().into(),
                );
            }
        }
        details.insert("cgroup_mode".into(), self.cfg.cgroup.mode.as_str().into());
        match &cgroup {
            Some(cg) => {
                details.insert("cgroup".into(), cg.path.display().to_string().into());
                details.insert("cgroup_cpu_max".into(), cg.limits.cpu_max().into());
                details.insert(
                    "cgroup_memory_max_bytes".into(),
                    cg.limits.memory_max_bytes.into(),
                );
                details.insert("cgroup_pids_max".into(), cg.limits.pids_max.into());
            }
            None => {
                details.insert("cgroup".into(), serde_json::Value::Null);
            }
        }
        details.insert("jailed".into(), self.cfg.jailer.is_some().into());
        if let (Some(jailer), Some(jail)) = (&self.cfg.jailer, &paths.jail) {
            details.insert("jail_root".into(), jail.root.display().to_string().into());
            details.insert("vmm_uid".into(), jailer.uid.into());
            details.insert("vmm_gid".into(), jailer.gid.into());
            details.insert("vmm_new_pid_ns".into(), jailer.new_pid_ns.into());
        }
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
            &paths.scratch_drive,
            &paths.pid_file,
            &paths.stage_app,
            &paths.dir.join(crate::network::LEASE_FILE),
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

    /// Whether the Firecracker process of this environment is still running.
    ///
    /// A paused microVM is a *running* VMM process with its vCPUs stopped, so
    /// this is exactly the question "is there anything left to pause or
    /// resume". Asked before every `PATCH /vm` so a dead VMM is reported as
    /// such instead of as an API error at a socket nobody is listening on.
    async fn vmm_alive(&self, environment_id: &EnvironmentId, paths: &EnvPaths) -> bool {
        if let Some(t) = self.running.lock().await.get_mut(environment_id) {
            return t.exited().is_none();
        }
        // Not spawned by this process (a previous gateway run): the pid file
        // is all we have.
        read_pid_file(&paths.pid_file).is_some_and(pid_alive)
    }

    /// `PATCH /vm {"state": <state>}` on one environment's API socket.
    ///
    /// The three ways this can fail are answered separately, because the
    /// caller (the environment pool) has to tell "this environment is gone" —
    /// retire it and start cold — from "the request itself was refused".
    ///
    /// 1. the environment is not one of ours any more -> `NotFound`;
    /// 2. the VMM process is dead -> `Internal`, naming the pid file;
    /// 3. the API socket is gone -> `Internal`, naming the socket.
    ///
    /// A VM that is *already* in the requested state is success: quiesce and
    /// resume are idempotent by contract, and that is the state the caller
    /// asked for.
    async fn set_vm_state(
        &self,
        environment_id: &EnvironmentId,
        state: &'static str,
    ) -> Result<(), ProviderError> {
        let paths = self.paths_for(environment_id);
        if !paths.dir.exists() && !self.running.lock().await.contains_key(environment_id) {
            return Err(ProviderError::NotFound(environment_id.clone()));
        }
        if !self.vmm_alive(environment_id, &paths).await {
            return Err(ProviderError::Internal(format!(
                "cannot set the state of {environment_id} to {state}: \
                 the firecracker process is gone (pid file {})",
                paths.pid_file.display()
            )));
        }
        if !paths.api_sock.exists() {
            return Err(ProviderError::Internal(format!(
                "cannot set the state of {environment_id} to {state}: \
                 the firecracker API socket {} is gone",
                paths.api_sock.display()
            )));
        }
        let api = ApiClient::new(&paths.api_sock, API_TIMEOUT);
        match api
            .patch("/vm", &serde_json::json!({ "state": state }))
            .await
        {
            Ok(()) => Ok(()),
            Err(crate::api::ApiError::Status { status, body, .. })
                if is_already_in_state(state, &body) =>
            {
                tracing::debug!(
                    env_id = %environment_id,
                    state,
                    status,
                    "the microVM was already in the requested state"
                );
                Ok(())
            }
            Err(e) => Err(ProviderError::Internal(format!(
                "PATCH /vm {{\"state\":\"{state}\"}} for {environment_id}: {e}"
            ))),
        }
    }

    /// Kill a tracked child, waiting for a self-initiated poweroff first when
    /// `graceful`. Returns whether it was still running when we started.
    async fn stop_tracked(&self, t: &mut Tracked, graceful: bool) -> bool {
        let was_running = t.exited().is_none();
        if was_running && graceful {
            let deadline = Instant::now() + self.cfg.kill_grace;
            while Instant::now() < deadline {
                if t.exited().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        t.kill().await;
        was_running
    }

    /// Remove the tap and nftables state of an environment (PLT-4622). Called
    /// after the VMM is dead and before the directory (and its lease) is
    /// removed. A failure is logged and reported; startup reconcile sweeps
    /// whatever is left.
    async fn teardown_network(&self, environment_id: &EnvironmentId, cleaned: &mut Vec<String>) {
        match self
            .net
            .teardown(&self.cfg.workdir, environment_id.as_str())
            .await
        {
            Ok(report) => {
                if let Some(counters) = &report.counters {
                    tracing::info!(
                        env_id = %environment_id,
                        counters = %counters,
                        "egress policy counters at teardown"
                    );
                }
                cleaned.extend(report.cleaned);
            }
            Err(e) => {
                tracing::error!(env_id = %environment_id, error = %e, "egress network teardown failed");
                cleaned.push(format!("network-teardown-failed:{e}"));
            }
        }
    }
}

/// `egress_restricted` / `egress_public_web` on a host that can enforce them.
///
/// PLT-4622, measured on real KVM (docs/evidence/isolation-20260917T031126Z,
/// scripts/kvm/measure-isolation.sh NET): with a per-environment tap and the
/// provider's nftables table, public-web reached 1.1.1.1 / 1.0.0.1 and DNS
/// through the configured resolver only, while metadata, the management
/// network, the node, private / CGNAT ranges, IPv6, other resolvers, a DNS
/// name and an HTTP redirect pointing at 169.254.169.254 all failed (16/16);
/// restricted reached only its allowlisted 1.1.1.1:443 (11/11 denied); two
/// tenants booted at once could not reach each other (0 accepted); the policy
/// was read back before InstanceStart on all 4 policed boots; no tap, table
/// or lease survived. aarch64 under nested virtualization only. Hosts without
/// CAP_NET_ADMIN / nftables / ip_forward get `Unsupported` with the reason
/// (`ExecutionProvider::capabilities`).
const EGRESS_NETWORK_SUPPORT: Support = Support::Supported;

/// `enforce_resource_limits` for this host.
///
/// The table value is what was measured with host cgroups in force (see
/// [`FirecrackerProvider::capability_table`]). Without them, `cpu_millis` is
/// enforced only in whole vCPUs and the VMM's host memory is unbounded, so
/// the claim is withdrawn: `Unsupported` when the configuration requires the
/// cgroups (every environment is refused), `Unverified` otherwise.
fn resource_limits_support(
    mode: CgroupMode,
    host: &Result<String, String>,
    measured: Support,
) -> Support {
    match (mode, host) {
        (CgroupMode::Required | CgroupMode::BestEffort, Ok(_)) => measured,
        (CgroupMode::Required, Err(reason)) => Support::unsupported(format!(
            "host cgroup v2 limits are required but unavailable, so no environment can be created: {reason}"
        )),
        (_, Err(reason)) => Support::unverified(format!(
            "host cgroup v2 limits are not applied ({reason}): cpu_millis is enforced in whole \
             vCPUs only and the VMM's host memory is not bounded"
        )),
        (CgroupMode::Off, Ok(_)) => Support::unverified(
            "host cgroup v2 limits are off: cpu_millis is enforced in whole vCPUs only and the \
             VMM's host memory is not bounded",
        ),
    }
}

/// Whether a `PATCH /vm` fault means "the microVM is already in **the state
/// that was requested**".
///
/// Matched on the fault message rather than the status code because
/// Firecracker answers `400 Bad Request` for every refused state change, so
/// the code alone cannot tell "already paused" from "cannot pause". Quiesce
/// and resume are idempotent by contract
/// ([`ExecutionProvider::idle_quiesce`]), and a retry that finds the VM
/// already in the requested state has got what it asked for.
///
/// The direction is part of the question (PLT-4633 review F1). A refusal of
/// `{"state":"Resumed"}` that talks about a *paused* VM is a refusal, not a
/// success: reporting it as success would hand the pool an environment whose
/// vCPUs are stopped, and the next invocation would hang in it until the
/// execution deadline. A message this cannot place is therefore a failure —
/// a false failure costs one cold start, a false success costs a hung
/// invocation.
fn is_already_in_state(requested: &str, fault: &str) -> bool {
    let fault = fault.to_ascii_lowercase();
    if !fault.contains("already") {
        return false;
    }
    // "paused"/"pause", and "resumed"/"resume"/"running" (Firecracker calls
    // the resumed state `Running` in some messages).
    let says_paused = fault.contains("paus");
    let says_running = fault.contains("resum") || fault.contains("running");
    // A message that names both states, or neither, cannot be placed.
    if says_paused == says_running {
        return false;
    }
    match requested {
        VM_STATE_PAUSED => says_paused,
        VM_STATE_RESUMED => says_running,
        _ => false,
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
        let mut caps = Self::capability_table();
        if let Err(reason) = &self.net_support {
            let note = format!("unavailable on this host: {reason}");
            caps.egress_restricted = Support::unsupported(note.clone());
            caps.egress_public_web = Support::unsupported(note);
        }
        caps.enforce_resource_limits = resource_limits_support(
            self.cfg.cgroup.mode,
            &self.cgroup_support,
            caps.enforce_resource_limits,
        );
        caps
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
        if spec.resources.ephemeral_storage_mib == 0 {
            return Err(ProviderError::InvalidSpec(
                "resources.ephemeral_storage_mib must be at least 1 (the scratch drive is sized from it)"
                    .into(),
            ));
        }
        if spec.egress != EgressProfile::None {
            // Checked again now, not only at construction: ip_forward or the
            // binaries may have changed. Fail closed before anything exists.
            if let Err(reason) = host_support(&self.cfg.network) {
                return Err(ProviderError::Unavailable(format!(
                    "egress {} cannot be enforced on this host: {reason}",
                    spec.egress.as_str()
                )));
            }
            if spec.egress == EgressProfile::Restricted && spec.egress_allow.is_empty() {
                return Err(ProviderError::InvalidSpec(
                    "egress restricted without allow rules".into(),
                ));
            }
            for rule in &spec.egress_allow {
                rule.validate()
                    .map_err(|e| ProviderError::InvalidSpec(format!("egress_allow: {e}")))?;
            }
        } else if !spec.egress_allow.is_empty() {
            return Err(ProviderError::InvalidSpec(
                "egress none carries no allow rules".into(),
            ));
        }
        // Host isolation (PLT-4622): fail closed before anything exists.
        if self.cfg.cgroup.mode == CgroupMode::Required
            && let Err(reason) = crate::cgroup::host_support(&self.cfg.cgroup)
        {
            return Err(ProviderError::Unavailable(format!(
                "host cgroup limits are required (cgroup mode \"required\") but unavailable: {reason}"
            )));
        }
        if let Some(jailer) = &self.cfg.jailer {
            if let Some(Err(reason)) = &self.jail_exec {
                return Err(ProviderError::Unavailable(format!("jailer: {reason}")));
            }
            if let Err(reason) = crate::jail::host_support(&self.cfg, jailer) {
                return Err(ProviderError::Unavailable(format!("jailer: {reason}")));
            }
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

        let mut slot: Option<Tracked> = None;
        match self.boot(&spec, &paths, created_at, &mut slot).await {
            Ok(handle) => {
                let tracked = slot.take().expect("boot succeeded with a VMM");
                self.running
                    .lock()
                    .await
                    .insert(spec.environment_id.clone(), tracked);
                Ok(handle)
            }
            Err(err) => {
                tracing::warn!(env_id = %spec.environment_id, error = %err, "boot failed; cleaning up");
                if let Some(mut t) = slot.take() {
                    t.kill().await;
                }
                let mut cleaned = Vec::new();
                self.release_host(&spec.environment_id, &paths, &mut cleaned)
                    .await;
                if spec.egress != EgressProfile::None {
                    self.teardown_network(&spec.environment_id, &mut cleaned)
                        .await;
                }
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
            // A tap, cgroup or jail may outlive a directory removed by hand;
            // never leave them.
            let mut cleaned = Vec::new();
            if self.host_state_exists(environment_id, &paths) {
                self.release_host(environment_id, &paths, &mut cleaned)
                    .await;
            }
            if std::path::Path::new("/sys/class/net")
                .join(crate::network::tap_name(environment_id.as_str()))
                .exists()
            {
                self.teardown_network(environment_id, &mut cleaned).await;
            }
            return Ok(TerminateReport {
                was_running: false,
                cleaned,
            });
        }
        // Only a guest that can still act on the `Shutdown` frame is waited
        // for. A quiesced microVM cannot: its vCPUs are stopped, so waiting
        // would spend the whole grace period for nothing (PLT-4633 review F3).
        let graceful = reason.waits_for_the_guest();
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
            match Self::pid_is_ours(pid, environment_id, &paths) {
                Some(true) => {
                    was_running = true;
                    if graceful {
                        wait_pid_gone(pid, self.cfg.kill_grace).await;
                    }
                    kill_vmm(pid);
                    wait_pid_gone(pid, Duration::from_secs(1)).await;
                    cleaned.push(format!("process-group:{pid}"));
                }
                Some(false) => {
                    tracing::warn!(pid, "pid file points at an unrelated process; not killed")
                }
                None => tracing::warn!(pid, "cannot verify pid ownership on this host; not killed"),
            }
        }
        // The cgroup is killed as a whole: nothing the VMM (or a jailer) left
        // behind survives, whether or not the pid could be attributed.
        self.release_host(environment_id, &paths, &mut cleaned)
            .await;

        if paths.dir.join(crate::network::LEASE_FILE).exists()
            || std::path::Path::new("/sys/class/net")
                .join(crate::network::tap_name(environment_id.as_str()))
                .exists()
        {
            self.teardown_network(environment_id, &mut cleaned).await;
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

    /// Pause the microVM: `PATCH /vm {"state": "Paused"}`.
    ///
    /// The vsock device and the open bridge connection survive a pause — the
    /// guest simply stops being scheduled — so the pooled session stays valid
    /// and no handshake is repeated when it is resumed.
    async fn idle_quiesce(&self, environment_id: &EnvironmentId) -> Result<(), ProviderError> {
        let started = Instant::now();
        self.set_vm_state(environment_id, VM_STATE_PAUSED).await?;
        tracing::debug!(
            env_id = %environment_id,
            ms = started.elapsed().as_millis() as u64,
            "microVM paused"
        );
        Ok(())
    }

    /// Resume the microVM: `PATCH /vm {"state": "Resumed"}`.
    async fn idle_resume(&self, environment_id: &EnvironmentId) -> Result<(), ProviderError> {
        let started = Instant::now();
        self.set_vm_state(environment_id, VM_STATE_RESUMED).await?;
        tracing::debug!(
            env_id = %environment_id,
            ms = started.elapsed().as_millis() as u64,
            "microVM resumed"
        );
        Ok(())
    }

    /// The VMM cgroup's `cpu.stat` / `memory.current` / `memory.peak`
    /// (guest + VMM). `None` without host cgroups or for an environment this
    /// provider does not run (PLT-4637; read-only).
    async fn environment_stats(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<Option<EnvironmentStats>, ProviderError> {
        let Some(cgroups) = &self.cgroups else {
            return Ok(None);
        };
        if !self.running.lock().await.contains_key(environment_id) {
            return Ok(None);
        }
        let path = cgroups.env_dir(environment_id.as_str());
        Ok(
            crate::cgroup::usage(&path).map(|(cpu, current, peak)| EnvironmentStats {
                cpu_seconds: cpu,
                memory_current_bytes: current,
                memory_peak_bytes: peak,
                scope: "cgroup_v2".into(),
            }),
        )
    }

    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        if let Some(t) = self.running.lock().await.get_mut(environment_id) {
            let pid = t.pid;
            return match &mut t.child {
                Some(child) => match child.try_wait() {
                    Ok(None) => Ok(EnvironmentObservation::Running {
                        host_pid: Some(pid),
                    }),
                    Ok(Some(status)) => {
                        use std::os::unix::process::ExitStatusExt;
                        Ok(EnvironmentObservation::Exited {
                            exit_code: status.code(),
                            signal: status.signal(),
                        })
                    }
                    Err(e) => Err(ProviderError::Internal(format!("try_wait: {e}"))),
                },
                // Not our child (jailer in a new PID namespace): no status.
                None => Ok(match t.exited() {
                    None => EnvironmentObservation::Running {
                        host_pid: Some(pid),
                    },
                    Some(_) => EnvironmentObservation::Exited {
                        exit_code: None,
                        signal: None,
                    },
                }),
            };
        }
        let paths = self.paths_for(environment_id);
        if !paths.dir.exists() {
            return Ok(EnvironmentObservation::NotFound);
        }
        match read_pid_file(&paths.pid_file) {
            Some(pid)
                if pid_alive(pid)
                    && Self::pid_is_ours(pid, environment_id, &paths) != Some(false) =>
            {
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
        // Startup reconcile lists environments first: sweep network state
        // (taps, chains, map entries) that belongs to no environment directory.
        let live: Vec<String> = ids.iter().map(|id| id.as_str().to_owned()).collect();
        match self.net.sweep(&self.cfg.workdir, &live).await {
            Ok(removed) if !removed.is_empty() => {
                tracing::warn!(?removed, "removed orphaned egress network state");
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "egress network sweep failed"),
        }
        // ... and cgroups / jails of environments that no longer exist.
        let mut removed = Vec::new();
        if let Some(cgroups) = &self.cgroups {
            removed.extend(cgroups.sweep(&live).await);
        }
        if let (Some(jailer), Some(Ok(exec))) = (&self.cfg.jailer, &self.jail_exec) {
            let live_instances: Vec<String> = live.iter().map(|id| instance_id_for(id)).collect();
            removed.extend(crate::jail::sweep(jailer, exec, &live_instances));
        }
        if !removed.is_empty() {
            tracing::warn!(?removed, "removed orphaned cgroups / jails");
        }
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
        // PLT-4622: vCPU / memory / ephemeral storage measured on real KVM
        // (docs/evidence/isolation-20260917T011555Z).
        assert!(c.enforce_resource_limits.is_supported());
        // PLT-4622: measured on real KVM (see capability_table()).
        assert_eq!(c.egress_restricted, EGRESS_NETWORK_SUPPORT);
        assert_eq!(c.egress_public_web, EGRESS_NETWORK_SUPPORT);
        assert!(matches!(c.host_metering, Support::Unverified { .. }));
        for s in [&c.snapshot_create, &c.snapshot_clone] {
            assert!(matches!(s, Support::Unsupported { .. }));
        }
        // PLT-4633: pause/resume was measured on real KVM
        // (docs/evidence/warm-20260916T162532Z), so both idle capabilities are
        // `Supported` and an operator who sets `[pool] enabled` gets warm reuse
        // without the measurement switch. `[pool]` still defaults to off, so
        // the default configuration is unchanged (docs/architecture.md §4).
        for s in [&c.idle_quiesce, &c.idle_resume] {
            assert!(s.is_supported(), "{s:?}");
        }
    }

    /// PLT-4633 (review F1): "already in that state" is success only for the
    /// state that was **requested**, and only in that direction.
    ///
    /// A refusal of `{"state":"Resumed"}` whose message talks about a paused
    /// microVM is a refusal. Reporting it as success would tell the pool that
    /// the environment resumed, and the next invocation would be dispatched
    /// into a VM whose vCPUs are stopped and hang there until the execution
    /// deadline. A message that cannot be placed is a failure too: a false
    /// failure costs one cold start, a false success costs a hung invocation.
    #[test]
    fn a_refusal_is_success_only_for_the_state_that_was_requested() {
        // Quiesce: only a message that says it is already paused.
        for fault in [
            r#"{"fault_message":"The microVM is already paused."}"#,
            r#"{"fault_message":"Vm is already Paused"}"#,
        ] {
            assert!(is_already_in_state(VM_STATE_PAUSED, fault), "{fault}");
            assert!(
                !is_already_in_state(VM_STATE_RESUMED, fault),
                "a resume was not granted by a message about a paused VM: {fault}"
            );
        }

        // Resume: Firecracker calls the running state both `Resumed` and
        // `Running` depending on the message.
        for fault in [
            r#"{"fault_message":"Vm is already Resumed"}"#,
            r#"{"fault_message":"the vm is already running"}"#,
        ] {
            assert!(is_already_in_state(VM_STATE_RESUMED, fault), "{fault}");
            assert!(
                !is_already_in_state(VM_STATE_PAUSED, fault),
                "a pause was not granted by a message about a running VM: {fault}"
            );
        }

        // Neither direction accepts these.
        for fault in [
            // The defect this test exists for: a refused resume whose text
            // mentions the other state.
            r#"{"fault_message":"cannot resume: the microVM is already paused"}"#,
            // Not an "already" message at all.
            r#"{"fault_message":"The requested operation is not supported: Paused"}"#,
            r#"{"fault_message":"Internal error"}"#,
            // "already", but no state that can be placed, or both at once.
            r#"{"fault_message":"the vm is already in the requested state"}"#,
            r#"{"fault_message":"already: paused, not running"}"#,
            "",
        ] {
            assert!(!is_already_in_state(VM_STATE_PAUSED, fault), "{fault}");
            assert!(!is_already_in_state(VM_STATE_RESUMED, fault), "{fault}");
        }
    }

    /// PLT-4633 (review F3): the grace period is for a guest that can still
    /// use it. A quiesced microVM cannot — its vCPUs are stopped — so this
    /// provider must not wait for one.
    #[test]
    fn a_quiesced_environment_is_not_waited_for() {
        assert!(!TerminateReason::Quiesced.waits_for_the_guest());
        for reason in [TerminateReason::Completed, TerminateReason::Shutdown] {
            assert!(reason.waits_for_the_guest(), "{reason:?}");
        }
        for reason in [
            TerminateReason::Timeout,
            TerminateReason::Cancelled,
            TerminateReason::InitFailed,
            TerminateReason::Crashed,
            TerminateReason::Reconcile,
        ] {
            assert!(!reason.waits_for_the_guest(), "{reason:?}");
        }
    }

    /// Nothing can be paused or resumed once the environment is gone, and the
    /// error says which environment it was.
    #[tokio::test]
    async fn idle_quiesce_and_resume_report_a_missing_environment() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        assert!(matches!(
            p.idle_quiesce(&id).await,
            Err(ProviderError::NotFound(missing)) if missing == id
        ));
        assert!(matches!(
            p.idle_resume(&id).await,
            Err(ProviderError::NotFound(missing)) if missing == id
        ));
    }

    /// A directory without a live VMM behind it is not something to pause: the
    /// error names the dead process rather than timing out on a socket nobody
    /// listens on.
    #[tokio::test]
    async fn idle_quiesce_reports_a_dead_vmm_process() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let id = EnvironmentId::generate();
        let paths = p.paths_for(&id);
        std::fs::create_dir_all(&paths.dir).unwrap();
        write_pid_file(&paths.pid_file, u32::MAX / 2).unwrap();
        let err = p.idle_quiesce(&id).await.unwrap_err();
        assert!(
            matches!(&err, ProviderError::Internal(m) if m.contains("firecracker process is gone")),
            "{err}"
        );
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
            egress_allow: Vec::new(),
            connect_timeout: Duration::from_secs(1),
        };
        assert!(matches!(
            p.create_environment(spec).await,
            Err(ProviderError::InvalidSpec(m)) if m.contains("already exists")
        ));
    }

    /// A host that cannot enforce the network profiles says so in its
    /// capabilities, with the reason, and leaves `none` alone.
    #[test]
    fn network_capabilities_follow_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let c = p.capabilities();
        assert!(c.egress_none.is_supported());
        match &p.net_support {
            Ok(_) => {
                assert!(c.egress_restricted.is_supported());
                assert!(c.egress_public_web.is_supported());
            }
            Err(reason) => {
                for s in [&c.egress_restricted, &c.egress_public_web] {
                    assert!(
                        matches!(s, Support::Unsupported { reason: r } if r.contains(reason.as_str())),
                        "{s:?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn create_rejects_non_none_egress_where_it_cannot_be_enforced() {
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
            egress_allow: Vec::new(),
            connect_timeout: Duration::from_secs(1),
        };
        if p.net_support.is_ok() {
            // A privileged Linux host can enforce it; covered on real KVM.
            return;
        }
        let mut none_with_rules = spec.clone();
        none_with_rules.egress = EgressProfile::None;
        none_with_rules.egress_allow = vec![tachyon_serverless_domain::EgressAllowRule {
            cidr: "1.1.1.1/32".into(),
            protocol: Default::default(),
            ports: vec![443],
        }];
        assert!(matches!(
            p.create_environment(spec).await,
            Err(ProviderError::Unavailable(m)) if m.contains("egress public-web cannot be enforced")
        ));
        assert!(matches!(
            p.create_environment(none_with_rules).await,
            Err(ProviderError::InvalidSpec(m)) if m.contains("allow rules")
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
            egress_allow: Vec::new(),
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

    /// PLT-4622: the measured resource-limit claim holds only while host
    /// cgroups are in force.
    #[test]
    fn resource_limits_claim_follows_the_host_cgroups() {
        let ok: Result<String, String> = Ok("cgroup v2".into());
        let missing: Result<String, String> = Err("not writable".into());
        for mode in [CgroupMode::Required, CgroupMode::BestEffort] {
            assert!(resource_limits_support(mode, &ok, Support::Supported).is_supported());
        }
        assert!(matches!(
            resource_limits_support(CgroupMode::Required, &missing, Support::Supported),
            Support::Unsupported { reason } if reason.contains("not writable")
        ));
        for (mode, host) in [
            (CgroupMode::BestEffort, &missing),
            (CgroupMode::Off, &missing),
            (CgroupMode::Off, &ok),
        ] {
            assert!(matches!(
                resource_limits_support(mode, host, Support::Supported),
                Support::Unverified { note } if note.contains("whole vCPUs")
            ));
        }
    }

    /// With the jailer, sockets move into the chroot and the jail directory
    /// is named after the instance id.
    #[test]
    fn jailed_paths_live_in_the_chroot() {
        let dir = tempfile::tempdir().unwrap();
        let p = FirecrackerProvider::new(FirecrackerConfig {
            firecracker_binary: dir.path().join("firecracker"),
            kernel: dir.path().join("vmlinux"),
            rootfs: dir.path().join("rootfs.ext4"),
            workdir: dir.path().join("run"),
            jailer: Some(crate::config::JailerConfig {
                chroot_base: dir.path().join("jail"),
                ..Default::default()
            }),
            ..Default::default()
        });
        let id = EnvironmentId::generate();
        let paths = p.paths_for(&id);
        let jail = paths.jail.as_ref().expect("jailed");
        assert_eq!(
            jail.jail_dir,
            dir.path()
                .join("jail/firecracker")
                .join(instance_id_for(id.as_str()))
        );
        assert!(paths.api_sock.starts_with(&jail.root));
        assert!(paths.vsock_listener.starts_with(&jail.root));
        assert!(paths.function_drive.starts_with(dir.path().join("run")));
    }

    /// A configuration that requires host cgroups refuses to create anything
    /// on a host that cannot provide them (macOS, unprivileged Linux).
    #[tokio::test]
    async fn required_cgroups_fail_closed_before_anything_exists() {
        let dir = tempfile::tempdir().unwrap();
        let p = FirecrackerProvider::new(FirecrackerConfig {
            firecracker_binary: dir.path().join("no-firecracker"),
            kernel: dir.path().join("vmlinux"),
            rootfs: dir.path().join("rootfs.ext4"),
            workdir: dir.path().join("run"),
            cgroup: crate::config::CgroupConfig {
                mode: CgroupMode::Required,
                root: dir.path().join("not-a-cgroup"),
                ..Default::default()
            },
            ..Default::default()
        });
        assert!(matches!(
            p.capabilities().enforce_resource_limits,
            Support::Unsupported { .. }
        ));
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
            egress: EgressProfile::None,
            egress_allow: Vec::new(),
            connect_timeout: Duration::from_secs(1),
        };
        let id = spec.environment_id.clone();
        assert!(matches!(
            p.create_environment(spec).await,
            Err(ProviderError::Unavailable(m)) if m.contains("cgroup")
        ));
        assert!(!p.paths_for(&id).dir.exists());
        let report = p.preflight().await.unwrap();
        let cg = report
            .checks
            .iter()
            .find(|c| c.name == crate::preflight::HOST_CGROUP_CHECK)
            .unwrap();
        assert!(!cg.ok && cg.detail.starts_with("required"), "{}", cg.detail);
        assert!(!report.ok);
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
