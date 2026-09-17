//! Snapshot and clone (X1, PLT-4653, feature `experimental-restore`;
//! docs/adr/0015 決定 2-4, docs/adr/0017).
//!
//! Only with the jailer: Firecracker records drive and vsock paths in the
//! snapshot and cannot override drives at load time, so the paths must be
//! chroot-relative (`/scratch.ext4`, `/v.sock`) to resolve to each clone's own
//! files.
//!
//! ```text
//! snapshot: source holds at the checkpoint (checked by the caller)
//!   PATCH /vm Paused -> PUT /snapshot/create Full (/snapshot.mem, /snapshot.vmstate in the chroot)
//!   -> copy scratch.ext4 and link function.ext4 while paused
//!   -> move memory + vmstate out of the chroot into <workdir>/_snapshots/<id>/ (root, 0640 root:<jail gid>)
//!   (the source stays paused; the caller terminates it)
//!
//! clone: new env dir, cgroup, jail (own chroot, own sockets)
//!   -> private scratch copy, shared read-only function drive, memory and vmstate hard-linked
//!   -> spawn jailed VMM -> PUT /snapshot/load (resume_vm = false)
//!   -> egress gate: GET /vm/config has no network interface and no MMDS
//!   -> PATCH /vm Resumed -> ring the guest doorbell -> accept the guest's reconnection
//! ```

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tachyon_serverless_domain::{
    DeviceModel, DriveSlot, EgressProfile, EnvironmentId, HostCpuIdentity, RuntimeProfile,
    Sha256Digest, SnapshotId,
};
use tachyon_serverless_provider_port::restore::files;
use tachyon_serverless_provider_port::{
    CloneSpec, CloneTimings, EnvironmentHandle, ProviderError, RestoreHostProfile, SnapshotCapture,
    SnapshotTimings, Support,
};
use tokio::net::UnixListener;

use super::{
    API_TIMEOUT, FirecrackerProvider, GUEST_CID, Tracked, VM_STATE_PAUSED, VM_STATE_RESUMED,
};
use crate::api::ApiClient;
use crate::cgroup::{CgroupLimits, EnvCgroup};
use crate::config::CgroupMode;
use crate::egress_gate::check_vm_config;
use crate::jail::JailInputs;
use crate::vmm::{EnvPaths, MAX_UNIX_SOCKET_PATH, create_fc_log};

/// Directory under `workdir` holding plaintext snapshot files.
pub const SNAPSHOT_DIR: &str = "_snapshots";
/// In-chroot names of the files a snapshot is created into / loaded from.
const CHROOT_MEM: &str = "snapshot.mem";
const CHROOT_VMSTATE: &str = "snapshot.vmstate";
/// `PUT /snapshot/create` writes guest memory; allow more than a config call.
const SNAPSHOT_API_TIMEOUT: Duration = Duration::from_secs(120);
/// How long the host keeps ringing the doorbell of a restored guest.
const DOORBELL_BUDGET: Duration = Duration::from_secs(3);

fn sha(hex: &str) -> Result<Sha256Digest, ProviderError> {
    Sha256Digest::parse(&format!("sha256:{hex}"))
        .map_err(|e| ProviderError::Internal(format!("digest: {e}")))
}

/// SHA-256 over the CPU identity lines of `/proc/cpuinfo` (model, vendor /
/// implementer, part, revision, flags / features), deduplicated.
fn cpu_model_hash() -> Result<Sha256Digest, ProviderError> {
    let text = std::fs::read_to_string("/proc/cpuinfo")
        .map_err(|e| ProviderError::Unavailable(format!("/proc/cpuinfo: {e}")))?;
    let keys = [
        "vendor_id",
        "cpu family",
        "model",
        "model name",
        "stepping",
        "microcode",
        "flags",
        "CPU implementer",
        "CPU architecture",
        "CPU variant",
        "CPU part",
        "CPU revision",
        "Features",
    ];
    let mut lines: Vec<String> = text
        .lines()
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim(), v.trim()))
        .filter(|(k, _)| keys.contains(k))
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    lines.sort();
    lines.dedup();
    Ok(Sha256Digest::of_bytes(lines.join("\n").as_bytes()))
}

/// SHA-256 over the KVM API version and the answers to
/// `KVM_CHECK_EXTENSION` for extensions 0..=255.
#[cfg(target_os = "linux")]
fn kvm_capabilities_hash() -> Result<Sha256Digest, ProviderError> {
    use std::os::fd::AsRawFd;
    const KVM_GET_API_VERSION: u64 = 0xAE00;
    const KVM_CHECK_EXTENSION: u64 = 0xAE03;
    let kvm = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .map_err(|e| ProviderError::Unavailable(format!("/dev/kvm: {e}")))?;
    let fd = kvm.as_raw_fd();
    // SAFETY: both ioctls take an integer argument and only read it.
    let api = unsafe { libc::ioctl(fd, KVM_GET_API_VERSION as _, 0) };
    let mut text = format!("api={api}");
    for ext in 0..=255u64 {
        // SAFETY: as above.
        let r = unsafe { libc::ioctl(fd, KVM_CHECK_EXTENSION as _, ext as libc::c_ulong) };
        text.push_str(&format!(";{ext}={r}"));
    }
    Ok(Sha256Digest::of_bytes(text.as_bytes()))
}

#[cfg(not(target_os = "linux"))]
fn kvm_capabilities_hash() -> Result<Sha256Digest, ProviderError> {
    Err(ProviderError::Unavailable("KVM needs Linux".into()))
}

/// `chown uid:gid` + `chmod mode` of a path.
fn own(path: &Path, uid: u32, gid: u32, mode: u32) -> Result<(), ProviderError> {
    use std::os::unix::fs::PermissionsExt;
    crate::jail::chown(path, uid, gid).map_err(ProviderError::Internal)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|e| ProviderError::Internal(format!("chmod {}: {e}", path.display())))
}

/// Copy a (sparse) image: `cp --sparse=always --reflink=auto`, or a plain
/// copy when `cp` refuses those flags.
async fn copy_image(src: &Path, dst: &Path) -> Result<(), ProviderError> {
    let cp = tokio::process::Command::new("cp")
        .arg("--sparse=always")
        .arg("--reflink=auto")
        .arg(src)
        .arg(dst)
        .stdin(std::process::Stdio::null())
        .output()
        .await;
    match cp {
        Ok(out) if out.status.success() => Ok(()),
        _ => tokio::fs::copy(src, dst).await.map(|_| ()).map_err(|e| {
            ProviderError::Internal(format!("copy {} -> {}: {e}", src.display(), dst.display()))
        }),
    }
}

/// Connect to the guest's doorbell port through the VMM's vsock UDS
/// (`CONNECT <port>\n` -> `OK <host port>\n`). Returns the number of attempts.
async fn ring_doorbell(uds: &Path, port: u32) -> Option<u32> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let deadline = Instant::now() + DOORBELL_BUDGET;
    let mut attempts = 0u32;
    while Instant::now() < deadline {
        attempts += 1;
        if let Ok(mut s) = tokio::net::UnixStream::connect(uds).await
            && s.write_all(format!("CONNECT {port}\n").as_bytes())
                .await
                .is_ok()
        {
            let mut line = String::new();
            let mut reader = BufReader::new(&mut s);
            let read =
                tokio::time::timeout(Duration::from_millis(500), reader.read_line(&mut line)).await;
            if matches!(read, Ok(Ok(n)) if n > 0) && line.starts_with("OK") {
                return Some(attempts);
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

impl FirecrackerProvider {
    /// `snapshot_create` / `snapshot_clone` on this host.
    pub(super) fn snapshot_support(&self) -> Support {
        match (&self.cfg.jailer, &self.jail_exec) {
            (Some(_), Some(Ok(_))) => Support::unverified(
                "X1 experimental (PLT-4653): Firecracker Full snapshot at the SDK checkpoint and \
                 clones in their own jail, cgroup and scratch copy; egress none only; measured only \
                 under nested virtualization on aarch64 (docs/evidence/x1-clone-*)",
            ),
            (Some(_), Some(Err(reason))) => Support::unsupported(format!("jailer: {reason}")),
            _ => Support::unsupported(
                "snapshots need [provider.firecracker.jailer]: drive and vsock paths recorded in \
                 a snapshot must be chroot-relative to resolve to each clone's own files",
            ),
        }
    }

    pub(super) fn snapshot_root(&self) -> PathBuf {
        self.cfg.workdir.join(SNAPSHOT_DIR)
    }

    pub(super) async fn restore_profile_impl(&self) -> Result<RestoreHostProfile, ProviderError> {
        let Some(jailer) = &self.cfg.jailer else {
            return Err(ProviderError::Unavailable(
                "snapshots need the jailer".into(),
            ));
        };
        let version = self.version.clone().ok_or_else(|| {
            ProviderError::Unavailable("firecracker version could not be probed".into())
        })?;
        let digest = |p: PathBuf| async move {
            self.digests
                .digest(&p)
                .await
                .map_err(|e| ProviderError::Unavailable(format!("digest {}: {e}", p.display())))
        };
        let vmm = digest(self.cfg.firecracker_binary.clone()).await?;
        let kernel = digest(self.cfg.kernel.clone()).await?;
        let rootfs = digest(self.cfg.rootfs.clone()).await?;
        let host_kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|s| s.trim().to_string())
            .map_err(|e| ProviderError::Unavailable(format!("host kernel release: {e}")))?;
        let (cpu, kvm) =
            tokio::task::spawn_blocking(|| (cpu_model_hash(), kvm_capabilities_hash()))
                .await
                .map_err(|e| ProviderError::Internal(format!("host identity task: {e}")))?;
        let drive = |id: &str, name: &str, ro: bool, root: bool| DriveSlot {
            drive_id: id.into(),
            path_in_vmm: format!("/{name}"),
            read_only: ro,
            root_device: root,
        };
        Ok(RestoreHostProfile {
            runtime: RuntimeProfile {
                provider_kind: "firecracker".into(),
                provider_version: version,
                vmm_sha256: sha(&vmm)?,
                kernel_sha256: sha(&kernel)?,
                rootfs_sha256: sha(&rootfs)?,
                bridge_protocol_version: tachyon_serverless_protocol::RESTORE_PROTOCOL_VERSION,
                jailer_mode: format!(
                    "jailer uid={} gid={} new_pid_ns={}",
                    jailer.uid, jailer.gid, jailer.new_pid_ns
                ),
                cgroup_mode: self.cfg.cgroup.mode.as_str().into(),
                host_kernel,
            },
            host_cpu: HostCpuIdentity {
                arch: std::env::consts::ARCH.into(),
                cpu_model_hash: cpu?,
                kvm_capabilities_hash: kvm?,
            },
            devices: DeviceModel {
                drives: vec![
                    drive("rootfs", crate::jail::ROOTFS, true, true),
                    drive("function", crate::jail::FUNCTION_DRIVE, true, false),
                    drive("scratch", crate::jail::SCRATCH_DRIVE, false, false),
                ],
                vsock_guest_cid: GUEST_CID,
                vsock_port: self.cfg.vsock_port,
                doorbell_port: tachyon_serverless_protocol::DOORBELL_VSOCK_PORT,
                network_interfaces: 0,
            },
        })
    }

    pub(super) async fn snapshot_impl(
        &self,
        environment_id: &EnvironmentId,
        snapshot_id: &SnapshotId,
    ) -> Result<SnapshotCapture, ProviderError> {
        let paths = self.paths_for(environment_id);
        let (Some(jailer), Some(jail)) = (&self.cfg.jailer, &paths.jail) else {
            return Err(ProviderError::Unavailable(
                "snapshots need the jailer".into(),
            ));
        };
        if !self.vmm_alive(environment_id, &paths).await {
            return Err(ProviderError::NotFound(environment_id.clone()));
        }
        let dest = self.snapshot_root().join(snapshot_id.as_str());
        tokio::fs::create_dir_all(&dest).await?;
        {
            use std::os::unix::fs::PermissionsExt;
            let root = self.snapshot_root();
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
            std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o700))?;
        }
        // The unprivileged VMM cannot create files in its chroot root.
        for name in [CHROOT_MEM, CHROOT_VMSTATE] {
            let p = jail.host(name);
            std::fs::File::create(&p)?;
            own(&p, jailer.uid, jailer.gid, 0o600)?;
        }
        let api = ApiClient::new(&paths.api_sock, SNAPSHOT_API_TIMEOUT);
        let t0 = Instant::now();
        self.set_vm_state(environment_id, VM_STATE_PAUSED).await?;
        let t1 = Instant::now();
        api.put(
            "/snapshot/create",
            &serde_json::json!({
                "snapshot_type": "Full",
                "snapshot_path": format!("/{CHROOT_VMSTATE}"),
                "mem_file_path": format!("/{CHROOT_MEM}"),
            }),
        )
        .await
        .map_err(|e| ProviderError::Internal(format!("PUT /snapshot/create: {e}")))?;
        let t2 = Instant::now();
        // Disk captured at the same pause point: the guest cannot write now.
        copy_image(
            &jail.host(crate::jail::SCRATCH_DRIVE),
            &dest.join(files::SCRATCH),
        )
        .await?;
        let func = dest.join(files::FUNCTION_DRIVE);
        let _ = std::fs::remove_file(&func);
        std::fs::hard_link(&paths.function_drive, &func).map_err(|e| {
            ProviderError::Internal(format!("link the function drive into the snapshot: {e}"))
        })?;
        let t3 = Instant::now();
        for (from, to) in [
            (CHROOT_MEM, files::MEMORY),
            (CHROOT_VMSTATE, files::VMSTATE),
        ] {
            let target = dest.join(to);
            std::fs::rename(jail.host(from), &target).map_err(|e| {
                ProviderError::Internal(format!("move {from} out of the chroot: {e}"))
            })?;
            // Readable by the jailed uid only through a hard link in a
            // clone's chroot (the directory is root-only); never writable.
            own(&target, 0, jailer.gid, 0o640)?;
        }
        own(&dest.join(files::SCRATCH), 0, 0, 0o600)?;
        let ms = |a: Instant, b: Instant| b.saturating_duration_since(a).as_millis() as u64;
        tracing::info!(
            env_id = %environment_id,
            %snapshot_id,
            pause_ms = ms(t0, t1),
            create_ms = ms(t1, t2),
            copy_ms = ms(t2, t3),
            "snapshot captured (source stays paused)"
        );
        Ok(SnapshotCapture {
            dir: dest,
            timings: SnapshotTimings {
                pause_ms: ms(t0, t1),
                create_ms: ms(t1, t2),
                copy_ms: ms(t2, t3),
            },
        })
    }

    pub(super) async fn clone_impl(
        &self,
        clone: CloneSpec,
    ) -> Result<(EnvironmentHandle, CloneTimings), ProviderError> {
        let started_at = Instant::now();
        let spec = &clone.spec;
        let env_id = spec.environment_id.clone();
        let paths = self.paths_for(&env_id);
        let Some(jailer) = self.cfg.jailer.clone() else {
            return Err(ProviderError::Unavailable("clones need the jailer".into()));
        };
        if let Some(Err(reason)) = &self.jail_exec {
            return Err(ProviderError::Unavailable(format!("jailer: {reason}")));
        }
        if let Err(reason) = crate::jail::host_support(&self.cfg, &jailer) {
            return Err(ProviderError::Unavailable(format!("jailer: {reason}")));
        }
        if spec.egress != EgressProfile::None || !spec.egress_allow.is_empty() {
            return Err(ProviderError::InvalidSpec(
                "a clone supports egress none only (X1): a restored NIC keeps the source's \
                 address, MAC and resolver"
                    .into(),
            ));
        }
        if self.cfg.cgroup.mode == CgroupMode::Required
            && let Err(reason) = crate::cgroup::host_support(&self.cfg.cgroup)
        {
            return Err(ProviderError::Unavailable(format!(
                "host cgroup limits are required but unavailable: {reason}"
            )));
        }
        if paths.longest_socket_path_len() > MAX_UNIX_SOCKET_PATH {
            return Err(ProviderError::InvalidSpec(
                "unix socket path too long".into(),
            ));
        }
        if paths.dir.exists() || self.running.lock().await.contains_key(&env_id) {
            return Err(ProviderError::InvalidSpec(format!(
                "environment {env_id} already exists"
            )));
        }
        for name in files::ALL {
            if !clone.snapshot_dir.join(name).is_file() {
                return Err(ProviderError::Boot(format!(
                    "snapshot file {name} is missing in {}",
                    clone.snapshot_dir.display()
                )));
            }
        }
        tokio::fs::create_dir_all(&paths.dir).await?;
        let mut slot: Option<Tracked> = None;
        match self
            .clone_boot(&clone, &jailer, &paths, started_at, &mut slot)
            .await
        {
            Ok(done) => {
                let tracked = slot.take().expect("clone booted with a VMM");
                self.running.lock().await.insert(env_id, tracked);
                Ok(done)
            }
            Err(err) => {
                tracing::warn!(env_id = %env_id, error = %err, "clone failed; cleaning up");
                if let Some(mut t) = slot.take() {
                    t.kill().await;
                }
                let mut cleaned = Vec::new();
                self.release_host(&env_id, &paths, &mut cleaned).await;
                let _ = self.archive_logs(&env_id, &paths).await;
                Self::remove_env_files(&paths, &mut cleaned).await;
                Err(err)
            }
        }
    }

    async fn clone_boot(
        &self,
        clone: &CloneSpec,
        jailer: &crate::config::JailerConfig,
        paths: &EnvPaths,
        started_at: Instant,
        slot: &mut Option<Tracked>,
    ) -> Result<(EnvironmentHandle, CloneTimings), ProviderError> {
        let spec = &clone.spec;
        let env_id = spec.environment_id.as_str();
        let snap = &clone.snapshot_dir;

        // Host disk: the private scratch copy (sparse, at most its size).
        let scratch_size = std::fs::metadata(snap.join(files::SCRATCH))?.len();
        let available = crate::host_guard::available_bytes(&paths.dir)?;
        let needed = scratch_size
            .saturating_add(self.cfg.console_log_max_bytes)
            .saturating_add(self.cfg.fc_log_max_bytes)
            .saturating_add(self.cfg.min_host_free_bytes);
        if available < needed {
            return Err(ProviderError::Unavailable(format!(
                "host disk too full for a clone: {available} bytes available, {needed} needed"
            )));
        }

        // Private writable area, shared read-only function drive.
        let copy_started = Instant::now();
        copy_image(&snap.join(files::SCRATCH), &paths.scratch_drive).await?;
        let scratch_copy_ms = copy_started.elapsed().as_millis() as u64;
        std::fs::hard_link(snap.join(files::FUNCTION_DRIVE), &paths.function_drive)
            .map_err(|e| ProviderError::Boot(format!("link the function drive: {e}")))?;

        // Cgroup of its own.
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

        // Jail of its own, with the snapshot hard-linked in.
        let jail = paths
            .jail
            .as_ref()
            .ok_or_else(|| ProviderError::Unavailable("clones need the jailer".into()))?;
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
        for (name, target) in [
            (files::MEMORY, CHROOT_MEM),
            (files::VMSTATE, CHROOT_VMSTATE),
        ] {
            std::fs::hard_link(snap.join(name), jail.host(target)).map_err(|e| {
                ProviderError::Boot(format!("link snapshot {name} into the jail: {e}"))
            })?;
        }

        // The guest reconnects to its (new) chroot's listener.
        let listener = UnixListener::bind(&paths.vsock_listener).map_err(|e| {
            ProviderError::Boot(format!("bind {}: {e}", paths.vsock_listener.display()))
        })?;
        crate::jail::chown(&paths.vsock_listener, jailer.uid, jailer.gid)
            .map_err(ProviderError::Boot)?;

        let (console, pid, instance_id) = self
            .spawn_and_wait_api(env_id, paths, cgroup.as_ref(), slot)
            .await?;
        let vmm = slot.as_mut().expect("spawned above");

        // Load paused, check the egress gate, then resume.
        let api = ApiClient::new(&paths.api_sock, SNAPSHOT_API_TIMEOUT);
        let load_started = Instant::now();
        if let Err(e) = api
            .put(
                "/snapshot/load",
                &serde_json::json!({
                    "snapshot_path": format!("/{CHROOT_VMSTATE}"),
                    "mem_backend": {"backend_type": "File", "backend_path": format!("/{CHROOT_MEM}")},
                    "resume_vm": false,
                }),
            )
            .await
        {
            return Err(match vmm.exited() {
                Some(status) => Self::boot_error_after_exit(
                    paths,
                    &console,
                    format!("firecracker exited during PUT /snapshot/load ({status}): {e}"),
                )
                .await,
                None => Self::boot_error(paths, format!("PUT /snapshot/load: {e}")),
            });
        }
        let loaded_at = Instant::now();
        let vm_config = ApiClient::new(&paths.api_sock, API_TIMEOUT)
            .get_json("/vm/config")
            .await
            .map_err(|e| {
                Self::boot_error(
                    paths,
                    format!("egress gate: cannot read the restored VM configuration: {e}"),
                )
            })?;
        check_vm_config(&vm_config, None).map_err(|e| Self::boot_error(paths, e))?;
        ApiClient::new(&paths.api_sock, API_TIMEOUT)
            .patch("/vm", &serde_json::json!({ "state": VM_STATE_RESUMED }))
            .await
            .map_err(|e| Self::boot_error(paths, format!("PATCH /vm Resumed after load: {e}")))?;
        let resumed_at = Instant::now();

        // Doorbell: the restored guest's old connection is dead; tell it now.
        let bell = ring_doorbell(&paths.vsock_uds, clone.doorbell_port).await;
        let doorbell_at = bell.map(|_| Instant::now());
        if bell.is_none() {
            tracing::warn!(
                env_id,
                "the restored guest did not answer its doorbell; waiting for it to reconnect by itself"
            );
        }

        let stream = tokio::select! {
            accepted = tokio::time::timeout(spec.connect_timeout, listener.accept()) => match accepted {
                Ok(Ok((stream, _))) => stream,
                Ok(Err(e)) => return Err(Self::boot_error(paths, format!("accept: {e}"))),
                Err(_) => {
                    return Err(Self::boot_error(
                        paths,
                        format!("timeout waiting for the restored guest to reconnect after {:?}", spec.connect_timeout),
                    ));
                }
            },
            status = vmm.wait_exit() => {
                return Err(Self::boot_error_after_exit(
                    paths,
                    &console,
                    format!("firecracker exited before the restored guest reconnected ({status})"),
                ).await);
            }
        };
        let connected_at = Instant::now();
        drop(listener);

        let ms = |a: Instant, b: Instant| b.saturating_duration_since(a).as_millis() as u64;
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
            "restored_from_snapshot".into(),
            clone.snapshot_id.to_string().into(),
        );
        details.insert("vcpus".into(), spec.resources.vcpus().into());
        details.insert("mem_mib".into(), spec.resources.memory_mib.into());
        details.insert("egress_profile".into(), "none".into());
        details.insert("network_interfaces".into(), 0.into());
        details.insert("scratch_drive_copy_ms".into(), scratch_copy_ms.into());
        details.insert("scratch_drive_bytes".into(), scratch_size.into());
        details.insert(
            "snapshot_load_ms".into(),
            ms(load_started, loaded_at).into(),
        );
        details.insert("resume_ms".into(), ms(loaded_at, resumed_at).into());
        details.insert("doorbell_attempts".into(), bell.unwrap_or(0).into());
        details.insert(
            "reconnect_after_resume_ms".into(),
            ms(resumed_at, connected_at).into(),
        );
        details.insert("cgroup_mode".into(), self.cfg.cgroup.mode.as_str().into());
        details.insert(
            "cgroup".into(),
            cgroup
                .as_ref()
                .map(|c| c.path.display().to_string().into())
                .unwrap_or(serde_json::Value::Null),
        );
        details.insert("jailed".into(), true.into());
        details.insert("jail_root".into(), jail.root.display().to_string().into());
        details.insert("vmm_uid".into(), jailer.uid.into());
        details.insert("env_dir".into(), paths.dir.display().to_string().into());
        details.insert("boot_ms".into(), ms(started_at, connected_at).into());
        Ok((
            EnvironmentHandle {
                environment_id: spec.environment_id.clone(),
                evidence: tachyon_serverless_domain::BootEvidence {
                    guest_boot_id: None,
                    host_pid: Some(pid),
                    details,
                },
                stream: Box::new(stream),
                created_at: started_at,
                connected_at,
            },
            CloneTimings {
                started_at,
                loaded_at,
                doorbell_at,
            },
        ))
    }
}
