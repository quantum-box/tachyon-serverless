//! Snapshot / clone port types (X1, PLT-4653, experimental;
//! docs/adr/0017-snapshot-manifest-and-clone.md).
//!
//! The provider only moves bytes between a paused VMM and a directory, and
//! loads a directory into a new VMM. Deciding *whether* a snapshot may be
//! taken or loaded (tenant, revision, profile, integrity, expiry, policy) is
//! the application's job, done before either call.

use std::path::PathBuf;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tachyon_serverless_domain::{DeviceModel, HostCpuIdentity, RuntimeProfile, SnapshotId};

use crate::EnvironmentSpec;

/// File names inside a snapshot directory.
pub mod files {
    /// Guest memory (loaded MAP_PRIVATE; never written by a clone).
    pub const MEMORY: &str = "memory";
    /// VMM device state.
    pub const VMSTATE: &str = "vmstate";
    /// Scratch drive captured while paused (each clone gets a private copy).
    pub const SCRATCH: &str = "scratch.ext4";
    /// Function drive the source had mounted (shared read-only).
    pub const FUNCTION_DRIVE: &str = "function.ext4";
    /// All four, in manifest order.
    pub const ALL: [&str; 4] = [MEMORY, VMSTATE, SCRATCH, FUNCTION_DRIVE];
}

/// Facts about this host and provider that a snapshot manifest pins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreHostProfile {
    pub runtime: RuntimeProfile,
    pub host_cpu: HostCpuIdentity,
    /// Device model of an environment with egress `none` (the only one X1
    /// snapshots).
    pub devices: DeviceModel,
}

/// Timings of one snapshot, host clock, milliseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTimings {
    /// `PATCH /vm Paused`.
    pub pause_ms: u64,
    /// `PUT /snapshot/create` (memory + vmstate).
    pub create_ms: u64,
    /// Scratch and function drive copied while paused.
    pub copy_ms: u64,
}

/// Result of [`crate::ExecutionProvider::snapshot_environment`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCapture {
    /// Directory holding [`files::ALL`], readable only by the host (and the
    /// jailed VMM uid through hard links).
    pub dir: PathBuf,
    pub timings: SnapshotTimings,
}

/// What [`crate::ExecutionProvider::clone_environment`] loads.
#[derive(Debug, Clone)]
pub struct CloneSpec {
    /// The new environment. Its id, cgroup, jail, sockets and scratch copy
    /// are new; resources must equal the snapshot's (checked before).
    pub spec: EnvironmentSpec,
    pub snapshot_id: SnapshotId,
    /// Directory with [`files::ALL`], already verified against the manifest.
    pub snapshot_dir: PathBuf,
    /// Guest port to ring after the load (`DOORBELL_VSOCK_PORT`).
    pub doorbell_port: u32,
}

/// Extra timings of a clone for evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloneTimings {
    pub started_at: Instant,
    /// `PUT /snapshot/load` returned.
    pub loaded_at: Instant,
    /// The doorbell connection was accepted by the guest (None if it never
    /// was; the guest may still have reconnected on its own).
    pub doorbell_at: Option<Instant>,
}
