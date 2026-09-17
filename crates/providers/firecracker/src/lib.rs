//! Firecracker (Linux/KVM) execution provider for Tachyon Serverless.
//!
//! Implements [`tachyon_serverless_provider_port::ExecutionProvider`] on top
//! of the Firecracker API socket, a per-environment read-only ext4 "function
//! drive" and a guest-initiated vsock connection to the runtime bridge
//! (docs/protocol.md §C). The crate compiles and unit-tests on any Unix host;
//! booting a microVM requires Linux with `/dev/kvm`.
//!
//! ```no_run
//! use std::path::PathBuf;
//! use tachyon_serverless_provider_firecracker::{FirecrackerConfig, FirecrackerProvider};
//!
//! let provider = FirecrackerProvider::new(FirecrackerConfig {
//!     firecracker_binary: PathBuf::from(".kvm/bin/firecracker"),
//!     kernel: PathBuf::from(".kvm/vmlinux"),
//!     rootfs: PathBuf::from(".kvm/rootfs.ext4"),
//!     workdir: PathBuf::from(".kvm/run"),
//!     vsock_port: 5000,
//!     ..Default::default()
//! });
//! # let _ = provider;
//! ```

pub mod api;
pub mod boot_args;
pub mod config;
pub mod drive;
pub mod egress_gate;
pub mod elf;
pub mod host_guard;
pub mod preflight;
pub mod provider;
pub mod vmm;

pub use config::FirecrackerConfig;
pub use provider::{FirecrackerProvider, artifact_location_for, check_elf};
