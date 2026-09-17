//! Experimental snapshot / restore (X1, PLT-4653;
//! docs/adr/0017-snapshot-manifest-and-clone.md).
//!
//! Off unless `[snapshots] enabled = true`. With it off, no revision can be
//! restored (a `require` revision fails with
//! `Host.RestoreRequiredUnavailable`, a `prefer` one starts cold) and nothing
//! in the invoke path changes for `disabled` revisions.

pub mod service;
pub mod store;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tachyon_serverless_domain::SnapshotSigningKey;

pub use service::{
    RestorePlan, RestoreUnavailable, SnapshotService, SnapshotServiceDeps, SnapshotSettings,
};
pub use store::{SnapshotRecord, SnapshotStore, StoreError};

use crate::durable::ObjectKey;
use crate::error::AppError;

/// `[snapshots]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SnapshotsConfig {
    pub enabled: bool,
    /// Catalog and sealed artifacts. Default `<data_dir>/snapshots`.
    pub root: Option<PathBuf>,
    /// 64 hex characters (32 bytes): encrypts the artifacts. Its fingerprint
    /// is the manifest's `encryption_key_generation`; a new key makes every
    /// older snapshot stale.
    pub key_file: Option<PathBuf>,
    pub key_env: Option<String>,
    /// 64 hex characters: signs manifests (HMAC-SHA256).
    pub signing_key_file: Option<PathBuf>,
    pub signing_key_env: Option<String>,
    /// Lifetime of a snapshot. Default 1 hour, at most 7 days.
    pub ttl_seconds: u64,
    /// Accept `Unverified` snapshot capabilities (a measurement run).
    pub allow_unverified: bool,
    /// Handshake budget of a snapshot source. Default 30 s.
    pub handshake_timeout_ms: u64,
}

impl Default for SnapshotsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            root: None,
            key_file: None,
            key_env: None,
            signing_key_file: None,
            signing_key_env: None,
            ttl_seconds: 3600,
            allow_unverified: false,
            handshake_timeout_ms: 30_000,
        }
    }
}

pub const MAX_TTL_SECONDS: u64 = 7 * 24 * 3600;

impl SnapshotsConfig {
    pub fn settings(&self) -> Result<SnapshotSettings, AppError> {
        if self.ttl_seconds == 0 || self.ttl_seconds > MAX_TTL_SECONDS {
            return Err(AppError::InvalidRequest(format!(
                "[snapshots] ttl_seconds must be within 1..={MAX_TTL_SECONDS}"
            )));
        }
        Ok(SnapshotSettings {
            ttl: Duration::from_secs(self.ttl_seconds),
            allow_unverified: self.allow_unverified,
            handshake_timeout: Duration::from_millis(self.handshake_timeout_ms.max(1000)),
        })
    }

    pub fn load_key(&self) -> Result<Arc<ObjectKey>, AppError> {
        match (&self.key_file, &self.key_env) {
            (Some(path), _) => ObjectKey::from_file(path),
            (None, Some(var)) => ObjectKey::from_env(var),
            (None, None) => {
                return Err(AppError::InvalidRequest(
                    "[snapshots] needs key_file or key_env".into(),
                ));
            }
        }
        .map(Arc::new)
        .map_err(|e| AppError::InvalidRequest(format!("[snapshots] key: {e}")))
    }

    pub fn load_signing_key(&self) -> Result<Arc<SnapshotSigningKey>, AppError> {
        let text = match (&self.signing_key_file, &self.signing_key_env) {
            (Some(path), _) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = std::fs::metadata(path)
                        .map_err(|e| {
                            AppError::InvalidRequest(format!(
                                "[snapshots] signing key {}: {e}",
                                path.display()
                            ))
                        })?
                        .permissions()
                        .mode();
                    if mode & 0o077 != 0 {
                        return Err(AppError::InvalidRequest(format!(
                            "[snapshots] signing key {} is readable by group or others (chmod 600)",
                            path.display()
                        )));
                    }
                }
                std::fs::read_to_string(path).map_err(|e| {
                    AppError::InvalidRequest(format!(
                        "[snapshots] signing key {}: {e}",
                        path.display()
                    ))
                })?
            }
            (None, Some(var)) => std::env::var(var).map_err(|_| {
                AppError::InvalidRequest(format!("[snapshots] signing key env {var} is not set"))
            })?,
            (None, None) => {
                return Err(AppError::InvalidRequest(
                    "[snapshots] needs signing_key_file or signing_key_env".into(),
                ));
            }
        };
        SnapshotSigningKey::from_hex(&text)
            .map(Arc::new)
            .map_err(|e| AppError::InvalidRequest(format!("[snapshots] signing key: {e}")))
    }
}
