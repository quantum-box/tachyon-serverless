//! Artifact store: content-addressed executables.

use std::path::PathBuf;

use async_trait::async_trait;
use tachyon_serverless_domain::Sha256Digest;

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact too large: {size} bytes (max {max})")]
    TooLarge { size: u64, max: u64 },
    #[error("artifact not found: {0}")]
    NotFound(Sha256Digest),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredArtifact {
    pub digest: Sha256Digest,
    pub size_bytes: u64,
    pub path: PathBuf,
}

#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store bytes; returns the digest. Idempotent for identical content.
    async fn put(&self, bytes: &[u8]) -> Result<StoredArtifact, ArtifactError>;
    async fn get(&self, digest: &Sha256Digest) -> Result<StoredArtifact, ArtifactError>;
    async fn exists(&self, digest: &Sha256Digest) -> Result<bool, ArtifactError>;
}
