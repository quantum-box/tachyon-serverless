//! Local / static implementations of the non-execution ports:
//! artifact store on disk, identity and secrets from configuration, and an
//! in-memory usage sink.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use parking_lot::Mutex;

use tachyon_serverless_domain::{InvocationId, Sha256Digest, TenantId, UsageEvent, UsageEventType};
use tachyon_serverless_provider_port::{
    ArtifactError, ArtifactStore, Credential, IdentityProvider, Principal, SecretDeliveryContext,
    SecretError, SecretProvider, SecretValue, StoredArtifact, UsageSink,
};

use crate::config::{SecretBindingConfig, TokenConfig};

// ---------------------------------------------------------------------------
// artifacts
// ---------------------------------------------------------------------------

/// Content-addressed store under `<data_dir>/artifacts/<hex>`. Files are
/// written atomically (tmp + rename) with mode 0755 so providers can execute
/// them directly. `get` re-hashes the file and fails when the content does
/// not match its name.
#[derive(Debug, Clone)]
pub struct LocalArtifactStore {
    root: PathBuf,
    max_bytes: u64,
}

impl LocalArtifactStore {
    pub fn new(data_dir: &Path, max_bytes: u64) -> Result<Self, ArtifactError> {
        let root = data_dir.join("artifacts");
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, max_bytes })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn path_for(&self, digest: &Sha256Digest) -> PathBuf {
        self.root.join(digest.hex())
    }
}

#[cfg(unix)]
fn set_executable(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn put(&self, bytes: &[u8]) -> Result<StoredArtifact, ArtifactError> {
        let size = bytes.len() as u64;
        if size > self.max_bytes {
            return Err(ArtifactError::TooLarge {
                size,
                max: self.max_bytes,
            });
        }
        let digest = Sha256Digest::of_bytes(bytes);
        let path = self.path_for(&digest);
        if tokio::fs::try_exists(&path).await? {
            return Ok(StoredArtifact {
                digest,
                size_bytes: size,
                path,
            });
        }
        let tmp = self
            .root
            .join(format!(".{}.{}.tmp", digest.hex(), std::process::id()));
        tokio::fs::write(&tmp, bytes).await?;
        set_executable(&tmp)?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(StoredArtifact {
            digest,
            size_bytes: size,
            path,
        })
    }

    async fn get(&self, digest: &Sha256Digest) -> Result<StoredArtifact, ArtifactError> {
        let path = self.path_for(digest);
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ArtifactError::NotFound(digest.clone()));
            }
            Err(e) => return Err(e.into()),
        };
        let verify_path = path.clone();
        let actual = tokio::task::spawn_blocking(move || -> std::io::Result<Sha256Digest> {
            let bytes = std::fs::read(&verify_path)?;
            Ok(Sha256Digest::of_bytes(&bytes))
        })
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))??;
        if &actual != digest {
            return Err(ArtifactError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("artifact {digest} is corrupt (content digest {actual})"),
            )));
        }
        Ok(StoredArtifact {
            digest: digest.clone(),
            size_bytes: meta.len(),
            path,
        })
    }

    async fn exists(&self, digest: &Sha256Digest) -> Result<bool, ArtifactError> {
        Ok(tokio::fs::try_exists(self.path_for(digest)).await?)
    }
}

// ---------------------------------------------------------------------------
// identity
// ---------------------------------------------------------------------------

/// Bearer tokens from configuration.
pub struct StaticIdentityProvider {
    tokens: HashMap<String, Principal>,
}

impl std::fmt::Debug for StaticIdentityProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticIdentityProvider")
            .field("tokens", &self.tokens.len())
            .finish()
    }
}

impl StaticIdentityProvider {
    pub fn from_config(tokens: &[TokenConfig]) -> Self {
        let tokens = tokens
            .iter()
            .map(|t| {
                (
                    t.token.expose().to_string(),
                    Principal {
                        subject: t.subject.clone(),
                        tenant_id: t.tenant_id.clone(),
                        roles: t.roles.clone(),
                    },
                )
            })
            .collect();
        Self { tokens }
    }
}

#[async_trait]
impl IdentityProvider for StaticIdentityProvider {
    async fn authenticate(&self, credential: &Credential) -> Option<Principal> {
        self.tokens.get(&credential.0).cloned()
    }
}

// ---------------------------------------------------------------------------
// secrets
// ---------------------------------------------------------------------------

/// Tenant-scoped secret bindings from configuration.
pub struct StaticSecretProvider {
    values: HashMap<(TenantId, String), SecretValue>,
    known_refs: HashSet<String>,
}

impl std::fmt::Debug for StaticSecretProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticSecretProvider")
            .field("bindings", &self.values.len())
            .finish()
    }
}

impl StaticSecretProvider {
    pub fn from_config(bindings: &[SecretBindingConfig]) -> Self {
        let mut values = HashMap::new();
        let mut known_refs = HashSet::new();
        for b in bindings {
            known_refs.insert(b.binding_ref.clone());
            values.insert(
                (b.tenant_id.clone(), b.binding_ref.clone()),
                SecretValue::new(b.value.expose()),
            );
        }
        Self { values, known_refs }
    }
}

#[async_trait]
impl SecretProvider for StaticSecretProvider {
    async fn resolve(
        &self,
        ctx: &SecretDeliveryContext,
        binding_ref: &str,
    ) -> Result<SecretValue, SecretError> {
        if let Some(v) = self
            .values
            .get(&(ctx.tenant_id.clone(), binding_ref.to_string()))
        {
            return Ok(v.clone());
        }
        if self.known_refs.contains(binding_ref) {
            Err(SecretError::Forbidden {
                binding: binding_ref.to_string(),
                tenant: ctx.tenant_id.clone(),
            })
        } else {
            Err(SecretError::NotFound(binding_ref.to_string()))
        }
    }
}

// ---------------------------------------------------------------------------
// usage
// ---------------------------------------------------------------------------

/// Host-observed totals for a set of invocations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageTotals {
    pub handler_ms_total: u64,
    pub environment_ms_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
    pub events: u64,
}

/// De-duplicating in-memory usage sink.
#[derive(Debug, Default)]
pub struct InMemoryUsageSink {
    events: Mutex<Vec<UsageEvent>>,
    seen: Mutex<HashSet<String>>,
}

impl InMemoryUsageSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<UsageEvent> {
        self.events.lock().clone()
    }

    pub fn events_for_invocation(&self, invocation: &InvocationId) -> Vec<UsageEvent> {
        self.events
            .lock()
            .iter()
            .filter(|e| e.invocation_id.as_ref() == Some(invocation))
            .cloned()
            .collect()
    }

    /// Sum the host-observed facts for the given invocations (a function's
    /// invocations, as resolved by the caller from the ledger).
    pub fn totals_for(&self, invocations: &[InvocationId]) -> UsageTotals {
        let wanted: HashSet<&InvocationId> = invocations.iter().collect();
        let mut t = UsageTotals::default();
        for e in self.events.lock().iter() {
            let Some(inv) = &e.invocation_id else {
                continue;
            };
            if !wanted.contains(inv) {
                continue;
            }
            t.events += 1;
            match e.event_type {
                UsageEventType::HandlerFinished => {
                    t.handler_ms_total += e.monotonic_duration_ms.unwrap_or(0);
                    t.bytes_in_total += e.bytes_in;
                    t.bytes_out_total += e.bytes_out;
                }
                UsageEventType::EnvironmentStopped => {
                    t.environment_ms_total += e.monotonic_duration_ms.unwrap_or(0);
                }
                UsageEventType::EnvironmentStarted | UsageEventType::HandlerStarted => {}
            }
        }
        t
    }
}

#[async_trait]
impl UsageSink for InMemoryUsageSink {
    async fn record(&self, event: UsageEvent) {
        if !self.seen.lock().insert(event.event_id.clone()) {
            return;
        }
        self.events.lock().push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConfigSecret, ConfigToken};
    use chrono::Utc;
    use tachyon_serverless_domain::{AttemptId, EnvironmentId, EvidenceQuality, RevisionId};
    use tachyon_serverless_provider_port::Role;

    #[tokio::test]
    async fn artifact_store_put_get_exists_and_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalArtifactStore::new(dir.path(), 1024).unwrap();
        let a = store.put(b"#!/bin/sh\necho hi\n").await.unwrap();
        assert!(a.path.starts_with(dir.path().join("artifacts")));
        assert!(store.exists(&a.digest).await.unwrap());
        let again = store.put(b"#!/bin/sh\necho hi\n").await.unwrap();
        assert_eq!(again, a, "idempotent");
        let got = store.get(&a.digest).await.unwrap();
        assert_eq!(got.size_bytes, a.size_bytes);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&a.path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o755);
        }
        assert!(matches!(
            store.put(&vec![0u8; 2048]).await,
            Err(ArtifactError::TooLarge { .. })
        ));
        assert!(matches!(
            store.get(&Sha256Digest::of_bytes(b"missing")).await,
            Err(ArtifactError::NotFound(_))
        ));
        // corrupt the file: get must fail
        std::fs::write(&a.path, b"tampered").unwrap();
        assert!(matches!(
            store.get(&a.digest).await,
            Err(ArtifactError::Io(_))
        ));
    }

    #[tokio::test]
    async fn identity_and_secrets_are_tenant_scoped() {
        let ta = TenantId::generate();
        let tb = TenantId::generate();
        let idp = StaticIdentityProvider::from_config(&[TokenConfig {
            token: ConfigToken::new("tok-a"),
            tenant_id: ta.clone(),
            subject: "a".into(),
            roles: vec![Role::Deploy],
        }]);
        let p = idp.authenticate(&Credential("tok-a".into())).await.unwrap();
        assert_eq!(p.tenant_id, ta);
        assert!(idp.authenticate(&Credential("nope".into())).await.is_none());

        let secrets = StaticSecretProvider::from_config(&[SecretBindingConfig {
            tenant_id: ta.clone(),
            binding_ref: "db".into(),
            value: ConfigSecret::new("s3cr3t-value-xyz"),
        }]);
        let ctx = |t: &TenantId| SecretDeliveryContext {
            tenant_id: t.clone(),
            revision_id: RevisionId::generate(),
            environment_id: EnvironmentId::generate(),
            epoch: 1,
        };
        assert_eq!(
            secrets.resolve(&ctx(&ta), "db").await.unwrap().expose(),
            "s3cr3t-value-xyz"
        );
        assert!(matches!(
            secrets.resolve(&ctx(&tb), "db").await,
            Err(SecretError::Forbidden { .. })
        ));
        assert!(matches!(
            secrets.resolve(&ctx(&ta), "other").await,
            Err(SecretError::NotFound(_))
        ));
        assert!(!format!("{secrets:?}").contains("s3cr3t-value-xyz"));
    }

    #[tokio::test]
    async fn usage_sink_dedups_and_sums() {
        let sink = InMemoryUsageSink::new();
        let inv = InvocationId::generate();
        let ev = |id: &str, t: UsageEventType, ms: u64| UsageEvent {
            event_id: id.into(),
            tenant_id: TenantId::generate(),
            environment_id: EnvironmentId::generate(),
            invocation_id: Some(inv.clone()),
            attempt_id: Some(AttemptId::generate()),
            event_type: t,
            sequence: 1,
            observed_at: Utc::now(),
            monotonic_duration_ms: Some(ms),
            memory_mib: 256,
            cpu_millis: 500,
            bytes_in: 10,
            bytes_out: 20,
            meter_version: 1,
            evidence_quality: EvidenceQuality::HostObserved,
        };
        sink.record(ev("1", UsageEventType::HandlerFinished, 5))
            .await;
        sink.record(ev("1", UsageEventType::HandlerFinished, 5))
            .await;
        sink.record(ev("2", UsageEventType::EnvironmentStopped, 50))
            .await;
        let t = sink.totals_for(std::slice::from_ref(&inv));
        assert_eq!(t.events, 2);
        assert_eq!(t.handler_ms_total, 5);
        assert_eq!(t.environment_ms_total, 50);
        assert_eq!(t.bytes_in_total, 10);
        assert_eq!(t.bytes_out_total, 20);
        assert_eq!(sink.totals_for(&[]).events, 0);
    }
}
