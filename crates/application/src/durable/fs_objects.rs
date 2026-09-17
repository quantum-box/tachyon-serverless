//! Filesystem [`ObjectStore`] (ADR-0008 §「object」).
//!
//! Layout under `root` (default `<data_dir>/objects`, directories 0700,
//! files 0600):
//!
//! ```text
//! <root>/<region>/<tenant_id>/<object_id>.data   MAGIC || nonce || AES-256-GCM(plaintext)
//! <root>/<region>/<tenant_id>/<object_id>.meta   JSON ObjectMeta (commit point)
//! <root>/<region>/<tenant_id>/.tmp-<object_id>.{data,meta}
//! ```
//!
//! A put writes and fsyncs the data, then the metadata; the metadata rename
//! is the commit point, so an object without `.meta` does not exist (it is an
//! incomplete write the GC removes after the grace period). A delete removes
//! the metadata first for the same reason.
//!
//! **Not replicated.** Everything lives on the local disk of one node; there
//! is no second copy, no cross-region copy, and losing the disk loses the
//! objects (ADR-0008 「保存先と複製」).
//!
//! Quota is checked and the object written under one in-process lock, so two
//! gateways sharing the same `root` could together exceed a tenant quota by
//! at most one object each. Sharing `root` between processes is not supported.

use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{Clock, Sha256Digest, TenantId, Timestamp};
use tachyon_serverless_durable_port::{
    EncryptionRef, ObjectError, ObjectId, ObjectListing, ObjectMeta, ObjectRef, ObjectScope,
    ObjectStore, PutObject, Region, StoredObject,
};

use super::crypto::{ALGORITHM, ObjectKey};

/// Store options (from `[objects]`).
#[derive(Debug, Clone)]
pub struct FsObjectOptions {
    pub regions: Vec<Region>,
    pub max_object_bytes: u64,
    pub tenant_quota_bytes: u64,
    /// Applied when a put has no TTL. `None`: no expiry.
    pub default_ttl: Option<Duration>,
}

#[derive(Serialize, Deserialize)]
struct MetaFile {
    format: u32,
    meta: ObjectMeta,
}

const META_FORMAT: u32 = 1;

struct Inner {
    root: PathBuf,
    key: ObjectKey,
    options: FsObjectOptions,
    clock: Arc<dyn Clock>,
    /// Serializes quota check + write.
    put_lock: parking_lot::Mutex<()>,
}

#[derive(Clone)]
pub struct FsObjectStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for FsObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsObjectStore")
            .field("root", &self.inner.root)
            .field("key_id", &self.inner.key.id())
            .field("options", &self.inner.options)
            .finish()
    }
}

fn io(e: std::io::Error) -> ObjectError {
    ObjectError::Io(e)
}

#[cfg(unix)]
fn private_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut f = options.open(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}

fn sync_dir(path: &Path) {
    if let Ok(d) = std::fs::File::open(path) {
        let _ = d.sync_all();
    }
}

fn aad(meta: &ObjectMeta) -> Vec<u8> {
    format!(
        "tachyon-serverless/object/v1|{}|{}|{}|{}|{}|{}",
        meta.reference.id,
        meta.reference.scope.tenant_id,
        meta.reference.scope.region,
        meta.encryption.key_id,
        meta.digest,
        meta.size_bytes
    )
    .into_bytes()
}

impl FsObjectStore {
    pub fn open(
        root: &Path,
        key: ObjectKey,
        options: FsObjectOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ObjectError> {
        if options.regions.is_empty() {
            return Err(ObjectError::Backend(
                "an object store must serve at least one region".into(),
            ));
        }
        private_dir(root).map_err(io)?;
        Ok(Self {
            inner: Arc::new(Inner {
                root: root.to_path_buf(),
                key,
                options,
                clock,
                put_lock: parking_lot::Mutex::new(()),
            }),
        })
    }

    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    pub fn key_id(&self) -> &str {
        self.inner.key.id()
    }

    /// Where an object's files live (tests tamper with them).
    pub fn paths_of(&self, scope: &ObjectScope, id: &ObjectId) -> (PathBuf, PathBuf) {
        self.inner.paths(scope, id)
    }

    async fn blocking<R: Send + 'static>(
        &self,
        f: impl FnOnce(&Inner) -> Result<R, ObjectError> + Send + 'static,
    ) -> Result<R, ObjectError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&inner))
            .await
            .map_err(|e| ObjectError::Backend(format!("object store task: {e}")))?
    }
}

impl Inner {
    fn dir(&self, scope: &ObjectScope) -> PathBuf {
        // Both components are validated labels (Region, TenantId): no
        // separators, no `..`.
        self.root
            .join(scope.region.as_str())
            .join(scope.tenant_id.as_str())
    }

    fn paths(&self, scope: &ObjectScope, id: &ObjectId) -> (PathBuf, PathBuf) {
        let dir = self.dir(scope);
        (
            dir.join(format!("{id}.data")),
            dir.join(format!("{id}.meta")),
        )
    }

    fn serves(&self, region: &Region) -> bool {
        self.options.regions.contains(region)
    }

    fn read_meta(&self, path: &Path) -> Result<Option<ObjectMeta>, ObjectError> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let file: MetaFile = serde_json::from_slice(&bytes).map_err(|e| {
                    ObjectError::Integrity(format!("unreadable metadata {}: {e}", path.display()))
                })?;
                if file.format != META_FORMAT {
                    return Err(ObjectError::Integrity(format!(
                        "unknown metadata format {} in {}",
                        file.format,
                        path.display()
                    )));
                }
                Ok(Some(file.meta))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    /// The metadata of `id` as seen from `scope`: absent, or recorded under
    /// another scope, are both `NotFound`.
    fn scoped_meta(&self, scope: &ObjectScope, id: &ObjectId) -> Result<ObjectMeta, ObjectError> {
        if !self.serves(&scope.region) {
            return Err(ObjectError::NotFound);
        }
        let (_, meta_path) = self.paths(scope, id);
        let meta = self.read_meta(&meta_path)?.ok_or(ObjectError::NotFound)?;
        if &meta.reference.id != id || &meta.reference.scope != scope {
            return Err(ObjectError::NotFound);
        }
        Ok(meta)
    }

    fn all_meta(&self) -> Result<Vec<(PathBuf, ObjectMeta)>, ObjectError> {
        let mut out = Vec::new();
        for region in &self.options.regions {
            let region_dir = self.root.join(region.as_str());
            let tenants = match std::fs::read_dir(&region_dir) {
                Ok(t) => t,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io(e)),
            };
            for tenant in tenants {
                let tenant = tenant.map_err(io)?;
                if !tenant.file_type().map_err(io)?.is_dir() {
                    continue;
                }
                for entry in std::fs::read_dir(tenant.path()).map_err(io)? {
                    let path = entry.map_err(io)?.path();
                    if path.extension().is_some_and(|e| e == "meta")
                        && !path
                            .file_name()
                            .is_some_and(|n| n.to_string_lossy().starts_with('.'))
                    {
                        match self.read_meta(&path) {
                            Ok(Some(meta)) => out.push((path, meta)),
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(path = %path.display(), error = %e, "skipping unreadable object metadata");
                            }
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn usage(&self, tenant: &TenantId) -> Result<u64, ObjectError> {
        Ok(self
            .all_meta()?
            .iter()
            .filter(|(_, m)| &m.reference.scope.tenant_id == tenant)
            .map(|(_, m)| m.size_bytes)
            .sum())
    }

    fn put(&self, request: PutObject) -> Result<ObjectMeta, ObjectError> {
        let size = request.bytes.len() as u64;
        if size > self.options.max_object_bytes {
            return Err(ObjectError::TooLarge {
                size,
                max: self.options.max_object_bytes,
            });
        }
        if !self.serves(&request.scope.region) {
            return Err(ObjectError::RegionNotServed(request.scope.region));
        }
        let _guard = self.put_lock.lock();
        let used = self.usage(&request.scope.tenant_id)?;
        if used.saturating_add(size) > self.options.tenant_quota_bytes {
            return Err(ObjectError::QuotaExceeded {
                used,
                requested: size,
                quota: self.options.tenant_quota_bytes,
            });
        }
        let now = self.clock.now();
        let ttl = request.ttl.or(self.options.default_ttl);
        let expires_at = ttl.and_then(|d| {
            chrono::Duration::from_std(d)
                .ok()
                .and_then(|d| now.checked_add_signed(d))
        });
        let id = ObjectId::generate();
        let meta = ObjectMeta {
            reference: ObjectRef {
                id: id.clone(),
                scope: request.scope.clone(),
            },
            size_bytes: size,
            digest: Sha256Digest::of_bytes(&request.bytes),
            created_at: now,
            expires_at,
            encryption: EncryptionRef {
                algorithm: ALGORITHM.to_string(),
                key_id: self.key.id().to_string(),
            },
        };
        let dir = self.dir(&request.scope);
        private_dir(&dir).map_err(io)?;
        let (data_path, meta_path) = self.paths(&request.scope, &id);
        let tmp_data = dir.join(format!(".tmp-{id}.data"));
        let tmp_meta = dir.join(format!(".tmp-{id}.meta"));
        let sealed = self.key.seal(&request.bytes, &aad(&meta));
        write_private(&tmp_data, &sealed).map_err(io)?;
        std::fs::rename(&tmp_data, &data_path).map_err(io)?;
        let meta_json = serde_json::to_vec(&MetaFile {
            format: META_FORMAT,
            meta: meta.clone(),
        })
        .map_err(|e| ObjectError::Backend(e.to_string()))?;
        write_private(&tmp_meta, &meta_json).map_err(io)?;
        std::fs::rename(&tmp_meta, &meta_path).map_err(io)?;
        sync_dir(&dir);
        Ok(meta)
    }

    fn get(&self, scope: &ObjectScope, id: &ObjectId) -> Result<StoredObject, ObjectError> {
        let meta = self.scoped_meta(scope, id)?;
        if meta.encryption.algorithm != ALGORITHM {
            return Err(ObjectError::Encryption(format!(
                "unsupported algorithm {}",
                meta.encryption.algorithm
            )));
        }
        if meta.encryption.key_id != self.key.id() {
            return Err(ObjectError::Encryption(format!(
                "object is protected by key {}, which this store does not hold (current key {})",
                meta.encryption.key_id,
                self.key.id()
            )));
        }
        let (data_path, _) = self.paths(scope, id);
        let envelope = match std::fs::read(&data_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectError::Integrity(
                    "metadata exists but the data file is missing".into(),
                ));
            }
            Err(e) => return Err(io(e)),
        };
        let bytes = self.key.open(&envelope, &aad(&meta)).ok_or_else(|| {
            ObjectError::Integrity(
                "authentication failed: the data or its metadata was altered".into(),
            )
        })?;
        if bytes.len() as u64 != meta.size_bytes || Sha256Digest::of_bytes(&bytes) != meta.digest {
            return Err(ObjectError::Integrity(
                "plaintext digest does not match the recorded digest".into(),
            ));
        }
        Ok(StoredObject { meta, bytes })
    }

    fn delete(&self, scope: &ObjectScope, id: &ObjectId) -> Result<bool, ObjectError> {
        match self.scoped_meta(scope, id) {
            Ok(_) => {}
            Err(ObjectError::NotFound) => return Ok(false),
            // Corrupt metadata in the right place is still deletable.
            Err(ObjectError::Integrity(_)) => {}
            Err(e) => return Err(e),
        }
        let (data_path, meta_path) = self.paths(scope, id);
        std::fs::remove_file(&meta_path).map_err(io)?;
        match std::fs::remove_file(&data_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(e)),
        }
        sync_dir(&self.dir(scope));
        Ok(true)
    }

    fn list_candidates(
        &self,
        now: Timestamp,
        created_before: Timestamp,
        limit: usize,
    ) -> Result<ObjectListing, ObjectError> {
        let mut listing = ObjectListing::default();
        let metas = self.all_meta()?;
        let committed: BTreeSet<PathBuf> = metas.iter().map(|(p, _)| p.clone()).collect();
        for (_, meta) in metas {
            if listing.objects.len() >= limit {
                break;
            }
            let expired = meta.expires_at.is_some_and(|t| t <= now);
            if expired || meta.created_at < created_before {
                listing.objects.push(meta);
            }
        }
        let cutoff = std::time::SystemTime::from(created_before);
        for region in &self.options.regions {
            let region_dir = self.root.join(region.as_str());
            let Ok(tenants) = std::fs::read_dir(&region_dir) else {
                continue;
            };
            for tenant in tenants.flatten() {
                let Ok(entries) = std::fs::read_dir(tenant.path()) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    let name = entry.file_name().to_string_lossy().to_string();
                    let orphan_data = name.ends_with(".data")
                        && !name.starts_with('.')
                        && !committed.contains(&path.with_extension("meta"));
                    let tmp = name.starts_with(".tmp-");
                    if !(orphan_data || tmp) {
                        continue;
                    }
                    let old = entry
                        .metadata()
                        .and_then(|m| m.modified())
                        .is_ok_and(|m| m < cutoff);
                    if old && let Ok(rel) = path.strip_prefix(&self.root) {
                        listing.incomplete.push(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
        Ok(listing)
    }

    fn remove_incomplete(&self, names: &[String]) -> Result<usize, ObjectError> {
        let mut removed = 0;
        for name in names {
            let rel = Path::new(name);
            if rel.components().count() != 3
                || rel.components().any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err(ObjectError::InvalidReference(format!(
                    "not an incomplete object path: {name}"
                )));
            }
            let file = rel
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            if !(file.starts_with(".tmp-") || file.ends_with(".data")) {
                return Err(ObjectError::InvalidReference(format!(
                    "not an incomplete object path: {name}"
                )));
            }
            let path = self.root.join(rel);
            if file.ends_with(".data")
                && !file.starts_with('.')
                && path.with_extension("meta").exists()
            {
                // committed meanwhile
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io(e)),
            }
        }
        Ok(removed)
    }
}

#[async_trait]
impl ObjectStore for FsObjectStore {
    fn backend(&self) -> &'static str {
        "filesystem"
    }

    async fn put(&self, request: PutObject) -> Result<ObjectMeta, ObjectError> {
        self.blocking(move |s| s.put(request)).await
    }

    async fn get(&self, scope: &ObjectScope, id: &ObjectId) -> Result<StoredObject, ObjectError> {
        let (scope, id) = (scope.clone(), id.clone());
        self.blocking(move |s| s.get(&scope, &id)).await
    }

    async fn head(&self, scope: &ObjectScope, id: &ObjectId) -> Result<ObjectMeta, ObjectError> {
        let (scope, id) = (scope.clone(), id.clone());
        self.blocking(move |s| s.scoped_meta(&scope, &id)).await
    }

    async fn delete(&self, scope: &ObjectScope, id: &ObjectId) -> Result<bool, ObjectError> {
        let (scope, id) = (scope.clone(), id.clone());
        self.blocking(move |s| s.delete(&scope, &id)).await
    }

    async fn usage(&self, tenant: &TenantId) -> Result<u64, ObjectError> {
        let tenant = tenant.clone();
        self.blocking(move |s| s.usage(&tenant)).await
    }

    async fn list_candidates(
        &self,
        now: Timestamp,
        created_before: Timestamp,
        limit: usize,
    ) -> Result<ObjectListing, ObjectError> {
        self.blocking(move |s| s.list_candidates(now, created_before, limit))
            .await
    }

    async fn remove_incomplete(&self, names: &[String]) -> Result<usize, ObjectError> {
        let names = names.to_vec();
        self.blocking(move |s| s.remove_incomplete(&names)).await
    }
}
