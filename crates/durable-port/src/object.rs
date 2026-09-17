//! Object store port for large invocation inputs and outputs (ADR-0008
//! §「object」).
//!
//! Contract:
//!
//! 1. **Scoped references.** An object is named by an opaque [`ObjectId`]
//!    and is only reachable together with the [`ObjectScope`] (tenant,
//!    region) it was written under. `get` / `head` / `delete` with any other
//!    scope answer [`ObjectError::NotFound`], indistinguishable from an id
//!    that never existed (docs/threat-model.md §6-1), and never touch the
//!    object.
//! 2. **Region.** A store serves an explicit set of regions; a put for
//!    another region is refused ([`ObjectError::RegionNotServed`]) rather
//!    than silently stored somewhere else.
//! 3. **Integrity.** The SHA-256 of the plaintext is recorded at put and
//!    verified on every read; a mismatch (or an authentication failure of
//!    the encryption) is [`ObjectError::Integrity`] and no bytes are returned.
//! 4. **Encryption at rest.** Every object records which algorithm and key
//!    id protect it ([`EncryptionRef`]). The key itself never leaves the
//!    store.
//! 5. **Limits.** Per-object size and per-tenant quota are enforced at put
//!    with explicit errors; nothing is partially stored.
//! 6. **TTL.** `expires_at` is metadata. The store never deletes by itself:
//!    collection is the GC's job, which first asks the ledger whether a
//!    non-terminal invocation still references the object.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tachyon_serverless_domain::{Sha256Digest, TenantId, Timestamp};

const ULID_LEN: usize = 26;

fn is_lower_crockford(s: &str) -> bool {
    s.len() == ULID_LEN
        && s.bytes().all(|b| {
            matches!(b, b'0'..=b'9' | b'a'..=b'h' | b'j'..=b'k' | b'm'..=b'n' | b'p'..=b't' | b'v'..=b'z')
        })
}

/// `obj_<26-char lowercase ULID>`. Opaque: it names nothing on its own and
/// is only usable together with its scope.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectId(String);

impl ObjectId {
    pub const PREFIX: &'static str = "obj";

    pub fn generate() -> Self {
        Self(format!(
            "{}_{}",
            Self::PREFIX,
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        ))
    }

    pub fn parse(raw: &str) -> Result<Self, ObjectError> {
        match raw.strip_prefix("obj_") {
            Some(rest) if is_lower_crockford(rest) => Ok(Self(raw.to_string())),
            _ => Err(ObjectError::InvalidReference(format!(
                "object id must be obj_<26-char lowercase ULID>, got {} bytes",
                raw.len()
            ))),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Creation time embedded in the ULID (milliseconds since the epoch).
    pub fn created_at_ms(&self) -> u64 {
        ulid::Ulid::from_string(&self.0[4..].to_ascii_uppercase())
            .map(|u| u.timestamp_ms())
            .unwrap_or(0)
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.0)
    }
}
impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl TryFrom<String> for ObjectId {
    type Error = ObjectError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<ObjectId> for String {
    fn from(value: ObjectId) -> String {
        value.0
    }
}

/// Region label: 1..=32 bytes of `[a-z0-9-]`, alphanumeric at both ends.
/// Safe as a single path component.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Region(String);

impl Region {
    pub fn parse(raw: &str) -> Result<Self, ObjectError> {
        let b = raw.as_bytes();
        let ok = !b.is_empty()
            && b.len() <= 32
            && b[0].is_ascii_alphanumeric()
            && b[b.len() - 1].is_ascii_alphanumeric()
            && b.iter()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-');
        if ok {
            Ok(Self(raw.to_string()))
        } else {
            Err(ObjectError::InvalidReference(format!(
                "region `{raw}` must be 1..=32 bytes of [a-z0-9-]"
            )))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Region({})", self.0)
    }
}
impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl TryFrom<String> for Region {
    type Error = ObjectError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<Region> for String {
    fn from(value: Region) -> String {
        value.0
    }
}

/// The boundary an object lives in.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectScope {
    pub tenant_id: TenantId,
    pub region: Region,
}

/// A complete reference, as it would be stored next to an invocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub id: ObjectId,
    pub scope: ObjectScope,
}

/// Which protection an object is stored under. The key id is a fingerprint,
/// never the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionRef {
    /// `"AES-256-GCM"`.
    pub algorithm: String,
    pub key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub reference: ObjectRef,
    /// Plaintext size.
    pub size_bytes: u64,
    /// SHA-256 of the plaintext, verified on read.
    pub digest: Sha256Digest,
    pub created_at: Timestamp,
    /// After this the object may be collected, unless a non-terminal
    /// invocation references it. `None`: only an orphan sweep collects it.
    pub expires_at: Option<Timestamp>,
    pub encryption: EncryptionRef,
}

#[derive(Clone, PartialEq, Eq)]
pub struct PutObject {
    pub scope: ObjectScope,
    pub bytes: Vec<u8>,
    /// Time to live from now. `None` uses the store's default retention.
    pub ttl: Option<Duration>,
}

impl fmt::Debug for PutObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PutObject")
            .field("scope", &self.scope)
            .field("bytes", &self.bytes.len())
            .field("ttl", &self.ttl)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub meta: ObjectMeta,
    pub bytes: Vec<u8>,
}

impl fmt::Debug for StoredObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredObject")
            .field("meta", &self.meta)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

/// Candidate objects for collection, as listed by the operator side.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectListing {
    pub objects: Vec<ObjectMeta>,
    /// Stored data without committed metadata (an interrupted put), older
    /// than the cutoff. Removed by [`ObjectStore::remove_incomplete`].
    pub incomplete: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectError {
    /// Absent, or not visible from the given scope. Deliberately the same.
    #[error("object not found")]
    NotFound,
    #[error("object too large: {size} bytes (max {max})")]
    TooLarge { size: u64, max: u64 },
    #[error("tenant quota exceeded: {used} bytes used + {requested} requested > {quota}")]
    QuotaExceeded {
        used: u64,
        requested: u64,
        quota: u64,
    },
    #[error("region `{0}` is not served by this object store")]
    RegionNotServed(Region),
    /// Digest mismatch or failed authentication: stored bytes were altered.
    #[error("object integrity check failed: {0}")]
    Integrity(String),
    /// The object is protected by a key this store does not hold.
    #[error("encryption: {0}")]
    Encryption(String),
    #[error("invalid object reference: {0}")]
    InvalidReference(String),
    #[error("object store io: {0}")]
    Io(#[from] std::io::Error),
    #[error("object store backend: {0}")]
    Backend(String),
}

impl ObjectError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::TooLarge { .. } => "object_too_large",
            Self::QuotaExceeded { .. } => "quota_exceeded",
            Self::RegionNotServed(_) => "region_not_served",
            Self::Integrity(_) => "integrity",
            Self::Encryption(_) => "encryption",
            Self::InvalidReference(_) => "invalid_reference",
            Self::Io(_) => "io",
            Self::Backend(_) => "backend",
        }
    }
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// `"filesystem"`.
    fn backend(&self) -> &'static str;

    async fn put(&self, request: PutObject) -> Result<ObjectMeta, ObjectError>;

    /// Decrypt and verify. Never returns bytes that fail verification.
    async fn get(&self, scope: &ObjectScope, id: &ObjectId) -> Result<StoredObject, ObjectError>;

    async fn head(&self, scope: &ObjectScope, id: &ObjectId) -> Result<ObjectMeta, ObjectError>;

    /// `Ok(false)` when there was nothing to delete in that scope.
    async fn delete(&self, scope: &ObjectScope, id: &ObjectId) -> Result<bool, ObjectError>;

    /// Bytes currently stored for `tenant` (all regions of this store).
    async fn usage(&self, tenant: &TenantId) -> Result<u64, ObjectError>;

    /// Operator listing for the GC: every object that expired at `now`, or
    /// that was created before `created_before`, plus incomplete writes older
    /// than `created_before`. At most `limit` objects.
    async fn list_candidates(
        &self,
        now: Timestamp,
        created_before: Timestamp,
        limit: usize,
    ) -> Result<ObjectListing, ObjectError>;

    /// Remove incomplete writes named by [`ObjectListing::incomplete`].
    async fn remove_incomplete(&self, names: &[String]) -> Result<usize, ObjectError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_regions_cannot_traverse_paths() {
        let id = ObjectId::generate();
        assert_eq!(ObjectId::parse(id.as_str()).unwrap(), id);
        for bad in [
            "",
            "obj_",
            "obj_../../etc/passwd",
            "fn_01hzzzzzzzzzzzzzzzzzzzzzza",
            "obj_01HZZZZZZZZZZZZZZZZZZZZZZA",
            "obj_01hzzzzzzzzzzzzzzzzzzzzzza/x",
        ] {
            assert!(ObjectId::parse(bad).is_err(), "{bad:?}");
        }
        for bad in ["", "..", "a/b", "A", "-a", &"a".repeat(33)] {
            assert!(Region::parse(bad).is_err(), "{bad:?}");
        }
        assert!(Region::parse("local-1").is_ok());
    }
}
