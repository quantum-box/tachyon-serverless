//! Ledger side of object retention (PLT-4638, ADR-0008 §「retention / GC」).
//!
//! The object store never decides on its own whether an object may go. The
//! ledger records which invocation references which object, and the GC asks
//! it — inside one store mutation — before deleting anything:
//!
//! - [`ObjectReferenceRepository::attach_object`] records a reference. It is
//!   refused when the invocation belongs to another tenant than the object,
//!   and when the object has already been claimed for collection (a
//!   tombstone exists). So "put, then insert the invocation that uses it"
//!   can never end with an invocation pointing at a deleted object: either
//!   the attach commits first and the GC sees a live reference, or the GC's
//!   tombstone commits first and the attach fails.
//! - [`ObjectReferenceRepository::claim_for_collection`] writes the
//!   tombstone only when no **non-terminal** invocation references the
//!   object and, for an orphan sweep, when nothing references it at all.
//! - [`ObjectReferenceRepository::forget_object`] drops the reference rows
//!   after the files are gone and keeps the tombstone, so a late attach of
//!   the collected id is still refused. Tombstones are purged after
//!   [`TOMBSTONE_RETENTION_DAYS`]; an attach of an object whose id (a ULID)
//!   is older than that is refused outright, so purging a tombstone can
//!   never re-open the race.

use tachyon_serverless_domain::{InvocationId, Timestamp};
use tachyon_serverless_durable_port::{ObjectId, ObjectRef};

use super::RepoError;

/// How long a collection tombstone is kept, and the maximum age of an object
/// id that can still be attached to an invocation.
pub const TOMBSTONE_RETENTION_DAYS: i64 = 30;

/// Refuse attaching an object whose id is older than the tombstone retention.
pub(crate) fn check_attachable(object: &ObjectId, now: Timestamp) -> Result<(), RepoError> {
    let limit = now - chrono::Duration::days(TOMBSTONE_RETENTION_DAYS);
    if (object.created_at_ms() as i64) < limit.timestamp_millis() {
        return Err(RepoError::Refused(format!(
            "object {object} is older than {TOMBSTONE_RETENTION_DAYS} days and can no longer be \
             attached; store it again"
        )));
    }
    Ok(())
}

/// Why the GC wants an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectReason {
    /// `expires_at` has passed.
    Expired,
    /// Older than the orphan grace period and never referenced.
    Orphan,
}

impl CollectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Orphan => "orphan",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectDecision {
    /// Tombstoned (now or by an earlier, unfinished run): delete the files,
    /// then call `forget_object`.
    Collect,
    /// A non-terminal invocation references it: keep.
    InUse { invocations: usize },
    /// Orphan sweep only: some (terminal) invocation references it, so it is
    /// not an orphan; its TTL decides.
    Referenced,
}

pub trait ObjectReferenceRepository: Send + Sync {
    /// Record that `invocation` uses `object`. Idempotent.
    ///
    /// `NotFound` when the invocation does not exist, `Refused` when it
    /// belongs to another tenant, when the object is being (or was)
    /// collected, or when its id is older than [`TOMBSTONE_RETENTION_DAYS`].
    fn attach_object(
        &self,
        object: &ObjectRef,
        invocation: &InvocationId,
        now: Timestamp,
    ) -> Result<(), RepoError>;

    /// Invocations that reference `object` (any status).
    fn object_references(&self, object: &ObjectId) -> Result<Vec<InvocationId>, RepoError>;

    /// Atomically decide and, for [`CollectDecision::Collect`], tombstone.
    fn claim_for_collection(
        &self,
        object: &ObjectRef,
        reason: CollectReason,
        now: Timestamp,
    ) -> Result<CollectDecision, RepoError>;

    /// Remove every reference row of a collected object (its tombstone stays).
    fn forget_object(&self, object: &ObjectId) -> Result<(), RepoError>;

    /// Delete tombstones older than `before`. Returns how many.
    fn purge_tombstones(&self, before: Timestamp) -> Result<usize, RepoError>;
}
