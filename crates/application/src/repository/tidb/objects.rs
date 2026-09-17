//! [`ObjectReferenceRepository`] on TiDB (migration 005).
//!
//! An attach and a collection claim of the same object must be serialized
//! (the SQLite store gets that from `BEGIN IMMEDIATE`). At READ-COMMITTED a
//! `FOR UPDATE` on the tombstone key takes no lock while no tombstone row
//! exists, so both sides **insert** the tombstone key first instead: an
//! uncommitted insert holds the key's lock and a second insert of it waits.
//!
//! - claim: insert the tombstone, then read the references (the read sees
//!   an attach that committed while the insert waited). If the object is in
//!   use or referenced, delete the tombstone again before committing.
//! - attach: insert a provisional `attaching` tombstone, insert the
//!   reference, delete the provisional row, commit. A duplicate key means a
//!   real tombstone is committed: the attach is refused.
//!
//! So "attach commits first, the claim sees the reference" or "the tombstone
//! commits first, the attach is refused" are the only outcomes.

use mysql::prelude::Queryable;

use tachyon_serverless_domain::{InvocationId, Timestamp};
use tachyon_serverless_durable_port::{ObjectId, ObjectRef};

use super::super::objects::{
    CollectDecision, CollectReason, ObjectReferenceRepository, check_attachable,
};
use super::{RepoError, TidbStore, exec_count, get_invocation, p, ts};

const PROVISIONAL: &str = "attaching";

fn tombstone_reason<Q: Queryable>(
    c: &mut Q,
    object: &ObjectId,
) -> Result<Option<String>, RepoError> {
    Ok(c.exec_first(
        "SELECT reason FROM object_tombstones WHERE object_id = ?",
        p![object.as_str()],
    )?)
}

fn insert_tombstone<Q: Queryable>(
    c: &mut Q,
    object: &ObjectRef,
    reason: &str,
    now: Timestamp,
) -> Result<(), RepoError> {
    c.exec_drop(
        "INSERT INTO object_tombstones (object_id, tenant_id, region, reason, tombstoned_at) \
         VALUES (?, ?, ?, ?, ?)",
        p![
            object.id.as_str(),
            object.scope.tenant_id.as_str(),
            object.scope.region.as_str(),
            reason,
            ts(&now)
        ],
    )?;
    Ok(())
}

fn remove_tombstone<Q: Queryable>(c: &mut Q, object: &ObjectId) -> Result<(), RepoError> {
    c.exec_drop(
        "DELETE FROM object_tombstones WHERE object_id = ?",
        p![object.as_str()],
    )?;
    Ok(())
}

impl ObjectReferenceRepository for TidbStore {
    fn attach_object(
        &self,
        object: &ObjectRef,
        invocation: &InvocationId,
        now: Timestamp,
    ) -> Result<(), RepoError> {
        check_attachable(&object.id, now)?;
        self.write(|tx| {
            let inv = get_invocation(tx, invocation, false)?
                .ok_or_else(|| RepoError::NotFound(format!("invocation {invocation}")))?;
            if inv.tenant_id != object.scope.tenant_id {
                return Err(RepoError::Refused(format!(
                    "object {} belongs to another tenant than invocation {invocation}",
                    object.id
                )));
            }
            match insert_tombstone(tx, object, PROVISIONAL, now) {
                Ok(()) => {}
                Err(RepoError::Conflict(_)) => {
                    let reason = tombstone_reason(tx, &object.id)?.unwrap_or_default();
                    return Err(RepoError::Refused(format!(
                        "object {} is being collected ({reason}); store it again",
                        object.id
                    )));
                }
                Err(other) => return Err(other),
            }
            tx.exec_drop(
                "INSERT IGNORE INTO object_refs \
                 (object_id, tenant_id, region, invocation_id, attached_at) \
                 VALUES (?, ?, ?, ?, ?)",
                p![
                    object.id.as_str(),
                    object.scope.tenant_id.as_str(),
                    object.scope.region.as_str(),
                    invocation.as_str(),
                    ts(&now)
                ],
            )?;
            remove_tombstone(tx, &object.id)
        })
    }

    fn object_references(&self, object: &ObjectId) -> Result<Vec<InvocationId>, RepoError> {
        self.read(|c| {
            let ids: Vec<String> = c.exec(
                "SELECT invocation_id FROM object_refs WHERE object_id = ? ORDER BY invocation_id",
                p![object.as_str()],
            )?;
            ids.iter()
                .map(|s| {
                    InvocationId::parse(s).map_err(|e| RepoError::Serialization(e.to_string()))
                })
                .collect()
        })
    }

    fn claim_for_collection(
        &self,
        object: &ObjectRef,
        reason: CollectReason,
        now: Timestamp,
    ) -> Result<CollectDecision, RepoError> {
        self.write(|tx| {
            if tombstone_reason(tx, &object.id)?.is_some() {
                return Ok(CollectDecision::Collect);
            }
            match insert_tombstone(tx, object, reason.as_str(), now) {
                Ok(()) => {}
                // Another claim committed its tombstone while this one waited.
                Err(RepoError::Conflict(_)) => return Ok(CollectDecision::Collect),
                Err(other) => return Err(other),
            }
            // A reference whose invocation row is missing protects the object
            // too: the ledger cannot prove that invocation finished. An open
            // dead letter keeps its input for a redrive (PLT-4640).
            let refs: Vec<(String, Option<i64>, i64)> = tx.exec(
                "SELECT r.invocation_id, i.terminal, \
                 EXISTS (SELECT 1 FROM dead_letters d \
                         WHERE d.invocation_id = r.invocation_id AND d.status = 'open') \
                 FROM object_refs r LEFT JOIN invocations i ON i.id = r.invocation_id \
                 WHERE r.object_id = ?",
                p![object.id.as_str()],
            )?;
            let live = refs
                .iter()
                .filter(|(_, terminal, dead_letter)| *terminal != Some(1) || *dead_letter == 1)
                .count();
            if live > 0 {
                remove_tombstone(tx, &object.id)?;
                return Ok(CollectDecision::InUse { invocations: live });
            }
            if reason == CollectReason::Orphan && !refs.is_empty() {
                remove_tombstone(tx, &object.id)?;
                return Ok(CollectDecision::Referenced);
            }
            Ok(CollectDecision::Collect)
        })
    }

    fn forget_object(&self, object: &ObjectId) -> Result<(), RepoError> {
        self.write(|tx| {
            tx.exec_drop(
                "DELETE FROM object_refs WHERE object_id = ?",
                p![object.as_str()],
            )?;
            Ok(())
        })
    }

    fn purge_tombstones(&self, before: Timestamp) -> Result<usize, RepoError> {
        self.write(|tx| {
            Ok(exec_count(
                tx,
                "DELETE FROM object_tombstones WHERE tombstoned_at < ? AND reason != 'attaching'",
                p![ts(&before)],
            )? as usize)
        })
    }
}
