//! [`ObjectReferenceRepository`] on `state.db` (migration 005). Every write
//! is one `BEGIN IMMEDIATE` transaction, so an attach and a collection claim
//! for the same object are serialized, also across processes.

use rusqlite::{OptionalExtension, params};

use tachyon_serverless_domain::{InvocationId, Timestamp};
use tachyon_serverless_durable_port::{ObjectId, ObjectRef};

use super::super::objects::{
    CollectDecision, CollectReason, ObjectReferenceRepository, check_attachable,
};
use super::{RepoError, SqliteStore, get_invocation, ts};

/// The attach inside an open transaction (also used by the asynchronous
/// acceptance, which attaches in the same transaction as the invocation
/// insert). The caller has run [`check_attachable`].
pub(super) fn attach_in(
    tx: &rusqlite::Connection,
    object: &ObjectRef,
    invocation: &InvocationId,
    now: Timestamp,
) -> Result<(), RepoError> {
    let inv = get_invocation(tx, invocation)?
        .ok_or_else(|| RepoError::NotFound(format!("invocation {invocation}")))?;
    if inv.tenant_id != object.scope.tenant_id {
        return Err(RepoError::Refused(format!(
            "object {} belongs to another tenant than invocation {invocation}",
            object.id
        )));
    }
    let tombstoned: Option<String> = tx
        .query_row(
            "SELECT reason FROM object_tombstones WHERE object_id = ?1",
            [object.id.as_str()],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(reason) = tombstoned {
        return Err(RepoError::Refused(format!(
            "object {} is being collected ({reason}); store it again",
            object.id
        )));
    }
    tx.execute(
        "INSERT OR IGNORE INTO object_refs (object_id, tenant_id, region, invocation_id, attached_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            object.id.as_str(),
            object.scope.tenant_id.as_str(),
            object.scope.region.as_str(),
            invocation.as_str(),
            ts(&now)
        ],
    )?;
    Ok(())
}

impl ObjectReferenceRepository for SqliteStore {
    fn attach_object(
        &self,
        object: &ObjectRef,
        invocation: &InvocationId,
        now: Timestamp,
    ) -> Result<(), RepoError> {
        check_attachable(&object.id, now)?;
        self.write(|tx| attach_in(tx, object, invocation, now))
    }

    fn object_references(&self, object: &ObjectId) -> Result<Vec<InvocationId>, RepoError> {
        self.read(|c| {
            let mut stmt = c.prepare(
                "SELECT invocation_id FROM object_refs WHERE object_id = ?1 ORDER BY invocation_id",
            )?;
            let ids = stmt
                .query_map([object.as_str()], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
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
            let already: Option<String> = tx
                .query_row(
                    "SELECT reason FROM object_tombstones WHERE object_id = ?1",
                    [object.id.as_str()],
                    |r| r.get(0),
                )
                .optional()?;
            if already.is_some() {
                return Ok(CollectDecision::Collect);
            }
            // A reference whose invocation row is missing protects the object
            // too: the ledger cannot prove that invocation finished.
            let live: i64 = tx.query_row(
                "SELECT COUNT(*) FROM object_refs r
                 LEFT JOIN invocations i ON i.id = r.invocation_id
                 WHERE r.object_id = ?1 AND (i.id IS NULL OR i.terminal = 0)",
                [object.id.as_str()],
                |r| r.get(0),
            )?;
            if live > 0 {
                return Ok(CollectDecision::InUse {
                    invocations: live as usize,
                });
            }
            if reason == CollectReason::Orphan {
                let any: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM object_refs WHERE object_id = ?1",
                    [object.id.as_str()],
                    |r| r.get(0),
                )?;
                if any > 0 {
                    return Ok(CollectDecision::Referenced);
                }
            }
            tx.execute(
                "INSERT INTO object_tombstones (object_id, tenant_id, region, reason, tombstoned_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    object.id.as_str(),
                    object.scope.tenant_id.as_str(),
                    object.scope.region.as_str(),
                    reason.as_str(),
                    ts(&now)
                ],
            )?;
            Ok(CollectDecision::Collect)
        })
    }

    fn forget_object(&self, object: &ObjectId) -> Result<(), RepoError> {
        self.write(|tx| {
            tx.execute(
                "DELETE FROM object_refs WHERE object_id = ?1",
                [object.as_str()],
            )?;
            Ok(())
        })
    }

    fn purge_tombstones(&self, before: Timestamp) -> Result<usize, RepoError> {
        self.write(|tx| {
            Ok(tx.execute(
                "DELETE FROM object_tombstones WHERE tombstoned_at < ?1",
                [ts(&before)],
            )?)
        })
    }
}
