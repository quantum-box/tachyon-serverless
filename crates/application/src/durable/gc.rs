//! Object retention and garbage collection (ADR-0008 §「retention / GC」).
//!
//! One pass:
//!
//! 1. list candidates: objects whose `expires_at` passed, objects older than
//!    the orphan grace, and incomplete writes older than the grace;
//! 2. for each object ask the ledger to claim it
//!    ([`ObjectReferenceRepository::claim_for_collection`]): an object that a
//!    non-terminal invocation references is **never** collected, whatever its
//!    TTL; an object that is only old (not expired) is collected only when
//!    nothing ever referenced it;
//! 3. delete the files of what was claimed, then forget the reference rows.
//!    A failure between 2 and 3 leaves a tombstone, and the next pass
//!    finishes the job (the claim answers `Collect` again). The tombstone
//!    outlives the files so that a late attach of the id is refused; it is
//!    purged after `TOMBSTONE_RETENTION_DAYS`.

use std::sync::Arc;

use tachyon_serverless_domain::Clock;
use tachyon_serverless_durable_port::ObjectStore;

use crate::repository::{CollectDecision, CollectReason, ObjectReferenceRepository};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcReport {
    pub examined: usize,
    pub collected_expired: usize,
    pub collected_orphans: usize,
    /// Kept because a non-terminal invocation references them.
    pub kept_in_use: usize,
    /// Old but referenced by a finished invocation: kept until they expire.
    pub kept_referenced: usize,
    pub incomplete_removed: usize,
    pub tombstones_purged: usize,
    pub failed: usize,
}

pub struct ObjectGc {
    store: Arc<dyn ObjectStore>,
    refs: Arc<dyn ObjectReferenceRepository>,
    clock: Arc<dyn Clock>,
    orphan_grace: chrono::Duration,
    batch: usize,
}

impl std::fmt::Debug for ObjectGc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectGc")
            .field("store", &self.store.backend())
            .field("orphan_grace", &self.orphan_grace)
            .field("batch", &self.batch)
            .finish_non_exhaustive()
    }
}

impl ObjectGc {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        refs: Arc<dyn ObjectReferenceRepository>,
        clock: Arc<dyn Clock>,
        orphan_grace: chrono::Duration,
    ) -> Self {
        Self {
            store,
            refs,
            clock,
            orphan_grace,
            batch: 1_000,
        }
    }

    pub async fn run(&self) -> GcReport {
        let mut report = GcReport::default();
        let now = self.clock.now();
        let cutoff = now - self.orphan_grace;
        let listing = match self.store.list_candidates(now, cutoff, self.batch).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "object gc: listing failed");
                report.failed += 1;
                return report;
            }
        };
        for meta in listing.objects {
            report.examined += 1;
            let reason = if meta.expires_at.is_some_and(|t| t <= now) {
                CollectReason::Expired
            } else {
                CollectReason::Orphan
            };
            let decision = match self.refs.claim_for_collection(&meta.reference, reason, now) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(object = %meta.reference.id, error = %e, "object gc: claim failed");
                    report.failed += 1;
                    continue;
                }
            };
            match decision {
                CollectDecision::InUse { .. } => report.kept_in_use += 1,
                CollectDecision::Referenced => report.kept_referenced += 1,
                CollectDecision::Collect => {
                    let scope = &meta.reference.scope;
                    match self.store.delete(scope, &meta.reference.id).await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(object = %meta.reference.id, error = %e, "object gc: delete failed; retried next pass");
                            report.failed += 1;
                            continue;
                        }
                    }
                    if let Err(e) = self.refs.forget_object(&meta.reference.id) {
                        tracing::warn!(object = %meta.reference.id, error = %e, "object gc: forgetting ledger rows failed");
                        report.failed += 1;
                        continue;
                    }
                    match reason {
                        CollectReason::Expired => report.collected_expired += 1,
                        CollectReason::Orphan => report.collected_orphans += 1,
                    }
                }
            }
        }
        match self.refs.purge_tombstones(
            now - chrono::Duration::days(crate::repository::objects::TOMBSTONE_RETENTION_DAYS),
        ) {
            Ok(n) => report.tombstones_purged = n,
            Err(e) => {
                tracing::warn!(error = %e, "object gc: purging tombstones failed");
                report.failed += 1;
            }
        }
        if !listing.incomplete.is_empty() {
            match self.store.remove_incomplete(&listing.incomplete).await {
                Ok(n) => report.incomplete_removed = n,
                Err(e) => {
                    tracing::warn!(error = %e, "object gc: removing incomplete writes failed");
                    report.failed += 1;
                }
            }
        }
        report
    }
}
