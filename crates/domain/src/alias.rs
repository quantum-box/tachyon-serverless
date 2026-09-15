//! FunctionAlias: a named pointer (e.g. `prod`) to a revision, updated with CAS.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::error::DomainError;
use crate::ids::{AliasName, FunctionId, RevisionId, TenantId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionAlias {
    pub function_id: FunctionId,
    pub tenant_id: TenantId,
    pub name: AliasName,
    pub revision_id: RevisionId,
    /// Incremented on every successful update. Callers pass the expected
    /// generation to detect concurrent updates.
    pub generation: u64,
    /// Previous revision, kept so a rollback can be performed without lookups.
    pub previous_revision_id: Option<RevisionId>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl FunctionAlias {
    pub fn new(
        function_id: FunctionId,
        tenant_id: TenantId,
        name: AliasName,
        revision_id: RevisionId,
        now: Timestamp,
    ) -> Self {
        Self {
            function_id,
            tenant_id,
            name,
            revision_id,
            generation: 1,
            previous_revision_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Point the alias at a new revision. If `expected_generation` is given and
    /// does not match, the update is rejected with `GenerationMismatch`.
    pub fn update(
        &mut self,
        revision_id: RevisionId,
        expected_generation: Option<u64>,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        if let Some(expected) = expected_generation
            && expected != self.generation
        {
            return Err(DomainError::GenerationMismatch {
                expected,
                actual: self.generation,
            });
        }
        if revision_id != self.revision_id {
            self.previous_revision_id = Some(std::mem::replace(&mut self.revision_id, revision_id));
        }
        self.generation += 1;
        self.updated_at = now;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> Timestamp {
        chrono::Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap()
    }

    #[test]
    fn cas_update_and_rollback_pointer() {
        let r1 = RevisionId::generate();
        let r2 = RevisionId::generate();
        let mut a = FunctionAlias::new(
            FunctionId::generate(),
            TenantId::generate(),
            AliasName::default_alias(),
            r1.clone(),
            now(),
        );
        assert_eq!(a.generation, 1);
        assert!(a.update(r2.clone(), Some(7), now()).is_err());
        a.update(r2.clone(), Some(1), now()).unwrap();
        assert_eq!(a.generation, 2);
        assert_eq!(a.revision_id, r2);
        assert_eq!(a.previous_revision_id, Some(r1.clone()));
        // rollback: point back to previous
        let prev = a.previous_revision_id.clone().unwrap();
        a.update(prev, None, now()).unwrap();
        assert_eq!(a.revision_id, r1);
        assert_eq!(a.previous_revision_id, Some(r2));
        assert_eq!(a.generation, 3);
    }
}
