//! Alias management with compare-and-set updates and rollback.

use std::sync::Arc;

use tachyon_serverless_domain::{
    AliasName, Clock, Function, FunctionAlias, FunctionId, RevisionId,
};
use tachyon_serverless_provider_port::Principal;

use crate::authz::{ensure_tenant, require_deploy, require_read};
use crate::error::AppError;
use crate::repository::Repositories;

pub struct AliasService {
    repos: Repositories,
    clock: Arc<dyn Clock>,
}

impl AliasService {
    pub fn new(repos: Repositories, clock: Arc<dyn Clock>) -> Self {
        Self { repos, clock }
    }

    pub fn get(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        name: &AliasName,
    ) -> Result<FunctionAlias, AppError> {
        require_read(principal)?;
        self.owned_function(principal, function_id)?;
        self.repos
            .aliases
            .get(function_id, name)?
            .ok_or_else(|| AppError::not_found(format!("alias `{name}` not found")))
    }

    pub fn list(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<Vec<FunctionAlias>, AppError> {
        require_read(principal)?;
        self.owned_function(principal, function_id)?;
        Ok(self.repos.aliases.list(function_id)?)
    }

    /// Point `name` at `revision_id`. With `expected_generation` the update
    /// only applies when the alias is currently at that generation
    /// (`Conflict` otherwise). Creates the alias when it does not exist and
    /// no generation was expected.
    pub fn update(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        name: &AliasName,
        revision_id: &RevisionId,
        expected_generation: Option<u64>,
    ) -> Result<FunctionAlias, AppError> {
        require_deploy(principal)?;
        let function = self.owned_function(principal, function_id)?;
        let revision = self
            .repos
            .revisions
            .get(revision_id)?
            .filter(|r| r.function_id == function.id && r.tenant_id == principal.tenant_id)
            .ok_or_else(|| AppError::not_found("revision not found"))?;
        if !revision.is_ready() {
            return Err(AppError::RevisionNotReady(format!(
                "revision {} is {}",
                revision.id,
                revision.status.name()
            )));
        }
        self.apply(&function, name, revision_id, expected_generation)
    }

    /// Roll back to the previous revision recorded on the alias, using the
    /// current generation as the CAS guard.
    pub fn rollback(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        name: &AliasName,
    ) -> Result<FunctionAlias, AppError> {
        require_deploy(principal)?;
        let function = self.owned_function(principal, function_id)?;
        let current = self
            .repos
            .aliases
            .get(function_id, name)?
            .ok_or_else(|| AppError::not_found(format!("alias `{name}` not found")))?;
        let previous = current.previous_revision_id.clone().ok_or_else(|| {
            AppError::Conflict(format!("alias `{name}` has no previous revision"))
        })?;
        self.apply(&function, name, &previous, Some(current.generation))
    }

    /// Internal publish (no principal): used when a revision becomes Ready
    /// with `publish_to_prod`. Never CAS-guarded: last Ready revision wins.
    pub(crate) fn publish(
        &self,
        function: &Function,
        name: &AliasName,
        revision_id: &RevisionId,
    ) -> Result<FunctionAlias, AppError> {
        self.apply(function, name, revision_id, None)
    }

    fn apply(
        &self,
        function: &Function,
        name: &AliasName,
        revision_id: &RevisionId,
        expected_generation: Option<u64>,
    ) -> Result<FunctionAlias, AppError> {
        let now = self.clock.now();
        let result = self.repos.aliases.modify(&function.id, name, &mut |slot| {
            match slot {
                Some(alias) => alias.update(revision_id.clone(), expected_generation, now)?,
                None => {
                    if let Some(expected) = expected_generation {
                        return Err(AppError::Conflict(format!(
                            "alias `{name}` does not exist (expected generation {expected})"
                        )));
                    }
                    *slot = Some(FunctionAlias::new(
                        function.id.clone(),
                        function.tenant_id.clone(),
                        name.clone(),
                        revision_id.clone(),
                        now,
                    ));
                }
            }
            Ok(())
        })?;
        result.ok_or_else(|| AppError::platform("alias vanished during update"))
    }

    fn owned_function(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<Function, AppError> {
        let function = self
            .repos
            .functions
            .get(function_id)?
            .ok_or_else(|| AppError::not_found("function not found"))?;
        ensure_tenant(principal, &function.tenant_id, "function")?;
        Ok(function)
    }
}
