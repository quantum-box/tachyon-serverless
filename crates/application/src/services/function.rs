//! Function CRUD.

use std::sync::Arc;

use tachyon_serverless_domain::{Clock, Function, FunctionId, FunctionName, IdGenerator};
use tachyon_serverless_provider_port::Principal;

use crate::authz::{ensure_tenant, require_deploy, require_read};
use crate::error::AppError;
use crate::repository::Repositories;

pub struct FunctionService {
    repos: Repositories,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
}

impl FunctionService {
    pub fn new(repos: Repositories, clock: Arc<dyn Clock>, ids: Arc<dyn IdGenerator>) -> Self {
        Self { repos, clock, ids }
    }

    pub fn create(
        &self,
        principal: &Principal,
        name: &str,
        description: &str,
    ) -> Result<Function, AppError> {
        require_deploy(principal)?;
        let name = FunctionName::parse(name)?;
        let function = Function::new(
            FunctionId::from_ulid(self.ids.next_ulid()),
            principal.tenant_id.clone(),
            name,
            description.to_string(),
            self.clock.now(),
        )?;
        self.repos.functions.insert(function.clone())?;
        Ok(function)
    }

    /// Tenant-scoped lookup: foreign or unknown ids are `NotFound`.
    pub fn get(&self, principal: &Principal, id: &FunctionId) -> Result<Function, AppError> {
        require_read(principal)?;
        self.load_owned(principal, id)
    }

    pub fn list(&self, principal: &Principal) -> Result<Vec<Function>, AppError> {
        require_read(principal)?;
        Ok(self.repos.functions.list(&principal.tenant_id)?)
    }

    /// Soft delete: the function stops accepting invocations.
    pub fn delete(&self, principal: &Principal, id: &FunctionId) -> Result<Function, AppError> {
        require_deploy(principal)?;
        let mut function = self.load_owned(principal, id)?;
        if function.is_deleted() {
            return Ok(function);
        }
        function.mark_deleted(self.clock.now())?;
        self.repos.functions.update(function.clone())?;
        Ok(function)
    }

    pub(crate) fn load_owned(
        &self,
        principal: &Principal,
        id: &FunctionId,
    ) -> Result<Function, AppError> {
        let function = self
            .repos
            .functions
            .get(id)?
            .ok_or_else(|| AppError::not_found("function not found"))?;
        ensure_tenant(principal, &function.tenant_id, "function")?;
        Ok(function)
    }
}
