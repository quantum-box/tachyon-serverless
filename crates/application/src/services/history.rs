//! Tenant-scoped read models: invocation history, logs and usage summaries.

use std::sync::Arc;

use tachyon_serverless_domain::{
    BootEvidence, FunctionId, Invocation, InvocationAttempt, InvocationId, InvocationStatus,
};
use tachyon_serverless_provider_port::Principal;

use crate::authz::{ensure_tenant, require_invoke};
use crate::error::AppError;
use crate::local_ports::InMemoryUsageSink;
use crate::repository::{LogQuery, Repositories};

/// An invocation with its attempts and the boot evidence of each attempt's
/// environment (docs/architecture.md §5 item 10).
#[derive(Debug, Clone)]
pub struct InvocationDetail {
    pub invocation: Invocation,
    pub attempts: Vec<(InvocationAttempt, BootEvidence)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSummary {
    pub function_id: FunctionId,
    pub invocations: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub handler_ms_total: u64,
    pub environment_ms_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
}

pub struct HistoryService {
    repos: Repositories,
    usage: Arc<InMemoryUsageSink>,
}

impl HistoryService {
    pub fn new(repos: Repositories, usage: Arc<InMemoryUsageSink>) -> Self {
        Self { repos, usage }
    }

    pub fn get_invocation(
        &self,
        principal: &Principal,
        invocation_id: &InvocationId,
    ) -> Result<InvocationDetail, AppError> {
        require_invoke(principal)?;
        let invocation = self.load_owned(principal, invocation_id)?;
        self.detail(invocation)
    }

    /// Newest first, at most `limit`.
    pub fn list_invocations(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        limit: usize,
    ) -> Result<Vec<InvocationDetail>, AppError> {
        require_invoke(principal)?;
        self.owned_function(principal, function_id)?;
        self.repos
            .invocations
            .list_by_function(function_id, limit)?
            .into_iter()
            .map(|inv| self.detail(inv))
            .collect()
    }

    pub fn usage_summary(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<UsageSummary, AppError> {
        require_invoke(principal)?;
        self.owned_function(principal, function_id)?;
        let invocations = self
            .repos
            .invocations
            .list_by_function(function_id, usize::MAX)?;
        let ids: Vec<InvocationId> = invocations.iter().map(|i| i.id.clone()).collect();
        let totals = self.usage.totals_for(&ids);
        let succeeded = invocations
            .iter()
            .filter(|i| matches!(i.status, InvocationStatus::Succeeded))
            .count() as u64;
        let failed = invocations
            .iter()
            .filter(|i| {
                matches!(
                    i.status,
                    InvocationStatus::Failed { .. }
                        | InvocationStatus::Cancelled
                        | InvocationStatus::OutcomeUnknown { .. }
                )
            })
            .count() as u64;
        Ok(UsageSummary {
            function_id: function_id.clone(),
            invocations: invocations.len() as u64,
            succeeded,
            failed,
            handler_ms_total: totals.handler_ms_total,
            environment_ms_total: totals.environment_ms_total,
            bytes_in_total: totals.bytes_in_total,
            bytes_out_total: totals.bytes_out_total,
        })
    }

    pub(crate) fn detail(&self, invocation: Invocation) -> Result<InvocationDetail, AppError> {
        let attempts = self
            .repos
            .invocations
            .attempts_of(&invocation.id)?
            .into_iter()
            .map(|a| {
                let evidence = self
                    .repos
                    .environments
                    .get(&a.environment_id)
                    .ok()
                    .flatten()
                    .map(|e| e.evidence)
                    .unwrap_or_default();
                (a, evidence)
            })
            .collect();
        Ok(InvocationDetail {
            invocation,
            attempts,
        })
    }

    fn load_owned(
        &self,
        principal: &Principal,
        invocation_id: &InvocationId,
    ) -> Result<Invocation, AppError> {
        let inv = self
            .repos
            .invocations
            .get(invocation_id)?
            .ok_or_else(|| AppError::not_found("invocation not found"))?;
        ensure_tenant(principal, &inv.tenant_id, "invocation")?;
        Ok(inv)
    }

    fn owned_function(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<(), AppError> {
        let function = self
            .repos
            .functions
            .get(function_id)?
            .ok_or_else(|| AppError::not_found("function not found"))?;
        ensure_tenant(principal, &function.tenant_id, "function")
    }
}

pub struct LogService {
    repos: Repositories,
}

impl LogService {
    pub fn new(repos: Repositories) -> Self {
        Self { repos }
    }

    pub fn for_invocation(
        &self,
        principal: &Principal,
        invocation_id: &InvocationId,
    ) -> Result<LogQuery, AppError> {
        require_invoke(principal)?;
        let inv = self
            .repos
            .invocations
            .get(invocation_id)?
            .ok_or_else(|| AppError::not_found("invocation not found"))?;
        ensure_tenant(principal, &inv.tenant_id, "invocation")?;
        // The repository filters by tenant as well (docs/adr/0018).
        Ok(self.repos.logs.query(&inv.tenant_id, invocation_id)?)
    }
}
