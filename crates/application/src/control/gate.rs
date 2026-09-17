//! What a gateway still accepts and starts, given its configuration cache,
//! its control plane and its provider (PLT-4636).
//!
//! Two different things are restricted, and they are reported apart:
//!
//! - **New invocations** need a valid authorization lease and valid delivered
//!   configuration (function, route, revision, policy). Without them the
//!   gateway refuses at acceptance, before anything is recorded.
//! - **New cold starts** additionally need the revision and the tenant's
//!   authorization to be valid *when the environment is booted* (acceptance
//!   may have queued), a provider whose control API answers (its preflight),
//!   and, while the control plane is unreachable, `[control_plane_outage]
//!   allow_cold_start = true`.
//!
//! **Existing executions always continue**: an invocation that is already
//! running is never cut short because the control plane went away or an
//! entry expired, and a pooled (warm) environment keeps serving new
//! invocations for as long as those are accepted.

use std::sync::Arc;

use serde::Serialize;

use tachyon_serverless_domain::{
    AliasName, ErrorClass, FunctionId, FunctionRevision, InvocationError, RevisionId, TenantId,
};
use tachyon_serverless_provider_port::Principal;

use super::ControlError;
use super::cache::{CacheState, CacheStatus, ConfigCache, Resolved};
use crate::config::GatewayRole;
use crate::error::AppError;
use crate::services::ProviderService;

pub struct InvokeGate {
    cache: Arc<ConfigCache>,
    provider: Arc<ProviderService>,
    role: GatewayRole,
    allow_cold_start_during_outage: bool,
}

/// The gate as `/readyz` reports it.
#[derive(Debug, Clone, Serialize)]
pub struct InvokeView {
    pub role: &'static str,
    /// `unknown` | `fresh` | `stale_but_valid` | `expired`.
    pub config_state: CacheState,
    /// The control plane answered the last refresh.
    pub control_plane_reachable: bool,
    /// Always `continue`: running invocations are never cut short.
    pub existing_executions: &'static str,
    /// `accepted` | `refused`.
    pub new_invocations: &'static str,
    /// `allowed` | `refused`.
    pub new_cold_starts: &'static str,
    /// `error_type` of the refusal, when something is refused.
    pub refusal: Option<&'static str>,
    pub reason: Option<String>,
    pub allow_cold_start_during_outage: bool,
    pub cache: CacheStatus,
}

impl InvokeGate {
    pub fn new(
        cache: Arc<ConfigCache>,
        provider: Arc<ProviderService>,
        role: GatewayRole,
        allow_cold_start_during_outage: bool,
    ) -> Self {
        Self {
            cache,
            provider,
            role,
            allow_cold_start_during_outage,
        }
    }

    pub fn cache(&self) -> &Arc<ConfigCache> {
        &self.cache
    }

    /// Resolve a new invocation from the cache. When environment reuse is off
    /// every invocation needs a cold start, so a cold-start refusal is
    /// answered here, before anything is recorded.
    pub async fn resolve(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        alias: Option<&AliasName>,
        revision_id: Option<&RevisionId>,
        reuse_enabled: bool,
    ) -> Result<Resolved, AppError> {
        let resolved = self
            .cache
            .resolve(principal, function_id, alias, revision_id)
            .await?;
        if !reuse_enabled {
            self.permit_cold_start(&resolved.revision, &resolved.function.tenant_id)
                .map_err(|(kind, message)| AppError::control(kind, message))?;
        }
        Ok(resolved)
    }

    /// May a new environment be booted for `revision` now?
    pub fn permit_cold_start(
        &self,
        revision: &FunctionRevision,
        tenant: &TenantId,
    ) -> Result<(), (ControlError, String)> {
        if let Err(kind) = self.cache.start_still_valid(&revision.id, tenant) {
            let message = match kind {
                ControlError::AuthLeaseExpired => {
                    "the tenant's authorization lease expired before the environment could start"
                        .to_string()
                }
                _ => format!(
                    "revision {} is no longer vouched for by the control plane",
                    revision.id
                ),
            };
            return Err((kind, message));
        }
        if !self.allow_cold_start_during_outage && self.cache.outage() {
            return Err((
                ControlError::ColdStartRestricted,
                "the control plane is unreachable and [control_plane_outage] allow_cold_start = \
                 false: running environments keep serving, new environments are not started"
                    .to_string(),
            ));
        }
        if self.provider.cached_ok() == Some(false) {
            return Err((
                ControlError::ProviderControlUnavailable,
                "the provider's control API is failing its preflight: running environments \
                 keep serving, new environments are not started"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// The refusal as an invocation error, for a cold start refused after
    /// acceptance.
    pub fn invocation_error(kind: ControlError, message: String) -> InvocationError {
        InvocationError::new(ErrorClass::PlatformError, kind.error_type(), message)
    }

    pub async fn view(&self, reuse_enabled: bool) -> InvokeView {
        self.cache.sync_if_authoritative().await;
        let state = self.cache.state();
        let status = self.cache.status();
        let (new_invocations, mut refusal, mut reason) = match state {
            CacheState::Fresh | CacheState::StaleButValid => ("accepted", None, None),
            CacheState::Unknown => (
                "refused",
                Some(ControlError::ConfigNotDelivered.error_type()),
                Some("no configuration has been delivered yet".to_string()),
            ),
            CacheState::Expired => {
                let now = self.cache.now();
                let auth_expired = status.auth_valid_until.is_some_and(|t| t <= now);
                let kind = if auth_expired {
                    ControlError::AuthLeaseExpired
                } else {
                    ControlError::ConfigExpired
                };
                (
                    "refused",
                    Some(kind.error_type()),
                    Some(format!(
                        "the delivered configuration was not confirmed in time (last success {})",
                        status
                            .last_success_at
                            .map_or_else(|| "never".to_string(), |t| t.to_rfc3339())
                    )),
                )
            }
        };
        let mut new_cold_starts = if new_invocations == "accepted" {
            "allowed"
        } else {
            "refused"
        };
        if new_cold_starts == "allowed" {
            let outage_refusal = !self.allow_cold_start_during_outage && self.cache.outage();
            let provider_refusal = self.provider.cached_ok() == Some(false);
            if outage_refusal || provider_refusal {
                let kind = if outage_refusal {
                    ControlError::ColdStartRestricted
                } else {
                    ControlError::ProviderControlUnavailable
                };
                new_cold_starts = "refused";
                refusal = Some(kind.error_type());
                reason = Some(if outage_refusal {
                    "control plane unreachable and [control_plane_outage] allow_cold_start = false"
                        .to_string()
                } else {
                    "provider preflight failing".to_string()
                });
            }
        }
        // Without reuse every invocation is a cold start.
        let new_invocations = if new_cold_starts == "refused" && !reuse_enabled {
            "refused"
        } else {
            new_invocations
        };
        InvokeView {
            role: self.role.as_str(),
            config_state: state,
            control_plane_reachable: !self.cache.outage(),
            existing_executions: "continue",
            new_invocations,
            new_cold_starts,
            refusal,
            reason,
            allow_cold_start_during_outage: self.allow_cold_start_during_outage,
            cache: status,
        }
    }
}
