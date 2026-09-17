//! Provider facade: kind, capabilities and a TTL-cached preflight.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;

use tachyon_serverless_api_types::ProviderInfo;
use tachyon_serverless_domain::ProviderKind;
use tachyon_serverless_provider_port::{
    Capabilities, ExecutionProvider, PreflightCheck, PreflightReport,
};

use crate::error::AppError;
use crate::services::pool::PoolPolicy;

pub struct ProviderService {
    provider: Arc<dyn ExecutionProvider>,
    ttl: Duration,
    /// The reuse decision this gateway booted with. It is part of the provider
    /// view because "does this gateway reuse environments, and was that ever
    /// measured" is a property of the (provider, configuration) pair, and an
    /// operator has to be able to read it without guessing from the capability
    /// table (PLT-4633 acceptance 4).
    policy: PoolPolicy,
    cache: Mutex<Option<(Instant, PreflightReport)>>,
    /// Serialises concurrent preflights so the provider is probed once.
    probe: AsyncMutex<()>,
}

impl ProviderService {
    pub fn new(provider: Arc<dyn ExecutionProvider>, ttl: Duration, policy: PoolPolicy) -> Self {
        Self {
            provider,
            ttl,
            policy,
            cache: Mutex::new(None),
            probe: AsyncMutex::new(()),
        }
    }

    /// The reuse decision behind [`ProviderService::info`].
    pub fn reuse_policy(&self) -> &PoolPolicy {
        &self.policy
    }

    pub fn kind(&self) -> ProviderKind {
        self.provider.kind()
    }

    pub fn capabilities(&self) -> Capabilities {
        self.provider.capabilities()
    }

    /// Cached preflight. A failing probe is also cached (for the TTL) so a
    /// broken host is not hammered by readiness checks.
    pub async fn preflight(&self) -> PreflightReport {
        if let Some((at, report)) = self.cache.lock().as_ref()
            && at.elapsed() < self.ttl
        {
            return report.clone();
        }
        let _guard = self.probe.lock().await;
        if let Some((at, report)) = self.cache.lock().as_ref()
            && at.elapsed() < self.ttl
        {
            return report.clone();
        }
        let report = match self.provider.preflight().await {
            Ok(r) => r,
            Err(e) => PreflightReport {
                provider: self.provider.kind().as_str().to_string(),
                ok: false,
                checks: vec![PreflightCheck {
                    name: "preflight".into(),
                    ok: false,
                    detail: e.to_string(),
                }],
            },
        };
        *self.cache.lock() = Some((Instant::now(), report.clone()));
        report
    }

    /// The cached preflight verdict, without probing: `None` when nothing is
    /// cached or the cache is older than the TTL. Used on the invoke path,
    /// which must not wait for a probe (PLT-4636).
    pub fn cached_ok(&self) -> Option<bool> {
        self.cache
            .lock()
            .as_ref()
            .filter(|(at, _)| at.elapsed() < self.ttl)
            .map(|(_, report)| report.ok)
    }

    /// Drop the cached preflight so the next call probes again.
    pub fn invalidate(&self) {
        *self.cache.lock() = None;
    }

    pub async fn is_ready(&self) -> bool {
        self.preflight().await.ok
    }

    pub async fn info(&self) -> Result<ProviderInfo, AppError> {
        let report = self.preflight().await;
        let caps = self.capabilities();
        let reuse = self.policy.info(&caps);
        Ok(ProviderInfo::from_port(&self.kind(), &caps, &report, reuse))
    }
}
