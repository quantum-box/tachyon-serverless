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

pub struct ProviderService {
    provider: Arc<dyn ExecutionProvider>,
    ttl: Duration,
    cache: Mutex<Option<(Instant, PreflightReport)>>,
    /// Serialises concurrent preflights so the provider is probed once.
    probe: AsyncMutex<()>,
}

impl ProviderService {
    pub fn new(provider: Arc<dyn ExecutionProvider>, ttl: Duration) -> Self {
        Self {
            provider,
            ttl,
            cache: Mutex::new(None),
            probe: AsyncMutex::new(()),
        }
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

    /// Drop the cached preflight so the next call probes again.
    pub fn invalidate(&self) {
        *self.cache.lock() = None;
    }

    pub async fn is_ready(&self) -> bool {
        self.preflight().await.ok
    }

    pub async fn info(&self) -> Result<ProviderInfo, AppError> {
        let report = self.preflight().await;
        Ok(ProviderInfo::from_port(
            &self.kind(),
            &self.capabilities(),
            &report,
        ))
    }
}
