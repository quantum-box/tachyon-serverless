//! Composition root: wires configuration, ports and services together.

use std::path::PathBuf;
use std::sync::Arc;

use tachyon_serverless_domain::{Clock, IdGenerator, Limits, SystemClock, UlidGenerator};
use tachyon_serverless_provider_port::{
    ArtifactStore, ExecutionProvider, IdentityProvider, SecretProvider, UsageSink,
};

use crate::config::{GatewayConfig, Profile, ProviderConfig, StoreBackend};
use crate::entrypoint::EntrypointPolicy;
use crate::error::AppError;
use crate::local_ports::{
    InMemoryUsageSink, LocalArtifactStore, StaticIdentityProvider, StaticSecretProvider,
};
use crate::repository::{InMemoryStore, Repositories, SqliteOptions, SqliteStore, StateStore};
use crate::services::invoke::InvokeServiceDeps;
use crate::services::{
    AliasService, ArtifactService, EnvironmentPool, FunctionService, HistoryService, InvokeService,
    LogService, PoolPolicy, PoolSweep, ProviderService, ReconcileReport, ReconcileService,
    RevisionService,
};

/// Builds the execution provider selected by configuration. The gateway
/// implements this (the application crate must not depend on concrete
/// providers); tests pass a fake provider directly to [`Application::bootstrap`].
pub trait ProviderFactory: Send + Sync {
    fn build(&self, config: &ProviderConfig) -> Result<Arc<dyn ExecutionProvider>, AppError>;
}

/// Everything the gateway needs, fully wired.
pub struct Application {
    pub config: Arc<GatewayConfig>,
    pub limits: Limits,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    /// The ledger: `<data_dir>/state.db` unless `[store] backend = "memory"`
    /// or [`BootstrapOptions::persist_state`] is off.
    pub store: Arc<dyn StateStore>,
    pub repos: Repositories,
    /// Content-addressed store. Tenant-facing uploads must go through
    /// [`Application::artifact_service`], which records ownership.
    pub artifacts: Arc<dyn ArtifactStore>,
    pub artifact_service: Arc<ArtifactService>,
    pub identity: Arc<dyn IdentityProvider>,
    pub secrets: Arc<dyn SecretProvider>,
    pub usage: Arc<InMemoryUsageSink>,
    pub provider: Arc<dyn ExecutionProvider>,
    pub functions: Arc<FunctionService>,
    pub revisions: Arc<RevisionService>,
    pub aliases: Arc<AliasService>,
    pub invoke: Arc<InvokeService>,
    pub logs: Arc<LogService>,
    pub history: Arc<HistoryService>,
    pub provider_service: Arc<ProviderService>,
    pub reconcile: Arc<ReconcileService>,
    /// Warm environment pool. Inert unless both the provider's idle
    /// capabilities and `[pool] enabled` allow reuse.
    pub pool: Arc<EnvironmentPool>,
}

impl std::fmt::Debug for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Application")
            .field("profile", &self.config.profile)
            .field("provider", &self.provider.kind())
            .field("data_dir", &self.config.data_dir)
            .finish_non_exhaustive()
    }
}

/// Options for [`Application::bootstrap_with`].
pub struct BootstrapOptions {
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    /// Keep the ledger in `<data_dir>/state.db` as `[store]` selects. When
    /// false the ledger is volatile whatever `[store]` says. Artifacts are
    /// always on disk.
    pub persist_state: bool,
    /// Secret backend to use instead of the static one built from
    /// `[[secrets.bindings]]`. Tests use it to rotate a value at runtime and
    /// observe that the reuse key's secret generation follows.
    pub secrets: Option<Arc<dyn SecretProvider>>,
}

impl Default for BootstrapOptions {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            ids: Arc::new(UlidGenerator),
            persist_state: true,
            secrets: None,
        }
    }
}

impl Application {
    /// Build the provider through `factory` and bootstrap.
    pub fn bootstrap_with_factory(
        config: GatewayConfig,
        factory: &dyn ProviderFactory,
    ) -> Result<Arc<Self>, AppError> {
        config
            .validate()
            .map_err(|e| AppError::InvalidRequest(e.to_string()))?;
        let provider = factory.build(&config.provider)?;
        Self::bootstrap(config, provider)
    }

    pub fn bootstrap(
        config: GatewayConfig,
        provider: Arc<dyn ExecutionProvider>,
    ) -> Result<Arc<Self>, AppError> {
        Self::bootstrap_with(config, provider, BootstrapOptions::default())
    }

    pub fn bootstrap_with(
        config: GatewayConfig,
        provider: Arc<dyn ExecutionProvider>,
        options: BootstrapOptions,
    ) -> Result<Arc<Self>, AppError> {
        config
            .validate()
            .map_err(|e| AppError::InvalidRequest(e.to_string()))?;
        let caps = provider.capabilities();
        if config.profile == Profile::Production && caps.dev_only {
            return Err(AppError::InvalidRequest(format!(
                "provider `{}` is dev-only and cannot be used with profile = \"production\"",
                provider.kind().as_str()
            )));
        }
        let limits = config.effective_limits();
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            AppError::platform(format!(
                "cannot create data_dir {}: {e}",
                config.data_dir.display()
            ))
        })?;
        let store: Arc<dyn StateStore> =
            if options.persist_state && config.store.backend == StoreBackend::Sqlite {
                let sqlite = SqliteStore::open(
                    &config.data_dir,
                    limits.clone(),
                    SqliteOptions {
                        output_retention: config.store.output_retention(),
                    },
                    options.clock.now(),
                )?;
                let report = sqlite.open_report();
                tracing::info!(
                    path = %config.data_dir.join(SqliteStore::FILE_NAME).display(),
                    schema_version = report.schema_version,
                    migrations_applied = ?report.migrations_applied,
                    imported_state_json = ?report.imported_state_json,
                    settled_invocations = report.settled.invocations,
                    settled_attempts = report.settled.attempts,
                    settled_environments = report.settled.environments,
                    released_leases = report.settled.leases,
                    dropped_idempotency_keys = report.settled.idempotency_dropped,
                    outputs_purged = report.outputs_purged,
                    "state store opened"
                );
                Arc::new(sqlite)
            } else {
                Arc::new(InMemoryStore::new(limits.clone()))
            };
        let repos = Repositories::from_store(store.clone());
        let artifacts: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
            &config.data_dir,
            limits.max_artifact_bytes,
        )?);
        let identity: Arc<dyn IdentityProvider> =
            Arc::new(StaticIdentityProvider::from_config(&config.identity.tokens));
        let secrets: Arc<dyn SecretProvider> = options.secrets.clone().unwrap_or_else(|| {
            Arc::new(StaticSecretProvider::from_config(&config.secrets.bindings))
        });
        let usage = Arc::new(InMemoryUsageSink::new());
        let provider_workdir: PathBuf = config
            .provider
            .workdir()
            .map(PathBuf::from)
            .unwrap_or_else(|| config.data_dir.join(provider.kind().as_str()));
        let entrypoints = EntrypointPolicy::new(provider_workdir);

        let clock = options.clock;
        let ids = options.ids;
        let functions = Arc::new(FunctionService::new(
            repos.clone(),
            clock.clone(),
            ids.clone(),
        ));
        let aliases = Arc::new(AliasService::new(repos.clone(), clock.clone()));
        let artifact_service = Arc::new(ArtifactService::new(repos.clone(), artifacts.clone()));
        let revisions = Arc::new(RevisionService::new(
            repos.clone(),
            artifacts.clone(),
            provider.clone(),
            aliases.clone(),
            clock.clone(),
            ids.clone(),
            limits.clone(),
        ));
        let history = Arc::new(HistoryService::new(repos.clone(), usage.clone()));
        let logs = Arc::new(LogService::new(repos.clone()));
        // Both gates are decided once, here: the provider's idle capabilities
        // and the `[pool]` section. Everything downstream only asks the
        // policy (docs/architecture.md §4).
        let policy = PoolPolicy::decide(&caps, &config.pool);
        let provider_service = Arc::new(ProviderService::new(
            provider.clone(),
            config.invoke.preflight_ttl(),
            policy,
        ));
        let reconcile = Arc::new(ReconcileService::new(
            repos.clone(),
            provider.clone(),
            clock.clone(),
        ));
        // The pool gets the usage sink because it, not the driver, is what
        // ends a pooled environment's life (TTL sweep, drain, retire) and
        // therefore what has to report it (docs/architecture.md §4).
        let pool = Arc::new(EnvironmentPool::new(
            repos.clone(),
            provider.clone(),
            usage.clone() as Arc<dyn UsageSink>,
            clock.clone(),
            policy,
        ));
        let invoke = InvokeService::new(InvokeServiceDeps {
            repos: repos.clone(),
            artifacts: artifacts.clone(),
            secrets: secrets.clone(),
            usage: usage.clone(),
            provider: provider.clone(),
            history: history.clone(),
            clock: clock.clone(),
            ids: ids.clone(),
            limits: limits.clone(),
            capacity: config.capacity.clone(),
            invoke: config.invoke.clone(),
            entrypoints,
            pool: pool.clone(),
        });
        // Reuse is visible at startup, on or off, with the gate that decided
        // it and the two capabilities behind it (PLT-4633 acceptance 4). The
        // same facts are on `GET /v1/provider`.
        tracing::info!(
            profile = config.profile.as_str(),
            provider = provider.kind().as_str(),
            dev_only = caps.dev_only,
            environment_reuse = policy.reuse_enabled(),
            reuse_verified = policy.idle_verified(),
            reuse_reason = policy.reason(),
            reuse_disabled = ?policy.disabled_reason(),
            idle_quiesce = caps.idle_quiesce.status_str(),
            idle_resume = caps.idle_resume.status_str(),
            data_dir = %config.data_dir.display(),
            store = store.backend(),
            "application bootstrapped"
        );
        if policy.reuse_enabled() && !policy.idle_verified() {
            tracing::warn!(
                provider = provider.kind().as_str(),
                idle_quiesce = caps.idle_quiesce.status_str(),
                idle_resume = caps.idle_resume.status_str(),
                "environment reuse is enabled for an UNVERIFIED idle capability \
                 ([pool] allow_unverified_idle): this is a measurement configuration. \
                 Its results must not be reported as a verified warm setup"
            );
        }
        Ok(Arc::new(Self {
            config: Arc::new(config),
            limits,
            clock,
            ids,
            store,
            repos,
            artifacts,
            artifact_service,
            identity,
            secrets,
            usage,
            provider,
            functions,
            revisions,
            aliases,
            invoke,
            logs,
            history,
            provider_service,
            reconcile,
            pool,
        }))
    }

    /// Replace inline invocation outputs past `[store]
    /// output_retention_seconds` by their digest. The gateway runs this on a
    /// timer; the store also runs it when it opens.
    pub fn purge_expired_outputs(&self) -> usize {
        match self.store.purge_expired_outputs(self.clock.now()) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "purging expired invocation outputs failed");
                0
            }
        }
    }

    /// Terminate every pooled environment that is past its idle TTL. The
    /// gateway runs this on a timer; tests call it directly. Returns `None`
    /// when reuse is gated off, because nothing can be pooled then.
    pub async fn sweep_idle_environments(&self) -> Option<PoolSweep> {
        if !self.pool.policy().reuse_enabled() {
            return None;
        }
        Some(self.pool.sweep().await)
    }

    /// Terminate everything the pool holds, whatever its TTL. Called on
    /// graceful shutdown: a pooled environment must never outlive the process
    /// that holds its bridge session (docs/threat-model.md T10).
    pub async fn drain_pool(&self) -> PoolSweep {
        self.pool.drain().await
    }

    /// Reclaim environments a previous process left behind: the provider is
    /// asked what it still runs and every environment this gateway does not
    /// know as active is terminated (docs/architecture.md §4).
    ///
    /// `serve()` calls this before the listener accepts; tests call it
    /// directly. It never fails: a provider that cannot be listed is logged
    /// and startup continues. Returns `None` when `[reconcile] on_startup`
    /// is off.
    pub async fn reconcile_on_startup(&self) -> Option<ReconcileReport> {
        if !self.config.reconcile.on_startup {
            tracing::info!("startup reconcile disabled by configuration");
            return None;
        }
        Some(self.reconcile.reconcile().await)
    }
}
