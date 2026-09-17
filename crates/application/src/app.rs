//! Composition root: wires configuration, ports and services together.

use std::path::PathBuf;
use std::sync::Arc;

use tachyon_serverless_domain::{Clock, IdGenerator, Limits, SystemClock, UlidGenerator};
use tachyon_serverless_provider_port::{
    ArtifactStore, ExecutionProvider, IdentityProvider, SecretProvider, UsageSink,
};

use crate::config::{GatewayConfig, GatewayRole, Profile, ProviderConfig, StoreBackend};
use crate::control::{
    CacheSettings, ConfigCache, ConfigChangeSignal, ConfigSource, InvokeGate, LedgerConfigSource,
    RefreshReport, SignalingConfigRepos, SourceError, grant_secret,
};
use crate::entrypoint::EntrypointPolicy;
use crate::error::AppError;
use crate::local_ports::{
    InMemoryUsageSink, LocalArtifactStore, StaticIdentityProvider, StaticSecretProvider,
};
use crate::repository::HeartbeatOutcome;
use crate::repository::{InMemoryStore, Repositories, SqliteOptions, SqliteStore, StateStore};
use crate::services::admission::{AdmissionController, AdmissionSettings};
use crate::services::invoke::InvokeServiceDeps;
use crate::services::{
    AliasService, ArtifactService, Dispatcher, EnvironmentPool, FunctionService, HistoryService,
    InvokeService, LogService, PoolPolicy, PoolSweep, ProviderService, ReclaimSummary,
    ReconcileReport, ReconcileService, RevisionService,
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
    /// This process's dispatcher identity and lease (PLT-4631).
    pub dispatcher: Arc<Dispatcher>,
    /// The configuration invoke reads: functions, routes, revisions,
    /// authorization grants and policy, with generations and expiry
    /// (PLT-4636).
    pub config_cache: Arc<ConfigCache>,
    /// What this gateway still accepts and starts given its cache, control
    /// plane and provider (PLT-4636).
    pub invoke_gate: Arc<InvokeGate>,
    /// The publication a `combined` gateway serves on
    /// `GET /v1/internal/config`. `None` on a data plane.
    pub config_publisher: Option<Arc<LedgerConfigSource>>,
    /// Capacity ledger, fair queue, quotas and autoscaler gate (PLT-4634).
    pub admission: Arc<AdmissionController>,
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
    /// Where a `data_plane` gateway takes its configuration from (the gateway
    /// passes an HTTP client of the management gateway; tests a fake). Must be
    /// `None` for a `combined` gateway, which publishes from its own ledger.
    pub config_source: Option<Arc<dyn ConfigSource>>,
}

impl Default for BootstrapOptions {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            ids: Arc::new(UlidGenerator),
            persist_state: true,
            secrets: None,
            config_source: None,
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
                        idempotency_retention: config.store.idempotency_retention(),
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
                    purged_idempotency_keys = report.settled.idempotency_purged,
                    outputs_purged = report.outputs_purged,
                    "state store opened"
                );
                Arc::new(sqlite)
            } else {
                Arc::new(
                    InMemoryStore::new(limits.clone())
                        .with_idempotency_retention(config.store.idempotency_retention()),
                )
            };
        // Every function / revision / alias write this process makes tells
        // the in-process configuration cache to refresh before it answers
        // (PLT-4636).
        let config_signal = Arc::new(ConfigChangeSignal::default());
        let mut repos = Repositories::from_store(store.clone());
        {
            let signaling = Arc::new(SignalingConfigRepos::new(
                store.clone(),
                config_signal.clone(),
            ));
            repos.functions = signaling.clone();
            repos.revisions = signaling.clone();
            repos.aliases = signaling;
        }
        // A new dispatcher incarnation for this process, then the ledger half
        // of the reclaim: only the work of dispatchers that lost their lease,
        // stopped, or whose previous incarnation is gone is settled. A second
        // gateway on the same data_dir leaves the first one's work alone.
        let instance = config
            .dispatcher
            .instance
            .clone()
            .unwrap_or_else(|| format!("gateway@{}", config.listen));
        let dispatcher = Dispatcher::register(
            repos.slots.clone(),
            options.clock.clone(),
            options.ids.as_ref(),
            config.dispatcher.clone(),
            instance,
        )?;
        if let Err(e) = dispatcher.reclaim_ledger() {
            tracing::warn!(error = %e, "reclaiming expired dispatchers failed; retried on the heartbeat");
        }
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
            dispatcher.clone(),
        ));
        // The pool gets the usage sink because it, not the driver, is what
        // ends a pooled environment's life (TTL sweep, drain, retire) and
        // therefore what has to report it (docs/architecture.md §4).
        let pool = Arc::new(
            EnvironmentPool::new(
                repos.clone(),
                provider.clone(),
                usage.clone() as Arc<dyn UsageSink>,
                clock.clone(),
                policy,
            )
            .owned_by(dispatcher.id().clone()),
        );
        // Configuration distribution (PLT-4636, docs/adr/0007). A combined
        // gateway publishes from its ledger and reads its own publication in
        // process; a data plane reads what its control plane delivered.
        let control = &config.control_plane;
        let config_publisher = (control.role == GatewayRole::Combined).then(|| {
            Arc::new(LedgerConfigSource::new(
                store.clone(),
                config_signal.clone(),
                &config.identity.tokens,
                control,
                dispatcher.instance().to_string(),
            ))
        });
        let config_source: Arc<dyn ConfigSource> = match (control.role, &options.config_source) {
            (GatewayRole::Combined, None) => config_publisher
                .clone()
                .expect("a combined gateway always has a publisher"),
            (GatewayRole::DataPlane, Some(source)) => source.clone(),
            (GatewayRole::Combined, Some(_)) => {
                return Err(AppError::InvalidRequest(
                    "a combined gateway publishes its own configuration; a config source is \
                     only for role = \"data_plane\""
                        .into(),
                ));
            }
            (GatewayRole::DataPlane, None) => {
                return Err(AppError::InvalidRequest(
                    "role = \"data_plane\" needs a configuration source".into(),
                ));
            }
        };
        let config_cache = Arc::new(ConfigCache::new(
            config_source,
            clock.clone(),
            CacheSettings::from_config(control),
            grant_secret(control),
        ));
        let invoke_gate = Arc::new(InvokeGate::new(
            config_cache.clone(),
            provider_service.clone(),
            control.role,
            config.control_plane_outage.allow_cold_start,
        ));
        // Admission (PLT-4634). When a cold start is blocked on node resources
        // that idle pooled environments hold, it asks the pool to evict them.
        let admission = AdmissionController::new(
            AdmissionSettings::from_config(&config.capacity),
            clock.clone(),
        );
        {
            let pool = Arc::downgrade(&pool);
            admission.set_evictor(move |count| {
                let Some(pool) = pool.upgrade() else {
                    return;
                };
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        pool.evict_idle(count).await;
                    });
                }
            });
        }
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
            dispatcher: dispatcher.clone(),
            gate: invoke_gate.clone(),
            admission: admission.clone(),
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
            dispatcher_id = %dispatcher.id(),
            node = %config.capacity.node.name,
            node_region = config.capacity.node.region.as_deref().unwrap_or("none"),
            node_memory_mib = ?config.capacity.node.memory_mib,
            max_concurrency = config.capacity.max_concurrency,
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
            dispatcher,
            config_cache,
            invoke_gate,
            config_publisher,
            admission,
        }))
    }

    /// One refresh of the configuration cache (PLT-4636). When it ends an
    /// outage, the dispatcher lease is re-validated at once instead of on the
    /// next heartbeat tick: a dispatcher that was fenced while the gateway
    /// could not reach its control plane keeps refusing new work
    /// (docs/adr/0007 §dispatcher). The gateway runs this in a loop with
    /// [`ConfigCache::next_delay`]; tests call it directly.
    pub async fn refresh_config(&self) -> Result<RefreshReport, SourceError> {
        let result = self.config_cache.refresh().await;
        match &result {
            Ok(report) if report.reconnected => {
                let lease = self.heartbeat();
                tracing::info!(
                    generation = report.generation,
                    applied = report.apply.applied,
                    dispatcher_id = %self.dispatcher.id(),
                    dispatcher_lease = ?lease,
                    fenced = self.dispatcher.is_fenced(),
                    "control plane reachable again; configuration converging, dispatcher lease \
                     re-validated"
                );
            }
            Ok(_) => {}
            Err(e) => {
                let status = self.config_cache.status();
                tracing::warn!(
                    error = %e,
                    consecutive_failures = status.consecutive_failures,
                    config_valid_until = ?status.config_valid_until,
                    auth_valid_until = ?status.auth_valid_until,
                    "configuration refresh failed; serving from the cache until it expires"
                );
            }
        }
        result
    }

    /// Renew this dispatcher's lease and the slot leases of its in-flight
    /// attempts. The gateway runs this every `[dispatcher]
    /// heartbeat_interval_seconds`; tests call it directly.
    pub fn heartbeat(&self) -> HeartbeatOutcome {
        match self.dispatcher.heartbeat() {
            Ok(outcome) => outcome,
            Err(e) => {
                tracing::warn!(error = %e, "dispatcher heartbeat failed");
                HeartbeatOutcome::Renewed { leases: 0 }
            }
        }
    }

    /// Reclaim the work of dispatchers that lost their lease and terminate
    /// the environments that fenced (PLT-4631). The gateway runs this on its
    /// heartbeat timer; tests call it directly.
    pub async fn reclaim_expired(&self) -> ReclaimSummary {
        self.reconcile.reclaim().await
    }

    /// Record a graceful stop of this dispatcher: whatever it still owns may
    /// be reclaimed at once by another gateway.
    pub fn stop_dispatcher(&self) {
        self.dispatcher.stop();
    }

    /// Delete idempotency bindings past `[store] idempotency_retention_seconds`.
    pub fn purge_expired_idempotency(&self) -> usize {
        match self.store.purge_expired_idempotency(self.clock.now()) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "purging expired idempotency keys failed");
                0
            }
        }
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
