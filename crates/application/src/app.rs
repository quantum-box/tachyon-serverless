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
use crate::repository::{AsyncDispatchRepository, AsyncInvocationRepository};
use crate::repository::{InMemoryStore, Repositories, SqliteOptions, SqliteStore, StateStore};
use crate::services::admission::{AdmissionController, AdmissionSettings, ScaleDefaults};
use crate::services::invoke::InvokeServiceDeps;
use crate::services::invoke_async::{
    AcceptedEvent, AsyncDispatcher, AsyncDispatcherDeps, AsyncInvokeService,
    AsyncInvokeServiceDeps, AsyncRefusal, DeadLetterService, HandleOutcome, OutboxPublisher,
    PublishReport, QueueHealth, ReapReport,
};
use crate::services::{
    AliasService, ArtifactService, Dispatcher, EnvironmentPool, FunctionService, HistoryService,
    InvokeService, LogService, PoolPolicy, PoolSweep, ProviderService, ReclaimSummary,
    ReconcileReport, ReconcileService, RevisionService, ScaleController, ScaleReport,
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
    /// Usage journal, collector, ledger and provisional rating (PLT-4642).
    /// The driver and the pool emit through it; `usage` is the in-memory view.
    pub usage_meter: Arc<crate::usage::UsageMeter>,
    /// Budget reservations, limits and alerts (PLT-4643).
    pub budget: Arc<crate::budget::BudgetService>,
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
    /// Durable queue, object store and object GC (PLT-4638). All `None`
    /// unless `[queue]` / `[objects]` turn them on.
    pub durable: crate::durable::DurableComponents,
    /// Scale to zero, `min_ready`, cooldown and drains (PLT-4635).
    pub scaling: Arc<ScaleController>,
    /// The durable ledger's asynchronous side (PLT-4639). `None` when the
    /// ledger is volatile.
    pub async_ledger: Option<Arc<dyn AsyncInvocationRepository>>,
    /// Asynchronous acceptance (PLT-4639). `None` without a queue or without
    /// the durable ledger.
    pub invoke_async: Option<Arc<AsyncInvokeService>>,
    /// The transactional outbox publisher. The gateway runs
    /// [`Application::publish_outbox`] in a loop; tests call it directly.
    pub outbox: Option<Arc<OutboxPublisher>>,
    /// Cron and signed webhook triggers (PLT-4641). `None` without
    /// asynchronous invoke or on a data plane.
    pub triggers: Option<Arc<crate::services::triggers::TriggerService>>,
    /// Test-only failpoints (inert unless built with `failpoints`).
    pub failpoints: Arc<crate::failpoints::Failpoints>,
    /// The asynchronous dispatcher: consumer, retries, dead letters and the
    /// reaper (PLT-4640). `None` without asynchronous invoke or with
    /// `[async_dispatch] enabled = false`.
    pub async_dispatcher: Option<Arc<AsyncDispatcher>>,
    /// Dead letters and redrive (PLT-4640). `None` without asynchronous invoke.
    pub dead_letters: Option<Arc<DeadLetterService>>,
    /// The durable ledger's dispatch side (PLT-4640). `None` when the ledger
    /// is volatile.
    pub dispatch_ledger: Option<Arc<dyn AsyncDispatchRepository>>,
    /// Dispatcher and redrive counters for `GET /metrics` (PLT-4640).
    pub dispatch_metrics: Option<Arc<crate::metrics::dispatch::AsyncDispatchMetrics>>,
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
        Self::bootstrap_with_durable(config, provider, options, Default::default())
    }

    /// [`Self::bootstrap_with`] plus durable components built outside the
    /// application crate (the JetStream queue, PLT-4638).
    pub fn bootstrap_with_durable(
        config: GatewayConfig,
        provider: Arc<dyn ExecutionProvider>,
        options: BootstrapOptions,
        durable: crate::durable::DurableOverrides,
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
        // The durable ledger, kept concretely as well: asynchronous acceptance
        // and the outbox exist only on it (PLT-4639).
        let mut durable_ledger: Option<Arc<SqliteStore>> = None;
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
                let sqlite = Arc::new(sqlite);
                durable_ledger = Some(sqlite.clone());
                sqlite
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
        // The usage journal and ledger live next to the ledger store: on disk
        // when the state is persisted, in memory otherwise (PLT-4642).
        let usage_meter = crate::usage::UsageMeter::open(
            &config.usage,
            (options.persist_state && config.store.backend == StoreBackend::Sqlite)
                .then_some(config.data_dir.as_path()),
            options.clock.clone(),
            config.profile,
        )
        .map_err(|e| AppError::InvalidRequest(format!("[usage]: {e}")))?;
        let metered_sink: Arc<dyn UsageSink> = Arc::new(crate::usage::JournalingUsageSink::new(
            usage_meter.clone(),
            usage.clone(),
        ));
        let provider_workdir: PathBuf = config
            .provider
            .workdir()
            .map(PathBuf::from)
            .unwrap_or_else(|| config.data_dir.join(provider.kind().as_str()));
        let entrypoints = EntrypointPolicy::new(provider_workdir);

        let clock = options.clock;
        let ids = options.ids;
        let durable = crate::durable::build(&config, store.clone(), clock.clone(), durable)?;
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
                metered_sink.clone(),
                clock.clone(),
                policy,
            )
            .owned_by(dispatcher.id().clone()),
        );
        // Configuration distribution (PLT-4636, docs/adr/0007). A combined
        // gateway publishes from its ledger and reads its own publication in
        // process; a data plane reads what its control plane delivered.
        let control = &config.control_plane;
        // Budgets (PLT-4643) are published by the control plane with the rest
        // of the configuration, in the price table's units.
        let budget_publisher = match (control.role, config.budget.publishes()) {
            (GatewayRole::Combined, true) => Some(Arc::new(
                crate::budget::BudgetPublisher::new(&config.budget, usage_meter.price_table())
                    .map_err(|e| AppError::InvalidRequest(format!("[budget]: {e}")))?,
            )),
            _ => None,
        };
        let config_publisher = (control.role == GatewayRole::Combined).then(|| {
            let source = LedgerConfigSource::new(
                store.clone(),
                config_signal.clone(),
                &config.identity.tokens,
                control,
                dispatcher.instance().to_string(),
            );
            Arc::new(match &budget_publisher {
                Some(p) => source.with_budgets(p.clone()),
                None => source,
            })
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
        let budget = crate::budget::BudgetService::open(
            &config.budget,
            (options.persist_state && config.store.backend == StoreBackend::Sqlite)
                .then_some(config.data_dir.as_path()),
            usage_meter.clone(),
            config_cache.clone(),
            clock.clone(),
            crate::budget::GatewayBounds {
                handshake_timeout_ms: config.invoke.handshake_timeout_ms,
                cancel_grace_ms: config.invoke.cancel_grace_ms,
                max_response_bytes: limits.max_response_bytes,
            },
        )
        .map_err(|e| AppError::InvalidRequest(format!("[budget]: {e}")))?;
        if let Some(p) = &budget_publisher {
            budget.set_publisher(p.clone());
        }
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
        let drain_timeout = config.scaling.effective_drain_timeout_seconds(
            limits.max_execution_timeout_seconds,
            config.invoke.cancel_grace(),
        );
        admission.set_scale_defaults(ScaleDefaults {
            idle_ttl_seconds: config.pool.idle_ttl_seconds,
            scale_down_cooldown_seconds: config.scaling.scale_down_cooldown_seconds,
            info: tachyon_serverless_api_types::ScalingInfo {
                reconcile_interval_ms: config.scaling.reconcile_interval_ms,
                default_idle_ttl_seconds: config.pool.idle_ttl_seconds,
                default_scale_down_cooldown_seconds: config.scaling.scale_down_cooldown_seconds,
                drain_timeout_seconds: drain_timeout,
                warm_pool: policy.reuse_enabled(),
                at_zero: "zero environments is not zero host cost: the gateway, its store and \
                          the node keep running"
                    .into(),
            },
        });
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
            usage: metered_sink.clone(),
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
            meter: usage_meter.clone(),
            budget: budget.clone(),
        });
        let scaling = ScaleController::new(
            admission.clone(),
            pool.clone(),
            invoke.clone(),
            invoke_gate.clone(),
            repos.clone(),
            clock.clone(),
            config.scaling.clone(),
            drain_timeout,
            control.role == GatewayRole::Combined,
        );
        // Asynchronous invoke (PLT-4639): only with a queue and the durable
        // ledger. The outbox publisher claims rows as this dispatcher.
        let failpoints = Arc::new(crate::failpoints::Failpoints::from_env());
        let async_ledger: Option<Arc<dyn AsyncInvocationRepository>> = durable_ledger
            .clone()
            .map(|l| l as Arc<dyn AsyncInvocationRepository>);
        let dispatch_ledger: Option<Arc<dyn AsyncDispatchRepository>> = durable_ledger
            .clone()
            .map(|l| l as Arc<dyn AsyncDispatchRepository>);
        let mut async_dispatcher = None;
        let mut dead_letters = None;
        let mut dispatch_metrics = None;
        let (invoke_async, outbox) = match (&durable.queue, &async_ledger) {
            (Some(queue), Some(ledger)) => {
                let health = Arc::new(QueueHealth::default());
                let wake = Arc::new(tokio::sync::Notify::new());
                let service = AsyncInvokeService::new(AsyncInvokeServiceDeps {
                    ledger: ledger.clone(),
                    invocations: store.clone(),
                    idempotency: store.clone(),
                    objects: durable.objects.clone(),
                    region: config
                        .objects
                        .parsed_regions()
                        .ok()
                        .and_then(|r| r.into_iter().next()),
                    gate: invoke_gate.clone(),
                    clock: clock.clone(),
                    ids: ids.clone(),
                    limits: limits.clone(),
                    config: config.invoke_async.clone(),
                    failpoints: failpoints.clone(),
                    health: health.clone(),
                    wake: wake.clone(),
                });
                let publisher = Arc::new(OutboxPublisher::new(
                    ledger.clone(),
                    queue.clone(),
                    dispatcher.id().to_string(),
                    clock.clone(),
                    config.invoke_async.clone(),
                    failpoints.clone(),
                    health.clone(),
                    wake.clone(),
                ));
                // PLT-4640: the consumer and dead letters on the same ledger.
                if let Some(dispatch) = &dispatch_ledger {
                    let counters =
                        Arc::new(crate::metrics::dispatch::AsyncDispatchMetrics::default());
                    dispatch_metrics = Some(counters.clone());
                    dead_letters = Some(Arc::new(DeadLetterService::new(
                        dispatch.clone(),
                        ledger.clone(),
                        store.clone(),
                        invoke_gate.clone(),
                        clock.clone(),
                        ids.clone(),
                        config.invoke_async.clone(),
                        health.clone(),
                        wake.clone(),
                        counters.clone(),
                    )));
                    if config.async_dispatch.enabled {
                        async_dispatcher = Some(AsyncDispatcher::new(AsyncDispatcherDeps {
                            ledger: ledger.clone(),
                            dispatch: dispatch.clone(),
                            invocations: store.clone(),
                            objects: durable.objects.clone(),
                            queue: queue.clone(),
                            invoke: invoke.clone(),
                            gate: invoke_gate.clone(),
                            clock: clock.clone(),
                            ids: ids.clone(),
                            owner: dispatcher.id().to_string(),
                            config: config.async_dispatch.clone(),
                            failpoints: failpoints.clone(),
                            publisher_wake: wake.clone(),
                            jitter: Arc::new(crate::services::invoke_async::retry::system_jitter),
                            metrics: counters.clone(),
                        })?);
                    }
                }
                (Some(service), Some(publisher))
            }
            (Some(_), None) => {
                tracing::warn!(
                    "[queue] is configured but the ledger is volatile: asynchronous invoke is \
                     refused (503 not_configured) because an accepted invocation would not \
                     survive a restart"
                );
                (None, None)
            }
            _ => (None, None),
        };
        // Triggers (PLT-4641): only where asynchronous invoke is, and only on
        // the gateway that owns the management store. Every fire goes through
        // the acceptance above.
        let triggers = match (&invoke_async, &durable_ledger) {
            (Some(service), Some(ledger)) if control.role == GatewayRole::Combined => {
                let secret_key = match (
                    &config.triggers.secret_key_file,
                    &config.triggers.secret_key_env,
                ) {
                    (Some(path), _) => Some(Arc::new(
                        crate::durable::ObjectKey::from_file(path)
                            .map_err(|e| AppError::InvalidRequest(format!("[triggers] {e}")))?,
                    )),
                    (None, Some(var)) => Some(Arc::new(
                        crate::durable::ObjectKey::from_env(var)
                            .map_err(|e| AppError::InvalidRequest(format!("[triggers] {e}")))?,
                    )),
                    (None, None) => None,
                };
                Some(crate::services::triggers::TriggerService::new(
                    crate::services::triggers::TriggerServiceDeps {
                        repo: ledger.clone(),
                        repos: repos.clone(),
                        invoke_async: service.clone(),
                        cache: config_cache.clone(),
                        dispatcher: dispatcher.clone(),
                        clock: clock.clone(),
                        ids: ids.clone(),
                        limits: limits.clone(),
                        config: config.triggers.clone(),
                        secret_key,
                    },
                ))
            }
            _ => None,
        };
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
            queue = config.queue.backend.as_str(),
            objects = config.objects.backend.as_str(),
            invoke_async = invoke_async.is_some(),
            triggers = triggers.is_some(),
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
            usage_meter,
            budget,
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
            durable,
            scaling,
            async_ledger,
            invoke_async,
            outbox,
            triggers,
            failpoints,
            async_dispatcher,
            dead_letters,
            dispatch_ledger,
            dispatch_metrics,
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

    /// One object retention pass (PLT-4638): expired and orphaned objects
    /// that no non-terminal invocation references. `None` without an object
    /// store. The gateway runs it every `[objects] gc_interval_seconds`.
    pub async fn collect_objects(&self) -> Option<crate::durable::GcReport> {
        let gc = self.durable.object_gc.as_ref()?;
        Some(gc.run().await)
    }

    /// One outbox publisher pass (PLT-4639). `None` without asynchronous
    /// invoke.
    pub async fn publish_outbox(&self) -> Option<PublishReport> {
        let outbox = self.outbox.as_ref()?;
        Some(outbox.run_once().await)
    }

    /// One cron scheduler pass (PLT-4641). `None` without triggers. The
    /// gateway runs it in a loop; tests call it directly.
    pub async fn run_trigger_scheduler(
        &self,
    ) -> Option<crate::services::triggers::SchedulerReport> {
        let triggers = self.triggers.as_ref()?;
        Some(triggers.run_scheduler_once().await)
    }

    /// Pull and handle at most one asynchronous event (PLT-4640). `None`
    /// without a dispatcher or when nothing arrived. The gateway runs this in
    /// `[async_dispatch] workers` loops; tests call it directly.
    pub async fn dispatch_async_once(&self) -> Option<HandleOutcome> {
        self.async_dispatcher.as_ref()?.run_once().await
    }

    /// One reaper pass of the asynchronous dispatcher (PLT-4640).
    pub async fn reap_async(&self) -> Option<ReapReport> {
        Some(self.async_dispatcher.as_ref()?.reap().await)
    }

    /// Resolve a delivered asynchronous invoke event against the ledger
    /// (invocation, tenant, pinned revision, input and its digest).
    pub async fn read_async_delivery(
        &self,
        delivery: &tachyon_serverless_durable_port::Delivery,
    ) -> Result<AcceptedEvent, AppError> {
        let Some(ledger) = &self.async_ledger else {
            return Err(AppError::AsyncRefused {
                reason: AsyncRefusal::NotConfigured,
                message: "asynchronous invoke needs the durable ledger".into(),
            });
        };
        crate::services::invoke_async::read_delivery(
            ledger.as_ref(),
            self.repos.invocations.as_ref(),
            self.durable.objects.as_deref(),
            delivery,
        )
        .await
    }

    /// Deliver the usage journal to the usage ledger (PLT-4642). The gateway
    /// runs this every `[usage] collect_interval_ms` and once more on
    /// shutdown; tests call it directly.
    pub fn collect_usage(&self) -> Result<crate::usage::CollectReport, String> {
        let result = self.usage_meter.collect();
        // Budget settlement (PLT-4643) follows every collection, whether it
        // delivered anything or not: expiries do not depend on the collector.
        let settled = self.budget.settle_ready();
        if settled != crate::budget::SettleReport::default() {
            tracing::debug!(?settled, "budget settlement pass");
        }
        result
    }

    /// Renew this dispatcher's lease and the slot leases of its in-flight
    /// attempts. The gateway runs this every `[dispatcher]
    /// heartbeat_interval_seconds`; tests call it directly.
    pub fn heartbeat(&self) -> HeartbeatOutcome {
        let metrics = self.admission.metrics();
        match self.dispatcher.heartbeat() {
            Ok(outcome) => {
                match &outcome {
                    HeartbeatOutcome::Renewed { leases } => metrics.heartbeat("renewed", *leases),
                    HeartbeatOutcome::Fenced => metrics.heartbeat("fenced", 0),
                }
                outcome
            }
            Err(e) => {
                tracing::warn!(error = %e, "dispatcher heartbeat failed");
                metrics.heartbeat("error", 0);
                HeartbeatOutcome::Renewed { leases: 0 }
            }
        }
    }

    /// `GET /metrics` (PLT-4637, docs/metrics.md): the Prometheus text
    /// exposition of admission, pool, attempts, boot identity, host usage of
    /// this dispatcher's live environments, the dispatcher lease and the
    /// configuration cache. Covers every tenant: serve it to the `[metrics]`
    /// operator credential only.
    ///
    /// Each call also takes one host usage sample per live environment; the
    /// idle CPU figures compare it with the previous call's.
    pub async fn render_metrics(&self) -> String {
        use crate::metrics::render::{EnvironmentUsage, MetricsInput, SeriesLimits, render};
        const MAX_SAMPLED: usize = 1024;
        let owner = self.dispatcher.id().clone();
        let live: Vec<_> = self
            .repos
            .environments
            .list_active()
            .unwrap_or_default()
            .into_iter()
            .filter(|e| e.owner.as_ref() == Some(&owner) && !e.state.is_terminal())
            .take(MAX_SAMPLED)
            .collect();
        let mut environments = Vec::with_capacity(live.len());
        let mut samples = Vec::new();
        for env in &live {
            let stats = self
                .provider
                .environment_stats(&env.id)
                .await
                .ok()
                .flatten();
            if let Some(cpu) = stats.as_ref().and_then(|s| s.cpu_seconds) {
                samples.push((
                    env.id.clone(),
                    crate::metrics::EnvSample {
                        at: std::time::Instant::now(),
                        cpu_seconds: cpu,
                        idle: env.state.name() == "idle",
                    },
                ));
            }
            environments.push(EnvironmentUsage {
                environment: env.id.to_string(),
                tenant: env.tenant_id.to_string(),
                revision: env.revision_id.to_string(),
                state: env.state.name(),
                stats,
            });
        }
        let metrics = self.admission.metrics();
        metrics.sample_round(&samples);
        // The async outbox backlog (PLT-4639), only where there is one.
        let outbox = match (&self.outbox, &self.async_ledger) {
            (Some(publisher), Some(ledger)) => ledger.outbox_stats().ok().map(|stats| {
                use crate::services::invoke_async::QueueCondition;
                crate::metrics::render::OutboxMetrics {
                    pending: stats.pending,
                    oldest_pending_at: stats.oldest_pending_at,
                    sent_retained: stats.sent,
                    queue_condition: match publisher.health().condition() {
                        QueueCondition::Healthy => "healthy",
                        QueueCondition::Full => "full",
                        QueueCondition::Unavailable => "unavailable",
                    },
                }
            }),
            _ => None,
        };
        let m = &self.config.metrics;
        render(&MetricsInput {
            version: env!("CARGO_PKG_VERSION").to_string(),
            provider: self.provider.kind().as_str().to_string(),
            reuse_enabled: self.pool.policy().reuse_enabled(),
            pool_held: self.pool.held() as u64,
            pool_quiescing: self.pool.quiescing() as u64,
            admission: self.admission.metrics_view(),
            events: metrics.snapshot(),
            environments,
            dispatcher_fenced: self.dispatcher.is_fenced(),
            config: self.config_cache.status(),
            now: self.clock.now(),
            limits: SeriesLimits {
                revisions: m.max_revision_series,
                tenants: m.max_tenant_series,
                environments: m.max_environment_series,
            },
            outbox,
            usage: Some(self.usage_metrics()),
            triggers: self.triggers.as_ref().map(|t| t.metrics().snapshot()),
            dispatch: self.dispatch_metrics.as_ref().map(|m| m.snapshot()),
            budget: Some(self.budget.metrics(m.max_tenant_series)),
        })
    }

    /// The usage pipeline's state for `GET /metrics` (PLT-4642).
    fn usage_metrics(&self) -> crate::metrics::render::UsageMetrics {
        let s = self.usage_meter.status();
        crate::metrics::render::UsageMetrics {
            journal_healthy: s.journal.healthy,
            journal_admitting: s.metered,
            journal_pending_events: s.journal.pending_events,
            journal_pending_bytes: s.journal.pending_bytes,
            journal_max_events: s.journal.limits.max_events,
            journal_max_bytes: s.journal.limits.max_bytes,
            unjournaled_events: s.journal.unjournaled_events,
            collector_runs: s.collector.runs,
            collector_failing: s.collector.last_error.is_some(),
            collector_last_success_at: s.collector.last_success_at,
            collector_delivered: s.collector.delivered,
            ledger_events: s.ledger.map(|l| l.events),
            ledger_duplicates_ignored: s.ledger.map(|l| l.duplicates_ignored),
        }
    }

    /// Whether this gateway reuses environments, and what the boot identity
    /// check has seen (PLT-4637). Part of `GET /v1/capacity`: node-wide, no
    /// tenant data.
    pub fn reuse_report(&self) -> tachyon_serverless_api_types::EnvironmentReuseReport {
        use crate::metrics::BootCheck;
        let events = self.admission.metrics().snapshot();
        let count = |c: BootCheck| events.boot_checks.get(&c).copied().unwrap_or(0);
        let policy = self.pool.policy();
        let enabled = policy.reuse_enabled();
        tachyon_serverless_api_types::EnvironmentReuseReport {
            provider: self.provider.kind().as_str().to_string(),
            mode: if enabled {
                "warm_reuse"
            } else {
                "every_invocation_boots"
            }
            .to_string(),
            reason: policy.reason().to_string(),
            first_boots: count(BootCheck::FirstBoot),
            same_boot_reuses: count(BootCheck::SameBoot),
            boot_id_changed: count(BootCheck::BootChanged),
            boot_id_unreported: count(BootCheck::Unreported),
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

    /// One scale reconcile round (PLT-4635): routes and drains, drain
    /// timeouts, the idle sweep, `min_ready` pre-starts and deletion
    /// finalization. The gateway runs this every `[scaling]
    /// reconcile_interval_ms`; tests call it directly.
    pub async fn reconcile_scaling(&self) -> ScaleReport {
        self.scaling.reconcile().await
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
