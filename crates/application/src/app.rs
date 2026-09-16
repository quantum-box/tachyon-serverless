//! Composition root: wires configuration, ports and services together.

use std::path::PathBuf;
use std::sync::Arc;

use tachyon_serverless_domain::{Clock, IdGenerator, Limits, SystemClock, UlidGenerator};
use tachyon_serverless_provider_port::{
    ArtifactStore, ExecutionProvider, IdentityProvider, SecretProvider,
};

use crate::config::{GatewayConfig, Profile, ProviderConfig};
use crate::entrypoint::EntrypointPolicy;
use crate::error::AppError;
use crate::local_ports::{
    InMemoryUsageSink, LocalArtifactStore, StaticIdentityProvider, StaticSecretProvider,
};
use crate::repository::{InMemoryStore, Repositories};
use crate::services::invoke::InvokeServiceDeps;
use crate::services::{
    AliasService, ArtifactService, FunctionService, HistoryService, InvokeService, LogService,
    ProviderService, RevisionService,
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
    pub store: Arc<InMemoryStore>,
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
    /// Write `state.json` through under `data_dir`. Artifacts are always on disk.
    pub persist_state: bool,
}

impl Default for BootstrapOptions {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            ids: Arc::new(UlidGenerator),
            persist_state: true,
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
        let store = Arc::new(if options.persist_state {
            InMemoryStore::with_persistence(&config.data_dir, limits.clone(), options.clock.now())?
        } else {
            InMemoryStore::new(limits.clone())
        });
        let repos = Repositories::in_memory(store.clone());
        let artifacts: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
            &config.data_dir,
            limits.max_artifact_bytes,
        )?);
        let identity: Arc<dyn IdentityProvider> =
            Arc::new(StaticIdentityProvider::from_config(&config.identity.tokens));
        let secrets: Arc<dyn SecretProvider> =
            Arc::new(StaticSecretProvider::from_config(&config.secrets.bindings));
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
        let provider_service = Arc::new(ProviderService::new(
            provider.clone(),
            config.invoke.preflight_ttl(),
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
        });
        tracing::info!(
            profile = config.profile.as_str(),
            provider = provider.kind().as_str(),
            dev_only = caps.dev_only,
            data_dir = %config.data_dir.display(),
            "application bootstrapped"
        );
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
        }))
    }
}
