//! Tachyon Serverless application layer.
//!
//! Use cases, repositories, local ports, the host side of the bridge
//! protocol and the synchronous invoke pipeline (docs/architecture.md §3-5).
//! This crate depends only on the domain, protocol, provider-port and
//! api-types contracts; it never imports a web framework or a hypervisor
//! API.

pub mod app;
pub mod authz;
pub mod bridge_session;
pub mod config;
pub mod control;
pub mod durable;
pub mod entrypoint;
pub mod error;
pub mod failpoints;
pub mod local_ports;
pub mod metrics;
pub mod repository;
pub mod services;
pub mod usage;

pub use app::{Application, BootstrapOptions, ProviderFactory};
pub use config::{
    ControlPlaneConfig, ControlPlaneOutageConfig, DispatcherConfig, GatewayConfig, GatewayRole,
    PoolConfig, Profile, ProviderConfig, ProviderKindConfig, ReconcileConfig, StoreBackend,
    StoreConfig,
};
pub use durable::{DurableComponents, DurableOverrides};
pub use error::AppError;
pub use services::{
    AliasService, ArtifactService, Dispatcher, EnvironmentPool, FunctionService, HistoryService,
    InvocationDetail, InvokeOutcome, InvokeRequest, InvokeService, LogService, PoolPolicy,
    PoolSweep, ProviderService, ReconcileReport, ReconcileService, ReuseDisabled, RevisionService,
    UsageSummary,
};
