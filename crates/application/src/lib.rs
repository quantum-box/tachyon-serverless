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
pub mod entrypoint;
pub mod error;
pub mod local_ports;
pub mod repository;
pub mod services;

pub use app::{Application, BootstrapOptions, ProviderFactory};
pub use config::{GatewayConfig, Profile, ProviderConfig, ProviderKindConfig};
pub use error::AppError;
pub use services::{
    AliasService, ArtifactService, FunctionService, HistoryService, InvocationDetail,
    InvokeOutcome, InvokeRequest, InvokeService, LogService, ProviderService, RevisionService,
    UsageSummary,
};
