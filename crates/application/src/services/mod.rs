//! Use cases. Each service is a thin orchestration over the repositories
//! and ports; all authorization goes through [`crate::authz`].

pub mod admission;
pub mod alias;
pub mod artifact;
pub mod dispatcher;
pub mod function;
pub mod history;
pub mod invoke;
pub mod pool;
pub mod provider;
pub mod reconcile;
pub mod revision;

pub use admission::{AdmissionController, Grant, RejectReason, Rejection};
pub use alias::AliasService;
pub use artifact::ArtifactService;
pub use dispatcher::Dispatcher;
pub use function::FunctionService;
pub use history::{HistoryService, InvocationDetail, LogService, UsageSummary};
pub use invoke::{InvokeOutcome, InvokeRequest, InvokeService};
pub use pool::{
    EnvironmentPool, PoolPolicy, PoolSweep, ReuseDisabled, WarmEnvironment, reuse_key_for,
    secret_binding_generation,
};
pub use provider::ProviderService;
pub use reconcile::{ReclaimSummary, ReconcileReport, ReconcileService};
pub use revision::RevisionService;
