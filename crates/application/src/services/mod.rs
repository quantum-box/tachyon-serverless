//! Use cases. Each service is a thin orchestration over the repositories
//! and ports; all authorization goes through [`crate::authz`].

pub mod alias;
pub mod artifact;
pub mod function;
pub mod history;
pub mod invoke;
pub mod provider;
pub mod reconcile;
pub mod revision;

pub use alias::AliasService;
pub use artifact::ArtifactService;
pub use function::FunctionService;
pub use history::{HistoryService, InvocationDetail, LogService, UsageSummary};
pub use invoke::{InvokeOutcome, InvokeRequest, InvokeService};
pub use provider::ProviderService;
pub use reconcile::{ReconcileReport, ReconcileService};
pub use revision::RevisionService;
