//! Use cases. Each service is a thin orchestration over the repositories
//! and ports; all authorization goes through [`crate::authz`].

pub mod alias;
pub mod function;
pub mod history;
pub mod invoke;
pub mod provider;
pub mod revision;

pub use alias::AliasService;
pub use function::FunctionService;
pub use history::{HistoryService, InvocationDetail, LogService, UsageSummary};
pub use invoke::{InvokeOutcome, InvokeRequest, InvokeService};
pub use provider::ProviderService;
pub use revision::RevisionService;
