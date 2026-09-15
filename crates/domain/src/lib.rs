//! Tachyon Serverless domain model.
//!
//! This crate is the single source of truth for identifiers, entities,
//! state machines and the error taxonomy shared by every other crate.
//!
//! Boundary rules (enforced by tests in `boundary.rs`):
//! - no async runtime, no HTTP framework, no database driver, no hypervisor API;
//! - every state transition is a method on the entity that returns
//!   `Result<_, DomainError>` and rejects updates to terminal states;
//! - identifiers follow the existing Tachyon convention `<prefix>_<lowercase ULID>`.

pub mod alias;
pub mod clock;
pub mod environment;
pub mod error;
pub mod function;
pub mod ids;
pub mod invocation;
pub mod limits;
pub mod log;
pub mod revision;
pub mod usage;

pub use alias::*;
pub use clock::*;
pub use environment::*;
pub use error::*;
pub use function::*;
pub use ids::*;
pub use invocation::*;
pub use limits::*;
pub use log::*;
pub use revision::*;
pub use usage::*;

#[cfg(test)]
mod boundary;
