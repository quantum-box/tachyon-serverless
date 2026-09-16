//! Ports (RFC §6.3, PLT-4617).
//!
//! The application layer depends only on these traits. Concrete providers
//! (`providers/firecracker`, `providers/process`, `providers/fake`) implement
//! [`ExecutionProvider`]; local/static implementations of the other ports
//! live in the application crate.

pub mod artifact;
pub mod execution;
pub mod identity;
pub mod secret;
pub mod usage;

pub use artifact::*;
pub use execution::*;
pub use identity::*;
pub use secret::*;
pub use usage::*;
