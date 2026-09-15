//! Tachyon Serverless protocol contracts.
//!
//! Two independent contracts live here (RFC §11):
//!
//! 1. **Bridge protocol** ([`wire`]): length-prefixed JSON frames exchanged
//!    between the host (gateway/dispatcher) and the guest *runtime bridge*
//!    over vsock (Firecracker) or a Unix socket (process provider).
//! 2. **Runtime API** ([`runtime_api`]): the local HTTP long-poll API the
//!    bridge exposes to the user function inside the guest. SDKs speak this.
//!
//! Both are versioned separately from the management API.

pub mod runtime_api;
pub mod wire;

pub use wire::*;

/// Name of the runtime protocol as declared in a revision spec.
pub const PROTOCOL_NAME: &str = tachyon_serverless_domain::RUNTIME_PROTOCOL_V1;

/// Numeric protocol version carried in the bridge handshake.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default vsock port the guest bridge connects to on the host (CID 2).
pub const DEFAULT_VSOCK_PORT: u32 = 5000;

/// Hard upper bound of a single frame on the wire. Larger frames are a
/// protocol violation and close the session.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Environment variables the bridge sets for the user process.
pub mod env {
    /// Base URL of the Runtime API, e.g. `http://127.0.0.1:9001`.
    pub const RUNTIME_API: &str = "TACHYON_RUNTIME_API";
    /// Environment id the process runs in.
    pub const ENVIRONMENT_ID: &str = "TACHYON_ENVIRONMENT_ID";
    /// Set to `1` when running under the process provider (no isolation).
    pub const UNISOLATED: &str = "TACHYON_UNISOLATED";
}
