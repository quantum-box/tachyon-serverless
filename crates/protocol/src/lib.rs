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
///
/// The host accepts a guest only when the two numbers are equal, so a bump is
/// how an addition that a guest must *understand* is kept from reaching a
/// guest that cannot.
///
/// - **1**: P1.
/// - **2** (PLT-4633): `HostMessage::Ping` / `GuestMessage::Pong` and
///   `HostMessage::Invoke.remaining_ms`. An unknown message type is a protocol
///   error, so a version-1 bridge must never be sent a `Ping`; and a
///   version-1 bridge would compute the user process's deadline from an
///   absolute host timestamp, which is wrong for a guest that was paused.
///
/// - **3** (PLT-4653, X1 experimental): the restore frames
///   [`GuestMessage::CheckpointWaiting`], [`GuestMessage::Reconnect`],
///   [`HostMessage::Restore`] and `HelloAck.snapshot_hold`. Nothing else
///   changed, so the host still accepts a version-2 guest
///   ([`MIN_PROTOCOL_VERSION`]) and never sends it a restore frame or a
///   snapshot hold. A bridge built without its `experimental-restore` feature
///   keeps saying 2 (docs/protocol.md §A「version の交渉」).
pub const PROTOCOL_VERSION: u32 = 3;

/// Oldest guest protocol version the host still accepts. The session speaks
/// the guest's version: frames introduced later are never sent to it.
pub const MIN_PROTOCOL_VERSION: u32 = 2;

/// First version that understands the experimental restore frames.
pub const RESTORE_PROTOCOL_VERSION: u32 = 3;

/// Whether a guest that said `version` in `Hello` is accepted by this host.
pub fn host_accepts(version: u32) -> bool {
    (MIN_PROTOCOL_VERSION..=PROTOCOL_VERSION).contains(&version)
}

/// Whether restore frames may be exchanged with a guest of `version`.
pub fn supports_restore(version: u32) -> bool {
    version >= RESTORE_PROTOCOL_VERSION
}

/// Guest vsock port of the restore doorbell (X1, PLT-4653): a guest held for
/// a snapshot listens here, and the host of a restored copy connects to it
/// right after the load so the guest drops its dead connection at once
/// (docs/adr/0015 §「Firecracker で分かったこと」1).
pub const DOORBELL_VSOCK_PORT: u32 = 5001;

/// Lifecycle phase a guest must report in [`GuestMessage::CheckpointWaiting`]
/// before the host may snapshot it.
pub const CHECKPOINT_PHASE: &str = "checkpoint";

/// Default vsock port the guest bridge connects to on the host (CID 2).
pub const DEFAULT_VSOCK_PORT: u32 = 5000;

/// Hard upper bound of a single frame on the wire. Larger frames are a
/// protocol violation and close the session.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Bytes of a frame reserved for the message envelope around a payload
/// (`type`, ids, epoch, timings). A payload of at most
/// `MAX_FRAME_BYTES - FRAME_ENVELOPE_HEADROOM` canonical JSON bytes always
/// fits a single frame.
pub const FRAME_ENVELOPE_HEADROOM: usize = 64 * 1024;

/// Largest response payload, measured as canonical JSON (what the `Response`
/// frame carries), that the bridge accepts regardless of
/// `HelloAck.max_response_bytes`. Hosts should reject limits above this when
/// validating their configuration.
pub const MAX_RESPONSE_PAYLOAD_BYTES: u64 = (MAX_FRAME_BYTES - FRAME_ENVELOPE_HEADROOM) as u64;

/// Environment variables the bridge sets for the user process.
pub mod env {
    /// Base URL of the Runtime API, e.g. `http://127.0.0.1:9001`.
    pub const RUNTIME_API: &str = "TACHYON_RUNTIME_API";
    /// Environment id the process runs in.
    pub const ENVIRONMENT_ID: &str = "TACHYON_ENVIRONMENT_ID";
    /// Set to `1` when running under the process provider (no isolation).
    pub const UNISOLATED: &str = "TACHYON_UNISOLATED";
}
