//! Runtime API served by the bridge inside the guest for the user process.
//!
//! ```text
//! GET  /runtime/v1/next                              -> 200 + event headers + JSON payload
//! POST /runtime/v1/invocations/{attempt_id}/response -> 202 (413: too large, attempt settled)
//! POST /runtime/v1/invocations/{attempt_id}/error    -> 202 (413: too large, attempt settled)
//! POST /runtime/v1/init/error                        -> 202
//! POST /runtime/v1/ready                             -> 202
//! ```
//!
//! Experimental restore-aware lifecycle (PLT-4651, X1; see [`lifecycle`]):
//!
//! ```text
//! POST /runtime/v1/lifecycle/bootstrap   -> 202 (409 when not the first lifecycle call)
//! POST /runtime/v1/lifecycle/checkpoint  -> 202 (409 unless bootstrapping)
//! GET  /runtime/v1/lifecycle/continue    -> 200 Continuation (blocks; 409 before checkpoint)
//! POST /runtime/v1/lifecycle/error       -> 202 (the bridge classifies the phase)
//! ```
//!
//! The bridge is the only client of the host; the user process never talks to
//! the host directly. Tenant, epoch and deadline are host-assigned and the
//! bridge rejects reports for attempts it did not hand out.

use serde::{Deserialize, Serialize};

/// Upper bound of a `RuntimeErrorReport` body (error and init error reports).
/// A larger report is answered `413`; for `/error` the bridge then completes
/// the attempt itself with `Runtime.ErrorReportTooLarge`. SDKs must truncate
/// reports so they stay below this bound.
pub const MAX_ERROR_REPORT_BYTES: usize = 1024 * 1024;

pub const PATH_NEXT: &str = "/runtime/v1/next";
pub const PATH_READY: &str = "/runtime/v1/ready";
pub const PATH_INIT_ERROR: &str = "/runtime/v1/init/error";
pub fn path_response(attempt_id: &str) -> String {
    format!("/runtime/v1/invocations/{attempt_id}/response")
}
pub fn path_error(attempt_id: &str) -> String {
    format!("/runtime/v1/invocations/{attempt_id}/error")
}

/// Response headers on `GET /runtime/v1/next`.
pub mod headers {
    pub const INVOCATION_ID: &str = "tachyon-invocation-id";
    pub const ATTEMPT_ID: &str = "tachyon-attempt-id";
    pub const EPOCH: &str = "tachyon-epoch";
    /// Absolute deadline, milliseconds since Unix epoch.
    pub const DEADLINE_MS: &str = "tachyon-deadline-ms";
    pub const TRACE_ID: &str = "tachyon-trace-id";
    pub const EVENT_TYPE: &str = "tachyon-event-type";
}

/// Event type identifiers carried in `tachyon-event-type`.
pub mod event_types {
    pub const JSON: &str = "tachyon.invoke.v1";
    pub const HTTP: &str = "tachyon.http.v1";
}

/// Error report posted by the user process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeErrorReport {
    /// e.g. `Handler.Error`, `Runtime.Panic`, `Runtime.InitError`.
    pub error_type: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

/// Experimental restore-aware lifecycle (PLT-4651, X1).
///
/// A function that opts in splits its initialization in two:
///
/// 1. `POST bootstrap`: "I am about to build reusable, instance-independent
///    state" (synchronous; no async runtime, no secrets, no connections).
/// 2. `POST checkpoint`: "that state is built; this is a safe point to
///    snapshot me".
/// 3. `GET continue`: blocks until the bridge decides how this instance
///    continues: [`Continuation::Cold`] (no snapshot was taken; the normal
///    start takes the same path) or [`Continuation::Restored`] (this process
///    is a copy resumed from a snapshot and must rebuild per-instance state).
/// 4. per-instance state (identity, RNG, clock, credentials, runtime,
///    connections), then `POST /runtime/v1/ready`.
///
/// While a lifecycle is open the bridge refuses `ready` and `next` until
/// `continue` has been answered, so a process never becomes Ready with a
/// half-initialized state. Failures are reported on `POST error`; the bridge
/// picks the error type from its own view of the phase. Timeouts are detected
/// by the bridge (the init deadline) and typed by the phase it was in.
///
/// None of this makes arbitrary libraries or multithreaded runtimes
/// snapshot-safe: it only gives a function a place to put state that must not
/// be shared between copies (docs/protocol.md §B "experimental lifecycle").
///
/// Everything here is additive: a process that never calls these paths sees
/// exactly the P1 Runtime API, and the host↔bridge frames are unchanged.
pub mod lifecycle {
    use serde::{Deserialize, Serialize};

    /// Version of the experimental lifecycle contract, carried in the
    /// [`HEADER_VERSION`] response header of `continue`.
    pub const VERSION: u32 = 1;
    pub const HEADER_VERSION: &str = "tachyon-lifecycle-version";

    pub const PATH_BOOTSTRAP: &str = "/runtime/v1/lifecycle/bootstrap";
    pub const PATH_CHECKPOINT: &str = "/runtime/v1/lifecycle/checkpoint";
    pub const PATH_CONTINUE: &str = "/runtime/v1/lifecycle/continue";
    pub const PATH_ERROR: &str = "/runtime/v1/lifecycle/error";

    /// `InitError.error_type` values produced by the lifecycle.
    pub mod error_types {
        /// The bootstrap hook failed (error or panic) before the checkpoint.
        pub const PRE_CHECKPOINT_FAILED: &str = "Runtime.PreCheckpointFailed";
        /// The after-restore hook failed (error or panic).
        pub const AFTER_RESTORE_FAILED: &str = "Runtime.AfterRestoreFailed";
        /// The init deadline passed before the process reported the checkpoint.
        pub const PRE_CHECKPOINT_TIMEOUT: &str = "Runtime.PreCheckpointTimeout";
        /// The init deadline passed after `continue` and before `ready`.
        pub const AFTER_RESTORE_TIMEOUT: &str = "Runtime.AfterRestoreTimeout";
        /// The init deadline passed while the bridge (provider) had not yet
        /// answered `continue`. Not the function's fault.
        pub const CHECKPOINT_TIMEOUT: &str = "Runtime.CheckpointTimeout";
        /// `ready` / `next` / a lifecycle call arrived out of order.
        pub const LIFECYCLE_VIOLATION: &str = "Runtime.LifecycleViolation";
    }

    /// How this process continues after the checkpoint. Body of
    /// `GET /runtime/v1/lifecycle/continue`.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub enum Continuation {
        /// No snapshot was taken: the process simply goes on. This is the
        /// answer whenever the provider has no snapshot support.
        Cold,
        /// The process was resumed from a snapshot.
        Restored {
            /// Identity of this copy; distinct for every restore.
            instance_id: String,
            /// Guest wall clock when the restore was announced, ms since Unix
            /// epoch.
            restored_at_ms: u64,
            /// How many times the snapshot has been restored (1 = first copy).
            generation: u64,
        },
    }
}

/// Payload of a `tachyon.http.v1` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequestEvent {
    pub method: String,
    /// Request-target path exactly as received: still percent-encoded (never
    /// decoded), without the query string, always starting with `/`. SDKs
    /// pass it to the router unchanged so the router performs the only
    /// decode (`%2F` stays distinct from `/`).
    pub path: String,
    /// Raw query string without the leading `?`. Empty when absent.
    #[serde(default)]
    pub query: String,
    /// Repeated headers are preserved as multiple entries.
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// Body encoded as standard base64.
    #[serde(default)]
    pub body_base64: String,
    /// Peer address as seen by the gateway, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<String>,
}

/// Response payload for a `tachyon.http.v1` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpResponsePayload {
    pub status: u16,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub body_base64: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_event_roundtrip() {
        let e = HttpRequestEvent {
            method: "POST".into(),
            path: "/echo".into(),
            query: "a=1".into(),
            headers: vec![("x-a".into(), "1".into()), ("x-a".into(), "2".into())],
            body_base64: "aGk=".into(),
            source_ip: None,
        };
        let j = serde_json::to_string(&e).unwrap();
        assert_eq!(serde_json::from_str::<HttpRequestEvent>(&j).unwrap(), e);
    }

    #[test]
    fn continuation_wire_shape() {
        use lifecycle::Continuation;
        assert_eq!(
            serde_json::to_string(&Continuation::Cold).unwrap(),
            r#"{"kind":"cold"}"#
        );
        let r = Continuation::Restored {
            instance_id: "inst_1".into(),
            restored_at_ms: 5,
            generation: 2,
        };
        let j = serde_json::to_string(&r).unwrap();
        assert_eq!(
            j,
            r#"{"kind":"restored","instance_id":"inst_1","restored_at_ms":5,"generation":2}"#
        );
        assert_eq!(serde_json::from_str::<Continuation>(&j).unwrap(), r);
        assert!(serde_json::from_str::<Continuation>(r#"{"kind":"teleported"}"#).is_err());
    }
}
