//! Runtime API served by the bridge inside the guest for the user process.
//!
//! ```text
//! GET  /runtime/v1/next                              -> 200 + event headers + JSON payload
//! POST /runtime/v1/invocations/{attempt_id}/response -> 202
//! POST /runtime/v1/invocations/{attempt_id}/error    -> 202
//! POST /runtime/v1/init/error                        -> 202
//! POST /runtime/v1/ready                             -> 202
//! ```
//!
//! The bridge is the only client of the host; the user process never talks to
//! the host directly. Tenant, epoch and deadline are host-assigned and the
//! bridge rejects reports for attempts it did not hand out.

use serde::{Deserialize, Serialize};

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

/// Payload of a `tachyon.http.v1` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequestEvent {
    pub method: String,
    /// Path without query string, always starting with `/`.
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
}
