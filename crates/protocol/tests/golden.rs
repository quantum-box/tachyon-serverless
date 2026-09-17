//! Golden fixtures for the two runtime contracts (PLT-4645).
//!
//! - `tests/golden/wire/*.json`: one host <-> bridge frame body per message
//!   variant (and per `GuestErrorKind`), plus `frame_ping.hex`, the exact bytes
//!   of one length-prefixed frame.
//! - `tests/golden/runtime_api/*.json`: the JSON bodies the bridge and the user
//!   process exchange over the in-guest Runtime API, and `constants.json`, the
//!   paths, headers, event types, limits and the protocol version.
//!
//! For every sample the test checks both directions: the committed fixture must
//! decode to the sample (an old peer's bytes are still understood), and the
//! sample must encode to the fixture (this build still sends the same shape).
//! A renamed, removed or retyped field, a renamed `type` / `kind` tag, a moved
//! path or header, or a changed limit fails here, and the fixture diff is what a
//! reviewer sees. Changing the wire shape of an existing message also needs a
//! `PROTOCOL_VERSION` decision (docs/protocol.md §A); `constants.json` records
//! the version so both land in the same review.
//!
//! To accept an intended change, regenerate and commit the fixtures:
//!
//! ```text
//! TSLS_UPDATE_SNAPSHOTS=1 cargo test -p tachyon-serverless-protocol --test golden
//! ```

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tachyon_serverless_protocol::runtime_api::{
    self, HttpRequestEvent, HttpResponsePayload, RuntimeErrorReport, lifecycle,
};
use tachyon_serverless_protocol::{
    CHECKPOINT_PHASE, DEFAULT_VSOCK_PORT, DOORBELL_VSOCK_PORT, FRAME_ENVELOPE_HEADROOM, FrameCodec,
    GuestErrorKind, GuestMessage, HostMessage, LogPhase, LogStream, MAX_FRAME_BYTES,
    MAX_RESPONSE_PAYLOAD_BYTES, MIN_PROTOCOL_VERSION, PROTOCOL_NAME, PROTOCOL_VERSION,
    RESTORE_PROTOCOL_VERSION, decode_message, encode_message, env,
};

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn updating() -> bool {
    std::env::var_os("TSLS_UPDATE_SNAPSHOTS").is_some()
}

/// Canonical fixture text: pretty JSON, sorted keys, trailing newline.
fn render(value: &Value) -> String {
    fn sorted(v: &Value) -> Value {
        match v {
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                Value::Object(
                    keys.into_iter()
                        .map(|k| (k.clone(), sorted(&map[k])))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    let mut s = serde_json::to_string_pretty(&sorted(value)).unwrap();
    s.push('\n');
    s
}

/// Compare (or, when updating, write) one fixture. Returns a failure message.
fn check<T>(rel: &str, sample: &T) -> Option<String>
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let path = golden_dir().join(rel);
    let encoded: Value = serde_json::from_slice(&encode_message(sample).unwrap()).unwrap();
    if updating() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, render(&encoded)).unwrap();
        return None;
    }
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => return Some(format!("{rel}: missing fixture ({e})")),
    };
    let mut problems = Vec::new();
    match decode_message::<T>(text.as_bytes()) {
        Ok(decoded) if &decoded == sample => {}
        Ok(decoded) => problems.push(format!(
            "fixture decodes to a different value:\n    fixture: {decoded:?}\n    sample:  {sample:?}"
        )),
        Err(e) => problems.push(format!("fixture no longer decodes: {e}")),
    }
    let fixture: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    if fixture != encoded {
        problems.push(format!(
            "this build encodes a different shape:\n    fixture: {}\n    encoded: {}",
            serde_json::to_string(&fixture).unwrap(),
            serde_json::to_string(&encoded).unwrap()
        ));
    }
    if problems.is_empty() && text != render(&fixture) {
        problems.push("fixture is not in canonical form (regenerate it)".into());
    }
    (!problems.is_empty()).then(|| format!("{rel}: {}", problems.join("; ")))
}

fn finish(failures: Vec<String>) {
    assert!(
        failures.is_empty(),
        "golden fixture mismatch - the runtime wire contract changed:\n  {}\n\
         If the change is intended, decide on PROTOCOL_VERSION (docs/protocol.md), then run \
         `TSLS_UPDATE_SNAPSHOTS=1 cargo test -p tachyon-serverless-protocol --test golden` \
         and commit crates/protocol/tests/golden/.",
        failures.join("\n  ")
    );
}

// --- samples -----------------------------------------------------------------------------

/// `type` tag of a host message. Exhaustive on purpose: a new variant does not
/// compile until it is named here, in `HOST_TYPES` and given a sample.
fn host_type(m: &HostMessage) -> &'static str {
    match m {
        HostMessage::HelloAck { .. } => "hello_ack",
        HostMessage::HelloReject { .. } => "hello_reject",
        HostMessage::Invoke { .. } => "invoke",
        HostMessage::Ping { .. } => "ping",
        HostMessage::Cancel { .. } => "cancel",
        HostMessage::Shutdown { .. } => "shutdown",
        HostMessage::Restore { .. } => "restore",
    }
}
const HOST_TYPES: &[&str] = &[
    "hello_ack",
    "hello_reject",
    "invoke",
    "ping",
    "cancel",
    "shutdown",
    "restore",
];

fn guest_type(m: &GuestMessage) -> &'static str {
    match m {
        GuestMessage::Hello { .. } => "hello",
        GuestMessage::Ready { .. } => "ready",
        GuestMessage::InitError { .. } => "init_error",
        GuestMessage::Log { .. } => "log",
        GuestMessage::Response { .. } => "response",
        GuestMessage::Error { .. } => "error",
        GuestMessage::Exited { .. } => "exited",
        GuestMessage::Heartbeat { .. } => "heartbeat",
        GuestMessage::Pong { .. } => "pong",
        GuestMessage::CheckpointWaiting { .. } => "checkpoint_waiting",
        GuestMessage::Reconnect { .. } => "reconnect",
    }
}
const GUEST_TYPES: &[&str] = &[
    "hello",
    "ready",
    "init_error",
    "log",
    "response",
    "error",
    "exited",
    "heartbeat",
    "pong",
    "checkpoint_waiting",
    "reconnect",
];

fn error_kind(k: &GuestErrorKind) -> &'static str {
    match k {
        GuestErrorKind::Handler => "handler",
        GuestErrorKind::Panic => "panic",
        GuestErrorKind::Crash { .. } => "crash",
        GuestErrorKind::Protocol => "protocol",
        GuestErrorKind::ResponseTooLarge { .. } => "response_too_large",
    }
}
const ERROR_KINDS: &[&str] = &[
    "handler",
    "panic",
    "crash",
    "protocol",
    "response_too_large",
];

fn host_samples() -> Vec<(String, HostMessage)> {
    vec![
        HostMessage::HelloAck {
            environment_id: "env_01j0000000000000000000000a".into(),
            epoch: 3,
            entrypoint: "/function/app".into(),
            args: vec!["--flag".into()],
            env: vec![("GREETING".into(), "hello".into())],
            working_dir: "/function".into(),
            init_timeout_ms: 10_000,
            max_response_bytes: 6 * 1024 * 1024,
            max_log_line_bytes: 8192,
            snapshot_hold: false,
        },
        HostMessage::HelloReject {
            reason: "protocol version mismatch".into(),
        },
        HostMessage::Invoke {
            invocation_id: "inv_01j0000000000000000000000a".into(),
            attempt_id: "att_01j0000000000000000000000a".into(),
            epoch: 3,
            event_type: runtime_api::event_types::JSON.into(),
            deadline_ms: 1_700_000_030_000,
            remaining_ms: 30_000,
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
            payload: json!({"name": "golden"}),
        },
        HostMessage::Ping { nonce: 7 },
        HostMessage::Cancel {
            attempt_id: "att_01j0000000000000000000000a".into(),
            grace_ms: 2000,
        },
        HostMessage::Shutdown {
            reason: "terminate".into(),
        },
        // Protocol version 3 (PLT-4653).
        HostMessage::Restore {
            environment_id: "env_01j0000000000000000000000b".into(),
            instance_id: "rst_01j0000000000000000000000c".into(),
            generation: 1,
            epoch: 1,
            host_now_ms: 1_700_000_000_000,
        },
    ]
    .into_iter()
    .map(|m| (format!("wire/host_{}.json", host_type(&m)), m))
    .chain(std::iter::once((
        // Version 3: the same `hello_ack` asking for a snapshot hold. The
        // version-2 fixture above stays byte-identical (the field is omitted
        // when false).
        "wire/host_hello_ack_snapshot_hold.json".to_string(),
        HostMessage::HelloAck {
            environment_id: "env_01j0000000000000000000000a".into(),
            epoch: 1,
            entrypoint: "/function/app".into(),
            args: vec![],
            env: vec![],
            working_dir: "/function".into(),
            init_timeout_ms: 10_000,
            max_response_bytes: 6 * 1024 * 1024,
            max_log_line_bytes: 8192,
            snapshot_hold: true,
        },
    )))
    .collect()
}

fn guest_samples() -> Vec<(String, GuestMessage)> {
    let mut out: Vec<(String, GuestMessage)> = vec![
        // A version-2 bridge (every bridge built without
        // `experimental-restore`) still says exactly this.
        GuestMessage::Hello {
            protocol_version: MIN_PROTOCOL_VERSION,
            bridge_version: "0.1.0".into(),
            environment_id: "env_01j0000000000000000000000a".into(),
            guest_boot_id: Some("6f1c1d0e-8d5b-4f55-9d7e-0a1b2c3d4e5f".into()),
            architecture: "x86_64".into(),
        },
        GuestMessage::Ready { init_ms: 215 },
        GuestMessage::InitError {
            error_type: "Runtime.InitError".into(),
            message: "missing configuration".into(),
            exit_code: Some(3),
        },
        GuestMessage::Log {
            stream: LogStream::Stdout,
            phase: LogPhase::Handler,
            attempt_id: Some("att_01j0000000000000000000000a".into()),
            ts_ms: 1_700_000_000_123,
            line: "hello golden".into(),
        },
        GuestMessage::Response {
            attempt_id: "att_01j0000000000000000000000a".into(),
            epoch: 3,
            payload: json!({"message": "hello golden"}),
            handler_ms: Some(12),
        },
        GuestMessage::Exited {
            exit_code: None,
            signal: Some(9),
        },
        GuestMessage::Heartbeat {
            ts_ms: 1_700_000_000_500,
        },
        GuestMessage::Pong { nonce: 7 },
        // Protocol version 3 (PLT-4653).
        GuestMessage::CheckpointWaiting {
            lifecycle_phase: CHECKPOINT_PHASE.into(),
            lifecycle_version: lifecycle::VERSION,
            after_restore_ran: false,
        },
        GuestMessage::Reconnect {
            protocol_version: RESTORE_PROTOCOL_VERSION,
            environment_id: "env_01j0000000000000000000000a".into(),
            guest_boot_id: Some("6f1c1d0e-8d5b-4f55-9d7e-0a1b2c3d4e5f".into()),
            reconnects: 1,
            lost: "vsock doorbell".into(),
        },
    ]
    .into_iter()
    .map(|m| (format!("wire/guest_{}.json", guest_type(&m)), m))
    .collect();
    out.push((
        "wire/guest_hello_v3.json".into(),
        GuestMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            bridge_version: "0.1.0".into(),
            environment_id: "env_01j0000000000000000000000a".into(),
            guest_boot_id: Some("6f1c1d0e-8d5b-4f55-9d7e-0a1b2c3d4e5f".into()),
            architecture: "aarch64".into(),
        },
    ));

    for kind in [
        GuestErrorKind::Handler,
        GuestErrorKind::Panic,
        GuestErrorKind::Crash {
            exit_code: Some(101),
            signal: None,
        },
        GuestErrorKind::Protocol,
        GuestErrorKind::ResponseTooLarge {
            size_bytes: 7_000_000,
            max_bytes: 6_000_000,
        },
    ] {
        let name = format!("wire/guest_error_{}.json", error_kind(&kind));
        out.push((
            name,
            GuestMessage::Error {
                attempt_id: "att_01j0000000000000000000000a".into(),
                epoch: 3,
                error: kind,
                error_type: "Handler.Error".into(),
                message: "boom".into(),
                stack_trace: Some("at main".into()),
                handler_ms: Some(4),
            },
        ));
    }
    out
}

/// Runtime API constants: paths, headers, event types, limits and versions.
fn constants() -> Value {
    json!({
        "protocol_name": PROTOCOL_NAME,
        "protocol_version": PROTOCOL_VERSION,
        "min_protocol_version": MIN_PROTOCOL_VERSION,
        "restore_protocol_version": RESTORE_PROTOCOL_VERSION,
        "doorbell_vsock_port": DOORBELL_VSOCK_PORT,
        "checkpoint_phase": CHECKPOINT_PHASE,
        "default_vsock_port": DEFAULT_VSOCK_PORT,
        "max_frame_bytes": MAX_FRAME_BYTES,
        "frame_envelope_headroom": FRAME_ENVELOPE_HEADROOM,
        "max_response_payload_bytes": MAX_RESPONSE_PAYLOAD_BYTES,
        "max_error_report_bytes": runtime_api::MAX_ERROR_REPORT_BYTES,
        "env": {
            "runtime_api": env::RUNTIME_API,
            "environment_id": env::ENVIRONMENT_ID,
            "unisolated": env::UNISOLATED,
        },
        "paths": {
            "next": runtime_api::PATH_NEXT,
            "ready": runtime_api::PATH_READY,
            "init_error": runtime_api::PATH_INIT_ERROR,
            "response": runtime_api::path_response("{attempt_id}"),
            "error": runtime_api::path_error("{attempt_id}"),
        },
        "headers": {
            "invocation_id": runtime_api::headers::INVOCATION_ID,
            "attempt_id": runtime_api::headers::ATTEMPT_ID,
            "epoch": runtime_api::headers::EPOCH,
            "deadline_ms": runtime_api::headers::DEADLINE_MS,
            "trace_id": runtime_api::headers::TRACE_ID,
            "event_type": runtime_api::headers::EVENT_TYPE,
        },
        "event_types": {
            "json": runtime_api::event_types::JSON,
            "http": runtime_api::event_types::HTTP,
        },
        "lifecycle": {
            "version": lifecycle::VERSION,
            "header_version": lifecycle::HEADER_VERSION,
            "paths": {
                "bootstrap": lifecycle::PATH_BOOTSTRAP,
                "checkpoint": lifecycle::PATH_CHECKPOINT,
                "continue": lifecycle::PATH_CONTINUE,
                "error": lifecycle::PATH_ERROR,
            },
            "error_types": {
                "pre_checkpoint_failed": lifecycle::error_types::PRE_CHECKPOINT_FAILED,
                "after_restore_failed": lifecycle::error_types::AFTER_RESTORE_FAILED,
                "pre_checkpoint_timeout": lifecycle::error_types::PRE_CHECKPOINT_TIMEOUT,
                "after_restore_timeout": lifecycle::error_types::AFTER_RESTORE_TIMEOUT,
                "checkpoint_timeout": lifecycle::error_types::CHECKPOINT_TIMEOUT,
                "lifecycle_violation": lifecycle::error_types::LIFECYCLE_VIOLATION,
            },
        },
    })
}

const FRAME_FIXTURE: &str = "wire/frame_ping.hex";
const CONSTANTS_FIXTURE: &str = "runtime_api/constants.json";

fn runtime_api_failures() -> Vec<String> {
    let mut failures = Vec::new();
    let mut push = |f: Option<String>| failures.extend(f);

    push(check(
        "runtime_api/error_report_full.json",
        &RuntimeErrorReport {
            error_type: "Handler.Error".into(),
            message: "boom".into(),
            stack_trace: Some("at main".into()),
        },
    ));
    push(check(
        "runtime_api/error_report_minimal.json",
        &RuntimeErrorReport {
            error_type: "Runtime.Panic".into(),
            message: "panicked".into(),
            stack_trace: None,
        },
    ));
    push(check(
        "runtime_api/http_request_event.json",
        &HttpRequestEvent {
            method: "POST".into(),
            path: "/echo/a%2Fb".into(),
            query: "x=1&x=2".into(),
            headers: vec![
                ("x-a".into(), "1".into()),
                ("x-a".into(), "2".into()),
                ("content-type".into(), "application/json".into()),
            ],
            body_base64: "aGk=".into(),
            source_ip: Some("192.0.2.10".into()),
        },
    ));
    push(check(
        "runtime_api/http_response_payload.json",
        &HttpResponsePayload {
            status: 201,
            headers: vec![("x-served-by".into(), "golden".into())],
            body_base64: "b2s=".into(),
        },
    ));
    push(check(
        "runtime_api/continuation_cold.json",
        &lifecycle::Continuation::Cold,
    ));
    push(check(
        "runtime_api/continuation_restored.json",
        &lifecycle::Continuation::Restored {
            instance_id: "inst_1".into(),
            restored_at_ms: 1_700_000_000_000,
            generation: 2,
        },
    ));
    failures
}

fn all_fixture_names() -> BTreeSet<String> {
    let mut names: BTreeSet<String> = host_samples().into_iter().map(|(n, _)| n).collect();
    names.extend(guest_samples().into_iter().map(|(n, _)| n));
    names.extend(
        [
            "runtime_api/error_report_full.json",
            "runtime_api/error_report_minimal.json",
            "runtime_api/http_request_event.json",
            "runtime_api/http_response_payload.json",
            "runtime_api/continuation_cold.json",
            "runtime_api/continuation_restored.json",
            FRAME_FIXTURE,
            CONSTANTS_FIXTURE,
        ]
        .map(String::from),
    );
    names
}

// --- tests ---------------------------------------------------------------------------------

#[test]
fn host_to_bridge_frames_match_the_golden_fixtures() {
    let failures = host_samples()
        .iter()
        .filter_map(|(name, m)| check(name, m))
        .collect();
    finish(failures);
}

#[test]
fn bridge_to_host_frames_match_the_golden_fixtures() {
    let failures = guest_samples()
        .iter()
        .filter_map(|(name, m)| check(name, m))
        .collect();
    finish(failures);
}

#[test]
fn every_message_variant_and_error_kind_has_a_fixture() {
    let host: BTreeSet<&str> = host_samples().iter().map(|(_, m)| host_type(m)).collect();
    assert_eq!(host, HOST_TYPES.iter().copied().collect());
    let guest: BTreeSet<&str> = guest_samples().iter().map(|(_, m)| guest_type(m)).collect();
    assert_eq!(guest, GUEST_TYPES.iter().copied().collect());
    let kinds: BTreeSet<&str> = guest_samples()
        .iter()
        .filter_map(|(_, m)| match m {
            GuestMessage::Error { error, .. } => Some(error_kind(error)),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, ERROR_KINDS.iter().copied().collect());

    // The names above are the `type` / `kind` tags on the wire.
    for (_, m) in host_samples() {
        let v: Value = serde_json::from_slice(&encode_message(&m).unwrap()).unwrap();
        assert_eq!(v["type"], host_type(&m));
    }
    for (_, m) in guest_samples() {
        let v: Value = serde_json::from_slice(&encode_message(&m).unwrap()).unwrap();
        assert_eq!(v["type"], guest_type(&m));
        if let GuestMessage::Error { error, .. } = &m {
            assert_eq!(v["error"]["kind"], error_kind(error));
        }
    }
}

#[test]
fn runtime_api_bodies_match_the_golden_fixtures() {
    finish(runtime_api_failures());
}

#[test]
fn runtime_api_constants_match_the_golden_fixture() {
    let path = golden_dir().join(CONSTANTS_FIXTURE);
    let current = constants();
    if updating() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, render(&current)).unwrap();
        return;
    }
    let text = std::fs::read_to_string(&path).expect("constants fixture");
    let fixture: Value = serde_json::from_str(&text).expect("constants fixture is JSON");
    if fixture != current {
        finish(vec![format!(
            "{CONSTANTS_FIXTURE}: paths, headers, limits or versions changed:\n    fixture: {}\n    current: {}",
            serde_json::to_string(&fixture).unwrap(),
            serde_json::to_string(&current).unwrap()
        )]);
    }
}

#[test]
fn a_length_prefixed_frame_matches_the_golden_bytes() {
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    let msg = HostMessage::Ping { nonce: 7 };
    let mut buf = BytesMut::new();
    FrameCodec
        .encode(encode_message(&msg).unwrap(), &mut buf)
        .unwrap();
    let hex: String = buf.iter().map(|b| format!("{b:02x}")).collect();
    let path = golden_dir().join(FRAME_FIXTURE);
    if updating() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, format!("{hex}\n")).unwrap();
        return;
    }
    let fixture = std::fs::read_to_string(&path).expect("frame fixture");
    let fixture = fixture.trim();
    assert_eq!(
        fixture, hex,
        "{FRAME_FIXTURE}: the frame encoding (u32 big-endian length + JSON body) changed"
    );
    let raw: Vec<u8> = (0..fixture.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&fixture[i..i + 2], 16).expect("hex"))
        .collect();
    let mut src = BytesMut::from(&raw[..]);
    let body = FrameCodec.decode(&mut src).unwrap().expect("a whole frame");
    assert!(src.is_empty());
    assert_eq!(decode_message::<HostMessage>(&body).unwrap(), msg);
}

#[test]
fn no_stale_fixture_is_left_behind() {
    fn walk(dir: &Path, root: &Path, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                walk(&p, root, out);
            } else {
                out.insert(
                    p.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    if updating() {
        return;
    }
    let root = golden_dir();
    let mut on_disk = BTreeSet::new();
    walk(&root, &root, &mut on_disk);
    let expected = all_fixture_names();
    assert_eq!(
        on_disk, expected,
        "fixtures on disk and samples differ: remove stale files or add samples"
    );
}
