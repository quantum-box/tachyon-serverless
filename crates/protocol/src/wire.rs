//! Host <-> guest bridge frames.
//!
//! Frame = `u32` big-endian length + UTF-8 JSON body. Messages are tagged by
//! `type`. Unknown message types are a protocol error; unknown *fields* are
//! ignored so additive changes stay compatible within a version.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::MAX_FRAME_BYTES;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("frame too large: {0} bytes (max {MAX_FRAME_BYTES})")]
    FrameTooLarge(usize),
    #[error("malformed frame: {0}")]
    Malformed(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Stream classification for guest log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
    Bridge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogPhase {
    Init,
    Handler,
    Shutdown,
}

/// Kind of guest-side error reported for an attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GuestErrorKind {
    /// Handler returned an error (user code decided to fail).
    Handler,
    /// Handler panicked but the process survived.
    Panic,
    /// User process exited while an attempt was in flight.
    Crash {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    /// The user process violated the runtime API (e.g. wrong attempt id).
    Protocol,
    /// Response exceeded the allowed size.
    ResponseTooLarge { size_bytes: u64, max_bytes: u64 },
}

/// Messages sent by the guest bridge to the host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GuestMessage {
    /// First message after connecting.
    Hello {
        protocol_version: u32,
        bridge_version: String,
        /// Environment id the bridge was started for (from kernel cmdline / argv).
        environment_id: String,
        /// `/proc/sys/kernel/random/boot_id` when available.
        guest_boot_id: Option<String>,
        architecture: String,
    },
    /// User process reported ready (or first `next` poll observed).
    Ready {
        /// Milliseconds from user process start to ready.
        init_ms: u64,
    },
    /// User process failed before becoming ready.
    InitError {
        error_type: String,
        message: String,
        exit_code: Option<i32>,
    },
    Log {
        stream: LogStream,
        phase: LogPhase,
        attempt_id: Option<String>,
        /// Milliseconds since Unix epoch as observed by the guest.
        ts_ms: u64,
        line: String,
    },
    Response {
        attempt_id: String,
        epoch: u64,
        payload: serde_json::Value,
        /// Handler wall time measured inside the guest (informational only).
        handler_ms: Option<u64>,
    },
    Error {
        attempt_id: String,
        epoch: u64,
        error: GuestErrorKind,
        error_type: String,
        message: String,
        stack_trace: Option<String>,
        handler_ms: Option<u64>,
    },
    /// User process exited after Ready with no attempt in flight.
    Exited {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    Heartbeat {
        ts_ms: u64,
    },
}

/// Messages sent by the host to the guest bridge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostMessage {
    /// Reply to `Hello`. Carries everything needed to start the user process.
    /// `env` may contain resolved secrets; the bridge must never log it.
    HelloAck {
        environment_id: String,
        epoch: u64,
        /// Absolute path of the executable inside the guest.
        entrypoint: String,
        args: Vec<String>,
        env: Vec<(String, String)>,
        /// Working directory for the user process.
        working_dir: String,
        init_timeout_ms: u64,
        max_response_bytes: u64,
        max_log_line_bytes: u64,
    },
    /// Handshake rejected (version mismatch, unknown environment...). The
    /// bridge exits after receiving this.
    HelloReject {
        reason: String,
    },
    Invoke {
        invocation_id: String,
        attempt_id: String,
        epoch: u64,
        event_type: String,
        /// Absolute deadline, milliseconds since Unix epoch.
        deadline_ms: u64,
        trace_id: String,
        payload: serde_json::Value,
    },
    /// Cooperative cancellation. The bridge signals the user process and,
    /// after `grace_ms`, kills it. The host terminates the environment anyway.
    Cancel {
        attempt_id: String,
        grace_ms: u64,
    },
    Shutdown {
        reason: String,
    },
}

/// Length-prefixed frame codec. Emits/consumes raw `Bytes`; use
/// [`encode_message`] / [`decode_message`] for typed access.
#[derive(Debug, Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Bytes;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Bytes>, ProtocolError> {
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len > MAX_FRAME_BYTES {
            return Err(ProtocolError::FrameTooLarge(len));
        }
        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }
        src.advance(4);
        Ok(Some(src.split_to(len).freeze()))
    }
}

impl Encoder<Bytes> for FrameCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        if item.len() > MAX_FRAME_BYTES {
            return Err(ProtocolError::FrameTooLarge(item.len()));
        }
        dst.reserve(4 + item.len());
        dst.put_u32(item.len() as u32);
        dst.extend_from_slice(&item);
        Ok(())
    }
}

pub fn encode_message<T: Serialize>(msg: &T) -> Result<Bytes, ProtocolError> {
    let v = serde_json::to_vec(msg).map_err(|e| ProtocolError::Malformed(e.to_string()))?;
    if v.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge(v.len()));
    }
    Ok(Bytes::from(v))
}

pub fn decode_message<T: for<'de> Deserialize<'de>>(frame: &[u8]) -> Result<T, ProtocolError> {
    serde_json::from_slice(frame).map_err(|e| ProtocolError::Malformed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use tokio_util::codec::{FramedRead, FramedWrite};

    #[tokio::test]
    async fn frames_roundtrip_over_duplex() {
        let (a, b) = tokio::io::duplex(1024);
        let mut w = FramedWrite::new(a, FrameCodec);
        let mut r = FramedRead::new(b, FrameCodec);
        let msg = HostMessage::Invoke {
            invocation_id: "inv_1".into(),
            attempt_id: "att_1".into(),
            epoch: 1,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 42,
            trace_id: "t".into(),
            payload: serde_json::json!({"name": "x"}),
        };
        w.send(encode_message(&msg).unwrap()).await.unwrap();
        let frame = r.next().await.unwrap().unwrap();
        let back: HostMessage = decode_message(&frame).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();
        buf.put_u32((MAX_FRAME_BYTES + 1) as u32);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::FrameTooLarge(_))
        ));
    }

    #[test]
    fn unknown_fields_are_ignored_but_unknown_types_fail() {
        let json = r#"{"type":"heartbeat","ts_ms":1,"extra":true}"#;
        let m: GuestMessage = decode_message(json.as_bytes()).unwrap();
        assert_eq!(m, GuestMessage::Heartbeat { ts_ms: 1 });
        let bad = r#"{"type":"teleport"}"#;
        assert!(decode_message::<GuestMessage>(bad.as_bytes()).is_err());
    }
}
