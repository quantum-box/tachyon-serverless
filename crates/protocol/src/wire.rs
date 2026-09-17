//! Host <-> guest bridge frames.
//!
//! Frame = `u32` big-endian length + UTF-8 JSON body. Messages are tagged by
//! `type`. Unknown message types are a protocol error; unknown *fields* are
//! ignored so additive changes stay compatible within a version.

use std::fmt;

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
    /// Answer to [`HostMessage::Ping`], carrying the nonce back unchanged.
    ///
    /// Sent by the bridge's own frame loop, so it proves that the guest is
    /// being scheduled and that the bridge is still reading its end of the
    /// connection — which is exactly what the host has to know after resuming
    /// a quiesced environment (protocol version 2, docs/protocol.md §A).
    Pong {
        nonce: u64,
    },
    /// Protocol version 3 (X1, PLT-4653). Sent once, and only when the host
    /// asked for a snapshot hold in `HelloAck`: the user process opened the
    /// experimental lifecycle, reported `checkpoint` and is now blocked in
    /// `continue`. `after_restore` has not run, so nothing it creates
    /// (identity, credentials, connections) is in guest memory. The host may
    /// snapshot the guest only after this frame.
    CheckpointWaiting {
        /// [`crate::CHECKPOINT_PHASE`] as observed by the bridge.
        lifecycle_phase: String,
        /// `tachyon-lifecycle-version` of the Runtime API.
        lifecycle_version: u32,
        /// Always `false` here; recorded in the manifest as evidence.
        after_restore_ran: bool,
    },
    /// Protocol version 3 (X1, PLT-4653). The first frame of a connection
    /// that a held guest re-established after its vsock transport was reset
    /// (a restored copy, or a resumed source). Never sent by a cold boot,
    /// which says `Hello`: a host expecting a restore that receives `Hello`
    /// is looking at a cold boot and must not count it as restored.
    Reconnect {
        protocol_version: u32,
        /// Environment id the guest was booted for (the snapshot source).
        environment_id: String,
        /// Unchanged by a restore: equal to the source's `Hello`.
        guest_boot_id: Option<String>,
        /// 1 for the first reconnect of this guest.
        reconnects: u64,
        /// Why the previous connection ended, as the guest saw it.
        lost: String,
    },
}

/// Messages sent by the host to the guest bridge.
///
/// `Debug` is implemented by hand: `HelloAck.env` carries resolved secrets
/// and renders as `<N vars, redacted>`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
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
        /// Protocol version 3 (X1, PLT-4653): hold the process at the
        /// lifecycle checkpoint and report [`GuestMessage::CheckpointWaiting`]
        /// instead of answering `continue` with `cold`. Omitted when false,
        /// and only ever set for a version-3 guest (a version-2 bridge would
        /// ignore it and start cold, which the host detects).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        snapshot_hold: bool,
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
        /// Absolute deadline, milliseconds since Unix epoch, **in the host's
        /// clock**. Informational for the guest: the host is what enforces it.
        deadline_ms: u64,
        /// Milliseconds left until `deadline_ms` when the host wrote this
        /// frame.
        ///
        /// The bridge derives the deadline it publishes to the user process
        /// from this (`guest now + remaining_ms`) rather than from
        /// `deadline_ms`, because a guest that was quiesced has a clock that
        /// stopped with it: after a resume of arbitrary length, an absolute
        /// host timestamp means nothing in the guest's frame and the guest
        /// would compute a deadline that is wrong by the length of the pause
        /// (protocol version 2, docs/protocol.md §A and §B).
        remaining_ms: u64,
        trace_id: String,
        payload: serde_json::Value,
    },
    /// Liveness probe. The bridge answers with [`GuestMessage::Pong`] carrying
    /// the same nonce and does nothing else; it never reaches the user
    /// process.
    ///
    /// The host uses it as the readiness check of a resumed environment: a
    /// quiesced guest cannot answer, so an answer is evidence that the guest
    /// is running again rather than an assumption that it is
    /// (docs/architecture.md §4).
    Ping {
        nonce: u64,
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
    /// Protocol version 3 (X1, PLT-4653). Answer to [`GuestMessage::Reconnect`]
    /// on a restored copy: the identity of this copy, delivered after the
    /// restore and never captured in the snapshot. The bridge sets the guest
    /// wall clock to `host_now_ms` and then answers the lifecycle `continue`
    /// with `restored`.
    Restore {
        /// Environment id the host tracks this copy as (not the source's).
        environment_id: String,
        /// Identity of this copy; distinct for every clone.
        instance_id: String,
        /// Restore count of the snapshot (1 = first copy).
        generation: u64,
        /// Epoch of the copy's first assignment.
        epoch: u64,
        /// Host wall clock, ms since the Unix epoch.
        host_now_ms: u64,
    },
}

/// Stand-in for a secret-bearing environment list in `Debug` output.
struct RedactedEnv(usize);

impl fmt::Debug for RedactedEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} vars, redacted>", self.0)
    }
}

impl fmt::Debug for HostMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostMessage::HelloAck {
                environment_id,
                epoch,
                entrypoint,
                args,
                env,
                working_dir,
                init_timeout_ms,
                max_response_bytes,
                max_log_line_bytes,
                snapshot_hold,
            } => f
                .debug_struct("HelloAck")
                .field("environment_id", environment_id)
                .field("epoch", epoch)
                .field("entrypoint", entrypoint)
                .field("args", args)
                .field("env", &RedactedEnv(env.len()))
                .field("working_dir", working_dir)
                .field("init_timeout_ms", init_timeout_ms)
                .field("max_response_bytes", max_response_bytes)
                .field("max_log_line_bytes", max_log_line_bytes)
                .field("snapshot_hold", snapshot_hold)
                .finish(),
            HostMessage::HelloReject { reason } => f
                .debug_struct("HelloReject")
                .field("reason", reason)
                .finish(),
            HostMessage::Invoke {
                invocation_id,
                attempt_id,
                epoch,
                event_type,
                deadline_ms,
                remaining_ms,
                trace_id,
                payload,
            } => f
                .debug_struct("Invoke")
                .field("invocation_id", invocation_id)
                .field("attempt_id", attempt_id)
                .field("epoch", epoch)
                .field("event_type", event_type)
                .field("deadline_ms", deadline_ms)
                .field("remaining_ms", remaining_ms)
                .field("trace_id", trace_id)
                .field("payload", payload)
                .finish(),
            HostMessage::Ping { nonce } => f.debug_struct("Ping").field("nonce", nonce).finish(),
            HostMessage::Cancel {
                attempt_id,
                grace_ms,
            } => f
                .debug_struct("Cancel")
                .field("attempt_id", attempt_id)
                .field("grace_ms", grace_ms)
                .finish(),
            HostMessage::Shutdown { reason } => {
                f.debug_struct("Shutdown").field("reason", reason).finish()
            }
            HostMessage::Restore {
                environment_id,
                instance_id,
                generation,
                epoch,
                host_now_ms,
            } => f
                .debug_struct("Restore")
                .field("environment_id", environment_id)
                .field("instance_id", instance_id)
                .field("generation", generation)
                .field("epoch", epoch)
                .field("host_now_ms", host_now_ms)
                .finish(),
        }
    }
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
    use crate::MAX_RESPONSE_PAYLOAD_BYTES;
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
            remaining_ms: 30_000,
            trace_id: "t".into(),
            payload: serde_json::json!({"name": "x"}),
        };
        w.send(encode_message(&msg).unwrap()).await.unwrap();
        let frame = r.next().await.unwrap().unwrap();
        let back: HostMessage = decode_message(&frame).unwrap();
        assert_eq!(back, msg);
    }

    /// PLT-4633 (review F2 and F5): protocol version 2 carries the liveness
    /// probe the host needs after a resume, and the time left that a guest
    /// with a stopped clock needs instead of an absolute host timestamp. Both
    /// survive the wire unchanged.
    #[test]
    fn version_2_carries_the_probe_and_the_remaining_time() {
        let ping = HostMessage::Ping { nonce: 7 };
        let back: HostMessage = decode_message(&encode_message(&ping).unwrap()).unwrap();
        assert_eq!(back, ping);
        assert_eq!(
            String::from_utf8(encode_message(&ping).unwrap().to_vec()).unwrap(),
            r#"{"type":"ping","nonce":7}"#
        );

        let pong = GuestMessage::Pong { nonce: 7 };
        let back: GuestMessage = decode_message(&encode_message(&pong).unwrap()).unwrap();
        assert_eq!(back, pong);

        // The nonce is what pairs an answer with its probe: a `Pong` from an
        // earlier probe must be distinguishable from the one being waited for.
        assert_ne!(back, GuestMessage::Pong { nonce: 8 });

        let invoke = HostMessage::Invoke {
            invocation_id: "inv_1".into(),
            attempt_id: "att_1".into(),
            epoch: 3,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 1_700_000_000_000,
            remaining_ms: 25_000,
            trace_id: "t".into(),
            payload: serde_json::json!({}),
        };
        let encoded = encode_message(&invoke).unwrap();
        assert!(
            String::from_utf8(encoded.to_vec())
                .unwrap()
                .contains(r#""remaining_ms":25000"#)
        );
        let back: HostMessage = decode_message(&encoded).unwrap();
        assert_eq!(back, invoke);
        assert!(format!("{invoke:?}").contains("remaining_ms: 25000"));
    }

    /// PLT-4653: version 3 adds the restore frames; a version-2 `HelloAck`
    /// (no `snapshot_hold`) encodes exactly as before, and the negotiation
    /// accepts versions 2 and 3 only.
    #[test]
    fn version_3_restore_frames_and_negotiation() {
        use crate::{MIN_PROTOCOL_VERSION, PROTOCOL_VERSION, host_accepts, supports_restore};
        assert_eq!((MIN_PROTOCOL_VERSION, PROTOCOL_VERSION), (2, 3));
        assert!(!host_accepts(1) && host_accepts(2) && host_accepts(3) && !host_accepts(4));
        assert!(!supports_restore(2) && supports_restore(3));

        let ack = |hold| HostMessage::HelloAck {
            environment_id: "env_1".into(),
            epoch: 1,
            entrypoint: "/function/app".into(),
            args: vec![],
            env: vec![],
            working_dir: "/function".into(),
            init_timeout_ms: 1000,
            max_response_bytes: 1,
            max_log_line_bytes: 1,
            snapshot_hold: hold,
        };
        let plain = String::from_utf8(encode_message(&ack(false)).unwrap().to_vec()).unwrap();
        assert!(!plain.contains("snapshot_hold"), "{plain}");
        let held = encode_message(&ack(true)).unwrap();
        assert!(
            String::from_utf8(held.to_vec())
                .unwrap()
                .contains(r#""snapshot_hold":true"#)
        );
        assert_eq!(decode_message::<HostMessage>(&held).unwrap(), ack(true));

        for m in [
            GuestMessage::CheckpointWaiting {
                lifecycle_phase: "checkpoint".into(),
                lifecycle_version: 1,
                after_restore_ran: false,
            },
            GuestMessage::Reconnect {
                protocol_version: 3,
                environment_id: "env_1".into(),
                guest_boot_id: Some("b".into()),
                reconnects: 1,
                lost: "doorbell".into(),
            },
        ] {
            assert_eq!(
                decode_message::<GuestMessage>(&encode_message(&m).unwrap()).unwrap(),
                m
            );
        }
        let restore = HostMessage::Restore {
            environment_id: "env_2".into(),
            instance_id: "i".into(),
            generation: 1,
            epoch: 1,
            host_now_ms: 5,
        };
        assert_eq!(
            decode_message::<HostMessage>(&encode_message(&restore).unwrap()).unwrap(),
            restore
        );
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
    fn hello_ack_debug_redacts_env() {
        let msg = HostMessage::HelloAck {
            environment_id: "env_1".into(),
            epoch: 4,
            entrypoint: "/function/app".into(),
            args: vec!["--flag".into()],
            env: vec![
                ("DEMO_SECRET".into(), "s3cr3t-value".into()),
                ("PLAIN".into(), "visible-only-as-count".into()),
            ],
            working_dir: "/function".into(),
            init_timeout_ms: 1000,
            max_response_bytes: 2048,
            max_log_line_bytes: 512,
            snapshot_hold: false,
        };
        for rendered in [format!("{msg:?}"), format!("{msg:#?}")] {
            assert!(!rendered.contains("s3cr3t-value"), "{rendered}");
            assert!(!rendered.contains("DEMO_SECRET"), "{rendered}");
            assert!(!rendered.contains("visible-only-as-count"), "{rendered}");
            assert!(rendered.contains("<2 vars, redacted>"), "{rendered}");
            // Non-secret fields stay useful for debugging.
            assert!(rendered.contains("/function/app"), "{rendered}");
            assert!(rendered.contains("env_1"), "{rendered}");
        }
        let other = HostMessage::Cancel {
            attempt_id: "att_9".into(),
            grace_ms: 250,
        };
        assert_eq!(
            format!("{other:?}"),
            r#"Cancel { attempt_id: "att_9", grace_ms: 250 }"#
        );
    }

    #[test]
    fn response_payload_bound_leaves_room_for_the_envelope() {
        let payload =
            serde_json::Value::String("x".repeat(MAX_RESPONSE_PAYLOAD_BYTES as usize - 2));
        assert_eq!(
            serde_json::to_vec(&payload).unwrap().len() as u64,
            MAX_RESPONSE_PAYLOAD_BYTES
        );
        let msg = GuestMessage::Response {
            attempt_id: format!("att_{}", "z".repeat(200)),
            epoch: u64::MAX,
            payload,
            handler_ms: Some(u64::MAX),
        };
        assert!(encode_message(&msg).is_ok());
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
