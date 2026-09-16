//! Host side of the bridge protocol (docs/protocol.md §A).
//!
//! ```text
//! handshake(stream)  : Hello  -> validate -> HelloAck (or HelloReject)
//! wait_ready(dl)     : Log* -> Ready | InitError | disconnect | timeout
//! send_invoke(..)    : Invoke
//! wait_result(dl)    : Log* -> Response | Error | Exited | disconnect | timeout
//! cancel(..)         : Cancel
//! shutdown(..)       : Shutdown
//! ```
//!
//! Only `Response` / `Error` frames whose `(attempt_id, epoch)` match the
//! active lease are accepted; anything else is counted as stale and ignored.
//!
//! A failed write does not end the read side: frames the guest queued before
//! closing (typically `Exited`) can still be read, so an `Invoke` that could
//! not be delivered is classified from what the guest reported rather than
//! as a lost connection. A frame that cannot be encoded (`FrameTooLarge`) is
//! refused before anything is written and leaves the session usable.
//! Heartbeats are ignored. Guest-reported timings are returned as
//! informational values only; the caller measures authoritative timings.
//! Log frames are forwarded to the [`LogRepository`] with per-line and
//! per-invocation bounds.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed;

use tachyon_serverless_domain::{
    AttemptId, Clock, EnvironmentId, InvocationId, LogPhase, LogRecord, LogStream, TenantId,
};
use tachyon_serverless_protocol::{
    FrameCodec, GuestErrorKind, GuestMessage, HostMessage, PROTOCOL_VERSION, ProtocolError,
    decode_message, encode_message,
};
use tachyon_serverless_provider_port::BridgeStream;

use crate::repository::{AppendOutcome, LogRepository};

/// Everything the host puts into `HelloAck`. `env` may contain resolved
/// secrets: `Debug` redacts it and it must never be logged.
#[derive(Clone)]
pub struct HelloAckParams {
    pub entrypoint: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: String,
    pub init_timeout: Duration,
    pub max_response_bytes: u64,
    pub max_log_line_bytes: u64,
}

impl std::fmt::Debug for HelloAckParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HelloAckParams")
            .field("entrypoint", &self.entrypoint)
            .field("args", &self.args)
            .field("env", &format_args!("<{} vars, redacted>", self.env.len()))
            .field("working_dir", &self.working_dir)
            .field("init_timeout", &self.init_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_log_line_bytes", &self.max_log_line_bytes)
            .finish()
    }
}

/// What the guest told us in `Hello`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloInfo {
    pub protocol_version: u32,
    pub bridge_version: String,
    pub guest_boot_id: Option<String>,
    pub architecture: String,
}

/// Guest-reported readiness (informational).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadyInfo {
    pub guest_init_ms: u64,
}

#[derive(Debug, Clone)]
pub struct InvokeParams {
    pub invocation_id: InvocationId,
    pub attempt_id: AttemptId,
    pub epoch: u64,
    pub event_type: String,
    /// Absolute deadline, milliseconds since Unix epoch.
    pub deadline_ms: u64,
    pub trace_id: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("handshake rejected: {0}")]
    HandshakeRejected(String),
    #[error("guest init error {error_type}: {message}")]
    InitError {
        error_type: String,
        message: String,
        exit_code: Option<i32>,
    },
    #[error("bridge disconnected")]
    Disconnected,
    #[error("timed out waiting for {stage}")]
    Timeout { stage: &'static str },
    #[error("protocol violation: {0}")]
    Protocol(String),
    /// A host frame exceeded `MAX_FRAME_BYTES`; nothing was written.
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
}

impl From<ProtocolError> for SessionError {
    fn from(e: ProtocolError) -> Self {
        match e {
            ProtocolError::Io(_) => Self::Disconnected,
            ProtocolError::FrameTooLarge(size) => Self::FrameTooLarge(size),
            other => Self::Protocol(other.to_string()),
        }
    }
}

/// Result of waiting for an attempt's outcome.
#[derive(Debug, Clone)]
pub enum Outcome {
    Response {
        payload: serde_json::Value,
        /// Guest-measured handler time, informational only.
        guest_handler_ms: Option<u64>,
    },
    GuestError {
        kind: GuestErrorKind,
        error_type: String,
        message: String,
        stack_trace: Option<String>,
        guest_handler_ms: Option<u64>,
    },
    /// The connection closed (or became unreadable) before a result arrived.
    Disconnected,
    /// The deadline elapsed before a result arrived.
    Timeout,
}

/// Where forwarded log lines are attributed.
#[derive(Debug, Clone)]
pub struct LogContext {
    pub tenant_id: TenantId,
    pub environment_id: EnvironmentId,
    pub invocation_id: Option<InvocationId>,
    pub max_line_bytes: usize,
}

/// Forwards guest log frames into the log repository.
#[derive(Clone)]
pub struct LogForwarder {
    repo: Arc<dyn LogRepository>,
    clock: Arc<dyn Clock>,
    ctx: LogContext,
}

impl LogForwarder {
    pub fn new(repo: Arc<dyn LogRepository>, clock: Arc<dyn Clock>, ctx: LogContext) -> Self {
        Self { repo, clock, ctx }
    }

    /// Append a platform-originated line (host side).
    pub fn platform(&self, phase: LogPhase, attempt_id: Option<&AttemptId>, line: &str) {
        self.push(LogStream::Platform, phase, attempt_id.cloned(), line);
    }

    fn push(
        &self,
        stream: LogStream,
        phase: LogPhase,
        attempt_id: Option<AttemptId>,
        line: &str,
    ) -> AppendOutcome {
        let (line, truncated) = LogRecord::bounded_line(line, self.ctx.max_line_bytes);
        self.repo.append(LogRecord {
            tenant_id: self.ctx.tenant_id.clone(),
            environment_id: self.ctx.environment_id.clone(),
            invocation_id: self.ctx.invocation_id.clone(),
            attempt_id,
            stream,
            phase,
            timestamp: self.clock.now(),
            line,
            truncated,
        })
    }

    fn forward_guest(
        &self,
        stream: tachyon_serverless_protocol::LogStream,
        phase: tachyon_serverless_protocol::LogPhase,
        attempt_id: Option<String>,
        line: &str,
    ) {
        use tachyon_serverless_protocol::{LogPhase as WP, LogStream as WS};
        let stream = match stream {
            WS::Stdout => LogStream::Stdout,
            WS::Stderr => LogStream::Stderr,
            WS::Bridge => LogStream::Platform,
        };
        let phase = match phase {
            WP::Init => LogPhase::Init,
            WP::Handler => LogPhase::Handler,
            WP::Shutdown => LogPhase::Shutdown,
        };
        let attempt_id = attempt_id.and_then(|s| AttemptId::parse(&s).ok());
        self.push(stream, phase, attempt_id, line);
    }
}

pub struct BridgeSession {
    framed: Framed<Box<dyn BridgeStream>, FrameCodec>,
    environment_id: EnvironmentId,
    epoch: u64,
    logs: LogForwarder,
    /// `(attempt_id, epoch)` of the active lease; results must match.
    lease: Option<(AttemptId, u64)>,
    stale_results: u32,
    /// No more frames can be read (EOF, IO or decode error, or closed by us).
    disconnected: bool,
    /// A write failed; frames already queued by the guest may still be read.
    write_closed: bool,
    /// The guest process reported `Exited`. The stream may still be open, but
    /// nothing behind it can serve another attempt.
    guest_exited: bool,
}

impl std::fmt::Debug for BridgeSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeSession")
            .field("environment_id", &self.environment_id)
            .field("epoch", &self.epoch)
            .field("lease", &self.lease)
            .field("stale_results", &self.stale_results)
            .finish_non_exhaustive()
    }
}

impl BridgeSession {
    /// Perform the `Hello` / `HelloAck` handshake within `timeout`.
    pub async fn handshake(
        stream: Box<dyn BridgeStream>,
        expected_environment_id: &EnvironmentId,
        epoch: u64,
        params: HelloAckParams,
        logs: LogForwarder,
        timeout: Duration,
    ) -> Result<(Self, HelloInfo), SessionError> {
        let mut session = Self {
            framed: Framed::new(stream, FrameCodec),
            environment_id: expected_environment_id.clone(),
            epoch,
            logs,
            lease: None,
            stale_results: 0,
            disconnected: false,
            write_closed: false,
            guest_exited: false,
        };
        let deadline = Instant::now() + timeout;
        let hello = loop {
            match session.next_message(deadline, "hello").await? {
                GuestMessage::Hello {
                    protocol_version,
                    bridge_version,
                    environment_id,
                    guest_boot_id,
                    architecture,
                } => {
                    break (
                        protocol_version,
                        bridge_version,
                        environment_id,
                        guest_boot_id,
                        architecture,
                    );
                }
                GuestMessage::Log {
                    stream,
                    phase,
                    attempt_id,
                    line,
                    ..
                } => session.logs.forward_guest(stream, phase, attempt_id, &line),
                GuestMessage::Heartbeat { .. } => {}
                other => {
                    let reason = format!("expected Hello, got {}", message_name(&other));
                    session.reject(&reason).await;
                    return Err(SessionError::HandshakeRejected(reason));
                }
            }
        };
        let (protocol_version, bridge_version, environment_id, guest_boot_id, architecture) = hello;
        if protocol_version != PROTOCOL_VERSION {
            let reason = format!(
                "unsupported protocol version {protocol_version} (host speaks {PROTOCOL_VERSION})"
            );
            session.reject(&reason).await;
            return Err(SessionError::HandshakeRejected(reason));
        }
        if environment_id != expected_environment_id.as_str() {
            let reason = format!(
                "environment id mismatch: guest says {environment_id}, host expected {expected_environment_id}"
            );
            session.reject(&reason).await;
            return Err(SessionError::HandshakeRejected(reason));
        }
        let ack = HostMessage::HelloAck {
            environment_id: expected_environment_id.to_string(),
            epoch,
            entrypoint: params.entrypoint,
            args: params.args,
            env: params.env,
            working_dir: params.working_dir,
            init_timeout_ms: params.init_timeout.as_millis() as u64,
            max_response_bytes: params.max_response_bytes,
            max_log_line_bytes: params.max_log_line_bytes,
        };
        session.send(&ack).await?;
        Ok((
            session,
            HelloInfo {
                protocol_version,
                bridge_version,
                guest_boot_id,
                architecture,
            },
        ))
    }

    pub fn environment_id(&self) -> &EnvironmentId {
        &self.environment_id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Number of `Response` / `Error` frames ignored because they did not
    /// match the active lease.
    pub fn stale_results(&self) -> u32 {
        self.stale_results
    }

    pub fn logs(&self) -> &LogForwarder {
        &self.logs
    }

    /// True while the session can still carry another attempt. A session whose
    /// stream is gone in either direction, or whose guest process has exited,
    /// must never go back into the pool.
    pub fn is_usable(&self) -> bool {
        !self.disconnected && !self.write_closed && !self.guest_exited
    }

    /// Consume what the finished attempt left in the read buffer, so that it
    /// cannot settle the next attempt on this session.
    ///
    /// Only frames that are *already* readable are taken (the deadline is
    /// now). `Log` lines are forwarded under the context currently installed —
    /// the one of the attempt they belong to, which is why this runs before
    /// [`Self::rearm`]; `Response` and `Error` frames are counted as stale;
    /// and an `Exited` frame or an EOF makes the session unusable, so the pool
    /// refuses it and the caller terminates the environment instead. `Exited`
    /// carries no attempt id or epoch, so draining it here is the only thing
    /// that keeps [`Self::wait_result`] from reporting it as the *next*
    /// attempt's crash.
    ///
    /// Returns the number of frames drained.
    pub async fn drain_stale(&mut self) -> u32 {
        let deadline = Instant::now();
        let mut drained = 0;
        while self.is_usable() {
            match self.next_message(deadline, "stale").await {
                Ok(GuestMessage::Log {
                    stream,
                    phase,
                    attempt_id,
                    line,
                    ..
                }) => self.logs.forward_guest(stream, phase, attempt_id, &line),
                Ok(GuestMessage::Response {
                    attempt_id, epoch, ..
                })
                | Ok(GuestMessage::Error {
                    attempt_id, epoch, ..
                }) => {
                    // Whatever the lease still says, the attempt this belongs
                    // to is already settled.
                    self.stale_results += 1;
                    tracing::warn!(
                        environment_id = %self.environment_id,
                        %attempt_id,
                        epoch,
                        "dropping a result the finished attempt left behind"
                    );
                }
                Ok(GuestMessage::Exited { exit_code, signal }) => {
                    self.guest_exited = true;
                    tracing::info!(
                        environment_id = %self.environment_id,
                        ?exit_code,
                        ?signal,
                        "the guest process exited; this session cannot carry another attempt"
                    );
                    return drained + 1;
                }
                Ok(_) => {}
                // `Timeout` means nothing more is buffered; anything else has
                // already marked the session disconnected.
                Err(_) => return drained,
            }
            drained += 1;
        }
        drained
    }

    /// Re-point a pooled session at the next attempt: record the environment's
    /// new epoch, attribute log lines to the new invocation and drop the
    /// previous lease, so a frame the previous attempt left behind is counted
    /// as stale instead of settling the new one.
    pub fn rearm(&mut self, epoch: u64, logs: LogForwarder) {
        self.epoch = epoch;
        self.logs = logs;
        self.lease = None;
    }

    /// Wait for `Ready` until `deadline`.
    pub async fn wait_ready(&mut self, deadline: Instant) -> Result<ReadyInfo, SessionError> {
        loop {
            match self.next_message(deadline, "ready").await? {
                GuestMessage::Ready { init_ms } => {
                    return Ok(ReadyInfo {
                        guest_init_ms: init_ms,
                    });
                }
                GuestMessage::InitError {
                    error_type,
                    message,
                    exit_code,
                } => {
                    return Err(SessionError::InitError {
                        error_type,
                        message,
                        exit_code,
                    });
                }
                GuestMessage::Exited { exit_code, signal } => {
                    return Err(SessionError::InitError {
                        error_type: "Runtime.Exited".into(),
                        message: format!(
                            "user process exited before Ready (exit_code={exit_code:?}, signal={signal:?})"
                        ),
                        exit_code,
                    });
                }
                GuestMessage::Log {
                    stream,
                    phase,
                    attempt_id,
                    line,
                    ..
                } => self.logs.forward_guest(stream, phase, attempt_id, &line),
                GuestMessage::Heartbeat { .. } => {}
                other => {
                    tracing::warn!(
                        environment_id = %self.environment_id,
                        message = message_name(&other),
                        "unexpected frame before Ready; ignored"
                    );
                }
            }
        }
    }

    /// Send `Invoke` and arm the lease for `(attempt_id, epoch)`.
    ///
    /// - `FrameTooLarge` / `Protocol`: the frame could not be encoded. Nothing
    ///   was written, the lease is not armed and the session stays usable.
    /// - `Disconnected`: the write failed, so the guest cannot have received a
    ///   complete `Invoke`. [`Self::wait_result`] still returns frames the
    ///   guest queued before closing (e.g. `Exited`).
    pub async fn send_invoke(&mut self, params: InvokeParams) -> Result<(), SessionError> {
        self.ensure_writable()?;
        let attempt_id = params.attempt_id.clone();
        let epoch = params.epoch;
        let bytes = encode_message(&HostMessage::Invoke {
            invocation_id: params.invocation_id.to_string(),
            attempt_id: params.attempt_id.to_string(),
            epoch: params.epoch,
            event_type: params.event_type,
            deadline_ms: params.deadline_ms,
            trace_id: params.trace_id,
            payload: params.payload,
        })?;
        self.lease = Some((attempt_id, epoch));
        self.write_frame(bytes).await
    }

    /// Wait for the outcome of the active lease until `deadline`. Cancel-safe:
    /// dropping the future keeps partially read frames in the buffer.
    pub async fn wait_result(&mut self, deadline: Instant) -> Outcome {
        loop {
            let msg = match self.next_message(deadline, "result").await {
                Ok(m) => m,
                Err(SessionError::Timeout { .. }) => return Outcome::Timeout,
                Err(_) => return Outcome::Disconnected,
            };
            match msg {
                GuestMessage::Response {
                    attempt_id,
                    epoch,
                    payload,
                    handler_ms,
                } => {
                    if self.accepts(&attempt_id, epoch) {
                        return Outcome::Response {
                            payload,
                            guest_handler_ms: handler_ms,
                        };
                    }
                }
                GuestMessage::Error {
                    attempt_id,
                    epoch,
                    error,
                    error_type,
                    message,
                    stack_trace,
                    handler_ms,
                } => {
                    if self.accepts(&attempt_id, epoch) {
                        return Outcome::GuestError {
                            kind: error,
                            error_type,
                            message,
                            stack_trace,
                            guest_handler_ms: handler_ms,
                        };
                    }
                }
                GuestMessage::Exited { exit_code, signal } => {
                    return Outcome::GuestError {
                        kind: GuestErrorKind::Crash { exit_code, signal },
                        error_type: "Runtime.Exited".into(),
                        message: format!(
                            "user process exited without a result (exit_code={exit_code:?}, signal={signal:?})"
                        ),
                        stack_trace: None,
                        guest_handler_ms: None,
                    };
                }
                GuestMessage::Log {
                    stream,
                    phase,
                    attempt_id,
                    line,
                    ..
                } => self.logs.forward_guest(stream, phase, attempt_id, &line),
                GuestMessage::Heartbeat { .. } | GuestMessage::Ready { .. } => {}
                other => {
                    tracing::warn!(
                        environment_id = %self.environment_id,
                        message = message_name(&other),
                        "unexpected frame while waiting for a result; ignored"
                    );
                }
            }
        }
    }

    /// Cooperative cancellation of the attempt. The caller still terminates
    /// the environment through the provider afterwards.
    pub async fn cancel(
        &mut self,
        attempt_id: &AttemptId,
        grace: Duration,
    ) -> Result<(), SessionError> {
        self.send(&HostMessage::Cancel {
            attempt_id: attempt_id.to_string(),
            grace_ms: grace.as_millis() as u64,
        })
        .await
    }

    /// After `Cancel`: keep forwarding log frames until the bridge closes the
    /// connection or `deadline` passes. Results are ignored (the host has
    /// already decided). Returns true when the guest closed in time.
    pub async fn drain_until_closed(&mut self, deadline: Instant) -> bool {
        loop {
            match self.next_message(deadline, "drain").await {
                Ok(GuestMessage::Log {
                    stream,
                    phase,
                    attempt_id,
                    line,
                    ..
                }) => self.logs.forward_guest(stream, phase, attempt_id, &line),
                Ok(_) => {}
                Err(SessionError::Timeout { .. }) => return false,
                Err(_) => return true,
            }
        }
    }

    /// Ask the bridge to shut down and close the stream.
    pub async fn shutdown(&mut self, reason: &str) -> Result<(), SessionError> {
        let r = self
            .send(&HostMessage::Shutdown {
                reason: reason.to_string(),
            })
            .await;
        let _ = self.framed.close().await;
        self.disconnected = true;
        r
    }

    fn accepts(&mut self, attempt_id: &str, epoch: u64) -> bool {
        let ok = match &self.lease {
            Some((att, ep)) => att.as_str() == attempt_id && *ep == epoch,
            None => false,
        };
        if !ok {
            self.stale_results += 1;
            tracing::warn!(
                environment_id = %self.environment_id,
                attempt_id,
                epoch,
                "ignoring result that does not match the active lease"
            );
        }
        ok
    }

    async fn reject(&mut self, reason: &str) {
        let _ = self
            .send(&HostMessage::HelloReject {
                reason: reason.to_string(),
            })
            .await;
        let _ = self.framed.close().await;
        self.disconnected = true;
    }

    fn ensure_writable(&self) -> Result<(), SessionError> {
        if self.disconnected || self.write_closed {
            Err(SessionError::Disconnected)
        } else {
            Ok(())
        }
    }

    async fn send(&mut self, msg: &HostMessage) -> Result<(), SessionError> {
        self.ensure_writable()?;
        let bytes = encode_message(msg)?;
        self.write_frame(bytes).await
    }

    async fn write_frame(&mut self, bytes: bytes::Bytes) -> Result<(), SessionError> {
        self.ensure_writable()?;
        match self.framed.send(bytes).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Only the write side is known to be gone; keep reading.
                self.write_closed = true;
                Err(e.into())
            }
        }
    }

    async fn next_message(
        &mut self,
        deadline: Instant,
        stage: &'static str,
    ) -> Result<GuestMessage, SessionError> {
        if self.disconnected {
            return Err(SessionError::Disconnected);
        }
        let next = tokio::time::timeout_at(deadline.into(), self.framed.next()).await;
        match next {
            Err(_) => Err(SessionError::Timeout { stage }),
            Ok(None) => {
                self.disconnected = true;
                Err(SessionError::Disconnected)
            }
            Ok(Some(Err(e))) => {
                self.disconnected = true;
                Err(e.into())
            }
            Ok(Some(Ok(frame))) => match decode_message::<GuestMessage>(&frame) {
                Ok(m) => Ok(m),
                Err(e) => {
                    self.disconnected = true;
                    Err(SessionError::Protocol(e.to_string()))
                }
            },
        }
    }
}

fn message_name(m: &GuestMessage) -> &'static str {
    match m {
        GuestMessage::Hello { .. } => "hello",
        GuestMessage::Ready { .. } => "ready",
        GuestMessage::InitError { .. } => "init_error",
        GuestMessage::Log { .. } => "log",
        GuestMessage::Response { .. } => "response",
        GuestMessage::Error { .. } => "error",
        GuestMessage::Exited { .. } => "exited",
        GuestMessage::Heartbeat { .. } => "heartbeat",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryStore;
    use tachyon_serverless_domain::{Limits, SystemClock};
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf};
    use tokio_util::codec::{FramedRead, FramedWrite};

    struct Guest {
        r: FramedRead<ReadHalf<DuplexStream>, FrameCodec>,
        w: FramedWrite<WriteHalf<DuplexStream>, FrameCodec>,
    }

    impl Guest {
        fn new(s: DuplexStream) -> Self {
            let (r, w) = tokio::io::split(s);
            Self {
                r: FramedRead::new(r, FrameCodec),
                w: FramedWrite::new(w, FrameCodec),
            }
        }
        async fn send(&mut self, m: &GuestMessage) {
            self.w.send(encode_message(m).unwrap()).await.unwrap();
        }
        async fn recv(&mut self) -> Option<HostMessage> {
            let f = self.r.next().await?.ok()?;
            Some(decode_message(&f).unwrap())
        }
    }

    fn setup(env: &EnvironmentId) -> (Arc<InMemoryStore>, LogForwarder, InvocationId) {
        let store = Arc::new(InMemoryStore::new(Limits {
            max_log_lines_per_invocation: 2,
            ..Limits::default()
        }));
        let inv = InvocationId::generate();
        let logs = LogForwarder::new(
            store.clone(),
            Arc::new(SystemClock),
            LogContext {
                tenant_id: TenantId::generate(),
                environment_id: env.clone(),
                invocation_id: Some(inv.clone()),
                max_line_bytes: 8,
            },
        );
        (store, logs, inv)
    }

    fn params() -> HelloAckParams {
        HelloAckParams {
            entrypoint: "/function/app".into(),
            args: vec![],
            env: vec![("SECRET".into(), "s3cr3t".into())],
            working_dir: "/tmp".into(),
            init_timeout: Duration::from_secs(1),
            max_response_bytes: 1024,
            max_log_line_bytes: 8,
        }
    }

    fn hello(env: &EnvironmentId) -> GuestMessage {
        GuestMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            bridge_version: "t".into(),
            environment_id: env.to_string(),
            guest_boot_id: Some("boot".into()),
            architecture: "aarch64".into(),
        }
    }

    #[test]
    fn hello_ack_params_debug_redacts_env() {
        let s = format!("{:?}", params());
        assert!(!s.contains("s3cr3t"));
        assert!(s.contains("redacted"));
    }

    #[tokio::test]
    async fn full_session_with_lease_fencing_and_log_bounds() {
        let (host, guest) = tokio::io::duplex(64 * 1024);
        let env = EnvironmentId::generate();
        let (store, logs, inv) = setup(&env);
        let mut g = Guest::new(guest);
        let env2 = env.clone();
        let guest_task = tokio::spawn(async move {
            g.send(&hello(&env2)).await;
            let ack = g.recv().await.unwrap();
            assert!(matches!(ack, HostMessage::HelloAck { epoch: 7, .. }));
            g.send(&GuestMessage::Log {
                stream: tachyon_serverless_protocol::LogStream::Stdout,
                phase: tachyon_serverless_protocol::LogPhase::Init,
                attempt_id: None,
                ts_ms: 0,
                line: "a very long init line".into(),
            })
            .await;
            g.send(&GuestMessage::Ready { init_ms: 3 }).await;
            let HostMessage::Invoke {
                attempt_id, epoch, ..
            } = g.recv().await.unwrap()
            else {
                panic!("expected invoke")
            };
            // stale: wrong epoch, then wrong attempt, then the real one
            g.send(&GuestMessage::Response {
                attempt_id: attempt_id.clone(),
                epoch: epoch + 1,
                payload: serde_json::json!("stale"),
                handler_ms: None,
            })
            .await;
            g.send(&GuestMessage::Response {
                attempt_id: AttemptId::generate().to_string(),
                epoch,
                payload: serde_json::json!("stale"),
                handler_ms: None,
            })
            .await;
            g.send(&GuestMessage::Heartbeat { ts_ms: 1 }).await;
            for i in 0..3 {
                g.send(&GuestMessage::Log {
                    stream: tachyon_serverless_protocol::LogStream::Stderr,
                    phase: tachyon_serverless_protocol::LogPhase::Handler,
                    attempt_id: Some(attempt_id.clone()),
                    ts_ms: 0,
                    line: format!("l{i}"),
                })
                .await;
            }
            g.send(&GuestMessage::Response {
                attempt_id,
                epoch,
                payload: serde_json::json!({"ok": 1}),
                handler_ms: Some(9),
            })
            .await;
            assert!(matches!(
                g.recv().await.unwrap(),
                HostMessage::Shutdown { .. }
            ));
        });

        let (mut s, info) = BridgeSession::handshake(
            Box::new(host),
            &env,
            7,
            params(),
            logs,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(info.guest_boot_id.as_deref(), Some("boot"));
        let ready = s
            .wait_ready(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(ready.guest_init_ms, 3);
        let att = AttemptId::generate();
        s.send_invoke(InvokeParams {
            invocation_id: inv.clone(),
            attempt_id: att.clone(),
            epoch: 7,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 0,
            trace_id: "t".into(),
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
        match s.wait_result(Instant::now() + Duration::from_secs(1)).await {
            Outcome::Response {
                payload,
                guest_handler_ms,
            } => {
                assert_eq!(payload, serde_json::json!({"ok": 1}));
                assert_eq!(guest_handler_ms, Some(9));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(s.stale_results(), 2);
        s.shutdown("done").await.unwrap();
        guest_task.await.unwrap();

        let q = store.query(&inv);
        assert_eq!(q.records.len(), 2, "bounded to 2 lines");
        assert!(q.dropped);
        assert!(q.records[0].truncated, "line cut at 8 bytes");
        assert_eq!(q.records[0].line, "a very l");
        assert_eq!(q.records[0].phase, LogPhase::Init);
        assert_eq!(q.records[1].attempt_id, Some(att));
        assert_eq!(q.records[1].stream, LogStream::Stderr);
    }

    #[tokio::test]
    async fn rejects_wrong_environment_and_protocol_version() {
        let (host, guest) = tokio::io::duplex(1024);
        let env = EnvironmentId::generate();
        let (_store, logs, _) = setup(&env);
        let mut g = Guest::new(guest);
        let t = tokio::spawn(async move {
            g.send(&hello(&EnvironmentId::generate())).await;
            g.recv().await
        });
        let err = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs,
            Duration::from_secs(1),
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(err, SessionError::HandshakeRejected(_)));
        assert!(matches!(
            t.await.unwrap(),
            Some(HostMessage::HelloReject { .. })
        ));
    }

    #[tokio::test]
    async fn init_error_timeout_and_disconnect() {
        // init error
        let (host, guest) = tokio::io::duplex(1024);
        let env = EnvironmentId::generate();
        let (_s, logs, _) = setup(&env);
        let mut g = Guest::new(guest);
        let e2 = env.clone();
        tokio::spawn(async move {
            g.send(&hello(&e2)).await;
            g.recv().await.unwrap();
            g.send(&GuestMessage::InitError {
                error_type: "Runtime.InitError".into(),
                message: "no".into(),
                exit_code: Some(3),
            })
            .await;
        });
        let (mut s, _) = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs.clone(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(matches!(
            s.wait_ready(Instant::now() + Duration::from_secs(1)).await,
            Err(SessionError::InitError { .. })
        ));

        // timeout waiting for hello
        let (host, _guest_keep) = tokio::io::duplex(1024);
        let err = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs.clone(),
            Duration::from_millis(50),
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(err, SessionError::Timeout { stage: "hello" }));

        // disconnect while waiting for a result
        let (host, guest) = tokio::io::duplex(1024);
        let mut g = Guest::new(guest);
        let e2 = env.clone();
        tokio::spawn(async move {
            g.send(&hello(&e2)).await;
            g.recv().await.unwrap();
            g.send(&GuestMessage::Ready { init_ms: 1 }).await;
            let _ = g.recv().await; // invoke
            // drop -> EOF on host
        });
        let (mut s, _) = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        s.wait_ready(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        s.send_invoke(InvokeParams {
            invocation_id: InvocationId::generate(),
            attempt_id: AttemptId::generate(),
            epoch: 1,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 0,
            trace_id: "t".into(),
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
        assert!(matches!(
            s.wait_result(Instant::now() + Duration::from_secs(1)).await,
            Outcome::Disconnected
        ));
    }

    fn invoke_params(payload: serde_json::Value) -> InvokeParams {
        InvokeParams {
            invocation_id: InvocationId::generate(),
            attempt_id: AttemptId::generate(),
            epoch: 1,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 0,
            trace_id: "t".into(),
            payload,
        }
    }

    #[tokio::test]
    async fn failed_invoke_write_still_reads_what_the_guest_queued() {
        let (host, guest) = tokio::io::duplex(64 * 1024);
        let env = EnvironmentId::generate();
        let (_s, logs, _) = setup(&env);
        let mut g = Guest::new(guest);
        let e2 = env.clone();
        let guest_task = tokio::spawn(async move {
            g.send(&hello(&e2)).await;
            g.recv().await.unwrap();
            g.send(&GuestMessage::Ready { init_ms: 1 }).await;
            g.send(&GuestMessage::Exited {
                exit_code: Some(0),
                signal: None,
            })
            .await;
            // dropping `g` closes both directions
        });
        let (mut s, _) = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        s.wait_ready(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        guest_task.await.unwrap();

        let err = s
            .send_invoke(invoke_params(serde_json::json!({})))
            .await
            .unwrap_err();
        assert!(matches!(err, SessionError::Disconnected), "{err}");
        match s.wait_result(Instant::now() + Duration::from_secs(1)).await {
            Outcome::GuestError {
                kind: GuestErrorKind::Crash { exit_code, .. },
                error_type,
                ..
            } => {
                assert_eq!(error_type, "Runtime.Exited");
                assert_eq!(exit_code, Some(0));
            }
            other => panic!("expected the queued Exited frame, got {other:?}"),
        }
        assert!(matches!(
            s.shutdown("done").await,
            Err(SessionError::Disconnected)
        ));
    }

    /// Regression (review F6): what the finished attempt left in the buffer is
    /// drained under *its* context, and an `Exited` frame — which carries no
    /// attempt id or epoch, so the lease cannot fence it — makes the session
    /// unusable instead of settling the next attempt.
    #[tokio::test]
    async fn draining_attributes_trailing_frames_to_the_attempt_that_left_them() {
        let (host, guest) = tokio::io::duplex(64 * 1024);
        let env = EnvironmentId::generate();
        let (store, logs, inv) = setup(&env);
        let mut g = Guest::new(guest);
        let e2 = env.clone();
        let guest_task = tokio::spawn(async move {
            g.send(&hello(&e2)).await;
            g.recv().await.unwrap();
            g.send(&GuestMessage::Ready { init_ms: 1 }).await;
            let HostMessage::Invoke {
                attempt_id, epoch, ..
            } = g.recv().await.unwrap()
            else {
                panic!("expected invoke")
            };
            g.send(&GuestMessage::Response {
                attempt_id: attempt_id.clone(),
                epoch,
                payload: serde_json::json!({"ok": 1}),
                handler_ms: None,
            })
            .await;
            // Everything below is what the finished attempt leaves behind.
            g.send(&GuestMessage::Log {
                stream: tachyon_serverless_protocol::LogStream::Stdout,
                phase: tachyon_serverless_protocol::LogPhase::Handler,
                attempt_id: Some(attempt_id.clone()),
                ts_ms: 0,
                line: "trailing".into(),
            })
            .await;
            g.send(&GuestMessage::Response {
                attempt_id,
                epoch,
                payload: serde_json::json!("late duplicate"),
                handler_ms: None,
            })
            .await;
            g.send(&GuestMessage::Exited {
                exit_code: Some(0),
                signal: None,
            })
            .await;
            // The stream stays open: only a read reveals the exit.
            std::future::pending::<()>().await;
        });

        let (mut s, _) = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        s.wait_ready(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let att = AttemptId::generate();
        s.send_invoke(InvokeParams {
            invocation_id: inv.clone(),
            attempt_id: att.clone(),
            epoch: 1,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 0,
            trace_id: "t".into(),
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
        assert!(matches!(
            s.wait_result(Instant::now() + Duration::from_secs(1)).await,
            Outcome::Response { .. }
        ));
        assert!(s.is_usable(), "nothing has been read past the result yet");

        let drained = s.drain_stale().await;
        assert_eq!(drained, 3, "log + duplicate result + exited");
        assert_eq!(s.stale_results(), 1, "the duplicate result was dropped");
        assert!(
            !s.is_usable(),
            "a guest that exited must never go back into the pool"
        );
        // The trailing line belongs to the invocation that produced it.
        let q = store.query(&inv);
        assert_eq!(q.records.len(), 1, "{:?}", q.records);
        assert_eq!(q.records[0].line, "trailing");
        assert_eq!(q.records[0].attempt_id, Some(att));
        assert_eq!(q.records[0].phase, LogPhase::Handler);
        guest_task.abort();
    }

    #[tokio::test]
    async fn oversized_invoke_is_refused_without_touching_the_session() {
        let (host, guest) = tokio::io::duplex(64 * 1024);
        let env = EnvironmentId::generate();
        let (_s, logs, _) = setup(&env);
        let mut g = Guest::new(guest);
        let e2 = env.clone();
        let guest_task = tokio::spawn(async move {
            g.send(&hello(&e2)).await;
            g.recv().await.unwrap();
            g.send(&GuestMessage::Ready { init_ms: 1 }).await;
            g.recv().await
        });
        let (mut s, _) = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        s.wait_ready(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let big = "a".repeat(tachyon_serverless_protocol::MAX_FRAME_BYTES + 1);
        let err = s
            .send_invoke(invoke_params(serde_json::json!({ "blob": big })))
            .await
            .unwrap_err();
        assert!(matches!(err, SessionError::FrameTooLarge(_)), "{err}");
        // Nothing was written and the session is still usable.
        s.shutdown("done").await.unwrap();
        assert!(matches!(
            guest_task.await.unwrap(),
            Some(HostMessage::Shutdown { .. })
        ));
    }
}
