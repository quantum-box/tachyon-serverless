//! One bridge session: handshake with the host, Runtime API server, user
//! process lifecycle and the frame loop (docs/protocol.md section A).
//!
//! Exit codes: 0 normal, 2 handshake rejected / protocol error, 3 init
//! error, 4 transport failure.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use tachyon_serverless_protocol::{
    FrameCodec, GuestErrorKind, GuestMessage, HostMessage, LogPhase, LogStream, MAX_FRAME_BYTES,
    MAX_RESPONSE_PAYLOAD_BYTES, PROTOCOL_VERSION, ProtocolError, decode_message, encode_message,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Sleep, timeout};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, info, warn};

use crate::process::{
    SIGKILL, SIGTERM, SpawnSpec, exit_parts, forward_logs, now_ms, signal_group, spawn_user,
};
use crate::runtime_api::{
    ApiEvent, ApiLimits, InvokeRequest, NoSnapshot, RestoreSource, RuntimeApi,
};
use crate::transport::BoxedHostStream;

/// Bridge process exit codes (docs/protocol.md section A).
pub mod exit_code {
    pub const OK: i32 = 0;
    pub const REJECTED: i32 = 2;
    pub const INIT_ERROR: i32 = 3;
    pub const TRANSPORT: i32 = 4;
}

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
/// Grace used when the bridge itself receives SIGTERM (process provider
/// terminate). Well below the process provider's TERMINATE_GRACE (3 s) so the
/// user process is gone before the bridge can be SIGKILLed. A SIGTERM that arrives while the
/// bridge is already stopping the user process escalates to SIGKILL at once
/// (see `terminate_user`).
const SIGTERM_GRACE: Duration = Duration::from_secs(1);
const LOG_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const OUTGOING_QUEUE: usize = 256;
/// Longest `error_type` kept when an unencodable error frame is rebuilt.
const MAX_SUBSTITUTE_ERROR_TYPE_BYTES: usize = 256;

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub environment_id: String,
    pub runtime_api_addr: SocketAddr,
    pub guest_boot_id: Option<String>,
    /// Forward `TACHYON_UNISOLATED=1` to the user process.
    pub unisolated: bool,
}

/// Variant name for logging. Never formats the message itself: `HelloAck`
/// carries secrets.
fn host_message_name(m: &HostMessage) -> &'static str {
    match m {
        HostMessage::HelloAck { .. } => "hello_ack",
        HostMessage::HelloReject { .. } => "hello_reject",
        HostMessage::Invoke { .. } => "invoke",
        HostMessage::Cancel { .. } => "cancel",
        HostMessage::Shutdown { .. } => "shutdown",
        HostMessage::Ping { .. } => "ping",
    }
}

/// The deadline handed to the user process, expressed in the **guest's** clock.
///
/// The host sends both its own absolute deadline and `remaining_ms`, the time
/// that was left when it wrote the frame. Only the second one can be used
/// here: a guest that was quiesced has a clock that stopped with it, so after
/// a resume of arbitrary length the host's absolute timestamp is ahead of the
/// guest's clock by the whole pause and the user process would compute a
/// deadline that is too generous by that much (PLT-4633 review F5). `now +
/// remaining` is correct whether the guest was paused or not, and the host
/// stays the authority: it enforces the real deadline and cancels regardless.
fn guest_deadline_ms(now_ms: u64, remaining_ms: u64) -> u64 {
    now_ms.saturating_add(remaining_ms)
}

/// Variant name of an outgoing frame for logging.
fn guest_message_name(m: &GuestMessage) -> &'static str {
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
    }
}

/// Launch parameters extracted from `HelloAck`. `env` is secret-bearing.
struct Launch {
    epoch: u64,
    entrypoint: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    working_dir: String,
    init_timeout_ms: u64,
    max_response_bytes: u64,
    max_log_line_bytes: u64,
}

enum Outcome {
    UserExited(std::process::ExitStatus),
    InitError,
    InitTimeout,
    Shutdown(String),
    /// The bridge process itself was asked to terminate.
    Terminated,
    TransportLost(String),
    ProtocolError(String),
}

/// Run a full session on an already connected host stream.
pub async fn run_session(stream: BoxedHostStream, cfg: SessionConfig) -> i32 {
    run_session_with(stream, cfg, Arc::new(NoSnapshot)).await
}

/// [`run_session`] with an explicit answer source for the experimental
/// lifecycle's `continue` (PLT-4651). No provider takes snapshots yet, so the
/// binary always uses [`NoSnapshot`]; tests inject a mock restore
/// notification here.
pub async fn run_session_with(
    stream: BoxedHostStream,
    cfg: SessionConfig,
    restore: Arc<dyn RestoreSource>,
) -> i32 {
    let (rd, wr) = tokio::io::split(stream);
    let mut reader = FramedRead::new(rd, FrameCodec);
    let mut writer = FramedWrite::new(wr, FrameCodec);

    // --- handshake -------------------------------------------------------
    let hello = GuestMessage::Hello {
        protocol_version: PROTOCOL_VERSION,
        bridge_version: env!("CARGO_PKG_VERSION").to_string(),
        environment_id: cfg.environment_id.clone(),
        guest_boot_id: cfg.guest_boot_id.clone(),
        architecture: std::env::consts::ARCH.to_string(),
    };
    if let Err(e) = send_direct(&mut writer, &hello).await {
        error!("cannot send hello: {e}");
        return exit_code::TRANSPORT;
    }
    let ack = match timeout(HANDSHAKE_TIMEOUT, reader.next()).await {
        Err(_) => {
            error!("timed out waiting for hello_ack");
            return exit_code::TRANSPORT;
        }
        Ok(None) => {
            error!("host closed the connection during handshake");
            return exit_code::TRANSPORT;
        }
        Ok(Some(Err(ProtocolError::Io(e)))) => {
            error!("transport error during handshake: {e}");
            return exit_code::TRANSPORT;
        }
        Ok(Some(Err(e))) => {
            error!("protocol error during handshake: {e}");
            return exit_code::REJECTED;
        }
        Ok(Some(Ok(frame))) => match decode_message::<HostMessage>(&frame) {
            Ok(m) => m,
            Err(e) => {
                error!("undecodable handshake frame: {e}");
                return exit_code::REJECTED;
            }
        },
    };
    let launch = match ack {
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
        } => {
            if environment_id != cfg.environment_id {
                error!(
                    expected = %cfg.environment_id,
                    got = %environment_id,
                    "hello_ack is for a different environment"
                );
                return exit_code::REJECTED;
            }
            Launch {
                epoch,
                entrypoint,
                args,
                env,
                working_dir,
                init_timeout_ms,
                max_response_bytes,
                max_log_line_bytes,
            }
        }
        HostMessage::HelloReject { reason } => {
            error!("handshake rejected by host: {reason}");
            return exit_code::REJECTED;
        }
        other => {
            error!(
                "unexpected {} frame during handshake",
                host_message_name(&other)
            );
            return exit_code::REJECTED;
        }
    };
    info!(
        environment_id = %cfg.environment_id,
        epoch = launch.epoch,
        entrypoint = %launch.entrypoint,
        args = launch.args.len(),
        env_vars = launch.env.len(),
        working_dir = %launch.working_dir,
        init_timeout_ms = launch.init_timeout_ms,
        max_response_bytes = launch.max_response_bytes,
        "handshake complete"
    );
    // A response must fit one frame whatever the host configured.
    let max_response_bytes = launch.max_response_bytes.min(MAX_RESPONSE_PAYLOAD_BYTES);
    if max_response_bytes < launch.max_response_bytes {
        warn!(
            requested = launch.max_response_bytes,
            effective = max_response_bytes,
            "hello_ack max_response_bytes exceeds what one frame can carry; clamping"
        );
    }

    // --- outgoing frame writer ------------------------------------------
    let (out_tx, out_rx) = mpsc::channel::<GuestMessage>(OUTGOING_QUEUE);
    let writer_task = tokio::spawn(write_loop(writer, out_rx));

    // --- runtime api server ---------------------------------------------
    let (api_tx, mut api_rx) = mpsc::channel::<ApiEvent>(64);
    let process_started_at = Instant::now();
    let api = RuntimeApi::with_restore_source(
        api_tx,
        ApiLimits { max_response_bytes },
        process_started_at,
        restore,
    );
    let listener = match TcpListener::bind(cfg.runtime_api_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("cannot bind runtime api on {}: {e}", cfg.runtime_api_addr);
            let _ = out_tx
                .send(GuestMessage::InitError {
                    error_type: "Bridge.RuntimeApiBind".into(),
                    message: format!("cannot bind runtime api on {}: {e}", cfg.runtime_api_addr),
                    exit_code: None,
                })
                .await;
            drop(out_tx);
            let _ = writer_task.await;
            return exit_code::INIT_ERROR;
        }
    };
    let api_addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let runtime_api_url = format!("http://{api_addr}");
    let server = tokio::spawn(axum::serve(listener, api.router()).into_future());
    info!(runtime_api = %runtime_api_url, "runtime api listening");

    // --- user process ---------------------------------------------------
    let spawn_spec = SpawnSpec {
        entrypoint: &launch.entrypoint,
        args: &launch.args,
        env: &launch.env,
        working_dir: &launch.working_dir,
        runtime_api_url: &runtime_api_url,
        environment_id: &cfg.environment_id,
        unisolated: cfg.unisolated,
    };
    let mut child = match spawn_user(&spawn_spec) {
        Ok(c) => c,
        Err(e) => {
            error!("cannot spawn user process {}: {e}", launch.entrypoint);
            let _ = out_tx
                .send(GuestMessage::InitError {
                    error_type: "Runtime.SpawnError".into(),
                    message: format!("cannot spawn {}: {e}", launch.entrypoint),
                    exit_code: None,
                })
                .await;
            drop(out_tx);
            let _ = writer_task.await;
            server.abort();
            return exit_code::INIT_ERROR;
        }
    };
    drop(launch.env);
    let pid = child.id().unwrap_or(0);
    info!(pid, "user process started");
    bridge_log(
        &out_tx,
        LogPhase::Init,
        None,
        format!("user process started (pid {pid})"),
    )
    .await;

    let max_line = launch.max_log_line_bytes.clamp(16, 1024 * 1024) as usize;
    let mut log_tasks = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        log_tasks.push(tokio::spawn(forward_logs(
            stdout,
            LogStream::Stdout,
            max_line,
            api.clone(),
            out_tx.clone(),
        )));
    }
    if let Some(stderr) = child.stderr.take() {
        log_tasks.push(tokio::spawn(forward_logs(
            stderr,
            LogStream::Stderr,
            max_line,
            api.clone(),
            out_tx.clone(),
        )));
    }
    let (exit_tx, mut exit_rx) = oneshot::channel::<std::process::ExitStatus>();
    tokio::spawn(async move {
        if let Ok(status) = child.wait().await {
            let _ = exit_tx.send(status);
        }
    });

    // --- main loop ------------------------------------------------------
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
        HEARTBEAT_INTERVAL,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut kill_deadline: Option<Pin<Box<Sleep>>> = None;
    let mut init_deadline: Option<Pin<Box<Sleep>>> = (launch.init_timeout_ms > 0).then(|| {
        Box::pin(tokio::time::sleep(Duration::from_millis(
            launch.init_timeout_ms,
        )))
    });
    let mut sigterm = bridge_sigterm();

    let outcome = loop {
        tokio::select! {
            // Deterministic priority: host frames, then Runtime API events
            // (an init error posted just before the process exits must win
            // over the exit notification), then the child exit and timers.
            biased;
            frame = reader.next() => match frame {
                None => break Outcome::TransportLost("host closed the connection".into()),
                Some(Err(ProtocolError::Io(e))) => break Outcome::TransportLost(e.to_string()),
                Some(Err(e)) => break Outcome::ProtocolError(e.to_string()),
                Some(Ok(frame)) => match decode_message::<HostMessage>(&frame) {
                    Err(e) => break Outcome::ProtocolError(format!("undecodable host frame: {e}")),
                    Ok(HostMessage::Invoke { invocation_id, attempt_id, epoch, event_type, deadline_ms: host_deadline_ms, remaining_ms, trace_id, payload }) => {
                        // What the user process is told is derived from the
                        // time left, not from the host's absolute timestamp:
                        // this guest's clock may have been stopped while the
                        // environment was quiesced (see `guest_deadline_ms`).
                        let deadline_ms = guest_deadline_ms(now_ms(), remaining_ms);
                        let request = InvokeRequest { invocation_id, attempt_id, epoch, event_type, deadline_ms, trace_id, payload };
                        info!(attempt_id = %request.attempt_id, epoch, host_deadline_ms, remaining_ms, deadline_ms, "invoke received");
                        if let Err(rejected) = api.dispatch(request) {
                            warn!(attempt_id = %rejected.attempt_id, "invoke rejected: an attempt is already in flight");
                            let _ = out_tx.send(GuestMessage::Error {
                                attempt_id: rejected.attempt_id,
                                epoch: rejected.epoch,
                                error: GuestErrorKind::Protocol,
                                error_type: "Runtime.Busy".into(),
                                message: "an attempt is already in flight in this environment".into(),
                                stack_trace: None,
                                handler_ms: None,
                            }).await;
                        }
                    }
                    Ok(HostMessage::Cancel { attempt_id, grace_ms }) => {
                        info!(%attempt_id, grace_ms, "cancel received; sending SIGTERM");
                        bridge_log(&out_tx, LogPhase::Handler, Some(attempt_id.clone()),
                            format!("cancel requested (grace {grace_ms} ms)")).await;
                        signal_group(pid, SIGTERM);
                        kill_deadline = Some(Box::pin(tokio::time::sleep(Duration::from_millis(grace_ms))));
                    }
                    Ok(HostMessage::Shutdown { reason }) => break Outcome::Shutdown(reason),
                    // Liveness probe: answered by this loop and nothing else.
                    // The user process is not involved, so the answer means
                    // exactly "this guest is scheduled and the bridge is
                    // reading" — which is what the host needs to know after
                    // resuming a quiesced environment.
                    Ok(HostMessage::Ping { nonce }) => {
                        let _ = out_tx.send(GuestMessage::Pong { nonce }).await;
                    }
                    Ok(other) => warn!("ignoring unexpected {} frame after handshake", host_message_name(&other)),
                },
            },
            // The writer only stops on a transport I/O error. Nothing can
            // reach the host any more, so do not linger with a silent session.
            _ = out_tx.closed() => break Outcome::TransportLost("frame writer stopped".into()),
            Some(event) = api_rx.recv() => match event {
                ApiEvent::Ready { init_ms } => {
                    info!(init_ms, "user process ready");
                    init_deadline = None;
                    let _ = out_tx.send(GuestMessage::Ready { init_ms }).await;
                }
                ApiEvent::Continued { restored } => {
                    info!(restored, "experimental lifecycle: continue answered");
                    bridge_log(&out_tx, LogPhase::Init, None, format!(
                        "lifecycle continue answered ({})", if restored { "restored" } else { "cold" })).await;
                    // A restored copy starts a fresh init budget for its
                    // after-restore phase: the bootstrap time was spent by the
                    // process the snapshot was taken from. A cold start keeps
                    // the single P1 budget.
                    if restored && launch.init_timeout_ms > 0 {
                        init_deadline = Some(Box::pin(tokio::time::sleep(Duration::from_millis(launch.init_timeout_ms))));
                    }
                }
                ApiEvent::InitError { error_type, message } => {
                    error!(%error_type, %message, "user process reported init error");
                    let _ = out_tx.send(GuestMessage::InitError { error_type, message, exit_code: None }).await;
                    break Outcome::InitError;
                }
                ApiEvent::Response { attempt_id, epoch, payload, handler_ms } => {
                    info!(%attempt_id, ?handler_ms, "response received");
                    let _ = out_tx.send(GuestMessage::Response { attempt_id, epoch, payload, handler_ms }).await;
                }
                ApiEvent::Error { attempt_id, epoch, error, error_type, message, stack_trace, handler_ms } => {
                    info!(%attempt_id, %error_type, "error received");
                    let _ = out_tx.send(GuestMessage::Error { attempt_id, epoch, error, error_type, message, stack_trace, handler_ms }).await;
                }
            },
            status = &mut exit_rx => match status {
                Ok(status) => break Outcome::UserExited(status),
                Err(_) => break Outcome::TransportLost("child wait task vanished".into()),
            },
            _ = heartbeat.tick() => {
                let _ = out_tx.send(GuestMessage::Heartbeat { ts_ms: now_ms() }).await;
            }
            _ = async { kill_deadline.as_mut().expect("guarded").await }, if kill_deadline.is_some() => {
                warn!("cancel grace elapsed; sending SIGKILL");
                signal_group(pid, SIGKILL);
                kill_deadline = None;
            }
            _ = async { init_deadline.as_mut().expect("guarded").await }, if init_deadline.is_some() => {
                init_deadline = None;
                if !api.is_ready() {
                    break Outcome::InitTimeout;
                }
            }
            _ = sigterm.recv() => {
                warn!("bridge received SIGTERM; terminating user process");
                break Outcome::Terminated;
            }
        }
    };

    // --- wind down ------------------------------------------------------
    let code = match outcome {
        Outcome::UserExited(status) => {
            let (exit_code, signal) = exit_parts(&status);
            info!(?exit_code, ?signal, "user process exited");
            drain_logs(&mut log_tasks).await;
            if !api.is_ready() {
                let _ = out_tx
                    .send(GuestMessage::InitError {
                        error_type: "Runtime.InitExit".into(),
                        message: format!(
                            "user process exited before ready (exit_code={exit_code:?}, signal={signal:?})"
                        ),
                        exit_code,
                    })
                    .await;
                exit_code::INIT_ERROR
            } else {
                if let Some((attempt_id, epoch, handler_ms)) = api.take_in_flight() {
                    let _ = out_tx
                        .send(GuestMessage::Error {
                            attempt_id,
                            epoch,
                            error: GuestErrorKind::Crash { exit_code, signal },
                            error_type: "Runtime.Crash".into(),
                            message: format!(
                                "user process exited while the attempt was in flight (exit_code={exit_code:?}, signal={signal:?})"
                            ),
                            stack_trace: None,
                            handler_ms,
                        })
                        .await;
                }
                let _ = out_tx
                    .send(GuestMessage::Exited { exit_code, signal })
                    .await;
                exit_code::OK
            }
        }
        Outcome::InitError => {
            signal_group(pid, SIGKILL);
            wait_exit(&mut exit_rx, SHUTDOWN_GRACE).await;
            drain_logs(&mut log_tasks).await;
            exit_code::INIT_ERROR
        }
        Outcome::InitTimeout => {
            // Typed by the lifecycle phase (PLT-4651); `Runtime.InitTimeout`
            // with the P1 message when the process never opened one.
            let (error_type, pending) = api.init_timeout_classification();
            let message = if error_type == "Runtime.InitTimeout" {
                format!(
                    "user process did not become ready within {} ms",
                    launch.init_timeout_ms
                )
            } else {
                format!(
                    "user process did not finish {pending} within {} ms",
                    launch.init_timeout_ms
                )
            };
            error!(%error_type, "{message}");
            signal_group(pid, SIGKILL);
            wait_exit(&mut exit_rx, SHUTDOWN_GRACE).await;
            drain_logs(&mut log_tasks).await;
            let _ = out_tx
                .send(GuestMessage::InitError {
                    error_type: error_type.into(),
                    message,
                    exit_code: None,
                })
                .await;
            exit_code::INIT_ERROR
        }
        Outcome::Shutdown(reason) => {
            info!(%reason, "shutdown received");
            api.shutdown();
            terminate_user(pid, &mut exit_rx, SHUTDOWN_GRACE, &mut sigterm).await;
            drain_logs(&mut log_tasks).await;
            exit_code::OK
        }
        Outcome::Terminated => {
            api.shutdown();
            terminate_user(pid, &mut exit_rx, SIGTERM_GRACE, &mut sigterm).await;
            drain_logs(&mut log_tasks).await;
            exit_code::OK
        }
        Outcome::TransportLost(reason) => {
            error!(%reason, "host connection lost; killing user process");
            api.shutdown();
            signal_group(pid, SIGKILL);
            wait_exit(&mut exit_rx, SHUTDOWN_GRACE).await;
            drain_logs(&mut log_tasks).await;
            exit_code::TRANSPORT
        }
        Outcome::ProtocolError(reason) => {
            error!(%reason, "protocol error; killing user process");
            api.shutdown();
            signal_group(pid, SIGKILL);
            wait_exit(&mut exit_rx, SHUTDOWN_GRACE).await;
            drain_logs(&mut log_tasks).await;
            exit_code::REJECTED
        }
    };

    // Close: flush every queued frame, then let the socket go.
    drop(out_tx);
    for t in &log_tasks {
        t.abort();
    }
    match timeout(SHUTDOWN_GRACE, writer_task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => warn!("writer finished with error: {e}"),
        Ok(Err(e)) => warn!("writer task failed: {e}"),
        Err(_) => warn!("writer did not flush within the grace period"),
    }
    server.abort();
    code
}

async fn send_direct<W: AsyncWrite + Unpin>(
    writer: &mut FramedWrite<W, FrameCodec>,
    msg: &GuestMessage,
) -> Result<(), ProtocolError> {
    writer.send(encode_message(msg)?).await
}

/// Serialize and send every queued frame. Returns only when the queue is
/// closed or the transport fails: a frame that cannot be encoded is replaced
/// (see [`unencodable_substitute`]) or dropped, never allowed to silence the
/// frames queued behind it.
async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: FramedWrite<W, FrameCodec>,
    mut rx: mpsc::Receiver<GuestMessage>,
) -> Result<(), ProtocolError> {
    while let Some(msg) = rx.recv().await {
        let bytes = match encode_message(&msg) {
            Ok(bytes) => bytes,
            Err(e) => {
                let name = guest_message_name(&msg);
                let Some(substitute) = unencodable_substitute(&msg, &e) else {
                    error!(frame = name, "dropping unencodable frame: {e}");
                    continue;
                };
                error!(
                    frame = name,
                    "frame cannot be encoded ({e}); sending an error frame instead"
                );
                match encode_message(&substitute) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        error!(frame = name, "dropping unencodable substitute frame: {e}");
                        continue;
                    }
                }
            }
        };
        // Encoded frames are within MAX_FRAME_BYTES, so only transport I/O
        // can fail here: the connection is gone.
        writer.send(bytes).await?;
    }
    writer.close().await
}

/// Replacement for a frame that cannot be encoded, so the host still learns
/// how the attempt ended instead of waiting for its deadline. `None` for
/// frames without an attempt to settle.
fn unencodable_substitute(msg: &GuestMessage, err: &ProtocolError) -> Option<GuestMessage> {
    match msg {
        GuestMessage::Response {
            attempt_id,
            epoch,
            handler_ms,
            ..
        } => {
            let (error, error_type) = match err {
                ProtocolError::FrameTooLarge(size) => (
                    GuestErrorKind::ResponseTooLarge {
                        size_bytes: *size as u64,
                        max_bytes: MAX_FRAME_BYTES as u64,
                    },
                    "Runtime.ResponseTooLarge",
                ),
                _ => (GuestErrorKind::Protocol, "Runtime.InvalidResponse"),
            };
            Some(GuestMessage::Error {
                attempt_id: attempt_id.clone(),
                epoch: *epoch,
                error,
                error_type: error_type.into(),
                message: format!("response cannot be sent to the host: {err}"),
                stack_trace: None,
                handler_ms: *handler_ms,
            })
        }
        GuestMessage::Error {
            attempt_id,
            epoch,
            error,
            error_type,
            handler_ms,
            ..
        } => Some(GuestMessage::Error {
            attempt_id: attempt_id.clone(),
            epoch: *epoch,
            error: error.clone(),
            error_type: error_type
                [..error_type.floor_char_boundary(MAX_SUBSTITUTE_ERROR_TYPE_BYTES)]
                .to_string(),
            message: format!("error report cannot be sent to the host: {err}"),
            stack_trace: None,
            handler_ms: *handler_ms,
        }),
        _ => None,
    }
}

async fn bridge_log(
    out: &mpsc::Sender<GuestMessage>,
    phase: LogPhase,
    attempt_id: Option<String>,
    line: String,
) {
    let _ = out
        .send(GuestMessage::Log {
            stream: LogStream::Bridge,
            phase,
            attempt_id,
            ts_ms: now_ms(),
            line,
        })
        .await;
}

/// Wait for the child exit notification (already consumed = already gone).
async fn wait_exit(
    exit_rx: &mut oneshot::Receiver<std::process::ExitStatus>,
    limit: Duration,
) -> bool {
    timeout(limit, exit_rx).await.is_ok()
}

/// SIGTERM, wait `grace`, SIGKILL, wait again.
///
/// A SIGTERM delivered to the bridge meanwhile means its own SIGKILL is on
/// the way (the provider terminates the environment). The user process lives
/// in its own process group and would outlive the bridge, so escalate to
/// SIGKILL at once instead of finishing the grace period.
async fn terminate_user(
    pid: u32,
    exit_rx: &mut oneshot::Receiver<std::process::ExitStatus>,
    grace: Duration,
    sigterm: &mut impl SignalSource,
) {
    signal_group(pid, SIGTERM);
    tokio::select! {
        biased;
        _ = &mut *exit_rx => return,
        _ = sigterm.recv() => {
            warn!("bridge received SIGTERM while stopping the user process; sending SIGKILL now");
        }
        _ = tokio::time::sleep(grace) => warn!("user process ignored SIGTERM; sending SIGKILL"),
    }
    signal_group(pid, SIGKILL);
    wait_exit(exit_rx, SHUTDOWN_GRACE).await;
}

/// Let the log forwarders emit whatever is still buffered in the pipes.
async fn drain_logs(tasks: &mut Vec<tokio::task::JoinHandle<()>>) {
    for task in tasks.iter_mut() {
        if timeout(LOG_DRAIN_TIMEOUT, &mut *task).await.is_err() {
            // A grandchild may still hold the pipe; do not wait for it.
            task.abort();
        }
    }
    tasks.clear();
}

/// SIGTERM stream for the bridge process itself.
#[cfg(unix)]
fn bridge_sigterm() -> impl SignalSource {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok()
}

#[cfg(not(unix))]
fn bridge_sigterm() -> impl SignalSource {
    None::<()>
}

/// Minimal abstraction over an optional signal stream so the select! branch
/// compiles on every platform.
pub trait SignalSource {
    fn recv(&mut self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
}

#[cfg(unix)]
impl SignalSource for Option<tokio::signal::unix::Signal> {
    fn recv(&mut self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            match self {
                Some(sig) => {
                    sig.recv().await;
                }
                None => std::future::pending::<()>().await,
            }
        })
    }
}

#[cfg(not(unix))]
impl SignalSource for Option<()> {
    fn recv(&mut self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::pending::<()>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use tachyon_serverless_protocol::runtime_api::lifecycle::Continuation;
    use tokio::io::{AsyncRead, DuplexStream, ReadBuf};
    use tokio_util::codec::Framed;

    const T: Duration = Duration::from_secs(10);

    async fn next_frame(host: &mut FramedRead<DuplexStream, FrameCodec>) -> Option<GuestMessage> {
        let frame = timeout(T, host.next()).await.expect("frame timeout")?;
        Some(decode_message(&frame.expect("valid frame")).expect("decodable frame"))
    }

    /// PLT-4633 (review F5): the deadline the user process sees is computed
    /// from the time the host said was left, in this guest's own clock.
    ///
    /// A guest that was quiesced for a while comes back with a clock that is
    /// behind the host's by the whole pause. Handing it the host's absolute
    /// deadline would give the handler that pause for free — it would compute
    /// "deadline − my now" and get too much — while the host cancels it at the
    /// real deadline.
    #[test]
    fn a_resumed_guest_gets_a_deadline_in_its_own_clock() {
        // Clocks in step: the two ways of computing it agree.
        let host_now = 1_700_000_000_000u64;
        let remaining = 30_000u64;
        assert_eq!(
            guest_deadline_ms(host_now, remaining),
            host_now + remaining,
            "an unpaused guest is unaffected"
        );

        // Paused for a minute: the guest's clock stopped with it.
        let guest_now = host_now - 60_000;
        let host_deadline = host_now + remaining;
        let guest_deadline = guest_deadline_ms(guest_now, remaining);
        assert_eq!(guest_deadline, guest_now + remaining);
        assert_eq!(
            guest_deadline.saturating_sub(guest_now),
            remaining,
            "the guest computes exactly the time the host said was left"
        );
        assert_eq!(
            host_deadline.saturating_sub(guest_now),
            remaining + 60_000,
            "which the host's absolute deadline would have overstated by the pause"
        );

        // Degenerate inputs stay sane: no time left, and no overflow.
        assert_eq!(guest_deadline_ms(guest_now, 0), guest_now);
        assert_eq!(guest_deadline_ms(u64::MAX, 5), u64::MAX);
    }

    #[tokio::test]
    async fn write_loop_replaces_unencodable_frames_and_keeps_writing() {
        let (bridge_end, host_end) = tokio::io::duplex(64 * 1024);
        let (tx, rx) = mpsc::channel(8);
        let writer = tokio::spawn(write_loop(FramedWrite::new(bridge_end, FrameCodec), rx));
        let mut host = FramedRead::new(host_end, FrameCodec);

        tx.send(GuestMessage::Response {
            attempt_id: "att_big".into(),
            epoch: 7,
            payload: serde_json::Value::String("x".repeat(MAX_FRAME_BYTES)),
            handler_ms: Some(5),
        })
        .await
        .unwrap();
        tx.send(GuestMessage::Heartbeat { ts_ms: 1 }).await.unwrap();
        tx.send(GuestMessage::Error {
            attempt_id: "att_err".into(),
            epoch: 8,
            error: GuestErrorKind::Handler,
            error_type: "Handler.Error".into(),
            message: "m".repeat(MAX_FRAME_BYTES),
            stack_trace: Some("st".into()),
            handler_ms: None,
        })
        .await
        .unwrap();
        tx.send(GuestMessage::Heartbeat { ts_ms: 2 }).await.unwrap();

        match next_frame(&mut host).await {
            Some(GuestMessage::Error {
                attempt_id,
                epoch,
                error,
                error_type,
                handler_ms,
                ..
            }) => {
                assert_eq!(attempt_id, "att_big");
                assert_eq!(epoch, 7);
                assert!(
                    matches!(
                        error,
                        GuestErrorKind::ResponseTooLarge { size_bytes, max_bytes }
                            if max_bytes == MAX_FRAME_BYTES as u64 && size_bytes > max_bytes
                    ),
                    "{error:?}"
                );
                assert_eq!(error_type, "Runtime.ResponseTooLarge");
                assert_eq!(handler_ms, Some(5));
            }
            other => panic!("expected a response_too_large error frame, got {other:?}"),
        }
        assert_eq!(
            next_frame(&mut host).await,
            Some(GuestMessage::Heartbeat { ts_ms: 1 })
        );
        match next_frame(&mut host).await {
            Some(GuestMessage::Error {
                attempt_id,
                error,
                error_type,
                message,
                stack_trace,
                ..
            }) => {
                assert_eq!(attempt_id, "att_err");
                assert_eq!(error, GuestErrorKind::Handler);
                assert_eq!(error_type, "Handler.Error");
                assert!(message.contains("cannot be sent"), "{message}");
                assert!(stack_trace.is_none());
            }
            other => panic!("expected a rebuilt error frame, got {other:?}"),
        }
        assert_eq!(
            next_frame(&mut host).await,
            Some(GuestMessage::Heartbeat { ts_ms: 2 })
        );

        drop(tx);
        timeout(T, writer).await.unwrap().unwrap().unwrap();
        assert!(next_frame(&mut host).await.is_none());
    }

    /// Host stream whose writes fail on demand while reads keep working: a
    /// transport that is dead in one direction only.
    struct WriteBroken {
        inner: DuplexStream,
        fail_writes: Arc<AtomicBool>,
    }

    impl WriteBroken {
        fn broken(&self) -> Option<std::io::Error> {
            self.fail_writes
                .load(Ordering::SeqCst)
                .then(|| std::io::ErrorKind::BrokenPipe.into())
        }
    }

    impl AsyncRead for WriteBroken {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for WriteBroken {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            match this.broken() {
                Some(e) => Poll::Ready(Err(e)),
                None => Pin::new(&mut this.inner).poll_write(cx, buf),
            }
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            match this.broken() {
                Some(e) => Poll::Ready(Err(e)),
                None => Pin::new(&mut this.inner).poll_flush(cx),
            }
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    /// Mock restore notification for the experimental lifecycle (PLT-4651).
    struct MockRestore {
        release: tokio::sync::Mutex<Option<oneshot::Receiver<Continuation>>>,
    }

    impl RestoreSource for MockRestore {
        fn continuation(
            &self,
        ) -> Pin<Box<dyn std::future::Future<Output = Continuation> + Send + '_>> {
            Box::pin(async move {
                let rx = self.release.lock().await.take().expect("asked once");
                rx.await.unwrap_or(Continuation::Cold)
            })
        }
    }

    /// A session whose user process only prints the Runtime API URL and
    /// sleeps: the test plays both the host (frames) and the user process
    /// (Runtime API calls), so it can drive the lifecycle step by step.
    struct LifecycleHarness {
        host: Framed<DuplexStream, FrameCodec>,
        session: tokio::task::JoinHandle<i32>,
        client: tachyon_serverless_sdk::RuntimeClient,
        _dir: tempfile::TempDir,
    }

    impl LifecycleHarness {
        async fn start(init_timeout_ms: u64, restore: Arc<dyn RestoreSource>) -> Self {
            const ENV: &str = "env_lifecycle";
            let (bridge_end, host_end) = tokio::io::duplex(1024 * 1024);
            let dir = tempfile::tempdir().unwrap();
            let working_dir = dir.path().to_string_lossy().into_owned();
            let session = tokio::spawn(run_session_with(
                Box::new(bridge_end),
                SessionConfig {
                    environment_id: ENV.into(),
                    runtime_api_addr: "127.0.0.1:0".parse().unwrap(),
                    guest_boot_id: None,
                    unisolated: false,
                },
                restore,
            ));
            let mut host = Framed::new(host_end, FrameCodec);
            let hello = timeout(T, host.next()).await.unwrap().unwrap().unwrap();
            assert!(matches!(
                decode_message::<GuestMessage>(&hello).unwrap(),
                GuestMessage::Hello { .. }
            ));
            let ack = HostMessage::HelloAck {
                environment_id: ENV.into(),
                epoch: 1,
                entrypoint: "/bin/sh".into(),
                args: vec![
                    "-c".into(),
                    "echo \"api=$TACHYON_RUNTIME_API\"; exec sleep 30".into(),
                ],
                env: vec![("PATH".into(), "/bin:/usr/bin".into())],
                working_dir,
                init_timeout_ms,
                max_response_bytes: 1024,
                max_log_line_bytes: 1024,
            };
            host.send(encode_message(&ack).unwrap()).await.unwrap();
            let mut url = None;
            while url.is_none() {
                let frame = timeout(T, host.next()).await.unwrap().unwrap().unwrap();
                if let GuestMessage::Log { line, .. } = decode_message(&frame).unwrap() {
                    url = line.strip_prefix("api=").map(str::to_string);
                }
            }
            let client = tachyon_serverless_sdk::RuntimeClient::new(&url.unwrap()).unwrap();
            Self {
                host,
                session,
                client,
                _dir: dir,
            }
        }

        async fn post(&self, path: &str) -> u16 {
            self.client.post_json(path, b"{}").await.unwrap().status
        }

        /// Frames other than logs and heartbeats received within `wait`.
        async fn frames_within(&mut self, wait: Duration) -> Vec<GuestMessage> {
            let mut out = Vec::new();
            let until = tokio::time::Instant::now() + wait;
            while let Ok(Some(Ok(frame))) = tokio::time::timeout_at(until, self.host.next()).await {
                match decode_message::<GuestMessage>(&frame).unwrap() {
                    GuestMessage::Log { .. } | GuestMessage::Heartbeat { .. } => {}
                    other => out.push(other),
                }
            }
            out
        }

        async fn init_error(&mut self) -> (String, String, i32) {
            let frames = self.frames_within(T).await;
            let code = timeout(T, &mut self.session).await.unwrap().unwrap();
            match frames.as_slice() {
                [
                    GuestMessage::InitError {
                        error_type,
                        message,
                        ..
                    },
                ] => (error_type.clone(), message.clone(), code),
                other => panic!("expected exactly one init error, got {other:?}"),
            }
        }
    }

    /// PLT-4651: order is bootstrap → checkpoint → (mock restore
    /// notification) → continue → ready, and the host sees `Ready` only at
    /// the very end.
    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_ready_reaches_the_host_only_after_the_restore() {
        use tachyon_serverless_protocol::runtime_api::{self as rapi, lifecycle};
        let (release, pending) = oneshot::channel();
        let mut h = LifecycleHarness::start(
            15_000,
            Arc::new(MockRestore {
                release: tokio::sync::Mutex::new(Some(pending)),
            }),
        )
        .await;
        assert_eq!(h.post(lifecycle::PATH_BOOTSTRAP).await, 202);
        assert_eq!(h.post(rapi::PATH_READY).await, 409);
        assert_eq!(h.post(lifecycle::PATH_CHECKPOINT).await, 202);
        let client = h.client.clone();
        let cont = tokio::spawn(async move { client.get(lifecycle::PATH_CONTINUE).await });
        assert!(
            h.frames_within(Duration::from_millis(300)).await.is_empty(),
            "no Ready while waiting for the restore"
        );
        assert!(!cont.is_finished());
        release
            .send(Continuation::Restored {
                instance_id: "inst_2".into(),
                restored_at_ms: 7,
                generation: 1,
            })
            .unwrap();
        let resp = timeout(T, cont).await.unwrap().unwrap().unwrap();
        assert_eq!(resp.status, 200);
        let c: Continuation = serde_json::from_slice(&resp.body).unwrap();
        assert!(matches!(c, Continuation::Restored { generation: 1, .. }));
        assert!(
            h.frames_within(Duration::from_millis(200)).await.is_empty(),
            "continue alone is not Ready"
        );
        assert_eq!(h.post(rapi::PATH_READY).await, 202);
        let frames = h.frames_within(Duration::from_millis(500)).await;
        assert!(
            matches!(frames.as_slice(), [GuestMessage::Ready { .. }]),
            "{frames:?}"
        );
        h.host
            .send(
                encode_message(&HostMessage::Shutdown {
                    reason: "test".into(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(timeout(T, h.session).await.unwrap().unwrap(), exit_code::OK);
    }

    /// PLT-4651: a timeout before the checkpoint and one after the restore are
    /// reported with different error types, and neither reaches Ready.
    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_timeouts_are_typed_by_phase() {
        use tachyon_serverless_protocol::runtime_api::lifecycle::{self, error_types};

        let mut h = LifecycleHarness::start(400, Arc::new(NoSnapshot)).await;
        assert_eq!(h.post(lifecycle::PATH_BOOTSTRAP).await, 202);
        let (error_type, message, code) = h.init_error().await;
        assert_eq!(error_type, error_types::PRE_CHECKPOINT_TIMEOUT, "{message}");
        assert_eq!(code, exit_code::INIT_ERROR);

        let mut h = LifecycleHarness::start(400, Arc::new(NoSnapshot)).await;
        assert_eq!(h.post(lifecycle::PATH_BOOTSTRAP).await, 202);
        assert_eq!(h.post(lifecycle::PATH_CHECKPOINT).await, 202);
        let resp = h.client.get(lifecycle::PATH_CONTINUE).await.unwrap();
        assert_eq!(resp.body, br#"{"kind":"cold"}"#);
        let (error_type, message, code) = h.init_error().await;
        assert_eq!(error_type, error_types::AFTER_RESTORE_TIMEOUT, "{message}");
        assert_eq!(code, exit_code::INIT_ERROR);
    }

    /// PLT-4651: a failed after-restore hook ends the session as an init
    /// error of its own type, without Ready.
    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_after_restore_failure_is_an_init_error() {
        use tachyon_serverless_protocol::runtime_api::lifecycle::{self, error_types};
        let mut h = LifecycleHarness::start(15_000, Arc::new(NoSnapshot)).await;
        assert_eq!(h.post(lifecycle::PATH_BOOTSTRAP).await, 202);
        assert_eq!(h.post(lifecycle::PATH_CHECKPOINT).await, 202);
        assert_eq!(
            h.client.get(lifecycle::PATH_CONTINUE).await.unwrap().status,
            200
        );
        let body = br#"{"error_type":"Handler.Error","message":"cannot connect"}"#;
        assert_eq!(
            h.client
                .post_json(lifecycle::PATH_ERROR, body)
                .await
                .unwrap()
                .status,
            202
        );
        let (error_type, message, code) = h.init_error().await;
        assert_eq!(error_type, error_types::AFTER_RESTORE_FAILED);
        assert_eq!(message, "Handler.Error: cannot connect");
        assert_eq!(code, exit_code::INIT_ERROR);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn session_ends_when_the_frame_writer_dies() {
        const ENV: &str = "env_writer_dies";
        let (bridge_end, host_end) = tokio::io::duplex(64 * 1024);
        let fail_writes = Arc::new(AtomicBool::new(false));
        let stream = WriteBroken {
            inner: bridge_end,
            fail_writes: fail_writes.clone(),
        };
        let dir = tempfile::tempdir().unwrap();
        let working_dir = dir.path().to_string_lossy().into_owned();

        // The host keeps its end open (no EOF) but every frame after the
        // Hello fails to write.
        let host = tokio::spawn(async move {
            let mut host = Framed::new(host_end, FrameCodec);
            let hello = timeout(T, host.next()).await.unwrap().unwrap().unwrap();
            assert!(matches!(
                decode_message::<GuestMessage>(&hello).unwrap(),
                GuestMessage::Hello { .. }
            ));
            fail_writes.store(true, Ordering::SeqCst);
            let ack = HostMessage::HelloAck {
                environment_id: ENV.into(),
                epoch: 1,
                entrypoint: "/bin/sh".into(),
                args: vec!["-c".into(), "sleep 30".into()],
                env: vec![("PATH".into(), "/bin:/usr/bin".into())],
                working_dir,
                init_timeout_ms: 0,
                max_response_bytes: 1024,
                max_log_line_bytes: 1024,
            };
            host.send(encode_message(&ack).unwrap()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
            drop(host);
        });

        let code = timeout(
            T,
            run_session(
                Box::new(stream),
                SessionConfig {
                    environment_id: ENV.into(),
                    runtime_api_addr: "127.0.0.1:0".parse().unwrap(),
                    guest_boot_id: None,
                    unisolated: false,
                },
            ),
        )
        .await
        .expect("the session must end once no frame can reach the host");
        assert_eq!(code, exit_code::TRANSPORT);
        host.abort();
    }
}
