//! One bridge session: handshake with the host, Runtime API server, user
//! process lifecycle and the frame loop (docs/protocol.md section A).
//!
//! Exit codes: 0 normal, 2 handshake rejected / protocol error, 3 init
//! error, 4 transport failure.

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use tachyon_serverless_protocol::{
    FrameCodec, GuestErrorKind, GuestMessage, HostMessage, LogPhase, LogStream, PROTOCOL_VERSION,
    ProtocolError, decode_message, encode_message,
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
use crate::runtime_api::{ApiEvent, ApiLimits, InvokeRequest, RuntimeApi};
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
/// terminate). Shorter than the provider's own 2 s so the user process is
/// gone before the bridge can be SIGKILLed.
const SIGTERM_GRACE: Duration = Duration::from_secs(1);
const LOG_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const OUTGOING_QUEUE: usize = 256;

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

    // --- outgoing frame writer ------------------------------------------
    let (out_tx, out_rx) = mpsc::channel::<GuestMessage>(OUTGOING_QUEUE);
    let writer_task = tokio::spawn(write_loop(writer, out_rx));

    // --- runtime api server ---------------------------------------------
    let (api_tx, mut api_rx) = mpsc::channel::<ApiEvent>(64);
    let process_started_at = Instant::now();
    let api = RuntimeApi::new(
        api_tx,
        ApiLimits {
            max_response_bytes: launch.max_response_bytes,
        },
        process_started_at,
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
                    Ok(HostMessage::Invoke { invocation_id, attempt_id, epoch, event_type, deadline_ms, trace_id, payload }) => {
                        let request = InvokeRequest { invocation_id, attempt_id, epoch, event_type, deadline_ms, trace_id, payload };
                        info!(attempt_id = %request.attempt_id, epoch, "invoke received");
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
                    Ok(other) => warn!("ignoring unexpected {} frame after handshake", host_message_name(&other)),
                },
            },
            Some(event) = api_rx.recv() => match event {
                ApiEvent::Ready { init_ms } => {
                    info!(init_ms, "user process ready");
                    init_deadline = None;
                    let _ = out_tx.send(GuestMessage::Ready { init_ms }).await;
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
            error!(
                "user process did not become ready within {} ms",
                launch.init_timeout_ms
            );
            signal_group(pid, SIGKILL);
            wait_exit(&mut exit_rx, SHUTDOWN_GRACE).await;
            drain_logs(&mut log_tasks).await;
            let _ = out_tx
                .send(GuestMessage::InitError {
                    error_type: "Runtime.InitTimeout".into(),
                    message: format!(
                        "user process did not become ready within {} ms",
                        launch.init_timeout_ms
                    ),
                    exit_code: None,
                })
                .await;
            exit_code::INIT_ERROR
        }
        Outcome::Shutdown(reason) => {
            info!(%reason, "shutdown received");
            api.shutdown();
            terminate_user(pid, &mut exit_rx, SHUTDOWN_GRACE).await;
            drain_logs(&mut log_tasks).await;
            exit_code::OK
        }
        Outcome::Terminated => {
            api.shutdown();
            terminate_user(pid, &mut exit_rx, SIGTERM_GRACE).await;
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

async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: FramedWrite<W, FrameCodec>,
    mut rx: mpsc::Receiver<GuestMessage>,
) -> Result<(), ProtocolError> {
    while let Some(msg) = rx.recv().await {
        let bytes = encode_message(&msg)?;
        writer.send(bytes).await?;
    }
    writer.close().await
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
async fn terminate_user(
    pid: u32,
    exit_rx: &mut oneshot::Receiver<std::process::ExitStatus>,
    grace: Duration,
) {
    signal_group(pid, SIGTERM);
    if wait_exit(exit_rx, grace).await {
        return;
    }
    warn!("user process ignored SIGTERM; sending SIGKILL");
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
