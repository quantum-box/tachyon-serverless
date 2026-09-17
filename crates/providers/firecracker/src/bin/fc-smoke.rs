//! `fc-smoke`: minimal host that proves a Firecracker microVM boots, the
//! guest bridge connects, one invocation round-trips and the environment is
//! torn down without leftovers (PLT-4615 / PLT-4621) without the gateway.
//!
//! ```text
//! fc-smoke --firecracker .kvm/bin/firecracker --kernel .kvm/vmlinux \
//!          --rootfs .kvm/rootfs.ext4 --workdir .kvm/run \
//!          --binary target/<arch>-unknown-linux-musl/release/example-hello \
//!          --payload '{"name":"kvm"}' [--timeout-seconds 30]
//!
//! fc-smoke ... --binary .../example-cpu-burn --payload '{"seconds":60}' \
//!          --timeout-demo [--deadline-seconds 3]
//! ```
//!
//! Progress goes to stderr; a single JSON summary goes to stdout. Exit code 0
//! on success, 1 on failure, 2 when preflight fails.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use tokio_util::codec::Framed;

use tachyon_serverless_domain::{
    Architecture, AttemptId, BootEvidence, EgressProfile, EnvironmentId, InvocationId,
    ResourceProfile, RevisionId, TenantId,
};
use tachyon_serverless_protocol::runtime_api::event_types;
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, decode_message, encode_message,
};
use tachyon_serverless_provider_firecracker::boot_args::GUEST_ENTRYPOINT;
use tachyon_serverless_provider_firecracker::vmm::{EnvPaths, pid_alive};
use tachyon_serverless_provider_firecracker::{
    FirecrackerConfig, FirecrackerProvider, artifact_location_for,
};
use tachyon_serverless_provider_port::{
    Capabilities, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec, ExecutionProvider,
    PreflightReport, TerminateReason, TerminateReport,
};

#[derive(Parser, Debug)]
#[command(
    name = "fc-smoke",
    about = "Boot one Firecracker microVM and run one invocation"
)]
struct Args {
    #[arg(long)]
    firecracker: PathBuf,
    #[arg(long)]
    kernel: PathBuf,
    #[arg(long)]
    rootfs: PathBuf,
    #[arg(long)]
    workdir: PathBuf,
    /// Static Linux binary of the guest function (musl).
    #[arg(long)]
    binary: PathBuf,
    /// JSON payload for the `tachyon.invoke.v1` event.
    #[arg(long, default_value = "{}")]
    payload: String,
    /// Boot/handshake/handler timeout in seconds.
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,
    /// Send the invoke with a short deadline and demonstrate host-side termination.
    #[arg(long)]
    timeout_demo: bool,
    /// Deadline used by --timeout-demo.
    #[arg(long, default_value_t = 3)]
    deadline_seconds: u64,
    #[arg(long, default_value_t = 5000)]
    vsock_port: u32,
    #[arg(long, default_value_t = 256)]
    memory_mib: u32,
    #[arg(long, default_value_t = 1000)]
    cpu_millis: u32,
    /// Extra kernel command line.
    #[arg(long)]
    boot_args_extra: Option<String>,
    /// Run even when preflight reports failures.
    #[arg(long)]
    skip_preflight: bool,
}

#[derive(Debug, Default, Serialize)]
struct Timings {
    boot_ms: Option<u64>,
    /// Host-measured: bridge connected -> Ready received.
    init_ms: Option<u64>,
    /// Guest-reported init time (informational).
    init_ms_guest: Option<u64>,
    /// Host-measured: Invoke sent -> Response/Error received.
    handler_ms: Option<u64>,
    /// Guest-reported handler time (informational).
    handler_ms_guest: Option<u64>,
    total_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LogLine {
    stream: String,
    phase: String,
    attempt_id: Option<String>,
    ts_ms: u64,
    line: String,
}

#[derive(Debug, Default, Serialize)]
struct Leftovers {
    process_alive: Option<bool>,
    env_dir_exists: bool,
    sockets: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
struct Summary {
    ok: bool,
    mode: String,
    error: Option<String>,
    environment_id: Option<String>,
    preflight: Option<PreflightReport>,
    capabilities: Option<Capabilities>,
    hello: Option<serde_json::Value>,
    evidence: Option<BootEvidence>,
    timings: Timings,
    outcome: Option<String>,
    response: Option<serde_json::Value>,
    error_frame: Option<serde_json::Value>,
    cancel_sent: bool,
    logs: Vec<LogLine>,
    terminate: Option<TerminateReport>,
    observation_after_terminate: Option<EnvironmentObservation>,
    leftovers: Option<Leftovers>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn ms(d: Duration) -> u64 {
    d.as_millis() as u64
}

type Bridge = Framed<Box<dyn tachyon_serverless_provider_port::BridgeStream>, FrameCodec>;

async fn send(bridge: &mut Bridge, msg: &HostMessage) -> anyhow::Result<()> {
    bridge
        .send(encode_message(msg)?)
        .await
        .context("send frame")
}

/// Next guest frame or `None` on EOF; errors on protocol violations.
async fn recv(bridge: &mut Bridge) -> anyhow::Result<Option<GuestMessage>> {
    match bridge.next().await {
        None => Ok(None),
        Some(Err(e)) => Err(anyhow!("frame error: {e}")),
        Some(Ok(frame)) => Ok(Some(decode_message::<GuestMessage>(&frame)?)),
    }
}

fn record_log(
    summary: &mut Summary,
    stream: &str,
    phase: &str,
    attempt_id: Option<String>,
    ts_ms: u64,
    line: String,
) {
    eprintln!("[guest {stream} {phase}] {line}");
    summary.logs.push(LogLine {
        stream: stream.to_owned(),
        phase: phase.to_owned(),
        attempt_id,
        ts_ms,
        line,
    });
}

fn log_frame(summary: &mut Summary, msg: &GuestMessage) -> bool {
    if let GuestMessage::Log {
        stream,
        phase,
        attempt_id,
        ts_ms,
        line,
    } = msg
    {
        let stream = serde_json::to_value(stream)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        let phase = serde_json::to_value(phase)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        record_log(
            summary,
            &stream,
            &phase,
            attempt_id.clone(),
            *ts_ms,
            line.clone(),
        );
        true
    } else {
        false
    }
}

struct Session {
    bridge: Bridge,
    env_id: EnvironmentId,
    connected_at: Instant,
}

/// Handshake + Ready + one Invoke. Returns the reason to terminate with.
async fn drive(
    args: &Args,
    session: &mut Session,
    summary: &mut Summary,
) -> anyhow::Result<TerminateReason> {
    let timeout = Duration::from_secs(args.timeout_seconds);
    let env_id = session.env_id.as_str().to_owned();
    let bridge = &mut session.bridge;

    // --- Hello / HelloAck -------------------------------------------------
    let hello = tokio::time::timeout(timeout, recv(bridge))
        .await
        .map_err(|_| anyhow!("timeout waiting for Hello"))??
        .ok_or_else(|| anyhow!("bridge closed before Hello"))?;
    let GuestMessage::Hello {
        protocol_version,
        bridge_version,
        environment_id,
        guest_boot_id,
        architecture,
    } = &hello
    else {
        bail!("expected Hello, got {hello:?}");
    };
    eprintln!(
        "hello: protocol={protocol_version} bridge={bridge_version} env={environment_id} boot_id={guest_boot_id:?} arch={architecture}"
    );
    summary.hello = Some(serde_json::to_value(&hello)?);
    if let Some(ev) = summary.evidence.as_mut() {
        ev.guest_boot_id = guest_boot_id.clone();
    }
    if *protocol_version != tachyon_serverless_protocol::PROTOCOL_VERSION
        || environment_id != &env_id
    {
        send(
            bridge,
            &HostMessage::HelloReject {
                reason: format!(
                    "expected protocol {} env {env_id}, got {protocol_version} {environment_id}",
                    tachyon_serverless_protocol::PROTOCOL_VERSION
                ),
            },
        )
        .await?;
        bail!("handshake mismatch: protocol={protocol_version} env={environment_id}");
    }
    send(
        bridge,
        &HostMessage::HelloAck {
            environment_id: env_id.clone(),
            epoch: 1,
            entrypoint: GUEST_ENTRYPOINT.to_owned(),
            args: vec![],
            env: vec![],
            working_dir: "/tmp".to_owned(),
            init_timeout_ms: args.timeout_seconds * 1000,
            max_response_bytes: 6 * 1024 * 1024,
            max_log_line_bytes: 16 * 1024,
        },
    )
    .await?;

    // --- wait for Ready ---------------------------------------------------
    let init_deadline = Instant::now() + timeout;
    loop {
        let remaining = init_deadline.saturating_duration_since(Instant::now());
        let msg = tokio::time::timeout(remaining, recv(bridge))
            .await
            .map_err(|_| anyhow!("timeout waiting for Ready"))??
            .ok_or_else(|| anyhow!("bridge closed before Ready"))?;
        if log_frame(summary, &msg) {
            continue;
        }
        match msg {
            GuestMessage::Ready { init_ms } => {
                summary.timings.init_ms = Some(ms(session.connected_at.elapsed()));
                summary.timings.init_ms_guest = Some(init_ms);
                eprintln!(
                    "ready: init_ms(host)={:?} init_ms(guest)={init_ms}",
                    summary.timings.init_ms
                );
                break;
            }
            GuestMessage::Heartbeat { .. } => {}
            GuestMessage::InitError {
                error_type,
                message,
                exit_code,
            } => bail!("init error: {error_type}: {message} (exit_code={exit_code:?})"),
            GuestMessage::Exited { exit_code, signal } => {
                bail!("user process exited before Ready (code={exit_code:?} signal={signal:?})")
            }
            other => bail!("unexpected frame before Ready: {other:?}"),
        }
    }

    // --- Invoke -----------------------------------------------------------
    let payload: serde_json::Value =
        serde_json::from_str(&args.payload).context("--payload is not JSON")?;
    let invocation_id = InvocationId::generate();
    let attempt_id = AttemptId::generate();
    let handler_budget = if args.timeout_demo {
        Duration::from_secs(args.deadline_seconds)
    } else {
        timeout
    };
    let invoke_at = Instant::now();
    let deadline = invoke_at + handler_budget;
    send(
        bridge,
        &HostMessage::Invoke {
            invocation_id: invocation_id.as_str().to_owned(),
            attempt_id: attempt_id.as_str().to_owned(),
            epoch: 1,
            event_type: event_types::JSON.to_owned(),
            deadline_ms: now_ms() + ms(handler_budget),
            remaining_ms: ms(handler_budget),
            trace_id: "fc-smoke".to_owned(),
            payload,
        },
    )
    .await?;
    eprintln!("invoke sent: attempt={attempt_id} deadline_in={handler_budget:?}");

    let mut outcome: Option<&str> = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = match tokio::time::timeout(remaining, recv(bridge)).await {
            Err(_) => break, // deadline
            Ok(r) => r?,
        };
        let Some(msg) = msg else {
            bail!("bridge closed while the attempt was in flight (outcome unknown)");
        };
        if log_frame(summary, &msg) {
            continue;
        }
        match msg {
            GuestMessage::Heartbeat { .. } => {}
            GuestMessage::Response {
                attempt_id: a,
                epoch,
                payload,
                handler_ms,
            } => {
                if a != attempt_id.as_str() || epoch != 1 {
                    bail!("response for unknown attempt {a}/{epoch}");
                }
                summary.timings.handler_ms = Some(ms(invoke_at.elapsed()));
                summary.timings.handler_ms_guest = handler_ms;
                summary.response = Some(payload);
                outcome = Some("response");
                break;
            }
            GuestMessage::Error {
                attempt_id: a,
                epoch,
                error,
                error_type,
                message,
                stack_trace,
                handler_ms,
            } => {
                if a != attempt_id.as_str() || epoch != 1 {
                    bail!("error for unknown attempt {a}/{epoch}");
                }
                summary.timings.handler_ms = Some(ms(invoke_at.elapsed()));
                summary.timings.handler_ms_guest = handler_ms;
                summary.error_frame = Some(serde_json::json!({
                    "error": error, "error_type": error_type, "message": message, "stack_trace": stack_trace,
                }));
                outcome = Some("error");
                break;
            }
            other => bail!("unexpected frame during invoke: {other:?}"),
        }
    }

    match outcome {
        Some(o) => {
            summary.outcome = Some(o.to_owned());
            eprintln!("outcome: {o} after {:?}", invoke_at.elapsed());
            if args.timeout_demo {
                bail!(
                    "--timeout-demo: handler finished ({o}) before the {handler_budget:?} deadline; use a longer-running payload"
                );
            }
            // Graceful shutdown: bridge exits, guest powers off, Firecracker exits.
            send(
                bridge,
                &HostMessage::Shutdown {
                    reason: "fc-smoke done".into(),
                },
            )
            .await?;
            let closed = tokio::time::timeout(Duration::from_secs(5), async {
                while let Ok(Some(msg)) = recv(bridge).await {
                    let _ = log_frame(summary, &msg);
                }
            })
            .await
            .is_ok();
            eprintln!("shutdown: bridge connection closed={closed}");
            Ok(TerminateReason::Completed)
        }
        None => {
            summary.outcome = Some("timeout".to_owned());
            eprintln!(
                "deadline reached after {:?}; sending Cancel(grace 1s)",
                invoke_at.elapsed()
            );
            let _ = send(
                bridge,
                &HostMessage::Cancel {
                    attempt_id: attempt_id.as_str().to_owned(),
                    grace_ms: 1000,
                },
            )
            .await;
            summary.cancel_sent = true;
            // Collect whatever the bridge reports during the grace period.
            let _ = tokio::time::timeout(Duration::from_millis(1200), async {
                while let Ok(Some(msg)) = recv(bridge).await {
                    if !log_frame(summary, &msg)
                        && let GuestMessage::Error { .. } = &msg
                    {
                        summary.error_frame = serde_json::to_value(&msg).ok();
                    }
                }
            })
            .await;
            if args.timeout_demo {
                Ok(TerminateReason::Timeout)
            } else {
                Err(anyhow!("handler did not finish within {handler_budget:?}"))
            }
        }
    }
}

async fn run(args: &Args, summary: &mut Summary) -> anyhow::Result<()> {
    let started = Instant::now();
    summary.mode = if args.timeout_demo {
        "timeout-demo"
    } else {
        "invoke"
    }
    .to_owned();

    let provider = FirecrackerProvider::new(FirecrackerConfig {
        firecracker_binary: args.firecracker.clone(),
        kernel: args.kernel.clone(),
        rootfs: args.rootfs.clone(),
        workdir: args.workdir.clone(),
        vsock_port: args.vsock_port,
        boot_args_extra: args.boot_args_extra.clone(),
        ..Default::default()
    });
    summary.capabilities = Some(provider.capabilities());

    let preflight = provider.preflight().await?;
    for c in &preflight.checks {
        eprintln!(
            "preflight {:<20} {} {}",
            c.name,
            if c.ok { "ok  " } else { "FAIL" },
            c.detail
        );
    }
    let preflight_ok = preflight.ok;
    summary.preflight = Some(preflight);
    if !preflight_ok && !args.skip_preflight {
        bail!("preflight failed");
    }

    let host_arch = Architecture::host().ok_or_else(|| anyhow!("unsupported host architecture"))?;
    let artifact = artifact_location_for(&args.binary)
        .with_context(|| format!("read --binary {}", args.binary.display()))?;
    provider.validate_artifact(&artifact, host_arch).await?;
    eprintln!(
        "artifact: {} ({} bytes, {})",
        args.binary.display(),
        artifact.size_bytes,
        artifact.digest
    );

    let env_id = EnvironmentId::generate();
    summary.environment_id = Some(env_id.as_str().to_owned());
    let spec = EnvironmentSpec {
        environment_id: env_id.clone(),
        tenant_id: TenantId::generate(),
        revision_id: RevisionId::generate(),
        artifact,
        architecture: host_arch,
        resources: ResourceProfile {
            memory_mib: args.memory_mib,
            cpu_millis: args.cpu_millis,
            ..Default::default()
        },
        egress: EgressProfile::None,
        egress_allow: Vec::new(),
        connect_timeout: Duration::from_secs(args.timeout_seconds),
    };
    eprintln!("creating environment {env_id} ...");
    let EnvironmentHandle {
        evidence,
        stream,
        created_at,
        connected_at,
        ..
    } = provider.create_environment(spec).await?;
    summary.timings.boot_ms = Some(ms(connected_at.duration_since(created_at)));
    eprintln!(
        "booted: pid={:?} firecracker={} boot_ms={:?}",
        evidence.host_pid,
        evidence
            .details
            .get("firecracker_version")
            .and_then(|v| v.as_str())
            .unwrap_or("?"),
        summary.timings.boot_ms
    );
    let host_pid = evidence.host_pid;
    summary.evidence = Some(evidence);

    let mut session = Session {
        bridge: Framed::new(stream, FrameCodec),
        env_id: env_id.clone(),
        connected_at,
    };
    let outcome = drive(args, &mut session, summary).await;
    drop(session);

    let reason = match &outcome {
        Ok(r) => *r,
        Err(_) => TerminateReason::Crashed,
    };
    let report = provider
        .terminate_environment(&env_id, reason)
        .await
        .context("terminate")?;
    eprintln!(
        "terminated: was_running={} cleaned={}",
        report.was_running,
        report.cleaned.len()
    );
    summary.terminate = Some(report);
    summary.observation_after_terminate = provider.observe_environment(&env_id).await.ok();

    let leftovers = audit_leftovers(
        &provider.config().workdir,
        &env_id,
        args.vsock_port,
        host_pid,
    );
    let clean = leftovers.process_alive != Some(true)
        && !leftovers.env_dir_exists
        && leftovers.sockets.is_empty();
    summary.leftovers = Some(leftovers);
    summary.timings.total_ms = Some(ms(started.elapsed()));

    outcome?;
    if summary.observation_after_terminate != Some(EnvironmentObservation::NotFound) {
        bail!(
            "environment still observable after terminate: {:?}",
            summary.observation_after_terminate
        );
    }
    if !clean {
        bail!("leftovers detected: {:?}", summary.leftovers);
    }
    if args.timeout_demo && summary.outcome.as_deref() != Some("timeout") {
        bail!("timeout demo did not time out");
    }
    Ok(())
}

fn audit_leftovers(
    workdir: &Path,
    env_id: &EnvironmentId,
    port: u32,
    pid: Option<u32>,
) -> Leftovers {
    let paths = EnvPaths::new(workdir, env_id.as_str(), port);
    let mut sockets = Vec::new();
    for p in [&paths.api_sock, &paths.vsock_uds, &paths.vsock_listener] {
        if p.exists() {
            sockets.push(p.display().to_string());
        }
    }
    Leftovers {
        process_alive: pid.map(pid_alive),
        env_dir_exists: paths.dir.exists(),
        sockets,
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    let mut summary = Summary::default();
    let result = run(&args, &mut summary).await;
    let preflight_failed =
        summary.preflight.as_ref().is_some_and(|p| !p.ok) && !args.skip_preflight;
    match result {
        Ok(()) => summary.ok = true,
        Err(e) => {
            summary.ok = false;
            summary.error = Some(format!("{e:#}"));
            eprintln!("FAILED: {e:#}");
        }
    }
    match serde_json::to_string_pretty(&summary) {
        Ok(json) => println!("{json}"),
        Err(e) => eprintln!("cannot serialise summary: {e}"),
    }
    if summary.ok {
        ExitCode::SUCCESS
    } else if preflight_failed {
        ExitCode::from(2)
    } else {
        ExitCode::from(1)
    }
}
