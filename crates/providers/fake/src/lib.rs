//! Fake execution provider (tests only).
//!
//! [`FakeExecutionProvider`] implements [`ExecutionProvider`] with an
//! in-process *scripted guest bridge* that speaks the real wire protocol from
//! `tachyon-serverless-protocol` over a [`tokio::io::duplex`] stream. Each
//! `create_environment` call pops the next [`FakeGuestScript`] from a queue
//! (falling back to a configurable default) and spawns a task that plays the
//! script against the host side of the connection.
//!
//! The provider records every created environment, every terminate call (with
//! its reason) and every host message the guest received, so tests can assert
//! cleanup, idempotency and cancellation behaviour.
//!
//! This crate must never be selected by a gateway configuration; the gateway
//! configuration only knows `process` and `firecracker`.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};

use tachyon_serverless_domain::{
    Architecture, BootEvidence, EnvironmentId, ProviderKind, RUNTIME_PROTOCOL_V1,
};
use tachyon_serverless_protocol::runtime_api::{HttpRequestEvent, HttpResponsePayload};
use tachyon_serverless_protocol::{
    FrameCodec, GuestErrorKind, GuestMessage, HostMessage, LogPhase, LogStream, PROTOCOL_VERSION,
    decode_message, encode_message,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, Capabilities, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec,
    ExecutionProvider, IsolationLevel, PreflightCheck, PreflightReport, ProviderError, Support,
    TerminateReason, TerminateReport,
};

/// Boxed future returned by a custom script closure.
pub type ScriptFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Everything a custom script closure gets: the raw duplex stream (guest end)
/// and the environment id it was created for.
pub struct CustomScriptContext {
    pub environment_id: EnvironmentId,
    pub stream: DuplexStream,
}

/// Closure form of a script. It receives the guest end of the stream and is
/// responsible for the whole conversation (including `Hello`).
pub type CustomScript = Arc<dyn Fn(CustomScriptContext) -> ScriptFuture + Send + Sync>;

/// What the fake guest does for one environment. Every variant first performs
/// the `Hello` / `HelloAck` handshake; the variants differ afterwards.
#[derive(Clone)]
pub enum FakeGuestScript {
    /// Ready, then answer the first `Invoke` with this JSON payload.
    RespondOk(serde_json::Value),
    /// Ready, then answer the first `Invoke` with the invoke payload itself.
    Echo,
    /// Ready, then treat the invoke payload as an `HttpRequestEvent` and answer
    /// with an `HttpResponsePayload` (status 200, JSON body = the event).
    EchoHttp,
    /// Ready, then answer with `Error{kind: handler}`.
    HandlerError { error_type: String, message: String },
    /// Ready, then answer with `Error{kind: panic}`.
    Panic,
    /// Ready, then on `Invoke` report `Error{kind: crash}` followed by `Exited`
    /// and close the connection (user process died while in flight).
    CrashAfterReady { exit_code: i32 },
    /// Never send `Ready`; report `InitError` and close.
    InitError { message: String },
    /// Send init logs, never send `Ready`, keep the connection open.
    NeverReady,
    /// Ready, then never answer `Invoke` (until the environment is terminated).
    HangForever,
    /// Ready, then drop the connection right after receiving `Invoke`.
    DisconnectAfterInvoke,
    /// Ready, then immediately send `Exited{0}` and close (no attempt in flight).
    ExitAfterReady,
    /// Ready, answer first with a `Response` carrying a wrong epoch, then the
    /// correct one. Tests lease fencing on the host side.
    WrongEpochThenOk(serde_json::Value),
    /// Ready, then answer *every* `Invoke` with this JSON payload, for as long
    /// as the environment lives. Used to serve several sequential attempts on
    /// one pooled environment.
    RespondOkForever(serde_json::Value),
    /// Ready, then answer *every* `Invoke` with the invoke payload itself.
    EchoForever,
    /// Ready, answer the first `Invoke` with this payload and then stop
    /// reading the connection entirely, without closing it: a guest that is
    /// still connected but no longer being scheduled (a resume that did not
    /// really take, a wedged guest). Nothing is queued for the host to find,
    /// so only a probe that waits for an answer can tell it from a healthy
    /// one (PLT-4633 review F2).
    RespondOkThenStopAnswering(serde_json::Value),
    /// Ready, answer the first `Invoke` with this payload and then send
    /// `Exited` *without closing the stream*: a user process that died right
    /// after answering, while the bridge is still connected. The host only
    /// learns about it by reading the queued frame, which is what a pooled
    /// session has to do before it can carry another attempt.
    RespondOkThenExit(serde_json::Value),
    /// Ready, then for every `Invoke` answer twice: first `{"stale": true}`
    /// carrying the *previous* epoch (a late frame of the attempt before),
    /// then this payload at the correct epoch. The two payloads differ so a
    /// test can tell which one settled the attempt, which is what makes epoch
    /// fencing across reuse observable.
    StaleEpochThenOkForever(serde_json::Value),
    /// Fully custom conversation.
    Custom(CustomScript),
}

impl std::fmt::Debug for FakeGuestScript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl FakeGuestScript {
    pub fn name(&self) -> &'static str {
        match self {
            Self::RespondOk(_) => "respond_ok",
            Self::Echo => "echo",
            Self::EchoHttp => "echo_http",
            Self::HandlerError { .. } => "handler_error",
            Self::Panic => "panic",
            Self::CrashAfterReady { .. } => "crash_after_ready",
            Self::InitError { .. } => "init_error",
            Self::NeverReady => "never_ready",
            Self::HangForever => "hang_forever",
            Self::DisconnectAfterInvoke => "disconnect_after_invoke",
            Self::ExitAfterReady => "exit_after_ready",
            Self::WrongEpochThenOk(_) => "wrong_epoch_then_ok",
            Self::RespondOkForever(_) => "respond_ok_forever",
            Self::EchoForever => "echo_forever",
            Self::RespondOkThenExit(_) => "respond_ok_then_exit",
            Self::RespondOkThenStopAnswering(_) => "respond_ok_then_stop_answering",
            Self::StaleEpochThenOkForever(_) => "stale_epoch_then_ok_forever",
            Self::Custom(_) => "custom",
        }
    }
}

/// Provider-level knobs, mostly to simulate failures around environment
/// creation and artifact validation.
#[derive(Debug, Clone, Default)]
pub struct FakeProviderOptions {
    /// Artificial delay before the bridge "connects" (boot time).
    pub boot_delay: Duration,
    /// When set, `validate_artifact` fails with this reason.
    pub reject_artifacts: Option<String>,
    /// When set, `create_environment` fails with `ProviderError::Boot`.
    pub fail_create: Option<String>,
    /// When set, `preflight` reports not ok.
    pub preflight_failure: Option<String>,
    /// Report `idle_quiesce` and `idle_resume` as `Supported`. The
    /// application only pools environments for a provider that does, so this
    /// is what lets a test exercise reuse end to end. Off by default: the
    /// default fake keeps the destroy-after-invoke behaviour of both shipped
    /// providers.
    pub warm_capable: bool,
    /// When set, `idle_quiesce` fails with this reason. The environment is
    /// then never pooled and the caller terminates it, exactly as it did
    /// before reuse existed (PLT-4633 acceptance 3).
    pub fail_quiesce: Option<String>,
    /// When set, `idle_resume` fails with this reason. The pool must retire
    /// the environment and fall back to a cold start, and must never dispatch
    /// into it.
    pub fail_resume: Option<String>,
    /// Artificial cost of `idle_resume`, so a test can observe that a warm
    /// start reports what it really cost instead of reporting zero.
    pub resume_delay: Duration,
    /// Artificial cost of `idle_quiesce`. A real pause goes through the
    /// hypervisor's API with its own timeout, and the caller's response must
    /// not wait for it (PLT-4633 review F4).
    pub quiesce_delay: Duration,
}

#[derive(Debug)]
struct FakeEnvironment {
    script: &'static str,
    guest: tokio::task::JoinHandle<()>,
    host_pid: u32,
    running: bool,
    /// True between a successful `idle_quiesce` and the next `idle_resume`.
    /// A real provider would have stopped the guest's vCPUs here.
    paused: bool,
    /// Host messages the guest received, in order.
    received: Arc<Mutex<Vec<HostMessage>>>,
    /// The `HelloAck` the guest received (test-only; contains secrets and
    /// must never be logged).
    hello_ack: Arc<Mutex<Option<HostMessage>>>,
}

#[derive(Default)]
struct Inner {
    scripts: VecDeque<FakeGuestScript>,
    default_script: Option<FakeGuestScript>,
    environments: HashMap<EnvironmentId, FakeEnvironment>,
    created: Vec<EnvironmentId>,
    terminated: Vec<(EnvironmentId, TerminateReason)>,
    /// Every `idle_quiesce` / `idle_resume` call in order, including the ones
    /// [`FakeProviderOptions`] made fail, so a test can assert both that the
    /// call happened and what the pool did about its failure.
    quiesced: Vec<EnvironmentId>,
    resumed: Vec<EnvironmentId>,
}

/// Test-only [`ExecutionProvider`].
pub struct FakeExecutionProvider {
    inner: Mutex<Inner>,
    options: FakeProviderOptions,
}

impl Default for FakeExecutionProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for FakeExecutionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("FakeExecutionProvider")
            .field("queued_scripts", &inner.scripts.len())
            .field("environments", &inner.environments.len())
            .finish_non_exhaustive()
    }
}

impl FakeExecutionProvider {
    /// A provider whose guests answer every invoke with `{"ok": true}`.
    pub fn new() -> Self {
        Self::with_options(FakeProviderOptions::default())
    }

    pub fn with_options(options: FakeProviderOptions) -> Self {
        Self {
            inner: Mutex::new(Inner {
                default_script: Some(FakeGuestScript::RespondOk(serde_json::json!({"ok": true}))),
                ..Inner::default()
            }),
            options,
        }
    }

    /// Provider with a fixed script queue (consumed in order).
    pub fn with_scripts(scripts: impl IntoIterator<Item = FakeGuestScript>) -> Self {
        let p = Self::new();
        for s in scripts {
            p.push_script(s);
        }
        p
    }

    /// Queue a script for the next environment.
    pub fn push_script(&self, script: FakeGuestScript) {
        self.inner.lock().scripts.push_back(script);
    }

    /// Script used when the queue is empty. `None` makes creation fail.
    pub fn set_default_script(&self, script: Option<FakeGuestScript>) {
        self.inner.lock().default_script = script;
    }

    /// Environment ids created so far, in creation order.
    pub fn created(&self) -> Vec<EnvironmentId> {
        self.inner.lock().created.clone()
    }

    /// Every terminate call (including idempotent repeats), in order.
    pub fn terminated(&self) -> Vec<(EnvironmentId, TerminateReason)> {
        self.inner.lock().terminated.clone()
    }

    /// Environments that were created and are still running (not terminated).
    pub fn running(&self) -> Vec<EnvironmentId> {
        let inner = self.inner.lock();
        inner
            .environments
            .iter()
            .filter(|(_, e)| e.running)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Every `idle_quiesce` call, in order (failed ones included).
    pub fn quiesced(&self) -> Vec<EnvironmentId> {
        self.inner.lock().quiesced.clone()
    }

    /// Every `idle_resume` call, in order (failed ones included).
    pub fn resumed(&self) -> Vec<EnvironmentId> {
        self.inner.lock().resumed.clone()
    }

    /// Environments currently quiesced. Nothing may be dispatched into these.
    pub fn paused(&self) -> Vec<EnvironmentId> {
        let inner = self.inner.lock();
        inner
            .environments
            .iter()
            .filter(|(_, e)| e.running && e.paused)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Name of the script that was played for `environment_id`.
    pub fn script_name(&self, environment_id: &EnvironmentId) -> Option<&'static str> {
        self.inner
            .lock()
            .environments
            .get(environment_id)
            .map(|e| e.script)
    }

    /// Host messages received by the guest of `environment_id`.
    pub fn host_messages(&self, environment_id: &EnvironmentId) -> Vec<HostMessage> {
        self.inner
            .lock()
            .environments
            .get(environment_id)
            .map(|e| e.received.lock().clone())
            .unwrap_or_default()
    }

    /// The `HelloAck` received by the guest of `environment_id`, if any.
    /// Test-only accessor: the message carries resolved secrets.
    pub fn hello_ack(&self, environment_id: &EnvironmentId) -> Option<HostMessage> {
        self.inner
            .lock()
            .environments
            .get(environment_id)
            .and_then(|e| e.hello_ack.lock().clone())
    }

    fn next_script(&self) -> Option<FakeGuestScript> {
        let mut inner = self.inner.lock();
        inner
            .scripts
            .pop_front()
            .or_else(|| inner.default_script.clone())
    }
}

#[async_trait]
impl ExecutionProvider for FakeExecutionProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Fake
    }

    fn capabilities(&self) -> Capabilities {
        let unsupported = |what: &str| Support::unsupported(format!("fake provider: {what}"));
        let idle = |what: &str| match self.options.warm_capable {
            true => Support::Supported,
            false => unsupported(what),
        };
        Capabilities {
            isolation: IsolationLevel::Process,
            create_terminate: Support::Supported,
            observe: Support::Supported,
            enforce_deadline: Support::Supported,
            enforce_resource_limits: unsupported("no resource limits"),
            egress_none: unsupported("no network model"),
            egress_restricted: unsupported("no network model"),
            egress_public_web: unsupported("no network model"),
            host_metering: unsupported("no metering"),
            idle_quiesce: idle("destroy-after-invoke"),
            idle_resume: idle("destroy-after-invoke"),
            snapshot_create: unsupported("no snapshots"),
            snapshot_clone: unsupported("no snapshots"),
            dev_only: true,
        }
    }

    async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
        let (ok, detail) = match &self.options.preflight_failure {
            Some(reason) => (false, reason.clone()),
            None => (true, "in-process fake guest".to_string()),
        };
        Ok(PreflightReport {
            provider: "fake".into(),
            ok,
            checks: vec![PreflightCheck {
                name: "fake".into(),
                ok,
                detail,
            }],
        })
    }

    async fn validate_artifact(
        &self,
        artifact: &ArtifactLocation,
        _architecture: Architecture,
    ) -> Result<(), ProviderError> {
        if let Some(reason) = &self.options.reject_artifacts {
            return Err(ProviderError::ArtifactRejected(reason.clone()));
        }
        if !artifact.path.exists() {
            return Err(ProviderError::ArtifactRejected(format!(
                "artifact path does not exist: {}",
                artifact.path.display()
            )));
        }
        Ok(())
    }

    async fn create_environment(
        &self,
        spec: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError> {
        let created_at = Instant::now();
        if let Some(reason) = &self.options.fail_create {
            return Err(ProviderError::Boot(reason.clone()));
        }
        if self
            .inner
            .lock()
            .environments
            .contains_key(&spec.environment_id)
        {
            return Err(ProviderError::InvalidSpec(format!(
                "environment {} already exists",
                spec.environment_id
            )));
        }
        let script = self.next_script().ok_or_else(|| {
            ProviderError::Internal("fake provider has no script for this environment".into())
        })?;
        if !self.options.boot_delay.is_zero() {
            tokio::time::sleep(self.options.boot_delay).await;
        }

        let (host_end, guest_end) = tokio::io::duplex(256 * 1024);
        let received = Arc::new(Mutex::new(Vec::new()));
        let hello_ack = Arc::new(Mutex::new(None));
        let guest_boot_id = format!("fake-boot-{}", spec.environment_id.as_str());
        let script_name = script.name();
        let guest = tokio::spawn(run_guest(
            script,
            GuestContext {
                environment_id: spec.environment_id.clone(),
                guest_boot_id: guest_boot_id.clone(),
                architecture: spec.architecture,
                received: received.clone(),
                hello_ack: hello_ack.clone(),
            },
            guest_end,
        ));
        let host_pid = std::process::id();
        {
            let mut inner = self.inner.lock();
            inner.created.push(spec.environment_id.clone());
            inner.environments.insert(
                spec.environment_id.clone(),
                FakeEnvironment {
                    script: script_name,
                    guest,
                    host_pid,
                    running: true,
                    paused: false,
                    received,
                    hello_ack,
                },
            );
        }
        let mut details = serde_json::Map::new();
        details.insert("provider".into(), "fake".into());
        details.insert("script".into(), script_name.into());
        details.insert("protocol".into(), RUNTIME_PROTOCOL_V1.into());
        Ok(EnvironmentHandle {
            environment_id: spec.environment_id,
            evidence: BootEvidence {
                guest_boot_id: Some(guest_boot_id),
                host_pid: Some(host_pid),
                details,
            },
            stream: Box::new(host_end),
            created_at,
            connected_at: Instant::now(),
        })
    }

    async fn terminate_environment(
        &self,
        environment_id: &EnvironmentId,
        reason: TerminateReason,
    ) -> Result<TerminateReport, ProviderError> {
        let mut inner = self.inner.lock();
        inner.terminated.push((environment_id.clone(), reason));
        match inner.environments.get_mut(environment_id) {
            Some(env) if env.running => {
                env.running = false;
                env.guest.abort();
                Ok(TerminateReport {
                    was_running: true,
                    cleaned: vec![format!("fake:duplex:{environment_id}")],
                })
            }
            _ => Ok(TerminateReport {
                was_running: false,
                cleaned: Vec::new(),
            }),
        }
    }

    /// Quiesce an environment on its way into the pool. Idempotent, like the
    /// real thing: quiescing an already quiesced environment is `Ok`.
    async fn idle_quiesce(&self, environment_id: &EnvironmentId) -> Result<(), ProviderError> {
        self.inner.lock().quiesced.push(environment_id.clone());
        if !self.options.quiesce_delay.is_zero() {
            tokio::time::sleep(self.options.quiesce_delay).await;
        }
        if let Some(reason) = &self.options.fail_quiesce {
            return Err(ProviderError::Internal(reason.clone()));
        }
        let mut inner = self.inner.lock();
        match inner.environments.get_mut(environment_id) {
            Some(env) if env.running => {
                env.paused = true;
                Ok(())
            }
            _ => Err(ProviderError::NotFound(environment_id.clone())),
        }
    }

    /// Resume a quiesced environment. `resume_delay` makes the call cost
    /// something measurable, so a test can check that a warm start reports
    /// what it really cost.
    async fn idle_resume(&self, environment_id: &EnvironmentId) -> Result<(), ProviderError> {
        self.inner.lock().resumed.push(environment_id.clone());
        if !self.options.resume_delay.is_zero() {
            tokio::time::sleep(self.options.resume_delay).await;
        }
        if let Some(reason) = &self.options.fail_resume {
            return Err(ProviderError::Internal(reason.clone()));
        }
        let mut inner = self.inner.lock();
        match inner.environments.get_mut(environment_id) {
            Some(env) if env.running => {
                env.paused = false;
                Ok(())
            }
            _ => Err(ProviderError::NotFound(environment_id.clone())),
        }
    }

    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        let inner = self.inner.lock();
        Ok(match inner.environments.get(environment_id) {
            Some(env) if env.running && !env.guest.is_finished() => {
                EnvironmentObservation::Running {
                    host_pid: Some(env.host_pid),
                }
            }
            Some(_) => EnvironmentObservation::Exited {
                exit_code: Some(0),
                signal: None,
            },
            None => EnvironmentObservation::NotFound,
        })
    }

    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
        let inner = self.inner.lock();
        Ok(inner
            .environments
            .iter()
            .filter(|(_, e)| e.running)
            .map(|(id, _)| id.clone())
            .collect())
    }
}

// ---------------------------------------------------------------------------
// scripted guest
// ---------------------------------------------------------------------------

struct GuestContext {
    environment_id: EnvironmentId,
    guest_boot_id: String,
    architecture: Architecture,
    received: Arc<Mutex<Vec<HostMessage>>>,
    hello_ack: Arc<Mutex<Option<HostMessage>>>,
}

type Reader = FramedRead<ReadHalf<DuplexStream>, FrameCodec>;
type Writer = FramedWrite<WriteHalf<DuplexStream>, FrameCodec>;

struct Guest {
    ctx: GuestContext,
    reader: Reader,
    writer: Writer,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Guest {
    async fn send(&mut self, msg: &GuestMessage) -> bool {
        match encode_message(msg) {
            Ok(bytes) => self.writer.send(bytes).await.is_ok(),
            Err(_) => false,
        }
    }

    async fn recv(&mut self) -> Option<HostMessage> {
        loop {
            let frame: Bytes = self.reader.next().await?.ok()?;
            match decode_message::<HostMessage>(&frame) {
                // The real bridge answers the host's liveness probe from its
                // own frame loop, without involving the user process, so every
                // script answers it here instead of each one remembering to.
                // A guest that must *not* answer stops reading altogether
                // ([`FakeGuestScript::RespondOkThenStopAnswering`]).
                Ok(HostMessage::Ping { nonce }) => {
                    self.ctx.received.lock().push(HostMessage::Ping { nonce });
                    if !self.send(&GuestMessage::Pong { nonce }).await {
                        return None;
                    }
                }
                Ok(msg) => {
                    self.ctx.received.lock().push(msg.clone());
                    return Some(msg);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "fake guest: undecodable host frame");
                }
            }
        }
    }

    async fn log(&mut self, phase: LogPhase, attempt_id: Option<&str>, line: &str) -> bool {
        self.send(&GuestMessage::Log {
            stream: LogStream::Stdout,
            phase,
            attempt_id: attempt_id.map(str::to_string),
            ts_ms: now_ms(),
            line: line.to_string(),
        })
        .await
    }

    /// Hello -> HelloAck. Returns false when rejected or disconnected.
    async fn handshake(&mut self) -> bool {
        let hello = GuestMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            bridge_version: format!("fake-{}", env!("CARGO_PKG_VERSION")),
            environment_id: self.ctx.environment_id.to_string(),
            guest_boot_id: Some(self.ctx.guest_boot_id.clone()),
            architecture: self.ctx.architecture.as_str().to_string(),
        };
        if !self.send(&hello).await {
            return false;
        }
        match self.recv().await {
            Some(ack @ HostMessage::HelloAck { .. }) => {
                *self.ctx.hello_ack.lock() = Some(ack);
                true
            }
            Some(HostMessage::HelloReject { reason }) => {
                tracing::debug!(%reason, "fake guest: handshake rejected");
                false
            }
            _ => false,
        }
    }

    async fn ready(&mut self) -> bool {
        self.log(LogPhase::Init, None, "fake guest: user process started")
            .await
            && self.send(&GuestMessage::Ready { init_ms: 1 }).await
    }

    /// Wait for the next `Invoke`. Returns `None` on shutdown / disconnect.
    async fn wait_invoke(&mut self) -> Option<Invoke> {
        loop {
            match self.recv().await? {
                HostMessage::Invoke {
                    invocation_id,
                    attempt_id,
                    epoch,
                    event_type,
                    payload,
                    ..
                } => {
                    return Some(Invoke {
                        invocation_id,
                        attempt_id,
                        epoch,
                        event_type,
                        payload,
                    });
                }
                HostMessage::Shutdown { .. } => return None,
                // `recv` answers a `Ping` itself, so one never gets here.
                HostMessage::Cancel { .. }
                | HostMessage::HelloAck { .. }
                | HostMessage::Ping { .. } => continue,
                HostMessage::HelloReject { .. } => return None,
            }
        }
    }

    /// After a result: keep answering until `Shutdown` or disconnect. A second
    /// `Invoke` is answered with a protocol error (one invocation per env).
    async fn drain_until_shutdown(&mut self) {
        while let Some(msg) = self.recv().await {
            match msg {
                HostMessage::Shutdown { .. } => break,
                HostMessage::Invoke {
                    attempt_id, epoch, ..
                } => {
                    let _ = self
                        .send(&GuestMessage::Error {
                            attempt_id,
                            epoch,
                            error: GuestErrorKind::Protocol,
                            error_type: "Runtime.Protocol".into(),
                            message: "an attempt is already in flight".into(),
                            stack_trace: None,
                            handler_ms: None,
                        })
                        .await;
                }
                _ => {}
            }
        }
    }

    async fn respond(&mut self, inv: &Invoke, epoch: u64, payload: serde_json::Value) -> bool {
        self.log(
            LogPhase::Handler,
            Some(&inv.attempt_id),
            &format!("fake guest: handling {}", inv.invocation_id),
        )
        .await
            && self
                .send(&GuestMessage::Response {
                    attempt_id: inv.attempt_id.clone(),
                    epoch,
                    payload,
                    handler_ms: Some(2),
                })
                .await
    }

    async fn error(
        &mut self,
        inv: &Invoke,
        kind: GuestErrorKind,
        error_type: &str,
        message: &str,
    ) -> bool {
        self.send(&GuestMessage::Error {
            attempt_id: inv.attempt_id.clone(),
            epoch: inv.epoch,
            error: kind,
            error_type: error_type.into(),
            message: message.into(),
            stack_trace: Some("fake guest stack trace".into()),
            handler_ms: Some(1),
        })
        .await
    }
}

struct Invoke {
    invocation_id: String,
    attempt_id: String,
    epoch: u64,
    event_type: String,
    payload: serde_json::Value,
}

fn http_echo(inv: &Invoke) -> serde_json::Value {
    let event: Result<HttpRequestEvent, _> = serde_json::from_value(inv.payload.clone());
    let payload = match event {
        Ok(event) if inv.event_type == "tachyon.http.v1" => {
            let body = serde_json::to_vec(&event).unwrap_or_default();
            HttpResponsePayload {
                status: 200,
                headers: vec![
                    ("content-type".into(), "application/json".into()),
                    ("x-echo-method".into(), event.method.clone()),
                    ("x-echo-path".into(), event.path.clone()),
                ],
                body_base64: base64::engine::general_purpose::STANDARD.encode(body),
            }
        }
        _ => HttpResponsePayload {
            status: 400,
            headers: vec![("content-type".into(), "text/plain".into())],
            body_base64: base64::engine::general_purpose::STANDARD.encode(b"not an http event"),
        },
    };
    serde_json::to_value(payload).unwrap_or(serde_json::Value::Null)
}

async fn run_guest(script: FakeGuestScript, ctx: GuestContext, stream: DuplexStream) {
    if let FakeGuestScript::Custom(f) = script {
        f(CustomScriptContext {
            environment_id: ctx.environment_id,
            stream,
        })
        .await;
        return;
    }
    let (r, w) = tokio::io::split(stream);
    let mut guest = Guest {
        ctx,
        reader: FramedRead::new(r, FrameCodec),
        writer: FramedWrite::new(w, FrameCodec),
    };
    if !guest.handshake().await {
        return;
    }
    match script {
        FakeGuestScript::InitError { message } => {
            guest
                .log(LogPhase::Init, None, "fake guest: init failing")
                .await;
            guest
                .send(&GuestMessage::InitError {
                    error_type: "Runtime.InitError".into(),
                    message,
                    exit_code: Some(3),
                })
                .await;
        }
        FakeGuestScript::NeverReady => {
            guest
                .log(LogPhase::Init, None, "fake guest: stuck in init")
                .await;
            // Keep the connection open until the environment is terminated
            // (the guest task is aborted). Heartbeats keep the stream busy.
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if !guest
                    .send(&GuestMessage::Heartbeat { ts_ms: now_ms() })
                    .await
                {
                    return;
                }
            }
        }
        FakeGuestScript::ExitAfterReady => {
            if !guest.ready().await {
                return;
            }
            guest
                .send(&GuestMessage::Exited {
                    exit_code: Some(0),
                    signal: None,
                })
                .await;
        }
        FakeGuestScript::HangForever => {
            if !guest.ready().await {
                return;
            }
            let Some(inv) = guest.wait_invoke().await else {
                return;
            };
            guest
                .log(
                    LogPhase::Handler,
                    Some(&inv.attempt_id),
                    "fake guest: hanging forever",
                )
                .await;
            // Record Cancel / Shutdown but never answer the attempt.
            while let Some(msg) = guest.recv().await {
                if matches!(msg, HostMessage::Shutdown { .. }) {
                    break;
                }
            }
        }
        FakeGuestScript::DisconnectAfterInvoke => {
            if !guest.ready().await {
                return;
            }
            let _ = guest.wait_invoke().await;
            // Dropping `guest` closes the duplex stream: the host sees EOF.
        }
        FakeGuestScript::CrashAfterReady { exit_code } => {
            if !guest.ready().await {
                return;
            }
            let Some(inv) = guest.wait_invoke().await else {
                return;
            };
            guest
                .error(
                    &inv,
                    GuestErrorKind::Crash {
                        exit_code: Some(exit_code),
                        signal: None,
                    },
                    "Runtime.Crash",
                    &format!("user process exited with code {exit_code}"),
                )
                .await;
            guest
                .send(&GuestMessage::Exited {
                    exit_code: Some(exit_code),
                    signal: None,
                })
                .await;
        }
        // Answer once, then never read another frame. The stream stays open,
        // so the host sees a healthy-looking connection with nothing on it.
        FakeGuestScript::RespondOkThenStopAnswering(payload) => {
            if !guest.ready().await {
                return;
            }
            let Some(inv) = guest.wait_invoke().await else {
                return;
            };
            if guest.respond(&inv, inv.epoch, payload).await {
                std::future::pending::<()>().await;
            }
        }
        FakeGuestScript::RespondOk(_)
        | FakeGuestScript::Echo
        | FakeGuestScript::EchoHttp
        | FakeGuestScript::HandlerError { .. }
        | FakeGuestScript::Panic
        | FakeGuestScript::RespondOkThenExit(_)
        | FakeGuestScript::WrongEpochThenOk(_) => {
            if !guest.ready().await {
                return;
            }
            let Some(inv) = guest.wait_invoke().await else {
                return;
            };
            let ok = match script {
                FakeGuestScript::RespondOk(v) => guest.respond(&inv, inv.epoch, v).await,
                FakeGuestScript::Echo => {
                    let v = inv.payload.clone();
                    guest.respond(&inv, inv.epoch, v).await
                }
                FakeGuestScript::EchoHttp => {
                    let v = http_echo(&inv);
                    guest.respond(&inv, inv.epoch, v).await
                }
                FakeGuestScript::HandlerError {
                    error_type,
                    message,
                } => {
                    guest
                        .error(&inv, GuestErrorKind::Handler, &error_type, &message)
                        .await
                }
                FakeGuestScript::Panic => {
                    guest
                        .error(
                            &inv,
                            GuestErrorKind::Panic,
                            "Runtime.Panic",
                            "handler panicked: fake panic",
                        )
                        .await
                }
                FakeGuestScript::WrongEpochThenOk(v) => {
                    guest.respond(&inv, inv.epoch + 1000, v.clone()).await
                        && guest.respond(&inv, inv.epoch, v).await
                }
                // The answer and the death notice arrive back to back; the
                // stream stays open, so only a read tells the host about it.
                FakeGuestScript::RespondOkThenExit(v) => {
                    guest.respond(&inv, inv.epoch, v).await
                        && guest
                            .send(&GuestMessage::Exited {
                                exit_code: Some(0),
                                signal: None,
                            })
                            .await
                }
                _ => unreachable!("handled above"),
            };
            if ok {
                guest.drain_until_shutdown().await;
            }
        }
        // One environment, many sequential attempts: the guest side of
        // environment reuse. The loop only ends when the host sends
        // `Shutdown` or the stream dies.
        FakeGuestScript::RespondOkForever(_)
        | FakeGuestScript::EchoForever
        | FakeGuestScript::StaleEpochThenOkForever(_) => {
            if !guest.ready().await {
                return;
            }
            loop {
                let Some(inv) = guest.wait_invoke().await else {
                    return;
                };
                let ok = match &script {
                    FakeGuestScript::RespondOkForever(v) => {
                        guest.respond(&inv, inv.epoch, v.clone()).await
                    }
                    FakeGuestScript::EchoForever => {
                        let v = inv.payload.clone();
                        guest.respond(&inv, inv.epoch, v).await
                    }
                    FakeGuestScript::StaleEpochThenOkForever(v) => {
                        guest
                            .respond(
                                &inv,
                                inv.epoch.saturating_sub(1),
                                serde_json::json!({"stale": true}),
                            )
                            .await
                            && guest.respond(&inv, inv.epoch, v.clone()).await
                    }
                    _ => unreachable!("handled above"),
                };
                if !ok {
                    return;
                }
            }
        }
        FakeGuestScript::Custom(_) => unreachable!("handled above"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tachyon_serverless_domain::{
        EgressProfile, ResourceProfile, RevisionId, Sha256Digest, TenantId,
    };
    use tokio::io::{AsyncRead, AsyncWrite};

    fn spec(id: &EnvironmentId) -> EnvironmentSpec {
        EnvironmentSpec {
            environment_id: id.clone(),
            tenant_id: TenantId::generate(),
            revision_id: RevisionId::generate(),
            artifact: ArtifactLocation {
                path: PathBuf::from("/nonexistent"),
                digest: Sha256Digest::of_bytes(b"x"),
                size_bytes: 1,
            },
            architecture: Architecture::Aarch64,
            resources: ResourceProfile::default(),
            egress: EgressProfile::None,
            connect_timeout: Duration::from_secs(1),
        }
    }

    struct Host {
        reader: FramedRead<
            ReadHalf<Box<dyn tachyon_serverless_provider_port::BridgeStream>>,
            FrameCodec,
        >,
        writer: FramedWrite<
            WriteHalf<Box<dyn tachyon_serverless_provider_port::BridgeStream>>,
            FrameCodec,
        >,
    }

    impl Host {
        fn new(stream: Box<dyn tachyon_serverless_provider_port::BridgeStream>) -> Self {
            let (r, w) = tokio::io::split(stream);
            Self {
                reader: FramedRead::new(r, FrameCodec),
                writer: FramedWrite::new(w, FrameCodec),
            }
        }
        async fn send(&mut self, m: &HostMessage) {
            self.writer.send(encode_message(m).unwrap()).await.unwrap();
        }
        async fn recv(&mut self) -> Option<GuestMessage> {
            let f = self.reader.next().await?.ok()?;
            Some(decode_message(&f).unwrap())
        }
        async fn recv_non_log(&mut self) -> Option<GuestMessage> {
            loop {
                match self.recv().await? {
                    GuestMessage::Log { .. } | GuestMessage::Heartbeat { .. } => continue,
                    m => return Some(m),
                }
            }
        }
    }

    fn ack(id: &EnvironmentId) -> HostMessage {
        HostMessage::HelloAck {
            environment_id: id.to_string(),
            epoch: 1,
            entrypoint: "/function/app".into(),
            args: vec![],
            env: vec![],
            working_dir: "/tmp".into(),
            init_timeout_ms: 1000,
            max_response_bytes: 1024,
            max_log_line_bytes: 1024,
        }
    }

    fn invoke(attempt: &str) -> HostMessage {
        HostMessage::Invoke {
            invocation_id: "inv_x".into(),
            attempt_id: attempt.into(),
            epoch: 1,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: now_ms() + 1000,
            remaining_ms: 1000,
            trace_id: "t".into(),
            payload: serde_json::json!({"hello": "world"}),
        }
    }

    // Make sure the trait object is usable as a stream (compile-time check).
    fn _assert_stream<T: AsyncRead + AsyncWrite + Unpin + Send>() {}

    #[tokio::test]
    async fn respond_ok_speaks_protocol_and_records_lifecycle() {
        let provider = FakeExecutionProvider::with_scripts([FakeGuestScript::RespondOk(
            serde_json::json!({"answer": 42}),
        )]);
        let id = EnvironmentId::generate();
        let handle = provider.create_environment(spec(&id)).await.unwrap();
        assert_eq!(handle.evidence.host_pid, Some(std::process::id()));
        let mut host = Host::new(handle.stream);
        match host.recv().await.unwrap() {
            GuestMessage::Hello {
                protocol_version,
                environment_id,
                ..
            } => {
                assert_eq!(protocol_version, PROTOCOL_VERSION);
                assert_eq!(environment_id, id.to_string());
            }
            other => panic!("expected Hello, got {other:?}"),
        }
        host.send(&ack(&id)).await;
        assert!(matches!(
            host.recv_non_log().await.unwrap(),
            GuestMessage::Ready { .. }
        ));
        host.send(&invoke("att_1")).await;
        match host.recv_non_log().await.unwrap() {
            GuestMessage::Response {
                attempt_id,
                epoch,
                payload,
                ..
            } => {
                assert_eq!(attempt_id, "att_1");
                assert_eq!(epoch, 1);
                assert_eq!(payload, serde_json::json!({"answer": 42}));
            }
            other => panic!("expected Response, got {other:?}"),
        }
        assert!(provider.hello_ack(&id).is_some());
        assert_eq!(provider.created(), vec![id.clone()]);
        let r = provider
            .terminate_environment(&id, TerminateReason::Completed)
            .await
            .unwrap();
        assert!(r.was_running);
        let r = provider
            .terminate_environment(&id, TerminateReason::Completed)
            .await
            .unwrap();
        assert!(!r.was_running, "terminate is idempotent");
        assert_eq!(provider.terminated().len(), 2);
        assert!(provider.running().is_empty());
        assert_eq!(
            provider.observe_environment(&id).await.unwrap(),
            EnvironmentObservation::Exited {
                exit_code: Some(0),
                signal: None
            }
        );
    }

    #[tokio::test]
    async fn disconnect_after_invoke_closes_stream() {
        let provider =
            FakeExecutionProvider::with_scripts([FakeGuestScript::DisconnectAfterInvoke]);
        let id = EnvironmentId::generate();
        let handle = provider.create_environment(spec(&id)).await.unwrap();
        let mut host = Host::new(handle.stream);
        host.recv().await.unwrap();
        host.send(&ack(&id)).await;
        assert!(matches!(
            host.recv_non_log().await.unwrap(),
            GuestMessage::Ready { .. }
        ));
        host.send(&invoke("att_1")).await;
        assert!(host.recv_non_log().await.is_none(), "stream must be closed");
    }

    #[tokio::test]
    async fn init_error_and_duplicate_id() {
        let provider = FakeExecutionProvider::with_scripts([FakeGuestScript::InitError {
            message: "boom".into(),
        }]);
        let id = EnvironmentId::generate();
        let handle = provider.create_environment(spec(&id)).await.unwrap();
        assert!(matches!(
            provider.create_environment(spec(&id)).await,
            Err(ProviderError::InvalidSpec(_))
        ));
        let mut host = Host::new(handle.stream);
        host.recv().await.unwrap();
        host.send(&ack(&id)).await;
        assert!(matches!(
            host.recv_non_log().await.unwrap(),
            GuestMessage::InitError { .. }
        ));
    }

    #[tokio::test]
    async fn capabilities_are_dev_only() {
        let caps = FakeExecutionProvider::new().capabilities();
        assert!(caps.dev_only);
        assert!(caps.create_terminate.is_supported());
        assert!(!caps.snapshot_create.is_supported());
        assert!(
            !caps.idle_quiesce.is_supported() && !caps.idle_resume.is_supported(),
            "the default fake is destroy-after-invoke, like both shipped providers"
        );
    }

    /// PLT-4633: quiesce and resume are recorded, are idempotent, and can be
    /// made to fail so the pool's two fallback paths can be exercised.
    #[tokio::test]
    async fn idle_quiesce_and_resume_are_recorded_and_can_be_made_to_fail() {
        let provider = FakeExecutionProvider::new();
        let id = EnvironmentId::generate();
        let _handle = provider.create_environment(spec(&id)).await.unwrap();

        provider.idle_quiesce(&id).await.unwrap();
        assert_eq!(provider.paused(), vec![id.clone()]);
        provider.idle_quiesce(&id).await.unwrap();
        assert_eq!(provider.paused(), vec![id.clone()], "quiesce is idempotent");
        provider.idle_resume(&id).await.unwrap();
        assert!(provider.paused().is_empty());
        assert_eq!(provider.quiesced().len(), 2);
        assert_eq!(provider.resumed(), vec![id.clone()]);

        // An environment the provider does not know cannot be paused.
        let missing = EnvironmentId::generate();
        assert!(matches!(
            provider.idle_quiesce(&missing).await,
            Err(ProviderError::NotFound(_))
        ));

        // The failure modes report the configured reason and change nothing.
        let failing = FakeExecutionProvider::with_options(FakeProviderOptions {
            fail_quiesce: Some("quiesce refused".into()),
            fail_resume: Some("resume refused".into()),
            ..FakeProviderOptions::default()
        });
        let id = EnvironmentId::generate();
        let _handle = failing.create_environment(spec(&id)).await.unwrap();
        assert!(matches!(
            failing.idle_quiesce(&id).await,
            Err(ProviderError::Internal(m)) if m == "quiesce refused"
        ));
        assert!(matches!(
            failing.idle_resume(&id).await,
            Err(ProviderError::Internal(m)) if m == "resume refused"
        ));
        assert!(
            failing.paused().is_empty(),
            "a failed quiesce pauses nothing"
        );
        assert_eq!(failing.quiesced(), vec![id.clone()], "the call is recorded");
        assert_eq!(failing.resumed(), vec![id]);
    }

    /// A provider without idle support keeps the port's default, which refuses
    /// both calls — the state every shipped provider was in before PLT-4633.
    #[tokio::test]
    async fn the_port_default_refuses_quiesce_and_resume() {
        struct Bare;
        #[async_trait]
        impl ExecutionProvider for Bare {
            fn kind(&self) -> ProviderKind {
                ProviderKind::Fake
            }
            fn capabilities(&self) -> Capabilities {
                FakeExecutionProvider::new().capabilities()
            }
            async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
                unimplemented!()
            }
            async fn validate_artifact(
                &self,
                _: &ArtifactLocation,
                _: Architecture,
            ) -> Result<(), ProviderError> {
                unimplemented!()
            }
            async fn create_environment(
                &self,
                _: EnvironmentSpec,
            ) -> Result<EnvironmentHandle, ProviderError> {
                unimplemented!()
            }
            async fn terminate_environment(
                &self,
                _: &EnvironmentId,
                _: TerminateReason,
            ) -> Result<TerminateReport, ProviderError> {
                unimplemented!()
            }
            async fn observe_environment(
                &self,
                _: &EnvironmentId,
            ) -> Result<EnvironmentObservation, ProviderError> {
                unimplemented!()
            }
            async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
                unimplemented!()
            }
        }
        let id = EnvironmentId::generate();
        for r in [Bare.idle_quiesce(&id).await, Bare.idle_resume(&id).await] {
            assert!(matches!(r, Err(ProviderError::Unavailable(_))), "{r:?}");
        }
    }

    #[tokio::test]
    async fn warm_capable_fake_reports_idle_support_and_serves_many_attempts() {
        let provider = FakeExecutionProvider::with_options(FakeProviderOptions {
            warm_capable: true,
            ..FakeProviderOptions::default()
        });
        let caps = provider.capabilities();
        assert!(caps.idle_quiesce.is_supported() && caps.idle_resume.is_supported());

        provider.push_script(FakeGuestScript::EchoForever);
        let id = EnvironmentId::generate();
        let handle = provider.create_environment(spec(&id)).await.unwrap();
        let mut host = Host::new(handle.stream);
        host.recv().await.unwrap();
        host.send(&ack(&id)).await;
        assert!(matches!(
            host.recv_non_log().await.unwrap(),
            GuestMessage::Ready { .. }
        ));
        // Three sequential attempts, one environment, no second Ready.
        for n in 0..3 {
            host.send(&invoke(&format!("att_{n}"))).await;
            match host.recv_non_log().await.unwrap() {
                GuestMessage::Response {
                    attempt_id,
                    payload,
                    ..
                } => {
                    assert_eq!(attempt_id, format!("att_{n}"));
                    assert_eq!(payload, serde_json::json!({"hello": "world"}));
                }
                other => panic!("expected a Response for attempt {n}, got {other:?}"),
            }
        }
        assert_eq!(provider.created(), vec![id], "one environment served all 3");
    }
}
