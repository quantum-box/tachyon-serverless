//! Dispatcher leases, fencing and `Idempotency-Key` across gateways
//! (PLT-4631): two applications on one `data_dir`, a completion delayed past
//! a reclaim, lease expiry judged with clocks that disagree, and replays of an
//! invocation another gateway drives.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_util::codec::{FramedRead, FramedWrite};

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_application::repository::HeartbeatOutcome;
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeOutcome, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, Architecture, Clock, EnvironmentId, EnvironmentState, EventKind, FixedClock,
    Function, InvocationId, InvocationStatus, ProviderKind, RevisionStatus, TenantId, Timestamp,
};
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message, encode_message,
};
use tachyon_serverless_provider_fake::{
    CustomScriptContext, FakeExecutionProvider, FakeGuestScript, ScriptFuture,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, Capabilities, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec,
    ExecutionProvider, PreflightReport, Principal, ProviderError, Role, TerminateReason,
    TerminateReport,
};

const TENANT: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";

fn config(data_dir: &std::path::Path, instance: &str) -> GatewayConfig {
    GatewayConfig::from_toml(&format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process"

[[identity.tokens]]
token = "tok"
tenant_id = "{TENANT}"
subject = "dev"
roles = ["deploy", "invoke"]

[invoke]
cancel_grace_ms = 100

[dispatcher]
instance = "{instance}"
lease_ttl_seconds = 30
heartbeat_interval_seconds = 10
max_clock_skew_ms = 2000
"#,
        data = data_dir.display(),
    ))
    .unwrap()
}

fn start(
    dir: &std::path::Path,
    instance: &str,
    fake: &Arc<FakeExecutionProvider>,
    clock: Arc<dyn Clock>,
) -> Arc<Application> {
    Application::bootstrap_with(
        config(dir, instance),
        fake.clone(),
        BootstrapOptions {
            persist_state: true,
            clock,
            ..BootstrapOptions::default()
        },
    )
    .unwrap()
}

fn principal() -> Principal {
    Principal {
        subject: "dev".into(),
        tenant_id: TenantId::parse(TENANT).unwrap(),
        roles: vec![Role::Deploy, Role::Invoke],
    }
}

fn epoch_start() -> Timestamp {
    use chrono::TimeZone;
    chrono::Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap()
}

async fn deploy(app: &Application, name: &str) -> Function {
    let p = principal();
    let artifact = app
        .artifact_service
        .upload(&p, format!("#!/bin/sh\necho {name}\n").as_bytes())
        .await
        .unwrap();
    let function = app.functions.create(&p, name, "").unwrap();
    let rev = app
        .revisions
        .create(
            &p,
            &function.id,
            &CreateRevisionRequest {
                artifact: ArtifactRequest::Binary {
                    digest: artifact.digest.to_string(),
                },
                architecture: "aarch64".into(),
                resources: ResourcesRequest::default(),
                execution: ExecutionRequest {
                    timeout_seconds: 60,
                    initialization_timeout_seconds: 30,
                    max_concurrency: 8,
                    ..ExecutionRequest::default()
                },
                egress: None,
                egress_allow: Vec::new(),
                env_vars: vec![],
                secrets: vec![],
                description: String::new(),
                publish_to_prod: true,
                required_region: None,
                restore: None,
            },
        )
        .await
        .unwrap();
    let rev = app
        .revisions
        .wait_terminal(&rev.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(rev.status, RevisionStatus::Ready);
    app.aliases
        .get(&p, &function.id, &AliasName::default_alias())
        .unwrap();
    function
}

fn request(function: &Function, payload: serde_json::Value, key: Option<&str>) -> InvokeRequest {
    InvokeRequest {
        principal: principal(),
        function_id: function.id.clone(),
        alias: None,
        revision_id: None,
        event_kind: EventKind::Json,
        payload,
        idempotency_key: key.map(str::to_string),
        client_timeout_ms: Some(20_000),
        trace_id: None,
    }
}

/// A guest that answers its `Invoke` with `payload` only once `gate` opens,
/// using the `(attempt_id, epoch)` of the frame it received: exactly the
/// callback a slow handler sends late.
fn gated(gate: watch::Receiver<bool>, payload: serde_json::Value) -> FakeGuestScript {
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        let mut gate = gate.clone();
        let payload = payload.clone();
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "lease-test".into(),
                environment_id: ctx.environment_id.to_string(),
                guest_boot_id: Some("boot".into()),
                architecture: "aarch64".into(),
            };
            if writer.send(encode_message(&hello).unwrap()).await.is_err() {
                return;
            }
            if !matches!(reader.next().await, Some(Ok(_))) {
                return;
            }
            let ready = encode_message(&GuestMessage::Ready { init_ms: 1 }).unwrap();
            if writer.send(ready).await.is_err() {
                return;
            }
            while let Some(Ok(frame)) = reader.next().await {
                if let Ok(HostMessage::Invoke {
                    attempt_id, epoch, ..
                }) = decode_message::<HostMessage>(&frame)
                {
                    let _ = gate.wait_for(|open| *open).await;
                    let response = GuestMessage::Response {
                        attempt_id,
                        epoch,
                        payload: payload.clone(),
                        handler_ms: Some(1),
                    };
                    if writer
                        .send(encode_message(&response).unwrap())
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        })
    }))
}

async fn wait_running(app: &Application, function: &Function) -> InvocationId {
    for _ in 0..500 {
        if let Some(inv) = app
            .repos
            .invocations
            .list_by_function(&function.id, 10)
            .unwrap()
            .into_iter()
            .find(|i| i.status == InvocationStatus::Running)
        {
            return inv.id;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the invocation never started running");
}

fn status(app: &Application, id: &InvocationId) -> InvocationStatus {
    app.repos.invocations.get(id).unwrap().unwrap().status
}

/// Dispatcher double start (acceptance 3, ADR-0003 「2 つの gateway が同じ
/// data_dir」): a second gateway bootstrapped on the same `data_dir` while the
/// first one is driving an invocation neither settles that invocation nor
/// terminates or marks its environment, however many reconcile and reclaim
/// passes it runs. The first gateway finishes the invocation normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_gateways_on_one_data_dir_never_settle_each_others_work() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let (open, gate) = watch::channel(false);
    fake.push_script(gated(gate, serde_json::json!({"from": "a"})));
    let clock: Arc<dyn Clock> = Arc::new(tachyon_serverless_domain::SystemClock);
    let a = start(dir.path(), "gateway-a", &fake, clock.clone());
    let function = deploy(&a, "shared").await;

    let running = {
        let a = a.clone();
        let req = request(&function, serde_json::json!({"n": 1}), None);
        tokio::spawn(async move { a.invoke.invoke(req).await })
    };
    let inv = wait_running(&a, &function).await;
    let env_id = a.repos.invocations.attempts_of(&inv).unwrap()[0]
        .environment_id
        .clone();

    // The second gateway starts on the same data_dir and runs everything a
    // start and its timers run.
    let b = start(dir.path(), "gateway-b", &fake, clock.clone());
    assert_ne!(a.dispatcher.id(), b.dispatcher.id());
    let report = b.reconcile_on_startup().await.unwrap();
    assert_eq!(report.terminated, 0, "{report:?}");
    assert_eq!(report.lost, 0, "{report:?}");
    assert!(report.foreign >= 1, "{report:?}");
    assert_eq!(report.reclaim.dispatchers, 0, "{report:?}");
    assert_eq!(b.reclaim_expired().await.dispatchers, 0);
    assert_eq!(
        b.heartbeat(),
        HeartbeatOutcome::Renewed { leases: 0 },
        "each gateway renews only its own"
    );
    assert!(matches!(
        a.heartbeat(),
        HeartbeatOutcome::Renewed { leases: 1 }
    ));
    assert_eq!(status(&b, &inv), InvocationStatus::Running);
    let env = b.repos.environments.get(&env_id).unwrap().unwrap();
    assert_eq!(env.state, EnvironmentState::Busy);
    assert!(!env.is_fenced());
    assert!(fake.terminated().is_empty(), "{:?}", fake.terminated());
    // Nor can it cancel what it does not drive: that would record a cancel
    // while the handler keeps running on the other gateway.
    let cancel = b.invoke.cancel(&principal(), &inv).await.err().unwrap();
    assert!(matches!(cancel, AppError::Conflict(_)), "{cancel}");
    assert_eq!(status(&b, &inv), InvocationStatus::Running);

    // The first gateway completes its own invocation.
    open.send_replace(true);
    let out = running.await.unwrap().unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert_eq!(out.output, Some(serde_json::json!({"from": "a"})));
    assert_eq!(status(&b, &inv), InvocationStatus::Succeeded);

    // And the second one serves its own.
    fake.push_script(FakeGuestScript::RespondOk(serde_json::json!({"from": "b"})));
    let own = b
        .invoke
        .invoke(request(&function, serde_json::json!({"n": 2}), None))
        .await
        .unwrap();
    assert!(own.succeeded(), "{:?}", own.invocation().status);
    assert_eq!(
        own.invocation().dispatcher_id.as_ref(),
        Some(b.dispatcher.id())
    );
}

/// Idempotency across gateways (acceptance 2): while one gateway runs an
/// invocation, the same key and input sent to the other gateway returns that
/// invocation once it finishes, without a second execution; a different
/// input is a 409 naming it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_replayed_on_another_gateway_returns_the_same_invocation_and_never_runs_twice() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    // Anything beyond the first environment fails to boot instead of running.
    fake.set_default_script(None);
    let (open, gate) = watch::channel(false);
    fake.push_script(gated(gate, serde_json::json!({"once": true})));
    let clock: Arc<dyn Clock> = Arc::new(tachyon_serverless_domain::SystemClock);
    let a = start(dir.path(), "gateway-a", &fake, clock.clone());
    let function = deploy(&a, "keyed").await;
    let b = start(dir.path(), "gateway-b", &fake, clock);

    let first = {
        let a = a.clone();
        let req = request(&function, serde_json::json!({"k": 1}), Some("same"));
        tokio::spawn(async move { a.invoke.invoke(req).await })
    };
    let inv = wait_running(&a, &function).await;
    let replay = {
        let b = b.clone();
        let req = request(&function, serde_json::json!({"k": 1}), Some("same"));
        tokio::spawn(async move { b.invoke.invoke(req).await })
    };
    let conflict = b
        .invoke
        .invoke(request(
            &function,
            serde_json::json!({"k": 2}),
            Some("same"),
        ))
        .await
        .err()
        .unwrap();
    match &conflict {
        AppError::IdempotencyConflict { invocation_id, .. } => assert_eq!(invocation_id, &inv),
        other => panic!("expected a 409, got {other}"),
    }
    assert_eq!(conflict.http_status(), 409);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !replay.is_finished(),
        "the replay follows the running invocation"
    );
    open.send_replace(true);

    let first: InvokeOutcome = first.await.unwrap().unwrap();
    let replayed: InvokeOutcome = replay.await.unwrap().unwrap();
    assert!(first.succeeded());
    assert!(!first.replayed);
    assert!(replayed.replayed);
    assert!(replayed.succeeded(), "{:?}", replayed.invocation().status);
    assert_eq!(replayed.invocation().id, inv);
    assert_eq!(replayed.output, Some(serde_json::json!({"once": true})));
    assert_eq!(
        fake.created().len(),
        1,
        "executed once across both gateways"
    );
}

/// Delayed callback and lease expiry with clock skew (acceptance 1 and 3):
/// gateway A stops renewing (its heartbeat is simply not run) while its
/// handler is still running. Gateway B's clock runs ahead; it reclaims only
/// once its clock is past A's lease plus the tolerated skew, and exactly once.
/// The reclaim settles the invocation (`OutcomeUnknown{Host.LeaseExpired}`)
/// and fences the environment. When A's handler finally answers, the late
/// completion is refused and changes nothing; A itself is fenced and takes no
/// new work. The environment is settled only after B's terminate succeeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_completion_delayed_past_a_reclaim_is_refused_and_the_slot_is_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let (open, gate) = watch::channel(false);
    fake.push_script(gated(gate, serde_json::json!({"late": true})));
    let clock_a = Arc::new(FixedClock::new(epoch_start()));
    let clock_b = Arc::new(FixedClock::new(epoch_start()));
    let a = start(dir.path(), "gateway-a", &fake, clock_a.clone());
    let function = deploy(&a, "late").await;
    let b = start(dir.path(), "gateway-b", &fake, clock_b.clone());

    let running = {
        let a = a.clone();
        let req = request(&function, serde_json::json!({}), None);
        tokio::spawn(async move { a.invoke.invoke(req).await })
    };
    let inv = wait_running(&a, &function).await;
    let attempt = a.repos.invocations.attempts_of(&inv).unwrap()[0].clone();
    let env_id = attempt.environment_id.clone();

    // B keeps renewing its own lease (a reclaimer without one reclaims
    // nothing). B's clock is 31 s ahead: A's 30 s lease has passed by 1 s,
    // which is within the 2 s skew B must tolerate. Nothing is reclaimed.
    clock_b.set(epoch_start() + chrono::Duration::seconds(20));
    assert_eq!(b.heartbeat(), HeartbeatOutcome::Renewed { leases: 0 });
    clock_b.set(epoch_start() + chrono::Duration::seconds(31));
    let early = b.reclaim_expired().await;
    assert_eq!((early.dispatchers, early.leases), (0, 0), "{early:?}");
    assert_eq!(status(&a, &inv), InvocationStatus::Running);

    // Past lease + skew: reclaimed once. Only the ledger half first, so the
    // guest is still alive to send its late answer.
    clock_b.set(epoch_start() + chrono::Duration::seconds(33));
    let reclaimed = b.dispatcher.reclaim_ledger().unwrap();
    assert_eq!(reclaimed.dispatchers, vec![a.dispatcher.id().clone()]);
    assert_eq!(reclaimed.leases, 1);
    assert_eq!(reclaimed.fenced.len(), 1);
    assert!(
        b.dispatcher.reclaim_ledger().unwrap().is_empty(),
        "exactly once"
    );
    let settled = status(&b, &inv);
    match &settled {
        InvocationStatus::OutcomeUnknown { error } => {
            assert_eq!(error.error_type, "Host.LeaseExpired")
        }
        other => panic!("{other:?}"),
    }
    let fenced = b.repos.environments.get(&env_id).unwrap().unwrap();
    assert!(fenced.is_fenced());
    assert_eq!(fenced.state, EnvironmentState::Draining);
    assert_eq!(fenced.epoch, attempt.epoch + 1);

    // The delayed callback arrives, carrying the old (attempt, epoch).
    open.send_replace(true);
    let out = running.await.unwrap().unwrap();
    assert_eq!(
        out.invocation().status,
        settled,
        "the reclaim's outcome stands"
    );
    assert_eq!(out.output, None, "a refused completion hands out no result");
    assert_eq!(status(&b, &inv), settled);
    let lease_released_by_reclaim = b.repos.environments.get(&env_id).unwrap().unwrap();
    assert!(
        lease_released_by_reclaim.is_fenced(),
        "the late completion did not unfence or pool the environment"
    );

    // A lost its lease: its heartbeat is refused and it takes no new work.
    assert_eq!(a.heartbeat(), HeartbeatOutcome::Fenced);
    let refused = a
        .invoke
        .invoke(request(&function, serde_json::json!({}), None))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(refused, AppError::ProviderUnavailable(_)),
        "{refused}"
    );

    // Expiry alone freed nothing: only the terminate B confirms settles it.
    let finished = b.reclaim_expired().await;
    assert_eq!(finished.terminated, 1, "{finished:?}");
    assert!(
        fake.terminated()
            .contains(&(env_id.clone(), TerminateReason::Reconcile)),
        "{:?}",
        fake.terminated()
    );
    assert!(matches!(
        b.repos.environments.get(&env_id).unwrap().unwrap().state,
        EnvironmentState::Lost { .. }
    ));
    let _ = clock_a;
}

/// A dispatcher that renews keeps its work, whatever the other gateway's
/// clock says within its lease; one that stops gracefully is taken over at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewal_keeps_the_lease_and_a_graceful_stop_hands_over_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let (open, gate) = watch::channel(false);
    fake.push_script(gated(gate, serde_json::json!({"ok": 1})));
    let clock_a = Arc::new(FixedClock::new(epoch_start()));
    let clock_b = Arc::new(FixedClock::new(epoch_start()));
    let a = start(dir.path(), "gateway-a", &fake, clock_a.clone());
    let function = deploy(&a, "renewed").await;
    let b = start(dir.path(), "gateway-b", &fake, clock_b.clone());
    let running = {
        let a = a.clone();
        let req = request(&function, serde_json::json!({}), None);
        tokio::spawn(async move { a.invoke.invoke(req).await })
    };
    let inv = wait_running(&a, &function).await;

    // A renews every 20 s; B's clock follows 25 s later each time.
    for step in 1..=4 {
        clock_a.set(epoch_start() + chrono::Duration::seconds(20 * step));
        assert_eq!(a.heartbeat(), HeartbeatOutcome::Renewed { leases: 1 });
        clock_b.set(epoch_start() + chrono::Duration::seconds(20 * step + 25));
        assert!(
            b.dispatcher.reclaim_ledger().unwrap().is_empty(),
            "step {step}"
        );
    }
    open.send_replace(true);
    assert!(running.await.unwrap().unwrap().succeeded());
    assert_eq!(status(&b, &inv), InvocationStatus::Succeeded);

    // A graceful stop: B may take over at once, even with its clock behind.
    a.stop_dispatcher();
    clock_b.set(epoch_start());
    let report = b.dispatcher.reclaim_ledger().unwrap();
    assert_eq!(report.dispatchers, vec![a.dispatcher.id().clone()]);
}

/// Fails `terminate_environment` while `failing` is set; everything else goes
/// to the fake.
struct FlakyTerminate {
    fake: Arc<FakeExecutionProvider>,
    failing: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl ExecutionProvider for FlakyTerminate {
    fn kind(&self) -> ProviderKind {
        self.fake.kind()
    }
    fn capabilities(&self) -> Capabilities {
        self.fake.capabilities()
    }
    async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
        self.fake.preflight().await
    }
    async fn validate_artifact(
        &self,
        artifact: &ArtifactLocation,
        architecture: Architecture,
    ) -> Result<(), ProviderError> {
        self.fake.validate_artifact(artifact, architecture).await
    }
    async fn create_environment(
        &self,
        spec: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError> {
        self.fake.create_environment(spec).await
    }
    async fn terminate_environment(
        &self,
        environment_id: &EnvironmentId,
        reason: TerminateReason,
    ) -> Result<TerminateReport, ProviderError> {
        if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ProviderError::Unavailable("host is not answering".into()));
        }
        self.fake
            .terminate_environment(environment_id, reason)
            .await
    }
    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        self.fake.observe_environment(environment_id).await
    }
    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
        self.fake.list_environments().await
    }
}

/// Acceptance 3 when the host does not confirm: an environment fenced after
/// its owner's lease expired stays fenced — out of the pool, not acquirable,
/// not settled — for as long as the provider cannot terminate it, and is
/// settled only by the pass whose terminate succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_environment_stays_fenced_until_its_terminate_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let (open, gate) = watch::channel(false);
    fake.push_script(gated(gate, serde_json::json!({"never": "accepted"})));
    let provider = Arc::new(FlakyTerminate {
        fake: fake.clone(),
        failing: std::sync::atomic::AtomicBool::new(false),
    });
    let clock_a = Arc::new(FixedClock::new(epoch_start()));
    let clock_b = Arc::new(FixedClock::new(epoch_start()));
    let bootstrap = |instance: &str, clock: Arc<FixedClock>| {
        Application::bootstrap_with(
            config(dir.path(), instance),
            provider.clone(),
            BootstrapOptions {
                persist_state: true,
                clock,
                ..BootstrapOptions::default()
            },
        )
        .unwrap()
    };
    let a = bootstrap("gateway-a", clock_a);
    let function = deploy(&a, "stuck").await;
    let b = bootstrap("gateway-b", clock_b.clone());
    let running = {
        let a = a.clone();
        let req = request(&function, serde_json::json!({}), None);
        tokio::spawn(async move { a.invoke.invoke(req).await })
    };
    let inv = wait_running(&a, &function).await;
    let env_id = a.repos.invocations.attempts_of(&inv).unwrap()[0]
        .environment_id
        .clone();

    provider
        .failing
        .store(true, std::sync::atomic::Ordering::SeqCst);
    clock_b.set(epoch_start() + chrono::Duration::seconds(20));
    assert_eq!(b.heartbeat(), HeartbeatOutcome::Renewed { leases: 0 });
    clock_b.set(epoch_start() + chrono::Duration::seconds(40));
    for pass in 0..3 {
        let summary = b.reclaim_expired().await;
        assert_eq!(summary.terminated, 0, "pass {pass}: {summary:?}");
        assert_eq!(summary.pending, 1, "pass {pass}: {summary:?}");
        let env = b.repos.environments.get(&env_id).unwrap().unwrap();
        assert!(env.is_fenced(), "pass {pass}");
        assert_eq!(env.state, EnvironmentState::Draining, "pass {pass}");
        assert!(
            b.repos
                .slots
                .claim_for_reuse(&env.reuse_key, env.owner.as_ref(), clock_b.now())
                .unwrap()
                .is_none()
        );
        assert_eq!(b.repos.slots.list_fenced().unwrap().len(), 1);
    }
    assert!(fake.running().contains(&env_id), "still on the host");

    provider
        .failing
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let summary = b.reclaim_expired().await;
    assert_eq!((summary.terminated, summary.pending), (1, 0), "{summary:?}");
    assert!(matches!(
        b.repos.environments.get(&env_id).unwrap().unwrap().state,
        EnvironmentState::Lost { .. }
    ));
    assert!(b.repos.slots.list_fenced().unwrap().is_empty());
    open.send_replace(true);
    let out = running.await.unwrap().unwrap();
    assert!(matches!(
        out.invocation().status,
        InvocationStatus::OutcomeUnknown { .. }
    ));
}
