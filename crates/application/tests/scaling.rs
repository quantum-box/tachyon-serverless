//! Scale to zero, `min_ready`, cooldown, drains on alias switch / secret
//! rotation / deletion, and the timer races between them (PLT-4635,
//! docs/adr/0009-scale-to-zero-and-drain.md), through the invoke pipeline
//! with the fake provider.
//!
//! The clock is the wall clock plus an offset a test moves ([`ShiftedClock`]),
//! so idle TTLs, cooldowns and drain timeouts pass instantly while the
//! guest's own sleeps stay real. Every test calls the scale reconciler
//! directly (`Application::reconcile_scaling`) instead of waiting for the
//! gateway's loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ErrorCode, ExecutionRequest, ResourcesRequest,
    RevisionCapacityInfo, SecretBindingRequest,
};
use tachyon_serverless_application::control::{ConfigDelivery, ConfigSource, SourceError};
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeOutcome, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, Clock, EnvironmentId, EnvironmentState, ErrorClass, EventKind, FixedClock, Function,
    FunctionRevision, InvocationStatus, RevisionStatus, StartKind, TenantId, Timestamp,
};
use tachyon_serverless_provider_fake::{
    FakeExecutionProvider, FakeGuestScript, FakeProviderOptions,
};
use tachyon_serverless_provider_port::{Principal, Role, TerminateReason};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";

struct ShiftedClock(Arc<AtomicI64>);

impl Clock for ShiftedClock {
    fn now(&self) -> Timestamp {
        chrono::Utc::now() + chrono::Duration::milliseconds(self.0.load(Ordering::SeqCst))
    }
}

struct RotatingSecrets(std::sync::Mutex<String>);

#[async_trait]
impl tachyon_serverless_provider_port::SecretProvider for RotatingSecrets {
    async fn resolve(
        &self,
        _ctx: &tachyon_serverless_provider_port::SecretDeliveryContext,
        _binding_ref: &str,
    ) -> Result<
        tachyon_serverless_provider_port::SecretValue,
        tachyon_serverless_provider_port::SecretError,
    > {
        Ok(tachyon_serverless_provider_port::SecretValue::new(
            self.0.lock().unwrap().clone(),
        ))
    }
}

struct H {
    app: Arc<Application>,
    fake: Arc<FakeExecutionProvider>,
    a: Principal,
    offset: Arc<AtomicI64>,
    _dir: tempfile::TempDir,
}

fn principal() -> Principal {
    Principal {
        subject: "a".into(),
        tenant_id: TenantId::parse(TENANT_A).unwrap(),
        roles: vec![Role::Deploy, Role::Invoke],
    }
}

fn warm_fake() -> Arc<FakeExecutionProvider> {
    let fake = Arc::new(FakeExecutionProvider::with_options(FakeProviderOptions {
        warm_capable: true,
        ..FakeProviderOptions::default()
    }));
    fake.set_default_script(Some(FakeGuestScript::SlowEchoForever));
    fake
}

fn config_toml(dir: &std::path::Path, extra: &str) -> String {
    format!(
        r#"
{base}

[[identity.tokens]]
token = "tok-a"
tenant_id = "{TENANT_A}"
subject = "a"
roles = ["deploy", "invoke"]

[[secrets.bindings]]
tenant_id = "{TENANT_A}"
binding_ref = "demo-secret"
value = "v1"

{extra}
"#,
        base = base_toml(dir, "process"),
    )
}

/// Everything but identities and secrets (a data plane takes those from its
/// control plane).
fn base_toml(dir: &std::path::Path, workdir: &str) -> String {
    format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/{workdir}"

[invoke]
cancel_grace_ms = 100
"#,
        data = dir.display()
    )
}

fn harness_with(
    extra: &str,
    secrets: Option<Arc<dyn tachyon_serverless_provider_port::SecretProvider>>,
) -> H {
    let dir = tempfile::tempdir().unwrap();
    let offset = Arc::new(AtomicI64::new(0));
    let fake = warm_fake();
    let app = Application::bootstrap_with(
        GatewayConfig::from_toml(&config_toml(dir.path(), extra)).unwrap(),
        fake.clone(),
        BootstrapOptions {
            clock: Arc::new(ShiftedClock(offset.clone())),
            secrets,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    H {
        app,
        fake,
        a: principal(),
        offset,
        _dir: dir,
    }
}

fn harness(extra: &str) -> H {
    harness_with(extra, None)
}

impl H {
    fn advance(&self, ms: i64) {
        self.offset.fetch_add(ms, Ordering::SeqCst);
    }

    async fn deploy(
        &self,
        name: &str,
        customize: impl FnOnce(&mut CreateRevisionRequest),
    ) -> (Function, FunctionRevision) {
        let function = self.app.functions.create(&self.a, name, "").unwrap();
        let rev = self.revision(&function, customize).await;
        (function, rev)
    }

    /// A new revision of `function`, published to `prod` (an alias switch
    /// when `prod` already exists).
    async fn revision(
        &self,
        function: &Function,
        customize: impl FnOnce(&mut CreateRevisionRequest),
    ) -> FunctionRevision {
        let artifact = self
            .app
            .artifact_service
            .upload(
                &self.a,
                format!("#!/bin/sh\necho {}\n", function.name).as_bytes(),
            )
            .await
            .unwrap();
        let mut req = CreateRevisionRequest {
            artifact: ArtifactRequest::Binary {
                digest: artifact.digest.to_string(),
            },
            architecture: "aarch64".into(),
            resources: ResourcesRequest::default(),
            execution: ExecutionRequest {
                timeout_seconds: 30,
                initialization_timeout_seconds: 10,
                max_concurrency: 4,
                ..ExecutionRequest::default()
            },
            egress: None,
            egress_allow: Vec::new(),
            env_vars: Vec::new(),
            secrets: Vec::new(),
            description: String::new(),
            publish_to_prod: true,
            required_region: None,
            restore: None,
        };
        customize(&mut req);
        let rev = self
            .app
            .revisions
            .create(&self.a, &function.id, &req)
            .await
            .unwrap();
        let rev = self
            .app
            .revisions
            .wait_terminal(&rev.id, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(rev.status, RevisionStatus::Ready, "{:?}", rev.status);
        rev
    }

    fn request(&self, f: &Function, payload: serde_json::Value) -> InvokeRequest {
        InvokeRequest {
            principal: self.a.clone(),
            function_id: f.id.clone(),
            alias: None,
            revision_id: None,
            event_kind: EventKind::Json,
            payload,
            idempotency_key: None,
            client_timeout_ms: None,
            trace_id: None,
        }
    }

    async fn invoke(&self, f: &Function, payload: serde_json::Value) -> InvokeOutcome {
        let out = self
            .app
            .invoke
            .invoke(self.request(f, payload))
            .await
            .unwrap();
        self.app.pool.settle().await;
        out
    }

    fn spawn_invoke(
        &self,
        req: InvokeRequest,
    ) -> tokio::task::JoinHandle<Result<InvokeOutcome, AppError>> {
        let app = self.app.clone();
        tokio::spawn(async move { app.invoke.invoke(req).await })
    }

    fn rev_info(&self, rev: &FunctionRevision) -> Option<RevisionCapacityInfo> {
        self.app
            .admission
            .snapshot(&self.a.tenant_id)
            .revisions
            .into_iter()
            .find(|r| r.revision_id == rev.id.to_string())
    }

    fn env_state(&self, id: &EnvironmentId) -> EnvironmentState {
        self.app.repos.environments.get(id).unwrap().unwrap().state
    }

    /// Wait (real time, bounded) until `cond` holds.
    async fn until(&self, what: &str, cond: impl Fn(&Self) -> bool) {
        for _ in 0..400 {
            if cond(self) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for: {what}");
    }
}

fn attempt(out: &InvokeOutcome) -> &tachyon_serverless_domain::InvocationAttempt {
    &out.detail.attempts.last().expect("an attempt").0
}

const POOL: &str = "[pool]\nenabled = true\nmax_idle_per_revision = 4\nidle_ttl_seconds = 60\nmax_total_idle = 8\n";

/// Acceptance 1: with `min_ready = 0` a revision's pooled environments are
/// all gone once idle past the TTL and the cooldown; the next invocation
/// boots cold and succeeds. The last scale event stays visible at zero.
#[tokio::test]
async fn min_ready_zero_scales_to_zero_after_idle_and_the_next_invoke_cold_starts() {
    let h = harness(POOL);
    let (f, rev) = h.deploy("zero", |_| {}).await;

    // 0 -> burst of 3 -> 3 environments, all pooled idle afterwards.
    let calls: Vec<_> = (0..3)
        .map(|_| h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 150}))))
        .collect();
    for c in calls {
        let out = c.await.unwrap().unwrap();
        assert!(out.succeeded(), "{:?}", out.invocation().status);
    }
    h.app.pool.settle().await;
    let created = h.fake.created().len();
    assert!((1..=3).contains(&created), "created {created}");
    assert_eq!(h.rev_info(&rev).unwrap().environments.idle, created as u64);

    // Inside the TTL: nothing goes.
    let report = h.app.reconcile_scaling().await;
    assert_eq!(report.sweep.unwrap().reaped, 0);
    assert!(h.fake.terminated().is_empty());

    // Idle past the TTL (60 s) and the cooldown (30 s): scale to zero.
    h.advance(120_000);
    let report = h.app.reconcile_scaling().await;
    assert_eq!(report.sweep.unwrap().reaped, created);
    assert!(
        h.fake.running().is_empty(),
        "no environment left on the host"
    );
    let info = h.rev_info(&rev).unwrap();
    assert_eq!(info.environments, Default::default());
    assert_eq!(info.last_scale_event.unwrap().kind, "scale_to_zero");
    for id in h.fake.created() {
        assert_eq!(h.env_state(&id), EnvironmentState::Stopped);
    }
    // Zero environments is not zero host cost, and the report says so.
    let snap = h.app.admission.snapshot(&h.a.tenant_id);
    assert!(snap.scaling.warm_pool);
    assert!(snap.scaling.at_zero.contains("not zero host cost"));

    // Re-access: a cold start that succeeds.
    let out = h.invoke(&f, serde_json::json!({"again": true})).await;
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert_eq!(attempt(&out).start_kind, StartKind::Cold);
    assert_eq!(h.fake.created().len(), created + 1);
    assert_eq!(
        h.rev_info(&rev).unwrap().last_scale_event.unwrap().kind,
        "activation"
    );
}

/// A burst arriving at zero is served by coalesced cold starts (never one
/// per request) and waits in the queue for the environments it started.
#[tokio::test]
async fn a_burst_at_zero_coalesces_cold_starts_and_waits_for_them() {
    let h = harness(POOL);
    let (f, _) = h.deploy("burst", |r| r.execution.max_concurrency = 2).await;
    let calls: Vec<_> = (0..8)
        .map(|i| h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 80, "i": i}))))
        .collect();
    let mut kinds = Vec::new();
    for c in calls {
        let out = c.await.unwrap().unwrap();
        assert!(out.succeeded(), "{:?}", out.invocation().status);
        kinds.push(attempt(&out).start_kind);
    }
    let created = h.fake.created().len();
    assert!(created <= 2, "8 requests booted {created} environments");
    assert_eq!(
        kinds.iter().filter(|k| **k == StartKind::Cold).count(),
        created
    );
    assert!(kinds.iter().filter(|k| **k == StartKind::Warm).count() >= 6);
}

/// Acceptance 2: a busy environment is never scaled down, whatever the idle
/// TTL says, and it goes back into the pool when its invocation finishes.
#[tokio::test]
async fn a_busy_environment_is_never_scaled_down() {
    let h = harness(
        "[pool]\nenabled = true\nmax_idle_per_revision = 2\nidle_ttl_seconds = 1\nmax_total_idle = 8\n\
         [scaling]\nscale_down_cooldown_seconds = 0\n",
    );
    let (f, rev) = h.deploy("busy", |r| r.execution.max_concurrency = 1).await;
    let first = h.invoke(&f, serde_json::json!({})).await;
    let env = attempt(&first).environment_id.clone();
    let long = h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 600})));
    h.until("the environment is busy", |h| {
        h.rev_info(&rev).is_some_and(|r| r.environments.busy == 1)
    })
    .await;
    // Far past the TTL while it runs.
    h.advance(3_600_000);
    for _ in 0..3 {
        let report = h.app.reconcile_scaling().await;
        assert_eq!(report.sweep.unwrap().reaped, 0);
    }
    assert!(h.fake.terminated().is_empty());
    assert_eq!(h.env_state(&env), EnvironmentState::Busy);
    let out = long.await.unwrap().unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert_eq!(attempt(&out).environment_id, env);
    h.app.pool.settle().await;
    assert_eq!(h.env_state(&env), EnvironmentState::Idle);
}

/// Acceptance 2 / 4: the sweeper and arriving invocations race for the same
/// expired idle environment round after round. Whoever loses behaves
/// correctly: no invocation fails, nothing in use is terminated, and the
/// ledger and admission agree at the end.
#[tokio::test]
async fn sweeps_racing_invocations_never_terminate_an_environment_in_use() {
    let h = harness(
        "[pool]\nenabled = true\nmax_idle_per_revision = 2\nidle_ttl_seconds = 1\nmax_total_idle = 8\n\
         [scaling]\nscale_down_cooldown_seconds = 0\n",
    );
    let (f, rev) = h.deploy("race", |_| {}).await;
    h.invoke(&f, serde_json::json!({})).await;
    let mut raced = 0;
    let mut reaped = 0;
    for round in 0..25 {
        h.advance(5_000);
        let (out, report) = tokio::join!(
            h.app
                .invoke
                .invoke(h.request(&f, serde_json::json!({"round": round}))),
            h.app.reconcile_scaling()
        );
        let out = out.unwrap();
        assert!(
            out.succeeded(),
            "round {round}: {:?}",
            out.invocation().status
        );
        let used = attempt(&out).environment_id.clone();
        assert!(
            !h.fake.terminated().iter().any(|(id, r)| *id == used
                && *r != TerminateReason::Quiesced
                && h.env_state(id) == EnvironmentState::Busy),
            "round {round}: an environment in use was terminated"
        );
        let sweep = report.sweep.unwrap();
        raced += sweep.raced + sweep.kept;
        reaped += sweep.reaped;
        h.app.pool.settle().await;
    }
    // Both sides won some rounds (not asserted: timing), and nothing leaked.
    let _ = (raced, reaped);
    let info = h.rev_info(&rev).unwrap();
    assert_eq!(info.environments.busy + info.environments.starting, 0);
    let idle_rows = h
        .fake
        .created()
        .iter()
        .filter(|id| h.env_state(id) == EnvironmentState::Idle)
        .count() as u64;
    assert_eq!(info.environments.idle, idle_rows);
    assert_eq!(h.fake.running().len() as u64, idle_rows);
}

/// Acceptance 3: after an alias switch only new admissions go to the new
/// revision; the invocation that was running keeps its revision and route
/// generation and completes; the old revision's idle environments are
/// drained at once and its busy one is not pooled again.
#[tokio::test]
async fn an_alias_switch_routes_only_new_invocations_and_drains_the_old_revision() {
    let h = harness(POOL);
    let (f, rev1) = h.deploy("switch", |_| {}).await;
    let gen1 = h
        .app
        .aliases
        .get(&h.a, &f.id, &AliasName::default_alias())
        .unwrap()
        .generation;
    // Two pooled environments of rev1.
    let warmup: Vec<_> = (0..2)
        .map(|_| h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 100}))))
        .collect();
    for w in warmup {
        assert!(w.await.unwrap().unwrap().succeeded());
    }
    h.app.pool.settle().await;
    let before = h.fake.created();

    // A long invocation takes one of them.
    let long = h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 700})));
    h.until("the long invocation runs", |h| {
        h.rev_info(&rev1).is_some_and(|r| r.environments.busy == 1)
    })
    .await;

    // The reconciler has seen rev1 routed (it runs every second in a gateway).
    h.app.reconcile_scaling().await;
    // Switch prod to rev2 while it runs.
    let rev2 = h.revision(&f, |_| {}).await;
    let gen2 = h
        .app
        .aliases
        .get(&h.a, &f.id, &AliasName::default_alias())
        .unwrap()
        .generation;
    assert!(gen2 > gen1);
    let report = h.app.reconcile_scaling().await;
    assert_eq!(
        report.drains_started,
        vec![(rev1.id.clone(), "alias_switch")]
    );
    let sweep = report.sweep.unwrap();
    assert_eq!(
        (sweep.reaped, sweep.drained),
        (before.len() - 1, before.len() - 1),
        "the idle rev1 environment is drained without waiting for its TTL"
    );
    assert_eq!(h.rev_info(&rev1).unwrap().route_state, "superseded");

    // New admissions: rev2, the new route generation.
    let new = h.invoke(&f, serde_json::json!({"new": true})).await;
    assert!(new.succeeded());
    assert_eq!(new.invocation().revision_id, rev2.id);
    assert_eq!(new.invocation().alias_generation, Some(gen2));
    assert!(!before.contains(&attempt(&new).environment_id));

    // The running one was not re-pointed.
    let old = long.await.unwrap().unwrap();
    assert!(old.succeeded(), "{:?}", old.invocation().status);
    assert_eq!(old.invocation().revision_id, rev1.id);
    assert_eq!(old.invocation().alias_generation, Some(gen1));
    assert!(old.invocation().accepted_at < new.invocation().accepted_at);
    h.app.pool.settle().await;
    let old_env = attempt(&old).environment_id.clone();
    assert_eq!(
        h.env_state(&old_env),
        EnvironmentState::Stopped,
        "a superseded revision's environment is not pooled again"
    );
    for id in &before {
        assert!(!h.fake.running().contains(id));
    }
    // Nothing of rev1 is left: its drain is finished and forgotten.
    h.app.reconcile_scaling().await;
    let info = h.rev_info(&rev1).unwrap();
    assert_eq!(info.environments, Default::default());
    assert_eq!(info.route_state, "unrouted");
    assert_eq!(h.rev_info(&rev2).unwrap().route_state, "routed");
}

/// An invocation still running on a drained revision when the drain timeout
/// passes is stopped like an execution timeout (`Host.DrainTimeout`), and
/// its environment is terminated.
/// Acceptance 3 under the defaults: the drain timeout is derived from the
/// longest revision timeout (900 s) + cancel grace + 60 s, so an invocation
/// that is still inside its own timeout is never cut short by an alias
/// switch. The clock moves 400 s and then 900 s past the switch (far beyond
/// the old 300 s default) while the handler runs: nothing is stopped and the
/// invocation completes on its revision.
#[tokio::test]
async fn under_the_default_drain_timeout_an_alias_switch_never_stops_a_long_handler() {
    let h = harness(POOL);
    let snap = h.app.admission.snapshot(&h.a.tenant_id);
    // 900 s max execution timeout + 1 s cancel grace (100 ms, rounded up) + 60 s.
    assert_eq!(snap.scaling.drain_timeout_seconds, 961);
    let (f, rev1) = h
        .deploy("long-drain", |r| r.execution.timeout_seconds = 900)
        .await;
    let long = h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 1_500})));
    h.until("the long invocation runs", |h| {
        h.rev_info(&rev1).is_some_and(|r| r.environments.busy == 1)
    })
    .await;
    h.app.reconcile_scaling().await;
    h.revision(&f, |_| {}).await;
    let report = h.app.reconcile_scaling().await;
    assert_eq!(
        report.drains_started,
        vec![(rev1.id.clone(), "alias_switch")]
    );
    for advance_s in [400, 500] {
        h.advance(advance_s * 1_000);
        let report = h.app.reconcile_scaling().await;
        assert_eq!(
            report.drain_timeouts,
            0,
            "stopped {} s after the switch under the default drain timeout",
            if advance_s == 400 { 400 } else { 900 }
        );
    }
    let out = long.await.unwrap().unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert_eq!(out.invocation().revision_id, rev1.id);
}

#[tokio::test]
async fn a_drain_timeout_stops_what_still_runs_on_a_drained_revision() {
    let h = harness(&format!(
        "{POOL}[scaling]\ndrain_timeout_seconds = 1\nallow_short_drain = true\n"
    ));
    let (f, rev1) = h.deploy("drain-timeout", |_| {}).await;
    let long = h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 20_000})));
    h.until("the long invocation runs", |h| {
        h.rev_info(&rev1).is_some_and(|r| r.environments.busy == 1)
    })
    .await;
    h.app.reconcile_scaling().await;
    h.revision(&f, |_| {}).await;
    let report = h.app.reconcile_scaling().await;
    assert_eq!(report.drains_started.len(), 1);
    assert_eq!(report.drain_timeouts, 0, "not before the drain timeout");
    h.advance(2_000);
    let report = h.app.reconcile_scaling().await;
    assert_eq!(report.drain_timeouts, 1);
    let out = tokio::time::timeout(Duration::from_secs(5), long)
        .await
        .expect("stopped well before its own 20 s")
        .unwrap()
        .unwrap();
    let InvocationStatus::Failed { error } = &out.invocation().status else {
        panic!("{:?}", out.invocation().status);
    };
    assert_eq!(error.class, ErrorClass::Timeout);
    assert_eq!(error.error_type, "Host.DrainTimeout");
    assert_eq!(out.error().unwrap().http_status(), 504);
    let env = attempt(&out).environment_id.clone();
    assert!(
        h.fake
            .terminated()
            .contains(&(env.clone(), TerminateReason::Timeout))
    );
    assert!(matches!(h.env_state(&env), EnvironmentState::Failed { .. }));
    // A second round does not stop anything again.
    h.advance(2_000);
    assert_eq!(h.app.reconcile_scaling().await.drain_timeouts, 0);
}

/// Acceptance 4 (deletion): new invocations are refused, an invocation that
/// was queued before the deletion is refused instead of started, a retry
/// with the same idempotency key is refused, the running invocation
/// completes, its environment is terminated rather than pooled, and the
/// deletion is finalized once nothing is left.
#[tokio::test]
async fn deleting_a_function_refuses_new_queued_and_retried_work_and_lets_running_work_finish() {
    let h = harness(POOL);
    let (f, rev) = h
        .deploy("doomed", |r| r.execution.max_concurrency = 1)
        .await;
    let idle = h.invoke(&f, serde_json::json!({})).await;
    let idle_env = attempt(&idle).environment_id.clone();

    let mut running = h.request(&f, serde_json::json!({"sleep_ms": 600}));
    running.idempotency_key = Some("key-running".into());
    let running = h.spawn_invoke(running);
    h.until("running", |h| {
        h.rev_info(&rev).is_some_and(|r| r.environments.busy == 1)
    })
    .await;
    // Another one queues behind it (revision max_concurrency = 1)...
    let mut queued = h.request(&f, serde_json::json!({"queued": true}));
    queued.idempotency_key = Some("key-queued".into());
    let queued = h.spawn_invoke(queued);
    h.until("queued", |h| {
        h.rev_info(&rev).is_some_and(|r| r.queued == 1)
    })
    .await;
    // ...and an idle environment of a second invocation's past is pooled.
    let spare = h
        .fake
        .created()
        .into_iter()
        .find(|id| h.env_state(id) == EnvironmentState::Idle);

    let deleted = h.app.functions.delete(&h.a, &f.id).unwrap();
    assert!(deleted.is_deleted());
    assert_eq!(deleted.deletion_state(), "deleting");
    let report = h.app.reconcile_scaling().await;
    assert_eq!(
        report.drains_started,
        vec![(rev.id.clone(), "function_deleted")]
    );

    // The queued one never started.
    let q = queued.await.unwrap().unwrap();
    let InvocationStatus::Failed { error } = &q.invocation().status else {
        panic!("{:?}", q.invocation().status);
    };
    assert_eq!(error.error_type, "Host.FunctionDeleted");
    assert!(q.detail.attempts.is_empty(), "no attempt was dispatched");
    assert_eq!(q.error().unwrap().code(), ErrorCode::FunctionDeleted);
    assert_eq!(q.error().unwrap().http_status(), 409);

    // New work and retries are refused.
    let new = h
        .app
        .invoke
        .invoke(h.request(&f, serde_json::json!({})))
        .await;
    assert!(matches!(new, Err(AppError::FunctionDeleted(_))), "{new:?}");
    let mut retry = h.request(&f, serde_json::json!({"queued": true}));
    retry.idempotency_key = Some("key-queued".into());
    let retry = h.app.invoke.invoke(retry).await;
    assert!(
        matches!(retry, Err(AppError::FunctionDeleted(_))),
        "{retry:?}"
    );
    let body = retry.unwrap_err().to_api_body(None);
    assert_eq!(
        body.error.error_type.as_deref(),
        Some("Host.FunctionDeleted")
    );

    // The running one completes on its environment, which is then ended.
    let r = running.await.unwrap().unwrap();
    assert!(r.succeeded(), "{:?}", r.invocation().status);
    h.app.pool.settle().await;
    assert_eq!(
        h.env_state(&attempt(&r).environment_id),
        EnvironmentState::Stopped
    );
    let mut retry_running = h.request(&f, serde_json::json!({"sleep_ms": 600}));
    retry_running.idempotency_key = Some("key-running".into());
    assert!(matches!(
        h.app.invoke.invoke(retry_running).await,
        Err(AppError::FunctionDeleted(_))
    ));
    let report = h.app.reconcile_scaling().await;
    if let Some(spare) = spare {
        assert_ne!(h.env_state(&spare), EnvironmentState::Idle);
    }
    assert!(h.fake.running().is_empty(), "{:?}", h.fake.running());
    let _ = idle_env;
    // Finalized in this round or the next (the drain lands first).
    let finalized = if report.finalized.contains(&f.id) {
        true
    } else {
        h.app.reconcile_scaling().await.finalized.contains(&f.id)
    };
    assert!(finalized);
    let stored = h.app.functions.get(&h.a, &f.id).unwrap();
    assert_eq!(stored.deletion_state(), "deleted");
    assert!(stored.drained_at.is_some());
    // Idempotent afterwards.
    assert!(h.app.reconcile_scaling().await.finalized.is_empty());
}

/// `min_ready`: the reconciler pre-starts environments into the pool, keeps
/// them through idle TTLs without re-creating anything (no flapping), serves
/// invocations warm from them, and drains them when the revision stops being
/// routed.
#[tokio::test]
async fn min_ready_pre_starts_into_the_pool_and_keeps_them_without_flapping() {
    let h = harness(
        "[pool]\nenabled = true\nmax_idle_per_revision = 1\nidle_ttl_seconds = 1\nmax_total_idle = 8\n\
         [scaling]\nscale_down_cooldown_seconds = 0\n",
    );
    let (f, rev) = h.deploy("warm", |r| r.execution.min_ready = 2).await;
    let report = h.app.reconcile_scaling().await;
    assert_eq!(report.prestarts, 2);
    h.app.scaling.settle().await;
    assert_eq!(h.fake.created().len(), 2);
    let info = h.rev_info(&rev).unwrap();
    assert_eq!((info.min_ready, info.environments.idle), (2, 2));
    assert_eq!(info.route_state, "routed");
    // Pre-started environments are metered like any other.
    let started = h
        .app
        .usage
        .events()
        .iter()
        .filter(|e| {
            matches!(
                e.event_type,
                tachyon_serverless_domain::UsageEventType::EnvironmentStarted
            ) && e.invocation_id.is_none()
        })
        .count();
    assert_eq!(started, 2);

    for _ in 0..10 {
        h.advance(30_000);
        let report = h.app.reconcile_scaling().await;
        assert_eq!(report.prestarts, 0);
        assert_eq!(report.sweep.unwrap().reaped, 0);
        h.app.scaling.settle().await;
    }
    assert_eq!(h.fake.created().len(), 2, "nothing was re-created");
    assert!(h.fake.terminated().is_empty(), "nothing was scaled down");

    let out = h.invoke(&f, serde_json::json!({})).await;
    assert!(out.succeeded());
    assert_eq!(attempt(&out).start_kind, StartKind::Warm);
    h.app.reconcile_scaling().await;
    h.app.scaling.settle().await;
    assert_eq!(h.fake.created().len(), 2);

    // prod moves to a revision without min_ready: both are drained.
    let rev2 = h.revision(&f, |_| {}).await;
    let report = h.app.reconcile_scaling().await;
    assert_eq!(report.sweep.unwrap().drained, 2);
    assert_eq!(report.prestarts, 0);
    assert!(h.fake.running().is_empty());
    assert_eq!(h.rev_info(&rev2).map_or(0, |r| r.environments.idle), 0);
}

/// A rotated secret supersedes the reuse key: the pooled environments of the
/// old generation are drained on the next round instead of waiting for their
/// TTL.
#[tokio::test]
async fn a_rotated_secret_drains_the_old_generations_pooled_environments() {
    let secrets = Arc::new(RotatingSecrets(std::sync::Mutex::new("v1".into())));
    let h = harness_with(POOL, Some(secrets.clone()));
    let (f, _) = h
        .deploy("rotate", |r| {
            r.secrets = vec![SecretBindingRequest {
                env_name: "DEMO_SECRET".into(),
                binding_ref: "demo-secret".into(),
            }]
        })
        .await;
    let calls: Vec<_> = (0..2)
        .map(|_| h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 100}))))
        .collect();
    for c in calls {
        assert!(c.await.unwrap().unwrap().succeeded());
    }
    h.app.pool.settle().await;
    let old: Vec<EnvironmentId> = h.fake.created();
    *secrets.0.lock().unwrap() = "v2".into();
    let fresh = h.invoke(&f, serde_json::json!({})).await;
    assert_eq!(attempt(&fresh).start_kind, StartKind::Cold);
    let report = h.app.reconcile_scaling().await;
    assert_eq!(report.sweep.unwrap().drained, old.len());
    for id in &old {
        assert_eq!(h.env_state(id), EnvironmentState::Stopped);
    }
    assert_eq!(
        h.env_state(&attempt(&fresh).environment_id),
        EnvironmentState::Idle
    );
}

// ---------------------------------------------------------------------------
// outage and reconnect storm (a data plane)
// ---------------------------------------------------------------------------

const INTERNAL: &str = "internal-credential-0123456789";

struct FlakySource {
    inner: Arc<dyn ConfigSource>,
    down: AtomicBool,
    fetches: AtomicUsize,
}

#[async_trait]
impl ConfigSource for FlakySource {
    fn describe(&self) -> String {
        "flaky".into()
    }
    async fn fetch(&self, since: u64) -> Result<ConfigDelivery, SourceError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        if self.down.load(Ordering::SeqCst) {
            return Err(SourceError::Unavailable("connection refused".into()));
        }
        self.inner.fetch(since).await
    }
}

/// Acceptance 4 (reconnect): while the control plane is unreachable and the
/// configuration expires, routes are *held* — nothing is drained, the
/// `min_ready` environment is kept — and a storm of disconnects and
/// reconnects neither drains nor re-creates anything.
#[tokio::test]
async fn an_outage_holds_routes_and_a_reconnect_storm_does_not_flap() {
    use chrono::TimeZone;
    let dir = tempfile::tempdir().unwrap();
    let t0 = chrono::Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap();
    let mgmt_clock = Arc::new(FixedClock::new(t0));
    let mgmt = Application::bootstrap_with(
        GatewayConfig::from_toml(&format!(
            "{}\n[dispatcher]\ninstance = \"management\"\n[control_plane]\ninternal_token = \"{INTERNAL}\"\n",
            config_toml(dir.path(), "")
        ))
        .unwrap(),
        warm_fake(),
        BootstrapOptions {
            clock: mgmt_clock.clone(),
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    let source = Arc::new(FlakySource {
        inner: mgmt.config_publisher.clone().unwrap(),
        down: AtomicBool::new(false),
        fetches: AtomicUsize::new(0),
    });
    let dp_clock = Arc::new(FixedClock::new(t0));
    let dp_fake = warm_fake();
    let dp = Application::bootstrap_with(
        GatewayConfig::from_toml(&format!(
            "{}\n{}\n[scaling]\nscale_down_cooldown_seconds = 0\n[dispatcher]\ninstance = \"data-plane\"\n\
             [control_plane]\nrole = \"data_plane\"\nurl = \"http://127.0.0.1:1\"\ninternal_token = \"{INTERNAL}\"\n\
             refresh_interval_ms = 1000\nconfig_ttl_seconds = 20\nauth_lease_seconds = 30\n",
            base_toml(dir.path(), "process-dp"),
            "[pool]\nenabled = true\nmax_idle_per_revision = 1\nidle_ttl_seconds = 1\nmax_total_idle = 8\n"
        ))
        .unwrap(),
        dp_fake.clone(),
        BootstrapOptions {
            clock: dp_clock.clone(),
            config_source: Some(source.clone()),
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    let a = principal();
    let function = mgmt.functions.create(&a, "held", "").unwrap();
    let artifact = mgmt
        .artifact_service
        .upload(&a, b"#!/bin/sh\necho held\n")
        .await
        .unwrap();
    let req = CreateRevisionRequest {
        artifact: ArtifactRequest::Binary {
            digest: artifact.digest.to_string(),
        },
        architecture: "aarch64".into(),
        resources: ResourcesRequest::default(),
        execution: ExecutionRequest {
            min_ready: 1,
            ..ExecutionRequest::default()
        },
        egress: None,
        egress_allow: Vec::new(),
        env_vars: Vec::new(),
        secrets: Vec::new(),
        description: String::new(),
        publish_to_prod: true,
        required_region: None,
        restore: None,
    };
    let rev = mgmt.revisions.create(&a, &function.id, &req).await.unwrap();
    mgmt.revisions
        .wait_terminal(&rev.id, Duration::from_secs(5))
        .await
        .unwrap();

    dp.refresh_config().await.unwrap();
    let report = dp.reconcile_scaling().await;
    assert_eq!((report.view, report.prestarts), ("valid", 1));
    dp.scaling.settle().await;
    assert_eq!(dp_fake.created().len(), 1);

    // Outage long enough for the configuration to expire.
    source.down.store(true, Ordering::SeqCst);
    dp_clock.advance(chrono::Duration::seconds(25));
    assert!(dp.refresh_config().await.is_err());
    let mut reports = Vec::new();
    for _ in 0..5 {
        dp_clock.advance(chrono::Duration::seconds(5));
        reports.push(dp.reconcile_scaling().await);
    }
    for r in reports.iter() {
        assert_eq!(r.view, "held");
        assert!(r.drains_started.is_empty());
        assert_eq!(
            r.sweep.as_ref().unwrap().reaped,
            0,
            "min_ready kept while held"
        );
    }

    // Reconnect storm: down, up, down, up...
    for i in 0..12 {
        source.down.store(i % 2 == 0, Ordering::SeqCst);
        dp_clock.advance(chrono::Duration::seconds(3));
        let _ = dp.refresh_config().await;
        let r = dp.reconcile_scaling().await;
        assert!(
            r.drains_started.is_empty(),
            "round {i}: {:?}",
            r.drains_started
        );
        assert!(r.drains_ended.is_empty());
        assert_eq!(r.sweep.as_ref().unwrap().reaped, 0);
        dp.scaling.settle().await;
    }
    source.down.store(false, Ordering::SeqCst);
    dp_clock.advance(chrono::Duration::seconds(3));
    dp.refresh_config().await.unwrap();
    let r = dp.reconcile_scaling().await;
    assert_eq!((r.view, r.prestarts), ("valid", 0));
    dp.scaling.settle().await;
    assert_eq!(dp_fake.created().len(), 1, "nothing was re-created");
    assert!(dp_fake.terminated().is_empty(), "nothing was drained");
    assert!(source.fetches.load(Ordering::SeqCst) > 12);
}

// ---------------------------------------------------------------------------
// metrics (PLT-4637)
// ---------------------------------------------------------------------------

/// The value of the sample of `family` whose labels contain every `label`
/// (`key="value"` fragments). `None` when no line matches.
fn metric(text: &str, family: &str, labels: &[&str]) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| {
            l.strip_prefix(family)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .find(|l| labels.iter().all(|want| l.contains(want)))
        .and_then(|l| l.rsplit_once(' '))
        .and_then(|(_, v)| v.parse().ok())
}

fn fake_stats(cpu: f64) -> tachyon_serverless_provider_port::EnvironmentStats {
    tachyon_serverless_provider_port::EnvironmentStats {
        cpu_seconds: Some(cpu),
        memory_current_bytes: Some(64 << 20),
        memory_peak_bytes: Some(128 << 20),
        scope: "fake".into(),
    }
}

/// Acceptance 1 and 2 through the pipeline: a revision goes 0 -> burst ->
/// its cap -> idle -> 0 and `GET /metrics` shows each step; the warm pool's
/// reuse is proven by boot identity (every warm attempt reports the boot id
/// its environment booted with, none changes); an idle environment that burns
/// CPU is visible to the idle CPU detector.
#[tokio::test]
async fn metrics_show_zero_to_cap_to_zero_and_boot_identity_proves_warm_reuse() {
    let h = harness(POOL);
    let (f, rev) = h
        .deploy("metrics", |r| r.execution.max_concurrency = 2)
        .await;
    let rev_label = format!("revision=\"{}\"", rev.id);

    let text = h.app.render_metrics().await;
    assert_eq!(metric(&text, "tsls_node_in_flight", &[]), Some(0.0));
    assert_eq!(
        metric(
            &text,
            "tsls_environment_reuse_mode",
            &["mode=\"warm_reuse\""]
        ),
        Some(1.0)
    );

    // Burst of 6 on a cap of 2, sampled while it runs.
    let calls: Vec<_> = (0..6)
        .map(|i| h.spawn_invoke(h.request(&f, serde_json::json!({"sleep_ms": 120, "i": i}))))
        .collect();
    let mut max_provisioned = 0.0f64;
    let mut max_queue = 0.0f64;
    for _ in 0..40 {
        let text = h.app.render_metrics().await;
        let provisioned: f64 = ["starting", "busy", "promised"]
            .iter()
            .filter_map(|s| {
                metric(
                    &text,
                    "tsls_revision_environments",
                    &[&rev_label, &format!("state=\"{s}\"")],
                )
            })
            .sum();
        max_provisioned = max_provisioned.max(provisioned);
        max_queue = max_queue.max(metric(&text, "tsls_queue_length", &[]).unwrap_or(0.0));
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let mut kinds = Vec::new();
    for c in calls {
        let out = c.await.unwrap().unwrap();
        assert!(out.succeeded(), "{:?}", out.invocation().status);
        kinds.push(attempt(&out).start_kind);
    }
    h.app.pool.settle().await;
    assert!(
        max_provisioned <= 2.0,
        "never above the cap: {max_provisioned}"
    );
    assert!(
        max_provisioned >= 1.0 && max_queue >= 1.0,
        "the burst was seen"
    );
    let created = h.fake.created().len();
    assert!(created <= 2, "created {created}");

    // Boot identity: first boots = environments, the rest reused the same guest.
    let text = h.app.render_metrics().await;
    let warm = kinds.iter().filter(|k| **k == StartKind::Warm).count();
    assert_eq!(warm, 6 - created);
    let check = |r: &str| {
        metric(
            &text,
            "tsls_boot_identity_checks_total",
            &[&format!("result=\"{r}\"")],
        )
    };
    assert_eq!(check("first_boot"), Some(created as f64));
    assert_eq!(check("same_boot"), Some(warm as f64));
    assert_eq!(check("boot_changed"), Some(0.0));
    let report = h.app.reuse_report();
    assert_eq!(report.mode, "warm_reuse");
    assert_eq!(report.same_boot_reuses, warm as u64);
    assert_eq!(report.boot_id_changed, 0);
    assert_eq!(
        metric(
            &text,
            "tsls_attempts_total",
            &["start_kind=\"warm\"", "status=\"succeeded\""]
        ),
        Some(warm as f64)
    );
    assert_eq!(
        metric(&text, "tsls_admission_grants_total", &["kind=\"warm\""]),
        Some(warm as f64)
    );
    assert_eq!(
        metric(
            &text,
            "tsls_revision_environments",
            &[&rev_label, "state=\"idle\""]
        ),
        Some(created as f64)
    );

    // Idle CPU: a quiesced environment that keeps burning CPU is measured.
    let idle_envs = h.fake.running();
    for id in &idle_envs {
        h.fake.set_environment_stats(id, fake_stats(1.0));
    }
    let _ = h.app.render_metrics().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for id in &idle_envs {
        h.fake.set_environment_stats(id, fake_stats(1.05));
    }
    let text = h.app.render_metrics().await;
    let ratio = metric(&text, "tsls_idle_environment_cpu_ratio_max", &[]).unwrap();
    assert!(ratio > 0.1, "50 ms of CPU in ~100 ms idle: {ratio}");
    assert_eq!(
        metric(&text, "tsls_idle_environments_sampled", &[]),
        Some(idle_envs.len() as f64)
    );
    assert!(
        metric(
            &text,
            "tsls_environment_memory_peak_bytes",
            &["state=\"idle\""]
        )
        .is_some()
    );

    // Idle past the TTL and cooldown: back to 0.
    h.advance(120_000);
    h.app.reconcile_scaling().await;
    let text = h.app.render_metrics().await;
    assert_eq!(metric(&text, "tsls_node_in_flight", &[]), Some(0.0));
    assert_eq!(
        metric(&text, "tsls_environments", &["state=\"idle\""]),
        Some(0.0)
    );
    assert_eq!(
        metric(&text, "tsls_node_reserved_memory_bytes", &[]),
        Some(0.0)
    );
    assert_eq!(
        metric(
            &text,
            "tsls_scale_events_total",
            &["kind=\"scale_to_zero\"", "reason=\"idle_ttl\""]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric(&text, "tsls_scale_events_total", &["kind=\"activation\""]),
        Some(1.0)
    );
}
