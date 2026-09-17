//! Admission, quotas and autoscaling through the invoke pipeline with the fake
//! provider (PLT-4634). Bounded and local: real time, at most a few seconds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeRequest, services::RejectReason,
};
use tachyon_serverless_domain::{
    AliasName, ErrorClass, EventKind, Function, FunctionRevision, InvocationStatus, RevisionStatus,
    TenantId,
};
use tachyon_serverless_provider_fake::{
    FakeExecutionProvider, FakeGuestScript, FakeProviderOptions,
};
use tachyon_serverless_provider_port::{Principal, Role};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

struct Harness {
    app: Arc<Application>,
    fake: Arc<FakeExecutionProvider>,
    _dir: tempfile::TempDir,
}

fn principal(tenant: &str) -> Principal {
    Principal {
        subject: "t".into(),
        tenant_id: TenantId::parse(tenant).unwrap(),
        roles: vec![Role::Deploy, Role::Invoke],
    }
}

fn harness(fake: FakeExecutionProvider, capacity: &str) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let text = format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process"

[store]
backend = "memory"

[[identity.tokens]]
token = "tok-a"
tenant_id = "{TENANT_A}"
subject = "a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "tok-b"
tenant_id = "{TENANT_B}"
subject = "b"
roles = ["deploy", "invoke"]

{capacity}
"#,
        data = dir.path().display()
    );
    let config = GatewayConfig::from_toml(&text).unwrap();
    let fake = Arc::new(fake);
    let app = Application::bootstrap_with(
        config,
        fake.clone(),
        BootstrapOptions {
            persist_state: false,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    Harness {
        app,
        fake,
        _dir: dir,
    }
}

async fn deploy(
    h: &Harness,
    p: &Principal,
    name: &str,
    max_concurrency: u32,
    required_region: Option<&str>,
) -> (Function, FunctionRevision) {
    let artifact = h
        .app
        .artifact_service
        .upload(p, format!("#!/bin/sh\necho {name}\n").as_bytes())
        .await
        .unwrap();
    let function = h.app.functions.create(p, name, "").unwrap();
    let req = CreateRevisionRequest {
        artifact: ArtifactRequest::Binary {
            digest: artifact.digest.to_string(),
        },
        architecture: "aarch64".into(),
        resources: ResourcesRequest::default(),
        execution: ExecutionRequest {
            timeout_seconds: 10,
            initialization_timeout_seconds: 5,
            max_concurrency,
            ..ExecutionRequest::default()
        },
        egress: None,
        egress_allow: Vec::new(),
        env_vars: Vec::new(),
        secrets: Vec::new(),
        description: String::new(),
        publish_to_prod: true,
        required_region: required_region.map(str::to_string),
    };
    let rev = h.app.revisions.create(p, &function.id, &req).await.unwrap();
    let rev = h
        .app
        .revisions
        .wait_terminal(&rev.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(rev.status, RevisionStatus::Ready, "{:?}", rev.status);
    h.app
        .aliases
        .get(p, &function.id, &AliasName::default_alias())
        .unwrap();
    (function, rev)
}

fn request(p: &Principal, f: &Function) -> InvokeRequest {
    InvokeRequest {
        principal: p.clone(),
        function_id: f.id.clone(),
        alias: None,
        revision_id: None,
        event_kind: EventKind::Json,
        payload: serde_json::json!({"n": 1}),
        idempotency_key: None,
        client_timeout_ms: None,
        trace_id: None,
    }
}

/// Acceptance 1 and 4 through the pipeline: a burst of 12 on a node whose
/// memory fits three environments *including* the VMM and bridge overhead
/// (3 × (256 + 24) = 840 ≤ 900 < 1120) never runs a fourth, every invocation
/// still succeeds by waiting, and everything is released afterwards.
#[tokio::test]
async fn a_burst_waits_for_node_memory_and_never_overshoots() {
    let h = harness(
        FakeExecutionProvider::with_options(FakeProviderOptions {
            boot_delay: Duration::from_millis(120),
            ..FakeProviderOptions::default()
        }),
        "[capacity]\nmax_concurrency = 8\nmax_queue = 32\nqueue_timeout_seconds = 10\n\
         [capacity.node]\nmemory_mib = 900\n",
    );
    let a = principal(TENANT_A);
    let (function, _) = deploy(&h, &a, "burst", 12, None).await;

    let stop = Arc::new(AtomicBool::new(false));
    let max_running = Arc::new(AtomicU64::new(0));
    let max_reserved = Arc::new(AtomicU64::new(0));
    let monitor = tokio::spawn({
        let (app, fake, stop) = (h.app.clone(), h.fake.clone(), stop.clone());
        let (max_running, max_reserved) = (max_running.clone(), max_reserved.clone());
        let tenant = a.tenant_id.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                max_running.fetch_max(fake.running().len() as u64, Ordering::SeqCst);
                let info = app.admission.snapshot(&tenant);
                max_reserved.fetch_max(info.reserved.memory_mib.unwrap_or(0), Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        }
    });
    let calls: Vec<_> = (0..12)
        .map(|_| {
            let app = h.app.clone();
            let req = request(&a, &function);
            tokio::spawn(async move { app.invoke.invoke(req).await })
        })
        .collect();
    for call in calls {
        let out = call.await.unwrap().unwrap();
        assert!(out.succeeded(), "{:?}", out.invocation().status);
    }
    stop.store(true, Ordering::SeqCst);
    monitor.await.unwrap();

    assert_eq!(h.fake.created().len(), 12, "destroy-after-invoke: one each");
    assert!(
        max_running.load(Ordering::SeqCst) <= 3,
        "running environments: {}",
        max_running.load(Ordering::SeqCst)
    );
    assert!(max_reserved.load(Ordering::SeqCst) <= 900);
    assert!(
        max_reserved.load(Ordering::SeqCst) >= 560,
        "it did scale out"
    );
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert_eq!(info.reserved.memory_mib, Some(0));
    assert_eq!(info.in_flight, 0);
    assert_eq!(info.queue.length, 0);
    assert!(info.rejections.is_empty(), "{:?}", info.rejections);
}

/// Acceptance 3: a node that stays full ends the wait with an explicit
/// `capacity` reason, recorded on the invocation and in the error body.
#[tokio::test]
async fn a_full_node_ends_the_wait_with_the_capacity_reason() {
    let h = harness(
        FakeExecutionProvider::with_scripts([FakeGuestScript::HangForever]),
        "[capacity]\nmax_concurrency = 8\nmax_queue = 4\nqueue_timeout_seconds = 1\n\
         [capacity.node]\nmemory_mib = 300\n",
    );
    let a = principal(TENANT_A);
    let (function, _) = deploy(&h, &a, "full", 4, None).await;
    let hanging = tokio::spawn({
        let app = h.app.clone();
        let req = request(&a, &function);
        async move { app.invoke.invoke(req).await }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while h.app.admission.snapshot(&a.tenant_id).in_flight == 0 {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let out = h.app.invoke.invoke(request(&a, &function)).await.unwrap();
    let InvocationStatus::Failed { error } = &out.invocation().status else {
        panic!("{:?}", out.invocation().status);
    };
    assert_eq!(error.class, ErrorClass::QueueTimeout);
    assert_eq!(error.error_type, "Host.CapacityWaitTimeout");
    let err = out.error().unwrap();
    assert_eq!(err.http_status(), 504);
    assert_eq!(
        err.to_api_body(None).error.reason.as_deref(),
        Some("capacity")
    );
    assert!(out.detail.attempts.is_empty(), "nothing was booted for it");

    let running = h
        .app
        .history
        .list_invocations(&a, &function.id, 10)
        .unwrap()
        .into_iter()
        .find(|d| d.invocation.status == InvocationStatus::Running)
        .unwrap();
    h.app
        .invoke
        .cancel(&a, &running.invocation.id)
        .await
        .unwrap();
    hanging.await.unwrap().unwrap();
}

/// Acceptance 3: jp-only is rejected explicitly on a node outside jp, with
/// nothing recorded, and accepted on a jp node. Tenant- and revision-level.
#[tokio::test]
async fn jp_only_is_rejected_on_a_node_outside_jp_and_never_relaxed() {
    let tenant_jp =
        format!("[[capacity.tenants]]\ntenant_id = \"{TENANT_B}\"\nrequired_region = \"jp\"\n");
    for (node_region, admitted) in [
        ("", false),
        ("region = \"us\"\n", false),
        ("region = \"jp\"\n", true),
    ] {
        let h = harness(
            FakeExecutionProvider::new(),
            &format!("[capacity]\nmax_concurrency = 4\n{tenant_jp}[capacity.node]\n{node_region}"),
        );
        let (a, b) = (principal(TENANT_A), principal(TENANT_B));
        let (fa, _) = deploy(&h, &a, "pinned", 4, Some("jp")).await;
        let (fb, _) = deploy(&h, &b, "tenant-jp", 4, None).await;
        let (fa_free, _) = deploy(&h, &a, "free", 4, None).await;
        for (p, f) in [(&a, &fa), (&b, &fb)] {
            let r = h.app.invoke.invoke(request(p, f)).await;
            match (admitted, r) {
                (true, Ok(out)) => assert!(out.succeeded()),
                (
                    false,
                    Err(
                        e @ AppError::Admission {
                            reason: RejectReason::Placement,
                            ..
                        },
                    ),
                ) => {
                    assert_eq!(e.http_status(), 503);
                    assert_eq!(
                        e.to_api_body(None).error.reason.as_deref(),
                        Some("placement")
                    );
                    assert!(
                        h.app
                            .history
                            .list_invocations(p, &f.id, 10)
                            .unwrap()
                            .is_empty(),
                        "a placement refusal records nothing"
                    );
                }
                (admitted, other) => panic!("node {node_region:?} admitted={admitted}: {other:?}"),
            }
        }
        // Unconstrained work is unaffected.
        assert!(
            h.app
                .invoke
                .invoke(request(&a, &fa_free))
                .await
                .unwrap()
                .succeeded()
        );
    }
}

/// Start-failure circuit breaker through the pipeline: after two failed
/// boots the third invocation is refused at once with `circuit_open` and
/// nothing is booted for it; another revision still runs.
#[tokio::test]
async fn repeated_boot_failures_open_the_breaker_and_fail_fast() {
    let h = harness(
        FakeExecutionProvider::with_options(FakeProviderOptions {
            fail_create: Some("kvm unavailable".into()),
            ..FakeProviderOptions::default()
        }),
        "[capacity]\nmax_concurrency = 4\n\
         [capacity.circuit_breaker]\nfailure_threshold = 2\ncooldown_seconds = 60\n",
    );
    let a = principal(TENANT_A);
    let (function, _) = deploy(&h, &a, "broken", 4, None).await;
    for _ in 0..2 {
        let out = h.app.invoke.invoke(request(&a, &function)).await.unwrap();
        let InvocationStatus::Failed { error } = &out.invocation().status else {
            panic!("{:?}", out.invocation().status);
        };
        assert_eq!(error.error_type, "Host.EnvironmentBootFailed");
    }
    let terminations = h.fake.terminated().len();
    let started = std::time::Instant::now();
    let err = h
        .app
        .invoke
        .invoke(request(&a, &function))
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_millis(500), "fast");
    assert!(
        matches!(
            err,
            AppError::Admission {
                reason: RejectReason::CircuitOpen,
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(err.http_status(), 503);
    assert_eq!(
        err.to_api_body(None).error.reason.as_deref(),
        Some("circuit_open")
    );
    assert_eq!(
        h.fake.terminated().len(),
        terminations,
        "nothing was attempted"
    );
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert_eq!(info.revisions[0].circuit_breaker, "open");
    assert_eq!(info.rejections.get("circuit_open"), Some(&1));
}

/// Acceptance 2 through the pipeline: tenant A floods a 2-slot node with
/// long invocations (quota 1 via tenant_defaults) while tenant B's short
/// invocations keep completing.
#[tokio::test]
async fn a_flooding_tenant_does_not_starve_another_one() {
    let h = harness(
        FakeExecutionProvider::new(),
        "[capacity]\nmax_concurrency = 2\nmax_queue = 64\nqueue_timeout_seconds = 10\n\
         [capacity.tenant_defaults]\nmax_concurrency = 1\nmax_queue = 32\n",
    );
    let (a, b) = (principal(TENANT_A), principal(TENANT_B));
    let (fa, _) = deploy(&h, &a, "long", 8, None).await;
    let (fb, _) = deploy(&h, &b, "short", 8, None).await;
    // A's one running invocation never ends (its quota is 1, so no other A
    // invocation boots and the script queue holds exactly this one).
    h.fake.push_script(FakeGuestScript::HangForever);
    let flood: Vec<_> = (0..8)
        .map(|_| {
            let app = h.app.clone();
            let req = request(&a, &fa);
            tokio::spawn(async move { app.invoke.invoke(req).await })
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while h.app.admission.snapshot(&a.tenant_id).tenant.queued < 7 {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A is at its quota with 7 queued; B's invocations run one after another.
    let started = std::time::Instant::now();
    for _ in 0..5 {
        let out = h.app.invoke.invoke(request(&b, &fb)).await.unwrap();
        assert!(out.succeeded(), "{:?}", out.invocation().status);
    }
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert_eq!(
        info.tenant.in_flight, 1,
        "the flooding tenant holds its quota only"
    );
    assert!(info.tenant.oldest_age_ms.is_some());

    h.app.invoke.shutdown_all(Duration::from_secs(5)).await;
    for f in flood {
        let _ = f.await.unwrap();
    }
}

/// With warm reuse on, a pooled environment keeps its reservation (counted
/// once, as idle), a warm start takes it over without reserving again, and a
/// cold start of another revision that the idle environment blocks on node
/// memory evicts it instead of waiting for the idle TTL.
#[tokio::test]
async fn pooled_environments_stay_reserved_once_and_are_evicted_for_capacity() {
    let h = harness(
        FakeExecutionProvider::with_options(FakeProviderOptions {
            warm_capable: true,
            ..FakeProviderOptions::default()
        }),
        "[capacity]\nmax_concurrency = 4\nqueue_timeout_seconds = 5\n\
         [capacity.node]\nmemory_mib = 300\n\
         [pool]\nenabled = true\nidle_ttl_seconds = 60\n",
    );
    h.fake
        .set_default_script(Some(FakeGuestScript::EchoForever));
    let a = principal(TENANT_A);
    let (fx, _) = deploy(&h, &a, "x", 4, None).await;
    let (fy, _) = deploy(&h, &a, "y", 4, None).await;

    assert!(
        h.app
            .invoke
            .invoke(request(&a, &fx))
            .await
            .unwrap()
            .succeeded()
    );
    h.app.pool.settle().await;
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert_eq!(info.environments.idle, 1);
    assert_eq!(info.reserved.memory_mib, Some(280), "idle, reserved once");
    assert_eq!(info.in_flight, 0);

    let warm = h.app.invoke.invoke(request(&a, &fx)).await.unwrap();
    assert!(warm.succeeded());
    assert_eq!(
        warm.detail.attempts[0].0.start_kind,
        tachyon_serverless_domain::StartKind::Warm
    );
    h.app.pool.settle().await;
    assert_eq!(h.fake.created().len(), 1);
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert_eq!(
        info.reserved.memory_mib,
        Some(280),
        "the warm start reserved nothing new"
    );

    // Y cannot fit next to X's idle environment: X is evicted, Y boots.
    let y = h.app.invoke.invoke(request(&a, &fy)).await.unwrap();
    assert!(y.succeeded(), "{:?}", y.invocation().status);
    assert_eq!(h.fake.created().len(), 2);
    h.app.pool.settle().await;
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert!(info.reserved.memory_mib.unwrap() <= 300);
    h.app.drain_pool().await;
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert_eq!(info.reserved.memory_mib, Some(0), "{info:?}");
    assert_eq!(info.environments, Default::default());
}

/// A cold start the invoke gate refuses (PLT-4636: here the provider's
/// control API fails its preflight) stays distinguishable from every
/// admission refusal: its own `error_type`, no admission `reason`, nothing
/// reserved, no admission rejection counted, and it never trips the
/// start-failure breaker however often it happens, because nothing booted.
#[tokio::test]
async fn control_plane_refusals_are_not_admission_refusals_and_do_not_trip_the_breaker() {
    let h = harness(
        FakeExecutionProvider::with_options(FakeProviderOptions {
            preflight_failure: Some("control API down".into()),
            ..FakeProviderOptions::default()
        }),
        "[capacity]\nmax_concurrency = 4\n\
         [capacity.circuit_breaker]\nfailure_threshold = 2\ncooldown_seconds = 60\n",
    );
    let a = principal(TENANT_A);
    let (function, _) = deploy(&h, &a, "gated", 4, None).await;
    assert!(!h.app.provider_service.preflight().await.ok);
    for _ in 0..4 {
        let error = match h.app.invoke.invoke(request(&a, &function)).await {
            Ok(out) => out.error().expect("refused"),
            Err(e) => e,
        };
        assert!(
            !matches!(error, AppError::Admission { .. }),
            "a gate refusal is not an admission refusal: {error}"
        );
        let body = error.to_api_body(None).error;
        assert!(
            error
                .to_string()
                .contains("Host.ProviderControlUnavailable")
                || body.error_type.as_deref() == Some("Host.ProviderControlUnavailable"),
            "{error}"
        );
        assert_eq!(body.reason, None, "no admission reason on a gate refusal");
        assert_eq!(error.http_status(), 503);
    }
    assert!(h.fake.created().is_empty(), "nothing was booted");
    let info = h.app.admission.snapshot(&a.tenant_id);
    assert_eq!(info.reserved.memory_mib, Some(0));
    assert_eq!(info.in_flight, 0);
    assert!(info.rejections.is_empty(), "{:?}", info.rejections);
    assert!(
        info.revisions.iter().all(|r| r.circuit_breaker == "closed"),
        "{:?}",
        info.revisions
    );
}
