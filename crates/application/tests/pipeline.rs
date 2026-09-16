//! End-to-end tests of the application layer against the fake provider.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{FramedRead, FramedWrite};

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
    SecretBindingRequest,
};
use tachyon_serverless_application::services::invoke::MAX_TRACE_ID_BYTES;
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeOutcome, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, Architecture, ArtifactRef, AttemptStatus, Clock, EgressProfile, EnvironmentId,
    EnvironmentState, ErrorClass, EventKind, ExecutionEnvironment, Function, FunctionRevision,
    InvocationId, InvocationStatus, ProviderKind, ResourceProfile, ReuseKey, RevisionId,
    RevisionStatus, Sha256Digest, TenantId, Timestamp, UsageEventType,
};
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message, encode_message,
};
use tachyon_serverless_provider_fake::{
    CustomScriptContext, FakeExecutionProvider, FakeGuestScript, FakeProviderOptions, ScriptFuture,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, Capabilities, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec,
    ExecutionProvider, PreflightReport, Principal, ProviderError, Role, TerminateReason,
    TerminateReport,
};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

fn config_toml(data_dir: &std::path::Path, profile: &str, extra: &str) -> String {
    format!(
        r#"
listen = "127.0.0.1:0"
profile = "{profile}"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process"

[[identity.tokens]]
token = "tok-a"
tenant_id = "{TENANT_A}"
subject = "dev-a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "tok-b"
tenant_id = "{TENANT_B}"
subject = "dev-b"
roles = ["deploy", "invoke"]

[[secrets.bindings]]
tenant_id = "{TENANT_A}"
binding_ref = "demo-secret"
value = "demo-secret-value-a"

[[secrets.bindings]]
tenant_id = "{TENANT_B}"
binding_ref = "demo-secret"
value = "demo-secret-value-b"

[invoke]
cancel_grace_ms = 100

{extra}
"#,
        data = data_dir.display(),
    )
}

struct Harness {
    app: Arc<Application>,
    fake: Arc<FakeExecutionProvider>,
    a: Principal,
    b: Principal,
    dir: tempfile::TempDir,
}

fn principal(tenant: &str, roles: Vec<Role>) -> Principal {
    Principal {
        subject: "t".into(),
        tenant_id: TenantId::parse(tenant).unwrap(),
        roles,
    }
}

fn harness(scripts: Vec<FakeGuestScript>, extra: &str) -> Harness {
    let fake = Arc::new(FakeExecutionProvider::with_scripts(scripts));
    harness_with(fake.clone(), fake, extra, None)
}

/// `fake` is the provider whose records the test inspects; `provider` is what
/// the application uses (the fake itself or a decorator around it).
fn harness_with(
    fake: Arc<FakeExecutionProvider>,
    provider: Arc<dyn ExecutionProvider>,
    extra: &str,
    clock: Option<Arc<dyn Clock>>,
) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let config = GatewayConfig::from_toml(&config_toml(dir.path(), "dev", extra)).unwrap();
    let mut options = BootstrapOptions {
        persist_state: true,
        ..BootstrapOptions::default()
    };
    if let Some(clock) = clock {
        options.clock = clock;
    }
    let app = Application::bootstrap_with(config, provider, options).unwrap();
    Harness {
        app,
        fake,
        a: principal(TENANT_A, vec![Role::Deploy, Role::Invoke]),
        b: principal(TENANT_B, vec![Role::Deploy, Role::Invoke]),
        dir,
    }
}

fn revision_request(
    digest: &str,
    timeout: u32,
    init_timeout: u32,
    max_concurrency: u32,
) -> CreateRevisionRequest {
    CreateRevisionRequest {
        artifact: ArtifactRequest::Binary {
            digest: digest.to_string(),
        },
        architecture: "aarch64".into(),
        resources: ResourcesRequest::default(),
        execution: ExecutionRequest {
            timeout_seconds: timeout,
            initialization_timeout_seconds: init_timeout,
            max_concurrency,
        },
        egress: None,
        env_vars: vec![("GREETING".into(), "hello".into())],
        secrets: vec![SecretBindingRequest {
            env_name: "DEMO_SECRET".into(),
            binding_ref: "demo-secret".into(),
        }],
        description: "test".into(),
        publish_to_prod: true,
    }
}

async fn deploy(h: &Harness, principal: &Principal, name: &str) -> (Function, FunctionRevision) {
    deploy_with(h, principal, name, 30, 30, 4).await
}

async fn deploy_with(
    h: &Harness,
    principal: &Principal,
    name: &str,
    timeout: u32,
    init_timeout: u32,
    max_concurrency: u32,
) -> (Function, FunctionRevision) {
    deploy_custom(h, principal, name, |req| {
        req.execution.timeout_seconds = timeout;
        req.execution.initialization_timeout_seconds = init_timeout;
        req.execution.max_concurrency = max_concurrency;
    })
    .await
}

/// Upload an artifact through the ownership-recording service, create the
/// function and a revision (adjusted by `customize`), and wait until it is
/// Ready and published to `prod`.
async fn deploy_custom(
    h: &Harness,
    principal: &Principal,
    name: &str,
    customize: impl FnOnce(&mut CreateRevisionRequest),
) -> (Function, FunctionRevision) {
    let artifact = h
        .app
        .artifact_service
        .upload(principal, format!("#!/bin/sh\necho {name}\n").as_bytes())
        .await
        .unwrap();
    let function = h.app.functions.create(principal, name, "").unwrap();
    let mut req = revision_request(artifact.digest.as_str(), 30, 30, 4);
    customize(&mut req);
    let rev = h
        .app
        .revisions
        .create(principal, &function.id, &req)
        .await
        .unwrap();
    assert_eq!(rev.status, RevisionStatus::Pending);
    let rev = h
        .app
        .revisions
        .wait_terminal(&rev.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(rev.status, RevisionStatus::Ready, "{:?}", rev.status);
    let alias = h
        .app
        .aliases
        .get(principal, &function.id, &AliasName::default_alias())
        .unwrap();
    assert_eq!(alias.revision_id, rev.id);
    (function, rev)
}

fn invoke_request(
    principal: &Principal,
    function: &Function,
    payload: serde_json::Value,
) -> InvokeRequest {
    InvokeRequest {
        principal: principal.clone(),
        function_id: function.id.clone(),
        alias: None,
        revision_id: None,
        event_kind: EventKind::Json,
        payload,
        idempotency_key: None,
        client_timeout_ms: None,
        trace_id: None,
    }
}

#[tokio::test]
async fn happy_path_records_timings_evidence_secrets_and_cleanup() {
    let h = harness(
        vec![FakeGuestScript::RespondOk(
            serde_json::json!({"answer": 42}),
        )],
        "",
    );
    let (function, rev) = deploy(&h, &h.a, "hello").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({"q": 1})))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert_eq!(out.output, Some(serde_json::json!({"answer": 42})));
    let inv = out.invocation();
    assert_eq!(inv.revision_id, rev.id);
    assert_eq!(inv.alias, Some(AliasName::default_alias()));
    assert!(inv.output.is_some(), "inline output stored");
    assert_eq!(out.detail.attempts.len(), 1);
    let (attempt, evidence) = &out.detail.attempts[0];
    assert_eq!(attempt.status, AttemptStatus::Succeeded);
    assert_eq!(attempt.epoch, 1);
    let t = attempt.timings;
    assert!(t.queue_wait_ms.is_some());
    assert!(t.environment_boot_ms.is_some());
    assert!(t.runtime_init_ms.is_some());
    assert!(t.handler_ms.is_some());
    assert!(t.total_ms.is_some());
    assert!(
        evidence
            .guest_boot_id
            .as_deref()
            .unwrap()
            .starts_with("fake-boot-")
    );
    assert_eq!(evidence.host_pid, Some(std::process::id()));
    assert_eq!(
        evidence.details.get("provider"),
        Some(&serde_json::json!("fake"))
    );
    assert!(evidence.details.contains_key("guest_handler_ms"));

    // HelloAck composition: env vars + secret + TACHYON_UNISOLATED, entrypoint = artifact path.
    let env_id = &attempt.environment_id;
    let Some(HostMessage::HelloAck {
        env,
        entrypoint,
        working_dir,
        epoch,
        ..
    }) = h.fake.hello_ack(env_id)
    else {
        panic!("guest did not receive HelloAck");
    };
    assert_eq!(epoch, 1);
    assert!(env.contains(&("GREETING".to_string(), "hello".to_string())));
    assert!(env.contains(&("DEMO_SECRET".to_string(), "demo-secret-value-a".to_string())));
    assert!(env.contains(&("TACHYON_UNISOLATED".to_string(), "1".to_string())));
    assert!(entrypoint.contains("/artifacts/"));
    assert!(working_dir.ends_with(env_id.as_str()));

    // destroy-after-invoke: terminated once with Completed, environment Stopped.
    assert_eq!(
        h.fake.terminated(),
        vec![(env_id.clone(), TerminateReason::Completed)]
    );
    assert!(h.fake.running().is_empty());
    let env = h.app.repos.environments.get(env_id).unwrap().unwrap();
    assert_eq!(env.state, EnvironmentState::Stopped);
    assert!(env.evidence.guest_boot_id.is_some());

    // logs: guest init/handler lines + platform lines, tenant scoped.
    let logs = h.app.logs.for_invocation(&h.a, &inv.id).unwrap();
    assert!(
        logs.records
            .iter()
            .any(|r| r.line.contains("fake guest: handling"))
    );
    assert!(
        logs.records
            .iter()
            .any(|r| r.line.contains("environment terminated"))
    );
    assert!(!logs.dropped);
    assert!(matches!(
        h.app.logs.for_invocation(&h.b, &inv.id),
        Err(AppError::NotFound(_))
    ));

    // usage: host observed
    let usage = h.app.history.usage_summary(&h.a, &function.id).unwrap();
    assert_eq!(usage.invocations, 1);
    assert_eq!(usage.succeeded, 1);
    assert!(usage.bytes_in_total > 0);
    assert!(usage.bytes_out_total > 0);

    // secrets never reach the ledger on disk
    let state = std::fs::read_to_string(h.app.store.persist_path().unwrap()).unwrap();
    assert!(!state.contains("demo-secret-value-a"));
    assert_eq!(h.app.invoke.in_flight_count(), 0);
}

#[tokio::test]
async fn user_error_panic_init_error_and_never_ready() {
    let h = harness(
        vec![
            FakeGuestScript::HandlerError {
                error_type: "Handler.Error".into(),
                message: "bad input".into(),
            },
            FakeGuestScript::Panic,
            FakeGuestScript::InitError {
                message: "cannot open db".into(),
            },
            FakeGuestScript::NeverReady,
            FakeGuestScript::CrashAfterReady { exit_code: 7 },
        ],
        "",
    );
    let (function, _) = deploy_with(&h, &h.a, "errors", 5, 1, 4).await;

    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    match &out.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::UserError);
            assert_eq!(error.error_type, "Handler.Error");
            assert_eq!(error.message, "bad input");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(out.error().unwrap().http_status(), 502);

    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    match &out.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::Crash);
            assert_eq!(error.error_type, "Runtime.Panic");
        }
        other => panic!("{other:?}"),
    }
    let logs = h
        .app
        .logs
        .for_invocation(&h.a, &out.invocation().id)
        .unwrap();
    assert!(
        logs.records
            .iter()
            .any(|r| r.line.contains("fake guest stack trace"))
    );

    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    match &out.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::InitError);
            assert_eq!(error.error_type, "Runtime.InitError");
            assert_eq!(error.message, "cannot open db");
        }
        other => panic!("{other:?}"),
    }
    assert!(out.detail.attempts.is_empty(), "no attempt before Ready");

    let started = std::time::Instant::now();
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    match &out.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::InitError);
            assert_eq!(error.error_type, "Host.InitTimeout");
        }
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert!(started.elapsed() < Duration::from_secs(4));

    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    match &out.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::Crash);
            assert_eq!(error.error_type, "Runtime.Crash");
        }
        other => panic!("{other:?}"),
    }

    // every environment was terminated exactly once and none is running
    let created = h.fake.created();
    assert_eq!(created.len(), 5);
    let terminated = h.fake.terminated();
    assert_eq!(terminated.len(), 5);
    for id in &created {
        assert!(terminated.iter().any(|(t, _)| t == id));
        let env = h.app.repos.environments.get(id).unwrap().unwrap();
        assert!(env.is_terminal(), "{:?}", env.state);
    }
    assert!(
        terminated
            .iter()
            .filter(|(_, r)| *r == TerminateReason::InitFailed)
            .count()
            == 2
    );
    assert!(
        terminated
            .iter()
            .any(|(_, r)| *r == TerminateReason::Crashed)
    );
    assert!(h.fake.running().is_empty());
    assert_eq!(h.app.invoke.in_flight_count(), 0);
}

#[tokio::test]
async fn hang_times_out_cancels_and_terminates_with_timeout() {
    let h = harness(vec![FakeGuestScript::HangForever], "");
    let (function, _) = deploy_with(&h, &h.a, "hang", 1, 5, 4).await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    match &out.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::Timeout);
            assert_eq!(error.error_type, "Host.Timeout");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(out.error().unwrap().http_status(), 504);
    let (attempt, _) = &out.detail.attempts[0];
    assert!(matches!(attempt.status, AttemptStatus::Failed { .. }));
    let env_id = &attempt.environment_id;
    let msgs = h.fake.host_messages(env_id);
    assert!(
        msgs.iter().any(|m| matches!(m, HostMessage::Cancel { .. })),
        "Cancel must be sent: {msgs:?}"
    );
    assert_eq!(
        h.fake.terminated(),
        vec![(env_id.clone(), TerminateReason::Timeout)]
    );
    let env = h.app.repos.environments.get(env_id).unwrap().unwrap();
    assert!(
        matches!(env.state, EnvironmentState::Failed { .. }),
        "{:?}",
        env.state
    );
    assert!(h.fake.running().is_empty());

    // A second invoke never reuses the environment.
    h.fake
        .push_script(FakeGuestScript::RespondOk(serde_json::json!(1)));
    let out2 = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    assert!(out2.succeeded());
    assert_ne!(out2.detail.attempts[0].0.environment_id, *env_id);
}

#[tokio::test]
async fn disconnect_after_invoke_is_outcome_unknown() {
    let h = harness(vec![FakeGuestScript::DisconnectAfterInvoke], "");
    let (function, _) = deploy(&h, &h.a, "disconnect").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    match &out.invocation().status {
        InvocationStatus::OutcomeUnknown { error } => {
            assert_eq!(error.class, ErrorClass::OutcomeUnknown);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        out.detail.attempts[0].0.status,
        AttemptStatus::OutcomeUnknown { .. }
    ));
    assert_eq!(out.error().unwrap().http_status(), 502);
    assert_eq!(h.fake.terminated().len(), 1);
    assert!(h.fake.running().is_empty());
}

#[tokio::test]
async fn capacity_exceeded_and_queue_timeout() {
    let h = harness(
        vec![FakeGuestScript::HangForever, FakeGuestScript::HangForever],
        "[capacity]\nmax_concurrency = 1\nmax_queue = 1\nqueue_timeout_seconds = 1\n",
    );
    let (function, _) = deploy_with(&h, &h.a, "busy", 10, 5, 4).await;
    let app = h.app.clone();
    let first = tokio::spawn({
        let req = invoke_request(&h.a, &function, serde_json::json!({}));
        let app = app.clone();
        async move { app.invoke.invoke(req).await }
    });
    // wait until the first invocation holds the only slot
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(app.invoke.in_flight_count(), 1);

    // second: queue has one slot -> waits -> 504 after queue_timeout
    let second = tokio::spawn({
        let req = invoke_request(&h.a, &function, serde_json::json!({}));
        let app = app.clone();
        async move { app.invoke.invoke(req).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // third: queue full -> 429 without a ledger entry
    let err = app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .err()
        .unwrap();
    assert!(matches!(err, AppError::CapacityExceeded(_)), "{err}");
    assert_eq!(err.http_status(), 429);

    let second = second.await.unwrap().unwrap();
    match &second.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::QueueTimeout);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(second.error().unwrap().http_status(), 504);
    assert!(second.detail.attempts.is_empty());

    // cancel the hanging one
    let first_id = {
        let list = app
            .history
            .list_invocations(&h.a, &function.id, 10)
            .unwrap();
        list.iter()
            .find(|d| d.invocation.status == InvocationStatus::Running)
            .map(|d| d.invocation.id.clone())
            .expect("running invocation")
    };
    let cancelled = app.invoke.cancel(&h.a, &first_id).await.unwrap();
    assert_eq!(cancelled.invocation.status, InvocationStatus::Cancelled);
    let first = first.await.unwrap().unwrap();
    assert_eq!(first.invocation().status, InvocationStatus::Cancelled);
    assert_eq!(first.error().unwrap().http_status(), 499);
    assert!(
        h.fake
            .terminated()
            .iter()
            .any(|(_, r)| *r == TerminateReason::Cancelled)
    );
    assert!(h.fake.running().is_empty());
    // only two invocations exist: the 429 never entered the ledger
    assert_eq!(
        app.history
            .list_invocations(&h.a, &function.id, 10)
            .unwrap()
            .len(),
        2
    );
    // cancelling again is idempotent; cancelling a finished one conflicts
    assert!(app.invoke.cancel(&h.a, &first_id).await.is_ok());
    assert!(matches!(
        app.invoke.cancel(&h.a, &second.invocation().id).await,
        Err(AppError::Conflict(_))
    ));
}

#[tokio::test]
async fn cross_tenant_resources_are_not_found() {
    let h = harness(vec![], "");
    let (function, rev) = deploy(&h, &h.a, "mine").await;
    assert!(matches!(
        h.app.functions.get(&h.b, &function.id),
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        h.app.revisions.get(&h.b, &function.id, &rev.id),
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        h.app
            .aliases
            .get(&h.b, &function.id, &AliasName::default_alias()),
        Err(AppError::NotFound(_))
    ));
    let err = h
        .app
        .invoke
        .invoke(invoke_request(&h.b, &function, serde_json::json!({})))
        .await
        .err()
        .unwrap();
    assert!(matches!(err, AppError::NotFound(_)));
    assert_eq!(err.http_status(), 404);
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    assert!(matches!(
        h.app.history.get_invocation(&h.b, &out.invocation().id),
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        h.app.invoke.cancel(&h.b, &out.invocation().id).await,
        Err(AppError::NotFound(_))
    ));
    assert!(h.app.functions.list(&h.b).unwrap().is_empty());
    // role check: invoke-only principal cannot deploy
    let invoke_only = principal(TENANT_A, vec![Role::Invoke]);
    assert!(matches!(
        h.app.functions.create(&invoke_only, "nope", ""),
        Err(AppError::Forbidden(_))
    ));
    // deleted function refuses invocations
    h.app.functions.delete(&h.a, &function.id).unwrap();
    assert!(matches!(
        h.app
            .invoke
            .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
            .await,
        Err(AppError::FunctionDeleted(_))
    ));
}

#[tokio::test]
async fn alias_cas_conflict_and_rollback() {
    let h = harness(vec![], "");
    let (function, rev1) = deploy(&h, &h.a, "cas").await;
    let artifact = h
        .app
        .artifact_service
        .upload(&h.a, b"#!/bin/sh\necho v2\n")
        .await
        .unwrap();
    let mut req = revision_request(artifact.digest.as_str(), 30, 30, 4);
    req.publish_to_prod = false;
    let rev2 = h
        .app
        .revisions
        .create(&h.a, &function.id, &req)
        .await
        .unwrap();
    let rev2 = h
        .app
        .revisions
        .wait_terminal(&rev2.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(rev2.is_ready());
    assert_eq!(rev2.number, 2);
    let prod = AliasName::default_alias();
    let alias = h.app.aliases.get(&h.a, &function.id, &prod).unwrap();
    assert_eq!(
        alias.revision_id, rev1.id,
        "publish_to_prod=false leaves prod alone"
    );
    assert_eq!(alias.generation, 1);

    let err = h
        .app
        .aliases
        .update(&h.a, &function.id, &prod, &rev2.id, Some(99))
        .err()
        .unwrap();
    assert!(matches!(err, AppError::Conflict(_)), "{err}");
    assert_eq!(err.http_status(), 409);

    let updated = h
        .app
        .aliases
        .update(&h.a, &function.id, &prod, &rev2.id, Some(1))
        .unwrap();
    assert_eq!(updated.generation, 2);
    assert_eq!(updated.revision_id, rev2.id);
    assert_eq!(updated.previous_revision_id, Some(rev1.id.clone()));

    let rolled = h.app.aliases.rollback(&h.a, &function.id, &prod).unwrap();
    assert_eq!(rolled.revision_id, rev1.id);
    assert_eq!(rolled.generation, 3);

    // pointing an alias at a non-ready revision is refused
    let mut bad = revision_request(
        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        30,
        30,
        4,
    );
    bad.publish_to_prod = false;
    let rev3 = h
        .app
        .revisions
        .create(&h.a, &function.id, &bad)
        .await
        .unwrap();
    let rev3 = h
        .app
        .revisions
        .wait_terminal(&rev3.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(rev3.status, RevisionStatus::Failed { .. }));
    assert!(matches!(
        h.app
            .aliases
            .update(&h.a, &function.id, &prod, &rev3.id, None),
        Err(AppError::RevisionNotReady(_))
    ));
}

#[tokio::test]
async fn idempotency_replay_and_conflict() {
    let h = harness(
        vec![FakeGuestScript::RespondOk(serde_json::json!({"n": 1}))],
        "",
    );
    let (function, _) = deploy(&h, &h.a, "idem").await;
    let mut req = invoke_request(&h.a, &function, serde_json::json!({"x": 1}));
    req.idempotency_key = Some("key-1".into());
    let first = h.app.invoke.invoke(req.clone()).await.unwrap();
    assert!(first.succeeded());
    let again = h.app.invoke.invoke(req.clone()).await.unwrap();
    assert!(again.replayed);
    assert_eq!(again.invocation().id, first.invocation().id);
    assert_eq!(again.output, Some(serde_json::json!({"n": 1})));
    assert_eq!(h.fake.created().len(), 1, "replay does not run again");

    req.payload = serde_json::json!({"x": 2});
    let err = h.app.invoke.invoke(req).await.err().unwrap();
    assert!(matches!(err, AppError::Conflict(_)), "{err}");
    assert_eq!(err.http_status(), 409);
}

#[tokio::test]
async fn revision_is_pinned_at_accept_even_if_alias_changes_mid_flight() {
    let h = harness(
        vec![
            FakeGuestScript::HangForever,
            FakeGuestScript::RespondOk(serde_json::json!(2)),
        ],
        "",
    );
    let (function, rev1) = deploy_with(&h, &h.a, "pin", 10, 5, 4).await;
    let app = h.app.clone();
    let running = tokio::spawn({
        let req = invoke_request(&h.a, &function, serde_json::json!({}));
        let app = app.clone();
        async move { app.invoke.invoke(req).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // publish a second revision to prod while the first invocation runs
    let artifact = app
        .artifact_service
        .upload(&h.a, b"#!/bin/sh\necho v2\n")
        .await
        .unwrap();
    let rev2 = app
        .revisions
        .create(
            &h.a,
            &function.id,
            &revision_request(artifact.digest.as_str(), 10, 5, 4),
        )
        .await
        .unwrap();
    let rev2 = app
        .revisions
        .wait_terminal(&rev2.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(rev2.is_ready());
    let alias = app
        .aliases
        .get(&h.a, &function.id, &AliasName::default_alias())
        .unwrap();
    assert_eq!(alias.revision_id, rev2.id);

    let list = app
        .history
        .list_invocations(&h.a, &function.id, 10)
        .unwrap();
    let inflight = &list[0].invocation;
    assert_eq!(inflight.status, InvocationStatus::Running);
    assert_eq!(inflight.revision_id, rev1.id, "still pinned to rev1");
    app.invoke.cancel(&h.a, &inflight.id).await.unwrap();
    let done = running.await.unwrap().unwrap();
    assert_eq!(done.invocation().revision_id, rev1.id);

    // the next invocation resolves the new alias target
    let out = app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    assert!(out.succeeded());
    assert_eq!(out.invocation().revision_id, rev2.id);

    // pinning a revision explicitly bypasses the alias
    let mut pinned = invoke_request(&h.a, &function, serde_json::json!({}));
    pinned.revision_id = Some(rev1.id.clone());
    h.fake
        .push_script(FakeGuestScript::RespondOk(serde_json::json!(1)));
    let out = app.invoke.invoke(pinned).await.unwrap();
    assert_eq!(out.invocation().revision_id, rev1.id);
    assert_eq!(out.invocation().alias, None);
}

#[tokio::test]
async fn production_profile_rejects_dev_only_provider() {
    let dir = tempfile::tempdir().unwrap();
    // the static config check already refuses `process` in production, so
    // pretend firecracker was configured and hand in a dev-only provider.
    let toml = config_toml(dir.path(), "production", "")
        .replace("kind = \"process\"", "kind = \"firecracker\"")
        .replace(
            "[provider.process]\nbridge_binary = \"target/debug/tachyon-serverless-runtime-bridge\"",
            "[provider.firecracker]\nfirecracker_binary = \".kvm/bin/firecracker\"\nkernel = \".kvm/vmlinux\"\nrootfs = \".kvm/rootfs.ext4\"",
        );
    let config = GatewayConfig::from_toml(&toml).unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    assert!(fake.capabilities().dev_only);
    let err = Application::bootstrap(config, fake).err().unwrap();
    assert!(matches!(err, AppError::InvalidRequest(_)), "{err}");
    assert!(err.to_string().contains("dev-only"));

    // and the static check for process + production
    let toml = config_toml(dir.path(), "production", "");
    assert!(GatewayConfig::from_toml(&toml).is_err());
}

#[tokio::test]
async fn shutdown_all_cancels_in_flight_and_refuses_new_work() {
    let h = harness(vec![FakeGuestScript::HangForever], "");
    let (function, _) = deploy_with(&h, &h.a, "shutdown", 10, 5, 4).await;
    let app = h.app.clone();
    let running = tokio::spawn({
        let req = invoke_request(&h.a, &function, serde_json::json!({}));
        let app = app.clone();
        async move { app.invoke.invoke(req).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    app.invoke.shutdown_all(Duration::from_secs(5)).await;
    let done = running.await.unwrap().unwrap();
    assert_eq!(done.invocation().status, InvocationStatus::Cancelled);
    assert!(
        h.fake
            .terminated()
            .iter()
            .any(|(_, r)| *r == TerminateReason::Shutdown)
    );
    assert!(h.fake.running().is_empty());
    assert!(matches!(
        app.invoke
            .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
            .await,
        Err(AppError::ProviderUnavailable(_))
    ));
}

// ---------------------------------------------------------------------------
// shared helpers for the regression tests below
// ---------------------------------------------------------------------------

fn with_status(h: &Harness, function: &Function, status: InvocationStatus) -> Option<InvocationId> {
    h.app
        .history
        .list_invocations(&h.a, &function.id, 50)
        .unwrap()
        .iter()
        .find(|d| d.invocation.status == status)
        .map(|d| d.invocation.id.clone())
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn failed_error(out: &InvokeOutcome) -> tachyon_serverless_domain::InvocationError {
    match &out.invocation().status {
        InvocationStatus::Failed { error } => error.clone(),
        other => panic!("expected a failed invocation, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// idempotency (docs/threat-model.md §10)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capacity_rejection_does_not_consume_the_idempotency_key() {
    let h = harness(
        vec![
            FakeGuestScript::HangForever,
            FakeGuestScript::RespondOk(serde_json::json!({"n": 2})),
            FakeGuestScript::RespondOk(serde_json::json!({"n": 3})),
        ],
        "[capacity]\nmax_concurrency = 1\nmax_queue = 1\nqueue_timeout_seconds = 10\n",
    );
    let (function, _) = deploy_with(&h, &h.a, "keyed", 10, 5, 4).await;
    let app = h.app.clone();
    let hanging = tokio::spawn({
        let req = invoke_request(&h.a, &function, serde_json::json!({}));
        let app = app.clone();
        async move { app.invoke.invoke(req).await }
    });
    wait_until("the first invocation to run", || {
        with_status(&h, &function, InvocationStatus::Running).is_some()
    })
    .await;
    let queued = tokio::spawn({
        let req = invoke_request(&h.a, &function, serde_json::json!({}));
        let app = app.clone();
        async move { app.invoke.invoke(req).await }
    });
    wait_until("the second invocation to queue", || {
        with_status(&h, &function, InvocationStatus::Queued).is_some()
    })
    .await;

    let mut keyed = invoke_request(&h.a, &function, serde_json::json!({"k": 1}));
    keyed.idempotency_key = Some("retry-me".into());
    for attempt in 0..2 {
        let err = app.invoke.invoke(keyed.clone()).await.err().unwrap();
        assert!(
            matches!(err, AppError::CapacityExceeded(_)),
            "attempt {attempt}: {err}"
        );
        assert_eq!(err.http_status(), 429);
    }

    // Drain: cancel the hanging invocation; the queued one runs.
    let hanging_id = with_status(&h, &function, InvocationStatus::Running).unwrap();
    app.invoke.cancel(&h.a, &hanging_id).await.unwrap();
    assert_eq!(
        hanging.await.unwrap().unwrap().invocation().status,
        InvocationStatus::Cancelled
    );
    assert!(queued.await.unwrap().unwrap().succeeded());

    // The retry after backpressure executes; it is neither a 404 nor a replay.
    let out = app.invoke.invoke(keyed.clone()).await.unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert!(!out.replayed);
    assert_eq!(out.output, Some(serde_json::json!({"n": 3})));
    let again = app.invoke.invoke(keyed).await.unwrap();
    assert!(again.replayed);
    assert_eq!(again.invocation().id, out.invocation().id);
    assert_eq!(h.fake.created().len(), 3);
}

#[tokio::test]
async fn invalid_idempotency_key_and_trace_id_are_rejected_without_side_effects() {
    let h = harness(vec![], "");
    let (function, _) = deploy(&h, &h.a, "strict").await;
    let long_key = "k".repeat(300);
    let mut req = invoke_request(&h.a, &function, serde_json::json!({}));
    req.idempotency_key = Some(long_key.clone());
    for attempt in 0..2 {
        let err = h.app.invoke.invoke(req.clone()).await.err().unwrap();
        assert!(
            matches!(err, AppError::InvalidRequest(_)),
            "attempt {attempt}: {err}"
        );
        assert_eq!(err.http_status(), 400);
    }
    let mut traced = invoke_request(&h.a, &function, serde_json::json!({}));
    traced.trace_id = Some("t".repeat(MAX_TRACE_ID_BYTES + 1));
    let err = h.app.invoke.invoke(traced).await.err().unwrap();
    assert!(matches!(err, AppError::InvalidRequest(_)), "{err}");

    assert!(h.fake.created().is_empty());
    assert!(
        h.app
            .history
            .list_invocations(&h.a, &function.id, 10)
            .unwrap()
            .is_empty()
    );
    let state = std::fs::read_to_string(h.app.store.persist_path().unwrap()).unwrap();
    assert!(
        !state.contains(&long_key),
        "the rejected key was not stored"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_with_the_same_key_run_once() {
    let fake = Arc::new(FakeExecutionProvider::with_options(FakeProviderOptions {
        boot_delay: Duration::from_millis(300),
        ..FakeProviderOptions::default()
    }));
    fake.push_script(FakeGuestScript::RespondOk(
        serde_json::json!({"once": true}),
    ));
    // A second environment would fail to boot instead of silently running.
    fake.set_default_script(None);
    let h = harness_with(fake.clone(), fake, "", None);
    let (function, _) = deploy(&h, &h.a, "once").await;
    let mut req = invoke_request(&h.a, &function, serde_json::json!({"same": 1}));
    req.idempotency_key = Some("one-key".into());

    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let app = h.app.clone();
            let req = req.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                app.invoke.invoke(req).await
            })
        })
        .collect();
    let mut ids = Vec::new();
    let mut executed = 0;
    for task in tasks {
        let out = task
            .await
            .unwrap()
            .expect("no request with the shared key may fail");
        assert!(out.succeeded(), "{:?}", out.invocation().status);
        assert_eq!(out.output, Some(serde_json::json!({"once": true})));
        if !out.replayed {
            executed += 1;
        }
        ids.push(out.invocation().id.clone());
    }
    assert_eq!(executed, 1);
    assert!(ids.windows(2).all(|w| w[0] == w[1]), "{ids:?}");
    assert_eq!(h.fake.created().len(), 1);
    assert_eq!(
        h.app
            .history
            .list_invocations(&h.a, &function.id, 10)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn dangling_idempotency_key_is_healed_on_restart() {
    let h = harness(vec![], "");
    let (function, _) = deploy(&h, &h.a, "healed").await;
    let payload = serde_json::json!({"retry": true});

    // A state file written by a version that bound keys before acceptance:
    // the key points at an invocation that was never recorded.
    let path = h.app.store.persist_path().unwrap().to_path_buf();
    let mut state: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    state["idempotency"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!([
            {"tenant_id": TENANT_A, "function_id": function.id.to_string(), "key": "stale"},
            {"invocation_id": InvocationId::generate().to_string(),
             "input_digest": Sha256Digest::of_bytes(&serde_json::to_vec(&payload).unwrap()).to_string()}
        ]));
    std::fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();

    // Restart on the same data_dir.
    let config = GatewayConfig::from_toml(&config_toml(h.dir.path(), "dev", "")).unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let app = Application::bootstrap_with(
        config,
        fake.clone(),
        BootstrapOptions {
            persist_state: true,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    let mut req = invoke_request(&h.a, &function, payload);
    req.idempotency_key = Some("stale".into());
    let out = app.invoke.invoke(req.clone()).await.unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert!(!out.replayed);
    assert_eq!(fake.created().len(), 1);
    let again = app.invoke.invoke(req).await.unwrap();
    assert!(again.replayed);
    assert_eq!(again.invocation().id, out.invocation().id);
}

// ---------------------------------------------------------------------------
// artifact ownership (docs/threat-model.md §14-1)
// ---------------------------------------------------------------------------

async fn revision_for_digest(
    h: &Harness,
    principal: &Principal,
    function: &Function,
    digest: &Sha256Digest,
) -> FunctionRevision {
    let mut req = revision_request(digest.as_str(), 30, 30, 4);
    req.publish_to_prod = false;
    let rev = h
        .app
        .revisions
        .create(principal, &function.id, &req)
        .await
        .unwrap();
    assert_eq!(rev.status, RevisionStatus::Pending);
    h.app
        .revisions
        .wait_terminal(&rev.id, Duration::from_secs(5))
        .await
        .unwrap()
}

fn failure_reason(rev: &FunctionRevision) -> String {
    match &rev.status {
        RevisionStatus::Failed { reason } => reason.clone(),
        other => panic!("expected a failed revision, got {other:?}"),
    }
}

fn artifact_size(rev: &FunctionRevision) -> u64 {
    match &rev.spec.artifact {
        ArtifactRef::Binary { size_bytes, .. } => *size_bytes,
        other => panic!("expected a binary artifact, got {other:?}"),
    }
}

#[tokio::test]
async fn revisions_cannot_reference_another_tenants_artifact() {
    let h = harness(vec![], "");
    let (_, owner_rev) = deploy(&h, &h.a, "owner").await;
    let ArtifactRef::Binary { digest: owned, .. } = owner_rev.spec.artifact.clone() else {
        panic!("binary artifact expected");
    };
    let missing = Sha256Digest::of_bytes(b"never uploaded");
    let borrower = h.app.functions.create(&h.b, "borrower", "").unwrap();

    let foreign = revision_for_digest(&h, &h.b, &borrower, &owned).await;
    let unknown = revision_for_digest(&h, &h.b, &borrower, &missing).await;
    assert_eq!(
        failure_reason(&foreign).replace(owned.as_str(), "<digest>"),
        failure_reason(&unknown).replace(missing.as_str(), "<digest>"),
        "a foreign digest must fail exactly like a missing one"
    );
    assert_eq!(artifact_size(&foreign), artifact_size(&unknown));

    // Uploading the same bytes makes B an owner of the digest as well.
    let again = h
        .app
        .artifact_service
        .upload(&h.b, b"#!/bin/sh\necho owner\n")
        .await
        .unwrap();
    assert_eq!(again.digest, owned);
    let own = revision_for_digest(&h, &h.b, &borrower, &owned).await;
    assert_eq!(own.status, RevisionStatus::Ready, "{:?}", own.status);
    assert!(
        h.app
            .revisions
            .get(&h.a, &owner_rev.function_id, &owner_rev.id)
            .unwrap()
            .is_ready()
    );
}

// ---------------------------------------------------------------------------
// secret bindings
// ---------------------------------------------------------------------------

fn bind_secret(binding: &'static str) -> impl FnOnce(&mut CreateRevisionRequest) {
    move |req| {
        req.secrets = vec![SecretBindingRequest {
            env_name: "DEMO_SECRET".into(),
            binding_ref: binding.into(),
        }];
    }
}

#[tokio::test]
async fn unavailable_secret_binding_is_an_init_error_without_booting() {
    let extra = format!(
        "[[secrets.bindings]]\ntenant_id = \"{TENANT_A}\"\nbinding_ref = \"only-a\"\nvalue = \"only-a-value\"\n"
    );
    let h = harness(vec![], &extra);
    let (foreign_fn, _) = deploy_custom(&h, &h.b, "foreign-binding", bind_secret("only-a")).await;
    let (missing_fn, _) =
        deploy_custom(&h, &h.b, "missing-binding", bind_secret("nobody-has-this")).await;

    let mut messages = Vec::new();
    for (function, binding) in [(&foreign_fn, "only-a"), (&missing_fn, "nobody-has-this")] {
        let out = h
            .app
            .invoke
            .invoke(invoke_request(&h.b, function, serde_json::json!({})))
            .await
            .unwrap();
        let error = failed_error(&out);
        assert_eq!(error.class, ErrorClass::InitError, "{binding}");
        assert_eq!(error.error_type, "Host.SecretBindingUnavailable");
        assert_eq!(out.error().unwrap().http_status(), 502);
        assert!(out.detail.attempts.is_empty());
        let body = serde_json::to_string(&out.error().unwrap().to_api_body(None)).unwrap();
        for leak in [TENANT_A, TENANT_B, "forbidden", "Forbidden", "only-a-value"] {
            assert!(!body.contains(leak), "{leak} in {body}");
        }
        let logs = h
            .app
            .logs
            .for_invocation(&h.b, &out.invocation().id)
            .unwrap();
        assert!(
            logs.records
                .iter()
                .all(|r| !r.line.contains("orbidden") && !r.line.contains(TENANT_A))
        );
        messages.push(error.message.replace(binding, "<binding>"));
    }
    assert_eq!(
        messages[0], messages[1],
        "foreign and missing bindings must be indistinguishable"
    );
    assert!(
        h.fake.created().is_empty(),
        "no environment is booted for an unusable binding"
    );
    assert!(h.fake.terminated().is_empty());
    assert!(h.fake.running().is_empty());
    assert!(h.app.repos.environments.list_active().unwrap().is_empty());
    assert_eq!(h.app.invoke.in_flight_count(), 0);
}

#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogs;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// docs/threat-model.md T03 (PLT-4623): the resolved secret value is handed
/// to the guest in `HelloAck` but never appears in host logs, at any level,
/// nor in the invocation log records.
#[tokio::test]
async fn secret_values_never_reach_host_logs() {
    let captured = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(captured.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let h = harness(
        vec![
            FakeGuestScript::RespondOk(serde_json::json!({"ok": true})),
            FakeGuestScript::HandlerError {
                error_type: "Handler.Error".into(),
                message: "nope".into(),
            },
            FakeGuestScript::HangForever,
        ],
        "",
    );
    let (function, _) = deploy_with(&h, &h.a, "quiet", 1, 5, 4).await;
    let mut invocations = Vec::new();
    for _ in 0..3 {
        let out = h
            .app
            .invoke
            .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
            .await
            .unwrap();
        // The guest did receive the value, so its absence below is meaningful.
        let env_id = &out.detail.attempts[0].0.environment_id;
        let Some(HostMessage::HelloAck { env, .. }) = h.fake.hello_ack(env_id) else {
            panic!("guest did not receive HelloAck");
        };
        assert!(env.iter().any(|(_, v)| v == "demo-secret-value-a"));
        invocations.push(out.invocation().id.clone());
    }

    // The driver keeps running after `invoke` returns (terminate, usage, final
    // log line), so a loaded machine can reach this point before the pipeline
    // has logged anything. Wait for the marker instead of racing it, otherwise
    // the secret-absence assertions below could pass on an empty capture.
    let mut text = captured.text();
    for _ in 0..100 {
        if text.contains("invocation finished") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        text = captured.text();
    }
    assert!(
        text.contains("invocation finished"),
        "the subscriber captured the pipeline"
    );
    assert!(
        !text.contains("demo-secret-value-a"),
        "secret value found in host logs"
    );
    for id in &invocations {
        let logs = h.app.logs.for_invocation(&h.a, id).unwrap();
        assert!(!logs.records.is_empty());
        assert!(
            logs.records
                .iter()
                .all(|r| !r.line.contains("demo-secret-value-a"))
        );
    }
}

// ---------------------------------------------------------------------------
// undelivered Invoke
// ---------------------------------------------------------------------------

#[tokio::test]
async fn guest_exit_before_the_invoke_is_delivered_is_a_crash_not_outcome_unknown() {
    let h = harness(vec![FakeGuestScript::ExitAfterReady; 3], "");
    let (function, _) = deploy(&h, &h.a, "exits").await;
    for _ in 0..3 {
        let out = h
            .app
            .invoke
            .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
            .await
            .unwrap();
        let error = failed_error(&out);
        assert_eq!(error.class, ErrorClass::Crash);
        assert!(
            matches!(
                error.error_type.as_str(),
                "Runtime.Exited" | "Host.BridgeDisconnectedBeforeInvoke"
            ),
            "{}",
            error.error_type
        );
        assert_eq!(out.error().unwrap().http_status(), 502);
        let (attempt, _) = &out.detail.attempts[0];
        assert!(
            matches!(attempt.status, AttemptStatus::Failed { .. }),
            "{:?}",
            attempt.status
        );
        assert!(
            !h.fake
                .host_messages(&attempt.environment_id)
                .iter()
                .any(|m| matches!(m, HostMessage::Invoke { .. }))
        );
    }
    let terminated = h.fake.terminated();
    assert_eq!(terminated.len(), 3);
    assert!(
        terminated
            .iter()
            .all(|(_, r)| *r == TerminateReason::Crashed)
    );
    assert!(h.fake.running().is_empty());
}

// ---------------------------------------------------------------------------
// driver panic
// ---------------------------------------------------------------------------

/// A bridge stream whose first read panics.
struct PanickingStream;

impl AsyncRead for PanickingStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        panic!("injected failure after the environment was created")
    }
}

impl AsyncWrite for PanickingStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Delegates to the fake provider but hands the driver a stream that panics,
/// i.e. the driver panics after `create_environment` returned.
struct PanicAfterCreate(Arc<FakeExecutionProvider>);

#[async_trait]
impl ExecutionProvider for PanicAfterCreate {
    fn kind(&self) -> ProviderKind {
        self.0.kind()
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }
    async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
        self.0.preflight().await
    }
    async fn validate_artifact(
        &self,
        artifact: &ArtifactLocation,
        architecture: Architecture,
    ) -> Result<(), ProviderError> {
        self.0.validate_artifact(artifact, architecture).await
    }
    async fn create_environment(
        &self,
        spec: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError> {
        let mut handle = self.0.create_environment(spec).await?;
        handle.stream = Box::new(PanickingStream);
        Ok(handle)
    }
    async fn terminate_environment(
        &self,
        environment_id: &EnvironmentId,
        reason: TerminateReason,
    ) -> Result<TerminateReport, ProviderError> {
        self.0.terminate_environment(environment_id, reason).await
    }
    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        self.0.observe_environment(environment_id).await
    }
    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
        self.0.list_environments().await
    }
}

#[tokio::test]
async fn driver_panic_after_environment_creation_still_terminates_it() {
    let fake = Arc::new(FakeExecutionProvider::new());
    let provider = Arc::new(PanicAfterCreate(fake.clone()));
    let h = harness_with(fake, provider, "", None);
    let (function, _) = deploy(&h, &h.a, "panics").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    let error = failed_error(&out);
    assert_eq!(error.class, ErrorClass::PlatformError);
    assert_eq!(error.error_type, "Host.DriverPanic");

    let created = h.fake.created();
    assert_eq!(created.len(), 1);
    assert_eq!(
        h.fake.terminated(),
        vec![(created[0].clone(), TerminateReason::Crashed)]
    );
    assert!(h.fake.running().is_empty());
    let env = h.app.repos.environments.get(&created[0]).unwrap().unwrap();
    assert!(
        matches!(env.state, EnvironmentState::Failed { .. }),
        "{:?}",
        env.state
    );
    assert!(h.app.repos.environments.list_active().unwrap().is_empty());
    assert_eq!(h.app.invoke.in_flight_count(), 0);
    let usage = h.app.usage.events_for_invocation(&out.invocation().id);
    let started = usage
        .iter()
        .find(|e| matches!(e.event_type, UsageEventType::EnvironmentStarted))
        .expect("EnvironmentStarted recorded");
    let stopped = usage
        .iter()
        .find(|e| matches!(e.event_type, UsageEventType::EnvironmentStopped))
        .expect("EnvironmentStopped recorded after the panic");
    assert_ne!(started.event_id, stopped.event_id);
    assert!(stopped.sequence > started.sequence);
}

// ---------------------------------------------------------------------------
// client deadline (docs/threat-model.md §8)
// ---------------------------------------------------------------------------

/// A wall clock that a guest script can move forward.
struct ShiftedClock(Arc<AtomicI64>);

impl Clock for ShiftedClock {
    fn now(&self) -> Timestamp {
        chrono::Utc::now() + chrono::Duration::milliseconds(self.0.load(Ordering::SeqCst))
    }
}

/// Custom guest: handshake, wait `delay`, run `before_ready`, send `Ready`,
/// then count the `Invoke` frames it receives until the host closes.
fn scripted_ready(
    delay: Duration,
    before_ready: Arc<dyn Fn() + Send + Sync>,
    invokes: Arc<AtomicUsize>,
) -> FakeGuestScript {
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        let before_ready = before_ready.clone();
        let invokes = invokes.clone();
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "test-guest".into(),
                environment_id: ctx.environment_id.to_string(),
                guest_boot_id: Some("test-boot".into()),
                architecture: "aarch64".into(),
            };
            if writer.send(encode_message(&hello).unwrap()).await.is_err() {
                return;
            }
            if !matches!(reader.next().await, Some(Ok(_))) {
                return;
            }
            tokio::time::sleep(delay).await;
            before_ready();
            let ready = encode_message(&GuestMessage::Ready { init_ms: 1 }).unwrap();
            if writer.send(ready).await.is_err() {
                return;
            }
            while let Some(Ok(frame)) = reader.next().await {
                if matches!(
                    decode_message::<HostMessage>(&frame),
                    Ok(HostMessage::Invoke { .. })
                ) {
                    invokes.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
    }))
}

fn assert_stopped_before_dispatch(h: &Harness, out: &InvokeOutcome) {
    let error = failed_error(out);
    assert_eq!(error.class, ErrorClass::Timeout);
    assert_eq!(error.error_type, "Host.ClientDeadline");
    assert_eq!(out.error().unwrap().http_status(), 504);
    assert!(out.detail.attempts.is_empty(), "no attempt was dispatched");
    let created = h.fake.created();
    assert_eq!(created.len(), 1);
    assert_eq!(
        h.fake.terminated(),
        vec![(created[0].clone(), TerminateReason::Cancelled)]
    );
    let env = h.app.repos.environments.get(&created[0]).unwrap().unwrap();
    assert_eq!(env.state, EnvironmentState::Stopped);
    let usage = h.app.usage.events_for_invocation(&out.invocation().id);
    assert!(
        !usage
            .iter()
            .any(|e| matches!(e.event_type, UsageEventType::HandlerStarted))
    );
    assert_eq!(h.app.invoke.in_flight_count(), 0);
}

#[tokio::test]
async fn client_deadline_during_initialization_never_starts_the_handler() {
    let invokes = Arc::new(AtomicUsize::new(0));
    let h = harness(
        vec![scripted_ready(
            Duration::from_millis(1500),
            Arc::new(|| {}),
            invokes.clone(),
        )],
        "",
    );
    let (function, _) = deploy(&h, &h.a, "slow-init").await;
    let mut req = invoke_request(&h.a, &function, serde_json::json!({}));
    req.client_timeout_ms = Some(400);
    let started = Instant::now();
    let out = h.app.invoke.invoke(req).await.unwrap();
    let elapsed = started.elapsed();
    assert_stopped_before_dispatch(&h, &out);
    assert!(
        elapsed < Duration::from_millis(1400),
        "answered at the client deadline, not after init: {elapsed:?}"
    );
    assert_eq!(invokes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn client_deadline_is_checked_again_right_before_dispatch() {
    let offset = Arc::new(AtomicI64::new(0));
    let invokes = Arc::new(AtomicUsize::new(0));
    // The wall clock jumps past the client deadline just before Ready.
    let jump: Arc<dyn Fn() + Send + Sync> = {
        let offset = offset.clone();
        Arc::new(move || offset.store(120_000, Ordering::SeqCst))
    };
    let fake = Arc::new(FakeExecutionProvider::with_scripts([scripted_ready(
        Duration::ZERO,
        jump,
        invokes.clone(),
    )]));
    let clock: Arc<dyn Clock> = Arc::new(ShiftedClock(offset));
    let h = harness_with(fake.clone(), fake, "", Some(clock));
    let (function, _) = deploy(&h, &h.a, "late-ready").await;
    let mut req = invoke_request(&h.a, &function, serde_json::json!({}));
    req.client_timeout_ms = Some(10_000);
    let out = h.app.invoke.invoke(req).await.unwrap();
    assert_stopped_before_dispatch(&h, &out);
    assert_eq!(
        invokes.load(Ordering::SeqCst),
        0,
        "the guest never receives an Invoke"
    );
}

#[tokio::test]
async fn client_deadline_clamps_the_execution_deadline() {
    let h = harness(vec![FakeGuestScript::HangForever], "");
    let (function, _) = deploy_with(&h, &h.a, "clamped", 30, 5, 4).await;
    let mut req = invoke_request(&h.a, &function, serde_json::json!({}));
    req.client_timeout_ms = Some(800);
    let started = Instant::now();
    let out = h.app.invoke.invoke(req).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let error = failed_error(&out);
    assert_eq!(error.class, ErrorClass::Timeout);
    assert_eq!(error.error_type, "Host.ClientDeadline");
    assert_eq!(out.error().unwrap().http_status(), 504);

    let deadlines = out.invocation().deadlines;
    assert_eq!(
        deadlines.execution_deadline,
        Some(deadlines.client_deadline)
    );
    assert!(deadlines.queue_deadline <= deadlines.client_deadline);
    assert!(deadlines.init_deadline.unwrap() <= deadlines.client_deadline);

    let (attempt, _) = &out.detail.attempts[0];
    let msgs = h.fake.host_messages(&attempt.environment_id);
    let guest_deadline = msgs
        .iter()
        .find_map(|m| match m {
            HostMessage::Invoke { deadline_ms, .. } => Some(*deadline_ms),
            _ => None,
        })
        .expect("the invoke was delivered");
    assert_eq!(
        guest_deadline,
        deadlines.client_deadline.timestamp_millis() as u64,
        "the guest observes the clamped deadline"
    );
    assert!(msgs.iter().any(|m| matches!(m, HostMessage::Cancel { .. })));
    assert_eq!(
        h.fake.terminated(),
        vec![(attempt.environment_id.clone(), TerminateReason::Timeout)]
    );
}

#[tokio::test]
async fn client_deadline_bounds_the_queue_wait() {
    let h = harness(
        vec![FakeGuestScript::HangForever],
        "[capacity]\nmax_concurrency = 1\nmax_queue = 4\nqueue_timeout_seconds = 10\n",
    );
    let (function, _) = deploy_with(&h, &h.a, "queue-bound", 30, 5, 4).await;
    let app = h.app.clone();
    let hanging = tokio::spawn({
        let req = invoke_request(&h.a, &function, serde_json::json!({}));
        let app = app.clone();
        async move { app.invoke.invoke(req).await }
    });
    wait_until("the first invocation to run", || {
        with_status(&h, &function, InvocationStatus::Running).is_some()
    })
    .await;

    let mut req = invoke_request(&h.a, &function, serde_json::json!({}));
    req.client_timeout_ms = Some(500);
    let started = Instant::now();
    let out = app.invoke.invoke(req).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(failed_error(&out).class, ErrorClass::QueueTimeout);
    let deadlines = out.invocation().deadlines;
    assert_eq!(deadlines.queue_deadline, deadlines.client_deadline);
    assert!(out.detail.attempts.is_empty());

    let running = with_status(&h, &function, InvocationStatus::Running).unwrap();
    app.invoke.cancel(&h.a, &running).await.unwrap();
    hanging.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// startup reconcile (docs/architecture.md §4, docs/threat-model.md §12 T10)
// ---------------------------------------------------------------------------

/// Spec for an environment created behind the gateway's back, the way a
/// process that died before terminating its environment would have left it.
fn orphan_spec(id: &EnvironmentId) -> EnvironmentSpec {
    EnvironmentSpec {
        environment_id: id.clone(),
        tenant_id: TenantId::parse(TENANT_A).unwrap(),
        revision_id: RevisionId::generate(),
        artifact: ArtifactLocation {
            path: "/nonexistent".into(),
            digest: Sha256Digest::of_bytes(b"orphan"),
            size_bytes: 1,
        },
        architecture: Architecture::Aarch64,
        resources: ResourceProfile::default(),
        egress: EgressProfile::None,
        connect_timeout: Duration::from_secs(1),
    }
}

/// A ledger row for an environment this gateway considers active.
fn ledger_environment(id: &EnvironmentId, now: Timestamp) -> ExecutionEnvironment {
    let tenant = TenantId::parse(TENANT_A).unwrap();
    let revision = RevisionId::generate();
    ExecutionEnvironment::request(
        id.clone(),
        tenant.clone(),
        revision.clone(),
        ProviderKind::Fake,
        ReuseKey {
            tenant_id: tenant,
            revision_id: revision,
            execution_role_version: 1,
            configuration_version: 1,
            resource_profile_digest: "d".into(),
            runtime_profile: "default".into(),
            network_policy_version: 1,
            secret_binding_generation: 1,
        },
        now,
    )
}

#[tokio::test]
async fn startup_reconcile_terminates_orphans_and_spares_live_environments() {
    let h = harness(vec![], "");
    let now = h.app.clock.now();
    // Left behind by a previous process: the provider still runs it, this
    // gateway never heard of it.
    let orphan = EnvironmentId::generate();
    h.fake
        .create_environment(orphan_spec(&orphan))
        .await
        .unwrap();
    // An environment of a live invocation: it is recorded in the ledger
    // before the provider creates it, so reconcile must leave it alone.
    let live = EnvironmentId::generate();
    h.app
        .repos
        .environments
        .insert(ledger_environment(&live, now))
        .unwrap();
    h.fake.create_environment(orphan_spec(&live)).await.unwrap();

    let report = h
        .app
        .reconcile_on_startup()
        .await
        .expect("reconcile is on by default");
    assert_eq!(report.found, 2);
    assert_eq!(report.adopted, 1);
    assert_eq!(report.terminated, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.lost, 0);
    assert_eq!(report.error, None);
    assert_eq!(
        h.fake.terminated(),
        vec![(orphan, TerminateReason::Reconcile)],
        "only the orphan is reclaimed"
    );
    assert_eq!(h.fake.running(), vec![live.clone()]);
    let live_env = h.app.repos.environments.get(&live).unwrap().unwrap();
    assert!(
        !live_env.is_terminal(),
        "a live environment is untouched: {:?}",
        live_env.state
    );
    assert_eq!(h.app.reconcile.last_report(), Some(report));
}

#[tokio::test]
async fn startup_reconcile_marks_environments_the_provider_no_longer_has() {
    let h = harness(vec![], "");
    let vanished = EnvironmentId::generate();
    h.app
        .repos
        .environments
        .insert(ledger_environment(&vanished, h.app.clock.now()))
        .unwrap();

    let report = h.app.reconcile_on_startup().await.unwrap();
    assert_eq!((report.found, report.terminated, report.lost), (0, 0, 1));
    let env = h.app.repos.environments.get(&vanished).unwrap().unwrap();
    assert!(
        matches!(env.state, EnvironmentState::Lost { .. }),
        "{:?}",
        env.state
    );
    assert!(h.app.repos.environments.list_active().unwrap().is_empty());
    assert!(
        h.fake.terminated().is_empty(),
        "there is nothing on the host to terminate"
    );
}

/// Delegates to the fake provider but cannot be listed, like a provider whose
/// host state is unreadable at startup.
struct UnlistableProvider(Arc<FakeExecutionProvider>);

#[async_trait]
impl ExecutionProvider for UnlistableProvider {
    fn kind(&self) -> ProviderKind {
        self.0.kind()
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }
    async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
        self.0.preflight().await
    }
    async fn validate_artifact(
        &self,
        artifact: &ArtifactLocation,
        architecture: Architecture,
    ) -> Result<(), ProviderError> {
        self.0.validate_artifact(artifact, architecture).await
    }
    async fn create_environment(
        &self,
        spec: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError> {
        self.0.create_environment(spec).await
    }
    async fn terminate_environment(
        &self,
        environment_id: &EnvironmentId,
        reason: TerminateReason,
    ) -> Result<TerminateReport, ProviderError> {
        self.0.terminate_environment(environment_id, reason).await
    }
    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        self.0.observe_environment(environment_id).await
    }
    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
        Err(ProviderError::Unavailable(
            "cannot read the provider workdir".into(),
        ))
    }
}

#[tokio::test]
async fn startup_reconcile_never_blocks_startup_on_a_provider_error() {
    let fake = Arc::new(FakeExecutionProvider::new());
    let provider = Arc::new(UnlistableProvider(fake.clone()));
    let h = harness_with(fake, provider, "", None);
    let live = EnvironmentId::generate();
    h.app
        .repos
        .environments
        .insert(ledger_environment(&live, h.app.clock.now()))
        .unwrap();

    let report = h.app.reconcile_on_startup().await.unwrap();
    assert!(
        report.error.is_some(),
        "the provider failure is recorded, not raised"
    );
    assert_eq!(
        (report.found, report.terminated, report.failed, report.lost),
        (0, 0, 0, 0)
    );
    assert!(
        !h.app
            .repos
            .environments
            .get(&live)
            .unwrap()
            .unwrap()
            .is_terminal(),
        "a provider that cannot be listed proves nothing about the ledger"
    );
    // Startup continued: the gateway still serves invocations.
    let (function, _) = deploy(&h, &h.a, "after-reconcile").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h.a, &function, serde_json::json!({})))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
}

#[tokio::test]
async fn startup_reconcile_can_be_turned_off() {
    let h = harness(vec![], "[reconcile]\non_startup = false\n");
    let orphan = EnvironmentId::generate();
    h.fake
        .create_environment(orphan_spec(&orphan))
        .await
        .unwrap();

    assert!(h.app.reconcile_on_startup().await.is_none());
    assert!(h.app.reconcile.last_report().is_none());
    assert!(h.fake.terminated().is_empty());
    assert_eq!(h.fake.running(), vec![orphan]);
}
