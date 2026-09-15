//! End-to-end tests of the application layer against the fake provider.

use std::sync::Arc;
use std::time::Duration;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
    SecretBindingRequest,
};
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, AttemptStatus, EnvironmentState, ErrorClass, EventKind, Function, FunctionRevision,
    InvocationStatus, RevisionStatus, TenantId,
};
use tachyon_serverless_protocol::HostMessage;
use tachyon_serverless_provider_fake::{FakeExecutionProvider, FakeGuestScript};
use tachyon_serverless_provider_port::{ExecutionProvider, Principal, Role, TerminateReason};

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
    _dir: tempfile::TempDir,
}

fn principal(tenant: &str, roles: Vec<Role>) -> Principal {
    Principal {
        subject: "t".into(),
        tenant_id: TenantId::parse(tenant).unwrap(),
        roles,
    }
}

fn harness(scripts: Vec<FakeGuestScript>, extra: &str) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let config = GatewayConfig::from_toml(&config_toml(dir.path(), "dev", extra)).unwrap();
    let fake = Arc::new(FakeExecutionProvider::with_scripts(scripts));
    let app = Application::bootstrap_with(
        config,
        fake.clone(),
        BootstrapOptions {
            persist_state: true,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    Harness {
        app,
        fake,
        a: principal(TENANT_A, vec![Role::Deploy, Role::Invoke]),
        b: principal(TENANT_B, vec![Role::Deploy, Role::Invoke]),
        _dir: dir,
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
    let artifact = h
        .app
        .artifacts
        .put(format!("#!/bin/sh\necho {name}\n").as_bytes())
        .await
        .unwrap();
    let function = h.app.functions.create(principal, name, "").unwrap();
    let rev = h
        .app
        .revisions
        .create(
            principal,
            &function.id,
            &revision_request(
                artifact.digest.as_str(),
                timeout,
                init_timeout,
                max_concurrency,
            ),
        )
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
    let artifact = h.app.artifacts.put(b"#!/bin/sh\necho v2\n").await.unwrap();
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
    let artifact = app.artifacts.put(b"#!/bin/sh\necho v2\n").await.unwrap();
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
