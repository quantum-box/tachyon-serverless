//! A gateway frozen inside a `state.db` write transaction (PLT-4646,
//! docs/adr/0003 「書込み transaction の規律」「store が止まった間の lease」,
//! docs/failure-matrix.md §8 `stale_owner_sync_lease`).
//!
//! Two applications share one `data_dir`. Gateway A is frozen *inside* a
//! write transaction (a store hook that blocks, standing in for a SIGSTOP that
//! lands between `BEGIN IMMEDIATE` and `COMMIT`). Nothing can take that lock
//! back, so gateway B's writes fail — within a bounded time, never queued —
//! for as long as A stays frozen, and both leases pass meanwhile.
//!
//! What must still hold when A wakes up: the gateway that kept running (B)
//! renews the lease nobody reclaimed, keeps its in-flight work and reclaims
//! A; the gateway that was frozen (A) is fenced by its own heartbeat and
//! reclaims nobody.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_util::codec::{FramedRead, FramedWrite};

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_application::repository::{HeartbeatOutcome, RepoError};
use tachyon_serverless_application::{Application, BootstrapOptions, GatewayConfig, InvokeRequest};
use tachyon_serverless_domain::{
    AliasName, Clock, EventKind, FixedClock, Function, InvocationId, InvocationStatus,
    RevisionStatus, TenantId, Timestamp,
};
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message, encode_message,
};
use tachyon_serverless_provider_fake::{
    CustomScriptContext, FakeExecutionProvider, FakeGuestScript, ScriptFuture,
};
use tachyon_serverless_provider_port::{Principal, Role};

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

fn secs(n: i64) -> Timestamp {
    use chrono::TimeZone;
    chrono::Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap() + chrono::Duration::seconds(n)
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
                    timeout_seconds: 120,
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

fn request(function: &Function) -> InvokeRequest {
    InvokeRequest {
        principal: principal(),
        function_id: function.id.clone(),
        alias: None,
        revision_id: None,
        event_kind: EventKind::Json,
        payload: serde_json::json!({}),
        idempotency_key: None,
        client_timeout_ms: Some(120_000),
        trace_id: None,
    }
}

/// Answers its `Invoke` once `gate` opens.
fn gated(gate: watch::Receiver<bool>) -> FakeGuestScript {
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        let mut gate = gate.clone();
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "store-stall-test".into(),
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
                        payload: serde_json::json!({"kept": true}),
                        handler_ms: Some(1),
                    };
                    let _ = writer.send(encode_message(&response).unwrap()).await;
                    return;
                }
            }
        })
    }))
}

async fn wait_running(app: &Application, function: &Function) -> InvocationId {
    for _ in 0..1000 {
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

/// A frozen-in-a-transaction gateway (A) stalls every writer of the shared
/// `state.db`; the other gateway (B) is refused within a bounded time instead
/// of hanging, is not fenced for it, and — once A resumes — renews the lease
/// nobody reclaimed, finishes its in-flight invocation and reclaims A, while A
/// fences itself and reclaims nobody.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_gateway_frozen_inside_a_write_transaction_does_not_cost_the_other_its_lease() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let clock_a = Arc::new(FixedClock::new(secs(0)));
    let clock_b = Arc::new(FixedClock::new(secs(0)));
    let a = start(dir.path(), "gateway-a", &fake, clock_a.clone());
    let b = start(dir.path(), "gateway-b", &fake, clock_b.clone());
    let function = deploy(&b, "kept").await;

    // B drives an invocation with a slot lease through the whole outage.
    let (open, gate) = watch::channel(false);
    fake.push_script(gated(gate));
    let running = {
        let b = b.clone();
        let req = request(&function);
        tokio::spawn(async move { b.invoke.invoke(req).await })
    };
    let inv = wait_running(&b, &function).await;
    assert!(matches!(
        b.dispatcher.heartbeat().unwrap(),
        HeartbeatOutcome::Renewed { leases: 1 }
    ));
    assert!(matches!(
        a.dispatcher.heartbeat().unwrap(),
        HeartbeatOutcome::Renewed { leases: 0 }
    ));

    // Freeze A between BEGIN IMMEDIATE and COMMIT of its next write.
    let frozen = Arc::new(AtomicBool::new(true));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<String>();
    let entered_tx = std::sync::Mutex::new(entered_tx);
    {
        let frozen = frozen.clone();
        a.store.set_write_hook(Some(Arc::new(move |site| {
            let _ = entered_tx
                .lock()
                .unwrap()
                .send(format!("{}:{}", site.file(), site.line()));
            while frozen.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(10));
            }
        })));
    }
    let a_writer = {
        let a = a.clone();
        std::thread::spawn(move || a.dispatcher.reclaim_ledger())
    };
    let site = entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(site.contains("slot.rs"), "frozen in {site}");

    // B cannot write while A is frozen: refused within the bound, not fenced.
    // A's own heartbeat waits behind its frozen writer the same way.
    clock_b.set(secs(5));
    clock_a.set(secs(5));
    let b_attempt = {
        let b = b.clone();
        std::thread::spawn(move || {
            let t = Instant::now();
            (b.dispatcher.heartbeat(), t.elapsed())
        })
    };
    let a_attempt = {
        let a = a.clone();
        std::thread::spawn(move || a.dispatcher.heartbeat())
    };
    let (b_result, b_waited) = b_attempt.join().unwrap();
    assert!(
        matches!(b_result, Err(RepoError::Store(_))),
        "the store is unavailable to B: {b_result:?}"
    );
    assert!(b_waited >= Duration::from_secs(4), "B waited {b_waited:?}");
    assert!(b_waited <= Duration::from_secs(12), "bounded: {b_waited:?}");
    assert!(
        !b.dispatcher.is_fenced(),
        "a store outage alone never fences"
    );
    assert!(matches!(
        a_attempt.join().unwrap(),
        Err(RepoError::Store(_))
    ));

    // Both leases (30 s) pass while A stays frozen. B keeps trying on its
    // timer; A, being frozen, does not.
    clock_b.set(secs(45));
    clock_a.set(secs(120));
    frozen.store(false, Ordering::SeqCst);
    a.store.set_write_hook(None);
    assert!(a_writer.join().unwrap().unwrap().is_empty());

    // A wakes up. A real SIGSTOP is noticed by the process-wide stall
    // watchdog; both gateways live in this test process, so the stall is
    // recorded for A alone. Its passed lease is refused (no take-back for a
    // process that was not running) and it reclaims nobody.
    a.dispatcher.note_stalled();
    assert_eq!(a.dispatcher.heartbeat().unwrap(), HeartbeatOutcome::Fenced);
    assert!(a.dispatcher.is_fenced());
    let a_reclaim = a.reclaim_expired().await;
    assert_eq!(a_reclaim.dispatchers, 0, "{a_reclaim:?}");
    assert_eq!(
        b.repos
            .slots
            .get_dispatcher(b.dispatcher.id())
            .unwrap()
            .unwrap()
            .reclaimed_at,
        None
    );

    // B, which kept trying, takes back what nobody reclaimed: its lease and
    // the slot lease of its running invocation.
    assert_eq!(
        b.dispatcher.heartbeat().unwrap(),
        HeartbeatOutcome::Renewed { leases: 1 }
    );
    assert!(!b.dispatcher.is_fenced());
    let b_record = b
        .repos
        .slots
        .get_dispatcher(b.dispatcher.id())
        .unwrap()
        .unwrap();
    assert_eq!(b_record.lease_expires_at, secs(75));
    // And reclaims A.
    let b_reclaim = b.dispatcher.reclaim_ledger().unwrap();
    assert_eq!(b_reclaim.dispatchers, vec![a.dispatcher.id().clone()]);

    // B's invocation, dispatched before the outage, completes normally.
    open.send_replace(true);
    let out = running.await.unwrap().unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    assert_eq!(out.output, Some(serde_json::json!({"kept": true})));
    assert_eq!(
        b.repos.invocations.get(&inv).unwrap().unwrap().status,
        InvocationStatus::Succeeded
    );

    // A frozen transaction is reported with the time it held the lock.
    let stats = a.store.write_transaction_stats();
    assert!(stats.slow >= 1, "{stats:?}");
    assert!(stats.max_held_ms >= 4_000, "{stats:?}");
}
