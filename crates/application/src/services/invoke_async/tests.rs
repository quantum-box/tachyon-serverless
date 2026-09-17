//! Asynchronous acceptance and the transactional outbox (PLT-4639): every
//! crash window of docs/adr/0010 is driven through a failpoint and followed by
//! a restart (a new `Application` on the same `data_dir`), and convergence is
//! checked on the queue itself: every accepted invocation is delivered, and
//! logically exactly once.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ErrorCode, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_domain::{
    AliasName, Clock, FixedClock, Function, FunctionRevision, InvocationId, InvocationMode,
    InvocationStatus, RevisionStatus, TenantId,
};
use tachyon_serverless_durable_port::{ConsumerName, ConsumerSpec, Delivery, MessageId, Topic};
use tachyon_serverless_provider_fake::FakeExecutionProvider;
use tachyon_serverless_provider_port::{Principal, Role};

use super::*;
use crate::failpoints::Action;
use crate::repository::{AsyncInputBody, CollectDecision, CollectReason};
use crate::{Application, BootstrapOptions, GatewayConfig, InvokeRequest};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

struct Knobs {
    inline_max: u64,
    max_pending: u64,
    max_object_bytes: u64,
    quota: u64,
    objects: bool,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            inline_max: 256,
            max_pending: 100,
            max_object_bytes: 64 * 1024,
            quota: 1024 * 1024,
            objects: true,
        }
    }
}

fn config(dir: &Path, k: &Knobs) -> GatewayConfig {
    let key = dir.join("objects.key");
    if !key.exists() {
        std::fs::write(&key, "42".repeat(32)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    let objects = if k.objects {
        format!(
            r#"
[objects]
backend = "filesystem"
key_file = "{key}"
max_object_bytes = {max}
tenant_quota_bytes = {quota}
orphan_grace_seconds = 60
"#,
            key = key.display(),
            max = k.max_object_bytes,
            quota = k.quota,
        )
    } else {
        String::new()
    };
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
token = "tok-a"
tenant_id = "{TENANT_A}"
subject = "a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "tok-b"
tenant_id = "{TENANT_B}"
subject = "b"
roles = ["deploy", "invoke"]

[queue]
backend = "sqlite"

[invoke_async]
inline_input_max_bytes = {inline}
max_pending_events = {pending}
max_pending_age_seconds = 300
claim_ttl_seconds = 30
retry_initial_ms = 1000
retry_max_ms = 4000
{objects}
"#,
        data = dir.display(),
        inline = k.inline_max,
        pending = k.max_pending,
    ))
    .unwrap()
}

fn principal(tenant: &str) -> Principal {
    Principal {
        subject: "t".into(),
        tenant_id: TenantId::parse(tenant).unwrap(),
        roles: vec![Role::Deploy, Role::Invoke],
    }
}

struct Env {
    dir: tempfile::TempDir,
    clock: Arc<FixedClock>,
    fake: Arc<FakeExecutionProvider>,
    knobs: Knobs,
}

impl Env {
    fn new(knobs: Knobs) -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            // Object ids carry the real time; the clock starts there.
            clock: Arc::new(FixedClock::new(chrono::Utc::now())),
            fake: Arc::new(FakeExecutionProvider::new()),
            knobs,
        }
    }

    /// A gateway process on this data_dir (a restart is a second call after
    /// dropping the first application).
    fn start(&self) -> Arc<Application> {
        Application::bootstrap_with(
            config(self.dir.path(), &self.knobs),
            self.fake.clone(),
            BootstrapOptions {
                clock: self.clock.clone(),
                ..BootstrapOptions::default()
            },
        )
        .unwrap()
    }

    fn advance(&self, seconds: i64) {
        self.clock.advance(chrono::Duration::seconds(seconds));
    }
}

async fn revision(app: &Application, function: &Function, tag: &str) -> FunctionRevision {
    let p = principal(function.tenant_id.as_str());
    let artifact = app
        .artifact_service
        .upload(&p, format!("#!/bin/sh\necho {tag}\n").as_bytes())
        .await
        .unwrap();
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
                    min_ready: 0,
                    idle_ttl_seconds: None,
                    scale_down_cooldown_seconds: None,
                },
                egress: None,
                egress_allow: Vec::new(),
                env_vars: vec![],
                secrets: vec![],
                description: String::new(),
                publish_to_prod: true,
                required_region: None,
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
    rev
}

async fn deploy(app: &Application, tenant: &str, name: &str) -> (Function, FunctionRevision) {
    let function = app.functions.create(&principal(tenant), name, "").unwrap();
    let rev = revision(app, &function, name).await;
    (function, rev)
}

fn request(
    function: &Function,
    payload: serde_json::Value,
    key: Option<&str>,
) -> InvokeAsyncRequest {
    InvokeAsyncRequest {
        principal: principal(function.tenant_id.as_str()),
        function_id: function.id.clone(),
        alias: None,
        revision_id: None,
        payload,
        idempotency_key: key.map(str::to_string),
        trace_id: None,
    }
}

fn small(n: usize) -> serde_json::Value {
    serde_json::json!({ "n": n })
}

fn large(n: usize, bytes: usize) -> serde_json::Value {
    serde_json::json!({ "n": n, "blob": "x".repeat(bytes) })
}

async fn accept(app: &Application, req: InvokeAsyncRequest) -> Result<AsyncAcceptance, AppError> {
    app.invoke_async.as_ref().unwrap().accept(req).await
}

fn consumer() -> ConsumerName {
    ConsumerName::parse("dispatcher").unwrap()
}

/// Every message now in the queue, acked.
async fn drain(app: &Application) -> Vec<Delivery> {
    let q = app.durable.queue.as_ref().unwrap();
    q.ensure_consumer(&ConsumerSpec {
        name: consumer(),
        topic: Topic::parse(INVOKE_TOPIC).unwrap(),
        ack_wait: Duration::from_secs(600),
        max_deliver: 10,
    })
    .await
    .unwrap();
    let mut out = Vec::new();
    loop {
        let batch = q
            .fetch(&consumer(), 100, Duration::from_millis(10))
            .await
            .unwrap();
        if batch.is_empty() {
            return out;
        }
        for d in batch {
            q.ack(&d.token).await.unwrap();
            out.push(d);
        }
    }
}

fn ids(deliveries: &[Delivery]) -> BTreeSet<String> {
    deliveries
        .iter()
        .map(|d| d.message_id.to_string())
        .collect()
}

fn inv(app: &Application, id: &InvocationId) -> Invocation {
    app.repos.invocations.get(id).unwrap().unwrap()
}

fn ledger(app: &Application) -> &Arc<dyn AsyncInvocationRepository> {
    app.async_ledger.as_ref().unwrap()
}

fn refusal(e: &AppError) -> Option<&'static str> {
    match e {
        AppError::AsyncRefused { reason, .. } => Some(reason.as_str()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// acceptance
// ---------------------------------------------------------------------------

/// DB commit before 202, no dual write: after acceptance the ledger holds the
/// invocation, its input and its event, and the queue holds nothing until the
/// publisher runs.
#[tokio::test]
async fn acceptance_commits_invocation_input_and_event_and_never_publishes_in_the_request() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, rev) = deploy(&app, TENANT_A, "acc").await;
    let a = accept(&app, request(&f, small(1), None)).await.unwrap();
    assert!(!a.replayed);
    assert_eq!(a.input_storage, "inline");
    let stored = inv(&app, &a.invocation.id);
    assert_eq!(stored.mode, InvocationMode::Async);
    assert_eq!(stored.status, InvocationStatus::Accepted);
    assert_eq!(stored.revision_id, rev.id);
    assert!(stored.dispatcher_id.is_none());
    let input = ledger(&app).async_input(&stored.id).unwrap().unwrap();
    assert!(matches!(input.body, AsyncInputBody::Inline(ref b) if b == br#"{"n":1}"#));
    let event = ledger(&app).outbox_event(&stored.id).unwrap().unwrap();
    assert!(event.sent_at.is_none());
    let envelope: InvokeEnvelope = serde_json::from_str(&event.payload).unwrap();
    assert_eq!(envelope.invocation_id, stored.id);
    assert!(
        !event.payload.contains(r#""n":1"#),
        "the envelope never carries the input body"
    );
    assert!(
        drain(&app).await.is_empty(),
        "nothing published before the outbox runs"
    );

    let report = app.publish_outbox().await.unwrap();
    assert_eq!((report.claimed, report.published, report.marked), (1, 1, 1));
    assert_eq!(inv(&app, &stored.id).status, InvocationStatus::Queued);
    let got = drain(&app).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message_id.as_str(), stored.id.as_str());
    let read = app.read_async_delivery(&got[0]).await.unwrap();
    assert_eq!(read.input, br#"{"n":1}"#);
    assert_eq!(read.envelope.revision_id, rev.id);
    // Nothing left to publish.
    assert_eq!(app.publish_outbox().await.unwrap().claimed, 0);
}

#[tokio::test]
async fn large_inputs_are_stored_as_objects_referenced_in_the_same_transaction() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "big").await;
    let a = accept(&app, request(&f, large(1, 4000), None))
        .await
        .unwrap();
    assert_eq!(a.input_storage, "object");
    let input = ledger(&app).async_input(&a.invocation.id).unwrap().unwrap();
    let AsyncInputBody::Object(reference) = input.body else {
        panic!("object input expected");
    };
    assert_eq!(
        app.store.object_references(&reference.id).unwrap(),
        vec![a.invocation.id.clone()]
    );
    // Past its orphan grace, a referenced input of a waiting invocation stays.
    env.advance(3600 * 24 * 8);
    let gc = app.collect_objects().await.unwrap();
    assert_eq!(gc.kept_in_use, 1, "{gc:?}");
    app.publish_outbox().await.unwrap();
    let got = drain(&app).await;
    let read = app.read_async_delivery(&got[0]).await.unwrap();
    assert_eq!(read.input.len() as u64, a.invocation.input_size_bytes);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&read.input).unwrap(),
        large(1, 4000)
    );
}

// ---------------------------------------------------------------------------
// transaction-boundary failpoints
// ---------------------------------------------------------------------------

/// The object was stored, then the process failed before the transaction:
/// nothing is recorded, the key is not consumed, and the orphan is collected
/// after the grace period.
#[tokio::test]
async fn a_failure_after_the_object_put_leaves_only_an_orphan_the_gc_collects() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "orphan").await;
    app.failpoints.set(
        crate::failpoints::ACCEPT_AFTER_OBJECT_PUT,
        Action::Error,
        Some(1),
    );
    let err = accept(&app, request(&f, large(1, 2000), Some("k1")))
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), 500, "{err}");
    assert_eq!(ledger(&app).outbox_stats().unwrap().pending, 0);
    assert!(
        app.history
            .list_invocations(&principal(TENANT_A), &f.id, 10)
            .unwrap()
            .is_empty()
    );
    // Within the grace period the orphan is kept, after it collected.
    assert_eq!(app.collect_objects().await.unwrap().collected_orphans, 0);
    env.advance(61);
    let gc = app.collect_objects().await.unwrap();
    assert_eq!(gc.collected_orphans, 1, "{gc:?}");
    // The key was never bound: the retry is a new, complete acceptance.
    let a = accept(&app, request(&f, large(1, 2000), Some("k1")))
        .await
        .unwrap();
    assert!(!a.replayed);
    app.publish_outbox().await.unwrap();
    let got = drain(&app).await;
    assert_eq!(ids(&got), BTreeSet::from([a.invocation.id.to_string()]));
    app.read_async_delivery(&got[0]).await.unwrap();
}

/// A failure inside the transaction (the ledger failing before COMMIT) rolls
/// back every row: 503, no invocation, no key, no event, no reference; the
/// already stored object is an orphan for the GC.
#[tokio::test]
async fn a_failure_before_commit_rolls_back_every_row_and_answers_503() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "rollback").await;
    app.failpoints.set(
        crate::failpoints::ACCEPT_BEFORE_COMMIT,
        Action::Error,
        Some(1),
    );
    let err = accept(&app, request(&f, large(1, 2000), Some("k1")))
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), 503, "{err}");
    assert_eq!(err.code(), ErrorCode::ControlPlaneUnavailable);
    assert_eq!(
        err.to_api_body(None).error.error_type.as_deref(),
        Some("Host.StoreUnavailable")
    );
    assert_eq!(ledger(&app).outbox_stats().unwrap(), OutboxStats::default());
    assert!(
        app.history
            .list_invocations(&principal(TENANT_A), &f.id, 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        app.store
            .lookup(&f.tenant_id, &f.id, "k1", env.clock.now())
            .unwrap()
            .is_none()
    );
    env.advance(61);
    assert_eq!(app.collect_objects().await.unwrap().collected_orphans, 1);
    assert!(drain(&app).await.is_empty());
}

/// Committed, then the response was lost (or the process died before the
/// 202): after a restart the client's retry with the same key answers the
/// same invocation, and the publisher delivers it once.
#[tokio::test]
async fn a_failure_after_commit_converges_on_the_same_invocation_after_a_restart() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "lost202").await;
    app.failpoints.set(
        crate::failpoints::ACCEPT_AFTER_COMMIT,
        Action::Error,
        Some(1),
    );
    let err = accept(&app, request(&f, large(7, 1000), Some("k7")))
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), 500);
    drop(app);

    let app = env.start();
    let retry = accept(&app, request(&f, large(7, 1000), Some("k7")))
        .await
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.input_storage, "object");
    assert_eq!(ledger(&app).outbox_stats().unwrap().pending, 1);
    let r = app.publish_outbox().await.unwrap();
    assert_eq!(r.marked, 1);
    let got = drain(&app).await;
    assert_eq!(ids(&got), BTreeSet::from([retry.invocation.id.to_string()]));
    assert_eq!(got.len(), 1);
}

/// Crash after commit, before publish: a restart does not settle the waiting
/// asynchronous invocations (unlike synchronous in-flight work) and the new
/// process publishes every one of them.
#[tokio::test]
async fn accepted_but_unpublished_invocations_survive_a_restart_and_are_delivered() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "restart").await;
    let mut accepted = BTreeSet::new();
    for n in 0..10 {
        let payload = if n % 3 == 0 { large(n, 1000) } else { small(n) };
        let a = accept(&app, request(&f, payload, None)).await.unwrap();
        accepted.insert(a.invocation.id.to_string());
    }
    drop(app);

    let app = env.start();
    for id in &accepted {
        let i = inv(&app, &InvocationId::parse(id).unwrap());
        assert_eq!(i.status, InvocationStatus::Accepted, "{id}");
    }
    let r = app.publish_outbox().await.unwrap();
    assert_eq!((r.published, r.marked), (10, 10));
    let got = drain(&app).await;
    assert_eq!(got.len(), 10);
    assert_eq!(ids(&got), accepted);
    for d in &got {
        app.read_async_delivery(d).await.unwrap();
    }
}

/// Crash after the broker's ACK, before the row is marked sent: after the
/// restart the claim expires, the row is published again and the broker
/// recognises the message id inside its duplicate window.
#[tokio::test]
async fn a_crash_between_publish_and_mark_is_absorbed_by_the_broker_dedup() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "dedup").await;
    let mut accepted = BTreeSet::new();
    for n in 0..5 {
        accepted.insert(
            accept(&app, request(&f, small(n), None))
                .await
                .unwrap()
                .invocation
                .id
                .to_string(),
        );
    }
    app.failpoints
        .set(crate::failpoints::OUTBOX_AFTER_PUBLISH, Action::Error, None);
    let r = app.publish_outbox().await.unwrap();
    assert_eq!((r.published, r.marked), (5, 0));
    drop(app);

    let app = env.start();
    // Still claimed by the dead publisher: nothing due yet.
    assert_eq!(app.publish_outbox().await.unwrap().claimed, 0);
    env.advance(31); // past claim_ttl, inside the 120 s duplicate window
    let r = app.publish_outbox().await.unwrap();
    assert_eq!(
        (r.claimed, r.published, r.duplicates, r.marked),
        (5, 0, 5, 5)
    );
    let got = drain(&app).await;
    assert_eq!(got.len(), 5, "one stored message per invocation");
    assert_eq!(ids(&got), accepted);
    for id in &accepted {
        let i = inv(&app, &InvocationId::parse(id).unwrap());
        assert_eq!(i.status, InvocationStatus::Queued);
    }
}

/// Outside the duplicate window the broker stores the re-publish as a second
/// message with the same message id: delivery is at-least-once, and the
/// ledger maps both deliveries to the one invocation (logically once).
#[tokio::test]
async fn a_republish_outside_the_dedup_window_is_only_a_logical_duplicate() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "window").await;
    let a = accept(&app, request(&f, small(1), None)).await.unwrap();
    app.failpoints.set(
        crate::failpoints::OUTBOX_AFTER_PUBLISH,
        Action::Error,
        Some(1),
    );
    app.publish_outbox().await.unwrap();
    drop(app);
    let app = env.start();
    env.advance(200); // past the claim and the 120 s duplicate window
    let r = app.publish_outbox().await.unwrap();
    assert_eq!((r.published, r.duplicates, r.marked), (1, 0, 1));
    let got = drain(&app).await;
    assert_eq!(got.len(), 2, "two stored messages");
    let mut logical = BTreeSet::new();
    for d in &got {
        let read = app.read_async_delivery(d).await.unwrap();
        logical.insert(read.invocation.id.to_string());
    }
    assert_eq!(logical, BTreeSet::from([a.invocation.id.to_string()]));
}

/// A publisher that panics on every pass (a crash loop) never loses an event:
/// once it stops crashing, everything is delivered once.
#[tokio::test]
async fn a_publisher_crash_loop_converges() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "crashloop").await;
    let mut accepted = BTreeSet::new();
    for n in 0..3 {
        accepted.insert(
            accept(&app, request(&f, small(n), None))
                .await
                .unwrap()
                .invocation
                .id
                .to_string(),
        );
    }
    app.failpoints.set(
        crate::failpoints::OUTBOX_BEFORE_PUBLISH,
        Action::Panic,
        Some(4),
    );
    for _ in 0..4 {
        let outbox = app.outbox.clone().unwrap();
        let pass = tokio::spawn(async move { outbox.run_once().await }).await;
        assert!(pass.is_err(), "the pass panicked");
        env.advance(31); // the dead pass's claims expire
    }
    let r = app.publish_outbox().await.unwrap();
    assert_eq!(r.marked, 3, "{r:?}");
    let got = drain(&app).await;
    assert_eq!(got.len(), 3);
    assert_eq!(ids(&got), accepted);
}

/// Two gateways on one ledger: each event is claimed by exactly one publisher.
#[tokio::test]
async fn two_publishers_on_one_ledger_publish_each_event_once() {
    let env = Env::new(Knobs::default());
    let a = env.start();
    let b = env.start();
    let (f, _) = deploy(&a, TENANT_A, "twopub").await;
    let mut accepted = BTreeSet::new();
    for n in 0..40 {
        let app = if n % 2 == 0 { &a } else { &b };
        accepted.insert(
            accept(app, request(&f, small(n), None))
                .await
                .unwrap()
                .invocation
                .id
                .to_string(),
        );
    }
    let (ra, rb) = tokio::join!(a.publish_outbox(), b.publish_outbox());
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    let (ra2, rb2) = tokio::join!(a.publish_outbox(), b.publish_outbox());
    let (ra2, rb2) = (ra2.unwrap(), rb2.unwrap());
    let total = |f: fn(&PublishReport) -> usize| f(&ra) + f(&rb) + f(&ra2) + f(&rb2);
    assert_eq!(total(|r| r.claimed), 40);
    assert_eq!(total(|r| r.published), 40);
    assert_eq!(total(|r| r.duplicates), 0);
    assert_eq!(total(|r| r.marked), 40);
    let got = drain(&a).await;
    assert_eq!(got.len(), 40);
    assert_eq!(ids(&got), accepted);
}

// ---------------------------------------------------------------------------
// backlog admission and outages
// ---------------------------------------------------------------------------

/// The queue stops: acceptances keep landing in the outbox while it has
/// headroom, then are refused with 503 `queue_unavailable`; when the queue is
/// back everything accepted is delivered and acceptance resumes.
#[tokio::test]
async fn a_queue_outage_fills_the_outbox_then_refuses_and_recovers() {
    let env = Env::new(Knobs {
        max_pending: 3,
        ..Knobs::default()
    });
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "outage").await;
    app.failpoints.set(
        crate::failpoints::OUTBOX_QUEUE_UNAVAILABLE,
        Action::Error,
        None,
    );
    let mut accepted = BTreeSet::new();
    for n in 0..3 {
        accepted.insert(
            accept(&app, request(&f, small(n), None))
                .await
                .unwrap()
                .invocation
                .id
                .to_string(),
        );
        let r = app.publish_outbox().await.unwrap();
        assert_eq!(r.marked, 0);
    }
    assert_eq!(
        app.outbox.as_ref().unwrap().health().condition(),
        QueueCondition::Unavailable
    );
    let err = accept(&app, request(&f, small(9), None)).await.unwrap_err();
    assert_eq!(refusal(&err), Some("queue_unavailable"), "{err}");
    assert_eq!(err.http_status(), 503);
    assert_eq!(ledger(&app).outbox_stats().unwrap().pending, 3);

    // The queue is back.
    app.failpoints
        .clear(crate::failpoints::OUTBOX_QUEUE_UNAVAILABLE);
    env.advance(5); // past the backoff
    let r = app.publish_outbox().await.unwrap();
    assert_eq!(r.marked, 3, "{r:?}");
    let a = accept(&app, request(&f, small(9), None)).await.unwrap();
    accepted.insert(a.invocation.id.to_string());
    app.publish_outbox().await.unwrap();
    let got = drain(&app).await;
    assert_eq!(got.len(), 4);
    assert_eq!(ids(&got), accepted);
}

/// With a healthy queue, an outbox that is over its count or age bound
/// refuses with 429 `backlog` before anything is stored.
#[tokio::test]
async fn an_outbox_over_its_bound_refuses_with_429_backlog() {
    let env = Env::new(Knobs {
        max_pending: 2,
        ..Knobs::default()
    });
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "backlog").await;
    accept(&app, request(&f, small(1), None)).await.unwrap();
    // Age bound (300 s) with one pending event.
    env.advance(301);
    let err = accept(&app, request(&f, large(2, 2000), None))
        .await
        .unwrap_err();
    assert_eq!(refusal(&err), Some("backlog"), "{err}");
    assert_eq!(err.http_status(), 429);
    app.publish_outbox().await.unwrap();
    // Count bound.
    accept(&app, request(&f, small(3), None)).await.unwrap();
    accept(&app, request(&f, small(4), None)).await.unwrap();
    let err = accept(&app, request(&f, small(5), None)).await.unwrap_err();
    assert_eq!(refusal(&err), Some("backlog"));
    // No object was stored for the refused large input.
    env.advance(3600);
    let gc = app.collect_objects().await.unwrap();
    assert_eq!(gc.collected_orphans, 0, "{gc:?}");
}

/// The in-transaction bound: acceptances racing on two gateways never
/// overshoot `max_pending_events`.
#[tokio::test]
async fn the_backlog_bound_holds_across_gateways() {
    let env = Env::new(Knobs {
        max_pending: 5,
        ..Knobs::default()
    });
    let a = env.start();
    let b = env.start();
    let (f, _) = deploy(&a, TENANT_A, "race").await;
    let mut tasks = Vec::new();
    for n in 0..30 {
        let app = if n % 2 == 0 { a.clone() } else { b.clone() };
        let f = f.clone();
        tasks.push(tokio::spawn(async move {
            accept(&app, request(&f, small(n), None)).await
        }));
    }
    let mut ok = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => ok += 1,
            Err(e) => assert_eq!(refusal(&e), Some("backlog"), "{e}"),
        }
    }
    assert_eq!(ok, 5);
    assert_eq!(ledger(&a).outbox_stats().unwrap().pending, 5);
}

/// Object store down, over quota, input too large, or no object store at
/// all: refused with the matching status and reason, nothing recorded.
#[tokio::test]
async fn object_store_refusals_answer_with_their_reason_and_record_nothing() {
    let env = Env::new(Knobs {
        max_object_bytes: 3000,
        quota: 5000,
        ..Knobs::default()
    });
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "objects").await;
    app.failpoints.set(
        crate::failpoints::OBJECT_PUT_UNAVAILABLE,
        Action::Error,
        Some(1),
    );
    let err = accept(&app, request(&f, large(1, 1000), None))
        .await
        .unwrap_err();
    assert_eq!(refusal(&err), Some("object_store_unavailable"), "{err}");
    assert_eq!(err.http_status(), 503);
    assert_eq!(err.code(), ErrorCode::AsyncUnavailable);

    let err = accept(&app, request(&f, large(2, 4000), None))
        .await
        .unwrap_err();
    assert_eq!(refusal(&err), Some("input_too_large"), "{err}");
    assert_eq!(err.http_status(), 413);

    accept(&app, request(&f, large(3, 2900), None))
        .await
        .unwrap();
    let err = accept(&app, request(&f, large(4, 2900), None))
        .await
        .unwrap_err();
    assert_eq!(refusal(&err), Some("object_quota"), "{err}");
    assert_eq!(err.http_status(), 429);
    assert_eq!(ledger(&app).outbox_stats().unwrap().pending, 1);

    let over = serde_json::json!({ "blob": "x".repeat(1024 * 1024) });
    let err = accept(&app, request(&f, over, None)).await.unwrap_err();
    assert_eq!(err.http_status(), 413);
}

#[tokio::test]
async fn without_an_object_store_only_inline_inputs_are_accepted() {
    let env = Env::new(Knobs {
        objects: false,
        ..Knobs::default()
    });
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "noobj").await;
    accept(&app, request(&f, small(1), None)).await.unwrap();
    let err = accept(&app, request(&f, large(1, 1000), None))
        .await
        .unwrap_err();
    assert_eq!(refusal(&err), Some("input_too_large"));
    assert_eq!(err.http_status(), 413);
}

// ---------------------------------------------------------------------------
// idempotency, revision pinning, tenants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_same_idempotency_key_converges_on_one_invocation() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "idem").await;
    let first = accept(&app, request(&f, large(1, 1000), Some("key")))
        .await
        .unwrap();
    let again = accept(&app, request(&f, large(1, 1000), Some("key")))
        .await
        .unwrap();
    assert!(again.replayed);
    assert_eq!(again.invocation.id, first.invocation.id);
    assert_eq!(again.input_storage, "object");
    // Concurrent retries of the same request.
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let app = app.clone();
        let f = f.clone();
        tasks.push(tokio::spawn(async move {
            accept(&app, request(&f, small(2), Some("race")))
                .await
                .unwrap()
        }));
    }
    let mut race_ids = BTreeSet::new();
    for t in tasks {
        race_ids.insert(t.await.unwrap().invocation.id);
    }
    assert_eq!(race_ids.len(), 1);

    // Same key, another input: 409 naming the bound invocation.
    let err = accept(&app, request(&f, large(2, 1000), Some("key")))
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), 409);
    assert!(matches!(
        &err,
        AppError::IdempotencyConflict { invocation_id, .. } if *invocation_id == first.invocation.id
    ));
    // Exactly one event per key, even after a restart.
    drop(app);
    let app = env.start();
    let again = accept(&app, request(&f, large(1, 1000), Some("key")))
        .await
        .unwrap();
    assert_eq!(again.invocation.id, first.invocation.id);
    app.publish_outbox().await.unwrap();
    assert_eq!(drain(&app).await.len(), 2);
}

/// A key is never shared between the synchronous and the asynchronous path.
#[tokio::test]
async fn a_key_bound_to_one_mode_is_a_conflict_in_the_other() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "modes").await;
    accept(&app, request(&f, small(1), Some("async-key")))
        .await
        .unwrap();
    let sync = app
        .invoke
        .invoke(InvokeRequest {
            principal: principal(TENANT_A),
            function_id: f.id.clone(),
            alias: None,
            revision_id: None,
            event_kind: tachyon_serverless_domain::EventKind::Json,
            payload: small(1),
            idempotency_key: Some("async-key".into()),
            client_timeout_ms: Some(5_000),
            trace_id: None,
        })
        .await
        .unwrap_err();
    assert_eq!(sync.http_status(), 409, "{sync}");
    let outcome = app
        .invoke
        .invoke(InvokeRequest {
            principal: principal(TENANT_A),
            function_id: f.id.clone(),
            alias: None,
            revision_id: None,
            event_kind: tachyon_serverless_domain::EventKind::Json,
            payload: small(2),
            idempotency_key: Some("sync-key".into()),
            client_timeout_ms: Some(5_000),
            trace_id: None,
        })
        .await
        .unwrap();
    assert_eq!(outcome.invocation().mode, InvocationMode::Sync);
    let err = accept(&app, request(&f, small(2), Some("sync-key")))
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), 409, "{err}");
}

/// The revision is resolved once, at acceptance. Moving the alias before the
/// publish, and a re-publish after a crash, both keep the accepted revision.
#[tokio::test]
async fn the_revision_is_pinned_at_acceptance_across_alias_changes_and_republishes() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, rev1) = deploy(&app, TENANT_A, "pin").await;
    let a = accept(&app, request(&f, small(1), None)).await.unwrap();
    assert_eq!(a.invocation.revision_id, rev1.id);
    assert_eq!(a.invocation.alias, Some(AliasName::default_alias()));
    let rev2 = revision(&app, &f, "pin-2").await;
    assert_ne!(rev1.id, rev2.id);
    let prod = app
        .aliases
        .get(&principal(TENANT_A), &f.id, &AliasName::default_alias())
        .unwrap();
    assert_eq!(prod.revision_id, rev2.id, "the alias moved");

    app.failpoints.set(
        crate::failpoints::OUTBOX_AFTER_PUBLISH,
        Action::Error,
        Some(1),
    );
    app.publish_outbox().await.unwrap();
    drop(app);
    let app = env.start();
    env.advance(200);
    app.publish_outbox().await.unwrap();
    let got = drain(&app).await;
    assert_eq!(got.len(), 2);
    for d in &got {
        let read = app.read_async_delivery(d).await.unwrap();
        assert_eq!(read.envelope.revision_id, rev1.id);
        assert_eq!(read.invocation.revision_id, rev1.id);
    }
    // A new acceptance takes the new revision.
    let b = accept(&app, request(&f, small(2), None)).await.unwrap();
    assert_eq!(b.invocation.revision_id, rev2.id);
}

/// Another tenant cannot accept on, read, or be delivered this tenant's
/// asynchronous invocations or their input objects.
#[tokio::test]
async fn async_invocations_and_their_inputs_never_cross_a_tenant() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "tenant").await;
    let foreign = InvokeAsyncRequest {
        principal: principal(TENANT_B),
        ..request(&f, small(1), None)
    };
    let err = accept(&app, foreign).await.unwrap_err();
    assert_eq!(err.http_status(), 404, "{err}");

    let a = accept(&app, request(&f, large(1, 2000), None))
        .await
        .unwrap();
    let err = app
        .history
        .get_invocation(&principal(TENANT_B), &a.invocation.id)
        .unwrap_err();
    assert_eq!(err.http_status(), 404);

    app.publish_outbox().await.unwrap();
    let mut got = drain(&app).await;
    let mut forged = got.remove(0);
    forged.tenant_id = TenantId::parse(TENANT_B).unwrap();
    let err = app.read_async_delivery(&forged).await.unwrap_err();
    assert_eq!(err.http_status(), 404, "{err}");
    let mut renamed = forged.clone();
    renamed.tenant_id = TenantId::parse(TENANT_A).unwrap();
    renamed.message_id = MessageId::parse("inv_01hzzzzzzzzzzzzzzzzzzzzzzz").unwrap();
    assert_eq!(
        app.read_async_delivery(&renamed)
            .await
            .unwrap_err()
            .http_status(),
        404
    );

    // The input object is invisible from tenant B's scope and its reference
    // cannot be re-attached to a tenant-B invocation.
    let input = ledger(&app).async_input(&a.invocation.id).unwrap().unwrap();
    let AsyncInputBody::Object(reference) = input.body else {
        panic!("object input expected");
    };
    let objects = app.durable.objects.as_ref().unwrap();
    let b_scope = tachyon_serverless_durable_port::ObjectScope {
        tenant_id: TenantId::parse(TENANT_B).unwrap(),
        region: reference.scope.region.clone(),
    };
    assert!(matches!(
        objects.get(&b_scope, &reference.id).await,
        Err(tachyon_serverless_durable_port::ObjectError::NotFound)
    ));
    let (fb, _) = deploy(&app, TENANT_B, "tenant-b").await;
    let b_inv = accept(&app, request(&fb, small(1), None)).await.unwrap();
    assert!(
        app.store
            .attach_object(&reference, &b_inv.invocation.id, env.clock.now())
            .is_err()
    );
    // And the GC still sees it as in use by tenant A's waiting invocation.
    assert!(matches!(
        app.store
            .claim_for_collection(&reference, CollectReason::Expired, env.clock.now())
            .unwrap(),
        CollectDecision::InUse { invocations: 1 }
    ));
}

#[tokio::test]
async fn backlog_refusal_maps_queue_conditions_to_reasons() {
    let now = chrono::Utc::now();
    let limits = BacklogLimits {
        max_pending: 2,
        max_pending_age: chrono::Duration::seconds(10),
    };
    let under = OutboxStats {
        pending: 1,
        oldest_pending_at: Some(now),
        sent: 0,
    };
    let over = OutboxStats {
        pending: 2,
        ..under
    };
    let reason =
        |s: &OutboxStats, q| backlog_refusal(s, &limits, q, now).map(|e| refusal(&e).unwrap());
    assert_eq!(reason(&under, QueueCondition::Healthy), None);
    assert_eq!(
        reason(&under, QueueCondition::Unavailable),
        None,
        "headroom: accept"
    );
    assert_eq!(reason(&under, QueueCondition::Full), Some("queue_full"));
    assert_eq!(reason(&over, QueueCondition::Healthy), Some("backlog"));
    assert_eq!(
        reason(&over, QueueCondition::Unavailable),
        Some("queue_unavailable")
    );
    assert_eq!(reason(&over, QueueCondition::Full), Some("queue_full"));
    let empty = OutboxStats::default();
    assert_eq!(reason(&empty, QueueCondition::Full), None);
}

/// A deleted function refuses asynchronous acceptance with the same 409
/// `Host.FunctionDeleted` as a synchronous invoke (PLT-4635), and records
/// nothing.
#[tokio::test]
async fn a_deleted_function_refuses_async_acceptance_with_function_deleted() {
    let env = Env::new(Knobs::default());
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "deleted").await;
    app.functions.delete(&principal(TENANT_A), &f.id).unwrap();
    let err = accept(&app, request(&f, large(1, 2000), Some("k")))
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), 409, "{err}");
    assert_eq!(err.code(), ErrorCode::FunctionDeleted);
    assert_eq!(
        err.to_api_body(None).error.error_type.as_deref(),
        Some("Host.FunctionDeleted")
    );
    assert_eq!(ledger(&app).outbox_stats().unwrap(), OutboxStats::default());
    env.advance(3600);
    let gc = app.collect_objects().await.unwrap();
    assert_eq!(gc.collected_orphans, 0, "nothing was stored: {gc:?}");
}
