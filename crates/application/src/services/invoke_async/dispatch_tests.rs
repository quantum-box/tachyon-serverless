//! The asynchronous dispatcher (PLT-4640): ACK loss, a consumer that dies
//! mid-run, concurrent duplicates, poison events, retries with backoff,
//! attempts / age / retry budget, non-retryable errors, deletion during
//! retries, and redrive with its authorization, audit and pinning.
//!
//! Every test runs the real pipeline (admission, the fake provider's guests,
//! the SQLite ledger and the SQLite queue) on a fixed clock that the test
//! advances; a "restart" is a new `Application` on the same `data_dir`.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_domain::{
    AttemptStatus, Clock, DeadLetterId, FixedClock, Function, FunctionRevision, InvocationId,
    InvocationStatus, RevisionStatus, TenantId,
};
use tachyon_serverless_durable_port::{ConsumerName, MessageId, OutgoingMessage, Topic};
use tachyon_serverless_provider_fake::{FakeExecutionProvider, FakeGuestScript};
use tachyon_serverless_provider_port::{Principal, Role};

use super::*;
use crate::failpoints::{self, Action};
use crate::repository::{
    AsyncDispatchRepository, ClaimOutcome, ClaimRequest, DeadLetter, DeadLetterReason,
    DeadLetterStatus, DispatchFence, DispatchSettle, DispatchState, SettleOutcome,
};
use crate::{Application, BootstrapOptions, GatewayConfig};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

#[derive(Clone)]
struct Knobs {
    max_attempts: u32,
    retry_budget: u32,
    extra: String,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            retry_budget: 0,
            extra: String::new(),
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
roles = ["deploy", "invoke", "redrive"]

[[identity.tokens]]
token = "tok-b"
tenant_id = "{TENANT_B}"
subject = "b"
roles = ["deploy", "invoke", "redrive"]

[queue]
backend = "sqlite"

[objects]
backend = "filesystem"
key_file = "{key}"
max_object_bytes = 65536
tenant_quota_bytes = 1048576
orphan_grace_seconds = 60

[invoke_async]
inline_input_max_bytes = 256
claim_ttl_seconds = 30

[async_dispatch]
fetch_wait_ms = 20
claim_ttl_seconds = 30
ack_wait_seconds = 120
admission_wait_ms = 500
max_attempts = {attempts}
backoff_initial_ms = 1000
backoff_max_ms = 1000
backoff_floor_ms = 1000
retry_budget = {budget}
retry_budget_window_seconds = 60
stall_timeout_seconds = 600
{extra}
"#,
        data = dir.display(),
        key = key.display(),
        attempts = k.max_attempts,
        budget = k.retry_budget,
        extra = k.extra,
    ))
    .unwrap()
}

fn principal(tenant: &str, roles: Vec<Role>) -> Principal {
    Principal {
        subject: format!("user-of-{}", &tenant[tenant.len() - 1..]),
        tenant_id: TenantId::parse(tenant).unwrap(),
        roles,
    }
}

fn full(tenant: &str) -> Principal {
    principal(tenant, vec![Role::Deploy, Role::Invoke, Role::Redrive])
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
            clock: Arc::new(FixedClock::new(chrono::Utc::now())),
            fake: Arc::new(FakeExecutionProvider::new()),
            knobs,
        }
    }

    async fn start(&self) -> Arc<Application> {
        let app = Application::bootstrap_with(
            config(self.dir.path(), &self.knobs),
            self.fake.clone(),
            BootstrapOptions {
                clock: self.clock.clone(),
                ..BootstrapOptions::default()
            },
        )
        .unwrap();
        dispatcher(&app).ensure_consumer().await.unwrap();
        app
    }

    fn advance(&self, seconds: i64) {
        self.clock.advance(chrono::Duration::seconds(seconds));
    }
}

fn dispatcher(app: &Application) -> &Arc<AsyncDispatcher> {
    app.async_dispatcher.as_ref().unwrap()
}

fn dispatch_repo(app: &Application) -> Arc<dyn AsyncDispatchRepository> {
    app.dispatch_ledger.clone().unwrap()
}

async fn revision(app: &Application, function: &Function, tag: &str) -> FunctionRevision {
    let p = full(function.tenant_id.as_str());
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
    let function = app.functions.create(&full(tenant), name, "").unwrap();
    let rev = revision(app, &function, name).await;
    (function, rev)
}

async fn accept(
    app: &Application,
    function: &Function,
    payload: serde_json::Value,
) -> InvocationId {
    app.invoke_async
        .as_ref()
        .unwrap()
        .accept(InvokeAsyncRequest {
            principal: full(function.tenant_id.as_str()),
            function_id: function.id.clone(),
            alias: None,
            revision_id: None,
            payload,
            idempotency_key: None,
            trace_id: None,
        })
        .await
        .unwrap()
        .invocation
        .id
}

/// Publish what is due and handle one delivery.
async fn pump(app: &Application) -> Option<HandleOutcome> {
    app.publish_outbox().await.unwrap();
    app.dispatch_async_once().await
}

fn inv(app: &Application, id: &InvocationId) -> Invocation {
    app.repos.invocations.get(id).unwrap().unwrap()
}

fn handler_error() -> FakeGuestScript {
    FakeGuestScript::HandlerError {
        error_type: "Handler.Transient".into(),
        message: "try again".into(),
    }
}

// ---------------------------------------------------------------------------
// retries
// ---------------------------------------------------------------------------

/// A handler that fails twice and then succeeds: each failure schedules the
/// next generation with the configured backoff (never earlier), the attempts
/// are numbered across runs, the revision stays pinned, and exactly one
/// terminal outcome is recorded.
#[tokio::test]
async fn a_retryable_failure_is_retried_with_backoff_and_then_succeeds() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, rev) = deploy(&app, TENANT_A, "flaky").await;
    env.fake.push_script(handler_error());
    env.fake.push_script(handler_error());
    let id = accept(&app, &f, serde_json::json!({"n": 1})).await;

    let first = pump(&app).await.unwrap();
    let HandleOutcome::Rescheduled {
        next_attempt_at,
        counted: true,
    } = first
    else {
        panic!("{first:?}");
    };
    assert_eq!(
        next_attempt_at,
        env.clock.now() + chrono::Duration::seconds(1)
    );
    assert_eq!(inv(&app, &id).status, InvocationStatus::Queued);
    // Not due yet: nothing is published.
    assert_eq!(app.publish_outbox().await.unwrap().claimed, 0);
    assert_eq!(app.dispatch_async_once().await, None);

    env.advance(1);
    assert!(matches!(
        pump(&app).await.unwrap(),
        HandleOutcome::Rescheduled { counted: true, .. }
    ));
    env.advance(1);
    assert_eq!(
        pump(&app).await.unwrap(),
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
    let done = inv(&app, &id);
    assert_eq!(done.status, InvocationStatus::Succeeded);
    assert_eq!(done.revision_id, rev.id);
    let attempts = app.repos.invocations.attempts_of(&id).unwrap();
    assert_eq!(
        attempts.iter().map(|a| a.number).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(matches!(attempts[0].status, AttemptStatus::Failed { .. }));
    assert!(matches!(attempts[2].status, AttemptStatus::Succeeded));
    let record = dispatch_repo(&app).dispatch_record(&id).unwrap().unwrap();
    assert_eq!(
        (record.state, record.attempts, record.generation),
        (DispatchState::Done, 3, 2)
    );
    // Nothing left anywhere.
    env.advance(5);
    assert_eq!(pump(&app).await, None);
    assert_eq!(env.fake.created().len(), 3);
    // Usage (PLT-4642): one `AttemptSettled` per run, numbered across runs,
    // the first `first` and the later ones `retry`.
    let settled: Vec<_> = app
        .usage
        .events_for_invocation(&id)
        .into_iter()
        .filter(|e| e.event_type == tachyon_serverless_domain::UsageEventType::AttemptSettled)
        .map(|e| (e.attempt_number, e.attempt_kind))
        .collect();
    use tachyon_serverless_domain::AttemptKind;
    assert_eq!(
        settled,
        vec![
            (Some(1), Some(AttemptKind::First)),
            (Some(2), Some(AttemptKind::Retry)),
            (Some(3), Some(AttemptKind::Retry)),
        ]
    );
}

/// A usage journal that cannot record (PLT-4642) starts nothing: the run is
/// deferred without counting an attempt, and runs once the journal recovers.
#[tokio::test]
async fn a_refusing_usage_journal_defers_the_run_without_counting() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "metered").await;
    let id = accept(&app, &f, serde_json::json!({})).await;
    app.usage_meter.journal().force_unavailable(true);
    let outcome = pump(&app).await.unwrap();
    assert!(
        matches!(outcome, HandleOutcome::Rescheduled { counted: false, .. }),
        "{outcome:?}"
    );
    assert!(env.fake.created().is_empty(), "nothing booted");
    let record = dispatch_repo(&app).dispatch_record(&id).unwrap().unwrap();
    assert_eq!((record.attempts, record.deferrals), (0, 1));
    assert_eq!(
        record.last_error.unwrap().error_type,
        crate::error::USAGE_JOURNAL_UNAVAILABLE
    );
    app.usage_meter.journal().force_unavailable(false);
    env.advance(2);
    assert_eq!(
        pump(&app).await.unwrap(),
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
}

/// `max_attempts` runs that all fail end in one dead letter with the last
/// error; the invocation is terminal with that error.
#[tokio::test]
async fn exhausted_attempts_are_dead_lettered_with_the_last_error() {
    let env = Env::new(Knobs {
        max_attempts: 2,
        ..Knobs::default()
    });
    env.fake.set_default_script(Some(handler_error()));
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "fails").await;
    let id = accept(&app, &f, serde_json::json!({"n": 2})).await;
    assert!(matches!(
        pump(&app).await.unwrap(),
        HandleOutcome::Rescheduled { .. }
    ));
    env.advance(1);
    assert_eq!(
        pump(&app).await.unwrap(),
        HandleOutcome::DeadLettered(DeadLetterReason::AttemptsExhausted)
    );
    let done = inv(&app, &id);
    let InvocationStatus::Failed { error } = &done.status else {
        panic!("{:?}", done.status);
    };
    assert_eq!(error.error_type, "Handler.Transient");
    let dl = dispatch_repo(&app).dead_letter_of(&id).unwrap().unwrap();
    assert_eq!(dl.reason, DeadLetterReason::AttemptsExhausted);
    assert_eq!(dl.status, DeadLetterStatus::Open);
    assert_eq!(dl.attempts, 2);
    assert_eq!(dl.last_error.unwrap().error_type, "Handler.Transient");
    assert_eq!(dl.input_storage.as_deref(), Some("inline"));
    env.advance(10);
    assert_eq!(pump(&app).await, None);
    assert_eq!(env.fake.created().len(), 2);
}

/// Older than its maximum age (a per-function override here): dead-lettered
/// as `expired` without running.
#[tokio::test]
async fn an_event_past_its_maximum_age_is_dead_lettered_without_running() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "old").await;
    drop(app);
    let env = Env {
        knobs: Knobs {
            extra: format!(
                "[[async_dispatch.function]]\nfunction_id = \"{}\"\nmax_event_age_seconds = 30\n",
                f.id
            ),
            ..Knobs::default()
        },
        ..env
    };
    let app = env.start().await;
    let id = accept(&app, &f, serde_json::json!({})).await;
    app.publish_outbox().await.unwrap();
    env.advance(31);
    assert_eq!(
        app.dispatch_async_once().await.unwrap(),
        HandleOutcome::DeadLettered(DeadLetterReason::Expired)
    );
    let InvocationStatus::Failed { error } = inv(&app, &id).status else {
        panic!()
    };
    assert_eq!(error.error_type, retry::EVENT_EXPIRED);
    assert!(env.fake.created().is_empty());
}

/// An input, validation or authorization error is not retried: one run, then
/// a dead letter (`non_retryable`). Here the stored input no longer matches
/// its accepted digest, so the handler never even starts.
#[tokio::test]
async fn a_non_retryable_error_is_dead_lettered_immediately() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "corrupt").await;
    let id = accept(&app, &f, serde_json::json!({"n": 3})).await;
    {
        let db = rusqlite::Connection::open(app.config.data_dir.join("state.db")).unwrap();
        db.execute(
            "UPDATE invocation_inputs SET inline_body = ?1 WHERE invocation_id = ?2",
            rusqlite::params![br#"{"n":4}"#.to_vec(), id.as_str()],
        )
        .unwrap();
    }
    assert_eq!(
        pump(&app).await.unwrap(),
        HandleOutcome::DeadLettered(DeadLetterReason::NonRetryable)
    );
    let dl = dispatch_repo(&app).dead_letter_of(&id).unwrap().unwrap();
    assert_eq!(dl.attempts, 1);
    assert_eq!(dl.last_error.unwrap().error_type, retry::INPUT_CORRUPT);
    assert!(env.fake.created().is_empty());
}

/// Over the per-function retry budget a retry is deferred to the next window
/// without counting an attempt; another function is not affected.
#[tokio::test]
async fn retries_over_the_budget_are_deferred_without_counting() {
    let env = Env::new(Knobs {
        retry_budget: 1,
        ..Knobs::default()
    });
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "storm").await;
    env.fake.push_script(handler_error());
    env.fake.push_script(handler_error());
    let a = accept(&app, &f, serde_json::json!({"n": "a"})).await;
    let b = accept(&app, &f, serde_json::json!({"n": "b"})).await;
    for _ in 0..2 {
        assert!(matches!(
            pump(&app).await.unwrap(),
            HandleOutcome::Rescheduled { counted: true, .. }
        ));
    }
    env.advance(1);
    let window_start = env.clock.now();
    // The first retry takes the budget, the second is deferred.
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        outcomes.push(pump(&app).await.unwrap());
    }
    assert!(
        outcomes.contains(&HandleOutcome::Completed {
            status: "succeeded"
        }),
        "{outcomes:?}"
    );
    let deferred = outcomes
        .iter()
        .find_map(|o| match o {
            HandleOutcome::Rescheduled {
                next_attempt_at,
                counted: false,
            } => Some(*next_attempt_at),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{outcomes:?}"));
    assert!(deferred >= window_start + chrono::Duration::seconds(60));
    let repo = dispatch_repo(&app);
    let records: Vec<_> = [&a, &b]
        .iter()
        .map(|id| repo.dispatch_record(id).unwrap().unwrap())
        .collect();
    assert!(
        records.iter().any(|r| r.attempts == 1 && r.deferrals == 1),
        "{records:?}"
    );
    env.advance(61);
    assert_eq!(
        pump(&app).await.unwrap(),
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
}

/// A function deleted while its invocation waits for a retry: the next run
/// is refused and dead-lettered as `function_deleted`.
#[tokio::test]
async fn a_function_deleted_during_retries_is_dead_lettered() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "gone").await;
    env.fake.push_script(handler_error());
    let id = accept(&app, &f, serde_json::json!({})).await;
    assert!(matches!(
        pump(&app).await.unwrap(),
        HandleOutcome::Rescheduled { .. }
    ));
    app.functions.delete(&full(TENANT_A), &f.id).unwrap();
    env.advance(1);
    assert_eq!(
        pump(&app).await.unwrap(),
        HandleOutcome::DeadLettered(DeadLetterReason::FunctionDeleted)
    );
    let InvocationStatus::Failed { error } = inv(&app, &id).status else {
        panic!()
    };
    assert_eq!(
        error.error_type,
        crate::services::admission::FUNCTION_DELETED
    );
    assert_eq!(env.fake.created().len(), 1);
}

// ---------------------------------------------------------------------------
// crashes, ACK loss, duplicates
// ---------------------------------------------------------------------------

/// The terminal state was committed and the process died before the ACK:
/// the redelivery after a restart is recognised as done and nothing runs
/// again.
#[tokio::test]
async fn an_ack_lost_after_the_terminal_commit_is_not_run_again() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "acklost").await;
    let id = accept(&app, &f, serde_json::json!({"n": 5})).await;
    app.failpoints
        .set(failpoints::DISPATCH_AFTER_COMMIT, Action::Error, Some(1));
    assert!(matches!(
        pump(&app).await.unwrap(),
        HandleOutcome::Failed(_)
    ));
    assert_eq!(inv(&app, &id).status, InvocationStatus::Succeeded);
    drop(app);

    let app = env.start().await;
    env.advance(121); // past ack_wait: the unacked message comes back
    assert_eq!(
        app.dispatch_async_once().await.unwrap(),
        HandleOutcome::Skipped("terminal")
    );
    assert_eq!(app.dispatch_async_once().await, None, "acked for good");
    let counters = app.dispatch_metrics.as_ref().unwrap().snapshot();
    assert_eq!(counters.deliveries.get("skipped_terminal"), Some(&1));
    assert_eq!(counters.queue_operations.get(&("ack", "ok")), Some(&1));
    assert_eq!(env.fake.created().len(), 1, "the handler ran once");
    assert_eq!(app.repos.invocations.attempts_of(&id).unwrap().len(), 1);
}

/// The dispatcher died right after claiming the run: its claim expires, the
/// redelivery takes the next attempt and completes.
#[tokio::test]
async fn a_consumer_that_stops_mid_run_is_taken_over_after_its_claim_expires() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "takeover").await;
    let id = accept(&app, &f, serde_json::json!({"n": 6})).await;
    app.failpoints
        .set(failpoints::DISPATCH_AFTER_CLAIM, Action::Error, Some(1));
    assert!(matches!(
        pump(&app).await.unwrap(),
        HandleOutcome::Failed(_)
    ));
    drop(app);

    let app = env.start().await;
    let repo = dispatch_repo(&app);
    let held = repo.dispatch_record(&id).unwrap().unwrap();
    assert_eq!((held.state, held.attempts), (DispatchState::Running, 1));
    // Still claimed: the reaper leaves it alone.
    assert_eq!(app.reap_async().await.unwrap().abandoned, 0);
    env.advance(121); // past the claim TTL and ack_wait
    assert_eq!(
        app.dispatch_async_once().await.unwrap(),
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
    let record = repo.dispatch_record(&id).unwrap().unwrap();
    assert_eq!((record.state, record.attempts), (DispatchState::Done, 2));
    assert_eq!(env.fake.created().len(), 1, "the first claim never ran");
}

/// The run finished (its side effects happened) and the process died before
/// the commit: the invocation survives the restart non-terminal, the attempt
/// is settled as outcome unknown, and the run is executed again —
/// at-least-once, never two terminal records.
#[tokio::test]
async fn a_crash_between_the_side_effect_and_the_commit_runs_the_handler_again() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "sideeffect").await;
    let id = accept(&app, &f, serde_json::json!({"n": 7})).await;
    app.failpoints
        .set(failpoints::DISPATCH_BEFORE_COMMIT, Action::Error, Some(1));
    assert!(matches!(
        pump(&app).await.unwrap(),
        HandleOutcome::Failed(_)
    ));
    assert_eq!(inv(&app, &id).status, InvocationStatus::Running);
    drop(app);

    let app = env.start().await;
    assert!(
        !inv(&app, &id).status.is_terminal(),
        "a restart never settles an asynchronous invocation"
    );
    env.advance(31);
    // The reaper reschedules the abandoned run (its attempt counted).
    let reaped = app.reap_async().await.unwrap();
    assert_eq!(reaped.abandoned, 1, "{reaped:?}");
    env.advance(2);
    let mut last = None;
    for _ in 0..3 {
        if let Some(o) = pump(&app).await {
            last = Some(o);
        }
    }
    assert_eq!(
        last,
        Some(HandleOutcome::Completed {
            status: "succeeded"
        })
    );
    assert_eq!(env.fake.created().len(), 2, "executed twice: at-least-once");
    // Both runs dispatched the handler and recorded their attempt; the
    // invocation has one terminal outcome.
    let attempts = app.repos.invocations.attempts_of(&id).unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(
        attempts
            .iter()
            .all(|a| matches!(a.status, AttemptStatus::Succeeded)),
        "{attempts:?}"
    );
    assert_eq!(inv(&app, &id).status, InvocationStatus::Succeeded);
}

/// A retry was decided and the process died before it was committed: the
/// claim expires and the next delivery retries.
#[tokio::test]
async fn a_crash_before_the_retry_commit_is_retried_after_the_claim_expires() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "retrycrash").await;
    env.fake.push_script(handler_error());
    let id = accept(&app, &f, serde_json::json!({})).await;
    app.failpoints.set(
        failpoints::DISPATCH_BEFORE_RETRY_COMMIT,
        Action::Error,
        Some(1),
    );
    assert!(matches!(
        pump(&app).await.unwrap(),
        HandleOutcome::Failed(_)
    ));
    drop(app);
    let app = env.start().await;
    env.advance(121);
    assert_eq!(
        app.dispatch_async_once().await.unwrap(),
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
    let record = dispatch_repo(&app).dispatch_record(&id).unwrap().unwrap();
    assert_eq!(record.attempts, 2);
}

/// Two deliveries of the same event handled at the same time (a redelivery
/// while the first run is still going): one run, one terminal outcome.
#[tokio::test]
async fn concurrent_duplicate_deliveries_run_the_handler_once() {
    let env = Env::new(Knobs::default());
    env.fake
        .set_default_script(Some(FakeGuestScript::SlowEchoForever));
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "dup").await;
    let id = accept(&app, &f, serde_json::json!({"sleep_ms": 400})).await;
    app.publish_outbox().await.unwrap();
    let q = app.durable.queue.clone().unwrap();
    let consumer = ConsumerName::parse("dispatcher").unwrap();
    let first = q
        .fetch(&consumer, 1, Duration::from_millis(50))
        .await
        .unwrap()
        .pop()
        .unwrap();
    env.advance(121);
    let second = q
        .fetch(&consumer, 1, Duration::from_millis(50))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.message_id, second.message_id);
    assert_eq!(second.delivery_count, 2);
    let d = dispatcher(&app).clone();
    let d2 = d.clone();
    let a = tokio::spawn(async move { d.handle(first).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let b = tokio::spawn(async move { d2.handle(second).await });
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    let outcomes = [a, b];
    assert!(
        outcomes.contains(&HandleOutcome::Completed {
            status: "succeeded"
        }),
        "{outcomes:?}"
    );
    assert!(
        outcomes.contains(&HandleOutcome::Skipped("claimed"))
            || outcomes.contains(&HandleOutcome::Skipped("terminal")),
        "{outcomes:?}"
    );
    assert_eq!(env.fake.created().len(), 1);
    assert_eq!(app.repos.invocations.attempts_of(&id).unwrap().len(), 1);
}

/// Fencing: a settle from a claim another dispatcher took over is refused,
/// so a slow run can never write a second terminal outcome.
#[tokio::test]
async fn a_settle_from_a_claim_that_was_taken_over_is_refused() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "fence").await;
    let id = accept(&app, &f, serde_json::json!({})).await;
    app.publish_outbox().await.unwrap();
    let repo = dispatch_repo(&app);
    let claim = |owner: &str| ClaimRequest {
        invocation_id: id.clone(),
        owner: owner.into(),
        generation: 0,
        now: env.clock.now(),
        ttl: chrono::Duration::seconds(30),
    };
    let ClaimOutcome::Claimed { invocation, .. } = repo.claim_dispatch(claim("dsp-a")).unwrap()
    else {
        panic!()
    };
    assert!(matches!(
        repo.claim_dispatch(claim("dsp-b")).unwrap(),
        ClaimOutcome::Held { .. }
    ));
    env.advance(31);
    let ClaimOutcome::Claimed { record, .. } = repo.claim_dispatch(claim("dsp-b")).unwrap() else {
        panic!()
    };
    assert_eq!(record.attempts, 2);
    let settle = |owner: &str, attempts: u32| {
        let mut done = (*invocation).clone();
        done.mark_cancelled(env.clock.now()).unwrap();
        DispatchSettle {
            fence: DispatchFence::Claim {
                owner: owner.into(),
                attempts,
            },
            invocation: done,
            state: DispatchState::Done,
            next_attempt_at: None,
            last_error: None,
            counted: true,
            dead_letter: None,
            republish: None,
            now: env.clock.now(),
        }
    };
    assert!(matches!(
        repo.settle_dispatch(settle("dsp-a", 1)).unwrap(),
        SettleOutcome::Lost(_)
    ));
    assert_eq!(
        repo.settle_dispatch(settle("dsp-b", 2)).unwrap(),
        SettleOutcome::Committed
    );
    assert!(matches!(
        repo.settle_dispatch(settle("dsp-b", 2)).unwrap(),
        SettleOutcome::Lost(_)
    ));
    assert_eq!(inv(&app, &id).status, InvocationStatus::Cancelled);
}

// ---------------------------------------------------------------------------
// poison
// ---------------------------------------------------------------------------

/// An undecodable event, and one that names another tenant's invocation, are
/// dead-lettered once as poison and terminated: never redelivered, never run.
#[tokio::test]
async fn poison_events_are_dead_lettered_once_and_never_redelivered() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "poison").await;
    let victim = accept(&app, &f, serde_json::json!({})).await;
    app.publish_outbox().await.unwrap();
    // Take the genuine event out of the way first.
    assert_eq!(
        app.dispatch_async_once().await.unwrap(),
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
    // Past the broker's duplicate window, so the forged copy of the genuine
    // message id is stored.
    env.advance(200);
    let q = app.durable.queue.clone().unwrap();
    q.publish(OutgoingMessage {
        tenant_id: TenantId::parse(TENANT_A).unwrap(),
        topic: Topic::parse(INVOKE_TOPIC).unwrap(),
        message_id: MessageId::parse("garbage-1").unwrap(),
        payload: b"{not json".to_vec(),
    })
    .await
    .unwrap();
    // A well-formed envelope of tenant A's invocation, routed under tenant B.
    let genuine = inv(&app, &victim);
    let envelope = InvokeEnvelope {
        version: ENVELOPE_VERSION,
        invocation_id: victim.clone(),
        tenant_id: TenantId::parse(TENANT_B).unwrap(),
        function_id: f.id.clone(),
        revision_id: genuine.revision_id.clone(),
        event_kind: genuine.event_kind,
        input_digest: genuine.input_digest.clone(),
        input_size_bytes: genuine.input_size_bytes,
        input_storage: "inline".into(),
        accepted_at: genuine.accepted_at,
        queue_deadline: genuine.deadlines.queue_deadline,
        trace_id: "t".into(),
        generation: 0,
    };
    q.publish(OutgoingMessage {
        tenant_id: TenantId::parse(TENANT_B).unwrap(),
        topic: Topic::parse(INVOKE_TOPIC).unwrap(),
        message_id: MessageId::parse(victim.as_str()).unwrap(),
        payload: serde_json::to_vec(&envelope).unwrap(),
    })
    .await
    .unwrap();
    assert_eq!(app.dispatch_async_once().await, Some(HandleOutcome::Poison));
    assert_eq!(app.dispatch_async_once().await, Some(HandleOutcome::Poison));
    assert_eq!(app.dispatch_async_once().await, None);
    env.advance(500);
    assert_eq!(
        app.dispatch_async_once().await,
        None,
        "terminated, not redelivered"
    );
    assert_eq!(env.fake.created().len(), 1);
    assert_eq!(inv(&app, &victim).status, InvocationStatus::Succeeded);
    let db = rusqlite::Connection::open(app.config.data_dir.join("state.db")).unwrap();
    let poison: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM dead_letters WHERE reason = 'poison'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(poison, 2);
    let tenant_b_linked: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM dead_letters WHERE tenant_id = ?1 AND invocation_id IS NOT NULL",
            [TENANT_B],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        tenant_b_linked, 0,
        "nothing of tenant A is linked to tenant B"
    );
    // Recording the same poison message twice keeps one entry.
    let repo = dispatch_repo(&app);
    let dl = |id: DeadLetterId| DeadLetter {
        id,
        tenant_id: TenantId::parse(TENANT_A).unwrap(),
        function_id: None,
        invocation_id: None,
        revision_id: None,
        reason: DeadLetterReason::Poison,
        status: DeadLetterStatus::Open,
        attempts: 0,
        deferrals: 0,
        last_error: None,
        accepted_at: None,
        first_attempt_at: None,
        last_attempt_at: None,
        created_at: env.clock.now(),
        input_digest: None,
        input_size_bytes: None,
        input_storage: None,
        message_id: Some("m-1".into()),
        message_sequence: Some(9),
        detail: None,
        redrive_count: 0,
        redriven_at: None,
    };
    assert!(repo.record_poison(dl(DeadLetterId::generate())).unwrap());
    assert!(!repo.record_poison(dl(DeadLetterId::generate())).unwrap());
}

// ---------------------------------------------------------------------------
// redrive
// ---------------------------------------------------------------------------

async fn dead_lettered(env: &Env, app: &Application, f: &Function) -> (InvocationId, DeadLetter) {
    let id = accept(
        app,
        f,
        serde_json::json!({"order": 42, "blob": "x".repeat(400)}),
    )
    .await;
    env.fake.push_script(handler_error());
    env.fake.push_script(handler_error());
    env.fake.push_script(handler_error());
    loop {
        match pump(app).await {
            Some(HandleOutcome::DeadLettered(_)) => break,
            Some(_) => env.advance(1),
            None => env.advance(1),
        }
    }
    let dl = dispatch_repo(app).dead_letter_of(&id).unwrap().unwrap();
    (id, dl)
}

/// Redrive needs `Invoke` and `Redrive` and stays inside the tenant: another
/// tenant sees a dead letter as missing (404), a caller without the role is
/// refused (403), and a poison entry cannot be redriven.
#[tokio::test]
async fn redrive_is_authorized_and_never_crosses_a_tenant() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, _) = deploy(&app, TENANT_A, "authz").await;
    let (_, dl) = dead_lettered(&env, &app, &f).await;
    let service = app.dead_letters.clone().unwrap();
    let req = |p: Principal| RedriveRequest {
        principal: p,
        dead_letter_id: dl.id.clone(),
        revision_id: None,
        reason: Some("fixed upstream".into()),
    };
    let no_role = service
        .redrive(req(principal(TENANT_A, vec![Role::Invoke])))
        .await
        .unwrap_err();
    assert_eq!(no_role.http_status(), 403, "{no_role}");
    let other = service.redrive(req(full(TENANT_B))).await.unwrap_err();
    assert_eq!(other.http_status(), 404, "{other}");
    assert_eq!(
        service
            .get(&full(TENANT_B), &dl.id)
            .unwrap_err()
            .http_status(),
        404
    );
    assert!(service.list(&full(TENANT_B), &f.id, 10).unwrap().is_empty());
    assert_eq!(service.list(&full(TENANT_A), &f.id, 10).unwrap().len(), 1);
    // Tenant B's own revision cannot be smuggled in either.
    let (fb, rev_b) = deploy(&app, TENANT_B, "other").await;
    let smuggle = service
        .redrive(RedriveRequest {
            revision_id: Some(rev_b.id.clone()),
            ..req(full(TENANT_A))
        })
        .await
        .unwrap_err();
    assert!(matches!(smuggle.http_status(), 400 | 404), "{smuggle}");
    let _ = fb;
    // Nothing happened to the dead letter.
    let view = service.get(&full(TENANT_A), &dl.id).unwrap();
    assert_eq!(view.dead_letter.status, DeadLetterStatus::Open);
    assert!(view.redrives.is_empty());
}

/// A redrive creates a new invocation, linked and audited, pinned to the
/// original revision (the alias moved meanwhile) with the same input by
/// reference, delivered through the outbox; the dead letter cannot be
/// redriven twice; the input object is kept while the entry is open.
#[tokio::test]
async fn a_redrive_creates_an_audited_invocation_on_the_pinned_revision_and_input() {
    let env = Env::new(Knobs::default());
    let app = env.start().await;
    let (f, rev1) = deploy(&app, TENANT_A, "redrive").await;
    let (source, dl) = dead_lettered(&env, &app, &f).await;
    assert_eq!(dl.input_storage.as_deref(), Some("object"));
    // The GC keeps the input of the open dead letter.
    env.advance(3600 * 24 * 8);
    let gc = app.collect_objects().await.unwrap();
    assert_eq!(gc.kept_in_use, 1, "{gc:?}");
    // The alias moves to a new revision.
    let rev2 = revision(&app, &f, "v2").await;
    assert_ne!(rev1.id, rev2.id);

    let service = app.dead_letters.clone().unwrap();
    let done = service
        .redrive(RedriveRequest {
            principal: full(TENANT_A),
            dead_letter_id: dl.id.clone(),
            revision_id: None,
            reason: Some("dependency fixed".into()),
        })
        .await
        .unwrap();
    let new = &done.invocation;
    assert_ne!(new.id, source);
    assert_eq!(new.revision_id, rev1.id, "pinned to the original revision");
    assert_eq!(new.input_digest, inv(&app, &source).input_digest);
    assert!(new.idempotency_key.is_none());
    let r = &done.redrive;
    assert_eq!(r.source_invocation_id, source);
    assert_eq!(r.invocation_id, new.id);
    assert_eq!(r.dead_letter_id, dl.id);
    assert_eq!(r.requested_by, "user-of-a");
    assert_eq!(r.reason.as_deref(), Some("dependency fixed"));
    assert!(!r.revision_overridden);
    let ledger = app.async_ledger.clone().unwrap();
    let (old_in, new_in) = (
        ledger.async_input(&source).unwrap().unwrap(),
        ledger.async_input(&new.id).unwrap().unwrap(),
    );
    assert_eq!(old_in.body, new_in.body, "the same object, by reference");
    let view = service.get(&full(TENANT_A), &dl.id).unwrap();
    assert_eq!(view.dead_letter.status, DeadLetterStatus::Redriven);
    assert_eq!(view.redrives.len(), 1);
    let (_, created_by) = service.links(&new.id).unwrap();
    assert_eq!(created_by.unwrap().id, r.id);
    let again = service
        .redrive(RedriveRequest {
            principal: full(TENANT_A),
            dead_letter_id: dl.id.clone(),
            revision_id: None,
            reason: None,
        })
        .await
        .unwrap_err();
    assert_eq!(again.http_status(), 409);
    // Delivered through the outbox and run on the pinned revision.
    let outcome = pump(&app).await.unwrap();
    assert_eq!(
        outcome,
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
    let finished = inv(&app, &new.id);
    assert_eq!(finished.status, InvocationStatus::Succeeded);
    assert_eq!(finished.revision_id, rev1.id);
    // An explicit override to another revision of the same function (this
    // one was accepted on rev2; it is redriven on rev1).
    let (source2, dl2) = dead_lettered(&env, &app, &f).await;
    assert_eq!(inv(&app, &source2).revision_id, rev2.id);
    let overridden = service
        .redrive(RedriveRequest {
            principal: full(TENANT_A),
            dead_letter_id: dl2.id.clone(),
            revision_id: Some(rev1.id.clone()),
            reason: None,
        })
        .await
        .unwrap();
    assert_eq!(overridden.invocation.revision_id, rev1.id);
    assert!(overridden.redrive.revision_overridden);
}

/// Budget (PLT-4643): every asynchronous run reserves its maximum charge at
/// the same admission point as a synchronous invoke, and settles from the
/// usage ledger; a tenant whose hard limit does not admit the run is deferred
/// without counting an attempt and without booting anything.
#[tokio::test]
async fn asynchronous_runs_reserve_budget_and_a_refused_run_is_deferred() {
    let env = Env::new(Knobs {
        extra: format!(
            "[budget]\nenabled = true\n\n[[budget.tenants]]\ntenant_id = \"{TENANT_A}\"\nhard_limit_micros = 1000000000\n\n\
             [[budget.tenants]]\ntenant_id = \"{TENANT_B}\"\nhard_limit_micros = 0\n"
        ),
        ..Knobs::default()
    });
    let app = env.start().await;
    let (fa, _) = deploy(&app, TENANT_A, "budgeted").await;
    let (fb, _) = deploy(&app, TENANT_B, "broke").await;

    let ida = accept(&app, &fa, serde_json::json!({})).await;
    assert_eq!(
        pump(&app).await.unwrap(),
        HandleOutcome::Completed {
            status: "succeeded"
        }
    );
    let period = crate::budget::period_of(&env.clock.now());
    let rows = app
        .budget
        .store()
        .reservations_of(TENANT_A, &period)
        .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(
        rows[0].reservation_id.starts_with(&format!("{ida}:run-0:")),
        "{rows:?}"
    );
    assert_eq!(rows[0].attempts.len(), 1);
    app.collect_usage().unwrap();
    let row = app
        .budget
        .store()
        .get(&rows[0].reservation_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, crate::budget::ReservationState::Settled);
    assert!(row.settled_micros > 0 && row.settled_micros < row.reserved_micros);

    let created = env.fake.created().len();
    let idb = accept(&app, &fb, serde_json::json!({})).await;
    let outcome = pump(&app).await.unwrap();
    assert!(
        matches!(outcome, HandleOutcome::Rescheduled { counted: false, .. }),
        "{outcome:?}"
    );
    assert_eq!(env.fake.created().len(), created, "nothing booted");
    let record = dispatch_repo(&app).dispatch_record(&idb).unwrap().unwrap();
    assert_eq!((record.attempts, record.deferrals), (0, 1));
    assert_eq!(
        record.last_error.unwrap().error_type,
        crate::budget::BUDGET_EXHAUSTED
    );
    assert!(
        app.budget
            .store()
            .reservations_of(TENANT_B, &period)
            .unwrap()
            .is_empty()
    );
    app.budget.store().verify_totals().unwrap();
}
