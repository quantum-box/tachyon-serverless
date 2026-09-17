//! Cron and webhook triggers (PLT-4641) against the real ledger (`state.db`)
//! and the embedded SQLite queue, with a fake clock: every fire goes through
//! the asynchronous acceptance, restarts are new `Application`s on the same
//! `data_dir`, and "nothing was stored" is checked on the ledger itself.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, CreateTriggerRequest, CronTriggerRequest,
    ExecutionRequest, MissedRunPolicyRequest, ResourcesRequest, TriggerTargetRequest,
    UpdateTriggerRequest, WebhookTriggerRequest,
};
use tachyon_serverless_domain::{
    FixedClock, Function, FunctionRevision, InvocationMode, InvocationStatus, RevisionStatus,
    TenantId, Timestamp,
};
use tachyon_serverless_provider_fake::FakeExecutionProvider;
use tachyon_serverless_provider_port::{Principal, Role};

use super::webhook::sign;
use super::*;
use crate::{Application, BootstrapOptions, GatewayConfig};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

fn config(dir: &Path, max_pending: u64) -> GatewayConfig {
    let key = dir.join("triggers.key");
    if !key.exists() {
        std::fs::write(&key, "17".repeat(32)).unwrap();
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
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "tok-b"
tenant_id = "{TENANT_B}"
subject = "b"
roles = ["deploy", "invoke"]

[queue]
backend = "sqlite"

[invoke_async]
max_pending_events = {max_pending}

[triggers]
grace_seconds = 30
max_catchup_seconds = 300
fire_retention_seconds = 2592000
webhook_max_body_bytes = 4096
webhook_default_tolerance_seconds = 300
webhook_max_tolerance_seconds = 600
webhook_dedup_retention_seconds = 7200
secret_key_file = "{key}"
"#,
        data = dir.display(),
        key = key.display(),
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

fn ts(s: &str) -> Timestamp {
    chrono::DateTime::parse_from_rfc3339(s)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

struct Env {
    dir: tempfile::TempDir,
    clock: Arc<FixedClock>,
    fake: Arc<FakeExecutionProvider>,
    max_pending: u64,
}

impl Env {
    fn new(start: &str) -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            clock: Arc::new(FixedClock::new(ts(start))),
            fake: Arc::new(FakeExecutionProvider::new()),
            max_pending: 10_000,
        }
    }

    fn start(&self) -> Arc<Application> {
        Application::bootstrap_with(
            config(self.dir.path(), self.max_pending),
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

    fn now(&self) -> Timestamp {
        self.clock.now()
    }
}

async fn deploy(app: &Application, tenant: &str, name: &str) -> (Function, FunctionRevision) {
    let p = principal(tenant);
    let function = app.functions.create(&p, name, "").unwrap();
    let artifact = app
        .artifact_service
        .upload(&p, format!("#!/bin/sh\necho {name}\n").as_bytes())
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
    (function, rev)
}

fn svc(app: &Application) -> &Arc<TriggerService> {
    app.triggers.as_ref().expect("triggers are configured")
}

fn cron_request(
    expression: &str,
    tz: &str,
    policy: MissedRunPolicyRequest,
) -> CreateTriggerRequest {
    CreateTriggerRequest {
        name: format!("cron {expression}"),
        kind: "cron".into(),
        enabled: true,
        target: TriggerTargetRequest::default(),
        cron: Some(CronTriggerRequest {
            expression: expression.into(),
            timezone: tz.into(),
            payload: serde_json::json!({"job": "report"}),
            missed_run_policy: policy,
        }),
        webhook: None,
    }
}

fn webhook_request() -> CreateTriggerRequest {
    CreateTriggerRequest {
        name: "hook".into(),
        kind: "webhook".into(),
        enabled: true,
        target: TriggerTargetRequest::default(),
        cron: None,
        webhook: Some(WebhookTriggerRequest {
            max_body_bytes: Some(1024),
            ..WebhookTriggerRequest::default()
        }),
    }
}

fn create(app: &Application, f: &Function, req: &CreateTriggerRequest) -> TriggerWithSecret {
    svc(app)
        .create(&principal(f.tenant_id.as_str()), &f.id, req)
        .unwrap()
}

fn current(app: &Application, t: &Trigger) -> Trigger {
    svc(app).repository().get_trigger(&t.id).unwrap().unwrap()
}

fn invocations(app: &Application, f: &Function) -> Vec<Invocation> {
    app.repos.invocations.list_by_function(&f.id, 1000).unwrap()
}

fn fires(app: &Application, t: &Trigger) -> Vec<FireRecord> {
    svc(app).repository().list_fires(&t.id, 1000).unwrap()
}

fn scheduled(app: &Application, t: &Trigger) -> Vec<String> {
    let mut out: Vec<String> = fires(app, t)
        .iter()
        .filter(|f| f.outcome == FireOutcome::Accepted)
        .map(|f| f.scheduled_at.unwrap().to_rfc3339())
        .collect();
    out.sort();
    out
}

fn pending(app: &Application) -> u64 {
    app.async_ledger
        .as_ref()
        .unwrap()
        .outbox_stats()
        .unwrap()
        .pending
}

// ---------------------------------------------------------------------------
// cron
// ---------------------------------------------------------------------------

/// Each scheduled time fires one asynchronous invocation through the common
/// acceptance: `async` mode, the revision pinned at acceptance, the
/// idempotency key `cron:{trigger}:{scheduled_at}`, an outbox event and a fire
/// row; a second pass at the same time creates nothing.
#[tokio::test]
async fn a_cron_trigger_fires_each_scheduled_time_once_through_the_async_acceptance() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, rev) = deploy(&app, TENANT_A, "cron").await;
    let t = create(
        &app,
        &f,
        &cron_request("0 * * * * *", "UTC", MissedRunPolicyRequest::Skip),
    );
    assert!(t.secret.is_none());
    assert_eq!(t.trigger.next_fire_at, Some(ts("2026-09-17T00:01:00Z")));
    // Not due yet.
    let r = app.run_trigger_scheduler().await.unwrap();
    assert!(r.owner);
    assert_eq!(r.due_triggers, 0);

    env.advance(60);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!((r.due_triggers, r.accepted), (1, 1), "{r:?}");
    let invs = invocations(&app, &f);
    assert_eq!(invs.len(), 1);
    let inv = &invs[0];
    assert_eq!(inv.mode, InvocationMode::Async);
    assert_eq!(inv.status, InvocationStatus::Accepted);
    assert_eq!(inv.revision_id, rev.id);
    assert_eq!(
        inv.idempotency_key.as_deref(),
        Some(format!("cron:{}:2026-09-17T00:01:00Z", t.trigger.id).as_str())
    );
    assert_eq!(pending(&app), 1, "the outbox event committed with the fire");
    let input = app
        .async_ledger
        .as_ref()
        .unwrap()
        .async_input(&inv.id)
        .unwrap()
        .unwrap();
    let crate::repository::AsyncInputBody::Inline(bytes) = input.body else {
        panic!("inline input expected");
    };
    let event: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(event["source"], "tachyon.cron");
    assert_eq!(event["scheduled_at"], "2026-09-17T00:01:00Z");
    assert_eq!(event["payload"]["job"], "report");
    let fire = &fires(&app, &t.trigger)[0];
    assert_eq!(fire.invocation_id.as_ref(), Some(&inv.id));
    assert_eq!(fire.fire_key, "cron:2026-09-17T00:01:00Z");
    let cur = current(&app, &t.trigger);
    assert_eq!(cur.next_fire_at, Some(ts("2026-09-17T00:02:00Z")));
    assert_eq!(cur.last_scheduled_at, Some(ts("2026-09-17T00:01:00Z")));

    // The same instant again: nothing due, nothing new.
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!(r.accepted, 0);
    assert_eq!(invocations(&app, &f).len(), 1);
    // The published event is the ordinary asynchronous one.
    assert_eq!(app.publish_outbox().await.unwrap().marked, 1);
}

/// A crash between the fire transaction and the cursor move leaves the cursor
/// on a time that already fired. A restarted gateway on the same data_dir
/// computes that time again and creates nothing.
#[tokio::test]
async fn a_restart_on_the_same_data_dir_never_fires_a_scheduled_time_twice() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "restart").await;
    let t = create(
        &app,
        &f,
        &cron_request(
            "*/10 * * * * *",
            "UTC",
            MissedRunPolicyRequest::RunAll { max_runs: 10 },
        ),
    );
    env.advance(10);
    // The snapshot the crashed pass had: cursor at 00:00:10.
    let stale = current(&app, &t.trigger);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!(r.accepted, 1);
    drop(app);

    let app = env.start();
    let mut report = SchedulerReport::default();
    svc(&app)
        .process_cron_trigger(&stale, env.now(), &mut report)
        .await;
    assert_eq!(
        (report.accepted, report.already_fired),
        (0, 1),
        "{report:?}"
    );
    // Downtime of 25 s, then a normal pass: 00:00:20 and 00:00:30 fire once.
    env.advance(25);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!(r.accepted, 2, "{r:?}");
    drop(app);
    let app = env.start();
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!(r.accepted, 0);
    assert_eq!(
        scheduled(&app, &t.trigger),
        vec![
            "2026-09-17T00:00:10+00:00",
            "2026-09-17T00:00:20+00:00",
            "2026-09-17T00:00:30+00:00"
        ]
    );
    assert_eq!(invocations(&app, &f).len(), 3);
}

/// Two gateways on one ledger: the scheduler lease has one owner, and even
/// two schedulers that both process the same trigger (a lease handover, a
/// clock skew) fire every scheduled time exactly once.
#[tokio::test]
async fn two_schedulers_on_one_ledger_fire_each_scheduled_time_once() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let a = env.start();
    let b = env.start();
    let (f, _) = deploy(&a, TENANT_A, "two").await;
    let t = create(
        &a,
        &f,
        &cron_request(
            "* * * * * *",
            "UTC",
            MissedRunPolicyRequest::RunAll { max_runs: 100 },
        ),
    );

    env.advance(1);
    let ra = a.run_trigger_scheduler().await.unwrap();
    let rb = b.run_trigger_scheduler().await.unwrap();
    assert!(ra.owner && !rb.owner, "one lease owner: {ra:?} {rb:?}");
    // The owner stops: the other takes over at once.
    a.stop_dispatcher();
    env.advance(1);
    let rb = b.run_trigger_scheduler().await.unwrap();
    assert!(rb.owner, "{rb:?}");

    // Both process the same snapshots concurrently, bypassing the lease.
    for _ in 0..20 {
        env.advance(1);
        let snap = current(&a, &t.trigger);
        let now = env.now();
        let (sa, sb) = (svc(&a).clone(), svc(&b).clone());
        let (s1, s2) = (snap.clone(), snap);
        let ja = tokio::spawn(async move {
            let mut r = SchedulerReport::default();
            sa.process_cron_trigger(&s1, now, &mut r).await;
            r
        });
        let jb = tokio::spawn(async move {
            let mut r = SchedulerReport::default();
            sb.process_cron_trigger(&s2, now, &mut r).await;
            r
        });
        let (x, y) = (ja.await.unwrap(), jb.await.unwrap());
        assert_eq!(x.accepted + y.accepted, 1, "{x:?} {y:?}");
    }
    let times = scheduled(&a, &t.trigger);
    let mut unique = times.clone();
    unique.dedup();
    assert_eq!(times, unique, "no scheduled time twice");
    assert_eq!(times.len(), 22);
    assert_eq!(invocations(&a, &f).len(), 22);
}

/// After 10 minutes without a scheduler, a per-minute schedule has 9 late
/// times and one on time. `skip` runs only the on-time one, `run_once` the
/// latest late one too, `run_all(3)` the three latest late ones, and
/// `run_all(100)` stops at the catch-up window (300 s).
#[tokio::test]
async fn missed_runs_follow_the_policy_after_downtime() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "missed").await;
    let skip = create(
        &app,
        &f,
        &cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip),
    );
    let once = create(
        &app,
        &f,
        &cron_request("* * * * *", "UTC", MissedRunPolicyRequest::RunOnce),
    );
    let three = create(
        &app,
        &f,
        &cron_request(
            "* * * * *",
            "UTC",
            MissedRunPolicyRequest::RunAll { max_runs: 3 },
        ),
    );
    let window = create(
        &app,
        &f,
        &cron_request(
            "* * * * *",
            "UTC",
            MissedRunPolicyRequest::RunAll { max_runs: 100 },
        ),
    );
    drop(app);
    env.advance(600);
    let app = env.start();
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!(r.due_triggers, 4);
    assert_eq!(
        scheduled(&app, &skip.trigger),
        vec!["2026-09-17T00:10:00+00:00"]
    );
    assert_eq!(
        scheduled(&app, &once.trigger),
        vec!["2026-09-17T00:09:00+00:00", "2026-09-17T00:10:00+00:00"]
    );
    assert_eq!(
        scheduled(&app, &three.trigger),
        vec![
            "2026-09-17T00:07:00+00:00",
            "2026-09-17T00:08:00+00:00",
            "2026-09-17T00:09:00+00:00",
            "2026-09-17T00:10:00+00:00"
        ]
    );
    assert_eq!(scheduled(&app, &window.trigger).len(), 6, "00:05..00:10");
    // Every cursor moved past now; nothing fires again.
    for t in [&skip, &once, &three, &window] {
        assert_eq!(
            current(&app, &t.trigger).next_fire_at,
            Some(ts("2026-09-17T00:11:00Z"))
        );
    }
    assert_eq!(app.run_trigger_scheduler().await.unwrap().accepted, 0);
    // A late time inside the grace period is on time for every policy.
    env.advance(60 + 20);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!(r.accepted, 4, "{r:?}");
}

/// The scheduler runs in the trigger's zone with a fake clock that jumps to
/// each due time: 02:30 New York does not exist on 2026-03-08 and is skipped,
/// 01:30 on 2026-11-01 exists twice and fires once; Tokyo has neither.
#[tokio::test]
async fn the_scheduler_follows_the_trigger_time_zone_across_dst() {
    /// A gateway from `start` with one daily 02:30 / 01:30 trigger in `zone`,
    /// the clock jumping to each next due time (the outbox published between
    /// jumps, so its age bound does not refuse the next fire).
    async fn run(start: &str, expression: &str, zone: &str, steps: usize) -> Vec<String> {
        let env = Env::new(start);
        let app = env.start();
        let (f, _) = deploy(&app, TENANT_A, "dst").await;
        let t = create(
            &app,
            &f,
            &cron_request(expression, zone, MissedRunPolicyRequest::Skip),
        )
        .trigger;
        for _ in 0..steps {
            let next = current(&app, &t).next_fire_at.unwrap();
            env.clock.set(next);
            let r = app.run_trigger_scheduler().await.unwrap();
            assert_eq!(r.accepted, 1, "{zone} {r:?}");
            app.publish_outbox().await.unwrap();
        }
        scheduled(&app, &t)
    }
    assert_eq!(
        run("2026-03-07T00:00:00Z", "30 2 * * *", "America/New_York", 2).await,
        vec!["2026-03-07T07:30:00+00:00", "2026-03-09T06:30:00+00:00"],
        "no fire on the spring-forward day"
    );
    assert_eq!(
        run("2026-03-07T00:00:00Z", "30 2 * * *", "Asia/Tokyo", 3).await,
        vec![
            "2026-03-07T17:30:00+00:00",
            "2026-03-08T17:30:00+00:00",
            "2026-03-09T17:30:00+00:00"
        ],
        "Tokyo fires daily at 17:30 UTC"
    );
    assert_eq!(
        run("2026-10-31T12:00:00Z", "30 1 * * *", "America/New_York", 2).await,
        vec!["2026-11-01T05:30:00+00:00", "2026-11-02T06:30:00+00:00"],
        "the repeated 01:30 fires once"
    );
}

/// Disable stops new fires at once and delete too; what was accepted before
/// stays an ordinary asynchronous invocation and is still published.
/// Re-enabling starts from now: the disabled period is not "missed".
#[tokio::test]
async fn disable_and_delete_stop_new_fires_and_accepted_invocations_continue() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "stop").await;
    let p = principal(TENANT_A);
    let t = create(
        &app,
        &f,
        &cron_request(
            "* * * * *",
            "UTC",
            MissedRunPolicyRequest::RunAll { max_runs: 10 },
        ),
    );
    env.advance(60);
    assert_eq!(app.run_trigger_scheduler().await.unwrap().accepted, 1);
    let accepted = invocations(&app, &f)[0].id.clone();

    let disabled = svc(&app)
        .update(
            &p,
            &f.id,
            &t.trigger.id,
            &UpdateTriggerRequest {
                enabled: Some(false),
                expected_generation: Some(1),
                ..UpdateTriggerRequest::default()
            },
        )
        .unwrap()
        .trigger;
    assert_eq!(disabled.status, TriggerStatus::Disabled);
    assert!(disabled.next_fire_at.is_none());
    env.advance(180);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!((r.due_triggers, r.accepted), (0, 0));
    assert_eq!(app.publish_outbox().await.unwrap().marked, 1);
    assert_eq!(
        app.repos
            .invocations
            .get(&accepted)
            .unwrap()
            .unwrap()
            .status,
        InvocationStatus::Queued,
        "an invocation accepted before the disable continues"
    );

    // A stale generation is a conflict.
    let err = svc(&app)
        .update(
            &p,
            &f.id,
            &t.trigger.id,
            &UpdateTriggerRequest {
                enabled: Some(true),
                expected_generation: Some(1),
                ..UpdateTriggerRequest::default()
            },
        )
        .unwrap_err();
    assert_eq!(err.http_status(), 409);
    let enabled = svc(&app)
        .update(
            &p,
            &f.id,
            &t.trigger.id,
            &UpdateTriggerRequest {
                enabled: Some(true),
                ..UpdateTriggerRequest::default()
            },
        )
        .unwrap()
        .trigger;
    assert_eq!(enabled.next_fire_at, Some(ts("2026-09-17T00:05:00Z")));
    env.advance(60);
    assert_eq!(
        app.run_trigger_scheduler().await.unwrap().accepted,
        1,
        "no catch-up of the disabled period"
    );

    svc(&app).delete(&p, &f.id, &t.trigger.id).unwrap();
    env.advance(600);
    assert_eq!(app.run_trigger_scheduler().await.unwrap().due_triggers, 0);
    assert_eq!(invocations(&app, &f).len(), 2);
    assert_eq!(
        svc(&app)
            .get(&p, &f.id, &t.trigger.id)
            .unwrap_err()
            .http_status(),
        404
    );
    assert_eq!(
        svc(&app)
            .delete(&p, &f.id, &t.trigger.id)
            .unwrap_err()
            .http_status(),
        404
    );
}

/// The race: a scheduler read the trigger as enabled, then it was disabled
/// (or deleted, or changed) before the fire transaction. The transaction
/// re-reads the trigger and commits nothing: no invocation, no outbox event,
/// no fire row, no idempotency key.
#[tokio::test]
async fn a_disable_or_delete_racing_a_scheduled_fire_commits_nothing() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "race").await;
    let p = principal(TENANT_A);
    for action in ["disable", "delete", "change"] {
        let t = create(
            &app,
            &f,
            &cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip),
        );
        env.advance(60);
        let snapshot = current(&app, &t.trigger);
        match action {
            "disable" => {
                svc(&app)
                    .update(
                        &p,
                        &f.id,
                        &t.trigger.id,
                        &UpdateTriggerRequest {
                            enabled: Some(false),
                            ..UpdateTriggerRequest::default()
                        },
                    )
                    .unwrap();
            }
            "delete" => {
                svc(&app).delete(&p, &f.id, &t.trigger.id).unwrap();
            }
            _ => {
                svc(&app)
                    .update(
                        &p,
                        &f.id,
                        &t.trigger.id,
                        &UpdateTriggerRequest {
                            payload: Some(serde_json::json!({"v": 2})),
                            ..UpdateTriggerRequest::default()
                        },
                    )
                    .unwrap();
            }
        }
        let before = invocations(&app, &f).len();
        let mut report = SchedulerReport::default();
        svc(&app)
            .process_cron_trigger(&snapshot, env.now(), &mut report)
            .await;
        assert_eq!(
            (report.accepted, report.inactive),
            (0, 1),
            "{action}: {report:?}"
        );
        assert_eq!(invocations(&app, &f).len(), before, "{action}");
        assert!(fires(&app, &t.trigger).is_empty(), "{action}");
        assert_eq!(pending(&app), 0, "{action}");
    }
}

/// A full outbox refuses the fire like any asynchronous acceptance (429
/// backlog): the time is deferred, not recorded, and fires once the outbox
/// drained.
#[tokio::test]
async fn trigger_fires_share_the_async_backlog_bound_and_retry_after_it_drains() {
    let mut env = Env::new("2026-09-17T00:00:00Z");
    env.max_pending = 1;
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "backlog").await;
    let t = create(
        &app,
        &f,
        &cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip),
    );
    env.advance(60);
    assert_eq!(app.run_trigger_scheduler().await.unwrap().accepted, 1);
    env.advance(60);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!((r.accepted, r.deferred), (0, 1), "{r:?}");
    assert_eq!(
        fires(&app, &t.trigger).len(),
        1,
        "a deferred time is not recorded"
    );
    assert_eq!(
        current(&app, &t.trigger).next_fire_at,
        Some(ts("2026-09-17T00:02:00Z"))
    );
    app.publish_outbox().await.unwrap();
    env.advance(5);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!(r.accepted, 1, "{r:?}");
    assert_eq!(scheduled(&app, &t.trigger).len(), 2);
}

/// A deleted function refuses the fire for good (recorded with its reason)
/// and the platform disables the trigger.
#[tokio::test]
async fn a_deleted_function_refuses_the_fire_and_disables_its_trigger() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "gone").await;
    let t = create(
        &app,
        &f,
        &cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip),
    );
    app.functions.delete(&principal(TENANT_A), &f.id).unwrap();
    env.advance(60);
    app.run_trigger_scheduler().await.unwrap();
    let fire = &fires(&app, &t.trigger)[0];
    assert_eq!(fire.outcome, FireOutcome::Refused);
    assert!(fire.invocation_id.is_none());
    assert!(fire.reason.as_deref().unwrap().contains("409"), "{fire:?}");
    let cur = current(&app, &t.trigger);
    assert_eq!(cur.status, TriggerStatus::Disabled);
    assert_eq!(cur.status_reason.as_deref(), Some(REASON_FUNCTION_DELETED));
}

// ---------------------------------------------------------------------------
// webhook
// ---------------------------------------------------------------------------

struct Hook {
    app: Arc<Application>,
    f: Function,
    t: Trigger,
    secret: String,
}

async fn hook(env: &Env) -> Hook {
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "hook").await;
    let created = create(&app, &f, &webhook_request());
    Hook {
        app,
        f,
        secret: created.secret.expect("the secret is shown once"),
        t: created.trigger,
    }
}

fn delivery(secret: &str, ts: i64, body: &[u8], event: Option<&str>) -> WebhookDelivery {
    WebhookDelivery {
        timestamp: Some(ts.to_string()),
        signature: Some(sign(secret, ts, body)),
        event_id: event.map(str::to_string),
        content_type: Some("application/json".into()),
    }
}

/// What a refused delivery must not have written.
fn stored(h: &Hook) -> (usize, usize, u64) {
    (
        invocations(&h.app, &h.f).len(),
        fires(&h.app, &h.t).len(),
        pending(&h.app),
    )
}

/// Signature fixtures: every refusal (bad signature, wrong secret, a
/// timestamp too old or in the future, an oversized body, a missing event id)
/// happens before anything is stored; the valid delivery is accepted through
/// the async path.
#[tokio::test]
async fn webhook_refusals_happen_before_any_durable_write() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let h = hook(&env).await;
    let svc = svc(&h.app);
    let now = env.now().timestamp();
    let body = br#"{"order":42}"#;
    let empty = stored(&h);
    assert_eq!(empty, (0, 0, 0));

    let expect = |r: Result<WebhookAcceptance, AppError>, status: u16, what: &str| {
        let e = r.expect_err(what);
        assert_eq!(e.http_status(), status, "{what}: {e}");
    };
    // bad signature
    let mut d = delivery(&h.secret, now, body, Some("evt-1"));
    d.signature = Some(format!("v1={}", "0".repeat(64)));
    expect(
        svc.receive_webhook(&h.t, &d, body).await,
        401,
        "bad signature",
    );
    // wrong secret
    let d = delivery(&webhook::generate_secret(), now, body, Some("evt-1"));
    expect(
        svc.receive_webhook(&h.t, &d, body).await,
        401,
        "wrong secret",
    );
    // body changed after signing
    let d = delivery(&h.secret, now, body, Some("evt-1"));
    expect(
        svc.receive_webhook(&h.t, &d, br#"{"order":43}"#).await,
        401,
        "tampered body",
    );
    // too old / in the future
    let d = delivery(&h.secret, now - 301, body, Some("evt-1"));
    expect(svc.receive_webhook(&h.t, &d, body).await, 401, "expired");
    let d = delivery(&h.secret, now + 301, body, Some("evt-1"));
    expect(svc.receive_webhook(&h.t, &d, body).await, 401, "future");
    // missing signature / timestamp
    let mut d = delivery(&h.secret, now, body, Some("evt-1"));
    d.signature = None;
    expect(svc.receive_webhook(&h.t, &d, body).await, 401, "unsigned");
    let mut d = delivery(&h.secret, now, body, Some("evt-1"));
    d.timestamp = None;
    expect(
        svc.receive_webhook(&h.t, &d, body).await,
        401,
        "no timestamp",
    );
    // oversized (1024 bytes max)
    let big = vec![b'x'; 1025];
    let d = delivery(&h.secret, now, &big, Some("evt-1"));
    expect(svc.receive_webhook(&h.t, &d, &big).await, 413, "oversized");
    // missing / invalid event id, after a valid signature
    let d = delivery(&h.secret, now, body, None);
    expect(
        svc.receive_webhook(&h.t, &d, body).await,
        400,
        "no event id",
    );
    let d = delivery(&h.secret, now, body, Some("has space"));
    expect(
        svc.receive_webhook(&h.t, &d, body).await,
        400,
        "bad event id",
    );
    assert_eq!(stored(&h), empty, "no refusal wrote anything");

    // valid
    let d = delivery(&h.secret, now - 299, body, Some("evt-1"));
    let a = svc.receive_webhook(&h.t, &d, body).await.unwrap();
    assert!(!a.replayed);
    assert_eq!(a.invocation.mode, InvocationMode::Async);
    assert_eq!(
        a.invocation.idempotency_key.as_deref(),
        Some(format!("webhook:{}:evt-1", h.t.id).as_str())
    );
    assert_eq!(stored(&h), (1, 1, 1));
    let input = h
        .app
        .async_ledger
        .as_ref()
        .unwrap()
        .async_input(&a.invocation.id)
        .unwrap()
        .unwrap();
    let crate::repository::AsyncInputBody::Inline(bytes) = input.body else {
        panic!("inline input expected");
    };
    let event: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(event["source"], "tachyon.webhook");
    assert_eq!(event["event_id"], "evt-1");
    assert_eq!(event["body"]["order"], 42);
}

/// A resend of an accepted event (re-signed, even with another body) answers
/// the same invocation; so does the same signed delivery replayed under
/// another event id. Nothing new is created.
#[tokio::test]
async fn a_replayed_webhook_answers_the_same_invocation_and_creates_nothing() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let h = hook(&env).await;
    let svc = svc(&h.app);
    let now = env.now().timestamp();
    let body = br#"{"order":1}"#;
    let first = svc
        .receive_webhook(&h.t, &delivery(&h.secret, now, body, Some("evt-9")), body)
        .await
        .unwrap();
    env.advance(30);
    let later = env.now().timestamp();
    let resend = svc
        .receive_webhook(&h.t, &delivery(&h.secret, later, body, Some("evt-9")), body)
        .await
        .unwrap();
    assert!(resend.replayed);
    assert_eq!(resend.invocation.id, first.invocation.id);
    let other_body = br#"{"order":1,"retry":true}"#;
    let resend = svc
        .receive_webhook(
            &h.t,
            &delivery(&h.secret, later, other_body, Some("evt-9")),
            other_body,
        )
        .await
        .unwrap();
    assert_eq!(resend.invocation.id, first.invocation.id);
    // The captured signed delivery with a forged event id header.
    let captured = delivery(&h.secret, now, body, Some("evt-forged"));
    let replay = svc.receive_webhook(&h.t, &captured, body).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.invocation.id, first.invocation.id);
    assert_eq!(replay.event_id, "evt-9");
    assert_eq!(stored(&h), (1, 1, 1));
    // A new event is a new invocation.
    let body2 = br#"{"order":2}"#;
    let second = svc
        .receive_webhook(
            &h.t,
            &delivery(&h.secret, later, body2, Some("evt-10")),
            body2,
        )
        .await
        .unwrap();
    assert_ne!(second.invocation.id, first.invocation.id);
    assert_eq!(stored(&h), (2, 2, 2));
}

/// The secret is in the create (and rotate) answer only: never in a read of
/// the trigger, never in `state.db` in plain text. A rotated secret replaces
/// the old one at once.
#[tokio::test]
async fn a_webhook_secret_is_shown_once_and_never_stored_or_returned_in_plain() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let h = hook(&env).await;
    let p = principal(TENANT_A);
    let svc = svc(&h.app);
    let read = svc.get(&p, &h.f.id, &h.t.id).unwrap();
    let listed = svc.list(&p, &h.f.id).unwrap();
    for text in [
        serde_json::to_string(&read).unwrap(),
        serde_json::to_string(&listed).unwrap(),
        format!("{read:?} {svc:?}"),
    ] {
        assert!(!text.contains(&h.secret), "secret leaked: {text}");
        assert!(!text.contains(&h.secret[6..]), "secret leaked: {text}");
    }
    let TriggerSpec::Webhook(spec) = &read.spec else {
        panic!("webhook expected");
    };
    assert_eq!(spec.secret_fingerprint, webhook::fingerprint(&h.secret));
    h.app.store.flush().unwrap();
    for file in ["state.db", "state.db-wal"] {
        let path = env.dir.path().join(file);
        if let Ok(bytes) = std::fs::read(&path) {
            let hay = String::from_utf8_lossy(&bytes);
            assert!(
                !hay.contains(&h.secret[6..]),
                "{file} holds the secret in plain text"
            );
        }
    }

    let rotated = svc
        .update(
            &p,
            &h.f.id,
            &h.t.id,
            &UpdateTriggerRequest {
                rotate_secret: Some(true),
                ..UpdateTriggerRequest::default()
            },
        )
        .unwrap();
    let new_secret = rotated
        .secret
        .expect("a rotation shows the new secret once");
    assert_ne!(new_secret, h.secret);
    let t = rotated.trigger;
    let now = env.now().timestamp();
    let body = b"{}";
    let old = svc
        .receive_webhook(&t, &delivery(&h.secret, now, body, Some("e1")), body)
        .await
        .unwrap_err();
    assert_eq!(old.http_status(), 401);
    svc.receive_webhook(&t, &delivery(&new_secret, now, body, Some("e1")), body)
        .await
        .unwrap();
    assert!(
        svc.get(&p, &h.f.id, &h.t.id).unwrap().spec
            != TriggerSpec::Cron(CronSpec {
                expression: String::new(),
                timezone: String::new(),
                payload: serde_json::Value::Null,
                missed_run_policy: MissedRunPolicy::Skip,
            })
    );
    // Deleting erases the sealed secret.
    svc.delete(&p, &h.f.id, &h.t.id).unwrap();
    assert!(svc.repository().trigger_secret(&h.t.id).unwrap().is_none());
}

/// Disabled: an unsigned request still gets 401 (nothing about the trigger
/// leaks), a correctly signed one 410. Deleted, unknown and cron triggers are
/// the same 404.
#[tokio::test]
async fn disabled_and_deleted_webhook_triggers_answer_without_leaking_existence() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let h = hook(&env).await;
    let p = principal(TENANT_A);
    let svc = svc(&h.app);
    let t = svc
        .update(
            &p,
            &h.f.id,
            &h.t.id,
            &UpdateTriggerRequest {
                enabled: Some(false),
                ..UpdateTriggerRequest::default()
            },
        )
        .unwrap()
        .trigger;
    let now = env.now().timestamp();
    let body = b"{}";
    let mut unsigned = delivery(&h.secret, now, body, Some("e"));
    unsigned.signature = Some(format!("v1={}", "1".repeat(64)));
    assert_eq!(
        svc.receive_webhook(&t, &unsigned, body)
            .await
            .unwrap_err()
            .http_status(),
        401
    );
    let looked_up = svc.webhook_trigger(&t.id).unwrap();
    assert_eq!(
        svc.receive_webhook(&looked_up, &delivery(&h.secret, now, body, Some("e")), body)
            .await
            .unwrap_err()
            .http_status(),
        410
    );
    // A delivery verified against a stale "enabled" snapshot is refused by
    // the transaction.
    let enabled_snapshot = h.t.clone();
    assert_eq!(
        svc.receive_webhook(
            &enabled_snapshot,
            &delivery(&h.secret, now, body, Some("e")),
            body
        )
        .await
        .unwrap_err()
        .http_status(),
        410
    );
    assert_eq!(stored(&h), (0, 0, 0));
    svc.delete(&p, &h.f.id, &h.t.id).unwrap();
    let cron = create(
        &h.app,
        &h.f,
        &cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip),
    );
    for id in [
        h.t.id.clone(),
        cron.trigger.id.clone(),
        TriggerId::generate(),
    ] {
        let e = svc.webhook_trigger(&id).unwrap_err();
        assert_eq!(e.http_status(), 404);
        assert_eq!(e.to_string(), "not found: trigger not found");
    }
}

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

/// Another tenant cannot see, list, change, delete or read the fires of a
/// trigger, nor create one on a foreign function: all 404. A token without
/// the deploy role cannot create one: 403.
#[tokio::test]
async fn trigger_crud_never_crosses_a_tenant() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, _) = deploy(&app, TENANT_A, "tenant").await;
    let t = create(&app, &f, &webhook_request()).trigger;
    let b = principal(TENANT_B);
    let s = svc(&app);
    assert_eq!(s.get(&b, &f.id, &t.id).unwrap_err().http_status(), 404);
    assert_eq!(s.list(&b, &f.id).unwrap_err().http_status(), 404);
    assert_eq!(
        s.update(
            &b,
            &f.id,
            &t.id,
            &UpdateTriggerRequest {
                enabled: Some(false),
                ..UpdateTriggerRequest::default()
            }
        )
        .unwrap_err()
        .http_status(),
        404
    );
    assert_eq!(s.delete(&b, &f.id, &t.id).unwrap_err().http_status(), 404);
    assert_eq!(
        s.list_fires(&b, &f.id, &t.id, 10)
            .unwrap_err()
            .http_status(),
        404
    );
    assert_eq!(
        s.create(&b, &f.id, &webhook_request())
            .unwrap_err()
            .http_status(),
        404
    );
    // B's own function cannot address A's trigger either.
    let (fb, _) = deploy(&app, TENANT_B, "tenant-b").await;
    assert_eq!(s.get(&b, &fb.id, &t.id).unwrap_err().http_status(), 404);
    let invoke_only = Principal {
        roles: vec![Role::Invoke],
        ..principal(TENANT_A)
    };
    assert_eq!(
        s.create(&invoke_only, &f.id, &webhook_request())
            .unwrap_err()
            .http_status(),
        403
    );
    assert!(
        s.get(&invoke_only, &f.id, &t.id).is_ok(),
        "reads need invoke or deploy"
    );
    assert_eq!(s.get(&principal(TENANT_A), &f.id, &t.id).unwrap().id, t.id);
}

#[tokio::test]
async fn invalid_trigger_specs_are_refused() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let app = env.start();
    let (f, rev) = deploy(&app, TENANT_A, "invalid").await;
    let p = principal(TENANT_A);
    let s = svc(&app);
    let bad = |req: CreateTriggerRequest| s.create(&p, &f.id, &req).unwrap_err().http_status();
    assert_eq!(
        bad(cron_request(
            "61 * * * *",
            "UTC",
            MissedRunPolicyRequest::Skip
        )),
        400
    );
    assert_eq!(
        bad(cron_request(
            "* * * * *",
            "Mars/Base",
            MissedRunPolicyRequest::Skip
        )),
        400
    );
    assert_eq!(
        bad(cron_request(
            "* * * * *",
            "UTC",
            MissedRunPolicyRequest::RunAll { max_runs: 0 }
        )),
        400
    );
    let mut both = cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip);
    both.target = TriggerTargetRequest {
        alias: Some("prod".into()),
        revision_id: Some(rev.id.to_string()),
    };
    assert_eq!(bad(both), 400);
    let mut other_rev = cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip);
    other_rev.target.revision_id = Some(RevisionId::generate().to_string());
    assert_eq!(bad(other_rev), 404);
    let mut kind = webhook_request();
    kind.kind = "queue".into();
    assert_eq!(bad(kind), 400);
    let mut w = webhook_request();
    w.webhook.as_mut().unwrap().max_body_bytes = Some(1_000_000);
    assert_eq!(bad(w), 400);
    let mut w = webhook_request();
    w.webhook.as_mut().unwrap().event_id_header = Some("x-tachyon-webhook-signature".into());
    assert_eq!(bad(w), 400);
    let mut w = webhook_request();
    w.webhook.as_mut().unwrap().source = Some("github".into());
    assert_eq!(bad(w), 400);
    // A pinned revision is fired as that revision.
    let mut pinned = cron_request("* * * * *", "UTC", MissedRunPolicyRequest::Skip);
    pinned.target.revision_id = Some(rev.id.to_string());
    let t = s.create(&p, &f.id, &pinned).unwrap();
    assert_eq!(t.trigger.target.revision_id.as_ref(), Some(&rev.id));
    // The per-function limit.
    let limit = s.config().max_triggers_per_function;
    for _ in 1..limit {
        s.create(&p, &f.id, &webhook_request()).unwrap();
    }
    assert_eq!(bad(webhook_request()), 409);
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

/// `GET /metrics` counts cron fires by result, missed runs by what the policy
/// did, and webhook deliveries by result, with closed label sets only.
#[tokio::test]
async fn trigger_metrics_count_fires_missed_runs_and_webhook_results() {
    let env = Env::new("2026-09-17T00:00:00Z");
    let h = hook(&env).await;
    let app = &h.app;
    create(
        app,
        &h.f,
        &cron_request(
            "* * * * *",
            "UTC",
            MissedRunPolicyRequest::RunAll { max_runs: 2 },
        ),
    );
    env.advance(300);
    let r = app.run_trigger_scheduler().await.unwrap();
    assert_eq!((r.accepted, r.late_run, r.skipped_late), (3, 2, 2), "{r:?}");
    let now = env.now().timestamp();
    let body = b"{}";
    let s = svc(app);
    s.receive_webhook(&h.t, &delivery(&h.secret, now, body, Some("m1")), body)
        .await
        .unwrap();
    s.receive_webhook(&h.t, &delivery(&h.secret, now, body, Some("m1")), body)
        .await
        .unwrap();
    let mut bad = delivery(&h.secret, now, body, Some("m2"));
    bad.signature = Some(format!("v1={}", "0".repeat(64)));
    let _ = s.receive_webhook(&h.t, &bad, body).await;
    let _ = s
        .receive_webhook(
            &h.t,
            &delivery(&h.secret, now - 3600, body, Some("m2")),
            body,
        )
        .await;
    let _ = s.webhook_trigger(&TriggerId::generate());
    let text = app.render_metrics().await;
    for line in [
        "tsls_trigger_scheduler_owner 1",
        r#"tsls_trigger_cron_fires_total{result="accepted"} 3"#,
        r#"tsls_trigger_cron_fires_total{result="deferred"} 0"#,
        r#"tsls_trigger_cron_missed_runs_total{action="run"} 2"#,
        r#"tsls_trigger_cron_missed_runs_total{action="skipped"} 2"#,
        r#"tsls_trigger_webhook_deliveries_total{result="accepted"} 1"#,
        r#"tsls_trigger_webhook_deliveries_total{result="replayed"} 1"#,
        r#"tsls_trigger_webhook_deliveries_total{result="signature_refused"} 1"#,
        r#"tsls_trigger_webhook_deliveries_total{result="timestamp_refused"} 1"#,
        r#"tsls_trigger_webhook_deliveries_total{result="not_found"} 1"#,
    ] {
        assert!(text.contains(line), "missing `{line}` in\n{text}");
    }
    assert!(!text.contains(h.t.id.as_str()), "no trigger id label");
    assert!(!text.contains("\"m1\""), "no event id label");
}
