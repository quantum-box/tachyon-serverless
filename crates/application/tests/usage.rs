//! Usage metering end to end through the application (PLT-4642,
//! docs/adr/0012): host-measured segments, retry vs first attempt, timeout,
//! idempotent replay, journal bound and availability, collector restart on the
//! same data_dir, wall-clock skew, tenant scoping and the exclusion of
//! guest-reported values from rating.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_util::codec::{FramedRead, FramedWrite};

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ErrorCode, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_application::usage::{JournalRefusal, UsageQuery};
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, AttemptKind, Clock, EventKind, Function, FunctionRevision, Measurement,
    RevisionStatus, TenantId, Timestamp, UsageEvent, UsageEventType, UsageOutcome,
};
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message, encode_message,
};
use tachyon_serverless_provider_fake::{
    CustomScriptContext, FakeExecutionProvider, FakeGuestScript, ScriptFuture,
};
use tachyon_serverless_provider_port::{EnvironmentStats, Principal, Role};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

/// Scheduling noise allowed on top of a known duration.
const TOLERANCE_MS: u64 = 400;

fn config(data_dir: &std::path::Path, extra: &str) -> GatewayConfig {
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
subject = "dev-a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "tok-b"
tenant_id = "{TENANT_B}"
subject = "dev-b"
roles = ["deploy", "invoke"]

[invoke]
cancel_grace_ms = 100

{extra}
"#,
        data = data_dir.display()
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

struct Harness {
    app: Arc<Application>,
    fake: Arc<FakeExecutionProvider>,
    dir: tempfile::TempDir,
}

fn boot(
    dir: tempfile::TempDir,
    fake: Arc<FakeExecutionProvider>,
    extra: &str,
    clock: Option<Arc<dyn Clock>>,
) -> Harness {
    let mut options = BootstrapOptions {
        persist_state: true,
        ..BootstrapOptions::default()
    };
    if let Some(clock) = clock {
        options.clock = clock;
    }
    let app =
        Application::bootstrap_with(config(dir.path(), extra), fake.clone(), options).unwrap();
    Harness { app, fake, dir }
}

fn harness(extra: &str) -> Harness {
    let fake = Arc::new(FakeExecutionProvider::new());
    fake.set_default_script(Some(FakeGuestScript::SlowEchoForever));
    boot(tempfile::tempdir().unwrap(), fake, extra, None)
}

async fn deploy(
    h: &Harness,
    p: &Principal,
    name: &str,
    timeout: u32,
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
        resources: ResourcesRequest {
            memory_mib: 512,
            cpu_millis: 1000,
            ..ResourcesRequest::default()
        },
        execution: ExecutionRequest {
            timeout_seconds: timeout,
            initialization_timeout_seconds: 10,
            ..ExecutionRequest::default()
        },
        egress: None,
        egress_allow: Vec::new(),
        env_vars: Vec::new(),
        secrets: Vec::new(),
        description: "usage test".into(),
        publish_to_prod: true,
        required_region: None,
    };
    let rev = h.app.revisions.create(p, &function.id, &req).await.unwrap();
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
        .get(p, &function.id, &AliasName::default_alias())
        .unwrap();
    assert_eq!(alias.revision_id, rev.id);
    (function, rev)
}

fn request(p: &Principal, f: &Function, payload: serde_json::Value) -> InvokeRequest {
    InvokeRequest {
        principal: p.clone(),
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

fn settled_events(h: &Harness) -> Vec<UsageEvent> {
    h.app
        .usage
        .events()
        .into_iter()
        .filter(|e| e.event_type == UsageEventType::AttemptSettled)
        .collect()
}

fn wide_query() -> UsageQuery {
    let now = chrono::Utc::now();
    UsageQuery {
        from: Some(now - chrono::Duration::days(3)),
        to: Some(now + chrono::Duration::days(3)),
        group_by: Some("function".into()),
        function_id: None,
    }
}

/// Known-duration sample: a 300 ms handler is metered by the host as
/// 300 ms (+ scheduling noise), every segment is `host_measured`, and the
/// provisional charge follows from exactly those numbers.
#[tokio::test]
async fn a_known_duration_handler_is_metered_by_the_host() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "known", 30).await;
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({"sleep_ms": 300})))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);

    let settled = settled_events(&h);
    assert_eq!(settled.len(), 1);
    let e = &settled[0];
    assert_eq!(e.meter_version, 2);
    assert_eq!(e.attempt_kind, Some(AttemptKind::First));
    assert_eq!(e.outcome, Some(UsageOutcome::Succeeded));
    assert_eq!(e.function_id.as_ref(), Some(&f.id));
    let handler = e.segments.handler_ms;
    assert_eq!(handler.measurement, Measurement::HostMeasured);
    let ms = handler.value.unwrap();
    assert!(
        (300..300 + TOLERANCE_MS).contains(&ms),
        "handler_ms {ms} for a 300 ms handler"
    );
    for (name, m) in e.segments.named() {
        assert_eq!(m.measurement, Measurement::HostMeasured, "{name}");
    }
    assert_eq!(
        e.bytes.request_bytes.value,
        Some(
            serde_json::to_vec(&serde_json::json!({"sleep_ms": 300}))
                .unwrap()
                .len() as u64
        )
    );
    // The guest's own claim is kept apart.
    assert_eq!(e.guest_reported.guest_handler_ms, Some(2));
    assert_eq!(e.resources.requested_cpu_millis, 1000);
    assert_eq!(
        e.resources.cgroup_cpu_usec.measurement,
        Measurement::Unknown
    );

    // Journal → ledger → rating.
    let report = h.app.collect_usage().unwrap();
    assert!(report.inserted >= 4, "{report:?}");
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert!(r.provisional && r.not_an_invoice && !r.billing_enabled);
    assert_eq!(r.lines.len(), 1);
    let u = &r.totals.usage;
    assert_eq!((u.invocations, u.attempts, u.retries), (1, 1, 0));
    assert_eq!(u.outcomes.succeeded, 1);
    assert_eq!(u.segments_ms.handler_ms, ms);
    let expected_billable = ms + e.segments.user_init_ms.value.unwrap();
    assert_eq!(u.billable_ms, expected_billable);
    assert_eq!(u.vcpu_milli_ms, expected_billable * 1000);
    assert_eq!(u.mib_ms, expected_billable * 512);
    let table = h.app.usage_meter.price_table();
    let c = r.totals.provisional_charges_micros;
    assert_eq!(
        c.vcpu,
        tachyon_serverless_application::usage::rating::charge(
            u128::from(expected_billable * 1000),
            table.unit_prices_micros.vcpu_second,
            1_000_000
        )
    );
    assert_eq!(r.totals.unmetered.attempts, 0);
    assert_eq!(r.totals.cost.environments_stopped, 1);
    assert_eq!(
        r.totals.cost.cgroup_cpu_unknown, 1,
        "the fake reports no cgroup"
    );
    assert_eq!(r.price_table.version, table.version);
}

/// A timeout is recorded as `timeout` with the host-measured time until the
/// host stopped waiting, not as a failure or a success.
#[tokio::test]
async fn a_timeout_is_recorded_as_timeout_with_the_host_measured_duration() {
    let h = harness("");
    h.fake
        .set_default_script(Some(FakeGuestScript::HangForever));
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "slow", 1).await;
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap();
    assert!(!out.succeeded());
    let settled = settled_events(&h);
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].outcome, Some(UsageOutcome::Timeout));
    let ms = settled[0].segments.handler_ms.value.unwrap();
    assert!((1000..1000 + TOLERANCE_MS).contains(&ms), "{ms}");
    h.app.collect_usage().unwrap();
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.totals.usage.outcomes.timeout, 1);
    assert_eq!(r.totals.usage.outcomes.succeeded, 0);
}

/// A client retry with the same idempotency key is a replay, not a second
/// execution: metered once.
#[tokio::test]
async fn an_idempotent_replay_is_metered_once() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "replayed", 30).await;
    let mut req = request(&a, &f, serde_json::json!({"n": 1}));
    req.idempotency_key = Some("usage-key-1".into());
    let first = h.app.invoke.invoke(req.clone()).await.unwrap();
    let second = h.app.invoke.invoke(req).await.unwrap();
    assert!(second.replayed);
    assert_eq!(first.invocation().id, second.invocation().id);
    assert_eq!(settled_events(&h).len(), 1);
    h.app.collect_usage().unwrap();
    // A second collection finds nothing new.
    assert_eq!(h.app.collect_usage().unwrap().read, 0);
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.totals.usage.invocations, 1);
}

/// Collector crash and restart on the same data_dir: events still in the
/// journal are delivered by the next process, a batch the ledger committed
/// before the crash is delivered again and dropped by event id, and the report
/// is identical to one without any crash.
#[tokio::test]
async fn a_collector_restart_on_the_same_data_dir_never_double_counts() {
    let h = harness("[usage]\ncollect_batch = 3\n");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "restart", 30).await;
    for n in 0..4 {
        let out = h
            .app
            .invoke
            .invoke(request(&a, &f, serde_json::json!({"n": n})))
            .await
            .unwrap();
        assert!(out.succeeded());
    }
    let pending = h.app.usage_meter.journal().status().pending_events;
    assert!(pending >= 4 * 4, "{pending}");
    // Crash window: the ledger accepted a batch, the cursor did not move.
    let (_, _, batch) = h.app.usage_meter.journal().read_batch(3).unwrap();
    let events: Vec<UsageEvent> = batch.into_iter().map(|e| e.event).collect();
    h.app
        .usage_meter
        .ledger()
        .accept(&events, chrono::Utc::now())
        .unwrap();
    let Harness { app, fake, dir } = h;
    app.stop_dispatcher();
    drop(app);

    let h = boot(dir, fake, "[usage]\ncollect_batch = 3\n", None);
    assert_eq!(h.app.usage_meter.journal().status().pending_events, pending);
    let report = h.app.collect_usage().unwrap();
    assert_eq!(report.read, pending);
    assert_eq!(report.duplicates, 3);
    assert_eq!(report.inserted, pending - 3);
    assert_eq!(h.app.usage_meter.ledger().stats().unwrap().events, pending);
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.totals.usage.invocations, 4);
    assert_eq!(r.totals.usage.attempts, 4);
    // Replaying everything once more changes nothing.
    assert_eq!(h.app.collect_usage().unwrap().read, 0);
}

/// A clock that starts `offset` away from the real time and runs backwards by
/// `step_ms` on every read: wall time that is wrong *and* jumps.
struct SkewedClock {
    base: Timestamp,
    calls: AtomicI64,
    step_ms: i64,
}

impl Clock for SkewedClock {
    fn now(&self) -> Timestamp {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        self.base - chrono::Duration::milliseconds(n * self.step_ms)
    }
}

/// Wall-clock skew: with a host clock 36 h behind that also jumps backwards
/// on every read, every quantity is still the monotonic one; only the day an
/// event lands on and the recorded skew change.
#[tokio::test]
async fn wall_clock_skew_does_not_change_quantities() {
    let fake = Arc::new(FakeExecutionProvider::new());
    fake.set_default_script(Some(FakeGuestScript::SlowEchoForever));
    let clock = Arc::new(SkewedClock {
        base: chrono::Utc::now() - chrono::Duration::hours(36),
        calls: AtomicI64::new(0),
        step_ms: 25,
    });
    let h = boot(tempfile::tempdir().unwrap(), fake, "", Some(clock));
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "skewed", 30).await;
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({"sleep_ms": 250})))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    let e = settled_events(&h).remove(0);
    let ms = e.segments.handler_ms.value.unwrap();
    assert!((250..250 + TOLERANCE_MS).contains(&ms), "{ms}");
    // The environment lifetime is a wall-clock difference: with this clock it
    // is nonsense, which is exactly why rating never reads it.
    h.app.collect_usage().unwrap();
    let skew = h
        .app
        .usage_meter
        .ledger()
        .recorded_skew_ms(&e.event_id)
        .unwrap();
    assert!(
        skew < 0,
        "the collector's clock read earlier than the event: {skew}"
    );
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.totals.usage.segments_ms.handler_ms, ms);
    assert_eq!(
        r.totals.usage.billable_ms,
        ms + e.segments.user_init_ms.value.unwrap()
    );
}

/// Journal full: new invocations are refused with `usage_journal_full` (503)
/// before anything is recorded, and accepted again once the collector caught
/// up. Admitted work never loses its events.
#[tokio::test]
async fn a_full_journal_refuses_new_invocations_fail_closed() {
    let h = harness("[usage]\njournal_max_events = 30\nadmission_headroom_events = 12\n");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "bounded", 30).await;
    let mut accepted = 0;
    let refusal = loop {
        match h
            .app
            .invoke
            .invoke(request(&a, &f, serde_json::json!({"n": accepted})))
            .await
        {
            Ok(out) => {
                assert!(out.succeeded());
                accepted += 1;
                assert!(accepted < 30, "never refused");
            }
            Err(e) => break e,
        }
    };
    assert!(accepted >= 2, "{accepted}");
    let AppError::UsageJournal { refusal: r, .. } = &refusal else {
        panic!("expected a usage journal refusal, got {refusal:?}");
    };
    assert_eq!(*r, JournalRefusal::Full);
    assert_eq!(refusal.code(), ErrorCode::UsageJournalFull);
    assert_eq!(refusal.http_status(), 503);
    let body = refusal.to_api_body(None);
    assert_eq!(body.error.reason.as_deref(), Some("usage_journal_full"));
    assert_eq!(
        body.error.error_type.as_deref(),
        Some("Host.UsageJournalFull")
    );
    // Nothing of the refused invocation was recorded.
    let invocations = h
        .app
        .repos
        .invocations
        .list_by_function(&f.id, 100)
        .unwrap();
    assert_eq!(invocations.len(), accepted);
    // Every admitted invocation's events made it.
    assert_eq!(h.app.usage_meter.journal().unjournaled_for(TENANT_A), 0);
    assert!(!h.app.usage_meter.status().accepting);

    h.app.collect_usage().unwrap();
    assert!(h.app.usage_meter.status().accepting);
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({"after": true})))
        .await
        .unwrap();
    assert!(out.succeeded());
}

/// Journal unavailable: the same fail-closed refusal, with its own reason.
#[tokio::test]
async fn an_unavailable_journal_refuses_new_invocations() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "unavailable", 30).await;
    h.app.usage_meter.journal().force_unavailable(true);
    let err = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::UsageJournalFull);
    assert_eq!(
        err.to_api_body(None).error.reason.as_deref(),
        Some("usage_journal_unavailable")
    );
    assert!(
        h.app
            .repos
            .invocations
            .list_by_function(&f.id, 10)
            .unwrap()
            .is_empty()
    );
    h.app.usage_meter.journal().force_unavailable(false);
    assert!(
        h.app
            .invoke
            .invoke(request(&a, &f, serde_json::json!({})))
            .await
            .unwrap()
            .succeeded()
    );
}

/// The dev-only `accept_unmetered` policy keeps serving, and what the journal
/// cannot take is counted as unjournaled for the tenant — never estimated.
#[tokio::test]
async fn accept_unmetered_counts_what_it_could_not_meter() {
    let h = harness(
        "[usage]\njournal_max_events = 12\nadmission_headroom_events = 6\n\
         on_journal_full = \"accept_unmetered\"\n",
    );
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "unmetered", 30).await;
    for n in 0..5 {
        let out = h
            .app
            .invoke
            .invoke(request(&a, &f, serde_json::json!({"n": n})))
            .await
            .unwrap();
        assert!(out.succeeded());
    }
    let lost = h.app.usage_meter.journal().unjournaled_for(TENANT_A);
    assert!(lost > 0, "the bound was crossed and counted");
    h.app.collect_usage().unwrap();
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.unjournaled_events, lost);
    assert!(
        r.totals.usage.invocations < 5,
        "unjournaled work is not in the report"
    );
    // Tenant B has lost nothing.
    let rb = h
        .app
        .usage_meter
        .report(&principal(TENANT_B), &wide_query())
        .unwrap();
    assert_eq!(rb.unjournaled_events, 0);
}

/// Tenant scoping: B never sees A's usage, not even with A's function id.
#[tokio::test]
async fn usage_reports_are_tenant_scoped() {
    let h = harness("");
    let a = principal(TENANT_A);
    let b = principal(TENANT_B);
    let (fa, _) = deploy(&h, &a, "tenant-a", 30).await;
    let (fb, _) = deploy(&h, &b, "tenant-b", 30).await;
    for (p, f) in [(&a, &fa), (&a, &fa), (&b, &fb)] {
        assert!(
            h.app
                .invoke
                .invoke(request(p, f, serde_json::json!({})))
                .await
                .unwrap()
                .succeeded()
        );
    }
    h.app.collect_usage().unwrap();
    let ra = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    let rb = h.app.usage_meter.report(&b, &wide_query()).unwrap();
    assert_eq!(ra.tenant_id, TENANT_A);
    assert_eq!(ra.totals.usage.invocations, 2);
    assert_eq!(rb.totals.usage.invocations, 1);
    assert!(
        ra.lines
            .iter()
            .all(|l| l.function_id.as_deref() == Some(fa.id.as_str()))
    );
    let stolen = h
        .app
        .usage_meter
        .report(
            &b,
            &UsageQuery {
                function_id: Some(fa.id.clone()),
                ..wide_query()
            },
        )
        .unwrap();
    assert!(stolen.lines.is_empty());
    assert_eq!(stolen.totals.usage.attempts, 0);
}

/// A guest that claims an hour of handler time and a long initialization is
/// billed for what the host measured.
#[tokio::test]
async fn guest_reported_times_never_reach_a_charge() {
    let h = harness("");
    h.fake.set_default_script(Some(lying_guest()));
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "liar", 30).await;
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    let e = settled_events(&h).remove(0);
    assert_eq!(e.guest_reported.guest_handler_ms, Some(3_600_000));
    assert_eq!(e.guest_reported.guest_init_ms, Some(9_000_000));
    let host = e.segments.handler_ms.value.unwrap();
    assert!(host < TOLERANCE_MS + 50, "{host}");
    h.app.collect_usage().unwrap();
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.totals.guest_reported.guest_handler_ms, 3_600_000);
    assert!(r.totals.usage.billable_ms < 2 * TOLERANCE_MS);
    // An hour at 1 vCPU would be 3600 × the vCPU-second price.
    let hour = 3_600
        * h.app
            .usage_meter
            .price_table()
            .unit_prices_micros
            .vcpu_second;
    assert!(r.totals.provisional_charges_micros.vcpu < hour / 100);
}

fn lying_guest() -> FakeGuestScript {
    scripted_guest(9_000_000, 3_600_000, Arc::new(|_| {}))
}

/// A guest that reports `init_ms` / `handler_ms` about itself and calls
/// `on_boot` with its environment id first.
fn scripted_guest(
    init_ms: u64,
    handler_ms: u64,
    on_boot: Arc<dyn Fn(&tachyon_serverless_domain::EnvironmentId) + Send + Sync>,
) -> FakeGuestScript {
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        on_boot(&ctx.environment_id);
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "lying-guest".into(),
                environment_id: ctx.environment_id.to_string(),
                guest_boot_id: Some("liar-boot".into()),
                architecture: "aarch64".into(),
            };
            if writer.send(encode_message(&hello).unwrap()).await.is_err() {
                return;
            }
            if !matches!(reader.next().await, Some(Ok(_))) {
                return;
            }
            let ready = GuestMessage::Ready { init_ms };
            if writer.send(encode_message(&ready).unwrap()).await.is_err() {
                return;
            }
            while let Some(Ok(frame)) = reader.next().await {
                if let Ok(HostMessage::Invoke {
                    attempt_id, epoch, ..
                }) = decode_message::<HostMessage>(&frame)
                {
                    let response = GuestMessage::Response {
                        attempt_id,
                        epoch,
                        payload: serde_json::json!({"ok": true}),
                        handler_ms: Some(handler_ms),
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

/// Provider-reported host usage (`ExecutionProvider::environment_stats`, the
/// VMM cgroup on Firecracker) sampled before terminate reaches host cost,
/// never a charge.
#[tokio::test]
async fn provider_reported_cgroup_usage_is_cost_not_price() {
    let fake = Arc::new(FakeExecutionProvider::new());
    let weak = Arc::downgrade(&fake);
    fake.set_default_script(Some(scripted_guest(
        1,
        1,
        Arc::new(move |env| {
            if let Some(fake) = weak.upgrade() {
                fake.set_environment_stats(
                    env,
                    EnvironmentStats {
                        cpu_seconds: Some(0.123_456),
                        memory_current_bytes: Some(1024),
                        memory_peak_bytes: Some(64 * 1024 * 1024),
                        scope: "fake".into(),
                    },
                );
            }
        }),
    )));
    let h = boot(tempfile::tempdir().unwrap(), fake, "", None);
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "cgroup", 30).await;
    assert!(
        h.app
            .invoke
            .invoke(request(&a, &f, serde_json::json!({})))
            .await
            .unwrap()
            .succeeded()
    );
    let stopped: Vec<_> = h
        .app
        .usage
        .events()
        .into_iter()
        .filter(|e| e.event_type == UsageEventType::EnvironmentStopped)
        .collect();
    assert_eq!(stopped.len(), 1);
    let cpu = stopped[0].resources.cgroup_cpu_usec;
    assert_eq!(cpu.measurement, Measurement::ProviderReported);
    assert_eq!(cpu.value, Some(123_456));
    h.app.collect_usage().unwrap();
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.totals.cost.cgroup_cpu_usec, 123_456);
    assert_eq!(r.totals.cost.cgroup_memory_peak_bytes_max, 64 * 1024 * 1024);
    assert_eq!(r.totals.cost.cgroup_cpu_unknown, 0);
    // The same invocation without cgroup stats is charged the same.
    let without = harness("");
    let (g, _) = deploy(&without, &a, "no-cgroup", 30).await;
    assert!(
        without
            .app
            .invoke
            .invoke(request(&a, &g, serde_json::json!({})))
            .await
            .unwrap()
            .succeeded()
    );
    without.app.collect_usage().unwrap();
    let r2 = without.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(
        r.totals.provisional_charges_micros.invocations,
        r2.totals.provisional_charges_micros.invocations
    );
    assert_eq!(r2.totals.cost.cgroup_cpu_usec, 0);
}

/// An environment that fails to initialize is accounted once, as an
/// environment without an attempt: host cost, no attempt, no charge.
#[tokio::test]
async fn a_failed_initialization_is_accounted_without_an_attempt() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "init-fails", 30).await;
    h.fake.set_default_script(Some(FakeGuestScript::InitError {
        message: "boom".into(),
    }));
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap();
    assert!(!out.succeeded());
    assert!(settled_events(&h).is_empty(), "no attempt was dispatched");
    let stopped: Vec<_> = h
        .app
        .usage
        .events()
        .into_iter()
        .filter(|e| e.event_type == UsageEventType::EnvironmentStopped)
        .collect();
    assert_eq!(stopped.len(), 1, "accounted exactly once");
    assert_eq!(stopped[0].outcome, Some(UsageOutcome::Failed));
    assert!(stopped[0].attempt_id.is_none());
    h.app.collect_usage().unwrap();
    let r = h.app.usage_meter.report(&a, &wide_query()).unwrap();
    assert_eq!(r.totals.usage.attempts, 0);
    assert_eq!(r.totals.provisional_charges_micros.total, 0);
    assert_eq!(r.totals.cost.environments_stopped, 1);
}
