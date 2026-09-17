//! Budget reservation, limits, alerts and fail-closed admission through the
//! application (PLT-4643, docs/adr/0016): concurrent invocations never
//! reserve beyond a hard limit, the excess returns after settlement, timeout /
//! cancel / lost runs, a stalled collector, live limit changes (running work
//! continues, queued work re-checks), alerts apart from the stop, distinct
//! refusal reasons and tenant isolation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ErrorCode, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_application::budget::{
    AlertLimits, NewReservation, ReservationState, ReserveOutcome,
};
use tachyon_serverless_application::usage::UsageQuery;
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, EventKind, Function, FunctionRevision, RevisionStatus, TenantId,
};
use tachyon_serverless_provider_fake::{FakeExecutionProvider, FakeGuestScript};
use tachyon_serverless_provider_port::{Principal, Role};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";
const TENANT_C: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzc";

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

[[identity.tokens]]
token = "tok-c"
tenant_id = "{TENANT_C}"
subject = "dev-c"
roles = ["deploy", "invoke"]

[invoke]
cancel_grace_ms = 100

[budget]
enabled = true
file = "{data}/budgets.toml"
max_unsettled_age_seconds = 1
expiry_grace_seconds = 1

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
    edits: AtomicU64,
}

impl Harness {
    /// Replace the budget file. The control plane publishes the change on
    /// the next configuration read (its change marker moves).
    fn set_budgets(&self, body: &str) {
        let n = self.edits.fetch_add(1, Ordering::SeqCst);
        // The edit counter changes the size too, so the change is seen even
        // on a file system with coarse modification times.
        std::fs::write(
            self.dir.path().join("budgets.toml"),
            format!("# edit {}\n{body}", "#".repeat(n as usize + 1)),
        )
        .unwrap();
    }
}

/// Budgets: A and B by argument, C has none (and there is no default).
fn budgets(a: &str, b: &str) -> String {
    format!(
        "[[tenants]]\ntenant_id = \"{TENANT_A}\"\n{a}\n\n[[tenants]]\ntenant_id = \"{TENANT_B}\"\n{b}\n"
    )
}

fn harness(extra: &str) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("budgets.toml"), budgets("", "")).unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    fake.set_default_script(Some(FakeGuestScript::SlowEchoForever));
    let app = Application::bootstrap_with(
        config(dir.path(), extra),
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
        dir,
        edits: AtomicU64::new(0),
    }
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
        description: "budget test".into(),
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

/// The reservation of one invocation of `rev` with `payload`.
fn max_of(h: &Harness, rev: &FunctionRevision, payload: &serde_json::Value) -> u64 {
    let c = &h.app.config.capacity;
    let exec = &rev.spec.execution;
    let window = (u64::from(exec.timeout_seconds)
        + u64::from(exec.initialization_timeout_seconds)
        + c.queue_timeout_seconds)
        * 1000;
    h.app
        .budget
        .max_charge_for(
            rev,
            serde_json::to_vec(payload).unwrap().len() as u64,
            window,
            c.queue_timeout_seconds * 1000,
        )
        .total_micros
}

fn budget_refusal(e: &AppError) -> Option<(ErrorCode, String, String)> {
    let body = e.to_api_body(None).error;
    Some((body.code, body.reason?, body.error_type?))
}

fn assert_budget(e: &AppError, code: ErrorCode, error_type: &str) {
    let (c, reason, et) = budget_refusal(e).unwrap_or_else(|| panic!("not a budget refusal: {e}"));
    assert_eq!(
        (c, reason.as_str(), et.as_str()),
        (code, "budget", error_type),
        "{e}"
    );
}

/// Many concurrent invocations against a hard limit that fits exactly four
/// maximum charges: four run, the rest are refused with reason `budget`
/// before admission counts them; after settlement the excess comes back, the
/// settled amount matches the ledger, and new invocations are admitted again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_invocations_never_reserve_beyond_the_hard_limit_and_the_excess_returns() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, rev) = deploy(&h, &a, "parallel", 5).await;
    let payload = serde_json::json!({"sleep_ms": 400});
    let max = max_of(&h, &rev, &payload);
    let limit = 4 * max + max / 2;
    h.set_budgets(&budgets(&format!("hard_limit_micros = {limit}"), ""));
    let arrivals_before = h.app.admission.metrics_view().counters.arrivals;

    let mut tasks = Vec::new();
    for _ in 0..12 {
        let app = h.app.clone();
        let req = request(&a, &f, payload.clone());
        tasks.push(tokio::spawn(async move { app.invoke.invoke(req).await }));
    }
    let (mut ok, mut refused) = (0, 0);
    for t in tasks {
        match t.await.unwrap() {
            Ok(out) => {
                assert!(out.succeeded(), "{:?}", out.invocation().status);
                ok += 1;
            }
            Err(e) => {
                assert_budget(&e, ErrorCode::BudgetExhausted, "Host.BudgetExhausted");
                assert_eq!(e.http_status(), 429);
                refused += 1;
            }
        }
    }
    assert_eq!((ok, refused), (4, 8));
    // A budget refusal never reached the queue or took a capacity grant.
    assert_eq!(
        h.app.admission.metrics_view().counters.arrivals - arrivals_before,
        4
    );
    let report = h.app.budget.report(&a, None).await.unwrap();
    assert_eq!(report.tenant.reserved_micros, 4 * max);
    assert!(report.tenant.committed_micros <= limit);
    assert_eq!(report.tenant.refusals, 8);

    // Collection settles the four runs at their rated charge.
    h.app.collect_usage().unwrap();
    let report = h.app.budget.report(&a, None).await.unwrap();
    let t = &report.tenant;
    assert_eq!(t.reserved_micros, 0, "{t:?}");
    assert_eq!(t.unmetered_hold_micros, 0);
    assert_eq!(t.settlements, 4);
    assert!(t.settled_micros > 0 && t.settled_micros < 4 * max, "{t:?}");
    assert_eq!(t.remaining_micros, Some(limit - t.settled_micros));
    // Against the ledger: the usage report rates the same events once per
    // line; per-run rounding differs by at most ½ per component per run.
    let now = chrono::Utc::now();
    let usage = h
        .app
        .usage_meter
        .report(
            &a,
            &UsageQuery {
                from: Some(now - chrono::Duration::days(1)),
                to: Some(now + chrono::Duration::days(1)),
                group_by: Some("none".into()),
                function_id: None,
            },
        )
        .unwrap();
    let ledger_total = usage.totals.provisional_charges_micros.total;
    assert!(
        t.settled_micros.abs_diff(ledger_total) <= 4 * 4,
        "settled {} vs ledger {ledger_total}",
        t.settled_micros
    );
    h.app.budget.store().verify_totals().unwrap();

    // The returned budget admits new work.
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, payload.clone()))
        .await
        .unwrap();
    assert!(out.succeeded());
}

/// A timed-out and a cancelled invocation settle at what was measured (not
/// at their maximum), and the budget ends balanced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_and_cancel_settle_at_their_measured_charge() {
    let h = harness("");
    let a = principal(TENANT_A);
    h.fake
        .set_default_script(Some(FakeGuestScript::HangForever));
    let (f, rev) = deploy(&h, &a, "hang", 1).await;
    let max = max_of(&h, &rev, &serde_json::json!({}));
    h.set_budgets(&budgets(&format!("hard_limit_micros = {}", 10 * max), ""));

    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap();
    assert!(!out.succeeded());

    let (f2, _) = deploy(&h, &a, "hang-long", 30).await;
    let app = h.app.clone();
    let req = request(&a, &f2, serde_json::json!({}));
    let running = tokio::spawn(async move { app.invoke.invoke(req).await });
    let id = loop {
        let list = h.app.history.list_invocations(&a, &f2.id, 10).unwrap();
        if let Some(d) = list.first()
            && d.invocation.status.name() == "running"
        {
            break d.invocation.id.clone();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    h.app.invoke.cancel(&a, &id).await.unwrap();
    let out = running.await.unwrap().unwrap();
    assert_eq!(out.invocation().status.name(), "cancelled");

    h.app.collect_usage().unwrap();
    let rows = h
        .app
        .budget
        .store()
        .reservations_of(
            TENANT_A,
            &tachyon_serverless_application::budget::period_of(&chrono::Utc::now()),
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert_eq!(r.state, ReservationState::Settled, "{r:?}");
        assert_eq!(r.held_micros, 0, "{r:?}");
        assert!(
            r.settled_micros > 0 && r.settled_micros < r.reserved_micros,
            "{r:?}"
        );
        assert_eq!(r.attempts.len(), 1);
    }
    h.app.budget.store().verify_totals().unwrap();
}

/// A run that never reports back (its gateway died mid-run) keeps its
/// reservation until its deadline + grace, then expires to an unmetered hold
/// that still counts against the limit.
#[tokio::test]
async fn a_run_that_never_reports_back_expires_to_an_unmetered_hold() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "lost", 5).await;
    h.set_budgets(&budgets("hard_limit_micros = 1000000", ""));
    let now = chrono::Utc::now();
    let period = tachyon_serverless_application::budget::period_of(&now);
    let lost = NewReservation {
        reservation_id: "inv_lost".into(),
        tenant_id: TENANT_A.into(),
        function_id: f.id.to_string(),
        invocation_id: "inv_lost".into(),
        period: period.clone(),
        amount_micros: 700_000,
        expires_at: now + chrono::Duration::milliseconds(300),
        price_table_version: h.app.usage_meter.price_table().version.clone(),
        config_generation: 1,
        tenant_limit: Some(1_000_000),
        function_limit: None,
    };
    assert_eq!(
        h.app.budget.store().reserve(&lost, now).unwrap(),
        ReserveOutcome::Reserved
    );
    // Before the expiry nothing happens to it.
    h.app.collect_usage().unwrap();
    assert_eq!(
        h.app.budget.store().get("inv_lost").unwrap().unwrap().state,
        ReservationState::Reserved
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    h.app.collect_usage().unwrap();
    let row = h.app.budget.store().get("inv_lost").unwrap().unwrap();
    assert_eq!(row.state, ReservationState::Expired);
    assert_eq!((row.settled_micros, row.held_micros), (0, 700_000));
    let report = h.app.budget.report(&a, None).await.unwrap();
    assert_eq!(report.tenant.unmetered_hold_micros, 700_000);
    assert_eq!(report.tenant.remaining_micros, Some(300_000));
    // A duplicate expiry or a late settlement changes nothing.
    assert!(
        !h.app
            .budget
            .store()
            .expire("inv_lost", chrono::Utc::now(), &AlertLimits::default())
            .unwrap()
            .changed
    );
    h.app.budget.store().verify_totals().unwrap();
}

/// A stopped collector: once a finished run waited longer than
/// `max_unsettled_age_seconds`, new invocations are refused with
/// `Host.BudgetUnknown` (503); the invocation already running is unaffected;
/// when the collector catches up, admission resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_collector_fails_closed_and_admission_resumes_after_it_catches_up() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "stall", 5).await;
    h.set_budgets(&budgets("hard_limit_micros = 100000000", ""));
    let pause = h.dir.path().join("usage").join("collector.pause");
    std::fs::write(&pause, b"").unwrap();

    let first = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap();
    assert!(first.succeeded());
    // A long one starts before the threshold passes and keeps running.
    let app = h.app.clone();
    let req = request(&a, &f, serde_json::json!({"sleep_ms": 1500}));
    let running = tokio::spawn(async move { app.invoke.invoke(req).await });
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // The paused collector delivers nothing.
    assert!(h.app.collect_usage().is_err());
    let err = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(&err, ErrorCode::BudgetUnavailable, "Host.BudgetUnknown");
    assert_eq!(err.http_status(), 503);
    let status = h.app.budget.status();
    assert!(status.collector_stalled && !status.accepting);
    // Tenant B is refused too: the budget of nobody can be known.
    let b = principal(TENANT_B);
    let (fb, _) = deploy(&h, &b, "stall-b", 5).await;
    let err = h
        .app
        .invoke
        .invoke(request(&b, &fb, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(&err, ErrorCode::BudgetUnavailable, "Host.BudgetUnknown");
    // Already started work is tracked to its end.
    assert!(running.await.unwrap().unwrap().succeeded());

    std::fs::remove_file(&pause).unwrap();
    h.app.collect_usage().unwrap();
    assert!(h.app.budget.status().accepting);
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap();
    assert!(out.succeeded());
    h.app.collect_usage().unwrap();
    let report = h.app.budget.report(&a, None).await.unwrap();
    assert_eq!(report.tenant.settlements, 3);
    assert_eq!(report.tenant.reserved_micros, 0);
}

/// Lowering a hard limit below what is reserved stops new invocations but
/// not the running one; raising it resumes admission. Each change is a new,
/// higher configuration generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lowering_a_limit_stops_new_work_but_not_running_work_and_raising_resumes() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, rev) = deploy(&h, &a, "live", 5).await;
    let max = max_of(&h, &rev, &serde_json::json!({"sleep_ms": 800}));
    h.set_budgets(&budgets(&format!("hard_limit_micros = {}", 10 * max), ""));
    let _ = h.app.budget.report(&a, None).await.unwrap();

    let app = h.app.clone();
    let req = request(&a, &f, serde_json::json!({"sleep_ms": 800}));
    let running = tokio::spawn(async move { app.invoke.invoke(req).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = h.app.budget.report(&a, None).await.unwrap();
    assert_eq!(before.tenant.active_reservations, 1);

    h.set_budgets(&budgets(&format!("hard_limit_micros = {}", max / 2), ""));
    let err = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(&err, ErrorCode::BudgetExhausted, "Host.BudgetExhausted");
    let lowered = h.app.budget.report(&a, None).await.unwrap();
    assert!(lowered.config_generation > before.config_generation);
    assert_eq!(lowered.tenant.hard_limit_micros, Some(max / 2));
    assert_eq!(lowered.tenant.remaining_micros, Some(0));
    assert!(!lowered.admitting);
    // Not killed.
    assert!(running.await.unwrap().unwrap().succeeded());

    h.set_budgets(&budgets(&format!("hard_limit_micros = {}", 10 * max), ""));
    let out = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap();
    assert!(out.succeeded());
    let raised = h.app.budget.report(&a, None).await.unwrap();
    assert!(raised.config_generation > lowered.config_generation);
}

/// A queued invocation re-checks its budget when it is granted: a limit
/// lowered while it waited refuses it (with its grant given back) instead of
/// starting it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queued_invocation_rechecks_the_budget_when_it_is_granted() {
    let h = harness("[capacity]\nmax_concurrency = 1\n");
    let a = principal(TENANT_A);
    let (f, rev) = deploy(&h, &a, "queued", 5).await;
    let max = max_of(&h, &rev, &serde_json::json!({"sleep_ms": 700}));
    h.set_budgets(&budgets(&format!("hard_limit_micros = {}", 10 * max), ""));

    let app = h.app.clone();
    let req = request(&a, &f, serde_json::json!({"sleep_ms": 700}));
    let first = tokio::spawn(async move { app.invoke.invoke(req).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let app = h.app.clone();
    let req = request(&a, &f, serde_json::json!({}));
    let second = tokio::spawn(async move { app.invoke.invoke(req).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(h.app.admission.metrics_view().queue_length, 1);
    // Lowered below the two reservations while the second waits.
    h.set_budgets(&budgets(&format!("hard_limit_micros = {}", max), ""));
    let _ = h.app.budget.report(&a, None).await;
    // The in-process source refreshes on the request path; make sure the new
    // generation is in the cache before the grant.
    h.app.refresh_config().await.unwrap();

    assert!(first.await.unwrap().unwrap().succeeded());
    let err = match second.await.unwrap() {
        Ok(out) => out.error().expect("refused"),
        Err(e) => e,
    };
    assert_budget(&err, ErrorCode::BudgetExhausted, "Host.BudgetExhausted");
    assert_eq!(h.app.budget.counters().recheck_refusals, 1);
    h.app.collect_usage().unwrap();
    let report = h.app.budget.report(&a, None).await.unwrap();
    assert_eq!(report.tenant.reserved_micros, 0);
    assert_eq!(report.tenant.settlements, 2, "{:?}", report.tenant);
    // The refused run never started anything: it settled at zero.
    let settled_with_attempts = h
        .app
        .budget
        .store()
        .reservations_of(TENANT_A, &report.period)
        .unwrap()
        .into_iter()
        .filter(|r| !r.attempts.is_empty())
        .count();
    assert_eq!(settled_with_attempts, 1);
}

/// Alerts and the stop are separate settings: a soft limit with thresholds
/// fires alerts (once each) and never refuses; a hard limit refuses and fires
/// nothing by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alerts_fire_without_stopping_and_the_hard_limit_stops_without_alerts() {
    let h = harness("");
    let a = principal(TENANT_A);
    let b = principal(TENANT_B);
    let (fa, _) = deploy(&h, &a, "alerts", 5).await;
    let (fb, _) = deploy(&h, &b, "stops", 5).await;
    h.set_budgets(&budgets(
        "soft_limit_micros = 1\nalert_thresholds_percent = [50, 100]",
        "hard_limit_micros = 0",
    ));
    for _ in 0..3 {
        let out = h
            .app
            .invoke
            .invoke(request(&a, &fa, serde_json::json!({"sleep_ms": 30})))
            .await
            .unwrap();
        assert!(out.succeeded());
        h.app.collect_usage().unwrap();
    }
    let ra = h.app.budget.report(&a, None).await.unwrap();
    let fired: Vec<u32> = ra
        .tenant
        .alerts_fired
        .iter()
        .map(|x| x.threshold_percent)
        .collect();
    assert_eq!(fired, vec![50, 100]);
    assert!(ra.admitting && ra.tenant.hard_limit_micros.is_none());

    let err = h
        .app
        .invoke
        .invoke(request(&b, &fb, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(&err, ErrorCode::BudgetExhausted, "Host.BudgetExhausted");
    let rb = h.app.budget.report(&b, None).await.unwrap();
    assert!(rb.tenant.alerts_fired.is_empty());
    assert_eq!(rb.tenant.refusals, 1);
    assert_eq!(h.app.budget.counters().alerts.get("tenant"), Some(&2));
    let metrics = h.app.render_metrics().await;
    assert!(
        metrics.contains("tsls_budget_alerts_total{scope=\"tenant\"} 2"),
        "{metrics}"
    );
    assert!(metrics.contains(
        "tsls_budget_refusals_total{reason=\"budget_exhausted\",cause=\"tenant_hard_limit\"} 1"
    ));
}

/// Tenants are isolated: A's exhausted budget does not refuse B, each report
/// shows the caller's tenant only, and a tenant without a delivered budget is
/// refused fail-closed (`Host.BudgetUnknown`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budgets_are_tenant_isolated_and_an_undelivered_budget_fails_closed() {
    let h = harness("");
    let a = principal(TENANT_A);
    let b = principal(TENANT_B);
    let c = principal(TENANT_C);
    let (fa, _) = deploy(&h, &a, "iso-a", 5).await;
    let (fb, _) = deploy(&h, &b, "iso-b", 5).await;
    let (fc, _) = deploy(&h, &c, "iso-c", 5).await;
    h.set_budgets(&budgets(
        "hard_limit_micros = 0",
        "hard_limit_micros = 100000000",
    ));

    let err = h
        .app
        .invoke
        .invoke(request(&a, &fa, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(&err, ErrorCode::BudgetExhausted, "Host.BudgetExhausted");
    assert!(
        h.app
            .invoke
            .invoke(request(&b, &fb, serde_json::json!({})))
            .await
            .unwrap()
            .succeeded()
    );
    let err = h
        .app
        .invoke
        .invoke(request(&c, &fc, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(&err, ErrorCode::BudgetUnavailable, "Host.BudgetUnknown");

    let ra = h.app.budget.report(&a, None).await.unwrap();
    let rb = h.app.budget.report(&b, None).await.unwrap();
    let rc = h.app.budget.report(&c, None).await.unwrap();
    assert_eq!(ra.tenant_id, TENANT_A);
    assert_eq!((ra.tenant.refusals, ra.tenant.reservations), (1, 0));
    assert_eq!((rb.tenant.refusals, rb.tenant.reservations), (0, 1));
    assert!(
        rb.functions
            .iter()
            .all(|x| x.function_id.as_deref() == Some(fb.id.as_str()))
    );
    assert_eq!(rc.config_state, "not_delivered");
    assert_eq!(rc.refusal.as_deref(), Some("Host.BudgetUnknown"));
    // A report never takes a tenant from anywhere but the principal.
    assert!(
        ra.functions
            .iter()
            .all(|x| x.function_id.as_deref() != Some(fb.id.as_str()))
    );
    // An operator-only principal gets no budget report.
    let operator = Principal {
        roles: vec![Role::Deploy],
        ..a.clone()
    };
    assert!(h.app.budget.report(&operator, None).await.is_err());
}

/// An unavailable budget store refuses new invocations with its own error
/// type, and the gateway admits again once the store is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unavailable_budget_store_refuses_with_its_own_error_type() {
    let h = harness("");
    let a = principal(TENANT_A);
    let (f, _) = deploy(&h, &a, "store", 5).await;
    h.set_budgets(&budgets("hard_limit_micros = 100000000", ""));
    h.app.budget.store().force_unavailable(true);
    let err = h
        .app
        .invoke
        .invoke(request(&a, &f, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(
        &err,
        ErrorCode::BudgetUnavailable,
        "Host.BudgetStoreUnavailable",
    );
    assert!(!h.app.budget.status().accepting);
    h.app.budget.store().force_unavailable(false);
    assert!(
        h.app
            .invoke
            .invoke(request(&a, &f, serde_json::json!({})))
            .await
            .unwrap()
            .succeeded()
    );
}

/// One ordered decision with distinct reasons: a static admission refusal
/// (placement) comes before the budget and reserves nothing; a budget refusal
/// comes before the queue and capacity and takes no grant; a quota or
/// capacity refusal of what the budget admitted gives the reservation back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_budget_and_capacity_refusals_are_distinct_and_ordered() {
    let h = harness(&format!(
        "[capacity]\nmax_concurrency = 2\nmax_queue = 0\n\n\
         [[capacity.tenants]]\ntenant_id = \"{TENANT_A}\"\nrequired_region = \"jp\"\n\n\
         [[capacity.tenants]]\ntenant_id = \"{TENANT_C}\"\nmax_concurrency = 1\nmax_queue = 0\n"
    ));
    let a = principal(TENANT_A);
    let b = principal(TENANT_B);
    let c = principal(TENANT_C);
    let (fa, _) = deploy(&h, &a, "order-a", 5).await;
    let (fb, _) = deploy(&h, &b, "order-b", 5).await;
    let (fb0, _) = deploy(&h, &b, "order-b-zero", 5).await;
    let (fc, _) = deploy(&h, &c, "order-c", 5).await;
    h.set_budgets(&format!(
        "{}\n[[tenants.functions]]\nfunction_id = \"{}\"\nhard_limit_micros = 0\n\n\
         [[tenants]]\ntenant_id = \"{TENANT_C}\"\nhard_limit_micros = 100000000\n",
        budgets("hard_limit_micros = 0", "hard_limit_micros = 100000000"),
        fb0.id
    ));
    let period = tachyon_serverless_application::budget::period_of(&chrono::Utc::now());
    let reason = |e: &AppError| e.to_api_body(None).error.reason.unwrap_or_default();

    // A: placement never fits and the budget is zero -> placement first,
    // nothing reserved.
    let err = h
        .app
        .invoke
        .invoke(request(&a, &fa, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_eq!(reason(&err), "placement", "{err}");
    assert!(
        h.app
            .budget
            .store()
            .reservations_of(TENANT_A, &period)
            .unwrap()
            .is_empty()
    );

    // B's zero-budget function -> budget, no arrival in admission.
    let arrivals = h.app.admission.metrics_view().counters.arrivals;
    let err = h
        .app
        .invoke
        .invoke(request(&b, &fb0, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_budget(&err, ErrorCode::BudgetExhausted, "Host.BudgetExhausted");
    assert_eq!(h.app.admission.metrics_view().counters.arrivals, arrivals);

    // C: one running, a second one -> quota; its reservation is released.
    let app = h.app.clone();
    let req = request(&c, &fc, serde_json::json!({"sleep_ms": 800}));
    let c_busy = tokio::spawn(async move { app.invoke.invoke(req).await });
    let app = h.app.clone();
    let req = request(&b, &fb, serde_json::json!({"sleep_ms": 800}));
    let b_busy = tokio::spawn(async move { app.invoke.invoke(req).await });
    tokio::time::sleep(Duration::from_millis(250)).await;
    let err = h
        .app
        .invoke
        .invoke(request(&c, &fc, serde_json::json!({})))
        .await
        .unwrap_err();
    assert_eq!(reason(&err), "quota", "{err}");
    // B: the node is full and nothing may wait -> a capacity-side refusal.
    let err = h
        .app
        .invoke
        .invoke(request(&b, &fb, serde_json::json!({})))
        .await
        .unwrap_err();
    assert!(
        ["queue_full", "capacity"].contains(&reason(&err).as_str()),
        "{err}"
    );
    assert!(c_busy.await.unwrap().unwrap().succeeded());
    assert!(b_busy.await.unwrap().unwrap().succeeded());
    for tenant in [TENANT_B, TENANT_C] {
        let rows = h
            .app
            .budget
            .store()
            .reservations_of(tenant, &period)
            .unwrap();
        let released = rows
            .iter()
            .filter(|r| r.state == ReservationState::Released)
            .count();
        assert_eq!(released, 1, "{tenant}: {rows:?}");
    }
    h.app.budget.store().verify_totals().unwrap();
}
