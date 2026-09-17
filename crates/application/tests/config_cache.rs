//! Configuration distribution, the data plane's expiring cache, the
//! authorization lease and control-plane outages (PLT-4636,
//! docs/adr/0007-config-distribution-and-auth-leases.md).
//!
//! A management application (`combined`) and a data-plane application share
//! one `data_dir`, as two gateways of one cell do. The data plane pulls from
//! the management application's publication through [`FlakySource`], which a
//! test can take down (a disconnect) and swap (a management restart). Both
//! run on [`FixedClock`]s, so expiry boundaries are exact.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_application::control::{
    ConfigDelivery, ConfigEntry, ConfigKey, ConfigSource, ConfigValue, ControlError, EntryState,
    SourceError, TenantGrant, grant_key,
};
use tachyon_serverless_application::repository::HeartbeatOutcome;
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, AttemptStatus, EventKind, FixedClock, Function, FunctionRevision, InvocationStatus,
    RevisionStatus, StartKind, TenantId, Timestamp,
};
use tachyon_serverless_provider_fake::{
    FakeExecutionProvider, FakeGuestScript, FakeProviderOptions,
};
use tachyon_serverless_provider_port::{Credential, Principal, Role};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";
const INTERNAL: &str = "internal-credential-0123456789";
/// Data plane: config TTL 20 s, auth lease 30 s, refresh every second.
const CONFIG_TTL: i64 = 20;
const AUTH_LEASE: i64 = 30;

fn t0() -> Timestamp {
    use chrono::TimeZone;
    chrono::Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap()
}

fn token_block(token: &str, tenant: &str) -> String {
    format!(
        "[[identity.tokens]]\ntoken = \"{token}\"\ntenant_id = \"{tenant}\"\nsubject = \"user\"\nroles = [\"deploy\", \"invoke\"]\n"
    )
}

fn management_config(dir: &std::path::Path, instance: &str, tokens: &str) -> GatewayConfig {
    GatewayConfig::from_toml(&format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process-{instance}"

[invoke]
cancel_grace_ms = 100

[dispatcher]
instance = "{instance}"

[control_plane]
internal_token = "{INTERNAL}"

{tokens}
"#,
        data = dir.display(),
    ))
    .unwrap()
}

fn data_plane_config(dir: &std::path::Path, extra: &str) -> GatewayConfig {
    GatewayConfig::from_toml(&format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process-dp"

[invoke]
cancel_grace_ms = 100

[dispatcher]
instance = "data-plane"

[control_plane]
role = "data_plane"
url = "http://127.0.0.1:1"
internal_token = "{INTERNAL}"
refresh_interval_ms = 1000
config_ttl_seconds = {CONFIG_TTL}
auth_lease_seconds = {AUTH_LEASE}

{extra}
"#,
        data = dir.display(),
    ))
    .unwrap()
}

/// A source that can be taken down and pointed at another control plane.
struct FlakySource {
    inner: Mutex<Arc<dyn ConfigSource>>,
    down: AtomicBool,
    fetches: AtomicUsize,
}

impl FlakySource {
    fn new(inner: Arc<dyn ConfigSource>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(inner),
            down: AtomicBool::new(false),
            fetches: AtomicUsize::new(0),
        })
    }
    fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }
    fn point_at(&self, inner: Arc<dyn ConfigSource>) {
        *self.inner.lock() = inner;
    }
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
        let inner = self.inner.lock().clone();
        inner.fetch(since).await
    }
}

struct Cell {
    dir: tempfile::TempDir,
    mgmt: Arc<Application>,
    mgmt_clock: Arc<FixedClock>,
    dp: Arc<Application>,
    dp_clock: Arc<FixedClock>,
    source: Arc<FlakySource>,
    dp_fake: Arc<FakeExecutionProvider>,
}

fn management(
    dir: &std::path::Path,
    instance: &str,
    tokens: &str,
    clock: Arc<FixedClock>,
) -> Arc<Application> {
    let fake = Arc::new(FakeExecutionProvider::new());
    fake.set_default_script(Some(FakeGuestScript::Echo));
    Application::bootstrap_with(
        management_config(dir, instance, tokens),
        fake,
        BootstrapOptions {
            clock,
            ..BootstrapOptions::default()
        },
    )
    .unwrap()
}

fn cell_with(dp_extra: &str, dp_fake: Arc<FakeExecutionProvider>) -> Cell {
    let dir = tempfile::tempdir().unwrap();
    let mgmt_clock = Arc::new(FixedClock::new(t0()));
    let tokens = format!(
        "{}\n{}",
        token_block("tok-a", TENANT_A),
        token_block("tok-b", TENANT_B)
    );
    let mgmt = management(dir.path(), "management", &tokens, mgmt_clock.clone());
    let source = FlakySource::new(mgmt.config_publisher.clone().unwrap());
    let dp_clock = Arc::new(FixedClock::new(t0()));
    let dp = Application::bootstrap_with(
        data_plane_config(dir.path(), dp_extra),
        dp_fake.clone(),
        BootstrapOptions {
            clock: dp_clock.clone(),
            config_source: Some(source.clone()),
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    Cell {
        dir,
        mgmt,
        mgmt_clock,
        dp,
        dp_clock,
        source,
        dp_fake,
    }
}

fn cell() -> Cell {
    let fake = Arc::new(FakeExecutionProvider::new());
    fake.set_default_script(Some(FakeGuestScript::Echo));
    cell_with("", fake)
}

fn principal(tenant: &str) -> Principal {
    Principal {
        subject: "t".into(),
        tenant_id: TenantId::parse(tenant).unwrap(),
        roles: vec![Role::Deploy, Role::Invoke],
    }
}

fn revision_request(digest: &str, publish: bool) -> CreateRevisionRequest {
    CreateRevisionRequest {
        artifact: ArtifactRequest::Binary {
            digest: digest.to_string(),
        },
        architecture: "aarch64".into(),
        resources: ResourcesRequest::default(),
        execution: ExecutionRequest {
            timeout_seconds: 60,
            initialization_timeout_seconds: 30,
            max_concurrency: 8,
        },
        egress: None,
        egress_allow: Vec::new(),
        env_vars: vec![],
        secrets: vec![],
        description: String::new(),
        publish_to_prod: publish,
        required_region: None,
    }
}

async fn new_revision(app: &Application, function: &Function, publish: bool) -> FunctionRevision {
    let p = principal(TENANT_A);
    let artifact = app
        .artifact_service
        .upload(
            &p,
            format!("#!/bin/sh\necho {}\n", function.name).as_bytes(),
        )
        .await
        .unwrap();
    let rev = app
        .revisions
        .create(
            &p,
            &function.id,
            &revision_request(artifact.digest.as_str(), publish),
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

async fn deploy(app: &Application, name: &str) -> (Function, FunctionRevision) {
    let function = app
        .functions
        .create(&principal(TENANT_A), name, "")
        .unwrap();
    let rev = new_revision(app, &function, true).await;
    (function, rev)
}

/// Authenticate `token` on the data plane and invoke `function` there, the
/// way the gateway does (auth lease first, then the pipeline).
async fn dp_invoke(
    app: &Application,
    token: &str,
    function: &Function,
    revision: Option<&FunctionRevision>,
) -> Result<tachyon_serverless_application::InvokeOutcome, AppError> {
    let principal = app
        .config_cache
        .authenticate(&Credential(token.to_string()))
        .await?;
    app.invoke
        .invoke(InvokeRequest {
            principal,
            function_id: function.id.clone(),
            alias: None,
            revision_id: revision.map(|r| r.id.clone()),
            event_kind: EventKind::Json,
            payload: serde_json::json!({"n": 1}),
            idempotency_key: None,
            client_timeout_ms: Some(20_000),
            trace_id: None,
        })
        .await
}

fn control_kind(result: Result<impl std::fmt::Debug, AppError>) -> ControlError {
    match result {
        Err(AppError::Control { kind, .. }) => kind,
        other => panic!("expected a control refusal, got {other:?}"),
    }
}

fn secs(n: i64) -> chrono::Duration {
    chrono::Duration::seconds(n)
}

fn ms(n: i64) -> chrono::Duration {
    chrono::Duration::milliseconds(n)
}

// ---------------------------------------------------------------------------

/// Acceptance 1: with the control plane gone, the configuration it delivered
/// keeps serving invocations for as long as it is valid. The data plane never
/// reads the management store for the decision, and the delivery carries no
/// token or secret value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invokes_continue_from_the_cache_while_the_control_plane_is_down() {
    let c = cell();
    let (function, rev) = deploy(&c.mgmt, "steady").await;

    // Before the first delivery the data plane refuses: nothing delivered.
    assert_eq!(
        control_kind(dp_invoke(&c.dp, "tok-a", &function, None).await),
        ControlError::ConfigNotDelivered
    );
    let report = c.dp.refresh_config().await.unwrap();
    assert!(report.apply.applied > 0);
    let ok = dp_invoke(&c.dp, "tok-a", &function, None).await.unwrap();
    assert!(ok.succeeded());
    assert_eq!(ok.invocation().revision_id, rev.id);

    // The control plane goes away.
    c.source.set_down(true);
    assert!(c.dp.refresh_config().await.is_err());
    assert!(c.dp.config_cache.outage());
    c.dp_clock.advance(secs(CONFIG_TTL / 2));
    let view = c.dp.invoke_gate.view(false).await;
    assert!(!view.control_plane_reachable);
    assert_eq!(view.existing_executions, "continue");
    assert_eq!(view.new_invocations, "accepted");
    assert_eq!(view.new_cold_starts, "allowed");
    let during = dp_invoke(&c.dp, "tok-a", &function, None).await.unwrap();
    assert!(during.succeeded(), "{:?}", during.invocation().status);
    let key = ConfigKey::Function {
        function_id: function.id.clone(),
    };
    assert_eq!(c.dp.config_cache.entry(&key).0, EntryState::StaleButValid);

    // What travelled: generations and references, never a token or a secret.
    let delivery = c
        .mgmt
        .config_publisher
        .as_ref()
        .unwrap()
        .publish(0)
        .unwrap();
    let text = serde_json::to_string(&delivery).unwrap();
    assert!(!text.contains("tok-a") && !text.contains("tok-b"));
    assert!(!text.contains(INTERNAL));
}

/// The expiry boundaries: an entry is valid until `valid_until` exclusive.
/// The config TTL (20 s) ends invocations first; the auth lease (30 s) then
/// refuses the credential itself. Each with its own error type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_work_is_refused_exactly_at_valid_until_with_the_matching_reason() {
    let c = cell();
    let (function, _) = deploy(&c.mgmt, "boundary").await;
    c.dp.refresh_config().await.unwrap();
    c.source.set_down(true);

    c.dp_clock.set(t0() + secs(CONFIG_TTL) - ms(1));
    assert!(
        dp_invoke(&c.dp, "tok-a", &function, None)
            .await
            .unwrap()
            .succeeded()
    );

    c.dp_clock.set(t0() + secs(CONFIG_TTL));
    assert_eq!(
        control_kind(dp_invoke(&c.dp, "tok-a", &function, None).await),
        ControlError::ConfigExpired
    );
    let view = c.dp.invoke_gate.view(false).await;
    assert_eq!(view.new_invocations, "refused");
    assert_eq!(view.refusal, Some("Host.ConfigExpired"));
    assert_eq!(view.existing_executions, "continue");

    c.dp_clock.set(t0() + secs(AUTH_LEASE) - ms(1));
    assert!(
        c.dp.config_cache
            .authenticate(&Credential("tok-a".into()))
            .await
            .is_ok(),
        "the auth lease is still valid"
    );
    c.dp_clock.set(t0() + secs(AUTH_LEASE));
    assert_eq!(
        control_kind(
            c.dp.config_cache
                .authenticate(&Credential("tok-a".into()))
                .await
        ),
        ControlError::AuthLeaseExpired
    );
    // An unknown credential is still just unknown.
    assert!(matches!(
        c.dp.config_cache
            .authenticate(&Credential("nope".into()))
            .await,
        Err(AppError::Unauthorized(_))
    ));
    // No invocation was recorded for any refusal.
    let listed = c
        .mgmt
        .history
        .list_invocations(&principal(TENANT_A), &function.id, 50)
        .unwrap();
    assert_eq!(listed.len(), 1);
}

/// Acceptance 4 (ordering): a delivery of an older generation never rolls a
/// newer route back, per entry or as a whole, and a regressed source renews
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn older_generations_never_roll_back_a_newer_configuration() {
    let c = cell();
    let (function, rev1) = deploy(&c.mgmt, "ordered").await;
    let publisher = c.mgmt.config_publisher.clone().unwrap();
    let first = publisher.publish(0).unwrap();
    let rev2 = new_revision(&c.mgmt, &function, true).await;
    let second = publisher.publish(first.generation).unwrap();
    assert!(second.generation > first.generation);

    let route = ConfigKey::Route {
        function_id: function.id.clone(),
        alias: AliasName::default_alias(),
    };
    let route_revision = |app: &Application| match app.config_cache.entry(&route).2 {
        Some(ConfigValue::Route(a)) => a.revision_id,
        other => panic!("no route: {other:?}"),
    };

    // Newest first, then the older full delivery: the route stays on rev2.
    let full_now = publisher.publish(0).unwrap();
    let applied = c.dp.config_cache.apply(full_now, t0());
    assert!(!applied.regressed);
    assert_eq!(route_revision(&c.dp), rev2.id);
    let late = c.dp.config_cache.apply(first.clone(), t0());
    assert!(
        late.regressed,
        "a delivery behind the cache is ignored whole"
    );
    assert_eq!(route_revision(&c.dp), rev2.id);

    // Per entry: a delivery that claims a current generation but carries an
    // older copy of the route is ignored for that entry.
    let status = c.dp.config_cache.status();
    let stale_route = first
        .entries
        .iter()
        .find(|e| e.key == route)
        .cloned()
        .unwrap();
    let crafted = ConfigDelivery {
        source: "replay".into(),
        generation: status.generation,
        since: status.generation,
        config_ttl_seconds: 0,
        auth_lease_seconds: 0,
        entries: vec![stale_route],
    };
    let report = c.dp.config_cache.apply(crafted, t0());
    assert_eq!(report.ignored_older, 1);
    assert_eq!(route_revision(&c.dp), rev2.id);

    // A regressed delivery confirms nothing: validity still counts from t0.
    c.dp_clock.set(t0() + secs(CONFIG_TTL - 1));
    c.dp.config_cache.apply(first, t0() + secs(CONFIG_TTL - 1));
    c.dp_clock.set(t0() + secs(CONFIG_TTL));
    assert_eq!(c.dp.config_cache.entry(&route).0, EntryState::Expired);
    let _ = (rev1, second);
}

/// Acceptance 4 (convergence) and the dispatcher: changes made while the data
/// plane was cut off arrive on the first refresh after the reconnect, which
/// also re-validates the dispatcher lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconnect_converges_to_the_latest_generation() {
    let c = cell();
    let (function, rev1) = deploy(&c.mgmt, "converge").await;
    c.dp.refresh_config().await.unwrap();

    c.source.set_down(true);
    assert!(c.dp.refresh_config().await.is_err());
    assert!(c.dp.refresh_config().await.is_err());
    assert_eq!(c.dp.config_cache.status().consecutive_failures, 2);
    // Backoff while failing, the refresh interval once healthy.
    assert_eq!(c.dp.config_cache.next_delay(), Duration::from_millis(1000));

    // Meanwhile the control plane publishes rev2 to prod.
    let rev2 = new_revision(&c.mgmt, &function, true).await;
    let during = dp_invoke(&c.dp, "tok-a", &function, None).await.unwrap();
    assert_eq!(
        during.invocation().revision_id,
        rev1.id,
        "the cut-off data plane keeps rev1"
    );

    c.source.set_down(false);
    c.dp_clock.advance(secs(5));
    let report = c.dp.refresh_config().await.unwrap();
    assert!(report.reconnected);
    let latest = c
        .mgmt
        .config_publisher
        .as_ref()
        .unwrap()
        .publish(0)
        .unwrap();
    assert_eq!(report.generation, latest.generation);
    let status = c.dp.config_cache.status();
    assert_eq!(status.consecutive_failures, 0);
    assert_eq!(status.reconnects, 1);
    assert!(!c.dp.dispatcher.is_fenced());
    let after = dp_invoke(&c.dp, "tok-a", &function, None).await.unwrap();
    assert_eq!(after.invocation().revision_id, rev2.id);
}

/// A dispatcher whose lease was reclaimed while its gateway was cut off stays
/// fenced after the control plane comes back: the reconnect re-validates the
/// lease and learns it is gone, and new work keeps being refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dispatcher_fenced_during_the_outage_stays_fenced_after_the_reconnect() {
    let c = cell();
    let (function, _) = deploy(&c.mgmt, "fenced").await;
    c.dp.refresh_config().await.unwrap();
    c.source.set_down(true);
    assert!(c.dp.refresh_config().await.is_err());

    // The data plane stalls past its dispatcher lease (30 s + 2 s skew) and
    // the management gateway reclaims it.
    c.mgmt_clock.advance(secs(40));
    let reclaimed = c.mgmt.dispatcher.reclaim_ledger().unwrap();
    assert_eq!(reclaimed.dispatchers, vec![c.dp.dispatcher.id().clone()]);

    c.source.set_down(false);
    c.dp_clock.advance(secs(1));
    let report = c.dp.refresh_config().await.unwrap();
    assert!(report.reconnected);
    assert!(c.dp.dispatcher.is_fenced());
    assert_eq!(c.dp.heartbeat(), HeartbeatOutcome::Fenced);
    assert!(matches!(
        dp_invoke(&c.dp, "tok-a", &function, None).await,
        Err(AppError::ProviderUnavailable(_))
    ));
}

/// Acceptance 2: an unknown tenant and a revision that was not delivered are
/// refused with their own error types; so is everything before the first
/// delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_tenants_and_undelivered_revisions_are_refused() {
    let c = cell();
    let (function, _) = deploy(&c.mgmt, "unknowns").await;
    c.dp.refresh_config().await.unwrap();

    // A revision published on the management side but not delivered yet: the
    // data plane refuses it as not delivered, the management side (which reads
    // its own ledger) serves it.
    let pinned = new_revision(&c.mgmt, &function, false).await;
    assert_eq!(
        control_kind(dp_invoke(&c.dp, "tok-a", &function, Some(&pinned)).await),
        ControlError::ConfigNotDelivered
    );
    let on_mgmt = c
        .mgmt
        .invoke
        .invoke(InvokeRequest {
            principal: principal(TENANT_A),
            function_id: function.id.clone(),
            alias: None,
            revision_id: Some(pinned.id.clone()),
            event_kind: EventKind::Json,
            payload: serde_json::json!({}),
            idempotency_key: None,
            client_timeout_ms: Some(20_000),
            trace_id: None,
        })
        .await
        .unwrap();
    assert!(on_mgmt.succeeded());
    // After the next refresh the data plane has it too.
    c.dp.refresh_config().await.unwrap();
    assert!(
        dp_invoke(&c.dp, "tok-a", &function, Some(&pinned))
            .await
            .unwrap()
            .succeeded()
    );
    // Another tenant's function stays invisible.
    assert!(matches!(
        dp_invoke(&c.dp, "tok-b", &function, None).await,
        Err(AppError::NotFound(_))
    ));

    // A grant whose tenant was never delivered (a partial or inconsistent
    // delivery): the credential is known, its tenant is not.
    let status = c.dp.config_cache.status();
    let stray = TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzzq").unwrap();
    let grant = ConfigEntry {
        key: ConfigKey::Grant {
            token_digest: grant_key(INTERNAL.as_bytes(), "tok-stray"),
        },
        generation: status.generation + 1,
        value: Some(ConfigValue::Grant(
            tachyon_serverless_application::control::AuthGrant {
                subject: "stray".into(),
                tenant_id: stray.clone(),
                roles: vec![Role::Invoke],
            },
        )),
    };
    let delivery = ConfigDelivery {
        source: "test".into(),
        generation: status.generation + 1,
        since: status.generation,
        config_ttl_seconds: 0,
        auth_lease_seconds: 0,
        entries: vec![grant],
    };
    c.dp.config_cache.apply(delivery, t0());
    assert_eq!(
        control_kind(
            c.dp.config_cache
                .authenticate(&Credential("tok-stray".into()))
                .await
        ),
        ControlError::UnknownTenant
    );
    // Once the tenant arrives, the same credential works.
    let tenant = ConfigEntry {
        key: ConfigKey::Tenant {
            tenant_id: stray.clone(),
        },
        generation: status.generation + 2,
        value: Some(ConfigValue::Tenant(TenantGrant { tenant_id: stray })),
    };
    c.dp.config_cache.apply(
        ConfigDelivery {
            source: "test".into(),
            generation: status.generation + 2,
            since: status.generation + 1,
            config_ttl_seconds: 0,
            auth_lease_seconds: 0,
            entries: vec![tenant],
        },
        t0(),
    );
    assert!(
        c.dp.config_cache
            .authenticate(&Credential("tok-stray".into()))
            .await
            .is_ok()
    );
}

/// A token removed on the control plane (a restart with a new
/// `[[identity.tokens]]`) is revoked on the data plane by the next successful
/// refresh; a data plane that cannot reach the control plane keeps honouring
/// it for at most the auth lease. Generations survive the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_token_stops_working_within_one_refresh_or_one_auth_lease() {
    let c = cell();
    let (function, _) = deploy(&c.mgmt, "revoke").await;
    c.dp.refresh_config().await.unwrap();
    let before = c.dp.config_cache.status().generation;
    assert!(
        c.dp.config_cache
            .authenticate(&Credential("tok-b".into()))
            .await
            .is_ok()
    );

    // The management gateway restarts without tok-b.
    let restarted = management(
        c.dir.path(),
        "management-2",
        &token_block("tok-a", TENANT_A),
        c.mgmt_clock.clone(),
    );

    // Cut off: tok-b keeps working until the auth lease ends, not beyond.
    c.source.set_down(true);
    c.source
        .point_at(restarted.config_publisher.clone().unwrap());
    c.dp_clock.set(t0() + secs(AUTH_LEASE) - ms(1));
    assert!(
        c.dp.config_cache
            .authenticate(&Credential("tok-b".into()))
            .await
            .is_ok()
    );
    c.dp_clock.set(t0() + secs(AUTH_LEASE));
    assert_eq!(
        control_kind(
            c.dp.config_cache
                .authenticate(&Credential("tok-b".into()))
                .await
        ),
        ControlError::AuthLeaseExpired
    );

    // Reachable again: the tombstone arrives, tok-b is simply unknown, tok-a
    // is renewed, and the generation kept growing across the restart.
    c.source.set_down(false);
    let report = c.dp.refresh_config().await.unwrap();
    assert!(report.generation > before);
    assert!(matches!(
        c.dp.config_cache
            .authenticate(&Credential("tok-b".into()))
            .await,
        Err(AppError::Unauthorized(_))
    ));
    assert!(
        dp_invoke(&c.dp, "tok-a", &function, None)
            .await
            .unwrap()
            .succeeded()
    );
    // Tenant B has no grant left: its tenant entry is a tombstone too.
    let tenant_b = ConfigKey::Tenant {
        tenant_id: TenantId::parse(TENANT_B).unwrap(),
    };
    let (_, generation, value) = c.dp.config_cache.entry(&tenant_b);
    assert!(generation.is_some() && value.is_none());
}

/// Outage policy: with `allow_cold_start = false` and no pooled environment,
/// an outage refuses new invocations before anything is recorded; with a
/// pooled environment the warm one keeps serving and only a cold start is
/// refused, as a 503 with `Host.ColdStartRestricted`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_outage_policy_restricts_cold_starts_but_not_running_environments() {
    // Without reuse every invocation would be a cold start.
    let cold = cell_with(
        "[control_plane_outage]\nallow_cold_start = false\n",
        Arc::new(FakeExecutionProvider::new()),
    );
    cold.dp_fake.set_default_script(Some(FakeGuestScript::Echo));
    let (function, _) = deploy(&cold.mgmt, "no-pool").await;
    cold.dp.refresh_config().await.unwrap();
    assert!(
        dp_invoke(&cold.dp, "tok-a", &function, None)
            .await
            .unwrap()
            .succeeded()
    );
    cold.source.set_down(true);
    assert!(cold.dp.refresh_config().await.is_err());
    let refused = dp_invoke(&cold.dp, "tok-a", &function, None).await;
    let err = refused.unwrap_err();
    assert!(
        matches!(
            err,
            AppError::Control {
                kind: ControlError::ColdStartRestricted,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(err.http_status(), 503);
    let view = cold.dp.invoke_gate.view(false).await;
    assert_eq!(view.new_cold_starts, "refused");
    assert_eq!(view.new_invocations, "refused");
    assert_eq!(view.existing_executions, "continue");

    // With reuse: a warm environment serves during the outage.
    let fake = Arc::new(FakeExecutionProvider::with_options(FakeProviderOptions {
        warm_capable: true,
        ..FakeProviderOptions::default()
    }));
    fake.set_default_script(Some(FakeGuestScript::EchoForever));
    let warm = cell_with(
        "[control_plane_outage]\nallow_cold_start = false\n\n[pool]\nenabled = true\nmax_idle_per_revision = 1\n",
        fake,
    );
    let (pooled, _) = deploy(&warm.mgmt, "pooled").await;
    let (other, _) = deploy(&warm.mgmt, "other").await;
    warm.dp.refresh_config().await.unwrap();
    let first = dp_invoke(&warm.dp, "tok-a", &pooled, None).await.unwrap();
    assert!(first.succeeded());
    warm.dp.pool.settle().await;
    assert_eq!(warm.dp.pool.held(), 1);

    warm.source.set_down(true);
    assert!(warm.dp.refresh_config().await.is_err());
    let view = warm.dp.invoke_gate.view(true).await;
    assert_eq!(view.new_invocations, "accepted");
    assert_eq!(view.new_cold_starts, "refused");

    let served = dp_invoke(&warm.dp, "tok-a", &pooled, None).await.unwrap();
    assert!(served.succeeded(), "{:?}", served.invocation().status);
    assert_eq!(served.detail.attempts[0].0.start_kind, StartKind::Warm);

    let needs_cold = dp_invoke(&warm.dp, "tok-a", &other, None).await.unwrap();
    match &needs_cold.invocation().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.error_type, "Host.ColdStartRestricted")
        }
        other => panic!("expected a refused cold start, got {other:?}"),
    }
    assert!(
        needs_cold.detail.attempts.is_empty()
            || matches!(
                needs_cold.detail.attempts[0].0.status,
                AttemptStatus::Failed { .. }
            )
    );
    assert_eq!(needs_cold.error().unwrap().http_status(), 503);
}

/// "Kubernetes API down" in this project is the provider's control API: when
/// its preflight fails, nothing new is booted (503
/// `Host.ProviderControlUnavailable`) and `/readyz` says running executions
/// continue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_provider_control_api_refuses_cold_starts_only() {
    let fake = Arc::new(FakeExecutionProvider::with_options(FakeProviderOptions {
        preflight_failure: Some("host control API is not answering".into()),
        ..FakeProviderOptions::default()
    }));
    fake.set_default_script(Some(FakeGuestScript::Echo));
    let c = cell_with("", fake);
    let (function, _) = deploy(&c.mgmt, "provider-down").await;
    c.dp.refresh_config().await.unwrap();
    // Nothing is known about the provider yet: the invoke path does not probe.
    assert!(
        dp_invoke(&c.dp, "tok-a", &function, None)
            .await
            .unwrap()
            .succeeded()
    );
    assert!(!c.dp.provider_service.preflight().await.ok);
    let err = dp_invoke(&c.dp, "tok-a", &function, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            AppError::Control {
                kind: ControlError::ProviderControlUnavailable,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(err.http_status(), 503);
    let view = c.dp.invoke_gate.view(false).await;
    assert_eq!(view.existing_executions, "continue");
    assert_eq!(view.new_cold_starts, "refused");
    assert_eq!(view.refusal, Some("Host.ProviderControlUnavailable"));
    assert!(view.control_plane_reachable);
}

/// The publication is stamped in the ledger: a restarted control plane
/// continues the generation counter, and an unchanged configuration is not
/// re-stamped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generations_are_monotonic_across_control_plane_restarts() {
    let c = cell();
    deploy(&c.mgmt, "monotonic").await;
    let publisher = c.mgmt.config_publisher.clone().unwrap();
    let a = publisher.publish(0).unwrap();
    let again = publisher.publish(a.generation).unwrap();
    assert_eq!(again.generation, a.generation);
    assert!(
        again.entries.is_empty(),
        "nothing changed, nothing re-stamped"
    );

    let tokens = format!(
        "{}\n{}",
        token_block("tok-a", TENANT_A),
        token_block("tok-b", TENANT_B)
    );
    let restarted = management(c.dir.path(), "management-2", &tokens, c.mgmt_clock.clone());
    let b = restarted
        .config_publisher
        .as_ref()
        .unwrap()
        .publish(a.generation)
        .unwrap();
    assert_eq!(b.generation, a.generation, "same tokens, same content");
    assert!(b.entries.is_empty());
    let _ = &c.dp;
}
