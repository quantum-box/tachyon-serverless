//! Metering of environments a reclaim or a reconcile ends (PLT-4642,
//! docs/adr/0012 「回収された環境の計量」, docs/failure-matrix.md §8).
//!
//! Every environment that reaches a terminal ledger state is reported by
//! exactly one `EnvironmentStopped` in the usage ledger, whichever process or
//! path ended it: the reclaim after its owner lost its lease, the startup
//! reconcile of orphans, the old owner settling late after a reclaim, a boot
//! abandoned by a graceful shutdown. Host usage is `provider_reported` when
//! the provider could still see the environment before its terminate, and
//! `unknown` otherwise — never invented.
//!
//! Two applications share one `data_dir` (and so one usage journal and
//! ledger) and one fake provider; each test runs once with host stats
//! available and once without.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_util::codec::{FramedRead, FramedWrite};

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest,
};
use tachyon_serverless_application::repository::HeartbeatOutcome;
use tachyon_serverless_application::services::reuse_key_for;
use tachyon_serverless_application::{Application, BootstrapOptions, GatewayConfig, InvokeRequest};
use tachyon_serverless_domain::{
    AliasName, Architecture, Clock, EnvironmentId, EnvironmentState, EventKind,
    ExecutionEnvironment, FixedClock, Function, InvocationId, InvocationStatus, LifetimeSource,
    Measurement, ProviderKind, RevisionStatus, StoppedBy, TenantId, Timestamp, UsageEvent,
    UsageEventType, environment_stopped_event_id,
};
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message, encode_message,
};
use tachyon_serverless_provider_fake::{
    CustomScriptContext, FakeExecutionProvider, FakeGuestScript, ScriptFuture,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, EnvironmentSpec, EnvironmentStats, ExecutionProvider, Principal, Role,
};

const TENANT: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const CPU_SECONDS: f64 = 0.25;
const PEAK_BYTES: u64 = 96 * 1024 * 1024;

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

[invoke]
cancel_grace_ms = 100

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

fn epoch_start() -> Timestamp {
    use chrono::TimeZone;
    chrono::Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap()
}

fn secs(n: i64) -> Timestamp {
    epoch_start() + chrono::Duration::seconds(n)
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
                    timeout_seconds: 60,
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
        client_timeout_ms: Some(20_000),
        trace_id: None,
    }
}

/// Handshake, then `Ready` only when `ready` is true; answers its `Invoke`
/// once `gate` opens, with the `(attempt_id, epoch)` it received.
fn guest(ready: bool, gate: watch::Receiver<bool>) -> FakeGuestScript {
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        let mut gate = gate.clone();
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "reclaim-usage-test".into(),
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
            if !ready {
                // Initializing forever: only a cancel or a terminate ends it.
                std::future::pending::<()>().await;
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
                        payload: serde_json::json!({"late": true}),
                        handler_ms: Some(1),
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

fn stats() -> EnvironmentStats {
    EnvironmentStats {
        cpu_seconds: Some(CPU_SECONDS),
        memory_current_bytes: Some(1024),
        memory_peak_bytes: Some(PEAK_BYTES),
        scope: "fake".into(),
    }
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

fn env_of(app: &Application, inv: &InvocationId) -> EnvironmentId {
    app.repos.invocations.attempts_of(inv).unwrap()[0]
        .environment_id
        .clone()
}

/// Every `EnvironmentStopped` of `env` in the shared usage ledger, after the
/// collector delivered the shared journal.
fn ledger_stops(app: &Application, env: &EnvironmentId) -> Vec<UsageEvent> {
    app.collect_usage().unwrap();
    app.usage_meter
        .ledger()
        .events_for(
            &TenantId::parse(TENANT).unwrap(),
            None,
            secs(-86_400 * 365),
            secs(86_400 * 365),
        )
        .unwrap()
        .into_iter()
        .filter(|e| &e.environment_id == env && e.event_type == UsageEventType::EnvironmentStopped)
        .collect()
}

/// Exactly one stop for `env`, with host usage `provider_reported` when the
/// provider had stats for it and `unknown` otherwise.
fn assert_one_stop(
    app: &Application,
    env: &EnvironmentId,
    with_stats: bool,
    by: StoppedBy,
) -> UsageEvent {
    let stops = ledger_stops(app, env);
    assert_eq!(stops.len(), 1, "exactly one stop for {env}: {stops:#?}");
    let stop = stops.into_iter().next().unwrap();
    assert_eq!(stop.event_id, environment_stopped_event_id(env));
    assert_eq!(stop.stopped_by, Some(by), "{stop:#?}");
    let cpu = stop.resources.cgroup_cpu_usec;
    let peak = stop.resources.cgroup_memory_peak_bytes;
    if with_stats {
        assert_eq!(cpu.measurement, Measurement::ProviderReported, "{stop:#?}");
        assert_eq!(cpu.value, Some((CPU_SECONDS * 1_000_000.0) as u64));
        assert_eq!(peak.measurement, Measurement::ProviderReported);
        assert_eq!(peak.value, Some(PEAK_BYTES));
    } else {
        assert_eq!(cpu.measurement, Measurement::Unknown, "{stop:#?}");
        assert_eq!(cpu.value, None, "never invented");
        assert_eq!(peak.measurement, Measurement::Unknown);
        assert_eq!(peak.value, None, "never invented");
    }
    stop
}

fn assert_terminal(app: &Application, env: &EnvironmentId) {
    let row = app.repos.environments.get(env).unwrap().unwrap();
    assert!(row.is_terminal(), "{env} is {}", row.state.name());
}

/// Two gateways on one `data_dir`; A drives an invocation whose guest answers
/// only when the returned gate opens. B's clock may be moved independently.
struct Pair {
    _dir: tempfile::TempDir,
    fake: Arc<FakeExecutionProvider>,
    a: Arc<Application>,
    b: Arc<Application>,
    clock_b: Arc<FixedClock>,
    function: Function,
}

async fn pair() -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let clock_a = Arc::new(FixedClock::new(epoch_start()));
    let clock_b = Arc::new(FixedClock::new(epoch_start()));
    let a = start(dir.path(), "gateway-a", &fake, clock_a);
    let function = deploy(&a, "reclaimed").await;
    let b = start(dir.path(), "gateway-b", &fake, clock_b.clone());
    Pair {
        _dir: dir,
        fake,
        a,
        b,
        clock_b,
        function,
    }
}

impl Pair {
    /// B renews its own lease while A's (never renewed) passes, then B's clock
    /// moves past A's lease plus the tolerated skew.
    fn expire_a(&self) {
        self.clock_b.set(secs(20));
        assert!(matches!(
            self.b.heartbeat(),
            HeartbeatOutcome::Renewed { .. }
        ));
        self.clock_b.set(secs(40));
    }
}

/// Reclaim after owner death: A stops renewing while its handler runs. B
/// reclaims, samples the VMM before terminating it and reports its stop once;
/// A's own late settle adds nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reclaimed_environment_is_metered_once_by_the_reclaimer() {
    for with_stats in [true, false] {
        let p = pair().await;
        let (open, gate) = watch::channel(false);
        p.fake.push_script(guest(true, gate));
        let running = {
            let a = p.a.clone();
            let req = request(&p.function);
            tokio::spawn(async move { a.invoke.invoke(req).await })
        };
        let inv = wait_running(&p.a, &p.function).await;
        let env = env_of(&p.a, &inv);
        if with_stats {
            p.fake.set_environment_stats(&env, stats());
        }

        p.expire_a();
        let summary = p.b.reclaim_expired().await;
        assert_eq!((summary.fenced, summary.terminated), (1, 1), "{summary:?}");
        assert_terminal(&p.b, &env);
        let stop = assert_one_stop(&p.b, &env, with_stats, StoppedBy::Reclaim);
        assert!(stop.monotonic_duration_ms.is_some());
        assert_eq!(stop.lifetime_source, LifetimeSource::LedgerReclaimerClock);
        assert_eq!(
            stop.revision_id,
            Some(
                p.a.repos
                    .environments
                    .get(&env)
                    .unwrap()
                    .unwrap()
                    .revision_id
            )
        );
        assert!(stop.function_id.is_some());

        // The old owner answers late: refused, and no second stop.
        open.send_replace(true);
        let out = running.await.unwrap().unwrap();
        assert!(matches!(
            out.invocation().status,
            InvocationStatus::OutcomeUnknown { .. }
        ));
        assert_one_stop(&p.a, &env, with_stats, StoppedBy::Reclaim);
    }
}

/// Fenced late settle, owner first: B only fences (the ledger half of the
/// reclaim), the old owner's late answer arrives, is refused, and the owner
/// terminates and settles the environment itself. With host stats it measured
/// the VMM and reports; B's terminate afterwards finds nothing, still tries to
/// report under the same event id, and the ledger drops the duplicate.
/// Without stats the owner leaves the stop to the reclaimer. Either way: one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_environment_settled_late_by_its_old_owner_is_metered_once() {
    for with_stats in [true, false] {
        let p = pair().await;
        let (open, gate) = watch::channel(false);
        p.fake.push_script(guest(true, gate));
        let running = {
            let a = p.a.clone();
            let req = request(&p.function);
            tokio::spawn(async move { a.invoke.invoke(req).await })
        };
        let inv = wait_running(&p.a, &p.function).await;
        let env = env_of(&p.a, &inv);
        if with_stats {
            p.fake.set_environment_stats(&env, stats());
        }
        p.expire_a();
        let fenced = p.b.dispatcher.reclaim_ledger().unwrap();
        assert_eq!(fenced.fenced.len(), 1);

        open.send_replace(true);
        let out = running.await.unwrap().unwrap();
        assert!(matches!(
            out.invocation().status,
            InvocationStatus::OutcomeUnknown { .. }
        ));
        let before = p.b.usage_meter.ledger().stats().unwrap();

        let summary = p.b.reclaim_expired().await;
        assert_eq!(summary.terminated, 1, "{summary:?}");
        assert_terminal(&p.b, &env);
        let by = match with_stats {
            true => StoppedBy::Owner,
            false => StoppedBy::Reclaim,
        };
        assert_one_stop(&p.b, &env, with_stats, by);
        let after = p.b.usage_meter.ledger().stats().unwrap();
        if with_stats {
            assert!(
                after.duplicates_ignored > before.duplicates_ignored,
                "both the late owner and the reclaimer reported it: {before:?} -> {after:?}"
            );
        }
    }
}

/// The event id of an environment's stop is the same on every path, so the
/// ledger keeps one whatever order two reporters reach the journal in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_reporters_of_one_stop_collide_on_the_event_id() {
    let p = pair().await;
    p.fake
        .push_script(FakeGuestScript::RespondOk(serde_json::json!({"ok": 1})));
    let out = p.a.invoke.invoke(request(&p.function)).await.unwrap();
    assert!(out.succeeded());
    let env = env_of(&p.a, &out.invocation().id);
    let owner = ledger_stops(&p.a, &env);
    assert_eq!(owner.len(), 1);
    assert_eq!(owner[0].stopped_by, Some(StoppedBy::Owner));
    assert_eq!(owner[0].lifetime_source, LifetimeSource::LedgerOwnerClock);

    // A reclaimer reporting the same environment again (e.g. its orphan
    // terminate after a restart) derives the same id: a duplicate.
    let row = p.b.repos.environments.get(&env).unwrap().unwrap();
    tachyon_serverless_application::services::stopped::record_reclaimed_stop(
        &p.b.repos,
        &tachyon_serverless_application::usage::JournalingUsageSink::new(
            p.b.usage_meter.clone(),
            p.b.usage.clone(),
        ),
        tachyon_serverless_application::services::stopped::ReclaimedStop {
            env: &row,
            stopped_by: StoppedBy::Reconcile,
            host_sample: None,
            teardown: None,
            ended_at: None,
            now: secs(1),
        },
    )
    .await;
    let stops = ledger_stops(&p.b, &env);
    assert_eq!(stops.len(), 1);
    assert_eq!(
        stops[0].stopped_by,
        Some(StoppedBy::Owner),
        "the first one stays"
    );
}

/// Restart reconcile: a new gateway reclaims the busy environment of an owner
/// whose lease passed, terminates an orphan still running on the host whose
/// row is already terminal, and marks `Lost` an environment the provider no
/// longer tracks. Each gets exactly one stop; the one nobody saw end has no
/// lifetime and no host usage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_startup_reconcile_meters_every_orphan_once() {
    for with_stats in [true, false] {
        let p = pair().await;
        let (_open, gate) = watch::channel(false);
        p.fake.push_script(guest(true, gate.clone()));
        let _running = {
            let a = p.a.clone();
            let req = request(&p.function);
            tokio::spawn(async move { a.invoke.invoke(req).await })
        };
        let inv = wait_running(&p.a, &p.function).await;
        let busy = env_of(&p.a, &inv);

        // An orphan: the ledger row is already terminal, the host still runs it.
        let revision =
            p.a.repos
                .revisions
                .get(
                    &p.a.repos
                        .environments
                        .get(&busy)
                        .unwrap()
                        .unwrap()
                        .revision_id,
                )
                .unwrap()
                .unwrap();
        let tenant = TenantId::parse(TENANT).unwrap();
        let orphan_id = EnvironmentId::generate();
        let mut orphan = ExecutionEnvironment::request(
            orphan_id.clone(),
            tenant.clone(),
            revision.id.clone(),
            ProviderKind::Fake,
            reuse_key_for(&tenant, &revision, 0),
            secs(0),
        )
        .owned_by(p.a.dispatcher.id().clone());
        p.a.repos.environments.insert(orphan.clone()).unwrap();
        orphan.mark_stopped(secs(1)).unwrap();
        p.a.repos.environments.update(orphan).unwrap();
        p.fake.push_script(guest(false, gate.clone()));
        let handle = p
            .fake
            .create_environment(EnvironmentSpec {
                environment_id: orphan_id.clone(),
                tenant_id: tenant.clone(),
                revision_id: revision.id.clone(),
                artifact: ArtifactLocation {
                    path: "/nonexistent".into(),
                    digest: match &revision.spec.artifact {
                        tachyon_serverless_domain::ArtifactRef::Binary { digest, .. } => {
                            digest.clone()
                        }
                        other => panic!("{other:?}"),
                    },
                    size_bytes: 1,
                },
                architecture: Architecture::Aarch64,
                resources: revision.spec.resources,
                egress: revision.spec.egress,
                egress_allow: Vec::new(),
                connect_timeout: Duration::from_secs(1),
            })
            .await
            .unwrap();
        // Gone: an environment of the reconciling gateway the host lost.
        let gone_id = EnvironmentId::generate();
        let gone = ExecutionEnvironment::request(
            gone_id.clone(),
            tenant.clone(),
            revision.id.clone(),
            ProviderKind::Fake,
            reuse_key_for(&tenant, &revision, 0),
            secs(0),
        )
        .owned_by(p.b.dispatcher.id().clone());
        p.b.repos.environments.insert(gone).unwrap();
        if with_stats {
            p.fake.set_environment_stats(&busy, stats());
            p.fake.set_environment_stats(&orphan_id, stats());
        }

        p.expire_a();
        let report = p.b.reconcile_on_startup().await.unwrap();
        assert_eq!(report.reclaim.terminated, 1, "{report:?}");
        assert_eq!(report.terminated, 1, "{report:?}");
        assert_eq!(report.lost, 1, "{report:?}");
        drop(handle);

        assert_terminal(&p.b, &busy);
        assert_one_stop(&p.b, &busy, with_stats, StoppedBy::Reclaim);
        let stop = assert_one_stop(&p.b, &orphan_id, with_stats, StoppedBy::Reconcile);
        assert_eq!(stop.lifetime_source, LifetimeSource::LedgerReclaimerClock);
        assert!(stop.segments.teardown_ms.value.is_some());
        assert!(matches!(
            p.b.repos.environments.get(&gone_id).unwrap().unwrap().state,
            EnvironmentState::Lost { .. }
        ));
        let stop = assert_one_stop(&p.b, &gone_id, false, StoppedBy::Reconcile);
        assert_eq!(stop.monotonic_duration_ms, None, "nobody saw it end");
        assert_eq!(stop.lifetime_source, LifetimeSource::Unknown);
    }
}

/// Shutdown while booting: a graceful shutdown cancels an invocation whose
/// environment is still initializing. The driver samples the environment
/// before its terminate, so the abandoned boot reports host usage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boot_abandoned_by_a_shutdown_is_metered_once() {
    for with_stats in [true, false] {
        let p = pair().await;
        let (_open, gate) = watch::channel(false);
        p.fake.push_script(guest(false, gate));
        let running = {
            let a = p.a.clone();
            let req = request(&p.function);
            tokio::spawn(async move { a.invoke.invoke(req).await })
        };
        let env = loop {
            if let Some(env) = p.fake.created().first().cloned()
                && p.a
                    .repos
                    .environments
                    .get(&env)
                    .unwrap()
                    .is_some_and(|row| {
                        row.state == EnvironmentState::Initializing
                            && row.evidence.details.contains_key("bridge_version")
                    })
            {
                break env;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        if with_stats {
            p.fake.set_environment_stats(&env, stats());
        }
        p.a.invoke.shutdown_all(Duration::from_secs(5)).await;
        let _ = running.await.unwrap();
        assert_terminal(&p.a, &env);
        let stop = assert_one_stop(&p.a, &env, with_stats, StoppedBy::Owner);
        assert!(stop.attempt_id.is_none(), "no attempt was dispatched");
    }
}
