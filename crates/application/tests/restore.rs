//! X1 (PLT-4653, experimental): snapshot creation, restore policy and clone
//! refusals against the fake provider (docs/adr/0017).
//!
//! The fake provider writes placeholder snapshot files and plays scripted
//! guests; what is under test is the host side: which snapshots may be
//! loaded, what `prefer` and `require` do when none may, and that only a guest
//! that really reconnects as the snapshot's source is counted as restored.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio_util::codec::{FramedRead, FramedWrite};

use tachyon_serverless_api_types::{
    ArtifactRequest, CreateRevisionRequest, ExecutionRequest, ResourcesRequest, RestoreRequest,
    SecretBindingRequest,
};
use tachyon_serverless_application::entrypoint::EntrypointPolicy;
use tachyon_serverless_application::snapshot::{
    SnapshotService, SnapshotServiceDeps, SnapshotSettings, SnapshotStore,
};
use tachyon_serverless_application::{
    AppError, Application, BootstrapOptions, GatewayConfig, InvokeOutcome, InvokeRequest,
};
use tachyon_serverless_domain::{
    AliasName, Architecture, EnvironmentId, ErrorClass, EventKind, Function, FunctionRevision,
    InvocationStatus, ProviderKind, RevisionStatus, SealedManifest, Sha256Digest, SnapshotId,
    SnapshotManifest, SnapshotSigningKey, StartKind, TenantId,
};
use tachyon_serverless_protocol::{
    CHECKPOINT_PHASE, FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message,
    encode_message,
};
use tachyon_serverless_provider_fake::{
    CustomScriptContext, FakeExecutionProvider, FakeGuestScript, FakeProviderOptions, ScriptFuture,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, Capabilities, CloneSpec, CloneTimings, EnvironmentHandle,
    EnvironmentObservation, EnvironmentSpec, ExecutionProvider, PreflightReport, Principal,
    ProviderError, RestoreHostProfile, Role, SnapshotCapture, TerminateReason, TerminateReport,
};

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";
const SOURCE_BOOT: &str = "boot-of-the-source";

fn write_key(dir: &Path, name: &str, byte: u8) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, hex::encode([byte; 32])).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path
}

fn config_toml(dir: &Path, snapshots: &str) -> String {
    let key = write_key(dir, "snap.key", 0x11);
    let signing = write_key(dir, "snap.sign", 0x22);
    format!(
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

[[secrets.bindings]]
tenant_id = "{TENANT_A}"
binding_ref = "demo-secret"
value = "demo-secret-value-a"

[invoke]
cancel_grace_ms = 100

{snapshots}
key_file = "{key}"
signing_key_file = "{signing}"
"#,
        data = dir.display(),
        key = key.display(),
        signing = signing.display(),
    )
}

struct Harness {
    app: Arc<Application>,
    fake: Arc<FakeExecutionProvider>,
    a: Principal,
    dir: tempfile::TempDir,
    /// `(environment id, boot id)` of the last snapshot source.
    source: Arc<Mutex<Option<(String, String)>>>,
}

fn principal(tenant: &str) -> Principal {
    Principal {
        subject: "t".into(),
        tenant_id: TenantId::parse(tenant).unwrap(),
        roles: vec![Role::Deploy, Role::Invoke],
    }
}

fn harness_with(
    snapshots: &str,
    wrap: impl FnOnce(Arc<FakeExecutionProvider>) -> Arc<dyn ExecutionProvider>,
) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::with_options(FakeProviderOptions {
        snapshot_root: Some(dir.path().join("provider-snapshots")),
        ..FakeProviderOptions::default()
    }));
    let config = GatewayConfig::from_toml(&config_toml(dir.path(), snapshots)).unwrap();
    let app = Application::bootstrap_with(
        config,
        wrap(fake.clone()),
        BootstrapOptions {
            persist_state: true,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    Harness {
        app,
        fake,
        a: principal(TENANT_A),
        dir,
        source: Arc::new(Mutex::new(None)),
    }
}

fn harness() -> Harness {
    harness_with(
        "[snapshots]\nenabled = true\nallow_unverified = true",
        |f| f,
    )
}

fn revision_request(digest: &str, policy: Option<&str>, synthetic: bool) -> CreateRevisionRequest {
    CreateRevisionRequest {
        artifact: ArtifactRequest::Binary {
            digest: digest.to_string(),
        },
        architecture: "aarch64".into(),
        resources: ResourcesRequest::default(),
        execution: ExecutionRequest {
            timeout_seconds: 10,
            initialization_timeout_seconds: 10,
            max_concurrency: 4,
            ..ExecutionRequest::default()
        },
        egress: None,
        egress_allow: Vec::new(),
        env_vars: vec![("GREETING".into(), "hello".into())],
        secrets: vec![],
        description: "restore test".into(),
        publish_to_prod: true,
        required_region: None,
        restore: policy.map(|p| RestoreRequest {
            policy: p.into(),
            synthetic_init_sample: synthetic,
        }),
    }
}

async fn deploy_req(
    h: &Harness,
    function: Option<&Function>,
    name: &str,
    req: CreateRevisionRequest,
) -> (Function, FunctionRevision) {
    let function = match function {
        Some(f) => f.clone(),
        None => h.app.functions.create(&h.a, name, "").unwrap(),
    };
    let artifact = h
        .app
        .artifact_service
        .upload(&h.a, format!("#!/bin/sh\necho {name}\n").as_bytes())
        .await
        .unwrap();
    let mut req = req;
    req.artifact = ArtifactRequest::Binary {
        digest: artifact.digest.to_string(),
    };
    let rev = h
        .app
        .revisions
        .create(&h.a, &function.id, &req)
        .await
        .unwrap();
    let rev = h
        .app
        .revisions
        .wait_terminal(&rev.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(rev.status, RevisionStatus::Ready, "{:?}", rev.status);
    (function, rev)
}

async fn deploy(h: &Harness, name: &str, policy: &str) -> (Function, FunctionRevision) {
    deploy_req(
        h,
        None,
        name,
        revision_request("sha256:x", Some(policy), true),
    )
    .await
}

fn invoke_request(h: &Harness, function: &Function, n: u64) -> InvokeRequest {
    InvokeRequest {
        principal: h.a.clone(),
        function_id: function.id.clone(),
        alias: None,
        revision_id: None,
        event_kind: EventKind::Json,
        payload: serde_json::json!({ "n": n }),
        idempotency_key: None,
        client_timeout_ms: None,
        trace_id: None,
    }
}

fn failed(out: &InvokeOutcome) -> tachyon_serverless_domain::InvocationError {
    match &out.invocation().status {
        InvocationStatus::Failed { error } => error.clone(),
        other => panic!("expected a failed invocation, got {other:?}"),
    }
}

// --- scripted guests -----------------------------------------------------------------------

/// The snapshot source: speaks version 3, must be asked to hold, must not
/// receive secrets, reports the checkpoint and then waits to be killed.
fn source_script(h: &Harness) -> FakeGuestScript {
    let source = h.source.clone();
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        let source = source.clone();
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            *source.lock() = Some((ctx.environment_id.to_string(), SOURCE_BOOT.to_string()));
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "test-source".into(),
                environment_id: ctx.environment_id.to_string(),
                guest_boot_id: Some(SOURCE_BOOT.into()),
                architecture: "aarch64".into(),
            };
            writer.send(encode_message(&hello).unwrap()).await.unwrap();
            let Some(Ok(frame)) = reader.next().await else {
                return;
            };
            match decode_message::<HostMessage>(&frame).unwrap() {
                HostMessage::HelloAck {
                    snapshot_hold, env, ..
                } => {
                    assert!(snapshot_hold, "a source is asked to hold");
                    assert!(
                        env.iter().all(|(k, _)| k != "DEMO_SECRET"),
                        "a snapshot source never receives secrets"
                    );
                }
                other => panic!("expected hello_ack, got {other:?}"),
            }
            let waiting = GuestMessage::CheckpointWaiting {
                lifecycle_phase: CHECKPOINT_PHASE.into(),
                lifecycle_version: 1,
                after_restore_ran: false,
            };
            writer
                .send(encode_message(&waiting).unwrap())
                .await
                .unwrap();
            while let Some(Ok(_)) = reader.next().await {}
        })
    }))
}

/// A restored copy: reconnects as the source, takes its identity from
/// `Restore`, becomes ready and answers every invoke with that identity.
fn restored_script(h: &Harness) -> FakeGuestScript {
    let source = h.source.clone();
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        let source = source.clone();
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let (env, boot) = source.lock().clone().expect("a snapshot was taken");
            let reconnect = GuestMessage::Reconnect {
                protocol_version: PROTOCOL_VERSION,
                environment_id: env,
                guest_boot_id: Some(boot),
                reconnects: 1,
                lost: "vsock doorbell".into(),
            };
            writer
                .send(encode_message(&reconnect).unwrap())
                .await
                .unwrap();
            let Some(Ok(frame)) = reader.next().await else {
                return;
            };
            let HostMessage::Restore {
                environment_id,
                instance_id,
                generation,
                ..
            } = decode_message::<HostMessage>(&frame).unwrap()
            else {
                return;
            };
            assert_eq!(environment_id, ctx.environment_id.to_string());
            writer
                .send(encode_message(&GuestMessage::Ready { init_ms: 1 }).unwrap())
                .await
                .unwrap();
            while let Some(Ok(frame)) = reader.next().await {
                if let Ok(HostMessage::Invoke {
                    attempt_id, epoch, ..
                }) = decode_message::<HostMessage>(&frame)
                {
                    let resp = GuestMessage::Response {
                        attempt_id,
                        epoch,
                        payload: serde_json::json!({
                            "restored": true,
                            "instance_id": instance_id,
                            "generation": generation,
                        }),
                        handler_ms: Some(1),
                    };
                    writer.send(encode_message(&resp).unwrap()).await.unwrap();
                }
            }
        })
    }))
}

/// A "clone" whose guest booted instead of resuming: it says hello.
fn cold_boot_on_clone_script() -> FakeGuestScript {
    FakeGuestScript::Custom(Arc::new(move |ctx: CustomScriptContext| -> ScriptFuture {
        Box::pin(async move {
            let (r, w) = tokio::io::split(ctx.stream);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "cold".into(),
                environment_id: ctx.environment_id.to_string(),
                guest_boot_id: Some("a-fresh-boot".into()),
                architecture: "aarch64".into(),
            };
            let _ = writer.send(encode_message(&hello).unwrap()).await;
            while let Some(Ok(_)) = reader.next().await {}
        })
    }))
}

async fn snapshot(
    h: &Harness,
    function: &Function,
) -> tachyon_serverless_api_types::SnapshotResponse {
    h.fake.push_script(source_script(h));
    h.app
        .snapshots
        .as_ref()
        .expect("snapshots enabled")
        .create(&h.a, &function.id, None)
        .await
        .unwrap()
}

fn snapshot_state(h: &Harness, id: &str) -> String {
    h.app
        .snapshots
        .as_ref()
        .unwrap()
        .store()
        .read_record(&SnapshotId::parse(id).unwrap())
        .unwrap()
        .state
        .name()
        .to_string()
}

fn provider_file(h: &Harness, id: &str, name: &str) -> PathBuf {
    h.dir.path().join("provider-snapshots").join(id).join(name)
}

// --- tests ---------------------------------------------------------------------------------

/// Normal cold start regression: a revision without a restore policy never
/// touches snapshots, even with `[snapshots] enabled` and a snapshot present.
#[tokio::test]
async fn a_revision_without_a_policy_starts_cold_exactly_as_before() {
    let h = harness();
    let (function, _) =
        deploy_req(&h, None, "plain", revision_request("sha256:x", None, false)).await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    let (attempt, evidence) = &out.detail.attempts[0];
    assert_eq!(attempt.start_kind, StartKind::Cold);
    assert!(
        evidence
            .details
            .keys()
            .all(|k| !k.starts_with("restore") && k != "snapshot_id"),
        "{:?}",
        evidence.details
    );
    assert!(h.fake.cloned().is_empty());
}

/// Acceptance: identity and write area per clone. Two concurrent invocations
/// are served by two clones of one snapshot; both are restored, and their
/// environments, instance ids and generations differ.
#[tokio::test]
async fn two_clones_are_restored_with_separate_identities() {
    let h = harness();
    let (function, rev) = deploy(&h, "restore-aware", "require").await;
    let snap = snapshot(&h, &function).await;
    assert_eq!(snap.state, "active");
    assert_eq!(snap.revision_id, rev.id.to_string());
    let (source_env, _) = h.source.lock().clone().unwrap();
    assert_eq!(snap.source_environment_id, source_env);
    // The source was killed, never resumed.
    assert!(
        h.fake
            .terminated()
            .iter()
            .any(|(e, r)| e.as_str() == source_env && *r == TerminateReason::Quiesced)
    );

    // The sealed artifacts do not contain the plaintext.
    let store = h.app.snapshots.as_ref().unwrap().store();
    let id = SnapshotId::parse(&snap.id).unwrap();
    let sealed = std::fs::read(store.sealed_path(&id, "memory")).unwrap();
    let plain = std::fs::read(provider_file(&h, &snap.id, "memory")).unwrap();
    assert!(!sealed.windows(plain.len()).any(|w| w == plain.as_slice()));

    h.fake.push_script(restored_script(&h));
    h.fake.push_script(restored_script(&h));
    let (a, b) = tokio::join!(
        h.app.invoke.invoke(invoke_request(&h, &function, 1)),
        h.app.invoke.invoke(invoke_request(&h, &function, 2)),
    );
    let mut instances = Vec::new();
    let mut envs = Vec::new();
    let mut generations = Vec::new();
    for out in [a.unwrap(), b.unwrap()] {
        assert!(out.succeeded(), "{:?}", out.invocation().status);
        let (attempt, evidence) = &out.detail.attempts[0];
        assert_eq!(attempt.start_kind, StartKind::Restored);
        assert_eq!(evidence.details["start"], "restored");
        assert_eq!(evidence.details["snapshot_id"], snap.id.as_str());
        let output = out.output.clone().unwrap();
        assert_eq!(
            output["instance_id"],
            evidence.details["restore_instance_id"]
        );
        instances.push(output["instance_id"].as_str().unwrap().to_string());
        generations.push(output["generation"].as_u64().unwrap());
        envs.push(attempt.environment_id.clone());
        // Boot identity of a clone is (source boot id, instance id).
        assert_eq!(
            evidence.guest_boot_id.as_deref(),
            Some(format!("{SOURCE_BOOT}/{}", output["instance_id"].as_str().unwrap()).as_str())
        );
    }
    assert_ne!(
        instances[0], instances[1],
        "each clone has its own identity"
    );
    assert_ne!(envs[0], envs[1], "each clone is its own environment");
    generations.sort();
    assert_eq!(generations, vec![1, 2]);
    assert_eq!(h.fake.cloned().len(), 2);
    let listed = h
        .app
        .snapshots
        .as_ref()
        .unwrap()
        .list(&h.a, &function.id)
        .unwrap();
    assert_eq!(listed[0].restores, 2);
}

/// Acceptance: `require` never silently starts cold.
#[tokio::test]
async fn require_without_a_snapshot_fails_explicitly_and_boots_nothing() {
    let h = harness();
    let (function, _) = deploy(&h, "req", "require").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    let error = failed(&out);
    assert_eq!(error.error_type, "Host.RestoreRequiredUnavailable");
    assert_eq!(error.class, ErrorClass::InitError);
    assert!(error.message.contains("no_snapshot"), "{}", error.message);
    assert!(h.fake.created().is_empty(), "nothing booted");
}

/// Acceptance: `prefer` falls back to a cold start, recorded as cold with
/// the reason.
#[tokio::test]
async fn prefer_without_a_snapshot_starts_cold_and_records_why() {
    let h = harness();
    let (function, _) = deploy(&h, "pref", "prefer").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    let (attempt, evidence) = &out.detail.attempts[0];
    assert_eq!(attempt.start_kind, StartKind::Cold);
    assert_eq!(evidence.details["restore_policy"], "prefer");
    assert_eq!(evidence.details["restore_fallback"], "no_snapshot");
}

/// Acceptance: a cold boot is never counted as restored. The clone's guest
/// says hello: `prefer` retires it and serves the invocation from a cold
/// environment recorded as cold; `require` fails.
#[tokio::test]
async fn a_cold_boot_on_a_clone_is_never_counted_as_restored() {
    let h = harness();
    let (function, _) = deploy(&h, "coldclone", "prefer").await;
    snapshot(&h, &function).await;
    h.fake.push_script(cold_boot_on_clone_script());
    h.fake.push_script(FakeGuestScript::RespondOk(
        serde_json::json!({"cold": true}),
    ));
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert!(out.succeeded(), "{:?}", out.invocation().status);
    let (attempt, evidence) = &out.detail.attempts[0];
    assert_eq!(attempt.start_kind, StartKind::Cold);
    assert_eq!(evidence.details["restore_fallback"], "cold_boot_detected");
    assert_eq!(out.output, Some(serde_json::json!({"cold": true})));
    let clone_env = &h.fake.cloned()[0].0;
    assert!(
        h.fake
            .terminated()
            .iter()
            .any(|(e, r)| e == clone_env && *r == TerminateReason::InitFailed),
        "the cold-booted clone is terminated"
    );

    let (strict, _) = deploy(&h, "coldclone-require", "require").await;
    snapshot(&h, &strict).await;
    h.fake.push_script(cold_boot_on_clone_script());
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &strict, 1))
        .await
        .unwrap();
    let error = failed(&out);
    assert_eq!(error.error_type, "Host.RestoreRequiredUnavailable");
    assert!(
        error.message.contains("cold_boot_detected"),
        "{}",
        error.message
    );
}

/// Acceptance: a broken artifact is refused. A flipped byte in the memory
/// file quarantines the snapshot; `require` fails, `prefer` starts cold.
#[tokio::test]
async fn a_corrupted_artifact_is_refused_and_quarantined() {
    let h = harness();
    let (function, _) = deploy(&h, "corrupt", "require").await;
    let snap = snapshot(&h, &function).await;
    let memory = provider_file(&h, &snap.id, "memory");
    let mut bytes = std::fs::read(&memory).unwrap();
    bytes[0] ^= 0x01;
    std::fs::write(&memory, bytes).unwrap();
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    let error = failed(&out);
    assert_eq!(error.error_type, "Host.RestoreRequiredUnavailable");
    assert!(
        error.message.contains("artifact_corrupted"),
        "{}",
        error.message
    );
    assert_eq!(snapshot_state(&h, &snap.id), "quarantined");
    assert!(h.fake.cloned().is_empty(), "nothing was loaded");
}

/// A missing plaintext file is decrypted from the sealed store; a flipped
/// byte in the sealed file is refused the same way.
#[tokio::test]
async fn a_corrupted_sealed_artifact_is_refused() {
    let h = harness();
    let (function, _) = deploy(&h, "sealed", "prefer").await;
    let snap = snapshot(&h, &function).await;
    let id = SnapshotId::parse(&snap.id).unwrap();
    let store = h.app.snapshots.as_ref().unwrap().store().clone();

    // Missing plaintext, intact sealed file: restored after decryption.
    let vmstate = provider_file(&h, &snap.id, "vmstate");
    let original = std::fs::read(&vmstate).unwrap();
    std::fs::remove_file(&vmstate).unwrap();
    h.fake.push_script(restored_script(&h));
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert_eq!(out.detail.attempts[0].0.start_kind, StartKind::Restored);
    assert_eq!(std::fs::read(&vmstate).unwrap(), original);

    // Missing plaintext, flipped sealed byte: refused, cold with the reason.
    std::fs::remove_file(&vmstate).unwrap();
    let sealed = store.sealed_path(&id, "vmstate");
    let mut bytes = std::fs::read(&sealed).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x80;
    std::fs::write(&sealed, bytes).unwrap();
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    let (attempt, evidence) = &out.detail.attempts[0];
    assert_eq!(attempt.start_kind, StartKind::Cold);
    assert_eq!(evidence.details["restore_fallback"], "artifact_corrupted");
    assert!(!vmstate.exists(), "no unverified plaintext is left behind");
    assert_eq!(snapshot_state(&h, &snap.id), "quarantined");
}

/// Acceptance: an updated revision does not use the old revision's snapshot.
#[tokio::test]
async fn a_new_revision_does_not_use_the_previous_revisions_snapshot() {
    let h = harness();
    let (function, _) = deploy(&h, "updated", "require").await;
    snapshot(&h, &function).await;
    let (_, rev2) = deploy_req(
        &h,
        Some(&function),
        "updated",
        revision_request("sha256:x", Some("require"), true),
    )
    .await;
    let prod = h
        .app
        .aliases
        .get(&h.a, &function.id, &AliasName::default_alias())
        .unwrap();
    assert_eq!(prod.revision_id, rev2.id);
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    let error = failed(&out);
    assert_eq!(error.error_type, "Host.RestoreRequiredUnavailable");
    assert!(
        error.message.contains("revision_mismatch"),
        "{}",
        error.message
    );
    assert!(h.fake.cloned().is_empty());
}

/// Acceptance: revoked and expired snapshots are never loaded.
#[tokio::test]
async fn revoked_and_expired_snapshots_are_refused() {
    let h = harness_with(
        "[snapshots]\nenabled = true\nallow_unverified = true\nttl_seconds = 1",
        |f| f,
    );
    let (function, _) = deploy(&h, "expiring", "require").await;
    let snap = snapshot(&h, &function).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert!(
        failed(&out).message.contains("expired"),
        "{}",
        failed(&out).message
    );
    assert_eq!(snapshot_state(&h, &snap.id), "expired");

    let h = harness();
    let (function, _) = deploy(&h, "revoked", "require").await;
    let snap = snapshot(&h, &function).await;
    let revoked = h
        .app
        .snapshots
        .as_ref()
        .unwrap()
        .revoke(
            &h.a,
            &function.id,
            &SnapshotId::parse(&snap.id).unwrap(),
            "test",
        )
        .await
        .unwrap();
    assert_eq!(revoked.state, "revoked");
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert!(
        failed(&out).message.contains("revoked"),
        "{}",
        failed(&out).message
    );
    assert!(h.fake.cloned().is_empty());
}

/// A second service over the same catalog with other keys or another
/// provider view, to change one input of the compatibility check at a time.
fn service_with(
    h: &Harness,
    provider: Arc<dyn ExecutionProvider>,
    key_byte: u8,
    signing_byte: u8,
    allow_unverified: bool,
) -> Arc<SnapshotService> {
    SnapshotService::new(SnapshotServiceDeps {
        store: SnapshotStore::open(h.dir.path().join("snapshots")).unwrap(),
        key: Arc::new(
            tachyon_serverless_application::durable::ObjectKey::from_bytes([key_byte; 32]),
        ),
        signing: Arc::new(SnapshotSigningKey::from_bytes([signing_byte; 32])),
        provider,
        repos: h.app.repos.clone(),
        artifacts: h.app.artifacts.clone(),
        entrypoints: EntrypointPolicy::new(h.dir.path().join("process")),
        limits: h.app.limits.clone(),
        clock: h.app.clock.clone(),
        ids: h.app.ids.clone(),
        settings: SnapshotSettings {
            ttl: Duration::from_secs(3600),
            allow_unverified,
            handshake_timeout: Duration::from_secs(5),
        },
    })
}

/// Acceptance: key rotation makes existing snapshots stale (revoked).
#[tokio::test]
async fn a_rotated_key_makes_the_snapshot_stale() {
    let h = harness();
    let (function, rev) = deploy(&h, "rotated", "require").await;
    let snap = snapshot(&h, &function).await;
    let rotated = service_with(&h, h.fake.clone(), 0x33, 0x22, true);
    let err = rotated.plan_restore(&function, &rev).await.unwrap_err();
    assert_eq!(err.code, "key_generation_changed", "{err}");
    assert_eq!(snapshot_state(&h, &snap.id), "revoked");

    let h = harness();
    let (function, rev) = deploy(&h, "resigned", "require").await;
    let snap = snapshot(&h, &function).await;
    let resigned = service_with(&h, h.fake.clone(), 0x11, 0x44, true);
    let err = resigned.plan_restore(&function, &rev).await.unwrap_err();
    assert_eq!(err.code, "key_generation_changed", "{err}");
    assert_eq!(snapshot_state(&h, &snap.id), "revoked");
}

/// Acceptance: an unverified capability is refused unless a measurement run
/// allows it.
#[tokio::test]
async fn an_unverified_capability_is_refused_without_allow_unverified() {
    let h = harness();
    let (function, rev) = deploy(&h, "unverified", "require").await;
    snapshot(&h, &function).await;
    let strict = service_with(&h, h.fake.clone(), 0x11, 0x22, false);
    let err = strict.plan_restore(&function, &rev).await.unwrap_err();
    assert_eq!(err.code, "capability_unverified");
}

/// Acceptance: a snapshot of another tenant is refused, even with a valid
/// signature and the same function id.
#[tokio::test]
async fn another_tenants_snapshot_is_refused() {
    let h = harness();
    let (function, rev) = deploy(&h, "tenant", "require").await;
    let snap = snapshot(&h, &function).await;
    let svc = h.app.snapshots.as_ref().unwrap();
    let store = svc.store();
    let id = SnapshotId::parse(&snap.id).unwrap();
    // Forge a correctly signed copy that belongs to tenant B.
    let sealed = store.read_manifest(&id).unwrap();
    let mut manifest: SnapshotManifest = serde_json::from_str(&sealed.manifest).unwrap();
    manifest.tenant_id = TenantId::parse(TENANT_B).unwrap();
    let signing = SnapshotSigningKey::from_bytes([0x22; 32]);
    let forged: SealedManifest = signing.seal(&manifest);
    store.write_manifest(&id, &forged).unwrap();
    let err = svc.plan_restore(&function, &rev).await.unwrap_err();
    assert_eq!(err.code, "tenant_mismatch", "{err}");
    assert!(h.fake.cloned().is_empty());
    // And a tampered manifest (not re-signed) is quarantined.
    let mut tampered = sealed.clone();
    tampered.manifest = tampered
        .manifest
        .replace("\"memory_mib\":256", "\"memory_mib\":257");
    assert_ne!(tampered.manifest, sealed.manifest);
    store.write_manifest(&id, &tampered).unwrap();
    let err = svc.plan_restore(&function, &rev).await.unwrap_err();
    assert_eq!(err.code, "manifest_invalid", "{err}");
    assert_eq!(snapshot_state(&h, &snap.id), "quarantined");
}

/// Acceptance: no production secret or customer data. Snapshots are refused
/// for revisions not marked as a synthetic sample, and for revisions with
/// secret bindings; a policy other than `disabled` cannot even be created
/// with secrets.
#[tokio::test]
async fn snapshots_are_refused_for_secrets_and_non_synthetic_revisions() {
    let h = harness();
    let svc = h.app.snapshots.as_ref().unwrap();
    let (plain, _) = deploy_req(
        &h,
        None,
        "not-synthetic",
        revision_request("sha256:x", None, false),
    )
    .await;
    let err = svc.create(&h.a, &plain.id, None).await.unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequest(ref m) if m.contains("synthetic")),
        "{err}"
    );

    let mut with_secret = revision_request("sha256:x", Some("disabled"), true);
    with_secret.secrets = vec![SecretBindingRequest {
        env_name: "DEMO_SECRET".into(),
        binding_ref: "demo-secret".into(),
    }];
    let (secret_fn, _) = deploy_req(&h, None, "with-secret", with_secret.clone()).await;
    let err = svc.create(&h.a, &secret_fn.id, None).await.unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequest(ref m) if m.contains("secret")),
        "{err}"
    );

    with_secret.restore = Some(RestoreRequest {
        policy: "prefer".into(),
        synthetic_init_sample: true,
    });
    let artifact = h
        .app
        .artifact_service
        .upload(&h.a, b"#!/bin/sh\necho secret-policy\n")
        .await
        .unwrap();
    with_secret.artifact = ArtifactRequest::Binary {
        digest: artifact.digest.to_string(),
    };
    let f = h.app.functions.create(&h.a, "secret-policy", "").unwrap();
    let err = h
        .app
        .revisions
        .create(&h.a, &f.id, &with_secret)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("secret bindings"), "{err}");
    assert!(h.fake.snapshotted().is_empty());
}

/// A source that becomes ready instead of holding at the checkpoint is not
/// snapshotted.
#[tokio::test]
async fn a_source_that_does_not_hold_is_not_snapshotted() {
    let h = harness();
    let (function, _) = deploy(&h, "noholder", "require").await;
    h.fake
        .push_script(FakeGuestScript::RespondOk(serde_json::json!({})));
    let err = h
        .app
        .snapshots
        .as_ref()
        .unwrap()
        .create(&h.a, &function.id, None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("handshake") || err.to_string().contains("checkpoint"),
        "{err}"
    );
    assert!(h.fake.snapshotted().is_empty());
    assert!(
        h.app
            .snapshots
            .as_ref()
            .unwrap()
            .list(&h.a, &function.id)
            .unwrap()
            .is_empty()
    );
}

/// Acceptance: a mismatched profile is refused. The provider's host profile
/// changes (another kernel) after the snapshot was taken.
#[tokio::test]
async fn a_profile_mismatch_is_refused() {
    let h = harness();
    let (function, rev) = deploy(&h, "profile", "require").await;
    snapshot(&h, &function).await;
    let other_kernel: Arc<dyn ExecutionProvider> = Arc::new(OtherKernel(h.fake.clone()));
    let svc = service_with(&h, other_kernel, 0x11, 0x22, true);
    let err = svc.plan_restore(&function, &rev).await.unwrap_err();
    assert_eq!(err.code, "runtime_mismatch", "{err}");
    assert!(err.detail.contains("kernel_sha256"), "{err}");
}

/// A restore also needs `[snapshots] enabled`.
#[tokio::test]
async fn without_the_snapshot_service_require_fails_and_prefer_is_cold() {
    let h = harness_with("[snapshots]\nenabled = false", |f| f);
    assert!(h.app.snapshots.is_none());
    let (function, _) = deploy(&h, "off-require", "require").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert!(failed(&out).message.contains("not_configured"));
    let (function, _) = deploy(&h, "off-prefer", "prefer").await;
    let out = h
        .app
        .invoke
        .invoke(invoke_request(&h, &function, 1))
        .await
        .unwrap();
    assert_eq!(out.detail.attempts[0].0.start_kind, StartKind::Cold);
    assert_eq!(
        out.detail.attempts[0].1.details["restore_fallback"],
        "not_configured"
    );
}

/// The fake with another kernel digest in its host profile.
struct OtherKernel(Arc<FakeExecutionProvider>);

#[async_trait]
impl ExecutionProvider for OtherKernel {
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
        self.0.list_environments().await
    }
    async fn restore_profile(&self) -> Result<RestoreHostProfile, ProviderError> {
        let mut p = self.0.restore_profile().await?;
        p.runtime.kernel_sha256 = Sha256Digest::of_bytes(b"another kernel");
        Ok(p)
    }
    fn snapshot_dir(&self, snapshot_id: &SnapshotId) -> Option<PathBuf> {
        self.0.snapshot_dir(snapshot_id)
    }
    async fn snapshot_environment(
        &self,
        environment_id: &EnvironmentId,
        snapshot_id: &SnapshotId,
    ) -> Result<SnapshotCapture, ProviderError> {
        self.0
            .snapshot_environment(environment_id, snapshot_id)
            .await
    }
    async fn clone_environment(
        &self,
        spec: CloneSpec,
    ) -> Result<(EnvironmentHandle, CloneTimings), ProviderError> {
        self.0.clone_environment(spec).await
    }
}
