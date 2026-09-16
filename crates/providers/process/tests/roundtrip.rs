//! Process provider end-to-end: create an environment (spawns the real
//! bridge), act as the host over the returned stream, invoke the bridge's
//! built-in self-test user, then terminate and verify nothing is left.
//!
//! The bridge binary comes from `CARGO_BIN_EXE_tachyon-serverless-runtime-bridge`
//! when set, otherwise from `target/debug` of the workspace. When neither
//! exists the test prints a skip message and returns (CI does not fail).

use std::path::PathBuf;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tachyon_serverless_domain::{Architecture, EnvironmentId, RevisionId, Sha256Digest, TenantId};
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, decode_message, encode_message,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, BridgeStream, EnvironmentObservation, EnvironmentSpec, ExecutionProvider,
    ProviderError, TerminateReason,
};
use tachyon_serverless_provider_process::{ProcessProvider, ProcessProviderConfig};
use tokio::time::timeout;
use tokio_util::codec::Framed;

const T: Duration = Duration::from_secs(20);

fn locate_bridge() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_tachyon-serverless-runtime-bridge") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.join("../../..");
    let mut candidates = Vec::new();
    if let Ok(target) = std::env::var("CARGO_TARGET_DIR") {
        candidates.push(PathBuf::from(target).join("debug/tachyon-serverless-runtime-bridge"));
    }
    candidates.push(workspace.join("target/debug/tachyon-serverless-runtime-bridge"));
    candidates.into_iter().find(|p| p.is_file())
}

fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 only checks for existence.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

async fn recv(host: &mut Framed<Box<dyn BridgeStream>, FrameCodec>) -> Option<GuestMessage> {
    loop {
        let frame = timeout(T, host.next()).await.expect("recv timeout")?;
        let msg: GuestMessage = decode_message(&frame.unwrap()).unwrap();
        match msg {
            GuestMessage::Heartbeat { .. } | GuestMessage::Log { .. } => continue,
            other => return Some(other),
        }
    }
}

async fn send(host: &mut Framed<Box<dyn BridgeStream>, FrameCodec>, msg: HostMessage) {
    timeout(T, host.send(encode_message(&msg).unwrap()))
        .await
        .expect("send timeout")
        .unwrap();
}

fn spec(id: EnvironmentId, bridge: &std::path::Path, connect_timeout: Duration) -> EnvironmentSpec {
    EnvironmentSpec {
        environment_id: id,
        tenant_id: TenantId::generate(),
        revision_id: RevisionId::generate(),
        artifact: ArtifactLocation {
            path: bridge.to_path_buf(),
            digest: Sha256Digest::of_bytes(b"bridge"),
            size_bytes: 0,
        },
        architecture: Architecture::host().expect("supported host"),
        resources: Default::default(),
        egress: Default::default(),
        connect_timeout,
    }
}

#[tokio::test]
async fn create_invoke_terminate_roundtrip() {
    let Some(bridge) = locate_bridge() else {
        eprintln!(
            "SKIP: runtime bridge binary not found; run `cargo build -p tachyon-serverless-runtime-bridge` first"
        );
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let workdir = tmp.path().join("process");
    let provider = ProcessProvider::new(ProcessProviderConfig {
        bridge_binary: bridge.clone(),
        workdir: workdir.clone(),
    });
    assert!(provider.preflight().await.unwrap().ok);
    provider
        .validate_artifact(
            &ArtifactLocation {
                path: bridge.clone(),
                digest: Sha256Digest::of_bytes(b"bridge"),
                size_bytes: 0,
            },
            Architecture::host().unwrap(),
        )
        .await
        .unwrap();

    let id = EnvironmentId::generate();
    let handle = timeout(T, provider.create_environment(spec(id.clone(), &bridge, T)))
        .await
        .expect("create timeout")
        .unwrap();
    assert_eq!(handle.environment_id, id);
    let pid = handle.evidence.host_pid.expect("host pid");
    assert!(pid_alive(pid));
    assert_eq!(handle.evidence.details["provider"], "process");
    assert_eq!(handle.evidence.details["isolation"], "none");
    assert!(handle.connected_at >= handle.created_at);
    let env_dir = workdir.join(id.as_str());
    assert!(env_dir.join("bridge.sock").exists());
    assert!(env_dir.join("bridge.pid").exists());
    assert!(env_dir.join("bridge.stdout").exists());
    assert!(env_dir.join("bridge.stderr").exists());

    // Duplicate id is refused while the first one lives.
    assert!(matches!(
        provider
            .create_environment(spec(id.clone(), &bridge, T))
            .await,
        Err(ProviderError::InvalidSpec(_))
    ));
    assert_eq!(
        provider.list_environments().await.unwrap(),
        vec![id.clone()]
    );
    assert_eq!(
        provider.observe_environment(&id).await.unwrap(),
        EnvironmentObservation::Running {
            host_pid: Some(pid)
        }
    );

    // Host side of the protocol.
    let mut host = Framed::new(handle.stream, FrameCodec);
    match recv(&mut host).await {
        Some(GuestMessage::Hello {
            environment_id,
            protocol_version,
            ..
        }) => {
            assert_eq!(environment_id, id.as_str());
            assert_eq!(
                protocol_version,
                tachyon_serverless_protocol::PROTOCOL_VERSION
            );
        }
        other => panic!("expected hello, got {other:?}"),
    }
    send(
        &mut host,
        HostMessage::HelloAck {
            environment_id: id.as_str().into(),
            epoch: 1,
            entrypoint: bridge.to_string_lossy().into_owned(),
            args: vec!["self-test-user".into()],
            env: vec![],
            working_dir: tmp.path().to_string_lossy().into_owned(),
            init_timeout_ms: 15_000,
            max_response_bytes: 1 << 20,
            max_log_line_bytes: 4096,
        },
    )
    .await;
    assert!(matches!(
        recv(&mut host).await,
        Some(GuestMessage::Ready { .. })
    ));
    send(
        &mut host,
        HostMessage::Invoke {
            invocation_id: "inv_01hzzzzzzzzzzzzzzzzzzzzzzb".into(),
            attempt_id: "att_01hzzzzzzzzzzzzzzzzzzzzzzb".into(),
            epoch: 1,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 4_102_444_800_000,
            remaining_ms: 60_000,
            trace_id: "t".into(),
            payload: serde_json::json!({"ping": "pong"}),
        },
    )
    .await;
    match recv(&mut host).await {
        Some(GuestMessage::Response {
            attempt_id,
            payload,
            ..
        }) => {
            assert_eq!(attempt_id, "att_01hzzzzzzzzzzzzzzzzzzzzzzb");
            assert_eq!(payload["echo"], serde_json::json!({"ping": "pong"}));
            assert_eq!(payload["environment_id"], id.as_str());
        }
        other => panic!("expected response, got {other:?}"),
    }

    // Terminate without a Shutdown frame: the provider must clean up on its own.
    let report = timeout(
        T,
        provider.terminate_environment(&id, TerminateReason::Completed),
    )
    .await
    .expect("terminate timeout")
    .unwrap();
    assert!(report.was_running);
    assert!(report.cleaned.iter().any(|c| c.ends_with("bridge.sock")));
    assert!(
        report
            .cleaned
            .iter()
            .any(|c| c == &env_dir.display().to_string())
    );
    assert!(!env_dir.exists());
    assert!(!pid_alive(pid), "bridge {pid} still alive");
    assert_eq!(
        provider.observe_environment(&id).await.unwrap(),
        EnvironmentObservation::NotFound
    );
    assert!(provider.list_environments().await.unwrap().is_empty());

    // Idempotent.
    let again = provider
        .terminate_environment(&id, TerminateReason::Completed)
        .await
        .unwrap();
    assert!(!again.was_running);
    assert!(again.cleaned.is_empty());
}

#[tokio::test]
async fn connect_timeout_kills_and_cleans_up() {
    let Some(bridge) = locate_bridge() else {
        eprintln!("SKIP: runtime bridge binary not found");
        return;
    };
    // A bridge pointed at a socket nobody accepts on would still connect to
    // our listener, so make the bridge itself unable to connect: use a
    // wrapper that sleeps instead. /bin/sh is available on macOS and Linux.
    let tmp = tempfile::tempdir().unwrap();
    let fake = tmp.path().join("slow-bridge.sh");
    std::fs::write(&fake, "#!/bin/sh\nsleep 30\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let workdir = tmp.path().join("process");
    let provider = ProcessProvider::new(ProcessProviderConfig {
        bridge_binary: fake,
        workdir: workdir.clone(),
    });
    let id = EnvironmentId::generate();
    let err = timeout(
        Duration::from_secs(15),
        provider.create_environment(spec(id.clone(), &bridge, Duration::from_millis(500))),
    )
    .await
    .expect("create should time out quickly")
    .unwrap_err();
    assert!(
        matches!(
            err,
            ProviderError::Timeout {
                stage: "bridge_connect"
            }
        ),
        "{err}"
    );
    assert!(!workdir.join(id.as_str()).exists());
    assert!(provider.list_environments().await.unwrap().is_empty());
}
