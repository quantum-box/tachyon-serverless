//! Experimental restore handshakes (X1, PLT-4653; docs/protocol.md §A-X1).
//!
//! ```text
//! source:  handshake_for_snapshot : Hello(v3) -> HelloAck{snapshot_hold}
//!          wait_checkpoint        : Log* -> CheckpointWaiting   (never Ready)
//! clone:   restore_handshake      : Reconnect(boot id == source) -> Restore{identity}
//!                                   Hello -> ColdBoot (never counted as restored)
//!          wait_ready             : unchanged
//! ```

use std::time::{Duration, Instant};

use tachyon_serverless_domain::EnvironmentId;
use tachyon_serverless_protocol::{CHECKPOINT_PHASE, GuestMessage, HostMessage, supports_restore};
use tachyon_serverless_provider_port::BridgeStream;

use super::{BridgeSession, HelloAckParams, HelloInfo, LogForwarder, SessionError, message_name};

/// What the guest reported when it reached the checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReport {
    pub lifecycle_phase: String,
    pub lifecycle_version: u32,
    pub after_restore_ran: bool,
}

/// The identity a restored copy receives, after the restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreIdentity {
    /// The clone's own environment id.
    pub environment_id: EnvironmentId,
    pub instance_id: String,
    pub generation: u64,
    pub epoch: u64,
    pub host_now_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreHandshakeError {
    /// The guest said `Hello`: it booted instead of resuming. Never a restore.
    #[error(
        "cold boot detected on a restored VMM: the guest sent hello (boot id {guest_boot_id:?})"
    )]
    ColdBoot { guest_boot_id: Option<String> },
    /// The guest reconnected but is not the snapshot's source.
    #[error("the reconnecting guest is not the snapshot source: {0}")]
    NotTheSource(String),
    #[error(transparent)]
    Session(#[from] SessionError),
}

impl RestoreHandshakeError {
    /// Stable reason code (evidence, metrics).
    pub fn code(&self) -> &'static str {
        match self {
            Self::ColdBoot { .. } => "cold_boot_detected",
            Self::NotTheSource(_) => "not_the_source",
            Self::Session(SessionError::Timeout { .. }) => "reconnect_timeout",
            Self::Session(_) => "reconnect_failed",
        }
    }
}

impl BridgeSession {
    /// Handshake with a guest that is to be snapshotted: it must speak
    /// protocol version 3, and `HelloAck` asks it to hold at the checkpoint.
    /// `params.env` must not carry secrets (checked by the caller: snapshots
    /// are refused for revisions with secret bindings).
    pub async fn handshake_for_snapshot(
        stream: Box<dyn BridgeStream>,
        expected_environment_id: &EnvironmentId,
        epoch: u64,
        params: HelloAckParams,
        logs: LogForwarder,
        timeout: Duration,
    ) -> Result<(Self, HelloInfo), SessionError> {
        Self::handshake_inner(
            stream,
            expected_environment_id,
            epoch,
            params,
            logs,
            timeout,
            true,
        )
        .await
    }

    /// Wait until the held guest reports the checkpoint. `Ready` instead
    /// means the process did not use the lifecycle (or the bridge ignored the
    /// hold): nothing may be snapshotted.
    pub async fn wait_checkpoint(
        &mut self,
        deadline: Instant,
    ) -> Result<CheckpointReport, SessionError> {
        loop {
            match self.next_message(deadline, "checkpoint").await? {
                GuestMessage::CheckpointWaiting {
                    lifecycle_phase,
                    lifecycle_version,
                    after_restore_ran,
                } => {
                    if lifecycle_phase != CHECKPOINT_PHASE || after_restore_ran {
                        return Err(SessionError::Protocol(format!(
                            "the guest reported phase `{lifecycle_phase}` (after_restore_ran = \
                             {after_restore_ran}); a snapshot is only taken at `{CHECKPOINT_PHASE}` \
                             before after_restore"
                        )));
                    }
                    return Ok(CheckpointReport {
                        lifecycle_phase,
                        lifecycle_version,
                        after_restore_ran,
                    });
                }
                GuestMessage::Ready { .. } => {
                    return Err(SessionError::Protocol(
                        "the guest became ready without holding at the checkpoint (does the \
                         function use the experimental lifecycle?)"
                            .into(),
                    ));
                }
                GuestMessage::InitError {
                    error_type,
                    message,
                    exit_code,
                } => {
                    return Err(SessionError::InitError {
                        error_type,
                        message,
                        exit_code,
                    });
                }
                GuestMessage::Exited { exit_code, signal } => {
                    return Err(SessionError::InitError {
                        error_type: "Runtime.Exited".into(),
                        message: format!(
                            "user process exited before the checkpoint (exit_code={exit_code:?}, signal={signal:?})"
                        ),
                        exit_code,
                    });
                }
                GuestMessage::Log {
                    stream,
                    phase,
                    attempt_id,
                    line,
                    ..
                } => self.logs.forward_guest(stream, phase, attempt_id, &line),
                GuestMessage::Heartbeat { .. } | GuestMessage::Pong { .. } => {}
                other => {
                    return Err(SessionError::Protocol(format!(
                        "unexpected {} while waiting for the checkpoint",
                        message_name(&other)
                    )));
                }
            }
        }
    }

    /// First exchange with a restored copy. Its first frame must be
    /// `Reconnect` from the snapshot's source environment with the source's
    /// boot id; a `Hello` is a cold boot and is refused
    /// ([`RestoreHandshakeError::ColdBoot`]). Then the copy receives its own
    /// identity and the host's clock in `Restore`.
    pub async fn restore_handshake(
        stream: Box<dyn BridgeStream>,
        source_environment_id: &EnvironmentId,
        source_boot_id: &str,
        identity: RestoreIdentity,
        logs: LogForwarder,
        timeout: Duration,
    ) -> Result<(Self, u64), RestoreHandshakeError> {
        let mut session =
            Self::new_unconnected(stream, &identity.environment_id, identity.epoch, logs);
        let deadline = Instant::now() + timeout;
        let (version, environment_id, guest_boot_id, reconnects) =
            match session.next_message(deadline, "reconnect").await? {
                GuestMessage::Reconnect {
                    protocol_version,
                    environment_id,
                    guest_boot_id,
                    reconnects,
                    ..
                } => (protocol_version, environment_id, guest_boot_id, reconnects),
                GuestMessage::Hello { guest_boot_id, .. } => {
                    session
                        .reject("expected a restored guest; a hello means it booted cold")
                        .await;
                    return Err(RestoreHandshakeError::ColdBoot { guest_boot_id });
                }
                other => {
                    let reason = format!("expected reconnect, got {}", message_name(&other));
                    session.reject(&reason).await;
                    return Err(RestoreHandshakeError::Session(SessionError::Protocol(
                        reason,
                    )));
                }
            };
        let mismatch = if !supports_restore(version) {
            Some(format!("protocol version {version} has no restore frames"))
        } else if environment_id != source_environment_id.as_str() {
            Some(format!(
                "reconnected as {environment_id}, the snapshot source is {source_environment_id}"
            ))
        } else if guest_boot_id.as_deref() != Some(source_boot_id) {
            Some(format!(
                "boot id {guest_boot_id:?} differs from the snapshot source's"
            ))
        } else {
            None
        };
        if let Some(reason) = mismatch {
            session.reject(&reason).await;
            return Err(RestoreHandshakeError::NotTheSource(reason));
        }
        session
            .send(&HostMessage::Restore {
                environment_id: identity.environment_id.to_string(),
                instance_id: identity.instance_id,
                generation: identity.generation,
                epoch: identity.epoch,
                host_now_ms: identity.host_now_ms,
            })
            .await?;
        Ok((session, reconnects))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{SinkExt, StreamExt};
    use tachyon_serverless_domain::{Limits, SystemClock, TenantId};
    use tachyon_serverless_protocol::{FrameCodec, decode_message, encode_message};
    use tokio_util::codec::Framed;

    use super::*;
    use crate::bridge_session::LogContext;
    use crate::repository::InMemoryStore;

    fn logs(env: &EnvironmentId) -> LogForwarder {
        LogForwarder::new(
            Arc::new(InMemoryStore::new(Limits::default())),
            Arc::new(SystemClock),
            LogContext {
                tenant_id: TenantId::generate(),
                environment_id: env.clone(),
                invocation_id: None,
                max_line_bytes: 1024,
            },
        )
    }

    fn identity(env: &EnvironmentId) -> RestoreIdentity {
        RestoreIdentity {
            environment_id: env.clone(),
            instance_id: "rst_a".into(),
            generation: 1,
            epoch: 1,
            host_now_ms: 42,
        }
    }

    fn params() -> HelloAckParams {
        HelloAckParams {
            entrypoint: "/function/app".into(),
            args: vec![],
            env: vec![],
            working_dir: "/function".into(),
            init_timeout: Duration::from_secs(5),
            max_response_bytes: 1024,
            max_log_line_bytes: 1024,
        }
    }

    /// PLT-4653 acceptance: a guest that boots (says `Hello`) where a restore
    /// was expected is refused and never becomes a restored session.
    #[tokio::test]
    async fn a_cold_boot_on_a_restore_is_refused() {
        let (host, guest) = tokio::io::duplex(1 << 16);
        let mut g = Framed::new(guest, FrameCodec);
        let src = EnvironmentId::generate();
        let clone = EnvironmentId::generate();
        g.send(
            encode_message(&GuestMessage::Hello {
                protocol_version: 3,
                bridge_version: "x".into(),
                environment_id: src.to_string(),
                guest_boot_id: Some("boot-src".into()),
                architecture: "aarch64".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let err = BridgeSession::restore_handshake(
            Box::new(host),
            &src,
            "boot-src",
            identity(&clone),
            logs(&clone),
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "cold_boot_detected");
        let reply: HostMessage = decode_message(&g.next().await.unwrap().unwrap()).unwrap();
        assert!(matches!(reply, HostMessage::HelloReject { .. }));
    }

    /// A reconnect with another boot id (another guest) is refused.
    #[tokio::test]
    async fn a_reconnect_from_another_boot_is_not_the_source() {
        let (host, guest) = tokio::io::duplex(1 << 16);
        let mut g = Framed::new(guest, FrameCodec);
        let src = EnvironmentId::generate();
        let clone = EnvironmentId::generate();
        g.send(
            encode_message(&GuestMessage::Reconnect {
                protocol_version: 3,
                environment_id: src.to_string(),
                guest_boot_id: Some("another-boot".into()),
                reconnects: 1,
                lost: "doorbell".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let err = BridgeSession::restore_handshake(
            Box::new(host),
            &src,
            "boot-src",
            identity(&clone),
            logs(&clone),
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "not_the_source");
    }

    #[tokio::test]
    async fn the_source_reconnect_receives_its_new_identity() {
        let (host, guest) = tokio::io::duplex(1 << 16);
        let mut g = Framed::new(guest, FrameCodec);
        let src = EnvironmentId::generate();
        let clone = EnvironmentId::generate();
        g.send(
            encode_message(&GuestMessage::Reconnect {
                protocol_version: 3,
                environment_id: src.to_string(),
                guest_boot_id: Some("boot-src".into()),
                reconnects: 1,
                lost: "doorbell".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let (session, reconnects) = BridgeSession::restore_handshake(
            Box::new(host),
            &src,
            "boot-src",
            identity(&clone),
            logs(&clone),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(reconnects, 1);
        assert_eq!(session.environment_id(), &clone);
        let reply: HostMessage = decode_message(&g.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(
            reply,
            HostMessage::Restore {
                environment_id: clone.to_string(),
                instance_id: "rst_a".into(),
                generation: 1,
                epoch: 1,
                host_now_ms: 42,
            }
        );
    }

    /// A version-2 guest is never asked for a hold, and a held guest that
    /// becomes ready instead of reporting the checkpoint is refused.
    #[tokio::test]
    async fn a_snapshot_needs_a_version_3_guest_that_holds() {
        let env = EnvironmentId::generate();
        let (host, guest) = tokio::io::duplex(1 << 16);
        let mut g = Framed::new(guest, FrameCodec);
        let hello = |v| GuestMessage::Hello {
            protocol_version: v,
            bridge_version: "x".into(),
            environment_id: env.to_string(),
            guest_boot_id: None,
            architecture: "aarch64".into(),
        };
        g.send(encode_message(&hello(2)).unwrap()).await.unwrap();
        let err = BridgeSession::handshake_for_snapshot(
            Box::new(host),
            &env,
            1,
            params(),
            logs(&env),
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SessionError::HandshakeRejected(_)), "{err}");

        let (host, guest) = tokio::io::duplex(1 << 16);
        let mut g = Framed::new(guest, FrameCodec);
        g.send(encode_message(&hello(3)).unwrap()).await.unwrap();
        let (mut s, _) = BridgeSession::handshake_for_snapshot(
            Box::new(host),
            &env,
            1,
            params(),
            logs(&env),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        match decode_message::<HostMessage>(&g.next().await.unwrap().unwrap()).unwrap() {
            HostMessage::HelloAck { snapshot_hold, .. } => assert!(snapshot_hold),
            other => panic!("{other:?}"),
        }
        g.send(encode_message(&GuestMessage::Ready { init_ms: 1 }).unwrap())
            .await
            .unwrap();
        let err = s
            .wait_checkpoint(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("without holding"), "{err}");

        // The normal handshake of a version-2 guest still works unchanged.
        let (host, guest) = tokio::io::duplex(1 << 16);
        let mut g = Framed::new(guest, FrameCodec);
        g.send(encode_message(&hello(2)).unwrap()).await.unwrap();
        let (_s, info) = BridgeSession::handshake(
            Box::new(host),
            &env,
            1,
            params(),
            logs(&env),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(info.protocol_version, 2);
        match decode_message::<HostMessage>(&g.next().await.unwrap().unwrap()).unwrap() {
            HostMessage::HelloAck { snapshot_hold, .. } => assert!(!snapshot_hold),
            other => panic!("{other:?}"),
        }
    }

    /// Negotiation: versions 2 and 3 are accepted, anything else is rejected.
    #[tokio::test]
    async fn only_protocol_versions_2_and_3_are_accepted() {
        for (version, accepted) in [(1, false), (2, true), (3, true), (4, false)] {
            let env = EnvironmentId::generate();
            let (host, guest) = tokio::io::duplex(1 << 16);
            let mut g = Framed::new(guest, FrameCodec);
            g.send(
                encode_message(&GuestMessage::Hello {
                    protocol_version: version,
                    bridge_version: "x".into(),
                    environment_id: env.to_string(),
                    guest_boot_id: None,
                    architecture: "aarch64".into(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
            let result = BridgeSession::handshake(
                Box::new(host),
                &env,
                1,
                params(),
                logs(&env),
                Duration::from_secs(2),
            )
            .await;
            assert_eq!(result.is_ok(), accepted, "version {version}");
            let reply: HostMessage = decode_message(&g.next().await.unwrap().unwrap()).unwrap();
            assert_eq!(
                matches!(reply, HostMessage::HelloAck { .. }),
                accepted,
                "version {version}: {reply:?}"
            );
        }
    }
}
