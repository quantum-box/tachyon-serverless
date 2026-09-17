//! The restore half of environment preparation (X1, PLT-4653; ADR-0017).
//!
//! Called from [`Driver::prepare_cold`] only for a revision whose restore
//! policy is not `disabled`, so every other revision takes exactly the path it
//! took before. The outcome is one of:
//!
//! - a restored environment (`start_kind = restored`): the provider loaded a
//!   verified snapshot into a new VMM, and the guest's first frame was a
//!   `Reconnect` from the snapshot's source with its boot id, followed by
//!   `Ready` after the after-restore hook. A guest that says `Hello` instead
//!   booted cold and is never counted as restored;
//! - `prefer`: a cold start with the reason recorded in the environment
//!   evidence (`restore_fallback`), `start_kind = cold`;
//! - `require`: the invocation fails with `Host.RestoreRequiredUnavailable`.

use std::time::{Duration, Instant};

use tachyon_serverless_domain::{
    ArtifactRef, EnvironmentId, ErrorClass, ExecutionEnvironment, InvocationError, LogPhase,
    RestorePolicy, ReuseKey, StartKind, UsageEventType,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, CloneSpec, EnvironmentSpec, TerminateReason,
};

use super::{Driver, Prepared, cancel_error, cancel_reason, wait_cancel};
use crate::bridge_session::{
    BridgeSession, LogContext, LogForwarder, RestoreHandshakeError, RestoreIdentity,
};
use crate::snapshot::RestoreUnavailable;

/// Error type of a `require` revision that could not be restored.
pub const RESTORE_REQUIRED_UNAVAILABLE: &str = "Host.RestoreRequiredUnavailable";

pub(super) enum RestoreAttempt {
    Restored(Box<Prepared>),
    /// Start cold; the value goes into the cold environment's evidence.
    Cold(serde_json::Map<String, serde_json::Value>),
    /// The invocation is already settled (failed or cancelled).
    Done,
}

impl Driver {
    pub(super) async fn try_restore(
        &mut self,
        reuse_key: &ReuseKey,
        secret_env_is_empty: bool,
        init_timeout: Duration,
    ) -> RestoreAttempt {
        let policy = self.revision.spec.restore.policy;
        let svc = self.svc.clone();
        let Some(snapshots) = svc.snapshots.get().cloned() else {
            return self.restore_unavailable(
                policy,
                RestoreUnavailable::new(
                    "not_configured",
                    "snapshots are not enabled on this gateway ([snapshots] enabled)",
                ),
            );
        };
        if !secret_env_is_empty {
            return self.restore_unavailable(
                policy,
                RestoreUnavailable::new(
                    "secret_bindings",
                    "a revision with secret bindings is never restored",
                ),
            );
        }
        let plan = match snapshots.plan_restore(&self.function, &self.revision).await {
            Ok(plan) => plan,
            Err(u) => return self.restore_unavailable(policy, u),
        };
        let snapshot_id = plan.manifest.snapshot_id.clone();
        let tenant = self.function.tenant_id.clone();
        let env_id = EnvironmentId::from_ulid(svc.ids.next_ulid());
        let mut env = ExecutionEnvironment::request(
            env_id.clone(),
            tenant.clone(),
            self.revision.id.clone(),
            svc.provider.kind(),
            reuse_key.clone(),
            self.now(),
        )
        .owned_by(svc.dispatcher.id().clone());
        if let Err(e) = svc.repos.environments.insert(env.clone()) {
            return self
                .restore_unavailable(policy, RestoreUnavailable::new("storage", e.to_string()));
        }
        let _ = env.mark_provisioning(self.now());
        self.save_env(&env);
        let logs = LogForwarder::new(
            svc.repos.logs.clone(),
            svc.clock.clone(),
            LogContext {
                tenant_id: tenant.clone(),
                environment_id: env_id.clone(),
                invocation_id: (!self.warmup).then(|| self.invocation_id.clone()),
                max_line_bytes: svc.limits.max_log_line_bytes,
            },
        );
        let artifact = match &self.revision.spec.artifact {
            ArtifactRef::Binary { digest, .. } => match svc.artifacts.get(digest).await {
                Ok(stored) => ArtifactLocation {
                    path: stored.path,
                    digest: stored.digest,
                    size_bytes: stored.size_bytes,
                },
                Err(e) => {
                    return self
                        .restore_failed(
                            policy,
                            &mut env,
                            None,
                            "artifact_unavailable",
                            e.to_string(),
                        )
                        .await;
                }
            },
            ArtifactRef::OciImage { .. } => {
                return self
                    .restore_failed(
                        policy,
                        &mut env,
                        None,
                        "artifact_unavailable",
                        "oci image".into(),
                    )
                    .await;
            }
        };
        let init_wait = init_timeout.min(self.client_remaining());
        let spec = EnvironmentSpec {
            environment_id: env_id.clone(),
            tenant_id: tenant.clone(),
            revision_id: self.revision.id.clone(),
            artifact,
            architecture: self.revision.spec.runtime.architecture,
            egress: self.revision.spec.egress,
            egress_allow: self.revision.spec.egress_allow.clone(),
            resources: self.revision.spec.resources,
            connect_timeout: init_wait,
        };
        logs.platform(
            LogPhase::Boot,
            None,
            &format!(
                "restoring environment {env_id} from snapshot {snapshot_id} (generation {}, policy {})",
                plan.generation,
                policy.as_str()
            ),
        );
        let started = Instant::now();
        let cloned = tokio::select! {
            r = svc.provider.clone_environment(CloneSpec {
                spec,
                snapshot_id: snapshot_id.clone(),
                snapshot_dir: plan.dir.clone(),
                doorbell_port: plan.manifest.devices.doorbell_port,
            }) => r,
            k = wait_cancel(&mut self.cancel_rx) => {
                let _ = env.mark_stopped(self.now());
                self.save_env(&env);
                let _ = svc.provider.terminate_environment(&env_id, cancel_reason(k)).await;
                self.fail_invocation(cancel_error(k, "during restore"));
                return RestoreAttempt::Done;
            }
        };
        let (handle, clone_timings) = match cloned {
            Ok(x) => x,
            Err(e) => {
                return self
                    .restore_failed(policy, &mut env, None, "clone_failed", e.to_string())
                    .await;
            }
        };
        let evidence = handle.evidence.clone();
        let instance_id = format!(
            "rst_{}",
            svc.ids.next_ulid().to_string().to_ascii_lowercase()
        );
        let handshake_wait = svc
            .invoke_cfg
            .handshake_timeout()
            .min(self.client_remaining());
        let identity = RestoreIdentity {
            environment_id: env_id.clone(),
            instance_id: instance_id.clone(),
            generation: plan.generation,
            epoch: env.epoch + 1,
            host_now_ms: self.now().timestamp_millis().max(0) as u64,
        };
        let handshake = BridgeSession::restore_handshake(
            handle.stream,
            &plan.manifest.source_environment_id,
            &plan.manifest.source_boot_id,
            identity,
            logs.clone(),
            handshake_wait,
        )
        .await;
        let reconnected_at = Instant::now();
        let (mut session, reconnects) = match handshake {
            Ok(x) => x,
            Err(e) => {
                let code = e.code();
                if let RestoreHandshakeError::ColdBoot { .. } = &e {
                    logs.platform(
                        LogPhase::Boot,
                        None,
                        "the clone booted cold instead of resuming; it is not counted as restored",
                    );
                }
                return self
                    .restore_failed(policy, &mut env, None, code, e.to_string())
                    .await;
            }
        };
        let _ = env.mark_initializing(evidence, self.now());
        self.save_env(&env);
        let ready = tokio::select! {
            r = session.wait_ready(started + init_wait) => r,
            k = wait_cancel(&mut self.cancel_rx) => {
                let _ = session.shutdown("cancelled").await;
                let _ = env.mark_stopped(self.now());
                self.save_env(&env);
                let _ = svc.provider.terminate_environment(&env_id, cancel_reason(k)).await;
                self.fail_invocation(cancel_error(k, "during restore"));
                return RestoreAttempt::Done;
            }
        };
        let ready = match ready {
            Ok(r) => r,
            Err(e) => {
                return self
                    .restore_failed(
                        policy,
                        &mut env,
                        Some(&mut session),
                        "init_failed",
                        e.to_string(),
                    )
                    .await;
            }
        };
        let ready_at = Instant::now();
        let ms = |a: Instant, b: Instant| b.saturating_duration_since(a).as_millis() as u64;
        // Boot identity of a clone: the source boot id is shared by every
        // copy, so it is paired with the instance id (ADR-0015 決定 4).
        let boot_identity = format!("{}/{instance_id}", plan.manifest.source_boot_id);
        env.record_guest_boot_id(boot_identity.clone());
        self.env_id = Some(env_id.clone());
        self.epoch = env.epoch;
        self.seq = 0;
        self.meter.environment_changed();
        self.meter.idle_pooled_ms = Some(0);
        self.meter.boot_id = Some(boot_identity);
        self.meter.vm_base_boot = Some(clone_timings.loaded_at.saturating_duration_since(started));
        self.meter.user_init = Some(ready_at.saturating_duration_since(reconnected_at));
        self.meter.guest_init_ms = Some(ready.guest_init_ms);
        let d = &mut env.evidence.details;
        d.insert("start".into(), "restored".into());
        d.insert("restore_policy".into(), policy.as_str().into());
        d.insert("snapshot_id".into(), snapshot_id.to_string().into());
        d.insert(
            "snapshot_manifest_digest".into(),
            plan.manifest_digest.clone().into(),
        );
        d.insert("restore_generation".into(), plan.generation.into());
        d.insert("restore_instance_id".into(), instance_id.into());
        d.insert(
            "restore_source_environment_id".into(),
            plan.manifest.source_environment_id.to_string().into(),
        );
        d.insert("restore_reconnects".into(), reconnects.into());
        d.insert("restore_verify_ms".into(), plan.verify_ms.into());
        d.insert(
            "restore_load_ms".into(),
            ms(clone_timings.started_at, clone_timings.loaded_at).into(),
        );
        if let Some(bell) = clone_timings.doorbell_at {
            d.insert(
                "restore_doorbell_ms".into(),
                ms(clone_timings.started_at, bell).into(),
            );
        }
        d.insert(
            "restore_reconnect_ms".into(),
            ms(started, reconnected_at).into(),
        );
        d.insert("restore_ready_ms".into(), ms(started, ready_at).into());
        d.insert("guest_init_ms".into(), ready.guest_init_ms.into());
        let _ = env.mark_ready(self.now());
        if let Some(g) = &self.grant {
            g.ready();
            g.start_result(true);
        }
        self.save_env(&env);
        self.seq += 1;
        self.emit_usage(
            &env_id,
            None,
            UsageEventType::EnvironmentStarted,
            self.seq,
            None,
            0,
            0,
        )
        .await;
        logs.platform(
            LogPhase::Boot,
            None,
            &format!(
                "restored from snapshot {snapshot_id}: reconnect {} ms, ready {} ms after the clone started",
                ms(started, reconnected_at),
                ms(started, ready_at)
            ),
        );
        RestoreAttempt::Restored(Box::new(Prepared {
            env,
            session,
            start_kind: StartKind::Restored,
            environment_boot_ms: ms(started, reconnected_at),
            runtime_init_ms: ms(reconnected_at, ready_at),
            warm: None,
            logs,
        }))
    }

    /// No restore was attempted: `require` fails the invocation, `prefer`
    /// starts cold with the reason.
    fn restore_unavailable(&self, policy: RestorePolicy, u: RestoreUnavailable) -> RestoreAttempt {
        tracing::info!(
            invocation_id = %self.invocation_id,
            revision_id = %self.revision.id,
            policy = policy.as_str(),
            code = %u.code,
            detail = %u.detail,
            "restore unavailable"
        );
        match policy {
            RestorePolicy::Require => {
                self.fail_invocation(InvocationError::new(
                    ErrorClass::InitError,
                    RESTORE_REQUIRED_UNAVAILABLE,
                    format!("restore required but unavailable: {u}"),
                ));
                RestoreAttempt::Done
            }
            _ => {
                let mut note = serde_json::Map::new();
                note.insert("restore_policy".into(), policy.as_str().into());
                note.insert("restore_fallback".into(), u.code.into());
                note.insert("restore_fallback_detail".into(), u.detail.into());
                RestoreAttempt::Cold(note)
            }
        }
    }

    /// A restore was attempted and failed: the clone is stopped and
    /// terminated first, then [`Self::restore_unavailable`].
    async fn restore_failed(
        &self,
        policy: RestorePolicy,
        env: &mut ExecutionEnvironment,
        session: Option<&mut BridgeSession>,
        code: &str,
        detail: String,
    ) -> RestoreAttempt {
        if let Some(session) = session {
            let _ = session.shutdown("restore failed").await;
        }
        let _ = env.mark_failed(format!("restore failed: {code}"), self.now());
        self.save_env(env);
        let _ = self
            .svc
            .provider
            .terminate_environment(&env.id, TerminateReason::InitFailed)
            .await;
        self.restore_unavailable(policy, RestoreUnavailable::new(code, detail))
    }
}
