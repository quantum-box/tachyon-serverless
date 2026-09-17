//! Dead letters: list, show and redrive (PLT-4640, docs/adr/0013).
//!
//! - Reading needs `Invoke` and is scoped to the caller's tenant: a dead
//!   letter of another tenant answers 404, exactly like one that does not
//!   exist.
//! - A **redrive** needs `Invoke` **and** `Redrive`. It creates a **new**
//!   asynchronous invocation (the dead-lettered one is terminal, and terminal
//!   is final), linked both ways: the redrive record names the dead letter,
//!   the source invocation, the new invocation, who asked, when and why. The
//!   new invocation is pinned to the source's revision unless the caller names
//!   another revision **of the same function** explicitly. Its input is the
//!   source's input by reference (the same inline bytes or the same object,
//!   in the same tenant), never copied anywhere else. It is committed together
//!   with its outbox event in one transaction and delivered by the publisher,
//!   like any acceptance. Poison entries have no invocation and cannot be
//!   redriven.

use std::sync::Arc;

use tokio::sync::Notify;

use tachyon_serverless_domain::{
    Clock, DeadLetterId, Deadlines, FunctionId, IdGenerator, Invocation, InvocationId,
    InvocationMode, RedriveId, RevisionId,
};
use tachyon_serverless_provider_port::Principal;

use super::{
    AsyncRefusal, ENVELOPE_VERSION, INVOKE_TOPIC, InvokeAsyncConfig, InvokeEnvelope, QueueHealth,
    backlog_refusal,
};
use crate::authz::{ensure_tenant, require_invoke, require_redrive};
use crate::control::{InvokeGate, Resolved};
use crate::error::AppError;
use crate::repository::{
    AsyncDispatchRepository, AsyncInput, AsyncInvocationRepository, BacklogLimits, DeadLetter,
    DeadLetterReason, DeadLetterStatus, InvocationRepository, OutboxEvent, Redrive, RedriveWrite,
    RepoError,
};

/// Upper bound of a redrive reason.
pub const MAX_REDRIVE_REASON_BYTES: usize = 1024;

#[derive(Debug, Clone)]
pub struct DeadLetterView {
    pub dead_letter: DeadLetter,
    /// Oldest first.
    pub redrives: Vec<Redrive>,
}

#[derive(Debug, Clone)]
pub struct RedriveRequest {
    pub principal: Principal,
    pub dead_letter_id: DeadLetterId,
    /// Another revision of the same function; `None` keeps the original.
    pub revision_id: Option<RevisionId>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RedriveResult {
    pub redrive: Redrive,
    pub invocation: Invocation,
}

pub struct DeadLetterService {
    dispatch: Arc<dyn AsyncDispatchRepository>,
    ledger: Arc<dyn AsyncInvocationRepository>,
    invocations: Arc<dyn InvocationRepository>,
    gate: Arc<InvokeGate>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    config: InvokeAsyncConfig,
    health: Arc<QueueHealth>,
    wake: Arc<Notify>,
    metrics: Arc<crate::metrics::dispatch::AsyncDispatchMetrics>,
}

impl std::fmt::Debug for DeadLetterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadLetterService").finish_non_exhaustive()
    }
}

impl DeadLetterService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dispatch: Arc<dyn AsyncDispatchRepository>,
        ledger: Arc<dyn AsyncInvocationRepository>,
        invocations: Arc<dyn InvocationRepository>,
        gate: Arc<InvokeGate>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGenerator>,
        config: InvokeAsyncConfig,
        health: Arc<QueueHealth>,
        wake: Arc<Notify>,
        metrics: Arc<crate::metrics::dispatch::AsyncDispatchMetrics>,
    ) -> Self {
        Self {
            dispatch,
            ledger,
            invocations,
            gate,
            clock,
            ids,
            config,
            health,
            wake,
            metrics,
        }
    }

    /// The dead letters of `function` in the caller's tenant, newest first.
    /// Another tenant's function lists nothing (indistinguishable from a
    /// function without dead letters).
    pub fn list(
        &self,
        principal: &Principal,
        function: &FunctionId,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, AppError> {
        require_invoke(principal)?;
        Ok(self
            .dispatch
            .list_dead_letters(&principal.tenant_id, function, limit.clamp(1, 1000))?)
    }

    pub fn get(
        &self,
        principal: &Principal,
        id: &DeadLetterId,
    ) -> Result<DeadLetterView, AppError> {
        require_invoke(principal)?;
        let dead_letter = self.owned(principal, id)?;
        let redrives = self.dispatch.redrives_of(id)?;
        Ok(DeadLetterView {
            dead_letter,
            redrives,
        })
    }

    /// The dead letter of `invocation` and the redrive that created it, for
    /// the invocation view. The caller has checked the invocation's tenant.
    pub fn links(
        &self,
        invocation: &InvocationId,
    ) -> Result<(Option<DeadLetter>, Option<Redrive>), AppError> {
        Ok((
            self.dispatch.dead_letter_of(invocation)?,
            self.dispatch.redrive_creating(invocation)?,
        ))
    }

    fn owned(&self, principal: &Principal, id: &DeadLetterId) -> Result<DeadLetter, AppError> {
        let dead = self
            .dispatch
            .dead_letter(id)?
            .ok_or_else(|| AppError::not_found("dead letter not found"))?;
        ensure_tenant(principal, &dead.tenant_id, "dead letter")?;
        Ok(dead)
    }

    pub async fn redrive(&self, req: RedriveRequest) -> Result<RedriveResult, AppError> {
        require_redrive(&req.principal)?;
        if let Some(reason) = &req.reason
            && reason.len() > MAX_REDRIVE_REASON_BYTES
        {
            return Err(AppError::InvalidRequest(format!(
                "reason must be at most {MAX_REDRIVE_REASON_BYTES} bytes"
            )));
        }
        let dead = self.owned(&req.principal, &req.dead_letter_id)?;
        if dead.status != DeadLetterStatus::Open {
            return Err(AppError::Conflict(format!(
                "dead letter {} is already {}",
                dead.id,
                dead.status.as_str()
            )));
        }
        let (Some(source_id), Some(function_id)) = (&dead.invocation_id, &dead.function_id) else {
            return Err(AppError::Conflict(format!(
                "dead letter {} is a poison event ({}); it names no invocation to redrive",
                dead.id,
                DeadLetterReason::Poison.as_str()
            )));
        };
        let source = self
            .invocations
            .get(source_id)?
            .ok_or_else(|| AppError::not_found("dead letter not found"))?;
        ensure_tenant(&req.principal, &source.tenant_id, "dead letter")?;
        let input = self
            .ledger
            .async_input(&source.id)?
            .filter(|i| i.tenant_id == source.tenant_id)
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "the input of invocation {} is no longer stored",
                    source.id
                ))
            })?;

        // The revision: the original one unless another revision of the same
        // function is named explicitly. Resolved like an acceptance, under the
        // caller's principal (a deleted function or an unready revision is
        // refused here).
        let target = req
            .revision_id
            .clone()
            .unwrap_or_else(|| source.revision_id.clone());
        let Resolved {
            function, revision, ..
        } = self
            .gate
            .cache()
            .resolve(&req.principal, function_id, None, Some(&target))
            .await?;
        if revision.function_id != source.function_id || function.id != source.function_id {
            return Err(AppError::InvalidRequest(
                "revision_id must name a revision of the dead letter's function".into(),
            ));
        }

        let now = self.clock.now();
        let limits = BacklogLimits {
            max_pending: self.config.max_pending_events,
            max_pending_age: self.config.max_pending_age(),
        };
        let stats = self.ledger.outbox_stats()?;
        if let Some(refusal) = backlog_refusal(&stats, &limits, self.health.condition(), now) {
            return Err(refusal);
        }
        let id = InvocationId::from_ulid(self.ids.next_ulid());
        let exec = &revision.spec.execution;
        let queue_deadline = now + self.config.queue_deadline();
        let deadlines = Deadlines {
            queue_deadline,
            init_deadline: None,
            execution_deadline: None,
            client_deadline: queue_deadline
                + chrono::Duration::seconds(
                    i64::from(exec.timeout_seconds)
                        + i64::from(exec.initialization_timeout_seconds),
                ),
        };
        let invocation = Invocation::accept(
            id.clone(),
            source.tenant_id.clone(),
            source.function_id.clone(),
            None,
            revision.id.clone(),
            InvocationMode::Async,
            source.event_kind,
            deadlines,
            None,
            source.input_digest.clone(),
            source.input_size_bytes,
            id.to_string(),
            now,
        )?;
        let new_input = AsyncInput {
            invocation_id: id.clone(),
            ..input
        };
        let envelope = InvokeEnvelope {
            version: ENVELOPE_VERSION,
            invocation_id: id.clone(),
            tenant_id: invocation.tenant_id.clone(),
            function_id: invocation.function_id.clone(),
            revision_id: invocation.revision_id.clone(),
            event_kind: invocation.event_kind,
            input_digest: invocation.input_digest.clone(),
            input_size_bytes: invocation.input_size_bytes,
            input_storage: new_input.storage().to_string(),
            accepted_at: now,
            queue_deadline,
            trace_id: invocation.trace_id.clone(),
            generation: 0,
        };
        let payload = serde_json::to_string(&envelope)
            .map_err(|e| AppError::platform(format!("envelope: {e}")))?;
        let event = OutboxEvent::new(
            id.clone(),
            invocation.tenant_id.clone(),
            INVOKE_TOPIC,
            payload,
            now,
        );
        let redrive = Redrive {
            id: RedriveId::from_ulid(self.ids.next_ulid()),
            dead_letter_id: dead.id.clone(),
            tenant_id: dead.tenant_id.clone(),
            function_id: source.function_id.clone(),
            source_invocation_id: source.id.clone(),
            invocation_id: id.clone(),
            revision_id: revision.id.clone(),
            revision_overridden: revision.id != source.revision_id,
            requested_by: req.principal.subject.clone(),
            reason: req.reason.clone(),
            created_at: now,
        };
        match self.dispatch.redrive(RedriveWrite {
            dead_letter_id: dead.id.clone(),
            invocation: invocation.clone(),
            input: new_input,
            event,
            redrive: redrive.clone(),
        }) {
            Ok(()) => {}
            Err(RepoError::Conflict(m)) => return Err(AppError::Conflict(m)),
            Err(RepoError::Refused(m)) if m.contains("being collected") => {
                return Err(AsyncRefusal::InputTooLarge.err_conflict(m));
            }
            Err(e) => return Err(e.into()),
        }
        self.wake.notify_one();
        self.metrics.redrive();
        tracing::info!(
            dead_letter_id = %dead.id,
            redrive_id = %redrive.id,
            source_invocation_id = %source.id,
            invocation_id = %id,
            revision_id = %revision.id,
            revision_overridden = redrive.revision_overridden,
            requested_by = %redrive.requested_by,
            "dead letter redriven"
        );
        Ok(RedriveResult {
            redrive,
            invocation,
        })
    }
}

impl AsyncRefusal {
    /// A redrive whose input object is already being collected.
    fn err_conflict(self, message: String) -> AppError {
        let _ = self;
        AppError::Conflict(format!("the input can no longer be redriven: {message}"))
    }
}
