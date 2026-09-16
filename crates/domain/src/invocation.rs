//! Invocation (one logical request) and InvocationAttempt (one execution try).

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::error::DomainError;
use crate::ids::{
    AliasName, AttemptId, EnvironmentId, FunctionId, InvocationId, RevisionId, Sha256Digest,
    TenantId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationMode {
    Sync,
    Async,
}

/// How the request reached the platform. Determines the event type delivered
/// to the guest runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// Generic JSON event (`tachyon.invoke.v1`).
    Json,
    /// HTTP request wrapped as an event (`tachyon.http.v1`).
    Http,
}

impl EventKind {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Json => "tachyon.invoke.v1",
            Self::Http => "tachyon.http.v1",
        }
    }
}

/// Failure classification. Distinguishes user code failures from platform
/// failures, and both from timeouts and unknown outcomes (RFC §7.2).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// Handler returned an error (user code).
    UserError,
    /// Handler panicked / process crashed after Ready.
    Crash,
    /// Guest runtime did not become Ready in time or reported init failure.
    InitError,
    /// Execution deadline elapsed; host terminated the environment.
    Timeout,
    /// Waited in queue past the queue deadline; never started.
    QueueTimeout,
    /// Platform-side failure (provider, bridge, internal).
    PlatformError,
    /// Explicit cancellation.
    Cancelled,
    /// Started but the result could not be confirmed. Never retried automatically.
    OutcomeUnknown,
}

impl ErrorClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UserError => "user_error",
            Self::Crash => "crash",
            Self::InitError => "init_error",
            Self::Timeout => "timeout",
            Self::QueueTimeout => "queue_timeout",
            Self::PlatformError => "platform_error",
            Self::Cancelled => "cancelled",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationError {
    pub class: ErrorClass,
    /// Machine-readable error type, e.g. `Handler.Error`, `Runtime.Panic`, `Host.Timeout`.
    pub error_type: String,
    pub message: String,
}

impl InvocationError {
    pub fn new(
        class: ErrorClass,
        error_type: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            class,
            error_type: error_type.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum InvocationStatus {
    /// Accepted and recorded; not yet dispatched.
    Accepted,
    /// Waiting for capacity.
    Queued,
    /// Dispatched to an environment; handler may be running.
    Running,
    Succeeded,
    Failed {
        error: InvocationError,
    },
    Cancelled,
    /// Result could not be determined. Terminal; surfaced to the caller.
    OutcomeUnknown {
        error: InvocationError,
    },
}

impl InvocationStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed { .. } | Self::Cancelled | Self::OutcomeUnknown { .. }
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Cancelled => "cancelled",
            Self::OutcomeUnknown { .. } => "outcome_unknown",
        }
    }
}

/// Absolute deadlines fixed at acceptance (RFC §9.2). All are wall-clock
/// instants; the host enforces them, the guest only observes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deadlines {
    /// Latest time the invocation may still be started from the queue.
    pub queue_deadline: Timestamp,
    /// Latest time the guest must report Ready after environment creation.
    /// `None` until an environment is assigned.
    pub init_deadline: Option<Timestamp>,
    /// Latest time the handler must produce a result once dispatched.
    /// `None` until dispatched.
    pub execution_deadline: Option<Timestamp>,
    /// Overall client-facing deadline; the platform never starts work after it.
    pub client_deadline: Timestamp,
}

/// Where the output lives. Bodies are never stored in the ledger unbounded;
/// small results are inlined up to a cap, larger ones are referenced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadRef {
    Inline {
        bytes_base64: String,
        size_bytes: u64,
    },
    Digest {
        digest: Sha256Digest,
        size_bytes: u64,
    },
}

impl PayloadRef {
    pub fn size_bytes(&self) -> u64 {
        match self {
            Self::Inline { size_bytes, .. } | Self::Digest { size_bytes, .. } => *size_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invocation {
    pub id: InvocationId,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    /// Alias used at acceptance, if any.
    pub alias: Option<AliasName>,
    /// Revision fixed at acceptance. Never changes during queueing or retries.
    pub revision_id: RevisionId,
    pub mode: InvocationMode,
    pub event_kind: EventKind,
    pub status: InvocationStatus,
    pub deadlines: Deadlines,
    pub idempotency_key: Option<String>,
    pub input_digest: Sha256Digest,
    pub input_size_bytes: u64,
    pub output: Option<PayloadRef>,
    /// For HTTP events: status code returned by the handler (user-level outcome).
    pub http_status: Option<u16>,
    pub trace_id: String,
    pub accepted_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub finished_at: Option<Timestamp>,
    pub attempt_ids: Vec<AttemptId>,
}

impl Invocation {
    #[allow(clippy::too_many_arguments)]
    pub fn accept(
        id: InvocationId,
        tenant_id: TenantId,
        function_id: FunctionId,
        alias: Option<AliasName>,
        revision_id: RevisionId,
        mode: InvocationMode,
        event_kind: EventKind,
        deadlines: Deadlines,
        idempotency_key: Option<String>,
        input_digest: Sha256Digest,
        input_size_bytes: u64,
        trace_id: String,
        now: Timestamp,
    ) -> Result<Self, DomainError> {
        if let Some(k) = &idempotency_key
            && (k.is_empty() || k.len() > 256)
        {
            return Err(DomainError::validation(
                "idempotency_key",
                "must be 1..=256 chars",
            ));
        }
        if deadlines.client_deadline < now {
            return Err(DomainError::validation(
                "client_deadline",
                "already elapsed at acceptance",
            ));
        }
        Ok(Self {
            id,
            tenant_id,
            function_id,
            alias,
            revision_id,
            mode,
            event_kind,
            status: InvocationStatus::Accepted,
            deadlines,
            idempotency_key,
            input_digest,
            input_size_bytes,
            output: None,
            http_status: None,
            trace_id,
            accepted_at: now,
            started_at: None,
            finished_at: None,
            attempt_ids: Vec::new(),
        })
    }

    fn ensure_not_terminal(&self) -> Result<(), DomainError> {
        if self.status.is_terminal() {
            Err(DomainError::Terminal {
                entity: "Invocation",
                state: self.status.name().into(),
            })
        } else {
            Ok(())
        }
    }

    fn illegal(&self, to: &str) -> DomainError {
        DomainError::IllegalTransition {
            entity: "Invocation",
            from: self.status.name().into(),
            to: to.into(),
        }
    }

    pub fn mark_queued(&mut self) -> Result<(), DomainError> {
        self.ensure_not_terminal()?;
        match self.status {
            InvocationStatus::Accepted => {
                self.status = InvocationStatus::Queued;
                Ok(())
            }
            _ => Err(self.illegal("queued")),
        }
    }

    /// Dispatch to an environment. Records the attempt and execution deadline.
    pub fn mark_running(
        &mut self,
        attempt_id: AttemptId,
        execution_deadline: Timestamp,
        init_deadline: Timestamp,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.ensure_not_terminal()?;
        match self.status {
            InvocationStatus::Accepted | InvocationStatus::Queued => {
                self.status = InvocationStatus::Running;
                self.started_at = Some(now);
                self.deadlines.execution_deadline = Some(execution_deadline);
                self.deadlines.init_deadline = Some(init_deadline);
                self.attempt_ids.push(attempt_id);
                Ok(())
            }
            _ => Err(self.illegal("running")),
        }
    }

    /// Record a further attempt on an invocation that is already `Running`.
    ///
    /// Only for a dispatch the host knows never reached its guest (the
    /// `Invoke` frame was not delivered, so the handler cannot have started —
    /// docs/threat-model.md §9). The deadlines move to the new attempt;
    /// `started_at` keeps the first dispatch. A result is never re-executed
    /// through this: `mark_running` stays the only way into `Running`.
    pub fn mark_retry(
        &mut self,
        attempt_id: AttemptId,
        execution_deadline: Timestamp,
        init_deadline: Timestamp,
    ) -> Result<(), DomainError> {
        self.ensure_not_terminal()?;
        match self.status {
            InvocationStatus::Running => {
                self.deadlines.execution_deadline = Some(execution_deadline);
                self.deadlines.init_deadline = Some(init_deadline);
                self.attempt_ids.push(attempt_id);
                Ok(())
            }
            _ => Err(self.illegal("retry")),
        }
    }

    pub fn mark_succeeded(
        &mut self,
        output: Option<PayloadRef>,
        http_status: Option<u16>,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.ensure_not_terminal()?;
        match self.status {
            InvocationStatus::Running => {
                self.status = InvocationStatus::Succeeded;
                self.output = output;
                self.http_status = http_status;
                self.finished_at = Some(now);
                Ok(())
            }
            _ => Err(self.illegal("succeeded")),
        }
    }

    /// Fail from any non-terminal state (e.g. queue timeout before running).
    pub fn mark_failed(
        &mut self,
        error: InvocationError,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.ensure_not_terminal()?;
        if error.class == ErrorClass::OutcomeUnknown {
            return Err(DomainError::validation(
                "error.class",
                "use mark_outcome_unknown for OutcomeUnknown",
            ));
        }
        if error.class == ErrorClass::Cancelled {
            return Err(DomainError::validation(
                "error.class",
                "use mark_cancelled for Cancelled",
            ));
        }
        self.status = InvocationStatus::Failed { error };
        self.finished_at = Some(now);
        Ok(())
    }

    pub fn mark_cancelled(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.ensure_not_terminal()?;
        self.status = InvocationStatus::Cancelled;
        self.finished_at = Some(now);
        Ok(())
    }

    /// Only valid once the handler may have started (Running).
    pub fn mark_outcome_unknown(
        &mut self,
        reason: impl Into<String>,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.ensure_not_terminal()?;
        match self.status {
            InvocationStatus::Running => {
                self.status = InvocationStatus::OutcomeUnknown {
                    error: InvocationError::new(
                        ErrorClass::OutcomeUnknown,
                        "Host.OutcomeUnknown",
                        reason,
                    ),
                };
                self.finished_at = Some(now);
                Ok(())
            }
            _ => Err(self.illegal("outcome_unknown")),
        }
    }

    pub fn ensure_owned_by(&self, tenant: &TenantId) -> Result<(), DomainError> {
        if &self.tenant_id == tenant {
            Ok(())
        } else {
            Err(DomainError::TenantMismatch(format!(
                "invocation {} is not owned by tenant {}",
                self.id, tenant
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AttemptStatus {
    Dispatched,
    Succeeded,
    Failed { error: InvocationError },
    OutcomeUnknown { error: InvocationError },
}

impl AttemptStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Dispatched)
    }
    pub fn name(&self) -> &'static str {
        match self {
            Self::Dispatched => "dispatched",
            Self::Succeeded => "succeeded",
            Self::Failed { .. } => "failed",
            Self::OutcomeUnknown { .. } => "outcome_unknown",
        }
    }
}

/// Cold/warm classification of the environment used by an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartKind {
    Cold,
    Warm,
    Restored,
}

/// Timing breakdown measured by the host (RFC §18.1). Milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AttemptTimings {
    pub queue_wait_ms: Option<u64>,
    pub environment_boot_ms: Option<u64>,
    pub runtime_init_ms: Option<u64>,
    /// Time spent resuming a quiesced environment on a warm start
    /// ([`StartKind::Warm`]). `None` on a cold start: nothing was resumed.
    ///
    /// A warm start does not boot and does not initialize, so
    /// `environment_boot_ms` and `runtime_init_ms` are legitimately zero for
    /// it — but resuming and checking the environment is real work, and it is
    /// reported here rather than hidden in those zeros (PLT-4633).
    pub resume_ms: Option<u64>,
    /// Time spent confirming, after the resume, that the environment is fit to
    /// be dispatched into. `None` on a cold start.
    pub readiness_ms: Option<u64>,
    pub handler_ms: Option<u64>,
    pub response_ms: Option<u64>,
    pub total_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationAttempt {
    pub id: AttemptId,
    pub invocation_id: InvocationId,
    pub tenant_id: TenantId,
    pub number: u32,
    pub environment_id: EnvironmentId,
    /// Epoch of the environment assignment; stale epochs are rejected by the host.
    pub epoch: u64,
    pub status: AttemptStatus,
    pub start_kind: StartKind,
    pub timings: AttemptTimings,
    pub dispatched_at: Timestamp,
    pub finished_at: Option<Timestamp>,
}

impl InvocationAttempt {
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch(
        id: AttemptId,
        invocation_id: InvocationId,
        tenant_id: TenantId,
        number: u32,
        environment_id: EnvironmentId,
        epoch: u64,
        start_kind: StartKind,
        now: Timestamp,
    ) -> Self {
        Self {
            id,
            invocation_id,
            tenant_id,
            number,
            environment_id,
            epoch,
            status: AttemptStatus::Dispatched,
            start_kind,
            timings: AttemptTimings::default(),
            dispatched_at: now,
            finished_at: None,
        }
    }

    fn finish(&mut self, status: AttemptStatus, now: Timestamp) -> Result<(), DomainError> {
        if self.status.is_terminal() {
            return Err(DomainError::Terminal {
                entity: "InvocationAttempt",
                state: self.status.name().into(),
            });
        }
        self.status = status;
        self.finished_at = Some(now);
        Ok(())
    }

    pub fn succeed(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.finish(AttemptStatus::Succeeded, now)
    }
    pub fn fail(&mut self, error: InvocationError, now: Timestamp) -> Result<(), DomainError> {
        self.finish(AttemptStatus::Failed { error }, now)
    }
    pub fn outcome_unknown(
        &mut self,
        error: InvocationError,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.finish(AttemptStatus::OutcomeUnknown { error }, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};

    fn now() -> Timestamp {
        chrono::Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap()
    }

    fn deadlines() -> Deadlines {
        Deadlines {
            queue_deadline: now() + Duration::seconds(10),
            init_deadline: None,
            execution_deadline: None,
            client_deadline: now() + Duration::seconds(60),
        }
    }

    fn accept() -> Invocation {
        Invocation::accept(
            InvocationId::generate(),
            TenantId::generate(),
            FunctionId::generate(),
            Some(AliasName::default_alias()),
            RevisionId::generate(),
            InvocationMode::Sync,
            EventKind::Json,
            deadlines(),
            None,
            Sha256Digest::of_bytes(b"{}"),
            2,
            "trace".into(),
            now(),
        )
        .unwrap()
    }

    #[test]
    fn happy_path() {
        let mut inv = accept();
        inv.mark_queued().unwrap();
        let att = AttemptId::generate();
        inv.mark_running(
            att.clone(),
            now() + Duration::seconds(30),
            now() + Duration::seconds(5),
            now(),
        )
        .unwrap();
        assert_eq!(inv.attempt_ids, vec![att]);
        inv.mark_succeeded(None, None, now()).unwrap();
        assert!(inv.status.is_terminal());
        assert!(
            inv.mark_failed(
                InvocationError::new(ErrorClass::Timeout, "Host.Timeout", "late"),
                now()
            )
            .is_err()
        );
    }

    #[test]
    fn cannot_succeed_before_running() {
        let mut inv = accept();
        assert!(inv.mark_succeeded(None, None, now()).is_err());
    }

    /// A retry after an undelivered dispatch records a second attempt without
    /// leaving `Running`, and is refused anywhere else.
    #[test]
    fn a_retry_records_another_attempt_only_while_running() {
        let mut inv = accept();
        let first = AttemptId::generate();
        assert!(
            inv.mark_retry(first.clone(), now(), now()).is_err(),
            "nothing to retry before the first dispatch"
        );
        inv.mark_running(
            first.clone(),
            now() + Duration::seconds(30),
            now() + Duration::seconds(5),
            now(),
        )
        .unwrap();
        let started = inv.started_at;

        let second = AttemptId::generate();
        inv.mark_retry(
            second.clone(),
            now() + Duration::seconds(60),
            now() + Duration::seconds(35),
        )
        .unwrap();
        assert_eq!(inv.status, InvocationStatus::Running);
        assert_eq!(inv.attempt_ids, vec![first, second]);
        assert_eq!(inv.started_at, started, "the first dispatch stands");
        assert_eq!(
            inv.deadlines.execution_deadline,
            Some(now() + Duration::seconds(60)),
            "the deadlines follow the new attempt"
        );

        inv.mark_succeeded(None, None, now()).unwrap();
        assert!(
            inv.mark_retry(AttemptId::generate(), now(), now()).is_err(),
            "a settled invocation is never retried"
        );
    }

    #[test]
    fn outcome_unknown_only_after_running() {
        let mut inv = accept();
        assert!(inv.mark_outcome_unknown("x", now()).is_err());
        inv.mark_running(AttemptId::generate(), now(), now(), now())
            .unwrap();
        inv.mark_outcome_unknown("bridge lost", now()).unwrap();
        assert!(matches!(
            inv.status,
            InvocationStatus::OutcomeUnknown { .. }
        ));
    }

    #[test]
    fn queue_timeout_fails_from_queued() {
        let mut inv = accept();
        inv.mark_queued().unwrap();
        inv.mark_failed(
            InvocationError::new(ErrorClass::QueueTimeout, "Host.QueueTimeout", "no capacity"),
            now(),
        )
        .unwrap();
    }

    #[test]
    fn rejects_elapsed_client_deadline_and_bad_key() {
        let mut d = deadlines();
        d.client_deadline = now() - Duration::seconds(1);
        let r = Invocation::accept(
            InvocationId::generate(),
            TenantId::generate(),
            FunctionId::generate(),
            None,
            RevisionId::generate(),
            InvocationMode::Sync,
            EventKind::Json,
            d,
            Some(String::new()),
            Sha256Digest::of_bytes(b""),
            0,
            "t".into(),
            now(),
        );
        assert!(r.is_err());
    }

    #[test]
    fn attempt_terminal_once() {
        let mut a = InvocationAttempt::dispatch(
            AttemptId::generate(),
            InvocationId::generate(),
            TenantId::generate(),
            1,
            EnvironmentId::generate(),
            1,
            StartKind::Cold,
            now(),
        );
        a.succeed(now()).unwrap();
        assert!(
            a.fail(InvocationError::new(ErrorClass::Crash, "x", "y"), now())
                .is_err()
        );
    }
}
