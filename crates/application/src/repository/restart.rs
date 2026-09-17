//! What happens to rows whose driver is gone (docs/threat-model.md §9).
//!
//! Two callers, one set of rules:
//! - a store that opens settles the rows **without an owner** (written before
//!   dispatchers existed, or by a P1 `state.json` import) with
//!   [`Cause::RESTARTED`];
//! - [`super::SlotStore::reclaim_expired`] settles the rows of a dispatcher
//!   whose lease expired ([`Cause::LEASE_EXPIRED`]) or that stopped / whose
//!   previous incarnation is gone ([`Cause::RESTARTED`]). Rows of a live
//!   dispatcher are never touched (PLT-4631).

use tachyon_serverless_domain::{
    ErrorClass, ExecutionEnvironment, ExecutionLease, Invocation, InvocationAttempt,
    InvocationError, InvocationStatus, Timestamp,
};

/// Error type carried by everything the restart reconcile settles.
pub const HOST_RESTARTED: &str = "Host.Restarted";
/// Error type carried by everything a lease-expiry reclaim settles.
pub const HOST_LEASE_EXPIRED: &str = "Host.LeaseExpired";

/// Why rows are settled: the error type and the messages they get.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cause {
    pub error_type: &'static str,
    /// For an invocation that never left `Accepted` / `Queued`.
    pub not_started: &'static str,
    /// For an invocation (and attempt) that was dispatched.
    pub unknown: &'static str,
    /// For an attempt of an invocation that was not dispatched.
    pub attempt: &'static str,
}

impl Cause {
    pub const RESTARTED: Cause = Cause {
        error_type: HOST_RESTARTED,
        not_started: "gateway restarted while the invocation was in flight",
        unknown: "gateway restarted after the invocation was dispatched; the handler may have run",
        attempt: "gateway restarted while the attempt was in flight",
    };
    pub const LEASE_EXPIRED: Cause = Cause {
        error_type: HOST_LEASE_EXPIRED,
        not_started: "the dispatcher that accepted the invocation lost its lease before dispatching it",
        unknown: "the dispatcher lost its lease after the invocation was dispatched; the handler may have run",
        attempt: "the dispatcher lost its lease while the attempt was in flight",
    };
}

/// An invocation that was already `Running` had its `Invoke` frame written,
/// so the handler may have run: its outcome is unknown and must never be
/// reported as a plain failure. One that never left `Accepted` / `Queued`
/// was never dispatched, so it provably did not start and fails with
/// `PlatformError`. Returns whether the invocation changed.
pub fn settle_invocation(inv: &mut Invocation, now: Timestamp) -> bool {
    settle_invocation_with(inv, Cause::RESTARTED, now)
}

/// [`settle_invocation`] for any [`Cause`].
pub fn settle_invocation_with(inv: &mut Invocation, cause: Cause, now: Timestamp) -> bool {
    if inv.status.is_terminal() {
        return false;
    }
    if matches!(inv.status, InvocationStatus::Running) {
        if inv.mark_outcome_unknown(cause.unknown, now).is_ok()
            && let InvocationStatus::OutcomeUnknown { error } = &mut inv.status
        {
            // The domain stamps the generic `Host.OutcomeUnknown`; the
            // cause names itself so it stays visible.
            error.error_type = cause.error_type.to_string();
        }
    } else {
        let _ = inv.mark_failed(
            InvocationError::new(
                ErrorClass::PlatformError,
                cause.error_type,
                cause.not_started,
            ),
            now,
        );
    }
    true
}

/// An attempt is settled like the invocation it belongs to, so a caller
/// never sees a failed attempt under an unknown outcome.
pub fn settle_attempt(
    att: &mut InvocationAttempt,
    invocation_outcome_unknown: bool,
    now: Timestamp,
) -> bool {
    settle_attempt_with(att, invocation_outcome_unknown, Cause::RESTARTED, now)
}

/// [`settle_attempt`] for any [`Cause`].
pub fn settle_attempt_with(
    att: &mut InvocationAttempt,
    invocation_outcome_unknown: bool,
    cause: Cause,
    now: Timestamp,
) -> bool {
    if att.status.is_terminal() {
        return false;
    }
    if invocation_outcome_unknown {
        let _ = att.outcome_unknown(
            InvocationError::new(ErrorClass::OutcomeUnknown, cause.error_type, cause.unknown),
            now,
        );
    } else {
        let _ = att.fail(
            InvocationError::new(ErrorClass::PlatformError, cause.error_type, cause.attempt),
            now,
        );
    }
    true
}

/// Every environment that was not terminal is `Lost`: the bridge session that
/// drove it died with the process. The host processes behind them are
/// reclaimed separately by [`crate::services::ReconcileService`].
pub fn settle_environment(env: &mut ExecutionEnvironment, now: Timestamp) -> bool {
    if env.is_terminal() {
        return false;
    }
    let _ = env.mark_lost("gateway restarted", now);
    true
}

/// A lease held by a driver of the previous process is released: nothing can
/// complete under it any more, and its environment is `Lost`.
pub fn settle_lease(lease: &mut ExecutionLease, now: Timestamp) -> bool {
    lease.released_at.is_none() && lease.release(now).is_ok()
}

/// An asynchronous invocation outlives the dispatcher that runs it
/// (PLT-4640): its input and its dispatch state are durable, and the next run
/// is decided by the dispatch row (`async_dispatch`), whose claim simply
/// expires. A reclaim or a restart therefore settles only its attempts, never
/// the invocation itself.
pub fn survives_dispatcher(inv: &Invocation) -> bool {
    inv.mode == tachyon_serverless_domain::InvocationMode::Async
}

/// Whether the non-terminal attempts of `inv` are settled as outcome unknown:
/// the invocation was, or still is (an asynchronous one), dispatched.
pub fn attempts_unknown(inv: &Invocation) -> bool {
    is_outcome_unknown(inv) || matches!(inv.status, InvocationStatus::Running)
}

pub fn is_outcome_unknown(inv: &Invocation) -> bool {
    matches!(inv.status, InvocationStatus::OutcomeUnknown { .. })
}
