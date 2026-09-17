//! What a restart does to rows whose driver died with the previous process
//! (docs/threat-model.md §9). Shared by the SQLite store (on every open) and
//! the one-time `state.json` import.

use tachyon_serverless_domain::{
    ErrorClass, ExecutionEnvironment, ExecutionLease, Invocation, InvocationAttempt,
    InvocationError, InvocationStatus, Timestamp,
};

/// Error type carried by everything the restart reconcile settles.
pub const HOST_RESTARTED: &str = "Host.Restarted";

const INVOCATION_MSG: &str = "gateway restarted while the invocation was in flight";
const UNKNOWN_MSG: &str =
    "gateway restarted after the invocation was dispatched; the handler may have run";
const ATTEMPT_MSG: &str = "gateway restarted while the attempt was in flight";

/// An invocation that was already `Running` had its `Invoke` frame written,
/// so the handler may have run: its outcome is unknown and must never be
/// reported as a plain failure. One that never left `Accepted` / `Queued`
/// was never dispatched, so it provably did not start and fails with
/// `PlatformError`. Returns whether the invocation changed.
pub fn settle_invocation(inv: &mut Invocation, now: Timestamp) -> bool {
    if inv.status.is_terminal() {
        return false;
    }
    if matches!(inv.status, InvocationStatus::Running) {
        if inv.mark_outcome_unknown(UNKNOWN_MSG, now).is_ok()
            && let InvocationStatus::OutcomeUnknown { error } = &mut inv.status
        {
            // The domain stamps the generic `Host.OutcomeUnknown`; a
            // restart names itself so the cause stays visible.
            error.error_type = HOST_RESTARTED.to_string();
        }
    } else {
        let _ = inv.mark_failed(
            InvocationError::new(ErrorClass::PlatformError, HOST_RESTARTED, INVOCATION_MSG),
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
    if att.status.is_terminal() {
        return false;
    }
    if invocation_outcome_unknown {
        let _ = att.outcome_unknown(
            InvocationError::new(ErrorClass::OutcomeUnknown, HOST_RESTARTED, UNKNOWN_MSG),
            now,
        );
    } else {
        let _ = att.fail(
            InvocationError::new(ErrorClass::PlatformError, HOST_RESTARTED, ATTEMPT_MSG),
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

pub fn is_outcome_unknown(inv: &Invocation) -> bool {
    matches!(inv.status, InvocationStatus::OutcomeUnknown { .. })
}
