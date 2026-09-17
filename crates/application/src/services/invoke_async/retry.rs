//! `[async_dispatch]`: the retry policy of asynchronous invocations and the
//! classification of run outcomes (PLT-4640, docs/adr/0013).
//!
//! - **Attempts.** A run that dispatched (or tried to dispatch) the handler
//!   counts; at most `max_attempts` count. A run that ended before anything
//!   could start for reasons of the platform's own capacity (admission,
//!   retry budget, gateway shutdown, a refused cold start during an outage)
//!   is a *deferral*: it does not count, but it is bounded by the age.
//! - **Age.** No run starts after `min(accepted_at + max_event_age_seconds,
//!   queue_deadline)`. A retry that would be due after that is dead-lettered
//!   at once (`expired`).
//! - **Backoff.** Exponential with full jitter: the delay before try `n + 1`
//!   is uniform in `[0, min(backoff_max_ms, backoff_initial_ms * 2^(n-1))]`,
//!   at least `backoff_floor_ms`.
//! - **Retry budget.** At most `retry_budget` retries per function per
//!   `retry_budget_window_seconds` (per gateway process). A retry over the
//!   budget is deferred to the next window: a retry storm turns into a
//!   bounded rate instead of hammering a failing dependency.
//! - **Classification** ([`classify`]): input, validation and authorization
//!   errors, a deleted function and an unrunnable revision are not retried;
//!   timeouts, crashes, unknown outcomes, handler errors and transient
//!   platform failures are.

use std::collections::HashMap;
use std::time::Duration;

use parking_lot::Mutex;
use serde::Deserialize;

use tachyon_serverless_domain::{ErrorClass, FunctionId, Invocation, InvocationError, Timestamp};

use crate::control::ControlError;
use crate::repository::DeadLetterReason;
use crate::services::admission::{
    CAPACITY_WAIT_TIMEOUT, FUNCTION_DELETED, QUEUE_TIMEOUT, QUOTA_WAIT_TIMEOUT, START_CIRCUIT_OPEN,
};
use crate::services::invoke::ASYNC_GATEWAY_SHUTDOWN;

/// Per-function overrides of the retry policy.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FunctionRetryPolicy {
    pub function_id: FunctionId,
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub max_event_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AsyncDispatchConfig {
    /// Run the consumer, the reaper and redrive. Only takes effect where
    /// asynchronous invoke is available (`[queue]` and the durable ledger).
    pub enabled: bool,
    /// Durable consumer name on the `invoke` topic.
    pub consumer: String,
    /// Concurrent runs of this gateway (each worker pulls one event at a
    /// time). 0 = half of `[capacity] max_concurrency` (at least 1): the
    /// asynchronous class never takes more than that, so synchronous invokes
    /// always have headroom.
    pub workers: usize,
    /// Visibility of a delivered, unacked message. A run longer than this is
    /// redelivered; the redelivery finds the live claim and is acked.
    pub ack_wait_seconds: u64,
    /// Broker-side delivery bound. Not the retry bound (delivery counts do not
    /// survive a broker crash, ADR-0008): the ledger's attempts are.
    pub max_deliver: u32,
    /// How long a worker waits for a message per fetch.
    pub fetch_wait_ms: u64,
    /// How long a dispatcher owns a run without renewing its claim. Renewed
    /// every third of it while the run lasts.
    pub claim_ttl_seconds: u64,
    /// How long an asynchronous run waits for admission before it is deferred.
    pub admission_wait_ms: u64,
    pub max_attempts: u32,
    pub max_event_age_seconds: u64,
    pub backoff_initial_ms: u64,
    pub backoff_max_ms: u64,
    /// Lower bound of a jittered delay.
    pub backoff_floor_ms: u64,
    /// Retries per function per window (per gateway). 0 = unlimited.
    pub retry_budget: u32,
    pub retry_budget_window_seconds: u64,
    /// How often the reaper looks for abandoned runs, lost events and expired
    /// invocations.
    pub reaper_interval_seconds: u64,
    /// An invocation with no unpublished event whose last event went out this
    /// long ago, while the consumer has nothing pending, is published again
    /// (a message the broker lost).
    pub stall_timeout_seconds: u64,
    #[serde(rename = "function")]
    pub functions: Vec<FunctionRetryPolicy>,
}

impl Default for AsyncDispatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            consumer: "dispatcher".into(),
            workers: 0,
            ack_wait_seconds: 300,
            max_deliver: 1000,
            fetch_wait_ms: 1000,
            claim_ttl_seconds: 60,
            admission_wait_ms: 2000,
            max_attempts: 3,
            max_event_age_seconds: 6 * 60 * 60,
            backoff_initial_ms: 1000,
            backoff_max_ms: 5 * 60 * 1000,
            backoff_floor_ms: 100,
            retry_budget: 100,
            retry_budget_window_seconds: 60,
            reaper_interval_seconds: 10,
            stall_timeout_seconds: 15 * 60,
            functions: Vec::new(),
        }
    }
}

fn secs(n: u64) -> chrono::Duration {
    chrono::Duration::seconds(n.min(i64::MAX as u64 / 1000) as i64)
}

/// The effective policy of one function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub max_event_age: chrono::Duration,
}

impl RetryPolicy {
    /// No run starts at or after this instant.
    pub fn expires_at(&self, invocation: &Invocation) -> Timestamp {
        (invocation.accepted_at + self.max_event_age).min(invocation.deadlines.queue_deadline)
    }
}

impl AsyncDispatchConfig {
    pub fn policy_for(&self, function: &FunctionId) -> RetryPolicy {
        let over = self.functions.iter().find(|f| &f.function_id == function);
        RetryPolicy {
            max_attempts: over
                .and_then(|f| f.max_attempts)
                .unwrap_or(self.max_attempts),
            max_event_age: secs(
                over.and_then(|f| f.max_event_age_seconds)
                    .unwrap_or(self.max_event_age_seconds),
            ),
        }
    }

    /// The smallest maximum age of any policy (the reaper's first filter).
    pub fn min_event_age(&self) -> chrono::Duration {
        secs(
            self.functions
                .iter()
                .filter_map(|f| f.max_event_age_seconds)
                .fold(self.max_event_age_seconds, u64::min),
        )
    }

    pub fn workers_for(&self, max_concurrency: usize) -> usize {
        match self.workers {
            0 => (max_concurrency / 2).max(1),
            n => n,
        }
    }

    pub fn ack_wait(&self) -> Duration {
        Duration::from_secs(self.ack_wait_seconds.max(1))
    }

    pub fn fetch_wait(&self) -> Duration {
        Duration::from_millis(self.fetch_wait_ms.max(1))
    }

    pub fn claim_ttl(&self) -> chrono::Duration {
        secs(self.claim_ttl_seconds)
    }

    pub fn admission_wait(&self) -> Duration {
        Duration::from_millis(self.admission_wait_ms)
    }

    pub fn reaper_interval(&self) -> Duration {
        Duration::from_secs(self.reaper_interval_seconds.max(1))
    }

    pub fn stall_timeout(&self) -> chrono::Duration {
        secs(self.stall_timeout_seconds)
    }

    /// The upper bound of the jittered delay before try `n + 1` after `n`
    /// tries (`n >= 1`).
    pub fn backoff_ceiling_ms(&self, n: u32) -> u64 {
        let shift = n.saturating_sub(1).min(30);
        self.backoff_initial_ms
            .saturating_mul(1u64 << shift)
            .min(self.backoff_max_ms)
    }

    /// Full jitter: `jitter(ceiling)` must return a value in `[0, ceiling]`.
    pub fn backoff(&self, n: u32, jitter: &dyn Fn(u64) -> u64) -> chrono::Duration {
        let ceiling = self.backoff_ceiling_ms(n);
        let ms = jitter(ceiling).min(ceiling).max(self.backoff_floor_ms);
        chrono::Duration::milliseconds(ms.min(i64::MAX as u64) as i64)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.max_attempts == 0 {
            return Err("[async_dispatch] max_attempts must be >= 1".into());
        }
        if self.max_event_age_seconds == 0 || self.claim_ttl_seconds == 0 {
            return Err(
                "[async_dispatch] max_event_age_seconds and claim_ttl_seconds must be > 0".into(),
            );
        }
        if self.backoff_initial_ms == 0 || self.backoff_initial_ms > self.backoff_max_ms {
            return Err("[async_dispatch] needs 0 < backoff_initial_ms <= backoff_max_ms".into());
        }
        if self.backoff_floor_ms > self.backoff_initial_ms {
            return Err("[async_dispatch] backoff_floor_ms must be <= backoff_initial_ms".into());
        }
        if self.retry_budget > 0 && self.retry_budget_window_seconds == 0 {
            return Err("[async_dispatch] retry_budget_window_seconds must be > 0".into());
        }
        if self.max_deliver == 0 || self.ack_wait_seconds == 0 {
            return Err("[async_dispatch] max_deliver and ack_wait_seconds must be > 0".into());
        }
        if !self
            .consumer
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || self.consumer.is_empty()
        {
            return Err("[async_dispatch] consumer must be [a-z0-9-]+".into());
        }
        for f in &self.functions {
            if f.max_attempts == Some(0) || f.max_event_age_seconds == Some(0) {
                return Err(format!(
                    "[[async_dispatch.function]] {}: max_attempts and max_event_age_seconds \
                     must be > 0",
                    f.function_id
                ));
            }
        }
        Ok(())
    }
}

/// Uniform jitter in `[0, ceiling]` from the OS RNG.
pub fn system_jitter(ceiling: u64) -> u64 {
    if ceiling == 0 {
        return 0;
    }
    let r = getrandom::u64().unwrap_or(ceiling / 2);
    r % (ceiling + 1)
}

/// What an outcome means for the next run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Retry, counting this run as an attempt.
    Retry,
    /// Nothing started for reasons of the platform's capacity or state: try
    /// again later without counting.
    Defer,
    /// Retrying cannot help: dead-letter now.
    DeadLetter(DeadLetterReason),
    /// The caller cancelled it: terminal, not a dead letter.
    Cancelled,
}

/// Classify a failed run (module docs).
pub fn classify(error: &InvocationError) -> Disposition {
    let t = error.error_type.as_str();
    if error.class == ErrorClass::Cancelled {
        return Disposition::Cancelled;
    }
    if t == FUNCTION_DELETED {
        return Disposition::DeadLetter(DeadLetterReason::FunctionDeleted);
    }
    match t {
        // Input, output and authorization: the same run fails the same way.
        "Host.SecretBindingUnavailable"
        | "Host.ResponseTooLarge"
        | "Runtime.ResponseTooLarge"
        | "Host.UnsupportedArtifact"
        | "Host.InvokeTooLarge"
        | "Host.InvokeEncode"
        | INVALID_INPUT
        | INPUT_CORRUPT => return Disposition::DeadLetter(DeadLetterReason::NonRetryable),
        // Capacity and platform state: nothing started.
        QUEUE_TIMEOUT
        | CAPACITY_WAIT_TIMEOUT
        | QUOTA_WAIT_TIMEOUT
        | START_CIRCUIT_OPEN
        | ASYNC_GATEWAY_SHUTDOWN
        | "Host.DispatcherFenced"
        | "Host.AlreadyRunning"
        | "Host.CapacityClosed"
        | RETRY_BUDGET_EXHAUSTED
        | INPUT_UNAVAILABLE
        | crate::error::USAGE_JOURNAL_FULL
        | crate::error::USAGE_JOURNAL_UNAVAILABLE => return Disposition::Defer,
        _ => {}
    }
    if let Some(kind) = ControlError::from_error_type(t) {
        return match kind {
            ControlError::PolicyDenied | ControlError::UnknownTenant => {
                Disposition::DeadLetter(DeadLetterReason::NonRetryable)
            }
            _ => Disposition::Defer,
        };
    }
    if error.class == ErrorClass::QueueTimeout {
        return Disposition::Defer;
    }
    Disposition::Retry
}

/// `error_type` of an input that is not a JSON event (non-retryable).
pub const INVALID_INPUT: &str = "Host.InvalidInput";
/// `error_type` of a stored input that no longer matches its digest.
pub const INPUT_CORRUPT: &str = "Host.InputCorrupt";
/// `error_type` of an input the object store could not return (deferred).
pub const INPUT_UNAVAILABLE: &str = "Host.InputUnavailable";
/// `error_type` of a retry deferred by the retry budget.
pub const RETRY_BUDGET_EXHAUSTED: &str = "Host.RetryBudgetExhausted";
/// `error_type` of an invocation dead-lettered because it got too old.
pub const EVENT_EXPIRED: &str = "Host.AsyncEventExpired";
/// `error_type` of a run whose dispatcher disappeared with its claim.
pub const RUN_ABANDONED: &str = "Host.AsyncRunAbandoned";
/// `error_type` of an invocation whose pinned revision cannot run any more.
pub const REVISION_UNAVAILABLE: &str = "Host.RevisionUnavailable";

/// The per-function retry budget of one gateway process.
#[derive(Debug, Default)]
pub struct RetryBudget {
    windows: Mutex<HashMap<FunctionId, (Timestamp, u32)>>,
}

impl RetryBudget {
    /// Take one retry for `function`. `Err(next_window)` when the budget of
    /// the current window is spent.
    pub fn take(
        &self,
        config: &AsyncDispatchConfig,
        function: &FunctionId,
        now: Timestamp,
    ) -> Result<(), Timestamp> {
        if config.retry_budget == 0 {
            return Ok(());
        }
        let window = secs(config.retry_budget_window_seconds);
        let mut w = self.windows.lock();
        let entry = w.entry(function.clone()).or_insert((now, 0));
        if now - entry.0 >= window {
            *entry = (now, 0);
        }
        if entry.1 >= config.retry_budget {
            return Err(entry.0 + window);
        }
        entry.1 += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: i64) -> Timestamp {
        chrono::DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
    }

    #[test]
    fn backoff_is_exponential_capped_and_fully_jittered() {
        let c = AsyncDispatchConfig {
            backoff_initial_ms: 1000,
            backoff_max_ms: 8000,
            backoff_floor_ms: 10,
            ..AsyncDispatchConfig::default()
        };
        assert_eq!(c.backoff_ceiling_ms(1), 1000);
        assert_eq!(c.backoff_ceiling_ms(2), 2000);
        assert_eq!(c.backoff_ceiling_ms(4), 8000);
        assert_eq!(c.backoff_ceiling_ms(40), 8000);
        let max = |ceiling: u64| ceiling;
        let zero = |_: u64| 0;
        assert_eq!(c.backoff(3, &max).num_milliseconds(), 4000);
        assert_eq!(c.backoff(3, &zero).num_milliseconds(), 10);
        for _ in 0..1000 {
            let d = c.backoff(2, &system_jitter).num_milliseconds();
            assert!((10..=2000).contains(&d), "{d}");
        }
        assert!(c.validate().is_ok());
    }

    #[test]
    fn classification_separates_retryable_deferrable_and_dead() {
        let e = |class: ErrorClass, t: &str| InvocationError::new(class, t, "m");
        assert_eq!(
            classify(&e(ErrorClass::UserError, "Handler.Error")),
            Disposition::Retry
        );
        assert_eq!(
            classify(&e(ErrorClass::Crash, "Runtime.Exited")),
            Disposition::Retry
        );
        assert_eq!(
            classify(&e(ErrorClass::Timeout, "Host.Timeout")),
            Disposition::Retry
        );
        assert_eq!(
            classify(&e(ErrorClass::OutcomeUnknown, "Host.OutcomeUnknown")),
            Disposition::Retry
        );
        assert_eq!(
            classify(&e(ErrorClass::QueueTimeout, CAPACITY_WAIT_TIMEOUT)),
            Disposition::Defer
        );
        assert_eq!(
            classify(&e(ErrorClass::PlatformError, "Host.ConfigExpired")),
            Disposition::Defer
        );
        assert_eq!(
            classify(&e(
                ErrorClass::PlatformError,
                crate::error::USAGE_JOURNAL_FULL
            )),
            Disposition::Defer
        );
        assert_eq!(
            classify(&e(ErrorClass::PlatformError, "Host.PolicyDenied")),
            Disposition::DeadLetter(DeadLetterReason::NonRetryable)
        );
        assert_eq!(
            classify(&e(ErrorClass::InitError, "Host.SecretBindingUnavailable")),
            Disposition::DeadLetter(DeadLetterReason::NonRetryable)
        );
        assert_eq!(
            classify(&e(ErrorClass::PlatformError, FUNCTION_DELETED)),
            Disposition::DeadLetter(DeadLetterReason::FunctionDeleted)
        );
        assert_eq!(
            classify(&e(ErrorClass::Cancelled, "Host.Cancelled")),
            Disposition::Cancelled
        );
    }

    #[test]
    fn the_retry_budget_is_per_function_and_per_window() {
        let c = AsyncDispatchConfig {
            retry_budget: 2,
            retry_budget_window_seconds: 10,
            ..AsyncDispatchConfig::default()
        };
        let b = RetryBudget::default();
        let f = FunctionId::generate();
        let g = FunctionId::generate();
        assert!(b.take(&c, &f, at(0)).is_ok());
        assert!(b.take(&c, &f, at(1)).is_ok());
        assert_eq!(b.take(&c, &f, at(2)), Err(at(10)));
        assert!(b.take(&c, &g, at(2)).is_ok());
        assert!(b.take(&c, &f, at(10)).is_ok());
    }
}
