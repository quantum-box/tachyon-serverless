//! `[invoke_async]` (PLT-4639, docs/adr/0010). Only used when `[queue]`
//! selects a queue and the ledger is durable (`state.db`).

use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InvokeAsyncConfig {
    /// Inputs up to this size are kept in the ledger next to the invocation;
    /// larger ones go to the object store (`[objects]`). Without an object
    /// store a larger input is refused with 413.
    pub inline_input_max_bytes: u64,
    /// Refuse new asynchronous invocations (429 `backlog`) while this many
    /// outbox events are not yet published.
    pub max_pending_events: u64,
    /// ... or while the oldest unpublished event is older than this.
    pub max_pending_age_seconds: u64,
    /// How long an accepted asynchronous invocation may wait before it must
    /// have started (its queue deadline). Enforced by the dispatcher
    /// (PLT-4640).
    pub queue_deadline_seconds: u64,
    /// Outbox rows claimed per publisher pass.
    pub publish_batch: usize,
    /// Pause between publisher passes when nothing was due.
    pub publish_interval_ms: u64,
    /// How long a publisher owns a claimed row. A publisher that dies with a
    /// claim delays that row by at most this long.
    pub claim_ttl_seconds: u64,
    /// Backoff of a failed publish: `initial * 2^(attempt-1)`, capped.
    pub retry_initial_ms: u64,
    pub retry_max_ms: u64,
    /// Published outbox rows are deleted after this long.
    pub sent_retention_seconds: u64,
}

impl Default for InvokeAsyncConfig {
    fn default() -> Self {
        Self {
            inline_input_max_bytes: 64 * 1024,
            max_pending_events: 10_000,
            max_pending_age_seconds: 300,
            queue_deadline_seconds: 24 * 60 * 60,
            publish_batch: 100,
            publish_interval_ms: 200,
            claim_ttl_seconds: 30,
            retry_initial_ms: 500,
            retry_max_ms: 30_000,
            sent_retention_seconds: 60 * 60,
        }
    }
}

fn secs(n: u64) -> chrono::Duration {
    chrono::Duration::seconds(n.min(i64::MAX as u64 / 1000) as i64)
}

impl InvokeAsyncConfig {
    pub fn max_pending_age(&self) -> chrono::Duration {
        secs(self.max_pending_age_seconds)
    }

    pub fn queue_deadline(&self) -> chrono::Duration {
        secs(self.queue_deadline_seconds)
    }

    pub fn claim_ttl(&self) -> chrono::Duration {
        secs(self.claim_ttl_seconds)
    }

    pub fn sent_retention(&self) -> chrono::Duration {
        secs(self.sent_retention_seconds)
    }

    pub fn publish_interval(&self) -> Duration {
        Duration::from_millis(self.publish_interval_ms.max(1))
    }

    /// Delay before the next try after `attempts` failed publishes (>= 1).
    pub fn backoff(&self, attempts: u32) -> chrono::Duration {
        let shift = attempts.saturating_sub(1).min(20);
        let ms = self
            .retry_initial_ms
            .saturating_mul(1u64 << shift)
            .min(self.retry_max_ms);
        chrono::Duration::milliseconds(ms.min(i64::MAX as u64) as i64)
    }

    /// `inline_input_max_bytes` above `limits.max_payload_bytes` is allowed:
    /// it only means that every accepted input is inline.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_pending_events == 0 || self.max_pending_age_seconds == 0 {
            return Err(
                "[invoke_async] max_pending_events and max_pending_age_seconds must be > 0: the \
                 outbox is bounded"
                    .into(),
            );
        }
        if self.queue_deadline_seconds == 0 {
            return Err("[invoke_async] queue_deadline_seconds must be > 0".into());
        }
        if self.publish_batch == 0 || self.claim_ttl_seconds == 0 {
            return Err("[invoke_async] publish_batch and claim_ttl_seconds must be > 0".into());
        }
        if self.retry_initial_ms == 0 || self.retry_initial_ms > self.retry_max_ms {
            return Err("[invoke_async] needs 0 < retry_initial_ms <= retry_max_ms".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_is_capped() {
        let c = InvokeAsyncConfig::default();
        assert_eq!(c.backoff(1).num_milliseconds(), 500);
        assert_eq!(c.backoff(2).num_milliseconds(), 1000);
        assert_eq!(c.backoff(7).num_milliseconds(), 30_000);
        assert_eq!(c.backoff(1000).num_milliseconds(), 30_000);
        assert!(c.validate().is_ok());
        let unbounded = InvokeAsyncConfig {
            max_pending_events: 0,
            ..InvokeAsyncConfig::default()
        };
        assert!(unbounded.validate().is_err());
    }
}
