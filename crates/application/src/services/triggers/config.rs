//! `[triggers]` (PLT-4641, docs/adr/0014). Triggers exist only where
//! asynchronous invoke does (a queue and the durable ledger) and only on a
//! `combined` gateway (the management store).

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TriggersConfig {
    /// Run the cron scheduler in this gateway. The webhook endpoint and the
    /// CRUD API do not depend on it.
    pub scheduler_enabled: bool,
    /// Pause between scheduler passes when nothing is due sooner.
    pub scheduler_interval_ms: u64,
    /// Due cron triggers handled per pass.
    pub scheduler_batch: usize,
    /// A scheduled time at most this late is on time (it fires under every
    /// missed-run policy). Covers the scheduler period and short stalls.
    pub grace_seconds: u64,
    /// Scheduled times older than this are never fired, whatever the policy.
    pub max_catchup_seconds: u64,
    /// Upper bound of `run_all.max_runs`.
    pub max_run_all: u32,
    /// Live triggers per function.
    pub max_triggers_per_function: usize,
    /// Upper bound (and default) of a webhook trigger's `max_body_bytes`.
    pub webhook_max_body_bytes: u64,
    /// Default `tolerance_seconds` of a webhook trigger.
    pub webhook_default_tolerance_seconds: u64,
    /// Upper bound of `tolerance_seconds`.
    pub webhook_max_tolerance_seconds: u64,
    /// How long a webhook event id (and signed delivery) is remembered: a
    /// resend within this window answers the same invocation.
    pub webhook_dedup_retention_seconds: u64,
    /// How long cron fire records are kept.
    pub fire_retention_seconds: u64,
    /// Key that seals webhook secrets at rest: 64 hex characters, mode 0600.
    /// At most one of `secret_key_file` / `secret_key_env`. Without one,
    /// webhook triggers cannot be created (cron triggers can).
    pub secret_key_file: Option<PathBuf>,
    pub secret_key_env: Option<String>,
}

impl Default for TriggersConfig {
    fn default() -> Self {
        Self {
            scheduler_enabled: true,
            scheduler_interval_ms: 1000,
            scheduler_batch: 100,
            grace_seconds: 30,
            max_catchup_seconds: 24 * 60 * 60,
            max_run_all: 100,
            max_triggers_per_function: 20,
            webhook_max_body_bytes: 256 * 1024,
            webhook_default_tolerance_seconds: 300,
            webhook_max_tolerance_seconds: 3600,
            webhook_dedup_retention_seconds: 7 * 24 * 60 * 60,
            fire_retention_seconds: 30 * 24 * 60 * 60,
            secret_key_file: None,
            secret_key_env: None,
        }
    }
}

fn secs(n: u64) -> chrono::Duration {
    chrono::Duration::seconds(n.min(i64::MAX as u64 / 1000) as i64)
}

impl TriggersConfig {
    pub fn scheduler_interval(&self) -> Duration {
        Duration::from_millis(self.scheduler_interval_ms.max(1))
    }

    pub fn grace(&self) -> chrono::Duration {
        secs(self.grace_seconds)
    }

    pub fn max_catchup(&self) -> chrono::Duration {
        secs(self.max_catchup_seconds)
    }

    pub fn webhook_dedup_retention(&self) -> chrono::Duration {
        secs(self.webhook_dedup_retention_seconds)
    }

    /// The webhook body bound actually applied: never above the invocation
    /// payload limit.
    pub fn effective_webhook_max_body_bytes(&self, max_payload_bytes: u64) -> u64 {
        self.webhook_max_body_bytes.min(max_payload_bytes)
    }

    pub fn fire_retention(&self) -> chrono::Duration {
        secs(self.fire_retention_seconds)
    }

    /// `max_payload_bytes` is the invocation input limit.
    pub fn validate(&self, max_payload_bytes: u64) -> Result<(), String> {
        let invalid = |m: String| Err(format!("[triggers] {m}"));
        if self.scheduler_interval_ms == 0 || self.scheduler_batch == 0 {
            return invalid("scheduler_interval_ms and scheduler_batch must be > 0".into());
        }
        if self.max_triggers_per_function == 0 || self.max_run_all == 0 {
            return invalid("max_triggers_per_function and max_run_all must be > 0".into());
        }
        if self.max_catchup_seconds < self.grace_seconds {
            return invalid("max_catchup_seconds must be >= grace_seconds".into());
        }
        // A cron fire row must outlive the window in which its scheduled time
        // can still be computed as due, or a restart could fire it again.
        if self.fire_retention_seconds <= self.max_catchup_seconds {
            return invalid(
                "fire_retention_seconds must be > max_catchup_seconds: a scheduled time must be \
                 remembered for as long as it can be fired"
                    .into(),
            );
        }
        // Above `limits.max_payload_bytes` it is clamped (see
        // `effective_webhook_max_body_bytes`): a body the acceptance would refuse
        // anyway is refused while reading.
        if self.webhook_max_body_bytes == 0 {
            return invalid("webhook_max_body_bytes must be > 0".into());
        }
        let _ = max_payload_bytes;
        if self.webhook_default_tolerance_seconds == 0
            || self.webhook_default_tolerance_seconds > self.webhook_max_tolerance_seconds
        {
            return invalid(
                "needs 0 < webhook_default_tolerance_seconds <= webhook_max_tolerance_seconds"
                    .into(),
            );
        }
        // A delivery whose signature is still inside the tolerance must still
        // be deduplicated, in both directions of clock skew.
        if self.webhook_dedup_retention_seconds < 2 * self.webhook_max_tolerance_seconds {
            return invalid(
                "webhook_dedup_retention_seconds must be >= 2 * webhook_max_tolerance_seconds: a \
                 replay inside the signature tolerance must still find its event id"
                    .into(),
            );
        }
        if self.secret_key_file.is_some() && self.secret_key_env.is_some() {
            return invalid("set at most one of secret_key_file / secret_key_env".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate_and_retention_bounds_are_enforced() {
        let c = TriggersConfig::default();
        assert!(c.validate(1024 * 1024).is_ok());
        let short = TriggersConfig {
            fire_retention_seconds: c.max_catchup_seconds,
            ..TriggersConfig::default()
        };
        assert!(short.validate(1024 * 1024).is_err());
        let dedup = TriggersConfig {
            webhook_dedup_retention_seconds: 3600,
            ..TriggersConfig::default()
        };
        assert!(dedup.validate(1024 * 1024).is_err());
        assert!(c.validate(1024).is_ok(), "clamped, not refused");
        assert_eq!(c.effective_webhook_max_body_bytes(1024), 1024);
    }
}
