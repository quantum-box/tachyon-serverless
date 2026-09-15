//! Usage sink: receives host-observed metering facts.

use async_trait::async_trait;
use tachyon_serverless_domain::UsageEvent;

#[async_trait]
pub trait UsageSink: Send + Sync {
    /// Record an event. Implementations de-duplicate by `event_id`.
    async fn record(&self, event: UsageEvent);
}
