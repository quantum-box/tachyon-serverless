//! Outbox publisher (PLT-4639, docs/adr/0010 §「publisher」).
//!
//! One pass ([`OutboxPublisher::run_once`]):
//!
//! 1. claim due, unsent outbox rows under a lease (`claim_ttl_seconds`), so
//!    that of several gateways on one `state.db` exactly one publishes a row
//!    at a time;
//! 2. publish each with message id = event id = invocation id;
//! 3. on the broker's ACK, mark the row sent (a CAS on the claim), which also
//!    moves the invocation `Accepted` → `Queued`;
//! 4. on a failure, release the claim with an exponential backoff. When the
//!    queue is unavailable the rest of the batch is released at once instead
//!    of waiting for one timeout per row.
//!
//! Crash windows:
//!
//! - **before the publish** (claimed, nothing sent): the claim expires and the
//!   row is published by the next pass of any publisher;
//! - **after the ACK, before the mark**: the row is published again. Inside
//!   the broker's duplicate window the broker answers `duplicate` and stores
//!   nothing; outside it (or after a JetStream restart that forgot the id,
//!   ADR-0008 §1) a second message with the **same message id** is stored.
//!   Delivery is at-least-once: the consumer settles on the ledger, keyed by
//!   the invocation id, never on the message (ADR-0008 §1, PLT-4640).

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use tachyon_serverless_domain::{Clock, Timestamp};
use tachyon_serverless_durable_port::{EventQueue, MessageId, OutgoingMessage, QueueError, Topic};

use super::{InvokeAsyncConfig, QueueHealth};
use crate::failpoints::{self, Failpoints};
use crate::repository::{AsyncInvocationRepository, OutboxEvent};

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishReport {
    pub claimed: usize,
    /// Stored by the broker as a new message.
    pub published: usize,
    /// The broker recognised the message id and stored nothing.
    pub duplicates: usize,
    /// Marked sent in the ledger.
    pub marked: usize,
    /// Published, but the claim was no longer ours when marking (another
    /// publisher took the row after our claim expired).
    pub lost_claims: usize,
    pub failed: usize,
    pub purged: usize,
}

pub struct OutboxPublisher {
    ledger: Arc<dyn AsyncInvocationRepository>,
    queue: Arc<dyn EventQueue>,
    owner: String,
    clock: Arc<dyn Clock>,
    config: InvokeAsyncConfig,
    failpoints: Arc<Failpoints>,
    health: Arc<QueueHealth>,
    wake: Arc<Notify>,
    last_purge: Mutex<Option<Timestamp>>,
}

impl std::fmt::Debug for OutboxPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboxPublisher")
            .field("owner", &self.owner)
            .field("queue", &self.queue.backend())
            .finish_non_exhaustive()
    }
}

impl OutboxPublisher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ledger: Arc<dyn AsyncInvocationRepository>,
        queue: Arc<dyn EventQueue>,
        owner: impl Into<String>,
        clock: Arc<dyn Clock>,
        config: InvokeAsyncConfig,
        failpoints: Arc<Failpoints>,
        health: Arc<QueueHealth>,
        wake: Arc<Notify>,
    ) -> Self {
        Self {
            ledger,
            queue,
            owner: owner.into(),
            clock,
            config,
            failpoints,
            health,
            wake,
            last_purge: Mutex::new(None),
        }
    }

    /// The claim owner (this process's dispatcher id).
    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn health(&self) -> &Arc<QueueHealth> {
        &self.health
    }

    /// Sleep until an acceptance wakes the publisher or `publish_interval_ms`
    /// passes.
    pub async fn wait(&self) {
        let _ = tokio::time::timeout(self.config.publish_interval(), self.wake.notified()).await;
    }

    pub async fn run_once(&self) -> PublishReport {
        let mut report = PublishReport::default();
        let now = self.clock.now();
        self.purge(now, &mut report);
        let events = match self.ledger.claim_outbox(
            &self.owner,
            now,
            self.config.claim_ttl(),
            self.config.publish_batch,
        ) {
            Ok(events) => events,
            Err(e) => {
                tracing::warn!(error = %e, "outbox: claiming failed");
                report.failed += 1;
                return report;
            }
        };
        report.claimed = events.len();
        let mut rest = events.into_iter();
        while let Some(event) = rest.next() {
            if self.failpoints.fire(failpoints::OUTBOX_BEFORE_PUBLISH) {
                report.failed += 1;
                self.release(&event, "failpoint outbox.before_publish");
                continue;
            }
            match self.publish(&event).await {
                Ok(receipt) => {
                    self.health.record_ok();
                    if receipt.duplicate {
                        report.duplicates += 1;
                    } else {
                        report.published += 1;
                    }
                    if self.failpoints.fire(failpoints::OUTBOX_AFTER_PUBLISH) {
                        // As if the process died here: the claim is left to
                        // expire and the row is published again.
                        continue;
                    }
                    match self.ledger.mark_outbox_sent(
                        &event.event_id,
                        &self.owner,
                        receipt.sequence,
                        self.clock.now(),
                    ) {
                        Ok(true) => report.marked += 1,
                        Ok(false) => report.lost_claims += 1,
                        Err(e) => {
                            tracing::warn!(event = %event.event_id, error = %e, "outbox: marking sent failed; the row is published again after its claim expires");
                            report.failed += 1;
                        }
                    }
                }
                Err(e) => {
                    report.failed += 1;
                    self.health.record_error(&e, self.clock.now());
                    tracing::warn!(event = %event.event_id, attempts = event.publish_attempts, code = e.code(), error = %e, "outbox: publish failed");
                    let message = format!("{}: {e}", e.code());
                    self.release(&event, &message);
                    if matches!(e, QueueError::Unavailable(_) | QueueError::Unauthorized(_)) {
                        // Do not wait for one timeout per row.
                        for event in rest.by_ref() {
                            self.release(&event, &message);
                        }
                    }
                }
            }
        }
        if report.duplicates > 0 || report.lost_claims > 0 {
            tracing::info!(
                duplicates = report.duplicates,
                lost_claims = report.lost_claims,
                marked = report.marked,
                "outbox: re-published events the broker already had, or whose claim moved on"
            );
        }
        report
    }

    async fn publish(
        &self,
        event: &OutboxEvent,
    ) -> Result<tachyon_serverless_durable_port::PublishReceipt, QueueError> {
        if self.failpoints.fire(failpoints::OUTBOX_QUEUE_UNAVAILABLE) {
            return Err(QueueError::Unavailable(
                "failpoint outbox.queue_unavailable".into(),
            ));
        }
        let message = OutgoingMessage {
            tenant_id: event.tenant_id.clone(),
            topic: Topic::parse(&event.topic)?,
            message_id: MessageId::parse(event.event_id.as_str())?,
            payload: event.payload.clone().into_bytes(),
        };
        self.queue.publish(message).await
    }

    fn release(&self, event: &OutboxEvent, error: &str) {
        let next = self.clock.now() + self.config.backoff(event.publish_attempts.max(1));
        if let Err(e) = self
            .ledger
            .retry_outbox(&event.event_id, &self.owner, next, error)
        {
            tracing::warn!(event = %event.event_id, error = %e, "outbox: releasing the claim failed; it expires on its own");
        }
    }

    fn purge(&self, now: Timestamp, report: &mut PublishReport) {
        {
            let mut last = self.last_purge.lock();
            if last.is_some_and(|t| now - t < chrono::Duration::seconds(60)) {
                return;
            }
            *last = Some(now);
        }
        match self
            .ledger
            .purge_sent_outbox(now - self.config.sent_retention())
        {
            Ok(n) => report.purged = n,
            Err(e) => tracing::warn!(error = %e, "outbox: purging sent rows failed"),
        }
    }
}
