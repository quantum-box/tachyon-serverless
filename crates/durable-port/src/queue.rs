//! Durable event queue port (ADR-0008 §「queue」).
//!
//! Semantics every implementation must provide (checked by
//! `crate::testkit` (feature `testkit`) against each of them):
//!
//! 1. **Durable publish.** `publish` returns only after the message is
//!    persisted by the backend (JetStream file store with `sync_interval:
//!    always`, or a committed SQLite transaction). A message whose publish
//!    returned `Ok` survives a restart of the queue process.
//! 2. **Dedup by message id.** A second publish with the same
//!    [`MessageId`] inside the duplicate window is not stored again and
//!    returns [`PublishReceipt::duplicate`] = true with the original sequence.
//!    Outside the window it is a new message. Dedup is a best-effort shield
//!    against a publisher retry, not exactly-once: the ledger keeps the
//!    authoritative idempotency (docs/threat-model.md §10).
//! 3. **Refuse, never evict.** When the stream is at `max_messages` or
//!    `max_bytes`, a publish fails with [`QueueError::QueueFull`] and nothing
//!    already stored is dropped (JetStream `discard: new`). A payload over
//!    `max_message_bytes` fails with [`QueueError::MessageTooLarge`].
//! 4. **Work-queue retention.** A message is removed when it is acked or
//!    terminated, or when it is older than `max_age` (acked or not). At most
//!    one consumer is bound to a topic.
//! 5. **Explicit ack.** A fetched message is invisible to other fetches for
//!    `ack_wait`; without an ack it is delivered again. `nak` makes it
//!    available again (after an optional delay), `term` removes it without
//!    further deliveries. After `max_deliver` deliveries it is not delivered
//!    again but stays until `max_age` (dead-lettering is PLT-4640).
//! 6. **At-least-once.** An ack can be lost (process crash between handler
//!    and ack), so a consumer must tolerate a redelivery; an ack that arrives
//!    after a redelivery may still remove the message.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tachyon_serverless_domain::{TenantId, Timestamp};

/// Publisher-supplied message id used for deduplication. 1..=128 bytes of
/// `[A-Za-z0-9._:-]`, so it is safe as a NATS header value and a SQL key.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MessageId(String);

impl MessageId {
    pub const MAX_BYTES: usize = 128;

    pub fn parse(raw: &str) -> Result<Self, QueueError> {
        if raw.is_empty()
            || raw.len() > Self::MAX_BYTES
            || !raw
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
        {
            return Err(QueueError::InvalidMessage(format!(
                "message id must be 1..={} bytes of [A-Za-z0-9._:-]",
                Self::MAX_BYTES
            )));
        }
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageId({})", self.0)
    }
}
impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl TryFrom<String> for MessageId {
    type Error = QueueError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<MessageId> for String {
    fn from(value: MessageId) -> String {
        value.0
    }
}

/// A label: 1..=64 bytes of `[a-z0-9-]`, starting and ending alphanumeric.
/// Used for topics and consumer names, which become NATS subject tokens and
/// durable names, so they can never contain `.`, `*`, `>` or whitespace.
fn is_label(raw: &str) -> bool {
    let b = raw.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

macro_rules! label {
    ($(#[$meta:meta])* $name:ident, $what:literal) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn parse(raw: &str) -> Result<Self, QueueError> {
                if is_label(raw) {
                    Ok(Self(raw.to_string()))
                } else {
                    Err(QueueError::InvalidMessage(format!(
                        concat!($what, " `{}` must be 1..=64 bytes of [a-z0-9-]"),
                        raw
                    )))
                }
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl TryFrom<String> for $name {
            type Error = QueueError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(&value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0
            }
        }
    };
}

label!(
    /// Event topic, e.g. `invoke`. One consumer per topic (work queue).
    Topic,
    "topic"
);
label!(
    /// Durable consumer name, e.g. `dispatcher`.
    ConsumerName,
    "consumer name"
);

/// A message to publish. The tenant is part of the routing key (NATS
/// subject `<prefix>.<tenant>.<topic>`) and comes back on every delivery.
#[derive(Clone, PartialEq, Eq)]
pub struct OutgoingMessage {
    pub tenant_id: TenantId,
    pub topic: Topic,
    pub message_id: MessageId,
    pub payload: Vec<u8>,
}

impl fmt::Debug for OutgoingMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutgoingMessage")
            .field("tenant_id", &self.tenant_id)
            .field("topic", &self.topic)
            .field("message_id", &self.message_id)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishReceipt {
    /// Stream sequence of the stored message (of the original one for a
    /// duplicate).
    pub sequence: u64,
    /// True when the message id was already published inside the duplicate
    /// window and nothing new was stored.
    pub duplicate: bool,
}

/// Opaque handle naming one delivery; passed back to ack / nak / term.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DeliveryToken(String);

impl DeliveryToken {
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DeliveryToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeliveryToken({})", self.0)
    }
}

#[derive(Clone)]
pub struct Delivery {
    pub sequence: u64,
    pub tenant_id: TenantId,
    pub topic: Topic,
    pub message_id: MessageId,
    pub payload: Vec<u8>,
    /// 1 on the first delivery, 2 on the first redelivery, ...
    pub delivery_count: u32,
    pub published_at: Timestamp,
    pub token: DeliveryToken,
}

impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("sequence", &self.sequence)
            .field("tenant_id", &self.tenant_id)
            .field("topic", &self.topic)
            .field("message_id", &self.message_id)
            .field("payload_bytes", &self.payload.len())
            .field("delivery_count", &self.delivery_count)
            .finish_non_exhaustive()
    }
}

/// A durable pull consumer bound to one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerSpec {
    pub name: ConsumerName,
    pub topic: Topic,
    /// How long a delivery stays invisible without an ack.
    pub ack_wait: Duration,
    /// Deliveries after which a message is no longer delivered (>= 1).
    pub max_deliver: u32,
}

/// Stream limits (infrastructure as code: the adapter applies them at
/// startup, idempotently).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueLimits {
    pub max_messages: u64,
    pub max_bytes: u64,
    pub max_message_bytes: u32,
    /// Messages older than this are removed, acked or not.
    pub max_age: Duration,
    /// Window in which a repeated message id is a duplicate (<= max_age).
    pub duplicate_window: Duration,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_messages: 100_000,
            max_bytes: 256 * 1024 * 1024,
            max_message_bytes: 256 * 1024,
            max_age: Duration::from_secs(7 * 24 * 60 * 60),
            duplicate_window: Duration::from_secs(2 * 60),
        }
    }
}

impl QueueLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_messages == 0 || self.max_bytes == 0 || self.max_message_bytes == 0 {
            return Err("max_messages, max_bytes and max_message_bytes must be > 0".into());
        }
        if u64::from(self.max_message_bytes) > self.max_bytes {
            return Err("max_message_bytes must be <= max_bytes".into());
        }
        if self.max_age.is_zero() {
            return Err("max_age must be > 0 (an unbounded stream is not allowed)".into());
        }
        if self.duplicate_window.is_zero() || self.duplicate_window > self.max_age {
            return Err("duplicate_window must be > 0 and <= max_age".into());
        }
        Ok(())
    }
}

/// Backlog of one consumer's topic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BacklogStats {
    /// Messages stored in the whole stream.
    pub stream_messages: u64,
    /// Bytes stored in the whole stream (backend-specific accounting).
    pub stream_bytes: u64,
    /// Messages of the consumer's topic never delivered yet.
    pub pending: u64,
    /// Delivered and neither acked nor terminated yet.
    pub ack_pending: u64,
    /// Messages delivered more than once.
    pub redelivered: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// The stream is at a capacity limit; nothing was stored (`queue_full`).
    #[error("queue_full: {0}")]
    QueueFull(String),
    #[error("message too large: {size} bytes (max {max})")]
    MessageTooLarge { size: u64, max: u64 },
    #[error("invalid message: {0}")]
    InvalidMessage(String),
    /// The backend refused the credentials, or none were configured.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("consumer not found: {0}")]
    ConsumerNotFound(String),
    /// Another consumer is already bound to the topic, or the consumer exists
    /// with a different topic.
    #[error("consumer conflict: {0}")]
    ConsumerConflict(String),
    /// The backend cannot be reached. Retryable.
    #[error("queue unavailable: {0}")]
    Unavailable(String),
    #[error("queue backend: {0}")]
    Backend(String),
}

impl QueueError {
    /// Stable machine-readable code (`queue_full`, ...).
    pub fn code(&self) -> &'static str {
        match self {
            Self::QueueFull(_) => "queue_full",
            Self::MessageTooLarge { .. } => "message_too_large",
            Self::InvalidMessage(_) => "invalid_message",
            Self::Unauthorized(_) => "unauthorized",
            Self::ConsumerNotFound(_) => "consumer_not_found",
            Self::ConsumerConflict(_) => "consumer_conflict",
            Self::Unavailable(_) => "unavailable",
            Self::Backend(_) => "backend",
        }
    }
}

/// Durable, at-least-once event queue (module docs for the contract).
#[async_trait]
pub trait EventQueue: Send + Sync {
    /// `"nats"` or `"sqlite"`.
    fn backend(&self) -> &'static str;

    /// Limits the stream was bootstrapped with.
    fn limits(&self) -> QueueLimits;

    /// Create the durable consumer, or confirm an identical one exists.
    /// Changing `ack_wait` / `max_deliver` of an existing consumer updates it.
    async fn ensure_consumer(&self, spec: &ConsumerSpec) -> Result<(), QueueError>;

    async fn publish(&self, message: OutgoingMessage) -> Result<PublishReceipt, QueueError>;

    /// Up to `max` deliveries, waiting at most `wait` for the first one.
    async fn fetch(
        &self,
        consumer: &ConsumerName,
        max: usize,
        wait: Duration,
    ) -> Result<Vec<Delivery>, QueueError>;

    /// Remove the message. Idempotent.
    async fn ack(&self, token: &DeliveryToken) -> Result<(), QueueError>;

    /// Make the message deliverable again after `delay` (at once when `None`).
    async fn nak(&self, token: &DeliveryToken, delay: Option<Duration>) -> Result<(), QueueError>;

    /// Remove the message without further deliveries (poison message).
    async fn term(&self, token: &DeliveryToken) -> Result<(), QueueError>;

    async fn stats(&self, consumer: &ConsumerName) -> Result<BacklogStats, QueueError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_and_ids_refuse_subject_metacharacters() {
        for bad in [
            "",
            "a.b",
            "a*",
            ">",
            "A",
            "a b",
            "-a",
            "a-",
            &"a".repeat(65),
        ] {
            assert!(Topic::parse(bad).is_err(), "{bad:?}");
            assert!(ConsumerName::parse(bad).is_err(), "{bad:?}");
        }
        assert!(Topic::parse("invoke-async").is_ok());
        for bad in ["", "a b", "a\nb", "a*", &"a".repeat(129)] {
            assert!(MessageId::parse(bad).is_err(), "{bad:?}");
        }
        assert!(MessageId::parse("inv_01j:attempt-1.x").is_ok());
        let id: Result<MessageId, _> = serde_json::from_str("\"bad id\"");
        assert!(id.is_err());
    }

    #[test]
    fn limits_must_be_bounded() {
        assert!(QueueLimits::default().validate().is_ok());
        let unbounded = QueueLimits {
            max_age: Duration::ZERO,
            ..QueueLimits::default()
        };
        assert!(unbounded.validate().is_err());
        let window = QueueLimits {
            duplicate_window: Duration::from_secs(10_000_000),
            ..QueueLimits::default()
        };
        assert!(window.validate().is_err());
        let msg = QueueLimits {
            max_bytes: 10,
            max_message_bytes: 11,
            ..QueueLimits::default()
        };
        assert!(msg.validate().is_err());
    }
}
