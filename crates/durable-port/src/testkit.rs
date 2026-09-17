//! Contract suite for [`EventQueue`] implementations (feature `testkit`).
//!
//! Every implementation runs every case through a [`QueueHarness`]:
//!
//! ```ignore
//! #[tokio::test]
//! async fn contract() {
//!     tachyon_serverless_durable_port::testkit::run_queue_contract(&MyHarness::new()).await;
//! }
//! ```
//!
//! The cases use real time with second-scale ack waits and ages, because the
//! JetStream server keeps its own clock. The whole suite takes ~15 s.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tachyon_serverless_domain::TenantId;

use crate::queue::*;

/// Opens isolated queues for the suite.
#[async_trait]
pub trait QueueHarness: Send + Sync {
    /// A fresh, empty queue (its own stream / file) with `limits`.
    async fn open(&self, limits: QueueLimits) -> Arc<dyn EventQueue>;

    /// Drop every connection to `queue` and open it again: same stream /
    /// file, new client. Whatever was published and not acked must still be
    /// there.
    async fn reopen(&self, queue: Arc<dyn EventQueue>) -> Arc<dyn EventQueue>;
}

pub fn tenant_a() -> TenantId {
    TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzza").unwrap()
}

pub fn tenant_b() -> TenantId {
    TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzzb").unwrap()
}

fn topic() -> Topic {
    Topic::parse("invoke").unwrap()
}

fn consumer() -> ConsumerName {
    ConsumerName::parse("dispatcher").unwrap()
}

pub fn small_limits() -> QueueLimits {
    QueueLimits {
        max_messages: 1_000,
        max_bytes: 1024 * 1024,
        max_message_bytes: 64 * 1024,
        max_age: Duration::from_secs(600),
        duplicate_window: Duration::from_secs(60),
    }
}

fn spec(ack_wait: Duration, max_deliver: u32) -> ConsumerSpec {
    ConsumerSpec {
        name: consumer(),
        topic: topic(),
        ack_wait,
        max_deliver,
    }
}

fn message(tenant: &TenantId, id: &str, payload: &[u8]) -> OutgoingMessage {
    OutgoingMessage {
        tenant_id: tenant.clone(),
        topic: topic(),
        message_id: MessageId::parse(id).unwrap(),
        payload: payload.to_vec(),
    }
}

async fn fetch_all(q: &dyn EventQueue, max: usize, wait: Duration) -> Vec<Delivery> {
    q.fetch(&consumer(), max, wait).await.expect("fetch")
}

async fn eventually<F, Fut>(what: &str, deadline: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if check().await {
            return;
        }
        assert!(start.elapsed() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Run every case. Each opens its own queue.
pub async fn run_queue_contract(h: &dyn QueueHarness) {
    publish_fetch_ack_removes_the_message(h).await;
    a_repeated_message_id_is_stored_once(h).await;
    a_full_stream_refuses_the_publish_and_keeps_what_it_has(h).await;
    an_unacked_delivery_is_redelivered_after_ack_wait(h).await;
    nak_redelivers_and_term_removes(h).await;
    max_deliver_stops_redelivery(h).await;
    messages_older_than_max_age_are_removed(h).await;
    unacked_messages_survive_a_reopen(h).await;
    one_consumer_per_topic(h).await;
}

pub async fn publish_fetch_ack_removes_the_message(h: &dyn QueueHarness) {
    let q = h.open(small_limits()).await;
    q.ensure_consumer(&spec(Duration::from_secs(30), 5))
        .await
        .unwrap();
    // idempotent
    q.ensure_consumer(&spec(Duration::from_secs(30), 5))
        .await
        .unwrap();
    let a = q
        .publish(message(&tenant_a(), "m-1", b"one"))
        .await
        .unwrap();
    let b = q
        .publish(message(&tenant_b(), "m-2", b"two"))
        .await
        .unwrap();
    assert!(!a.duplicate && !b.duplicate);
    assert!(b.sequence > a.sequence);

    let stats = q.stats(&consumer()).await.unwrap();
    assert_eq!(stats.stream_messages, 2);
    assert_eq!(stats.pending, 2);

    let got = fetch_all(q.as_ref(), 10, Duration::from_secs(2)).await;
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got[0].payload, b"one");
    assert_eq!(got[0].tenant_id, tenant_a());
    assert_eq!(got[0].message_id.as_str(), "m-1");
    assert_eq!(got[0].delivery_count, 1);
    assert_eq!(got[1].tenant_id, tenant_b());
    let stats = q.stats(&consumer()).await.unwrap();
    assert_eq!(stats.ack_pending, 2);
    assert_eq!(stats.pending, 0);

    for d in &got {
        q.ack(&d.token).await.unwrap();
        // idempotent
        q.ack(&d.token).await.unwrap();
    }
    eventually(
        "acked messages leave the stream",
        Duration::from_secs(5),
        || {
            let q = q.clone();
            async move {
                let s = q.stats(&consumer()).await.unwrap();
                s.stream_messages == 0 && s.ack_pending == 0
            }
        },
    )
    .await;
    let none = fetch_all(q.as_ref(), 10, Duration::from_millis(300)).await;
    assert!(none.is_empty(), "{none:?}");
}

pub async fn a_repeated_message_id_is_stored_once(h: &dyn QueueHarness) {
    let q = h.open(small_limits()).await;
    q.ensure_consumer(&spec(Duration::from_secs(30), 5))
        .await
        .unwrap();
    let first = q.publish(message(&tenant_a(), "dup", b"x")).await.unwrap();
    let again = q.publish(message(&tenant_a(), "dup", b"x")).await.unwrap();
    assert!(!first.duplicate);
    assert!(again.duplicate, "{again:?}");
    assert_eq!(again.sequence, first.sequence);
    assert_eq!(q.stats(&consumer()).await.unwrap().stream_messages, 1);
}

pub async fn a_full_stream_refuses_the_publish_and_keeps_what_it_has(h: &dyn QueueHarness) {
    let limits = QueueLimits {
        max_messages: 3,
        max_bytes: 1024 * 1024,
        max_message_bytes: 1024,
        ..small_limits()
    };
    let q = h.open(limits).await;
    q.ensure_consumer(&spec(Duration::from_secs(30), 5))
        .await
        .unwrap();
    for i in 0..3 {
        q.publish(message(&tenant_a(), &format!("cap-{i}"), b"x"))
            .await
            .unwrap();
    }
    let err = q
        .publish(message(&tenant_a(), "cap-3", b"x"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), "queue_full", "{err}");
    let err = q
        .publish(message(&tenant_a(), "big", &vec![0u8; 2048]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            QueueError::MessageTooLarge { .. } | QueueError::QueueFull(_)
        ),
        "{err}"
    );
    // Discard-new: the first three are intact and in order.
    let got = fetch_all(q.as_ref(), 10, Duration::from_secs(2)).await;
    let ids: Vec<_> = got
        .iter()
        .map(|d| d.message_id.as_str().to_string())
        .collect();
    assert_eq!(ids, ["cap-0", "cap-1", "cap-2"]);
    // Acking frees capacity (work-queue retention).
    q.ack(&got[0].token).await.unwrap();
    eventually(
        "capacity is freed by an ack",
        Duration::from_secs(5),
        || {
            let q = q.clone();
            async move { q.publish(message(&tenant_a(), "cap-4", b"x")).await.is_ok() }
        },
    )
    .await;
    // A payload over max_message_bytes with room in the stream is too large.
    let q = h
        .open(QueueLimits {
            max_message_bytes: 1024,
            ..small_limits()
        })
        .await;
    let err = q
        .publish(message(&tenant_a(), "big", &vec![0u8; 2048]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            QueueError::MessageTooLarge { .. } | QueueError::QueueFull(_)
        ),
        "{err}"
    );
}

pub async fn an_unacked_delivery_is_redelivered_after_ack_wait(h: &dyn QueueHarness) {
    let q = h.open(small_limits()).await;
    q.ensure_consumer(&spec(Duration::from_secs(1), 5))
        .await
        .unwrap();
    q.publish(message(&tenant_a(), "r-1", b"x")).await.unwrap();
    let first = fetch_all(q.as_ref(), 1, Duration::from_secs(2)).await;
    assert_eq!(first.len(), 1);
    // Invisible while in flight.
    let hidden = fetch_all(q.as_ref(), 1, Duration::from_millis(200)).await;
    assert!(hidden.is_empty(), "{hidden:?}");
    let again = fetch_all(q.as_ref(), 1, Duration::from_secs(4)).await;
    assert_eq!(again.len(), 1, "redelivered after ack_wait");
    assert_eq!(again[0].message_id.as_str(), "r-1");
    assert_eq!(again[0].delivery_count, 2);
    q.ack(&again[0].token).await.unwrap();
    eventually("acked", Duration::from_secs(5), || {
        let q = q.clone();
        async move { q.stats(&consumer()).await.unwrap().stream_messages == 0 }
    })
    .await;
}

pub async fn nak_redelivers_and_term_removes(h: &dyn QueueHarness) {
    let q = h.open(small_limits()).await;
    q.ensure_consumer(&spec(Duration::from_secs(30), 5))
        .await
        .unwrap();
    q.publish(message(&tenant_a(), "n-1", b"nak"))
        .await
        .unwrap();
    q.publish(message(&tenant_a(), "t-1", b"term"))
        .await
        .unwrap();
    let got = fetch_all(q.as_ref(), 2, Duration::from_secs(2)).await;
    assert_eq!(got.len(), 2);
    q.nak(&got[0].token, None).await.unwrap();
    q.term(&got[1].token).await.unwrap();
    let again = fetch_all(q.as_ref(), 2, Duration::from_secs(3)).await;
    assert_eq!(again.len(), 1, "{again:?}");
    assert_eq!(again[0].message_id.as_str(), "n-1");
    assert_eq!(again[0].delivery_count, 2);
    // a delayed nak hides the message for the delay
    q.nak(&again[0].token, Some(Duration::from_secs(2)))
        .await
        .unwrap();
    let hidden = fetch_all(q.as_ref(), 1, Duration::from_millis(500)).await;
    assert!(hidden.is_empty(), "{hidden:?}");
    let third = fetch_all(q.as_ref(), 1, Duration::from_secs(5)).await;
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].delivery_count, 3);
    q.ack(&third[0].token).await.unwrap();
    eventually(
        "termed and acked messages are gone",
        Duration::from_secs(5),
        || {
            let q = q.clone();
            async move { q.stats(&consumer()).await.unwrap().stream_messages == 0 }
        },
    )
    .await;
}

pub async fn max_deliver_stops_redelivery(h: &dyn QueueHarness) {
    let q = h.open(small_limits()).await;
    q.ensure_consumer(&spec(Duration::from_secs(30), 2))
        .await
        .unwrap();
    q.publish(message(&tenant_a(), "p-1", b"poison"))
        .await
        .unwrap();
    let one = fetch_all(q.as_ref(), 1, Duration::from_secs(2)).await;
    assert_eq!(one[0].delivery_count, 1);
    q.nak(&one[0].token, None).await.unwrap();
    let two = fetch_all(q.as_ref(), 1, Duration::from_secs(3)).await;
    assert_eq!(two.len(), 1);
    assert_eq!(two[0].delivery_count, 2);
    q.nak(&two[0].token, None).await.unwrap();
    let three = fetch_all(q.as_ref(), 1, Duration::from_secs(1)).await;
    assert!(
        three.is_empty(),
        "not delivered past max_deliver: {three:?}"
    );
    // still stored (dead-lettering is PLT-4640), not silently dropped
    assert_eq!(q.stats(&consumer()).await.unwrap().stream_messages, 1);
}

pub async fn messages_older_than_max_age_are_removed(h: &dyn QueueHarness) {
    let q = h
        .open(QueueLimits {
            max_age: Duration::from_secs(1),
            duplicate_window: Duration::from_secs(1),
            ..small_limits()
        })
        .await;
    q.ensure_consumer(&spec(Duration::from_secs(30), 5))
        .await
        .unwrap();
    q.publish(message(&tenant_a(), "old-1", b"x"))
        .await
        .unwrap();
    q.publish(message(&tenant_a(), "old-2", b"x"))
        .await
        .unwrap();
    // one of them in flight: max_age applies to unacked messages too
    let got = fetch_all(q.as_ref(), 1, Duration::from_secs(2)).await;
    assert_eq!(got.len(), 1);
    eventually("max_age expiry", Duration::from_secs(6), || {
        let q = q.clone();
        async move { q.stats(&consumer()).await.unwrap().stream_messages == 0 }
    })
    .await;
    let none = fetch_all(q.as_ref(), 5, Duration::from_millis(300)).await;
    assert!(none.is_empty(), "{none:?}");
}

pub async fn unacked_messages_survive_a_reopen(h: &dyn QueueHarness) {
    let q = h.open(small_limits()).await;
    q.ensure_consumer(&spec(Duration::from_secs(1), 5))
        .await
        .unwrap();
    for i in 0..5 {
        q.publish(message(
            &tenant_a(),
            &format!("s-{i}"),
            format!("{i}").as_bytes(),
        ))
        .await
        .unwrap();
    }
    let got = fetch_all(q.as_ref(), 2, Duration::from_secs(2)).await;
    assert_eq!(got.len(), 2);
    q.ack(&got[0].token).await.unwrap();
    // got[1] stays in flight across the reopen
    let q = h.reopen(q).await;
    q.ensure_consumer(&spec(Duration::from_secs(1), 5))
        .await
        .unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let start = Instant::now();
    while seen.len() < 4 && start.elapsed() < Duration::from_secs(8) {
        for d in fetch_all(q.as_ref(), 10, Duration::from_secs(2)).await {
            seen.insert(d.message_id.as_str().to_string());
            q.ack(&d.token).await.unwrap();
        }
    }
    let expected: std::collections::BTreeSet<String> = ["s-1", "s-2", "s-3", "s-4"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        seen, expected,
        "acked s-0 is gone, everything else survived"
    );
    // dedup state survives too
    let dup = q.publish(message(&tenant_a(), "s-2", b"2")).await.unwrap();
    assert!(dup.duplicate, "{dup:?}");
}

pub async fn one_consumer_per_topic(h: &dyn QueueHarness) {
    let q = h.open(small_limits()).await;
    q.ensure_consumer(&spec(Duration::from_secs(30), 5))
        .await
        .unwrap();
    let other = ConsumerSpec {
        name: ConsumerName::parse("other").unwrap(),
        ..spec(Duration::from_secs(30), 5)
    };
    let err = q.ensure_consumer(&other).await.unwrap_err();
    assert!(matches!(err, QueueError::ConsumerConflict(_)), "{err}");
    let err = q
        .fetch(
            &ConsumerName::parse("missing").unwrap(),
            1,
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::ConsumerNotFound(_)), "{err}");
}
