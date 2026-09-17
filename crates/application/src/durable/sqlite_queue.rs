//! Embedded SQLite [`EventQueue`] for development and CI (ADR-0008).
//!
//! One file, `<data_dir>/queue.db` (mode 0600, WAL, `synchronous = FULL`),
//! is one work-queue stream with the same semantics as the JetStream adapter
//! (`crates/adapters/queue-nats`); both pass the contract suite in
//! `tachyon_serverless_durable_port::testkit`. It is **not** a production
//! queue: it lives on one node's disk and is serialized by one connection.
//!
//! Every operation is one `BEGIN IMMEDIATE` transaction, so a publish that
//! returned `Ok` is on disk and a second process on the same file sees it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::Notify;

use tachyon_serverless_domain::{Clock, TenantId, Timestamp};
use tachyon_serverless_durable_port::{
    BacklogStats, ConsumerName, ConsumerSpec, Delivery, DeliveryToken, EventQueue, MessageId,
    OutgoingMessage, PublishReceipt, QueueError, QueueLimits, Topic,
};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS queue_meta (
    meta_key   TEXT NOT NULL PRIMARY KEY,
    meta_value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id    TEXT    NOT NULL,
    topic        TEXT    NOT NULL,
    message_id   TEXT    NOT NULL,
    payload      BLOB    NOT NULL,
    size         INTEGER NOT NULL,
    published_ms INTEGER NOT NULL,
    deliveries   INTEGER NOT NULL DEFAULT 0,
    visible_ms   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS messages_topic_visible ON messages (topic, visible_ms, seq);
CREATE INDEX IF NOT EXISTS messages_published ON messages (published_ms);
CREATE TABLE IF NOT EXISTS dedup (
    message_id   TEXT    NOT NULL PRIMARY KEY,
    seq          INTEGER NOT NULL,
    published_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS consumers (
    name        TEXT    NOT NULL PRIMARY KEY,
    topic       TEXT    NOT NULL UNIQUE,
    ack_wait_ms INTEGER NOT NULL,
    max_deliver INTEGER NOT NULL
);
INSERT OR IGNORE INTO queue_meta (meta_key, meta_value) VALUES ('schema_version', '1');
";

fn backend(e: rusqlite::Error) -> QueueError {
    QueueError::Backend(e.to_string())
}

fn ms(t: Timestamp) -> i64 {
    t.timestamp_millis()
}

fn dur_ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn create_private(path: &Path) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

pub struct SqliteEventQueue {
    conn: Mutex<Connection>,
    limits: QueueLimits,
    clock: Arc<dyn Clock>,
    path: PathBuf,
    wake: Notify,
}

impl std::fmt::Debug for SqliteEventQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteEventQueue")
            .field("path", &self.path)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl SqliteEventQueue {
    pub const FILE_NAME: &'static str = "queue.db";

    /// Open (or create) `path`.
    pub fn open(
        path: &Path,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, QueueError> {
        limits.validate().map_err(QueueError::InvalidMessage)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| QueueError::Backend(e.to_string()))?;
        }
        create_private(path).map_err(|e| QueueError::Backend(e.to_string()))?;
        let conn = Connection::open(path).map_err(backend)?;
        conn.busy_timeout(Duration::from_secs(5)).map_err(backend)?;
        let _mode: String = conn
            .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
            .map_err(backend)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(backend)?;
        conn.pragma_update(None, "secure_delete", "ON")
            .map_err(backend)?;
        conn.execute_batch(SCHEMA).map_err(backend)?;
        Ok(Self {
            conn: Mutex::new(conn),
            limits,
            clock,
            path: path.to_path_buf(),
            wake: Notify::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn tx<R>(
        &self,
        f: impl FnOnce(&Connection, i64) -> Result<R, QueueError>,
    ) -> Result<R, QueueError> {
        let now = ms(self.clock.now());
        let mut conn = self.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        // Retention applies before anything reads the stream.
        tx.execute(
            "DELETE FROM messages WHERE published_ms < ?1",
            [now.saturating_sub(dur_ms(self.limits.max_age))],
        )
        .map_err(backend)?;
        let out = f(&tx, now)?;
        tx.commit().map_err(backend)?;
        Ok(out)
    }

    fn consumer(conn: &Connection, name: &ConsumerName) -> Result<(String, i64, i64), QueueError> {
        conn.query_row(
            "SELECT topic, ack_wait_ms, max_deliver FROM consumers WHERE name = ?1",
            [name.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(backend)?
        .ok_or_else(|| QueueError::ConsumerNotFound(name.to_string()))
    }

    fn try_fetch(&self, name: &ConsumerName, max: usize) -> Result<Vec<Delivery>, QueueError> {
        self.tx(|tx, now| {
            let (topic, ack_wait, max_deliver) = Self::consumer(tx, name)?;
            let mut stmt = tx
                .prepare(
                    "SELECT seq, tenant_id, topic, message_id, payload, deliveries, published_ms
                     FROM messages
                     WHERE topic = ?1 AND visible_ms <= ?2 AND deliveries < ?3
                     ORDER BY seq LIMIT ?4",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map(
                    params![topic, now, max_deliver, max.min(10_000) as i64],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, Vec<u8>>(4)?,
                            r.get::<_, i64>(5)?,
                            r.get::<_, i64>(6)?,
                        ))
                    },
                )
                .map_err(backend)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(backend)?;
            let mut out = Vec::with_capacity(rows.len());
            for (seq, tenant, topic, message_id, payload, deliveries, published) in rows {
                let count = deliveries + 1;
                tx.execute(
                    "UPDATE messages SET deliveries = ?1, visible_ms = ?2 WHERE seq = ?3",
                    params![count, now.saturating_add(ack_wait), seq],
                )
                .map_err(backend)?;
                out.push(Delivery {
                    sequence: seq as u64,
                    tenant_id: TenantId::parse(&tenant)
                        .map_err(|e| QueueError::Backend(format!("stored tenant id: {e}")))?,
                    topic: Topic::parse(&topic)?,
                    message_id: MessageId::parse(&message_id)?,
                    payload,
                    delivery_count: u32::try_from(count).unwrap_or(u32::MAX),
                    published_at: chrono::DateTime::from_timestamp_millis(published)
                        .unwrap_or_default(),
                    token: DeliveryToken::new(format!("sqlite:{seq}:{count}")),
                });
            }
            Ok(out)
        })
    }

    fn parse_token(token: &DeliveryToken) -> Result<(i64, i64), QueueError> {
        let mut parts = token.as_str().split(':');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("sqlite"), Some(seq), Some(count), None) => {
                match (seq.parse::<i64>(), count.parse::<i64>()) {
                    (Ok(s), Ok(c)) => Ok((s, c)),
                    _ => Err(QueueError::InvalidMessage(
                        "malformed delivery token".into(),
                    )),
                }
            }
            _ => Err(QueueError::InvalidMessage(
                "malformed delivery token".into(),
            )),
        }
    }
}

#[async_trait]
impl EventQueue for SqliteEventQueue {
    fn backend(&self) -> &'static str {
        "sqlite"
    }

    fn limits(&self) -> QueueLimits {
        self.limits
    }

    async fn ensure_consumer(&self, spec: &ConsumerSpec) -> Result<(), QueueError> {
        if spec.max_deliver == 0 || spec.ack_wait.is_zero() {
            return Err(QueueError::InvalidMessage(
                "a consumer needs max_deliver >= 1 and ack_wait > 0".into(),
            ));
        }
        self.tx(|tx, _| {
            let bound: Option<String> = tx
                .query_row(
                    "SELECT name FROM consumers WHERE topic = ?1",
                    [spec.topic.as_str()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(backend)?;
            if let Some(other) = bound
                && other != spec.name.as_str()
            {
                return Err(QueueError::ConsumerConflict(format!(
                    "topic `{}` is already consumed by `{other}` (work queue)",
                    spec.topic
                )));
            }
            let existing: Option<String> = tx
                .query_row(
                    "SELECT topic FROM consumers WHERE name = ?1",
                    [spec.name.as_str()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(backend)?;
            if let Some(topic) = existing
                && topic != spec.topic.as_str()
            {
                return Err(QueueError::ConsumerConflict(format!(
                    "consumer `{}` is bound to topic `{topic}`",
                    spec.name
                )));
            }
            tx.execute(
                "INSERT INTO consumers (name, topic, ack_wait_ms, max_deliver) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (name) DO UPDATE SET ack_wait_ms = excluded.ack_wait_ms,
                                                  max_deliver = excluded.max_deliver",
                params![
                    spec.name.as_str(),
                    spec.topic.as_str(),
                    dur_ms(spec.ack_wait),
                    i64::from(spec.max_deliver)
                ],
            )
            .map_err(backend)?;
            Ok(())
        })
    }

    async fn publish(&self, message: OutgoingMessage) -> Result<PublishReceipt, QueueError> {
        let size = message.payload.len() as u64;
        let max = u64::from(self.limits.max_message_bytes);
        if size > max {
            return Err(QueueError::MessageTooLarge { size, max });
        }
        let receipt = self.tx(|tx, now| {
            tx.execute(
                "DELETE FROM dedup WHERE published_ms < ?1",
                [now.saturating_sub(dur_ms(self.limits.duplicate_window))],
            )
            .map_err(backend)?;
            let seen: Option<i64> = tx
                .query_row(
                    "SELECT seq FROM dedup WHERE message_id = ?1",
                    [message.message_id.as_str()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(backend)?;
            if let Some(seq) = seen {
                return Ok(PublishReceipt {
                    sequence: seq as u64,
                    duplicate: true,
                });
            }
            let (count, bytes): (i64, i64) = tx
                .query_row(
                    "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM messages",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(backend)?;
            if count as u64 >= self.limits.max_messages {
                return Err(QueueError::QueueFull(format!(
                    "maximum messages exceeded ({} of {})",
                    count, self.limits.max_messages
                )));
            }
            if bytes as u64 + size > self.limits.max_bytes {
                return Err(QueueError::QueueFull(format!(
                    "maximum bytes exceeded ({bytes} + {size} > {})",
                    self.limits.max_bytes
                )));
            }
            tx.execute(
                "INSERT INTO messages (tenant_id, topic, message_id, payload, size, published_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    message.tenant_id.as_str(),
                    message.topic.as_str(),
                    message.message_id.as_str(),
                    message.payload,
                    size as i64,
                    now
                ],
            )
            .map_err(backend)?;
            let seq = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO dedup (message_id, seq, published_ms) VALUES (?1, ?2, ?3)",
                params![message.message_id.as_str(), seq, now],
            )
            .map_err(backend)?;
            Ok(PublishReceipt {
                sequence: seq as u64,
                duplicate: false,
            })
        })?;
        self.wake.notify_waiters();
        Ok(receipt)
    }

    async fn fetch(
        &self,
        consumer: &ConsumerName,
        max: usize,
        wait: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let got = self.try_fetch(consumer, max.max(1))?;
            if !got.is_empty() {
                return Ok(got);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(Vec::new());
            }
            // Woken by a publish / nak; otherwise re-check for redeliveries.
            let step = (deadline - now).min(Duration::from_millis(50));
            let _ = tokio::time::timeout(step, self.wake.notified()).await;
        }
    }

    async fn ack(&self, token: &DeliveryToken) -> Result<(), QueueError> {
        let (seq, _) = Self::parse_token(token)?;
        self.tx(|tx, _| {
            tx.execute("DELETE FROM messages WHERE seq = ?1", [seq])
                .map_err(backend)?;
            Ok(())
        })
    }

    async fn nak(&self, token: &DeliveryToken, delay: Option<Duration>) -> Result<(), QueueError> {
        let (seq, count) = Self::parse_token(token)?;
        self.tx(|tx, now| {
            // Only the current delivery can be nak'ed; a stale token is a no-op.
            tx.execute(
                "UPDATE messages SET visible_ms = ?1 WHERE seq = ?2 AND deliveries = ?3",
                params![
                    now.saturating_add(delay.map(dur_ms).unwrap_or(0)),
                    seq,
                    count
                ],
            )
            .map_err(backend)?;
            Ok(())
        })?;
        self.wake.notify_waiters();
        Ok(())
    }

    async fn term(&self, token: &DeliveryToken) -> Result<(), QueueError> {
        self.ack(token).await
    }

    async fn stats(&self, consumer: &ConsumerName) -> Result<BacklogStats, QueueError> {
        self.tx(|tx, now| {
            let (topic, _, max_deliver) = Self::consumer(tx, consumer)?;
            let (messages, bytes): (i64, i64) = tx
                .query_row(
                    "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM messages",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(backend)?;
            let (pending, ack_pending, redelivered): (i64, i64, i64) = tx
                .query_row(
                    "SELECT
                        COALESCE(SUM(deliveries = 0), 0),
                        COALESCE(SUM(deliveries > 0 AND visible_ms > ?2 AND deliveries <= ?3), 0),
                        COALESCE(SUM(deliveries > 1), 0)
                     FROM messages WHERE topic = ?1",
                    params![topic, now, max_deliver],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .map_err(backend)?;
            Ok(BacklogStats {
                stream_messages: messages as u64,
                stream_bytes: bytes as u64,
                pending: pending as u64,
                ack_pending: ack_pending as u64,
                redelivered: redelivered as u64,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_serverless_domain::SystemClock;
    use tachyon_serverless_durable_port::testkit::{self, QueueHarness};

    #[derive(Default)]
    struct Harness {
        dir: Option<tempfile::TempDir>,
        opened: parking_lot::Mutex<Vec<(usize, PathBuf)>>,
    }

    fn key(q: &Arc<dyn EventQueue>) -> usize {
        Arc::as_ptr(q) as *const () as usize
    }

    impl Harness {
        fn new() -> Self {
            Self {
                dir: Some(tempfile::tempdir().unwrap()),
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl QueueHarness for Harness {
        async fn open(&self, limits: QueueLimits) -> Arc<dyn EventQueue> {
            let path = self.dir.as_ref().unwrap().path().join(format!(
                "{}.db",
                ulid::Ulid::new().to_string().to_lowercase()
            ));
            let q: Arc<dyn EventQueue> =
                Arc::new(SqliteEventQueue::open(&path, limits, Arc::new(SystemClock)).unwrap());
            self.opened.lock().push((key(&q), path));
            q
        }

        async fn reopen(&self, queue: Arc<dyn EventQueue>) -> Arc<dyn EventQueue> {
            let limits = queue.limits();
            let k = key(&queue);
            let path = self
                .opened
                .lock()
                .iter()
                .rev()
                .find(|(p, _)| *p == k)
                .map(|(_, path)| path.clone())
                .expect("a queue this harness opened");
            drop(queue);
            let q: Arc<dyn EventQueue> =
                Arc::new(SqliteEventQueue::open(&path, limits, Arc::new(SystemClock)).unwrap());
            self.opened.lock().push((key(&q), path));
            q
        }
    }

    /// PLT-4638: the SQLite queue passes the same contract as JetStream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_queue_passes_the_queue_contract() {
        let h = Harness::new();
        testkit::run_queue_contract(&h).await;
    }

    /// Capacity boundary on its own, so the security regression list can
    /// name it: a full stream refuses with `queue_full` and keeps its data.
    #[tokio::test]
    async fn a_full_queue_refuses_publish_with_queue_full() {
        let h = Harness::new();
        testkit::a_full_stream_refuses_the_publish_and_keeps_what_it_has(&h).await;
    }

    /// Messages published and not acked survive the process: a second open
    /// of the same file (the state a `kill -9` leaves) delivers them.
    #[tokio::test]
    async fn committed_messages_survive_a_reopen_of_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(SqliteEventQueue::FILE_NAME);
        let spec = ConsumerSpec {
            name: ConsumerName::parse("dispatcher").unwrap(),
            topic: Topic::parse("invoke").unwrap(),
            ack_wait: Duration::from_secs(30),
            max_deliver: 3,
        };
        {
            let q = SqliteEventQueue::open(&path, testkit::small_limits(), Arc::new(SystemClock))
                .unwrap();
            q.ensure_consumer(&spec).await.unwrap();
            for i in 0..10 {
                q.publish(OutgoingMessage {
                    tenant_id: testkit::tenant_a(),
                    topic: spec.topic.clone(),
                    message_id: MessageId::parse(&format!("m-{i}")).unwrap(),
                    payload: vec![i as u8],
                })
                .await
                .unwrap();
            }
            // leaked, never closed: no graceful shutdown
            std::mem::forget(q);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "queue.db is private");
        }
        let q =
            SqliteEventQueue::open(&path, testkit::small_limits(), Arc::new(SystemClock)).unwrap();
        let got = q
            .fetch(&spec.name, 100, Duration::from_millis(500))
            .await
            .unwrap();
        assert_eq!(got.len(), 10);
        assert_eq!(got[9].payload, vec![9u8]);
    }
}
