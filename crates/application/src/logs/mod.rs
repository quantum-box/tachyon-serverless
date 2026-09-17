//! Durable, bounded invocation logs (docs/adr/0018).
//!
//! `<data_dir>/logs/logs.db` is a SQLite file of its own, apart from
//! `state.db`, so the log write volume never competes for the ledger's
//! writer lock. The bridge session appends lines into a bounded in-memory
//! queue without waiting for IO; one writer thread commits the queue in
//! batches (every `flush_interval_ms` or `flush_max_lines`), enforces the
//! per-invocation and per-attempt caps, and runs retention by age and by
//! total size. See [`store::DurableLogStore`].
//!
//! `[store] backend = "memory"` (and a gateway that does not persist state)
//! keeps the bounded memory buffer of `repository::logs` instead.

pub mod store;
#[cfg(test)]
mod tests;

use std::time::Duration;

use serde::Deserialize;

pub use store::{DROP_REASONS, DurableLogStore, LogStoreMetrics, LogStoreStatus, RetentionReport};

/// `[logs]`: retention, size cap, caps per attempt and the writer queue.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogsConfig {
    /// Seconds the lines of an invocation are kept after its last line.
    /// `0` keeps them until the size cap removes them.
    pub retention_seconds: u64,
    /// Line bytes (the text of every stored line, markers included) above
    /// which the retention pass deletes the oldest terminal invocations'
    /// logs. `0` disables the size cap. SQLite pages and indexes come on top:
    /// the file is larger than this.
    pub max_total_bytes: u64,
    /// Lines one attempt may store. Defaults to
    /// `[limits] max_log_lines_per_invocation`.
    pub max_lines_per_attempt: Option<u32>,
    /// Line bytes one attempt may store. Defaults to
    /// `[limits] max_log_bytes_per_invocation`.
    pub max_bytes_per_attempt: Option<u64>,
    /// The writer commits whatever is queued at least this often. It is also
    /// the crash window: lines younger than this may be lost on a crash.
    pub flush_interval_ms: u64,
    /// The writer commits early once this many lines are queued.
    pub flush_max_lines: usize,
    /// Lines the queue holds before new lines are dropped (and counted).
    pub queue_max_lines: usize,
    /// Line bytes the queue holds before new lines are dropped.
    pub queue_max_bytes: u64,
    /// Seconds between two retention passes of the writer.
    pub retention_interval_seconds: u64,
    /// How long a read waits for the lines queued before it to be committed
    /// (read-your-writes). It never waits longer, even when `logs.db` is
    /// locked.
    pub read_flush_wait_ms: u64,
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            retention_seconds: 7 * 24 * 60 * 60,
            max_total_bytes: 1024 * 1024 * 1024,
            max_lines_per_attempt: None,
            max_bytes_per_attempt: None,
            flush_interval_ms: 200,
            flush_max_lines: 1000,
            queue_max_lines: 20_000,
            queue_max_bytes: 16 * 1024 * 1024,
            retention_interval_seconds: 60,
            read_flush_wait_ms: 1000,
        }
    }
}

impl LogsConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=60_000).contains(&self.flush_interval_ms) {
            return Err("[logs] flush_interval_ms must be within 1..=60000".into());
        }
        if self.flush_max_lines == 0 || self.queue_max_lines == 0 {
            return Err("[logs] flush_max_lines and queue_max_lines must be >= 1".into());
        }
        if self.queue_max_bytes < 64 * 1024 {
            return Err("[logs] queue_max_bytes must be >= 65536".into());
        }
        if self.retention_interval_seconds == 0 {
            return Err("[logs] retention_interval_seconds must be >= 1".into());
        }
        if self.read_flush_wait_ms > 30_000 {
            return Err("[logs] read_flush_wait_ms must be <= 30000".into());
        }
        if self.max_lines_per_attempt == Some(0) || self.max_bytes_per_attempt == Some(0) {
            return Err(
                "[logs] max_lines_per_attempt and max_bytes_per_attempt must be >= 1".into(),
            );
        }
        Ok(())
    }

    pub fn flush_interval(&self) -> Duration {
        Duration::from_millis(self.flush_interval_ms)
    }

    pub fn retention_interval(&self) -> Duration {
        Duration::from_secs(self.retention_interval_seconds)
    }

    pub fn read_flush_wait(&self) -> Duration {
        Duration::from_millis(self.read_flush_wait_ms)
    }
}
