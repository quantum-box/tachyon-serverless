//! Per-invocation log buffer, bounded by [`Limits`]. Logs are never
//! persisted (docs/adr/0003 decision 5): both stores embed this.

use std::collections::HashMap;

use parking_lot::RwLock;
use tachyon_serverless_domain::{InvocationId, Limits, LogRecord};

use super::{AppendOutcome, LogQuery};

#[derive(Debug, Default)]
struct LogBucket {
    records: Vec<LogRecord>,
    bytes: u64,
    dropped: bool,
}

#[derive(Debug, Default)]
pub(crate) struct LogBuffer {
    buckets: RwLock<HashMap<String, LogBucket>>,
}

fn log_key(record: &LogRecord) -> String {
    match &record.invocation_id {
        Some(inv) => format!("inv:{inv}"),
        None => format!("env:{}", record.environment_id),
    }
}

impl LogBuffer {
    pub(crate) fn append(&self, limits: &Limits, record: LogRecord) -> AppendOutcome {
        let key = log_key(&record);
        let max_lines = limits.max_log_lines_per_invocation as usize;
        let max_bytes = limits.max_log_bytes_per_invocation;
        let mut buckets = self.buckets.write();
        let bucket = buckets.entry(key).or_default();
        let line_bytes = record.line.len() as u64;
        if bucket.records.len() >= max_lines || bucket.bytes + line_bytes > max_bytes {
            bucket.dropped = true;
            return AppendOutcome::Dropped;
        }
        bucket.bytes += line_bytes;
        bucket.records.push(record);
        AppendOutcome::Stored
    }

    pub(crate) fn query(&self, invocation: &InvocationId) -> LogQuery {
        match self.buckets.read().get(&format!("inv:{invocation}")) {
            Some(b) => LogQuery {
                records: b.records.clone(),
                dropped: b.dropped,
            },
            None => LogQuery::default(),
        }
    }
}
