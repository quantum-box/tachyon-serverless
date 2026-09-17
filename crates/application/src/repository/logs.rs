//! Per-invocation log buffer, bounded by [`Limits`]. Every store embeds
//! this; the gateway replaces it by the durable log store
//! (`crate::logs::DurableLogStore`, docs/adr/0018) whenever the ledger is
//! durable, so it only serves `[store] backend = "memory"` and tests.

use std::collections::HashMap;

use parking_lot::RwLock;
use tachyon_serverless_domain::{InvocationId, Limits, LogRecord, TenantId};

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

    /// Lines of `invocation` that belong to `tenant`; another tenant's
    /// invocation answers like one without lines.
    pub(crate) fn query(&self, tenant: &TenantId, invocation: &InvocationId) -> LogQuery {
        match self.buckets.read().get(&format!("inv:{invocation}")) {
            Some(b) => {
                let records: Vec<LogRecord> = b
                    .records
                    .iter()
                    .filter(|r| &r.tenant_id == tenant)
                    .cloned()
                    .collect();
                let dropped = b.dropped && !records.is_empty();
                LogQuery { records, dropped }
            }
            None => LogQuery::default(),
        }
    }
}
