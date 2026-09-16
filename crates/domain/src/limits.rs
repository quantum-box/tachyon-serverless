//! Platform limits. Values are prototype defaults (RFC §11.3), not SLAs.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum synchronous / asynchronous request payload in bytes.
    pub max_payload_bytes: u64,
    /// Maximum response payload in bytes.
    pub max_response_bytes: u64,
    /// Maximum execution timeout a revision may declare, in seconds.
    pub max_execution_timeout_seconds: u32,
    /// Maximum initialization timeout a revision may declare, in seconds.
    pub max_init_timeout_seconds: u32,
    /// Memory bounds in MiB.
    pub min_memory_mib: u32,
    pub max_memory_mib: u32,
    /// CPU bounds in millicores.
    pub min_cpu_millis: u32,
    pub max_cpu_millis: u32,
    /// Ephemeral storage upper bound in MiB.
    pub max_ephemeral_storage_mib: u32,
    /// Maximum number of log lines retained per invocation.
    pub max_log_lines_per_invocation: u32,
    /// Maximum bytes of log retained per invocation.
    pub max_log_bytes_per_invocation: u64,
    /// Maximum bytes of a single log line (longer lines are truncated).
    pub max_log_line_bytes: usize,
    /// Maximum artifact size accepted for upload, in bytes.
    pub max_artifact_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1024 * 1024,
            max_response_bytes: 6 * 1024 * 1024,
            max_execution_timeout_seconds: 15 * 60,
            max_init_timeout_seconds: 120,
            min_memory_mib: 128,
            max_memory_mib: 4096,
            min_cpu_millis: 250,
            max_cpu_millis: 2000,
            max_ephemeral_storage_mib: 2048,
            max_log_lines_per_invocation: 2000,
            max_log_bytes_per_invocation: 1024 * 1024,
            max_log_line_bytes: 16 * 1024,
            max_artifact_bytes: 256 * 1024 * 1024,
        }
    }
}
