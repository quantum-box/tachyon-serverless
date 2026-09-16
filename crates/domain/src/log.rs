//! Log records bound to environment / invocation / attempt.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::ids::{AttemptId, EnvironmentId, InvocationId, TenantId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
    /// Emitted by the platform (host or bridge), not by user code.
    Platform,
}

/// Which phase of the environment lifecycle produced the line. Startup logs
/// are distinguished from handler logs (PLT-4628).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogPhase {
    Boot,
    Init,
    Handler,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub tenant_id: TenantId,
    pub environment_id: EnvironmentId,
    pub invocation_id: Option<InvocationId>,
    pub attempt_id: Option<AttemptId>,
    pub stream: LogStream,
    pub phase: LogPhase,
    pub timestamp: Timestamp,
    pub line: String,
    /// True when the line was cut at the per-line byte limit.
    pub truncated: bool,
}

impl LogRecord {
    /// Truncate a line to at most `max_bytes` on a char boundary.
    pub fn bounded_line(line: &str, max_bytes: usize) -> (String, bool) {
        if line.len() <= max_bytes {
            return (line.to_string(), false);
        }
        let mut end = max_bytes;
        while end > 0 && !line.is_char_boundary(end) {
            end -= 1;
        }
        (line[..end].to_string(), true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_line_respects_char_boundaries() {
        let (s, t) = LogRecord::bounded_line("あいう", 4);
        assert_eq!(s, "あ");
        assert!(t);
        let (s, t) = LogRecord::bounded_line("ok", 4);
        assert_eq!(s, "ok");
        assert!(!t);
    }
}
