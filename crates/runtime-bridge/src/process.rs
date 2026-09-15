//! User process supervision: spawning in its own process group, signalling
//! the whole group, and forwarding stdout/stderr lines as `Log` frames with
//! bounded memory.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tachyon_serverless_protocol::{GuestMessage, LogStream};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::runtime_api::RuntimeApi;

/// Everything needed to start the user process. `env` may contain secrets
/// and must never be logged or debug-printed.
pub struct SpawnSpec<'a> {
    pub entrypoint: &'a str,
    pub args: &'a [String],
    pub env: &'a [(String, String)],
    pub working_dir: &'a str,
    pub runtime_api_url: &'a str,
    pub environment_id: &'a str,
    /// Forward `TACHYON_UNISOLATED=1` (process provider).
    pub unisolated: bool,
}

/// Spawn the user process with a clean environment in a fresh process group
/// (pgid == pid) so the bridge can signal the process and its descendants.
pub fn spawn_user(spec: &SpawnSpec<'_>) -> std::io::Result<Child> {
    let mut cmd = Command::new(spec.entrypoint);
    cmd.args(spec.args)
        .env_clear()
        .envs(spec.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .env(
            tachyon_serverless_protocol::env::RUNTIME_API,
            spec.runtime_api_url,
        )
        .env(
            tachyon_serverless_protocol::env::ENVIRONMENT_ID,
            spec.environment_id,
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if spec.unisolated {
        cmd.env(tachyon_serverless_protocol::env::UNISOLATED, "1");
    }
    if !spec.working_dir.is_empty() && Path::new(spec.working_dir).is_dir() {
        cmd.current_dir(spec.working_dir);
    }
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.spawn()
}

/// Send `signal` to the process group led by `pid` (falls back to the pid
/// itself when the group is already gone).
#[cfg(unix)]
pub fn signal_group(pid: u32, signal: i32) {
    // SAFETY: plain libc calls with a pid we spawned; errors (ESRCH when the
    // process is already gone) are intentionally ignored.
    unsafe {
        if libc::killpg(pid as libc::pid_t, signal) != 0 {
            libc::kill(pid as libc::pid_t, signal);
        }
    }
}

#[cfg(not(unix))]
pub fn signal_group(_pid: u32, _signal: i32) {}

#[cfg(unix)]
pub const SIGTERM: i32 = libc::SIGTERM;
#[cfg(unix)]
pub const SIGKILL: i32 = libc::SIGKILL;
#[cfg(not(unix))]
pub const SIGTERM: i32 = 15;
#[cfg(not(unix))]
pub const SIGKILL: i32 = 9;

/// Exit code and signal of a finished child.
pub fn exit_parts(status: &std::process::ExitStatus) -> (Option<i32>, Option<i32>) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        (status.code(), status.signal())
    }
    #[cfg(not(unix))]
    {
        (status.code(), None)
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Read `reader` line by line and forward each line as a `Log` frame.
///
/// Memory is bounded twice: a line is cut at `max_line_bytes` (the rest is
/// discarded up to the newline) and the outgoing channel is bounded, so a
/// chatty user process blocks on its pipe instead of growing the bridge.
pub async fn forward_logs<R: AsyncRead + Unpin>(
    reader: R,
    stream: LogStream,
    max_line_bytes: usize,
    api: Arc<RuntimeApi>,
    out: mpsc::Sender<GuestMessage>,
) {
    let max_line_bytes = max_line_bytes.max(16);
    let mut reader = BufReader::with_capacity(8192, reader);
    let mut line: Vec<u8> = Vec::with_capacity(256);
    let mut truncated = false;
    while let Ok(buf) = reader.fill_buf().await {
        if buf.is_empty() {
            break;
        }
        let (chunk, newline) = match buf.iter().position(|b| *b == b'\n') {
            Some(i) => (&buf[..i], true),
            None => (buf, false),
        };
        let consumed = chunk.len() + usize::from(newline);
        if line.len() < max_line_bytes {
            let room = max_line_bytes - line.len();
            if chunk.len() > room {
                line.extend_from_slice(&chunk[..room]);
                truncated = true;
            } else {
                line.extend_from_slice(chunk);
            }
        } else if !chunk.is_empty() {
            truncated = true;
        }
        reader.consume(consumed);
        if newline {
            emit_line(&api, &out, stream, &mut line, truncated).await;
            truncated = false;
        }
    }
    if !line.is_empty() {
        emit_line(&api, &out, stream, &mut line, truncated).await;
    }
}

async fn emit_line(
    api: &RuntimeApi,
    out: &mpsc::Sender<GuestMessage>,
    stream: LogStream,
    line: &mut Vec<u8>,
    truncated: bool,
) {
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    let mut text = String::from_utf8_lossy(line).into_owned();
    if truncated {
        text.push_str(" [truncated]");
    }
    line.clear();
    let (phase, attempt_id) = api.log_context();
    // If the session is gone there is nobody to forward to; stop quietly.
    let _ = out
        .send(GuestMessage::Log {
            stream,
            phase,
            attempt_id,
            ts_ms: now_ms(),
            line: text,
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_api::ApiLimits;
    use std::time::Instant;

    async fn collect(input: &'static [u8], max: usize) -> Vec<String> {
        let (etx, _erx) = mpsc::channel(4);
        let api = RuntimeApi::new(
            etx,
            ApiLimits {
                max_response_bytes: 1024,
            },
            Instant::now(),
        );
        let (tx, mut rx) = mpsc::channel(64);
        forward_logs(input, LogStream::Stdout, max, api, tx).await;
        let mut lines = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let GuestMessage::Log { line, .. } = msg {
                lines.push(line);
            }
        }
        lines
    }

    #[tokio::test]
    async fn splits_lines_and_strips_cr() {
        let lines = collect(b"a\r\nb\nlast", 100).await;
        assert_eq!(lines, vec!["a", "b", "last"]);
    }

    #[tokio::test]
    async fn truncates_long_lines_without_buffering_them() {
        let lines = collect(b"0123456789012345678901234567890123456789\nok\n", 16).await;
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "0123456789012345 [truncated]");
        assert_eq!(lines[1], "ok");
    }

    #[tokio::test]
    async fn init_phase_before_ready() {
        let (etx, _erx) = mpsc::channel(4);
        let api = RuntimeApi::new(
            etx,
            ApiLimits {
                max_response_bytes: 1024,
            },
            Instant::now(),
        );
        let (tx, mut rx) = mpsc::channel(64);
        forward_logs(&b"boot\n"[..], LogStream::Stderr, 100, api, tx).await;
        match rx.try_recv().unwrap() {
            GuestMessage::Log {
                phase,
                stream,
                attempt_id,
                ..
            } => {
                assert_eq!(phase, tachyon_serverless_protocol::LogPhase::Init);
                assert_eq!(stream, LogStream::Stderr);
                assert!(attempt_id.is_none());
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
