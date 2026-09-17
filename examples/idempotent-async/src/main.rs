//! `idempotent-async`: an asynchronous handler that is safe to execute more
//! than once (PLT-4640, docs/api.md「非同期 invoke の at-least-once 契約」).
//!
//! Asynchronous invocations are delivered and executed **at least once**: a
//! run can perform its external side effect and the gateway can die before
//! the result is committed, and the run is then executed again. The platform
//! never records two outcomes for one invocation, but it cannot undo what the
//! first run did outside of it. The handler therefore keys its side effect by
//! a **business idempotency key** (`order_id`) and applies it at most once:
//!
//! - the side effect is `effects/<order_id>.json`, created with `create_new`
//!   (an atomic "insert if absent"); a second execution finds it and reports
//!   `applied: false` instead of doing the work again;
//! - every execution, applied or not, is appended to `executions.log`
//!   (`<order_id> <invocation_id> <attempt_id> <applied|skipped|failed>`), so
//!   a test can count how often the handler really ran.
//!
//! A real function would use a database with a unique constraint on the key
//! (or a conditional write) instead of a local directory; the shape is the
//! same. The directory is `$IDEMPOTENT_ASYNC_DIR` (default
//! `/tmp/idempotent-async`); it only works where the function can reach the
//! host file system, i.e. with the dev-only process provider.
//!
//! In a microVM (Firecracker) nothing on the host file system is reachable and
//! the guest `/tmp` dies with the environment, so the store must live outside:
//! with `$IDEMPOTENT_ASYNC_URL=http://<ipv4>:<port>` the same three operations
//! go to a small HTTP store instead (`scripts/queue/effects-store.py`, reached
//! through egress `restricted`): `POST /count/<key>` (returns the execution
//! number), `POST /log` (appends one line) and `POST /effect/<key>` (201 when
//! created, 409 when it already existed). The store keeps the same files as
//! the directory mode, so the end-to-end checks read them unchanged.
//!
//! Knobs for the end-to-end test: `fail_first: N` makes the first N
//! executions of an order fail with the retryable handler error
//! `Downstream.Unavailable`; `response_bytes: M` answers with an M-byte string
//! (over the gateway's response limit this is the non-retryable
//! `Host.ResponseTooLarge`); `sleep_ms: T` (at most 60 000) waits before the
//! side effect, so a failure test can stop the gateway mid-run. No network
//! access unless `$IDEMPOTENT_ASYNC_URL` is set.

use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tachyon_serverless_sdk::{Context, Event, HandlerError, SdkError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    tachyon_serverless_sdk::run(handler).await
}

fn root() -> PathBuf {
    PathBuf::from(
        std::env::var("IDEMPOTENT_ASYNC_DIR").unwrap_or_else(|_| "/tmp/idempotent-async".into()),
    )
}

fn io(e: std::io::Error) -> HandlerError {
    HandlerError::with_type("Storage.Error", e.to_string())
}

/// `order_id` is used as a file name: only `[A-Za-z0-9_-]`, 1..=64 bytes.
fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn append_line(path: &Path, line: &str) -> Result<(), HandlerError> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(io)?;
    // One write(2) per line: `writeln!` issues several, and two runs appending
    // at once (at-least-once re-execution) interleaved their lines (PLT-4646).
    f.write_all(format!("{line}\n").as_bytes()).map_err(io)
}

/// How many times this order was executed, including this execution.
fn count_execution(dir: &Path, key: &str) -> Result<u64, HandlerError> {
    let path = dir.join("executions").join(format!("{key}.count"));
    let n = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
        + 1;
    std::fs::write(&path, n.to_string()).map_err(io)?;
    Ok(n)
}

/// Where the idempotency records live: a local directory (process provider) or
/// an HTTP store outside the environment (microVM providers).
enum Store {
    Dir(PathBuf),
    Http(String),
}

impl Store {
    fn from_env() -> Result<Self, HandlerError> {
        match std::env::var("IDEMPOTENT_ASYNC_URL") {
            Ok(url) if !url.is_empty() => {
                let addr = url
                    .strip_prefix("http://")
                    .map(|a| a.trim_end_matches('/'))
                    .filter(|a| !a.is_empty() && !a.contains('/'))
                    .ok_or_else(|| {
                        HandlerError::with_type(
                            "Storage.Config",
                            "IDEMPOTENT_ASYNC_URL must look like http://<host>:<port>",
                        )
                    })?;
                Ok(Self::Http(addr.to_string()))
            }
            _ => {
                let dir = root();
                for sub in ["executions", "effects"] {
                    std::fs::create_dir_all(dir.join(sub)).map_err(io)?;
                }
                Ok(Self::Dir(dir))
            }
        }
    }

    async fn count_execution(&self, key: &str) -> Result<u64, HandlerError> {
        match self {
            Self::Dir(dir) => count_execution(dir, key),
            Self::Http(addr) => {
                let (_, body) = http_post(addr, &format!("/count/{key}"), "").await?;
                body.trim().parse::<u64>().map_err(|_| {
                    HandlerError::with_type("Storage.Error", format!("bad count answer {body:?}"))
                })
            }
        }
    }

    async fn log(&self, line: &str) -> Result<(), HandlerError> {
        match self {
            Self::Dir(dir) => append_line(&dir.join("executions.log"), line),
            Self::Http(addr) => http_post(addr, "/log", line).await.map(|_| ()),
        }
    }

    /// Creates the side effect of `key` unless it exists: true when created.
    async fn create_effect(&self, key: &str, record: &Value) -> Result<bool, HandlerError> {
        match self {
            Self::Dir(dir) => {
                let effect = dir.join("effects").join(format!("{key}.json"));
                match OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&effect)
                {
                    Ok(mut f) => {
                        writeln!(f, "{record}").map_err(io)?;
                        Ok(true)
                    }
                    Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
                    Err(e) => Err(io(e)),
                }
            }
            Self::Http(addr) => {
                let (status, _) =
                    http_post(addr, &format!("/effect/{key}"), &record.to_string()).await?;
                Ok(status == 201)
            }
        }
    }
}

/// One `POST` over HTTP/1.1 with `Connection: close`: (status, body). Status
/// 200, 201 and 409 are answers; anything else is a storage error.
async fn http_post(addr: &str, path: &str, body: &str) -> Result<(u16, String), HandlerError> {
    let storage = |m: String| HandlerError::with_type("Storage.Error", m);
    let exchange = async {
        let mut s = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| storage(format!("connect {addr}: {e}")))?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: text/plain\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        s.write_all(request.as_bytes())
            .await
            .map_err(|e| storage(format!("write {addr}: {e}")))?;
        let mut raw = Vec::new();
        s.read_to_end(&mut raw)
            .await
            .map_err(|e| storage(format!("read {addr}: {e}")))?;
        let text = String::from_utf8_lossy(&raw);
        let (head, answer) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let status = head
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0);
        match status {
            200 | 201 | 409 => Ok((status, answer.to_string())),
            _ => Err(storage(format!("{path}: unexpected answer {head:?}"))),
        }
    };
    tokio::time::timeout(Duration::from_secs(5), exchange)
        .await
        .map_err(|_| storage(format!("{path}: store {addr} did not answer within 5 s")))?
}

async fn handler(event: Event, ctx: Context) -> Result<Value, HandlerError> {
    let payload = event.json();
    let key = payload
        .get("order_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if !valid_key(&key) {
        return Err(HandlerError::with_type(
            "Order.InvalidKey",
            "order_id must be 1..=64 bytes of [A-Za-z0-9_-]",
        ));
    }
    let store = Store::from_env()?;
    let execution = store.count_execution(&key).await?;
    let fail_first = payload
        .get("fail_first")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if execution <= fail_first {
        store
            .log(&format!(
                "{key} {} {} failed",
                ctx.invocation_id, ctx.attempt_id
            ))
            .await?;
        return Err(HandlerError::with_type(
            "Downstream.Unavailable",
            format!("execution {execution} of {key} fails on purpose (fail_first = {fail_first})"),
        ));
    }
    if let Some(bytes) = payload.get("response_bytes").and_then(Value::as_u64)
        && bytes > 0
    {
        store
            .log(&format!(
                "{key} {} {} oversized",
                ctx.invocation_id, ctx.attempt_id
            ))
            .await?;
        return Ok(json!({ "order_id": key, "blob": "x".repeat(bytes as usize) }));
    }
    // `sleep_ms` keeps the run in flight before its side effect, so a failure
    // test (PLT-4646) can stop or kill the gateway while the handler runs.
    if let Some(ms) = payload.get("sleep_ms").and_then(Value::as_u64)
        && ms > 0
    {
        tokio::time::sleep(Duration::from_millis(ms.min(60_000))).await;
    }
    // The side effect, at most once per business key: an atomic create.
    let record = json!({
        "order_id": key,
        "invocation_id": ctx.invocation_id,
        "attempt_id": ctx.attempt_id,
    });
    let applied = store.create_effect(&key, &record).await?;
    store
        .log(&format!(
            "{key} {} {} {}",
            ctx.invocation_id,
            ctx.attempt_id,
            if applied { "applied" } else { "skipped" }
        ))
        .await?;
    println!("idempotent-async: order={key} execution={execution} applied={applied}");
    Ok(json!({ "order_id": key, "applied": applied, "execution": execution }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_safe_file_names() {
        assert!(valid_key("order-42_a"));
        for bad in ["", "../x", "a/b", "a b", &"x".repeat(65)] {
            assert!(!valid_key(bad), "{bad:?}");
        }
    }
}
