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
//! Knobs for the end-to-end test: `fail_first: N` makes the first N
//! executions of an order fail with the retryable handler error
//! `Downstream.Unavailable`; `response_bytes: M` answers with an M-byte string
//! (over the gateway's response limit this is the non-retryable
//! `Host.ResponseTooLarge`). No network access.

use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tachyon_serverless_sdk::{Context, Event, HandlerError, SdkError};

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
    writeln!(f, "{line}").map_err(io)
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
    let dir = root();
    for sub in ["executions", "effects"] {
        std::fs::create_dir_all(dir.join(sub)).map_err(io)?;
    }
    let log = dir.join("executions.log");
    let execution = count_execution(&dir, &key)?;
    let fail_first = payload
        .get("fail_first")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if execution <= fail_first {
        append_line(
            &log,
            &format!("{key} {} {} failed", ctx.invocation_id, ctx.attempt_id),
        )?;
        return Err(HandlerError::with_type(
            "Downstream.Unavailable",
            format!("execution {execution} of {key} fails on purpose (fail_first = {fail_first})"),
        ));
    }
    if let Some(bytes) = payload.get("response_bytes").and_then(Value::as_u64)
        && bytes > 0
    {
        append_line(
            &log,
            &format!("{key} {} {} oversized", ctx.invocation_id, ctx.attempt_id),
        )?;
        return Ok(json!({ "order_id": key, "blob": "x".repeat(bytes as usize) }));
    }
    // The side effect, at most once per business key: an atomic create.
    let effect = dir.join("effects").join(format!("{key}.json"));
    let applied = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&effect)
    {
        Ok(mut f) => {
            let record = json!({
                "order_id": key,
                "invocation_id": ctx.invocation_id,
                "attempt_id": ctx.attempt_id,
            });
            writeln!(f, "{record}").map_err(io)?;
            true
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => false,
        Err(e) => return Err(io(e)),
    };
    append_line(
        &log,
        &format!(
            "{key} {} {} {}",
            ctx.invocation_id,
            ctx.attempt_id,
            if applied { "applied" } else { "skipped" }
        ),
    )?;
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
