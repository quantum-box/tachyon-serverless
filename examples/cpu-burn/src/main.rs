//! `cpu-burn`: a CPU-bound function for timeout and cancellation demos.
//!
//! Payload `{"seconds": N (default 1), "ignore_sigterm": bool}` busy-loops
//! for N seconds of wall time doing integer arithmetic and answers
//! `{"burned_seconds": f64, "iterations": u64, "cancelled": bool}`.
//!
//! By default the loop watches `Context::is_cancelled()` and stops early
//! when the host sends `SIGTERM` (`cancelled: true`). With
//! `ignore_sigterm: true` the process ignores `SIGTERM` entirely, so the
//! host has to `SIGKILL` it after the grace period. No network access.

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tachyon_serverless_sdk::{Context, Event, HandlerError, SdkError};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    tachyon_serverless_sdk::run(handler).await
}

async fn handler(event: Event, ctx: Context) -> Result<Value, HandlerError> {
    let payload = event.json();
    let seconds = payload
        .get("seconds")
        .and_then(Value::as_f64)
        .unwrap_or(1.0)
        .max(0.0);
    let ignore_sigterm = payload
        .get("ignore_sigterm")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if ignore_sigterm {
        ignore_sigterm_signal();
    }
    // The loop is blocking; keep it off the async runtime threads.
    let report = tokio::task::spawn_blocking(move || {
        burn(Duration::from_secs_f64(seconds), &ctx, ignore_sigterm)
    })
    .await
    .map_err(|e| HandlerError::new(format!("burn task failed: {e}")))?;
    Ok(report)
}

fn burn(budget: Duration, ctx: &Context, ignore_cancel: bool) -> Value {
    let started = Instant::now();
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut iterations: u64 = 0;
    let mut cancelled = false;
    loop {
        // A short batch of arithmetic between clock checks.
        for _ in 0..100_000 {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            x ^= x >> 29;
        }
        iterations += 100_000;
        if started.elapsed() >= budget {
            break;
        }
        if !ignore_cancel && ctx.is_cancelled() {
            cancelled = true;
            break;
        }
    }
    std::hint::black_box(x);
    json!({
        "burned_seconds": started.elapsed().as_secs_f64(),
        "iterations": iterations,
        "cancelled": cancelled,
    })
}

/// Make the process ignore `SIGTERM` so only `SIGKILL` stops it.
#[cfg(unix)]
fn ignore_sigterm_signal() {
    // SAFETY: setting a signal disposition to SIG_IGN has no preconditions.
    unsafe {
        libc::signal(libc::SIGTERM, libc::SIG_IGN);
    }
}

#[cfg(not(unix))]
fn ignore_sigterm_signal() {}
