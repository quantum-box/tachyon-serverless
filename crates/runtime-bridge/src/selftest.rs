//! `self-test-user`: an SDK-based user process baked into the bridge binary
//! so the bridge's own roundtrip tests need no other artifacts.
//!
//! Payload contract:
//! - default: `{"echo": <payload>, "environment_id": <env>}`
//! - `{"panic": true}` panics inside the handler
//! - `{"sleep_ms": N}` sleeps N ms before answering
//! - `{"fail": true}` returns a handler error `SelfTest.Failure`
//! - env `SELFTEST_FAIL_INIT=1` reports an init error before becoming ready
//!
//! The first stdout line is `selftest pid=<pid>` so tests can verify the
//! process is gone afterwards.

use serde_json::Value;
use tachyon_serverless_sdk::{Context, Event, HandlerError};

pub async fn run() -> i32 {
    println!("selftest pid={}", std::process::id());
    if std::env::var("SELFTEST_FAIL_INIT").as_deref() == Ok("1") {
        tachyon_serverless_sdk::init_error("SELFTEST_FAIL_INIT set");
    }
    let result = tachyon_serverless_sdk::run(|event: Event, ctx: Context| async move {
        let payload = event.into_json();
        if payload.get("panic").and_then(Value::as_bool) == Some(true) {
            panic!("self-test panic requested");
        }
        if let Some(ms) = payload.get("sleep_ms").and_then(Value::as_u64) {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
        if payload.get("fail").and_then(Value::as_bool) == Some(true) {
            return Err(HandlerError::with_type(
                "SelfTest.Failure",
                "fail requested",
            ));
        }
        eprintln!("selftest handled attempt {}", ctx.attempt_id);
        Ok(serde_json::json!({
            "echo": payload,
            "environment_id": ctx.environment_id,
        }))
    })
    .await;
    match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("selftest: {e}");
            1
        }
    }
}
