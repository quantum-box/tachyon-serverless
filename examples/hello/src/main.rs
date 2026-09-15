//! `hello`: the smallest Tachyon Serverless function.
//!
//! Payload `{"name": "..."}` answers
//! `{"message": "hello, <name|world>", "greeting": $GREETING | null,
//!   "secret_present": <DEMO_SECRET set?>, "unisolated": <bool>}`.
//!
//! Failure knobs for demos: `{"fail": true}` returns the handler error
//! `Demo.Failure`, `{"panic": true}` panics (reported as `Runtime.Panic`),
//! and `HELLO_FAIL_INIT=1` reports an init error before the runtime is ready.
//! No network access.

use serde_json::{Value, json};
use tachyon_serverless_sdk::{Context, Event, HandlerError, SdkError};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    if std::env::var("HELLO_FAIL_INIT").as_deref() == Ok("1") {
        tachyon_serverless_sdk::init_error("HELLO_FAIL_INIT set");
    }
    tachyon_serverless_sdk::run(handler).await
}

async fn handler(event: Event, ctx: Context) -> Result<Value, HandlerError> {
    let payload = event.json();
    if payload.get("fail").and_then(Value::as_bool) == Some(true) {
        return Err(HandlerError::with_type("Demo.Failure", "fail requested"));
    }
    if payload.get("panic").and_then(Value::as_bool) == Some(true) {
        panic!("panic requested");
    }
    let name = payload
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("world");
    let greeting = std::env::var("GREETING").ok();
    // Only report presence: secret values never leave the process.
    let secret_present = std::env::var_os("DEMO_SECRET").is_some();
    Ok(json!({
        "message": format!("hello, {name}"),
        "greeting": greeting,
        "secret_present": secret_present,
        "unisolated": ctx.is_unisolated(),
    }))
}
