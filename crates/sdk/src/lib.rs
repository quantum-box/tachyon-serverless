//! Tachyon Serverless Rust SDK.
//!
//! A function is a normal binary that calls [`run`] with a handler, or
//! [`serve_http`] with an axum [`Router`]. The SDK long-polls the in-guest
//! Runtime API served by the runtime bridge (`TACHYON_RUNTIME_API`), hands
//! each event to the handler and posts the result back
//! (`docs/protocol.md` section B).
//!
//! ```no_run
//! use tachyon_serverless_sdk::{Context, Event, HandlerError};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), tachyon_serverless_sdk::SdkError> {
//!     tachyon_serverless_sdk::run(|event: Event, ctx: Context| async move {
//!         let name = event.json()["name"].as_str().unwrap_or("world").to_string();
//!         Ok::<_, HandlerError>(serde_json::json!({ "message": format!("hello, {name}"), "attempt": ctx.attempt_id }))
//!     })
//!     .await
//! }
//! ```
//!
//! Error mapping (fixed by the protocol):
//! - handler `Err(HandlerError)` -> `error_type` from the error (default `Handler.Error`)
//! - handler panic -> `Runtime.Panic` (caught with `catch_unwind`)
//! - non-HTTP event delivered to [`serve_http`] -> `Runtime.UnsupportedEvent`
//!
//! `SIGTERM` is the host's cooperative cancel signal: the SDK records it and
//! exposes it through [`Context::is_cancelled`]; a process idle in the
//! long-poll exits cleanly.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tachyon_serverless_protocol::runtime_api::{
    self as api, HttpRequestEvent, MAX_ERROR_REPORT_BYTES, RuntimeErrorReport, event_types, headers,
};
use tokio::sync::watch;

pub mod client;
mod http;
#[cfg(feature = "experimental-restore")]
pub mod lifecycle;

pub use axum::Router;
pub use client::{HttpResponse, RuntimeClient};
pub use http::handle_http_event;
pub use tachyon_serverless_protocol::runtime_api::HttpResponsePayload;

/// Errors surfaced by the SDK itself (never by user handlers).
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("environment variable {0} is not set or invalid")]
    MissingEnv(&'static str),
    #[error("runtime api i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("runtime api protocol error: {0}")]
    Protocol(String),
    #[error("runtime api returned unexpected status {status} for {path}")]
    UnexpectedStatus { status: u16, path: String },
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported event type: {0}")]
    UnsupportedEvent(String),
    #[error("invalid http event: {0}")]
    InvalidHttpEvent(String),
    /// A lifecycle hook failed or timed out; already reported to the bridge
    /// (experimental, PLT-4651).
    #[cfg(feature = "experimental-restore")]
    #[error("{error_type}: {message}")]
    Lifecycle { error_type: String, message: String },
}

/// Error returned by a handler. `error_type` is a stable, machine-readable
/// identifier (`Handler.Error` by default); `message` is free text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerError {
    pub error_type: String,
    pub message: String,
}

impl HandlerError {
    pub const DEFAULT_TYPE: &'static str = "Handler.Error";

    pub fn new(message: impl Into<String>) -> Self {
        Self {
            error_type: Self::DEFAULT_TYPE.to_string(),
            message: message.into(),
        }
    }

    pub fn with_type(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}

impl std::error::Error for HandlerError {}

impl From<String> for HandlerError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}
impl From<&str> for HandlerError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}
impl From<serde_json::Error> for HandlerError {
    fn from(e: serde_json::Error) -> Self {
        Self::with_type("Handler.InvalidPayload", e.to_string())
    }
}
impl From<std::io::Error> for HandlerError {
    fn from(e: std::io::Error) -> Self {
        Self::new(e.to_string())
    }
}
impl From<Box<dyn std::error::Error + Send + Sync>> for HandlerError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::new(e.to_string())
    }
}

/// The event delivered to a handler: an arbitrary JSON payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    value: serde_json::Value,
}

impl Event {
    pub fn new(value: serde_json::Value) -> Self {
        Self { value }
    }

    /// Borrow the raw JSON payload.
    pub fn json(&self) -> &serde_json::Value {
        &self.value
    }

    /// Take the raw JSON payload.
    pub fn into_json(self) -> serde_json::Value {
        self.value
    }

    /// Deserialize the payload into a typed struct.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        T::deserialize(&self.value)
    }
}

impl From<serde_json::Value> for Event {
    fn from(value: serde_json::Value) -> Self {
        Self::new(value)
    }
}

/// Per-invocation context. All identifiers are host-assigned.
#[derive(Debug, Clone)]
pub struct Context {
    pub invocation_id: String,
    pub attempt_id: String,
    pub epoch: u64,
    /// Absolute deadline after which the host terminates the environment.
    pub deadline: SystemTime,
    pub trace_id: String,
    /// `tachyon.invoke.v1` or `tachyon.http.v1`.
    pub event_type: String,
    pub environment_id: String,
    cancelled: watch::Receiver<bool>,
}

impl Context {
    /// Time left before the host deadline (zero when already past).
    pub fn remaining_time(&self) -> Duration {
        self.deadline
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO)
    }

    /// True under the process provider (no isolation boundary).
    pub fn is_unisolated(&self) -> bool {
        is_unisolated()
    }

    /// True once the host asked for cooperative cancellation (`SIGTERM`).
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    /// Resolves once cancellation is requested.
    pub async fn cancelled(&self) {
        let mut rx = self.cancelled.clone();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            if rx.changed().await.is_err() {
                // Sender gone: cancellation can no longer be signalled.
                std::future::pending::<()>().await;
            }
        }
    }

    /// A context with placeholder identifiers, for unit-testing handlers.
    pub fn for_test() -> Self {
        // The sender is dropped immediately: `is_cancelled()` stays false and
        // `cancelled()` never resolves, which is what a test context wants.
        let (_tx, rx) = watch::channel(false);
        Self {
            invocation_id: "inv_00000000000000000000000000".into(),
            attempt_id: "att_00000000000000000000000000".into(),
            epoch: 1,
            deadline: SystemTime::now() + Duration::from_secs(30),
            trace_id: String::new(),
            event_type: event_types::JSON.into(),
            environment_id: "env_00000000000000000000000000".into(),
            cancelled: rx,
        }
    }
}

/// True when `TACHYON_UNISOLATED` is set (process provider).
pub fn is_unisolated() -> bool {
    std::env::var(tachyon_serverless_protocol::env::UNISOLATED)
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false)
}

/// Report an initialization failure to the bridge (best effort) and exit
/// with status 1. Call before [`run`] when startup preconditions fail.
pub fn init_error(message: impl Into<String>) -> ! {
    let message = message.into();
    eprintln!("tachyon init error: {message}");
    if let Ok(client) = RuntimeClient::from_env() {
        let report = RuntimeErrorReport {
            error_type: "Runtime.InitError".into(),
            message,
            stack_trace: None,
        };
        if let Ok(body) = encode_error_report(&report) {
            let _ = client.request_blocking(
                "POST",
                api::PATH_INIT_ERROR,
                Some(&body),
                Duration::from_secs(2),
            );
        }
    }
    std::process::exit(1)
}

/// Run a JSON event handler until the bridge shuts the runtime down.
///
/// The handler is invoked once per event. Returned values are serialized as
/// the response payload; errors and panics are reported as described in the
/// crate docs. Returns `Ok(())` when the Runtime API answers `410 Gone` or a
/// `SIGTERM` arrives while idle.
pub async fn run<F, Fut, T>(handler: F) -> Result<(), SdkError>
where
    F: Fn(Event, Context) -> Fut,
    Fut: Future<Output = Result<T, HandlerError>>,
    T: Serialize,
{
    Runtime::from_env()?.run(handler).await
}

/// Serve an axum [`Router`] for `tachyon.http.v1` events. Each event is
/// converted to an `http::Request`, dispatched to the router in-process
/// (no TCP listener) and the response is posted back as
/// [`HttpResponsePayload`].
pub async fn serve_http(router: Router) -> Result<(), SdkError> {
    Runtime::from_env()?.serve_http(router).await
}

/// Connection to one Runtime API. [`run`] / [`serve_http`] build it from the
/// environment; tests can point it at a mock server with [`Runtime::connect`].
#[derive(Debug, Clone)]
pub struct Runtime {
    client: RuntimeClient,
    environment_id: String,
    cancel: CancelSignal,
}

impl Runtime {
    /// Build from `TACHYON_RUNTIME_API` and `TACHYON_ENVIRONMENT_ID`.
    pub fn from_env() -> Result<Self, SdkError> {
        let client = RuntimeClient::from_env()?;
        let environment_id =
            std::env::var(tachyon_serverless_protocol::env::ENVIRONMENT_ID).unwrap_or_default();
        Ok(Self::with_client(client, environment_id))
    }

    /// Build for an explicit base URL (e.g. `http://127.0.0.1:9001`).
    pub fn connect(base_url: &str, environment_id: impl Into<String>) -> Result<Self, SdkError> {
        Ok(Self::with_client(
            RuntimeClient::new(base_url)?,
            environment_id.into(),
        ))
    }

    fn with_client(client: RuntimeClient, environment_id: String) -> Self {
        Self {
            client,
            environment_id,
            cancel: CancelSignal::new(),
        }
    }

    pub fn environment_id(&self) -> &str {
        &self.environment_id
    }

    /// See [`run`].
    pub async fn run<F, Fut, T>(&self, handler: F) -> Result<(), SdkError>
    where
        F: Fn(Event, Context) -> Fut,
        Fut: Future<Output = Result<T, HandlerError>>,
        T: Serialize,
    {
        self.run_loop(|event, ctx| {
            let fut = handler(event, ctx);
            async move {
                let value = fut.await?;
                serde_json::to_value(value)
                    .map_err(|e| HandlerError::with_type("Runtime.SerializeError", e.to_string()))
            }
        })
        .await
    }

    /// See [`serve_http`].
    pub async fn serve_http(&self, router: Router) -> Result<(), SdkError> {
        self.run_loop(|event, ctx| {
            let router = router.clone();
            async move {
                if ctx.event_type != event_types::HTTP {
                    return Err(HandlerError::with_type(
                        "Runtime.UnsupportedEvent",
                        format!(
                            "serve_http expects {} events, got {}",
                            event_types::HTTP,
                            ctx.event_type
                        ),
                    ));
                }
                let request: HttpRequestEvent = event.deserialize().map_err(|e| {
                    HandlerError::with_type(
                        "Runtime.InvalidHttpEvent",
                        format!("payload is not a tachyon.http.v1 request: {e}"),
                    )
                })?;
                let response = handle_http_event(&router, request)
                    .await
                    .map_err(|e| HandlerError::with_type("Runtime.HttpAdapter", e.to_string()))?;
                serde_json::to_value(response)
                    .map_err(|e| HandlerError::with_type("Runtime.SerializeError", e.to_string()))
            }
        })
        .await
    }

    /// Shared long-poll loop. `dispatch` produces the response JSON; panics
    /// inside it are caught and reported as `Runtime.Panic`.
    async fn run_loop<D, Fut>(&self, dispatch: D) -> Result<(), SdkError>
    where
        D: Fn(Event, Context) -> Fut,
        Fut: Future<Output = Result<serde_json::Value, HandlerError>>,
    {
        self.cancel.install();
        // Explicit ready: tightens the init measurement compared to relying on
        // the first poll. Best effort; the first `next` also implies readiness.
        let _ = self.client.post_json(api::PATH_READY, b"{}").await;

        loop {
            let response = tokio::select! {
                r = self.client.get(api::PATH_NEXT) => r?,
                _ = self.cancel.wait() => return Ok(()),
            };
            match response.status {
                200 => {}
                410 => return Ok(()),
                status => {
                    return Err(SdkError::UnexpectedStatus {
                        status,
                        path: api::PATH_NEXT.to_string(),
                    });
                }
            }
            let (event, ctx) = self.parse_next(&response)?;
            let attempt_id = ctx.attempt_id.clone();

            let outcome = AssertUnwindSafe(dispatch(event, ctx)).catch_unwind().await;
            match outcome {
                Ok(Ok(value)) => {
                    let body = serde_json::to_vec(&value)?;
                    self.post_expect_accepted(&api::path_response(&attempt_id), &body)
                        .await?;
                }
                Ok(Err(err)) => {
                    self.post_error(&attempt_id, err.error_type, err.message, None)
                        .await?;
                }
                Err(panic) => {
                    let message = panic_message(panic.as_ref());
                    self.post_error(&attempt_id, "Runtime.Panic".into(), message, None)
                        .await?;
                }
            }
            if self.cancel.is_cancelled() {
                return Ok(());
            }
        }
    }

    async fn post_error(
        &self,
        attempt_id: &str,
        error_type: String,
        message: String,
        stack_trace: Option<String>,
    ) -> Result<(), SdkError> {
        let report = RuntimeErrorReport {
            error_type,
            message,
            stack_trace,
        };
        let body = encode_error_report(&report)?;
        self.post_expect_accepted(&api::path_error(attempt_id), &body)
            .await
    }

    async fn post_expect_accepted(&self, path: &str, body: &[u8]) -> Result<(), SdkError> {
        let resp = self.client.post_json(path, body).await?;
        match resp.status {
            // 202 accepted; 404/409 mean the bridge already settled the
            // attempt (crash, timeout); 413 means the response or error report
            // was too large and the bridge settled the attempt with a
            // `*TooLarge` error: nothing more to do here.
            200..=299 | 404 | 409 | 413 => Ok(()),
            status => Err(SdkError::UnexpectedStatus {
                status,
                path: path.to_string(),
            }),
        }
    }

    fn parse_next(&self, response: &HttpResponse) -> Result<(Event, Context), SdkError> {
        let header = |name: &'static str| -> Result<String, SdkError> {
            response
                .header(name)
                .map(str::to_string)
                .ok_or_else(|| SdkError::Protocol(format!("missing header {name} on next")))
        };
        let epoch: u64 = header(headers::EPOCH)?
            .parse()
            .map_err(|_| SdkError::Protocol("invalid epoch header".into()))?;
        let deadline_ms: u64 = header(headers::DEADLINE_MS)?
            .parse()
            .map_err(|_| SdkError::Protocol("invalid deadline header".into()))?;
        let payload: serde_json::Value = if response.body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&response.body)?
        };
        let ctx = Context {
            invocation_id: header(headers::INVOCATION_ID)?,
            attempt_id: header(headers::ATTEMPT_ID)?,
            epoch,
            deadline: UNIX_EPOCH + Duration::from_millis(deadline_ms),
            trace_id: response.header(headers::TRACE_ID).unwrap_or("").to_string(),
            event_type: response
                .header(headers::EVENT_TYPE)
                .unwrap_or(event_types::JSON)
                .to_string(),
            environment_id: self.environment_id.clone(),
            cancelled: self.cancel.receiver(),
        };
        Ok((Event::new(payload), ctx))
    }
}

/// Process-wide cancellation flag driven by `SIGTERM`.
#[derive(Debug, Clone)]
struct CancelSignal {
    tx: Arc<watch::Sender<bool>>,
    rx: watch::Receiver<bool>,
    installed: Arc<std::sync::atomic::AtomicBool>,
}

impl CancelSignal {
    fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            tx: Arc::new(tx),
            rx,
            installed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Install the `SIGTERM` listener once. Must run inside a tokio runtime.
    fn install(&self) {
        use std::sync::atomic::Ordering;
        if self.installed.swap(true, Ordering::SeqCst) {
            return;
        }
        #[cfg(unix)]
        {
            let tx = self.tx.clone();
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sig) => {
                    tokio::spawn(async move {
                        if sig.recv().await.is_some() {
                            let _ = tx.send(true);
                        }
                    });
                }
                Err(e) => eprintln!("tachyon sdk: cannot install SIGTERM handler: {e}"),
            }
        }
    }

    fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    fn receiver(&self) -> watch::Receiver<bool> {
        self.rx.clone()
    }

    async fn wait(&self) {
        let mut rx = self.rx.clone();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Longest `error_type` the SDK reports, in bytes.
const MAX_REPORTED_ERROR_TYPE_BYTES: usize = 256;
/// Longest `message` the SDK reports, in bytes.
const MAX_REPORTED_MESSAGE_BYTES: usize = 64 * 1024;
/// Longest `stack_trace` the SDK reports, in bytes.
const MAX_REPORTED_STACK_TRACE_BYTES: usize = 256 * 1024;
/// `message` bound used, without a stack trace, when JSON escaping still
/// inflates the report past [`MAX_ERROR_REPORT_BYTES`] (a control character
/// escapes to six bytes).
const FALLBACK_REPORTED_MESSAGE_BYTES: usize = 16 * 1024;

/// `s` cut to at most `max` bytes on a char boundary, noting what was cut.
fn truncate_report_field(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let cut = s.floor_char_boundary(max);
    format!("{}...[truncated {} bytes]", &s[..cut], s.len() - cut)
}

/// Serialize an error report whose body always stays within
/// [`MAX_ERROR_REPORT_BYTES`], so the bridge never has to reject it. Long
/// fields keep their beginning; the stack trace is dropped only if escaping
/// makes the bounded report still too large.
fn encode_error_report(report: &RuntimeErrorReport) -> Result<Vec<u8>, SdkError> {
    let bounded = |message_max: usize, keep_stack_trace: bool| {
        serde_json::to_vec(&RuntimeErrorReport {
            error_type: truncate_report_field(&report.error_type, MAX_REPORTED_ERROR_TYPE_BYTES),
            message: truncate_report_field(&report.message, message_max),
            stack_trace: report
                .stack_trace
                .as_deref()
                .filter(|_| keep_stack_trace)
                .map(|st| truncate_report_field(st, MAX_REPORTED_STACK_TRACE_BYTES)),
        })
    };
    let body = bounded(MAX_REPORTED_MESSAGE_BYTES, true)?;
    if body.len() <= MAX_ERROR_REPORT_BYTES {
        return Ok(body);
    }
    // At most ~16 KiB of text escaped six times: always far below the bound.
    Ok(bounded(FALLBACK_REPORTED_MESSAGE_BYTES, false)?)
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "handler panicked".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use std::sync::Mutex;

    /// Mock Runtime API: serves the scripted events, then 410, and records
    /// every posted body.
    #[derive(Default)]
    struct Mock {
        events: Mutex<Vec<(String, serde_json::Value)>>,
        posted: Mutex<Vec<(String, Vec<u8>)>>,
        ready: Mutex<u32>,
    }

    async fn mock_next(State(m): State<Arc<Mock>>) -> axum::response::Response {
        let next = m.events.lock().unwrap().pop();
        match next {
            None => axum::http::StatusCode::GONE.into_response(),
            Some((attempt, payload)) => {
                let mut resp = axum::Json(payload).into_response();
                let h = resp.headers_mut();
                h.insert(headers::INVOCATION_ID, "inv_1".parse().unwrap());
                h.insert(headers::ATTEMPT_ID, attempt.parse().unwrap());
                h.insert(headers::EPOCH, "7".parse().unwrap());
                h.insert(headers::DEADLINE_MS, "4102444800000".parse().unwrap());
                h.insert(headers::TRACE_ID, "trace-x".parse().unwrap());
                h.insert(headers::EVENT_TYPE, event_types::JSON.parse().unwrap());
                resp
            }
        }
    }

    async fn mock_post(
        State(m): State<Arc<Mock>>,
        Path((attempt, kind)): Path<(String, String)>,
        body: bytes::Bytes,
    ) -> axum::http::StatusCode {
        m.posted
            .lock()
            .unwrap()
            .push((format!("{attempt}/{kind}"), body.to_vec()));
        axum::http::StatusCode::ACCEPTED
    }

    async fn mock_ready(State(m): State<Arc<Mock>>) -> axum::http::StatusCode {
        *m.ready.lock().unwrap() += 1;
        axum::http::StatusCode::ACCEPTED
    }

    async fn start_mock(events: Vec<(&str, serde_json::Value)>) -> (Arc<Mock>, String) {
        let mock = Arc::new(Mock::default());
        {
            let mut e = mock.events.lock().unwrap();
            for (a, p) in events.into_iter().rev() {
                e.push((a.to_string(), p));
            }
        }
        let router = Router::new()
            .route(api::PATH_NEXT, get(mock_next))
            .route("/runtime/v1/invocations/{attempt}/{kind}", post(mock_post))
            .route(api::PATH_READY, post(mock_ready))
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (mock, format!("http://{addr}"))
    }

    #[tokio::test]
    async fn run_serves_one_event_then_stops_on_410() {
        let (mock, url) = start_mock(vec![("att_1", serde_json::json!({"name": "tachyon"}))]).await;
        let rt = Runtime::connect(&url, "env_test").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        tokio::time::timeout(
            Duration::from_secs(10),
            rt.run(move |event: Event, ctx: Context| {
                let seen = seen2.clone();
                async move {
                    seen.lock().unwrap().push((
                        ctx.attempt_id.clone(),
                        ctx.epoch,
                        ctx.trace_id.clone(),
                    ));
                    assert_eq!(ctx.environment_id, "env_test");
                    assert!(ctx.remaining_time() > Duration::from_secs(1));
                    assert!(!ctx.is_cancelled());
                    let name = event.json()["name"].as_str().unwrap().to_string();
                    Ok::<_, HandlerError>(serde_json::json!({"hello": name}))
                }
            }),
        )
        .await
        .expect("run finished")
        .unwrap();

        assert_eq!(
            *seen.lock().unwrap(),
            vec![("att_1".to_string(), 7, "trace-x".to_string())]
        );
        assert_eq!(*mock.ready.lock().unwrap(), 1);
        let posted = mock.posted.lock().unwrap();
        assert_eq!(posted.len(), 1);
        assert_eq!(posted[0].0, "att_1/response");
        let v: serde_json::Value = serde_json::from_slice(&posted[0].1).unwrap();
        assert_eq!(v, serde_json::json!({"hello": "tachyon"}));
    }

    #[tokio::test]
    async fn handler_error_and_panic_are_reported() {
        let (mock, url) = start_mock(vec![
            ("att_err", serde_json::json!({"fail": true})),
            ("att_panic", serde_json::json!({"panic": true})),
        ])
        .await;
        let rt = Runtime::connect(&url, "env_test").unwrap();
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            rt.run(|event: Event, _ctx: Context| async move {
                if event.json()["panic"] == true {
                    panic!("boom {}", 42);
                }
                Err::<serde_json::Value, _>(HandlerError::with_type(
                    "Demo.Failure",
                    "asked to fail",
                ))
            }),
        )
        .await
        .expect("run finished");
        std::panic::set_hook(prev_hook);
        result.unwrap();

        let posted = mock.posted.lock().unwrap();
        assert_eq!(posted.len(), 2);
        assert_eq!(posted[0].0, "att_err/error");
        let r: RuntimeErrorReport = serde_json::from_slice(&posted[0].1).unwrap();
        assert_eq!(r.error_type, "Demo.Failure");
        assert_eq!(r.message, "asked to fail");
        assert_eq!(posted[1].0, "att_panic/error");
        let r: RuntimeErrorReport = serde_json::from_slice(&posted[1].1).unwrap();
        assert_eq!(r.error_type, "Runtime.Panic");
        assert_eq!(r.message, "boom 42");
    }

    #[tokio::test]
    async fn oversized_handler_error_is_truncated_before_posting() {
        let (mock, url) = start_mock(vec![("att_big", serde_json::json!({}))]).await;
        let rt = Runtime::connect(&url, "env_test").unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            rt.run(|_event: Event, _ctx: Context| async move {
                Err::<serde_json::Value, _>(HandlerError::new(format!(
                    "bad input: {}",
                    "p".repeat(2 * 1024 * 1024)
                )))
            }),
        )
        .await
        .expect("run finished")
        .unwrap();

        let posted = mock.posted.lock().unwrap();
        assert_eq!(posted.len(), 1, "the error report must reach the bridge");
        assert_eq!(posted[0].0, "att_big/error");
        assert!(posted[0].1.len() <= MAX_ERROR_REPORT_BYTES);
        let r: RuntimeErrorReport = serde_json::from_slice(&posted[0].1).unwrap();
        assert_eq!(r.error_type, HandlerError::DEFAULT_TYPE);
        assert!(r.message.starts_with("bad input: ppp"));
        assert!(r.message.ends_with("bytes]"), "truncation must be visible");
    }

    #[test]
    fn error_reports_are_bounded() {
        let parse = |body: &[u8]| -> RuntimeErrorReport {
            assert!(body.len() <= MAX_ERROR_REPORT_BYTES, "{} bytes", body.len());
            serde_json::from_slice(body).unwrap()
        };

        // Small reports are posted unchanged.
        let small = RuntimeErrorReport {
            error_type: "Demo.Failure".into(),
            message: "nope".into(),
            stack_trace: Some("at main".into()),
        };
        assert_eq!(parse(&encode_error_report(&small).unwrap()), small);

        // Long fields keep their beginning and say they were cut.
        let r = parse(
            &encode_error_report(&RuntimeErrorReport {
                error_type: "T".repeat(10_000),
                message: "x".repeat(2 * 1024 * 1024),
                stack_trace: Some("s".repeat(2 * 1024 * 1024)),
            })
            .unwrap(),
        );
        assert!(r.error_type.starts_with("TTT"));
        assert!(r.error_type.len() <= MAX_REPORTED_ERROR_TYPE_BYTES + 64);
        assert!(r.message.starts_with("xxx") && r.message.ends_with("bytes]"));
        assert!(r.message.len() <= MAX_REPORTED_MESSAGE_BYTES + 64);
        let st = r.stack_trace.expect("stack trace kept when it fits");
        assert!(st.len() <= MAX_REPORTED_STACK_TRACE_BYTES + 64);

        // Multi-byte text is cut on a char boundary (an odd offset here).
        let r = parse(
            &encode_error_report(&RuntimeErrorReport {
                error_type: "Handler.Error".into(),
                message: format!("a{}", "é".repeat(MAX_REPORTED_MESSAGE_BYTES)),
                stack_trace: None,
            })
            .unwrap(),
        );
        assert!(r.message.starts_with("aé"));

        // Escaping-heavy text still fits: the stack trace goes first.
        let r = parse(
            &encode_error_report(&RuntimeErrorReport {
                error_type: "\u{1}".repeat(10_000),
                message: "\u{1}".repeat(MAX_ERROR_REPORT_BYTES),
                stack_trace: Some("\u{1}".repeat(MAX_ERROR_REPORT_BYTES)),
            })
            .unwrap(),
        );
        assert!(r.stack_trace.is_none());
        assert!(r.message.starts_with('\u{1}'));
    }

    #[tokio::test]
    async fn serve_http_rejects_non_http_events() {
        let (mock, url) = start_mock(vec![("att_json", serde_json::json!({"x": 1}))]).await;
        let rt = Runtime::connect(&url, "env_test").unwrap();
        let router = Router::new().route("/", get(|| async { "ok" }));
        tokio::time::timeout(Duration::from_secs(10), rt.serve_http(router))
            .await
            .unwrap()
            .unwrap();
        let posted = mock.posted.lock().unwrap();
        assert_eq!(posted[0].0, "att_json/error");
        let r: RuntimeErrorReport = serde_json::from_slice(&posted[0].1).unwrap();
        assert_eq!(r.error_type, "Runtime.UnsupportedEvent");
    }

    #[test]
    fn event_helpers() {
        #[derive(serde::Deserialize, PartialEq, Debug)]
        struct P {
            name: String,
        }
        let e = Event::new(serde_json::json!({"name": "x"}));
        assert_eq!(e.deserialize::<P>().unwrap(), P { name: "x".into() });
        assert_eq!(e.json()["name"], "x");
        let err: HandlerError = e.deserialize::<Vec<u8>>().unwrap_err().into();
        assert_eq!(err.error_type, "Handler.InvalidPayload");
    }

    #[test]
    fn context_for_test_has_time_left() {
        let ctx = Context::for_test();
        assert!(ctx.remaining_time() > Duration::from_secs(1));
        assert!(!ctx.is_cancelled());
    }
}
