//! Runtime API served to the user process (docs/protocol.md section B).
//!
//! The state machine here is deliberately small: at most one attempt is in
//! flight per environment, the user process long-polls `GET /next`, and every
//! report is validated against the attempt the bridge handed out. Events that
//! matter to the host session are emitted on a channel; the session turns
//! them into wire frames.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use http_body_util::{BodyExt, Limited};
use tachyon_serverless_protocol::runtime_api::lifecycle::{
    self, Continuation, error_types as lifecycle_errors,
};
use tachyon_serverless_protocol::runtime_api::{
    self as api, MAX_ERROR_REPORT_BYTES, RuntimeErrorReport, headers,
};
use tachyon_serverless_protocol::{
    GuestErrorKind, LogPhase, MAX_FRAME_BYTES, MAX_RESPONSE_PAYLOAD_BYTES,
};
use tokio::sync::{Notify, OnceCell, mpsc};

/// Where the answer to `GET /runtime/v1/lifecycle/continue` comes from
/// (experimental lifecycle, PLT-4651).
///
/// The bridge never takes snapshots itself: a provider that can (PLT-4653)
/// supplies a source that resolves once the snapshot was taken and this copy
/// was restored. Every provider today uses [`NoSnapshot`], so a function that
/// opts into the lifecycle goes on at once with [`Continuation::Cold`].
pub trait RestoreSource: Send + Sync + 'static {
    fn continuation(&self) -> Pin<Box<dyn Future<Output = Continuation> + Send + '_>>;
}

/// No snapshot support: always [`Continuation::Cold`], immediately.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSnapshot;

impl RestoreSource for NoSnapshot {
    fn continuation(&self) -> Pin<Box<dyn Future<Output = Continuation> + Send + '_>> {
        Box::pin(std::future::ready(Continuation::Cold))
    }
}

/// Progress of the experimental lifecycle as the bridge has observed it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LifecyclePhase {
    /// The process never called a lifecycle path (P1 behaviour).
    #[default]
    NotUsed,
    /// `bootstrap` received; reusable state is being built.
    Bootstrapping,
    /// `checkpoint` received; `continue` not answered yet.
    AwaitingContinue {
        /// `continue` was requested (the wait is on the bridge/provider).
        requested: bool,
    },
    /// `continue` answered; per-instance state is being built until `ready`.
    AfterRestore { restored: bool },
}

/// Length of the canonical JSON encoding of `value`, i.e. what the
/// `Response` frame will carry, measured without allocating it. Numbers such
/// as `1e15` re-serialize longer than they were posted, so the raw body size
/// alone does not bound the frame.
fn canonical_json_len(value: &serde_json::Value) -> u64 {
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len() as u64;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => counter.0,
        Err(_) => u64::MAX,
    }
}

/// An invocation handed to the bridge by the host.
#[derive(Debug, Clone, PartialEq)]
pub struct InvokeRequest {
    pub invocation_id: String,
    pub attempt_id: String,
    pub epoch: u64,
    pub event_type: String,
    pub deadline_ms: u64,
    pub trace_id: String,
    pub payload: serde_json::Value,
}

/// Events the Runtime API raises towards the bridge session.
#[derive(Debug, Clone, PartialEq)]
pub enum ApiEvent {
    /// The user process is ready (first `/ready` or first `/next`).
    Ready { init_ms: u64 },
    /// The user process reported an initialization failure.
    InitError { error_type: String, message: String },
    /// `continue` was answered for the first time (experimental lifecycle).
    Continued { restored: bool },
    Response {
        attempt_id: String,
        epoch: u64,
        payload: serde_json::Value,
        handler_ms: Option<u64>,
    },
    Error {
        attempt_id: String,
        epoch: u64,
        error: GuestErrorKind,
        error_type: String,
        message: String,
        stack_trace: Option<String>,
        handler_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct ApiLimits {
    pub max_response_bytes: u64,
}

/// An invocation refused because another attempt is in flight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected {
    pub attempt_id: String,
    pub epoch: u64,
}

/// Outcome of a completion attempt for an attempt id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptLookup {
    InFlight,
    Completed,
    Unknown,
}

#[derive(Debug)]
struct InFlight {
    request: InvokeRequest,
    /// Set when the invocation was handed to the user process.
    delivered_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct Inner {
    ready: bool,
    shutdown: bool,
    in_flight: Option<InFlight>,
    completed: HashSet<String>,
    lifecycle: LifecyclePhase,
}

/// Shared Runtime API state. Cheap to clone via `Arc`.
pub struct RuntimeApi {
    inner: Mutex<Inner>,
    /// Wakes long-pollers when an invocation arrives or on shutdown.
    notify: Notify,
    events: mpsc::Sender<ApiEvent>,
    process_started_at: Instant,
    limits: ApiLimits,
    restore: Arc<dyn RestoreSource>,
    /// The single answer to `continue`, shared by retries.
    continuation: OnceCell<Continuation>,
}

impl std::fmt::Debug for RuntimeApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeApi")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl RuntimeApi {
    pub fn new(
        events: mpsc::Sender<ApiEvent>,
        limits: ApiLimits,
        process_started_at: Instant,
    ) -> Arc<Self> {
        Self::with_restore_source(events, limits, process_started_at, Arc::new(NoSnapshot))
    }

    /// Like [`Self::new`] with an explicit answer source for the experimental
    /// lifecycle's `continue` (tests use a mock restore notification).
    pub fn with_restore_source(
        events: mpsc::Sender<ApiEvent>,
        limits: ApiLimits,
        process_started_at: Instant,
        restore: Arc<dyn RestoreSource>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            notify: Notify::new(),
            events,
            process_started_at,
            limits,
            restore,
            continuation: OnceCell::new(),
        })
    }

    pub fn lifecycle_phase(&self) -> LifecyclePhase {
        self.lock().lifecycle.clone()
    }

    /// `(error_type, what was pending)` for an init deadline that passed now.
    /// Outside the experimental lifecycle this is the P1 `Runtime.InitTimeout`.
    pub fn init_timeout_classification(&self) -> (&'static str, &'static str) {
        match self.lock().lifecycle {
            LifecyclePhase::NotUsed => ("Runtime.InitTimeout", "ready"),
            LifecyclePhase::Bootstrapping
            | LifecyclePhase::AwaitingContinue { requested: false } => (
                lifecycle_errors::PRE_CHECKPOINT_TIMEOUT,
                "the bootstrap (pre-checkpoint) phase",
            ),
            LifecyclePhase::AwaitingContinue { requested: true } => (
                lifecycle_errors::CHECKPOINT_TIMEOUT,
                "the checkpoint / restore decision",
            ),
            LifecyclePhase::AfterRestore { .. } => (
                lifecycle_errors::AFTER_RESTORE_TIMEOUT,
                "the after-restore phase",
            ),
        }
    }

    /// Whether `ready` (explicit or implied by `next`) may be accepted now.
    /// Inside the experimental lifecycle only an explicit `ready` after
    /// `continue` counts; `next` never implies it.
    fn ready_allowed(&self, explicit: bool) -> bool {
        let g = self.lock();
        match g.lifecycle {
            LifecyclePhase::NotUsed => true,
            LifecyclePhase::AfterRestore { .. } => g.ready || explicit,
            LifecyclePhase::Bootstrapping | LifecyclePhase::AwaitingContinue { .. } => false,
        }
    }

    pub fn limits(&self) -> ApiLimits {
        self.limits
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Queue an invocation for the next `GET /next`. Fails when another
    /// attempt is already in flight (one execution per environment).
    pub fn dispatch(&self, request: InvokeRequest) -> Result<(), Rejected> {
        {
            let mut g = self.lock();
            if g.in_flight.is_some() {
                return Err(Rejected {
                    attempt_id: request.attempt_id,
                    epoch: request.epoch,
                });
            }
            g.in_flight = Some(InFlight {
                request,
                delivered_at: None,
            });
        }
        self.notify.notify_waiters();
        Ok(())
    }

    /// Mark the runtime ready. Returns `Some(init_ms)` the first time only.
    pub fn mark_ready(&self) -> Option<u64> {
        let mut g = self.lock();
        if g.ready {
            return None;
        }
        g.ready = true;
        Some(self.process_started_at.elapsed().as_millis() as u64)
    }

    pub fn is_ready(&self) -> bool {
        self.lock().ready
    }

    /// Answer every pending and future `GET /next` with `410 Gone`.
    pub fn shutdown(&self) {
        self.lock().shutdown = true;
        self.notify.notify_waiters();
    }

    pub fn is_shutdown(&self) -> bool {
        self.lock().shutdown
    }

    /// Attempt currently in flight, if any: `(attempt_id, epoch, handler_ms)`.
    pub fn in_flight(&self) -> Option<(String, u64, Option<u64>)> {
        let g = self.lock();
        g.in_flight.as_ref().map(|f| {
            (
                f.request.attempt_id.clone(),
                f.request.epoch,
                f.delivered_at.map(|t| t.elapsed().as_millis() as u64),
            )
        })
    }

    /// Remove the in-flight attempt (used when the user process dies).
    /// Returns `(attempt_id, epoch, handler_ms)`.
    pub fn take_in_flight(&self) -> Option<(String, u64, Option<u64>)> {
        let mut g = self.lock();
        let f = g.in_flight.take()?;
        g.completed.insert(f.request.attempt_id.clone());
        Some((
            f.request.attempt_id,
            f.request.epoch,
            f.delivered_at.map(|t| t.elapsed().as_millis() as u64),
        ))
    }

    /// Phase and attempt to stamp on user log lines right now.
    pub fn log_context(&self) -> (LogPhase, Option<String>) {
        let g = self.lock();
        if g.shutdown {
            return (
                LogPhase::Shutdown,
                g.in_flight.as_ref().map(|f| f.request.attempt_id.clone()),
            );
        }
        if !g.ready {
            return (LogPhase::Init, None);
        }
        (
            LogPhase::Handler,
            g.in_flight.as_ref().map(|f| f.request.attempt_id.clone()),
        )
    }

    pub fn lookup(&self, attempt_id: &str) -> AttemptLookup {
        let g = self.lock();
        if g.in_flight
            .as_ref()
            .is_some_and(|f| f.request.attempt_id == attempt_id)
        {
            AttemptLookup::InFlight
        } else if g.completed.contains(attempt_id) {
            AttemptLookup::Completed
        } else {
            AttemptLookup::Unknown
        }
    }

    /// Atomically complete the in-flight attempt if it matches.
    fn complete(&self, attempt_id: &str) -> Result<(u64, Option<u64>), AttemptLookup> {
        let mut g = self.lock();
        match g.in_flight.as_ref() {
            Some(f) if f.request.attempt_id == attempt_id => {
                let f = g.in_flight.take().expect("checked above");
                g.completed.insert(f.request.attempt_id);
                Ok((
                    f.request.epoch,
                    f.delivered_at.map(|t| t.elapsed().as_millis() as u64),
                ))
            }
            _ => {
                if g.completed.contains(attempt_id) {
                    Err(AttemptLookup::Completed)
                } else {
                    Err(AttemptLookup::Unknown)
                }
            }
        }
    }

    async fn emit(&self, event: ApiEvent) {
        // The session may already be gone (shutting down); dropping the event
        // is then the right thing to do.
        let _ = self.events.send(event).await;
    }

    async fn emit_ready_if_first(&self) {
        if let Some(init_ms) = self.mark_ready() {
            self.emit(ApiEvent::Ready { init_ms }).await;
        }
    }

    /// axum router serving the Runtime API on top of this state.
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route(api::PATH_NEXT, get(get_next))
            .route(
                "/runtime/v1/invocations/{attempt_id}/response",
                post(post_response),
            )
            .route(
                "/runtime/v1/invocations/{attempt_id}/error",
                post(post_error),
            )
            .route(api::PATH_INIT_ERROR, post(post_init_error))
            .route(api::PATH_READY, post(post_ready))
            .route(lifecycle::PATH_BOOTSTRAP, post(post_lifecycle_bootstrap))
            .route(lifecycle::PATH_CHECKPOINT, post(post_lifecycle_checkpoint))
            .route(lifecycle::PATH_CONTINUE, get(get_lifecycle_continue))
            .route(lifecycle::PATH_ERROR, post(post_lifecycle_error))
            .with_state(self.clone())
    }
}

type Api = State<Arc<RuntimeApi>>;

fn lifecycle_conflict(message: String) -> Response {
    (
        StatusCode::CONFLICT,
        format!("{}: {message}", lifecycle_errors::LIFECYCLE_VIOLATION),
    )
        .into_response()
}

async fn post_ready(State(api): Api) -> Response {
    if !api.ready_allowed(true) {
        return lifecycle_conflict(format!(
            "ready refused in lifecycle phase {:?}: continue has not been answered",
            api.lifecycle_phase()
        ));
    }
    api.emit_ready_if_first().await;
    StatusCode::ACCEPTED.into_response()
}

async fn post_lifecycle_bootstrap(State(api): Api) -> Response {
    {
        let mut g = api.lock();
        if g.ready || g.lifecycle != LifecyclePhase::NotUsed {
            let phase = g.lifecycle.clone();
            drop(g);
            return lifecycle_conflict(format!(
                "bootstrap must be the first lifecycle call before ready (phase {phase:?})"
            ));
        }
        g.lifecycle = LifecyclePhase::Bootstrapping;
    }
    StatusCode::ACCEPTED.into_response()
}

async fn post_lifecycle_checkpoint(State(api): Api) -> Response {
    {
        let mut g = api.lock();
        if g.lifecycle != LifecyclePhase::Bootstrapping {
            let phase = g.lifecycle.clone();
            drop(g);
            return lifecycle_conflict(format!(
                "checkpoint is only valid while bootstrapping (phase {phase:?})"
            ));
        }
        g.lifecycle = LifecyclePhase::AwaitingContinue { requested: false };
    }
    StatusCode::ACCEPTED.into_response()
}

async fn get_lifecycle_continue(State(api): Api) -> Response {
    {
        let mut g = api.lock();
        match g.lifecycle {
            LifecyclePhase::AwaitingContinue { .. } => {
                g.lifecycle = LifecyclePhase::AwaitingContinue { requested: true };
            }
            // A retry after the answer was given gets the same answer.
            LifecyclePhase::AfterRestore { .. } if !g.ready => {}
            ref phase => {
                let phase = phase.clone();
                drop(g);
                return lifecycle_conflict(format!(
                    "continue is only valid after checkpoint and before ready (phase {phase:?})"
                ));
            }
        }
    }
    let continuation = api
        .continuation
        .get_or_init(|| api.restore.continuation())
        .await
        .clone();
    let restored = matches!(continuation, Continuation::Restored { .. });
    let first = {
        let mut g = api.lock();
        match g.lifecycle {
            LifecyclePhase::AwaitingContinue { .. } => {
                g.lifecycle = LifecyclePhase::AfterRestore { restored };
                true
            }
            _ => false,
        }
    };
    if first {
        api.emit(ApiEvent::Continued { restored }).await;
    }
    let mut response = axum::Json(continuation).into_response();
    if let Ok(v) = HeaderValue::from_str(&lifecycle::VERSION.to_string()) {
        response.headers_mut().insert(lifecycle::HEADER_VERSION, v);
    }
    response
}

async fn post_lifecycle_error(State(api): Api, request: Request<Body>) -> Response {
    let report = match read_body(request, MAX_ERROR_REPORT_BYTES).await {
        BodyRead::Ok(bytes) => match serde_json::from_slice::<RuntimeErrorReport>(&bytes) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("error report is not valid: {e}"),
                )
                    .into_response();
            }
        },
        BodyRead::TooLarge { .. } => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "error report too large").into_response();
        }
        BodyRead::Failed(e) => {
            return (StatusCode::BAD_REQUEST, format!("cannot read body: {e}")).into_response();
        }
    };
    // The bridge, not the process, decides which phase failed.
    let error_type = {
        let g = api.lock();
        match g.lifecycle {
            LifecyclePhase::Bootstrapping | LifecyclePhase::AwaitingContinue { .. } => {
                lifecycle_errors::PRE_CHECKPOINT_FAILED
            }
            LifecyclePhase::AfterRestore { .. } if !g.ready => {
                lifecycle_errors::AFTER_RESTORE_FAILED
            }
            ref phase => {
                let phase = phase.clone();
                drop(g);
                return lifecycle_conflict(format!(
                    "lifecycle error outside an open lifecycle (phase {phase:?})"
                ));
            }
        }
    };
    api.emit(ApiEvent::InitError {
        error_type: error_type.to_string(),
        message: format!("{}: {}", report.error_type, report.message),
    })
    .await;
    StatusCode::ACCEPTED.into_response()
}

async fn get_next(State(api): Api) -> Response {
    if !api.ready_allowed(false) {
        return lifecycle_conflict(format!(
            "next refused in lifecycle phase {:?}: post ready after continue first",
            api.lifecycle_phase()
        ));
    }
    api.emit_ready_if_first().await;
    loop {
        // Register interest before inspecting state so a notification that
        // races with the check is not lost.
        let notified = api.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let delivered = {
            let mut g = api.lock();
            if g.shutdown {
                return StatusCode::GONE.into_response();
            }
            match g.in_flight.as_mut() {
                Some(f) if f.delivered_at.is_none() => {
                    f.delivered_at = Some(Instant::now());
                    Some(f.request.clone())
                }
                _ => None,
            }
        };
        if let Some(request) = delivered {
            return event_response(&request);
        }
        notified.await;
    }
}

fn event_response(request: &InvokeRequest) -> Response {
    let body = match serde_json::to_vec(&request.payload) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("payload not serializable: {e}"),
            )
                .into_response();
        }
    };
    let mut response = (StatusCode::OK, body).into_response();
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let set = |h: &mut axum::http::HeaderMap, name: &'static str, value: &str| {
        if let Ok(v) = HeaderValue::from_str(value) {
            h.insert(name, v);
        }
    };
    set(h, headers::INVOCATION_ID, &request.invocation_id);
    set(h, headers::ATTEMPT_ID, &request.attempt_id);
    set(h, headers::EPOCH, &request.epoch.to_string());
    set(h, headers::DEADLINE_MS, &request.deadline_ms.to_string());
    set(h, headers::TRACE_ID, &request.trace_id);
    set(h, headers::EVENT_TYPE, &request.event_type);
    response
}

fn lookup_status(l: AttemptLookup) -> StatusCode {
    match l {
        AttemptLookup::InFlight => StatusCode::ACCEPTED,
        AttemptLookup::Completed => StatusCode::CONFLICT,
        AttemptLookup::Unknown => StatusCode::NOT_FOUND,
    }
}

enum BodyRead {
    Ok(Vec<u8>),
    TooLarge { size_bytes: u64 },
    Failed(String),
}

/// Oversized bodies declaring at most this many bytes are read and discarded
/// before the 413 is sent. A client that writes its whole request before
/// reading (the SDK does) then receives the answer instead of a broken pipe,
/// and the user process keeps serving.
const DISCARD_LIMIT_BYTES: u64 = 2 * MAX_FRAME_BYTES as u64;
/// Upper bound on the time spent discarding an oversized body.
const DISCARD_TIMEOUT: Duration = Duration::from_secs(5);

/// Read a body bounded by `max` bytes without buffering more than that.
async fn read_body(request: Request<Body>, max: usize) -> BodyRead {
    if let Some(len) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        && len > max as u64
    {
        if len <= DISCARD_LIMIT_BYTES {
            discard_body(request.into_body()).await;
        }
        return BodyRead::TooLarge { size_bytes: len };
    }
    match Limited::new(request.into_body(), max).collect().await {
        Ok(collected) => BodyRead::Ok(collected.to_bytes().to_vec()),
        Err(e) => {
            if e.downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                BodyRead::TooLarge {
                    size_bytes: max as u64 + 1,
                }
            } else {
                BodyRead::Failed(e.to_string())
            }
        }
    }
}

/// Read and drop a body (bounded in size and time) without buffering it.
async fn discard_body(body: Body) {
    let mut body = Limited::new(body, DISCARD_LIMIT_BYTES as usize);
    let _ = tokio::time::timeout(DISCARD_TIMEOUT, async {
        while let Some(Ok(_)) = body.frame().await {}
    })
    .await;
}

async fn post_response(
    State(api): Api,
    Path(attempt_id): Path<String>,
    request: Request<Body>,
) -> Response {
    let pre = api.lookup(&attempt_id);
    if pre != AttemptLookup::InFlight {
        return lookup_status(pre).into_response();
    }
    // Never accept more than a single frame can carry, whatever the host
    // configured.
    let max = api
        .limits
        .max_response_bytes
        .min(MAX_RESPONSE_PAYLOAD_BYTES);
    let payload = match read_body(request, max as usize).await {
        BodyRead::Ok(bytes) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(v) => v,
            Err(e) => {
                let message = format!("response body is not valid JSON: {e}");
                return fail_attempt(
                    &api,
                    &attempt_id,
                    GuestErrorKind::Protocol,
                    "Runtime.InvalidResponse",
                    message,
                    StatusCode::BAD_REQUEST,
                )
                .await;
            }
        },
        BodyRead::TooLarge { size_bytes } => {
            let message =
                format!("response of {size_bytes} bytes exceeds the limit of {max} bytes");
            return fail_attempt(
                &api,
                &attempt_id,
                GuestErrorKind::ResponseTooLarge {
                    size_bytes,
                    max_bytes: max,
                },
                "Runtime.ResponseTooLarge",
                message,
                StatusCode::PAYLOAD_TOO_LARGE,
            )
            .await;
        }
        BodyRead::Failed(e) => {
            return (StatusCode::BAD_REQUEST, format!("cannot read body: {e}")).into_response();
        }
    };
    // Measure what will actually be framed (and what the host re-measures).
    let canonical_len = canonical_json_len(&payload);
    if canonical_len > max {
        let message = format!(
            "response of {canonical_len} bytes (canonical JSON) exceeds the limit of {max} bytes"
        );
        return fail_attempt(
            &api,
            &attempt_id,
            GuestErrorKind::ResponseTooLarge {
                size_bytes: canonical_len,
                max_bytes: max,
            },
            "Runtime.ResponseTooLarge",
            message,
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .await;
    }
    match api.complete(&attempt_id) {
        Ok((epoch, handler_ms)) => {
            api.emit(ApiEvent::Response {
                attempt_id,
                epoch,
                payload,
                handler_ms,
            })
            .await;
            StatusCode::ACCEPTED.into_response()
        }
        Err(l) => lookup_status(l).into_response(),
    }
}

/// Complete the attempt with an error frame and answer `status`.
async fn fail_attempt(
    api: &RuntimeApi,
    attempt_id: &str,
    error: GuestErrorKind,
    error_type: &str,
    message: String,
    status: StatusCode,
) -> Response {
    match api.complete(attempt_id) {
        Ok((epoch, handler_ms)) => {
            api.emit(ApiEvent::Error {
                attempt_id: attempt_id.to_string(),
                epoch,
                error,
                error_type: error_type.to_string(),
                message: message.clone(),
                stack_trace: None,
                handler_ms,
            })
            .await;
            (status, message).into_response()
        }
        Err(l) => lookup_status(l).into_response(),
    }
}

async fn post_error(
    State(api): Api,
    Path(attempt_id): Path<String>,
    request: Request<Body>,
) -> Response {
    let pre = api.lookup(&attempt_id);
    if pre != AttemptLookup::InFlight {
        return lookup_status(pre).into_response();
    }
    let report = match read_body(request, MAX_ERROR_REPORT_BYTES).await {
        BodyRead::Ok(bytes) => match serde_json::from_slice::<RuntimeErrorReport>(&bytes) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("error report is not valid: {e}"),
                )
                    .into_response();
            }
        },
        BodyRead::TooLarge { size_bytes } => {
            // The handler did fail; settle the attempt as a handler error so
            // it is neither left in flight (host timeout) nor lost.
            let message = format!(
                "error report of {size_bytes} bytes exceeds the limit of {MAX_ERROR_REPORT_BYTES} bytes"
            );
            return fail_attempt(
                &api,
                &attempt_id,
                GuestErrorKind::Handler,
                "Runtime.ErrorReportTooLarge",
                message,
                StatusCode::PAYLOAD_TOO_LARGE,
            )
            .await;
        }
        BodyRead::Failed(e) => {
            return (StatusCode::BAD_REQUEST, format!("cannot read body: {e}")).into_response();
        }
    };
    let error = if report.error_type == "Runtime.Panic" {
        GuestErrorKind::Panic
    } else {
        GuestErrorKind::Handler
    };
    match api.complete(&attempt_id) {
        Ok((epoch, handler_ms)) => {
            api.emit(ApiEvent::Error {
                attempt_id,
                epoch,
                error,
                error_type: report.error_type,
                message: report.message,
                stack_trace: report.stack_trace,
                handler_ms,
            })
            .await;
            StatusCode::ACCEPTED.into_response()
        }
        Err(l) => lookup_status(l).into_response(),
    }
}

async fn post_init_error(State(api): Api, request: Request<Body>) -> Response {
    let report = match read_body(request, MAX_ERROR_REPORT_BYTES).await {
        BodyRead::Ok(bytes) if bytes.is_empty() => RuntimeErrorReport {
            error_type: "Runtime.InitError".into(),
            message: "user process reported an initialization error".into(),
            stack_trace: None,
        },
        BodyRead::Ok(bytes) => match serde_json::from_slice::<RuntimeErrorReport>(&bytes) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("error report is not valid: {e}"),
                )
                    .into_response();
            }
        },
        BodyRead::TooLarge { .. } => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "error report too large").into_response();
        }
        BodyRead::Failed(e) => {
            return (StatusCode::BAD_REQUEST, format!("cannot read body: {e}")).into_response();
        }
    };
    api.emit(ApiEvent::InitError {
        error_type: report.error_type,
        message: report.message,
    })
    .await;
    StatusCode::ACCEPTED.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn setup(max_response_bytes: u64) -> (Arc<RuntimeApi>, mpsc::Receiver<ApiEvent>, Router) {
        let (tx, rx) = mpsc::channel(16);
        let api = RuntimeApi::new(tx, ApiLimits { max_response_bytes }, Instant::now());
        let router = api.router();
        (api, rx, router)
    }

    fn invoke(attempt: &str) -> InvokeRequest {
        InvokeRequest {
            invocation_id: "inv_1".into(),
            attempt_id: attempt.into(),
            epoch: 3,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 1234,
            trace_id: "tr".into(),
            payload: serde_json::json!({"k": "v"}),
        }
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn post(path: &str, body: &str) -> Request<Body> {
        // `oneshot` bypasses hyper, so set the length like a real client would.
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("content-length", body.len().to_string())
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn ready_is_emitted_once() {
        let (_api, mut rx, router) = setup(1024);
        for _ in 0..2 {
            let resp = router
                .clone()
                .oneshot(post(api::PATH_READY, ""))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
        }
        assert!(matches!(rx.recv().await, Some(ApiEvent::Ready { .. })));
        assert!(rx.try_recv().is_err(), "second ready must not emit");
    }

    #[tokio::test]
    async fn next_delivers_headers_and_payload_then_response_completes() {
        let (api, mut rx, router) = setup(1024);
        api.dispatch(invoke("att_1")).unwrap();
        let resp = router
            .clone()
            .oneshot(Request::get(api::PATH_NEXT).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let h = resp.headers();
        assert_eq!(h[headers::ATTEMPT_ID], "att_1");
        assert_eq!(h[headers::INVOCATION_ID], "inv_1");
        assert_eq!(h[headers::EPOCH], "3");
        assert_eq!(h[headers::DEADLINE_MS], "1234");
        assert_eq!(h[headers::TRACE_ID], "tr");
        assert_eq!(h[headers::EVENT_TYPE], "tachyon.invoke.v1");
        assert_eq!(body_json(resp).await, serde_json::json!({"k": "v"}));
        // First /next implies ready.
        assert!(matches!(rx.recv().await, Some(ApiEvent::Ready { .. })));
        assert_eq!(api.log_context(), (LogPhase::Handler, Some("att_1".into())));

        let resp = router
            .clone()
            .oneshot(post(&api::path_response("att_1"), r#"{"ok":true}"#))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        match rx.recv().await.unwrap() {
            ApiEvent::Response {
                attempt_id,
                epoch,
                payload,
                handler_ms,
            } => {
                assert_eq!(attempt_id, "att_1");
                assert_eq!(epoch, 3);
                assert_eq!(payload, serde_json::json!({"ok": true}));
                assert!(handler_ms.is_some());
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(api.log_context(), (LogPhase::Handler, None));

        // Completed attempt -> 409, unknown -> 404.
        let resp = router
            .clone()
            .oneshot(post(&api::path_response("att_1"), "{}"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let resp = router
            .clone()
            .oneshot(post(&api::path_response("att_nope"), "{}"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let resp = router
            .clone()
            .oneshot(post(&api::path_error("att_nope"), "{}"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn next_long_polls_until_dispatch() {
        let (api, _rx, router) = setup(1024);
        let r2 = router.clone();
        let poll = tokio::spawn(async move {
            r2.oneshot(Request::get(api::PATH_NEXT).body(Body::empty()).unwrap())
                .await
                .unwrap()
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !poll.is_finished(),
            "must block until an invocation arrives"
        );
        api.dispatch(invoke("att_2")).unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), poll)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[headers::ATTEMPT_ID], "att_2");
    }

    #[tokio::test]
    async fn second_dispatch_while_busy_is_rejected() {
        let (api, _rx, _router) = setup(1024);
        api.dispatch(invoke("att_1")).unwrap();
        let rejected = api.dispatch(invoke("att_2")).unwrap_err();
        assert_eq!(rejected.attempt_id, "att_2");
        assert_eq!(api.in_flight().map(|f| f.0), Some("att_1".into()));
    }

    #[tokio::test]
    async fn error_report_maps_panic_and_handler() {
        let (api, mut rx, router) = setup(1024);
        api.dispatch(invoke("att_1")).unwrap();
        let resp = router
            .clone()
            .oneshot(post(
                &api::path_error("att_1"),
                r#"{"error_type":"Runtime.Panic","message":"boom","stack_trace":"st"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        match rx.recv().await.unwrap() {
            ApiEvent::Error {
                error,
                error_type,
                message,
                stack_trace,
                ..
            } => {
                assert_eq!(error, GuestErrorKind::Panic);
                assert_eq!(error_type, "Runtime.Panic");
                assert_eq!(message, "boom");
                assert_eq!(stack_trace.as_deref(), Some("st"));
            }
            other => panic!("unexpected {other:?}"),
        }

        api.dispatch(invoke("att_2")).unwrap();
        let resp = router
            .clone()
            .oneshot(post(
                &api::path_error("att_2"),
                r#"{"error_type":"Demo.Failure","message":"nope"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            rx.recv().await.unwrap(),
            ApiEvent::Error {
                error: GuestErrorKind::Handler,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn oversized_response_is_413_and_error_frame() {
        let (api, mut rx, router) = setup(16);
        api.dispatch(invoke("att_1")).unwrap();
        let big = format!(r#"{{"x":"{}"}}"#, "y".repeat(64));
        let resp = router
            .clone()
            .oneshot(post(&api::path_response("att_1"), &big))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        match rx.recv().await.unwrap() {
            ApiEvent::Error { error, .. } => {
                assert_eq!(
                    error,
                    GuestErrorKind::ResponseTooLarge {
                        size_bytes: big.len() as u64,
                        max_bytes: 16
                    }
                );
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(api.in_flight().is_none());
        assert_eq!(api.lookup("att_1"), AttemptLookup::Completed);
    }

    #[tokio::test]
    async fn oversized_response_without_content_length_is_413() {
        let (api, mut rx, router) = setup(16);
        api.dispatch(invoke("att_1")).unwrap();
        let big = "z".repeat(64);
        let req = Request::builder()
            .method("POST")
            .uri(api::path_response("att_1"))
            .body(Body::from_stream(futures::stream::iter(vec![Ok::<
                _,
                std::io::Error,
            >(
                bytes::Bytes::from(big),
            )])))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(matches!(
            rx.recv().await.unwrap(),
            ApiEvent::Error {
                error: GuestErrorKind::ResponseTooLarge { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn response_is_measured_as_canonical_json() {
        // `1e15` is 4 bytes on the wire but re-serializes as
        // `1000000000000000.0`: the raw body fits, the framed payload does not.
        let (api, mut rx, router) = setup(4096);
        api.dispatch(invoke("att_1")).unwrap();
        let body = format!("[{}]", vec!["1e15"; 700].join(","));
        assert!(body.len() <= 4096);
        let canonical =
            serde_json::to_vec(&serde_json::from_str::<serde_json::Value>(&body).unwrap())
                .unwrap()
                .len() as u64;
        assert!(canonical > 4096);
        let resp = router
            .clone()
            .oneshot(post(&api::path_response("att_1"), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        match rx.try_recv().expect("error event") {
            ApiEvent::Error {
                error, error_type, ..
            } => {
                assert_eq!(
                    error,
                    GuestErrorKind::ResponseTooLarge {
                        size_bytes: canonical,
                        max_bytes: 4096
                    }
                );
                assert_eq!(error_type, "Runtime.ResponseTooLarge");
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(api.lookup("att_1"), AttemptLookup::Completed);
    }

    #[tokio::test]
    async fn response_limit_is_clamped_to_frame_capacity() {
        // A host limit above what one frame can carry must not let a payload
        // through that the session cannot send.
        let (api, mut rx, router) = setup(64 * 1024 * 1024);
        api.dispatch(invoke("att_1")).unwrap();
        let body = format!(
            "\"{}\"",
            "a".repeat(MAX_RESPONSE_PAYLOAD_BYTES as usize - 1)
        );
        let resp = router
            .clone()
            .oneshot(post(&api::path_response("att_1"), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        match rx.try_recv().expect("error event") {
            ApiEvent::Error { error, .. } => assert_eq!(
                error,
                GuestErrorKind::ResponseTooLarge {
                    size_bytes: MAX_RESPONSE_PAYLOAD_BYTES + 1,
                    max_bytes: MAX_RESPONSE_PAYLOAD_BYTES
                }
            ),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(api.lookup("att_1"), AttemptLookup::Completed);
    }

    #[tokio::test]
    async fn oversized_error_report_settles_the_attempt() {
        let (api, mut rx, router) = setup(1024);
        api.dispatch(invoke("att_1")).unwrap();
        let report = format!(
            r#"{{"error_type":"Handler.Error","message":"{}"}}"#,
            "m".repeat(MAX_ERROR_REPORT_BYTES)
        );
        let resp = router
            .clone()
            .oneshot(post(&api::path_error("att_1"), &report))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        match rx.try_recv().expect("the attempt must be completed") {
            ApiEvent::Error {
                attempt_id,
                epoch,
                error,
                error_type,
                message,
                ..
            } => {
                assert_eq!(attempt_id, "att_1");
                assert_eq!(epoch, 3);
                assert_eq!(error, GuestErrorKind::Handler);
                assert_eq!(error_type, "Runtime.ErrorReportTooLarge");
                assert!(message.contains(&report.len().to_string()), "{message}");
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(api.in_flight().is_none());
        assert_eq!(api.lookup("att_1"), AttemptLookup::Completed);
        // A late retry is told the attempt is already settled.
        let resp = router
            .clone()
            .oneshot(post(
                &api::path_error("att_1"),
                r#"{"error_type":"Handler.Error","message":"short"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn malformed_response_is_protocol_error() {
        let (api, mut rx, router) = setup(1024);
        api.dispatch(invoke("att_1")).unwrap();
        let resp = router
            .clone()
            .oneshot(post(&api::path_response("att_1"), "not json"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(matches!(
            rx.recv().await.unwrap(),
            ApiEvent::Error {
                error: GuestErrorKind::Protocol,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn init_error_is_forwarded() {
        let (_api, mut rx, router) = setup(1024);
        let resp = router
            .clone()
            .oneshot(post(
                api::PATH_INIT_ERROR,
                r#"{"error_type":"Runtime.InitError","message":"no config"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            rx.recv().await.unwrap(),
            ApiEvent::InitError {
                error_type: "Runtime.InitError".into(),
                message: "no config".into()
            }
        );
    }

    #[tokio::test]
    async fn shutdown_answers_next_with_410() {
        let (api, _rx, router) = setup(1024);
        let r2 = router.clone();
        let poll = tokio::spawn(async move {
            r2.oneshot(Request::get(api::PATH_NEXT).body(Body::empty()).unwrap())
                .await
                .unwrap()
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        api.shutdown();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), poll)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resp.status(), StatusCode::GONE);
        // Future polls also get 410.
        let resp = router
            .oneshot(Request::get(api::PATH_NEXT).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::GONE);
    }

    /// Mock restore notification: `continue` blocks until the test releases
    /// the continuation.
    struct MockRestore(tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<Continuation>>>);

    impl RestoreSource for MockRestore {
        fn continuation(&self) -> Pin<Box<dyn Future<Output = Continuation> + Send + '_>> {
            Box::pin(async move {
                let rx = self.0.lock().await.take().expect("asked once");
                rx.await.expect("test released the continuation")
            })
        }
    }

    fn get(path: &str) -> Request<Body> {
        Request::get(path).body(Body::empty()).unwrap()
    }

    async fn status(router: &Router, req: Request<Body>) -> StatusCode {
        router.clone().oneshot(req).await.unwrap().status()
    }

    /// PLT-4651: with no snapshot support the lifecycle answers `cold` at
    /// once, and `ready` is only accepted after `continue`.
    #[tokio::test]
    async fn lifecycle_cold_start_gates_ready_until_continue() {
        let (api, mut rx, router) = setup(1024);
        assert_eq!(
            status(&router, post(lifecycle::PATH_CHECKPOINT, "")).await,
            StatusCode::CONFLICT,
            "checkpoint before bootstrap"
        );
        assert_eq!(
            status(&router, post(lifecycle::PATH_BOOTSTRAP, "")).await,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            api.init_timeout_classification().0,
            lifecycle_errors::PRE_CHECKPOINT_TIMEOUT
        );
        assert_eq!(
            status(&router, post(api::PATH_READY, "")).await,
            StatusCode::CONFLICT,
            "ready while bootstrapping"
        );
        assert_eq!(
            status(&router, get(api::PATH_NEXT)).await,
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(&router, get(lifecycle::PATH_CONTINUE)).await,
            StatusCode::CONFLICT,
            "continue before checkpoint"
        );
        assert_eq!(
            status(&router, post(lifecycle::PATH_CHECKPOINT, "")).await,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            status(&router, post(api::PATH_READY, "")).await,
            StatusCode::CONFLICT,
            "ready before continue"
        );
        let resp = router
            .clone()
            .oneshot(get(lifecycle::PATH_CONTINUE))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[lifecycle::HEADER_VERSION], "1");
        assert_eq!(body_json(resp).await, serde_json::json!({"kind": "cold"}));
        assert_eq!(
            rx.try_recv().unwrap(),
            ApiEvent::Continued { restored: false }
        );
        assert!(rx.try_recv().is_err(), "nothing reached Ready yet");
        assert_eq!(
            api.init_timeout_classification().0,
            lifecycle_errors::AFTER_RESTORE_TIMEOUT
        );
        // `next` does not imply readiness inside the lifecycle.
        assert_eq!(
            status(&router, get(api::PATH_NEXT)).await,
            StatusCode::CONFLICT
        );
        assert!(!api.is_ready());
        assert_eq!(
            status(&router, post(api::PATH_READY, "")).await,
            StatusCode::ACCEPTED
        );
        assert!(matches!(rx.try_recv(), Ok(ApiEvent::Ready { .. })));
        assert!(api.is_ready());
        // Once ready, the lifecycle is closed.
        assert_eq!(
            status(&router, get(lifecycle::PATH_CONTINUE)).await,
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(
                &router,
                post(lifecycle::PATH_ERROR, r#"{"error_type":"x","message":"y"}"#)
            )
            .await,
            StatusCode::CONFLICT
        );
    }

    /// PLT-4651: a mock restore notification is what `continue` returns, and
    /// the wait for it is typed as the bridge's, not the function's.
    #[tokio::test]
    async fn lifecycle_continue_waits_for_the_restore_notification() {
        let (tx, mut rx) = mpsc::channel(16);
        let (release, pending) = tokio::sync::oneshot::channel();
        let api = RuntimeApi::with_restore_source(
            tx,
            ApiLimits {
                max_response_bytes: 1024,
            },
            Instant::now(),
            Arc::new(MockRestore(tokio::sync::Mutex::new(Some(pending)))),
        );
        let router = api.router();
        assert_eq!(
            status(&router, post(lifecycle::PATH_BOOTSTRAP, "")).await,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            status(&router, post(lifecycle::PATH_CHECKPOINT, "")).await,
            StatusCode::ACCEPTED
        );
        let r2 = router.clone();
        let poll = tokio::spawn(async move { r2.oneshot(get(lifecycle::PATH_CONTINUE)).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!poll.is_finished(), "continue blocks until the restore");
        assert_eq!(
            api.init_timeout_classification().0,
            lifecycle_errors::CHECKPOINT_TIMEOUT
        );
        let restored = Continuation::Restored {
            instance_id: "inst_mock".into(),
            restored_at_ms: 42,
            generation: 3,
        };
        release.send(restored.clone()).unwrap();
        let resp = timeout_ok(poll).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            body_json(resp).await,
            serde_json::to_value(&restored).unwrap()
        );
        assert_eq!(
            rx.recv().await,
            Some(ApiEvent::Continued { restored: true })
        );
        assert_eq!(
            api.lifecycle_phase(),
            LifecyclePhase::AfterRestore { restored: true }
        );
        // A retry gets the same answer and no second event.
        let resp = router
            .clone()
            .oneshot(get(lifecycle::PATH_CONTINUE))
            .await
            .unwrap();
        assert_eq!(
            body_json(resp).await,
            serde_json::to_value(&restored).unwrap()
        );
        assert!(rx.try_recv().is_err());
    }

    async fn timeout_ok(
        poll: tokio::task::JoinHandle<Result<Response, std::convert::Infallible>>,
    ) -> Response {
        tokio::time::timeout(Duration::from_secs(5), poll)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    /// PLT-4651: hook failures are typed by the phase the bridge observed,
    /// whatever the process called them.
    #[tokio::test]
    async fn lifecycle_errors_are_typed_by_phase() {
        let report = r#"{"error_type":"Handler.Error","message":"no table"}"#;
        let (_api, mut rx, router) = setup(1024);
        assert_eq!(
            status(&router, post(lifecycle::PATH_ERROR, report)).await,
            StatusCode::CONFLICT,
            "no lifecycle open"
        );
        status(&router, post(lifecycle::PATH_BOOTSTRAP, "")).await;
        assert_eq!(
            status(&router, post(lifecycle::PATH_ERROR, report)).await,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            ApiEvent::InitError {
                error_type: lifecycle_errors::PRE_CHECKPOINT_FAILED.into(),
                message: "Handler.Error: no table".into()
            }
        );

        let (_api, mut rx, router) = setup(1024);
        status(&router, post(lifecycle::PATH_BOOTSTRAP, "")).await;
        status(&router, post(lifecycle::PATH_CHECKPOINT, "")).await;
        status(&router, get(lifecycle::PATH_CONTINUE)).await;
        assert_eq!(
            rx.try_recv().unwrap(),
            ApiEvent::Continued { restored: false }
        );
        let wrong = r#"{"error_type":"Runtime.PreCheckpointFailed","message":"lying"}"#;
        assert_eq!(
            status(&router, post(lifecycle::PATH_ERROR, wrong)).await,
            StatusCode::ACCEPTED
        );
        match rx.try_recv().unwrap() {
            ApiEvent::InitError { error_type, .. } => {
                assert_eq!(error_type, lifecycle_errors::AFTER_RESTORE_FAILED)
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    /// PLT-4651: a P1 process that never touches the lifecycle is unaffected.
    #[tokio::test]
    async fn without_lifecycle_next_still_implies_ready() {
        let (api, mut rx, router) = setup(1024);
        assert_eq!(api.init_timeout_classification().0, "Runtime.InitTimeout");
        api.shutdown();
        assert_eq!(status(&router, get(api::PATH_NEXT)).await, StatusCode::GONE);
        assert!(matches!(rx.try_recv(), Ok(ApiEvent::Ready { .. })));
        assert_eq!(
            status(&router, post(lifecycle::PATH_BOOTSTRAP, "")).await,
            StatusCode::CONFLICT,
            "the lifecycle cannot be opened after ready"
        );
    }

    #[tokio::test]
    async fn take_in_flight_marks_completed() {
        let (api, _rx, _router) = setup(1024);
        assert_eq!(api.log_context(), (LogPhase::Init, None));
        api.dispatch(invoke("att_1")).unwrap();
        let (id, epoch, handler_ms) = api.take_in_flight().unwrap();
        assert_eq!((id.as_str(), epoch, handler_ms), ("att_1", 3, None));
        assert_eq!(api.lookup("att_1"), AttemptLookup::Completed);
        assert!(api.take_in_flight().is_none());
    }
}
