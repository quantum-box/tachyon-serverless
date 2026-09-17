//! Experimental restore-aware lifecycle (PLT-4651, X1). Requires the
//! `experimental-restore` cargo feature.
//!
//! It separates the state a function can build **once and reuse across
//! copies** of a snapshot from the state that must be **rebuilt for every
//! instance**:
//!
//! ```text
//! main (no #[tokio::main])
//!   POST lifecycle/bootstrap
//!   bootstrap()            synchronous. No async runtime, no secrets, no sockets,
//!                          no threads, no clock/identity/RNG that must be unique.
//!   POST lifecycle/checkpoint      <- a snapshot may be taken from here on
//!   GET  lifecycle/continue        -> {"kind":"cold"} | {"kind":"restored", ...}
//!   read the clock, build the Tokio runtime
//!   after_restore(fixed, ctx)      identity, RNG reseed, credentials, connections,
//!                                  background tasks
//!   POST ready                     only now can the bridge report Ready
//!   invocation loop (same as `run` / `serve_http`)
//! ```
//!
//! With no snapshot support (every provider today) the bridge answers
//! `cold` immediately, so a normal start goes through exactly the same hooks.
//!
//! ```no_run
//! # #[cfg(feature = "experimental-restore")]
//! # fn main() -> Result<(), tachyon_serverless_sdk::SdkError> {
//! use tachyon_serverless_sdk::{Context, Event, HandlerError, lifecycle};
//!
//! lifecycle::builder()
//!     .bootstrap(|| Ok::<_, HandlerError>(vec![1u32, 2, 3]))
//!     .after_restore(|table, ctx| async move {
//!         Ok::<_, HandlerError>((table, ctx.instance_id))
//!     })
//!     .run(|state, _event: Event, _ctx: Context| async move {
//!         Ok::<_, HandlerError>(serde_json::json!({ "instance": state.1, "n": state.0.len() }))
//!     })
//! # }
//! # #[cfg(not(feature = "experimental-restore"))]
//! # fn main() {}
//! ```
//!
//! # What this does **not** do
//!
//! It does not make arbitrary code snapshot-safe. Libraries that cache a
//! random seed, a hostname, a monotonic clock base, file descriptors, thread
//! pools or TLS sessions during `bootstrap` will carry those into every
//! restored copy; a multithreaded runtime started before the checkpoint would
//! be copied mid-flight. The SDK only guarantees that **it** starts no async
//! runtime, thread, signal handler or connection before the checkpoint and
//! that the bridge never reports Ready before `after_restore` succeeded. What
//! the hooks themselves touch is the function author's responsibility.
//!
//! Environment variables are part of the process image: in a restored copy
//! `std::env` holds the values from the snapshot's process. Fresh credentials
//! for a restored copy need a delivery channel that does not exist yet
//! (PLT-4653); today only the cold path runs, where the environment is current.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use serde::Serialize;
use tachyon_serverless_protocol::runtime_api::lifecycle::{
    self as wire, Continuation, error_types,
};
use tachyon_serverless_protocol::runtime_api::{self as api, RuntimeErrorReport};

use crate::{
    Context, Event, HandlerError, Router, Runtime, RuntimeClient, SdkError, encode_error_report,
    panic_message,
};

/// Bound on connecting to / writing to the bridge, and on reading the
/// answers of the short lifecycle calls.
const CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// Attempts for `continue` when the connection fails (a connection that
/// crossed a restore may be reset); the bridge answers retries identically.
const CONTINUE_ATTEMPTS: u32 = 5;

/// What the process learns after the checkpoint point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreContext {
    /// `true` when this process was resumed from a snapshot; `false` for a
    /// normal (cold) start.
    pub restored: bool,
    /// Identity of this instance. The bridge-assigned restore id for a
    /// restored copy; the environment id for a cold start.
    pub instance_id: String,
    /// Number of restores of the snapshot (0 for a cold start).
    pub generation: u64,
    /// Guest wall clock of the restore announcement (restored copies only).
    pub restored_at: Option<SystemTime>,
    /// Wall clock read by the SDK right after `continue`, i.e. after any
    /// restore. Never a value captured before the checkpoint.
    pub started_at: SystemTime,
    pub environment_id: String,
}

/// Which Tokio runtime the SDK builds **after** the checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeFlavor {
    /// Single-threaded (default).
    #[default]
    CurrentThread,
    /// Multi-threaded. Built after `continue`, so no worker thread exists at
    /// the checkpoint; this is not a claim that such a runtime could be
    /// snapshotted while running.
    MultiThread,
}

/// Start a lifecycle description. See the module docs.
pub fn builder() -> Builder {
    Builder::default()
}

/// Lifecycle settings before the bootstrap hook is given.
#[derive(Debug, Clone, Default)]
pub struct Builder {
    runtime_api: Option<String>,
    environment_id: Option<String>,
    flavor: RuntimeFlavor,
}

impl Builder {
    /// Use this Runtime API base URL instead of `TACHYON_RUNTIME_API`.
    pub fn runtime_api(mut self, base_url: impl Into<String>) -> Self {
        self.runtime_api = Some(base_url.into());
        self
    }

    /// Use this environment id instead of `TACHYON_ENVIRONMENT_ID`.
    pub fn environment_id(mut self, id: impl Into<String>) -> Self {
        self.environment_id = Some(id.into());
        self
    }

    pub fn runtime_flavor(mut self, flavor: RuntimeFlavor) -> Self {
        self.flavor = flavor;
        self
    }

    /// Synchronous hook that builds reusable, instance-independent state.
    /// It runs with no async runtime; do not read secrets, open sockets or
    /// files you keep, spawn threads, or derive identity/randomness here.
    pub fn bootstrap<F, B>(self, bootstrap: F) -> WithBootstrap<F>
    where
        F: FnOnce() -> Result<B, HandlerError>,
    {
        WithBootstrap {
            base: self,
            bootstrap,
        }
    }
}

/// A lifecycle with its bootstrap hook.
pub struct WithBootstrap<F> {
    base: Builder,
    bootstrap: F,
}

impl<F> WithBootstrap<F> {
    /// Hook that turns the fixed state into per-instance state. Runs inside
    /// the Tokio runtime the SDK builds after `continue`, once per process
    /// (cold start or restored copy). Readiness is reported only after it
    /// returns `Ok`.
    pub fn after_restore<B, G, Fut, S>(self, after_restore: G) -> Lifecycle<F, G>
    where
        F: FnOnce() -> Result<B, HandlerError>,
        G: FnOnce(B, RestoreContext) -> Fut,
        Fut: Future<Output = Result<S, HandlerError>>,
    {
        Lifecycle {
            base: self.base,
            bootstrap: self.bootstrap,
            after_restore,
        }
    }
}

/// A complete lifecycle, ready to serve.
pub struct Lifecycle<F, G> {
    base: Builder,
    bootstrap: F,
    after_restore: G,
}

impl<F, G, B, Fut, S> Lifecycle<F, G>
where
    F: FnOnce() -> Result<B, HandlerError>,
    G: FnOnce(B, RestoreContext) -> Fut,
    Fut: Future<Output = Result<S, HandlerError>>,
    S: Send + Sync + 'static,
{
    /// Run the lifecycle, then serve JSON events like [`crate::run`], with the
    /// per-instance state passed to every call. Blocks the calling thread;
    /// call it from a plain `fn main`, not from inside a Tokio runtime.
    pub fn run<H, HFut, T>(self, handler: H) -> Result<(), SdkError>
    where
        H: Fn(Arc<S>, Event, Context) -> HFut,
        HFut: Future<Output = Result<T, HandlerError>>,
        T: Serialize,
    {
        let (rt, runtime, state) = self.start()?;
        let state = Arc::new(state);
        rt.block_on(runtime.run(move |event, ctx| handler(state.clone(), event, ctx)))
    }

    /// Run the lifecycle, then serve `tachyon.http.v1` events like
    /// [`crate::serve_http`] with the router built from the per-instance
    /// state.
    pub fn serve_http<M>(self, make_router: M) -> Result<(), SdkError>
    where
        M: FnOnce(S) -> Router,
    {
        let (rt, runtime, state) = self.start()?;
        let router = make_router(state);
        rt.block_on(runtime.serve_http(router))
    }

    /// Everything up to and including `ready`.
    fn start(self) -> Result<(tokio::runtime::Runtime, Runtime, S), SdkError> {
        let Lifecycle {
            base,
            bootstrap,
            after_restore,
        } = self;
        let client = match &base.runtime_api {
            Some(url) => RuntimeClient::new(url)?,
            None => RuntimeClient::from_env()?,
        };
        let environment_id = base.environment_id.clone().unwrap_or_else(|| {
            std::env::var(tachyon_serverless_protocol::env::ENVIRONMENT_ID).unwrap_or_default()
        });

        // --- before the checkpoint: blocking calls only -------------------
        expect_accepted(
            post_blocking(&client, wire::PATH_BOOTSTRAP)?,
            wire::PATH_BOOTSTRAP,
        )?;
        let fixed = match std::panic::catch_unwind(AssertUnwindSafe(bootstrap)) {
            Ok(Ok(fixed)) => fixed,
            Ok(Err(e)) => {
                return Err(report_blocking(
                    &client,
                    error_types::PRE_CHECKPOINT_FAILED,
                    e,
                ));
            }
            Err(panic) => {
                let e = HandlerError::with_type("Runtime.Panic", panic_message(panic.as_ref()));
                return Err(report_blocking(
                    &client,
                    error_types::PRE_CHECKPOINT_FAILED,
                    e,
                ));
            }
        };
        expect_accepted(
            post_blocking(&client, wire::PATH_CHECKPOINT)?,
            wire::PATH_CHECKPOINT,
        )?;
        let continuation = wait_continue(&client)?;

        // --- after the checkpoint ------------------------------------------
        let started_at = SystemTime::now();
        let ctx = match continuation {
            Continuation::Cold => RestoreContext {
                restored: false,
                instance_id: environment_id.clone(),
                generation: 0,
                restored_at: None,
                started_at,
                environment_id: environment_id.clone(),
            },
            Continuation::Restored {
                instance_id,
                restored_at_ms,
                generation,
            } => RestoreContext {
                restored: true,
                instance_id,
                generation,
                restored_at: Some(UNIX_EPOCH + Duration::from_millis(restored_at_ms)),
                started_at,
                environment_id: environment_id.clone(),
            },
        };
        let rt = match base.flavor {
            RuntimeFlavor::CurrentThread => tokio::runtime::Builder::new_current_thread(),
            RuntimeFlavor::MultiThread => tokio::runtime::Builder::new_multi_thread(),
        }
        .enable_all()
        .build()?;
        let runtime = Runtime::with_client(client.clone(), environment_id);
        let state = rt.block_on(async {
            let outcome =
                match std::panic::catch_unwind(AssertUnwindSafe(|| after_restore(fixed, ctx))) {
                    Ok(fut) => AssertUnwindSafe(fut).catch_unwind().await,
                    Err(panic) => Err(panic),
                };
            let error = match outcome {
                Ok(Ok(state)) => return Ok(state),
                Ok(Err(e)) => e,
                Err(panic) => {
                    HandlerError::with_type("Runtime.Panic", panic_message(panic.as_ref()))
                }
            };
            Err(report_async(&client, error_types::AFTER_RESTORE_FAILED, error).await)
        })?;
        let resp = rt.block_on(client.post_json(api::PATH_READY, b"{}"))?;
        expect_accepted(resp, api::PATH_READY)?;
        Ok((rt, runtime, state))
    }
}

fn post_blocking(client: &RuntimeClient, path: &str) -> Result<crate::HttpResponse, SdkError> {
    client.request_blocking("POST", path, Some(b"{}"), CALL_TIMEOUT)
}

fn expect_accepted(resp: crate::HttpResponse, path: &str) -> Result<(), SdkError> {
    if (200..300).contains(&resp.status) {
        Ok(())
    } else {
        Err(SdkError::UnexpectedStatus {
            status: resp.status,
            path: path.to_string(),
        })
    }
}

/// Block (without any async runtime) until the bridge says how to continue.
fn wait_continue(client: &RuntimeClient) -> Result<Continuation, SdkError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match client.request_blocking_with("GET", wire::PATH_CONTINUE, None, CALL_TIMEOUT, None) {
            Ok(resp) => {
                expect_accepted(resp.clone(), wire::PATH_CONTINUE)?;
                return Ok(serde_json::from_slice(&resp.body)?);
            }
            Err(SdkError::Io(e)) if attempt < CONTINUE_ATTEMPTS => {
                eprintln!("tachyon sdk: lifecycle continue failed ({e}); retrying");
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => return Err(e),
        }
    }
}

fn failure(phase_type: &str, error: &HandlerError) -> (Vec<u8>, SdkError) {
    let report = RuntimeErrorReport {
        error_type: error.error_type.clone(),
        message: error.message.clone(),
        stack_trace: None,
    };
    let body = encode_error_report(&report).unwrap_or_default();
    // The bridge types the phase itself; the SDK's view matches it.
    let err = SdkError::Lifecycle {
        error_type: phase_type.to_string(),
        message: format!("{}: {}", error.error_type, error.message),
    };
    (body, err)
}

fn report_blocking(client: &RuntimeClient, phase_type: &str, error: HandlerError) -> SdkError {
    eprintln!("tachyon lifecycle: {phase_type}: {error}");
    let (body, err) = failure(phase_type, &error);
    let _ = client.request_blocking("POST", wire::PATH_ERROR, Some(&body), CALL_TIMEOUT);
    err
}

async fn report_async(client: &RuntimeClient, phase_type: &str, error: HandlerError) -> SdkError {
    eprintln!("tachyon lifecycle: {phase_type}: {error}");
    let (body, err) = failure(phase_type, &error);
    let _ = tokio::time::timeout(CALL_TIMEOUT, client.post_json(wire::PATH_ERROR, &body)).await;
    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use std::sync::Mutex;
    use tachyon_serverless_protocol::runtime_api::headers;

    /// Mock Runtime API with the lifecycle paths. `continue` blocks until the
    /// test sends the mock restore notification.
    struct Mock {
        log: Arc<Mutex<Vec<String>>>,
        release: Mutex<Option<std::sync::mpsc::Receiver<Continuation>>>,
        events: Mutex<Vec<serde_json::Value>>,
        posted: Mutex<Vec<(String, Vec<u8>)>>,
    }

    fn push(m: &Mock, s: &str) {
        m.log.lock().unwrap().push(s.to_string());
    }

    async fn bootstrap(State(m): State<Arc<Mock>>) -> StatusCode {
        push(&m, "POST bootstrap");
        StatusCode::ACCEPTED
    }
    async fn checkpoint(State(m): State<Arc<Mock>>) -> StatusCode {
        push(&m, "POST checkpoint");
        StatusCode::ACCEPTED
    }
    async fn cont(State(m): State<Arc<Mock>>) -> axum::response::Response {
        push(&m, "GET continue");
        let rx = m
            .release
            .lock()
            .unwrap()
            .take()
            .expect("continue asked once");
        let c = tokio::task::spawn_blocking(move || rx.recv().unwrap())
            .await
            .unwrap();
        push(&m, "continue answered");
        axum::Json(c).into_response()
    }
    async fn lerror(State(m): State<Arc<Mock>>, body: bytes::Bytes) -> StatusCode {
        push(&m, "POST lifecycle/error");
        m.posted
            .lock()
            .unwrap()
            .push(("lifecycle/error".into(), body.to_vec()));
        StatusCode::ACCEPTED
    }
    async fn ready(State(m): State<Arc<Mock>>) -> StatusCode {
        push(&m, "POST ready");
        StatusCode::ACCEPTED
    }
    async fn next(State(m): State<Arc<Mock>>) -> axum::response::Response {
        push(&m, "GET next");
        match m.events.lock().unwrap().pop() {
            None => StatusCode::GONE.into_response(),
            Some(payload) => {
                let mut resp = axum::Json(payload).into_response();
                let h = resp.headers_mut();
                h.insert(headers::INVOCATION_ID, "inv_1".parse().unwrap());
                h.insert(headers::ATTEMPT_ID, "att_1".parse().unwrap());
                h.insert(headers::EPOCH, "1".parse().unwrap());
                h.insert(headers::DEADLINE_MS, "4102444800000".parse().unwrap());
                resp
            }
        }
    }
    async fn response(State(m): State<Arc<Mock>>, body: bytes::Bytes) -> StatusCode {
        push(&m, "POST response");
        m.posted
            .lock()
            .unwrap()
            .push(("response".into(), body.to_vec()));
        StatusCode::ACCEPTED
    }

    struct Harness {
        mock: Arc<Mock>,
        url: String,
        release: std::sync::mpsc::Sender<Continuation>,
        _server: std::thread::JoinHandle<()>,
    }

    fn start(events: Vec<serde_json::Value>) -> Harness {
        let (release, pending) = std::sync::mpsc::channel();
        let mock = Arc::new(Mock {
            log: Arc::new(Mutex::new(Vec::new())),
            release: Mutex::new(Some(pending)),
            events: Mutex::new(events),
            posted: Mutex::new(Vec::new()),
        });
        let router = Router::new()
            .route(wire::PATH_BOOTSTRAP, post(bootstrap))
            .route(wire::PATH_CHECKPOINT, post(checkpoint))
            .route(wire::PATH_CONTINUE, get(cont))
            .route(wire::PATH_ERROR, post(lerror))
            .route(api::PATH_READY, post(ready))
            .route(api::PATH_NEXT, get(next))
            .route("/runtime/v1/invocations/{a}/response", post(response))
            .with_state(mock.clone());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(listener, router).await.unwrap();
            });
        });
        Harness {
            mock,
            url,
            release,
            _server: server,
        }
    }

    fn log(h: &Harness) -> Vec<String> {
        h.mock.log.lock().unwrap().clone()
    }

    fn in_runtime() -> bool {
        tokio::runtime::Handle::try_current().is_ok()
    }

    /// PLT-4651: bootstrap runs before the checkpoint without an async
    /// runtime, after_restore only after the (mock) restore notification and
    /// inside one, and ready is posted last.
    #[test]
    fn hooks_run_in_order_around_a_mock_restore() {
        let h = start(vec![]);
        let log_b = h.mock.log.clone();
        let log_r = h.mock.log.clone();
        let release = h.release.clone();
        let log_watch = h.mock.log.clone();
        // Release the restore only once the process is waiting in continue,
        // and check that nothing after the checkpoint ran before it.
        let releaser = std::thread::spawn(move || {
            loop {
                if log_watch
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|l| l == "GET continue")
                {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            std::thread::sleep(Duration::from_millis(100));
            let seen = log_watch.lock().unwrap().clone();
            assert!(!seen.iter().any(|l| l == "hook after_restore"), "{seen:?}");
            release
                .send(Continuation::Restored {
                    instance_id: "inst_mock_7".into(),
                    restored_at_ms: 1_000,
                    generation: 7,
                })
                .unwrap();
        });
        let result = builder()
            .runtime_api(&h.url)
            .environment_id("env_test")
            .bootstrap(move || {
                log_b.lock().unwrap().push("hook bootstrap".into());
                assert!(!in_runtime(), "no async runtime before the checkpoint");
                Ok::<_, HandlerError>(vec![2u64, 3, 5, 7])
            })
            .after_restore(move |table, ctx| async move {
                log_r.lock().unwrap().push("hook after_restore".into());
                assert!(in_runtime(), "after_restore runs in the SDK's runtime");
                assert!(ctx.restored);
                assert_eq!(ctx.instance_id, "inst_mock_7");
                assert_eq!(ctx.generation, 7);
                assert_eq!(ctx.restored_at, Some(UNIX_EPOCH + Duration::from_secs(1)));
                assert!(ctx.started_at > UNIX_EPOCH + Duration::from_secs(1));
                assert_eq!(ctx.environment_id, "env_test");
                Ok::<_, HandlerError>(table.iter().sum::<u64>())
            })
            .run(|_state: Arc<u64>, _e: Event, _c: Context| async move {
                Ok::<_, HandlerError>(serde_json::json!({}))
            });
        releaser.join().unwrap();
        result.unwrap();
        assert_eq!(
            log(&h),
            vec![
                "POST bootstrap",
                "hook bootstrap",
                "POST checkpoint",
                "GET continue",
                "continue answered",
                "hook after_restore",
                "POST ready",
                // `run` then enters the normal loop (its best-effort ready).
                "POST ready",
                "GET next",
            ]
        );
    }

    /// PLT-4651: a normal start (no snapshot, `cold`) uses the same API and
    /// serves events with the per-instance state.
    #[test]
    fn cold_start_takes_the_same_path_and_serves() {
        let h = start(vec![serde_json::json!({"k": 1})]);
        h.release.send(Continuation::Cold).unwrap();
        builder()
            .runtime_api(&h.url)
            .environment_id("env_cold")
            .runtime_flavor(RuntimeFlavor::MultiThread)
            .bootstrap(|| Ok::<_, HandlerError>("fixed"))
            .after_restore(|fixed, ctx| async move {
                assert!(!ctx.restored);
                assert_eq!(ctx.instance_id, "env_cold");
                assert_eq!(ctx.generation, 0);
                assert_eq!(ctx.restored_at, None);
                Ok::<_, HandlerError>(format!("{fixed}+{}", ctx.instance_id))
            })
            .run(|state: Arc<String>, event: Event, _c: Context| async move {
                Ok::<_, HandlerError>(serde_json::json!({"state": *state, "k": event.json()["k"]}))
            })
            .unwrap();
        let posted = h.mock.posted.lock().unwrap();
        assert_eq!(posted.len(), 1);
        let v: serde_json::Value = serde_json::from_slice(&posted[0].1).unwrap();
        assert_eq!(v, serde_json::json!({"state": "fixed+env_cold", "k": 1}));
    }

    /// PLT-4651: a failing or panicking bootstrap is reported as a
    /// pre-checkpoint failure; nothing after it happens.
    #[test]
    fn bootstrap_failure_never_reaches_checkpoint_or_ready() {
        for panics in [false, true] {
            let h = start(vec![]);
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let err = builder()
                .runtime_api(&h.url)
                .bootstrap(move || {
                    if panics {
                        panic!("table corrupt");
                    }
                    Err::<(), _>(HandlerError::with_type("Demo.Bootstrap", "table corrupt"))
                })
                .after_restore(|_: (), _ctx| async { Ok::<_, HandlerError>(()) })
                .run(|_s: Arc<()>, _e: Event, _c: Context| async {
                    Ok::<_, HandlerError>(serde_json::json!({}))
                })
                .unwrap_err();
            std::panic::set_hook(prev);
            match err {
                SdkError::Lifecycle {
                    error_type,
                    message,
                } => {
                    assert_eq!(error_type, error_types::PRE_CHECKPOINT_FAILED);
                    assert!(message.contains("table corrupt"), "{message}");
                }
                other => panic!("unexpected {other:?}"),
            }
            assert_eq!(log(&h), vec!["POST bootstrap", "POST lifecycle/error"]);
            let posted = h.mock.posted.lock().unwrap();
            let r: RuntimeErrorReport = serde_json::from_slice(&posted[0].1).unwrap();
            let expected = if panics {
                "Runtime.Panic"
            } else {
                "Demo.Bootstrap"
            };
            assert_eq!(r.error_type, expected);
        }
    }

    /// PLT-4651: a failing or panicking after_restore is reported as an
    /// after-restore failure and `ready` is never posted.
    #[test]
    fn after_restore_failure_never_posts_ready() {
        for panics in [false, true] {
            let h = start(vec![]);
            h.release.send(Continuation::Cold).unwrap();
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let err = builder()
                .runtime_api(&h.url)
                .bootstrap(|| Ok::<_, HandlerError>(()))
                .after_restore(move |_: (), _ctx| async move {
                    if panics {
                        panic!("db unreachable");
                    }
                    Err::<(), _>(HandlerError::new("db unreachable"))
                })
                .run(|_s: Arc<()>, _e: Event, _c: Context| async {
                    Ok::<_, HandlerError>(serde_json::json!({}))
                })
                .unwrap_err();
            std::panic::set_hook(prev);
            assert!(
                matches!(&err, SdkError::Lifecycle { error_type, message }
                    if error_type == error_types::AFTER_RESTORE_FAILED && message.contains("db unreachable")),
                "{err:?}"
            );
            let seen = log(&h);
            assert_eq!(
                seen,
                vec![
                    "POST bootstrap",
                    "POST checkpoint",
                    "GET continue",
                    "continue answered",
                    "POST lifecycle/error"
                ]
            );
        }
    }

    /// Without a reachable bridge the lifecycle stops before the bootstrap
    /// hook: the bridge must know a lifecycle is open before state is built.
    #[test]
    fn unreachable_bridge_stops_before_bootstrap() {
        // No mock at all: connecting fails before bootstrap runs.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let ran = Arc::new(Mutex::new(false));
        let ran2 = ran.clone();
        let err = builder()
            .runtime_api(url)
            .bootstrap(move || {
                *ran2.lock().unwrap() = true;
                Ok::<_, HandlerError>(())
            })
            .after_restore(|_: (), _ctx| async { Ok::<_, HandlerError>(()) })
            .run(|_s: Arc<()>, _e: Event, _c: Context| async {
                Ok::<_, HandlerError>(serde_json::json!({}))
            })
            .unwrap_err();
        assert!(matches!(err, SdkError::Io(_)), "{err:?}");
        assert!(!*ran.lock().unwrap(), "bootstrap must not run");
    }
}
