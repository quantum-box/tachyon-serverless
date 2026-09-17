//! Tachyon Serverless gateway (axum 0.8).
//!
//! `router(state)` builds the HTTP API described in
//! `crates/api-types/src/lib.rs`; `serve(config, shutdown)` runs it with
//! graceful shutdown. The binary in `main.rs` is a thin wrapper.
//!
//! Route notes:
//! - matchit (axum's router) does not accept a suffix after a path parameter,
//!   so `POST /v1/functions/{function_id}:invoke` is served by registering
//!   `POST /v1/functions/{function_id}` and stripping the `:invoke` suffix in
//!   the handler. `POST /v1/functions/{function_id}/invoke` is registered
//!   too. The same applies to `:cancel` / `/cancel` on invocations.
//! - The HTTP adapter is mounted at `/v1/functions/{function_id}/http`,
//!   `/http/` and `/http/{*path}` for any method.

pub mod error;
pub mod handlers;
pub mod middleware;
pub mod openapi;
pub mod providers;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{any, get, post, put};
use tokio::net::TcpListener;

use tachyon_serverless_application::{Application, GatewayConfig};

/// Shared state handed to every handler.
pub type AppState = Arc<Application>;

/// Build the API router. Authentication applies to everything under `/v1`.
pub fn router(state: AppState) -> Router {
    let limits = state.limits.clone();
    let artifact_limit = usize::try_from(limits.max_artifact_bytes).unwrap_or(usize::MAX);
    let payload_limit = usize::try_from(limits.max_payload_bytes.saturating_mul(2))
        .unwrap_or(usize::MAX)
        .max(64 * 1024);

    let artifacts = Router::new()
        .route("/v1/artifacts", post(handlers::upload_artifact))
        .layer(DefaultBodyLimit::max(artifact_limit));

    let invoke = Router::new()
        // `POST /v1/functions/{id}:invoke` lands here with id = "fn_...:invoke".
        .route("/v1/functions/{function_id}", post(handlers::invoke_colon))
        .route(
            "/v1/functions/{function_id}/invoke",
            post(handlers::invoke_json),
        )
        .route("/v1/functions/{function_id}/http", any(handlers::http_root))
        .route(
            "/v1/functions/{function_id}/http/",
            any(handlers::http_root),
        )
        .route(
            "/v1/functions/{function_id}/http/{*path}",
            any(handlers::http_path),
        )
        .layer(DefaultBodyLimit::max(payload_limit));

    let management = Router::new()
        .route("/v1/provider", get(handlers::provider_info))
        .route(
            "/v1/functions",
            post(handlers::create_function).get(handlers::list_functions),
        )
        .route(
            "/v1/functions/{function_id}",
            get(handlers::get_function).delete(handlers::delete_function),
        )
        .route(
            "/v1/functions/{function_id}/revisions",
            post(handlers::create_revision).get(handlers::list_revisions),
        )
        .route(
            "/v1/functions/{function_id}/revisions/{revision_id}",
            get(handlers::get_revision),
        )
        .route(
            "/v1/functions/{function_id}/aliases",
            get(handlers::list_aliases),
        )
        .route(
            "/v1/functions/{function_id}/aliases/{alias}",
            put(handlers::update_alias).get(handlers::get_alias),
        )
        .route(
            "/v1/functions/{function_id}/invocations",
            get(handlers::list_invocations),
        )
        .route("/v1/functions/{function_id}/usage", get(handlers::usage))
        .route(
            "/v1/invocations/{invocation_id}",
            get(handlers::get_invocation).post(handlers::cancel_colon),
        )
        .route(
            "/v1/invocations/{invocation_id}/cancel",
            post(handlers::cancel_invocation),
        )
        .route(
            "/v1/invocations/{invocation_id}/logs",
            get(handlers::invocation_logs),
        );

    let authenticated = Router::new()
        .merge(artifacts)
        .merge(invoke)
        .merge(management)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            middleware::authenticate,
        ));

    Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/openapi.json", get(handlers::openapi_json))
        .merge(authenticated)
        .fallback(handlers::not_found)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            middleware::request_id,
        ))
        .with_state(state)
}

/// Run the gateway until `shutdown` resolves. In-flight invocations are
/// cancelled and their environments terminated before the server exits.
pub async fn serve(
    config: GatewayConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let provider = providers::build_provider(&config)?;
    let app = Application::bootstrap(config, provider)?;
    // Reclaim environments a previous process left behind before the listener
    // accepts, so an orphan cannot outlive a crash or a kill.
    app.reconcile_on_startup().await;
    let listener = TcpListener::bind(&app.config.listen).await?;
    let addr = listener.local_addr()?;
    tracing::info!(
        %addr,
        profile = app.config.profile.as_str(),
        provider = app.provider.kind().as_str(),
        "gateway listening"
    );
    let service = router(app.clone()).into_make_service_with_connect_info::<SocketAddr>();
    // Reap pooled environments past their idle TTL. Not spawned at all unless
    // environment reuse is on, in which case nothing is ever pooled.
    let sweeper = app.pool.policy().reuse_enabled().then(|| {
        let app = app.clone();
        let every = (app.config.pool.idle_ttl() / 2).max(Duration::from_secs(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                app.sweep_idle_environments().await;
            }
        })
    });
    // Inline invocation outputs past their retention become digests
    // (`[store] output_retention_seconds`).
    let retention = {
        let app = app.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(600));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let purged = app.purge_expired_outputs();
                if purged > 0 {
                    tracing::info!(
                        purged,
                        "expired invocation outputs replaced by their digest"
                    );
                }
            }
        })
    };
    let draining = app.clone();
    axum::serve(listener, service)
        .with_graceful_shutdown(async move {
            shutdown.await;
            tracing::info!("shutdown requested; cancelling in-flight invocations");
            draining.invoke.shutdown_all(Duration::from_secs(10)).await;
            // A pooled environment must never outlive this process: its
            // bridge session dies with us and nothing could reclaim it.
            let swept = draining.drain_pool().await;
            if swept.reaped > 0 {
                tracing::info!(
                    reaped = swept.reaped,
                    "pooled environments terminated on shutdown"
                );
            }
        })
        .await?;
    if let Some(sweeper) = sweeper {
        sweeper.abort();
    }
    retention.abort();
    if let Err(e) = app.store.flush() {
        tracing::warn!(error = %e, "final state checkpoint failed");
    }
    tracing::info!("gateway stopped");
    Ok(())
}
