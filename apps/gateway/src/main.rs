//! `tachyon-serverless-gateway` binary.
//!
//! ```text
//! tachyon-serverless-gateway --config config/gateway.dev.toml
//! GATEWAY_CONFIG=config/gateway.dev.toml tachyon-serverless-gateway
//! LOG_FORMAT=json RUST_LOG=info tachyon-serverless-gateway
//! ```
//!
//! SIGINT / SIGTERM trigger a graceful shutdown: in-flight invocations are
//! cancelled, their environments terminated, and the state flushed.

use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use tachyon_serverless_application::GatewayConfig;

#[derive(Debug, Parser)]
#[command(
    name = "tachyon-serverless-gateway",
    version,
    about = "Tachyon Serverless gateway"
)]
struct Args {
    /// Path to the gateway configuration (TOML).
    #[arg(
        long,
        env = "GATEWAY_CONFIG",
        default_value = "config/gateway.dev.toml"
    )]
    config: PathBuf,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info,hyper=warn"));
    let json = std::env::var("LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .flatten_event(true)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "cannot listen for SIGINT");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => tracing::info!("SIGINT received"),
        _ = terminate => tracing::info!("SIGTERM received"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let config = GatewayConfig::load(&args.config)
        .map_err(|e| anyhow::anyhow!("{}: {e}", args.config.display()))?;
    tracing::info!(config = %args.config.display(), "configuration loaded");
    tachyon_serverless_gateway::serve(config, shutdown_signal()).await
}
