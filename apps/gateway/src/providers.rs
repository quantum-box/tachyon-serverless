//! Provider selection for the binary.
//!
//! The concrete providers live in `crates/providers/{process,firecracker}`.
//! Their construction is gated behind the `providers` cargo feature (on by
//! default); `--no-default-features` yields a gateway that refuses every
//! provider, which is only useful for API-surface builds and tests.
//!
//! The fake provider is intentionally not constructible from configuration.

use std::sync::Arc;

use tachyon_serverless_application::{AppError, GatewayConfig, ProviderConfig, ProviderFactory};
use tachyon_serverless_provider_port::ExecutionProvider;

/// Factory used by `serve`.
#[derive(Debug, Default, Clone, Copy)]
pub struct GatewayProviderFactory;

impl ProviderFactory for GatewayProviderFactory {
    fn build(&self, config: &ProviderConfig) -> Result<Arc<dyn ExecutionProvider>, AppError> {
        build_from(config)
    }
}

/// Build the provider named in `config.provider`.
pub fn build_provider(config: &GatewayConfig) -> Result<Arc<dyn ExecutionProvider>, AppError> {
    build_from(&config.provider)
}

#[cfg(feature = "providers")]
fn build_from(config: &ProviderConfig) -> Result<Arc<dyn ExecutionProvider>, AppError> {
    use tachyon_serverless_application::ProviderKindConfig;
    match config.kind {
        ProviderKindConfig::Process => {
            let p = config
                .process
                .as_ref()
                .ok_or_else(|| AppError::InvalidRequest("[provider.process] is missing".into()))?;
            let provider = tachyon_serverless_provider_process::ProcessProvider::new(
                tachyon_serverless_provider_process::ProcessProviderConfig {
                    bridge_binary: p.bridge_binary.clone(),
                    workdir: p.workdir.clone(),
                },
            );
            Ok(Arc::new(provider))
        }
        ProviderKindConfig::Firecracker => {
            let f = config.firecracker.as_ref().ok_or_else(|| {
                AppError::InvalidRequest("[provider.firecracker] is missing".into())
            })?;
            let provider = tachyon_serverless_provider_firecracker::FirecrackerProvider::new(
                tachyon_serverless_provider_firecracker::FirecrackerConfig {
                    firecracker_binary: f.firecracker_binary.clone(),
                    kernel: f.kernel.clone(),
                    rootfs: f.rootfs.clone(),
                    workdir: f.workdir.clone(),
                    vsock_port: f.vsock_port,
                    ..Default::default()
                },
            );
            Ok(Arc::new(provider))
        }
    }
}

#[cfg(not(feature = "providers"))]
fn build_from(config: &ProviderConfig) -> Result<Arc<dyn ExecutionProvider>, AppError> {
    Err(AppError::ProviderUnavailable(format!(
        "provider `{}` requested but this gateway binary was built without the `providers` \
         feature (cargo build -p tachyon-serverless-gateway --features providers)",
        config.kind.as_str()
    )))
}
