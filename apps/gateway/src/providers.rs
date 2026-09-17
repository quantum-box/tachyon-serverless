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
            use tachyon_serverless_provider_firecracker::config::{
                CgroupConfig, CgroupMode, JailerConfig,
            };
            let cgroup_defaults = CgroupConfig::default();
            let cgroup = CgroupConfig {
                // Unresolved only when the config was not built by from_toml:
                // fail closed.
                mode: match f.cgroup.mode.as_deref() {
                    Some("best-effort") => CgroupMode::BestEffort,
                    Some("off") => CgroupMode::Off,
                    _ => CgroupMode::Required,
                },
                root: f.cgroup.root.clone().unwrap_or(cgroup_defaults.root),
                parent: f.cgroup.parent.clone().unwrap_or(cgroup_defaults.parent),
                memory_overhead_mib: f
                    .cgroup
                    .memory_overhead_mib
                    .unwrap_or(cgroup_defaults.memory_overhead_mib),
                pids_max: f.cgroup.pids_max.unwrap_or(cgroup_defaults.pids_max),
                cpu_period_us: cgroup_defaults.cpu_period_us,
            };
            let jailer = f.jailer.enabled.then(|| {
                let d = JailerConfig::default();
                JailerConfig {
                    binary: f.jailer.binary.clone().unwrap_or(d.binary),
                    uid: f.jailer.uid.unwrap_or(d.uid),
                    gid: f.jailer.gid.unwrap_or(d.gid),
                    chroot_base: f.jailer.chroot_base.clone().unwrap_or(d.chroot_base),
                    new_pid_ns: f.jailer.new_pid_ns.unwrap_or(d.new_pid_ns),
                }
            });
            let provider = tachyon_serverless_provider_firecracker::FirecrackerProvider::new(
                tachyon_serverless_provider_firecracker::FirecrackerConfig {
                    cgroup,
                    jailer,
                    firecracker_binary: f.firecracker_binary.clone(),
                    kernel: f.kernel.clone(),
                    rootfs: f.rootfs.clone(),
                    workdir: f.workdir.clone(),
                    vsock_port: f.vsock_port,
                    network: tachyon_serverless_provider_firecracker::network::NetworkConfig {
                        guest_cidr: f.network.guest_network().map_err(|e| {
                            AppError::InvalidRequest(format!("[provider.firecracker.network]: {e}"))
                        })?,
                        dns_resolver: f.network.dns_resolver,
                        nft_binary: f.network.nft_binary.clone(),
                        ip_binary: f.network.ip_binary.clone(),
                    },
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
