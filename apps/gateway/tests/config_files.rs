//! The configuration files shipped in `config/` must stay loadable.

use std::path::PathBuf;

use tachyon_serverless_application::{GatewayConfig, Profile, ProviderKindConfig, StoreBackend};
use tachyon_serverless_domain::TenantId;

fn config_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config")
}

#[test]
fn dev_config_loads() {
    let cfg = GatewayConfig::load(config_dir().join("gateway.dev.toml")).unwrap();
    assert_eq!(cfg.profile, Profile::Dev);
    assert_eq!(cfg.provider.kind, ProviderKindConfig::Process);
    let p = cfg.provider.process.as_ref().unwrap();
    assert!(
        p.bridge_binary.is_absolute()
            && p.bridge_binary
                .ends_with("target/debug/tachyon-serverless-runtime-bridge")
    );
    assert!(p.workdir.is_absolute() && p.workdir.ends_with("data/process"));
    assert_eq!(cfg.identity.tokens.len(), 2);
    assert_eq!(
        cfg.identity.tokens[0].tenant_id,
        TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzza").unwrap()
    );
    assert_eq!(cfg.identity.tokens[0].token.expose(), "dev-token-tenant-a");
    assert_eq!(cfg.identity.tokens[1].token.expose(), "dev-token-tenant-b");
    assert_eq!(cfg.secrets.bindings.len(), 2);
    assert!(
        cfg.secrets
            .bindings
            .iter()
            .all(|b| b.binding_ref == "demo-secret")
    );
    assert_eq!(cfg.store.backend, StoreBackend::Sqlite);
    assert_eq!(cfg.store.output_retention_seconds, 7 * 24 * 60 * 60);
}

#[test]
fn firecracker_config_loads() {
    let cfg = GatewayConfig::load(config_dir().join("gateway.firecracker.toml")).unwrap();
    assert_eq!(cfg.profile, Profile::Production);
    assert_eq!(cfg.provider.kind, ProviderKindConfig::Firecracker);
    let f = cfg.provider.firecracker.as_ref().unwrap();
    assert!(
        f.firecracker_binary.is_absolute()
            && f.firecracker_binary.ends_with(".kvm/bin/firecracker")
    );
    assert!(f.kernel.is_absolute() && f.kernel.ends_with(".kvm/vmlinux"));
    assert!(f.rootfs.is_absolute() && f.rootfs.ends_with(".kvm/rootfs.ext4"));
    assert!(f.workdir.is_absolute() && f.workdir.ends_with(".kvm/run"));
    assert_eq!(f.vsock_port, 5000);
    assert_eq!(cfg.identity.tokens.len(), 2);
    assert_eq!(cfg.store.backend, StoreBackend::Sqlite);
}
