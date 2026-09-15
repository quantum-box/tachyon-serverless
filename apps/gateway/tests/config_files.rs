//! The configuration files shipped in `config/` must stay loadable.

use std::path::PathBuf;

use tachyon_serverless_application::{GatewayConfig, Profile, ProviderKindConfig};
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
    assert_eq!(
        p.bridge_binary,
        PathBuf::from("target/debug/tachyon-serverless-runtime-bridge")
    );
    assert_eq!(p.workdir, PathBuf::from("./data/process"));
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
}

#[test]
fn firecracker_config_loads() {
    let cfg = GatewayConfig::load(config_dir().join("gateway.firecracker.toml")).unwrap();
    assert_eq!(cfg.profile, Profile::Production);
    assert_eq!(cfg.provider.kind, ProviderKindConfig::Firecracker);
    let f = cfg.provider.firecracker.as_ref().unwrap();
    assert_eq!(f.firecracker_binary, PathBuf::from(".kvm/bin/firecracker"));
    assert_eq!(f.kernel, PathBuf::from(".kvm/vmlinux"));
    assert_eq!(f.rootfs, PathBuf::from(".kvm/rootfs.ext4"));
    assert_eq!(f.workdir, PathBuf::from(".kvm/run"));
    assert_eq!(f.vsock_port, 5000);
    assert_eq!(cfg.identity.tokens.len(), 2);
}
