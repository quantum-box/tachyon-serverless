//! Architectural boundary tests: the domain crate must not depend on
//! frameworks, runtimes or provider implementations.

#[test]
fn domain_has_no_framework_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    let forbidden = [
        "axum",
        "tokio",
        "sqlx",
        "hyper",
        "reqwest",
        "firecracker",
        "kube",
        "k8s",
        "vsock",
    ];
    for name in forbidden {
        assert!(
            !manifest.contains(&format!("\n{name} ")) && !manifest.contains(&format!("\n{name}=")),
            "domain Cargo.toml must not depend on `{name}`"
        );
    }
}
