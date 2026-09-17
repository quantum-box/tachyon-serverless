//! Egress readiness gate (PLT-4622).
//!
//! The issue asks that user code never runs before the network policy is in
//! force. P1's policy is `egress none`, enforced structurally: the microVM gets
//! no network device, so there is nothing to race against. This module makes
//! that structure an explicit, fail-closed check instead of an absence:
//!
//! 1. [`check_planned_calls`] — before anything is sent, the API calls the
//!    provider is about to make must not configure a network interface or
//!    MMDS (which needs one);
//! 2. [`check_vm_config`] — after configuration and **before**
//!    `InstanceStart`, the VMM's own view (`GET /vm/config`) must list no
//!    network interface and no MMDS. Anything unexpected — including a
//!    response without the `network-interfaces` key — refuses the boot.
//!
//! The guest cannot start (and so user code cannot run) until both pass.

use serde_json::Value;

/// API paths that would give the guest a network path.
const NETWORK_PATH_PREFIXES: [&str; 2] = ["/network-interfaces", "/mmds"];

/// Refuse a configuration plan that touches networking.
pub fn check_planned_calls<'a>(paths: impl IntoIterator<Item = &'a str>) -> Result<(), String> {
    for path in paths {
        if NETWORK_PATH_PREFIXES.iter().any(|p| path.starts_with(p)) {
            return Err(format!(
                "egress gate: the VM configuration plan contains `{path}`, \
                 but egress none requires a microVM without a network device"
            ));
        }
    }
    Ok(())
}

/// Refuse a VMM configuration (`GET /vm/config`) that has a network path.
pub fn check_vm_config(config: &Value) -> Result<(), String> {
    match config.get("network-interfaces") {
        Some(Value::Array(ifaces)) if ifaces.is_empty() => {}
        Some(Value::Array(ifaces)) => {
            let ids: Vec<&str> = ifaces
                .iter()
                .map(|i| i.get("iface_id").and_then(Value::as_str).unwrap_or("?"))
                .collect();
            return Err(format!(
                "egress gate: the VMM has {} network interface(s) configured ({}) before InstanceStart",
                ifaces.len(),
                ids.join(", ")
            ));
        }
        Some(Value::Null) | None => {
            return Err(
                "egress gate: GET /vm/config did not report `network-interfaces`; \
                 cannot prove the microVM has no network device"
                    .into(),
            );
        }
        Some(other) => {
            return Err(format!(
                "egress gate: unexpected `network-interfaces` in GET /vm/config: {other}"
            ));
        }
    }
    match config.get("mmds-config") {
        None | Some(Value::Null) => Ok(()),
        Some(other) => Err(format!(
            "egress gate: MMDS is configured ({other}); egress none allows no metadata service"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plans_without_networking_pass() {
        check_planned_calls([
            "/machine-config",
            "/boot-source",
            "/drives/rootfs",
            "/drives/function",
            "/drives/scratch",
            "/vsock",
        ])
        .unwrap();
    }

    #[test]
    fn plans_with_networking_fail_closed() {
        for path in ["/network-interfaces/eth0", "/mmds/config", "/mmds"] {
            let err = check_planned_calls(["/machine-config", path]).unwrap_err();
            assert!(err.contains(path), "{err}");
        }
    }

    /// Shape taken from Firecracker v1.17 `GET /vm/config` before boot.
    #[test]
    fn a_vm_config_without_nic_passes() {
        let cfg = json!({
            "balloon": null, "drives": [], "machine-config": {"vcpu_count": 1},
            "mmds-config": null, "network-interfaces": [], "vsock": null
        });
        check_vm_config(&cfg).unwrap();
    }

    #[test]
    fn a_vm_config_with_a_nic_or_mmds_or_unknown_shape_fails_closed() {
        let nic = json!({"network-interfaces": [{"iface_id": "eth0", "host_dev_name": "tap0"}], "mmds-config": null});
        assert!(check_vm_config(&nic).unwrap_err().contains("eth0"));
        let mmds =
            json!({"network-interfaces": [], "mmds-config": {"network_interfaces": ["eth0"]}});
        assert!(check_vm_config(&mmds).unwrap_err().contains("MMDS"));
        for cfg in [
            json!({}),
            json!({"network-interfaces": null}),
            json!({"network-interfaces": "none"}),
        ] {
            assert!(check_vm_config(&cfg).is_err(), "{cfg}");
        }
    }
}
