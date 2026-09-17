//! Egress readiness gate (PLT-4622, docs/adr/0005-egress-profiles.md).
//!
//! The issue asks that user code never runs before the network policy is in
//! force. Two shapes of environment exist:
//!
//! - egress `none`: the microVM gets no network device, so there is nothing to
//!   race against;
//! - egress `restricted` / `public-web`: the microVM gets exactly one virtio-net
//!   device backed by a per-environment tap, and the host firewall chain for
//!   that tap must be installed **and read back** before the device may be
//!   configured and before `InstanceStart`.
//!
//! This module makes both an explicit, fail-closed check instead of an
//! assumption:
//!
//! 1. [`check_planned_calls`] — before anything is sent, the API calls the
//!    provider is about to make may configure a network interface only when a
//!    verified policy exists, and then only that one interface; MMDS is always
//!    refused;
//! 2. [`check_vm_config`] — after configuration and **before**
//!    `InstanceStart`, the VMM's own view (`GET /vm/config`) must list exactly
//!    the expected interface (or none) and no MMDS. Anything unexpected —
//!    including a response without the `network-interfaces` key — refuses the
//!    boot.
//!
//! The provider re-reads the firewall policy between step 2 and
//! `InstanceStart` ([`crate::network::HostNetwork::verify`]). The guest cannot
//! start (and so user code cannot run) until all of this passes.

use serde_json::Value;

/// Guest-side name of the single network interface.
pub const GUEST_IFACE_ID: &str = "eth0";

/// The one network interface a verified egress policy allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedNic {
    pub iface_id: String,
    pub host_dev_name: String,
}

impl ExpectedNic {
    /// API path the interface is configured through.
    pub fn api_path(&self) -> String {
        format!("/network-interfaces/{}", self.iface_id)
    }
}

/// Refuse a configuration plan that touches networking in a way the policy
/// does not allow: MMDS always, a network interface unless it is exactly the
/// expected one (configured once).
pub fn check_planned_calls<'a>(
    paths: impl IntoIterator<Item = &'a str>,
    expected: Option<&ExpectedNic>,
) -> Result<(), String> {
    let mut nics = 0usize;
    for path in paths {
        if path.starts_with("/mmds") {
            return Err(format!(
                "egress gate: the VM configuration plan contains `{path}`; \
                 no egress profile allows a metadata service"
            ));
        }
        if path.starts_with("/network-interfaces") {
            match expected {
                Some(nic) if path == nic.api_path() => nics += 1,
                Some(nic) => {
                    return Err(format!(
                        "egress gate: the VM configuration plan contains `{path}`, \
                         but the verified egress policy only covers `{}`",
                        nic.api_path()
                    ));
                }
                None => {
                    return Err(format!(
                        "egress gate: the VM configuration plan contains `{path}`, \
                         but egress none requires a microVM without a network device"
                    ));
                }
            }
        }
    }
    if expected.is_some() && nics != 1 {
        return Err(format!(
            "egress gate: the plan configures the policed network interface {nics} times (expected 1)"
        ));
    }
    Ok(())
}

/// Refuse a VMM configuration (`GET /vm/config`) whose network path is not
/// exactly what the verified policy covers.
pub fn check_vm_config(config: &Value, expected: Option<&ExpectedNic>) -> Result<(), String> {
    match config.get("network-interfaces") {
        Some(Value::Array(ifaces)) => match (expected, ifaces.as_slice()) {
            (None, []) => {}
            (Some(nic), [only])
                if only.get("iface_id").and_then(Value::as_str) == Some(&nic.iface_id)
                    && only.get("host_dev_name").and_then(Value::as_str)
                        == Some(&nic.host_dev_name) => {}
            (_, _) => {
                let ids: Vec<String> = ifaces
                    .iter()
                    .map(|i| {
                        format!(
                            "{}->{}",
                            i.get("iface_id").and_then(Value::as_str).unwrap_or("?"),
                            i.get("host_dev_name")
                                .and_then(Value::as_str)
                                .unwrap_or("?")
                        )
                    })
                    .collect();
                return Err(match expected {
                    None => format!(
                        "egress gate: the VMM has {} network interface(s) configured ({}) before InstanceStart",
                        ifaces.len(),
                        ids.join(", ")
                    ),
                    Some(nic) => format!(
                        "egress gate: the VMM has network interfaces [{}] before InstanceStart, \
                         but the verified egress policy covers exactly {}->{}",
                        ids.join(", "),
                        nic.iface_id,
                        nic.host_dev_name
                    ),
                });
            }
        },
        Some(Value::Null) | None => {
            return Err(
                "egress gate: GET /vm/config did not report `network-interfaces`; \
                 cannot prove which network devices the microVM has"
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
            "egress gate: MMDS is configured ({other}); no egress profile allows a metadata service"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BASE: [&str; 6] = [
        "/machine-config",
        "/boot-source",
        "/drives/rootfs",
        "/drives/function",
        "/drives/scratch",
        "/vsock",
    ];

    fn nic() -> ExpectedNic {
        ExpectedNic {
            iface_id: "eth0".into(),
            host_dev_name: "tsls0123456789a".into(),
        }
    }

    #[test]
    fn plans_without_networking_pass() {
        check_planned_calls(BASE, None).unwrap();
    }

    #[test]
    fn plans_with_networking_fail_closed_without_a_policy() {
        for path in ["/network-interfaces/eth0", "/mmds/config", "/mmds"] {
            let err = check_planned_calls(["/machine-config", path], None).unwrap_err();
            assert!(err.contains(path), "{err}");
        }
    }

    #[test]
    fn a_policy_allows_exactly_its_interface_once_and_never_mmds() {
        let n = nic();
        let with = |extra: &[&'static str]| {
            let mut v: Vec<&str> = BASE.to_vec();
            v.extend_from_slice(extra);
            v
        };
        check_planned_calls(with(&["/network-interfaces/eth0"]), Some(&n)).unwrap();
        for bad in [
            with(&[]),
            with(&["/network-interfaces/eth0", "/network-interfaces/eth0"]),
            with(&["/network-interfaces/eth1"]),
            with(&["/network-interfaces/eth0", "/network-interfaces/eth1"]),
            with(&["/network-interfaces/eth0", "/mmds/config"]),
        ] {
            assert!(
                check_planned_calls(bad.clone(), Some(&n)).is_err(),
                "{bad:?}"
            );
        }
    }

    /// Shape taken from Firecracker v1.17 `GET /vm/config` before boot.
    #[test]
    fn a_vm_config_without_nic_passes() {
        let cfg = json!({
            "balloon": null, "drives": [], "machine-config": {"vcpu_count": 1},
            "mmds-config": null, "network-interfaces": [], "vsock": null
        });
        check_vm_config(&cfg, None).unwrap();
        assert!(
            check_vm_config(&cfg, Some(&nic())).is_err(),
            "a policed environment must really have its interface"
        );
    }

    #[test]
    fn a_vm_config_with_a_nic_or_mmds_or_unknown_shape_fails_closed() {
        let tap0 = json!({"network-interfaces": [{"iface_id": "eth0", "host_dev_name": "tap0"}], "mmds-config": null});
        assert!(check_vm_config(&tap0, None).unwrap_err().contains("eth0"));
        assert!(
            check_vm_config(&tap0, Some(&nic())).is_err(),
            "wrong tap behind eth0"
        );
        let mmds =
            json!({"network-interfaces": [], "mmds-config": {"network_interfaces": ["eth0"]}});
        assert!(check_vm_config(&mmds, None).unwrap_err().contains("MMDS"));
        for cfg in [
            json!({}),
            json!({"network-interfaces": null}),
            json!({"network-interfaces": "none"}),
        ] {
            assert!(check_vm_config(&cfg, None).is_err(), "{cfg}");
            assert!(check_vm_config(&cfg, Some(&nic())).is_err(), "{cfg}");
        }
    }

    #[test]
    fn a_vm_config_with_exactly_the_policed_nic_passes() {
        let n = nic();
        let ok = json!({"network-interfaces": [{"iface_id": "eth0", "host_dev_name": n.host_dev_name, "guest_mac": "06:00:ac:1e:00:02"}], "mmds-config": null});
        check_vm_config(&ok, Some(&n)).unwrap();
        let two = json!({"network-interfaces": [
            {"iface_id": "eth0", "host_dev_name": n.host_dev_name},
            {"iface_id": "eth1", "host_dev_name": "tap9"}
        ], "mmds-config": null});
        assert!(check_vm_config(&two, Some(&n)).is_err());
        let with_mmds = json!({"network-interfaces": [{"iface_id": "eth0", "host_dev_name": n.host_dev_name}], "mmds-config": {"version": "V2"}});
        assert!(check_vm_config(&with_mmds, Some(&n)).is_err());
    }
}
