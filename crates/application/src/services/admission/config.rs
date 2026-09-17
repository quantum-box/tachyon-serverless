//! `[capacity]` sub-sections of the gateway configuration (PLT-4634,
//! docs/adr/0006-autoscaling-and-admission.md).
//!
//! The top-level `[capacity]` keys (`max_concurrency`, `max_queue`,
//! `queue_timeout_seconds`) stay in [`crate::config::CapacityConfig`]; the
//! types here describe the node, its per-environment overhead, tenant quotas,
//! the start-rate limiter, the start-failure circuit breaker and the
//! autoscaler's rate window. Every section has defaults, so a configuration
//! written before PLT-4634 keeps parsing and keeps its old behaviour (no
//! resource bound, no tenant quota).

use serde::Deserialize;

use tachyon_serverless_domain::TenantId;

use super::resources::Resources;

/// `[capacity.node]`: the one host this gateway places environments on.
///
/// A resource left unset is unbounded (only `max_concurrency` limits it). The
/// overheads are added to **every** environment's reservation on top of the
/// revision's own `resources`: the VMM process (Firecracker's own RSS and
/// page tables), the guest runtime bridge and the per-environment host
/// artefacts. They are configuration, not measurements: the defaults are
/// conservative estimates and have to be replaced by the jailer/cgroup
/// measurements of the node that runs the gateway.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Name reported by `GET /v1/capacity`.
    pub name: String,
    /// Location label of this node (e.g. `jp`). A tenant or revision that
    /// requires a region is only placed on a node whose label is exactly that
    /// region; a node without a label satisfies no requirement.
    pub region: Option<String>,
    pub cpu_millis: Option<u64>,
    pub memory_mib: Option<u64>,
    pub ephemeral_storage_mib: Option<u64>,
    /// Memory of the VMM process per environment, outside the guest.
    pub vmm_overhead_memory_mib: u64,
    /// Memory of the guest runtime bridge / init per environment.
    pub bridge_overhead_memory_mib: u64,
    /// CPU reserved per environment for the VMM and the bridge.
    pub overhead_cpu_millis: u64,
    /// Host storage per environment outside the ephemeral drive (logs,
    /// sockets, the function drive image).
    pub overhead_storage_mib: u64,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            name: "local".into(),
            region: None,
            cpu_millis: None,
            memory_mib: None,
            ephemeral_storage_mib: None,
            vmm_overhead_memory_mib: 16,
            bridge_overhead_memory_mib: 8,
            overhead_cpu_millis: 0,
            overhead_storage_mib: 0,
        }
    }
}

impl NodeConfig {
    /// What one environment reserves on top of its revision's resources.
    pub fn overhead(&self) -> Resources {
        Resources {
            cpu_millis: self.overhead_cpu_millis,
            memory_mib: self
                .vmm_overhead_memory_mib
                .saturating_add(self.bridge_overhead_memory_mib),
            ephemeral_storage_mib: self.overhead_storage_mib,
        }
    }
}

/// Quota fields shared by `[capacity.tenant_defaults]` and each
/// `[[capacity.tenants]]` entry.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct TenantQuotaConfig {
    /// Environments this tenant may have starting or busy at once. Unset:
    /// only the node's `max_concurrency` applies.
    pub max_concurrency: Option<usize>,
    /// Invocations of this tenant allowed to wait. Unset: only the node-wide
    /// `max_queue` applies.
    pub max_queue: Option<usize>,
    /// Share weight for fair queueing (default 1).
    pub weight: Option<u32>,
    /// Region every invocation of this tenant must run in (e.g. `jp`). Never
    /// relaxed: a node with another or no region label rejects the
    /// invocation.
    pub required_region: Option<String>,
}

/// One `[[capacity.tenants]]` entry.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TenantQuotaEntry {
    pub tenant_id: TenantId,
    #[serde(default)]
    pub max_concurrency: Option<usize>,
    #[serde(default)]
    pub max_queue: Option<usize>,
    #[serde(default)]
    pub weight: Option<u32>,
    #[serde(default)]
    pub required_region: Option<String>,
}

/// `[capacity.start_rate]`: token bucket in front of every cold start.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct StartRateConfig {
    /// Cold starts per second in the steady state.
    pub per_second: u32,
    /// Cold starts allowed at once from a full bucket.
    pub burst: u32,
}

impl Default for StartRateConfig {
    fn default() -> Self {
        Self {
            per_second: 20,
            burst: 20,
        }
    }
}

/// `[capacity.circuit_breaker]`: per revision, on consecutive boot failures.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    /// Consecutive failed cold starts that open the breaker.
    pub failure_threshold: u32,
    /// Seconds the breaker stays open before one probe start is allowed.
    pub cooldown_seconds: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown_seconds: 30,
        }
    }
}

/// `[capacity.autoscaler]`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct AutoscalerConfig {
    /// Time constant (seconds) of the decaying arrival-rate estimate.
    pub rate_window_seconds: u64,
}

impl Default for AutoscalerConfig {
    fn default() -> Self {
        Self {
            rate_window_seconds: 10,
        }
    }
}

/// Validate the admission sections. Called from
/// [`crate::config::GatewayConfig::validate`].
pub fn validate_admission(
    node: &NodeConfig,
    defaults: &TenantQuotaConfig,
    tenants: &[TenantQuotaEntry],
    start_rate: &StartRateConfig,
    breaker: &CircuitBreakerConfig,
    autoscaler: &AutoscalerConfig,
    max_queue_bytes: u64,
) -> Result<(), String> {
    for (name, v) in [
        ("capacity.node.cpu_millis", node.cpu_millis),
        ("capacity.node.memory_mib", node.memory_mib),
        (
            "capacity.node.ephemeral_storage_mib",
            node.ephemeral_storage_mib,
        ),
    ] {
        if v == Some(0) {
            return Err(format!("{name} must be >= 1 when set"));
        }
    }
    if node.name.is_empty() || node.name.len() > 128 {
        return Err("capacity.node.name must be 1..=128 bytes".into());
    }
    let region_ok = |r: &Option<String>| {
        r.as_deref().is_none_or(|r| {
            !r.is_empty()
                && r.len() <= 64
                && r.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
    };
    if !region_ok(&node.region) || !region_ok(&defaults.required_region) {
        return Err("regions must be 1..=64 bytes of [a-z0-9-]".into());
    }
    let mut seen = std::collections::HashSet::new();
    for t in tenants {
        if !seen.insert(t.tenant_id.clone()) {
            return Err(format!(
                "duplicate [[capacity.tenants]] entry for {}",
                t.tenant_id
            ));
        }
        if !region_ok(&t.required_region) {
            return Err("regions must be 1..=64 bytes of [a-z0-9-]".into());
        }
        if t.max_concurrency == Some(0) || t.weight == Some(0) {
            return Err(format!(
                "capacity.tenants[{}]: max_concurrency and weight must be >= 1",
                t.tenant_id
            ));
        }
    }
    if defaults.max_concurrency == Some(0) || defaults.weight == Some(0) {
        return Err("capacity.tenant_defaults: max_concurrency and weight must be >= 1".into());
    }
    if start_rate.per_second == 0 || start_rate.burst == 0 {
        return Err("capacity.start_rate.per_second and burst must be >= 1".into());
    }
    if breaker.failure_threshold == 0 || breaker.cooldown_seconds == 0 {
        return Err(
            "capacity.circuit_breaker.failure_threshold and cooldown_seconds must be >= 1".into(),
        );
    }
    if autoscaler.rate_window_seconds == 0 {
        return Err("capacity.autoscaler.rate_window_seconds must be >= 1".into());
    }
    if max_queue_bytes == 0 {
        return Err("capacity.max_queue_bytes must be >= 1".into());
    }
    Ok(())
}
