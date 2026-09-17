//! Gateway configuration (`config/gateway.toml`, see docs/architecture.md §4).
//!
//! Secrets and bearer tokens are wrapped in newtypes whose `Debug` output is
//! redacted so that a dumped configuration never leaks them.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use tachyon_serverless_domain::{Limits, TenantId};
use tachyon_serverless_protocol::MAX_FRAME_BYTES;
use tachyon_serverless_provider_port::Role;

pub use crate::services::admission::config::{
    AutoscalerConfig, CircuitBreakerConfig, NodeConfig, StartRateConfig, TenantQuotaConfig,
    TenantQuotaEntry,
};
pub use crate::services::scaling::ScalingConfig;

/// Frame bytes reserved for the `Invoke` / `Response` envelope (ids, event
/// type, deadline, trace id) on top of the payload. `limits.max_payload_bytes`
/// and `limits.max_response_bytes` plus this reserve must fit
/// [`MAX_FRAME_BYTES`], so an accepted payload can always be framed.
pub const FRAME_ENVELOPE_RESERVE_BYTES: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    #[default]
    Dev,
    Production,
}

impl Profile {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Production => "production",
        }
    }
}

/// Provider kinds that may be selected from configuration. The fake provider
/// is deliberately absent: it can only be wired in by code (tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKindConfig {
    Process,
    Firecracker,
}

impl ProviderKindConfig {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Firecracker => "firecracker",
        }
    }
    /// Whether this kind is only acceptable in the `dev` profile.
    pub fn is_dev_only(&self) -> bool {
        matches!(self, Self::Process)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessProviderConfig {
    pub bridge_binary: PathBuf,
    pub workdir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FirecrackerProviderConfig {
    pub firecracker_binary: PathBuf,
    pub kernel: PathBuf,
    pub rootfs: PathBuf,
    pub workdir: PathBuf,
    #[serde(default = "default_vsock_port")]
    pub vsock_port: u32,
    /// `[provider.firecracker.network]`: host network of the egress profiles
    /// `restricted` / `public-web` (PLT-4622). Defaults apply when omitted.
    #[serde(default)]
    pub network: FirecrackerNetworkConfig,
    /// `[provider.firecracker.cgroup]`: host cgroup v2 limits per VMM (PLT-4622).
    #[serde(default)]
    pub cgroup: FirecrackerCgroupConfig,
    /// `[provider.firecracker.jailer]`: launch the VMM through the jailer (PLT-4622).
    #[serde(default)]
    pub jailer: FirecrackerJailerConfig,
}

/// Host cgroup v2 limits of the Firecracker provider. `mode` defaults to
/// `required` under `profile = "production"` and `best-effort` otherwise;
/// production refuses anything but `required`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FirecrackerCgroupConfig {
    /// `required` | `best-effort` | `off`.
    pub mode: Option<String>,
    pub root: Option<PathBuf>,
    pub parent: Option<String>,
    pub memory_overhead_mib: Option<u64>,
    pub pids_max: Option<u64>,
}

/// Firecracker jailer settings (off unless `enabled = true`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FirecrackerJailerConfig {
    #[serde(default)]
    pub enabled: bool,
    pub binary: Option<PathBuf>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub chroot_base: Option<PathBuf>,
    pub new_pid_ns: Option<bool>,
}

/// Accepted values of `[provider.firecracker.cgroup] mode`.
pub const CGROUP_MODES: [&str; 3] = ["required", "best-effort", "off"];

/// Host network settings of the Firecracker provider (docs/adr/0005-egress-profiles.md).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FirecrackerNetworkConfig {
    /// Pool the per-environment /30s are carved from (private range).
    #[serde(default = "default_guest_cidr")]
    pub guest_cidr: String,
    /// The only DNS server a `public-web` guest may query (public IPv4).
    #[serde(default = "default_dns_resolver")]
    pub dns_resolver: std::net::Ipv4Addr,
    #[serde(default = "default_nft_binary")]
    pub nft_binary: PathBuf,
    #[serde(default = "default_ip_binary")]
    pub ip_binary: PathBuf,
}

impl Default for FirecrackerNetworkConfig {
    fn default() -> Self {
        Self {
            guest_cidr: default_guest_cidr(),
            dns_resolver: default_dns_resolver(),
            nft_binary: default_nft_binary(),
            ip_binary: default_ip_binary(),
        }
    }
}

impl FirecrackerNetworkConfig {
    /// Parsed pool, refusing a public or too-small range and a non-public resolver.
    pub fn guest_network(&self) -> Result<tachyon_serverless_domain::Ipv4Cidr, String> {
        use tachyon_serverless_domain::{BLOCKED_IPV4, Ipv4Cidr, is_public_ipv4};
        let pool = Ipv4Cidr::parse(&self.guest_cidr).map_err(|e| e.to_string())?;
        if pool.prefix() > 29 || !BLOCKED_IPV4.iter().any(|b| b.cidr().contains_net(&pool)) {
            return Err(format!(
                "guest_cidr {pool} must be a private range of at least /29 (e.g. 172.30.0.0/16)"
            ));
        }
        if !is_public_ipv4(self.dns_resolver) {
            return Err(format!(
                "dns_resolver {} must be a public unicast address",
                self.dns_resolver
            ));
        }
        Ok(pool)
    }
}

fn default_guest_cidr() -> String {
    "172.30.0.0/16".into()
}
fn default_dns_resolver() -> std::net::Ipv4Addr {
    std::net::Ipv4Addr::new(1, 1, 1, 1)
}
fn default_nft_binary() -> PathBuf {
    PathBuf::from("nft")
}
fn default_ip_binary() -> PathBuf {
    PathBuf::from("ip")
}

fn default_vsock_port() -> u32 {
    tachyon_serverless_protocol::DEFAULT_VSOCK_PORT
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConfig {
    pub kind: ProviderKindConfig,
    #[serde(default)]
    pub process: Option<ProcessProviderConfig>,
    #[serde(default)]
    pub firecracker: Option<FirecrackerProviderConfig>,
}

impl ProviderConfig {
    /// Directory under which the provider keeps per-environment state. Used
    /// to derive the guest working directory for unisolated providers.
    pub fn workdir(&self) -> Option<&Path> {
        match self.kind {
            ProviderKindConfig::Process => self.process.as_ref().map(|p| p.workdir.as_path()),
            ProviderKindConfig::Firecracker => {
                self.firecracker.as_ref().map(|p| p.workdir.as_path())
            }
        }
    }
}

/// `[capacity]`: admission, quotas and autoscaling (PLT-4634,
/// docs/adr/0006-autoscaling-and-admission.md).
#[derive(Debug, Clone, Deserialize)]
pub struct CapacityConfig {
    /// Node-wide upper bound of environments starting or busy at once.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    /// Invocations allowed to wait for capacity; beyond this -> 429.
    #[serde(default = "default_max_queue")]
    pub max_queue: usize,
    #[serde(default = "default_queue_timeout")]
    pub queue_timeout_seconds: u64,
    /// Payload bytes the wait queue may hold in total; beyond this -> 429.
    #[serde(default = "default_max_queue_bytes")]
    pub max_queue_bytes: u64,
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub tenant_defaults: TenantQuotaConfig,
    #[serde(default)]
    pub tenants: Vec<TenantQuotaEntry>,
    #[serde(default)]
    pub start_rate: StartRateConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub autoscaler: AutoscalerConfig,
}

fn default_max_concurrency() -> usize {
    8
}
fn default_max_queue() -> usize {
    32
}
fn default_queue_timeout() -> u64 {
    10
}
fn default_max_queue_bytes() -> u64 {
    32 * 1024 * 1024
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            max_concurrency: default_max_concurrency(),
            max_queue: default_max_queue(),
            queue_timeout_seconds: default_queue_timeout(),
            max_queue_bytes: default_max_queue_bytes(),
            node: NodeConfig::default(),
            tenant_defaults: TenantQuotaConfig::default(),
            tenants: Vec::new(),
            start_rate: StartRateConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            autoscaler: AutoscalerConfig::default(),
        }
    }
}

impl CapacityConfig {
    pub fn queue_timeout(&self) -> Duration {
        Duration::from_secs(self.queue_timeout_seconds)
    }
}

/// Bearer token. `Debug` is redacted.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct ConfigToken(String);

impl ConfigToken {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ConfigToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConfigToken(<redacted>)")
    }
}

/// Secret value from configuration. `Debug` is redacted.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct ConfigSecret(String);

impl ConfigSecret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ConfigSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConfigSecret(<redacted>)")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenConfig {
    pub token: ConfigToken,
    pub tenant_id: TenantId,
    pub subject: String,
    pub roles: Vec<Role>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct IdentityConfig {
    #[serde(default)]
    pub tokens: Vec<TokenConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecretBindingConfig {
    pub tenant_id: TenantId,
    pub binding_ref: String,
    pub value: ConfigSecret,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SecretsConfig {
    #[serde(default)]
    pub bindings: Vec<SecretBindingConfig>,
}

/// Optional overrides of [`Limits`]. Unset fields keep the defaults.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LimitsOverrides {
    pub max_payload_bytes: Option<u64>,
    pub max_response_bytes: Option<u64>,
    pub max_execution_timeout_seconds: Option<u32>,
    pub max_init_timeout_seconds: Option<u32>,
    pub min_memory_mib: Option<u32>,
    pub max_memory_mib: Option<u32>,
    pub min_cpu_millis: Option<u32>,
    pub max_cpu_millis: Option<u32>,
    pub min_ephemeral_storage_mib: Option<u32>,
    pub max_ephemeral_storage_mib: Option<u32>,
    pub max_log_lines_per_invocation: Option<u32>,
    pub max_log_bytes_per_invocation: Option<u64>,
    pub max_log_line_bytes: Option<usize>,
    pub max_artifact_bytes: Option<u64>,
}

impl LimitsOverrides {
    pub fn apply(&self, mut limits: Limits) -> Limits {
        macro_rules! set {
            ($($field:ident),*) => {
                $( if let Some(v) = self.$field { limits.$field = v; } )*
            };
        }
        set!(
            max_payload_bytes,
            max_response_bytes,
            max_execution_timeout_seconds,
            max_init_timeout_seconds,
            min_memory_mib,
            max_memory_mib,
            min_cpu_millis,
            max_cpu_millis,
            min_ephemeral_storage_mib,
            max_ephemeral_storage_mib,
            max_log_lines_per_invocation,
            max_log_bytes_per_invocation,
            max_log_line_bytes,
            max_artifact_bytes
        );
        limits
    }
}

/// Invoke pipeline tunables.
#[derive(Debug, Clone, Deserialize)]
pub struct InvokeConfig {
    /// Outputs up to this size are stored inline in the invocation ledger.
    #[serde(default = "default_inline_output_max")]
    pub inline_output_max_bytes: u64,
    /// Grace given to the guest after `Cancel` before the environment is killed.
    #[serde(default = "default_cancel_grace_ms")]
    pub cancel_grace_ms: u64,
    /// How long a provider preflight result is cached.
    #[serde(default = "default_preflight_ttl")]
    pub preflight_ttl_seconds: u64,
    /// Maximum wait for a bridge `Hello` after the provider reported the
    /// connection as accepted.
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,
}

fn default_inline_output_max() -> u64 {
    64 * 1024
}
fn default_cancel_grace_ms() -> u64 {
    1000
}
fn default_preflight_ttl() -> u64 {
    30
}
fn default_handshake_timeout_ms() -> u64 {
    5000
}

impl Default for InvokeConfig {
    fn default() -> Self {
        Self {
            inline_output_max_bytes: default_inline_output_max(),
            cancel_grace_ms: default_cancel_grace_ms(),
            preflight_ttl_seconds: default_preflight_ttl(),
            handshake_timeout_ms: default_handshake_timeout_ms(),
        }
    }
}

impl InvokeConfig {
    pub fn cancel_grace(&self) -> Duration {
        Duration::from_millis(self.cancel_grace_ms)
    }
    pub fn preflight_ttl(&self) -> Duration {
        Duration::from_secs(self.preflight_ttl_seconds)
    }
    pub fn handshake_timeout(&self) -> Duration {
        Duration::from_millis(self.handshake_timeout_ms)
    }
}

/// Environment pool / warm reuse (docs/architecture.md §4).
///
/// This is only one half of the gate. Reuse also requires the provider to
/// report both `idle_quiesce` and `idle_resume` as `Supported`; `Unverified`
/// is not enough unless [`PoolConfig::allow_unverified_idle`] is set for a
/// measurement. The process provider reports `Unsupported` and the Firecracker
/// provider `Unverified`, so with the defaults neither of them pools anything
/// and both keep destroying the environment after every invocation.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PoolConfig {
    /// Allow environments to be reused. Off by default.
    pub enabled: bool,
    /// Idle environments kept per reuse key.
    pub max_idle_per_revision: usize,
    /// An environment idle for longer than this is terminated by the sweeper.
    pub idle_ttl_seconds: u64,
    /// Idle environments kept across all reuse keys.
    pub max_total_idle: usize,
    /// **Measurement only.** Accept `Unverified` for `idle_quiesce` and
    /// `idle_resume` instead of requiring `Supported`.
    ///
    /// It exists because of a chicken and egg: a provider may only report
    /// those capabilities as `Supported` once a real pause/resume cycle has
    /// been measured, and the measurement cannot be taken while the pool
    /// refuses to reuse anything. Switching this on lets
    /// `scripts/kvm/measure-warm.sh` take it.
    ///
    /// It never turns an unverified configuration into a verified one: with
    /// this set, `GET /v1/provider` reports `reuse.verified = false` and the
    /// bootstrap log warns, so a measurement run can never be mistaken for a
    /// warm success (PLT-4633 acceptance 4). Off by default.
    ///
    /// **Refused under `profile = "production"`**, exactly like a dev-only
    /// provider ([`GatewayConfig::validate`]). Running unmeasured pause/resume
    /// code against production traffic is the thing the capability gate exists
    /// to prevent, and a warning is not a gate: nobody reads the log of a
    /// gateway that started successfully (PLT-4633 review F8,
    /// docs/architecture.md §4).
    pub allow_unverified_idle: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_idle_per_revision: 1,
            idle_ttl_seconds: 60,
            max_total_idle: 8,
            allow_unverified_idle: false,
        }
    }
}

impl PoolConfig {
    pub fn idle_ttl(&self) -> Duration {
        Duration::from_secs(self.idle_ttl_seconds)
    }

    /// The same TTL as a `chrono` duration, for comparing ledger timestamps.
    pub fn idle_ttl_chrono(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.idle_ttl_seconds.min(i64::MAX as u64) as i64)
    }
}

/// Which store backs the control-plane ledger (docs/adr/0003).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum StoreBackend {
    /// `<data_dir>/state.db`: embedded SQLite, WAL, forward-only migrations.
    #[default]
    Sqlite,
    /// Volatile: nothing survives the process. For throwaway runs and tests.
    Memory,
}

impl StoreBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Memory => "memory",
        }
    }
}

/// `[store]`: where the ledger lives and how long invocation bodies stay.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    pub backend: StoreBackend,
    /// Seconds an inline invocation output (at most
    /// `[invoke] inline_output_max_bytes`) is kept after the invocation
    /// finished. After that only its digest and size remain. `0` keeps it for
    /// as long as the row exists.
    pub output_retention_seconds: u64,
    /// Seconds an `Idempotency-Key` keeps answering after its invocation
    /// finished (PLT-4631). A key never expires while its invocation is in
    /// flight. After that the key can be used for a new invocation and the
    /// binding is purged. `0` keeps it for as long as the row exists.
    pub idempotency_retention_seconds: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            backend: StoreBackend::Sqlite,
            output_retention_seconds: 7 * 24 * 60 * 60,
            idempotency_retention_seconds: 24 * 60 * 60,
        }
    }
}

fn seconds(n: u64) -> Option<chrono::Duration> {
    (n > 0).then(|| chrono::Duration::seconds(n.min(i64::MAX as u64 / 1000) as i64))
}

impl StoreConfig {
    pub fn output_retention(&self) -> Option<chrono::Duration> {
        seconds(self.output_retention_seconds)
    }

    pub fn idempotency_retention(&self) -> Option<chrono::Duration> {
        seconds(self.idempotency_retention_seconds)
    }
}

/// Dispatcher identity and ownership leases (PLT-4631,
/// docs/architecture.md §4).
///
/// Every gateway process registers as a new dispatcher in the store, renews
/// its lease (and the slot leases of its in-flight attempts) every
/// `heartbeat_interval_seconds`, and only reclaims the work of another
/// dispatcher once that one's lease is `lease_ttl_seconds` old plus
/// `max_clock_skew_ms`, or once it stopped, or once it is the previous
/// incarnation of the same `instance` on the same host and its process is
/// gone.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DispatcherConfig {
    /// Stable name of this gateway instance. Defaults to `gateway@<listen>`.
    /// Two gateways that share a `data_dir` must not share an instance name.
    pub instance: Option<String>,
    pub lease_ttl_seconds: u64,
    pub heartbeat_interval_seconds: u64,
    /// Clock difference tolerated between dispatchers on the host.
    pub max_clock_skew_ms: u64,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            instance: None,
            lease_ttl_seconds: 30,
            heartbeat_interval_seconds: 10,
            max_clock_skew_ms: 2_000,
        }
    }
}

impl DispatcherConfig {
    pub fn lease_ttl(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.lease_ttl_seconds.min(i64::MAX as u64 / 1000) as i64)
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.heartbeat_interval_seconds)
    }

    pub fn max_clock_skew(&self) -> chrono::Duration {
        chrono::Duration::milliseconds(self.max_clock_skew_ms.min(i64::MAX as u64) as i64)
    }
}

/// Startup reconciliation (docs/architecture.md §4).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReconcileConfig {
    /// Ask the provider what it still runs when the gateway starts and
    /// terminate every environment this gateway does not know as active.
    pub on_startup: bool,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self { on_startup: true }
    }
}

/// Which half of the control-plane / data-plane split this gateway runs
/// (PLT-4636, docs/adr/0007-config-distribution-and-auth-leases.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GatewayRole {
    /// Management API, invoke and (when `internal_token` is set) the internal
    /// config endpoint for data planes. Invoke still reads its configuration
    /// through the cache, fed in-process from the ledger.
    #[default]
    Combined,
    /// Invoke only. Functions, routes, revisions, authorization grants and
    /// policy come from a management gateway at `url`; the management API
    /// answers 503.
    DataPlane,
}

impl GatewayRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Combined => "combined",
            Self::DataPlane => "data_plane",
        }
    }
}

/// `[control_plane]`: configuration distribution and authorization leases
/// (PLT-4636).
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlPlaneConfig {
    pub role: GatewayRole,
    /// Base URL of the management gateway (`data_plane` only).
    pub url: Option<String>,
    /// Credential of `GET /v1/internal/config`. A `combined` gateway serves
    /// the endpoint only when this is set; a `data_plane` presents it. Also
    /// the key under which bearer tokens are digested for distribution.
    pub internal_token: Option<ConfigToken>,
    /// Refresh period while the control plane answers.
    pub refresh_interval_ms: u64,
    /// How long a delivered function / route / revision / policy stays valid
    /// after the refresh that last confirmed it.
    pub config_ttl_seconds: u64,
    /// How long a delivered authorization grant (bearer token -> tenant,
    /// roles) stays valid after the refresh that last confirmed it: the auth
    /// lease, and the upper bound of how long a revoked token keeps working
    /// on a data plane that cannot reach the control plane.
    pub auth_lease_seconds: u64,
    /// An entry not confirmed for longer than this is reported as
    /// stale-but-valid. Defaults to three refresh intervals.
    pub stale_after_ms: Option<u64>,
    /// Retry backoff after a failed refresh: doubles from `backoff_initial_ms`
    /// up to `backoff_max_ms`.
    pub backoff_initial_ms: u64,
    pub backoff_max_ms: u64,
    /// Upper bound of one fetch from the control plane.
    pub fetch_timeout_ms: u64,
    /// Policy published by a `combined` gateway: egress profiles a revision
    /// may run with.
    pub allowed_egress: Vec<tachyon_serverless_domain::EgressProfile>,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        use tachyon_serverless_domain::EgressProfile;
        Self {
            role: GatewayRole::Combined,
            url: None,
            internal_token: None,
            refresh_interval_ms: 2_000,
            config_ttl_seconds: 60,
            auth_lease_seconds: 60,
            stale_after_ms: None,
            backoff_initial_ms: 500,
            backoff_max_ms: 10_000,
            fetch_timeout_ms: 2_000,
            allowed_egress: vec![
                EgressProfile::None,
                EgressProfile::Restricted,
                EgressProfile::PublicWeb,
            ],
        }
    }
}

impl ControlPlaneConfig {
    /// Shortest accepted internal credential.
    pub const MIN_INTERNAL_TOKEN_LEN: usize = 16;

    pub fn refresh_interval(&self) -> Duration {
        Duration::from_millis(self.refresh_interval_ms)
    }
    pub fn config_ttl(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.config_ttl_seconds.min(i64::MAX as u64 / 1000) as i64)
    }
    pub fn auth_lease(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.auth_lease_seconds.min(i64::MAX as u64 / 1000) as i64)
    }
    pub fn stale_after(&self) -> chrono::Duration {
        let ms = self
            .stale_after_ms
            .unwrap_or(self.refresh_interval_ms.saturating_mul(3));
        chrono::Duration::milliseconds(ms.min(i64::MAX as u64 / 2) as i64)
    }
    pub fn backoff_initial(&self) -> Duration {
        Duration::from_millis(self.backoff_initial_ms)
    }
    pub fn backoff_max(&self) -> Duration {
        Duration::from_millis(self.backoff_max_ms)
    }
    pub fn fetch_timeout(&self) -> Duration {
        Duration::from_millis(self.fetch_timeout_ms)
    }
}

/// `[control_plane_outage]`: what a gateway still starts while its control
/// plane is unreachable (PLT-4636).
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlPlaneOutageConfig {
    /// Boot new environments while the control plane is unreachable, as long
    /// as the revision and the authorization lease are still valid. `false`
    /// lets only already-running (pooled) environments serve during an
    /// outage.
    pub allow_cold_start: bool,
}

impl Default for ControlPlaneOutageConfig {
    fn default() -> Self {
        Self {
            allow_cold_start: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default)]
    pub profile: Profile,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    pub provider: ProviderConfig,
    #[serde(default)]
    pub capacity: CapacityConfig,
    #[serde(default)]
    pub identity: IdentityConfig,
    #[serde(default)]
    pub secrets: SecretsConfig,
    #[serde(default)]
    pub limits: LimitsOverrides,
    #[serde(default)]
    pub invoke: InvokeConfig,
    #[serde(default)]
    pub reconcile: ReconcileConfig,
    #[serde(default)]
    pub pool: PoolConfig,
    /// Scale-to-zero, `min_ready`, cooldown and drains (PLT-4635).
    #[serde(default)]
    pub scaling: ScalingConfig,
    #[serde(default)]
    pub store: StoreConfig,
    #[serde(default)]
    pub dispatcher: DispatcherConfig,
    #[serde(default)]
    pub control_plane: ControlPlaneConfig,
    #[serde(default)]
    pub control_plane_outage: ControlPlaneOutageConfig,
    /// `[queue]` (PLT-4638). Off by default.
    #[serde(default)]
    pub queue: crate::durable::QueueConfig,
    /// `[objects]` (PLT-4638). Off by default.
    #[serde(default)]
    pub objects: crate::durable::ObjectsConfig,
    /// `[invoke_async]` (PLT-4639): input storage, outbox bounds and the
    /// publisher. Used only when `[queue]` selects a queue.
    #[serde(default)]
    pub invoke_async: crate::services::invoke_async::InvokeAsyncConfig,
}

fn default_listen() -> String {
    "127.0.0.1:8080".to_string()
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

impl GatewayConfig {
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        let mut cfg: Self = toml::from_str(text)?;
        if let Some(f) = cfg.provider.firecracker.as_mut()
            && f.cgroup.mode.is_none()
        {
            let mode = if cfg.profile == Profile::Production {
                "required"
            } else {
                "best-effort"
            };
            f.cgroup.mode = Some(mode.into());
        }
        cfg.validate()?;
        let base = std::env::current_dir().map_err(|source| ConfigError::Read {
            path: PathBuf::from("."),
            source,
        })?;
        cfg.absolutize_paths(&base);
        Ok(cfg)
    }

    /// Make every filesystem path absolute relative to `base`.
    ///
    /// Artifact paths and working directories are handed to other processes
    /// (the runtime bridge, the user function) whose current directory differs
    /// from the gateway's, so relative paths must be resolved once, here.
    /// Bare command names (no path separator) are left for `PATH` lookup.
    pub fn absolutize_paths(&mut self, base: &Path) {
        fn abs(base: &Path, p: &mut PathBuf) {
            if p.is_relative() {
                *p = base.join(&*p);
            }
        }
        fn abs_binary(base: &Path, p: &mut PathBuf) {
            if p.components().count() > 1 {
                abs(base, p);
            }
        }
        abs(base, &mut self.data_dir);
        if let Some(p) = self.provider.process.as_mut() {
            abs_binary(base, &mut p.bridge_binary);
            abs(base, &mut p.workdir);
        }
        if let Some(f) = self.provider.firecracker.as_mut() {
            abs_binary(base, &mut f.firecracker_binary);
            abs(base, &mut f.kernel);
            abs(base, &mut f.rootfs);
            abs(base, &mut f.workdir);
            if let Some(b) = f.jailer.binary.as_mut() {
                abs_binary(base, b);
            }
            if let Some(c) = f.jailer.chroot_base.as_mut() {
                abs(base, c);
            }
        }
        crate::durable::config::absolutize(&mut self.queue, &mut self.objects, base);
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml(&text)
    }

    /// Effective platform limits (defaults + overrides).
    pub fn effective_limits(&self) -> Limits {
        self.limits.apply(Limits::default())
    }

    /// `[control_plane]` (PLT-4636).
    fn validate_control_plane(&self) -> Result<(), ConfigError> {
        let cp = &self.control_plane;
        let invalid = |m: &str| Err(ConfigError::Invalid(format!("[control_plane] {m}")));
        if cp.config_ttl_seconds == 0 || cp.auth_lease_seconds == 0 {
            return invalid("config_ttl_seconds and auth_lease_seconds must be >= 1");
        }
        let shortest_ms = cp
            .config_ttl_seconds
            .min(cp.auth_lease_seconds)
            .saturating_mul(1000);
        if cp.refresh_interval_ms == 0 || cp.refresh_interval_ms >= shortest_ms {
            return invalid(
                "needs 0 < refresh_interval_ms < min(config_ttl_seconds, auth_lease_seconds) * 1000: \
                 an entry must be confirmed at least once before it expires",
            );
        }
        if cp.backoff_initial_ms == 0 || cp.backoff_initial_ms > cp.backoff_max_ms {
            return invalid("needs 0 < backoff_initial_ms <= backoff_max_ms");
        }
        if cp.fetch_timeout_ms == 0 {
            return invalid("fetch_timeout_ms must be >= 1");
        }
        if let Some(t) = &cp.internal_token
            && t.expose().len() < ControlPlaneConfig::MIN_INTERNAL_TOKEN_LEN
        {
            return Err(ConfigError::Invalid(format!(
                "[control_plane] internal_token must be at least {} bytes",
                ControlPlaneConfig::MIN_INTERNAL_TOKEN_LEN
            )));
        }
        if cp.role == GatewayRole::DataPlane {
            match cp.url.as_deref() {
                Some(u) if u.starts_with("http://") || u.starts_with("https://") => {}
                _ => {
                    return invalid(
                        "role = \"data_plane\" needs url = \"http(s)://<management gateway>\"",
                    );
                }
            }
            if cp.internal_token.is_none() {
                return invalid("role = \"data_plane\" needs internal_token");
            }
            if !self.identity.tokens.is_empty() {
                return invalid(
                    "role = \"data_plane\" takes its authorization grants from the control plane; \
                     remove [[identity.tokens]]",
                );
            }
        }
        Ok(())
    }

    /// Static validation. Provider-capability checks (`dev_only`) that need
    /// a constructed provider are performed again at bootstrap.
    pub fn validate(&self) -> Result<(), ConfigError> {
        match self.provider.kind {
            ProviderKindConfig::Process if self.provider.process.is_none() => {
                return Err(ConfigError::Invalid(
                    "[provider.process] is required when provider.kind = \"process\"".into(),
                ));
            }
            ProviderKindConfig::Firecracker if self.provider.firecracker.is_none() => {
                return Err(ConfigError::Invalid(
                    "[provider.firecracker] is required when provider.kind = \"firecracker\""
                        .into(),
                ));
            }
            _ => {}
        }
        if let Some(f) = &self.provider.firecracker
            && let Err(e) = f.network.guest_network()
        {
            return Err(ConfigError::Invalid(format!(
                "[provider.firecracker.network]: {e}"
            )));
        }
        if let Some(f) = &self.provider.firecracker {
            // PLT-4622: production never runs a VMM without host limits.
            match f.cgroup.mode.as_deref() {
                None => {}
                Some(m) if !CGROUP_MODES.contains(&m) => {
                    return Err(ConfigError::Invalid(format!(
                        "[provider.firecracker.cgroup] mode `{m}` must be one of {CGROUP_MODES:?}"
                    )));
                }
                Some(m) if m != "required" && self.profile == Profile::Production => {
                    return Err(ConfigError::Invalid(format!(
                        "[provider.firecracker.cgroup] mode `{m}` is not allowed with profile = \
                         \"production\": host cgroup limits must be `required`"
                    )));
                }
                Some(_) => {}
            }
            if f.jailer.enabled && (f.jailer.uid == Some(0) || f.jailer.gid == Some(0)) {
                return Err(ConfigError::Invalid(
                    "[provider.firecracker.jailer] uid / gid must not be 0".into(),
                ));
            }
        }
        if self.profile == Profile::Production && self.provider.kind.is_dev_only() {
            return Err(ConfigError::Invalid(format!(
                "provider `{}` is dev-only and cannot be used with profile = \"production\"",
                self.provider.kind.as_str()
            )));
        }
        // The measurement switch is refused under production for the same
        // reason a dev-only provider is: it runs code whose behaviour nobody
        // has measured on real hardware, and the environment pool is exactly
        // where that shows up as a hung invocation rather than as a warning
        // (PLT-4633 review F8). Measurements are taken under `profile = "dev"`
        // (`scripts/kvm/measure-warm.sh`, docs/kvm.md §3.7).
        if self.profile == Profile::Production && self.pool.allow_unverified_idle {
            return Err(ConfigError::Invalid(
                "[pool] allow_unverified_idle is a measurement-only switch and cannot be used \
                 with profile = \"production\": it accepts an idle capability nobody has \
                 measured. Take the measurement under profile = \"dev\" \
                 (scripts/kvm/measure-warm.sh), then promote the capability"
                    .into(),
            ));
        }
        if self.dispatcher.lease_ttl_seconds == 0
            || self.dispatcher.heartbeat_interval_seconds == 0
            || self.dispatcher.heartbeat_interval_seconds >= self.dispatcher.lease_ttl_seconds
        {
            return Err(ConfigError::Invalid(
                "[dispatcher] needs 0 < heartbeat_interval_seconds < lease_ttl_seconds".into(),
            ));
        }
        if self
            .dispatcher
            .instance
            .as_deref()
            .is_some_and(|i| i.is_empty() || i.len() > 256)
        {
            return Err(ConfigError::Invalid(
                "[dispatcher] instance must be 1..=256 bytes".into(),
            ));
        }
        self.validate_control_plane()?;
        if self.capacity.max_concurrency == 0 {
            return Err(ConfigError::Invalid(
                "capacity.max_concurrency must be >= 1".into(),
            ));
        }
        if self.capacity.queue_timeout_seconds == 0 {
            return Err(ConfigError::Invalid(
                "capacity.queue_timeout_seconds must be >= 1".into(),
            ));
        }
        let c = &self.capacity;
        crate::services::admission::config::validate_admission(
            &c.node,
            &c.tenant_defaults,
            &c.tenants,
            &c.start_rate,
            &c.circuit_breaker,
            &c.autoscaler,
            c.max_queue_bytes,
        )
        .map_err(ConfigError::Invalid)?;
        let mut seen = HashSet::new();
        for t in &self.identity.tokens {
            if t.token.expose().is_empty() {
                return Err(ConfigError::Invalid(
                    "identity.tokens[].token must not be empty".into(),
                ));
            }
            if !seen.insert(t.token.expose().to_string()) {
                return Err(ConfigError::Invalid(
                    "identity.tokens[].token values must be unique".into(),
                ));
            }
            if t.roles.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "identity token for subject `{}` has no roles",
                    t.subject
                )));
            }
        }
        let mut bindings = HashSet::new();
        for b in &self.secrets.bindings {
            if b.binding_ref.is_empty() {
                return Err(ConfigError::Invalid(
                    "secrets.bindings[].binding_ref must not be empty".into(),
                ));
            }
            if !bindings.insert((b.tenant_id.clone(), b.binding_ref.clone())) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate secret binding `{}` for tenant {}",
                    b.binding_ref, b.tenant_id
                )));
            }
        }
        let limits = self.effective_limits();
        if limits.max_payload_bytes == 0 || limits.max_artifact_bytes == 0 {
            return Err(ConfigError::Invalid(
                "limits.max_payload_bytes and limits.max_artifact_bytes must be > 0".into(),
            ));
        }
        let max_frame = MAX_FRAME_BYTES as u64;
        for (name, value) in [
            ("limits.max_payload_bytes", limits.max_payload_bytes),
            ("limits.max_response_bytes", limits.max_response_bytes),
        ] {
            if value.saturating_add(FRAME_ENVELOPE_RESERVE_BYTES) > max_frame {
                return Err(ConfigError::Invalid(format!(
                    "{name} ({value}) plus {FRAME_ENVELOPE_RESERVE_BYTES} bytes of envelope \
                     headroom must fit in a protocol frame ({max_frame} bytes)"
                )));
            }
        }
        if self.invoke.inline_output_max_bytes > limits.max_response_bytes {
            return Err(ConfigError::Invalid(
                "invoke.inline_output_max_bytes must be <= limits.max_response_bytes".into(),
            ));
        }
        crate::durable::config::validate(
            &self.queue,
            &self.objects,
            self.profile == Profile::Production,
        )
        .map_err(ConfigError::Invalid)?;
        self.invoke_async.validate().map_err(ConfigError::Invalid)?;
        if self.pool.enabled {
            if self.pool.max_idle_per_revision == 0 {
                return Err(ConfigError::Invalid(
                    "pool.max_idle_per_revision must be >= 1 when the pool is enabled".into(),
                ));
            }
            if self.pool.idle_ttl_seconds == 0 {
                return Err(ConfigError::Invalid(
                    "pool.idle_ttl_seconds must be >= 1 when the pool is enabled".into(),
                ));
            }
            if self.pool.max_total_idle < self.pool.max_idle_per_revision {
                return Err(ConfigError::Invalid(format!(
                    "pool.max_total_idle ({}) must be >= pool.max_idle_per_revision ({})",
                    self.pool.max_total_idle, self.pool.max_idle_per_revision
                )));
            }
        }
        self.scaling
            .validate(
                limits.max_execution_timeout_seconds,
                self.invoke.cancel_grace(),
            )
            .map_err(ConfigError::Invalid)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEV: &str = r#"
listen = "127.0.0.1:8080"
profile = "dev"
data_dir = "./data"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "./data/process"

[capacity]
max_concurrency = 8
max_queue = 32
queue_timeout_seconds = 10

[[identity.tokens]]
token = "dev-token-tenant-a"
tenant_id = "tn_01hzzzzzzzzzzzzzzzzzzzzzza"
subject = "dev-a"
roles = ["deploy", "invoke"]

[[secrets.bindings]]
tenant_id = "tn_01hzzzzzzzzzzzzzzzzzzzzzza"
binding_ref = "demo-secret"
value = "demo-secret-value-a"
"#;

    #[test]
    fn parses_dev_config_and_redacts_secrets() {
        let cfg = GatewayConfig::from_toml(DEV).unwrap();
        assert_eq!(cfg.profile, Profile::Dev);
        assert_eq!(cfg.provider.kind, ProviderKindConfig::Process);
        assert_eq!(
            cfg.identity.tokens[0].roles,
            vec![Role::Deploy, Role::Invoke]
        );
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("demo-secret-value-a"));
        assert!(!dbg.contains("dev-token-tenant-a"));
        assert_eq!(cfg.effective_limits(), Limits::default());
    }

    #[test]
    fn production_rejects_dev_only_provider() {
        let text = DEV.replace("profile = \"dev\"", "profile = \"production\"");
        let err = GatewayConfig::from_toml(&text).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)), "{err}");
    }

    /// The dev config with a production profile and a provider that is allowed
    /// there, so the only thing under test is the `[pool]` section.
    fn production_firecracker(pool: &str) -> String {
        let base = DEV
            .replace("profile = \"dev\"", "profile = \"production\"")
            .replace(
                "kind = \"process\"\n\n[provider.process]\n\
                 bridge_binary = \"target/debug/tachyon-serverless-runtime-bridge\"\n\
                 workdir = \"./data/process\"",
                "kind = \"firecracker\"\n\n[provider.firecracker]\n\
                 firecracker_binary = \".kvm/bin/firecracker\"\n\
                 kernel = \".kvm/vmlinux\"\n\
                 rootfs = \".kvm/rootfs.ext4\"\n\
                 workdir = \".kvm/run\"",
            );
        format!("{base}\n{pool}")
    }

    /// PLT-4622: host cgroup limits default to required in production, which
    /// refuses weaker modes; dev defaults to best-effort. The jailer is opt-in.
    #[test]
    fn firecracker_cgroup_and_jailer_sections() {
        let prod = GatewayConfig::from_toml(&production_firecracker("")).unwrap();
        let f = prod.provider.firecracker.as_ref().unwrap();
        assert_eq!(f.cgroup.mode.as_deref(), Some("required"));
        assert!(!f.jailer.enabled);
        for weak in ["best-effort", "off"] {
            let toml = production_firecracker(&format!(
                "[provider.firecracker.cgroup]\nmode = \"{weak}\"\n"
            ));
            assert!(GatewayConfig::from_toml(&toml).is_err(), "{weak}");
        }
        let dev =
            production_firecracker("").replace("profile = \"production\"", "profile = \"dev\"");
        let cfg = GatewayConfig::from_toml(&dev).unwrap();
        assert_eq!(
            cfg.provider
                .firecracker
                .as_ref()
                .unwrap()
                .cgroup
                .mode
                .as_deref(),
            Some("best-effort")
        );
        assert!(
            GatewayConfig::from_toml(&format!(
                "{dev}\n[provider.firecracker.cgroup]\nmode = \"sometimes\"\n"
            ))
            .is_err()
        );
        let jailed = GatewayConfig::from_toml(&production_firecracker(
            "[provider.firecracker.jailer]\nenabled = true\nbinary = \".kvm/bin/jailer\"\nuid = 64000\ngid = 64000\n",
        ))
        .unwrap();
        let j = &jailed.provider.firecracker.as_ref().unwrap().jailer;
        assert!(j.enabled && j.binary.as_ref().unwrap().is_absolute());
        assert!(
            GatewayConfig::from_toml(&production_firecracker(
                "[provider.firecracker.jailer]\nenabled = true\nuid = 0\n"
            ))
            .is_err()
        );
    }

    /// PLT-4622: the egress network section has safe defaults and refuses a
    /// public guest pool or a resolver on the management network.
    #[test]
    fn firecracker_network_section_defaults_and_refuses_unsafe_values() {
        let cfg = GatewayConfig::from_toml(&production_firecracker("")).unwrap();
        let net = &cfg.provider.firecracker.as_ref().unwrap().network;
        assert_eq!(net, &FirecrackerNetworkConfig::default());
        assert_eq!(net.guest_network().unwrap().to_string(), "172.30.0.0/16");
        assert_eq!(net.dns_resolver, std::net::Ipv4Addr::new(1, 1, 1, 1));

        let custom = production_firecracker(
            "[provider.firecracker.network]\nguest_cidr = \"10.200.0.0/20\"\ndns_resolver = \"9.9.9.9\"\n",
        );
        let cfg = GatewayConfig::from_toml(&custom).unwrap();
        let net = &cfg.provider.firecracker.as_ref().unwrap().network;
        assert_eq!(net.guest_network().unwrap().to_string(), "10.200.0.0/20");

        for bad in [
            "guest_cidr = \"8.8.0.0/16\"",
            "guest_cidr = \"172.30.0.0/30\"",
            "dns_resolver = \"192.168.5.3\"",
            "unknown_key = 1",
        ] {
            let toml = production_firecracker(&format!("[provider.firecracker.network]\n{bad}\n"));
            assert!(GatewayConfig::from_toml(&toml).is_err(), "{bad}");
        }
    }

    /// PLT-4633 (review F8): the measurement switch is refused under
    /// `profile = "production"`, like a dev-only provider.
    ///
    /// It accepts an idle capability nobody has measured, which is the one
    /// thing the capability gate exists to keep away from production traffic;
    /// a warning would not, because nobody reads the log of a gateway that
    /// started successfully. The message says where the measurement belongs.
    #[test]
    fn the_measurement_switch_is_refused_under_production() {
        // The same configuration without the switch is accepted, so the switch
        // is what this rejects rather than the profile or the provider.
        let plain = production_firecracker("[pool]\nenabled = true\n");
        let cfg = GatewayConfig::from_toml(&plain).expect("a production pool config is fine");
        assert_eq!(cfg.profile, Profile::Production);
        assert!(cfg.pool.enabled && !cfg.pool.allow_unverified_idle);

        let measuring =
            production_firecracker("[pool]\nenabled = true\nallow_unverified_idle = true\n");
        let err = GatewayConfig::from_toml(&measuring).unwrap_err();
        let ConfigError::Invalid(message) = &err else {
            panic!("expected an invalid-config error, got {err}");
        };
        assert!(message.contains("allow_unverified_idle"), "{message}");
        assert!(message.contains("production"), "{message}");
        assert!(
            message.contains("dev"),
            "the message says where to take the measurement instead: {message}"
        );

        // And it is accepted under dev, which is where measurements are taken.
        let dev = format!("{DEV}\n[pool]\nenabled = true\nallow_unverified_idle = true\n");
        assert!(GatewayConfig::from_toml(&dev).is_ok());
    }

    /// PLT-4634: the admission sections default to the old behaviour, parse,
    /// and refuse values that would disable a limit by accident.
    #[test]
    fn capacity_sections_default_parse_and_validate() {
        let cfg = GatewayConfig::from_toml(DEV).unwrap().capacity;
        assert_eq!(
            cfg.node.memory_mib, None,
            "no resource bound unless configured"
        );
        assert_eq!(cfg.node.overhead().memory_mib, 24);
        assert!(cfg.tenants.is_empty() && cfg.tenant_defaults.max_concurrency.is_none());

        let text = format!(
            "{DEV}\n[capacity.node]\nname = \"n1\"\nregion = \"jp\"\nmemory_mib = 4096\n\
             vmm_overhead_memory_mib = 30\n\
             [capacity.tenant_defaults]\nmax_concurrency = 4\n\
             [[capacity.tenants]]\ntenant_id = \"tn_01hzzzzzzzzzzzzzzzzzzzzzza\"\nrequired_region = \"jp\"\n\
             [capacity.start_rate]\nper_second = 3\nburst = 5\n\
             [capacity.circuit_breaker]\nfailure_threshold = 2\ncooldown_seconds = 9\n"
        );
        let cfg = GatewayConfig::from_toml(&text).unwrap().capacity;
        assert_eq!(cfg.node.region.as_deref(), Some("jp"));
        assert_eq!(cfg.node.overhead().memory_mib, 38);
        assert_eq!(cfg.tenants[0].required_region.as_deref(), Some("jp"));
        assert_eq!(cfg.start_rate.burst, 5);
        assert_eq!(cfg.circuit_breaker.cooldown_seconds, 9);

        for bad in [
            "[capacity.node]\nmemory_mib = 0\n",
            "[capacity.node]\nregion = \"JP only\"\n",
            "[capacity.node]\nbogus = 1\n",
            "[capacity.start_rate]\nper_second = 0\n",
            "[capacity.circuit_breaker]\nfailure_threshold = 0\n",
            "[capacity.tenant_defaults]\nmax_concurrency = 0\n",
            "[[capacity.tenants]]\ntenant_id = \"tn_01hzzzzzzzzzzzzzzzzzzzzzza\"\n\
             [[capacity.tenants]]\ntenant_id = \"tn_01hzzzzzzzzzzzzzzzzzzzzzza\"\n",
        ] {
            assert!(
                GatewayConfig::from_toml(&format!("{DEV}\n{bad}")).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn fake_provider_cannot_be_selected() {
        let text = DEV.replace("kind = \"process\"", "kind = \"fake\"");
        assert!(matches!(
            GatewayConfig::from_toml(&text),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn invalid_tenant_id_is_rejected() {
        let text = DEV.replace("tn_01hzzzzzzzzzzzzzzzzzzzzzza", "tn_01HZZZ");
        assert!(GatewayConfig::from_toml(&text).is_err());
    }

    #[test]
    fn limits_overrides_apply() {
        let text = format!("{DEV}\n[limits]\nmax_payload_bytes = 10\n");
        let cfg = GatewayConfig::from_toml(&text).unwrap();
        assert_eq!(cfg.effective_limits().max_payload_bytes, 10);
        assert_eq!(
            cfg.effective_limits().max_response_bytes,
            Limits::default().max_response_bytes
        );
    }

    #[test]
    fn limits_must_leave_frame_headroom() {
        let max = MAX_FRAME_BYTES as u64;
        for field in ["max_payload_bytes", "max_response_bytes"] {
            let too_big = format!("{DEV}\n[limits]\n{field} = {max}\n");
            let err = GatewayConfig::from_toml(&too_big).unwrap_err();
            assert!(err.to_string().contains(field), "{err}");
            let fits = format!(
                "{DEV}\n[limits]\n{field} = {}\n",
                max - FRAME_ENVELOPE_RESERVE_BYTES
            );
            GatewayConfig::from_toml(&fits).unwrap();
        }
        // the defaults fit
        GatewayConfig::from_toml(DEV).unwrap();
    }

    #[test]
    fn startup_reconcile_is_on_unless_it_is_turned_off() {
        assert!(
            GatewayConfig::from_toml(DEV).unwrap().reconcile.on_startup,
            "orphan reclamation is the default"
        );
        let off = format!("{DEV}\n[reconcile]\non_startup = false\n");
        assert!(!GatewayConfig::from_toml(&off).unwrap().reconcile.on_startup);
    }

    #[test]
    fn environment_reuse_is_off_by_default_and_validated_when_enabled() {
        let default = GatewayConfig::from_toml(DEV).unwrap().pool;
        assert!(
            !default.enabled,
            "reuse is opt-in; the shipped providers destroy after every invoke"
        );
        assert_eq!(default.max_idle_per_revision, 1);
        assert_eq!(default.idle_ttl_seconds, 60);
        assert_eq!(default.max_total_idle, 8);
        assert_eq!(default.idle_ttl(), Duration::from_secs(60));
        assert!(
            !default.allow_unverified_idle,
            "an unmeasured idle capability is not accepted unless an operator asks for it"
        );

        // PLT-4633: the measurement switch is opt-in and independent of the
        // caps, so a config can turn it on without touching anything else.
        let measuring = format!("{DEV}\n[pool]\nenabled = true\nallow_unverified_idle = true\n");
        let pool = GatewayConfig::from_toml(&measuring).unwrap().pool;
        assert!(pool.enabled && pool.allow_unverified_idle);
        assert!(
            GatewayConfig::from_toml(&measuring)
                .unwrap()
                .validate()
                .is_ok(),
            "a measurement configuration is valid under the dev profile"
        );

        let on = format!("{DEV}\n[pool]\nenabled = true\nidle_ttl_seconds = 5\n");
        let pool = GatewayConfig::from_toml(&on).unwrap().pool;
        assert!(pool.enabled);
        assert_eq!(pool.idle_ttl_seconds, 5);
        assert_eq!(pool.idle_ttl_chrono(), chrono::Duration::seconds(5));

        for (extra, needle) in [
            ("max_idle_per_revision = 0", "pool.max_idle_per_revision"),
            ("idle_ttl_seconds = 0", "pool.idle_ttl_seconds"),
            (
                "max_idle_per_revision = 4\nmax_total_idle = 2",
                "max_total_idle",
            ),
        ] {
            let text = format!("{DEV}\n[pool]\nenabled = true\n{extra}\n");
            let err = GatewayConfig::from_toml(&text).unwrap_err();
            assert!(err.to_string().contains(needle), "{err}");
            // The same values are accepted while the pool is off: nothing reads them.
            let off = format!("{DEV}\n[pool]\nenabled = false\n{extra}\n");
            GatewayConfig::from_toml(&off).unwrap();
        }
    }

    /// PLT-4636: a data plane needs its control plane and an internal
    /// credential, takes no tokens of its own, and every entry must be
    /// confirmable before it expires.
    #[test]
    fn control_plane_section_is_validated() {
        let cfg = GatewayConfig::from_toml(DEV).unwrap();
        assert_eq!(cfg.control_plane.role, GatewayRole::Combined);
        assert!(cfg.control_plane_outage.allow_cold_start);
        assert!(cfg.control_plane.internal_token.is_none());

        let no_tokens = DEV.split("[[identity.tokens]]").next().unwrap().to_string();
        let dp = format!(
            "{no_tokens}\n[control_plane]\nrole = \"data_plane\"\nurl = \"http://127.0.0.1:8080\"\n\
             internal_token = \"0123456789abcdef\"\n"
        );
        let cfg = GatewayConfig::from_toml(&dp).unwrap();
        assert_eq!(cfg.control_plane.role, GatewayRole::DataPlane);
        assert!(!format!("{cfg:?}").contains("0123456789abcdef"));

        for (text, needle) in [
            (
                format!(
                    "{no_tokens}\n[control_plane]\nrole = \"data_plane\"\ninternal_token = \"0123456789abcdef\"\n"
                ),
                "url",
            ),
            (
                format!(
                    "{no_tokens}\n[control_plane]\nrole = \"data_plane\"\nurl = \"http://x\"\n"
                ),
                "internal_token",
            ),
            (
                format!(
                    "{DEV}\n[control_plane]\nrole = \"data_plane\"\nurl = \"http://x\"\ninternal_token = \"0123456789abcdef\"\n"
                ),
                "identity.tokens",
            ),
            (
                format!("{DEV}\n[control_plane]\ninternal_token = \"short\"\n"),
                "internal_token",
            ),
            (
                format!(
                    "{DEV}\n[control_plane]\nrefresh_interval_ms = 60000\nauth_lease_seconds = 60\n"
                ),
                "refresh_interval_ms",
            ),
            (
                format!(
                    "{DEV}\n[control_plane]\nbackoff_initial_ms = 5000\nbackoff_max_ms = 1000\n"
                ),
                "backoff",
            ),
            (format!("{DEV}\n[control_plane]\nunknown = 1\n"), "unknown"),
        ] {
            let err = GatewayConfig::from_toml(&text).unwrap_err();
            assert!(err.to_string().contains(needle), "{needle}: {err}");
        }
    }

    #[test]
    fn relative_paths_are_absolutized_against_base() {
        let mut cfg = GatewayConfig::from_toml(
            r#"
[provider]
kind = "process"
[provider.process]
bridge_binary = "target/debug/bridge"
workdir = "./data/process"
[provider.firecracker]
firecracker_binary = "firecracker"
kernel = ".kvm/vmlinux"
rootfs = ".kvm/rootfs.ext4"
workdir = ".kvm/run"
"#,
        )
        .unwrap();
        cfg.absolutize_paths(Path::new("/base"));
        assert!(cfg.data_dir.is_absolute());
        let p = cfg.provider.process.as_ref().unwrap();
        assert!(p.bridge_binary.is_absolute());
        assert!(p.workdir.is_absolute());
        let f = cfg.provider.firecracker.as_ref().unwrap();
        assert_eq!(
            f.firecracker_binary,
            PathBuf::from("firecracker"),
            "bare command stays PATH-resolved"
        );
        assert!(f.kernel.is_absolute() && f.rootfs.is_absolute() && f.workdir.is_absolute());
    }
}
