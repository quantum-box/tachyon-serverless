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

#[derive(Debug, Clone, Deserialize)]
pub struct CapacityConfig {
    /// Gateway-wide upper bound of simultaneously running environments.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    /// Invocations allowed to wait for capacity; beyond this -> 429.
    #[serde(default = "default_max_queue")]
    pub max_queue: usize,
    #[serde(default = "default_queue_timeout")]
    pub queue_timeout_seconds: u64,
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

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            max_concurrency: default_max_concurrency(),
            max_queue: default_max_queue(),
            queue_timeout_seconds: default_queue_timeout(),
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
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            backend: StoreBackend::Sqlite,
            output_retention_seconds: 7 * 24 * 60 * 60,
        }
    }
}

impl StoreConfig {
    pub fn output_retention(&self) -> Option<chrono::Duration> {
        (self.output_retention_seconds > 0).then(|| {
            chrono::Duration::seconds(
                self.output_retention_seconds.min(i64::MAX as u64 / 1000) as i64
            )
        })
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
    #[serde(default)]
    pub store: StoreConfig,
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
        }
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
