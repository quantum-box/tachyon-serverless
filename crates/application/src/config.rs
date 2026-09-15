//! Gateway configuration (`config/gateway.toml`, see docs/architecture.md §4).
//!
//! Secrets and bearer tokens are wrapped in newtypes whose `Debug` output is
//! redacted so that a dumped configuration never leaks them.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use tachyon_serverless_domain::{Limits, TenantId};
use tachyon_serverless_provider_port::Role;

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
        if self.invoke.inline_output_max_bytes > limits.max_response_bytes {
            return Err(ConfigError::Invalid(
                "invoke.inline_output_max_bytes must be <= limits.max_response_bytes".into(),
            ));
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
