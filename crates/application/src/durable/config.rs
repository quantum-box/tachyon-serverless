//! `[queue]` and `[objects]` (PLT-4638, docs/architecture.md §4「durable
//! queue と object store」). Both default to off, so a gateway configured
//! before PLT-4638 behaves exactly as before.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use tachyon_serverless_durable_port::{QueueLimits, Region};

/// Which queue backs asynchronous delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum QueueBackend {
    /// No queue (sync invoke only). The default.
    #[default]
    None,
    /// `<data_dir>/queue.db`: embedded, single node, dev / CI only.
    Sqlite,
    /// NATS JetStream (`[queue.nats]`).
    Nats,
}

impl QueueBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Sqlite => "sqlite",
            Self::Nats => "nats",
        }
    }
}

/// `[queue.limits]`: the stream's hard limits (applied at startup).
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueLimitsConfig {
    pub max_messages: u64,
    pub max_bytes: u64,
    pub max_message_bytes: u32,
    pub max_age_seconds: u64,
    pub duplicate_window_seconds: u64,
}

impl Default for QueueLimitsConfig {
    fn default() -> Self {
        let d = QueueLimits::default();
        Self {
            max_messages: d.max_messages,
            max_bytes: d.max_bytes,
            max_message_bytes: d.max_message_bytes,
            max_age_seconds: d.max_age.as_secs(),
            duplicate_window_seconds: d.duplicate_window.as_secs(),
        }
    }
}

impl QueueLimitsConfig {
    pub fn limits(&self) -> QueueLimits {
        QueueLimits {
            max_messages: self.max_messages,
            max_bytes: self.max_bytes,
            max_message_bytes: self.max_message_bytes,
            max_age: Duration::from_secs(self.max_age_seconds),
            duplicate_window: Duration::from_secs(self.duplicate_window_seconds),
        }
    }
}

/// `[queue.nats]`. Anonymous connections are refused by validation: exactly
/// one of `user` + `password_file`, or `nkey_seed_file`, must be set.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsQueueConfig {
    /// e.g. `nats://127.0.0.1:4222`.
    pub url: String,
    #[serde(default = "default_stream")]
    pub stream: String,
    #[serde(default = "default_subject_prefix")]
    pub subject_prefix: String,
    #[serde(default)]
    pub user: Option<String>,
    /// File holding the password (mode 0600, checked at connect).
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    /// File holding an nkey seed (`SU...`, mode 0600).
    #[serde(default)]
    pub nkey_seed_file: Option<PathBuf>,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_stream() -> String {
    "TACHYON_EVENTS".into()
}
fn default_subject_prefix() -> String {
    "tachyon.events".into()
}
fn default_connect_timeout_ms() -> u64 {
    5_000
}
fn default_request_timeout_ms() -> u64 {
    5_000
}

/// `[queue]`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct QueueConfig {
    pub backend: QueueBackend,
    /// SQLite file. Default `<data_dir>/queue.db`.
    pub path: Option<PathBuf>,
    pub nats: Option<NatsQueueConfig>,
    pub limits: QueueLimitsConfig,
}

/// Which store keeps large invocation inputs and outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ObjectsBackend {
    /// No object store. The default.
    #[default]
    None,
    /// Encrypted files under `root` on this node's disk.
    Filesystem,
}

impl ObjectsBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Filesystem => "filesystem",
        }
    }
}

/// `[objects]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObjectsConfig {
    pub backend: ObjectsBackend,
    /// Default `<data_dir>/objects`.
    pub root: Option<PathBuf>,
    /// Regions this store serves. A put for another region is refused.
    pub regions: Vec<String>,
    /// 64 hex characters, mode 0600. Exactly one of `key_file` / `key_env`.
    pub key_file: Option<PathBuf>,
    /// Name of an environment variable holding the 64 hex characters.
    pub key_env: Option<String>,
    pub max_object_bytes: u64,
    pub tenant_quota_bytes: u64,
    /// TTL of an object stored without one. `0`: no expiry.
    pub default_ttl_seconds: u64,
    /// An object nothing ever referenced is collected after this long.
    pub orphan_grace_seconds: u64,
    pub gc_interval_seconds: u64,
}

impl Default for ObjectsConfig {
    fn default() -> Self {
        Self {
            backend: ObjectsBackend::None,
            root: None,
            regions: vec!["local".into()],
            key_file: None,
            key_env: None,
            max_object_bytes: 8 * 1024 * 1024,
            tenant_quota_bytes: 1024 * 1024 * 1024,
            default_ttl_seconds: 7 * 24 * 60 * 60,
            orphan_grace_seconds: 60 * 60,
            gc_interval_seconds: 600,
        }
    }
}

impl ObjectsConfig {
    pub fn default_ttl(&self) -> Option<Duration> {
        (self.default_ttl_seconds > 0).then(|| Duration::from_secs(self.default_ttl_seconds))
    }

    pub fn orphan_grace(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.orphan_grace_seconds.min(i64::MAX as u64 / 1000) as i64)
    }

    pub fn gc_interval(&self) -> Duration {
        Duration::from_secs(self.gc_interval_seconds.max(1))
    }

    pub fn parsed_regions(&self) -> Result<Vec<Region>, String> {
        self.regions
            .iter()
            .map(|r| Region::parse(r).map_err(|e| e.to_string()))
            .collect()
    }
}

/// Validation of both sections. `production` refuses the dev-only SQLite
/// queue, like a dev-only provider.
pub fn validate(
    queue: &QueueConfig,
    objects: &ObjectsConfig,
    production: bool,
) -> Result<(), String> {
    match queue.backend {
        QueueBackend::None => {}
        QueueBackend::Sqlite => {
            if production {
                return Err(
                    "[queue] backend = \"sqlite\" is a single-node development queue and cannot \
                     be used with profile = \"production\""
                        .into(),
                );
            }
            queue
                .limits
                .limits()
                .validate()
                .map_err(|e| format!("[queue.limits]: {e}"))?;
        }
        QueueBackend::Nats => {
            let nats = queue
                .nats
                .as_ref()
                .ok_or("[queue.nats] is required when [queue] backend = \"nats\"")?;
            if nats.url.is_empty() {
                return Err("[queue.nats] url must not be empty".into());
            }
            let password = nats.user.is_some() || nats.password_file.is_some();
            let nkey = nats.nkey_seed_file.is_some();
            match (password, nkey) {
                (false, false) => {
                    return Err("[queue.nats] needs credentials: user + password_file, or \
                                nkey_seed_file. Anonymous connections are not allowed"
                        .into());
                }
                (true, true) => {
                    return Err(
                        "[queue.nats] set either user + password_file or nkey_seed_file, not both"
                            .into(),
                    );
                }
                (true, false) if nats.user.is_none() || nats.password_file.is_none() => {
                    return Err("[queue.nats] user and password_file go together".into());
                }
                _ => {}
            }
            let token = |s: &str| {
                !s.is_empty()
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            };
            if !token(&nats.stream) {
                return Err("[queue.nats] stream must be [A-Za-z0-9_-]+".into());
            }
            if nats.subject_prefix.is_empty() || !nats.subject_prefix.split('.').all(token) {
                return Err(
                    "[queue.nats] subject_prefix must be dot-separated [A-Za-z0-9_-] tokens".into(),
                );
            }
            queue
                .limits
                .limits()
                .validate()
                .map_err(|e| format!("[queue.limits]: {e}"))?;
        }
    }
    if objects.backend == ObjectsBackend::Filesystem {
        let regions = objects
            .parsed_regions()
            .map_err(|e| format!("[objects] regions: {e}"))?;
        if regions.is_empty() {
            return Err("[objects] regions must name at least one region".into());
        }
        match (&objects.key_file, &objects.key_env) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => {
                return Err(
                    "[objects] needs exactly one of key_file / key_env: objects are always \
                     encrypted at rest"
                        .into(),
                );
            }
        }
        if objects.max_object_bytes == 0 || objects.tenant_quota_bytes < objects.max_object_bytes {
            return Err(
                "[objects] needs max_object_bytes > 0 and tenant_quota_bytes >= max_object_bytes"
                    .into(),
            );
        }
        if objects.orphan_grace_seconds == 0 {
            return Err(
                "[objects] orphan_grace_seconds must be > 0 (a put and the insert of the \
                        invocation that uses it are not one transaction)"
                    .into(),
            );
        }
    }
    Ok(())
}

/// Resolve relative paths against `base` (see `GatewayConfig::absolutize_paths`).
pub fn absolutize(queue: &mut QueueConfig, objects: &mut ObjectsConfig, base: &Path) {
    fn abs(base: &Path, p: &mut Option<PathBuf>) {
        if let Some(path) = p
            && path.is_relative()
        {
            *path = base.join(&*path);
        }
    }
    abs(base, &mut queue.path);
    if let Some(n) = queue.nats.as_mut() {
        abs(base, &mut n.password_file);
        abs(base, &mut n.nkey_seed_file);
    }
    abs(base, &mut objects.root);
    abs(base, &mut objects.key_file);
}
