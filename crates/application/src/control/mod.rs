//! Control-plane / data-plane split: generation-stamped configuration
//! distribution, the data plane's expiring configuration cache and the
//! authorization lease (PLT-4636,
//! docs/adr/0007-config-distribution-and-auth-leases.md).
//!
//! ```text
//! control plane (combined gateway)            data plane (any gateway)
//!   ledger + [[identity.tokens]] + policy       ConfigCache  <- refresh loop
//!     └─ LedgerConfigSource ──(in process)────▶   │  (valid_until per entry,
//!     └─ GET /v1/internal/config ──(HTTP)─────▶   │   generation per key)
//!                                                 └─ InvokeGate: authenticate,
//!                                                    resolve, permit cold start
//! ```

pub mod cache;
pub mod gate;
pub mod source;
pub mod wire;

pub use cache::{
    ApplyReport, BudgetEntry, CacheSettings, CacheState, CacheStatus, ConfigCache, EntryState,
    RefreshReport, Resolved, ScaleView,
};
pub use gate::{InvokeGate, InvokeView};
pub use source::{
    ConfigChangeSignal, ConfigSource, LedgerConfigSource, SignalingConfigRepos, SourceError,
    grant_secret,
};
pub use wire::{
    AuthGrant, ConfigDelivery, ConfigEntry, ConfigKey, ConfigPolicy, ConfigValue, TenantGrant,
    constant_time_eq, grant_key,
};

use tachyon_serverless_api_types::ErrorCode;

/// Why a gateway refuses work because of its control plane or its
/// configuration cache. Each has its own `error_type`, so a client (and an
/// operator reading `/readyz`) can tell "retry later, the configuration will
/// come" from "retry elsewhere" from "you are not allowed".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlError {
    /// The needed entry (grant, revision, route target, policy) has not been
    /// delivered to this data plane yet.
    ConfigNotDelivered,
    /// It was delivered, but not confirmed by the control plane within its
    /// TTL.
    ConfigExpired,
    /// The caller's authorization grant was not confirmed within the auth
    /// lease.
    AuthLeaseExpired,
    /// The credential is known but its tenant is not (not delivered, or
    /// removed).
    UnknownTenant,
    /// The delivered policy does not allow this revision.
    PolicyDenied,
    /// The control plane is unreachable and `[control_plane_outage]
    /// allow_cold_start = false`: only running environments may serve.
    ColdStartRestricted,
    /// The host's execution control API (provider preflight) is failing:
    /// nothing new can be booted; running environments continue.
    ProviderControlUnavailable,
    /// The management API is not served by this gateway (a data plane).
    ControlPlaneUnavailable,
    /// The ledger store failed.
    StoreUnavailable,
}

impl ControlError {
    pub const ALL: [ControlError; 9] = [
        Self::ConfigNotDelivered,
        Self::ConfigExpired,
        Self::AuthLeaseExpired,
        Self::UnknownTenant,
        Self::PolicyDenied,
        Self::ColdStartRestricted,
        Self::ProviderControlUnavailable,
        Self::ControlPlaneUnavailable,
        Self::StoreUnavailable,
    ];

    pub fn error_type(&self) -> &'static str {
        match self {
            Self::ConfigNotDelivered => "Host.ConfigNotDelivered",
            Self::ConfigExpired => "Host.ConfigExpired",
            Self::AuthLeaseExpired => "Host.AuthLeaseExpired",
            Self::UnknownTenant => "Host.UnknownTenant",
            Self::PolicyDenied => "Host.PolicyDenied",
            Self::ColdStartRestricted => "Host.ColdStartRestricted",
            Self::ProviderControlUnavailable => "Host.ProviderControlUnavailable",
            Self::ControlPlaneUnavailable => "Host.ControlPlaneUnavailable",
            Self::StoreUnavailable => "Host.StoreUnavailable",
        }
    }

    pub fn from_error_type(error_type: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.error_type() == error_type)
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            Self::ConfigNotDelivered
            | Self::ConfigExpired
            | Self::AuthLeaseExpired
            | Self::ColdStartRestricted => ErrorCode::ConfigUnavailable,
            Self::UnknownTenant | Self::PolicyDenied => ErrorCode::Forbidden,
            Self::ProviderControlUnavailable => ErrorCode::ProviderUnavailable,
            Self::ControlPlaneUnavailable | Self::StoreUnavailable => {
                ErrorCode::ControlPlaneUnavailable
            }
        }
    }
}
