//! What the control plane delivers to a data plane (PLT-4636,
//! docs/adr/0007-config-distribution-and-auth-leases.md).
//!
//! A delivery is a list of generation-stamped entries. Every entry is keyed
//! ([`ConfigKey`]) and either carries a value or is a tombstone (the key was
//! removed: a revoked token, a tenant without grants). Generations are one
//! counter per control plane, persisted in the ledger, so they only grow
//! across restarts; an entry's generation is the counter value at which its
//! content last changed.
//!
//! A delivery answers `since = n`: every entry whose generation is above `n`.
//! It also asserts that everything *not* in it is unchanged as of
//! `generation`, which is what lets a successful delta renew the validity of
//! every cached entry at once.
//!
//! No secret value is ever part of a delivery: revisions carry secret binding
//! *references* only, and bearer tokens are replaced by a keyed digest
//! ([`grant_key`]).

use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{
    AliasName, EgressProfile, Function, FunctionAlias, FunctionId, FunctionRevision, RevisionId,
    RevisionStatus, Sha256Digest, TenantId,
};
use tachyon_serverless_provider_port::Role;

/// The key of one delivered entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConfigKey {
    Function {
        function_id: FunctionId,
    },
    /// `function` + `alias` -> revision.
    Route {
        function_id: FunctionId,
        alias: AliasName,
    },
    Revision {
        revision_id: RevisionId,
    },
    /// An authorization grant, keyed by [`grant_key`] of the bearer token.
    Grant {
        token_digest: String,
    },
    /// A tenant this control plane knows (has at least one grant).
    Tenant {
        tenant_id: TenantId,
    },
    /// The invoke policy (one per control plane).
    Policy,
}

impl ConfigKey {
    /// Authorization entries live under the auth lease, everything else
    /// under the config TTL.
    pub fn is_authorization(&self) -> bool {
        matches!(self, Self::Grant { .. } | Self::Tenant { .. })
    }

    /// Stable text form, the primary key of the publication table.
    pub fn storage_key(&self) -> String {
        // Field order is the declaration order, so this is deterministic.
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_storage_key(raw: &str) -> Option<Self> {
        serde_json::from_str(raw).ok()
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Function { .. } => "function",
            Self::Route { .. } => "route",
            Self::Revision { .. } => "revision",
            Self::Grant { .. } => "grant",
            Self::Tenant { .. } => "tenant",
            Self::Policy => "policy",
        }
    }
}

/// Bearer token -> principal. The token itself never leaves the control
/// plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthGrant {
    pub subject: String,
    pub tenant_id: TenantId,
    pub roles: Vec<Role>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantGrant {
    pub tenant_id: TenantId,
}

/// Invoke policy. Quotas and budget tokens are P3 (PLT-4643).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigPolicy {
    /// Egress profiles a revision may be started with.
    pub allowed_egress: Vec<EgressProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ConfigValue {
    Function(Function),
    Route(FunctionAlias),
    Revision(Box<FunctionRevision>),
    Grant(AuthGrant),
    Tenant(TenantGrant),
    Policy(ConfigPolicy),
}

impl ConfigValue {
    /// Digest of the value, for "did it change".
    pub fn digest(&self) -> Sha256Digest {
        Sha256Digest::of_bytes(&serde_json::to_vec(self).unwrap_or_default())
    }

    /// A version the value itself carries, when it has one. The publication
    /// never replaces an entry by a value with a *lower* natural version, so
    /// a stale read of the ledger cannot roll a route back even if it raced a
    /// newer publication.
    pub fn natural_version(&self) -> u64 {
        match self {
            Self::Function(f) => {
                u64::from(f.deleted_at.is_some()) + u64::from(f.drained_at.is_some())
            }
            Self::Route(a) => a.generation,
            Self::Revision(r) => match r.status {
                RevisionStatus::Pending => 0,
                RevisionStatus::Preparing => 1,
                RevisionStatus::Validating => 2,
                RevisionStatus::Ready | RevisionStatus::Failed { .. } => 3,
            },
            Self::Grant(_) | Self::Tenant(_) | Self::Policy(_) => 0,
        }
    }
}

/// One stamped entry. `value = None` is a tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub key: ConfigKey,
    pub generation: u64,
    #[serde(default)]
    pub value: Option<ConfigValue>,
}

/// Body of `GET /v1/internal/config?since=<generation>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDelivery {
    /// Who published it (for logs and `/readyz`).
    pub source: String,
    /// The control plane's generation when this delivery was built.
    pub generation: u64,
    /// The `since` this delivery answers.
    pub since: u64,
    /// Upper bounds the control plane sets for the data plane's config TTL and
    /// auth lease. The data plane uses the smaller of these and its own.
    pub config_ttl_seconds: u64,
    pub auth_lease_seconds: u64,
    /// Entries with `generation > since`, tombstones included.
    pub entries: Vec<ConfigEntry>,
}

/// HMAC-SHA256 of a bearer token under `secret`, hex encoded.
///
/// The control plane distributes grants under this digest instead of the
/// token, keyed with the internal credential: a copy of a delivery cannot be
/// replayed as bearer tokens, and without the internal credential it cannot
/// even be used to test guesses of low-entropy tokens.
pub fn grant_key(secret: &[u8], token: &str) -> String {
    use sha2::{Digest, Sha256};
    const BLOCK: usize = 64;
    let mut key = [0u8; BLOCK];
    if secret.len() > BLOCK {
        key[..32].copy_from_slice(&Sha256::digest(secret));
    } else {
        key[..secret.len()].copy_from_slice(secret);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= key[i];
        opad[i] ^= key[i];
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(token.as_bytes())
        .finalize();
    let outer = Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize();
    hex::encode(outer)
}

/// Compare two secrets without an early exit on the first difference.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_key_is_hmac_sha256() {
        // RFC 4231 test case 2.
        assert_eq!(
            grant_key(b"Jefe", "what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // A key longer than the block is hashed first (RFC 4231 case 6).
        assert_eq!(
            grant_key(
                &[0xaa; 131],
                "Test Using Larger Than Block-Size Key - Hash Key First"
            ),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn keys_round_trip_through_their_storage_form() {
        let keys = [
            ConfigKey::Function {
                function_id: FunctionId::generate(),
            },
            ConfigKey::Route {
                function_id: FunctionId::generate(),
                alias: AliasName::default_alias(),
            },
            ConfigKey::Grant {
                token_digest: "ab".into(),
            },
            ConfigKey::Policy,
        ];
        for k in keys {
            assert_eq!(ConfigKey::from_storage_key(&k.storage_key()), Some(k));
        }
    }

    #[test]
    fn constant_time_eq_compares() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
