//! Typed identifiers following the Tachyon convention `<prefix>_<26-char lowercase ULID>`.
//!
//! Prefixes are stable, persisted namespace tokens. Never rename an existing prefix.
//!
//! | Prefix | Type                 |
//! |--------|----------------------|
//! | `tn`   | Tenant (existing)    |
//! | `fn`   | Function             |
//! | `rev`  | FunctionRevision     |
//! | `inv`  | Invocation           |
//! | `att`  | InvocationAttempt    |
//! | `env`  | ExecutionEnvironment |
//! | `lse`  | ExecutionLease       |
//! | `trg`  | Trigger (cron / webhook, PLT-4641) |
//! | `dlq`  | DeadLetter           |
//! | `rdv`  | Redrive              |

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::DomainError;

const ULID_LEN: usize = 26;

fn is_lower_crockford(s: &str) -> bool {
    s.len() == ULID_LEN
        && s.bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'h' | b'j'..=b'k' | b'm'..=b'n' | b'p'..=b't' | b'v'..=b'z'))
}

fn parse_prefixed(prefix: &str, raw: &str) -> Result<String, DomainError> {
    let Some(rest) = raw.strip_prefix(prefix).and_then(|r| r.strip_prefix('_')) else {
        return Err(DomainError::InvalidId {
            expected_prefix: prefix.to_string(),
            value: raw.to_string(),
        });
    };
    if !is_lower_crockford(rest) {
        return Err(DomainError::InvalidId {
            expected_prefix: prefix.to_string(),
            value: raw.to_string(),
        });
    }
    Ok(raw.to_string())
}

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub const PREFIX: &'static str = $prefix;

            /// Generate a fresh identifier from a new ULID.
            pub fn generate() -> Self {
                Self::from_ulid(ulid::Ulid::new())
            }

            /// Build an identifier from an explicit ULID (useful for deterministic tests).
            pub fn from_ulid(ulid: ulid::Ulid) -> Self {
                Self(format!("{}_{}", $prefix, ulid.to_string().to_ascii_lowercase()))
            }

            pub fn parse(raw: &str) -> Result<Self, DomainError> {
                parse_prefixed($prefix, raw).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = DomainError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl TryFrom<String> for $name {
            type Error = DomainError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(&value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

define_id!(
    /// Tenant identifier. Reuses the existing Tachyon `tn_` namespace; this crate never mints tenants.
    TenantId,
    "tn"
);
define_id!(
    /// Logical function owned by a tenant.
    FunctionId,
    "fn"
);
define_id!(
    /// Immutable revision of a function.
    RevisionId,
    "rev"
);
define_id!(
    /// One logical invocation request.
    InvocationId,
    "inv"
);
define_id!(
    /// One execution attempt of an invocation.
    AttemptId,
    "att"
);
define_id!(
    /// One execution environment (microVM / process) that may serve attempts.
    EnvironmentId,
    "env"
);
define_id!(
    /// Ownership of an execution slot for one attempt.
    LeaseId,
    "lse"
);
define_id!(
    /// One running dispatcher (gateway process incarnation) that owns
    /// invocations, environments and slot leases (PLT-4631). A new id is
    /// minted on every start, so a restarted gateway never inherits the
    /// leases of the process before it.
    DispatcherId,
    "dsp"
);
define_id!(
    /// A cron or webhook trigger of a function (PLT-4641).
    TriggerId,
    "trg"
);

define_id!(
    /// A dead-lettered asynchronous invocation (PLT-4640): the invocation
    /// exhausted its retries, expired, failed with a non-retryable error, or
    /// its event could not be read at all (poison).
    DeadLetterId,
    "dlq"
);
define_id!(
    /// One redrive of a dead letter (PLT-4640): who re-submitted it, when,
    /// why, and the new invocation it created.
    RedriveId,
    "rdv"
);

/// Alias name such as `prod`. Lowercase DNS-label-like, 1..=32 chars.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AliasName(String);

impl AliasName {
    pub const DEFAULT: &'static str = "prod";

    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        if is_label(raw, 32) {
            Ok(Self(raw.to_string()))
        } else {
            Err(DomainError::InvalidName {
                kind: "alias",
                value: raw.to_string(),
            })
        }
    }

    pub fn default_alias() -> Self {
        Self(Self::DEFAULT.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AliasName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AliasName({})", self.0)
    }
}
impl fmt::Display for AliasName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl FromStr for AliasName {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}
impl TryFrom<String> for AliasName {
    type Error = DomainError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<AliasName> for String {
    fn from(value: AliasName) -> String {
        value.0
    }
}

/// Function name: lowercase, digits and dashes, 1..=63 chars, must start with alphanumeric.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct FunctionName(String);

impl FunctionName {
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        if is_label(raw, 63) {
            Ok(Self(raw.to_string()))
        } else {
            Err(DomainError::InvalidName {
                kind: "function",
                value: raw.to_string(),
            })
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for FunctionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FunctionName({})", self.0)
    }
}
impl fmt::Display for FunctionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl FromStr for FunctionName {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}
impl TryFrom<String> for FunctionName {
    type Error = DomainError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<FunctionName> for String {
    fn from(value: FunctionName) -> String {
        value.0
    }
}

fn is_label(raw: &str, max: usize) -> bool {
    if raw.is_empty() || raw.len() > max {
        return false;
    }
    let bytes = raw.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// A SHA-256 digest in canonical `sha256:<64 lowercase hex>` form.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Sha256Digest(String);

impl Sha256Digest {
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        let Some(hex_part) = raw.strip_prefix("sha256:") else {
            return Err(DomainError::InvalidDigest(raw.to_string()));
        };
        if hex_part.len() != 64
            || !hex_part
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(DomainError::InvalidDigest(raw.to_string()));
        }
        Ok(Self(raw.to_string()))
    }

    /// Compute the digest of a byte slice.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(bytes);
        Self(format!("sha256:{}", hex::encode(hash)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn hex(&self) -> &str {
        &self.0["sha256:".len()..]
    }
}
impl fmt::Debug for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sha256Digest({})", self.0)
    }
}
impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl FromStr for Sha256Digest {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}
impl TryFrom<String> for Sha256Digest {
    type Error = DomainError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<Sha256Digest> for String {
    fn from(value: Sha256Digest) -> String {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_have_prefix_and_lowercase_ulid() {
        let id = FunctionId::generate();
        assert!(id.as_str().starts_with("fn_"));
        assert_eq!(id.as_str().len(), 3 + 26);
        assert_eq!(id.as_str(), id.as_str().to_ascii_lowercase());
        assert_eq!(FunctionId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn wrong_prefix_is_rejected() {
        let id = FunctionId::generate();
        let raw = id.as_str().replacen("fn_", "rev_", 1);
        assert!(RevisionId::parse(&raw).is_ok());
        assert!(FunctionId::parse(&raw).is_err());
    }

    #[test]
    fn uppercase_or_short_ulid_is_rejected() {
        assert!(FunctionId::parse("fn_01HZZZZZZZZZZZZZZZZZZZZZZZ").is_err());
        assert!(FunctionId::parse("fn_abc").is_err());
        assert!(FunctionId::parse("fn_").is_err());
        assert!(FunctionId::parse("").is_err());
    }

    #[test]
    fn existing_tenant_ids_parse() {
        // Real tenant id shape used by Tachyon today.
        assert!(TenantId::parse("tn_01hjjn348rn3t49zz6hvmfq67p").is_ok());
    }

    #[test]
    fn serde_roundtrip_validates() {
        let id = InvocationId::generate();
        let json = serde_json::to_string(&id).unwrap();
        let back: InvocationId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
        let bad: Result<InvocationId, _> =
            serde_json::from_str("\"att_00000000000000000000000000\"");
        assert!(bad.is_err());
    }

    #[test]
    fn names_and_aliases() {
        assert!(FunctionName::parse("hello").is_ok());
        assert!(FunctionName::parse("hello-world-2").is_ok());
        assert!(FunctionName::parse("-bad").is_err());
        assert!(FunctionName::parse("Bad").is_err());
        assert!(FunctionName::parse("").is_err());
        assert!(FunctionName::parse(&"a".repeat(64)).is_err());
        assert!(AliasName::parse("prod").is_ok());
        assert!(AliasName::parse("with space").is_err());
    }

    #[test]
    fn digests() {
        let d = Sha256Digest::of_bytes(b"hello");
        assert_eq!(
            d.as_str(),
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert!(Sha256Digest::parse("sha256:zz").is_err());
        assert!(Sha256Digest::parse(d.as_str()).is_ok());
    }
}
