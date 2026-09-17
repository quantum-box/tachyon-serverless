//! Object encryption at rest: AES-256-GCM with a 32-byte key loaded from a
//! key file or an environment variable (ADR-0008 §「暗号化」).
//!
//! - The key id recorded in object metadata is a fingerprint
//!   (`k1-` + 16 hex of SHA-256 over a domain-separation label and the key);
//!   it identifies the key without revealing it.
//! - Every object has a fresh random 96-bit nonce.
//! - The associated data binds the ciphertext to the object's id, tenant,
//!   region, key id, plaintext digest and size. Moving a ciphertext to another
//!   tenant's directory, or editing the digest in its metadata, makes
//!   decryption fail instead of returning someone else's bytes.

use std::path::Path;

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use tachyon_serverless_domain::Sha256Digest;

pub const ALGORITHM: &str = "AES-256-GCM";
const MAGIC: &[u8; 4] = b"TSO1";
const NONCE_LEN: usize = 12;

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("cannot read object key file {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "object key file {0} is readable by group or others; restrict it to the owner (chmod 600)"
    )]
    Permissions(String),
    #[error("environment variable {0} is not set")]
    MissingEnv(String),
    #[error("an object key must be 64 hex characters (32 bytes)")]
    Format,
}

/// A loaded data key. `Debug` never shows the key.
pub struct ObjectKey {
    key: [u8; 32],
    id: String,
}

impl std::fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectKey")
            .field("id", &self.id)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl Drop for ObjectKey {
    fn drop(&mut self) {
        self.key.fill(0);
    }
}

impl ObjectKey {
    pub fn from_hex(text: &str) -> Result<Self, KeyError> {
        let bytes = hex::decode(text.trim()).map_err(|_| KeyError::Format)?;
        let key: [u8; 32] = bytes.try_into().map_err(|_| KeyError::Format)?;
        Ok(Self::from_bytes(key))
    }

    pub fn from_bytes(key: [u8; 32]) -> Self {
        let mut labelled = b"tachyon-serverless/object-key-id/v1\0".to_vec();
        labelled.extend_from_slice(&key);
        let fingerprint = Sha256Digest::of_bytes(&labelled);
        labelled.fill(0);
        Self {
            key,
            id: format!("k1-{}", &fingerprint.hex()[..16]),
        }
    }

    /// Load from a file holding 64 hex characters. On unix the file must not
    /// be accessible by group or others.
    pub fn from_file(path: &Path) -> Result<Self, KeyError> {
        let read_err = |source| KeyError::Read {
            path: path.display().to_string(),
            source,
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(path)
                .map_err(read_err)?
                .permissions()
                .mode();
            if mode & 0o077 != 0 {
                return Err(KeyError::Permissions(path.display().to_string()));
            }
        }
        let text = std::fs::read_to_string(path).map_err(read_err)?;
        Self::from_hex(&text)
    }

    pub fn from_env(var: &str) -> Result<Self, KeyError> {
        let text = std::env::var(var).map_err(|_| KeyError::MissingEnv(var.to_string()))?;
        Self::from_hex(&text)
    }

    /// Fresh random key (tests and `scripts/queue/objects-key.sh`-style setup).
    pub fn generate() -> Self {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).expect("operating system randomness");
        Self::from_bytes(key)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(GenericArray::from_slice(&self.key))
    }

    /// `MAGIC || nonce || ciphertext+tag`.
    pub fn seal(&self, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).expect("operating system randomness");
        let mut sealed = plaintext.to_vec();
        self.cipher()
            .encrypt_in_place(GenericArray::from_slice(&nonce), aad, &mut sealed)
            .expect("AES-GCM encryption of an in-memory buffer cannot fail");
        let mut out = Vec::with_capacity(MAGIC.len() + NONCE_LEN + sealed.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        out
    }

    /// `None` when the envelope is malformed or authentication fails.
    pub fn open(&self, envelope: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
        let rest = envelope.strip_prefix(MAGIC)?;
        if rest.len() < NONCE_LEN {
            return None;
        }
        let (nonce, sealed) = rest.split_at(NONCE_LEN);
        let mut plain = sealed.to_vec();
        self.cipher()
            .decrypt_in_place(GenericArray::from_slice(nonce), aad, &mut plain)
            .ok()?;
        Some(plain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_round_trip_and_tamper_detection() {
        let key = ObjectKey::generate();
        let sealed = key.seal(b"hello", b"aad");
        assert!(!sealed.windows(5).any(|w| w == b"hello"));
        assert_eq!(key.open(&sealed, b"aad").unwrap(), b"hello");
        assert!(key.open(&sealed, b"other aad").is_none());
        let mut flipped = sealed.clone();
        *flipped.last_mut().unwrap() ^= 1;
        assert!(key.open(&flipped, b"aad").is_none());
        let other = ObjectKey::generate();
        assert_ne!(other.id(), key.id());
        assert!(other.open(&sealed, b"aad").is_none());
        // two seals of the same plaintext differ (fresh nonce)
        assert_ne!(key.seal(b"hello", b"aad"), sealed);
    }

    #[test]
    fn key_loading_is_strict_and_redacted() {
        let hex_key = "11".repeat(32);
        let key = ObjectKey::from_hex(&format!("{hex_key}\n")).unwrap();
        assert!(key.id().starts_with("k1-") && key.id().len() == 19);
        assert!(!format!("{key:?}").contains(&hex_key));
        assert_eq!(ObjectKey::from_hex(&hex_key).unwrap().id(), key.id());
        assert!(ObjectKey::from_hex("abcd").is_err());
        assert!(ObjectKey::from_hex(&"zz".repeat(32)).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object.key");
        std::fs::write(&path, &hex_key).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(matches!(
                ObjectKey::from_file(&path),
                Err(KeyError::Permissions(_))
            ));
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(ObjectKey::from_file(&path).unwrap().id(), key.id());
    }
}
