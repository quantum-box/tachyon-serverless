//! `generic-hmac` webhook verification (PLT-4641, docs/adr/0014 §3).
//!
//! - Signed content: `"{timestamp}.{body}"` where `timestamp` is the decimal
//!   Unix seconds of `x-tachyon-webhook-timestamp` exactly as sent and `body`
//!   the raw request body bytes.
//! - Signature: `x-tachyon-webhook-signature: v1=<hex HMAC-SHA256(secret,
//!   signed content)>`; several comma-separated entries are accepted and one
//!   valid `v1` entry suffices. The key is the UTF-8 bytes of the whole secret
//!   string (`whsec_...`) as the trigger's create / rotate response showed it.
//! - Every comparison is constant-time. Every refusal here happens before
//!   anything is written.

use sha2::{Digest, Sha256};

use crate::control::constant_time_eq;

/// Prefix of generated secrets.
pub const SECRET_PREFIX: &str = "whsec_";
/// Largest accepted signature header (a few rotated entries).
const MAX_SIGNATURE_HEADER: usize = 1024;
/// Largest accepted event id.
pub const MAX_EVENT_ID_BYTES: usize = 128;

/// Why a delivery was refused before anything was stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyError {
    MissingTimestamp,
    MalformedTimestamp,
    /// Older or newer than the tolerance.
    TimestampOutsideTolerance,
    MissingSignature,
    MalformedSignature,
    /// No `v1` entry matches.
    SignatureMismatch,
}

impl VerifyError {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingTimestamp => "missing_timestamp",
            Self::MalformedTimestamp => "malformed_timestamp",
            Self::TimestampOutsideTolerance => "timestamp_outside_tolerance",
            Self::MissingSignature => "missing_signature",
            Self::MalformedSignature => "malformed_signature",
            Self::SignatureMismatch => "signature_mismatch",
        }
    }
}

/// HMAC-SHA256 (RFC 2104) over bytes.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(message)
        .finalize();
    let outer = Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize();
    k.fill(0);
    outer.into()
}

/// The `v1=` value a sender puts in the signature header.
pub fn sign(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut message = format!("{timestamp}.").into_bytes();
    message.extend_from_slice(body);
    format!(
        "v1={}",
        hex::encode(hmac_sha256(secret.as_bytes(), &message))
    )
}

/// Check the timestamp header against `now_unix` and `tolerance_seconds`.
/// Returns the parsed seconds.
pub fn check_timestamp(
    header: Option<&str>,
    now_unix: i64,
    tolerance_seconds: u64,
) -> Result<i64, VerifyError> {
    let raw = header.ok_or(VerifyError::MissingTimestamp)?;
    if raw.is_empty() || raw.len() > 12 || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(VerifyError::MalformedTimestamp);
    }
    let ts: i64 = raw.parse().map_err(|_| VerifyError::MalformedTimestamp)?;
    let tolerance = i64::try_from(tolerance_seconds).unwrap_or(i64::MAX);
    if (now_unix - ts).abs() > tolerance {
        return Err(VerifyError::TimestampOutsideTolerance);
    }
    Ok(ts)
}

/// Verify the signature header over `"{timestamp}.{body}"`. Returns the
/// SHA-256 (hex) of the matching `v1` value, the replay key of this signed
/// delivery.
pub fn verify_signature(
    header: Option<&str>,
    secret: &str,
    timestamp_raw: &str,
    body: &[u8],
) -> Result<String, VerifyError> {
    let raw = header.ok_or(VerifyError::MissingSignature)?;
    if raw.is_empty() || raw.len() > MAX_SIGNATURE_HEADER {
        return Err(VerifyError::MalformedSignature);
    }
    let mut message = Vec::with_capacity(timestamp_raw.len() + 1 + body.len());
    message.extend_from_slice(timestamp_raw.as_bytes());
    message.push(b'.');
    message.extend_from_slice(body);
    let expected = hmac_sha256(secret.as_bytes(), &message);
    let mut saw_v1 = false;
    let mut matched: Option<Vec<u8>> = None;
    for entry in raw.split(',').map(str::trim) {
        let Some(hex_sig) = entry.strip_prefix("v1=") else {
            continue;
        };
        saw_v1 = true;
        let Ok(candidate) = hex::decode(hex_sig) else {
            continue;
        };
        // Constant-time per entry, and every entry is compared.
        if constant_time_eq(&candidate, &expected) && matched.is_none() {
            matched = Some(candidate);
        }
    }
    match (saw_v1, matched) {
        (false, _) => Err(VerifyError::MalformedSignature),
        (true, None) => Err(VerifyError::SignatureMismatch),
        (true, Some(sig)) => Ok(hex::encode(Sha256::digest(&sig))),
    }
}

/// A usable event id: 1..=128 visible ASCII characters.
pub fn valid_event_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_EVENT_ID_BYTES && id.bytes().all(|b| b.is_ascii_graphic())
}

/// A fresh secret: `whsec_` + 64 hex characters (256 random bits).
pub fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("operating system randomness");
    let s = format!("{SECRET_PREFIX}{}", hex::encode(bytes));
    bytes.fill(0);
    s
}

/// `sha256:` + 12 hex characters: tells secrets apart without revealing them.
pub fn fingerprint(secret: &str) -> String {
    let mut labelled = b"tachyon-serverless/webhook-secret-fingerprint/v1\0".to_vec();
    labelled.extend_from_slice(secret.as_bytes());
    format!("sha256:{}", &hex::encode(Sha256::digest(&labelled))[..12])
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "whsec_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn hmac_matches_rfc_4231() {
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        assert_eq!(
            hex::encode(hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    /// Fixture: the same computation as `openssl dgst -sha256 -hmac "$SECRET"`
    /// over `1789000000.{"hello":"world"}` (scripts/queue/triggers-e2e.sh).
    #[test]
    fn signature_fixtures_verify_and_refuse() {
        let body = br#"{"hello":"world"}"#;
        let good = sign(SECRET, 1_789_000_000, body);
        assert_eq!(
            good,
            format!(
                "v1={}",
                hex::encode(hmac_sha256(
                    SECRET.as_bytes(),
                    b"1789000000.{\"hello\":\"world\"}"
                ))
            )
        );
        // valid
        let digest = verify_signature(Some(&good), SECRET, "1789000000", body).unwrap();
        assert_eq!(digest.len(), 64);
        // valid among rotated entries
        let rotated = format!("v1=deadbeef, {good}, v0=ignored");
        assert_eq!(
            verify_signature(Some(&rotated), SECRET, "1789000000", body).unwrap(),
            digest
        );
        // bad signature
        let mut bad = good.clone();
        bad.replace_range(bad.len() - 1.., if good.ends_with('0') { "1" } else { "0" });
        assert_eq!(
            verify_signature(Some(&bad), SECRET, "1789000000", body),
            Err(VerifyError::SignatureMismatch)
        );
        // wrong secret
        let other = sign(&generate_secret(), 1_789_000_000, body);
        assert_eq!(
            verify_signature(Some(&other), SECRET, "1789000000", body),
            Err(VerifyError::SignatureMismatch)
        );
        // body or timestamp changed after signing
        assert_eq!(
            verify_signature(Some(&good), SECRET, "1789000000", br#"{"hello":"World"}"#),
            Err(VerifyError::SignatureMismatch)
        );
        assert_eq!(
            verify_signature(Some(&good), SECRET, "1789000001", body),
            Err(VerifyError::SignatureMismatch)
        );
        // malformed / missing
        assert_eq!(
            verify_signature(None, SECRET, "1789000000", body),
            Err(VerifyError::MissingSignature)
        );
        assert_eq!(
            verify_signature(Some("sha256=abc"), SECRET, "1789000000", body),
            Err(VerifyError::MalformedSignature)
        );
        assert_eq!(
            verify_signature(Some(&"v1=00,".repeat(300)), SECRET, "1789000000", body),
            Err(VerifyError::MalformedSignature)
        );
    }

    #[test]
    fn timestamps_outside_the_tolerance_are_refused_both_ways() {
        let now = 1_789_000_000;
        assert_eq!(check_timestamp(Some("1789000000"), now, 300), Ok(now));
        assert_eq!(check_timestamp(Some("1788999700"), now, 300), Ok(now - 300));
        assert_eq!(
            check_timestamp(Some("1788999699"), now, 300),
            Err(VerifyError::TimestampOutsideTolerance)
        );
        assert_eq!(
            check_timestamp(Some("1789000301"), now, 300),
            Err(VerifyError::TimestampOutsideTolerance)
        );
        for bad in ["", "-5", "12.5", "abc", "9999999999999"] {
            assert_eq!(
                check_timestamp(Some(bad), now, 300),
                Err(VerifyError::MalformedTimestamp),
                "{bad}"
            );
        }
        assert_eq!(
            check_timestamp(None, now, 300),
            Err(VerifyError::MissingTimestamp)
        );
    }

    #[test]
    fn secrets_event_ids_and_fingerprints() {
        let a = generate_secret();
        assert!(a.starts_with(SECRET_PREFIX) && a.len() == 70);
        assert_ne!(a, generate_secret());
        assert_ne!(fingerprint(&a), fingerprint(SECRET));
        assert!(!fingerprint(&a).contains(&a[6..18]));
        assert!(valid_event_id("evt_123"));
        assert!(!valid_event_id(""));
        assert!(!valid_event_id("has space"));
        assert!(!valid_event_id(&"x".repeat(129)));
    }
}
