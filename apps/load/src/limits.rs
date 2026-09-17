//! Declared load limits and the target allowlist.
//!
//! A scenario declares its limits (concurrency, requests, duration) on the
//! command line; they are recorded with the results, checked against the plan
//! before anything is sent, and enforced while it runs. The limits can never
//! exceed the compiled-in ceilings, and load is only ever sent to a loopback
//! gateway or a lab host named explicitly.

use serde::{Deserialize, Serialize};

/// Ceilings no declaration can raise.
pub const CEILING_CONCURRENCY: u32 = 64;
pub const CEILING_REQUESTS: u64 = 2_000;
pub const CEILING_DURATION_SECONDS: u64 = 1_800;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadLimits {
    /// Invocations in flight at once, per `load` run.
    pub max_concurrency: u32,
    /// Invocations sent over the whole scenario (every `load` run of it).
    pub max_requests: u64,
    /// Wall time of the whole scenario from its first `load` run.
    pub max_duration_seconds: u64,
}

impl LoadLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_concurrency == 0 || self.max_concurrency > CEILING_CONCURRENCY {
            return Err(format!(
                "max_concurrency {} must be within 1..={CEILING_CONCURRENCY}",
                self.max_concurrency
            ));
        }
        if self.max_requests == 0 || self.max_requests > CEILING_REQUESTS {
            return Err(format!(
                "max_requests {} must be within 1..={CEILING_REQUESTS}",
                self.max_requests
            ));
        }
        if self.max_duration_seconds == 0 || self.max_duration_seconds > CEILING_DURATION_SECONDS {
            return Err(format!(
                "max_duration_seconds {} must be within 1..={CEILING_DURATION_SECONDS}",
                self.max_duration_seconds
            ));
        }
        Ok(())
    }
}

/// Refuse any target that is not loopback or an explicitly allowed lab host.
///
/// The host is compared literally: names other than `localhost` are not
/// resolved (a DNS answer could point anywhere), redirects are never followed
/// by the client, and only `http` / `https` are accepted.
pub fn check_target(base_url: &str, lab_hosts: &[String]) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(base_url).map_err(|e| format!("invalid url {base_url}: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("scheme `{}` is not allowed", url.scheme()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("credentials in the url are not allowed".into());
    }
    let host = url.host_str().unwrap_or_default().to_string();
    let loopback = matches!(host.as_str(), "127.0.0.1" | "[::1]" | "::1" | "localhost");
    if loopback || lab_hosts.iter().any(|h| h.eq_ignore_ascii_case(&host)) {
        Ok(url)
    } else {
        Err(format!(
            "refusing to send load to `{host}`: only 127.0.0.1, ::1, localhost or an explicit \
             --lab-host are allowed (never a production or external host)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_loopback_or_an_explicit_lab_host_is_a_load_target() {
        for ok in [
            "http://127.0.0.1:8080",
            "http://localhost:1",
            "http://[::1]:9000/",
        ] {
            assert!(check_target(ok, &[]).is_ok(), "{ok}");
        }
        for bad in [
            "http://10.0.0.5:8080",
            "https://api.example.com",
            "http://127.0.0.2:80",
            "http://localhost.example.com",
            "http://user:pw@127.0.0.1:1",
            "ftp://127.0.0.1/",
            "not a url",
        ] {
            assert!(check_target(bad, &[]).is_err(), "{bad}");
        }
        assert!(check_target("http://lab-kvm.local:8080", &["lab-kvm.local".into()]).is_ok());
        assert!(check_target("http://lab-kvm.local:8080", &[]).is_err());
    }

    #[test]
    fn declared_limits_never_exceed_the_ceilings() {
        let ok = LoadLimits {
            max_concurrency: CEILING_CONCURRENCY,
            max_requests: CEILING_REQUESTS,
            max_duration_seconds: CEILING_DURATION_SECONDS,
        };
        assert!(ok.validate().is_ok());
        for bad in [
            LoadLimits {
                max_concurrency: CEILING_CONCURRENCY + 1,
                ..ok
            },
            LoadLimits {
                max_requests: CEILING_REQUESTS + 1,
                ..ok
            },
            LoadLimits {
                max_duration_seconds: CEILING_DURATION_SECONDS + 1,
                ..ok
            },
            LoadLimits {
                max_concurrency: 0,
                ..ok
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }
}
