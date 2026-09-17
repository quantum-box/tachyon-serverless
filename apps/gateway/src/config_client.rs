//! `ConfigSource` over HTTP: a data-plane gateway pulling
//! `GET /v1/internal/config?since=<generation>` from its management gateway
//! (PLT-4636, docs/adr/0007-config-distribution-and-auth-leases.md).

use std::time::Duration;

use async_trait::async_trait;

use tachyon_serverless_application::control::{ConfigDelivery, ConfigSource, SourceError};

/// Largest delivery accepted, so a broken or hostile control plane cannot
/// make a data plane buffer without bound.
const MAX_DELIVERY_BYTES: usize = 64 * 1024 * 1024;

pub struct HttpConfigSource {
    client: reqwest::Client,
    base: String,
    token: String,
}

impl std::fmt::Debug for HttpConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfigSource")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl HttpConfigSource {
    pub fn new(base: &str, token: &str, timeout: Duration) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .build()?;
        Ok(Self {
            client,
            base: base.trim_end_matches('/').to_string(),
            token: token.to_string(),
        })
    }
}

#[async_trait]
impl ConfigSource for HttpConfigSource {
    fn describe(&self) -> String {
        format!("http:{}", self.base)
    }

    async fn fetch(&self, since: u64) -> Result<ConfigDelivery, SourceError> {
        let url = format!("{}/v1/internal/config?since={since}", self.base);
        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| SourceError::Unavailable(format!("GET {url}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| SourceError::Unavailable(format!("GET {url}: reading the body: {e}")))?;
        if bytes.len() > MAX_DELIVERY_BYTES {
            return Err(SourceError::Rejected(format!(
                "GET {url}: delivery of {} bytes exceeds {MAX_DELIVERY_BYTES}",
                bytes.len()
            )));
        }
        if status.is_server_error() {
            return Err(SourceError::Unavailable(format!("GET {url}: {status}")));
        }
        if !status.is_success() {
            return Err(SourceError::Rejected(format!(
                "GET {url}: {status}: {}",
                String::from_utf8_lossy(&bytes[..bytes.len().min(512)])
            )));
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| SourceError::Rejected(format!("GET {url}: malformed delivery: {e}")))
    }
}
