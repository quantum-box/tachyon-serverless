//! Thin HTTP client over the gateway API (`crates/api-types`).
//!
//! Every response is returned raw (`status`, `headers`, `body`) so that `--json`
//! can print the server JSON unmodified; typed accessors decode on top.

use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use tachyon_serverless_api_types::{ApiErrorBody, headers};

use crate::error::CliError;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub api_url: String,
    pub token: Option<String>,
    pub tenant_id: Option<String>,
    pub timeout: Duration,
    /// Use `:invoke` / `:cancel` instead of `/invoke` / `/cancel`.
    pub colon_routes: bool,
}

#[derive(Debug, Clone)]
pub struct ApiResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl ApiResponse {
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Decode the body as `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, CliError> {
        serde_json::from_slice(&self.body).map_err(|e| CliError::Http {
            status: self.status,
            body: format!("cannot decode response body ({e}): {}", self.body_text()),
        })
    }

    /// Try to decode the body as the API error shape.
    pub fn api_error(&self) -> Option<ApiErrorBody> {
        serde_json::from_slice::<ApiErrorBody>(&self.body).ok()
    }

    /// Convert a non-2xx response into the matching [`CliError`].
    pub fn into_error(self) -> CliError {
        match self.api_error() {
            Some(body) => CliError::Api {
                status: self.status,
                error: Box::new(body.error),
                raw: self.body_text(),
            },
            None => CliError::Http {
                status: self.status,
                body: self.body_text(),
            },
        }
    }

    /// Return `Ok(self)` for 2xx, otherwise the mapped error.
    pub fn ok(self) -> Result<Self, CliError> {
        if self.is_success() {
            Ok(self)
        } else {
            Err(self.into_error())
        }
    }
}

pub struct ApiClient {
    cfg: ClientConfig,
    http: reqwest::Client,
}

impl ApiClient {
    pub fn new(cfg: ClientConfig) -> Result<Self, CliError> {
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .build()
            .map_err(|e| CliError::Usage(format!("cannot build HTTP client: {e}")))?;
        Ok(Self { cfg, http })
    }

    pub fn config(&self) -> &ClientConfig {
        &self.cfg
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.cfg.api_url.trim_end_matches('/'), path)
    }

    /// Path of the invoke route for a function id.
    pub fn invoke_path(&self, function_id: &str) -> String {
        if self.cfg.colon_routes {
            format!("/v1/functions/{function_id}:invoke")
        } else {
            format!("/v1/functions/{function_id}/invoke")
        }
    }

    /// Path of the cancel route for an invocation id.
    pub fn cancel_path(&self, invocation_id: &str) -> String {
        if self.cfg.colon_routes {
            format!("/v1/invocations/{invocation_id}:cancel")
        } else {
            format!("/v1/invocations/{invocation_id}/cancel")
        }
    }

    fn require_token(&self) -> Result<&str, CliError> {
        self.cfg.token.as_deref().ok_or_else(|| {
            CliError::usage("no token: pass --token or set TSLS_TOKEN (see docs/cli.md)")
        })
    }

    /// Send a request. `auth = true` requires a token and attaches the bearer /
    /// tenant headers; unauthenticated endpoints (`/healthz`) pass `false`.
    pub async fn send(
        &self,
        method: Method,
        path: &str,
        auth: bool,
        content_type: Option<&str>,
        extra_headers: &[(String, String)],
        body: Option<Bytes>,
    ) -> Result<ApiResponse, CliError> {
        let url = self.url(path);
        let mut req = self.http.request(method, &url);
        if auth {
            let token = self.require_token()?;
            req = req.bearer_auth(token);
            if let Some(t) = &self.cfg.tenant_id {
                req = req.header(headers::TENANT_ID, t);
            }
        } else if let Some(token) = &self.cfg.token {
            req = req.bearer_auth(token);
        }
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        for (k, v) in extra_headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| CliError::usage(format!("invalid header name `{k}`: {e}")))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| CliError::usage(format!("invalid header value for `{k}`: {e}")))?;
            req = req.header(name, value);
        }
        if let Some(b) = body {
            req = req.body(b);
        }
        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                CliError::Timeout(format!("request to {url} exceeded --timeout-secs"))
            } else {
                CliError::Transport(format!("{url}: {e}"))
            }
        })?;
        let status: StatusCode = resp.status();
        let headers = resp.headers().clone();
        let body = resp
            .bytes()
            .await
            .map_err(|e| CliError::Transport(format!("{url}: reading body: {e}")))?;
        Ok(ApiResponse {
            status: status.as_u16(),
            headers,
            body,
        })
    }

    pub async fn get(&self, path: &str) -> Result<ApiResponse, CliError> {
        self.send(Method::GET, path, true, None, &[], None).await
    }

    pub async fn get_unauth(&self, path: &str) -> Result<ApiResponse, CliError> {
        self.send(Method::GET, path, false, None, &[], None).await
    }

    pub async fn delete(&self, path: &str) -> Result<ApiResponse, CliError> {
        self.send(Method::DELETE, path, true, None, &[], None).await
    }

    pub async fn post_json<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<ApiResponse, CliError> {
        let bytes = Bytes::from(serde_json::to_vec(body)?);
        self.send(
            Method::POST,
            path,
            true,
            Some("application/json"),
            &[],
            Some(bytes),
        )
        .await
    }

    pub async fn put_json<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<ApiResponse, CliError> {
        let bytes = Bytes::from(serde_json::to_vec(body)?);
        self.send(
            Method::PUT,
            path,
            true,
            Some("application/json"),
            &[],
            Some(bytes),
        )
        .await
    }

    /// `GET` and decode, mapping non-2xx to errors.
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, CliError> {
        self.get(path).await?.ok()?.json()
    }
}
