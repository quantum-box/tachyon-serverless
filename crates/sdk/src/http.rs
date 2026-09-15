//! `tachyon.http.v1` adapter: turns an [`HttpRequestEvent`] into an
//! `http::Request`, dispatches it to an axum [`Router`] as a `tower::Service`
//! and packs the response into an [`HttpResponsePayload`]. No TCP involved.

use axum::Router;
use axum::body::Body;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use http::header::{HeaderName, HeaderValue};
use http::{Method, Request};
use http_body_util::BodyExt;
use tachyon_serverless_protocol::runtime_api::{HttpRequestEvent, HttpResponsePayload};
use tower::ServiceExt;

use crate::SdkError;

/// Dispatch one HTTP event to `router` and return the wire payload.
///
/// - `uri` = `path` + `?` + `query` (when the query is non-empty)
/// - repeated request headers are preserved in order
/// - bodies travel base64-encoded in both directions
pub async fn handle_http_event(
    router: &Router,
    event: HttpRequestEvent,
) -> Result<HttpResponsePayload, SdkError> {
    let request = build_request(event)?;
    // `Router` is an infallible `Service<Request<Body>>`.
    let response = match router.clone().oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|e| SdkError::Protocol(format!("failed to read handler response body: {e}")))?
        .to_bytes();
    Ok(HttpResponsePayload {
        status,
        headers,
        body_base64: BASE64.encode(&body),
    })
}

fn build_request(event: HttpRequestEvent) -> Result<Request<Body>, SdkError> {
    let method = Method::from_bytes(event.method.as_bytes())
        .map_err(|e| SdkError::InvalidHttpEvent(format!("method {:?}: {e}", event.method)))?;
    let path = if event.path.starts_with('/') {
        event.path.clone()
    } else {
        format!("/{}", event.path)
    };
    let uri = if event.query.is_empty() {
        path
    } else {
        format!("{path}?{}", event.query)
    };
    let mut builder = Request::builder().method(method).uri(uri.as_str());
    {
        let headers = builder
            .headers_mut()
            .ok_or_else(|| SdkError::InvalidHttpEvent(format!("invalid request line for {uri}")))?;
        for (name, value) in &event.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| SdkError::InvalidHttpEvent(format!("header name {name:?}: {e}")))?;
            let value = HeaderValue::from_str(value)
                .map_err(|e| SdkError::InvalidHttpEvent(format!("header value for {name}: {e}")))?;
            headers.append(name, value);
        }
    }
    let body = BASE64
        .decode(event.body_base64.as_bytes())
        .map_err(|e| SdkError::InvalidHttpEvent(format!("body_base64: {e}")))?;
    builder
        .body(Body::from(body))
        .map_err(|e| SdkError::InvalidHttpEvent(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, Query};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use std::collections::BTreeMap;

    fn router() -> Router {
        Router::new()
            .route("/", get(|| async { "ok" }))
            .route(
                "/headers",
                get(|headers: HeaderMap| async move {
                    let all: Vec<(String, String)> = headers
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
                        .collect();
                    axum::Json(all)
                }),
            )
            .route(
                "/echo",
                post(|headers: HeaderMap, body: bytes::Bytes| async move {
                    let ct = headers
                        .get("content-type")
                        .cloned()
                        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
                    ([(http::header::CONTENT_TYPE, ct)], body)
                }),
            )
            .route(
                "/status/{code}",
                get(|Path(code): Path<u16>| async move {
                    (
                        StatusCode::from_u16(code).unwrap(),
                        format!("status {code}"),
                    )
                }),
            )
            .route(
                "/q",
                get(|Query(q): Query<BTreeMap<String, String>>| async move { axum::Json(q) }),
            )
    }

    fn event(method: &str, path: &str) -> HttpRequestEvent {
        HttpRequestEvent {
            method: method.into(),
            path: path.into(),
            query: String::new(),
            headers: vec![],
            body_base64: String::new(),
            source_ip: None,
        }
    }

    #[tokio::test]
    async fn get_root() {
        let r = handle_http_event(&router(), event("GET", "/"))
            .await
            .unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(BASE64.decode(&r.body_base64).unwrap(), b"ok");
        assert!(
            r.headers
                .iter()
                .any(|(k, v)| k == "content-type" && v.starts_with("text/plain"))
        );
    }

    #[tokio::test]
    async fn echo_binary_body_keeps_content_type() {
        let body: Vec<u8> = (0..=255u8).collect();
        let mut e = event("POST", "/echo");
        e.headers
            .push(("content-type".into(), "application/x-bytes".into()));
        e.body_base64 = BASE64.encode(&body);
        let r = handle_http_event(&router(), e).await.unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(BASE64.decode(&r.body_base64).unwrap(), body);
        assert!(r.headers.contains(&(
            "content-type".to_string(),
            "application/x-bytes".to_string()
        )));
    }

    #[tokio::test]
    async fn repeated_headers_are_preserved() {
        let mut e = event("GET", "/headers");
        e.headers.push(("x-multi".into(), "1".into()));
        e.headers.push(("x-multi".into(), "2".into()));
        e.headers.push(("X-Single".into(), "s".into()));
        let r = handle_http_event(&router(), e).await.unwrap();
        assert_eq!(r.status, 200);
        let seen: Vec<(String, String)> =
            serde_json::from_slice(&BASE64.decode(&r.body_base64).unwrap()).unwrap();
        let multi: Vec<&str> = seen
            .iter()
            .filter(|(k, _)| k == "x-multi")
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(multi, vec!["1", "2"]);
        assert!(seen.contains(&("x-single".to_string(), "s".to_string())));
    }

    #[tokio::test]
    async fn status_and_query() {
        let r = handle_http_event(&router(), event("GET", "/status/418"))
            .await
            .unwrap();
        assert_eq!(r.status, 418);
        assert_eq!(BASE64.decode(&r.body_base64).unwrap(), b"status 418");

        let mut e = event("GET", "/q");
        e.query = "a=1&b=two".into();
        let r = handle_http_event(&router(), e).await.unwrap();
        let q: BTreeMap<String, String> =
            serde_json::from_slice(&BASE64.decode(&r.body_base64).unwrap()).unwrap();
        assert_eq!(q["a"], "1");
        assert_eq!(q["b"], "two");

        let r = handle_http_event(&router(), event("GET", "/missing"))
            .await
            .unwrap();
        assert_eq!(r.status, 404);
    }

    #[tokio::test]
    async fn invalid_event_is_rejected() {
        let mut e = event("GET", "/");
        e.body_base64 = "*not base64*".into();
        assert!(matches!(
            handle_http_event(&router(), e).await,
            Err(SdkError::InvalidHttpEvent(_))
        ));
        let mut e = event("GET", "/");
        e.headers.push(("bad header".into(), "x".into()));
        assert!(handle_http_event(&router(), e).await.is_err());
    }
}
