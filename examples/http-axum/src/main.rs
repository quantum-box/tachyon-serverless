//! `http-axum`: an ordinary axum router served through
//! `tachyon_serverless_sdk::serve_http`.
//!
//! The gateway wraps each HTTP request as a `tachyon.http.v1` event; the SDK
//! turns it into an `http::Request` and calls this router in-process. No TCP
//! listener is opened and no network access is performed.
//!
//! Routes:
//! - `GET /`               -> `ok`
//! - `GET /headers`        -> JSON object of the request headers
//! - `POST /echo`          -> the request body, same content-type
//! - `GET /status/{code}`  -> that status with a short text body
//! - `GET /env`            -> `{"greeting": $GREETING | null}`

use axum::Router;
use axum::body::Bytes;
use axum::extract::Path;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde_json::json;
use tachyon_serverless_sdk::SdkError;

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    tachyon_serverless_sdk::serve_http(router()).await
}

fn router() -> Router {
    Router::new()
        .route("/", get(|| async { "ok" }))
        .route("/headers", get(headers))
        .route("/echo", post(echo))
        .route("/status/{code}", get(status))
        .route("/env", get(env))
}

async fn headers(headers: HeaderMap) -> axum::Json<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (name, value) in &headers {
        let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        match map.get_mut(name.as_str()) {
            // Repeated headers become a JSON array.
            Some(serde_json::Value::Array(items)) => items.push(value.into()),
            Some(existing) => {
                let first = existing.take();
                *existing = json!([first, value]);
            }
            None => {
                map.insert(name.as_str().to_string(), value.into());
            }
        }
    }
    axum::Json(serde_json::Value::Object(map))
}

async fn echo(headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
    ([(header::CONTENT_TYPE, content_type)], body)
}

async fn status(Path(code): Path<u16>) -> impl IntoResponse {
    match StatusCode::from_u16(code) {
        Ok(status) => (status, format!("status {code}")),
        Err(_) => (
            StatusCode::BAD_REQUEST,
            format!("invalid status code {code}"),
        ),
    }
}

async fn env() -> axum::Json<serde_json::Value> {
    axum::Json(json!({ "greeting": std::env::var("GREETING").ok() }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use tachyon_serverless_protocol::runtime_api::HttpRequestEvent;
    use tachyon_serverless_sdk::handle_http_event;

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
    async fn routes_answer_through_the_sdk_adapter() {
        let r = router();
        let b64 = base64::engine::general_purpose::STANDARD;

        let resp = handle_http_event(&r, event("GET", "/")).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(b64.decode(resp.body_base64).unwrap(), b"ok");

        let mut e = event("POST", "/echo");
        e.headers
            .push(("content-type".into(), "text/x-demo".into()));
        e.body_base64 = b64.encode(b"payload");
        let resp = handle_http_event(&r, e).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(b64.decode(resp.body_base64).unwrap(), b"payload");
        assert!(
            resp.headers
                .contains(&("content-type".into(), "text/x-demo".into()))
        );

        let resp = handle_http_event(&r, event("GET", "/status/503"))
            .await
            .unwrap();
        assert_eq!(resp.status, 503);

        let mut e = event("GET", "/headers");
        e.headers.push(("x-a".into(), "1".into()));
        e.headers.push(("x-a".into(), "2".into()));
        let resp = handle_http_event(&r, e).await.unwrap();
        let v: serde_json::Value =
            serde_json::from_slice(&b64.decode(resp.body_base64).unwrap()).unwrap();
        assert_eq!(v["x-a"], json!(["1", "2"]));

        let resp = handle_http_event(&r, event("GET", "/env")).await.unwrap();
        assert_eq!(resp.status, 200);
    }
}
