//! Triggers over HTTP (PLT-4641): CRUD with the bearer token, the webhook
//! endpoint with the signature only, and the refusals that must happen before
//! anything is stored (checked on the ledger).

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use tower::ServiceExt;

use tachyon_serverless_application::services::triggers::webhook::sign;
use tachyon_serverless_application::{Application, BootstrapOptions, GatewayConfig};
use tachyon_serverless_domain::FunctionId;
use tachyon_serverless_gateway::router;
use tachyon_serverless_provider_fake::FakeExecutionProvider;

const TOKEN_A: &str = "trigger-token-a";
const TOKEN_B: &str = "trigger-token-b";
const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

struct Api {
    router: Router,
    app: Arc<Application>,
    _dir: tempfile::TempDir,
}

fn api() -> Api {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("triggers.key");
    std::fs::write(&key, "ab".repeat(32)).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let toml = format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process"

[[identity.tokens]]
token = "{TOKEN_A}"
tenant_id = "{TENANT_A}"
subject = "a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "{TOKEN_B}"
tenant_id = "{TENANT_B}"
subject = "b"
roles = ["deploy", "invoke"]

[queue]
backend = "sqlite"

[triggers]
scheduler_enabled = false
webhook_max_body_bytes = 2048
secret_key_file = "{key}"
"#,
        data = dir.path().display(),
        key = key.display()
    );
    let app = Application::bootstrap_with(
        GatewayConfig::from_toml(&toml).unwrap(),
        Arc::new(FakeExecutionProvider::new()),
        BootstrapOptions::default(),
    )
    .unwrap();
    Api {
        router: router(app.clone()),
        app,
        _dir: dir,
    }
}

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

async fn call(router: &Router, req: Request<Body>) -> Reply {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap()
        .to_vec();
    Reply {
        status,
        headers,
        body,
    }
}

async fn send(
    router: &Router,
    method: Method,
    path: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> Reply {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    let body = match body {
        Some(v) => {
            b = b.header(header::CONTENT_TYPE, "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    call(router, b.body(body).unwrap()).await
}

async fn deploy(api: &Api, token: &str) -> String {
    let r = &api.router;
    let f = send(
        r,
        Method::POST,
        "/v1/functions",
        token,
        Some(serde_json::json!({"name": "hooked"})),
    )
    .await;
    assert_eq!(f.status, StatusCode::CREATED, "{}", f.text());
    let function_id = f.json()["id"].as_str().unwrap().to_string();
    let upload = call(
        r,
        Request::builder()
            .method(Method::POST)
            .uri("/v1/artifacts")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from("#!/bin/sh\necho hooked\n"))
            .unwrap(),
    )
    .await;
    let digest = upload.json()["digest"].as_str().unwrap().to_string();
    let rev = send(
        r,
        Method::POST,
        &format!("/v1/functions/{function_id}/revisions"),
        token,
        Some(serde_json::json!({
            "artifact": {"kind": "binary", "digest": digest},
            "architecture": "aarch64",
            "publish_to_prod": true
        })),
    )
    .await;
    assert_eq!(rev.status, StatusCode::ACCEPTED, "{}", rev.text());
    let rev_id = rev.json()["id"].as_str().unwrap().to_string();
    for _ in 0..100 {
        let r = send(
            r,
            Method::GET,
            &format!("/v1/functions/{function_id}/revisions/{rev_id}"),
            token,
            None,
        )
        .await;
        if r.json()["status"] == "ready" {
            return function_id;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("revision never became ready");
}

fn invocations(api: &Api, function_id: &str) -> usize {
    api.app
        .repos
        .invocations
        .list_by_function(&FunctionId::parse(function_id).unwrap(), 1000)
        .unwrap()
        .len()
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn deliver(
    router: &Router,
    trigger_id: &str,
    secret: &str,
    ts: i64,
    event: &str,
    body: &[u8],
) -> Reply {
    call(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(format!("/v1/hooks/{trigger_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-tachyon-webhook-timestamp", ts.to_string())
            .header("x-tachyon-webhook-signature", sign(secret, ts, body))
            .header("x-tachyon-webhook-id", event)
            .body(Body::from(body.to_vec()))
            .unwrap(),
    )
    .await
}

/// The secret is in the 201 only (with `Cache-Control: no-store`); GET, list
/// and PATCH never show it. Another tenant gets 404 on every trigger route.
#[tokio::test]
async fn trigger_http_crud_never_returns_the_secret_and_never_crosses_a_tenant() {
    let api = api();
    let r = &api.router;
    let function_id = deploy(&api, TOKEN_A).await;
    let base = format!("/v1/functions/{function_id}/triggers");
    let created = send(
        r,
        Method::POST,
        &base,
        TOKEN_A,
        Some(serde_json::json!({"name": "orders", "kind": "webhook"})),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.text());
    assert_eq!(
        created.headers.get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    let body = created.json();
    let secret = body["secret"].as_str().unwrap().to_string();
    assert!(secret.starts_with("whsec_"));
    let id = body["id"].as_str().unwrap().to_string();
    assert_eq!(body["webhook"]["url"], format!("/v1/hooks/{id}"));
    let item = format!("{base}/{id}");

    for reply in [
        send(r, Method::GET, &item, TOKEN_A, None).await,
        send(r, Method::GET, &base, TOKEN_A, None).await,
        send(
            r,
            Method::PATCH,
            &item,
            TOKEN_A,
            Some(serde_json::json!({"tolerance_seconds": 120})),
        )
        .await,
    ] {
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
        assert!(!reply.text().contains(&secret[6..]), "{}", reply.text());
        assert!(!reply.text().contains("\"secret\""), "{}", reply.text());
    }
    let openapi = call(
        r,
        Request::builder()
            .uri("/openapi.json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(!openapi.text().contains(&secret[6..]));

    for (method, path, body) in [
        (Method::GET, item.clone(), None),
        (Method::GET, base.clone(), None),
        (Method::GET, format!("{item}/fires"), None),
        (
            Method::PATCH,
            item.clone(),
            Some(serde_json::json!({"enabled": false})),
        ),
        (Method::DELETE, item.clone(), None),
        (
            Method::POST,
            base.clone(),
            Some(serde_json::json!({"name": "x", "kind": "webhook"})),
        ),
    ] {
        let reply = send(r, method.clone(), &path, TOKEN_B, body).await;
        assert_eq!(
            reply.status,
            StatusCode::NOT_FOUND,
            "{method} {path}: {}",
            reply.text()
        );
    }
    // Still enabled and intact for its owner.
    let own = send(r, Method::GET, &item, TOKEN_A, None).await;
    assert_eq!(own.json()["enabled"], true);
    // No bearer token on CRUD: 401.
    let anon = call(
        r,
        Request::builder().uri(&base).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(anon.status, StatusCode::UNAUTHORIZED);
}

/// The webhook endpoint needs no bearer token: a valid signature is accepted
/// (202, the invocation id), a resend of the event id answers the same
/// invocation, and every refusal (bad signature, expired, oversized by
/// Content-Length or while streaming, unknown trigger) leaves no invocation.
#[tokio::test]
async fn webhook_http_refuses_before_storing_and_dedups_resends() {
    let api = api();
    let r = &api.router;
    let function_id = deploy(&api, TOKEN_A).await;
    let created = send(
        r,
        Method::POST,
        &format!("/v1/functions/{function_id}/triggers"),
        TOKEN_A,
        Some(serde_json::json!({
            "name": "orders", "kind": "webhook", "webhook": {"max_body_bytes": 512}
        })),
    )
    .await;
    let secret = created.json()["secret"].as_str().unwrap().to_string();
    let id = created.json()["id"].as_str().unwrap().to_string();
    let body = br#"{"order":7}"#;

    // Invalid signature.
    let bad = deliver(r, &id, "whsec_wrong", now(), "evt-1", body).await;
    assert_eq!(bad.status, StatusCode::UNAUTHORIZED, "{}", bad.text());
    // Expired.
    let old = deliver(r, &id, &secret, now() - 3600, "evt-1", body).await;
    assert_eq!(old.status, StatusCode::UNAUTHORIZED, "{}", old.text());
    // Oversized by Content-Length: refused before the body is read.
    let big = vec![b' '; 513];
    let over = deliver(r, &id, &secret, now(), "evt-1", &big).await;
    assert_eq!(
        over.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{}",
        over.text()
    );
    // Oversized while streaming (no Content-Length).
    let chunks = futures::stream::iter(
        (0..64).map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![b' '; 64]))),
    );
    let ts = now();
    let streamed = call(
        r,
        Request::builder()
            .method(Method::POST)
            .uri(format!("/v1/hooks/{id}"))
            .header("x-tachyon-webhook-timestamp", ts.to_string())
            .header("x-tachyon-webhook-signature", sign(&secret, ts, b""))
            .header("x-tachyon-webhook-id", "evt-1")
            .body(Body::from_stream(chunks))
            .unwrap(),
    )
    .await;
    assert_eq!(
        streamed.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{}",
        streamed.text()
    );
    // Unknown trigger and a malformed id: the same 404.
    let unknown = deliver(
        r,
        "trg_01hzzzzzzzzzzzzzzzzzzzzzzz",
        &secret,
        now(),
        "evt-1",
        body,
    )
    .await;
    let malformed = deliver(r, "nope", &secret, now(), "evt-1", body).await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    assert_eq!(malformed.status, StatusCode::NOT_FOUND);
    assert_eq!(
        unknown.json()["error"]["message"],
        malformed.json()["error"]["message"]
    );
    assert_eq!(
        invocations(&api, &function_id),
        0,
        "no refusal stored anything"
    );

    let ok = deliver(r, &id, &secret, now(), "evt-1", body).await;
    assert_eq!(ok.status, StatusCode::ACCEPTED, "{}", ok.text());
    let first = ok.json();
    assert_eq!(first["replayed"], false);
    assert_eq!(first["status"], "accepted");
    let inv = first["invocation_id"].as_str().unwrap().to_string();
    assert_eq!(
        ok.headers.get("x-tachyon-invocation-id").unwrap(),
        inv.as_str()
    );
    let again = deliver(r, &id, &secret, now(), "evt-1", body).await;
    assert_eq!(again.status, StatusCode::ACCEPTED);
    assert_eq!(again.json()["invocation_id"], inv.as_str());
    assert_eq!(again.json()["replayed"], true);
    assert_eq!(invocations(&api, &function_id), 1);

    // The invocation is readable with the tenant's token like any other.
    let status = send(
        r,
        Method::GET,
        &format!("/v1/invocations/{inv}"),
        TOKEN_A,
        None,
    )
    .await;
    assert_eq!(status.status, StatusCode::OK);
    assert_eq!(status.json()["mode"], "async");
    let fires = send(
        r,
        Method::GET,
        &format!("/v1/functions/{function_id}/triggers/{id}/fires"),
        TOKEN_A,
        None,
    )
    .await;
    assert_eq!(fires.json()["items"][0]["event_id"], "evt-1");
    assert_eq!(fires.json()["items"][0]["invocation_id"], inv.as_str());

    // Disabled: a signed delivery gets 410, an unsigned one still 401.
    send(
        r,
        Method::PATCH,
        &format!("/v1/functions/{function_id}/triggers/{id}"),
        TOKEN_A,
        Some(serde_json::json!({"enabled": false})),
    )
    .await;
    let gone = deliver(r, &id, &secret, now(), "evt-2", body).await;
    assert_eq!(gone.status, StatusCode::GONE, "{}", gone.text());
    let unsigned = deliver(r, &id, "whsec_wrong", now(), "evt-2", body).await;
    assert_eq!(unsigned.status, StatusCode::UNAUTHORIZED);
    // Deleted: the same 404 as an unknown trigger.
    send(
        r,
        Method::DELETE,
        &format!("/v1/functions/{function_id}/triggers/{id}"),
        TOKEN_A,
        None,
    )
    .await;
    let deleted = deliver(r, &id, &secret, now(), "evt-3", body).await;
    assert_eq!(deleted.status, StatusCode::NOT_FOUND);
    assert_eq!(
        deleted.json()["error"]["message"],
        unknown.json()["error"]["message"]
    );
    assert_eq!(invocations(&api, &function_id), 1);
}
