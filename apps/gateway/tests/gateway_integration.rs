//! End-to-end test of the HTTP API against the fake provider, driven through
//! `tower::ServiceExt::oneshot` on `router()`.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use base64::Engine;
use tower::ServiceExt;

use tachyon_serverless_application::{Application, BootstrapOptions, GatewayConfig};
use tachyon_serverless_gateway::router;
use tachyon_serverless_provider_fake::{FakeExecutionProvider, FakeGuestScript};

const TOKEN_A: &str = "dev-token-tenant-a";
const TOKEN_B: &str = "dev-token-tenant-b";
/// `roles = ["operator"]` on a tenant that owns nothing.
const TOKEN_OP: &str = "dev-token-operator";
/// `roles = ["operator"]` on tenant A.
const TOKEN_A_OP: &str = "dev-token-operator-a";
const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";
const TENANT_OP: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzc";

fn config(data_dir: &std::path::Path) -> GatewayConfig {
    GatewayConfig::from_toml(&config_toml(data_dir)).unwrap()
}

fn config_toml(data_dir: &std::path::Path) -> String {
    format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process"

[invoke]
cancel_grace_ms = 100

[[identity.tokens]]
token = "{TOKEN_A}"
tenant_id = "{TENANT_A}"
subject = "dev-a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "{TOKEN_B}"
tenant_id = "{TENANT_B}"
subject = "dev-b"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "{TOKEN_OP}"
tenant_id = "{TENANT_OP}"
subject = "operator"
roles = ["operator"]

[[identity.tokens]]
token = "{TOKEN_A_OP}"
tenant_id = "{TENANT_A}"
subject = "operator-a"
roles = ["operator"]

[[secrets.bindings]]
tenant_id = "{TENANT_A}"
binding_ref = "demo-secret"
value = "demo-secret-value-a"
"#,
        data = data_dir.display()
    )
}

struct Api {
    router: Router,
    _dir: tempfile::TempDir,
    fake: Arc<FakeExecutionProvider>,
}

fn api(scripts: Vec<FakeGuestScript>) -> Api {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::with_scripts(scripts));
    let app = Application::bootstrap_with(
        config(dir.path()),
        fake.clone(),
        BootstrapOptions {
            persist_state: false,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    Api {
        router: router(app),
        _dir: dir,
        fake,
    }
}

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "body is not JSON ({e}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

async fn call(router: &Router, req: Request<Body>) -> Reply {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = axum::body::to_bytes(res.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap()
        .to_vec();
    Reply {
        status,
        headers,
        body,
    }
}

fn req(method: Method, path: &str, token: Option<&str>) -> axum::http::request::Builder {
    let mut b = Request::builder().method(method).uri(path);
    if let Some(t) = token {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    b
}

fn json_body(v: serde_json::Value) -> Body {
    Body::from(serde_json::to_vec(&v).unwrap())
}

async fn get(router: &Router, path: &str, token: &str) -> Reply {
    call(
        router,
        req(Method::GET, path, Some(token))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

async fn post_json(router: &Router, path: &str, token: &str, v: serde_json::Value) -> Reply {
    call(
        router,
        req(Method::POST, path, Some(token))
            .header(header::CONTENT_TYPE, "application/json")
            .body(json_body(v))
            .unwrap(),
    )
    .await
}

/// create function -> upload artifact -> create revision -> wait ready.
async fn deploy(router: &Router, name: &str) -> (String, String) {
    let created = post_json(
        router,
        "/v1/functions",
        TOKEN_A,
        serde_json::json!({"name": name, "description": "e2e"}),
    )
    .await;
    assert_eq!(
        created.status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let function_id = created.json()["id"].as_str().unwrap().to_string();
    assert_eq!(created.json()["tenant_id"], TENANT_A);

    let upload = call(
        router,
        req(Method::POST, "/v1/artifacts", Some(TOKEN_A))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(format!("#!/bin/sh\necho {name}\n")))
            .unwrap(),
    )
    .await;
    assert_eq!(
        upload.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&upload.body)
    );
    let digest = upload.json()["digest"].as_str().unwrap().to_string();
    assert!(digest.starts_with("sha256:"));

    let rev = post_json(
        router,
        &format!("/v1/functions/{function_id}/revisions"),
        TOKEN_A,
        serde_json::json!({
            "artifact": {"kind": "binary", "digest": digest},
            "architecture": "aarch64",
            "execution": {"timeout_seconds": 5, "initialization_timeout_seconds": 5, "max_concurrency": 4},
            "env_vars": [["GREETING", "hi"]],
            "secrets": [{"env_name": "DEMO_SECRET", "binding_ref": "demo-secret"}],
            "publish_to_prod": true
        }),
    )
    .await;
    assert_eq!(
        rev.status,
        StatusCode::ACCEPTED,
        "{}",
        String::from_utf8_lossy(&rev.body)
    );
    let revision_id = rev.json()["id"].as_str().unwrap().to_string();
    assert_eq!(rev.json()["status"], "pending");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let r = get(
            router,
            &format!("/v1/functions/{function_id}/revisions/{revision_id}"),
            TOKEN_A,
        )
        .await;
        assert_eq!(r.status, StatusCode::OK);
        match r.json()["status"].as_str().unwrap() {
            "ready" => break,
            "failed" => panic!("revision failed: {}", r.json()["failure_reason"]),
            _ if tokio::time::Instant::now() > deadline => panic!("revision never became ready"),
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let alias = get(
        router,
        &format!("/v1/functions/{function_id}/aliases/prod"),
        TOKEN_A,
    )
    .await;
    assert_eq!(alias.status, StatusCode::OK);
    assert_eq!(alias.json()["revision_id"], revision_id);
    assert_eq!(alias.json()["generation"], 1);
    (function_id, revision_id)
}

#[tokio::test]
async fn full_api_roundtrip() {
    let api = api(vec![
        FakeGuestScript::RespondOk(serde_json::json!({"greeting": "hello"})),
        FakeGuestScript::Echo,
        FakeGuestScript::EchoHttp,
        FakeGuestScript::HangForever,
    ]);
    let r = &api.router;

    // meta
    let h = get(r, "/healthz", "").await;
    assert_eq!(h.status, StatusCode::OK);
    assert!(
        h.header("x-request-id").is_some(),
        "request id is always assigned"
    );
    let ready = call(r, Request::get("/readyz").body(Body::empty()).unwrap()).await;
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(ready.json()["ready"], true);

    // auth
    let anon = call(
        r,
        Request::get("/v1/functions").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(anon.status, StatusCode::UNAUTHORIZED);
    assert_eq!(anon.json()["error"]["code"], "unauthorized");
    let bad = get(r, "/v1/functions", "nope").await;
    assert_eq!(bad.status, StatusCode::UNAUTHORIZED);
    let mismatch = call(
        r,
        req(Method::GET, "/v1/functions", Some(TOKEN_A))
            .header("x-tachyon-tenant-id", TENANT_B)
            .header("x-request-id", "req-123")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(mismatch.status, StatusCode::FORBIDDEN);
    assert_eq!(mismatch.header("x-request-id"), Some("req-123"));
    assert_eq!(mismatch.json()["error"]["request_id"], "req-123");
    let matching = call(
        r,
        req(Method::GET, "/v1/functions", Some(TOKEN_A))
            .header("x-tachyon-tenant-id", TENANT_A)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(matching.status, StatusCode::OK);

    let provider = get(r, "/v1/provider", TOKEN_A).await;
    assert_eq!(provider.status, StatusCode::OK);
    assert_eq!(provider.json()["kind"], "fake");
    assert_eq!(provider.json()["dev_only"], true);
    // PLT-4633 acceptance 4: whether environments are reused, whether that was
    // ever measured, and why, are all on the API — never only in a log.
    let reuse = provider.json()["reuse"].clone();
    assert_eq!(reuse["enabled"], false);
    assert_eq!(reuse["verified"], false);
    assert_eq!(reuse["idle_quiesce"], "unsupported");
    assert_eq!(reuse["idle_resume"], "unsupported");
    assert!(
        reuse["reason"].as_str().is_some_and(|r| !r.is_empty()),
        "the reason is always present: {reuse}"
    );

    // deploy
    let (function_id, revision_id) = deploy(r, "hello").await;
    let list = get(r, "/v1/functions", TOKEN_A).await;
    assert_eq!(list.json()["items"].as_array().unwrap().len(), 1);

    // invoke (slash form)
    let inv = call(
        r,
        req(
            Method::POST,
            &format!("/v1/functions/{function_id}/invoke"),
            Some(TOKEN_A),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .header("idempotency-key", "k1")
        .body(json_body(serde_json::json!({"name": "world"})))
        .unwrap(),
    )
    .await;
    assert_eq!(
        inv.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&inv.body)
    );
    assert_eq!(inv.json(), serde_json::json!({"greeting": "hello"}));
    let invocation_id = inv.header("x-tachyon-invocation-id").unwrap().to_string();
    assert!(invocation_id.starts_with("inv_"));

    // idempotent replay does not run the guest again
    let replay = call(
        r,
        req(
            Method::POST,
            &format!("/v1/functions/{function_id}/invoke"),
            Some(TOKEN_A),
        )
        .header("idempotency-key", "k1")
        .body(json_body(serde_json::json!({"name": "world"})))
        .unwrap(),
    )
    .await;
    assert_eq!(replay.status, StatusCode::OK);
    assert_eq!(
        replay.header("x-tachyon-invocation-id").unwrap(),
        invocation_id
    );
    assert_eq!(api.fake.created().len(), 1);

    // the same key with another input is a 409 naming the bound invocation
    // (PLT-4631, docs/api.md §2)
    let reused = call(
        r,
        req(
            Method::POST,
            &format!("/v1/functions/{function_id}/invoke"),
            Some(TOKEN_A),
        )
        .header("idempotency-key", "k1")
        .body(json_body(serde_json::json!({"name": "someone else"})))
        .unwrap(),
    )
    .await;
    assert_eq!(reused.status, StatusCode::CONFLICT);
    let body = reused.json();
    assert_eq!(body["error"]["code"], "conflict");
    assert_eq!(body["error"]["invocation_id"], invocation_id.as_str());
    assert_eq!(body["error"]["error_type"], "Host.IdempotencyKeyReused");
    assert_eq!(api.fake.created().len(), 1, "a conflict runs nothing");

    // invoke (colon form) with a pinned revision
    let inv2 = call(
        r,
        req(
            Method::POST,
            &format!("/v1/functions/{function_id}:invoke?revision_id={revision_id}"),
            Some(TOKEN_A),
        )
        .body(json_body(serde_json::json!({"echo": 1})))
        .unwrap(),
    )
    .await;
    assert_eq!(
        inv2.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&inv2.body)
    );
    assert_eq!(inv2.json(), serde_json::json!({"echo": 1}));
    assert_ne!(
        inv2.header("x-tachyon-invocation-id").unwrap(),
        invocation_id
    );

    // invocation detail with boot evidence
    let detail = get(r, &format!("/v1/invocations/{invocation_id}"), TOKEN_A).await;
    assert_eq!(detail.status, StatusCode::OK);
    let d = detail.json();
    assert_eq!(d["status"], "succeeded");
    assert_eq!(d["revision_id"], revision_id);
    assert_eq!(d["alias"], "prod");
    assert_eq!(d["output"], serde_json::json!({"greeting": "hello"}));
    let attempts = d["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0]["status"], "succeeded");
    assert!(
        attempts[0]["boot_evidence"]["guest_boot_id"]
            .as_str()
            .unwrap()
            .starts_with("fake-boot-")
    );
    assert!(attempts[0]["boot_evidence"]["host_pid"].as_u64().is_some());
    assert!(attempts[0]["timings"]["handler_ms"].as_u64().is_some());
    assert!(
        attempts[0]["timings"]["environment_boot_ms"]
            .as_u64()
            .is_some()
    );

    // logs
    let logs = get(r, &format!("/v1/invocations/{invocation_id}/logs"), TOKEN_A).await;
    assert_eq!(logs.status, StatusCode::OK);
    let items = logs.json()["items"].as_array().unwrap().clone();
    assert!(!items.is_empty());
    assert!(
        items
            .iter()
            .any(|l| l["phase"] == "handler" && l["stream"] == "stdout")
    );
    assert!(
        items
            .iter()
            .any(|l| l["phase"] == "boot" && l["stream"] == "platform")
    );
    assert_eq!(logs.json()["dropped"], false);

    // history + usage
    let history = get(
        r,
        &format!("/v1/functions/{function_id}/invocations?limit=10"),
        TOKEN_A,
    )
    .await;
    assert_eq!(history.status, StatusCode::OK);
    assert_eq!(history.json()["items"].as_array().unwrap().len(), 2);
    let usage = get(r, &format!("/v1/functions/{function_id}/usage"), TOKEN_A).await;
    assert_eq!(usage.status, StatusCode::OK);
    assert_eq!(usage.json()["invocations"], 2);
    assert_eq!(usage.json()["succeeded"], 2);
    assert_eq!(usage.json()["not_billable"], true);

    // HTTP adapter roundtrip
    let http = call(
        r,
        req(
            Method::PUT,
            &format!("/v1/functions/{function_id}/http/items/42?verbose=1&x=y"),
            Some(TOKEN_A),
        )
        .header("x-custom", "one")
        .header("x-custom", "two")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("payload"))
        .unwrap(),
    )
    .await;
    assert_eq!(
        http.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&http.body)
    );
    assert!(
        http.header("x-tachyon-invocation-id")
            .unwrap()
            .starts_with("inv_")
    );
    assert_eq!(http.header("x-echo-method"), Some("PUT"));
    assert_eq!(http.header("x-echo-path"), Some("/items/42"));
    assert_eq!(http.header("content-type"), Some("application/json"));
    let event = http.json();
    assert_eq!(event["method"], "PUT");
    assert_eq!(event["path"], "/items/42");
    assert_eq!(event["query"], "verbose=1&x=y");
    let hdrs = event["headers"].as_array().unwrap();
    let customs: Vec<&str> = hdrs
        .iter()
        .filter(|h| h[0] == "x-custom")
        .map(|h| h[1].as_str().unwrap())
        .collect();
    assert_eq!(customs, vec!["one", "two"], "repeated headers preserved");
    assert!(
        !hdrs.iter().any(|h| h[0] == "authorization"),
        "credential is not forwarded"
    );
    let body = base64::engine::general_purpose::STANDARD
        .decode(event["body_base64"].as_str().unwrap())
        .unwrap();
    assert_eq!(body, b"payload");
    let http_inv = get(
        r,
        &format!(
            "/v1/invocations/{}",
            http.header("x-tachyon-invocation-id").unwrap()
        ),
        TOKEN_A,
    )
    .await;
    assert_eq!(http_inv.json()["http_status"], 200);

    // cross-tenant -> 404
    let foreign = get(r, &format!("/v1/functions/{function_id}"), TOKEN_B).await;
    assert_eq!(foreign.status, StatusCode::NOT_FOUND);
    assert_eq!(foreign.json()["error"]["code"], "not_found");
    let foreign_inv = get(r, &format!("/v1/invocations/{invocation_id}"), TOKEN_B).await;
    assert_eq!(foreign_inv.status, StatusCode::NOT_FOUND);
    let foreign_logs = get(r, &format!("/v1/invocations/{invocation_id}/logs"), TOKEN_B).await;
    assert_eq!(foreign_logs.status, StatusCode::NOT_FOUND);
    let foreign_invoke = post_json(
        r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_B,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(foreign_invoke.status, StatusCode::NOT_FOUND);

    // alias CAS -> 409
    let cas = call(
        r,
        req(
            Method::PUT,
            &format!("/v1/functions/{function_id}/aliases/prod"),
            Some(TOKEN_A),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body(json_body(
            serde_json::json!({"revision_id": revision_id, "expected_generation": 99}),
        ))
        .unwrap(),
    )
    .await;
    assert_eq!(
        cas.status,
        StatusCode::CONFLICT,
        "{}",
        String::from_utf8_lossy(&cas.body)
    );
    assert_eq!(cas.json()["error"]["code"], "conflict");
    let ok = call(
        r,
        req(
            Method::PUT,
            &format!("/v1/functions/{function_id}/aliases/prod"),
            Some(TOKEN_A),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body(json_body(
            serde_json::json!({"revision_id": revision_id, "expected_generation": 1}),
        ))
        .unwrap(),
    )
    .await;
    assert_eq!(ok.status, StatusCode::OK);
    assert_eq!(ok.json()["generation"], 2);
    let aliases = get(r, &format!("/v1/functions/{function_id}/aliases"), TOKEN_A).await;
    assert_eq!(aliases.json()["items"].as_array().unwrap().len(), 1);

    // cancel a hanging invoke through the colon endpoint
    let hang_router = r.clone();
    let fid = function_id.clone();
    let hanging = tokio::spawn(async move {
        call(
            &hang_router,
            req(
                Method::POST,
                &format!("/v1/functions/{fid}/invoke"),
                Some(TOKEN_A),
            )
            .body(json_body(serde_json::json!({})))
            .unwrap(),
        )
        .await
    });
    let running_id = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let h = get(
                r,
                &format!("/v1/functions/{function_id}/invocations"),
                TOKEN_A,
            )
            .await;
            let running = h.json()["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|i| i["status"] == "running")
                .map(|i| i["id"].as_str().unwrap().to_string());
            if let Some(id) = running {
                break id;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "invocation never started"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let cancelled = call(
        r,
        req(
            Method::POST,
            &format!("/v1/invocations/{running_id}:cancel"),
            Some(TOKEN_A),
        )
        .body(Body::empty())
        .unwrap(),
    )
    .await;
    assert_eq!(
        cancelled.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&cancelled.body)
    );
    assert_eq!(cancelled.json()["status"], "cancelled");
    let hung = hanging.await.unwrap();
    assert_eq!(hung.status.as_u16(), 499);
    assert_eq!(hung.json()["error"]["code"], "cancelled");
    assert_eq!(
        hung.header("x-tachyon-invocation-id"),
        Some(running_id.as_str())
    );
    // second cancel is idempotent (slash form)
    let again = call(
        r,
        req(
            Method::POST,
            &format!("/v1/invocations/{running_id}/cancel"),
            Some(TOKEN_A),
        )
        .body(Body::empty())
        .unwrap(),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK);
    assert!(
        api.fake.running().is_empty(),
        "every environment is terminated"
    );

    // unknown colon action -> 404
    let weird = call(
        r,
        req(
            Method::POST,
            &format!("/v1/functions/{function_id}:explode"),
            Some(TOKEN_A),
        )
        .body(Body::empty())
        .unwrap(),
    )
    .await;
    assert_eq!(weird.status, StatusCode::NOT_FOUND);

    // delete stops invocations
    let deleted = call(
        r,
        req(
            Method::DELETE,
            &format!("/v1/functions/{function_id}"),
            Some(TOKEN_A),
        )
        .body(Body::empty())
        .unwrap(),
    )
    .await;
    assert_eq!(deleted.status, StatusCode::OK);
    assert!(deleted.json()["deleted_at"].is_string());
    let after = post_json(
        r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(after.status, StatusCode::CONFLICT);
    assert_eq!(after.json()["error"]["code"], "function_deleted");

    // openapi
    let spec = call(
        r,
        Request::get("/openapi.json").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(spec.status, StatusCode::OK);
    let doc = spec.json();
    assert!(doc["openapi"].as_str().unwrap().starts_with("3."));
    let paths = doc["paths"].as_object().unwrap();
    for p in [
        "/v1/functions",
        "/v1/functions/{function_id}",
        "/v1/functions/{function_id}/revisions",
        "/v1/functions/{function_id}/invoke",
        "/v1/functions/{function_id}/http/{path}",
        "/v1/invocations/{invocation_id}/cancel",
        "/v1/invocations/{invocation_id}/logs",
        "/v1/functions/{function_id}/usage",
    ] {
        assert!(paths.contains_key(p), "missing path {p}");
    }
    assert!(doc["components"]["schemas"]["ApiErrorBody"].is_object());
}

#[tokio::test]
async fn invoke_failures_map_to_status_codes() {
    let api = api(vec![
        FakeGuestScript::HandlerError {
            error_type: "Handler.Error".into(),
            message: "nope".into(),
        },
        FakeGuestScript::InitError {
            message: "boom".into(),
        },
    ]);
    let r = &api.router;
    let (function_id, _) = deploy(r, "fails").await;

    let user = post_json(
        r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(user.status, StatusCode::BAD_GATEWAY);
    assert_eq!(user.json()["error"]["code"], "user_error");
    assert_eq!(user.json()["error"]["error_type"], "Handler.Error");
    assert!(user.header("x-tachyon-invocation-id").is_some());

    let init = post_json(
        r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(init.status, StatusCode::BAD_GATEWAY);
    assert_eq!(init.json()["error"]["code"], "init_error");

    // payload too large -> 413 before anything runs
    let big = vec![b'a'; 2 * 1024 * 1024];
    let too_big = call(
        r,
        req(
            Method::POST,
            &format!("/v1/functions/{function_id}/invoke"),
            Some(TOKEN_A),
        )
        .body(json_body(
            serde_json::json!({"blob": String::from_utf8(big).unwrap()}),
        ))
        .unwrap(),
    )
    .await;
    assert_eq!(too_big.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(too_big.json()["error"]["code"], "payload_too_large");

    // invalid JSON -> 400
    let bad = call(
        r,
        req(
            Method::POST,
            &format!("/v1/functions/{function_id}/invoke"),
            Some(TOKEN_A),
        )
        .body(Body::from("{not json"))
        .unwrap(),
    )
    .await;
    assert_eq!(bad.status, StatusCode::BAD_REQUEST);

    // unknown function -> 404, invalid id shape -> 404
    let missing = post_json(
        r,
        "/v1/functions/fn_01hzzzzzzzzzzzzzzzzzzzzzzz/invoke",
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    let bad_id = get(r, "/v1/functions/not-an-id", TOKEN_A).await;
    assert_eq!(bad_id.status, StatusCode::NOT_FOUND);
    assert_eq!(api.fake.created().len(), 2);
}

async fn send(router: &Router, method: Method, path: &str, token: &str, body: Body) -> Reply {
    call(
        router,
        req(method, path, Some(token))
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap(),
    )
    .await
}

/// Pins the implemented operator role (docs/threat-model.md §7): read-only
/// metadata of the operator's own tenant plus `/v1/provider`. Other tenants'
/// resources do not exist for it, and invocation data, logs, usage, invoke
/// and every mutation are forbidden.
#[tokio::test]
async fn operator_role_is_own_tenant_read_only() {
    let api = api(vec![FakeGuestScript::RespondOk(
        serde_json::json!({"ok": 1}),
    )]);
    let r = &api.router;
    let (function_id, revision_id) = deploy(r, "watched").await;
    let inv = post_json(
        r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(inv.status, StatusCode::OK);
    let invocation_id = inv.header("x-tachyon-invocation-id").unwrap().to_string();

    let metadata = [
        format!("/v1/functions/{function_id}"),
        format!("/v1/functions/{function_id}/revisions"),
        format!("/v1/functions/{function_id}/revisions/{revision_id}"),
        format!("/v1/functions/{function_id}/aliases"),
        format!("/v1/functions/{function_id}/aliases/prod"),
    ];
    let invocation_data = [
        format!("/v1/invocations/{invocation_id}"),
        format!("/v1/functions/{function_id}/invocations"),
        format!("/v1/functions/{function_id}/usage"),
        format!("/v1/invocations/{invocation_id}/logs"),
    ];
    // Well-formed bodies, so that the role check (not body parsing) decides.
    let revision_body = serde_json::json!({
        "artifact": {"kind": "binary", "digest": tachyon_serverless_domain::Sha256Digest::of_bytes(b"x").to_string()},
        "architecture": "aarch64",
        "execution": {"timeout_seconds": 5, "initialization_timeout_seconds": 5, "max_concurrency": 1},
        "publish_to_prod": false
    });
    let mutations = [
        (
            Method::POST,
            "/v1/functions".to_string(),
            serde_json::json!({"name": "by-operator"}),
        ),
        (
            Method::POST,
            "/v1/artifacts".to_string(),
            serde_json::json!("bytes"),
        ),
        (
            Method::DELETE,
            format!("/v1/functions/{function_id}"),
            serde_json::json!({}),
        ),
        (
            Method::POST,
            format!("/v1/functions/{function_id}/revisions"),
            revision_body,
        ),
        (
            Method::PUT,
            format!("/v1/functions/{function_id}/aliases/prod"),
            serde_json::json!({"revision_id": revision_id}),
        ),
        (
            Method::POST,
            format!("/v1/functions/{function_id}/invoke"),
            serde_json::json!({}),
        ),
        (
            Method::POST,
            format!("/v1/functions/{function_id}:invoke"),
            serde_json::json!({}),
        ),
        (
            Method::GET,
            format!("/v1/functions/{function_id}/http/anything"),
            serde_json::json!({}),
        ),
        (
            Method::POST,
            format!("/v1/invocations/{invocation_id}:cancel"),
            serde_json::json!({}),
        ),
    ];

    // Operator of another tenant: A's resources are not found.
    for path in &metadata {
        let res = get(r, path, TOKEN_OP).await;
        assert_eq!(res.status, StatusCode::NOT_FOUND, "GET {path}");
        assert_eq!(res.json()["error"]["code"], "not_found");
    }
    let list = get(r, "/v1/functions", TOKEN_OP).await;
    assert_eq!(list.status, StatusCode::OK);
    assert!(
        list.json()["items"].as_array().unwrap().is_empty(),
        "no cross-tenant listing"
    );
    for path in &invocation_data {
        let res = get(r, path, TOKEN_OP).await;
        assert_eq!(res.status, StatusCode::FORBIDDEN, "GET {path}");
        assert_eq!(res.json()["error"]["code"], "forbidden");
    }
    let provider = get(r, "/v1/provider", TOKEN_OP).await;
    assert_eq!(provider.status, StatusCode::OK);

    // Operator of tenant A: metadata is readable, nothing else is.
    for path in &metadata {
        let res = get(r, path, TOKEN_A_OP).await;
        assert_eq!(res.status, StatusCode::OK, "GET {path}");
    }
    let list = get(r, "/v1/functions", TOKEN_A_OP).await;
    assert_eq!(list.json()["items"].as_array().unwrap().len(), 1);
    for path in &invocation_data {
        let res = get(r, path, TOKEN_A_OP).await;
        assert_eq!(res.status, StatusCode::FORBIDDEN, "GET {path}");
    }

    for token in [TOKEN_OP, TOKEN_A_OP] {
        for (method, path, body) in &mutations {
            let res = send(r, method.clone(), path, token, json_body(body.clone())).await;
            assert_eq!(
                res.status,
                StatusCode::FORBIDDEN,
                "{method} {path} as {token}"
            );
        }
    }
    // Nothing was invoked, deleted or created by the operators.
    assert_eq!(api.fake.created().len(), 1);
    let still = get(r, &format!("/v1/functions/{function_id}"), TOKEN_A).await;
    assert!(still.json()["deleted_at"].is_null());
}

async fn wait_revision_terminal(
    router: &Router,
    token: &str,
    function_id: &str,
    revision_id: &str,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let r = get(
            router,
            &format!("/v1/functions/{function_id}/revisions/{revision_id}"),
            token,
        )
        .await;
        assert_eq!(r.status, StatusCode::OK);
        let body = r.json();
        match body["status"].as_str().unwrap() {
            "ready" | "failed" => return body,
            _ if tokio::time::Instant::now() > deadline => panic!("revision never finished"),
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

async fn create_binary_revision(
    router: &Router,
    token: &str,
    function_id: &str,
    digest: &str,
) -> serde_json::Value {
    let rev = post_json(
        router,
        &format!("/v1/functions/{function_id}/revisions"),
        token,
        serde_json::json!({
            "artifact": {"kind": "binary", "digest": digest},
            "architecture": "aarch64",
            "execution": {"timeout_seconds": 5, "initialization_timeout_seconds": 5, "max_concurrency": 1},
            "publish_to_prod": false
        }),
    )
    .await;
    assert_eq!(
        rev.status,
        StatusCode::ACCEPTED,
        "{}",
        String::from_utf8_lossy(&rev.body)
    );
    let revision_id = rev.json()["id"].as_str().unwrap().to_string();
    wait_revision_terminal(router, token, function_id, &revision_id).await
}

/// docs/threat-model.md §14-1: a revision may only reference artifacts its
/// tenant uploaded, and a foreign digest fails exactly like a missing one.
#[tokio::test]
async fn foreign_artifact_digest_is_indistinguishable_from_a_missing_one() {
    let api = api(vec![]);
    let r = &api.router;
    // tenant A uploads (inside `deploy`) and runs its artifact
    deploy(r, "owner").await;
    let bytes = b"#!/bin/sh\necho owner\n";
    let owned = tachyon_serverless_domain::Sha256Digest::of_bytes(bytes).to_string();
    let missing = tachyon_serverless_domain::Sha256Digest::of_bytes(b"never uploaded").to_string();

    let created = post_json(
        r,
        "/v1/functions",
        TOKEN_B,
        serde_json::json!({"name": "borrower"}),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let fb = created.json()["id"].as_str().unwrap().to_string();

    let foreign = create_binary_revision(r, TOKEN_B, &fb, &owned).await;
    let unknown = create_binary_revision(r, TOKEN_B, &fb, &missing).await;
    assert_eq!(foreign["status"], "failed");
    assert_eq!(unknown["status"], "failed");
    let foreign_reason = foreign["failure_reason"]
        .as_str()
        .unwrap()
        .replace(&owned, "<digest>");
    let unknown_reason = unknown["failure_reason"]
        .as_str()
        .unwrap()
        .replace(&missing, "<digest>");
    assert_eq!(foreign_reason, unknown_reason);
    assert_eq!(
        foreign["artifact"]["size_bytes"],
        unknown["artifact"]["size_bytes"]
    );

    // Uploading the same bytes makes B an owner of the digest too.
    let upload = call(
        r,
        req(Method::POST, "/v1/artifacts", Some(TOKEN_B))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(&bytes[..]))
            .unwrap(),
    )
    .await;
    assert_eq!(upload.status, StatusCode::OK);
    assert_eq!(upload.json()["digest"], owned);
    let own = create_binary_revision(r, TOKEN_B, &fb, &owned).await;
    assert_eq!(own["status"], "ready", "{own}");
}

/// The HTTP adapter forwards the request-target path as received
/// (percent-encoded), so encoded separators and spaces are not decoded by the
/// gateway and cannot re-route the request inside the function.
#[tokio::test]
async fn http_adapter_forwards_the_raw_request_path() {
    let cases = [
        ("a%20b", "/a%20b", ""),
        ("a%3Fb?x=1", "/a%3Fb", "x=1"),
        ("a%2Fb", "/a%2Fb", ""),
        ("a%2520b", "/a%2520b", ""),
    ];
    let api = api(cases.iter().map(|_| FakeGuestScript::EchoHttp).collect());
    let r = &api.router;
    let (function_id, _) = deploy(r, "raw-path").await;
    for (suffix, path, query) in cases {
        let res = call(
            r,
            req(
                Method::GET,
                &format!("/v1/functions/{function_id}/http/{suffix}"),
                Some(TOKEN_A),
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await;
        assert_eq!(
            res.status,
            StatusCode::OK,
            "{suffix}: {}",
            String::from_utf8_lossy(&res.body)
        );
        let event = res.json();
        assert_eq!(event["path"], path, "{suffix}");
        assert_eq!(event["query"], query, "{suffix}");
    }
}

/// A gateway that restarts on a `data_dir` left behind by a crash converges:
/// the ledger settles a dispatched invocation as `outcome_unknown` and its
/// environment as `lost`, and the environment the provider still runs is
/// reclaimed with `Reconcile` before the listener accepts. `/readyz` shows
/// the pass (docs/architecture.md §4).
#[tokio::test]
async fn bootstrap_converges_on_a_state_file_left_behind_by_a_crash() {
    use tachyon_serverless_domain::{
        Architecture, AttemptId, Clock, Deadlines, EgressProfile, EnvironmentId, EnvironmentState,
        ErrorClass, EventKind, ExecutionEnvironment, FunctionId, Invocation, InvocationId,
        InvocationMode, InvocationStatus, ProviderKind, ResourceProfile, ReuseKey, RevisionId,
        Sha256Digest, SystemClock, TenantId,
    };
    use tachyon_serverless_provider_port::{
        ArtifactLocation, EnvironmentSpec, ExecutionProvider, TerminateReason,
    };

    let dir = tempfile::tempdir().unwrap();
    let now = SystemClock.now();
    let tenant = TenantId::parse(TENANT_A).unwrap();
    let revision = RevisionId::generate();

    // A ledger written by a process that died while an invocation was running.
    let mut invocation = Invocation::accept(
        InvocationId::generate(),
        tenant.clone(),
        FunctionId::generate(),
        None,
        revision.clone(),
        InvocationMode::Sync,
        EventKind::Json,
        Deadlines {
            queue_deadline: now,
            init_deadline: None,
            execution_deadline: None,
            client_deadline: now + Duration::from_secs(60),
        },
        None,
        Sha256Digest::of_bytes(b"{}"),
        2,
        "trace".into(),
        now,
    )
    .unwrap();
    invocation
        .mark_running(
            AttemptId::generate(),
            now + Duration::from_secs(30),
            now + Duration::from_secs(5),
            now,
        )
        .unwrap();
    let stale_env = ExecutionEnvironment::request(
        EnvironmentId::generate(),
        tenant.clone(),
        revision.clone(),
        ProviderKind::Fake,
        ReuseKey {
            tenant_id: tenant.clone(),
            revision_id: revision.clone(),
            execution_role_version: 1,
            configuration_version: 1,
            resource_profile_digest: "d".into(),
            runtime_profile: "default".into(),
            network_policy_version: 1,
            secret_binding_generation: 1,
        },
        now,
    );
    let mut invocations = serde_json::Map::new();
    invocations.insert(
        invocation.id.to_string(),
        serde_json::to_value(&invocation).unwrap(),
    );
    let mut environments = serde_json::Map::new();
    environments.insert(
        stale_env.id.to_string(),
        serde_json::to_value(&stale_env).unwrap(),
    );
    std::fs::write(
        dir.path().join("state.json"),
        serde_json::to_vec(&serde_json::json!({
            "invocations": invocations,
            "environments": environments,
        }))
        .unwrap(),
    )
    .unwrap();

    // ... and an environment that outlived that process.
    let fake = Arc::new(FakeExecutionProvider::new());
    let orphan = EnvironmentId::generate();
    fake.create_environment(EnvironmentSpec {
        environment_id: orphan.clone(),
        tenant_id: tenant.clone(),
        revision_id: revision.clone(),
        artifact: ArtifactLocation {
            path: "/nonexistent".into(),
            digest: Sha256Digest::of_bytes(b"orphan"),
            size_bytes: 1,
        },
        architecture: Architecture::Aarch64,
        resources: ResourceProfile::default(),
        egress: EgressProfile::None,
        egress_allow: Vec::new(),
        connect_timeout: Duration::from_secs(1),
    })
    .await
    .unwrap();

    let app = Application::bootstrap_with(
        config(dir.path()),
        fake.clone(),
        BootstrapOptions {
            persist_state: true,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    let report = app
        .reconcile_on_startup()
        .await
        .expect("reconcile is on by default");
    assert_eq!(
        (
            report.found,
            report.adopted,
            report.terminated,
            report.failed
        ),
        (1, 0, 1, 0)
    );
    assert_eq!(report.error, None);
    assert_eq!(
        fake.terminated(),
        vec![(orphan, TerminateReason::Reconcile)]
    );
    assert!(
        fake.running().is_empty(),
        "nothing of the previous process is left running"
    );

    // The ledger settled: dispatched work is unknown, its environment lost.
    let settled = app.repos.invocations.get(&invocation.id).unwrap().unwrap();
    match settled.status {
        InvocationStatus::OutcomeUnknown { error } => {
            assert_eq!(error.class, ErrorClass::OutcomeUnknown);
            assert_eq!(error.error_type, "Host.Restarted");
        }
        other => panic!("a dispatched invocation may have run: {other:?}"),
    }
    let env = app.repos.environments.get(&stale_env.id).unwrap().unwrap();
    assert!(
        matches!(env.state, EnvironmentState::Lost { .. }),
        "{:?}",
        env.state
    );

    // and the pass is visible on /readyz.
    let r = router(app);
    let ready = call(&r, Request::get("/readyz").body(Body::empty()).unwrap()).await;
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(ready.json()["reconcile"]["found"], 1);
    assert_eq!(ready.json()["reconcile"]["terminated"], 1);
    assert_eq!(ready.json()["reconcile"]["error"], serde_json::Value::Null);
}

// ---------------------------------------------------------------------------
// PLT-4636: control plane / data plane split over the HTTP surface
// ---------------------------------------------------------------------------

const INTERNAL: &str = "internal-credential-0123456789";

/// A `ConfigSource` that calls `GET /v1/internal/config` on a management
/// router in process: the same handler, credential check and JSON wire as the
/// HTTP client, without a socket.
struct RouterSource {
    router: Router,
    token: &'static str,
    down: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl tachyon_serverless_application::control::ConfigSource for RouterSource {
    fn describe(&self) -> String {
        "router".into()
    }
    async fn fetch(
        &self,
        since: u64,
    ) -> Result<
        tachyon_serverless_application::control::ConfigDelivery,
        tachyon_serverless_application::control::SourceError,
    > {
        use tachyon_serverless_application::control::SourceError;
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(SourceError::Unavailable("connection refused".into()));
        }
        let reply = get(
            &self.router,
            &format!("/v1/internal/config?since={since}"),
            self.token,
        )
        .await;
        if reply.status != StatusCode::OK {
            return Err(SourceError::Rejected(format!("{}", reply.status)));
        }
        serde_json::from_slice(&reply.body).map_err(|e| SourceError::Rejected(e.to_string()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_plane_gateway_serves_invokes_from_delivered_configuration() {
    use tachyon_serverless_application::GatewayRole;
    use tachyon_serverless_application::config::ConfigToken;

    let dir = tempfile::tempdir().unwrap();
    let mut mgmt_cfg = config(dir.path());
    mgmt_cfg.control_plane.internal_token = Some(ConfigToken::new(INTERNAL));
    mgmt_cfg.dispatcher.instance = Some("management".into());
    let mgmt_fake = Arc::new(FakeExecutionProvider::new());
    let mgmt = Application::bootstrap_with(
        mgmt_cfg,
        mgmt_fake,
        BootstrapOptions {
            persist_state: true,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    let mgmt_router = router(mgmt);
    let (function_id, revision_id) = deploy(&mgmt_router, "split").await;

    // The internal endpoint takes the internal credential, never a tenant token.
    for token in [None, Some(TOKEN_A), Some("internal-credential-wrong-00")] {
        let r = call(
            &mgmt_router,
            req(Method::GET, "/v1/internal/config", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{token:?}");
    }
    let delivery = get(&mgmt_router, "/v1/internal/config?since=0", INTERNAL).await;
    assert_eq!(delivery.status, StatusCode::OK);
    let text = String::from_utf8_lossy(&delivery.body).to_string();
    assert!(delivery.json()["generation"].as_u64().unwrap() > 0);
    assert!(!text.contains(TOKEN_A) && !text.contains("demo-secret-value-a"));

    // The data plane: no tokens of its own, the secret binding value is local.
    let mut dp_cfg = config(dir.path());
    dp_cfg.identity.tokens.clear();
    dp_cfg.dispatcher.instance = Some("data-plane".into());
    dp_cfg.control_plane.role = GatewayRole::DataPlane;
    dp_cfg.control_plane.url = Some("http://management.invalid".into());
    dp_cfg.control_plane.internal_token = Some(ConfigToken::new(INTERNAL));
    let source = Arc::new(RouterSource {
        router: mgmt_router.clone(),
        token: INTERNAL,
        down: std::sync::atomic::AtomicBool::new(false),
    });
    let dp_fake = Arc::new(FakeExecutionProvider::new());
    dp_fake.set_default_script(Some(FakeGuestScript::Echo));
    let dp = Application::bootstrap_with(
        dp_cfg,
        dp_fake,
        BootstrapOptions {
            persist_state: true,
            config_source: Some(source.clone()),
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    let dp_router = router(dp.clone());

    // Before the first delivery: not ready, and invoke says why.
    let ready = call(
        &dp_router,
        Request::get("/readyz").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(ready.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        ready.json()["control_plane"]["refusal"],
        "Host.ConfigNotDelivered"
    );
    let early = post_json(
        &dp_router,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({"x": 1}),
    )
    .await;
    assert_eq!(early.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(early.json()["error"]["code"], "config_unavailable");
    assert_eq!(
        early.json()["error"]["error_type"],
        "Host.ConfigNotDelivered"
    );

    dp.refresh_config().await.unwrap();
    let ready = call(
        &dp_router,
        Request::get("/readyz").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(ready.status, StatusCode::OK, "{}", ready.json());
    assert_eq!(ready.json()["control_plane"]["role"], "data_plane");
    assert_eq!(
        ready.json()["control_plane"]["existing_executions"],
        "continue"
    );

    let invoked = post_json(
        &dp_router,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({"x": 1}),
    )
    .await;
    assert_eq!(
        invoked.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&invoked.body)
    );
    assert_eq!(invoked.json()["x"], 1);
    let invocation_id = invoked
        .header("x-tachyon-invocation-id")
        .unwrap()
        .to_string();
    let detail = get(
        &dp_router,
        &format!("/v1/invocations/{invocation_id}"),
        TOKEN_A,
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK);
    assert_eq!(detail.json()["revision_id"], revision_id);
    // Tenant B cannot see it through the data plane either.
    let foreign = get(
        &dp_router,
        &format!("/v1/invocations/{invocation_id}"),
        TOKEN_B,
    )
    .await;
    assert_eq!(foreign.status, StatusCode::NOT_FOUND);

    // The management API is not served by a data plane.
    for (method, path) in [
        (Method::GET, "/v1/functions".to_string()),
        (Method::POST, "/v1/artifacts".to_string()),
        (Method::GET, format!("/v1/functions/{function_id}")),
    ] {
        let r = send(&dp_router, method.clone(), &path, TOKEN_A, Body::empty()).await;
        assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{method} {path}");
        assert_eq!(r.json()["error"]["code"], "control_plane_unavailable");
        assert_eq!(
            r.json()["error"]["error_type"],
            "Host.ControlPlaneUnavailable"
        );
    }
    let internal = get(&dp_router, "/v1/internal/config", INTERNAL).await;
    assert_eq!(internal.status, StatusCode::NOT_FOUND);

    // The control plane goes away: the data plane keeps serving from its
    // cache and says so.
    source.down.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(dp.refresh_config().await.is_err());
    let again = post_json(
        &dp_router,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({"x": 2}),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK);
    let ready = call(
        &dp_router,
        Request::get("/readyz").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(
        ready.json()["control_plane"]["control_plane_reachable"],
        false
    );
    assert_eq!(ready.json()["control_plane"]["new_cold_starts"], "allowed");
}

#[tokio::test]
async fn the_internal_config_endpoint_is_absent_without_an_internal_credential() {
    let api = api(vec![]);
    let r = get(&api.router, "/v1/internal/config", "anything-at-all-000000").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let ready = call(
        &api.router,
        Request::get("/readyz").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(ready.json()["control_plane"]["role"], "combined");
    assert_eq!(ready.json()["control_plane"]["new_invocations"], "accepted");
}

/// PLT-4634: `GET /v1/capacity` needs a token, reports the host separately
/// from its environments, and shows a tenant only its own revisions. A burst
/// of concurrent invokes through the HTTP API is served and fully released.
#[tokio::test]
async fn capacity_is_reported_per_tenant_and_a_burst_is_released() {
    let api = api(Vec::new());
    let r = &api.router;
    let anonymous = call(
        r,
        req(Method::GET, "/v1/capacity", None)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED);

    let (function_id, revision_id) = deploy(r, "capacity").await;
    let calls: Vec<_> = (0..6)
        .map(|i| {
            let router = r.clone();
            let path = format!("/v1/functions/{function_id}/invoke");
            tokio::spawn(async move {
                post_json(&router, &path, TOKEN_A, serde_json::json!({ "i": i })).await
            })
        })
        .collect();
    for c in calls {
        let reply = c.await.unwrap();
        assert_eq!(
            reply.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
    }
    assert_eq!(api.fake.created().len(), 6);

    let a = get(r, "/v1/capacity", TOKEN_A).await;
    assert_eq!(a.status, StatusCode::OK);
    let a = a.json();
    assert_eq!(a["node"]["hosts"], 1);
    assert_eq!(a["node"]["host_scale_out"], "not_supported");
    assert_eq!(a["node"]["max_concurrency"], 8);
    assert_eq!(a["node"]["per_environment_overhead"]["memory_mib"], 24);
    assert_eq!(a["in_flight"], 0);
    assert_eq!(a["reserved"]["memory_mib"], 0);
    assert_eq!(a["queue"]["length"], 0);
    assert_eq!(a["tenant"]["tenant_id"], TENANT_A);
    let revisions = a["revisions"].as_array().unwrap();
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0]["revision_id"], revision_id.as_str());

    let b = get(r, "/v1/capacity", TOKEN_B).await.json();
    assert_eq!(b["tenant"]["tenant_id"], TENANT_B);
    assert!(b["revisions"].as_array().unwrap().is_empty());
    assert!(!b.to_string().contains(&revision_id));
}

// ---------------------------------------------------------------------------
// invokeAsync (PLT-4639)
// ---------------------------------------------------------------------------

struct AsyncApi {
    router: Router,
    app: Arc<Application>,
    _dir: tempfile::TempDir,
}

/// A gateway with the durable ledger and the embedded SQLite queue.
fn async_api(extra: &str) -> AsyncApi {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "{}\n[queue]\nbackend = \"sqlite\"\n\n[invoke_async]\ninline_input_max_bytes = 1024\n{extra}",
        config_toml(dir.path())
    );
    let app = Application::bootstrap_with(
        GatewayConfig::from_toml(&toml).unwrap(),
        Arc::new(FakeExecutionProvider::new()),
        BootstrapOptions::default(),
    )
    .unwrap();
    AsyncApi {
        router: router(app.clone()),
        app,
        _dir: dir,
    }
}

async fn post_with_key(
    router: &Router,
    path: &str,
    token: &str,
    key: &str,
    v: serde_json::Value,
) -> Reply {
    call(
        router,
        req(Method::POST, path, Some(token))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", key)
            .body(json_body(v))
            .unwrap(),
    )
    .await
}

/// 202 only after commit, with the status URL; the invocation reads
/// `accepted` until the outbox publishes it and `queued` afterwards; the same
/// key and input answers the same invocation, another input 409.
#[tokio::test]
async fn invoke_async_answers_202_with_a_status_url_and_converges_idempotently() {
    let api = async_api("");
    let r = &api.router;
    let (function_id, revision_id) = deploy(r, "async-http").await;
    let path = format!("/v1/functions/{function_id}:invokeAsync");
    let a = post_with_key(r, &path, TOKEN_A, "k-1", serde_json::json!({"n": 1})).await;
    assert_eq!(
        a.status,
        StatusCode::ACCEPTED,
        "{}",
        String::from_utf8_lossy(&a.body)
    );
    let body = a.json();
    let id = body["invocation_id"].as_str().unwrap().to_string();
    let status_url = format!("/v1/invocations/{id}");
    assert_eq!(body["status_url"], status_url.as_str());
    assert_eq!(a.header("location"), Some(status_url.as_str()));
    assert_eq!(a.header("x-tachyon-invocation-id"), Some(id.as_str()));
    assert_eq!(body["status"], "accepted");
    assert_eq!(body["revision_id"], revision_id.as_str());
    assert_eq!(body["input_storage"], "inline");
    assert_eq!(body["replayed"], false);

    let s = get(r, &status_url, TOKEN_A).await;
    assert_eq!(s.status, StatusCode::OK);
    assert_eq!(s.json()["mode"], "async");
    assert_eq!(s.json()["status"], "accepted");

    // Slash form, same key, same input: the same invocation.
    let again = post_with_key(
        r,
        &format!("/v1/functions/{function_id}/invokeAsync"),
        TOKEN_A,
        "k-1",
        serde_json::json!({"n": 1}),
    )
    .await;
    assert_eq!(again.status, StatusCode::ACCEPTED);
    assert_eq!(again.json()["invocation_id"], id.as_str());
    assert_eq!(again.json()["replayed"], true);
    // Same key, other input.
    let conflict = post_with_key(r, &path, TOKEN_A, "k-1", serde_json::json!({"n": 2})).await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.json()["error"]["invocation_id"], id.as_str());
    // Same key on the synchronous path.
    let sync = post_with_key(
        r,
        &format!("/v1/functions/{function_id}:invoke"),
        TOKEN_A,
        "k-1",
        serde_json::json!({"n": 1}),
    )
    .await;
    assert_eq!(sync.status, StatusCode::CONFLICT);

    let report = api.app.publish_outbox().await.unwrap();
    assert_eq!(report.marked, 1);
    let s = get(r, &status_url, TOKEN_A).await;
    assert_eq!(s.json()["status"], "queued");
    assert_eq!(s.json()["revision_id"], revision_id.as_str());
}

/// Another tenant can neither accept on this tenant's function nor read its
/// asynchronous invocation: both are 404.
#[tokio::test]
async fn invoke_async_status_and_acceptance_never_cross_a_tenant() {
    let api = async_api("");
    let r = &api.router;
    let (function_id, _) = deploy(r, "async-tenant").await;
    let path = format!("/v1/functions/{function_id}:invokeAsync");
    let foreign = post_json(r, &path, TOKEN_B, serde_json::json!({})).await;
    assert_eq!(foreign.status, StatusCode::NOT_FOUND);
    let a = post_json(r, &path, TOKEN_A, serde_json::json!({})).await;
    assert_eq!(a.status, StatusCode::ACCEPTED);
    let id = a.json()["invocation_id"].as_str().unwrap().to_string();
    let b = get(r, &format!("/v1/invocations/{id}"), TOKEN_B).await;
    assert_eq!(b.status, StatusCode::NOT_FOUND);
    assert!(!String::from_utf8_lossy(&b.body).contains(&function_id));
}

/// Over the inline limit without an object store: 413 with its reason. The
/// outbox bound: 429 `backlog`.
#[tokio::test]
async fn invoke_async_refusals_carry_status_and_reason() {
    let api = async_api("max_pending_events = 1\n");
    let r = &api.router;
    let (function_id, _) = deploy(r, "async-refuse").await;
    let path = format!("/v1/functions/{function_id}:invokeAsync");
    let big = post_json(
        r,
        &path,
        TOKEN_A,
        serde_json::json!({"blob": "x".repeat(4096)}),
    )
    .await;
    assert_eq!(big.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(big.json()["error"]["reason"], "input_too_large");
    assert_eq!(
        post_json(r, &path, TOKEN_A, serde_json::json!({}))
            .await
            .status,
        StatusCode::ACCEPTED
    );
    let full = post_json(r, &path, TOKEN_A, serde_json::json!({})).await;
    assert_eq!(full.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(full.json()["error"]["reason"], "backlog");
    assert_eq!(full.json()["error"]["code"], "capacity_exceeded");
}

#[tokio::test]
async fn invoke_async_without_a_queue_is_503_not_configured() {
    let api = api(vec![]);
    let r = &api.router;
    let (function_id, _) = deploy(r, "async-off").await;
    let res = post_json(
        r,
        &format!("/v1/functions/{function_id}:invokeAsync"),
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(res.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(res.json()["error"]["code"], "async_unavailable");
    assert_eq!(res.json()["error"]["reason"], "not_configured");
}

// ---------------------------------------------------------------------------
// metrics (PLT-4637)
// ---------------------------------------------------------------------------

const METRICS_TOKEN: &str = "metrics-operator-credential";

fn api_with_metrics(token: Option<&str>) -> Api {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    let mut cfg = config(dir.path());
    cfg.metrics.bearer_token = token.map(tachyon_serverless_application::config::ConfigToken::new);
    let app = Application::bootstrap_with(
        cfg,
        fake.clone(),
        BootstrapOptions {
            persist_state: false,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    Api {
        router: router(app),
        _dir: dir,
        fake,
    }
}

fn metric(text: &str, family: &str, labels: &[&str]) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| {
            l.strip_prefix(family)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .find(|l| labels.iter().all(|want| l.contains(want)))
        .and_then(|l| l.rsplit_once(' '))
        .and_then(|(_, v)| v.parse().ok())
}

/// Security (PLT-4637): `GET /metrics` names every tenant's revisions, so it
/// does not exist without `[metrics] bearer_token`, and with one it refuses
/// anonymous callers, tenant tokens and operator-role tenant tokens alike;
/// refusals leak no tenant or revision id. `GET /v1/capacity` (tenant scoped)
/// never lists another tenant.
#[tokio::test]
async fn metrics_require_the_operator_credential_and_never_leak_tenants_without_it() {
    let disabled = api_with_metrics(None);
    for token in [None, Some(TOKEN_A), Some(METRICS_TOKEN)] {
        let reply = call(
            &disabled.router,
            req(Method::GET, "/metrics", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{token:?}");
    }

    let api = api_with_metrics(Some(METRICS_TOKEN));
    let r = &api.router;
    let (function_id, revision_id) = deploy(r, "metrics-secure").await;
    let reply = post_json(
        r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({"x": 1}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);

    for token in [
        None,
        Some(TOKEN_A),
        Some(TOKEN_B),
        Some(TOKEN_OP),
        Some(TOKEN_A_OP),
        Some("metrics-operator-credentia"),
        Some("metrics-operator-credential-x"),
    ] {
        let reply = call(
            r,
            req(Method::GET, "/metrics", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{token:?}");
        let body = String::from_utf8_lossy(&reply.body);
        assert!(!body.contains(TENANT_A), "{body}");
        assert!(!body.contains(&revision_id), "{body}");
        assert!(!body.contains("tsls_"), "{body}");
    }

    let ok = get(r, "/metrics", METRICS_TOKEN).await;
    assert_eq!(ok.status, StatusCode::OK);
    assert!(
        ok.header("content-type")
            .unwrap()
            .starts_with("text/plain; version=0.0.4")
    );
    let text = String::from_utf8_lossy(&ok.body);
    assert!(text.contains(&format!("tenant=\"{TENANT_A}\"")));

    // Tenant B's capacity view carries the node-wide reuse report only.
    let b = get(r, "/v1/capacity", TOKEN_B).await.json();
    assert!(!b.to_string().contains(TENANT_A));
    assert!(!b.to_string().contains(&revision_id));
    assert_eq!(b["reuse"]["mode"], "every_invocation_boots");
}

/// A scripted sequence through the HTTP API, then one scrape: a burst over
/// the revision's cap, a handler error, and the counters, histograms and the
/// "every invocation boots" profile of a provider without a warm stage.
#[tokio::test]
async fn metrics_scrape_reflects_a_scripted_invoke_sequence() {
    let api = api_with_metrics(Some(METRICS_TOKEN));
    let r = &api.router;
    let (function_id, revision_id) = deploy(r, "metrics-seq").await;
    let calls: Vec<_> = (0..6)
        .map(|i| {
            let router = r.clone();
            let path = format!("/v1/functions/{function_id}/invoke");
            tokio::spawn(async move {
                post_json(&router, &path, TOKEN_A, serde_json::json!({ "i": i })).await
            })
        })
        .collect();
    for c in calls {
        assert_eq!(c.await.unwrap().status, StatusCode::OK);
    }
    api.fake.push_script(FakeGuestScript::HandlerError {
        error_type: "Demo.Error".into(),
        message: "boom".into(),
    });
    let failed = post_json(
        r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_ne!(failed.status, StatusCode::OK);

    let text = String::from_utf8_lossy(&get(r, "/metrics", METRICS_TOKEN).await.body).to_string();
    assert_eq!(
        metric(&text, "tsls_admission_arrivals_total", &[]),
        Some(7.0)
    );
    assert_eq!(
        metric(&text, "tsls_admission_grants_total", &["kind=\"cold\""]),
        Some(7.0)
    );
    assert_eq!(
        metric(
            &text,
            "tsls_attempts_total",
            &["start_kind=\"cold\"", "status=\"succeeded\""]
        ),
        Some(6.0)
    );
    assert_eq!(
        metric(
            &text,
            "tsls_attempts_total",
            &["start_kind=\"cold\"", "status=\"failed\""]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric(
            &text,
            "tsls_attempt_phase_seconds_count",
            &["phase=\"total\"", "start_kind=\"cold\""]
        ),
        Some(7.0)
    );
    assert_eq!(
        metric(
            &text,
            "tsls_environment_reuse_mode",
            &["provider=\"fake\"", "mode=\"every_invocation_boots\""]
        ),
        Some(1.0)
    );
    // Every attempt was its environment's first (and only) boot.
    assert_eq!(
        metric(
            &text,
            "tsls_boot_identity_checks_total",
            &["result=\"first_boot\""]
        ),
        Some(7.0)
    );
    assert_eq!(
        metric(
            &text,
            "tsls_boot_identity_checks_total",
            &["result=\"same_boot\""]
        ),
        Some(0.0)
    );
    assert_eq!(metric(&text, "tsls_node_in_flight", &[]), Some(0.0));
    assert_eq!(metric(&text, "tsls_queue_length", &[]), Some(0.0));
    assert_eq!(
        metric(
            &text,
            "tsls_revision_max_environments",
            &[&format!("revision=\"{revision_id}\"")]
        ),
        Some(4.0),
        "the revision's cap (default max_concurrency 4)"
    );
    assert_eq!(
        metric(
            &text,
            "tsls_scale_events_total",
            &["kind=\"activation\"", "reason=\"backlog\""]
        )
        .map(|v| v >= 1.0),
        Some(true)
    );
    let cap = get(r, "/v1/capacity", TOKEN_A).await.json();
    assert_eq!(cap["reuse"]["mode"], "every_invocation_boots");
    assert_eq!(cap["reuse"]["first_boots"], 7);
    assert_eq!(cap["reuse"]["boot_id_changed"], 0);
}

/// PLT-4642: `GET /v1/usage` is a provisional, tenant-scoped report that says
/// it is not an invoice; `/readyz` shows the journal and collector; a journal
/// that cannot be written refuses invocations with 503 `usage_journal_full`.
#[tokio::test]
async fn usage_report_is_provisional_tenant_scoped_and_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeExecutionProvider::new());
    fake.set_default_script(Some(FakeGuestScript::RespondOk(
        serde_json::json!({"ok": true}),
    )));
    let app = Application::bootstrap_with(
        config(dir.path()),
        fake.clone(),
        BootstrapOptions {
            persist_state: false,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    let r = router(app.clone());
    let (function_id, _) = deploy(&r, "metered").await;
    for _ in 0..2 {
        let out = post_json(
            &r,
            &format!("/v1/functions/{function_id}/invoke"),
            TOKEN_A,
            serde_json::json!({"hello": "usage"}),
        )
        .await;
        assert_eq!(out.status, StatusCode::OK);
    }
    app.collect_usage().unwrap();

    let report = get(&r, "/v1/usage?group_by=function", TOKEN_A).await;
    assert_eq!(
        report.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&report.body)
    );
    let j = report.json();
    assert_eq!(j["provisional"], true);
    assert_eq!(j["not_an_invoice"], true);
    assert_eq!(j["billing_enabled"], false);
    assert!(j["notice"].as_str().unwrap().contains("not an invoice"));
    assert_eq!(j["tenant_id"], TENANT_A);
    assert_eq!(j["totals"]["usage"]["invocations"], 2);
    assert_eq!(j["lines"][0]["function_id"], function_id.as_str());
    assert!(j["price_table"]["version"].as_str().is_some());
    assert!(
        j["totals"]["provisional_charges_micros"]["total"]
            .as_u64()
            .unwrap()
            > 0
    );

    // Tenant B: nothing, even when naming A's function.
    let b = get(&r, &format!("/v1/usage?function_id={function_id}"), TOKEN_B).await;
    assert_eq!(b.status, StatusCode::OK);
    assert_eq!(b.json()["tenant_id"], TENANT_B);
    assert_eq!(b.json()["totals"]["usage"]["attempts"], 0);
    assert!(b.json()["lines"].as_array().unwrap().is_empty());
    // The operator role reads no usage, like invocation history.
    assert_eq!(
        get(&r, "/v1/usage", TOKEN_A_OP).await.status,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &r,
            req(Method::GET, "/v1/usage", None)
                .body(Body::empty())
                .unwrap()
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&r, "/v1/usage?from=yesterday", TOKEN_A).await.status,
        StatusCode::BAD_REQUEST
    );

    let ready = call(
        &r,
        req(Method::GET, "/readyz", None)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(ready.json()["usage"]["accepting"], true);
    assert_eq!(ready.json()["usage"]["billing_enabled"], false);
    assert!(ready.json()["usage"]["collector"]["runs"].as_u64().unwrap() >= 1);

    // Fail closed.
    app.usage_meter.journal().force_unavailable(true);
    let refused = post_json(
        &r,
        &format!("/v1/functions/{function_id}/invoke"),
        TOKEN_A,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(refused.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(refused.json()["error"]["code"], "usage_journal_full");
    assert_eq!(
        refused.json()["error"]["reason"],
        "usage_journal_unavailable"
    );
    assert_eq!(
        fake.created().len(),
        2,
        "nothing booted for the refused one"
    );
    let not_ready = call(
        &r,
        req(Method::GET, "/readyz", None)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(not_ready.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(not_ready.json()["usage"]["accepting"], false);
    app.usage_meter.journal().force_unavailable(false);
}
