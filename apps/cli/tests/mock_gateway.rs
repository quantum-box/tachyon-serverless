//! Integration tests: the CLI against an in-process axum mock of the gateway API.
//!
//! The mock is a single fallback handler with manual routing so that both the
//! `/invoke` and `:invoke` route forms can be served and every request is
//! recorded for assertions.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use clap::Parser;
use serde_json::{Value, json};
use tachyon_serverless_cli::{Cli, ExitCode, run};

const FN_HELLO: &str = "fn_01hzzzzzzzzzzzzzzzzzzzzzz1";
const FN_OTHER: &str = "fn_01hzzzzzzzzzzzzzzzzzzzzzz2";
const REV_A: &str = "rev_01hzzzzzzzzzzzzzzzzzzzzzza";
const REV_B: &str = "rev_01hzzzzzzzzzzzzzzzzzzzzzzb";
const INV: &str = "inv_01hzzzzzzzzzzzzzzzzzzzzzz1";
const TOKEN: &str = "test-token";
const TS: &str = "2026-09-15T00:00:00Z";

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Default)]
struct MockState {
    requests: Vec<Recorded>,
    /// How many revision polls answer `pending` before `ready`.
    pending_polls: u32,
    /// Response for the invoke route: (status, body, invocation id header).
    invoke: Option<(u16, Value)>,
    /// Response for the HTTP adapter: (status, body).
    http_adapter: Option<(u16, String)>,
    /// Alias generation reported by GET.
    alias_generation: u64,
    /// When set, PUT alias answers 409.
    alias_conflict: bool,
}

type Shared = Arc<Mutex<MockState>>;

fn function_json(id: &str, name: &str) -> Value {
    json!({
        "id": id, "tenant_id": "tn_01hzzzzzzzzzzzzzzzzzzzzzza", "name": name,
        "description": "", "created_at": TS, "updated_at": TS
    })
}

fn revision_json(id: &str, status: &str) -> Value {
    json!({
        "id": id, "function_id": FN_HELLO, "number": 2, "status": status,
        "spec": {"runtime": {"architecture": "aarch64"}, "description": "d"},
        "spec_digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        "created_at": TS, "updated_at": TS
    })
}

fn alias_json(revision: &str, generation: u64, previous: Option<&str>) -> Value {
    json!({
        "function_id": FN_HELLO, "name": "prod", "revision_id": revision,
        "generation": generation, "previous_revision_id": previous, "updated_at": TS
    })
}

fn invocation_json(status: &str) -> Value {
    json!({
        "id": INV, "function_id": FN_HELLO, "revision_id": REV_A, "alias": "prod",
        "mode": "sync", "status": status, "trace_id": "t", "input_digest": "sha256:ab",
        "input_size_bytes": 2, "accepted_at": TS,
        "deadlines": {"queue_deadline": TS, "client_deadline": TS},
        "attempts": [{
            "id": "att_01hzzzzzzzzzzzzzzzzzzzzzz1", "number": 1,
            "environment_id": "env_01hzzzzzzzzzzzzzzzzzzzzzz1", "epoch": 1,
            "status": "succeeded", "start_kind": "cold",
            "timings": {"queue_wait_ms": 1, "environment_boot_ms": 2, "runtime_init_ms": 3,
                        "handler_ms": 4, "response_ms": 5, "total_ms": 15},
            "boot_evidence": {"guest_boot_id": null, "host_pid": 4242, "details": {}},
            "dispatched_at": TS
        }]
    })
}

fn api_error(
    status: u16,
    code: &str,
    invocation: Option<&str>,
    error_type: Option<&str>,
) -> (u16, Value) {
    (
        status,
        json!({"error": {"code": code, "message": format!("{code} happened"),
                         "invocation_id": invocation, "error_type": error_type}}),
    )
}

fn json_response(status: u16, v: &Value, extra: &[(&str, &str)]) -> Response {
    let mut r = (
        StatusCode::from_u16(status).unwrap(),
        [("content-type", "application/json")],
        v.to_string(),
    )
        .into_response();
    for (k, v) in extra {
        r.headers_mut().insert(
            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    r
}

async fn handler(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("").to_string();
    let mut st = state.lock().unwrap();
    st.requests.push(Recorded {
        method: method.to_string(),
        path: if query.is_empty() {
            path.clone()
        } else {
            format!("{path}?{query}")
        },
        headers: headers
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect(),
        body: body.to_vec(),
    });

    // Authentication: everything under /v1 except /v1/provider needs the token.
    if path.starts_with("/v1/") && path != "/v1/provider" {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if auth != format!("Bearer {TOKEN}") {
            let (s, b) = api_error(401, "unauthorized", None, None);
            return json_response(s, &b, &[]);
        }
    }

    let m = method.as_str();
    match (m, path.as_str()) {
        ("GET", "/healthz") => (StatusCode::OK, "ok").into_response(),
        ("GET", "/readyz") => (StatusCode::OK, "ready").into_response(),
        ("GET", "/v1/provider") => json_response(
            200,
            &json!({
                "kind": "process", "dev_only": true, "isolation": "process",
                "capabilities": {"isolation": "process", "dev_only": true,
                    "create_terminate": {"status": "supported"},
                    "snapshot_create": {"status": "unsupported", "reason": "P1"}},
                "preflight": {"provider": "process", "ok": true,
                    "checks": [{"name": "bridge", "ok": true, "detail": "found"}]}
            }),
            &[],
        ),
        ("GET", "/v1/functions") => json_response(
            200,
            &json!({"items": [function_json(FN_HELLO, "hello"), function_json(FN_OTHER, "other")]}),
            &[],
        ),
        ("POST", "/v1/functions") => {
            let req: Value = serde_json::from_slice(&body).unwrap();
            json_response(
                201,
                &function_json(FN_HELLO, req["name"].as_str().unwrap()),
                &[],
            )
        }
        ("POST", "/v1/artifacts") => {
            let ct = headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if ct != "application/octet-stream" {
                let (s, b) = api_error(400, "invalid_request", None, None);
                return json_response(s, &b, &[]);
            }
            json_response(
                201,
                &json!({"digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111", "size_bytes": body.len()}),
                &[],
            )
        }
        _ => {
            if path == format!("/v1/functions/{FN_HELLO}") && m == "GET" {
                return json_response(200, &function_json(FN_HELLO, "hello"), &[]);
            }
            if path == format!("/v1/functions/{FN_OTHER}") && m == "GET" {
                let (s, b) = api_error(404, "not_found", None, None);
                return json_response(s, &b, &[]);
            }
            if path == format!("/v1/functions/{FN_HELLO}/revisions") && m == "POST" {
                return json_response(202, &revision_json(REV_B, "pending"), &[]);
            }
            if path == format!("/v1/functions/{FN_HELLO}/revisions/{REV_B}") && m == "GET" {
                if st.pending_polls > 0 {
                    st.pending_polls -= 1;
                    return json_response(200, &revision_json(REV_B, "validating"), &[]);
                }
                return json_response(200, &revision_json(REV_B, "ready"), &[]);
            }
            if path == format!("/v1/functions/{FN_HELLO}/aliases/prod") && m == "GET" {
                let generation = st.alias_generation;
                return json_response(200, &alias_json(REV_B, generation, Some(REV_A)), &[]);
            }
            if path == format!("/v1/functions/{FN_HELLO}/aliases/prod") && m == "PUT" {
                if st.alias_conflict {
                    let (s, b) = api_error(409, "conflict", None, None);
                    return json_response(s, &b, &[]);
                }
                let req: Value = serde_json::from_slice(&body).unwrap();
                let generation = st.alias_generation + 1;
                let target = req["revision_id"].as_str().unwrap().to_string();
                return json_response(200, &alias_json(&target, generation, Some(REV_B)), &[]);
            }
            if (path == format!("/v1/functions/{FN_HELLO}/invoke")
                || path == format!("/v1/functions/{FN_HELLO}:invoke"))
                && m == "POST"
            {
                let (status, body) = st
                    .invoke
                    .clone()
                    .unwrap_or((200, json!({"message": "hello, demo"})));
                return json_response(status, &body, &[("x-tachyon-invocation-id", INV)]);
            }
            if path == format!("/v1/functions/{FN_OTHER}/invoke") && m == "POST" {
                let (s, b) = api_error(404, "not_found", None, None);
                return json_response(s, &b, &[]);
            }
            if path.starts_with(&format!("/v1/functions/{FN_HELLO}/http/")) {
                let (status, body) = st.http_adapter.clone().unwrap_or((200, "ok".to_string()));
                return (StatusCode::from_u16(status).unwrap(), [("x-fn", "1")], body)
                    .into_response();
            }
            if path == format!("/v1/functions/{FN_HELLO}/invocations") && m == "GET" {
                return json_response(200, &json!({"items": [invocation_json("succeeded")]}), &[]);
            }
            if path == format!("/v1/invocations/{INV}") && m == "GET" {
                return json_response(200, &invocation_json("succeeded"), &[]);
            }
            if path == format!("/v1/invocations/{INV}/logs") && m == "GET" {
                return json_response(
                    200,
                    &json!({"items": [
                        {"timestamp": TS, "stream": "stdout", "phase": "handler",
                         "environment_id": "env_01hzzzzzzzzzzzzzzzzzzzzzz1", "line": "handler ran", "truncated": false},
                        {"timestamp": TS, "stream": "stderr", "phase": "init",
                         "environment_id": "env_01hzzzzzzzzzzzzzzzzzzzzzz1", "line": "cut", "truncated": true}
                    ], "dropped": true}),
                    &[],
                );
            }
            if (path == format!("/v1/invocations/{INV}/cancel")
                || path == format!("/v1/invocations/{INV}:cancel"))
                && m == "POST"
            {
                return json_response(200, &invocation_json("cancelled"), &[]);
            }
            let (s, b) = api_error(404, "not_found", None, None);
            json_response(s, &b, &[])
        }
    }
}

struct Mock {
    url: String,
    state: Shared,
}

impl Mock {
    async fn start(state: MockState) -> Self {
        let state: Shared = Arc::new(Mutex::new(state));
        let app = Router::new().fallback(handler).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{addr}"),
            state,
        }
    }

    fn requests(&self) -> Vec<Recorded> {
        self.state.lock().unwrap().requests.clone()
    }

    fn paths(&self) -> Vec<String> {
        self.requests()
            .iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect()
    }

    /// Run the CLI with the mock URL and token prepended. Returns (exit, stdout, stderr).
    async fn tsls(&self, args: &[&str]) -> (ExitCode, String, String) {
        self.tsls_with_token(TOKEN, args).await
    }

    async fn tsls_with_token(&self, token: &str, args: &[&str]) -> (ExitCode, String, String) {
        let mut argv = vec![
            "tsls",
            "--api-url",
            &self.url,
            "--token",
            token,
            "--timeout-secs",
            "5",
        ];
        argv.extend_from_slice(args);
        let cli = Cli::try_parse_from(argv).expect("args parse");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = run(cli, &mut out, &mut err).await;
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }
}

#[tokio::test]
async fn resolves_function_by_name_and_by_id() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, _) = mock.tsls(&["functions", "get", "hello"]).await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains(FN_HELLO), "{out}");
    let paths = mock.paths();
    assert_eq!(paths[0], "GET /v1/functions");
    assert_eq!(paths[1], format!("GET /v1/functions/{FN_HELLO}"));

    let (code, out, _) = mock.tsls(&["functions", "get", FN_HELLO, "--json"]).await;
    assert_eq!(code, ExitCode::Ok);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["name"], "hello");
    // An id is used directly without listing.
    assert_eq!(mock.paths()[2], format!("GET /v1/functions/{FN_HELLO}"));
}

#[tokio::test]
async fn unknown_name_is_exit_2_with_json_error() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, err) = mock.tsls(&["functions", "get", "nope", "--json"]).await;
    assert_eq!(code, ExitCode::Api);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["error"]["code"], "not_found");
    assert!(err.contains("not found"), "{err}");
}

#[tokio::test]
async fn cross_tenant_resource_is_404_exit_2() {
    let mock = Mock::start(MockState::default()).await;
    let (code, _, err) = mock.tsls(&["functions", "get", FN_OTHER]).await;
    assert_eq!(code, ExitCode::Api);
    assert!(err.contains("not_found (HTTP 404)"), "{err}");
    let (code, _, _) = mock
        .tsls(&["functions", "invoke", FN_OTHER, "--payload", "{}"])
        .await;
    assert_eq!(code, ExitCode::Api);
}

#[tokio::test]
async fn missing_token_is_usage_error() {
    let mock = Mock::start(MockState::default()).await;
    let cli = Cli::try_parse_from([
        "tsls",
        "--api-url",
        &mock.url,
        "--token",
        "",
        "functions",
        "list",
    ])
    .unwrap();
    // An explicitly empty token still counts as "present"; simulate absence via a fresh parse
    // without --token and with the env var cleared.
    drop(cli);
    let mut cli =
        Cli::try_parse_from(["tsls", "--api-url", &mock.url, "functions", "list"]).unwrap();
    cli.token = None;
    let mut out = Vec::new();
    let mut err = Vec::new();
    let code = run(cli, &mut out, &mut err).await;
    assert_eq!(code, ExitCode::Usage);
    assert!(String::from_utf8(err).unwrap().contains("TSLS_TOKEN"));
}

#[tokio::test]
async fn wrong_token_is_exit_2() {
    let mock = Mock::start(MockState::default()).await;
    let (code, _, err) = mock.tsls_with_token("bad", &["functions", "list"]).await;
    assert_eq!(code, ExitCode::Api);
    assert!(err.contains("unauthorized"), "{err}");
}

#[tokio::test]
async fn deploy_uploads_polls_and_reads_alias() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("hello");
    std::fs::write(&bin, b"#!/bin/sh\necho hi\n").unwrap();
    let mock = Mock::start(MockState {
        pending_polls: 2,
        alias_generation: 5,
        ..Default::default()
    })
    .await;
    let (code, out, err) = mock
        .tsls(&[
            "functions",
            "deploy",
            "--function",
            "hello",
            "--binary",
            bin.to_str().unwrap(),
            "--arch",
            "aarch64",
            "--env",
            "GREETING=v1",
            "--secret",
            "DEMO_SECRET=demo-secret",
            "--timeout-seconds",
            "2",
            "--json",
        ])
        .await;
    assert_eq!(code, ExitCode::Ok, "stderr: {err}");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["id"], REV_B);
    assert_eq!(v["status"], "ready");

    let reqs = mock.requests();
    let paths = mock.paths();
    assert_eq!(paths[0], "GET /v1/functions", "name resolution first");
    assert_eq!(paths[1], "POST /v1/artifacts");
    assert_eq!(reqs[1].body, b"#!/bin/sh\necho hi\n");
    assert_eq!(paths[2], format!("POST /v1/functions/{FN_HELLO}/revisions"));
    let rev_req: Value = serde_json::from_slice(&reqs[2].body).unwrap();
    assert_eq!(rev_req["artifact"]["kind"], "binary");
    assert_eq!(
        rev_req["artifact"]["digest"],
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    );
    assert_eq!(rev_req["architecture"], "aarch64");
    assert_eq!(rev_req["env_vars"][0], json!(["GREETING", "v1"]));
    assert_eq!(rev_req["secrets"][0]["binding_ref"], "demo-secret");
    assert_eq!(rev_req["execution"]["timeout_seconds"], 2);
    assert_eq!(rev_req["publish_to_prod"], true);
    // Three polls: validating, validating, ready.
    let polls = paths
        .iter()
        .filter(|p| *p == &format!("GET /v1/functions/{FN_HELLO}/revisions/{REV_B}"))
        .count();
    assert_eq!(polls, 3);
    assert!(paths.contains(&format!("GET /v1/functions/{FN_HELLO}/aliases/prod")));
    assert!(err.contains("ready"), "{err}");
}

#[tokio::test]
async fn deploy_no_wait_returns_after_create() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("hello");
    std::fs::write(&bin, b"x").unwrap();
    let mock = Mock::start(MockState::default()).await;
    let (code, out, _) = mock
        .tsls(&[
            "functions",
            "deploy",
            "--function",
            FN_HELLO,
            "--binary",
            bin.to_str().unwrap(),
            "--no-wait",
            "--no-publish",
            "--json",
        ])
        .await;
    assert_eq!(code, ExitCode::Ok);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["status"], "pending");
    assert_eq!(mock.paths().len(), 2, "no polling, no alias fetch");
}

#[tokio::test]
async fn deploy_missing_binary_is_usage_error() {
    let mock = Mock::start(MockState::default()).await;
    let (code, _, err) = mock
        .tsls(&[
            "functions",
            "deploy",
            "--function",
            FN_HELLO,
            "--binary",
            "/nonexistent/binary",
        ])
        .await;
    assert_eq!(code, ExitCode::Usage);
    assert!(err.contains("cannot read --binary"), "{err}");
}

#[tokio::test]
async fn rollback_uses_previous_revision_and_cas_generation() {
    let mock = Mock::start(MockState {
        alias_generation: 3,
        ..Default::default()
    })
    .await;
    let (code, out, _) = mock.tsls(&["functions", "rollback", "hello"]).await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains(&format!("from        {REV_B}")), "{out}");
    assert!(out.contains(&format!("to          {REV_A}")), "{out}");
    assert!(out.contains("3 -> 4"), "{out}");
    let put = mock
        .requests()
        .into_iter()
        .find(|r| r.method == "PUT")
        .expect("PUT alias");
    let body: Value = serde_json::from_slice(&put.body).unwrap();
    assert_eq!(body["revision_id"], REV_A);
    assert_eq!(body["expected_generation"], 3);
}

#[tokio::test]
async fn rollback_to_explicit_revision_and_conflict() {
    let mock = Mock::start(MockState {
        alias_generation: 7,
        ..Default::default()
    })
    .await;
    let (code, out, _) = mock
        .tsls(&["functions", "rollback", FN_HELLO, "--to", REV_A, "--json"])
        .await;
    assert_eq!(code, ExitCode::Ok);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["revision_id"], REV_A);
    assert_eq!(v["generation"], 8);

    mock.state.lock().unwrap().alias_conflict = true;
    let (code, _, err) = mock.tsls(&["functions", "rollback", FN_HELLO]).await;
    assert_eq!(code, ExitCode::Api);
    assert!(err.contains("conflict"), "{err}");
}

#[tokio::test]
async fn invoke_success_prints_output_and_invocation_id() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, err) = mock
        .tsls(&[
            "functions",
            "invoke",
            "hello",
            "--payload",
            r#"{"name":"demo"}"#,
            "--client-timeout-ms",
            "1500",
            "--idempotency-key",
            "k1",
            "--json",
        ])
        .await;
    assert_eq!(code, ExitCode::Ok);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["message"], "hello, demo");
    assert!(err.contains(INV), "{err}");
    let req = mock
        .requests()
        .into_iter()
        .find(|r| r.path.ends_with("/invoke"))
        .unwrap();
    assert_eq!(req.body, br#"{"name":"demo"}"#);
    assert!(
        req.headers
            .contains(&("x-tachyon-client-timeout-ms".into(), "1500".into()))
    );
    assert!(
        req.headers
            .contains(&("idempotency-key".into(), "k1".into()))
    );
    assert!(
        req.headers
            .contains(&("content-type".into(), "application/json".into()))
    );
}

#[tokio::test]
async fn invoke_query_and_colon_routes() {
    let mock = Mock::start(MockState::default()).await;
    let (code, _, _) = mock
        .tsls(&[
            "functions",
            "invoke",
            FN_HELLO,
            "--revision-id",
            REV_A,
            "--colon-routes",
        ])
        .await;
    assert_eq!(code, ExitCode::Ok);
    assert_eq!(
        mock.paths()[0],
        format!("POST /v1/functions/{FN_HELLO}:invoke?revision_id={REV_A}")
    );
    let (code, _, _) = mock
        .tsls(&["functions", "invoke", FN_HELLO, "--alias", "canary"])
        .await;
    assert_eq!(code, ExitCode::Ok);
    assert_eq!(
        mock.paths()[1],
        format!("POST /v1/functions/{FN_HELLO}/invoke?alias=canary")
    );
}

#[tokio::test]
async fn invoke_error_classes_map_to_exit_codes() {
    let cases = [
        (
            api_error(502, "user_error", Some(INV), Some("Handler.Error")),
            ExitCode::InvocationFailed,
        ),
        (
            api_error(502, "crash", Some(INV), Some("Runtime.Panic")),
            ExitCode::InvocationFailed,
        ),
        (
            api_error(502, "init_error", Some(INV), None),
            ExitCode::InvocationFailed,
        ),
        (
            api_error(504, "timeout", Some(INV), Some("Host.Timeout")),
            ExitCode::Timeout,
        ),
        (
            api_error(504, "queue_timeout", Some(INV), None),
            ExitCode::Timeout,
        ),
        (
            api_error(502, "outcome_unknown", Some(INV), None),
            ExitCode::OutcomeUnknown,
        ),
        (
            api_error(500, "platform_error", None, None),
            ExitCode::Platform,
        ),
        (
            api_error(503, "provider_unavailable", None, None),
            ExitCode::Platform,
        ),
        (
            api_error(409, "revision_not_ready", None, None),
            ExitCode::Api,
        ),
        (
            api_error(429, "capacity_exceeded", None, None),
            ExitCode::Api,
        ),
        (
            api_error(499, "cancelled", Some(INV), None),
            ExitCode::InvocationFailed,
        ),
    ];
    for ((status, body), expected) in cases {
        let code_name = body["error"]["code"].as_str().unwrap().to_string();
        let mock = Mock::start(MockState {
            invoke: Some((status, body.clone())),
            ..Default::default()
        })
        .await;
        let (code, out, err) = mock
            .tsls(&["functions", "invoke", FN_HELLO, "--json"])
            .await;
        assert_eq!(code, expected, "{code_name}: {err}");
        let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{code_name}: {out}"));
        assert_eq!(v["error"]["code"], code_name);
        assert!(err.contains(&code_name), "{err}");
        if body["error"]["invocation_id"].is_string() {
            assert!(err.contains(INV), "invocation id in message: {err}");
        }
    }
}

#[tokio::test]
async fn unparseable_5xx_is_platform_exit() {
    let mock = Mock::start(MockState {
        invoke: Some((500, json!("boom"))),
        ..Default::default()
    })
    .await;
    let (code, _, _) = mock.tsls(&["functions", "invoke", FN_HELLO]).await;
    assert_eq!(code, ExitCode::Platform);
}

#[tokio::test]
async fn unreachable_gateway_is_platform_exit() {
    let cli = Cli::try_parse_from([
        "tsls",
        "--api-url",
        "http://127.0.0.1:1",
        "--token",
        "t",
        "functions",
        "list",
    ])
    .unwrap();
    let mut out = Vec::new();
    let mut err = Vec::new();
    assert_eq!(run(cli, &mut out, &mut err).await, ExitCode::Platform);
}

#[tokio::test]
async fn http_adapter_passes_function_status_through() {
    let mock = Mock::start(MockState {
        http_adapter: Some((404, "nope".into())),
        ..Default::default()
    })
    .await;
    let (code, out, _) = mock
        .tsls(&[
            "functions",
            "http",
            "hello",
            "--method",
            "post",
            "--path",
            "/status/404?x=1",
            "--data",
            "body",
            "--header",
            "x-demo: 1",
            "--json",
        ])
        .await;
    assert_eq!(code, ExitCode::Ok, "function 404 is not a CLI failure");
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["status"], 404);
    assert_eq!(v["body"], "nope");
    assert!(
        v["headers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h[0] == "x-fn")
    );
    let req = mock.requests().into_iter().last().unwrap();
    assert_eq!(req.method, "POST");
    assert_eq!(
        req.path,
        format!("/v1/functions/{FN_HELLO}/http/status/404?x=1")
    );
    assert_eq!(req.body, b"body");
    assert!(req.headers.contains(&("x-demo".into(), "1".into())));

    let (code, out, _) = mock
        .tsls(&["functions", "http", FN_HELLO, "--path", "/", "-v"])
        .await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.starts_with("HTTP 404\n"), "{out}");
    assert!(out.contains("x-fn: 1"), "{out}");
}

#[tokio::test]
async fn http_adapter_platform_error_is_nonzero() {
    let (_, body) = api_error(404, "not_found", None, None);
    let mock = Mock::start(MockState {
        http_adapter: Some((404, body.to_string())),
        ..Default::default()
    })
    .await;
    let (code, _, _) = mock.tsls(&["functions", "http", FN_HELLO]).await;
    assert_eq!(code, ExitCode::Api);
}

#[tokio::test]
async fn logs_render_markers() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, _) = mock.tsls(&["functions", "logs", "--invocation", INV]).await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains("[stdout/handler] handler ran\n"), "{out}");
    assert!(out.contains("[stderr/init] cut [truncated]\n"), "{out}");
    assert!(out.contains("-- dropped"), "{out}");

    let (code, out, _) = mock
        .tsls(&["functions", "logs", "--function", "hello", "--limit", "3"])
        .await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains(&format!("== {INV} (succeeded)")), "{out}");
    assert!(
        mock.paths()
            .contains(&format!("GET /v1/functions/{FN_HELLO}/invocations?limit=3"))
    );
}

#[tokio::test]
async fn invocation_detail_and_history() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, _) = mock.tsls(&["functions", "invocation", INV]).await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains("host_pid=4242"), "{out}");
    assert!(out.contains("handler=4 ms"), "{out}");
    let (code, out, _) = mock
        .tsls(&["functions", "invocations", "hello", "--limit", "7"])
        .await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains(INV), "{out}");
    assert!(
        mock.paths()
            .contains(&format!("GET /v1/functions/{FN_HELLO}/invocations?limit=7"))
    );
}

#[tokio::test]
async fn cancel_routes() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, _) = mock.tsls(&["functions", "cancel", INV]).await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains("cancelled"), "{out}");
    assert_eq!(
        mock.paths()[0],
        format!("POST /v1/invocations/{INV}/cancel")
    );
    let (code, _, _) = mock
        .tsls(&["functions", "cancel", INV, "--colon-routes"])
        .await;
    assert_eq!(code, ExitCode::Ok);
    assert_eq!(
        mock.paths()[1],
        format!("POST /v1/invocations/{INV}:cancel")
    );
}

#[tokio::test]
async fn provider_and_health() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, err) = mock.tsls(&["provider"]).await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.contains("kind       process"), "{out}");
    assert!(out.contains("snapshot_create   unsupported  P1"), "{out}");
    assert!(err.contains("NO isolation"), "{err}");
    let (code, out, _) = mock.tsls(&["health", "--json"]).await;
    assert_eq!(code, ExitCode::Ok);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["healthz"]["status"], 200);
    assert_eq!(v["readyz"]["status"], 200);
}

#[tokio::test]
async fn create_and_list_functions() {
    let mock = Mock::start(MockState::default()).await;
    let (code, out, _) = mock
        .tsls(&["functions", "create", "--name", "hello", "--json"])
        .await;
    assert_eq!(code, ExitCode::Ok);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["name"], "hello");
    let req = mock.requests().into_iter().next().unwrap();
    let body: Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body, json!({"name": "hello", "description": ""}));

    let (code, out, _) = mock.tsls(&["functions", "list"]).await;
    assert_eq!(code, ExitCode::Ok);
    assert!(out.starts_with("ID"), "{out}");
    assert!(out.contains("hello") && out.contains("other"), "{out}");
}
