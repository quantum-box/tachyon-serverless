//! `[console]` (PLT-4644): the static console is off by default, adds no
//! credential and never serves a file outside its root.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use tower::ServiceExt;

use tachyon_serverless_application::{Application, BootstrapOptions, GatewayConfig};
use tachyon_serverless_gateway::console::{CONTENT_SECURITY_POLICY, ConsoleConfig};
use tachyon_serverless_gateway::router_with_console;
use tachyon_serverless_provider_fake::FakeExecutionProvider;

const TOKEN_A: &str = "console-test-token-a";
const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";

fn gateway_toml(data: &std::path::Path, console: &str) -> String {
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

[[identity.tokens]]
token = "{TOKEN_A}"
tenant_id = "{TENANT_A}"
subject = "console-a"
roles = ["deploy", "invoke"]

{console}
"#,
        data = data.display()
    )
}

struct Fixture {
    router: Router,
    _data: tempfile::TempDir,
    _site: tempfile::TempDir,
}

/// A gateway whose configuration file carries `console` verbatim, with a
/// fake built console (index, a nested page, 404 page, an asset) in a
/// temporary directory, and a file *next to* that directory that must never
/// be reachable.
fn fixture(console: impl FnOnce(&std::path::Path) -> String) -> Fixture {
    let data = tempfile::tempdir().unwrap();
    let site = tempfile::tempdir().unwrap();
    let root = site.path().join("out");
    std::fs::create_dir_all(root.join("functions")).unwrap();
    std::fs::create_dir_all(root.join("_next/static/chunks")).unwrap();
    std::fs::write(root.join("index.html"), "<html>console-index</html>").unwrap();
    std::fs::write(
        root.join("functions/index.html"),
        "<html>console-functions</html>",
    )
    .unwrap();
    std::fs::write(root.join("404.html"), "<html>console-404</html>").unwrap();
    std::fs::write(root.join("_next/static/chunks/app.js"), "console.log(1)").unwrap();
    std::fs::write(site.path().join("outside.txt"), "outside-the-root").unwrap();

    let text = gateway_toml(data.path(), &console(&root));
    // The application configuration ignores `[console]`; the gateway reads it.
    let config = GatewayConfig::from_toml(&text).unwrap();
    let console_config = ConsoleConfig::from_toml(&text).unwrap();
    console_config.validate().unwrap();
    let app = Application::bootstrap_with(
        config,
        Arc::new(FakeExecutionProvider::with_scripts(vec![])),
        BootstrapOptions {
            persist_state: false,
            ..BootstrapOptions::default()
        },
    )
    .unwrap();
    Fixture {
        router: router_with_console(app, &console_config),
        _data: data,
        _site: site,
    }
}

async fn send(
    router: &Router,
    method: Method,
    path: &str,
    token: Option<&str>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut b = Request::builder().method(method).uri(path);
    if let Some(t) = token {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let res = router
        .clone()
        .oneshot(b.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, headers, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn console_is_off_by_default() {
    let f = fixture(|_| String::new());
    let (status, _, body) = send(&f.router, Method::GET, "/console/", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("not_found"), "the gateway's JSON 404: {body}");
    let (status, _, _) = send(&f.router, Method::GET, "/console", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn enabled_console_serves_the_export_with_security_headers() {
    let f = fixture(|root| format!("[console]\nenabled = true\ndir = \"{}\"\n", root.display()));

    let (status, headers, _) = send(&f.router, Method::GET, "/console", None).await;
    assert_eq!(status, StatusCode::PERMANENT_REDIRECT);
    assert_eq!(headers[header::LOCATION], "/console/");

    let (status, headers, body) = send(&f.router, Method::GET, "/console/", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("console-index"));
    assert_eq!(
        headers[header::CONTENT_SECURITY_POLICY],
        CONTENT_SECURITY_POLICY
    );
    assert_eq!(headers[header::X_FRAME_OPTIONS], "DENY");
    assert_eq!(headers[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
    assert_eq!(headers[header::REFERRER_POLICY], "no-referrer");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert!(
        headers[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );

    let (status, _, body) = send(&f.router, Method::GET, "/console/functions/", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("console-functions"));

    let (status, headers, _) = send(
        &f.router,
        Method::GET,
        "/console/_next/static/chunks/app.js",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers[header::CACHE_CONTROL]
            .to_str()
            .unwrap()
            .contains("immutable")
    );

    // Unknown pages answer the export's 404 page with 404.
    let (status, _, body) = send(&f.router, Method::GET, "/console/no-such-page", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("console-404"), "{body}");
}

#[tokio::test]
async fn enabled_console_never_leaves_its_root_and_grants_nothing() {
    let f = fixture(|root| format!("[console]\nenabled = true\ndir = \"{}\"\n", root.display()));
    for path in [
        "/console/../outside.txt",
        "/console/%2e%2e/outside.txt",
        "/console/..%2foutside.txt",
        "/console/%2e%2e%2foutside.txt",
    ] {
        let (status, _, body) = send(&f.router, Method::GET, path, None).await;
        assert!(
            !body.contains("outside-the-root"),
            "{path} escaped the console root ({status})"
        );
    }
    // The console carries no credential: the API still refuses a request
    // without a token, and a page load does not change that.
    let _ = send(&f.router, Method::GET, "/console/", None).await;
    let (status, _, _) = send(&f.router, Method::GET, "/v1/functions", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = send(&f.router, Method::GET, "/v1/functions", Some(TOKEN_A)).await;
    assert_eq!(status, StatusCode::OK);
    // Writes under /console are not a thing.
    let (status, _, _) = send(&f.router, Method::POST, "/console/", None).await;
    assert!(
        status.is_client_error(),
        "POST /console/ must be refused, got {status}"
    );
}

#[test]
fn enabled_console_without_a_build_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let c = ConsoleConfig::from_toml(&format!(
        "[console]\nenabled = true\ndir = \"{}\"\n",
        dir.path().join("missing").display()
    ))
    .unwrap();
    assert!(c.validate().unwrap_err().contains("index.html"));
}
