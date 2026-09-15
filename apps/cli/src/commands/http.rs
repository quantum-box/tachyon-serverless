//! `functions http`: send a request through the HTTP adapter and pass the
//! function's response through. Any status the function returns is exit 0;
//! only platform errors (a well-formed API error body) are non-zero.

use bytes::Bytes;
use reqwest::Method;

use crate::args::{HttpArgs, parse_header};
use crate::client::{ApiClient, ApiResponse};
use crate::error::CliError;
use crate::output::Printer;
use crate::resolve::resolve_function_id;

/// `/v1/functions/{id}/http` + path (leading slash enforced).
pub fn adapter_path(function_id: &str, path: &str) -> String {
    let p = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    format!("/v1/functions/{function_id}/http{p}")
}

/// True when the gateway (not the function) produced the response.
pub fn is_platform_error(resp: &ApiResponse) -> bool {
    resp.status >= 400 && resp.api_error().is_some()
}

fn load_body(args: &HttpArgs) -> Result<Option<Bytes>, CliError> {
    match (&args.data, &args.data_file) {
        (Some(d), _) => Ok(Some(Bytes::from(d.clone()))),
        (None, Some(path)) => std::fs::read(path)
            .map(|b| Some(Bytes::from(b)))
            .map_err(|e| {
                CliError::usage(format!("cannot read --data-file {}: {e}", path.display()))
            }),
        (None, None) => Ok(None),
    }
}

pub async fn http(
    client: &ApiClient,
    args: &HttpArgs,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let method = Method::from_bytes(args.method.to_ascii_uppercase().as_bytes())
        .map_err(|_| CliError::usage(format!("invalid --method `{}`", args.method)))?;
    let headers = args
        .header
        .iter()
        .map(|h| parse_header(h))
        .collect::<Result<Vec<_>, _>>()?;
    let body = load_body(args)?;
    let function_id = resolve_function_id(client, &args.function).await?;
    let path = adapter_path(&function_id, &args.path);
    let resp = client
        .send(method, &path, true, None, &headers, body)
        .await?;
    if is_platform_error(&resp) {
        return Err(resp.into_error());
    }
    print_http_response(&resp, args.verbose, p)
}

pub fn print_http_response(
    resp: &ApiResponse,
    verbose: bool,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let body = resp.body_text();
    if p.json {
        let headers: Vec<serde_json::Value> = resp
            .headers
            .iter()
            .map(|(k, v)| serde_json::json!([k.as_str(), String::from_utf8_lossy(v.as_bytes())]))
            .collect();
        let v = serde_json::json!({
            "status": resp.status,
            "headers": headers,
            "body": body,
        });
        return p.raw(&v.to_string());
    }
    p.line(format!("HTTP {}", resp.status))?;
    if verbose {
        for (k, v) in resp.headers.iter() {
            p.line(format!("{}: {}", k, String::from_utf8_lossy(v.as_bytes())))?;
        }
        p.line("")?;
    }
    if !body.is_empty() {
        p.raw(&body)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_path_normalises_slash() {
        assert_eq!(adapter_path("fn_x", "/"), "/v1/functions/fn_x/http/");
        assert_eq!(
            adapter_path("fn_x", "status/404?x=1"),
            "/v1/functions/fn_x/http/status/404?x=1"
        );
    }

    #[test]
    fn platform_error_detection() {
        let mk = |status: u16, body: &str| ApiResponse {
            status,
            headers: Default::default(),
            body: Bytes::from(body.to_string()),
        };
        assert!(is_platform_error(&mk(
            404,
            r#"{"error":{"code":"not_found","message":"no"}}"#
        )));
        assert!(!is_platform_error(&mk(404, "not found")));
        assert!(!is_platform_error(&mk(
            200,
            r#"{"error":{"code":"not_found","message":"no"}}"#
        )));
    }
}
