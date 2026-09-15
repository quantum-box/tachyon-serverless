//! `functions invoke`: synchronous JSON invoke.

use bytes::Bytes;
use reqwest::Method;
use tachyon_serverless_api_types::headers;

use crate::args::InvokeArgs;
use crate::client::{ApiClient, ApiResponse};
use crate::error::CliError;
use crate::output::Printer;
use crate::resolve::{resolve_function_id, urlencode};

/// Load and validate the payload (`{}` when none given).
pub fn load_payload(args: &InvokeArgs) -> Result<String, CliError> {
    let raw = match (&args.payload, &args.payload_file) {
        (Some(p), _) => p.clone(),
        (None, Some(path)) => std::fs::read_to_string(path).map_err(|e| {
            CliError::usage(format!(
                "cannot read --payload-file {}: {e}",
                path.display()
            ))
        })?,
        (None, None) => "{}".to_string(),
    };
    serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|e| CliError::usage(format!("--payload is not valid JSON: {e}")))?;
    Ok(raw)
}

/// Build the invoke path including the alias / revision query.
pub fn invoke_path(client: &ApiClient, function_id: &str, args: &InvokeArgs) -> String {
    let mut path = client.invoke_path(function_id);
    let mut query = Vec::new();
    if let Some(a) = &args.alias {
        query.push(format!("alias={}", urlencode(a)));
    }
    if let Some(r) = &args.revision_id {
        query.push(format!("revision_id={}", urlencode(r)));
    }
    if !query.is_empty() {
        path.push('?');
        path.push_str(&query.join("&"));
    }
    path
}

pub async fn send_invoke(
    client: &ApiClient,
    function_id: &str,
    args: &InvokeArgs,
    payload: String,
) -> Result<ApiResponse, CliError> {
    let path = invoke_path(client, function_id, args);
    let mut extra = Vec::new();
    if let Some(ms) = args.client_timeout_ms {
        extra.push((headers::CLIENT_TIMEOUT_MS.to_string(), ms.to_string()));
    }
    if let Some(k) = &args.idempotency_key {
        extra.push((headers::IDEMPOTENCY_KEY.to_string(), k.clone()));
    }
    client
        .send(
            Method::POST,
            &path,
            true,
            Some("application/json"),
            &extra,
            Some(Bytes::from(payload)),
        )
        .await
}

/// Print a successful invoke response.
pub fn print_invoke_response(resp: &ApiResponse, p: &mut Printer<'_>) -> Result<(), CliError> {
    let body = resp.body_text();
    if let Some(id) = resp.header(headers::INVOCATION_ID) {
        let trace = resp
            .header(headers::TRACE_ID)
            .map(|t| format!(" trace={t}"))
            .unwrap_or_default();
        p.note(format!("invocation {id}{trace}"))?;
    }
    if p.json {
        return p.raw(&body);
    }
    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(v) => p.pretty_json(&v),
        Err(_) => p.raw(&body),
    }
}

pub async fn invoke(
    client: &ApiClient,
    args: &InvokeArgs,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let payload = load_payload(args)?;
    let function_id = resolve_function_id(client, &args.function).await?;
    let resp = send_invoke(client, &function_id, args, payload).await?;
    if !resp.is_success() {
        return Err(resp.into_error());
    }
    print_invoke_response(&resp, p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> InvokeArgs {
        InvokeArgs {
            function: "hello".into(),
            payload: None,
            payload_file: None,
            alias: None,
            revision_id: None,
            client_timeout_ms: None,
            idempotency_key: None,
        }
    }

    #[test]
    fn default_payload_is_empty_object() {
        assert_eq!(load_payload(&args()).unwrap(), "{}");
    }

    #[test]
    fn invalid_payload_is_usage_error() {
        let mut a = args();
        a.payload = Some("{not json".into());
        assert!(matches!(load_payload(&a), Err(CliError::Usage(_))));
    }
}
