//! Function reference resolution: `fn_<ulid>` ids pass through, anything else is
//! looked up by name in the tenant's function list.

use tachyon_serverless_api_types::{FunctionResponse, ListResponse};

use crate::client::ApiClient;
use crate::error::{CliError, ExitCode};

const ULID_LEN: usize = 26;

/// True when `s` has the shape of a function id (`fn_` + 26 lowercase Crockford chars).
pub fn looks_like_function_id(s: &str) -> bool {
    match s.strip_prefix("fn_") {
        Some(rest) => {
            rest.len() == ULID_LEN
                && rest.bytes().all(|b| {
                    matches!(b, b'0'..=b'9' | b'a'..=b'h' | b'j'..=b'k' | b'm'..=b'n' | b'p'..=b't' | b'v'..=b'z')
                })
        }
        None => false,
    }
}

/// Fetch every function of the tenant, following `next_cursor`.
pub async fn list_all_functions(client: &ApiClient) -> Result<Vec<FunctionResponse>, CliError> {
    let mut items = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..1000 {
        let path = match &cursor {
            Some(c) => format!("/v1/functions?cursor={}", urlencode(c)),
            None => "/v1/functions".to_string(),
        };
        let page: ListResponse<FunctionResponse> = client.get_json(&path).await?;
        items.extend(page.items);
        match page.next_cursor {
            Some(c) if !c.is_empty() => cursor = Some(c),
            _ => break,
        }
    }
    Ok(items)
}

/// Resolve a name or id to a function id (does not verify that an id exists).
pub async fn resolve_function_id(client: &ApiClient, reference: &str) -> Result<String, CliError> {
    if looks_like_function_id(reference) {
        return Ok(reference.to_string());
    }
    let functions = list_all_functions(client).await?;
    let mut matches = functions
        .into_iter()
        .filter(|f| f.name == reference && f.deleted_at.is_none());
    match (matches.next(), matches.next()) {
        (Some(f), None) => Ok(f.id),
        (Some(_), Some(_)) => Err(CliError::usage(format!(
            "function name `{reference}` is ambiguous; use the fn_ id"
        ))),
        (None, _) => {
            let message = format!("function `{reference}` not found in this tenant");
            let raw = serde_json::json!({"error": {"code": "not_found", "message": message}});
            Err(CliError::failed(
                ExitCode::Api,
                message,
                Some(raw.to_string()),
            ))
        }
    }
}

pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_shape() {
        assert!(looks_like_function_id("fn_01hzzzzzzzzzzzzzzzzzzzzzza"));
        assert!(!looks_like_function_id("fn_01HZZZZZZZZZZZZZZZZZZZZZZA"));
        assert!(!looks_like_function_id("hello"));
        assert!(!looks_like_function_id("fn_short"));
        assert!(!looks_like_function_id("rev_01hzzzzzzzzzzzzzzzzzzzzzza"));
    }

    #[test]
    fn urlencode_escapes() {
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
        assert_eq!(urlencode("plain-1_2.3~"), "plain-1_2.3~");
    }
}
