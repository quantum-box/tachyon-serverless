//! `functions rollback`: point an alias back at its previous revision with CAS.

use tachyon_serverless_api_types::{AliasResponse, UpdateAliasRequest};

use crate::client::ApiClient;
use crate::error::{CliError, ExitCode};
use crate::output::Printer;
use crate::resolve::resolve_function_id;

pub async fn rollback(
    client: &ApiClient,
    function: &str,
    alias: &str,
    to: Option<&str>,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let function_id = resolve_function_id(client, function).await?;
    let alias_path = format!("/v1/functions/{function_id}/aliases/{alias}");
    let current: AliasResponse = client.get_json(&alias_path).await?;
    let target = match to {
        Some(t) => t.to_string(),
        None => current.previous_revision_id.clone().ok_or_else(|| {
            CliError::failed(
                ExitCode::Api,
                format!(
                    "alias `{alias}` of {function_id} has no previous revision to roll back to; pass --to <rev_id>"
                ),
                None,
            )
        })?,
    };
    if target == current.revision_id {
        p.note(format!(
            "alias `{alias}` already points at {target} (generation {})",
            current.generation
        ))?;
    }
    let req = UpdateAliasRequest {
        revision_id: target.clone(),
        expected_generation: Some(current.generation),
    };
    let resp = client.put_json(&alias_path, &req).await?.ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let updated: AliasResponse = resp.json()?;
    p.kv(&[
        ("alias", alias.to_string()),
        ("function", function_id),
        ("from", current.revision_id),
        ("to", updated.revision_id),
        (
            "generation",
            format!("{} -> {}", current.generation, updated.generation),
        ),
    ])
}
