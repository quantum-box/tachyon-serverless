//! `dead-letters list|show|redrive` (PLT-4640).

use tachyon_serverless_api_types::{
    DeadLetterResponse, RedriveAcceptedResponse, RedriveRequestBody,
};

use crate::client::ApiClient;
use crate::commands::decode_items;
use crate::error::CliError;
use crate::output::{Printer, Table, ellipsize, opt, opt_ts, ts};
use crate::resolve::resolve_function_id;

fn last_error(d: &DeadLetterResponse) -> String {
    d.last_error
        .as_ref()
        .map(|e| format!("{}/{}", e.class, e.error_type))
        .unwrap_or_else(|| "-".into())
}

pub async fn list(
    client: &ApiClient,
    function: &str,
    limit: u32,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!("/v1/functions/{id}/dead-letters?limit={limit}"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let items: Vec<DeadLetterResponse> = decode_items(&resp)?;
    let mut t = Table::new(&[
        "ID",
        "REASON",
        "STATUS",
        "INVOCATION",
        "ATTEMPTS",
        "CREATED",
        "LAST ERROR",
    ]);
    for d in &items {
        t.row(vec![
            d.id.clone(),
            d.reason.clone(),
            d.status.clone(),
            opt(&d.invocation_id),
            d.attempts.to_string(),
            ts(&d.created_at),
            ellipsize(&last_error(d), 48),
        ]);
    }
    if t.is_empty() {
        p.note("no dead letters")?;
    }
    p.table(&t)
}

pub async fn show(client: &ApiClient, id: &str, p: &mut Printer<'_>) -> Result<(), CliError> {
    let resp = client.get(&format!("/v1/dead-letters/{id}")).await?.ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let d: DeadLetterResponse = resp.json()?;
    p.kv(&[
        ("id", d.id.clone()),
        ("reason", d.reason.clone()),
        ("status", d.status.clone()),
        ("function", opt(&d.function_id)),
        ("invocation", opt(&d.invocation_id)),
        ("revision", opt(&d.revision_id)),
        ("attempts", d.attempts.to_string()),
        ("deferrals", d.deferrals.to_string()),
        ("last error", last_error(&d)),
        (
            "last error message",
            d.last_error
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_else(|| "-".into()),
        ),
        ("accepted", opt_ts(&d.accepted_at)),
        ("first attempt", opt_ts(&d.first_attempt_at)),
        ("last attempt", opt_ts(&d.last_attempt_at)),
        ("dead-lettered", ts(&d.created_at)),
        ("input", {
            match (&d.input_storage, &d.input_size_bytes, &d.input_digest) {
                (Some(s), Some(n), Some(digest)) => format!("{s}, {n} bytes, {digest}"),
                _ => "-".into(),
            }
        }),
        ("message id", opt(&d.message_id)),
        ("detail", opt(&d.detail)),
        ("redrives", d.redrive_count.to_string()),
    ])?;
    if !d.redrives.is_empty() {
        let mut t = Table::new(&[
            "REDRIVE",
            "INVOCATION",
            "REVISION",
            "OVERRIDDEN",
            "BY",
            "AT",
            "REASON",
        ]);
        for r in &d.redrives {
            t.row(vec![
                r.id.clone(),
                r.invocation_id.clone(),
                r.revision_id.clone(),
                r.revision_overridden.to_string(),
                r.requested_by.clone(),
                ts(&r.created_at),
                ellipsize(r.reason.as_deref().unwrap_or("-"), 40),
            ]);
        }
        p.table(&t)?;
    }
    Ok(())
}

pub async fn redrive(
    client: &ApiClient,
    id: &str,
    revision_id: Option<&str>,
    reason: Option<&str>,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let body = RedriveRequestBody {
        revision_id: revision_id.map(str::to_string),
        reason: reason.map(str::to_string),
    };
    let resp = client
        .post_json(&format!("/v1/dead-letters/{id}/redrive"), &body)
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let r: RedriveAcceptedResponse = resp.json()?;
    p.kv(&[
        ("redrive", r.redrive.id.clone()),
        ("dead letter", r.redrive.dead_letter_id.clone()),
        ("source invocation", r.redrive.source_invocation_id.clone()),
        ("new invocation", r.invocation.invocation_id.clone()),
        ("revision", r.invocation.revision_id.clone()),
        (
            "revision overridden",
            r.redrive.revision_overridden.to_string(),
        ),
        ("status", r.invocation.status.clone()),
        ("status url", r.invocation.status_url.clone()),
    ])
}
