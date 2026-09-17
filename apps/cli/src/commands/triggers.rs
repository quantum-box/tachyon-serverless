//! `triggers create|list|get|update|delete|fires|webhook-sign` (PLT-4641).

use bytes::Bytes;
use reqwest::Method;
use sha2::{Digest, Sha256};

use tachyon_serverless_api_types::{
    CreateTriggerRequest, CronTriggerRequest, MissedRunPolicyRequest, TriggerFireResponse,
    TriggerResponse, TriggerTargetRequest, UpdateTriggerRequest, WebhookTriggerRequest,
    webhook_headers,
};

use crate::args::{TriggerCreateArgs, TriggerUpdateArgs, WebhookSignArgs};
use crate::client::ApiClient;
use crate::commands::decode_items;
use crate::error::CliError;
use crate::output::{Printer, Table, opt, opt_ts, ts};
use crate::resolve::resolve_function_id;

fn policy(
    raw: Option<&str>,
    max_runs: Option<u32>,
) -> Result<Option<MissedRunPolicyRequest>, CliError> {
    Ok(match (raw, max_runs) {
        (None, None) => None,
        (Some("skip"), None) => Some(MissedRunPolicyRequest::Skip),
        (Some("run-once" | "run_once"), None) => Some(MissedRunPolicyRequest::RunOnce),
        (Some("run-all" | "run_all") | None, Some(n)) => {
            Some(MissedRunPolicyRequest::RunAll { max_runs: n })
        }
        (Some("run-all" | "run_all"), None) => {
            return Err(CliError::usage("--missed-run run-all needs --max-runs N"));
        }
        (Some(other), _) => {
            return Err(CliError::usage(format!(
                "--missed-run must be skip, run-once or run-all, not `{other}`"
            )));
        }
    })
}

fn payload(raw: Option<&str>) -> Result<Option<serde_json::Value>, CliError> {
    raw.map(|p| {
        serde_json::from_str(p).map_err(|e| CliError::usage(format!("--payload is not JSON: {e}")))
    })
    .transpose()
}

fn target(alias: Option<String>, revision_id: Option<String>) -> Option<TriggerTargetRequest> {
    (alias.is_some() || revision_id.is_some())
        .then_some(TriggerTargetRequest { alias, revision_id })
}

fn print_trigger(t: &TriggerResponse, p: &mut Printer<'_>) -> Result<(), CliError> {
    let mut rows = vec![
        ("id", t.id.clone()),
        ("function_id", t.function_id.clone()),
        ("name", t.name.clone()),
        ("kind", t.kind.clone()),
        ("status", t.status.clone()),
        ("status_reason", opt(&t.status_reason)),
        ("generation", t.generation.to_string()),
        ("alias", opt(&t.target.alias)),
        ("revision_id", opt(&t.target.revision_id)),
    ];
    if let Some(c) = &t.cron {
        rows.push(("expression", c.expression.clone()));
        rows.push(("timezone", c.timezone.clone()));
        rows.push((
            "missed_run_policy",
            serde_json::to_string(&c.missed_run_policy).unwrap_or_default(),
        ));
        rows.push(("next_fire_at", opt_ts(&c.next_fire_at)));
        rows.push(("last_scheduled_at", opt_ts(&c.last_scheduled_at)));
    }
    if let Some(w) = &t.webhook {
        rows.push(("url", w.url.clone()));
        rows.push(("tolerance_seconds", w.tolerance_seconds.to_string()));
        rows.push(("max_body_bytes", w.max_body_bytes.to_string()));
        rows.push(("event_id_header", w.event_id_header.clone()));
        rows.push(("secret_fingerprint", w.secret_fingerprint.clone()));
    }
    rows.push(("updated_at", ts(&t.updated_at)));
    p.kv(&rows)?;
    if let Some(secret) = &t.secret {
        // The only time the secret is shown: stdout, so it can be captured.
        p.line(format!("secret {secret}"))?;
        p.note("store this secret now: it is never shown again (rotate with `triggers update --rotate-secret`)")?;
    }
    Ok(())
}

pub async fn create(
    client: &ApiClient,
    args: &TriggerCreateArgs,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, &args.function).await?;
    let req = match args.kind.as_str() {
        "cron" => CreateTriggerRequest {
            name: args.name.clone(),
            kind: "cron".into(),
            enabled: !args.disabled,
            target: target(args.alias.clone(), args.revision_id.clone()).unwrap_or_default(),
            cron: Some(CronTriggerRequest {
                expression: args
                    .schedule
                    .clone()
                    .ok_or_else(|| CliError::usage("a cron trigger needs --schedule"))?,
                timezone: args.timezone.clone().unwrap_or_else(|| "UTC".into()),
                payload: payload(args.payload.as_deref())?.unwrap_or(serde_json::Value::Null),
                missed_run_policy: policy(args.missed_run.as_deref(), args.max_runs)?
                    .unwrap_or(MissedRunPolicyRequest::Skip),
            }),
            webhook: None,
        },
        "webhook" => CreateTriggerRequest {
            name: args.name.clone(),
            kind: "webhook".into(),
            enabled: !args.disabled,
            target: target(args.alias.clone(), args.revision_id.clone()).unwrap_or_default(),
            cron: None,
            webhook: Some(WebhookTriggerRequest {
                source: None,
                tolerance_seconds: args.tolerance_seconds,
                max_body_bytes: args.max_body_bytes,
                event_id_header: args.event_id_header.clone(),
            }),
        },
        other => {
            return Err(CliError::usage(format!(
                "--kind must be cron or webhook, not `{other}`"
            )));
        }
    };
    let resp = client
        .post_json(&format!("/v1/functions/{id}/triggers"), &req)
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    print_trigger(&resp.json()?, p)
}

pub async fn list(client: &ApiClient, function: &str, p: &mut Printer<'_>) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!("/v1/functions/{id}/triggers"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let items: Vec<TriggerResponse> = decode_items(&resp)?;
    let mut t = Table::new(&[
        "ID",
        "NAME",
        "KIND",
        "STATUS",
        "GEN",
        "SCHEDULE / URL",
        "NEXT FIRE",
    ]);
    for tr in &items {
        let (what, next) = match (&tr.cron, &tr.webhook) {
            (Some(c), _) => (
                format!("{} ({})", c.expression, c.timezone),
                opt_ts(&c.next_fire_at),
            ),
            (_, Some(w)) => (w.url.clone(), "-".into()),
            _ => ("-".into(), "-".into()),
        };
        t.row(vec![
            tr.id.clone(),
            tr.name.clone(),
            tr.kind.clone(),
            tr.status.clone(),
            tr.generation.to_string(),
            what,
            next,
        ]);
    }
    if t.is_empty() {
        p.note("no triggers")?;
    }
    p.table(&t)
}

pub async fn get(
    client: &ApiClient,
    function: &str,
    trigger: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!("/v1/functions/{id}/triggers/{trigger}"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    print_trigger(&resp.json()?, p)
}

pub async fn update(
    client: &ApiClient,
    args: &TriggerUpdateArgs,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, &args.function).await?;
    let enabled = match (args.enable, args.disable) {
        (true, true) => return Err(CliError::usage("--enable and --disable are exclusive")),
        (true, false) => Some(true),
        (false, true) => Some(false),
        _ => None,
    };
    let req = UpdateTriggerRequest {
        expected_generation: args.expected_generation,
        name: args.name.clone(),
        enabled,
        target: target(args.alias.clone(), args.revision_id.clone()),
        expression: args.schedule.clone(),
        timezone: args.timezone.clone(),
        payload: payload(args.payload.as_deref())?,
        missed_run_policy: policy(args.missed_run.as_deref(), args.max_runs)?,
        tolerance_seconds: args.tolerance_seconds,
        max_body_bytes: args.max_body_bytes,
        event_id_header: args.event_id_header.clone(),
        rotate_secret: args.rotate_secret.then_some(true),
    };
    let bytes = Bytes::from(serde_json::to_vec(&req)?);
    let resp = client
        .send(
            Method::PATCH,
            &format!("/v1/functions/{id}/triggers/{}", args.trigger),
            true,
            Some("application/json"),
            &[],
            Some(bytes),
        )
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    print_trigger(&resp.json()?, p)
}

pub async fn delete(
    client: &ApiClient,
    function: &str,
    trigger: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .delete(&format!("/v1/functions/{id}/triggers/{trigger}"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    p.line(format!("deleted {trigger}"))
}

pub async fn fires(
    client: &ApiClient,
    function: &str,
    trigger: &str,
    limit: u32,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!(
            "/v1/functions/{id}/triggers/{trigger}/fires?limit={limit}"
        ))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let items: Vec<TriggerFireResponse> = decode_items(&resp)?;
    let mut t = Table::new(&["FIRE KEY", "OUTCOME", "INVOCATION", "REASON", "CREATED"]);
    for f in &items {
        t.row(vec![
            f.fire_key.clone(),
            f.outcome.clone(),
            opt(&f.invocation_id),
            opt(&f.reason),
            ts(&f.created_at),
        ]);
    }
    if t.is_empty() {
        p.note("no fires")?;
    }
    p.table(&t)
}

/// HMAC-SHA256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(message)
        .finalize();
    Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize()
        .into()
}

/// The `x-tachyon-webhook-signature` value for `timestamp` and `body`.
pub fn signature(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut message = format!("{timestamp}.").into_bytes();
    message.extend_from_slice(body);
    format!(
        "v1={}",
        hex::encode(hmac_sha256(secret.as_bytes(), &message))
    )
}

/// Print the headers of a signed delivery (for testing a webhook trigger with
/// curl). Offline: nothing is sent.
pub fn webhook_sign(args: &WebhookSignArgs, p: &mut Printer<'_>) -> Result<(), CliError> {
    let secret = match (&args.secret, &args.secret_env) {
        (Some(s), None) => s.clone(),
        (None, Some(var)) => std::env::var(var)
            .map_err(|_| CliError::usage(format!("environment variable {var} is not set")))?,
        _ => {
            return Err(CliError::usage(
                "pass exactly one of --secret / --secret-env",
            ));
        }
    };
    let body = match (&args.body, &args.body_file) {
        (Some(b), None) => b.clone().into_bytes(),
        (None, Some(path)) => std::fs::read(path)
            .map_err(|e| CliError::usage(format!("cannot read {}: {e}", path.display())))?,
        (None, None) => Vec::new(),
        _ => return Err(CliError::usage("pass at most one of --body / --body-file")),
    };
    let timestamp = args
        .timestamp
        .unwrap_or_else(|| chrono::Utc::now().timestamp());
    let sig = signature(&secret, timestamp, &body);
    if p.json {
        let v = serde_json::json!({
            "timestamp": timestamp,
            "signature": sig,
            "headers": {
                webhook_headers::TIMESTAMP: timestamp.to_string(),
                webhook_headers::SIGNATURE: sig,
            }
        });
        return p.raw(&v.to_string());
    }
    p.line(format!("{}: {timestamp}", webhook_headers::TIMESTAMP))?;
    p.line(format!("{}: {sig}", webhook_headers::SIGNATURE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_the_gateway_fixture() {
        // RFC 4231 case 2, and the same message layout the gateway verifies.
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let s = signature("whsec_test", 1_789_000_000, br#"{"a":1}"#);
        assert_eq!(
            s,
            format!(
                "v1={}",
                hex::encode(hmac_sha256(b"whsec_test", b"1789000000.{\"a\":1}"))
            )
        );
    }

    #[test]
    fn missed_run_flags_map_to_policies() {
        assert_eq!(policy(None, None).unwrap(), None);
        assert_eq!(
            policy(Some("run-all"), Some(3)).unwrap(),
            Some(MissedRunPolicyRequest::RunAll { max_runs: 3 })
        );
        assert_eq!(
            policy(Some("run-once"), None).unwrap(),
            Some(MissedRunPolicyRequest::RunOnce)
        );
        assert!(policy(Some("run-all"), None).is_err());
        assert!(policy(Some("later"), None).is_err());
    }
}
