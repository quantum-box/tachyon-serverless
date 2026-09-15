//! `functions logs`: print `ts [stream/phase] line` with truncation / drop markers.

use tachyon_serverless_api_types::{InvocationResponse, LogsResponse};

use crate::args::LogsArgs;
use crate::client::ApiClient;
use crate::commands::decode_items;
use crate::error::CliError;
use crate::output::{Printer, ts};
use crate::resolve::resolve_function_id;

pub fn format_logs(logs: &LogsResponse) -> String {
    let mut s = String::new();
    for e in &logs.items {
        s.push_str(&format!(
            "{} [{}/{}] {}{}\n",
            ts(&e.timestamp),
            e.stream,
            e.phase,
            e.line,
            if e.truncated { " [truncated]" } else { "" }
        ));
    }
    if logs.dropped {
        s.push_str("-- dropped: some lines were dropped because of retention limits\n");
    }
    s
}

pub async fn fetch_logs(
    client: &ApiClient,
    invocation_id: &str,
) -> Result<(LogsResponse, String), CliError> {
    let resp = client
        .get(&format!("/v1/invocations/{invocation_id}/logs"))
        .await?
        .ok()?;
    let raw = resp.body_text();
    let logs: LogsResponse = resp.json()?;
    Ok((logs, raw))
}

pub async fn print_invocation_logs(
    client: &ApiClient,
    invocation_id: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let (logs, raw) = fetch_logs(client, invocation_id).await?;
    if p.json {
        return p.raw(&raw);
    }
    let text = format_logs(&logs);
    if text.is_empty() {
        p.note(format!("no logs for {invocation_id}"))?;
    } else {
        p.raw(&text)?;
    }
    Ok(())
}

pub async fn logs(
    client: &ApiClient,
    args: &LogsArgs,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    if let Some(id) = &args.invocation {
        return print_invocation_logs(client, id, p).await;
    }
    let function = args
        .function
        .as_deref()
        .ok_or_else(|| CliError::usage("pass --invocation <id> or --function <fn>"))?;
    let function_id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!(
            "/v1/functions/{function_id}/invocations?limit={}",
            args.limit
        ))
        .await?
        .ok()?;
    let mut items: Vec<InvocationResponse> = decode_items(&resp)?;
    // Oldest first so the output reads chronologically.
    items.sort_by_key(|i| i.accepted_at);
    if items.is_empty() {
        p.note("no invocations")?;
    }
    for i in items {
        if !p.json {
            p.line(format!("== {} ({})", i.id, i.status))?;
        }
        // Under --json each invocation's LogsResponse is printed on its own line (NDJSON).
        print_invocation_logs(client, &i.id, p).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tachyon_serverless_api_types::LogEntryResponse;

    #[test]
    fn formats_lines_and_markers() {
        let t = chrono::Utc.with_ymd_and_hms(2026, 9, 15, 1, 2, 3).unwrap();
        let logs = LogsResponse {
            items: vec![
                LogEntryResponse {
                    timestamp: t,
                    stream: "stdout".into(),
                    phase: "handler".into(),
                    environment_id: "env_x".into(),
                    invocation_id: None,
                    attempt_id: None,
                    line: "hello".into(),
                    truncated: false,
                },
                LogEntryResponse {
                    timestamp: t,
                    stream: "stderr".into(),
                    phase: "init".into(),
                    environment_id: "env_x".into(),
                    invocation_id: None,
                    attempt_id: None,
                    line: "long".into(),
                    truncated: true,
                },
            ],
            dropped: true,
        };
        let s = format_logs(&logs);
        assert_eq!(
            s,
            "2026-09-15T01:02:03.000Z [stdout/handler] hello\n2026-09-15T01:02:03.000Z [stderr/init] long [truncated]\n-- dropped: some lines were dropped because of retention limits\n"
        );
    }
}
