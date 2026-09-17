//! `usage` (PLT-4642): the tenant's provisional usage report from
//! `GET /v1/usage`. Not an invoice; the header line says so every time.

use tachyon_serverless_api_types::UsageReportResponse;

use crate::args::UsageArgs;
use crate::client::ApiClient;
use crate::error::CliError;
use crate::output::{Printer, Table};
use crate::resolve::{resolve_function_id, urlencode};

/// The query string of `GET /v1/usage` for `args` (function already resolved).
pub fn usage_path(args: &UsageArgs, function_id: Option<&str>) -> String {
    let mut params = Vec::new();
    if let Some(from) = &args.from {
        params.push(format!("from={}", urlencode(from)));
    }
    if let Some(to) = &args.to {
        params.push(format!("to={}", urlencode(to)));
    }
    if let Some(group_by) = &args.group_by {
        params.push(format!("group_by={}", urlencode(group_by)));
    }
    if let Some(f) = function_id {
        params.push(format!("function_id={}", urlencode(f)));
    }
    match params.is_empty() {
        true => "/v1/usage".to_string(),
        false => format!("/v1/usage?{}", params.join("&")),
    }
}

/// Micro-units as a decimal amount (`1234567` → `1.234567`).
pub fn micros(v: u64) -> String {
    format!("{}.{:06}", v / 1_000_000, v % 1_000_000)
}

pub async fn usage(
    client: &ApiClient,
    args: &UsageArgs,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let function_id = match &args.function {
        Some(f) => Some(resolve_function_id(client, f).await?),
        None => None,
    };
    let resp = client
        .get(&usage_path(args, function_id.as_deref()))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let r: UsageReportResponse = resp.json()?;
    p.line(format!("PROVISIONAL - {}", r.notice))?;
    let t = &r.price_table;
    p.kv(&[
        ("tenant", r.tenant_id.clone()),
        (
            "range",
            format!("{} .. {}", r.from.to_rfc3339(), r.to.to_rfc3339()),
        ),
        (
            "price table",
            format!(
                "{} (effective {}, {}; billable: {})",
                t.version,
                t.effective_from.to_rfc3339(),
                t.currency,
                t.billable_segments.join(", ")
            ),
        ),
        (
            "collected through",
            r.collected_through
                .map_or_else(|| "never".to_string(), |c| c.to_rfc3339()),
        ),
        ("unjournaled events", r.unjournaled_events.to_string()),
    ])?;
    let mut table = Table::new(&[
        "FUNCTION",
        "DAY",
        "INVOCATIONS",
        "ATTEMPTS",
        "RETRIES",
        "TIMEOUTS",
        "HANDLER MS",
        "BILLABLE MS",
        "UNMETERED",
        "CHARGE (PROVISIONAL)",
    ]);
    let mut rows = r.lines.clone();
    let mut total = r.totals.clone();
    total.function_id = Some("TOTAL".into());
    rows.push(total);
    for l in &rows {
        table.row(vec![
            l.function_id.clone().unwrap_or_else(|| "-".into()),
            l.day.clone().unwrap_or_else(|| "-".into()),
            l.usage.invocations.to_string(),
            l.usage.attempts.to_string(),
            l.usage.retries.to_string(),
            l.usage.outcomes.timeout.to_string(),
            l.usage.segments_ms.handler_ms.to_string(),
            l.usage.billable_ms.to_string(),
            l.unmetered.attempts.to_string(),
            format!(
                "{} {}",
                micros(l.provisional_charges_micros.total),
                t.currency
            ),
        ]);
    }
    p.table(&table)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_query_string_carries_only_what_was_given() {
        let args = UsageArgs {
            from: Some("2026-09-17".into()),
            to: None,
            group_by: Some("function,day".into()),
            function: None,
        };
        assert_eq!(
            usage_path(&args, Some("fn_01hzzzzzzzzzzzzzzzzzzzzzzz")),
            "/v1/usage?from=2026-09-17&group_by=function%2Cday&function_id=fn_01hzzzzzzzzzzzzzzzzzzzzzzz"
        );
        let none = UsageArgs {
            from: None,
            to: None,
            group_by: None,
            function: None,
        };
        assert_eq!(usage_path(&none, None), "/v1/usage");
    }

    #[test]
    fn micro_units_render_as_decimals() {
        assert_eq!(micros(0), "0.000000");
        assert_eq!(micros(1_234_567), "1.234567");
        assert_eq!(micros(30), "0.000030");
    }
}
