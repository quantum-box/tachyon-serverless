//! `budget` (PLT-4643): the tenant's budget from `GET /v1/budget`. The
//! amounts are the provisional rating of PLT-4642; the header line says so
//! every time.

use tachyon_serverless_api_types::{BudgetReportResponse, BudgetScopeReport};

use crate::args::BudgetArgs;
use crate::client::ApiClient;
use crate::commands::usage::micros;
use crate::error::CliError;
use crate::output::{Printer, Table};
use crate::resolve::urlencode;

/// The path of `GET /v1/budget` for `args`.
pub fn budget_path(args: &BudgetArgs) -> String {
    match &args.period {
        Some(p) => format!("/v1/budget?period={}", urlencode(p)),
        None => "/v1/budget".to_string(),
    }
}

fn opt(v: Option<u64>) -> String {
    v.map_or_else(|| "-".to_string(), micros)
}

fn row(name: String, s: &BudgetScopeReport) -> Vec<String> {
    let alerts = s
        .alerts_fired
        .iter()
        .map(|a| format!("{}%", a.threshold_percent))
        .collect::<Vec<_>>();
    vec![
        name,
        opt(s.hard_limit_micros),
        micros(s.reserved_micros),
        micros(s.settled_micros),
        micros(s.unmetered_hold_micros),
        opt(s.remaining_micros),
        s.active_reservations.to_string(),
        s.refusals.to_string(),
        match (s.soft_limit_micros, alerts.is_empty()) {
            (None, _) => "-".to_string(),
            (Some(soft), true) => format!("none (soft {})", micros(soft)),
            (Some(soft), false) => format!("{} (soft {})", alerts.join(","), micros(soft)),
        },
    ]
}

pub async fn budget(
    client: &ApiClient,
    args: &BudgetArgs,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let resp = client.get(&budget_path(args)).await?.ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let r: BudgetReportResponse = resp.json()?;
    p.line(format!("PROVISIONAL - {}", r.notice))?;
    p.kv(&[
        ("tenant", r.tenant_id.clone()),
        ("enforced", r.enabled.to_string()),
        (
            "period",
            format!(
                "{} ({} .. {}, {})",
                r.period,
                r.period_start.to_rfc3339(),
                r.period_end.to_rfc3339(),
                r.period_kind
            ),
        ),
        (
            "price table",
            format!("{} ({})", r.price_table_version, r.currency),
        ),
        (
            "configuration",
            match r.config_generation {
                Some(g) => format!("{} (generation {g})", r.config_state),
                None => r.config_state.clone(),
            },
        ),
        (
            "admitting",
            match &r.refusal {
                Some(why) => format!("{} ({why})", r.admitting),
                None => r.admitting.to_string(),
            },
        ),
    ])?;
    let mut table = Table::new(&[
        "SCOPE",
        "HARD LIMIT",
        "RESERVED",
        "SETTLED",
        "UNMETERED HOLD",
        "REMAINING",
        "ACTIVE",
        "REFUSALS",
        "ALERTS",
    ]);
    table.row(row("tenant".into(), &r.tenant));
    for f in &r.functions {
        table.row(row(f.function_id.clone().unwrap_or_default(), f));
    }
    p.table(&table)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_period_is_passed_only_when_given() {
        assert_eq!(budget_path(&BudgetArgs { period: None }), "/v1/budget");
        assert_eq!(
            budget_path(&BudgetArgs {
                period: Some("2026-09".into())
            }),
            "/v1/budget?period=2026-09"
        );
    }
}
