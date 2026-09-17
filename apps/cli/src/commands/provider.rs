//! `provider` (capability table) and `health` (/healthz, /readyz).

use tachyon_serverless_api_types::{CapacityInfo, ProviderInfo, ResourceAmounts, ReuseInfo};

use crate::client::ApiClient;
use crate::error::{CliError, ExitCode};
use crate::output::{Printer, Table};

pub const NO_ISOLATION_BANNER: &str = "!! WARNING: this provider has NO isolation (dev_only). Functions run as plain host processes. Never use it outside development. !!";

/// Best-effort isolation warning for commands that show a function's result
/// (`functions invoke`, `functions http`; ADR-0002 decision 4). Looks up the
/// authenticated `GET /v1/provider` and prints [`NO_ISOLATION_BANNER`] on
/// stderr when the provider is `dev_only`. When the lookup fails it prints a
/// warning instead. It never touches stdout and never changes the exit code.
pub async fn warn_if_dev_only(client: &ApiClient, p: &mut Printer<'_>) {
    let note = match provider_info(client).await {
        Ok(info) if info.dev_only => NO_ISOLATION_BANNER.to_string(),
        Ok(_) => return,
        Err(e) => {
            let e = e.to_string();
            format!(
                "warning: could not determine provider isolation (GET /v1/provider: {})",
                e.strip_prefix("error: ").unwrap_or(&e)
            )
        }
    };
    // A failed stderr write must not turn the command's outcome into an error.
    let _ = p.note(note);
}

async fn provider_info(client: &ApiClient) -> Result<ProviderInfo, CliError> {
    client.get("/v1/provider").await?.ok()?.json()
}

/// Render the `capabilities` object as rows `(name, status, note)`.
pub fn capability_rows(caps: &serde_json::Value) -> Vec<(String, String, String)> {
    let mut rows = Vec::new();
    let Some(map) = caps.as_object() else {
        return rows;
    };
    for (k, v) in map {
        if k == "isolation" || k == "dev_only" {
            continue;
        }
        match v {
            serde_json::Value::Object(o) => {
                let status = o
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("?")
                    .to_string();
                let note = o
                    .get("reason")
                    .or_else(|| o.get("note"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();
                rows.push((k.clone(), status, note));
            }
            other => rows.push((k.clone(), other.to_string(), String::new())),
        }
    }
    rows
}

/// One line an operator can act on: whether this gateway reuses environments,
/// whether the provider's idle support was ever measured, and why.
///
/// An enabled but unverified configuration is spelled out as a measurement
/// run, because it must never read as a warm success (PLT-4633 acceptance 4).
/// `verified` is a fact about the provider, not about the gate, so it is shown
/// on the "off" line too instead of being silently folded into it (review F7).
pub fn reuse_summary(reuse: &ReuseInfo) -> String {
    let state = match (reuse.enabled, reuse.verified) {
        (true, true) => "on (verified)",
        (true, false) => "on (UNVERIFIED: measurement only)",
        (false, true) => "off (the provider's idle support is measured)",
        (false, false) => "off",
    };
    format!("{state}; {}", reuse.reason)
}

pub async fn provider(client: &ApiClient, p: &mut Printer<'_>) -> Result<(), CliError> {
    let resp = client.get_unauth("/v1/provider").await?.ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let info: ProviderInfo = resp.json()?;
    if info.dev_only {
        p.note(NO_ISOLATION_BANNER)?;
    }
    p.kv(&[
        ("kind", info.kind.clone()),
        ("isolation", info.isolation.clone()),
        ("dev_only", info.dev_only.to_string()),
        ("environment_reuse", reuse_summary(&info.reuse)),
    ])?;
    let mut t = Table::new(&["CAPABILITY", "STATUS", "NOTE"]);
    for (name, status, note) in capability_rows(&info.capabilities) {
        t.row(vec![name, status, note]);
    }
    p.line("capabilities:")?;
    p.table(&t)?;
    let ok = info
        .preflight
        .get("ok")
        .and_then(|v| v.as_bool())
        .map(|b| b.to_string())
        .unwrap_or_else(|| "?".into());
    p.line(format!("preflight: ok={ok}"))?;
    if let Some(checks) = info.preflight.get("checks").and_then(|c| c.as_array()) {
        let mut t = Table::new(&["CHECK", "OK", "DETAIL"]);
        for c in checks {
            t.row(vec![
                c.get("name").and_then(|v| v.as_str()).unwrap_or("?").into(),
                c.get("ok")
                    .and_then(|v| v.as_bool())
                    .map(|b| b.to_string())
                    .unwrap_or_else(|| "?".into()),
                c.get("detail")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .into(),
            ]);
        }
        p.table(&t)?;
    }
    Ok(())
}

/// `cpu / memory / storage`, with `-` for a dimension that is not bounded.
pub fn amounts(r: &ResourceAmounts) -> String {
    let v = |x: Option<u64>| x.map_or("-".to_string(), |n| n.to_string());
    format!(
        "cpu {} m / mem {} MiB / disk {} MiB",
        v(r.cpu_millis),
        v(r.memory_mib),
        v(r.ephemeral_storage_mib)
    )
}

/// `GET /v1/capacity` (PLT-4634): the host, what is reserved on it, the queue
/// and this tenant's revisions.
pub async fn capacity(client: &ApiClient, p: &mut Printer<'_>) -> Result<(), CliError> {
    let resp = client.get("/v1/capacity").await?.ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let info: CapacityInfo = resp.json()?;
    let e = &info.environments;
    p.kv(&[
        (
            "node",
            format!(
                "{} (region {}, hosts {}, host scale-out {})",
                info.node.name,
                info.node.region.as_deref().unwrap_or("none"),
                info.node.hosts,
                info.node.host_scale_out
            ),
        ),
        ("capacity", amounts(&info.node.capacity)),
        ("overhead/env", amounts(&info.node.per_environment_overhead)),
        ("reserved", amounts(&info.reserved)),
        (
            "environments",
            format!(
                "starting {} / busy {} / promised {} / parking {} / idle {} / draining {}",
                e.starting, e.busy, e.promised, e.parking, e.idle, e.draining
            ),
        ),
        (
            "in_flight",
            format!("{} of {}", info.in_flight, info.node.max_concurrency),
        ),
        (
            "queue",
            format!(
                "{} of {} ({} of {} bytes), oldest {} ms, timeout {} s",
                info.queue.length,
                info.queue.max_length,
                info.queue.bytes,
                info.queue.max_bytes,
                info.queue.oldest_age_ms.unwrap_or(0),
                info.queue.timeout_seconds
            ),
        ),
        (
            "start_rate",
            format!(
                "{}/s, burst {}, tokens {}",
                info.start_rate.per_second, info.start_rate.burst, info.start_rate.tokens
            ),
        ),
    ])?;
    let mut t = Table::new(&[
        "REVISION", "DESIRED", "MAX", "STARTING", "BUSY", "IDLE", "QUEUED", "RATE/S", "BREAKER",
    ]);
    for r in &info.revisions {
        t.row(vec![
            r.revision_id.clone(),
            r.desired.to_string(),
            r.max_environments.to_string(),
            r.environments.starting.to_string(),
            r.environments.busy.to_string(),
            r.environments.idle.to_string(),
            r.queued.to_string(),
            format!("{:.2}", r.arrival_rate_per_second),
            r.circuit_breaker.clone(),
        ]);
    }
    p.table(&t)?;
    Ok(())
}

pub async fn health(client: &ApiClient, p: &mut Printer<'_>) -> Result<(), CliError> {
    let healthz = client.get_unauth("/healthz").await?;
    let readyz = client.get_unauth("/readyz").await?;
    let body_value = |s: String| -> serde_json::Value {
        serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s))
    };
    if p.json {
        let v = serde_json::json!({
            "healthz": {"status": healthz.status, "body": body_value(healthz.body_text())},
            "readyz": {"status": readyz.status, "body": body_value(readyz.body_text())},
        });
        p.raw(&v.to_string())?;
    } else {
        p.line(format!(
            "healthz: {} {}",
            healthz.status,
            healthz.body_text().trim()
        ))?;
        p.line(format!(
            "readyz:  {} {}",
            readyz.status,
            readyz.body_text().trim()
        ))?;
    }
    if !healthz.is_success() || !readyz.is_success() {
        return Err(CliError::failed(
            ExitCode::Platform,
            format!(
                "gateway not healthy (healthz={}, readyz={})",
                healthz.status, readyz.status
            ),
            None,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PLT-4633 acceptance 4: an unverified configuration is never displayed
    /// as a warm success, and the reason is always shown.
    #[test]
    fn reuse_is_summarised_without_claiming_an_unverified_setup_works() {
        let base = ReuseInfo {
            enabled: true,
            verified: true,
            reason: "both idle capabilities are supported".into(),
            idle_quiesce: "supported".into(),
            idle_resume: "supported".into(),
        };
        assert_eq!(
            reuse_summary(&base),
            "on (verified); both idle capabilities are supported"
        );

        let measuring = ReuseInfo {
            verified: false,
            reason: "allow_unverified_idle: a measurement run".into(),
            idle_quiesce: "unverified".into(),
            idle_resume: "unverified".into(),
            ..base.clone()
        };
        let line = reuse_summary(&measuring);
        assert!(line.contains("UNVERIFIED"), "{line}");
        assert!(line.contains("measurement"), "{line}");

        let off = ReuseInfo {
            enabled: false,
            verified: false,
            reason: "[pool] enabled is false".into(),
            ..base.clone()
        };
        assert_eq!(reuse_summary(&off), "off; [pool] enabled is false");

        // Reuse off on a provider whose idle support *was* measured: the two
        // facts are separate, and the line says both rather than implying that
        // nothing was measured (review F7).
        let measured_but_off = ReuseInfo {
            enabled: false,
            verified: true,
            reason: "[pool] enabled is false".into(),
            ..base
        };
        let line = reuse_summary(&measured_but_off);
        assert!(line.starts_with("off ("), "{line}");
        assert!(line.contains("measured"), "{line}");
        assert!(!line.contains("UNVERIFIED"), "{line}");
        assert_eq!(
            reuse_summary(&ReuseInfo::default()),
            "off; environment reuse is off"
        );
    }

    #[test]
    fn capability_rows_flatten_support_objects() {
        let caps = serde_json::json!({
            "isolation": "process",
            "dev_only": true,
            "create_terminate": {"status": "supported"},
            "snapshot_create": {"status": "unsupported", "reason": "P1"},
            "egress_none": {"status": "unverified", "note": "no tap"},
        });
        let rows = capability_rows(&caps);
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&("create_terminate".into(), "supported".into(), String::new())));
        assert!(rows.contains(&("snapshot_create".into(), "unsupported".into(), "P1".into())));
        assert!(rows.contains(&("egress_none".into(), "unverified".into(), "no tap".into())));
    }
}
