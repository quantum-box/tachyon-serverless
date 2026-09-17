//! Scenario report: machine-readable summary, detector findings and a
//! timeline (SVG and ASCII) of environments by state and queue length.
//!
//! Every number is an observation of one run on one machine, never an SLA.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use anyhow::Result;
use serde_json::{Value, json};

use crate::detect::{Snapshot, Thresholds, detect};
use crate::limits::LoadLimits;
use crate::prom::{Flat, value};
use crate::run::read_jsonl;

/// One point of the timeline.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Point {
    pub t_ms: u64,
    pub up: bool,
    pub starting: f64,
    pub busy: f64,
    pub idle: f64,
    pub parking: f64,
    pub promised: f64,
    pub draining: f64,
    pub in_flight: f64,
    pub queue: f64,
}

impl Point {
    pub fn provisioned(&self) -> f64 {
        self.starting + self.busy + self.idle + self.parking + self.promised + self.draining
    }
}

pub fn point(t_ms: u64, up: bool, m: &Flat) -> Point {
    let st = |s: &str| value(m, "tsls_environments", &[("state", s)]).unwrap_or(0.0);
    Point {
        t_ms,
        up,
        starting: st("starting"),
        busy: st("busy"),
        idle: st("idle"),
        parking: st("parking"),
        promised: st("promised"),
        draining: st("draining"),
        in_flight: value(m, "tsls_node_in_flight", &[]).unwrap_or(0.0),
        queue: value(m, "tsls_queue_length", &[]).unwrap_or(0.0),
    }
}

/// What the timeline shows, in order: 0 -> up -> cap -> down -> 0 -> restart.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Lifecycle {
    pub zero_before: bool,
    pub rose: bool,
    pub max_in_flight: f64,
    pub max_provisioned: f64,
    pub max_queue: f64,
    /// `max_in_flight` reached the expected cap (when one was given).
    pub reached_cap: Option<bool>,
    pub decreased: bool,
    pub zero_after_peak: bool,
    pub zero_after_peak_t_ms: Option<u64>,
    /// Down gaps (a gateway restart): start and end t_ms.
    pub restarts: Vec<(u64, u64)>,
    /// After the last restart the node rose from zero again.
    pub active_after_restart: bool,
    pub zero_after_restart: bool,
}

pub fn lifecycle(points: &[Point], cap: Option<f64>) -> Lifecycle {
    let mut l = Lifecycle::default();
    let up: Vec<&Point> = points.iter().filter(|p| p.up).collect();
    l.zero_before = up.first().is_some_and(|p| p.provisioned() == 0.0);
    l.rose = up.iter().any(|p| p.provisioned() > 0.0);
    l.max_in_flight = up.iter().map(|p| p.in_flight).fold(0.0, f64::max);
    l.max_provisioned = up.iter().map(|p| p.provisioned()).fold(0.0, f64::max);
    l.max_queue = up.iter().map(|p| p.queue).fold(0.0, f64::max);
    l.reached_cap = cap.map(|c| l.max_in_flight >= c);
    if let Some(peak) = up
        .iter()
        .position(|p| p.in_flight == l.max_in_flight && l.rose)
    {
        let after = &up[peak..];
        l.decreased = after
            .iter()
            .any(|p| p.in_flight > 0.0 && p.in_flight < l.max_in_flight);
        if let Some(z) = after
            .iter()
            .find(|p| p.provisioned() == 0.0 && p.queue == 0.0)
        {
            l.zero_after_peak = true;
            l.zero_after_peak_t_ms = Some(z.t_ms);
            // A decrease straight to zero between two samples still decreased.
            l.decreased |= l.max_in_flight > 0.0;
        }
    }
    let mut gap_start = None;
    for (i, p) in points.iter().enumerate() {
        match (p.up, gap_start) {
            (false, None) if i > 0 => gap_start = Some(p.t_ms),
            (true, Some(s)) => {
                l.restarts.push((s, p.t_ms));
                gap_start = None;
            }
            _ => {}
        }
    }
    if let Some((_, back)) = l.restarts.last() {
        let after: Vec<&&Point> = up.iter().filter(|p| p.t_ms >= *back).collect();
        if let Some(first_active) = after.iter().position(|p| p.provisioned() > 0.0) {
            l.active_after_restart = true;
            l.zero_after_restart = after[first_active..]
                .iter()
                .any(|p| p.provisioned() == 0.0 && p.queue == 0.0);
        }
    }
    l
}

fn percentile(sorted: &[u64], q: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted.get(idx).copied()
}

/// Highest number of requests in flight at once, from their send/done times.
pub fn max_overlap(requests: &[Value]) -> u64 {
    let mut events: Vec<(u64, i64)> = Vec::new();
    for r in requests {
        if let (Some(s), Some(d)) = (r["sent_t_ms"].as_u64(), r["done_t_ms"].as_u64()) {
            events.push((s, 1));
            events.push((d, -1));
        }
    }
    // Ends before starts at the same millisecond.
    events.sort();
    let (mut cur, mut max) = (0i64, 0i64);
    for (_, delta) in events {
        cur += delta;
        max = max.max(cur);
    }
    max.max(0) as u64
}

fn latency_stats(rs: &[&Value]) -> Value {
    let mut lat: Vec<u64> = rs.iter().filter_map(|r| r["latency_ms"].as_u64()).collect();
    lat.sort_unstable();
    let mut statuses: BTreeMap<String, u64> = BTreeMap::new();
    for r in rs {
        *statuses.entry(r["status"].to_string()).or_default() += 1;
    }
    json!({
        "count": rs.len(), "statuses": statuses,
        "latency_ms": {"p50": percentile(&lat, 0.5), "p95": percentile(&lat, 0.95), "max": lat.last()},
    })
}

/// Reuse evidence from the invocations read back: environments per attempt,
/// boot ids per environment.
pub fn reuse(invocations: &[Value]) -> Value {
    let mut per_env: BTreeMap<String, (u64, BTreeSet<String>)> = BTreeMap::new();
    let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
    let mut attempts = 0u64;
    for inv in invocations {
        for a in inv["attempts"].as_array().into_iter().flatten() {
            attempts += 1;
            *kinds
                .entry(a["start_kind"].as_str().unwrap_or("?").to_string())
                .or_default() += 1;
            let env = a["environment_id"].as_str().unwrap_or_default().to_string();
            let entry = per_env.entry(env).or_default();
            entry.0 += 1;
            if let Some(b) = a["guest_boot_id"].as_str() {
                entry.1.insert(b.to_string());
            }
        }
    }
    let reused = per_env.values().filter(|(n, _)| *n > 1).count();
    let changed = per_env.values().filter(|(_, b)| b.len() > 1).count();
    let reported = per_env.values().filter(|(_, b)| !b.is_empty()).count();
    json!({
        "attempts": attempts,
        "environments": per_env.len(),
        "start_kinds": kinds,
        "environments_with_several_attempts": reused,
        "environments_with_a_boot_id": reported,
        "environments_whose_boot_id_changed": changed,
        "every_attempt_booted_its_own_environment": attempts > 0 && per_env.len() as u64 == attempts,
    })
}

/// Everything `report` produces.
pub struct Outputs {
    pub summary: Value,
    pub svg: String,
    pub ascii: String,
}

pub struct ReportInput<'a> {
    pub dir: &'a Path,
    pub limits: LoadLimits,
    pub thresholds: Thresholds,
    pub cap: Option<f64>,
    pub expect: Vec<String>,
    pub title: String,
}

pub fn build(input: &ReportInput) -> Result<Outputs> {
    let dir = input.dir;
    let samples = read_jsonl(&dir.join("samples.jsonl"));
    let requests = read_jsonl(&dir.join("requests.jsonl"));
    let invocations = read_jsonl(&dir.join("invocations.jsonl"));
    let phases = read_jsonl(&dir.join("phases.jsonl"));
    let run_meta: Value = std::fs::read_to_string(dir.join("run.json"))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Null);

    let snaps: Vec<Snapshot> = samples
        .iter()
        .map(|s| Snapshot {
            t_ms: s["t_ms"].as_u64().unwrap_or(0),
            up: s["up"].as_bool().unwrap_or(false),
            metrics: serde_json::from_value(s["metrics"].clone()).unwrap_or_default(),
        })
        .collect();
    let points: Vec<Point> = snaps
        .iter()
        .map(|s| point(s.t_ms, s.up, &s.metrics))
        .collect();
    let life = lifecycle(&points, input.cap);
    let findings = detect(&snaps, &input.thresholds);

    let all: Vec<&Value> = requests.iter().collect();
    let mut by_phase: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    let mut by_tenant: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for r in &requests {
        by_phase
            .entry(r["phase"].as_str().unwrap_or("?").to_string())
            .or_default()
            .push(r);
        by_tenant
            .entry(r["tenant"].as_str().unwrap_or("?").to_string())
            .or_default()
            .push(r);
    }
    let last_done = requests
        .iter()
        .filter_map(|r| r["done_t_ms"].as_u64())
        .max()
        .unwrap_or(0);
    let overlap = max_overlap(&requests);
    let limits_respected = overlap <= u64::from(input.limits.max_concurrency)
        && requests.len() as u64 <= input.limits.max_requests
        && last_done <= input.limits.max_duration_seconds * 1000;
    let succeeded = requests.iter().filter(|r| r["status"] == 200).count();
    let reuse_mode = samples
        .iter()
        .rev()
        .find_map(|s| s["capacity"]["reuse"]["mode"].as_str().map(str::to_string));
    let reuse_ev = reuse(&invocations);

    // Expectations named by the scenario.
    let mut checks = BTreeMap::new();
    for e in &input.expect {
        let ok = match e.as_str() {
            "zero_before" => life.zero_before,
            "rose" => life.rose,
            "cap" => life.reached_cap.unwrap_or(false),
            "queue" => life.max_queue > 0.0,
            "decreased" => life.decreased,
            "zero_after" => life.zero_after_peak,
            "restart" => !life.restarts.is_empty(),
            "active_after_restart" => life.active_after_restart,
            "zero_after_restart" => life.zero_after_restart,
            "all_succeeded" => !requests.is_empty() && succeeded == requests.len(),
            "no_findings" => findings.findings() == 0,
            "two_tenants_served" => findings.coverage.tenants_served >= 2,
            "every_invocation_boots" => {
                reuse_mode.as_deref() == Some("every_invocation_boots")
                    && reuse_ev["every_attempt_booted_its_own_environment"] == true
            }
            "warm_reuse" => {
                reuse_mode.as_deref() == Some("warm_reuse")
                    && reuse_ev["environments_whose_boot_id_changed"] == 0
                    && reuse_ev["environments_with_several_attempts"].as_u64() > Some(0)
            }
            other => {
                if let Some((tenant, ms)) = other
                    .strip_prefix("p95_below_ms:")
                    .and_then(|r| r.split_once('='))
                {
                    let limit: u64 = ms.parse().unwrap_or(0);
                    by_tenant
                        .get(tenant)
                        .map(|rs| latency_stats(rs))
                        .and_then(|s| s["latency_ms"]["p95"].as_u64())
                        .is_some_and(|p| p < limit)
                } else {
                    false
                }
            }
        };
        checks.insert(e.clone(), ok);
    }
    let ok = limits_respected && findings.findings() == 0 && checks.values().all(|v| *v);

    let summary = json!({
        "title": input.title,
        "run": run_meta,
        "note": "observations of one run on one machine; not an SLA",
        "limits": input.limits,
        "limits_observed": {
            "requests": requests.len(), "max_in_flight_requests": overlap,
            "last_response_t_ms": last_done, "respected": limits_respected,
        },
        "requests": latency_stats(&all),
        "requests_by_phase": by_phase.iter().map(|(k, v)| (k.clone(), latency_stats(v))).collect::<BTreeMap<_, _>>(),
        "requests_by_tenant": by_tenant.iter().map(|(k, v)| (k.clone(), latency_stats(v))).collect::<BTreeMap<_, _>>(),
        "phases": phases,
        "samples": {"count": snaps.len(), "up": findings.coverage.samples_up},
        "lifecycle": life,
        "reuse": {"mode": reuse_mode, "evidence": reuse_ev},
        "detectors": {
            "thresholds": input.thresholds,
            "findings": findings.findings(),
            "overshoot": findings.overshoot, "starvation": findings.starvation,
            "idle_cpu": findings.idle_cpu, "boot_identity": findings.boot_identity,
            "coverage": findings.coverage,
        },
        "checks": checks,
        "ok": ok,
    });
    Ok(Outputs {
        svg: svg(&input.title, &points, &phases),
        ascii: ascii(&points),
        summary,
    })
}

const SERIES: [(&str, &str, &str); 4] = [
    // (name, light, dark) — reference categorical order: blue, orange, aqua, red.
    ("busy", "#2a78d6", "#3987e5"),
    ("starting", "#eb6834", "#d95926"),
    ("idle", "#1baf7a", "#199e70"),
    ("queue", "#e34948", "#e66767"),
];

fn series_value(p: &Point, name: &str) -> f64 {
    match name {
        "busy" => p.busy + p.promised + p.starting,
        "starting" => p.starting,
        "idle" => p.idle + p.parking,
        _ => p.queue,
    }
}

/// A line chart of environments by state and queue length over time. Gaps
/// where the gateway did not answer are shaded; phases are labelled.
pub fn svg(title: &str, points: &[Point], phases: &[Value]) -> String {
    let (w, h) = (960.0, 380.0);
    let (left, right, top, bottom) = (48.0, 24.0, 64.0, 64.0);
    let t_max = points.iter().map(|p| p.t_ms).max().unwrap_or(1).max(1) as f64;
    let y_max = points
        .iter()
        .map(|p| {
            SERIES
                .iter()
                .map(|(n, _, _)| series_value(p, n))
                .fold(0.0, f64::max)
        })
        .fold(1.0, f64::max)
        .ceil();
    let x = |t: f64| left + (w - left - right) * t / t_max;
    let y = |v: f64| top + (h - top - bottom) * (1.0 - v / y_max);
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    };
    let mut s = String::new();
    let _ = write!(
        s,
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" font-family="system-ui, sans-serif" font-size="11">
<style>
  .surface {{ fill: #fcfcfb; }} .ink {{ fill: #0b0b0b; }} .muted {{ fill: #52514e; }}
  .grid {{ stroke: #e4e3dc; stroke-width: 1; }} .gap {{ fill: #d9d8d0; opacity: .6; }} .band {{ fill: #efeee8; }}
{css}
  @media (prefers-color-scheme: dark) {{
    .surface {{ fill: #1a1a19; }} .ink {{ fill: #ffffff; }} .muted {{ fill: #c3c2b7; }}
    .grid {{ stroke: #3a3a37; }} .gap {{ fill: #4a4a45; }} .band {{ fill: #242423; }}
{css_dark}
  }}
</style>
<title>{title}</title>
<rect class="surface" width="{w}" height="{h}"/>
<text class="ink" x="{left}" y="20" font-size="14" font-weight="600">{title}</text>
<text class="muted" x="{left}" y="36">environments by admission state and queue length (tsls_environments, tsls_queue_length) sampled from GET /metrics; observations, not an SLA</text>
"#,
        title = esc(title),
        css = SERIES
            .iter()
            .map(|(n, l, _)| format!("  .s-{n} {{ stroke: {l}; fill: none; }}\n"))
            .collect::<String>(),
        css_dark = SERIES
            .iter()
            .map(|(n, _, d)| format!("    .s-{n} {{ stroke: {d}; fill: none; }}\n"))
            .collect::<String>(),
    );
    // Phase bands (alternate shading) with labels below the axis.
    for (i, p) in phases.iter().enumerate() {
        let (Some(a), Some(b)) = (p["start_t_ms"].as_u64(), p["end_t_ms"].as_u64()) else {
            continue;
        };
        let (xa, xb) = (x(a as f64), x(b as f64).max(x(a as f64) + 1.0));
        if i % 2 == 0 {
            let _ = writeln!(
                s,
                r#"<rect class="band" x="{xa:.1}" y="{top}" width="{:.1}" height="{:.1}"/>"#,
                xb - xa,
                h - top - bottom
            );
        }
        let _ = writeln!(
            s,
            r#"<text class="muted" x="{:.1}" y="{:.1}" text-anchor="middle" font-size="10">{}</text>"#,
            (xa + xb) / 2.0,
            h - bottom + 30.0 + (i % 2) as f64 * 12.0,
            esc(p["phase"].as_str().unwrap_or(""))
        );
    }
    // Gateway down (restart) gaps.
    let mut gap: Option<u64> = None;
    for p in points {
        match (p.up, gap) {
            (false, None) => gap = Some(p.t_ms),
            (true, Some(g)) => {
                let _ = writeln!(
                    s,
                    r#"<rect class="gap" x="{:.1}" y="{top}" width="{:.1}" height="{:.1}"><title>gateway not answering (restart)</title></rect>
<text class="muted" x="{:.1}" y="{:.1}" font-size="10">restart</text>"#,
                    x(g as f64),
                    (x(p.t_ms as f64) - x(g as f64)).max(2.0),
                    h - top - bottom,
                    x(g as f64) + 2.0,
                    top + 12.0
                );
                gap = None;
            }
            _ => {}
        }
    }
    // Grid and axes.
    let ticks = y_max.min(8.0) as u64;
    for i in 0..=ticks {
        let v = y_max * i as f64 / ticks.max(1) as f64;
        let _ = writeln!(
            s,
            r#"<line class="grid" x1="{left}" x2="{:.1}" y1="{:.1}" y2="{:.1}"/><text class="muted" x="{:.1}" y="{:.1}" text-anchor="end">{}</text>"#,
            w - right,
            y(v),
            y(v),
            left - 6.0,
            y(v) + 4.0,
            (v * 10.0).round() / 10.0
        );
    }
    for i in 0..=6 {
        let t = t_max * i as f64 / 6.0;
        let _ = writeln!(
            s,
            r#"<text class="muted" x="{:.1}" y="{:.1}" text-anchor="middle">{:.0}s</text>"#,
            x(t),
            h - bottom + 14.0,
            t / 1000.0
        );
    }
    // Lines (broken at down samples) with a direct label at the right edge.
    for (name, _, _) in SERIES {
        let mut d = String::new();
        let mut pen = false;
        for p in points {
            if !p.up {
                pen = false;
                continue;
            }
            let _ = write!(
                d,
                "{}{:.1},{:.1} ",
                if pen { "L" } else { "M" },
                x(p.t_ms as f64),
                y(series_value(p, name))
            );
            pen = true;
        }
        let dash = if name == "queue" {
            r#" stroke-dasharray="5 3""#
        } else {
            ""
        };
        let _ = writeln!(
            s,
            r#"<path class="s-{name}" d="{d}" fill="none" stroke-width="2" stroke-linejoin="round"{dash}/>"#
        );
    }
    // Legend (identity never by color alone: names next to swatches).
    let mut lx = left;
    let ly = 52.0;
    for (name, _, _) in SERIES.iter() {
        let label = match *name {
            "busy" => "in flight (starting + busy + promised)",
            "starting" => "of which starting",
            "idle" => "idle + parking (warm pool)",
            _ => "queue length",
        };
        let dash = if *name == "queue" {
            r#" stroke-dasharray="5 3""#
        } else {
            ""
        };
        let _ = writeln!(
            s,
            r#"<line class="s-{name}" x1="{lx:.1}" x2="{:.1}" y1="{ly:.1}" y2="{ly:.1}" stroke-width="2"{dash}/><text class="ink" x="{:.1}" y="{:.1}">{label}</text>"#,
            lx + 16.0,
            lx + 20.0,
            ly + 4.0
        );
        lx += 32.0 + label.len() as f64 * 6.0;
    }
    s.push_str("</svg>\n");
    s
}

/// A terminal timeline: one row per bucket of time.
pub fn ascii(points: &[Point]) -> String {
    let mut out = String::from("t(s)    up  start busy idle queue  provisioned\n");
    let step = (points.len() / 60).max(1);
    for chunk in points.chunks(step) {
        let p = chunk
            .iter()
            .max_by(|a, b| a.provisioned().total_cmp(&b.provisioned()))
            .copied()
            .unwrap_or_default();
        let up = chunk.iter().all(|p| p.up);
        let bar: String = if up {
            "#".repeat(p.provisioned() as usize) + &"~".repeat(p.queue as usize)
        } else {
            "(gateway down)".into()
        };
        let _ = writeln!(
            out,
            "{:>6.1}  {:<3} {:>5} {:>4} {:>4} {:>5}  {}",
            chunk[0].t_ms as f64 / 1000.0,
            if up { "yes" } else { "no" },
            p.starting,
            p.busy + p.promised,
            p.idle + p.parking,
            p.queue,
            bar
        );
    }
    out.push_str("# = provisioned environment, ~ = queued invocation\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(t_ms: u64, up: bool, busy: f64, queue: f64) -> Point {
        Point {
            t_ms,
            up,
            busy,
            in_flight: busy,
            queue,
            ..Point::default()
        }
    }

    #[test]
    fn the_lifecycle_zero_up_cap_down_zero_restart_is_recognised() {
        let points = vec![
            p(0, true, 0.0, 0.0),
            p(100, true, 1.0, 0.0),
            p(200, true, 3.0, 4.0),
            p(300, true, 2.0, 0.0),
            p(400, true, 0.0, 0.0),
            p(500, false, 0.0, 0.0),
            p(600, false, 0.0, 0.0),
            p(700, true, 0.0, 0.0),
            p(800, true, 1.0, 0.0),
            p(900, true, 0.0, 0.0),
        ];
        let l = lifecycle(&points, Some(3.0));
        assert!(l.zero_before && l.rose && l.decreased && l.zero_after_peak);
        assert_eq!(l.reached_cap, Some(true));
        assert_eq!((l.max_in_flight, l.max_queue), (3.0, 4.0));
        assert_eq!(l.restarts, vec![(500, 700)]);
        assert!(l.active_after_restart && l.zero_after_restart);
        assert_eq!(lifecycle(&points, Some(4.0)).reached_cap, Some(false));
        let svg = svg(
            "t <x>",
            &points,
            &[json!({"phase": "burst", "start_t_ms": 100, "end_t_ms": 300})],
        );
        assert!(svg.starts_with("<svg") && svg.contains("t &lt;x&gt;") && svg.contains("restart"));
        assert!(ascii(&points).contains("(gateway down)"));
    }

    #[test]
    fn overlap_and_reuse_evidence() {
        let reqs = vec![
            json!({"sent_t_ms": 0, "done_t_ms": 100}),
            json!({"sent_t_ms": 50, "done_t_ms": 150}),
            json!({"sent_t_ms": 100, "done_t_ms": 200}),
        ];
        assert_eq!(
            max_overlap(&reqs),
            2,
            "a request ending as another starts does not overlap"
        );
        let every = reuse(&[
            json!({"attempts": [{"environment_id": "e1", "start_kind": "cold", "guest_boot_id": null}]}),
            json!({"attempts": [{"environment_id": "e2", "start_kind": "cold", "guest_boot_id": null}]}),
        ]);
        assert_eq!(every["every_attempt_booted_its_own_environment"], true);
        let warm = reuse(&[
            json!({"attempts": [{"environment_id": "e1", "start_kind": "cold", "guest_boot_id": "b1"}]}),
            json!({"attempts": [{"environment_id": "e1", "start_kind": "warm", "guest_boot_id": "b1"}]}),
        ]);
        assert_eq!(warm["environments_with_several_attempts"], 1);
        assert_eq!(warm["environments_whose_boot_id_changed"], 0);
        assert_eq!(warm["every_attempt_booted_its_own_environment"], false);
    }
}
