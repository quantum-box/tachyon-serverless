//! Detectors over a sequence of `/metrics` samples (PLT-4637).
//!
//! The same conditions are Prometheus alert rules in
//! `deploy/prometheus/alerts.yml`; here they run over the samples a scenario
//! recorded, so a local run needs no Prometheus.
//!
//! - **reservation overshoot**: reserved CPU / memory above the node's
//!   capacity, in-flight above `max_concurrency`, a revision's starting + busy
//!   + promised above its `max_environments`, a tenant above its quota;
//! - **starvation**: a tenant's oldest waiter older than the threshold while
//!   other tenants were granted environments during that wait;
//! - **unexpected idle resource use**: an environment idle in two successive
//!   samples used more CPU per wall second than the threshold;
//! - **boot identity**: an environment reported a different guest boot id
//!   than it booted with.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::prom::{Flat, series, value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Milliseconds since the scenario started.
    pub t_ms: u64,
    /// The scrape answered.
    pub up: bool,
    pub metrics: Flat,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Thresholds {
    /// A tenant waiting longer than this while others are granted.
    pub starvation_seconds: f64,
    /// Grants to other tenants during the wait that make it starvation.
    pub starvation_min_other_grants: f64,
    /// CPU seconds per wall second of an idle environment.
    pub idle_cpu_ratio: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            starvation_seconds: 5.0,
            starvation_min_other_grants: 1.0,
            idle_cpu_ratio: 0.05,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub detector: String,
    pub t_ms: u64,
    pub detail: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub overshoot: Vec<Finding>,
    pub starvation: Vec<Finding>,
    pub idle_cpu: Vec<Finding>,
    pub boot_identity: Vec<Finding>,
    /// Whether each detector had something to look at (a detector that saw
    /// nothing is "not exercised", not "passed").
    pub coverage: Coverage,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Coverage {
    pub samples: usize,
    pub samples_up: usize,
    /// Samples with at least one reservation.
    pub overshoot_samples_with_load: usize,
    /// Tenants that were granted anything.
    pub tenants_served: usize,
    /// Samples in which at least one idle environment was measured.
    pub idle_cpu_samples: usize,
    /// Warm reuses whose boot id was checked.
    pub same_boot_reuses: f64,
}

impl Report {
    pub fn findings(&self) -> usize {
        self.overshoot.len()
            + self.starvation.len()
            + self.idle_cpu.len()
            + self.boot_identity.len()
    }
}

fn finding(detector: &str, t_ms: u64, detail: String) -> Finding {
    Finding {
        detector: detector.into(),
        t_ms,
        detail,
    }
}

/// Reservation overshoot in one sample.
pub fn overshoot(s: &Snapshot) -> Vec<Finding> {
    let m = &s.metrics;
    let mut out = Vec::new();
    for (reserved, capacity, what) in [
        (
            "tsls_node_reserved_memory_bytes",
            "tsls_node_capacity_memory_bytes",
            "memory",
        ),
        (
            "tsls_node_reserved_cpu_millicores",
            "tsls_node_capacity_cpu_millicores",
            "cpu",
        ),
        (
            "tsls_node_reserved_ephemeral_storage_bytes",
            "tsls_node_capacity_ephemeral_storage_bytes",
            "ephemeral storage",
        ),
    ] {
        if let (Some(r), Some(c)) = (value(m, reserved, &[]), value(m, capacity, &[]))
            && r > c
        {
            out.push(finding(
                "overshoot",
                s.t_ms,
                format!("reserved {what} {r} > node capacity {c}"),
            ));
        }
    }
    if let (Some(n), Some(max)) = (
        value(m, "tsls_node_in_flight", &[]),
        value(m, "tsls_node_max_concurrency", &[]),
    ) && n > max
    {
        out.push(finding(
            "overshoot",
            s.t_ms,
            format!("node in flight {n} > max_concurrency {max}"),
        ));
    }
    let mut per_revision: BTreeMap<(String, String), f64> = BTreeMap::new();
    for (labels, v) in series(m, "tsls_revision_environments") {
        if matches!(
            labels.get("state").map(String::as_str),
            Some("starting" | "busy" | "promised")
        ) {
            *per_revision
                .entry((
                    labels.get("tenant").cloned().unwrap_or_default(),
                    labels.get("revision").cloned().unwrap_or_default(),
                ))
                .or_default() += v;
        }
    }
    for ((tenant, revision), in_flight) in per_revision {
        if revision == "_other" {
            continue;
        }
        if let Some(max) = value(
            m,
            "tsls_revision_max_environments",
            &[("tenant", &tenant), ("revision", &revision)],
        ) && in_flight > max
        {
            out.push(finding(
                "overshoot",
                s.t_ms,
                format!(
                    "revision {revision}: starting + busy + promised {in_flight} > max_environments {max}"
                ),
            ));
        }
    }
    for (labels, in_flight) in series(m, "tsls_tenant_in_flight") {
        let tenant = labels.get("tenant").cloned().unwrap_or_default();
        if tenant == "_other" {
            continue;
        }
        if let Some(max) = value(m, "tsls_tenant_max_concurrency", &[("tenant", &tenant)])
            && in_flight > max
        {
            out.push(finding(
                "overshoot",
                s.t_ms,
                format!("tenant {tenant}: in flight {in_flight} > quota {max}"),
            ));
        }
    }
    out
}

fn tenant_grants(m: &Flat) -> BTreeMap<String, f64> {
    series(m, "tsls_tenant_grants_total")
        .map(|(l, v)| (l.get("tenant").cloned().unwrap_or_default(), v))
        .collect()
}

/// Run every detector over `snaps` (in time order).
pub fn detect(snaps: &[Snapshot], th: &Thresholds) -> Report {
    let mut report = Report::default();
    let up: Vec<&Snapshot> = snaps.iter().filter(|s| s.up).collect();
    report.coverage.samples = snaps.len();
    report.coverage.samples_up = up.len();
    let mut served = BTreeSet::new();
    let mut starving: BTreeSet<String> = BTreeSet::new();
    let mut boot_changed_seen = 0.0;
    for (i, s) in up.iter().enumerate() {
        let m = &s.metrics;
        if value(m, "tsls_node_in_flight", &[]).unwrap_or(0.0) > 0.0 {
            report.coverage.overshoot_samples_with_load += 1;
        }
        report.overshoot.extend(overshoot(s));

        let grants = tenant_grants(m);
        for (t, g) in &grants {
            if *g > 0.0 && t != "_other" {
                served.insert(t.clone());
            }
        }
        for (labels, age) in series(m, "tsls_tenant_queue_oldest_age_seconds") {
            let tenant = labels.get("tenant").cloned().unwrap_or_default();
            if age <= th.starvation_seconds {
                starving.remove(&tenant);
                continue;
            }
            if starving.contains(&tenant) {
                continue;
            }
            // The sample closest to (not after) the moment the wait began.
            let began = s.t_ms.saturating_sub((age * 1000.0) as u64);
            let Some(before) = up[..=i].iter().rev().find(|p| p.t_ms <= began) else {
                continue;
            };
            let then = tenant_grants(&before.metrics);
            let others = |g: &BTreeMap<String, f64>| -> f64 {
                g.iter()
                    .filter(|(t, _)| **t != tenant)
                    .map(|(_, v)| v)
                    .sum()
            };
            let granted = others(&grants) - others(&then);
            if granted >= th.starvation_min_other_grants {
                starving.insert(tenant.clone());
                report.starvation.push(finding(
                    "starvation",
                    s.t_ms,
                    format!(
                        "tenant {tenant} waited {age:.1} s (> {} s) while other tenants were \
                         granted {granted} environments",
                        th.starvation_seconds
                    ),
                ));
            }
        }

        let sampled = value(m, "tsls_idle_environments_sampled", &[]).unwrap_or(0.0);
        if sampled > 0.0 {
            report.coverage.idle_cpu_samples += 1;
            let ratio = value(m, "tsls_idle_environment_cpu_ratio_max", &[]).unwrap_or(0.0);
            if ratio > th.idle_cpu_ratio {
                report.idle_cpu.push(finding(
                    "idle_cpu",
                    s.t_ms,
                    format!(
                        "an idle environment used {ratio:.3} CPU s per s (> {}) over {sampled} measured",
                        th.idle_cpu_ratio
                    ),
                ));
            }
        }

        let changed = value(
            m,
            "tsls_boot_identity_checks_total",
            &[("result", "boot_changed")],
        )
        .unwrap_or(0.0);
        if changed > boot_changed_seen {
            report.boot_identity.push(finding(
                "boot_identity",
                s.t_ms,
                format!(
                    "{} attempts reported a different guest boot id than their environment booted with",
                    changed - boot_changed_seen
                ),
            ));
        }
        // A gateway restart resets counters.
        boot_changed_seen = changed;
        report.coverage.same_boot_reuses = report.coverage.same_boot_reuses.max(
            value(
                m,
                "tsls_boot_identity_checks_total",
                &[("result", "same_boot")],
            )
            .unwrap_or(0.0),
        );
    }
    report.coverage.tenants_served = served.len();
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(t_ms: u64, series: &[(&str, f64)]) -> Snapshot {
        Snapshot {
            t_ms,
            up: true,
            metrics: series.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        }
    }

    #[test]
    fn overshoot_is_detected_on_resources_concurrency_revision_and_tenant_caps() {
        let ok = snap(
            0,
            &[
                ("tsls_node_reserved_memory_bytes", 100.0),
                ("tsls_node_capacity_memory_bytes", 100.0),
                ("tsls_node_in_flight", 3.0),
                ("tsls_node_max_concurrency", 3.0),
                (
                    "tsls_revision_environments{revision=r1,state=busy,tenant=t1}",
                    2.0,
                ),
                (
                    "tsls_revision_environments{revision=r1,state=starting,tenant=t1}",
                    1.0,
                ),
                (
                    "tsls_revision_environments{revision=r1,state=idle,tenant=t1}",
                    5.0,
                ),
                ("tsls_revision_max_environments{revision=r1,tenant=t1}", 3.0),
                ("tsls_tenant_in_flight{tenant=t1}", 3.0),
                ("tsls_tenant_max_concurrency{tenant=t1}", 3.0),
            ],
        );
        assert!(overshoot(&ok).is_empty(), "at the caps is not over them");
        let bad = snap(
            10,
            &[
                ("tsls_node_reserved_memory_bytes", 101.0),
                ("tsls_node_capacity_memory_bytes", 100.0),
                ("tsls_node_reserved_cpu_millicores", 900.0),
                ("tsls_node_in_flight", 4.0),
                ("tsls_node_max_concurrency", 3.0),
                (
                    "tsls_revision_environments{revision=r1,state=busy,tenant=t1}",
                    3.0,
                ),
                (
                    "tsls_revision_environments{revision=r1,state=promised,tenant=t1}",
                    1.0,
                ),
                ("tsls_revision_max_environments{revision=r1,tenant=t1}", 3.0),
                ("tsls_tenant_in_flight{tenant=t1}", 4.0),
                ("tsls_tenant_max_concurrency{tenant=t1}", 3.0),
            ],
        );
        let found = overshoot(&bad);
        assert_eq!(found.len(), 4, "{found:?}");
        assert!(found[0].detail.contains("memory"));
        assert!(
            found.iter().all(|f| !f.detail.contains("cpu")),
            "no cpu capacity configured"
        );
    }

    #[test]
    fn starvation_needs_a_long_wait_while_others_are_granted() {
        let at = |t: u64, age_b: f64, grants_a: f64| {
            snap(
                t,
                &[
                    ("tsls_tenant_grants_total{tenant=a}", grants_a),
                    ("tsls_tenant_grants_total{tenant=b}", 0.0),
                    ("tsls_tenant_queue_oldest_age_seconds{tenant=b}", age_b),
                    ("tsls_tenant_queue_oldest_age_seconds{tenant=a}", 0.0),
                ],
            )
        };
        // B waits 8 s while A is granted 4 more: starvation, reported once.
        let starving = vec![
            at(0, 0.0, 10.0),
            at(1_000, 0.5, 10.0),
            at(5_000, 4.5, 12.0),
            at(9_000, 8.5, 14.0),
            at(10_000, 9.5, 15.0),
        ];
        let r = detect(&starving, &Thresholds::default());
        assert_eq!(r.starvation.len(), 1, "{:?}", r.starvation);
        assert!(r.starvation[0].detail.contains("tenant b"));
        assert_eq!(r.coverage.tenants_served, 1);

        // A long wait while nobody else is served is a full node, not starvation.
        let idle_node = vec![at(0, 0.0, 10.0), at(9_000, 8.5, 10.0)];
        assert!(
            detect(&idle_node, &Thresholds::default())
                .starvation
                .is_empty()
        );
        // A short wait is never starvation.
        let short = vec![at(0, 0.0, 10.0), at(4_000, 4.0, 30.0)];
        assert!(detect(&short, &Thresholds::default()).starvation.is_empty());
    }

    #[test]
    fn idle_cpu_and_boot_identity_findings() {
        let snaps = vec![
            snap(
                0,
                &[
                    ("tsls_idle_environments_sampled", 0.0),
                    ("tsls_idle_environment_cpu_ratio_max", 0.0),
                    ("tsls_boot_identity_checks_total{result=boot_changed}", 0.0),
                    ("tsls_boot_identity_checks_total{result=same_boot}", 3.0),
                ],
            ),
            snap(
                1_000,
                &[
                    ("tsls_idle_environments_sampled", 2.0),
                    ("tsls_idle_environment_cpu_ratio_max", 0.01),
                ],
            ),
            snap(
                2_000,
                &[
                    ("tsls_idle_environments_sampled", 2.0),
                    ("tsls_idle_environment_cpu_ratio_max", 0.4),
                    ("tsls_boot_identity_checks_total{result=boot_changed}", 1.0),
                ],
            ),
        ];
        let r = detect(&snaps, &Thresholds::default());
        assert_eq!(r.idle_cpu.len(), 1);
        assert_eq!(r.idle_cpu[0].t_ms, 2_000);
        assert_eq!(r.boot_identity.len(), 1);
        assert_eq!(r.coverage.idle_cpu_samples, 2);
        assert_eq!(r.coverage.same_boot_reuses, 3.0);
        assert_eq!(r.findings(), 2);
    }

    #[test]
    fn samples_of_a_gateway_that_is_down_are_ignored() {
        let mut down = snap(
            0,
            &[
                ("tsls_node_in_flight", 99.0),
                ("tsls_node_max_concurrency", 1.0),
            ],
        );
        down.up = false;
        let r = detect(&[down], &Thresholds::default());
        assert!(r.overshoot.is_empty());
        assert_eq!((r.coverage.samples, r.coverage.samples_up), (1, 0));
    }
}
