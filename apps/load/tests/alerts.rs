//! `deploy/prometheus/alerts.yml` is valid YAML in the Prometheus rule-file
//! shape, and every expression names metric families the gateway exposes
//! (PLT-4637). `promtool check rules` is run by hand when available; this test
//! is the CI check.

use std::collections::BTreeSet;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleFile {
    groups: Vec<Group>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Group {
    name: String,
    rules: Vec<Rule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    alert: String,
    expr: String,
    #[serde(rename = "for")]
    for_: Option<String>,
    labels: std::collections::BTreeMap<String, String>,
    annotations: std::collections::BTreeMap<String, String>,
}

#[test]
fn alert_rules_parse_and_reference_only_exposed_metric_families() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/prometheus/alerts.yml"
    );
    let text = std::fs::read_to_string(path).expect("alerts.yml");
    let file: RuleFile = serde_norway::from_str(&text).expect("valid rule file");
    let known: BTreeSet<&str> = tachyon_serverless_application::metrics::catalog::FAMILIES
        .iter()
        .map(|(n, _, _)| *n)
        .collect();
    let mut alerts = BTreeSet::new();
    let mut detectors = BTreeSet::new();
    for g in &file.groups {
        assert!(!g.name.is_empty() && !g.rules.is_empty());
        for r in &g.rules {
            assert!(
                alerts.insert(r.alert.clone()),
                "duplicate alert {}",
                r.alert
            );
            assert!(r.labels.contains_key("severity"), "{}", r.alert);
            assert!(r.annotations.contains_key("summary"), "{}", r.alert);
            if let Some(f) = &r.for_ {
                assert!(f.ends_with('s') || f.ends_with('m'), "{}: for {f}", r.alert);
            }
            detectors.insert(r.labels["detector"].clone());
            let names: Vec<&str> = r
                .expr
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .filter(|w| w.starts_with("tsls_"))
                .collect();
            assert!(!names.is_empty(), "{}", r.alert);
            for n in names {
                assert!(
                    known.contains(n),
                    "{} references unknown metric {n}",
                    r.alert
                );
            }
            assert_eq!(
                r.expr.matches('(').count(),
                r.expr.matches(')').count(),
                "{}",
                r.alert
            );
        }
    }
    for required in ["overshoot", "starvation", "idle_cpu", "boot_identity"] {
        assert!(detectors.contains(required), "no {required} alert");
    }
}
