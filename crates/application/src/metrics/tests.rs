//! Metrics registry and exposition tests (PLT-4637).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::TimeZone;

use tachyon_serverless_domain::{
    AttemptTimings, EnvironmentId, FunctionId, RevisionId, StartKind, TenantId, Timestamp,
};
use tachyon_serverless_provider_port::EnvironmentStats;

use super::render::{EnvironmentUsage, MetricsInput, OTHER, SeriesLimits, render};
use super::*;
use crate::control::CacheStatus;
use crate::services::admission::{
    AdmissionSettings, AdmissionState, NodeConfig, Resources, TenantQuotaConfig, Ticket,
};

fn t(ms: i64) -> Timestamp {
    chrono::Utc
        .timestamp_millis_opt(1_900_000_000_000 + ms)
        .unwrap()
}

fn timings(boot_ms: u64, handler_ms: u64) -> AttemptTimings {
    AttemptTimings {
        queue_wait_ms: Some(2),
        environment_boot_ms: Some(boot_ms),
        runtime_init_ms: Some(boot_ms / 10),
        resume_ms: None,
        readiness_ms: None,
        handler_ms: Some(handler_ms),
        response_ms: Some(1),
        total_ms: Some(boot_ms + handler_ms + 3),
    }
}

#[test]
fn histograms_bucket_by_upper_bound_and_count_everything() {
    let mut h = Histogram::default();
    for s in [0.001, 0.005, 0.006, 3.0, 1_000.0] {
        h.observe(s);
    }
    assert_eq!(h.count, 5);
    assert_eq!(h.buckets[0], 2, "<= 5 ms, the bound is inclusive");
    assert_eq!(h.buckets[1], 1);
    assert_eq!(h.buckets[PHASE_BUCKETS.len()], 1, "+Inf");
    assert!((h.sum - 1_003.012).abs() < 1e-9);
}

/// Acceptance 2 (boot identity): the same environment keeps its boot id
/// across warm dispatches; a changed boot id is detected; a provider without
/// a guest kernel is `unreported`. Warm starts do not feed the boot and init
/// histograms.
#[test]
fn boot_identity_is_checked_per_environment_and_warm_starts_skip_boot_phases() {
    let m = Metrics::default();
    let env = EnvironmentId::generate();
    assert_eq!(
        m.observe_attempt(
            &env,
            StartKind::Cold,
            "succeeded",
            &timings(900, 20),
            Some("boot-1")
        ),
        BootCheck::FirstBoot
    );
    let warm = AttemptTimings {
        environment_boot_ms: Some(0),
        runtime_init_ms: Some(0),
        resume_ms: Some(3),
        ..timings(0, 10)
    };
    for _ in 0..3 {
        assert_eq!(
            m.observe_attempt(&env, StartKind::Warm, "succeeded", &warm, Some("boot-1")),
            BootCheck::SameBoot
        );
    }
    assert_eq!(
        m.observe_attempt(&env, StartKind::Warm, "succeeded", &warm, Some("boot-2")),
        BootCheck::BootChanged
    );
    let process_env = EnvironmentId::generate();
    assert_eq!(
        m.observe_attempt(
            &process_env,
            StartKind::Cold,
            "failed",
            &timings(40, 5),
            None
        ),
        BootCheck::Unreported
    );
    let snap = m.snapshot();
    assert_eq!(snap.boot_checks[&BootCheck::FirstBoot], 1);
    assert_eq!(snap.boot_checks[&BootCheck::SameBoot], 3);
    assert_eq!(snap.boot_checks[&BootCheck::BootChanged], 1);
    assert_eq!(snap.boot_checks[&BootCheck::Unreported], 1);
    assert_eq!(snap.attempts[&("warm", "succeeded")], 4);
    assert_eq!(snap.attempts[&("cold", "failed")], 1);
    assert_eq!(snap.phases[&("boot", "cold")].count, 2);
    assert!(!snap.phases.contains_key(&("boot", "warm")));
    assert!(!snap.phases.contains_key(&("init", "warm")));
    assert_eq!(snap.phases[&("resume", "warm")].count, 4);
    assert_eq!(snap.phases[&("handler", "warm")].count, 4);
}

/// Acceptance 3 (idle resource use): CPU is attributed to "idle" only between
/// two samples that both saw the environment idle; the round's maximum ratio
/// is what the detector compares with its threshold.
#[test]
fn idle_cpu_is_measured_between_two_idle_samples_only() {
    let m = Metrics::default();
    let (quiet, noisy, busy) = (
        EnvironmentId::generate(),
        EnvironmentId::generate(),
        EnvironmentId::generate(),
    );
    let t0 = Instant::now();
    let sample = |at: Instant, cpu: f64, idle: bool| EnvSample {
        at,
        cpu_seconds: cpu,
        idle,
    };
    let first = m.sample_round(&[
        (quiet.clone(), sample(t0, 1.0, true)),
        (noisy.clone(), sample(t0, 1.0, true)),
        (busy.clone(), sample(t0, 1.0, false)),
    ]);
    assert_eq!(
        first,
        IdleCpuRound::default(),
        "one sample measures nothing"
    );
    let t1 = t0 + Duration::from_secs(2);
    let second = m.sample_round(&[
        (quiet.clone(), sample(t1, 1.01, true)),
        (noisy.clone(), sample(t1, 2.0, true)),
        // busy -> idle: the CPU in between was not spent idle.
        (busy.clone(), sample(t1, 3.0, true)),
    ]);
    assert_eq!(second.sampled, 2);
    assert!((second.max_ratio - 0.5).abs() < 1e-9, "{second:?}");
    let snap = m.snapshot();
    assert!((snap.idle_cpu_seconds - 1.01).abs() < 1e-9);
    // An environment that is gone is forgotten: no stale delta later.
    let t2 = t1 + Duration::from_secs(1);
    let third = m.sample_round(&[(quiet.clone(), sample(t2, 1.01, true))]);
    assert_eq!(third.sampled, 1);
    assert_eq!(third.max_ratio, 0.0);
    let t3 = t2 + Duration::from_secs(1);
    let fourth = m.sample_round(&[(noisy, sample(t3, 9.0, true))]);
    assert_eq!(fourth.sampled, 0, "no previous sample after it disappeared");
}

fn tenant(n: u8) -> TenantId {
    TenantId::parse(&format!("tn_01hzzzzzzzzzzzzzzzzzzzzz{:02}", n)).unwrap()
}

fn ticket(tenant: &TenantId, revision: &RevisionId) -> Ticket {
    Ticket {
        tenant: tenant.clone(),
        function: FunctionId::generate(),
        revision: revision.clone(),
        idle_ttl_seconds: 60,
        scale_down_cooldown_seconds: 30,
        resources: Resources {
            cpu_millis: 500,
            memory_mib: 280,
            ephemeral_storage_mib: 256,
        },
        max_environments: 4,
        concurrency_per_environment: 1,
        min_ready: 0,
        payload_bytes: 10,
        deadline: t(60_000),
        required_region: None,
        cold_only: false,
    }
}

fn admission(tenants: u8) -> (AdmissionState, Vec<TenantId>) {
    let mut s = AdmissionState::new(AdmissionSettings {
        node: NodeConfig {
            name: "node-a".into(),
            memory_mib: Some(4096),
            ..NodeConfig::default()
        },
        max_concurrency: 3,
        max_queue: 100,
        max_queue_bytes: 1 << 20,
        queue_timeout_seconds: 10,
        tenant_defaults: TenantQuotaConfig::default(),
        tenants: Vec::new(),
        start_rate_per_second: 100,
        start_burst: 100,
        breaker_threshold: 3,
        breaker_cooldown_seconds: 30,
        rate_window_seconds: 10,
    });
    let ids: Vec<TenantId> = (1..=tenants).map(tenant).collect();
    for id in &ids {
        let rev = RevisionId::generate();
        for _ in 0..2 {
            s.enqueue(ticket(id, &rev), false, t(0)).unwrap();
        }
    }
    s.take_outbox();
    (s, ids)
}

fn input(s: &mut AdmissionState, limits: SeriesLimits, events: &Metrics) -> MetricsInput {
    let env = |state: &'static str, cpu: f64| EnvironmentUsage {
        environment: EnvironmentId::generate().to_string(),
        tenant: tenant(1).to_string(),
        revision: RevisionId::generate().to_string(),
        state,
        stats: Some(EnvironmentStats {
            cpu_seconds: Some(cpu),
            memory_current_bytes: Some(64 << 20),
            memory_peak_bytes: Some(96 << 20),
            scope: "fake".into(),
        }),
    };
    MetricsInput {
        version: "0.0.0-test".into(),
        provider: "fake".into(),
        reuse_enabled: false,
        pool_held: 0,
        pool_quiescing: 0,
        admission: s.metrics(t(2_500)),
        events: events.snapshot(),
        environments: vec![
            env("busy", 1.5),
            env("idle", 0.25),
            EnvironmentUsage {
                stats: None,
                ..env("ready", 0.0)
            },
        ],
        dispatcher_fenced: false,
        config: CacheStatus {
            source: "ledger".into(),
            generation: 7,
            ever_synced: true,
            last_attempt_at: None,
            last_success_at: None,
            consecutive_failures: 0,
            last_error: None,
            config_valid_until: Some(t(62_500)),
            auth_valid_until: Some(t(32_500)),
            reconnects: 1,
            ignored_older_entries: 0,
            ignored_regressed_deliveries: 0,
            entries: 12,
        },
        now: t(2_500),
        limits,
        outbox: Some(super::render::OutboxMetrics {
            pending: 2,
            oldest_pending_at: Some(t(500)),
            sent_retained: 5,
            queue_condition: "full",
        }),
    }
}

/// A parsed exposition: `(name, labels) -> value`, plus the family order.
struct Exposition {
    samples: HashMap<(String, BTreeMap<String, String>), f64>,
}

impl Exposition {
    fn parse(text: &str) -> Self {
        let mut samples = HashMap::new();
        let mut typed: HashMap<String, String> = HashMap::new();
        let mut helped: HashSet<String> = HashSet::new();
        let mut closed: HashSet<String> = HashSet::new();
        let mut current: Option<String> = None;
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(helped.insert(name.clone()), "HELP twice for {name}");
                if let Some(prev) = current.replace(name.clone())
                    && prev != name
                {
                    closed.insert(prev);
                }
                assert!(!closed.contains(&name), "family {name} is not contiguous");
                continue;
            }
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let mut it = rest.split(' ');
                let name = it.next().unwrap().to_string();
                let kind = it.next().unwrap().to_string();
                assert!(helped.contains(&name), "TYPE before HELP for {name}");
                typed.insert(name, kind);
                continue;
            }
            assert!(!line.starts_with('#'), "unexpected comment {line}");
            let (series, value) = line.rsplit_once(' ').expect("sample line");
            let value: f64 = match value {
                "+Inf" => f64::INFINITY,
                v => v.parse().unwrap_or_else(|_| panic!("value in {line}")),
            };
            let (name, labels) = match series.split_once('{') {
                None => (series.to_string(), BTreeMap::new()),
                Some((name, rest)) => {
                    let body = rest.strip_suffix('}').expect("closing brace");
                    let mut labels = BTreeMap::new();
                    for pair in body.split("\",") {
                        let (k, v) = pair.split_once("=\"").expect("label");
                        labels.insert(k.to_string(), v.trim_end_matches('"').to_string());
                    }
                    (name.to_string(), labels)
                }
            };
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| name.strip_suffix(s))
                .filter(|f| typed.get(*f).map(String::as_str) == Some("histogram"))
                .unwrap_or(&name)
                .to_string();
            assert_eq!(
                current.as_deref(),
                Some(family.as_str()),
                "sample {name} outside its family block"
            );
            assert!(
                catalog::family(&family).is_some(),
                "{family} is not in the catalog"
            );
            assert!(
                samples.insert((name, labels), value).is_none(),
                "duplicate series: {line}"
            );
        }
        Self { samples }
    }

    fn get(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let labels: BTreeMap<String, String> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.samples.get(&(name.to_string(), labels)).copied()
    }

    fn series_of(&self, name: &str) -> Vec<&BTreeMap<String, String>> {
        self.samples
            .keys()
            .filter(|(n, _)| n == name)
            .map(|(_, l)| l)
            .collect()
    }
}

#[test]
fn the_exposition_is_well_formed_and_carries_admission_attempts_and_host_usage() {
    let (mut s, tenants) = admission(2);
    let events = Metrics::default();
    let env = EnvironmentId::generate();
    events.observe_attempt(&env, StartKind::Cold, "succeeded", &timings(120, 30), None);
    events.gate_refusal("Host.ConfigExpired");
    events.heartbeat("renewed", 2);
    let text = render(&input(&mut s, SeriesLimits::default(), &events));
    let x = Exposition::parse(&text);

    assert_eq!(x.get("tsls_node_in_flight", &[]), Some(3.0));
    assert_eq!(x.get("tsls_queue_length", &[]), Some(1.0));
    assert_eq!(x.get("tsls_queue_oldest_age_seconds", &[]), Some(2.5));
    assert_eq!(
        x.get("tsls_node_capacity_memory_bytes", &[]),
        Some(4096.0 * 1024.0 * 1024.0)
    );
    assert_eq!(x.get("tsls_node_capacity_cpu_millicores", &[]), None);
    assert_eq!(
        x.get("tsls_node_reserved_memory_bytes", &[]),
        Some(3.0 * 280.0 * 1024.0 * 1024.0)
    );
    assert_eq!(
        x.get("tsls_environments", &[("state", "starting")]),
        Some(3.0)
    );
    assert_eq!(
        x.get("tsls_admission_grants_total", &[("kind", "cold")]),
        Some(3.0)
    );
    assert_eq!(
        x.get(
            "tsls_admission_rejections_total",
            &[("reason", "queue_full")]
        ),
        Some(0.0),
        "every reason starts at zero"
    );
    assert_eq!(
        x.get(
            "tsls_tenant_queue_oldest_age_seconds",
            &[("tenant", tenants[1].as_str())]
        ),
        Some(2.5)
    );
    assert_eq!(
        x.get(
            "tsls_tenant_grants_total",
            &[("tenant", tenants[0].as_str())]
        ),
        Some(2.0)
    );
    assert_eq!(
        x.get(
            "tsls_environment_reuse_mode",
            &[("provider", "fake"), ("mode", "every_invocation_boots")]
        ),
        Some(1.0)
    );
    assert_eq!(
        x.get(
            "tsls_attempts_total",
            &[("start_kind", "cold"), ("status", "succeeded")]
        ),
        Some(1.0)
    );
    let bucket = |le: &str| {
        x.get(
            "tsls_attempt_phase_seconds_bucket",
            &[("phase", "boot"), ("start_kind", "cold"), ("le", le)],
        )
    };
    assert_eq!(bucket("0.1"), Some(0.0));
    assert_eq!(bucket("0.25"), Some(1.0));
    assert_eq!(bucket("+Inf"), Some(1.0));
    assert_eq!(
        x.get(
            "tsls_boot_identity_checks_total",
            &[("result", "unreported")]
        ),
        Some(1.0)
    );
    assert_eq!(
        x.get(
            "tsls_gate_refusals_total",
            &[("error_type", "Host.ConfigExpired")]
        ),
        Some(1.0)
    );
    assert_eq!(
        x.get("tsls_dispatcher_slot_lease_renewals_total", &[]),
        Some(2.0)
    );
    assert_eq!(
        x.get("tsls_environment_stats", &[("result", "unavailable")]),
        Some(1.0)
    );
    assert_eq!(x.series_of("tsls_environment_cpu_seconds_total").len(), 2);
    assert_eq!(
        x.get("tsls_config_valid_remaining_seconds", &[]),
        Some(60.0)
    );
    assert_eq!(x.get("tsls_auth_lease_remaining_seconds", &[]), Some(30.0));
    assert_eq!(x.get("tsls_async_outbox_pending_events", &[]), Some(2.0));
    assert_eq!(
        x.get("tsls_async_outbox_oldest_pending_age_seconds", &[]),
        Some(2.0)
    );
    assert_eq!(
        x.get("tsls_async_queue_condition", &[("condition", "full")]),
        Some(1.0)
    );
    // Histogram buckets are cumulative and end at the count.
    let mut les: Vec<(f64, f64)> = x
        .samples
        .iter()
        .filter(|((n, l), _)| n == "tsls_attempt_phase_seconds_bucket" && l["phase"] == "total")
        .map(|((_, l), v)| {
            let le = if l["le"] == "+Inf" {
                f64::INFINITY
            } else {
                l["le"].parse().unwrap()
            };
            (le, *v)
        })
        .collect();
    les.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    assert!(les.windows(2).all(|w| w[0].1 <= w[1].1));
}

/// Label cardinality is bounded: beyond the caps, tenants and revisions are
/// folded into one `_other` series and the number folded is reported.
#[test]
fn tenant_and_revision_series_beyond_the_caps_fold_into_other() {
    let (mut s, tenants) = admission(5);
    let events = Metrics::default();
    let limits = SeriesLimits {
        revisions: 2,
        tenants: 2,
        environments: 1,
    };
    let text = render(&input(&mut s, limits, &events));
    let x = Exposition::parse(&text);
    let tenant_series = x.series_of("tsls_tenant_queue_length");
    assert_eq!(tenant_series.len(), 3, "2 tenants + _other");
    assert!(tenant_series.iter().any(|l| l["tenant"] == OTHER));
    let revision_series = x.series_of("tsls_revision_desired_environments");
    assert_eq!(revision_series.len(), 3);
    assert_eq!(
        x.get("tsls_metrics_series_truncated", &[("dimension", "tenant")]),
        Some(3.0)
    );
    assert_eq!(
        x.get(
            "tsls_metrics_series_truncated",
            &[("dimension", "revision")]
        ),
        Some(3.0)
    );
    assert_eq!(
        x.get(
            "tsls_metrics_series_truncated",
            &[("dimension", "environment")]
        ),
        Some(2.0),
        "3 environments capped at 1 series"
    );
    // The folded sums keep the node-wide totals.
    let total: f64 = x
        .samples
        .iter()
        .filter(|((n, _), _)| n == "tsls_tenant_queue_length")
        .map(|(_, v)| v)
        .sum();
    assert_eq!(Some(total), x.get("tsls_queue_length", &[]));
    assert_eq!(tenants.len(), 5);
}

/// Label values are escaped (a quote, a backslash or a newline in a node
/// name cannot break the exposition).
#[test]
fn label_values_are_escaped() {
    let mut s = AdmissionState::new(AdmissionSettings {
        node: NodeConfig {
            name: "odd\"node\\\nname".into(),
            ..NodeConfig::default()
        },
        max_concurrency: 1,
        max_queue: 1,
        max_queue_bytes: 1,
        queue_timeout_seconds: 1,
        tenant_defaults: TenantQuotaConfig::default(),
        tenants: Vec::new(),
        start_rate_per_second: 1,
        start_burst: 1,
        breaker_threshold: 1,
        breaker_cooldown_seconds: 1,
        rate_window_seconds: 1,
    });
    let events = Metrics::default();
    let text = render(&input(&mut s, SeriesLimits::default(), &events));
    assert!(text.contains(r#"tsls_node_info{node="odd\"node\\\nname"} 1"#));
    assert_eq!(text.lines().filter(|l| l.contains("odd")).count(), 1);
}

/// Every family in the catalog is documented in docs/metrics.md.
#[test]
fn every_metric_family_is_documented() {
    let doc = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/metrics.md"
    ))
    .expect("docs/metrics.md");
    let missing: Vec<&str> = catalog::FAMILIES
        .iter()
        .map(|(n, _, _)| *n)
        .filter(|n| !doc.contains(&format!("`{n}`")))
        .collect();
    assert!(
        missing.is_empty(),
        "undocumented metric families: {missing:?}"
    );
}
