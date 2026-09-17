//! Prometheus text exposition (format 0.0.4) of [`MetricsInput`].
//!
//! A pure function: the gateway gathers the input (admission state, pool,
//! cache, provider samples, event registry) and this module only formats it,
//! folding label sets beyond the `[metrics]` caps into an `_other` series.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use tachyon_serverless_api_types::EnvironmentCounts;
use tachyon_serverless_provider_port::EnvironmentStats;

use super::catalog;
use super::{BootCheck, MetricsSnapshot, PHASE_BUCKETS, PHASES};
use crate::control::CacheStatus;
use crate::services::admission::{AdmissionMetrics, RejectReason, RevisionMetrics, TenantMetrics};

/// Label value of the series that aggregates what a cap folded away.
pub const OTHER: &str = "_other";

/// The `Content-Type` of the exposition.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

const MIB: u64 = 1024 * 1024;

/// Cardinality caps (`[metrics] max_*_series`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesLimits {
    pub revisions: usize,
    pub tenants: usize,
    pub environments: usize,
}

impl Default for SeriesLimits {
    fn default() -> Self {
        Self {
            revisions: 64,
            tenants: 32,
            environments: 128,
        }
    }
}

/// One live environment of this dispatcher and its host usage.
#[derive(Debug, Clone, PartialEq)]
pub struct EnvironmentUsage {
    pub environment: String,
    pub tenant: String,
    pub revision: String,
    /// Ledger state name (`ready`, `busy`, `idle`, ...).
    pub state: &'static str,
    pub stats: Option<EnvironmentStats>,
}

/// Everything one scrape renders.
#[derive(Debug, Clone)]
pub struct MetricsInput {
    pub version: String,
    pub provider: String,
    pub reuse_enabled: bool,
    pub pool_held: u64,
    pub pool_quiescing: u64,
    pub admission: AdmissionMetrics,
    pub events: MetricsSnapshot,
    pub environments: Vec<EnvironmentUsage>,
    pub dispatcher_fenced: bool,
    pub config: CacheStatus,
    pub now: tachyon_serverless_domain::Timestamp,
    pub limits: SeriesLimits,
    /// The asynchronous invoke outbox (PLT-4639), when this gateway has one.
    pub outbox: Option<OutboxMetrics>,
    /// Trigger counters (PLT-4641), when this gateway has triggers.
    pub triggers: Option<crate::services::triggers::metrics::TriggerMetricsSnapshot>,
    /// The usage journal, collector and ledger (PLT-4642).
    pub usage: Option<UsageMetrics>,
    /// The asynchronous dispatcher (PLT-4640), when this gateway runs one.
    pub dispatch: Option<crate::metrics::dispatch::AsyncDispatchSnapshot>,
}

/// Usage metering pipeline state (PLT-4642, docs/adr/0012).
#[derive(Debug, Clone, PartialEq)]
pub struct UsageMetrics {
    pub journal_healthy: bool,
    /// New invocations are admitted *metered*.
    pub journal_admitting: bool,
    pub journal_pending_events: u64,
    pub journal_pending_bytes: u64,
    pub journal_max_events: u64,
    pub journal_max_bytes: u64,
    pub unjournaled_events: u64,
    pub collector_runs: u64,
    pub collector_failing: bool,
    pub collector_last_success_at: Option<tachyon_serverless_domain::Timestamp>,
    pub collector_delivered: u64,
    pub ledger_events: Option<u64>,
    pub ledger_duplicates_ignored: Option<u64>,
}

/// The transactional outbox's backlog and the queue as its publisher last saw it.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxMetrics {
    pub pending: u64,
    pub oldest_pending_at: Option<tachyon_serverless_domain::Timestamp>,
    pub sent_retained: u64,
    /// `healthy` | `full` | `unavailable`.
    pub queue_condition: &'static str,
}

struct Writer {
    out: String,
    family: Option<&'static str>,
}

fn escape(v: &str) -> String {
    let mut s = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => s.push_str("\\\\"),
            '"' => s.push_str("\\\""),
            '\n' => s.push_str("\\n"),
            c => s.push(c),
        }
    }
    s
}

fn number(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v == f64::INFINITY {
        "+Inf".into()
    } else if v == f64::NEG_INFINITY {
        "-Inf".into()
    } else if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

impl Writer {
    fn new() -> Self {
        Self {
            out: String::with_capacity(16 * 1024),
            family: None,
        }
    }

    fn header(&mut self, family: &'static str) {
        if self.family == Some(family) {
            return;
        }
        let (kind, help) = catalog::family(family)
            .unwrap_or_else(|| panic!("metric family `{family}` is not in the catalog"));
        let _ = writeln!(self.out, "# HELP {family} {help}");
        let _ = writeln!(self.out, "# TYPE {family} {kind}");
        self.family = Some(family);
    }

    fn line(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                let _ = write!(self.out, "{k}=\"{}\"", escape(v));
            }
            self.out.push('}');
        }
        let _ = writeln!(self.out, " {}", number(value));
    }

    /// One sample of a gauge or counter family.
    fn sample(&mut self, family: &'static str, labels: &[(&str, &str)], value: f64) {
        self.header(family);
        self.line(family, labels, value);
    }
}

fn state_counts(c: &EnvironmentCounts) -> [(&'static str, u64); 6] {
    [
        ("promised", c.promised),
        ("starting", c.starting),
        ("busy", c.busy),
        ("parking", c.parking),
        ("idle", c.idle),
        ("draining", c.draining),
    ]
}

fn provisioned(c: &EnvironmentCounts) -> u64 {
    c.promised + c.starting + c.busy + c.parking + c.idle + c.draining
}

fn breaker_value(name: &str) -> u64 {
    match name {
        "open" => 2,
        "half_open" => 1,
        _ => 0,
    }
}

/// A revision series after the cardinality cap.
struct RevisionRow {
    tenant: String,
    revision: String,
    environments: EnvironmentCounts,
    desired: u32,
    max_environments: u32,
    min_ready: u32,
    queued: u32,
    breaker: &'static str,
}

/// A tenant series after the cardinality cap.
struct TenantRow {
    tenant: String,
    in_flight: u64,
    queued: u64,
    oldest_wait_seconds: Option<f64>,
    grants: u64,
    max_concurrency: Option<u64>,
}

/// Keep the `limit` most active revisions (provisioned + queued); fold the
/// rest into one `_other` series. Returns the rows and how many were folded.
fn cap_revisions(revisions: &[RevisionMetrics], limit: usize) -> (Vec<RevisionRow>, usize) {
    let mut sorted: Vec<&RevisionMetrics> = revisions.iter().collect();
    let weight = |r: &RevisionMetrics| provisioned(&r.environments) + u64::from(r.queued);
    sorted.sort_by(|a, b| {
        weight(b)
            .cmp(&weight(a))
            .then_with(|| a.revision.cmp(&b.revision))
    });
    let row = |r: &RevisionMetrics| RevisionRow {
        tenant: r.tenant.to_string(),
        revision: r.revision.to_string(),
        environments: r.environments,
        desired: r.desired,
        max_environments: r.max_environments,
        min_ready: r.min_ready,
        queued: r.queued,
        breaker: r.breaker,
    };
    if sorted.len() <= limit {
        let mut rows: Vec<RevisionRow> = sorted.into_iter().map(row).collect();
        rows.sort_by(|a, b| a.revision.cmp(&b.revision));
        return (rows, 0);
    }
    let rest = sorted.split_off(limit);
    let mut rows: Vec<RevisionRow> = sorted.into_iter().map(row).collect();
    rows.sort_by(|a, b| a.revision.cmp(&b.revision));
    let mut other = RevisionRow {
        tenant: OTHER.into(),
        revision: OTHER.into(),
        environments: EnvironmentCounts {
            starting: 0,
            busy: 0,
            promised: 0,
            parking: 0,
            idle: 0,
            draining: 0,
        },
        desired: 0,
        max_environments: 0,
        min_ready: 0,
        queued: 0,
        breaker: "closed",
    };
    for r in &rest {
        let c = &mut other.environments;
        c.starting += r.environments.starting;
        c.busy += r.environments.busy;
        c.promised += r.environments.promised;
        c.parking += r.environments.parking;
        c.idle += r.environments.idle;
        c.draining += r.environments.draining;
        other.desired += r.desired;
        other.max_environments += r.max_environments;
        other.min_ready += r.min_ready;
        other.queued += r.queued;
        if breaker_value(r.breaker) > breaker_value(other.breaker) {
            other.breaker = r.breaker;
        }
    }
    rows.push(other);
    (rows, rest.len())
}

/// Keep the `limit` busiest tenants (queued, in flight, grants); fold the
/// rest into `_other` (sums; the oldest wait is the maximum).
fn cap_tenants(tenants: &[TenantMetrics], limit: usize) -> (Vec<TenantRow>, usize) {
    let mut sorted: Vec<&TenantMetrics> = tenants.iter().collect();
    sorted.sort_by(|a, b| {
        (b.queued, b.in_flight, b.grants)
            .cmp(&(a.queued, a.in_flight, a.grants))
            .then_with(|| a.tenant.cmp(&b.tenant))
    });
    let row = |t: &TenantMetrics| TenantRow {
        tenant: t.tenant.to_string(),
        in_flight: t.in_flight,
        queued: t.queued,
        oldest_wait_seconds: t.oldest_wait_seconds,
        grants: t.grants,
        max_concurrency: t.max_concurrency,
    };
    let rest = if sorted.len() > limit {
        sorted.split_off(limit)
    } else {
        Vec::new()
    };
    let mut rows: Vec<TenantRow> = sorted.into_iter().map(row).collect();
    rows.sort_by(|a, b| a.tenant.cmp(&b.tenant));
    if !rest.is_empty() {
        let mut other = TenantRow {
            tenant: OTHER.into(),
            in_flight: 0,
            queued: 0,
            oldest_wait_seconds: None,
            grants: 0,
            max_concurrency: None,
        };
        for t in &rest {
            other.in_flight += t.in_flight;
            other.queued += t.queued;
            other.grants += t.grants;
            other.oldest_wait_seconds = match (other.oldest_wait_seconds, t.oldest_wait_seconds) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
        }
        rows.push(other);
    }
    (rows, rest.len())
}

/// Render one scrape.
pub fn render(input: &MetricsInput) -> String {
    let mut w = Writer::new();
    let a = &input.admission;
    let limits = input.limits;

    w.sample("tsls_build_info", &[("version", &input.version)], 1.0);
    for mode in ["warm_reuse", "every_invocation_boots"] {
        let active = (mode == "warm_reuse") == input.reuse_enabled;
        w.sample(
            "tsls_environment_reuse_mode",
            &[("provider", &input.provider), ("mode", mode)],
            if active { 1.0 } else { 0.0 },
        );
    }
    w.sample(
        "tsls_pool_reuse_enabled",
        &[],
        f64::from(u8::from(input.reuse_enabled)),
    );
    w.sample("tsls_pool_held_environments", &[], input.pool_held as f64);
    w.sample(
        "tsls_pool_quiescing_environments",
        &[],
        input.pool_quiescing as f64,
    );

    // node ------------------------------------------------------------------
    w.sample("tsls_node_info", &[("node", &a.node_name)], 1.0);
    if let Some(cpu) = a.capacity.cpu_millis {
        w.sample("tsls_node_capacity_cpu_millicores", &[], cpu as f64);
    }
    if let Some(mem) = a.capacity.memory_mib {
        w.sample(
            "tsls_node_capacity_memory_bytes",
            &[],
            (mem.saturating_mul(MIB)) as f64,
        );
    }
    if let Some(disk) = a.capacity.ephemeral_storage_mib {
        w.sample(
            "tsls_node_capacity_ephemeral_storage_bytes",
            &[],
            (disk.saturating_mul(MIB)) as f64,
        );
    }
    w.sample(
        "tsls_node_reserved_cpu_millicores",
        &[],
        a.reserved.cpu_millis as f64,
    );
    w.sample(
        "tsls_node_reserved_memory_bytes",
        &[],
        a.reserved.memory_mib.saturating_mul(MIB) as f64,
    );
    w.sample(
        "tsls_node_reserved_ephemeral_storage_bytes",
        &[],
        a.reserved.ephemeral_storage_mib.saturating_mul(MIB) as f64,
    );
    w.sample(
        "tsls_node_environment_overhead_memory_bytes",
        &[],
        a.overhead.memory_mib.saturating_mul(MIB) as f64,
    );
    w.sample("tsls_node_max_concurrency", &[], a.max_concurrency as f64);
    w.sample("tsls_node_in_flight", &[], a.in_flight as f64);
    for (state, n) in state_counts(&a.environments) {
        w.sample("tsls_environments", &[("state", state)], n as f64);
    }

    // revisions -------------------------------------------------------------
    let (revisions, folded_revisions) = cap_revisions(&a.revisions, limits.revisions);
    for r in &revisions {
        for (state, n) in state_counts(&r.environments) {
            w.sample(
                "tsls_revision_environments",
                &[
                    ("tenant", &r.tenant),
                    ("revision", &r.revision),
                    ("state", state),
                ],
                n as f64,
            );
        }
    }
    type RevisionValue = fn(&RevisionRow) -> f64;
    let per_revision: [(&'static str, RevisionValue); 5] = [
        ("tsls_revision_desired_environments", |r| {
            f64::from(r.desired)
        }),
        ("tsls_revision_max_environments", |r| {
            f64::from(r.max_environments)
        }),
        ("tsls_revision_min_ready", |r| f64::from(r.min_ready)),
        ("tsls_revision_queue_length", |r| f64::from(r.queued)),
        ("tsls_revision_circuit_breaker_state", |r| {
            breaker_value(r.breaker) as f64
        }),
    ];
    for (family, value) in per_revision {
        for r in &revisions {
            w.sample(
                family,
                &[("tenant", &r.tenant), ("revision", &r.revision)],
                value(r),
            );
        }
    }

    // queue -----------------------------------------------------------------
    w.sample("tsls_queue_length", &[], a.queue_length as f64);
    w.sample("tsls_queue_bytes", &[], a.queue_bytes as f64);
    w.sample("tsls_queue_max_length", &[], a.max_queue as f64);
    w.sample("tsls_queue_max_bytes", &[], a.max_queue_bytes as f64);
    w.sample(
        "tsls_queue_oldest_age_seconds",
        &[],
        a.oldest_wait_seconds.unwrap_or(0.0),
    );
    let (tenants, folded_tenants) = cap_tenants(&a.tenants, limits.tenants);
    type TenantValue = fn(&TenantRow) -> Option<f64>;
    let per_tenant: [(&'static str, TenantValue); 5] = [
        ("tsls_tenant_queue_length", |t| Some(t.queued as f64)),
        ("tsls_tenant_queue_oldest_age_seconds", |t| {
            Some(t.oldest_wait_seconds.unwrap_or(0.0))
        }),
        ("tsls_tenant_in_flight", |t| Some(t.in_flight as f64)),
        ("tsls_tenant_max_concurrency", |t| {
            t.max_concurrency.map(|m| m as f64)
        }),
        ("tsls_tenant_grants_total", |t| Some(t.grants as f64)),
    ];
    for (family, value) in per_tenant {
        for t in &tenants {
            if let Some(v) = value(t) {
                w.sample(family, &[("tenant", &t.tenant)], v);
            }
        }
    }
    w.sample("tsls_start_rate_tokens", &[], f64::from(a.start_tokens));

    // admission events --------------------------------------------------------
    let c = &a.counters;
    w.sample("tsls_admission_arrivals_total", &[], c.arrivals as f64);
    w.sample(
        "tsls_admission_grants_total",
        &[("kind", "cold")],
        c.grants_cold as f64,
    );
    w.sample(
        "tsls_admission_grants_total",
        &[("kind", "warm")],
        c.grants_warm as f64,
    );
    for reason in RejectReason::ALL {
        w.sample(
            "tsls_admission_rejections_total",
            &[("reason", reason.as_str())],
            a.rejections.get(&reason).copied().unwrap_or(0) as f64,
        );
    }
    w.sample(
        "tsls_admission_coalesced_waits_total",
        &[],
        c.coalesced_waits as f64,
    );
    w.sample(
        "tsls_admission_starts_avoided_total",
        &[],
        c.starts_avoided as f64,
    );
    w.sample(
        "tsls_environment_starts_total",
        &[("result", "success")],
        c.start_successes as f64,
    );
    w.sample(
        "tsls_environment_starts_total",
        &[("result", "failure")],
        c.start_failures as f64,
    );
    w.sample(
        "tsls_circuit_breaker_opens_total",
        &[],
        c.breaker_opens as f64,
    );
    for kind in [
        "activation",
        "scale_up",
        "prestart",
        "scale_down",
        "scale_to_zero",
        "drain",
    ] {
        let mut any = false;
        for ((k, reason), n) in &c.scale_events {
            if *k == kind {
                any = true;
                w.sample(
                    "tsls_scale_events_total",
                    &[("kind", k), ("reason", reason)],
                    *n as f64,
                );
            }
        }
        if !any {
            w.sample(
                "tsls_scale_events_total",
                &[("kind", kind), ("reason", "none")],
                0.0,
            );
        }
    }
    let e = &input.events;
    if e.gate_refusals.is_empty() {
        w.sample("tsls_gate_refusals_total", &[("error_type", "none")], 0.0);
    }
    for (error_type, n) in &e.gate_refusals {
        w.sample(
            "tsls_gate_refusals_total",
            &[("error_type", error_type)],
            *n as f64,
        );
    }

    // attempts ----------------------------------------------------------------
    for kind in ["cold", "warm", "restored"] {
        let mut any = false;
        for ((k, status), n) in &e.attempts {
            if *k == kind {
                any = true;
                w.sample(
                    "tsls_attempts_total",
                    &[("start_kind", k), ("status", status)],
                    *n as f64,
                );
            }
        }
        if !any {
            w.sample(
                "tsls_attempts_total",
                &[("start_kind", kind), ("status", "succeeded")],
                0.0,
            );
        }
    }
    w.header("tsls_attempt_phase_seconds");
    let mut by_phase: BTreeMap<(usize, &str), &super::Histogram> = BTreeMap::new();
    for ((phase, kind), h) in &e.phases {
        let order = PHASES
            .iter()
            .position(|p| p == phase)
            .unwrap_or(PHASES.len());
        by_phase.insert((order, kind), h);
    }
    for ((order, kind), h) in by_phase {
        let phase = PHASES.get(order).copied().unwrap_or("other");
        let mut cumulative = 0;
        for (i, upper) in PHASE_BUCKETS.iter().enumerate() {
            cumulative += h.buckets[i];
            w.line(
                "tsls_attempt_phase_seconds_bucket",
                &[
                    ("phase", phase),
                    ("start_kind", kind),
                    ("le", &number(*upper)),
                ],
                cumulative as f64,
            );
        }
        w.line(
            "tsls_attempt_phase_seconds_bucket",
            &[("phase", phase), ("start_kind", kind), ("le", "+Inf")],
            h.count as f64,
        );
        w.line(
            "tsls_attempt_phase_seconds_sum",
            &[("phase", phase), ("start_kind", kind)],
            h.sum,
        );
        w.line(
            "tsls_attempt_phase_seconds_count",
            &[("phase", phase), ("start_kind", kind)],
            h.count as f64,
        );
    }
    for check in BootCheck::ALL {
        w.sample(
            "tsls_boot_identity_checks_total",
            &[("result", check.as_str())],
            e.boot_checks.get(&check).copied().unwrap_or(0) as f64,
        );
    }

    // host usage --------------------------------------------------------------
    let mut envs: Vec<&EnvironmentUsage> = input.environments.iter().collect();
    // Busy first, then idle, then the rest; stable by id.
    let rank = |s: &str| match s {
        "busy" => 0,
        "idle" => 1,
        _ => 2,
    };
    envs.sort_by(|x, y| {
        rank(x.state)
            .cmp(&rank(y.state))
            .then_with(|| x.environment.cmp(&y.environment))
    });
    let folded_envs = envs.len().saturating_sub(limits.environments);
    envs.truncate(limits.environments);
    let available = input
        .environments
        .iter()
        .filter(|e| e.stats.is_some())
        .count();
    type StatValue = fn(&EnvironmentStats) -> Option<f64>;
    let per_env: [(&'static str, StatValue); 3] = [
        ("tsls_environment_cpu_seconds_total", |s| s.cpu_seconds),
        ("tsls_environment_memory_bytes", |s| {
            s.memory_current_bytes.map(|b| b as f64)
        }),
        ("tsls_environment_memory_peak_bytes", |s| {
            s.memory_peak_bytes.map(|b| b as f64)
        }),
    ];
    for (family, value) in per_env {
        for env in &envs {
            let Some(stats) = &env.stats else { continue };
            if let Some(v) = value(stats) {
                w.sample(
                    family,
                    &[
                        ("environment", &env.environment),
                        ("tenant", &env.tenant),
                        ("revision", &env.revision),
                        ("state", env.state),
                        ("scope", &stats.scope),
                    ],
                    v,
                );
            }
        }
    }
    w.sample(
        "tsls_environment_stats",
        &[("result", "available")],
        available as f64,
    );
    w.sample(
        "tsls_environment_stats",
        &[("result", "unavailable")],
        (input.environments.len() - available) as f64,
    );
    w.sample(
        "tsls_idle_environment_cpu_seconds_total",
        &[],
        e.idle_cpu_seconds,
    );
    w.sample(
        "tsls_idle_environment_cpu_ratio_max",
        &[],
        e.idle_round.max_ratio,
    );
    w.sample(
        "tsls_idle_environments_sampled",
        &[],
        e.idle_round.sampled as f64,
    );

    // dispatcher / configuration --------------------------------------------
    w.sample(
        "tsls_dispatcher_fenced",
        &[],
        f64::from(u8::from(input.dispatcher_fenced)),
    );
    for result in ["renewed", "fenced", "error"] {
        w.sample(
            "tsls_dispatcher_heartbeats_total",
            &[("result", result)],
            e.heartbeats.get(result).copied().unwrap_or(0) as f64,
        );
    }
    w.sample(
        "tsls_dispatcher_slot_lease_renewals_total",
        &[],
        e.slot_lease_renewals as f64,
    );
    let cfg = &input.config;
    w.sample("tsls_config_generation", &[], cfg.generation as f64);
    w.sample(
        "tsls_config_synced",
        &[],
        f64::from(u8::from(cfg.ever_synced)),
    );
    w.sample(
        "tsls_config_consecutive_failures",
        &[],
        f64::from(cfg.consecutive_failures),
    );
    let remaining = |until: Option<tachyon_serverless_domain::Timestamp>| {
        until.map(|u| (u - input.now).num_milliseconds() as f64 / 1000.0)
    };
    if let Some(v) = remaining(cfg.config_valid_until) {
        w.sample("tsls_config_valid_remaining_seconds", &[], v);
    }
    if let Some(v) = remaining(cfg.auth_valid_until) {
        w.sample("tsls_auth_lease_remaining_seconds", &[], v);
    }
    w.sample("tsls_config_reconnects_total", &[], cfg.reconnects as f64);
    w.sample("tsls_config_entries", &[], cfg.entries as f64);

    if let Some(t) = &input.triggers {
        use crate::services::triggers::metrics::{CRON_RESULTS, MISSED_ACTIONS, WEBHOOK_RESULTS};
        w.sample(
            "tsls_trigger_scheduler_owner",
            &[],
            f64::from(u8::from(t.scheduler_owner)),
        );
        for result in CRON_RESULTS {
            w.sample(
                "tsls_trigger_cron_fires_total",
                &[("result", result)],
                t.cron.get(result).copied().unwrap_or(0) as f64,
            );
        }
        for action in MISSED_ACTIONS {
            w.sample(
                "tsls_trigger_cron_missed_runs_total",
                &[("action", action)],
                t.missed.get(action).copied().unwrap_or(0) as f64,
            );
        }
        for result in WEBHOOK_RESULTS {
            w.sample(
                "tsls_trigger_webhook_deliveries_total",
                &[("result", result)],
                t.webhook.get(result).copied().unwrap_or(0) as f64,
            );
        }
    }
    if let Some(o) = &input.outbox {
        w.sample("tsls_async_outbox_pending_events", &[], o.pending as f64);
        w.sample(
            "tsls_async_outbox_oldest_pending_age_seconds",
            &[],
            o.oldest_pending_at.map_or(0.0, |t| {
                ((input.now - t).num_milliseconds().max(0) as f64) / 1000.0
            }),
        );
        w.sample(
            "tsls_async_outbox_sent_retained_events",
            &[],
            o.sent_retained as f64,
        );
        for condition in ["healthy", "full", "unavailable"] {
            w.sample(
                "tsls_async_queue_condition",
                &[("condition", condition)],
                f64::from(u8::from(o.queue_condition == condition)),
            );
        }
    }

    if let Some(u) = &input.usage {
        let flag = |b: bool| f64::from(u8::from(b));
        w.sample("tsls_usage_journal_healthy", &[], flag(u.journal_healthy));
        w.sample(
            "tsls_usage_journal_admitting",
            &[],
            flag(u.journal_admitting),
        );
        w.sample(
            "tsls_usage_journal_pending_events",
            &[],
            u.journal_pending_events as f64,
        );
        w.sample(
            "tsls_usage_journal_pending_bytes",
            &[],
            u.journal_pending_bytes as f64,
        );
        w.sample(
            "tsls_usage_journal_max_events",
            &[],
            u.journal_max_events as f64,
        );
        w.sample(
            "tsls_usage_journal_max_bytes",
            &[],
            u.journal_max_bytes as f64,
        );
        w.sample(
            "tsls_usage_unjournaled_events_total",
            &[],
            u.unjournaled_events as f64,
        );
        w.sample(
            "tsls_usage_collector_runs_total",
            &[],
            u.collector_runs as f64,
        );
        w.sample(
            "tsls_usage_collector_failing",
            &[],
            flag(u.collector_failing),
        );
        if let Some(at) = u.collector_last_success_at {
            w.sample(
                "tsls_usage_collector_last_success_age_seconds",
                &[],
                ((input.now - at).num_milliseconds().max(0) as f64) / 1000.0,
            );
        }
        w.sample(
            "tsls_usage_collector_delivered_events_total",
            &[],
            u.collector_delivered as f64,
        );
        if let Some(v) = u.ledger_events {
            w.sample("tsls_usage_ledger_events", &[], v as f64);
        }
        if let Some(v) = u.ledger_duplicates_ignored {
            w.sample("tsls_usage_ledger_duplicates_ignored_total", &[], v as f64);
        }
    }

    if let Some(d) = &input.dispatch {
        use crate::metrics::dispatch::{
            DEAD_LETTER_REASONS, DELIVERY_OUTCOMES, QUEUE_OPERATIONS, REAPER_ACTIONS, RETRY_KINDS,
        };
        for outcome in DELIVERY_OUTCOMES {
            w.sample(
                "tsls_async_dispatch_deliveries_total",
                &[("outcome", outcome)],
                d.deliveries.get(outcome).copied().unwrap_or(0) as f64,
            );
        }
        for operation in QUEUE_OPERATIONS {
            for result in ["ok", "error"] {
                w.sample(
                    "tsls_async_dispatch_queue_operations_total",
                    &[("operation", operation), ("result", result)],
                    d.queue_operations
                        .get(&(operation, result))
                        .copied()
                        .unwrap_or(0) as f64,
                );
            }
        }
        w.sample(
            "tsls_async_dispatch_runs_in_flight",
            &[],
            d.in_flight as f64,
        );
        for kind in RETRY_KINDS {
            w.sample(
                "tsls_async_retries_scheduled_total",
                &[("kind", kind)],
                d.retries.get(kind).copied().unwrap_or(0) as f64,
            );
        }
        for reason in DEAD_LETTER_REASONS {
            w.sample(
                "tsls_async_dead_letters_total",
                &[("reason", reason)],
                d.dead_letters.get(reason).copied().unwrap_or(0) as f64,
            );
        }
        w.sample("tsls_async_redrives_total", &[], d.redrives as f64);
        for action in REAPER_ACTIONS {
            w.sample(
                "tsls_async_reaper_actions_total",
                &[("action", action)],
                d.reaper.get(action).copied().unwrap_or(0) as f64,
            );
        }
    }

    for (family, folded) in [
        ("revision", folded_revisions),
        ("tenant", folded_tenants),
        ("environment", folded_envs),
    ] {
        w.sample(
            "tsls_metrics_series_truncated",
            &[("dimension", family)],
            folded as f64,
        );
    }
    w.out
}
