//! Reuse and scaling metrics (PLT-4637, docs/metrics.md, docs/adr/0010).
//!
//! Two kinds of numbers end up on `GET /metrics`:
//!
//! - **state read at scrape time**: environments by state, reservations,
//!   queue, per-tenant waits, breakers, the configuration cache, the
//!   dispatcher lease and per-environment host usage. They come from the
//!   admission state machine ([`crate::services::admission::AdmissionMetrics`]),
//!   the pool, the cache and the provider, and nothing here duplicates them;
//! - **events** recorded by the code that makes them happen: attempts by
//!   start kind with their phase durations, boot identity checks, invoke-gate
//!   refusals, dispatcher heartbeats and the idle-environment CPU derived from
//!   successive samples. They live in [`Metrics`].
//!
//! Everything is hand-rolled (a few counters and fixed-bucket histograms
//! behind one lock) to avoid a metrics dependency for about forty series
//! families. Rendering is [`render::render`], a pure function of
//! [`render::MetricsInput`], so the exposition format is testable without a
//! gateway.

pub mod catalog;
pub mod dispatch;
pub mod render;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use parking_lot::Mutex;

use tachyon_serverless_domain::{AttemptTimings, EnvironmentId, StartKind};

/// Histogram bucket upper bounds, seconds. Chosen to separate a warm dispatch
/// (milliseconds), a process cold start (tens to hundreds of ms) and a
/// nested-virtualisation microVM boot (seconds).
pub const PHASE_BUCKETS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
];

/// Environments whose first boot id is remembered for the reuse check.
const MAX_TRACKED_BOOTS: usize = 4096;

/// A cumulative histogram with [`PHASE_BUCKETS`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Histogram {
    /// Count per bucket (not cumulative); the last slot is `+Inf`.
    pub buckets: [u64; PHASE_BUCKETS.len() + 1],
    pub sum: f64,
    pub count: u64,
}

impl Histogram {
    pub fn observe(&mut self, seconds: f64) {
        let slot = PHASE_BUCKETS
            .iter()
            .position(|b| seconds <= *b)
            .unwrap_or(PHASE_BUCKETS.len());
        self.buckets[slot] += 1;
        self.sum += seconds;
        self.count += 1;
    }
}

/// A phase of one attempt, as timed by the host (`AttemptTimings`).
pub const PHASES: [&str; 6] = ["queue_wait", "boot", "init", "resume", "handler", "total"];

pub fn start_kind_label(kind: StartKind) -> &'static str {
    match kind {
        StartKind::Cold => "cold",
        StartKind::Warm => "warm",
        StartKind::Restored => "restored",
    }
}

/// Result of comparing an attempt's guest boot id with the one its
/// environment first reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BootCheck {
    /// The environment's first dispatch: its boot id is remembered.
    FirstBoot,
    /// A later dispatch into the same environment reported the same boot id:
    /// the same guest kernel served it (reuse proven by boot identity).
    SameBoot,
    /// A later dispatch reported a different boot id for the same
    /// environment id: must never happen.
    BootChanged,
    /// No boot id (the process provider has no guest kernel).
    Unreported,
}

impl BootCheck {
    pub const ALL: [BootCheck; 4] = [
        BootCheck::FirstBoot,
        BootCheck::SameBoot,
        BootCheck::BootChanged,
        BootCheck::Unreported,
    ];
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FirstBoot => "first_boot",
            Self::SameBoot => "same_boot",
            Self::BootChanged => "boot_changed",
            Self::Unreported => "unreported",
        }
    }
}

/// Idle-environment CPU derived from two successive host samples.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EnvSample {
    pub at: Instant,
    pub cpu_seconds: f64,
    pub idle: bool,
}

/// What one round of samples says about idle environments.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct IdleCpuRound {
    /// Idle environments with a usable previous sample.
    pub sampled: u64,
    /// Highest `cpu seconds / wall seconds` over them (0 when none).
    pub max_ratio: f64,
}

#[derive(Debug, Default)]
struct Inner {
    attempts: BTreeMap<(&'static str, &'static str), u64>,
    phases: BTreeMap<(&'static str, &'static str), Histogram>,
    boot_checks: BTreeMap<BootCheck, u64>,
    boots: HashMap<EnvironmentId, Option<String>>,
    boot_order: VecDeque<EnvironmentId>,
    gate_refusals: BTreeMap<&'static str, u64>,
    heartbeats: BTreeMap<&'static str, u64>,
    slot_lease_renewals: u64,
    samples: HashMap<EnvironmentId, EnvSample>,
    idle_cpu_seconds: f64,
    idle_round: IdleCpuRound,
}

/// Event counters and histograms (see the module docs).
#[derive(Debug, Default)]
pub struct Metrics {
    inner: Mutex<Inner>,
}

/// A copy of [`Metrics`] for rendering.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetricsSnapshot {
    pub attempts: BTreeMap<(&'static str, &'static str), u64>,
    pub phases: BTreeMap<(&'static str, &'static str), Histogram>,
    pub boot_checks: BTreeMap<BootCheck, u64>,
    pub gate_refusals: BTreeMap<&'static str, u64>,
    pub heartbeats: BTreeMap<&'static str, u64>,
    pub slot_lease_renewals: u64,
    pub idle_cpu_seconds: f64,
    pub idle_round: IdleCpuRound,
}

impl Metrics {
    /// One finished attempt: its start kind, final status and host timings,
    /// and the boot id its environment reported (`None` for providers without
    /// a guest kernel). Returns the boot identity check.
    pub fn observe_attempt(
        &self,
        environment: &EnvironmentId,
        start_kind: StartKind,
        status: &'static str,
        timings: &AttemptTimings,
        guest_boot_id: Option<&str>,
    ) -> BootCheck {
        let kind = start_kind_label(start_kind);
        let mut inner = self.inner.lock();
        *inner.attempts.entry((kind, status)).or_default() += 1;
        let ms = |v: Option<u64>| v.map(|ms| ms as f64 / 1000.0);
        // Boot and init are zero on a warm start because nothing booted;
        // they are recorded only for starts that booted, so a warm start does
        // not pull the boot histogram towards zero.
        let booted = start_kind != StartKind::Warm;
        let phases = [
            ("queue_wait", ms(timings.queue_wait_ms)),
            ("boot", ms(timings.environment_boot_ms).filter(|_| booted)),
            ("init", ms(timings.runtime_init_ms).filter(|_| booted)),
            ("resume", ms(timings.resume_ms)),
            ("handler", ms(timings.handler_ms)),
            ("total", ms(timings.total_ms)),
        ];
        for (phase, value) in phases {
            if let Some(seconds) = value {
                inner
                    .phases
                    .entry((phase, kind))
                    .or_default()
                    .observe(seconds);
            }
        }
        let check = match guest_boot_id {
            None => BootCheck::Unreported,
            Some(id) => match inner.boots.get(environment) {
                Some(Some(first)) if first == id => BootCheck::SameBoot,
                Some(Some(_)) => BootCheck::BootChanged,
                _ => {
                    if inner.boots.len() >= MAX_TRACKED_BOOTS
                        && let Some(oldest) = inner.boot_order.pop_front()
                    {
                        inner.boots.remove(&oldest);
                    }
                    inner
                        .boots
                        .insert(environment.clone(), Some(id.to_string()));
                    inner.boot_order.push_back(environment.clone());
                    BootCheck::FirstBoot
                }
            },
        };
        *inner.boot_checks.entry(check).or_default() += 1;
        check
    }

    /// The invoke gate refused new work or a cold start (PLT-4636).
    pub fn gate_refusal(&self, error_type: &'static str) {
        *self
            .inner
            .lock()
            .gate_refusals
            .entry(error_type)
            .or_default() += 1;
    }

    /// One dispatcher heartbeat: `renewed` (with the slot leases it renewed),
    /// `fenced` or `error`.
    pub fn heartbeat(&self, result: &'static str, slot_leases: usize) {
        let mut inner = self.inner.lock();
        *inner.heartbeats.entry(result).or_default() += 1;
        inner.slot_lease_renewals += slot_leases as u64;
    }

    /// A round of host samples of every live environment. An environment
    /// that was idle in both this and its previous sample contributes its CPU
    /// time in between to the idle counter and its CPU/wall ratio to the
    /// round's maximum. Environments missing from the round are forgotten.
    pub fn sample_round(&self, samples: &[(EnvironmentId, EnvSample)]) -> IdleCpuRound {
        let mut inner = self.inner.lock();
        let mut round = IdleCpuRound::default();
        let mut next = HashMap::with_capacity(samples.len());
        for (id, sample) in samples {
            if let Some(prev) = inner.samples.get(id)
                && prev.idle
                && sample.idle
            {
                let wall = sample.at.saturating_duration_since(prev.at).as_secs_f64();
                let cpu = (sample.cpu_seconds - prev.cpu_seconds).max(0.0);
                inner.idle_cpu_seconds += cpu;
                if wall > 0.0 {
                    round.sampled += 1;
                    round.max_ratio = round.max_ratio.max(cpu / wall);
                }
            }
            next.insert(id.clone(), *sample);
        }
        inner.samples = next;
        inner.idle_round = round;
        round
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let inner = self.inner.lock();
        MetricsSnapshot {
            attempts: inner.attempts.clone(),
            phases: inner.phases.clone(),
            boot_checks: inner.boot_checks.clone(),
            gate_refusals: inner.gate_refusals.clone(),
            heartbeats: inner.heartbeats.clone(),
            slot_lease_renewals: inner.slot_lease_renewals,
            idle_cpu_seconds: inner.idle_cpu_seconds,
            idle_round: inner.idle_round,
        }
    }
}

#[cfg(test)]
mod tests;
