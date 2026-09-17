//! One `EnvironmentStopped` per environment, whoever ends it (PLT-4642,
//! docs/adr/0012 「回収された環境の計量」).
//!
//! An environment can end on many paths: its driver, its pool, a reclaim by
//! another dispatcher after the owner lost its lease, the startup reconcile
//! of the next process. Before this module only the first two reported it,
//! so the host cost of every environment a reclaim or a reconcile ended was
//! never metered.
//!
//! The rules every path follows:
//!
//! - **One id.** The event id is
//!   [`environment_stopped_event_id`](tachyon_serverless_domain::environment_stopped_event_id),
//!   derived from the environment id alone. An old owner that settles late
//!   and the reclaimer that already ended the environment derive the same id,
//!   so the usage ledger (primary key `event_id`) keeps one.
//! - **Sample, then terminate.** Whoever terminates reads
//!   `ExecutionProvider::environment_stats` first, while the provider can
//!   still see the VMM (on Firecracker its cgroup, even when another process
//!   started it). What the provider cannot see is `unknown`, never estimated.
//! - **Emit, then confirm.** A reclaimer appends the event after the provider
//!   confirmed the terminate and *before* it settles the ledger row: a crash
//!   in between leaves the row fenced, the next pass terminates again
//!   (idempotent) and emits the same id again, which the ledger drops.
//! - **The lifetime comes from the ledger.** `created_at` to the moment the
//!   environment was seen to end, with the clock that read the end recorded
//!   ([`LifetimeSource`]). An end nobody observed (the provider no longer
//!   tracks the environment) has no lifetime.

use std::time::Duration;

use tachyon_serverless_domain::{
    ExecutionEnvironment, LifetimeSource, Metered, STOP_SEQUENCE_UNKNOWN, StoppedBy, Timestamp,
    UsageEvent, UsageEventType, UsageSegments, environment_stopped_event_id,
};
use tachyon_serverless_provider_port::{EnvironmentStats, UsageSink};

use crate::repository::Repositories;
use crate::services::invoke::usage_resources;
use crate::services::pool::environment_lifetime_ms;

/// Milliseconds, rounded up (a teardown that happened is never 0).
fn ceil_ms(d: Duration) -> u64 {
    d.as_micros().div_ceil(1000) as u64
}

/// What a path that did not drive the environment knows about its end.
#[derive(Debug, Clone)]
pub struct ReclaimedStop<'a> {
    /// The ledger row as the path read it (the fenced row for a reclaim).
    pub env: &'a ExecutionEnvironment,
    pub stopped_by: StoppedBy,
    /// The provider's host sample read just before the terminate.
    pub host_sample: Option<EnvironmentStats>,
    /// How long the terminate took, when this path terminated it.
    pub teardown: Option<Duration>,
    /// When the end was observed (this dispatcher's clock). `None` when
    /// nobody saw it end: the lifetime is then unknown.
    pub ended_at: Option<Timestamp>,
    /// The wall-clock reading of this dispatcher, used to place the event on
    /// a day.
    pub now: Timestamp,
}

/// The `EnvironmentStopped` of an environment ended by a reclaim or the
/// startup reconcile.
pub fn reclaimed_stop_event(repos: &Repositories, stop: &ReclaimedStop<'_>) -> UsageEvent {
    let env = stop.env;
    let revision = repos.revisions.get(&env.revision_id).ok().flatten();
    let resources = revision
        .as_ref()
        .map(|r| r.spec.resources)
        .unwrap_or_default();
    let mut event = UsageEvent::new(
        environment_stopped_event_id(&env.id),
        env.tenant_id.clone(),
        env.id.clone(),
        UsageEventType::EnvironmentStopped,
        STOP_SEQUENCE_UNKNOWN,
        stop.now,
    );
    event.memory_mib = resources.memory_mib;
    event.cpu_millis = resources.cpu_millis;
    event.function_id = revision.as_ref().map(|r| r.function_id.clone());
    event.revision_id = Some(env.revision_id.clone());
    event.epoch = env.epoch;
    event.boot_id = env.evidence.guest_boot_id.clone();
    event.monotonic_duration_ms = stop.ended_at.map(|t| environment_lifetime_ms(env, t));
    event.lifetime_source = match stop.ended_at {
        Some(_) => LifetimeSource::LedgerReclaimerClock,
        None => LifetimeSource::Unknown,
    };
    // Nothing of the attempt or the pool is known to a reclaimer: those
    // segments stay unknown. The teardown is what this path timed itself.
    event.segments = UsageSegments {
        teardown_ms: stop
            .teardown
            .map_or_else(Metered::unknown, |d| Metered::host(ceil_ms(d))),
        ..UsageSegments::default()
    };
    event.resources = usage_resources(&resources, stop.host_sample.as_ref());
    event.stopped_by = Some(stop.stopped_by);
    event
}

/// Build and record [`reclaimed_stop_event`].
pub async fn record_reclaimed_stop(
    repos: &Repositories,
    usage: &dyn UsageSink,
    stop: ReclaimedStop<'_>,
) {
    let event = reclaimed_stop_event(repos, &stop);
    tracing::info!(
        environment_id = %stop.env.id,
        stopped_by = stop.stopped_by.as_str(),
        cgroup_cpu_usec = event.resources.cgroup_cpu_usec.measurement.as_str(),
        lifetime_ms = ?event.monotonic_duration_ms,
        "environment stop metered by a path that did not drive it"
    );
    usage.record(event).await;
}
