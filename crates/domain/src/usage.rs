//! Host-observed usage facts (RFC §16.1). Not a tariff; metering only.
//!
//! Schema version 2 (PLT-4642, docs/adr/0012): every event says *which*
//! function, revision, invocation and attempt it belongs to, how the attempt
//! ended, and for each quantity *who measured it* ([`Measurement`]). Rating
//! (`crates/application/src/usage/rating.rs`) only ever reads
//! [`Measurement::HostMeasured`] and [`Measurement::ProviderReported`]
//! quantities: a value the guest reported, or one nobody measured, contributes
//! nothing to a provisional charge and is counted as unmetered instead.
//!
//! Version 1 events (no `segments`, `resources`, `bytes`) still deserialize:
//! every new field has a default that reads as "unknown".

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::ids::{AttemptId, EnvironmentId, FunctionId, InvocationId, RevisionId, TenantId};

/// The schema version new events carry in [`UsageEvent::meter_version`].
pub const USAGE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageEventType {
    EnvironmentStarted,
    HandlerStarted,
    HandlerFinished,
    /// One attempt is settled: its outcome and every host-measured segment of
    /// it (queue, boot, init, handler, teardown). The only event type rating
    /// charges for (schema v2).
    AttemptSettled,
    EnvironmentStopped,
}

impl UsageEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EnvironmentStarted => "environment_started",
            Self::HandlerStarted => "handler_started",
            Self::HandlerFinished => "handler_finished",
            Self::AttemptSettled => "attempt_settled",
            Self::EnvironmentStopped => "environment_stopped",
        }
    }
}

/// Quality of the evidence behind a usage event. Guest self-reports are
/// never the sole basis for metering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceQuality {
    HostObserved,
    GuestReported,
    Unknown,
}

/// Who measured one quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Measurement {
    /// The gateway process measured it with the host's monotonic clock (or
    /// counted the bytes itself).
    HostMeasured,
    /// The execution provider read it on the host, outside the guest (cgroup
    /// v2 `cpu.stat` / `memory.peak` of the VMM).
    ProviderReported,
    /// The guest said so. Recorded for diagnosis, never rated.
    GuestReported,
    /// Nobody measured it. Never estimated, never rated.
    #[default]
    Unknown,
}

impl Measurement {
    /// Whether rating may use a quantity measured this way.
    pub fn ratable(&self) -> bool {
        matches!(self, Self::HostMeasured | Self::ProviderReported)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::HostMeasured => "host_measured",
            Self::ProviderReported => "provider_reported",
            Self::GuestReported => "guest_reported",
            Self::Unknown => "unknown",
        }
    }
}

/// One metered quantity and who measured it. `value` is `None` exactly when
/// the measurement is [`Measurement::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Metered {
    #[serde(default)]
    pub value: Option<u64>,
    #[serde(default)]
    pub measurement: Measurement,
}

impl Metered {
    pub fn host(value: u64) -> Self {
        Self {
            value: Some(value),
            measurement: Measurement::HostMeasured,
        }
    }

    pub fn provider(value: u64) -> Self {
        Self {
            value: Some(value),
            measurement: Measurement::ProviderReported,
        }
    }

    pub fn guest(value: u64) -> Self {
        Self {
            value: Some(value),
            measurement: Measurement::GuestReported,
        }
    }

    pub fn unknown() -> Self {
        Self::default()
    }

    /// `Some(host measured)` or `Unknown` when not measured.
    pub fn host_opt(value: Option<u64>) -> Self {
        value.map_or_else(Self::unknown, Self::host)
    }

    /// The value rating may use: host-measured or provider-reported only.
    pub fn ratable_value(&self) -> Option<u64> {
        match self.measurement.ratable() {
            true => self.value,
            false => None,
        }
    }

    pub fn is_unknown(&self) -> bool {
        self.ratable_value().is_none()
    }
}

/// How an attempt came to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptKind {
    /// The first dispatch of an invocation.
    First,
    /// A later dispatch of the same invocation (today: the cold retry after
    /// a pooled environment was gone before the `Invoke` frame reached it).
    Retry,
}

impl AttemptKind {
    pub fn from_number(number: u32) -> Self {
        match number {
            0 | 1 => Self::First,
            _ => Self::Retry,
        }
    }
}

/// How an attempt ended, as far as usage is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageOutcome {
    Succeeded,
    Failed,
    Timeout,
    Cancelled,
    OutcomeUnknown,
}

impl UsageOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

/// The host-measured parts of an attempt (or of an environment's end), in
/// milliseconds rounded **up** from the monotonic clock. A segment that did
/// not happen is `host(0)`; one that happened but was not measured is
/// `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UsageSegments {
    /// Acceptance to the capacity grant.
    #[serde(default)]
    pub queue_wait_ms: Metered,
    /// Provider create to the bridge connection (cold); resume and readiness
    /// check of a pooled environment (warm).
    #[serde(default)]
    pub vm_base_boot_ms: Metered,
    /// Bridge connection to `Ready`: handshake and the user's initialization.
    #[serde(default)]
    pub user_init_ms: Metered,
    /// `Invoke` frame written to the result (or the timeout / cancel).
    #[serde(default)]
    pub handler_ms: Metered,
    /// Result to the environment being terminated (or handed to the pool).
    #[serde(default)]
    pub teardown_ms: Metered,
    /// Time the environment spent quiesced in the pool before this event.
    #[serde(default)]
    pub idle_pooled_ms: Metered,
}

impl UsageSegments {
    /// `(name, segment)` in a fixed order.
    pub fn named(&self) -> [(&'static str, Metered); 6] {
        [
            ("queue_wait_ms", self.queue_wait_ms),
            ("vm_base_boot_ms", self.vm_base_boot_ms),
            ("user_init_ms", self.user_init_ms),
            ("handler_ms", self.handler_ms),
            ("teardown_ms", self.teardown_ms),
            ("idle_pooled_ms", self.idle_pooled_ms),
        ]
    }

    pub fn get(&self, name: &str) -> Option<Metered> {
        self.named()
            .into_iter()
            .find(|(n, _)| *n == name)
            .map(|(_, m)| m)
    }
}

/// Resources of the environment: what was requested (from the revision) and
/// what the provider observed on the host, when it can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UsageResources {
    #[serde(default)]
    pub requested_cpu_millis: u32,
    #[serde(default)]
    pub requested_memory_mib: u32,
    #[serde(default)]
    pub requested_storage_mib: u32,
    /// cgroup v2 `cpu.stat usage_usec` of the VMM at teardown.
    #[serde(default)]
    pub cgroup_cpu_usec: Metered,
    /// cgroup v2 `memory.peak` of the VMM at teardown, bytes.
    #[serde(default)]
    pub cgroup_memory_peak_bytes: Metered,
}

/// Payload bytes counted by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UsageBytes {
    #[serde(default)]
    pub request_bytes: Metered,
    #[serde(default)]
    pub response_bytes: Metered,
}

/// What the guest said about itself. Kept for diagnosis; never rated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GuestReportedUsage {
    #[serde(default)]
    pub guest_init_ms: Option<u64>,
    #[serde(default)]
    pub guest_handler_ms: Option<u64>,
}

/// Where `observed_at` came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WallClockSource {
    /// The gateway's injected clock (`Clock`), i.e. the host's wall clock.
    /// Used to place the event on a day, never to compute a quantity.
    HostClock,
    #[default]
    Unknown,
}

/// Which path ended the environment an `EnvironmentStopped` reports
/// (docs/adr/0012 「回収された環境」). Every path emits the same event id
/// ([`environment_stopped_event_id`]), so whichever reaches the journal first
/// is the one the ledger keeps and a second attempt is a duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoppedBy {
    /// The dispatcher that drove it (the invoke driver).
    Owner,
    /// The pool of the dispatcher that owned it (TTL sweep, drain, retire).
    Pool,
    /// Another dispatcher (or the next incarnation of the owner) after the
    /// owner lost its lease: fence, provider terminate, settle `Lost`.
    Reclaim,
    /// The startup reconcile: an orphan still running on the host, or an
    /// environment the provider no longer tracks.
    Reconcile,
}

impl StoppedBy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Pool => "pool",
            Self::Reclaim => "reclaim",
            Self::Reconcile => "reconcile",
        }
    }
}

/// Where [`UsageEvent::monotonic_duration_ms`] of an `EnvironmentStopped`
/// came from. The lifetime is always the ledger's `created_at` to the moment
/// the environment ended, i.e. the difference of two wall-clock readings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LifetimeSource {
    /// `created_at` and the end were both read from the wall clock of the
    /// dispatcher that created the environment.
    LedgerOwnerClock,
    /// `created_at` from the owner's wall clock, the end from the reclaiming
    /// dispatcher's: the two may differ by up to `max_clock_skew_ms`.
    LedgerReclaimerClock,
    /// Not measured (the end was not observed, or an event written before
    /// this field existed).
    #[default]
    Unknown,
}

/// The single id of an environment's `EnvironmentStopped`, whoever emits it.
///
/// An environment id is minted once and an environment ends once, so the id
/// needs neither the epoch (a reclaim moves it) nor the sequence (a reclaimer
/// cannot know it): an old owner that settles late and the dispatcher that
/// reclaimed the environment derive the same id, and the ledger (primary key
/// `event_id`) keeps exactly one of them.
pub fn environment_stopped_event_id(environment_id: &EnvironmentId) -> String {
    format!("{environment_id}:environment-stopped")
}

/// The `sequence` of an `EnvironmentStopped` written by a dispatcher that did
/// not drive the environment and so cannot know how many events it had (a
/// reclaim or the startup reconcile). The largest value the ledger's
/// `INTEGER` column holds, so the stop still orders last.
pub const STOP_SEQUENCE_UNKNOWN: u64 = i64::MAX as u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    /// Unique per event; duplicates (re-sends) must be de-duplicated by this id.
    /// Deterministic: `<environment>:<epoch>:<sequence>` (or a fixed suffix),
    /// so re-sending the same fact keeps the same id.
    pub event_id: String,
    pub tenant_id: TenantId,
    pub environment_id: EnvironmentId,
    pub invocation_id: Option<InvocationId>,
    pub attempt_id: Option<AttemptId>,
    pub event_type: UsageEventType,
    /// Monotonic per environment: the host counts the events of one
    /// environment from its first boot to the stop event that ends it, across
    /// every invocation that ran on it. A reused environment continues the
    /// count where the attempt before it stopped — the pool carries it with
    /// the environment — so two events of one environment never share a
    /// sequence, and the events of one environment can be ordered without a
    /// clock.
    pub sequence: u64,
    /// Host wall clock (see [`Self::wall_clock_source`]). Only used to place
    /// the event on a day.
    pub observed_at: Timestamp,
    /// Duration since the matching start event, when applicable:
    /// `HandlerFinished` measures the handler, and `EnvironmentStopped` the
    /// whole life of the environment (its ledger `created_at` to the moment it
    /// ended) — the same quantity whoever ended it. The environment lifetime is
    /// derived from wall-clock timestamps and is reported as host cost only;
    /// rating never reads this field.
    pub monotonic_duration_ms: Option<u64>,
    pub memory_mib: u32,
    pub cpu_millis: u32,
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// Schema version of this event ([`USAGE_SCHEMA_VERSION`]).
    pub meter_version: u32,
    pub evidence_quality: EvidenceQuality,

    // ---- schema v2 (PLT-4642) --------------------------------------------
    #[serde(default)]
    pub function_id: Option<FunctionId>,
    #[serde(default)]
    pub revision_id: Option<RevisionId>,
    /// Attempt number in the invocation ledger (1 = first dispatch).
    #[serde(default)]
    pub attempt_number: Option<u32>,
    #[serde(default)]
    pub attempt_kind: Option<AttemptKind>,
    #[serde(default)]
    pub outcome: Option<UsageOutcome>,
    /// Epoch of the environment assignment the event belongs to.
    #[serde(default)]
    pub epoch: u64,
    /// The guest's boot id from `Hello`, when it sent one. Guest-reported:
    /// identifies the boot for correlation, never proves anything.
    #[serde(default)]
    pub boot_id: Option<String>,
    #[serde(default)]
    pub wall_clock_source: WallClockSource,
    #[serde(default)]
    pub segments: UsageSegments,
    #[serde(default)]
    pub resources: UsageResources,
    #[serde(default)]
    pub bytes: UsageBytes,
    #[serde(default)]
    pub guest_reported: GuestReportedUsage,
    /// `EnvironmentStopped` only: which path ended the environment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_by: Option<StoppedBy>,
    /// `EnvironmentStopped` only: where the lifetime came from.
    #[serde(default)]
    pub lifetime_source: LifetimeSource,
}

impl UsageEvent {
    /// A v2 event with every v2 quantity unknown and no v1 quantity. Callers
    /// fill in what they measured.
    pub fn new(
        event_id: String,
        tenant_id: TenantId,
        environment_id: EnvironmentId,
        event_type: UsageEventType,
        sequence: u64,
        observed_at: Timestamp,
    ) -> Self {
        Self {
            event_id,
            tenant_id,
            environment_id,
            invocation_id: None,
            attempt_id: None,
            event_type,
            sequence,
            observed_at,
            monotonic_duration_ms: None,
            memory_mib: 0,
            cpu_millis: 0,
            bytes_in: 0,
            bytes_out: 0,
            meter_version: USAGE_SCHEMA_VERSION,
            evidence_quality: EvidenceQuality::HostObserved,
            function_id: None,
            revision_id: None,
            attempt_number: None,
            attempt_kind: None,
            outcome: None,
            epoch: 0,
            boot_id: None,
            wall_clock_source: WallClockSource::HostClock,
            segments: UsageSegments::default(),
            resources: UsageResources::default(),
            bytes: UsageBytes::default(),
            guest_reported: GuestReportedUsage::default(),
            stopped_by: None,
            lifetime_source: LifetimeSource::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_host_and_provider_measurements_are_ratable() {
        assert_eq!(Metered::host(5).ratable_value(), Some(5));
        assert_eq!(Metered::provider(7).ratable_value(), Some(7));
        assert_eq!(Metered::guest(9).ratable_value(), None);
        assert_eq!(Metered::unknown().ratable_value(), None);
        assert!(Metered::guest(9).is_unknown());
        assert_eq!(Metered::host_opt(None), Metered::unknown());
    }

    #[test]
    fn a_v1_event_still_deserializes_with_everything_new_unknown() {
        let v1 = serde_json::json!({
            "event_id": "env:1:1",
            "tenant_id": "tn_01hzzzzzzzzzzzzzzzzzzzzzza",
            "environment_id": "env_01hzzzzzzzzzzzzzzzzzzzzzza",
            "invocation_id": null,
            "attempt_id": null,
            "event_type": "handler_finished",
            "sequence": 2,
            "observed_at": "2026-09-17T00:00:00Z",
            "monotonic_duration_ms": 5,
            "memory_mib": 256,
            "cpu_millis": 500,
            "bytes_in": 1,
            "bytes_out": 2,
            "meter_version": 1,
            "evidence_quality": "host_observed"
        });
        let e: UsageEvent = serde_json::from_value(v1).unwrap();
        assert_eq!(e.meter_version, 1);
        assert!(e.segments.handler_ms.is_unknown());
        assert_eq!(e.wall_clock_source, WallClockSource::Unknown);
        let v2 = serde_json::to_value(&e).unwrap();
        assert_eq!(v2["segments"]["handler_ms"]["measurement"], "unknown");
    }

    #[test]
    fn attempt_kind_follows_the_attempt_number() {
        assert_eq!(AttemptKind::from_number(1), AttemptKind::First);
        assert_eq!(AttemptKind::from_number(2), AttemptKind::Retry);
    }
}
