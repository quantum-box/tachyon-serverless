//! Host-observed usage facts (RFC §16.1). Not a tariff; metering only.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::ids::{AttemptId, EnvironmentId, InvocationId, TenantId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageEventType {
    EnvironmentStarted,
    HandlerStarted,
    HandlerFinished,
    EnvironmentStopped,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    /// Unique per event; duplicates (re-sends) must be de-duplicated by this id.
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
    pub observed_at: Timestamp,
    /// Duration since the matching start event, when applicable:
    /// `HandlerFinished` measures the handler, and `EnvironmentStopped` the
    /// whole life of the environment (its ledger `created_at` to the moment it
    /// ended) — the same quantity whoever ended it.
    pub monotonic_duration_ms: Option<u64>,
    pub memory_mib: u32,
    pub cpu_millis: u32,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub meter_version: u32,
    pub evidence_quality: EvidenceQuality,
}
