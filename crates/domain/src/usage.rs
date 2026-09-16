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
    /// Monotonic per environment.
    pub sequence: u64,
    pub observed_at: Timestamp,
    /// Duration since the matching start event, when applicable.
    pub monotonic_duration_ms: Option<u64>,
    pub memory_mib: u32,
    pub cpu_millis: u32,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub meter_version: u32,
    pub evidence_quality: EvidenceQuality,
}
