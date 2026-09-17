//! The maximum charge a run can be rated at (PLT-4643, docs/adr/0016 §2).
//!
//! A reservation must be at least what PLT-4642 rating can charge for the
//! run's `AttemptSettled` events, so it is computed with the *same* price
//! table and the same per-component rounding (`rating::charge`) from upper
//! bounds of every rated quantity:
//!
//! - **billable ms** = Σ over the table's billable segments of that segment's
//!   bound, times the attempts that can carry it, capped by the run window,
//!   plus `reservation_slack_ms` (attempts are serial, so the sum is also
//!   capped by the run window + cancel grace):
//!   - `queue_wait_ms` ≤ queue timeout; `vm_base_boot_ms` ≤ run window;
//!   - `user_init_ms` ≤ initialization timeout + handshake timeout;
//!   - `handler_ms` ≤ execution timeout + cancel grace;
//!   - `teardown_ms` and `idle_pooled_ms` have **no** bound a reservation can
//!     know (teardown after the deadline, idle time before the claim): a
//!     price table that bills them cannot be used with budgets.
//!   - attempts: a run has at most two (the cold retry after an undelivered
//!     warm dispatch). The first of those never ran user init (warm) nor the
//!     handler (undelivered), so with only `user_init_ms` / `handler_ms`
//!     billable one attempt's bound suffices; otherwise two.
//! - **vCPU-ms / MiB-ms** = billable ms × requested `cpu_millis` /
//!   `memory_mib` (rating uses the requested resources);
//! - **transfer bytes** = request bytes × 2 attempts + max response bytes;
//! - **invocations** = 1.
//!
//! Anything measured above the reservation is still settled at its measured
//! value (never released below what was measured) and counted as overrun.

use super::super::usage::rating::{
    BYTES_PER_GB, MIB_MS_PER_GIB_SECOND, PriceTable, VCPU_MILLI_MS_PER_VCPU_SECOND, charge,
};

/// Segments whose duration a reservation cannot bound.
pub const UNBOUNDED_SEGMENTS: [&str; 2] = ["teardown_ms", "idle_pooled_ms"];

/// Inputs of [`max_charge`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunBounds {
    pub cpu_millis: u32,
    pub memory_mib: u32,
    /// Acceptance (or run start) to the run deadline.
    pub window_ms: u64,
    pub queue_timeout_ms: u64,
    pub init_timeout_ms: u64,
    pub handshake_timeout_ms: u64,
    pub execution_timeout_ms: u64,
    pub cancel_grace_ms: u64,
    pub request_bytes: u64,
    pub max_response_bytes: u64,
    pub slack_ms: u64,
}

/// The maximum charge and the quantities it was computed from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MaxCharge {
    pub billable_ms: u64,
    pub transfer_bytes: u64,
    pub vcpu_micros: u64,
    pub memory_micros: u64,
    pub invocation_micros: u64,
    pub transfer_micros: u64,
    pub total_micros: u64,
}

/// Whether `table` can be used with budgets.
pub fn check_table(table: &PriceTable) -> Result<(), String> {
    for s in &table.billable_segments {
        if UNBOUNDED_SEGMENTS.contains(&s.as_str()) {
            return Err(format!(
                "[budget] price table {} bills `{s}`, whose duration no reservation can bound; \
                 budgets need a table without it",
                table.version
            ));
        }
    }
    Ok(())
}

pub fn max_charge(table: &PriceTable, b: &RunBounds) -> MaxCharge {
    let mut per_attempt: u64 = 0;
    let mut only_single_attempt_segments = true;
    for s in &table.billable_segments {
        let bound = match s.as_str() {
            "queue_wait_ms" => b.queue_timeout_ms,
            "vm_base_boot_ms" => b.window_ms,
            "user_init_ms" => b.init_timeout_ms.saturating_add(b.handshake_timeout_ms),
            "handler_ms" => b.execution_timeout_ms.saturating_add(b.cancel_grace_ms),
            // Refused by `check_table`; bounded by the window if it slips by.
            _ => b.window_ms,
        };
        if !matches!(s.as_str(), "user_init_ms" | "handler_ms") {
            only_single_attempt_segments = false;
        }
        per_attempt = per_attempt.saturating_add(bound);
    }
    let attempts: u64 = if only_single_attempt_segments { 1 } else { 2 };
    let billable_ms = per_attempt
        .saturating_mul(attempts)
        // Attempts run one after the other inside the run window.
        .min(b.window_ms.saturating_add(b.cancel_grace_ms))
        .saturating_add(b.slack_ms);
    let transfer_bytes = b
        .request_bytes
        .saturating_mul(2)
        .saturating_add(b.max_response_bytes);
    let p = table.unit_prices_micros;
    let vcpu_micros = charge(
        u128::from(billable_ms) * u128::from(b.cpu_millis),
        p.vcpu_second,
        VCPU_MILLI_MS_PER_VCPU_SECOND,
    );
    let memory_micros = charge(
        u128::from(billable_ms) * u128::from(b.memory_mib),
        p.gib_second,
        MIB_MS_PER_GIB_SECOND,
    );
    let invocation_micros = charge(1, p.invocation, 1);
    let transfer_micros = charge(u128::from(transfer_bytes), p.gb_transferred, BYTES_PER_GB);
    MaxCharge {
        billable_ms,
        transfer_bytes,
        vcpu_micros,
        memory_micros,
        invocation_micros,
        transfer_micros,
        total_micros: vcpu_micros
            .saturating_add(memory_micros)
            .saturating_add(invocation_micros)
            .saturating_add(transfer_micros),
    }
}
