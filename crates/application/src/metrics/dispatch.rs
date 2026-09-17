//! Asynchronous dispatcher counters (PLT-4640, docs/metrics.md §3.9).
//!
//! Recorded by [`crate::services::invoke_async::AsyncDispatcher`] and
//! [`crate::services::invoke_async::DeadLetterService`]; rendered on
//! `GET /metrics` only on a gateway that runs the dispatcher. Every label
//! value is a fixed string (no tenant, function or invocation ids).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// Outcomes of one delivery (`tsls_async_dispatch_deliveries_total{outcome}`).
pub const DELIVERY_OUTCOMES: [&str; 10] = [
    "completed",
    "rescheduled",
    "dead_lettered",
    "skipped_terminal",
    "skipped_stale",
    "skipped_claimed",
    "not_due",
    "poison",
    "failed",
    "lost_claim",
];

/// Queue operations (`tsls_async_dispatch_queue_operations_total{operation,result}`).
pub const QUEUE_OPERATIONS: [&str; 3] = ["ack", "nak", "term"];

/// Kinds of scheduled next tries (`tsls_async_retries_scheduled_total{kind}`).
pub const RETRY_KINDS: [&str; 2] = ["retry", "deferral"];

/// Dead-letter reasons (`tsls_async_dead_letters_total{reason}`).
pub const DEAD_LETTER_REASONS: [&str; 6] = [
    "non_retryable",
    "attempts_exhausted",
    "expired",
    "function_deleted",
    "revision_unavailable",
    "poison",
];

/// Reaper actions (`tsls_async_reaper_actions_total{action}`).
pub const REAPER_ACTIONS: [&str; 5] = [
    "abandoned",
    "republished",
    "dead_lettered",
    "lost",
    "failed",
];

#[derive(Debug, Default)]
pub struct AsyncDispatchMetrics {
    counters: Mutex<BTreeMap<(&'static str, &'static str, &'static str), u64>>,
    in_flight: AtomicU64,
}

/// A copy for rendering.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AsyncDispatchSnapshot {
    pub deliveries: BTreeMap<&'static str, u64>,
    pub queue_operations: BTreeMap<(&'static str, &'static str), u64>,
    pub retries: BTreeMap<&'static str, u64>,
    pub dead_letters: BTreeMap<&'static str, u64>,
    pub redrives: u64,
    pub reaper: BTreeMap<&'static str, u64>,
    pub in_flight: u64,
}

impl AsyncDispatchMetrics {
    fn add(&self, family: &'static str, a: &'static str, b: &'static str) {
        *self.counters.lock().entry((family, a, b)).or_default() += 1;
    }

    pub fn delivery(&self, outcome: &'static str) {
        self.add("delivery", outcome, "");
    }

    pub fn queue_operation(&self, operation: &'static str, ok: bool) {
        self.add("queue", operation, if ok { "ok" } else { "error" });
    }

    pub fn retry_scheduled(&self, counted: bool) {
        self.add("retry", if counted { "retry" } else { "deferral" }, "");
    }

    pub fn dead_letter(&self, reason: &'static str) {
        self.add("dead_letter", reason, "");
    }

    pub fn redrive(&self) {
        self.add("redrive", "", "");
    }

    pub fn reaper(&self, action: &'static str, n: usize) {
        if n == 0 {
            return;
        }
        *self
            .counters
            .lock()
            .entry(("reaper", action, ""))
            .or_default() += n as u64;
    }

    pub fn run_started(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
    }

    pub fn run_finished(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> AsyncDispatchSnapshot {
        let counters = self.counters.lock();
        let mut s = AsyncDispatchSnapshot {
            in_flight: self.in_flight.load(Ordering::SeqCst),
            ..AsyncDispatchSnapshot::default()
        };
        for ((family, a, b), n) in counters.iter() {
            match *family {
                "delivery" => {
                    s.deliveries.insert(a, *n);
                }
                "queue" => {
                    s.queue_operations.insert((a, b), *n);
                }
                "retry" => {
                    s.retries.insert(a, *n);
                }
                "dead_letter" => {
                    s.dead_letters.insert(a, *n);
                }
                "redrive" => s.redrives = *n,
                "reaper" => {
                    s.reaper.insert(a, *n);
                }
                _ => {}
            }
        }
        s
    }
}
