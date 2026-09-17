//! The metric families `GET /metrics` exposes, with their type and help text.
//!
//! One list, used by the renderer (`# HELP` / `# TYPE`), by the test that
//! every family in `docs/metrics.md` exists, and by the test that every
//! alert rule in `deploy/prometheus/alerts.yml` names a real family.

/// `(name, type, help)`. Units are in the names (`_seconds`, `_bytes`,
/// `_millicores`, `_total`).
pub const FAMILIES: &[(&str, &str, &str)] = &[
    // build / provider
    (
        "tsls_build_info",
        "gauge",
        "Gateway version (value is always 1).",
    ),
    (
        "tsls_environment_reuse_mode",
        "gauge",
        "1 for the active mode: warm_reuse (pooled environments are reused) or every_invocation_boots (no warm stage: every invocation boots its own environment).",
    ),
    (
        "tsls_pool_reuse_enabled",
        "gauge",
        "1 when the provider's idle capabilities and [pool] enabled allow environment reuse.",
    ),
    (
        "tsls_pool_held_environments",
        "gauge",
        "Environments whose bridge session the pool holds (idle or being quiesced).",
    ),
    (
        "tsls_pool_quiescing_environments",
        "gauge",
        "Environments on their way into the pool (being quiesced).",
    ),
    // node / reservations
    (
        "tsls_node_info",
        "gauge",
        "The node this gateway schedules on (value is always 1).",
    ),
    (
        "tsls_node_capacity_cpu_millicores",
        "gauge",
        "Configured node CPU ([capacity.node] cpu_millis). Absent when unbounded.",
    ),
    (
        "tsls_node_capacity_memory_bytes",
        "gauge",
        "Configured node memory. Absent when unbounded.",
    ),
    (
        "tsls_node_capacity_ephemeral_storage_bytes",
        "gauge",
        "Configured node ephemeral storage. Absent when unbounded.",
    ),
    (
        "tsls_node_reserved_cpu_millicores",
        "gauge",
        "CPU reserved by live reservations (starting, busy, parking, idle, draining) including per-environment overhead.",
    ),
    (
        "tsls_node_reserved_memory_bytes",
        "gauge",
        "Memory reserved by live reservations including per-environment overhead.",
    ),
    (
        "tsls_node_reserved_ephemeral_storage_bytes",
        "gauge",
        "Ephemeral storage reserved by live reservations.",
    ),
    (
        "tsls_node_environment_overhead_memory_bytes",
        "gauge",
        "Per-environment VMM + bridge memory overhead added to every reservation.",
    ),
    (
        "tsls_node_max_concurrency",
        "gauge",
        "[capacity] max_concurrency: the cap on promised + starting + busy.",
    ),
    (
        "tsls_node_in_flight",
        "gauge",
        "Promised + starting + busy reservations on the node.",
    ),
    (
        "tsls_environments",
        "gauge",
        "Environments on the node by admission state (promised, starting, busy, parking, idle, draining). idle is the ready pool.",
    ),
    (
        "tsls_revision_environments",
        "gauge",
        "Environments of a revision by admission state.",
    ),
    (
        "tsls_revision_desired_environments",
        "gauge",
        "The autoscaler's desired environments for a revision.",
    ),
    (
        "tsls_revision_max_environments",
        "gauge",
        "A revision's max_concurrency (its environment cap).",
    ),
    (
        "tsls_revision_min_ready",
        "gauge",
        "A revision's min_ready.",
    ),
    (
        "tsls_revision_queue_length",
        "gauge",
        "Invocations of a revision waiting for admission.",
    ),
    (
        "tsls_revision_circuit_breaker_state",
        "gauge",
        "Start-failure breaker of a revision: 0 closed, 1 half_open, 2 open.",
    ),
    // queue
    (
        "tsls_queue_length",
        "gauge",
        "Invocations waiting for admission.",
    ),
    (
        "tsls_queue_bytes",
        "gauge",
        "Payload bytes held by waiting invocations.",
    ),
    ("tsls_queue_max_length", "gauge", "[capacity] max_queue."),
    (
        "tsls_queue_max_bytes",
        "gauge",
        "[capacity] max_queue_bytes.",
    ),
    (
        "tsls_queue_oldest_age_seconds",
        "gauge",
        "Age of the oldest waiting invocation (0 when the queue is empty).",
    ),
    (
        "tsls_tenant_queue_length",
        "gauge",
        "Invocations of a tenant waiting for admission.",
    ),
    (
        "tsls_tenant_queue_oldest_age_seconds",
        "gauge",
        "Age of a tenant's oldest waiting invocation (0 when none).",
    ),
    (
        "tsls_tenant_in_flight",
        "gauge",
        "Promised + starting + busy of a tenant.",
    ),
    (
        "tsls_tenant_max_concurrency",
        "gauge",
        "A tenant's concurrency quota. Absent when unlimited.",
    ),
    (
        "tsls_tenant_grants_total",
        "counter",
        "Admission grants to a tenant (cold and warm).",
    ),
    (
        "tsls_start_rate_tokens",
        "gauge",
        "Cold-start tokens available now.",
    ),
    // admission events
    (
        "tsls_admission_arrivals_total",
        "counter",
        "Invocations that asked admission for the first time.",
    ),
    (
        "tsls_admission_grants_total",
        "counter",
        "Admission grants by kind: cold (a new environment reserved) or warm (a pooled environment promised).",
    ),
    (
        "tsls_admission_rejections_total",
        "counter",
        "Admission refusals by reason (queue_full, quota, capacity, queue_deadline, circuit_open, placement, function_deleted).",
    ),
    (
        "tsls_admission_coalesced_waits_total",
        "counter",
        "Grants to invocations the autoscaler gate held back while enough environments were ready or starting (activation coalescing).",
    ),
    (
        "tsls_admission_starts_avoided_total",
        "counter",
        "Coalesced waits served by an existing environment instead of a new boot.",
    ),
    (
        "tsls_environment_starts_total",
        "counter",
        "Cold start results reported to the circuit breakers (success, failure).",
    ),
    (
        "tsls_circuit_breaker_opens_total",
        "counter",
        "Times a revision's start-failure breaker opened.",
    ),
    (
        "tsls_scale_events_total",
        "counter",
        "Scale decisions by kind (activation, scale_up, prestart, scale_down, scale_to_zero, drain) and reason.",
    ),
    (
        "tsls_gate_refusals_total",
        "counter",
        "Invocations or cold starts the invoke gate refused (configuration, authorization, control-plane outage), by error_type.",
    ),
    // attempts
    (
        "tsls_attempts_total",
        "counter",
        "Finished attempts by start kind (cold, warm, restored) and status.",
    ),
    (
        "tsls_attempt_phase_seconds",
        "histogram",
        "Host-measured phase durations by phase (queue_wait, boot, init, resume, handler, total) and start kind. boot and init are recorded for starts that booted only.",
    ),
    (
        "tsls_boot_identity_checks_total",
        "counter",
        "Attempt guest boot ids compared with the first boot id of their environment: first_boot, same_boot (reuse of the same guest), boot_changed (must stay 0), unreported (no guest kernel).",
    ),
    // host usage
    (
        "tsls_environment_cpu_seconds_total",
        "counter",
        "Host CPU time of a live environment as its provider measures it (scope label says what was measured).",
    ),
    (
        "tsls_environment_memory_bytes",
        "gauge",
        "Memory currently charged to a live environment.",
    ),
    (
        "tsls_environment_memory_peak_bytes",
        "gauge",
        "Highest memory charged to a live environment.",
    ),
    (
        "tsls_environment_stats",
        "gauge",
        "Live environments of this dispatcher whose host usage was available or unavailable in this scrape.",
    ),
    (
        "tsls_idle_environment_cpu_seconds_total",
        "counter",
        "Host CPU spent by environments that were idle in two successive samples (scrapes).",
    ),
    (
        "tsls_idle_environment_cpu_ratio_max",
        "gauge",
        "Highest CPU seconds per wall second among environments idle in the last two samples (0 when none).",
    ),
    (
        "tsls_idle_environments_sampled",
        "gauge",
        "Environments the last idle CPU round measured.",
    ),
    // dispatcher / configuration
    (
        "tsls_dispatcher_fenced",
        "gauge",
        "1 when this dispatcher lost its lease and takes no new work.",
    ),
    (
        "tsls_dispatcher_heartbeats_total",
        "counter",
        "Dispatcher lease heartbeats by result (renewed, fenced, error).",
    ),
    (
        "tsls_dispatcher_slot_lease_renewals_total",
        "counter",
        "Slot leases renewed by heartbeats.",
    ),
    (
        "tsls_config_generation",
        "gauge",
        "Highest configuration generation applied to the cache.",
    ),
    (
        "tsls_config_synced",
        "gauge",
        "1 once the configuration cache received a delivery.",
    ),
    (
        "tsls_config_consecutive_failures",
        "gauge",
        "Failed configuration refreshes since the last success (>0: control plane unreachable).",
    ),
    (
        "tsls_config_valid_remaining_seconds",
        "gauge",
        "Seconds the delivered configuration stays valid without a refresh (negative: expired).",
    ),
    (
        "tsls_auth_lease_remaining_seconds",
        "gauge",
        "Seconds the delivered authorization grants stay valid (negative: expired).",
    ),
    (
        "tsls_config_reconnects_total",
        "counter",
        "Refreshes that succeeded after at least one failure.",
    ),
    (
        "tsls_config_entries",
        "gauge",
        "Entries in the configuration cache.",
    ),
    // asynchronous invoke (PLT-4639), only on a gateway with an outbox
    (
        "tsls_async_outbox_pending_events",
        "gauge",
        "Accepted asynchronous invocations whose event is not published to the queue yet.",
    ),
    (
        "tsls_async_outbox_oldest_pending_age_seconds",
        "gauge",
        "Age of the oldest unpublished outbox event (0 when none).",
    ),
    (
        "tsls_async_outbox_sent_retained_events",
        "gauge",
        "Published outbox events still retained (sent_retention_seconds).",
    ),
    (
        "tsls_async_queue_condition",
        "gauge",
        "1 for the queue condition the outbox publisher last saw (healthy, full, unavailable).",
    ),
    // triggers (PLT-4641), only on a gateway with triggers
    (
        "tsls_trigger_scheduler_owner",
        "gauge",
        "1 when this gateway held the cron scheduler lease on its last pass.",
    ),
    (
        "tsls_trigger_cron_fires_total",
        "counter",
        "Scheduled cron times handled by result (accepted, already_fired, refused, deferred, inactive).",
    ),
    (
        "tsls_trigger_cron_missed_runs_total",
        "counter",
        "Late scheduled cron times by what the missed-run policy did (run, skipped).",
    ),
    (
        "tsls_trigger_webhook_deliveries_total",
        "counter",
        "Webhook deliveries by result (accepted, replayed, signature_refused, timestamp_refused, too_large, invalid_event_id, disabled, not_found, refused).",
    ),
    // usage metering (PLT-4642)
    (
        "tsls_usage_journal_healthy",
        "gauge",
        "1 when the usage journal can be read and written (PLT-4642).",
    ),
    (
        "tsls_usage_journal_admitting",
        "gauge",
        "1 when the usage journal has more than its admission headroom left: new invocations are admitted metered.",
    ),
    (
        "tsls_usage_journal_pending_events",
        "gauge",
        "Usage events written to the journal and not yet delivered to the usage ledger.",
    ),
    (
        "tsls_usage_journal_pending_bytes",
        "gauge",
        "Bytes of the pending usage events.",
    ),
    (
        "tsls_usage_journal_max_events",
        "gauge",
        "[usage] journal_max_events.",
    ),
    (
        "tsls_usage_journal_max_bytes",
        "gauge",
        "[usage] journal_max_bytes.",
    ),
    (
        "tsls_usage_unjournaled_events_total",
        "counter",
        "Usage events the journal refused (full or unavailable): unmetered, never estimated.",
    ),
    (
        "tsls_usage_collector_runs_total",
        "counter",
        "Usage collector runs of this process.",
    ),
    (
        "tsls_usage_collector_failing",
        "gauge",
        "1 when the last usage collector run failed (ledger unavailable, journal integrity).",
    ),
    (
        "tsls_usage_collector_last_success_age_seconds",
        "gauge",
        "Seconds since the usage collector last delivered successfully (collector lag). Absent before the first success.",
    ),
    (
        "tsls_usage_collector_delivered_events_total",
        "counter",
        "Journal entries this process's collector delivered to the ledger (re-deliveries included).",
    ),
    (
        "tsls_usage_ledger_events",
        "gauge",
        "Distinct usage events in the usage ledger.",
    ),
    (
        "tsls_usage_ledger_duplicates_ignored_total",
        "counter",
        "Deliveries the usage ledger dropped because it already had the event id.",
    ),
    // asynchronous dispatcher (PLT-4640), only on a gateway that runs it
    (
        "tsls_async_dispatch_deliveries_total",
        "counter",
        "Queue deliveries the dispatcher handled, by outcome (completed, rescheduled, dead_lettered, skipped_terminal, skipped_stale, skipped_claimed, not_due, poison, failed, lost_claim).",
    ),
    (
        "tsls_async_dispatch_queue_operations_total",
        "counter",
        "ack / nak / term calls on the queue, by result (ok, error). An ack is only sent after the ledger commit.",
    ),
    (
        "tsls_async_dispatch_runs_in_flight",
        "gauge",
        "Asynchronous deliveries being handled by this gateway now.",
    ),
    (
        "tsls_async_retries_scheduled_total",
        "counter",
        "Next tries committed with the next outbox generation: retry (counted against max_attempts) or deferral (capacity, retry budget, shutdown; not counted).",
    ),
    (
        "tsls_async_dead_letters_total",
        "counter",
        "Dead letters committed by this gateway, by reason.",
    ),
    (
        "tsls_async_redrives_total",
        "counter",
        "Dead letters redriven into a new asynchronous invocation.",
    ),
    (
        "tsls_async_reaper_actions_total",
        "counter",
        "What the reaper committed: abandoned (a run whose claim expired, rescheduled), republished (an event the broker lost), dead_lettered, lost (fence moved on), failed.",
    ),
    (
        "tsls_metrics_series_truncated",
        "gauge",
        "Label sets folded into the _other series of a family because of the [metrics] cardinality caps.",
    ),
];

/// The `(type, help)` of a family.
pub fn family(name: &str) -> Option<(&'static str, &'static str)> {
    FAMILIES
        .iter()
        .find(|(n, _, _)| *n == name)
        .map(|(_, t, h)| (*t, *h))
}
