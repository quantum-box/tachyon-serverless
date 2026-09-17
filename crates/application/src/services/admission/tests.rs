//! Fake-clock scale tests of admission (PLT-4634). Everything here drives
//! [`AdmissionState`] with explicit timestamps, so no test sleeps and every
//! run is identical; the last few exercise the async delivery wrapper.

use std::collections::HashMap;

use chrono::TimeZone;

use tachyon_serverless_domain::{FixedClock, RevisionId, TenantId, Timestamp};

use super::state::{Outcome, ResState};
use super::*;

const MIB: u64 = 1;

fn t(ms: i64) -> Timestamp {
    chrono::Utc
        .timestamp_millis_opt(1_900_000_000_000 + ms)
        .unwrap()
}

fn settings() -> AdmissionSettings {
    AdmissionSettings {
        node: NodeConfig {
            name: "test-node".into(),
            region: Some("jp".into()),
            ..NodeConfig::default()
        },
        max_concurrency: 64,
        max_queue: 1000,
        max_queue_bytes: 1 << 30,
        queue_timeout_seconds: 10,
        tenant_defaults: TenantQuotaConfig::default(),
        tenants: Vec::new(),
        start_rate_per_second: 1000,
        start_burst: 1000,
        breaker_threshold: 3,
        breaker_cooldown_seconds: 30,
        rate_window_seconds: 10,
    }
}

fn tenant(n: u8) -> TenantId {
    TenantId::parse(&format!("tn_01hzzzzzzzzzzzzzzzzzzzzz{:02}", n)).unwrap()
}

fn env_resources() -> Resources {
    // 256 MiB guest + 16 MiB VMM + 8 MiB bridge (the default overhead).
    Resources {
        cpu_millis: 500,
        memory_mib: 280 * MIB,
        ephemeral_storage_mib: 256,
    }
}

fn ticket(tenant: &TenantId, revision: &RevisionId, max_env: u32, deadline: Timestamp) -> Ticket {
    Ticket {
        tenant: tenant.clone(),
        function: tachyon_serverless_domain::FunctionId::generate(),
        revision: revision.clone(),
        idle_ttl_seconds: 60,
        scale_down_cooldown_seconds: 30,
        resources: env_resources(),
        max_environments: max_env,
        concurrency_per_environment: 1,
        min_ready: 0,
        payload_bytes: 100,
        deadline,
        required_region: None,
        cold_only: false,
    }
}

/// What the outbox said, keyed by waiter.
#[derive(Default)]
struct Deliveries {
    granted: HashMap<WaiterId, (ReservationId, GrantKind)>,
    rejected: HashMap<WaiterId, RejectReason>,
}

impl Deliveries {
    fn collect(&mut self, s: &mut AdmissionState) {
        for (w, o) in s.take_outbox() {
            match o {
                Outcome::Granted { reservation, kind } => {
                    self.granted.insert(w, (reservation, kind));
                }
                Outcome::Rejected(r) => {
                    self.rejected.insert(w, r.reason);
                }
            }
        }
    }
    fn cold(&self) -> usize {
        self.granted
            .values()
            .filter(|(_, k)| *k == GrantKind::Cold)
            .count()
    }
    fn warm(&self) -> usize {
        self.granted
            .values()
            .filter(|(_, k)| *k == GrantKind::Warm)
            .count()
    }
}

/// Acceptance 1: a burst scales up to the revision's cap, a `Starting`
/// reservation is counted once while booting and once it is `Busy`, and the
/// cap is never exceeded however the reservations move.
#[test]
fn burst_scales_to_the_cap_and_starting_reservations_never_overshoot() {
    let mut s = AdmissionState::new(settings());
    let mut d = Deliveries::default();
    let (a, rev) = (tenant(1), RevisionId::generate());
    let deadline = t(10_000);
    let waiters: Vec<WaiterId> = (0..10)
        .map(|_| {
            s.enqueue(ticket(&a, &rev, 4, deadline), false, t(0))
                .unwrap()
        })
        .collect();
    d.collect(&mut s);
    s.check_invariants();
    assert_eq!(d.cold(), 4, "the burst grows to max_environments at once");
    assert_eq!(s.queue_len(), 6);
    assert_eq!(
        s.reserved(),
        Resources {
            cpu_millis: 4 * 500,
            memory_mib: 4 * 280,
            ephemeral_storage_mib: 4 * 256
        },
        "each starting environment reserves its resources plus overhead exactly once"
    );

    // Booting finishes: Starting -> Busy changes nothing about the total.
    let granted: Vec<ReservationId> = d.granted.values().map(|(r, _)| *r).collect();
    for r in &granted {
        s.ready(*r);
        s.pump(t(100));
        s.check_invariants();
    }
    d.collect(&mut s);
    assert_eq!(d.cold(), 4, "Ready does not free a slot");
    assert_eq!(s.reserved().memory_mib, 4 * 280);

    // Every release hands exactly one slot to the next waiter.
    for (i, r) in granted.iter().enumerate() {
        s.release(*r, t(200 + i as i64));
        s.check_invariants();
        d.collect(&mut s);
        assert_eq!(d.cold(), 5 + i);
    }
    assert!(waiters.iter().all(|w| !d.rejected.contains_key(w)));
}

/// Acceptance 1/4: node resources (with the VMM and bridge overhead) bound
/// the environments, and `max_concurrency` bounds them independently.
#[test]
fn node_resources_including_overhead_bound_the_environments() {
    let mut cfg = settings();
    cfg.node.memory_mib = Some(900);
    let mut s = AdmissionState::new(cfg);
    let mut d = Deliveries::default();
    let (a, rev) = (tenant(1), RevisionId::generate());
    // 3 × 280 = 840 fits in 900; the fourth does not (a 256 MiB guest alone
    // would fit, the overhead is what does not).
    let ids: Vec<_> = (0..5)
        .map(|_| {
            s.enqueue(ticket(&a, &rev, 100, t(5_000)), false, t(0))
                .unwrap()
        })
        .collect();
    d.collect(&mut s);
    s.check_invariants();
    assert_eq!(d.cold(), 3);
    assert_eq!(s.reserved().memory_mib, 840);

    // The node stays full until the deadline: the waiters are refused for
    // `capacity`, which is what the invocation reports.
    s.pump(t(5_000));
    d.collect(&mut s);
    assert_eq!(d.rejected.get(&ids[3]), Some(&RejectReason::Capacity));
    assert_eq!(d.rejected.get(&ids[4]), Some(&RejectReason::Capacity));

    // An environment that can never fit is refused at once.
    let mut huge = ticket(&a, &rev, 100, t(9_000));
    huge.resources.memory_mib = 901;
    assert_eq!(
        s.enqueue(huge, false, t(6_000)).unwrap_err().reason,
        RejectReason::Capacity
    );

    let mut cfg = settings();
    cfg.max_concurrency = 2;
    let mut s = AdmissionState::new(cfg);
    let mut d = Deliveries::default();
    for _ in 0..4 {
        s.enqueue(ticket(&a, &rev, 100, t(5_000)), false, t(0))
            .unwrap();
    }
    d.collect(&mut s);
    assert_eq!(d.cold(), 2, "max_concurrency caps environments node-wide");
}

/// Scale-up is rate limited: a burst beyond the bucket waits for tokens
/// that refill with the (fake) clock.
#[test]
fn start_rate_limits_a_cold_burst_and_refills_with_the_clock() {
    let mut cfg = settings();
    cfg.start_rate_per_second = 2;
    cfg.start_burst = 3;
    let mut s = AdmissionState::new(cfg);
    let mut d = Deliveries::default();
    let (a, rev) = (tenant(1), RevisionId::generate());
    for _ in 0..10 {
        s.enqueue(ticket(&a, &rev, 100, t(60_000)), false, t(0))
            .unwrap();
    }
    d.collect(&mut s);
    assert_eq!(d.cold(), 3, "the burst size");
    s.pump(t(400));
    d.collect(&mut s);
    assert_eq!(d.cold(), 3, "less than one token after 400 ms");
    s.pump(t(500));
    d.collect(&mut s);
    assert_eq!(d.cold(), 4);
    s.pump(t(2_500));
    d.collect(&mut s);
    assert_eq!(
        d.cold(),
        7,
        "2/s for 2 s, but never more than the burst at once"
    );
    s.pump(t(3_000));
    d.collect(&mut s);
    assert_eq!(d.cold(), 8);
    s.check_invariants();
}

/// Activation coalescing: a burst for a revision with pooled environments
/// starts only `desired - (ready + starting)` new ones, and an environment on
/// its way into the pool is waited for instead of booting another.
#[test]
fn activation_coalescing_counts_ready_and_parking_environments() {
    let mut s = AdmissionState::new(settings());
    let mut d = Deliveries::default();
    let (a, rev) = (tenant(1), RevisionId::generate());
    // Two environments serve two invocations and go to the pool.
    for _ in 0..2 {
        s.enqueue(ticket(&a, &rev, 10, t(60_000)), false, t(0))
            .unwrap();
    }
    d.collect(&mut s);
    let first: Vec<ReservationId> = d.granted.values().map(|(r, _)| *r).collect();
    for r in &first {
        s.ready(*r);
        s.park(*r, t(10));
        s.parked(*r, t(20));
    }
    s.check_invariants();

    // A burst of 5: 2 are promised the idle environments, 3 boot. Never 5.
    let mut d = Deliveries::default();
    for _ in 0..5 {
        s.enqueue(ticket(&a, &rev, 10, t(60_000)), false, t(100))
            .unwrap();
    }
    d.collect(&mut s);
    s.check_invariants();
    assert_eq!(d.warm(), 2);
    assert_eq!(d.cold(), 3, "desired 5 - ready 2 - starting 0");

    // The promises are taken; every environment finishes and one parks.
    let promised: Vec<ReservationId> = d
        .granted
        .values()
        .filter(|(_, k)| *k == GrantKind::Warm)
        .map(|(r, _)| *r)
        .collect();
    let mut busy = Vec::new();
    for (p, idle) in promised.iter().zip(&first) {
        busy.push(s.adopt(Some(*p), *idle, t(200)));
    }
    s.check_invariants();
    for r in d
        .granted
        .values()
        .filter(|(_, k)| *k == GrantKind::Cold)
        .map(|(r, _)| *r)
    {
        s.release(r, t(300));
    }
    s.park(busy[0], t(400));
    s.release(busy[1], t(400));
    s.check_invariants();

    // One new invocation while that environment is still quiescing: it
    // waits for it instead of booting a sixth environment...
    let mut d = Deliveries::default();
    let w = s
        .enqueue(ticket(&a, &rev, 10, t(60_000)), false, t(500))
        .unwrap();
    d.collect(&mut s);
    assert!(
        d.granted.is_empty(),
        "no start while an environment is parking"
    );
    assert!(s.is_queued(w));
    // ...and takes it once it is idle.
    s.parked(busy[0], t(510));
    d.collect(&mut s);
    assert_eq!(d.granted.get(&w).map(|(_, k)| *k), Some(GrantKind::Warm));
    s.check_invariants();
}

/// Acceptance 2: a tenant with long invocations does not starve one with
/// short invocations. Simulated for 60 s of fake time on a 4-slot node, with
/// the long tenant arriving first (the worst case: admission is not
/// preemptive, so only the per-tenant quota keeps it from taking every slot
/// before the short tenant shows up; fair ordering does the rest).
#[test]
fn a_long_running_tenant_does_not_starve_a_short_one() {
    let mut cfg = settings();
    cfg.max_concurrency = 4;
    cfg.tenant_defaults.max_concurrency = Some(3);
    let mut s = AdmissionState::new(cfg);
    let (long, short) = (tenant(1), tenant(2));
    let (long_rev, short_rev) = (RevisionId::generate(), RevisionId::generate());
    // Each tenant keeps 8 invocations queued at all times.
    let mut queued: HashMap<WaiterId, (TenantId, i64)> = HashMap::new();
    let mut running: Vec<(ReservationId, TenantId, i64)> = Vec::new();
    let mut short_done = 0;
    let mut long_done = 0;
    let mut max_short_wait = 0;
    let mut max_long_in_flight = 0;
    let mut max_long_in_flight_after_first_round = 0;
    let mut now_ms = 0;
    while now_ms <= 60_000 {
        let now = t(now_ms);
        running.retain(|(r, who, end)| {
            if *end <= now_ms {
                if *who == short {
                    short_done += 1;
                } else {
                    long_done += 1;
                }
                s.release(*r, now);
                false
            } else {
                true
            }
        });
        for who in [&long, &short] {
            let (rev, _) = if *who == long {
                (&long_rev, ())
            } else {
                (&short_rev, ())
            };
            while queued.values().filter(|(t, _)| t == who).count() < 8 {
                let id = s
                    .enqueue(ticket(who, rev, 100, t(now_ms + 3_600_000)), false, now)
                    .unwrap();
                queued.insert(id, (who.clone(), now_ms));
            }
        }
        s.pump(now);
        for (w, o) in s.take_outbox() {
            let Outcome::Granted { reservation, .. } = o else {
                panic!("nothing is refused here");
            };
            let (who, since) = queued.remove(&w).unwrap();
            let duration = if who == short { 200 } else { 30_000 };
            if who == short {
                max_short_wait = max_short_wait.max(now_ms - since);
            }
            s.ready(reservation);
            running.push((reservation, who, now_ms + duration));
        }
        let long_now = running.iter().filter(|(_, w, _)| *w == long).count();
        max_long_in_flight = max_long_in_flight.max(long_now);
        if now_ms > 30_000 {
            max_long_in_flight_after_first_round =
                max_long_in_flight_after_first_round.max(long_now);
        }
        s.check_invariants();
        now_ms += 50;
    }
    assert!(
        long_done >= 2,
        "the long tenant still makes progress: {long_done}"
    );
    assert!(
        max_long_in_flight <= 3,
        "the tenant quota caps the long tenant: {max_long_in_flight}"
    );
    assert!(
        max_long_in_flight_after_first_round <= 2,
        "once its first invocations end, fair ordering holds it to half: \
         {max_long_in_flight_after_first_round}"
    );
    assert!(
        short_done >= 400,
        "the short tenant keeps two slots turning over: {short_done}"
    );
    assert!(
        max_short_wait <= 3_000,
        "a short invocation never waits behind long ones for long: {max_short_wait} ms"
    );
}

/// Acceptance 2: the queue is bounded by count, total payload bytes, the
/// tenant's share and each item's deadline; the oldest age is reported.
#[test]
fn the_queue_is_bounded_by_count_bytes_tenant_share_and_deadline() {
    let mut cfg = settings();
    cfg.max_concurrency = 1;
    cfg.max_queue = 3;
    cfg.max_queue_bytes = 1_000;
    let c = tenant(3);
    cfg.tenants = vec![TenantQuotaEntry {
        tenant_id: c.clone(),
        max_concurrency: None,
        max_queue: Some(1),
        weight: None,
        required_region: None,
    }];
    let mut s = AdmissionState::new(cfg);
    let mut d = Deliveries::default();
    let (a, rev) = (tenant(1), RevisionId::generate());
    let with_bytes = |bytes: u64, deadline: Timestamp| {
        let mut tk = ticket(&a, &rev, 100, deadline);
        tk.payload_bytes = bytes;
        tk
    };
    // Running: not queued, so its bytes do not count.
    s.enqueue(with_bytes(900, t(10_000)), false, t(0)).unwrap();
    d.collect(&mut s);
    assert_eq!(d.cold(), 1);

    let w1 = s.enqueue(with_bytes(400, t(2_000)), false, t(0)).unwrap();
    let w2 = s
        .enqueue(with_bytes(400, t(10_000)), false, t(100))
        .unwrap();
    let bytes = s
        .enqueue(with_bytes(400, t(10_000)), false, t(200))
        .unwrap_err();
    assert_eq!(bytes.reason, RejectReason::QueueFull, "{}", bytes.message);
    assert!(bytes.message.contains("bytes"), "{}", bytes.message);
    let _w3 = s.enqueue(with_bytes(10, t(10_000)), false, t(300)).unwrap();
    let count = s
        .enqueue(with_bytes(10, t(10_000)), false, t(300))
        .unwrap_err();
    assert_eq!(count.reason, RejectReason::QueueFull);
    assert!(count.message.contains("slots"), "{}", count.message);

    let info = s.snapshot(&a, t(1_000));
    assert_eq!(info.queue.length, 3);
    assert_eq!(info.queue.bytes, 810);
    assert_eq!(info.queue.oldest_age_ms, Some(1_000));
    assert_eq!(info.tenant.queued, 3);

    // The deadline: w1 is refused, blocked on capacity; the others stay.
    s.pump(t(2_000));
    d.collect(&mut s);
    assert_eq!(d.rejected.get(&w1), Some(&RejectReason::Capacity));
    assert!(s.is_queued(w2));

    // A tenant's own share of the queue.
    let c_rev = RevisionId::generate();
    s.enqueue(ticket(&c, &c_rev, 100, t(10_000)), false, t(2_100))
        .unwrap();
    let share = s
        .enqueue(ticket(&c, &c_rev, 100, t(10_000)), false, t(2_100))
        .unwrap_err();
    assert_eq!(share.reason, RejectReason::Quota);
    s.check_invariants();
}

/// Tenant and revision quotas keep one tenant from taking the node.
#[test]
fn tenant_and_pool_quotas_cap_one_tenant() {
    let mut cfg = settings();
    cfg.max_concurrency = 10;
    let a = tenant(1);
    cfg.tenants = vec![TenantQuotaEntry {
        tenant_id: a.clone(),
        max_concurrency: Some(3),
        max_queue: None,
        weight: None,
        required_region: None,
    }];
    let mut s = AdmissionState::new(cfg);
    let mut d = Deliveries::default();
    let (r1, r2) = (RevisionId::generate(), RevisionId::generate());
    for _ in 0..3 {
        s.enqueue(ticket(&a, &r1, 2, t(1_000)), false, t(0))
            .unwrap();
        s.enqueue(ticket(&a, &r2, 100, t(1_000)), false, t(0))
            .unwrap();
    }
    let b = tenant(2);
    let b_w = s
        .enqueue(
            ticket(&b, &RevisionId::generate(), 100, t(1_000)),
            false,
            t(0),
        )
        .unwrap();
    d.collect(&mut s);
    s.check_invariants();
    assert_eq!(d.cold(), 4, "tenant A holds 3 (its quota), B gets its one");
    assert!(d.granted.contains_key(&b_w));
    s.pump(t(1_000));
    d.collect(&mut s);
    assert!(
        d.rejected.values().all(|r| *r == RejectReason::Quota),
        "{:?}",
        d.rejected
    );
    assert_eq!(d.rejected.len(), 3);
}

/// Start-failure circuit breaker: opens after K consecutive boot failures,
/// refuses queued and new invocations fast, lets one probe through after the
/// cooldown and closes on its success. Other revisions are not affected.
#[test]
fn circuit_breaker_opens_rejects_fast_and_probes_after_cooldown() {
    let mut s = AdmissionState::new(settings());
    let mut d = Deliveries::default();
    let (a, bad, good) = (tenant(1), RevisionId::generate(), RevisionId::generate());
    let ids: Vec<_> = (0..5)
        .map(|_| {
            s.enqueue(ticket(&a, &bad, 3, t(600_000)), false, t(0))
                .unwrap()
        })
        .collect();
    d.collect(&mut s);
    let starts: Vec<ReservationId> = d.granted.values().map(|(r, _)| *r).collect();
    assert_eq!(starts.len(), 3);
    // Three boots fail in a row (the environments are torn down after the
    // results are known, as the driver does).
    for r in &starts {
        s.start_result(*r, false, t(1_000));
        s.check_invariants();
    }
    for r in &starts {
        s.release(*r, t(1_000));
    }
    d.collect(&mut s);
    let refused: Vec<_> = ids
        .iter()
        .filter(|w| d.rejected.get(w) == Some(&RejectReason::CircuitOpen))
        .collect();
    assert_eq!(
        refused.len(),
        2,
        "queued invocations are refused when it opens"
    );
    assert_eq!(
        s.enqueue(ticket(&a, &bad, 3, t(600_000)), false, t(2_000))
            .unwrap_err()
            .reason,
        RejectReason::CircuitOpen
    );
    let other = s.enqueue(ticket(&a, &good, 3, t(600_000)), false, t(2_000));
    assert!(other.is_ok(), "another revision still starts");
    d.collect(&mut s);
    assert_eq!(s.snapshot(&a, t(2_000)).revisions.len(), 2);

    // After the cooldown: exactly one probe.
    let p1 = s
        .enqueue(ticket(&a, &bad, 3, t(600_000)), false, t(31_000))
        .unwrap();
    let p2 = s
        .enqueue(ticket(&a, &bad, 3, t(600_000)), false, t(31_000))
        .unwrap();
    d.collect(&mut s);
    assert!(d.granted.contains_key(&p1));
    assert!(s.is_queued(p2), "the second waits for the probe's result");
    let probe = d.granted[&p1].0;
    s.start_result(probe, true, t(32_000));
    d.collect(&mut s);
    assert!(d.granted.contains_key(&p2), "a successful probe closes it");
    s.check_invariants();
}

/// Acceptance 3: `jp-only` is never relaxed, whatever the load.
#[test]
fn jp_only_placement_is_never_relaxed() {
    let rev = RevisionId::generate();
    let (plain, jp_tenant) = (tenant(1), tenant(2));
    let jp_entry = TenantQuotaEntry {
        tenant_id: jp_tenant.clone(),
        max_concurrency: None,
        max_queue: None,
        weight: None,
        required_region: Some("jp".into()),
    };
    for (region, admitted) in [(Some("jp"), true), (Some("us"), false), (None, false)] {
        let mut cfg = settings();
        cfg.node.region = region.map(str::to_string);
        cfg.tenants = vec![jp_entry.clone()];
        let mut s = AdmissionState::new(cfg);
        // An idle node: capacity is not the reason.
        let r = s.enqueue(ticket(&jp_tenant, &rev, 4, t(1_000)), false, t(0));
        assert_eq!(r.is_ok(), admitted, "node region {region:?}");
        if let Err(e) = r {
            assert_eq!(e.reason, RejectReason::Placement);
            assert!(e.message.contains("never relaxed"), "{}", e.message);
        }
        // A revision-level requirement behaves the same.
        let mut tk = ticket(&plain, &RevisionId::generate(), 4, t(1_000));
        tk.required_region = Some("jp".into());
        assert_eq!(s.enqueue(tk, false, t(0)).is_ok(), admitted);
        // Unconstrained work runs anywhere.
        assert!(
            s.enqueue(
                ticket(&plain, &RevisionId::generate(), 4, t(1_000)),
                false,
                t(0)
            )
            .is_ok()
        );
    }
    // A jp tenant with a revision pinned elsewhere cannot be satisfied.
    let mut cfg = settings();
    cfg.tenants = vec![jp_entry];
    let mut s = AdmissionState::new(cfg);
    let mut tk = ticket(&jp_tenant, &rev, 4, t(1_000));
    tk.required_region = Some("us".into());
    assert_eq!(
        s.enqueue(tk, false, t(0)).unwrap_err().reason,
        RejectReason::Placement
    );
}

/// Acceptance 4: the report separates the host (capacity, overhead, one
/// host, no scale-out) from the environments on it, and shows a tenant only
/// its own queue and revisions.
#[test]
fn the_report_separates_host_capacity_from_environments() {
    let mut cfg = settings();
    cfg.node.memory_mib = Some(4096);
    cfg.node.cpu_millis = Some(4000);
    let mut s = AdmissionState::new(cfg);
    let (a, b) = (tenant(1), tenant(2));
    let (ra, rb) = (RevisionId::generate(), RevisionId::generate());
    s.enqueue(ticket(&a, &ra, 4, t(1_000)), false, t(0))
        .unwrap();
    s.enqueue(ticket(&b, &rb, 4, t(1_000)), false, t(0))
        .unwrap();
    let info = s.snapshot(&a, t(10));
    assert_eq!(info.node.hosts, 1);
    assert_eq!(info.node.host_scale_out, "not_supported");
    assert_eq!(info.node.capacity.memory_mib, Some(4096));
    assert_eq!(info.node.capacity.ephemeral_storage_mib, None);
    assert_eq!(info.node.per_environment_overhead.memory_mib, Some(24));
    assert_eq!(info.reserved.memory_mib, Some(560));
    assert_eq!(info.environments.starting, 2);
    assert_eq!(info.in_flight, 2);
    assert_eq!(info.tenant.tenant_id, a.to_string());
    assert_eq!(info.revisions.len(), 1, "B's revision is not shown to A");
    assert_eq!(info.revisions[0].revision_id, ra.to_string());
    assert_eq!(info.revisions[0].environments.starting, 1);
    assert_eq!(info.revisions[0].circuit_breaker, "closed");
    let json = serde_json::to_string(&info).unwrap();
    assert!(!json.contains(&b.to_string()), "{json}");
}

/// Property: under any sequence of arrivals, grants, boots, pool moves,
/// adoptions, redemptions, releases, withdrawals and clock steps, every
/// counter equals a recomputation from the live reservations, and neither
/// node capacity, `max_concurrency`, a tenant quota nor a revision cap is
/// ever exceeded. Deterministic seeds.
#[test]
fn reservations_are_counted_exactly_once_under_random_operations() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }
    for seed in 1..=12u64 {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03));
        let mut cfg = settings();
        cfg.max_concurrency = 6;
        cfg.max_queue = 12;
        cfg.node.memory_mib = Some(2_000);
        cfg.node.cpu_millis = Some(3_000);
        cfg.start_rate_per_second = 5;
        cfg.start_burst = 4;
        cfg.breaker_threshold = 2;
        cfg.breaker_cooldown_seconds = 1;
        cfg.tenants = vec![TenantQuotaEntry {
            tenant_id: tenant(1),
            max_concurrency: Some(3),
            max_queue: Some(6),
            weight: Some(2),
            required_region: None,
        }];
        let mut s = AdmissionState::new(cfg);
        let tenants = [tenant(1), tenant(2), tenant(3)];
        let revisions: Vec<RevisionId> = (0..4).map(|_| RevisionId::generate()).collect();
        let mut now = 0i64;
        let mut waiting: Vec<WaiterId> = Vec::new();
        let mut held: Vec<ReservationId> = Vec::new();
        let mut grants: Vec<GrantKind> = Vec::new();
        let mut rejected = 0usize;
        for _ in 0..3_000 {
            match rng.below(12) {
                0..=2 => {
                    let i = rng.below(4) as usize;
                    let rev = &revisions[i];
                    // A revision belongs to one tenant, and its cap is part
                    // of its immutable spec.
                    let tn = &tenants[i % 3];
                    let mut tk = ticket(tn, rev, 1 + i as u32, t(now + rng.below(3_000) as i64));
                    tk.resources.memory_mib = 100 + rng.below(400);
                    tk.payload_bytes = rng.below(500);
                    if let Ok(w) = s.enqueue(tk, rng.below(8) == 0, t(now)) {
                        waiting.push(w);
                    }
                }
                3 => {
                    if !waiting.is_empty() {
                        let w = waiting.swap_remove(rng.below(waiting.len() as u64) as usize);
                        s.withdraw(w);
                    }
                }
                4 => now += rng.below(700) as i64,
                _ if held.is_empty() => s.pump(t(now)),
                5 => s.ready(held[rng.below(held.len() as u64) as usize]),
                6 => {
                    let r = held[rng.below(held.len() as u64) as usize];
                    s.start_result(r, rng.below(3) != 0, t(now));
                }
                7 => {
                    let r = held[rng.below(held.len() as u64) as usize];
                    match s.state_of(r) {
                        Some(ResState::Busy) => s.park(r, t(now)),
                        Some(ResState::Parking) => s.parked(r, t(now)),
                        Some(ResState::Idle) => s.drain(r, t(now)),
                        _ => {}
                    }
                }
                8 => {
                    // A holder takes some idle environment.
                    let idle: Vec<ReservationId> = held
                        .iter()
                        .copied()
                        .filter(|r| s.state_of(*r) == Some(ResState::Idle))
                        .collect();
                    let holders: Vec<ReservationId> = held
                        .iter()
                        .copied()
                        .filter(|r| {
                            matches!(
                                s.state_of(*r),
                                Some(ResState::Promised | ResState::Starting)
                            )
                        })
                        .collect();
                    // The pool only hands out environments of the claimer's
                    // own revision (the reuse key contains it).
                    if let Some(i) = idle.first().copied()
                        && let Some(h) = holders
                            .iter()
                            .copied()
                            .find(|h| s.revision_of(*h) == s.revision_of(i))
                    {
                        s.adopt(Some(h), i, t(now));
                    }
                }
                9 => {
                    let r = held[rng.below(held.len() as u64) as usize];
                    if s.state_of(r) == Some(ResState::Promised) {
                        s.redeem(r, t(now));
                    }
                }
                _ => {
                    let i = rng.below(held.len() as u64) as usize;
                    s.release(held.swap_remove(i), t(now));
                }
            }
            for (w, o) in s.take_outbox() {
                waiting.retain(|x| *x != w);
                match o {
                    Outcome::Granted { reservation, kind } => {
                        held.push(reservation);
                        grants.push(kind);
                    }
                    Outcome::Rejected(_) => rejected += 1,
                }
            }
            held.retain(|r| s.state_of(*r).is_some());
            waiting.retain(|w| s.is_queued(*w));
            s.check_invariants();
        }
        let cold = grants.iter().filter(|k| **k == GrantKind::Cold).count();
        let warm = grants.len() - cold;
        assert!(
            cold > 100 && warm > 0 && rejected > 0,
            "seed {seed} exercised too little: cold {cold}, warm {warm}, rejected {rejected}"
        );
        // Tear everything down: nothing may remain reserved.
        for w in waiting.drain(..) {
            s.withdraw(w);
        }
        while let Some(r) = held.pop() {
            s.release(r, t(now));
            for (_, o) in s.take_outbox() {
                if let Outcome::Granted { reservation, .. } = o {
                    held.push(reservation);
                }
            }
        }
        s.check_invariants();
        assert_eq!(s.reserved(), Resources::ZERO, "seed {seed}");
    }
}

// ---------------------------------------------------------------------------
// async wrapper
// ---------------------------------------------------------------------------

fn revision(max_concurrency: u32) -> tachyon_serverless_domain::FunctionRevision {
    use tachyon_serverless_domain::*;
    FunctionRevision::new(
        RevisionId::generate(),
        FunctionId::generate(),
        tenant(1),
        1,
        RevisionSpec {
            artifact: ArtifactRef::Binary {
                digest: Sha256Digest::of_bytes(b"x"),
                size_bytes: 1,
            },
            runtime: RuntimeSpec {
                protocol: RUNTIME_PROTOCOL_V1.into(),
                architecture: Architecture::Aarch64,
            },
            resources: ResourceProfile::default(),
            execution: ExecutionPolicy {
                max_concurrency,
                ..ExecutionPolicy::default()
            },
            egress: EgressProfile::None,
            egress_allow: Vec::new(),
            env_vars: Vec::new(),
            secrets: Vec::new(),
            description: String::new(),
            placement: Placement::default(),
        },
        &Limits::default(),
        t(0),
    )
    .unwrap()
}

#[tokio::test]
async fn a_dropped_grant_is_released_and_hands_its_slot_to_the_next_waiter() {
    let clock = Arc::new(FixedClock::new(t(0)));
    let mut cfg = settings();
    cfg.max_concurrency = 1;
    let ctrl = AdmissionController::new(cfg, clock.clone());
    let rev = revision(4);
    let a = tenant(1);
    let mut first = ctrl.admit(ctrl.ticket(&a, &rev, 10, t(60_000))).unwrap();
    assert!(!first.is_waiting());
    let grant = first
        .wait(
            tokio::time::Instant::now() + Duration::from_secs(1),
            std::future::pending::<()>(),
        )
        .await
        .unwrap();
    assert_eq!(grant.kind(), GrantKind::Cold);
    let mut second = ctrl.admit(ctrl.ticket(&a, &rev, 10, t(60_000))).unwrap();
    assert!(second.is_waiting());
    assert_eq!(ctrl.snapshot(&a).queue.length, 1);
    drop(grant);
    let second = second
        .wait(
            tokio::time::Instant::now() + Duration::from_secs(1),
            std::future::pending::<()>(),
        )
        .await
        .unwrap();
    assert_eq!(ctrl.snapshot(&a).in_flight, 1);

    // A waiter that times out is withdrawn with the reason that blocked it.
    let third = ctrl.admit(ctrl.ticket(&a, &rev, 10, t(60_000))).unwrap();
    let err = third
        .wait(
            tokio::time::Instant::now() + Duration::from_millis(50),
            std::future::pending::<()>(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, WaitError::Timeout(RejectReason::Capacity)),
        "{err:?}"
    );
    assert_eq!(ctrl.snapshot(&a).queue.length, 0);

    // A cancelled or abandoned waiter leaves nothing behind.
    let fourth = ctrl.admit(ctrl.ticket(&a, &rev, 10, t(60_000))).unwrap();
    let err = fourth
        .wait(
            tokio::time::Instant::now() + Duration::from_secs(5),
            async { 7 },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, WaitError::Cancelled(7)));
    let fifth = ctrl.admit(ctrl.ticket(&a, &rev, 10, t(60_000))).unwrap();
    drop(fifth);
    let info = ctrl.snapshot(&a);
    assert_eq!((info.queue.length, info.in_flight), (0, 1));
    drop(second);
    assert_eq!(ctrl.snapshot(&a).in_flight, 0);
    assert_eq!(ctrl.snapshot(&a).reserved.memory_mib, Some(0));
}

#[tokio::test]
async fn a_grant_for_a_waiter_that_went_away_is_released() {
    let clock = Arc::new(FixedClock::new(t(0)));
    let mut cfg = settings();
    cfg.max_concurrency = 1;
    let ctrl = AdmissionController::new(cfg, clock);
    let rev = revision(4);
    let a = tenant(1);
    let first = ctrl.admit(ctrl.ticket(&a, &rev, 10, t(60_000))).unwrap();
    let mut second = ctrl.admit(ctrl.ticket(&a, &rev, 10, t(60_000))).unwrap();
    assert!(second.is_waiting());
    // The first one's grant sits in its channel; dropping the pending drops
    // it, which releases the slot and grants the second.
    drop(first);
    assert!(!second.is_waiting());
    drop(second);
    let info = ctrl.snapshot(&a);
    assert_eq!((info.queue.length, info.in_flight), (0, 0));
}

// ---------------------------------------------------------------------------
// scale to zero, min_ready, cooldown and drains (PLT-4635)
// ---------------------------------------------------------------------------

/// A ticket with an explicit scale policy.
fn scale_ticket(
    tenant: &TenantId,
    revision: &RevisionId,
    min_ready: u32,
    idle_ttl_seconds: u64,
    cooldown_seconds: u64,
) -> Ticket {
    Ticket {
        min_ready,
        idle_ttl_seconds,
        scale_down_cooldown_seconds: cooldown_seconds,
        ..ticket(tenant, revision, 4, t(3_600_000))
    }
}

/// Serve one invocation of `tk` on a cold environment at `now` and park the
/// environment idle. Returns its reservation.
fn serve_and_park(s: &mut AdmissionState, tk: Ticket, now: Timestamp) -> ReservationId {
    let w = s.enqueue(tk, false, now).unwrap();
    let mut d = Deliveries::default();
    d.collect(s);
    let (r, kind) = d.granted[&w];
    assert_eq!(kind, GrantKind::Cold);
    s.ready(r);
    s.park(r, now);
    s.parked(r, now);
    r
}

fn idle_since(at: Timestamp) -> ScaleDown {
    ScaleDown::Idle { idle_since: at }
}

/// Acceptance 2 / 4 (timer race): the sweeper decided an environment is past
/// its TTL, then an invocation arrives before it acts. The invocation is
/// promised the idle environment, so the scale-down is aborted; the
/// environment is not terminated under it. In the other order the sweeper
/// wins, the environment is no longer idle, and the arrival starts cold
/// instead of being promised an environment that is going away.
#[test]
fn an_arrival_between_the_sweep_decision_and_the_terminate_aborts_the_scale_down() {
    let mut s = AdmissionState::new(settings());
    let (a, rev) = (tenant(1), RevisionId::generate());
    let r = serve_and_park(&mut s, scale_ticket(&a, &rev, 0, 60, 30), t(0));
    // Sweep scheduled at t = 200 s (past TTL and cooldown)...
    let now = t(200_000);
    // ...a request arrives first and is promised the idle environment.
    let mut d = Deliveries::default();
    let w = s
        .enqueue(scale_ticket(&a, &rev, 0, 60, 30), false, now)
        .unwrap();
    d.collect(&mut s);
    let (promise, kind) = d.granted[&w];
    assert_eq!(kind, GrantKind::Warm);
    assert_eq!(
        s.try_scale_down(r, idle_since(t(0)), now),
        Err(KeepReason::Promised),
        "the sweep is aborted: the environment is promised"
    );
    assert_eq!(s.state_of(r), Some(ResState::Idle));
    // The claimer takes it: busy environments are never scaled down.
    let busy = s.adopt(Some(promise), r, now);
    assert_eq!(
        s.try_scale_down(busy, idle_since(t(0)), now),
        Err(KeepReason::NotIdle)
    );
    s.check_invariants();

    // The other order: the sweeper decides first and wins.
    s.park(busy, now);
    s.parked(busy, now);
    let later = t(400_000);
    assert_eq!(s.try_scale_down(busy, idle_since(now), later), Ok(()));
    assert_eq!(s.state_of(busy), Some(ResState::Draining));
    let mut d = Deliveries::default();
    let w = s
        .enqueue(scale_ticket(&a, &rev, 0, 60, 30), false, later)
        .unwrap();
    d.collect(&mut s);
    assert_eq!(
        d.granted[&w].1,
        GrantKind::Cold,
        "a draining environment is never promised"
    );
    s.check_invariants();
}

/// Acceptance 4 (no flapping): no scale-down within the cooldown after an
/// activation, however long the environment has been idle; right after the
/// cooldown it may go, and a new activation starts the cooldown again.
#[test]
fn no_scale_down_within_the_cooldown_after_an_activation() {
    let mut s = AdmissionState::new(settings());
    let (a, rev) = (tenant(1), RevisionId::generate());
    let tk = || scale_ticket(&a, &rev, 0, 1, 30);
    let r = serve_and_park(&mut s, tk(), t(0));
    let snap = s.snapshot(&a, t(0));
    let event = snap.revisions[0].last_scale_event.clone().unwrap();
    assert_eq!(
        (event.kind.as_str(), event.reason.as_str()),
        ("activation", "backlog")
    );
    // Idle 10 s > TTL 1 s, but within the 30 s cooldown.
    assert_eq!(
        s.try_scale_down(r, idle_since(t(0)), t(10_000)),
        Err(KeepReason::Cooldown)
    );
    assert_eq!(
        s.try_scale_down(r, idle_since(t(29_000)), t(29_999)),
        Err(KeepReason::IdleTtl)
    );
    assert_eq!(s.try_scale_down(r, idle_since(t(0)), t(30_000)), Ok(()));
    let snap = s.snapshot(&a, t(30_000));
    let event = snap.revisions[0].last_scale_event.clone().unwrap();
    assert_eq!(event.kind, "scale_to_zero");
    s.release(r, t(30_001));
    // Zero environments; the last decision stays visible.
    let snap = s.snapshot(&a, t(30_002));
    assert_eq!(snap.revisions.len(), 1);
    assert_eq!(snap.revisions[0].environments.idle, 0);
    assert_eq!(
        snap.revisions[0].last_scale_event.as_ref().unwrap().kind,
        "scale_to_zero"
    );

    // Re-activation at 31 s restarts the cooldown: no immediate re-drain.
    let r2 = serve_and_park(&mut s, tk(), t(31_000));
    assert_eq!(
        s.try_scale_down(r2, idle_since(t(31_000)), t(40_000)),
        Err(KeepReason::Cooldown)
    );
    assert_eq!(
        s.try_scale_down(r2, idle_since(t(31_000)), t(61_000)),
        Ok(())
    );
    s.check_invariants();
}

/// Acceptance 2: an idle environment is not scaled down while invocations of
/// its revision wait in the queue (here: held back by the tenant quota).
#[test]
fn waiting_invocations_keep_the_revisions_idle_environment() {
    let mut cfg = settings();
    cfg.tenant_defaults.max_concurrency = Some(1);
    let mut s = AdmissionState::new(cfg);
    let a = tenant(1);
    let (x, y) = (RevisionId::generate(), RevisionId::generate());
    let idle_y = serve_and_park(&mut s, scale_ticket(&a, &y, 0, 1, 0), t(0));
    // X takes the tenant's only in-flight slot.
    let mut d = Deliveries::default();
    let wx = s
        .enqueue(scale_ticket(&a, &x, 0, 1, 0), false, t(10))
        .unwrap();
    d.collect(&mut s);
    let (busy_x, _) = d.granted[&wx];
    s.ready(busy_x);
    // A Y invocation waits on the quota.
    let wy = s
        .enqueue(scale_ticket(&a, &y, 0, 1, 0), false, t(20))
        .unwrap();
    d.collect(&mut s);
    assert!(s.is_queued(wy));
    assert_eq!(
        s.try_scale_down(idle_y, idle_since(t(0)), t(120_000)),
        Err(KeepReason::Queued)
    );
    // Once it is served (warm, on that environment), nothing is left to keep.
    s.release(busy_x, t(120_001));
    d.collect(&mut s);
    assert_eq!(d.granted[&wy].1, GrantKind::Warm);
    s.check_invariants();
}

/// `min_ready`: pre-starts converge to `min_ready` and never beyond, count
/// against every cap, never go ahead of a waiter, and the environments they
/// keep are not scaled down while the revision is routed.
#[test]
fn min_ready_pre_starts_converge_within_the_caps_and_are_kept_while_routed() {
    let mut cfg = settings();
    cfg.max_concurrency = 3;
    let mut s = AdmissionState::new(cfg);
    let (a, rev, other) = (tenant(1), RevisionId::generate(), RevisionId::generate());
    let tk = || scale_ticket(&a, &rev, 2, 1, 0);
    assert_eq!(
        s.try_prestart(tk(), t(0)),
        Err(PrestartSkip::NotRouted),
        "an unrouted revision is not pre-started"
    );
    s.set_routed([rev.clone()].into_iter().collect());
    let r1 = s.try_prestart(tk(), t(0)).unwrap();
    let r2 = s.try_prestart(tk(), t(0)).unwrap();
    assert_eq!(s.try_prestart(tk(), t(0)), Err(PrestartSkip::Satisfied));
    assert_eq!(s.provisioned(&rev), 2);
    for r in [r1, r2] {
        assert_eq!(s.state_of(r), Some(ResState::Starting));
        s.ready(r);
        s.park(r, t(10));
        s.parked(r, t(10));
    }
    s.check_invariants();
    // Repeated reconciles change nothing.
    for i in 0..50 {
        assert_eq!(
            s.try_prestart(tk(), t(20 + i)),
            Err(PrestartSkip::Satisfied)
        );
    }
    // Kept for min_ready, whatever the TTL.
    assert_eq!(
        s.try_scale_down(r1, idle_since(t(10)), t(900_000)),
        Err(KeepReason::MinReady)
    );
    let snap = s.snapshot(&a, t(900_000));
    let info = snap
        .revisions
        .iter()
        .find(|r| r.revision_id == rev.to_string())
        .unwrap();
    assert_eq!((info.min_ready, info.environments.idle), (2, 2));
    assert_eq!(info.route_state, "routed");

    // Waiters first: with the node full of busy environments and one
    // invocation waiting, no pre-start takes capacity.
    s.set_routed([rev.clone(), other.clone()].into_iter().collect());
    let mut d = Deliveries::default();
    let busy: Vec<ReservationId> = (0..3)
        .map(|i| {
            let w = s
                .enqueue(ticket(&a, &other, 8, t(3_600_000)), false, t(1_000 + i))
                .unwrap();
            d.collect(&mut s);
            d.granted[&w].0
        })
        .collect();
    let queued = s
        .enqueue(ticket(&a, &other, 8, t(3_600_000)), false, t(1_010))
        .unwrap();
    d.collect(&mut s);
    assert!(s.is_queued(queued));
    // One pooled environment is needed again: scale r2's revision below
    // min_ready by draining it (the revision stops being routed).
    s.set_routed([other.clone()].into_iter().collect());
    assert_eq!(s.try_scale_down(r2, idle_since(t(10)), t(900_000)), Ok(()));
    s.release(r2, t(900_001));
    s.set_routed([rev.clone(), other.clone()].into_iter().collect());
    assert_eq!(
        s.try_prestart(tk(), t(900_002)),
        Err(PrestartSkip::WaitersFirst)
    );
    s.withdraw(queued);
    assert_eq!(
        s.try_prestart(tk(), t(900_003)),
        Err(PrestartSkip::Blocked("capacity")),
        "node max_concurrency is full of busy environments"
    );
    s.release(busy[0], t(900_004));
    let r3 = s.try_prestart(tk(), t(900_005)).unwrap();
    assert_eq!(
        s.try_prestart(tk(), t(900_005)),
        Err(PrestartSkip::Satisfied)
    );
    s.check_invariants();
    for r in [r1, r3, busy[1], busy[2]] {
        s.release(r, t(900_010));
    }
    s.check_invariants();
}

/// Acceptance 4 (deletion): a deletion refuses the waiters of its revisions
/// at once and every later arrival, drains idle environments without waiting
/// for the TTL or the cooldown, and is never undone.
#[test]
fn a_function_deletion_refuses_waiters_and_arrivals_and_drains_at_once() {
    let mut cfg = settings();
    cfg.tenant_defaults.max_concurrency = Some(1);
    let mut s = AdmissionState::new(cfg);
    let a = tenant(1);
    let (rev, other) = (RevisionId::generate(), RevisionId::generate());
    let idle = serve_and_park(&mut s, scale_ticket(&a, &rev, 1, 600, 600), t(0));
    let mut d = Deliveries::default();
    let w_other = s
        .enqueue(ticket(&a, &other, 4, t(3_600_000)), false, t(5))
        .unwrap();
    d.collect(&mut s);
    let (busy_other, _) = d.granted[&w_other];
    s.ready(busy_other);
    let waiting = s
        .enqueue(scale_ticket(&a, &rev, 1, 600, 600), false, t(6))
        .unwrap();
    d.collect(&mut s);
    assert!(s.is_queued(waiting));
    s.set_routed([rev.clone()].into_iter().collect());

    assert!(s.begin_drain(&rev, DrainReason::FunctionDeleted, t(10)));
    d.collect(&mut s);
    assert_eq!(
        d.rejected.get(&waiting),
        Some(&RejectReason::FunctionDeleted)
    );
    let refused = s.enqueue(scale_ticket(&a, &rev, 1, 600, 600), false, t(11));
    assert_eq!(refused.unwrap_err().reason, RejectReason::FunctionDeleted);
    assert!(matches!(
        s.try_prestart(scale_ticket(&a, &rev, 1, 600, 600), t(12)),
        Err(PrestartSkip::Refused(r)) if r.reason == RejectReason::FunctionDeleted
    ));
    // No TTL, no cooldown, no min_ready for a drained revision.
    assert_eq!(s.try_scale_down(idle, idle_since(t(12)), t(13)), Ok(()));
    let snap = s.snapshot(&a, t(14));
    let info = snap
        .revisions
        .iter()
        .find(|r| r.revision_id == rev.to_string())
        .unwrap();
    assert_eq!(info.route_state, "deleting");
    let event = info.last_scale_event.as_ref().unwrap();
    assert_eq!(
        (event.kind.as_str(), event.reason.as_str()),
        ("drain", "function_deleted")
    );
    assert!(!s.end_drain(&rev), "a deletion is never undone");
    assert!(
        !s.begin_drain(&rev, DrainReason::AliasSwitch, t(15)),
        "and not downgraded"
    );
    assert_eq!(s.drain_reason(&rev), Some(DrainReason::FunctionDeleted));
    s.release(idle, t(16));
    s.release(busy_other, t(16));
    assert!(s.revision_is_empty(&rev));
    s.check_invariants();
}

/// Alias switch: the superseded revision's idle environments are drained
/// without TTL, cooldown or min_ready, but never out from under a promise;
/// a rollback ends the drain.
#[test]
fn an_alias_switch_drain_skips_the_ttl_but_never_takes_a_promised_environment() {
    let mut s = AdmissionState::new(settings());
    let (a, old) = (tenant(1), RevisionId::generate());
    s.set_routed([old.clone()].into_iter().collect());
    let tk = || scale_ticket(&a, &old, 1, 600, 600);
    let e1 = serve_and_park(&mut s, tk(), t(0));
    // An invocation of the old revision accepted before the switch is
    // promised e1.
    let mut d = Deliveries::default();
    let w = s.enqueue(tk(), false, t(1)).unwrap();
    d.collect(&mut s);
    let (promise, kind) = d.granted[&w];
    assert_eq!(kind, GrantKind::Warm);

    s.set_routed(std::collections::HashSet::new());
    assert!(s.begin_drain(&old, DrainReason::AliasSwitch, t(2)));
    assert_eq!(
        s.try_scale_down(
            e1,
            ScaleDown::Drain {
                reason: "alias_switch"
            },
            t(3)
        ),
        Err(KeepReason::Promised)
    );
    let busy = s.adopt(Some(promise), e1, t(4));
    s.park(busy, t(5));
    s.parked(busy, t(5));
    assert_eq!(s.try_scale_down(busy, idle_since(t(5)), t(6)), Ok(()));
    let snap = s.snapshot(&a, t(6));
    assert_eq!(snap.revisions[0].route_state, "superseded");
    s.release(busy, t(7));
    assert!(s.end_drain(&old), "a rollback ends an alias-switch drain");
    assert_eq!(s.drain_reason(&old), None);
    s.check_invariants();
}

/// The ledger stays exact when scale-downs, pre-starts, drains, promises and
/// releases interleave at random (the PLT-4634 property, with the PLT-4635
/// operations added).
#[test]
fn scale_operations_keep_the_ledger_exact_under_random_interleavings() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }
    for seed in 1..=8u64 {
        let mut rng = Rng(0xA076_1D64_78BD_642F ^ seed.wrapping_mul(0xE703_7ED1_A0B4_28DB));
        let mut cfg = settings();
        cfg.max_concurrency = 5;
        cfg.node.memory_mib = Some(1_700);
        cfg.start_rate_per_second = 10;
        cfg.start_burst = 5;
        let mut s = AdmissionState::new(cfg);
        let a = tenant(1);
        let revs: Vec<RevisionId> = (0..3).map(|_| RevisionId::generate()).collect();
        let mut held: Vec<ReservationId> = Vec::new();
        let mut waiters: Vec<WaiterId> = Vec::new();
        let mut now = 0i64;
        for _ in 0..2_000 {
            now += rng.below(3_000) as i64;
            let rev = revs[rng.below(3) as usize].clone();
            let tk = scale_ticket(&a, &rev, rng.below(3) as u32, 5, 10);
            match rng.below(10) {
                0 | 1 => {
                    if let Ok(w) = s.enqueue(tk, rng.below(4) == 0, t(now)) {
                        waiters.push(w);
                    }
                }
                2 => {
                    if let Ok(r) = s.try_prestart(tk, t(now)) {
                        held.push(r);
                    }
                }
                3 => {
                    let routed = revs.iter().filter(|_| rng.below(2) == 0).cloned().collect();
                    s.set_routed(routed);
                }
                4 => {
                    let reason = if rng.below(4) == 0 {
                        DrainReason::FunctionDeleted
                    } else {
                        DrainReason::AliasSwitch
                    };
                    if rng.below(2) == 0 {
                        s.begin_drain(&rev, reason, t(now));
                    } else {
                        s.end_drain(&rev);
                    }
                }
                5 => {
                    if let Some(w) = waiters.pop() {
                        s.withdraw(w);
                    }
                }
                _ => {
                    if held.is_empty() {
                        continue;
                    }
                    let i = rng.below(held.len() as u64) as usize;
                    let r = held[i];
                    match s.state_of(r) {
                        Some(ResState::Starting) => {
                            s.start_result(r, rng.below(5) != 0, t(now));
                            s.ready(r);
                        }
                        Some(ResState::Busy) => {
                            if rng.below(2) == 0 {
                                s.park(r, t(now));
                            } else {
                                s.release(held.swap_remove(i), t(now));
                            }
                        }
                        Some(ResState::Parking) => s.parked(r, t(now)),
                        Some(ResState::Idle) => {
                            let how = if rng.below(3) == 0 {
                                ScaleDown::Drain { reason: "test" }
                            } else {
                                ScaleDown::Idle {
                                    idle_since: t(now - rng.below(20_000) as i64),
                                }
                            };
                            let _ = s.try_scale_down(r, how, t(now));
                        }
                        Some(ResState::Draining) => s.release(held.swap_remove(i), t(now)),
                        Some(ResState::Promised) => {
                            let revision = s.revision_of(r);
                            let idle = held.iter().copied().find(|x| {
                                s.state_of(*x) == Some(ResState::Idle)
                                    && s.revision_of(*x) == revision
                            });
                            held.swap_remove(i);
                            match idle {
                                Some(idle) => {
                                    s.adopt(Some(r), idle, t(now));
                                }
                                None => {
                                    if s.redeem(r, t(now)) {
                                        held.push(r);
                                    }
                                }
                            }
                        }
                        None => {
                            held.swap_remove(i);
                        }
                    }
                }
            }
            for (w, o) in s.take_outbox() {
                waiters.retain(|x| *x != w);
                if let Outcome::Granted { reservation, .. } = o {
                    held.push(reservation);
                }
            }
            s.check_invariants();
        }
        for w in waiters {
            s.withdraw(w);
        }
        s.take_outbox();
        for r in held {
            s.release(r, t(now + 1));
        }
        for (_, o) in s.take_outbox() {
            if let Outcome::Granted { reservation, .. } = o {
                s.release(reservation, t(now + 2));
            }
        }
        s.check_invariants();
        assert_eq!(s.reserved(), Resources::ZERO, "seed {seed}");
    }
}

// ---------------------------------------------------------------------------
// metrics (PLT-4637)
// ---------------------------------------------------------------------------

fn tenant_metrics<'a>(m: &'a AdmissionMetrics, t: &TenantId) -> &'a TenantMetrics {
    m.tenants.iter().find(|x| &x.tenant == t).expect("tenant")
}

/// Every gauge and counter `GET /metrics` reads from admission follows the
/// state transitions, on a fake clock: environments by state, reservations,
/// queue length and wait ages (node and per tenant), grants by kind and per
/// tenant, scale events, start results, breaker opens and rejections.
#[test]
fn metrics_follow_admission_state_transitions_on_a_fake_clock() {
    let mut cfg = settings();
    cfg.max_concurrency = 2;
    cfg.max_queue = 2;
    let mut s = AdmissionState::new(cfg);
    let mut d = Deliveries::default();
    let (a, b) = (tenant(1), tenant(2));
    let (rev_a, rev_b) = (RevisionId::generate(), RevisionId::generate());

    let m = s.metrics(t(0));
    assert_eq!(m.in_flight, 0);
    assert_eq!(m.queue_length, 0);
    assert_eq!(m.oldest_wait_seconds, None);
    assert!(m.revisions.is_empty());
    assert_eq!(m.counters, AdmissionCounters::default());

    // 0 -> 2 starting, 1 waiting.
    for _ in 0..3 {
        s.enqueue(ticket(&a, &rev_a, 4, t(60_000)), false, t(0))
            .unwrap();
    }
    d.collect(&mut s);
    let m = s.metrics(t(0));
    assert_eq!(m.environments.starting, 2);
    assert_eq!(m.in_flight, 2);
    assert_eq!(m.reserved.memory_mib, 2 * 280);
    assert_eq!(m.queue_length, 1);
    assert_eq!(m.counters.arrivals, 3);
    assert_eq!(m.counters.grants_cold, 2);
    assert_eq!(
        m.counters.scale_events.get(&("activation", "backlog")),
        Some(&1)
    );
    assert_eq!(
        m.counters.scale_events.get(&("scale_up", "backlog")),
        Some(&1)
    );
    assert_eq!(tenant_metrics(&m, &a).grants, 2);
    assert_eq!(m.revisions.len(), 1);
    assert_eq!(m.revisions[0].environments.starting, 2);
    assert_eq!(m.revisions[0].queued, 1);
    assert_eq!(m.revisions[0].breaker, "closed");

    // The wait ages advance with the clock.
    let m = s.metrics(t(1_500));
    assert_eq!(m.oldest_wait_seconds, Some(1.5));
    assert_eq!(tenant_metrics(&m, &a).oldest_wait_seconds, Some(1.5));

    // Booted: starting -> busy, start results counted.
    let granted: Vec<ReservationId> = d.granted.values().map(|(r, _)| *r).collect();
    for r in &granted {
        s.start_result(*r, true, t(1_600));
        s.ready(*r);
    }
    let m = s.metrics(t(1_600));
    assert_eq!((m.environments.starting, m.environments.busy), (0, 2));
    assert_eq!(m.counters.start_successes, 2);

    // Tenant B waits behind a full node while A's waiter is older.
    s.enqueue(ticket(&b, &rev_b, 4, t(60_000)), false, t(2_000))
        .unwrap();
    let m = s.metrics(t(4_000));
    assert_eq!(m.queue_length, 2);
    assert_eq!(tenant_metrics(&m, &b).oldest_wait_seconds, Some(2.0));
    assert_eq!(tenant_metrics(&m, &b).grants, 0);
    // The queue is full now: the next arrival is refused and counted.
    assert_eq!(
        s.enqueue(ticket(&a, &rev_a, 4, t(60_000)), false, t(4_000))
            .unwrap_err()
            .reason,
        RejectReason::QueueFull
    );

    // One environment ends: the fair queue serves B (0 in flight) first.
    s.release(granted[0], t(4_500));
    d.collect(&mut s);
    let m = s.metrics(t(4_500));
    assert_eq!(tenant_metrics(&m, &b).grants, 1);
    assert_eq!(tenant_metrics(&m, &b).oldest_wait_seconds, None);
    assert_eq!(m.rejections.get(&RejectReason::QueueFull), Some(&1));
    assert_eq!(m.counters.grants_cold, 3);
    s.check_invariants();

    // Everything ends: back to 0 with the counters kept.
    let rest: Vec<ReservationId> = d
        .granted
        .values()
        .map(|(r, _)| *r)
        .filter(|r| *r != granted[0])
        .collect();
    for r in rest {
        s.release(r, t(5_000));
    }
    for (_, o) in s.take_outbox() {
        if let Outcome::Granted { reservation, .. } = o {
            s.release(reservation, t(5_100));
        }
    }
    let m = s.metrics(t(5_200));
    assert_eq!(m.in_flight, 0);
    assert_eq!(m.reserved, Resources::ZERO);
    assert_eq!(m.queue_length, 0);
    assert_eq!(
        m.counters.arrivals, 5,
        "the refused arrival was counted too"
    );
    assert!(m.counters.grants_cold >= 3);
}

/// Coalesced waits and starts avoided: an arrival held back while an
/// environment of its revision is parking takes that environment instead of
/// booting one; three failed boots open the breaker once.
#[test]
fn metrics_count_coalesced_starts_and_breaker_opens() {
    let mut s = AdmissionState::new(settings());
    let (a, rev) = (tenant(1), RevisionId::generate());
    let r = serve_and_park(&mut s, ticket(&a, &rev, 10, t(60_000)), t(0));
    // Take the idle environment, then send it back into the pool (parking).
    let w = s
        .enqueue(ticket(&a, &rev, 10, t(60_000)), false, t(100))
        .unwrap();
    let mut d = Deliveries::default();
    d.collect(&mut s);
    let (promise, kind) = d.granted[&w];
    assert_eq!(kind, GrantKind::Warm);
    let busy = s.adopt(Some(promise), r, t(110));
    s.park(busy, t(200));
    // An arrival while it parks is coalesced onto it.
    let w = s
        .enqueue(ticket(&a, &rev, 10, t(60_000)), false, t(210))
        .unwrap();
    d.collect(&mut s);
    assert!(s.is_queued(w));
    s.parked(busy, t(300));
    d.collect(&mut s);
    assert_eq!(d.granted[&w].1, GrantKind::Warm);
    let m = s.metrics(t(300));
    assert_eq!(m.counters.grants_warm, 2);
    assert_eq!(m.counters.coalesced_waits, 1);
    assert_eq!(m.counters.starts_avoided, 1);
    assert_eq!(m.environments.promised, 1);

    let bad = RevisionId::generate();
    for _ in 0..3 {
        s.enqueue(ticket(&a, &bad, 3, t(60_000)), false, t(400))
            .unwrap();
    }
    let mut d = Deliveries::default();
    d.collect(&mut s);
    for (r, _) in d.granted.values() {
        s.start_result(*r, false, t(500));
    }
    let m = s.metrics(t(500));
    assert_eq!(m.counters.start_failures, 3);
    assert_eq!(m.counters.breaker_opens, 1);
    let bad_rev = m.revisions.iter().find(|x| x.revision == bad).unwrap();
    assert_eq!(bad_rev.breaker, "open");
}
