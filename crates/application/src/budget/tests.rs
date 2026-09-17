//! Unit and property tests of the budget store and reservation math (PLT-4643).

use std::collections::HashMap;
use std::sync::Arc;

use tachyon_serverless_domain::{
    AttemptId, AttemptKind, FunctionId, InvocationId, Metered, TenantId, Timestamp, UsageBytes,
    UsageEvent, UsageEventType, UsageOutcome, UsageSegments,
};

use super::*;
use crate::usage::PriceTable;

const TENANT: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const FUNCTION: &str = "fn_01hzzzzzzzzzzzzzzzzzzzzzzf";

/// Deterministic xorshift, like `usage::tests`.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn t(ms: i64) -> Timestamp {
    chrono::DateTime::parse_from_rfc3339("2026-09-17T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
        + chrono::Duration::milliseconds(ms)
}

fn new_res(id: &str, amount: u64, limit: Option<u64>) -> NewReservation {
    NewReservation {
        reservation_id: id.into(),
        tenant_id: TENANT.into(),
        function_id: FUNCTION.into(),
        invocation_id: id.into(),
        period: "2026-09".into(),
        amount_micros: amount,
        expires_at: t(60_000),
        price_table_version: "v1".into(),
        config_generation: 1,
        tenant_limit: limit,
        function_limit: None,
    }
}

fn tenant_totals(store: &BudgetStore) -> ScopeTotals {
    store
        .totals(TENANT, "2026-09")
        .unwrap()
        .into_iter()
        .find(|s| s.scope.is_empty())
        .unwrap()
}

fn no_alerts() -> AlertLimits {
    AlertLimits::default()
}

#[test]
fn a_reservation_is_refused_when_it_does_not_fit_and_settlement_returns_the_excess() {
    let store = BudgetStore::open(None);
    let limit = Some(1_000);
    assert_eq!(
        store.reserve(&new_res("r1", 600, limit), t(0)).unwrap(),
        ReserveOutcome::Reserved
    );
    match store.reserve(&new_res("r2", 600, limit), t(1)).unwrap() {
        ReserveOutcome::Refused(r) => {
            assert_eq!(r.scope, BudgetScope::Tenant);
            assert_eq!((r.committed_micros, r.requested_micros), (600, 600));
        }
        other => panic!("{other:?}"),
    }
    // The run finished; its measured charge is 150: 450 comes back.
    assert!(
        store
            .finish("r1", &["a1".into()], true, Some(3), t(2))
            .unwrap()
    );
    let s = store
        .settle(
            "r1",
            Settlement {
                measured_micros: 150,
                complete: true,
            },
            t(3),
            &no_alerts(),
        )
        .unwrap();
    assert!(s.changed && s.state == ReservationState::Settled);
    let totals = tenant_totals(&store);
    assert_eq!(
        (
            totals.reserved_micros,
            totals.settled_micros,
            totals.held_micros
        ),
        (0, 150, 0)
    );
    assert_eq!(totals.refusals, 1);
    // Now the second one fits.
    assert_eq!(
        store.reserve(&new_res("r2", 600, limit), t(4)).unwrap(),
        ReserveOutcome::Reserved
    );
    store.verify_totals().unwrap();
}

#[test]
fn duplicates_are_no_ops_and_terminal_states_never_move() {
    let store = BudgetStore::open(None);
    store.reserve(&new_res("r", 500, None), t(0)).unwrap();
    assert_eq!(
        store.reserve(&new_res("r", 500, None), t(1)).unwrap(),
        ReserveOutcome::Exists(ReservationState::Reserved)
    );
    let settle = Settlement {
        measured_micros: 100,
        complete: true,
    };
    assert!(
        store
            .settle("r", settle, t(2), &no_alerts())
            .unwrap()
            .changed
    );
    let again = store.settle("r", settle, t(3), &no_alerts()).unwrap();
    assert!(!again.changed);
    assert_eq!(again.state, ReservationState::Settled);
    // Neither release nor expiry moves a settled reservation.
    assert!(!store.release("r", t(4)).unwrap().changed);
    assert!(!store.expire("r", t(999_999), &no_alerts()).unwrap().changed);
    assert_eq!(
        store.reserve(&new_res("r", 500, None), t(5)).unwrap(),
        ReserveOutcome::Exists(ReservationState::Settled)
    );
    let totals = tenant_totals(&store);
    assert_eq!((totals.reserved_micros, totals.settled_micros), (0, 100));
    assert_eq!(totals.settlements, 1);
    store.verify_totals().unwrap();
}

#[test]
fn a_finished_run_is_never_released_or_expired_and_an_unfinished_one_expires_to_a_hold() {
    let store = BudgetStore::open(None);
    store.reserve(&new_res("done", 400, None), t(0)).unwrap();
    store.reserve(&new_res("lost", 300, None), t(0)).unwrap();
    store.finish("done", &[], true, None, t(1)).unwrap();
    // Nothing started must be proven by the run not having finished.
    assert!(!store.release("done", t(2)).unwrap().changed);
    // Expiry only after `expires_at`, and never for a finished run.
    assert!(
        !store
            .expire("lost", t(59_999), &no_alerts())
            .unwrap()
            .changed
    );
    assert!(
        !store
            .expire("done", t(60_000), &no_alerts())
            .unwrap()
            .changed
    );
    let due = store.due_for_expiry(t(60_000), 10).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].reservation_id, "lost");
    let e = store.expire("lost", t(60_000), &no_alerts()).unwrap();
    assert!(e.changed && e.state == ReservationState::Expired);
    let totals = tenant_totals(&store);
    // The maximum of the lost run stays held; nothing is billed for it.
    assert_eq!(
        (
            totals.reserved_micros,
            totals.settled_micros,
            totals.held_micros
        ),
        (400, 0, 300)
    );
    assert_eq!(totals.committed(), 700);
    store.verify_totals().unwrap();
}

#[test]
fn an_incomplete_measurement_holds_the_rest_and_an_overrun_is_settled_in_full() {
    let store = BudgetStore::open(None);
    store
        .reserve(&new_res("partial", 1_000, None), t(0))
        .unwrap();
    store.reserve(&new_res("over", 100, None), t(0)).unwrap();
    store
        .settle(
            "partial",
            Settlement {
                measured_micros: 250,
                complete: false,
            },
            t(1),
            &no_alerts(),
        )
        .unwrap();
    store
        .settle(
            "over",
            Settlement {
                measured_micros: 130,
                complete: true,
            },
            t(1),
            &no_alerts(),
        )
        .unwrap();
    let rows: HashMap<String, ReservationRow> = store
        .reservations_of(TENANT, "2026-09")
        .unwrap()
        .into_iter()
        .map(|r| (r.reservation_id.clone(), r))
        .collect();
    assert_eq!(
        (rows["partial"].settled_micros, rows["partial"].held_micros),
        (250, 750)
    );
    assert_eq!(
        (rows["over"].settled_micros, rows["over"].overrun_micros),
        (130, 30)
    );
    let totals = tenant_totals(&store);
    assert_eq!(
        (
            totals.settled_micros,
            totals.held_micros,
            totals.overrun_micros
        ),
        (380, 750, 30)
    );
    store.verify_totals().unwrap();
}

#[test]
fn function_limits_are_checked_next_to_the_tenant_limit() {
    let store = BudgetStore::open(None);
    let mut r = new_res("f1", 300, Some(10_000));
    r.function_limit = Some(500);
    assert_eq!(store.reserve(&r, t(0)).unwrap(), ReserveOutcome::Reserved);
    let mut r2 = new_res("f2", 300, Some(10_000));
    r2.function_limit = Some(500);
    match store.reserve(&r2, t(0)).unwrap() {
        ReserveOutcome::Refused(refusal) => assert_eq!(refusal.scope, BudgetScope::Function),
        other => panic!("{other:?}"),
    }
    // Another function of the same tenant is not limited by it.
    let mut r3 = new_res("g1", 300, Some(10_000));
    r3.function_id = "fn_01hzzzzzzzzzzzzzzzzzzzzzzg".into();
    r3.function_limit = Some(500);
    assert_eq!(store.reserve(&r3, t(0)).unwrap(), ReserveOutcome::Reserved);
}

#[test]
fn a_lowered_limit_fails_the_recheck_of_a_queued_reservation() {
    let store = BudgetStore::open(None);
    store
        .reserve(&new_res("q", 400, Some(1_000)), t(0))
        .unwrap();
    assert_eq!(
        store.recheck("q", Some(1_000), None).unwrap(),
        Ok(ReservationState::Reserved)
    );
    let refused = store.recheck("q", Some(300), None).unwrap().unwrap_err();
    assert_eq!(refused.scope, BudgetScope::Tenant);
    assert_eq!(refused.requested_micros, 400);
}

#[test]
fn alerts_fire_once_per_threshold_and_never_refuse() {
    let store = BudgetStore::open(None);
    let alerts = AlertLimits {
        tenant: Some(BudgetLimits {
            soft_limit_micros: Some(1_000),
            alert_thresholds_percent: vec![50, 80, 100],
            hard_limit_micros: None,
        }),
        function: None,
    };
    let mut fired = Vec::new();
    for (i, measured) in [300u64, 300, 300, 300].into_iter().enumerate() {
        let id = format!("a{i}");
        // No hard limit: the soft limit never refuses, even above 100 %.
        assert_eq!(
            store.reserve(&new_res(&id, 400, None), t(0)).unwrap(),
            ReserveOutcome::Reserved
        );
        let tr = store
            .settle(
                &id,
                Settlement {
                    measured_micros: measured,
                    complete: true,
                },
                t(i as i64),
                &alerts,
            )
            .unwrap();
        fired.push(
            tr.alerts
                .iter()
                .map(|a| a.threshold_percent)
                .collect::<Vec<_>>(),
        );
    }
    // 300 → none, 600 → 50, 900 → 80, 1200 → 100.
    assert_eq!(fired, vec![vec![], vec![50], vec![80], vec![100]]);
    assert_eq!(store.alerts(TENANT, "2026-09").unwrap().len(), 3);
}

#[test]
fn an_unavailable_store_refuses_every_operation_and_recovers() {
    let store = BudgetStore::open(None);
    store.force_unavailable(true);
    assert!(store.reserve(&new_res("x", 1, None), t(0)).is_err());
    assert!(store.stats().is_err());
    store.force_unavailable(false);
    assert!(store.reserve(&new_res("x", 1, None), t(0)).is_ok());
}

/// Many threads, each with its own connection to the same database file,
/// reserving concurrently against one hard limit: the reserved sum never
/// exceeds it, and exactly `limit / amount` reservations win.
#[test]
fn concurrent_reservations_on_separate_connections_never_exceed_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("budget.db");
    let limit = 10_000u64;
    let amount = 700u64;
    let threads: Vec<_> = (0..16)
        .map(|i| {
            let path = path.clone();
            std::thread::spawn(move || {
                let store = BudgetStore::open(Some(path));
                let mut won = 0;
                for j in 0..10 {
                    let id = format!("t{i}-{j}");
                    match store.reserve(&new_res(&id, amount, Some(limit)), t(0)) {
                        Ok(ReserveOutcome::Reserved) => won += 1,
                        Ok(ReserveOutcome::Refused(_)) => {}
                        other => panic!("{other:?}"),
                    }
                }
                won
            })
        })
        .collect();
    let won: u64 = threads.into_iter().map(|h| h.join().unwrap()).sum();
    let store = BudgetStore::open(Some(path));
    let totals = tenant_totals(&store);
    assert_eq!(won, limit / amount);
    assert_eq!(totals.reserved_micros, won * amount);
    assert!(totals.reserved_micros <= limit);
    assert_eq!(totals.refusals, 160 - won);
    store.verify_totals().unwrap();
}

/// Random interleavings of reserve / finish / settle / release / expire,
/// each duplicated at random, over a few reservations: the totals always
/// equal the sum of the rows, nothing is ever negative, each reservation
/// leaves `reserved` at most once, and the committed amount never exceeds
/// the hard limit plus measured overruns.
#[test]
fn property_duplicate_and_interleaved_transitions_never_break_the_balance() {
    let mut rng = Rng(0x5eed);
    for round in 0..300 {
        let store = BudgetStore::open(None);
        let limit = 1_000 + rng.below(4_000);
        let n = 2 + rng.below(8) as usize;
        let mut transitions: HashMap<String, u32> = HashMap::new();
        let mut now = 0i64;
        for _ in 0..(n * 12) {
            now += rng.below(20_000) as i64;
            let id = format!("r{}", rng.below(n as u64));
            let op = rng.below(6);
            let repeat = 1 + rng.below(3);
            for _ in 0..repeat {
                let tr = match op {
                    0 => {
                        let mut r = new_res(&id, 200 + rng.below(1_500), Some(limit));
                        r.expires_at = t(now + rng.below(40_000) as i64);
                        store.reserve(&r, t(now)).unwrap();
                        None
                    }
                    1 => {
                        store
                            .finish(&id, &["a".into()], rng.below(2) == 0, Some(1), t(now))
                            .unwrap();
                        None
                    }
                    2 => Some(
                        store
                            .settle(
                                &id,
                                Settlement {
                                    measured_micros: rng.below(1_800),
                                    complete: rng.below(3) != 0,
                                },
                                t(now),
                                &no_alerts(),
                            )
                            .unwrap(),
                    ),
                    3 => Some(store.release(&id, t(now)).unwrap()),
                    4 => Some(store.expire(&id, t(now), &no_alerts()).unwrap()),
                    _ => {
                        let _ = store.recheck(&id, Some(limit), None).unwrap();
                        None
                    }
                };
                if let Some(tr) = tr
                    && tr.changed
                {
                    *transitions.entry(id.clone()).or_default() += 1;
                }
            }
            store
                .verify_totals()
                .unwrap_or_else(|e| panic!("round {round}: {e}"));
            let totals = tenant_totals(&store);
            assert!(
                totals.committed() <= limit + totals.overrun_micros,
                "round {round}: committed {} > limit {limit} + overrun {}",
                totals.committed(),
                totals.overrun_micros
            );
        }
        for (id, count) in transitions {
            assert!(
                count <= 1,
                "round {round}: {id} left `reserved` {count} times"
            );
        }
    }
}

// -- maximum charge ---------------------------------------------------------

fn bounds() -> RunBounds {
    RunBounds {
        cpu_millis: 1_000,
        memory_mib: 512,
        window_ms: 40_000,
        queue_timeout_ms: 30_000,
        init_timeout_ms: 10_000,
        handshake_timeout_ms: 5_000,
        execution_timeout_ms: 2_000,
        cancel_grace_ms: 100,
        request_bytes: 20,
        max_response_bytes: 1_000,
        slack_ms: 250,
    }
}

#[test]
fn the_maximum_follows_the_documented_formula() {
    let table = PriceTable::builtin();
    let m = max_charge(&table, &bounds());
    // user_init (10 000 + 5 000) + handler (2 000 + 100), one attempt, + slack.
    assert_eq!(m.billable_ms, 17_350);
    assert_eq!(m.transfer_bytes, 1_040);
    // 17 350 ms × 1 vCPU × 2 500 µ/s = 43 375; × 0.5 GiB × 400 µ/s = 3 470.
    assert_eq!(m.vcpu_micros, 43_375);
    assert_eq!(m.memory_micros, 3_470);
    assert_eq!(m.invocation_micros, 30);
    assert_eq!(m.transfer_micros, 16);
    assert_eq!(m.total_micros, 43_375 + 3_470 + 30 + 16);
    // A table that bills an unboundable segment is refused.
    let mut idle = table.clone();
    idle.billable_segments.push("idle_pooled_ms".into());
    assert!(check_table(&idle).is_err());
    assert!(check_table(&table).is_ok());
}

fn attempt_event(rng: &mut Rng, b: &RunBounds, number: u32, warm_undelivered: bool) -> UsageEvent {
    let tenant = TenantId::parse(TENANT).unwrap();
    let mut e = UsageEvent::new(
        format!("e{number}-{}", rng.next()),
        tenant,
        tachyon_serverless_domain::EnvironmentId::generate(),
        UsageEventType::AttemptSettled,
        1,
        t(0),
    );
    e.function_id = Some(FunctionId::parse(FUNCTION).unwrap());
    e.invocation_id = Some(InvocationId::generate());
    e.attempt_id = Some(AttemptId::generate());
    e.attempt_number = Some(number);
    e.attempt_kind = Some(AttemptKind::from_number(number));
    e.outcome = Some(UsageOutcome::Succeeded);
    let (init, handler) = if warm_undelivered {
        (0, 0)
    } else {
        (
            rng.below(b.init_timeout_ms + b.handshake_timeout_ms + 1),
            rng.below(b.execution_timeout_ms + b.cancel_grace_ms + 1),
        )
    };
    e.segments = UsageSegments {
        queue_wait_ms: Metered::host(rng.below(b.queue_timeout_ms)),
        vm_base_boot_ms: Metered::host(rng.below(5_000)),
        user_init_ms: Metered::host(init),
        handler_ms: Metered::host(handler),
        teardown_ms: Metered::host(rng.below(9_000)),
        idle_pooled_ms: Metered::host(rng.below(90_000)),
    };
    e.resources.requested_cpu_millis = b.cpu_millis;
    e.resources.requested_memory_mib = b.memory_mib;
    e.bytes = UsageBytes {
        request_bytes: Metered::host(b.request_bytes),
        response_bytes: Metered::host(if warm_undelivered {
            0
        } else {
            rng.below(b.max_response_bytes + 1)
        }),
    };
    e
}

/// Whatever a run within its bounds is rated at — one attempt, or an
/// undelivered warm attempt plus its cold retry — never exceeds the
/// reservation computed for it.
#[test]
fn property_the_reservation_covers_every_rating_within_the_bounds() {
    let table = PriceTable::builtin();
    let mut rng = Rng(99);
    for _ in 0..5_000 {
        let b = RunBounds {
            cpu_millis: 100 + rng.below(4_000) as u32,
            memory_mib: 128 + rng.below(8_000) as u32,
            execution_timeout_ms: 1_000 * (1 + rng.below(900)),
            init_timeout_ms: 1_000 * (1 + rng.below(120)),
            request_bytes: rng.below(6 << 20),
            max_response_bytes: rng.below(6 << 20),
            ..bounds()
        };
        let b = RunBounds {
            window_ms: b.execution_timeout_ms + b.init_timeout_ms + b.queue_timeout_ms,
            ..b
        };
        let max = max_charge(&table, &b);
        let events = match rng.below(2) {
            0 => vec![attempt_event(&mut rng, &b, 1, false)],
            _ => vec![
                attempt_event(&mut rng, &b, 1, true),
                attempt_event(&mut rng, &b, 2, false),
            ],
        };
        let (rated_micros, fully) = rated(&events, &table);
        assert!(fully);
        assert!(
            rated_micros <= max.total_micros,
            "rated {rated_micros} > reserved {} for {b:?}",
            max.total_micros
        );
    }
}

// -- configuration ----------------------------------------------------------

#[test]
fn alerts_and_stop_are_separate_settings_and_validated() {
    let ok = BudgetBook::from_toml(&format!(
        r#"
[[tenants]]
tenant_id = "{TENANT}"
soft_limit_micros = 1000
alert_thresholds_percent = [50, 80, 100]
hard_limit_micros = 5000

[[tenants.functions]]
function_id = "{FUNCTION}"
hard_limit_micros = 700

[default_tenant]
hard_limit_micros = 0
"#
    ))
    .unwrap();
    let b = ok
        .budget_for(
            &TenantId::parse(TENANT).unwrap(),
            "calendar_month_utc",
            "JPY",
            "v",
        )
        .unwrap();
    assert_eq!(b.limits.hard_limit_micros, Some(5_000));
    assert_eq!(
        b.function(&FunctionId::parse(FUNCTION).unwrap())
            .unwrap()
            .hard_limit_micros,
        Some(700)
    );
    // A tenant without an entry gets the default; without a default, none.
    let other = TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzzb").unwrap();
    assert_eq!(
        ok.budget_for(&other, "p", "JPY", "v")
            .unwrap()
            .limits
            .hard_limit_micros,
        Some(0)
    );
    let no_default = BudgetBook {
        default_tenant: None,
        ..ok
    };
    assert!(no_default.budget_for(&other, "p", "JPY", "v").is_none());

    for bad in [
        // thresholds without the soft limit they are percentages of
        format!("[[tenants]]\ntenant_id = \"{TENANT}\"\nalert_thresholds_percent = [50]\n"),
        format!(
            "[[tenants]]\ntenant_id = \"{TENANT}\"\nsoft_limit_micros = 1\nalert_thresholds_percent = [80, 50]\n"
        ),
        format!("[[tenants]]\ntenant_id = \"{TENANT}\"\n[[tenants]]\ntenant_id = \"{TENANT}\"\n"),
        "[[tenants]]\ntenant_id = \"not-a-tenant\"\n".to_string(),
        format!("[[tenants]]\ntenant_id = \"{TENANT}\"\nhard_limit = 5\n"),
    ] {
        assert!(BudgetBook::from_toml(&bad).is_err(), "{bad}");
    }
    let config = BudgetConfig {
        period: "rolling_30d".into(),
        ..BudgetConfig::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn periods_are_calendar_months_in_utc() {
    assert_eq!(period_of(&t(0)), "2026-09");
    let (start, end) = period_bounds("2026-12").unwrap();
    assert_eq!(start.to_rfc3339(), "2026-12-01T00:00:00+00:00");
    assert_eq!(end.to_rfc3339(), "2027-01-01T00:00:00+00:00");
    assert!(period_bounds("2026-13").is_none());
    assert!(period_bounds("september").is_none());
}

#[test]
fn a_dropped_store_file_is_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BudgetStore::open(Some(dir.path().join("b.db"))));
    store.reserve(&new_res("x", 5, None), t(0)).unwrap();
    assert_eq!(store.stats().unwrap().active_reservations, 1);
}

/// `budget.db` is locked by another process. Every concurrent reservation must
/// be refused (`Host.BudgetStoreUnavailable`) within one busy timeout plus the
/// connection wait, not queue behind the other callers' busy timeouts on the
/// connection mutex, and the operator view (`last_error`) must not wait for
/// the connection at all (PLT-4646).
#[test]
fn a_locked_store_refuses_every_concurrent_reservation_within_a_bounded_time() {
    use std::time::{Duration, Instant};
    let bound = store::BUSY_TIMEOUT + crate::sqlite_wait::STORE_WAIT + Duration::from_secs(2);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("budget.db");
    let store = Arc::new(BudgetStore::open(Some(path.clone())));
    store.reserve(&new_res("before", 5, None), t(0)).unwrap();
    let locker = rusqlite::Connection::open(&path).unwrap();
    locker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let started = Instant::now();
    let callers: Vec<_> = (0..4)
        .map(|i| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let at = Instant::now();
                let r = store.reserve(&new_res(&format!("r{i}"), 5, None), t(1));
                (r, at.elapsed())
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(200));
    let view = Instant::now();
    let _ = store.last_error();
    assert!(
        view.elapsed() < Duration::from_secs(1),
        "the operator view never waits for the connection"
    );
    for caller in callers {
        let (result, elapsed) = caller.join().unwrap();
        assert!(result.is_err(), "a locked store refuses: {result:?}");
        assert!(
            elapsed <= bound,
            "a reservation waited {elapsed:?} (bound: connection wait + busy timeout)"
        );
    }
    assert!(started.elapsed() < bound + Duration::from_secs(2));
    assert!(store.last_error().is_some());
    locker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        store.reserve(&new_res("after", 5, None), t(2)).unwrap(),
        ReserveOutcome::Reserved
    );
    assert_eq!(store.stats().unwrap().active_reservations, 2);
    assert!(store.last_error().is_none());
}
