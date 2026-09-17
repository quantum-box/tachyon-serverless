//! Unit and property tests of the usage journal, ledger, collector and rating
//! (PLT-4642).

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};

use tachyon_serverless_domain::{
    AttemptId, AttemptKind, EnvironmentId, FixedClock, FunctionId, Metered, RevisionId, TenantId,
    Timestamp, UsageEvent, UsageEventType, UsageOutcome, UsageSegments,
};

use super::journal::{JournalLimits, UsageJournal, chain_next};
use super::rating::{self, GroupBy, PriceTable, ceil_ms, charge, round_half_up_div};
use super::*;
use crate::config::Profile;

const TENANT_A: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzza";
const TENANT_B: &str = "tn_01hzzzzzzzzzzzzzzzzzzzzzzb";

fn t0() -> Timestamp {
    Utc.with_ymd_and_hms(2026, 9, 17, 12, 0, 0).unwrap()
}

/// Deterministic generator for the property tests (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn limits(max_events: u64, headroom: u64) -> JournalLimits {
    JournalLimits {
        max_events,
        max_bytes: 64 * 1024 * 1024,
        admission_headroom_events: headroom,
        admission_headroom_bytes: 1024,
    }
}

/// A settled attempt of `tenant` with the given host-measured handler time.
fn settled(tenant: &str, function: &FunctionId, seq: u64, handler_ms: u64) -> UsageEvent {
    let env = EnvironmentId::generate();
    let mut e = UsageEvent::new(
        format!("{env}:1:{seq}"),
        TenantId::parse(tenant).unwrap(),
        env,
        UsageEventType::AttemptSettled,
        seq,
        t0(),
    );
    e.function_id = Some(function.clone());
    e.revision_id = Some(RevisionId::generate());
    e.attempt_id = Some(AttemptId::generate());
    e.attempt_number = Some(1);
    e.attempt_kind = Some(AttemptKind::First);
    e.outcome = Some(UsageOutcome::Succeeded);
    e.resources.requested_cpu_millis = 500;
    e.resources.requested_memory_mib = 256;
    e.segments = UsageSegments {
        queue_wait_ms: Metered::host(3),
        vm_base_boot_ms: Metered::host(120),
        user_init_ms: Metered::host(40),
        handler_ms: Metered::host(handler_ms),
        teardown_ms: Metered::host(8),
        idle_pooled_ms: Metered::host(0),
    };
    e.bytes.request_bytes = Metered::host(100);
    e.bytes.response_bytes = Metered::host(900);
    e
}

fn meter_in(dir: Option<&std::path::Path>, config: UsageConfig) -> Arc<UsageMeter> {
    UsageMeter::open(&config, dir, Arc::new(FixedClock::new(t0())), Profile::Dev).unwrap()
}

// ---------------------------------------------------------------------------
// rounding properties
// ---------------------------------------------------------------------------

#[test]
fn round_half_up_is_exact_on_small_cases() {
    assert_eq!(round_half_up_div(0, 10), 0);
    assert_eq!(round_half_up_div(4, 10), 0);
    assert_eq!(round_half_up_div(5, 10), 1);
    assert_eq!(round_half_up_div(15, 10), 2);
    assert_eq!(round_half_up_div(14, 10), 1);
    assert_eq!(ceil_ms(Duration::from_micros(1)), 1);
    assert_eq!(ceil_ms(Duration::from_micros(1000)), 1);
    assert_eq!(ceil_ms(Duration::from_micros(1001)), 2);
    assert_eq!(ceil_ms(Duration::ZERO), 0);
}

/// Charges are never negative (unsigned by construction, and never wrap) and
/// never decrease when the quantity grows.
#[test]
fn property_charges_are_monotonic_in_the_quantity() {
    let mut rng = Rng(42);
    for _ in 0..20_000 {
        let price = rng.below(50_000_000);
        let per = [1u128, 1_000_000, 1_024_000, 1_000_000_000][rng.below(4) as usize];
        // Kept within u64 after pricing (a charge saturates beyond it).
        let q = u128::from(rng.below(1 << 30));
        let dq = u128::from(rng.below(1 << 20));
        let a = charge(q, price, per);
        let b = charge(q + dq, price, per);
        assert!(b >= a, "q={q} dq={dq} price={price} per={per}: {a} > {b}");
        // Within half a unit of the exact value.
        let exact_x2 = 2 * q * u128::from(price);
        let charged_x2 = 2 * u128::from(a) * per;
        assert!(charged_x2 + per >= exact_x2 && charged_x2 <= exact_x2 + per);
    }
}

/// Rounding a duration up per invocation never undercounts the aggregate:
/// `Σ ceil_ms(d_i) >= ceil_ms(Σ d_i)`, and by less than one ms per item.
#[test]
fn property_per_invocation_ceil_never_undercounts_the_sum() {
    let mut rng = Rng(7);
    for _ in 0..5_000 {
        let n = 1 + rng.below(50);
        let ds: Vec<Duration> = (0..n)
            .map(|_| Duration::from_micros(rng.below(5_000_000)))
            .collect();
        let sum_of_ceil: u64 = ds.iter().map(|d| ceil_ms(*d)).sum();
        let ceil_of_sum = ceil_ms(ds.iter().sum());
        assert!(sum_of_ceil >= ceil_of_sum);
        assert!(sum_of_ceil < ceil_of_sum + n);
    }
}

/// Splitting a line into `n` lines moves each charge component by at most
/// `n / 2` micro-units; the report total is exactly the sum of line totals.
#[test]
fn property_line_split_is_bounded_and_totals_are_sums() {
    let table = PriceTable::builtin();
    let function = FunctionId::generate();
    let mut rng = Rng(99);
    for round in 0..300 {
        let n = 1 + rng.below(12) as usize;
        let mut events = Vec::new();
        for i in 0..n {
            let mut e = settled(
                TENANT_A,
                &function,
                (round * 100 + i) as u64,
                rng.below(90_000),
            );
            // A different day per event: one line each with group_by = day.
            e.observed_at = t0() + chrono::Duration::days(i as i64);
            e.resources.requested_cpu_millis = 1 + rng.below(4000) as u32;
            e.resources.requested_memory_mib = 1 + rng.below(8192) as u32;
            e.bytes.response_bytes = Metered::host(rng.below(10_000_000));
            events.push(e);
        }
        let (split, split_total) = rating::rate(
            &events,
            &table,
            GroupBy {
                function: false,
                day: true,
            },
        );
        let (merged, merged_total) = rating::rate(&events, &table, GroupBy::default());
        assert_eq!(split.len(), n);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            split_total.provisional_charges_micros.total,
            split
                .iter()
                .map(|l| l.provisional_charges_micros.total)
                .sum::<u64>()
        );
        let half_n = n as u64 / 2 + 1;
        let (s, m) = (
            split_total.provisional_charges_micros,
            merged_total.provisional_charges_micros,
        );
        for (a, b) in [
            (s.vcpu, m.vcpu),
            (s.memory, m.memory),
            (s.transfer, m.transfer),
            (s.invocations, m.invocations),
        ] {
            assert!(a.abs_diff(b) <= half_n, "split {a} vs merged {b} for n={n}");
        }
        // Quantities never depend on the grouping.
        assert_eq!(split_total.usage, merged_total.usage);
    }
}

#[test]
fn rating_follows_the_documented_formula() {
    let table = PriceTable::builtin();
    let f = FunctionId::generate();
    let e = settled(TENANT_A, &f, 1, 1_000);
    let both = GroupBy::parse(None).unwrap();
    assert!(both.function && both.day);
    let (lines, total) = rating::rate(&[e], &table, both);
    assert_eq!(lines.len(), 1);
    let u = &total.usage;
    // Billable: user_init 40 + handler 1000.
    assert_eq!(u.billable_ms, 1_040);
    assert_eq!(u.vcpu_milli_ms, 1_040 * 500);
    assert_eq!(u.mib_ms, 1_040 * 256);
    let p = table.unit_prices_micros;
    let c = total.provisional_charges_micros;
    assert_eq!(c.vcpu, charge(1_040 * 500, p.vcpu_second, 1_000_000));
    assert_eq!(c.memory, charge(1_040 * 256, p.gib_second, 1_024_000));
    assert_eq!(c.invocations, p.invocation);
    assert_eq!(c.transfer, charge(1_000, p.gb_transferred, 1_000_000_000));
    assert_eq!(c.total, c.vcpu + c.memory + c.invocations + c.transfer);
    assert_eq!(lines[0].day.as_deref(), Some("2026-09-17"));
    assert_eq!(lines[0].function_id.as_deref(), Some(f.as_str()));
}

#[test]
fn unknown_and_guest_reported_segments_contribute_nothing_and_are_reported() {
    let table = PriceTable::builtin();
    let f = FunctionId::generate();
    let honest = settled(TENANT_A, &f, 1, 500);
    let mut unknown = settled(TENANT_A, &f, 2, 500);
    unknown.segments.handler_ms = Metered::unknown();
    let mut guest = settled(TENANT_A, &f, 3, 500);
    guest.segments.handler_ms = Metered::guest(3_600_000);
    guest.guest_reported.guest_handler_ms = Some(3_600_000);
    let (_, one) = rating::rate(std::slice::from_ref(&honest), &table, GroupBy::default());
    let (_, all) = rating::rate(&[honest, unknown, guest], &table, GroupBy::default());
    // Only the honest attempt's handler is billed; the two others add their
    // user_init (host-measured) and nothing for the handler.
    assert_eq!(all.usage.billable_ms, one.usage.billable_ms + 40 + 40);
    assert_eq!(all.usage.segments_ms.handler_ms, 500);
    assert_eq!(all.unmetered.attempts, 2);
    assert_eq!(all.unmetered.segments.handler_ms, 2);
    assert_eq!(all.guest_reported.guest_handler_ms, 3_600_000);
    assert!(
        all.provisional_charges_micros.vcpu < one.provisional_charges_micros.vcpu * 3,
        "the guest's claim of an hour never reaches a charge"
    );
}

#[test]
fn events_without_requested_resources_are_unmetered_not_guessed() {
    let table = PriceTable::builtin();
    let f = FunctionId::generate();
    let mut e = settled(TENANT_A, &f, 1, 500);
    e.resources.requested_cpu_millis = 0;
    let (_, t) = rating::rate(&[e], &table, GroupBy::default());
    assert_eq!(t.usage.vcpu_milli_ms, 0);
    assert_eq!(t.provisional_charges_micros.vcpu, 0);
    assert_eq!(t.unmetered.attempts, 1);
}

#[test]
fn price_tables_are_validated() {
    let ok = r#"
version = "test-v2"
effective_from = "2026-09-01T00:00:00Z"
currency = "USD"
billable_segments = ["handler_ms"]
[unit_prices_micros]
vcpu_second = 1
gib_second = 2
invocation = 3
gb_transferred = 4
"#;
    let t = PriceTable::from_toml(ok).unwrap();
    assert_eq!(t.version, "test-v2");
    assert!(PriceTable::from_toml(&ok.replace("handler_ms", "sleeping_ms")).is_err());
    assert!(PriceTable::from_toml(&ok.replace("test-v2", "")).is_err());
    assert!(PriceTable::from_toml(&format!("{ok}\nextra = 1\n")).is_err());
}

#[test]
fn billing_cannot_be_enabled_and_accept_unmetered_is_dev_only() {
    let mut c = UsageConfig::default();
    c.billing.enabled = true;
    assert!(
        c.validate(Profile::Dev)
            .unwrap_err()
            .contains("hard-disabled")
    );
    let c = UsageConfig {
        on_journal_full: JournalFullPolicy::AcceptUnmetered,
        ..UsageConfig::default()
    };
    assert!(c.validate(Profile::Dev).is_ok());
    assert!(c.validate(Profile::Production).is_err());
    let c = UsageConfig {
        admission_headroom_events: 10,
        journal_max_events: 10,
        ..UsageConfig::default()
    };
    assert!(c.validate(Profile::Dev).is_err());
    const { assert!(!BILLING_ENABLED) };
}

// ---------------------------------------------------------------------------
// journal
// ---------------------------------------------------------------------------

#[test]
fn the_journal_bound_refuses_appends_and_counts_them_per_tenant() {
    let j = UsageJournal::open(None, limits(3, 1));
    let f = FunctionId::generate();
    for i in 0..3 {
        j.append(&settled(TENANT_A, &f, i, 1), t0()).unwrap();
    }
    assert_eq!(
        j.append(&settled(TENANT_A, &f, 9, 1), t0()),
        Err(JournalRefusal::Full)
    );
    assert_eq!(
        j.append(&settled(TENANT_B, &f, 10, 1), t0()),
        Err(JournalRefusal::Full)
    );
    assert_eq!(j.unjournaled_for(TENANT_A), 1);
    assert_eq!(j.unjournaled_for(TENANT_B), 1);
    let s = j.status();
    assert_eq!(s.pending_events, 3);
    assert_eq!(s.unjournaled_events, 2);
    assert!(!s.admitting);
}

#[test]
fn admission_keeps_headroom_for_work_already_admitted() {
    let j = UsageJournal::open(None, limits(10, 4));
    let f = FunctionId::generate();
    for i in 0..5 {
        j.append(&settled(TENANT_A, &f, i, 1), t0()).unwrap();
    }
    assert_eq!(j.admission(), Ok(()));
    j.append(&settled(TENANT_A, &f, 5, 1), t0()).unwrap();
    // 4 left: new invocations refused, admitted ones still append.
    assert_eq!(j.admission(), Err(JournalRefusal::Full));
    for i in 6..10 {
        j.append(&settled(TENANT_A, &f, i, 1), t0()).unwrap();
    }
    assert_eq!(j.unjournaled_for(TENANT_A), 0);
}

#[test]
fn an_unavailable_journal_refuses_and_recovers() {
    let j = UsageJournal::open(None, limits(10, 1));
    let f = FunctionId::generate();
    j.force_unavailable(true);
    assert_eq!(j.admission(), Err(JournalRefusal::Unavailable));
    assert_eq!(
        j.append(&settled(TENANT_A, &f, 1, 1), t0()),
        Err(JournalRefusal::Unavailable)
    );
    assert!(!j.probe());
    j.force_unavailable(false);
    assert!(j.probe());
    assert_eq!(j.admission(), Ok(()));
    assert_eq!(j.unjournaled_for(TENANT_A), 1);
}

#[test]
fn a_journal_that_cannot_be_opened_is_unavailable_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    // A directory where the database file should be.
    let path = dir.path().join("journal.db");
    std::fs::create_dir_all(&path).unwrap();
    let j = UsageJournal::open(Some(path), limits(10, 1));
    assert_eq!(j.admission(), Err(JournalRefusal::Unavailable));
    assert!(!j.status().healthy);
}

#[test]
fn a_tampered_journal_row_stops_collection() {
    let dir = tempfile::tempdir().unwrap();
    let meter = meter_in(Some(dir.path()), UsageConfig::default());
    let f = FunctionId::generate();
    for i in 0..3 {
        meter
            .journal()
            .append(&settled(TENANT_A, &f, i, 1_000), t0())
            .unwrap();
    }
    // Someone edits the handler time of the second row on disk.
    let conn = rusqlite::Connection::open(dir.path().join("usage/journal.db")).unwrap();
    let body: String = conn
        .query_row(
            "SELECT body FROM function_usage_journal ORDER BY seq LIMIT 1 OFFSET 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let forged = body.replace("\"value\":1000", "\"value\":1");
    assert_ne!(forged, body);
    conn.execute(
        "UPDATE function_usage_journal SET body = ?1 WHERE seq = (SELECT seq FROM \
         function_usage_journal ORDER BY seq LIMIT 1 OFFSET 1)",
        [forged],
    )
    .unwrap();
    let err = meter.collect().unwrap_err();
    assert!(err.contains("integrity"), "{err}");
    assert_eq!(
        meter.ledger().stats().unwrap().events,
        0,
        "nothing delivered"
    );
    assert!(meter.status().collector.last_error.is_some());
    assert_eq!(chain_next("genesis", "x").len(), 64);
}

// ---------------------------------------------------------------------------
// collector and ledger
// ---------------------------------------------------------------------------

#[test]
fn a_duplicate_event_is_a_single_ledger_row() {
    let meter = meter_in(None, UsageConfig::default());
    let f = FunctionId::generate();
    let e = settled(TENANT_A, &f, 1, 250);
    meter.journal().append(&e, t0()).unwrap();
    // The emitter re-sends the same fact (same event id).
    meter.journal().append(&e, t0()).unwrap();
    let report = meter.collect().unwrap();
    assert_eq!(report.read, 2);
    assert_eq!(report.inserted, 1);
    assert_eq!(report.duplicates, 1);
    // A replay of the ledger delivery itself.
    let again = meter.ledger().accept(&[e], t0()).unwrap();
    assert_eq!(again.inserted, 0);
    assert_eq!(meter.ledger().stats().unwrap().events, 1);
}

#[test]
fn a_crash_between_the_ledger_commit_and_the_cursor_never_double_counts() {
    let dir = tempfile::tempdir().unwrap();
    let f = FunctionId::generate();
    let config = UsageConfig {
        collect_batch: 2,
        ..UsageConfig::default()
    };
    {
        let meter = meter_in(Some(dir.path()), config.clone());
        for i in 0..5 {
            meter
                .journal()
                .append(&settled(TENANT_A, &f, i, 100 + i), t0())
                .unwrap();
        }
        // The collector read a batch and the ledger committed it; the
        // process died before the cursor moved.
        let (_, _, batch) = meter.journal().read_batch(2).unwrap();
        let events: Vec<UsageEvent> = batch.into_iter().map(|e| e.event).collect();
        assert_eq!(meter.ledger().accept(&events, t0()).unwrap().inserted, 2);
    }
    // Restart on the same data_dir.
    let meter = meter_in(Some(dir.path()), config);
    assert_eq!(meter.journal().status().pending_events, 5);
    let report = meter.collect().unwrap();
    assert_eq!(report.read, 5, "the uncommitted batch is delivered again");
    assert_eq!(report.duplicates, 2);
    assert_eq!(report.inserted, 3);
    assert_eq!(meter.ledger().stats().unwrap().events, 5);
    let s = meter.journal().status();
    assert_eq!((s.pending_events, s.pending_bytes), (0, 0));
    // Nothing left to deliver; a second run is a no-op.
    assert_eq!(meter.collect().unwrap().read, 0);
    let principal = tachyon_serverless_provider_port::Principal {
        subject: "a".into(),
        tenant_id: TenantId::parse(TENANT_A).unwrap(),
        roles: vec![tachyon_serverless_provider_port::Role::Invoke],
    };
    let r = meter
        .report(
            &principal,
            &UsageQuery {
                from: Some(t0() - chrono::Duration::days(1)),
                to: Some(t0() + chrono::Duration::days(1)),
                ..UsageQuery::default()
            },
        )
        .unwrap();
    assert_eq!(r.totals.usage.invocations, 5);
    assert_eq!(
        r.totals.usage.segments_ms.handler_ms,
        100 + 101 + 102 + 103 + 104
    );
}

#[test]
fn late_and_out_of_order_events_are_accepted_by_id() {
    let meter = meter_in(None, UsageConfig::default());
    let f = FunctionId::generate();
    let mut late = settled(TENANT_A, &f, 1, 10);
    late.observed_at = t0() - chrono::Duration::hours(30);
    let now = settled(TENANT_A, &f, 2, 20);
    meter.journal().append(&now, t0()).unwrap();
    meter.journal().append(&late, t0()).unwrap();
    assert_eq!(meter.collect().unwrap().inserted, 2);
    assert!(meter.ledger().recorded_skew_ms(&late.event_id).unwrap() >= 30 * 3600 * 1000);
}

#[test]
fn the_ledger_only_answers_for_the_callers_tenant() {
    let meter = meter_in(None, UsageConfig::default());
    let f = FunctionId::generate();
    meter
        .journal()
        .append(&settled(TENANT_A, &f, 1, 10), t0())
        .unwrap();
    meter.collect().unwrap();
    let b = tachyon_serverless_provider_port::Principal {
        subject: "b".into(),
        tenant_id: TenantId::parse(TENANT_B).unwrap(),
        roles: vec![tachyon_serverless_provider_port::Role::Invoke],
    };
    let q = UsageQuery {
        from: Some(t0() - chrono::Duration::days(1)),
        to: Some(t0() + chrono::Duration::days(1)),
        function_id: Some(f.clone()),
        ..UsageQuery::default()
    };
    let r = meter.report(&b, &q).unwrap();
    assert!(r.lines.is_empty());
    assert_eq!(r.totals.usage.attempts, 0);
    assert!(r.provisional && r.not_an_invoice && !r.billing_enabled);
    // Without the invoke role: refused.
    let operator = tachyon_serverless_provider_port::Principal {
        roles: vec![tachyon_serverless_provider_port::Role::Operator],
        ..b
    };
    assert!(meter.report(&operator, &q).is_err());
}

#[test]
fn report_ranges_are_validated() {
    let meter = meter_in(None, UsageConfig::default());
    let a = tachyon_serverless_provider_port::Principal {
        subject: "a".into(),
        tenant_id: TenantId::parse(TENANT_A).unwrap(),
        roles: vec![tachyon_serverless_provider_port::Role::Invoke],
    };
    let bad = |from: Timestamp, to: Timestamp| {
        meter
            .report(
                &a,
                &UsageQuery {
                    from: Some(from),
                    to: Some(to),
                    ..UsageQuery::default()
                },
            )
            .is_err()
    };
    assert!(bad(t0(), t0()));
    assert!(bad(t0() - chrono::Duration::days(93), t0()));
    // Before the built-in table's effective_from.
    assert!(bad(
        Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap(),
        t0()
    ));
    assert!(
        meter
            .report(
                &a,
                &UsageQuery {
                    group_by: Some("tenant".into()),
                    ..UsageQuery::default()
                }
            )
            .is_err()
    );
    assert!(
        parse_report_time(Some("2026-09-17"), "from")
            .unwrap()
            .is_some()
    );
    assert!(parse_report_time(Some("yesterday"), "from").is_err());
}

#[test]
fn a_stopped_ledger_keeps_events_in_the_journal_until_it_accepts_again() {
    let meter = meter_in(None, UsageConfig::default());
    let f = FunctionId::generate();
    for i in 0..3 {
        meter
            .journal()
            .append(&settled(TENANT_A, &f, i, 10), t0())
            .unwrap();
    }
    meter.ledger().force_unavailable(true);
    assert!(meter.collect().is_err());
    let s = meter.status();
    assert_eq!(s.journal.pending_events, 3, "nothing is dropped");
    assert_eq!(s.journal.cursor_seq, 0, "the cursor did not move");
    assert!(s.collector.last_error.is_some());
    meter.ledger().force_unavailable(false);
    let report = meter.collect().unwrap();
    assert_eq!((report.read, report.inserted), (3, 3));
    assert_eq!(meter.journal().status().pending_events, 0);
}

/// Only a host sample that covers the whole environment (the VMM cgroup) is a
/// resource quantity; the process provider's bridge-only procfs sample stays
/// unknown instead of undercounting.
#[test]
fn only_whole_environment_host_samples_become_provider_reported() {
    use crate::services::invoke::usage_resources;
    use tachyon_serverless_domain::{Measurement, ResourceProfile};
    use tachyon_serverless_provider_port::EnvironmentStats;
    let sample = |scope: &str| EnvironmentStats {
        cpu_seconds: Some(1.5),
        memory_current_bytes: Some(1),
        memory_peak_bytes: Some(2048),
        scope: scope.into(),
    };
    let profile = ResourceProfile::default();
    let vmm = usage_resources(&profile, Some(&sample("cgroup_v2")));
    assert_eq!(vmm.cgroup_cpu_usec.value, Some(1_500_000));
    assert_eq!(
        vmm.cgroup_cpu_usec.measurement,
        Measurement::ProviderReported
    );
    assert_eq!(vmm.cgroup_memory_peak_bytes.value, Some(2048));
    for partial in ["procfs", "proc_pid_rusage"] {
        let r = usage_resources(&profile, Some(&sample(partial)));
        assert!(r.cgroup_cpu_usec.is_unknown(), "{partial}");
        assert!(r.cgroup_memory_peak_bytes.is_unknown(), "{partial}");
    }
    let none = usage_resources(&profile, None);
    assert!(none.cgroup_cpu_usec.is_unknown());
    assert_eq!(none.requested_cpu_millis, profile.cpu_millis);
}
