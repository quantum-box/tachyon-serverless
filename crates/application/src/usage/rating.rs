//! Provisional rating (仮料金, PLT-4642, docs/adr/0012 §4).
//!
//! Three things never mix here:
//! - **usage**: host-measured / provider-reported quantities from the ledger;
//! - **price**: a versioned [`PriceTable`] (a file, or the built-in dev table);
//! - **cost**: what the host spent (environment lifetime, idle pool, cgroup
//!   CPU). Reported next to usage, never multiplied by a price.
//!
//! Rounding, in the order it applies (and property-tested below):
//! 1. Every segment is measured in whole milliseconds rounded **up** from the
//!    monotonic clock (the emitter's `ceil_ms`): a 1 µs handler is 1 ms.
//! 2. Per attempt, `billable_ms` is the exact sum of its *ratable* billable
//!    segments. Unknown and guest-reported segments contribute 0 and are
//!    counted in `unmetered`.
//! 3. Quantities are aggregated **exactly** (integers) per line
//!    (tenant × function × day, as grouped).
//! 4. Each charge component of a line is `round_half_up(quantity × unit
//!    price / unit divisor)` in integer micro-units — once per line, never per
//!    event. A line's total is the sum of its components; the report total is
//!    the sum of the line totals (no second rounding).
//!
//! Consequences: charges are never negative and never decrease when a
//! quantity grows; splitting a line into `n` lines changes a component by at
//! most `n / 2` micro-units (each rounding moves it by at most ½).

use std::collections::BTreeMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use tachyon_serverless_api_types::{
    GuestReportedTotals, HostCostFacts, OutcomeCounts, PriceTableInfo, ProvisionalCharges,
    SegmentTotals, UnitPricesMicros, UnmeteredUsage, UsageQuantities, UsageReportLine,
};
use tachyon_serverless_domain::{
    AttemptKind, Metered, Timestamp, UsageEvent, UsageEventType, UsageOutcome,
};

/// Segment names a price table may list as billable.
pub const SEGMENT_NAMES: [&str; 6] = [
    "queue_wait_ms",
    "vm_base_boot_ms",
    "user_init_ms",
    "handler_ms",
    "teardown_ms",
    "idle_pooled_ms",
];

/// `1 vCPU-second` in `cpu_millis × ms`.
pub const VCPU_MILLI_MS_PER_VCPU_SECOND: u128 = 1_000 * 1_000;
/// `1 GiB-second` in `MiB × ms`.
pub const MIB_MS_PER_GIB_SECOND: u128 = 1_024 * 1_000;
/// `1 GB` (decimal) in bytes.
pub const BYTES_PER_GB: u128 = 1_000_000_000;

/// A versioned price table. Unit prices are integer micro-units of
/// `currency`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceTable {
    pub version: String,
    pub effective_from: DateTime<Utc>,
    pub currency: String,
    pub billable_segments: Vec<String>,
    pub unit_prices_micros: UnitPrices,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnitPrices {
    pub vcpu_second: u64,
    pub gib_second: u64,
    pub invocation: u64,
    pub gb_transferred: u64,
}

pub const ROUNDING_RULES: [&str; 4] = [
    "each segment: whole milliseconds rounded up from the host monotonic clock",
    "per attempt: billable_ms = exact sum of host-measured billable segments; unknown and \
     guest-reported segments count 0 and are reported as unmetered",
    "per line (tenant x function x day): quantities summed exactly",
    "per line and component: round_half_up(quantity x unit price / unit) in integer \
     micro-units; line total = sum of components; report total = sum of line totals",
];

impl PriceTable {
    /// The built-in table of the prototype. Numbers are placeholders chosen to
    /// make rounding visible, not a tariff.
    pub fn builtin() -> Self {
        Self {
            version: "provisional-dev-2026-09-v1".into(),
            effective_from: DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
                .expect("valid constant")
                .with_timezone(&Utc),
            currency: "JPY".into(),
            billable_segments: vec!["user_init_ms".into(), "handler_ms".into()],
            unit_prices_micros: UnitPrices {
                vcpu_second: 2_500,
                gib_second: 400,
                invocation: 30,
                gb_transferred: 15_000_000,
            },
        }
    }

    pub fn from_toml(text: &str) -> Result<Self, String> {
        let table: Self = toml::from_str(text).map_err(|e| format!("price table: {e}"))?;
        table.validate()?;
        Ok(table)
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read price table {}: {e}", path.display()))?;
        Self::from_toml(&text)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version.trim().is_empty() {
            return Err("price table: version must not be empty".into());
        }
        if self.currency.trim().is_empty() {
            return Err("price table: currency must not be empty".into());
        }
        for s in &self.billable_segments {
            if !SEGMENT_NAMES.contains(&s.as_str()) {
                return Err(format!(
                    "price table: unknown billable segment `{s}` (known: {})",
                    SEGMENT_NAMES.join(", ")
                ));
            }
        }
        Ok(())
    }

    pub fn info(&self) -> PriceTableInfo {
        PriceTableInfo {
            version: self.version.clone(),
            effective_from: self.effective_from,
            currency: self.currency.clone(),
            billable_segments: self.billable_segments.clone(),
            unit_prices_micros: UnitPricesMicros {
                vcpu_second: self.unit_prices_micros.vcpu_second,
                gib_second: self.unit_prices_micros.gib_second,
                invocation: self.unit_prices_micros.invocation,
                gb_transferred: self.unit_prices_micros.gb_transferred,
            },
            rounding: ROUNDING_RULES.iter().map(|r| r.to_string()).collect(),
        }
    }
}

/// `round(n / d)` with halves rounded up. `d > 0`.
pub fn round_half_up_div(n: u128, d: u128) -> u128 {
    debug_assert!(d > 0);
    (n / d) + u128::from((n % d) * 2 >= d)
}

/// Charge in micro-units of `quantity` at `unit_price` micro-units per
/// `per` quantity units.
pub fn charge(quantity: u128, unit_price: u64, per: u128) -> u64 {
    u64::try_from(round_half_up_div(
        quantity.saturating_mul(u128::from(unit_price)),
        per,
    ))
    .unwrap_or(u64::MAX)
}

/// Milliseconds of `d`, rounded up.
pub fn ceil_ms(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_micros().div_ceil(1000)).unwrap_or(u64::MAX)
}

/// How report lines are grouped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GroupBy {
    pub function: bool,
    pub day: bool,
}

impl GroupBy {
    /// `function`, `day`, `function,day` (any order), `none` or empty.
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        let Some(raw) = raw else {
            return Ok(Self {
                function: true,
                day: true,
            });
        };
        let mut g = Self::default();
        for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            match part {
                "function" => g.function = true,
                "day" => g.day = true,
                "none" => {}
                other => {
                    return Err(format!(
                        "group_by: unknown key `{other}` (use function, day, function,day or none)"
                    ));
                }
            }
        }
        Ok(g)
    }

    pub fn keys(&self) -> Vec<String> {
        let mut k = Vec::new();
        if self.function {
            k.push("function".to_string());
        }
        if self.day {
            k.push("day".to_string());
        }
        k
    }
}

/// Exact (unrounded) accumulator of one line.
#[derive(Debug, Clone, Default)]
struct Acc {
    usage: UsageQuantities,
    vcpu_milli_ms: u128,
    mib_ms: u128,
    transfer_bytes: u128,
    unmetered: UnmeteredUsage,
    cost: HostCostFacts,
    guest: GuestReportedTotals,
}

fn add_segments(total: &mut SegmentTotals, name: &str, v: u64) {
    let slot = match name {
        "queue_wait_ms" => &mut total.queue_wait_ms,
        "vm_base_boot_ms" => &mut total.vm_base_boot_ms,
        "user_init_ms" => &mut total.user_init_ms,
        "handler_ms" => &mut total.handler_ms,
        "teardown_ms" => &mut total.teardown_ms,
        "idle_pooled_ms" => &mut total.idle_pooled_ms,
        _ => return,
    };
    *slot = slot.saturating_add(v);
}

fn ratable(m: Metered) -> Option<u64> {
    m.ratable_value()
}

impl Acc {
    fn add(&mut self, e: &UsageEvent, table: &PriceTable) {
        match e.event_type {
            UsageEventType::AttemptSettled => self.add_attempt(e, table),
            UsageEventType::EnvironmentStopped => self.add_stop(e),
            // Lifecycle markers: the quantities they carry are repeated in
            // `AttemptSettled` / `EnvironmentStopped`, so they are not summed.
            UsageEventType::EnvironmentStarted
            | UsageEventType::HandlerStarted
            | UsageEventType::HandlerFinished => {}
        }
        if let Some(ms) = e.guest_reported.guest_handler_ms {
            self.guest.guest_handler_ms = self.guest.guest_handler_ms.saturating_add(ms);
        }
        if let Some(ms) = e.guest_reported.guest_init_ms {
            self.guest.guest_init_ms = self.guest.guest_init_ms.saturating_add(ms);
        }
    }

    fn add_attempt(&mut self, e: &UsageEvent, table: &PriceTable) {
        let u = &mut self.usage;
        u.attempts += 1;
        match e.attempt_kind {
            Some(AttemptKind::Retry) => u.retries += 1,
            _ => u.invocations += 1,
        }
        match e.outcome {
            Some(UsageOutcome::Succeeded) => u.outcomes.succeeded += 1,
            Some(UsageOutcome::Failed) => u.outcomes.failed += 1,
            Some(UsageOutcome::Timeout) => u.outcomes.timeout += 1,
            Some(UsageOutcome::Cancelled) => u.outcomes.cancelled += 1,
            Some(UsageOutcome::OutcomeUnknown) | None => u.outcomes.outcome_unknown += 1,
        }
        let mut billable_ms: u64 = 0;
        let mut any_unknown_billable = false;
        for (name, m) in e.segments.named() {
            match ratable(m) {
                Some(v) => add_segments(&mut u.segments_ms, name, v),
                None => add_segments(&mut self.unmetered.segments, name, 1),
            }
            if table.billable_segments.iter().any(|b| b == name) {
                match ratable(m) {
                    Some(v) => billable_ms = billable_ms.saturating_add(v),
                    None => any_unknown_billable = true,
                }
            }
        }
        // Without the requested resources (a v1 event) compute cannot be
        // rated: counted as unmetered, never guessed.
        let cpu = e.resources.requested_cpu_millis;
        let mem = e.resources.requested_memory_mib;
        if cpu == 0 || mem == 0 {
            any_unknown_billable = true;
        }
        if any_unknown_billable {
            self.unmetered.attempts += 1;
        }
        u.billable_ms = u.billable_ms.saturating_add(billable_ms);
        self.vcpu_milli_ms += u128::from(billable_ms) * u128::from(cpu);
        self.mib_ms += u128::from(billable_ms) * u128::from(mem);
        match (
            ratable(e.bytes.request_bytes),
            ratable(e.bytes.response_bytes),
        ) {
            (Some(req), Some(resp)) => {
                u.request_bytes = u.request_bytes.saturating_add(req);
                u.response_bytes = u.response_bytes.saturating_add(resp);
                self.transfer_bytes += u128::from(req) + u128::from(resp);
            }
            (req, resp) => {
                self.unmetered.bytes += 1;
                // What was measured still counts.
                if let Some(req) = req {
                    u.request_bytes = u.request_bytes.saturating_add(req);
                    self.transfer_bytes += u128::from(req);
                }
                if let Some(resp) = resp {
                    u.response_bytes = u.response_bytes.saturating_add(resp);
                    self.transfer_bytes += u128::from(resp);
                }
            }
        }
    }

    fn add_stop(&mut self, e: &UsageEvent) {
        let c = &mut self.cost;
        c.environments_stopped += 1;
        c.environment_lifetime_ms = c
            .environment_lifetime_ms
            .saturating_add(e.monotonic_duration_ms.unwrap_or(0));
        if let Some(v) = ratable(e.segments.idle_pooled_ms) {
            c.idle_pooled_ms = c.idle_pooled_ms.saturating_add(v);
        }
        if let Some(v) = ratable(e.segments.teardown_ms) {
            c.teardown_ms = c.teardown_ms.saturating_add(v);
        }
        // A stop that carries an outcome but no attempt is an environment
        // that failed (or was abandoned) before any dispatch.
        if e.attempt_id.is_none() && e.outcome.is_some() {
            let boot = ratable(e.segments.vm_base_boot_ms).unwrap_or(0);
            let init = ratable(e.segments.user_init_ms).unwrap_or(0);
            c.boot_without_attempt_ms = c
                .boot_without_attempt_ms
                .saturating_add(boot.saturating_add(init));
        }
        match ratable(e.resources.cgroup_cpu_usec) {
            Some(v) => c.cgroup_cpu_usec = c.cgroup_cpu_usec.saturating_add(v),
            None => c.cgroup_cpu_unknown += 1,
        }
        if let Some(v) = ratable(e.resources.cgroup_memory_peak_bytes) {
            c.cgroup_memory_peak_bytes_max = c.cgroup_memory_peak_bytes_max.max(v);
        }
    }

    fn finish(mut self, table: &PriceTable) -> UsageReportLine {
        let p = table.unit_prices_micros;
        self.usage.vcpu_milli_ms = u64::try_from(self.vcpu_milli_ms).unwrap_or(u64::MAX);
        self.usage.mib_ms = u64::try_from(self.mib_ms).unwrap_or(u64::MAX);
        let mut charges = ProvisionalCharges {
            vcpu: charge(
                self.vcpu_milli_ms,
                p.vcpu_second,
                VCPU_MILLI_MS_PER_VCPU_SECOND,
            ),
            memory: charge(self.mib_ms, p.gib_second, MIB_MS_PER_GIB_SECOND),
            invocations: charge(u128::from(self.usage.invocations), p.invocation, 1),
            transfer: charge(self.transfer_bytes, p.gb_transferred, BYTES_PER_GB),
            total: 0,
        };
        charges.total = charges
            .vcpu
            .saturating_add(charges.memory)
            .saturating_add(charges.invocations)
            .saturating_add(charges.transfer);
        UsageReportLine {
            function_id: None,
            day: None,
            usage: self.usage,
            unmetered: self.unmetered,
            cost: self.cost,
            provisional_charges_micros: charges,
            guest_reported: self.guest,
        }
    }
}

/// Rate `events` into lines grouped by `group_by`, plus the totals line.
/// Only `AttemptSettled` is charged; everything else is usage or cost facts.
pub fn rate(
    events: &[UsageEvent],
    table: &PriceTable,
    group_by: GroupBy,
) -> (Vec<UsageReportLine>, UsageReportLine) {
    let mut groups: BTreeMap<(Option<String>, Option<String>), Acc> = BTreeMap::new();
    for e in events {
        let function = group_by.function.then(|| {
            e.function_id
                .as_ref()
                .map_or_else(|| "unattributed".to_string(), |f| f.to_string())
        });
        let day = group_by.day.then(|| day_of(&e.observed_at));
        groups.entry((function, day)).or_default().add(e, table);
    }
    let mut lines = Vec::with_capacity(groups.len());
    for ((function, day), acc) in groups {
        let mut line = acc.finish(table);
        line.function_id = function;
        line.day = day;
        lines.push(line);
    }
    let totals = sum_lines(&lines);
    (lines, totals)
}

/// `YYYY-MM-DD` of a host wall-clock timestamp (UTC).
pub fn day_of(t: &Timestamp) -> String {
    t.format("%Y-%m-%d").to_string()
}

fn sum_segments(a: &mut SegmentTotals, b: &SegmentTotals) {
    a.queue_wait_ms = a.queue_wait_ms.saturating_add(b.queue_wait_ms);
    a.vm_base_boot_ms = a.vm_base_boot_ms.saturating_add(b.vm_base_boot_ms);
    a.user_init_ms = a.user_init_ms.saturating_add(b.user_init_ms);
    a.handler_ms = a.handler_ms.saturating_add(b.handler_ms);
    a.teardown_ms = a.teardown_ms.saturating_add(b.teardown_ms);
    a.idle_pooled_ms = a.idle_pooled_ms.saturating_add(b.idle_pooled_ms);
}

fn sum_outcomes(a: &mut OutcomeCounts, b: &OutcomeCounts) {
    a.succeeded += b.succeeded;
    a.failed += b.failed;
    a.timeout += b.timeout;
    a.cancelled += b.cancelled;
    a.outcome_unknown += b.outcome_unknown;
}

/// The totals line: every quantity and every charge summed (no re-rounding).
pub fn sum_lines(lines: &[UsageReportLine]) -> UsageReportLine {
    let mut t = UsageReportLine::default();
    for l in lines {
        let (u, lu) = (&mut t.usage, &l.usage);
        u.invocations += lu.invocations;
        u.attempts += lu.attempts;
        u.retries += lu.retries;
        sum_outcomes(&mut u.outcomes, &lu.outcomes);
        sum_segments(&mut u.segments_ms, &lu.segments_ms);
        u.billable_ms = u.billable_ms.saturating_add(lu.billable_ms);
        u.vcpu_milli_ms = u.vcpu_milli_ms.saturating_add(lu.vcpu_milli_ms);
        u.mib_ms = u.mib_ms.saturating_add(lu.mib_ms);
        u.request_bytes = u.request_bytes.saturating_add(lu.request_bytes);
        u.response_bytes = u.response_bytes.saturating_add(lu.response_bytes);
        t.unmetered.attempts += l.unmetered.attempts;
        sum_segments(&mut t.unmetered.segments, &l.unmetered.segments);
        t.unmetered.bytes += l.unmetered.bytes;
        let (c, lc) = (&mut t.cost, &l.cost);
        c.environments_stopped += lc.environments_stopped;
        c.environment_lifetime_ms = c
            .environment_lifetime_ms
            .saturating_add(lc.environment_lifetime_ms);
        c.idle_pooled_ms = c.idle_pooled_ms.saturating_add(lc.idle_pooled_ms);
        c.teardown_ms = c.teardown_ms.saturating_add(lc.teardown_ms);
        c.boot_without_attempt_ms = c
            .boot_without_attempt_ms
            .saturating_add(lc.boot_without_attempt_ms);
        c.cgroup_cpu_usec = c.cgroup_cpu_usec.saturating_add(lc.cgroup_cpu_usec);
        c.cgroup_cpu_unknown += lc.cgroup_cpu_unknown;
        c.cgroup_memory_peak_bytes_max = c
            .cgroup_memory_peak_bytes_max
            .max(lc.cgroup_memory_peak_bytes_max);
        let (p, lp) = (
            &mut t.provisional_charges_micros,
            &l.provisional_charges_micros,
        );
        p.vcpu = p.vcpu.saturating_add(lp.vcpu);
        p.memory = p.memory.saturating_add(lp.memory);
        p.invocations = p.invocations.saturating_add(lp.invocations);
        p.transfer = p.transfer.saturating_add(lp.transfer);
        p.total = p.total.saturating_add(lp.total);
        t.guest_reported.guest_handler_ms = t
            .guest_reported
            .guest_handler_ms
            .saturating_add(l.guest_reported.guest_handler_ms);
        t.guest_reported.guest_init_ms = t
            .guest_reported
            .guest_init_ms
            .saturating_add(l.guest_reported.guest_init_ms);
    }
    t
}
