//! Cron expressions and their scheduled times in an IANA time zone
//! (PLT-4641, docs/adr/0014 §2).
//!
//! Syntax (Vixie cron, plus an optional leading seconds field):
//!
//! - 5 fields `minute hour day-of-month month day-of-week` (seconds = 0), or
//!   6 fields `second minute hour day-of-month month day-of-week`;
//! - each field: `*`, `n`, `a-b`, `*/s`, `a-b/s`, `a/s`, and comma lists of
//!   those; months `JAN`-`DEC`, weekdays `SUN`-`SAT`, `0` and `7` are Sunday;
//! - when both day-of-month and day-of-week are restricted, a day matches
//!   when **either** matches (Vixie semantics);
//! - `@yearly` / `@annually`, `@monthly`, `@weekly`, `@daily` / `@midnight`,
//!   `@hourly`.
//!
//! Times are local wall-clock times of the trigger's zone. Mapping one to an
//! instant:
//!
//! - a local time that does not exist (the hour skipped when DST starts) is
//!   **skipped**: nothing fires for it;
//! - a local time that exists twice (the hour repeated when DST ends) fires
//!   **once**, at its earlier instant.
//!
//! So `30 2 * * *` in `America/New_York` does not fire on the spring-forward
//! day and fires once on the fall-back day, and `*/15 * * * *` fires four
//! times, not eight, during the repeated hour.

use chrono::{Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

use tachyon_serverless_domain::Timestamp;

/// A search for the next time gives up after this many years (an expression
/// such as `0 0 30 2 *` never matches).
const SEARCH_YEARS: i32 = 5;
/// Bound on local times examined while mapping through DST gaps and repeats.
const MAX_LOCAL_STEPS: usize = 200_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    /// Bit `i` set when value `i` matches.
    bits: u64,
    /// `*` (or `*/1`): unrestricted, for the day-of-month / day-of-week rule.
    any: bool,
}

impl Field {
    fn matches(&self, v: u32) -> bool {
        v < 64 && self.bits & (1 << v) != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedule {
    seconds: Field,
    minutes: Field,
    hours: Field,
    days: Field,
    months: Field,
    weekdays: Field,
    has_seconds: bool,
}

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const WEEKDAYS: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

fn value(raw: &str, min: u32, max: u32, names: &[&str], names_from: u32) -> Result<u32, String> {
    if let Ok(n) = raw.parse::<u32>() {
        if n < min || n > max {
            return Err(format!("`{raw}` is outside {min}-{max}"));
        }
        return Ok(n);
    }
    let upper = raw.to_ascii_uppercase();
    names
        .iter()
        .position(|n| *n == upper)
        .map(|i| i as u32 + names_from)
        .ok_or_else(|| format!("`{raw}` is not a number or a name"))
}

fn field(raw: &str, min: u32, max: u32, names: &[&str], names_from: u32) -> Result<Field, String> {
    let mut bits = 0u64;
    let mut any = false;
    if raw.is_empty() {
        return Err("empty field".into());
    }
    for part in raw.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => {
                let s: u32 = s
                    .parse()
                    .map_err(|_| format!("step `{s}` is not a number"))?;
                if s == 0 {
                    return Err("a step must be >= 1".into());
                }
                (r, s)
            }
            None => (part, 1),
        };
        let (lo, hi) = if range == "*" {
            if step == 1 {
                any = true;
            }
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            let (a, b) = (
                value(a, min, max, names, names_from)?,
                value(b, min, max, names, names_from)?,
            );
            if a > b {
                return Err(format!("range `{range}` is reversed"));
            }
            (a, b)
        } else {
            let a = value(range, min, max, names, names_from)?;
            // `a/s` means `a-max/s`.
            if part.contains('/') { (a, max) } else { (a, a) }
        };
        let mut v = lo;
        while v <= hi {
            bits |= 1 << v;
            v += step;
        }
    }
    Ok(Field { bits, any })
}

impl CronSchedule {
    pub fn parse(expression: &str) -> Result<Self, String> {
        let expanded = match expression.trim() {
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" | "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            other if other.starts_with('@') => {
                return Err(format!("unknown macro `{other}`"));
            }
            other => other,
        };
        let parts: Vec<&str> = expanded.split_whitespace().collect();
        let (has_seconds, rest) = match parts.len() {
            5 => (false, &parts[..]),
            6 => (true, &parts[1..]),
            n => {
                return Err(format!(
                    "a cron expression has 5 fields (minute hour day month weekday) or 6 with a \
                     leading second, not {n}"
                ));
            }
        };
        let ctx = |name: &str, r: Result<Field, String>| r.map_err(|e| format!("{name}: {e}"));
        let seconds = if has_seconds {
            ctx("second", field(parts[0], 0, 59, &[], 0))?
        } else {
            Field {
                bits: 1,
                any: false,
            }
        };
        let minutes = ctx("minute", field(rest[0], 0, 59, &[], 0))?;
        let hours = ctx("hour", field(rest[1], 0, 23, &[], 0))?;
        let days = ctx("day-of-month", field(rest[2], 1, 31, &[], 0))?;
        let months = ctx("month", field(rest[3], 1, 12, &MONTHS, 1))?;
        let mut weekdays = ctx("day-of-week", field(rest[4], 0, 7, &WEEKDAYS, 0))?;
        if weekdays.matches(7) {
            weekdays.bits |= 1;
        }
        let schedule = Self {
            seconds,
            minutes,
            hours,
            days,
            months,
            weekdays,
            has_seconds,
        };
        // An expression that never matches is refused up front.
        let probe = NaiveDate::from_ymd_opt(2000, 1, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .expect("a valid date");
        if schedule.next_local_after(probe).is_none() {
            return Err(format!(
                "`{expression}` never matches within {SEARCH_YEARS} years"
            ));
        }
        Ok(schedule)
    }

    /// True for a 6-field expression.
    pub fn has_seconds(&self) -> bool {
        self.has_seconds
    }

    fn day_matches(&self, d: NaiveDate) -> bool {
        let dom = self.days.matches(d.day());
        let dow = self.weekdays.matches(d.weekday().num_days_from_sunday());
        match (self.days.any, self.weekdays.any) {
            (true, true) => true,
            (true, false) => dow,
            (false, true) => dom,
            (false, false) => dom || dow,
        }
    }

    /// The first matching local time strictly after `after` (whole seconds).
    fn next_local_after(&self, after: NaiveDateTime) -> Option<NaiveDateTime> {
        let mut t = after.with_nanosecond(0)? + Duration::seconds(1);
        let limit = after.year() + SEARCH_YEARS;
        while t.year() <= limit {
            if !self.months.matches(t.month()) {
                let (y, m) = if t.month() == 12 {
                    (t.year() + 1, 1)
                } else {
                    (t.year(), t.month() + 1)
                };
                t = NaiveDate::from_ymd_opt(y, m, 1)?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !self.day_matches(t.date()) {
                t = t.date().succ_opt()?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !self.hours.matches(t.hour()) {
                t = t.date().and_hms_opt(t.hour(), 0, 0)? + Duration::hours(1);
                continue;
            }
            if !self.minutes.matches(t.minute()) {
                t = t.date().and_hms_opt(t.hour(), t.minute(), 0)? + Duration::minutes(1);
                continue;
            }
            if !self.seconds.matches(t.second()) {
                t += Duration::seconds(1);
                continue;
            }
            return Some(t);
        }
        None
    }

    /// The first scheduled instant strictly after `after`, in `tz` (module
    /// docs: nonexistent local times are skipped, repeated ones fire once at
    /// their earlier instant).
    pub fn next_after(&self, tz: Tz, after: Timestamp) -> Option<Timestamp> {
        let mut local = after.with_timezone(&tz).naive_local();
        for _ in 0..MAX_LOCAL_STEPS {
            local = self.next_local_after(local)?;
            let instant = match tz.from_local_datetime(&local) {
                LocalResult::None => continue,
                LocalResult::Single(t) => t,
                LocalResult::Ambiguous(earlier, _) => earlier,
            };
            let utc = instant.with_timezone(&Utc);
            if utc > after {
                return Some(utc);
            }
        }
        None
    }

    /// Every scheduled instant in `(after, until]`, at most `max`, oldest
    /// first. The second value is true when more were left out.
    pub fn between(
        &self,
        tz: Tz,
        after: Timestamp,
        until: Timestamp,
        max: usize,
    ) -> (Vec<Timestamp>, bool) {
        let mut out = Vec::new();
        let mut cursor = after;
        while let Some(t) = self.next_after(tz, cursor) {
            if t > until {
                return (out, false);
            }
            if out.len() == max {
                return (out, true);
            }
            out.push(t);
            cursor = t;
        }
        (out, false)
    }
}

/// Parse an IANA zone name (`UTC`, `Asia/Tokyo`, `America/New_York`).
pub fn parse_timezone(name: &str) -> Result<Tz, String> {
    name.parse::<Tz>()
        .map_err(|_| format!("`{name}` is not an IANA time zone"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> Timestamp {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn next(expr: &str, tz: &str, after: &str) -> String {
        CronSchedule::parse(expr)
            .unwrap()
            .next_after(parse_timezone(tz).unwrap(), utc(after))
            .unwrap()
            .to_rfc3339()
    }

    fn all(expr: &str, tz: &str, after: &str, until: &str) -> Vec<String> {
        CronSchedule::parse(expr)
            .unwrap()
            .between(parse_timezone(tz).unwrap(), utc(after), utc(until), 1000)
            .0
            .iter()
            .map(|t| t.to_rfc3339())
            .collect()
    }

    #[test]
    fn five_and_six_field_expressions_and_macros_parse() {
        assert_eq!(
            next("*/15 * * * *", "UTC", "2026-01-01T00:07:00Z"),
            "2026-01-01T00:15:00+00:00"
        );
        assert_eq!(
            next("*/5 * * * * *", "UTC", "2026-01-01T00:00:07Z"),
            "2026-01-01T00:00:10+00:00"
        );
        assert!(CronSchedule::parse("*/5 * * * * *").unwrap().has_seconds());
        assert_eq!(
            next("@daily", "UTC", "2026-01-01T00:00:00Z"),
            "2026-01-02T00:00:00+00:00"
        );
        assert_eq!(
            next("0 9 * * MON-FRI", "UTC", "2026-09-18T10:00:00Z"), // a Friday
            "2026-09-21T09:00:00+00:00"
        );
        assert_eq!(
            next("0 0 1 JAN,jul *", "UTC", "2026-02-01T00:00:00Z"),
            "2026-07-01T00:00:00+00:00"
        );
        // Sunday is 0 and 7.
        assert_eq!(
            next("0 0 * * 7", "UTC", "2026-09-17T00:00:00Z"),
            next("0 0 * * 0", "UTC", "2026-09-17T00:00:00Z")
        );
        // 5/20 in minutes means 5,25,45.
        assert_eq!(
            all(
                "5/20 0 * * *",
                "UTC",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:59:00Z"
            ),
            vec![
                "2026-01-01T00:05:00+00:00",
                "2026-01-01T00:25:00+00:00",
                "2026-01-01T00:45:00+00:00"
            ]
        );
    }

    #[test]
    fn day_of_month_and_day_of_week_match_either_when_both_are_restricted() {
        // The 13th, or any Friday.
        let got = all(
            "0 0 13 * FRI",
            "UTC",
            "2026-11-01T00:00:00Z",
            "2026-11-14T00:00:00Z",
        );
        assert_eq!(
            got,
            vec!["2026-11-06T00:00:00+00:00", "2026-11-13T00:00:00+00:00"]
        );
    }

    #[test]
    fn invalid_expressions_are_refused() {
        for bad in [
            "",
            "* * * *",
            "* * * * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * * 13 *",
            "* * * * 8",
            "*/0 * * * *",
            "5-1 * * * *",
            "* * * FOO *",
            "@every 5s",
            "0 0 30 2 *",
        ] {
            assert!(CronSchedule::parse(bad).is_err(), "{bad:?} must be refused");
        }
        assert!(parse_timezone("Mars/Olympus").is_err());
        assert!(parse_timezone("Asia/Tokyo").is_ok());
    }

    /// Spring forward in New York (2026-03-08, 02:00 EST -> 03:00 EDT): the
    /// local time 02:30 does not exist and is skipped; the hourly schedule
    /// has no 02:00 fire that day.
    #[test]
    fn a_local_time_skipped_by_dst_does_not_fire() {
        assert_eq!(
            all(
                "30 2 * * *",
                "America/New_York",
                "2026-03-07T00:00:00Z",
                "2026-03-10T00:00:00Z"
            ),
            vec![
                "2026-03-07T07:30:00+00:00", // 02:30 EST
                "2026-03-09T06:30:00+00:00"  // 02:30 EDT, the 8th is skipped
            ]
        );
        assert_eq!(
            all(
                "0 * * * *",
                "America/New_York",
                "2026-03-08T05:30:00Z",
                "2026-03-08T07:30:00Z"
            ),
            vec![
                "2026-03-08T06:00:00+00:00", // 01:00 EST
                "2026-03-08T07:00:00+00:00"  // 03:00 EDT (no 02:00)
            ]
        );
    }

    /// Fall back in New York (2026-11-01, 02:00 EDT -> 01:00 EST): 01:30
    /// exists twice and fires once, at the earlier (EDT) instant; a
    /// quarter-hourly schedule fires 4 times in the repeated hour, not 8.
    #[test]
    fn a_repeated_local_time_fires_once_at_its_earlier_instant() {
        assert_eq!(
            all(
                "30 1 * * *",
                "America/New_York",
                "2026-10-31T12:00:00Z",
                "2026-11-02T12:00:00Z"
            ),
            vec![
                "2026-11-01T05:30:00+00:00", // 01:30 EDT only
                "2026-11-02T06:30:00+00:00"  // 01:30 EST
            ]
        );
        let got = all(
            "*/15 * * * *",
            "America/New_York",
            "2026-11-01T04:50:00Z", // 00:50 EDT
            "2026-11-01T07:10:00Z", // 02:10 EST
        );
        assert_eq!(
            got,
            vec![
                "2026-11-01T05:00:00+00:00", // 01:00 EDT
                "2026-11-01T05:15:00+00:00",
                "2026-11-01T05:30:00+00:00",
                "2026-11-01T05:45:00+00:00", // 01:45 EDT; 01:xx EST is not repeated
                "2026-11-01T07:00:00+00:00", // 02:00 EST
            ]
        );
        // A cursor inside the second (EST) occurrence of the repeated hour
        // does not go back to the first one.
        assert_eq!(
            next("*/15 * * * *", "America/New_York", "2026-11-01T06:20:00Z"),
            "2026-11-01T07:00:00+00:00"
        );
    }

    /// Asia/Tokyo has no DST: every local time maps to exactly one instant,
    /// nine hours ahead of UTC, including across the dates the US and Europe
    /// change their clocks.
    #[test]
    fn a_zone_without_dst_maps_every_local_time_once() {
        assert_eq!(
            all(
                "30 2 * * *",
                "Asia/Tokyo",
                "2026-03-07T00:00:00Z",
                "2026-03-10T00:00:00Z"
            ),
            vec![
                "2026-03-07T17:30:00+00:00",
                "2026-03-08T17:30:00+00:00",
                "2026-03-09T17:30:00+00:00"
            ]
        );
        // Midnight in Tokyo is 15:00 UTC the day before: a day boundary.
        assert_eq!(
            next("0 0 1 * *", "Asia/Tokyo", "2026-09-30T14:59:59Z"),
            "2026-09-30T15:00:00+00:00"
        );
        assert_eq!(
            next("0 0 1 * *", "Asia/Tokyo", "2026-09-30T15:00:00Z"),
            "2026-10-31T15:00:00+00:00"
        );
    }

    #[test]
    fn between_is_bounded_and_reports_truncation() {
        let s = CronSchedule::parse("* * * * * *").unwrap();
        let (got, more) = s.between(
            Tz::UTC,
            utc("2026-01-01T00:00:00Z"),
            utc("2026-01-01T01:00:00Z"),
            10,
        );
        assert_eq!(got.len(), 10);
        assert!(more);
        let leap = CronSchedule::parse("0 0 29 2 *").unwrap();
        assert_eq!(
            leap.next_after(Tz::UTC, utc("2026-03-01T00:00:00Z"))
                .unwrap()
                .to_rfc3339(),
            "2028-02-29T00:00:00+00:00"
        );
    }
}
