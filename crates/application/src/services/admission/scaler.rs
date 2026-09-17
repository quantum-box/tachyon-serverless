//! Clock-driven building blocks of the autoscaler: the start-rate token
//! bucket, the per-revision start-failure circuit breaker and the demand
//! estimate behind `desired`. Everything takes `now` explicitly, so the fake
//! clock tests are deterministic.

use serde::Serialize;

use tachyon_serverless_domain::Timestamp;

fn secs_between(from: Timestamp, to: Timestamp) -> f64 {
    ((to - from).num_microseconds().unwrap_or(i64::MAX) as f64 / 1e6).max(0.0)
}

// ---------------------------------------------------------------------------
// start-rate limiter
// ---------------------------------------------------------------------------

/// Token bucket: `burst` tokens, refilled at `per_second`.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    per_second: f64,
    burst: f64,
    tokens: f64,
    at: Option<Timestamp>,
}

impl TokenBucket {
    pub fn new(per_second: u32, burst: u32) -> Self {
        Self {
            per_second: f64::from(per_second.max(1)),
            burst: f64::from(burst.max(1)),
            tokens: f64::from(burst.max(1)),
            at: None,
        }
    }

    fn refill(&mut self, now: Timestamp) {
        if let Some(at) = self.at {
            self.tokens = (self.tokens + secs_between(at, now) * self.per_second).min(self.burst);
        }
        if self.at.is_none_or(|at| now > at) {
            self.at = Some(now);
        }
    }

    /// Whether a start may happen now (without taking the token).
    pub fn available(&mut self, now: Timestamp) -> bool {
        self.refill(now);
        self.tokens >= 1.0
    }

    /// Take one token. Callers check [`Self::available`] first.
    pub fn take(&mut self, now: Timestamp) -> bool {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Whole tokens left at `now`.
    pub fn tokens(&mut self, now: Timestamp) -> u32 {
        self.refill(now);
        self.tokens.floor() as u32
    }

    pub fn per_second(&self) -> u32 {
        self.per_second as u32
    }

    pub fn burst(&self) -> u32 {
        self.burst as u32
    }
}

// ---------------------------------------------------------------------------
// circuit breaker
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed { consecutive_failures: u32 },
    Open { until: Timestamp },
    HalfOpen { probe_in_flight: bool },
}

/// What the breaker lets a cold start of its revision do right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerGate {
    /// Start freely.
    Allow,
    /// Half-open: this start is the single probe.
    Probe,
    /// Half-open with the probe still running: wait for its result.
    Wait,
    /// Open: reject fast.
    Reject,
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    threshold: u32,
    cooldown: chrono::Duration,
    state: BreakerState,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown_seconds: u64) -> Self {
        Self {
            threshold: threshold.max(1),
            cooldown: chrono::Duration::seconds(cooldown_seconds.min(i64::MAX as u64 / 1000) as i64),
            state: BreakerState::Closed {
                consecutive_failures: 0,
            },
        }
    }

    /// Move `Open` to `HalfOpen` once the cooldown passed.
    fn tick(&mut self, now: Timestamp) {
        if let BreakerState::Open { until } = self.state
            && now >= until
        {
            self.state = BreakerState::HalfOpen {
                probe_in_flight: false,
            };
        }
    }

    pub fn state(&mut self, now: Timestamp) -> BreakerState {
        self.tick(now);
        self.state
    }

    pub fn gate(&mut self, now: Timestamp) -> BreakerGate {
        self.tick(now);
        match self.state {
            BreakerState::Closed { .. } => BreakerGate::Allow,
            BreakerState::Open { .. } => BreakerGate::Reject,
            BreakerState::HalfOpen {
                probe_in_flight: false,
            } => BreakerGate::Probe,
            BreakerState::HalfOpen {
                probe_in_flight: true,
            } => BreakerGate::Wait,
        }
    }

    /// Whether new arrivals are refused outright.
    pub fn is_open(&mut self, now: Timestamp) -> bool {
        self.gate(now) == BreakerGate::Reject
    }

    pub fn probe_started(&mut self) {
        if let BreakerState::HalfOpen { .. } = self.state {
            self.state = BreakerState::HalfOpen {
                probe_in_flight: true,
            };
        }
    }

    /// The probe ended without a boot result (cancelled, client deadline):
    /// let another one try.
    pub fn probe_abandoned(&mut self) {
        if let BreakerState::HalfOpen {
            probe_in_flight: true,
        } = self.state
        {
            self.state = BreakerState::HalfOpen {
                probe_in_flight: false,
            };
        }
    }

    pub fn record_success(&mut self) {
        self.state = BreakerState::Closed {
            consecutive_failures: 0,
        };
    }

    /// Returns true when this failure opened the breaker.
    pub fn record_failure(&mut self, now: Timestamp) -> bool {
        self.tick(now);
        match self.state {
            BreakerState::Closed {
                consecutive_failures,
            } => {
                let n = consecutive_failures + 1;
                if n >= self.threshold {
                    self.state = BreakerState::Open {
                        until: now + self.cooldown,
                    };
                    true
                } else {
                    self.state = BreakerState::Closed {
                        consecutive_failures: n,
                    };
                    false
                }
            }
            BreakerState::HalfOpen { .. } => {
                self.state = BreakerState::Open {
                    until: now + self.cooldown,
                };
                true
            }
            // A start that was granted before the breaker opened: keep the
            // cooldown that is already running.
            BreakerState::Open { .. } => false,
        }
    }

    pub fn name(&mut self, now: Timestamp) -> &'static str {
        match self.state(now) {
            BreakerState::Closed { .. } => "closed",
            BreakerState::Open { .. } => "open",
            BreakerState::HalfOpen { .. } => "half_open",
        }
    }
}

// ---------------------------------------------------------------------------
// demand estimate
// ---------------------------------------------------------------------------

/// Decaying arrival rate and handler duration of one revision.
#[derive(Debug, Clone, Default)]
pub struct DemandStats {
    /// Exponentially decayed arrival count divided by the window: arrivals
    /// per second as of `rate_at`.
    rate: f64,
    rate_at: Option<Timestamp>,
    /// Moving average of handler durations, seconds.
    avg_duration: Option<f64>,
}

impl DemandStats {
    fn decayed(&self, now: Timestamp, window: f64) -> f64 {
        match self.rate_at {
            Some(at) => self.rate * (-secs_between(at, now) / window).exp(),
            None => 0.0,
        }
    }

    pub fn record_arrival(&mut self, now: Timestamp, window: f64) {
        self.rate = self.decayed(now, window) + 1.0 / window;
        self.rate_at = Some(match self.rate_at {
            Some(at) if at > now => at,
            _ => now,
        });
    }

    pub fn record_duration(&mut self, seconds: f64) {
        self.avg_duration = Some(match self.avg_duration {
            Some(avg) => 0.8 * avg + 0.2 * seconds,
            None => seconds,
        });
    }

    pub fn arrival_rate(&self, now: Timestamp, window: f64) -> f64 {
        self.decayed(now, window)
    }

    pub fn avg_duration(&self) -> Option<f64> {
        self.avg_duration
    }
}

/// Inputs of [`desired_environments`].
#[derive(Debug, Clone, Copy)]
pub struct DemandInput {
    pub arrival_rate: f64,
    pub avg_duration_seconds: f64,
    pub in_flight: u32,
    pub backlog: u32,
    pub concurrency_per_environment: u32,
    pub min_ready: u32,
    pub max_environments: u32,
}

/// `desired = ceil((max(λ·W, in_flight) + backlog) / concurrency)`, clamped to
/// `[min_ready, max_environments]`.
///
/// `λ·W` (Little's law) already estimates the number of invocations in
/// flight, so it is not *added* to the observed in-flight count: that would
/// count every running invocation twice and overshoot after every burst.
/// The larger of the estimate and the observation is used instead, and every
/// waiting invocation adds one unit of backlog pressure.
pub fn desired_environments(d: DemandInput) -> u32 {
    let expected = (d.arrival_rate * d.avg_duration_seconds).max(f64::from(d.in_flight));
    let load = expected + f64::from(d.backlog);
    let per_env = f64::from(d.concurrency_per_environment.max(1));
    let raw = (load / per_env - 1e-9).ceil().max(0.0);
    let raw = if raw > f64::from(u32::MAX) {
        u32::MAX
    } else {
        raw as u32
    };
    raw.clamp(d.min_ready.min(d.max_environments), d.max_environments)
}

#[derive(Debug, Clone, Serialize)]
pub struct BreakerView {
    pub state: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(ms: i64) -> Timestamp {
        chrono::Utc
            .timestamp_millis_opt(1_800_000_000_000 + ms)
            .unwrap()
    }

    #[test]
    fn token_bucket_limits_bursts_and_refills_with_the_clock() {
        let mut b = TokenBucket::new(2, 3);
        for _ in 0..3 {
            assert!(b.take(t(0)));
        }
        assert!(!b.take(t(0)), "the burst is spent");
        assert!(!b.take(t(400)), "0.8 tokens after 400 ms");
        assert!(b.take(t(500)), "one token after 500 ms");
        assert!(!b.available(t(500)));
        assert_eq!(b.tokens(t(10_000)), 3, "never above the burst");
    }

    #[test]
    fn breaker_opens_after_k_failures_probes_once_and_closes_on_success() {
        let mut br = CircuitBreaker::new(3, 10);
        assert_eq!(br.gate(t(0)), BreakerGate::Allow);
        assert!(!br.record_failure(t(0)));
        assert!(!br.record_failure(t(1)));
        br.record_success();
        assert!(!br.record_failure(t(2)), "a success resets the count");
        assert!(!br.record_failure(t(3)));
        assert!(br.record_failure(t(4)), "third consecutive failure opens");
        assert_eq!(br.gate(t(5)), BreakerGate::Reject);
        assert_eq!(br.gate(t(10_003)), BreakerGate::Reject);
        assert_eq!(br.gate(t(10_004)), BreakerGate::Probe, "cooldown over");
        br.probe_started();
        assert_eq!(br.gate(t(10_005)), BreakerGate::Wait, "one probe at a time");
        assert!(br.record_failure(t(10_006)), "a failed probe reopens");
        assert_eq!(br.gate(t(10_007)), BreakerGate::Reject);
        assert_eq!(br.gate(t(20_006)), BreakerGate::Probe);
        br.probe_started();
        br.probe_abandoned();
        assert_eq!(br.gate(t(20_007)), BreakerGate::Probe, "abandoned probe");
        br.probe_started();
        br.record_success();
        assert_eq!(br.gate(t(20_008)), BreakerGate::Allow);
    }

    #[test]
    fn desired_uses_the_larger_of_estimate_and_observation_plus_backlog() {
        let base = DemandInput {
            arrival_rate: 0.0,
            avg_duration_seconds: 0.0,
            in_flight: 0,
            backlog: 0,
            concurrency_per_environment: 1,
            min_ready: 0,
            max_environments: 10,
        };
        assert_eq!(desired_environments(base), 0);
        assert_eq!(desired_environments(DemandInput { backlog: 7, ..base }), 7);
        // 4/s × 0.5 s = 2 expected in flight; 3 observed wins.
        assert_eq!(
            desired_environments(DemandInput {
                arrival_rate: 4.0,
                avg_duration_seconds: 0.5,
                in_flight: 3,
                backlog: 1,
                ..base
            }),
            4
        );
        // 10/s × 1 s = 10 expected; clamped by max_environments.
        assert_eq!(
            desired_environments(DemandInput {
                arrival_rate: 10.0,
                avg_duration_seconds: 1.0,
                in_flight: 2,
                backlog: 5,
                ..base
            }),
            10
        );
        assert_eq!(
            desired_environments(DemandInput {
                in_flight: 3,
                concurrency_per_environment: 2,
                ..base
            }),
            2
        );
    }

    #[test]
    fn arrival_rate_decays_with_the_clock() {
        let mut s = DemandStats::default();
        for i in 0..10 {
            s.record_arrival(t(i * 100), 10.0);
        }
        let r = s.arrival_rate(t(1000), 10.0);
        assert!((0.85..=1.0).contains(&r), "{r}");
        let later = s.arrival_rate(t(31_000), 10.0);
        assert!(later < r * 0.06, "{later}");
        s.record_duration(2.0);
        s.record_duration(1.0);
        assert!((s.avg_duration().unwrap() - 1.8).abs() < 1e-9);
    }
}
