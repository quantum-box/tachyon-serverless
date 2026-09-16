//! Injectable clock and identifier generation.
//!
//! Application code must not call `Utc::now()` or `Ulid::new()` directly so
//! that tests can control time and identifiers deterministically.

use chrono::{DateTime, Utc};

pub type Timestamp = DateTime<Utc>;

/// Source of wall-clock time.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// Real system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Utc::now()
    }
}

/// A clock that returns a fixed, manually advanced time. Test helper.
#[derive(Debug)]
pub struct FixedClock {
    now: std::sync::Mutex<Timestamp>,
}

impl FixedClock {
    pub fn new(now: Timestamp) -> Self {
        Self {
            now: std::sync::Mutex::new(now),
        }
    }

    pub fn set(&self, now: Timestamp) {
        *self.now.lock().expect("clock poisoned") = now;
    }

    pub fn advance(&self, delta: chrono::Duration) {
        let mut guard = self.now.lock().expect("clock poisoned");
        *guard += delta;
    }
}

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        *self.now.lock().expect("clock poisoned")
    }
}

/// Source of fresh ULIDs.
pub trait IdGenerator: Send + Sync {
    fn next_ulid(&self) -> ulid::Ulid;
}

/// Random ULID generator.
#[derive(Debug, Default, Clone, Copy)]
pub struct UlidGenerator;

impl IdGenerator for UlidGenerator {
    fn next_ulid(&self) -> ulid::Ulid {
        ulid::Ulid::new()
    }
}

/// Sequential generator for deterministic tests.
#[derive(Debug, Default)]
pub struct SequentialIdGenerator {
    counter: std::sync::atomic::AtomicU64,
}

impl IdGenerator for SequentialIdGenerator {
    fn next_ulid(&self) -> ulid::Ulid {
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        ulid::Ulid::from_parts(0, n as u128)
    }
}
