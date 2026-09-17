//! Test-only failpoints (PLT-4639, docs/adr/0010 §「failpoint」).
//!
//! A failpoint is a named place in the asynchronous acceptance and outbox
//! publishing paths where a test can make the process fail on purpose: return
//! an error, panic, or kill the whole process with `SIGKILL` (the E2E script
//! restarts it afterwards). They exist to prove that every crash window
//! converges after a restart.
//!
//! Failpoints are **inert unless the crate is built for its own unit tests or
//! with the `failpoints` feature** ([`ENABLED`]). A release gateway ignores
//! both [`Failpoints::set`] and `TSLS_FAILPOINTS`, so a stray environment
//! variable in production cannot crash it.
//!
//! `TSLS_FAILPOINTS` (read once at bootstrap) is a `;`-separated list of
//! `name=action[*count]` with action `error` | `panic` | `kill`, e.g.
//! `TSLS_FAILPOINTS="outbox.after_publish=kill"` or
//! `TSLS_FAILPOINTS="outbox.before_publish=error*3"`.

use std::collections::HashMap;

use parking_lot::Mutex;

/// Whether failpoints can fire in this build.
pub const ENABLED: bool = cfg!(any(test, feature = "failpoints"));

/// Environment variable read by [`Failpoints::from_env`].
pub const ENV: &str = "TSLS_FAILPOINTS";

/// After the input object was stored, before the acceptance transaction.
pub const ACCEPT_AFTER_OBJECT_PUT: &str = "accept.after_object_put";
/// Inside the acceptance transaction, after every row was written, before
/// `COMMIT` (the transaction rolls back).
pub const ACCEPT_BEFORE_COMMIT: &str = "accept.before_commit";
/// After the acceptance transaction committed, before the 202 is returned.
pub const ACCEPT_AFTER_COMMIT: &str = "accept.after_commit";
/// The object store refuses the put as unavailable.
pub const OBJECT_PUT_UNAVAILABLE: &str = "objects.put_unavailable";
/// A publisher claimed an outbox row, before it publishes.
pub const OUTBOX_BEFORE_PUBLISH: &str = "outbox.before_publish";
/// The queue answers the publish as unavailable (a stopped broker).
pub const OUTBOX_QUEUE_UNAVAILABLE: &str = "outbox.queue_unavailable";
/// The broker acknowledged the publish, before the row is marked sent.
pub const OUTBOX_AFTER_PUBLISH: &str = "outbox.after_publish";

/// A dispatcher claimed a run of an asynchronous invocation, before the
/// handler starts (a crash mid-run: the claim expires, the next run retries).
pub const DISPATCH_AFTER_CLAIM: &str = "dispatch.after_claim";
/// The run finished (its side effects happened), before its outcome or retry
/// is committed.
pub const DISPATCH_BEFORE_COMMIT: &str = "dispatch.before_commit";
/// A retry was decided, before the retry schedule is committed.
pub const DISPATCH_BEFORE_RETRY_COMMIT: &str = "dispatch.before_retry_commit";
/// The terminal state (or the retry schedule) is committed, before the queue
/// message is acknowledged.
pub const DISPATCH_AFTER_COMMIT: &str = "dispatch.after_commit";

pub const ALL: &[&str] = &[
    ACCEPT_AFTER_OBJECT_PUT,
    ACCEPT_BEFORE_COMMIT,
    ACCEPT_AFTER_COMMIT,
    OBJECT_PUT_UNAVAILABLE,
    OUTBOX_BEFORE_PUBLISH,
    OUTBOX_QUEUE_UNAVAILABLE,
    OUTBOX_AFTER_PUBLISH,
    DISPATCH_AFTER_CLAIM,
    DISPATCH_BEFORE_COMMIT,
    DISPATCH_BEFORE_RETRY_COMMIT,
    DISPATCH_AFTER_COMMIT,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// The caller returns an error as if the step had failed.
    Error,
    /// Panic at the failpoint.
    Panic,
    /// `SIGKILL` this process: nothing after the failpoint runs, no destructor,
    /// no flush.
    Kill,
}

impl Action {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "error" => Some(Self::Error),
            "panic" => Some(Self::Panic),
            "kill" => Some(Self::Kill),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Armed {
    action: Action,
    /// Fires this many more times; `None` forever.
    remaining: Option<u32>,
}

/// The failpoints of one application instance (tests run many in parallel,
/// so there is no global registry).
#[derive(Debug, Default)]
pub struct Failpoints {
    armed: Mutex<HashMap<String, Armed>>,
    hits: Mutex<HashMap<String, u64>>,
}

impl Failpoints {
    /// Parse `TSLS_FAILPOINTS`. Unknown names or actions are logged and ignored.
    pub fn from_env() -> Self {
        let fp = Self::default();
        if !ENABLED {
            if std::env::var_os(ENV).is_some() {
                tracing::warn!(
                    "{ENV} is set but this build has no failpoints (feature `failpoints`); ignored"
                );
            }
            return fp;
        }
        if let Ok(spec) = std::env::var(ENV) {
            for item in spec.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                let Some((name, rest)) = item.split_once('=') else {
                    tracing::warn!(item, "failpoint spec must be name=action[*count]");
                    continue;
                };
                let (action, count) = match rest.split_once('*') {
                    Some((a, n)) => (a, n.parse::<u32>().ok()),
                    None => (rest, None),
                };
                match (ALL.contains(&name), Action::parse(action)) {
                    (true, Some(action)) => {
                        tracing::warn!(name, ?action, ?count, "failpoint armed from {ENV}");
                        fp.set(name, action, count);
                    }
                    _ => tracing::warn!(item, "unknown failpoint or action; ignored"),
                }
            }
        }
        fp
    }

    /// Arm `name`. `times = None` fires on every hit.
    pub fn set(&self, name: &str, action: Action, times: Option<u32>) {
        if !ENABLED {
            return;
        }
        self.armed.lock().insert(
            name.to_string(),
            Armed {
                action,
                remaining: times,
            },
        );
    }

    pub fn clear(&self, name: &str) {
        self.armed.lock().remove(name);
    }

    /// How often `name` fired.
    pub fn hits(&self, name: &str) -> u64 {
        self.hits.lock().get(name).copied().unwrap_or(0)
    }

    /// Evaluate `name`. `Kill` and `Panic` never return; `true` means the
    /// caller must fail the step.
    pub fn fire(&self, name: &str) -> bool {
        if !ENABLED {
            return false;
        }
        let action = {
            let mut armed = self.armed.lock();
            let Some(entry) = armed.get_mut(name) else {
                return false;
            };
            let action = entry.action;
            match &mut entry.remaining {
                Some(0) => {
                    armed.remove(name);
                    return false;
                }
                Some(n) => {
                    *n -= 1;
                    if *n == 0 {
                        armed.remove(name);
                    }
                }
                None => {}
            }
            action
        };
        *self.hits.lock().entry(name.to_string()).or_default() += 1;
        match action {
            Action::Error => true,
            Action::Panic => panic!("failpoint {name} fired"),
            Action::Kill => {
                tracing::error!(name, "failpoint fired: SIGKILL");
                // SAFETY: kill(2) on our own pid with a constant signal.
                unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                }
                // SIGKILL cannot be caught; this is never reached.
                std::process::abort()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counted_failpoints_disarm_themselves() {
        let fp = Failpoints::default();
        assert!(!fp.fire(ACCEPT_BEFORE_COMMIT));
        fp.set(ACCEPT_BEFORE_COMMIT, Action::Error, Some(2));
        assert!(fp.fire(ACCEPT_BEFORE_COMMIT));
        assert!(fp.fire(ACCEPT_BEFORE_COMMIT));
        assert!(!fp.fire(ACCEPT_BEFORE_COMMIT));
        assert_eq!(fp.hits(ACCEPT_BEFORE_COMMIT), 2);
        fp.set(OUTBOX_AFTER_PUBLISH, Action::Error, None);
        for _ in 0..5 {
            assert!(fp.fire(OUTBOX_AFTER_PUBLISH));
        }
        fp.clear(OUTBOX_AFTER_PUBLISH);
        assert!(!fp.fire(OUTBOX_AFTER_PUBLISH));
    }
}
