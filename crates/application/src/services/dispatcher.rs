//! Dispatcher identity, heartbeat and reclaim (PLT-4631,
//! docs/architecture.md §4, docs/threat-model.md §6).
//!
//! Every [`crate::Application`] registers one [`Dispatcher`]: a fresh
//! [`DispatcherId`] per process start, recorded in the store with the
//! instance name, host name and pid. Everything the gateway accepts and
//! creates carries that id as its owner, and every slot lease it takes is
//! owned by it.
//!
//! - **Heartbeat.** [`Dispatcher::heartbeat`] renews the dispatcher lease and
//!   all of its unexpired slot leases. An ordinary renewal never revives a
//!   lease that already passed (only the store-outage renewal below does, and
//!   only for a process that kept running); a dispatcher whose heartbeat is
//!   refused is *fenced*: it
//!   refuses new invocations (`ProviderUnavailable`), and whatever it still
//!   drives is refused at the store when it completes, because the reclaimer
//!   released the lease and moved the environment's epoch.
//! - **Reclaim.** [`Dispatcher::reclaim_request`] describes what this process
//!   may reclaim: other dispatchers whose lease is past expiry plus the
//!   tolerated clock skew, that stopped, or that are the previous incarnation
//!   of the same instance on the same host whose process is provably gone
//!   (its pid is not alive, or it lived in this very process and its handle
//!   was dropped). A live dispatcher's work is never touched, so two gateways
//!   on one `data_dir` do not settle each other's invocations. A reclaimer
//!   that no longer holds its own lease reclaims nothing.
//! - **Store outage (PLT-4646).** A heartbeat the store could not answer
//!   (another process holds the SQLite write lock, the connection stayed
//!   busy) neither renews nor fences. When the store answers again and the
//!   ordinary renewal is refused because the lease passed meanwhile, a
//!   dispatcher whose process kept running renews through
//!   [`SlotStore::renew_after_store_outage`], which succeeds as long as
//!   nobody reclaimed it. "Kept running" means no stall of the process since
//!   its last renewal: a watchdog thread notices when the process was not
//!   scheduled for [`PROCESS_STALL`] or longer (SIGSTOP, a frozen VM). A
//!   process that was itself frozen is fenced as before: of two gateways
//!   stuck behind one frozen writer, the one that was running keeps its work
//!   and reclaims the one that was not (docs/adr/0003 「store が止まった間の
//!   lease」).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use tachyon_serverless_domain::{Clock, DispatcherId, IdGenerator, Timestamp};

use crate::config::DispatcherConfig;
use crate::error::AppError;
use crate::repository::{
    DispatcherRecord, HeartbeatOutcome, ReclaimReport, ReclaimRequest, RepoError, SlotStore,
};

/// A wake-up of the watchdog this late means the whole process was not
/// running (stopped, frozen, starved) for about that long.
pub const PROCESS_STALL: std::time::Duration = std::time::Duration::from_secs(5);

/// When the watchdog last noticed this process had stalled.
static LAST_STALL: Mutex<Option<std::time::Instant>> = Mutex::new(None);
/// When the watchdog last woke up. A heartbeat that runs right after the
/// process continued may come before the watchdog noticed the stall; a beat
/// older than [`PROCESS_STALL`] is that stall.
static LAST_BEAT: Mutex<Option<std::time::Instant>> = Mutex::new(None);

/// Start (once per process) the thread that notices stalls of the process.
fn watch_for_stalls() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        let spawned = std::thread::Builder::new()
            .name("tsls-stall-watchdog".into())
            .spawn(|| {
                let tick = std::time::Duration::from_millis(250);
                let mut last = std::time::Instant::now();
                loop {
                    std::thread::sleep(tick);
                    let now = std::time::Instant::now();
                    *LAST_BEAT.lock() = Some(now);
                    let late = now.saturating_duration_since(last).saturating_sub(tick);
                    if late >= PROCESS_STALL {
                        *LAST_STALL.lock() = Some(now);
                        tracing::warn!(
                            stalled_ms = late.as_millis() as u64,
                            "this process was not running for a while (stopped, frozen or \
                             starved): a lease that passed meanwhile is not taken back"
                        );
                    }
                    last = now;
                }
            });
        if let Err(e) = spawned {
            tracing::warn!(error = %e, "cannot start the stall watchdog; store outages always fence");
            WATCHDOG_DOWN.store(true, Ordering::SeqCst);
        }
    });
}

/// Set when the watchdog could not be started: nothing proves the process
/// kept running, so no lease is taken back.
static WATCHDOG_DOWN: AtomicBool = AtomicBool::new(false);

/// Whether the process stalled at or after `since`, or may be stalled right
/// now without the watchdog having run since.
fn stalled_since(since: std::time::Instant) -> bool {
    if WATCHDOG_DOWN.load(Ordering::SeqCst) {
        return true;
    }
    let beat_is_stale = LAST_BEAT
        .lock()
        .is_some_and(|beat| beat.elapsed() >= PROCESS_STALL);
    beat_is_stale || LAST_STALL.lock().is_some_and(|at| at >= since)
}

/// Dispatchers alive in this process. A dispatcher that shares this process's
/// pid but is not in the set was dropped: its incarnation is over.
static LIVE_IN_PROCESS: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

/// The host name, for telling "the same host" apart. Empty when unknown,
/// which never matches, so nothing is presumed dead on a guess.
pub fn hostname() -> String {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: the buffer is valid for its whole length and gethostname
        // writes at most that many bytes.
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if rc == 0 {
            let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
            return String::from_utf8_lossy(&buf[..end]).into_owned();
        }
    }
    String::new()
}

/// Whether `pid` names a live process on this host. Errs on the side of
/// "alive": only a definite "no such process" counts as dead.
pub fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return true;
        };
        if pid <= 0 {
            return true;
        }
        // SAFETY: signal 0 performs the permission and existence checks only.
        let rc = unsafe { libc::kill(pid, 0) };
        if rc == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

pub struct Dispatcher {
    id: DispatcherId,
    instance: String,
    hostname: String,
    pid: u32,
    slots: Arc<dyn SlotStore>,
    clock: Arc<dyn Clock>,
    config: DispatcherConfig,
    fenced: AtomicBool,
    heartbeats: Mutex<HeartbeatTrack>,
}

/// What the heartbeat remembers between attempts (store outage handling).
#[derive(Debug, Default)]
struct HeartbeatTrack {
    /// [`Dispatcher::note_stalled`] since the last renewal.
    stalled: bool,
    /// Set by an attempt the store could not answer; cleared by a renewal
    /// or a refusal.
    outage: Option<StoreOutage>,
    /// The last renewal (or the registration), on the monotonic clock.
    renewed: Option<std::time::Instant>,
}

#[derive(Debug, Clone, Copy)]
struct StoreOutage {
    since: Timestamp,
    failures: u32,
}

impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatcher")
            .field("id", &self.id)
            .field("instance", &self.instance)
            .field("fenced", &self.is_fenced())
            .finish_non_exhaustive()
    }
}

impl Dispatcher {
    /// Mint a new dispatcher id and register it.
    pub fn register(
        slots: Arc<dyn SlotStore>,
        clock: Arc<dyn Clock>,
        ids: &dyn IdGenerator,
        config: DispatcherConfig,
        instance: String,
    ) -> Result<Arc<Self>, AppError> {
        let id = DispatcherId::from_ulid(ids.next_ulid());
        let now = clock.now();
        let record = DispatcherRecord {
            id: id.clone(),
            instance: instance.clone(),
            hostname: hostname(),
            pid: std::process::id(),
            started_at: now,
            heartbeat_at: now,
            lease_expires_at: now + config.lease_ttl(),
            stopped_at: None,
            reclaimed_at: None,
        };
        let dispatcher = Arc::new(Self {
            id,
            instance,
            hostname: record.hostname.clone(),
            pid: record.pid,
            slots,
            clock,
            config,
            fenced: AtomicBool::new(false),
            heartbeats: Mutex::new(HeartbeatTrack {
                renewed: Some(std::time::Instant::now()),
                ..HeartbeatTrack::default()
            }),
        });
        watch_for_stalls();
        dispatcher.slots.register_dispatcher(record)?;
        LIVE_IN_PROCESS.lock().insert(dispatcher.id.to_string());
        tracing::info!(
            dispatcher_id = %dispatcher.id,
            instance = %dispatcher.instance,
            pid = dispatcher.pid,
            lease_ttl_seconds = dispatcher.config.lease_ttl_seconds,
            "dispatcher registered"
        );
        Ok(dispatcher)
    }

    pub fn id(&self) -> &DispatcherId {
        &self.id
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    pub fn config(&self) -> &DispatcherConfig {
        &self.config
    }

    /// Ownership expiry for a lease taken at `now`.
    pub fn lease_expiry(&self, now: Timestamp) -> Timestamp {
        now + self.config.lease_ttl()
    }

    /// True once a heartbeat was refused: the dispatcher lost its lease and
    /// must not take new work.
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }

    /// Record that this dispatcher's process was not running since `at`
    /// (tests: two dispatchers share one process, so the process-wide stall
    /// watchdog cannot tell which one a test meant to freeze).
    #[doc(hidden)]
    pub fn note_stalled(&self) {
        self.heartbeats.lock().stalled = true;
    }

    /// Renew this dispatcher's lease and the slot leases it holds.
    ///
    /// When the ordinary renewal is refused because the lease already passed
    /// — its renewals could not reach the store in time (another process held
    /// the write lock, the connection stayed busy, every async worker was
    /// waiting on the store) — and this process did not stall itself since
    /// its last renewal, the lease is taken back if nobody reclaimed it
    /// ([`SlotStore::renew_after_store_outage`], module docs «Store outage»).
    /// An error is the store's: nothing is renewed and nothing is fenced.
    pub fn heartbeat(&self) -> Result<HeartbeatOutcome, RepoError> {
        // The whole attempt is timed from here: a renewal whose transaction
        // began before a stall (the frozen process's own heartbeat, which
        // commits a stale expiry the moment it continues) must not count as
        // evidence that the process was running afterwards.
        let started = std::time::Instant::now();
        let now = self.clock.now();
        let ttl = self.config.lease_ttl();
        let mut outcome = match self.slots.heartbeat(&self.id, ttl, now) {
            Ok(outcome) => outcome,
            Err(e) => {
                if matches!(e, RepoError::Store(_) | RepoError::Io(_)) {
                    let mut track = self.heartbeats.lock();
                    let outage = track.outage.get_or_insert(StoreOutage {
                        since: now,
                        failures: 0,
                    });
                    outage.failures += 1;
                }
                return Err(e);
            }
        };
        let mut revived = false;
        if outcome == HeartbeatOutcome::Fenced && !self.is_fenced() {
            let kept_running = {
                let track = self.heartbeats.lock();
                !track.stalled && track.renewed.is_some_and(|at| !stalled_since(at))
            };
            if kept_running {
                outcome = self.slots.renew_after_store_outage(&self.id, ttl, now)?;
                revived = matches!(outcome, HeartbeatOutcome::Renewed { .. });
            }
        }
        let outage = {
            let mut track = self.heartbeats.lock();
            if matches!(outcome, HeartbeatOutcome::Renewed { .. }) {
                track.renewed = Some(started);
                track.stalled = track.stalled && stalled_since(started);
            }
            track.outage.take()
        };
        if (revived || outage.is_some())
            && let HeartbeatOutcome::Renewed { leases } = &outcome
        {
            tracing::warn!(
                dispatcher_id = %self.id,
                outage_started_at = ?outage.map(|o| o.since),
                failed_heartbeats = outage.map_or(0, |o| o.failures),
                revived,
                leases,
                "dispatcher lease renewed after the store was unavailable"
            );
        }
        if outcome == HeartbeatOutcome::Fenced && !self.fenced.swap(true, Ordering::SeqCst) {
            tracing::error!(
                dispatcher_id = %self.id,
                "dispatcher lease lost: refusing new invocations; in-flight completions \
                 will be refused if another dispatcher reclaimed them"
            );
        }
        Ok(outcome)
    }

    /// Graceful shutdown: what is left may be reclaimed immediately.
    pub fn stop(&self) {
        if let Err(e) = self.slots.stop_dispatcher(&self.id, self.clock.now()) {
            tracing::warn!(error = %e, dispatcher_id = %self.id, "cannot record the dispatcher stop");
        }
        self.fenced.store(true, Ordering::SeqCst);
    }

    /// Other dispatchers provably gone without waiting for their lease: the
    /// same instance on the same host, whose process no longer exists (or,
    /// for one that lived in this very process, whose handle was dropped).
    pub fn presumed_dead(&self) -> Vec<DispatcherId> {
        let records = match self.slots.list_dispatchers() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "cannot list dispatchers");
                return Vec::new();
            }
        };
        let live_here = LIVE_IN_PROCESS.lock().clone();
        records
            .into_iter()
            .filter(|d| d.id != self.id && d.is_live())
            .filter(|d| {
                !self.hostname.is_empty()
                    && d.hostname == self.hostname
                    && d.instance == self.instance
            })
            .filter(|d| {
                if d.pid == self.pid {
                    !live_here.contains(d.id.as_str())
                } else {
                    !pid_alive(d.pid)
                }
            })
            .map(|d| d.id)
            .collect()
    }

    pub fn reclaim_request(&self) -> ReclaimRequest {
        ReclaimRequest {
            reclaimer: self.id.clone(),
            now: self.clock.now(),
            skew: self.config.max_clock_skew(),
            presumed_dead: self.presumed_dead(),
        }
    }

    /// The ledger half of a reclaim: release, settle and fence. The fenced
    /// environments still need a provider terminate
    /// ([`crate::services::ReconcileService::reclaim`]).
    pub fn reclaim_ledger(&self) -> Result<ReclaimReport, RepoError> {
        let report = self.slots.reclaim_expired(self.reclaim_request())?;
        if !report.is_empty() {
            tracing::warn!(
                dispatcher_id = %self.id,
                reclaimed_dispatchers = ?report.dispatchers,
                leases = report.leases,
                invocations = report.invocations,
                attempts = report.attempts,
                fenced = report.fenced.len(),
                "reclaimed the work of dispatchers that lost their lease"
            );
        }
        Ok(report)
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        LIVE_IN_PROCESS.lock().remove(self.id.as_str());
    }
}
