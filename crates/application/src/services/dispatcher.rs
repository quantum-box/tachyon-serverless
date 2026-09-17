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
//!   all of its unexpired slot leases. A renewal never revives a lease that
//!   already passed; a dispatcher whose heartbeat is refused is *fenced*: it
//!   refuses new invocations (`ProviderUnavailable`), and whatever it still
//!   drives is refused at the store when it completes, because the reclaimer
//!   released the lease and moved the environment's epoch.
//! - **Reclaim.** [`Dispatcher::reclaim_request`] describes what this process
//!   may reclaim: other dispatchers whose lease is past expiry plus the
//!   tolerated clock skew, that stopped, or that are the previous incarnation
//!   of the same instance on the same host whose process is provably gone
//!   (its pid is not alive, or it lived in this very process and its handle
//!   was dropped). A live dispatcher's work is never touched, so two gateways
//!   on one `data_dir` do not settle each other's invocations.

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
        });
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

    /// Renew this dispatcher's lease and the slot leases it holds.
    pub fn heartbeat(&self) -> Result<HeartbeatOutcome, RepoError> {
        let outcome = self
            .slots
            .heartbeat(&self.id, self.config.lease_ttl(), self.clock.now())?;
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
