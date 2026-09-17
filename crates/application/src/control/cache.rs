//! The data plane's configuration cache (PLT-4636,
//! docs/adr/0007-config-distribution-and-auth-leases.md).
//!
//! Invoke reads functions, routes, revisions, authorization grants and policy
//! **only** from here. Rules:
//!
//! - **Validity.** Every entry has a `valid_until`: the start of the refresh
//!   that last confirmed it plus the config TTL (functions, routes, revisions,
//!   policy) or the auth lease (grants, tenants), each the smaller of this
//!   gateway's setting and the control plane's. A successful refresh confirms
//!   *every* entry, because a delivery asserts that what it does not mention
//!   is unchanged as of its generation. An entry is valid while
//!   `now < valid_until`; at `valid_until` it is expired.
//! - **Generations.** An entry is replaced only by a strictly higher
//!   generation. An older or equal one is ignored, whatever order deliveries
//!   arrive in, so an old generation can never roll a newer one back. A
//!   delivery from a source whose generation is *below* the cache's (a
//!   restored backup, a replayed response) is ignored as a whole and does not
//!   confirm anything: a regressed source must not keep revoked grants alive.
//! - **States.** `unknown` (never delivered), `fresh` (confirmed within
//!   `stale_after`), `stale_but_valid`, `expired`. Tombstones (removed keys)
//!   answer like unknown ones.
//! - **Refresh.** One refresh at a time. While the control plane answers, the
//!   next one is `refresh_interval` later; after a failure the delay doubles
//!   from `backoff_initial` to `backoff_max`. An authoritative (in-process)
//!   source is also refreshed on the request path when its change marker
//!   moved or the cache is about to expire, so a combined gateway sees its own
//!   writes at once.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::Serialize;

use tachyon_serverless_domain::{
    AliasName, Clock, Function, FunctionId, FunctionRevision, RevisionId, TenantId, Timestamp,
};
use tachyon_serverless_provider_port::{Credential, Principal};

use super::ControlError;
use super::source::{ConfigSource, SourceError};
use super::wire::{ConfigDelivery, ConfigKey, ConfigValue, grant_key};
use crate::config::ControlPlaneConfig;
use crate::error::AppError;
use crate::services::revision::ensure_ready;

/// Timing of the cache.
#[derive(Debug, Clone)]
pub struct CacheSettings {
    pub refresh_interval: Duration,
    pub config_ttl: chrono::Duration,
    pub auth_lease: chrono::Duration,
    pub stale_after: chrono::Duration,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
}

impl CacheSettings {
    pub fn from_config(c: &ControlPlaneConfig) -> Self {
        Self {
            refresh_interval: c.refresh_interval(),
            config_ttl: c.config_ttl(),
            auth_lease: c.auth_lease(),
            stale_after: c.stale_after(),
            backoff_initial: c.backoff_initial(),
            backoff_max: c.backoff_max(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    Fresh,
    StaleButValid,
    Expired,
    Unknown,
}

impl EntryState {
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Fresh | Self::StaleButValid)
    }
}

/// The cache as a whole: `unknown` until the first delivery, `expired` once
/// the config TTL or the auth lease passed without a confirmation.
pub type CacheState = EntryState;

#[derive(Debug, Clone)]
struct CachedEntry {
    generation: u64,
    value: Option<ConfigValue>,
    valid_until: Timestamp,
    confirmed_at: Timestamp,
}

/// Synchronisation status, as `/readyz` shows it.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CacheStatus {
    pub source: String,
    /// Highest generation applied.
    pub generation: u64,
    pub ever_synced: bool,
    pub last_attempt_at: Option<Timestamp>,
    pub last_success_at: Option<Timestamp>,
    /// Failed refreshes since the last success. `> 0` means the control plane
    /// is currently unreachable (an outage).
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    /// Until when the delivered functions, routes, revisions and policy are
    /// valid without another confirmation.
    pub config_valid_until: Option<Timestamp>,
    /// Until when the delivered authorization grants are valid (auth lease).
    pub auth_valid_until: Option<Timestamp>,
    /// Refreshes that succeeded after at least one failure.
    pub reconnects: u64,
    /// Entries ignored because the cache already had that or a newer
    /// generation.
    pub ignored_older_entries: u64,
    /// Whole deliveries ignored because their source was behind the cache.
    pub ignored_regressed_deliveries: u64,
    pub entries: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// Entries inserted or replaced.
    pub applied: usize,
    /// Entries with a generation the cache already had or exceeded.
    pub ignored_older: usize,
    /// Entries newer than the delivery claims to be (inconsistent source).
    pub ignored_inconsistent: usize,
    /// The whole delivery was ignored: its source is behind the cache, or it
    /// answers a `since` the cache is not at.
    pub regressed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefreshReport {
    pub apply: ApplyReport,
    /// This refresh ended an outage (the previous one failed).
    pub reconnected: bool,
    pub generation: u64,
}

/// A tenant's delivered budget as the cache holds it (PLT-4643).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetEntry {
    /// Never delivered, or removed (a tombstone).
    NotDelivered,
    /// Delivered but not confirmed within the auth lease.
    Expired,
    Valid {
        budget: crate::budget::TenantBudget,
        generation: u64,
    },
}

/// What an invocation resolves to.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub function: Function,
    pub revision: FunctionRevision,
    pub alias: Option<AliasName>,
    /// Generation of the route that chose the revision (PLT-4635). `None`
    /// for a pinned revision.
    pub alias_generation: Option<u64>,
}

/// What the scale reconciler reads from a *valid* configuration
/// (PLT-4635).
#[derive(Debug, Clone, Default)]
pub struct ScaleView {
    /// Revisions an alias of a live function points at.
    pub routed: std::collections::HashMap<RevisionId, (Function, FunctionRevision)>,
    /// Deleted functions (deleting or drained) and the revisions delivered
    /// for them.
    pub deleted: Vec<(Function, Vec<RevisionId>)>,
}

#[derive(Default)]
struct Inner {
    entries: BTreeMap<ConfigKey, CachedEntry>,
    status: CacheStatus,
    marker: Option<(u64, u64)>,
}

enum Lookup<'a> {
    Unknown,
    Expired,
    Valid(&'a ConfigValue),
}

pub struct ConfigCache {
    source: Arc<dyn ConfigSource>,
    clock: Arc<dyn Clock>,
    settings: CacheSettings,
    grant_secret: Vec<u8>,
    inner: RwLock<Inner>,
    refreshing: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for ConfigCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigCache")
            .field("source", &self.source.describe())
            .field("generation", &self.inner.read().status.generation)
            .finish_non_exhaustive()
    }
}

fn secs(n: u64) -> Option<chrono::Duration> {
    (n > 0).then(|| chrono::Duration::seconds(n.min(i64::MAX as u64 / 1000) as i64))
}

impl ConfigCache {
    pub fn new(
        source: Arc<dyn ConfigSource>,
        clock: Arc<dyn Clock>,
        settings: CacheSettings,
        grant_secret: Vec<u8>,
    ) -> Self {
        let inner = Inner {
            status: CacheStatus {
                source: source.describe(),
                ..CacheStatus::default()
            },
            ..Inner::default()
        };
        Self {
            source,
            clock,
            settings,
            grant_secret,
            inner: RwLock::new(inner),
            refreshing: tokio::sync::Mutex::new(()),
        }
    }

    pub fn settings(&self) -> &CacheSettings {
        &self.settings
    }

    pub fn now(&self) -> Timestamp {
        self.clock.now()
    }

    pub fn authoritative(&self) -> bool {
        self.source.authoritative()
    }

    pub fn status(&self) -> CacheStatus {
        let s = self.inner.read();
        let mut status = s.status.clone();
        status.entries = s.entries.len();
        status
    }

    /// The control plane is unreachable: the last refresh failed, or none
    /// ever succeeded.
    pub fn outage(&self) -> bool {
        let s = self.inner.read();
        s.status.consecutive_failures > 0 || !s.status.ever_synced
    }

    /// Delay before the next background refresh.
    pub fn next_delay(&self) -> Duration {
        let failures = self.inner.read().status.consecutive_failures;
        backoff_delay(&self.settings, failures)
    }

    // -- refresh -------------------------------------------------------------

    /// Pull from the source and apply. Errors are recorded in
    /// [`ConfigCache::status`] and returned; cached entries are untouched by
    /// a failure and simply age towards their `valid_until`.
    pub async fn refresh(&self) -> Result<RefreshReport, SourceError> {
        let _one = self.refreshing.lock().await;
        self.refresh_locked().await
    }

    async fn refresh_locked(&self) -> Result<RefreshReport, SourceError> {
        let marker = self.source.change_marker();
        // Validity counts from *before* the request: a slow answer never
        // extends a lease beyond what the control plane could vouch for.
        let started = self.clock.now();
        let (since, was_failing) = {
            let s = self.inner.read();
            (s.status.generation, s.status.consecutive_failures > 0)
        };
        let delivery = self.source.fetch(since).await;
        let delivery = match delivery {
            Ok(d) => d,
            Err(e) => {
                self.record_failure(started, e.to_string());
                return Err(e);
            }
        };
        let apply = self.apply(delivery, started);
        if apply.regressed {
            let msg = "the control plane answered with an older generation than this cache holds; \
                       nothing was applied or renewed";
            self.record_failure(started, msg.to_string());
            return Err(SourceError::Rejected(msg.to_string()));
        }
        let mut s = self.inner.write();
        s.marker = marker;
        if was_failing {
            s.status.reconnects += 1;
        }
        Ok(RefreshReport {
            apply,
            reconnected: was_failing,
            generation: s.status.generation,
        })
    }

    fn record_failure(&self, at: Timestamp, error: String) {
        let mut s = self.inner.write();
        s.status.last_attempt_at = Some(at);
        s.status.consecutive_failures = s.status.consecutive_failures.saturating_add(1);
        s.status.last_error = Some(error);
    }

    /// Apply one delivery fetched at `fetched_at` (the moment the request was
    /// sent). Public so tests can deliver out of order.
    pub fn apply(&self, delivery: ConfigDelivery, fetched_at: Timestamp) -> ApplyReport {
        let mut report = ApplyReport::default();
        let mut s = self.inner.write();
        if delivery.generation < s.status.generation || delivery.since > s.status.generation {
            s.status.ignored_regressed_deliveries += 1;
            report.regressed = true;
            tracing::warn!(
                source = %delivery.source,
                delivery_generation = delivery.generation,
                delivery_since = delivery.since,
                cache_generation = s.status.generation,
                "configuration delivery ignored: the source is behind this cache"
            );
            return report;
        }
        let config_ttl = match secs(delivery.config_ttl_seconds) {
            Some(cap) => cap.min(self.settings.config_ttl),
            None => self.settings.config_ttl,
        };
        let auth_ttl = match secs(delivery.auth_lease_seconds) {
            Some(cap) => cap.min(self.settings.auth_lease),
            None => self.settings.auth_lease,
        };
        for e in delivery.entries {
            if e.generation > delivery.generation {
                report.ignored_inconsistent += 1;
                continue;
            }
            if let Some(existing) = s.entries.get(&e.key)
                && existing.generation >= e.generation
            {
                report.ignored_older += 1;
                continue;
            }
            report.applied += 1;
            s.entries.insert(
                e.key,
                CachedEntry {
                    generation: e.generation,
                    value: e.value,
                    valid_until: fetched_at,
                    confirmed_at: fetched_at,
                },
            );
        }
        s.status.ignored_older_entries += report.ignored_older as u64;
        s.status.generation = s.status.generation.max(delivery.generation);
        let config_until = fetched_at + config_ttl;
        let auth_until = fetched_at + auth_ttl;
        for (key, entry) in s.entries.iter_mut() {
            let until = if key.is_authorization() {
                auth_until
            } else {
                config_until
            };
            entry.valid_until = entry.valid_until.max(until);
            entry.confirmed_at = entry.confirmed_at.max(fetched_at);
        }
        let st = &mut s.status;
        st.ever_synced = true;
        st.last_attempt_at = Some(fetched_at);
        st.last_success_at = Some(st.last_success_at.map_or(fetched_at, |t| t.max(fetched_at)));
        st.consecutive_failures = 0;
        st.last_error = None;
        st.config_valid_until = Some(
            st.config_valid_until
                .map_or(config_until, |t| t.max(config_until)),
        );
        st.auth_valid_until = Some(
            st.auth_valid_until
                .map_or(auth_until, |t| t.max(auth_until)),
        );
        report
    }

    /// Refresh an authoritative source on the request path when its content
    /// may have changed or the cache is about to expire. A no-op for a remote
    /// source: the request path never waits for the control plane.
    pub async fn sync_if_authoritative(&self) {
        if !self.source.authoritative() || !self.needs_sync() {
            return;
        }
        let _one = self.refreshing.lock().await;
        // Another request may have refreshed while this one waited.
        if self.needs_sync()
            && let Err(e) = self.refresh_locked().await
        {
            tracing::warn!(error = %e, "in-process configuration refresh failed");
        }
    }

    fn needs_sync(&self) -> bool {
        let horizon = self.clock.now()
            + chrono::Duration::from_std(self.settings.refresh_interval)
                .unwrap_or(chrono::Duration::zero());
        let marker = self.source.change_marker();
        let s = self.inner.read();
        !s.status.ever_synced
            || s.marker != marker
            || s.status.config_valid_until.is_none_or(|t| t <= horizon)
            || s.status.auth_valid_until.is_none_or(|t| t <= horizon)
    }

    // -- lookups ---------------------------------------------------------------

    fn lookup<'a>(inner: &'a Inner, key: &ConfigKey, now: Timestamp) -> Lookup<'a> {
        match inner.entries.get(key) {
            None => Lookup::Unknown,
            Some(e) => match &e.value {
                None => Lookup::Unknown,
                Some(_) if now >= e.valid_until => Lookup::Expired,
                Some(v) => Lookup::Valid(v),
            },
        }
    }

    /// State, generation and value of one key.
    pub fn entry(&self, key: &ConfigKey) -> (EntryState, Option<u64>, Option<ConfigValue>) {
        let now = self.clock.now();
        let s = self.inner.read();
        match s.entries.get(key) {
            None => (EntryState::Unknown, None, None),
            Some(e) => (self.state_of(e, now), Some(e.generation), e.value.clone()),
        }
    }

    fn state_of(&self, e: &CachedEntry, now: Timestamp) -> EntryState {
        if now >= e.valid_until {
            EntryState::Expired
        } else if now - e.confirmed_at > self.settings.stale_after {
            EntryState::StaleButValid
        } else {
            EntryState::Fresh
        }
    }

    /// The cache as a whole (see [`CacheState`]).
    pub fn state(&self) -> CacheState {
        let now = self.clock.now();
        let s = self.inner.read();
        let st = &s.status;
        if !st.ever_synced {
            return EntryState::Unknown;
        }
        let until = match (st.config_valid_until, st.auth_valid_until) {
            (Some(a), Some(b)) => a.min(b),
            _ => return EntryState::Unknown,
        };
        if now >= until {
            EntryState::Expired
        } else if st
            .last_success_at
            .is_some_and(|t| now - t > self.settings.stale_after)
        {
            EntryState::StaleButValid
        } else {
            EntryState::Fresh
        }
    }

    fn not_delivered(what: impl std::fmt::Display) -> AppError {
        AppError::control(
            ControlError::ConfigNotDelivered,
            format!("{what} has not been delivered to this gateway yet"),
        )
    }

    fn expired(what: impl std::fmt::Display) -> AppError {
        AppError::control(
            ControlError::ConfigExpired,
            format!(
                "{what} was not confirmed by the control plane within its TTL; \
                 new invocations are refused until it is"
            ),
        )
    }

    /// Bearer token -> principal, from the delivered grants.
    pub async fn authenticate(&self, credential: &Credential) -> Result<Principal, AppError> {
        self.sync_if_authoritative().await;
        let now = self.clock.now();
        let key = ConfigKey::Grant {
            token_digest: grant_key(&self.grant_secret, &credential.0),
        };
        let s = self.inner.read();
        if !s.status.ever_synced {
            return Err(Self::not_delivered("authorization"));
        }
        let grant = match Self::lookup(&s, &key, now) {
            Lookup::Unknown => return Err(AppError::Unauthorized("unknown credential".into())),
            Lookup::Expired => {
                return Err(AppError::control(
                    ControlError::AuthLeaseExpired,
                    "the authorization lease of this credential expired: the control plane has \
                     not confirmed it within auth_lease_seconds",
                ));
            }
            Lookup::Valid(ConfigValue::Grant(g)) => g.clone(),
            Lookup::Valid(_) => return Err(AppError::Unauthorized("unknown credential".into())),
        };
        let tenant = ConfigKey::Tenant {
            tenant_id: grant.tenant_id.clone(),
        };
        match Self::lookup(&s, &tenant, now) {
            Lookup::Valid(ConfigValue::Tenant(_)) => {}
            Lookup::Expired => {
                return Err(AppError::control(
                    ControlError::AuthLeaseExpired,
                    "the authorization lease of this tenant expired",
                ));
            }
            _ => {
                return Err(AppError::control(
                    ControlError::UnknownTenant,
                    "the tenant of this credential is not known to this gateway",
                ));
            }
        }
        Ok(Principal {
            subject: grant.subject,
            tenant_id: grant.tenant_id,
            roles: grant.roles,
        })
    }

    /// A trigger fire has no bearer token (PLT-4641): its authority is the
    /// tenant's, and the tenant must still be delivered and inside its auth
    /// lease, exactly like the tenant check of [`Self::authenticate`]. A
    /// tenant removed from the grants stops its triggers within one refresh
    /// or one auth lease.
    pub async fn authorize_tenant(&self, tenant_id: &TenantId) -> Result<(), AppError> {
        self.sync_if_authoritative().await;
        let now = self.clock.now();
        let s = self.inner.read();
        if !s.status.ever_synced {
            return Err(Self::not_delivered("authorization"));
        }
        let key = ConfigKey::Tenant {
            tenant_id: tenant_id.clone(),
        };
        match Self::lookup(&s, &key, now) {
            Lookup::Valid(ConfigValue::Tenant(_)) => Ok(()),
            Lookup::Expired => Err(AppError::control(
                ControlError::AuthLeaseExpired,
                "the authorization lease of this tenant expired",
            )),
            _ => Err(AppError::control(
                ControlError::UnknownTenant,
                "the tenant of this trigger is not known to this gateway",
            )),
        }
    }

    /// Function, alias route, revision and policy for a new invocation.
    /// Refuses with `NotFound` / `FunctionDeleted` / `RevisionNotReady`
    /// exactly like the ledger did, and with a [`ControlError`] when the
    /// cache cannot vouch for the answer.
    pub async fn resolve(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        alias: Option<&AliasName>,
        revision_id: Option<&RevisionId>,
    ) -> Result<Resolved, AppError> {
        self.sync_if_authoritative().await;
        let now = self.clock.now();
        let authoritative = self.source.authoritative();
        let s = self.inner.read();
        if !s.status.ever_synced {
            return Err(Self::not_delivered("configuration"));
        }
        let function = match Self::lookup(
            &s,
            &ConfigKey::Function {
                function_id: function_id.clone(),
            },
            now,
        ) {
            // A data plane cannot tell "does not exist" from "not delivered
            // yet" for a function; both answer 404 so existence never leaks
            // across tenants (docs/api.md: propagation delay).
            Lookup::Unknown => return Err(AppError::not_found("function not found")),
            Lookup::Expired => return Err(Self::expired(format!("function {function_id}"))),
            Lookup::Valid(ConfigValue::Function(f)) => f.clone(),
            Lookup::Valid(_) => return Err(AppError::not_found("function not found")),
        };
        crate::authz::ensure_tenant(principal, &function.tenant_id, "function")?;
        if function.is_deleted() {
            return Err(AppError::FunctionDeleted(format!(
                "function {} is deleted",
                function.id
            )));
        }
        let (target, alias, alias_generation) = match revision_id {
            Some(pinned) => (pinned.clone(), None, None),
            None => {
                let name = alias.cloned().unwrap_or_else(AliasName::default_alias);
                let key = ConfigKey::Route {
                    function_id: function.id.clone(),
                    alias: name.clone(),
                };
                match Self::lookup(&s, &key, now) {
                    Lookup::Unknown => {
                        return Err(AppError::not_found(format!("alias `{name}` not found")));
                    }
                    Lookup::Expired => {
                        return Err(Self::expired(format!("alias `{name}` of {}", function.id)));
                    }
                    Lookup::Valid(ConfigValue::Route(a)) => {
                        (a.revision_id.clone(), Some(name), Some(a.generation))
                    }
                    Lookup::Valid(_) => {
                        return Err(AppError::not_found(format!("alias `{name}` not found")));
                    }
                }
            }
        };
        let revision = match Self::lookup(
            &s,
            &ConfigKey::Revision {
                revision_id: target.clone(),
            },
            now,
        ) {
            Lookup::Unknown if authoritative => {
                return Err(AppError::not_found("revision not found"));
            }
            Lookup::Unknown => return Err(Self::not_delivered(format!("revision {target}"))),
            Lookup::Expired => return Err(Self::expired(format!("revision {target}"))),
            Lookup::Valid(ConfigValue::Revision(r))
                if r.function_id == function.id && r.tenant_id == function.tenant_id =>
            {
                (**r).clone()
            }
            Lookup::Valid(_) => return Err(AppError::not_found("revision not found")),
        };
        ensure_ready(&revision)?;
        match Self::lookup(&s, &ConfigKey::Policy, now) {
            Lookup::Unknown => return Err(Self::not_delivered("the invoke policy")),
            Lookup::Expired => return Err(Self::expired("the invoke policy")),
            Lookup::Valid(ConfigValue::Policy(p)) => {
                if !p.allowed_egress.contains(&revision.spec.egress) {
                    return Err(AppError::control(
                        ControlError::PolicyDenied,
                        format!(
                            "egress profile `{}` of revision {} is not allowed by the policy",
                            revision.spec.egress.as_str(),
                            revision.id
                        ),
                    ));
                }
            }
            Lookup::Valid(_) => return Err(Self::not_delivered("the invoke policy")),
        }
        Ok(Resolved {
            function,
            revision,
            alias,
            alias_generation,
        })
    }

    /// The routes, revisions and deleted functions the scale reconciler
    /// works from (PLT-4635). `None` unless the cache as a whole is valid:
    /// an outage or an expired cache must never read as "every route went
    /// away", which would drain everything and flap back on reconnect.
    pub fn scale_view(&self) -> Option<ScaleView> {
        if !self.state().is_valid() {
            return None;
        }
        let now = self.clock.now();
        let s = self.inner.read();
        let valid = |e: &CachedEntry| now < e.valid_until;
        let mut functions: std::collections::HashMap<FunctionId, Function> =
            std::collections::HashMap::new();
        let mut revisions: std::collections::HashMap<RevisionId, FunctionRevision> =
            std::collections::HashMap::new();
        for entry in s.entries.values().filter(|e| valid(e)) {
            match &entry.value {
                Some(ConfigValue::Function(f)) => {
                    functions.insert(f.id.clone(), f.clone());
                }
                Some(ConfigValue::Revision(r)) => {
                    revisions.insert(r.id.clone(), (**r).clone());
                }
                _ => {}
            }
        }
        let mut view = ScaleView::default();
        for entry in s.entries.values().filter(|e| valid(e)) {
            if let Some(ConfigValue::Route(a)) = &entry.value
                && let Some(f) = functions.get(&a.function_id)
                && !f.is_deleted()
                && let Some(r) = revisions.get(&a.revision_id)
                && r.function_id == f.id
            {
                view.routed.insert(r.id.clone(), (f.clone(), r.clone()));
            }
        }
        for f in functions.values().filter(|f| f.is_deleted()) {
            let revs = revisions
                .values()
                .filter(|r| r.function_id == f.id)
                .map(|r| r.id.clone())
                .collect();
            view.deleted.push((f.clone(), revs));
        }
        Some(view)
    }

    /// Whether the cache says `function` is deleted now. Checked right
    /// before an accepted invocation is dispatched (PLT-4635): a deletion
    /// that landed while it queued refuses it instead of starting it. An
    /// unknown or expired entry is not a deletion.
    pub async fn function_deleted(&self, function: &FunctionId) -> bool {
        self.sync_if_authoritative().await;
        let now = self.clock.now();
        let s = self.inner.read();
        matches!(
            Self::lookup(
                &s,
                &ConfigKey::Function {
                    function_id: function.clone(),
                },
                now,
            ),
            Lookup::Valid(ConfigValue::Function(f)) if f.is_deleted()
        )
    }

    /// The delivered budget of `tenant` (PLT-4643), with its generation.
    /// Never refreshed on the request path beyond what `authenticate` /
    /// `resolve` already did.
    pub fn budget(&self, tenant: &TenantId) -> BudgetEntry {
        let now = self.clock.now();
        let s = self.inner.read();
        if !s.status.ever_synced {
            return BudgetEntry::NotDelivered;
        }
        let key = ConfigKey::Budget {
            tenant_id: tenant.clone(),
        };
        let generation = s.entries.get(&key).map(|e| e.generation);
        match Self::lookup(&s, &key, now) {
            Lookup::Unknown => BudgetEntry::NotDelivered,
            Lookup::Expired => BudgetEntry::Expired,
            Lookup::Valid(ConfigValue::Budget(b)) => BudgetEntry::Valid {
                budget: b.clone(),
                generation: generation.unwrap_or(0),
            },
            Lookup::Valid(_) => BudgetEntry::NotDelivered,
        }
    }

    /// Whether `revision` and the authorization of `tenant` are still valid
    /// now (a cold start may begin long after acceptance, after queueing).
    pub fn start_still_valid(
        &self,
        revision: &RevisionId,
        tenant: &TenantId,
    ) -> Result<(), ControlError> {
        let now = self.clock.now();
        let s = self.inner.read();
        match Self::lookup(
            &s,
            &ConfigKey::Revision {
                revision_id: revision.clone(),
            },
            now,
        ) {
            Lookup::Valid(_) => {}
            Lookup::Expired => return Err(ControlError::ConfigExpired),
            Lookup::Unknown => return Err(ControlError::ConfigNotDelivered),
        }
        match Self::lookup(
            &s,
            &ConfigKey::Tenant {
                tenant_id: tenant.clone(),
            },
            now,
        ) {
            Lookup::Valid(_) => Ok(()),
            Lookup::Expired => Err(ControlError::AuthLeaseExpired),
            Lookup::Unknown => Err(ControlError::UnknownTenant),
        }
    }
}

/// `refresh_interval` while healthy; after `failures` consecutive failures
/// `backoff_initial * 2^(failures-1)`, capped at `backoff_max`.
pub fn backoff_delay(settings: &CacheSettings, failures: u32) -> Duration {
    if failures == 0 {
        return settings.refresh_interval;
    }
    let factor = 1u32 << (failures - 1).min(20);
    settings
        .backoff_initial
        .saturating_mul(factor)
        .min(settings.backoff_max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_backoff_doubles_up_to_the_cap_and_resets_on_success() {
        let settings = CacheSettings::from_config(&ControlPlaneConfig {
            refresh_interval_ms: 2_000,
            backoff_initial_ms: 500,
            backoff_max_ms: 3_000,
            ..ControlPlaneConfig::default()
        });
        let delays: Vec<u64> = (0..6)
            .map(|f| backoff_delay(&settings, f).as_millis() as u64)
            .collect();
        assert_eq!(delays, vec![2_000, 500, 1_000, 2_000, 3_000, 3_000]);
        assert_eq!(
            backoff_delay(&settings, u32::MAX),
            Duration::from_millis(3_000)
        );
    }
}
