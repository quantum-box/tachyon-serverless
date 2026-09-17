//! The repository contract, run against both stores (docs/adr/0003 A9,
//! PLT-4618): creation, restore, state transitions, terminal rows that
//! cannot be rewritten, alias CAS races, tenant boundaries, duplicate ids,
//! revision tampering, bounded bodies, the environment pool and the slot
//! store (acquire CAS, fenced completion, lease renewal and expiry with clock
//! skew, reclaim and fencing, idempotency expiry; PLT-4631).
//!
//! Every test is a function over `&Arc<dyn StateStore>`; `contract!` runs it
//! once on [`InMemoryStore`], once on a volatile [`SqliteStore`] and, when
//! `TSLS_TIDB_URL` is set, once on a real TiDB (`tidb::TidbStore`, a database
//! per test; skipped with a message otherwise).

use std::sync::Arc;

use chrono::Duration;
use tachyon_serverless_domain::{
    AttemptStatus, BootEvidence, DispatcherId, EnvironmentState, ErrorClass, ExecutionEnvironment,
    ExecutionLease, FunctionAlias, FunctionRevision, Invocation, InvocationAttempt,
    InvocationError, InvocationId, InvocationStatus, LeaseId, Limits, LogPhase, LogRecord,
    LogStream, PayloadRef, ReuseKey, RevisionId, RevisionStatus, Sha256Digest, StartKind, TenantId,
    Timestamp,
};

use super::*;

pub(crate) mod fx {
    //! Fixtures shared with the SQLite-only tests.
    use chrono::TimeZone;
    use tachyon_serverless_domain::*;

    pub fn now() -> Timestamp {
        chrono::Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap()
    }

    pub fn function(tenant: &TenantId, name: &str) -> Function {
        Function::new(
            FunctionId::generate(),
            tenant.clone(),
            FunctionName::parse(name).unwrap(),
            String::new(),
            now(),
        )
        .unwrap()
    }

    pub fn spec() -> RevisionSpec {
        RevisionSpec {
            artifact: ArtifactRef::Binary {
                digest: Sha256Digest::of_bytes(b"bin"),
                size_bytes: 3,
            },
            runtime: RuntimeSpec {
                protocol: RUNTIME_PROTOCOL_V1.into(),
                architecture: Architecture::Aarch64,
            },
            resources: ResourceProfile::default(),
            execution: ExecutionPolicy::default(),
            egress: EgressProfile::None,
            egress_allow: Vec::new(),
            env_vars: vec![],
            secrets: vec![SecretBinding {
                env_name: "DATABASE_URL".into(),
                binding_ref: "billing-db".into(),
            }],
            description: String::new(),
            placement: Default::default(),
            restore: Default::default(),
        }
    }

    pub fn revision(f: &Function, number: u64) -> FunctionRevision {
        FunctionRevision::new(
            RevisionId::generate(),
            f.id.clone(),
            f.tenant_id.clone(),
            number,
            spec(),
            &Limits::default(),
            now(),
        )
        .unwrap()
    }

    pub fn ready_revision(f: &Function, number: u64) -> FunctionRevision {
        let mut r = revision(f, number);
        r.start_preparing(now()).unwrap();
        r.start_validating(now()).unwrap();
        r.mark_ready(now()).unwrap();
        r
    }

    pub fn invocation(t: &TenantId, f: &FunctionId, key: Option<&str>) -> Invocation {
        Invocation::accept(
            InvocationId::generate(),
            t.clone(),
            f.clone(),
            None,
            RevisionId::generate(),
            InvocationMode::Sync,
            EventKind::Json,
            Deadlines {
                queue_deadline: now(),
                init_deadline: None,
                execution_deadline: None,
                client_deadline: now(),
            },
            key.map(str::to_string),
            Sha256Digest::of_bytes(b"{}"),
            2,
            "trace".into(),
            now(),
        )
        .unwrap()
    }

    pub fn attempt(inv: &Invocation, env: &EnvironmentId) -> InvocationAttempt {
        InvocationAttempt::dispatch(
            AttemptId::generate(),
            inv.id.clone(),
            inv.tenant_id.clone(),
            1,
            env.clone(),
            1,
            StartKind::Cold,
            now(),
        )
    }

    pub fn key(t: &TenantId, r: &RevisionId) -> ReuseKey {
        ReuseKey {
            tenant_id: t.clone(),
            revision_id: r.clone(),
            execution_role_version: 1,
            configuration_version: 7,
            resource_profile_digest: "rp".into(),
            runtime_profile: "tachyon.runtime.v1".into(),
            network_policy_version: 3,
            // A truncated digest: exercises the full u64 range.
            secret_binding_generation: u64::MAX - 11,
        }
    }

    pub fn environment(key: &ReuseKey) -> ExecutionEnvironment {
        ExecutionEnvironment::request(
            EnvironmentId::generate(),
            key.tenant_id.clone(),
            key.revision_id.clone(),
            ProviderKind::Fake,
            key.clone(),
            now(),
        )
    }

    pub fn ready_environment(key: &ReuseKey) -> ExecutionEnvironment {
        let mut env = environment(key);
        env.mark_provisioning(now()).unwrap();
        env.mark_initializing(BootEvidence::default(), now())
            .unwrap();
        env.mark_ready(now()).unwrap();
        env
    }

    /// A registered dispatcher whose lease expires `ttl_s` seconds after
    /// `now()`.
    pub fn dispatcher(s: &dyn super::SlotStore, instance: &str, ttl_s: i64) -> DispatcherId {
        let id = DispatcherId::generate();
        s.register_dispatcher(super::DispatcherRecord {
            id: id.clone(),
            instance: instance.into(),
            hostname: "contract-host".into(),
            pid: u32::MAX,
            started_at: now(),
            heartbeat_at: now(),
            lease_expires_at: now() + chrono::Duration::seconds(ttl_s),
            stopped_at: None,
            reclaimed_at: None,
        })
        .unwrap();
        id
    }

    pub fn inline(bytes: &[u8]) -> PayloadRef {
        use base64::Engine as _;
        PayloadRef::Inline {
            bytes_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
            size_bytes: bytes.len() as u64,
        }
    }
}

use fx::now;

type Store = Arc<dyn StateStore>;

fn memory(limits: Limits) -> Store {
    Arc::new(InMemoryStore::new(limits))
}

fn sqlite(limits: Limits) -> Store {
    Arc::new(SqliteStore::open_volatile(limits, SqliteOptions::default()).unwrap())
}

/// A migrated database of its own on the TiDB named by `TSLS_TIDB_URL`
/// (`scripts/db/tidb-verify.sh`), dropped with the store.
fn tidb(limits: Limits) -> Store {
    let url = super::tidb::test_url().expect("TSLS_TIDB_URL");
    Arc::new(super::tidb::TidbStore::open_ephemeral(&url, limits).unwrap())
}

/// True (after saying so) when the TiDB instantiation must be skipped.
pub(crate) fn skip_without_tidb(test: &str) -> bool {
    if super::tidb::test_url().is_some() {
        return false;
    }
    eprintln!("skipped {test}: TSLS_TIDB_URL is not set (scripts/db/tidb-verify.sh sets it)");
    true
}

macro_rules! contract {
    ($($test:ident),* $(,)?) => {
        mod memory {
            $( #[test] fn $test() { super::$test(super::memory, ) } )*
        }
        mod sqlite {
            $( #[test] fn $test() { super::$test(super::sqlite, ) } )*
        }
        mod tidb {
            $( #[test] fn $test() {
                if super::skip_without_tidb(stringify!($test)) { return; }
                super::$test(super::tidb, )
            } )*
        }
    };
}

contract!(
    function_name_unique_per_tenant,
    duplicate_ids_are_refused_for_every_entity,
    revision_numbers_are_allocated_and_unique_per_function,
    a_revision_cannot_be_tampered_with_and_its_final_status_is_final,
    an_invocation_is_created_restored_and_driven_to_a_final_state,
    terminal_rows_are_never_rewritten_property,
    attempts_and_leases_are_final_once_settled,
    an_environment_copy_from_another_epoch_is_refused,
    rows_never_cross_a_tenant,
    alias_updates_are_compare_and_set,
    concurrent_alias_updates_have_exactly_one_winner,
    inline_output_is_bounded,
    idempotency_key_is_bound_with_its_invocation,
    logs_are_bounded_per_invocation,
    artifact_ownership_is_per_tenant,
    acquire_is_a_cas_with_exactly_one_winner_per_epoch,
    a_completion_with_a_stale_epoch_never_overwrites_state,
    leases_renew_only_while_unexpired_and_expire_past_the_clock_skew,
    reclaim_happens_once_fences_and_only_a_confirmed_terminate_settles,
    a_live_dispatcher_is_never_reclaimed_and_a_stopped_one_is_at_once,
    a_fenced_dispatcher_can_neither_renew_nor_acquire,
    idempotency_bindings_expire_after_their_invocation_finished,
    the_pool_only_hands_out_and_sweeps_its_owners_environments,
    only_an_exactly_matching_reuse_key_is_reused,
    nothing_is_dispatched_before_ready_and_busy_is_never_handed_out,
    concurrent_claims_never_hand_the_same_environment_to_two_callers,
    releasing_respects_the_pool_caps_and_refuses_a_stale_copy,
    a_ready_environment_is_not_in_the_pool_and_is_never_claimed,
    a_never_assigned_ready_environment_can_be_pre_started_into_the_pool,
    taking_an_idle_environment_for_termination_excludes_a_claim,
);

fn fns(s: &Store) -> &dyn FunctionRepository {
    &**s
}
fn revs(s: &Store) -> &dyn RevisionRepository {
    &**s
}
fn aliases(s: &Store) -> &dyn AliasRepository {
    &**s
}
fn invs(s: &Store) -> &dyn InvocationRepository {
    &**s
}
fn envs(s: &Store) -> &dyn EnvironmentRepository {
    &**s
}
fn slots(s: &Store) -> &dyn SlotStore {
    &**s
}

/// Everything an acquire of `env` (as stored, `Ready`/`Idle`) by `owner`
/// for `inv` writes, with its lease expiring `ttl_s` after `at`.
fn slot_request(
    env: &ExecutionEnvironment,
    inv: &Invocation,
    owner: &DispatcherId,
    at: Timestamp,
    ttl_s: i64,
) -> SlotAcquire {
    let mut assigned = env.clone();
    assigned.assign(at).unwrap();
    let attempt = InvocationAttempt::dispatch(
        tachyon_serverless_domain::AttemptId::generate(),
        inv.id.clone(),
        inv.tenant_id.clone(),
        (inv.attempt_ids.len() + 1) as u32,
        env.id.clone(),
        assigned.epoch,
        StartKind::Cold,
        at,
    );
    let lease = ExecutionLease::acquire(
        LeaseId::generate(),
        env.id.clone(),
        attempt.id.clone(),
        env.tenant_id.clone(),
        assigned.epoch,
        at + Duration::seconds(600),
        at,
    )
    .owned_by(owner.clone(), at + Duration::seconds(ttl_s));
    let mut running = inv.clone();
    match running.status {
        InvocationStatus::Running => running
            .mark_retry(attempt.id.clone(), at + Duration::seconds(600), at)
            .unwrap(),
        _ => running
            .mark_running(attempt.id.clone(), at + Duration::seconds(600), at, at)
            .unwrap(),
    }
    SlotAcquire {
        env: assigned,
        expected_epoch: env.epoch,
        lease,
        attempt,
        invocation: Some(running),
    }
}

/// A `Ready` environment of `owner` in the store, with an accepted
/// invocation of the same tenant to run on it.
fn ready_slot(s: &Store, owner: &DispatcherId) -> (ExecutionEnvironment, Invocation) {
    let t = TenantId::generate();
    let key = fx::key(&t, &RevisionId::generate());
    let env = fx::ready_environment(&key).owned_by(owner.clone());
    envs(s).insert(env.clone()).unwrap();
    let mut inv = fx::invocation(&t, &FunctionId::generate(), None);
    inv.dispatcher_id = Some(owner.clone());
    invs(s).insert(inv.clone()).unwrap();
    (env, inv)
}

fn is_refused<T: std::fmt::Debug>(r: Result<T, RepoError>) -> bool {
    matches!(r, Err(RepoError::Refused(_)))
}

fn is_conflict<T: std::fmt::Debug>(r: Result<T, RepoError>) -> bool {
    matches!(r, Err(RepoError::Conflict(_)))
}

// ---------------------------------------------------------------------------
// functions and revisions
// ---------------------------------------------------------------------------

fn function_name_unique_per_tenant(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    fns(&s).insert(fx::function(&t, "hello")).unwrap();
    assert!(is_conflict(fns(&s).insert(fx::function(&t, "hello"))));
    let other = TenantId::generate();
    fns(&s).insert(fx::function(&other, "hello")).unwrap();
    assert_eq!(fns(&s).list(&t).unwrap().len(), 1);

    // A deleted function frees its name.
    let mut live = fns(&s)
        .find_by_name(&t, &"hello".parse().unwrap())
        .unwrap()
        .unwrap();
    live.mark_deleted(now()).unwrap();
    fns(&s).update(live.clone()).unwrap();
    assert!(
        fns(&s)
            .find_by_name(&t, &"hello".parse().unwrap())
            .unwrap()
            .is_none()
    );
    fns(&s).insert(fx::function(&t, "hello")).unwrap();
    assert_eq!(fns(&s).list(&t).unwrap().len(), 2);
    // ... and stays deleted.
    let mut undeleted = live;
    undeleted.deleted_at = None;
    assert!(is_refused(fns(&s).update(undeleted)));
}

fn duplicate_ids_are_refused_for_every_entity(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let f = fx::function(&t, "dup");
    fns(&s).insert(f.clone()).unwrap();
    let mut renamed = f.clone();
    renamed.name = "other".parse().unwrap();
    assert!(is_conflict(fns(&s).insert(renamed)), "function id");

    let r = fx::ready_revision(&f, 1);
    revs(&s).insert(r.clone()).unwrap();
    assert!(is_conflict(revs(&s).insert(r.clone())), "revision id");

    let alias = FunctionAlias::new(
        f.id.clone(),
        t.clone(),
        AliasName::default_alias(),
        r.id.clone(),
        now(),
    );
    aliases(&s).insert(alias.clone()).unwrap();
    assert!(is_conflict(aliases(&s).insert(alias)), "alias name");

    let inv = fx::invocation(&t, &f.id, None);
    invs(&s).insert(inv.clone()).unwrap();
    assert!(is_conflict(invs(&s).insert(inv.clone())), "invocation id");
    assert!(
        is_conflict(s.insert_bound(inv.clone())),
        "invocation id via insert_bound"
    );

    let key = fx::key(&t, &r.id);
    let env = fx::environment(&key);
    envs(&s).insert(env.clone()).unwrap();
    assert!(is_conflict(envs(&s).insert(env.clone())), "environment id");

    let att = fx::attempt(&inv, &env.id);
    invs(&s).insert_attempt(att.clone()).unwrap();
    assert!(
        is_conflict(invs(&s).insert_attempt(att.clone())),
        "attempt id"
    );

    // A lease id taken by one acquire cannot be reused by another.
    let owner = fx::dispatcher(slots(&s), "dup", 30);
    let (first_env, first_inv) = ready_slot(&s, &owner);
    let first = slot_request(&first_env, &first_inv, &owner, now(), 30);
    let lease_id = first.lease.id.clone();
    assert_eq!(slots(&s).acquire(first).unwrap(), AcquireOutcome::Acquired);
    let (second_env, second_inv) = ready_slot(&s, &owner);
    let mut second = slot_request(&second_env, &second_inv, &owner, now(), 30);
    second.lease.id = lease_id;
    assert!(is_conflict(slots(&s).acquire(second)), "lease id");
}

fn revision_numbers_are_allocated_and_unique_per_function(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let f = fx::function(&t, "numbers");
    let g = fx::function(&t, "numbers-too");
    fns(&s).insert(f.clone()).unwrap();
    fns(&s).insert(g.clone()).unwrap();
    assert_eq!(revs(&s).allocate_number(&f.id).unwrap(), 1);
    assert_eq!(revs(&s).allocate_number(&f.id).unwrap(), 2);
    assert_eq!(revs(&s).allocate_number(&g.id).unwrap(), 1);

    revs(&s).insert(fx::revision(&f, 1)).unwrap();
    assert!(
        is_conflict(revs(&s).insert(fx::revision(&f, 1))),
        "a second revision 1 of the same function"
    );
    revs(&s).insert(fx::revision(&g, 1)).unwrap();
    revs(&s).insert(fx::revision(&f, 2)).unwrap();
    let numbers: Vec<u64> = revs(&s)
        .list_by_function(&f.id)
        .unwrap()
        .iter()
        .map(|r| r.number)
        .collect();
    assert_eq!(numbers, vec![1, 2]);
}

fn a_revision_cannot_be_tampered_with_and_its_final_status_is_final(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let f = fx::function(&t, "tamper");
    fns(&s).insert(f.clone()).unwrap();
    let mut r = fx::revision(&f, 1);
    revs(&s).insert(r.clone()).unwrap();

    // Every identity field of a revision is immutable.
    let mut spec_changed = r.clone();
    spec_changed
        .spec
        .env_vars
        .push(("INJECTED".into(), "1".into()));
    assert!(is_refused(revs(&s).update(spec_changed)), "spec");
    let mut digest_changed = r.clone();
    digest_changed.spec_digest = Sha256Digest::of_bytes(b"other");
    assert!(is_refused(revs(&s).update(digest_changed)), "spec digest");
    let mut renumbered = r.clone();
    renumbered.number = 9;
    assert!(is_refused(revs(&s).update(renumbered)), "number");
    let mut moved = r.clone();
    moved.function_id = FunctionId::generate();
    assert!(is_refused(revs(&s).update(moved)), "function");
    let mut stolen = r.clone();
    stolen.tenant_id = TenantId::generate();
    assert!(is_refused(revs(&s).update(stolen)), "tenant");

    // The legal path, then nothing more.
    r.start_preparing(now()).unwrap();
    revs(&s).update(r.clone()).unwrap();
    r.start_validating(now()).unwrap();
    revs(&s).update(r.clone()).unwrap();
    let validating = r.clone();
    r.mark_ready(now()).unwrap();
    revs(&s).update(r.clone()).unwrap();
    revs(&s).update(r.clone()).unwrap(); // identical: a no-op
    assert!(
        is_refused(revs(&s).update(validating)),
        "a Ready revision never goes back"
    );
    let mut failed = r.clone();
    failed.status = RevisionStatus::Failed {
        reason: "late".into(),
    };
    assert!(is_refused(revs(&s).update(failed)), "Ready is final");
    let stored = revs(&s).get(&r.id).unwrap().unwrap();
    assert_eq!(stored, r);
    stored.verify_integrity().unwrap();
}

// ---------------------------------------------------------------------------
// invocations, attempts, leases
// ---------------------------------------------------------------------------

fn an_invocation_is_created_restored_and_driven_to_a_final_state(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let f = fx::function(&t, "life");
    fns(&s).insert(f.clone()).unwrap();
    let mut inv = fx::invocation(&t, &f.id, None);
    invs(&s).insert(inv.clone()).unwrap();
    assert_eq!(invs(&s).get(&inv.id).unwrap().unwrap(), inv, "restored");

    inv.mark_queued().unwrap();
    invs(&s).update(inv.clone()).unwrap();
    inv.mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    invs(&s).update(inv.clone()).unwrap();
    let running = inv.clone();
    inv.mark_succeeded(Some(fx::inline(b"{\"ok\":true}")), Some(200), now())
        .unwrap();
    invs(&s).update(inv.clone()).unwrap();
    assert_eq!(invs(&s).get(&inv.id).unwrap().unwrap(), inv);

    // Identical rewrite is a no-op; anything else is refused.
    invs(&s).update(inv.clone()).unwrap();
    assert!(
        is_refused(invs(&s).update(running)),
        "a stale Running copy never overwrites a result"
    );
    let mut failed = inv.clone();
    failed.status = InvocationStatus::Failed {
        error: InvocationError::new(ErrorClass::PlatformError, "X", "late"),
    };
    assert!(is_refused(invs(&s).update(failed)));
    let mut rekeyed = inv.clone();
    rekeyed.idempotency_key = Some("k".into());
    assert!(
        is_refused(invs(&s).update(rekeyed)),
        "identity is immutable"
    );
    assert_eq!(invs(&s).get(&inv.id).unwrap().unwrap(), inv);
    assert!(matches!(
        invs(&s).update(fx::invocation(&t, &f.id, None)),
        Err(RepoError::NotFound(_))
    ));
    assert_eq!(invs(&s).list_by_function(&f.id, 10).unwrap(), vec![inv]);
}

/// Small deterministic generator, so the property test needs no extra crate
/// and every failure reproduces from its seed.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Property: for any interleaving of legal transitions and stale writes, the
/// stored invocation only ever moves forward, and once it is terminal it
/// stays exactly the first terminal value written.
fn terminal_rows_are_never_rewritten_property(make: fn(Limits) -> Store) {
    for seed in 0..64u64 {
        let s = make(Limits::default());
        let mut rng = Lcg(seed.wrapping_mul(0x9e3779b97f4a7c15) | 1);
        let t = TenantId::generate();
        let f = FunctionId::generate();
        let first = fx::invocation(&t, &f, None);
        invs(&s).insert(first.clone()).unwrap();
        let mut snapshots = vec![first.clone()];
        let mut current = first;
        let mut settled: Option<Invocation> = None;

        for step in 0..24 {
            let case = format!("seed={seed} step={step}");
            let mut next = current.clone();
            let changed = match rng.below(7) {
                0 => next.mark_queued().is_ok(),
                1 => next
                    .mark_running(AttemptId::generate(), now(), now(), now())
                    .is_ok(),
                2 => next
                    .mark_succeeded(Some(fx::inline(b"1")), None, now())
                    .is_ok(),
                3 => next
                    .mark_failed(
                        InvocationError::new(ErrorClass::UserError, "E", "boom"),
                        now(),
                    )
                    .is_ok(),
                4 => next.mark_cancelled(now()).is_ok(),
                5 => next.mark_outcome_unknown("lost", now()).is_ok(),
                _ => {
                    // A stale writer replays an older snapshot.
                    next = snapshots[rng.below(snapshots.len() as u64) as usize].clone();
                    true
                }
            };
            if !changed {
                continue;
            }
            let result = invs(&s).update(next.clone());
            let stored = invs(&s).get(&next.id).unwrap().unwrap();
            match &settled {
                Some(final_row) => {
                    assert_eq!(&stored, final_row, "{case}: a terminal row changed");
                    if &next != final_row {
                        assert!(is_refused(result), "{case}: rewrite was not refused");
                    }
                }
                None => {
                    result.unwrap_or_else(|e| panic!("{case}: {e}"));
                    assert_eq!(stored, next, "{case}");
                    if next.status.is_terminal() {
                        settled = Some(next.clone());
                    }
                    current = next.clone();
                    snapshots.push(next);
                }
            }
        }
    }
}

fn attempts_and_leases_are_final_once_settled(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let inv = fx::invocation(&t, &FunctionId::generate(), None);
    invs(&s).insert(inv.clone()).unwrap();
    let env = EnvironmentId::generate();
    let mut att = fx::attempt(&inv, &env);
    invs(&s).insert_attempt(att.clone()).unwrap();
    let dispatched = att.clone();
    att.succeed(now()).unwrap();
    invs(&s).update_attempt(att.clone()).unwrap();
    assert!(is_refused(invs(&s).update_attempt(dispatched)));
    let mut failed = att.clone();
    failed.status = AttemptStatus::Failed {
        error: InvocationError::new(ErrorClass::PlatformError, "X", "late"),
    };
    assert!(is_refused(invs(&s).update_attempt(failed)));
    let mut re_epoched = att.clone();
    re_epoched.epoch = 2;
    assert!(is_refused(invs(&s).update_attempt(re_epoched)));
    assert_eq!(invs(&s).attempts_of(&inv.id).unwrap(), vec![att.clone()]);

    let _ = (env, t);

    // A released lease is final: it cannot be released, renewed or completed
    // again, and it accepts nothing any more.
    let owner = fx::dispatcher(slots(&s), "final", 30);
    let (slot_env, slot_inv) = ready_slot(&s, &owner);
    let req = slot_request(&slot_env, &slot_inv, &owner, now(), 30);
    let (lease, attempt) = (req.lease.clone(), req.attempt.clone());
    slots(&s).acquire(req).unwrap();
    assert!(
        slots(&s)
            .release_lease(&lease.id, &attempt.id, lease.epoch, now())
            .unwrap()
    );
    assert!(
        !slots(&s)
            .release_lease(&lease.id, &attempt.id, lease.epoch, now())
            .unwrap(),
        "a released lease is never released twice"
    );
    assert!(
        !slots(&s)
            .renew_lease(&lease.id, &owner, lease.epoch, Duration::seconds(30), now())
            .unwrap(),
        "a released lease is never revived"
    );
    let stored = slots(&s).get_lease(&lease.id).unwrap().unwrap();
    assert_eq!(stored.released_at, Some(now()));
    assert!(!stored.accepts(&attempt.id, lease.epoch));
}

fn an_environment_copy_from_another_epoch_is_refused(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let owner = fx::dispatcher(slots(&s), "epoch", 30);
    let mut env = fx::ready_environment(&key).owned_by(owner.clone());
    env.assign(now()).unwrap();
    env.mark_idle(now()).unwrap();
    envs(&s).insert(env.clone()).unwrap();
    let before_claim = env.clone();

    let reserved = slots(&s)
        .claim_for_reuse(&key, Some(&owner), now())
        .unwrap()
        .unwrap();
    assert_eq!(reserved.epoch, 1, "a claim reserves; it does not assign");
    let mut inv = fx::invocation(&key.tenant_id, &FunctionId::generate(), None);
    inv.dispatcher_id = Some(owner.clone());
    invs(&s).insert(inv.clone()).unwrap();
    let req = slot_request(&reserved, &inv, &owner, now(), 30);
    let lease = req.lease.clone();
    let attempt = req.attempt.clone();
    assert_eq!(slots(&s).acquire(req).unwrap(), AcquireOutcome::Acquired);
    let mut done = attempt.clone();
    done.succeed(now()).unwrap();
    assert_eq!(
        slots(&s)
            .complete(SlotCompletion {
                lease_id: lease.id.clone(),
                attempt: done,
                invocation: None,
                now: now(),
            })
            .unwrap(),
        CompletionOutcome::Accepted
    );
    let claimed = envs(&s).get(&reserved.id).unwrap().unwrap();
    assert_eq!(claimed.epoch, 2);
    let mut stale = before_claim;
    stale.mark_draining(now()).unwrap();
    assert!(
        is_refused(envs(&s).update(stale)),
        "a copy from epoch 1 cannot drain the environment epoch 2 runs on"
    );
    let mut rekeyed = claimed.clone();
    rekeyed.reuse_key.configuration_version += 1;
    assert!(
        is_refused(envs(&s).update(rekeyed)),
        "reuse key is immutable"
    );

    let mut stopped = claimed.clone();
    stopped.mark_stopped(now()).unwrap();
    envs(&s).update(stopped.clone()).unwrap();
    assert!(
        is_refused(envs(&s).update(claimed)),
        "a stopped environment never comes back"
    );
    assert_eq!(envs(&s).get(&stopped.id).unwrap().unwrap(), stopped);
    assert!(envs(&s).list_active().unwrap().is_empty());

    // Only an acquire makes an environment busy.
    let fresh = fx::ready_environment(&key).owned_by(owner.clone());
    envs(&s).insert(fresh.clone()).unwrap();
    let mut sneaked = fresh;
    sneaked.mark_busy(now()).unwrap();
    assert!(
        is_refused(envs(&s).update(sneaked)),
        "a plain update cannot assign an environment without a lease"
    );
}

fn rows_never_cross_a_tenant(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let a = TenantId::generate();
    let b = TenantId::generate();
    let fa = fx::function(&a, "mine");
    let fb = fx::function(&b, "theirs");
    fns(&s).insert(fa.clone()).unwrap();
    fns(&s).insert(fb.clone()).unwrap();
    let ra = fx::ready_revision(&fa, 1);
    let rb = fx::ready_revision(&fb, 1);
    revs(&s).insert(ra.clone()).unwrap();
    revs(&s).insert(rb.clone()).unwrap();

    // A revision of tenant b's function claimed by tenant a.
    let mut foreign_rev = fx::revision(&fb, 2);
    foreign_rev.tenant_id = a.clone();
    assert!(is_refused(revs(&s).insert(foreign_rev)), "revision");

    // An alias of a's function pointing at b's revision, or at nothing.
    let alias = |rev: &RevisionId| {
        FunctionAlias::new(
            fa.id.clone(),
            a.clone(),
            AliasName::default_alias(),
            rev.clone(),
            now(),
        )
    };
    assert!(
        is_refused(aliases(&s).insert(alias(&rb.id))),
        "alias target"
    );
    assert!(
        is_refused(aliases(&s).insert(alias(&RevisionId::generate()))),
        "alias to a revision that does not exist"
    );
    let mut foreign_alias = alias(&ra.id);
    foreign_alias.tenant_id = b.clone();
    assert!(
        is_refused(aliases(&s).insert(foreign_alias)),
        "alias tenant"
    );
    aliases(&s).insert(alias(&ra.id)).unwrap();
    let mut hijack = alias(&ra.id);
    hijack.update(rb.id.clone(), None, now()).unwrap();
    assert!(
        is_refused(aliases(&s).compare_and_set(hijack, 1)),
        "an alias cannot be moved onto another tenant's revision"
    );

    // An invocation of b's function in tenant a, and children of it.
    assert!(
        is_refused(invs(&s).insert(fx::invocation(&a, &fb.id, None))),
        "invocation"
    );
    assert!(
        is_refused(s.insert_bound(fx::invocation(&a, &fb.id, Some("k")))),
        "keyed invocation"
    );
    let inv_b = fx::invocation(&b, &fb.id, None);
    invs(&s).insert(inv_b.clone()).unwrap();
    let mut att = fx::attempt(&inv_b, &EnvironmentId::generate());
    att.tenant_id = a.clone();
    assert!(is_refused(invs(&s).insert_attempt(att)), "attempt");
    let mut moved = inv_b.clone();
    moved.tenant_id = a.clone();
    assert!(
        is_refused(invs(&s).update(moved)),
        "invocation tenant change"
    );

    // Environments and leases.
    let mut env = fx::environment(&fx::key(&a, &rb.id));
    assert!(
        is_refused(envs(&s).insert(env.clone())),
        "environment of b's revision"
    );
    env.reuse_key.tenant_id = b.clone();
    assert!(
        is_refused(envs(&s).insert(env)),
        "reuse key naming another tenant"
    );
    let owner = fx::dispatcher(slots(&s), "tenants", 30);
    let env_b = fx::ready_environment(&fx::key(&b, &rb.id)).owned_by(owner.clone());
    envs(&s).insert(env_b.clone()).unwrap();
    let mut inv_for_b = fx::invocation(&b, &fb.id, None);
    inv_for_b.dispatcher_id = Some(owner.clone());
    invs(&s).insert(inv_for_b.clone()).unwrap();
    let mut foreign_lease = slot_request(&env_b, &inv_for_b, &owner, now(), 30);
    foreign_lease.lease.tenant_id = a.clone();
    assert!(is_refused(slots(&s).acquire(foreign_lease)), "lease");

    // Nothing leaked into tenant a.
    assert_eq!(fns(&s).list(&a).unwrap(), vec![fa.clone()]);
    assert!(invs(&s).list_by_function(&fa.id, 10).unwrap().is_empty());
}

fn alias_updates_are_compare_and_set(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let f = fx::function(&t, "cas");
    fns(&s).insert(f.clone()).unwrap();
    let r1 = fx::ready_revision(&f, 1);
    let r2 = fx::ready_revision(&f, 2);
    revs(&s).insert(r1.clone()).unwrap();
    revs(&s).insert(r2.clone()).unwrap();
    let name = AliasName::default_alias();
    let alias = FunctionAlias::new(f.id.clone(), t.clone(), name.clone(), r1.id.clone(), now());
    aliases(&s).insert(alias.clone()).unwrap();

    let mut to_r2 = alias.clone();
    to_r2.update(r2.id.clone(), Some(1), now()).unwrap();
    assert!(aliases(&s).compare_and_set(to_r2.clone(), 1).unwrap());
    // The same write again carries a generation that is gone.
    assert!(
        !aliases(&s).compare_and_set(to_r2.clone(), 1).unwrap(),
        "a stale generation loses"
    );
    let stored = aliases(&s).get(&f.id, &name).unwrap().unwrap();
    assert_eq!(stored, to_r2);
    assert_eq!(stored.previous_revision_id, Some(r1.id.clone()));

    let mut not_advanced = stored.clone();
    not_advanced.revision_id = r1.id.clone();
    assert!(
        is_refused(aliases(&s).compare_and_set(not_advanced, 2)),
        "a CAS write must advance the generation"
    );
    let mut renamed = stored.clone();
    renamed.generation = 3;
    renamed.created_at = now() + chrono::Duration::seconds(1);
    assert!(is_refused(aliases(&s).compare_and_set(renamed, 2)));
    let mut missing = stored.clone();
    missing.name = "canary".parse().unwrap();
    missing.generation = 3;
    assert!(matches!(
        aliases(&s).compare_and_set(missing, 2),
        Err(RepoError::NotFound(_))
    ));
    assert_eq!(aliases(&s).list(&f.id).unwrap(), vec![stored]);
}

/// N writers read generation 1 and race to move the alias. Exactly one wins;
/// every loser is told so and wrote nothing.
fn concurrent_alias_updates_have_exactly_one_winner(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let f = fx::function(&t, "race");
    fns(&s).insert(f.clone()).unwrap();
    let base = fx::ready_revision(&f, 1);
    revs(&s).insert(base.clone()).unwrap();
    let alias = FunctionAlias::new(
        f.id.clone(),
        t.clone(),
        AliasName::default_alias(),
        base.id.clone(),
        now(),
    );
    aliases(&s).insert(alias.clone()).unwrap();

    let writers = 8;
    let targets: Vec<FunctionRevision> = (0..writers)
        .map(|i| {
            let r = fx::ready_revision(&f, 2 + i as u64);
            revs(&s).insert(r.clone()).unwrap();
            r
        })
        .collect();
    let barrier = Arc::new(std::sync::Barrier::new(writers));
    let handles: Vec<_> = targets
        .into_iter()
        .map(|r| {
            let s = s.clone();
            let barrier = barrier.clone();
            let mut next = alias.clone();
            next.update(r.id.clone(), Some(1), now()).unwrap();
            std::thread::spawn(move || {
                barrier.wait();
                let won = aliases(&s).compare_and_set(next.clone(), 1).unwrap();
                (won, next)
            })
        })
        .collect();
    let results: Vec<(bool, FunctionAlias)> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();
    let winners: Vec<&FunctionAlias> = results.iter().filter(|(w, _)| *w).map(|(_, a)| a).collect();
    assert_eq!(winners.len(), 1, "exactly one writer moves the alias");
    let stored = aliases(&s)
        .get(&f.id, &AliasName::default_alias())
        .unwrap()
        .unwrap();
    assert_eq!(&stored, winners[0]);
    assert_eq!(stored.generation, 2);
}

fn inline_output_is_bounded(make: fn(Limits) -> Store) {
    let limits = Limits {
        max_response_bytes: 16,
        ..Limits::default()
    };
    let s = make(limits);
    let t = TenantId::generate();
    let f = FunctionId::generate();
    let mut inv = fx::invocation(&t, &f, None);
    invs(&s).insert(inv.clone()).unwrap();
    inv.mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    invs(&s).update(inv.clone()).unwrap();

    let mut too_big = inv.clone();
    too_big
        .mark_succeeded(Some(fx::inline(&[b'x'; 17])), None, now())
        .unwrap();
    assert!(is_refused(invs(&s).update(too_big)), "17 bytes > 16");
    let mut lying = inv.clone();
    lying
        .mark_succeeded(
            Some(PayloadRef::Inline {
                bytes_base64: "A".repeat(4096),
                size_bytes: 1,
            }),
            None,
            now(),
        )
        .unwrap();
    assert!(
        is_refused(invs(&s).update(lying)),
        "the encoded body is bounded too, not just the claimed size"
    );
    // A large output is recorded by digest only.
    let mut by_digest = inv.clone();
    by_digest
        .mark_succeeded(
            Some(PayloadRef::Digest {
                digest: Sha256Digest::of_bytes(&[b'x'; 1 << 20]),
                size_bytes: 1 << 20,
            }),
            None,
            now(),
        )
        .unwrap();
    invs(&s).update(by_digest.clone()).unwrap();
    assert_eq!(invs(&s).get(&inv.id).unwrap().unwrap(), by_digest);

    let mut fresh = fx::invocation(&t, &f, None);
    fresh.output = Some(fx::inline(&[b'y'; 64]));
    assert!(is_refused(invs(&s).insert(fresh)), "bounded on insert too");
}

// ---------------------------------------------------------------------------
// idempotency, logs, artifacts (P1 tests, now on both stores)
// ---------------------------------------------------------------------------

fn idempotency_key_is_bound_with_its_invocation(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let t = TenantId::generate();
    let f = FunctionId::generate();
    assert_eq!(s.lookup(&t, &f, "k", now()).unwrap(), None);
    let first = fx::invocation(&t, &f, Some("k"));
    let first_id = first.id.clone();
    assert_eq!(s.insert_bound(first).unwrap(), IdempotencyOutcome::Inserted);
    let binding = IdempotencyBinding {
        invocation_id: first_id.clone(),
        input_digest: Sha256Digest::of_bytes(b"{}"),
        expires_at: None,
    };
    assert_eq!(s.lookup(&t, &f, "k", now()).unwrap(), Some(binding.clone()));
    assert!(invs(&s).get(&first_id).unwrap().is_some());

    // A second invocation with the same key is not inserted.
    let second = fx::invocation(&t, &f, Some("k"));
    let second_id = second.id.clone();
    assert_eq!(
        s.insert_bound(second).unwrap(),
        IdempotencyOutcome::Existing(binding)
    );
    assert!(invs(&s).get(&second_id).unwrap().is_none());
    // Same key, other tenant: its own scope.
    let other = TenantId::generate();
    assert_eq!(
        s.insert_bound(fx::invocation(&other, &f, Some("k")))
            .unwrap(),
        IdempotencyOutcome::Inserted
    );
    // No key: plain insert; a duplicate id conflicts.
    let plain = fx::invocation(&t, &f, None);
    assert_eq!(
        s.insert_bound(plain.clone()).unwrap(),
        IdempotencyOutcome::Inserted
    );
    assert!(is_conflict(s.insert_bound(plain)));
}

fn logs_are_bounded_per_invocation(make: fn(Limits) -> Store) {
    let limits = Limits {
        max_log_lines_per_invocation: 2,
        ..Limits::default()
    };
    let s = make(limits);
    let inv = InvocationId::generate();
    let tenant = TenantId::generate();
    let rec = |line: &str| LogRecord {
        tenant_id: tenant.clone(),
        environment_id: EnvironmentId::generate(),
        invocation_id: Some(inv.clone()),
        attempt_id: None,
        stream: LogStream::Stdout,
        phase: LogPhase::Handler,
        timestamp: now(),
        line: line.into(),
        truncated: false,
    };
    assert_eq!(s.append(rec("a")), AppendOutcome::Stored);
    assert_eq!(s.append(rec("b")), AppendOutcome::Stored);
    assert_eq!(s.append(rec("c")), AppendOutcome::Dropped);
    let q = s.query(&tenant, &inv).unwrap();
    assert_eq!(q.records.len(), 2);
    assert!(q.dropped);
    // Another tenant never sees the lines, nor that some were dropped.
    let q = s.query(&TenantId::generate(), &inv).unwrap();
    assert!(q.records.is_empty());
    assert!(!q.dropped);
}

fn artifact_ownership_is_per_tenant(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let a = TenantId::generate();
    let b = TenantId::generate();
    let d = Sha256Digest::of_bytes(b"binary");
    assert!(!s.is_owned_by(&a, &d).unwrap());
    s.claim(&a, &d).unwrap();
    s.claim(&a, &d).unwrap();
    assert!(s.is_owned_by(&a, &d).unwrap());
    assert!(!s.is_owned_by(&b, &d).unwrap());
    s.claim(&b, &d).unwrap();
    assert!(s.is_owned_by(&b, &d).unwrap());
}

// ---------------------------------------------------------------------------
// environment pool (PLT-4632, now on both stores)
// ---------------------------------------------------------------------------

const POOL: PoolLimits = PoolLimits {
    max_idle_per_key: 2,
    max_total_idle: 4,
};

/// An environment that reached Ready, served an attempt and went back into
/// the pool.
fn pooled(s: &Store, key: &ReuseKey) -> EnvironmentId {
    let mut env = fx::ready_environment(key);
    env.assign(now()).unwrap();
    env.mark_idle(now()).unwrap();
    let id = env.id.clone();
    envs(s).insert(env).unwrap();
    id
}

/// One dimension of the reuse key: its name, and how to make it differ.
type Dimension = (&'static str, fn(&mut ReuseKey));

/// PLT-4632 acceptance 1: one differing field of the reuse key is enough to
/// make two environments incompatible.
fn only_an_exactly_matching_reuse_key_is_reused(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let id = pooled(&s, &key);

    let dimensions: [Dimension; 8] = [
        ("tenant", |k| k.tenant_id = TenantId::generate()),
        ("revision", |k| k.revision_id = RevisionId::generate()),
        ("execution role version", |k| k.execution_role_version += 1),
        ("configuration version", |k| k.configuration_version += 1),
        ("resource profile", |k| {
            k.resource_profile_digest = "other".into()
        }),
        ("runtime profile", |k| {
            k.runtime_profile = "tachyon.runtime.v2".into()
        }),
        ("network policy version", |k| k.network_policy_version += 1),
        ("secret binding generation", |k| {
            k.secret_binding_generation += 1
        }),
    ];
    for (name, change) in dimensions {
        let mut other = key.clone();
        change(&mut other);
        assert_ne!(other, key, "{name} must actually differ");
        assert!(
            slots(&s)
                .claim_for_reuse(&other, None, now())
                .unwrap()
                .is_none(),
            "a differing {name} must never reuse the environment"
        );
    }
    // Nothing above touched the pooled environment.
    assert_eq!(slots(&s).list_idle(None).unwrap().len(), 1);
    let claimed = slots(&s)
        .claim_for_reuse(&key, None, now())
        .unwrap()
        .expect("the exact key hits");
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.state, EnvironmentState::Ready);
    assert_eq!(
        claimed.epoch, 1,
        "a claim reserves the environment; the acquire advances the epoch"
    );
    assert!(
        slots(&s)
            .claim_for_reuse(&key, None, now())
            .unwrap()
            .is_none(),
        "it was handed out once"
    );
}

/// PLT-4632 acceptance 2.
fn nothing_is_dispatched_before_ready_and_busy_is_never_handed_out(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let fresh = || fx::environment(&key);

    let mut provisioning = fresh();
    provisioning.mark_provisioning(now()).unwrap();
    let mut initializing = fresh();
    initializing.mark_provisioning(now()).unwrap();
    initializing
        .mark_initializing(BootEvidence::default(), now())
        .unwrap();
    let mut busy = fx::ready_environment(&key);
    busy.assign(now()).unwrap();
    let busy_id = busy.id.clone();
    let mut draining = fx::ready_environment(&key);
    draining.assign(now()).unwrap();
    draining.mark_idle(now()).unwrap();
    draining.mark_draining(now()).unwrap();
    let mut stopped = fx::ready_environment(&key);
    stopped.mark_stopped(now()).unwrap();

    for env in [fresh(), provisioning, initializing, busy, draining, stopped] {
        envs(&s).insert(env).unwrap();
    }
    assert!(
        slots(&s)
            .claim_for_reuse(&key, None, now())
            .unwrap()
            .is_none(),
        "only an environment that reported ready and is free may be handed out"
    );
    assert_eq!(
        envs(&s).get(&busy_id).unwrap().unwrap().epoch,
        1,
        "a refused claim never advances an epoch"
    );
    assert!(slots(&s).list_idle(None).unwrap().is_empty());

    // The same key hits as soon as one is actually pooled.
    let id = pooled(&s, &key);
    assert_eq!(
        slots(&s)
            .claim_for_reuse(&key, None, now())
            .unwrap()
            .map(|e| e.id),
        Some(id)
    );
}

/// Property: with `pooled` idle environments and `claimers` threads racing
/// for them, exactly `min(pooled, claimers)` claims win, no environment is
/// handed to two callers, and every winner comes back one epoch further on.
fn concurrent_claims_never_hand_the_same_environment_to_two_callers(make: fn(Limits) -> Store) {
    for (idle, claimers) in [(1usize, 2usize), (1, 16), (3, 8), (8, 3), (4, 4)] {
        let s = make(Limits::default());
        let key = fx::key(&TenantId::generate(), &RevisionId::generate());
        for _ in 0..idle {
            pooled(&s, &key);
        }
        let barrier = Arc::new(std::sync::Barrier::new(claimers));
        let racers: Vec<_> = (0..claimers)
            .map(|_| {
                let s = s.clone();
                let key = key.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    slots(&s).claim_for_reuse(&key, None, now()).unwrap()
                })
            })
            .collect();
        let winners: Vec<ExecutionEnvironment> = racers
            .into_iter()
            .filter_map(|h| h.join().expect("claim panicked"))
            .collect();

        let case = format!("idle={idle} claimers={claimers}");
        assert_eq!(winners.len(), idle.min(claimers), "{case}");
        let mut ids: Vec<String> = winners.iter().map(|e| e.id.to_string()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            winners.len(),
            "{case}: an environment was handed out twice"
        );
        for w in &winners {
            assert_eq!(w.state, EnvironmentState::Ready, "{case}: reserved");
            assert_eq!(w.epoch, 1, "{case}: a reservation does not assign");
        }
        assert_eq!(
            slots(&s).list_idle(None).unwrap().len(),
            idle.saturating_sub(claimers),
            "{case}: the losers' environments stay pooled"
        );
    }
}

fn releasing_respects_the_pool_caps_and_refuses_a_stale_copy(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let other_key = fx::key(&key.tenant_id, &RevisionId::generate());

    // Two per key is the cap: the third stays Busy for the caller to kill.
    let mut busy = Vec::new();
    for _ in 0..3 {
        let mut env = fx::ready_environment(&key);
        env.mark_busy(now()).unwrap();
        envs(&s).insert(env.clone()).unwrap();
        busy.push(env);
    }
    assert!(
        slots(&s)
            .release_to_pool(&busy[0], POOL, now())
            .unwrap()
            .is_some()
    );
    assert!(
        slots(&s)
            .release_to_pool(&busy[1], POOL, now())
            .unwrap()
            .is_some()
    );
    assert!(
        slots(&s)
            .release_to_pool(&busy[2], POOL, now())
            .unwrap()
            .is_none(),
        "max_idle_per_key is enforced"
    );
    assert_eq!(
        envs(&s).get(&busy[2].id).unwrap().unwrap().state,
        EnvironmentState::Busy,
        "a refused release leaves the environment for the caller to terminate"
    );

    // A stale copy (the row moved on since) is refused.
    let pooled_row = envs(&s).get(&busy[0].id).unwrap().unwrap();
    assert!(
        slots(&s)
            .release_to_pool(&busy[0], POOL, now())
            .unwrap()
            .is_none(),
        "the row is Idle now, not Busy"
    );
    let mut wrong_epoch = busy[2].clone();
    wrong_epoch.epoch = 99;
    assert!(
        slots(&s)
            .release_to_pool(&wrong_epoch, POOL, now())
            .unwrap()
            .is_none(),
        "a copy from another epoch never goes back into the pool"
    );
    assert_eq!(pooled_row.state, EnvironmentState::Idle);
    assert_eq!(pooled_row.idle_since, Some(now()));

    // Two more keys still fit under max_total_idle (4).
    for _ in 0..2 {
        let mut env = fx::ready_environment(&other_key);
        env.mark_busy(now()).unwrap();
        envs(&s).insert(env.clone()).unwrap();
        assert!(
            slots(&s)
                .release_to_pool(&env, POOL, now())
                .unwrap()
                .is_some()
        );
    }
    let mut env = fx::ready_environment(&other_key);
    env.mark_busy(now()).unwrap();
    envs(&s).insert(env.clone()).unwrap();
    assert!(
        slots(&s)
            .release_to_pool(&env, POOL, now())
            .unwrap()
            .is_none(),
        "max_total_idle is enforced across keys"
    );
    assert_eq!(slots(&s).list_idle(None).unwrap().len(), 4);
}

/// Regression (PLT-4632 review F1): pool membership is `Idle` only.
fn a_ready_environment_is_not_in_the_pool_and_is_never_claimed(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let ready = fx::ready_environment(&key);
    let ready_id = ready.id.clone();
    envs(&s).insert(ready).unwrap();

    assert!(
        slots(&s).list_idle(None).unwrap().is_empty(),
        "a Ready environment is not pool membership"
    );
    assert!(
        slots(&s)
            .claim_for_reuse(&key, None, now())
            .unwrap()
            .is_none(),
        "an environment that was never released to the pool is never claimed"
    );
    let untouched = envs(&s).get(&ready_id).unwrap().unwrap();
    assert_eq!(untouched.state, EnvironmentState::Ready);
    assert_eq!(
        untouched.epoch, 0,
        "a refused claim never advances an epoch"
    );

    let pooled_id = pooled(&s, &key);
    assert_eq!(
        slots(&s)
            .claim_for_reuse(&key, None, now())
            .unwrap()
            .map(|e| e.id),
        Some(pooled_id)
    );
}

/// PLT-4635: a `min_ready` pre-start is published straight from `Ready` at
/// epoch 0 (it never served an attempt); a `Ready` row that was already
/// assigned once, or claimed back out of the pool at a later epoch, is not.
fn a_never_assigned_ready_environment_can_be_pre_started_into_the_pool(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let fresh = fx::ready_environment(&key);
    envs(&s).insert(fresh.clone()).unwrap();
    let pooled_env = slots(&s)
        .release_to_pool(&fresh, POOL, now())
        .unwrap()
        .expect("a never-assigned Ready environment is pooled");
    assert_eq!(pooled_env.state, EnvironmentState::Idle);
    assert_eq!(pooled_env.epoch, 0);
    // Claimed (Idle -> Ready at epoch 0), then assigned (epoch 1): it
    // cannot be published from Ready again.
    let claimed = slots(&s)
        .claim_for_reuse(&key, None, now())
        .unwrap()
        .expect("claimable");
    assert_eq!(claimed.state, EnvironmentState::Ready);
    let mut assigned = claimed.clone();
    assigned.assign(now()).unwrap();
    assert!(
        slots(&s)
            .release_to_pool(&assigned, POOL, now())
            .unwrap()
            .is_none(),
        "a copy at another epoch than the stored row is refused"
    );
}

fn taking_an_idle_environment_for_termination_excludes_a_claim(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let id = pooled(&s, &key);

    assert!(slots(&s).take_idle_for_termination(&id, now()).unwrap());
    assert!(
        !slots(&s).take_idle_for_termination(&id, now()).unwrap(),
        "the second caller loses"
    );
    assert!(
        slots(&s)
            .claim_for_reuse(&key, None, now())
            .unwrap()
            .is_none(),
        "an environment the sweeper owns is never handed to an attempt"
    );
    assert_eq!(
        envs(&s).get(&id).unwrap().unwrap().state,
        EnvironmentState::Draining
    );

    // The other way round: a claimed environment cannot be swept.
    let claimed = pooled(&s, &key);
    assert!(
        slots(&s)
            .claim_for_reuse(&key, None, now())
            .unwrap()
            .is_some()
    );
    assert!(
        !slots(&s)
            .take_idle_for_termination(&claimed, now())
            .unwrap()
    );
}

// ---------------------------------------------------------------------------
// slot store: acquire, fencing, leases, reclaim (PLT-4631)
// ---------------------------------------------------------------------------

fn at(seconds: i64) -> Timestamp {
    now() + Duration::seconds(seconds)
}

fn reclaim(s: &Store, reclaimer: &DispatcherId, when: Timestamp) -> ReclaimReport {
    slots(s)
        .reclaim_expired(ReclaimRequest {
            reclaimer: reclaimer.clone(),
            now: when,
            skew: Duration::seconds(2),
            presumed_dead: Vec::new(),
        })
        .unwrap()
}

fn terminal_attempt(attempt: &InvocationAttempt, when: Timestamp) -> InvocationAttempt {
    let mut done = attempt.clone();
    done.succeed(when).unwrap();
    done
}

fn succeeded(inv: &Invocation, when: Timestamp) -> Invocation {
    let mut done = invs_get(inv);
    done.mark_succeeded(Some(fx::inline(b"{}")), None, when)
        .unwrap();
    done
}

fn invs_get(inv: &Invocation) -> Invocation {
    inv.clone()
}

/// Property: however many callers race for the same slot at the same epoch —
/// each with its own attempt, lease and invocation — exactly one acquire is
/// written, the slot moves exactly one epoch, and nothing of a loser (lease,
/// attempt, `Running`) exists. Repeated over several rounds of release and
/// reuse, so the property holds at every epoch.
fn acquire_is_a_cas_with_exactly_one_winner_per_epoch(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let owner = fx::dispatcher(slots(&s), "race", 3600);
    let (env, _) = ready_slot(&s, &owner);
    let big = PoolLimits {
        max_idle_per_key: 100,
        max_total_idle: 100,
    };
    for (round, racers) in [2usize, 8, 16, 4].into_iter().enumerate() {
        let stored = envs(&s).get(&env.id).unwrap().unwrap();
        let requests: Vec<SlotAcquire> = (0..racers)
            .map(|_| {
                let mut inv = fx::invocation(&env.tenant_id, &FunctionId::generate(), None);
                inv.dispatcher_id = Some(owner.clone());
                invs(&s).insert(inv.clone()).unwrap();
                slot_request(&stored, &inv, &owner, now(), 30)
            })
            .collect();
        let barrier = Arc::new(std::sync::Barrier::new(racers));
        let handles: Vec<_> = requests
            .clone()
            .into_iter()
            .map(|req| {
                let s = s.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    slots(&s).acquire(req).unwrap()
                })
            })
            .collect();
        let outcomes: Vec<AcquireOutcome> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        let case = format!("round {round}, {racers} racers");
        let winners: Vec<usize> = outcomes
            .iter()
            .enumerate()
            .filter(|(_, o)| **o == AcquireOutcome::Acquired)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(winners.len(), 1, "{case}: {outcomes:?}");
        let after = envs(&s).get(&env.id).unwrap().unwrap();
        assert_eq!(after.epoch, stored.epoch + 1, "{case}");
        assert_eq!(after.state, EnvironmentState::Busy, "{case}");
        for (i, req) in requests.iter().enumerate() {
            let lease = slots(&s).get_lease(&req.lease.id).unwrap();
            let attempt = invs(&s).get_attempt(&req.attempt.id).unwrap();
            let inv = invs(&s)
                .get(&req.invocation.as_ref().unwrap().id)
                .unwrap()
                .unwrap();
            if i == winners[0] {
                assert!(lease.is_some_and(|l| l.released_at.is_none()), "{case}");
                assert!(attempt.is_some(), "{case}");
                assert_eq!(inv.status, InvocationStatus::Running, "{case}");
            } else {
                assert!(lease.is_none(), "{case}: a loser's lease was written");
                assert!(attempt.is_none(), "{case}: a loser's attempt was written");
                assert_eq!(inv.status, InvocationStatus::Accepted, "{case}");
            }
        }
        // Finish the winner and pool the environment for the next round.
        let win = &requests[winners[0]];
        assert_eq!(
            slots(&s)
                .complete(SlotCompletion {
                    lease_id: win.lease.id.clone(),
                    attempt: terminal_attempt(&win.attempt, now()),
                    invocation: Some(succeeded(win.invocation.as_ref().unwrap(), now())),
                    now: now(),
                })
                .unwrap(),
            CompletionOutcome::Accepted
        );
        assert!(
            slots(&s)
                .release_to_pool(&after, big, now())
                .unwrap()
                .is_some(),
            "{case}"
        );
    }

    // Losing preconditions, one at a time.
    let stored = envs(&s).get(&env.id).unwrap().unwrap();
    let mut inv = fx::invocation(&env.tenant_id, &FunctionId::generate(), None);
    inv.dispatcher_id = Some(owner.clone());
    invs(&s).insert(inv.clone()).unwrap();
    let mut wrong_epoch = slot_request(&stored, &inv, &owner, now(), 30);
    wrong_epoch.expected_epoch -= 1;
    wrong_epoch.env.epoch -= 1;
    wrong_epoch.lease.epoch -= 1;
    wrong_epoch.attempt.epoch -= 1;
    assert!(matches!(
        slots(&s).acquire(wrong_epoch).unwrap(),
        AcquireOutcome::Lost(_)
    ));
    let stranger = fx::dispatcher(slots(&s), "stranger", 3600);
    let foreign = slot_request(&stored, &inv, &stranger, now(), 30);
    assert!(
        matches!(slots(&s).acquire(foreign), Ok(AcquireOutcome::Lost(_))),
        "only the owner of an environment (the holder of its session) acquires it"
    );
    let mut malformed = slot_request(&stored, &inv, &owner, now(), 30);
    malformed.lease.epoch += 1;
    assert!(matches!(
        slots(&s).acquire(malformed),
        Err(RepoError::Refused(_))
    ));
    assert_eq!(
        envs(&s).get(&env.id).unwrap().unwrap(),
        stored,
        "nothing of a refused or lost acquire is written"
    );
}

/// PLT-4631 acceptance 1: a late completion carrying an older epoch (a
/// delayed callback of an earlier assignment, or of a reclaimed lease) never
/// overwrites what the current assignment or the reclaim recorded.
fn a_completion_with_a_stale_epoch_never_overwrites_state(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let owner = fx::dispatcher(slots(&s), "late", 3600);
    let (env, first_inv) = ready_slot(&s, &owner);
    let first = slot_request(&env, &first_inv, &owner, now(), 30);
    slots(&s).acquire(first.clone()).unwrap();
    // The first attempt finishes, the environment is pooled and reassigned.
    assert_eq!(
        slots(&s)
            .complete(SlotCompletion {
                lease_id: first.lease.id.clone(),
                attempt: terminal_attempt(&first.attempt, now()),
                invocation: Some(succeeded(first.invocation.as_ref().unwrap(), now())),
                now: now(),
            })
            .unwrap(),
        CompletionOutcome::Accepted
    );
    let busy = envs(&s).get(&env.id).unwrap().unwrap();
    let limits = PoolLimits {
        max_idle_per_key: 4,
        max_total_idle: 4,
    };
    slots(&s)
        .release_to_pool(&busy, limits, now())
        .unwrap()
        .unwrap();
    let reserved = slots(&s)
        .claim_for_reuse(&env.reuse_key, Some(&owner), now())
        .unwrap()
        .unwrap();
    let mut second_inv = fx::invocation(&env.tenant_id, &FunctionId::generate(), None);
    second_inv.dispatcher_id = Some(owner.clone());
    invs(&s).insert(second_inv.clone()).unwrap();
    let second = slot_request(&reserved, &second_inv, &owner, now(), 30);
    slots(&s).acquire(second.clone()).unwrap();
    assert_eq!(second.attempt.epoch, 2);

    // A delayed callback of the first assignment, replayed.
    let mut replayed = first.attempt.clone();
    replayed
        .fail(
            InvocationError::new(ErrorClass::Crash, "Late", "late frame"),
            now(),
        )
        .unwrap();
    assert!(matches!(
        slots(&s)
            .complete(SlotCompletion {
                lease_id: first.lease.id.clone(),
                attempt: replayed,
                invocation: None,
                now: now(),
            })
            .unwrap(),
        CompletionOutcome::Stale(_)
    ));
    // The current lease, but a result stamped with the previous epoch.
    let mut stale_epoch = terminal_attempt(&second.attempt, now());
    stale_epoch.epoch = 1;
    assert!(matches!(
        slots(&s)
            .complete(SlotCompletion {
                lease_id: second.lease.id.clone(),
                attempt: stale_epoch,
                invocation: Some(succeeded(second.invocation.as_ref().unwrap(), now())),
                now: now(),
            })
            .unwrap(),
        CompletionOutcome::Stale(_)
    ));
    assert!(
        !slots(&s)
            .release_lease(&second.lease.id, &first.attempt.id, 1, now())
            .unwrap(),
        "a stale release is refused too"
    );
    // Nothing moved.
    let lease = slots(&s).get_lease(&second.lease.id).unwrap().unwrap();
    assert!(lease.released_at.is_none());
    assert_eq!(
        invs(&s).get(&second_inv.id).unwrap().unwrap().status,
        InvocationStatus::Running
    );
    assert_eq!(
        invs(&s).get(&first_inv.id).unwrap().unwrap().status,
        InvocationStatus::Succeeded
    );
    assert_eq!(envs(&s).get(&env.id).unwrap().unwrap().epoch, 2);

    // A completion that arrives after the lease was reclaimed is refused and
    // leaves the reclaim's `OutcomeUnknown` in place.
    let reclaimer = fx::dispatcher(slots(&s), "reclaimer", 3600);
    let report = reclaim(&s, &reclaimer, at(40));
    assert_eq!(report.leases, 1);
    let late = slots(&s)
        .complete(SlotCompletion {
            lease_id: second.lease.id.clone(),
            attempt: terminal_attempt(&second.attempt, at(41)),
            invocation: Some(succeeded(second.invocation.as_ref().unwrap(), at(41))),
            now: at(41),
        })
        .unwrap();
    assert!(matches!(late, CompletionOutcome::Stale(_)), "{late:?}");
    let settled = invs(&s).get(&second_inv.id).unwrap().unwrap();
    match settled.status {
        InvocationStatus::OutcomeUnknown { error } => {
            assert_eq!(error.error_type, HOST_LEASE_EXPIRED)
        }
        other => panic!("the reclaim's outcome was overwritten: {other:?}"),
    }
}

/// Renewal only extends a live lease; expiry is judged with the reclaimer's
/// clock minus the tolerated skew, and the reclaim happens exactly once.
fn leases_renew_only_while_unexpired_and_expire_past_the_clock_skew(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let owner = fx::dispatcher(slots(&s), "renew", 30);
    let (env, inv) = ready_slot(&s, &owner);
    let req = slot_request(&env, &inv, &owner, now(), 10);
    let lease_id = req.lease.id.clone();
    slots(&s).acquire(req.clone()).unwrap();
    let ttl = Duration::seconds(10);

    assert!(
        slots(&s)
            .renew_lease(&lease_id, &owner, req.lease.epoch, ttl, at(5))
            .unwrap()
    );
    assert_eq!(
        slots(&s).get_lease(&lease_id).unwrap().unwrap().expires_at,
        Some(at(15))
    );
    let other = fx::dispatcher(slots(&s), "other", 3600);
    assert!(
        !slots(&s)
            .renew_lease(&lease_id, &other, req.lease.epoch, ttl, at(6))
            .unwrap(),
        "only the owner renews"
    );
    assert!(
        !slots(&s)
            .renew_lease(&lease_id, &owner, req.lease.epoch + 1, ttl, at(6))
            .unwrap(),
        "only at the lease's epoch"
    );

    // A reclaimer whose clock reads 16 s (1 s past expiry, inside the 2 s
    // skew) must not reclaim: the owner's clock may be behind.
    assert!(reclaim(&s, &other, at(16)).is_empty());
    // The owner's own renewal at 16 s is too late: an expired lease is never
    // revived, even though nobody reclaimed it yet.
    assert!(
        !slots(&s)
            .renew_lease(&lease_id, &owner, req.lease.epoch, ttl, at(16))
            .unwrap()
    );
    assert_eq!(
        slots(&s).heartbeat(&owner, ttl, at(16)).unwrap(),
        HeartbeatOutcome::Renewed { leases: 0 },
        "the dispatcher itself is still live; its expired slot lease is not renewed"
    );

    // Past expiry + skew: reclaimed, once.
    let first = reclaim(&s, &other, at(17));
    assert_eq!(first.leases, 1);
    assert_eq!(first.fenced.len(), 1);
    assert!(
        first.dispatchers.is_empty(),
        "the owner's own lease is fine"
    );
    assert!(reclaim(&s, &other, at(18)).is_empty(), "exactly once");
    let lease = slots(&s).get_lease(&lease_id).unwrap().unwrap();
    assert_eq!(lease.released_at, Some(at(17)));
}

fn reclaim_happens_once_fences_and_only_a_confirmed_terminate_settles(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let dead = fx::dispatcher(slots(&s), "dead", 10);
    let (busy_env, running_inv) = ready_slot(&s, &dead);
    let req = slot_request(&busy_env, &running_inv, &dead, now(), 10);
    slots(&s).acquire(req.clone()).unwrap();
    // An accepted invocation that never reached a slot, and a pooled one.
    let mut queued = fx::invocation(&busy_env.tenant_id, &FunctionId::generate(), None);
    queued.dispatcher_id = Some(dead.clone());
    invs(&s).insert(queued.clone()).unwrap();
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let mut idle = fx::ready_environment(&key).owned_by(dead.clone());
    idle.assign(now()).unwrap();
    idle.mark_idle(now()).unwrap();
    envs(&s).insert(idle.clone()).unwrap();

    let reclaimer = fx::dispatcher(slots(&s), "reclaimer", 3600);
    assert!(
        reclaim(&s, &reclaimer, at(5)).is_empty(),
        "nothing expired yet"
    );
    let report = reclaim(&s, &reclaimer, at(13));
    assert_eq!(report.dispatchers, vec![dead.clone()]);
    assert_eq!(report.leases, 1);
    assert_eq!(report.invocations, 2);
    assert_eq!(report.attempts, 1);
    assert_eq!(report.fenced.len(), 2);
    assert!(reclaim(&s, &reclaimer, at(14)).is_empty(), "exactly once");

    match invs(&s).get(&running_inv.id).unwrap().unwrap().status {
        InvocationStatus::OutcomeUnknown { error } => {
            assert_eq!(error.error_type, HOST_LEASE_EXPIRED)
        }
        other => panic!("{other:?}"),
    }
    match invs(&s).get(&queued.id).unwrap().unwrap().status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::PlatformError);
            assert_eq!(error.error_type, HOST_LEASE_EXPIRED);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        invs(&s)
            .get_attempt(&req.attempt.id)
            .unwrap()
            .unwrap()
            .status,
        AttemptStatus::OutcomeUnknown { .. }
    ));

    // Fenced: draining, one epoch further, out of every pool, not acquirable.
    let fenced = envs(&s).get(&busy_env.id).unwrap().unwrap();
    assert!(fenced.is_fenced());
    assert_eq!(fenced.state, EnvironmentState::Draining);
    assert_eq!(fenced.epoch, req.env.epoch + 1);
    let fenced_idle = envs(&s).get(&idle.id).unwrap().unwrap();
    assert!(fenced_idle.is_fenced());
    assert_eq!(fenced_idle.epoch, 2);
    assert!(slots(&s).list_idle(Some(&dead)).unwrap().is_empty());
    assert!(
        slots(&s)
            .claim_for_reuse(&key, Some(&dead), at(15))
            .unwrap()
            .is_none()
    );
    assert_eq!(slots(&s).list_fenced().unwrap().len(), 2);
    let mut again = fx::invocation(&busy_env.tenant_id, &FunctionId::generate(), None);
    again.dispatcher_id = Some(dead.clone());
    invs(&s).insert(again.clone()).unwrap();
    let mut reuse = fenced.clone();
    reuse.fenced_at = None;
    reuse.state = EnvironmentState::Ready;
    let request = slot_request(&reuse, &again, &dead, at(15), 10);
    assert!(
        matches!(
            slots(&s).acquire(request),
            Err(RepoError::Refused(_)) | Ok(AcquireOutcome::Lost(_))
        ),
        "expiry alone never makes a fenced environment acquirable"
    );

    // Only a confirmed terminate, at the fenced epoch, settles it.
    assert!(
        !slots(&s)
            .confirm_terminated(&busy_env.id, fenced.epoch - 1, at(16))
            .unwrap()
    );
    assert!(
        slots(&s)
            .confirm_terminated(&busy_env.id, fenced.epoch, at(16))
            .unwrap()
    );
    assert!(
        !slots(&s)
            .confirm_terminated(&busy_env.id, fenced.epoch, at(16))
            .unwrap(),
        "settled once"
    );
    assert!(matches!(
        envs(&s).get(&busy_env.id).unwrap().unwrap().state,
        EnvironmentState::Lost { .. }
    ));
    assert_eq!(slots(&s).list_fenced().unwrap().len(), 1);
}

fn a_live_dispatcher_is_never_reclaimed_and_a_stopped_one_is_at_once(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let live = fx::dispatcher(slots(&s), "live", 30);
    let (env, inv) = ready_slot(&s, &live);
    slots(&s)
        .acquire(slot_request(&env, &inv, &live, now(), 30))
        .unwrap();
    let second = fx::dispatcher(slots(&s), "second", 30);
    // A second gateway on the same store, well within the first one's lease.
    assert!(reclaim(&s, &second, at(20)).is_empty());
    assert_eq!(
        invs(&s).get(&inv.id).unwrap().unwrap().status,
        InvocationStatus::Running
    );
    assert_eq!(
        envs(&s).get(&env.id).unwrap().unwrap().state,
        EnvironmentState::Busy
    );
    // The reclaimer never reclaims itself, however late its own lease is.
    let (own_env, own_inv) = ready_slot(&s, &second);
    slots(&s)
        .acquire(slot_request(&own_env, &own_inv, &second, now(), 1))
        .unwrap();
    assert!(reclaim(&s, &second, at(25)).is_empty());

    // A proven-dead previous incarnation is reclaimed without waiting.
    let report = slots(&s)
        .reclaim_expired(ReclaimRequest {
            reclaimer: second.clone(),
            now: at(21),
            skew: Duration::seconds(2),
            presumed_dead: vec![live.clone()],
        })
        .unwrap();
    assert_eq!(report.dispatchers, vec![live.clone()]);
    match invs(&s).get(&inv.id).unwrap().unwrap().status {
        InvocationStatus::OutcomeUnknown { error } => {
            assert_eq!(error.error_type, HOST_RESTARTED)
        }
        other => panic!("{other:?}"),
    }

    // A dispatcher that stopped gracefully: at once.
    let stopped = fx::dispatcher(slots(&s), "stopped", 3600);
    let mut left = fx::invocation(&TenantId::generate(), &FunctionId::generate(), None);
    left.dispatcher_id = Some(stopped.clone());
    invs(&s).insert(left.clone()).unwrap();
    slots(&s).stop_dispatcher(&stopped, at(1)).unwrap();
    let third = fx::dispatcher(slots(&s), "third", 3600);
    let report = reclaim(&s, &third, at(1));
    assert!(report.dispatchers.contains(&stopped));
    assert!(
        invs(&s)
            .get(&left.id)
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
}

fn a_fenced_dispatcher_can_neither_renew_nor_acquire(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let ttl = Duration::seconds(10);
    let owner = fx::dispatcher(slots(&s), "fenced", 10);
    assert_eq!(
        slots(&s).heartbeat(&owner, ttl, at(5)).unwrap(),
        HeartbeatOutcome::Renewed { leases: 0 }
    );
    let reclaimer = fx::dispatcher(slots(&s), "reclaimer", 3600);
    // Renewed at 5 s to 15 s; past that plus the 2 s skew it is reclaimed.
    assert_eq!(
        reclaim(&s, &reclaimer, at(18)).dispatchers,
        vec![owner.clone()]
    );
    assert_eq!(
        slots(&s).heartbeat(&owner, ttl, at(14)).unwrap(),
        HeartbeatOutcome::Fenced,
        "a reclaimed dispatcher is fenced even if its own clock is behind"
    );
    let (env, inv) = ready_slot(&s, &owner);
    assert!(matches!(
        slots(&s)
            .acquire(slot_request(&env, &inv, &owner, at(14), 10))
            .unwrap(),
        AcquireOutcome::Lost(_)
    ));

    // Expired but not reclaimed yet: still no renewal.
    let late = fx::dispatcher(slots(&s), "late", 10);
    assert_eq!(
        slots(&s).heartbeat(&late, ttl, at(10)).unwrap(),
        HeartbeatOutcome::Fenced
    );
    assert_eq!(
        slots(&s)
            .get_dispatcher(&late)
            .unwrap()
            .unwrap()
            .lease_expires_at,
        at(10)
    );
}

fn idempotency_bindings_expire_after_their_invocation_finished(make: fn(Limits) -> Store) {
    let retention = Some(Duration::hours(1));
    let s: Store = match make(Limits::default()).backend() {
        "memory" => {
            Arc::new(InMemoryStore::new(Limits::default()).with_idempotency_retention(retention))
        }
        _ => Arc::new(
            SqliteStore::open_volatile(
                Limits::default(),
                SqliteOptions {
                    idempotency_retention: retention,
                    ..SqliteOptions::default()
                },
            )
            .unwrap(),
        ),
    };
    let t = TenantId::generate();
    let f = FunctionId::generate();
    let inv = fx::invocation(&t, &f, Some("key"));
    assert_eq!(
        s.insert_bound(inv.clone()).unwrap(),
        IdempotencyOutcome::Inserted
    );

    // In flight, a binding never expires.
    let in_flight = s.lookup(&t, &f, "key", at(7200)).unwrap().unwrap();
    assert_eq!(in_flight.expires_at, None);
    assert_eq!(s.purge_expired_idempotency(at(7200)).unwrap(), 0);

    // Finished at +10 min: it answers for one more hour.
    let mut done = inv.clone();
    done.mark_running(
        tachyon_serverless_domain::AttemptId::generate(),
        at(900),
        at(0),
        at(0),
    )
    .unwrap();
    done.mark_succeeded(None, None, at(600)).unwrap();
    invs(&s).update(done).unwrap();
    let binding = s.lookup(&t, &f, "key", at(1800)).unwrap().unwrap();
    assert_eq!(binding.invocation_id, inv.id);
    assert_eq!(binding.input_digest, inv.input_digest);
    assert_eq!(binding.expires_at, Some(at(4200)));
    assert_eq!(s.lookup(&t, &f, "key", at(4200)).unwrap(), None);

    // A new request with the key after expiry is a new invocation.
    let mut fresh = fx::invocation(&t, &f, Some("key"));
    fresh.accepted_at = at(4300);
    assert_eq!(
        s.insert_bound(fresh.clone()).unwrap(),
        IdempotencyOutcome::Inserted
    );
    assert_eq!(
        s.lookup(&t, &f, "key", at(4300))
            .unwrap()
            .unwrap()
            .invocation_id,
        fresh.id
    );

    // Expired bindings are purged; live ones are not.
    let other = fx::invocation(&t, &f, Some("other"));
    s.insert_bound(other.clone()).unwrap();
    let mut other_done = other.clone();
    other_done
        .mark_running(
            tachyon_serverless_domain::AttemptId::generate(),
            at(900),
            at(0),
            at(0),
        )
        .unwrap();
    other_done
        .mark_failed(
            InvocationError::new(ErrorClass::UserError, "E", "no"),
            at(0),
        )
        .unwrap();
    invs(&s).update(other_done).unwrap();
    assert_eq!(s.purge_expired_idempotency(at(3599)).unwrap(), 0);
    assert_eq!(s.purge_expired_idempotency(at(3600)).unwrap(), 1);
    assert_eq!(s.lookup(&t, &f, "other", at(0)).unwrap(), None);
    assert!(s.lookup(&t, &f, "key", at(4300)).unwrap().is_some());
}

fn the_pool_only_hands_out_and_sweeps_its_owners_environments(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let a = fx::dispatcher(slots(&s), "a", 3600);
    let b = fx::dispatcher(slots(&s), "b", 3600);
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let mut env = fx::ready_environment(&key).owned_by(a.clone());
    env.assign(now()).unwrap();
    env.mark_idle(now()).unwrap();
    envs(&s).insert(env.clone()).unwrap();

    assert!(slots(&s).list_idle(Some(&b)).unwrap().is_empty());
    assert!(slots(&s).list_idle(None).unwrap().is_empty());
    assert!(
        slots(&s)
            .claim_for_reuse(&key, Some(&b), now())
            .unwrap()
            .is_none(),
        "another dispatcher holds no session for it"
    );
    assert_eq!(slots(&s).list_idle(Some(&a)).unwrap().len(), 1);
    assert_eq!(
        slots(&s)
            .claim_for_reuse(&key, Some(&a), now())
            .unwrap()
            .map(|e| e.id),
        Some(env.id)
    );
}
