//! The repository contract, run against both stores (docs/adr/0003 A9,
//! PLT-4618): creation, restore, state transitions, terminal rows that
//! cannot be rewritten, alias CAS races, tenant boundaries, duplicate ids,
//! revision tampering, bounded bodies and the environment pool.
//!
//! Every test is a function over `&Arc<dyn StateStore>`; `contract!` runs it
//! once on [`InMemoryStore`] and once on a volatile [`SqliteStore`].

use std::sync::Arc;

use tachyon_serverless_domain::{
    AttemptStatus, BootEvidence, EnvironmentState, ErrorClass, ExecutionEnvironment,
    ExecutionLease, FunctionAlias, FunctionRevision, Invocation, InvocationError, InvocationId,
    InvocationStatus, LeaseId, Limits, LogPhase, LogRecord, LogStream, PayloadRef, ReuseKey,
    RevisionId, RevisionStatus, Sha256Digest, TenantId,
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

macro_rules! contract {
    ($($test:ident),* $(,)?) => {
        mod memory {
            $( #[test] fn $test() { super::$test(super::memory, ) } )*
        }
        mod sqlite {
            $( #[test] fn $test() { super::$test(super::sqlite, ) } )*
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
    only_an_exactly_matching_reuse_key_is_reused,
    nothing_is_dispatched_before_ready_and_busy_is_never_handed_out,
    concurrent_claims_never_hand_the_same_environment_to_two_callers,
    releasing_respects_the_pool_caps_and_refuses_a_stale_copy,
    a_ready_environment_is_not_in_the_pool_and_is_never_claimed,
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

    let lease = ExecutionLease::acquire(
        LeaseId::generate(),
        env.id.clone(),
        att.id.clone(),
        t.clone(),
        1,
        now(),
        now(),
    );
    envs(&s).insert_lease(lease.clone()).unwrap();
    assert!(is_conflict(envs(&s).insert_lease(lease)), "lease id");
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

    let mut lease =
        ExecutionLease::acquire(LeaseId::generate(), env, att.id.clone(), t, 1, now(), now());
    envs(&s).insert_lease(lease.clone()).unwrap();
    let held = lease.clone();
    lease.release(now()).unwrap();
    envs(&s).update_lease(lease.clone()).unwrap();
    assert!(
        is_refused(envs(&s).update_lease(held)),
        "a released lease is never revived"
    );
    assert_eq!(envs(&s).get_lease(&lease.id).unwrap().unwrap(), lease);
    assert!(!lease.accepts(&att.id, 1));
}

fn an_environment_copy_from_another_epoch_is_refused(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let mut env = fx::ready_environment(&key);
    env.mark_busy(now()).unwrap();
    env.mark_idle(now()).unwrap();
    envs(&s).insert(env.clone()).unwrap();
    let before_claim = env.clone();

    let claimed = envs(&s).claim_for_reuse(&key, now()).unwrap().unwrap();
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
    let env_b = fx::environment(&fx::key(&b, &rb.id));
    envs(&s).insert(env_b.clone()).unwrap();
    let lease = ExecutionLease::acquire(
        LeaseId::generate(),
        env_b.id.clone(),
        AttemptId::generate(),
        a.clone(),
        1,
        now(),
        now(),
    );
    assert!(is_refused(envs(&s).insert_lease(lease)), "lease");

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
    assert_eq!(s.lookup(&t, &f, "k").unwrap(), None);
    let first = fx::invocation(&t, &f, Some("k"));
    let first_id = first.id.clone();
    assert_eq!(s.insert_bound(first).unwrap(), IdempotencyOutcome::Inserted);
    let binding = IdempotencyBinding {
        invocation_id: first_id.clone(),
        input_digest: Sha256Digest::of_bytes(b"{}"),
    };
    assert_eq!(s.lookup(&t, &f, "k").unwrap(), Some(binding.clone()));
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
    let rec = |line: &str| LogRecord {
        tenant_id: TenantId::generate(),
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
    let q = s.query(&inv);
    assert_eq!(q.records.len(), 2);
    assert!(q.dropped);
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
    env.mark_busy(now()).unwrap();
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
            envs(&s).claim_for_reuse(&other, now()).unwrap().is_none(),
            "a differing {name} must never reuse the environment"
        );
    }
    // Nothing above touched the pooled environment.
    assert_eq!(envs(&s).list_idle().unwrap().len(), 1);
    let claimed = envs(&s)
        .claim_for_reuse(&key, now())
        .unwrap()
        .expect("the exact key hits");
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.state, EnvironmentState::Busy);
    assert_eq!(claimed.epoch, 2, "a reassignment advances the epoch");
    assert!(
        envs(&s).claim_for_reuse(&key, now()).unwrap().is_none(),
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
    busy.mark_busy(now()).unwrap();
    let busy_id = busy.id.clone();
    let mut draining = fx::ready_environment(&key);
    draining.mark_busy(now()).unwrap();
    draining.mark_idle(now()).unwrap();
    draining.mark_draining(now()).unwrap();
    let mut stopped = fx::ready_environment(&key);
    stopped.mark_stopped(now()).unwrap();

    for env in [fresh(), provisioning, initializing, busy, draining, stopped] {
        envs(&s).insert(env).unwrap();
    }
    assert!(
        envs(&s).claim_for_reuse(&key, now()).unwrap().is_none(),
        "only an environment that reported ready and is free may be handed out"
    );
    assert_eq!(
        envs(&s).get(&busy_id).unwrap().unwrap().epoch,
        1,
        "a refused claim never advances an epoch"
    );
    assert!(envs(&s).list_idle().unwrap().is_empty());

    // The same key hits as soon as one is actually pooled.
    let id = pooled(&s, &key);
    assert_eq!(
        envs(&s).claim_for_reuse(&key, now()).unwrap().map(|e| e.id),
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
                    envs(&s).claim_for_reuse(&key, now()).unwrap()
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
            assert_eq!(w.state, EnvironmentState::Busy, "{case}");
            assert_eq!(w.epoch, 2, "{case}: every reassignment advances the epoch");
        }
        assert_eq!(
            envs(&s).list_idle().unwrap().len(),
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
        envs(&s)
            .release_to_pool(&busy[0], POOL, now())
            .unwrap()
            .is_some()
    );
    assert!(
        envs(&s)
            .release_to_pool(&busy[1], POOL, now())
            .unwrap()
            .is_some()
    );
    assert!(
        envs(&s)
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
        envs(&s)
            .release_to_pool(&busy[0], POOL, now())
            .unwrap()
            .is_none(),
        "the row is Idle now, not Busy"
    );
    let mut wrong_epoch = busy[2].clone();
    wrong_epoch.epoch = 99;
    assert!(
        envs(&s)
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
            envs(&s)
                .release_to_pool(&env, POOL, now())
                .unwrap()
                .is_some()
        );
    }
    let mut env = fx::ready_environment(&other_key);
    env.mark_busy(now()).unwrap();
    envs(&s).insert(env.clone()).unwrap();
    assert!(
        envs(&s)
            .release_to_pool(&env, POOL, now())
            .unwrap()
            .is_none(),
        "max_total_idle is enforced across keys"
    );
    assert_eq!(envs(&s).list_idle().unwrap().len(), 4);
}

/// Regression (PLT-4632 review F1): pool membership is `Idle` only.
fn a_ready_environment_is_not_in_the_pool_and_is_never_claimed(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let ready = fx::ready_environment(&key);
    let ready_id = ready.id.clone();
    envs(&s).insert(ready).unwrap();

    assert!(
        envs(&s).list_idle().unwrap().is_empty(),
        "a Ready environment is not pool membership"
    );
    assert!(
        envs(&s).claim_for_reuse(&key, now()).unwrap().is_none(),
        "an environment that was never released to the pool is never claimed"
    );
    let untouched = envs(&s).get(&ready_id).unwrap().unwrap();
    assert_eq!(untouched.state, EnvironmentState::Ready);
    assert_eq!(
        untouched.epoch, 1,
        "a refused claim never advances an epoch"
    );

    let pooled_id = pooled(&s, &key);
    assert_eq!(
        envs(&s).claim_for_reuse(&key, now()).unwrap().map(|e| e.id),
        Some(pooled_id)
    );
}

fn taking_an_idle_environment_for_termination_excludes_a_claim(make: fn(Limits) -> Store) {
    let s = make(Limits::default());
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let id = pooled(&s, &key);

    assert!(envs(&s).take_idle_for_termination(&id, now()).unwrap());
    assert!(
        !envs(&s).take_idle_for_termination(&id, now()).unwrap(),
        "the second caller loses"
    );
    assert!(
        envs(&s).claim_for_reuse(&key, now()).unwrap().is_none(),
        "an environment the sweeper owns is never handed to an attempt"
    );
    assert_eq!(
        envs(&s).get(&id).unwrap().unwrap().state,
        EnvironmentState::Draining
    );

    // The other way round: a claimed environment cannot be swept.
    let claimed = pooled(&s, &key);
    assert!(envs(&s).claim_for_reuse(&key, now()).unwrap().is_some());
    assert!(!envs(&s).take_idle_for_termination(&claimed, now()).unwrap());
}
