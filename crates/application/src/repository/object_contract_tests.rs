//! Contract of [`ObjectReferenceRepository`] (PLT-4638), run on both stores:
//! references never cross a tenant, a non-terminal reference always protects
//! an object, and an attach racing a collection claim never leaves an
//! invocation pointing at a collected object.

use std::sync::Arc;

use tachyon_serverless_domain::{AttemptId, Invocation, Limits, TenantId};
use tachyon_serverless_durable_port::{ObjectId, ObjectRef, ObjectScope, Region};

use super::contract_tests::fx::{self, now};
use super::*;

type Store = Arc<dyn StateStore>;

fn memory() -> Store {
    Arc::new(InMemoryStore::new(Limits::default()))
}

fn sqlite() -> Store {
    Arc::new(SqliteStore::open_volatile(Limits::default(), SqliteOptions::default()).unwrap())
}

macro_rules! contract {
    ($($test:ident),* $(,)?) => {
        mod memory {
            $( #[test] fn $test() { super::$test(super::memory) } )*
        }
        mod sqlite {
            $( #[test] fn $test() { super::$test(super::sqlite) } )*
        }
    };
}

contract!(
    an_object_reference_never_crosses_a_tenant,
    a_non_terminal_reference_protects_an_expired_object,
    orphans_are_only_collected_when_nothing_ever_referenced_them,
    an_attach_after_a_collection_claim_is_refused,
);

fn object_ref(tenant: &TenantId) -> ObjectRef {
    ObjectRef {
        id: ObjectId::generate(),
        scope: ObjectScope {
            tenant_id: tenant.clone(),
            region: Region::parse("local").unwrap(),
        },
    }
}

fn running_invocation(s: &Store, tenant: &TenantId) -> Invocation {
    let f = fx::function(
        tenant,
        &format!("f{}", &ObjectId::generate().as_str()[20..]),
    );
    FunctionRepository::insert(s.as_ref(), f.clone()).unwrap();
    let inv = fx::invocation(tenant, &f.id, None);
    InvocationRepository::insert(s.as_ref(), inv.clone()).unwrap();
    inv
}

fn finish(s: &Store, mut inv: Invocation) {
    inv.mark_queued().unwrap();
    InvocationRepository::update(s.as_ref(), inv.clone()).unwrap();
    inv.mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    InvocationRepository::update(s.as_ref(), inv.clone()).unwrap();
    inv.mark_succeeded(None, Some(200), now()).unwrap();
    InvocationRepository::update(s.as_ref(), inv).unwrap();
}

fn an_object_reference_never_crosses_a_tenant(make: fn() -> Store) {
    let s = make();
    let (a, b) = (TenantId::generate(), TenantId::generate());
    let inv_b = running_invocation(&s, &b);
    let obj_a = object_ref(&a);
    let err = s.attach_object(&obj_a, &inv_b.id, now()).unwrap_err();
    assert!(matches!(err, RepoError::Refused(_)), "{err}");
    assert!(s.object_references(&obj_a.id).unwrap().is_empty());
    // And the refused reference does not protect the object.
    assert_eq!(
        s.claim_for_collection(&obj_a, CollectReason::Orphan, now())
            .unwrap(),
        CollectDecision::Collect
    );
    // An unknown invocation is NotFound.
    let err = s
        .attach_object(
            &object_ref(&a),
            &fx::invocation(&a, &fx::function(&a, "x").id, None).id,
            now(),
        )
        .unwrap_err();
    assert!(matches!(err, RepoError::NotFound(_)), "{err}");
}

fn a_non_terminal_reference_protects_an_expired_object(make: fn() -> Store) {
    let s = make();
    let t = TenantId::generate();
    let inv = running_invocation(&s, &t);
    let obj = object_ref(&t);
    s.attach_object(&obj, &inv.id, now()).unwrap();
    s.attach_object(&obj, &inv.id, now()).unwrap(); // idempotent
    assert_eq!(s.object_references(&obj.id).unwrap(), vec![inv.id.clone()]);
    for reason in [CollectReason::Expired, CollectReason::Orphan] {
        assert_eq!(
            s.claim_for_collection(&obj, reason, now()).unwrap(),
            CollectDecision::InUse { invocations: 1 },
            "{reason:?}"
        );
    }
    // Once the invocation is terminal the TTL decides; it is no orphan.
    finish(&s, inv);
    assert_eq!(
        s.claim_for_collection(&obj, CollectReason::Orphan, now())
            .unwrap(),
        CollectDecision::Referenced
    );
    assert_eq!(
        s.claim_for_collection(&obj, CollectReason::Expired, now())
            .unwrap(),
        CollectDecision::Collect
    );
    s.forget_object(&obj.id).unwrap();
    assert!(s.object_references(&obj.id).unwrap().is_empty());
}

fn orphans_are_only_collected_when_nothing_ever_referenced_them(make: fn() -> Store) {
    let s = make();
    let t = TenantId::generate();
    let orphan = object_ref(&t);
    assert_eq!(
        s.claim_for_collection(&orphan, CollectReason::Orphan, now())
            .unwrap(),
        CollectDecision::Collect
    );
    // A second pass (files not deleted yet) still answers Collect.
    assert_eq!(
        s.claim_for_collection(&orphan, CollectReason::Orphan, now())
            .unwrap(),
        CollectDecision::Collect
    );
}

fn an_attach_after_a_collection_claim_is_refused(make: fn() -> Store) {
    let s = make();
    let t = TenantId::generate();
    let inv = running_invocation(&s, &t);
    let obj = object_ref(&t);
    assert_eq!(
        s.claim_for_collection(&obj, CollectReason::Orphan, now())
            .unwrap(),
        CollectDecision::Collect
    );
    let err = s.attach_object(&obj, &inv.id, now()).unwrap_err();
    assert!(matches!(err, RepoError::Refused(_)), "{err}");

    // Racing threads: whatever the interleaving, an attach that succeeded
    // means the claim did not collect, and a collect means the attach failed.
    for _ in 0..20 {
        let obj = object_ref(&t);
        let (s1, s2) = (s.clone(), s.clone());
        let (o1, o2) = (obj.clone(), obj.clone());
        let id = inv.id.clone();
        let attach = std::thread::spawn(move || s1.attach_object(&o1, &id, now()).is_ok());
        let claim = std::thread::spawn(move || {
            s2.claim_for_collection(&o2, CollectReason::Orphan, now())
                .unwrap()
        });
        let attached = attach.join().unwrap();
        let decision = claim.join().unwrap();
        assert!(
            attached != (decision == CollectDecision::Collect),
            "attached={attached} decision={decision:?}"
        );
    }
}
