//! Object store, retention / GC and configuration tests (PLT-4638).

use std::sync::Arc;
use std::time::Duration;

use chrono::TimeZone;
use tachyon_serverless_domain::{
    AttemptId, Clock, FixedClock, FunctionId, FunctionName, Invocation, Limits, TenantId, Timestamp,
};
use tachyon_serverless_durable_port::{ObjectError, ObjectScope, ObjectStore, PutObject, Region};

use super::*;
use crate::repository::contract_tests::fx;
use crate::repository::{
    FunctionRepository, InvocationRepository, SqliteOptions, SqliteStore, StateStore,
};

fn t0() -> Timestamp {
    chrono::Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap()
}

fn tenant_a() -> TenantId {
    TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzza").unwrap()
}

fn tenant_b() -> TenantId {
    TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzzb").unwrap()
}

fn scope(t: &TenantId, region: &str) -> ObjectScope {
    ObjectScope {
        tenant_id: t.clone(),
        region: Region::parse(region).unwrap(),
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    clock: Arc<FixedClock>,
    key_hex: String,
    store: FsObjectStore,
}

fn options() -> FsObjectOptions {
    FsObjectOptions {
        regions: vec![
            Region::parse("local").unwrap(),
            Region::parse("local-2").unwrap(),
        ],
        max_object_bytes: 1024,
        tenant_quota_bytes: 3000,
        default_ttl: Some(Duration::from_secs(3600)),
    }
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(FixedClock::new(t0()));
    let key_hex = "42".repeat(32);
    let store = FsObjectStore::open(
        &dir.path().join("objects"),
        ObjectKey::from_hex(&key_hex).unwrap(),
        options(),
        clock.clone(),
    )
    .unwrap();
    Fixture {
        dir,
        clock,
        key_hex,
        store,
    }
}

impl Fixture {
    fn reopen(&self) -> FsObjectStore {
        FsObjectStore::open(
            &self.dir.path().join("objects"),
            ObjectKey::from_hex(&self.key_hex).unwrap(),
            options(),
            self.clock.clone(),
        )
        .unwrap()
    }
}

fn put(bytes: &[u8], t: &TenantId) -> PutObject {
    PutObject {
        scope: scope(t, "local"),
        bytes: bytes.to_vec(),
        ttl: None,
    }
}

#[tokio::test]
async fn objects_round_trip_encrypted_and_survive_a_reopen() {
    let f = fixture();
    let meta = f
        .store
        .put(put(b"large input payload", &tenant_a()))
        .await
        .unwrap();
    assert_eq!(meta.size_bytes, 19);
    assert_eq!(meta.encryption.algorithm, "AES-256-GCM");
    assert_eq!(meta.encryption.key_id, f.store.key_id());
    assert_eq!(meta.expires_at, Some(t0() + chrono::Duration::hours(1)));
    let (data, meta_path) = f.store.paths_of(&meta.reference.scope, &meta.reference.id);
    let raw = std::fs::read(&data).unwrap();
    assert!(
        !raw.windows(19).any(|w| w == b"large input payload"),
        "plaintext is not on disk"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for p in [&data, &meta_path] {
            assert_eq!(
                std::fs::metadata(p).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let dir = data.parent().unwrap();
        assert_eq!(
            std::fs::metadata(dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    let reopened = f.reopen();
    let got = reopened
        .get(&meta.reference.scope, &meta.reference.id)
        .await
        .unwrap();
    assert_eq!(got.bytes, b"large input payload");
    assert_eq!(got.meta, meta);
    assert_eq!(
        reopened
            .head(&meta.reference.scope, &meta.reference.id)
            .await
            .unwrap(),
        meta
    );
    // Another key cannot read it, and says which key it needs.
    let other = FsObjectStore::open(
        &f.dir.path().join("objects"),
        ObjectKey::generate(),
        options(),
        f.clock.clone(),
    )
    .unwrap();
    let err = other
        .get(&meta.reference.scope, &meta.reference.id)
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectError::Encryption(_)), "{err}");
    assert!(err.to_string().contains(&meta.encryption.key_id), "{err}");
}

/// PLT-4638 acceptance: a reference used from another tenant (or another
/// region) is not found, and cannot delete or read the object.
#[tokio::test]
async fn objects_are_invisible_across_tenants() {
    let f = fixture();
    let meta = f
        .store
        .put(put(b"tenant a secret", &tenant_a()))
        .await
        .unwrap();
    let id = &meta.reference.id;
    for wrong in [
        scope(&tenant_b(), "local"),
        scope(&tenant_a(), "local-2"),
        scope(&tenant_a(), "elsewhere"),
    ] {
        assert!(
            matches!(f.store.get(&wrong, id).await, Err(ObjectError::NotFound)),
            "{wrong:?}"
        );
        assert!(
            matches!(f.store.head(&wrong, id).await, Err(ObjectError::NotFound)),
            "{wrong:?}"
        );
        assert!(!f.store.delete(&wrong, id).await.unwrap(), "{wrong:?}");
    }
    // Indistinguishable from an id that never existed.
    let missing = tachyon_serverless_durable_port::ObjectId::generate();
    assert!(matches!(
        f.store.get(&scope(&tenant_b(), "local"), &missing).await,
        Err(ObjectError::NotFound)
    ));
    // A copy of A's files planted into B's directory does not decrypt as B's:
    // the ciphertext is bound to its tenant.
    let (data, meta_path) = f.store.paths_of(&meta.reference.scope, id);
    let b_scope = scope(&tenant_b(), "local");
    let (b_data, b_meta) = f.store.paths_of(&b_scope, id);
    std::fs::create_dir_all(b_data.parent().unwrap()).unwrap();
    std::fs::copy(&data, &b_data).unwrap();
    let mut forged: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    forged["meta"]["reference"]["scope"]["tenant_id"] = tenant_b().as_str().into();
    std::fs::write(&b_meta, serde_json::to_vec(&forged).unwrap()).unwrap();
    let err = f.store.get(&b_scope, id).await.unwrap_err();
    assert!(matches!(err, ObjectError::Integrity(_)), "{err}");
    // A's object is intact.
    assert_eq!(
        f.store.get(&meta.reference.scope, id).await.unwrap().bytes,
        b"tenant a secret"
    );
    // A put for a region the store does not serve is refused, not rerouted.
    let err = f
        .store
        .put(PutObject {
            scope: scope(&tenant_a(), "elsewhere"),
            bytes: b"x".to_vec(),
            ttl: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectError::RegionNotServed(_)), "{err}");
}

/// PLT-4638 acceptance: tampering with stored bytes or with the recorded
/// digest is detected on read and no bytes are returned.
#[tokio::test]
async fn tampered_objects_fail_verification() {
    let f = fixture();
    let meta = f.store.put(put(b"0123456789", &tenant_a())).await.unwrap();
    let (data, meta_path) = f.store.paths_of(&meta.reference.scope, &meta.reference.id);

    let original = std::fs::read(&data).unwrap();
    let mut flipped = original.clone();
    let mid = flipped.len() / 2;
    flipped[mid] ^= 0x01;
    std::fs::write(&data, &flipped).unwrap();
    let err = f
        .store
        .get(&meta.reference.scope, &meta.reference.id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "integrity", "{err}");
    std::fs::write(&data, &original).unwrap();

    let meta_original = std::fs::read(&meta_path).unwrap();
    let mut doc: serde_json::Value = serde_json::from_slice(&meta_original).unwrap();
    doc["meta"]["digest"] = tachyon_serverless_domain::Sha256Digest::of_bytes(b"other")
        .as_str()
        .into();
    std::fs::write(&meta_path, serde_json::to_vec(&doc).unwrap()).unwrap();
    let err = f
        .store
        .get(&meta.reference.scope, &meta.reference.id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "integrity", "{err}");
    std::fs::write(&meta_path, &meta_original).unwrap();

    std::fs::remove_file(&data).unwrap();
    let err = f
        .store
        .get(&meta.reference.scope, &meta.reference.id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "integrity", "{err}");
}

/// PLT-4638 acceptance: over-size objects and over-quota tenants are refused
/// with explicit errors and nothing is stored.
#[tokio::test]
async fn object_size_and_tenant_quota_are_enforced() {
    let f = fixture();
    let err = f
        .store
        .put(put(&[0u8; 1025], &tenant_a()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ObjectError::TooLarge {
                size: 1025,
                max: 1024
            }
        ),
        "{err}"
    );
    assert_eq!(f.store.usage(&tenant_a()).await.unwrap(), 0);
    f.store.put(put(&[0u8; 1024], &tenant_a())).await.unwrap();
    f.store.put(put(&[0u8; 1024], &tenant_a())).await.unwrap();
    // quota 3000: 2048 used, 1024 more would be 3072
    let err = f
        .store
        .put(put(&[0u8; 1024], &tenant_a()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ObjectError::QuotaExceeded {
                used: 2048,
                requested: 1024,
                quota: 3000
            }
        ),
        "{err}"
    );
    assert_eq!(err.code(), "quota_exceeded");
    // quota counts every region of the tenant
    let err = f
        .store
        .put(PutObject {
            scope: scope(&tenant_a(), "local-2"),
            bytes: vec![0u8; 1000],
            ttl: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectError::QuotaExceeded { .. }), "{err}");
    f.store.put(put(&[0u8; 952], &tenant_a())).await.unwrap();
    assert_eq!(f.store.usage(&tenant_a()).await.unwrap(), 3000);
    // other tenants are unaffected
    f.store.put(put(&[0u8; 1024], &tenant_b())).await.unwrap();
    assert_eq!(f.store.usage(&tenant_b()).await.unwrap(), 1024);
}

// ---------------------------------------------------------------------------
// retention / GC
// ---------------------------------------------------------------------------

struct GcFixture {
    f: Fixture,
    ledger: Arc<SqliteStore>,
    gc: ObjectGc,
}

fn gc_fixture() -> GcFixture {
    let f = fixture();
    let ledger = Arc::new(
        SqliteStore::open(
            &f.dir.path().join("ledger"),
            Limits::default(),
            SqliteOptions::default(),
            t0(),
        )
        .unwrap(),
    );
    let gc = ObjectGc::new(
        Arc::new(f.store.clone()),
        ledger.clone(),
        f.clock.clone(),
        chrono::Duration::minutes(10),
    );
    GcFixture { f, ledger, gc }
}

fn invocation(ledger: &SqliteStore, t: &TenantId) -> Invocation {
    let func = tachyon_serverless_domain::Function::new(
        FunctionId::generate(),
        t.clone(),
        FunctionName::parse(&format!(
            "gc-{}",
            &tachyon_serverless_durable_port::ObjectId::generate().as_str()[22..]
        ))
        .unwrap(),
        String::new(),
        t0(),
    )
    .unwrap();
    FunctionRepository::insert(ledger, func.clone()).unwrap();
    let inv = fx::invocation(t, &func.id, None);
    InvocationRepository::insert(ledger, inv.clone()).unwrap();
    inv
}

fn finish(ledger: &SqliteStore, mut inv: Invocation) {
    let now = fx::now();
    inv.mark_queued().unwrap();
    InvocationRepository::update(ledger, inv.clone()).unwrap();
    inv.mark_running(AttemptId::generate(), now, now, now)
        .unwrap();
    InvocationRepository::update(ledger, inv.clone()).unwrap();
    inv.mark_succeeded(None, Some(200), now).unwrap();
    InvocationRepository::update(ledger, inv).unwrap();
}

/// PLT-4638 acceptance: retention never removes an object a non-terminal
/// invocation references, even long after its TTL; once the invocation is
/// terminal the TTL applies.
#[tokio::test]
async fn gc_never_collects_objects_of_unfinished_invocations() {
    let g = gc_fixture();
    let t = tenant_a();
    let inv = invocation(&g.ledger, &t);
    let input =
        g.f.store
            .put(PutObject {
                ttl: Some(Duration::from_secs(60)),
                ..put(b"input", &t)
            })
            .await
            .unwrap();
    g.ledger
        .attach_object(&input.reference, &inv.id, g.f.clock.now())
        .unwrap();
    let unreferenced =
        g.f.store
            .put(PutObject {
                ttl: Some(Duration::from_secs(60)),
                ..put(b"loose", &t)
            })
            .await
            .unwrap();

    g.f.clock.advance(chrono::Duration::days(30));
    let report = g.gc.run().await;
    assert_eq!(report.kept_in_use, 1, "{report:?}");
    assert_eq!(report.collected_expired, 1, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    let s = &input.reference.scope;
    assert_eq!(
        g.f.store.get(s, &input.reference.id).await.unwrap().bytes,
        b"input"
    );
    assert!(matches!(
        g.f.store.get(s, &unreferenced.reference.id).await,
        Err(ObjectError::NotFound)
    ));

    finish(&g.ledger, inv);
    let report = g.gc.run().await;
    assert_eq!(report.collected_expired, 1, "{report:?}");
    assert!(matches!(
        g.f.store.get(s, &input.reference.id).await,
        Err(ObjectError::NotFound)
    ));
    assert!(
        g.ledger
            .object_references(&input.reference.id)
            .unwrap()
            .is_empty()
    );
}

/// Orphans (never referenced) go after the grace period, not before; an
/// object referenced by a finished invocation is not an orphan and waits for
/// its TTL.
#[tokio::test]
async fn gc_collects_orphans_only_after_the_grace_period() {
    let g = gc_fixture();
    let t = tenant_a();
    let orphan =
        g.f.store
            .put(PutObject {
                ttl: Some(Duration::from_secs(86_400)),
                ..put(b"orphan", &t)
            })
            .await
            .unwrap();
    let inv = invocation(&g.ledger, &t);
    let used =
        g.f.store
            .put(PutObject {
                ttl: Some(Duration::from_secs(86_400)),
                ..put(b"used", &t)
            })
            .await
            .unwrap();
    g.ledger
        .attach_object(&used.reference, &inv.id, g.f.clock.now())
        .unwrap();
    finish(&g.ledger, inv);

    g.f.clock.advance(chrono::Duration::minutes(5));
    let report = g.gc.run().await;
    assert_eq!(report, GcReport::default(), "nothing is old enough yet");

    g.f.clock.advance(chrono::Duration::minutes(6));
    let report = g.gc.run().await;
    assert_eq!(report.collected_orphans, 1, "{report:?}");
    assert_eq!(report.kept_referenced, 1, "{report:?}");
    let s = &orphan.reference.scope;
    assert!(matches!(
        g.f.store.get(s, &orphan.reference.id).await,
        Err(ObjectError::NotFound)
    ));
    assert!(g.f.store.get(s, &used.reference.id).await.is_ok());
}

/// The put-then-insert race: an object stored for an invocation that is
/// inserted later. Inside the grace period the GC leaves it alone; if the
/// GC claims it first (a client slower than the grace), attaching fails
/// instead of pointing an invocation at a deleted object.
#[tokio::test]
async fn gc_and_attach_race_never_leave_a_dangling_reference() {
    let g = gc_fixture();
    let t = tenant_a();
    // put, GC runs before the invocation exists (within grace): kept
    let early = g.f.store.put(put(b"early", &t)).await.unwrap();
    g.f.clock.advance(chrono::Duration::minutes(1));
    assert_eq!(g.gc.run().await.collected_orphans, 0);
    let inv = invocation(&g.ledger, &t);
    g.ledger
        .attach_object(&early.reference, &inv.id, g.f.clock.now())
        .unwrap();
    g.f.clock.advance(chrono::Duration::hours(2)); // past TTL and grace
    let report = g.gc.run().await;
    assert_eq!(report.kept_in_use, 1, "{report:?}");
    assert!(
        g.f.store
            .get(&early.reference.scope, &early.reference.id)
            .await
            .is_ok()
    );

    // put, then the client stalls past the grace period: GC wins
    let late = g.f.store.put(put(b"late", &t)).await.unwrap();
    g.f.clock.advance(chrono::Duration::minutes(11));
    let report = g.gc.run().await;
    assert_eq!(report.collected_orphans, 1, "{report:?}");
    // The tombstone outlives the files: a late attach is refused.
    let err = g
        .ledger
        .attach_object(&late.reference, &inv.id, g.f.clock.now())
        .unwrap_err();
    assert!(
        matches!(err, crate::repository::RepoError::Refused(_)),
        "{err}"
    );
    assert!(matches!(
        g.f.store
            .head(&late.reference.scope, &late.reference.id)
            .await,
        Err(ObjectError::NotFound)
    ));

    // Interleaved at the ledger: tombstone committed, files not yet deleted.
    let mid = g.f.store.put(put(b"mid", &t)).await.unwrap();
    assert_eq!(
        g.ledger
            .claim_for_collection(
                &mid.reference,
                crate::repository::CollectReason::Orphan,
                g.f.clock.now()
            )
            .unwrap(),
        crate::repository::CollectDecision::Collect
    );
    let err = g
        .ledger
        .attach_object(&mid.reference, &inv.id, g.f.clock.now())
        .unwrap_err();
    assert!(
        matches!(err, crate::repository::RepoError::Refused(_)),
        "{err}"
    );
    // the next pass finishes the collection
    g.f.clock.advance(chrono::Duration::minutes(11));
    let report = g.gc.run().await;
    assert!(report.collected_orphans >= 1, "{report:?}");
    assert!(matches!(
        g.f.store
            .head(&mid.reference.scope, &mid.reference.id)
            .await,
        Err(ObjectError::NotFound)
    ));
}

#[tokio::test]
async fn gc_removes_incomplete_writes_after_the_grace_period() {
    let g = gc_fixture();
    let t = tenant_a();
    let meta = g.f.store.put(put(b"x", &t)).await.unwrap();
    // simulate a crash between data and metadata: drop the metadata
    let (data, meta_path) =
        g.f.store
            .paths_of(&meta.reference.scope, &meta.reference.id);
    std::fs::remove_file(&meta_path).unwrap();
    let tmp = data.parent().unwrap().join(".tmp-obj_leftover.data");
    std::fs::write(&tmp, b"partial").unwrap();
    // mtime is wall clock; move the logical clock past it plus the grace
    g.f.clock
        .set(chrono::Utc::now() + chrono::Duration::hours(1));
    let report = g.gc.run().await;
    assert_eq!(report.incomplete_removed, 2, "{report:?}");
    assert!(!data.exists() && !tmp.exists());
}

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

const BASE: &str = r#"
[provider]
kind = "process"
[provider.process]
bridge_binary = "target/debug/bridge"
workdir = "./data/process"
"#;

#[test]
fn queue_and_objects_are_off_by_default() {
    let cfg = crate::GatewayConfig::from_toml(BASE).unwrap();
    assert_eq!(cfg.queue.backend, QueueBackend::None);
    assert_eq!(cfg.objects.backend, ObjectsBackend::None);
    let durable = build(
        &cfg,
        Arc::new(crate::repository::InMemoryStore::new(Limits::default())),
        Arc::new(FixedClock::new(t0())),
        DurableOverrides::default(),
    )
    .unwrap();
    assert!(durable.queue.is_none() && durable.objects.is_none() && durable.object_gc.is_none());
}

/// PLT-4638 acceptance: the JetStream queue is never configured for an
/// anonymous connection, and the dev-only SQLite queue is refused under
/// production.
#[test]
fn nats_config_refuses_anonymous_connections() {
    let anonymous = format!(
        "{BASE}\n[queue]\nbackend = \"nats\"\n[queue.nats]\nurl = \"nats://127.0.0.1:4222\"\n"
    );
    let err = crate::GatewayConfig::from_toml(&anonymous).unwrap_err();
    assert!(err.to_string().contains("Anonymous"), "{err}");
    let half = format!("{anonymous}user = \"gateway\"\n");
    assert!(crate::GatewayConfig::from_toml(&half).is_err());
    let both =
        format!("{anonymous}user = \"gateway\"\npassword_file = \"p\"\nnkey_seed_file = \"s\"\n");
    assert!(crate::GatewayConfig::from_toml(&both).is_err());
    let ok = format!("{anonymous}user = \"gateway\"\npassword_file = \"secrets/nats.pw\"\n");
    let cfg = crate::GatewayConfig::from_toml(&ok).unwrap();
    assert!(
        cfg.queue
            .nats
            .as_ref()
            .unwrap()
            .password_file
            .as_ref()
            .unwrap()
            .is_absolute()
    );
    let nkey = format!("{anonymous}nkey_seed_file = \"s\"\n");
    assert!(crate::GatewayConfig::from_toml(&nkey).is_ok());
    let no_section = format!("{BASE}\n[queue]\nbackend = \"nats\"\n");
    assert!(crate::GatewayConfig::from_toml(&no_section).is_err());
    let bad_subject = format!("{ok}subject_prefix = \"a.>\"\n");
    assert!(crate::GatewayConfig::from_toml(&bad_subject).is_err());
    // unbounded or inconsistent limits are refused
    let unbounded = format!("{ok}[queue.limits]\nmax_age_seconds = 0\n");
    assert!(crate::GatewayConfig::from_toml(&unbounded).is_err());

    let sqlite = format!("{BASE}\n[queue]\nbackend = \"sqlite\"\n");
    assert!(crate::GatewayConfig::from_toml(&sqlite).is_ok());
    let production = sqlite
        .replace("kind = \"process\"", "kind = \"firecracker\"")
        .replace("[provider.process]\nbridge_binary = \"target/debug/bridge\"\nworkdir = \"./data/process\"", "[provider.firecracker]\nfirecracker_binary = \"f\"\nkernel = \"k\"\nrootfs = \"r\"\nworkdir = \"w\"");
    let production = format!("profile = \"production\"\n{production}");
    let err = crate::GatewayConfig::from_toml(&production).unwrap_err();
    assert!(err.to_string().contains("sqlite"), "{err}");
}

#[test]
fn object_config_requires_a_key_and_bounded_limits() {
    let no_key = format!("{BASE}\n[objects]\nbackend = \"filesystem\"\n");
    let err = crate::GatewayConfig::from_toml(&no_key).unwrap_err();
    assert!(err.to_string().contains("encrypted"), "{err}");
    let both = format!("{no_key}key_file = \"k\"\nkey_env = \"K\"\n");
    assert!(crate::GatewayConfig::from_toml(&both).is_err());
    let ok = format!("{no_key}key_env = \"TACHYON_TEST_OBJECT_KEY_UNSET\"\n");
    let cfg = crate::GatewayConfig::from_toml(&ok).unwrap();
    // a missing key fails at bootstrap, not silently unencrypted
    let err = build(
        &cfg,
        Arc::new(crate::repository::InMemoryStore::new(Limits::default())),
        Arc::new(FixedClock::new(t0())),
        DurableOverrides::default(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("TACHYON_TEST_OBJECT_KEY_UNSET"),
        "{err}"
    );
    for bad in [
        "regions = []",
        "regions = [\"../x\"]",
        "max_object_bytes = 0",
        "tenant_quota_bytes = 1\nmax_object_bytes = 2",
        "orphan_grace_seconds = 0",
    ] {
        let text = format!("{ok}{bad}\n");
        assert!(crate::GatewayConfig::from_toml(&text).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn sqlite_queue_and_objects_are_built_from_config() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("object.key");
    std::fs::write(&key, "ab".repeat(32)).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let text = format!(
        "data_dir = \"{}\"\n{BASE}\n[queue]\nbackend = \"sqlite\"\n[objects]\nbackend = \"filesystem\"\nkey_file = \"{}\"\n",
        dir.path().display(),
        key.display()
    );
    let cfg = crate::GatewayConfig::from_toml(&text).unwrap();
    let store: Arc<dyn StateStore> =
        Arc::new(crate::repository::InMemoryStore::new(Limits::default()));
    let durable = build(
        &cfg,
        store,
        Arc::new(FixedClock::new(t0())),
        DurableOverrides::default(),
    )
    .unwrap();
    assert_eq!(durable.queue.as_ref().unwrap().backend(), "sqlite");
    assert_eq!(durable.objects.as_ref().unwrap().backend(), "filesystem");
    assert!(dir.path().join("queue.db").exists());
    assert!(dir.path().join("objects").is_dir());
}
