//! Manifest and compatibility tests (PLT-4653). The "property" tests draw
//! manifests from a seeded generator (no external crate) and check the
//! invariants over many cases: a sealed manifest round-trips, any single
//! changed field is refused with its reason, another tenant is always refused
//! alone, and non-active or expired snapshots are always refused.

use chrono::{Duration, TimeZone, Utc};

use super::*;
use crate::revision::{
    ArtifactRef, ExecutionPolicy, Placement, ResourceProfile, RuntimeSpec, SecretBinding,
};
use crate::{Architecture, RUNTIME_PROTOCOL_V1};

/// xorshift64*: deterministic, seeded, good enough to vary test inputs.
struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn digest(&mut self) -> Sha256Digest {
        Sha256Digest::of_bytes(&self.next().to_le_bytes())
    }
    fn word(&mut self) -> String {
        format!("w{:x}", self.next())
    }
    fn ulid(&mut self) -> ulid::Ulid {
        ulid::Ulid::from_parts(self.below(1 << 40), u128::from(self.next()))
    }
}

fn now() -> Timestamp {
    Utc.with_ymd_and_hms(2026, 9, 17, 9, 0, 0).unwrap()
}

fn manifest(g: &mut Gen) -> SnapshotManifest {
    let created_at = now() - Duration::milliseconds(g.below(3_600_000) as i64);
    SnapshotManifest {
        manifest_version: SNAPSHOT_MANIFEST_VERSION,
        snapshot_id: SnapshotId::from_ulid(g.ulid()),
        tenant_id: TenantId::from_ulid(g.ulid()),
        function_id: FunctionId::from_ulid(g.ulid()),
        revision_id: RevisionId::from_ulid(g.ulid()),
        revision_spec_digest: g.digest(),
        runtime: RuntimeProfile {
            provider_kind: "firecracker".into(),
            provider_version: format!("Firecracker v1.{}.0", g.below(30)),
            vmm_sha256: g.digest(),
            kernel_sha256: g.digest(),
            rootfs_sha256: g.digest(),
            bridge_protocol_version: 3,
            jailer_mode: format!("jailer uid={} gid={} new_pid_ns=true", g.below(9999), 123),
            cgroup_mode: "required".into(),
            host_kernel: g.word(),
        },
        host_cpu: HostCpuIdentity {
            arch: if g.below(2) == 0 { "aarch64" } else { "x86_64" }.into(),
            cpu_model_hash: g.digest(),
            kvm_capabilities_hash: g.digest(),
        },
        devices: DeviceModel {
            drives: vec![
                DriveSlot {
                    drive_id: "rootfs".into(),
                    path_in_vmm: "/rootfs.ext4".into(),
                    read_only: true,
                    root_device: true,
                },
                DriveSlot {
                    drive_id: "scratch".into(),
                    path_in_vmm: "/scratch.ext4".into(),
                    read_only: false,
                    root_device: false,
                },
            ],
            vsock_guest_cid: 3,
            vsock_port: 5000,
            doorbell_port: 5001,
            network_interfaces: 0,
        },
        memory_mib: 128 + 128 * g.below(4) as u32,
        vcpus: 1 + g.below(2) as u32,
        storage: StorageLayout {
            scratch_mib: 64 + g.below(128) as u32,
        },
        network: NetworkProfile {
            egress: EgressProfile::None,
            allowlist_digest: None,
        },
        encryption_key_generation: format!("k1-{:016x}", g.next()),
        secret_generation: "none".into(),
        created_at,
        expires_at: now() + Duration::milliseconds(1 + g.below(86_400_000) as i64),
        source_environment_id: EnvironmentId::from_ulid(g.ulid()),
        source_boot_id: g.word(),
        sdk_lifecycle_version: 1,
        checkpoint_phase: CHECKPOINT_PHASE.into(),
        artifacts: SnapshotArtifacts {
            memory: ArtifactDigest {
                sha256: g.digest(),
                size_bytes: g.next() >> 30,
            },
            vmstate: ArtifactDigest {
                sha256: g.digest(),
                size_bytes: g.below(1 << 20),
            },
            scratch: ArtifactDigest {
                sha256: g.digest(),
                size_bytes: g.below(1 << 30),
            },
            function_drive: ArtifactDigest {
                sha256: g.digest(),
                size_bytes: g.below(1 << 30),
            },
        },
    }
}

fn target_of(m: &SnapshotManifest) -> RestoreTarget {
    RestoreTarget {
        tenant_id: m.tenant_id.clone(),
        function_id: m.function_id.clone(),
        revision_id: m.revision_id.clone(),
        revision_spec_digest: m.revision_spec_digest.clone(),
        runtime: m.runtime.clone(),
        host_cpu: m.host_cpu.clone(),
        devices: m.devices.clone(),
        memory_mib: m.memory_mib,
        vcpus: m.vcpus,
        storage: m.storage.clone(),
        network: m.network.clone(),
        encryption_key_generation: m.encryption_key_generation.clone(),
        secret_generation: m.secret_generation.clone(),
        sdk_lifecycle_version: m.sdk_lifecycle_version,
    }
}

type Mutation = (&'static str, fn(&mut RestoreTarget, &mut Gen));

/// Every field of the target, each changed on its own, with the reason code
/// it must produce.
fn mutations() -> Vec<Mutation> {
    vec![
        ("function_mismatch", |t, g| {
            t.function_id = FunctionId::from_ulid(g.ulid())
        }),
        ("revision_mismatch", |t, g| {
            t.revision_id = RevisionId::from_ulid(g.ulid())
        }),
        ("spec_digest_mismatch", |t, g| {
            t.revision_spec_digest = g.digest()
        }),
        ("runtime_mismatch", |t, _| {
            t.runtime.provider_kind = "cloud-hypervisor".into()
        }),
        ("runtime_mismatch", |t, g| {
            t.runtime.provider_version = g.word()
        }),
        ("runtime_mismatch", |t, g| t.runtime.vmm_sha256 = g.digest()),
        ("runtime_mismatch", |t, g| {
            t.runtime.kernel_sha256 = g.digest()
        }),
        ("runtime_mismatch", |t, g| {
            t.runtime.rootfs_sha256 = g.digest()
        }),
        ("runtime_mismatch", |t, _| {
            t.runtime.bridge_protocol_version = 2
        }),
        ("runtime_mismatch", |t, _| {
            t.runtime.jailer_mode = "off".into()
        }),
        ("runtime_mismatch", |t, _| {
            t.runtime.cgroup_mode = "best-effort".into()
        }),
        ("runtime_mismatch", |t, g| t.runtime.host_kernel = g.word()),
        ("host_cpu_mismatch", |t, _| {
            t.host_cpu.arch = "riscv64".into()
        }),
        ("host_cpu_mismatch", |t, g| {
            t.host_cpu.cpu_model_hash = g.digest()
        }),
        ("host_cpu_mismatch", |t, g| {
            t.host_cpu.kvm_capabilities_hash = g.digest()
        }),
        ("device_model_mismatch", |t, _| {
            t.devices.drives[1].path_in_vmm = "/other.ext4".into()
        }),
        ("device_model_mismatch", |t, _| {
            t.devices.drives.pop().map(drop).unwrap_or(())
        }),
        ("device_model_mismatch", |t, _| t.devices.vsock_port += 1),
        ("device_model_mismatch", |t, _| t.devices.doorbell_port += 1),
        ("device_model_mismatch", |t, _| {
            t.devices.vsock_guest_cid += 1
        }),
        ("memory_mismatch", |t, _| t.memory_mib += 1),
        ("vcpu_mismatch", |t, _| t.vcpus += 1),
        ("storage_mismatch", |t, _| t.storage.scratch_mib += 1),
        ("network_mismatch", |t, g| {
            t.network.allowlist_digest = Some(g.digest())
        }),
        ("key_generation_changed", |t, g| {
            t.encryption_key_generation = g.word()
        }),
        ("secret_generation_changed", |t, g| {
            t.secret_generation = g.word()
        }),
        ("lifecycle_version_mismatch", |t, _| {
            t.sdk_lifecycle_version += 1
        }),
    ]
}

fn codes(c: &Compatibility) -> Vec<&'static str> {
    c.reasons().iter().map(IncompatibleReason::code).collect()
}

#[test]
fn property_identical_target_is_compatible_and_manifest_roundtrips() {
    let key = SnapshotSigningKey::from_bytes([7; 32]);
    let mut g = Gen(0x9e37_79b9_7f4a_7c15);
    for _ in 0..300 {
        let m = manifest(&mut g);
        let c = check_compatibility(&m, &SnapshotState::Active, &target_of(&m), now());
        assert_eq!(c, Compatibility::Compatible, "{m:?}");
        let sealed = key.seal(&m);
        assert_eq!(sealed.digest, m.digest());
        let back = key.verify(&sealed).unwrap();
        assert_eq!(back, m);
        // Canonical: serializing again gives the signed bytes.
        assert_eq!(back.canonical_bytes(), sealed.manifest.as_bytes());
        let stored = serde_json::to_string(&sealed).unwrap();
        let reread: SealedManifest = serde_json::from_str(&stored).unwrap();
        assert_eq!(key.verify(&reread).unwrap(), m);
    }
}

#[test]
fn property_any_single_field_change_is_incompatible_with_its_reason() {
    let mut g = Gen(42);
    let all = mutations();
    for _ in 0..200 {
        let m = manifest(&mut g);
        for (code, mutate) in &all {
            let mut t = target_of(&m);
            mutate(&mut t, &mut g);
            let c = check_compatibility(&m, &SnapshotState::Active, &t, now());
            assert!(!c.is_compatible(), "{code}: {t:?}");
            assert!(
                codes(&c).contains(code),
                "{code} missing in {:?}",
                codes(&c)
            );
        }
    }
}

#[test]
fn property_another_tenant_is_always_refused_alone() {
    let mut g = Gen(7);
    let all = mutations();
    for _ in 0..200 {
        let m = manifest(&mut g);
        let mut t = target_of(&m);
        // Whatever else differs, or does not ...
        for _ in 0..g.below(4) {
            let (_, mutate) = &all[g.below(all.len() as u64) as usize];
            mutate(&mut t, &mut g);
        }
        t.tenant_id = TenantId::from_ulid(g.ulid());
        for state in [
            SnapshotState::Active,
            SnapshotState::Expired,
            SnapshotState::Revoked { reason: "x".into() },
        ] {
            let c = check_compatibility(&m, &state, &t, now());
            // ... the answer is exactly "another tenant" and nothing else.
            assert_eq!(
                c,
                Compatibility::Incompatible {
                    reasons: vec![IncompatibleReason::TenantMismatch]
                }
            );
        }
    }
}

#[test]
fn property_expired_revoked_and_quarantined_are_refused() {
    let mut g = Gen(99);
    for _ in 0..200 {
        let m = manifest(&mut g);
        let t = target_of(&m);
        let at_expiry = m.expires_at;
        assert_eq!(
            codes(&check_compatibility(
                &m,
                &SnapshotState::Active,
                &t,
                at_expiry
            )),
            vec!["expired"]
        );
        let later = m.expires_at + Duration::seconds(g.below(1000) as i64);
        assert!(!check_compatibility(&m, &SnapshotState::Active, &t, later).is_compatible());
        for (state, code) in [
            (SnapshotState::Expired, "expired"),
            (
                SnapshotState::Revoked {
                    reason: "revision updated".into(),
                },
                "revoked",
            ),
            (
                SnapshotState::Quarantined {
                    reason: "memory digest mismatch".into(),
                },
                "quarantined",
            ),
        ] {
            let c = check_compatibility(&m, &state, &t, now());
            assert_eq!(codes(&c), vec![code], "{state:?}");
        }
    }
}

#[test]
fn restricted_or_public_egress_is_unsupported_even_when_equal() {
    let mut g = Gen(3);
    let mut m = manifest(&mut g);
    m.network.egress = EgressProfile::PublicWeb;
    let t = target_of(&m);
    let c = check_compatibility(&m, &SnapshotState::Active, &t, now());
    assert_eq!(codes(&c), vec!["egress_unsupported"]);
}

#[test]
fn a_manifest_of_another_version_or_phase_is_refused() {
    let mut g = Gen(5);
    let mut m = manifest(&mut g);
    let t = target_of(&m);
    m.manifest_version = 2;
    m.checkpoint_phase = "after_restore".into();
    let c = check_compatibility(&m, &SnapshotState::Active, &t, now());
    assert_eq!(codes(&c), vec!["manifest_version", "checkpoint_phase"]);
}

#[test]
fn a_tampered_or_foreign_manifest_does_not_verify() {
    let key = SnapshotSigningKey::from_bytes([1; 32]);
    let other = SnapshotSigningKey::from_bytes([2; 32]);
    let mut g = Gen(11);
    for _ in 0..50 {
        let m = manifest(&mut g);
        let sealed = key.seal(&m);
        assert!(matches!(
            other.verify(&sealed),
            Err(ManifestError::KeyMismatch { .. })
        ));
        // A forged key id with the other key's MAC still fails the MAC.
        let mut forged = other.seal(&m);
        forged.key_id = key.id().to_string();
        assert_eq!(key.verify(&forged), Err(ManifestError::BadSignature));
        // Any flipped byte of the signed content.
        let mut bytes = sealed.manifest.clone().into_bytes();
        let i = g.below(bytes.len() as u64) as usize;
        bytes[i] = if bytes[i] == b'0' { b'1' } else { b'0' };
        let tampered = SealedManifest {
            manifest: String::from_utf8(bytes).unwrap(),
            ..sealed.clone()
        };
        if tampered.manifest != sealed.manifest {
            assert_eq!(key.verify(&tampered), Err(ManifestError::BadSignature));
        }
        // A digest that does not match the signed content.
        let wrong_digest = SealedManifest {
            digest: Sha256Digest::of_bytes(b"x"),
            ..sealed.clone()
        };
        assert_eq!(
            key.verify(&wrong_digest),
            Err(ManifestError::DigestMismatch)
        );
    }
}

#[test]
fn a_non_canonical_manifest_is_refused_even_when_signed() {
    let key = SnapshotSigningKey::from_bytes([9; 32]);
    let m = manifest(&mut Gen(13));
    let pretty = serde_json::to_string_pretty(&m).unwrap();
    let canonical = key.seal(&m);
    let mac = hmac_sha256(&key.key, &signed_message(key.id(), pretty.as_bytes()));
    let sealed = SealedManifest {
        digest: Sha256Digest::of_bytes(pretty.as_bytes()),
        manifest: pretty,
        key_id: canonical.key_id,
        signature: hex::encode(mac),
    };
    assert_eq!(key.verify(&sealed), Err(ManifestError::NotCanonical));
}

#[test]
fn hmac_matches_rfc_4231() {
    // Test case 2.
    assert_eq!(
        hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    // Test case 6 (key longer than the block).
    assert_eq!(
        hex::encode(hmac_sha256(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First"
        )),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

#[test]
fn signing_key_debug_hides_the_key() {
    let key = SnapshotSigningKey::from_hex(&"ab".repeat(32)).unwrap();
    let shown = format!("{key:?}");
    assert!(!shown.contains("abab"), "{shown}");
    assert!(shown.contains("s1-"));
    assert!(SnapshotSigningKey::from_hex("zz").is_err());
}

fn spec() -> RevisionSpec {
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
        secrets: vec![],
        description: String::new(),
        placement: Placement::default(),
        restore: RestoreSettings::default(),
    }
}

#[test]
fn restore_settings_default_leaves_the_spec_digest_unchanged() {
    let plain = spec();
    let json = serde_json::to_string(&plain).unwrap();
    assert!(!json.contains("restore"), "{json}");
    let mut prefer = spec();
    prefer.restore = RestoreSettings {
        policy: RestorePolicy::Prefer,
        synthetic_init_sample: true,
    };
    assert_ne!(prefer.digest(), plain.digest());
    assert!(prefer.validate(&crate::Limits::default()).is_ok());
}

#[test]
fn a_restore_policy_needs_a_synthetic_sample_without_secrets_and_egress() {
    let limits = crate::Limits::default();
    let mut s = spec();
    s.restore.policy = RestorePolicy::Require;
    assert!(s.validate(&limits).is_err(), "not marked synthetic");
    s.restore.synthetic_init_sample = true;
    assert!(s.validate(&limits).is_ok());
    let mut with_secret = s.clone();
    with_secret.secrets.push(SecretBinding {
        env_name: "DB".into(),
        binding_ref: "db".into(),
    });
    let err = with_secret.validate(&limits).unwrap_err().to_string();
    assert!(err.contains("secret bindings"), "{err}");
    let mut public = s.clone();
    public.egress = EgressProfile::PublicWeb;
    assert!(public.validate(&limits).is_err());
    // Snapshot eligibility is checked the same way without a policy.
    let mut disabled = spec();
    assert!(RestoreSettings::snapshot_eligibility(&disabled).is_err());
    disabled.restore.synthetic_init_sample = true;
    assert!(RestoreSettings::snapshot_eligibility(&disabled).is_ok());
    assert_eq!(
        RestorePolicy::parse("prefer").unwrap(),
        RestorePolicy::Prefer
    );
    assert!(RestorePolicy::parse("sometimes").is_err());
}
