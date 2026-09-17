//! Encrypted snapshot artifact store and catalog (X1, PLT-4653).
//!
//! ```text
//! <root>/<snapshot_id>/
//!   manifest.json          SealedManifest (canonical manifest + digest + HMAC)
//!   record.json            catalog state: active | revoked | quarantined | expired, restores
//!   memory.sealed          \
//!   vmstate.sealed          | AES-256-GCM in 4 MiB chunks
//!   scratch.ext4.sealed     |
//!   function.ext4.sealed   /
//! ```
//!
//! Every chunk is sealed with associated data that binds it to the tenant,
//! the snapshot, the artifact name, its index and whether it is the last one,
//! so a chunk moved to another tenant's or snapshot's file, reordered,
//! dropped or appended fails to open. The plaintext SHA-256 is recorded in the
//! signed manifest and checked after decryption and before every load.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tachyon_serverless_domain::{
    ArtifactDigest, FunctionId, RevisionId, SealedManifest, Sha256Digest, SnapshotId,
    SnapshotState, TenantId, Timestamp,
};

use crate::durable::ObjectKey;

const MAGIC: &[u8; 8] = b"TSSNAP1\0";
/// Plaintext bytes per sealed chunk.
pub const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MANIFEST: &str = "manifest.json";
const RECORD: &str = "record.json";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Decryption or authentication failed, the file is truncated or
    /// malformed, or the plaintext digest differs from the manifest.
    #[error("integrity: {0}")]
    Integrity(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("malformed {what}: {detail}")]
    Malformed { what: &'static str, detail: String },
}

/// Catalog entry of one snapshot. The manifest is the authority on what the
/// snapshot *is*; this records what may be done with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub snapshot_id: SnapshotId,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    pub revision_id: RevisionId,
    pub created_at: Timestamp,
    pub state: SnapshotState,
    /// Clones started from it (the generation of the next one is this + 1).
    pub restores: u64,
    /// Source snapshot timings (ms): pause, create, copy, seal.
    #[serde(default)]
    pub timings: serde_json::Value,
}

/// Filesystem catalog + sealed artifacts.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    root: PathBuf,
    chunk_bytes: usize,
}

fn aad(tenant: &TenantId, snapshot: &SnapshotId, name: &str, index: u64, last: bool) -> Vec<u8> {
    let mut a = b"tachyon-serverless/snapshot-artifact/v1\0".to_vec();
    for part in [tenant.as_str(), snapshot.as_str(), name] {
        a.extend_from_slice(part.as_bytes());
        a.push(0);
    }
    a.extend_from_slice(&index.to_be_bytes());
    a.push(u8::from(last));
    a
}

/// Write `path` atomically (temporary file + rename), mode 0600.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = open_private(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

fn open_private(path: &Path) -> std::io::Result<File> {
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    o.open(path)
}

/// SHA-256 and size of a file, streamed.
pub fn digest_file(path: &Path) -> std::io::Result<ArtifactDigest> {
    let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut size = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok(ArtifactDigest {
        sha256: Sha256Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize())))
            .expect("a hex sha256 is a valid digest"),
        size_bytes: size,
    })
}

/// Read up to `buf.len()` bytes (short only at EOF).
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = r.read(&mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

impl SnapshotStore {
    pub fn open(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(Self {
            root,
            chunk_bytes: CHUNK_BYTES,
        })
    }

    /// Seal with a smaller chunk size (tests; the reader takes the size
    /// from the file header).
    pub fn with_chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        self.chunk_bytes = chunk_bytes.clamp(1, CHUNK_BYTES);
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn dir(&self, id: &SnapshotId) -> PathBuf {
        self.root.join(id.as_str())
    }

    pub fn sealed_path(&self, id: &SnapshotId, name: &str) -> PathBuf {
        self.dir(id).join(format!("{name}.sealed"))
    }

    /// Encrypt `plain` into this snapshot's `name.sealed`, returning the
    /// plaintext digest. Blocking; call from `spawn_blocking`.
    pub fn seal_artifact(
        &self,
        key: &ObjectKey,
        tenant: &TenantId,
        id: &SnapshotId,
        name: &str,
        plain: &Path,
    ) -> Result<ArtifactDigest, StoreError> {
        std::fs::create_dir_all(self.dir(id))?;
        let dst = self.sealed_path(id, name);
        let tmp = dst.with_extension("sealed.tmp");
        let chunk_bytes = self.chunk_bytes;
        let mut reader = BufReader::with_capacity(chunk_bytes, File::open(plain)?);
        let mut out = BufWriter::new(open_private(&tmp)?);
        out.write_all(MAGIC)?;
        out.write_all(&(chunk_bytes as u32).to_be_bytes())?;
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let mut current = vec![0u8; chunk_bytes];
        let mut next = vec![0u8; chunk_bytes];
        let mut current_len = read_full(&mut reader, &mut current)?;
        let mut index = 0u64;
        loop {
            // Look one chunk ahead to know whether this one is the last.
            let next_len = if current_len == chunk_bytes {
                read_full(&mut reader, &mut next)?
            } else {
                0
            };
            let last = next_len == 0;
            let chunk = &current[..current_len];
            hasher.update(chunk);
            size += current_len as u64;
            let sealed = key.seal(chunk, &aad(tenant, id, name, index, last));
            out.write_all(&[u8::from(last)])?;
            out.write_all(&(sealed.len() as u32).to_be_bytes())?;
            out.write_all(&sealed)?;
            if last {
                break;
            }
            std::mem::swap(&mut current, &mut next);
            current_len = next_len;
            index += 1;
        }
        let file = out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &dst)?;
        Ok(ArtifactDigest {
            sha256: Sha256Digest::parse(&format!("sha256:{}", hex::encode(hasher.finalize())))
                .expect("a hex sha256 is a valid digest"),
            size_bytes: size,
        })
    }

    /// Decrypt `name.sealed` into `plain` (written atomically) and verify it
    /// against `expected`. Nothing is left at `plain` on failure. Blocking.
    pub fn open_artifact(
        &self,
        key: &ObjectKey,
        tenant: &TenantId,
        id: &SnapshotId,
        name: &str,
        plain: &Path,
        expected: &ArtifactDigest,
    ) -> Result<(), StoreError> {
        let src = self.sealed_path(id, name);
        let file = File::open(&src).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                StoreError::NotFound(format!("sealed artifact {}", src.display()))
            }
            _ => StoreError::Io(e),
        })?;
        let mut reader = BufReader::with_capacity(CHUNK_BYTES + 64, file);
        let bad = |d: String| StoreError::Integrity(format!("{name}: {d}"));
        let mut magic = [0u8; 8];
        if read_full(&mut reader, &mut magic)? != 8 || &magic != MAGIC {
            return Err(bad("not a sealed snapshot artifact".into()));
        }
        let mut u32buf = [0u8; 4];
        if read_full(&mut reader, &mut u32buf)? != 4 {
            return Err(bad("truncated header".into()));
        }
        let chunk_bytes = u32::from_be_bytes(u32buf) as usize;
        if chunk_bytes == 0 || chunk_bytes > 64 * 1024 * 1024 {
            return Err(bad(format!("chunk size {chunk_bytes} out of range")));
        }
        let tmp = plain.with_extension("open.tmp");
        let result = (|| -> Result<(), StoreError> {
            let mut out = BufWriter::new(open_private(&tmp)?);
            let mut hasher = Sha256::new();
            let mut size = 0u64;
            let mut index = 0u64;
            let mut sealed = Vec::new();
            loop {
                let mut flag = [0u8; 1];
                if read_full(&mut reader, &mut flag)? != 1 {
                    return Err(bad("truncated: the last chunk is missing".into()));
                }
                if read_full(&mut reader, &mut u32buf)? != 4 {
                    return Err(bad("truncated chunk header".into()));
                }
                let len = u32::from_be_bytes(u32buf) as usize;
                if len > chunk_bytes + 64 {
                    return Err(bad(format!("chunk {index} length {len} out of range")));
                }
                sealed.resize(len, 0);
                if read_full(&mut reader, &mut sealed)? != len {
                    return Err(bad(format!("chunk {index} truncated")));
                }
                let last = flag[0] == 1;
                let plain_chunk = key
                    .open(&sealed, &aad(tenant, id, name, index, last))
                    .ok_or_else(|| bad(format!("chunk {index} does not authenticate")))?;
                hasher.update(&plain_chunk);
                size += plain_chunk.len() as u64;
                out.write_all(&plain_chunk)?;
                if last {
                    let mut probe = [0u8; 1];
                    if read_full(&mut reader, &mut probe)? != 0 {
                        return Err(bad("data after the last chunk".into()));
                    }
                    break;
                }
                index += 1;
            }
            let file = out.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
            let got = format!("sha256:{}", hex::encode(hasher.finalize()));
            if got != expected.sha256.as_str() || size != expected.size_bytes {
                return Err(bad(format!(
                    "plaintext {got} ({size} bytes) differs from the manifest {} ({} bytes)",
                    expected.sha256, expected.size_bytes
                )));
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                std::fs::rename(&tmp, plain)?;
                Ok(())
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e)
            }
        }
    }

    pub fn write_manifest(
        &self,
        id: &SnapshotId,
        sealed: &SealedManifest,
    ) -> Result<(), StoreError> {
        std::fs::create_dir_all(self.dir(id))?;
        let bytes = serde_json::to_vec_pretty(sealed).expect("sealed manifest serializes");
        write_atomic(&self.dir(id).join(MANIFEST), &bytes)?;
        Ok(())
    }

    pub fn read_manifest(&self, id: &SnapshotId) -> Result<SealedManifest, StoreError> {
        let path = self.dir(id).join(MANIFEST);
        let bytes = std::fs::read(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => StoreError::NotFound(format!("snapshot {id}")),
            _ => StoreError::Io(e),
        })?;
        serde_json::from_slice(&bytes).map_err(|e| StoreError::Malformed {
            what: "manifest",
            detail: e.to_string(),
        })
    }

    pub fn write_record(&self, record: &SnapshotRecord) -> Result<(), StoreError> {
        let id = &record.snapshot_id;
        std::fs::create_dir_all(self.dir(id))?;
        let bytes = serde_json::to_vec_pretty(record).expect("record serializes");
        write_atomic(&self.dir(id).join(RECORD), &bytes)?;
        Ok(())
    }

    pub fn read_record(&self, id: &SnapshotId) -> Result<SnapshotRecord, StoreError> {
        let path = self.dir(id).join(RECORD);
        let bytes = std::fs::read(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => StoreError::NotFound(format!("snapshot {id}")),
            _ => StoreError::Io(e),
        })?;
        serde_json::from_slice(&bytes).map_err(|e| StoreError::Malformed {
            what: "record",
            detail: e.to_string(),
        })
    }

    /// Every snapshot record, newest first. Unreadable entries are skipped.
    pub fn list(&self) -> Vec<SnapshotRecord> {
        let Ok(rd) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        let mut out: Vec<SnapshotRecord> = rd
            .flatten()
            .filter_map(|e| e.file_name().into_string().ok())
            .filter_map(|name| SnapshotId::parse(&name).ok())
            .filter_map(|id| self.read_record(&id).ok())
            .collect();
        out.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        out
    }

    /// Remove a snapshot's catalog entry and sealed artifacts.
    pub fn remove(&self, id: &SnapshotId) -> std::io::Result<()> {
        match std::fs::remove_dir_all(self.dir(id)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CHUNK: usize = 64 * 1024;

    fn fixture(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i * 31 % 251) as u8).collect()
    }

    fn roundtrip(size: usize) {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().join("snapshots"))
            .unwrap()
            .with_chunk_bytes(TEST_CHUNK);
        let key = ObjectKey::generate();
        let tenant = TenantId::generate();
        let id = SnapshotId::generate();
        let plain = dir.path().join("memory");
        std::fs::write(&plain, fixture(size)).unwrap();
        let digest = store
            .seal_artifact(&key, &tenant, &id, "memory", &plain)
            .unwrap();
        assert_eq!(digest, digest_file(&plain).unwrap());
        if size >= 4096 {
            let sealed = std::fs::read(store.sealed_path(&id, "memory")).unwrap();
            let probe = &fixture(size)[1000..1256];
            assert!(
                !sealed.windows(probe.len()).any(|w| w == probe),
                "plaintext must not appear in the sealed file"
            );
        }
        let out = dir.path().join("restored");
        store
            .open_artifact(&key, &tenant, &id, "memory", &out, &digest)
            .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), fixture(size));
    }

    #[test]
    fn artifacts_roundtrip_across_chunk_boundaries() {
        for size in [
            0,
            1,
            TEST_CHUNK - 1,
            TEST_CHUNK,
            TEST_CHUNK + 1,
            2 * TEST_CHUNK + 7,
        ] {
            roundtrip(size);
        }
    }

    /// A flipped byte anywhere in the sealed file, a truncated file, another
    /// tenant, another key or a digest that does not match are all refused
    /// and leave no plaintext behind.
    #[test]
    fn corrupted_or_foreign_artifacts_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().join("snapshots"))
            .unwrap()
            .with_chunk_bytes(TEST_CHUNK);
        let key = ObjectKey::generate();
        let tenant = TenantId::generate();
        let id = SnapshotId::generate();
        let plain = dir.path().join("vmstate");
        std::fs::write(&plain, fixture(TEST_CHUNK + 100)).unwrap();
        let digest = store
            .seal_artifact(&key, &tenant, &id, "vmstate", &plain)
            .unwrap();
        let sealed_path = store.sealed_path(&id, "vmstate");
        let original = std::fs::read(&sealed_path).unwrap();
        let out = dir.path().join("out");
        let open = |k: &ObjectKey, t: &TenantId, d: &ArtifactDigest| {
            store.open_artifact(k, t, &id, "vmstate", &out, d)
        };

        for pos in [20, original.len() / 2, original.len() - 1] {
            let mut bytes = original.clone();
            bytes[pos] ^= 0x01;
            std::fs::write(&sealed_path, &bytes).unwrap();
            assert!(
                matches!(open(&key, &tenant, &digest), Err(StoreError::Integrity(_))),
                "flipped byte at {pos}"
            );
            assert!(!out.exists());
        }
        std::fs::write(&sealed_path, &original[..original.len() - 10]).unwrap();
        assert!(matches!(
            open(&key, &tenant, &digest),
            Err(StoreError::Integrity(_))
        ));
        // Drop the last chunk entirely: the "last" flag is authenticated.
        let first_chunk_end = 12 + 1 + 4 + (TEST_CHUNK + 12 + 16 + 4);
        std::fs::write(&sealed_path, &original[..first_chunk_end]).unwrap();
        assert!(matches!(
            open(&key, &tenant, &digest),
            Err(StoreError::Integrity(_))
        ));
        std::fs::write(&sealed_path, &original).unwrap();
        assert!(matches!(
            open(&key, &TenantId::generate(), &digest),
            Err(StoreError::Integrity(_))
        ));
        assert!(matches!(
            open(&ObjectKey::generate(), &tenant, &digest),
            Err(StoreError::Integrity(_))
        ));
        let wrong = ArtifactDigest {
            sha256: Sha256Digest::of_bytes(b"other"),
            size_bytes: digest.size_bytes,
        };
        assert!(matches!(
            open(&key, &tenant, &wrong),
            Err(StoreError::Integrity(_))
        ));
        assert!(!out.exists());
        open(&key, &tenant, &digest).unwrap();
    }

    #[test]
    fn records_are_listed_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let mk = |secs: i64| SnapshotRecord {
            snapshot_id: SnapshotId::generate(),
            tenant_id: TenantId::generate(),
            function_id: FunctionId::generate(),
            revision_id: RevisionId::generate(),
            created_at: chrono::DateTime::from_timestamp(secs, 0).unwrap(),
            state: SnapshotState::Active,
            restores: 0,
            timings: serde_json::Value::Null,
        };
        let (a, b) = (mk(10), mk(20));
        store.write_record(&a).unwrap();
        store.write_record(&b).unwrap();
        let listed = store.list();
        assert_eq!(listed, vec![b.clone(), a]);
        store.remove(&b.snapshot_id).unwrap();
        assert_eq!(store.list().len(), 1);
    }
}
