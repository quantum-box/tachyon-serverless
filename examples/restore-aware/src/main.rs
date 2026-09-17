//! `restore-aware`: the experimental restore-aware lifecycle (PLT-4651, X1).
//!
//! The split this example demonstrates, and why:
//!
//! - **bootstrap** (before the checkpoint, synchronous, no Tokio): builds a
//!   prime sieve. It is pure, deterministic, synthetic data that every copy
//!   of a snapshot can share. Nothing here may be unique per instance or
//!   secret: whatever exists at the checkpoint would be duplicated into every
//!   restored copy.
//! - **after_restore** (after `continue`, inside the runtime the SDK builds
//!   only then): everything that must differ between copies or be fresh.
//!   - identity: the instance id from the restore context plus an OS nonce
//!     (two copies of one snapshot must not share an id);
//!   - RNG: reseeded from `/dev/urandom`; a seed taken in bootstrap would make
//!     every copy produce the same "random" sequence;
//!   - clock: read now; a timestamp captured before the checkpoint is stale by
//!     however long the snapshot sat on disk;
//!   - credentials: only their presence is checked, and only here. In a
//!     restored copy the process environment is the snapshot's, so real fresh
//!     credentials need a delivery channel that does not exist yet (PLT-4653);
//!   - "connection": a fake handle standing in for a DB/TLS connection. Sockets
//!     do not survive a restore meaningfully, so none may exist before the
//!     checkpoint.
//!
//! This does not make arbitrary libraries or a multithreaded runtime
//! snapshot-safe; it only keeps this example's own state on the right side.
//!
//! Payload `{"n": <u32>}` answers whether `n` is prime (from the table) plus
//! the per-instance facts. `RESTORE_AWARE_FAIL=bootstrap|after_restore` makes
//! that hook fail, to show the distinct error types. No network access.

use std::io::Read;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tachyon_serverless_sdk::lifecycle::{self, RestoreContext};
use tachyon_serverless_sdk::{Context, Event, HandlerError, SdkError};

/// Size of the synthetic lookup table.
const TABLE_LIMIT: usize = 100_000;

/// Reusable, instance-independent state (safe to capture in a snapshot).
struct Fixed {
    is_prime: Vec<bool>,
    prime_count: usize,
}

/// Per-instance state, created only after the checkpoint.
struct Instance {
    fixed: Fixed,
    instance_id: String,
    restored: bool,
    generation: u64,
    started_at_ms: u64,
    rng: Mutex<XorShift64>,
    secret_present: bool,
    connection: FakeConnection,
    /// What the instance's scratch marker held before this copy wrote its
    /// own id (empty for any copy of a snapshot: the source never wrote one).
    scratch_before: String,
}

/// Stand-in for a database or TLS connection: opened in after_restore only.
struct FakeConnection {
    id: String,
    opened_at_ms: u64,
    queries: AtomicU64,
}

struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn failure_requested(hook: &str) -> bool {
    // A demo knob read from the (snapshot-captured) environment; it is not a
    // secret and not per-instance.
    std::env::var("RESTORE_AWARE_FAIL").as_deref() == Ok(hook)
}

fn sieve(limit: usize) -> Fixed {
    let mut is_prime = vec![true; limit + 1];
    is_prime[0] = false;
    if limit >= 1 {
        is_prime[1] = false;
    }
    let mut i = 2;
    while i * i <= limit {
        if is_prime[i] {
            let mut j = i * i;
            while j <= limit {
                is_prime[j] = false;
                j += i;
            }
        }
        i += 1;
    }
    let prime_count = is_prime.iter().filter(|p| **p).count();
    Fixed {
        is_prime,
        prime_count,
    }
}

fn bootstrap() -> Result<Fixed, HandlerError> {
    if failure_requested("bootstrap") {
        return Err(HandlerError::with_type(
            "Demo.BootstrapFailure",
            "RESTORE_AWARE_FAIL=bootstrap",
        ));
    }
    eprintln!("restore-aware: bootstrap: building the lookup table (no runtime, no secrets)");
    Ok(sieve(TABLE_LIMIT))
}

fn os_random_u64() -> Result<u64, HandlerError> {
    let mut buf = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .map_err(|e| HandlerError::new(format!("cannot read /dev/urandom: {e}")))?;
    Ok(u64::from_le_bytes(buf))
}

fn ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

async fn after_restore(fixed: Fixed, ctx: RestoreContext) -> Result<Instance, HandlerError> {
    if failure_requested("after_restore") {
        return Err(HandlerError::with_type(
            "Demo.AfterRestoreFailure",
            "RESTORE_AWARE_FAIL=after_restore",
        ));
    }
    let nonce = os_random_u64()?;
    let seed = os_random_u64()? | 1;
    let now_ms = ms(SystemTime::now());
    let instance_id = format!("{}-{nonce:016x}", ctx.instance_id);
    let secret_present = std::env::var_os("DEMO_SECRET").is_some();
    // Per-instance write area (PLT-4653): each copy records its own id on the
    // writable scratch drive. Two copies of one snapshot must never see each
    // other's marker.
    let marker = scratch_marker_path();
    let scratch_before = std::fs::read_to_string(&marker).unwrap_or_default();
    let _ = std::fs::write(&marker, format!("{instance_id}\n"));
    let connection = FakeConnection {
        id: format!("conn-{:016x}", os_random_u64()?),
        opened_at_ms: now_ms,
        queries: AtomicU64::new(0),
    };
    eprintln!(
        "restore-aware: after_restore: restored={} generation={} instance={instance_id}",
        ctx.restored, ctx.generation
    );
    Ok(Instance {
        fixed,
        instance_id,
        restored: ctx.restored,
        generation: ctx.generation,
        started_at_ms: ms(ctx.started_at),
        rng: Mutex::new(XorShift64(seed)),
        secret_present,
        connection,
        scratch_before,
    })
}

fn answer(state: &Instance, payload: &Value) -> Result<Value, HandlerError> {
    let n = payload.get("n").and_then(Value::as_u64).ok_or_else(|| {
        HandlerError::with_type("Handler.InvalidPayload", "expected {\"n\": u32}")
    })?;
    let is_prime = state
        .fixed
        .is_prime
        .get(n as usize)
        .copied()
        .ok_or_else(|| {
            HandlerError::with_type(
                "Handler.InvalidPayload",
                format!("n must be at most {TABLE_LIMIT}"),
            )
        })?;
    let random = state.rng.lock().map(|mut r| r.next()).unwrap_or_default();
    let queries = state.connection.queries.fetch_add(1, Ordering::Relaxed) + 1;
    Ok(json!({
        "n": n,
        "is_prime": is_prime,
        "table_primes": state.fixed.prime_count,
        "instance_id": state.instance_id,
        "restored": state.restored,
        "generation": state.generation,
        "started_at_ms": state.started_at_ms,
        "random": random,
        "connection_id": state.connection.id,
        "connection_opened_at_ms": state.connection.opened_at_ms,
        "connection_queries": queries,
        "secret_present": state.secret_present,
        "scratch_marker": std::fs::read_to_string(scratch_marker_path()).unwrap_or_default().trim(),
        "scratch_marker_before": state.scratch_before.trim(),
    }))
}

/// Marker file on the writable scratch drive (`/tmp` in a Firecracker guest).
fn scratch_marker_path() -> std::path::PathBuf {
    std::env::var_os("RESTORE_AWARE_SCRATCH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("restore-aware-instance"))
}

fn main() -> Result<(), SdkError> {
    lifecycle::builder()
        .bootstrap(bootstrap)
        .after_restore(after_restore)
        .run(
            |state: std::sync::Arc<Instance>, event: Event, _ctx: Context| async move {
                answer(&state, event.json())
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_a_correct_sieve() {
        let f = sieve(100);
        let primes: Vec<usize> = (0..=100).filter(|i| f.is_prime[*i]).collect();
        assert_eq!(primes.len(), 25);
        assert_eq!(&primes[..5], &[2, 3, 5, 7, 11]);
        assert_eq!(sieve(TABLE_LIMIT).prime_count, 9592);
    }

    #[test]
    fn copies_get_distinct_identity_and_random_streams() {
        let ctx = |restored| RestoreContext {
            restored,
            instance_id: "inst".into(),
            generation: 1,
            restored_at: None,
            started_at: SystemTime::now(),
            environment_id: "env".into(),
        };
        // Two "copies" of the same fixed state: nothing per-instance may match.
        let a = poll_ready(after_restore(sieve(10), ctx(true))).unwrap();
        let b = poll_ready(after_restore(sieve(10), ctx(true))).unwrap();
        assert_ne!(a.instance_id, b.instance_id);
        assert_ne!(a.connection.id, b.connection.id);
        let ra = answer(&a, &json!({"n": 7})).unwrap();
        let rb = answer(&b, &json!({"n": 7})).unwrap();
        assert_eq!(ra["is_prime"], true);
        assert_ne!(ra["random"], rb["random"]);
    }

    /// `after_restore` here never waits on I/O, so one poll completes it.
    fn poll_ready<F: std::future::Future>(f: F) -> F::Output {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match std::pin::pin!(f).poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => panic!("after_restore unexpectedly pending"),
        }
    }
}
