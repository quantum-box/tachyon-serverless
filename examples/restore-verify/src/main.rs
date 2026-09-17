//! `restore-verify`: the verification sample for snapshot restore (PLT-4654,
//! X1, experimental). Synthetic data only.
//!
//! It is built to make a wrong restore **visible** in one response:
//!
//! - **bootstrap** (before the checkpoint): a deterministic table of
//!   `RESTORE_VERIFY_DATASET_MIB` MiB (default 64) of `splitmix64(SEED ^ i)`
//!   values, its sha256 (`checksum`, known in advance: the unit test pins the
//!   value for 1 MiB) and a chained digest over `RESTORE_VERIFY_PRECOMPUTE_PASSES`
//!   passes (the "expensive precomputation"). It appends one line to
//!   `bootstrap.log` on the scratch drive and counts its runs, so a restored copy
//!   can show that bootstrap ran once, in the snapshot's source, and not again.
//! - **after_restore** (after `continue`): everything a copy must not share —
//!   an instance id and a session token from `getrandom`, a userspace RNG seeded
//!   from `getrandom`, the wall / monotonic clocks and a Tokio timer, a loopback
//!   "DB" server with an authenticated session, a loopback TLS server whose
//!   certificate is generated now and a client session to it, and a scratch file
//!   named after the instance id.
//! - **handler**: payload `{"i": <index>, "verify": <bool>}`. Answers a table
//!   lookup checked against the generator, 64 random spot checks, optionally a
//!   full recomputation of the checksum (`verify`), and reports all per-instance
//!   facts, the live DB / TLS state and the scratch directory listing.
//!
//! Loopback is all there is: clones run with egress `none` (PLT-4653 refuses
//! anything else), so a real external database or TLS endpoint cannot be
//! reached from a clone and its reconnect is **not** tested here; the loopback
//! servers stand in for "a connection created after the restore works and is
//! unique to this copy".

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tachyon_serverless_sdk::lifecycle::{self, RestoreContext};
use tachyon_serverless_sdk::{Context, Event, HandlerError, SdkError};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// Seed of the synthetic table. Fixed: every build and every copy computes the
/// same table and checksum.
const SEED: u64 = 0x7a63_6879_6f6e_0001;
const DEFAULT_DATASET_MIB: u64 = 64;
const MAX_DATASET_MIB: u64 = 1024;
const DEFAULT_PRECOMPUTE_PASSES: u64 = 1;
const SPOT_CHECKS: usize = 64;
const TIMER_MS: u64 = 50;
const TICK_MS: u64 = 10;
const BOOTSTRAP_LOG: &str = "rv-bootstrap.log";
const INSTANCE_PREFIX: &str = "rv-instance-";

static BOOTSTRAP_RUNS: AtomicU64 = AtomicU64::new(0);

/// Instance-independent state built before the checkpoint.
struct Fixed {
    table: Vec<u64>,
    checksum: String,
    precompute: String,
    passes: u64,
    bootstrap_ms: u64,
    bootstrap_wall_ms: u64,
    bootstrap_env: String,
    bootstrap_pid: u32,
    /// Positive control for the "absent from the snapshot" grep: this string is
    /// created before the checkpoint, so it must be found in the memory file.
    bootstrap_marker: String,
}

/// Per-instance state created after the checkpoint.
struct Instance {
    fixed: Fixed,
    restored: bool,
    generation: u64,
    ctx_instance_id: String,
    ctx_environment_id: String,
    restored_at_ms: Option<u64>,
    started_at_ms: u64,
    instance_id: String,
    token: String,
    rng: std::sync::Mutex<XorShift64>,
    rng_first: u64,
    clock: RestoreClock,
    ticks: Arc<AtomicU64>,
    db: Mutex<DbClient>,
    db_addr: std::net::SocketAddr,
    tls: TlsFacts,
    tls_client: Mutex<Option<tokio_rustls::client::TlsStream<TcpStream>>>,
    scratch_dir: PathBuf,
    scratch_foreign_before: Vec<String>,
    after_restore_ms: u64,
}

struct RestoreClock {
    wall_ms: u64,
    mono: Instant,
    guest_uptime_ms: u64,
    timer_mono_ms: u64,
    timer_wall_ms: i64,
}

#[derive(Clone)]
struct TlsFacts {
    cert_sha256: String,
    client_exporter: String,
    server_exporter: String,
    protocol: String,
    cipher: String,
    handshake_ms: u64,
}

pub struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn expected(i: u64) -> u64 {
    splitmix64(SEED ^ i)
}

fn build_table(mib: u64) -> Vec<u64> {
    let entries = (mib * 1024 * 1024 / 8) as usize;
    (0..entries as u64).map(expected).collect()
}

/// sha256 over the little-endian bytes of the table.
fn table_sha256(table: &[u64]) -> [u8; 32] {
    let mut h = Sha256::new();
    let mut buf = Vec::with_capacity(8 * 4096);
    for chunk in table.chunks(4096) {
        buf.clear();
        for v in chunk {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        h.update(&buf);
    }
    h.finalize().into()
}

/// `d_0 = checksum`, `d_k = sha256(d_{k-1} || table)`: the stand-in for an
/// expensive, deterministic precomputation.
fn precompute(table: &[u64], checksum: [u8; 32], passes: u64) -> [u8; 32] {
    let mut d = checksum;
    for _ in 0..passes {
        let mut h = Sha256::new();
        h.update(d);
        h.update(table_sha256(table));
        d = h.finalize().into();
    }
    d
}

fn env_u64(name: &str, default: u64, max: u64) -> Result<u64, HandlerError> {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<u64>()
            .ok()
            .filter(|n| *n <= max)
            .ok_or_else(|| HandlerError::new(format!("{name} must be an integer 0..={max}"))),
        Err(_) => Ok(default),
    }
}

fn now_ms() -> u64 {
    ms(SystemTime::now())
}

fn ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn scratch_dir() -> PathBuf {
    std::env::var_os("RESTORE_VERIFY_SCRATCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn bootstrap() -> Result<Fixed, HandlerError> {
    let runs = BOOTSTRAP_RUNS.fetch_add(1, Ordering::SeqCst) + 1;
    let started = Instant::now();
    let wall = now_ms();
    let mib = env_u64(
        "RESTORE_VERIFY_DATASET_MIB",
        DEFAULT_DATASET_MIB,
        MAX_DATASET_MIB,
    )?;
    let passes = env_u64(
        "RESTORE_VERIFY_PRECOMPUTE_PASSES",
        DEFAULT_PRECOMPUTE_PASSES,
        16,
    )?;
    let table = build_table(mib);
    let checksum = table_sha256(&table);
    let chain = precompute(&table, checksum, passes);
    let env = std::env::var("TACHYON_ENVIRONMENT_ID").unwrap_or_default();
    let pid = std::process::id();
    let marker = format!("rv-bootstrap-marker-{env}-{wall}");
    let bootstrap_ms = started.elapsed().as_millis() as u64;
    // Written before the checkpoint on purpose: the snapshot's scratch copy is
    // taken at the same pause point as the memory, so every copy must see this
    // one line (and a cold start its own).
    let line = format!(
        "run={runs} pid={pid} env={env} wall_ms={wall} checksum={} bootstrap_ms={bootstrap_ms}\n",
        hex::encode(checksum)
    );
    let log = scratch_dir().join(BOOTSTRAP_LOG);
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .and_then(|mut f| f.write_all(line.as_bytes()))
        .map_err(|e| HandlerError::new(format!("cannot write {}: {e}", log.display())))?;
    eprintln!(
        "restore-verify: bootstrap run {runs}: {mib} MiB, {passes} passes, {bootstrap_ms} ms, checksum {}",
        hex::encode(checksum)
    );
    Ok(Fixed {
        table,
        checksum: hex::encode(checksum),
        precompute: hex::encode(chain),
        passes,
        bootstrap_ms,
        bootstrap_wall_ms: wall,
        bootstrap_env: env,
        bootstrap_pid: pid,
        bootstrap_marker: marker,
    })
}

fn random_bytes<const N: usize>() -> Result<[u8; N], HandlerError> {
    let mut b = [0u8; N];
    getrandom::fill(&mut b).map_err(|e| HandlerError::new(format!("getrandom: {e}")))?;
    Ok(b)
}

fn random_u64() -> Result<u64, HandlerError> {
    Ok(u64::from_le_bytes(random_bytes::<8>()?))
}

fn guest_uptime_ms() -> u64 {
    std::fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse::<f64>().ok())
        .map(|s| (s * 1000.0) as u64)
        .unwrap_or(0)
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn err(context: &str) -> impl Fn(std::io::Error) -> HandlerError + '_ {
    move |e| HandlerError::new(format!("{context}: {e}"))
}

// --- loopback "DB" --------------------------------------------------------------------------

/// Line protocol: `AUTH <token>` -> `OK <session id> <server nonce>`;
/// `WHOAMI` -> `ME <sha256(token)> <session id> <queries>`; anything else
/// before `AUTH` -> `DENIED`.
async fn db_server(listener: TcpListener, server_nonce: String) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let nonce = server_nonce.clone();
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            let mut session: Option<(String, String)> = None;
            let mut queries = 0u64;
            while let Ok(Some(line)) = lines.next_line().await {
                let reply = match (line.split_once(' '), line.as_str(), &session) {
                    (Some(("AUTH", token)), _, _) => {
                        let id = match random_u64() {
                            Ok(v) => format!("sess-{v:016x}"),
                            Err(_) => break,
                        };
                        session = Some((sha256_hex(token.as_bytes()), id.clone()));
                        format!("OK {id} {nonce}")
                    }
                    (_, "WHOAMI", Some((digest, id))) => {
                        queries += 1;
                        format!("ME {digest} {id} {queries}")
                    }
                    _ => "DENIED".to_string(),
                };
                if w.write_all(format!("{reply}\n").as_bytes()).await.is_err() {
                    break;
                }
            }
        });
    }
}

struct DbClient {
    lines: Option<(
        tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
        tokio::net::tcp::OwnedWriteHalf,
    )>,
    session_id: String,
    server_nonce: String,
    handshakes: u64,
    connect_ms: u64,
}

impl DbClient {
    async fn connect(addr: std::net::SocketAddr, token: &str) -> Result<Self, HandlerError> {
        let mut c = DbClient {
            lines: None,
            session_id: String::new(),
            server_nonce: String::new(),
            handshakes: 0,
            connect_ms: 0,
        };
        c.handshake(addr, token).await?;
        Ok(c)
    }

    async fn handshake(
        &mut self,
        addr: std::net::SocketAddr,
        token: &str,
    ) -> Result<(), HandlerError> {
        let t = Instant::now();
        let stream = TcpStream::connect(addr).await.map_err(err("db connect"))?;
        let (r, mut w) = stream.into_split();
        let mut lines = BufReader::new(r).lines();
        w.write_all(format!("AUTH {token}\n").as_bytes())
            .await
            .map_err(err("db auth write"))?;
        let reply = lines
            .next_line()
            .await
            .map_err(err("db auth read"))?
            .unwrap_or_default();
        let mut parts = reply.split(' ');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("OK"), Some(id), Some(nonce)) => {
                self.session_id = id.to_string();
                self.server_nonce = nonce.to_string();
            }
            _ => return Err(HandlerError::new(format!("db auth refused: {reply}"))),
        }
        self.lines = Some((lines, w));
        self.handshakes += 1;
        self.connect_ms = t.elapsed().as_millis() as u64;
        Ok(())
    }

    /// `WHOAMI` on the live session; one reconnect + re-auth if it broke.
    async fn whoami(
        &mut self,
        addr: std::net::SocketAddr,
        token: &str,
    ) -> Result<(String, String, u64, bool), HandlerError> {
        let mut reconnected = false;
        for _ in 0..2 {
            if let Some((lines, w)) = self.lines.as_mut() {
                let ok = w.write_all(b"WHOAMI\n").await.is_ok();
                if ok && let Ok(Some(reply)) = lines.next_line().await {
                    let p: Vec<&str> = reply.split(' ').collect();
                    if p.len() == 4 && p[0] == "ME" {
                        return Ok((
                            p[1].to_string(),
                            p[2].to_string(),
                            p[3].parse().unwrap_or(0),
                            reconnected,
                        ));
                    }
                }
            }
            self.lines = None;
            self.handshake(addr, token).await?;
            reconnected = true;
        }
        Err(HandlerError::new("db session unusable after reconnect"))
    }
}

// --- loopback TLS ---------------------------------------------------------------------------

const EXPORTER_LABEL: &[u8] = b"EXPORTER-tachyon-restore-verify";

async fn tls_setup() -> Result<(TlsFacts, tokio_rustls::client::TlsStream<TcpStream>), HandlerError>
{
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};

    let t = Instant::now();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // Generated after the restore: every copy has its own key pair.
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| HandlerError::new(format!("rcgen: {e}")))?;
    let cert_der: CertificateDer<'static> = certified.cert.der().clone();
    let key_der =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()));
    let tls_err = |e: rustls::Error| HandlerError::new(format!("rustls: {e}"));
    let server_cfg = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(tls_err)?
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .map_err(tls_err)?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der.clone()).map_err(tls_err)?;
    let client_cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(tls_err)?
        .with_root_certificates(roots)
        .with_no_client_auth();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(err("tls bind"))?;
    let addr = listener.local_addr().map_err(err("tls addr"))?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
    let (exporter_tx, exporter_rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(async move {
        let mut exporter_tx = Some(exporter_tx);
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut tls) = acceptor.accept(tcp).await else {
                continue;
            };
            let exp = tls
                .get_ref()
                .1
                .export_keying_material([0u8; 32], EXPORTER_LABEL, None)
                .map(hex::encode)
                .unwrap_or_default();
            if let Some(tx) = exporter_tx.take() {
                let _ = tx.send(exp.clone());
            }
            tokio::spawn(async move {
                let mut buf = [0u8; 4];
                // PING -> PONG <server exporter prefix>, until the client goes away.
                while tls.read_exact(&mut buf).await.is_ok() {
                    let reply = format!("PONG {}\n", &exp[..16.min(exp.len())]);
                    if tls.write_all(reply.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let tcp = TcpStream::connect(addr).await.map_err(err("tls connect"))?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));
    let name = ServerName::try_from("localhost")
        .map_err(|e| HandlerError::new(format!("server name: {e}")))?;
    let client = connector
        .connect(name, tcp)
        .await
        .map_err(err("tls handshake"))?;
    let conn = client.get_ref().1;
    let client_exporter = conn
        .export_keying_material([0u8; 32], EXPORTER_LABEL, None)
        .map(hex::encode)
        .map_err(tls_err)?;
    let protocol = conn
        .protocol_version()
        .map(|v| format!("{v:?}"))
        .unwrap_or_default();
    let cipher = conn
        .negotiated_cipher_suite()
        .map(|s| format!("{:?}", s.suite()))
        .unwrap_or_default();
    let server_exporter = tokio::time::timeout(Duration::from_secs(5), exporter_rx)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    Ok((
        TlsFacts {
            cert_sha256: sha256_hex(cert_der.as_ref()),
            client_exporter,
            server_exporter,
            protocol,
            cipher,
            handshake_ms: t.elapsed().as_millis() as u64,
        },
        client,
    ))
}

// --- scratch --------------------------------------------------------------------------------

fn instance_files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with(INSTANCE_PREFIX))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn scratch_listing(dir: &Path) -> Vec<Value> {
    let mut entries: Vec<Value> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                .map(|e| {
                    let content = std::fs::read_to_string(e.path()).unwrap_or_default();
                    json!({
                        "name": e.file_name().to_string_lossy(),
                        "bytes": content.len(),
                        "lines": content.lines().take(8).collect::<Vec<_>>(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    entries.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    entries
}

// --- lifecycle ------------------------------------------------------------------------------

async fn after_restore(fixed: Fixed, ctx: RestoreContext) -> Result<Instance, HandlerError> {
    after_restore_in(fixed, ctx, scratch_dir()).await
}

async fn after_restore_in(
    fixed: Fixed,
    ctx: RestoreContext,
    dir: PathBuf,
) -> Result<Instance, HandlerError> {
    let started = Instant::now();
    let wall_ms = now_ms();
    let mono = Instant::now();
    let guest_uptime = guest_uptime_ms();
    let instance_id = format!("{}-{:016x}", ctx.instance_id, random_u64()?);
    let token = format!("rvtok-{}", hex::encode(random_bytes::<24>()?));
    let mut rng = XorShift64::new(random_u64()?);
    let rng_first = rng.next();

    // Timer: a Tokio sleep measured on both clocks. A wall clock that jumps
    // (e.g. set by the restore frame in the middle) shows up as a difference.
    let (t0, w0) = (Instant::now(), SystemTime::now());
    tokio::time::sleep(Duration::from_millis(TIMER_MS)).await;
    let timer_mono_ms = t0.elapsed().as_millis() as u64;
    let timer_wall_ms = SystemTime::now()
        .duration_since(w0)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_else(|e| -(e.duration().as_millis() as i64));

    let ticks = Arc::new(AtomicU64::new(0));
    let t = ticks.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_millis(TICK_MS));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            iv.tick().await;
            t.fetch_add(1, Ordering::Relaxed);
        }
    });

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(err("db bind"))?;
    let db_addr = listener.local_addr().map_err(err("db addr"))?;
    let server_nonce = format!("srv-{:016x}", random_u64()?);
    tokio::spawn(db_server(listener, server_nonce));
    let db = DbClient::connect(db_addr, &token).await?;

    let (tls, tls_client) = tls_setup().await?;

    let scratch_foreign_before = instance_files(&dir);
    let own = dir.join(format!("{INSTANCE_PREFIX}{instance_id}"));
    std::fs::write(
        &own,
        format!(
            "instance={instance_id}\ntoken_sha256={}\nsession={}\n",
            sha256_hex(token.as_bytes()),
            db.session_id
        ),
    )
    .map_err(|e| HandlerError::new(format!("cannot write {}: {e}", own.display())))?;

    eprintln!(
        "restore-verify: after_restore: restored={} generation={} instance={instance_id}",
        ctx.restored, ctx.generation
    );
    Ok(Instance {
        restored: ctx.restored,
        generation: ctx.generation,
        ctx_instance_id: ctx.instance_id.clone(),
        ctx_environment_id: ctx.environment_id.clone(),
        restored_at_ms: ctx.restored_at.map(ms),
        started_at_ms: ms(ctx.started_at),
        fixed,
        instance_id,
        token,
        rng: std::sync::Mutex::new(rng),
        rng_first,
        clock: RestoreClock {
            wall_ms,
            mono,
            guest_uptime_ms: guest_uptime,
            timer_mono_ms,
            timer_wall_ms,
        },
        ticks,
        db: Mutex::new(db),
        db_addr,
        tls,
        tls_client: Mutex::new(Some(tls_client)),
        scratch_dir: dir,
        scratch_foreign_before,
        after_restore_ms: started.elapsed().as_millis() as u64,
    })
}

async fn handle(state: &Instance, payload: &Value) -> Result<Value, HandlerError> {
    let t = Instant::now();
    let f = &state.fixed;
    let entries = f.table.len() as u64;
    let (i, value, spot_ok, rng_next) = {
        let mut rng = state
            .rng
            .lock()
            .map_err(|_| HandlerError::new("rng poisoned"))?;
        let i = match payload.get("i") {
            Some(v) => v.as_u64().ok_or_else(|| {
                HandlerError::with_type("Handler.InvalidPayload", "\"i\" must be an integer")
            })?,
            None => rng.next() % entries.max(1),
        };
        if i >= entries {
            return Err(HandlerError::with_type(
                "Handler.InvalidPayload",
                format!("i must be below {entries}"),
            ));
        }
        let value = f.table[i as usize];
        let spot_ok = (0..SPOT_CHECKS).all(|_| {
            let j = (rng.next() % entries.max(1)) as usize;
            f.table.get(j).copied() == Some(expected(j as u64))
        });
        (i, value, spot_ok, rng.next())
    };

    let verify = payload
        .get("verify")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let full = if verify {
        let v = Instant::now();
        let sum = hex::encode(table_sha256(&f.table));
        Some(
            json!({"checksum_now": sum, "ok": sum == f.checksum, "ms": v.elapsed().as_millis() as u64}),
        )
    } else {
        None
    };

    // Timer + clocks now.
    let (t0, w0) = (Instant::now(), SystemTime::now());
    tokio::time::sleep(Duration::from_millis(TIMER_MS)).await;
    let timer_mono_ms = t0.elapsed().as_millis() as u64;
    let timer_wall_ms = SystemTime::now()
        .duration_since(w0)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_else(|e| -(e.duration().as_millis() as i64));
    let wall_now = now_ms();
    let mono_since = state.clock.mono.elapsed().as_millis() as u64;
    let wall_since = wall_now as i64 - state.clock.wall_ms as i64;

    let db = {
        let mut db = state.db.lock().await;
        match db.whoami(state.db_addr, &state.token).await {
            Ok((digest, session, queries, reconnected)) => json!({
                "ok": digest == sha256_hex(state.token.as_bytes()) && session == db.session_id,
                "session_id": session,
                "server_nonce": db.server_nonce,
                "token_sha256_seen_by_server": digest,
                "queries": queries,
                "handshakes": db.handshakes,
                "reconnected_now": reconnected,
                "connect_ms": db.connect_ms,
            }),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        }
    };

    let tls_ping = {
        let mut guard = state.tls_client.lock().await;
        match guard.as_mut() {
            Some(c) => {
                let res = async {
                    c.write_all(b"PING").await?;
                    let mut buf = Vec::new();
                    let mut byte = [0u8; 1];
                    while c.read_exact(&mut byte).await.is_ok() && byte[0] != b'\n' {
                        buf.push(byte[0]);
                    }
                    Ok::<_, std::io::Error>(String::from_utf8_lossy(&buf).into_owned())
                }
                .await;
                match res {
                    Ok(reply) => json!({"ok": reply.starts_with("PONG "), "reply": reply}),
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                }
            }
            None => json!({"ok": false, "error": "no tls session"}),
        }
    };
    let tls = &state.tls;

    let dir = &state.scratch_dir;
    let own_name = format!("{INSTANCE_PREFIX}{}", state.instance_id);
    let files_now = instance_files(dir);
    let foreign: Vec<&String> = files_now.iter().filter(|n| **n != own_name).collect();
    let bootstrap_log = std::fs::read_to_string(dir.join(BOOTSTRAP_LOG)).unwrap_or_default();
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .unwrap_or_default()
        .trim()
        .to_string();

    Ok(json!({
        "sample": "restore-verify",
        "restored": state.restored,
        "generation": state.generation,
        "ctx_instance_id": state.ctx_instance_id,
        "ctx_environment_id": state.ctx_environment_id,
        "restored_at_ms": state.restored_at_ms,
        "started_at_ms": state.started_at_ms,
        "instance_id": state.instance_id,
        "token": state.token,
        "token_sha256": sha256_hex(state.token.as_bytes()),
        "guest_boot_id": boot_id,
        "after_restore_ms": state.after_restore_ms,
        "dataset": {
            "entries": entries,
            "mib": entries * 8 / (1024 * 1024),
            "checksum": f.checksum,
            "precompute": f.precompute,
            "precompute_passes": f.passes,
            "bootstrap_ms": f.bootstrap_ms,
            "bootstrap_wall_ms": f.bootstrap_wall_ms,
            "bootstrap_env": f.bootstrap_env,
            "bootstrap_pid": f.bootstrap_pid,
            "bootstrap_runs_in_process": BOOTSTRAP_RUNS.load(Ordering::SeqCst),
            "bootstrap_marker": f.bootstrap_marker,
            "lookup": {"i": i, "value": value, "ok": value == expected(i)},
            "spot_checks": SPOT_CHECKS,
            "spot_ok": spot_ok,
            "full": full,
        },
        "rng": {"first": state.rng_first, "next": rng_next},
        "clock": {
            "wall_now_ms": wall_now,
            "after_restore_wall_ms": state.clock.wall_ms,
            "guest_uptime_at_after_restore_ms": state.clock.guest_uptime_ms,
            "guest_uptime_now_ms": guest_uptime_ms(),
            "after_restore_timer": {"requested_ms": TIMER_MS, "mono_ms": state.clock.timer_mono_ms, "wall_ms": state.clock.timer_wall_ms},
            "handler_timer": {"requested_ms": TIMER_MS, "mono_ms": timer_mono_ms, "wall_ms": timer_wall_ms},
            "mono_since_after_restore_ms": mono_since,
            "wall_since_after_restore_ms": wall_since,
            "ticks": state.ticks.load(Ordering::Relaxed),
            "tick_ms": TICK_MS,
        },
        "db": db,
        "tls": {
            "cert_sha256": tls.cert_sha256,
            "client_exporter": tls.client_exporter,
            "server_exporter": tls.server_exporter,
            "exporters_match": !tls.client_exporter.is_empty() && tls.client_exporter == tls.server_exporter,
            "protocol": tls.protocol,
            "cipher": tls.cipher,
            "handshake_ms": tls.handshake_ms,
            "ping": tls_ping,
        },
        "scratch": {
            "dir": dir.display().to_string(),
            "own_file": own_name,
            "own_present": files_now.contains(&own_name),
            "foreign_instance_files": foreign,
            "foreign_before_own_write": state.scratch_foreign_before,
            "bootstrap_log_lines": bootstrap_log.lines().collect::<Vec<_>>(),
            "entries": scratch_listing(dir),
        },
        "handler_ms": t.elapsed().as_millis() as u64,
    }))
}

fn main() -> Result<(), SdkError> {
    lifecycle::builder()
        .bootstrap(bootstrap)
        .after_restore(after_restore)
        .run(
            |state: Arc<Instance>, event: Event, _ctx: Context| async move {
                handle(&state, event.json()).await
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_deterministic_with_a_known_checksum() {
        let t = build_table(1);
        assert_eq!(t.len(), 131_072);
        assert_eq!(t[7], expected(7));
        let sum = hex::encode(table_sha256(&t));
        assert_eq!(sum, hex::encode(table_sha256(&build_table(1))));
        assert_eq!(
            sum,
            "ab20c46cacae7ae8e71241f5bd9d5ffab6a18c0b15591d27c4160a76b1686270"
        );
        let chain0 = precompute(&t, table_sha256(&t), 0);
        assert_eq!(chain0, table_sha256(&t));
        assert_ne!(precompute(&t, table_sha256(&t), 1), chain0);
    }

    /// The default 64 MiB table (slow in a debug build):
    /// `cargo test --release -p example-restore-verify -- --ignored`.
    #[test]
    #[ignore]
    fn default_table_checksum() {
        let sum = hex::encode(table_sha256(&build_table(DEFAULT_DATASET_MIB)));
        assert_eq!(
            sum,
            "e5aa88d035ed23006cb25d35e12e4cd66d794bbcdc9bde45c5af33c548b11c94"
        );
    }

    fn ctx(instance: &str) -> RestoreContext {
        RestoreContext {
            restored: true,
            instance_id: instance.into(),
            generation: 1,
            restored_at: Some(SystemTime::now()),
            started_at: SystemTime::now(),
            environment_id: "env_src".into(),
        }
    }

    fn fixed() -> Fixed {
        let table = build_table(1);
        let sum = table_sha256(&table);
        Fixed {
            checksum: hex::encode(sum),
            precompute: hex::encode(precompute(&table, sum, 1)),
            table,
            passes: 1,
            bootstrap_ms: 0,
            bootstrap_wall_ms: 0,
            bootstrap_env: "env_src".into(),
            bootstrap_pid: 1,
            bootstrap_marker: "m".into(),
        }
    }

    #[tokio::test]
    async fn copies_diverge_and_share_the_fixed_data() {
        let dir = std::env::temp_dir().join(format!("rv-test-{:016x}", random_u64().unwrap()));
        std::fs::create_dir_all(&dir).unwrap();
        let a = after_restore_in(fixed(), ctx("rst_a"), dir.clone())
            .await
            .unwrap();
        let b = after_restore_in(fixed(), ctx("rst_b"), dir.clone())
            .await
            .unwrap();
        let ra = handle(&a, &json!({"i": 5, "verify": true})).await.unwrap();
        let rb = handle(&b, &json!({"i": 5, "verify": true})).await.unwrap();
        assert_eq!(ra["dataset"]["checksum"], rb["dataset"]["checksum"]);
        assert_eq!(ra["dataset"]["full"]["ok"], true);
        assert_eq!(ra["dataset"]["lookup"]["ok"], true);
        assert_eq!(ra["dataset"]["spot_ok"], true);
        for key in ["instance_id", "token"] {
            assert_ne!(ra[key], rb[key], "{key}");
        }
        assert_ne!(ra["rng"]["first"], rb["rng"]["first"]);
        assert_ne!(ra["db"]["session_id"], rb["db"]["session_id"]);
        assert_eq!(ra["db"]["ok"], true);
        assert_ne!(ra["tls"]["cert_sha256"], rb["tls"]["cert_sha256"]);
        assert_eq!(ra["tls"]["exporters_match"], true);
        assert_eq!(ra["tls"]["ping"]["ok"], true);
        assert_ne!(ra["tls"]["client_exporter"], rb["tls"]["client_exporter"]);
        // Same directory here, so each sees the other's file as foreign: the
        // check is that the sample reports it (a clone must report none).
        assert_eq!(
            rb["scratch"]["foreign_instance_files"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(ra["scratch"]["own_present"], true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn db_session_reconnects_and_reauthenticates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(db_server(listener, "srv-x".into()));
        let mut c = DbClient::connect(addr, "tok").await.unwrap();
        let first = c.session_id.clone();
        c.lines = None; // the connection is gone
        let (digest, session, queries, reconnected) = c.whoami(addr, "tok").await.unwrap();
        assert!(reconnected);
        assert_eq!(digest, sha256_hex(b"tok"));
        assert_ne!(session, first);
        assert_eq!(queries, 1);
        assert_eq!(c.handshakes, 2);
    }
}
