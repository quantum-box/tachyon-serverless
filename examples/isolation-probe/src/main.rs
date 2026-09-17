//! `isolation-probe`: the guest side of the ADR-0001 M8 / M9 measurements.
//!
//! `scripts/kvm/measure-isolation.sh` deploys this function on a real
//! Firecracker guest and reads the JSON it answers. The payload selects the
//! probe:
//!
//! - `{"probe":"egress"}` (M8) opens a TCP connection, with a short timeout, to
//!   targets that must be unreachable while the guest has no network device
//!   (`1.1.1.1:443`, the link-local metadata address `169.254.169.254:80` and
//!   the host-side gateway candidate `10.0.2.2:80`) and resolves a public name.
//!   Every attempt is expected to fail; a success is the finding, never a
//!   precondition. The record also lists the guest interfaces (`/proc/net/dev`)
//!   and routes (`/proc/net/route`), so it shows whether the guest really only
//!   has loopback.
//! - `{"probe":"resources"}` (M9) reports the CPU count the guest sees
//!   (`/proc/cpuinfo` entries and `available_parallelism`), `MemTotal` from
//!   `/proc/meminfo` and the cgroup limits if any are visible. With
//!   `{"probe":"resources","alloc_mib":N}` it also allocates and touches N MiB
//!   in 16 MiB chunks (`chunk_mib` overrides the size) and reports how much it
//!   touched. Progress is printed to stdout after every chunk and flushed,
//!   because driving the allocation past the configured memory ends in a kernel
//!   kill that the host observes as a crash.
//! - `{"probe":"disk","fill_mib":N}` (PLT-4622) writes a file of up to N MiB
//!   (default and maximum 64 GiB, i.e. "until the disk is full") into `dir`
//!   (default `/tmp`) in 1 MiB chunks until the write fails, and reports how
//!   much was written, why it stopped (`enospc`, `limit` or `error`), the
//!   file system before / after (`statvfs`, `/proc/mounts`) and whether `/`
//!   and `/function` refuse writes. The file is removed afterwards unless
//!   `keep` is true, so a reused environment gets its space back.
//! - `{"probe":"all"}` (the default) runs egress and resources; the allocation
//!   step still only runs when `alloc_mib` is set, and the disk probe only runs
//!   when asked for by name.
//!
//! Other payload keys: `targets` (array of `host:port`), `dns_name`,
//! `connect_timeout_ms` (default 2000), `dns_timeout_ms` (default 5000).
//!
//! Only `std` and `libc` (for `statvfs`) are used — no network crates — so the
//! example links statically for `aarch64-unknown-linux-musl` and
//! `x86_64-unknown-linux-musl` without extra system libraries.

use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use tachyon_serverless_sdk::{Context, Event, HandlerError, SdkError};

/// Targets that must stay unreachable while the guest has no NIC.
const DEFAULT_TARGETS: [&str; 3] = ["1.1.1.1:443", "169.254.169.254:80", "10.0.2.2:80"];
const DEFAULT_DNS_NAME: &str = "example.com";
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_DNS_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_CHUNK_MIB: usize = 16;
/// Upper bound for any single wait, so a hung resolver cannot eat the whole
/// handler deadline.
const MAX_TIMEOUT_MS: u64 = 60_000;
/// Upper bound for `alloc_mib`, so a typo cannot ask for terabytes.
const MAX_ALLOC_MIB: usize = 64 * 1024;
/// Upper bound (and default) for `fill_mib`: large enough to mean "until full".
const MAX_FILL_MIB: usize = 64 * 1024;
const DEFAULT_FILL_DIR: &str = "/tmp";
/// Paths that must refuse writes in a Firecracker guest (read-only drives).
const READ_ONLY_PROBES: [&str; 2] = ["/", "/function"];
const MIB: usize = 1024 * 1024;
const PAGE_BYTES: usize = 4096;

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    eprintln!(
        "isolation-probe: starting (arch={} unisolated={})",
        std::env::consts::ARCH,
        tachyon_serverless_sdk::is_unisolated()
    );
    tachyon_serverless_sdk::run(handler).await
}

async fn handler(event: Event, ctx: Context) -> Result<Value, HandlerError> {
    let request = ProbeRequest::from_payload(event.json())?;
    log_progress(&format!(
        "probe={} invocation={} attempt={} alloc_mib={}",
        request.probe.as_str(),
        ctx.invocation_id,
        ctx.attempt_id,
        request.alloc_mib
    ));
    let guest = json!({
        "architecture": std::env::consts::ARCH,
        "os": std::env::consts::OS,
        "unisolated": ctx.is_unisolated(),
        "environment_id": ctx.environment_id,
        "invocation_id": ctx.invocation_id,
        "attempt_id": ctx.attempt_id,
        // Changes with every microVM boot; `null` outside a Linux guest.
        "boot_id": read_trimmed("/proc/sys/kernel/random/boot_id"),
    });
    // The probes block (connect timeouts, page faults): keep them off the
    // async runtime threads.
    tokio::task::spawn_blocking(move || run(&request, guest))
        .await
        .map_err(|e| HandlerError::new(format!("probe task failed: {e}")))
}

/// Build the report for one request.
fn run(request: &ProbeRequest, guest: Value) -> Value {
    let mut report = Map::new();
    report.insert("probe".into(), request.probe.as_str().into());
    report.insert("guest".into(), guest);
    report.insert("interfaces".into(), interfaces_report());
    if request.probe.runs_egress() {
        report.insert("egress".into(), egress_report(request));
    }
    if request.probe.runs_resources() {
        report.insert("resources".into(), resources_report(request));
    }
    if request.probe == Probe::Disk {
        report.insert("disk".into(), disk_report(request));
    }
    Value::Object(report)
}

// ---------------------------------------------------------------------------
// request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Probe {
    Egress,
    Resources,
    Disk,
    All,
}

impl Probe {
    fn as_str(self) -> &'static str {
        match self {
            Self::Egress => "egress",
            Self::Resources => "resources",
            Self::Disk => "disk",
            Self::All => "all",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "egress" => Some(Self::Egress),
            "resources" | "resource" => Some(Self::Resources),
            "disk" => Some(Self::Disk),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    fn runs_egress(self) -> bool {
        matches!(self, Self::Egress | Self::All)
    }

    fn runs_resources(self) -> bool {
        matches!(self, Self::Resources | Self::All)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbeRequest {
    probe: Probe,
    targets: Vec<String>,
    dns_name: String,
    connect_timeout: Duration,
    dns_timeout: Duration,
    alloc_mib: usize,
    chunk_mib: usize,
    fill_mib: usize,
    fill_dir: String,
    keep_fill: bool,
}

impl ProbeRequest {
    fn from_payload(payload: &Value) -> Result<Self, HandlerError> {
        let probe = match payload.get("probe") {
            None | Some(Value::Null) => Probe::All,
            Some(Value::String(name)) => Probe::parse(name).ok_or_else(|| {
                HandlerError::with_type(
                    "Probe.Unknown",
                    format!("unknown probe `{name}` (expected egress, resources, disk or all)"),
                )
            })?,
            Some(other) => {
                return Err(HandlerError::with_type(
                    "Probe.InvalidPayload",
                    format!("`probe` must be a string, got {other}"),
                ));
            }
        };
        let targets = match payload.get("targets") {
            None | Some(Value::Null) => DEFAULT_TARGETS.iter().map(|t| (*t).to_string()).collect(),
            Some(Value::Array(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let target = item.as_str().ok_or_else(|| {
                        HandlerError::with_type(
                            "Probe.InvalidPayload",
                            "`targets` must be an array of \"address:port\" strings",
                        )
                    })?;
                    out.push(target.to_string());
                }
                out
            }
            Some(other) => {
                return Err(HandlerError::with_type(
                    "Probe.InvalidPayload",
                    format!("`targets` must be an array, got {other}"),
                ));
            }
        };
        Ok(Self {
            probe,
            targets,
            dns_name: payload
                .get("dns_name")
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_DNS_NAME)
                .to_string(),
            connect_timeout: duration_field(
                payload,
                "connect_timeout_ms",
                DEFAULT_CONNECT_TIMEOUT_MS,
            ),
            dns_timeout: duration_field(payload, "dns_timeout_ms", DEFAULT_DNS_TIMEOUT_MS),
            alloc_mib: usize_field(payload, "alloc_mib", 0, MAX_ALLOC_MIB),
            chunk_mib: usize_field(payload, "chunk_mib", DEFAULT_CHUNK_MIB, MAX_ALLOC_MIB).max(1),
            fill_mib: usize_field(payload, "fill_mib", MAX_FILL_MIB, MAX_FILL_MIB),
            fill_dir: payload
                .get("dir")
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_FILL_DIR)
                .to_string(),
            keep_fill: payload
                .get("keep")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }
}

/// A millisecond field, clamped to `1..=MAX_TIMEOUT_MS`.
fn duration_field(payload: &Value, field: &str, default_ms: u64) -> Duration {
    let ms = payload
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or(default_ms)
        .clamp(1, MAX_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// A non-negative integer field, capped at `max`.
fn usize_field(payload: &Value, field: &str, default: usize, max: usize) -> usize {
    let raw = payload
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or(default as u64);
    raw.min(max as u64) as usize
}

// ---------------------------------------------------------------------------
// M8: egress
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnectRecord {
    target: String,
    /// False when the target could not even be parsed into an address.
    attempted: bool,
    connected: bool,
    peer: Option<String>,
    error_kind: Option<String>,
    os_error: Option<i32>,
    error: Option<String>,
    elapsed_ms: u64,
}

impl ConnectRecord {
    fn to_json(&self) -> Value {
        json!({
            "target": self.target,
            "attempted": self.attempted,
            "connected": self.connected,
            "peer": self.peer,
            "error_kind": self.error_kind,
            "os_error": self.os_error,
            "error": self.error,
            "elapsed_ms": self.elapsed_ms,
        })
    }

    fn summary(&self) -> String {
        if self.connected {
            format!("CONNECTED (peer {})", self.peer.as_deref().unwrap_or("?"))
        } else if !self.attempted {
            format!("not attempted: {}", self.error.as_deref().unwrap_or("?"))
        } else {
            format!(
                "failed ({})",
                self.error_kind.as_deref().unwrap_or("unknown kind")
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DnsRecord {
    name: String,
    resolved: bool,
    addresses: Vec<String>,
    error_kind: Option<String>,
    error: Option<String>,
    /// The resolver did not answer within the probe timeout.
    timed_out: bool,
    elapsed_ms: u64,
}

impl DnsRecord {
    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "resolved": self.resolved,
            "addresses": self.addresses,
            "error_kind": self.error_kind,
            "error": self.error,
            "timed_out": self.timed_out,
            "elapsed_ms": self.elapsed_ms,
        })
    }
}

/// True when anything answered: a completed TCP connection, or a name that
/// resolved (which means a resolver was reachable).
fn reached_network(targets: &[ConnectRecord], dns: &DnsRecord) -> bool {
    targets.iter().any(|t| t.connected) || dns.resolved
}

/// One TCP connect attempt. Success is the finding, never a precondition: the
/// stream is closed immediately and nothing is sent or read.
fn connect_probe(target: &str, timeout: Duration) -> ConnectRecord {
    let started = Instant::now();
    let addr: SocketAddr = match target.parse() {
        Ok(addr) => addr,
        Err(e) => {
            return ConnectRecord {
                target: target.to_string(),
                attempted: false,
                connected: false,
                peer: None,
                error_kind: Some("InvalidTarget".to_string()),
                os_error: None,
                error: Some(e.to_string()),
                elapsed_ms: 0,
            };
        }
    };
    let outcome = TcpStream::connect_timeout(&addr, timeout);
    let elapsed = elapsed_ms(started);
    match outcome {
        Ok(stream) => {
            let peer = stream.peer_addr().ok().map(|a| a.to_string());
            drop(stream);
            ConnectRecord {
                target: target.to_string(),
                attempted: true,
                connected: true,
                peer,
                error_kind: None,
                os_error: None,
                error: None,
                elapsed_ms: elapsed,
            }
        }
        Err(e) => ConnectRecord {
            target: target.to_string(),
            attempted: true,
            connected: false,
            peer: None,
            error_kind: Some(format!("{:?}", e.kind())),
            os_error: e.raw_os_error(),
            error: Some(e.to_string()),
            elapsed_ms: elapsed,
        },
    }
}

/// Resolve `name` with a bounded wait. The resolver runs on its own thread so
/// that a stuck lookup (musl retries its nameservers for seconds) cannot spend
/// the handler deadline.
fn resolve_probe(name: &str, timeout: Duration) -> DnsRecord {
    let started = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    let query = format!("{name}:80");
    std::thread::spawn(move || {
        let outcome = query
            .to_socket_addrs()
            .map(|addrs| addrs.map(|addr| addr.to_string()).collect::<Vec<_>>());
        let outcome = outcome.map_err(|e| (format!("{:?}", e.kind()), e.to_string()));
        // The receiver is gone after a probe timeout; dropping the result is fine.
        let _ = tx.send(outcome);
    });
    let base = DnsRecord {
        name: name.to_string(),
        resolved: false,
        addresses: Vec::new(),
        error_kind: None,
        error: None,
        timed_out: false,
        elapsed_ms: 0,
    };
    match rx.recv_timeout(timeout) {
        Ok(Ok(addresses)) => DnsRecord {
            resolved: !addresses.is_empty(),
            addresses,
            elapsed_ms: elapsed_ms(started),
            ..base
        },
        Ok(Err((kind, message))) => DnsRecord {
            error_kind: Some(kind),
            error: Some(message),
            elapsed_ms: elapsed_ms(started),
            ..base
        },
        Err(_) => DnsRecord {
            error_kind: Some("ProbeTimeout".to_string()),
            error: Some(format!(
                "no resolver answer within {} ms",
                timeout.as_millis()
            )),
            timed_out: true,
            elapsed_ms: elapsed_ms(started),
            ..base
        },
    }
}

fn egress_report(request: &ProbeRequest) -> Value {
    let targets: Vec<ConnectRecord> = request
        .targets
        .iter()
        .map(|target| {
            let record = connect_probe(target, request.connect_timeout);
            log_progress(&format!(
                "connect {} -> {} in {} ms",
                record.target,
                record.summary(),
                record.elapsed_ms
            ));
            record
        })
        .collect();
    let dns = resolve_probe(&request.dns_name, request.dns_timeout);
    log_progress(&format!(
        "dns {} -> resolved={} in {} ms",
        dns.name, dns.resolved, dns.elapsed_ms
    ));
    json!({
        "connect_timeout_ms": request.connect_timeout.as_millis() as u64,
        "dns_timeout_ms": request.dns_timeout.as_millis() as u64,
        "targets": targets.iter().map(ConnectRecord::to_json).collect::<Vec<_>>(),
        "dns": dns.to_json(),
        "tcp_connected": targets.iter().any(|t| t.connected),
        "dns_resolved": dns.resolved,
        "reached_network": reached_network(&targets, &dns),
        "expectation": "every target must fail to connect while the guest has no network device (ADR-0001 M8)",
    })
}

// ---------------------------------------------------------------------------
// interfaces
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Interface {
    name: String,
    rx_bytes: u64,
    tx_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Route {
    iface: String,
    destination: String,
    gateway: String,
    flags: String,
}

/// Parse `/proc/net/dev`: two header lines, then `  <name>: <rx fields...> <tx fields...>`.
fn parse_proc_net_dev(text: &str) -> Vec<Interface> {
    let mut out = Vec::new();
    for line in text.lines().skip(2) {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let field = |index: usize| {
            fields
                .get(index)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        };
        out.push(Interface {
            name: name.to_string(),
            rx_bytes: field(0),
            // Receive has 8 columns; transmit starts at the ninth.
            tx_bytes: field(8),
        });
    }
    out
}

/// Parse `/proc/net/route`: a header line, then `Iface Destination Gateway Flags ...`
/// with hexadecimal, little-endian addresses.
fn parse_proc_net_route(text: &str) -> Vec<Route> {
    let mut out = Vec::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 4 || fields[0].eq_ignore_ascii_case("iface") {
            continue;
        }
        out.push(Route {
            iface: fields[0].to_string(),
            destination: fields[1].to_string(),
            gateway: fields[2].to_string(),
            flags: fields[3].to_string(),
        });
    }
    out
}

/// A route to `0.0.0.0` is a default route: none may exist without a NIC.
fn is_default_route(route: &Route) -> bool {
    route.destination.trim_start_matches('0').is_empty() && !route.destination.is_empty()
}

fn interfaces_report() -> Value {
    let dev = std::fs::read_to_string("/proc/net/dev").ok();
    let route = std::fs::read_to_string("/proc/net/route").ok();
    let interfaces: Vec<Interface> = dev.as_deref().map(parse_proc_net_dev).unwrap_or_default();
    let routes: Vec<Route> = route
        .as_deref()
        .map(parse_proc_net_route)
        .unwrap_or_default();
    let non_loopback: Vec<&str> = interfaces
        .iter()
        .map(|i| i.name.as_str())
        .filter(|name| *name != "lo")
        .collect();
    // Unknown rather than `true` when /proc is not mounted (e.g. on a dev host).
    let loopback_only = match dev.is_some() {
        true => Value::Bool(non_loopback.is_empty() && !interfaces.is_empty()),
        false => Value::Null,
    };
    json!({
        "source": "/proc/net/dev",
        "available": dev.is_some(),
        "names": interfaces.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
        "non_loopback": non_loopback,
        "loopback_only": loopback_only,
        "entries": interfaces.iter().map(|i| json!({
            "name": i.name,
            "rx_bytes": i.rx_bytes,
            "tx_bytes": i.tx_bytes,
        })).collect::<Vec<_>>(),
        "routes": {
            "source": "/proc/net/route",
            "available": route.is_some(),
            "default_routes": routes.iter().filter(|r| is_default_route(r)).count(),
            "entries": routes.iter().map(|r| json!({
                "iface": r.iface,
                "destination": r.destination,
                "gateway": r.gateway,
                "flags": r.flags,
            })).collect::<Vec<_>>(),
        },
    })
}

// ---------------------------------------------------------------------------
// M9: resources
// ---------------------------------------------------------------------------

/// `MemTotal:       249876 kB` -> `249876`.
fn parse_meminfo_kib(text: &str, key: &str) -> Option<u64> {
    for line in text.lines() {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        if name.trim() != key {
            continue;
        }
        return rest.split_whitespace().next()?.parse().ok();
    }
    None
}

/// Number of `processor` entries in `/proc/cpuinfo`.
fn count_cpuinfo_processors(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            line.split_once(':')
                .is_some_and(|(key, _)| key.trim() == "processor")
        })
        .count()
}

/// cgroup v2 `memory.max`: a byte count, or `max` for "no limit" (`None`).
fn parse_cgroup_memory_max(raw: &str) -> Option<u64> {
    raw.trim().parse().ok()
}

/// cgroup v2 `cpu.max`: `<quota|max> <period>` in microseconds.
fn parse_cgroup_cpu_max(raw: &str) -> (Option<u64>, Option<u64>) {
    let mut fields = raw.split_whitespace();
    let quota = fields.next().and_then(|v| v.parse().ok());
    let period = fields.next().and_then(|v| v.parse().ok());
    (quota, period)
}

fn cgroup_report() -> Value {
    let memory_max = read_trimmed("/sys/fs/cgroup/memory.max");
    let cpu_max = read_trimmed("/sys/fs/cgroup/cpu.max");
    let memory_max_bytes = memory_max.as_deref().and_then(parse_cgroup_memory_max);
    let (cpu_quota_us, cpu_period_us) = cpu_max
        .as_deref()
        .map(parse_cgroup_cpu_max)
        .unwrap_or((None, None));
    json!({
        "available": memory_max.is_some() || cpu_max.is_some(),
        "memory_max": memory_max,
        "memory_max_bytes": memory_max_bytes,
        "cpu_max": cpu_max,
        "cpu_quota_us": cpu_quota_us,
        "cpu_period_us": cpu_period_us,
    })
}

fn resources_report(request: &ProbeRequest) -> Value {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok();
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    let mem_total_kib = meminfo
        .as_deref()
        .and_then(|text| parse_meminfo_kib(text, "MemTotal"));
    let alloc = if request.alloc_mib > 0 {
        allocate_and_touch(request.alloc_mib, request.chunk_mib)
    } else {
        Value::Null
    };
    json!({
        "cpuinfo_processors": cpuinfo.as_deref().map(count_cpuinfo_processors),
        "available_parallelism": std::thread::available_parallelism().map(|n| n.get()).ok(),
        "mem_total_kib": mem_total_kib,
        "mem_total_mib": mem_total_kib.map(|kib| kib / 1024),
        "cgroup": cgroup_report(),
        "alloc": alloc,
        "expectation": "the vCPU count and MemTotal must match the machine-config the host asked for (ADR-0001 M9)",
    })
}

/// MiB per chunk: `chunk_mib` each, the last one carrying the remainder.
fn chunk_plan(alloc_mib: usize, chunk_mib: usize) -> Vec<usize> {
    let chunk = chunk_mib.max(1);
    let mut plan = Vec::new();
    let mut left = alloc_mib;
    while left > 0 {
        let take = left.min(chunk);
        plan.push(take);
        left -= take;
    }
    plan
}

/// Allocate and touch `alloc_mib` MiB in `chunk_mib` chunks and report how much
/// was touched. Progress is printed and flushed after every chunk: when the
/// request goes past the configured memory the kernel kills the process, the
/// host sees a crash, and these lines are the only record of how far it got.
fn allocate_and_touch(alloc_mib: usize, chunk_mib: usize) -> Value {
    let started = Instant::now();
    let plan = chunk_plan(alloc_mib, chunk_mib);
    let mut held: Vec<Vec<u8>> = Vec::with_capacity(plan.len());
    let mut touched_mib = 0usize;
    let mut failure: Option<String> = None;
    log_progress(&format!(
        "alloc start requested={alloc_mib} MiB chunk={chunk_mib} MiB chunks={}",
        plan.len()
    ));
    for (index, mib) in plan.iter().copied().enumerate() {
        let bytes = mib * MIB;
        let mut chunk: Vec<u8> = Vec::new();
        // `try_reserve_exact` reports an allocation failure instead of aborting
        // the process, so a refused allocation still produces a report.
        if let Err(e) = chunk.try_reserve_exact(bytes) {
            failure = Some(format!(
                "allocation of chunk {} ({mib} MiB) failed: {e}",
                index + 1
            ));
            break;
        }
        chunk.resize(bytes, 0);
        // One non-zero byte per page, so the pages are really resident and
        // cannot be backed by the shared zero page.
        let mut offset = 0;
        while offset < bytes {
            chunk[offset] = (index % 251 + 1) as u8;
            offset += PAGE_BYTES;
        }
        held.push(chunk);
        touched_mib += mib;
        log_progress(&format!(
            "alloc touched {touched_mib} MiB of {alloc_mib} MiB"
        ));
    }
    let resident_mib = held.iter().map(Vec::len).sum::<usize>() / MIB;
    let report = json!({
        "requested_mib": alloc_mib,
        "chunk_mib": chunk_mib,
        "chunks": plan.len(),
        "touched_mib": touched_mib,
        "resident_mib": resident_mib,
        "completed": failure.is_none() && touched_mib == alloc_mib,
        "error": failure,
        "elapsed_ms": elapsed_ms(started),
    });
    // Freed only after the report is built, so the numbers describe live memory.
    drop(held);
    log_progress(&format!(
        "alloc done touched={touched_mib} MiB of {alloc_mib} MiB"
    ));
    report
}

// ---------------------------------------------------------------------------
// PLT-4622: ephemeral storage
// ---------------------------------------------------------------------------

/// One line of `/proc/mounts`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MountEntry {
    device: String,
    mount_point: String,
    fs_type: String,
    options: String,
}

fn parse_proc_mounts(text: &str) -> Vec<MountEntry> {
    text.lines()
        .filter_map(|line| {
            let mut f = line.split_whitespace();
            Some(MountEntry {
                device: f.next()?.to_string(),
                mount_point: f.next()?.to_string(),
                fs_type: f.next()?.to_string(),
                options: f.next()?.to_string(),
            })
        })
        .collect()
}

/// The mount that holds `path`: the longest mount point that prefixes it
/// (the last one wins when a point is mounted over).
fn mount_for<'a>(mounts: &'a [MountEntry], path: &str) -> Option<&'a MountEntry> {
    mounts
        .iter()
        .filter(|m| {
            path == m.mount_point
                || m.mount_point == "/"
                || path.starts_with(&format!("{}/", m.mount_point))
        })
        .fold(None, |best: Option<&MountEntry>, m| match best {
            Some(b) if b.mount_point.len() > m.mount_point.len() => Some(b),
            _ => Some(m),
        })
}

fn mount_json(entry: Option<&MountEntry>) -> Value {
    match entry {
        Some(m) => json!({
            "device": m.device,
            "mount_point": m.mount_point,
            "fs_type": m.fs_type,
            "options": m.options,
            "read_only": m.options.split(',').any(|o| o == "ro"),
        }),
        None => Value::Null,
    }
}

/// `statvfs` of `path` in bytes, or the error.
fn fs_stats(path: &str) -> Value {
    let Ok(c) = std::ffi::CString::new(path) else {
        return json!({"error": "path contains NUL"});
    };
    // SAFETY: zeroed statvfs is a valid out-parameter; `c` is NUL-terminated.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        let e = std::io::Error::last_os_error();
        return json!({"error": e.to_string(), "os_error": e.raw_os_error()});
    }
    #[allow(clippy::unnecessary_cast)] // field widths differ per platform
    let (frsize, blocks, bfree, bavail) = (
        st.f_frsize as u64,
        st.f_blocks as u64,
        st.f_bfree as u64,
        st.f_bavail as u64,
    );
    json!({
        "total_bytes": blocks * frsize,
        "free_bytes": bfree * frsize,
        "avail_bytes": bavail * frsize,
        "total_mib": blocks * frsize / MIB as u64,
        "avail_mib": bavail * frsize / MIB as u64,
    })
}

fn io_error_json(e: &std::io::Error) -> Value {
    json!({
        "error_kind": format!("{:?}", e.kind()),
        "os_error": e.raw_os_error(),
        "error": e.to_string(),
    })
}

/// Try to create a file in `dir`; a read-only file system answers EROFS.
fn write_refused(dir: &str) -> Value {
    let path = format!("{}/.isolation-probe-write-test", dir.trim_end_matches('/'));
    match std::fs::File::create(&path) {
        Ok(_) => {
            let _ = std::fs::remove_file(&path);
            json!({"dir": dir, "refused": false})
        }
        Err(e) => {
            let mut v = io_error_json(&e);
            v["dir"] = dir.into();
            v["refused"] = true.into();
            v["read_only_fs"] = (e.raw_os_error() == Some(libc::EROFS)).into();
            v
        }
    }
}

/// Outcome of filling a file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FillOutcome {
    written_bytes: u64,
    /// `enospc`, `limit` or `error`.
    stopped_by: &'static str,
    error: Option<(String, Option<i32>, String)>,
}

/// Write up to `limit_mib` MiB of non-zero bytes into `file`, counting partial
/// writes exactly, and stop at the first error.
fn fill_file(file: &mut std::fs::File, limit_mib: usize) -> FillOutcome {
    let chunk: Vec<u8> = (0..MIB).map(|i| (i % 251 + 1) as u8).collect();
    let limit = (limit_mib as u64) * MIB as u64;
    let mut written = 0u64;
    let mut next_log = 16 * MIB as u64;
    let failed = |written, e: std::io::Error| FillOutcome {
        written_bytes: written,
        stopped_by: if e.raw_os_error() == Some(libc::ENOSPC) {
            "enospc"
        } else {
            "error"
        },
        error: Some((format!("{:?}", e.kind()), e.raw_os_error(), e.to_string())),
    };
    while written < limit {
        let want = ((limit - written) as usize).min(MIB);
        let mut off = 0;
        while off < want {
            match file.write(&chunk[off..want]) {
                Ok(0) => {
                    return failed(written, std::io::Error::from(std::io::ErrorKind::WriteZero));
                }
                Ok(n) => {
                    off += n;
                    written += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return failed(written, e),
            }
        }
        if written >= next_log {
            log_progress(&format!("disk wrote {} MiB", written / MIB as u64));
            next_log += 16 * MIB as u64;
        }
    }
    // Delayed allocation can defer the failure to the flush.
    if let Err(e) = file.sync_all() {
        return failed(written, e);
    }
    FillOutcome {
        written_bytes: written,
        stopped_by: "limit",
        error: None,
    }
}

fn disk_report(request: &ProbeRequest) -> Value {
    let started = Instant::now();
    let dir = request.fill_dir.trim_end_matches('/').to_string();
    let dir = if dir.is_empty() { "/".to_string() } else { dir };
    let mounts = std::fs::read_to_string("/proc/mounts")
        .map(|t| parse_proc_mounts(&t))
        .unwrap_or_default();
    let before = fs_stats(&dir);
    let read_only: Vec<Value> = READ_ONLY_PROBES.iter().map(|d| write_refused(d)).collect();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let path = format!("{dir}/isolation-probe-fill.{}.{nanos}", std::process::id());
    log_progress(&format!(
        "disk fill start path={path} limit={} MiB",
        request.fill_mib
    ));
    let (outcome, open_error) = match std::fs::File::create(&path) {
        Ok(mut file) => (Some(fill_file(&mut file, request.fill_mib)), None),
        Err(e) => (None, Some(io_error_json(&e))),
    };
    let after_fill = fs_stats(&dir);
    let removed = if request.keep_fill {
        false
    } else {
        std::fs::remove_file(&path).is_ok()
    };
    let after_cleanup = fs_stats(&dir);
    let fill = match &outcome {
        Some(o) => {
            log_progress(&format!(
                "disk fill done wrote={} MiB stopped_by={}",
                o.written_bytes / MIB as u64,
                o.stopped_by
            ));
            json!({
                "written_bytes": o.written_bytes,
                "written_mib": o.written_bytes / MIB as u64,
                "stopped_by": o.stopped_by,
                "error_kind": o.error.as_ref().map(|e| e.0.clone()),
                "os_error": o.error.as_ref().and_then(|e| e.1),
                "error": o.error.as_ref().map(|e| e.2.clone()),
            })
        }
        None => {
            json!({"written_bytes": 0, "written_mib": 0, "stopped_by": "error", "open_error": open_error})
        }
    };
    json!({
        "dir": dir,
        "path": path,
        "requested_mib": request.fill_mib,
        "fill": fill,
        "removed": removed,
        "mount": mount_json(mount_for(&mounts, &dir)),
        "fs_before": before,
        "fs_after_fill": after_fill,
        "fs_after_cleanup": after_cleanup,
        "read_only_checks": read_only,
        "mounts": mounts.iter().map(|m| mount_json(Some(m))).collect::<Vec<_>>(),
        "elapsed_ms": elapsed_ms(started),
        "expectation": "the write stops with ENOSPC at the revision's ephemeral_storage_mib, and / and /function refuse writes (PLT-4622)",
    })
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// A progress line on stdout, flushed at once: the host forwards it with
/// `phase=handler` and it survives a kill of this process.
fn log_progress(line: &str) {
    println!("isolation-probe: {line}");
    let _ = std::io::stdout().flush();
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROC_NET_DEV_LOOPBACK_ONLY: &str = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:     120       2    0    0    0     0          0         0      120       2    0    0    0     0       0          0
";
    const PROC_NET_DEV_WITH_NIC: &str = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:       0       0    0    0    0     0          0         0        0       0    0    0    0     0       0          0
  eth0:  918273     514    0    0    0     0          0         0   123456     321    0    0    0     0       0          0
";

    #[test]
    fn payload_selects_the_probe_and_bounds_the_work() {
        let all = ProbeRequest::from_payload(&json!({})).unwrap();
        assert_eq!(all.probe, Probe::All);
        assert!(all.probe.runs_egress() && all.probe.runs_resources());
        assert_eq!(all.targets, DEFAULT_TARGETS.map(str::to_string).to_vec());
        assert_eq!(all.dns_name, DEFAULT_DNS_NAME);
        assert_eq!(all.connect_timeout, Duration::from_millis(2_000));
        assert_eq!(all.alloc_mib, 0);
        assert_eq!(all.chunk_mib, DEFAULT_CHUNK_MIB);

        let egress = ProbeRequest::from_payload(&json!({"probe": "egress"})).unwrap();
        assert!(egress.probe.runs_egress() && !egress.probe.runs_resources());

        let resources = ProbeRequest::from_payload(&json!({
            "probe": "resources", "alloc_mib": 512, "chunk_mib": 32
        }))
        .unwrap();
        assert!(!resources.probe.runs_egress() && resources.probe.runs_resources());
        assert_eq!((resources.alloc_mib, resources.chunk_mib), (512, 32));

        // Timeouts are clamped, silly values cannot hang or overflow the handler.
        let bounded = ProbeRequest::from_payload(&json!({
            "connect_timeout_ms": 0, "dns_timeout_ms": 9_000_000,
            "alloc_mib": 1_000_000, "chunk_mib": 0,
            "targets": ["127.0.0.1:1"], "dns_name": "probe.invalid"
        }))
        .unwrap();
        assert_eq!(bounded.connect_timeout, Duration::from_millis(1));
        assert_eq!(bounded.dns_timeout, Duration::from_millis(MAX_TIMEOUT_MS));
        assert_eq!(bounded.alloc_mib, MAX_ALLOC_MIB);
        assert_eq!(bounded.chunk_mib, 1);
        assert_eq!(bounded.targets, vec!["127.0.0.1:1".to_string()]);
        assert_eq!(bounded.dns_name, "probe.invalid");
    }

    #[test]
    fn disk_payload_defaults_to_filling_tmp_until_full() {
        let disk = ProbeRequest::from_payload(&json!({"probe": "disk"})).unwrap();
        assert_eq!(disk.probe, Probe::Disk);
        assert!(!disk.probe.runs_egress() && !disk.probe.runs_resources());
        assert_eq!(disk.fill_mib, MAX_FILL_MIB);
        assert_eq!(disk.fill_dir, "/tmp");
        assert!(!disk.keep_fill);
        let bounded = ProbeRequest::from_payload(
            &json!({"probe": "disk", "fill_mib": u64::MAX, "dir": "/x", "keep": true}),
        )
        .unwrap();
        assert_eq!(bounded.fill_mib, MAX_FILL_MIB);
        assert_eq!(bounded.fill_dir, "/x");
        assert!(bounded.keep_fill);
        // `all` never fills a disk.
        let all = ProbeRequest::from_payload(&json!({})).unwrap();
        assert_ne!(all.probe, Probe::Disk);
    }

    #[test]
    fn proc_mounts_resolve_the_mount_of_a_path() {
        let mounts = parse_proc_mounts(
            "/dev/root / ext4 ro,relatime 0 0\n\
             devtmpfs /dev devtmpfs rw,relatime 0 0\n\
             /dev/vdb /function ext4 ro,nosuid,relatime 0 0\n\
             /dev/vdc /tmp ext4 rw,nosuid,nodev,relatime 0 0\n",
        );
        assert_eq!(mounts.len(), 4);
        assert_eq!(mount_for(&mounts, "/tmp").unwrap().device, "/dev/vdc");
        assert_eq!(mount_for(&mounts, "/tmp/a/b").unwrap().device, "/dev/vdc");
        assert_eq!(mount_for(&mounts, "/tmpx").unwrap().device, "/dev/root");
        assert_eq!(mount_for(&mounts, "/function").unwrap().fs_type, "ext4");
        assert_eq!(mount_json(mount_for(&mounts, "/"))["read_only"], true);
        assert_eq!(mount_json(mount_for(&mounts, "/tmp"))["read_only"], false);
    }

    #[test]
    fn fill_stops_at_the_limit_and_the_report_cleans_up() {
        let dir = std::env::temp_dir().join(format!("probe-fill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut f = std::fs::File::create(dir.join("f")).unwrap();
        let o = fill_file(&mut f, 2);
        assert_eq!(o.stopped_by, "limit");
        assert_eq!(o.written_bytes, 2 * MIB as u64);
        assert_eq!(
            std::fs::metadata(dir.join("f")).unwrap().len(),
            2 * MIB as u64
        );

        let request = ProbeRequest::from_payload(
            &json!({"probe": "disk", "fill_mib": 1, "dir": dir.to_str().unwrap()}),
        )
        .unwrap();
        let report = disk_report(&request);
        assert_eq!(report["fill"]["stopped_by"], "limit");
        assert_eq!(report["fill"]["written_mib"], 1);
        assert_eq!(report["removed"], true);
        assert!(report["fs_before"]["total_bytes"].as_u64().unwrap() > 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn invalid_payloads_are_handler_errors() {
        let err = ProbeRequest::from_payload(&json!({"probe": "network"})).unwrap_err();
        assert_eq!(err.error_type, "Probe.Unknown");
        let err = ProbeRequest::from_payload(&json!({"probe": 1})).unwrap_err();
        assert_eq!(err.error_type, "Probe.InvalidPayload");
        let err = ProbeRequest::from_payload(&json!({"targets": [42]})).unwrap_err();
        assert_eq!(err.error_type, "Probe.InvalidPayload");
    }

    #[test]
    fn proc_net_dev_shows_whether_the_guest_has_more_than_loopback() {
        let only_lo = parse_proc_net_dev(PROC_NET_DEV_LOOPBACK_ONLY);
        assert_eq!(
            only_lo,
            vec![Interface {
                name: "lo".into(),
                rx_bytes: 120,
                tx_bytes: 120
            }]
        );
        let with_nic = parse_proc_net_dev(PROC_NET_DEV_WITH_NIC);
        assert_eq!(
            with_nic.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
            vec!["lo", "eth0"]
        );
        assert_eq!(with_nic[1].rx_bytes, 918_273);
        assert_eq!(with_nic[1].tx_bytes, 123_456);
        assert!(parse_proc_net_dev("").is_empty());
    }

    #[test]
    fn proc_net_route_lists_default_routes() {
        let routes = parse_proc_net_route(
            "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\n\
             eth0\t00000000\t0102000A\t0003\t0\t0\t0\t00000000\n\
             eth0\t000200A0\t00000000\t0001\t0\t0\t0\t00FFFFFF\n",
        );
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].iface, "eth0");
        assert_eq!(routes[0].gateway, "0102000A");
        assert!(is_default_route(&routes[0]));
        assert!(!is_default_route(&routes[1]));
        // A guest without a NIC has only the header.
        assert!(
            parse_proc_net_route(
                "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\n"
            )
            .is_empty()
        );
    }

    #[test]
    fn meminfo_cpuinfo_and_cgroup_limits_are_parsed() {
        let meminfo = "MemTotal:         249876 kB\nMemFree:          210000 kB\n";
        assert_eq!(parse_meminfo_kib(meminfo, "MemTotal"), Some(249_876));
        assert_eq!(parse_meminfo_kib(meminfo, "Hugepagesize"), None);

        let cpuinfo = "processor\t: 0\nBogoMIPS\t: 50.00\nprocessor\t: 1\nBogoMIPS\t: 50.00\n";
        assert_eq!(count_cpuinfo_processors(cpuinfo), 2);
        assert_eq!(count_cpuinfo_processors(""), 0);

        assert_eq!(parse_cgroup_memory_max("268435456\n"), Some(268_435_456));
        assert_eq!(parse_cgroup_memory_max("max\n"), None);
        assert_eq!(
            parse_cgroup_cpu_max("200000 100000\n"),
            (Some(200_000), Some(100_000))
        );
        assert_eq!(parse_cgroup_cpu_max("max 100000\n"), (None, Some(100_000)));
    }

    #[test]
    fn allocation_touches_every_chunk_and_reports_the_remainder() {
        assert_eq!(chunk_plan(40, 16), vec![16, 16, 8]);
        assert_eq!(chunk_plan(0, 16), Vec::<usize>::new());
        assert_eq!(chunk_plan(3, 0), vec![1, 1, 1]);

        let report = allocate_and_touch(3, 1);
        assert_eq!(report["requested_mib"], 3);
        assert_eq!(report["touched_mib"], 3);
        assert_eq!(report["resident_mib"], 3);
        assert_eq!(report["chunks"], 3);
        assert_eq!(report["completed"], true);
        assert_eq!(report["error"], Value::Null);
    }

    #[test]
    fn a_target_that_answers_is_reported_as_a_network_reach() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let reachable = connect_probe(&addr.to_string(), Duration::from_secs(2));
        assert!(reachable.attempted && reachable.connected, "{reachable:?}");
        assert!(reachable.peer.is_some());

        let unresolved = DnsRecord {
            name: "probe.invalid".into(),
            resolved: false,
            addresses: Vec::new(),
            error_kind: Some("Uncategorized".into()),
            error: Some("failed to lookup address information".into()),
            timed_out: false,
            elapsed_ms: 1,
        };
        assert!(reached_network(&[reachable], &unresolved));

        // Nothing attempted and nothing resolved is the expected M8 result.
        let unparsable = connect_probe("not-an-address", Duration::from_millis(50));
        assert!(!unparsable.attempted && !unparsable.connected);
        assert_eq!(unparsable.error_kind.as_deref(), Some("InvalidTarget"));
        assert!(!reached_network(&[unparsable], &unresolved));
    }

    #[test]
    fn a_resolved_name_counts_as_a_reach() {
        let resolved = DnsRecord {
            name: "example.com".into(),
            resolved: true,
            addresses: vec!["93.184.216.34:80".into()],
            error_kind: None,
            error: None,
            timed_out: false,
            elapsed_ms: 5,
        };
        assert!(reached_network(&[], &resolved));
    }

    #[test]
    fn the_report_carries_the_selected_sections() {
        let guest = json!({"architecture": "aarch64"});
        let request = ProbeRequest::from_payload(&json!({
            "probe": "resources", "alloc_mib": 1, "chunk_mib": 1
        }))
        .unwrap();
        let report = run(&request, guest.clone());
        assert_eq!(report["probe"], "resources");
        assert_eq!(report["guest"], guest);
        assert!(report.get("egress").is_none());
        assert_eq!(report["resources"]["alloc"]["touched_mib"], 1);
        // The interface list is part of every record.
        assert!(report["interfaces"]["available"].is_boolean());
    }
}
