//! PLT-4622 noisy-neighbour probes: load generators for a tenant that tries to
//! take more than its share, and a fixed workload for the tenant whose latency
//! is measured next to it.
//!
//! - `{"probe":"cpu","burn_ms":N,"threads":T}` spins T threads for N ms and
//!   reports the work done and the guest's own view of CPU time (`/proc/stat`
//!   deltas, including `steal`: time the host did not run this vCPU, which is
//!   where a host-side quota shows up inside the guest).
//! - `{"probe":"io","duration_ms":N,"block_kib":K,"fill_first":true}` first
//!   fills `dir` (default `/tmp`) until `ENOSPC`, then keeps rewriting K KiB
//!   blocks inside that file with `fdatasync` after each one until N ms have
//!   passed, and reports the write latencies. Without `fill_first` it rewrites
//!   a `file_mib` file instead.
//! - `{"probe":"work","cpu_iterations":I,"files":F,"file_kib":K}` runs a fixed
//!   CPU loop of I iterations and then writes F files of K KiB with `fsync`
//!   each, timing both parts. Its duration is the latency-sensitive signal.

use std::io::{Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const MAX_BURN_MS: u64 = 600_000;
const MAX_THREADS: u64 = 64;
const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;

fn u64_field(payload: &Value, key: &str, default: u64, max: u64) -> u64 {
    payload
        .get(key)
        .and_then(Value::as_u64)
        .unwrap_or(default)
        .min(max)
}

fn micros(d: Duration) -> u64 {
    d.as_micros().min(u128::from(u64::MAX)) as u64
}

fn ms_f(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Nearest-rank percentile of `values` (sorted in place); `None` when empty.
pub fn percentile(values: &mut [f64], pct: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let rank = ((pct / 100.0) * values.len() as f64).ceil() as usize;
    Some(values[rank.clamp(1, values.len()) - 1])
}

/// The aggregate `cpu` line of `/proc/stat` in clock ticks:
/// user, nice, system, idle, iowait, irq, softirq, steal.
pub fn parse_proc_stat_cpu(text: &str) -> Option<[u64; 8]> {
    let line = text.lines().find(|l| l.starts_with("cpu "))?;
    let mut out = [0u64; 8];
    let mut fields = line.split_whitespace().skip(1);
    for slot in &mut out {
        *slot = fields.next()?.parse().ok()?;
    }
    Some(out)
}

fn proc_stat_cpu() -> Option<[u64; 8]> {
    parse_proc_stat_cpu(&std::fs::read_to_string("/proc/stat").ok()?)
}

/// Deltas of two `/proc/stat` samples and the share of steal time.
pub fn stat_delta(before: [u64; 8], after: [u64; 8]) -> Value {
    let names = [
        "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
    ];
    let mut out = serde_json::Map::new();
    let mut total = 0u64;
    for (i, name) in names.iter().enumerate() {
        let d = after[i].saturating_sub(before[i]);
        total += d;
        out.insert((*name).to_owned(), d.into());
    }
    let steal = after[7].saturating_sub(before[7]);
    out.insert("total".into(), total.into());
    out.insert(
        "steal_pct".into(),
        if total == 0 {
            Value::Null
        } else {
            json!((steal as f64 * 1000.0 / total as f64).round() / 10.0)
        },
    );
    Value::Object(out)
}

/// A small integer mixer: work that cannot be optimised away.
#[inline]
fn mix(state: u64) -> u64 {
    let x = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    x ^ (x >> 29)
}

fn spin(iterations: u64) -> u64 {
    let mut s = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..iterations {
        s = std::hint::black_box(mix(s));
    }
    s
}

pub fn cpu_report(payload: &Value) -> Value {
    let burn = Duration::from_millis(u64_field(payload, "burn_ms", 10_000, MAX_BURN_MS).max(1));
    let threads = u64_field(payload, "threads", 4, MAX_THREADS).max(1);
    let before = proc_stat_cpu();
    let started = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            std::thread::spawn(move || {
                let mut chunks = 0u64;
                while started.elapsed() < burn {
                    spin(100_000);
                    chunks += 1;
                }
                chunks
            })
        })
        .collect();
    let chunks: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap_or(0)).collect();
    let elapsed = started.elapsed();
    let after = proc_stat_cpu();
    let total: u64 = chunks.iter().sum();
    json!({
        "burn_ms": burn.as_millis() as u64,
        "threads": threads,
        "elapsed_ms": elapsed.as_millis() as u64,
        "available_parallelism": std::thread::available_parallelism().map(|n| n.get()).ok(),
        "iterations": total.saturating_mul(100_000),
        "iterations_per_thread": chunks.iter().map(|c| c.saturating_mul(100_000)).collect::<Vec<_>>(),
        "iterations_per_sec": (total as f64 * 100_000.0 / elapsed.as_secs_f64().max(1e-9)).round(),
        "proc_stat_delta": match (before, after) {
            (Some(b), Some(a)) => stat_delta(b, a),
            _ => Value::Null,
        },
        "expectation": "the host keeps this environment at its cpu_millis quota however many threads spin (PLT-4622)",
    })
}

fn latencies_json(mut ms: Vec<f64>) -> Value {
    let count = ms.len();
    let sum: f64 = ms.iter().sum();
    json!({
        "count": count,
        "p50_ms": percentile(&mut ms, 50.0),
        "p95_ms": percentile(&mut ms, 95.0),
        "max_ms": ms.last().copied(),
        "mean_ms": if count == 0 { None } else { Some(sum / count as f64) },
    })
}

fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut s = seed | 1;
    (0..len)
        .map(|_| {
            s = mix(s);
            (s >> 56) as u8
        })
        .collect()
}

pub fn io_report(payload: &Value) -> Value {
    let started = Instant::now();
    let duration = Duration::from_millis(u64_field(payload, "duration_ms", 10_000, MAX_BURN_MS));
    let block = (u64_field(payload, "block_kib", 256, 64 * 1024).max(4) as usize) * KIB;
    let fill_first = payload
        .get("fill_first")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let file_bytes = (u64_field(payload, "file_mib", 16, 4096).max(1) as usize) * MIB;
    let dir = payload.get("dir").and_then(Value::as_str).unwrap_or("/tmp");
    let path = format!("{dir}/isolation-probe-io.{}", std::process::id());
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(f) => f,
        Err(e) => return json!({"path": path, "open_error": e.to_string()}),
    };
    let chunk = pattern(MIB, 7);
    // Phase 1: fill until the file system refuses.
    let mut filled = 0usize;
    let mut stopped_by = "not_run";
    if fill_first {
        stopped_by = "limit";
        loop {
            match file.write(&chunk) {
                Ok(0) => {
                    stopped_by = "write_zero";
                    break;
                }
                Ok(n) => filled += n,
                Err(e) if e.raw_os_error() == Some(libc::ENOSPC) => {
                    stopped_by = "enospc";
                    break;
                }
                Err(_) => {
                    stopped_by = "error";
                    break;
                }
            }
            if filled >= 64 * 1024 * MIB {
                break;
            }
        }
        let _ = file.sync_data();
    }
    let fill_ms = started.elapsed().as_millis() as u64;
    // Phase 2: rewrite blocks inside what exists, syncing each one.
    let span = if fill_first {
        filled - filled % block
    } else {
        file_bytes - file_bytes % block
    };
    let data = pattern(block, 11);
    let mut latencies = Vec::new();
    let mut errors = 0u64;
    let mut first_error = None;
    let rewrite_started = Instant::now();
    let mut offset = 0usize;
    while span >= block && rewrite_started.elapsed() < duration {
        let t = Instant::now();
        let result = file
            .seek(SeekFrom::Start(offset as u64))
            .and_then(|_| file.write_all(&data))
            .and_then(|_| file.sync_data());
        match result {
            Ok(()) => latencies.push(ms_f(t.elapsed())),
            Err(e) => {
                errors += 1;
                first_error.get_or_insert_with(|| e.to_string());
                if errors > 100 {
                    break;
                }
            }
        }
        offset = (offset + block) % span;
    }
    let rewrite = rewrite_started.elapsed();
    let ops = latencies.len() as u64;
    drop(file);
    let removed = std::fs::remove_file(&path).is_ok();
    json!({
        "path": path,
        "fill": {"enabled": fill_first, "written_bytes": filled, "written_mib": filled / MIB,
                 "stopped_by": stopped_by, "ms": fill_ms},
        "rewrite": {
            "block_bytes": block, "span_bytes": span, "ops": ops, "errors": errors,
            "first_error": first_error,
            "bytes": ops.saturating_mul(block as u64),
            "elapsed_ms": rewrite.as_millis() as u64,
            "mib_per_sec": (ops as f64 * block as f64 / MIB as f64 / rewrite.as_secs_f64().max(1e-9) * 10.0).round() / 10.0,
            "latency": latencies_json(latencies),
        },
        "removed": removed,
        "elapsed_ms": started.elapsed().as_millis() as u64,
        "expectation": "the guest cannot write past its scratch drive; its IO shares the host device (PLT-4622)",
    })
}

pub fn work_report(payload: &Value) -> Value {
    let started = Instant::now();
    let iterations = u64_field(payload, "cpu_iterations", 200_000_000, 50_000_000_000);
    let files = u64_field(payload, "files", 32, 10_000);
    let file_bytes = (u64_field(payload, "file_kib", 64, 64 * 1024) as usize) * KIB;
    let dir = payload.get("dir").and_then(Value::as_str).unwrap_or("/tmp");

    let cpu_started = Instant::now();
    let checksum = spin(iterations);
    let cpu = cpu_started.elapsed();

    let data = pattern(file_bytes, 3);
    let mut latencies = Vec::new();
    let mut error = None;
    let writes_started = Instant::now();
    for i in 0..files {
        let path = format!("{dir}/isolation-probe-work.{}.{i}", std::process::id());
        let t = Instant::now();
        let result = std::fs::File::create(&path)
            .and_then(|mut f| f.write_all(&data).and_then(|_| f.sync_all()));
        let _ = std::fs::remove_file(&path);
        match result {
            Ok(()) => latencies.push(ms_f(t.elapsed())),
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    let writes = writes_started.elapsed();
    json!({
        "cpu": {"iterations": iterations, "ms": ms_f(cpu), "us": micros(cpu), "checksum": format!("{checksum:016x}")},
        "writes": {"files": files, "file_bytes": file_bytes, "ms": ms_f(writes), "error": error,
                   "latency": latencies_json(latencies)},
        "total_ms": ms_f(started.elapsed()),
        "ok": error.is_none(),
        "expectation": "a fixed workload whose duration shows interference from other tenants (PLT-4622)",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_nearest_rank() {
        let mut v = vec![5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(percentile(&mut v, 50.0), Some(3.0));
        assert_eq!(percentile(&mut v, 95.0), Some(5.0));
        assert_eq!(percentile(&mut v, 0.0), Some(1.0));
        assert_eq!(percentile(&mut [], 50.0), None);
    }

    #[test]
    fn proc_stat_cpu_line_parses_and_deltas_report_steal() {
        let a = parse_proc_stat_cpu("cpu  10 0 5 100 1 0 0 4 0 0\ncpu0 10 0 5 100 1 0 0 4 0 0\n")
            .unwrap();
        let b = parse_proc_stat_cpu("cpu  60 0 5 100 1 0 0 54 0 0\n").unwrap();
        let d = stat_delta(a, b);
        assert_eq!(d["user"], 50);
        assert_eq!(d["steal"], 50);
        assert_eq!(d["total"], 100);
        assert_eq!(d["steal_pct"], 50.0);
        assert!(parse_proc_stat_cpu("intr 1 2 3").is_none());
    }

    #[test]
    fn workloads_run_small_and_report_shapes() {
        let dir = tempfile_dir();
        let cpu = cpu_report(&json!({"burn_ms": 20, "threads": 2}));
        assert_eq!(cpu["threads"], 2);
        assert!(cpu["iterations"].as_u64().unwrap() > 0);

        let work =
            work_report(&json!({"cpu_iterations": 1000, "files": 2, "file_kib": 4, "dir": dir}));
        assert_eq!(work["ok"], true);
        assert_eq!(work["writes"]["latency"]["count"], 2);

        let io = io_report(
            &json!({"duration_ms": 20, "block_kib": 4, "fill_first": false, "file_mib": 1, "dir": dir}),
        );
        assert_eq!(io["fill"]["enabled"], false);
        assert!(io["rewrite"]["ops"].as_u64().unwrap() > 0);
        assert_eq!(io["removed"], true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn tempfile_dir() -> String {
        let d = std::env::temp_dir().join(format!("isolation-probe-load-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d.display().to_string()
    }
}
