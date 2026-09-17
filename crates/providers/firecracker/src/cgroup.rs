//! Host-side cgroup v2 limits of one VMM (PLT-4622).
//!
//! The guest sees whole vCPUs and its configured memory, but nothing on the
//! host stopped the VMM process from using a full core per vCPU (a revision
//! with `cpu_millis = 500` still got 1 000 m) or from growing past the guest
//! size. Every environment therefore gets its own cgroup:
//!
//! ```text
//! <root>/<parent>/<env_id>/
//!   cpu.max      "<cpu_millis * period / 1000> <period>"  (all vCPUs + VMM threads)
//!   memory.max   guest memory + memory_overhead_mib
//!   memory.swap.max 0 (when the controller offers it)
//!   pids.max     pids_max
//! ```
//!
//! The VMM is placed into it **before it executes**: the spawned child writes
//! `0` into the cgroup's `cgroup.procs` between `fork` and `exec`
//! ([`EnvCgroup::procs_fd`]), so every thread Firecracker (or the jailer, and
//! the VMM it clones) ever creates starts inside the limits. Placement is then
//! proven by reading `cgroup.procs` and `/proc/<pid>/cgroup`
//! ([`verify_member`]). On terminate the cgroup is killed (`cgroup.kill`),
//! its statistics are read, and it is removed; startup reconcile removes
//! cgroups of environments that no longer exist ([`HostCgroups::sweep`]).

use std::fs::File;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::config::{CgroupConfig, CgroupMode};

/// Controllers every environment cgroup needs.
pub const CONTROLLERS: [&str; 3] = ["cpu", "memory", "pids"];
/// How long [`HostCgroups::remove`] waits for a killed cgroup to empty.
pub const DRAIN_WAIT: Duration = Duration::from_secs(5);

const MIB: u64 = 1024 * 1024;

/// Limits written into one environment's cgroup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CgroupLimits {
    pub cpu_quota_us: u64,
    pub cpu_period_us: u64,
    pub memory_max_bytes: u64,
    pub pids_max: u64,
}

impl CgroupLimits {
    /// `cpu.max` for `cpu_millis` (e.g. 500 m -> `50000 100000`), shared by
    /// all vCPUs and the VMM's own threads; `memory.max` is the guest memory
    /// plus the configured overhead.
    pub fn for_resources(cpu_millis: u32, memory_mib: u32, cfg: &CgroupConfig) -> Self {
        let period = cfg.cpu_period_us.clamp(1_000, 1_000_000);
        // The kernel refuses quotas below 1 ms.
        let quota = (u64::from(cpu_millis) * period / 1000).max(1_000);
        Self {
            cpu_quota_us: quota,
            cpu_period_us: period,
            memory_max_bytes: (u64::from(memory_mib) + cfg.memory_overhead_mib) * MIB,
            pids_max: cfg.pids_max.max(8),
        }
    }

    pub fn cpu_max(&self) -> String {
        format!("{} {}", self.cpu_quota_us, self.cpu_period_us)
    }

    /// Cores the quota amounts to (0.5 for 500 m).
    pub fn cpu_cores(&self) -> f64 {
        self.cpu_quota_us as f64 / self.cpu_period_us as f64
    }
}

/// Whether this process can create environment cgroups under `cfg.root` and
/// move processes into them. `Err` carries the reason.
pub fn host_support(cfg: &CgroupConfig) -> Result<String, String> {
    if cfg.mode == CgroupMode::Off {
        return Err("host cgroup limits are disabled (mode = \"off\")".into());
    }
    if !cfg!(target_os = "linux") {
        return Err(format!(
            "cgroup v2 limits need Linux (host os is {})",
            std::env::consts::OS
        ));
    }
    validate_name(&cfg.parent)?;
    let controllers_file = cfg.root.join("cgroup.controllers");
    let available = std::fs::read_to_string(&controllers_file).map_err(|e| {
        format!(
            "{} is not a cgroup v2 hierarchy ({}: {e})",
            cfg.root.display(),
            controllers_file.display()
        )
    })?;
    let available: Vec<&str> = available.split_whitespace().collect();
    let missing: Vec<&str> = CONTROLLERS
        .iter()
        .copied()
        .filter(|c| !available.contains(c))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "controllers {missing:?} are not available in {} (have {available:?})",
            cfg.root.display()
        ));
    }
    // Moving a process from the gateway's cgroup into ours needs write access
    // to `cgroup.procs` of the common ancestor, which is `root` at best.
    for file in ["cgroup.procs", "cgroup.subtree_control"] {
        let p = cfg.root.join(file);
        if !crate::preflight::access(&p, libc::W_OK) {
            return Err(format!(
                "{} is not writable by uid {}: run the gateway as root or point \
                 [provider.firecracker.cgroup] root at a subtree delegated to it",
                p.display(),
                // SAFETY: geteuid has no preconditions.
                unsafe { libc::geteuid() }
            ));
        }
    }
    Ok(format!(
        "cgroup v2 at {} with {} (environments under {}/)",
        cfg.root.display(),
        CONTROLLERS.join(", "),
        cfg.root.join(&cfg.parent).display()
    ))
}

fn validate_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "cgroup name `{name}` must be a single path component of [A-Za-z0-9._-]"
        ))
    }
}

/// One environment's cgroup, created and verified, ready to receive the VMM.
#[derive(Debug)]
pub struct EnvCgroup {
    pub path: PathBuf,
    pub limits: CgroupLimits,
    /// `cgroup.procs` opened for writing (close-on-exec). The spawned child
    /// writes `0` into it before `exec`.
    procs: File,
}

impl EnvCgroup {
    pub fn procs_fd(&self) -> std::os::fd::RawFd {
        self.procs.as_raw_fd()
    }
}

/// Manager of `<root>/<parent>/`.
#[derive(Debug)]
pub struct HostCgroups {
    cfg: CgroupConfig,
    /// Serialises creating / removing the shared parent directory.
    lock: tokio::sync::Mutex<()>,
}

impl HostCgroups {
    pub fn new(cfg: CgroupConfig) -> Self {
        Self {
            cfg,
            lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn config(&self) -> &CgroupConfig {
        &self.cfg
    }

    pub fn parent_dir(&self) -> PathBuf {
        self.cfg.root.join(&self.cfg.parent)
    }

    pub fn env_dir(&self, env_id: &str) -> PathBuf {
        self.parent_dir().join(env_id)
    }

    /// Create `<parent>/<env_id>` with `limits`, read every value back and
    /// open its `cgroup.procs`. On `Err` nothing is left behind.
    pub async fn create(&self, env_id: &str, limits: CgroupLimits) -> Result<EnvCgroup, String> {
        validate_name(env_id)?;
        let _guard = self.lock.lock().await;
        let parent = self.parent_dir();
        enable_controllers(&self.cfg.root)?;
        match std::fs::create_dir(&parent) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(format!("mkdir {}: {e}", parent.display())),
        }
        enable_controllers(&parent)?;
        let path = parent.join(env_id);
        if path.exists() {
            // A leftover of a crashed run with the same id cannot exist (ids
            // are unique), but never reuse one blindly.
            kill_and_remove(&path, DRAIN_WAIT)
                .await
                .map_err(|e| format!("stale cgroup {}: {e}", path.display()))?;
        }
        std::fs::create_dir(&path).map_err(|e| format!("mkdir {}: {e}", path.display()))?;
        let result = (|| {
            write_value(&path, "cpu.max", &limits.cpu_max())?;
            write_value(&path, "memory.max", &limits.memory_max_bytes.to_string())?;
            if path.join("memory.swap.max").exists() {
                write_value(&path, "memory.swap.max", "0")?;
            }
            write_value(&path, "pids.max", &limits.pids_max.to_string())?;
            verify_limits(&path, &limits)?;
            std::fs::OpenOptions::new()
                .write(true)
                .open(path.join("cgroup.procs"))
                .map_err(|e| format!("open {}/cgroup.procs: {e}", path.display()))
        })();
        match result {
            Ok(procs) => Ok(EnvCgroup {
                path,
                limits,
                procs,
            }),
            Err(e) => {
                let _ = std::fs::remove_dir(&path);
                let _ = std::fs::remove_dir(&parent);
                Err(e)
            }
        }
    }

    /// Kill whatever is left in the environment's cgroup, read its statistics
    /// and remove it (and the parent once it is empty). `Ok(None)` when there
    /// was no cgroup.
    pub async fn remove(&self, env_id: &str) -> Result<Option<Value>, String> {
        if validate_name(env_id).is_err() {
            return Ok(None);
        }
        let path = self.env_dir(env_id);
        if !path.exists() {
            return Ok(None);
        }
        let stats = kill_and_remove(&path, DRAIN_WAIT).await?;
        let _guard = self.lock.lock().await;
        // Fails with ENOTEMPTY / EBUSY while other environments run: fine.
        let _ = std::fs::remove_dir(self.parent_dir());
        Ok(Some(stats))
    }

    /// Remove cgroups of environments that are not in `live`.
    pub async fn sweep(&self, live: &[String]) -> Vec<String> {
        let mut removed = Vec::new();
        let Ok(rd) = std::fs::read_dir(self.parent_dir()) else {
            return removed;
        };
        let orphans: Vec<String> = rd
            .flatten()
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| !live.contains(name))
            .collect();
        for name in orphans {
            match self.remove(&name).await {
                Ok(Some(_)) => removed.push(format!("cgroup:{}", self.env_dir(&name).display())),
                Ok(None) => {}
                Err(e) => tracing::error!(cgroup = name, error = %e, "orphan cgroup not removed"),
            }
        }
        if removed.is_empty() {
            let _guard = self.lock.lock().await;
            let _ = std::fs::remove_dir(self.parent_dir());
        }
        removed
    }
}

/// Enable [`CONTROLLERS`] for the children of `dir` (only the missing ones).
fn enable_controllers(dir: &Path) -> Result<(), String> {
    let file = dir.join("cgroup.subtree_control");
    let current =
        std::fs::read_to_string(&file).map_err(|e| format!("read {}: {e}", file.display()))?;
    let enabled: Vec<&str> = current.split_whitespace().collect();
    let missing: Vec<String> = CONTROLLERS
        .iter()
        .filter(|c| !enabled.contains(c))
        .map(|c| format!("+{c}"))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    std::fs::OpenOptions::new()
        .write(true)
        .open(&file)
        .and_then(|mut f| f.write_all(missing.join(" ").as_bytes()))
        .map_err(|e| format!("enable {} in {}: {e}", missing.join(" "), file.display()))
}

fn write_value(dir: &Path, file: &str, value: &str) -> Result<(), String> {
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join(file))
        .and_then(|mut f| f.write_all(value.as_bytes()))
        .map_err(|e| format!("write `{value}` to {}/{file}: {e}", dir.display()))
}

fn read_value(dir: &Path, file: &str) -> Option<String> {
    std::fs::read_to_string(dir.join(file))
        .ok()
        .map(|s| s.trim().to_owned())
}

/// Read the limits back: a value the kernel rounded or refused is an error.
fn verify_limits(dir: &Path, limits: &CgroupLimits) -> Result<(), String> {
    let expect = [
        ("cpu.max", limits.cpu_max()),
        ("memory.max", limits.memory_max_bytes.to_string()),
        ("pids.max", limits.pids_max.to_string()),
    ];
    for (file, want) in expect {
        let got = read_value(dir, file).unwrap_or_default();
        if !limit_matches(file, &got, &want) {
            return Err(format!(
                "{}/{file} reads `{got}` after writing `{want}`",
                dir.display()
            ));
        }
    }
    Ok(())
}

/// `memory.max` is rounded down to a page; everything else must be exact.
fn limit_matches(file: &str, got: &str, want: &str) -> bool {
    if file == "memory.max" {
        match (got.parse::<u64>(), want.parse::<u64>()) {
            (Ok(g), Ok(w)) => g <= w && w - g < 64 * 1024,
            _ => false,
        }
    } else {
        got == want
    }
}

/// Prove that `pid` runs in the cgroup at `path` (listed in `cgroup.procs`
/// and named by `/proc/<pid>/cgroup`).
pub fn verify_member(root: &Path, path: &Path, pid: u32) -> Result<(), String> {
    let procs = std::fs::read_to_string(path.join("cgroup.procs"))
        .map_err(|e| format!("read {}/cgroup.procs: {e}", path.display()))?;
    if !procs.lines().any(|l| l.trim() == pid.to_string()) {
        return Err(format!(
            "pid {pid} is not in {}/cgroup.procs (members: {})",
            path.display(),
            procs.split_whitespace().collect::<Vec<_>>().join(",")
        ));
    }
    let rel = path
        .strip_prefix(root)
        .map_err(|_| format!("{} is not under {}", path.display(), root.display()))?;
    let own = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .map_err(|e| format!("read /proc/{pid}/cgroup: {e}"))?;
    let want = format!("/{}", rel.display());
    if !own
        .lines()
        .any(|l| l.strip_prefix("0::") == Some(want.as_str()))
    {
        return Err(format!(
            "/proc/{pid}/cgroup is `{}`, expected 0::{want}",
            own.trim()
        ));
    }
    Ok(())
}

/// Parse a flat-keyed cgroup file (`cpu.stat`, `memory.events`).
pub fn parse_flat_keyed(text: &str) -> Map<String, Value> {
    text.lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let key = it.next()?;
            let value: u64 = it.next()?.parse().ok()?;
            Some((key.to_owned(), Value::from(value)))
        })
        .collect()
}

/// Statistics of a cgroup, for evidence and the teardown log.
pub fn stats(path: &Path) -> Value {
    let mut out = Map::new();
    for file in ["cpu.stat", "memory.events"] {
        if let Some(text) = read_value(path, file) {
            out.insert(file.to_owned(), Value::Object(parse_flat_keyed(&text)));
        }
    }
    for file in [
        "cpu.max",
        "memory.max",
        "memory.peak",
        "memory.current",
        "pids.max",
        "pids.peak",
    ] {
        if let Some(v) = read_value(path, file) {
            out.insert(
                file.to_owned(),
                v.parse::<u64>().map(Value::from).unwrap_or(Value::from(v)),
            );
        }
    }
    Value::Object(out)
}

fn populated(path: &Path) -> bool {
    read_value(path, "cgroup.events")
        .map(|t| parse_flat_keyed(&t).get("populated") != Some(&Value::from(0u64)))
        .unwrap_or(false)
}

/// `cgroup.kill`, wait until it is empty, read the statistics, `rmdir`.
async fn kill_and_remove(path: &Path, wait: Duration) -> Result<Value, String> {
    if populated(path) {
        write_value(path, "cgroup.kill", "1")?;
        let started = Instant::now();
        while populated(path) {
            if started.elapsed() > wait {
                return Err(format!(
                    "{} still has processes {:?} after cgroup.kill and {wait:?}",
                    path.display(),
                    read_value(path, "cgroup.procs")
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let stats = stats(path);
    // rmdir can briefly answer EBUSY right after the last task left.
    let started = Instant::now();
    loop {
        match std::fs::remove_dir(path) {
            Ok(()) => return Ok(stats),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(stats),
            Err(e) if started.elapsed() < Duration::from_secs(1) => {
                tracing::debug!(path = %path.display(), error = %e, "cgroup rmdir retry");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => return Err(format!("rmdir {}: {e}", path.display())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_follow_cpu_millis_and_memory() {
        let cfg = CgroupConfig::default();
        let half = CgroupLimits::for_resources(500, 256, &cfg);
        assert_eq!(half.cpu_max(), "50000 100000");
        assert!((half.cpu_cores() - 0.5).abs() < f64::EPSILON);
        assert_eq!(half.memory_max_bytes, (256 + 64) * MIB);
        assert_eq!(half.pids_max, 64);
        // 1 500 m is two vCPUs sharing 1.5 cores.
        assert_eq!(
            CgroupLimits::for_resources(1500, 128, &cfg).cpu_max(),
            "150000 100000"
        );
        // Tiny values are raised to what the kernel accepts.
        let tiny = CgroupLimits::for_resources(
            1,
            64,
            &CgroupConfig {
                cpu_period_us: 10,
                pids_max: 1,
                ..CgroupConfig::default()
            },
        );
        assert_eq!(tiny.cpu_max(), "1000 1000");
        assert_eq!(tiny.pids_max, 8);
    }

    #[test]
    fn names_are_single_components() {
        for good in ["tachyon", "env_01hzzzzzzzzzzzzzzzzzzzzzzz", "a.b-c"] {
            validate_name(good).unwrap();
        }
        for bad in ["", ".", "..", "a/b", "../x", "a b"] {
            assert!(validate_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn memory_max_may_round_down_to_a_page_only() {
        assert!(limit_matches("memory.max", "335544320", "335544320"));
        assert!(limit_matches("memory.max", "335540224", "335544320"));
        assert!(!limit_matches("memory.max", "max", "335544320"));
        assert!(!limit_matches("memory.max", "335548416", "335544320"));
        assert!(limit_matches("cpu.max", "50000 100000", "50000 100000"));
        assert!(!limit_matches("cpu.max", "max 100000", "50000 100000"));
    }

    #[test]
    fn flat_keyed_files_parse() {
        let m = parse_flat_keyed("usage_usec 3025214\nnr_throttled 57\nbogus\nx y\n");
        assert_eq!(m.get("usage_usec"), Some(&Value::from(3_025_214u64)));
        assert_eq!(m.get("nr_throttled"), Some(&Value::from(57u64)));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn host_support_explains_itself() {
        let off = CgroupConfig {
            mode: CgroupMode::Off,
            ..CgroupConfig::default()
        };
        assert!(host_support(&off).unwrap_err().contains("off"));
        let dir = tempfile::tempdir().unwrap();
        let not_cgroup = CgroupConfig {
            root: dir.path().to_path_buf(),
            ..CgroupConfig::default()
        };
        let err = host_support(&not_cgroup).unwrap_err();
        assert!(!err.is_empty());
    }

    #[tokio::test]
    async fn remove_and_sweep_without_a_hierarchy_are_noops() {
        let dir = tempfile::tempdir().unwrap();
        let cg = HostCgroups::new(CgroupConfig {
            root: dir.path().to_path_buf(),
            ..CgroupConfig::default()
        });
        assert_eq!(cg.remove("env_x").await.unwrap(), None);
        assert_eq!(cg.remove("../escape").await.unwrap(), None);
        assert!(cg.sweep(&[]).await.is_empty());
    }
}
