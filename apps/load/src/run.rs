//! Sending load and sampling the gateway.
//!
//! Every file lives in the scenario's output directory:
//!
//! - `t0` — the scenario's start (unix ms); every `t_ms` is relative to it;
//! - `budget.json` — requests sent so far by every `load` run (limits are
//!   scenario-wide);
//! - `samples.jsonl` — one line per sample: `/metrics` (flattened) and the
//!   tenant-A view of `/v1/capacity`;
//! - `requests.jsonl` — one line per invocation sent;
//! - `invocations.jsonl` — start kind, environment and guest boot id of
//!   every invocation (read back after its phase);
//! - `phases.jsonl` — start and end of every phase.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::limits::LoadLimits;
use crate::plan::{Phase, PhaseKind, Rng, check_plan};
use crate::prom;

pub fn now_unix_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

/// The scenario start, created by whichever command runs first.
pub fn ensure_t0(dir: &Path) -> Result<u64> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join("t0");
    if let Ok(text) = std::fs::read_to_string(&path)
        && let Ok(t0) = text.trim().parse()
    {
        return Ok(t0);
    }
    let t0 = now_unix_ms();
    std::fs::write(&path, t0.to_string())?;
    Ok(t0)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Budget {
    pub sent: u64,
    pub refused_by_limits: u64,
}

fn read_budget(dir: &Path) -> Budget {
    std::fs::read_to_string(dir.join("budget.json"))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn write_budget(dir: &Path, b: &Budget) -> Result<()> {
    std::fs::write(dir.join("budget.json"), serde_json::to_vec_pretty(b)?)?;
    Ok(())
}

async fn append(path: &Path, lock: &Mutex<()>, line: &Value) -> Result<()> {
    let _g = lock.lock().await;
    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut bytes = serde_json::to_vec(line)?;
    bytes.push(b'\n');
    f.write_all(&bytes).await?;
    Ok(())
}

pub fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        // Never follow a redirect: it could lead off the allowed host.
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .build()?)
}

/// A tenant's credential and the function the scenario deployed for it.
#[derive(Debug, Clone)]
pub struct TenantTarget {
    pub token: String,
    pub function_id: String,
}

struct Ctx {
    t0: u64,
    deadline_ms: u64,
    client: reqwest::Client,
    lock: Arc<Mutex<()>>,
    seed: u64,
}

pub struct LoadRun {
    pub base: reqwest::Url,
    pub dir: PathBuf,
    pub limits: LoadLimits,
    pub seed: u64,
    pub tenants: std::collections::BTreeMap<String, TenantTarget>,
    pub phases: Vec<Phase>,
}

fn capacity_is_zero(v: &Value) -> bool {
    let envs = v["environments"].as_object();
    let provisioned: u64 = envs
        .map(|m| m.values().filter_map(Value::as_u64).sum())
        .unwrap_or(u64::MAX);
    provisioned == 0
        && v["in_flight"].as_u64() == Some(0)
        && v["queue"]["length"].as_u64() == Some(0)
}

impl LoadRun {
    fn url(&self, path: &str) -> Result<reqwest::Url> {
        Ok(self.base.join(path)?)
    }

    fn tenant(&self, key: &str) -> Result<&TenantTarget> {
        self.tenants
            .get(key)
            .ok_or_else(|| anyhow!("no token / function given for tenant `{key}`"))
    }

    /// Run every phase in order. Refuses the whole plan up front when it
    /// would exceed the declared limits; stops sending when the scenario's
    /// duration is used up.
    pub async fn run(self) -> Result<Budget> {
        let t0 = ensure_t0(&self.dir)?;
        let mut budget = read_budget(&self.dir);
        check_plan(&self.phases, &self.limits, budget.sent).map_err(|e| anyhow!(e))?;
        for p in &self.phases {
            if let PhaseKind::Invoke { tenant, .. } | PhaseKind::IdleUntilZero { tenant, .. } =
                &p.kind
            {
                self.tenant(tenant)?;
            }
        }
        let deadline_ms = t0 + self.limits.max_duration_seconds * 1000;
        if now_unix_ms() >= deadline_ms {
            return Err(anyhow!(
                "the scenario's max_duration_seconds ({}) is already used up",
                self.limits.max_duration_seconds
            ));
        }
        let client = client()?;
        let lock = Arc::new(Mutex::new(()));
        let ctx = Ctx {
            t0,
            deadline_ms,
            client: client.clone(),
            lock: lock.clone(),
            seed: self.seed ^ budget.sent,
        };
        // Consecutive phases with the same `wave` run at the same time.
        let mut i = 0;
        while i < self.phases.len() {
            let wave = self.phases[i].wave;
            let mut j = i + 1;
            while wave.is_some() && j < self.phases.len() && self.phases[j].wave == wave {
                j += 1;
            }
            let results = futures::future::join_all((i..j).map(|k| self.run_phase(k, &ctx))).await;
            for r in results {
                let (sent, refused) = r?;
                budget.sent += sent;
                budget.refused_by_limits += refused;
            }
            write_budget(&self.dir, &budget)?;
            i = j;
        }
        self.read_back_invocations(&client, &lock).await?;
        Ok(budget)
    }

    async fn run_phase(&self, index: usize, ctx: &Ctx) -> Result<(u64, u64)> {
        let (t0, deadline_ms, client, lock) = (ctx.t0, ctx.deadline_ms, &ctx.client, &ctx.lock);
        let phase = &self.phases[index];
        let mut rng = Rng::new(ctx.seed ^ (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let (mut sent_total, mut refused_total) = (0, 0);
        {
            let started = now_unix_ms();
            let mut extra = json!({});
            match &phase.kind {
                PhaseKind::Pause { ms } => {
                    let until = (started + ms).min(deadline_ms);
                    tokio::time::sleep(Duration::from_millis(until.saturating_sub(started))).await;
                }
                PhaseKind::IdleUntilZero { tenant, timeout_ms } => {
                    let target = self.tenant(tenant)?;
                    let until = (started + timeout_ms).min(deadline_ms);
                    let mut reached = None;
                    while now_unix_ms() < until {
                        if let Ok(resp) = client
                            .get(self.url("/v1/capacity")?)
                            .bearer_auth(&target.token)
                            .timeout(Duration::from_secs(2))
                            .send()
                            .await
                            && let Ok(v) = resp.json::<Value>().await
                            && capacity_is_zero(&v)
                        {
                            reached = Some(now_unix_ms() - t0);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    extra = json!({"reached_zero_t_ms": reached});
                }
                PhaseKind::Invoke {
                    tenant,
                    concurrency,
                    requests,
                    handler_ms,
                    jitter_ms,
                } => {
                    let target = self.tenant(tenant)?.clone();
                    // Draw the whole phase's jitter now: reproducible from the seed
                    // whatever order the workers take the requests in.
                    let plan: Arc<Vec<(u64, u64)>> = Arc::new(
                        (0..*requests)
                            .map(|_| (rng.below(jitter_ms + 1), rng.below(jitter_ms + 1)))
                            .collect(),
                    );
                    let next = Arc::new(AtomicU64::new(0));
                    let sent = Arc::new(AtomicU64::new(0));
                    let refused = Arc::new(AtomicU64::new(0));
                    let mut workers = Vec::new();
                    for _ in 0..*concurrency {
                        let (client, lock, plan, next, sent, refused) = (
                            client.clone(),
                            lock.clone(),
                            plan.clone(),
                            next.clone(),
                            sent.clone(),
                            refused.clone(),
                        );
                        let url =
                            self.url(&format!("/v1/functions/{}/invoke", target.function_id))?;
                        let (token, phase_name, tenant, dir) = (
                            target.token.clone(),
                            phase.name.clone(),
                            tenant.clone(),
                            self.dir.clone(),
                        );
                        let handler_ms = *handler_ms;
                        let requests = *requests;
                        workers.push(tokio::spawn(async move {
                            loop {
                                let i = next.fetch_add(1, Ordering::SeqCst);
                                if i >= requests {
                                    return;
                                }
                                let (delay, extra_ms) = plan[i as usize];
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                                let now = now_unix_ms();
                                if now + 500 >= deadline_ms {
                                    refused.fetch_add(1, Ordering::SeqCst);
                                    continue;
                                }
                                sent.fetch_add(1, Ordering::SeqCst);
                                let seconds = (handler_ms + extra_ms) as f64 / 1000.0;
                                let result = client
                                    .post(url.clone())
                                    .bearer_auth(&token)
                                    .json(&json!({"seconds": seconds}))
                                    .timeout(Duration::from_millis(deadline_ms - now))
                                    .send()
                                    .await;
                                let done = now_unix_ms();
                                let line = match result {
                                    Ok(resp) => {
                                        let status = resp.status().as_u16();
                                        let id = resp
                                            .headers()
                                            .get("x-tachyon-invocation-id")
                                            .and_then(|v| v.to_str().ok())
                                            .map(str::to_string);
                                        let body: Value = resp.json().await.unwrap_or(Value::Null);
                                        json!({
                                            "phase": phase_name, "tenant": tenant, "seq": i,
                                            "sent_t_ms": now - t0, "done_t_ms": done - t0,
                                            "latency_ms": done - now, "status": status,
                                            "invocation_id": id, "handler_seconds": seconds,
                                            "error_reason": body["error"]["reason"],
                                            "error_type": body["error"]["error_type"],
                                        })
                                    }
                                    Err(e) => json!({
                                        "phase": phase_name, "tenant": tenant, "seq": i,
                                        "sent_t_ms": now - t0, "done_t_ms": done - t0,
                                        "latency_ms": done - now, "status": 0,
                                        "transport_error": e.to_string(),
                                    }),
                                };
                                let _ = append(&dir.join("requests.jsonl"), &lock, &line).await;
                            }
                        }));
                    }
                    for w in workers {
                        w.await?;
                    }
                    sent_total = sent.load(Ordering::SeqCst);
                    refused_total = refused.load(Ordering::SeqCst);
                    extra = json!({
                        "sent": sent.load(Ordering::SeqCst),
                        "not_sent_duration_limit": refused.load(Ordering::SeqCst),
                    });
                }
            }
            let line = json!({
                "phase": phase.name, "spec": phase,
                "start_t_ms": started - t0, "end_t_ms": now_unix_ms() - t0,
                "result": extra,
            });
            append(&self.dir.join("phases.jsonl"), lock, &line).await?;
        }
        Ok((sent_total, refused_total))
    }

    /// Start kind, environment and boot id of every invocation this scenario
    /// sent that was not read back yet (one GET each, bounded by the requests).
    async fn read_back_invocations(
        &self,
        client: &reqwest::Client,
        lock: &Mutex<()>,
    ) -> Result<()> {
        let done: std::collections::BTreeSet<String> =
            read_jsonl(&self.dir.join("invocations.jsonl"))
                .iter()
                .filter_map(|v| v["invocation_id"].as_str().map(str::to_string))
                .collect();
        for req in read_jsonl(&self.dir.join("requests.jsonl")) {
            let (Some(id), Some(tenant)) = (req["invocation_id"].as_str(), req["tenant"].as_str())
            else {
                continue;
            };
            if done.contains(id) {
                continue;
            }
            let target = self.tenant(tenant)?;
            let Ok(resp) = client
                .get(self.url(&format!("/v1/invocations/{id}"))?)
                .bearer_auth(&target.token)
                .timeout(Duration::from_secs(5))
                .send()
                .await
            else {
                continue;
            };
            let Ok(v) = resp.json::<Value>().await else {
                continue;
            };
            let attempts: Vec<Value> = v["attempts"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|a| {
                    json!({
                        "number": a["number"], "status": a["status"],
                        "start_kind": a["start_kind"], "environment_id": a["environment_id"],
                        "guest_boot_id": a["boot_evidence"]["guest_boot_id"],
                        "host_pid": a["boot_evidence"]["host_pid"],
                        "timings": a["timings"],
                    })
                })
                .collect();
            let line = json!({
                "invocation_id": id, "tenant": tenant, "phase": req["phase"],
                "status": v["status"], "revision_id": v["revision_id"], "attempts": attempts,
            });
            append(&self.dir.join("invocations.jsonl"), lock, &line).await?;
        }
        Ok(())
    }
}

pub fn read_jsonl(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

pub struct Sampler {
    pub base: reqwest::Url,
    pub dir: PathBuf,
    pub metrics_token: String,
    pub tenant_token: String,
    pub interval: Duration,
    pub max_duration: Duration,
}

impl Sampler {
    /// Sample until `<dir>/stop` exists or `max_duration` passed. A gateway
    /// that does not answer (a restart) is recorded as `up: false`.
    pub async fn run(self) -> Result<usize> {
        let t0 = ensure_t0(&self.dir)?;
        let client = client()?;
        let lock = Mutex::new(());
        let started = tokio::time::Instant::now();
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut n = 0;
        let metrics_url = self.base.join("/metrics")?;
        let capacity_url = self.base.join("/v1/capacity")?;
        while started.elapsed() < self.max_duration && !self.dir.join("stop").exists() {
            ticker.tick().await;
            let t_ms = now_unix_ms().saturating_sub(t0);
            let metrics = async {
                let resp = client
                    .get(metrics_url.clone())
                    .bearer_auth(&self.metrics_token)
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await
                    .ok()?;
                if !resp.status().is_success() {
                    return None;
                }
                resp.text().await.ok()
            }
            .await;
            let capacity = async {
                let resp = client
                    .get(capacity_url.clone())
                    .bearer_auth(&self.tenant_token)
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await
                    .ok()?;
                resp.json::<Value>().await.ok()
            }
            .await;
            let line = json!({
                "t_ms": t_ms,
                "up": metrics.is_some(),
                "metrics": metrics.as_deref().map(prom::flatten).unwrap_or_default(),
                "capacity": capacity.map(|c| json!({
                    "environments": c["environments"], "in_flight": c["in_flight"],
                    "queue": c["queue"]["length"], "reuse": c["reuse"],
                    "scaling_warm_pool": c["scaling"]["warm_pool"],
                })),
            });
            append(&self.dir.join("samples.jsonl"), &lock, &line).await?;
            n += 1;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_means_no_environment_nothing_in_flight_and_an_empty_queue() {
        let zero = json!({"environments": {"starting": 0, "busy": 0, "idle": 0}, "in_flight": 0, "queue": {"length": 0}});
        assert!(capacity_is_zero(&zero));
        let idle = json!({"environments": {"starting": 0, "busy": 0, "idle": 1}, "in_flight": 0, "queue": {"length": 0}});
        assert!(!capacity_is_zero(&idle));
        assert!(!capacity_is_zero(&json!({"error": "unauthorized"})));
    }
}
