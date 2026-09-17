//! `tsls-load` — see `scripts/load/scenarios.sh` and docs/metrics.md.
//!
//! ```text
//! tsls-load sample --base-url http://127.0.0.1:PORT --out DIR --metrics-token T --token-a A &
//! tsls-load load   --base-url ... --out DIR --token-a A --function-a FN --seed 42 \
//!                  --max-concurrency 8 --max-requests 100 --max-duration-seconds 300 \
//!                  --phase name=burst,tenant=a,concurrency=8,requests=24,handler_ms=400
//! touch DIR/stop
//! tsls-load report --out DIR --max-concurrency 8 ... --cap 3 --expect zero_before,rose,cap
//! ```
//!
//! Exit codes: 0 ok, 1 report checks failed, 2 refused (limits or target).

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

use tachyon_serverless_load::detect::Thresholds;
use tachyon_serverless_load::limits::{LoadLimits, check_target};
use tachyon_serverless_load::plan::parse_phase;
use tachyon_serverless_load::report::{ReportInput, build};
use tachyon_serverless_load::run::{LoadRun, Sampler, TenantTarget};

#[derive(Parser)]
#[command(name = "tsls-load", about = "Bounded local load scenarios (PLT-4637)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct Target {
    /// Gateway base URL: loopback only unless the host is given to --lab-host.
    #[arg(long)]
    base_url: String,
    /// An explicitly allowed lab host (repeatable). Never a production host.
    #[arg(long = "lab-host")]
    lab_hosts: Vec<String>,
    #[arg(long)]
    out: PathBuf,
}

#[derive(Args, Clone, Copy)]
struct LimitArgs {
    #[arg(long)]
    max_concurrency: u32,
    #[arg(long)]
    max_requests: u64,
    #[arg(long)]
    max_duration_seconds: u64,
}

impl From<LimitArgs> for LoadLimits {
    fn from(a: LimitArgs) -> Self {
        LoadLimits {
            max_concurrency: a.max_concurrency,
            max_requests: a.max_requests,
            max_duration_seconds: a.max_duration_seconds,
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Sample /metrics and /v1/capacity into <out>/samples.jsonl until <out>/stop exists.
    Sample {
        #[command(flatten)]
        target: Target,
        #[arg(long, env = "TSLS_METRICS_TOKEN")]
        metrics_token: String,
        #[arg(long, env = "TSLS_TOKEN_A")]
        token_a: String,
        #[arg(long, default_value_t = 250)]
        interval_ms: u64,
        #[arg(long)]
        max_duration_seconds: u64,
    },
    /// Send the phases' invocations within the declared limits.
    Load {
        #[command(flatten)]
        target: Target,
        #[command(flatten)]
        limits: LimitArgs,
        #[arg(long)]
        seed: u64,
        #[arg(long, env = "TSLS_TOKEN_A")]
        token_a: String,
        #[arg(long)]
        function_a: String,
        #[arg(long, env = "TSLS_TOKEN_B")]
        token_b: Option<String>,
        #[arg(long)]
        function_b: Option<String>,
        /// Repeatable, run in order (see plan::parse_phase).
        #[arg(long = "phase", required = true)]
        phases: Vec<String>,
    },
    /// Check that a target would be accepted (exit 2 if not).
    CheckTarget {
        #[arg(long)]
        base_url: String,
        #[arg(long = "lab-host")]
        lab_hosts: Vec<String>,
    },
    /// Write summary.json, timeline.svg and timeline.txt from <out>.
    Report {
        #[arg(long)]
        out: PathBuf,
        #[command(flatten)]
        limits: LimitArgs,
        #[arg(long, default_value = "tsls load scenario")]
        title: String,
        /// Expected node in-flight cap (for the `cap` check).
        #[arg(long)]
        cap: Option<f64>,
        /// Comma-separated checks (zero_before, rose, cap, queue, decreased, zero_after,
        /// restart, active_after_restart, zero_after_restart, all_succeeded, no_findings,
        /// two_tenants_served, every_invocation_boots, warm_reuse, p95_below_ms:<tenant>=<ms>).
        #[arg(long, value_delimiter = ',')]
        expect: Vec<String>,
        #[arg(long, default_value_t = 5.0)]
        starvation_seconds: f64,
        #[arg(long, default_value_t = 0.05)]
        idle_cpu_ratio: f64,
    },
}

fn refuse(msg: impl std::fmt::Display) -> ! {
    eprintln!("tsls-load: refused: {msg}");
    std::process::exit(2);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::CheckTarget {
            base_url,
            lab_hosts,
        } => {
            let url = check_target(&base_url, &lab_hosts).unwrap_or_else(|e| refuse(e));
            println!("allowed: {url}");
        }
        Cmd::Sample {
            target,
            metrics_token,
            token_a,
            interval_ms,
            max_duration_seconds,
        } => {
            let base =
                check_target(&target.base_url, &target.lab_hosts).unwrap_or_else(|e| refuse(e));
            let n = Sampler {
                base,
                dir: target.out,
                metrics_token,
                tenant_token: token_a,
                interval: Duration::from_millis(interval_ms.max(50)),
                max_duration: Duration::from_secs(
                    max_duration_seconds
                        .min(tachyon_serverless_load::limits::CEILING_DURATION_SECONDS),
                ),
            }
            .run()
            .await?;
            eprintln!("tsls-load: {n} samples");
        }
        Cmd::Load {
            target,
            limits,
            seed,
            token_a,
            function_a,
            token_b,
            function_b,
            phases,
        } => {
            let base =
                check_target(&target.base_url, &target.lab_hosts).unwrap_or_else(|e| refuse(e));
            let phases: Vec<_> = phases
                .iter()
                .map(|p| parse_phase(p))
                .collect::<Result<_, _>>()
                .unwrap_or_else(|e| refuse(e));
            let mut tenants = std::collections::BTreeMap::new();
            tenants.insert(
                "a".to_string(),
                TenantTarget {
                    token: token_a,
                    function_id: function_a,
                },
            );
            if let (Some(token), Some(function_id)) = (token_b, function_b) {
                tenants.insert("b".to_string(), TenantTarget { token, function_id });
            }
            let limits: LoadLimits = limits.into();
            if let Err(e) = tachyon_serverless_load::plan::check_plan(&phases, &limits, 0) {
                refuse(e);
            }
            let run = LoadRun {
                base,
                dir: target.out,
                limits,
                seed,
                tenants,
                phases,
            };
            match run.run().await {
                Ok(b) => eprintln!(
                    "tsls-load: sent {} requests in this scenario so far",
                    b.sent
                ),
                Err(e)
                    if e.to_string().contains("declared") || e.to_string().contains("used up") =>
                {
                    refuse(e)
                }
                Err(e) => return Err(e),
            }
        }
        Cmd::Report {
            out,
            limits,
            title,
            cap,
            expect,
            starvation_seconds,
            idle_cpu_ratio,
        } => {
            let outputs = build(&ReportInput {
                dir: &out,
                limits: limits.into(),
                thresholds: Thresholds {
                    starvation_seconds,
                    idle_cpu_ratio,
                    ..Thresholds::default()
                },
                cap,
                expect,
                title,
            })?;
            std::fs::write(
                out.join("summary.json"),
                serde_json::to_vec_pretty(&outputs.summary)?,
            )?;
            std::fs::write(out.join("timeline.svg"), &outputs.svg)?;
            std::fs::write(out.join("timeline.txt"), &outputs.ascii)?;
            print!("{}", outputs.ascii);
            println!(
                "checks: {}  detector findings: {}  ok: {}",
                outputs.summary["checks"],
                outputs.summary["detectors"]["findings"],
                outputs.summary["ok"]
            );
            if outputs.summary["ok"] != true {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
