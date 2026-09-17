//! Command line definition (`clap` derive). See `docs/cli.md` for the reference.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::error::CliError;

pub const DEFAULT_API_URL: &str = "http://127.0.0.1:8080";

/// `tsls` — Tachyon Serverless prototype CLI.
#[derive(Debug, Parser)]
#[command(
    name = "tsls",
    version,
    about = "Tachyon Serverless prototype CLI (deploy / invoke / logs / rollback / dev)",
    propagate_version = true
)]
pub struct Cli {
    /// Gateway base URL.
    #[arg(long, global = true, env = "TSLS_API_URL", default_value = DEFAULT_API_URL)]
    pub api_url: String,

    /// Bearer token. Never printed.
    #[arg(long, global = true, env = "TSLS_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Tenant id sent as `x-tachyon-tenant-id` (must match the token's tenant).
    #[arg(long, global = true, env = "TSLS_TENANT_ID")]
    pub tenant_id: Option<String>,

    /// Print the server JSON unmodified instead of tables.
    #[arg(long, global = true)]
    pub json: bool,

    /// HTTP client timeout in seconds (applies per request).
    #[arg(long, global = true, default_value_t = 120)]
    pub timeout_secs: u64,

    /// Use the `:invoke` / `:cancel` route forms instead of `/invoke` / `/cancel`.
    #[arg(long, global = true)]
    pub colon_routes: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Manage and run functions.
    Functions {
        #[command(subcommand)]
        command: FunctionsCommand,
    },
    /// Show the execution provider kind, isolation and capability table.
    Provider,
    /// Cron and signed webhook triggers of a function (PLT-4641).
    Triggers {
        #[command(subcommand)]
        command: TriggersCommand,
    },
    /// Show node capacity versus reservations, the wait queue and this tenant's
    /// revisions (autoscaler view).
    Capacity,
    /// Check /healthz and /readyz.
    Health,
    /// Provisional usage report of the tenant (not an invoice; billing is
    /// disabled).
    Usage(UsageArgs),
    /// Run a binary end to end on a throwaway local gateway (process provider, no isolation).
    Dev(DevArgs),
    /// Inspect and redrive dead-lettered asynchronous invocations.
    DeadLetters {
        #[command(subcommand)]
        command: DeadLettersCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum DeadLettersCommand {
    /// List the dead letters of a function (newest first).
    List {
        function: String,
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Show one dead letter with its last error and redrives.
    Show { dead_letter_id: String },
    /// Re-submit a dead letter as a new asynchronous invocation (needs the
    /// `invoke` and `redrive` roles).
    Redrive {
        dead_letter_id: String,
        /// Run another revision of the same function instead of the original.
        #[arg(long)]
        revision_id: Option<String>,
        /// Why (kept in the audit record).
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum FunctionsCommand {
    /// Create a function.
    Create {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "")]
        description: String,
    },
    /// List functions of the tenant.
    List,
    /// Show one function (by name or `fn_...` id).
    Get { function: String },
    /// Delete a function (stops new invocations).
    Delete { function: String },
    /// Upload a binary, create a revision, wait until ready and show the `prod` alias.
    Deploy(DeployArgs),
    /// Invoke a function synchronously with a JSON payload.
    Invoke(InvokeArgs),
    /// Send an HTTP request through the HTTP adapter (`/v1/functions/{id}/http<path>`).
    Http(HttpArgs),
    /// List recent invocations of a function.
    Invocations {
        function: String,
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
    /// Show one invocation in full (attempts, timings, boot evidence).
    Invocation { invocation_id: String },
    /// Print logs of an invocation, or of the recent invocations of a function.
    Logs(LogsArgs),
    /// List revisions of a function.
    Revisions { function: String },
    /// Show one revision.
    Revision {
        function: String,
        revision_id: String,
    },
    /// List aliases of a function.
    Aliases { function: String },
    /// Point an alias at a revision (compare-and-swap on `--expected-generation`).
    AliasSet {
        function: String,
        #[arg(long, default_value = "prod")]
        alias: String,
        #[arg(long)]
        revision_id: String,
        #[arg(long)]
        expected_generation: Option<u64>,
    },
    /// Roll an alias back to its previous revision (or to `--to`).
    Rollback {
        function: String,
        #[arg(long, default_value = "prod")]
        alias: String,
        #[arg(long)]
        to: Option<String>,
    },
    /// Cancel a running invocation.
    Cancel { invocation_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ArchArg {
    /// Use the architecture of the machine running the CLI.
    Auto,
    #[value(name = "x86_64")]
    X86_64,
    Aarch64,
}

impl ArchArg {
    /// Resolve to the wire value (`x86_64` / `aarch64`).
    pub fn resolve(self) -> Result<&'static str, CliError> {
        match self {
            Self::X86_64 => Ok("x86_64"),
            Self::Aarch64 => Ok("aarch64"),
            Self::Auto => match std::env::consts::ARCH {
                "x86_64" => Ok("x86_64"),
                "aarch64" => Ok("aarch64"),
                other => Err(CliError::usage(format!(
                    "cannot auto-detect a supported architecture from host `{other}`; pass --arch"
                ))),
            },
        }
    }
}

#[derive(Debug, Args, Clone)]
pub struct DeployArgs {
    /// Function name or id.
    #[arg(long)]
    pub function: String,
    /// Path of the executable to upload. For the firecracker provider this must be a
    /// static Linux binary (musl); the CLI does not validate the format.
    #[arg(long)]
    pub binary: PathBuf,
    #[arg(long, value_enum, default_value_t = ArchArg::Auto)]
    pub arch: ArchArg,
    #[arg(long)]
    pub memory_mib: Option<u32>,
    #[arg(long)]
    pub cpu_millis: Option<u32>,
    /// Writable scratch space (`/tmp`) of each environment, in MiB. The firecracker
    /// provider backs it with a drive of exactly this size (default 256).
    #[arg(long)]
    pub ephemeral_storage_mib: Option<u32>,
    #[arg(long)]
    pub timeout_seconds: Option<u32>,
    #[arg(long)]
    pub init_timeout_seconds: Option<u32>,
    #[arg(long)]
    pub max_concurrency: Option<u32>,
    /// Environments kept provisioned while an alias routes the revision (default 0:
    /// scale to zero). Needs a gateway with environment reuse on.
    #[arg(long)]
    pub min_ready: Option<u32>,
    /// Idle seconds before a pooled environment may be scaled down (default: the
    /// gateway's `[pool] idle_ttl_seconds`).
    #[arg(long)]
    pub idle_ttl_seconds: Option<u32>,
    /// No scale-down within this many seconds of a scale-up or activation (default: the
    /// gateway's `[scaling] scale_down_cooldown_seconds`).
    #[arg(long)]
    pub scale_down_cooldown_seconds: Option<u32>,
    /// Non-secret environment variable `KEY=VALUE` (repeatable).
    #[arg(long = "env", value_name = "KEY=VALUE")]
    pub env: Vec<String>,
    /// Secret binding `ENV_NAME=binding_ref` (repeatable). Values never pass through the CLI.
    #[arg(long = "secret", value_name = "ENV_NAME=BINDING_REF")]
    pub secret: Vec<String>,
    #[arg(long, default_value = "")]
    pub description: String,
    /// Egress profile: `none` (default, no network device), `restricted` (only the
    /// `--egress-allow` destinations) or `public-web` (public IPv4 unicast, DNS through
    /// the provider's resolver).
    #[arg(long, value_name = "PROFILE")]
    pub egress: Option<String>,
    /// Destination a `restricted` revision may open (repeatable):
    /// `[tcp|udp:]CIDR:PORT[,PORT...]`, e.g. `1.1.1.1/32:443` or `udp:1.1.1.1/32:53`.
    #[arg(long = "egress-allow", value_name = "[PROTO:]CIDR:PORTS")]
    pub egress_allow: Vec<String>,
    /// Region the revision must run in (e.g. `jp`). A gateway whose node has another
    /// or no region label rejects its invocations; the constraint is never relaxed.
    #[arg(long = "region", value_name = "REGION")]
    pub region: Option<String>,
    /// Do not move the `prod` alias to the new revision.
    #[arg(long)]
    pub no_publish: bool,
    /// Poll the revision until it is `ready` or `failed` (default). `--no-wait` returns immediately.
    #[arg(long, overrides_with = "no_wait", default_value_t = true)]
    pub wait: bool,
    #[arg(long, overrides_with = "wait")]
    pub no_wait: bool,
    /// Maximum time to wait for the revision, in seconds.
    #[arg(long, default_value_t = 120)]
    pub wait_timeout: u64,
}

impl DeployArgs {
    pub fn should_wait(&self) -> bool {
        !self.no_wait
    }
}

#[derive(Debug, Subcommand)]
pub enum TriggersCommand {
    /// Create a cron or webhook trigger. A webhook trigger's secret is printed once.
    Create(TriggerCreateArgs),
    /// List the triggers of a function.
    List { function: String },
    /// Show one trigger (never its secret).
    Get { function: String, trigger: String },
    /// Change, enable / disable or rotate the secret of a trigger (CAS on
    /// `--expected-generation`).
    Update(TriggerUpdateArgs),
    /// Delete a trigger: no new fires; fires accepted before continue.
    Delete { function: String, trigger: String },
    /// Show the fires of a trigger (scheduled times and webhook events).
    Fires {
        function: String,
        trigger: String,
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Print the timestamp and signature headers of a webhook delivery (offline).
    WebhookSign(WebhookSignArgs),
}

#[derive(Debug, Args, Clone)]
pub struct TriggerCreateArgs {
    /// Function name or id.
    pub function: String,
    #[arg(long)]
    pub name: String,
    /// `cron` or `webhook`.
    #[arg(long)]
    pub kind: String,
    /// Create it disabled.
    #[arg(long)]
    pub disabled: bool,
    /// Alias resolved at each fire (default `prod`).
    #[arg(long, conflicts_with = "revision_id")]
    pub alias: Option<String>,
    /// Pin a revision instead of an alias.
    #[arg(long)]
    pub revision_id: Option<String>,
    /// Cron: 5 fields, or 6 with a leading seconds field (quote it).
    #[arg(long)]
    pub schedule: Option<String>,
    /// Cron: IANA time zone (default UTC).
    #[arg(long)]
    pub timezone: Option<String>,
    /// Cron: static JSON payload.
    #[arg(long)]
    pub payload: Option<String>,
    /// Cron: skip (default), run-once or run-all (with --max-runs).
    #[arg(long)]
    pub missed_run: Option<String>,
    #[arg(long)]
    pub max_runs: Option<u32>,
    /// Webhook: accepted clock difference in seconds.
    #[arg(long)]
    pub tolerance_seconds: Option<u64>,
    /// Webhook: largest accepted body.
    #[arg(long)]
    pub max_body_bytes: Option<u64>,
    /// Webhook: header carrying the event id (default x-tachyon-webhook-id).
    #[arg(long)]
    pub event_id_header: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct TriggerUpdateArgs {
    /// Function name or id.
    pub function: String,
    pub trigger: String,
    #[arg(long)]
    pub expected_generation: Option<u64>,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub enable: bool,
    #[arg(long)]
    pub disable: bool,
    #[arg(long, conflicts_with = "revision_id")]
    pub alias: Option<String>,
    #[arg(long)]
    pub revision_id: Option<String>,
    #[arg(long)]
    pub schedule: Option<String>,
    #[arg(long)]
    pub timezone: Option<String>,
    #[arg(long)]
    pub payload: Option<String>,
    #[arg(long)]
    pub missed_run: Option<String>,
    #[arg(long)]
    pub max_runs: Option<u32>,
    #[arg(long)]
    pub tolerance_seconds: Option<u64>,
    #[arg(long)]
    pub max_body_bytes: Option<u64>,
    #[arg(long)]
    pub event_id_header: Option<String>,
    /// Webhook: generate a new secret (printed once); the old one stops working.
    #[arg(long)]
    pub rotate_secret: bool,
}

#[derive(Debug, Args, Clone)]
pub struct WebhookSignArgs {
    /// The trigger secret (`whsec_...`). Prefer --secret-env: arguments are visible
    /// in the process list.
    #[arg(long)]
    pub secret: Option<String>,
    /// Environment variable holding the secret.
    #[arg(long)]
    pub secret_env: Option<String>,
    /// Body exactly as it will be sent.
    #[arg(long)]
    pub body: Option<String>,
    #[arg(long)]
    pub body_file: Option<PathBuf>,
    /// Unix seconds to sign (default now).
    #[arg(long, allow_hyphen_values = true)]
    pub timestamp: Option<i64>,
}

#[derive(Debug, Args, Clone)]
pub struct InvokeArgs {
    /// Function name or id.
    pub function: String,
    /// Inline JSON payload (default `{}`).
    #[arg(long, conflicts_with = "payload_file")]
    pub payload: Option<String>,
    /// Read the JSON payload from a file.
    #[arg(long)]
    pub payload_file: Option<PathBuf>,
    /// Alias to resolve (server default `prod`).
    #[arg(long, conflicts_with = "revision_id")]
    pub alias: Option<String>,
    /// Pin a specific revision instead of an alias.
    #[arg(long)]
    pub revision_id: Option<String>,
    /// Sent as `x-tachyon-client-timeout-ms`.
    #[arg(long)]
    pub client_timeout_ms: Option<u64>,
    /// Sent as `idempotency-key`.
    #[arg(long)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct HttpArgs {
    /// Function name or id.
    pub function: String,
    #[arg(long, default_value = "GET")]
    pub method: String,
    /// Path (and optional query string) forwarded to the function.
    #[arg(long, default_value = "/")]
    pub path: String,
    /// Request body.
    #[arg(long, conflicts_with = "data_file")]
    pub data: Option<String>,
    #[arg(long)]
    pub data_file: Option<PathBuf>,
    /// Request header `Name: value` (repeatable).
    #[arg(long = "header", value_name = "NAME:VALUE")]
    pub header: Vec<String>,
    /// Also print response headers.
    #[arg(long, short = 'v')]
    pub verbose: bool,
}

#[derive(Debug, Args, Clone)]
pub struct LogsArgs {
    /// Invocation id.
    #[arg(
        long,
        conflicts_with = "function",
        required_unless_present = "function"
    )]
    pub invocation: Option<String>,
    /// Function name or id: prints the logs of its most recent invocations.
    #[arg(long)]
    pub function: Option<String>,
    /// Number of recent invocations to include with `--function`.
    #[arg(long, default_value_t = 5)]
    pub limit: u32,
}

/// `tsls usage` (PLT-4642).
#[derive(Debug, Args, Clone)]
pub struct UsageArgs {
    /// Inclusive start: RFC 3339 or YYYY-MM-DD (default: 31 days before `--to`).
    #[arg(long)]
    pub from: Option<String>,
    /// Exclusive end: RFC 3339 or YYYY-MM-DD (default: now).
    #[arg(long)]
    pub to: Option<String>,
    /// `function`, `day`, `function,day` (default) or `none`.
    #[arg(long)]
    pub group_by: Option<String>,
    /// Only this function (name or fn_ id).
    #[arg(long)]
    pub function: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct DevArgs {
    /// Executable to run (host-native binary; process provider).
    #[arg(long)]
    pub binary: PathBuf,
    /// JSON payload for the single invocation (default `{}`).
    #[arg(long, default_value = "{}")]
    pub payload: String,
    /// Gateway executable (default: `tachyon-serverless-gateway` next to `tsls`).
    #[arg(long)]
    pub gateway_binary: Option<PathBuf>,
    /// Runtime bridge executable (default: `tachyon-serverless-runtime-bridge` next to `tsls`).
    #[arg(long)]
    pub bridge_binary: Option<PathBuf>,
    /// Flag used to pass the generated config path to the gateway.
    #[arg(long, default_value = "--config")]
    pub gateway_config_flag: String,
    /// Keep the temporary directory (config, data, gateway log) after the run.
    #[arg(long)]
    pub keep: bool,
    /// Function timeout used for the throwaway revision, in seconds.
    #[arg(long, default_value_t = 30)]
    pub timeout_seconds: u32,
}

/// Parse `KEY=VALUE`.
pub fn parse_key_value(raw: &str, what: &str) -> Result<(String, String), CliError> {
    match raw.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(CliError::usage(format!(
            "invalid {what} `{raw}`: expected KEY=VALUE"
        ))),
    }
}

/// Parse `--egress-allow [tcp|udp:]CIDR:PORT[,PORT...]` into `(protocol, cidr, ports)`.
/// The CIDR itself is validated by the server.
pub fn parse_egress_allow(raw: &str) -> Result<(Option<String>, String, Vec<u16>), CliError> {
    let usage = || {
        CliError::usage(format!(
            "invalid --egress-allow `{raw}`: expected [tcp|udp:]CIDR:PORT[,PORT...]"
        ))
    };
    let parts: Vec<&str> = raw.split(':').collect();
    let (protocol, cidr, ports) = match parts.as_slice() {
        [cidr, ports] => (None, *cidr, *ports),
        [proto @ ("tcp" | "udp"), cidr, ports] => (Some((*proto).to_string()), *cidr, *ports),
        _ => return Err(usage()),
    };
    if cidr.is_empty() {
        return Err(usage());
    }
    let ports = ports
        .split(',')
        .map(|p| p.trim().parse::<u16>().map_err(|_| usage()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((protocol, cidr.to_string(), ports))
}

/// Parse `Name: value` (the space after the colon is optional).
pub fn parse_header(raw: &str) -> Result<(String, String), CliError> {
    match raw.split_once(':') {
        Some((k, v)) if !k.trim().is_empty() => Ok((k.trim().to_string(), v.trim().to_string())),
        _ => Err(CliError::usage(format!(
            "invalid header `{raw}`: expected NAME:VALUE"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("tsls").chain(args.iter().copied()))
    }

    #[test]
    fn global_flags_and_defaults() {
        let cli = parse(&[
            "--api-url",
            "http://localhost:1",
            "--token",
            "t",
            "--json",
            "functions",
            "list",
        ])
        .unwrap();
        assert_eq!(cli.api_url, "http://localhost:1");
        assert_eq!(cli.token.as_deref(), Some("t"));
        assert!(cli.json);
        assert_eq!(cli.timeout_secs, 120);
        assert!(!cli.colon_routes);
        assert!(matches!(
            cli.command,
            Command::Functions {
                command: FunctionsCommand::List
            }
        ));
    }

    #[test]
    fn global_flags_after_subcommand() {
        let cli = parse(&["functions", "list", "--json", "--colon-routes"]).unwrap();
        assert!(cli.json);
        assert!(cli.colon_routes);
    }

    #[test]
    fn deploy_arguments() {
        let cli = parse(&[
            "functions",
            "deploy",
            "--function",
            "hello",
            "--binary",
            "/tmp/hello",
            "--env",
            "A=1",
            "--env",
            "B=2",
            "--secret",
            "S=ref",
            "--timeout-seconds",
            "2",
            "--no-publish",
            "--no-wait",
        ])
        .unwrap();
        let Command::Functions {
            command: FunctionsCommand::Deploy(d),
        } = cli.command
        else {
            panic!("expected deploy");
        };
        assert_eq!(d.function, "hello");
        assert_eq!(d.env, vec!["A=1", "B=2"]);
        assert_eq!(d.secret, vec!["S=ref"]);
        assert_eq!(d.timeout_seconds, Some(2));
        assert!(d.no_publish);
        assert!(!d.should_wait());
        assert_eq!(d.arch, ArchArg::Auto);
        assert_eq!(d.wait_timeout, 120);
    }

    #[test]
    fn deploy_waits_by_default() {
        let cli = parse(&["functions", "deploy", "--function", "f", "--binary", "b"]).unwrap();
        let Command::Functions {
            command: FunctionsCommand::Deploy(d),
        } = cli.command
        else {
            panic!("expected deploy");
        };
        assert!(d.should_wait());
    }

    #[test]
    fn invoke_payload_conflicts() {
        assert!(
            parse(&[
                "functions",
                "invoke",
                "f",
                "--payload",
                "{}",
                "--payload-file",
                "x.json"
            ])
            .is_err()
        );
        assert!(
            parse(&[
                "functions",
                "invoke",
                "f",
                "--alias",
                "a",
                "--revision-id",
                "r"
            ])
            .is_err()
        );
        let cli = parse(&[
            "functions",
            "invoke",
            "f",
            "--payload",
            "{\"a\":1}",
            "--client-timeout-ms",
            "500",
            "--idempotency-key",
            "k",
        ])
        .unwrap();
        let Command::Functions {
            command: FunctionsCommand::Invoke(i),
        } = cli.command
        else {
            panic!("expected invoke");
        };
        assert_eq!(i.client_timeout_ms, Some(500));
        assert_eq!(i.idempotency_key.as_deref(), Some("k"));
    }

    #[test]
    fn logs_requires_invocation_or_function() {
        assert!(parse(&["functions", "logs"]).is_err());
        assert!(parse(&["functions", "logs", "--invocation", "inv_x"]).is_ok());
        assert!(parse(&["functions", "logs", "--function", "f", "--limit", "3"]).is_ok());
        assert!(parse(&["functions", "logs", "--function", "f", "--invocation", "i"]).is_err());
    }

    #[test]
    fn arch_values() {
        let cli = parse(&[
            "functions",
            "deploy",
            "--function",
            "f",
            "--binary",
            "b",
            "--arch",
            "x86_64",
        ])
        .unwrap();
        let Command::Functions {
            command: FunctionsCommand::Deploy(d),
        } = cli.command
        else {
            panic!("expected deploy");
        };
        assert_eq!(d.arch, ArchArg::X86_64);
        assert_eq!(ArchArg::X86_64.resolve().unwrap(), "x86_64");
        assert_eq!(ArchArg::Aarch64.resolve().unwrap(), "aarch64");
        let auto = ArchArg::Auto.resolve().unwrap();
        assert!(auto == "x86_64" || auto == "aarch64");
    }

    #[test]
    fn key_value_and_header_parsing() {
        assert_eq!(
            parse_key_value("A=b=c", "env").unwrap(),
            ("A".into(), "b=c".into())
        );
        assert!(parse_key_value("=x", "env").is_err());
        assert!(parse_key_value("novalue", "env").is_err());
        assert_eq!(
            parse_header("X-Demo: 1").unwrap(),
            ("X-Demo".into(), "1".into())
        );
        assert!(parse_header("nocolon").is_err());
    }

    #[test]
    fn other_commands_parse() {
        assert!(parse(&["provider"]).is_ok());
        assert!(parse(&["capacity"]).is_ok());
        assert!(parse(&["health"]).is_ok());
        assert!(parse(&["dev", "--binary", "/tmp/x", "--keep"]).is_ok());
        assert!(parse(&["functions", "cancel", "inv_x"]).is_ok());
        assert!(parse(&["functions", "rollback", "hello", "--to", "rev_x"]).is_ok());
        assert!(
            parse(&[
                "functions",
                "alias-set",
                "hello",
                "--revision-id",
                "rev_x",
                "--expected-generation",
                "3"
            ])
            .is_ok()
        );
        assert!(
            parse(&[
                "functions",
                "http",
                "h",
                "--method",
                "POST",
                "--path",
                "/echo",
                "--data",
                "x",
                "--header",
                "a:b",
                "-v"
            ])
            .is_ok()
        );
        assert!(parse(&["functions", "nope"]).is_err());
    }
}
