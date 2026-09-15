//! `tsls dev`: run one binary end to end on a throwaway local gateway.
//!
//! The gateway is started with a generated config (`profile = "dev"`, process
//! provider) in a temporary directory, the binary is deployed as function
//! `dev`, invoked once, logs are printed, then the gateway gets SIGTERM and the
//! directory is removed (unless `--keep`).
//!
//! The process provider has NO isolation: the function runs as a plain child
//! process of the current user.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tachyon_serverless_api_types::{CreateFunctionRequest, FunctionResponse, headers};
use tokio::process::{Child, Command};

use crate::args::{ArchArg, DeployArgs, DevArgs, InvokeArgs};
use crate::client::{ApiClient, ClientConfig};
use crate::commands::{deploy, invoke, logs};
use crate::error::CliError;
use crate::output::Printer;

pub const GATEWAY_BINARY: &str = "tachyon-serverless-gateway";
pub const BRIDGE_BINARY: &str = "tachyon-serverless-runtime-bridge";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(10);

pub const BANNER: &str = "\
==================================================================
 tsls dev: PROCESS PROVIDER -- NO ISOLATION
 The function runs as a plain child process of this user on this
 machine. This is a development convenience, not a sandbox.
==================================================================";

/// Locate a sibling executable of the running `tsls` binary.
pub fn sibling_binary(explicit: Option<&Path>, name: &str) -> Result<PathBuf, CliError> {
    if let Some(p) = explicit {
        return if p.exists() {
            Ok(p.to_path_buf())
        } else {
            Err(CliError::usage(format!("{} not found", p.display())))
        };
    }
    let exe = std::env::current_exe()
        .map_err(|e| CliError::usage(format!("cannot locate current executable: {e}")))?;
    let dir = exe
        .parent()
        .ok_or_else(|| CliError::usage("cannot determine executable directory"))?;
    let candidate = dir.join(name);
    if candidate.exists() {
        Ok(candidate)
    } else {
        Err(CliError::usage(format!(
            "{name} not found next to {} (build it with `cargo build -p tachyon-serverless-gateway -p tachyon-serverless-runtime-bridge` or pass --gateway-binary / --bridge-binary)",
            exe.display()
        )))
    }
}

/// Identity generated for one `tsls dev` run.
#[derive(Debug, Clone)]
pub struct DevIdentity {
    pub token: String,
    pub tenant_id: String,
}

impl DevIdentity {
    pub fn generate() -> Self {
        let ulid = ulid::Ulid::new().to_string().to_ascii_lowercase();
        Self {
            token: format!(
                "dev-{}-{}",
                ulid,
                ulid::Ulid::new().to_string().to_ascii_lowercase()
            ),
            tenant_id: format!("tn_{ulid}"),
        }
    }
}

/// Bind to port 0 and release it. Racy by nature but fine for a dev helper.
pub fn free_port() -> Result<u16, CliError> {
    let l = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(l.local_addr()?.port())
}

/// Render the gateway config described in `docs/architecture.md` §4.
pub fn render_gateway_config(
    listen: &str,
    data_dir: &Path,
    bridge_binary: &Path,
    workdir: &Path,
    identity: &DevIdentity,
) -> Result<String, CliError> {
    let v = serde_json::json!({
        "listen": listen,
        "profile": "dev",
        "data_dir": data_dir.to_string_lossy(),
        "provider": {
            "kind": "process",
            "process": {
                "bridge_binary": bridge_binary.to_string_lossy(),
                "workdir": workdir.to_string_lossy(),
            }
        },
        "capacity": {
            "max_concurrency": 4,
            "max_queue": 8,
            "queue_timeout_seconds": 10,
        },
        "identity": {
            "tokens": [{
                "token": identity.token,
                "tenant_id": identity.tenant_id,
                "subject": "tsls-dev",
                "roles": ["deploy", "invoke"],
            }]
        },
        "secrets": { "bindings": [] },
    });
    toml::to_string(&v).map_err(|e| CliError::usage(format!("cannot render gateway config: {e}")))
}

async fn wait_for_health(
    client: &ApiClient,
    child: &mut Child,
    log: &Path,
) -> Result<(), CliError> {
    let start = Instant::now();
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| CliError::usage(format!("cannot poll gateway: {e}")))?
        {
            return Err(CliError::failed(
                crate::error::ExitCode::Platform,
                format!(
                    "gateway exited early with {status}; see {}\n{}",
                    log.display(),
                    log_tail(log)
                ),
                None,
            ));
        }
        if let Ok(r) = client.get_unauth("/healthz").await
            && r.is_success()
        {
            return Ok(());
        }
        if start.elapsed() >= HEALTH_TIMEOUT {
            return Err(CliError::Timeout(format!(
                "gateway did not answer /healthz within {}s; see {}\n{}",
                HEALTH_TIMEOUT.as_secs(),
                log.display(),
                log_tail(log)
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn log_tail(path: &Path) -> String {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let lines: Vec<&str> = s.lines().collect();
            let start = lines.len().saturating_sub(20);
            lines[start..].join("\n")
        }
        Err(_) => String::new(),
    }
}

async fn terminate_gateway(child: &mut Child, p: &mut Printer<'_>) -> Result<(), CliError> {
    if let Some(pid) = child.id() {
        // SAFETY: plain libc call with a pid we own; kill(2) has no memory-safety preconditions.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }
        match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(Ok(status)) => p.note(format!("gateway stopped ({status})"))?,
            _ => {
                p.note("gateway did not stop after SIGTERM; killing")?;
                let _ = child.kill().await;
            }
        }
    }
    Ok(())
}

pub async fn run(args: &DevArgs, base: &ClientConfig, p: &mut Printer<'_>) -> Result<(), CliError> {
    p.note(BANNER)?;
    if !args.binary.exists() {
        return Err(CliError::usage(format!(
            "--binary {} does not exist",
            args.binary.display()
        )));
    }
    let gateway = sibling_binary(args.gateway_binary.as_deref(), GATEWAY_BINARY)?;
    let bridge = sibling_binary(args.bridge_binary.as_deref(), BRIDGE_BINARY)?;
    let tmp = tempfile::Builder::new()
        .prefix("tsls-dev-")
        .tempdir()
        .map_err(|e| CliError::usage(format!("cannot create temp dir: {e}")))?;
    let root = tmp.path().to_path_buf();
    let data_dir = root.join("data");
    let workdir = root.join("process");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&workdir)?;
    let port = free_port()?;
    let listen = format!("127.0.0.1:{port}");
    let identity = DevIdentity::generate();
    let config = render_gateway_config(&listen, &data_dir, &bridge, &workdir, &identity)?;
    let config_path = root.join("gateway.toml");
    std::fs::write(&config_path, config)?;
    let log_path = root.join("gateway.log");
    let log_file = std::fs::File::create(&log_path)?;
    let log_err = log_file.try_clone()?;

    p.note(format!("workdir {}", root.display()))?;
    p.note(format!("starting {} on {listen}", gateway.display()))?;
    let mut child = Command::new(&gateway)
        .arg(&args.gateway_config_flag)
        .arg(&config_path)
        .env("TACHYON_GATEWAY_CONFIG", &config_path)
        .env("LOG_FORMAT", "json")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err))
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| CliError::usage(format!("cannot start {}: {e}", gateway.display())))?;

    let client = ApiClient::new(ClientConfig {
        api_url: format!("http://{listen}"),
        token: Some(identity.token.clone()),
        tenant_id: Some(identity.tenant_id.clone()),
        timeout: base.timeout,
        colon_routes: base.colon_routes,
    })?;

    let result = run_inner(args, &client, &mut child, &log_path, p).await;
    terminate_gateway(&mut child, p).await?;
    if args.keep {
        let kept = tmp.keep();
        p.note(format!("kept {}", kept.display()))?;
    }
    result
}

async fn run_inner(
    args: &DevArgs,
    client: &ApiClient,
    child: &mut Child,
    log_path: &Path,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    wait_for_health(client, child, log_path).await?;
    p.note("gateway healthy")?;

    let created: FunctionResponse = client
        .post_json(
            "/v1/functions",
            &CreateFunctionRequest {
                name: "dev".into(),
                description: "tsls dev".into(),
            },
        )
        .await?
        .ok()?
        .json()?;
    p.note(format!("function dev = {}", created.id))?;

    let deploy_args = DeployArgs {
        function: created.id.clone(),
        binary: args.binary.clone(),
        arch: ArchArg::Auto,
        memory_mib: None,
        cpu_millis: None,
        timeout_seconds: Some(args.timeout_seconds),
        init_timeout_seconds: None,
        max_concurrency: Some(1),
        env: Vec::new(),
        secret: Vec::new(),
        description: "tsls dev".into(),
        no_publish: false,
        wait: true,
        no_wait: false,
        wait_timeout: 120,
    };
    // Deploy output is progress only; suppress its stdout rendering by using a
    // JSON printer into a sink.
    let mut sink = Vec::new();
    let outcome = {
        let mut quiet = Printer {
            out: &mut sink,
            err: p.err,
            json: true,
        };
        deploy::deploy(client, &deploy_args, &mut quiet).await?
    };
    p.note(format!(
        "revision {} ready (alias prod generation {})",
        outcome.revision.id,
        outcome
            .alias
            .as_ref()
            .map(|a| a.generation.to_string())
            .unwrap_or_else(|| "-".into())
    ))?;

    let invoke_args = InvokeArgs {
        function: created.id.clone(),
        payload: Some(args.payload.clone()),
        payload_file: None,
        alias: None,
        revision_id: None,
        client_timeout_ms: None,
        idempotency_key: None,
    };
    let payload = invoke::load_payload(&invoke_args)?;
    p.note(format!("invoking with {payload}"))?;
    let resp = invoke::send_invoke(client, &created.id, &invoke_args, payload).await?;
    let invocation_id = resp
        .header(headers::INVOCATION_ID)
        .or_else(|| resp.api_error().and_then(|b| b.error.invocation_id));
    let invoke_result = if resp.is_success() {
        invoke::print_invoke_response(&resp, p)
    } else {
        Err(resp.into_error())
    };
    if let Some(id) = invocation_id {
        p.note(format!("logs of {id}:"))?;
        // Logs are best effort; the invocation result decides the exit code.
        if let Err(e) = logs::print_invocation_logs(client, &id, p).await {
            p.note(format!("could not fetch logs: {e}"))?;
        }
    }
    invoke_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_renders_valid_toml_matching_the_documented_schema() {
        let id = DevIdentity::generate();
        assert!(id.tenant_id.starts_with("tn_"));
        assert_eq!(id.tenant_id.len(), 3 + 26);
        let s = render_gateway_config(
            "127.0.0.1:4242",
            Path::new("/tmp/x/data"),
            Path::new("/tmp/x/bridge"),
            Path::new("/tmp/x/process"),
            &id,
        )
        .unwrap();
        let v: toml::Value = toml::from_str(&s).unwrap();
        assert_eq!(v["listen"].as_str(), Some("127.0.0.1:4242"));
        assert_eq!(v["profile"].as_str(), Some("dev"));
        assert_eq!(v["provider"]["kind"].as_str(), Some("process"));
        assert_eq!(
            v["provider"]["process"]["bridge_binary"].as_str(),
            Some("/tmp/x/bridge")
        );
        let tokens = v["identity"]["tokens"].as_array().unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0]["token"].as_str(), Some(id.token.as_str()));
        assert_eq!(tokens[0]["tenant_id"].as_str(), Some(id.tenant_id.as_str()));
        assert_eq!(v["capacity"]["max_concurrency"].as_integer(), Some(4));
    }

    #[test]
    fn free_port_is_nonzero() {
        assert!(free_port().unwrap() > 0);
    }

    #[test]
    fn missing_sibling_is_usage_error() {
        let r = sibling_binary(Some(Path::new("/nonexistent/gateway")), GATEWAY_BINARY);
        assert!(matches!(r, Err(CliError::Usage(_))));
    }
}
