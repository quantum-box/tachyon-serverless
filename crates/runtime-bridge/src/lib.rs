//! Tachyon Serverless guest runtime bridge.
//!
//! The bridge is the guest-side agent of an execution environment. It
//! connects to the host (vsock inside a Firecracker microVM, a unix socket
//! under the process provider), performs the `Hello` / `HelloAck` handshake,
//! serves the Runtime API to the user process over loopback HTTP and
//! supervises that process: log forwarding, one attempt in flight, cancel
//! and shutdown signalling (docs/protocol.md).
//!
//! The binary is `tachyon-serverless-runtime-bridge`; the library exposes
//! the pieces so they can be unit tested.

pub mod cli;
pub mod init;
pub mod process;
pub mod runtime_api;
pub mod selftest;
pub mod session;
pub mod transport;

use cli::{Cli, Command, TransportKind};
use session::{SessionConfig, exit_code};
use tracing::error;

/// Entry point shared by `main`: returns the process exit code.
pub async fn run(cli: Cli) -> i32 {
    match cli.command {
        Some(Command::SelfTestUser) => selftest::run().await,
        None => run_bridge(cli).await,
    }
}

/// In `--init` mode the kernel cmdline overrides the CLI values.
#[cfg(target_os = "linux")]
fn boot_overrides(cli: &Cli) -> Result<(Option<String>, u32), i32> {
    if !cli.init {
        return Ok((cli.environment_id.clone(), cli.vsock_port));
    }
    match init::setup() {
        Ok(params) => Ok((
            params.env_id.or_else(|| cli.environment_id.clone()),
            params.vsock_port.unwrap_or(cli.vsock_port),
        )),
        Err(e) => {
            error!("init setup failed: {e}");
            Err(exit_code::TRANSPORT)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn boot_overrides(cli: &Cli) -> Result<(Option<String>, u32), i32> {
    if cli.init {
        error!("--init (PID 1 mode) is only supported on Linux guests");
        return Err(exit_code::REJECTED);
    }
    Ok((cli.environment_id.clone(), cli.vsock_port))
}

async fn run_bridge(cli: Cli) -> i32 {
    let (environment_id, vsock_port) = match boot_overrides(&cli) {
        Ok(v) => v,
        Err(code) => return code,
    };

    let Some(environment_id) = environment_id else {
        error!(
            "--environment-id is required (or tachyon.env_id on the kernel cmdline in --init mode)"
        );
        return exit_code::REJECTED;
    };

    let stream = match cli.transport {
        TransportKind::Unix => {
            let Some(path) = cli.unix_path.as_ref() else {
                error!("--unix-path is required with --transport unix");
                return exit_code::REJECTED;
            };
            transport::connect_unix(path).await
        }
        TransportKind::Vsock => transport::connect_vsock(cli.vsock_cid, vsock_port).await,
    };
    let stream = match stream {
        Ok(s) => s,
        Err(e) => {
            error!("cannot connect to host: {e}");
            return exit_code::TRANSPORT;
        }
    };

    let unisolated = std::env::var(tachyon_serverless_protocol::env::UNISOLATED)
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false);
    session::run_session(
        stream,
        SessionConfig {
            environment_id,
            runtime_api_addr: cli.runtime_api_addr,
            guest_boot_id: init::read_boot_id(),
            unisolated,
        },
    )
    .await
}
