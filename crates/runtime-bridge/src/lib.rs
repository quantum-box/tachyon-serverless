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
#[cfg(feature = "experimental-restore")]
pub mod restore_link;
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
    let session_cfg = SessionConfig {
        environment_id,
        runtime_api_addr: cli.runtime_api_addr,
        guest_boot_id: init::read_boot_id(),
        unisolated,
    };
    #[cfg(feature = "experimental-restore")]
    {
        linked::run(&cli, vsock_port, stream, session_cfg).await
    }
    #[cfg(not(feature = "experimental-restore"))]
    {
        session::run_session(stream, session_cfg).await
    }
}

/// The session behind the restore link (X1, PLT-4653).
#[cfg(feature = "experimental-restore")]
mod linked {
    use std::sync::Arc;
    use std::time::Duration;

    use tracing::{error, warn};

    use crate::cli::{Cli, TransportKind};
    use crate::restore_link::{self, Connect, LinkConfig, LinkOutcome};
    use crate::session::{self, SessionConfig};
    use crate::transport::BoxedHostStream;

    pub async fn run(
        cli: &Cli,
        vsock_port: u32,
        stream: BoxedHostStream,
        cfg: SessionConfig,
    ) -> i32 {
        let connect: Connect = match cli.transport {
            TransportKind::Unix => {
                let path = cli.unix_path.clone().unwrap_or_default();
                Box::new(move || {
                    let p = path.clone();
                    Box::pin(async move {
                        let s = tokio::net::UnixStream::connect(p).await?;
                        Ok(Box::new(s) as BoxedHostStream)
                    })
                })
            }
            TransportKind::Vsock => {
                let cid = cli.vsock_cid;
                Box::new(move || Box::pin(crate::transport::connect_vsock_once(cid, vsock_port)))
            }
        };
        #[cfg(target_os = "linux")]
        let (arm_doorbell, set_clock): (
            Option<restore_link::ArmDoorbell>,
            Option<restore_link::SetClock>,
        ) = if cli.init && cli.transport == TransportKind::Vsock {
            (
                Some(restore_link::vsock_doorbell(
                    tachyon_serverless_protocol::DOORBELL_VSOCK_PORT,
                )),
                Some(Box::new(restore_link::set_realtime_clock)),
            )
        } else {
            (None, None)
        };
        #[cfg(not(target_os = "linux"))]
        let (arm_doorbell, set_clock) = (None, None);
        let link_cfg = LinkConfig {
            environment_id: cfg.environment_id.clone(),
            guest_boot_id: cfg.guest_boot_id.clone(),
            reconnect_budget: Duration::from_secs(120),
            reconnect_interval: Duration::from_millis(20),
            arm_doorbell,
            set_clock,
        };
        let (source, link) = restore_link::link();
        let (session_side, link_side) =
            tokio::io::duplex(2 * tachyon_serverless_protocol::MAX_FRAME_BYTES);
        let pump = tokio::spawn(link.run(link_side, stream, connect, link_cfg));
        let code = session::run_session_with(Box::new(session_side), cfg, Arc::new(source)).await;
        match tokio::time::timeout(Duration::from_secs(2), pump).await {
            Ok(Ok(LinkOutcome::GaveUp(e))) => error!("restore link gave up: {e}"),
            Ok(Ok(LinkOutcome::HostLost(e))) => warn!("restore link: host connection ended: {e}"),
            Ok(Ok(LinkOutcome::SessionClosed)) => {}
            Ok(Err(e)) => error!("restore link task failed: {e}"),
            Err(_) => warn!("restore link did not stop"),
        }
        code
    }
}
