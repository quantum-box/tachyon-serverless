//! Command line of the runtime bridge.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TransportKind {
    /// virtio-vsock to the host (Firecracker guest, Linux only).
    Vsock,
    /// Unix domain socket (process provider).
    Unix,
}

#[derive(Debug, Parser)]
#[command(
    name = "tachyon-serverless-runtime-bridge",
    version,
    about = "Guest runtime bridge: connects to the host, serves the Runtime API and supervises the user process."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// How to reach the host.
    #[arg(long, value_enum, default_value_t = TransportKind::Vsock)]
    pub transport: TransportKind,

    /// vsock CID of the host (2 = the hypervisor host).
    #[arg(long, default_value_t = 2)]
    pub vsock_cid: u32,

    /// vsock port the host listens on.
    #[arg(long, default_value_t = tachyon_serverless_protocol::DEFAULT_VSOCK_PORT)]
    pub vsock_port: u32,

    /// Unix socket path (required with --transport unix).
    #[arg(long)]
    pub unix_path: Option<PathBuf>,

    /// Environment id this bridge serves (env_...). In --init mode the kernel
    /// cmdline key tachyon.env_id takes precedence.
    #[arg(long)]
    pub environment_id: Option<String>,

    /// Address of the Runtime API served to the user process. Port 0 picks
    /// an ephemeral port.
    #[arg(long, default_value = "127.0.0.1:9001")]
    pub runtime_api_addr: SocketAddr,

    /// PID 1 mode inside a Firecracker guest (Linux only): mount pseudo file
    /// systems and the function drive, read the kernel cmdline, power off on
    /// exit.
    #[arg(long)]
    pub init: bool,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Internal: act as an SDK user process for the bridge's own tests.
    #[command(hide = true, name = "self-test-user")]
    SelfTestUser,
}
