//! `x1-guest-init` (PLT-4652, experimental): PID 1 of the X1 experiment
//! rootfs. Same guest contract as `/sbin/tachyon-init` (`docs/protocol.md`
//! section C: mounts, cmdline, vsock to CID 2) and the unmodified bridge
//! session, but the session talks to a frame pump that survives a vsock
//! transport reset and answers the lifecycle `continue` from the host
//! (`tachyon_serverless_x1_restore::pump`). Never part of a product rootfs.

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("x1-guest-init only runs as PID 1 inside a Linux microVM");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
    if std::process::id() != 1 {
        eprintln!("x1-guest-init must run as PID 1 (init=/sbin/tachyon-init)");
        std::process::exit(2);
    }
    let code = match tachyon_serverless_runtime_bridge::init::setup() {
        Ok(params) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let code = runtime.block_on(linux::run(params));
            runtime.shutdown_timeout(std::time::Duration::from_millis(200));
            code
        }
        Err(e) => {
            tracing::error!("init setup failed: {e}");
            4
        }
    };
    tachyon_serverless_runtime_bridge::init::power_off(code)
}

#[cfg(target_os = "linux")]
mod linux {
    use std::path::PathBuf;
    use std::time::Duration;

    use tachyon_serverless_runtime_bridge::init::{self, BootParams, SCRATCH_MOUNT};
    use tachyon_serverless_runtime_bridge::session::{self, SessionConfig, exit_code};
    use tachyon_serverless_x1_restore::pump::{PumpConfig, PumpOutcome, restore_pair, run_pump};
    use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener, VsockStream};
    use tracing::{error, info, warn};

    const HOST_CID: u32 = 2;
    /// Guest vsock port the host of a restored copy connects to.
    const DOORBELL_PORT: u32 = 5001;

    pub async fn run(params: BootParams) -> i32 {
        let Some(environment_id) = params.env_id.clone() else {
            error!("tachyon.env_id missing from the kernel cmdline");
            return exit_code::REJECTED;
        };
        let port = params
            .vsock_port
            .unwrap_or(tachyon_serverless_protocol::DEFAULT_VSOCK_PORT);
        // Same retry as the bridge's connect_vsock (30 x 100 ms), kept concrete
        // because the pump reconnects with the same type.
        let mut attempt = 0;
        let first = loop {
            match VsockStream::connect(VsockAddr::new(HOST_CID, port)).await {
                Ok(s) => break s,
                Err(e) if attempt >= 29 => {
                    error!("cannot connect to host: {e}");
                    return exit_code::TRANSPORT;
                }
                Err(_) => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let guest_boot_id = init::read_boot_id();
        let (source, handle) = restore_pair();
        let (session_side, pump_side) =
            tokio::io::duplex(2 * tachyon_serverless_protocol::MAX_FRAME_BYTES);
        let cfg = PumpConfig {
            guest_boot_id: guest_boot_id.clone(),
            scratch_marker: params
                .scratch_dev
                .as_ref()
                .map(|_| PathBuf::from(SCRATCH_MOUNT).join("x1-instance")),
            reconnect_budget: Duration::from_secs(600),
            reconnect_interval: Duration::from_millis(20),
        };
        let (reset_tx, resets) = tokio::sync::mpsc::unbounded_channel();
        // Doorbell: a guest listen socket survives a restore (Firecracker only
        // resets connected sockets), so the host of a restored copy connects
        // here to tell the guest at once instead of waiting for its next write.
        let doorbell_tx = reset_tx.clone();
        tokio::spawn(async move {
            match VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, DOORBELL_PORT)) {
                Ok(listener) => loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            info!(peer_cid = peer.cid(), "x1: doorbell rang");
                            drop(stream);
                            let _ = doorbell_tx.send("vsock doorbell".to_string());
                        }
                        Err(e) => {
                            warn!("x1: doorbell accept failed: {e}");
                            break;
                        }
                    }
                },
                Err(e) => warn!("x1: doorbell bind failed: {e}"),
            }
        });
        std::thread::spawn(move || {
            let e = tachyon_serverless_x1_restore::uevent::watch_vmgenid(|| {
                info!("x1: vmgenid changed (restored copy)");
                let _ = reset_tx.send("vmgenid uevent".to_string());
            });
            warn!("x1: uevent watch stopped: {e}");
        });
        let pump = tokio::spawn(run_pump(
            pump_side,
            first,
            move || VsockStream::connect(VsockAddr::new(HOST_CID, port)),
            handle,
            resets,
            cfg,
        ));
        info!(%environment_id, port, "x1: session starting behind the pump");
        let code = session::run_session_with(
            Box::new(session_side),
            SessionConfig {
                environment_id,
                runtime_api_addr: "127.0.0.1:9001".parse().expect("static address"),
                guest_boot_id,
                unisolated: false,
            },
            source,
        )
        .await;
        match tokio::time::timeout(Duration::from_secs(2), pump).await {
            Ok(Ok(PumpOutcome::GaveUp(e))) => error!("x1: pump gave up: {e}"),
            Ok(Ok(PumpOutcome::SessionClosed)) => {}
            Ok(Err(e)) => error!("x1: pump task failed: {e}"),
            Err(_) => error!("x1: pump did not stop"),
        }
        code
    }
}
