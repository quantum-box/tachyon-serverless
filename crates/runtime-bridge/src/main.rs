use clap::Parser;
use tachyon_serverless_runtime_bridge::cli::Cli;

fn main() {
    let mut cli = Cli::parse();
    // The kernel starts `init=/sbin/tachyon-init` without arguments, so PID 1
    // implies --init (docs/protocol.md section C).
    if std::process::id() == 1 {
        cli.init = true;
    }
    let init_mode = cli.init;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = runtime.block_on(tachyon_serverless_runtime_bridge::run(cli));
    // Give spawned tasks no chance to outlive the decision: the process ends here.
    runtime.shutdown_timeout(std::time::Duration::from_millis(200));

    #[cfg(target_os = "linux")]
    if init_mode {
        tachyon_serverless_runtime_bridge::init::power_off(code);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = init_mode;

    std::process::exit(code)
}
