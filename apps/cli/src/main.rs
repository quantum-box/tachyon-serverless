//! `tsls` binary entry point.

use clap::Parser;

use tachyon_serverless_cli::{Cli, ExitCode, run};

fn main() {
    // Usage errors from clap exit with 1 (`ExitCode::Usage`); `--help` / `--version` exit 0.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            use clap::error::ErrorKind;
            let code = match e.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => ExitCode::Ok,
                _ => ExitCode::Usage,
            };
            let _ = e.print();
            std::process::exit(code.code());
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = runtime.block_on(async {
        let mut out = std::io::stdout().lock();
        let mut err = std::io::stderr().lock();
        run(cli, &mut out, &mut err).await
    });
    std::process::exit(code.code());
}
