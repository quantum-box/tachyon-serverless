//! `tsls`: command line client for the Tachyon Serverless prototype gateway.
//!
//! The CLI talks to the gateway only through the HTTP API described in
//! `crates/api-types`; it never links the application or gateway crates.
//! See `docs/cli.md` for the command reference and exit codes.

pub mod args;
pub mod client;
pub mod commands;
pub mod error;
pub mod output;
pub mod resolve;

use std::io::Write;

pub use args::Cli;
pub use error::{CliError, ExitCode};

/// Execute a parsed command line, writing to `out` / `err`, and return the exit code.
///
/// Errors are rendered here: the human message always goes to `err`; under
/// `--json` the raw server error body (when there is one) goes to `out` so
/// that scripts can read `.error.code`.
pub async fn run(cli: Cli, out: &mut dyn Write, err: &mut dyn Write) -> ExitCode {
    let json = cli.json;
    let mut printer = output::Printer { out, err, json };
    match commands::dispatch(cli, &mut printer).await {
        Ok(()) => ExitCode::Ok,
        Err(e) => {
            let code = e.exit_code();
            if json && let Some(raw) = e.raw_json() {
                let _ = writeln!(printer.out, "{}", raw.trim_end());
            }
            let _ = writeln!(printer.err, "{e}");
            code
        }
    }
}
