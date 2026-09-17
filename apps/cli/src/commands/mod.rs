//! Command implementations. Each module takes an [`ApiClient`] and a
//! [`Printer`] and returns a [`CliError`] whose exit code the binary uses.

pub mod deploy;
pub mod dev;
pub mod functions;
pub mod http;
pub mod invoke;
pub mod logs;
pub mod provider;
pub mod rollback;
pub mod triggers;
pub mod usage;

use std::time::Duration;

use serde::de::DeserializeOwned;
use tachyon_serverless_api_types::ListResponse;

use crate::args::{Cli, Command, FunctionsCommand, TriggersCommand};
use crate::client::{ApiClient, ApiResponse, ClientConfig};
use crate::error::CliError;
use crate::output::Printer;

pub fn client_config(cli: &Cli) -> ClientConfig {
    ClientConfig {
        api_url: cli.api_url.clone(),
        token: cli.token.clone(),
        tenant_id: cli.tenant_id.clone(),
        timeout: Duration::from_secs(cli.timeout_secs.max(1)),
        colon_routes: cli.colon_routes,
    }
}

/// Decode a list endpoint leniently: `{"items": [...]}` or a bare array.
pub fn decode_items<T: DeserializeOwned>(resp: &ApiResponse) -> Result<Vec<T>, CliError> {
    if let Ok(list) = serde_json::from_slice::<ListResponse<T>>(&resp.body) {
        return Ok(list.items);
    }
    resp.json::<Vec<T>>()
}

pub async fn dispatch(cli: Cli, p: &mut Printer<'_>) -> Result<(), CliError> {
    let cfg = client_config(&cli);
    match cli.command {
        Command::Functions { command } => {
            let client = ApiClient::new(cfg)?;
            match command {
                FunctionsCommand::Create { name, description } => {
                    functions::create(&client, &name, &description, p).await
                }
                FunctionsCommand::List => functions::list(&client, p).await,
                FunctionsCommand::Get { function } => functions::get(&client, &function, p).await,
                FunctionsCommand::Delete { function } => {
                    functions::delete(&client, &function, p).await
                }
                FunctionsCommand::Deploy(args) => {
                    deploy::deploy(&client, &args, p).await.map(|_| ())
                }
                FunctionsCommand::Invoke(args) => invoke::invoke(&client, &args, p).await,
                FunctionsCommand::Http(args) => http::http(&client, &args, p).await,
                FunctionsCommand::Invocations { function, limit } => {
                    functions::invocations(&client, &function, limit, p).await
                }
                FunctionsCommand::Invocation { invocation_id } => {
                    functions::invocation(&client, &invocation_id, p).await
                }
                FunctionsCommand::Logs(args) => logs::logs(&client, &args, p).await,
                FunctionsCommand::Revisions { function } => {
                    functions::revisions(&client, &function, p).await
                }
                FunctionsCommand::Revision {
                    function,
                    revision_id,
                } => functions::revision(&client, &function, &revision_id, p).await,
                FunctionsCommand::Aliases { function } => {
                    functions::aliases(&client, &function, p).await
                }
                FunctionsCommand::AliasSet {
                    function,
                    alias,
                    revision_id,
                    expected_generation,
                } => {
                    functions::alias_set(
                        &client,
                        &function,
                        &alias,
                        &revision_id,
                        expected_generation,
                        p,
                    )
                    .await
                }
                FunctionsCommand::Rollback {
                    function,
                    alias,
                    to,
                } => rollback::rollback(&client, &function, &alias, to.as_deref(), p).await,
                FunctionsCommand::Cancel { invocation_id } => {
                    functions::cancel(&client, &invocation_id, p).await
                }
            }
        }
        Command::Provider => {
            let client = ApiClient::new(cfg)?;
            provider::provider(&client, p).await
        }
        Command::Triggers { command } => match command {
            TriggersCommand::WebhookSign(args) => triggers::webhook_sign(&args, p),
            command => {
                let client = ApiClient::new(cfg)?;
                match command {
                    TriggersCommand::Create(args) => triggers::create(&client, &args, p).await,
                    TriggersCommand::List { function } => {
                        triggers::list(&client, &function, p).await
                    }
                    TriggersCommand::Get { function, trigger } => {
                        triggers::get(&client, &function, &trigger, p).await
                    }
                    TriggersCommand::Update(args) => triggers::update(&client, &args, p).await,
                    TriggersCommand::Delete { function, trigger } => {
                        triggers::delete(&client, &function, &trigger, p).await
                    }
                    TriggersCommand::Fires {
                        function,
                        trigger,
                        limit,
                    } => triggers::fires(&client, &function, &trigger, limit, p).await,
                    TriggersCommand::WebhookSign(args) => triggers::webhook_sign(&args, p),
                }
            }
        },
        Command::Capacity => {
            let client = ApiClient::new(cfg)?;
            provider::capacity(&client, p).await
        }
        Command::Health => {
            let client = ApiClient::new(cfg)?;
            provider::health(&client, p).await
        }
        Command::Usage(args) => {
            let client = ApiClient::new(cfg)?;
            usage::usage(&client, &args, p).await
        }
        Command::Dev(args) => dev::run(&args, &cfg, p).await,
    }
}
