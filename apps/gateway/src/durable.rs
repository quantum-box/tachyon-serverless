//! Durable components the application crate cannot build itself (PLT-4638):
//! the JetStream queue needs an async connect.

use std::sync::Arc;
use std::time::Duration;

use tachyon_serverless_application::durable::QueueBackend;
use tachyon_serverless_application::{AppError, DurableOverrides, GatewayConfig};
use tachyon_serverless_queue_nats::{NatsCredentials, NatsEventQueue, NatsQueueOptions};

/// Connect `[queue.nats]` when it is selected. Refuses to start on missing or
/// rejected credentials rather than running without the queue.
pub async fn connect(config: &GatewayConfig) -> Result<DurableOverrides, AppError> {
    if config.queue.backend != QueueBackend::Nats {
        return Ok(DurableOverrides::default());
    }
    let nats = config
        .queue
        .nats
        .as_ref()
        .ok_or_else(|| AppError::InvalidRequest("[queue.nats] is missing".into()))?;
    let credentials = match (&nats.user, &nats.password_file, &nats.nkey_seed_file) {
        (Some(user), Some(password_file), None) => {
            NatsCredentials::user_password_file(user, password_file)
        }
        (None, None, Some(seed)) => NatsCredentials::nkey_seed_file(seed),
        _ => {
            return Err(AppError::InvalidRequest(
                "[queue.nats] needs user + password_file or nkey_seed_file".into(),
            ));
        }
    }
    .map_err(|e| AppError::InvalidRequest(format!("[queue.nats]: {e}")))?;
    let queue = NatsEventQueue::connect(NatsQueueOptions {
        url: nats.url.clone(),
        stream: nats.stream.clone(),
        subject_prefix: nats.subject_prefix.clone(),
        credentials,
        limits: config.queue.limits.limits(),
        connect_timeout: Duration::from_millis(nats.connect_timeout_ms),
        request_timeout: Duration::from_millis(nats.request_timeout_ms),
    })
    .await
    .map_err(|e| AppError::platform(format!("cannot connect the JetStream queue: {e}")))?;
    tracing::info!(url = %nats.url, stream = %nats.stream, "JetStream queue connected and stream bootstrapped");
    Ok(DurableOverrides {
        queue: Some(Arc::new(queue)),
    })
}
