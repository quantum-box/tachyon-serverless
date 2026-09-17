//! NATS JetStream [`EventQueue`] (PLT-4638, docs/adr/0008).
//!
//! **Stream as code.** [`NatsEventQueue::connect`] creates or updates one
//! stream with exactly these settings, every time it starts:
//!
//! | setting | value | why |
//! |---|---|---|
//! | `storage` | `file` | messages survive a server restart |
//! | `retention` | `workqueue` | an ack (or term) removes the message; one consumer per subject |
//! | `discard` | `new` | a full stream refuses the publish (`queue_full`) instead of evicting old messages |
//! | `max_msgs` / `max_bytes` / `max_msg_size` / `max_age` | `[queue.limits]` | bounded capacity and age |
//! | `duplicate_window` | `[queue.limits]` | `Nats-Msg-Id` dedup of publisher retries |
//! | `num_replicas` | `1` | **single node, no replication** (ADR-0008 「保存先と複製」) |
//! | `subjects` | `<prefix>.>` | subject `<prefix>.<tenant>.<topic>` |
//!
//! Consumers are durable pull consumers with `ack_policy = explicit`,
//! `deliver_policy = all`, the consumer's `ack_wait` and `max_deliver`, and a
//! filter `<prefix>.*.<topic>`.
//!
//! **Authentication.** There is no anonymous mode: [`NatsCredentials`] is
//! either a user + password or an nkey seed, both read from files that must
//! not be readable by group or others. The server side (`deploy/nats/`)
//! defines no `no_auth_user`, so an unauthenticated client is refused by the
//! server as well.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use async_nats::jetstream::{self, consumer, stream};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;

use tachyon_serverless_domain::TenantId;
use tachyon_serverless_durable_port::{
    BacklogStats, ConsumerName, ConsumerSpec, Delivery, DeliveryToken, EventQueue, MessageId,
    OutgoingMessage, PublishReceipt, QueueError, QueueLimits, Topic,
};

const MSG_ID_HEADER: &str = "Nats-Msg-Id";

/// How the adapter authenticates. `Debug` never shows a secret.
#[derive(Clone)]
pub enum NatsCredentials {
    UserPassword { user: String, password: String },
    NkeySeed(String),
}

impl std::fmt::Debug for NatsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UserPassword { user, .. } => f
                .debug_struct("UserPassword")
                .field("user", user)
                .field("password", &"<redacted>")
                .finish(),
            Self::NkeySeed(_) => f.write_str("NkeySeed(<redacted>)"),
        }
    }
}

fn read_secret_file(path: &Path) -> Result<String, QueueError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|e| QueueError::Unauthorized(format!("{}: {e}", path.display())))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(QueueError::Unauthorized(format!(
                "{} is readable by group or others; restrict it to the owner (chmod 600)",
                path.display()
            )));
        }
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| QueueError::Unauthorized(format!("{}: {e}", path.display())))?;
    let secret = text.trim().to_string();
    if secret.is_empty() {
        return Err(QueueError::Unauthorized(format!(
            "{} is empty",
            path.display()
        )));
    }
    Ok(secret)
}

impl NatsCredentials {
    pub fn user_password_file(user: &str, password_file: &Path) -> Result<Self, QueueError> {
        if user.is_empty() {
            return Err(QueueError::Unauthorized("empty NATS user".into()));
        }
        Ok(Self::UserPassword {
            user: user.to_string(),
            password: read_secret_file(password_file)?,
        })
    }

    pub fn nkey_seed_file(path: &Path) -> Result<Self, QueueError> {
        Ok(Self::NkeySeed(read_secret_file(path)?))
    }
}

#[derive(Debug, Clone)]
pub struct NatsQueueOptions {
    pub url: String,
    pub stream: String,
    pub subject_prefix: String,
    pub credentials: NatsCredentials,
    pub limits: QueueLimits,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

pub struct NatsEventQueue {
    client: async_nats::Client,
    js: jetstream::Context,
    options: NatsQueueOptions,
    consumers: Mutex<HashMap<String, consumer::PullConsumer>>,
}

impl std::fmt::Debug for NatsEventQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsEventQueue")
            .field("url", &self.options.url)
            .field("stream", &self.options.stream)
            .finish_non_exhaustive()
    }
}

/// The JetStream API error code carried by an async-nats error. The client
/// keeps the API error in the error *kind* (not the source chain) and prints
/// it as `... (code 404, error code 10059)`, so both are looked at.
fn api_code(e: &(dyn std::error::Error + 'static)) -> Option<u64> {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        if let Some(api) = err.downcast_ref::<jetstream::Error>() {
            return Some(api.error_code().0);
        }
        cur = err.source();
    }
    let text = e.to_string();
    let (_, rest) = text.rsplit_once("error code ")?;
    rest.trim_end_matches(')').trim().parse().ok()
}

fn unavailable(e: impl std::fmt::Display) -> QueueError {
    QueueError::Unavailable(e.to_string())
}

fn classify(what: &str, e: &(dyn std::error::Error + 'static)) -> QueueError {
    use jetstream::ErrorCode as C;
    let text = e.to_string();
    if let Some(code) = api_code(e).map(jetstream::ErrorCode) {
        if code == C::STREAM_STORE_FAILED
            || code == C::STORAGE_RESOURCES_EXCEEDED
            || code == C::ACCOUNT_RESOURCES_EXCEEDED
            || code == C::INSUFFICIENT_RESOURCES
            || code == C::STREAM_MESSAGE_EXCEEDS_MAXIMUM
        {
            return QueueError::QueueFull(format!("{what}: {text}"));
        }
        if code == C::CONSUMER_NOT_FOUND {
            return QueueError::ConsumerNotFound(text);
        }
        if code == C::CONSUMER_WQ_CONSUMER_NOT_UNIQUE || code == C::CONSUMER_ALREADY_EXISTS {
            return QueueError::ConsumerConflict(text);
        }
        return QueueError::Backend(format!("{what}: {text}"));
    }
    if text.contains("maximum messages") || text.contains("maximum bytes") {
        return QueueError::QueueFull(format!("{what}: {text}"));
    }
    if text.contains("timed out") || text.contains("broken pipe") || text.contains("no responders")
    {
        return QueueError::Unavailable(format!("{what}: {text}"));
    }
    QueueError::Backend(format!("{what}: {text}"))
}

impl NatsEventQueue {
    /// Connect, authenticate and apply the stream configuration.
    pub async fn connect(options: NatsQueueOptions) -> Result<Self, QueueError> {
        options
            .limits
            .validate()
            .map_err(QueueError::InvalidMessage)?;
        let connect = match &options.credentials {
            NatsCredentials::UserPassword { user, password } => {
                async_nats::ConnectOptions::with_user_and_password(user.clone(), password.clone())
            }
            NatsCredentials::NkeySeed(seed) => async_nats::ConnectOptions::with_nkey(seed.clone()),
        }
        .name("tachyon-serverless-gateway")
        .connection_timeout(options.connect_timeout)
        .request_timeout(Some(options.request_timeout));
        let client = connect.connect(options.url.as_str()).await.map_err(|e| {
            use async_nats::ConnectErrorKind as K;
            match e.kind() {
                K::Authentication | K::AuthorizationViolation => {
                    QueueError::Unauthorized(e.to_string())
                }
                _ => QueueError::Unavailable(e.to_string()),
            }
        })?;
        let mut js = jetstream::new(client.clone());
        js.set_timeout(options.request_timeout);
        let queue = Self {
            client,
            js,
            options,
            consumers: Mutex::new(HashMap::new()),
        };
        queue.bootstrap_stream().await?;
        Ok(queue)
    }

    /// The stream configuration this adapter enforces (see module docs).
    pub fn stream_config(options: &NatsQueueOptions) -> stream::Config {
        let l = options.limits;
        stream::Config {
            name: options.stream.clone(),
            subjects: vec![format!("{}.>", options.subject_prefix)],
            storage: stream::StorageType::File,
            retention: stream::RetentionPolicy::WorkQueue,
            discard: stream::DiscardPolicy::New,
            max_messages: i64::try_from(l.max_messages).unwrap_or(i64::MAX),
            max_bytes: i64::try_from(l.max_bytes).unwrap_or(i64::MAX),
            max_message_size: i32::try_from(l.max_message_bytes).unwrap_or(i32::MAX),
            max_age: l.max_age,
            duplicate_window: l.duplicate_window,
            num_replicas: 1,
            description: Some(
                "tachyon-serverless durable events (PLT-4638). Managed by the gateway: edits are overwritten at startup."
                    .into(),
            ),
            ..Default::default()
        }
    }

    async fn bootstrap_stream(&self) -> Result<(), QueueError> {
        let config = Self::stream_config(&self.options);
        match self.js.get_stream(&self.options.stream).await {
            Ok(_) => {
                self.js
                    .update_stream(config)
                    .await
                    .map_err(|e| classify("update stream", &e))?;
            }
            Err(e) if api_code(&e) == Some(jetstream::ErrorCode::STREAM_NOT_FOUND.0) => {
                self.js
                    .create_stream(config)
                    .await
                    .map_err(|e| classify("create stream", &e))?;
            }
            Err(e) => return Err(classify("get stream", &e)),
        }
        Ok(())
    }

    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }

    fn subject(&self, tenant: &TenantId, topic: &Topic) -> String {
        format!("{}.{}.{}", self.options.subject_prefix, tenant, topic)
    }

    fn filter(&self, topic: &Topic) -> String {
        format!("{}.*.{}", self.options.subject_prefix, topic)
    }

    async fn stream(&self) -> Result<stream::Stream, QueueError> {
        self.js
            .get_stream(&self.options.stream)
            .await
            .map_err(|e| classify("get stream", &e))
    }

    async fn consumer(&self, name: &ConsumerName) -> Result<consumer::PullConsumer, QueueError> {
        if let Some(c) = self.consumers.lock().get(name.as_str()) {
            return Ok(c.clone());
        }
        let stream = self.stream().await?;
        let c: consumer::PullConsumer = stream.get_consumer(name.as_str()).await.map_err(|e| {
            match classify("get consumer", &*e) {
                QueueError::Backend(t) if t.contains("not found") => {
                    QueueError::ConsumerNotFound(name.to_string())
                }
                other => other,
            }
        })?;
        self.consumers
            .lock()
            .insert(name.as_str().to_string(), c.clone());
        Ok(c)
    }

    fn to_delivery(&self, m: jetstream::Message) -> Result<Delivery, QueueError> {
        let info = m
            .info()
            .map_err(|e| QueueError::Backend(format!("message info: {e}")))?;
        let (sequence, delivered, published) =
            (info.stream_sequence, info.delivered, info.published);
        let mut parts = m.subject.as_str().rsplitn(3, '.');
        let topic = parts.next().unwrap_or_default();
        let tenant = parts.next().unwrap_or_default();
        let message_id = m
            .headers
            .as_ref()
            .and_then(|h| h.get(MSG_ID_HEADER))
            .map(|v| v.as_str().to_string())
            .unwrap_or_default();
        let reply = m
            .reply
            .as_ref()
            .ok_or_else(|| QueueError::Backend("message without ack subject".into()))?
            .to_string();
        let published_at =
            chrono::DateTime::from_timestamp(published.unix_timestamp(), published.nanosecond())
                .unwrap_or_default();
        Ok(Delivery {
            sequence,
            tenant_id: TenantId::parse(tenant)
                .map_err(|e| QueueError::Backend(format!("subject tenant: {e}")))?,
            topic: Topic::parse(topic)?,
            message_id: MessageId::parse(&message_id)?,
            payload: m.payload.to_vec(),
            delivery_count: u32::try_from(delivered).unwrap_or(u32::MAX),
            published_at,
            token: DeliveryToken::new(reply),
        })
    }

    fn check_token(token: &DeliveryToken) -> Result<String, QueueError> {
        let t = token.as_str();
        if !t.starts_with("$JS.ACK.") || t.contains(char::is_whitespace) {
            return Err(QueueError::InvalidMessage(
                "malformed delivery token".into(),
            ));
        }
        Ok(t.to_string())
    }

    /// Send an ack kind and wait for the server to confirm it.
    async fn confirm(&self, token: &DeliveryToken, body: Bytes) -> Result<(), QueueError> {
        let subject = Self::check_token(token)?;
        match tokio::time::timeout(
            self.options.request_timeout,
            self.client.request(subject, body),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(unavailable(format!("ack not confirmed: {e}"))),
            Err(_) => Err(unavailable("ack not confirmed: timed out")),
        }
    }
}

#[async_trait]
impl EventQueue for NatsEventQueue {
    fn backend(&self) -> &'static str {
        "nats"
    }

    fn limits(&self) -> QueueLimits {
        self.options.limits
    }

    async fn ensure_consumer(&self, spec: &ConsumerSpec) -> Result<(), QueueError> {
        if spec.max_deliver == 0 || spec.ack_wait.is_zero() {
            return Err(QueueError::InvalidMessage(
                "a consumer needs max_deliver >= 1 and ack_wait > 0".into(),
            ));
        }
        let stream = self.stream().await?;
        let config = consumer::pull::Config {
            durable_name: Some(spec.name.as_str().to_string()),
            description: Some("tachyon-serverless (PLT-4638)".into()),
            deliver_policy: consumer::DeliverPolicy::All,
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: spec.ack_wait,
            max_deliver: i64::from(spec.max_deliver),
            filter_subject: self.filter(&spec.topic),
            ..Default::default()
        };
        match stream.consumer_info(spec.name.as_str()).await {
            Ok(info) => {
                if info.config.filter_subject != config.filter_subject {
                    return Err(QueueError::ConsumerConflict(format!(
                        "consumer `{}` is bound to `{}`",
                        spec.name, info.config.filter_subject
                    )));
                }
                stream
                    .update_consumer(config)
                    .await
                    .map_err(|e| classify("update consumer", &e))?;
            }
            Err(_) => {
                stream
                    .create_consumer(config)
                    .await
                    .map_err(|e| classify("create consumer", &e))?;
            }
        }
        self.consumers.lock().remove(spec.name.as_str());
        Ok(())
    }

    async fn publish(&self, message: OutgoingMessage) -> Result<PublishReceipt, QueueError> {
        let size = message.payload.len() as u64;
        let max = u64::from(self.options.limits.max_message_bytes);
        if size > max {
            return Err(QueueError::MessageTooLarge { size, max });
        }
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(MSG_ID_HEADER, message.message_id.as_str());
        let ack = self
            .js
            .publish_with_headers(
                self.subject(&message.tenant_id, &message.topic),
                headers,
                Bytes::from(message.payload),
            )
            .await
            .map_err(|e| classify("publish", &e))?
            .await
            .map_err(|e| classify("publish", &e))?;
        Ok(PublishReceipt {
            sequence: ack.sequence,
            duplicate: ack.duplicate,
        })
    }

    async fn fetch(
        &self,
        consumer: &ConsumerName,
        max: usize,
        wait: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
        let c = self.consumer(consumer).await?;
        let max = max.clamp(1, 10_000);
        let mut out = Vec::new();
        // What is available now, without waiting.
        let mut batch = c
            .fetch()
            .max_messages(max)
            .messages()
            .await
            .map_err(|e| classify("fetch", &e))?;
        while let Some(m) = batch.next().await {
            let m = m.map_err(|e| classify("fetch", &*e))?;
            out.push(self.to_delivery(m)?);
        }
        if out.is_empty() && !wait.is_zero() {
            // Nothing yet: wait up to `wait` for the first one.
            let expires = wait.max(Duration::from_millis(100));
            let mut batch = c
                .batch()
                .max_messages(1)
                .expires(expires)
                .messages()
                .await
                .map_err(|e| classify("fetch", &e))?;
            while let Some(m) = batch.next().await {
                let m = m.map_err(|e| classify("fetch", &*e))?;
                out.push(self.to_delivery(m)?);
            }
            if !out.is_empty() && max > 1 {
                let mut rest = c
                    .fetch()
                    .max_messages(max - 1)
                    .messages()
                    .await
                    .map_err(|e| classify("fetch", &e))?;
                while let Some(m) = rest.next().await {
                    let m = m.map_err(|e| classify("fetch", &*e))?;
                    out.push(self.to_delivery(m)?);
                }
            }
        }
        Ok(out)
    }

    async fn ack(&self, token: &DeliveryToken) -> Result<(), QueueError> {
        self.confirm(token, jetstream::AckKind::Ack.into()).await
    }

    async fn nak(&self, token: &DeliveryToken, delay: Option<Duration>) -> Result<(), QueueError> {
        self.confirm(token, jetstream::AckKind::Nak(delay).into())
            .await
    }

    async fn term(&self, token: &DeliveryToken) -> Result<(), QueueError> {
        self.confirm(token, jetstream::AckKind::Term.into()).await
    }

    async fn stats(&self, consumer: &ConsumerName) -> Result<BacklogStats, QueueError> {
        let stream = self.stream().await?;
        let info = stream
            .get_info()
            .await
            .map_err(|e| classify("stream info", &e))?;
        let c = self.consumer(consumer).await?;
        let ci = c
            .get_info()
            .await
            .map_err(|e| classify("consumer info", &e))?;
        Ok(BacklogStats {
            stream_messages: info.state.messages,
            stream_bytes: info.state.bytes,
            pending: ci.num_pending,
            ack_pending: ci.num_ack_pending as u64,
            redelivered: ci.num_redelivered as u64,
        })
    }
}

#[cfg(test)]
mod tests;
