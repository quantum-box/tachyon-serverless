//! `tachyon-queue-probe`: drive the JetStream adapter from a shell
//! (`scripts/queue/verify.sh`). Output is `key=value` lines.
//!
//! ```text
//! tachyon-queue-probe --stream S --subject-prefix p publish --count 100 --id-prefix run1
//! tachyon-queue-probe ... consume --max 10 --wait-ms 2000 [--no-ack]
//! tachyon-queue-probe ... stats
//! tachyon-queue-probe --url nats://127.0.0.1:14222 anonymous
//! ```

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};

use tachyon_serverless_domain::TenantId;
use tachyon_serverless_durable_port::{
    ConsumerName, ConsumerSpec, EventQueue, MessageId, OutgoingMessage, QueueError, QueueLimits,
    Topic,
};
use tachyon_serverless_queue_nats::{NatsCredentials, NatsEventQueue, NatsQueueOptions};

#[derive(Debug, Parser)]
#[command(name = "tachyon-queue-probe")]
struct Args {
    #[arg(
        long,
        env = "TACHYON_NATS_URL",
        default_value = "nats://127.0.0.1:14222"
    )]
    url: String,
    #[arg(long, env = "TACHYON_NATS_USER", default_value = "gateway")]
    user: String,
    #[arg(long, env = "TACHYON_NATS_PASSWORD_FILE")]
    password_file: Option<PathBuf>,
    #[arg(long, default_value = "TACHYON_EVENTS")]
    stream: String,
    #[arg(long, default_value = "tachyon.events")]
    subject_prefix: String,
    #[arg(long, default_value_t = 100_000)]
    max_messages: u64,
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    max_bytes: u64,
    #[arg(long, default_value_t = 256 * 1024)]
    max_message_bytes: u32,
    #[arg(long, default_value_t = 7 * 24 * 3600)]
    max_age_secs: u64,
    #[arg(long, default_value_t = 120)]
    duplicate_window_secs: u64,
    #[arg(long, default_value = "dispatcher")]
    consumer: String,
    #[arg(long, default_value = "invoke")]
    topic: String,
    #[arg(long, default_value_t = 2_000)]
    ack_wait_ms: u64,
    #[arg(long, default_value_t = 5)]
    max_deliver: u32,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Publish `count` messages with ids `<id-prefix>-<n>`.
    Publish {
        #[arg(long)]
        count: u64,
        #[arg(long)]
        id_prefix: String,
        #[arg(long, default_value_t = 32)]
        payload_bytes: usize,
    },
    /// Fetch until `max` deliveries or `wait-ms` passes without one.
    Consume {
        #[arg(long)]
        max: usize,
        #[arg(long, default_value_t = 2_000)]
        wait_ms: u64,
        #[arg(long)]
        no_ack: bool,
    },
    Stats,
    /// Try to connect without credentials; exit 0 when the server refuses.
    Anonymous,
}

fn tenant() -> TenantId {
    TenantId::parse("tn_01hzzzzzzzzzzzzzzzzzzzzzza").unwrap()
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(code) => code,
        Err(e) => {
            println!("error_code={}", e.code());
            println!("error={e}");
            ExitCode::from(1)
        }
    }
}

async fn run(args: Args) -> Result<ExitCode, QueueError> {
    if let Command::Anonymous = args.command {
        let r = async_nats::ConnectOptions::new()
            .connection_timeout(Duration::from_secs(3))
            .connect(args.url.as_str())
            .await;
        return Ok(match r {
            Ok(_) => {
                println!("anonymous=accepted");
                ExitCode::from(1)
            }
            Err(e) => {
                println!("anonymous=refused");
                println!("reason={e}");
                ExitCode::SUCCESS
            }
        });
    }
    let password_file = args
        .password_file
        .clone()
        .ok_or_else(|| QueueError::Unauthorized("--password-file is required".into()))?;
    let q = NatsEventQueue::connect(NatsQueueOptions {
        url: args.url.clone(),
        stream: args.stream.clone(),
        subject_prefix: args.subject_prefix.clone(),
        credentials: NatsCredentials::user_password_file(&args.user, &password_file)?,
        limits: QueueLimits {
            max_messages: args.max_messages,
            max_bytes: args.max_bytes,
            max_message_bytes: args.max_message_bytes,
            max_age: Duration::from_secs(args.max_age_secs),
            duplicate_window: Duration::from_secs(args.duplicate_window_secs),
        },
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
    })
    .await?;
    let topic = Topic::parse(&args.topic)?;
    let consumer = ConsumerName::parse(&args.consumer)?;
    q.ensure_consumer(&ConsumerSpec {
        name: consumer.clone(),
        topic: topic.clone(),
        ack_wait: Duration::from_millis(args.ack_wait_ms),
        max_deliver: args.max_deliver,
    })
    .await?;
    match args.command {
        Command::Publish {
            count,
            id_prefix,
            payload_bytes,
        } => {
            let (mut published, mut duplicates, mut full, mut other) = (0u64, 0u64, 0u64, 0u64);
            let mut first_error = String::new();
            for n in 0..count {
                let r = q
                    .publish(OutgoingMessage {
                        tenant_id: tenant(),
                        topic: topic.clone(),
                        message_id: MessageId::parse(&format!("{id_prefix}-{n}"))?,
                        payload: vec![b'x'; payload_bytes],
                    })
                    .await;
                match r {
                    Ok(receipt) if receipt.duplicate => duplicates += 1,
                    Ok(_) => published += 1,
                    Err(e) => {
                        if matches!(e, QueueError::QueueFull(_)) {
                            full += 1;
                        } else {
                            other += 1;
                        }
                        if first_error.is_empty() {
                            first_error = format!("{}: {e}", e.code());
                        }
                    }
                }
            }
            println!("published={published}");
            println!("duplicates={duplicates}");
            println!("queue_full={full}");
            println!("other_errors={other}");
            println!("first_error={first_error}");
        }
        Command::Consume {
            max,
            wait_ms,
            no_ack,
        } => {
            let mut received = 0usize;
            let mut redelivered = 0usize;
            let mut ids = Vec::new();
            while received < max {
                let batch = q
                    .fetch(&consumer, max - received, Duration::from_millis(wait_ms))
                    .await?;
                if batch.is_empty() {
                    break;
                }
                for d in batch {
                    received += 1;
                    if d.delivery_count > 1 {
                        redelivered += 1;
                    }
                    ids.push(d.message_id.to_string());
                    if !no_ack {
                        q.ack(&d.token).await?;
                    }
                }
            }
            ids.sort();
            ids.dedup();
            println!("received={received}");
            println!("unique={}", ids.len());
            println!("redelivered={redelivered}");
        }
        Command::Stats => {
            let s = q.stats(&consumer).await?;
            println!("stream_messages={}", s.stream_messages);
            println!("stream_bytes={}", s.stream_bytes);
            println!("pending={}", s.pending);
            println!("ack_pending={}", s.ack_pending);
            println!("redelivered={}", s.redelivered);
        }
        Command::Anonymous => unreachable!(),
    }
    Ok(ExitCode::SUCCESS)
}
