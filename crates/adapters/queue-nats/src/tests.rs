//! JetStream contract tests. They need a running nats-server with JetStream
//! and a user; `scripts/queue/up.sh` starts one and prints the environment:
//!
//! ```text
//! TACHYON_NATS_URL=nats://127.0.0.1:14222
//! TACHYON_NATS_USER=gateway
//! TACHYON_NATS_PASSWORD_FILE=.queue/nats/gateway.password
//! ```
//!
//! Without `TACHYON_NATS_URL` every test here prints why and returns
//! (hosted CI without the server). With `TACHYON_NATS_REQUIRED=1` a missing
//! server is a failure instead (`scripts/queue/verify.sh` and the CI job).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tachyon_serverless_durable_port::testkit::{self, QueueHarness};

use super::*;

struct Env {
    url: String,
    credentials: NatsCredentials,
}

fn env() -> Option<Env> {
    let Ok(url) = std::env::var("TACHYON_NATS_URL") else {
        assert!(
            std::env::var("TACHYON_NATS_REQUIRED").as_deref() != Ok("1"),
            "TACHYON_NATS_REQUIRED=1 but TACHYON_NATS_URL is not set"
        );
        eprintln!(
            "SKIPPED: no NATS server (set TACHYON_NATS_URL / TACHYON_NATS_USER / \
             TACHYON_NATS_PASSWORD_FILE, e.g. from scripts/queue/up.sh)"
        );
        return None;
    };
    let user = std::env::var("TACHYON_NATS_USER").expect("TACHYON_NATS_USER");
    let password_file =
        std::env::var("TACHYON_NATS_PASSWORD_FILE").expect("TACHYON_NATS_PASSWORD_FILE");
    Some(Env {
        url,
        credentials: NatsCredentials::user_password_file(&user, Path::new(&password_file))
            .expect("credentials"),
    })
}

fn options(env: &Env, limits: QueueLimits) -> NatsQueueOptions {
    let unique = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    NatsQueueOptions {
        url: env.url.clone(),
        stream: format!("TACHYON_TEST_{unique}"),
        subject_prefix: format!("tachyon-test.{unique}"),
        credentials: env.credentials.clone(),
        limits,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
    }
}

struct Harness {
    env: Env,
    opened: Mutex<Vec<(usize, NatsQueueOptions)>>,
}

fn key(q: &Arc<dyn EventQueue>) -> usize {
    Arc::as_ptr(q) as *const () as usize
}

#[async_trait]
impl QueueHarness for Harness {
    async fn open(&self, limits: QueueLimits) -> Arc<dyn EventQueue> {
        let o = options(&self.env, limits);
        let q: Arc<dyn EventQueue> = Arc::new(NatsEventQueue::connect(o.clone()).await.unwrap());
        self.opened.lock().push((key(&q), o));
        q
    }

    async fn reopen(&self, queue: Arc<dyn EventQueue>) -> Arc<dyn EventQueue> {
        let k = key(&queue);
        let o = self
            .opened
            .lock()
            .iter()
            .rev()
            .find(|(p, _)| *p == k)
            .map(|(_, o)| o.clone())
            .expect("opened by this harness");
        drop(queue);
        let q: Arc<dyn EventQueue> = Arc::new(NatsEventQueue::connect(o.clone()).await.unwrap());
        self.opened.lock().push((key(&q), o));
        q
    }
}

/// PLT-4638: the JetStream adapter passes the same contract as the SQLite
/// queue (durable publish, dedup, discard-new capacity, ack / nak / term,
/// max_deliver, max_age, reopen).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nats_queue_passes_the_queue_contract() {
    let Some(env) = env() else { return };
    let h = Harness {
        env,
        opened: Mutex::new(Vec::new()),
    };
    testkit::run_queue_contract(&h).await;
}

/// The stream is bootstrapped as code, and re-applied idempotently: file
/// storage, work queue, discard new, one replica, the configured limits.
#[tokio::test]
async fn nats_stream_is_bootstrapped_as_code_and_idempotent() {
    let Some(env) = env() else { return };
    let limits = QueueLimits {
        max_messages: 42,
        ..testkit::small_limits()
    };
    let o = options(&env, limits);
    let q = NatsEventQueue::connect(o.clone()).await.unwrap();
    let again = NatsEventQueue::connect(o.clone()).await.unwrap();
    let info = again.stream().await.unwrap().get_info().await.unwrap();
    assert_eq!(info.config.storage, stream::StorageType::File);
    assert_eq!(info.config.retention, stream::RetentionPolicy::WorkQueue);
    assert_eq!(info.config.discard, stream::DiscardPolicy::New);
    assert_eq!(info.config.num_replicas, 1);
    assert_eq!(info.config.max_messages, 42);
    assert_eq!(info.config.max_age, limits.max_age);
    assert_eq!(info.config.duplicate_window, limits.duplicate_window);
    // a changed limit is applied on the next start
    let bigger = NatsQueueOptions {
        limits: QueueLimits {
            max_messages: 43,
            ..limits
        },
        ..o
    };
    let updated = NatsEventQueue::connect(bigger).await.unwrap();
    let info = updated.stream().await.unwrap().get_info().await.unwrap();
    assert_eq!(info.config.max_messages, 43);
    drop(q);
}

/// PLT-4638 acceptance: unauthenticated and wrongly authenticated clients
/// are refused by the server.
#[tokio::test]
async fn nats_refuses_unauthenticated_and_wrong_credentials() {
    let Some(env) = env() else { return };
    let anonymous = async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_secs(3))
        .connect(env.url.as_str())
        .await;
    assert!(anonymous.is_err(), "anonymous connect must be refused");

    let dir = tempfile::tempdir().unwrap();
    let wrong = dir.path().join("wrong.password");
    std::fs::write(&wrong, "not-the-password").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrong, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let user = match &env.credentials {
        NatsCredentials::UserPassword { user, .. } => user.clone(),
        NatsCredentials::NkeySeed(_) => "gateway".into(),
    };
    let err = NatsEventQueue::connect(NatsQueueOptions {
        credentials: NatsCredentials::user_password_file(&user, &wrong).unwrap(),
        ..options(&env, testkit::small_limits())
    })
    .await
    .unwrap_err();
    assert!(matches!(err, QueueError::Unauthorized(_)), "{err}");
}

#[test]
fn credential_files_must_be_private_and_are_redacted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pw");
    std::fs::write(&path, "hunter2\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            NatsCredentials::user_password_file("gateway", &path),
            Err(QueueError::Unauthorized(_))
        ));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let c = NatsCredentials::user_password_file("gateway", &path).unwrap();
    let dbg = format!("{c:?}");
    assert!(!dbg.contains("hunter2") && dbg.contains("gateway"), "{dbg}");
    let NatsCredentials::UserPassword { password, .. } = c else {
        panic!()
    };
    assert_eq!(password, "hunter2");
    std::fs::write(&path, "\n").unwrap();
    assert!(NatsCredentials::user_password_file("gateway", &path).is_err());
    assert!(NatsCredentials::user_password_file("", &path).is_err());
}

#[test]
fn stream_config_is_file_workqueue_discard_new_single_replica() {
    let o = NatsQueueOptions {
        url: "nats://127.0.0.1:1".into(),
        stream: "TACHYON_EVENTS".into(),
        subject_prefix: "tachyon.events".into(),
        credentials: NatsCredentials::NkeySeed("x".into()),
        limits: QueueLimits::default(),
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
    };
    let c = NatsEventQueue::stream_config(&o);
    assert_eq!(c.storage, stream::StorageType::File);
    assert_eq!(c.retention, stream::RetentionPolicy::WorkQueue);
    assert_eq!(c.discard, stream::DiscardPolicy::New);
    assert_eq!(c.num_replicas, 1);
    assert_eq!(c.subjects, vec!["tachyon.events.>".to_string()]);
    assert_eq!(c.max_age, QueueLimits::default().max_age);
}
