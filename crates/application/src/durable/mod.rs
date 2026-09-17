//! Durable queue and object store (PLT-4638, docs/adr/0008).
//!
//! Optional components of the application. With the defaults (`[queue]
//! backend = "none"`, `[objects] backend = "none"`) nothing here is built and
//! the gateway is the synchronous-only gateway it was before. Nothing in the
//! invoke pipeline uses these yet: `invokeAsync` and the outbox are PLT-4639.

pub mod config;
pub mod crypto;
pub mod fs_objects;
pub mod gc;
pub mod sqlite_queue;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use tachyon_serverless_domain::Clock;
use tachyon_serverless_durable_port::{EventQueue, ObjectStore};

pub use config::{
    NatsQueueConfig, ObjectsBackend, ObjectsConfig, QueueBackend, QueueConfig, QueueLimitsConfig,
};
pub use crypto::ObjectKey;
pub use fs_objects::{FsObjectOptions, FsObjectStore};
pub use gc::{GcReport, ObjectGc};
pub use sqlite_queue::SqliteEventQueue;

use crate::config::GatewayConfig;
use crate::error::AppError;
use crate::repository::ObjectReferenceRepository;

/// Components the composition root cannot build on its own. The JetStream
/// adapter needs an async connect and lives in its own crate, so the gateway
/// connects it and hands it in.
#[derive(Default, Clone)]
pub struct DurableOverrides {
    pub queue: Option<Arc<dyn EventQueue>>,
}

/// What [`build`] produced.
#[derive(Default, Clone)]
pub struct DurableComponents {
    pub queue: Option<Arc<dyn EventQueue>>,
    pub objects: Option<Arc<dyn ObjectStore>>,
    pub object_gc: Option<Arc<ObjectGc>>,
}

impl std::fmt::Debug for DurableComponents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableComponents")
            .field("queue", &self.queue.as_ref().map(|q| q.backend()))
            .field("objects", &self.objects.as_ref().map(|o| o.backend()))
            .finish()
    }
}

pub fn build(
    config: &GatewayConfig,
    refs: Arc<dyn ObjectReferenceRepository>,
    clock: Arc<dyn Clock>,
    overrides: DurableOverrides,
) -> Result<DurableComponents, AppError> {
    let queue: Option<Arc<dyn EventQueue>> = match (config.queue.backend, overrides.queue) {
        (QueueBackend::None, None) => None,
        (QueueBackend::None, Some(_)) => {
            return Err(AppError::InvalidRequest(
                "a queue was supplied but [queue] backend = \"none\"".into(),
            ));
        }
        (QueueBackend::Sqlite, None) => {
            let path = config
                .queue
                .path
                .clone()
                .unwrap_or_else(|| config.data_dir.join(SqliteEventQueue::FILE_NAME));
            let q = SqliteEventQueue::open(&path, config.queue.limits.limits(), clock.clone())
                .map_err(|e| {
                    AppError::platform(format!("cannot open queue {}: {e}", path.display()))
                })?;
            Some(Arc::new(q))
        }
        (QueueBackend::Nats, Some(q)) if q.backend() == "nats" => Some(q),
        (backend, Some(q)) => {
            return Err(AppError::InvalidRequest(format!(
                "[queue] backend = \"{}\" but a `{}` queue was supplied",
                backend.as_str(),
                q.backend()
            )));
        }
        (QueueBackend::Nats, None) => {
            return Err(AppError::InvalidRequest(
                "[queue] backend = \"nats\" needs a connected JetStream queue; the gateway \
                 connects it before bootstrap (tachyon-serverless-queue-nats)"
                    .into(),
            ));
        }
    };

    let (objects, object_gc) = match config.objects.backend {
        ObjectsBackend::None => (None, None),
        ObjectsBackend::Filesystem => {
            let o = &config.objects;
            let key = match (&o.key_file, &o.key_env) {
                (Some(path), _) => ObjectKey::from_file(path),
                (None, Some(var)) => ObjectKey::from_env(var),
                (None, None) => {
                    return Err(AppError::InvalidRequest(
                        "[objects] needs key_file or key_env".into(),
                    ));
                }
            }
            .map_err(|e| AppError::InvalidRequest(format!("[objects]: {e}")))?;
            let regions = o
                .parsed_regions()
                .map_err(|e| AppError::InvalidRequest(format!("[objects] regions: {e}")))?;
            let root = o
                .root
                .clone()
                .unwrap_or_else(|| config.data_dir.join("objects"));
            let store: Arc<dyn ObjectStore> = Arc::new(
                FsObjectStore::open(
                    &root,
                    key,
                    FsObjectOptions {
                        regions,
                        max_object_bytes: o.max_object_bytes,
                        tenant_quota_bytes: o.tenant_quota_bytes,
                        default_ttl: o.default_ttl(),
                    },
                    clock.clone(),
                )
                .map_err(|e| {
                    AppError::platform(format!("cannot open object store {}: {e}", root.display()))
                })?,
            );
            let gc = Arc::new(ObjectGc::new(store.clone(), refs, clock, o.orphan_grace()));
            (Some(store), Some(gc))
        }
    };
    Ok(DurableComponents {
        queue,
        objects,
        object_gc,
    })
}
