//! Bounded waits for the embedded SQLite stores (PLT-4646).
//!
//! Every SQLite store of the gateway serializes its callers on one
//! connection behind a mutex. The caller that holds the mutex may sit in
//! SQLite's `busy_timeout` while another process holds the database write
//! lock; a plain `lock()` would then queue every other caller behind it, one
//! busy timeout after the other, so the n-th caller waited n × busy timeout
//! instead of being refused. Stores take their connection with
//! [`lock_connection`], which gives up after [`STORE_WAIT`], and map `None`
//! to their own "unavailable" refusal. One store call is therefore bounded
//! by `STORE_WAIT` + the store's busy timeout, whatever the number of
//! concurrent callers.

use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};

/// How long a caller waits for a store's connection mutex, and the default
/// SQLite `busy_timeout` of the stores.
pub const STORE_WAIT: Duration = Duration::from_secs(5);

/// The store's connection, or `None` when another caller kept it for longer
/// than [`STORE_WAIT`] (the store is unavailable right now).
pub fn lock_connection<T>(conn: &Mutex<T>) -> Option<MutexGuard<'_, T>> {
    conn.try_lock_for(STORE_WAIT)
}

/// The message every store reports when [`lock_connection`] gives up.
pub fn busy_message(store: &str) -> String {
    format!(
        "the {store} connection stayed busy for {} s (database locked?)",
        STORE_WAIT.as_secs()
    )
}

/// Whether SQLite refused because another connection holds the lock (the
/// busy timeout ran out): retryable, the store is unavailable, not broken.
pub fn is_busy(e: &rusqlite::Error) -> bool {
    matches!(
        e.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy) | Some(rusqlite::ErrorCode::DatabaseLocked)
    )
}
