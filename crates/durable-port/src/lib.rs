//! Durable ports (PLT-4638, docs/adr/0008-durable-queue-and-object-store.md).
//!
//! - [`EventQueue`]: at-least-once delivery of asynchronous events with a
//!   publisher-supplied message id (dedup window), pull consumers with
//!   explicit ack / nak / term, a bounded number of deliveries, an ack wait,
//!   and hard capacity limits that refuse a publish instead of dropping old
//!   messages.
//! - [`ObjectStore`]: large inputs and outputs kept outside the ledger, under
//!   keys scoped to one tenant and one region, encrypted at rest, verified
//!   against a SHA-256 digest of the plaintext on every read, with a TTL.
//!
//! Responsibility split (ADR-0008): the queue only *delivers*. Whether an
//! invocation ran, finished or must not run again is decided by the ledger
//! (`state.db`), never by an ack. A queue implementation may deliver the same
//! message more than once; consumers settle through the ledger's CAS.
//!
//! These ports live in their own crate rather than in `provider-port`
//! because they are control-plane storage, not the execution-provider /
//! guest surface that `provider-port` changes gate on KVM (docs/ci.md §3).

pub mod object;
pub mod queue;

#[cfg(feature = "testkit")]
pub mod testkit;

pub use object::*;
pub use queue::*;
