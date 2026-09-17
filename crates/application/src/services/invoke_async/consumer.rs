//! Reading a delivered asynchronous invoke event back through the ledger
//! (PLT-4639). The dispatcher of PLT-4640 builds on this; tests and the
//! E2E consumer use it to prove that what was delivered is what was accepted.
//!
//! A delivery is only a pointer. Everything that matters is checked against
//! the ledger: the invocation exists, belongs to the tenant the message was
//! routed under, names the revision fixed at acceptance, and its input (inline
//! or in the object store, read under the invocation's own tenant scope)
//! still hashes to the accepted digest.

use tachyon_serverless_domain::Invocation;
use tachyon_serverless_durable_port::{Delivery, ObjectStore};

use super::InvokeEnvelope;
use crate::error::AppError;
use crate::repository::{AsyncInputBody, AsyncInvocationRepository, InvocationRepository};

#[derive(Debug, Clone)]
pub struct AcceptedEvent {
    pub envelope: InvokeEnvelope,
    pub invocation: Invocation,
    pub input: Vec<u8>,
}

fn mismatch(what: &str) -> AppError {
    // Indistinguishable from a missing invocation: a message routed under
    // another tenant must not reveal anything about this one.
    AppError::NotFound(format!(
        "delivered event does not match the ledger ({what})"
    ))
}

pub async fn read_delivery(
    ledger: &dyn AsyncInvocationRepository,
    invocations: &dyn InvocationRepository,
    objects: Option<&dyn ObjectStore>,
    delivery: &Delivery,
) -> Result<AcceptedEvent, AppError> {
    let envelope: InvokeEnvelope = serde_json::from_slice(&delivery.payload)
        .map_err(|e| AppError::InvalidRequest(format!("event envelope: {e}")))?;
    if crate::repository::outbox::message_id_for(&envelope.invocation_id, envelope.generation)
        != delivery.message_id.as_str()
    {
        return Err(mismatch("message id"));
    }
    if envelope.tenant_id != delivery.tenant_id {
        return Err(mismatch("routing tenant"));
    }
    let invocation = invocations
        .get(&envelope.invocation_id)?
        .ok_or_else(|| mismatch("invocation"))?;
    if invocation.tenant_id != delivery.tenant_id
        || invocation.revision_id != envelope.revision_id
        || invocation.function_id != envelope.function_id
        || invocation.input_digest != envelope.input_digest
    {
        return Err(mismatch("invocation fields"));
    }
    let input = ledger
        .async_input(&invocation.id)?
        .ok_or_else(|| mismatch("input"))?;
    if input.tenant_id != invocation.tenant_id {
        return Err(mismatch("input tenant"));
    }
    let bytes = match input.body {
        AsyncInputBody::Inline(bytes) => bytes,
        AsyncInputBody::Object(reference) => {
            if reference.scope.tenant_id != invocation.tenant_id {
                return Err(mismatch("object tenant"));
            }
            let objects = objects.ok_or_else(|| {
                AppError::platform("the input is in the object store, but none is configured")
            })?;
            objects
                .get(&reference.scope, &reference.id)
                .await
                .map_err(|e| AppError::platform(format!("reading the input object: {e}")))?
                .bytes
        }
    };
    if tachyon_serverless_domain::Sha256Digest::of_bytes(&bytes) != invocation.input_digest {
        return Err(AppError::platform(
            "the stored input does not match the accepted digest",
        ));
    }
    Ok(AcceptedEvent {
        envelope,
        invocation,
        input: bytes,
    })
}
