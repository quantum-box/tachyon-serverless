//! Row invariants every store enforces, whatever the service layer checked
//! before (PLT-4618). They are pure functions over the stored row and the
//! row being written, so the volatile and the SQLite store refuse exactly the
//! same writes.
//!
//! - **Tenant boundary**: a row that names a parent which exists must carry
//!   the parent's tenant. Parents are not required to exist (the ledger
//!   outlives a deleted function), except that an alias must point at an
//!   existing revision of its own function.
//! - **Identity is immutable**: ids, tenant, parent ids, numbers, specs,
//!   digests and creation times never change on update.
//! - **Terminal is final**: a terminal invocation / attempt / environment, a
//!   Ready / Failed revision, a released lease and a deleted function are
//!   never rewritten. Writing the identical row again is a no-op.
//! - **Fencing**: an environment update must carry the stored epoch.
//! - **Bounded bodies**: inline output never exceeds the response limit.

use tachyon_serverless_domain::{
    ExecutionEnvironment, ExecutionLease, Function, FunctionAlias, FunctionRevision, Invocation,
    InvocationAttempt, PayloadRef, TenantId,
};

use super::RepoError;

/// Whether the write changes anything. `Unchanged` means the caller must not
/// write (and must not fail).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    Apply,
    Unchanged,
}

fn refuse(msg: impl Into<String>) -> RepoError {
    RepoError::Refused(msg.into())
}

fn same_tenant(kind: &str, id: &str, row: &TenantId, parent: &TenantId) -> Result<(), RepoError> {
    if row == parent {
        Ok(())
    } else {
        Err(refuse(format!(
            "{kind} {id} belongs to another tenant than its parent"
        )))
    }
}

macro_rules! immutable {
    ($kind:expr, $id:expr, $old:expr, $new:expr, [$($field:ident),+ $(,)?]) => {
        $(
            if $old.$field != $new.$field {
                return Err(refuse(format!(
                    "{} {}: `{}` is immutable",
                    $kind,
                    $id,
                    stringify!($field)
                )));
            }
        )+
    };
}

pub fn function_update(old: &Function, new: &Function) -> Result<Write, RepoError> {
    immutable!(
        "function",
        old.id,
        old,
        new,
        [id, tenant_id, name, created_at]
    );
    if old == new {
        return Ok(Write::Unchanged);
    }
    if old.is_deleted() {
        return Err(refuse(format!("function {} is deleted", old.id)));
    }
    Ok(Write::Apply)
}

pub fn revision_insert(
    function: Option<&Function>,
    revision: &FunctionRevision,
) -> Result<(), RepoError> {
    if let Some(f) = function {
        same_tenant(
            "revision",
            revision.id.as_str(),
            &revision.tenant_id,
            &f.tenant_id,
        )?;
    }
    Ok(())
}

pub fn revision_update(old: &FunctionRevision, new: &FunctionRevision) -> Result<Write, RepoError> {
    immutable!(
        "revision",
        old.id,
        old,
        new,
        [
            id,
            function_id,
            tenant_id,
            number,
            spec,
            spec_digest,
            created_at
        ]
    );
    if old == new {
        return Ok(Write::Unchanged);
    }
    if old.status.is_terminal() {
        return Err(refuse(format!(
            "revision {} is {} and final",
            old.id,
            old.status.name()
        )));
    }
    Ok(Write::Apply)
}

/// An alias may only point at an existing revision of its own function, in
/// its own tenant.
pub fn alias_target(
    function: Option<&Function>,
    revision: Option<&FunctionRevision>,
    alias: &FunctionAlias,
) -> Result<(), RepoError> {
    if let Some(f) = function {
        same_tenant("alias", alias.name.as_str(), &alias.tenant_id, &f.tenant_id)?;
    }
    let Some(r) = revision else {
        return Err(refuse(format!(
            "alias `{}` points at revision {} which does not exist",
            alias.name, alias.revision_id
        )));
    };
    if r.function_id != alias.function_id || r.tenant_id != alias.tenant_id {
        return Err(refuse(format!(
            "alias `{}` cannot point at revision {} of another function",
            alias.name, alias.revision_id
        )));
    }
    Ok(())
}

/// Identity of an alias across a compare-and-set. Whether the generation
/// still matches is the store's CAS, not a refusal.
pub fn alias_update(old: &FunctionAlias, new: &FunctionAlias) -> Result<(), RepoError> {
    immutable!(
        "alias",
        old.name,
        old,
        new,
        [function_id, tenant_id, name, created_at]
    );
    if new.generation <= old.generation {
        return Err(refuse(format!(
            "alias `{}`: generation must advance ({} -> {})",
            old.name, old.generation, new.generation
        )));
    }
    Ok(())
}

pub fn invocation_insert(
    function: Option<&Function>,
    invocation: &Invocation,
    max_inline_bytes: u64,
) -> Result<(), RepoError> {
    if let Some(f) = function {
        same_tenant(
            "invocation",
            invocation.id.as_str(),
            &invocation.tenant_id,
            &f.tenant_id,
        )?;
    }
    bounded_output(invocation, max_inline_bytes)
}

pub fn invocation_update(
    old: &Invocation,
    new: &Invocation,
    max_inline_bytes: u64,
) -> Result<Write, RepoError> {
    immutable!(
        "invocation",
        old.id,
        old,
        new,
        [
            id,
            tenant_id,
            function_id,
            alias,
            mode,
            event_kind,
            idempotency_key,
            input_digest,
            input_size_bytes,
            trace_id,
            accepted_at,
        ]
    );
    if old == new {
        return Ok(Write::Unchanged);
    }
    if old.status.is_terminal() {
        return Err(refuse(format!(
            "invocation {} is {} and final",
            old.id,
            old.status.name()
        )));
    }
    bounded_output(new, max_inline_bytes)?;
    Ok(Write::Apply)
}

/// Inline output is the only body the ledger holds; anything larger is
/// recorded by digest (`[invoke] inline_output_max_bytes`). The store refuses
/// an inline body above the response limit whatever the caller decided.
pub fn bounded_output(invocation: &Invocation, max_inline_bytes: u64) -> Result<(), RepoError> {
    if let Some(PayloadRef::Inline {
        bytes_base64,
        size_bytes,
    }) = &invocation.output
    {
        let encoded_max = max_inline_bytes.div_ceil(3).saturating_mul(4);
        if *size_bytes > max_inline_bytes || bytes_base64.len() as u64 > encoded_max {
            return Err(refuse(format!(
                "invocation {}: inline output of {} bytes exceeds {} bytes",
                invocation.id, size_bytes, max_inline_bytes
            )));
        }
    }
    Ok(())
}

pub fn attempt_insert(
    invocation: Option<&Invocation>,
    attempt: &InvocationAttempt,
) -> Result<(), RepoError> {
    if let Some(inv) = invocation {
        same_tenant(
            "attempt",
            attempt.id.as_str(),
            &attempt.tenant_id,
            &inv.tenant_id,
        )?;
    }
    Ok(())
}

pub fn attempt_update(
    old: &InvocationAttempt,
    new: &InvocationAttempt,
) -> Result<Write, RepoError> {
    immutable!(
        "attempt",
        old.id,
        old,
        new,
        [
            id,
            invocation_id,
            tenant_id,
            number,
            environment_id,
            epoch,
            start_kind,
            dispatched_at,
        ]
    );
    if old == new {
        return Ok(Write::Unchanged);
    }
    if old.status.is_terminal() {
        return Err(refuse(format!(
            "attempt {} is {} and final",
            old.id,
            old.status.name()
        )));
    }
    Ok(Write::Apply)
}

pub fn environment_insert(
    revision: Option<&FunctionRevision>,
    env: &ExecutionEnvironment,
) -> Result<(), RepoError> {
    if env.reuse_key.tenant_id != env.tenant_id || env.reuse_key.revision_id != env.revision_id {
        return Err(refuse(format!(
            "environment {}: its reuse key names another tenant or revision",
            env.id
        )));
    }
    if let Some(r) = revision {
        same_tenant("environment", env.id.as_str(), &env.tenant_id, &r.tenant_id)?;
    }
    Ok(())
}

pub fn environment_update(
    old: &ExecutionEnvironment,
    new: &ExecutionEnvironment,
) -> Result<Write, RepoError> {
    immutable!(
        "environment",
        old.id,
        old,
        new,
        [id, tenant_id, revision_id, provider, reuse_key, created_at]
    );
    if old == new {
        return Ok(Write::Unchanged);
    }
    if old.is_terminal() {
        return Err(refuse(format!(
            "environment {} is {} and final",
            old.id,
            old.state.name()
        )));
    }
    if old.epoch != new.epoch {
        return Err(refuse(format!(
            "environment {}: stale copy at epoch {} (stored epoch {})",
            old.id, new.epoch, old.epoch
        )));
    }
    Ok(Write::Apply)
}

pub fn lease_insert(
    env: Option<&ExecutionEnvironment>,
    lease: &ExecutionLease,
) -> Result<(), RepoError> {
    if let Some(e) = env {
        same_tenant("lease", lease.id.as_str(), &lease.tenant_id, &e.tenant_id)?;
    }
    Ok(())
}

pub fn lease_update(old: &ExecutionLease, new: &ExecutionLease) -> Result<Write, RepoError> {
    immutable!(
        "lease",
        old.id,
        old,
        new,
        [
            id,
            environment_id,
            attempt_id,
            tenant_id,
            epoch,
            acquired_at
        ]
    );
    if old == new {
        return Ok(Write::Unchanged);
    }
    if old.released_at.is_some() {
        return Err(refuse(format!("lease {} is released", old.id)));
    }
    Ok(Write::Apply)
}
