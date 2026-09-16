//! `functions create|list|get|delete|revisions|revision|aliases|alias-set|invocations|invocation|cancel`.

use tachyon_serverless_api_types::{
    AliasResponse, AttemptResponse, CreateFunctionRequest, FunctionResponse, InvocationResponse,
    RevisionResponse, UpdateAliasRequest,
};

use crate::client::ApiClient;
use crate::commands::decode_items;
use crate::error::CliError;
use crate::output::{Printer, Table, ellipsize, opt, opt_ms, opt_ts, ts};
use crate::resolve::{list_all_functions, resolve_function_id};

pub async fn create(
    client: &ApiClient,
    name: &str,
    description: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let req = CreateFunctionRequest {
        name: name.to_string(),
        description: description.to_string(),
    };
    let resp = client.post_json("/v1/functions", &req).await?.ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let f: FunctionResponse = resp.json()?;
    print_function(&f, p)
}

pub async fn list(client: &ApiClient, p: &mut Printer<'_>) -> Result<(), CliError> {
    if p.json {
        let resp = client.get("/v1/functions").await?.ok()?;
        return p.raw(&resp.body_text());
    }
    let functions = list_all_functions(client).await?;
    let mut t = Table::new(&["ID", "NAME", "DESCRIPTION", "CREATED", "DELETED"]);
    for f in &functions {
        t.row(vec![
            f.id.clone(),
            f.name.clone(),
            ellipsize(&f.description, 40),
            ts(&f.created_at),
            opt_ts(&f.deleted_at),
        ]);
    }
    if t.is_empty() {
        p.note("no functions")?;
    }
    p.table(&t)
}

pub async fn get(client: &ApiClient, function: &str, p: &mut Printer<'_>) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client.get(&format!("/v1/functions/{id}")).await?.ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let f: FunctionResponse = resp.json()?;
    print_function(&f, p)
}

pub async fn delete(
    client: &ApiClient,
    function: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client.delete(&format!("/v1/functions/{id}")).await?.ok()?;
    if p.json {
        let body = resp.body_text();
        return p.raw(if body.trim().is_empty() {
            "{}"
        } else {
            body.as_str()
        });
    }
    p.line(format!("deleted {id}"))
}

pub async fn revisions(
    client: &ApiClient,
    function: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!("/v1/functions/{id}/revisions"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let items: Vec<RevisionResponse> = decode_items(&resp)?;
    let mut t = Table::new(&["ID", "NUMBER", "STATUS", "ARCH", "CREATED", "DESCRIPTION"]);
    for r in &items {
        t.row(vec![
            r.id.clone(),
            r.number.to_string(),
            revision_status(r),
            spec_arch(&r.spec),
            ts(&r.created_at),
            ellipsize(&r.spec_description(), 40),
        ]);
    }
    if t.is_empty() {
        p.note("no revisions")?;
    }
    p.table(&t)
}

pub async fn revision(
    client: &ApiClient,
    function: &str,
    revision_id: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!("/v1/functions/{id}/revisions/{revision_id}"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let r: RevisionResponse = resp.json()?;
    print_revision(&r, p)
}

pub async fn aliases(
    client: &ApiClient,
    function: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!("/v1/functions/{id}/aliases"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let items: Vec<AliasResponse> = decode_items(&resp)?;
    let mut t = Table::new(&["ALIAS", "REVISION", "GENERATION", "PREVIOUS", "UPDATED"]);
    for a in &items {
        t.row(vec![
            a.name.clone(),
            a.revision_id.clone(),
            a.generation.to_string(),
            opt(&a.previous_revision_id),
            ts(&a.updated_at),
        ]);
    }
    if t.is_empty() {
        p.note("no aliases")?;
    }
    p.table(&t)
}

pub async fn alias_set(
    client: &ApiClient,
    function: &str,
    alias: &str,
    revision_id: &str,
    expected_generation: Option<u64>,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let req = UpdateAliasRequest {
        revision_id: revision_id.to_string(),
        expected_generation,
    };
    let resp = client
        .put_json(&format!("/v1/functions/{id}/aliases/{alias}"), &req)
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let a: AliasResponse = resp.json()?;
    print_alias(&a, p)
}

pub async fn invocations(
    client: &ApiClient,
    function: &str,
    limit: u32,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let id = resolve_function_id(client, function).await?;
    let resp = client
        .get(&format!("/v1/functions/{id}/invocations?limit={limit}"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let items: Vec<InvocationResponse> = decode_items(&resp)?;
    let mut t = Table::new(&[
        "ID", "STATUS", "ALIAS", "REVISION", "ACCEPTED", "FINISHED", "ERROR",
    ]);
    for i in &items {
        t.row(vec![
            i.id.clone(),
            i.status.clone(),
            opt(&i.alias),
            i.revision_id.clone(),
            ts(&i.accepted_at),
            opt_ts(&i.finished_at),
            i.error
                .as_ref()
                .map(|e| format!("{}/{}", e.class, e.error_type))
                .unwrap_or_else(|| "-".into()),
        ]);
    }
    if t.is_empty() {
        p.note("no invocations")?;
    }
    p.table(&t)
}

pub async fn invocation(
    client: &ApiClient,
    invocation_id: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let resp = client
        .get(&format!("/v1/invocations/{invocation_id}"))
        .await?
        .ok()?;
    if p.json {
        return p.raw(&resp.body_text());
    }
    let i: InvocationResponse = resp.json()?;
    print_invocation(&i, p)
}

pub async fn cancel(
    client: &ApiClient,
    invocation_id: &str,
    p: &mut Printer<'_>,
) -> Result<(), CliError> {
    let path = client.cancel_path(invocation_id);
    let resp = client
        .post_json(&path, &serde_json::json!({}))
        .await?
        .ok()?;
    if p.json {
        let body = resp.body_text();
        return p.raw(if body.trim().is_empty() {
            "{}"
        } else {
            body.as_str()
        });
    }
    match resp.json::<InvocationResponse>() {
        Ok(i) => p.line(format!("{} {}", i.id, i.status)),
        Err(_) => p.line(format!(
            "cancel requested for {invocation_id} (HTTP {})",
            resp.status
        )),
    }
}

// ---------------------------------------------------------------------------
// rendering
// ---------------------------------------------------------------------------

pub fn print_function(f: &FunctionResponse, p: &mut Printer<'_>) -> Result<(), CliError> {
    p.kv(&[
        ("id", f.id.clone()),
        ("name", f.name.clone()),
        ("tenant", f.tenant_id.clone()),
        ("description", f.description.clone()),
        ("created_at", ts(&f.created_at)),
        ("updated_at", ts(&f.updated_at)),
        ("deleted_at", opt_ts(&f.deleted_at)),
    ])
}

pub fn revision_status(r: &RevisionResponse) -> String {
    match &r.failure_reason {
        Some(reason) => format!("{} ({})", r.status, ellipsize(reason, 60)),
        None => r.status.clone(),
    }
}

fn spec_arch(spec: &serde_json::Value) -> String {
    spec.pointer("/runtime/architecture")
        .and_then(|v| v.as_str())
        .unwrap_or("-")
        .to_string()
}

trait SpecDescription {
    fn spec_description(&self) -> String;
}

impl SpecDescription for RevisionResponse {
    fn spec_description(&self) -> String {
        self.spec
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }
}

pub fn print_revision(r: &RevisionResponse, p: &mut Printer<'_>) -> Result<(), CliError> {
    p.kv(&[
        ("id", r.id.clone()),
        ("function", r.function_id.clone()),
        ("number", r.number.to_string()),
        ("status", revision_status(r)),
        ("spec_digest", r.spec_digest.clone()),
        ("created_at", ts(&r.created_at)),
        ("updated_at", ts(&r.updated_at)),
    ])?;
    p.line("spec:")?;
    p.pretty_json(&r.spec)
}

pub fn print_alias(a: &AliasResponse, p: &mut Printer<'_>) -> Result<(), CliError> {
    p.kv(&[
        ("alias", a.name.clone()),
        ("function", a.function_id.clone()),
        ("revision", a.revision_id.clone()),
        ("generation", a.generation.to_string()),
        ("previous", opt(&a.previous_revision_id)),
        ("updated_at", ts(&a.updated_at)),
    ])
}

pub fn print_invocation(i: &InvocationResponse, p: &mut Printer<'_>) -> Result<(), CliError> {
    let error = i
        .error
        .as_ref()
        .map(|e| format!("{} / {}: {}", e.class, e.error_type, e.message))
        .unwrap_or_else(|| "-".into());
    let d = &i.deadlines;
    p.kv(&[
        ("id", i.id.clone()),
        ("function", i.function_id.clone()),
        ("revision", i.revision_id.clone()),
        ("alias", opt(&i.alias)),
        ("mode", i.mode.clone()),
        ("status", i.status.clone()),
        ("error", error),
        ("http_status", opt(&i.http_status)),
        ("trace_id", i.trace_id.clone()),
        (
            "input",
            format!("{} ({} bytes)", i.input_digest, i.input_size_bytes),
        ),
        ("accepted_at", ts(&i.accepted_at)),
        ("started_at", opt_ts(&i.started_at)),
        ("finished_at", opt_ts(&i.finished_at)),
        (
            "deadlines",
            format!(
                "queue={} init={} execution={} client={}",
                ts(&d.queue_deadline),
                opt_ts(&d.init_deadline),
                opt_ts(&d.execution_deadline),
                ts(&d.client_deadline)
            ),
        ),
    ])?;
    if let Some(out) = &i.output {
        p.line("output:")?;
        p.pretty_json(out)?;
    }
    p.line(format!("attempts: {}", i.attempts.len()))?;
    for a in &i.attempts {
        print_attempt(a, p)?;
    }
    Ok(())
}

fn print_attempt(a: &AttemptResponse, p: &mut Printer<'_>) -> Result<(), CliError> {
    let error = a
        .error
        .as_ref()
        .map(|e| format!(" error={}/{}: {}", e.class, e.error_type, e.message))
        .unwrap_or_default();
    p.line(format!(
        "  #{} {} env={} epoch={} status={} start={}{}",
        a.number, a.id, a.environment_id, a.epoch, a.status, a.start_kind, error
    ))?;
    let t = &a.timings;
    p.line(format!(
        "     timings: queue_wait={} boot={} init={} handler={} response={} total={}",
        opt_ms(&t.queue_wait_ms),
        opt_ms(&t.environment_boot_ms),
        opt_ms(&t.runtime_init_ms),
        opt_ms(&t.handler_ms),
        opt_ms(&t.response_ms),
        opt_ms(&t.total_ms),
    ))?;
    let ev = &a.boot_evidence;
    let host_pid = ev
        .get("host_pid")
        .and_then(|v| v.as_u64())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".into());
    let boot_id = ev
        .get("guest_boot_id")
        .and_then(|v| v.as_str())
        .unwrap_or("-")
        .to_string();
    let details = ev
        .get("details")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "{}".into());
    p.line(format!(
        "     boot_evidence: host_pid={host_pid} guest_boot_id={boot_id} details={details}"
    ))?;
    p.line(format!(
        "     dispatched_at={} finished_at={}",
        ts(&a.dispatched_at),
        opt_ts(&a.finished_at)
    ))
}
