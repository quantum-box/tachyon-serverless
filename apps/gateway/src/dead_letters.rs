//! Dead letters and redrive (PLT-4640, docs/adr/0013).
//!
//! - `GET /v1/functions/{function_id}/dead-letters`
//! - `GET /v1/dead-letters/{dead_letter_id}`
//! - `POST /v1/dead-letters/{dead_letter_id}:redrive` (and `/redrive`)
//!
//! Data-plane endpoints: they read and write this cell's ledger and are
//! authenticated like invoke. Reading needs `invoke`; a redrive needs `invoke`
//! **and** `redrive`. Another tenant's dead letter is a 404.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use tachyon_serverless_api_types::{
    ApiErrorBody, AsyncDispatchResponse, DeadLetterResponse, InvocationErrorResponse,
    InvokeAsyncResponse, ListResponse, RedriveAcceptedResponse, RedriveRequestBody,
    RedriveResponse, headers,
};
use tachyon_serverless_application::repository::{DeadLetter, Redrive};
use tachyon_serverless_application::services::invoke_async::{AsyncRefusal, RedriveRequest};
use tachyon_serverless_application::{AppError, InvocationDetail};
use tachyon_serverless_domain::{DeadLetterId, FunctionId, InvocationMode, RevisionId};

use crate::AppState;
use crate::error::{GatewayError, ResultExt};
use crate::middleware::Ctx;

type ApiResult<T> = Result<T, GatewayError>;

fn not_configured(request_id: &str) -> GatewayError {
    GatewayError::new(
        AppError::AsyncRefused {
            reason: AsyncRefusal::NotConfigured,
            message: "dead letters need asynchronous invoke ([queue] and the durable ledger)"
                .into(),
        },
        Some(request_id.to_string()),
    )
}

fn parse_dead_letter_id(raw: &str, request_id: &str) -> ApiResult<DeadLetterId> {
    DeadLetterId::parse(raw)
        .map_err(|_| AppError::NotFound(format!("dead letter `{raw}` not found")))
        .ctx(request_id)
}

pub fn redrive_response(r: &Redrive) -> RedriveResponse {
    RedriveResponse {
        id: r.id.to_string(),
        dead_letter_id: r.dead_letter_id.to_string(),
        function_id: r.function_id.to_string(),
        source_invocation_id: r.source_invocation_id.to_string(),
        invocation_id: r.invocation_id.to_string(),
        revision_id: r.revision_id.to_string(),
        revision_overridden: r.revision_overridden,
        requested_by: r.requested_by.clone(),
        reason: r.reason.clone(),
        created_at: r.created_at,
    }
}

pub fn dead_letter_response(d: &DeadLetter, redrives: &[Redrive]) -> DeadLetterResponse {
    DeadLetterResponse {
        id: d.id.to_string(),
        reason: d.reason.as_str().to_string(),
        status: d.status.as_str().to_string(),
        function_id: d.function_id.as_ref().map(ToString::to_string),
        invocation_id: d.invocation_id.as_ref().map(ToString::to_string),
        revision_id: d.revision_id.as_ref().map(ToString::to_string),
        attempts: d.attempts,
        deferrals: d.deferrals,
        last_error: d.last_error.as_ref().map(InvocationErrorResponse::from),
        accepted_at: d.accepted_at,
        first_attempt_at: d.first_attempt_at,
        last_attempt_at: d.last_attempt_at,
        created_at: d.created_at,
        input_digest: d.input_digest.as_ref().map(ToString::to_string),
        input_size_bytes: d.input_size_bytes,
        input_storage: d.input_storage.clone(),
        message_id: d.message_id.clone(),
        detail: d.detail.clone(),
        redrive_count: d.redrive_count,
        redriven_at: d.redriven_at,
        redrives: redrives.iter().map(redrive_response).collect(),
    }
}

/// The dispatch section of an asynchronous invocation's view. The caller has
/// already checked the invocation's tenant.
pub fn dispatch_section(
    state: &AppState,
    detail: &InvocationDetail,
) -> Option<AsyncDispatchResponse> {
    let inv = &detail.invocation;
    if inv.mode != InvocationMode::Async {
        return None;
    }
    let ledger = state.dispatch_ledger.as_ref()?;
    let record = ledger.dispatch_record(&inv.id).ok()?;
    let (dead, created_by) = match &state.dead_letters {
        Some(service) => service.links(&inv.id).ok()?,
        None => (None, None),
    };
    Some(AsyncDispatchResponse {
        state: record
            .as_ref()
            .map_or("pending", |r| r.state.as_str())
            .to_string(),
        attempts: record.as_ref().map_or(0, |r| r.attempts),
        deferrals: record.as_ref().map_or(0, |r| r.deferrals),
        generation: record.as_ref().map_or(0, |r| r.generation),
        next_attempt_at: record.as_ref().and_then(|r| r.next_attempt_at),
        last_error: record
            .as_ref()
            .and_then(|r| r.last_error.as_ref())
            .map(InvocationErrorResponse::from),
        dead_letter_id: dead.map(|d| d.id.to_string()),
        redriven_from: created_by.as_ref().map(redrive_response),
    })
}

#[derive(Debug, Default, Deserialize)]
pub struct DeadLetterQuery {
    pub limit: Option<usize>,
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/dead-letters", tag = "dead-letters", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("limit" = Option<usize>, Query, description = "max items (default 50, at most 1000)")),
    responses(
        (status = 200, description = "newest first; another tenant's function lists nothing", body = ListResponse<DeadLetterResponse>),
        (status = 403, body = ApiErrorBody),
        (status = 503, description = "`reason` = `not_configured`", body = ApiErrorBody)
    ))]
pub async fn list_dead_letters(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
    Query(q): Query<DeadLetterQuery>,
) -> ApiResult<Json<ListResponse<DeadLetterResponse>>> {
    let Some(service) = state.dead_letters.clone() else {
        return Err(not_configured(&ctx.request_id));
    };
    let fid = FunctionId::parse(&function_id)
        .map_err(|_| AppError::NotFound(format!("function `{function_id}` not found")))
        .ctx(&ctx.request_id)?;
    let items = service
        .list(&ctx.principal, &fid, q.limit.unwrap_or(50))
        .ctx(&ctx.request_id)?
        .iter()
        .map(|d| dead_letter_response(d, &[]))
        .collect();
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

#[utoipa::path(get, path = "/v1/dead-letters/{dead_letter_id}", tag = "dead-letters", security(("bearer" = [])),
    params(("dead_letter_id" = String, Path, description = "dead letter id (`dlq_...`)")),
    responses(
        (status = 200, description = "the entry and its redrives", body = DeadLetterResponse),
        (status = 403, body = ApiErrorBody),
        (status = 404, description = "unknown, or another tenant's", body = ApiErrorBody)
    ))]
pub async fn get_dead_letter(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(raw): Path<String>,
) -> ApiResult<Json<DeadLetterResponse>> {
    let Some(service) = state.dead_letters.clone() else {
        return Err(not_configured(&ctx.request_id));
    };
    let id = parse_dead_letter_id(&raw, &ctx.request_id)?;
    let view = service.get(&ctx.principal, &id).ctx(&ctx.request_id)?;
    Ok(Json(dead_letter_response(
        &view.dead_letter,
        &view.redrives,
    )))
}

async fn run_redrive(state: AppState, ctx: Ctx, raw: &str, body: Bytes) -> ApiResult<Response> {
    let Some(service) = state.dead_letters.clone() else {
        return Err(not_configured(&ctx.request_id));
    };
    let id = parse_dead_letter_id(raw, &ctx.request_id)?;
    let req: RedriveRequestBody = if body.is_empty() {
        RedriveRequestBody::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::InvalidRequest(format!("body: {e}")))
            .ctx(&ctx.request_id)?
    };
    let revision_id = match &req.revision_id {
        Some(raw) => Some(
            RevisionId::parse(raw)
                .map_err(|_| AppError::InvalidRequest(format!("revision_id `{raw}` is invalid")))
                .ctx(&ctx.request_id)?,
        ),
        None => None,
    };
    let done = service
        .redrive(RedriveRequest {
            principal: ctx.principal.clone(),
            dead_letter_id: id,
            revision_id,
            reason: req.reason,
        })
        .await
        .ctx(&ctx.request_id)?;
    let inv = &done.invocation;
    let status_url = format!("/v1/invocations/{}", inv.id);
    let input_storage = state
        .async_ledger
        .as_ref()
        .and_then(|l| l.async_input(&inv.id).ok().flatten())
        .map_or("inline", |i| i.storage());
    let body = RedriveAcceptedResponse {
        redrive: redrive_response(&done.redrive),
        invocation: InvokeAsyncResponse {
            invocation_id: inv.id.to_string(),
            function_id: inv.function_id.to_string(),
            revision_id: inv.revision_id.to_string(),
            alias: None,
            status: inv.status.name().to_string(),
            status_url: status_url.clone(),
            input_digest: inv.input_digest.to_string(),
            input_size_bytes: inv.input_size_bytes,
            input_storage: input_storage.to_string(),
            replayed: false,
            trace_id: inv.trace_id.clone(),
            accepted_at: inv.accepted_at,
        },
    };
    let mut res = (StatusCode::ACCEPTED, Json(body)).into_response();
    let h = res.headers_mut();
    for (name, value) in [
        (headers::INVOCATION_ID, inv.id.as_str()),
        ("location", status_url.as_str()),
    ] {
        if let Ok(v) = HeaderValue::from_str(value) {
            h.insert(HeaderName::from_static(name), v);
        }
    }
    Ok(res)
}

#[utoipa::path(post, path = "/v1/dead-letters/{dead_letter_id}/redrive", tag = "dead-letters", security(("bearer" = [])),
    params(("dead_letter_id" = String, Path, description = "dead letter id (`dlq_...`); `:redrive` is accepted too")),
    request_body(content = RedriveRequestBody, description = "optional: `revision_id` (another revision of the same function) and `reason`"),
    responses(
        (status = 202, description = "a new asynchronous invocation was committed with its outbox event and the audit record", body = RedriveAcceptedResponse),
        (status = 400, description = "invalid body, or `revision_id` of another function", body = ApiErrorBody),
        (status = 403, description = "the `invoke` and `redrive` roles are both required", body = ApiErrorBody),
        (status = 404, description = "unknown, or another tenant's dead letter or revision", body = ApiErrorBody),
        (status = 409, description = "already redriven, a poison event, the function deleted, or the revision not ready", body = ApiErrorBody),
        (status = 429, description = "`reason` = `backlog` | `queue_full`", body = ApiErrorBody),
        (status = 503, description = "`reason` = `queue_unavailable` | `not_configured`", body = ApiErrorBody)
    ))]
pub async fn redrive(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(raw): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    run_redrive(state, ctx, &raw, body).await
}

/// `POST /v1/dead-letters/{dead_letter_id}:redrive` (colon form).
pub async fn redrive_colon(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(raw): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let Some(id) = raw.strip_suffix(":redrive") else {
        return Err(GatewayError::new(
            AppError::NotFound(format!("no route for POST /v1/dead-letters/{raw}")),
            Some(ctx.request_id),
        ));
    };
    run_redrive(state, ctx, id, body).await
}
