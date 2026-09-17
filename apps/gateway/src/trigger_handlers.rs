//! Trigger CRUD and the signed webhook endpoint (PLT-4641, docs/adr/0014).

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use tachyon_serverless_api_types::{
    ApiErrorBody, CreateTriggerRequest, CronTriggerInfo, ListResponse, TriggerFireResponse,
    TriggerResponse, UpdateTriggerRequest, WebhookAcceptedResponse, WebhookTriggerInfo, headers,
    webhook_headers,
};
use tachyon_serverless_application::AppError;
use tachyon_serverless_application::repository::{FireRecord, Trigger, TriggerSpec};
use tachyon_serverless_application::services::invoke_async::AsyncRefusal;
use tachyon_serverless_application::services::triggers::{
    TriggerService, WebhookDelivery, policy_to_api, target_to_api,
};
use tachyon_serverless_domain::{FunctionId, TriggerId};

use crate::AppState;
use crate::error::{GatewayError, ResultExt};
use crate::middleware::{Ctx, RequestId};

type ApiResult<T> = Result<T, GatewayError>;

fn service(state: &AppState, request_id: &str) -> ApiResult<std::sync::Arc<TriggerService>> {
    state.triggers.clone().ok_or_else(|| {
        GatewayError::new(
            AppError::AsyncRefused {
                reason: AsyncRefusal::NotConfigured,
                message: "triggers need asynchronous invoke ([queue] and [store] backend = \
                          \"sqlite\") on a combined gateway"
                    .into(),
            },
            Some(request_id.to_string()),
        )
    })
}

fn parse_function_id(raw: &str, request_id: &str) -> ApiResult<FunctionId> {
    FunctionId::parse(raw)
        .map_err(|_| AppError::NotFound(format!("function `{raw}` not found")))
        .ctx(request_id)
}

fn parse_trigger_id(raw: &str, request_id: &str) -> ApiResult<TriggerId> {
    TriggerId::parse(raw)
        .map_err(|_| AppError::NotFound("trigger not found".into()))
        .ctx(request_id)
}

pub fn trigger_response(t: &Trigger, secret: Option<String>) -> TriggerResponse {
    let (cron, webhook) = match &t.spec {
        TriggerSpec::Cron(c) => (
            Some(CronTriggerInfo {
                expression: c.expression.clone(),
                timezone: c.timezone.clone(),
                payload: c.payload.clone(),
                missed_run_policy: policy_to_api(c.missed_run_policy),
                next_fire_at: t.next_fire_at,
                last_scheduled_at: t.last_scheduled_at,
            }),
            None,
        ),
        TriggerSpec::Webhook(w) => (
            None,
            Some(WebhookTriggerInfo {
                source: w.source.clone(),
                tolerance_seconds: w.tolerance_seconds,
                max_body_bytes: w.max_body_bytes,
                event_id_header: w.event_id_header.clone(),
                timestamp_header: webhook_headers::TIMESTAMP.to_string(),
                signature_header: webhook_headers::SIGNATURE.to_string(),
                url: format!("/v1/hooks/{}", t.id),
                secret_fingerprint: w.secret_fingerprint.clone(),
            }),
        ),
    };
    TriggerResponse {
        id: t.id.to_string(),
        function_id: t.function_id.to_string(),
        name: t.name.clone(),
        kind: t.kind().as_str().to_string(),
        enabled: t.is_enabled(),
        status: t.status.as_str().to_string(),
        status_reason: t.status_reason.clone(),
        generation: t.generation,
        target: target_to_api(&t.target),
        cron,
        webhook,
        secret,
        created_at: t.created_at,
        updated_at: t.updated_at,
    }
}

fn fire_response(f: &FireRecord) -> TriggerFireResponse {
    TriggerFireResponse {
        trigger_id: f.trigger_id.to_string(),
        fire_key: f.fire_key.clone(),
        scheduled_at: f.scheduled_at,
        event_id: f.event_id.clone(),
        outcome: f.outcome.as_str().to_string(),
        invocation_id: f.invocation_id.as_ref().map(ToString::to_string),
        reason: f.reason.clone(),
        created_at: f.created_at,
    }
}

/// A response that carries a secret must never be cached.
fn no_store(mut res: Response) -> Response {
    res.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    res
}

#[utoipa::path(post, path = "/v1/functions/{function_id}/triggers", tag = "triggers", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    request_body = CreateTriggerRequest,
    responses(
        (status = 201, description = "created; a webhook trigger's `secret` is in this response only", body = TriggerResponse),
        (status = 400, body = ApiErrorBody), (status = 403, body = ApiErrorBody), (status = 404, body = ApiErrorBody),
        (status = 409, description = "function deleted, or the function has `max_triggers_per_function` triggers", body = ApiErrorBody),
        (status = 413, description = "cron payload over the payload limit", body = ApiErrorBody),
        (status = 503, description = "`reason` = `not_configured`: no asynchronous invoke on this gateway", body = ApiErrorBody)
    ))]
pub async fn create_trigger(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
    Json(req): Json<CreateTriggerRequest>,
) -> ApiResult<Response> {
    let svc = service(&state, &ctx.request_id)?;
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let created = svc
        .create(&ctx.principal, &fid, &req)
        .ctx(&ctx.request_id)?;
    Ok(no_store(
        (
            StatusCode::CREATED,
            Json(trigger_response(&created.trigger, created.secret)),
        )
            .into_response(),
    ))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/triggers", tag = "triggers", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    responses((status = 200, body = ListResponse<TriggerResponse>), (status = 404, body = ApiErrorBody)))]
pub async fn list_triggers(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
) -> ApiResult<Json<ListResponse<TriggerResponse>>> {
    let svc = service(&state, &ctx.request_id)?;
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let items = svc
        .list(&ctx.principal, &fid)
        .ctx(&ctx.request_id)?
        .iter()
        .map(|t| trigger_response(t, None))
        .collect();
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/triggers/{trigger_id}", tag = "triggers", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("trigger_id" = String, Path, description = "trigger id")),
    responses((status = 200, body = TriggerResponse), (status = 404, body = ApiErrorBody)))]
pub async fn get_trigger(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((function_id, trigger_id)): Path<(String, String)>,
) -> ApiResult<Json<TriggerResponse>> {
    let svc = service(&state, &ctx.request_id)?;
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let tid = parse_trigger_id(&trigger_id, &ctx.request_id)?;
    let t = svc.get(&ctx.principal, &fid, &tid).ctx(&ctx.request_id)?;
    Ok(Json(trigger_response(&t, None)))
}

#[utoipa::path(patch, path = "/v1/functions/{function_id}/triggers/{trigger_id}", tag = "triggers", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("trigger_id" = String, Path, description = "trigger id")),
    request_body = UpdateTriggerRequest,
    responses(
        (status = 200, description = "updated; `secret` present only when `rotate_secret` was set", body = TriggerResponse),
        (status = 400, body = ApiErrorBody), (status = 404, body = ApiErrorBody),
        (status = 409, description = "`expected_generation` does not match, or a concurrent change", body = ApiErrorBody)
    ))]
pub async fn update_trigger(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((function_id, trigger_id)): Path<(String, String)>,
    Json(req): Json<UpdateTriggerRequest>,
) -> ApiResult<Response> {
    let svc = service(&state, &ctx.request_id)?;
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let tid = parse_trigger_id(&trigger_id, &ctx.request_id)?;
    let updated = svc
        .update(&ctx.principal, &fid, &tid, &req)
        .ctx(&ctx.request_id)?;
    Ok(no_store(
        Json(trigger_response(&updated.trigger, updated.secret)).into_response(),
    ))
}

#[utoipa::path(delete, path = "/v1/functions/{function_id}/triggers/{trigger_id}", tag = "triggers", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("trigger_id" = String, Path, description = "trigger id")),
    responses(
        (status = 200, description = "deleted: no fire commits after this; fires accepted before continue as ordinary asynchronous invocations", body = TriggerResponse),
        (status = 404, body = ApiErrorBody), (status = 409, body = ApiErrorBody)
    ))]
pub async fn delete_trigger(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((function_id, trigger_id)): Path<(String, String)>,
) -> ApiResult<Json<TriggerResponse>> {
    let svc = service(&state, &ctx.request_id)?;
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let tid = parse_trigger_id(&trigger_id, &ctx.request_id)?;
    let t = svc
        .delete(&ctx.principal, &fid, &tid)
        .ctx(&ctx.request_id)?;
    Ok(Json(trigger_response(&t, None)))
}

#[derive(Debug, Default, Deserialize)]
pub struct FiresQuery {
    pub limit: Option<usize>,
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/triggers/{trigger_id}/fires", tag = "triggers", security(("bearer" = [])),
    params(
        ("function_id" = String, Path, description = "function id"),
        ("trigger_id" = String, Path, description = "trigger id"),
        ("limit" = Option<usize>, Query, description = "max items (default 50, at most 1000)")
    ),
    responses((status = 200, body = ListResponse<TriggerFireResponse>), (status = 404, body = ApiErrorBody)))]
pub async fn list_trigger_fires(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((function_id, trigger_id)): Path<(String, String)>,
    Query(q): Query<FiresQuery>,
) -> ApiResult<Json<ListResponse<TriggerFireResponse>>> {
    let svc = service(&state, &ctx.request_id)?;
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let tid = parse_trigger_id(&trigger_id, &ctx.request_id)?;
    let items = svc
        .list_fires(&ctx.principal, &fid, &tid, q.limit.unwrap_or(50))
        .ctx(&ctx.request_id)?
        .iter()
        .map(fire_response)
        .collect();
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    let name = HeaderName::from_bytes(name.as_bytes()).ok()?;
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
}

/// Read at most `max` body bytes. `Content-Length` over the limit is refused
/// before a byte is read; a body that grows past it while streaming stops at
/// the limit (nothing larger is ever buffered).
async fn read_limited(headers: &HeaderMap, body: Body, max: u64) -> Result<bytes::Bytes, AppError> {
    let too_large = |size| AppError::PayloadTooLarge { size, max };
    if let Some(len) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        && len > max
    {
        return Err(too_large(len));
    }
    let limit = usize::try_from(max).unwrap_or(usize::MAX);
    axum::body::to_bytes(body, limit)
        .await
        .map_err(|_| too_large(max.saturating_add(1)))
}

#[utoipa::path(post, path = "/v1/hooks/{trigger_id}", tag = "triggers", security(("webhook_signature" = [])),
    params(
        ("trigger_id" = String, Path, description = "webhook trigger id"),
        ("x-tachyon-webhook-timestamp" = String, Header, description = "Unix seconds at signing; must be within the trigger's tolerance of the gateway clock"),
        ("x-tachyon-webhook-signature" = String, Header, description = "`v1=<hex HMAC-SHA256(secret, \"{timestamp}.{body}\")>` (comma-separated entries accepted)"),
        ("x-tachyon-webhook-id" = String, Header, description = "event id (1..=128 visible ASCII); the header name is the trigger's `event_id_header`")
    ),
    request_body(content = String, content_type = "application/json", description = "raw body as signed; JSON is passed as `body`, other UTF-8 as `body_text`, anything else as `body_base64`"),
    responses(
        (status = 202, description = "accepted (invocation, input, outbox event and fire row in one transaction), or a resend of an accepted event id / signed delivery (`replayed = true`, the same invocation)", body = WebhookAcceptedResponse),
        (status = 400, description = "missing or invalid event id (after a valid signature)", body = ApiErrorBody),
        (status = 401, description = "missing, malformed or wrong signature, or a timestamp outside the tolerance; nothing is stored", body = ApiErrorBody),
        (status = 404, description = "unknown, deleted or non-webhook trigger (indistinguishable)", body = ApiErrorBody),
        (status = 410, description = "the trigger is disabled (only answered to a correctly signed delivery)", body = ApiErrorBody),
        (status = 413, description = "body over the trigger's `max_body_bytes`; refused before it is read in full", body = ApiErrorBody),
        (status = 429, body = ApiErrorBody),
        (status = 503, body = ApiErrorBody)
    ))]
pub async fn receive_webhook(
    State(state): State<AppState>,
    Path(trigger_id): Path<String>,
    req: Request,
) -> ApiResult<Response> {
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .map(|r| r.0.clone())
        .unwrap_or_default();
    let svc = service(&state, &request_id)?;
    let tid = parse_trigger_id(&trigger_id, &request_id)?;
    let trigger = svc.webhook_trigger(&tid).ctx(&request_id)?;
    let TriggerSpec::Webhook(spec) = &trigger.spec else {
        return Err(GatewayError::new(
            AppError::NotFound("trigger not found".into()),
            Some(request_id),
        ));
    };
    let (parts, body) = req.into_parts();
    let bytes = match read_limited(&parts.headers, body, spec.max_body_bytes).await {
        Ok(b) => b,
        Err(e) => {
            svc.record_webhook_too_large();
            return Err(GatewayError::new(e, Some(request_id)));
        }
    };
    let delivery = WebhookDelivery {
        timestamp: header_string(&parts.headers, webhook_headers::TIMESTAMP),
        signature: header_string(&parts.headers, webhook_headers::SIGNATURE),
        event_id: header_string(&parts.headers, &spec.event_id_header),
        content_type: header_string(&parts.headers, header::CONTENT_TYPE.as_str()),
    };
    let accepted = svc
        .receive_webhook(&trigger, &delivery, &bytes)
        .await
        .ctx(&request_id)?;
    let inv = &accepted.invocation;
    let body = WebhookAcceptedResponse {
        trigger_id: accepted.trigger_id.to_string(),
        event_id: accepted.event_id.clone(),
        invocation_id: inv.id.to_string(),
        status: inv.status.name().to_string(),
        replayed: accepted.replayed,
    };
    let mut res = (StatusCode::ACCEPTED, Json(body)).into_response();
    if let Ok(v) = HeaderValue::from_str(inv.id.as_str()) {
        res.headers_mut().insert(headers::INVOCATION_ID, v);
    }
    Ok(res)
}
