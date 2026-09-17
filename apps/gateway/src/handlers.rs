//! Route handlers. Every handler is annotated for the OpenAPI document.

use std::net::SocketAddr;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use serde::Deserialize;

use tachyon_serverless_api_types::{
    AliasResponse, ApiErrorBody, ArtifactUploadResponse, AttemptResponse, CapacityInfo,
    CreateFunctionRequest, CreateRevisionRequest, DeadlinesResponse, FunctionResponse,
    InvocationErrorResponse, InvocationResponse, InvokeQuery, ListResponse, LogEntryResponse,
    LogsResponse, ProviderInfo, RevisionResponse, TimingsResponse, UpdateAliasRequest,
    UsageSummaryResponse, headers,
};
use tachyon_serverless_application::services::invoke::inline_output;
use tachyon_serverless_application::{AppError, InvocationDetail, InvokeOutcome, InvokeRequest};
use tachyon_serverless_domain::{
    AliasName, AttemptStatus, EventKind, FunctionId, InvocationId, InvocationMode,
    InvocationStatus, RevisionId, StartKind,
};
use tachyon_serverless_protocol::runtime_api::{HttpRequestEvent, HttpResponsePayload};

use crate::AppState;
use crate::error::{GatewayError, ResultExt};
use crate::middleware::{Ctx, RequestId};

type ApiResult<T> = Result<T, GatewayError>;

// ---------------------------------------------------------------------------
// health / meta
// ---------------------------------------------------------------------------

#[utoipa::path(get, path = "/healthz", tag = "meta", responses((status = 200, description = "alive")))]
pub async fn healthz() -> &'static str {
    "ok"
}

#[utoipa::path(get, path = "/readyz", tag = "meta", responses(
    (status = 200, description = "provider preflight ok and new invocations are accepted", body = Object),
    (status = 503, description = "provider not ready, dispatcher fenced, or new invocations refused (see `control_plane`)", body = Object)
))]
pub async fn readyz(State(state): State<AppState>) -> Response {
    let report = state.provider_service.preflight().await;
    // A dispatcher that lost its lease takes no new work (PLT-4631).
    let fenced = state.dispatcher.is_fenced();
    // What the configuration cache and the control plane still allow
    // (PLT-4636): running executions always continue; new invocations and new
    // cold starts are reported separately, each with its refusal.
    let control = state
        .invoke_gate
        .view(state.pool.policy().reuse_enabled())
        .await;
    let ready = report.ok && !fenced && control.new_invocations == "accepted";
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "ready": ready,
            "dispatcher": {
                "id": state.dispatcher.id().as_str(),
                "instance": state.dispatcher.instance(),
                "fenced": fenced,
            },
            "preflight": report,
            "control_plane": control,
            // `null` until the startup reconcile ran (or when it is off).
            "reconcile": state.reconcile.last_report(),
        })),
    )
        .into_response()
}

#[derive(Debug, Default, Deserialize)]
pub struct SinceQuery {
    pub since: Option<u64>,
}

/// `GET /v1/internal/config?since=<generation>` (PLT-4636): the generation
/// stamped configuration for data planes. Authenticated with the internal
/// credential (`[control_plane] internal_token`) as a bearer token, never with
/// a tenant token; served only by a `combined` gateway that has one
/// configured (404 otherwise).
#[utoipa::path(get, path = "/v1/internal/config", tag = "internal", security(("bearer" = [])),
    params(("since" = Option<u64>, Query, description = "return entries with a generation above this (default 0: everything)")),
    responses(
        (status = 200, description = "configuration delivery (`ConfigDelivery`: generation, since, TTL caps, entries with tombstones)", body = Object),
        (status = 401, body = ApiErrorBody),
        (status = 404, body = ApiErrorBody),
        (status = 503, description = "the ledger store is unavailable", body = ApiErrorBody)
    ))]
pub async fn internal_config(
    State(state): State<AppState>,
    Query(query): Query<SinceQuery>,
    req: Request,
) -> Response {
    use tachyon_serverless_application::control::{ControlError, constant_time_eq};
    let request_id = req.extensions().get::<RequestId>().map(|r| r.0.clone());
    let (Some(publisher), Some(expected)) = (
        state.config_publisher.as_ref(),
        state.config.control_plane.internal_token.as_ref(),
    ) else {
        return GatewayError::new(
            AppError::NotFound(format!("no route for GET {}", req.uri().path())),
            request_id,
        )
        .into_response();
    };
    let presented = crate::middleware::bearer_token(&req).unwrap_or_default();
    if !constant_time_eq(presented.as_bytes(), expected.expose().as_bytes()) {
        return GatewayError::new(
            AppError::Unauthorized("the internal credential is required".into()),
            request_id,
        )
        .into_response();
    }
    match publisher.publish(query.since.unwrap_or(0)) {
        Ok(delivery) => (StatusCode::OK, Json(delivery)).into_response(),
        Err(e) => GatewayError::new(
            AppError::control(ControlError::StoreUnavailable, format!("ledger: {e}")),
            request_id,
        )
        .into_response(),
    }
}

#[utoipa::path(get, path = "/openapi.json", tag = "meta", responses((status = 200, description = "OpenAPI document", body = Object)))]
pub async fn openapi_json() -> Json<serde_json::Value> {
    Json(crate::openapi::document())
}

pub async fn not_found(req: Request) -> Response {
    let request_id = req.extensions().get::<RequestId>().map(|r| r.0.clone());
    GatewayError::new(
        AppError::NotFound(format!(
            "no route for {} {}",
            req.method(),
            req.uri().path()
        )),
        request_id,
    )
    .into_response()
}

#[utoipa::path(get, path = "/v1/provider", tag = "provider", security(("bearer" = [])), responses(
    (status = 200, body = ProviderInfo),
    (status = 401, body = ApiErrorBody)
))]
pub async fn provider_info(
    State(state): State<AppState>,
    ctx: Ctx,
) -> ApiResult<Json<ProviderInfo>> {
    let info = state.provider_service.info().await.ctx(&ctx.request_id)?;
    Ok(Json(info))
}

/// Node capacity versus reservations, environments by state, the wait queue,
/// the start-rate limiter and the caller's own tenant and revisions
/// (PLT-4634). Other tenants are never listed.
#[utoipa::path(get, path = "/v1/capacity", tag = "provider", security(("bearer" = [])), responses(
    (status = 200, body = CapacityInfo),
    (status = 401, body = ApiErrorBody)
))]
pub async fn capacity_info(State(state): State<AppState>, ctx: Ctx) -> Json<CapacityInfo> {
    Json(state.admission.snapshot(&ctx.principal.tenant_id))
}

// ---------------------------------------------------------------------------
// artifacts
// ---------------------------------------------------------------------------

#[utoipa::path(post, path = "/v1/artifacts", tag = "artifacts", security(("bearer" = [])),
    request_body(content = String, content_type = "application/octet-stream", description = "raw executable bytes"),
    responses(
        (status = 200, body = ArtifactUploadResponse),
        (status = 413, body = ApiErrorBody)
    ))]
pub async fn upload_artifact(
    State(state): State<AppState>,
    ctx: Ctx,
    req: Request,
) -> ApiResult<Json<ArtifactUploadResponse>> {
    tachyon_serverless_application::authz::require_deploy(&ctx.principal).ctx(&ctx.request_id)?;
    let max = state.limits.max_artifact_bytes;
    let body = read_body(req, max).await.ctx(&ctx.request_id)?;
    // The service records the caller's tenant as an owner of the digest;
    // revisions may only reference digests their tenant uploaded.
    let stored = state
        .artifact_service
        .upload(&ctx.principal, &body)
        .await
        .ctx(&ctx.request_id)?;
    Ok(Json(ArtifactUploadResponse {
        digest: stored.digest.to_string(),
        size_bytes: stored.size_bytes,
    }))
}

// ---------------------------------------------------------------------------
// functions
// ---------------------------------------------------------------------------

/// Buffer a request body of at most `max` bytes; anything larger (including
/// a rejection by the router's body limit) is a `PayloadTooLarge`.
async fn read_body(req: Request, max: u64) -> Result<Bytes, AppError> {
    let limit = usize::try_from(max).unwrap_or(usize::MAX);
    let body = req.into_body();
    match axum::body::to_bytes(body, limit).await {
        Ok(b) => Ok(b),
        Err(_) => Err(AppError::PayloadTooLarge {
            size: max.saturating_add(1),
            max,
        }),
    }
}

fn parse_function_id(raw: &str, request_id: &str) -> ApiResult<FunctionId> {
    FunctionId::parse(raw)
        .map_err(|_| AppError::NotFound(format!("function `{raw}` not found")))
        .ctx(request_id)
}

fn parse_invocation_id(raw: &str, request_id: &str) -> ApiResult<InvocationId> {
    InvocationId::parse(raw)
        .map_err(|_| AppError::NotFound(format!("invocation `{raw}` not found")))
        .ctx(request_id)
}

#[utoipa::path(post, path = "/v1/functions", tag = "functions", security(("bearer" = [])),
    request_body = CreateFunctionRequest,
    responses((status = 201, body = FunctionResponse), (status = 400, body = ApiErrorBody), (status = 409, body = ApiErrorBody)))]
pub async fn create_function(
    State(state): State<AppState>,
    ctx: Ctx,
    Json(req): Json<CreateFunctionRequest>,
) -> ApiResult<(StatusCode, Json<FunctionResponse>)> {
    let f = state
        .functions
        .create(&ctx.principal, &req.name, &req.description)
        .ctx(&ctx.request_id)?;
    Ok((StatusCode::CREATED, Json(FunctionResponse::from(&f))))
}

#[utoipa::path(get, path = "/v1/functions", tag = "functions", security(("bearer" = [])),
    responses((status = 200, body = ListResponse<FunctionResponse>)))]
pub async fn list_functions(
    State(state): State<AppState>,
    ctx: Ctx,
) -> ApiResult<Json<ListResponse<FunctionResponse>>> {
    let items = state
        .functions
        .list(&ctx.principal)
        .ctx(&ctx.request_id)?
        .iter()
        .map(FunctionResponse::from)
        .collect();
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}", tag = "functions", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    responses((status = 200, body = FunctionResponse), (status = 404, body = ApiErrorBody)))]
pub async fn get_function(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
) -> ApiResult<Json<FunctionResponse>> {
    let id = parse_function_id(&function_id, &ctx.request_id)?;
    let f = state
        .functions
        .get(&ctx.principal, &id)
        .ctx(&ctx.request_id)?;
    Ok(Json(FunctionResponse::from(&f)))
}

#[utoipa::path(delete, path = "/v1/functions/{function_id}", tag = "functions", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    responses((status = 200, body = FunctionResponse), (status = 404, body = ApiErrorBody)))]
pub async fn delete_function(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
) -> ApiResult<Json<FunctionResponse>> {
    let id = parse_function_id(&function_id, &ctx.request_id)?;
    let f = state
        .functions
        .delete(&ctx.principal, &id)
        .ctx(&ctx.request_id)?;
    Ok(Json(FunctionResponse::from(&f)))
}

// ---------------------------------------------------------------------------
// revisions
// ---------------------------------------------------------------------------

#[utoipa::path(post, path = "/v1/functions/{function_id}/revisions", tag = "revisions", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    request_body = CreateRevisionRequest,
    responses((status = 202, body = RevisionResponse), (status = 400, body = ApiErrorBody), (status = 404, body = ApiErrorBody)))]
pub async fn create_revision(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
    Json(req): Json<CreateRevisionRequest>,
) -> ApiResult<(StatusCode, Json<RevisionResponse>)> {
    let id = parse_function_id(&function_id, &ctx.request_id)?;
    let r = state
        .revisions
        .create(&ctx.principal, &id, &req)
        .await
        .ctx(&ctx.request_id)?;
    Ok((StatusCode::ACCEPTED, Json(RevisionResponse::from(&r))))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/revisions", tag = "revisions", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    responses((status = 200, body = ListResponse<RevisionResponse>), (status = 404, body = ApiErrorBody)))]
pub async fn list_revisions(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
) -> ApiResult<Json<ListResponse<RevisionResponse>>> {
    let id = parse_function_id(&function_id, &ctx.request_id)?;
    let items = state
        .revisions
        .list(&ctx.principal, &id)
        .ctx(&ctx.request_id)?
        .iter()
        .map(RevisionResponse::from)
        .collect();
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/revisions/{revision_id}", tag = "revisions", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("revision_id" = String, Path, description = "revision id")),
    responses((status = 200, body = RevisionResponse), (status = 404, body = ApiErrorBody)))]
pub async fn get_revision(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((function_id, revision_id)): Path<(String, String)>,
) -> ApiResult<Json<RevisionResponse>> {
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let rid = RevisionId::parse(&revision_id)
        .map_err(|_| AppError::NotFound("revision not found".into()))
        .ctx(&ctx.request_id)?;
    let r = state
        .revisions
        .get(&ctx.principal, &fid, &rid)
        .ctx(&ctx.request_id)?;
    Ok(Json(RevisionResponse::from(&r)))
}

// ---------------------------------------------------------------------------
// aliases
// ---------------------------------------------------------------------------

fn parse_alias(raw: &str, request_id: &str) -> ApiResult<AliasName> {
    AliasName::parse(raw)
        .map_err(|e| AppError::InvalidRequest(e.to_string()))
        .ctx(request_id)
}

#[utoipa::path(put, path = "/v1/functions/{function_id}/aliases/{alias}", tag = "aliases", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("alias" = String, Path, description = "alias name")),
    request_body = UpdateAliasRequest,
    responses((status = 200, body = AliasResponse), (status = 409, body = ApiErrorBody), (status = 404, body = ApiErrorBody)))]
pub async fn update_alias(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((function_id, alias)): Path<(String, String)>,
    Json(req): Json<UpdateAliasRequest>,
) -> ApiResult<Json<AliasResponse>> {
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let name = parse_alias(&alias, &ctx.request_id)?;
    let rid = RevisionId::parse(&req.revision_id)
        .map_err(|_| AppError::NotFound("revision not found".into()))
        .ctx(&ctx.request_id)?;
    let a = state
        .aliases
        .update(&ctx.principal, &fid, &name, &rid, req.expected_generation)
        .ctx(&ctx.request_id)?;
    Ok(Json(AliasResponse::from(&a)))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/aliases/{alias}", tag = "aliases", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("alias" = String, Path, description = "alias name")),
    responses((status = 200, body = AliasResponse), (status = 404, body = ApiErrorBody)))]
pub async fn get_alias(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((function_id, alias)): Path<(String, String)>,
) -> ApiResult<Json<AliasResponse>> {
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let name = parse_alias(&alias, &ctx.request_id)?;
    let a = state
        .aliases
        .get(&ctx.principal, &fid, &name)
        .ctx(&ctx.request_id)?;
    Ok(Json(AliasResponse::from(&a)))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/aliases", tag = "aliases", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    responses((status = 200, body = ListResponse<AliasResponse>), (status = 404, body = ApiErrorBody)))]
pub async fn list_aliases(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
) -> ApiResult<Json<ListResponse<AliasResponse>>> {
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let items = state
        .aliases
        .list(&ctx.principal, &fid)
        .ctx(&ctx.request_id)?
        .iter()
        .map(AliasResponse::from)
        .collect();
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

// ---------------------------------------------------------------------------
// invoke
// ---------------------------------------------------------------------------

fn invocation_response(detail: &InvocationDetail) -> InvocationResponse {
    let inv = &detail.invocation;
    let error = match &inv.status {
        InvocationStatus::Failed { error } | InvocationStatus::OutcomeUnknown { error } => {
            Some(InvocationErrorResponse::from(error))
        }
        _ => None,
    };
    let attempts = detail
        .attempts
        .iter()
        .map(|(a, evidence)| AttemptResponse {
            id: a.id.to_string(),
            number: a.number,
            environment_id: a.environment_id.to_string(),
            epoch: a.epoch,
            status: a.status.name().to_string(),
            error: match &a.status {
                AttemptStatus::Failed { error } | AttemptStatus::OutcomeUnknown { error } => {
                    Some(InvocationErrorResponse::from(error))
                }
                _ => None,
            },
            start_kind: match a.start_kind {
                StartKind::Cold => "cold",
                StartKind::Warm => "warm",
                StartKind::Restored => "restored",
            }
            .to_string(),
            timings: TimingsResponse::from(&a.timings),
            boot_evidence: serde_json::to_value(evidence).unwrap_or_default(),
            dispatched_at: a.dispatched_at,
            finished_at: a.finished_at,
        })
        .collect();
    InvocationResponse {
        id: inv.id.to_string(),
        function_id: inv.function_id.to_string(),
        revision_id: inv.revision_id.to_string(),
        alias: inv.alias.as_ref().map(ToString::to_string),
        alias_generation: inv.alias_generation,
        mode: match inv.mode {
            InvocationMode::Sync => "sync",
            InvocationMode::Async => "async",
        }
        .to_string(),
        status: inv.status.name().to_string(),
        error,
        output: inline_output(inv),
        http_status: inv.http_status,
        trace_id: inv.trace_id.clone(),
        input_digest: inv.input_digest.to_string(),
        input_size_bytes: inv.input_size_bytes,
        accepted_at: inv.accepted_at,
        started_at: inv.started_at,
        finished_at: inv.finished_at,
        deadlines: DeadlinesResponse::from(&inv.deadlines),
        attempts,
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn invoke_request_from(
    ctx: &Ctx,
    function_id: FunctionId,
    query: &InvokeQuery,
    headers: &HeaderMap,
    event_kind: EventKind,
    payload: serde_json::Value,
) -> ApiResult<InvokeRequest> {
    let alias = match &query.alias {
        Some(a) => Some(parse_alias(a, &ctx.request_id)?),
        None => None,
    };
    let revision_id = match &query.revision_id {
        Some(r) => Some(
            RevisionId::parse(r)
                .map_err(|_| AppError::NotFound("revision not found".into()))
                .ctx(&ctx.request_id)?,
        ),
        None => None,
    };
    let client_timeout_ms = header_str(headers, headers::CLIENT_TIMEOUT_MS)
        .map(|v| {
            v.parse::<u64>().map_err(|_| {
                AppError::InvalidRequest(format!(
                    "{} must be a positive integer",
                    headers::CLIENT_TIMEOUT_MS
                ))
            })
        })
        .transpose()
        .ctx(&ctx.request_id)?;
    Ok(InvokeRequest {
        principal: ctx.principal.clone(),
        function_id,
        alias,
        revision_id,
        event_kind,
        payload,
        idempotency_key: header_str(headers, headers::IDEMPOTENCY_KEY).map(str::to_string),
        client_timeout_ms,
        trace_id: header_str(headers, headers::TRACE_ID).map(str::to_string),
    })
}

fn invocation_headers(outcome: &InvokeOutcome) -> [(HeaderName, HeaderValue); 2] {
    let inv = outcome.invocation();
    [
        (
            HeaderName::from_static(headers::INVOCATION_ID),
            HeaderValue::from_str(inv.id.as_str()).unwrap_or(HeaderValue::from_static("")),
        ),
        (
            HeaderName::from_static(headers::TRACE_ID),
            HeaderValue::from_str(&inv.trace_id).unwrap_or(HeaderValue::from_static("")),
        ),
    ]
}

async fn run_json_invoke(
    state: AppState,
    ctx: Ctx,
    function_id: FunctionId,
    query: InvokeQuery,
    req: Request,
) -> ApiResult<Response> {
    let headers = req.headers().clone();
    let max = state.limits.max_payload_bytes;
    let body = read_body(req, max).await.ctx(&ctx.request_id)?;
    let payload: serde_json::Value = if body.is_empty() {
        serde_json::Value::Object(Default::default())
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::InvalidRequest(format!("body is not valid JSON: {e}")))
            .ctx(&ctx.request_id)?
    };
    let req = invoke_request_from(
        &ctx,
        function_id,
        &query,
        &headers,
        EventKind::Json,
        payload,
    )?;
    let outcome = state.invoke.invoke(req).await.ctx(&ctx.request_id)?;
    let hdrs = invocation_headers(&outcome);
    match outcome.error() {
        None => {
            let output = outcome.output.clone().unwrap_or(serde_json::Value::Null);
            Ok((StatusCode::OK, hdrs, Json(output)).into_response())
        }
        Some(err) => {
            let mut res = GatewayError::new(err, Some(ctx.request_id)).into_response();
            for (k, v) in hdrs {
                res.headers_mut().insert(k, v);
            }
            Ok(res)
        }
    }
}

#[utoipa::path(post, path = "/v1/functions/{function_id}/invoke", tag = "invoke", security(("bearer" = [])),
    params(
        ("function_id" = String, Path, description = "function id"),
        ("alias" = Option<String>, Query, description = "alias to resolve (default prod)"),
        ("revision_id" = Option<String>, Query, description = "pin a revision instead of resolving an alias"),
        ("idempotency-key" = Option<String>, Header, description = "replay protection"),
        ("x-tachyon-client-timeout-ms" = Option<u64>, Header, description = "client deadline (relative ms)")
    ),
    request_body(content = Object, description = "JSON event payload"),
    responses(
        (status = 200, description = "handler output (JSON); x-tachyon-invocation-id header", body = Object),
        (status = 404, body = ApiErrorBody), (status = 409, body = ApiErrorBody), (status = 413, body = ApiErrorBody),
        (status = 429, body = ApiErrorBody), (status = 502, body = ApiErrorBody),
        (status = 503, description = "admission refused: `error.reason` is `placement` or `circuit_open`", body = ApiErrorBody),
        (status = 504, body = ApiErrorBody)
    ))]
pub async fn invoke_json(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
    Query(query): Query<InvokeQuery>,
    req: Request,
) -> ApiResult<Response> {
    let id = parse_function_id(&function_id, &ctx.request_id)?;
    run_json_invoke(state, ctx, id, query, req).await
}

/// `POST /v1/functions/{function_id}:invoke` (colon form). Any other suffix
/// is a 404.
pub async fn invoke_colon(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(raw): Path<String>,
    Query(query): Query<InvokeQuery>,
    req: Request,
) -> ApiResult<Response> {
    let Some(function_id) = raw.strip_suffix(":invoke") else {
        return Err(GatewayError::new(
            AppError::NotFound(format!("no route for POST /v1/functions/{raw}")),
            Some(ctx.request_id),
        ));
    };
    let id = parse_function_id(function_id, &ctx.request_id)?;
    run_json_invoke(state, ctx, id, query, req).await
}

// ---------------------------------------------------------------------------
// HTTP adapter
// ---------------------------------------------------------------------------

/// Headers never forwarded to the function: the platform credential and
/// tenant selector (they belong to the gateway), plus hop-by-hop headers.
const STRIPPED_REQUEST_HEADERS: &[&str] = &[
    "authorization",
    headers::TENANT_ID,
    "connection",
    "transfer-encoding",
    "keep-alive",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
];

/// Response headers the handler is not allowed to set.
const STRIPPED_RESPONSE_HEADERS: &[&str] = &[
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    headers::INVOCATION_ID,
    headers::TRACE_ID,
    headers::REQUEST_ID,
];

#[utoipa::path(get, path = "/v1/functions/{function_id}/http/{path}", tag = "invoke", security(("bearer" = [])),
    params(
        ("function_id" = String, Path, description = "function id"),
        ("path" = String, Path, description = "path relative to the function root (any method is accepted)"),
        ("x-tachyon-alias" = Option<String>, Header, description = "alias to resolve (default prod)"),
        ("x-tachyon-revision-id" = Option<String>, Header, description = "pin a revision")
    ),
    responses(
        (status = 200, description = "the handler's HTTP response (status, headers and body as returned by the function); x-tachyon-invocation-id header"),
        (status = 404, body = ApiErrorBody), (status = 502, body = ApiErrorBody), (status = 504, body = ApiErrorBody)
    ))]
pub async fn http_path(
    State(state): State<AppState>,
    ctx: Ctx,
    // The `{*path}` capture is percent-decoded by axum and therefore not
    // used for the event; see [`adapter_path`].
    Path((function_id, _decoded_path)): Path<(String, String)>,
    req: Request,
) -> ApiResult<Response> {
    run_http(state, ctx, function_id, req).await
}

pub async fn http_root(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
    req: Request,
) -> ApiResult<Response> {
    run_http(state, ctx, function_id, req).await
}

/// The path relative to the function root, exactly as the client sent it
/// (still percent-encoded, without the query). Taken from the raw
/// request-target rather than the decoded route capture so that `%2F`, `%3F`,
/// `%23`, `%25` and spaces reach the function's router unchanged and the
/// router performs the single decode. Leading slashes collapse to one.
fn adapter_path(raw_path: &str) -> String {
    let rest = raw_path
        .strip_prefix("/v1/functions/")
        .and_then(|s| s.split_once('/'))
        .and_then(|(_, rest)| rest.strip_prefix("http"))
        .unwrap_or("");
    format!("/{}", rest.trim_start_matches('/'))
}

async fn run_http(
    state: AppState,
    ctx: Ctx,
    function_id: String,
    req: Request,
) -> ApiResult<Response> {
    let id = parse_function_id(&function_id, &ctx.request_id)?;
    let rel_path = adapter_path(req.uri().path());
    let query = InvokeQuery {
        alias: header_str(req.headers(), "x-tachyon-alias").map(str::to_string),
        revision_id: header_str(req.headers(), "x-tachyon-revision-id").map(str::to_string),
    };
    let source_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_string());
    let method = req.method().to_string();
    let raw_query = req.uri().query().unwrap_or("").to_string();
    let mut forwarded = Vec::new();
    for (name, value) in req.headers() {
        let n = name.as_str();
        if STRIPPED_REQUEST_HEADERS.contains(&n) {
            continue;
        }
        forwarded.push((
            n.to_string(),
            String::from_utf8_lossy(value.as_bytes()).into_owned(),
        ));
    }
    let headers = req.headers().clone();
    let body = read_body(req, state.limits.max_payload_bytes)
        .await
        .ctx(&ctx.request_id)?;
    let event = HttpRequestEvent {
        method,
        path: rel_path,
        query: raw_query,
        headers: forwarded,
        body_base64: base64::engine::general_purpose::STANDARD.encode(&body),
        source_ip,
    };
    let payload = serde_json::to_value(&event)
        .map_err(|e| AppError::platform(e.to_string()))
        .ctx(&ctx.request_id)?;
    let invoke = invoke_request_from(&ctx, id, &query, &headers, EventKind::Http, payload)?;
    let outcome = state.invoke.invoke(invoke).await.ctx(&ctx.request_id)?;
    let hdrs = invocation_headers(&outcome);
    if let Some(err) = outcome.error() {
        let mut res = GatewayError::new(err, Some(ctx.request_id)).into_response();
        for (k, v) in hdrs {
            res.headers_mut().insert(k, v);
        }
        return Ok(res);
    }
    let payload: HttpResponsePayload = outcome
        .output
        .clone()
        .and_then(|v| serde_json::from_value(v).ok())
        .ok_or_else(|| AppError::Invocation {
            invocation_id: outcome.invocation().id.clone(),
            error: tachyon_serverless_domain::InvocationError::new(
                tachyon_serverless_domain::ErrorClass::UserError,
                "Handler.InvalidHttpResponse",
                "handler did not return an HttpResponsePayload",
            ),
        })
        .ctx(&ctx.request_id)?;
    let status = StatusCode::from_u16(payload.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let body = base64::engine::general_purpose::STANDARD
        .decode(&payload.body_base64)
        .unwrap_or_default();
    let mut res = Response::builder().status(status);
    for (k, v) in &payload.headers {
        if STRIPPED_RESPONSE_HEADERS.contains(&k.to_ascii_lowercase().as_str()) {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(k.as_str()),
            HeaderValue::try_from(v.as_str()),
        ) {
            res = res.header(name, value);
        }
    }
    for (k, v) in hdrs {
        res = res.header(k, v);
    }
    res.body(axum::body::Body::from(body))
        .map_err(|e| AppError::platform(e.to_string()))
        .ctx(&ctx.request_id)
}

// ---------------------------------------------------------------------------
// history / logs / usage / cancel
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
pub struct HistoryQuery {
    pub limit: Option<usize>,
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/invocations", tag = "invocations", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("limit" = Option<usize>, Query, description = "max items (default 50)")),
    responses((status = 200, body = ListResponse<InvocationResponse>), (status = 404, body = ApiErrorBody)))]
pub async fn list_invocations(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<Json<ListResponse<InvocationResponse>>> {
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let items = state
        .history
        .list_invocations(&ctx.principal, &fid, limit)
        .ctx(&ctx.request_id)?
        .iter()
        .map(invocation_response)
        .collect();
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

#[utoipa::path(get, path = "/v1/invocations/{invocation_id}", tag = "invocations", security(("bearer" = [])),
    params(("invocation_id" = String, Path, description = "invocation id")),
    responses((status = 200, body = InvocationResponse), (status = 404, body = ApiErrorBody)))]
pub async fn get_invocation(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(invocation_id): Path<String>,
) -> ApiResult<Json<InvocationResponse>> {
    let id = parse_invocation_id(&invocation_id, &ctx.request_id)?;
    let detail = state
        .history
        .get_invocation(&ctx.principal, &id)
        .ctx(&ctx.request_id)?;
    Ok(Json(invocation_response(&detail)))
}

async fn run_cancel(state: AppState, ctx: Ctx, raw: &str) -> ApiResult<Json<InvocationResponse>> {
    let id = parse_invocation_id(raw, &ctx.request_id)?;
    let detail = state
        .invoke
        .cancel(&ctx.principal, &id)
        .await
        .ctx(&ctx.request_id)?;
    Ok(Json(invocation_response(&detail)))
}

#[utoipa::path(post, path = "/v1/invocations/{invocation_id}/cancel", tag = "invocations", security(("bearer" = [])),
    params(("invocation_id" = String, Path, description = "invocation id")),
    responses((status = 200, body = InvocationResponse), (status = 404, body = ApiErrorBody), (status = 409, body = ApiErrorBody)))]
pub async fn cancel_invocation(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(invocation_id): Path<String>,
) -> ApiResult<Json<InvocationResponse>> {
    run_cancel(state, ctx, &invocation_id).await
}

/// `POST /v1/invocations/{invocation_id}:cancel` (colon form).
pub async fn cancel_colon(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(raw): Path<String>,
) -> ApiResult<Json<InvocationResponse>> {
    let Some(invocation_id) = raw.strip_suffix(":cancel") else {
        return Err(GatewayError::new(
            AppError::NotFound(format!("no route for POST /v1/invocations/{raw}")),
            Some(ctx.request_id),
        ));
    };
    run_cancel(state, ctx, invocation_id).await
}

#[utoipa::path(get, path = "/v1/invocations/{invocation_id}/logs", tag = "invocations", security(("bearer" = [])),
    params(("invocation_id" = String, Path, description = "invocation id")),
    responses((status = 200, body = LogsResponse), (status = 404, body = ApiErrorBody)))]
pub async fn invocation_logs(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(invocation_id): Path<String>,
) -> ApiResult<Json<LogsResponse>> {
    let id = parse_invocation_id(&invocation_id, &ctx.request_id)?;
    let q = state
        .logs
        .for_invocation(&ctx.principal, &id)
        .ctx(&ctx.request_id)?;
    Ok(Json(LogsResponse {
        items: q.records.iter().map(LogEntryResponse::from).collect(),
        dropped: q.dropped,
    }))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/usage", tag = "usage", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    responses((status = 200, body = UsageSummaryResponse), (status = 404, body = ApiErrorBody)))]
pub async fn usage(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(function_id): Path<String>,
) -> ApiResult<Json<UsageSummaryResponse>> {
    let fid = parse_function_id(&function_id, &ctx.request_id)?;
    let s = state
        .history
        .usage_summary(&ctx.principal, &fid)
        .ctx(&ctx.request_id)?;
    Ok(Json(UsageSummaryResponse {
        function_id: s.function_id.to_string(),
        invocations: s.invocations,
        succeeded: s.succeeded,
        failed: s.failed,
        handler_ms_total: s.handler_ms_total,
        environment_ms_total: s.environment_ms_total,
        bytes_in_total: s.bytes_in_total,
        bytes_out_total: s.bytes_out_total,
        not_billable: true,
    }))
}

#[cfg(test)]
mod tests {
    use super::adapter_path;

    #[test]
    fn adapter_path_keeps_percent_encoding_and_normalises_the_root() {
        let base = "/v1/functions/fn_01hzzzzzzzzzzzzzzzzzzzzzzz/http";
        assert_eq!(adapter_path(base), "/");
        assert_eq!(adapter_path(&format!("{base}/")), "/");
        assert_eq!(adapter_path(&format!("{base}/items/42")), "/items/42");
        assert_eq!(adapter_path(&format!("{base}/a%20b")), "/a%20b");
        assert_eq!(adapter_path(&format!("{base}/a%2Fb/c")), "/a%2Fb/c");
        assert_eq!(adapter_path(&format!("{base}//x")), "/x");
    }
}
