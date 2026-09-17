//! Experimental snapshots (X1, PLT-4653, docs/adr/0017): create, list, revoke.
//! Every route answers 503 unless `[snapshots] enabled`.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use tachyon_serverless_api_types::{
    ApiErrorBody, CreateSnapshotRequest, ListResponse, RevokeSnapshotRequest, SnapshotResponse,
};
use tachyon_serverless_application::AppError;
use tachyon_serverless_application::snapshot::SnapshotService;
use tachyon_serverless_domain::{FunctionId, RevisionId, SnapshotId};

use crate::AppState;
use crate::error::{GatewayError, ResultExt};
use crate::middleware::Ctx;

type ApiResult<T> = Result<T, GatewayError>;

fn service(state: &AppState, request_id: &str) -> ApiResult<std::sync::Arc<SnapshotService>> {
    state.snapshots.clone().ok_or_else(|| {
        GatewayError::new(
            AppError::ProviderUnavailable(
                "snapshots are experimental and not enabled on this gateway ([snapshots] enabled)"
                    .into(),
            ),
            Some(request_id.to_string()),
        )
    })
}

fn function_id(raw: &str, request_id: &str) -> ApiResult<FunctionId> {
    FunctionId::parse(raw)
        .map_err(|_| AppError::NotFound(format!("function `{raw}` not found")))
        .ctx(request_id)
}

#[utoipa::path(post, path = "/v1/functions/{function_id}/snapshots", tag = "snapshots", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    request_body = CreateSnapshotRequest,
    responses((status = 201, body = SnapshotResponse), (status = 400, body = ApiErrorBody), (status = 404, body = ApiErrorBody), (status = 503, body = ApiErrorBody)))]
pub async fn create_snapshot(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(raw): Path<String>,
    Json(req): Json<CreateSnapshotRequest>,
) -> ApiResult<(StatusCode, Json<SnapshotResponse>)> {
    let svc = service(&state, &ctx.request_id)?;
    let id = function_id(&raw, &ctx.request_id)?;
    let revision = match &req.revision_id {
        Some(r) => Some(
            RevisionId::parse(r)
                .map_err(|_| AppError::NotFound(format!("revision `{r}` not found")))
                .ctx(&ctx.request_id)?,
        ),
        None => None,
    };
    let snap = svc
        .create(&ctx.principal, &id, revision.as_ref())
        .await
        .ctx(&ctx.request_id)?;
    Ok((StatusCode::CREATED, Json(snap)))
}

#[utoipa::path(get, path = "/v1/functions/{function_id}/snapshots", tag = "snapshots", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id")),
    responses((status = 200, body = ListResponse<SnapshotResponse>), (status = 404, body = ApiErrorBody), (status = 503, body = ApiErrorBody)))]
pub async fn list_snapshots(
    State(state): State<AppState>,
    ctx: Ctx,
    Path(raw): Path<String>,
) -> ApiResult<Json<ListResponse<SnapshotResponse>>> {
    let svc = service(&state, &ctx.request_id)?;
    let id = function_id(&raw, &ctx.request_id)?;
    let items = svc.list(&ctx.principal, &id).ctx(&ctx.request_id)?;
    Ok(Json(ListResponse {
        items,
        next_cursor: None,
    }))
}

#[utoipa::path(post, path = "/v1/functions/{function_id}/snapshots/{snapshot_id}/revoke", tag = "snapshots", security(("bearer" = [])),
    params(("function_id" = String, Path, description = "function id"), ("snapshot_id" = String, Path, description = "snapshot id")),
    request_body = RevokeSnapshotRequest,
    responses((status = 200, body = SnapshotResponse), (status = 404, body = ApiErrorBody), (status = 503, body = ApiErrorBody)))]
pub async fn revoke_snapshot(
    State(state): State<AppState>,
    ctx: Ctx,
    Path((raw, snapshot)): Path<(String, String)>,
    Json(req): Json<RevokeSnapshotRequest>,
) -> ApiResult<Json<SnapshotResponse>> {
    let svc = service(&state, &ctx.request_id)?;
    let id = function_id(&raw, &ctx.request_id)?;
    let snapshot_id = SnapshotId::parse(&snapshot)
        .map_err(|_| AppError::NotFound(format!("snapshot `{snapshot}` not found")))
        .ctx(&ctx.request_id)?;
    let snap = svc
        .revoke(&ctx.principal, &id, &snapshot_id, &req.reason)
        .await
        .ctx(&ctx.request_id)?;
    Ok(Json(snap))
}
