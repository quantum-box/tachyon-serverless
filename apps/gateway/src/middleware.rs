//! Request-id and bearer-token middleware.

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use tachyon_serverless_api_types::headers;
use tachyon_serverless_application::control::ControlError;
use tachyon_serverless_application::{AppError, GatewayRole};
use tachyon_serverless_domain::TenantId;
use tachyon_serverless_provider_port::{Credential, Principal};

use crate::AppState;
use crate::error::GatewayError;

/// Request id taken from `x-request-id` or generated.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

/// Echo / assign `x-request-id`.
pub async fn request_id(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let id = req
        .headers()
        .get(headers::REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .map(str::to_string)
        .unwrap_or_else(|| state.ids.next_ulid().to_string().to_ascii_lowercase());
    req.extensions_mut().insert(RequestId(id.clone()));
    let mut res = next.run(req).await;
    if let Ok(v) = HeaderValue::from_str(&id) {
        res.headers_mut().insert(headers::REQUEST_ID, v);
    }
    res
}

/// The bearer token of `Authorization: Bearer <token>`, if well formed.
pub fn bearer_token(req: &Request) -> Option<String> {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, rest) = v.split_once(' ')?;
            if scheme.eq_ignore_ascii_case("bearer") {
                Some(rest.trim().to_string())
            } else {
                None
            }
        })
        .filter(|t| !t.is_empty())
}

/// Management API: `Authorization: Bearer <token>` -> [`Principal`] from the
/// control plane's own identity provider (`[[identity.tokens]]`); the
/// optional `x-tachyon-tenant-id` header must match the token's tenant.
pub async fn authenticate(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let request_id = req.extensions().get::<RequestId>().map(|r| r.0.clone());
    let Some(token) = bearer_token(&req) else {
        return missing_bearer(request_id);
    };
    let Some(principal) = state.identity.authenticate(&Credential(token)).await else {
        return GatewayError::new(
            AppError::Unauthorized("unknown credential".into()),
            request_id,
        )
        .into_response();
    };
    admit(principal, request_id, req, next).await
}

/// Invoke and invocation reads: the principal comes from the configuration
/// cache (delivered grants under an auth lease, PLT-4636), never from the
/// management store. An expired lease or an unknown tenant is refused with
/// its own `error_type`.
pub async fn authenticate_invoke(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let request_id = req.extensions().get::<RequestId>().map(|r| r.0.clone());
    let Some(token) = bearer_token(&req) else {
        return missing_bearer(request_id);
    };
    match state.config_cache.authenticate(&Credential(token)).await {
        Ok(principal) => admit(principal, request_id, req, next).await,
        Err(e) => GatewayError::new(e, request_id).into_response(),
    }
}

/// The management API is served by the control plane only: a data-plane
/// gateway answers 503 with `Host.ControlPlaneUnavailable` (PLT-4636).
pub async fn management_gate(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if state.config.control_plane.role == GatewayRole::DataPlane {
        let request_id = req.extensions().get::<RequestId>().map(|r| r.0.clone());
        return GatewayError::new(
            AppError::control(
                ControlError::ControlPlaneUnavailable,
                format!(
                    "this gateway is a data plane; the management API is served by the control \
                     plane at {}",
                    state
                        .config
                        .control_plane
                        .url
                        .as_deref()
                        .unwrap_or("(unset)")
                ),
            ),
            request_id,
        )
        .into_response();
    }
    next.run(req).await
}

fn missing_bearer(request_id: Option<String>) -> Response {
    GatewayError::new(
        AppError::Unauthorized("missing or malformed Authorization: Bearer header".into()),
        request_id,
    )
    .into_response()
}

async fn admit(
    principal: Principal,
    request_id: Option<String>,
    mut req: Request,
    next: Next,
) -> Response {
    if let Some(claimed) = req
        .headers()
        .get(headers::TENANT_ID)
        .and_then(|v| v.to_str().ok())
    {
        match TenantId::parse(claimed.trim()) {
            Ok(t) if t == principal.tenant_id => {}
            _ => {
                return GatewayError::new(
                    AppError::Forbidden(format!(
                        "{} does not match the authenticated tenant",
                        headers::TENANT_ID
                    )),
                    request_id,
                )
                .into_response();
            }
        }
    }
    req.extensions_mut().insert(principal);
    next.run(req).await
}

/// Handler context: the authenticated principal and the request id.
#[derive(Debug, Clone)]
pub struct Ctx {
    pub principal: Principal,
    pub request_id: String,
}

impl<S: Send + Sync> FromRequestParts<S> for Ctx {
    type Rejection = GatewayError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let request_id = parts
            .extensions
            .get::<RequestId>()
            .map(|r| r.0.clone())
            .unwrap_or_default();
        let principal = parts
            .extensions
            .get::<Principal>()
            .cloned()
            .ok_or_else(|| {
                GatewayError::new(
                    AppError::Unauthorized("not authenticated".into()),
                    Some(request_id.clone()),
                )
            })?;
        Ok(Self {
            principal,
            request_id,
        })
    }
}
