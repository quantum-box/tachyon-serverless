//! Request-id and bearer-token middleware.

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use tachyon_serverless_api_types::headers;
use tachyon_serverless_application::AppError;
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

/// `Authorization: Bearer <token>` -> [`Principal`]; the optional
/// `x-tachyon-tenant-id` header must match the token's tenant.
pub async fn authenticate(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let request_id = req.extensions().get::<RequestId>().map(|r| r.0.clone());
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, rest) = v.split_once(' ')?;
            if scheme.eq_ignore_ascii_case("bearer") {
                Some(rest.trim().to_string())
            } else {
                None
            }
        });
    let Some(token) = token.filter(|t| !t.is_empty()) else {
        return GatewayError::new(
            AppError::Unauthorized("missing or malformed Authorization: Bearer header".into()),
            request_id,
        )
        .into_response();
    };
    let Some(principal) = state.identity.authenticate(&Credential(token)).await else {
        return GatewayError::new(
            AppError::Unauthorized("unknown credential".into()),
            request_id,
        )
        .into_response();
    };
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
