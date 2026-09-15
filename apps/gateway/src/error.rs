//! HTTP rendering of application errors.

use axum::Json;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use tachyon_serverless_api_types::headers;
use tachyon_serverless_application::AppError;

/// An application error plus the request context needed to render it.
#[derive(Debug)]
pub struct GatewayError {
    pub error: AppError,
    pub request_id: Option<String>,
}

impl GatewayError {
    pub fn new(error: impl Into<AppError>, request_id: Option<String>) -> Self {
        Self {
            error: error.into(),
            request_id,
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.error.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = self.error.to_api_body(self.request_id);
        let invocation_id = body.error.invocation_id.clone();
        let mut response = (status, Json(body)).into_response();
        if let Some(id) = invocation_id
            && let Ok(v) = HeaderValue::from_str(&id)
        {
            response.headers_mut().insert(headers::INVOCATION_ID, v);
        }
        response
    }
}

/// Attach the request id to a service result.
pub trait ResultExt<T> {
    fn ctx(self, request_id: &str) -> Result<T, GatewayError>;
}

impl<T, E: Into<AppError>> ResultExt<T> for Result<T, E> {
    fn ctx(self, request_id: &str) -> Result<T, GatewayError> {
        self.map_err(|e| GatewayError::new(e, Some(request_id.to_string())))
    }
}
