use axum::{http::HeaderMap, response::IntoResponse, Json};
use serde::Serialize;

use crate::error::ArunaError;

pub(super) fn extract_token(header: &HeaderMap) -> Option<String> {
    header
        .get("Authorization".to_string())
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|v| v.to_string())
}

#[tracing::instrument(level = "trace", skip(response))]
pub fn into_axum_response<T: Serialize>(response: Result<T, ArunaError>) -> impl IntoResponse {
    let res = response
        .map(|r| (axum::http::StatusCode::OK, Json(r)).into_response())
        .unwrap_or_else(|e| e.into_axum_tuple().into_response());
    tracing::trace!(status=?res.status());
    res
}
