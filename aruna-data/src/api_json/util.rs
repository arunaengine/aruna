use crate::error::ArunaDataError;
use axum::{Json, http::HeaderMap, response::IntoResponse};
use serde::Serialize;
use ulid::Ulid;

pub(super) fn extract_token(header: &HeaderMap) -> Option<String> {
    header
        .get("Authorization".to_string())
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|v| v.to_string())
}

pub fn into_axum_response<T: Serialize>(response: Result<T, ArunaDataError>) -> impl IntoResponse {
    response
        .map(|r| (axum::http::StatusCode::OK, Json(r)).into_response())
        .unwrap_or_else(|e| e.into_axum_tuple().into_response())
}
