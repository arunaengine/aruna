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

pub fn xor_ulids(a: &Ulid, b: &Ulid) -> Vec<u8> {
    a.to_bytes()
        .iter()
        .zip(b.to_bytes().iter())
        .map(|(x1, x2)| *x1 ^ *x2)
        .collect()
}
