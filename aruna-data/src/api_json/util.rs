use crate::{api_json::requests::Location, error::ArunaDataError};
use axum::{Json, http::HeaderMap, response::IntoResponse};
use iroh::NodeAddr;
use serde::Serialize;

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

impl From<NodeAddr> for Location {
    fn from(value: NodeAddr) -> Self {
        Location {
            node_id: value.node_id.to_string(),
            relay_url: value.relay_url.map(|url| url.to_string()),
            direct_addresses: value
                .direct_addresses
                .iter()
                .map(|addr| addr.to_string())
                .collect(),
        }
    }
}
