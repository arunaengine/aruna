use crate::auth::auth_middleware;
use crate::server_state::ServerState;
use crate::telemetry::request_tracing_middleware;
use axum::Router;
use axum::middleware::{from_fn, from_fn_with_state};
use std::sync::Arc;

pub mod blobs;
pub mod credentials;
pub mod groups;
pub mod metadata;
pub mod onboarding;

pub fn rest_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .merge(onboarding::router())
        .merge(blobs::router())
        .merge(credentials::router())
        .merge(groups::router())
        .merge(metadata::router())
        .layer(from_fn_with_state(state.clone(), auth_middleware))
        .layer(from_fn(request_tracing_middleware))
        .with_state(state)
}
