use crate::auth::auth_middleware;
use crate::server_state::ServerState;
use axum::Router;
use axum::middleware::from_fn_with_state;
use std::sync::Arc;

pub mod blobs;
pub mod credentials;
pub mod groups;

pub fn rest_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .merge(crate::onboarding::router())
        .merge(blobs::router())
        .merge(credentials::router())
        .merge(groups::router())
        .layer(from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state)
}
