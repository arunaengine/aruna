use crate::auth::auth_middleware;
use crate::server_state::ServerState;
use crate::telemetry::request_tracing_middleware;
use axum::Router;
use axum::middleware::from_fn_with_state;
use std::sync::Arc;

pub mod blobs;
pub mod connectors;
pub mod credentials;
pub mod drs;
pub mod groups;
pub mod info;
pub mod jobs;
pub mod metadata;
pub mod notifications;
pub mod onboarding;
pub mod search;
pub mod staging;
pub mod tes;
pub mod users;

pub fn rest_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .merge(info::router())
        .merge(onboarding::router())
        .merge(blobs::router())
        .merge(drs::router())
        .merge(staging::router())
        .merge(connectors::router())
        .merge(credentials::router())
        .merge(groups::router())
        .merge(jobs::router())
        .merge(metadata::router())
        .merge(notifications::router())
        .merge(search::router())
        .merge(tes::router())
        .merge(users::router())
        .layer(from_fn_with_state(state.clone(), auth_middleware))
        .layer(from_fn_with_state(
            state.clone(),
            request_tracing_middleware,
        ))
        .with_state(state)
}
