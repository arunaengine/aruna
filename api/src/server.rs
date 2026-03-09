use crate::error::ServerSetupError;
use crate::routes::rest_router;
pub(crate) use crate::server_state::{ServerState, swagger_ui};
use axum::Router;
use axum::response::Redirect;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
pub struct Server {
    state: Arc<ServerState>,
    config: ServerConfig,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
}

impl Server {
    pub fn new(state: Arc<ServerState>, config: ServerConfig) -> Self {
        Self { state, config }
    }
    pub fn build_router(&self) -> Router {
        // Build the main API router
        let api_v1 = Router::new().merge(rest_router(self.state.clone()));

        // Build the root router with body size limit for REST API

        Router::new()
            .route(
                "/",
                axum::routing::get(|| async { Redirect::permanent("/swagger-ui") }),
            )
            .nest("/api/v1", api_v1)
            .merge(swagger_ui())
    }

    pub async fn run(self) -> Result<(), ServerSetupError> {
        let router = self.build_router();

        // Create TCP listener for main HTTP server
        let listener = TcpListener::bind(self.config.http_addr).await?;

        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .map_err(|e| ServerSetupError::Runtime(e.to_string()))?;

        Ok(())
    }
}
