use crate::cors::CorsConfig;
use crate::error::ServerSetupError;
use crate::portal;
use crate::routes::rest_router;
pub(crate) use crate::server_state::{ServerState, swagger_ui};
use axum::Router;
use axum::extract::DefaultBodyLimit;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

pub const DEFAULT_MAX_HTTP_BODY_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Server {
    state: Arc<ServerState>,
    config: ServerConfig,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
    pub max_http_body_size: usize,
    pub cors: CorsConfig,
}

impl Server {
    pub fn new(state: Arc<ServerState>, config: ServerConfig) -> Self {
        Self { state, config }
    }
    pub fn build_router(&self) -> Router {
        // Build the main API router
        let api_v1 = Router::new().merge(rest_router(self.state.clone()));

        // Build the root router with body size limit for REST API

        let mut router = Router::new()
            .nest("/api/v1", api_v1)
            .layer(DefaultBodyLimit::max(self.config.max_http_body_size))
            .merge(swagger_ui())
            .merge(portal::router(self.state.clone()));
        if let Some(cors_layer) = self.config.cors.rest_layer() {
            router = router.layer(cors_layer);
        }
        router
    }

    pub async fn run(self) -> Result<(), ServerSetupError> {
        let listener = TcpListener::bind(self.config.http_addr).await?;
        self.run_with_listener(listener).await
    }

    pub async fn run_with_listener(self, listener: TcpListener) -> Result<(), ServerSetupError> {
        let bound_addr = listener.local_addr()?;
        self.state.register_rest_interface(bound_addr).await;
        let router = self.build_router();

        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .map_err(|e| ServerSetupError::Runtime(e.to_string()))?;

        Ok(())
    }
}
