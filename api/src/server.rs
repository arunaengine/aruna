use crate::cors::CorsConfig;
use crate::csp::{PortalCspConfig, baseline_security_headers};
use crate::error::ServerSetupError;
use crate::portal;
use crate::routes::rest_router;
pub(crate) use crate::server_state::{ServerState, swagger_ui};
use axum::Router;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{Method, Uri, header};
use axum::middleware::{Next, from_fn, from_fn_with_state};
use axum::response::{IntoResponse, Redirect, Response};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

pub const DEFAULT_MAX_HTTP_BODY_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Server {
    state: Arc<ServerState>,
    config: ServerConfig,
    api_public_url: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
    pub max_http_body_size: usize,
    pub cors: CorsConfig,
    pub portal_csp: PortalCspConfig,
}

impl Server {
    pub fn new(state: Arc<ServerState>, config: ServerConfig) -> Self {
        Self {
            state,
            config,
            api_public_url: None,
        }
    }

    pub fn with_api_public_url(mut self, api_public_url: Option<String>) -> Self {
        self.api_public_url = api_public_url;
        self
    }
    pub fn build_router(&self) -> Router {
        // Build the main API router
        let api_v1 = Router::new().merge(rest_router(self.state.clone()));
        let api_authority = self
            .api_public_url
            .as_deref()
            .and_then(|url| url.parse::<Uri>().ok())
            .and_then(|url| url.authority().map(ToString::to_string));

        // Build the root router with body size limit for REST API

        let mut router = Router::new()
            .nest("/api/v1", api_v1)
            .layer(DefaultBodyLimit::max(self.config.max_http_body_size))
            .merge(swagger_ui())
            .merge(portal::router(
                self.state.clone(),
                self.config.portal_csp.clone(),
            ))
            .layer(from_fn_with_state(api_authority, redirect_swagger))
            .layer(from_fn(baseline_security_headers));
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
        self.state
            .register_rest_interface_with_public_url(bound_addr, self.api_public_url.as_deref())
            .await;
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

async fn redirect_swagger(
    State(api_authority): State<Option<String>>,
    request: Request,
    next: Next,
) -> Response {
    let is_alias = matches!(request.uri().path(), "/" | "/api/v1" | "/swagger");
    let is_api_host = api_authority.as_deref().is_some_and(|authority| {
        request
            .headers()
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .is_some_and(|host| host.eq_ignore_ascii_case(authority))
    });

    if (request.method() == Method::GET || request.method() == Method::HEAD)
        && is_alias
        && is_api_host
    {
        return Redirect::temporary("/swagger-ui/").into_response();
    }

    next.run(request).await
}
