use crate::cors::CorsConfig;
use crate::csp::baseline_security_headers;
use crate::error::ServerSetupError;
use crate::routes::rest_router;
pub(crate) use crate::server_state::{ServerState, swagger_ui};
use axum::Router;
use axum::extract::{DefaultBodyLimit, MatchedPath, Request};
use axum::http::{Method, StatusCode};
use axum::middleware::{Next, from_fn, from_fn_with_state};
use axum::response::{IntoResponse, Redirect, Response};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_MAX_HTTP_BODY_SIZE: usize = 1024 * 1024;

// Backstop only, far above any legitimate request: the interactive bounds live
// in the discovery/fanout/open_stream deadlines. This catches handler paths
// that would otherwise hold the connection for unbounded peer I/O. Streaming
// response bodies (SSE, archive downloads) are not covered — the layer bounds
// the time to produce the response, not the body. Routes that read the request
// body inside the handler are exempt, see TIMEOUT_EXEMPT_ROUTES.
const REST_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

// Body-streaming routes: upload duration counts against the handler, so a
// deadline here truncates legitimate slow transfers.
const TIMEOUT_EXEMPT_ROUTES: &[&str] = &["/metadata/rocrate/uploads"];

#[derive(Clone, Debug)]
pub struct Server {
    state: Arc<ServerState>,
    config: ServerConfig,
    api_public_url: Option<String>,
    mcp_enabled: bool,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
    pub max_http_body_size: usize,
    pub cors: CorsConfig,
}

impl Server {
    pub fn new(state: Arc<ServerState>, config: ServerConfig) -> Self {
        Self {
            state,
            config,
            api_public_url: None,
            mcp_enabled: true,
        }
    }

    pub fn with_api_public_url(mut self, api_public_url: Option<String>) -> Self {
        self.api_public_url = api_public_url;
        self
    }

    pub fn with_mcp_enabled(mut self, mcp_enabled: bool) -> Self {
        self.mcp_enabled = mcp_enabled;
        self
    }

    pub fn build_router(&self) -> Router {
        // Build the main API router
        let api_v1 = Router::new()
            .merge(rest_router(self.state.clone()))
            .layer(from_fn(rest_timeout));

        // Build the root router with body size limit for REST API

        let mut router = Router::new()
            .nest("/api/v1", api_v1)
            .layer(DefaultBodyLimit::max(self.config.max_http_body_size))
            .merge(swagger_ui())
            .layer(from_fn(redirect_swagger))
            .layer(from_fn(baseline_security_headers));
        if self.mcp_enabled {
            router = router.merge(crate::mcp::router(
                self.state.clone(),
                &self.config.cors,
                self.api_public_url.as_deref(),
            ));
        }
        if let Some(cors_layer) = self.config.cors.rest_layer() {
            router = router.layer(cors_layer);
        }
        // Outermost: the IP limiter runs before CORS, auth, and extraction so an
        // invalid or expensive attempt still consumes IP capacity.
        router.layer(from_fn_with_state(
            self.state.clone(),
            crate::rate_limit::ip_middleware,
        ))
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), ServerSetupError> {
        let listener = TcpListener::bind(self.config.http_addr).await?;
        self.run_with_listener(listener, shutdown).await
    }

    /// Serves until `shutdown` is cancelled: the listener stops accepting and
    /// requests already in flight run to completion before this returns.
    pub async fn run_with_listener(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), ServerSetupError> {
        let bound_addr = listener.local_addr()?;
        self.state
            .register_rest_interface_with_public_url(bound_addr, self.api_public_url.as_deref())
            .await;
        if self.mcp_enabled {
            self.state.register_mcp_interface().await;
        }
        let abort_requests = CancellationToken::new();
        let request_abort = abort_requests.clone();
        let _abort_requests = abort_requests.drop_guard();
        let router = self
            .build_router()
            .layer(from_fn(move |request: Request, next: Next| {
                let request_abort = request_abort.clone();
                async move {
                    tokio::select! {
                        biased;
                        _ = request_abort.cancelled() => {
                            StatusCode::SERVICE_UNAVAILABLE.into_response()
                        }
                        response = next.run(request) => response,
                    }
                }
            }));

        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .map_err(|e| ServerSetupError::Runtime(e.to_string()))?;

        Ok(())
    }
}

fn is_exempt(matched: Option<&str>) -> bool {
    matched.is_some_and(|path| {
        TIMEOUT_EXEMPT_ROUTES
            .iter()
            .any(|route| path == *route || path.ends_with(route))
    })
}

async fn rest_timeout(request: Request, next: Next) -> Response {
    let matched = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string());
    if is_exempt(matched.as_deref()) {
        return next.run(request).await;
    }
    match tokio::time::timeout(REST_REQUEST_TIMEOUT, next.run(request)).await {
        Ok(response) => response,
        Err(_) => StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}

async fn redirect_swagger(request: Request, next: Next) -> Response {
    let is_alias = matches!(request.uri().path(), "/" | "/api/v1" | "/swagger");
    if (request.method() == Method::GET || request.method() == Method::HEAD) && is_alias {
        return Redirect::temporary("/swagger-ui/").into_response();
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MAX_HTTP_BODY_SIZE, Server, ServerConfig, TIMEOUT_EXEMPT_ROUTES, is_exempt,
    };
    use axum::Router;
    use axum::body::Body;
    use axum::extract::MatchedPath;
    use axum::http::{Method, Request, StatusCode, header};
    use axum::middleware::{Next, from_fn};
    use axum::routing::{get, post};
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    #[tokio::test]
    async fn limiter_precedes_auth() {
        // An unauthenticated request must consume IP capacity: the gate sits
        // outside auth, so router edits that move it inward break this.
        use crate::rate_limit::ApiRateLimits;
        use crate::server_state::ServerState;
        use aruna_core::structs::{NodeCapabilities, RealmId};

        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[5u8; 32])
                .verifying_key()
                .to_bytes(),
        );
        let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let driver_ctx = Arc::new(aruna_operations::driver::DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await
            .with_rate_limits(ApiRateLimits::for_test(2)),
        );

        let router = Server::new(
            state,
            ServerConfig {
                http_addr: "127.0.0.1:0".parse().unwrap(),
                max_http_body_size: DEFAULT_MAX_HTTP_BODY_SIZE,
                cors: crate::cors::CorsConfig::default(),
            },
        )
        .build_router();

        let request = || {
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/users/credentials")
                .body(Body::empty())
                .unwrap()
        };
        let first = router.clone().oneshot(request()).await.unwrap();
        assert_ne!(first.status(), StatusCode::TOO_MANY_REQUESTS);
        let _second = router.clone().oneshot(request()).await.unwrap();
        let third = router.oneshot(request()).await.unwrap();
        assert_eq!(third.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn root_redirects_swagger() {
        // The portal has its own listener, so the API origin no longer depends
        // on a Host header to decide that `/` belongs to swagger.
        let router = Router::new()
            .route("/swagger-ui/", get(|| async { StatusCode::OK }))
            .layer(from_fn(super::redirect_swagger));

        for path in ["/", "/api/v1", "/swagger"] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header(header::HOST, "portal.test")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{path}");
            assert_eq!(
                response.headers().get(header::LOCATION).unwrap(),
                "/swagger-ui/"
            );
        }
    }

    #[test]
    fn matches_exempt_route() {
        assert!(is_exempt(Some("/metadata/rocrate/uploads")));
        assert!(is_exempt(Some("/api/v1/metadata/rocrate/uploads")));
        assert!(!is_exempt(Some("/api/v1/metadata/rocrate/imports")));
        assert!(!is_exempt(None));
    }

    #[tokio::test]
    async fn nested_matched_path() {
        // The timeout layer sits inside the /api/v1 nest and must still see the
        // route template that drives the exemption.
        let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let probe = seen.clone();
        let inner = Router::new()
            .route(TIMEOUT_EXEMPT_ROUTES[0], post(|| async { StatusCode::OK }))
            .layer(from_fn(move |request: Request<Body>, next: Next| {
                let probe = probe.clone();
                async move {
                    let matched = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(|matched| matched.as_str().to_string());
                    *probe.lock().unwrap() = matched;
                    next.run(request).await
                }
            }));

        let response = Router::new()
            .nest("/api/v1", inner)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/metadata/rocrate/uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(is_exempt(seen.lock().unwrap().as_deref()));
    }
}
