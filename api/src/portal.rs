use crate::server_state::{PortalRuntimeState, ServerState};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tower::ServiceExt;
use tower_http::services::{ServeDir, ServeFile};

const API_BASE_URL: &str = "/api/v1";
const ASSETS_PREFIX: &str = "/assets/";
const IMMUTABLE_CACHE: &str = "public, max-age=31536000, immutable";
const NO_CACHE: &str = "no-cache";

pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/portal-config.json", get(portal_config))
        .fallback(serve_portal)
        .with_state(state)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PortalRuntimeConfig {
    api_base_url: &'static str,
}

async fn portal_config(State(state): State<Arc<ServerState>>) -> Response {
    match portal_dir(&state).await {
        Ok(_) => Json(PortalRuntimeConfig {
            api_base_url: API_BASE_URL,
        })
        .into_response(),
        Err(error) => error.into_response(),
    }
}

async fn serve_portal(State(state): State<Arc<ServerState>>, request: Request) -> Response {
    serve_portal_request(&state, request).await
}

async fn serve_portal_request(state: &ServerState, request: Request) -> Response {
    let request_path = request.uri().path().to_string();
    if is_reserved_path(&request_path) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let method = request.method().clone();
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let portal_dir = match portal_dir(state).await {
        Ok(portal_dir) => portal_dir,
        Err(error) => return error.into_response(),
    };

    let service = ServeDir::new(&portal_dir).append_index_html_on_directories(true);
    match service.oneshot(request).await {
        Ok(response) if response.status() != StatusCode::NOT_FOUND => {
            let mut response = response.into_response();
            apply_cache_headers(&mut response, &request_path, false);
            response
        }
        Ok(_) if request_path.starts_with(ASSETS_PREFIX) => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => serve_portal_index_fallback(&portal_dir, method, &request_path).await,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn serve_portal_index_fallback(
    portal_dir: &std::path::Path,
    method: Method,
    request_path: &str,
) -> Response {
    let request = Request::builder()
        .method(method)
        .uri("/index.html")
        .body(Body::empty())
        .expect("portal fallback request must build");
    match ServeFile::new(portal_dir.join("index.html"))
        .oneshot(request)
        .await
    {
        Ok(response) => {
            let mut response = response.into_response();
            apply_cache_headers(&mut response, request_path, true);
            response
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

fn apply_cache_headers(response: &mut Response, request_path: &str, fallback_to_index: bool) {
    if response.status() != StatusCode::OK {
        return;
    }

    let value = if request_path.starts_with(ASSETS_PREFIX) && !fallback_to_index {
        IMMUTABLE_CACHE
    } else {
        NO_CACHE
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(value));
}

async fn portal_dir(state: &ServerState) -> Result<PathBuf, PortalServeError> {
    let PortalRuntimeState { status, portal_dir } = state.portal_runtime_state().await;

    if let Some(portal_dir) = portal_dir {
        return Ok(portal_dir);
    }

    if status.mode == "artifact" {
        Err(PortalServeError::Unavailable)
    } else {
        Err(PortalServeError::NotFound)
    }
}

fn is_reserved_path(path: &str) -> bool {
    matches!(path, "/api" | "/api/" | "/info" | "/info/" | "/s3" | "/s3/")
        || path.starts_with("/api/")
        || path.starts_with("/info/")
        || path.starts_with("/s3/")
        || path == "/swagger-ui"
        || path.starts_with("/swagger-ui/")
        || path == "/api-docs"
        || path.starts_with("/api-docs/")
        || path == "/.well-known"
        || path.starts_with("/.well-known/")
}

enum PortalServeError {
    NotFound,
    Unavailable,
}

impl IntoResponse for PortalServeError {
    fn into_response(self) -> Response {
        match self {
            PortalServeError::NotFound => StatusCode::NOT_FOUND,
            PortalServeError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
        .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{IMMUTABLE_CACHE, NO_CACHE, serve_portal_request};
    use crate::server_state::{PortalStatus, ServerState};
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use axum::body::{Body, to_bytes};
    use axum::extract::Request;
    use axum::http::{Method, StatusCode, header};
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};

    async fn setup_state() -> (Arc<ServerState>, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
            )
            .await,
        );

        (state, tempdir)
    }

    async fn enable_portal(state: &ServerState, dir: &std::path::Path) {
        state
            .set_portal_dir(
                PortalStatus {
                    installed: true,
                    mode: "artifact".to_string(),
                    version: Some("test".to_string()),
                    source: None,
                    url: Some("https://example.test/portal.tar.gz".to_string()),
                    checksum: Some("0".repeat(64)),
                    fetched_at: None,
                    last_error: None,
                },
                dir.to_path_buf(),
            )
            .await;
    }

    fn request(method: Method, path: &str) -> Request {
        Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn portal_serves_cached_index_and_static_assets() {
        let (state, tempdir) = setup_state().await;
        let portal_dir = tempdir.path().join("portal");
        std::fs::create_dir_all(portal_dir.join("assets")).unwrap();
        std::fs::write(portal_dir.join("index.html"), "<html>portal</html>").unwrap();
        std::fs::write(portal_dir.join("assets/app.js"), "console.log('portal');").unwrap();
        enable_portal(&state, &portal_dir).await;

        let index = serve_portal_request(&state, request(Method::GET, "/")).await;
        assert_eq!(index.status(), StatusCode::OK);
        assert_eq!(
            index.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html"
        );
        assert_eq!(
            index.headers().get(header::CACHE_CONTROL).unwrap(),
            NO_CACHE
        );
        let body = to_bytes(index.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"<html>portal</html>");

        let asset = serve_portal_request(&state, request(Method::GET, "/assets/app.js")).await;
        assert_eq!(asset.status(), StatusCode::OK);
        assert_eq!(
            asset.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/javascript"
        );
        assert_eq!(
            asset.headers().get(header::CACHE_CONTROL).unwrap(),
            IMMUTABLE_CACHE
        );
        let body = to_bytes(asset.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"console.log('portal');");
    }

    #[tokio::test]
    async fn portal_falls_back_to_index_for_client_routes() {
        let (state, tempdir) = setup_state().await;
        let portal_dir = tempdir.path().join("portal");
        std::fs::create_dir_all(&portal_dir).unwrap();
        std::fs::write(portal_dir.join("index.html"), "<html>portal</html>").unwrap();
        enable_portal(&state, &portal_dir).await;

        let response =
            serve_portal_request(&state, request(Method::GET, "/groups/test-group")).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            NO_CACHE
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"<html>portal</html>");
    }

    #[tokio::test]
    async fn missing_assets_do_not_fall_back_to_index() {
        let (state, tempdir) = setup_state().await;
        let portal_dir = tempdir.path().join("portal");
        std::fs::create_dir_all(&portal_dir).unwrap();
        std::fs::write(portal_dir.join("index.html"), "<html>portal</html>").unwrap();
        enable_portal(&state, &portal_dir).await;

        let response =
            serve_portal_request(&state, request(Method::GET, "/assets/missing.js")).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response.headers().get(header::CACHE_CONTROL).is_none());
    }

    #[tokio::test]
    async fn portal_config_returns_api_base_url_when_portal_is_available() {
        let (state, tempdir) = setup_state().await;
        let portal_dir = tempdir.path().join("portal");
        std::fs::create_dir_all(&portal_dir).unwrap();
        std::fs::write(portal_dir.join("index.html"), "<html>portal</html>").unwrap();
        enable_portal(&state, &portal_dir).await;

        let response = super::portal_config(axum::extract::State(state)).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let config: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(config, serde_json::json!({ "apiBaseUrl": "/api/v1" }));
    }

    #[tokio::test]
    async fn portal_returns_not_found_when_disabled_and_unavailable_when_artifact_is_not_ready() {
        let (state, _tempdir) = setup_state().await;

        let disabled = serve_portal_request(&state, request(Method::GET, "/")).await;
        assert_eq!(disabled.status(), StatusCode::NOT_FOUND);

        state
            .set_portal_status(PortalStatus {
                installed: false,
                mode: "artifact".to_string(),
                version: None,
                source: None,
                url: Some("https://example.test/portal.tar.gz".to_string()),
                checksum: Some("0".repeat(64)),
                fetched_at: None,
                last_error: None,
            })
            .await;

        let unavailable = serve_portal_request(&state, request(Method::GET, "/")).await;
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn portal_fallback_does_not_shadow_reserved_routes() {
        let (state, tempdir) = setup_state().await;
        let portal_dir = tempdir.path().join("portal");
        std::fs::create_dir_all(&portal_dir).unwrap();
        std::fs::write(portal_dir.join("index.html"), "<html>portal</html>").unwrap();
        enable_portal(&state, &portal_dir).await;

        for path in [
            "/api/v1/missing",
            "/api-docs/openapi.json",
            "/swagger-ui/missing",
            "/s3/missing",
            "/info",
            "/.well-known/openid-configuration",
        ] {
            let response = serve_portal_request(&state, request(Method::GET, path)).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert!(body.is_empty(), "{path}");
        }

        let response = serve_portal_request(&state, request(Method::POST, "/api/v1/missing")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
