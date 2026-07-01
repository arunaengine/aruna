use crate::server_state::{PortalRuntimeState, ServerState};
use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use percent_encoding::percent_decode_str;
use serde::Serialize;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const API_BASE_URL: &str = "/api/v1";

pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/", get(serve_portal_index))
        .route("/portal-config.json", get(portal_config))
        .fallback(serve_portal_fallback)
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

async fn serve_portal_index(State(state): State<Arc<ServerState>>, method: Method) -> Response {
    serve_portal_request(&state, &method, "/").await
}

async fn serve_portal_fallback(
    State(state): State<Arc<ServerState>>,
    method: Method,
    uri: Uri,
) -> Response {
    serve_portal_request(&state, &method, uri.path()).await
}

async fn serve_portal_request(state: &ServerState, method: &Method, path: &str) -> Response {
    if is_reserved_path(path) {
        return StatusCode::NOT_FOUND.into_response();
    }

    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let portal_dir = match portal_dir(state).await {
        Ok(portal_dir) => portal_dir,
        Err(error) => return error.into_response(),
    };
    let requested_path = match safe_portal_path(&portal_dir, path) {
        Some(path) => path,
        None => return StatusCode::NOT_FOUND.into_response(),
    };

    let (served_path, bytes) = match read_portal_file(&requested_path).await {
        Ok(Some(bytes)) => (requested_path, bytes),
        Ok(None) => {
            let index_path = portal_dir.join("index.html");
            match read_portal_file(&index_path).await {
                Ok(Some(bytes)) => (index_path, bytes),
                Ok(None) | Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            }
        }
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };

    file_response(method, &served_path, bytes)
}

async fn portal_dir(state: &ServerState) -> Result<PathBuf, PortalServeError> {
    let PortalRuntimeState {
        status,
        artifact_dir,
    } = state.portal_runtime_state().await;

    if let Some(artifact_dir) = artifact_dir {
        return Ok(artifact_dir);
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
}

fn safe_portal_path(root: &Path, request_path: &str) -> Option<PathBuf> {
    let raw_path = request_path.strip_prefix('/').unwrap_or(request_path);
    if raw_path.is_empty() {
        return Some(root.join("index.html"));
    }

    let decoded = percent_decode_str(raw_path).decode_utf8().ok()?;
    if decoded.contains('\0') {
        return None;
    }

    let mut relative = PathBuf::new();
    for component in Path::new(decoded.as_ref()).components() {
        match component {
            Component::Normal(part) => {
                if part.to_str().is_none_or(|segment| segment.starts_with('.')) {
                    return None;
                }
                relative.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if relative.as_os_str().is_empty() {
        Some(root.join("index.html"))
    } else {
        Some(root.join(relative))
    }
}

async fn read_portal_file(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() => tokio::fs::read(path).await.map(Some),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn file_response(method: &Method, path: &Path, bytes: Vec<u8>) -> Response {
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(bytes)
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type(path))
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("css") => "text/css; charset=utf-8",
        Some("gif") => "image/gif",
        Some("html") => "text/html; charset=utf-8",
        Some("ico") => "image/x-icon",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("js" | "mjs") => "application/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        Some("webp") => "image/webp",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
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
    use super::serve_portal_request;
    use crate::server_state::{PortalStatus, ServerState};
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use axum::body::to_bytes;
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
            .set_portal_artifact(
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

    #[tokio::test]
    async fn portal_serves_cached_index_and_static_assets() {
        let (state, tempdir) = setup_state().await;
        let portal_dir = tempdir.path().join("portal");
        std::fs::create_dir_all(portal_dir.join("assets")).unwrap();
        std::fs::write(portal_dir.join("index.html"), "<html>portal</html>").unwrap();
        std::fs::write(portal_dir.join("assets/app.js"), "console.log('portal');").unwrap();
        enable_portal(&state, &portal_dir).await;

        let index = serve_portal_request(&state, &Method::GET, "/").await;
        assert_eq!(index.status(), StatusCode::OK);
        assert_eq!(
            index.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        let body = to_bytes(index.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"<html>portal</html>");

        let asset = serve_portal_request(&state, &Method::GET, "/assets/app.js").await;
        assert_eq!(asset.status(), StatusCode::OK);
        assert_eq!(
            asset.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/javascript; charset=utf-8"
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

        let response = serve_portal_request(&state, &Method::GET, "/groups/test-group").await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"<html>portal</html>");
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

        let disabled = serve_portal_request(&state, &Method::GET, "/").await;
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

        let unavailable = serve_portal_request(&state, &Method::GET, "/").await;
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
        ] {
            let response = serve_portal_request(&state, &Method::GET, path).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert!(body.is_empty(), "{path}");
        }

        let response = serve_portal_request(&state, &Method::POST, "/api/v1/missing").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
