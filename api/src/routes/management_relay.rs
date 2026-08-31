//! Transparent relay of management-only REST routes.
//!
//! A caller must not need to know which node kind serves their portal. A
//! management-only route that reaches a node of another kind is re-issued
//! against a management node and its answer is passed back verbatim.

use crate::error::ServerError;
use crate::routes::info::{load_node_info_documents_best_effort, management_node_urls};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_operations::device::realm_documents::installed_management_urls;
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use axum::body::Bytes;
use axum::extract::{FromRequest, MatchedPath, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Loop guard: a request that already carries a hop is answered by the node it
/// reached, never relayed again.
pub(crate) const RELAY_HOP_HEADER: HeaderName = HeaderName::from_static("x-aruna-relay-hop");

/// The nest every REST route is served under, stripped before the path is
/// appended to a peer's published api base url.
const API_PREFIX: &str = "/api/v1";

/// Realm membership changes rarely, so the resolved targets are reused inside
/// this window instead of re-reading realm state per request.
const MANAGEMENT_URL_TTL: Duration = Duration::from_secs(60);
const RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RELAY_TIMEOUT: Duration = Duration::from_secs(30);
const RELAY_TARGET_LIMIT: usize = 3;

/// Management-only routes, by method and route template. Explicit by design:
/// node-local routes such as compute drain, placement diagnostics and sync
/// quarantine must keep answering on the node they were called on.
const RELAYED_ROUTES: &[(&str, &str)] = &[
    ("DELETE", "/admin/devices/{node_id}"),
    ("DELETE", "/admin/onboarding/secrets/{id}"),
    ("DELETE", "/users/me/devices/{id}"),
    ("GET", "/admin/onboarding/secrets"),
    ("GET", "/info/realm/placement"),
    ("GET", "/onboarding/secrets/{id}/status"),
    ("GET", "/users/token"),
    ("PATCH", "/info/realm/placement"),
    ("POST", "/admin/onboarding/secrets"),
    ("POST", "/onboarding/bootstrap"),
    ("POST", "/users/sessions"),
    ("PUT", "/admin/compute/config"),
    ("PUT", "/info/realm/quota"),
    ("PUT", "/policies/realm"),
];

static RELAY_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(RELAY_CONNECT_TIMEOUT)
        .timeout(RELAY_TIMEOUT)
        .build()
        .unwrap_or_default()
});

/// Management api urls this node last resolved, and when.
#[derive(Debug, Default)]
pub struct ManagementUrlCache {
    urls: Vec<String>,
    refreshed_at: Option<Instant>,
}

pub(crate) async fn relay_middleware(
    State(state): State<Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    let matched = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string());
    let route = relay_route(
        request.method(),
        matched.as_deref(),
        state.is_management_node(),
        request.headers().contains_key(RELAY_HOP_HEADER),
    );
    match route {
        Some(route) => relay(&state, route, request).await,
        None => next.run(request).await,
    }
}

/// The allowlisted route this request must be relayed for, if any.
fn relay_route(
    method: &Method,
    matched: Option<&str>,
    is_management: bool,
    has_hop: bool,
) -> Option<&'static str> {
    if is_management || has_hop {
        return None;
    }
    let matched = matched?;
    let path = matched.strip_prefix(API_PREFIX).unwrap_or(matched);
    RELAYED_ROUTES
        .iter()
        .find(|(route_method, route)| method.as_str() == *route_method && path == *route)
        .map(|(_, route)| *route)
}

async fn relay(state: &Arc<ServerState>, route: &'static str, request: Request) -> Response {
    let targets = management_targets(state).await;
    if targets.is_empty() {
        warn!(route, "No management node is known for a relayed route");
        return ServerError::NoManagementNode.into_response();
    }

    let method = request.method().clone();
    let uri = request.uri().clone();
    let authorization = request.headers().get(header::AUTHORIZATION).cloned();
    let content_type = request.headers().get(header::CONTENT_TYPE).cloned();
    let body = match Bytes::from_request(request, &()).await {
        Ok(body) => body,
        Err(rejection) => return rejection.into_response(),
    };
    let hop = HeaderValue::from_str(&state.get_node_id().to_string())
        .unwrap_or_else(|_| HeaderValue::from_static("relayed"));

    // Targets are tried in order, but only a failure that provably predates
    // processing may move a non-idempotent request on to the next target.
    let mut unknown = None;
    for target in targets.iter().take(RELAY_TARGET_LIMIT) {
        let url = relay_url(target, &uri);
        let mut outgoing = RELAY_CLIENT
            .request(method.clone(), &url)
            .header(RELAY_HOP_HEADER, hop.clone())
            .body(body.clone());
        if let Some(authorization) = &authorization {
            outgoing = outgoing.header(header::AUTHORIZATION, authorization.clone());
        }
        if let Some(content_type) = &content_type {
            outgoing = outgoing.header(header::CONTENT_TYPE, content_type.clone());
        }
        match outgoing.send().await {
            Ok(response) if may_try_response(route, response.status()) => {
                unknown = Some((url, response));
            }
            Ok(response) => return relayed_response(route, &url, response).await,
            Err(error) => {
                let is_connect = error.is_connect();
                warn!(route, relay_target = %url, is_connect, error = %error, "Management relay target failed");
                if !may_try_next(&method, is_connect) {
                    return ServerError::RelayFailed.into_response();
                }
            }
        }
    }

    if let Some((url, response)) = unknown {
        return relayed_response(route, &url, response).await;
    }

    warn!(route, "No management node answered a relayed route");
    ServerError::NoManagementNode.into_response()
}

/// Whether a send failure may be retried against the next target. A connect
/// failure, which reqwest also reports for a connect timeout, provably predates
/// any processing; later failures are only safe to repeat for an idempotent method.
fn may_try_next(method: &Method, is_connect: bool) -> bool {
    is_connect || method == Method::GET
}

/// Whether a node-local enrollment miss may be tried on the next target.
fn may_try_response(route: &str, status: StatusCode) -> bool {
    matches!(
        (route, status),
        (
            "/admin/onboarding/secrets/{id}"
                | "/onboarding/secrets/{id}/status"
                | "/users/me/devices/{id}",
            StatusCode::NOT_FOUND
        ) | ("/onboarding/bootstrap", StatusCode::UNAUTHORIZED)
    )
}

/// Published api urls are bare origins (`API_PUBLIC_URL`), so the nest comes
/// from the incoming path; a target that already carries it contributes it.
fn relay_url(target: &str, uri: &Uri) -> String {
    let base = target.trim_end_matches('/');
    let path = uri.path();
    let suffix = match base.ends_with(API_PREFIX) {
        true => path.strip_prefix(API_PREFIX).unwrap_or(path),
        false => path,
    };
    match uri.query() {
        Some(query) => format!("{base}{suffix}?{query}"),
        None => format!("{base}{suffix}"),
    }
}

async fn relayed_response(route: &'static str, url: &str, response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = relayed_headers(response.headers());
    match response.bytes().await {
        Ok(body) => {
            debug!(route, relay_target = %url, status = status.as_u16(), "Relayed a management route");
            let mut relayed = (status, body).into_response();
            relayed.headers_mut().remove(header::CONTENT_TYPE);
            for (name, value) in headers {
                relayed.headers_mut().insert(name, value);
            }
            relayed
        }
        Err(error) => {
            warn!(route, relay_target = %url, error = %error, "Management relay response failed");
            ServerError::BadGateway.into_response()
        }
    }
}

fn relayed_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    [&header::CONTENT_TYPE, &header::RETRY_AFTER]
        .into_iter()
        .filter_map(|name| {
            headers
                .get(name)
                .cloned()
                .map(|value| (name.clone(), value))
        })
        .collect()
}

/// Cached management targets; a stale window triggers a realm-config read, and
/// a failed read reuses the last known set rather than dropping every target.
async fn management_targets(state: &Arc<ServerState>) -> Vec<String> {
    let cache = state.management_url_cache();
    {
        let cached = cache.read().await;
        if is_fresh(cached.refreshed_at) {
            return cached.urls.clone();
        }
    }

    let mut cached = cache.write().await;
    if is_fresh(cached.refreshed_at) {
        return cached.urls.clone();
    }
    match drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(config) => {
            let documents = load_node_info_documents_best_effort(state, &config).await;
            let peers = peer_management_urls(state.get_node_id(), &config, &documents);
            let installed = installed_management_urls(&state.get_ctx(), state.get_realm_id()).await;
            cached.urls = relay_targets(peers, installed);
        }
        Err(error) => debug!(error = %error, "Management relay reuses cached management urls"),
    }
    cached.refreshed_at = Some(Instant::now());
    cached.urls.clone()
}

fn is_fresh(refreshed_at: Option<Instant>) -> bool {
    refreshed_at.is_some_and(|refreshed_at| refreshed_at.elapsed() < MANAGEMENT_URL_TTL)
}

/// The targets a relayed route is tried against. A device holds no peer
/// node-info document, so the list a realm node installed on it is the only
/// address it has for a management node.
fn relay_targets(peers: Vec<String>, installed: Vec<String>) -> Vec<String> {
    match peers.is_empty() {
        true => installed,
        false => peers,
    }
}

/// Management peers in node-id order, this node excluded. The order is stable
/// so repeated calls pin the same peer: an onboarding secret is minted into one
/// management node's local store and its status and revoke must return there.
fn peer_management_urls(
    current: NodeId,
    config: &aruna_core::structs::RealmConfigDocument,
    documents: &BTreeMap<NodeId, aruna_core::structs::NodeInfoDocument>,
) -> Vec<String> {
    let ordered: BTreeMap<NodeId, String> = management_node_urls(config, documents)
        .into_iter()
        .filter(|(node_id, _)| *node_id != current)
        .filter_map(|(node_id, url)| url.map(|url| (node_id, url)))
        .collect();
    let mut urls: Vec<String> = Vec::new();
    for url in ordered.into_values() {
        if !urls.contains(&url) {
            urls.push(url);
        }
    }
    urls
}

#[cfg(test)]
mod tests {
    use super::{
        API_PREFIX, RELAYED_ROUTES, may_try_next, may_try_response, relay_route, relay_targets,
        relay_url, relayed_headers,
    };
    use crate::error::{ErrorResponse, ServerError};
    use axum::body::to_bytes;
    use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
    use axum::response::IntoResponse;

    #[test]
    fn matches_allowlisted_routes() {
        assert_eq!(
            relay_route(&Method::PUT, Some("/api/v1/info/realm/quota"), false, false),
            Some("/info/realm/quota")
        );
        assert_eq!(
            relay_route(
                &Method::DELETE,
                Some("/api/v1/admin/onboarding/secrets/{id}"),
                false,
                false
            ),
            Some("/admin/onboarding/secrets/{id}")
        );
        assert_eq!(
            relay_route(&Method::GET, Some("/api/v1/users/token"), false, false),
            Some("/users/token")
        );
        assert_eq!(
            relay_route(&Method::POST, Some("/api/v1/users/sessions"), false, false),
            Some("/users/sessions")
        );
        // Node-local admin routes stay on the node they were called on.
        assert_eq!(
            relay_route(
                &Method::POST,
                Some("/api/v1/admin/compute/drain"),
                false,
                false
            ),
            None
        );
        assert_eq!(
            relay_route(
                &Method::GET,
                Some("/api/v1/admin/placement-diagnostics"),
                false,
                false
            ),
            None
        );
        // The method is part of the match: only PUT on the quota route relays.
        assert_eq!(
            relay_route(&Method::GET, Some("/api/v1/info/realm/quota"), false, false),
            None
        );
    }

    #[test]
    fn hop_header_stops_relay() {
        assert_eq!(
            relay_route(&Method::PUT, Some("/api/v1/info/realm/quota"), false, true),
            None
        );
    }

    #[test]
    fn management_node_answers_itself() {
        assert_eq!(
            relay_route(&Method::PUT, Some("/api/v1/info/realm/quota"), true, false),
            None
        );
    }

    #[test]
    fn builds_target_url() {
        // Node info documents publish `API_PUBLIC_URL` as a bare origin.
        let uri: Uri = "/api/v1/admin/onboarding/secrets?limit=5".parse().unwrap();
        assert_eq!(
            relay_url("http://127.0.0.1:43001", &uri),
            "http://127.0.0.1:43001/api/v1/admin/onboarding/secrets?limit=5"
        );
        assert_eq!(
            relay_url("https://mgmt.example.test/api/v1/", &uri),
            "https://mgmt.example.test/api/v1/admin/onboarding/secrets?limit=5"
        );
    }

    #[test]
    fn uses_installed_urls() {
        // A device resolves no peer from node-info documents it never holds.
        let installed = vec!["https://mgmt.example.test/api/v1".to_string()];
        assert_eq!(
            relay_targets(Vec::new(), installed.clone()),
            installed,
            "a device relays to the list its realm installed"
        );
        let peers = vec!["https://peer.example.test/api/v1".to_string()];
        assert_eq!(
            relay_targets(peers.clone(), installed),
            peers,
            "a resolved peer is never displaced by an installed copy"
        );
        assert!(relay_targets(Vec::new(), Vec::new()).is_empty());
    }

    #[test]
    fn connect_failure_advances() {
        assert!(may_try_next(&Method::POST, true));
        assert!(may_try_next(&Method::GET, true));
    }

    #[test]
    fn enrollment_miss_advances() {
        assert!(may_try_response(
            "/admin/onboarding/secrets/{id}",
            StatusCode::NOT_FOUND
        ));
        assert!(may_try_response(
            "/onboarding/secrets/{id}/status",
            StatusCode::NOT_FOUND
        ));
        assert!(may_try_response(
            "/users/me/devices/{id}",
            StatusCode::NOT_FOUND
        ));
        assert!(may_try_response(
            "/onboarding/bootstrap",
            StatusCode::UNAUTHORIZED
        ));
        assert!(!may_try_response(
            "/admin/onboarding/secrets",
            StatusCode::NOT_FOUND
        ));
    }

    #[test]
    fn keeps_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RETRY_AFTER, HeaderValue::from_static("7"));
        headers.insert(header::SET_COOKIE, HeaderValue::from_static("secret=value"));

        assert_eq!(
            relayed_headers(&headers),
            vec![(header::RETRY_AFTER, HeaderValue::from_static("7"))]
        );
    }

    #[test]
    fn get_failure_advances() {
        assert!(may_try_next(&Method::GET, false));
    }

    #[tokio::test]
    async fn post_failure_stops() {
        // A failure after the connect succeeded may already have minted a secret.
        assert!(!may_try_next(&Method::POST, false));
        assert!(!may_try_next(&Method::DELETE, false));

        let response = ServerError::RelayFailed.into_response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body: ErrorResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body.code.as_deref(), Some("relay_failed"));
    }

    #[test]
    fn allowlist_matches_router() {
        // A renamed or removed route must not leave a dead allowlist entry
        // behind, silently turning a relayed route into a local 403.
        let documented = crate::routes::rest_openapi().paths.paths;
        for (method, route) in RELAYED_ROUTES {
            let item = documented
                .get(*route)
                .unwrap_or_else(|| panic!("{route} is not a registered route"));
            let registered = match *method {
                "DELETE" => item.delete.is_some(),
                "GET" => item.get.is_some(),
                "PATCH" => item.patch.is_some(),
                "POST" => item.post.is_some(),
                "PUT" => item.put.is_some(),
                other => panic!("{other} is not covered by the allowlist check"),
            };
            assert!(registered, "{method} {route} is not a registered route");
            assert!(
                relay_route(
                    &Method::from_bytes(method.as_bytes()).expect("valid method"),
                    Some(&format!("{API_PREFIX}{route}")),
                    false,
                    false
                )
                .is_some(),
                "{method} {route} must match the relay allowlist"
            );
        }
    }
}

#[cfg(test)]
mod route_tests {
    use std::sync::Arc;

    use super::{RELAY_HOP_HEADER, relay_middleware};
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{DEVICE_MANAGEMENT_URL_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::{
        Actor, NodeCapabilities, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use ed25519_dalek::SigningKey;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tower::ServiceExt;
    use ulid::Ulid;

    struct Fixture {
        _dir: TempDir,
        state: Arc<ServerState>,
    }

    async fn write_row(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn setup(installed: Vec<String>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes(
            SigningKey::from_bytes(&[7u8; 32])
                .verifying_key()
                .to_bytes(),
        );
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let owner = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
        let state = Arc::new(
            ServerState::new(
                Arc::new(DriverContext {
                    storage_handle: storage,
                    net_handle: None,
                    blob_handle: None,
                    metadata_handle: None,
                    task_handle: Some(TaskHandle::new()),
                    compute_handle: None,
                }),
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await,
        );
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(node_id, RealmNodeKind::User { owner });
        write_row(
            &state,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            config.to_bytes(&actor).unwrap(),
        )
        .await;
        if !installed.is_empty() {
            write_row(
                &state,
                DEVICE_MANAGEMENT_URL_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                postcard::to_allocvec(&installed).unwrap(),
            )
            .await;
        }
        Fixture { _dir: dir, state }
    }

    fn relay_app(state: Arc<ServerState>) -> Router {
        Router::new()
            .route("/users/token", get(|| async { StatusCode::IM_A_TEAPOT }))
            .layer(from_fn_with_state(state.clone(), relay_middleware))
            .with_state(state)
    }

    async fn fake_upstream() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(|| async {
            (
                [(header::CONTENT_TYPE, "application/json")],
                "{\"relayed\":true}",
            )
        });
        let handle = tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });
        (format!("http://{address}"), handle)
    }

    fn token_request() -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri("/users/token")
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn relays_to_management() {
        let (upstream, handle) = fake_upstream().await;
        let fixture = setup(vec![upstream]).await;
        let response = relay_app(fixture.state.clone())
            .oneshot(token_request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"{\"relayed\":true}");
        handle.abort();
    }

    #[tokio::test]
    async fn no_management_target() {
        // A resolved realm config with no reachable management node answers 503.
        let fixture = setup(Vec::new()).await;
        let response = relay_app(fixture.state.clone())
            .oneshot(token_request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn hop_stays_local() {
        // A request already carrying a hop is answered locally, never relayed on.
        let fixture = setup(vec!["http://127.0.0.1:1".to_string()]).await;
        let request = Request::builder()
            .method("GET")
            .uri("/users/token")
            .header(RELAY_HOP_HEADER, "peer")
            .body(Body::empty())
            .unwrap();
        let response = relay_app(fixture.state.clone())
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
    }
}
