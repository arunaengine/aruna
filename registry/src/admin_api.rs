//! Admin routes under `/v1/admin` and the admin page under `/admin`. Every admin route checks
//! the caller with [`AdminAuth`]; a registry without admin sign-in answers 404 there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::federation::{Registration, Signed};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_secs;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;
use tracing::warn;

use crate::admin::{AdminState, AuditRecord};
use crate::store::{NameRecord, StoreError, is_stale, leased};
use crate::verify::RouteProblem;
use crate::{RegistryState, parse_realm};

/// Environment variable holding the admin bearer token, set from a Kubernetes Secret.
pub const TOKEN_VAR: &str = "ARUNA_REGISTRY_ADMIN_TOKEN";
/// Shortest accepted admin token, so a guessable value never guards the admin API.
pub const MIN_TOKEN_LEN: usize = 32;
/// Actor name recorded in audit records for changes made with the shared token.
const TOKEN_ACTOR: &str = "admin token";

const PAGE: &str = include_str!("../ui/admin.html");
const SCRIPT: &str = include_str!("../ui/admin.js");
const STYLE: &str = include_str!("../ui/admin.css");
const PAGE_POLICY: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
    connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; \
    frame-ancestors 'none'";

/// How admin callers sign in. An OIDC variant can join the token without changing the routes.
pub enum AdminAuth {
    Disabled,
    /// SHA-256 of the shared token, compared in constant time.
    Token([u8; 32]),
}

#[derive(Debug, Error)]
pub enum AuthSetupError {
    #[error("{TOKEN_VAR} must hold at least {MIN_TOKEN_LEN} characters")]
    ShortToken,
}

/// The signed-in admin, named in audit records.
#[derive(Debug, Serialize)]
pub struct Admin {
    pub subject: String,
    pub method: &'static str,
}

impl AdminAuth {
    /// Admin sign-in from the token variable; unset or empty disables the admin API.
    pub fn from_env() -> Result<Self, AuthSetupError> {
        match std::env::var(TOKEN_VAR) {
            Ok(token) if !token.is_empty() => Self::token(&token),
            _ => Ok(Self::Disabled),
        }
    }

    pub fn token(token: &str) -> Result<Self, AuthSetupError> {
        if token.chars().count() < MIN_TOKEN_LEN {
            return Err(AuthSetupError::ShortToken);
        }
        Ok(Self::Token(Sha256::digest(token.as_bytes()).into()))
    }

    pub fn enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    fn authorize(&self, headers: &HeaderMap) -> Result<Admin, AdminApiError> {
        let Self::Token(expected) = self else {
            return Err(AdminApiError::Disabled);
        };
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or(AdminApiError::Unauthorized)?;
        let digest: [u8; 32] = Sha256::digest(presented.trim().as_bytes()).into();
        if !bool::from(digest.ct_eq(expected)) {
            return Err(AdminApiError::Unauthorized);
        }
        Ok(Admin {
            subject: TOKEN_ACTOR.to_string(),
            method: "token",
        })
    }
}

#[derive(Debug, Error)]
pub enum AdminApiError {
    #[error("admin sign-in is not configured")]
    Disabled,
    #[error("missing or invalid admin token")]
    Unauthorized,
    #[error("path is not a realm id")]
    BadRealm,
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl IntoResponse for AdminApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Disabled => StatusCode::NOT_FOUND,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::BadRealm => StatusCode::BAD_REQUEST,
            Self::Store(StoreError::NotFound) => StatusCode::NOT_FOUND,
            Self::Store(StoreError::Admin(_)) => StatusCode::CONFLICT,
            Self::Store(error) => {
                warn!(error = %error, "Registry admin storage failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        let mut response = (status, self.to_string()).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

/// One stored realm as the admin sees it, also after its lease ended.
#[derive(Debug, Serialize)]
pub struct AdminListing {
    pub realm_id: String,
    /// The last registration; `None` once the realm withdrew it.
    pub registration: Option<Signed<Registration>>,
    pub received_at: Option<u64>,
    pub withdrawn: bool,
    pub leased: bool,
    pub verified: bool,
    /// Full route problems with addresses and error chains, for admins only.
    pub problems: Vec<RouteProblem>,
    pub stale: bool,
    pub admin: AdminState,
    /// Every name the realm registered with, oldest first; the last is the current one.
    pub names: Vec<NameRecord>,
}

pub fn router() -> Router<Arc<RegistryState>> {
    Router::new()
        .route("/admin", get(page))
        .route("/admin/admin.js", get(script))
        .route("/admin/admin.css", get(style))
        .route("/v1/admin/whoami", get(whoami))
        .route("/v1/admin/realms", get(list_admin))
        .route("/v1/admin/realms/{realm_id}", axum::routing::put(set_admin))
        .route("/v1/admin/realms/{realm_id}/audit", get(audit))
}

fn asset(content_type: &'static str, body: &'static str) -> Response {
    let mut response = body.into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(PAGE_POLICY),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

async fn page() -> Response {
    asset("text/html; charset=utf-8", PAGE)
}

async fn script() -> Response {
    asset("text/javascript; charset=utf-8", SCRIPT)
}

async fn style() -> Response {
    asset("text/css; charset=utf-8", STYLE)
}

fn admin_realm(value: &str) -> Result<RealmId, AdminApiError> {
    parse_realm(value).map_err(|_| AdminApiError::BadRealm)
}

fn private(body: impl Serialize) -> Response {
    let mut response = Json(body).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn whoami(
    State(state): State<Arc<RegistryState>>,
    headers: HeaderMap,
) -> Result<Response, AdminApiError> {
    Ok(private(state.admin_auth.authorize(&headers)?))
}

fn admin_listing(
    state: &RegistryState,
    realm_id: RealmId,
    now: u64,
) -> Result<AdminListing, AdminApiError> {
    let entry = state.store.stored_entry(&realm_id)?;
    let entry = entry.as_ref();
    Ok(AdminListing {
        realm_id: realm_id.to_string(),
        received_at: entry.map(|entry| entry.received_at),
        withdrawn: entry.is_none(),
        leased: entry.is_some_and(|entry| leased(entry, now)),
        verified: entry.is_some_and(|entry| entry.verified),
        stale: entry.is_some_and(|entry| is_stale(entry, now)),
        admin: state.store.admin(&realm_id)?,
        names: state.store.names(&realm_id)?,
        problems: entry
            .map(|entry| entry.problems.clone())
            .unwrap_or_default(),
        registration: entry.map(|entry| entry.registration.clone()),
    })
}

async fn list_admin(
    State(state): State<Arc<RegistryState>>,
    headers: HeaderMap,
) -> Result<Response, AdminApiError> {
    state.admin_auth.authorize(&headers)?;
    let now = unix_timestamp_secs();
    let listings = state
        .store
        .known_realms()?
        .into_iter()
        .map(|realm_id| admin_listing(&state, realm_id, now))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(private(listings))
}

/// Replaces the realm's admin state; the response is the stored state.
async fn set_admin(
    State(state): State<Arc<RegistryState>>,
    Path(realm_id): Path<String>,
    headers: HeaderMap,
    Json(next): Json<AdminState>,
) -> Result<Response, AdminApiError> {
    let admin = state.admin_auth.authorize(&headers)?;
    let realm_id = admin_realm(&realm_id)?;
    let stored =
        state
            .store
            .update_admin(&realm_id, next, &admin.subject, unix_timestamp_secs())?;
    Ok(private(stored))
}

async fn audit(
    State(state): State<Arc<RegistryState>>,
    Path(realm_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, AdminApiError> {
    state.admin_auth.authorize(&headers)?;
    let records: Vec<AuditRecord> = state.store.audit(&admin_realm(&realm_id)?)?;
    Ok(private(records))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::RealmStatus;
    use crate::store::tests::{named_registration, realm_id, registration};
    use crate::store::{Entry, Store};
    use aruna_blob::egress::EgressGuard;
    use aruna_core::egress::EgressPolicy;
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use axum::body::Body;
    use axum::http::Request;
    use ed25519_dalek::SigningKey;
    use tower::ServiceExt;

    const TOKEN: &str = "test-admin-token-0123456789abcdef";

    fn state(dir: &std::path::Path, auth: AdminAuth) -> Arc<RegistryState> {
        let egress = EgressGuard::new(EgressPolicy::strict()).unwrap();
        Arc::new(RegistryState::new(Store::open(dir).unwrap(), egress).with_admin(auth))
    }

    fn register(state: &RegistryState, name: &str, issued_at: u64) {
        let mut entry = Entry {
            registration: registration("https://realm.example.org", issued_at),
            received_at: issued_at,
            verified: false,
            problems: vec![RouteProblem {
                route: "portal".to_string(),
                reason: "connection failed".to_string(),
                detail: "https://10.1.2.3/.well-known/aruna-realm?t=1: connection refused".into(),
            }],
        };
        entry.registration.payload.descriptor.payload.name = name.to_string();
        state
            .store
            .register(&realm_id(), &entry, issued_at)
            .unwrap();
    }

    async fn call(
        state: &Arc<RegistryState>,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let body = match body {
            Some(body) => {
                request = request.header(header::CONTENT_TYPE, "application/json");
                Body::from(body.to_string())
            }
            None => Body::empty(),
        };
        let response = crate::router(state.clone())
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&bytes).into()));
        (status, value)
    }

    #[test]
    fn short_token_refused() {
        assert!(matches!(
            AdminAuth::token("short"),
            Err(AuthSetupError::ShortToken)
        ));
        assert!(AdminAuth::token(TOKEN).unwrap().enabled());
    }

    #[tokio::test]
    async fn admin_needs_token() {
        // Without a configured token every admin route is absent; with one, only it passes.
        let (dir, other) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let disabled = state(other.path(), AdminAuth::Disabled);
        let (status, _) = call(&disabled, "GET", "/v1/admin/realms", Some(TOKEN), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let state = state(dir.path(), AdminAuth::token(TOKEN).unwrap());
        for token in [
            None,
            Some("wrong"),
            Some("test-admin-token-0123456789abcdeX"),
        ] {
            let (status, _) = call(&state, "GET", "/v1/admin/whoami", token, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{token:?}");
        }
        let (status, admin) = call(&state, "GET", "/v1/admin/whoami", Some(TOKEN), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(admin["method"], "token");
        let path = format!("/v1/admin/realms/{}", realm_id());
        let body = serde_json::json!({"status": "approved", "namespace": null, "note": ""});
        let (status, _) = call(&state, "PUT", &path, Some("wrong"), Some(body)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_changes_audited() {
        // A change is stored with an audit record and shapes the public listing.
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), AdminAuth::token(TOKEN).unwrap());
        let now = unix_timestamp_secs();
        let path = format!("/v1/admin/realms/{}", realm_id());
        let approve =
            serde_json::json!({"status": "approved", "namespace": "giessen", "note": "v3"});
        let (status, _) = call(&state, "PUT", &path, Some(TOKEN), Some(approve.clone())).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "unknown realms have no admin state"
        );
        register(&state, "Realm", now);
        let (status, stored) = call(&state, "PUT", &path, Some(TOKEN), Some(approve)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stored["namespace"], "giessen");
        // The public listing shows only the short reason; the admin view keeps the detail.
        let (_, listed) = call(&state, "GET", "/v1/realms", None, None).await;
        assert_eq!(listed[0]["status"], "approved");
        assert_eq!(listed[0]["problems"][0], "portal route: connection failed");
        assert!(!listed.to_string().contains("10.1.2.3"));
        let (_, admin) = call(&state, "GET", "/v1/admin/realms", Some(TOKEN), None).await;
        assert_eq!(admin[0]["admin"]["status"], "approved");
        assert_eq!(admin[0]["verified"], false);
        let detail = admin[0]["problems"][0]["detail"].as_str().unwrap();
        assert!(detail.contains("10.1.2.3"), "{detail}");
        let hide = serde_json::json!({"status": "hidden", "namespace": "giessen", "note": "spam"});
        call(&state, "PUT", &path, Some(TOKEN), Some(hide)).await;
        let (_, listed) = call(&state, "GET", "/v1/realms", None, None).await;
        assert_eq!(listed, serde_json::json!([]));
        let (_, records) = call(&state, "GET", &format!("{path}/audit"), Some(TOKEN), None).await;
        assert_eq!(records.as_array().unwrap().len(), 2);
        assert_eq!(records[0]["before"]["status"], "listed");
        assert_eq!(records[1]["after"]["status"], "hidden");
        assert_eq!(records[1]["actor"], TOKEN_ACTOR);
        assert!(!records.to_string().contains(TOKEN));
    }

    #[tokio::test]
    async fn retired_refused_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), AdminAuth::token(TOKEN).unwrap());
        register(&state, "Realm", unix_timestamp_secs());
        let path = format!("/v1/admin/realms/{}", realm_id());
        let retire = serde_json::json!({"status": "retired", "namespace": null, "note": ""});
        call(&state, "PUT", &path, Some(TOKEN), Some(retire)).await;
        let back = serde_json::json!({"status": "listed", "namespace": null, "note": ""});
        let (status, _) = call(&state, "PUT", &path, Some(TOKEN), Some(back)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            state.store.admin(&realm_id()).unwrap().status,
            RealmStatus::Retired
        );
    }

    #[tokio::test]
    async fn renames_keep_admin() {
        // A realm's own renames are recorded as name history and never touch its admin state.
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), AdminAuth::token(TOKEN).unwrap());
        let now = unix_timestamp_secs();
        register(&state, "Old name", now - 20);
        let path = format!("/v1/admin/realms/{}", realm_id());
        let approve = serde_json::json!({"status": "approved", "namespace": null, "note": ""});
        call(&state, "PUT", &path, Some(TOKEN), Some(approve)).await;
        register(&state, "Old name", now - 10);
        register(&state, "New name", now);
        let (_, admin) = call(&state, "GET", "/v1/admin/realms", Some(TOKEN), None).await;
        let names: Vec<&str> = admin[0]["names"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| record["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["Old name", "New name"]);
        assert_eq!(admin[0]["names"][1]["since"], now);
        assert_eq!(admin[0]["admin"]["status"], "approved");
    }

    #[tokio::test]
    async fn signed_rename_propagates() {
        // A renewal signed by the realm key renames the entry; another key is refused.
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), AdminAuth::token(TOKEN).unwrap());
        let now = unix_timestamp_secs();
        // A private address literal fails verification at once, without any lookup.
        let base = "https://10.0.0.5";
        let path = format!("/v1/realms/{}", realm_id());
        let first = serde_json::to_value(named_registration(base, "Old", now - 1)).unwrap();
        let (status, _) = call(&state, "PUT", &path, None, Some(first)).await;
        assert_eq!(status, StatusCode::OK);
        let renamed = serde_json::to_value(named_registration(base, "New", now)).unwrap();
        let (status, listing) = call(&state, "PUT", &path, None, Some(renamed)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            listing["registration"]["payload"]["descriptor"]["payload"]["name"],
            "New"
        );
        let other = NodeCapabilities::management_node(SigningKey::from_bytes(&[8; 32])).unwrap();
        let mut forged = named_registration(base, "Taken over", now + 1).payload;
        forged.descriptor = Signed::sign(forged.descriptor.payload, &other).unwrap();
        let forged = serde_json::to_value(Signed::sign(forged, &other).unwrap()).unwrap();
        let (status, _) = call(&state, "PUT", &path, None, Some(forged)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let names = state.store.names(&realm_id()).unwrap();
        let names: Vec<&str> = names.iter().map(|record| record.name.as_str()).collect();
        assert_eq!(names, ["Old", "New"]);
    }

    #[tokio::test]
    async fn withdrawn_stays_manageable() {
        // A withdrawn realm keeps its admin record, is listed as withdrawn and can be retired.
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), AdminAuth::token(TOKEN).unwrap());
        let now = unix_timestamp_secs();
        register(&state, "Realm", now - 10);
        let path = format!("/v1/admin/realms/{}", realm_id());
        let grant = serde_json::json!({"status": "approved", "namespace": "giessen", "note": ""});
        call(&state, "PUT", &path, Some(TOKEN), Some(grant)).await;
        state.store.withdraw(&realm_id(), now, now).unwrap();
        let (_, admin) = call(&state, "GET", "/v1/admin/realms", Some(TOKEN), None).await;
        assert_eq!(admin[0]["withdrawn"], true);
        assert_eq!(admin[0]["registration"], serde_json::Value::Null);
        assert_eq!(admin[0]["admin"]["namespace"], "giessen");
        assert_eq!(admin[0]["names"][0]["name"], "Realm");
        let retire = serde_json::json!({"status": "retired", "namespace": "giessen", "note": ""});
        let (status, stored) = call(&state, "PUT", &path, Some(TOKEN), Some(retire)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stored["status"], "retired");
    }

    #[tokio::test]
    async fn page_served() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path(), AdminAuth::Disabled);
        let response = crate::router(state)
            .oneshot(Request::get("/admin").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let policy = &response.headers()[header::CONTENT_SECURITY_POLICY];
        assert!(policy.to_str().unwrap().contains("script-src 'self'"));
    }
}
