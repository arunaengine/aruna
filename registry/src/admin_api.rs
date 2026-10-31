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
use crate::store::{Entry, NameRecord, StoreError, is_stale, leased};
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
    pub registration: Signed<Registration>,
    pub received_at: u64,
    pub leased: bool,
    pub verified: bool,
    pub problems: Vec<String>,
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
    entry: Entry,
    now: u64,
) -> Result<AdminListing, AdminApiError> {
    let realm_id = entry.registration.payload.descriptor.payload.realm_id;
    Ok(AdminListing {
        realm_id: realm_id.to_string(),
        received_at: entry.received_at,
        leased: leased(&entry, now),
        verified: entry.verified,
        stale: is_stale(&entry, now),
        admin: state.store.admin(&realm_id)?,
        names: state.store.names(&realm_id)?,
        problems: entry.problems,
        registration: entry.registration,
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
        .all_entries()?
        .into_iter()
        .map(|entry| admin_listing(&state, entry, now))
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
