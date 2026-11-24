//! The one-use sign-in exchange of applet hosts: the sign-in host mints a code, the edge trades
//! it once for the visitor's own token bound to one applet.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aruna_clock::unix_timestamp_secs;
use aruna_core::applet::hosts::normalize_host;
use aruna_core::applet::record::{Access, Applet, Serving};
use aruna_core::applet::settings::visitor_marker;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission, SessionKind};
use aruna_ops_identity::document_repository::{GetGroupConfig, GetGroupOperation};
use aruna_ops_identity::session::create::{CreateSessionConfig, CreateSessionOperation};
use aruna_ops_identity::users::read_document::ReadUserOperation;
use aruna_ops_work::applets::reads::LoadAppletOperation;
use aruna_ops_work::applets::{AppletAccessOperation, AppletError, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::edge::{edge_auth, previewer};
use super::wire::{AccessBody, GrantsBody};
use super::{applet_error, applet_id, hosting};
use crate::auth::UnrestrictedAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

/// How long a sign-in code lives.
const CODE_LIFETIME: Duration = Duration::from_secs(30);
/// How long a visitor token from a redeemed code lives.
const VISITOR_SECONDS: u64 = 3600;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(signin_info))
        .routes(routes!(signin_code))
        .routes(routes!(redeem_code))
}

/// Codes minted on this node, redeemed here once; a restart drops them, which only means
/// signing in again.
#[derive(Default)]
pub(crate) struct SigninCodes {
    table: Mutex<HashMap<String, PendingCode>>,
}

struct PendingCode {
    applet_id: Ulid,
    host: String,
    user: AuthContext,
    preview: bool,
    expires: Instant,
}

// Shows only how many codes wait, never a code.
impl std::fmt::Debug for SigninCodes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let waiting = self
            .table
            .lock()
            .map(|table| table.len())
            .unwrap_or_default();
        formatter
            .debug_struct("SigninCodes")
            .field("waiting", &waiting)
            .finish()
    }
}

impl SigninCodes {
    fn insert(&self, code: String, pending: PendingCode) {
        if let Ok(mut table) = self.table.lock() {
            let now = Instant::now();
            table.retain(|_, entry| entry.expires > now);
            table.insert(code, pending);
        }
    }

    fn take(&self, code: &str) -> Option<PendingCode> {
        let entry = self.table.lock().ok()?.remove(code)?;
        (entry.expires > Instant::now()).then_some(entry)
    }
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SigninInfoQuery {
    /// The applet's own host being signed into; custom domains never sign in.
    pub host: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "name": "Bakta web", "slug": "bakta", "host": "bakta.apps.example.org",
    "host_node": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
    "group_id": "01JABCDEF0123456789ABCDEFG", "group_name": "Lab A", "access": "public",
    "published_by": "Ada Lovelace",
    "grants": {"read": [], "references": [], "start": null, "submissions": null,
        "limits": {"cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "upload_bytes": null,
        "visitor_hourly": null, "visitor_daily": null, "applet_daily": null}},
    "session_ttl_s": 3600
}))]
pub struct SigninInfoResponse {
    pub name: String,
    pub slug: String,
    pub host: String,
    pub host_node: String,
    pub group_id: String,
    pub group_name: String,
    pub access: AccessBody,
    pub published_by: Option<String>,
    pub grants: GrantsBody,
    pub session_ttl_s: u64,
}

async fn can_confirm(state: &ServerState, auth: &AuthContext, applet: &Applet) -> bool {
    match applet.access {
        Access::Public => true,
        Access::GroupMembers => {
            let check = AppletAccessOperation::new(auth, applet.applet_id, Need::Read);
            drive(check, &state.get_ctx()).await.is_ok()
        }
        Access::Private => previewer(state, applet, Some(auth)).await,
    }
}

async fn publisher_name(state: &ServerState, applet: &Applet) -> Option<String> {
    let approver = applet
        .live
        .and_then(|version| applet.version(version))
        .and_then(|entry| entry.approved_by)?;
    let read = drive(ReadUserOperation::new(approver), &state.get_ctx()).await;
    let display = read
        .ok()
        .map(|user| user.public_name())
        .filter(|name| !name.is_empty());
    Some(display.unwrap_or_else(|| approver.to_string()))
}

#[utoipa::path(
    get,
    path = "/applets/{applet_id}/signin-info",
    tag = "applets",
    summary = "Read the sign-in confirmation summary of an applet",
    description = r#"Returns the facts the portal sign-in page shows before a visitor confirms sign-in.

**Authentication**: realm bearer token without path restrictions on the applet's `host_node`.

**Behavior**
- `host` must match the applet's own host.
- Public applets answer any signed-in realm user; group-members applets require READ on the group
  (404 otherwise); private applets require the creator or an approver (404 otherwise).
- Returns no versions list, source details, pending approvals or group members."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        SigninInfoQuery
    ),
    responses(
        (status = 200, description = "The confirmation summary", body = SigninInfoResponse),
        (status = 400, description = "The host is not the applet's own host", body = ErrorResponse),
        (status = 403, description = "Applets are disabled for this group or node", body = ErrorResponse),
        (status = 404, description = "No such applet, or the caller may not see it", body = ErrorResponse),
        (status = 409, description = "This node is not the applet's host node", body = ErrorResponse),
        (status = 410, description = "The applet was taken down", body = ErrorResponse),
        (status = 503, description = "The applet is paused or this node hosts no applets", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn signin_info(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Query(query): Query<SigninInfoQuery>,
) -> ServerResult<Json<SigninInfoResponse>> {
    hosting(&state)?;
    let id = applet_id(&id)?;
    let load = drive(LoadAppletOperation::new(id), &state.get_ctx()).await;
    let applet = load.map_err(applet_error)?.ok_or(ServerError::NotFound)?;
    if applet.host_node != state.get_node_id() {
        return Err(applet_error(AppletError::Elsewhere(applet.host_node)));
    }
    if normalize_host(&query.host).ok().as_deref() != Some(applet.host.as_str()) {
        return Err(ServerError::BadRequestReason(
            "not the applet's own host".to_string(),
        ));
    }
    if !can_confirm(&state, &auth, &applet).await {
        return Err(ServerError::NotFound);
    }
    match super::serving(&state, &applet).await? {
        Serving::Live { .. } => {}
        Serving::Preview { .. } if previewer(&state, &applet, Some(&auth)).await => {}
        Serving::Gone | Serving::Preview { .. } => {
            return Err(ServerError::Refused(
                StatusCode::GONE,
                "applet_gone",
                "this applet was taken down".to_string(),
            ));
        }
        Serving::Paused => {
            return Err(ServerError::Refused(
                StatusCode::SERVICE_UNAVAILABLE,
                "applet_paused",
                "this applet is paused".to_string(),
            ));
        }
        Serving::FeaturePaused => {
            return Err(ServerError::Refused(
                StatusCode::FORBIDDEN,
                "feature_disabled",
                "applets are disabled for this group or node".to_string(),
            ));
        }
    }
    let group_name = drive(
        GetGroupOperation::new(GetGroupConfig {
            group_id: applet.group_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map(|(group, _)| group.display_name)
    .unwrap_or_else(|_| applet.group_id.to_string());
    let published_by = publisher_name(&state, &applet).await;
    Ok(Json(SigninInfoResponse {
        name: applet.name,
        slug: applet.slug,
        host: applet.host,
        host_node: applet.host_node.to_string(),
        group_id: applet.group_id.to_string(),
        group_name,
        access: applet.access.into(),
        published_by,
        grants: (&applet.grants).into(),
        session_ttl_s: VISITOR_SECONDS,
    }))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"host": "bakta.apps.example.org", "preview": false}))]
pub struct CodeRequest {
    /// The applet's own host the code is for; custom domains never sign in.
    pub host: String,
    /// A preview session for the creator or an approver, framed by the portal.
    #[serde(default)]
    pub preview: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({"code": "6f1c0e3b8a2d4c5e9f7a1b3c5d7e9f0a2b4c6d8e0f1a3b5c7d9e1f3a5b7c9d1e", "expires_in": 30}))]
pub struct CodeResponse {
    pub code: String,
    pub expires_in: u64,
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/signin-code",
    tag = "applets",
    summary = "Mint a sign-in code for an applet host",
    description = r#"Mints a one-use code, valid for 30 seconds, that the edge trades for a visitor session on the applet's host.

**Authentication**: realm bearer token without path restrictions; the sign-in host calls it for the
signed-in user.

**Behavior**
- The code is bound to the applet and its own host and can be redeemed once, on this node.
- `preview` asks for a preview session; only the creator or an approver gets one, and only
  where the node configures a portal origin."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = CodeRequest,
    responses(
        (status = 200, description = "The one-use code", body = CodeResponse),
        (status = 400, description = "The host is not the applet's own host", body = ErrorResponse),
        (status = 403, description = "A preview for someone who is neither creator nor approver", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 503, description = "This node hosts no applets", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn signin_code(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<CodeRequest>,
) -> ServerResult<Json<CodeResponse>> {
    let settings = hosting(&state)?.clone();
    let id = applet_id(&id)?;
    let load = drive(LoadAppletOperation::new(id), &state.get_ctx()).await;
    let applet = load.map_err(applet_error)?.ok_or(ServerError::NotFound)?;
    if applet.host_node != state.get_node_id() {
        return Err(applet_error(AppletError::Elsewhere(applet.host_node)));
    }
    if normalize_host(&body.host).ok().as_deref() != Some(applet.host.as_str()) {
        return Err(ServerError::BadRequestReason(
            "not the applet's own host".to_string(),
        ));
    }
    if body.preview
        && (settings.portal_origin.is_none() || !previewer(&state, &applet, Some(&auth)).await)
    {
        return Err(ServerError::Forbidden);
    }
    let code = hex::encode(rand::random::<[u8; 32]>());
    state.signin_codes().insert(
        code.clone(),
        PendingCode {
            applet_id: id,
            host: applet.host,
            user: auth,
            preview: body.preview,
            expires: Instant::now() + CODE_LIFETIME,
        },
    );
    Ok(Json(CodeResponse {
        code,
        expires_in: CODE_LIFETIME.as_secs(),
    }))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"code": "6f1c0e3b8a2d4c5e9f7a1b3c5d7e9f0a2b4c6d8e0f1a3b5c7d9e1f3a5b7c9d1e",
    "host": "bakta.apps.example.org"}))]
pub struct RedeemRequest {
    pub code: String,
    pub host: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({"applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "visitor_token": "EXAMPLE-SESSION-TOKEN-PLACEHOLDER",
    "expires_at": 1764000000u64, "preview": false, "portal_origin": null}))]
pub struct RedeemResponse {
    pub applet_id: String,
    /// The visitor's own token for this applet only; the edge seals it into a host cookie.
    pub visitor_token: String,
    /// Unix seconds.
    pub expires_at: u64,
    pub preview: bool,
    /// Set for a preview: the origin the edge allows in `frame-ancestors`.
    pub portal_origin: Option<String>,
}

#[utoipa::path(
    post,
    path = "/edge/v1/signin/redeem",
    tag = "applets",
    summary = "Redeem a sign-in code",
    description = r#"Trades a one-use sign-in code for the visitor's own token, bound to one applet.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`.

**Behavior**
- A code works once, within 30 seconds, and only for the host it was minted for.
- The token carries one path restriction naming the applet, so it reaches no data route; the
  node decides each applet operation with the visitor's own rights cut with the applet's."#,
    request_body = RedeemRequest,
    responses(
        (status = 200, description = "The visitor session", body = RedeemResponse),
        (status = 401, description = "No valid edge key", body = ErrorResponse),
        (status = 404, description = "An unknown, used or expired code, or another host", body = ErrorResponse)
    )
)]
pub async fn redeem_code(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(body): Json<RedeemRequest>,
) -> ServerResult<Json<RedeemResponse>> {
    let settings = edge_auth(&state, &headers)?.clone();
    let pending = state
        .signin_codes()
        .take(&body.code)
        .ok_or(ServerError::NotFound)?;
    if normalize_host(&body.host).ok().as_deref() != Some(pending.host.as_str()) {
        return Err(ServerError::NotFound);
    }
    let user = pending.user;
    let restriction = PathRestriction {
        pattern: visitor_marker(user.realm_id, pending.applet_id),
        permission: Permission::READ,
    };
    let now = unix_timestamp_secs();
    let created = drive(
        CreateSessionOperation::new(CreateSessionConfig {
            time: now,
            expiry: now + VISITOR_SECONDS,
            user_id: user.user_id,
            realm_id: user.realm_id,
            node_capabilities: state.node_capabilities().clone(),
            kind: SessionKind::Api,
            label: Some(format!("applet:{}", pending.applet_id)),
            name: None,
            restrictions: Some(vec![restriction]),
            via: None,
            auth_time: None,
            cutoff: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(ServerError::from)?;
    Ok(Json(RedeemResponse {
        applet_id: pending.applet_id.to_string(),
        visitor_token: created.token.expose().to_string(),
        expires_at: created.session.expires_at,
        preview: pending.preview,
        portal_origin: settings.portal_origin.filter(|_| pending.preview),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(expires: Instant) -> PendingCode {
        let realm = aruna_core::structs::identity::realm::RealmId([1; 32]);
        PendingCode {
            applet_id: Ulid::nil(),
            host: "bakta.apps.example.org".into(),
            user: AuthContext::new(aruna_core::user::id::UserId::nil(realm), realm),
            preview: false,
            expires,
        }
    }

    // A code is redeemed once, and never after it expired.
    #[test]
    fn codes_used_once() {
        let codes = SigninCodes::default();
        codes.insert("live".into(), pending(Instant::now() + CODE_LIFETIME));
        assert!(codes.take("live").is_some());
        assert!(codes.take("live").is_none());
        codes.insert(
            "stale".into(),
            pending(Instant::now() - Duration::from_secs(1)),
        );
        assert!(codes.take("stale").is_none());
        assert!(codes.take("unknown").is_none());
    }
}
