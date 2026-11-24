//! The decision interface of the stateless applet edge: host lookup and pinned files. Only a
//! caller with the configured edge key reaches it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::hosts::{HostKind, normalize_host};
use aruna_core::applet::manifest::VersionManifest;
use aruna_core::applet::record::{Access, Applet, PinnedObject, Serving, VersionState};
use aruna_core::applet::settings::{HostingSettings, visitor_marker};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_ops_data::s3::object::get::{GetObjectInput, GetObjectOperation};
use aruna_ops_work::applets::reads::{HostLookupOperation, LoadAppletOperation};
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::{applet_error, applet_id, hosting};
use crate::auth::bearer_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::rate_limit::LocalKey;
use crate::server::state::ServerState;

/// Bytes a version manifest may hold.
pub(super) const MAX_MANIFEST: usize = 16 << 20;
/// How long the edge may cache a host decision.
const MAX_AGE_SECONDS: u32 = 30;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(host_decision))
        .routes(routes!(version_file))
}

/// The hosting settings when the caller sent the configured edge key.
pub(super) fn edge_auth<'a>(
    state: &'a ServerState,
    headers: &HeaderMap,
) -> ServerResult<&'a HostingSettings> {
    let hosting = hosting(state)?;
    let key = (headers.get("x-aruna-edge-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    match hosting.edge_key_valid(key) {
        true => Ok(hosting),
        false => Err(ServerError::Unauthorized),
    }
}

/// The visitor a sealed visitor token names, only when that token was made for `applet_id`; the
/// returned context drops the marker, so checks see the visitor's own rights.
pub(super) async fn visitor(
    state: &ServerState,
    headers: &HeaderMap,
    applet_id: Ulid,
) -> Option<AuthContext> {
    let token = headers.get("x-aruna-visitor")?.to_str().ok()?;
    let (auth, _) = bearer_auth(state, token).await?;
    let marker = visitor_marker(auth.realm_id, applet_id);
    let restrictions = auth.path_restrictions.as_deref()?;
    let bound = restrictions.len() == 1 && restrictions[0].pattern == marker;
    (bound && auth.realm_id == state.get_realm_id()).then_some(AuthContext {
        path_restrictions: None,
        ..auth
    })
}

/// Whether `visitor` may see the applet's private versions: its creator or an approver.
pub(super) async fn previewer(
    state: &ServerState,
    applet: &Applet,
    visitor: Option<&AuthContext>,
) -> bool {
    let Some(visitor) = visitor else {
        return false;
    };
    if visitor.user_id == applet.created_by {
        return true;
    }
    let check = AppletAccessOperation::new(visitor, applet.applet_id, Need::Publish);
    drive(check, &state.get_ctx()).await.is_ok()
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
    "host_node": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
    "custom_domain": false, "serving": "live", "version": 3, "access": "public",
    "frame_ancestors": ["https://lab.example.org"], "guests": false, "guests_in_frames": false,
    "portal_origin": "https://portal.example.org", "max_age_s": 30
}))]
pub struct HostDecision {
    pub applet_id: String,
    pub group_id: String,
    pub host_node: String,
    pub custom_domain: bool,
    pub serving: super::wire::ServingBody,
    /// The version the host serves: the live one, or the newest private one for a preview.
    pub version: Option<u32>,
    pub access: super::wire::AccessBody,
    /// Exact origins for `frame-ancestors`; empty means `'none'` and `X-Frame-Options: DENY`.
    pub frame_ancestors: Vec<String>,
    pub guests: bool,
    pub guests_in_frames: bool,
    /// The portal origin that may frame private previews.
    pub portal_origin: Option<String>,
    /// Seconds the edge may cache this answer.
    pub max_age_s: u32,
}

#[utoipa::path(
    get,
    path = "/edge/v1/hosts/{host}",
    tag = "applets",
    summary = "Decide what an applet host serves",
    description = r#"Answers which applet and version a host serves and the browser policy the edge sets for it.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`; no user token.

**Behavior**
- A custom domain answers only while its DNS checks pass, so the edge can also back an operator
  proxy's on-demand TLS check with this route.
- `frame_ancestors` stays empty unless embedding is on and the applet is live and not private.
- The edge caches the answer for at most `max_age_s` seconds."#,
    params(("host" = String, Path, description = "The requested host name")),
    responses(
        (status = 200, description = "The decision for the host", body = HostDecision),
        (status = 401, description = "No valid edge key", body = ErrorResponse),
        (status = 404, description = "No applet serves this host", body = ErrorResponse),
        (status = 503, description = "This node hosts no applets", body = ErrorResponse)
    )
)]
pub async fn host_decision(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path(host): Path<String>,
) -> ServerResult<Json<HostDecision>> {
    let settings = edge_auth(&state, &headers)?.clone();
    let host = normalize_host(&host).map_err(|_| ServerError::NotFound)?;
    let lookup = drive(HostLookupOperation::new(&host), &state.get_ctx()).await;
    let (row, applet) = lookup.map_err(applet_error)?.ok_or(ServerError::NotFound)?;
    let serving = super::serving(&state, &applet).await?;
    let version = match serving {
        Serving::Live { version } | Serving::Preview { version } => Some(version),
        _ => None,
    };
    let live = matches!(serving, Serving::Live { .. });
    let framed = live && applet.access != Access::Private && applet.embedding.enabled;
    let guests = applet.guests.as_ref();
    Ok(Json(HostDecision {
        applet_id: applet.applet_id.to_string(),
        group_id: applet.group_id.to_string(),
        host_node: applet.host_node.to_string(),
        custom_domain: row.kind == HostKind::Custom,
        serving: serving.into(),
        version,
        access: applet.access.into(),
        frame_ancestors: match framed && settings.embedding {
            true => applet.embedding.origins.clone(),
            false => Vec::new(),
        },
        guests: guests.is_some() && settings.guests,
        guests_in_frames: guests.is_some_and(|policy| policy.in_frames),
        portal_origin: settings.portal_origin,
        max_age_s: MAX_AGE_SECONDS,
    }))
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct FileQuery {
    /// The request path; a folder serves its `index.html`.
    pub path: String,
}

/// Reads one pinned object version of the held bucket as the node.
async fn read_pinned(
    state: &ServerState,
    applet: &Applet,
    (bucket, key, version_id): (&str, &str, &str),
) -> ServerResult<aruna_ops_data::s3::object::get::GetObjectResult> {
    let version_id = Ulid::from_string(version_id).map_err(|_| ServerError::NotFound)?;
    let read = GetObjectOperation::new(GetObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: Some(version_id),
        range: None,
        group_id: applet.group_id,
        user_identity: applet.created_by,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
    });
    drive(read, &state.get_ctx())
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))
}

/// The bytes of a small pinned object, at most `max` of them.
pub(super) async fn read_bytes(
    state: &ServerState,
    applet: &Applet,
    pinned: &PinnedObject,
    max: usize,
) -> ServerResult<Vec<u8>> {
    let object = (
        pinned.bucket.as_str(),
        pinned.key.as_str(),
        pinned.version_id.as_str(),
    );
    let mut blob = read_pinned(state, applet, object).await?.blob;
    let mut bytes = Vec::new();
    while let Some(chunk) = blob.next().await {
        let chunk =
            chunk.map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
        bytes.extend_from_slice(&chunk);
        if bytes.len() > max {
            return Err(ServerError::InternalError("the object is too large".into()));
        }
    }
    Ok(bytes)
}

pub(super) async fn read_manifest(
    state: &ServerState,
    applet: &Applet,
    pinned: &PinnedObject,
) -> ServerResult<VersionManifest> {
    let bytes = read_bytes(state, applet, pinned, MAX_MANIFEST).await?;
    VersionManifest::from_bytes(&bytes).map_err(|error| ServerError::InternalError(error.into()))
}

#[utoipa::path(
    get,
    path = "/edge/v1/applets/{applet_id}/versions/{version}/file",
    tag = "applets",
    summary = "Read a pinned file of a version",
    description = r#"Streams one file of an applet version exactly as it was pinned when the version was made.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`, plus the visitor's sealed
token in `X-Aruna-Visitor` where the version needs a visitor.

**Behavior**
- The live version of a public applet needs no visitor. A group members applet needs a visitor
  who may read the group's data; a private applet and every private version need the applet's
  creator or an approver.
- `ETag` is the file's BLAKE3 digest, so the edge can cache it for good. An `If-None-Match`
  that names it answers 304 without reading the file."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("version" = u32, Path, description = "Version number"),
        FileQuery
    ),
    responses(
        (status = 200, description = "The file bytes with their content type", content_type = "application/octet-stream", body = Vec<u8>),
        (status = 304, description = "The edge holds this file already"),
        (status = 401, description = "No valid edge key", body = ErrorResponse),
        (status = 403, description = "The visitor may not see this version", body = ErrorResponse),
        (status = 404, description = "No such applet, version or file", body = ErrorResponse)
    )
)]
pub async fn version_file(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, u32)>,
    Query(query): Query<FileQuery>,
) -> ServerResult<Response> {
    edge_auth(&state, &headers)?;
    let id = applet_id(&id)?;
    let load = drive(LoadAppletOperation::new(id), &state.get_ctx()).await;
    let applet = load.map_err(applet_error)?.ok_or(ServerError::NotFound)?;
    let entry = applet
        .version(version)
        .ok_or(ServerError::NotFound)?
        .clone();
    let visitor = visitor(&state, &headers, id).await;
    let live = applet.live == Some(version) && entry.state == VersionState::Approved;
    let allowed = match (live, applet.access) {
        (_, _) if entry.state == VersionState::TakenDown => false,
        (true, Access::Public) => true,
        (true, Access::GroupMembers) => match &visitor {
            Some(visitor) => {
                let check = AppletAccessOperation::new(visitor, id, Need::Read);
                drive(check, &state.get_ctx()).await.is_ok()
            }
            None => false,
        },
        _ => previewer(&state, &applet, visitor.as_ref()).await,
    };
    if !allowed {
        return Err(ServerError::Forbidden);
    }
    let manifest = read_manifest(&state, &applet, &entry.manifest).await?;
    let file = manifest.resolve(&query.path).ok_or(ServerError::NotFound)?;
    let pinned = file.pinned();
    let typed = (file.content_type.as_str(), Some(file.size_bytes));
    serve_pinned(&state, (&applet, &headers), &pinned, typed).await
}

/// Whether the edge already holds the object tagged `blake3`, per its `If-None-Match`.
fn held_by_edge(headers: &HeaderMap, blake3: &str) -> bool {
    let tag = format!("\"{blake3}\"");
    (headers.get(header::IF_NONE_MATCH))
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            (value.split(',').map(str::trim))
                .any(|held| held.trim_start_matches("W/") == tag || held == "*")
        })
}

/// Streams one pinned object with its digest as `ETag`, or answers 304 without reading it
/// when the edge already holds that digest.
pub(super) async fn serve_pinned(
    state: &ServerState,
    (applet, request): (&Applet, &HeaderMap),
    pinned: &PinnedObject,
    (content_type, size): (&str, Option<u64>),
) -> ServerResult<Response> {
    let tag = header::HeaderValue::from_str(&format!("\"{}\"", pinned.blake3))
        .map_err(|error| ServerError::InternalError(error.to_string().into()))?;
    if held_by_edge(request, &pinned.blake3) {
        let mut response = Response::new(axum::body::Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response.headers_mut().insert(header::ETAG, tag);
        return Ok(response);
    }
    let object = (
        pinned.bucket.as_str(),
        pinned.key.as_str(),
        pinned.version_id.as_str(),
    );
    let read = read_pinned(state, applet, object).await?;
    let permit = crate::download::admit(state, LocalKey::User(applet.created_by))
        .map_err(|_| ServerError::ServiceUnavailableReason("too many downloads".to_string()))?;
    let mut response = Response::new(crate::download::body(read.blob, permit));
    let headers = response.headers_mut();
    if let Ok(typed) = header::HeaderValue::from_str(content_type) {
        headers.insert(header::CONTENT_TYPE, typed);
    }
    headers.insert(header::ETAG, tag);
    if let Some(size) = size {
        headers.insert(header::CONTENT_LENGTH, size.into());
    }
    Ok(response)
}
