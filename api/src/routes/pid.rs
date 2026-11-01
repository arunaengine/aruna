//! Routes typed w3id lifecycle and landing resolution to each document's PID authority.
//! Documents use /aruna/{document_id} and profiles use /aruna/profile/{document_id}.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_operations::metadata::document_holder::choose_holder;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::secondary_id::{
    IdentifierOrigin, SecondaryIdKind, normalize_endpoint, normalize_value,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::{ARUNA_DATA_PREFIX, W3idIdentifier};
use aruna_core::structs::{
    PersistentIdFailure, PersistentIdKind, PersistentIdMapping, PersistentIdProvider,
    PersistentIdStatus,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::drive;
use aruna_operations::metadata::PersistentIdResolution;
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::api::lookup_identifier_distributed;
use aruna_operations::metadata::get_document::load_document_record;
use aruna_operations::metadata::persistent_id::forward::{
    read_pid_routed, resolve_pid_routed, withdraw_pid_routed,
};
use aruna_operations::metadata::secondary_ids::IdentifierMatch;
use aruna_operations::realm::get_config::GetConfigOperation;

use crate::auth::{ValidatedBearer, ensure_permission, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{forwarded_auth_token, map_api_error};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "pid", description = "w3id persistent identifier landing and lifecycle"),
        (name = "metadata/pids", description = "Persistent identifiers for metadata documents")
    )
)]
pub struct PidApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PidApiDoc::openapi())
        .routes(routes!(resolve_pid, withdraw_pid))
        .routes(routes!(resolve_profile_pid))
        .routes(routes!(list_persistent_ids))
        .routes(routes!(lookup_identifier))
        .merge(super::routes_at(
            OpenApiRouter::new(),
            "/w3id/{*rest}",
            routes!(resolve_w3id),
        ))
}

/// A canonical ULID; `from_string` alone maps a first digit above 7 onto another id.
fn landing_id(value: &str) -> Option<Ulid> {
    match value.as_bytes().first() {
        Some(b'0'..=b'7') => Ulid::from_string(value).ok(),
        _ => None,
    }
}

fn rocrate_location(document_id: Ulid) -> String {
    format!("/api/v1/metadata/{document_id}/rocrate")
}

#[utoipa::path(
    get,
    path = "/pid/{document_id}",
    tag = "pid",
    summary = "Resolve a w3id persistent identifier",
    description = r#"Public landing route for the ordinary identity `https://w3id.org/aruna/{document_id}`.

**Authentication**: none; public route, no bearer token is read.

**Behavior**: a Profile resolves under `/profile/{document_id}`, so this path answers 404 for a
Profile and never acts as a duplicate PID. A public document redirects to the RO-Crate route of
one of its holders: two random holders with a fresh heartbeat, the one with more free download
slots wins, and the chosen holder serves its own copy. Without a fresh holder the redirect stays on this node."#,
    params(("document_id" = String, Path, description = "Document ULID carried by the w3id PID, for example 01JMETADATA0123456789ABCDE")),
    responses(
        (status = 302, description = "Active public identifier; the Location header points at the document's RO-Crate read route, which applies its own authorization: on one holder chosen by fresh heartbeat and free download slots (absolute URL with `here=true`), or on this node"),
        (status = 404, description = "Unknown, malformed, private or Profile identifier, or a mapping still in `requested`, `processing` or `failed`"),
        (
            status = 410,
            description = "The identifier was withdrawn and stays withdrawn",
            body = Object,
            content_type = "application/json",
            example = json!({
                "pid": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                "status": "withdrawn"
            })
        ),
        (status = 503, description = "The PID authority is unreachable, which is reported instead of a false 404; retryable")
    ),
    security(())
)]
async fn resolve_pid(
    State(state): State<Arc<ServerState>>,
    Path(document_id): Path<String>,
) -> Response {
    let Some(document_id) = landing_id(&document_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    document_landing(&state, document_id, None).await
}

/// The landing of a document PID; `page` replaces the RO-Crate target of an active public PID.
async fn document_landing(
    state: &Arc<ServerState>,
    document_id: Ulid,
    page: Option<String>,
) -> Response {
    let ctx = state.get_ctx();
    let resolved = resolve_pid_routed(
        &ctx,
        state.get_realm_id(),
        document_id,
        MetadataRegistryRecord::graph_iri_for(document_id),
    )
    .await;
    if matches!(resolved, Ok(PersistentIdResolution::Redirect)) {
        if let Some(page) = page {
            return redirect(&page);
        }
        if let Some(holder) =
            choose_holder(ctx.as_ref(), state.get_node_id(), document_id, now_ms()).await
        {
            let api = holder.urls.api.as_deref().unwrap_or_default();
            return redirect(&rocrate_url(api, document_id));
        }
    }
    landing_response(document_id, resolved)
}

/// The pinned RO-Crate read of `document_id` on a holder's published api base, which may
/// already carry the `/api/v1` nest.
fn rocrate_url(api: &str, document_id: Ulid) -> String {
    let path = format!("/metadata/{document_id}/rocrate?here=true");
    crate::routes::storage::relay::relay_url(api, &path)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[utoipa::path(
    get,
    path = "/profile/{document_id}",
    tag = "pid",
    summary = "Resolve a Profile w3id persistent identifier",
    description = r#"Public landing route for the Profile identity `https://w3id.org/aruna/profile/{document_id}`.

**Authentication**: none; public route, no bearer token is read.

**Behavior**: resolves only when that exact value is the document's stored automatic primary PID.
Privacy, lifecycle and authority handling match `GET /pid/{document_id}`."#,
    params(("document_id" = String, Path, description = "Profile document ULID")),
    responses(
        (status = 302, description = "Active public Profile identifier; the Location header points at the RO-Crate read route"),
        (status = 404, description = "Unknown, private, non-Profile or non-active identifier"),
        (status = 410, description = "The Profile identifier was withdrawn and stays withdrawn"),
        (status = 503, description = "The PID authority is unreachable; retryable")
    ),
    security(())
)]
async fn resolve_profile_pid(
    State(state): State<Arc<ServerState>>,
    Path(document_id): Path<String>,
) -> Response {
    let Some(document_id) = landing_id(&document_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    profile_landing(&state, document_id, None).await
}

/// The landing of a Profile PID; `page` replaces the RO-Crate target of an active public PID.
async fn profile_landing(
    state: &Arc<ServerState>,
    document_id: Ulid,
    page: Option<String>,
) -> Response {
    let resolved = resolve_pid_routed(
        &state.get_ctx(),
        state.get_realm_id(),
        document_id,
        PersistentIdMapping::profile_pid(document_id),
    )
    .await;
    match (resolved, page) {
        (Ok(PersistentIdResolution::Redirect), Some(page)) => redirect(&page),
        (resolved, _) => landing_response(document_id, resolved),
    }
}

#[utoipa::path(
    get,
    path = "/w3id/{rest}",
    tag = "pid",
    summary = "Resolve any Aruna w3id path",
    description = r#"The node route the Aruna resolver redirects `https://w3id.org/aruna/{rest}` to.

**Authentication**: none; public route, no bearer token is read.

**Behavior**: `{rest}` is the path after `https://w3id.org/aruna/`:
- `{document_id}`: a dataset, release or run, resolved as `GET /pid/{document_id}`.
- `profile/{document_id}`: a Profile, resolved as `GET /profile/{document_id}`.
- `data/{id}`: a content hash, global id or versioned ARN, redirected to `GET /resolve/{id}`.

A browser (`Accept` ranks `text/html` above zero and above `application/ld+json`) is sent to the
portal page of an active public document or Profile when the realm's federation settings name a
portal; other clients get the RO-Crate.
An id that provably names no metadata placement of this realm is a 404; a placement this node
does not know yet, as for a legacy object id, is a retryable 503."#,
    params(("rest" = String, Path, description = "The w3id path after `https://w3id.org/aruna/`, for example `01JMETADATA0123456789ABCDE` or `profile/01JMETADATA0123456789ABCDE`")),
    responses(
        (status = 302, description = "Active public document or Profile; `Location` is its portal page for a browser, else its RO-Crate read route, and the answer varies by `Accept`"),
        (status = 307, description = "A data id; `Location` is `/api/v1/resolve/{id}`"),
        (status = 404, description = "Unknown shape, malformed id, or an identifier this realm does not resolve"),
        (status = 410, description = "The identifier was withdrawn and stays withdrawn", body = Object, content_type = "application/json"),
        (status = 503, description = "The PID authority is unreachable; retryable")
    ),
    security(())
)]
async fn resolve_w3id(
    State(state): State<Arc<ServerState>>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    // The raw path keeps an ARN key percent-encoded, as its canonical form requires.
    let rest = uri.path().split_once("/w3id/").map_or("", |(_, rest)| rest);
    if let Some(id) = rest.strip_prefix("data/") {
        return data_redirect(id);
    }
    let (profile, id) = match rest.strip_prefix("profile/") {
        Some(id) => (true, id),
        None => (false, rest),
    };
    let Some(document_id) = landing_id(id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let page = match wants_page(&headers) {
        true => portal_page(&state, profile, document_id).await,
        false => None,
    };
    let mut response = match profile {
        true => profile_landing(&state, document_id, page).await,
        false => document_landing(&state, document_id, page).await,
    };
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("accept"));
    response
}

/// A 307 to the data resolve route for a well-formed data id, which is re-serialized.
fn data_redirect(id: &str) -> Response {
    let Ok(parsed) = W3idIdentifier::parse(&format!("{ARUNA_DATA_PREFIX}{id}")) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let canonical = parsed.to_w3id();
    let id = canonical
        .strip_prefix(ARUNA_DATA_PREFIX)
        .unwrap_or_default();
    let mut response = redirect(&format!("/api/v1/resolve/{id}"));
    *response.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    response
}

/// True when `Accept` ranks `text/html` above zero and above `application/ld+json`; wildcards
/// name neither, so a client without a preference gets the RO-Crate.
fn wants_page(headers: &axum::http::HeaderMap) -> bool {
    let mut html = 0.0_f32;
    let mut crate_json = 0.0_f32;
    let ranges = headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|range| range.trim().parse::<mime::Mime>().ok());
    for range in ranges {
        let quality = match range.get_param("q") {
            Some(value) => match value.as_str().parse::<f32>() {
                Ok(quality) if (0.0..=1.0).contains(&quality) => quality,
                _ => continue,
            },
            None => 1.0,
        };
        let slot = match range.essence_str() {
            "text/html" => &mut html,
            "application/ld+json" => &mut crate_json,
            _ => continue,
        };
        *slot = slot.max(quality);
    }
    html > 0.0 && html > crate_json
}

/// The portal page of a document or Profile, when the federation settings name a portal.
async fn portal_page(state: &ServerState, profile: bool, document_id: Ulid) -> Option<String> {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .ok()?;
    let mut portal = config.federation?.portal_url;
    if !portal.path().ends_with('/') {
        portal.set_path(&format!("{}/", portal.path()));
    }
    let page = match profile {
        true => format!("app/profiles/{document_id}"),
        false => format!("app/datasets/{document_id}"),
    };
    portal.join(&page).ok().map(String::from)
}

fn landing_response(
    document_id: Ulid,
    resolved: Result<PersistentIdResolution, MetadataApiError>,
) -> Response {
    match resolved {
        Ok(PersistentIdResolution::Redirect) => redirect(&rocrate_location(document_id)),
        Ok(PersistentIdResolution::Gone { pid }) => gone(&pid),
        Ok(PersistentIdResolution::Missing) => StatusCode::NOT_FOUND.into_response(),
        Err(MetadataApiError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

fn redirect(location: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::FOUND;
    if let Ok(value) = HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

fn gone(pid: &str) -> Response {
    (
        StatusCode::GONE,
        Json(serde_json::json!({ "pid": pid, "status": "withdrawn" })),
    )
        .into_response()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
#[schema(as = PersistentIdStateView)]
enum PersistentStateView {
    Requested,
    Processing,
    Active,
    Failed,
    AdminWithdrawn,
    Tombstoned,
    Unknown,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = PersistentIdFailureView)]
struct PersistentFailureView {
    message: String,
    retryable: bool,
    recorded_at_ms: u64,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = PersistentIdView)]
pub(crate) struct PersistentView {
    kind: String,
    provider: String,
    value: Option<String>,
    state: PersistentStateView,
    document_id: String,
    job_id: Option<String>,
    failure: Option<PersistentFailureView>,
    requested_at_ms: Option<u64>,
    minted_at_ms: Option<u64>,
    withdrawn_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    secondary_identifiers: Vec<SecondaryView>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = SecondaryIdentifierKind)]
pub(crate) enum SecondaryKindView {
    Doi,
    InvenioRecord,
    InvenioParent,
}

impl From<SecondaryIdKind> for SecondaryKindView {
    fn from(kind: SecondaryIdKind) -> Self {
        match kind {
            SecondaryIdKind::Doi => Self::Doi,
            SecondaryIdKind::InvenioRecord => Self::InvenioRecord,
            SecondaryIdKind::InvenioParent => Self::InvenioParent,
        }
    }
}

impl From<SecondaryKindView> for SecondaryIdKind {
    fn from(kind: SecondaryKindView) -> Self {
        match kind {
            SecondaryKindView::Doi => Self::Doi,
            SecondaryKindView::InvenioRecord => Self::InvenioRecord,
            SecondaryKindView::InvenioParent => Self::InvenioParent,
        }
    }
}

/// `published`: the document was pushed or exported to the record. `imported`: copied from it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = SecondaryIdentifierOrigin)]
pub(crate) enum OriginView {
    Published,
    Imported,
}

impl From<IdentifierOrigin> for OriginView {
    fn from(origin: IdentifierOrigin) -> Self {
        match origin {
            IdentifierOrigin::Published => Self::Published,
            IdentifierOrigin::Imported => Self::Imported,
        }
    }
}

/// An external identifier; Invenio ids carry the repository API root they belong to.
#[derive(Debug, Serialize, ToSchema)]
#[schema(as = SecondaryIdentifierView)]
struct SecondaryView {
    kind: SecondaryKindView,
    value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    origin: OriginView,
}

fn status_view(mapping: &PersistentIdMapping) -> PersistentView {
    let state = match mapping.status {
        PersistentIdStatus::Requested => PersistentStateView::Requested,
        PersistentIdStatus::Processing => PersistentStateView::Processing,
        PersistentIdStatus::Active => PersistentStateView::Active,
        PersistentIdStatus::Failed => PersistentStateView::Failed,
        PersistentIdStatus::AdminWithdrawn => PersistentStateView::AdminWithdrawn,
        PersistentIdStatus::Tombstoned => PersistentStateView::Tombstoned,
    };
    PersistentView {
        kind: match mapping.kind {
            PersistentIdKind::Conceptual => "conceptual".to_string(),
        },
        provider: match mapping.provider {
            PersistentIdProvider::W3id => "w3id".to_string(),
        },
        value: Some(mapping.pid.clone()),
        state,
        document_id: mapping.target.to_string(),
        job_id: mapping.job_id.map(|job_id| job_id.to_string()),
        failure: mapping.failure.as_ref().map(
            |PersistentIdFailure {
                 message,
                 retryable,
                 recorded_at_ms,
             }| PersistentFailureView {
                message: message.clone(),
                retryable: *retryable,
                recorded_at_ms: *recorded_at_ms,
            },
        ),
        requested_at_ms: mapping.requested_at_ms,
        minted_at_ms: mapping.minted_at_ms,
        withdrawn_at_ms: mapping.withdrawn_at_ms,
        secondary_identifiers: mapping
            .secondary_identifiers
            .iter()
            .map(|identifier| SecondaryView {
                kind: identifier.kind.into(),
                value: identifier.value.clone(),
                endpoint: identifier.endpoint.clone(),
                origin: identifier.origin.into(),
            })
            .collect(),
    }
}

fn synthetic_status_view(
    document_id: Ulid,
    value: Option<String>,
    state: PersistentStateView,
) -> PersistentView {
    PersistentView {
        kind: "conceptual".to_string(),
        provider: "w3id".to_string(),
        value,
        state,
        document_id: document_id.to_string(),
        job_id: None,
        failure: None,
        requested_at_ms: None,
        minted_at_ms: None,
        withdrawn_at_ms: None,
        secondary_identifiers: Vec::new(),
    }
}

async fn require_status_visibility(
    state: &ServerState,
    auth: Option<&AuthContext>,
    mapping: Option<&PersistentIdMapping>,
    record: Option<&MetadataRegistryRecord>,
) -> ServerResult<()> {
    let public = record
        .map(|record| record.public)
        .or_else(|| mapping.and_then(|mapping| mapping.public));
    if public == Some(true) {
        return Ok(());
    }
    let Some(auth) = auth else {
        return Err(ServerError::NotFound);
    };
    let path = mapping
        .and_then(|mapping| mapping.permission_path.clone())
        .or_else(|| record.map(|record| record.permission_path.clone()))
        .unwrap_or_else(|| format!("/{}/admin/pids/**", state.get_realm_id()));
    ensure_permission(state, auth, path, Permission::READ).await
}

/// Returns authenticated status and failure facts solely from the durable PID mapping.
/// `unknown` means the authority lacks a definitive row for a readable document.
/// Anonymous access to private mappings remains 404 and reveals no existence.
#[utoipa::path(
    get,
    path = "/metadata/{document_id}/pids",
    tag = "metadata/pids",
    summary = "List a document's typed persistent identifiers",
    description = r#"Returns the one stored automatic w3id record for a document as a typed list.

**Authentication**: optional bearer token; a public record may be read anonymously, a private one
needs READ on the document's frozen permission path and answers 404 anonymously.

**Behavior**
- The stored mapping is authoritative for the `requested`, `processing`, `active`, `failed`,
  `admin-withdrawn` and `tombstoned` states and for the job and failure fields, so a failed job
  read is never reported as unminted.
- A missing or unavailable authority mapping is reported as `unknown`.
- `secondary_identifiers` lists external identifiers such as repository DOIs and is omitted
  when there are none. `origin` is `published` when this document was pushed or exported to the
  record, and `imported` when it was copied from it."#,
    params(("document_id" = String, Path, description = "Metadata document ULID")),
    responses(
        (
            status = 200,
            description = "Typed list containing the automatic w3id status",
            body = [PersistentView],
            example = json!([
                {
                    "kind": "conceptual",
                    "provider": "w3id",
                    "value": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                    "state": "active",
                    "document_id": "01JMETADATA0123456789ABCDE",
                    "job_id": "01JABCDEF0123456789ABCDEFG",
                    "failure": null,
                    "requested_at_ms": 1755500000000u64,
                    "minted_at_ms": 1755500001000u64,
                    "withdrawn_at_ms": null,
                    "secondary_identifiers": [
                        {"kind": "doi", "value": "10.5281/zenodo.1234567", "origin": "published"},
                        {"kind": "invenio_parent", "value": "abcde-12345", "endpoint": "https://zenodo.org/api", "origin": "published"}
                    ]
                }
            ])
        ),
        (status = 400, description = "Malformed document id", body = ErrorResponse),
        (status = 403, description = "Caller lacks READ on the document", body = ErrorResponse),
        (status = 404, description = "Document not found, or a private record read anonymously", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ())
)]
pub(crate) async fn list_persistent_ids(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
) -> ServerResult<Json<Vec<PersistentView>>> {
    let document_id = Ulid::from_string(&document_id).map_err(|_| ServerError::BadRequest)?;
    let ctx = state.get_ctx();
    let record = load_document_record(&ctx, document_id)
        .await
        .map_err(|error| ServerError::InternalError(format!("{error:?}")))?;
    let routed = read_pid_routed(&ctx, state.get_realm_id(), document_id).await;
    match routed {
        Ok(Some(mapping)) => {
            require_status_visibility(&state, auth.as_ref(), Some(&mapping), record.as_ref())
                .await?;
            Ok(Json(vec![status_view(&mapping)]))
        }
        Ok(None) => {
            let record = record.as_ref().ok_or(ServerError::NotFound)?;
            require_status_visibility(&state, auth.as_ref(), None, Some(record)).await?;
            Ok(Json(vec![synthetic_status_view(
                document_id,
                None,
                PersistentStateView::Unknown,
            )]))
        }
        Err(_) => {
            let record = record.as_ref().ok_or(ServerError::NotFound)?;
            require_status_visibility(&state, auth.as_ref(), None, Some(record)).await?;
            Ok(Json(vec![synthetic_status_view(
                document_id,
                None,
                PersistentStateView::Unknown,
            )]))
        }
    }
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct LookupQuery {
    /// Identifier kind.
    pub(crate) kind: SecondaryKindView,
    /// Identifier value; DOIs may carry a `doi:` or resolver URL prefix.
    pub(crate) value: String,
    /// Repository API root that narrows an Invenio id to one repository.
    pub(crate) endpoint: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = PidLookupMatch)]
pub(crate) struct LookupMatchView {
    pub(crate) document_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pid: Option<String>,
    pub(crate) origin: OriginView,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = PidLookupResponse)]
pub(crate) struct LookupView {
    pub(crate) matches: Vec<LookupMatchView>,
}

impl From<IdentifierMatch> for LookupMatchView {
    fn from(found: IdentifierMatch) -> Self {
        Self {
            document_id: found.document_id.to_string(),
            pid: found.pid,
            origin: found.origin.into(),
        }
    }
}

#[utoipa::path(
    get,
    path = "/pid/lookup",
    tag = "metadata/pids",
    summary = "Find documents by an external identifier",
    description = r#"Returns every readable document that holds a secondary identifier, such as a DOI.

**Authentication**: optional bearer token; the same read rules as reading the document apply.

**Behavior**
- DOIs are compared case-insensitively without `doi:` or resolver prefixes.
- Invenio ids are unique per repository; without `endpoint` every repository's match is returned.
- Matches are ordered `published` first, then by document id. One document appears once.
- `pid` is present once the document's w3id identifier is active.
- Every node answers the same way: the lookup asks all realm nodes, because each node indexes
  only the mappings it holds.

**Errors**
- 404 when no readable document holds the identifier, so private documents stay hidden.
- 503 when nothing was found and some node did not answer; retryable."#,
    params(LookupQuery),
    responses(
        (status = 200, description = "The readable documents holding the identifier", body = LookupView, example = json!({
            "matches": [{
                "document_id": "01JMETADATA0123456789ABCDE",
                "pid": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                "origin": "published"
            }]
        })),
        (status = 400, description = "Unknown kind or malformed value", body = ErrorResponse),
        (status = 404, description = "No readable document holds the identifier", body = ErrorResponse),
        (status = 503, description = "No match found while some node was unreachable; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ())
)]
pub(crate) async fn lookup_identifier(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Query(query): Query<LookupQuery>,
) -> ServerResult<Json<LookupView>> {
    let kind = SecondaryIdKind::from(query.kind);
    let value = normalize_value(kind, &query.value)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let endpoint = query
        .endpoint
        .as_deref()
        .map(normalize_endpoint)
        .transpose()
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?
        .filter(|_| kind != SecondaryIdKind::Doi);
    let (matches, incomplete) = lookup_identifier_distributed(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        auth,
        bearer_token.map(|carrier| carrier.as_str().to_string()),
        (kind, value, endpoint),
    )
    .await
    .map_err(map_api_error)?;
    match (matches.is_empty(), incomplete) {
        (true, true) => Err(ServerError::ServiceUnavailable),
        (true, false) => Err(ServerError::NotFound),
        (false, _) => Ok(Json(LookupView {
            matches: matches.into_iter().map(LookupMatchView::from).collect(),
        })),
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[schema(as = WithdrawPersistentIdRequest)]
struct WithdrawPersistentRequest {
    provider: String,
    confirm_pid: String,
    reason: String,
}

fn validated_withdrawal_reason(request: &WithdrawPersistentRequest) -> ServerResult<String> {
    let reason = request.reason.trim();
    if request.provider != "w3id"
        || reason.is_empty()
        || reason.len() > 1_024
        || reason.chars().any(char::is_control)
    {
        return Err(ServerError::BadRequest);
    }
    Ok(reason.to_string())
}

/// Exceptional admin-only withdrawal. Normal deletion uses the distinct
/// tombstone transition; owners with only document WRITE cannot call this path.
#[utoipa::path(
    delete,
    path = "/pid/{document_id}",
    tag = "pid",
    summary = "Withdraw a document's w3id persistent identifier",
    description = r#"Exceptional administration route that terminally withdraws a document's w3id identifier.

**Authentication**: unrestricted realm bearer token with WRITE on
`/{realm}/admin/pids/{document_id}`; document WRITE alone is deliberately insufficient.

**Behavior**
- The authority stores `admin-withdrawn`, the actor and the reason, and writes a
  `WithdrawPersistentId` metadata audit row in the same transaction.
- Normal document deletion instead stores `tombstoned`.
- The transition is terminal and idempotent.

**Limits**
- `provider` must be `w3id`.
- `confirm_pid` must equal the stored PID value exactly.
- `reason` is trimmed first and must then be 1 to 1024 bytes long and free of control characters."#,
    params(("document_id" = String, Path, description = "Document ULID whose PID is withdrawn, for example 01JMETADATA0123456789ABCDE")),
    request_body(
        content = WithdrawPersistentRequest,
        description = "Provider, the exact stored PID as confirmation, and a non-empty reason",
        example = json!({
            "provider": "w3id",
            "confirm_pid": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
            "reason": "published in error"
        })
    ),
    responses(
        (status = 204, description = "The withdrawal is durable on the PID authority"),
        (status = 400, description = "Malformed id, unsupported provider, wrong confirmation, or invalid reason", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller lacks WRITE on the realm PID administration path", body = ErrorResponse),
        (status = 404, description = "No identifier is registered for this document", body = ErrorResponse),
        (status = 503, description = "The PID authority is unreachable; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn withdraw_pid(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
    Json(request): Json<WithdrawPersistentRequest>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let document_id = Ulid::from_string(&document_id).map_err(|_| ServerError::BadRequest)?;
    let reason = validated_withdrawal_reason(&request)?;
    let ctx = state.get_ctx();
    ensure_permission(
        &state,
        &auth,
        format!("/{}/admin/pids/{document_id}", state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let existing = read_pid_routed(&ctx, state.get_realm_id(), document_id)
        .await
        .map_err(map_api_error)?
        .ok_or(ServerError::NotFound)?;
    if request.confirm_pid != existing.pid {
        return Err(ServerError::BadRequest);
    }
    let mapping = withdraw_pid_routed(
        &ctx,
        state.get_realm_id(),
        document_id,
        auth.user_id,
        reason,
        unix_timestamp_millis(),
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_api_error)?;
    if mapping.status != PersistentIdStatus::AdminWithdrawn {
        return Err(ServerError::ServiceUnavailable);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holder_redirect_nested() {
        let id = Ulid::from_bytes([1; 16]);
        let pinned = format!("https://b.example/api/v1/metadata/{id}/rocrate?here=true");
        assert_eq!(rocrate_url("https://b.example/", id), pinned);
        assert_eq!(rocrate_url("https://b.example/api/v1", id), pinned);
    }

    // the RO-Crate Location header targets the read route
    #[test]
    fn location_targets_read() {
        let id = Ulid::from_bytes([1; 16]);
        assert_eq!(
            rocrate_location(id),
            format!("/api/v1/metadata/{id}/rocrate")
        );
    }

    #[test]
    fn landing_maps_resolutions() {
        let id = Ulid::from_bytes([1; 16]);
        let status = |resolved| landing_response(id, resolved).status();

        assert_eq!(
            status(Ok(PersistentIdResolution::Redirect)),
            StatusCode::FOUND
        );
        assert_eq!(
            status(Ok(PersistentIdResolution::Gone {
                pid: "pid".to_string()
            })),
            StatusCode::GONE
        );
        assert_eq!(
            status(Ok(PersistentIdResolution::Missing)),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(Err(MetadataApiError::NotFound)),
            StatusCode::NOT_FOUND
        );
        // An unreachable authority is never a 404: that would turn a live PID dead.
        assert_eq!(
            status(Err(MetadataApiError::ServiceUnavailable)),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn intent_ignores_readability() {
        let id = Ulid::from_bytes([3; 16]);
        let mapping = PersistentIdMapping::requested(
            id,
            false,
            aruna_core::UserId::local(
                Ulid::from_bytes([4; 16]),
                aruna_core::structs::identity::realm::RealmId([5; 32]),
            ),
            aruna_core::structs::execution::job::JobId::from_bytes([6; 16]),
            false,
            "/private/document".to_string(),
            aruna_core::structs::PersistentIdRevision {
                event_id: Ulid::from_bytes([7; 16]),
                actor: iroh::SecretKey::from_bytes(&[8; 32]).public(),
                occurred_at_ms: 9,
            },
        );

        let view = status_view(&mapping);
        assert_eq!(view.state, PersistentStateView::Requested);
        assert_eq!(view.job_id, mapping.job_id.map(|job_id| job_id.to_string()));
        assert_eq!(view.value.as_deref(), Some(mapping.pid.as_str()));
    }

    #[test]
    fn withdrawal_requires_confirmation() {
        let valid = WithdrawPersistentRequest {
            provider: "w3id".to_string(),
            confirm_pid: "https://w3id.org/aruna/example".to_string(),
            reason: "  duplicate external registration  ".to_string(),
        };
        assert_eq!(
            validated_withdrawal_reason(&valid).unwrap(),
            "duplicate external registration"
        );
        assert!(
            validated_withdrawal_reason(&WithdrawPersistentRequest {
                provider: "doi".to_string(),
                ..valid
            })
            .is_err()
        );
    }

    use aruna_core::structs::identity::auth::Actor;
    use axum::http::{HeaderMap, Uri};

    /// A node whose realm holds one active public PID, and that document's id.
    async fn public_pid() -> (
        tempfile::TempDir,
        Arc<ServerState>,
        Ulid,
        Arc<aruna_operations::driver::DriverContext>,
        Actor,
    ) {
        seeded_pid(true, true).await
    }

    /// An active PID whose intent froze `frozen` and whose document is `public` now.
    async fn seeded_pid(
        frozen: bool,
        public: bool,
    ) -> (
        tempfile::TempDir,
        Arc<ServerState>,
        Ulid,
        Arc<aruna_operations::driver::DriverContext>,
        Actor,
    ) {
        use crate::tests::routes::{
            seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state,
            test_storage, write_doc,
        };
        use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, ID_MAPPING_KEYSPACE};
        use aruna_core::storage_entries::metadata_document_key;
        use aruna_core::structs::identity::auth::NodeCapabilities;
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::persistent_id_key;
        use aruna_core::structs::placement::record::PlacementRef;

        let (dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[7; 32])
                .verifying_key()
                .to_bytes(),
        );
        let node_id = iroh::SecretKey::from_bytes(&[8; 32]).public();
        let user_id = aruna_core::UserId::local(Ulid::from_bytes([4; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let context = Arc::new(test_context(storage));
        seed_realm_config(&context, realm_id, &actor).await;
        seed_realm_auth(&context, realm_id, &actor).await;
        let document_id = Ulid::from_bytes([10; 16]);
        let group_id = Ulid::from_bytes([11; 16]);
        seed_group_docs(&context, realm_id, &actor, group_id, "public", user_id).await;
        let revision = aruna_core::structs::PersistentIdRevision {
            event_id: Ulid::from_bytes([7; 16]),
            actor: node_id,
            occurred_at_ms: 9,
        };
        let path = "datasets/public".to_string();
        let mut mapping = PersistentIdMapping::requested(
            document_id,
            false,
            user_id,
            aruna_core::structs::execution::job::JobId::from_bytes([6; 16]),
            frozen,
            path.clone(),
            revision,
        );
        assert!(mapping.activate(user_id, revision));
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                &path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 2,
            establishing_event_id: Ulid::from_bytes([8; 16]),
            last_event_id: Ulid::from_bytes([9; 16]),
        };
        let mapping_row = mapping.to_bytes().unwrap();
        let key = persistent_id_key(document_id);
        write_doc(
            &context,
            ID_MAPPING_KEYSPACE,
            key.into(),
            mapping_row.into(),
        )
        .await;
        let record_row = postcard::to_allocvec(&record).unwrap();
        let key = metadata_document_key(document_id);
        write_doc(&context, DOCUMENT_INDEX_KEYSPACE, key, record_row.into()).await;
        let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
        let state = test_state(context.clone(), realm_id, node_id, capabilities).await;
        (dir, Arc::new(state), document_id, context, actor)
    }

    /// An active public PID redirects to the RO-Crate read; with no holder chosen, on this node.
    #[tokio::test]
    async fn redirects_public_pid() {
        let (_dir, state, document_id, _, _) = public_pid().await;

        let response = resolve_pid(State(state.clone()), Path(document_id.to_string())).await;

        assert_eq!(response.status(), StatusCode::FOUND);
        let location = response.headers()[axum::http::header::LOCATION]
            .to_str()
            .unwrap();
        assert_eq!(location, rocrate_location(document_id));
        let malformed = resolve_pid(State(state.clone()), Path("not-a-ulid".to_string())).await;
        assert_eq!(malformed.status(), StatusCode::NOT_FOUND);
        // A first digit above 7 overflows and would otherwise alias the same document.
        let alias = format!("8{}", &document_id.to_string()[1..]);
        assert_eq!(Ulid::from_string(&alias).unwrap(), document_id);
        let document = resolve_pid(State(state.clone()), Path(alias.clone())).await;
        assert_eq!(document.status(), StatusCode::NOT_FOUND);
        let profile = resolve_profile_pid(State(state.clone()), Path(alias.clone())).await;
        assert_eq!(profile.status(), StatusCode::NOT_FOUND);
        let response = w3id(&state, &alias, "application/ld+json").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Resolution follows the document's visibility now, not the one frozen at creation.
    #[tokio::test]
    async fn follows_live_visibility() {
        for (frozen, public, expected) in [
            (false, true, StatusCode::FOUND),
            (true, false, StatusCode::NOT_FOUND),
            (false, false, StatusCode::NOT_FOUND),
        ] {
            let (_dir, state, document_id, _, _) = seeded_pid(frozen, public).await;
            let id = document_id.to_string();
            let response = resolve_pid(State(state.clone()), Path(id.clone())).await;
            assert_eq!(
                response.status(),
                expected,
                "frozen {frozen}, public {public}"
            );
            let response = w3id(&state, &id, "application/ld+json").await;
            assert_eq!(
                response.status(),
                expected,
                "frozen {frozen}, public {public}"
            );
            let status = list_persistent_ids(State(state), Extension(None), Path(id)).await;
            assert_eq!(status.is_ok(), public, "frozen {frozen}, public {public}");
        }
    }

    async fn w3id(state: &Arc<ServerState>, rest: &str, accept: &str) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_str(accept).unwrap());
        let uri: Uri = format!("/api/v1/w3id/{rest}").parse().unwrap();
        resolve_w3id(State(state.clone()), headers, uri).await
    }

    fn location(response: &Response) -> &str {
        response.headers()[header::LOCATION].to_str().unwrap()
    }

    /// Browsers get the portal page once the realm names a portal; clients keep the RO-Crate.
    #[tokio::test]
    async fn w3id_picks_pages() {
        use aruna_core::effects::StorageEffect;
        use aruna_core::federation::{
            AcceptedRealms, FederationSettings, RealmDescriptor, RegistrationMode, Signed,
        };
        use aruna_core::keys::generate_signing_key;
        use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
        use aruna_core::structs::identity::auth::NodeCapabilities;

        let (_dir, state, document_id, context, actor) = public_pid().await;
        let id = document_id.to_string();
        let client = w3id(&state, &id, "application/ld+json").await;
        assert_eq!(client.status(), StatusCode::FOUND);
        assert_eq!(location(&client), rocrate_location(document_id));
        assert_eq!(client.headers()[header::VARY], "accept");
        // Without federation settings no portal is known, so a browser gets the RO-Crate too.
        let html = "text/html,application/xhtml+xml";
        let browser = w3id(&state, &id, html).await;
        assert_eq!(location(&browser), rocrate_location(document_id));

        let mut config = drive(GetConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        let descriptor = RealmDescriptor {
            realm_id: actor.realm_id,
            name: "Realm".to_string(),
            description: String::new(),
            api_url: url::Url::parse("https://api.example.org/api/v1").unwrap(),
            portal_url: url::Url::parse("https://portal.example.org/portal").unwrap(),
            contact: None,
            login_providers: Vec::new(),
            issued_at: 1,
        };
        let signer = NodeCapabilities::management_node(generate_signing_key()).unwrap();
        config.federation = Some(FederationSettings {
            name: descriptor.name.clone(),
            description: String::new(),
            contact: None,
            login_providers: Vec::new(),
            api_url: descriptor.api_url.clone(),
            portal_url: descriptor.portal_url.clone(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: AcceptedRealms::None,
            descriptor: Signed::sign(descriptor, &signer).unwrap(),
        });
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: actor.realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        let browser = w3id(&state, &id, html).await;
        assert_eq!(browser.status(), StatusCode::FOUND);
        assert_eq!(
            location(&browser),
            format!("https://portal.example.org/portal/app/datasets/{id}")
        );
        let client = w3id(&state, &id, "application/ld+json").await;
        assert_eq!(location(&client), rocrate_location(document_id));
        // The document is no Profile, so its profile form names nothing, not even for a browser.
        let profile = w3id(&state, &format!("profile/{id}"), html).await;
        assert_eq!(profile.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn page_follows_quality() {
        let wants = |accept: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT, HeaderValue::from_str(accept).unwrap());
            wants_page(&headers)
        };
        for browser in [
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "TEXT/HTML",
            "application/ld+json;q=0.5, text/html",
        ] {
            assert!(wants(browser), "{browser}");
        }
        for client in [
            "application/ld+json,text/html;q=0",
            "application/ld+json, text/html;q=0.5",
            "text/html, application/ld+json",
            "text/htmlx",
            "*/*",
            "text/*",
            "text/html;q=abc",
            "text/html;q=2",
            "",
        ] {
            assert!(!wants(client), "{client}");
        }
    }

    #[tokio::test]
    async fn w3id_dispatches_shapes() {
        let (_dir, state, _, _, _) = public_pid().await;
        let hash = "ab".repeat(32);
        let data = w3id(&state, &format!("data/{hash}"), "*/*").await;
        assert_eq!(data.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(location(&data), format!("/api/v1/resolve/{hash}"));
        let realm = aruna_core::structs::identity::realm::RealmId::from_bytes([3; 32]);
        let global = format!("{realm}/01JMETADATA0123456789ABCDE");
        let version = w3id(&state, &format!("data/{global}"), "*/*").await;
        assert_eq!(location(&version), format!("/api/v1/resolve/{global}"));
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let arn = format!("arn:aruna:{realm}:{node}:s3/reads/a%20b@01JMETADATA0123456789ABCDE");
        let object = w3id(&state, &format!("data/{arn}"), "*/*").await;
        assert_eq!(location(&object), format!("/api/v1/resolve/{arn}"));
        // An unknown ULID, a legacy one included, is a 404 like every unknown shape.
        let unknown = Ulid::from_bytes([42; 16]).to_string();
        for rest in [
            unknown.as_str(),
            "01H8Y3R7Z5X9C2V4B6N8M0Q1W3",
            "profile/01H8Y3R7Z5X9C2V4B6N8M0Q1W3",
            "data/not-an-id",
            "data/../../x",
            "terms/Release",
            "dev/01H8Y3R7Z5X9C2V4B6N8M0Q1W3",
            "01JMETADATA0123456789ABCDE/x",
            "profile/81JMETADATA0123456789ABCDE",
        ] {
            let response = w3id(&state, rest, "*/*").await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{rest}");
        }
    }
}
