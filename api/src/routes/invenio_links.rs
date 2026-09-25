//! Lasting Invenio links that push later dataset changes to one repository record lineage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::repository::invenio::validate_id;
use aruna_core::repository::{LinkRemote, LinkStatus, RepositoryLink};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::drive;
use aruna_operations::harvest::read_connector::{GetRepositoryOperation, ReadConnectorError};
use aruna_operations::jobs::invenio::export::missing_metadata;
use aruna_operations::jobs::invenio::link_queue::owner_holds;
use aruna_operations::jobs::invenio::links::{
    LinkChange, LinkError, change_link, list_links, read_link,
};
use aruna_operations::jobs::invenio::{TransferError, seal_link_token};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::auth::{ensure_permission, ensure_permission_with, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{ensure_metadata_scope, load_document_record, parse_document_id};
use crate::routes::execution::jobs::job_urls;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(create_link, list_repository_links))
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateLinkRequest {
    pub group_id: String,
    pub connector_id: String,
    /// Personal access token; stored sealed for this link and never returned.
    #[serde(skip_serializing_if = "omit_token")]
    #[schema(write_only = true)]
    pub access_token: String,
    /// Continues this repository record lineage instead of starting a new record.
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub auto_publish: bool,
    #[serde(default)]
    pub public_files: bool,
    /// Native repository fields that override the mapped crate fields.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

impl std::fmt::Debug for CreateLinkRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateLinkRequest")
            .field("group_id", &self.group_id)
            .field("connector_id", &self.connector_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RotateTokenRequest {
    #[serde(skip_serializing_if = "omit_token")]
    #[schema(write_only = true)]
    pub access_token: String,
}

impl std::fmt::Debug for RotateTokenRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RotateTokenRequest").finish_non_exhaustive()
    }
}

fn omit_token(_: &String) -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchLinkRequest {
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub auto_publish: Option<bool>,
    #[serde(default)]
    pub public_files: Option<bool>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    /// Pull links only: import new versions without asking.
    #[serde(default)]
    pub auto_update: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LinkRemoteResponse {
    pub parent_id: Option<String>,
    pub draft_id: Option<String>,
    /// The last version this link published.
    pub record_id: Option<String>,
    /// The open draft's reserved DOI while doi_reserved is true, else the published DOI.
    pub doi: Option<String>,
    pub doi_reserved: bool,
    /// The DOI that names every version of the record.
    pub concept_doi: Option<String>,
    pub record_url: Option<String>,
    pub published: bool,
    /// Community review of the first version: none, pending, accepted or declined.
    pub review: String,
    /// Pull links: the lineage's latest published version at the last check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_remote_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LastPushResponse {
    pub event_id: String,
    pub job_id: String,
    pub pushed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvenioLinkResponse {
    pub link_id: String,
    pub document_id: String,
    pub group_id: String,
    pub connector_id: String,
    pub endpoint: String,
    pub owner_node_url: String,
    pub created_by: String,
    /// push sends dataset changes to the repository; pull imports new repository versions.
    pub direction: String,
    /// enabled, paused or failed.
    pub status: String,
    /// Failure reason such as remote_changed, token_rejected, source_unavailable,
    /// too_many_files or owner_not_holder. An enabled pull link shows update_available, or
    /// local_changed when a local edit holds the update back. An enabled push link shows
    /// review_declined after a declined community review. These are information, not failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// A check that failed after the repository had already published the last push.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    pub auto_publish: bool,
    pub public_files: bool,
    /// A push is queued or running, or a pull is running.
    pub pending: bool,
    /// Pull links only: new versions are imported without asking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_update: Option<bool>,
    /// Pull links only: when the repository was last asked for a new version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checked_at: Option<String>,
    pub remote: LinkRemoteResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_push: Option<LastPushResponse>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LinkJobResponse {
    pub job_id: String,
    pub status_url: String,
}

pub(super) fn link_example() -> serde_json::Value {
    serde_json::json!({
        "link_id": "01ARZ3NDEKTSV4RRFFQ69G5FAY", "document_id": "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "endpoint": "https://zenodo.org/api/", "owner_node_url": "https://node.example/api/v1",
        "created_by": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
        "direction": "push",
        "status": "enabled", "auto_publish": false, "public_files": false, "pending": false,
        "remote": {"parent_id": "abcde-12345", "draft_id": "fghij-67890", "record_id": null,
            "doi": "10.5281/zenodo.123457", "doi_reserved": true,
            "concept_doi": "10.5281/zenodo.123456",
            "record_url": "https://zenodo.org/uploads/fghij-67890", "published": false,
            "review": "none"},
        "last_push": {"event_id": "01ARZ3NDEKTSV4RRFFQ69G5FB0", "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "pushed_at": "2026-09-23T10:00:00+00:00"},
        "created_at": "2026-09-23T09:00:00+00:00", "updated_at": "2026-09-23T10:00:00+00:00"
    })
}

fn timestamp(value: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()
}

/// `holds` is false once the owner node lost the dataset; such a link cannot push any more.
pub(super) fn response(link: RepositoryLink, queued: bool, holds: bool) -> InvenioLinkResponse {
    let info = link.info_reason().map(str::to_string);
    let pull = link.pull().cloned();
    let (status, reason) = match &link.status {
        LinkStatus::Enabled if !holds => ("failed", Some("owner_not_holder".to_string())),
        LinkStatus::Enabled => ("enabled", info),
        LinkStatus::Paused => ("paused", None),
        LinkStatus::Failed { reason } => ("failed", Some(reason.reason().to_string())),
    };
    let LinkRemote {
        parent_id,
        draft_id,
        record_id,
        doi,
        record_url,
        published,
        concept_doi,
        doi_reserved,
        review,
        ..
    } = link.remote;
    InvenioLinkResponse {
        link_id: link.link_id.to_string(),
        document_id: link.document_id.to_string(),
        group_id: link.group_id.to_string(),
        connector_id: link.connector_id.to_string(),
        endpoint: link.endpoint,
        owner_node_url: link.owner_node_url,
        created_by: link.created_by.to_string(),
        direction: if pull.is_some() { "pull" } else { "push" }.to_string(),
        status: status.to_string(),
        reason,
        warning: link.warning,
        auto_publish: link.auto_publish,
        public_files: link.public_files,
        // A pull link always has its next check queued, so only a running pull is pending.
        pending: (queued && pull.is_none()) || link.active_job.is_some(),
        auto_update: pull.as_ref().map(|pull| pull.auto_update),
        last_checked_at: pull
            .as_ref()
            .and_then(|pull| pull.last_checked_at)
            .map(timestamp),
        remote: LinkRemoteResponse {
            parent_id,
            draft_id,
            record_id,
            doi,
            doi_reserved,
            concept_doi,
            record_url,
            published,
            review: review.name().to_string(),
            latest_remote_id: pull.and_then(|pull| pull.latest_remote_id),
        },
        last_push: link.last_push.map(|push| LastPushResponse {
            event_id: push.event_id.to_string(),
            job_id: push.job_id.to_string(),
            pushed_at: timestamp(push.pushed_at),
        }),
        created_at: timestamp(link.created_at),
        updated_at: timestamp(link.updated_at),
    }
}

pub(super) fn link_error(error: LinkError) -> ServerError {
    match error {
        LinkError::NotFound => ServerError::NotFound,
        LinkError::Exists
        | LinkError::Busy(_)
        | LinkError::NoRevision
        | LinkError::NotOwner(_)
        | LinkError::JobLimit(_)
        | LinkError::NotHolder
        | LinkError::Lineage => ServerError::Conflict(error.to_string()),
        LinkError::Submit(_) | LinkError::Fenced => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
        LinkError::ForeignToken
        | LinkError::Storage(_)
        | LinkError::Conversion(_)
        | LinkError::Unexpected(_) => ServerError::InternalError(error.to_string()),
    }
}

/// Answers 400 with the missing fields when the dataset cannot become a repository record.
pub(crate) async fn check_mapping(
    state: &ServerState,
    auth: &AuthContext,
    document_id: Ulid,
    group_id: Ulid,
    connector_id: Ulid,
    metadata_json: &str,
) -> ServerResult<()> {
    let missing = Box::pin(missing_metadata(
        &state.get_ctx(),
        auth,
        document_id,
        group_id,
        connector_id,
        metadata_json,
        state.rocrate_limits().metadata_bytes,
    ))
    .await
    .map_err(|error| match error {
        TransferError::Permanent(message) => ServerError::BadRequestReason(message),
        _ => ServerError::ServiceUnavailableReason("the dataset crate is unavailable".into()),
    })?;
    if missing.is_empty() {
        return Ok(());
    }
    Err(ServerError::MissingMetadata(
        missing.into_iter().map(str::to_string).collect(),
    ))
}

pub(super) fn seal_error(error: TransferError) -> ServerError {
    match error {
        TransferError::Permanent(message) => ServerError::BadRequestReason(message),
        _ => ServerError::ServiceUnavailableReason("the link token could not be sealed".into()),
    }
}

pub(super) fn parse_ulid(value: &str) -> ServerResult<Ulid> {
    Ulid::from_string(value).map_err(|_| ServerError::BadRequest)
}

pub(super) fn metadata_json(
    state: &ServerState,
    value: Option<serde_json::Value>,
) -> ServerResult<String> {
    let value = value.unwrap_or_else(|| serde_json::json!({}));
    let text = value.to_string();
    if !value.is_object() || text.len() as u64 > state.rocrate_limits().metadata_bytes {
        return Err(ServerError::BadRequestReason(
            "metadata overrides must be an object within the metadata limit".into(),
        ));
    }
    Ok(text)
}

/// READ on the dataset gates every link route.
pub(super) async fn readable(
    state: &ServerState,
    auth: Option<AuthContext>,
    document_id: &str,
) -> ServerResult<(AuthContext, Ulid)> {
    let auth = require_unrestricted_auth(state, auth)?;
    let document_id = parse_document_id(document_id)?;
    let record = load_document_record(state, document_id).await?;
    ensure_permission_with(
        state,
        &auth,
        record.permission_path,
        Permission::READ,
        PolicyRequestExtras::operation("metadata.read"),
    )
    .await?;
    Ok((auth, document_id))
}

/// Managing needs metadata WRITE in the connector group, as the creator or a group admin,
/// on the link's owner node.
pub(super) async fn managed(
    state: &ServerState,
    auth: Option<AuthContext>,
    document_id: &str,
    link_id: &str,
) -> ServerResult<(AuthContext, RepositoryLink)> {
    let (auth, document_id) = readable(state, auth, document_id).await?;
    let link_id = parse_ulid(link_id)?;
    let link = read_link(&state.get_ctx().storage_handle, document_id, link_id)
        .await
        .map_err(link_error)?
        .ok_or(ServerError::NotFound)?;
    ensure_metadata_scope(state, &auth, link.group_id, Permission::WRITE).await?;
    if link.created_by != auth.user_id {
        let admin = format!("/{}/g/{}/admin", state.get_realm_id(), link.group_id);
        ensure_permission(state, &auth, admin, Permission::WRITE).await?;
    }
    // Holders keep copies; only the owner node can open the token and change the link.
    if link.owner_node != state.get_node_id() {
        return Err(link_error(LinkError::NotOwner(link.owner_node_url)));
    }
    Ok((auth, link))
}

pub(super) async fn change(
    state: &ServerState,
    link: &RepositoryLink,
    change: LinkChange,
) -> ServerResult<Option<RepositoryLink>> {
    change_link(state.get_ctx().as_ref(), link, change)
        .await
        .map_err(link_error)
}

/// The links a node keeps for the dataset. Holders keep replicas; only the owner queues checks.
async fn responses(
    state: &ServerState,
    document_id: Ulid,
) -> ServerResult<Vec<InvenioLinkResponse>> {
    let context = state.get_ctx();
    let mut views = Vec::new();
    for (link, queued) in list_links(&context.storage_handle, document_id)
        .await
        .map_err(link_error)?
    {
        let holds = owner_holds(&context, &link).await;
        let queued = queued && link.owner_node == state.get_node_id();
        views.push(response(link, queued, holds));
    }
    Ok(views)
}

pub(super) async fn view(
    state: &ServerState,
    document_id: Ulid,
    link_id: Ulid,
) -> ServerResult<Json<InvenioLinkResponse>> {
    let link_id = link_id.to_string();
    responses(state, document_id)
        .await?
        .into_iter()
        .find(|link| link.link_id == link_id)
        .map(Json)
        .ok_or(ServerError::NotFound)
}

pub(super) async fn job_response(
    state: &ServerState,
    job_id: aruna_core::structs::execution::job::JobId,
) -> ServerResult<(StatusCode, Json<LinkJobResponse>)> {
    let urls = job_urls(state, job_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(LinkJobResponse {
            job_id: job_id.to_string(),
            status_url: urls.status_url,
        }),
    ))
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/invenio/links", tag = "metadata/invenio",
    summary = "Link a dataset to an Invenio repository",
    description = r#"Creates a lasting link that pushes each later dataset change to one open repository draft.

**Authentication**

Requires WRITE on the dataset, WRITE on the metadata path of the repository connector group and the caller's personal access_token. The token is sealed for this link, the caller, this node and the connector endpoint. It is never returned or logged.

**Behavior**

The link starts with a push. Without parent_id the first push creates a record draft and reserves its DOI; with parent_id it starts a new version of that record lineage.

Changes push 10 s after the last change, at the latest 5 minutes after the first one, and update the open draft. Publishing happens through the publish route or, with auto_publish, once the draft has been quiet for 15 minutes.

With a connector community the first version is submitted for review instead. After a publish the next change starts a new version.

**Limits**

The node that creates a link owns it and must hold the dataset. Only that node can open the token, pushes and changes the link. The other holders keep a copy of the link without the token.

**Errors**

Invalid input returns 400. A dataset whose mapped metadata lacks title, publication_date, resource_type or creators returns 400 with `missing` listing them. Denied access returns 403.

An unknown dataset, or a connector that does not exist in the group or is no Invenio connector, returns 404. A node that does not hold the dataset returns 409, as does an enabled pull link of the dataset that follows the same record lineage (parent_id) or an existing link with the same id."#,
    params(("document_id" = String, Path, description = "Metadata document identifier")),
    request_body(content = CreateLinkRequest, example = json!({
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "access_token": "<personal-access-token>", "parent_id": "abcde-12345", "auto_publish": false
    })),
    responses(
        (status = 201, description = "Link created and first push queued", body = InvenioLinkResponse, example = json!(link_example())),
        (status = 400, description = "Invalid token, metadata or identifier, or missing required metadata", body = ErrorResponse, example = json!({"error": "the dataset lacks required repository metadata", "code": "missing_metadata", "missing": ["creators"]})),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Dataset or connector access denied", body = ErrorResponse),
        (status = 404, description = "Dataset or Invenio connector not found", body = ErrorResponse),
        (status = 409, description = "This node does not hold the dataset, or an enabled pull link follows the same record lineage", body = ErrorResponse),
        (status = 503, description = "Node credential key unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn create_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Json(request): Json<CreateLinkRequest>,
) -> ServerResult<(StatusCode, Json<InvenioLinkResponse>)> {
    let (auth, document_id) = readable(&state, auth, &document_id).await?;
    let group_id = parse_ulid(&request.group_id)?;
    let connector_id = parse_ulid(&request.connector_id)?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::WRITE).await?;
    // Publishing the dataset as a repository record needs WRITE on it.
    let record = Box::pin(load_document_record(&state, document_id)).await?;
    Box::pin(ensure_permission(
        &state,
        &auth,
        record.permission_path,
        Permission::WRITE,
    ))
    .await?;
    let context = state.get_ctx();
    let connector = Box::pin(drive(
        GetRepositoryOperation::new(group_id, connector_id),
        &context,
    ))
    .await
    .map_err(|error| match error {
        ReadConnectorError::NotFound => ServerError::NotFound,
        _ => ServerError::ServiceUnavailableReason("repository connector unavailable".into()),
    })?;
    if connector.connector.kind != RepositoryConnectorKind::Invenio {
        return Err(ServerError::NotFound);
    }
    if let Some(parent) = &request.parent_id {
        validate_id(parent).map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    }
    let metadata_json = metadata_json(&state, request.metadata)?;
    Box::pin(check_mapping(
        &state,
        &auth,
        document_id,
        group_id,
        connector_id,
        &metadata_json,
    ))
    .await?;
    let context = state.get_ctx();
    let holds = aruna_operations::forward::routing::origin_holds_document(
        &context,
        state.get_realm_id(),
        state.get_node_id(),
        document_id,
    )
    .await
    .map_err(|_| ServerError::ServiceUnavailableReason("dataset placement unavailable".into()))?;
    if !holds {
        return Err(ServerError::Conflict(
            "create the link on a node that holds this dataset".into(),
        ));
    }
    let link_id = Ulid::generate();
    let secret = seal_link_token(
        &context,
        auth.user_id,
        group_id,
        connector_id,
        link_id,
        &request.access_token,
    )
    .await
    .map_err(seal_error)?;
    let owner_node_url = state
        .interface_state()
        .await
        .rest
        .map(|rest| rest.api_base_url)
        .ok_or_else(|| ServerError::InternalError("REST interface URL is unavailable".into()))?;
    let now = SystemTime::now();
    let link = RepositoryLink {
        link_id,
        document_id,
        group_id,
        connector_id,
        endpoint: secret.endpoint.clone(),
        owner_node: state.get_node_id(),
        owner_node_url,
        created_by: auth.user_id,
        status: LinkStatus::Enabled,
        auto_publish: request.auto_publish,
        public_files: request.public_files,
        metadata_json,
        remote: LinkRemote {
            parent_id: request.parent_id,
            ..LinkRemote::default()
        },
        last_push: None,
        active_job: None,
        sequence: 0,
        limits: state.rocrate_limits().clone(),
        created_at: now,
        updated_at: now,
        generation: 0,
        warning: None,
        direction: aruna_core::repository::LinkDirection::Push,
    };
    let change = LinkChange::Create {
        link: Box::new(link.clone()),
        secret: Some(secret),
    };
    let created = change_link(context.as_ref(), &link, change)
        .await
        .map_err(link_error)?
        .ok_or_else(|| ServerError::InternalError("created link missing".into()))?;
    Ok((StatusCode::CREATED, Json(response(created, true, true))))
}

#[utoipa::path(
    get, path = "/metadata/{document_id}/invenio/links", tag = "metadata/invenio",
    summary = "List the repository links of a dataset",
    description = r#"Lists the Invenio links this node keeps for the dataset.

**Authentication**

Requires READ on the dataset.

**Behavior**

Each link shows its direction, state, failure reason, the repository draft or record it pushes to, the last DOI and the last push. pending is true while a push is queued or running. Tokens are never returned.

A pull link (direction pull) comes from an import with keep_updated. remote names the version the dataset holds, remote.latest_remote_id the latest version at last_checked_at, and auto_update whether new versions are imported without asking.

An enabled pull link shows reason update_available when a newer version or a repository edit waits, and local_changed when a local edit since the last pull stopped the automatic update.

An enabled push link shows reason review_declined when the community declined the first version. Pushes still update the draft, auto_publish waits, and an explicit publish submits the draft for review again.

**Limits**

Every holder of the dataset lists its links. owner_node_url names the node that pushes and manages each link. pending covers queued pushes only on that node. A link whose node no longer holds the dataset shows status failed with reason owner_not_holder."#,
    params(("document_id" = String, Path, description = "Metadata document identifier")),
    responses(
        (status = 200, description = "Links of the dataset", body = Vec<InvenioLinkResponse>, example = json!([link_example()])),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Dataset access denied", body = ErrorResponse),
        (status = 404, description = "Dataset not found", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn list_repository_links(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
) -> ServerResult<Json<Vec<InvenioLinkResponse>>> {
    let (_, document_id) = readable(&state, auth, &document_id).await?;
    Ok(Json(responses(&state, document_id).await?))
}

#[cfg(test)]
#[path = "invenio_links_tests.rs"]
mod tests;
