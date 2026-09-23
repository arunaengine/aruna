//! Lasting Invenio links that push later dataset changes to one repository record lineage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::invenio::{InvenioLink, LinkRemote, LinkStatus, validate_id};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::drive;
use aruna_operations::jobs::invenio::links::{
    ChangeLinkOperation, LinkChange, LinkError, list_links, read_link,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LinkRemoteResponse {
    pub parent_id: Option<String>,
    pub draft_id: Option<String>,
    /// The last version this link published.
    pub record_id: Option<String>,
    pub doi: Option<String>,
    pub record_url: Option<String>,
    pub published: bool,
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
    /// enabled, paused or failed.
    pub status: String,
    /// Failure reason such as remote_changed, token_rejected or source_unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub auto_publish: bool,
    pub public_files: bool,
    /// A push is queued or running.
    pub pending: bool,
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
        "status": "enabled", "auto_publish": false, "public_files": false, "pending": false,
        "remote": {"parent_id": "abcde-12345", "draft_id": "fghij-67890", "record_id": null,
            "doi": null, "record_url": "https://zenodo.org/uploads/fghij-67890", "published": false},
        "last_push": {"event_id": "01ARZ3NDEKTSV4RRFFQ69G5FB0", "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "pushed_at": "2026-09-23T10:00:00+00:00"},
        "created_at": "2026-09-23T09:00:00+00:00", "updated_at": "2026-09-23T10:00:00+00:00"
    })
}

fn timestamp(value: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()
}

pub(super) fn response(link: InvenioLink, queued: bool) -> InvenioLinkResponse {
    let (status, reason) = match &link.status {
        LinkStatus::Enabled => ("enabled", None),
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
    } = link.remote;
    InvenioLinkResponse {
        link_id: link.link_id.to_string(),
        document_id: link.document_id.to_string(),
        group_id: link.group_id.to_string(),
        connector_id: link.connector_id.to_string(),
        endpoint: link.endpoint,
        owner_node_url: link.owner_node_url,
        created_by: link.created_by.to_string(),
        status: status.to_string(),
        reason,
        auto_publish: link.auto_publish,
        public_files: link.public_files,
        pending: queued || link.active_job.is_some(),
        remote: LinkRemoteResponse {
            parent_id,
            draft_id,
            record_id,
            doi,
            record_url,
            published,
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
        LinkError::Exists | LinkError::Busy(_) | LinkError::NoRevision => {
            ServerError::Conflict(error.to_string())
        }
        LinkError::Submit(_) => ServerError::ServiceUnavailableReason(error.to_string()),
        LinkError::ForeignToken
        | LinkError::Storage(_)
        | LinkError::Conversion(_)
        | LinkError::Unexpected(_) => ServerError::InternalError(error.to_string()),
    }
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

/// Managing needs metadata WRITE in the connector group, as the creator or a group admin.
pub(super) async fn managed(
    state: &ServerState,
    auth: Option<AuthContext>,
    document_id: &str,
    link_id: &str,
) -> ServerResult<(AuthContext, InvenioLink)> {
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
    Ok((auth, link))
}

pub(super) async fn change(
    state: &ServerState,
    link: &InvenioLink,
    change: LinkChange,
) -> ServerResult<Option<InvenioLink>> {
    drive(
        ChangeLinkOperation::new(link.document_id, link.link_id, change),
        state.get_ctx().as_ref(),
    )
    .await
    .map_err(link_error)
}

pub(super) async fn view(
    state: &ServerState,
    document_id: Ulid,
    link_id: Ulid,
) -> ServerResult<Json<InvenioLinkResponse>> {
    list_links(&state.get_ctx().storage_handle, document_id)
        .await
        .map_err(link_error)?
        .into_iter()
        .find(|(link, _)| link.link_id == link_id)
        .map(|(link, queued)| Json(response(link, queued)))
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

Requires READ on the dataset, WRITE on the metadata path of the repository connector group and the caller's personal access_token. The token is sealed for this link, the caller, this node and the connector endpoint. It is never returned or logged.

**Behavior**

The link starts with a push. Without parent_id the first push creates a record draft; with parent_id it starts a new version of that record lineage. Later changes update the open draft. Publishing happens through the publish route or, with auto_publish, after each push. After a publish the next change starts a new version.

**Limits**

Links live on the node that created them, and that node must hold the dataset. Only that node can open the token.

**Errors**

Invalid input returns 400, denied access 403, an unknown dataset or connector 404 and a node that does not hold the dataset 409."#,
    params(("document_id" = String, Path, description = "Metadata document identifier")),
    request_body(content = CreateLinkRequest, example = json!({
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "access_token": "<personal-access-token>", "parent_id": "abcde-12345", "auto_publish": false
    })),
    responses(
        (status = 201, description = "Link created and first push queued", body = InvenioLinkResponse, example = json!(link_example())),
        (status = 400, description = "Invalid token, metadata or identifier", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Dataset or connector access denied", body = ErrorResponse),
        (status = 404, description = "Dataset or connector not found", body = ErrorResponse),
        (status = 409, description = "This node does not hold the dataset", body = ErrorResponse),
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
    if let Some(parent) = &request.parent_id {
        validate_id(parent).map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    }
    let metadata_json = metadata_json(&state, request.metadata)?;
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
    let link = InvenioLink {
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
    };
    let created = drive(
        ChangeLinkOperation::new(
            document_id,
            link_id,
            LinkChange::Create {
                link: Box::new(link),
                secret,
            },
        ),
        context.as_ref(),
    )
    .await
    .map_err(link_error)?
    .ok_or_else(|| ServerError::InternalError("created link missing".into()))?;
    Ok((StatusCode::CREATED, Json(response(created, true))))
}

#[utoipa::path(
    get, path = "/metadata/{document_id}/invenio/links", tag = "metadata/invenio",
    summary = "List the repository links of a dataset",
    description = r#"Lists the Invenio links this node keeps for the dataset.

**Authentication**

Requires READ on the dataset.

**Behavior**

Each link shows its state, failure reason, the repository draft or record it pushes to, the last DOI and the last push. pending is true while a push is queued or running. Tokens are never returned.

**Limits**

Only links created on this node are listed; owner_node_url names the node of each link."#,
    params(("document_id" = String, Path, description = "Metadata document identifier")),
    responses(
        (status = 200, description = "Links of the dataset on this node", body = Vec<InvenioLinkResponse>, example = json!([link_example()])),
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
    let links = list_links(&state.get_ctx().storage_handle, document_id)
        .await
        .map_err(link_error)?;
    Ok(Json(
        links
            .into_iter()
            .map(|(link, queued)| response(link, queued))
            .collect(),
    ))
}
