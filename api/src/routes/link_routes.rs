//! Changes one Invenio link: read, configure, remove, push, publish and replace its token.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::invenio::{LinkPatch, LinkStatus};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::jobs::invenio::link_queue::{current_event, start_push};
use aruna_operations::jobs::invenio::links::LinkChange;
use aruna_operations::jobs::invenio::seal_link_token;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use utoipa_axum::{router::OpenApiRouter, routes};

use super::invenio_links::{
    InvenioLinkResponse, LinkJobResponse, PatchLinkRequest, RotateTokenRequest, change,
    job_response, link_error, link_example, managed, metadata_json, parse_ulid, readable,
    seal_error, view,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(get_link, patch_link, delete_link))
        .routes(routes!(push_link))
        .routes(routes!(publish_link))
        .routes(routes!(rotate_token))
}

#[utoipa::path(
    get, path = "/metadata/{document_id}/invenio/links/{link_id}", tag = "metadata/invenio",
    summary = "Read one repository link",
    description = r#"Returns one Invenio link of the dataset.

**Authentication**

Requires READ on the dataset.

**Behavior**

The shape matches the list entries. Tokens are never returned."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 200, description = "The link", body = InvenioLinkResponse, example = json!(link_example())),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Dataset access denied", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn get_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<Json<InvenioLinkResponse>> {
    let (_, document_id) = readable(&state, auth, &document_id).await?;
    view(&state, document_id, parse_ulid(&link_id)?).await
}

#[utoipa::path(
    patch, path = "/metadata/{document_id}/invenio/links/{link_id}", tag = "metadata/invenio",
    summary = "Pause, resume or configure a link",
    description = r#"Changes the pause state, publish policy, file access or metadata overrides of a link.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin.

**Behavior**

paused true stops pushing. paused false resumes a paused or failed link and pushes when the dataset differs from the last push. Omitted fields stay unchanged. File access applies to newly created records only.

**Errors**

A metadata value that is not an object returns 400."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    request_body(content = PatchLinkRequest, example = json!({"paused": true})),
    responses(
        (status = 200, description = "The changed link", body = InvenioLinkResponse, example = json!(link_example())),
        (status = 400, description = "Invalid metadata overrides", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn patch_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
    Json(request): Json<PatchLinkRequest>,
) -> ServerResult<Json<InvenioLinkResponse>> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    let metadata_json = match request.metadata {
        Some(value) => Some(metadata_json(&state, Some(value))?),
        None => None,
    };
    let patch = LinkPatch {
        paused: request.paused,
        auto_publish: request.auto_publish,
        public_files: request.public_files,
        metadata_json,
    };
    change(&state, &link, LinkChange::Patch(patch)).await?;
    view(&state, link.document_id, link.link_id).await
}

#[utoipa::path(
    delete, path = "/metadata/{document_id}/invenio/links/{link_id}", tag = "metadata/invenio",
    summary = "Remove a repository link",
    description = r#"Removes the link together with its sealed token.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin.

**Behavior**

Records and drafts in the repository stay. A push that is still running fails once it needs the removed token."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 204, description = "Link and token removed"),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn delete_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    change(&state, &link, LinkChange::Delete).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/invenio/links/{link_id}/push", tag = "metadata/invenio",
    summary = "Push the dataset to its repository now",
    description = r#"Starts a push of the current dataset revision as an export_rocrate job.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin. The job runs as the link creator.

**Behavior**

A failed link is retried. While a push runs, its job is returned instead of a new one. The push updates the open draft or starts a new version after a publish.

**Errors**

A paused link or a dataset without a revision on this node returns 409."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 202, description = "Push job accepted", body = LinkJobResponse, example = json!({
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "status_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX"
        })),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse),
        (status = 409, description = "Link paused or dataset revision unavailable", body = ErrorResponse),
        (status = 503, description = "Job could not be started", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn push_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<LinkJobResponse>)> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    if link.status == LinkStatus::Paused {
        return Err(ServerError::Conflict(
            "resume the link before pushing".into(),
        ));
    }
    if let Some(job_id) = link.active_job {
        return job_response(&state, job_id).await;
    }
    let context = state.get_ctx();
    let event = current_event(&context, link.document_id)
        .await
        .map_err(link_error)?;
    let job_id = start_push(&context, &link, event, false)
        .await
        .map_err(link_error)?;
    job_response(&state, job_id).await
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/invenio/links/{link_id}/publish", tag = "metadata/invenio",
    summary = "Publish the open repository draft",
    description = r#"Pushes the current dataset revision to the open draft and publishes it as an export_rocrate job.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin. The job runs as the link creator.

**Behavior**

Publishing is permanent in the repository and assigns its DOI. The next dataset change starts a new version draft in the same record lineage.

**Errors**

A link without an open draft or with a running push returns 409."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 202, description = "Publish job accepted", body = LinkJobResponse, example = json!({
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "status_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX"
        })),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse),
        (status = 409, description = "No open draft or a push is running", body = ErrorResponse),
        (status = 503, description = "Job could not be started", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn publish_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<LinkJobResponse>)> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    if link.remote.draft_id.is_none() {
        return Err(ServerError::Conflict(
            "the link has no open draft to publish".into(),
        ));
    }
    if link.active_job.is_some() {
        return Err(ServerError::Conflict(
            "a push of this link is running; publish after it finished".into(),
        ));
    }
    let context = state.get_ctx();
    let event = current_event(&context, link.document_id)
        .await
        .map_err(link_error)?;
    let job_id = start_push(&context, &link, event, true)
        .await
        .map_err(link_error)?;
    job_response(&state, job_id).await
}

#[utoipa::path(
    put, path = "/metadata/{document_id}/invenio/links/{link_id}/token", tag = "metadata/invenio",
    summary = "Replace the personal token of a link",
    description = r#"Replaces the sealed personal access token of a link.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group. Only the link creator may replace the token, because pushes run as that user.

**Behavior**

The new token is sealed like the first one and never returned. A link that failed with token_rejected is enabled again and pushes pending changes.

**Errors**

An empty token returns 400; a connector whose endpoint changed since the link was created returns 409."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    request_body(content = RotateTokenRequest, example = json!({"access_token": "<personal-access-token>"})),
    responses(
        (status = 204, description = "Token replaced"),
        (status = 400, description = "Invalid token", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the link creator", body = ErrorResponse),
        (status = 404, description = "Dataset, link or connector not found", body = ErrorResponse),
        (status = 409, description = "Connector endpoint changed", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn rotate_token(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
    Json(request): Json<RotateTokenRequest>,
) -> ServerResult<StatusCode> {
    let (auth, link) = managed(&state, auth, &document_id, &link_id).await?;
    if link.created_by != auth.user_id {
        return Err(ServerError::Forbidden);
    }
    let secret = seal_link_token(
        &state.get_ctx(),
        link.created_by,
        link.group_id,
        link.connector_id,
        link.link_id,
        &request.access_token,
    )
    .await
    .map_err(seal_error)?;
    if secret.endpoint != link.endpoint {
        return Err(ServerError::Conflict(
            "the connector endpoint changed; create a new link".into(),
        ));
    }
    change(&state, &link, LinkChange::Rotate(secret)).await?;
    Ok(StatusCode::NO_CONTENT)
}
