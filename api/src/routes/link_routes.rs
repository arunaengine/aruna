//! Changes one repository link: read, configure, remove, push, publish and replace its token.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::repository::{LinkPatch, LinkStatus, RepositoryLink};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::jobs::repository::link_queue::{current_event, refresh_review, start_push};
use aruna_operations::jobs::repository::links::LinkChange;
use aruna_operations::jobs::repository::pull::{check_now, start_pull};
use aruna_operations::jobs::repository::{Action, TransferError, remote_state, seal_link_token};
use aruna_operations::jobs::service::cancel_owned_job;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use utoipa_axum::{router::OpenApiRouter, routes};

use super::repository::links::{
    LinkJobResponse, PatchLinkRequest, RepositoryLinkResponse, RotateTokenRequest, change,
    ensure_capable, job_response, link_error, link_example, managed, metadata_json, parse_ulid,
    readable, seal_error, view,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(get_link, patch_link, delete_link))
        .routes(routes!(push_link))
        .routes(routes!(publish_link))
        .routes(routes!(rotate_token))
        .routes(routes!(accept_remote))
        .routes(routes!(pull_link))
}

/// Publishing and the push settings act for the creator's repository account, so only the
/// creator may change them.
fn ensure_creator(auth: &AuthContext, link: &RepositoryLink) -> ServerResult<()> {
    if link.created_by != auth.user_id {
        return Err(ServerError::Forbidden);
    }
    Ok(())
}

/// Push actions do not apply to a link that pulls.
fn ensure_push(link: &RepositoryLink) -> ServerResult<()> {
    if link.pull().is_some() {
        return Err(ServerError::Conflict(
            "this link pulls from the repository; use the pull route".into(),
        ));
    }
    Ok(())
}

/// Stops the link's running push; the job also stops by itself at its next remote write.
async fn cancel_push(state: &ServerState, link: &RepositoryLink) {
    let Some(job_id) = link.active_job else {
        return;
    };
    let runtime = state.jobs_runtime();
    let context = state.get_ctx();
    let cancelled = Box::pin(cancel_owned_job(
        &context,
        &runtime,
        link.created_by,
        job_id,
    ));
    if let Err(error) = cancelled.await {
        tracing::warn!(%job_id, %error, "Cancelling the link push failed");
    }
}

#[utoipa::path(
    get, path = "/metadata/{document_id}/repository/links/{link_id}", tag = "metadata/repository",
    summary = "Read one repository link",
    description = r#"Returns one repository link of the dataset.

**Authentication**

Requires READ on the dataset.

**Behavior**

The shape matches the list entries. Tokens are never returned."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 200, description = "The link", body = RepositoryLinkResponse, example = json!(link_example())),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Dataset access denied", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn get_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<Json<RepositoryLinkResponse>> {
    let (_, document_id) = readable(&state, auth, &document_id).await?;
    view(&state, document_id, parse_ulid(&link_id)?).await
}

#[utoipa::path(
    patch, path = "/metadata/{document_id}/repository/links/{link_id}", tag = "metadata/repository",
    summary = "Pause, resume or configure a link",
    description = r#"Changes the pause state, publish policy, file access or metadata overrides of a link.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group. The link creator or a group admin may pause and resume; only the creator may change auto_publish, public_files or metadata.

**Behavior**

paused true stops pushing and cancels a running push. paused false resumes a paused or failed link and pushes when the dataset differs from the last push. Omitted fields stay unchanged. public_files applies to the open draft with the next push.

On a pull link, paused true cancels a running pull and paused false checks the repository again. auto_update, which only the creator may change, imports new versions without asking.

**Errors**

A metadata value that is not an object returns 400. auto_update on a push link, or auto_publish, public_files or metadata on a pull link, returns 400."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    request_body(content = PatchLinkRequest, example = json!({"paused": true})),
    responses(
        (status = 200, description = "The changed link", body = RepositoryLinkResponse, example = json!(link_example())),
        (status = 400, description = "Invalid metadata overrides, or settings of the other link direction", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin, or settings changed by someone else than the creator", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse),
        (status = 409, description = "The link is managed on its owner node", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn patch_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
    Json(request): Json<PatchLinkRequest>,
) -> ServerResult<Json<RepositoryLinkResponse>> {
    let (auth, link) = managed(&state, auth, &document_id, &link_id).await?;
    let push_settings = request.auto_publish.is_some()
        || request.public_files.is_some()
        || request.metadata.is_some();
    let pulls = link.pull().is_some();
    if (pulls && push_settings) || (!pulls && request.auto_update.is_some()) {
        return Err(ServerError::BadRequestReason(
            "auto_update applies to pull links; auto_publish, public_files and metadata to push links"
                .into(),
        ));
    }
    if push_settings || request.auto_update.is_some() {
        ensure_creator(&auth, &link)?;
    }
    let metadata_json = match request.metadata {
        Some(value) => Some(metadata_json(&state, Some(value))?),
        None => None,
    };
    let patch = LinkPatch {
        paused: request.paused,
        auto_publish: request.auto_publish,
        public_files: request.public_files,
        metadata_json,
        auto_update: request.auto_update,
    };
    let pause = patch.paused == Some(true);
    change(&state, &link, LinkChange::Patch(patch)).await?;
    if pause {
        cancel_push(&state, &link).await;
    }
    view(&state, link.document_id, link.link_id).await
}

#[utoipa::path(
    delete, path = "/metadata/{document_id}/repository/links/{link_id}", tag = "metadata/repository",
    summary = "Remove a repository link",
    description = r#"Removes the link together with its sealed token.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin.

**Behavior**

Records and drafts in the repository stay. A running push is cancelled and stops before its next repository write."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 204, description = "Link and token removed"),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse),
        (status = 409, description = "The link is managed on its owner node", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn delete_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    change(&state, &link, LinkChange::Delete).await?;
    cancel_push(&state, &link).await;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/repository/links/{link_id}/push", tag = "metadata/repository",
    summary = "Push the dataset to its repository now",
    description = r#"Starts a push of the current dataset revision as an export_rocrate job.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin. The job runs as the link creator.

**Behavior**

A failed link is retried. While a push runs, its job is returned instead of a new one. The push updates the open draft or starts a new version after a publish.

**Errors**

A paused link, a dataset without a revision, a creator at the active job limit or a node that is not the link's owner returns 409. The message of the last case names owner_node_url. A node that no longer holds the dataset fails the link with owner_not_holder and returns 409."#,
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
        (status = 409, description = "Link paused, dataset revision unavailable, job limit reached, or the link is managed on its owner node", body = ErrorResponse),
        (status = 503, description = "Job could not be started", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn push_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<LinkJobResponse>)> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    ensure_push(&link)?;
    let link = refresh_review(state.get_ctx().as_ref(), &link)
        .await
        .map_err(link_error)?;
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
    post, path = "/metadata/{document_id}/repository/links/{link_id}/publish", tag = "metadata/repository",
    summary = "Publish the open repository draft",
    description = r#"Pushes the current dataset revision to the open draft and publishes it as an export_rocrate job.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator. The job runs as the link creator.

**Behavior**

Publishing is permanent in the repository and registers the reserved DOI. With a connector community the first version is submitted for review instead, and review shows pending until the community decides. The next dataset change starts a new version draft in the same record lineage.

**Errors**

A link without an open draft, a running push, a creator at the active job limit or a node that is not the link's owner returns 409."#,
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
        (status = 400, description = "The repository kind cannot publish drafts (code not_supported)", body = ErrorResponse),
        (status = 403, description = "Not the link creator", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse),
        (status = 409, description = "No open draft, a push is running, job limit reached, or the link is managed on its owner node", body = ErrorResponse),
        (status = 503, description = "Job could not be started", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn publish_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<LinkJobResponse>)> {
    let (auth, link) = managed(&state, auth, &document_id, &link_id).await?;
    ensure_creator(&auth, &link)?;
    ensure_push(&link)?;
    ensure_capable(link.kind, Action::Drafts)?;
    let link = refresh_review(state.get_ctx().as_ref(), &link)
        .await
        .map_err(link_error)?;
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
    put, path = "/metadata/{document_id}/repository/links/{link_id}/token", tag = "metadata/repository",
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
        (status = 409, description = "Connector endpoint changed or the link is managed on its owner node", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn rotate_token(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
    Json(request): Json<RotateTokenRequest>,
) -> ServerResult<StatusCode> {
    let (auth, link) = managed(&state, auth, &document_id, &link_id).await?;
    ensure_creator(&auth, &link)?;
    ensure_push(&link)?;
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

#[utoipa::path(
    post, path = "/metadata/{document_id}/repository/links/{link_id}/accept-remote", tag = "metadata/repository",
    summary = "Accept the repository's current state",
    description = r#"Makes the repository's current draft and latest published version the link's new base.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin. The repository is read with the creator's token.

**Behavior**

Use it after remote_changed: edits made in the repository become the base, a failed link is enabled again and the next dataset change pushes on top of them. Files in the open draft count as pushed, so later pushes may replace or remove them.

**Errors**

A running push returns 409. A rejected token returns 409 with reason token_rejected; an unreachable repository 503."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 200, description = "The link with its new base", body = RepositoryLinkResponse, example = json!(link_example())),
        (status = 400, description = "The repository kind has no drafts to accept (code not_supported)", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse),
        (status = 409, description = "A push is running, the token was rejected, or the link is managed on its owner node", body = ErrorResponse),
        (status = 502, description = "The repository answered unexpectedly", body = ErrorResponse),
        (status = 503, description = "The repository is unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn accept_remote(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<Json<RepositoryLinkResponse>> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    ensure_push(&link)?;
    ensure_capable(link.kind, Action::Drafts)?;
    if link.active_job.is_some() {
        return Err(ServerError::Conflict(
            "a push of this link is running; accept after it finished".into(),
        ));
    }
    let remote = remote_state(link.kind, state.get_ctx().as_ref(), &link)
        .await
        .map_err(|error| match error {
            TransferError::Refused(_) => ServerError::Conflict(error.to_string()),
            TransferError::Permanent(message) => ServerError::BadGatewayReason(message),
            error => ServerError::ServiceUnavailableReason(error.to_string()),
        })?;
    change(&state, &link, LinkChange::Accept(Box::new(remote))).await?;
    view(&state, link.document_id, link.link_id).await
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/repository/links/{link_id}/pull", tag = "metadata/repository",
    summary = "Import the repository's new version now",
    description = r#"Checks the record lineage of a pull link and imports its latest version into the dataset as an import_rocrate job.

**Authentication**

Requires READ on the dataset and WRITE on the metadata path of the connector group, as the link creator or a group admin. The job runs as the link creator and needs WRITE on the dataset and its target bucket.

**Behavior**

The new version becomes a new versions/{id}/ part with its files, in the mode of the first import. The dataset root takes the new version's metadata through a normal metadata update, and the version's DOIs and ids are registered as imported identifiers.

This also applies an update that local_changed held back: the root metadata is overwritten, while local parts and files stay. A running pull returns its job.

**Errors**

A paused or push link, a dataset that already holds the latest version, a refused repository request, a creator at the active job limit or a node that is not the link's owner returns 409. An unreachable repository returns 503."#,
    params(
        ("document_id" = String, Path, description = "Metadata document identifier"),
        ("link_id" = String, Path, description = "Link identifier")
    ),
    responses(
        (status = 202, description = "Pull job accepted", body = LinkJobResponse, example = json!({
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "status_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX"
        })),
        (status = 400, description = "The repository kind has no pull links (code not_supported)", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Not the creator or a group admin", body = ErrorResponse),
        (status = 404, description = "Dataset or link not found", body = ErrorResponse),
        (status = 409, description = "Nothing to pull, link paused or pushing, repository refused, job limit reached, or the link is managed on its owner node", body = ErrorResponse),
        (status = 503, description = "Repository unavailable or job could not be started", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn pull_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((document_id, link_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<LinkJobResponse>)> {
    let (_, link) = managed(&state, auth, &document_id, &link_id).await?;
    if link.pull().is_none() {
        return Err(ServerError::Conflict(
            "this link pushes to the repository; use the push route".into(),
        ));
    }
    ensure_capable(link.kind, Action::Pull)?;
    if link.status == LinkStatus::Paused {
        return Err(ServerError::Conflict(
            "resume the link before pulling".into(),
        ));
    }
    if let Some(job_id) = link.active_job {
        return job_response(&state, job_id).await;
    }
    let context = state.get_ctx();
    let checked = check_now(&context, &link)
        .await
        .map_err(link_error)?
        .ok_or(ServerError::NotFound)?;
    if let LinkStatus::Failed { reason } = &checked.status {
        return Err(ServerError::Conflict(format!(
            "the repository refused the check ({})",
            reason.reason()
        )));
    }
    if checked.pull().is_some_and(|pull| pull.failures > 0) {
        return Err(ServerError::ServiceUnavailableReason(
            "the repository is unavailable".into(),
        ));
    }
    if !checked.update_available() {
        return Err(ServerError::Conflict(
            "the dataset already holds the latest version".into(),
        ));
    }
    let job_id = start_pull(&context, &checked).await.map_err(link_error)?;
    job_response(&state, job_id).await
}
