//! Checks a link's repository lineage before a push and records the push on the link after it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::repository::invenio::validate_id;
use aruna_core::repository::{
    InvenioDestination, InvenioRecord, LinkFailure, LinkReview, LinkStatus, LinkTarget,
    PushOutcome, RemoteState, RepositoryLink,
};
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobError, JobErrorKind};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use http::Method;
use serde_json::Value;

use super::export::{file_keys, record_from};
use super::links::{LinkChange, LinkError, change_link, read_link, read_secret};
use super::{TransferError, connect};
use crate::driver::DriverContext;
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::export::{ExportCheckpoint, persist_checkpoint, read_export_checkpoint};

/// The destination with the link's token, after the link confirmed this job and lineage.
pub(super) async fn prepare(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    target: &LinkTarget,
    checkpoint: &mut ExportCheckpoint,
) -> Result<InvenioDestination, TransferError> {
    let storage = &ctx.driver.storage_handle;
    let retry = |error: LinkError| TransferError::Retryable(error.to_string());
    let link = read_link(storage, spec.document_id, target.link_id)
        .await
        .map_err(retry)?
        .filter(|link| link.status != LinkStatus::Paused)
        .ok_or(TransferError::Cancelled)?;
    match link.active_job {
        Some(job_id) if job_id == ctx.job_id => {}
        None => {
            return Err(TransferError::Retryable(
                "the link has not started this push".into(),
            ));
        }
        Some(_) => return Err(permanent("a newer push of this link replaced this job")),
    }
    let credential = read_secret(storage, target.link_id)
        .await
        .map_err(retry)?
        .filter(|secret| secret.link_id == Some(link.link_id) && secret.endpoint == link.endpoint)
        .ok_or_else(|| permanent("the link token was removed"))?;
    let mut destination = destination.clone();
    destination.credential = Some(credential);
    if checkpoint.repository.is_none()
        && checkpoint.repository_base.is_none()
        && let Some(base) = check_lineage(ctx, spec, &destination, target).await?
    {
        checkpoint.repository_base = Some(base);
        persist_checkpoint(ctx, checkpoint)
            .await
            .map_err(TransferError::Retryable)?;
    }
    destination.new_version = destination
        .new_version
        .or_else(|| checkpoint.repository_base.clone());
    Ok(destination)
}

/// Fails as remote changed when the lineage moved outside Aruna; returns a parent's latest version.
async fn check_lineage(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    target: &LinkTarget,
) -> Result<Option<String>, TransferError> {
    let client = connect(
        &ctx.driver,
        &spec.auth_context,
        destination.group_id,
        destination.connector_id,
        Permission::WRITE,
        spec.limits.metadata_bytes,
        destination.credential.as_ref(),
    )
    .await?;
    let changed = || TransferError::Refused(LinkFailure::RemoteChanged);
    let read = async |url| match client.json(Method::GET, url, None).await {
        Err(InvenioError::Status(404 | 410)) => Err(changed()),
        result => result.map_err(TransferError::from),
    };
    if let Some(draft) = &destination.draft_id {
        validate_id(draft)?;
        let current = read(client.url(&["records", draft, "draft"])?).await?;
        // Another revision than this link's last write means someone edited the draft.
        if target
            .revision_id
            .is_some_and(|revision| current["revision_id"].as_u64() != Some(revision))
        {
            return Err(changed());
        }
    }
    // The record's own version flag comes from the database, so no search index lag applies.
    if let Some(published) = &target.published_id {
        validate_id(published)?;
        let record = read(client.url(&["records", published])?).await?;
        if record["versions"]["is_latest"] != true {
            return Err(changed());
        }
        return Ok(None);
    }
    let Some(parent) = target
        .parent_id
        .as_ref()
        .filter(|_| destination.draft_id.is_none())
    else {
        return Ok(None);
    };
    validate_id(parent)?;
    let mut url = client.url(&["records"])?;
    url.query_pairs_mut()
        .append_pair("q", &format!("parent.id:{parent}"))
        .append_pair("size", "1");
    let page = read(url).await?;
    let hit = &page["hits"]["hits"][0];
    match hit["id"].as_str() {
        Some(id) if hit["parent"]["id"] == parent.as_str() => {
            validate_id(id)?;
            Ok(Some(id.to_string()))
        }
        _ => Err(changed()),
    }
}

/// Records the outcome on the link; a failed record keeps the job retrying.
pub(crate) async fn settle(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    target: &LinkTarget,
    outcome: JobRunOutcome,
) -> JobRunOutcome {
    let storage = &ctx.driver.storage_handle;
    let link = match read_link(storage, spec.document_id, target.link_id).await {
        Ok(Some(link)) if link.active_job == Some(ctx.job_id) => link,
        Ok(_) => return outcome,
        Err(error) => return retry(error.to_string()),
    };
    let checkpoint = match read_export_checkpoint(ctx, ctx.job_id).await {
        Ok(checkpoint) => checkpoint.unwrap_or_default(),
        Err(error) => return retry(error),
    };
    let settled = match &outcome {
        JobRunOutcome::Failed(error) => error.kind == JobErrorKind::Permanent,
        JobRunOutcome::Succeeded(_) | JobRunOutcome::Cancelled => true,
        _ => false,
    };
    // A push that reached the repository counts as pushed, even when a later step failed.
    let push = match (&outcome, checkpoint.pushed_outcome()) {
        _ if !settled => return outcome,
        (_, Some(pushed)) => pushed,
        (JobRunOutcome::Failed(error), None) => {
            PushOutcome::Failed(checkpoint.push_failure(&error.message))
        }
        (JobRunOutcome::Cancelled, None) => PushOutcome::Cancelled,
        _ => return outcome,
    };
    let requeue = match &push {
        PushOutcome::Pushed { event_id, .. } => !matches!(
            super::link_queue::current_event(&ctx.driver, spec.document_id).await,
            Ok(current) if current == *event_id
        ),
        _ => false,
    };
    let change = LinkChange::Finish {
        job_id: ctx.job_id,
        outcome: Box::new(push),
        requeue,
    };
    match change_link(ctx.driver.as_ref(), &link, change).await {
        Ok(_) | Err(LinkError::NotFound) => outcome,
        // A cancelled job cannot retry; the queue drain settles its link later.
        Err(_) if matches!(outcome, JobRunOutcome::Cancelled) => outcome,
        Err(error) => retry(format!("recording the link push failed: {error}")),
    }
}

/// Stops a push once its link is gone, paused or runs another job; checked before remote writes.
pub(super) async fn guard(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
) -> Result<(), TransferError> {
    let Some(target) = &destination.link else {
        return Ok(());
    };
    match read_link(&ctx.driver.storage_handle, spec.document_id, target.link_id).await {
        Ok(Some(link))
            if link.active_job == Some(ctx.job_id) && link.status != LinkStatus::Paused =>
        {
            Ok(())
        }
        Ok(_) => Err(TransferError::Cancelled),
        Err(error) => Err(TransferError::Retryable(error.to_string())),
    }
}

/// Stores the job's draft on the link, so a failed push continues the same draft.
pub(super) async fn record_draft(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    target: &LinkTarget,
    record: &InvenioRecord,
) -> Result<(), TransferError> {
    let storage = &ctx.driver.storage_handle;
    let link = read_link(storage, spec.document_id, target.link_id)
        .await
        .map_err(|error| TransferError::Retryable(error.to_string()))?
        .ok_or(TransferError::Cancelled)?;
    let change = LinkChange::Draft {
        job_id: ctx.job_id,
        record: Box::new(record.clone()),
    };
    match change_link(ctx.driver.as_ref(), &link, change).await {
        Ok(_) => Ok(()),
        Err(LinkError::NotFound | LinkError::Busy(_)) => Err(TransferError::Cancelled),
        Err(error) => Err(TransferError::Retryable(error.to_string())),
    }
}

/// The identity pushes of a link run as: its creator.
pub(crate) fn creator_auth(link: &RepositoryLink) -> AuthContext {
    AuthContext {
        user_id: link.created_by,
        realm_id: link.created_by.realm_id,
        path_restrictions: None,
        session: None,
    }
}

async fn link_client<'a>(
    context: &'a DriverContext,
    link: &RepositoryLink,
) -> Result<InvenioClient<'a>, TransferError> {
    let credential = read_secret(&context.storage_handle, link.link_id)
        .await
        .map_err(|error| TransferError::Retryable(error.to_string()))?
        .filter(|secret| secret.link_id == Some(link.link_id) && secret.endpoint == link.endpoint)
        .ok_or_else(|| permanent("the link token was removed"))?;
    let auth = creator_auth(link);
    connect(
        context,
        &auth,
        link.group_id,
        link.connector_id,
        Permission::WRITE,
        link.limits.metadata_bytes,
        Some(&credential),
    )
    .await
}

async fn read_optional(
    client: &InvenioClient<'_>,
    path: &[&str],
) -> Result<Option<Value>, TransferError> {
    match client.json(Method::GET, client.url(path)?, None).await {
        Ok(value) => Ok(Some(value)),
        Err(InvenioError::Status(404 | 410)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn review_of(record: &Value) -> Option<LinkReview> {
    match record["parent"]["review"]["status"].as_str()? {
        "created" | "submitted" => Some(LinkReview::Pending),
        "accepted" => Some(LinkReview::Accepted),
        "declined" | "cancelled" | "expired" => Some(LinkReview::Declined),
        _ => None,
    }
}

/// The repository's answer to a pending review; `None` while the community still decides.
pub(crate) async fn review_state(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<Option<RemoteState>, TransferError> {
    let Some(draft_id) = link.remote.draft_id.as_deref() else {
        return Ok(None);
    };
    validate_id(draft_id)?;
    let client = link_client(context, link).await?;
    if let Some(draft) = read_optional(&client, &["records", draft_id, "draft"]).await? {
        let review = review_of(&draft).unwrap_or(LinkReview::Declined);
        if review == LinkReview::Pending {
            return Ok(None);
        }
        return Ok(Some(RemoteState {
            draft: Some(record_from(&client, &draft)?),
            latest: None,
            review,
            files: file_keys(&client, draft_id, false).await?,
        }));
    }
    // An accepted review publishes the draft, which then leaves the draft endpoint.
    let Some(record) = read_optional(&client, &["records", draft_id]).await? else {
        return Err(TransferError::Refused(LinkFailure::RemoteChanged));
    };
    Ok(Some(RemoteState {
        draft: None,
        latest: Some(record_from(&client, &record)?),
        review: LinkReview::Accepted,
        files: file_keys(&client, draft_id, true).await?,
    }))
}

/// The repository's current draft and latest published version, for accepting remote edits.
pub async fn remote_state(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<RemoteState, TransferError> {
    let client = link_client(context, link).await?;
    let draft = match link.remote.draft_id.as_deref() {
        Some(id) => {
            validate_id(id)?;
            read_optional(&client, &["records", id, "draft"]).await?
        }
        None => None,
    };
    let anchor = link
        .remote
        .record_id
        .as_deref()
        .or(link.remote.draft_id.as_deref());
    let mut latest = None;
    if let Some(anchor) = anchor {
        validate_id(anchor)?;
        let mut url = client.url(&["records", anchor, "versions"])?;
        url.query_pairs_mut()
            .append_pair("size", "1")
            .append_pair("sort", "version");
        let page = match client.json(Method::GET, url, None).await {
            Ok(page) => page,
            Err(InvenioError::Status(404 | 410)) => Value::Null,
            Err(error) => return Err(error.into()),
        };
        let hit = &page["hits"]["hits"][0];
        if hit.is_object() {
            latest = Some(record_from(&client, hit)?);
        }
    }
    let review = match &draft {
        Some(draft) => review_of(draft).unwrap_or(LinkReview::None),
        None if link.remote.review == LinkReview::Pending && latest.is_some() => {
            LinkReview::Accepted
        }
        None if link.remote.review == LinkReview::Pending => LinkReview::None,
        None => link.remote.review,
    };
    let files = match (&draft, &latest) {
        (Some(draft), _) => file_keys(&client, &record_from(&client, draft)?.id, false).await?,
        (None, Some(latest)) => file_keys(&client, &latest.id, true).await?,
        (None, None) => Vec::new(),
    };
    Ok(RemoteState {
        draft: draft
            .map(|draft| record_from(&client, &draft))
            .transpose()?,
        latest,
        review,
        files,
    })
}

fn permanent(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}

fn retry(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}
