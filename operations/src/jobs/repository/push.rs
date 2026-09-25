//! Checks a link's repository lineage before a push and records the push on the link after it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::repository::{
    LinkStatus, LinkTarget, PushOutcome, RepositoryDestination, RepositoryLink, RepositoryRecord,
};
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobError, JobErrorKind};
use aruna_core::structs::identity::auth::AuthContext;

use super::TransferError;
use super::invenio::remote::check_lineage;
use super::links::{LinkChange, LinkError, change_link, read_link, read_secret};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::export::{ExportCheckpoint, persist_checkpoint, read_export_checkpoint};

/// The destination with the link's token, after the link confirmed this job and lineage.
pub(super) async fn prepare(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &RepositoryDestination,
    target: &LinkTarget,
    checkpoint: &mut ExportCheckpoint,
) -> Result<RepositoryDestination, TransferError> {
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
    destination.published_id = destination
        .published_id
        .or_else(|| checkpoint.repository_base.clone());
    Ok(destination)
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
    destination: &RepositoryDestination,
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
    record: &RepositoryRecord,
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

fn permanent(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}

fn retry(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}
