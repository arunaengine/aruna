//! Checks a link's repository lineage before a push and records the push on the link after it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::invenio::InvenioError;
use aruna_core::invenio::{
    InvenioDestination, InvenioRecord, LinkFailure, LinkTarget, PushOutcome, validate_id,
};
use aruna_core::structs::execution::job::{
    ExportRoCrateSpec, JobError, JobErrorKind, JobResultPayload,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::secondary_id::{SecondaryIdKind, SecondaryIdentifier};
use http::Method;

use super::links::{LinkChange, LinkError, change_link, read_link, read_secret};
use super::{TransferError, connect};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::export::{ExportCheckpoint, persist_checkpoint, read_export_checkpoint};
use crate::metadata::AuthToken;
use crate::metadata::api::MetadataApiError;
use crate::metadata::persistent_id::forward::add_identifiers_routed;

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
        .ok_or_else(|| permanent("the repository link was removed"))?;
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
        read(client.url(&["records", draft, "draft"])?).await?;
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
    let push = match &outcome {
        JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) => {
            let (Some(record), Some((event_id, dataset_digest))) =
                (result.repository.clone(), checkpoint.pushed_revision())
            else {
                return outcome;
            };
            if let Err(error) = register(ctx, spec, &link.endpoint, &record).await {
                return retry(error);
            }
            PushOutcome::Pushed {
                record,
                event_id,
                dataset_digest,
            }
        }
        JobRunOutcome::Failed(error) if error.kind == JobErrorKind::Permanent => {
            PushOutcome::Failed(checkpoint.push_failure(&error.message))
        }
        JobRunOutcome::Cancelled => PushOutcome::Cancelled,
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

/// Registers the record's DOI, id and parent id as secondary identifiers of the dataset.
async fn register(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    endpoint: &str,
    record: &InvenioRecord,
) -> Result<(), String> {
    let identifiers = [
        (SecondaryIdKind::Doi, record.doi.as_deref()),
        (SecondaryIdKind::InvenioRecord, Some(record.id.as_str())),
        (
            SecondaryIdKind::InvenioParent,
            Some(record.parent_id.as_str()),
        ),
    ]
    .into_iter()
    .filter_map(|(kind, value)| SecondaryIdentifier::new(kind, value?, Some(endpoint)).ok())
    .collect();
    let result = add_identifiers_routed(
        &ctx.driver,
        spec.auth_context.realm_id,
        spec.document_id,
        identifiers,
        aruna_core::time::unix_timestamp_millis(),
        Some(AuthToken::internal(spec.auth_context.clone())),
    )
    .await;
    match result {
        Ok(_) => Ok(()),
        // The pushed record stays valid without the lookup entries.
        Err(error @ (MetadataApiError::Forbidden | MetadataApiError::Unauthorized)) => {
            tracing::warn!(document_id = %spec.document_id, %error, "push identifiers refused");
            Ok(())
        }
        Err(error) => Err(format!(
            "registering repository identifiers failed: {error}"
        )),
    }
}

fn permanent(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}

fn retry(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}
