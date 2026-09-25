//! Checks pull links against their record lineage and starts imports of new versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::repository::{
    LinkFailure, LinkStatus, PullCheck, PushOutcome, RepositoryLink, RepositoryPull,
};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, JobId, JobState,
};

use super::link_queue::ensure_holder;
use super::links::{LinkChange, LinkError, change_link, ensure_lineage};
use super::push::creator_auth;
use super::{TransferError, latest_version};
use crate::driver::DriverContext;
use crate::jobs::service::submit_rocrate_import;
use crate::jobs::store::read_job_record;
use crate::jobs::submit::SubmitJobError;
use crate::metadata::get_document::load_document_record;

/// How often a check waits for a running pull before it looks again.
const ACTIVE_RETRY_MS: u64 = 60_000;

/// Checks one due pull link and starts an automatic update; returns when to look again if
/// no link change moved its queued check.
pub(super) async fn check_due(
    context: &Arc<DriverContext>,
    link: &RepositoryLink,
    now: u64,
) -> Result<Option<u64>, LinkError> {
    if let Some(job_id) = link.active_job {
        return settle_stale(context, link, job_id, now).await;
    }
    let Some(stored) = check_now(context, link).await? else {
        return Ok(None);
    };
    if stored.auto_pulls() {
        match start_pull(context, &stored).await {
            Ok(_) => {}
            Err(LinkError::JobLimit(_)) => return Ok(Some(now.saturating_add(ACTIVE_RETRY_MS))),
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

/// A pull job that ended without recording its outcome, for example cancelled while queued.
async fn settle_stale(
    context: &DriverContext,
    link: &RepositoryLink,
    job_id: JobId,
    now: u64,
) -> Result<Option<u64>, LinkError> {
    let record = read_job_record(&context.storage_handle, job_id, None)
        .await
        .map_err(LinkError::Unexpected)?;
    let outcome = match record {
        Some(record) if !record.state.is_terminal() => {
            return Ok(Some(now.saturating_add(ACTIVE_RETRY_MS)));
        }
        Some(record) if record.state == JobState::Failed => {
            let message = record
                .last_error
                .map_or("pull job failed".to_string(), |error| error.message);
            PushOutcome::Failed(LinkFailure::Other(message))
        }
        Some(_) => PushOutcome::Cancelled,
        None => PushOutcome::Failed(LinkFailure::Other("pull job missing".into())),
    };
    let change = LinkChange::Finish {
        job_id,
        outcome: Box::new(outcome),
        requeue: false,
    };
    change_link(context, link, change).await?;
    Ok(None)
}

/// Asks the repository for the lineage's latest version and stores the answer on the link.
/// A refusal, such as a withdrawn record, fails the link. Returns the stored link.
pub async fn check_now(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<Option<RepositoryLink>, LinkError> {
    let change = match latest_version(RepositoryConnectorKind::Invenio, context, link).await {
        Ok(check) => LinkChange::Checked(check),
        Err(TransferError::Refused(reason)) => LinkChange::Fail(reason),
        Err(TransferError::Permanent(message)) => LinkChange::Fail(LinkFailure::Other(message)),
        Err(_) => LinkChange::Checked(PullCheck::Unavailable),
    };
    change_link(context, link, change).await
}

/// Submits an import of the lineage's latest version into the linked dataset, as the creator.
pub async fn start_pull(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<JobId, LinkError> {
    let pull = link
        .pull()
        .ok_or_else(|| LinkError::Unexpected("the link does not pull".into()))?;
    let record_id = pull
        .latest_remote_id
        .clone()
        .or_else(|| link.remote.record_id.clone())
        .ok_or(LinkError::NoRevision)?;
    ensure_holder(context, link).await?;
    // A failed link runs again only if no enabled link follows its lineage the other way.
    if matches!(link.status, LinkStatus::Failed { .. }) {
        Box::pin(ensure_lineage(&context.storage_handle, link)).await?;
    }
    let document = load_document_record(context, link.document_id)
        .await
        .map_err(|error| LinkError::Unexpected(format!("{error:?}")))?
        .ok_or(LinkError::NotHolder)?;
    let spec = ImportRoCrateSpec {
        auth_context: creator_auth(link),
        source: ImportRoCrateSource::Repository {
            group_id: link.group_id,
            connector_id: link.connector_id,
            record_id: record_id.clone(),
            options: pull.options.clone(),
            pull: Some(RepositoryPull::Update {
                link_id: link.link_id,
            }),
        },
        target: pull.target.clone(),
        metadata: ImportMetadataTarget {
            group_id: document.group_id,
            path: document.document_path,
            public: document.public,
        },
        limits: link.limits.clone(),
        document_id: link.document_id,
    };
    let owner = context
        .net_handle
        .as_ref()
        .map_or(link.owner_node, |net| net.node_id());
    let key = format!(
        "invenio-pull/{}/{record_id}/{}",
        link.link_id, link.sequence
    );
    let submitted = submit_rocrate_import(context, spec, owner, Some(key))
        .await
        .map_err(|error| match error {
            SubmitJobError::ActiveJobLimit { limit } => LinkError::JobLimit(limit),
            error => LinkError::Submit(error.to_string()),
        })?;
    change_link(context, link, LinkChange::Begin(submitted.job_id)).await?;
    Ok(submitted.job_id)
}
