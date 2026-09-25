//! Checks pull links against their record lineage and starts imports of new versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_blob::invenio::InvenioError;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::invenio::{
    InvenioLink, InvenioPull, LinkFailure, LinkStatus, PullCheck, PushOutcome,
};
use aruna_core::keyspaces::INVENIO_LINK_KEYSPACE;
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, JobId, JobState,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use aruna_tasks::TaskHandle;
use http::Method;
use tracing::warn;

use super::link_queue::{current_event, document_gone, ensure_holder};
use super::links::{LinkChange, LinkError, change_link, ensure_lineage, schedule_pulls};
use super::push::creator_auth;
use super::{TransferError, connect};
use crate::driver::DriverContext;
use crate::jobs::service::submit_rocrate_import;
use crate::jobs::store::read_job_record;
use crate::jobs::submit::SubmitJobError;
use crate::metadata::get_document::load_document_record;
use crate::tasks::queue_backoff::{due_after, min_due_at};

const LINK_PAGE: usize = 256;
/// How often a check waits for a running pull before it looks again.
const ACTIVE_RETRY_MS: u64 = 60_000;
const ERROR_RETRY_MS: u64 = 300_000;

/// Checks every due pull link this node owns and returns the wait until the next check.
pub async fn drain_pulls(context: &Arc<DriverContext>) -> Result<Option<Duration>, LinkError> {
    let now = unix_timestamp_millis();
    let mut next = None;
    for link in local_pulls(context).await? {
        let due = link.pull().map_or(u64::MAX, |pull| pull.next_check_ms);
        if due > now {
            next = min_due_at(next, due);
            continue;
        }
        let retry_at = match check_link(context, &link, now).await {
            Ok(retry_at) => retry_at,
            Err(error) => {
                warn!(link_id = %link.link_id, %error, "Invenio pull check failed");
                Some(now.saturating_add(ERROR_RETRY_MS))
            }
        };
        if let Some(retry_at) = retry_at {
            next = min_due_at(next, retry_at);
        }
    }
    Ok(next.map(|due| due_after(unix_timestamp_millis(), due)))
}

/// Arms the pull check for the earliest due link this node owns.
pub async fn restore_pull_timer(context: &DriverContext, task_handle: &TaskHandle) {
    let links = match local_pulls(context).await {
        Ok(links) => links,
        Err(error) => {
            warn!(%error, "Failed to scan the Invenio pull links");
            return;
        }
    };
    let Some(due) = links
        .iter()
        .filter_map(|link| link.pull().map(|pull| pull.next_check_ms))
        .min()
    else {
        return;
    };
    let after = due_after(unix_timestamp_millis(), due);
    if let Event::Task(TaskEvent::Error { message, .. }) =
        task_handle.send_effect(schedule_pulls(after)).await
    {
        warn!(%message, "Failed to arm the Invenio pull check");
    }
}

/// The enabled pull links this node owns.
async fn local_pulls(context: &DriverContext) -> Result<Vec<InvenioLink>, LinkError> {
    let local = context.net_handle.as_ref().map(|net| net.node_id());
    let mut links = Vec::new();
    let mut start = None;
    loop {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: INVENIO_LINK_KEYSPACE.to_string(),
                prefix: None,
                start: start.take().map(IterStart::After),
                limit: LINK_PAGE,
                txn_id: None,
            })
            .await;
        let (values, next_start) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => return Err(LinkError::Unexpected(format!("{other:?}"))),
        };
        for (_, value) in values {
            let link = InvenioLink::from_bytes(&value)?;
            if link.pull().is_some()
                && link.status == LinkStatus::Enabled
                && local.is_none_or(|node| node == link.owner_node)
            {
                links.push(link);
            }
        }
        match next_start {
            Some(key) => start = Some(key),
            None => return Ok(links),
        }
    }
}

/// Checks one due link and starts an automatic update; returns when to look again.
async fn check_link(
    context: &Arc<DriverContext>,
    link: &InvenioLink,
    now: u64,
) -> Result<Option<u64>, LinkError> {
    if document_gone(&context.storage_handle, link.document_id).await? {
        change_link(context, link, LinkChange::Delete).await?;
        return Ok(None);
    }
    if let Err(LinkError::NotHolder) = ensure_holder(context, link).await {
        return Ok(None);
    }
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
    Ok(stored.pull().map(|pull| pull.next_check_ms))
}

/// A pull job that ended without recording its outcome, for example cancelled while queued.
async fn settle_stale(
    context: &DriverContext,
    link: &InvenioLink,
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
    Ok(Some(now))
}

/// Asks the repository for the lineage's latest version and stores the answer on the link.
/// A refusal, such as a withdrawn record, fails the link. Returns the stored link.
pub async fn check_now(
    context: &DriverContext,
    link: &InvenioLink,
) -> Result<Option<InvenioLink>, LinkError> {
    let change = match latest_version(context, link).await {
        Ok(check) => LinkChange::Checked(check),
        Err(TransferError::Refused(reason)) => LinkChange::Fail(reason),
        Err(TransferError::Permanent(message)) => LinkChange::Fail(LinkFailure::Other(message)),
        Err(_) => LinkChange::Checked(PullCheck::Unavailable),
    };
    change_link(context, link, change).await
}

/// The held record's own version flag comes from the database; only a newer version needs the
/// version listing.
async fn latest_version(
    context: &DriverContext,
    link: &InvenioLink,
) -> Result<PullCheck, TransferError> {
    let held = link
        .remote
        .record_id
        .as_deref()
        .ok_or_else(|| TransferError::Permanent("the link holds no record".into()))?;
    aruna_core::invenio::validate_id(held)?;
    let local = current_event(context, link.document_id).await.ok();
    let auth = creator_auth(link);
    let client = connect(
        context,
        &auth,
        link.group_id,
        link.connector_id,
        Permission::READ,
        link.limits.metadata_bytes,
        None,
    )
    .await?;
    let gone = |error| match error {
        InvenioError::Status(404 | 410) => TransferError::Refused(LinkFailure::SourceUnavailable),
        error => error.into(),
    };
    let record = client
        .json(Method::GET, client.url(&["records", held])?, None)
        .await
        .map_err(gone)?;
    let latest = if record["versions"]["is_latest"] == true {
        record
    } else {
        let mut url = client.url(&["records", held, "versions"])?;
        url.query_pairs_mut()
            .append_pair("size", "1")
            .append_pair("sort", "version");
        let page = client.json(Method::GET, url, None).await.map_err(gone)?;
        page["hits"]["hits"][0].clone()
    };
    let latest_id = aruna_core::invenio::record_id(&latest)?.to_string();
    let revision = latest["revision_id"]
        .as_u64()
        .ok_or_else(|| TransferError::Permanent("missing record revision".into()))?;
    Ok(PullCheck::Found {
        latest_id,
        revision,
        local,
    })
}

/// Submits an import of the lineage's latest version into the linked dataset, as the creator.
pub async fn start_pull(context: &DriverContext, link: &InvenioLink) -> Result<JobId, LinkError> {
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
        source: ImportRoCrateSource::Invenio {
            group_id: link.group_id,
            connector_id: link.connector_id,
            record_id: record_id.clone(),
            options: pull.options.clone(),
            pull: Some(InvenioPull::Update {
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
