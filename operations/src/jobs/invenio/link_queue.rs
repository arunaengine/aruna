//! Queues link push checks after dataset changes and starts one push job per changed revision.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::invenio::{
    InvenioLink, LINK_DEBOUNCE_MS, LinkFailure, LinkQueueEntry, LinkStatus, PushOutcome,
    link_prefix,
};
use aruna_core::keyspaces::{INVENIO_LINK_KEYSPACE, LINK_QUEUE_KEYSPACE};
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobId, JobRecord, JobState};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::links::{
    ChangeLinkOperation, LinkChange, LinkError, change_link, id_key, read_link, schedule_drain,
};
use crate::driver::{DriverContext, drive};
use crate::jobs::service::submit_export_job;
use crate::jobs::store::read_job_record;
use crate::jobs::submit::SubmitJobError;
use crate::metadata::api::load_realm_config;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::raw_revision::load_raw_revision;
use crate::placement::holds_placement;
use crate::tasks::queue_backoff::{due_after, min_due_at};

const QUEUE_PAGE: usize = 256;
/// How often a check waits for a running push before it looks again.
const ACTIVE_RETRY_MS: u64 = 30_000;
const ERROR_RETRY_MS: u64 = 60_000;

/// Push-check rows for the enabled links of changed documents that have none queued yet.
pub(crate) async fn queue_rows(
    storage: &StorageHandle,
    documents: impl IntoIterator<Item = Ulid>,
) -> Result<Vec<(String, Key, Value)>, LinkError> {
    let due_at_ms = unix_timestamp_millis().saturating_add(LINK_DEBOUNCE_MS);
    let mut candidates = Vec::new();
    for document_id in documents {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: INVENIO_LINK_KEYSPACE.to_string(),
                prefix: Some(ByteView::from(link_prefix(document_id))),
                start: None,
                limit: QUEUE_PAGE,
                txn_id: None,
            })
            .await;
        let values = match event {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => return Err(LinkError::Unexpected(format!("{other:?}"))),
        };
        for (_, value) in values {
            let link = InvenioLink::from_bytes(&value)?;
            if link.status == LinkStatus::Enabled {
                candidates.push((link.link_id, document_id));
            }
        }
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let reads = candidates
        .iter()
        .map(|(link_id, _)| (LINK_QUEUE_KEYSPACE.to_string(), id_key(*link_id)))
        .collect();
    let queued = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(LinkError::Unexpected(format!("{other:?}"))),
    };
    let mut rows = Vec::new();
    for ((link_id, document_id), (_, existing)) in candidates.into_iter().zip(queued) {
        if existing.is_none() {
            rows.push(queue_row(link_id, document_id, due_at_ms)?);
        }
    }
    Ok(rows)
}

fn queue_row(
    link_id: Ulid,
    document_id: Ulid,
    due_at_ms: u64,
) -> Result<(String, Key, Value), ConversionError> {
    let entry = LinkQueueEntry {
        document_id,
        due_at_ms,
    };
    Ok((
        LINK_QUEUE_KEYSPACE.to_string(),
        id_key(link_id),
        ByteView::from(postcard::to_allocvec(&entry)?),
    ))
}

async fn scan_queue(
    storage: &StorageHandle,
    start: Option<Key>,
) -> Result<(Vec<(Key, Value)>, Option<Key>), LinkError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: LINK_QUEUE_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: QUEUE_PAGE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LinkError::Unexpected(format!("{other:?}"))),
    }
}

/// Runs every due check and returns the wait until the next one.
pub async fn drain_links(context: &Arc<DriverContext>) -> Result<Option<Duration>, LinkError> {
    let now = unix_timestamp_millis();
    let storage = &context.storage_handle;
    let mut next = None;
    let mut start = None;
    loop {
        let (values, next_start) = scan_queue(storage, start).await?;
        for (key, value) in values {
            let parsed = <[u8; 16]>::try_from(key.as_ref())
                .ok()
                .zip(postcard::from_bytes::<LinkQueueEntry>(&value).ok());
            let Some((link_id, entry)) = parsed else {
                warn!(key = ?key.to_vec(), "Deleting malformed link queue entry");
                delete_entry(storage, key).await?;
                continue;
            };
            if entry.due_at_ms > now {
                next = min_due_at(next, entry.due_at_ms);
                continue;
            }
            let link_id = Ulid::from_bytes(link_id);
            let retry_at = match check_link(context, link_id, &entry, now).await {
                Ok(retry_at) => retry_at,
                Err(error) => {
                    warn!(%link_id, %error, "Invenio link push check failed");
                    Some(now.saturating_add(ERROR_RETRY_MS))
                }
            };
            if let Some(retry_at) = retry_at {
                let row = queue_row(link_id, entry.document_id, retry_at)?;
                write_row(storage, row).await?;
                next = min_due_at(next, retry_at);
            }
        }
        match next_start {
            Some(key) => start = Some(key),
            None => break,
        }
    }
    Ok(next.map(|due| due_after(unix_timestamp_millis(), due)))
}

/// Starts a push when the dataset moved on; returns when to look again if it must wait.
async fn check_link(
    context: &Arc<DriverContext>,
    link_id: Ulid,
    entry: &LinkQueueEntry,
    now: u64,
) -> Result<Option<u64>, LinkError> {
    let storage = &context.storage_handle;
    let local = context.net_handle.as_ref().map(|net| net.node_id());
    let link = match read_link(storage, entry.document_id, link_id).await? {
        Some(link)
            if link.status == LinkStatus::Enabled && local.is_none_or(|n| n == link.owner_node) =>
        {
            link
        }
        _ => return drop_entry(storage, link_id).await,
    };
    if let Err(LinkError::NotHolder) = ensure_holder(context, &link).await {
        return drop_entry(storage, link_id).await;
    }
    if let Some(job_id) = link.active_job {
        let record = read_job_record(storage, job_id, None)
            .await
            .map_err(LinkError::Unexpected)?;
        return match record {
            Some(record) if !record.state.is_terminal() => {
                Ok(Some(now.saturating_add(ACTIVE_RETRY_MS)))
            }
            record => settle_stale(context, &link, job_id, record.as_ref()).await,
        };
    }
    let revision = load_raw_revision(context, entry.document_id, None)
        .await
        .map_err(|error| LinkError::Unexpected(error.to_string()))?;
    match revision {
        Some(revision) if link.changed(revision.winning_event_id, revision.dataset_digest) => {
            match start_push(context, &link, revision.winning_event_id, false).await {
                Ok(_) => Ok(None),
                // The check stays queued, so the link shows pending until a job slot frees up.
                Err(LinkError::JobLimit(_)) => Ok(Some(now.saturating_add(ACTIVE_RETRY_MS))),
                Err(error) => Err(error),
            }
        }
        _ => drop_entry(storage, link_id).await,
    }
}

/// A push job ended without recording its outcome, for example cancelled while queued.
async fn settle_stale(
    context: &Arc<DriverContext>,
    link: &InvenioLink,
    job_id: JobId,
    record: Option<&JobRecord>,
) -> Result<Option<u64>, LinkError> {
    let outcome = match record {
        Some(record) if record.state == JobState::Failed => {
            let message = record
                .last_error
                .as_ref()
                .map(|error| error.message.clone());
            PushOutcome::Failed(LinkFailure::Other(
                message.unwrap_or_else(|| "push job failed".into()),
            ))
        }
        Some(_) => PushOutcome::Cancelled,
        None => PushOutcome::Failed(LinkFailure::Other("push job missing".into())),
    };
    let cancelled = record.is_some_and(|record| record.state == JobState::Cancelled);
    let change = LinkChange::Finish {
        job_id,
        outcome: Box::new(outcome),
        requeue: false,
    };
    change_link(context, link, change).await?;
    // A cancelled push skips its change; the next change queues a new check.
    if cancelled {
        return drop_entry(&context.storage_handle, link.link_id).await;
    }
    Ok(Some(unix_timestamp_millis()))
}

/// Submits the push job as the link's creator and records it as the running push.
pub async fn start_push(
    context: &Arc<DriverContext>,
    link: &InvenioLink,
    event_id: Ulid,
    publish: bool,
) -> Result<JobId, LinkError> {
    let spec = ExportRoCrateSpec {
        destination: Some(link.destination(publish)),
        auth_context: AuthContext {
            user_id: link.created_by,
            realm_id: link.created_by.realm_id,
            path_restrictions: None,
            session: None,
        },
        document_id: link.document_id,
        limits: link.limits.clone(),
    };
    ensure_holder(context, link).await?;
    let owner = context
        .net_handle
        .as_ref()
        .map_or(link.owner_node, |net| net.node_id());
    let key = Some(link.push_key(event_id, publish));
    let submitted =
        submit_export_job(context, spec, owner, key)
            .await
            .map_err(|error| match error {
                SubmitJobError::ActiveJobLimit { limit } => LinkError::JobLimit(limit),
                error => LinkError::Submit(error.to_string()),
            })?;
    change_link(context, link, LinkChange::Begin(submitted.job_id)).await?;
    Ok(submitted.job_id)
}

/// Whether the link's owner still holds the dataset; an unknown placement counts as held.
pub async fn owner_holds(context: &DriverContext, link: &InvenioLink) -> bool {
    let realm_id = link.created_by.realm_id;
    let Some(config) = load_realm_config(context, realm_id).await else {
        return true;
    };
    resolve_metadata_id(&config, realm_id, None, link.document_id).map_or(true, |placement| {
        holds_placement(&config, &placement, link.owner_node)
    })
}

/// Fails the link with owner_not_holder once this node lost the dataset. The change stays local:
/// a node outside the holder set cannot publish to them, and holders derive the same state.
async fn ensure_holder(context: &DriverContext, link: &InvenioLink) -> Result<(), LinkError> {
    if owner_holds(context, link).await {
        return Ok(());
    }
    let change = LinkChange::Fail(LinkFailure::OwnerNotHolder);
    drive(
        ChangeLinkOperation::new(link.document_id, link.link_id, change),
        context,
    )
    .await?;
    Err(LinkError::NotHolder)
}

/// The current revision event of a held document, for push keys and change checks.
pub async fn current_event(context: &DriverContext, document_id: Ulid) -> Result<Ulid, LinkError> {
    load_raw_revision(context, document_id, None)
        .await
        .map_err(|error| LinkError::Unexpected(error.to_string()))?
        .map(|revision| revision.winning_event_id)
        .ok_or(LinkError::NoRevision)
}

async fn drop_entry(storage: &StorageHandle, link_id: Ulid) -> Result<Option<u64>, LinkError> {
    delete_entry(storage, id_key(link_id)).await?;
    Ok(None)
}

async fn delete_entry(storage: &StorageHandle, key: Key) -> Result<(), LinkError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: LINK_QUEUE_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LinkError::Unexpected(format!("{other:?}"))),
    }
}

async fn write_row(storage: &StorageHandle, row: (String, Key, Value)) -> Result<(), LinkError> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: row.0,
            key: row.1,
            value: row.2,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LinkError::Unexpected(format!("{other:?}"))),
    }
}

/// Arms the drain for the earliest queued check; the queue itself is the durable state.
pub async fn restore_link_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let now = unix_timestamp_millis();
    let mut next = None;
    let mut start = None;
    loop {
        let Ok((values, next_start)) = scan_queue(storage, start).await else {
            warn!("Failed to scan the Invenio link queue");
            return;
        };
        for (_, value) in values {
            let due = postcard::from_bytes::<LinkQueueEntry>(&value).map_or(now, |e| e.due_at_ms);
            next = min_due_at(next, due);
        }
        match next_start {
            Some(key) => start = Some(key),
            None => break,
        }
    }
    let Some(due) = next else {
        return;
    };
    if let Event::Task(TaskEvent::Error { message, .. }) = task_handle
        .send_effect(schedule_drain(due_after(now, due)))
        .await
    {
        warn!(%message, "Failed to arm the Invenio link queue");
    }
}
