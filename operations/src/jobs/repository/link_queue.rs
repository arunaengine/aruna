//! Queues link push checks after dataset changes and starts one push job per changed revision.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{LINK_QUEUE_KEYSPACE, REPOSITORY_LINK_KEYSPACE};
use aruna_core::repository::{
    LinkFailure, LinkQueueEntry, LinkReview, LinkStatus, PushOutcome, REVIEW_POLL_MS,
    RepositoryLink, link_prefix,
};
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobId, JobRecord, JobState};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, TxnId, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::TransferError;
use super::links::{
    ChangeLinkOperation, LinkChange, LinkError, change_link, ensure_lineage, id_key, read_link,
    schedule_drain,
};
use crate::driver::{DriverContext, drive};
use crate::jobs::service::submit_export_job;
use crate::jobs::store::read_job_record;
use crate::jobs::submit::SubmitJobError;
use crate::metadata::api::load_realm_config;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::get_document::load_document_record;
use crate::metadata::raw_revision::load_raw_revision;
use crate::metadata::repository::{parse_registry_read, read_document_registry};
use crate::placement::holds_placement;
use crate::tasks::queue_backoff::{due_after, min_due_at};

const QUEUE_PAGE: usize = 256;
/// How often a check waits for a running push before it looks again.
const ACTIVE_RETRY_MS: u64 = 30_000;
const ERROR_RETRY_MS: u64 = 60_000;

/// Push-check rows for the push links of changed documents that push or retry after unmet
/// requirements; each change moves the due time.
/// A deleted document queues all its links, so the check removes them.
pub(crate) async fn queue_rows(
    storage: &StorageHandle,
    documents: impl IntoIterator<Item = Ulid>,
) -> Result<Vec<(String, Key, Value)>, LinkError> {
    let now_ms = unix_timestamp_millis();
    let mut candidates = Vec::new();
    for document_id in documents {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REPOSITORY_LINK_KEYSPACE.to_string(),
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
        let gone = !values.is_empty() && document_gone(storage, document_id).await?;
        for (_, value) in values {
            let link = RepositoryLink::from_bytes(&value)?;
            let pushes = link.status == LinkStatus::Enabled || link.retries_on_change();
            if gone || (pushes && link.pull().is_none()) {
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
        let existing = existing.and_then(|bytes| postcard::from_bytes(&bytes).ok());
        let entry = LinkQueueEntry::debounce(document_id, existing.as_ref(), now_ms);
        rows.push(queue_row(link_id, &entry)?);
    }
    Ok(rows)
}

fn queue_row(
    link_id: Ulid,
    entry: &LinkQueueEntry,
) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        LINK_QUEUE_KEYSPACE.to_string(),
        id_key(link_id),
        ByteView::from(postcard::to_allocvec(entry)?),
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
            let retry_at = match check_link(context, link_id, &entry, &value, now).await {
                Ok(retry_at) => retry_at,
                Err(error) => {
                    warn!(%link_id, %error, "Repository link push check failed");
                    Some(now.saturating_add(ERROR_RETRY_MS))
                }
            };
            if let Some(retry_at) = retry_at {
                settle_entry(storage, link_id, &value, Some(retry_at)).await?;
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
    queued: &Value,
    now: u64,
) -> Result<Option<u64>, LinkError> {
    let storage = &context.storage_handle;
    let local = context.net_handle.as_ref().map(|net| net.node_id());
    let drop_entry = async || drop_unchanged(storage, link_id, queued).await;
    let link = match read_link(storage, entry.document_id, link_id).await? {
        Some(link) if local.is_none_or(|n| n == link.owner_node) => link,
        _ => return drop_entry().await,
    };
    // A deleted dataset takes its links and their sealed tokens along; remote records stay.
    if document_gone(storage, entry.document_id).await? {
        change_link(context, &link, LinkChange::Delete).await?;
        return Ok(None);
    }
    if link.status != LinkStatus::Enabled && !link.retries_on_change() {
        return drop_entry().await;
    }
    if let Err(LinkError::NotHolder) = ensure_holder(context, &link).await {
        return drop_entry().await;
    }
    if link.pull().is_some() {
        return super::pull::check_due(context, &link, now).await;
    }
    if let Some(job_id) = link.active_job {
        let record = read_job_record(storage, job_id, None)
            .await
            .map_err(LinkError::Unexpected)?;
        return match record {
            Some(record) if !record.state.is_terminal() => {
                Ok(Some(now.saturating_add(ACTIVE_RETRY_MS)))
            }
            record => settle_stale(context, &link, job_id, record.as_ref(), queued).await,
        };
    }
    // A decided review stores the repository's answer, which queues another check.
    if refresh_review(context, &link).await? != link {
        return Ok(None);
    }
    let Some((event_id, digest)) = push_revision(context, entry.document_id).await? else {
        return drop_entry().await;
    };
    let publish = match link.publish_due_ms() {
        _ if link.changed(event_id, digest) => false,
        Some(due) if due <= now => true,
        Some(due) => return Ok(Some(due)),
        None if link.remote.review == LinkReview::Pending => {
            return Ok(Some(now.saturating_add(REVIEW_POLL_MS)));
        }
        None => return drop_entry().await,
    };
    match start_push(context, &link, event_id, publish).await {
        Ok(_) => Ok(None),
        // The check stays queued, so the link shows pending until a job slot frees up.
        Err(LinkError::JobLimit(_)) => Ok(Some(now.saturating_add(ACTIVE_RETRY_MS))),
        Err(error) => Err(error),
    }
}

/// A push job ended without recording its outcome, for example cancelled while queued.
async fn settle_stale(
    context: &Arc<DriverContext>,
    link: &RepositoryLink,
    job_id: JobId,
    record: Option<&JobRecord>,
    queued: &Value,
) -> Result<Option<u64>, LinkError> {
    // A push that finished remotely but failed to record it still counts as pushed.
    let checkpoint = crate::jobs::export::stored_checkpoint(&context.storage_handle, job_id)
        .await
        .map_err(LinkError::Unexpected)?;
    let pushed = checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.pushed_outcome());
    let outcome = match (pushed, record) {
        (Some(pushed), _) => pushed,
        (None, Some(record)) if record.state == JobState::Failed => {
            let message = record
                .last_error
                .as_ref()
                .map_or("push job failed", |error| error.message.as_str());
            PushOutcome::Failed(match &checkpoint {
                Some(checkpoint) => checkpoint.push_failure(message),
                None => LinkFailure::Other(message.to_string()),
            })
        }
        (None, Some(_)) => PushOutcome::Cancelled,
        (None, None) => PushOutcome::Failed(LinkFailure::Other("push job missing".into())),
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
        return drop_unchanged(&context.storage_handle, link.link_id, queued).await;
    }
    Ok(Some(unix_timestamp_millis()))
}

/// Asks the repository about a pending review and stores a decided one; returns the link as
/// stored afterwards. A refused request fails the link.
pub async fn refresh_review(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<RepositoryLink, LinkError> {
    if link.remote.review != LinkReview::Pending {
        return Ok(link.clone());
    }
    let change = match super::review_state(link.kind, context, link).await {
        Ok(Some(state)) => LinkChange::Accept(Box::new(state)),
        Ok(None) => return Ok(link.clone()),
        Err(TransferError::Refused(reason)) => LinkChange::Fail(reason),
        Err(error) => return Err(LinkError::Unexpected(error.to_string())),
    };
    change_link(context, link, change)
        .await?
        .ok_or(LinkError::NotFound)
}

/// Submits the push job as the link's creator and records it as the running push.
pub async fn start_push(
    context: &Arc<DriverContext>,
    link: &RepositoryLink,
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
    // A failed link runs again only if no enabled link follows its lineage the other way.
    if matches!(link.status, LinkStatus::Failed { .. }) {
        Box::pin(ensure_lineage(&context.storage_handle, link)).await?;
    }
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
pub async fn owner_holds(context: &DriverContext, link: &RepositoryLink) -> bool {
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
pub(super) async fn ensure_holder(
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<(), LinkError> {
    if owner_holds(context, link).await {
        return Ok(());
    }
    let change = LinkChange::Fail(LinkFailure::OwnerNotHolder);
    drive(
        ChangeLinkOperation::new(link.document_id, link.link_id, change, SystemTime::now()),
        context,
    )
    .await?;
    Err(LinkError::NotHolder)
}

/// The current revision event of a held document, for push keys and change checks.
pub async fn current_event(context: &DriverContext, document_id: Ulid) -> Result<Ulid, LinkError> {
    push_revision(context, document_id)
        .await?
        .map(|(event_id, _)| event_id)
        .ok_or(LinkError::NoRevision)
}

/// The revision a push exports: the raw revision, else a scaffold's rendered graph at its last
/// event, the same crate the export job and the dataset view use.
async fn push_revision(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Option<(Ulid, Option<[u8; 32]>)>, LinkError> {
    let raw = load_raw_revision(context, document_id, None)
        .await
        .map_err(|error| LinkError::Unexpected(error.to_string()))?;
    if let Some(revision) = raw {
        return Ok(Some((revision.winning_event_id, revision.dataset_digest)));
    }
    let record = load_document_record(context, document_id)
        .await
        .map_err(|error| LinkError::Unexpected(format!("{error:?}")))?;
    Ok(record.map(|record| (record.last_event_id, None)))
}

/// Whether the dataset was deleted, which removes its registry record.
pub(super) async fn document_gone(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<bool, LinkError> {
    let event = storage
        .send_effect(read_document_registry(document_id, None))
        .await;
    parse_registry_read(event)
        .map(|record| record.is_none())
        .map_err(|error| LinkError::Unexpected(format!("{error:?}")))
}

/// Deletes the queued check unless a change rewrote it since `queued` was read.
async fn drop_unchanged(
    storage: &StorageHandle,
    link_id: Ulid,
    queued: &Value,
) -> Result<Option<u64>, LinkError> {
    settle_entry(storage, link_id, queued, None)
        .await
        .map(|_| None)
}

/// Deletes the queued check, or with `retry_at` moves it there. A change written since `queued`
/// was read stays, due no later than `retry_at`; a removed check stays removed.
async fn settle_entry(
    storage: &StorageHandle,
    link_id: Ulid,
    queued: &Value,
    retry_at: Option<u64>,
) -> Result<(), LinkError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(LinkError::Unexpected(format!("{other:?}"))),
    };
    let result = settle_in(storage, link_id, queued, retry_at, txn_id).await;
    if result.is_err() {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }
    match result {
        // A conflicting write queued a newer change; its check runs later.
        Err(LinkError::Storage(StorageError::TransactionConflict)) => Ok(()),
        result => result,
    }
}

async fn settle_in(
    storage: &StorageHandle,
    link_id: Ulid,
    queued: &Value,
    retry_at: Option<u64>,
    txn_id: TxnId,
) -> Result<(), LinkError> {
    let current = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: LINK_QUEUE_KEYSPACE.to_string(),
            key: id_key(link_id),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(LinkError::Unexpected(format!("{other:?}"))),
    };
    let unchanged = current.as_ref() == Some(queued);
    let entry = |bytes: &Value| postcard::from_bytes::<LinkQueueEntry>(bytes).ok();
    let effect = match (current.as_ref(), retry_at) {
        (Some(_), None) if unchanged => StorageEffect::Delete {
            key_space: LINK_QUEUE_KEYSPACE.to_string(),
            key: id_key(link_id),
            txn_id: Some(txn_id),
        },
        (Some(current), Some(retry_at)) => {
            let base = if unchanged { None } else { entry(current) };
            let Some(mut next) = base.clone().or_else(|| entry(queued)) else {
                return Err(LinkError::Unexpected("unreadable link queue entry".into()));
            };
            next.due_at_ms = base.map_or(retry_at, |base| base.due_at_ms.min(retry_at));
            let (key_space, key, value) = queue_row(link_id, &next)?;
            StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: Some(txn_id),
            }
        }
        _ => StorageEffect::AbortTransaction { txn_id },
    };
    let aborting = matches!(effect, StorageEffect::AbortTransaction { .. });
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::TransactionAborted { .. }) if aborting => return Ok(()),
        Event::Storage(StorageEvent::DeleteResult { .. } | StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(LinkError::Unexpected(format!("{other:?}"))),
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(LinkError::Unexpected(format!("{other:?}"))),
    }
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

/// Queues checks for the links of the deleted document behind `graph_iri`; each check
/// then removes its link and token.
pub async fn queue_deleted(context: &DriverContext, graph_iri: &str) -> Result<(), LinkError> {
    let document_id = graph_iri
        .rsplit('/')
        .next()
        .and_then(|id| Ulid::from_string(id).ok())
        .filter(|id| MetadataRegistryRecord::graph_iri_for(*id) == graph_iri);
    let Some(document_id) = document_id else {
        return Ok(());
    };
    let storage = &context.storage_handle;
    let rows = queue_rows(storage, [document_id]).await?;
    if rows.is_empty() {
        return Ok(());
    }
    for row in rows {
        write_row(storage, row).await?;
    }
    if let Some(task_handle) = &context.task_handle {
        restore_link_timer(storage, task_handle).await;
    }
    Ok(())
}

/// Arms the drain for the earliest queued check; the queue itself is the durable state.
pub async fn restore_link_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let now = unix_timestamp_millis();
    let mut next = None;
    let mut start = None;
    loop {
        let Ok((values, next_start)) = scan_queue(storage, start).await else {
            warn!("Failed to scan the repository link queue");
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
        warn!(%message, "Failed to arm the repository link queue");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_storage::FjallStorage;

    async fn stored(storage: &StorageHandle, link_id: Ulid) -> Option<LinkQueueEntry> {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: LINK_QUEUE_KEYSPACE.to_string(),
                key: id_key(link_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                value.map(|bytes| postcard::from_bytes(&bytes).unwrap())
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn retry_keeps_newer() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let (link_id, document_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(1, 2));
        let entry = |due_at_ms, first_at_ms| LinkQueueEntry {
            document_id,
            due_at_ms,
            first_at_ms,
        };
        let queued = queue_row(link_id, &entry(100, 90)).unwrap();
        write_row(&storage, queued.clone()).await.unwrap();

        settle_entry(&storage, link_id, &queued.2, Some(500))
            .await
            .unwrap();
        assert_eq!(stored(&storage, link_id).await, Some(entry(500, 90)));

        // A change queued while the check ran keeps its earlier due time.
        let newer = queue_row(link_id, &entry(200, 150)).unwrap();
        write_row(&storage, newer).await.unwrap();
        settle_entry(&storage, link_id, &queued.2, Some(500))
            .await
            .unwrap();
        assert_eq!(stored(&storage, link_id).await, Some(entry(200, 150)));
        settle_entry(&storage, link_id, &queued.2, None)
            .await
            .unwrap();
        assert_eq!(stored(&storage, link_id).await, Some(entry(200, 150)));

        // A check removed meanwhile, for example by a started push, stays removed.
        delete_entry(&storage, id_key(link_id)).await.unwrap();
        settle_entry(&storage, link_id, &queued.2, Some(500))
            .await
            .unwrap();
        assert_eq!(stored(&storage, link_id).await, None);
    }
}
