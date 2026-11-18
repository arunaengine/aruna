//! Turns live copy obligations into replication jobs and retires the ones that are done.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    NODE_STATE_KEYSPACE, RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE,
    REPLICATION_JOB_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionSnapshot};
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{
    ReferenceHandling, SyncMode, SyncRelationship, sync_relationship_key, sync_relationship_prefix,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use tracing::warn;

use super::drain::abort_cursor;
use super::members::{BoundRecordOperation, member_jobs};
use super::page::PAGE_BYTES;
use super::queue::BlobQueueError;
use crate::driver::{DriverContext, drive};
use aruna_core::structs::storage::replication_queue::SyncOrigin;
use aruna_core::structs::storage::replication_queue::{
    BlobJobRecord, LiveObligationRecord, LiveReplicationContinuation, RelationshipJobTarget,
    blob_job_entry, blob_job_key, blob_job_preferred, continuation_newer, live_obligation_entry,
    relationship_job, same_endpoint, validate_sync_key,
};

pub(super) const REPLICATION_PAGE_SIZE: usize = 512;

/// Obligations one pass reads; those without sync work are retired together.
pub(super) const OBLIGATION_BATCH_SIZE: usize = super::page::PAGE_RECORDS;

/// Obligations one pass turns into replication jobs.
const OBLIGATION_WORK_LIMIT: usize = 64;

pub(super) const REPLICATION_JOB_LIMIT: usize = 64;

pub(super) const REPLICATION_RELATIONSHIP_LIMIT: usize = 1024;

/// The obligation the next obligation pass continues after, so later ones get their turn.
const OBLIGATION_CURSOR_KEY: &[u8] = b"live_obligation_cursor";

pub(super) struct RelationshipPage {
    pub(super) values: Vec<(Vec<u8>, SyncRelationship)>,
    pub(super) next: Option<Vec<u8>>,
}

pub(super) struct LiveRepairWrite {
    pub(super) queued: usize,
    continuation: Option<LiveReplicationContinuation>,
    progressed: bool,
}

#[derive(Default)]
pub(super) struct LiveRepairResult {
    pub(super) processed: usize,
    pub(super) queued: usize,
    pub(super) has_more: bool,
    /// When the oldest obligation read was written.
    pub(super) oldest_ms: Option<u64>,
}

pub(super) async fn live_obligations_exist(
    storage: &StorageHandle,
) -> Result<bool, BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn process_live_obligations(
    context: &DriverContext,
) -> Result<LiveRepairResult, BlobQueueError> {
    let after = read_obligation_cursor(&context.storage_handle).await?;
    let (obligations, next) = read_live_obligations(&context.storage_handle, after.clone()).await?;
    // A pass that started past the first obligation wraps to the start next.
    let has_more = next.is_some() || after.is_some();
    let mut starts = HashMap::<String, Option<Vec<u8>>>::new();
    for (_, obligation) in &obligations {
        if obligation
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count >= 4)
            || obligation
                .continuation
                .as_ref()
                .is_some_and(|cursor| cursor.relationships_complete)
        {
            continue;
        }
        let start = obligation
            .continuation
            .as_ref()
            .and_then(|cursor| cursor.relationship_after.clone());
        starts
            .entry(obligation.bucket.clone())
            .and_modify(|current| {
                let replace = match (current.as_ref(), start.as_ref()) {
                    (Some(current), Some(start)) => start < current,
                    (Some(_), None) => true,
                    _ => false,
                };
                if replace {
                    *current = start.clone();
                }
            })
            .or_insert(start);
    }
    let mut records = HashMap::<String, Option<(DistributionBinding, DistributionSnapshot)>>::new();
    for (_, obligation) in &obligations {
        if !records.contains_key(&obligation.bucket) {
            let record = drive(BoundRecordOperation::new(&obligation.bucket), context)
                .await
                .map_err(|error| error.to_string())
                .map_err(BlobQueueError::UnexpectedEvent)?;
            records.insert(obligation.bucket.clone(), record);
        }
    }
    let mut relationship_cache = HashMap::<String, RelationshipPage>::new();
    let mut relationship_work = 0usize;
    for (bucket, start) in starts {
        let remaining = REPLICATION_RELATIONSHIP_LIMIT.saturating_sub(relationship_work);
        if remaining == 0 {
            break;
        }
        let page =
            read_relationships_limit(&context.storage_handle, &bucket, start, remaining).await?;
        relationship_work = relationship_work.saturating_add(page.values.len());
        relationship_cache.insert(bucket, page);
    }
    let mut result = LiveRepairResult {
        has_more,
        oldest_ms: (obligations.iter())
            .map(|(_, obligation)| obligation.version_id.timestamp_ms())
            .min(),
        ..Default::default()
    };
    let mut retired = Vec::new();
    let mut member_work = Vec::new();
    let mut worked = 0usize;

    for (obligation_key, obligation) in obligations {
        let cursor = obligation.continuation.clone().unwrap_or_default();
        // A hop-exhausted obligation is never scanned for relationships, so it
        // must reach write_live_jobs to be retired instead of blocking the scan.
        let hop_exhausted = obligation
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count >= 4);
        let relationships = if cursor.relationships_complete {
            None
        } else {
            relationship_cache.get(&obligation.bucket)
        };
        if !hop_exhausted && !cursor.relationships_complete && relationships.is_none() {
            result.has_more = true;
            continue;
        }
        let members = match records.get(&obligation.bucket) {
            Some(Some((binding, snapshot))) if !cursor.members_done => {
                member_jobs(binding, snapshot, &obligation, unix_timestamp_millis())
            }
            _ => Vec::new(),
        };
        // A version of a bucket nothing replicates to owes no job: such obligations, one per
        // imported file, are retired in one transaction instead of one each.
        let idle = relationships.is_none_or(|page| page.values.is_empty() && page.next.is_none());
        // Without sync work an obligation owes only its member jobs, queued in one batch.
        if idle && !hop_exhausted {
            member_work.extend(members);
            retired.push((obligation_key, obligation.continuation));
            continue;
        }
        if worked == OBLIGATION_WORK_LIMIT {
            result.has_more = true;
            continue;
        }
        worked += 1;
        let write =
            write_live_jobs(&context.storage_handle, &obligation, relationships, members).await?;
        if !write.progressed {
            result.has_more = true;
            continue;
        }
        if let Some(continuation) = write.continuation {
            let mut next = obligation.clone();
            next.continuation = Some(continuation);
            write_live_obligation(&context.storage_handle, &next).await?;
            result.has_more = true;
        } else {
            delete_live_obligation(
                &context.storage_handle,
                obligation_key,
                obligation.continuation.as_ref(),
            )
            .await?;
        }
        result.processed = result.processed.saturating_add(1);
        result.queued = result.queued.saturating_add(write.queued);
    }
    result.processed = result.processed.saturating_add(retired.len());
    result.queued = result.queued.saturating_add(member_work.len());
    let storage = &context.storage_handle;
    retire_obligations(storage, retired, member_work, (after, next)).await?;

    Ok(result)
}

/// Queues `jobs` and deletes the obligations, a continued one only while it still holds the
/// continuation read, with the obligation cursor moved from `cursor.0` to `cursor.1`, in one
/// transaction.
pub(super) async fn retire_obligations(
    storage: &StorageHandle,
    obligations: Vec<(Vec<u8>, Option<LiveReplicationContinuation>)>,
    jobs: Vec<BlobJobRecord>,
    cursor: (Option<Vec<u8>>, Option<Vec<u8>>),
) -> Result<(), BlobQueueError> {
    if obligations.is_empty() && jobs.is_empty() {
        if cursor.0 != cursor.1 {
            write_obligation_cursor(storage, cursor.1).await?;
        }
        return Ok(());
    }
    let unexpected = |other: Event| BlobQueueError::UnexpectedEvent(other.label().to_string());
    // One drain at a time creates or parks member jobs, so this read needs no transaction.
    let existing = read_existing_jobs(storage, &jobs).await?;
    // Only a drain sets continuations; any other rewrite owes just the jobs queued here.
    let (fresh, obligations): (Vec<_>, Vec<_>) =
        (obligations.into_iter()).partition(|(_, expected)| expected.is_none());
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(unexpected(other)),
    };
    let reads: Vec<_> = (obligations.iter())
        .map(|(key, _)| {
            let key = ByteView::from(key.clone());
            (REPLICATION_OBLIGATION_KEYSPACE.to_string(), key)
        })
        .collect();
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    };
    let values = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(match other {
                Event::Storage(StorageEvent::Error { error }) => error.into(),
                other => unexpected(other),
            });
        }
    };
    let mut writes = Vec::with_capacity(jobs.len() + 1);
    for (job, value) in jobs.iter().zip(existing) {
        let kept = value
            .as_deref()
            .and_then(|value| BlobJobRecord::from_bytes(value).ok())
            .is_some_and(|existing| blob_job_preferred(&existing, job));
        if !kept {
            writes.push(blob_job_entry(job)?);
        }
    }
    writes.push(obligation_cursor_entry(cursor.1)?);
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    match storage.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(match other {
                Event::Storage(StorageEvent::Error { error }) => error.into(),
                other => unexpected(other),
            });
        }
    }
    let deletes = (obligations.into_iter().zip(values))
        .filter(|((_, expected), (_, value))| {
            value
                .as_deref()
                .is_some_and(|value| match LiveObligationRecord::from_bytes(value) {
                    Ok(record) => record.continuation == *expected,
                    Err(_) => expected.is_none(),
                })
        })
        .map(|((key, _), _)| key)
        .chain(fresh.into_iter().map(|(key, _)| key))
        .map(|key| (REPLICATION_OBLIGATION_KEYSPACE.to_string(), key.into()))
        .collect();
    let delete = StorageEffect::BatchDelete {
        deletes,
        txn_id: Some(txn_id),
    };
    match storage.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(match other {
                Event::Storage(StorageEvent::Error { error }) => error.into(),
                other => unexpected(other),
            });
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(unexpected(other)),
    }
}

/// The stored row of each job, from one range read while the range holds no other rows, as
/// lookups key by key cost far more than a range in these churned keyspaces.
async fn read_existing_jobs(
    storage: &StorageHandle,
    jobs: &[BlobJobRecord],
) -> Result<Vec<Option<ByteView>>, BlobQueueError> {
    let keys = jobs
        .iter()
        .map(blob_job_key)
        .collect::<Result<Vec<_>, _>>()?;
    let (Some(first), Some(last)) = (keys.iter().min(), keys.iter().max()) else {
        return Ok(Vec::new());
    };
    let range = StorageEffect::IterRange {
        key_space: REPLICATION_JOB_KEYSPACE.to_string(),
        start: IterStart::At(first.clone()),
        end: IterEnd::Through(last.clone()),
        limit: keys.len(),
        max_bytes: PAGE_BYTES,
        txn_id: None,
    };
    match storage.send_storage_effect(range).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }) => {
            let rows: HashMap<_, _> = values.into_iter().collect();
            return Ok(keys.iter().map(|key| rows.get(key).cloned()).collect());
        }
        // A range holding more rows than `jobs` is read key by key.
        Event::Storage(StorageEvent::IterResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
    let reads = (keys.into_iter())
        .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), key))
        .collect();
    match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

/// Obligations after `after`, and the key the next pass continues after; `None` once the
/// read reached the end.
async fn read_live_obligations(
    storage: &StorageHandle,
    after: Option<Vec<u8>>,
) -> Result<(Vec<(Vec<u8>, LiveObligationRecord)>, Option<Vec<u8>>), BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: None,
            start: after.map(|key| IterStart::After(key.into())),
            limit: OBLIGATION_BATCH_SIZE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let mut obligations = Vec::with_capacity(values.len());
            for (key, value) in values {
                match LiveObligationRecord::from_bytes(&value) {
                    Ok(record) => obligations.push((key.to_vec(), record)),
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed live replication obligation");
                        delete_live_obligation(storage, key, None).await?;
                    }
                }
            }
            Ok((obligations, next_start_after.map(|key| key.to_vec())))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn write_live_obligation(
    storage: &StorageHandle,
    record: &LiveObligationRecord,
) -> Result<(), BlobQueueError> {
    let (key_space, key, value) = live_obligation_entry(record)?;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let (current, valid) = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.clone(),
            key: key.clone(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => (None, true),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => match LiveObligationRecord::from_bytes(&value) {
            Ok(record) => (Some(record), true),
            Err(_) => (None, false),
        },
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let should_write = !valid
        || match current.as_ref() {
            None => true,
            Some(current) => match (record.continuation.as_ref(), current.continuation.as_ref()) {
                (Some(candidate), Some(current)) => continuation_newer(candidate, current),
                (Some(_), None) => true,
                (None, None) | (None, Some(_)) => false,
            },
        };
    if !should_write {
        abort_cursor(storage, txn_id).await;
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn read_relationships_limit(
    storage: &StorageHandle,
    bucket: &str,
    start: Option<Vec<u8>>,
    limit: usize,
) -> Result<RelationshipPage, BlobQueueError> {
    let mut start_after = start.clone();
    let mut relationships = Vec::with_capacity(limit.min(REPLICATION_PAGE_SIZE));
    if limit == 0 {
        return Ok(RelationshipPage {
            values: relationships,
            next: None,
        });
    }
    loop {
        let page_limit = (limit - relationships.len()).min(REPLICATION_PAGE_SIZE);
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                prefix: Some(sync_relationship_prefix(bucket).into()),
                start: start_after.take().map(|key| IterStart::After(key.into())),
                limit: page_limit,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
            }
        };
        for (key, value) in values {
            let relationship = SyncRelationship::from_bytes(&value)?;
            validate_sync_key(bucket, &key, &relationship)?;
            relationships.push((key.to_vec(), relationship));
        }
        if relationships.len() == limit {
            return Ok(RelationshipPage {
                values: relationships,
                next: next_start_after.map(|key| key.to_vec()),
            });
        }
        match next_start_after {
            Some(next) => start_after = Some(next.to_vec()),
            None => {
                return Ok(RelationshipPage {
                    values: relationships,
                    next: None,
                });
            }
        }
    }
}

pub(super) async fn write_live_jobs(
    storage: &StorageHandle,
    obligation: &LiveObligationRecord,
    relationships: Option<&RelationshipPage>,
    members: Vec<BlobJobRecord>,
) -> Result<LiveRepairWrite, BlobQueueError> {
    if obligation
        .origin
        .as_ref()
        .is_some_and(|origin| origin.hop_count >= 4)
    {
        warn!(
            bucket = %obligation.bucket,
            key = %obligation.key,
            version_id = %obligation.version_id,
            "Retiring live replication obligation at the hop limit"
        );
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation: None,
            progressed: true,
        });
    }
    let inbound_source = match obligation.origin.as_ref() {
        Some(origin) => read_inbound_source(storage, &obligation.bucket, origin).await?,
        None => None,
    };
    let mut upstream_sources = obligation.upstream_sources.clone();
    if let Some(source) = inbound_source
        && !upstream_sources
            .iter()
            .any(|existing| same_endpoint(existing, &source))
    {
        upstream_sources.push(source);
    }
    let mut cursor = obligation.continuation.clone().unwrap_or_default();
    // Publications to set members are bounded by the member count and queued once.
    let mut queued = 0;
    if !cursor.members_done && !members.is_empty() {
        queued = persist_live_jobs(storage, members, None).await?.queued;
    }
    cursor.members_done = true;
    let mut relationship_jobs = Vec::with_capacity(REPLICATION_JOB_LIMIT);
    if !cursor.relationships_complete {
        let Some(page) = relationships else {
            return Ok(LiveRepairWrite {
                queued: 0,
                continuation: None,
                progressed: false,
            });
        };
        let mut found = false;
        let mut complete = page.next.is_none();
        for (index, (key, relationship)) in page.values.iter().enumerate() {
            if cursor
                .relationship_after
                .as_ref()
                .is_some_and(|after| key <= after)
            {
                continue;
            }
            found = true;
            cursor.relationship_after = Some(key.clone());
            if obligation.reference_advance.is_some()
                && relationship.mode != SyncMode::Reference
                && relationship.reference_handling != ReferenceHandling::Preserve
            {
                continue;
            }
            if let Some(job) = relationship_job(
                obligation.local_node_id,
                RelationshipJobTarget {
                    bucket: &obligation.bucket,
                    key: &obligation.key,
                    version_id: obligation.version_id,
                    delete_marker: obligation.delete_marker,
                },
                relationship.clone(),
                obligation.origin.as_ref(),
                &upstream_sources,
            )
            .map(|mut job| {
                if let Some(advance) = obligation.reference_advance {
                    job.input.auth_context = obligation.auth_context.clone();
                    job.writer_auth_context = None;
                    job.with_reference_advance(advance)
                } else {
                    job.with_writer_auth(obligation.auth_context.clone())
                }
            }) {
                if relationship_jobs.len() == REPLICATION_JOB_LIMIT {
                    complete = false;
                    break;
                }
                relationship_jobs.push(job);
                if relationship_jobs.len() == REPLICATION_JOB_LIMIT {
                    complete = index + 1 == page.values.len() && page.next.is_none();
                    break;
                }
            }
        }
        if !found {
            if page.next.is_none() {
                cursor.relationship_after = None;
                cursor.relationships_complete = true;
            } else {
                return Ok(LiveRepairWrite {
                    queued: 0,
                    continuation: None,
                    progressed: false,
                });
            }
        } else if complete {
            cursor.relationship_after = None;
            cursor.relationships_complete = true;
        } else {
            cursor.relationships_complete = false;
        }
    }
    let continuation = (!cursor.relationships_complete).then_some(cursor);
    let mut write = persist_live_jobs(storage, relationship_jobs, continuation).await?;
    write.queued = write.queued.saturating_add(queued);
    Ok(write)
}

async fn persist_live_jobs(
    storage: &StorageHandle,
    jobs: Vec<BlobJobRecord>,
    continuation: Option<LiveReplicationContinuation>,
) -> Result<LiveRepairWrite, BlobQueueError> {
    if jobs.is_empty() {
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation,
            progressed: true,
        });
    }

    let reads = jobs
        .iter()
        .map(|job| Ok((REPLICATION_JOB_KEYSPACE.to_string(), blob_job_key(job)?)))
        .collect::<Result<Vec<_>, ConversionError>>()?;
    let values = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    if values.len() != jobs.len() {
        return Err(BlobQueueError::UnexpectedEvent(
            "blob replication existing job read count mismatch".to_string(),
        ));
    }

    let mut writes = Vec::with_capacity(jobs.len());
    for (job, (_, value)) in jobs.iter().zip(values) {
        match value {
            Some(value) => match BlobJobRecord::from_bytes(&value) {
                Ok(existing) if blob_job_preferred(&existing, job) => {}
                Ok(_) | Err(_) => writes.push(blob_job_entry(job)?),
            },
            None => writes.push(blob_job_entry(job)?),
        }
    }
    if writes.is_empty() {
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation,
            progressed: true,
        });
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { entries }) => Ok(LiveRepairWrite {
            queued: entries.len(),
            continuation,
            progressed: true,
        }),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

async fn read_inbound_source(
    storage: &StorageHandle,
    bucket: &str,
    origin: &SyncOrigin,
) -> Result<Option<ArunaArn>, BlobQueueError> {
    let key = sync_relationship_key(bucket, origin.relationship_id);
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: RELATIONSHIP_IN_KEYSPACE.to_string(),
            key: ByteView::from(key.clone()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let relationship = SyncRelationship::from_bytes(&value)?;
            if relationship.id != origin.relationship_id
                || relationship.target.bucket() != Some(bucket)
                || sync_relationship_key(bucket, relationship.id) != key
            {
                return Err(ConversionError::FromStrError(
                    "incoming sync relationship key does not match payload".to_string(),
                )
                .into());
            }
            Ok(Some(relationship.source))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn delete_live_obligation(
    storage: &StorageHandle,
    key: Vec<u8>,
    expected: Option<&LiveReplicationContinuation>,
) -> Result<(), BlobQueueError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let current = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key: ByteView::from(key.clone()),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let should_delete = match current {
        None => false,
        Some(value) => match LiveObligationRecord::from_bytes(&value) {
            Ok(record) => record.continuation.as_ref() == expected,
            Err(_) => expected.is_none(),
        },
    };
    if !should_delete {
        abort_cursor(storage, txn_id).await;
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

async fn read_obligation_cursor(
    storage: &StorageHandle,
) -> Result<Option<Vec<u8>>, BlobQueueError> {
    let read = StorageEffect::Read {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: ByteView::from(OBLIGATION_CURSOR_KEY.to_vec()),
        txn_id: None,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .filter(|value| !value.is_empty())
            .map(|value| value.to_vec())),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

/// The cursor row; an empty value starts the next pass at the first obligation.
fn obligation_cursor_entry(
    after: Option<Vec<u8>>,
) -> Result<(String, Key, ByteView), BlobQueueError> {
    Ok((
        NODE_STATE_KEYSPACE.to_string(),
        ByteView::from(OBLIGATION_CURSOR_KEY.to_vec()),
        ByteView::from(after.unwrap_or_default()),
    ))
}

async fn write_obligation_cursor(
    storage: &StorageHandle,
    after: Option<Vec<u8>>,
) -> Result<(), BlobQueueError> {
    let (key_space, key, value) = obligation_cursor_entry(after)?;
    let write = StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id: None,
    };
    match storage.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}
