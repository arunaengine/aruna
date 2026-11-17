//! Runs a metadata document update under this node's per-document lock, after its revision
//! and Profile checks, and keeps the registry cache in step.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DOCUMENT_JOB_KEYSPACE, EVENT_LOG_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
    PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataError, MetadataEventPayload,
    MetadataEventRecord,
};
use aruna_core::storage_entries::{
    document_job_prefix, event_log_prefix, materialization_status_key,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::profile::rules::stale_status;
use crate::metadata::profile::validation::validate_submission;
use crate::metadata::projector::dot_applied;
use crate::metadata::update_document::{
    UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};

static UPDATE_LOCKS: std::sync::LazyLock<[tokio::sync::Mutex<()>; 64]> =
    std::sync::LazyLock::new(|| std::array::from_fn(|_| tokio::sync::Mutex::new(())));

/// Serializes this node's writes to one document, so its actor's dots stay in order.
pub(crate) async fn document_lock(document_id: Ulid) -> tokio::sync::MutexGuard<'static, ()> {
    UPDATE_LOCKS[usize::from(document_id.to_bytes()[15]) % 64]
        .lock()
        .await
}

/// Whether a mutation is planned against this holder's graph, so its expected revision must be
/// applied here first.
fn plans_on_graph(mutation: &UpdateDocumentMutation) -> bool {
    matches!(
        mutation,
        UpdateDocumentMutation::ReplaceRoCrate { .. }
            | UpdateDocumentMutation::UpsertDataEntity { .. }
            | UpdateDocumentMutation::UpsertContextualEntity { .. }
            | UpdateDocumentMutation::ReplaceDataEntity { .. }
            | UpdateDocumentMutation::RemoveDataEntity { .. }
    )
}

/// Refuses a revision-checked change this holder cannot plan yet: a base other than its newest
/// event is a conflict, and one its graph has not applied (a status short of it or a queued
/// materialization job) is retryable. The transactional revision check still runs later.
async fn ready_base(
    context: &DriverContext,
    document_id: Ulid,
    expected: Ulid,
) -> Result<(), UpdateDocumentError> {
    let storage = &context.storage_handle;
    let read = |effect| async move {
        match storage.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
            Event::Storage(StorageEvent::Error { error }) => Err(UpdateDocumentError::from(error)),
            _ => Err(UpdateDocumentError::MissingTransaction),
        }
    };
    let newest = read(StorageEffect::Last {
        key_space: EVENT_LOG_KEYSPACE.to_string(),
        prefix: Some(event_log_prefix(document_id)),
        txn_id: None,
    })
    .await?;
    let current = (newest.first())
        .and_then(|(key, _)| <[u8; 16]>::try_from(key.get(16..32)?).ok())
        .map_or(Ulid::nil(), Ulid::from_bytes);
    if current != expected {
        return Err(UpdateDocumentError::RevisionConflict { expected, current });
    }
    let status = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: MATERIALIZATION_STATUS_KEYSPACE.to_string(),
            key: materialization_status_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(UpdateDocumentError::MissingTransaction),
    };
    let status =
        status.and_then(|bytes| postcard::from_bytes::<MaterializationStatusRecord>(&bytes).ok());
    let applied = status.is_some_and(|status| {
        status.state == MaterializationState::Materialized && status.event_id >= expected
    });
    // A received event keeps its projection marker until the projector has written its
    // materialization job, so markers read before jobs miss no event moving between them.
    if !applied || unprojected(context, document_id).await? {
        return Err(UpdateDocumentError::GraphPending { expected });
    }
    let queued = read(StorageEffect::Iter {
        key_space: DOCUMENT_JOB_KEYSPACE.to_string(),
        prefix: Some(document_job_prefix(document_id)),
        start: None,
        limit: 1,
        txn_id: None,
    })
    .await?;
    match applied && queued.is_empty() {
        true => Ok(()),
        false => Err(UpdateDocumentError::GraphPending { expected }),
    }
}

/// Whether the document has a received event the projector has not handed on yet whose own
/// change is not in the graph: a marker of an applied batch is only a deferred lifecycle mint.
async fn unprojected(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<bool, UpdateDocumentError> {
    let storage = &context.storage_handle;
    let mut start = None;
    loop {
        let page = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: PENDING_PROJECTION_KEYSPACE.to_string(),
                prefix: Some(event_log_prefix(document_id)),
                start: start.clone(),
                limit: 100,
                txn_id: None,
            })
            .await;
        let (values, next) = match page {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            _ => return Err(UpdateDocumentError::MissingTransaction),
        };
        for (key, _) in values {
            let read = StorageEffect::Read {
                key_space: EVENT_LOG_KEYSPACE.to_string(),
                key: key.clone(),
                txn_id: None,
            };
            let logged = match storage.send_storage_effect(read).await {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
                Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
                _ => return Err(UpdateDocumentError::MissingTransaction),
            };
            let event =
                logged.and_then(|bytes| postcard::from_bytes::<MetadataEventRecord>(&bytes).ok());
            let Some(MetadataEventPayload::ApplyBatch { batch, .. }) =
                event.map(|event| event.payload)
            else {
                return Ok(true);
            };
            let applied = dot_applied(context, &batch.graph_iri, &batch).await;
            if !applied.map_err(|error| MetadataError::Backend(error.to_string()))? {
                return Ok(true);
            }
        }
        match next {
            Some(after) => start = Some(IterStart::After(after)),
            None => return Ok(false),
        }
    }
}

/// Type-erased, so futures that await an update do not repeat its whole `Send` proof.
pub fn update_metadata_document(
    mut operation: UpdateDocumentOperation,
    context: &DriverContext,
) -> std::pin::Pin<
    Box<dyn Future<Output = Result<MetadataRegistryRecord, UpdateDocumentError>> + Send + '_>,
> {
    Box::pin(async move {
        let _guard = document_lock(operation.config.document_id).await;
        if let Some(expected) = operation.config.expected_revision
            && plans_on_graph(&operation.config.mutation)
        {
            ready_base(context, operation.config.document_id, expected).await?;
        }
        operation.route_profile_status = Some(match &operation.config.mutation {
            UpdateDocumentMutation::ReplaceRoCrate { jsonld } => {
                validate_submission(
                    context,
                    operation.config.document_id,
                    operation.config.group_id,
                    jsonld,
                )
                .await?
            }
            UpdateDocumentMutation::UpsertDataEntity { .. }
            | UpdateDocumentMutation::UpsertContextualEntity { .. }
            | UpdateDocumentMutation::ReplaceDataEntity { .. }
            | UpdateDocumentMutation::RemoveDataEntity { .. }
            | UpdateDocumentMutation::ApplyBatch { .. }
            | UpdateDocumentMutation::Checkpoint => {
                stale_status(operation.config.document_id, "dataset_revision_changed")
            }
        });
        let cache_generation = context
            .metadata_handle
            .as_ref()
            .map(|metadata_handle| metadata_handle.visibility_generation());
        let updated = drive(operation, context).await?;
        if let (Some(metadata_handle), Some(cache_generation)) =
            (context.metadata_handle.as_ref(), cache_generation)
        {
            metadata_handle.upsert_cached_at(updated.clone(), cache_generation);
        }
        Ok(updated)
    })
}
