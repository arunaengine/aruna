use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncOutboxRecord,
    DocumentSyncRevision, DocumentSyncTarget,
};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_MATERIALIZATION_STATUS_KEYSPACE, METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataDocumentLifecycleRecord, MetadataError,
    MetadataGraphLifecycleRecord, MetadataMaterializationStatusRecord,
};
use aruna_core::storage_entries::{
    metadata_document_lifecycle_revision_change, metadata_document_lifecycle_write_entry,
    metadata_event_log_key, metadata_graph_lifecycle_key, metadata_materialization_status_key,
    metadata_pending_projection_delete_entry, metadata_pending_projection_key,
    metadata_pending_projection_target, metadata_registry_delete_entries,
};
use aruna_core::structs::{
    MetadataAuditRecord, MetadataRegistryRecord, PlacementRef, RealmConfigDocument, RealmId,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::schedule_outbox_drain_effect;
use crate::driver::DriverContext;
use crate::metadata::materialization_queue::{
    new_materialization_job, new_pending_materialization_status,
    schedule_metadata_materialization_drain_effect,
};
use crate::metadata::repository::{
    create_records_and_outbox_write_entries,
    create_records_outbox_and_materialization_write_entries, read_registry_by_document_effect,
};
use crate::placement::{registry_placement, resolve_shard_holders};
use crate::sync_placement::sort_node_ids;
use crate::task_persistence::persist_task_effect;

const REPLAY_PAGE_SIZE: usize = 1_024;
const PENDING_PROJECTION_PAGE_SIZE: usize = 256;
pub const METADATA_PROJECTION_RETRY_AFTER: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingMetadataProjectionDrainResult {
    pub markers_examined: usize,
    pub projected: usize,
    pub has_more: bool,
}

/// Conflict resolution is last-writer-wins on wall-clock time, so an event
/// stamped far in the future would win every conflict forever. Inbound
/// events beyond the configured skew are deferred until retry; operators must
/// run NTP.
const DEFAULT_MAX_CLOCK_SKEW_SECS: u64 = 300;

static CLOCK_SKEW_REJECTIONS: AtomicU64 = AtomicU64::new(0);

fn max_clock_skew_ms() -> u64 {
    static SKEW_MS: OnceLock<u64> = OnceLock::new();
    *SKEW_MS.get_or_init(|| {
        std::env::var("MAX_CLOCK_SKEW_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_CLOCK_SKEW_SECS)
            .saturating_mul(1000)
    })
}

pub fn clock_skew_rejection_count() -> u64 {
    CLOCK_SKEW_REJECTIONS.load(Ordering::Relaxed)
}

fn exceeds_clock_skew(event: &MetadataCreateEventRecord, now_ms: u64, max_skew_ms: u64) -> bool {
    let limit = now_ms.saturating_add(max_skew_ms);
    event.record.updated_at_ms > limit || event.occurred_at_ms > limit
}

#[derive(Debug, Error)]
pub enum MetadataProjectionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("metadata handle missing")]
    MetadataHandleMissing,
    #[error("metadata create event log record not found for {document_id}/{event_id}")]
    MetadataCreateEventMissing { document_id: MetaResourceId, event_id: Ulid },
    #[error("deferred {deferred} metadata create event(s) stamped too far in the future")]
    ClockSkewDeferred { deferred: usize },
    #[error("unexpected event while projecting metadata create event: {0}")]
    UnexpectedEvent(String),
}

fn pending_metadata_projection_drain_task_effect(after: Duration) -> TaskEffect {
    TaskEffect::ResetTimer {
        key: TaskKey::DrainMetadataProjectionQueue,
        after,
    }
}

pub async fn schedule_pending_metadata_projection_drain(
    context: &DriverContext,
    after: Duration,
) -> Result<(), MetadataProjectionError> {
    let effect = pending_metadata_projection_drain_task_effect(after);
    persist_task_effect(&context.storage_handle, &effect)
        .await
        .map_err(MetadataProjectionError::UnexpectedEvent)?;

    let Some(task_handle) = context.task_handle.as_ref() else {
        return Ok(());
    };
    match task_handle.send_effect(Effect::Task(effect)).await {
        Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(()),
        Event::Task(TaskEvent::Error { message, .. }) => {
            Err(MetadataProjectionError::UnexpectedEvent(message))
        }
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub async fn restore_pending_metadata_projection_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
) {
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty() => {}
        Event::Storage(StorageEvent::IterResult { .. }) => {
            let event = task_handle
                .send_effect(Effect::Task(pending_metadata_projection_drain_task_effect(
                    Duration::ZERO,
                )))
                .await;
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore metadata projection timer");
            }
        }
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan metadata pending projection markers");
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning metadata pending projection markers");
        }
    }
}

pub async fn replay_metadata_event_log(
    context: &DriverContext,
) -> Result<usize, MetadataProjectionError> {
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let mut start_after: Option<Key> = None;
    let mut projected = 0usize;

    loop {
        let page = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: REPLAY_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match page {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataProjectionError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        let mut events = Vec::with_capacity(values.len());
        for (_, value) in values {
            events.push(postcard::from_bytes(&value).map_err(ConversionError::from)?);
        }
        projected = projected
            .saturating_add(project_metadata_create_events(context, events, local_node_id).await?);

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(projected),
        }
    }
}

pub async fn drain_pending_metadata_projection_queue(
    context: &DriverContext,
) -> Result<PendingMetadataProjectionDrainResult, MetadataProjectionError> {
    let page = context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: PENDING_PROJECTION_PAGE_SIZE,
            txn_id: None,
        })
        .await;
    let (values, next_start_after) = match page {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => (values, next_start_after),
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(MetadataProjectionError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };

    let mut targets = Vec::with_capacity(values.len());
    for (key, _) in &values {
        let Some(target) = metadata_pending_projection_target(key.as_ref()) else {
            let key = key.to_vec();
            warn!(key = ?key, "Deleting malformed metadata pending projection marker");
            delete_pending_projection_marker_keys(context, vec![key]).await?;
            continue;
        };
        targets.push(target);
    }
    let projected_from_log =
        project_metadata_create_events_from_log_inner(context, targets, true).await?;
    Ok(PendingMetadataProjectionDrainResult {
        markers_examined: projected_from_log.existing_events,
        projected: projected_from_log.projected,
        has_more: next_start_after.is_some(),
    })
}

pub async fn project_metadata_create_event_from_log(
    context: &DriverContext,
    document_id: MetaResourceId,
    event_id: Ulid,
) -> Result<(), MetadataProjectionError> {
    project_metadata_create_events_from_log(context, [(document_id, event_id)])
        .await
        .map(|_| ())
}

pub async fn project_metadata_create_events_from_log(
    context: &DriverContext,
    targets: impl IntoIterator<Item = (MetaResourceId, Ulid)>,
) -> Result<usize, MetadataProjectionError> {
    Ok(
        project_metadata_create_events_from_log_inner(context, targets, false)
            .await?
            .projected,
    )
}

struct MetadataProjectionFromLogResult {
    projected: usize,
    existing_events: usize,
}

async fn project_metadata_create_events_from_log_inner(
    context: &DriverContext,
    targets: impl IntoIterator<Item = (MetaResourceId, Ulid)>,
    delete_orphan_markers: bool,
) -> Result<MetadataProjectionFromLogResult, MetadataProjectionError> {
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let mut seen = BTreeSet::new();
    let mut events = Vec::new();
    let mut missing_event_markers = BTreeSet::new();
    for (document_id, event_id) in targets {
        if !seen.insert((document_id, event_id)) {
            continue;
        }
        match read_create_event_from_log(context, document_id, event_id).await {
            Ok(event) => events.push(event),
            Err(MetadataProjectionError::MetadataCreateEventMissing {
                document_id,
                event_id,
            }) if delete_orphan_markers => {
                warn!(%document_id, %event_id, "Deleting orphan metadata pending projection marker");
                missing_event_markers.insert((document_id, event_id));
            }
            Err(MetadataProjectionError::MetadataCreateEventMissing {
                document_id,
                event_id,
            }) => {
                return Err(MetadataProjectionError::MetadataCreateEventMissing {
                    document_id,
                    event_id,
                });
            }
            Err(error) => return Err(error),
        }
    }
    delete_pending_projection_markers(context, missing_event_markers).await?;
    let existing_events = events.len();
    let projected = project_metadata_create_events(context, events, local_node_id).await?;
    Ok(MetadataProjectionFromLogResult {
        projected,
        existing_events,
    })
}

async fn read_create_event_from_log(
    context: &DriverContext,
    document_id: MetaResourceId,
    event_id: Ulid,
) -> Result<MetadataCreateEventRecord, MetadataProjectionError> {
    let value = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
            key: metadata_event_log_key(document_id, event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(MetadataProjectionError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let Some(value) = value else {
        return Err(MetadataProjectionError::MetadataCreateEventMissing {
            document_id,
            event_id,
        });
    };
    let event: MetadataCreateEventRecord =
        postcard::from_bytes(&value).map_err(ConversionError::from)?;
    if event.record.document_id != document_id || event.event_id != event_id {
        return Err(MetadataProjectionError::UnexpectedEvent(format!(
            "metadata create event log target {document_id}/{event_id} did not match payload {}/{}",
            event.record.document_id, event.event_id
        )));
    }
    Ok(event)
}

pub async fn project_metadata_create_event(
    context: &DriverContext,
    event: MetadataCreateEventRecord,
    local_node_id: Option<NodeId>,
) -> Result<(), MetadataProjectionError> {
    project_metadata_create_events(context, vec![event], local_node_id)
        .await
        .map(|_| ())
}

pub async fn project_metadata_create_events(
    context: &DriverContext,
    events: Vec<MetadataCreateEventRecord>,
    local_node_id: Option<NodeId>,
) -> Result<usize, MetadataProjectionError> {
    if events.is_empty() {
        return Ok(0);
    }

    let mut realm_configs = BTreeMap::new();
    let mut lifecycle_cache: BTreeMap<String, bool> = BTreeMap::new();
    let mut registry_cache: BTreeMap<MetaResourceId, Option<MetadataRegistryRecord>> = BTreeMap::new();
    let mut status_cache: BTreeMap<MetaResourceId, Option<MetadataMaterializationStatusRecord>> =
        BTreeMap::new();
    let mut writes = Vec::new();
    let mut repair_deletes = Vec::new();
    let mut repaired_records = Vec::new();
    let mut outboxes = Vec::new();
    let mut pending_projection_delete_targets = BTreeSet::new();
    let mut pending_projection_retry_targets = BTreeSet::new();
    let mut needs_materialization_drain = false;
    let mut projected = 0usize;
    let mut projected_records = Vec::new();

    for event in events {
        let document_id = event.record.document_id;
        let now_ms = aruna_core::util::unix_timestamp_millis();
        if exceeds_clock_skew(&event, now_ms, max_clock_skew_ms()) {
            let rejected_total = CLOCK_SKEW_REJECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                event = "metadata.event.rejected",
                reason = "clock_skew",
                event_id = %event.event_id,
                document_id = %event.record.document_id,
                node_id = %event.node_id,
                updated_at_ms = event.record.updated_at_ms,
                occurred_at_ms = event.occurred_at_ms,
                now_ms,
                rejected_total,
                "Deferring metadata event stamped too far in the future; check NTP on the emitting node"
            );
            pending_projection_retry_targets.insert((document_id, event.event_id));
            continue;
        }
        pending_projection_delete_targets.insert((document_id, event.event_id));
        if metadata_graph_deleted_cached(context, &event.record.graph_iri, &mut lifecycle_cache)
            .await?
        {
            let existing_registry = match registry_cache.get(&document_id) {
                Some(record) => record.clone(),
                None => {
                    let record = read_existing_registry(context, document_id).await?;
                    registry_cache.insert(document_id, None);
                    record
                }
            };
            let stale_record = existing_registry.as_ref().unwrap_or(&event.record);
            repair_deletes.extend(metadata_registry_delete_entries(
                stale_record.group_id,
                stale_record.document_id,
            ));
            repaired_records.push(stale_record.clone());
            registry_cache.insert(document_id, None);
            status_cache.insert(document_id, None);
            continue;
        }

        let event =
            expand_create_event_holders_cached(context, event, local_node_id, &mut realm_configs)
                .await?;
        let existing_registry = match registry_cache.get(&document_id) {
            Some(record) => record.clone(),
            None => {
                let record = read_existing_registry(context, document_id).await?;
                registry_cache.insert(document_id, record.clone());
                record
            }
        };
        let event_is_newer = existing_registry
            .as_ref()
            .map(|record| {
                (event.record.updated_at_ms, event.event_id)
                    > (record.updated_at_ms, record.last_event_id)
            })
            .unwrap_or(true);
        let holders_changed = existing_registry
            .as_ref()
            .map(|record| record.holder_node_ids != event.record.holder_node_ids)
            .unwrap_or(false);
        let registry_exists = existing_registry.is_some();
        // The materialization status record tracks the newest event whose
        // materialization was enqueued or finished, so re-deliveries decide
        // the skip path from storage alone without a craqle round trip.
        let needs_materialization = if registry_exists {
            let status = match status_cache.get(&document_id) {
                Some(status) => status.clone(),
                None => {
                    let status = read_materialization_status(context, document_id).await?;
                    status_cache.insert(document_id, status.clone());
                    status
                }
            };
            status
                .map(|status| status.event_id < event.event_id)
                .unwrap_or(true)
        } else {
            true
        };
        let needs_projection =
            !registry_exists || event_is_newer || holders_changed || needs_materialization;

        if !needs_projection {
            continue;
        }

        let realm_config = realm_configs
            .get(&event.record.realm_id)
            .and_then(|config| config.as_ref());
        let authored_here = local_node_id == Some(event.node_id)
            && (!registry_exists || needs_materialization || holders_changed);
        let outbox = if authored_here && !event.record.holder_node_ids.is_empty() {
            // The local node authored this create event, so it originates the
            // document's lifecycle sync topic and may mint its genesis.
            Some(create_event_outbox_record(&event, realm_config, true))
        } else {
            None
        };
        // The registry row rides its own everywhere-bound topic, so it goes out
        // even when the document's own bucket has no holders to publish to.
        let registry_outbox =
            authored_here.then(|| registry_outbox_record(&event, realm_config, true));
        let audit = audit_record(&event);
        if needs_materialization {
            let now = aruna_core::util::unix_timestamp_millis();
            let status = new_pending_materialization_status(&event, now);
            let job = new_materialization_job(&event, now);
            writes.extend(create_records_outbox_and_materialization_write_entries(
                &event.record,
                &audit,
                event.event_id,
                outbox.as_ref(),
                &status,
                &job,
            )?);
            needs_materialization_drain = true;
            status_cache.insert(document_id, Some(status));
        } else {
            writes.extend(create_records_and_outbox_write_entries(
                &event.record,
                &audit,
                event.event_id,
                outbox.as_ref(),
            )?);
        }
        if let Some(outbox) = outbox {
            outboxes.push(outbox);
        }
        if let Some(registry_outbox) = registry_outbox.flatten() {
            writes.push(
                crate::document_sync_outbox::outbox_write_entry(&registry_outbox)
                    .map_err(ConversionError::from)?,
            );
            outboxes.push(registry_outbox);
        }
        if local_node_id == Some(event.node_id) {
            writes.push(metadata_document_lifecycle_write_entry(
                &MetadataDocumentLifecycleRecord::Upsert {
                    event: Box::new(event.clone()),
                },
            )?);
        }
        registry_cache.insert(document_id, Some(event.record.clone()));
        projected_records.push(event.record);
        projected = projected.saturating_add(1);
    }

    if !repair_deletes.is_empty() {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes: repair_deletes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataProjectionError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
        if let Some(metadata_handle) = context.metadata_handle.as_ref() {
            for record in &repaired_records {
                metadata_handle.remove_cached_registry_record(record.document_id);
            }
        }
    }

    if !writes.is_empty() {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataProjectionError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
        if let Some(metadata_handle) = context.metadata_handle.as_ref() {
            metadata_handle.upsert_cached_registry_records(&projected_records);
        }
    }
    if !outboxes.is_empty() {
        schedule_outbox_drain(context).await?;
    }
    if needs_materialization_drain {
        schedule_materialization_drain(context).await?;
    }
    write_pending_projection_markers(context, &pending_projection_retry_targets).await?;
    delete_pending_projection_markers(context, pending_projection_delete_targets).await?;

    if !pending_projection_retry_targets.is_empty() {
        return Err(MetadataProjectionError::ClockSkewDeferred {
            deferred: pending_projection_retry_targets.len(),
        });
    }

    Ok(projected)
}

async fn write_pending_projection_markers(
    context: &DriverContext,
    targets: &BTreeSet<(MetaResourceId, Ulid)>,
) -> Result<(), MetadataProjectionError> {
    if targets.is_empty() {
        return Ok(());
    }
    let writes = targets
        .iter()
        .map(|(document_id, event_id)| {
            (
                METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                metadata_pending_projection_key(*document_id, *event_id),
                ByteView::from(Vec::new()),
            )
        })
        .collect();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn delete_pending_projection_markers(
    context: &DriverContext,
    targets: BTreeSet<(MetaResourceId, Ulid)>,
) -> Result<(), MetadataProjectionError> {
    if targets.is_empty() {
        return Ok(());
    }
    let deletes = targets
        .into_iter()
        .map(|(document_id, event_id)| {
            metadata_pending_projection_delete_entry(document_id, event_id)
        })
        .collect();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn delete_pending_projection_marker_keys(
    context: &DriverContext,
    keys: Vec<Vec<u8>>,
) -> Result<(), MetadataProjectionError> {
    if keys.is_empty() {
        return Ok(());
    }
    let deletes = keys
        .into_iter()
        .map(|key| {
            (
                METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                ByteView::from(key),
            )
        })
        .collect();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn expand_create_event_holders_cached(
    context: &DriverContext,
    event: MetadataCreateEventRecord,
    local_node_id: Option<NodeId>,
    realm_configs: &mut BTreeMap<RealmId, Option<RealmConfigDocument>>,
) -> Result<MetadataCreateEventRecord, MetadataProjectionError> {
    let realm_config = if local_node_id == Some(event.node_id) {
        let realm_id = event.record.realm_id;
        match realm_configs.get(&realm_id) {
            Some(config) => config.clone(),
            None => {
                let config = read_realm_config(context, realm_id).await?;
                realm_configs.insert(realm_id, config.clone());
                config
            }
        }
    } else {
        None
    };

    expand_create_event_holders(event, local_node_id, realm_config.as_ref())
}

fn expand_create_event_holders(
    mut event: MetadataCreateEventRecord,
    local_node_id: Option<NodeId>,
    realm_config: Option<&RealmConfigDocument>,
) -> Result<MetadataCreateEventRecord, MetadataProjectionError> {
    event.record.last_event_id = event.event_id;
    let mut holders = event.record.holder_node_ids.clone();
    sort_node_ids(&mut holders);

    if local_node_id != Some(event.node_id) {
        event.record.holder_node_ids = holders;
        return Ok(event);
    };

    // Holders are derived from the bucket the create stamped, never re-chosen:
    // this runs again on replay, and a config change between runs would move the
    // document to a different topic.
    event.record.holder_node_ids = realm_config
        .map(|config| resolve_shard_holders(config, &event.record.placement))
        .unwrap_or_default();
    Ok(event)
}

async fn read_realm_config(
    context: &DriverContext,
    realm_id: aruna_core::structs::RealmId,
) -> Result<Option<RealmConfigDocument>, MetadataProjectionError> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    match context
        .storage_handle
        .send_effect(crate::document_repository::read_effect(&target, None))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| RealmConfigDocument::from_bytes(&value))
            .transpose()
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

/// The document's registry row, published on the registry class's own topic
/// rather than the document's bucket topic.
///
/// The bucket is replica-capped, so on a realm larger than the replication factor
/// most nodes never see the document's bucket topic at all — and every registry
/// row a node has today arrives as a side effect of that topic's lifecycle
/// events. Those nodes would therefore never learn the document exists: a GET
/// through them 404s forever, and update/delete could not even load the record
/// they need in order to decide to forward it to a holder. The registry class is
/// bound "everywhere", so this row reaches every node and gives each one the
/// `document_id -> placement -> holders` mapping the routing layer runs on.
///
/// `None` without a readable realm config (no strategy to resolve, and a
/// NIL-placed shard record would derive a NIL topic) and `None` when the registry
/// bucket has no holder at all: the realm has no eligible node, so there is nobody
/// to publish to, and replay re-plans the row once there is.
pub fn registry_outbox_record(
    event: &MetadataCreateEventRecord,
    realm_config: Option<&RealmConfigDocument>,
    allow_genesis: bool,
) -> Option<DocumentSyncOutboxRecord> {
    let record = &event.record;
    let config = realm_config?;
    let placement = registry_placement(config, record);
    if placement == PlacementRef::NIL {
        return None;
    }
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id: record.group_id,
        document_id: record.document_id,
    };
    let peers = resolve_shard_holders(config, &placement);
    if peers.is_empty() {
        return None;
    }
    let change = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: record.updated_at_ms,
            event_id: event.event_id,
            actor: event.node_id,
            updated_at_ms: record.updated_at_ms,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    };
    Some(DocumentSyncOutboxRecord {
        outbox_id: event.event_id,
        node_id: event.node_id,
        target,
        peers,
        event: DocumentSyncOutboxEvent::Upsert {
            bytes: postcard::to_allocvec(record).expect("metadata registry record serializes"),
            change,
        },
        placement,
        updated_at: event.occurred_at_ms / 1_000,
        allow_genesis,
    })
}

pub fn create_event_outbox_record(
    event: &MetadataCreateEventRecord,
    realm_config: Option<&RealmConfigDocument>,
    allow_genesis: bool,
) -> DocumentSyncOutboxRecord {
    let lifecycle = MetadataDocumentLifecycleRecord::Upsert {
        event: Box::new(event.clone()),
    };
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: event.record.document_id,
    };
    // The bucket is the one the create stamped; peers are its live holders, so a
    // publish after a rebalance targets the current holder set and not the
    // event-time one.
    let placement = event.record.placement;
    let peers = realm_config
        .map(|config| resolve_shard_holders(config, &placement))
        .unwrap_or_default();
    let change = metadata_document_lifecycle_revision_change(&lifecycle, event.node_id, placement);
    DocumentSyncOutboxRecord {
        outbox_id: event.event_id,
        node_id: event.node_id,
        target,
        peers,
        event: DocumentSyncOutboxEvent::Upsert {
            bytes: postcard::to_allocvec(&lifecycle)
                .expect("metadata document lifecycle event serializes"),
            change,
        },
        placement,
        updated_at: event.occurred_at_ms / 1_000,
        allow_genesis,
    }
}

async fn metadata_graph_deleted_cached(
    context: &DriverContext,
    graph_iri: &str,
    lifecycle_cache: &mut BTreeMap<String, bool>,
) -> Result<bool, MetadataProjectionError> {
    if let Some(deleted) = lifecycle_cache.get(graph_iri) {
        return Ok(*deleted);
    }
    let deleted = metadata_graph_deleted(context, graph_iri).await?;
    lifecycle_cache.insert(graph_iri.to_string(), deleted);
    Ok(deleted)
}

async fn metadata_graph_deleted(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<bool, MetadataProjectionError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(graph_iri),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let record: MetadataGraphLifecycleRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            Ok(record.is_deleted())
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn audit_record(event: &MetadataCreateEventRecord) -> MetadataAuditRecord {
    MetadataAuditRecord {
        realm_id: event.record.realm_id,
        group_id: event.record.group_id,
        document_id: event.record.document_id,
        graph_iri: event.record.graph_iri.clone(),
        user_id: event.user_id,
        node_id: event.node_id,
        operation: event.payload.audit_operation(),
        occurred_at_ms: event.occurred_at_ms,
        details: Some(format!(
            "kind={} holders={}",
            event.payload.materialization_kind(),
            event.record.holder_node_ids.len()
        )),
    }
}

async fn read_existing_registry(
    context: &DriverContext,
    document_id: MetaResourceId,
) -> Result<Option<MetadataRegistryRecord>, MetadataProjectionError> {
    match context
        .storage_handle
        .send_effect(read_registry_by_document_effect(document_id, None))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| postcard::from_bytes(&value).map_err(ConversionError::from))
            .transpose()
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_materialization_status(
    context: &DriverContext,
    document_id: MetaResourceId,
) -> Result<Option<MetadataMaterializationStatusRecord>, MetadataProjectionError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
            key: metadata_materialization_status_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| postcard::from_bytes(&value).map_err(ConversionError::from))
            .transpose()
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn schedule_outbox_drain(context: &DriverContext) -> Result<(), MetadataProjectionError> {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return Ok(());
    };
    match task_handle
        .send_effect(schedule_outbox_drain_effect())
        .await
    {
        Event::Task(aruna_core::task::TaskEvent::TimerScheduled { .. }) => Ok(()),
        Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) => {
            Err(MetadataProjectionError::UnexpectedEvent(message))
        }
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn schedule_materialization_drain(
    context: &DriverContext,
) -> Result<(), MetadataProjectionError> {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return Ok(());
    };
    match task_handle
        .send_effect(schedule_metadata_materialization_drain_effect())
        .await
    {
        Event::Task(aruna_core::task::TaskEvent::TimerScheduled { .. }) => Ok(()),
        Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) => {
            Err(MetadataProjectionError::UnexpectedEvent(message))
        }
        other => Err(MetadataProjectionError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::metadata::{MetadataCreateEventPayload, MetadataDocumentLifecycleRecord};
    use aruna_core::storage_entries::{
        metadata_create_event_write_entry, metadata_pending_projection_key,
    };
    use aruna_core::structs::{
        PlacementRef, PlacementStrategy, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use aruna_tasks::{InboundTaskHandler, TaskHandle};
    use async_trait::async_trait;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    struct RecordingTaskHandler {
        seen: mpsc::Sender<TaskKey>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingTaskHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let _ = self.seen.send(key).await;
        }
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn create_event() -> MetadataCreateEventRecord {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let group_id = Ulid::r#gen();
        let document_id = Ulid::r#gen();
        let event_id = Ulid::r#gen();
        let document_path = "datasets/outbox-lifecycle";
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![node(2)],
            created_at_ms: 1_000,
            updated_at_ms: 1_000,
            last_event_id: event_id,
        };
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: aruna_core::UserId::local(Ulid::r#gen(), realm_id),
            node_id: node(1),
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Lifecycle Outbox".to_string(),
                description: "Projector outbox envelope".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
            occurred_at_ms: 1_000,
        }
    }

    // Stands in for the create operation: stamps the bucket its origin would
    // have chosen from the buckets it holds.
    fn stamped(
        mut event: MetadataCreateEventRecord,
        config: &RealmConfigDocument,
    ) -> MetadataCreateEventRecord {
        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: event.record.document_id,
        };
        let context = crate::placement::PlacementResolutionContext {
            group_id: Some(event.record.group_id),
            metadata_path: Some(event.record.document_path.as_str()),
        };
        let (strategy, _) = crate::placement::strategy_for_target(config, &target, context)
            .expect("strategy resolves");
        event.record.placement = crate::placement::choose_origin_bucket(
            config,
            strategy,
            event.node_id,
            &crate::placement::subject_bytes(&target),
        )
        .expect("origin holds a bucket");
        event
    }

    fn realm_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.seed_default_placement();
        for node in nodes {
            config.ensure_node(*node, RealmNodeKind::Server);
        }
        config
    }

    async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn pending_projection_marker_exists(storage: &StorageHandle, key: Vec<u8>) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_pending_projection_marker_is_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let corrupt_key = b"short".to_vec();
        write_entries(
            &storage,
            vec![(
                METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                ByteView::from(corrupt_key.clone()),
                ByteView::from(Vec::new()),
            )],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drain_pending_metadata_projection_queue(&context)
            .await
            .expect("projection drain succeeds");

        assert_eq!(result.markers_examined, 0);
        assert_eq!(result.projected, 0);
        assert!(!result.has_more);
        assert!(!pending_projection_marker_exists(&storage, corrupt_key).await);
    }

    #[tokio::test]
    async fn malformed_pending_projection_marker_before_valid_is_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let corrupt_key = b"short".to_vec();
        let event = create_event();
        let valid_key = metadata_pending_projection_key(event.record.document_id, event.event_id);
        write_entries(
            &storage,
            vec![
                (
                    METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                    ByteView::from(corrupt_key.clone()),
                    ByteView::from(Vec::new()),
                ),
                metadata_create_event_write_entry(&event).expect("event log entry"),
                (
                    METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                    valid_key.clone(),
                    ByteView::from(Vec::new()),
                ),
            ],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drain_pending_metadata_projection_queue(&context)
            .await
            .expect("projection drain succeeds");

        assert_eq!(result.markers_examined, 1);
        assert_eq!(result.projected, 1);
        assert!(!pending_projection_marker_exists(&storage, corrupt_key).await);
        assert!(!pending_projection_marker_exists(&storage, valid_key.to_vec()).await);
    }

    #[tokio::test]
    async fn orphan_pending_projection_marker_is_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let document_id = Ulid::from_bytes([21u8; 16]);
        let event_id = Ulid::from_parts(21, 1);
        let marker_key = metadata_pending_projection_key(document_id, event_id);
        write_entries(
            &storage,
            vec![(
                METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                marker_key.clone(),
                ByteView::from(Vec::new()),
            )],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drain_pending_metadata_projection_queue(&context)
            .await
            .expect("projection drain succeeds");

        assert_eq!(result.markers_examined, 0);
        assert_eq!(result.projected, 0);
        assert!(!pending_projection_marker_exists(&storage, marker_key.to_vec()).await);
    }

    #[tokio::test]
    async fn restore_pending_projection_timer_schedules_when_marker_exists() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let event = create_event();
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).expect("event log entry"),
                (
                    METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                    metadata_pending_projection_key(event.record.document_id, event.event_id),
                    ByteView::from(Vec::new()),
                ),
            ],
        )
        .await;
        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
            .await;

        restore_pending_metadata_projection_timer(&storage, &task_handle).await;

        let restored_key = tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
            .await
            .expect("restored projection timer should fire")
            .expect("recording handler should receive timer key");
        assert_eq!(restored_key, TaskKey::DrainMetadataProjectionQueue);
    }

    #[test]
    fn metadata_origin_expands_holders_with_rendezvous() {
        let mut event = create_event();
        event.node_id = node(1);
        event.record.holder_node_ids = vec![node(1)];
        let config = realm_config(event.record.realm_id, &[node(1), node(2), node(3), node(4)]);
        let event = stamped(event, &config);

        let expanded =
            expand_create_event_holders(event.clone(), Some(event.node_id), Some(&config))
                .expect("holders expand");

        assert_eq!(
            expanded.record.holder_node_ids,
            resolve_shard_holders(&config, &event.record.placement)
        );
        assert_eq!(expanded.record.holder_node_ids.len(), 3);
        assert!(expanded.record.holder_node_ids.contains(&event.node_id));
        assert_eq!(expanded.record.last_event_id, event.event_id);
    }

    // The origin's outbox record rides the bucket stored on the record, and its
    // peers are that bucket's live holders.
    #[tokio::test]
    async fn outbox_rides_stored_bucket() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let event = create_event();
        let config = realm_config(event.record.realm_id, &[event.node_id, node(2), node(3)]);
        let event = stamped(event, &config);
        write_entries(
            &storage,
            vec![(
                aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*event.record.realm_id.as_bytes()),
                ByteView::from(postcard::to_allocvec(&config).expect("config serializes")),
            )],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        project_metadata_create_event(&context, event.clone(), Some(event.node_id))
            .await
            .expect("projection succeeds");

        let expected_holders = resolve_shard_holders(&config, &event.record.placement);

        // Two publishes, on two topics: the lifecycle event onto the document's
        // own capped bucket, the registry row onto the everywhere-bound registry
        // class. They share the event id, so they must not share an outbox key.
        let records = match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 4,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .iter()
                .map(|(_, value)| {
                    postcard::from_bytes::<DocumentSyncOutboxRecord>(value.as_ref())
                        .expect("outbox record decodes")
                })
                .collect::<Vec<_>>(),
            other => panic!("expected outbox iter result, got {other:?}"),
        };
        assert_eq!(records.len(), 2);

        let outbox = records
            .iter()
            .find(|record| {
                matches!(
                    record.target,
                    DocumentSyncTarget::MetadataDocumentLifecycle { .. }
                )
            })
            .expect("origin projection writes a lifecycle outbox record");
        assert_eq!(outbox.placement, event.record.placement);
        assert_eq!(outbox.peers, expected_holders);
        let DocumentSyncOutboxEvent::Upsert { change, .. } = &outbox.event else {
            panic!("expected lifecycle upsert outbox event");
        };
        assert_eq!(change.placement, event.record.placement);

        let registry = records
            .iter()
            .find(|record| matches!(record.target, DocumentSyncTarget::MetadataRegistry { .. }))
            .expect("origin projection writes a registry outbox record");
        let registry_ref = registry_placement(&config, &event.record);
        assert_eq!(registry.placement, registry_ref);
        assert_ne!(registry_ref, event.record.placement);
    }

    // A locally authored event with no eligible holders must not enqueue an
    // outbox publish; replay re-plans it once eligible holders exist.
    #[tokio::test]
    async fn holderless_projection_defers() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let event = create_event();
        let config = realm_config(event.record.realm_id, &[]);
        write_entries(
            &storage,
            vec![(
                aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*event.record.realm_id.as_bytes()),
                ByteView::from(postcard::to_allocvec(&config).expect("config serializes")),
            )],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        project_metadata_create_event(&context, event.clone(), Some(event.node_id))
            .await
            .expect("projection succeeds");

        let outboxes = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            outboxes,
            Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty()
        ));

        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: event.record.document_id,
        };
        let lifecycle = storage
            .send_effect(crate::document_repository::read_effect(&target, None))
            .await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = lifecycle
        else {
            panic!("origin must retain lifecycle transfer source, got {lifecycle:?}");
        };
        let lifecycle: MetadataDocumentLifecycleRecord =
            postcard::from_bytes(&value).expect("lifecycle decodes");
        let mut expected_event = event;
        expected_event.record.holder_node_ids.clear();
        assert_eq!(
            lifecycle,
            MetadataDocumentLifecycleRecord::Upsert {
                event: Box::new(expected_event)
            }
        );
    }

    #[test]
    fn metadata_strategy_replica_count_overrides_legacy_factor() {
        let mut event = create_event();
        event.node_id = node(1);
        event.record.holder_node_ids = vec![node(1), node(4)];
        let mut config = RealmConfigDocument::new(event.record.realm_id, Vec::new(), 4);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([9; 16]),
            name: "metadata-two".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for candidate in [node(1), node(2), node(3), node(4)] {
            config.ensure_node(candidate, RealmNodeKind::Server);
        }

        let event = stamped(event, &config);
        let expanded =
            expand_create_event_holders(event.clone(), Some(event.node_id), Some(&config))
                .expect("holders resolve");
        let expected = resolve_shard_holders(&config, &event.record.placement);

        assert_eq!(config.metadata_replication.default_replication_factor, 4);
        assert_eq!(expanded.record.holder_node_ids, expected);
        assert_eq!(expanded.record.holder_node_ids.len(), 2);
    }

    #[test]
    fn metadata_origin_excludes_user_node_from_holders() {
        let mut event = create_event();
        event.node_id = node(1);
        event.record.holder_node_ids = vec![node(1)];
        let mut config = RealmConfigDocument::new(event.record.realm_id, Vec::new(), 3);
        config.seed_default_placement();
        config.ensure_node(node(1), RealmNodeKind::User);
        config.ensure_node(node(2), RealmNodeKind::Server);
        config.ensure_node(node(3), RealmNodeKind::Server);
        config.ensure_node(node(4), RealmNodeKind::Server);
        // A user node holds no bucket, so its create fell back to the hashed
        // one (stage 1); it still never becomes a holder of it.
        event.record.placement = crate::placement::placement_ref_for_target(
            &config,
            &DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: event.record.document_id,
            },
            Default::default(),
        );

        let expanded = expand_create_event_holders(event, Some(node(1)), Some(&config))
            .expect("holders expand");

        let mut actual = expanded.record.holder_node_ids;
        sort_node_ids(&mut actual);
        let mut expected = vec![node(2), node(3), node(4)];
        sort_node_ids(&mut expected);
        assert_eq!(actual, expected);
    }

    #[test]
    fn metadata_recipient_preserves_authoritative_holders() {
        let mut event = create_event();
        event.node_id = node(1);
        event.record.holder_node_ids = vec![node(3), node(1), node(3)];
        let config = realm_config(event.record.realm_id, &[node(1), node(2), node(3), node(4)]);

        let expanded = expand_create_event_holders(event, Some(node(2)), Some(&config))
            .expect("holders normalize");

        assert_eq!(expanded.record.holder_node_ids, vec![node(1), node(3)]);
    }

    #[test]
    fn metadata_recipient_does_not_synthesize_origin_holder() {
        let mut event = create_event();
        event.node_id = node(1);
        event.record.holder_node_ids.clear();
        let config = realm_config(event.record.realm_id, &[node(1), node(2), node(3), node(4)]);

        let expanded = expand_create_event_holders(event, Some(node(2)), Some(&config))
            .expect("holders normalize");

        assert!(expanded.record.holder_node_ids.is_empty());
    }

    #[test]
    fn create_event_outbox_record_uses_document_lifecycle_stream() {
        let event = create_event();
        let outbox = create_event_outbox_record(&event, None, true);

        assert!(outbox.allow_genesis);
        assert_eq!(outbox.outbox_id, event.event_id);
        // Without a realm config the holders are unresolvable here; the outbox
        // drain resolves them from the bucket before publishing.
        assert!(outbox.peers.is_empty());
        assert_eq!(
            outbox.target,
            DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: event.record.document_id
            }
        );
        let DocumentSyncOutboxEvent::Upsert { bytes, change } = outbox.event else {
            panic!("expected lifecycle upsert outbox event");
        };
        let lifecycle: MetadataDocumentLifecycleRecord =
            postcard::from_bytes(&bytes).expect("lifecycle payload decodes");
        assert_eq!(
            lifecycle,
            MetadataDocumentLifecycleRecord::Upsert {
                event: Box::new(event.clone())
            }
        );
        assert_eq!(
            change.kind,
            aruna_core::document::DocumentSyncChangeKind::Upsert
        );
        assert_eq!(change.current.event_id, event.event_id);
    }

    #[test]
    fn create_and_update_stamp_equal_placement_refs() {
        let create = create_event();
        let config = realm_config(create.record.realm_id, &[node(1), node(2)]);
        let create = stamped(create, &config);

        // An update of the same document reuses its id and path; only the event
        // identity/timestamps advance.
        let mut update = create.clone();
        update.event_id = Ulid::r#gen();
        update.record.last_event_id = update.event_id;
        update.record.updated_at_ms = create.record.updated_at_ms + 1;
        update.occurred_at_ms = create.occurred_at_ms + 1;

        let placement_of = |event: &MetadataCreateEventRecord| {
            let DocumentSyncOutboxEvent::Upsert { change, .. } =
                create_event_outbox_record(event, Some(&config), true).event
            else {
                panic!("expected upsert outbox event");
            };
            change.placement
        };

        let create_ref = placement_of(&create);
        assert_ne!(create_ref, aruna_core::structs::PlacementRef::NIL);
        assert_eq!(create_ref, placement_of(&update));
    }

    fn skew_event(updated_at_ms: u64, occurred_at_ms: u64) -> MetadataCreateEventRecord {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let document_id = Ulid::r#gen();
        MetadataCreateEventRecord {
            event_id: Ulid::r#gen(),
            record: MetadataRegistryRecord {
                realm_id,
                group_id: Ulid::r#gen(),
                document_id,
                document_path: "datasets/skew".to_string(),
                graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
                public: false,
                permission_path: String::new(),
                placement: PlacementRef::NIL,
                holder_node_ids: Vec::new(),
                created_at_ms: updated_at_ms,
                updated_at_ms,
                last_event_id: Ulid::r#gen(),
            },
            user_id: UserId::nil(realm_id),
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            payload: MetadataCreateEventPayload::RoCrate {
                jsonld: String::new(),
            },
            occurred_at_ms,
        }
    }

    #[tokio::test]
    async fn skewed_direct_projection_writes_pending_marker_for_retry() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let now_ms = aruna_core::util::unix_timestamp_millis();
        let event = skew_event(now_ms + max_clock_skew_ms() + 60_000, now_ms);
        let marker_key = metadata_pending_projection_key(event.record.document_id, event.event_id);
        write_entries(
            &storage,
            vec![metadata_create_event_write_entry(&event).expect("event log entry")],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let error = project_metadata_create_events(&context, vec![event], None)
            .await
            .expect_err("skewed event should be deferred");

        assert!(matches!(
            error,
            MetadataProjectionError::ClockSkewDeferred { deferred: 1 }
        ));
        assert!(pending_projection_marker_exists(&storage, marker_key.to_vec()).await);
    }

    #[tokio::test]
    async fn skewed_queue_projection_keeps_pending_marker_for_retry() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let now_ms = aruna_core::util::unix_timestamp_millis();
        let event = skew_event(now_ms + max_clock_skew_ms() + 60_000, now_ms);
        let marker_key = metadata_pending_projection_key(event.record.document_id, event.event_id);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).expect("event log entry"),
                (
                    METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                    marker_key.clone(),
                    ByteView::from(Vec::new()),
                ),
            ],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let error = drain_pending_metadata_projection_queue(&context)
            .await
            .expect_err("skewed queued event should be deferred");

        assert!(matches!(
            error,
            MetadataProjectionError::ClockSkewDeferred { deferred: 1 }
        ));
        assert!(pending_projection_marker_exists(&storage, marker_key.to_vec()).await);
    }

    #[test]
    fn skew_guard_accepts_events_at_the_threshold() {
        let now_ms = 1_000_000;
        let max_skew_ms = 300_000;
        assert!(!exceeds_clock_skew(
            &skew_event(now_ms + max_skew_ms, now_ms),
            now_ms,
            max_skew_ms
        ));
        assert!(!exceeds_clock_skew(
            &skew_event(now_ms, now_ms + max_skew_ms),
            now_ms,
            max_skew_ms
        ));
        assert!(!exceeds_clock_skew(&skew_event(0, 0), now_ms, max_skew_ms));
    }

    #[test]
    fn skew_guard_rejects_events_past_the_threshold() {
        let now_ms = 1_000_000;
        let max_skew_ms = 300_000;
        assert!(exceeds_clock_skew(
            &skew_event(now_ms + max_skew_ms + 1, now_ms),
            now_ms,
            max_skew_ms
        ));
        assert!(exceeds_clock_skew(
            &skew_event(now_ms, now_ms + max_skew_ms + 1),
            now_ms,
            max_skew_ms
        ));
    }
}
