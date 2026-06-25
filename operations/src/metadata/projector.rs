use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget};
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
    metadata_event_log_key, metadata_graph_lifecycle_key, metadata_materialization_status_key,
    metadata_pending_projection_delete_entry, metadata_pending_projection_target,
    metadata_registry_delete_entries,
};
use aruna_core::structs::{
    MetadataAuditRecord, MetadataRegistryRecord, RealmConfigDocument, RealmId,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use thiserror::Error;
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
use crate::sync_placement::{complete_authoritative_holders, sort_node_ids};
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
    MetadataCreateEventMissing { document_id: Ulid, event_id: Ulid },
    #[error("unexpected event while projecting metadata create event: {0}")]
    UnexpectedEvent(String),
}

fn metadata_projection_drain_task_effect(after: Duration) -> TaskEffect {
    TaskEffect::ResetTimer {
        key: TaskKey::DrainMetadataProjectionQueue,
        after,
    }
}

pub fn schedule_metadata_projection_drain_effect(after: Duration) -> Effect {
    Effect::Task(metadata_projection_drain_task_effect(after))
}

pub async fn schedule_metadata_projection_retry(
    context: &DriverContext,
    after: Duration,
) -> Result<(), MetadataProjectionError> {
    let effect = metadata_projection_drain_task_effect(after);
    persist_task_effect(&context.storage_handle, &effect)
        .await
        .map_err(MetadataProjectionError::UnexpectedEvent)?;

    let Some(task_handle) = context.task_handle.as_ref() else {
        return Err(MetadataProjectionError::UnexpectedEvent(
            "task handle unavailable".to_string(),
        ));
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
            return Err(MetadataProjectionError::UnexpectedEvent(
                "invalid metadata pending projection marker key".to_string(),
            ));
        };
        targets.push(target);
    }
    let markers_examined = targets.len();
    let projected = project_metadata_create_events_from_log(context, targets).await?;
    Ok(PendingMetadataProjectionDrainResult {
        markers_examined,
        projected,
        has_more: next_start_after.is_some(),
    })
}

pub async fn project_metadata_create_event_from_log(
    context: &DriverContext,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<(), MetadataProjectionError> {
    project_metadata_create_events_from_log(context, [(document_id, event_id)])
        .await
        .map(|_| ())
}

pub async fn project_metadata_create_events_from_log(
    context: &DriverContext,
    targets: impl IntoIterator<Item = (Ulid, Ulid)>,
) -> Result<usize, MetadataProjectionError> {
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let mut seen = BTreeSet::new();
    let mut events = Vec::new();
    for (document_id, event_id) in targets {
        if !seen.insert((document_id, event_id)) {
            continue;
        }
        events.push(read_create_event_from_log(context, document_id, event_id).await?);
    }
    project_metadata_create_events(context, events, local_node_id).await
}

async fn read_create_event_from_log(
    context: &DriverContext,
    document_id: Ulid,
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
    let mut registry_cache: BTreeMap<Ulid, Option<MetadataRegistryRecord>> = BTreeMap::new();
    let mut status_cache: BTreeMap<Ulid, Option<MetadataMaterializationStatusRecord>> =
        BTreeMap::new();
    let mut writes = Vec::new();
    let mut repair_deletes = Vec::new();
    let mut repaired_records = Vec::new();
    let mut outboxes = Vec::new();
    let mut pending_projection_delete_targets = BTreeSet::new();
    let mut needs_materialization_drain = false;
    let mut projected = 0usize;
    let mut projected_records = Vec::new();

    for event in events {
        let document_id = event.record.document_id;
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

        let outbox = if local_node_id == Some(event.node_id)
            && (!registry_exists || needs_materialization || holders_changed)
        {
            Some(create_event_outbox_record(&event))
        } else {
            None
        };
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
                metadata_handle.remove_visible_registry_record(record.document_id);
            }
        }
        for record in &repaired_records {
            crate::metadata::visible_registry::remove_visible_registry_record(
                context,
                record.group_id,
                record.document_id,
            );
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
            metadata_handle.upsert_visible_registry_records(&projected_records);
        }
        crate::metadata::visible_registry::upsert_visible_registry_records(
            context,
            &projected_records,
        );
    }
    if !outboxes.is_empty() {
        schedule_outbox_drain(context).await?;
    }
    if needs_materialization_drain {
        schedule_materialization_drain(context).await?;
    }
    delete_pending_projection_markers(context, pending_projection_delete_targets).await?;

    Ok(projected)
}

async fn delete_pending_projection_markers(
    context: &DriverContext,
    targets: BTreeSet<(Ulid, Ulid)>,
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
    if !holders.contains(&event.node_id) {
        holders.push(event.node_id);
    }
    sort_node_ids(&mut holders);

    let Some(realm_config) = realm_config else {
        event.record.holder_node_ids = holders;
        return Ok(event);
    };
    if local_node_id != Some(event.node_id) {
        event.record.holder_node_ids = holders;
        return Ok(event);
    };

    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: event.record.document_id,
    };
    let desired_holder_count = realm_config.metadata_replication_factor_for(
        event.record.group_id,
        Some(event.record.document_path.as_str()),
    );
    let candidates = realm_config.node_ids()?;

    event.record.holder_node_ids =
        complete_authoritative_holders(&target, &candidates, &holders, desired_holder_count);
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

pub fn create_event_outbox_record(event: &MetadataCreateEventRecord) -> DocumentSyncOutboxRecord {
    let lifecycle = MetadataDocumentLifecycleRecord::Upsert {
        event: Box::new(event.clone()),
    };
    DocumentSyncOutboxRecord {
        outbox_id: event.event_id,
        node_id: event.node_id,
        target: DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: event.record.document_id,
        },
        peers: event.record.holder_node_ids.clone(),
        event: DocumentSyncOutboxEvent::Upsert {
            bytes: postcard::to_allocvec(&lifecycle)
                .expect("metadata document lifecycle event serializes"),
        },
        updated_at: event.occurred_at_ms / 1_000,
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
    document_id: ulid::Ulid,
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
    document_id: Ulid,
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
    use aruna_core::structs::{RealmConfigDocument, RealmId, RealmNodeKind};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn create_event() -> MetadataCreateEventRecord {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let group_id = Ulid::new();
        let document_id = Ulid::new();
        let event_id = Ulid::new();
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
            holder_node_ids: vec![node(2)],
            created_at_ms: 1_000,
            updated_at_ms: 1_000,
            last_event_id: event_id,
        };
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
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

    fn realm_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        for node in nodes {
            config.ensure_node(*node, RealmNodeKind::Server);
        }
        config
    }

    #[test]
    fn metadata_origin_expands_holders_with_rendezvous() {
        let mut event = create_event();
        event.node_id = node(1);
        event.record.holder_node_ids = vec![node(1)];
        let config = realm_config(event.record.realm_id, &[node(1), node(2), node(3), node(4)]);

        let expanded =
            expand_create_event_holders(event.clone(), Some(event.node_id), Some(&config))
                .expect("holders expand");

        assert_eq!(expanded.record.holder_node_ids.len(), 3);
        assert!(expanded.record.holder_node_ids.contains(&event.node_id));
        assert_eq!(expanded.record.last_event_id, event.event_id);
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
    fn metadata_recipient_does_not_expand_single_holder_legacy_event() {
        let mut event = create_event();
        event.node_id = node(1);
        event.record.holder_node_ids.clear();
        let config = realm_config(event.record.realm_id, &[node(1), node(2), node(3), node(4)]);

        let expanded = expand_create_event_holders(event, Some(node(2)), Some(&config))
            .expect("holders normalize");

        assert_eq!(expanded.record.holder_node_ids, vec![node(1)]);
    }

    #[test]
    fn create_event_outbox_record_uses_document_lifecycle_stream() {
        let event = create_event();
        let outbox = create_event_outbox_record(&event);

        assert_eq!(outbox.outbox_id, event.event_id);
        assert_eq!(outbox.peers, event.record.holder_node_ids);
        assert_eq!(
            outbox.target,
            DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: event.record.document_id
            }
        );
        let DocumentSyncOutboxEvent::Upsert { bytes } = outbox.event else {
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
    }
}
