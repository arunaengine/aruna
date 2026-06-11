use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataError, MetadataMaterializationStatusRecord,
};
use aruna_core::storage_entries::{metadata_event_log_key, metadata_materialization_status_key};
use aruna_core::structs::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord, RealmConfigDocument,
    RealmId,
};
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
use crate::sync_placement::{select_sync_peers, sort_node_ids};

const REPLAY_PAGE_SIZE: usize = 1_024;

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
                start_after: start_after.take(),
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
    let mut registry_cache: BTreeMap<Ulid, Option<MetadataRegistryRecord>> = BTreeMap::new();
    let mut status_cache: BTreeMap<Ulid, Option<MetadataMaterializationStatusRecord>> =
        BTreeMap::new();
    let mut writes = Vec::new();
    let mut outboxes = Vec::new();
    let mut needs_materialization_drain = false;
    let mut projected = 0usize;
    let mut projected_records = Vec::new();

    for event in events {
        let event = expand_create_event_holders_cached(context, event, &mut realm_configs).await?;
        let document_id = event.record.document_id;
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

    Ok(projected)
}

async fn expand_create_event_holders_cached(
    context: &DriverContext,
    mut event: MetadataCreateEventRecord,
    realm_configs: &mut BTreeMap<RealmId, Option<RealmConfigDocument>>,
) -> Result<MetadataCreateEventRecord, MetadataProjectionError> {
    event.record.last_event_id = event.event_id;
    let realm_id = event.record.realm_id;
    let realm_config = match realm_configs.get(&realm_id) {
        Some(config) => config.clone(),
        None => {
            let config = read_realm_config(context, realm_id).await?;
            realm_configs.insert(realm_id, config.clone());
            config
        }
    };
    let Some(realm_config) = realm_config else {
        sort_node_ids(&mut event.record.holder_node_ids);
        if !event.record.holder_node_ids.contains(&event.node_id) {
            event.record.holder_node_ids.push(event.node_id);
            sort_node_ids(&mut event.record.holder_node_ids);
        }
        return Ok(event);
    };

    let target = DocumentSyncTarget::MetadataCreateEvent {
        document_id: event.record.document_id,
        event_id: event.event_id,
    };
    let desired_holder_count = realm_config.metadata_replication_factor_for(
        event.record.group_id,
        Some(event.record.document_path.as_str()),
    );
    let mut holders = event.record.holder_node_ids.clone();
    if !holders.contains(&event.node_id) {
        holders.push(event.node_id);
    }
    sort_node_ids(&mut holders);

    if holders.len() < desired_holder_count {
        let candidates = realm_config.node_ids()?;
        let mut additional = select_sync_peers(
            &target,
            event.node_id,
            &candidates,
            &holders,
            desired_holder_count.saturating_sub(holders.len()),
        );
        holders.append(&mut additional);
        sort_node_ids(&mut holders);
    }

    event.record.holder_node_ids = holders;
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
    DocumentSyncOutboxRecord {
        outbox_id: event.event_id,
        node_id: event.node_id,
        target: DocumentSyncTarget::MetadataCreateEvent {
            document_id: event.record.document_id,
            event_id: event.event_id,
        },
        peers: event.record.holder_node_ids.clone(),
        event: DocumentSyncOutboxEvent::Upsert {
            bytes: postcard::to_allocvec(event).expect("metadata create event serializes"),
        },
        updated_at: event.occurred_at_ms / 1_000,
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
        operation: MetadataAuditOperation::Create,
        occurred_at_ms: event.occurred_at_ms,
        details: Some(format!("holders={}", event.record.holder_node_ids.len())),
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
