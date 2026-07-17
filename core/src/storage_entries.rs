use byteview::ByteView;
use ulid::Ulid;

use crate::NodeId;
use crate::admin_document_reducer::{AdminDocumentConflict, AdminDocumentReducerState};
use crate::admin_documents::AdminDocumentTarget;
use crate::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncConflict, DocumentSyncRevision,
    DocumentSyncTarget, ShardManifestEntry,
};
use crate::errors::ConversionError;
use crate::keyspaces::{
    ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE,
    DOCUMENT_SYNC_CONFLICT_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE,
    METADATA_CREATE_ACCEPTANCE_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
    METADATA_DOCUMENT_LIFECYCLE_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE,
    METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_GRAPH_PRUNE_JOB_KEYSPACE,
    METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE, METADATA_IRI_REFERENCE_INDEX_KEYSPACE,
    METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE, METADATA_MATERIALIZATION_JOB_KEYSPACE,
    METADATA_MATERIALIZATION_STATUS_KEYSPACE, METADATA_PENDING_PROJECTION_KEYSPACE,
    NOTIFICATION_INBOX_KEYSPACE, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE,
    NOTIFICATION_OUTBOX_KEYSPACE, NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE,
    SHARD_MANIFEST_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
};
use crate::metadata::{
    MetadataCreateEventRecord, MetadataDocumentLifecycleRecord, MetadataGraphLifecycleRecord,
    MetadataGraphPruneJobRecord, MetadataIriReferenceIndexRecord, MetadataMaterializationJobRecord,
    MetadataMaterializationStatusRecord,
};
use crate::structs::{
    MetadataRegistryRecord, NotificationOutboxRecord, NotificationRecord, PlacementRef, User,
    WatchSubscription, notification_inbox_key, notification_outbox_key,
    notification_prune_index_key, watch_subscription_key,
};
use crate::types::{GroupId, Key, KeySpace, UserId, Value};

pub fn subject_index_key(subject_id: &str) -> Key {
    ByteView::from(subject_id.as_bytes().to_vec())
}

pub fn subject_index_value(user_id: UserId) -> Value {
    ByteView::from(user_id.to_storage_key())
}

pub fn subject_index_writes(user: &User) -> Vec<(KeySpace, Key, Value)> {
    user.subject_ids
        .iter()
        .map(|subject_id| {
            (
                USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                subject_index_key(subject_id),
                subject_index_value(user.user_id),
            )
        })
        .collect()
}

pub fn stale_subject_index_deletes(
    previous: Option<&User>,
    current: Option<&User>,
) -> Vec<(KeySpace, Key)> {
    let Some(previous) = previous else {
        return Vec::new();
    };
    previous
        .subject_ids
        .iter()
        .filter(|subject_id| {
            current
                .map(|user| !user.subject_ids.contains(*subject_id))
                .unwrap_or(true)
        })
        .map(|subject_id| {
            (
                USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                subject_index_key(subject_id),
            )
        })
        .collect()
}

pub fn metadata_registry_key(group_id: GroupId, document_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&document_id.to_bytes());
    ByteView::from(bytes)
}

pub fn metadata_registry_prefix(group_id: GroupId) -> Key {
    ByteView::from(group_id.to_bytes().to_vec())
}

pub fn metadata_document_key(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub fn metadata_iri_reference_prefix(predicate_iri: &str, object_iri: &str) -> Key {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(blake3::hash(predicate_iri.as_bytes()).as_bytes());
    bytes.extend_from_slice(blake3::hash(object_iri.as_bytes()).as_bytes());
    ByteView::from(bytes)
}

pub fn metadata_iri_reference_key(
    predicate_iri: &str,
    object_iri: &str,
    document_id: Ulid,
    document_cursor: Ulid,
) -> Key {
    let mut bytes = metadata_iri_reference_prefix(predicate_iri, object_iri)
        .as_ref()
        .to_vec();
    bytes.extend_from_slice(&document_id.to_bytes());
    bytes.extend_from_slice(&document_cursor.to_bytes());
    ByteView::from(bytes)
}

/// Document id and cursor packed into an IRI reference index key, or `None` when
/// the key is not the expected 96-byte layout.
pub fn metadata_iri_reference_key_ids(key: &[u8]) -> Option<(Ulid, Ulid)> {
    if key.len() != 96 {
        return None;
    }
    let mut document_id = [0u8; 16];
    document_id.copy_from_slice(&key[64..80]);
    let mut document_cursor = [0u8; 16];
    document_cursor.copy_from_slice(&key[80..96]);
    Some((
        Ulid::from_bytes(document_id),
        Ulid::from_bytes(document_cursor),
    ))
}

pub fn metadata_create_acceptance_key(document_id: Ulid) -> Key {
    metadata_document_key(document_id)
}

pub fn metadata_graph_lifecycle_key(graph_iri: &str) -> Key {
    ByteView::from(blake3::hash(graph_iri.as_bytes()).as_bytes().to_vec())
}

pub fn metadata_document_lifecycle_key(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub fn metadata_event_log_prefix(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub fn metadata_event_log_key(document_id: Ulid, event_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&document_id.to_bytes());
    bytes.extend_from_slice(&event_id.to_bytes());
    ByteView::from(bytes)
}

pub fn metadata_pending_projection_key(document_id: Ulid, event_id: Ulid) -> Key {
    metadata_event_log_key(document_id, event_id)
}

pub fn metadata_pending_projection_target(key: &[u8]) -> Option<(Ulid, Ulid)> {
    if key.len() != 32 {
        return None;
    }
    let mut document_id = [0u8; 16];
    document_id.copy_from_slice(&key[..16]);
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&key[16..]);
    Some((Ulid::from_bytes(document_id), Ulid::from_bytes(event_id)))
}

pub fn document_sync_revision_key(target: &DocumentSyncTarget) -> Key {
    document_sync_target_sidecar_key(target)
}

pub fn document_sync_conflict_key(target: &DocumentSyncTarget) -> Key {
    document_sync_target_sidecar_key(target)
}

fn document_sync_target_sidecar_key(target: &DocumentSyncTarget) -> Key {
    let storage_key = target.storage_key();
    let keyspace = target.storage_keyspace().as_bytes();
    let mut bytes = Vec::with_capacity(keyspace.len() + 1 + storage_key.as_ref().len());
    bytes.extend_from_slice(keyspace);
    bytes.push(0);
    bytes.extend_from_slice(storage_key.as_ref());
    ByteView::from(bytes)
}

pub fn metadata_materialization_status_key(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub fn metadata_materialization_document_job_prefix(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub fn metadata_materialization_document_job_key(document_id: Ulid, event_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&document_id.to_bytes());
    bytes.extend_from_slice(&event_id.to_bytes());
    ByteView::from(bytes)
}

pub fn metadata_materialization_job_key(record: &MetadataMaterializationJobRecord) -> Key {
    let mut bytes = Vec::with_capacity(40);
    bytes.extend_from_slice(&record.due_at_ms.to_be_bytes());
    bytes.extend_from_slice(&record.document_id.to_bytes());
    bytes.extend_from_slice(&record.event_id.to_bytes());
    ByteView::from(bytes)
}

pub fn admin_document_target_key(target: &AdminDocumentTarget) -> Key {
    ByteView::from(admin_document_target_key_bytes(target))
}

pub fn admin_document_reducer_state_key(target: &AdminDocumentTarget) -> Key {
    admin_document_target_key(target)
}

pub fn admin_document_reducer_conflict_prefix(target: &AdminDocumentTarget) -> Key {
    admin_document_target_key(target)
}

pub fn admin_document_reducer_conflict_key(target: &AdminDocumentTarget, path: &str) -> Key {
    let mut bytes = admin_document_target_key_bytes(target);
    bytes.extend_from_slice(path.as_bytes());
    ByteView::from(bytes)
}

fn admin_document_target_key_bytes(target: &AdminDocumentTarget) -> Vec<u8> {
    match target {
        AdminDocumentTarget::Group { group_id } => {
            let mut bytes = Vec::with_capacity(17);
            bytes.push(b'g');
            bytes.extend_from_slice(&group_id.to_bytes());
            bytes
        }
        AdminDocumentTarget::Realm { realm_id } => {
            let mut bytes = Vec::with_capacity(33);
            bytes.push(b'r');
            bytes.extend_from_slice(realm_id.as_bytes());
            bytes
        }
        AdminDocumentTarget::User { user_id } => {
            let mut bytes = Vec::with_capacity(49);
            bytes.push(b'u');
            bytes.extend_from_slice(&user_id.to_storage_key());
            bytes
        }
        AdminDocumentTarget::RealmConfig { realm_id } => {
            let mut bytes = Vec::with_capacity(33);
            bytes.push(b'c');
            bytes.extend_from_slice(realm_id.as_bytes());
            bytes
        }
    }
}

pub fn metadata_graph_prune_job_key(record: &MetadataGraphPruneJobRecord) -> Key {
    let mut bytes = Vec::with_capacity(40);
    bytes.extend_from_slice(&record.due_at_ms.to_be_bytes());
    bytes.extend_from_slice(blake3::hash(record.graph_iri.as_bytes()).as_bytes());
    ByteView::from(bytes)
}

pub fn metadata_create_event_write_entry(
    event: &MetadataCreateEventRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_EVENT_LOG_KEYSPACE.to_string(),
        metadata_event_log_key(event.record.document_id, event.event_id),
        postcard::to_allocvec(event)?.into(),
    ))
}

pub fn metadata_create_acceptance_write_entry(
    event: &MetadataCreateEventRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
        metadata_create_acceptance_key(event.record.document_id),
        postcard::to_allocvec(event)?.into(),
    ))
}

pub fn metadata_pending_projection_write_entry(
    event: &MetadataCreateEventRecord,
) -> (KeySpace, Key, Value) {
    (
        METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
        metadata_pending_projection_key(event.record.document_id, event.event_id),
        ByteView::from(Vec::new()),
    )
}

pub fn metadata_pending_projection_delete_entry(
    document_id: Ulid,
    event_id: Ulid,
) -> (KeySpace, Key) {
    (
        METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
        metadata_pending_projection_key(document_id, event_id),
    )
}

pub fn metadata_create_event_and_pending_projection_write_entries(
    event: &MetadataCreateEventRecord,
) -> Result<Vec<(KeySpace, Key, Value)>, ConversionError> {
    Ok(vec![
        metadata_create_event_write_entry(event)?,
        metadata_pending_projection_write_entry(event),
    ])
}

pub fn metadata_graph_lifecycle_write_entry(
    record: &MetadataGraphLifecycleRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
        metadata_graph_lifecycle_key(&record.graph_iri),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn metadata_document_lifecycle_write_entry(
    record: &MetadataDocumentLifecycleRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
        metadata_document_lifecycle_key(record.document_id()),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn document_sync_revision_write_entry(
    target: &DocumentSyncTarget,
    change: &DocumentSyncChange,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
        document_sync_revision_key(target),
        postcard::to_allocvec(change)?.into(),
    ))
}

/// Manifest-row key: `strategy(16) ‖ epoch(8, le) ‖ shard(4, be) ‖ per-target
/// sidecar key`. The 28-byte shard prefix isolates one shard's rows on a scan;
/// the sidecar tail (keyspace-discriminated, see
/// [`document_sync_revision_key`]) keeps two targets sharing a storage key from
/// colliding.
pub fn shard_manifest_key(placement: &PlacementRef, target: &DocumentSyncTarget) -> Key {
    let tail = document_sync_target_sidecar_key(target);
    let mut bytes = Vec::with_capacity(28 + tail.as_ref().len());
    bytes.extend_from_slice(&shard_manifest_prefix(placement));
    bytes.extend_from_slice(tail.as_ref());
    ByteView::from(bytes)
}

/// The 28-byte shard prefix (`strategy ‖ epoch ‖ shard`) a manifest assembly
/// scan iterates.
pub fn shard_manifest_prefix(placement: &PlacementRef) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(28);
    bytes.extend_from_slice(&placement.strategy_id.to_bytes());
    bytes.extend_from_slice(&placement.epoch.to_le_bytes());
    bytes.extend_from_slice(&placement.shard.to_be_bytes());
    bytes
}

/// Manifest row for a shard-classed change: upsert or delete both write the row
/// at `change.current` (a delete leaves a tombstone). `Ok(None)` for shared
/// realm-scoped targets or a NIL placement (no shard governs the change yet).
pub fn shard_manifest_write_entry(
    target: &DocumentSyncTarget,
    change: &DocumentSyncChange,
) -> Result<Option<(KeySpace, Key, Value)>, ConversionError> {
    if !target.uses_shard_topic() || change.placement == PlacementRef::NIL {
        return Ok(None);
    }
    let entry = ShardManifestEntry {
        target: target.clone(),
        revision: change.current,
    };
    Ok(Some((
        SHARD_MANIFEST_KEYSPACE.to_string(),
        shard_manifest_key(&change.placement, target),
        postcard::to_allocvec(&entry)?.into(),
    )))
}

/// Manifest row for a metadata document lifecycle record, built from the same
/// revision change its sidecar carries so the manifest and the sidecar agree.
pub fn metadata_document_lifecycle_manifest_write_entry(
    record: &MetadataDocumentLifecycleRecord,
    delete_actor: NodeId,
    placement: PlacementRef,
) -> Result<Option<(KeySpace, Key, Value)>, ConversionError> {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: record.document_id(),
    };
    let change = metadata_document_lifecycle_revision_change(record, delete_actor, placement);
    shard_manifest_write_entry(&target, &change)
}

pub fn document_sync_conflict_write_entry(
    target: &DocumentSyncTarget,
    conflict: &DocumentSyncConflict,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        DOCUMENT_SYNC_CONFLICT_KEYSPACE.to_string(),
        document_sync_conflict_key(target),
        postcard::to_allocvec(conflict)?.into(),
    ))
}

pub fn metadata_document_lifecycle_revision_change(
    record: &MetadataDocumentLifecycleRecord,
    delete_actor: NodeId,
    placement: PlacementRef,
) -> DocumentSyncChange {
    match record {
        MetadataDocumentLifecycleRecord::Upsert { event } => DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: event.record.updated_at_ms,
                event_id: event.event_id,
                actor: event.node_id,
                updated_at_ms: event.occurred_at_ms,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        },
        MetadataDocumentLifecycleRecord::Delete { event } => DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: event.tombstone.updated_at_ms,
                event_id: event.event_id,
                actor: delete_actor,
                updated_at_ms: event.tombstone.updated_at_ms,
            },
            kind: DocumentSyncChangeKind::Delete,
            placement,
        },
    }
}

pub fn metadata_document_lifecycle_revision_write_entry(
    record: &MetadataDocumentLifecycleRecord,
    delete_actor: NodeId,
    placement: PlacementRef,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: record.document_id(),
    };
    let change = metadata_document_lifecycle_revision_change(record, delete_actor, placement);
    document_sync_revision_write_entry(&target, &change)
}

pub fn metadata_materialization_status_write_entry(
    record: &MetadataMaterializationStatusRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
        metadata_materialization_status_key(record.document_id),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn metadata_iri_reference_write_entry(
    record: &MetadataIriReferenceIndexRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
        metadata_iri_reference_key(
            &record.predicate_iri,
            &record.object_iri,
            record.document_id,
            record.document_cursor,
        ),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn metadata_materialization_job_write_entry(
    record: &MetadataMaterializationJobRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
        metadata_materialization_job_key(record),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn metadata_materialization_document_job_write_entry(
    record: &MetadataMaterializationJobRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
        metadata_materialization_document_job_key(record.document_id, record.event_id),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn metadata_graph_prune_job_write_entry(
    record: &MetadataGraphPruneJobRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
        metadata_graph_prune_job_key(record),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn notification_inbox_write_entries(
    record: &NotificationRecord,
) -> Result<Vec<(KeySpace, Key, Value)>, ConversionError> {
    Ok(vec![
        (
            NOTIFICATION_INBOX_KEYSPACE.to_string(),
            notification_inbox_key(
                record.recipient,
                record.created_at_ms,
                record.notification_id,
            ),
            record.to_bytes()?.into(),
        ),
        (
            NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(),
            notification_prune_index_key(record),
            ByteView::from(Vec::new()),
        ),
    ])
}

pub fn notification_inbox_delete_entries(record: &NotificationRecord) -> Vec<(KeySpace, Key)> {
    vec![
        (
            NOTIFICATION_INBOX_KEYSPACE.to_string(),
            notification_inbox_key(
                record.recipient,
                record.created_at_ms,
                record.notification_id,
            ),
        ),
        (
            NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(),
            notification_prune_index_key(record),
        ),
    ]
}

pub fn notification_inbox_update_entry(
    record: &NotificationRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        NOTIFICATION_INBOX_KEYSPACE.to_string(),
        notification_inbox_key(
            record.recipient,
            record.created_at_ms,
            record.notification_id,
        ),
        record.to_bytes()?.into(),
    ))
}

pub fn notification_outbox_write_entry(
    record: &NotificationOutboxRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
        notification_outbox_key(record.outbox_id),
        postcard::to_allocvec(record)?.into(),
    ))
}

pub fn watch_subscription_write_entry(
    subscription: &WatchSubscription,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
        watch_subscription_key(subscription.owner, subscription.watch_id),
        subscription.to_bytes()?.into(),
    ))
}

pub fn watch_subscription_delete_entry(owner: UserId, watch_id: Ulid) -> (KeySpace, Key) {
    (
        NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
        watch_subscription_key(owner, watch_id),
    )
}

pub fn admin_document_reducer_state_write_entry(
    state: &AdminDocumentReducerState,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&state.target),
        postcard::to_allocvec(state)?.into(),
    ))
}

pub fn admin_document_reducer_state_delete_entry(target: &AdminDocumentTarget) -> (KeySpace, Key) {
    (
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(target),
    )
}

pub fn admin_document_conflict_write_entry(
    target: &AdminDocumentTarget,
    conflict: &AdminDocumentConflict,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
        admin_document_reducer_conflict_key(target, &conflict.path),
        postcard::to_allocvec(conflict)?.into(),
    ))
}

pub fn admin_document_conflict_write_entries(
    state: &AdminDocumentReducerState,
) -> Result<Vec<(KeySpace, Key, Value)>, ConversionError> {
    state
        .conflicts
        .values()
        .map(|conflict| admin_document_conflict_write_entry(&state.target, conflict))
        .collect()
}

pub fn admin_document_conflict_delete_entry(
    target: &AdminDocumentTarget,
    path: &str,
) -> (KeySpace, Key) {
    (
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
        admin_document_reducer_conflict_key(target, path),
    )
}

pub fn stale_admin_document_conflict_delete_entries(
    previous: Option<&AdminDocumentReducerState>,
    current: Option<&AdminDocumentReducerState>,
) -> Vec<(KeySpace, Key)> {
    let Some(previous) = previous else {
        return Vec::new();
    };
    let current_conflicts = current
        .filter(|state| state.target == previous.target)
        .map(|state| &state.conflicts);

    previous
        .conflicts
        .keys()
        .filter(|path| {
            current_conflicts
                .map(|conflicts| !conflicts.contains_key(*path))
                .unwrap_or(true)
        })
        .map(|path| admin_document_conflict_delete_entry(&previous.target, path))
        .collect()
}

pub fn metadata_registry_write_entries(
    record: &MetadataRegistryRecord,
) -> Result<Vec<(KeySpace, Key, Value)>, ConversionError> {
    Ok(vec![
        (
            METADATA_INDEX_KEYSPACE.to_string(),
            metadata_registry_key(record.group_id, record.document_id),
            postcard::to_allocvec(record)?.into(),
        ),
        (
            METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
            metadata_document_key(record.document_id),
            postcard::to_allocvec(record)?.into(),
        ),
        (
            METADATA_HOLDERS_KEYSPACE.to_string(),
            metadata_registry_key(record.group_id, record.document_id),
            postcard::to_allocvec(&record.holder_node_ids)?.into(),
        ),
    ])
}

pub fn metadata_registry_delete_entries(
    group_id: GroupId,
    document_id: Ulid,
) -> Vec<(KeySpace, Key)> {
    vec![
        (
            METADATA_INDEX_KEYSPACE.to_string(),
            metadata_registry_key(group_id, document_id),
        ),
        (
            METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
            metadata_document_key(document_id),
        ),
        (
            METADATA_HOLDERS_KEYSPACE.to_string(),
            metadata_registry_key(group_id, document_id),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use ulid::Ulid;

    use super::{
        admin_document_conflict_write_entries, admin_document_reducer_conflict_key,
        admin_document_reducer_conflict_prefix, admin_document_reducer_state_key,
        admin_document_reducer_state_write_entry, document_sync_conflict_key,
        document_sync_conflict_write_entry, document_sync_revision_key,
        document_sync_revision_write_entry, metadata_iri_reference_key,
        metadata_iri_reference_prefix, metadata_iri_reference_write_entry, shard_manifest_key,
        shard_manifest_prefix, shard_manifest_write_entry,
        stale_admin_document_conflict_delete_entries,
    };
    use crate::admin_document_reducer::{
        AdminDocumentAttributeVersion, AdminDocumentConflict, AdminDocumentConflictValue,
        AdminDocumentReducerState,
    };
    use crate::admin_documents::{AdminDocumentClock, AdminDocumentDot, AdminDocumentTarget};
    use crate::document::ShardManifestEntry;
    use crate::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncConflict, DocumentSyncRevision,
        DocumentSyncTarget,
    };
    use crate::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE,
        DOCUMENT_SYNC_CONFLICT_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE,
        METADATA_IRI_REFERENCE_INDEX_KEYSPACE, SHARD_MANIFEST_KEYSPACE,
    };
    use crate::metadata::MetadataIriReferenceIndexRecord;
    use crate::structs::{PlacementRef, RealmId};
    use crate::{NodeId, UserId};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm_id(seed: u8) -> RealmId {
        RealmId::from_bytes([seed; 32])
    }

    fn user_id(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), realm_id(seed + 1))
    }

    fn user_target(seed: u8) -> AdminDocumentTarget {
        AdminDocumentTarget::User {
            user_id: user_id(seed),
        }
    }

    #[test]
    fn metadata_iri_reference_write_entry_roundtrips_exact_iris() {
        let document_id = Ulid::from_bytes([7; 16]);
        let record = MetadataIriReferenceIndexRecord {
            document_id,
            document_cursor: Ulid::from_bytes([8; 16]),
            predicate_iri: "http://schema.org/conformsTo".to_string(),
            object_iri: "https://example.test/profiles/one".to_string(),
            subject_iris: vec!["https://example.test/dataset".to_string()],
        };

        let (keyspace, key, value) = metadata_iri_reference_write_entry(&record).unwrap();
        let prefix = metadata_iri_reference_prefix(&record.predicate_iri, &record.object_iri);
        let decoded: MetadataIriReferenceIndexRecord =
            postcard::from_bytes(value.as_ref()).unwrap();

        assert_eq!(keyspace, METADATA_IRI_REFERENCE_INDEX_KEYSPACE);
        assert_eq!(prefix.as_ref().len(), 64);
        assert_eq!(key.as_ref().len(), 96);
        assert!(key.as_ref().starts_with(prefix.as_ref()));
        assert_eq!(
            key,
            metadata_iri_reference_key(
                &record.predicate_iri,
                &record.object_iri,
                document_id,
                record.document_cursor,
            )
        );
        assert_ne!(
            key,
            metadata_iri_reference_key(
                &record.predicate_iri,
                &record.object_iri,
                document_id,
                Ulid::from_bytes([9; 16]),
            )
        );
        assert_eq!(decoded, record);
    }

    fn realm_target(seed: u8) -> AdminDocumentTarget {
        AdminDocumentTarget::Realm {
            realm_id: realm_id(seed),
        }
    }

    fn realm_config_target(seed: u8) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: realm_id(seed),
        }
    }

    fn dot(seed: u8) -> AdminDocumentDot {
        AdminDocumentDot {
            event_id: Ulid::from_bytes([seed; 16]),
            origin_node_id: node(seed),
            origin_seq: u64::from(seed),
        }
    }

    fn revision(seed: u8, generation: u64) -> DocumentSyncRevision {
        DocumentSyncRevision {
            generation,
            event_id: Ulid::from_bytes([seed; 16]),
            actor: node(seed),
            updated_at_ms: u64::from(seed),
        }
    }

    fn conflict(path: &str, first_seed: u8, second_seed: u8) -> AdminDocumentConflict {
        AdminDocumentConflict {
            path: path.to_string(),
            values: vec![
                AdminDocumentConflictValue {
                    value: Some(format!("value-{first_seed}")),
                    dot: dot(first_seed),
                },
                AdminDocumentConflictValue {
                    value: Some(format!("value-{second_seed}")),
                    dot: dot(second_seed),
                },
            ],
        }
    }

    #[test]
    fn admin_document_reducer_state_write_entry_roundtrips() {
        let target = user_target(8);
        let attr_dot = dot(1);
        let state = AdminDocumentReducerState {
            target: target.clone(),
            clock: AdminDocumentClock::default().with_observed(attr_dot.origin_node_id, 1),
            applied_event_ids: BTreeSet::from([attr_dot.event_id]),
            user_attributes: BTreeMap::from([(
                "department".to_string(),
                AdminDocumentAttributeVersion {
                    value: Some("biology".to_string()),
                    dot: attr_dot,
                },
            )]),
            conflicts: BTreeMap::from([(
                "user.attributes.title".to_string(),
                conflict("user.attributes.title", 2, 3),
            )]),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
        };

        let (keyspace, key, value) = admin_document_reducer_state_write_entry(&state).unwrap();
        let decoded: AdminDocumentReducerState = postcard::from_bytes(value.as_ref()).unwrap();

        assert_eq!(keyspace, ADMIN_DOCUMENT_STATE_KEYSPACE);
        assert_eq!(key, admin_document_reducer_state_key(&target));
        assert_eq!(decoded, state);
    }

    #[test]
    fn realm_config_admin_document_reducer_state_write_entry_roundtrips() {
        let target = realm_config_target(8);
        let attr_dot = dot(1);
        let path = format!("realm_config.nodes.{}", node(4));
        let state = AdminDocumentReducerState {
            target: target.clone(),
            clock: AdminDocumentClock::default().with_observed(attr_dot.origin_node_id, 1),
            applied_event_ids: BTreeSet::from([attr_dot.event_id]),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            user_name: None,
            user_subject_ids: BTreeMap::from([(
                path,
                AdminDocumentAttributeVersion {
                    value: Some("server".to_string()),
                    dot: attr_dot,
                },
            )]),
            equivalent_value_dots: BTreeMap::new(),
        };

        let (keyspace, key, value) = admin_document_reducer_state_write_entry(&state).unwrap();
        let decoded: AdminDocumentReducerState = postcard::from_bytes(value.as_ref()).unwrap();

        assert_eq!(keyspace, ADMIN_DOCUMENT_STATE_KEYSPACE);
        assert_eq!(key, admin_document_reducer_state_key(&target));
        assert_eq!(decoded, state);
    }

    #[test]
    fn document_sync_revision_write_entry_roundtrips() {
        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([7; 16]),
        };
        let base = revision(1, 1);
        let change = DocumentSyncChange {
            base: Some(base),
            current: revision(2, 2),
            kind: DocumentSyncChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };

        let (keyspace, key, value) = document_sync_revision_write_entry(&target, &change).unwrap();
        let decoded: DocumentSyncChange = postcard::from_bytes(value.as_ref()).unwrap();

        assert_eq!(keyspace, DOCUMENT_SYNC_REVISION_KEYSPACE);
        assert_eq!(key, document_sync_revision_key(&target));
        assert_eq!(decoded, change);
    }

    fn shard_placement(shard: u32) -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            epoch: 0,
            shard,
        }
    }

    #[test]
    fn shard_manifest_write_entry_records_upsert_revision() {
        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([7; 16]),
        };
        let placement = shard_placement(3);
        let change = DocumentSyncChange {
            base: None,
            current: revision(2, 5),
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        };

        let (keyspace, key, value) = shard_manifest_write_entry(&target, &change)
            .unwrap()
            .unwrap();
        assert_eq!(keyspace, SHARD_MANIFEST_KEYSPACE);
        assert_eq!(key, shard_manifest_key(&placement, &target));
        let decoded: ShardManifestEntry = postcard::from_bytes(value.as_ref()).unwrap();
        assert_eq!(decoded.target, target);
        assert_eq!(decoded.revision, change.current);
    }

    #[test]
    fn shard_manifest_delete_keeps_tombstone_revision_at_same_key() {
        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([7; 16]),
        };
        let placement = shard_placement(3);
        let upsert = DocumentSyncChange {
            base: None,
            current: revision(2, 5),
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        };
        let delete = DocumentSyncChange {
            base: Some(upsert.current),
            current: revision(3, 6),
            kind: DocumentSyncChangeKind::Delete,
            placement,
        };

        let (_, upsert_key, _) = shard_manifest_write_entry(&target, &upsert)
            .unwrap()
            .unwrap();
        let (_, delete_key, delete_value) = shard_manifest_write_entry(&target, &delete)
            .unwrap()
            .unwrap();

        // A delete overwrites the same row with the delete revision (a tombstone),
        // it never removes it.
        assert_eq!(upsert_key, delete_key);
        let decoded: ShardManifestEntry = postcard::from_bytes(delete_value.as_ref()).unwrap();
        assert_eq!(decoded.revision, delete.current);
    }

    #[test]
    fn shard_manifest_write_entry_skips_shared_and_nil_placements() {
        let shared = DocumentSyncTarget::RealmConfig {
            realm_id: realm_id(2),
        };
        let change = DocumentSyncChange {
            base: None,
            current: revision(1, 1),
            kind: DocumentSyncChangeKind::Upsert,
            placement: shard_placement(3),
        };
        // Shared realm-scoped targets never ride a shard, so they get no row.
        assert!(
            shard_manifest_write_entry(&shared, &change)
                .unwrap()
                .is_none()
        );

        // A shard-classed target with a NIL placement has no governing shard yet.
        let shard_target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([7; 16]),
        };
        let nil_change = DocumentSyncChange {
            placement: PlacementRef::NIL,
            ..change
        };
        assert!(
            shard_manifest_write_entry(&shard_target, &nil_change)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn shard_manifest_keys_isolate_shards_and_targets() {
        let a = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([7; 16]),
        };
        let b = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([8; 16]),
        };
        let shard3 = shard_placement(3);
        let shard4 = shard_placement(4);

        // Every row for one shard shares the 28-byte shard prefix; a different
        // shard does not.
        let prefix3 = shard_manifest_prefix(&shard3);
        assert!(
            shard_manifest_key(&shard3, &a)
                .as_ref()
                .starts_with(&prefix3)
        );
        assert!(
            shard_manifest_key(&shard3, &b)
                .as_ref()
                .starts_with(&prefix3)
        );
        assert!(
            !shard_manifest_key(&shard4, &a)
                .as_ref()
                .starts_with(&prefix3)
        );
        // Distinct documents in one shard get distinct rows.
        assert_ne!(
            shard_manifest_key(&shard3, &a),
            shard_manifest_key(&shard3, &b)
        );
    }

    #[test]
    fn document_sync_conflict_write_entry_roundtrips() {
        let target = DocumentSyncTarget::User {
            user_id: user_id(8),
        };
        let local_change = DocumentSyncChange {
            base: None,
            current: revision(1, 1),
            kind: DocumentSyncChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        let incoming_change = DocumentSyncChange {
            base: None,
            current: revision(2, 2),
            kind: DocumentSyncChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        let conflict = DocumentSyncConflict {
            target: target.clone(),
            local_change: Some(local_change),
            local_bytes: Some(vec![1, 2]),
            incoming_change,
            incoming_bytes: vec![3, 4],
        };

        let (keyspace, key, value) =
            document_sync_conflict_write_entry(&target, &conflict).unwrap();
        let decoded: DocumentSyncConflict = postcard::from_bytes(value.as_ref()).unwrap();

        assert_eq!(keyspace, DOCUMENT_SYNC_CONFLICT_KEYSPACE);
        assert_eq!(key, document_sync_conflict_key(&target));
        assert_eq!(decoded, conflict);
    }

    #[test]
    fn document_sync_revision_keys_include_primary_keyspace() {
        let group_id = Ulid::from_bytes([4; 16]);
        let group = DocumentSyncTarget::Group { group_id };
        let auth = DocumentSyncTarget::GroupAuthorization { group_id };

        assert_ne!(
            document_sync_revision_key(&group),
            document_sync_revision_key(&auth)
        );
    }

    #[test]
    fn admin_document_conflict_entries_are_scoped_by_target_prefix() {
        let target = user_target(8);
        let other_target = user_target(9);
        let state = AdminDocumentReducerState {
            target: target.clone(),
            clock: AdminDocumentClock::default(),
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::from([
                (
                    "user.attributes.department".to_string(),
                    conflict("user.attributes.department", 1, 2),
                ),
                (
                    "user.attributes.title".to_string(),
                    conflict("user.attributes.title", 3, 4),
                ),
            ]),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
        };

        let entries = admin_document_conflict_write_entries(&state).unwrap();
        let prefix = admin_document_reducer_conflict_prefix(&target);
        let other_prefix = admin_document_reducer_conflict_prefix(&other_target);

        assert_eq!(entries.len(), 2);
        for (keyspace, key, value) in entries {
            let decoded: AdminDocumentConflict = postcard::from_bytes(value.as_ref()).unwrap();

            assert_eq!(keyspace, ADMIN_DOCUMENT_CONFLICT_KEYSPACE);
            assert!(key.as_ref().starts_with(prefix.as_ref()));
            assert!(!key.as_ref().starts_with(other_prefix.as_ref()));
            assert_eq!(
                &key.as_ref()[prefix.as_ref().len()..],
                decoded.path.as_bytes()
            );
            assert_eq!(
                key,
                admin_document_reducer_conflict_key(&target, &decoded.path)
            );
        }
    }

    #[test]
    fn realm_config_admin_document_keys_are_scoped_from_realm_auth() {
        let target = realm_config_target(8);
        let realm_auth_target = realm_target(8);
        let path = format!("realm_config.nodes.{}", node(4));
        let state_key = admin_document_reducer_state_key(&target);
        let conflict_prefix = admin_document_reducer_conflict_prefix(&target);
        let conflict_key = admin_document_reducer_conflict_key(&target, &path);

        assert_ne!(
            state_key,
            admin_document_reducer_state_key(&realm_auth_target)
        );
        assert_eq!(conflict_prefix, state_key);
        assert!(conflict_key.as_ref().starts_with(conflict_prefix.as_ref()));
        assert_eq!(
            &conflict_key.as_ref()[conflict_prefix.as_ref().len()..],
            path.as_bytes()
        );
    }

    #[test]
    fn stale_admin_document_conflict_delete_entries_remove_only_missing_paths() {
        let target = user_target(8);
        let previous = AdminDocumentReducerState {
            target: target.clone(),
            clock: AdminDocumentClock::default(),
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::from([
                (
                    "user.attributes.department".to_string(),
                    conflict("user.attributes.department", 1, 2),
                ),
                (
                    "user.attributes.title".to_string(),
                    conflict("user.attributes.title", 3, 4),
                ),
            ]),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
        };
        let current = AdminDocumentReducerState {
            conflicts: BTreeMap::from([(
                "user.attributes.title".to_string(),
                conflict("user.attributes.title", 3, 4),
            )]),
            ..previous.clone()
        };

        let deletes = stale_admin_document_conflict_delete_entries(Some(&previous), Some(&current));

        assert_eq!(
            deletes,
            vec![(
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                admin_document_reducer_conflict_key(&target, "user.attributes.department"),
            )]
        );
        assert!(stale_admin_document_conflict_delete_entries(None, Some(&current)).is_empty());
    }
}
