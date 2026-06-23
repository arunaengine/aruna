use byteview::ByteView;
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::keyspaces::{
    METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_GRAPH_PRUNE_JOB_KEYSPACE, METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE,
    METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE, METADATA_MATERIALIZATION_JOB_KEYSPACE,
    METADATA_MATERIALIZATION_STATUS_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
};
use crate::metadata::{
    MetadataCreateEventRecord, MetadataDocumentLifecycleRecord, MetadataGraphLifecycleRecord,
    MetadataGraphPruneJobRecord, MetadataMaterializationJobRecord,
    MetadataMaterializationStatusRecord,
};
use crate::structs::{MetadataRegistryRecord, User};
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

pub fn metadata_materialization_status_write_entry(
    record: &MetadataMaterializationStatusRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
        metadata_materialization_status_key(record.document_id),
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
