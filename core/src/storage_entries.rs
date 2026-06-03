use byteview::ByteView;
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::keyspaces::{
    METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_HOLDERS_KEYSPACE,
    METADATA_INDEX_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
};
use crate::metadata::MetadataGraphLifecycleRecord;
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

pub fn metadata_graph_lifecycle_write_entry(
    record: &MetadataGraphLifecycleRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
        metadata_graph_lifecycle_key(&record.graph_iri),
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
