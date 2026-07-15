use std::collections::{BTreeMap, BTreeSet, HashMap};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    METADATA_IRI_REFERENCE_INDEX_KEYSPACE, METADATA_MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataError, MetadataIriReferenceIndexRecord, MetadataMaterializationState,
    MetadataMaterializationStatusRecord,
};
use aruna_core::storage_entries::{
    metadata_iri_reference_prefix, metadata_iri_reference_write_entry,
    metadata_materialization_status_key,
};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use ulid::Ulid;

use crate::driver::DriverContext;

const IRI_INDEX_PAGE_SIZE: usize = 128;
const IRI_INDEX_WRITE_BATCH_SIZE: usize = 128;

pub(crate) const SCHEMA_CONFORMS_TO_IRI: &str = "http://schema.org/conformsTo";

pub(crate) fn project_metadata_iri_references(
    document_id: Ulid,
    document_cursor: Ulid,
    references: Vec<(String, String, String)>,
) -> Vec<MetadataIriReferenceIndexRecord> {
    let mut grouped = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for (subject_iri, predicate_iri, object_iri) in references {
        grouped
            .entry((predicate_iri, object_iri))
            .or_default()
            .insert(subject_iri);
    }

    grouped
        .into_iter()
        .map(
            |((predicate_iri, object_iri), subject_iris)| MetadataIriReferenceIndexRecord {
                document_id,
                document_cursor,
                predicate_iri,
                object_iri,
                subject_iris: subject_iris.into_iter().collect(),
            },
        )
        .collect()
}

pub(crate) fn metadata_iri_reference_write_entries(
    records: &[MetadataIriReferenceIndexRecord],
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    records
        .iter()
        .map(metadata_iri_reference_write_entry)
        .collect()
}

pub(crate) async fn lookup_metadata_iri_references(
    storage: &StorageHandle,
    registry_records: &[MetadataRegistryRecord],
    predicate_iri: &str,
    object_iri: &str,
) -> Result<BTreeMap<Ulid, Vec<String>>, MetadataError> {
    let prefix = metadata_iri_reference_prefix(predicate_iri, object_iri);
    let mut start_after = None;
    let mut records = Vec::new();

    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start_after.map(IterStart::After),
                limit: IRI_INDEX_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                for (_, value) in values {
                    records.push(
                        postcard::from_bytes::<MetadataIriReferenceIndexRecord>(&value).map_err(
                            |error| {
                                MetadataError::Backend(format!(
                                    "failed to decode metadata IRI reference index: {error}"
                                ))
                            },
                        )?,
                    );
                }
                match next_start_after {
                    Some(next) => start_after = Some(next),
                    None => break,
                }
            }
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataError::Backend(error.to_string()));
            }
            other => {
                return Err(MetadataError::Backend(format!(
                    "unexpected metadata IRI reference index event: {other:?}"
                )));
            }
        }
    }

    Ok(collect_matching_iri_references(
        records,
        registry_records,
        predicate_iri,
        object_iri,
    ))
}

pub(crate) async fn rebuild_metadata_iri_reference_index(
    context: &DriverContext,
) -> Result<usize, MetadataError> {
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataError::HandleMissing)?;
    let registry_records = metadata_handle.list_cached_registry_records().await?;
    let mut written = 0usize;

    for record in registry_records.iter() {
        let Some(status) =
            read_materialization_status(&context.storage_handle, record.document_id).await?
        else {
            continue;
        };
        if status.state != MetadataMaterializationState::Materialized
            || status.event_id != record.last_event_id
            || status.graph_iri != record.graph_iri
        {
            continue;
        }

        let references = metadata_handle
            .snapshot_iri_references(record.graph_iri.clone())
            .await?;
        let projected =
            project_metadata_iri_references(record.document_id, record.last_event_id, references);
        write_metadata_iri_references(&context.storage_handle, &projected).await?;
        written = written.saturating_add(projected.len());
    }

    Ok(written)
}

fn collect_matching_iri_references(
    records: Vec<MetadataIriReferenceIndexRecord>,
    registry_records: &[MetadataRegistryRecord],
    predicate_iri: &str,
    object_iri: &str,
) -> BTreeMap<Ulid, Vec<String>> {
    let current_cursors = registry_records
        .iter()
        .map(|record| (record.document_id, record.last_event_id))
        .collect::<HashMap<_, _>>();
    let mut matches = BTreeMap::<Ulid, BTreeSet<String>>::new();
    for record in records {
        if record.predicate_iri != predicate_iri
            || record.object_iri != object_iri
            || current_cursors.get(&record.document_id) != Some(&record.document_cursor)
        {
            continue;
        }
        matches
            .entry(record.document_id)
            .or_default()
            .extend(record.subject_iris);
    }
    matches
        .into_iter()
        .map(|(document_id, subject_iris)| (document_id, subject_iris.into_iter().collect()))
        .collect()
}

async fn read_materialization_status(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<Option<MetadataMaterializationStatusRecord>, MetadataError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
            key: metadata_materialization_status_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes(&bytes).map_err(|error| {
                    MetadataError::Backend(format!(
                        "failed to decode metadata materialization status: {error}"
                    ))
                })
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataError::Backend(error.to_string()))
        }
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata materialization status event: {other:?}"
        ))),
    }
}

async fn write_metadata_iri_references(
    storage: &StorageHandle,
    records: &[MetadataIriReferenceIndexRecord],
) -> Result<(), MetadataError> {
    for records in records.chunks(IRI_INDEX_WRITE_BATCH_SIZE) {
        let writes = metadata_iri_reference_write_entries(records)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataError::Backend(error.to_string()));
            }
            other => {
                return Err(MetadataError::Backend(format!(
                    "unexpected metadata IRI reference index write event: {other:?}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{PlacementRef, RealmId};

    fn registry_record(document_id: Ulid, cursor: Ulid) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let group_id = Ulid::from_parts(1, 1);
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/indexed".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/indexed",
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            last_event_id: cursor,
        }
    }

    #[test]
    fn projection_groups_and_sorts_subjects() {
        let document_id = Ulid::from_parts(1, 1);
        let cursor = Ulid::from_parts(2, 1);
        let records = project_metadata_iri_references(
            document_id,
            cursor,
            vec![
                (
                    "https://example.test/subject-b".to_string(),
                    SCHEMA_CONFORMS_TO_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
                (
                    "https://example.test/subject-a".to_string(),
                    SCHEMA_CONFORMS_TO_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
                (
                    "https://example.test/subject-a".to_string(),
                    SCHEMA_CONFORMS_TO_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
            ],
        );

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].document_id, document_id);
        assert_eq!(records[0].document_cursor, cursor);
        assert_eq!(
            records[0].subject_iris,
            vec![
                "https://example.test/subject-a".to_string(),
                "https://example.test/subject-b".to_string(),
            ]
        );
    }

    #[test]
    fn lookup_fences_exact_strings_and_document_cursor() {
        let current_document_id = Ulid::from_parts(1, 1);
        let stale_document_id = Ulid::from_parts(1, 2);
        let cursor = Ulid::from_parts(2, 1);
        let profile = "https://example.test/profile";
        let records = vec![
            MetadataIriReferenceIndexRecord {
                document_id: current_document_id,
                document_cursor: cursor,
                predicate_iri: SCHEMA_CONFORMS_TO_IRI.to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/current".to_string()],
            },
            MetadataIriReferenceIndexRecord {
                document_id: stale_document_id,
                document_cursor: Ulid::from_parts(2, 0),
                predicate_iri: SCHEMA_CONFORMS_TO_IRI.to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/stale".to_string()],
            },
            MetadataIriReferenceIndexRecord {
                document_id: current_document_id,
                document_cursor: cursor,
                predicate_iri: "https://example.test/hash-collision".to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/collision".to_string()],
            },
        ];
        let registry_records = vec![
            registry_record(current_document_id, cursor),
            registry_record(stale_document_id, cursor),
        ];

        let matches = collect_matching_iri_references(
            records,
            &registry_records,
            SCHEMA_CONFORMS_TO_IRI,
            profile,
        );

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches.get(&current_document_id),
            Some(&vec!["https://example.test/current".to_string()])
        );
    }
}
