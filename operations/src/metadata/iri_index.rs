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
    metadata_iri_reference_key_ids, metadata_iri_reference_prefix,
    metadata_iri_reference_write_entry, metadata_materialization_status_key,
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
    let records = scan_iri_reference_records(storage, Some(prefix)).await?;
    Ok(collect_matching_iri_references(
        records,
        registry_records,
        predicate_iri,
        object_iri,
    ))
}

/// One referencing document's link to a scanned object, at one predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IriBacklink {
    pub document_id: Ulid,
    pub predicate_iri: String,
    pub subject_iris: Vec<String>,
}

/// Backlinks to `object_iri`. A `predicate_iri` uses the predicate-first index
/// prefix; without it the keyspace is scanned and filtered on the object, since
/// the object is not a key prefix. Both fence stale document cursors.
pub(crate) async fn lookup_iri_backlinks(
    storage: &StorageHandle,
    registry_records: &[MetadataRegistryRecord],
    object_iri: &str,
    predicate_iri: Option<&str>,
) -> Result<Vec<IriBacklink>, MetadataError> {
    let prefix =
        predicate_iri.map(|predicate| metadata_iri_reference_prefix(predicate, object_iri));
    let records = scan_iri_reference_records(storage, prefix).await?;
    Ok(collect_iri_backlinks(
        records,
        registry_records,
        object_iri,
        predicate_iri,
    ))
}

async fn scan_iri_reference_records(
    storage: &StorageHandle,
    prefix: Option<ByteView>,
) -> Result<Vec<MetadataIriReferenceIndexRecord>, MetadataError> {
    let mut start_after = None;
    let mut records = Vec::new();
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
                prefix: prefix.clone(),
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
    Ok(records)
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
    let mut reprojected: HashMap<Ulid, Ulid> = HashMap::new();

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
        reprojected.insert(record.document_id, record.last_event_id);
        written = written.saturating_add(projected.len());
    }

    // Re-read the registry so cleanup keys deletion on the current cursor: a
    // materialization that advanced a document mid-rebuild keeps its fresh rows,
    // while rows for documents gone from the registry are pruned.
    let registered = metadata_handle
        .list_cached_registry_records()
        .await?
        .iter()
        .map(|record| (record.document_id, record.last_event_id))
        .collect::<HashMap<_, _>>();
    let stale =
        stale_rebuild_iri_reference_keys(&context.storage_handle, &reprojected, &registered)
            .await?;
    delete_iri_reference_keys(&context.storage_handle, stale).await?;

    Ok(written)
}

/// Scans the IRI reference index and returns the keyspace-qualified keys for
/// which `select` accepts the row's document id and cursor.
async fn select_iri_reference_keys<F>(
    storage: &StorageHandle,
    txn_id: Option<Ulid>,
    mut select: F,
) -> Result<Vec<(String, ByteView)>, MetadataError>
where
    F: FnMut(Ulid, Ulid) -> bool,
{
    let mut start_after = None;
    let mut selected = Vec::new();
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: IRI_INDEX_PAGE_SIZE,
                txn_id,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataError::Backend(error.to_string()));
            }
            other => {
                return Err(MetadataError::Backend(format!(
                    "unexpected metadata IRI reference index event: {other:?}"
                )));
            }
        };
        for (key, _) in values {
            if let Some((document_id, cursor)) = metadata_iri_reference_key_ids(key.as_ref())
                && select(document_id, cursor)
            {
                selected.push((METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(), key));
            }
        }
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    Ok(selected)
}

/// Keys of index rows whose document is re-projected under a newer cursor.
/// Superseded rows are already fenced from results but bloat storage and the
/// predicate-less scan, so callers delete them alongside the current projection.
pub(crate) async fn superseded_iri_reference_keys(
    storage: &StorageHandle,
    txn_id: Option<Ulid>,
    current_cursors: &HashMap<Ulid, Ulid>,
) -> Result<Vec<(String, ByteView)>, MetadataError> {
    select_iri_reference_keys(storage, txn_id, |document_id, cursor| {
        current_cursors
            .get(&document_id)
            .is_some_and(|current| *current != cursor)
    })
    .await
}

/// Keys the full rebuild must drop. A row survives only at the cursor the
/// rebuild just projected or the document's current registry cursor, so a
/// materialization that raced ahead keeps its rows; rows for documents in
/// neither map (deleted or reverted) are pruned.
async fn stale_rebuild_iri_reference_keys(
    storage: &StorageHandle,
    reprojected: &HashMap<Ulid, Ulid>,
    registered: &HashMap<Ulid, Ulid>,
) -> Result<Vec<(String, ByteView)>, MetadataError> {
    select_iri_reference_keys(storage, None, |document_id, cursor| {
        reprojected.get(&document_id) != Some(&cursor)
            && registered.get(&document_id) != Some(&cursor)
    })
    .await
}

/// Keys for the given documents at every cursor, so a deletion can prune the
/// rows it would otherwise leak into the predicate-less backlink scan.
pub(crate) async fn document_iri_reference_keys(
    storage: &StorageHandle,
    document_ids: &BTreeSet<Ulid>,
) -> Result<Vec<(String, ByteView)>, MetadataError> {
    select_iri_reference_keys(storage, None, |document_id, _| {
        document_ids.contains(&document_id)
    })
    .await
}

pub(crate) async fn delete_iri_reference_keys(
    storage: &StorageHandle,
    keys: Vec<(String, ByteView)>,
) -> Result<(), MetadataError> {
    for chunk in keys.chunks(IRI_INDEX_WRITE_BATCH_SIZE) {
        match storage
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes: chunk.to_vec(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataError::Backend(error.to_string()));
            }
            other => {
                return Err(MetadataError::Backend(format!(
                    "unexpected metadata IRI reference index delete event: {other:?}"
                )));
            }
        }
    }
    Ok(())
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

fn collect_iri_backlinks(
    records: Vec<MetadataIriReferenceIndexRecord>,
    registry_records: &[MetadataRegistryRecord],
    object_iri: &str,
    predicate_iri: Option<&str>,
) -> Vec<IriBacklink> {
    let current_cursors = registry_records
        .iter()
        .map(|record| (record.document_id, record.last_event_id))
        .collect::<HashMap<_, _>>();
    let mut grouped = BTreeMap::<(Ulid, String), BTreeSet<String>>::new();
    for record in records {
        if record.object_iri != object_iri
            || predicate_iri.is_some_and(|predicate| record.predicate_iri != predicate)
            || current_cursors.get(&record.document_id) != Some(&record.document_cursor)
        {
            continue;
        }
        grouped
            .entry((record.document_id, record.predicate_iri))
            .or_default()
            .extend(record.subject_iris);
    }
    grouped
        .into_iter()
        .map(|((document_id, predicate_iri), subject_iris)| IriBacklink {
            document_id,
            predicate_iri,
            subject_iris: subject_iris.into_iter().collect(),
        })
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
    use aruna_core::storage_entries::metadata_iri_reference_key;
    use aruna_core::structs::{PlacementRef, RealmId};
    use aruna_storage::FjallStorage;

    fn index_key(document_id: Ulid, cursor: Ulid) -> ByteView {
        metadata_iri_reference_key("p", "o", document_id, cursor)
    }

    async fn write_index_row(storage: &StorageHandle, document_id: Ulid, cursor: Ulid) {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![(
                    METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
                    index_key(document_id, cursor),
                    ByteView::from(vec![1u8]),
                )],
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }

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

    #[test]
    fn backlinks_by_predicate() {
        // Object match and cursor fence hold across predicates; a predicate
        // filter narrows the grouped backlinks.
        let document_id = Ulid::from_parts(1, 1);
        let cursor = Ulid::from_parts(2, 1);
        let object = "https://example.test/object";
        let records = vec![
            MetadataIriReferenceIndexRecord {
                document_id,
                document_cursor: cursor,
                predicate_iri: "https://example.test/license".to_string(),
                object_iri: object.to_string(),
                subject_iris: vec!["https://example.test/root".to_string()],
            },
            MetadataIriReferenceIndexRecord {
                document_id,
                document_cursor: cursor,
                predicate_iri: "https://example.test/based-on".to_string(),
                object_iri: object.to_string(),
                subject_iris: vec!["https://example.test/root".to_string()],
            },
            MetadataIriReferenceIndexRecord {
                document_id,
                document_cursor: Ulid::from_parts(2, 0),
                predicate_iri: "https://example.test/license".to_string(),
                object_iri: object.to_string(),
                subject_iris: vec!["https://example.test/stale".to_string()],
            },
            MetadataIriReferenceIndexRecord {
                document_id,
                document_cursor: cursor,
                predicate_iri: "https://example.test/license".to_string(),
                object_iri: "https://example.test/other".to_string(),
                subject_iris: vec!["https://example.test/miss".to_string()],
            },
        ];
        let registry_records = vec![registry_record(document_id, cursor)];

        let all = collect_iri_backlinks(records.clone(), &registry_records, object, None);
        assert_eq!(all.len(), 2);

        let filtered = collect_iri_backlinks(
            records,
            &registry_records,
            object,
            Some("https://example.test/license"),
        );
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].predicate_iri, "https://example.test/license");
        assert_eq!(
            filtered[0].subject_iris,
            vec!["https://example.test/root".to_string()]
        );
    }

    #[tokio::test]
    async fn keeps_raced_cursor() {
        // A cursor a concurrent materialization advanced past the reprojected one
        // survives cleanup; only genuinely older rows are dropped.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let doc = Ulid::from_parts(5, 1);
        let ancient = Ulid::from_parts(1, 1);
        let reprojected_cursor = Ulid::from_parts(2, 1);
        let raced = Ulid::from_parts(3, 1);
        for cursor in [ancient, reprojected_cursor, raced] {
            write_index_row(&storage, doc, cursor).await;
        }
        let reprojected = HashMap::from([(doc, reprojected_cursor)]);
        let registered = HashMap::from([(doc, raced)]);

        let stale = stale_rebuild_iri_reference_keys(&storage, &reprojected, &registered)
            .await
            .unwrap();
        let keys: Vec<ByteView> = stale.into_iter().map(|(_, key)| key).collect();
        assert_eq!(keys, vec![index_key(doc, ancient)]);
    }

    #[tokio::test]
    async fn prunes_deleted_docs() {
        // A document gone from the registry has all its rows marked stale.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let live = Ulid::from_parts(1, 1);
        let gone = Ulid::from_parts(2, 1);
        let cursor = Ulid::from_parts(3, 1);
        write_index_row(&storage, live, cursor).await;
        write_index_row(&storage, gone, cursor).await;
        let reprojected = HashMap::from([(live, cursor)]);
        let registered = HashMap::from([(live, cursor)]);

        let stale = stale_rebuild_iri_reference_keys(&storage, &reprojected, &registered)
            .await
            .unwrap();
        let keys: Vec<ByteView> = stale.into_iter().map(|(_, key)| key).collect();
        assert_eq!(keys, vec![index_key(gone, cursor)]);
    }

    #[tokio::test]
    async fn deletes_document_rows() {
        // The deletion path collects every cursor's rows for the given documents.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let deleted = Ulid::from_parts(1, 1);
        let kept = Ulid::from_parts(2, 1);
        let first = Ulid::from_parts(1, 1);
        let second = Ulid::from_parts(2, 1);
        write_index_row(&storage, deleted, first).await;
        write_index_row(&storage, deleted, second).await;
        write_index_row(&storage, kept, first).await;
        let targets = BTreeSet::from([deleted]);

        let keys = document_iri_reference_keys(&storage, &targets)
            .await
            .unwrap();
        let keys: Vec<ByteView> = keys.into_iter().map(|(_, key)| key).collect();
        assert!(keys.contains(&index_key(deleted, first)));
        assert!(keys.contains(&index_key(deleted, second)));
        assert!(!keys.contains(&index_key(kept, first)));
    }
}
