//! Builds and rebuilds the IRI reference index that maps referenced IRIs back to documents.
//! Rows are keyed object first, so a backlink lookup is a seek, and each document keeps its
//! own rows, so a materialization replaces exactly its own postings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashSet};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{DOCUMENT_IRI_KEYSPACE, IRI_INDEX_KEYSPACE, prefix_upper_bound};
use aruna_core::metadata::{
    IriIndexRecord, MaterializationState, MaterializationStatusRecord, MetadataError,
};
use aruna_core::storage_entries::{iri_object_prefix, iri_reference_entry, iri_reference_prefix};
use aruna_core::structs::storage::data_link::document_key;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;

use super::data_links::{DataLinkError, LinkBudget, LinkRows, document_rows, iter_page};
use super::handle::REGISTRY_CANDIDATE_LIMIT;
use super::queue_storage::{
    MetadataQueueError, abort_storage_transaction, commit_storage_transaction,
    start_write_transaction,
};
use super::repository::{StorageReadError, parse_status_read, read_status_effect};

const INDEX_PAGE_SIZE: usize = 128;
const INDEX_BATCH_SIZE: usize = 128;

pub(crate) const DCTERMS_CONFORMS_IRI: &str = "http://purl.org/dc/terms/conformsTo";

/// Keeps the storage cause intact so callers can tell an overloaded backend
/// apart from a genuine index or document problem.
#[derive(Debug, Error)]
pub(crate) enum MetadataIriError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    DataLink(#[from] DataLinkError),
    #[error("unexpected metadata IRI reference index event: {0}")]
    UnexpectedEvent(String),
    #[error("metadata IRI backlink lookup exceeded its budget")]
    BudgetExhausted,
}

impl From<MetadataIriError> for MetadataError {
    fn from(error: MetadataIriError) -> Self {
        MetadataError::Backend(error.to_string())
    }
}

pub(crate) fn project_iri_references(
    document_id: Ulid,
    document_cursor: Ulid,
    references: Vec<(String, String, String)>,
) -> Vec<IriIndexRecord> {
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
            |((predicate_iri, object_iri), subject_iris)| IriIndexRecord {
                document_id,
                document_cursor,
                predicate_iri,
                object_iri,
                subject_iris: subject_iris.into_iter().collect(),
            },
        )
        .collect()
}

/// Replaces the document's IRI postings: writes the projection and deletes every older row
/// of the document that the projection does not write again.
pub(crate) async fn plan_iri_rows(
    storage: &StorageHandle,
    document_id: Ulid,
    records: &[IriIndexRecord],
) -> Result<LinkRows, MetadataIriError> {
    let mut rows = LinkRows::default();
    let mut written = HashSet::new();
    for record in records {
        let (key_space, key, value) = iri_reference_entry(record)?;
        rows.writes.push((
            DOCUMENT_IRI_KEYSPACE.to_string(),
            document_key(document_id, &key).into(),
            ByteView::from(Vec::new()),
        ));
        written.insert(key.to_vec());
        rows.writes.push((key_space, key, value));
    }
    for key in document_rows(storage, DOCUMENT_IRI_KEYSPACE, document_id).await? {
        if !written.contains(&key) {
            let document_row = document_key(document_id, &key).into();
            rows.deletes
                .push((IRI_INDEX_KEYSPACE.to_string(), key.into()));
            rows.deletes
                .push((DOCUMENT_IRI_KEYSPACE.to_string(), document_row));
        }
    }
    Ok(rows)
}

/// One page of the postings that name an object: every row of at most `limit` documents
/// after the requested one, by document id. `next` is the last document when more may follow.
#[derive(Debug, Default)]
pub(crate) struct BacklinkPage {
    pub documents: BTreeMap<Ulid, Vec<IriIndexRecord>>,
    pub next: Option<Ulid>,
}

/// Reads one page of the postings that name `object_iri`. Without a predicate, each
/// predicate group under the object is found by one seek and read from `after` on.
pub(crate) async fn read_backlinks(
    storage: &StorageHandle,
    object_iri: &str,
    predicate_iri: Option<&str>,
    after: Option<Ulid>,
    limit: usize,
    budget: &mut LinkBudget,
) -> Result<BacklinkPage, MetadataIriError> {
    let limit = limit.max(1);
    let mut documents = BTreeMap::<Ulid, Vec<IriIndexRecord>>::new();
    let mut truncated = false;
    let group = GroupRead {
        object_iri,
        predicate_iri,
        after,
        limit,
    };
    match predicate_iri {
        Some(predicate) => {
            let prefix = iri_reference_prefix(predicate, object_iri);
            truncated = group.read(storage, prefix, budget, &mut documents).await?;
        }
        None => {
            let object = iri_object_prefix(object_iri);
            let mut start = IterStart::At(object.clone());
            loop {
                if !budget.take_seek() {
                    return Err(MetadataIriError::BudgetExhausted);
                }
                let (values, _) = iter_page(
                    storage,
                    IRI_INDEX_KEYSPACE,
                    Some(object.clone()),
                    Some(start),
                    1,
                )
                .await?;
                let Some(prefix) = values.first().and_then(|(key, _)| key.get(..64)) else {
                    break;
                };
                let next = prefix_upper_bound(prefix);
                let prefix = ByteView::from(prefix.to_vec());
                truncated |= group.read(storage, prefix, budget, &mut documents).await?;
                let Some(next) = next else {
                    break;
                };
                start = IterStart::At(ByteView::from(next));
            }
        }
    }
    let more = truncated || documents.len() > limit;
    let documents: BTreeMap<_, _> = documents.into_iter().take(limit).collect();
    let next = if more {
        documents.keys().next_back().copied()
    } else {
        None
    };
    Ok(BacklinkPage { documents, next })
}

/// One object and predicate group of a backlink page.
struct GroupRead<'a> {
    object_iri: &'a str,
    predicate_iri: Option<&'a str>,
    after: Option<Ulid>,
    limit: usize,
}

impl GroupRead<'_> {
    /// Reads up to `limit + 1` rows of the group after the page start; `true` when more
    /// rows may follow. Hash collisions are dropped by comparing the stored IRIs.
    async fn read(
        &self,
        storage: &StorageHandle,
        prefix: ByteView,
        budget: &mut LinkBudget,
        documents: &mut BTreeMap<Ulid, Vec<IriIndexRecord>>,
    ) -> Result<bool, MetadataIriError> {
        if !budget.take_seek() {
            return Err(MetadataIriError::BudgetExhausted);
        }
        let start = self
            .after
            .map(|document_id| IterStart::After(ByteView::from(group_key(&prefix, document_id))));
        let (values, next) = iter_page(
            storage,
            IRI_INDEX_KEYSPACE,
            Some(prefix),
            start,
            self.limit.saturating_add(1),
        )
        .await?;
        budget.rows = budget
            .rows
            .checked_sub(values.len())
            .ok_or(MetadataIriError::BudgetExhausted)?;
        for (_, value) in values {
            let record =
                postcard::from_bytes::<IriIndexRecord>(&value).map_err(ConversionError::from)?;
            if record.object_iri != self.object_iri
                || self
                    .predicate_iri
                    .is_some_and(|predicate| record.predicate_iri != predicate)
            {
                continue;
            }
            documents
                .entry(record.document_id)
                .or_default()
                .push(record);
        }
        Ok(next.is_some())
    }
}

fn group_key(prefix: &[u8], document_id: Ulid) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&document_id.to_bytes());
    key
}

pub(crate) async fn lookup_iri_references(
    storage: &StorageHandle,
    registry_records: &[MetadataRegistryRecord],
    predicate_iri: &str,
    object_iri: &str,
) -> Result<BTreeMap<Ulid, Vec<String>>, MetadataError> {
    // The registry holds at most this many documents, so more postings mean a broken index.
    let mut budget = LinkBudget {
        seeks: REGISTRY_CANDIDATE_LIMIT,
        rows: REGISTRY_CANDIDATE_LIMIT.saturating_mul(2),
    };
    let mut records = Vec::new();
    let mut after = None;
    loop {
        let page = read_backlinks(
            storage,
            object_iri,
            Some(predicate_iri),
            after,
            INDEX_PAGE_SIZE,
            &mut budget,
        )
        .await?;
        records.extend(page.documents.into_values().flatten());
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let registry_records = super::api::filter_live_records(storage, registry_records)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    Ok(collect_iri_matches(
        records,
        &registry_records,
        predicate_iri,
        object_iri,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IriFreshnessState {
    Current,
    Pending,
    Failed,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IriFreshness {
    pub state: IriFreshnessState,
    pub oldest_status_updated: Option<u64>,
}

/// Materialization state of the given documents, one status read each; callers pass the
/// documents a lookup returned, never the whole registry.
pub(crate) async fn iri_index_freshness(
    storage: &StorageHandle,
    registry_records: &[MetadataRegistryRecord],
) -> Result<IriFreshness, MetadataError> {
    let mut current = false;
    let mut pending = false;
    let mut failed = false;
    let mut oldest_status_updated = None;
    for record in registry_records {
        let status = read_materialization_status(storage, record.document_id).await?;
        if let Some(status) = status.as_ref() {
            oldest_status_updated = Some(
                oldest_status_updated.map_or(status.updated_at_ms, |oldest: u64| {
                    oldest.min(status.updated_at_ms)
                }),
            );
        }
        match status {
            Some(status) if status_current(&status, record) => current = true,
            Some(status) if status.state == MaterializationState::Failed => {
                failed = true;
            }
            Some(_) | None => pending = true,
        }
    }
    let kinds = usize::from(current) + usize::from(pending) + usize::from(failed);
    let state = if kinds > 1 {
        IriFreshnessState::Mixed
    } else if failed {
        IriFreshnessState::Failed
    } else if pending {
        IriFreshnessState::Pending
    } else {
        IriFreshnessState::Current
    };
    Ok(IriFreshness {
        state,
        oldest_status_updated,
    })
}

fn status_current(status: &MaterializationStatusRecord, record: &MetadataRegistryRecord) -> bool {
    status.event_id == record.last_event_id
        && status.graph_iri == record.graph_iri
        && status.state == MaterializationState::Materialized
}

/// Re-projects each live materialized document on its own and replaces its postings.
pub(crate) async fn rebuild_index(context: &DriverContext) -> Result<usize, MetadataError> {
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataError::HandleMissing)?;
    let storage = &context.storage_handle;
    let registry_records = metadata_handle.list_cached_records().await?;
    let registry_records = super::api::filter_live_records(storage, registry_records.as_ref())
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    let mut written = 0usize;

    for record in registry_records.iter() {
        let Some(status) = read_materialization_status(storage, record.document_id).await? else {
            continue;
        };
        if !status_current(&status, record) {
            continue;
        }

        let references = metadata_handle
            .snapshot_iri_references(record.graph_iri.clone())
            .await?;
        let mut links =
            super::data_links::project_links(&record.graph_iri, record.last_event_id, &references);
        super::data_links::associate_versions(storage, &mut links).await?;
        let mut rows =
            super::data_links::plan_link_rows(storage, record.document_id, &links).await?;
        let projected =
            project_iri_references(record.document_id, record.last_event_id, references);
        let iri_rows = plan_iri_rows(storage, record.document_id, &projected).await?;
        rows.writes.extend(iri_rows.writes);
        rows.deletes.extend(iri_rows.deletes);
        if replace_rows(storage, record, rows).await? {
            written = written.saturating_add(projected.len());
        }
    }
    Ok(written)
}

/// Commits one document's rows only while its status still names the projected event, so a
/// newer materialization is never overwritten; `false` when one got there first.
async fn replace_rows(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
    rows: LinkRows,
) -> Result<bool, MetadataError> {
    let backend = |error: MetadataQueueError| MetadataError::Backend(error.to_string());
    let txn_id = start_write_transaction(storage).await.map_err(backend)?;
    let result = async {
        let status = parse_status_read(
            storage
                .send_effect(read_status_effect(record.document_id, Some(txn_id)))
                .await,
        )
        .map_err(status_error)?;
        if !status.is_some_and(|status| status_current(&status, record)) {
            return Ok(false);
        }
        if !rows.writes.is_empty() {
            expect_batch(
                storage
                    .send_storage_effect(StorageEffect::BatchWrite {
                        writes: rows.writes,
                        txn_id: Some(txn_id),
                    })
                    .await,
            )?;
        }
        if !rows.deletes.is_empty() {
            expect_batch(
                storage
                    .send_storage_effect(StorageEffect::BatchDelete {
                        deletes: rows.deletes,
                        txn_id: Some(txn_id),
                    })
                    .await,
            )?;
        }
        Ok::<_, MetadataIriError>(true)
    }
    .await;
    match result {
        Ok(true) => match commit_storage_transaction(storage, txn_id).await {
            Ok(()) => Ok(true),
            Err(MetadataQueueError::Storage(StorageError::TransactionConflict)) => Ok(false),
            Err(error) => Err(backend(error)),
        },
        other => {
            abort_storage_transaction(
                storage,
                txn_id,
                "Failed to abort IRI index rebuild transaction",
                "Unexpected IRI index rebuild transaction abort result",
            )
            .await;
            Ok(other?)
        }
    }
}

fn expect_batch(event: Event) -> Result<(), MetadataIriError> {
    match event {
        Event::Storage(StorageEvent::BatchWriteResult { .. })
        | Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataIriError::UnexpectedEvent(format!("{other:?}"))),
    }
}

/// Postings and per-document rows of one document, from its own rows. Each posting comes
/// before its per-document row, so an interrupted deletion never orphans a posting.
async fn document_keys(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<Vec<(String, ByteView)>, MetadataIriError> {
    let mut keys = Vec::new();
    for key in document_rows(storage, DOCUMENT_IRI_KEYSPACE, document_id).await? {
        let document_row = ByteView::from(document_key(document_id, &key));
        keys.push((IRI_INDEX_KEYSPACE.to_string(), ByteView::from(key)));
        keys.push((DOCUMENT_IRI_KEYSPACE.to_string(), document_row));
    }
    Ok(keys)
}

/// Keys for the given documents, read through each document's own rows, so a deletion
/// prunes its postings without touching any other document.
pub(crate) async fn document_iri_keys(
    storage: &StorageHandle,
    document_ids: &BTreeSet<Ulid>,
) -> Result<Vec<(String, ByteView)>, MetadataError> {
    let mut keys = Vec::new();
    for document_id in document_ids {
        keys.extend(document_keys(storage, *document_id).await?);
    }
    Ok(keys)
}

pub(crate) async fn delete_keys(
    storage: &StorageHandle,
    keys: Vec<(String, ByteView)>,
) -> Result<(), MetadataError> {
    for chunk in keys.chunks(INDEX_BATCH_SIZE) {
        expect_batch(
            storage
                .send_storage_effect(StorageEffect::BatchDelete {
                    deletes: chunk.to_vec(),
                    txn_id: None,
                })
                .await,
        )?;
    }
    Ok(())
}

fn collect_iri_matches(
    records: Vec<IriIndexRecord>,
    registry_records: &[MetadataRegistryRecord],
    predicate_iri: &str,
    object_iri: &str,
) -> BTreeMap<Ulid, Vec<String>> {
    let current_cursors = registry_records
        .iter()
        .map(|record| (record.document_id, record.last_event_id))
        .collect::<std::collections::HashMap<_, _>>();
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

fn status_error(error: StorageReadError) -> MetadataIriError {
    match error {
        StorageReadError::Storage(error) => error.into(),
        StorageReadError::Conversion(error) => error.into(),
    }
}

async fn read_materialization_status(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<Option<MaterializationStatusRecord>, MetadataIriError> {
    let event = storage
        .send_effect(read_status_effect(document_id, None))
        .await;
    parse_status_read(event).map_err(status_error)
}

#[cfg(test)]
mod tests {
    use super::super::data_links::apply_rows;
    use super::*;
    use aruna_core::storage_entries::{iri_reference_key, materialization_status_entry};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_storage::FjallStorage;

    const OBJECT: &str = "https://example.test/object";

    fn record(document_id: Ulid, cursor: Ulid, predicate: &str, object: &str) -> IriIndexRecord {
        IriIndexRecord {
            document_id,
            document_cursor: cursor,
            predicate_iri: predicate.to_string(),
            object_iri: object.to_string(),
            subject_iris: vec!["https://example.test/root".to_string()],
        }
    }

    async fn write_document(
        storage: &StorageHandle,
        document_id: Ulid,
        records: &[IriIndexRecord],
    ) {
        let rows = plan_iri_rows(storage, document_id, records).await.unwrap();
        apply_rows(storage, rows).await.unwrap();
    }

    async fn row_exists(storage: &StorageHandle, key_space: &str, key: Vec<u8>) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    fn open_budget() -> LinkBudget {
        LinkBudget {
            seeks: 64,
            rows: 1024,
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
            establishing_event_id: cursor,
            last_event_id: cursor,
        }
    }

    #[test]
    fn groups_sorts_subjects() {
        let document_id = Ulid::from_parts(1, 1);
        let cursor = Ulid::from_parts(2, 1);
        let records = project_iri_references(
            document_id,
            cursor,
            vec![
                (
                    "https://example.test/subject-b".to_string(),
                    DCTERMS_CONFORMS_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
                (
                    "https://example.test/subject-a".to_string(),
                    DCTERMS_CONFORMS_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
                (
                    "https://example.test/subject-a".to_string(),
                    DCTERMS_CONFORMS_IRI.to_string(),
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
    fn lookup_fences_exact() {
        let current_document_id = Ulid::from_parts(1, 1);
        let stale_document_id = Ulid::from_parts(1, 2);
        let cursor = Ulid::from_parts(2, 1);
        let profile = "https://example.test/profile";
        let records = vec![
            IriIndexRecord {
                document_id: current_document_id,
                document_cursor: cursor,
                predicate_iri: DCTERMS_CONFORMS_IRI.to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/current".to_string()],
            },
            IriIndexRecord {
                document_id: stale_document_id,
                document_cursor: Ulid::from_parts(2, 0),
                predicate_iri: DCTERMS_CONFORMS_IRI.to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/stale".to_string()],
            },
            IriIndexRecord {
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

        let matches =
            collect_iri_matches(records, &registry_records, DCTERMS_CONFORMS_IRI, profile);

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches.get(&current_document_id),
            Some(&vec!["https://example.test/current".to_string()])
        );
    }

    #[tokio::test]
    async fn replaces_old_postings() {
        // A new revision deletes the rows it does not write again, with their document rows.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let doc = Ulid::from_parts(5, 1);
        let first = Ulid::from_parts(1, 1);
        let second = Ulid::from_parts(2, 1);
        write_document(
            &storage,
            doc,
            &[record(doc, first, "p", "o1"), record(doc, first, "p", "o2")],
        )
        .await;
        write_document(&storage, doc, &[record(doc, second, "p", "o2")]).await;

        let dropped = iri_reference_key("p", "o1", doc).to_vec();
        let kept = iri_reference_key("p", "o2", doc).to_vec();
        assert!(!row_exists(&storage, IRI_INDEX_KEYSPACE, dropped.clone()).await);
        assert!(!row_exists(&storage, DOCUMENT_IRI_KEYSPACE, document_key(doc, &dropped)).await);
        assert!(row_exists(&storage, DOCUMENT_IRI_KEYSPACE, document_key(doc, &kept)).await);
        let page = read_backlinks(&storage, "o2", None, None, 10, &mut open_budget())
            .await
            .unwrap();
        assert_eq!(page.documents[&doc][0].document_cursor, second);
    }

    #[tokio::test]
    async fn reads_object_prefix() {
        // More than a registry's worth of rows under other objects: a predicate-less lookup
        // still costs a few seeks and only the object's own rows.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(1, 1);
        let noise: Vec<_> = (0..1100u64)
            .map(|index| {
                let doc = Ulid::from_parts(100, index.into());
                record(
                    doc,
                    cursor,
                    "p",
                    &format!("https://example.test/other/{index}"),
                )
            })
            .collect();
        let rows = LinkRows {
            writes: noise
                .iter()
                .map(|record| iri_reference_entry(record).unwrap())
                .collect(),
            deletes: Vec::new(),
        };
        apply_rows(&storage, rows).await.unwrap();
        let doc = Ulid::from_parts(7, 1);
        write_document(
            &storage,
            doc,
            &[
                record(doc, cursor, "p1", OBJECT),
                record(doc, cursor, "p2", OBJECT),
            ],
        )
        .await;

        let mut budget = LinkBudget { seeks: 5, rows: 4 };
        let page = read_backlinks(&storage, OBJECT, None, None, 10, &mut budget)
            .await
            .unwrap();

        assert_eq!(page.documents.len(), 1);
        assert_eq!(page.documents[&doc].len(), 2);
        assert_eq!(page.next, None);
        assert_eq!(budget.rows, 2);
    }

    #[tokio::test]
    async fn pages_after_document() {
        // Documents come in id order across predicates; `next` continues after the last one.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(1, 1);
        let docs: Vec<_> = (1..=5u128)
            .map(|index| Ulid::from_parts(9, index))
            .collect();
        for (index, doc) in docs.iter().enumerate() {
            let predicate = if index % 2 == 0 { "p1" } else { "p2" };
            let mut records = vec![record(*doc, cursor, predicate, OBJECT)];
            if index == 2 {
                records.push(record(*doc, cursor, "p2", OBJECT));
            }
            write_document(&storage, *doc, &records).await;
        }

        let mut budget = open_budget();
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let page = read_backlinks(&storage, OBJECT, None, after, 2, &mut budget)
                .await
                .unwrap();
            assert!(page.documents.len() <= 2);
            seen.extend(page.documents.keys().copied());
            if let Some(both) = page.documents.get(&docs[2]) {
                assert_eq!(both.len(), 2);
            }
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, docs);

        let filtered = read_backlinks(&storage, OBJECT, Some("p2"), None, 10, &mut budget)
            .await
            .unwrap();
        let ids: Vec<_> = filtered.documents.keys().copied().collect();
        assert_eq!(ids, vec![docs[1], docs[2], docs[3]]);
    }

    #[tokio::test]
    async fn budget_ends_lookup() {
        // An exhausted budget fails the lookup instead of returning a partial page.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let doc = Ulid::from_parts(3, 1);
        let cursor = Ulid::from_parts(1, 1);
        write_document(&storage, doc, &[record(doc, cursor, "p", OBJECT)]).await;

        let mut budget = LinkBudget { seeks: 1, rows: 8 };
        let error = read_backlinks(&storage, OBJECT, None, None, 10, &mut budget)
            .await
            .expect_err("two seeks are needed");
        assert!(matches!(error, MetadataIriError::BudgetExhausted));
    }

    #[tokio::test]
    async fn decode_error_classified() {
        // A corrupt row must stay a conversion failure: callers charge those to
        // the document, while storage failures must never spend that budget.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let key = iri_reference_key("p", OBJECT, Ulid::from_parts(1, 1));
        let rows = LinkRows {
            writes: vec![(
                IRI_INDEX_KEYSPACE.to_string(),
                key,
                ByteView::from(vec![1u8]),
            )],
            deletes: Vec::new(),
        };
        apply_rows(&storage, rows).await.unwrap();

        let error = read_backlinks(&storage, OBJECT, None, None, 10, &mut open_budget())
            .await
            .expect_err("malformed row fails to decode");
        assert!(matches!(error, MetadataIriError::Conversion(_)));
    }

    #[tokio::test]
    async fn deletes_document_rows() {
        // The deletion path reads only the deleted document's own rows.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let deleted = Ulid::from_parts(1, 1);
        let kept = Ulid::from_parts(2, 1);
        let cursor = Ulid::from_parts(1, 1);
        write_document(
            &storage,
            deleted,
            &[
                record(deleted, cursor, "p", "o1"),
                record(deleted, cursor, "p", "o2"),
            ],
        )
        .await;
        write_document(&storage, kept, &[record(kept, cursor, "p", "o1")]).await;

        let keys = document_iri_keys(&storage, &BTreeSet::from([deleted]))
            .await
            .unwrap();
        assert_eq!(keys.len(), 4);
        delete_keys(&storage, keys).await.unwrap();

        let page = read_backlinks(&storage, "o1", None, None, 10, &mut open_budget())
            .await
            .unwrap();
        let ids: Vec<_> = page.documents.keys().copied().collect();
        assert_eq!(ids, vec![kept]);
        let kept_key = iri_reference_key("p", "o1", kept).to_vec();
        assert!(
            row_exists(
                &storage,
                DOCUMENT_IRI_KEYSPACE,
                document_key(kept, &kept_key)
            )
            .await
        );
    }

    #[tokio::test]
    async fn freshness_of_documents() {
        // Only the documents passed in count; a pending one elsewhere does not.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(1, 1);
        let current = registry_record(Ulid::from_parts(4, 1), cursor);
        let pending = registry_record(Ulid::from_parts(4, 2), cursor);
        let status = |record: &MetadataRegistryRecord, state| MaterializationStatusRecord {
            document_id: record.document_id,
            event_id: cursor,
            graph_iri: record.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
            state,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 5,
        };
        let rows = LinkRows {
            writes: vec![
                materialization_status_entry(&status(&current, MaterializationState::Materialized))
                    .unwrap(),
                materialization_status_entry(&status(&pending, MaterializationState::Pending))
                    .unwrap(),
            ],
            deletes: Vec::new(),
        };
        apply_rows(&storage, rows).await.unwrap();

        let returned = iri_index_freshness(&storage, std::slice::from_ref(&current))
            .await
            .unwrap();
        assert_eq!(returned.state, IriFreshnessState::Current);
        assert_eq!(returned.oldest_status_updated, Some(5));
        let both = iri_index_freshness(&storage, &[current, pending])
            .await
            .unwrap();
        assert_eq!(both.state, IriFreshnessState::Mixed);
    }

    #[tokio::test]
    async fn rebuild_skips_newer() {
        // A rebuild commits a document's rows only while its status still names the event.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(1, 1);
        let record_at = registry_record(Ulid::from_parts(6, 1), cursor);
        let doc = record_at.document_id;
        let rows = plan_iri_rows(&storage, doc, &[record(doc, cursor, "p", OBJECT)])
            .await
            .unwrap();

        assert!(!replace_rows(&storage, &record_at, rows).await.unwrap());
        let key = iri_reference_key("p", OBJECT, doc).to_vec();
        assert!(!row_exists(&storage, IRI_INDEX_KEYSPACE, key.clone()).await);

        let status = MaterializationStatusRecord {
            document_id: doc,
            event_id: cursor,
            graph_iri: record_at.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
            state: MaterializationState::Materialized,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        };
        let status_rows = LinkRows {
            writes: vec![materialization_status_entry(&status).unwrap()],
            deletes: Vec::new(),
        };
        apply_rows(&storage, status_rows).await.unwrap();
        let rows = plan_iri_rows(&storage, doc, &[record(doc, cursor, "p", OBJECT)])
            .await
            .unwrap();
        assert!(replace_rows(&storage, &record_at, rows).await.unwrap());
        assert!(row_exists(&storage, IRI_INDEX_KEYSPACE, key).await);
    }
}
