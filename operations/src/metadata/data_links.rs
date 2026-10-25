//! Keeps the node-local data link postings of materialized documents: identifier first, so a
//! lookup is a seek, and per document, so a materialization replaces exactly its own rows.
//! Prefix lookups probe only the prefix lengths a bucket's postings use.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashSet};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DATA_LINK_KEYSPACE, DEAD_LETTER_KEYSPACE, DISTRIBUTION_KEYSPACE, DOCUMENT_LINK_KEYSPACE,
    MATERIALIZATION_JOB_KEYSPACE, S3_BUCKET_KEYSPACE, VERSION_INDEX_KEYSPACE,
};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::data_link::{
    DataLink, LinkPosting, LinkScope, MAX_DOCUMENT_LINKS, MAX_LINK_SUBJECTS, document_key,
    posting_document, posting_key,
};
use aruna_core::structs::storage::data_link::{set_versions_key, unverified_key};
use aruna_core::structs::storage::distribution::inventory::index_key;
use aruna_core::structs::storage::distribution::{DistributionView, SetHint};
use aruna_core::structs::storage::global_id::MAX_PREFIX_BYTES;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

const PAGE_SIZE: usize = 256;
const BATCH_SIZE: usize = 256;
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SCHEMA_BASES: [&str; 2] = ["http://schema.org/", "https://schema.org/"];

#[derive(Debug, Error)]
pub(crate) enum DataLinkError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected data link storage event: {0}")]
    UnexpectedEvent(String),
}

impl From<DataLinkError> for MetadataError {
    fn from(error: DataLinkError) -> Self {
        MetadataError::Backend(error.to_string())
    }
}

/// The rows one materialization writes and the stale rows of the document it deletes.
#[derive(Debug, Default)]
pub(crate) struct LinkRows {
    pub writes: Vec<(String, ByteView, ByteView)>,
    pub deletes: Vec<(String, ByteView)>,
}

fn schema_term(iri: &str, term: &str) -> bool {
    SCHEMA_BASES
        .iter()
        .any(|base| iri.strip_prefix(base) == Some(term))
}

/// The links a document's references name, from `(subject, predicate, object)` triples. A
/// Dataset-typed `hasPart` of the root is a linked part; the root is the graph IRI or the
/// entity the metadata descriptor is `about`.
pub(crate) fn project_links(
    graph_iri: &str,
    cursor: Ulid,
    references: &[(String, String, String)],
) -> BTreeMap<DataLink, LinkPosting> {
    let mut roots = BTreeSet::from([graph_iri.to_string()]);
    let mut datasets = HashSet::new();
    for (subject, predicate, object) in references {
        if schema_term(predicate, "about") && subject.ends_with("ro-crate-metadata.json") {
            roots.insert(object.clone());
        }
        if predicate == RDF_TYPE && schema_term(object, "Dataset") {
            datasets.insert(subject.as_str());
        }
    }
    let mut links = BTreeMap::<DataLink, LinkPosting>::new();
    for (subject, predicate, object) in references {
        let Some(link) = DataLink::parse(object) else {
            continue;
        };
        if links.len() >= MAX_DOCUMENT_LINKS && !links.contains_key(&link) {
            warn!(
                graph_iri,
                "Document names more data links than one projection keeps"
            );
            break;
        }
        let part = schema_term(predicate, "hasPart")
            && roots.contains(subject)
            && datasets.contains(object.as_str())
            && link.scope().is_some();
        let posting = links.entry(link).or_insert_with(|| LinkPosting {
            cursor,
            ..LinkPosting::default()
        });
        posting.part |= part;
        if posting.subjects.len() < MAX_LINK_SUBJECTS && !posting.subjects.contains(subject) {
            posting.subjects.push(subject.clone());
        }
    }
    links
}

/// Replaces the document's postings: writes the projection and deletes every older row of
/// the document that the projection does not write again.
pub(crate) async fn plan_link_rows(
    storage: &StorageHandle,
    document_id: Ulid,
    links: &BTreeMap<DataLink, LinkPosting>,
) -> Result<LinkRows, DataLinkError> {
    let mut rows = LinkRows::default();
    let mut written = HashSet::new();
    for (link, posting) in links {
        let key = posting_key(&link.key(), document_id);
        rows.writes.push((
            DATA_LINK_KEYSPACE.to_string(),
            key.clone().into(),
            posting.to_bytes()?.into(),
        ));
        rows.writes.push((
            DOCUMENT_LINK_KEYSPACE.to_string(),
            document_key(document_id, &key).into(),
            ByteView::from(Vec::new()),
        ));
        written.insert(key);
    }
    for key in document_rows(storage, DOCUMENT_LINK_KEYSPACE, document_id).await? {
        if !written.contains(&key) {
            rows.deletes.push((
                DOCUMENT_LINK_KEYSPACE.to_string(),
                document_key(document_id, &key).into(),
            ));
            rows.deletes
                .push((DATA_LINK_KEYSPACE.to_string(), key.into()));
        }
    }
    Ok(rows)
}

/// The posting keys a document has rows for, from its per-document rows in `key_space`.
pub(crate) async fn document_rows(
    storage: &StorageHandle,
    key_space: &str,
    document_id: Ulid,
) -> Result<Vec<Vec<u8>>, DataLinkError> {
    let prefix = ByteView::from(document_id.to_bytes().to_vec());
    let mut start = None;
    let mut keys = Vec::new();
    loop {
        let (values, next) =
            iter_page(storage, key_space, Some(prefix.clone()), start, PAGE_SIZE).await?;
        keys.extend(
            values
                .into_iter()
                .filter_map(|(key, _)| key.get(16..).map(<[u8]>::to_vec)),
        );
        match next {
            Some(next) => start = Some(IterStart::After(next)),
            None => break,
        }
    }
    Ok(keys)
}

/// Deletes every posting of the given documents, for documents that were deleted.
pub(crate) async fn delete_documents(
    storage: &StorageHandle,
    document_ids: &BTreeSet<Ulid>,
) -> Result<(), DataLinkError> {
    let mut deletes = Vec::new();
    for document_id in document_ids {
        for key in document_rows(storage, DOCUMENT_LINK_KEYSPACE, *document_id).await? {
            deletes.push((
                DOCUMENT_LINK_KEYSPACE.to_string(),
                ByteView::from(document_key(*document_id, &key)),
            ));
            deletes.push((DATA_LINK_KEYSPACE.to_string(), ByteView::from(key)));
        }
    }
    apply_rows(
        storage,
        LinkRows {
            writes: Vec::new(),
            deletes,
        },
    )
    .await
}

/// Writes and deletes rows outside a transaction, for rebuilds and pruning.
pub(crate) async fn apply_rows(
    storage: &StorageHandle,
    rows: LinkRows,
) -> Result<(), DataLinkError> {
    for chunk in rows.writes.chunks(BATCH_SIZE) {
        expect_ok(
            storage
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes: chunk.to_vec(),
                    txn_id: None,
                })
                .await,
        )?;
    }
    for chunk in rows.deletes.chunks(BATCH_SIZE) {
        expect_ok(
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

fn expect_ok(event: Event) -> Result<(), DataLinkError> {
    match event {
        Event::Storage(StorageEvent::BatchWriteResult { .. })
        | Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(DataLinkError::UnexpectedEvent(format!("{other:?}"))),
    }
}

pub(crate) async fn iter_page(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Option<ByteView>,
    start: Option<IterStart>,
    limit: usize,
) -> Result<(Vec<(ByteView, ByteView)>, Option<ByteView>), DataLinkError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix,
            start,
            limit,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(DataLinkError::UnexpectedEvent(format!("{other:?}"))),
    }
}

/// One lookup's shared allowance of storage seeks and posting rows; an exhausted budget
/// makes the answer incomplete, never a scan.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LinkBudget {
    pub seeks: usize,
    pub rows: usize,
}

impl LinkBudget {
    pub(crate) fn exhausted(&self) -> bool {
        self.seeks == 0 || self.rows == 0
    }

    pub(crate) fn take_seek(&mut self) -> bool {
        if self.seeks == 0 {
            return false;
        }
        self.seeks -= 1;
        true
    }
}

/// One page of the postings of one identifier after document `after`, and whether more
/// postings may follow.
pub(crate) async fn read_postings(
    storage: &StorageHandle,
    link_key: &[u8],
    after: Option<Ulid>,
    limit: usize,
    budget: &mut LinkBudget,
) -> Result<(Vec<(Ulid, LinkPosting)>, bool), DataLinkError> {
    if !budget.take_seek() {
        return Ok((Vec::new(), true));
    }
    let limit = limit.min(budget.rows).max(1);
    let start =
        after.map(|document_id| IterStart::After(posting_key(link_key, document_id).into()));
    let (values, next) = iter_page(
        storage,
        DATA_LINK_KEYSPACE,
        Some(ByteView::from(link_key.to_vec())),
        start,
        limit,
    )
    .await?;
    budget.rows = budget.rows.saturating_sub(values.len());
    let mut postings = Vec::with_capacity(values.len());
    for (key, value) in values {
        // A longer identifier sharing this one as a byte prefix is no match.
        if key.len() != link_key.len() + 16 {
            continue;
        }
        let Some(document_id) = posting_document(&key) else {
            continue;
        };
        postings.push((document_id, LinkPosting::from_bytes(&value)?));
    }
    Ok((postings, next.is_some()))
}

/// The prefix lengths up to `max` the scope's postings use, ascending: one seek per length
/// found. `false` when the budget ended the walk early.
pub(crate) async fn prefix_lengths(
    storage: &StorageHandle,
    scope: &LinkScope,
    max: usize,
    budget: &mut LinkBudget,
) -> Result<(Vec<u16>, bool), DataLinkError> {
    let max = max.min(MAX_PREFIX_BYTES) as u16;
    let scope_key = ByteView::from(scope.key());
    let mut lengths = Vec::new();
    let mut next_length = 0u16;
    loop {
        if !budget.take_seek() {
            return Ok((lengths, false));
        }
        let start = IterStart::At(scope.length_start(next_length).into());
        let (values, _) = iter_page(
            storage,
            DATA_LINK_KEYSPACE,
            Some(scope_key.clone()),
            Some(start),
            1,
        )
        .await?;
        let Some(length) = values.first().and_then(|(key, _)| scope.length_of(key)) else {
            return Ok((lengths, true));
        };
        if length > max {
            return Ok((lengths, true));
        }
        lengths.push(length);
        let Some(after) = length.checked_add(1) else {
            return Ok((lengths, true));
        };
        next_length = after;
    }
}

/// The identifiers a lookup for `key` in `scope` reads: one exact prefix per length the
/// scope uses, each a prefix of `key`. `false` when the budget cut the list short.
pub(crate) async fn key_prefixes(
    storage: &StorageHandle,
    scope: &LinkScope,
    key: &str,
    budget: &mut LinkBudget,
) -> Result<(Vec<DataLink>, bool), DataLinkError> {
    let (lengths, complete) = prefix_lengths(storage, scope, key.len(), budget).await?;
    let links = lengths
        .into_iter()
        .filter_map(|length| key.get(..usize::from(length)))
        .map(|prefix| scope.link(prefix))
        .collect();
    Ok((links, complete))
}

/// At most this many distribution records are read to verify one document's versions.
const MAX_SETS: usize = 4096;

/// Adds a derived posting per version link: verified in its set when a local member's version
/// id index holds it (only then does it carry a set), otherwise unverified and counted.
pub(crate) async fn associate_versions(
    storage: &StorageHandle,
    links: &mut BTreeMap<DataLink, LinkPosting>,
) -> Result<(), DataLinkError> {
    let versions: Vec<_> = links
        .iter()
        .filter_map(|(link, posting)| match link {
            DataLink::Version {
                realm_id,
                version_id,
            } => Some((*realm_id, *version_id, posting.clone())),
            _ => None,
        })
        .collect();
    if versions.is_empty() {
        return Ok(());
    }
    let members = local_members(storage).await?;
    for (realm_id, version_id, posting) in versions {
        let hint = SetHint::of_version(version_id);
        let mut verified = false;
        for set_id in members.get(&hint).into_iter().flatten() {
            let index = read_value(
                storage,
                VERSION_INDEX_KEYSPACE,
                index_key(*set_id, version_id),
            )
            .await?;
            if index.is_some() {
                links.insert(
                    DataLink::SetVersion {
                        realm_id,
                        set_id: *set_id,
                        version_id,
                    },
                    posting.clone(),
                );
                verified = true;
            }
        }
        if !verified {
            links.insert(
                DataLink::Unverified {
                    realm_id,
                    version_id,
                },
                posting,
            );
        }
    }
    Ok(())
}

/// The sets this node holds a bound member bucket of, by hint.
async fn local_members(
    storage: &StorageHandle,
) -> Result<BTreeMap<SetHint, Vec<Ulid>>, DataLinkError> {
    let mut members = BTreeMap::<SetHint, Vec<Ulid>>::new();
    let mut start = None;
    let mut seen = 0usize;
    loop {
        let (values, next) =
            iter_page(storage, DISTRIBUTION_KEYSPACE, None, start, PAGE_SIZE).await?;
        for (_, value) in values {
            seen += 1;
            let Ok(view) = DistributionView::from_bytes(&value) else {
                continue;
            };
            let snapshot = view.snapshot();
            let set_id = snapshot.set_id();
            let bucket = read_value(
                storage,
                S3_BUCKET_KEYSPACE,
                snapshot.config.bucket.as_bytes().to_vec(),
            )
            .await?
            .map(|value| BucketInfo::from_bytes(&value))
            .transpose()?;
            if bucket
                .and_then(|info| info.distribution)
                .is_some_and(|binding| binding.set_id == set_id)
            {
                members
                    .entry(SetHint::of_set(set_id))
                    .or_default()
                    .push(set_id);
            }
        }
        match next {
            Some(next) if seen < MAX_SETS => start = Some(IterStart::After(next)),
            _ => return Ok(members),
        }
    }
}

async fn read_value(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<ByteView>, DataLinkError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(DataLinkError::UnexpectedEvent(format!("{other:?}"))),
    }
}

/// Version links on this node no member could verify yet, counted up to `cap`.
pub(crate) async fn unverified_versions(
    storage: &StorageHandle,
    cap: usize,
) -> Result<usize, DataLinkError> {
    let prefix = ByteView::from(unverified_key());
    let (rows, _) = iter_page(storage, DATA_LINK_KEYSPACE, Some(prefix), None, cap).await?;
    Ok(rows.len())
}

/// One page of the postings that name a set: its prefix links, then its verified versions,
/// in key order after `after`. The next position is the last key read.
pub(crate) async fn set_postings(
    storage: &StorageHandle,
    scope: &LinkScope,
    set_id: Ulid,
    after: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, Ulid, LinkPosting)>, Option<Vec<u8>>), DataLinkError> {
    let mut rows = Vec::new();
    let mut last = None;
    for range in [scope.key(), set_versions_key(set_id)] {
        if rows.len() >= limit {
            return Ok((rows, last));
        }
        let start = match &after {
            Some(after) if after.starts_with(&range) => {
                Some(IterStart::After(after.clone().into()))
            }
            Some(after) if after.as_slice() > range.as_slice() => continue,
            _ => None,
        };
        let (values, next) = iter_page(
            storage,
            DATA_LINK_KEYSPACE,
            Some(range.into()),
            start,
            limit - rows.len(),
        )
        .await?;
        for (key, value) in values {
            last = Some(key.to_vec());
            if let Some(document_id) = posting_document(&key) {
                rows.push((key.to_vec(), document_id, LinkPosting::from_bytes(&value)?));
            }
        }
        if next.is_some() {
            return Ok((rows, last));
        }
    }
    // A full page names its last key even at the end; the page after it is empty.
    let next = (rows.len() >= limit).then_some(last).flatten();
    Ok((rows, next))
}

/// Queued and parked materializations on this node, each counted up to `cap`: postings of
/// such documents may be missing or stale.
pub(crate) async fn unsettled_documents(
    storage: &StorageHandle,
    cap: usize,
) -> Result<(usize, usize), DataLinkError> {
    let (pending, _) = iter_page(storage, MATERIALIZATION_JOB_KEYSPACE, None, None, cap).await?;
    let (failed, _) = iter_page(storage, DEAD_LETTER_KEYSPACE, None, None, cap).await?;
    Ok((pending.len(), failed.len()))
}

#[cfg(test)]
#[path = "data_links_tests.rs"]
mod tests;
