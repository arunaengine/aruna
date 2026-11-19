//! Keeps the node-local data link postings of materialized documents: identifier first, so a
//! lookup is a seek, and per document, so a materialization replaces exactly its own rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Prefix lookups probe only the prefix lengths a bucket's postings use.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Mutex;
use tokio::time::Instant;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DATA_LINK_KEYSPACE, DISTRIBUTION_KEYSPACE, DOCUMENT_LINK_KEYSPACE, S3_BUCKET_KEYSPACE,
    VERSION_INDEX_KEYSPACE,
};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::data::link::unverified_key;
use aruna_core::structs::storage::data::link::{
    DataLink, LinkPosting, MAX_DOCUMENT_LINKS, MAX_LINK_SUBJECTS, document_key, posting_document,
    posting_key,
};
use aruna_core::structs::storage::distribution::inventory::index_key;
use aruna_core::structs::storage::distribution::{DistributionView, SetHint};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::distribution::resolve::{GlobalResolution, RESOLVE_BUDGET, resolve_version, resolved};
use crate::driver::{DriverContext, drive_until};

const PAGE_SIZE: usize = 256;
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
            // The marker posting makes every lookup on this node report itself incomplete.
            let marker = LinkPosting {
                cursor,
                ..LinkPosting::default()
            };
            links.insert(DataLink::Truncated, marker);
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
        other => Err(DataLinkError::UnexpectedEvent(other.label().to_string())),
    }
}

/// One lookup's shared allowance of storage seeks and posting rows; an exhausted budget
/// makes the answer incomplete, never a scan.
#[derive(Clone, Copy, Debug, PartialEq)]
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
        let sets = local_sets(storage, &members, version_id).await?;
        for set_id in &sets {
            let link = DataLink::SetVersion {
                realm_id,
                set_id: *set_id,
                version_id,
            };
            links.insert(link, posting.clone());
        }
        if sets.is_empty() {
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

/// The sets among `members` whose version id index on this node holds `version_id`.
async fn local_sets(
    storage: &StorageHandle,
    members: &BTreeMap<SetHint, Vec<Ulid>>,
    version_id: Ulid,
) -> Result<Vec<Ulid>, DataLinkError> {
    let mut sets = Vec::new();
    for set_id in members
        .get(&SetHint::of_version(version_id))
        .into_iter()
        .flatten()
    {
        let key = index_key(*set_id, version_id);
        if read_value(storage, VERSION_INDEX_KEYSPACE, key)
            .await?
            .is_some()
        {
            sets.push(*set_id);
        }
    }
    Ok(sets)
}

/// Unverified version links one background pass tries to verify.
const RETRY_PAGE: usize = 16;
/// The unverified posting the next pass continues after; a restart begins again at the start.
static RETRY_CURSOR: Mutex<Option<Vec<u8>>> = Mutex::new(None);

/// Tries a page of the version links no member could verify at materialization: through a
/// local member's index, else by asking the set's members. A verified link gains its set
/// posting; a version no set holds stops counting. Returns whether more links follow.
pub(crate) async fn retry_unverified(context: &DriverContext) -> Result<bool, DataLinkError> {
    let storage = &context.storage_handle;
    let start = (RETRY_CURSOR.lock().ok())
        .and_then(|cursor| cursor.clone())
        .map(|after| IterStart::After(after.into()));
    let prefix = ByteView::from(unverified_key());
    let (rows, next) =
        iter_page(storage, DATA_LINK_KEYSPACE, Some(prefix), start, RETRY_PAGE).await?;
    let members = match rows.is_empty() {
        true => BTreeMap::new(),
        false => local_members(storage).await?,
    };
    for (key, value) in rows {
        let Some((realm_id, version_id, document_id)) = unverified_parts(&key) else {
            continue;
        };
        let local = local_sets(storage, &members, version_id).await?;
        let verified = match local.first() {
            Some(set_id) => Some(Some(*set_id)),
            None => {
                let deadline = Instant::now() + RESOLVE_BUDGET;
                match resolved(
                    drive_until(
                        resolve_version(context.node_and_realm(), version_id, deadline),
                        context,
                        deadline,
                    )
                    .await,
                ) {
                    GlobalResolution::Found(found) => Some(Some(found.set_id)),
                    GlobalResolution::NotFound => Some(None),
                    GlobalResolution::Unavailable | GlobalResolution::Ambiguous => None,
                }
            }
        };
        let Some(set_id) = verified else {
            continue;
        };
        let link = set_id.map(|set_id| DataLink::SetVersion {
            realm_id,
            set_id,
            version_id,
        });
        settle_unverified(storage, (&key, &value), document_id, link).await?;
    }
    if let Ok(mut cursor) = RETRY_CURSOR.lock() {
        *cursor = next.as_ref().map(|next| next.to_vec());
    }
    Ok(next.is_some())
}

/// The realm, version and document of an unverified posting key.
fn unverified_parts(key: &[u8]) -> Option<(RealmId, Ulid, Ulid)> {
    let rest = key.strip_prefix(unverified_key().as_slice())?;
    let realm: [u8; 32] = rest.get(..32)?.try_into().ok()?;
    let version: [u8; 16] = rest.get(32..48)?.try_into().ok()?;
    let document = posting_document(key).filter(|_| rest.len() == 64)?;
    Some((
        RealmId::from_bytes(realm),
        Ulid::from_bytes(version),
        document,
    ))
}

/// Replaces an unverified posting by its set posting, or drops it, while the document's
/// materialization has not replaced it meanwhile.
async fn settle_unverified(
    storage: &StorageHandle,
    (key, value): (&ByteView, &ByteView),
    document_id: Ulid,
    link: Option<DataLink>,
) -> Result<(), DataLinkError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(DataLinkError::UnexpectedEvent(other.label().to_string())),
    };
    let current = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: DATA_LINK_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(DataLinkError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let mut writes = Vec::new();
    if let Some(link) = link {
        let posting = posting_key(&link.key(), document_id);
        let row = document_key(document_id, &posting);
        writes.push((
            DOCUMENT_LINK_KEYSPACE.to_string(),
            row.into(),
            ByteView::from(Vec::new()),
        ));
        writes.push((
            DATA_LINK_KEYSPACE.to_string(),
            posting.into(),
            value.clone(),
        ));
    }
    let deletes = vec![
        (DATA_LINK_KEYSPACE.to_string(), key.clone()),
        (
            DOCUMENT_LINK_KEYSPACE.to_string(),
            document_key(document_id, key).into(),
        ),
    ];
    let effects = match current.as_ref() == Some(value) {
        true => vec![
            StorageEffect::BatchWrite {
                writes,
                txn_id: Some(txn_id),
            },
            StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            },
            StorageEffect::CommitTransaction { txn_id },
        ],
        false => vec![StorageEffect::AbortTransaction { txn_id }],
    };
    for effect in effects {
        match storage.send_storage_effect(effect).await {
            // A materialization that won the race keeps its own rows.
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) => return Ok(()),
            Event::Storage(StorageEvent::Error { error }) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error.into());
            }
            _ => {}
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
        other => Err(DataLinkError::UnexpectedEvent(other.label().to_string())),
    }
}

#[cfg(test)]
#[path = "data_links_tests.rs"]
pub(super) mod tests;
