//! Tests data link projection and postings: parts, replacement, probes, budgets and retries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::storage::data::link::{set_versions_key, truncated_key};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_storage::StorageHandle;
use tempfile::tempdir;

use crate::tests::calls::{child, runs_child};

const GRAPH: &str = "https://example.org/graph/1";
const HAS_PART: &str = "http://schema.org/hasPart";

fn realm() -> RealmId {
    RealmId::from_bytes([4; 32])
}

fn set_url(prefix: &str) -> String {
    GlobalId::Set {
        realm_id: realm(),
        set_id: Ulid::from_bytes([5; 16]),
        prefix: prefix.to_string(),
    }
    .to_url()
}

fn triple(subject: &str, predicate: &str, object: &str) -> (String, String, String) {
    (
        subject.to_string(),
        predicate.to_string(),
        object.to_string(),
    )
}

#[test]
fn dataset_folder_part() {
    let folder = set_url("raw/");
    let references = vec![
        triple(GRAPH, HAS_PART, &folder),
        triple(&folder, RDF_TYPE, "http://schema.org/Dataset"),
        triple(
            "https://example.org/other",
            "http://schema.org/isBasedOn",
            &set_url("x/"),
        ),
    ];
    let links = project_links(GRAPH, Ulid::from_bytes([1; 16]), &references);
    assert_eq!(links.len(), 2);
    let part = &links[&DataLink::parse(&folder).unwrap()];
    assert!(part.part);
    assert_eq!(part.subjects, vec![GRAPH.to_string()]);
    // A mention outside the root's Dataset parts never makes files part of the document.
    assert!(!links[&DataLink::parse(&set_url("x/")).unwrap()].part);
}

#[test]
fn untyped_part_mentions() {
    let folder = set_url("raw/");
    let links = project_links(GRAPH, Ulid::nil(), &[triple(GRAPH, HAS_PART, &folder)]);
    assert!(!links[&DataLink::parse(&folder).unwrap()].part);
}

#[test]
fn descriptor_names_root() {
    let folder = set_url("");
    let root = "https://example.org/crate/";
    let references = vec![
        triple(
            "https://example.org/crate/ro-crate-metadata.json",
            "http://schema.org/about",
            root,
        ),
        triple(root, HAS_PART, &folder),
        triple(&folder, RDF_TYPE, "http://schema.org/Dataset"),
    ];
    let links = project_links(GRAPH, Ulid::nil(), &references);
    assert!(links[&DataLink::parse(&folder).unwrap()].part);
}

/// Writes and deletes rows outside a transaction, as fixtures.
pub(crate) async fn apply_rows(
    storage: &StorageHandle,
    rows: LinkRows,
) -> Result<(), DataLinkError> {
    let writes = StorageEffect::BatchWrite {
        writes: rows.writes,
        txn_id: None,
    };
    let deletes = StorageEffect::BatchDelete {
        deletes: rows.deletes,
        txn_id: None,
    };
    for effect in [writes, deletes] {
        match storage.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::BatchWriteResult { .. })
            | Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => return Err(DataLinkError::UnexpectedEvent(other.label().to_string())),
        }
    }
    Ok(())
}

/// The rows of the data link keyspace below `prefix`.
async fn rows_below(storage: &StorageHandle, prefix: Vec<u8>) -> Vec<(ByteView, ByteView)> {
    let scan = StorageEffect::Iter {
        key_space: DATA_LINK_KEYSPACE.to_string(),
        prefix: Some(prefix.into()),
        start: None,
        limit: 64,
        txn_id: None,
    };
    match storage.send_storage_effect(scan).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        other => panic!("expected the rows, got {}", other.label()),
    }
}

fn context(storage: &StorageHandle) -> DriverContext {
    DriverContext::storage_only(storage.clone())
}

async fn plan_link_rows(
    storage: &StorageHandle,
    document_id: Ulid,
    links: &BTreeMap<DataLink, LinkPosting>,
) -> Result<LinkRows, DataLinkError> {
    let rows = LinkRowsOperation::new(document_id, links.clone());
    drive(rows, &context(storage)).await
}

async fn document_rows(
    storage: &StorageHandle,
    key_space: &'static str,
    document_id: Ulid,
) -> Result<Vec<Vec<u8>>, DataLinkError> {
    let rows = DocumentRowsOperation::new(key_space, document_id);
    drive(rows, &context(storage)).await
}

async fn settle_unverified(
    storage: &StorageHandle,
    (key, value): (&ByteView, &ByteView),
    document_id: Ulid,
    link: Option<DataLink>,
) -> Result<(), DataLinkError> {
    let settle = SettleUnverifiedOperation::new((key.clone(), value.clone()), document_id, link);
    drive(settle, &context(storage)).await
}

async fn stored(rows: LinkRows, storage: &StorageHandle) {
    apply_rows(storage, rows).await.unwrap();
}

#[tokio::test]
async fn replaces_own_postings() {
    let dir = tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document = Ulid::from_bytes([9; 16]);
    let first = project_links(
        GRAPH,
        Ulid::from_bytes([1; 16]),
        &[
            triple(GRAPH, HAS_PART, &set_url("a/")),
            triple(GRAPH, HAS_PART, &set_url("b/")),
        ],
    );
    stored(
        plan_link_rows(&storage, document, &first).await.unwrap(),
        &storage,
    )
    .await;

    let second = project_links(
        GRAPH,
        Ulid::from_bytes([2; 16]),
        &[triple(GRAPH, HAS_PART, &set_url("b/"))],
    );
    let rows = plan_link_rows(&storage, document, &second).await.unwrap();
    // Only the dropped link is deleted; a rewritten key must not be deleted after its write.
    assert_eq!(rows.deletes.len(), 2);
    stored(rows, &storage).await;

    let a = DataLink::parse(&set_url("a/")).unwrap().key();
    assert!(rows_below(&storage, a).await.is_empty());
    let b = DataLink::parse(&set_url("b/")).unwrap().key();
    let kept = rows_below(&storage, b).await;
    assert_eq!(kept.len(), 1);
    let posting = LinkPosting::from_bytes(&kept[0].1).unwrap();
    assert_eq!(posting.cursor, Ulid::from_bytes([2; 16]));
}

// A document over the link limit keeps a marker, so lookups never claim to be complete.
#[tokio::test]
async fn truncation_is_marked() {
    let references: Vec<_> = (0..=MAX_DOCUMENT_LINKS)
        .map(|index| triple(GRAPH, HAS_PART, &set_url(&format!("{index}/"))))
        .collect();
    let links = project_links(GRAPH, Ulid::nil(), &references);
    assert!(links.contains_key(&DataLink::Truncated));
    assert_eq!(links.len(), MAX_DOCUMENT_LINKS + 1);

    let dir = tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document = Ulid::from_bytes([1; 16]);
    let marked = BTreeMap::from([(DataLink::Truncated, LinkPosting::default())]);
    stored(
        plan_link_rows(&storage, document, &marked).await.unwrap(),
        &storage,
    )
    .await;
    assert_eq!(rows_below(&storage, truncated_key()).await.len(), 1);
    // A later projection within the limit drops the marker with the document's other rows.
    let rows = plan_link_rows(&storage, document, &BTreeMap::new())
        .await
        .unwrap();
    stored(rows, &storage).await;
    assert_eq!(rows_below(&storage, truncated_key()).await.len(), 0);
}

// A verified retry moves the posting to its set; a newer materialization's row is kept.
#[tokio::test]
async fn retry_settles_unverified() {
    let dir = tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (document, version_id, set_id) = (
        Ulid::from_bytes([1; 16]),
        Ulid::from_bytes([6; 16]),
        Ulid::from_bytes([5; 16]),
    );
    let unverified = DataLink::Unverified {
        realm_id: realm(),
        version_id,
    };
    let links = BTreeMap::from([(unverified.clone(), LinkPosting::default())]);
    stored(
        plan_link_rows(&storage, document, &links).await.unwrap(),
        &storage,
    )
    .await;
    let key = ByteView::from(posting_key(&unverified.key(), document));
    assert_eq!(
        unverified_parts(&key),
        Some((realm(), version_id, document))
    );
    let value = ByteView::from(LinkPosting::default().to_bytes().unwrap());

    // A row that changed since it was read stays as it is.
    let stale = ByteView::from(Vec::new());
    settle_unverified(&storage, (&key, &stale), document, None)
        .await
        .unwrap();
    assert_eq!(rows_below(&storage, unverified_key()).await.len(), 1);

    let verified = DataLink::SetVersion {
        realm_id: realm(),
        set_id,
        version_id,
    };
    let verified_key = verified.key();
    settle_unverified(&storage, (&key, &value), document, Some(verified))
        .await
        .unwrap();
    assert_eq!(rows_below(&storage, unverified_key()).await.len(), 0);
    assert_eq!(
        rows_below(&storage, set_versions_key(set_id)).await.len(),
        1
    );
    // The document's own rows name the set posting, so its next projection replaces it.
    let rows = document_rows(&storage, DOCUMENT_LINK_KEYSPACE, document)
        .await
        .unwrap();
    assert_eq!(rows, vec![posting_key(&verified_key, document)]);
}

fn version() -> Ulid {
    Ulid::from_parts(9, 9)
}

fn posting() -> LinkPosting {
    LinkPosting {
        cursor: Ulid::from_parts(1, 1),
        part: false,
        subjects: vec!["#file".to_string()],
    }
}

fn version_link() -> DataLink {
    DataLink::Version {
        realm_id: realm(),
        version_id: version(),
    }
}

// One local set whose hint matches the version, so the version index is read there.
fn members(set_id: Ulid) -> BTreeMap<SetHint, Vec<Ulid>> {
    BTreeMap::from([(SetHint::of_version(version()), vec![set_id])])
}

fn index_read(set_id: Ulid) -> IoCall {
    let key = Key::from(index_key(set_id, version()));
    IoCall::Read(vec![(VERSION_INDEX_KEYSPACE.to_string(), key)])
}

#[test]
fn members_empty_page() {
    let mut local = LocalMembersOperation::new();
    let page = page_call(DISTRIBUTION_KEYSPACE, Vec::new(), None);
    assert_eq!(local.first(), Ok(Next::Call(page)));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(local.next(empty), Ok(Next::Done(BTreeMap::new())));
}

#[test]
fn members_skip_malformed() {
    let mut local = LocalMembersOperation::new();
    local.first().unwrap();
    let after = Key::from(vec![2]);
    let malformed = IoResult::Page {
        rows: vec![(Key::from(vec![1]), ByteView::from(vec![0xff]))],
        next: Some(after.clone()),
    };
    let page = page_call(DISTRIBUTION_KEYSPACE, Vec::new(), Some(after));
    assert_eq!(local.next(malformed), Ok(Next::Call(page)));
}

#[test]
fn no_versions_done() {
    let link = DataLink::SetPrefix {
        realm_id: realm(),
        set_id: Ulid::from_parts(5, 5),
        prefix: String::new(),
    };
    let links = BTreeMap::from([(link, posting())]);
    let mut associate = AssociateVersionsOperation::new(links.clone());
    assert_eq!(associate.first(), Ok(Next::Done(links)));
}

#[test]
fn unknown_version_unverified() {
    let mut associate =
        AssociateVersionsOperation::new(BTreeMap::from([(version_link(), posting())]));
    assert!(runs_child(associate.first()));
    let none = child(Ok::<_, DataLinkError>(BTreeMap::<SetHint, Vec<Ulid>>::new()));
    let unverified = DataLink::Unverified {
        realm_id: realm(),
        version_id: version(),
    };
    let expected = BTreeMap::from([(version_link(), posting()), (unverified, posting())]);
    assert_eq!(associate.next(none), Ok(Next::Done(expected)));
}

#[test]
fn indexed_version_verified() {
    let set_id = Ulid::from_parts(5, 5);
    let mut associate =
        AssociateVersionsOperation::new(BTreeMap::from([(version_link(), posting())]));
    associate.first().unwrap();
    let local = child(Ok::<_, DataLinkError>(members(set_id)));
    assert_eq!(associate.next(local), Ok(Next::Call(index_read(set_id))));
    let found = IoResult::Values(vec![Some(ByteView::from(Vec::new()))]);
    let verified = DataLink::SetVersion {
        realm_id: realm(),
        set_id,
        version_id: version(),
    };
    let expected = BTreeMap::from([(version_link(), posting()), (verified, posting())]);
    assert_eq!(associate.next(found), Ok(Next::Done(expected)));
}

fn unverified_row() -> (Key, ByteView) {
    let link = DataLink::Unverified {
        realm_id: realm(),
        version_id: version(),
    };
    let key = posting_key(&link.key(), Ulid::from_parts(3, 3));
    (
        Key::from(key),
        ByteView::from(posting().to_bytes().unwrap()),
    )
}

#[test]
fn retry_empty_page() {
    let mut retry = RetryUnverifiedOperation::new(None, None);
    let page = IoCall::Page {
        space: DATA_LINK_KEYSPACE.to_string(),
        prefix: Key::from(unverified_key()),
        after: None,
        limit: RETRY_PAGE,
    };
    assert_eq!(retry.first(), Ok(Next::Call(page)));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(retry.next(empty), Ok(Next::Done(None)));
}

#[test]
fn retry_settles_local() {
    let set_id = Ulid::from_parts(5, 5);
    let mut retry = RetryUnverifiedOperation::new(None, None);
    retry.first().unwrap();
    let after = Key::from(vec![7]);
    let page = IoResult::Page {
        rows: vec![unverified_row()],
        next: Some(after),
    };
    assert!(runs_child(retry.next(page)));
    let local = child(Ok::<_, DataLinkError>(members(set_id)));
    assert_eq!(retry.next(local), Ok(Next::Call(index_read(set_id))));
    let found = IoResult::Values(vec![Some(ByteView::from(Vec::new()))]);
    assert!(runs_child(retry.next(found)));
    let settled = child(Ok::<(), DataLinkError>(()));
    assert_eq!(retry.next(settled), Ok(Next::Done(Some(vec![7]))));
}

#[test]
fn retry_skips_conflict() {
    let mut retry = RetryUnverifiedOperation::new(None, None);
    retry.first().unwrap();
    let page = IoResult::Page {
        rows: vec![unverified_row()],
        next: None,
    };
    retry.next(page).unwrap();
    let set_id = Ulid::from_parts(5, 5);
    retry
        .next(child(Ok::<_, DataLinkError>(members(set_id))))
        .unwrap();
    retry
        .next(IoResult::Values(vec![Some(ByteView::from(Vec::new()))]))
        .unwrap();
    let conflict = DataLinkError::Storage(StorageError::TransactionConflict);
    let lost = child(Err::<(), _>(conflict));
    assert_eq!(retry.next(lost), Ok(Next::Done(None)));
}
