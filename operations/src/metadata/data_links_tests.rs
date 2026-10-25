// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::global_id::GlobalId;
use tempfile::tempdir;

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

fn scope() -> LinkScope {
    LinkScope::Set {
        realm_id: realm(),
        set_id: Ulid::from_bytes([5; 16]),
    }
}

fn budget() -> LinkBudget {
    LinkBudget {
        seeks: 64,
        rows: 1024,
    }
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

    let mut budget = budget();
    let a = DataLink::parse(&set_url("a/")).unwrap().key();
    let (gone, _) = read_postings(&storage, &a, None, 10, &mut budget)
        .await
        .unwrap();
    assert!(gone.is_empty());
    let b = DataLink::parse(&set_url("b/")).unwrap().key();
    let (kept, more) = read_postings(&storage, &b, None, 10, &mut budget)
        .await
        .unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].1.cursor, Ulid::from_bytes([2; 16]));
    assert!(!more);
}

#[tokio::test]
async fn probes_used_lengths() {
    let dir = tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    for (seed, prefix) in [(1u8, ""), (2, "raw/"), (3, "raw/2026/"), (4, "other/")] {
        let links = project_links(
            GRAPH,
            Ulid::nil(),
            &[triple(GRAPH, HAS_PART, &set_url(prefix))],
        );
        let rows = plan_link_rows(&storage, Ulid::from_bytes([seed; 16]), &links)
            .await
            .unwrap();
        stored(rows, &storage).await;
    }
    let mut budget = budget();
    let (links, complete) = key_prefixes(&storage, &scope(), "raw/2026/a.fastq", &mut budget)
        .await
        .unwrap();
    assert!(complete);
    let prefixes: Vec<_> = links
        .iter()
        .map(|link| link.scope().unwrap().1.to_string())
        .collect();
    // The length of `other/` is probed as `raw/20`; no key outside the used lengths is read.
    assert_eq!(prefixes, vec!["", "raw/", "raw/20", "raw/2026/"]);
    // One seek per used length plus the final miss.
    assert_eq!(budget.seeks, 64 - 5);
}

#[tokio::test]
async fn budget_marks_incomplete() {
    let dir = tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    for (seed, prefix) in [(1u8, "a"), (2, "ab"), (3, "abc")] {
        let links = project_links(
            GRAPH,
            Ulid::nil(),
            &[triple(GRAPH, HAS_PART, &set_url(prefix))],
        );
        let rows = plan_link_rows(&storage, Ulid::from_bytes([seed; 16]), &links)
            .await
            .unwrap();
        stored(rows, &storage).await;
    }
    let mut budget = LinkBudget { seeks: 2, rows: 10 };
    let (links, complete) = key_prefixes(&storage, &scope(), "abcd", &mut budget)
        .await
        .unwrap();
    assert!(!complete);
    assert_eq!(links.len(), 2);
}

#[tokio::test]
async fn deletion_leaves_nothing() {
    let dir = tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document = Ulid::from_bytes([7; 16]);
    let links = project_links(
        GRAPH,
        Ulid::nil(),
        &[triple(GRAPH, HAS_PART, &set_url("a/"))],
    );
    stored(
        plan_link_rows(&storage, document, &links).await.unwrap(),
        &storage,
    )
    .await;
    delete_documents(&storage, &BTreeSet::from([document]))
        .await
        .unwrap();
    assert!(
        document_rows(&storage, DOCUMENT_LINK_KEYSPACE, document)
            .await
            .unwrap()
            .is_empty()
    );
    let mut budget = budget();
    let key = DataLink::parse(&set_url("a/")).unwrap().key();
    let (postings, _) = read_postings(&storage, &key, None, 10, &mut budget)
        .await
        .unwrap();
    assert!(postings.is_empty());
}

#[tokio::test]
async fn pages_both_ranges() {
    let dir = tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let set_id = Ulid::from_bytes([5; 16]);
    let mut links = project_links(
        GRAPH,
        Ulid::nil(),
        &[triple(GRAPH, HAS_PART, &set_url("a/"))],
    );
    links.insert(
        DataLink::SetVersion {
            realm_id: realm(),
            set_id,
            version_id: Ulid::from_bytes([8; 16]),
        },
        LinkPosting::default(),
    );
    links.insert(
        DataLink::Unverified {
            realm_id: realm(),
            version_id: Ulid::from_bytes([6; 16]),
        },
        LinkPosting::default(),
    );
    let rows = plan_link_rows(&storage, Ulid::from_bytes([1; 16]), &links)
        .await
        .unwrap();
    stored(rows, &storage).await;

    // One row per page walks the prefix range first, then the verified versions.
    let (first, next) = set_postings(&storage, &scope(), set_id, None, 1)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert!(first[0].0.starts_with(&scope().key()));
    let (second, next) = set_postings(&storage, &scope(), set_id, next, 1)
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert!(second[0].0.starts_with(&set_versions_key(set_id)));
    let (rest, end) = set_postings(&storage, &scope(), set_id, next, 1)
        .await
        .unwrap();
    assert!(rest.is_empty());
    assert_eq!(end, None);
    assert_eq!(unverified_versions(&storage, 10).await.unwrap(), 1);
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
    assert_eq!(truncated_documents(&storage, 10).await.unwrap(), 1);
    // A later projection within the limit drops the marker with the document's other rows.
    let rows = plan_link_rows(&storage, document, &BTreeMap::new())
        .await
        .unwrap();
    stored(rows, &storage).await;
    assert_eq!(truncated_documents(&storage, 10).await.unwrap(), 0);
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
    assert_eq!(unverified_versions(&storage, 10).await.unwrap(), 1);

    let verified = DataLink::SetVersion {
        realm_id: realm(),
        set_id,
        version_id,
    };
    let verified_key = verified.key();
    settle_unverified(&storage, (&key, &value), document, Some(verified))
        .await
        .unwrap();
    assert_eq!(unverified_versions(&storage, 10).await.unwrap(), 0);
    let (rows, _) = set_postings(&storage, &scope(), set_id, None, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // The document's own rows name the set posting, so its next projection replaces it.
    let rows = document_rows(&storage, DOCUMENT_LINK_KEYSPACE, document)
        .await
        .unwrap();
    assert_eq!(rows, vec![posting_key(&verified_key, document)]);
}
