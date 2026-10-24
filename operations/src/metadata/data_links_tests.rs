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
fn root_dataset_parts_are_parts() {
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
fn untyped_part_is_mention() {
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
async fn probes_only_used_lengths() {
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
async fn deleted_documents_leave_nothing() {
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
    assert!(document_rows(&storage, document).await.unwrap().is_empty());
    let mut budget = budget();
    let key = DataLink::parse(&set_url("a/")).unwrap().key();
    let (postings, _) = read_postings(&storage, &key, None, 10, &mut budget)
        .await
        .unwrap();
    assert!(postings.is_empty());
}
