// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{LinkDocument, LinkHit, LinkNodePage, merge_pages};
use ulid::Ulid;

fn document(seed: u8, link: &str) -> LinkDocument {
    LinkDocument {
        document_id: Ulid::from_bytes([seed; 16]),
        group_id: "group".to_string(),
        document_path: format!("datasets/{seed}"),
        graph_iri: format!("https://example.org/{seed}"),
        title: None,
        links: vec![LinkHit {
            id: link.to_string(),
            part: true,
            subject_iris: Vec::new(),
        }],
    }
}

fn node(documents: Vec<LinkDocument>) -> LinkNodePage {
    LinkNodePage {
        documents,
        ..LinkNodePage::default()
    }
}

#[test]
fn holders_merge_by_document() {
    // Two holders of one document answer it once, with the links of both.
    let page = merge_pages(
        [
            node(vec![document(1, "a"), document(3, "a")]),
            node(vec![document(1, "b"), document(2, "a")]),
        ],
        10,
        0,
    );
    let ids: Vec<_> = page.documents.iter().map(|doc| doc.document_id).collect();
    assert_eq!(
        ids,
        vec![
            Ulid::from_bytes([1; 16]),
            Ulid::from_bytes([2; 16]),
            Ulid::from_bytes([3; 16])
        ]
    );
    assert_eq!(page.documents[0].links.len(), 2);
    assert!(page.complete);
    assert_eq!(page.next_after, None);
}

#[test]
fn limit_sets_cursor() {
    let page = merge_pages(
        [node(vec![
            document(1, "a"),
            document(2, "a"),
            document(3, "a"),
        ])],
        2,
        0,
    );
    assert_eq!(page.documents.len(), 2);
    assert_eq!(page.next_after, Some(Ulid::from_bytes([2; 16])));
    assert!(!page.complete);
}

#[test]
fn unsettled_or_failed_is_incomplete() {
    // A missing node, a cut budget or a pending materialization never reads as complete.
    let failed = merge_pages([node(vec![document(1, "a")])], 10, 1);
    assert!(!failed.complete);
    let cut = merge_pages(
        [LinkNodePage {
            cut: true,
            ..LinkNodePage::default()
        }],
        10,
        0,
    );
    assert!(!cut.complete);
    let pending = merge_pages(
        [LinkNodePage {
            pending_materializations: 1,
            ..LinkNodePage::default()
        }],
        10,
        0,
    );
    assert!(!pending.complete);
    assert_eq!(pending.pending_materializations, 1);
}

#[test]
fn hidden_survives_merge() {
    let page = merge_pages(
        [
            LinkNodePage {
                hidden: true,
                ..LinkNodePage::default()
            },
            node(vec![document(1, "a")]),
        ],
        10,
        0,
    );
    assert!(page.hidden);
    assert_eq!(page.documents.len(), 1);
}
