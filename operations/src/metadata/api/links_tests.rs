//! Tests merging data link pages from several nodes: cursors, completeness and hidden entries.
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
fn holders_merge_documents() {
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
fn unsettled_stays_incomplete() {
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

#[test]
fn hidden_page_continues() {
    // A node whose examined documents were all hidden still names where to continue.
    let hidden = LinkNodePage {
        hidden: true,
        next: Some(Ulid::from_bytes([5; 16])),
        ..LinkNodePage::default()
    };
    let page = merge_pages([hidden], 10, 0);
    assert!(page.documents.is_empty());
    assert_eq!(page.next_after, Some(Ulid::from_bytes([5; 16])));
    assert!(!page.complete);
}

#[test]
fn smallest_end_bounds() {
    // Documents past another node's end wait for the next page, so none is skipped there.
    let short = LinkNodePage {
        next: Some(Ulid::from_bytes([2; 16])),
        ..node(vec![document(1, "a"), document(2, "a")])
    };
    let page = merge_pages([short, node(vec![document(3, "b")])], 10, 0);
    let ids: Vec<_> = page.documents.iter().map(|doc| doc.document_id).collect();
    assert_eq!(ids, [Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16])]);
    assert_eq!(page.next_after, Some(Ulid::from_bytes([2; 16])));
}

#[test]
fn preview_merges_nodes() {
    use super::{SetPreviewPage, merge_previews};
    // One document on two holders, and a node with unverified links: incomplete, merged once.
    let first = SetPreviewPage {
        page: node(vec![document(1, "a")]),
        ..SetPreviewPage::default()
    };
    let second = SetPreviewPage {
        page: node(vec![document(1, "b"), document(2, "a")]),
        unverified: 1,
        ..SetPreviewPage::default()
    };
    let preview = merge_previews([first, second], (2, 0), 10);
    assert_eq!(preview.documents.len(), 2);
    assert_eq!(preview.documents[0].links.len(), 2);
    assert!(!preview.complete());
    let settled = merge_previews([SetPreviewPage::default()], (1, 0), 10);
    assert!(settled.complete());
    let cut = merge_previews([SetPreviewPage::default()], (2, 1), 10);
    assert!(!cut.complete());
}
