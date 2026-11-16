//! Holds the link, part-of and set preview types and merges their node pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::structs::storage::data::link::{DataLink, LinkScope};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Documents one page returns at most.
pub const LINKS_MAX_LIMIT: usize = 100;

/// One identifier, or the prefixes of a key a bucket's prefix postings are probed for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkQuery {
    Exact(DataLink),
    KeyPrefixes { scope: LinkScope, key: String },
}

/// What one metadata node is asked: the queries, the page position and size.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkNodeRequest {
    pub queries: Vec<LinkQuery>,
    /// Only postings of linked parts (Dataset-typed `hasPart` of a root) count.
    pub parts_only: bool,
    /// Documents after this id; documents sort by id on every node.
    pub after: Option<Ulid>,
    pub limit: usize,
    /// A document whose link is reported by `confirmed` even if the caller may not read it.
    pub confirm: Option<Ulid>,
}

/// One link of a document to one of the queried identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkHit {
    pub id: String,
    pub part: bool,
    pub subject_iris: Vec<String>,
}

/// One readable document that links the target.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkDocument {
    pub document_id: Ulid,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub title: Option<String>,
    pub links: Vec<LinkHit>,
}

/// One node's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkNodePage {
    pub documents: Vec<LinkDocument>,
    /// Every matching document up to this id was examined, readable or not, and more may
    /// follow it; `None` once nothing follows.
    pub next: Option<Ulid>,
    /// The budget ran out before every queried identifier was read.
    pub cut: bool,
    /// A matching document exists that the caller may not read.
    pub hidden: bool,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
    /// Documents whose links were cut at the projection limit; any of them may match.
    pub truncated_documents: usize,
    /// The requested `confirm` document matches but the caller may not read it.
    pub confirmed: bool,
}

/// The merged answer of every metadata node.
#[derive(Clone, Debug, Default)]
pub struct LinkPage {
    pub documents: Vec<LinkDocument>,
    pub next_after: Option<Ulid>,
    /// Every node answered with every posting read and no materialization unsettled.
    pub complete: bool,
    pub hidden: bool,
    pub nodes_failed: usize,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
    pub truncated_documents: usize,
    pub confirmed: bool,
}

/// One node's page of the documents that link a distributed set: its prefix links and its
/// verified version links, in posting key order. For the set's deletion preview.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetPreviewPage {
    pub page: LinkNodePage,
    /// The posting key to continue after; `None` once both ranges are read.
    pub next: Option<Vec<u8>>,
    /// Version links on this node that no member could verify, counted up to a cap; while
    /// any exist the preview is incomplete.
    pub unverified: usize,
}

/// A set's deletion preview over every metadata node: each node's first page of the postings
/// that name the set, merged by document id.
#[derive(Clone, Debug, Default)]
pub struct SetPreview {
    pub documents: Vec<LinkDocument>,
    /// A node holds more postings of the set than its first page.
    pub more: bool,
    pub hidden: bool,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub unverified: usize,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
    pub truncated_documents: usize,
}

impl SetPreview {
    /// Every node answered its whole set with nothing unsettled, unverified or truncated.
    pub fn complete(&self) -> bool {
        !self.more
            && self.nodes_failed == 0
            && self.unverified == 0
            && self.pending_materializations == 0
            && self.failed_materializations == 0
            && self.truncated_documents == 0
    }
}

/// Merges node previews by document id, keeping the first `limit` documents.
pub fn merge_previews(
    pages: impl IntoIterator<Item = SetPreviewPage>,
    (nodes_queried, nodes_failed): (usize, usize),
    limit: usize,
) -> SetPreview {
    let mut preview = SetPreview {
        nodes_queried,
        nodes_failed,
        ..SetPreview::default()
    };
    let mut merged = BTreeMap::<Ulid, LinkDocument>::new();
    for node in pages {
        preview.more |= node.next.is_some();
        preview.hidden |= node.page.hidden;
        preview.unverified += node.unverified;
        preview.pending_materializations += node.page.pending_materializations;
        preview.failed_materializations += node.page.failed_materializations;
        preview.truncated_documents += node.page.truncated_documents;
        for document in node.page.documents {
            let entry = merged
                .entry(document.document_id)
                .or_insert_with(|| LinkDocument {
                    links: Vec::new(),
                    ..document.clone()
                });
            for link in document.links {
                if !entry.links.contains(&link) {
                    entry.links.push(link);
                }
            }
        }
    }
    preview.more |= merged.len() > limit;
    preview.documents = merged.into_values().take(limit).collect();
    preview
}

/// Merges node pages: one entry per document, the first `limit` documents by id.
pub fn merge_pages(
    pages: impl IntoIterator<Item = LinkNodePage>,
    limit: usize,
    nodes_failed: usize,
) -> LinkPage {
    let mut merged = BTreeMap::<Ulid, LinkDocument>::new();
    let mut page = LinkPage {
        nodes_failed,
        ..LinkPage::default()
    };
    // Every node examined everything up to its own end; together, up to the smallest one.
    let mut end: Option<Ulid> = None;
    let mut cut = false;
    for node in pages {
        if let Some(next) = node.next {
            end = Some(end.map_or(next, |end| end.min(next)));
        }
        cut |= node.cut;
        page.hidden |= node.hidden;
        page.confirmed |= node.confirmed;
        page.pending_materializations += node.pending_materializations;
        page.failed_materializations += node.failed_materializations;
        page.truncated_documents += node.truncated_documents;
        for document in node.documents {
            let entry = merged
                .entry(document.document_id)
                .or_insert_with(|| LinkDocument {
                    links: Vec::new(),
                    ..document.clone()
                });
            for link in document.links {
                if !entry.links.contains(&link) {
                    entry.links.push(link);
                }
            }
        }
    }
    if let Some(end) = end {
        merged.retain(|id, _| *id <= end);
    }
    if merged.len() > limit {
        end = merged.keys().nth(limit.saturating_sub(1)).copied();
    }
    page.documents = merged.into_values().take(limit).collect();
    // Hidden documents count as examined, so a page of only hidden ones still moves on.
    page.next_after = end;
    page.complete = end.is_none()
        && !cut
        && nodes_failed == 0
        && page.pending_materializations == 0
        && page.failed_materializations == 0
        && page.truncated_documents == 0;
    page
}

/// Where a linked part's files are browsed: the bucket name, its prefix, and whether this
/// node holds the bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartTarget {
    pub link: DataLink,
    pub bucket: Option<String>,
    pub prefix: String,
    /// The node of an ordinary bucket; a distributed bucket is browsed on any member.
    pub node_id: Option<NodeId>,
    pub held_here: bool,
    /// The permission paths whose READ lets a caller list the part, as far as this node knows.
    pub read_paths: Vec<String>,
}

/// At most this many datasets a file names as derived `isPartOf`; the paged listing has all.
pub const PART_OF_LIMIT: usize = 10;

/// The datasets whose live linked parts contain a file: its derived `isPartOf`, computed now
/// and never stored per file.
#[derive(Clone, Debug, Default)]
pub struct PartOf {
    pub datasets: Vec<LinkDocument>,
    /// More datasets exist than this answer names.
    pub more: bool,
    pub complete: bool,
    pub hidden: bool,
}
