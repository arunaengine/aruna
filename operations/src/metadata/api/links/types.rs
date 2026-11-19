//! Holds the link, part-of and set preview types and merges their node pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::metadata::api::{LinkDocument, LinkNodePage, SetPreviewPage};
use aruna_core::structs::storage::data::link::DataLink;
use ulid::Ulid;

/// Documents one page returns at most.
pub const LINKS_MAX_LIMIT: usize = 100;

/// The merged answer of every metadata node.
#[derive(Clone, Debug, Default, PartialEq)]
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

/// A set's deletion preview over every metadata node: each node's first page of the postings
/// that name the set, merged by document id.
#[derive(Clone, Debug, Default, PartialEq)]
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
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartOf {
    pub datasets: Vec<LinkDocument>,
    /// More datasets exist than this answer names.
    pub more: bool,
    pub complete: bool,
    pub hidden: bool,
}
