//! Merges the bucket pages of several nodes into one page with a cursor per node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{BucketSearchHit, MetadataApiError, NodeId, QUERY_MAX_NODES};
use base64::Engine;
use serde::{Deserialize, Serialize};

const CURSOR_MAX_BYTES: usize = 16 * 1024;

/// Scan position of one node: the last bucket it gave, and whether it reached its end.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct NodePosition {
    pub(super) node_id: [u8; 32],
    pub(super) after: Option<String>,
    pub(super) done: bool,
}

impl NodePosition {
    pub(super) fn of(positions: &[Self], node_id: NodeId) -> Option<&Self> {
        positions
            .iter()
            .find(|entry| entry.node_id == *node_id.as_bytes())
    }
}

pub(super) fn decode_cursor(raw: Option<&str>) -> Result<Vec<NodePosition>, MetadataApiError> {
    let invalid = || MetadataApiError::InvalidCursor("invalid bucket search cursor".to_string());
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.len() > CURSOR_MAX_BYTES {
        return Err(invalid());
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| invalid())?;
    let positions: Vec<NodePosition> = postcard::from_bytes(&bytes).map_err(|_| invalid())?;
    if positions.len() > QUERY_MAX_NODES
        || positions
            .iter()
            .any(|entry| NodeId::from_bytes(&entry.node_id).is_err())
    {
        return Err(invalid());
    }
    Ok(positions)
}

/// `None` once every node reached its end.
pub(super) fn encode_cursor(positions: &[NodePosition]) -> Option<String> {
    if positions.iter().all(|entry| entry.done) {
        return None;
    }
    let bytes = postcard::to_allocvec(positions).ok()?;
    Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

/// Orders all node pages by bucket name and node, cuts them to `limit` and moves each node
/// past the hits it gave to this page. A node that gave fewer than `limit` hits, all on this
/// page, is done; a failed node keeps its position.
pub(super) fn merge_pages(
    parts: Vec<(NodeId, Vec<BucketSearchHit>)>,
    failed: &[NodeId],
    mut positions: Vec<NodePosition>,
    limit: usize,
) -> (Vec<BucketSearchHit>, Vec<NodePosition>) {
    let mut hits: Vec<_> = parts.iter().flat_map(|(_, hits)| hits.clone()).collect();
    hits.sort_by(|left, right| {
        let node = |hit: &BucketSearchHit| *hit.node_id.as_bytes();
        (&left.bucket, node(left)).cmp(&(&right.bucket, node(right)))
    });
    hits.truncate(limit);
    let nodes = parts
        .iter()
        .map(|(node, _)| *node)
        .chain(failed.iter().copied());
    for node in nodes {
        if NodePosition::of(&positions, node).is_none() {
            positions.push(NodePosition {
                node_id: *node.as_bytes(),
                after: None,
                done: false,
            });
        }
    }
    for (node, given) in &parts {
        let taken: Vec<_> = hits.iter().filter(|hit| hit.node_id == *node).collect();
        let Some(entry) = positions
            .iter_mut()
            .find(|entry| entry.node_id == *node.as_bytes())
        else {
            continue;
        };
        if let Some(last) = taken.last() {
            entry.after = Some(last.bucket.clone());
        }
        entry.done |= given.len() < limit && taken.len() == given.len();
    }
    (hits, positions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::types::GroupId;
    use std::time::SystemTime;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn hit(bucket: &str, node_id: NodeId) -> BucketSearchHit {
        BucketSearchHit {
            arn: String::new(),
            bucket: bucket.to_string(),
            node_id,
            group_id: GroupId::nil(),
            group_name: None,
            created_at: SystemTime::UNIX_EPOCH,
        }
    }

    /// Answers each node's buckets after its position, at most `limit`, like a node search.
    fn page(
        stores: &[(NodeId, Vec<&str>)],
        positions: &[NodePosition],
        limit: usize,
    ) -> Vec<(NodeId, Vec<BucketSearchHit>)> {
        stores
            .iter()
            .map(|(node_id, buckets)| {
                let entry = NodePosition::of(positions, *node_id);
                let after = entry.and_then(|entry| entry.after.clone());
                let hits = match entry.is_some_and(|entry| entry.done) {
                    true => Vec::new(),
                    false => buckets
                        .iter()
                        .filter(|bucket| after.as_deref().is_none_or(|after| **bucket > after))
                        .take(limit)
                        .map(|bucket| hit(bucket, *node_id))
                        .collect(),
                };
                (*node_id, hits)
            })
            .collect()
    }

    #[test]
    fn pages_across_nodes() {
        let stores = [
            (node(1), vec!["a", "c", "e", "g"]),
            (node(2), vec!["b", "d"]),
        ];
        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let positions = decode_cursor(cursor.as_deref()).unwrap();
            let parts = page(&stores, &positions, 3);
            let (hits, positions) = merge_pages(parts, &[], positions, 3);
            assert!(hits.len() <= 3);
            seen.extend(hits.into_iter().map(|hit| hit.bucket));
            cursor = encode_cursor(&positions);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, ["a", "b", "c", "d", "e", "g"]);
    }

    #[test]
    fn failed_node_resumes() {
        let stores = [(node(1), vec!["a", "b"])];
        let (hits, positions) = merge_pages(Vec::new(), &[node(1)], Vec::new(), 5);
        assert!(hits.is_empty());
        // The failed node is asked again from its start on the next page.
        let cursor = encode_cursor(&positions).expect("a failed node keeps the listing open");
        let positions = decode_cursor(Some(&cursor)).unwrap();
        let (hits, positions) = merge_pages(page(&stores, &positions, 5), &[], positions, 5);
        assert_eq!(hits.len(), 2);
        assert_eq!(encode_cursor(&positions), None);
        assert!(decode_cursor(Some("not a cursor")).is_err());
    }
}
