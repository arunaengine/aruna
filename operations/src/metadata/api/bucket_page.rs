//! Merges the bucket pages of several nodes into one page with a cursor per node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{BucketPage, BucketSearchHit, MetadataApiError, NodeId};
use base64::Engine;
use serde::{Deserialize, Serialize};

/// Scan position of one node: the last bucket it gave or its sealed scan position, and whether
/// it reached its end.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct NodePosition {
    pub(super) node_id: [u8; 32],
    pub(super) after: Option<Vec<u8>>,
    pub(super) done: bool,
}

impl NodePosition {
    pub(super) fn of(positions: &[Self], node_id: NodeId) -> Option<&Self> {
        positions
            .iter()
            .find(|entry| entry.node_id == *node_id.as_bytes())
    }
}

/// Where every node's scan stands, and the distributed buckets already shown.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketCursor {
    pub(super) positions: Vec<NodePosition>,
    pub(super) shown: Vec<ShownSet>,
}

/// A distributed bucket a page showed, kept while one of its members may still reach it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ShownSet {
    pub(super) set_id: ulid::Ulid,
    pub(super) bucket: String,
}

impl From<Vec<NodePosition>> for BucketCursor {
    fn from(positions: Vec<NodePosition>) -> Self {
        Self {
            positions,
            shown: Vec::new(),
        }
    }
}

pub(super) fn decode_cursor(raw: Option<&str>) -> Result<BucketCursor, MetadataApiError> {
    let invalid = || MetadataApiError::InvalidCursor("invalid bucket search cursor".to_string());
    let Some(raw) = raw else {
        return Ok(BucketCursor::default());
    };
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| invalid())?;
    let cursor: BucketCursor = postcard::from_bytes(&bytes).map_err(|_| invalid())?;
    if (cursor.positions.iter()).any(|entry| NodeId::from_bytes(&entry.node_id).is_err()) {
        return Err(invalid());
    }
    Ok(cursor)
}

/// `None` once every node reached its end.
pub(super) fn encode_cursor(cursor: &BucketCursor) -> Option<String> {
    if cursor.positions.iter().all(|entry| entry.done) {
        return None;
    }
    let bytes = postcard::to_allocvec(cursor).ok()?;
    Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

/// Whether a node at `position` may still reach `bucket`: it is not done, and its position is
/// sealed, so unknown, or a bucket name before it.
fn may_reach(position: &NodePosition, bucket: &str) -> bool {
    !position.done
        && position.after.as_deref().is_none_or(|after| {
            crate::s3::object::search::sealed_position(after) || after < bucket.as_bytes()
        })
}

/// The nodes whose scan ended because the cursor's position no longer opens there.
pub(super) fn retired_nodes(parts: &[(NodeId, BucketPage)]) -> Vec<NodeId> {
    let retired = parts.iter().filter(|(_, page)| page.retired);
    retired.map(|(node_id, _)| *node_id).collect()
}

/// Orders node pages by bucket name and node, cuts them to `limit` and moves each node past
/// its hits on this page, or past its scan when it showed every hit and its scan was cut. A
/// node with fewer than `limit` hits, all shown and an uncut scan, is done. A failed node keeps
/// its position, a node left out by the fanout cap is kept for a later page, done nodes stay
/// done, and other nodes this page did not ask are dropped from the cursor. A distributed bucket
/// shows once: its other members count as given, so their nodes move past it too, and the
/// cursor remembers it while a member may still reach it on a later page.
pub(super) fn merge_pages(
    parts: Vec<(NodeId, BucketPage)>,
    (failed, omitted): (&[NodeId], &[NodeId]),
    cursor: BucketCursor,
    limit: usize,
) -> (Vec<BucketSearchHit>, BucketCursor) {
    let BucketCursor {
        mut positions,
        mut shown,
    } = cursor;
    let mut hits: Vec<_> = (parts.iter())
        .flat_map(|(_, page)| page.hits.clone())
        .collect();
    hits.sort_by(|left, right| {
        let node = |hit: &BucketSearchHit| *hit.node_id.as_bytes();
        (&left.bucket, node(left)).cmp(&(&right.bucket, node(right)))
    });
    // Members of one distributed bucket answer alike, so the first one stands for the set.
    let mut sets: std::collections::HashSet<_> = shown.iter().map(|set| set.set_id).collect();
    let mut page = Vec::with_capacity(limit);
    let mut given_hits = Vec::new();
    // A node gives only a prefix of its page: after its first hit left out, nothing more.
    let mut cut = std::collections::HashSet::new();
    for hit in hits {
        let repeat = hit.set_id.is_some_and(|set_id| sets.contains(&set_id));
        if cut.contains(&hit.node_id) {
            continue;
        }
        if !repeat && page.len() == limit {
            cut.insert(hit.node_id);
            continue;
        }
        if !repeat {
            sets.extend(hit.set_id);
            page.push(hit.clone());
        }
        given_hits.push(hit);
    }
    let nodes: Vec<NodeId> = parts
        .iter()
        .map(|(node, _)| *node)
        .chain(failed.iter().copied())
        .chain(omitted.iter().copied())
        .collect();
    positions
        .retain(|entry| entry.done || nodes.iter().any(|node| entry.node_id == *node.as_bytes()));
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
        let taken: Vec<_> = (given_hits.iter())
            .filter(|hit| hit.node_id == *node)
            .collect();
        let Some(entry) = positions
            .iter_mut()
            .find(|entry| entry.node_id == *node.as_bytes())
        else {
            continue;
        };
        let all_shown = taken.len() == given.hits.len();
        match (&given.scanned_to, taken.last()) {
            (Some(scanned_to), _) if all_shown => entry.after = Some(scanned_to.clone()),
            (_, Some(last)) => entry.after = Some(last.bucket.clone().into_bytes()),
            _ => {}
        }
        entry.done |=
            given.retired || (given.scanned_to.is_none() && given.hits.len() < limit && all_shown);
    }
    shown.extend(page.iter().filter_map(|hit| {
        let set_id = hit.set_id?;
        Some(ShownSet {
            set_id,
            bucket: hit.bucket.clone(),
        })
    }));
    shown.retain(|set| (positions.iter()).any(|position| may_reach(position, &set.bucket)));
    let cursor = BucketCursor { positions, shown };
    (page, cursor)
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
            set_id: None,
            metadata: None,
            public: false,
        }
    }

    /// Answers each node's buckets after its position, at most `limit`, like a node search.
    fn page(
        stores: &[(NodeId, Vec<&str>)],
        positions: &[NodePosition],
        limit: usize,
    ) -> Vec<(NodeId, BucketPage)> {
        stores
            .iter()
            .map(|(node_id, buckets)| {
                let entry = NodePosition::of(positions, *node_id);
                let after = entry.and_then(|entry| entry.after.clone());
                let hits = match entry.is_some_and(|entry| entry.done) {
                    true => Vec::new(),
                    false => buckets
                        .iter()
                        .filter(|bucket| {
                            after
                                .as_deref()
                                .is_none_or(|after| bucket.as_bytes() > after)
                        })
                        .take(limit)
                        .map(|bucket| hit(bucket, *node_id))
                        .collect(),
                };
                let page = BucketPage {
                    hits,
                    ..BucketPage::default()
                };
                (*node_id, page)
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
            let parts = page(&stores, &positions.positions, 3);
            let (hits, positions) = merge_pages(parts, (&[], &[]), positions, 3);
            assert!(hits.len() <= 3);
            seen.extend(hits.into_iter().map(|hit| hit.bucket));
            cursor = encode_cursor(&positions);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, ["a", "b", "c", "d", "e", "g"]);
    }

    // Two members of one set answer the same bucket; the page shows it once, and both
    // nodes still move past it.
    #[test]
    fn folds_set_members() {
        let set_id = ulid::Ulid::from_bytes([7; 16]);
        let member = |node_id| BucketSearchHit {
            set_id: Some(set_id),
            ..hit("refseq", node_id)
        };
        let parts = [node(1), node(2)].map(|node_id| {
            let hits = vec![
                member(node_id),
                hit(&format!("own-{}", node_id.fmt_short()), node_id),
            ];
            (
                node_id,
                BucketPage {
                    hits,
                    ..BucketPage::default()
                },
            )
        });

        let (hits, positions) = merge_pages(parts.to_vec(), (&[], &[]), Default::default(), 10);

        let names: Vec<_> = hits.iter().map(|hit| hit.bucket.as_str()).collect();
        assert_eq!(names.iter().filter(|name| **name == "refseq").count(), 1);
        assert_eq!(hits.len(), 3);
        assert!(positions.positions.iter().all(|entry| entry.done));
    }

    // A remembered member hit after a left-out hit of the same node is not given, so the
    // left-out bucket is not skipped for good.
    #[test]
    fn gives_only_prefix() {
        let set_id = ulid::Ulid::from_bytes([7; 16]);
        let member = BucketSearchHit {
            set_id: Some(set_id),
            ..hit("m", node(2))
        };
        let parts = vec![
            (
                node(1),
                BucketPage {
                    hits: vec![hit("a", node(1)), hit("b", node(1))],
                    ..BucketPage::default()
                },
            ),
            (
                node(2),
                BucketPage {
                    hits: vec![hit("c", node(2)), member],
                    ..BucketPage::default()
                },
            ),
        ];
        let cursor = BucketCursor {
            positions: Vec::new(),
            shown: vec![ShownSet {
                set_id,
                bucket: "m".to_string(),
            }],
        };
        let (hits, cursor) = merge_pages(parts, (&[], &[]), cursor, 2);
        let names: Vec<_> = hits.iter().map(|hit| hit.bucket.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        let second = NodePosition::of(&cursor.positions, node(2)).unwrap();
        assert_eq!((second.after.as_deref(), second.done), (None, false));
    }

    // With one hit per page, two members of one set still show the bucket only once.
    #[test]
    fn folds_across_pages() {
        let set_id = ulid::Ulid::from_bytes([7; 16]);
        let stores = [
            (node(1), vec!["a", "refseq"]),
            (node(2), vec!["refseq", "z"]),
        ];
        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let positions = decode_cursor(cursor.as_deref()).unwrap();
            let mut parts = page(&stores, &positions.positions, 1);
            for (_, page) in &mut parts {
                for hit in &mut page.hits {
                    hit.set_id = (hit.bucket == "refseq").then_some(set_id);
                }
            }
            let (hits, positions) = merge_pages(parts, (&[], &[]), positions, 1);
            assert!(hits.len() <= 1);
            seen.extend(hits.into_iter().map(|hit| hit.bucket));
            cursor = encode_cursor(&positions);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, ["a", "refseq", "z"]);
    }

    // A member whose scan was cut before a shown distributed bucket reaches it on a later
    // page; the cursor still knows the set, so the bucket shows once.
    #[test]
    fn remembers_shown_sets() {
        let set_id = ulid::Ulid::from_bytes([7; 16]);
        let member = |node_id| BucketSearchHit {
            set_id: Some(set_id),
            ..hit("refseq", node_id)
        };
        let sealed = crate::s3::object::search::seal_position(b"bucket-key").unwrap();
        let first = vec![
            (
                node(1),
                BucketPage {
                    hits: vec![member(node(1))],
                    ..BucketPage::default()
                },
            ),
            (
                node(2),
                BucketPage {
                    scanned_to: Some(sealed),
                    ..BucketPage::default()
                },
            ),
        ];
        let (hits, cursor) = merge_pages(first, (&[], &[]), Default::default(), 5);
        assert_eq!(hits.len(), 1);
        assert_eq!(cursor.shown.len(), 1);
        let cursor = decode_cursor(encode_cursor(&cursor).as_deref()).unwrap();

        let second = vec![(
            node(2),
            BucketPage {
                hits: vec![member(node(2)), hit("z", node(2))],
                ..BucketPage::default()
            },
        )];
        let (hits, cursor) = merge_pages(second, (&[], &[]), cursor, 5);
        let names: Vec<_> = hits.iter().map(|hit| hit.bucket.as_str()).collect();
        assert_eq!(names, ["z"]);
        // Every member is past the bucket now, so the cursor forgets it.
        assert!(cursor.shown.is_empty());
    }

    #[test]
    fn failed_node_resumes() {
        let stores = [(node(1), vec!["a", "b"])];
        let (hits, positions) = merge_pages(Vec::new(), (&[node(1)], &[]), Default::default(), 5);
        assert!(hits.is_empty());
        // The failed node is asked again from its start on the next page.
        let cursor = encode_cursor(&positions).expect("a failed node keeps the listing open");
        let positions = decode_cursor(Some(&cursor)).unwrap();
        let parts = page(&stores, &positions.positions, 5);
        let (hits, positions) = merge_pages(parts, (&[], &[]), positions, 5);
        assert_eq!(hits.len(), 2);
        assert_eq!(encode_cursor(&positions), None);
        assert!(decode_cursor(Some("not a cursor")).is_err());
    }

    #[test]
    fn drops_departed_nodes() {
        // A node that left the realm ends its listing instead of keeping the cursor open.
        let gone = NodePosition {
            node_id: *node(9).as_bytes(),
            after: Some(b"m".to_vec()),
            done: false,
        };
        let stores = [(node(1), vec!["a"])];
        let positions = vec![gone];
        let parts = page(&stores, &positions, 5);
        let (hits, positions) = merge_pages(parts, (&[], &[]), positions.into(), 5);
        assert_eq!(hits.len(), 1);
        assert!(NodePosition::of(&positions.positions, node(9)).is_none());
        assert_eq!(encode_cursor(&positions), None);
        // A failed node that is still a member keeps its position for the next page.
        let failed = NodePosition {
            node_id: *node(2).as_bytes(),
            after: Some(b"c".to_vec()),
            done: false,
        };
        let positions = vec![failed.clone()].into();
        let (_, positions) = merge_pages(Vec::new(), (&[node(2)], &[]), positions, 5);
        assert_eq!(positions.positions, [failed]);
    }

    #[test]
    fn keeps_cut_progress() {
        // A node whose scan was cut moves past its scan, not to its end.
        let cut = BucketPage {
            hits: vec![hit("b", node(1))],
            scanned_to: Some(b"k".to_vec()),
            retired: false,
        };
        let parts = vec![(node(1), cut)];
        let (hits, positions) = merge_pages(parts, (&[], &[]), Default::default(), 5);
        assert_eq!(hits.len(), 1);
        let entry = NodePosition::of(&positions.positions, node(1)).unwrap();
        assert_eq!(
            (entry.after.as_deref(), entry.done),
            (Some(&b"k"[..]), false)
        );
        // A node left out by the fanout cap stays in the cursor, and a done node stays done.
        let done = NodePosition {
            node_id: *node(3).as_bytes(),
            after: Some(b"z".to_vec()),
            done: true,
        };
        let parts = vec![(node(1), BucketPage::default())];
        let (_, positions) = merge_pages(parts, (&[], &[node(2)]), vec![done.clone()].into(), 5);
        let left = &positions.positions;
        assert!(NodePosition::of(left, node(2)).is_some_and(|entry| !entry.done));
        assert_eq!(NodePosition::of(left, node(3)), Some(&done));
        let cursor = encode_cursor(&positions).expect("the left-out node keeps the listing open");
        assert_eq!(decode_cursor(Some(&cursor)).unwrap(), positions);
    }

    #[test]
    fn many_nodes_resume() {
        // A cursor over more nodes than any fixed size allowed still decodes.
        let positions: Vec<_> = (0..400u16)
            .map(|index| NodePosition {
                node_id: *iroh::SecretKey::from_bytes(&[(index % 250) as u8 + 1; 32])
                    .public()
                    .as_bytes(),
                after: Some(format!("bucket-{index:04}-{}", "x".repeat(40)).into_bytes()),
                done: false,
            })
            .collect();
        let positions: BucketCursor = positions.into();
        let cursor = encode_cursor(&positions).unwrap();
        assert!(cursor.len() > 16 * 1024);
        assert_eq!(decode_cursor(Some(&cursor)).unwrap(), positions);
        assert!(decode_cursor(Some("not a cursor")).is_err());
    }

    #[test]
    fn retired_node_ends() {
        // A node whose position no longer opens ends its part, so the listing can end.
        let stale = NodePosition {
            node_id: *node(1).as_bytes(),
            after: Some(vec![0xFF, 1, 2]),
            done: false,
        };
        let retired = BucketPage {
            retired: true,
            ..BucketPage::default()
        };
        let parts = vec![(node(1), retired)];
        assert_eq!(retired_nodes(&parts), [node(1)]);
        let (hits, positions) = merge_pages(parts, (&[], &[]), vec![stale].into(), 5);
        assert!(hits.is_empty());
        assert!(NodePosition::of(&positions.positions, node(1)).is_some_and(|entry| entry.done));
        assert_eq!(encode_cursor(&positions), None);
    }
}
