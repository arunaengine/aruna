use std::cmp::Ordering;
use std::collections::HashMap;

use aruna_core::NodeId;
use aruna_core::metadata::MetadataSearchHit;
use base64::Engine;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::api::MetadataApiQueryMode;

pub const METADATA_SEARCH_DEFAULT_PAGE_SIZE: usize = 25;
pub const METADATA_SEARCH_MAX_PAGE_SIZE: usize = 100;
pub const METADATA_SEARCH_MAX_PAGINATION_DEPTH: usize = 1000;

const SEARCH_CURSOR_VERSION: u8 = 1;

/// Sort key of the last hit emitted on a page, used as the exact resume point in
/// the merged, deduplicated ordering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchWatermark {
    pub score: f32,
    pub graph_iri: String,
    pub subject_iri: String,
}

/// Opaque, query-bound continuation token. Serialized with postcard and base64url
/// so it stays compact and URL-safe.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchCursor {
    pub version: u8,
    pub fingerprint: [u8; 32],
    pub watermark: SearchWatermark,
    pub resume: Vec<([u8; 32], u32)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SearchCursorError {
    #[error("invalid search cursor")]
    Invalid,
    #[error("search cursor does not match query")]
    QueryMismatch,
}

impl SearchCursor {
    pub fn new(
        fingerprint: [u8; 32],
        watermark: SearchWatermark,
        resume: Vec<(NodeId, u32)>,
    ) -> Self {
        Self {
            version: SEARCH_CURSOR_VERSION,
            fingerprint,
            watermark,
            resume: resume
                .into_iter()
                .map(|(node_id, position)| (*node_id.as_bytes(), position))
                .collect(),
        }
    }

    pub fn encode(&self) -> String {
        let bytes = postcard::to_allocvec(self).expect("search cursor serializes");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    pub fn decode(raw: &str) -> Result<Self, SearchCursorError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| SearchCursorError::Invalid)?;
        let cursor: SearchCursor =
            postcard::from_bytes(&bytes).map_err(|_| SearchCursorError::Invalid)?;
        if cursor.version != SEARCH_CURSOR_VERSION {
            return Err(SearchCursorError::Invalid);
        }
        Ok(cursor)
    }

    pub fn resume_positions(&self) -> HashMap<NodeId, u32> {
        self.resume
            .iter()
            .filter_map(|(bytes, position)| {
                NodeId::from_bytes(bytes)
                    .ok()
                    .map(|node_id| (node_id, *position))
            })
            .collect()
    }
}

/// Binds a cursor to the query that produced it. Recomputed on every continuation
/// request; a mismatch rejects the cursor. The query string itself is not stored.
pub fn query_fingerprint(
    query: &str,
    graph_iris: Option<&[String]>,
    mode: Option<MetadataApiQueryMode>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(query.as_bytes());
    hasher.update(&[0x00]);
    let mut graphs: Vec<&String> = graph_iris
        .map(|iris| iris.iter().collect())
        .unwrap_or_default();
    graphs.sort();
    graphs.dedup();
    for graph in graphs {
        hasher.update(graph.as_bytes());
        hasher.update(&[0x00]);
    }
    hasher.update(&[mode_byte(mode)]);
    *hasher.finalize().as_bytes()
}

fn mode_byte(mode: Option<MetadataApiQueryMode>) -> u8 {
    match mode {
        None => 0,
        Some(MetadataApiQueryMode::Local) => 1,
        Some(MetadataApiQueryMode::Distributed) => 2,
    }
}

/// Raw hits returned by one answering node, plus whether the node returned exactly
/// its requested limit (so it may still hold deeper results).
pub struct NodeSearchResult {
    pub node_id: NodeId,
    pub hits: Vec<MetadataSearchHit>,
    pub saturated: bool,
}

pub struct SearchPageCursor {
    pub watermark: SearchWatermark,
    pub resume: Vec<(NodeId, u32)>,
}

pub struct SearchPage {
    pub hits: Vec<MetadataSearchHit>,
    pub next: Option<SearchPageCursor>,
}

/// Deduplicate hits on `(graph_iri, subject_iri)` keeping the max score, preserving
/// title/snippet from whichever copy carries them, and order by score descending
/// with a `(graph_iri, subject_iri)` tie-break for a stable total order.
pub fn merge_search_hits(hits: Vec<MetadataSearchHit>) -> Vec<MetadataSearchHit> {
    let mut deduped: HashMap<(String, String), MetadataSearchHit> = HashMap::new();
    for hit in hits {
        let key = (hit.graph_iri.clone(), hit.subject_iri.clone());
        match deduped.get_mut(&key) {
            Some(existing) => {
                if hit.score > existing.score {
                    let mut winner = hit;
                    if winner.snippet.is_none() {
                        winner.snippet = existing.snippet.take();
                    }
                    *existing = winner;
                } else if existing.snippet.is_none() {
                    existing.snippet = hit.snippet;
                }
            }
            None => {
                deduped.insert(key, hit);
            }
        }
    }
    let mut hits: Vec<MetadataSearchHit> = deduped.into_values().collect();
    hits.sort_by(compare_hits);
    hits
}

fn compare_hits(left: &MetadataSearchHit, right: &MetadataSearchHit) -> Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.graph_iri.cmp(&right.graph_iri))
        .then_with(|| left.subject_iri.cmp(&right.subject_iri))
}

/// Turn merged node results into one page plus an optional continuation.
///
/// The `watermark` is the resume point in the merged, deduplicated order: every
/// hit at or above it was already emitted, so it is dropped here (coordinator-side
/// dedup-then-filter). Per-node resume positions size the next fetch. A page still
/// continues when a node was saturated even if this page added nothing, and paging
/// stops once the deepest resume reaches `max_depth`.
pub fn paginate(
    node_results: Vec<NodeSearchResult>,
    watermark: Option<SearchWatermark>,
    page_size: usize,
    max_depth: usize,
) -> SearchPage {
    let merged = merge_search_hits(
        node_results
            .iter()
            .flat_map(|node| node.hits.iter().cloned())
            .collect(),
    );

    let mut remaining: Vec<MetadataSearchHit> = match &watermark {
        Some(mark) => merged
            .into_iter()
            .filter(|hit| hit_after_watermark(hit, mark))
            .collect(),
        None => merged,
    };

    let page_len = remaining.len().min(page_size);
    let page: Vec<MetadataSearchHit> = remaining.drain(..page_len).collect();
    let leftover = !remaining.is_empty();
    let saturated = node_results.iter().any(|node| node.saturated);

    let next_watermark = page.last().map(watermark_of).or(watermark);
    let has_more = leftover || saturated;

    let next = if has_more {
        next_watermark.and_then(|mark| {
            let resume: Vec<(NodeId, u32)> = node_results
                .iter()
                .map(|node| {
                    let position = node
                        .hits
                        .iter()
                        .filter(|hit| !hit_after_watermark(hit, &mark))
                        .count() as u32;
                    (node.node_id, position)
                })
                .collect();
            let deepest = resume
                .iter()
                .map(|(_, position)| *position as usize)
                .max()
                .unwrap_or(0);
            if deepest >= max_depth {
                return None;
            }
            Some(SearchPageCursor {
                watermark: mark,
                resume,
            })
        })
    } else {
        None
    };

    SearchPage { hits: page, next }
}

/// Per-node fetch depth: resume position plus one page, defaulting unknown nodes to
/// the deepest known resume so a newly seen node is not asked too shallow.
pub fn resume_fetch_limit(
    resume: &HashMap<NodeId, u32>,
    node_id: NodeId,
    page_size: usize,
    max_depth: usize,
) -> usize {
    let base = match resume.get(&node_id) {
        Some(position) => *position as usize,
        None => resume.values().copied().max().unwrap_or(0) as usize,
    };
    (base + page_size).min(max_depth)
}

fn hit_after_watermark(hit: &MetadataSearchHit, watermark: &SearchWatermark) -> bool {
    watermark
        .score
        .total_cmp(&hit.score)
        .then_with(|| hit.graph_iri.cmp(&watermark.graph_iri))
        .then_with(|| hit.subject_iri.cmp(&watermark.subject_iri))
        == Ordering::Greater
}

fn watermark_of(hit: &MetadataSearchHit) -> SearchWatermark {
    SearchWatermark {
        score: hit.score,
        graph_iri: hit.graph_iri.clone(),
        subject_iri: hit.subject_iri.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn hit(graph: &str, subject: &str, score: f32) -> MetadataSearchHit {
        MetadataSearchHit {
            document_id: graph.to_string(),
            group_id: "01G".to_string(),
            document_path: format!("datasets/{graph}"),
            graph_iri: format!("https://w3id.org/aruna/{graph}"),
            subject_iri: subject.to_string(),
            score,
            title: subject.to_string(),
            snippet: None,
        }
    }

    #[test]
    fn cursor_roundtrips_with_node_keys_and_exact_scores() {
        let cursor = SearchCursor::new(
            [7u8; 32],
            SearchWatermark {
                score: 0.8,
                graph_iri: "https://w3id.org/aruna/01A".to_string(),
                subject_iri: "./file.txt".to_string(),
            },
            vec![(node_id(1), 3), (node_id(2), 0)],
        );
        let decoded = SearchCursor::decode(&cursor.encode()).unwrap();
        assert_eq!(decoded, cursor);
        assert_eq!(decoded.watermark.score.to_bits(), 0.8f32.to_bits());
        let positions = decoded.resume_positions();
        assert_eq!(positions.get(&node_id(1)), Some(&3));
        assert_eq!(positions.get(&node_id(2)), Some(&0));
    }

    #[test]
    fn cursor_decode_rejects_garbage_and_wrong_version() {
        assert_eq!(
            SearchCursor::decode("not*base64"),
            Err(SearchCursorError::Invalid)
        );
        assert_eq!(
            SearchCursor::decode("QUJD"),
            Err(SearchCursorError::Invalid)
        );

        let mut cursor = SearchCursor::new(
            [0u8; 32],
            SearchWatermark {
                score: 1.0,
                graph_iri: "g".to_string(),
                subject_iri: "s".to_string(),
            },
            Vec::new(),
        );
        cursor.version = 2;
        assert_eq!(
            SearchCursor::decode(&cursor.encode()),
            Err(SearchCursorError::Invalid)
        );
    }

    #[test]
    fn fingerprint_binds_query_graphs_and_mode() {
        let base = query_fingerprint("alpha", None, Some(MetadataApiQueryMode::Distributed));
        assert_eq!(
            base,
            query_fingerprint("alpha", None, Some(MetadataApiQueryMode::Distributed))
        );
        assert_ne!(
            base,
            query_fingerprint("beta", None, Some(MetadataApiQueryMode::Distributed))
        );
        assert_ne!(
            base,
            query_fingerprint(
                "alpha",
                Some(&["g".to_string()]),
                Some(MetadataApiQueryMode::Distributed)
            )
        );
        assert_ne!(
            base,
            query_fingerprint("alpha", None, Some(MetadataApiQueryMode::Local))
        );
    }

    #[test]
    fn merge_keeps_max_score_and_enriched_snippet() {
        let mut bare = hit("01A", "./file.txt", 0.5);
        bare.snippet = None;
        let mut enriched = hit("01A", "./file.txt", 0.8);
        enriched.snippet = Some("matched text".to_string());

        let merged = merge_search_hits(vec![bare, enriched]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].score, 0.8);
        assert_eq!(merged[0].snippet.as_deref(), Some("matched text"));

        // Enrichment survives even when the lower-scored copy is the enriched one.
        let mut top = hit("01B", "./file.txt", 0.9);
        top.snippet = None;
        let mut low = hit("01B", "./file.txt", 0.2);
        low.snippet = Some("kept".to_string());
        let merged = merge_search_hits(vec![top, low]);
        assert_eq!(merged[0].score, 0.9);
        assert_eq!(merged[0].snippet.as_deref(), Some("kept"));
    }

    #[test]
    fn merge_orders_by_score_then_keys() {
        let merged = merge_search_hits(vec![
            hit("01B", "./file-b.txt", 0.7),
            hit("01A", "./file-b.txt", 0.7),
            hit("01A", "./file-a.txt", 0.7),
            hit("01C", "./file-c.txt", 0.9),
        ]);
        let keys: Vec<_> = merged
            .iter()
            .map(|h| (h.graph_iri.as_str(), h.subject_iri.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("https://w3id.org/aruna/01C", "./file-c.txt"),
                ("https://w3id.org/aruna/01A", "./file-a.txt"),
                ("https://w3id.org/aruna/01A", "./file-b.txt"),
                ("https://w3id.org/aruna/01B", "./file-b.txt"),
            ]
        );
    }

    #[test]
    fn paginate_first_page_sets_watermark_and_resume() {
        let node = NodeSearchResult {
            node_id: node_id(1),
            hits: vec![
                hit("01A", "./a", 0.9),
                hit("01B", "./b", 0.8),
                hit("01C", "./c", 0.7),
            ],
            saturated: true,
        };
        let page = paginate(vec![node], None, 2, METADATA_SEARCH_MAX_PAGINATION_DEPTH);
        assert_eq!(page.hits.len(), 2);
        assert_eq!(page.hits[0].subject_iri, "./a");
        assert_eq!(page.hits[1].subject_iri, "./b");
        let next = page.next.expect("more pages remain");
        assert_eq!(next.watermark.subject_iri, "./b");
        // Two raw hits are at or above the watermark, so resume past them.
        assert_eq!(next.resume, vec![(node_id(1), 2)]);
    }

    #[test]
    fn paginate_second_page_drops_already_emitted_and_terminates() {
        let hits = vec![
            hit("01A", "./a", 0.9),
            hit("01B", "./b", 0.8),
            hit("01C", "./c", 0.7),
        ];
        let watermark = SearchWatermark {
            score: 0.8,
            graph_iri: "https://w3id.org/aruna/01B".to_string(),
            subject_iri: "./b".to_string(),
        };
        let node = NodeSearchResult {
            node_id: node_id(1),
            hits,
            saturated: false,
        };
        let page = paginate(
            vec![node],
            Some(watermark),
            2,
            METADATA_SEARCH_MAX_PAGINATION_DEPTH,
        );
        assert_eq!(page.hits.len(), 1);
        assert_eq!(page.hits[0].subject_iri, "./c");
        assert!(page.next.is_none());
    }

    #[test]
    fn paginate_dedups_hit_present_on_two_nodes() {
        let left = NodeSearchResult {
            node_id: node_id(1),
            hits: vec![hit("01A", "./shared", 0.9), hit("01B", "./l", 0.6)],
            saturated: false,
        };
        let right = NodeSearchResult {
            node_id: node_id(2),
            hits: vec![hit("01A", "./shared", 0.5), hit("01C", "./r", 0.7)],
            saturated: false,
        };
        let page = paginate(
            vec![left, right],
            None,
            1,
            METADATA_SEARCH_MAX_PAGINATION_DEPTH,
        );
        assert_eq!(page.hits.len(), 1);
        assert_eq!(page.hits[0].subject_iri, "./shared");
        assert_eq!(page.hits[0].score, 0.9);
        let next = page.next.unwrap();
        let resume: HashMap<_, _> = next.resume.into_iter().collect();
        // Resume counts each node's raw hits at or above the watermark by their
        // local score. Node 1 owns the winning 0.9 copy, so it resumes past it;
        // node 2's 0.5 copy sorts below the merged 0.9 watermark and counts zero.
        assert_eq!(resume.get(&node_id(1)), Some(&1));
        assert_eq!(resume.get(&node_id(2)), Some(&0));
    }

    #[test]
    fn paginate_continues_when_a_node_is_saturated_without_new_hits() {
        let watermark = SearchWatermark {
            score: 0.9,
            graph_iri: "https://w3id.org/aruna/01A".to_string(),
            subject_iri: "./a".to_string(),
        };
        let node = NodeSearchResult {
            node_id: node_id(1),
            hits: vec![hit("01A", "./a", 0.9)],
            saturated: true,
        };
        let page = paginate(
            vec![node],
            Some(watermark.clone()),
            2,
            METADATA_SEARCH_MAX_PAGINATION_DEPTH,
        );
        assert!(page.hits.is_empty());
        let next = page.next.expect("saturation keeps paging");
        assert_eq!(next.watermark, watermark);
        assert_eq!(next.resume, vec![(node_id(1), 1)]);
    }

    #[test]
    fn paginate_churn_does_not_re_emit_or_duplicate() {
        let watermark = SearchWatermark {
            score: 0.8,
            graph_iri: "https://w3id.org/aruna/01B".to_string(),
            subject_iri: "./b".to_string(),
        };
        // A higher-scored hit appears between pages; it sorts above the watermark
        // and must be suppressed rather than duplicated onto a later page.
        let node = NodeSearchResult {
            node_id: node_id(1),
            hits: vec![
                hit("01Z", "./new", 0.95),
                hit("01A", "./a", 0.9),
                hit("01C", "./c", 0.7),
            ],
            saturated: false,
        };
        let page = paginate(
            vec![node],
            Some(watermark),
            5,
            METADATA_SEARCH_MAX_PAGINATION_DEPTH,
        );
        let subjects: Vec<_> = page.hits.iter().map(|h| h.subject_iri.as_str()).collect();
        assert_eq!(subjects, vec!["./c"]);
    }

    #[test]
    fn paginate_stops_at_depth_cap() {
        let node = NodeSearchResult {
            node_id: node_id(1),
            hits: vec![hit("01A", "./a", 0.9), hit("01B", "./b", 0.8)],
            saturated: true,
        };
        let page = paginate(vec![node], None, 1, 1);
        assert_eq!(page.hits.len(), 1);
        // Deepest resume (1) has reached the cap, so no continuation is offered.
        assert!(page.next.is_none());
    }

    #[test]
    fn resume_fetch_limit_defaults_unknown_nodes_to_deepest() {
        let mut resume = HashMap::new();
        resume.insert(node_id(1), 4);
        resume.insert(node_id(2), 7);
        assert_eq!(resume_fetch_limit(&resume, node_id(1), 3, 1000), 7);
        // Unknown node uses the deepest known resume plus a page.
        assert_eq!(resume_fetch_limit(&resume, node_id(9), 3, 1000), 10);
        // Empty resume is the first-page case.
        assert_eq!(resume_fetch_limit(&HashMap::new(), node_id(9), 3, 1000), 3);
        // Depth cap clamps the fetch.
        assert_eq!(resume_fetch_limit(&resume, node_id(2), 100, 25), 25);
    }
}
