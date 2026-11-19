//! Plans object searches, reads and builds their page cursors, and assembles their pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::search_types::{
    ObjectCursor, ObjectCursorPartition, ObjectCursorPayload, ObjectExecution,
    ObjectPartitionCoverage, ObjectPartitionState, ObjectQueryMode, PendingBucket, RangeLayout,
    SearchQueryRequest,
};
use super::{
    AuthContext, BlobHeadKey, CursorEnvelopeError, HashMap, HashSet, MetadataApiError,
    MetadataFanoutStats, NodeId, ObjectKeyMatch, QUERY_MAX_NODES, RealmId, SearchNodePage,
    SignedCursor, SystemTime, object_search_fingerprint, select_fanout_nodes,
};
use crate::distribution::feed_walks::IndexProgress;
use crate::metadata::search_cursor::blank_signature;
use crate::s3::object::search_scan::changed_until;
use aruna_core::cursor::OBJECT_CURSOR_CONTEXT;

const OBJECT_CURSOR_VERSION: u8 = 1;

pub(super) const CURSOR_MAX_BYTES: usize = 64 * 1024;

const SEARCH_MAX_BYTES: usize = 2 * 1024;

/// Skipped distributed buckets one realm-wide page searches by name; the rest wait in its cursor.
pub(super) const MAX_SKIPPED_SEARCHES: usize = 8;

impl SignedCursor<ObjectCursorPayload> {
    pub(super) fn decode(
        raw: &str,
        fingerprint: [u8; 32],
        authorized_signers: &[NodeId],
    ) -> Result<Self, MetadataApiError> {
        if raw.len() > CURSOR_MAX_BYTES {
            return Err(MetadataApiError::InvalidCursor(
                "invalid object search cursor".to_string(),
            ));
        }
        let cursor =
            Self::decode_verified(raw, OBJECT_CURSOR_CONTEXT, authorized_signers, |cursor| {
                if cursor.version != OBJECT_CURSOR_VERSION || cursor.fingerprint != fingerprint {
                    Err(CursorEnvelopeError::QueryMismatch)
                } else {
                    Ok(())
                }
            })
            .map_err(|error| match error {
                CursorEnvelopeError::Invalid => {
                    MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
                }
                CursorEnvelopeError::QueryMismatch => MetadataApiError::InvalidCursor(
                    "object search cursor does not match query".to_string(),
                ),
            })?;
        if cursor.payload.partitions.len() > QUERY_MAX_NODES
            || cursor.payload.failed_partitions.len() > QUERY_MAX_NODES
        {
            return Err(MetadataApiError::InvalidCursor(
                "invalid object search cursor".to_string(),
            ));
        }
        let mut nodes = HashSet::new();
        for partition in &cursor.payload.partitions {
            NodeId::from_bytes(&partition.node_id).map_err(|_| {
                MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
            })?;
            if !nodes.insert(partition.node_id)
                || partition.start_after.as_ref().is_some_and(|key| {
                    // A sealed position is checked by the node that sealed it.
                    let plain = !crate::s3::object::search_scan::sealed_position(key);
                    key.len() > SEARCH_MAX_BYTES || plain && BlobHeadKey::from_bytes(key).is_err()
                })
            {
                return Err(MetadataApiError::InvalidCursor(
                    "invalid object search cursor".to_string(),
                ));
            }
        }
        for node_id in &cursor.payload.failed_partitions {
            NodeId::from_bytes(node_id).map_err(|_| {
                MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
            })?;
            if !nodes.insert(*node_id) {
                return Err(MetadataApiError::InvalidCursor(
                    "invalid object search cursor".to_string(),
                ));
            }
        }
        if cursor.payload.pending.is_empty()
            && !cursor
                .payload
                .partitions
                .iter()
                .any(|partition| !partition.exhausted)
        {
            return Err(MetadataApiError::InvalidCursor(
                "exhausted object search cursor".to_string(),
            ));
        }
        Ok(cursor)
    }

    pub(super) fn partition_states(&self) -> Result<Vec<ObjectPartitionState>, MetadataApiError> {
        self.payload
            .partitions
            .iter()
            .map(|partition| {
                Ok(ObjectPartitionState {
                    node_id: NodeId::from_bytes(&partition.node_id).map_err(|_| {
                        MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
                    })?,
                    start_after: partition.start_after.clone(),
                    exhausted: partition.exhausted,
                    observed_at: partition.observed_at,
                })
            })
            .collect()
    }

    pub(super) fn failed_nodes(&self) -> Result<Vec<NodeId>, MetadataApiError> {
        self.payload
            .failed_partitions
            .iter()
            .map(|node_id| {
                NodeId::from_bytes(node_id).map_err(|_| {
                    MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_signed(
        fingerprint: [u8; 32],
        as_of: SystemTime,
        partitions: &[ObjectPartitionState],
        failed_partitions: &[NodeId],
        discovery_failed: bool,
        omitted_partitions: usize,
        pending: &[PendingBucket],
        (changed_keys, layout): (Option<usize>, RangeLayout),
        signer: NodeId,
        sign: impl FnOnce(&[u8]) -> iroh::Signature,
    ) -> Result<Self, postcard::Error> {
        let partitions: Vec<ObjectCursorPartition> = partitions
            .iter()
            .map(|partition| ObjectCursorPartition {
                node_id: *partition.node_id.as_bytes(),
                start_after: partition.start_after.clone(),
                exhausted: partition.exhausted,
                observed_at: partition.observed_at,
            })
            .collect();
        let failed_partitions: Vec<[u8; 32]> = failed_partitions
            .iter()
            .map(|node_id| *node_id.as_bytes())
            .collect();
        Self::build_signed(
            OBJECT_CURSOR_VERSION,
            OBJECT_CURSOR_CONTEXT,
            fingerprint,
            ObjectCursorPayload {
                as_of,
                partitions,
                failed_partitions,
                discovery_failed,
                omitted_partitions,
                pending: pending.to_vec(),
                changed_keys,
                layout,
            },
            signer,
            sign,
        )
    }
}

pub(super) struct ObjectSearchPlan {
    pub(super) auth: AuthContext,
    pub(super) bearer_token: Option<String>,
    pub(super) query: String,
    pub(super) key_match: ObjectKeyMatch,
    pub(super) bucket: Option<String>,
    pub(super) limit: usize,
    pub(super) cursor: Option<String>,
    pub(super) mode: ObjectQueryMode,
    pub(super) target_nodes: Option<Vec<NodeId>>,
    pub(super) fingerprint: [u8; 32],
}

pub(super) struct ObjectSearchPartitions {
    pub(super) as_of: SystemTime,
    pub(super) partitions: Vec<ObjectPartitionState>,
    pub(super) failed_partitions: Vec<NodeId>,
    pub(super) discovery_failed: bool,
    pub(super) omitted_partitions: usize,
    pub(super) pending: Vec<PendingBucket>,
    /// Changed keys the earlier pages passed.
    pub(super) changed_keys: Option<usize>,
    pub(super) layout: RangeLayout,
}

/// The distributed buckets a realm-wide page skipped, once each. They no longer count as
/// unsearched: the page searches them through their range indexes.
pub(super) fn take_skipped(parts: &mut [(NodeId, SearchNodePage)]) -> Vec<String> {
    let mut skipped = Vec::new();
    for (_, page) in parts.iter_mut() {
        page.incomplete_ranges =
            (page.incomplete_ranges).saturating_sub(page.skipped_buckets.len());
        skipped.append(&mut page.skipped_buckets);
    }
    skipped.sort_unstable();
    skipped.dedup();
    skipped
}

pub(super) fn plan_object_search(
    realm_id: RealmId,
    request: SearchQueryRequest,
) -> Result<ObjectSearchPlan, MetadataApiError> {
    if request.query.is_empty() || request.auth.realm_id != realm_id {
        return Err(if request.query.is_empty() {
            MetadataApiError::BadRequest
        } else {
            MetadataApiError::Forbidden
        });
    }
    let limit = request
        .limit
        .clamp(1, crate::s3::object::search::SEARCH_MAX_LIMIT);
    let fingerprint = object_search_fingerprint(
        realm_id,
        &request.query,
        request.key_match,
        request.bucket.as_deref(),
        request.mode,
    );
    Ok(ObjectSearchPlan {
        auth: request.auth,
        bearer_token: request.bearer_token,
        query: request.query,
        key_match: request.key_match,
        bucket: request.bucket,
        limit,
        cursor: request.cursor,
        mode: request.mode,
        target_nodes: request.target_nodes,
        fingerprint,
    })
}

/// The partitions a cursor resumes, signed by one of `signers`.
pub(super) fn cursor_partitions(
    plan: &ObjectSearchPlan,
    local_node_id: NodeId,
    raw: &str,
    signers: &[NodeId],
) -> Result<ObjectSearchPartitions, MetadataApiError> {
    let cursor = ObjectCursor::decode(raw, plan.fingerprint, signers)?;
    let partitions = cursor.partition_states()?;
    if plan.mode == ObjectQueryMode::Local
        && (partitions.len() != 1 || partitions[0].node_id != local_node_id)
    {
        return Err(MetadataApiError::InvalidCursor(
            "invalid local object search cursor".to_string(),
        ));
    }
    checked_partitions(
        plan,
        ObjectSearchPartitions {
            as_of: cursor.payload.as_of,
            partitions,
            failed_partitions: cursor.failed_nodes()?,
            discovery_failed: cursor.payload.discovery_failed,
            omitted_partitions: cursor.payload.omitted_partitions,
            pending: cursor.payload.pending.clone(),
            changed_keys: cursor.payload.changed_keys,
            layout: cursor.payload.layout.clone(),
        },
    )
}

/// The partitions of a first page over `nodes`, at most `QUERY_MAX_NODES` of them by rank.
pub(super) fn fresh_partitions(
    plan: &ObjectSearchPlan,
    local_node_id: NodeId,
    (mut nodes, discovery_failed): (Vec<NodeId>, bool),
    as_of: SystemTime,
) -> Result<ObjectSearchPartitions, MetadataApiError> {
    if nodes.is_empty() {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let omitted_partitions = if nodes.len() > QUERY_MAX_NODES {
        let selected = select_fanout_nodes(&nodes, local_node_id, &plan.fingerprint);
        let omitted = nodes.len().saturating_sub(selected.len());
        nodes = selected;
        omitted
    } else {
        0
    };
    nodes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let partitions = (nodes.into_iter())
        .map(|node_id| ObjectPartitionState {
            node_id,
            start_after: None,
            exhausted: false,
            observed_at: None,
        })
        .collect();
    checked_partitions(
        plan,
        ObjectSearchPartitions {
            as_of,
            partitions,
            failed_partitions: Vec::new(),
            discovery_failed,
            omitted_partitions,
            pending: Vec::new(),
            changed_keys: Some(0),
            layout: RangeLayout::default(),
        },
    )
}

fn checked_partitions(
    plan: &ObjectSearchPlan,
    partitions: ObjectSearchPartitions,
) -> Result<ObjectSearchPartitions, MetadataApiError> {
    if plan.mode == ObjectQueryMode::DistributedStrict
        && (partitions.discovery_failed
            || partitions.omitted_partitions > 0
            || !partitions.failed_partitions.is_empty())
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let unfinished = partitions
        .partitions
        .iter()
        .any(|partition| !partition.exhausted);
    if partitions.pending.is_empty() && !unfinished {
        return Err(MetadataApiError::InvalidCursor(
            "exhausted object search cursor".to_string(),
        ));
    }
    Ok(partitions)
}

/// The page the partitions answered, with the cursor `signer` must sign when more follows.
pub(super) fn assemble_object_execution(
    (plan, signer): (&ObjectSearchPlan, NodeId),
    partitions: ObjectSearchPartitions,
    parts: Vec<(NodeId, SearchNodePage)>,
    mut fanout_stats: MetadataFanoutStats,
) -> Result<(ObjectExecution, Option<ObjectCursor>), MetadataApiError> {
    let ObjectSearchPartitions {
        as_of,
        mut partitions,
        mut failed_partitions,
        discovery_failed,
        omitted_partitions,
        pending,
        changed_keys,
        layout,
    } = partitions;
    let newly_failed = fanout_stats.failed_partitions.clone();
    failed_partitions.extend(newly_failed.iter().copied());
    failed_partitions.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    failed_partitions.dedup();
    partitions.retain(|partition| !newly_failed.contains(&partition.node_id));
    partitions
        .sort_unstable_by(|left, right| left.node_id.as_bytes().cmp(right.node_id.as_bytes()));

    let local = plan.mode == ObjectQueryMode::Local;
    let incomplete_ranges = (parts.iter())
        .map(|(_, page)| page.incomplete_ranges + if local { page.remote_ranges } else { 0 })
        .sum::<usize>();
    let index_progress: Vec<_> = (parts.iter())
        .flat_map(|(node_id, page)| {
            let progress = page.index_progress.iter();
            progress.map(|(range_id, progress)| (*node_id, *range_id, *progress))
        })
        .collect();
    let partial_ranges: Vec<_> = (parts.iter())
        .flat_map(|(node_id, page)| {
            let partial = page.partial_ranges.iter();
            partial.map(|(range_id, through)| (*node_id, *range_id, through.clone()))
        })
        .collect();
    // Strict mode answers complete pages only: an unsearched range fails the request.
    if plan.mode == ObjectQueryMode::DistributedStrict && incomplete_ranges > 0 {
        return Err(MetadataApiError::SearchIncomplete(incomplete_reason(
            incomplete_ranges,
            partial_ranges.len(),
            &index_progress,
        )));
    }
    let layout = (parts.iter())
        .filter_map(|(_, page)| page.layout)
        .fold(layout, RangeLayout::observe);
    let moved = layout.changed;
    if plan.mode == ObjectQueryMode::DistributedStrict && moved {
        return Err(MetadataApiError::SearchIncomplete(
            "the key ranges of the distributed bucket changed during the search; start it again"
                .to_string(),
        ));
    }
    let mut pages = parts.into_iter().collect::<HashMap<_, _>>();
    let mut hits = Vec::with_capacity(plan.limit);
    let mut remaining = plan.limit;
    let (mut page_changed, mut certain) = (0, true);
    for partition in &mut partitions {
        if partition.exhausted {
            continue;
        }
        let Some(mut page) = pages.remove(&partition.node_id) else {
            continue;
        };
        partition.observed_at = Some(page.observed_at);
        if remaining == 0 {
            // A page the next one asks again counts its changed keys then.
            if page.hits.is_empty() && page.next_start_after.is_none() {
                partition.exhausted = true;
                certain &= page.changed.is_some();
                page_changed += page.changed.map_or(0, |changed| changed.len());
            }
            continue;
        }

        let consumed = remaining.min(page.hits.len());
        hits.extend(
            page.hits
                .iter()
                .take(consumed)
                .map(|candidate| candidate.hit.clone()),
        );
        remaining = remaining.saturating_sub(consumed);
        if consumed < page.hits.len() {
            partition.start_after = page
                .hits
                .get(consumed.saturating_sub(1))
                .map(|candidate| candidate.cursor_key.clone());
            partition.exhausted = false;
            if let Some(changed) = &mut page.changed {
                changed_until(changed, partition.start_after.as_deref());
            }
        } else if consumed > 0 || page.hits.is_empty() {
            partition.start_after = page.next_start_after;
            partition.exhausted = partition.start_after.is_none();
        }
        certain &= page.changed.is_some();
        page_changed += page.changed.map_or(0, |changed| changed.len());
    }
    let changed_keys = changed_keys
        .filter(|_| certain && !moved)
        .map(|keys| keys + page_changed);

    let unfinished = partitions.iter().any(|partition| !partition.exhausted);
    let next_cursor = if unfinished || !pending.is_empty() {
        let cursor = ObjectCursor::new_signed(
            plan.fingerprint,
            as_of,
            &partitions,
            &failed_partitions,
            discovery_failed,
            omitted_partitions,
            &pending,
            (changed_keys, layout),
            signer,
            blank_signature,
        );
        Some(cursor.map_err(|error| MetadataApiError::Internal(error.to_string()))?)
    } else {
        None
    };
    let coverage = partitions
        .iter()
        .filter_map(|partition| {
            partition
                .observed_at
                .map(|observed_at| ObjectPartitionCoverage {
                    node_id: partition.node_id,
                    observed_at,
                    truncated: !partition.exhausted,
                })
        })
        .collect::<Vec<_>>();

    fanout_stats.failed_partitions = failed_partitions;
    fanout_stats.nodes_failed =
        fanout_stats.failed_partitions.len() + omitted_partitions + usize::from(discovery_failed);
    let complete = fanout_stats.nodes_failed == 0 && incomplete_ranges == 0 && !moved;
    let execution = ObjectExecution {
        hits,
        next_cursor: None,
        as_of,
        partitions: coverage,
        fanout_stats,
        omitted_partitions,
        incomplete_ranges,
        index_progress,
        partial_ranges,
        complete,
        changed_keys,
        page_changed,
        layout_changed: moved,
    };
    Ok((execution, next_cursor))
}

/// Why a strict search refused: how many ranges were not searched or only in part, and how
/// many keys their indexes still miss.
pub(super) fn incomplete_reason(
    ranges: usize,
    partial: usize,
    progress: &[(NodeId, ulid::Ulid, IndexProgress)],
) -> String {
    let pending = (progress.iter())
        .map(|(_, _, progress)| progress.keys.saturating_sub(progress.indexed))
        .sum::<u64>();
    let mut reason = format!(
        "{ranges} distributed bucket ranges were not searched: their search index is missing \
         or still being built, or the search names no bucket"
    );
    if partial > 0 {
        reason.push_str(&format!(
            "; {partial} of them are indexed only up to a key before the end of the query"
        ));
    }
    if pending > 0 {
        reason.push_str(&format!("; about {pending} keys are still to be indexed"));
    }
    reason
}
