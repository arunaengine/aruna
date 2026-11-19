//! Plans a reference preflight, groups resolved versions into targets and merges the nodes'
//! answers into one page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::preflight_types::{
    MetadataExcludedForm, MetadataIndexState, MetadataPreflightLocation, MetadataResolvedTarget,
    MetadataStorageOperation, MetadataVisibleReference, ReferenceCoverage, ReferenceExecution,
    ReferenceNodeExecution, ReferenceRequest, ReferenceTarget, ReferenceTargetExecution,
};
use super::{
    ARUNA_DATA_PREFIX, ApiQueryMode, AuthContext, BTreeMap, BTreeSet, BlobVersion,
    BlobVersionState, HashMap, Key, MAX_PAGINATION_DEPTH, MetadataApiError, MetadataFanoutStats,
    MetadataSearchHit, NodeId, NodeSearchResult, REFERENCES_LIMIT, REFERENCES_MAX_LIMIT, RealmId,
    SearchCursor, SearchWatermark, Ulid, Value, VersionKey, paginate,
};

use super::head_versions::HeadVersionsError;
use super::prefix_scan::PrefixScanError;
use crate::metadata::data_links::LinkBudget;
use crate::metadata::search_cursor::{blank_signature, tie_order};

/// The content hash of a location target, which has none: its bytes were never read.
const LOCATION_HASH: [u8; 32] = [0; 32];

/// Groups versions into targets: stored bytes by their content identity, reference and pending
/// versions, whose bytes were never read, by the `s3://` location of their key.
pub(super) fn version_targets(
    versions: Vec<(VersionKey, BlobVersion)>,
    operation: MetadataStorageOperation,
    local_node_id: NodeId,
    s3_endpoint: Option<&str>,
) -> Vec<MetadataResolvedTarget> {
    let mut targets = BTreeMap::<String, MetadataResolvedTarget>::new();
    for (version_key, version) in versions {
        let (id, content_hash) = match version.state {
            BlobVersionState::Materialized { blob_hash, .. } => (
                format!("{ARUNA_DATA_PREFIX}{}", hex::encode(blob_hash)),
                blob_hash,
            ),
            BlobVersionState::Reference { .. } | BlobVersionState::PendingContent { .. } => (
                format!("s3://{}/{}", version_key.bucket, version_key.key),
                LOCATION_HASH,
            ),
            BlobVersionState::Deleted => continue,
        };
        let location = MetadataPreflightLocation {
            node_id: local_node_id,
            bucket: version_key.bucket.clone(),
            key: version_key.key.clone(),
            version_id: version_key.version_id,
        };
        let target = targets
            .entry(id.clone())
            .or_insert_with(|| MetadataResolvedTarget {
                queried_iris: vec![id.clone()],
                content_w3id: id,
                content_hash,
                targeted_versions: Vec::new(),
                removed_locations: Vec::new(),
                remove_resolvable_locations: false,
            });
        add_location_iris(
            &mut target.queried_iris,
            s3_endpoint,
            &location.bucket,
            &location.key,
        );
        target.targeted_versions.push(location.clone());
        if operation == MetadataStorageOperation::AllVersionsPurge {
            target.removed_locations.push(location);
        }
    }
    let order = |location: &MetadataPreflightLocation| {
        (
            location.bucket.clone(),
            location.key.clone(),
            location.version_id,
        )
    };
    for target in targets.values_mut() {
        target.queried_iris.sort();
        target.queried_iris.dedup();
        target.targeted_versions.sort_by_key(order);
        target.removed_locations.sort_by_key(order);
    }
    targets.into_values().collect()
}

impl From<HeadVersionsError> for MetadataApiError {
    fn from(error: HeadVersionsError) -> Self {
        match error {
            HeadVersionsError::Unavailable => Self::ServiceUnavailable,
            HeadVersionsError::Invalid(message) => Self::Internal(message),
        }
    }
}

impl From<PrefixScanError> for MetadataApiError {
    fn from(_: PrefixScanError) -> Self {
        Self::ServiceUnavailable
    }
}

pub(super) fn decode_version(
    key: &Key,
    value: &Value,
) -> Result<(VersionKey, BlobVersion), MetadataApiError> {
    let version_key = VersionKey::from_bytes(key.as_ref())
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let version = BlobVersion::from_bytes(value.as_ref())
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    Ok((version_key, version))
}

/// Reads at most `limit` rows under the prefix from `from` on; true reports that more rows exist.
pub(super) fn add_location_iris(
    iris: &mut Vec<String>,
    s3_endpoint: Option<&str>,
    bucket: &str,
    key: &str,
) {
    iris.push(format!("s3://{bucket}/{key}"));
    if let Some(endpoint) = s3_endpoint.filter(|endpoint| !endpoint.is_empty()) {
        iris.push(format!("{}/{bucket}/{key}", endpoint.trim_end_matches('/')));
    }
}

/// The index state of the returned documents, with documents still to be materialized
/// (`pending`) and documents whose materialization failed (`failed`) on this node.
pub(super) fn index_state(
    returned: crate::metadata::iri_index::IriFreshnessState,
    pending: bool,
    failed: bool,
) -> MetadataIndexState {
    use crate::metadata::iri_index::IriFreshnessState;
    let pending = pending || returned == IriFreshnessState::Pending;
    let failed = failed || returned == IriFreshnessState::Failed;
    match (returned == IriFreshnessState::Mixed, pending, failed) {
        (true, _, _) | (false, true, true) => MetadataIndexState::Mixed,
        (false, false, true) => MetadataIndexState::Failed,
        (false, true, false) => MetadataIndexState::Pending,
        (false, false, false) => MetadataIndexState::Current,
    }
}

/// Keeps the first `limit` references in watermark order and reports whether more exist.
/// The coordinator pages node prefixes by that order, so a cut in any other order skips hits.
pub(super) fn visible_prefix(
    visible: BTreeSet<(String, Ulid)>,
    limit: usize,
) -> (Vec<(String, Ulid)>, bool) {
    let mut keyed = visible
        .into_iter()
        .map(|(content_w3id, document_id)| (content_w3id, document_id.to_string(), document_id))
        .collect::<Vec<_>>();
    keyed.sort_by(|left, right| tie_order((&left.0, &left.1), (&right.0, &right.1)));
    let saturated = keyed.len() > limit;
    keyed.truncate(limit);
    let selected = keyed
        .into_iter()
        .map(|(content_w3id, _, document_id)| (content_w3id, document_id))
        .collect();
    (selected, saturated)
}

pub(super) const MAX_TARGET_VERSIONS: usize = 128;
/// Postings one node's preflight may read across all of its target IRIs.
pub(super) const PREFLIGHT_BUDGET: LinkBudget = LinkBudget {
    seeks: 16_384,
    rows: 262_144,
};

/// Version rows of keys of `MAX_KEY_LEN` bytes and more one purge preflight reads at most.
pub(super) const LONG_KEY_ROWS: usize = 1024;

#[derive(Debug, PartialEq)]
pub(super) struct ResolvedPreflightTargets {
    pub(super) targets: Vec<MetadataResolvedTarget>,
    pub(super) complete: bool,
}

#[derive(Debug, PartialEq)]
pub(super) struct PreflightPlan {
    pub(super) auth: AuthContext,
    pub(super) bearer_token: Option<String>,
    pub(super) s3_endpoint: Option<String>,
    pub(super) cursor: Option<String>,
    pub(super) page_size: usize,
    pub(super) mode: Option<ApiQueryMode>,
    pub(super) target_nodes: Option<Vec<NodeId>>,
    pub(super) allow_partial: bool,
}

#[derive(Debug, PartialEq)]
pub(super) struct PreflightCursorPlan {
    pub(super) fingerprint: [u8; 32],
    pub(super) watermark: Option<SearchWatermark>,
    pub(super) resume: HashMap<NodeId, u32>,
    pub(super) target_nodes: Option<Vec<NodeId>>,
    pub(super) discovery_failed: bool,
}

pub(super) fn plan_preflight_request(
    realm_id: RealmId,
    request: ReferenceRequest,
) -> Result<(PreflightPlan, ReferenceTarget), MetadataApiError> {
    let ReferenceRequest {
        auth,
        bearer_token,
        target,
        s3_endpoint,
        limit,
        cursor,
        mode,
        target_nodes,
        allow_partial,
    } = request;
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let page_size = limit
        .unwrap_or(REFERENCES_LIMIT)
        .clamp(1, REFERENCES_MAX_LIMIT);
    Ok((
        PreflightPlan {
            auth,
            bearer_token,
            s3_endpoint,
            cursor,
            page_size,
            mode,
            target_nodes,
            allow_partial,
        },
        target,
    ))
}

/// The page the nodes answered, with the cursor `signer` must sign when more follows.
pub(super) fn assemble_preflight_execution(
    signer: NodeId,
    resolved: ResolvedPreflightTargets,
    plan: &PreflightPlan,
    cursor: PreflightCursorPlan,
    node_parts: Vec<(NodeId, ReferenceNodeExecution)>,
    fanout_stats: MetadataFanoutStats,
) -> Result<(ReferenceExecution, Option<SearchCursor>), MetadataApiError> {
    let mut node_results = Vec::new();
    let mut node_freshness = Vec::new();
    let mut hidden = BTreeSet::new();
    let mut locations = BTreeMap::<String, (bool, bool)>::new();
    let mut path_style_complete = true;
    for (node_id, part) in node_parts {
        node_freshness.push(part.freshness.clone());
        path_style_complete &= part.path_style_available;
        for target in part.targets {
            if target.hidden_references_exist {
                hidden.insert(target.content_w3id.clone());
            }
            let entry = locations.entry(target.content_w3id).or_default();
            entry.0 |= target.resolvable_location_found;
            entry.1 |= target.location_after_operation;
        }
        let hits = part
            .visible_references
            .into_iter()
            .map(|reference| MetadataSearchHit {
                document_id: reference.document_id.clone(),
                group_id: String::new(),
                document_path: String::new(),
                graph_iri: reference.content_w3id,
                subject_iri: reference.document_id,
                score: 0.0,
                title: reference.title,
                snippet: None,
                subject_types: Vec::new(),
            })
            .collect();
        node_results.push(NodeSearchResult {
            node_id,
            hits,
            saturated: part.saturated,
        });
    }
    node_freshness.sort_by_key(|freshness| freshness.node_id.to_string());
    let page = paginate(
        node_results,
        cursor.watermark,
        plan.page_size,
        MAX_PAGINATION_DEPTH,
    );
    let mut visible_by_target = BTreeMap::<String, Vec<MetadataVisibleReference>>::new();
    for hit in page.hits {
        visible_by_target
            .entry(hit.graph_iri.clone())
            .or_default()
            .push(MetadataVisibleReference {
                content_w3id: hit.graph_iri,
                document_id: hit.document_id,
                title: hit.title,
            });
    }
    // A location target has a path-style IRI only when the node it was resolved on serves S3.
    let unread = (resolved.targets.iter()).any(|target| target.location().is_some());
    path_style_complete &= plan.s3_endpoint.is_some() || !unread;
    let index_current = node_freshness
        .iter()
        .all(|freshness| freshness.index_state == MetadataIndexState::Current);
    let complete = fanout_stats.nodes_failed == 0
        && resolved.complete
        && index_current
        && path_style_complete
        && !page.truncated;
    if !plan.allow_partial && !complete {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let targets = resolved
        .targets
        .into_iter()
        .map(|target| {
            let (found, remaining) = locations
                .remove(&target.content_w3id)
                .unwrap_or((false, false));
            let removes_location =
                target.remove_resolvable_locations || !target.removed_locations.is_empty();
            // Purging a location target's versions ends its only known location.
            let location = target.location().is_some();
            let last = location || (found && !remaining);
            ReferenceTargetExecution {
                visible_references: visible_by_target
                    .remove(&target.content_w3id)
                    .unwrap_or_default(),
                hidden_references_exist: hidden.contains(&target.content_w3id),
                would_remove_location: complete && removes_location && last,
                location_impact_complete: complete,
                content_w3id: (!location).then(|| target.content_w3id.clone()),
                location_iri: location.then_some(target.content_w3id),
                targeted_versions: target.targeted_versions,
            }
        })
        .collect();
    let next_cursor = match page.next {
        Some(next) => {
            let cursor = SearchCursor::new_signed(
                cursor.fingerprint,
                next.watermark,
                next.resume,
                signer,
                blank_signature,
            );
            Some(cursor.map_err(|error| MetadataApiError::Internal(error.to_string()))?)
        }
        None => None,
    };
    let distributed = plan.mode.unwrap_or(ApiQueryMode::Distributed) == ApiQueryMode::Distributed;
    let execution = ReferenceExecution {
        targets,
        next_cursor: None,
        truncated: page.truncated,
        nodes_queried: fanout_stats.nodes_queried,
        nodes_failed: fanout_stats.nodes_failed,
        complete,
        failed_partitions: fanout_stats.failed_partitions,
        coverage: ReferenceCoverage {
            queried_scope: if distributed { "realm" } else { "local_node" },
            queried_forms: vec![
                "canonical_content_w3id",
                "legacy_s3_iri",
                "legacy_path_style_http_iri",
            ],
            excluded_forms: vec![
                MetadataExcludedForm {
                    form: "literal_content_url",
                    reason: "literal objects are not materialized in the NamedNode IRI index",
                },
                MetadataExcludedForm {
                    form: "imported_relative_identity",
                    reason: "relative imported identities are outside exact absolute-IRI matching",
                },
                MetadataExcludedForm {
                    form: "imported_external_identity",
                    reason: "external identities without an Aruna content mapping are outside coverage",
                },
            ]
            .into_iter()
            .chain(unread.then_some(MetadataExcludedForm {
                form: "unread_content_w3id",
                reason: "reference and pending versions have no content identity until their bytes \
                         are read, so documents citing those bytes by content w3id are not found",
            }))
            .collect(),
            node_freshness,
            target_resolution_complete: resolved.complete,
            path_style_complete,
            realm_coverage_complete: distributed && complete,
        },
    };
    Ok((execution, next_cursor))
}
