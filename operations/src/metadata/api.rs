use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::short_display_id;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataError, MetadataGraphLifecycleRecord, MetadataQueryResults,
    MetadataRoCratePage, MetadataSearchHit,
};
use aruna_core::storage_entries::{
    metadata_event_log_key, metadata_graph_lifecycle_key, metadata_pending_projection_target,
};
use aruna_core::structs::{
    AuthContext, MetadataRegistryRecord, PathClaimRecord, Permission, PlacementRef,
    RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::GroupId;
use aruna_core::{MetaResourceId, NodeId, StructuredId};
use futures_util::StreamExt;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{Instrument, Span, debug_span, field, warn};
use ulid::Ulid;

use super::MetadataAuthToken;
use super::handle::{
    METADATA_QUERY_MAX_BYTES, METADATA_QUERY_MAX_RESULT_BYTES, METADATA_QUERY_MAX_ROWS,
    METADATA_REGISTRY_CANDIDATE_LIMIT,
};
use super::protocol::{
    MetadataPathCandidate, MetadataPathResolution, MetadataPathWinner, MetadataReadError,
    MetadataTransportMessage,
};
use super::search_cursor::{
    METADATA_SEARCH_DEFAULT_PAGE_SIZE, METADATA_SEARCH_MAX_PAGE_SIZE,
    METADATA_SEARCH_MAX_PAGINATION_DEPTH, NodeSearchResult, SearchCursor, SearchCursorError,
    SearchPageCursor, SearchWatermark, paginate, query_fingerprint, resume_fetch_limit,
};
use super::summary_cache::summary_cache;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::get_metadata_document::{
    is_metadata_record_materialized_for_graph_read, load_metadata_record_by_document,
};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::get_realm_nodes::{GetRealmNodesOperation, REALM_DISCOVERY_TIMEOUT};
use crate::list_groups::ListGroupOperation;
use crate::metadata::repository::{
    LIST_METADATA_PAGE_SIZE, StorageReadError, iter_registry_effect, parse_registry_iter,
};
use crate::permission_rules::GroupPermissionRules;
use crate::placement::selector::{ROLE_NODE, neg_log2_q48, selector_hash};
use crate::placement::{
    holds_placement, meta_bucket_subject, registry_placement, registry_placement_for,
    registry_strategy, resolve_shard_holders,
};
use crate::s3::search_buckets::{BucketSearchHit, SearchBucketsInput, search_local_buckets};

const DEFAULT_LIST_METADATA_LIMIT: usize = 50;
const MAX_LIST_METADATA_LIMIT: usize = 1_000;
/// Bounds the response payload and the number of RO-Crate summary exports an
/// unauthenticated caller can force per request. The realm-wide registry scan
/// is removed by the cached list path, not by this clamp.
const ANONYMOUS_LIST_METADATA_LIMIT: usize = 100;
/// Splits a targeted lookup from a browse page: the portal pages at 48, a
/// run-crate or preview lookup at 1, and only a browse page pays the estimate.
const METADATA_ESTIMATE_MIN_LIMIT: usize = 24;
// Bounded so a single summary page cannot saturate the craqle read permits.
const METADATA_SUMMARY_FANOUT_LIMIT: usize = 8;
const METADATA_REFERENCES_DEFAULT_LIMIT: usize = 25;
const METADATA_REFERENCES_MAX_LIMIT: usize = 100;
const METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT: usize = 8;
const METADATA_DISTRIBUTED_QUERY_MAX_NODES: usize = 32;
const METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT: Duration = Duration::from_secs(10);
const METADATA_DISTRIBUTED_QUERY_DEADLINE: Duration = Duration::from_secs(12);
// Four bounded fanout waves must finish before the request deadline.
const METADATA_PATH_PEER_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum MetadataApiError {
    #[error("bad request")]
    BadRequest,
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("not found")]
    NotFound,
    #[error("service unavailable")]
    ServiceUnavailable,
    #[error("{0}")]
    InvalidCursor(String),
    #[error("{0}")]
    Internal(String),
}

/// Order the visible metadata listing is paginated in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataListOrder {
    /// Ascending document id, which is creation order for ULID ids.
    #[default]
    Created,
    /// Descending `updated_at_ms`, tie-broken by descending document id.
    Recent,
}

#[derive(Debug, Clone)]
pub struct ListVisibleMetadataDocumentsRequest {
    pub group_id: Option<GroupId>,
    pub path_prefix: Option<String>,
    pub include_summary: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: MetadataListOrder,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedMetadataDocument {
    pub record: MetadataRegistryRecord,
    pub rocrate_summary_jsonld: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListVisibleMetadataDocumentsResult {
    pub documents: Vec<ListedMetadataDocument>,
    pub limit: usize,
    pub offset: usize,
    pub total_returned: usize,
    /// Approximate number of documents matching the request filters across all
    /// pages. Group-granular, so it can over- or under-count against the
    /// glob-granular read rules. `None` when the request was too small to be a
    /// browse page and the estimate was not computed.
    pub total_estimate: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct MetadataPathLookupRequest {
    pub group_id: GroupId,
    pub document_path: String,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone)]
pub struct MetadataPathLookupResult {
    pub winner: MetadataPathWinner,
    pub conflicts: Vec<Ulid>,
}

#[derive(Debug, Clone)]
pub struct GetVisibleMetadataDocumentRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataRoCrateExportView {
    Full,
    Summary,
    Page,
    Raw,
}

#[derive(Debug, Clone)]
pub struct ExportMetadataRoCrateRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub view: MetadataRoCrateExportView,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExportMetadataRoCrateResult {
    Full {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Summary {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Page {
        record: MetadataRegistryRecord,
        page: MetadataRoCratePage,
    },
    Raw {
        record: MetadataRegistryRecord,
        raw: crate::metadata::raw::MetadataRawView,
        dataset_digest: Option<[u8; 32]>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataApiQueryMode {
    Local,
    Distributed,
}

#[derive(Debug, Clone)]
pub struct MetadataDocumentQueryRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataSearchRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub conforms_to: Option<String>,
    pub group_id: Option<GroupId>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryExecution {
    pub results: MetadataQueryResults,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct MetadataSearchExecution {
    pub hits: Vec<MetadataSearchHit>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct BucketSearchRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub query: String,
    pub limit: usize,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct BucketSearchExecution {
    pub hits: Vec<BucketSearchHit>,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct MetadataReferencesRequest {
    pub auth: Option<AuthContext>,
    pub iri: String,
    pub predicate: Option<String>,
    pub limit: Option<usize>,
    pub resolve: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataReferenceEntry {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub predicate: Option<String>,
    pub subject_iris: Vec<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetadataReferencesExecution {
    pub references: Vec<MetadataReferenceEntry>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MetadataFanoutStats {
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub failed_partitions: Vec<NodeId>,
    pub discovery_failed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct MetadataRealmNodeDiscovery {
    pub(crate) nodes: Vec<NodeId>,
    pub(crate) failed: bool,
}

#[derive(Debug)]
struct MetadataFanoutScope {
    mode: Option<MetadataApiQueryMode>,
    target_nodes: Option<Vec<NodeId>>,
    allow_partial: bool,
    discovery_failed: bool,
}

impl MetadataFanoutScope {
    fn new(
        mode: Option<MetadataApiQueryMode>,
        target_nodes: Option<Vec<NodeId>>,
        allow_partial: bool,
    ) -> Self {
        Self {
            mode,
            target_nodes,
            allow_partial,
            discovery_failed: false,
        }
    }

    fn with_discovery_failed(mut self, discovery_failed: bool) -> Self {
        self.discovery_failed = discovery_failed;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataQueryForm {
    Select,
    Ask,
}

pub async fn list_visible_metadata_documents(
    context: &DriverContext,
    realm_id: RealmId,
    request: ListVisibleMetadataDocumentsRequest,
) -> Result<ListVisibleMetadataDocumentsResult, MetadataApiError> {
    let limit = effective_list_limit(request.limit, request.auth.is_none());
    let offset = request.offset.unwrap_or(0);

    let group_ids = check_policy_limit(match request.group_id {
        Some(group_id) => vec![group_id],
        None => drive(
            ListGroupOperation::with_pagination(METADATA_REGISTRY_CANDIDATE_LIMIT + 1, 0),
            context,
        )
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?
        .into_iter()
        .map(|group| group.group_id)
        .collect(),
    })?;
    // Summary listings and recency listings must show documents whose projection
    // has not landed yet; the pending keyspace is scanned once per request,
    // never once per group.
    let recent = request.order == MetadataListOrder::Recent;
    let mut pending = if request.include_summary || recent {
        load_pending_records(context, request.group_id, METADATA_REGISTRY_CANDIDATE_LIMIT).await?
    } else {
        HashMap::new()
    };

    let mut records = Vec::new();
    for group_id in group_ids {
        let remaining = METADATA_REGISTRY_CANDIDATE_LIMIT.saturating_sub(records.len());
        let mut group_records = load_group_records(context, group_id, remaining).await?;
        if let Some(pending_records) = pending.remove(&group_id) {
            merge_pending_metadata_records(&mut group_records, pending_records);
            if group_records.len() > remaining {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            group_records.sort_by_key(|record| record.document_id);
        }
        records.extend(group_records);
    }
    // Ordering precedes both the estimate scan and the offset window so that
    // pagination and the early exit page the same sequence.
    if recent {
        records.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| right.document_id.cmp(&left.document_id))
        });
    }

    // One rule collection (a read per distinct group) replaces the per-record
    // permission drives; `record_visible` mirrors `can_read_record` for the
    // same caller and record, so every later check is pure memory.
    let auth = request
        .auth
        .as_ref()
        .filter(|auth| auth.realm_id == realm_id);
    let permissions = GroupPermissionRules::collect(
        context,
        auth,
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .map(|record| record.group_id),
    )
    .await;
    // RBAC/public visibility is additionally constrained by the metadata.read
    // request policies, loaded once per distinct group (fail-closed on error).
    let evaluators = crate::request_policy::PolicyEvaluator::load_bulk(
        context,
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .map(|record| (record.realm_id, record.group_id)),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let policy_user = request.auth.as_ref().map(|auth| auth.user_id);
    let record_visible = |record: &MetadataRegistryRecord| {
        permissions.record_visible(record)
            && evaluators
                .get(&(record.realm_id, record.group_id))
                .is_some_and(|evaluator| {
                    evaluator
                        .evaluate(&metadata_read_request(
                            &record.permission_path,
                            policy_user.as_ref(),
                        ))
                        .is_ok()
                })
    };

    let mut total_estimate = None;
    if limit >= METADATA_ESTIMATE_MIN_LIMIT {
        let matching = records
            .iter()
            .filter(|record| {
                metadata_record_matches_filters(record, request.path_prefix.as_deref())
            })
            .filter(|record| record_visible(record))
            .count();
        total_estimate = Some(matching);
    }

    let needed = offset.saturating_add(limit);
    let mut selected = Vec::with_capacity(limit.min(records.len()));
    let mut visible_count = 0usize;
    for record in records {
        if !metadata_record_matches_filters(&record, request.path_prefix.as_deref()) {
            continue;
        }
        if !record_visible(&record) {
            continue;
        }
        visible_count += 1;
        if visible_count > offset {
            selected.push(record);
            if visible_count >= needed {
                break;
            }
        }
    }

    let mut documents = Vec::with_capacity(selected.len());
    if request.include_summary {
        let exports = selected
            .iter()
            .map(|record| async move {
                // The registry cursor advances at event acceptance, but the
                // graph only at materialization. Exporting inside that window
                // would hand out the content (and cache it under the new cursor)
                // the event just replaced, so a pending document lists without
                // a summary instead.
                ensure_record_materialized_for_graph_read(context, record).await?;
                export_rocrate_summary_jsonld(context, &record.graph_iri, record.last_event_id)
                    .await
            })
            .collect::<Vec<_>>();
        let summaries = stream::iter(exports)
            .buffered(METADATA_SUMMARY_FANOUT_LIMIT)
            .collect::<Vec<_>>()
            .await;
        for (record, summary) in selected.into_iter().zip(summaries) {
            let rocrate_summary_jsonld = match summary {
                Ok(summary) => Some(summary),
                Err(MetadataApiError::ServiceUnavailable) => None,
                Err(error) => return Err(error),
            };
            documents.push(ListedMetadataDocument {
                record,
                rocrate_summary_jsonld,
            });
        }
    } else {
        documents.extend(selected.into_iter().map(|record| ListedMetadataDocument {
            record,
            rocrate_summary_jsonld: None,
        }));
    }

    let total_returned = documents.len();
    Ok(ListVisibleMetadataDocumentsResult {
        documents,
        limit,
        offset,
        total_returned,
        // Never report fewer than the page already discloses.
        total_estimate: total_estimate.map(|estimate| estimate.max(total_returned)),
    })
}

pub async fn lookup_metadata_path(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataPathLookupRequest,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataPathLookupResult, MetadataApiError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
    if normalized.is_empty() {
        return Err(MetadataApiError::BadRequest);
    }
    if context.net_handle.is_none() {
        let candidates = local_path_candidates(
            context,
            realm_id,
            request.group_id,
            &normalized,
            request.auth.as_ref(),
        )
        .await?;
        return reduce_path_candidates(candidates);
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let local_node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let trusted_origin = config.nodes.iter().any(|node| {
        node.node_id == local_node.to_string()
            && matches!(node.kind, RealmNodeKind::Management | RealmNodeKind::Server)
    });
    if !trusted_origin {
        let config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        return forward_path_resolution(
            context,
            realm_id,
            &config,
            request,
            auth_token,
            config_digest,
        )
        .await;
    }
    let strategy = registry_strategy(&config).ok_or(MetadataApiError::ServiceUnavailable)?;
    if strategy.shard_count == 0 {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let shard_count = strategy.shard_count;
    let auth_token = auth_token.or_else(|| request.auth.clone().map(MetadataAuthToken::internal));
    let group_id = request.group_id;
    let auth = request.auth.as_ref();
    let mut replica_counts = vec![0usize; shard_count as usize];
    let mut holder_shards: HashMap<NodeId, Vec<u32>> = HashMap::new();
    for shard in 0..shard_count {
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
            shard,
        };
        let holders = resolve_shard_holders(&config, &placement);
        if holders.is_empty() {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        for holder in holders {
            replica_counts[shard as usize] += 1;
            holder_shards.entry(holder).or_default().push(shard);
        }
    }
    if holder_shards.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let mut holders = holder_shards.into_iter().collect::<Vec<_>>();
    holders.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    let requests = stream::iter(holders.into_iter().map(|(holder, shards)| {
        let auth_token = auth_token.clone();
        let normalized = normalized.clone();
        async move {
            let result = if holder == local_node {
                local_path_candidates(context, realm_id, group_id, &normalized, auth).await
            } else {
                load_path_holder(
                    context,
                    group_id,
                    &normalized,
                    holder,
                    auth_token,
                    config_digest,
                )
                .await
            };
            (holder, shards, result)
        }
    }))
    .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT)
    .collect::<Vec<_>>();
    let responses = tokio::time::timeout(METADATA_DISTRIBUTED_QUERY_DEADLINE, requests)
        .await
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let mut views = Vec::new();
    let mut auth_error = None;
    let mut failed = false;
    let mut only_auth = true;
    let mut candidate_count = 0usize;
    for (_holder, shards, response) in responses {
        match response {
            Ok(returned) => {
                candidate_count = candidate_count.saturating_add(returned.len());
                if candidate_count > METADATA_REGISTRY_CANDIDATE_LIMIT {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                only_auth = false;
                let mut partitions = shards.iter().map(|_| Vec::new()).collect::<Vec<_>>();
                for candidate in returned {
                    let placement = validate_path_candidate(
                        &config,
                        realm_id,
                        group_id,
                        &normalized,
                        &candidate,
                    )?;
                    let index = shards
                        .binary_search(&placement.shard)
                        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
                    partitions[index].push(candidate);
                }
                views.extend(
                    shards
                        .into_iter()
                        .zip(partitions)
                        .map(|(shard, candidates)| PathShardView { shard, candidates }),
                );
            }
            Err(error @ (MetadataApiError::Unauthorized | MetadataApiError::Forbidden)) => {
                failed = true;
                auth_error.get_or_insert(error);
            }
            Err(_) => {
                failed = true;
                only_auth = false;
            }
        }
    }
    if failed {
        if only_auth && let Some(error) = auth_error {
            return Err(error);
        }
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let candidates = merge_path_views(&replica_counts, views)?;
    reduce_path_candidates(candidates)
}

fn select_forward_peers(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    normalized: &str,
    local_node: NodeId,
) -> Result<Vec<NodeId>, MetadataApiError> {
    let mut subject = meta_bucket_subject(realm_id, group_id, normalized);
    subject.extend_from_slice(local_node.as_bytes());
    let mut ranked = Vec::with_capacity(METADATA_DISTRIBUTED_QUERY_MAX_NODES);
    for node in config
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, RealmNodeKind::Management | RealmNodeKind::Server))
    {
        let Ok(peer) = node.node_id.parse::<NodeId>() else {
            continue;
        };
        if peer == local_node || ranked.iter().any(|(candidate, _)| *candidate == peer) {
            continue;
        }
        let score = neg_log2_q48(selector_hash(ROLE_NODE, &subject, peer.as_bytes()));
        if ranked.len() < METADATA_DISTRIBUTED_QUERY_MAX_NODES {
            ranked.push((peer, score));
            continue;
        }
        let Some((worst_index, (worst_peer, worst_score))) =
            ranked.iter().enumerate().max_by(|(_, left), (_, right)| {
                left.1
                    .cmp(&right.1)
                    .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
            })
        else {
            continue;
        };
        if score < *worst_score
            || (score == *worst_score && peer.as_bytes() < worst_peer.as_bytes())
        {
            ranked[worst_index] = (peer, score);
        }
    }
    ranked.sort_unstable_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
    });
    Ok(ranked.into_iter().map(|(peer, _)| peer).collect())
}

async fn forward_path_resolution(
    context: &DriverContext,
    realm_id: RealmId,
    config: &RealmConfigDocument,
    request: MetadataPathLookupRequest,
    auth_token: Option<MetadataAuthToken>,
    config_digest: [u8; 32],
) -> Result<MetadataPathLookupResult, MetadataApiError> {
    if request.auth.is_some() && auth_token.is_none() {
        return Err(MetadataApiError::Unauthorized);
    }
    let local_node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
    let peers = select_forward_peers(config, realm_id, request.group_id, &normalized, local_node)?;
    if peers.is_empty() {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let requests = stream::iter(peers.into_iter().map(|peer| {
        let auth_token = auth_token.clone();
        let document_path = request.document_path.clone();
        async move {
            let response = tokio::time::timeout(
                METADATA_PATH_PEER_TIMEOUT,
                metadata.request_forwarded_write(
                    peer,
                    MetadataTransportMessage::ForwardPathResolution {
                        auth_token,
                        group_id: request.group_id,
                        document_path,
                        config_digest,
                    },
                ),
            )
            .await;
            (response, peer)
        }
    }))
    .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT);
    futures_util::pin_mut!(requests);
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let mut auth_error = None;
    let mut not_found = false;
    let mut unavailable = false;
    let mut success = None;
    loop {
        let response = match tokio::time::timeout_at(deadline, requests.next()).await {
            Ok(response) => response,
            Err(_) => return Err(MetadataApiError::ServiceUnavailable),
        };
        let Some((response, _peer)) = response else {
            break;
        };
        match response {
            Ok(Ok(MetadataTransportMessage::ForwardedPathResolution { result: Ok(result) })) => {
                if validate_path_resolution(
                    realm_id,
                    request.group_id,
                    &request.document_path,
                    &result,
                )
                .is_ok()
                {
                    success.get_or_insert(MetadataPathLookupResult {
                        winner: result.winner,
                        conflicts: result.conflicts,
                    });
                } else {
                    unavailable = true;
                }
            }
            Ok(Ok(MetadataTransportMessage::ForwardedPathResolution {
                result: Err(MetadataReadError::Unauthorized),
            })) => {
                auth_error.get_or_insert(MetadataApiError::Unauthorized);
            }
            Ok(Ok(MetadataTransportMessage::ForwardedPathResolution {
                result: Err(MetadataReadError::Forbidden),
            })) => {
                auth_error.get_or_insert(MetadataApiError::Forbidden);
            }
            Ok(Ok(MetadataTransportMessage::ForwardedPathResolution {
                result: Err(MetadataReadError::NotFound),
            })) => not_found = true,
            _ => unavailable = true,
        }
    }
    if let Some(error) = auth_error {
        return Err(error);
    }
    if let Some(result) = success {
        return Ok(result);
    }
    if unavailable {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    if not_found {
        Err(MetadataApiError::NotFound)
    } else {
        Err(MetadataApiError::ServiceUnavailable)
    }
}

fn validate_path_resolution(
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    resolution: &MetadataPathResolution,
) -> Result<(), MetadataApiError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    let winner_id = MetaResourceId::from_bytes(resolution.winner.document_id.to_bytes())
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if resolution.winner.realm_id != realm_id
        || resolution.winner.group_id != group_id
        || resolution.winner.document_path != normalized
        || resolution.winner.graph_iri
            != MetadataRegistryRecord::graph_iri_for(resolution.winner.document_id)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let mut seen = HashSet::from([winner_id]);
    for conflict in &resolution.conflicts {
        let conflict = MetaResourceId::from_bytes(conflict.to_bytes())
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        if !seen.insert(conflict) {
            return Err(MetadataApiError::ServiceUnavailable);
        }
    }
    Ok(())
}

struct PathShardView {
    shard: u32,
    candidates: Vec<MetadataPathCandidate>,
}

fn merge_path_views(
    expected: &[usize],
    views: Vec<PathShardView>,
) -> Result<Vec<MetadataPathCandidate>, MetadataApiError> {
    let mut consensus = vec![None; expected.len()];
    let mut seen = vec![0usize; expected.len()];
    for mut view in views {
        let shard = view.shard as usize;
        if shard >= expected.len() || seen[shard] >= expected[shard] {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        normalize_path_view(&mut view.candidates)?;
        seen[shard] += 1;
        match consensus[shard].as_ref() {
            Some(current) if current != &view.candidates => {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            Some(_) => {}
            None => consensus[shard] = Some(view.candidates),
        }
    }

    let mut candidates = Vec::new();
    for (shard, expected) in expected.iter().copied().enumerate() {
        if expected == 0 || seen[shard] != expected {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        candidates.extend(consensus[shard].take().unwrap_or_default());
    }
    Ok(candidates)
}

fn normalize_path_view(candidates: &mut [MetadataPathCandidate]) -> Result<(), MetadataApiError> {
    candidates.sort_by_key(|candidate| {
        (
            candidate.claim.document_id,
            candidate.claim.establishing_event_id,
        )
    });
    if candidates
        .windows(2)
        .any(|pair| pair[0].claim.document_id == pair[1].claim.document_id)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(())
}

async fn load_path_holder(
    context: &DriverContext,
    group_id: GroupId,
    document_path: &str,
    holder: NodeId,
    auth_token: Option<MetadataAuthToken>,
    config_digest: [u8; 32],
) -> Result<Vec<MetadataPathCandidate>, MetadataApiError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    match tokio::time::timeout(
        METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT,
        metadata.request_forwarded_write(
            holder,
            MetadataTransportMessage::ForwardPathLookup {
                auth_token,
                group_id,
                document_path: document_path.to_string(),
                config_digest,
            },
        ),
    )
    .await
    {
        Ok(Ok(MetadataTransportMessage::ForwardedPathLookup { result: Ok(result) })) => Ok(result),
        Ok(Ok(MetadataTransportMessage::ForwardedPathLookup {
            result: Err(MetadataReadError::Unauthorized),
        })) => Err(MetadataApiError::Unauthorized),
        Ok(Ok(MetadataTransportMessage::ForwardedPathLookup {
            result: Err(MetadataReadError::Forbidden),
        })) => Err(MetadataApiError::Forbidden),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

pub(crate) async fn local_path_candidates(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    auth: Option<&AuthContext>,
) -> Result<Vec<MetadataPathCandidate>, MetadataApiError> {
    let mut records = load_claim_records(context, realm_id, Some(group_id)).await?;
    records.retain(|record| record.document_path == document_path);
    if let Some(net) = context.net_handle.as_ref() {
        let config = load_realm_config(context, realm_id)
            .await
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        registry_strategy(&config).ok_or(MetadataApiError::ServiceUnavailable)?;
        records.retain(|record| {
            holds_placement(&config, &registry_placement(&config, record), net.node_id())
        });
    }
    let permissions = GroupPermissionRules::collect(
        context,
        auth.filter(|auth| auth.realm_id == realm_id),
        records.iter().map(|record| record.group_id),
    )
    .await;
    let evaluators = crate::request_policy::PolicyEvaluator::load_bulk(
        context,
        records
            .iter()
            .map(|record| (record.realm_id, record.group_id)),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let policy_user = auth.map(|auth| auth.user_id);
    let mut candidates = records
        .into_iter()
        .map(|record| {
            let document_id = MetaResourceId::from_bytes(record.document_id.to_bytes())
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
            let claim = PathClaimRecord {
                document_id,
                establishing_event_id: record.establishing_event_id,
                requested_path: record.document_path.clone(),
            };
            let visible = permissions.record_visible(&record)
                && evaluators
                    .get(&(record.realm_id, record.group_id))
                    .is_some_and(|evaluator| {
                        evaluator
                            .evaluate(&metadata_read_request(
                                &record.permission_path,
                                policy_user.as_ref(),
                            ))
                            .is_ok()
                    });
            let record = visible.then_some(record);
            Ok(MetadataPathCandidate { claim, record })
        })
        .collect::<Result<Vec<_>, MetadataApiError>>()?;
    candidates.sort_by_key(|candidate| {
        (
            candidate.claim.document_id,
            candidate.claim.establishing_event_id,
        )
    });
    Ok(candidates)
}

fn validate_path_candidate(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    candidate: &MetadataPathCandidate,
) -> Result<PlacementRef, MetadataApiError> {
    if candidate.claim.requested_path != document_path {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let placement = registry_placement_for(config, group_id, candidate.claim.document_id.as_ulid());
    if let Some(record) = candidate.record.as_ref()
        && (record.realm_id != realm_id
            || record.group_id != group_id
            || record.document_id != candidate.claim.document_id.as_ulid()
            || record.establishing_event_id != candidate.claim.establishing_event_id
            || record.document_path != document_path
            || record.graph_iri != MetadataRegistryRecord::graph_iri_for(record.document_id)
            || record.permission_path
                != MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    document_path,
                    record.document_id,
                )
            || registry_placement(config, record) != placement)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(placement)
}

fn reduce_path_candidates(
    candidates: Vec<MetadataPathCandidate>,
) -> Result<MetadataPathLookupResult, MetadataApiError> {
    let claims = candidates
        .iter()
        .map(|candidate| candidate.claim.clone())
        .collect::<Vec<_>>();
    let resolution =
        aruna_core::structs::resolve_path_claim(&claims).ok_or(MetadataApiError::NotFound)?;
    let winner = candidates
        .iter()
        .filter(|candidate| candidate.claim == resolution.winner)
        .find_map(|candidate| candidate.record.clone())
        .ok_or(MetadataApiError::NotFound)?;
    let winner = sanitize_path_winner(winner)?;
    let conflicts = resolution
        .conflicts
        .iter()
        .filter_map(|claim| {
            candidates
                .iter()
                .filter(|candidate| candidate.claim == *claim)
                .find_map(|candidate| candidate.record.as_ref())
                .map(|record| record.document_id)
        })
        .collect::<Vec<_>>();
    Ok(MetadataPathLookupResult { winner, conflicts })
}

fn sanitize_path_winner(
    record: MetadataRegistryRecord,
) -> Result<MetadataPathWinner, MetadataApiError> {
    MetaResourceId::from_bytes(record.document_id.to_bytes())
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if record.graph_iri != MetadataRegistryRecord::graph_iri_for(record.document_id)
        || record.permission_path
            != MetadataRegistryRecord::permission_path_for(
                &record.realm_id,
                record.group_id,
                &record.document_path,
                record.document_id,
            )
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(MetadataPathWinner {
        realm_id: record.realm_id,
        group_id: record.group_id,
        document_id: record.document_id,
        document_path: record.document_path,
        graph_iri: record.graph_iri,
        public: record.public,
        replicas: record.holder_node_ids.len(),
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
    })
}

pub async fn get_visible_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    request: GetVisibleMetadataDocumentRequest,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record).await?;
    ensure_record_materialized_for_graph_read(context, &record).await?;
    Ok(record)
}

pub async fn export_metadata_rocrate(
    context: &DriverContext,
    realm_id: RealmId,
    request: ExportMetadataRoCrateRequest,
) -> Result<ExportMetadataRoCrateResult, MetadataApiError> {
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record).await?;

    match request.view {
        MetadataRoCrateExportView::Full => {
            ensure_record_materialized_for_graph_read(context, &record).await?;
            Ok(ExportMetadataRoCrateResult::Full {
                jsonld: export_rocrate_jsonld(context, &record.graph_iri).await?,
                record,
            })
        }
        MetadataRoCrateExportView::Summary => {
            ensure_record_materialized_for_graph_read(context, &record).await?;
            Ok(ExportMetadataRoCrateResult::Summary {
                jsonld: export_rocrate_summary_jsonld(
                    context,
                    &record.graph_iri,
                    record.last_event_id,
                )
                .await?,
                record,
            })
        }
        MetadataRoCrateExportView::Page => {
            ensure_record_materialized_for_graph_read(context, &record).await?;
            Ok(ExportMetadataRoCrateResult::Page {
                page: export_rocrate_page(
                    context,
                    &record.graph_iri,
                    request.limit,
                    request.offset,
                    request.after,
                )
                .await?,
                record,
            })
        }
        MetadataRoCrateExportView::Raw => {
            let raw = crate::metadata::raw::load_raw_view(context, record.document_id)
                .await
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?
                .ok_or(MetadataApiError::NotFound)?;
            let dataset_digest = raw.revision.dataset_digest;
            Ok(ExportMetadataRoCrateResult::Raw {
                record,
                raw,
                dataset_digest,
            })
        }
    }
}

pub async fn query_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataDocumentQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    ensure_supported_query_form(&request.query)?;
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record).await?;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    if request.mode == Some(MetadataApiQueryMode::Local) {
        ensure_record_materialized_for_graph_read(context, &record).await?;
        let results = metadata
            .query_authorized_local(request.auth, Some(vec![record.graph_iri]), request.query)
            .await
            .map_err(map_metadata_query_error)?;
        return Ok(MetadataQueryExecution {
            results,
            fanout_stats: MetadataFanoutStats {
                nodes_queried: 1,
                ..MetadataFanoutStats::default()
            },
        });
    }

    let Some(config) = load_realm_config(context, realm_id).await else {
        if !request.allow_partial {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        ensure_record_materialized_for_graph_read(context, &record).await?;
        let results = metadata
            .query_authorized_local(request.auth, Some(vec![record.graph_iri]), request.query)
            .await
            .map_err(map_metadata_query_error)?;
        return Ok(MetadataQueryExecution {
            results,
            fanout_stats: MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 1,
                discovery_failed: true,
                ..MetadataFanoutStats::default()
            },
        });
    };
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let mut holders = document_replica_query_nodes(Some(&config), &record, local_node_id);
    if let Some(index) = holders.iter().position(|holder| *holder == local_node_id) {
        holders.swap(0, index);
    }
    let remote_auth = match request.bearer_token.as_deref() {
        Some(token) => {
            Some(MetadataAuthToken::bearer(token).map_err(|_| MetadataApiError::BadRequest)?)
        }
        None => request.auth.clone().map(MetadataAuthToken::internal),
    };
    let mut fanout_stats = MetadataFanoutStats::default();
    for holder in holders {
        fanout_stats.nodes_queried += 1;
        let result = if holder == local_node_id {
            match ensure_record_materialized_for_graph_read(context, &record).await {
                Ok(()) => {
                    metadata
                        .query_authorized_local(
                            request.auth.clone(),
                            Some(vec![record.graph_iri.clone()]),
                            request.query.clone(),
                        )
                        .await
                }
                Err(error) => Err(MetadataError::Backend(error.to_string())),
            }
        } else {
            metadata
                .request_document_query(
                    holder,
                    remote_auth.clone(),
                    config_digest,
                    request.document_id,
                    request.query.clone(),
                )
                .await
        };
        match result {
            Ok(results) => {
                fanout_stats.nodes_failed = 0;
                fanout_stats.failed_partitions.clear();
                return Ok(MetadataQueryExecution {
                    results,
                    fanout_stats,
                });
            }
            Err(error) => {
                fanout_stats.nodes_failed += 1;
                fanout_stats.failed_partitions.push(holder);
                warn!(%holder, %error, "Document query holder failed; trying the next replica");
            }
        }
    }
    Err(MetadataApiError::ServiceUnavailable)
}

pub async fn query_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    ensure_supported_query_form(&request.query)?;
    let (results, fanout_stats) = run_query_distributed(
        context,
        realm_id,
        local_node_id,
        request.auth,
        request.bearer_token,
        request.graph_iris,
        request.query,
        MetadataFanoutScope::new(request.mode, request.target_nodes, request.allow_partial),
    )
    .await?;
    Ok(MetadataQueryExecution {
        results,
        fanout_stats,
    })
}

pub async fn search_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataSearchRequest,
) -> Result<MetadataSearchExecution, MetadataApiError> {
    if request.query.trim().is_empty() && request.conforms_to.is_none() {
        return Err(MetadataApiError::BadRequest);
    }
    if request
        .conforms_to
        .as_deref()
        .is_some_and(|iri| oxrdf::NamedNode::new(iri).is_err())
    {
        return Err(MetadataApiError::BadRequest);
    }
    let page_size = request
        .limit
        .unwrap_or(METADATA_SEARCH_DEFAULT_PAGE_SIZE)
        .clamp(1, METADATA_SEARCH_MAX_PAGE_SIZE);

    let fingerprint = query_fingerprint(
        &request.query,
        request.graph_iris.as_deref(),
        request.mode,
        request.conforms_to.as_deref(),
        request.group_id,
    );
    let mut cursor_discovery = None;
    let (watermark, resume) = match request.cursor.as_deref() {
        Some(raw) => {
            let signer_nodes = match request.mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => match request.target_nodes.clone() {
                    Some(nodes) => fanout_nodes_with_local(nodes, local_node_id),
                    None => {
                        let discovery =
                            load_metadata_realm_nodes_with_status(context, realm_id, local_node_id)
                                .await;
                        let nodes = discovery.nodes.clone();
                        cursor_discovery = Some(discovery);
                        nodes
                    }
                },
            };
            let cursor = SearchCursor::decode(raw, &signer_nodes)
                .map_err(|error| MetadataApiError::InvalidCursor(error.to_string()))?;
            if cursor.fingerprint != fingerprint {
                return Err(MetadataApiError::InvalidCursor(
                    SearchCursorError::QueryMismatch.to_string(),
                ));
            }
            (Some(cursor.watermark.clone()), cursor.resume_positions())
        }
        None => (None, HashMap::new()),
    };

    // On a continuation, attempt every node in the resume map even if realm
    // discovery no longer reports it, so its remaining hits are not skipped.
    let (target_nodes, discovery_failed) = if request.cursor.is_some() {
        let mut nodes = match request.target_nodes.clone() {
            Some(nodes) => nodes,
            None => match request.mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => cursor_discovery
                    .as_ref()
                    .map(|discovery| discovery.nodes.clone())
                    .unwrap_or_else(|| vec![local_node_id]),
            },
        };
        for node_id in resume.keys() {
            if !nodes.contains(node_id) {
                nodes.push(*node_id);
            }
        }
        (
            Some(deduplicate_fanout_nodes(nodes)),
            cursor_discovery
                .as_ref()
                .is_some_and(|discovery| discovery.failed),
        )
    } else {
        (request.target_nodes.clone(), false)
    };

    let (hits, next, truncated, fanout_stats) = run_search_distributed(
        context,
        realm_id,
        local_node_id,
        request.auth,
        request.bearer_token,
        request.graph_iris,
        request.query,
        request.conforms_to,
        request.group_id,
        resume,
        watermark,
        page_size,
        MetadataFanoutScope::new(request.mode, target_nodes, true)
            .with_discovery_failed(discovery_failed),
    )
    .await?;
    let next_cursor = match next {
        Some(cursor) => {
            let net = context.net_handle.as_ref().ok_or_else(|| {
                MetadataApiError::Internal(
                    "net handle unavailable for search cursor signing".to_string(),
                )
            })?;
            Some(
                SearchCursor::new_signed(
                    fingerprint,
                    cursor.watermark,
                    cursor.resume,
                    net.node_id(),
                    |bytes| net.sign(bytes),
                )
                .encode(),
            )
        }
        None => None,
    };
    Ok(MetadataSearchExecution {
        hits,
        next_cursor,
        truncated,
        fanout_stats,
    })
}

/// Reference lookup (backlinks). Scans the local IRI reference index for
/// documents that name `iri` as an object, joins each to its registry record,
/// and drops any the caller may not read. When the scan is empty and `iri` is a
/// known graph IRI, or when `resolve` is set, the matching document's summary is
/// returned as a single predicate-less entry. Local-node-only in v1.
pub async fn references_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataReferencesRequest,
) -> Result<MetadataReferencesExecution, MetadataApiError> {
    if request.iri.trim().is_empty() || oxrdf::NamedNode::new(&request.iri).is_err() {
        return Err(MetadataApiError::BadRequest);
    }
    if request
        .predicate
        .as_deref()
        .is_some_and(|iri| oxrdf::NamedNode::new(iri).is_err())
    {
        return Err(MetadataApiError::BadRequest);
    }
    let limit = request
        .limit
        .unwrap_or(METADATA_REFERENCES_DEFAULT_LIMIT)
        .clamp(1, METADATA_REFERENCES_MAX_LIMIT);

    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let registry = handle
        .list_cached_registry_records()
        .await
        .map_err(map_metadata_internal_error)?;

    if request.resolve {
        let entry = resolve_graph_reference(context, realm_id, &request, registry.as_ref()).await?;
        return Ok(MetadataReferencesExecution {
            references: entry.into_iter().collect(),
            next_cursor: None,
        });
    }

    let backlinks = super::iri_index::lookup_iri_backlinks(
        &context.storage_handle,
        registry.as_ref(),
        &request.iri,
        request.predicate.as_deref(),
    )
    .await
    .map_err(map_metadata_internal_error)?;

    let registry_by_id: HashMap<Ulid, &MetadataRegistryRecord> = registry
        .iter()
        .map(|record| (record.document_id, record))
        .collect();

    let mut references = Vec::new();
    let mut authorized: HashMap<Ulid, bool> = HashMap::new();
    let mut titles: HashMap<Ulid, Option<String>> = HashMap::new();
    for backlink in backlinks {
        let Some(record) = registry_by_id.get(&backlink.document_id) else {
            continue;
        };
        let allowed = match authorized.get(&backlink.document_id) {
            Some(allowed) => *allowed,
            None => {
                let allowed =
                    can_read_record(context, realm_id, request.auth.as_ref(), record).await?;
                authorized.insert(backlink.document_id, allowed);
                allowed
            }
        };
        if !allowed {
            continue;
        }
        let title = match titles.get(&backlink.document_id) {
            Some(title) => title.clone(),
            None => {
                let title = reference_document_title(context, record).await;
                titles.insert(backlink.document_id, title.clone());
                title
            }
        };
        references.push(MetadataReferenceEntry {
            document_id: record.document_id.to_string(),
            group_id: record.group_id.to_string(),
            document_path: record.document_path.clone(),
            graph_iri: record.graph_iri.clone(),
            predicate: Some(backlink.predicate_iri),
            subject_iris: backlink.subject_iris,
            title,
        });
        if references.len() >= limit {
            break;
        }
    }

    if references.is_empty()
        && let Some(entry) =
            resolve_graph_reference(context, realm_id, &request, registry.as_ref()).await?
    {
        references.push(entry);
    }

    Ok(MetadataReferencesExecution {
        references,
        next_cursor: None,
    })
}

async fn resolve_graph_reference(
    context: &DriverContext,
    realm_id: RealmId,
    request: &MetadataReferencesRequest,
    registry: &[MetadataRegistryRecord],
) -> Result<Option<MetadataReferenceEntry>, MetadataApiError> {
    let Some(record) = registry
        .iter()
        .find(|record| record.graph_iri == request.iri)
    else {
        return Ok(None);
    };
    if !can_read_record(context, realm_id, request.auth.as_ref(), record).await? {
        return Ok(None);
    }
    let title = reference_document_title(context, record).await;
    Ok(Some(MetadataReferenceEntry {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        graph_iri: record.graph_iri.clone(),
        predicate: None,
        subject_iris: Vec::new(),
        title,
    }))
}

async fn reference_document_title(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Option<String> {
    let handle = context.metadata_handle.clone()?;
    let properties = handle
        .describe_root_properties(record.graph_iri.clone())
        .await;
    // Root subject "./" makes the fallback the document path, not the id tail.
    let title = super::search_enrichment::hit_title(&properties, &record.document_path, "./");
    (!title.is_empty()).then_some(title)
}

pub async fn load_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    match drive(GetRealmConfigOperation::new(realm_id), context).await {
        Ok(config) => Some(config),
        Err(error) => {
            warn!(error = %error, "realm config unavailable; querying the local replica only");
            None
        }
    }
}

pub async fn load_metadata_realm_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> Vec<NodeId> {
    load_metadata_realm_nodes_with_status(context, realm_id, local_node_id)
        .await
        .nodes
}

pub(crate) async fn load_metadata_realm_nodes_with_status(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> MetadataRealmNodeDiscovery {
    let Some(config) = load_realm_config(context, realm_id).await else {
        return MetadataRealmNodeDiscovery {
            nodes: vec![local_node_id],
            failed: true,
        };
    };
    // Race discovery against REALM_DISCOVERY_TIMEOUT so fanout queries degrade
    // to reachable/local partitions instead of stalling behind offline peers.
    let discovery = tokio::time::timeout(
        REALM_DISCOVERY_TIMEOUT,
        drive(GetRealmNodesOperation::new(realm_id), context),
    )
    .await;
    let nodes = match discovery {
        Ok(Ok(nodes)) => match authorized_realm_nodes(&config, nodes) {
            Ok(nodes) => (nodes, false),
            Err(error) => {
                warn!(error = %error, "realm config contains invalid node ids; using local-only metadata results");
                return MetadataRealmNodeDiscovery {
                    nodes: vec![local_node_id],
                    failed: true,
                };
            }
        },
        Ok(Err(error)) => {
            warn!(
                error = %error,
                "realm node discovery failed, using best-effort local-only metadata results"
            );
            (HashSet::new(), true)
        }
        Err(_) => {
            warn!("realm node discovery timed out, using best-effort local-only metadata results");
            (HashSet::new(), true)
        }
    };
    let (nodes, failed) = nodes;
    let mut nodes = nodes.into_iter().collect::<Vec<_>>();
    if !nodes.contains(&local_node_id) {
        nodes.push(local_node_id);
    }
    nodes.sort_by_key(|node_id| node_id.to_string());
    MetadataRealmNodeDiscovery { nodes, failed }
}

fn authorized_realm_nodes(
    config: &RealmConfigDocument,
    nodes: HashSet<NodeId>,
) -> Result<HashSet<NodeId>, ConversionError> {
    let authorized = config
        .sync_eligible_node_ids()?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(nodes
        .into_iter()
        .filter(|node_id| authorized.contains(node_id))
        .collect())
}

fn effective_list_limit(requested: Option<usize>, anonymous: bool) -> usize {
    let maximum = if anonymous {
        ANONYMOUS_LIST_METADATA_LIMIT
    } else {
        MAX_LIST_METADATA_LIMIT
    };
    requested
        .unwrap_or(DEFAULT_LIST_METADATA_LIMIT)
        .clamp(1, maximum)
}

fn check_policy_limit(group_ids: Vec<GroupId>) -> Result<Vec<GroupId>, MetadataApiError> {
    if group_ids.len() > METADATA_REGISTRY_CANDIDATE_LIMIT {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(group_ids)
}

async fn load_group_records(
    context: &DriverContext,
    group_id: GroupId,
    limit: usize,
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    // Listing remains eventually consistent: the handle-owned visibility cache
    // serves stale snapshots while one refill updates the operation-owned read path.
    if let Some(metadata_handle) = context.metadata_handle.as_ref() {
        let group_records = metadata_handle
            .list_group_records(group_id, limit)
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        return Ok(group_records.as_ref().clone());
    }

    let mut records = Vec::new();
    let mut start_after = None;
    loop {
        let event = context
            .storage_handle
            .send_effect(iter_registry_effect(group_id, start_after, None))
            .await;
        let (page, next_start_after) =
            parse_registry_iter(event).map_err(|_| MetadataApiError::ServiceUnavailable)?;
        if records.len().saturating_add(page.len()) > limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        records.extend(page);
        match next_start_after {
            Some(cursor) => start_after = Some(cursor),
            None => break,
        }
    }
    let mut visible = Vec::with_capacity(records.len());
    for record in records {
        if !metadata_graph_is_deleted(context, &record.graph_iri).await? {
            visible.push(record);
        }
    }
    Ok(visible)
}

async fn load_claim_records(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: Option<GroupId>,
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    let group_ids = check_policy_limit(match group_id {
        Some(group_id) => vec![group_id],
        None => drive(
            ListGroupOperation::with_pagination(METADATA_REGISTRY_CANDIDATE_LIMIT + 1, 0),
            context,
        )
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?
        .into_iter()
        .map(|group| group.group_id)
        .collect(),
    })?;
    let mut pending =
        load_pending_records(context, group_id, METADATA_REGISTRY_CANDIDATE_LIMIT).await?;
    let mut records = Vec::new();
    for group_id in group_ids {
        let remaining = METADATA_REGISTRY_CANDIDATE_LIMIT.saturating_sub(records.len());
        let mut group_records = load_group_records(context, group_id, remaining).await?;
        if let Some(pending_records) = pending.remove(&group_id) {
            merge_pending_metadata_records(&mut group_records, pending_records);
            if group_records.len() > remaining {
                return Err(MetadataApiError::ServiceUnavailable);
            }
        }
        group_records.sort_by_key(|record| record.document_id);
        records.extend(group_records);
    }
    for pending_records in pending.into_values() {
        if records.len().saturating_add(pending_records.len()) > METADATA_REGISTRY_CANDIDATE_LIMIT {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        records.extend(pending_records);
    }
    records.retain(|record| record.realm_id == realm_id);
    Ok(records)
}

async fn load_pending_records(
    context: &DriverContext,
    group_filter: Option<GroupId>,
    limit: usize,
) -> Result<HashMap<GroupId, Vec<MetadataRegistryRecord>>, MetadataApiError> {
    let limit = limit.min(METADATA_REGISTRY_CANDIDATE_LIMIT);
    let mut targets = Vec::with_capacity(limit);
    let mut start_after = None;
    let mut scanned = 0usize;

    loop {
        let page = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: LIST_METADATA_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match page {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataApiError::Internal(error.to_string()));
            }
            other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
        };
        scanned = scanned.saturating_add(values.len());
        if scanned > limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }

        targets.extend(
            values
                .into_iter()
                .filter_map(|(key, _)| metadata_pending_projection_target(key.as_ref())),
        );

        if next_start_after.is_none() {
            break;
        }
        start_after = next_start_after;
    }

    if targets.is_empty() {
        return Ok(HashMap::new());
    }

    let event_reads = targets
        .iter()
        .map(|(document_id, event_id)| {
            (
                METADATA_EVENT_LOG_KEYSPACE.to_string(),
                metadata_event_log_key(*document_id, *event_id),
            )
        })
        .collect::<Vec<_>>();
    let event_values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads: event_reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values })
            if values.len() == targets.len() =>
        {
            values
        }
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            return Err(MetadataApiError::Internal(format!(
                "metadata pending event batch returned {} values for {} targets",
                values.len(),
                targets.len()
            )));
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataApiError::Internal(error.to_string()));
        }
        other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
    };

    let mut pending = Vec::with_capacity(event_values.len());
    for ((document_id, event_id), (key, value)) in targets.into_iter().zip(event_values) {
        if key != metadata_event_log_key(document_id, event_id) {
            return Err(MetadataApiError::Internal(
                "metadata pending event batch key mismatch".to_string(),
            ));
        }
        let Some(value) = value else {
            continue;
        };
        let event: MetadataCreateEventRecord = postcard::from_bytes(&value)
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        if event.record.document_id != document_id || event.event_id != event_id {
            return Err(MetadataApiError::Internal(format!(
                "metadata create event log target {document_id}/{event_id} did not match payload {}/{}",
                event.record.document_id, event.event_id
            )));
        }
        if group_filter.is_none_or(|group_id| event.record.group_id == group_id) {
            pending.push(event.record);
        }
    }

    if pending.is_empty() {
        return Ok(HashMap::new());
    }

    let lifecycle_reads = pending
        .iter()
        .map(|record| {
            (
                METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                metadata_graph_lifecycle_key(&record.graph_iri),
            )
        })
        .collect::<Vec<_>>();
    let lifecycle_values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads: lifecycle_reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values })
            if values.len() == pending.len() =>
        {
            values
        }
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            return Err(MetadataApiError::Internal(format!(
                "metadata pending lifecycle batch returned {} values for {} records",
                values.len(),
                pending.len()
            )));
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataApiError::Internal(error.to_string()));
        }
        other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
    };

    let mut records: HashMap<GroupId, Vec<MetadataRegistryRecord>> = HashMap::new();
    let mut count = 0usize;
    for (record, (key, value)) in pending.into_iter().zip(lifecycle_values) {
        if key != metadata_graph_lifecycle_key(&record.graph_iri) {
            return Err(MetadataApiError::Internal(
                "metadata pending lifecycle batch key mismatch".to_string(),
            ));
        }
        let deleted = value
            .map(|value| {
                postcard::from_bytes::<MetadataGraphLifecycleRecord>(&value)
                    .map(|record| record.is_deleted())
                    .map_err(|error| MetadataApiError::Internal(error.to_string()))
            })
            .transpose()?
            .unwrap_or(false);
        if deleted {
            continue;
        }
        if count >= limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        count += 1;
        records.entry(record.group_id).or_default().push(record);
    }
    Ok(records)
}

async fn metadata_graph_is_deleted(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<bool, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(graph_iri),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&value)
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
            Ok(record.is_deleted())
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
        other => Err(MetadataApiError::Internal(format!("{other:?}"))),
    }
}

fn merge_pending_metadata_records(
    records: &mut Vec<MetadataRegistryRecord>,
    pending_records: Vec<MetadataRegistryRecord>,
) {
    let mut positions = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.document_id, index))
        .collect::<HashMap<_, _>>();

    for pending_record in pending_records {
        if let Some(&index) = positions.get(&pending_record.document_id) {
            let existing_record = &records[index];
            if (pending_record.updated_at_ms, pending_record.last_event_id)
                > (existing_record.updated_at_ms, existing_record.last_event_id)
            {
                records[index] = pending_record;
            }
        } else {
            positions.insert(pending_record.document_id, records.len());
            records.push(pending_record);
        }
    }
}

async fn load_record_by_document(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    match load_metadata_record_by_document(context, document_id).await {
        Ok(Some(record)) => Ok(record),
        Ok(None) => Err(MetadataApiError::NotFound),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

async fn ensure_record_materialized_for_graph_read(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<(), MetadataApiError> {
    match is_metadata_record_materialized_for_graph_read(context, record).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(MetadataApiError::ServiceUnavailable),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

/// The canonical `metadata.read` policy request for one record path and caller,
/// shared by the single-record and bulk visibility seams.
pub(crate) fn metadata_read_request(
    permission_path: &str,
    user: Option<&aruna_core::UserId>,
) -> aruna_core::request_policy::PolicyRequest {
    crate::request_policy::policy_request_with(
        permission_path,
        &Permission::READ,
        user,
        crate::request_policy::PolicyRequestExtras::operation("metadata.read"),
    )
}

async fn ensure_record_readable(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
) -> Result<(), MetadataApiError> {
    if record.public {
        // A policy denial on a found public record must read as NotFound, matching
        // the private-denied path, so read-by-id is not an existence oracle.
        return crate::request_policy::enforce_policies(
            context,
            realm_id,
            &crate::request_policy::policy_request_with(
                &record.permission_path,
                &Permission::READ,
                auth.map(|auth| &auth.user_id),
                crate::request_policy::PolicyRequestExtras::operation("metadata.read"),
            ),
        )
        .await
        .map_err(|_| MetadataApiError::NotFound);
    }
    // Read-by-id must not distinguish an unreadable record from an absent one:
    // present-but-unreadable, including anonymous callers, maps to NotFound so
    // existence cannot be probed (anonymous returns NotFound, never 401).
    let Some(auth) = auth.cloned() else {
        return Err(MetadataApiError::NotFound);
    };
    match ensure_permission(
        context,
        realm_id,
        auth,
        record.permission_path.clone(),
        Permission::READ,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(MetadataApiError::Forbidden | MetadataApiError::Unauthorized) => {
            Err(MetadataApiError::NotFound)
        }
        Err(other) => Err(other),
    }
}

async fn can_read_record(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
) -> Result<bool, MetadataApiError> {
    if record.public {
        let allowed = crate::request_policy::enforce_policies(
            context,
            realm_id,
            &crate::request_policy::policy_request_with(
                &record.permission_path,
                &Permission::READ,
                auth.map(|auth| &auth.user_id),
                crate::request_policy::PolicyRequestExtras::operation("metadata.read"),
            ),
        )
        .await
        .is_ok();
        return Ok(allowed);
    }
    let Some(auth) = auth.cloned() else {
        return Ok(false);
    };
    if auth.realm_id != realm_id {
        return Ok(false);
    }

    match aruna_core::telemetry::time_stage(
        "permission",
        crate::request_authorization::authorize(
            context,
            realm_id,
            &auth,
            &record.permission_path,
            &Permission::READ,
            crate::request_policy::PolicyRequestExtras::operation("metadata.read"),
        ),
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

async fn ensure_permission(
    context: &DriverContext,
    realm_id: RealmId,
    auth: AuthContext,
    path: String,
    required_permission: Permission,
) -> Result<(), MetadataApiError> {
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let auth_user = auth.user_id;
    let allowed = aruna_core::telemetry::time_stage(
        "permission",
        drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth,
                path: path.clone(),
                required_permission: required_permission.clone(),
            }),
            context,
        ),
    )
    .await
    .map_err(|err| match err {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => MetadataApiError::Forbidden,
        _ => MetadataApiError::Internal(err.to_string()),
    })?;
    if !allowed {
        return Err(MetadataApiError::Forbidden);
    }
    crate::request_policy::enforce_policies(
        context,
        realm_id,
        &metadata_read_request(&path, Some(&auth_user)),
    )
    .await
    .map_err(|_| MetadataApiError::Forbidden)?;
    Ok(())
}

fn metadata_record_matches_filters(
    record: &MetadataRegistryRecord,
    path_prefix: Option<&str>,
) -> bool {
    path_prefix
        .map(|path_prefix| metadata_path_matches_prefix(&record.document_path, path_prefix))
        .unwrap_or(true)
}

fn metadata_path_matches_prefix(document_path: &str, path_prefix: &str) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(document_path);
    let normalized_prefix = MetadataRegistryRecord::normalize_document_path(path_prefix);
    normalized_prefix.is_empty()
        || normalized_path == normalized_prefix
        || normalized_path
            .strip_prefix(&normalized_prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

async fn export_rocrate_jsonld(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<String, MetadataApiError> {
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_jsonld(graph_iri.to_string())
        .await
        .map_err(map_metadata_event_error)
}

/// Summaries are cached per `(graph_iri, cursor)`. The lookup carries no
/// authorization data because it only runs once `can_read_record` accepted that
/// record; it MUST NOT be moved above that check.
async fn export_rocrate_summary_jsonld(
    context: &DriverContext,
    graph_iri: &str,
    cursor: Ulid,
) -> Result<String, MetadataApiError> {
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    // The handle rejects deleted graphs before every export, so a hit has to
    // re-check the authoritative lifecycle record itself.
    if let Some(summary) = summary_cache().get(graph_iri, cursor, Instant::now()) {
        if !metadata_graph_is_deleted(context, graph_iri).await? {
            return Ok(summary.to_string());
        }
        summary_cache().remove(graph_iri);
    }

    let summary = handle
        .export_rocrate_summary_jsonld(graph_iri.to_string())
        .await
        .map_err(map_metadata_event_error)?;
    summary_cache().insert(graph_iri, cursor, &summary, Instant::now());
    Ok(summary)
}

async fn export_rocrate_page(
    context: &DriverContext,
    graph_iri: &str,
    limit: Option<usize>,
    offset: Option<usize>,
    after: Option<String>,
) -> Result<MetadataRoCratePage, MetadataApiError> {
    if offset.is_some() && after.is_some() {
        return Err(MetadataApiError::BadRequest);
    }
    let limit = limit.unwrap_or(100).clamp(1, 1_000);
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_page(graph_iri.to_string(), limit, offset, after)
        .await
        .map_err(map_metadata_event_error)
}

fn map_metadata_event_error(error: MetadataError) -> MetadataApiError {
    match error {
        MetadataError::GraphNotFound => MetadataApiError::ServiceUnavailable,
        other => MetadataApiError::Internal(other.to_string()),
    }
}

fn map_metadata_query_error(error: MetadataError) -> MetadataApiError {
    match error {
        MetadataError::InvalidInput(_) => MetadataApiError::BadRequest,
        other => map_metadata_event_error(other),
    }
}

fn map_metadata_internal_error(error: MetadataError) -> MetadataApiError {
    MetadataApiError::Internal(error.to_string())
}

fn ensure_supported_query_mode(mode: &Option<MetadataApiQueryMode>) {
    match mode {
        None | Some(MetadataApiQueryMode::Local) | Some(MetadataApiQueryMode::Distributed) => {}
    }
}

fn ensure_supported_query_form(query: &str) -> Result<(), MetadataApiError> {
    if query.len() > METADATA_QUERY_MAX_BYTES {
        return Err(MetadataApiError::BadRequest);
    }
    let parsed = spargebra::SparqlParser::new()
        .parse_query(query)
        .map_err(|_| MetadataApiError::BadRequest)?;
    let pattern = match &parsed {
        spargebra::Query::Select { pattern, .. } | spargebra::Query::Ask { pattern, .. } => pattern,
        _ => return Err(MetadataApiError::BadRequest),
    };
    if graph_pattern_contains_service(pattern) {
        return Err(MetadataApiError::BadRequest);
    }
    if matches!(
        pattern,
        spargebra::algebra::GraphPattern::Slice {
            length: Some(length),
            ..
        } if *length > METADATA_QUERY_MAX_ROWS
    ) {
        return Err(MetadataApiError::BadRequest);
    }
    Ok(())
}

fn graph_pattern_contains_service(pattern: &spargebra::algebra::GraphPattern) -> bool {
    use spargebra::algebra::GraphPattern;

    match pattern {
        GraphPattern::Service { .. } => true,
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            graph_pattern_contains_service(left) || graph_pattern_contains_service(right)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            graph_pattern_contains_service(left)
                || graph_pattern_contains_service(right)
                || expression.as_ref().is_some_and(expression_contains_service)
        }
        GraphPattern::Filter { expr, inner } => {
            expression_contains_service(expr) || graph_pattern_contains_service(inner)
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => graph_pattern_contains_service(inner),
        GraphPattern::Extend {
            inner, expression, ..
        } => expression_contains_service(expression) || graph_pattern_contains_service(inner),
        GraphPattern::OrderBy { inner, expression } => {
            graph_pattern_contains_service(inner)
                || expression.iter().any(|expression| match expression {
                    spargebra::algebra::OrderExpression::Asc(expression)
                    | spargebra::algebra::OrderExpression::Desc(expression) => {
                        expression_contains_service(expression)
                    }
                })
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            graph_pattern_contains_service(inner)
                || aggregates.iter().any(|(_, aggregate)| match aggregate {
                    spargebra::algebra::AggregateExpression::CountSolutions { .. } => false,
                    spargebra::algebra::AggregateExpression::FunctionCall { expr, .. } => {
                        expression_contains_service(expr)
                    }
                })
        }
    }
}

fn expression_contains_service(expression: &spargebra::algebra::Expression) -> bool {
    use spargebra::algebra::Expression;

    match expression {
        Expression::Exists(pattern) => graph_pattern_contains_service(pattern),
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => false,
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            expression_contains_service(inner)
        }
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            expression_contains_service(left) || expression_contains_service(right)
        }
        Expression::In(left, right) => {
            expression_contains_service(left) || right.iter().any(expression_contains_service)
        }
        Expression::If(condition, left, right) => {
            expression_contains_service(condition)
                || expression_contains_service(left)
                || expression_contains_service(right)
        }
        Expression::Coalesce(expressions) | Expression::FunctionCall(_, expressions) => {
            expressions.iter().any(expression_contains_service)
        }
    }
}

fn distributed_query_is_union_safe(query: &str) -> bool {
    let Ok(parsed) = spargebra::SparqlParser::new().parse_query(query) else {
        return false;
    };
    match parsed {
        spargebra::Query::Select { pattern, .. } => {
            let pattern = match pattern {
                spargebra::algebra::GraphPattern::Slice {
                    inner, start: 0, ..
                } => *inner,
                spargebra::algebra::GraphPattern::Slice { .. } => return false,
                pattern => pattern,
            };
            let spargebra::algebra::GraphPattern::Distinct { inner } = pattern else {
                return false;
            };
            let spargebra::algebra::GraphPattern::Project { inner, .. } = *inner else {
                return false;
            };
            distributed_union_pattern_is_safe(&inner)
        }
        spargebra::Query::Ask { pattern, .. } => {
            let spargebra::algebra::GraphPattern::Project { inner, .. } = pattern else {
                return false;
            };
            distributed_union_pattern_is_safe(&inner)
        }
        _ => false,
    }
}

fn distributed_union_pattern_is_safe(pattern: &spargebra::algebra::GraphPattern) -> bool {
    match pattern {
        spargebra::algebra::GraphPattern::Bgp { patterns } => patterns.len() <= 1,
        spargebra::algebra::GraphPattern::Union { left, right } => {
            distributed_union_pattern_is_safe(left) && distributed_union_pattern_is_safe(right)
        }
        spargebra::algebra::GraphPattern::Graph { inner, .. } => {
            distributed_union_pattern_is_safe(inner)
        }
        _ => false,
    }
}

/// Nodes a document query fans out to: the live holders of the bucket the
/// document was created into, not the holder set stamped at event time (which a
/// rebalance leaves stale).
pub fn document_replica_query_nodes(
    config: Option<&RealmConfigDocument>,
    record: &MetadataRegistryRecord,
    local_node_id: NodeId,
) -> Vec<NodeId> {
    let holders = config
        .map(|config| resolve_shard_holders(config, &record.placement))
        .unwrap_or_default();
    let nodes = deduplicate_fanout_nodes(holders);
    if nodes.is_empty() {
        vec![local_node_id]
    } else {
        nodes
    }
}

pub fn deduplicate_fanout_nodes(nodes: Vec<NodeId>) -> Vec<NodeId> {
    let mut seen = HashSet::with_capacity(nodes.len());
    nodes
        .into_iter()
        .filter(|node_id| seen.insert(*node_id))
        .collect()
}

fn fanout_nodes_with_local(mut nodes: Vec<NodeId>, local_node_id: NodeId) -> Vec<NodeId> {
    if !nodes.contains(&local_node_id) {
        nodes.push(local_node_id);
    }
    deduplicate_fanout_nodes(nodes)
}

pub fn forwarded_bearer(
    token: Option<&str>,
) -> Result<Option<MetadataAuthToken>, MetadataApiError> {
    token
        .map(MetadataAuthToken::bearer)
        .transpose()
        .map_err(|_| MetadataApiError::BadRequest)
}

fn fanout_bearer(token: Option<&str>) -> Option<MetadataAuthToken> {
    token.and_then(|token| MetadataAuthToken::bearer(token).ok())
}

type MetadataNodeCall<T> =
    Arc<dyn Fn(NodeId) -> BoxFuture<'static, Result<T, MetadataError>> + Send + Sync>;

fn metadata_node_call<C, T, F, Fut>(context: C, call: F) -> MetadataNodeCall<T>
where
    C: Clone + Send + Sync + 'static,
    T: Send + 'static,
    F: Fn(C, NodeId) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, MetadataError>> + Send + 'static,
{
    Arc::new(move |node_id| {
        let context = context.clone();
        call(context, node_id).boxed()
    })
}

#[derive(Clone, Copy)]
enum MetadataFanoutOperation {
    Query,
    Search,
    BucketSearch,
}

impl MetadataFanoutOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Search => "search",
            Self::BucketSearch => "bucket_search",
        }
    }

    fn timeout_error(self) -> MetadataError {
        MetadataError::Backend(format!(
            "distributed metadata {} node timed out after {}ms",
            self.label(),
            METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT.as_millis()
        ))
    }
}

fn metadata_fanout_node_span(
    operation: MetadataFanoutOperation,
    node_id: NodeId,
    local: bool,
) -> Span {
    match operation {
        MetadataFanoutOperation::Query => debug_span!(
            "metadata.operation.query_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::Search => debug_span!(
            "metadata.operation.search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::BucketSearch => debug_span!(
            "metadata.operation.bucket_search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
    }
}

async fn run_metadata_fanout_node<T>(
    operation: MetadataFanoutOperation,
    node_id: NodeId,
    local: bool,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataError>),
    record_stage_detail: bool,
) -> Result<T, MetadataError> {
    let node_span = metadata_fanout_node_span(operation, node_id, local);
    let node_started = Instant::now();
    let result = if local {
        local_call(node_id).instrument(node_span.clone()).await
    } else {
        match tokio::time::timeout(
            METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT,
            remote_call(node_id).instrument(node_span.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(operation.timeout_error()),
        }
    };
    let elapsed = record_elapsed_ms(&node_span, "elapsed_ms", node_started);
    if record_stage_detail {
        aruna_core::telemetry::record_stage_detail(
            "fanout_node",
            || short_display_id(node_id),
            elapsed,
        );
    }
    record_result(&node_span, &result);
    result
}

async fn metadata_fanout_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    span: &Span,
    target_nodes: Option<Vec<NodeId>>,
) -> MetadataRealmNodeDiscovery {
    match target_nodes {
        Some(nodes) => {
            span.record("discovery_ms", 0u64);
            MetadataRealmNodeDiscovery {
                nodes: deduplicate_fanout_nodes(nodes),
                failed: false,
            }
        }
        None => {
            let discovery_started = Instant::now();
            let discovery = aruna_core::telemetry::time_stage(
                "discovery",
                load_metadata_realm_nodes_with_status(context, realm_id, local_node_id),
            )
            .await;
            record_elapsed_ms(span, "discovery_ms", discovery_started);
            discovery
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_metadata_fanout<T>(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    scope: MetadataFanoutScope,
    operation: MetadataFanoutOperation,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataError>),
    map_local_error: fn(MetadataError) -> MetadataApiError,
) -> Result<(Vec<(NodeId, T)>, MetadataFanoutStats), MetadataApiError>
where
    T: Send + 'static,
{
    let span = Span::current();
    let MetadataFanoutScope {
        mode,
        target_nodes,
        allow_partial,
        discovery_failed: scope_discovery_failed,
    } = scope;
    ensure_supported_query_mode(&mode);
    match mode.unwrap_or(MetadataApiQueryMode::Distributed) {
        MetadataApiQueryMode::Local => {
            let result = run_metadata_fanout_node(
                operation,
                local_node_id,
                true,
                local_call,
                remote_call,
                record_result,
                false,
            )
            .await;
            let fanout_stats = MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 0,
                failed_partitions: Vec::new(),
                discovery_failed: false,
            };
            match result {
                Ok(result) => Ok((vec![(local_node_id, result)], fanout_stats)),
                Err(error) => Err(map_local_error(error)),
            }
        }
        MetadataApiQueryMode::Distributed => {
            let discovery =
                metadata_fanout_nodes(context, realm_id, local_node_id, &span, target_nodes).await;
            let discovery_failed = scope_discovery_failed || discovery.failed;
            let mut nodes = discovery.nodes;
            if discovery_failed && !allow_partial {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            let all_nodes = nodes.clone();
            let mut failed_partitions = Vec::new();
            if matches!(
                operation,
                MetadataFanoutOperation::Query | MetadataFanoutOperation::BucketSearch
            ) && nodes.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES
            {
                if let Some(local_index) = nodes
                    .iter()
                    .position(|node_id| *node_id == local_node_id)
                    .filter(|index| *index >= METADATA_DISTRIBUTED_QUERY_MAX_NODES)
                {
                    nodes.swap(local_index, METADATA_DISTRIBUTED_QUERY_MAX_NODES - 1);
                }
                failed_partitions.extend(nodes.drain(METADATA_DISTRIBUTED_QUERY_MAX_NODES..));
                if !allow_partial {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
            }
            span.record("node_count", nodes.len() as u64);
            let mut fanout_stats = MetadataFanoutStats {
                nodes_queried: nodes.len(),
                nodes_failed: failed_partitions.len() + usize::from(discovery_failed),
                failed_partitions,
                discovery_failed,
            };
            let fanout_started = Instant::now();
            let mut node_parts = Vec::new();
            let node_order = all_nodes
                .into_iter()
                .enumerate()
                .map(|(index, node_id)| (node_id, index))
                .collect::<HashMap<_, _>>();
            let mut outstanding = nodes.iter().copied().collect::<HashSet<_>>();
            // Every interactive fanout gets the overall deadline; offline
            // partitions land in failed_partitions and partial-tolerant
            // callers (search) still answer from the reachable nodes.
            let deadline = Some(tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE);

            let pending =
                stream::iter(nodes.into_iter().enumerate().map(|(node_index, node_id)| {
                    let local_call = local_call.clone();
                    let remote_call = remote_call.clone();
                    async move {
                        let result = run_metadata_fanout_node(
                            operation,
                            node_id,
                            node_id == local_node_id,
                            local_call,
                            remote_call,
                            record_result,
                            true,
                        )
                        .await;
                        (node_index, node_id, result)
                    }
                }))
                .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT);
            futures_util::pin_mut!(pending);

            loop {
                let next = match deadline {
                    Some(deadline) => match tokio::time::timeout_at(deadline, pending.next()).await
                    {
                        Ok(next) => next,
                        Err(_) => {
                            fanout_stats
                                .failed_partitions
                                .extend(outstanding.iter().copied());
                            fanout_stats.nodes_failed = fanout_stats.failed_partitions.len()
                                + usize::from(fanout_stats.discovery_failed);
                            if !allow_partial {
                                return Err(MetadataApiError::ServiceUnavailable);
                            }
                            break;
                        }
                    },
                    None => pending.next().await,
                };
                let Some((node_index, node_id, result)) = next else {
                    break;
                };
                outstanding.remove(&node_id);
                match result {
                    Ok(result) => node_parts.push((node_index, node_id, result)),
                    Err(error) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            error = %error,
                            "distributed metadata skipped failed node result"
                        );
                        if !allow_partial {
                            return Err(MetadataApiError::ServiceUnavailable);
                        }
                    }
                }
            }

            node_parts.sort_by_key(|(node_index, _, _)| *node_index);
            fanout_stats
                .failed_partitions
                .sort_by_key(|node_id| node_order.get(node_id).copied().unwrap_or(usize::MAX));
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
            Ok((
                node_parts
                    .into_iter()
                    .map(|(_, node_id, result)| (node_id, result))
                    .collect(),
                fanout_stats,
            ))
        }
    }
}

pub async fn search_buckets_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: BucketSearchRequest,
) -> Result<BucketSearchExecution, MetadataApiError> {
    let limit = request.limit.clamp(1, 50);
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(request.bearer_token.as_deref());
    let local_call: MetadataNodeCall<Vec<BucketSearchHit>> = metadata_node_call(
        (
            context.clone(),
            request.auth,
            realm_id,
            request.query.clone(),
            limit,
        ),
        |(context, auth, realm_id, query, limit), node_id| async move {
            search_local_buckets(
                &context,
                SearchBucketsInput {
                    auth,
                    realm_id,
                    node_id,
                    query,
                    limit,
                    start_after: None,
                },
            )
            .await
            .map_err(|error| MetadataError::Backend(error.to_string()))
        },
    );
    let remote_call: MetadataNodeCall<Vec<BucketSearchHit>> = metadata_node_call(
        (handle, remote_auth_token, request.query, limit),
        |(handle, auth_token, query, limit), node_id| async move {
            handle
                .request_bucket_search(node_id, auth_token, query, limit)
                .await
        },
    );
    let (parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(
            Some(MetadataApiQueryMode::Distributed),
            request.target_nodes,
            true,
        ),
        MetadataFanoutOperation::BucketSearch,
        local_call,
        remote_call,
        record_bucket_result,
        map_metadata_internal_error,
    )
    .await?;
    let mut hits = parts
        .into_iter()
        .flat_map(|(_, hits)| hits)
        .collect::<Vec<_>>();
    hits.truncate(limit);
    Ok(BucketSearchExecution { hits, fanout_stats })
}

fn record_bucket_result(span: &Span, result: &Result<Vec<BucketSearchHit>, MetadataError>) {
    match result {
        Ok(hits) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_query_node_result(span: &Span, result: &Result<MetadataQueryResults, MetadataError>) {
    match result {
        Ok(result) => {
            span.record("result", result.kind());
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_search_node_result(
    span: &Span,
    result: &Result<(Vec<MetadataSearchHit>, usize), MetadataError>,
) {
    match result {
        Ok((hits, _)) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

#[tracing::instrument(
    name = "metadata.operation.query_distributed",
    level = "debug",
    skip(context, auth, bearer_token, query, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        cache = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
async fn run_query_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    scope: MetadataFanoutScope,
) -> Result<(MetadataQueryResults, MetadataFanoutStats), MetadataApiError> {
    let span = Span::current();
    let total_started = Instant::now();
    let mode = scope.mode.unwrap_or(MetadataApiQueryMode::Distributed);
    let single_dataset_result = mode == MetadataApiQueryMode::Local || graph_iris.is_some();
    if mode == MetadataApiQueryMode::Distributed
        && graph_iris.is_none()
        && !distributed_query_is_union_safe(&query)
    {
        return Err(MetadataApiError::BadRequest);
    }
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let query_form = query_form(&query).ok_or(MetadataApiError::BadRequest)?;
    let select_limit = match query_form {
        MetadataQueryForm::Select => query_select_limit(&query),
        MetadataQueryForm::Ask => None,
    };
    let remote_auth_token = fanout_bearer(bearer_token.as_deref());

    // Remote partitions authorize on the forwarded credential, so entries are
    // partitioned by credential digest. The local invalidation signals only
    // cover the local partition; the TTL bounds remote staleness.
    let cache_stamp = handle.query_cache().stamp(handle.visibility_generation());
    let cache_key = super::query_cache::credential_digest(auth.as_ref(), bearer_token.as_deref())
        .map(|credential| {
            super::query_cache::remote_key(&super::query_cache::RemoteKeyInput {
                distributed: mode == MetadataApiQueryMode::Distributed,
                realm_id,
                credential: &credential,
                graph_iris: graph_iris.as_deref(),
                sparql: &query,
                allow_partial: scope.allow_partial,
                target_nodes: scope.target_nodes.as_deref(),
            })
        })
        .filter(|_| !scope.discovery_failed);
    if let Some(key) = cache_key
        && let Some(cached) = handle.query_cache().get(&key, cache_stamp, Instant::now())
    {
        span.record("cache", "hit");
        span.record("result", cached.results.kind());
        record_elapsed_ms(&span, "elapsed_ms", total_started);
        return Ok((
            (*cached.results).clone(),
            super::query_cache::cached_stats(&cached),
        ));
    }
    span.record("cache", "miss");

    let local_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
        ),
        |(handle, auth, graph_iris, query), _| async move {
            handle.query_authorized_local(auth, graph_iris, query).await
        },
    );
    let remote_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
        ),
        |(handle, auth_token, graph_iris, query), node_id| async move {
            handle
                .request_remote_query_graphs(node_id, auth_token, graph_iris, query)
                .await
        },
    );
    let (parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Query,
        local_call,
        remote_call,
        record_query_node_result,
        map_metadata_query_error,
    )
    .await?;

    let parts: Vec<_> = parts.into_iter().map(|(_, result)| result).collect();
    let result = if single_dataset_result {
        match parts.into_iter().next() {
            Some(result) => Ok(result),
            None => aggregate_query_results(Vec::new(), query_form, select_limit),
        }
    } else {
        aggregate_query_results(parts, query_form, select_limit)
    };
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    match &result {
        Ok(results) => {
            span.record("result", results.kind());
            if let Some(key) = cache_key
                && super::query_cache::store_complete(
                    handle.query_cache(),
                    key,
                    results,
                    &fanout_stats,
                    cache_stamp,
                    handle.visibility_generation(),
                    Instant::now(),
                )
            {
                span.record("cache", "stored");
            }
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
    result.map(|results| (results, fanout_stats))
}

#[tracing::instrument(
    name = "metadata.operation.search_distributed",
    level = "debug",
    skip(context, auth, bearer_token, query, resume, watermark, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        page_size = page_size as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        hit_count = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
async fn run_search_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    conforms_to: Option<String>,
    group_id: Option<GroupId>,
    resume: HashMap<NodeId, u32>,
    watermark: Option<SearchWatermark>,
    page_size: usize,
    scope: MetadataFanoutScope,
) -> Result<
    (
        Vec<MetadataSearchHit>,
        Option<SearchPageCursor>,
        bool,
        MetadataFanoutStats,
    ),
    MetadataApiError,
> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(bearer_token.as_deref());
    let resume = Arc::new(resume);

    let local_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to.clone(),
            group_id,
            resume.clone(),
            page_size,
        ),
        |(handle, auth, graph_iris, query, conforms_to, group_id, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = match conforms_to {
                Some(object_iri) => {
                    handle
                        .search_authorized_local_filtered(
                            auth,
                            graph_iris,
                            query,
                            limit,
                            super::iri_index::DCTERMS_CONFORMS_TO_IRI.to_string(),
                            object_iri,
                            group_id,
                        )
                        .await?
                }
                None => {
                    handle
                        .search_authorized_local(auth, graph_iris, query, limit, group_id)
                        .await?
                }
            };
            Ok((hits, limit))
        },
    );
    let remote_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to,
            group_id,
            resume.clone(),
            page_size,
        ),
        |(handle, auth_token, graph_iris, query, conforms_to, group_id, resume, page_size),
         node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = match conforms_to {
                Some(object_iri) => {
                    handle
                        .request_remote_filtered_search_graphs(
                            node_id,
                            auth_token,
                            graph_iris,
                            query,
                            limit,
                            super::iri_index::DCTERMS_CONFORMS_TO_IRI.to_string(),
                            object_iri,
                            group_id,
                        )
                        .await?
                }
                None => {
                    handle
                        .request_remote_search_graphs(
                            node_id, auth_token, graph_iris, query, limit, group_id,
                        )
                        .await?
                }
            };
            Ok((hits, limit))
        },
    );
    let (node_parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        record_search_node_result,
        map_metadata_internal_error,
    )
    .await?;

    let node_results = node_parts
        .into_iter()
        .map(|(node_id, (hits, requested))| NodeSearchResult {
            node_id,
            saturated: hits.len() >= requested,
            hits,
        })
        .collect();
    let page = paginate(
        node_results,
        watermark,
        page_size,
        METADATA_SEARCH_MAX_PAGINATION_DEPTH,
    );
    span.record("hit_count", page.hits.len() as u64);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok((page.hits, page.next, page.truncated, fanout_stats))
}

pub fn aggregate_query_results(
    results: Vec<MetadataQueryResults>,
    query_form: MetadataQueryForm,
    select_limit: Option<usize>,
) -> Result<MetadataQueryResults, MetadataApiError> {
    match query_form {
        MetadataQueryForm::Ask => {
            Ok(MetadataQueryResults::Boolean(results.into_iter().any(
                |result| matches!(result, MetadataQueryResults::Boolean(true)),
            )))
        }
        MetadataQueryForm::Select => {
            let mut seen = HashSet::new();
            let mut merged = Vec::new();
            let mut merged_bytes = 32usize;
            let row_limit = select_limit
                .unwrap_or(METADATA_QUERY_MAX_ROWS)
                .min(METADATA_QUERY_MAX_ROWS);
            if row_limit == 0 {
                return Ok(MetadataQueryResults::Solutions(Vec::new()));
            }
            for result in results {
                let MetadataQueryResults::Solutions(rows) = result else {
                    continue;
                };
                for row in rows {
                    let key = serde_json::to_string(&row)
                        .map_err(|err| MetadataApiError::Internal(err.to_string()))?;
                    if seen.insert(key) {
                        merged_bytes = merged_bytes.saturating_add(
                            serde_json::to_vec(&row)
                                .map_err(|err| MetadataApiError::Internal(err.to_string()))?
                                .len()
                                .saturating_add(1),
                        );
                        if merged_bytes > METADATA_QUERY_MAX_RESULT_BYTES {
                            return Err(MetadataApiError::BadRequest);
                        }
                        merged.push(row);
                        if merged.len() >= row_limit {
                            return Ok(MetadataQueryResults::Solutions(merged));
                        }
                    }
                }
            }
            Ok(MetadataQueryResults::Solutions(merged))
        }
    }
}

pub fn query_select_limit(query: &str) -> Option<usize> {
    let parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select { pattern, .. } = parsed else {
        return None;
    };
    let spargebra::algebra::GraphPattern::Slice { length, .. } = pattern else {
        return None;
    };
    length
}

pub fn query_form(query: &str) -> Option<MetadataQueryForm> {
    match spargebra::SparqlParser::new().parse_query(query).ok()? {
        spargebra::Query::Select { .. } => Some(MetadataQueryForm::Select),
        spargebra::Query::Ask { .. } => Some(MetadataQueryForm::Ask),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use aruna_core::UserId;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::metadata::MetadataCreateEventPayload;
    use aruna_core::storage_entries::metadata_create_event_and_pending_projection_write_entries;
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, PlacementRef, RealmAuthorizationDocument, Role,
    };
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::types::{Key, RoleId};
    use aruna_storage::storage;
    use tempfile::{TempDir, tempdir};

    use crate::metadata::MetadataHandle;

    const TEST_REALM_ID: RealmId = RealmId([7u8; 32]);

    struct MetadataTest {
        context: DriverContext,
        _storage_dir: TempDir,
        _metadata_dir: TempDir,
    }

    fn metadata_test() -> MetadataTest {
        let storage_dir = tempdir().expect("storage dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().expect("storage path"))
                .expect("storage opens");
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            storage_handle.clone(),
            None,
            None,
            None,
        )
        .expect("metadata handle");
        MetadataTest {
            context: DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: Some(metadata_handle),
                task_handle: None,
                compute_handle: None,
            },
            _storage_dir: storage_dir,
            _metadata_dir: metadata_dir,
        }
    }

    fn public_record(group_id: GroupId, document_id: Ulid) -> MetadataRegistryRecord {
        let event_id = Ulid::generate();
        let document_id = MetaResourceId::from_parts(
            document_id.timestamp_ms(),
            PlacementHandle::new(1).unwrap(),
            BucketId::new(0).unwrap(),
            document_id.0 as u64 & ((1_u64 << 48) - 1),
        )
        .unwrap()
        .as_ulid();
        let document_path = format!("datasets/cached/{document_id}");
        MetadataRegistryRecord {
            realm_id: TEST_REALM_ID,
            group_id,
            document_id,
            document_path: document_path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &TEST_REALM_ID,
                group_id,
                &document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: event_id,
            last_event_id: event_id,
        }
    }

    fn summary_request(
        group_id: GroupId,
        include_summary: bool,
    ) -> ListVisibleMetadataDocumentsRequest {
        ListVisibleMetadataDocumentsRequest {
            group_id: Some(group_id),
            path_prefix: None,
            include_summary,
            limit: None,
            offset: None,
            order: MetadataListOrder::default(),
            auth: None,
        }
    }

    // The visibility cache only accepts upserts once it has been filled.
    async fn seed_registry_cache(test: &MetadataTest, record: &MetadataRegistryRecord) {
        let handle = test
            .context
            .metadata_handle
            .as_ref()
            .expect("metadata handle");
        handle
            .list_cached_registry_records_for_group(record.group_id)
            .await
            .expect("registry cache fills");
        handle.upsert_cached_registry_record(record.clone());
    }

    async fn write_pending_marker(test: &MetadataTest, record: &MetadataRegistryRecord) {
        let event = MetadataCreateEventRecord {
            event_id: record.last_event_id,
            record: record.clone(),
            user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Pending".to_string(),
                description: "Projection in flight".to_string(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
            occurred_at_ms: 1,
        };
        for (key_space, key, value) in
            metadata_create_event_and_pending_projection_write_entries(&event)
                .expect("event encodes")
        {
            match test
                .context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected write event: {other:?}"),
            }
        }
    }

    #[test]
    fn anonymous_limit_clamped() {
        assert_eq!(
            effective_list_limit(None, true),
            DEFAULT_LIST_METADATA_LIMIT
        );
        assert_eq!(
            effective_list_limit(Some(MAX_LIST_METADATA_LIMIT), true),
            ANONYMOUS_LIST_METADATA_LIMIT
        );
        assert_eq!(
            effective_list_limit(Some(MAX_LIST_METADATA_LIMIT), false),
            MAX_LIST_METADATA_LIMIT
        );
        assert_eq!(
            effective_list_limit(Some(usize::MAX), false),
            MAX_LIST_METADATA_LIMIT
        );
        assert_eq!(effective_list_limit(Some(0), true), 1);
    }

    #[test]
    fn policy_scope_limit() {
        let within = (0..METADATA_REGISTRY_CANDIDATE_LIMIT)
            .map(|_| Ulid::generate())
            .collect();
        assert!(check_policy_limit(within).is_ok());

        let over = (0..=METADATA_REGISTRY_CANDIDATE_LIMIT)
            .map(|_| Ulid::generate())
            .collect();
        assert!(matches!(
            check_policy_limit(over),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn peers_are_bounded() {
        let local = iroh::SecretKey::from_bytes(&[255u8; 32]).public();
        let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 2);
        config.ensure_node(local, aruna_core::structs::RealmNodeKind::Server);
        for seed in 1u8..=40 {
            config.ensure_node(
                iroh::SecretKey::from_bytes(&[seed; 32]).public(),
                aruna_core::structs::RealmNodeKind::Server,
            );
        }
        config.nodes.push(aruna_core::structs::RealmNode {
            node_id: "invalid-node".to_string(),
            kind: aruna_core::structs::RealmNodeKind::Server,
        });
        let mut reversed = config.clone();
        reversed.nodes.reverse();
        let first = select_forward_peers(
            &config,
            TEST_REALM_ID,
            Ulid::from_parts(0, 1),
            "datasets/lookup",
            local,
        )
        .expect("peer selection succeeds");
        let second = select_forward_peers(
            &reversed,
            TEST_REALM_ID,
            Ulid::from_parts(0, 1),
            "datasets/lookup",
            local,
        )
        .expect("peer selection succeeds");
        assert_eq!(first.len(), METADATA_DISTRIBUTED_QUERY_MAX_NODES);
        assert_eq!(first, second);
    }

    #[test]
    fn peer_timeout_fits() {
        let waves =
            (METADATA_DISTRIBUTED_QUERY_MAX_NODES + METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT - 1)
                / METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT;
        assert!(
            METADATA_PATH_PEER_TIMEOUT.as_secs() * waves as u64
                <= METADATA_DISTRIBUTED_QUERY_DEADLINE.as_secs()
        );
    }

    #[test]
    fn metadata_read_operation() {
        let request = metadata_read_request("/realm/g/group/meta/document", None);
        assert_eq!(request.operation, "metadata.read");
    }

    // The record lives only in the registry cache and the graph was never
    // projected, so a returned summary can only come from the summary cache.
    #[tokio::test]
    async fn summary_from_cache() {
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        seed_registry_cache(&test, &record).await;
        summary_cache().insert(
            &record.graph_iri,
            record.last_event_id,
            "{\"cached\":true}",
            Instant::now(),
        );

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, true),
        )
        .await
        .expect("summary listing succeeds");

        assert_eq!(result.documents.len(), 1);
        assert_eq!(
            result.documents[0].rocrate_summary_jsonld.as_deref(),
            Some("{\"cached\":true}")
        );
    }

    #[tokio::test]
    async fn stale_summary_refused() {
        // A cursor advance must fall through to the handle, not serve the entry.
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        seed_registry_cache(&test, &record).await;
        summary_cache().insert(
            &record.graph_iri,
            Ulid::generate(),
            "{\"stale\":true}",
            Instant::now(),
        );

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, true),
        )
        .await
        .expect("summary listing succeeds");

        assert_eq!(result.documents.len(), 1);
        assert!(result.documents[0].rocrate_summary_jsonld.is_none());
    }

    #[tokio::test]
    async fn pending_summary_listed() {
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        write_pending_marker(&test, &record).await;

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, true),
        )
        .await
        .expect("summary listing succeeds");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].record.document_id, record.document_id);
        assert!(result.documents[0].rocrate_summary_jsonld.is_none());

        let plain = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, false),
        )
        .await
        .expect("plain listing succeeds");
        assert!(plain.documents.is_empty());
    }

    // The page window must not truncate the estimate, and paging must not move it.
    #[tokio::test]
    async fn estimate_beyond_page() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let seeded = METADATA_ESTIMATE_MIN_LIMIT + 2;
        for _ in 0..seeded {
            seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
        }

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(page.documents.len(), METADATA_ESTIMATE_MIN_LIMIT);
        assert_eq!(page.total_returned, METADATA_ESTIMATE_MIN_LIMIT);
        assert_eq!(page.total_estimate, Some(seeded));

        let tail = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
                offset: Some(seeded - 1),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(tail.documents.len(), 1);
        assert_eq!(tail.total_estimate, Some(seeded));
    }

    // A targeted lookup must not pay for the realm-wide estimate scan, and
    // must report the estimate as absent rather than as a truncated count.
    #[tokio::test]
    async fn lookup_omits_estimate() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        for _ in 0..3 {
            seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
        }

        let lookup = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT - 1),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(lookup.total_returned, 3);
        assert_eq!(lookup.total_estimate, None);

        let browse = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(browse.total_estimate, Some(3));
    }

    // Anonymous callers collect no rules, so only public records count.
    #[tokio::test]
    async fn estimate_skips_private() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let readable = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &readable).await;
        let mut private = public_record(group_id, Ulid::generate());
        private.public = false;
        seed_registry_cache(&test, &private).await;

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .expect("listing succeeds");

        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].record.document_id, readable.document_id);
        assert_eq!(result.total_estimate, Some(1));
    }

    #[tokio::test]
    async fn cross_shard_unknown() {
        // Local listings cannot resolve claims that may live on another registry shard.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 3);
        config.seed_default_placement();
        let mut first = public_record(group_id, Ulid::generate());
        let first_shard = registry_placement(&config, &first).shard;
        let mut second = loop {
            let candidate = public_record(group_id, Ulid::generate());
            if registry_placement(&config, &candidate).shard != first_shard {
                break candidate;
            }
        };
        second.document_path = first.document_path.clone();
        second.permission_path = MetadataRegistryRecord::permission_path_for(
            &TEST_REALM_ID,
            group_id,
            &second.document_path,
            second.document_id,
        );
        let claims = [&first, &second]
            .into_iter()
            .map(|record| PathClaimRecord {
                document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
                establishing_event_id: record.establishing_event_id,
                requested_path: record.document_path.clone(),
            })
            .collect::<Vec<_>>();
        let resolution = aruna_core::structs::resolve_path_claim(&claims).unwrap();
        let winner_id = resolution.winner.document_id.as_ulid();
        let loser_id = resolution.conflicts[0].document_id.as_ulid();
        first.public = first.document_id == loser_id;
        second.public = second.document_id == loser_id;
        seed_registry_cache(&test, &first).await;
        seed_registry_cache(&test, &second).await;

        let listed = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .unwrap();
        assert_eq!(listed.documents.len(), 1);
        assert_eq!(listed.documents[0].record.document_id, loser_id);
        assert_ne!(winner_id, loser_id);
    }

    #[test]
    fn user_result_opaque() {
        // An unreadable winner still participates and cannot promote a readable loser.
        let group_id = Ulid::generate();
        let first = public_record(group_id, Ulid::generate());
        let mut second = public_record(group_id, Ulid::generate());
        second.document_path = first.document_path.clone();
        let mut candidates = [&first, &second]
            .into_iter()
            .map(|record| MetadataPathCandidate {
                claim: PathClaimRecord {
                    document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
                    establishing_event_id: record.establishing_event_id,
                    requested_path: record.document_path.clone(),
                },
                record: Some(record.clone()),
            })
            .collect::<Vec<_>>();
        let claims = candidates
            .iter()
            .map(|candidate| candidate.claim.clone())
            .collect::<Vec<_>>();
        let winner = aruna_core::structs::resolve_path_claim(&claims)
            .unwrap()
            .winner;
        let hidden_id = winner.document_id.to_bytes();
        candidates
            .iter_mut()
            .find(|candidate| candidate.claim == winner)
            .unwrap()
            .record = None;

        let result = reduce_path_candidates(candidates);
        assert!(matches!(result, Err(MetadataApiError::NotFound)));
        let response = MetadataTransportMessage::ForwardedPathResolution {
            result: Err(MetadataReadError::NotFound),
        };
        let encoded = postcard::to_allocvec(&response).unwrap();
        assert!(
            !encoded
                .windows(hidden_id.len())
                .any(|window| window == hidden_id)
        );
    }

    fn path_candidate(record: &MetadataRegistryRecord) -> MetadataPathCandidate {
        MetadataPathCandidate {
            claim: PathClaimRecord {
                document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
                establishing_event_id: record.establishing_event_id,
                requested_path: record.document_path.clone(),
            },
            record: Some(record.clone()),
        }
    }

    #[test]
    fn missing_replica_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let views = vec![PathShardView {
            shard: 0,
            candidates: vec![path_candidate(&record)],
        }];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn stale_replica_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let views = vec![
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&record)],
            },
            PathShardView {
                shard: 0,
                candidates: Vec::new(),
            },
        ];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn divergent_claims_fail() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let mut divergent = record.clone();
        divergent.establishing_event_id = Ulid::generate();
        let views = vec![
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&record)],
            },
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&divergent)],
            },
        ];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn divergent_evidence_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let mut divergent = record.clone();
        divergent.public = false;
        let views = vec![
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&record)],
            },
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&divergent)],
            },
        ];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn invalid_resolution_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let mut unrelated = MetadataPathResolution {
            winner: sanitize_path_winner(record.clone()).unwrap(),
            conflicts: Vec::new(),
        };
        unrelated.winner.group_id = Ulid::generate();
        assert!(matches!(
            validate_path_resolution(
                TEST_REALM_ID,
                record.group_id,
                &record.document_path,
                &unrelated,
            ),
            Err(MetadataApiError::ServiceUnavailable)
        ));

        let duplicate = MetadataPathResolution {
            winner: sanitize_path_winner(record.clone()).unwrap(),
            conflicts: vec![record.document_id],
        };
        assert!(matches!(
            validate_path_resolution(
                TEST_REALM_ID,
                record.group_id,
                &record.document_path,
                &duplicate,
            ),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn winner_wire_sanitized() {
        let mut record = public_record(Ulid::generate(), Ulid::generate());
        let holder = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        record.holder_node_ids = vec![holder];
        let permission_path = record.permission_path.clone();
        let winner = sanitize_path_winner(record).unwrap();
        let encoded = postcard::to_allocvec(&winner).unwrap();

        assert!(
            !encoded
                .windows(holder.as_bytes().len())
                .any(|window| window == holder.as_bytes())
        );
        assert!(
            !encoded
                .windows(permission_path.len())
                .any(|window| window == permission_path.as_bytes())
        );
    }

    fn auth_for(user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: TEST_REALM_ID,
            path_restrictions: None,
        }
    }

    fn user_role(
        user_id: UserId,
        permissions: HashMap<String, Permission>,
    ) -> HashMap<RoleId, Role> {
        let role_id = Ulid::generate();
        HashMap::from([(
            role_id,
            Role {
                role_id,
                name: "listing".to_string(),
                permissions,
                assigned_users: HashSet::from([user_id]),
            },
        )])
    }

    // The rules collection reads both documents; without them a group yields no
    // rules and every non-public record in it stays hidden.
    async fn write_auth_docs(test: &MetadataTest, group_id: GroupId, roles: HashMap<RoleId, Role>) {
        write_policy_docs(test, group_id, roles, Vec::new()).await;
    }

    async fn write_policy_docs(
        test: &MetadataTest,
        group_id: GroupId,
        roles: HashMap<RoleId, Role>,
        policies: Vec<aruna_core::request_policy::RequestPolicy>,
    ) {
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
            realm_id: TEST_REALM_ID,
        };
        let realm_doc = RealmAuthorizationDocument::new_default_realm_doc(TEST_REALM_ID);
        let group = Group {
            display_name: "Test".to_string(),
            group_id,
            realm_id: TEST_REALM_ID,
            owner: actor.user_id,
            roles: roles.keys().copied().collect(),
        };
        let group_doc = GroupAuthorizationDocument {
            group_id,
            roles,
            policies,
        };
        // The policy evaluator reads the group through GetGroupOperation, which
        // needs the group record as well as the auth doc.
        let entries = [
            (
                AUTH_KEYSPACE,
                Key::from(*TEST_REALM_ID.as_bytes()),
                realm_doc.to_bytes(&actor).expect("realm doc encodes"),
            ),
            (
                AUTH_KEYSPACE,
                Key::from(group_id.to_bytes()),
                group_doc.to_bytes(&actor).expect("group doc encodes"),
            ),
            (
                GROUP_KEYSPACE,
                Key::from(group_id.to_bytes()),
                group.to_bytes(&actor).expect("group encodes"),
            ),
        ];
        for (key_space, key, value) in entries {
            match test
                .context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key,
                    value: value.into(),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected write event: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn hidden_ids_match() {
        // Missing, private-denied, and policy-denied public ids must all return
        // NotFound so read-by-id cannot probe existence.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
        write_policy_docs(
            &test,
            group_id,
            user_role(
                UserId::local(Ulid::generate(), TEST_REALM_ID),
                HashMap::from([(
                    format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                    Permission::WRITE,
                )]),
            ),
            vec![aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "no-reads".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: "permission == 'read'".to_string(),
                enabled: true,
            }],
        )
        .await;

        let public = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &public).await;
        let mut private = public_record(group_id, Ulid::generate());
        private.public = false;
        seed_registry_cache(&test, &private).await;
        let missing = public_record(group_id, Ulid::generate());

        for document_id in [public.document_id, private.document_id, missing.document_id] {
            let result = get_visible_metadata_document(
                &test.context,
                TEST_REALM_ID,
                GetVisibleMetadataDocumentRequest {
                    document_id,
                    auth: Some(auth_for(stranger)),
                },
            )
            .await;
            assert!(matches!(result, Err(MetadataApiError::NotFound)));
        }
    }

    #[tokio::test]
    async fn policy_hides_record() {
        // A group deny policy removes one public record from the bulk listing
        // while leaving an allowed public record visible.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
        let hidden = public_record(group_id, Ulid::generate());
        let visible = public_record(group_id, Ulid::generate());
        write_policy_docs(
            &test,
            group_id,
            HashMap::new(),
            vec![aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "hide-one".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: format!("path.contains('{}')", hidden.document_id),
                enabled: true,
            }],
        )
        .await;
        seed_registry_cache(&test, &hidden).await;
        seed_registry_cache(&test, &visible).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(stranger)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&page), vec![visible.document_id]);
        assert_eq!(page.total_estimate, Some(1));
    }

    // A caller who holds no role in the group sees the public records only, and
    // the counts describe the visible set rather than the scanned one.
    #[tokio::test]
    async fn stranger_sees_public() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
        write_auth_docs(
            &test,
            group_id,
            user_role(
                UserId::local(Ulid::generate(), TEST_REALM_ID),
                HashMap::from([(
                    format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                    Permission::WRITE,
                )]),
            ),
        )
        .await;
        let visible = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &visible).await;
        for _ in 0..2 {
            let mut hidden = public_record(group_id, Ulid::generate());
            hidden.public = false;
            seed_registry_cache(&test, &hidden).await;
        }

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(stranger)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&page), vec![visible.document_id]);
        assert_eq!(page.total_returned, 1);
        assert_eq!(page.total_estimate, Some(1));

        let beyond = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                offset: Some(1),
                auth: Some(auth_for(stranger)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert!(beyond.documents.is_empty());
        assert_eq!(beyond.total_returned, 0);
        assert_eq!(beyond.total_estimate, Some(1));
    }

    // An unauthenticated caller must not inherit a member's grants.
    #[tokio::test]
    async fn anonymous_sees_public() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let member = UserId::local(Ulid::generate(), TEST_REALM_ID);
        write_auth_docs(
            &test,
            group_id,
            user_role(
                member,
                HashMap::from([(
                    format!("/{TEST_REALM_ID}/g/{group_id}/meta/**"),
                    Permission::READ,
                )]),
            ),
        )
        .await;
        let visible = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &visible).await;
        let mut hidden = public_record(group_id, Ulid::generate());
        hidden.public = false;
        seed_registry_cache(&test, &hidden).await;

        let anonymous = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&anonymous), vec![visible.document_id]);
        assert_eq!(anonymous.total_estimate, Some(1));

        let signed = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(member)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(signed.total_returned, 2);
        assert_eq!(signed.total_estimate, Some(2));
    }

    #[tokio::test]
    async fn foreign_policy_identity() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let foreign_realm = RealmId([8u8; 32]);
        let foreign_user = UserId::local(Ulid::generate(), foreign_realm);
        let record = public_record(group_id, Ulid::generate());
        write_policy_docs(
            &test,
            group_id,
            HashMap::new(),
            vec![aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "foreign-user".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Require,
                when: None,
                expression: format!("user == '{foreign_user}'"),
                enabled: true,
            }],
        )
        .await;
        seed_registry_cache(&test, &record).await;
        let auth = AuthContext {
            user_id: foreign_user,
            realm_id: foreign_realm,
            path_restrictions: None,
        };

        let listed = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth.clone()),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&listed), vec![record.document_id]);

        let candidates = local_path_candidates(
            &test.context,
            TEST_REALM_ID,
            group_id,
            &record.document_path,
            Some(&auth),
        )
        .await
        .expect("path lookup succeeds");
        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].record.is_some());
    }

    // A per-document DENY inside a group-wide grant: the estimate must decide
    // each document, not reuse one representative answer for the whole group.
    #[tokio::test]
    async fn estimate_counts_exact() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let member = UserId::local(Ulid::generate(), TEST_REALM_ID);
        let mut allowed = public_record(group_id, Ulid::generate());
        allowed.public = false;
        let mut denied = public_record(group_id, Ulid::generate());
        denied.public = false;
        write_auth_docs(
            &test,
            group_id,
            user_role(
                member,
                HashMap::from([
                    (
                        format!("/{TEST_REALM_ID}/g/{group_id}/meta/**"),
                        Permission::READ,
                    ),
                    (denied.permission_path.clone(), Permission::DENY),
                ]),
            ),
        )
        .await;
        seed_registry_cache(&test, &allowed).await;
        seed_registry_cache(&test, &denied).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(member)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&page), vec![allowed.document_id]);
        assert_eq!(page.total_estimate, Some(1));

        // A targeted lookup still reports no estimate for the same caller.
        let lookup = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT - 1),
                auth: Some(auth_for(member)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(lookup.total_returned, 1);
        assert_eq!(lookup.total_estimate, None);
    }

    // path_prefix must scope the estimate to the same set the page came from.
    #[tokio::test]
    async fn estimate_honours_prefix() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        for _ in 0..2 {
            seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
        }
        let mut other = public_record(group_id, Ulid::generate());
        other.document_path = "other/excluded".to_string();
        seed_registry_cache(&test, &other).await;

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                path_prefix: Some("datasets".to_string()),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");

        assert_eq!(result.total_returned, 2);
        assert_eq!(result.total_estimate, Some(2));
    }

    // Update stamps deliberately disagree with the ascending document ids.
    async fn seed_timed_records(
        test: &MetadataTest,
        group_id: GroupId,
    ) -> Vec<MetadataRegistryRecord> {
        let mut records = Vec::new();
        for updated_at_ms in [10u64, 30, 20] {
            let mut record = public_record(group_id, Ulid::generate());
            record.updated_at_ms = updated_at_ms;
            seed_registry_cache(test, &record).await;
            records.push(record);
        }
        records
    }

    fn listed_ids(result: &ListVisibleMetadataDocumentsResult) -> Vec<Ulid> {
        result
            .documents
            .iter()
            .map(|document| document.record.document_id)
            .collect()
    }

    // Recency ordering must precede the offset window so pages walk it too.
    #[tokio::test]
    async fn orders_recent_first() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let records = seed_timed_records(&test, group_id).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                order: MetadataListOrder::Recent,
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(
            listed_ids(&page),
            vec![
                records[1].document_id,
                records[2].document_id,
                records[0].document_id
            ]
        );

        let second = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(1),
                offset: Some(1),
                order: MetadataListOrder::Recent,
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&second), vec![records[2].document_id]);
    }

    // The default page stays in ascending document id order.
    #[tokio::test]
    async fn default_keeps_created() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let records = seed_timed_records(&test, group_id).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .expect("listing succeeds");

        let mut expected = records
            .iter()
            .map(|record| record.document_id)
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(listed_ids(&page), expected);
    }

    #[test]
    fn deduplicates_select_rows_from_multiple_nodes() {
        let results = aggregate_query_results(
            vec![
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
                ]),
                MetadataQueryResults::Solutions(vec![BTreeMap::from([(
                    String::from("s"),
                    String::from("<urn:a>"),
                )])]),
            ],
            MetadataQueryForm::Select,
            None,
        )
        .unwrap();

        let MetadataQueryResults::Solutions(rows) = results else {
            panic!("expected solutions");
        };
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn reapplies_select_limit_after_distributed_merge() {
        let results = aggregate_query_results(
            vec![
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
                ]),
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:c>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:d>"))]),
                ]),
            ],
            MetadataQueryForm::Select,
            Some(3),
        )
        .unwrap();

        let MetadataQueryResults::Solutions(rows) = results else {
            panic!("expected solutions");
        };
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn query_select_limit_reads_outermost_limit_only() {
        assert_eq!(
            query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 5"),
            Some(5)
        );
        assert_eq!(
            query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 7 OFFSET 3"),
            Some(7)
        );
        assert_eq!(query_select_limit("SELECT ?s WHERE { ?s ?p ?o }"), None);
        assert_eq!(
            query_select_limit(
                "SELECT ?s WHERE { { SELECT ?s WHERE { ?s ?p ?o } LIMIT 5 } ?s ?p ?o }"
            ),
            None
        );
        assert_eq!(query_select_limit("ASK WHERE { ?s ?p ?o }"), None);
        assert_eq!(query_select_limit("not sparql"), None);
    }

    #[test]
    fn query_form_accepts_single_line_declarations() {
        assert_eq!(
            query_form("PREFIX ex: <https://example.org/> SELECT ?s WHERE { ?s ?p ?o }").unwrap(),
            MetadataQueryForm::Select
        );
        assert_eq!(
            query_form("BASE <https://example.org/> ASK WHERE { ?s ?p ?o }").unwrap(),
            MetadataQueryForm::Ask
        );
        assert_eq!(query_form("CONSTRUCT WHERE { ?s ?p ?o }"), None);
    }

    #[test]
    fn query_validation_rejects_updates_and_service() {
        assert!(ensure_supported_query_form("SELECT ?s WHERE { ?s ?p ?o }").is_ok());
        assert!(ensure_supported_query_form("ASK WHERE { ?s ?p ?o }").is_ok());
        assert!(ensure_supported_query_form("INSERT DATA { <urn:s> <urn:p> <urn:o> }").is_err());
        assert!(
            ensure_supported_query_form(
                "SELECT ?s WHERE { SERVICE <https://example.org/sparql> { ?s ?p ?o } }"
            )
            .is_err()
        );
        assert!(
            ensure_supported_query_form(
                "ASK WHERE { FILTER EXISTS { SERVICE SILENT ?endpoint { ?s ?p ?o } } }"
            )
            .is_err()
        );
    }

    #[test]
    fn distributed_query_validation_accepts_only_union_safe_forms() {
        assert!(distributed_query_is_union_safe("ASK WHERE { ?s ?p ?o }"));
        assert!(!distributed_query_is_union_safe(
            "ASK WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"
        ));
        assert!(distributed_query_is_union_safe(
            "SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 10"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT ?s WHERE { ?s ?p ?o }"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT DISTINCT ?s WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT DISTINCT ?s WHERE { ?s ?p ?o } OFFSET 1"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"
        ));
    }

    #[test]
    fn query_validation_enforces_byte_and_row_bounds() {
        assert!(ensure_supported_query_form(&" ".repeat(METADATA_QUERY_MAX_BYTES + 1)).is_err());
        assert!(
            ensure_supported_query_form(&format!(
                "SELECT ?s WHERE {{ ?s ?p ?o }} LIMIT {}",
                METADATA_QUERY_MAX_ROWS + 1
            ))
            .is_err()
        );
    }

    // Fan-out follows the live holders of the stored bucket; the event-time
    // holder stamp on the record is ignored, and no config means local only.
    #[test]
    fn query_fans_out_to_holders() {
        let local_node_id = iroh::SecretKey::from_bytes(&[21u8; 32]).public();
        let remote_node_id = iroh::SecretKey::from_bytes(&[22u8; 32]).public();
        let stale_node_id = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
        let realm_id = RealmId([3u8; 32]);
        let document_id = Ulid::generate();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.seed_default_placement();
        config.ensure_node(local_node_id, aruna_core::structs::RealmNodeKind::Server);
        config.ensure_node(remote_node_id, aruna_core::structs::RealmNodeKind::Server);
        let strategy = config
            .strategy(&config.default_strategy_id.expect("default strategy"))
            .expect("default strategy resolves");
        let placement = crate::placement::choose_origin_bucket(
            &config,
            strategy,
            local_node_id,
            &document_id.to_bytes(),
        )
        .expect("origin holds a bucket");

        let record = MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::generate(),
            document_id,
            document_path: "datasets/query-targets".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: "/metadata/query-targets".to_string(),
            placement,
            holder_node_ids: vec![stale_node_id],
            created_at_ms: 0,
            updated_at_ms: 0,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        };

        let nodes = document_replica_query_nodes(Some(&config), &record, local_node_id);
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&local_node_id) && nodes.contains(&remote_node_id));
        assert!(!nodes.contains(&stale_node_id));

        assert_eq!(
            document_replica_query_nodes(None, &record, local_node_id),
            vec![local_node_id]
        );
    }

    #[test]
    fn fanout_filters_nodes() {
        let server = iroh::SecretKey::from_bytes(&[24u8; 32]).public();
        let user = iroh::SecretKey::from_bytes(&[25u8; 32]).public();
        let unknown = iroh::SecretKey::from_bytes(&[26u8; 32]).public();
        let mut config = RealmConfigDocument::new(RealmId([4u8; 32]), Vec::new(), 2);
        config.ensure_node(server, aruna_core::structs::RealmNodeKind::Server);
        config.ensure_node(user, aruna_core::structs::RealmNodeKind::User);

        let nodes = authorized_realm_nodes(&config, HashSet::from([server, user, unknown]))
            .expect("valid node ids");

        assert_eq!(nodes, HashSet::from([server]));
    }

    #[test]
    fn deduplicate_fanout_nodes_preserves_first_seen_order() {
        let first = iroh::SecretKey::from_bytes(&[31u8; 32]).public();
        let second = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
        let third = iroh::SecretKey::from_bytes(&[33u8; 32]).public();

        assert_eq!(
            deduplicate_fanout_nodes(vec![first, second, first, third, second]),
            vec![first, second, third]
        );
    }

    #[tokio::test]
    async fn bucket_fanout_partial() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[41u8; 32]).public();
        let healthy = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        let failed = iroh::SecretKey::from_bytes(&[43u8; 32]).public();
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call(failed, |failed, node_id| async move {
                if node_id == failed {
                    Err(MetadataError::Backend("offline".to_string()))
                } else {
                    Ok(2)
                }
            });

        let (parts, stats) = run_metadata_fanout(
            &context,
            RealmId::from_bytes([9u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, healthy, failed]),
                true,
            ),
            MetadataFanoutOperation::BucketSearch,
            local_call,
            remote_call,
            |_, _| {},
            map_metadata_internal_error,
        )
        .await
        .unwrap();

        assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
        assert_eq!(stats.nodes_queried, 3);
        assert_eq!(stats.nodes_failed, 1);
        assert_eq!(stats.failed_partitions, vec![failed]);
    }

    #[tokio::test(start_paused = true)]
    async fn search_deadline_partial() {
        // A node that never answers must not stall search fanout: the overall
        // deadline fails its partition and the reachable nodes still answer.
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[44u8; 32]).public();
        let healthy = iroh::SecretKey::from_bytes(&[45u8; 32]).public();
        let hanging = iroh::SecretKey::from_bytes(&[46u8; 32]).public();
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call(hanging, |hanging, node_id| async move {
                if node_id == hanging {
                    std::future::pending::<Result<usize, MetadataError>>().await
                } else {
                    Ok(2)
                }
            });

        let (parts, stats) = run_metadata_fanout(
            &context,
            RealmId::from_bytes([10u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, healthy, hanging]),
                true,
            ),
            MetadataFanoutOperation::Search,
            local_call,
            remote_call,
            |_, _| {},
            map_metadata_internal_error,
        )
        .await
        .unwrap();

        assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
        assert_eq!(stats.nodes_queried, 3);
        assert_eq!(stats.nodes_failed, 1);
        assert_eq!(stats.failed_partitions, vec![hanging]);
    }

    #[test]
    fn bearer_limits() {
        assert!(matches!(
            forwarded_bearer(Some(&"x".repeat(4096))),
            Ok(Some(MetadataAuthToken::Bearer(_)))
        ));
        assert!(matches!(
            forwarded_bearer(Some(&"x".repeat(4097))),
            Err(MetadataApiError::BadRequest)
        ));
        assert!(fanout_bearer(Some(&"x".repeat(4097))).is_none());
        assert!(matches!(forwarded_bearer(None), Ok(None)));
    }
}
