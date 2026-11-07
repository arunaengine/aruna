//! Searches metadata graphs, buckets and objects across nodes with signed page cursors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    ApiQueryMode, AuthContext, BlobHeadKey, BucketSearchHit, CursorEnvelopeError,
    DISTRIBUTED_QUERY_DEADLINE, Deserialize, DriverContext, GroupId, HashMap, HashSet,
    MetadataApiError, MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats,
    MetadataNodeCall, MetadataReadError, MetadataSearchHit, NodeId, ObjectInventoryHit,
    ObjectKeyMatch, QUERY_MAX_NODES, RealmId, RealmNodeDiscovery, SearchCursor, SearchCursorError,
    SearchNodePage, SearchObjectsInput, Serialize, SignedCursor, SystemTime,
    deduplicate_fanout_nodes, discover_realm_nodes, fanout_bearer, load_realm_config,
    map_read_error, metadata_node_call, object_search_fingerprint, query_fingerprint,
    record_object_result, run_metadata_fanout, search_local_objects, select_fanout_nodes,
};

use super::distributed::run_search_distributed;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::index_feed::IndexProgress;
use crate::metadata::search_cursor::{SEARCH_MAX_PAGE, SEARCH_PAGE_SIZE};
use crate::s3::object::search::changed_until;

const OBJECT_CURSOR_VERSION: u8 = 1;

const CURSOR_SIGNATURE_CONTEXT: &[u8] = b"aruna.object.search.cursor.v1";

const CURSOR_MAX_BYTES: usize = 64 * 1024;

const SEARCH_MAX_BYTES: usize = 2 * 1024;

/// Skipped distributed buckets one realm-wide page searches by name; the rest wait in its cursor.
const MAX_SKIPPED_SEARCHES: usize = 8;

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
    pub mode: Option<ApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
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
    /// An empty query lists every readable bucket.
    pub query: String,
    /// Admits a bucket by READ on this key or key prefix inside it instead of on its root.
    pub key: Option<String>,
    /// An MCP tool's policy context, operation and parameters, whose policies must also allow
    /// each hit on each node before members merge; it only adds to the mandatory read check.
    pub extras: Option<PolicyRequestExtras>,
    pub limit: usize,
    /// Only buckets of this group.
    pub group_id: Option<GroupId>,
    /// `next_cursor` of the previous page.
    pub cursor: Option<String>,
    pub target_nodes: Option<Vec<NodeId>>,
    /// Only buckets whose metadata marks them as a database.
    pub database: bool,
}

#[derive(Debug, Clone)]
pub struct BucketSearchExecution {
    pub hits: Vec<BucketSearchHit>,
    /// Continues the listing; `None` once every node reached its end.
    pub next_cursor: Option<String>,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectQueryMode {
    Local,
    DistributedBestEffort,
    DistributedStrict,
}

impl ObjectQueryMode {
    fn fanout_mode(self) -> ApiQueryMode {
        match self {
            Self::Local => ApiQueryMode::Local,
            Self::DistributedBestEffort | Self::DistributedStrict => ApiQueryMode::Distributed,
        }
    }

    pub(super) fn allow_partial(self) -> bool {
        !matches!(self, Self::DistributedStrict)
    }
}

#[derive(Debug, Clone)]
pub struct SearchQueryRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub query: String,
    pub key_match: ObjectKeyMatch,
    pub bucket: Option<String>,
    pub limit: usize,
    pub cursor: Option<String>,
    pub mode: ObjectQueryMode,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct ObjectPartitionCoverage {
    pub node_id: NodeId,
    pub observed_at: SystemTime,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct ObjectExecution {
    pub hits: Vec<ObjectInventoryHit>,
    pub next_cursor: Option<String>,
    pub as_of: SystemTime,
    pub partitions: Vec<ObjectPartitionCoverage>,
    pub fanout_stats: MetadataFanoutStats,
    pub omitted_partitions: usize,
    /// Distributed bucket ranges this page could not search: a missing or building index,
    /// or in local mode a range another member coordinates.
    pub incomplete_ranges: usize,
    /// Index progress of incomplete ranges, by the node that coordinates them.
    pub index_progress: Vec<(NodeId, ulid::Ulid, IndexProgress)>,
    /// Incomplete ranges answered up to a key, by their coordinator.
    pub partial_ranges: Vec<(NodeId, ulid::Ulid, Option<String>)>,
    pub complete: bool,
    /// Readable keys of the first page's snapshot replaced or deleted since, through this page;
    /// `None` once a searched range index changed after the snapshot.
    pub changed_keys: Option<usize>,
    /// The part of `changed_keys` this page passed.
    pub page_changed: usize,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectPartitionState {
    pub(super) node_id: NodeId,
    pub(super) start_after: Option<Vec<u8>>,
    pub(super) exhausted: bool,
    pub(super) observed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ObjectCursorPartition {
    node_id: [u8; 32],
    start_after: Option<Vec<u8>>,
    exhausted: bool,
    observed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ObjectCursorPayload {
    pub(super) as_of: SystemTime,
    pub(super) partitions: Vec<ObjectCursorPartition>,
    pub(super) failed_partitions: Vec<[u8; 32]>,
    pub(super) discovery_failed: bool,
    pub(super) omitted_partitions: usize,
    pub(super) pending: Vec<PendingBucket>,
    pub(super) changed_keys: Option<usize>,
    pub(super) layout: RangeLayout,
}

/// The key ranges of a distributed bucket the pages of one search were answered under.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum RangeLayout {
    /// No page searched a distributed bucket yet.
    #[default]
    Unknown,
    Seen([u8; 32]),
    /// A range changed coordinator or bounds between pages or between nodes, so a member may
    /// have skipped keys another member no longer searched.
    Changed,
}

impl RangeLayout {
    pub(super) fn observe(self, digest: [u8; 32]) -> Self {
        match self {
            Self::Unknown => Self::Seen(digest),
            Self::Seen(seen) if seen == digest => self,
            Self::Seen(_) | Self::Changed => Self::Changed,
        }
    }
}

/// A distributed bucket a realm-wide search still owes: its name and the cursor of its own
/// bucket search, `None` before its first page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PendingBucket {
    pub(super) bucket: String,
    pub(super) cursor: Option<String>,
}

pub(super) type ObjectCursor = SignedCursor<ObjectCursorPayload>;

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
        let cursor = Self::decode_verified(
            raw,
            CURSOR_SIGNATURE_CONTEXT,
            authorized_signers,
            |cursor| {
                if cursor.version != OBJECT_CURSOR_VERSION || cursor.fingerprint != fingerprint {
                    Err(CursorEnvelopeError::QueryMismatch)
                } else {
                    Ok(())
                }
            },
        )
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
                    let plain = !crate::s3::object::search::sealed_position(key);
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
            CURSOR_SIGNATURE_CONTEXT,
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

pub async fn search_objects(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: SearchQueryRequest,
) -> Result<ObjectExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE;
    search_until(context, (realm_id, local_node_id), request, deadline).await
}

/// One search page that ends by `deadline`, which its distributed bucket searches share.
async fn search_until(
    context: &DriverContext,
    (realm_id, local_node_id): (RealmId, NodeId),
    request: SearchQueryRequest,
    deadline: tokio::time::Instant,
) -> Result<ObjectExecution, MetadataApiError> {
    let plan = plan_object_search(realm_id, request)?;
    let partitions =
        resolve_object_partitions(context, realm_id, local_node_id, &plan, deadline).await?;
    let (mut parts, fanout_stats) = run_object_fanout(
        context,
        realm_id,
        local_node_id,
        &plan,
        &partitions,
        deadline,
    )
    .await?;
    let mut partitions = partitions;
    let mut queue = std::mem::take(&mut partitions.pending);
    for bucket in take_skipped(&mut parts) {
        if !queue.iter().any(|pending| pending.bucket == bucket) {
            queue.push(PendingBucket {
                bucket,
                cursor: None,
            });
        }
    }
    let mut found = Vec::new();
    let mut failed = 0;
    for (index, pending) in queue.into_iter().enumerate() {
        if index >= MAX_SKIPPED_SEARCHES {
            partitions.pending.push(pending);
            continue;
        }
        let ids = (realm_id, local_node_id);
        match Box::pin(search_skipped(context, ids, &plan, &pending, deadline)).await {
            Ok(page) => {
                if let Some(cursor) = page.next_cursor.clone() {
                    partitions.pending.push(PendingBucket {
                        bucket: pending.bucket,
                        cursor: Some(cursor),
                    });
                }
                found.push(page);
            }
            Err(error) if plan.mode == ObjectQueryMode::DistributedStrict => return Err(error),
            Err(_) => failed += 1,
        }
    }
    // The cursor carries the rest; what would not fit in it stays unsearched.
    let size = |pending: &[PendingBucket]| {
        (pending.iter())
            .map(|entry| entry.bucket.len() + entry.cursor.as_ref().map_or(0, String::len))
            .sum::<usize>()
    };
    while size(&partitions.pending) > CURSOR_MAX_BYTES / 2 {
        partitions.pending.pop();
        failed += 1;
    }
    let found_changed = found.iter().map(|page| page.page_changed).sum::<usize>();
    let certain = found.iter().all(|page| page.changed_keys.is_some());
    partitions.changed_keys =
        (partitions.changed_keys.filter(|_| certain)).map(|keys| keys + found_changed);
    let mut execution = assemble_object_execution(context, &plan, partitions, parts, fanout_stats)?;
    execution.page_changed += found_changed;
    for page in found {
        execution.hits.extend(page.hits);
        execution.incomplete_ranges += page.incomplete_ranges;
        execution.index_progress.extend(page.index_progress);
        execution.partial_ranges.extend(page.partial_ranges);
        execution.complete &= page.complete;
    }
    execution.incomplete_ranges += failed;
    execution.complete &= failed == 0;
    // Strict mode answers complete pages only; work left in the cursor is not missing.
    if plan.mode == ObjectQueryMode::DistributedStrict && execution.incomplete_ranges > 0 {
        return Err(MetadataApiError::SearchIncomplete(incomplete_reason(
            execution.incomplete_ranges,
            execution.partial_ranges.len(),
            &execution.index_progress,
        )));
    }
    Ok(execution)
}

/// The distributed buckets a realm-wide page skipped, once each. They no longer count as
/// unsearched: [`search_skipped`] searches them through their range indexes.
fn take_skipped(parts: &mut [(NodeId, SearchNodePage)]) -> Vec<String> {
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

/// One page of a skipped distributed bucket, searched through its range indexes from where
/// the realm-wide cursor left it.
async fn search_skipped(
    context: &DriverContext,
    (realm_id, local_node_id): (RealmId, NodeId),
    plan: &ObjectSearchPlan,
    pending: &PendingBucket,
    deadline: tokio::time::Instant,
) -> Result<ObjectExecution, MetadataApiError> {
    let request = SearchQueryRequest {
        auth: plan.auth.clone(),
        bearer_token: plan.bearer_token.clone(),
        query: plan.query.clone(),
        key_match: plan.key_match,
        bucket: Some(pending.bucket.clone()),
        limit: plan.limit,
        cursor: pending.cursor.clone(),
        mode: plan.mode,
        target_nodes: plan.target_nodes.clone(),
    };
    search_until(context, (realm_id, local_node_id), request, deadline).await
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

pub(super) async fn resolve_object_partitions(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    plan: &ObjectSearchPlan,
    deadline: tokio::time::Instant,
) -> Result<ObjectSearchPartitions, MetadataApiError> {
    let (as_of, partitions, failed_partitions, discovery_failed, omitted_partitions, carried) =
        match plan.cursor.as_deref() {
            Some(raw) => {
                let authorized_signers = match plan.mode {
                    ObjectQueryMode::Local => vec![local_node_id],
                    ObjectQueryMode::DistributedBestEffort | ObjectQueryMode::DistributedStrict => {
                        load_realm_config(context, realm_id)
                            .await
                            .ok_or(MetadataApiError::ServiceUnavailable)?
                            .node_ids()
                            .map_err(|_| MetadataApiError::ServiceUnavailable)?
                    }
                };
                let cursor = ObjectCursor::decode(raw, plan.fingerprint, &authorized_signers)?;
                let partitions = cursor.partition_states()?;
                if plan.mode == ObjectQueryMode::Local
                    && (partitions.len() != 1 || partitions[0].node_id != local_node_id)
                {
                    return Err(MetadataApiError::InvalidCursor(
                        "invalid local object search cursor".to_string(),
                    ));
                }
                (
                    cursor.payload.as_of,
                    partitions,
                    cursor.failed_nodes()?,
                    cursor.payload.discovery_failed,
                    cursor.payload.omitted_partitions,
                    (
                        cursor.payload.pending.clone(),
                        cursor.payload.changed_keys,
                        cursor.payload.layout,
                    ),
                )
            }
            None => {
                let as_of = SystemTime::now();
                let (mut nodes, discovery_failed) = match plan.mode {
                    ObjectQueryMode::Local => (vec![local_node_id], false),
                    ObjectQueryMode::DistributedBestEffort | ObjectQueryMode::DistributedStrict => {
                        match plan.target_nodes.clone() {
                            Some(nodes) => (deduplicate_fanout_nodes(nodes), false),
                            None => {
                                let discovery = tokio::time::timeout_at(
                                    deadline,
                                    discover_realm_nodes(context, realm_id, local_node_id),
                                )
                                .await
                                .unwrap_or(RealmNodeDiscovery {
                                    nodes: vec![local_node_id],
                                    failed: true,
                                });
                                (discovery.nodes, discovery.failed)
                            }
                        }
                    }
                };
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
                if plan.mode == ObjectQueryMode::DistributedStrict
                    && (discovery_failed || omitted_partitions > 0)
                {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                nodes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                (
                    as_of,
                    nodes
                        .into_iter()
                        .map(|node_id| ObjectPartitionState {
                            node_id,
                            start_after: None,
                            exhausted: false,
                            observed_at: None,
                        })
                        .collect(),
                    Vec::new(),
                    discovery_failed,
                    omitted_partitions,
                    (Vec::new(), Some(0), RangeLayout::Unknown),
                )
            }
        };
    // The pending buckets, the changed keys and the range layout of the earlier pages.
    let (pending, changed_keys, layout) = carried;

    if plan.mode == ObjectQueryMode::DistributedStrict
        && (discovery_failed || omitted_partitions > 0 || !failed_partitions.is_empty())
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    if pending.is_empty() && !partitions.iter().any(|partition| !partition.exhausted) {
        return Err(MetadataApiError::InvalidCursor(
            "exhausted object search cursor".to_string(),
        ));
    }
    Ok(ObjectSearchPartitions {
        as_of,
        partitions,
        failed_partitions,
        discovery_failed,
        omitted_partitions,
        pending,
        changed_keys,
        layout,
    })
}

pub(super) async fn run_object_fanout(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    plan: &ObjectSearchPlan,
    partitions: &ObjectSearchPartitions,
    deadline: tokio::time::Instant,
) -> Result<(Vec<(NodeId, SearchNodePage)>, MetadataFanoutStats), MetadataApiError> {
    let active_nodes = partitions
        .partitions
        .iter()
        .filter(|partition| !partition.exhausted)
        .map(|partition| partition.node_id)
        .collect::<Vec<_>>();
    let start_positions = partitions
        .partitions
        .iter()
        .map(|partition| (partition.node_id, partition.start_after.clone()))
        .collect::<HashMap<_, _>>();
    let remote_auth_token = fanout_bearer(plan.bearer_token.as_deref());
    let handle = context.metadata_handle.clone();

    let local_call: MetadataNodeCall<SearchNodePage> = metadata_node_call(
        (
            context.clone(),
            plan.auth.clone(),
            realm_id,
            plan.query.clone(),
            plan.key_match,
            plan.bucket.clone(),
            plan.limit,
            partitions.as_of,
            start_positions.clone(),
        ),
        |(context, auth, realm_id, query, key_match, bucket, limit, as_of, starts), node_id| async move {
            search_local_objects(
                &context,
                SearchObjectsInput {
                    auth,
                    realm_id,
                    node_id,
                    query,
                    key_match,
                    bucket,
                    limit,
                    start_after: starts.get(&node_id).cloned().flatten(),
                    as_of,
                },
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)
        },
    );
    let remote_call: MetadataNodeCall<SearchNodePage> = metadata_node_call(
        (
            handle,
            remote_auth_token,
            plan.query.clone(),
            plan.key_match,
            plan.bucket.clone(),
            plan.limit,
            partitions.as_of,
            start_positions,
        ),
        |(handle, auth_token, query, key_match, bucket, limit, as_of, starts), node_id| async move {
            let Some(handle) = handle else {
                return Err(MetadataReadError::Unavailable);
            };
            handle
                .request_object_search(
                    node_id,
                    auth_token,
                    query,
                    key_match,
                    bucket,
                    limit,
                    starts.get(&node_id).cloned().flatten(),
                    as_of,
                )
                .await
        },
    );
    run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(
            Some(plan.mode.fanout_mode()),
            Some(active_nodes),
            plan.mode.allow_partial(),
        )
        .with_subject(plan.fingerprint)
        .with_discovery_failed(partitions.discovery_failed)
        .with_deadline(deadline),
        MetadataFanoutOperation::ObjectSearch,
        local_call,
        remote_call,
        record_object_result,
        map_read_error,
    )
    .await
}

pub(super) fn assemble_object_execution(
    context: &DriverContext,
    plan: &ObjectSearchPlan,
    partitions: ObjectSearchPartitions,
    parts: Vec<(NodeId, SearchNodePage)>,
    mut fanout_stats: MetadataFanoutStats,
) -> Result<ObjectExecution, MetadataApiError> {
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
    let moved = layout == RangeLayout::Changed;
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
        let net = context.net_handle.as_ref().ok_or_else(|| {
            MetadataApiError::Internal(
                "net handle unavailable for object search cursor signing".to_string(),
            )
        })?;
        Some(
            ObjectCursor::new_signed(
                plan.fingerprint,
                as_of,
                &partitions,
                &failed_partitions,
                discovery_failed,
                omitted_partitions,
                &pending,
                (changed_keys, layout),
                net.node_id(),
                |bytes| net.sign(bytes),
            )
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?
            .encode()
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?,
        )
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
    Ok(ObjectExecution {
        hits,
        next_cursor,
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
    })
}

/// Why a strict search refused: how many ranges were not searched or only in part, and how
/// many keys their indexes still miss.
fn incomplete_reason(
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

pub async fn search_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    mut request: MetadataSearchRequest,
) -> Result<MetadataSearchExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE;
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
    if let Some(iri) = request.conforms_to.take() {
        request.conforms_to = crate::metadata::profile::validation::equivalent_profile_iris(&iri)
            .into_iter()
            .next();
    }
    let page_size = request
        .limit
        .unwrap_or(SEARCH_PAGE_SIZE)
        .clamp(1, SEARCH_MAX_PAGE);

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
            // Check the full realm because capped fan-out may omit the cursor's signer.
            let signer_nodes = match request.mode.unwrap_or(ApiQueryMode::Distributed) {
                ApiQueryMode::Local => vec![local_node_id],
                ApiQueryMode::Distributed => match request.target_nodes.as_ref() {
                    Some(nodes) => {
                        let mut signers = nodes.clone();
                        signers.push(local_node_id);
                        signers
                    }
                    None => {
                        let discovery = tokio::time::timeout_at(
                            deadline,
                            discover_realm_nodes(context, realm_id, local_node_id),
                        )
                        .await
                        .unwrap_or(RealmNodeDiscovery {
                            nodes: vec![local_node_id],
                            failed: true,
                        });
                        let mut signers = discovery.nodes.clone();
                        signers.push(local_node_id);
                        let nodes =
                            select_fanout_nodes(&discovery.nodes, local_node_id, &fingerprint);
                        let mut discovery = discovery;
                        discovery.nodes = nodes;
                        cursor_discovery = Some(discovery);
                        signers
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
            (
                Some(cursor.payload.watermark.clone()),
                cursor.resume_positions(),
            )
        }
        None => (None, HashMap::new()),
    };

    // Keep resumed nodes in the bounded selection so remaining hits are not
    // silently discarded when discovery changes.
    let (target_nodes, discovery_failed) = if request.cursor.is_some() {
        let mut nodes = match request.target_nodes.as_ref() {
            Some(nodes) => select_fanout_nodes(nodes, local_node_id, &fingerprint),
            None => match request.mode.unwrap_or(ApiQueryMode::Distributed) {
                ApiQueryMode::Local => vec![local_node_id],
                ApiQueryMode::Distributed => cursor_discovery
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
        (request.target_nodes.take(), false)
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
            .with_subject(fingerprint)
            .with_discovery_failed(discovery_failed)
            .with_deadline(deadline),
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
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?
                .encode()
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?,
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
