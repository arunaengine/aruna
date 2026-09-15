use super::{
    ApiQueryMode, AuthContext, BlobHeadKey, BucketSearchHit, CursorEnvelopeError, Deserialize,
    DriverContext, GroupId, HashMap, HashSet, METADATA_DISTRIBUTED_QUERY_DEADLINE,
    METADATA_DISTRIBUTED_QUERY_MAX_NODES, MetadataApiError, MetadataFanoutOperation,
    MetadataFanoutScope, MetadataFanoutStats, MetadataNodeCall, MetadataReadError,
    MetadataSearchHit, NodeId, ObjectInventoryHit, ObjectKeyMatch, RealmId, RealmNodeDiscovery,
    SearchCursor, SearchCursorError, SearchNodePage, SearchObjectsInput, Serialize, SignedCursor,
    SystemTime, deduplicate_fanout_nodes, discover_realm_nodes, fanout_bearer, load_realm_config,
    map_read_error, metadata_node_call, object_search_fingerprint, query_fingerprint,
    record_object_result, run_metadata_fanout, search_local_objects, select_fanout_nodes,
};

use super::distributed::run_search_distributed;
use crate::metadata::search_cursor::{
    METADATA_SEARCH_DEFAULT_PAGE_SIZE, METADATA_SEARCH_MAX_PAGE_SIZE,
};

const OBJECT_SEARCH_CURSOR_VERSION: u8 = 1;

const OBJECT_SEARCH_CURSOR_SIGNATURE_CONTEXT: &[u8] = b"aruna.object.search.cursor.v1";

const OBJECT_SEARCH_CURSOR_MAX_BYTES: usize = 64 * 1024;

const OBJECT_SEARCH_CURSOR_MAX_KEY_BYTES: usize = 2 * 1024;

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
    pub query: String,
    pub limit: usize,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct BucketSearchExecution {
    pub hits: Vec<BucketSearchHit>,
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
    pub complete: bool,
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
}

pub(super) type ObjectCursor = SignedCursor<ObjectCursorPayload>;

impl SignedCursor<ObjectCursorPayload> {
    pub(super) fn decode(
        raw: &str,
        fingerprint: [u8; 32],
        authorized_signers: &[NodeId],
    ) -> Result<Self, MetadataApiError> {
        if raw.len() > OBJECT_SEARCH_CURSOR_MAX_BYTES {
            return Err(MetadataApiError::InvalidCursor(
                "invalid object search cursor".to_string(),
            ));
        }
        let cursor = Self::decode_verified(
            raw,
            OBJECT_SEARCH_CURSOR_SIGNATURE_CONTEXT,
            authorized_signers,
            |cursor| {
                if cursor.version != OBJECT_SEARCH_CURSOR_VERSION
                    || cursor.fingerprint != fingerprint
                {
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
        if cursor.payload.partitions.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES
            || cursor.payload.failed_partitions.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES
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
                    key.len() > OBJECT_SEARCH_CURSOR_MAX_KEY_BYTES
                        || BlobHeadKey::from_bytes(key).is_err()
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
        if !cursor
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
            OBJECT_SEARCH_CURSOR_VERSION,
            OBJECT_SEARCH_CURSOR_SIGNATURE_CONTEXT,
            fingerprint,
            ObjectCursorPayload {
                as_of,
                partitions,
                failed_partitions,
                discovery_failed,
                omitted_partitions,
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
}

pub async fn search_objects(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: SearchQueryRequest,
) -> Result<ObjectExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let plan = plan_object_search(realm_id, request)?;
    let partitions =
        resolve_object_partitions(context, realm_id, local_node_id, &plan, deadline).await?;
    let (parts, fanout_stats) = run_object_fanout(
        context,
        realm_id,
        local_node_id,
        &plan,
        &partitions,
        deadline,
    )
    .await?;
    assemble_object_execution(context, &plan, partitions, parts, fanout_stats)
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
        .clamp(1, crate::s3::search_objects::OBJECT_SEARCH_MAX_LIMIT);
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
    let (as_of, partitions, failed_partitions, discovery_failed, omitted_partitions) =
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
                let omitted_partitions = if nodes.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES {
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
                )
            }
        };

    if plan.mode == ObjectQueryMode::DistributedStrict
        && (discovery_failed || omitted_partitions > 0 || !failed_partitions.is_empty())
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    if !partitions.iter().any(|partition| !partition.exhausted) {
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
    } = partitions;
    let newly_failed = fanout_stats.failed_partitions.clone();
    failed_partitions.extend(newly_failed.iter().copied());
    failed_partitions.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    failed_partitions.dedup();
    partitions.retain(|partition| !newly_failed.contains(&partition.node_id));
    partitions
        .sort_unstable_by(|left, right| left.node_id.as_bytes().cmp(right.node_id.as_bytes()));

    let mut pages = parts.into_iter().collect::<HashMap<_, _>>();
    let mut hits = Vec::with_capacity(plan.limit);
    let mut remaining = plan.limit;
    for partition in &mut partitions {
        if partition.exhausted {
            continue;
        }
        let Some(page) = pages.remove(&partition.node_id) else {
            continue;
        };
        partition.observed_at = Some(page.observed_at);
        if remaining == 0 {
            if page.hits.is_empty() && page.next_start_after.is_none() {
                partition.exhausted = true;
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
        } else if consumed > 0 || page.hits.is_empty() {
            partition.start_after = page.next_start_after;
            partition.exhausted = partition.start_after.is_none();
        }
    }

    let next_cursor = if partitions.iter().any(|partition| !partition.exhausted) {
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
    let complete = fanout_stats.nodes_failed == 0;
    Ok(ObjectExecution {
        hits,
        next_cursor,
        as_of,
        partitions: coverage,
        fanout_stats,
        omitted_partitions,
        complete,
    })
}

pub async fn search_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    mut request: MetadataSearchRequest,
) -> Result<MetadataSearchExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
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
        request.conforms_to = crate::metadata::profile_validation::equivalent_profile_iris(&iri)
            .into_iter()
            .next();
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
