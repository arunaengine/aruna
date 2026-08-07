use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::structs::{
    AuthContext, BucketInfo, Permission, blob_bucket_permission_path, blob_object_permission_path,
};
use aruna_operations::blob_holders::{GetBlobHoldersError, GetBlobHoldersOperation};
use aruna_operations::driver::{drive, drive_until};
use aruna_operations::replication::location_summary::{
    LocationSummaryError, LocationSummaryOperation, QueuedReplicaNodesOperation, QueuedReplicas,
    RelationshipReplicaNodesOperation, RemoteLocationSummaryOperation,
};
use aruna_operations::replication::protocol::{
    LocationCopyStorage, LocationSummary, LocationSummaryRequest, ReplicationMode,
};
use aruna_operations::replication::queue::QueueBlobReplicationOperation;
use aruna_operations::replication::version_replication::{
    ReplicateScopeInput, ReplicateScopeTarget,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "blobs", description = "Blob management and replication")),
    paths(replicate_blob, blob_locations)
)]
pub struct BlobsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/blobs/replicate", post(replicate_blob))
        .route("/blobs/locations", get(blob_locations))
}

/// Replication targets are few and operator-controlled, so the fan-out stays
/// small; the deadline keeps an offline target from holding the answer.
const LOCATION_FANOUT_LIMIT: usize = 8;
const LOCATION_SUMMARY_TIMEOUT: Duration = Duration::from_secs(5);
/// Ceilings on the whole request. The queued scan alone can name far more
/// destinations than a caller will wait for, so the request bounds its own work
/// rather than trusting the candidate list to stay small.
const LOCATION_CANDIDATE_LIMIT: usize = 64;
const LOCATION_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// One place a copy can be: the node, and the bucket and key it stores it
/// under. A sync relationship maps the key, so one node can be several.
type Destination = (NodeId, String, String);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplicateBlobRequest {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplicateBlobResponse {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub target_node_id: String,
}

async fn load_bucket(state: &ServerState, bucket: &str) -> ServerResult<BucketInfo> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(bucket_info)) => Ok(bucket_info),
        Ok(None) | Err(GetBucketInfoError::NotFound) => Err(ServerError::NotFound),
        Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

#[utoipa::path(
    post,
    path = "/blobs/replicate",
    tag = "blobs",
    request_body = ReplicateBlobRequest,
    responses(
        (status = 202, description = "Replication accepted", body = ReplicateBlobResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replicate_blob(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ReplicateBlobRequest>,
) -> ServerResult<(StatusCode, Json<ReplicateBlobResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.realm_id != state.get_realm_id() {
        return Err(ServerError::Forbidden);
    }

    let bucket_info = load_bucket(&state, &request.bucket).await?;

    let permission_path = match request.path.as_deref() {
        Some(path) => blob_object_permission_path(
            state.get_realm_id(),
            bucket_info.group_id,
            state.get_node_id(),
            &request.bucket,
            path,
        ),
        None => blob_bucket_permission_path(
            state.get_realm_id(),
            bucket_info.group_id,
            state.get_node_id(),
            &request.bucket,
        ),
    };

    crate::auth::ensure_permission(&state, &auth, permission_path, Permission::WRITE).await?;

    let node_id = NodeId::from_str(&request.node_id).map_err(|_| ServerError::BadRequest)?;
    let target = match (request.path.as_deref(), request.version_id.as_deref()) {
        (None, None) => ReplicateScopeTarget::Bucket,
        (None, Some(_)) => return Err(ServerError::BadRequest),
        (Some(path), None) => ReplicateScopeTarget::Object {
            key: path.to_string(),
        },
        (Some(path), Some(version_id)) => ReplicateScopeTarget::Version {
            key: path.to_string(),
            version_id: ulid::Ulid::from_string(version_id).map_err(|_| ServerError::BadRequest)?,
        },
    };

    let path = match &target {
        ReplicateScopeTarget::Bucket => None,
        ReplicateScopeTarget::Prefix(prefix) => Some(prefix.clone()),
        ReplicateScopeTarget::Object { key } | ReplicateScopeTarget::Version { key, .. } => {
            Some(key.clone())
        }
    };
    let version_id = match &target {
        ReplicateScopeTarget::Version { version_id, .. } => Some(version_id.to_string()),
        _ => None,
    };
    let input = ReplicateScopeInput {
        bucket: request.bucket,
        target,
        target_node_id: node_id,
        auth_context: auth,
        replicate_delete_markers: true,
        mode: ReplicationMode::OnDemand,
    };
    let response = ReplicateBlobResponse {
        bucket: input.bucket.clone(),
        path: path.clone(),
        version_id: version_id.clone(),
        target_node_id: input.target_node_id.to_string(),
    };
    let queue_result = drive(
        QueueBlobReplicationOperation::new(input, None),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    if !queue_result.scheduled {
        warn!(
            bucket = %response.bucket,
            path = ?response.path,
            version_id = ?response.version_id,
            target_node = %response.target_node_id,
            "On-demand replication job persisted but drain scheduling was not acknowledged"
        );
    }

    Ok((StatusCode::ACCEPTED, Json(response)))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlobLocationsQuery {
    pub bucket: String,
    pub path: String,
    #[serde(default)]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum BlobCopyState {
    Present,
    Pending,
    Unreachable,
    /// The node holds this bucket under access rules the caller does not pass,
    /// so it refused to say whether a copy is there.
    Denied,
    /// The version exists but carries no bytes anywhere: a delete marker, or a
    /// version that only references content held elsewhere.
    NotStored,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum BlobCopyStorage {
    NodeManaged,
    GroupBackend,
}

/// One copy of a version at one destination. A node reached under several
/// destination paths has one entry per path, so `node_id` may repeat and only
/// the whole `(node_id, bucket, key)` triple identifies an entry.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlobCopyResponse {
    pub node_id: String,
    pub local: bool,
    /// Bucket this copy is stored under on that node, which a sync relationship
    /// can map away from the requested one.
    pub bucket: String,
    /// Key this copy is stored under on that node.
    pub key: String,
    pub state: BlobCopyState,
    pub storage: Option<BlobCopyStorage>,
    pub storage_class: Option<String>,
    pub group_backend_id: Option<String>,
    pub group_backend_name: Option<String>,
}

/// Why an answer could not cover every node that might hold a copy.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum LocationScanLimit {
    /// The queued-replication scan hit its page cap before the keyspace ended.
    QueuedScanTruncated,
    /// The queued-replication scan itself failed, so no queued copy is known.
    QueuedScanFailed,
    /// The sync-relationship scan failed, so the destinations a relationship
    /// will place a copy on are unknown.
    RelationshipScanFailed,
    /// Queued job records could not be decoded and were skipped.
    QueuedRecordUnreadable,
    /// More candidate nodes than one request asks; the rest were not asked.
    CandidateCapReached,
    /// The holder index could not be queried, so copies outside the current
    /// configuration and queue are unknown.
    HolderLookupFailed,
    /// A node the holder index names holds the bytes but knows no copy under
    /// the bucket and key it was asked about, so its copy list may be short.
    HolderPathUnknown,
    /// A node gave no answer, so whether it holds a copy stays unknown.
    HolderUnreachable,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlobLocationsResponse {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub copies: Vec<BlobCopyResponse>,
    /// Every node that might hold a copy was enumerated and asked. When false,
    /// a copy may be missing from `copies` rather than genuinely absent.
    pub complete: bool,
    /// What stopped the search from covering everything; empty when `complete`.
    pub limits: Vec<LocationScanLimit>,
}

fn pending_copy(destination: &Destination, state: BlobCopyState) -> BlobCopyResponse {
    let (node_id, bucket, key) = destination;
    BlobCopyResponse {
        node_id: node_id.to_string(),
        local: false,
        bucket: bucket.clone(),
        key: key.clone(),
        state,
        storage: None,
        storage_class: None,
        group_backend_id: None,
        group_backend_name: None,
    }
}

fn copy_response(
    destination: &Destination,
    local: bool,
    summary: LocationSummary,
) -> BlobCopyResponse {
    let base = BlobCopyResponse {
        local,
        ..pending_copy(destination, BlobCopyState::Pending)
    };
    let unstored = summary.version_id.is_some() && !summary.materialized;
    match summary.storage.filter(|_| summary.held) {
        Some(LocationCopyStorage::NodeManaged { storage_class }) => BlobCopyResponse {
            state: BlobCopyState::Present,
            storage: Some(BlobCopyStorage::NodeManaged),
            storage_class,
            ..base
        },
        Some(LocationCopyStorage::GroupBackend { backend_id, name }) => BlobCopyResponse {
            state: BlobCopyState::Present,
            storage: Some(BlobCopyStorage::GroupBackend),
            group_backend_id: Some(backend_id.to_string()),
            group_backend_name: name,
            ..base
        },
        None if unstored => BlobCopyResponse {
            state: BlobCopyState::NotStored,
            ..base
        },
        None => base,
    }
}

#[utoipa::path(
    get,
    path = "/blobs/locations",
    tag = "blobs",
    params(
        ("bucket" = String, Query, description = "Bucket holding the object"),
        ("path" = String, Query, description = "Object key"),
        ("version_id" = Option<String>, Query, description = "Version to inspect, defaulting to the current one")
    ),
    responses(
        (status = 200, description = "Copies of one version", body = BlobLocationsResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Object or version not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn blob_locations(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<BlobLocationsQuery>,
) -> ServerResult<Json<BlobLocationsResponse>> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.realm_id != state.get_realm_id() {
        return Err(ServerError::Forbidden);
    }
    let ctx = state.get_ctx();
    let local_node = state.get_node_id();
    let version_id = query
        .version_id
        .as_deref()
        .map(ulid::Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;
    crate::auth::ensure_permission(
        &state,
        &auth,
        blob_object_permission_path(
            state.get_realm_id(),
            load_bucket(&state, &query.bucket).await?.group_id,
            local_node,
            &query.bucket,
            &query.path,
        ),
        Permission::READ,
    )
    .await?;
    let request = LocationSummaryRequest {
        realm_id: state.get_realm_id(),
        bucket: query.bucket.clone(),
        key: query.path.clone(),
        version_id,
        auth_context: auth,
    };
    let local = drive(
        LocationSummaryOperation::new_local(local_node, request.clone()).with_policy(true),
        ctx.as_ref(),
    )
    .await
    .map_err(|error| match error {
        LocationSummaryError::Denied => ServerError::Forbidden,
        LocationSummaryError::BucketNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    let Some(resolved) = local.summary.version_id else {
        return Err(ServerError::NotFound);
    };
    let blake3 = local.blake3;
    let bucket_info = local.bucket;
    let delete_marker = local.delete_marker;

    let here = (local_node, query.bucket.clone(), query.path.clone());
    let mut copies = vec![copy_response(&here, true, local.summary)];
    let mut limits = Vec::new();
    let mut candidates: BTreeSet<Destination> = BTreeSet::new();
    let mut expected: BTreeSet<Destination> = BTreeSet::new();
    let mut capped = false;
    // First, because these are the only candidates that carry the path the copy
    // is actually stored under; the source path every other source has is a
    // guess whenever a relationship rewrites the key.
    match drive(
        RelationshipReplicaNodesOperation::new(
            local_node,
            query.bucket.clone(),
            query.path.clone(),
            delete_marker,
        ),
        ctx.as_ref(),
    )
    .await
    {
        Ok(targets) => {
            for target in targets {
                let destination = (target.node_id, target.bucket, target.key);
                expected.insert(destination.clone());
                capped |= !add_candidate(&mut candidates, destination);
            }
        }
        Err(error) => {
            warn!(
                bucket = %query.bucket,
                key = %query.path,
                error = %error,
                "Sync relationship scan failed; relationship copies are unknown"
            );
            limits.push(LocationScanLimit::RelationshipScanFailed);
        }
    }
    for (node_id, bucket) in configured_targets(bucket_info.as_ref(), local_node, delete_marker) {
        let destination = (node_id, bucket, query.path.clone());
        expected.insert(destination.clone());
        capped |= !add_candidate(&mut candidates, destination);
    }
    let queued = match drive(
        QueuedReplicaNodesOperation::new(
            query.bucket.clone(),
            query.path.clone(),
            resolved,
            delete_marker,
        ),
        ctx.as_ref(),
    )
    .await
    {
        Ok(queued) => queued,
        Err(error) => {
            warn!(
                bucket = %query.bucket,
                key = %query.path,
                error = %error,
                "Queued replication scan failed; queued copies are unknown"
            );
            limits.push(LocationScanLimit::QueuedScanFailed);
            QueuedReplicas::default()
        }
    };
    // Queue records and holder entries carry the source path, so they are asked
    // about it. A relationship that rewrites the key already contributed the
    // stored path above, and both destinations get their own entry.
    for node_id in queued.nodes.iter().filter(|node| **node != local_node) {
        let destination = (*node_id, query.bucket.clone(), query.path.clone());
        expected.insert(destination.clone());
        capped |= !add_candidate(&mut candidates, destination);
    }
    // Config and queue only name copies that are planned. A destination dropped
    // from the config, or one whose queue record is already consumed, still
    // stores the bytes and is only found through the holder index.
    match holder_nodes(&ctx, blake3, state.get_realm_id(), local_node).await {
        Ok(holders) => {
            for node_id in holders {
                let destination = (node_id, query.bucket.clone(), query.path.clone());
                capped |= !add_candidate(&mut candidates, destination);
            }
        }
        Err(error) => {
            warn!(
                bucket = %query.bucket,
                key = %query.path,
                error = %error,
                "Blob holder lookup failed; copies outside the configuration are unknown"
            );
            limits.push(LocationScanLimit::HolderLookupFailed);
        }
    }
    if queued.truncated {
        warn!(
            bucket = %query.bucket,
            key = %query.path,
            "Queued replication scan hit its page cap; queued copies may be missing"
        );
        limits.push(LocationScanLimit::QueuedScanTruncated);
    }
    if queued.skipped > 0 {
        warn!(
            bucket = %query.bucket,
            key = %query.path,
            skipped = queued.skipped,
            "Queued replication records could not be decoded"
        );
        limits.push(LocationScanLimit::QueuedRecordUnreadable);
    }
    if capped {
        warn!(
            bucket = %query.bucket,
            key = %query.path,
            "More candidate nodes than the per-request cap; some were not asked"
        );
        limits.push(LocationScanLimit::CandidateCapReached);
    }

    // One deadline for the whole fan-out, so a wall of stalled peers costs the
    // caller the deadline rather than the deadline times the candidate count.
    let deadline = Instant::now() + LOCATION_REQUEST_DEADLINE;
    let answers = stream::iter(candidates.into_iter().map(|destination| {
        let (node_id, bucket, key) = destination.clone();
        let request = LocationSummaryRequest {
            bucket,
            key,
            version_id: Some(resolved),
            ..request.clone()
        };
        let ctx = ctx.clone();
        async move {
            let answer = drive_until(
                RemoteLocationSummaryOperation::new(node_id, request),
                ctx.as_ref(),
                deadline.min(Instant::now() + LOCATION_SUMMARY_TIMEOUT),
            )
            .await;
            (destination, answer)
        }
    }))
    .buffer_unordered(LOCATION_FANOUT_LIMIT)
    .collect::<Vec<_>>()
    .await;

    // A node asked under several destination paths keeps one entry per path;
    // only a node no path answered for is short of a copy it may hold.
    let mut asked: BTreeSet<NodeId> = BTreeSet::new();
    let mut answered: BTreeSet<NodeId> = BTreeSet::new();
    let mut unreachable = false;
    for (destination, answer) in answers {
        asked.insert(destination.0);
        let Some(copy) = peer_copy(&destination, expected.contains(&destination), answer) else {
            continue;
        };
        answered.insert(destination.0);
        unreachable |= copy.state == BlobCopyState::Unreachable;
        copies.push(copy);
    }
    if unreachable {
        limits.push(LocationScanLimit::HolderUnreachable);
    }
    if asked.iter().any(|node| !answered.contains(node)) {
        warn!(
            bucket = %query.bucket,
            key = %query.path,
            "A holder answered under no path it knows; copies may be missing"
        );
        limits.push(LocationScanLimit::HolderPathUnknown);
    }
    copies.sort_by(|left, right| {
        (!left.local, &left.node_id, &left.bucket, &left.key).cmp(&(
            !right.local,
            &right.node_id,
            &right.bucket,
            &right.key,
        ))
    });

    Ok(Json(BlobLocationsResponse {
        bucket: query.bucket,
        key: query.path,
        version_id: resolved.to_string(),
        copies,
        complete: limits.is_empty(),
        limits,
    }))
}

/// The copy to report for one peer's answer. `None` drops a holder-index
/// candidate that does not hold this version under the path it was asked
/// about, which the answer admits as `HolderPathUnknown`.
fn peer_copy(
    destination: &Destination,
    expected: bool,
    answer: Result<LocationSummary, LocationSummaryError>,
) -> Option<BlobCopyResponse> {
    match answer {
        Ok(summary) if !summary.held && !expected => None,
        Ok(summary) => Some(copy_response(destination, false, summary)),
        Err(LocationSummaryError::Denied) => Some(pending_copy(destination, BlobCopyState::Denied)),
        Err(error) => {
            warn!(node = %destination.0, error = %error, "Location summary peer gave no answer");
            Some(pending_copy(destination, BlobCopyState::Unreachable))
        }
    }
}

/// Nodes the durable holder index says store these bytes. A version with no
/// materialized content has no hash and therefore no holders.
async fn holder_nodes(
    ctx: &Arc<aruna_operations::driver::DriverContext>,
    blake3: Option<[u8; 32]>,
    realm_id: aruna_core::structs::RealmId,
    local_node: NodeId,
) -> Result<Vec<NodeId>, GetBlobHoldersError> {
    let Some(blake3) = blake3 else {
        return Ok(Vec::new());
    };
    drive_until(
        GetBlobHoldersOperation::new(blake3, realm_id, local_node),
        ctx.as_ref(),
        Instant::now() + LOCATION_SUMMARY_TIMEOUT,
    )
    .await
}

/// Legacy bucket replication targets that will receive this version. A target
/// declining delete markers never gets a job for one, so reporting it would
/// promise a copy that is not coming.
fn configured_targets(
    bucket: Option<&BucketInfo>,
    local_node: NodeId,
    delete_marker: bool,
) -> Vec<(NodeId, String)> {
    bucket
        .iter()
        .flat_map(|bucket| bucket.replication.iter())
        .flat_map(|config| config.targets.iter())
        .filter(|target| target.node_id != local_node)
        .filter(|target| !delete_marker || target.replicate_delete_markers)
        .map(|target| (target.node_id, target.bucket.clone()))
        .collect()
}

/// Adds one destination unless the request is already at its cap, which counts
/// destinations rather than nodes because each one is a separate question.
/// `false` means the destination was dropped, which the answer has to admit.
fn add_candidate(candidates: &mut BTreeSet<Destination>, destination: Destination) -> bool {
    if candidates.contains(&destination) {
        return true;
    }
    if candidates.len() >= LOCATION_CANDIDATE_LIMIT {
        return false;
    }
    candidates.insert(destination);
    true
}

#[cfg(test)]
mod tests {
    use super::{
        BlobCopyState, BlobCopyStorage, BlobLocationsQuery, blob_locations, configured_targets,
        copy_response, pending_copy,
    };
    use crate::error::ServerError;
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::{
        Actor, AuthContext, BucketInfo, BucketReplicationConfig, BucketReplicationTarget,
        GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument,
        RealmConfigDocument, RealmId,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::replication::location_summary::LocationSummaryError;
    use aruna_operations::replication::protocol::{LocationCopyStorage, LocationSummary};
    use aruna_storage::FjallStorage;
    use axum::Extension;
    use axum::extract::{Query, State};
    use byteview::ByteView;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

    fn node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn peer_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[4u8; 32]).public()
    }

    fn configured(markers: bool) -> BucketInfo {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::types::UserId::nil(realm_id),
            cors_configuration: None,
            replication: Some(BucketReplicationConfig {
                targets: vec![BucketReplicationTarget {
                    node_id: peer_id(),
                    realm_id,
                    bucket: "mirror".to_string(),
                    arn: String::new(),
                    replicate_delete_markers: markers,
                }],
            }),
            storage_routing: Vec::new(),
        }
    }

    #[test]
    fn skips_declined_markers() {
        // A legacy target that declines delete markers gets no job for one, so
        // reporting it as pending would promise a copy that never arrives.
        assert!(configured_targets(Some(&configured(false)), node_id(), true).is_empty());
        assert_eq!(
            configured_targets(Some(&configured(true)), node_id(), true),
            vec![(peer_id(), "mirror".to_string())]
        );
        assert_eq!(
            configured_targets(Some(&configured(false)), node_id(), false),
            vec![(peer_id(), "mirror".to_string())]
        );
    }

    #[test]
    fn hides_backend_name() {
        // Node-managed copies expose the class only; backend names stay operator-side.
        let copy = copy_response(
            &(node_id(), "raw".to_string(), "a.tar".to_string()),
            true,
            LocationSummary {
                version_id: Some(Ulid::from_bytes([1u8; 16])),
                held: true,
                storage: Some(LocationCopyStorage::NodeManaged {
                    storage_class: Some("cold".to_string()),
                }),
                materialized: true,
            },
        );

        assert_eq!(copy.state, BlobCopyState::Present);
        assert_eq!(copy.storage, Some(BlobCopyStorage::NodeManaged));
        assert_eq!(copy.storage_class.as_deref(), Some("cold"));
        assert!(copy.group_backend_id.is_none());
    }

    #[test]
    fn names_group_backend() {
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let copy = copy_response(
            &(node_id(), "raw".to_string(), "a.tar".to_string()),
            false,
            LocationSummary {
                version_id: Some(Ulid::from_bytes([1u8; 16])),
                held: true,
                storage: Some(LocationCopyStorage::GroupBackend {
                    backend_id,
                    name: Some("lab-minio".to_string()),
                }),
                materialized: true,
            },
        );

        assert_eq!(copy.storage, Some(BlobCopyStorage::GroupBackend));
        assert_eq!(
            copy.group_backend_id.as_deref(),
            Some(&*backend_id.to_string())
        );
        assert_eq!(copy.group_backend_name.as_deref(), Some("lab-minio"));
        assert!(copy.storage_class.is_none());
    }

    #[test]
    fn reports_unstored_version() {
        // A delete marker resolves a version that holds bytes nowhere.
        let copy = copy_response(
            &(node_id(), "raw".to_string(), "a.tar".to_string()),
            false,
            LocationSummary {
                version_id: Some(Ulid::from_bytes([1u8; 16])),
                held: false,
                storage: None,
                materialized: false,
            },
        );

        assert_eq!(copy.state, BlobCopyState::NotStored);
        assert!(copy.storage.is_none());
        assert_eq!(
            serde_json::to_value(BlobCopyState::NotStored).unwrap(),
            serde_json::json!("not-stored")
        );
    }

    #[test]
    fn absent_copy_pends() {
        // No version at the peer is a copy still to come, not a missing one.
        let destination = (node_id(), "archive".to_string(), "images/a.jpg".to_string());
        let copy = copy_response(&destination, false, LocationSummary::absent());

        assert_eq!(copy.state, BlobCopyState::Pending);
        assert!(copy.storage.is_none());
        // The mapped destination travels with the answer, so two copies on one
        // node stay apart.
        assert_eq!(copy.bucket, "archive");
        assert_eq!(copy.key, "images/a.jpg");
        assert_eq!(
            pending_copy(&destination, BlobCopyState::Unreachable).state,
            BlobCopyState::Unreachable
        );
    }

    #[test]
    fn caps_candidates() {
        // Past the cap a destination is dropped, and the caller has to be told.
        let mut candidates = std::collections::BTreeSet::new();
        for seed in 0..super::LOCATION_CANDIDATE_LIMIT {
            let node = iroh::SecretKey::from_bytes(&[seed as u8 + 1; 32]).public();
            assert!(super::add_candidate(
                &mut candidates,
                (node, "raw".to_string(), "a.tar".to_string())
            ));
        }

        let extra = iroh::SecretKey::from_bytes(&[0u8; 32]).public();
        assert!(!super::add_candidate(
            &mut candidates,
            (extra, "raw".to_string(), "a.tar".to_string())
        ));
        assert_eq!(candidates.len(), super::LOCATION_CANDIDATE_LIMIT);

        let known = candidates.iter().next().unwrap().0;
        assert!(super::add_candidate(
            &mut candidates,
            (known, "raw".to_string(), "a.tar".to_string())
        ));
    }

    #[test]
    fn keeps_mapped_path() {
        // The same node under two destination paths is two questions: dropping
        // one would hide the copy stored under the other.
        let mut candidates = std::collections::BTreeSet::new();
        let node = node_id();
        assert!(super::add_candidate(
            &mut candidates,
            (node, "archive".to_string(), "images/a.jpg".to_string())
        ));
        assert!(super::add_candidate(
            &mut candidates,
            (node, "raw".to_string(), "photos/a.jpg".to_string())
        ));

        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn drops_unheld_holder() {
        // A node holding the same bytes under another object is not a copy of
        // this version, but a configured or queued target that has not received
        // it yet still is.
        let absent = LocationSummary::absent();
        let destination = (node_id(), "raw".to_string(), "a.tar".to_string());
        assert!(super::peer_copy(&destination, false, Ok(absent.clone())).is_none());
        assert_eq!(
            super::peer_copy(&destination, true, Ok(absent.clone())).map(|copy| copy.state),
            Some(BlobCopyState::Pending)
        );
        assert_eq!(
            super::peer_copy(&destination, false, Err(LocationSummaryError::Denied))
                .map(|copy| copy.state),
            Some(BlobCopyState::Denied)
        );
        assert_eq!(
            super::peer_copy(&destination, false, Err(LocationSummaryError::Aborted))
                .map(|copy| copy.state),
            Some(BlobCopyState::Unreachable)
        );
    }

    #[test]
    fn openapi_lists_locations() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

        assert!(openapi["paths"].get("/blobs/locations").is_some());
        assert!(
            openapi["components"]["schemas"]["LocationScanLimit"]["enum"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("holder-path-unknown"))
        );
        assert!(
            openapi["components"]["schemas"]["BlobCopyState"]["enum"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("not-stored"))
        );
        assert!(
            openapi["components"]["schemas"]["BlobCopyResponse"]["properties"]
                .get("key")
                .is_some()
        );
        assert!(
            openapi["components"]["schemas"]["BlobLocationsResponse"]["properties"]
                .get("complete")
                .is_some()
        );
    }

    #[test]
    fn openapi_includes_replicate_blob_response_schema() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

        assert!(openapi["paths"].get("/blobs/replicate").is_some());
        assert!(
            openapi["components"]["schemas"]["ReplicateBlobResponse"]["properties"]
                .get("bucket")
                .is_some()
        );
        assert!(
            openapi["components"]["schemas"]["ReplicateBlobResponse"]["properties"]
                .get("target_node_id")
                .is_some()
        );
    }

    const TEST_BUCKET: &str = "locations-bucket";

    fn realm_for(seed: u8) -> RealmId {
        RealmId::from_bytes(
            *ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .as_bytes(),
        )
    }

    async fn write_fixture(state: &ServerState, key_space: &str, key: ByteView, value: ByteView) {
        match state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected fixture write event: {other:?}"),
        }
    }

    /// A realm holding one group whose owner may read `TEST_BUCKET`.
    async fn setup_bucket(realm_id: RealmId, owner: UserId) -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let state = Arc::new(
            ServerState::new(
                Arc::new(DriverContext {
                    storage_handle: storage,
                    net_handle: None,
                    blob_handle: None,
                    metadata_handle: None,
                    task_handle: None,
                    compute_handle: None,
                }),
                realm_id,
                node_id(),
                NodeCapabilities::local_node(realm_id).expect("capabilities"),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );
        let group_id = Ulid::from_bytes([11u8; 16]);
        let actor = Actor {
            node_id: node_id(),
            user_id: owner,
            realm_id,
        };
        write_fixture(
            &state,
            AUTH_KEYSPACE,
            ByteView::from(realm_id.as_bytes().to_vec()),
            ByteView::from(
                RealmAuthorizationDocument::new_default_realm_doc(realm_id)
                    .to_bytes(&actor)
                    .expect("realm auth serializes"),
            ),
        )
        .await;
        write_fixture(
            &state,
            AUTH_KEYSPACE,
            ByteView::from(group_id.to_bytes().to_vec()),
            ByteView::from(
                GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id)
                    .to_bytes(&actor)
                    .expect("group auth serializes"),
            ),
        )
        .await;
        write_fixture(
            &state,
            S3_BUCKET_KEYSPACE,
            ByteView::from(TEST_BUCKET.as_bytes().to_vec()),
            ByteView::from(
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: owner,
                    cors_configuration: None,
                    replication: None,
                    storage_routing: Vec::new(),
                }
                .to_bytes()
                .expect("bucket serializes"),
            ),
        )
        .await;
        (dir, state)
    }

    async fn install_deny_policy(state: &ServerState, realm_id: RealmId, expression: &str) {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.request_policies.push(RequestPolicy {
            policy_id: Ulid::generate(),
            name: "deny".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        });
        let actor = Actor {
            node_id: node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        write_fixture(
            state,
            REALM_CONFIG_KEYSPACE,
            ByteView::from(realm_id.as_bytes().to_vec()),
            ByteView::from(config.to_bytes(&actor).expect("config serializes")),
        )
        .await;
    }

    fn locations_query() -> Query<BlobLocationsQuery> {
        Query(BlobLocationsQuery {
            bucket: TEST_BUCKET.to_string(),
            path: "reports/a.csv".to_string(),
            version_id: None,
        })
    }

    fn auth_for(user_id: UserId, realm_id: RealmId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        }
    }

    #[tokio::test]
    async fn locations_require_read() {
        // A member of no group must not learn where an object is held; the owner
        // passes authorization and only then reports the absent object.
        let realm_id = realm_for(31);
        let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
        let stranger = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
        let (_dir, state) = setup_bucket(realm_id, owner).await;

        let denied = blob_locations(
            State(state.clone()),
            Extension(Some(auth_for(stranger, realm_id))),
            locations_query(),
        )
        .await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));

        let allowed = blob_locations(
            State(state),
            Extension(Some(auth_for(owner, realm_id))),
            locations_query(),
        )
        .await;
        assert!(matches!(allowed, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn policy_denies_locations() {
        // A realm deny-reads policy must reach the location read, which only
        // ordinary RBAC used to gate.
        let realm_id = realm_for(32);
        let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
        let (_dir, state) = setup_bucket(realm_id, owner).await;
        install_deny_policy(&state, realm_id, "permission == 'read'").await;

        let denied = blob_locations(
            State(state),
            Extension(Some(auth_for(owner, realm_id))),
            locations_query(),
        )
        .await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));
    }
}
