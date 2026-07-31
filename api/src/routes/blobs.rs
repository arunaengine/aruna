use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::structs::{
    AuthContext, Permission, blob_bucket_permission_path, blob_object_permission_path,
};
use aruna_operations::blob_holders::{GetBlobHoldersError, GetBlobHoldersOperation};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::{drive, drive_until};
use aruna_operations::replication::location_summary::{
    LocationSummaryError, LocationSummaryOperation, QueuedReplicaNodesOperation, QueuedReplicas,
    RemoteLocationSummaryOperation,
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
use std::collections::{BTreeMap, BTreeSet};
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
/// nodes than a caller will wait for, so the request bounds its own work
/// rather than trusting the candidate list to stay small.
const LOCATION_CANDIDATE_LIMIT: usize = 64;
const LOCATION_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

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

    let bucket_info = match drive(
        GetBucketInfoOperation::new(request.bucket.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(bucket_info))) => bucket_info,
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Err(GetBucketInfoError::NotFound) => {
            return Err(ServerError::NotFound);
        }
        Ok(Some(Err(err))) | Err(err) => return Err(ServerError::InternalError(err.to_string())),
        Ok(None) => return Err(ServerError::NotFound),
    };

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

    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: permission_path,
            required_permission: Permission::WRITE,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    if !allowed {
        return Err(ServerError::Forbidden);
    }

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
#[serde(rename_all = "lowercase")]
pub enum BlobCopyState {
    Present,
    Pending,
    Unreachable,
    /// The node holds this bucket under access rules the caller does not pass,
    /// so it refused to say whether a copy is there.
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum BlobCopyStorage {
    NodeManaged,
    GroupBackend,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlobCopyResponse {
    pub node_id: String,
    pub local: bool,
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
    /// Queued job records could not be decoded and were skipped.
    QueuedRecordUnreadable,
    /// More candidate nodes than one request asks; the rest were not asked.
    CandidateCapReached,
    /// The holder index could not be queried, so copies outside the current
    /// configuration and queue are unknown.
    HolderLookupFailed,
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

fn pending_copy(node_id: NodeId, state: BlobCopyState) -> BlobCopyResponse {
    BlobCopyResponse {
        node_id: node_id.to_string(),
        local: false,
        state,
        storage: None,
        storage_class: None,
        group_backend_id: None,
        group_backend_name: None,
    }
}

fn copy_response(node_id: NodeId, local: bool, summary: LocationSummary) -> BlobCopyResponse {
    let base = BlobCopyResponse {
        local,
        ..pending_copy(node_id, BlobCopyState::Pending)
    };
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
    let bucket_info = match drive(
        GetBucketInfoOperation::new(query.bucket.clone()),
        ctx.as_ref(),
    )
    .await
    {
        Ok(Some(Ok(bucket_info))) => bucket_info,
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Err(GetBucketInfoError::NotFound) => {
            return Err(ServerError::NotFound);
        }
        Ok(Some(Err(err))) | Err(err) => return Err(ServerError::InternalError(err.to_string())),
        Ok(None) => return Err(ServerError::NotFound),
    };

    let local_node = state.get_node_id();
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: blob_object_permission_path(
                state.get_realm_id(),
                bucket_info.group_id,
                local_node,
                &query.bucket,
                &query.path,
            ),
            required_permission: Permission::READ,
        }),
        ctx.as_ref(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    if !allowed {
        return Err(ServerError::Forbidden);
    }

    let version_id = query
        .version_id
        .as_deref()
        .map(ulid::Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;
    let request = LocationSummaryRequest {
        realm_id: state.get_realm_id(),
        bucket: query.bucket.clone(),
        key: query.path.clone(),
        version_id,
        auth_context: auth,
    };
    let local = drive(
        LocationSummaryOperation::new_local(request.clone()),
        ctx.as_ref(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let Some(resolved) = local.summary.version_id else {
        return Err(ServerError::NotFound);
    };
    let blake3 = local.blake3;

    let mut copies = vec![copy_response(local_node, true, local.summary)];
    let mut limits = Vec::new();
    let mut candidates = BTreeMap::new();
    let mut expected: BTreeSet<NodeId> = BTreeSet::new();
    let mut capped = false;
    for target in bucket_info
        .replication
        .iter()
        .flat_map(|config| config.targets.iter())
        .filter(|target| target.node_id != local_node)
    {
        expected.insert(target.node_id);
        capped |= !add_candidate(&mut candidates, target.node_id, &target.bucket);
    }
    let queued = match drive(
        QueuedReplicaNodesOperation::new(query.bucket.clone(), query.path.clone(), resolved),
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
    for node_id in queued.nodes.iter().filter(|node| **node != local_node) {
        expected.insert(*node_id);
        capped |= !add_candidate(&mut candidates, *node_id, &query.bucket);
    }
    // Config and queue only name copies that are planned. A destination dropped
    // from the config, or one whose queue record is already consumed, still
    // stores the bytes and is only found through the holder index.
    match holder_nodes(&ctx, blake3, state.get_realm_id(), local_node).await {
        Ok(holders) => {
            for node_id in holders {
                capped |= !add_candidate(&mut candidates, node_id, &query.bucket);
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
    let answers = stream::iter(candidates.into_iter().map(|(node_id, bucket)| {
        let request = LocationSummaryRequest {
            bucket,
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
            (node_id, answer)
        }
    }))
    .buffer_unordered(LOCATION_FANOUT_LIMIT)
    .collect::<Vec<_>>()
    .await;

    for (node_id, answer) in answers {
        copies.extend(peer_copy(node_id, expected.contains(&node_id), answer));
    }
    copies.sort_by(|left, right| (!left.local, &left.node_id).cmp(&(!right.local, &right.node_id)));

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
/// candidate that does not hold this version: it merely stores the same bytes
/// under another object, and nothing expects a copy of this one there.
fn peer_copy(
    node_id: NodeId,
    expected: bool,
    answer: Result<LocationSummary, LocationSummaryError>,
) -> Option<BlobCopyResponse> {
    match answer {
        Ok(summary) if !summary.held && !expected => None,
        Ok(summary) => Some(copy_response(node_id, false, summary)),
        Err(LocationSummaryError::Denied) => Some(pending_copy(node_id, BlobCopyState::Denied)),
        Err(error) => {
            warn!(node = %node_id, error = %error, "Location summary peer gave no answer");
            Some(pending_copy(node_id, BlobCopyState::Unreachable))
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

/// Adds a candidate node unless the request is already at its cap. `false`
/// means the node was dropped, which the answer has to admit.
fn add_candidate(candidates: &mut BTreeMap<NodeId, String>, node: NodeId, bucket: &str) -> bool {
    if candidates.contains_key(&node) {
        return true;
    }
    if candidates.len() >= LOCATION_CANDIDATE_LIMIT {
        return false;
    }
    candidates.insert(node, bucket.to_string());
    true
}

#[cfg(test)]
mod tests {
    use super::{BlobCopyState, BlobCopyStorage, copy_response, pending_copy};
    use crate::openapi::ApiDoc;
    use aruna_operations::replication::location_summary::LocationSummaryError;
    use aruna_operations::replication::protocol::{LocationCopyStorage, LocationSummary};
    use ulid::Ulid;

    fn node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    #[test]
    fn hides_backend_name() {
        // Node-managed copies expose the class only; backend names stay operator-side.
        let copy = copy_response(
            node_id(),
            true,
            LocationSummary {
                version_id: Some(Ulid::from_bytes([1u8; 16])),
                held: true,
                storage: Some(LocationCopyStorage::NodeManaged {
                    storage_class: Some("cold".to_string()),
                }),
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
            node_id(),
            false,
            LocationSummary {
                version_id: Some(Ulid::from_bytes([1u8; 16])),
                held: true,
                storage: Some(LocationCopyStorage::GroupBackend {
                    backend_id,
                    name: Some("lab-minio".to_string()),
                }),
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
    fn absent_copy_pends() {
        let copy = copy_response(node_id(), false, LocationSummary::absent());

        assert_eq!(copy.state, BlobCopyState::Pending);
        assert!(copy.storage.is_none());
        assert_eq!(
            pending_copy(node_id(), BlobCopyState::Unreachable).state,
            BlobCopyState::Unreachable
        );
    }

    #[test]
    fn caps_candidates() {
        // Past the cap a node is dropped, and the caller has to be told.
        let mut candidates = std::collections::BTreeMap::new();
        for seed in 0..super::LOCATION_CANDIDATE_LIMIT {
            let node = iroh::SecretKey::from_bytes(&[seed as u8 + 1; 32]).public();
            assert!(super::add_candidate(&mut candidates, node, "raw"));
        }

        let extra = iroh::SecretKey::from_bytes(&[0u8; 32]).public();
        assert!(!super::add_candidate(&mut candidates, extra, "raw"));
        assert_eq!(candidates.len(), super::LOCATION_CANDIDATE_LIMIT);

        let known = *candidates.keys().next().unwrap();
        assert!(super::add_candidate(&mut candidates, known, "raw"));
    }

    #[test]
    fn drops_unheld_holder() {
        // A node holding the same bytes under another object is not a copy of
        // this version, but a configured or queued target that has not received
        // it yet still is.
        let absent = LocationSummary::absent();
        assert!(super::peer_copy(node_id(), false, Ok(absent.clone())).is_none());
        assert_eq!(
            super::peer_copy(node_id(), true, Ok(absent.clone())).map(|copy| copy.state),
            Some(BlobCopyState::Pending)
        );
        assert_eq!(
            super::peer_copy(node_id(), false, Err(LocationSummaryError::Denied))
                .map(|copy| copy.state),
            Some(BlobCopyState::Denied)
        );
        assert_eq!(
            super::peer_copy(node_id(), false, Err(LocationSummaryError::Aborted))
                .map(|copy| copy.state),
            Some(BlobCopyState::Unreachable)
        );
    }

    #[test]
    fn openapi_lists_locations() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

        assert!(openapi["paths"].get("/blobs/locations").is_some());
        assert!(
            openapi["components"]["schemas"]["BlobCopyResponse"]["properties"]
                .get("state")
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
}
