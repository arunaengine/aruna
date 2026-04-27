use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::structs::{AuthContext, Permission};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
use aruna_operations::replication::protocol::ReplicationMode;
use aruna_operations::replication::version_replication::{
    ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeTarget,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{Instrument, info, warn};
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "blobs", description = "Blob management and replication")),
    paths(replicate_blob)
)]
pub struct BlobsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/blobs/replicate", post(replicate_blob))
}

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
pub struct ReplicateBlobResponse {}

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

    let mut permission_path = format!(
        "/{}/g/{}/data/{}/{}",
        state.get_realm_id(),
        bucket_info.group_id,
        state.get_node_id(),
        request.bucket
    );
    if let Some(path) = request.path.as_deref() {
        permission_path.push('/');
        permission_path.push_str(path);
    }

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
    let bucket = input.bucket.clone();
    let path_for_span = path.clone();
    let version_id_for_span = version_id.clone();
    let target_node_id = input.target_node_id;
    let ctx = state.get_ctx();
    let span = tracing::info_span!(
        "api.on_demand_replication",
        bucket = %bucket,
        path = ?path_for_span,
        version_id = ?version_id_for_span,
        target_node = %target_node_id,
    );

    tokio::spawn(
        async move {
            match drive(ReplicateScopeOperation::new(input), &ctx).await {
                Ok(Some(Ok(result))) if result.failed == 0 => {
                    info!(bucket,
                        path = ?path,
                        version_id = ?version_id,
                        target_node = %target_node_id,
                        "On-demand replication succeeded"
                    );
                }
                Ok(Some(Ok(result))) => {
                    warn!(
                        bucket,
                        path = ?path,
                        version_id = ?version_id,
                        target_node = %target_node_id,
                        replicated = result.replicated,
                        skipped = result.skipped,
                        failed = result.failed,
                        "On-demand replication completed with failures"
                    );
                }
                Ok(Some(Err(err))) | Err(err) => {
                    warn!(
                        bucket,
                        path = ?path,
                        version_id = ?version_id,
                        target_node = %target_node_id,
                        error = %err,
                        "On-demand replication failed"
                    );
                }
                Ok(None) => {
                    warn!(
                        bucket,
                        path = ?path,
                        version_id = ?version_id,
                        target_node = %target_node_id,
                        "On-demand replication produced no result"
                    );
                }
            }
        }
        .instrument(span),
    );

    Ok((StatusCode::ACCEPTED, Json(ReplicateBlobResponse {})))
}
