use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, Permission, UserIdentity};
use aruna_core::NodeId;
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
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
use tracing::{info, warn, Instrument};
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
    let (auth, bucket_info) = require_auth_and_bucket(&state, auth, &request.bucket).await?;

    // Check permissions
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
    let path = request.path.clone();
    let version_id = request.version_id.clone();
    let target = match (path.as_deref(), version_id.as_deref()) {
        (None, None) => ReplicateScopeTarget::Bucket,
        (None, Some(_)) => return Err(ServerError::BadRequest),
        (Some(p), None) => ReplicateScopeTarget::Object { key: p.to_string() },
        (Some(p), Some(v)) => ReplicateScopeTarget::Version {
            key: p.to_string(),
            version_id: ulid::Ulid::from_string(v).map_err(|_| ServerError::BadRequest)?,
        },
    };

    let bucket = request.bucket.clone();
    let input = ReplicateScopeInput {
        bucket: request.bucket,
        target,
        target_node_id: node_id,
        user_identity: UserIdentity {
            user_id: auth.user_id,
            realm_key: auth.realm_id,
        },
        replicate_delete_markers: true,
    };
    let target_node_id = input.target_node_id;
    let ctx = state.get_ctx();
    let span = tracing::info_span!(
        "api.manual_replication",
        bucket = %bucket,
        path = ?path,
        version_id = ?version_id,
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
                        "Manual replication succeeded"
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
                        "Manual replication completed with failures"
                    );
                }
                Ok(Some(Err(err))) | Err(err) => {
                    warn!(
                        bucket,
                        path = ?path,
                        version_id = ?version_id,
                        target_node = %target_node_id,
                        error = %err,
                        "Manual replication failed"
                    );
                }
                Ok(None) => {
                    warn!(
                        bucket,
                        path = ?path,
                        version_id = ?version_id,
                        target_node = %target_node_id,
                        "Manual replication produced no result"
                    );
                }
            }
        }
        .instrument(span),
    );

    Ok((StatusCode::ACCEPTED, Json(ReplicateBlobResponse {})))
}

async fn require_auth_and_bucket(
    state: &ServerState,
    auth: Option<AuthContext>,
    bucket: &str,
) -> ServerResult<(AuthContext, aruna_core::structs::BucketInfo)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.realm_id != state.get_realm_id() {
        return Err(ServerError::Forbidden);
    }
    let bucket_info = drive(
        GetBucketInfoOperation::new(bucket.to_owned()),
        &state.get_ctx(),
    )
    .await
    .map_err(|e| match e {
        GetBucketInfoError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?
    .ok_or(ServerError::NotFound)?
    .map_err(|e| match e {
        GetBucketInfoError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok((auth, bucket_info))
}
