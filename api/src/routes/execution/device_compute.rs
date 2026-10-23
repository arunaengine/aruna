//! Serves the owner view of this device's local executor and the runs on it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use aruna_core::id::NodeId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::{RemoteBinding, SyncRefusal};
use aruna_operations::device::compute::{ComputeStatus, compute_status};
use aruna_operations::device::publish_run::{
    PublishRunError, PublishRunInput, PublishedRun, publish_run,
};

use crate::auth::require_owner;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::execution::jobs::{map_job_request, map_local_error};
use crate::server::state::ServerState;

/// The device tag itself is declared by the device plane's own document.
#[derive(OpenApi)]
#[openapi()]
pub struct DeviceComputeDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DeviceComputeDoc::openapi())
        .routes(routes!(get_device_compute))
        .routes(routes!(publish_device_run))
}

/// The local executor and the owner's runs on it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceComputeResponse {
    /// Whether this device has an executor backend at all.
    pub enabled: bool,
    /// Wire kind of the backend a local run would use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    /// Whether that backend answered its health probe just now.
    pub healthy: bool,
    /// A paused plane accepts no local run.
    pub paused: bool,
    pub limits: DeviceComputeLimits,
    pub running: u32,
    pub queued: u32,
    /// Why a local run would be refused right now, if it would be.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// The owner's configured ceilings. An absent dimension is unmeasured, never
/// zero, and bounds nothing.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceComputeLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cpu_cores: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ram_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_disk_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
}

impl From<ComputeStatus> for DeviceComputeResponse {
    fn from(status: ComputeStatus) -> Self {
        Self {
            enabled: status.enabled,
            backend: status.backend,
            healthy: status.healthy,
            paused: status.paused,
            limits: DeviceComputeLimits {
                max_cpu_cores: status.limits.max_cpu_cores,
                max_ram_bytes: status.limits.max_ram_bytes,
                max_disk_bytes: status.limits.max_disk_bytes,
                max_concurrent: status.limits.max_concurrent,
            },
            running: status.running,
            queued: status.queued,
            message: status.message,
        }
    }
}

#[utoipa::path(
    get,
    path = "/device/compute",
    tag = "device",
    summary = "Report this device's compute plane",
    description = r#"Describes the executor this machine runs the owner's jobs on.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- `enabled` reflects the executor the desktop app configured, and `healthy` is probed on the spot,
  so a stopped container daemon reads as enabled but unhealthy.
- `paused` is the device's compute drain: while it is set, a local submission is refused.
- `message` names why a local run would be refused right now: the pause, a missing backend, or the
  health probe's own reason.
- `max_concurrent` also refuses a submission beyond it; the other limits only describe the ceiling.
- `running` and `queued` come from this device's durable job records in one bounded scan, so they
  are exact and survive a restart.
- The runs themselves are listed by this device's own `GET /jobs/`; there is no separate listing.
- Nothing here is realm state: the plane is never advertised and the realm never dispatches to it."#,
    responses(
        (status = 200, description = "The device's compute plane", body = DeviceComputeResponse,
            example = json!({
                "enabled": true,
                "backend": "docker",
                "healthy": true,
                "paused": false,
                "limits": {"max_cpu_cores": 4, "max_ram_bytes": 8589934592_i64, "max_concurrent": 1},
                "running": 1,
                "queued": 0
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_device_compute(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<DeviceComputeResponse>> {
    let auth = require_owner(&state, auth).await?;
    let status = compute_status(&state.get_ctx(), auth.user_id)
        .await
        .map_err(map_local_error)?;
    Ok(Json(status.into()))
}

/// Where a finished local run is published.
#[derive(Debug, Deserialize, ToSchema)]
pub struct PublishRunRequest {
    /// Realm node that holds the target bucket.
    pub node_id: String,
    pub bucket: String,
    /// Key prefix the outputs are copied under. Empty copies them to the bucket root.
    #[serde(default)]
    pub prefix: String,
}

/// One output as the realm node now holds it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PublishedOutput {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PublishRunResponse {
    pub outputs: Vec<PublishedOutput>,
    /// The run crate outcome, as `GET /compute/jobs/{job_id}` reports it.
    pub run_crate: serde_json::Value,
}

impl From<PublishedRun> for PublishRunResponse {
    fn from(run: PublishedRun) -> Self {
        Self {
            outputs: run
                .outputs
                .into_iter()
                .map(|output| PublishedOutput {
                    bucket: output.bucket,
                    key: output.key,
                    version_id: output.version_id.to_string(),
                    size: output.size,
                })
                .collect(),
            run_crate: run.run_crate.to_public_json(),
        }
    }
}

fn map_publish_error(error: PublishRunError) -> ServerError {
    match error {
        PublishRunError::NotFound => ServerError::NotFound,
        PublishRunError::NotFinished
        | PublishRunError::Collision(_)
        | PublishRunError::TargetChanged => ServerError::Conflict(error.to_string()),
        PublishRunError::NotRealmNode(_)
        | PublishRunError::Refused {
            refusal: SyncRefusal::NotFound | SyncRefusal::Invalid(_),
            ..
        } => ServerError::BadRequestReason(error.to_string()),
        PublishRunError::Refused {
            refusal: SyncRefusal::Unauthorized | SyncRefusal::Forbidden,
            ..
        } => ServerError::Forbidden,
        PublishRunError::Unreachable(_) => ServerError::BadGatewayReason(error.to_string()),
        PublishRunError::Refused {
            refusal: SyncRefusal::Unavailable,
            ..
        }
        | PublishRunError::Crate(_)
        | PublishRunError::Unavailable(_) => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/device/compute/runs/{job_id}/publish",
    tag = "device",
    summary = "Publish a finished local run to the realm",
    description = r#"Copies the outputs of one finished local run into a realm bucket and writes its crate there.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- A local run stays on this device until it is published: until then its `run_crate` reads
  `local` and nothing about it reaches the realm.
- The realm node named by `node_id` pulls each output at the exact version the run wrote and
  stores it at `<prefix>/<source bucket>/<key>` in `bucket`, as the device owner, so outputs of
  different device buckets never meet. The bucket must exist there and grant the owner WRITE under
  `prefix`; that is checked before anything is copied, also for a run without outputs.
- Once every output is copied, the run crate is created in the realm and names the realm copies.
- The first request freezes its target before anything is copied. Repeating it is safe and answers
  with the copies already made; a crate already written is not written again. A request naming
  another node, bucket or prefix for the same run is refused with 409.
- A run that has not finished, whose crate is still being prepared, or with two outputs on one
  key is refused with 409."#,
    params(("job_id" = String, Path, description = "Job id of the local run: a 26-character ULID; an unparseable id is 404")),
    request_body(content = PublishRunRequest, example = json!({
        "node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga",
        "bucket": "lab-data",
        "prefix": "runs/assembly"
    })),
    responses(
        (status = 200, description = "The outputs were copied and the crate outcome is recorded", body = PublishRunResponse,
            example = json!({
                "outputs": [{
                    "bucket": "lab-data",
                    "key": "runs/assembly/run-out/contigs.fasta",
                    "version_id": "01JZ7Q3V4K8M2N6P0R5S9T1W3X",
                    "size": 1048576
                }],
                "run_crate": {"status": "written", "resource": "01JZ7Q3V4K8M2N6P0R5S9T1W3Y"}
            })),
        (status = 400, description = "The node is no realm server, or the bucket does not exist there", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the device owner, or the realm refused the owner's write", body = ErrorResponse),
        (status = 404, description = "No such local run, or this node is not a user node", body = ErrorResponse),
        (status = 409, description = "The run has not finished, two outputs share one key, or it is published to another target", body = ErrorResponse),
        (status = 502, description = "The realm node could not be reached", body = ErrorResponse),
        (status = 503, description = "The realm node or this device could not finish the publish now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn publish_device_run(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Json(request): Json<PublishRunRequest>,
) -> ServerResult<Json<PublishRunResponse>> {
    let auth = require_owner(&state, auth).await?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(map_job_request)?;
    let node_id = NodeId::from_str(&request.node_id)
        .map_err(|_| ServerError::BadRequestReason("node_id is not a node id".to_string()))?;
    let published = publish_run(
        &state.get_ctx(),
        PublishRunInput {
            job_id,
            owner: auth.user_id,
            node_id: state.get_node_id(),
            target: RemoteBinding {
                node_id,
                bucket: request.bucket,
                prefix: request.prefix,
            },
        },
    )
    .await
    .map_err(map_publish_error)?;
    Ok(Json(published.into()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn openapi_lists_route() {
        let openapi = crate::openapi::ApiDoc::openapi();
        assert!(openapi.paths.paths.contains_key("/device/compute"));
        assert!(
            openapi
                .paths
                .paths
                .contains_key("/device/compute/runs/{job_id}/publish")
        );
    }
}
