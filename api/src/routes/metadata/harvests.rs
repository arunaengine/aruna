//! Serves the group route that submits one harvest run of an OAI-PMH repository connector.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::harvest::{HarvestJobSpec, HarvestSelector};
use aruna_core::structs::execution::job::JobPayload;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_operations::driver::drive;
use aruna_operations::harvest::create_source::{
    CreateSourceError, CreateSourceInput, CreateSourceOperation,
};
use aruna_operations::jobs::service::{lookup_job_dedup, submit_harvest_job};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;

use crate::auth::{parse_group_id, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::ensure_metadata_scope;
use crate::routes::execution::jobs::{job_urls, map_submit_error};
use crate::server::state::ServerState;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HarvestRequest {
    /// Names the source within the group; records keep their identity per namespace.
    pub namespace: String,
    pub target_prefix: String,
    #[serde(default)]
    pub set: Option<String>,
    #[serde(default)]
    pub metadata_prefix: Option<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct HarvestResponse {
    pub job_id: String,
    pub created: bool,
    pub source_id: String,
    pub owner_node_url: String,
    pub status_url: String,
    pub report_url: String,
}

#[utoipa::path(
    post,
    path = "/metadata/groups/{group_id}/repositories/{connector_id}/harvests",
    tag = "metadata/repositories",
    summary = "Submit a harvest run",
    description = r#"Queues one harvest run of an OAI-PMH connector into the group's metadata.

**Authentication**: unrestricted realm bearer token with WRITE on the group's metadata path.

**Behavior**
- The connector and `namespace` name one harvest source. The first run creates it, and later runs
  reuse it, so each run continues from the stored cursor and only applies newer records.
- Records become metadata documents under `target_prefix` and keep one identity per namespace, so
  a repeated or overlapping run creates no duplicates.
- Only `oai_dc` is mapped; an omitted `metadata_prefix` means `oai_dc`. `set` limits the run to one
  OAI-PMH set.
- Send `idempotency_key` to make retries safe: a repeat with the same key and the same source
  returns the earlier job with `created` false.
- Follow and cancel the run with the job routes at `status_url`.

**Errors**
- 409 when the source exists with another target prefix or set, when its last run has not
  finished, when the key is bound to another job, or when the group is being deleted."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "OAI-PMH connector to harvest, as a 26-character ULID")
    ),
    request_body(content = HarvestRequest, example = json!({
        "namespace": "zenodo", "target_prefix": "imported/zenodo", "set": "user-aruna",
        "idempotency_key": "zenodo-2026-11-03"
    })),
    responses(
        (status = 202, description = "The harvest job was admitted or an earlier one with the same key returned", body = HarvestResponse, example = json!({
            "job_id": "01JHRVST0123456789ABCDEFGH", "created": true,
            "source_id": "01JSRCE00123456789ABCDEFGH",
            "owner_node_url": "https://node-a.example.org/api/v1",
            "status_url": "https://node-a.example.org/api/v1/compute/jobs/01JHRVST0123456789ABCDEFGH",
            "report_url": "https://node-a.example.org/api/v1/compute/jobs/01JHRVST0123456789ABCDEFGH/report"
        })),
        (status = 400, description = "Invalid ids, namespace, target prefix or metadata prefix, or a connector that is not OAI-PMH", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or no WRITE on the group's metadata path", body = ErrorResponse),
        (status = 404, description = "The connector does not exist in the group", body = ErrorResponse),
        (status = 409, description = "Another source definition, an unfinished run of the source, an idempotency key bound to another job, or a group being deleted", body = ErrorResponse),
        (status = 503, description = "The job could not be placed right now; the unchanged request may be retried", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_harvest(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
    Json(request): Json<HarvestRequest>,
) -> ServerResult<(StatusCode, Json<HarvestResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = Ulid::from_string(&connector_id)
        .map_err(|_| ServerError::BadRequestReason("connector_id is not a ULID".to_string()))?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::WRITE).await?;
    let source = drive(
        CreateSourceOperation::new(CreateSourceInput {
            group_id,
            created_by: auth.user_id,
            connector_id,
            namespace: request.namespace,
            target_prefix: request.target_prefix,
            selector: HarvestSelector {
                set: request.set,
                metadata_prefix: request.metadata_prefix,
            },
            schedule_interval_ms: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(source_error)?;
    let spec = HarvestJobSpec {
        source_id: source.source_id,
        group_id,
    };
    let replay = match request.idempotency_key.as_deref() {
        Some(key) => lookup_job_dedup(
            &state.get_ctx(),
            auth.user_id,
            key,
            JobPayload::Harvest(spec.clone()).plan_digest(),
        )
        .await
        .map_err(map_submit_error)?,
        None => None,
    };
    let result = match replay {
        Some(result) => result,
        None => submit_harvest_job(
            &state.get_ctx(),
            spec,
            auth.user_id,
            auth.realm_id,
            state.get_node_id(),
            request.idempotency_key,
        )
        .await
        .map_err(map_submit_error)?,
    };
    let urls = job_urls(&state, result.job_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(HarvestResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
            source_id: source.source_id.to_string(),
            owner_node_url: urls.owner_node_url,
            status_url: urls.status_url,
            report_url: urls.report_url,
        }),
    ))
}

fn source_error(error: CreateSourceError) -> ServerError {
    match error {
        CreateSourceError::GroupWrite(GroupWriteError::Frozen | GroupWriteError::Deleted)
        | CreateSourceError::SourceConflict(_) => ServerError::Conflict(error.to_string()),
        CreateSourceError::ConnectorNotFound => ServerError::NotFound,
        CreateSourceError::EmptyNamespace
        | CreateSourceError::EmptyTargetPrefix
        | CreateSourceError::LongTargetPrefix(_)
        | CreateSourceError::UnsupportedMetadataPrefix(_)
        | CreateSourceError::UnsupportedConnector => {
            ServerError::BadRequestReason(error.to_string())
        }
        CreateSourceError::GroupWrite(_)
        | CreateSourceError::Storage(_)
        | CreateSourceError::Conversion(_)
        | CreateSourceError::Failed
        | CreateSourceError::InvalidStateEvent { .. } => {
            ServerError::InternalError(error.to_string())
        }
    }
}

#[cfg(test)]
#[path = "harvests_tests.rs"]
mod tests;
