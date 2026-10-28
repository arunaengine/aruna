//! Serves where a run id leads: its run crate, read on the node that ran it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_operations::jobs::JobRouteError;
use aruna_operations::jobs::runs::{RunReadError, read_run};
use aruna_operations::jobs::service::resolve_job_owner;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::relay::{node_directed, node_read};
use crate::server::state::ServerState;

/// How long a run read relayed to the node that ran it may take, body included.
const RELAY_DEADLINE: Duration = Duration::from_secs(30);

#[derive(OpenApi)]
#[openapi()]
pub struct RunsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(RunsApiDoc::openapi()).routes(routes!(get_run))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunResponse {
    pub run_id: String,
    /// The submitted job; shown to the submitter only.
    pub logical_job_id: Option<String>,
    /// The run crate document, once written.
    pub run_crate: Option<String>,
    pub run_crate_iri: Option<String>,
    /// The crate's state for the submitter: `pending`, `written`, `denied`, `failed` or `local`,
    /// with a `message` when it was not written.
    #[schema(value_type = Option<Object>)]
    pub run_crate_status: Option<serde_json::Value>,
}

#[utoipa::path(
    get,
    path = "/compute/runs/{run_id}",
    tag = "compute/jobs",
    summary = "Read where a run leads",
    description = r#"Returns the run crate of one run, by the run id a file's origin names.

**Authentication**: realm bearer token. The submitter needs a token without path
restrictions; anyone else needs READ on the run crate with their own token.

**Behavior**
- The node that ran the job keeps the link, also after the job record is pruned. Any other
  node relays the call there with the caller's token.
- Runs made before links were kept answer only while their job record exists.
- A caller who may not see the run gets 404, as for job reads.
- The submitter also gets `run_crate_status`, so a crate that was denied, failed or is kept
  on a device says why instead of only being absent.

**Limits**
- One run per call."#,
    params(("run_id" = String, Path, description = "Run id, as named in a file's origin")),
    responses(
        (status = 200, description = "The run's links", body = RunResponse, example = json!({
            "run_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
            "logical_job_id": "01K8W1A0000000000000000000",
            "run_crate": "01K8W1C0000000000000000000",
            "run_crate_iri": "https://w3id.org/aruna/01K8W1C0000000000000000000",
            "run_crate_status": { "status": "written", "resource": "01K8W1C0000000000000000000" }
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Unknown run, or not visible to the caller", body = ErrorResponse),
        (status = 409, description = "The read was directed at another node", body = ErrorResponse),
        (status = 503, description = "The node that ran the job did not answer", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_run(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> ServerResult<Json<RunResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let deadline = tokio::time::Instant::now() + RELAY_DEADLINE;
    run_answer(&state, &auth, bearer, &headers, (&run_id, deadline)).await
}

/// What the caller may see of run `run_id`, read on the node that ran it.
pub(crate) async fn run_answer(
    state: &ServerState,
    auth: &AuthContext,
    bearer: Option<ValidatedBearer>,
    headers: &HeaderMap,
    (run_id, deadline): (&str, tokio::time::Instant),
) -> ServerResult<Json<RunResponse>> {
    let run_id = crate::jobs::parse_job_id(run_id).map_err(|_| ServerError::NotFound)?;
    let context = state.get_ctx();
    if !node_directed(state, headers)? && context.net_handle.is_some() {
        let owner = resolve_job_owner(&context, run_id)
            .await
            .map_err(|error| match error {
                JobRouteError::Unavailable(reason) => ServerError::ServiceUnavailableReason(reason),
                _ => ServerError::NotFound,
            })?;
        if owner != state.get_node_id() {
            let path = format!("/compute/runs/{run_id}");
            let answer = node_read(state, owner, &path, bearer.as_ref(), deadline).await?;
            return Ok(Json(answer));
        }
    }
    let view = read_run(&context, state.get_realm_id(), auth, run_id)
        .await
        .map_err(|error| match error {
            RunReadError::NotFound => ServerError::NotFound,
            RunReadError::Unavailable(reason) => ServerError::ServiceUnavailableReason(reason),
        })?;
    Ok(Json(RunResponse {
        run_id: view.run_id.to_string(),
        logical_job_id: view.logical_job_id.map(|id| id.to_string()),
        run_crate: view.run_crate.map(|id| id.to_string()),
        run_crate_iri: view.run_crate.map(MetadataRegistryRecord::graph_iri_for),
        run_crate_status: view.run_crate_status,
    }))
}

#[cfg(test)]
#[path = "runs_tests.rs"]
mod tests;
