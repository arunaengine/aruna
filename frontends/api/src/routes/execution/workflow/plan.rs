//! Serves the placement preview of a workflow run: where each step would run and what it would
//! stage, planned like a launch but without writing or admitting anything.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_clock::unix_timestamp_millis;
use aruna_ops_work::jobs::lifecycle::submit_external::IngressNode;
use aruna_ops_work::jobs::node_inputs::ingress_node;
use aruna_ops_work::workflows::runs::plan::{PlanRunOperation, PlannedStep, RunPlan};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Extension, State};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::catalog_error;
use super::run_wire::WorkflowRunRequest;
use super::runs::run_request;
use crate::auth::{RealmAuth, ValidatedBearer};
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(plan_run))
}

/// The plan of one step: where its tasks would run and what they would stage there.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PlannedTaskBody {
    pub step: String,
    pub node_id: Option<String>,
    /// `compute_to_data`, `data_to_compute` or `placed_at_runtime`.
    pub mode: String,
    /// Bytes staged from other nodes; null when the plan cannot know them.
    pub bytes: Option<u64>,
    pub transfer_ms: Option<u64>,
    pub cached: bool,
    /// Tasks of the step; null when a scatter's width is known only at run time.
    pub task_count: Option<u64>,
}

/// A node no task could run on, and why.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RejectedNodeBody {
    pub node_id: String,
    pub verdict: String,
}

/// The placement preview of a run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunPlanBody {
    /// False when dynamic branches, scatters or later placements are known only at run time.
    pub complete: bool,
    pub tasks: Vec<PlannedTaskBody>,
    /// Value checks and simulation messages: `message`, `severity` and where known
    /// `instance_path`.
    #[schema(value_type = Vec<Object>)]
    pub diagnostics: Vec<Value>,
    pub rejected: Vec<RejectedNodeBody>,
}

impl From<&PlannedStep> for PlannedTaskBody {
    fn from(step: &PlannedStep) -> Self {
        Self {
            step: step.step.clone(),
            node_id: step.node_id.map(|node| node.to_string()),
            mode: step.mode.to_owned(),
            bytes: step.bytes,
            transfer_ms: step.transfer_ms,
            cached: step.cached,
            task_count: step.task_count,
        }
    }
}

impl From<RunPlan> for RunPlanBody {
    fn from(plan: RunPlan) -> Self {
        Self {
            complete: plan.complete,
            tasks: plan.tasks.iter().map(PlannedTaskBody::from).collect(),
            diagnostics: plan.diagnostics,
            rejected: (plan.rejected.into_iter())
                .map(|(node, verdict)| RejectedNodeBody {
                    node_id: node.to_string(),
                    verdict,
                })
                .collect(),
        }
    }
}

#[utoipa::path(
    post,
    path = "/compute/workflow-runs/plan",
    tag = "compute/workflow-runs",
    summary = "Preview where a workflow run would run",
    description = r#"Plans a run request without starting it: nothing is written and no job is admitted.

**Authentication**: realm bearer token with READ on the group's workflows.

**Behavior**
- The caller must read the version and every pinned input this node holds.
- Values that do not pass the version's form come back as `diagnostics`, with no tasks and
  `complete` false.
- Each task that reads only pinned inputs is planned like its launch: `compute_to_data` where
  every input already lies, else `data_to_compute` with the bytes and time to stage them.
- A task that reads the outputs of other tasks is `placed_at_runtime`; its `node_id` hints at
  the node of the task it reads from. Such tasks, and steps whose number of tasks is known only
  at run time, make `complete` false.
- `rejected` names each node a planned task could not run on, with the reason.

**Errors**
- 400 for malformed ids, a version that is no number, values that are no object, a missing
  input bucket, or a placement on another node.
- 403 without READ on a pinned input.
- 404 when the group or the version is not visible to the caller.
- 503 when no workflow coordinator answered."#,
    request_body(content = WorkflowRunRequest, example = json!({
        "group_id": "01JABCDEF0123456789ABCDEFG", "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
        "version": "1", "values": {"message": "Hola"},
        "inputs": {"reads": {"bucket": "reads", "key": "run1/a.fastq", "version_id": "01K8W1C0000000000000000000"}}
    })),
    responses(
        (status = 200, description = "The plan", body = RunPlanBody, example = json!({
            "complete": false,
            "tasks": [
                {"step": "align", "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
                    "mode": "compute_to_data", "bytes": 0, "transfer_ms": 0, "cached": false, "task_count": 1},
                {"step": "report", "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
                    "mode": "placed_at_runtime", "bytes": null, "transfer_ms": null, "cached": false, "task_count": 1}
            ],
            "diagnostics": [],
            "rejected": [{"node_id": "8a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f9",
                "verdict": "not enough resources"}]
        })),
        (status = 400, description = "An invalid request", body = ErrorResponse),
        (status = 403, description = "A missing permission", body = ErrorResponse),
        (status = 404, description = "The group or version is not visible", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn plan_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<WorkflowRunRequest>,
) -> ServerResult<Json<RunPlanBody>> {
    let token = crate::jobs::forwarded_job_auth(bearer)?;
    let request = run_request(&state, request, None)?;
    let context = state.get_ctx();
    let now_ms = unix_timestamp_millis();
    let node = ingress_node(context.node_and_realm(), context.fresh_heartbeats(), now_ms)
        .unwrap_or_else(|| IngressNode {
            local: state.get_node_id(),
            realm_id: state.get_realm_id(),
            now_ms,
            fresh: Default::default(),
        });
    let plan = PlanRunOperation::new(&auth, node, request).with_token(token);
    let plan = drive(plan, &context).await.map_err(catalog_error)?;
    Ok(Json(plan.into()))
}
