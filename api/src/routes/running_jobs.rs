//! Serves the list of background jobs this node runs right now, filtered per caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::permission_granted;
use crate::error::{ServerError, ServerResult};
use crate::routes::execution::jobs::{JobProgressResponse, rfc3339};
use crate::routes::info::{InfoAccess, info_access};
use crate::server::state::ServerState;
use aruna_core::jobs::{JobTarget, RunningJobView};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_operations::auth::request_authorization::BucketAccess;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(get_running_jobs))
}

/// The jobs this node runs now: rows the caller may see, and how many others run.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NodeJobsResponse {
    pub jobs: Vec<NodeJobResponse>,
    /// Jobs the caller may not see in detail.
    pub hidden: u32,
}

/// One running job. A visitor gets only `kind`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NodeJobResponse {
    pub kind: String,
    /// Absent when the job works on no single bucket or dataset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<JobTargetResponse>,
    /// Absent while the job counts nothing; `total` is absent when only a count exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<JobProgressResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum JobTargetResponse {
    Bucket {
        bucket: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    Dataset {
        dataset_id: String,
    },
}

impl From<RunningJobView> for NodeJobResponse {
    fn from(job: RunningJobView) -> Self {
        Self {
            kind: job.kind.name().to_string(),
            target: job.target.map(|target| match target {
                JobTarget::Bucket { bucket, path, .. } => {
                    JobTargetResponse::Bucket { bucket, path }
                }
                JobTarget::Dataset { dataset_id } => JobTargetResponse::Dataset {
                    dataset_id: dataset_id.to_string(),
                },
            }),
            progress: job.progress.map(|progress| JobProgressResponse {
                current: progress.current,
                total: progress.total,
                unit: progress.unit,
            }),
            started_at: Some(rfc3339(job.started_at_ms)),
        }
    }
}

#[utoipa::path(
    get,
    path = "/system/jobs",
    tag = "system/info",
    summary = "List the background jobs this node runs now",
    description = r#"Lists the background jobs this node runs right now, answered locally and never routed to a peer.

**Authentication**: optional bearer token. A token of another realm counts as anonymous.

**Behavior**
- A realm admin (WRITE on the realm configuration admin path) sees every job with what it works on,
  its progress and its start time, including jobs that belong to no group. `hidden` is 0.
- Another caller with a token of this realm sees a job in detail when it may read what the job
  works on (its key, or all of its prefix or bucket with no deny below it), or the job's group
  when the job works on no bucket, or when it created the job with an unrestricted token. Every
  other job only adds to `hidden`.
- An anonymous caller gets one entry with only `kind` per running job and `hidden` 0.
- `target` is a bucket with an optional key or prefix, or a dataset id. `progress` is present only
  while the job counts something; `total` is absent when the job has only a count.
- The list is a snapshot: a job may finish right after it is listed."#,
    responses(
        (
            status = 200,
            description = "The running jobs the caller may see and how many others run",
            body = NodeJobsResponse,
            example = json!({
                "jobs": [
                    {
                        "kind": "import_rocrate",
                        "target": {"bucket": "bakrep-reference", "path": "records/part-1/"},
                        "progress": {"current": 14402, "total": 20000, "unit": "entries"},
                        "started_at": "2026-11-01T10:00:00+00:00"
                    },
                    {
                        "kind": "register_identifiers",
                        "target": {"dataset_id": "01JABCDEF0123456789ABCDEFG"},
                        "started_at": "2026-11-01T10:40:00+00:00"
                    }
                ],
                "hidden": 2
            })
        )
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_running_jobs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<NodeJobsResponse>)> {
    let jobs = state.jobs_runtime().running_list();
    let response = visible_jobs(&state, auth.as_ref(), jobs).await?;
    Ok((StatusCode::OK, Json(response)))
}

/// Grades each job for the caller with the realm admin check and the normal read check.
pub(crate) async fn visible_jobs(
    state: &ServerState,
    auth: Option<&AuthContext>,
    mut jobs: Vec<RunningJobView>,
) -> ServerResult<NodeJobsResponse> {
    jobs.sort_by_key(|job| (job.started_at_ms, job.kind.name()));
    let access = info_access(state, auth).await;
    let auth = match (access, auth) {
        (InfoAccess::Public, _) | (_, None) => {
            let jobs = jobs.iter().map(NodeJobResponse::kind_only).collect();
            return Ok(NodeJobsResponse { jobs, hidden: 0 });
        }
        (InfoAccess::Admin, Some(_)) => {
            let jobs = jobs.into_iter().map(NodeJobResponse::from).collect();
            return Ok(NodeJobsResponse { jobs, hidden: 0 });
        }
        (InfoAccess::Realm, Some(auth)) => auth,
    };
    let mut response = NodeJobsResponse {
        jobs: Vec::new(),
        hidden: 0,
    };
    for job in jobs {
        if may_read(state, auth, &job).await? {
            response.jobs.push(job.into());
        } else {
            response.hidden = response.hidden.saturating_add(1);
        }
    }
    Ok(response)
}

impl NodeJobResponse {
    fn kind_only(job: &RunningJobView) -> Self {
        Self {
            kind: job.kind.name().to_string(),
            target: None,
            progress: None,
            started_at: None,
        }
    }
}

async fn may_read(
    state: &ServerState,
    auth: &AuthContext,
    job: &RunningJobView,
) -> ServerResult<bool> {
    if auth.path_restrictions.is_none() && job.created_by == auth.user_id {
        return Ok(true);
    }
    let Some(group_id) = job.group_id else {
        return Ok(false);
    };
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    let Some(JobTarget::Bucket {
        bucket,
        path,
        exact,
    }) = &job.target
    else {
        let path = group_permission_path(realm_id, group_id, node_id);
        return permission_granted(state, auth, path, Permission::READ).await;
    };
    let context = state.get_ctx();
    let access = BucketAccess::load(&context, auth, (realm_id, group_id, node_id), bucket)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    // A prefix shows only when the caller may read every key the job may touch below it.
    let path = path.as_deref().unwrap_or_default();
    Ok(match exact {
        true => access.allows(Some(path), &Permission::READ),
        false => access.reads_below(path),
    })
}

#[cfg(test)]
#[path = "running_tests.rs"]
mod tests;
