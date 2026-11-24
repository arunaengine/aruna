//! Serves the workflow registry: list, create, read, change and delete workflows, and check an
//! unsaved workflow without storing it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::workflow::StoredWorkflowLanguage;
use aruna_ops_work::workflows::import::{CreateWorkflowOperation, NewWorkflow};
use aruna_ops_work::workflows::inspect::{Draft, ValidateDraftOperation};
use aruna_ops_work::workflows::reads::{
    DeleteWorkflowOperation, GetWorkflowOperation, ListWorkflowsOperation, PatchWorkflowOperation,
    WorkflowFilter, WorkflowPatch,
};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{
    CreateWorkflowRequest, SourceFileBody, ValidateRequest, ValidateResponse, WorkflowCreated,
    WorkflowPage, WorkflowPatchBody, WorkflowQuery, WorkflowSummary,
};
use super::{catalog_error, limit, source_input, ulid};
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_workflows, create_workflow))
        .routes(routes!(get_workflow, patch_workflow, delete_workflow))
        .routes(routes!(validate_workflow))
}

#[utoipa::path(
    get,
    path = "/compute/workflows",
    tag = "compute/workflows",
    summary = "List workflows",
    description = r#"Lists the workflows the caller sees, paged by workflow id.

**Authentication**: realm bearer token.

**Behavior**
- Without `group_id` the list holds the workflows of every group the caller may read and every
  public workflow; with it, the workflows of that group the caller sees.
- `can_write` tells whether the caller may change, version and delete a workflow.
- `next_cursor` continues the list; `total` counts every match."#,
    params(WorkflowQuery),
    responses(
        (status = 200, description = "One page of workflows", body = WorkflowPage, example = json!({
            "items": [{
                "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "Read quality", "description": "FastQC and MultiQC over a run",
                "language": "nextflow", "visibility": "group", "latest_version": "2",
                "version_count": 2, "edam_formats": [], "updated_at": "2026-11-24T08:00:00+00:00",
                "created_by": "01K8W1A0000000000000000000", "can_write": true
            }],
            "next_cursor": null, "total": 1
        })),
        (status = 400, description = "A malformed group id, language or cursor", body = ErrorResponse),
        (status = 403, description = "The token cannot read the named group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_workflows(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<WorkflowQuery>,
) -> ServerResult<Json<WorkflowPage>> {
    let group_id = query
        .group_id
        .as_deref()
        .map(|group| ulid(group, "group_id"))
        .transpose()?;
    let language = match query.language.as_deref() {
        Some(text) => Some(StoredWorkflowLanguage::parse(text).ok_or_else(|| {
            ServerError::BadRequestReason(format!("{text} is no workflow language"))
        })?),
        None => None,
    };
    let after = query
        .cursor
        .as_deref()
        .map(|cursor| ulid(cursor, "cursor"))
        .transpose()?;
    let filter = WorkflowFilter {
        group_id,
        language,
        edam_format: query.edam_format,
        text: query.q,
    };
    let list = ListWorkflowsOperation::new(&auth, filter, after, limit(query.limit));
    let page = drive(list, &state.get_ctx())
        .await
        .map_err(super::catalog_error)?;
    Ok(Json(WorkflowPage {
        items: page.items.into_iter().map(WorkflowSummary::from).collect(),
        next_cursor: page.next.map(|id| id.to_string()),
        total: Some(page.total as u64),
    }))
}

#[utoipa::path(
    post,
    path = "/compute/workflows",
    tag = "compute/workflows",
    summary = "Create a workflow",
    description = r#"Creates a workflow in a group with its first version from exactly one source.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- Sources: `uwe_yaml` (a composed text lowered with the group's catalog tools), `upload`
  (files as UTF-8 or base64), `wir` (a wir document), `objects` (bucket objects read under the
  caller's READ) or `git`.
- A preflight coordinator checks the files; the check, the input schema and the graph are
  stored with the version, and the images it names are pinned where a backend pins them.
- Without `language` the coordinator detects it.

**Errors**
- 400 for paths that leave the workflow folder, a missing entry file, files it cannot read, or
  a Git source, which this node cannot fetch yet.
- 503 when no coordinator answers."#,
    request_body(content = CreateWorkflowRequest, example = json!({
        "group_id": "01JABCDEF0123456789ABCDEFG", "name": "Say hello", "visibility": "group",
        "upload": {"entrypoint": "hello.cwl", "files": [{"path": "hello.cwl",
            "content": "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\n"}]}
    })),
    responses(
        (status = 201, description = "The workflow and its first version", body = WorkflowCreated, example = json!({
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1", "commit": null,
            "digest": "sha256:5f2b8c0a6e0d3f1b9c4d7e8a1b2c3d4e5f60718293a4b5c6d7e8f90123456789"
        })),
        (status = 400, description = "An invalid source or name", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's workflows", body = ErrorResponse),
        (status = 404, description = "The bucket of an objects source does not exist", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_workflow(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Json(request): Json<CreateWorkflowRequest>,
) -> ServerResult<(StatusCode, Json<WorkflowCreated>)> {
    let input = NewWorkflow {
        group_id: ulid(&request.group_id, "group_id")?,
        name: request.name,
        description: request.description,
        language: request.language.map(Into::into),
        visibility: request.visibility.unwrap_or_default().into(),
        source: source_input(request.source)?,
    };
    let create = CreateWorkflowOperation::new(&auth, state.get_node_id(), input);
    let created = drive(create, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok((StatusCode::CREATED, Json(created_body(created))))
}

pub(super) fn created_body(created: aruna_ops_work::workflows::import::Created) -> WorkflowCreated {
    WorkflowCreated {
        workflow_id: created.workflow_id.to_string(),
        version: created.version.to_string(),
        commit: created.commit,
        digest: Some(created.digest),
    }
}

#[utoipa::path(
    get,
    path = "/compute/workflows/{workflow_id}",
    tag = "compute/workflows",
    summary = "Read a workflow",
    description = r#"Returns one workflow the caller sees.

**Authentication**: realm bearer token.

**Behavior**
- A workflow is seen by the readers of its group, and by every realm user once it is public.
- A workflow the caller does not see is not found."#,
    params(("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The workflow", body = WorkflowSummary, example = json!({
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "Read quality", "description": null, "language": "nextflow", "visibility": "public",
            "latest_version": "2", "version_count": 2, "edam_formats": [],
            "updated_at": "2026-11-24T08:00:00+00:00", "created_by": "01K8W1A0000000000000000000",
            "can_write": false
        })),
        (status = 400, description = "The id is not a ULID", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_workflow(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(workflow_id): Path<String>,
) -> ServerResult<Json<WorkflowSummary>> {
    let workflow_id = ulid(&workflow_id, "workflow_id")?;
    let view = drive(
        GetWorkflowOperation::new(&auth, workflow_id),
        &state.get_ctx(),
    )
    .await
    .map_err(catalog_error)?;
    Ok(Json(view.into()))
}

#[utoipa::path(
    patch,
    path = "/compute/workflows/{workflow_id}",
    tag = "compute/workflows",
    summary = "Change a workflow",
    description = r#"Renames, describes or shares a workflow the caller may write.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- Fields left out stay; a `description` of null clears it.
- `visibility: public` lets every realm user read the workflow; only the group writes it."#,
    params(("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID")),
    request_body(content = WorkflowPatchBody, example = json!({"name": "Read quality", "visibility": "public"})),
    responses(
        (status = 200, description = "The changed workflow", body = WorkflowSummary, example = json!({
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "Read quality", "description": null, "language": "nextflow", "visibility": "public",
            "latest_version": "2", "version_count": 2, "edam_formats": [],
            "updated_at": "2026-11-24T08:00:00+00:00", "created_by": "01K8W1A0000000000000000000",
            "can_write": false
        })),
        (status = 400, description = "An empty name or a malformed id", body = ErrorResponse),
        (status = 403, description = "The caller sees the workflow but may not change it", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn patch_workflow(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(workflow_id): Path<String>,
    Json(patch): Json<WorkflowPatchBody>,
) -> ServerResult<Json<WorkflowSummary>> {
    let workflow_id = ulid(&workflow_id, "workflow_id")?;
    let patch = WorkflowPatch {
        name: patch.name,
        description: patch.description,
        visibility: patch.visibility.map(Into::into),
    };
    let view = drive(
        PatchWorkflowOperation::new(&auth, workflow_id, patch),
        &state.get_ctx(),
    )
    .await
    .map_err(catalog_error)?;
    Ok(Json(view.into()))
}

#[utoipa::path(
    delete,
    path = "/compute/workflows/{workflow_id}",
    tag = "compute/workflows",
    summary = "Delete a workflow",
    description = r#"Deletes a workflow the caller may write with all its versions and files.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- A second delete answers 404."#,
    params(("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID")),
    responses(
        (status = 204, description = "The workflow is deleted"),
        (status = 400, description = "The id is not a ULID", body = ErrorResponse),
        (status = 403, description = "The caller sees the workflow but may not delete it", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_workflow(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(workflow_id): Path<String>,
) -> ServerResult<StatusCode> {
    let workflow_id = ulid(&workflow_id, "workflow_id")?;
    drive(
        DeleteWorkflowOperation::new(&auth, workflow_id),
        &state.get_ctx(),
    )
    .await
    .map_err(catalog_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/compute/workflows/validate",
    tag = "compute/workflows",
    summary = "Check an unsaved workflow",
    description = r#"Checks an unsaved composed text or workflow files and previews the graph; it stores nothing.

**Authentication**: realm bearer token; with `group_id`, READ on the group's workflows.

**Behavior**
- `uwe` lowers `uwe_yaml` with the catalog tools of `group_id` and answers the wir when it checks.
- Any other language checks `files` from `entrypoint` and inspects them for the graph.
- A workflow the coordinator cannot read answers 200 with `ok: false` and the reason.

**Errors**
- 503 when no coordinator answers."#,
    request_body(content = ValidateRequest, example = json!({
        "language": "uwe", "group_id": "01JABCDEF0123456789ABCDEFG",
        "uwe_yaml": "uwe_version: \"1\"\nsteps:\n  first:\n    tool: reverse\n"
    })),
    responses(
        (status = 200, description = "The check and the graph", body = ValidateResponse, example = json!({
            "check": {"ok": true, "diagnostics": []},
            "graph": {"nodes": [{"id": "first", "kind": "step", "label": "first", "tool": "reverse", "version": "1"}],
                "edges": [{"from": {"node": "input:text", "port": "text"}, "to": {"node": "first", "port": "text"}}],
                "complete": true},
            "wir": {"wir_version": "1"}
        })),
        (status = 400, description = "Missing text, files or entry file", body = ErrorResponse),
        (status = 403, description = "No READ on the group's workflows", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn validate_workflow(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Json(request): Json<ValidateRequest>,
) -> ServerResult<Json<ValidateResponse>> {
    let files =
        (request.files.into_iter().map(SourceFileBody::decoded)).collect::<Result<_, _>>()?;
    let draft = Draft {
        language: request.language.into(),
        group_id: request
            .group_id
            .as_deref()
            .map(|group| ulid(group, "group_id"))
            .transpose()?,
        uwe_yaml: request.uwe_yaml,
        files,
        entrypoint: request.entrypoint,
    };
    let report = drive(ValidateDraftOperation::new(&auth, draft), &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(Json(ValidateResponse {
        check: report.check.into(),
        graph: report.graph.map(Into::into),
        wir: report.wir,
    }))
}
