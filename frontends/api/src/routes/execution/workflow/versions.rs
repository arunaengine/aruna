//! Serves the versions of a workflow and their stored preflight results: check, input schema,
//! graph and tools, and the check of input values.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::workflows::import::AddVersionOperation;
use aruna_ops_work::workflows::inspect::{
    Facet, FacetValue, InspectOperation, ValidateValuesOperation,
};
use aruna_ops_work::workflows::versions::{GetVersionOperation, ListVersionsOperation};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde_json::Value;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::registry::created_body;
use super::wire::{
    CreateVersionRequest, PageQuery, ToolList, ToolSummary, ValuesCheck, ValuesRequest,
    VersionPage, WorkflowCheck, WorkflowCreated, WorkflowGraphBody, WorkflowVersionBody,
};
use super::{catalog_error, limit, source_input, ulid, version_number};
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_versions, create_version))
        .routes(routes!(get_version))
        .routes(routes!(get_check))
        .routes(routes!(get_schema))
        .routes(routes!(get_graph))
        .routes(routes!(get_tools))
        .routes(routes!(validate_values))
}

#[utoipa::path(
    get,
    path = "/compute/workflows/{workflow_id}/versions",
    tag = "compute/workflows",
    summary = "List workflow versions",
    description = r#"Lists the versions of a workflow the caller sees, in number order.

**Authentication**: realm bearer token.

**Behavior**
- `next_cursor` continues the list; `total` counts every version."#,
    params(
        ("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID"),
        PageQuery
    ),
    responses(
        (status = 200, description = "One page of versions", body = VersionPage, example = json!({
            "items": [{
                "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1", "language": "cwl",
                "entrypoint": "hello.cwl", "files": ["hello.cwl"], "commit": null,
                "digest": "sha256:5f2b8c0a6e0d3f1b9c4d7e8a1b2c3d4e5f60718293a4b5c6d7e8f90123456789",
                "created_at": "2026-11-24T08:00:00+00:00", "created_by": "01K8W1A0000000000000000000",
                "message": null, "uwe_yaml": null
            }],
            "next_cursor": null, "total": 1
        })),
        (status = 400, description = "A malformed id or cursor", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_versions(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(workflow_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> ServerResult<Json<VersionPage>> {
    let workflow_id = ulid(&workflow_id, "workflow_id")?;
    let after =
        match query.cursor.as_deref() {
            Some(cursor) => Some(cursor.parse().map_err(|_| {
                ServerError::BadRequestReason("cursor is no version number".into())
            })?),
            None => None,
        };
    let list = ListVersionsOperation::new(&auth, workflow_id, after, limit(query.limit));
    let page = drive(list, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(VersionPage {
        items: page
            .items
            .into_iter()
            .map(WorkflowVersionBody::from)
            .collect(),
        next_cursor: page.next.map(|version| version.to_string()),
        total: Some(page.total as u64),
    }))
}

#[utoipa::path(
    post,
    path = "/compute/workflows/{workflow_id}/versions",
    tag = "compute/workflows",
    summary = "Add a workflow version",
    description = r#"Adds the next version to a workflow the caller may write, from exactly one source.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- Takes the sources of a new workflow; a composed workflow takes only `uwe_yaml`, and its
  versions keep their language.
- The version is checked, inspected and stored like the first one.

**Errors**
- 503 when no coordinator answers."#,
    params(("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID")),
    request_body(content = CreateVersionRequest, example = json!({
        "uwe_yaml": "uwe_version: \"1\"\nsteps:\n  first:\n    tool: reverse\n",
        "message": "Adds a second step"
    })),
    responses(
        (status = 201, description = "The new version", body = WorkflowCreated, example = json!({
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "2", "commit": null,
            "digest": "sha256:5f2b8c0a6e0d3f1b9c4d7e8a1b2c3d4e5f60718293a4b5c6d7e8f90123456789"
        })),
        (status = 400, description = "An invalid source, or one of another language", body = ErrorResponse),
        (status = 403, description = "The caller sees the workflow but may not version it", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_version(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(workflow_id): Path<String>,
    Json(request): Json<CreateVersionRequest>,
) -> ServerResult<(StatusCode, Json<WorkflowCreated>)> {
    let workflow_id = ulid(&workflow_id, "workflow_id")?;
    let current = drive(
        aruna_ops_work::workflows::reads::GetWorkflowOperation::new(&auth, workflow_id),
        &state.get_ctx(),
    )
    .await
    .map_err(catalog_error)?;
    crate::features::require(
        &state,
        aruna_core::structs::identity::features::Feature::Workflows,
        Some(current.record.group_id),
    )
    .await?;
    let source = source_input(request.source)?;
    let add = AddVersionOperation::new(
        (&auth, state.get_node_id()),
        workflow_id,
        source,
        request.message,
    );
    let created = drive(add, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok((StatusCode::CREATED, Json(created_body(created))))
}

#[utoipa::path(
    get,
    path = "/compute/workflows/{workflow_id}/versions/{version}",
    tag = "compute/workflows",
    summary = "Read a workflow version",
    description = r#"Returns one version of a workflow the caller sees, with the text of a composed one.

**Authentication**: realm bearer token."#,
    params(
        ("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID"),
        ("version" = String, Path, description = "Version number, from 1")
    ),
    responses(
        (status = 200, description = "The version", body = WorkflowVersionBody, example = json!({
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1", "language": "uwe",
            "entrypoint": "workflow.uwe.yaml", "files": ["workflow.uwe.yaml", "workflow.wir.json"],
            "commit": null,
            "digest": "sha256:5f2b8c0a6e0d3f1b9c4d7e8a1b2c3d4e5f60718293a4b5c6d7e8f90123456789",
            "created_at": "2026-11-24T08:00:00+00:00", "created_by": "01K8W1A0000000000000000000",
            "message": null, "uwe_yaml": "uwe_version: \"1\"\nsteps:\n  first:\n    tool: reverse\n"
        })),
        (status = 400, description = "The workflow id is not a ULID", body = ErrorResponse),
        (status = 404, description = "Unknown version, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_version(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path((workflow_id, version)): Path<(String, String)>,
) -> ServerResult<Json<WorkflowVersionBody>> {
    let read = GetVersionOperation::new(
        &auth,
        ulid(&workflow_id, "workflow_id")?,
        version_number(&version)?,
    );
    let view = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(view.version.into()))
}

async fn facet(
    state: &ServerState,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    (workflow_id, version): (String, String),
    facet: Facet,
) -> ServerResult<FacetValue> {
    let workflow_id = ulid(&workflow_id, "workflow_id")?;
    let inspect = InspectOperation::new(auth, workflow_id, version_number(&version)?, facet);
    drive(inspect, &state.get_ctx())
        .await
        .map_err(catalog_error)
}

fn other_facet() -> ServerError {
    ServerError::InternalError("the registry answered another facet".into())
}

#[utoipa::path(
    get,
    path = "/compute/workflows/{workflow_id}/versions/{version}/check",
    tag = "compute/workflows",
    summary = "Read the check of a version",
    description = r#"Returns what uwe can run of a version: refused parts, problems and notes.

**Authentication**: realm bearer token.

**Behavior**
- The check is stored with the version; a version without one is checked once and stored.
- `ok` is false when a part or feature is not supported."#,
    params(
        ("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID"),
        ("version" = String, Path, description = "Version number, from 1")
    ),
    responses(
        (status = 200, description = "The check", body = WorkflowCheck, example = json!({
            "ok": false,
            "diagnostics": [{"severity": "error", "message": "step second: no tool shout", "node": "second"},
                {"severity": "warning", "message": "first/out->second/in: the formats may not fit",
                    "node": "second", "edge": "first/out->second/in"}]
        })),
        (status = 400, description = "The workflow id is not a ULID", body = ErrorResponse),
        (status = 404, description = "Unknown version, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_check(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(path): Path<(String, String)>,
) -> ServerResult<Json<WorkflowCheck>> {
    match facet(&state, &auth, path, Facet::Check).await? {
        FacetValue::Check(report) => Ok(Json(report.into())),
        _ => Err(other_facet()),
    }
}

#[utoipa::path(
    get,
    path = "/compute/workflows/{workflow_id}/versions/{version}/schema",
    tag = "compute/workflows",
    summary = "Read the input schema of a version",
    description = r#"Returns the JSON Schema of the inputs of a version, as uwe's form describes them.

**Authentication**: realm bearer token.

**Behavior**
- The schema is stored with the version; a version without one is inspected once and stored."#,
    params(
        ("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID"),
        ("version" = String, Path, description = "Version number, from 1")
    ),
    responses(
        (status = 200, description = "The JSON Schema of the inputs", body = Object, example = json!({
            "type": "object", "required": ["message"],
            "properties": {"message": {"type": "string", "default": "Hello"}}
        })),
        (status = 400, description = "The workflow id is not a ULID, or the inputs cannot be read", body = ErrorResponse),
        (status = 404, description = "Unknown version, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_schema(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(path): Path<(String, String)>,
) -> ServerResult<Json<Value>> {
    match facet(&state, &auth, path, Facet::Schema).await? {
        FacetValue::Schema(schema) => Ok(Json(schema)),
        _ => Err(other_facet()),
    }
}

#[utoipa::path(
    get,
    path = "/compute/workflows/{workflow_id}/versions/{version}/graph",
    tag = "compute/workflows",
    summary = "Read the graph of a version",
    description = r#"Returns the step graph of a version as nodes and edges between their ports.

**Authentication**: realm bearer token.

**Behavior**
- A composed workflow gives its whole graph; another workflow gives the steps a simulated run
  with default values reaches, so `complete` is false.
- The graph is stored with the version; a version without one is inspected once and stored."#,
    params(
        ("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID"),
        ("version" = String, Path, description = "Version number, from 1")
    ),
    responses(
        (status = 200, description = "The graph", body = WorkflowGraphBody, example = json!({
            "nodes": [{"id": "input:text", "kind": "input", "label": "text"},
                {"id": "first", "kind": "step", "label": "first", "tool": "reverse", "version": "1"}],
            "edges": [{"from": {"node": "input:text", "port": "text"}, "to": {"node": "first", "port": "text"}}],
            "complete": true
        })),
        (status = 400, description = "The workflow id is not a ULID, or the workflow cannot be read", body = ErrorResponse),
        (status = 404, description = "Unknown version, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_graph(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(path): Path<(String, String)>,
) -> ServerResult<Json<WorkflowGraphBody>> {
    match facet(&state, &auth, path, Facet::Graph).await? {
        FacetValue::Graph(graph) => Ok(Json(graph.into())),
        _ => Err(other_facet()),
    }
}

#[utoipa::path(
    get,
    path = "/compute/workflows/{workflow_id}/versions/{version}/tools",
    tag = "compute/workflows",
    summary = "Read the tools of a version",
    description = r#"Returns the tool definitions found in the steps of a version.

**Authentication**: realm bearer token.

**Behavior**
- The tools are extracted once and stored with the version.
- They join no catalog; `POST /compute/tools/extract` with `save` adds them to one."#,
    params(
        ("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID"),
        ("version" = String, Path, description = "Version number, from 1")
    ),
    responses(
        (status = 200, description = "The tools", body = ToolList, example = json!({
            "tools": [{"tool_id": "reverse", "version": "1", "title": "Reverse", "description": null,
                "language": "argo", "group_id": null, "curation": "unchecked", "image": null,
                "workflow": false, "inputs": [{"name": "text", "kind": "file"}],
                "outputs": [{"name": "reversed", "kind": "file"}], "parameters": null, "operations": []}]
        })),
        (status = 400, description = "The workflow id is not a ULID, or the workflow cannot be read", body = ErrorResponse),
        (status = 404, description = "Unknown version, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_tools(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(path): Path<(String, String)>,
) -> ServerResult<Json<ToolList>> {
    match facet(&state, &auth, path, Facet::Tools).await? {
        FacetValue::Tools(tools) => Ok(Json(ToolList {
            tools: tools.iter().map(ToolSummary::of_definition).collect(),
        })),
        _ => Err(other_facet()),
    }
}

#[utoipa::path(
    post,
    path = "/compute/workflows/{workflow_id}/versions/{version}/values/validate",
    tag = "compute/workflows",
    summary = "Check input values",
    description = r#"Checks input values against the form of a version, with uwe's diagnostics per value.

**Authentication**: realm bearer token.

**Behavior**
- Nothing is stored and no task starts.
- Each diagnostic names the value by `instance_path` and, for a missing one, by `input`.

**Errors**
- 503 when no coordinator answers."#,
    params(
        ("workflow_id" = String, Path, description = "Workflow id, as a 26-character ULID"),
        ("version" = String, Path, description = "Version number, from 1")
    ),
    request_body(content = ValuesRequest, example = json!({"values": {"message": "Hola"}})),
    responses(
        (status = 200, description = "Whether the values fit", body = ValuesCheck, example = json!({
            "ok": false,
            "diagnostics": [{"message": "message is required", "instance_path": "", "keyword": "required", "input": "message"}]
        })),
        (status = 400, description = "The workflow id is not a ULID, or the form cannot be read", body = ErrorResponse),
        (status = 404, description = "Unknown version, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn validate_values(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path((workflow_id, version)): Path<(String, String)>,
    Json(request): Json<ValuesRequest>,
) -> ServerResult<Json<ValuesCheck>> {
    let workflow_id = ulid(&workflow_id, "workflow_id")?;
    let check = ValidateValuesOperation::new(
        &auth,
        workflow_id,
        version_number(&version)?,
        request.values,
    );
    let report = drive(check, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(Json(ValuesCheck {
        ok: report.ok,
        diagnostics: report.diagnostics,
    }))
}
