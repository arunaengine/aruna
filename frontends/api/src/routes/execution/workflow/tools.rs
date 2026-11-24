//! Serves the tool catalog: list, add, read and remove tool versions, extract them from
//! workflows and suggest tools whose ports fit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::workflows::tools::{
    CreateToolOperation, DeleteToolOperation, ExtractSource, ExtractToolsOperation,
    GetToolOperation, ListToolsOperation, PortQuery, SuggestToolsOperation, ToolFilter,
};
use aruna_runner::driver::drive;
use aruna_workflow_contract::preflight::SuggestDirection;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde_json::json;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{
    CreateToolRequest, ExtractRequest, ExtractResponse, ExtractSourceBody, SourceFileBody,
    SuggestRequest, SuggestResponse, SuggestWay, ToolDetail, ToolPage, ToolQuery, ToolSuggestion,
    ToolSummary, WorkflowDiagnostic,
};
use super::{catalog_error, git_input, limit, ulid, version_number};
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

/// Suggestions of one request when the caller names no limit.
const DEFAULT_SUGGESTIONS: u32 = 20;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_tools, create_tool))
        .routes(routes!(get_tool, delete_tool))
        .routes(routes!(extract_tools))
        .routes(routes!(suggest_tools))
}

#[utoipa::path(
    get,
    path = "/compute/tools",
    tag = "compute/tools",
    summary = "List catalog tools",
    description = r#"Lists the tool versions of the group catalogs the caller may read.

**Authentication**: realm bearer token.

**Behavior**
- Without `group_id` the list holds the tools of every group the caller may read.
- `format` keeps tools with a port that takes or gives the EDAM format.
- `next_cursor` continues the list; `total` counts every match."#,
    params(ToolQuery),
    responses(
        (status = 200, description = "One page of tools", body = ToolPage, example = json!({
            "items": [{"tool_id": "reverse", "version": "1", "title": "Reverse", "description": null,
                "language": "argo", "group_id": "01JABCDEF0123456789ABCDEFG", "curation": "unchecked",
                "image": "debian:stable-slim", "workflow": false,
                "inputs": [{"name": "text", "kind": "file"}], "outputs": [{"name": "reversed", "kind": "file"}],
                "parameters": null, "operations": []}],
            "next_cursor": null, "total": 1
        })),
        (status = 400, description = "A malformed group id or cursor", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_tools(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<ToolQuery>,
) -> ServerResult<Json<ToolPage>> {
    let filter = ToolFilter {
        group_id: query
            .group_id
            .as_deref()
            .map(|group| ulid(group, "group_id"))
            .transpose()?,
        language: query.language,
        format: query.format,
        curation: query.curation.map(Into::into),
        text: query.q,
    };
    let after = match query.cursor.as_deref() {
        Some(cursor) => Some(
            aruna_core::cursor::decode(cursor)
                .map_err(|_| ServerError::BadRequestReason("cursor is no tool cursor".into()))?,
        ),
        None => None,
    };
    let list = ListToolsOperation::new(&auth, filter, after.map(Into::into), limit(query.limit));
    let page = drive(list, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(ToolPage {
        items: page.items.iter().map(ToolSummary::from).collect(),
        next_cursor: page.next.map(aruna_core::cursor::encode),
        total: Some(page.total as u64),
    }))
}

#[utoipa::path(
    post,
    path = "/compute/tools",
    tag = "compute/tools",
    summary = "Add a catalog tool",
    description = r#"Adds a tool definition version to the catalog of a group the caller may write.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- The definition is uwe's tool definition; it needs `id`, `version` and `title`.
- Another group may hold the same tool version in its own catalog.

**Errors**
- 409 when the group's catalog holds this tool version already."#,
    request_body(content = CreateToolRequest, example = json!({
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "definition": {"id": "reverse", "version": "1", "title": "Reverse",
            "image": {"name": "debian:stable-slim"},
            "inputs": [{"name": "text", "kind": "file"}], "outputs": [{"name": "reversed", "kind": "file"}]}
    })),
    responses(
        (status = 201, description = "The stored tool", body = ToolSummary, example = json!({
            "tool_id": "reverse", "version": "1", "title": "Reverse", "description": null,
            "language": null, "group_id": "01JABCDEF0123456789ABCDEFG", "curation": "unchecked",
            "image": "debian:stable-slim", "workflow": false,
            "inputs": [{"name": "text", "kind": "file"}], "outputs": [{"name": "reversed", "kind": "file"}],
            "parameters": null, "operations": []
        })),
        (status = 400, description = "A definition without id, version or title, or a malformed group id", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's workflows", body = ErrorResponse),
        (status = 409, description = "The group holds this tool version already", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_tool(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Json(request): Json<CreateToolRequest>,
) -> ServerResult<(StatusCode, Json<ToolSummary>)> {
    let group_id = ulid(&request.group_id, "group_id")?;
    let create = CreateToolOperation::new(&auth, group_id, request.definition);
    let tool = drive(create, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok((StatusCode::CREATED, Json(ToolSummary::from(&tool))))
}

#[utoipa::path(
    get,
    path = "/compute/tools/{tool_id}/versions/{version}",
    tag = "compute/tools",
    summary = "Read a catalog tool",
    description = r#"Returns one tool version with its full definition.

**Authentication**: realm bearer token.

**Behavior**
- When several groups hold the tool version, the first readable group in id order answers."#,
    params(
        ("tool_id" = String, Path, description = "Tool id, as its definition names it"),
        ("version" = String, Path, description = "Tool version, as its definition names it")
    ),
    responses(
        (status = 200, description = "The tool", body = ToolDetail, example = json!({
            "tool_id": "reverse", "version": "1", "title": "Reverse", "description": null,
            "language": "argo", "group_id": "01JABCDEF0123456789ABCDEFG", "curation": "unchecked",
            "image": null, "workflow": false,
            "inputs": [{"name": "text", "kind": "file"}], "outputs": [{"name": "reversed", "kind": "file"}],
            "parameters": null, "operations": [],
            "definition": {"id": "reverse", "version": "1", "title": "Reverse",
                "source": {"kind": "argo", "address": "reverse/1/tools/reverse.yaml", "revision": "r1"}}
        })),
        (status = 404, description = "Unknown, or in no catalog the caller reads", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_tool(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path((tool_id, version)): Path<(String, String)>,
) -> ServerResult<Json<ToolDetail>> {
    let tool = drive(
        GetToolOperation::new(&auth, &tool_id, &version),
        &state.get_ctx(),
    )
    .await
    .map_err(catalog_error)?;
    Ok(Json(ToolDetail::from(&tool)))
}

#[utoipa::path(
    delete,
    path = "/compute/tools/{tool_id}/versions/{version}",
    tag = "compute/tools",
    summary = "Remove a catalog tool",
    description = r#"Removes a tool version from every group catalog the caller may write.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- Copies in catalogs the caller only reads stay.
- A second delete answers 404."#,
    params(
        ("tool_id" = String, Path, description = "Tool id, as its definition names it"),
        ("version" = String, Path, description = "Tool version, as its definition names it")
    ),
    responses(
        (status = 204, description = "The tool version is removed"),
        (status = 403, description = "The caller reads the tool but may write none of its catalogs", body = ErrorResponse),
        (status = 404, description = "Unknown, or in no catalog the caller reads", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_tool(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path((tool_id, version)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    drive(
        DeleteToolOperation::new(&auth, &tool_id, &version),
        &state.get_ctx(),
    )
    .await
    .map_err(catalog_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/compute/tools/extract",
    tag = "compute/tools",
    summary = "Extract tools from a workflow",
    description = r#"Extracts the tool definitions of a stored version, of uploaded files or of Git.

**Authentication**: realm bearer token with READ on the group's workflows; WRITE with `save`.

**Behavior**
- With `save` the tools join the group's catalog with the file their source names, below
  `<tool id>/<version>/`; a tool version saved before is replaced.
- A workflow the coordinator cannot read answers 200 with no tools and the reason.

**Errors**
- 400 for a Git source, which this node cannot fetch yet.
- 503 when no coordinator answers."#,
    request_body(content = ExtractRequest, example = json!({
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "source": {"workflow": {"workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1"}},
        "save": true
    })),
    responses(
        (status = 200, description = "The tools and what the extraction found", body = ExtractResponse, example = json!({
            "tools": [{"tool_id": "reverse", "version": "1", "title": "Reverse", "description": null,
                "language": "argo", "group_id": "01JABCDEF0123456789ABCDEFG", "curation": "unchecked",
                "image": null, "workflow": false, "inputs": [{"name": "text", "kind": "file"}],
                "outputs": [{"name": "reversed", "kind": "file"}], "parameters": null, "operations": []}],
            "diagnostics": [{"severity": "info", "message": "one tool found"}]
        })),
        (status = 400, description = "A malformed id, invalid files or a Git source", body = ErrorResponse),
        (status = 403, description = "No READ, or for `save` no WRITE, on the group's workflows", body = ErrorResponse),
        (status = 404, description = "The workflow version is unknown or not visible", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn extract_tools(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Json(request): Json<ExtractRequest>,
) -> ServerResult<Json<ExtractResponse>> {
    let group_id = ulid(&request.group_id, "group_id")?;
    let source = match request.source {
        ExtractSourceBody::Workflow { workflow } => ExtractSource::Workflow {
            workflow_id: ulid(&workflow.workflow_id, "workflow_id")?,
            version: version_number(&workflow.version)?,
        },
        ExtractSourceBody::Files {
            files,
            entrypoint,
            language,
        } => ExtractSource::Files {
            files: (files.into_iter().map(SourceFileBody::decoded)).collect::<Result<_, _>>()?,
            entrypoint,
            language: language.into(),
        },
        ExtractSourceBody::Git { git, .. } => ExtractSource::Git(git_input(git)),
    };
    let extract = ExtractToolsOperation::new(&auth, group_id, source, request.save);
    let report = drive(extract, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(Json(ExtractResponse {
        tools: report.tools.iter().map(ToolSummary::from).collect(),
        diagnostics: report
            .diagnostics
            .iter()
            .map(WorkflowDiagnostic::from)
            .collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/compute/tools/suggest",
    tag = "compute/tools",
    summary = "Suggest fitting catalog tools",
    description = r#"Suggests catalog tools whose ports fit a given port, best first.

**Authentication**: realm bearer token; with `group_id`, READ on the group's workflows.

**Behavior**
- `next` finds tool inputs that take what the described output gives; `previous` finds tool
  outputs that can feed the described input.
- Without `group_id` the tools of every group the caller may read are asked.
- `fit` is `fits` or `unknown`; tools that do not fit are left out.

**Errors**
- 503 when no coordinator answers."#,
    request_body(content = SuggestRequest, example = json!({
        "group_id": "01JABCDEF0123456789ABCDEFG", "direction": "next", "kind": "file",
        "cardinality": "one", "formats": ["http://edamontology.org/format_1930"], "limit": 5
    })),
    responses(
        (status = 200, description = "The fitting tools", body = SuggestResponse, example = json!({
            "suggestions": [{"port": "text", "fit": "fits", "scatter": false,
                "tool": {"tool_id": "reverse", "version": "1", "title": "Reverse", "description": null,
                    "language": "argo", "group_id": "01JABCDEF0123456789ABCDEFG", "curation": "unchecked",
                    "image": null, "workflow": false, "inputs": [{"name": "text", "kind": "file"}],
                    "outputs": [{"name": "reversed", "kind": "file"}], "parameters": null, "operations": []}}]
        })),
        (status = 400, description = "A malformed group id or port", body = ErrorResponse),
        (status = 403, description = "No READ on the group's workflows", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn suggest_tools(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Json(request): Json<SuggestRequest>,
) -> ServerResult<Json<SuggestResponse>> {
    let group_id = request
        .group_id
        .as_deref()
        .map(|group| ulid(group, "group_id"))
        .transpose()?;
    let query = PortQuery {
        direction: match request.direction {
            SuggestWay::Next => SuggestDirection::Followers,
            SuggestWay::Previous => SuggestDirection::Producers,
        },
        kind: request.kind,
        cardinality: request.cardinality.unwrap_or_else(|| json!("one")),
        formats: request.formats,
        conditions: request.conditions,
        limit: request.limit.unwrap_or(DEFAULT_SUGGESTIONS).clamp(1, 500),
    };
    let suggest = SuggestToolsOperation::new(&auth, group_id, query);
    let found = drive(suggest, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    let suggestions = (found.iter())
        .map(|suggestion| ToolSuggestion {
            tool: ToolSummary::from(&suggestion.tool),
            port: suggestion.port.clone(),
            fit: suggestion.fit.clone(),
            scatter: suggestion.scatter,
        })
        .collect();
    Ok(Json(SuggestResponse { suggestions }))
}
