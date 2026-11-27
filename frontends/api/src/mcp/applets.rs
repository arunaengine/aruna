//! MCP tools that let an agent create an applet, edit, build and release its source, keep build
//! drafts, and ask for approval. Agents never approve: every version they make stays private.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};

use super::{JsonPayload, McpServer, request_auth, server_error};
use crate::auth::{RealmAuth, UnrestrictedAuth};
use crate::routes::applets::{builds, lifecycle, node, records, source, wire};

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CreateInput {
    pub group_id: String,
    pub name: String,
    /// One DNS label; the applet lives at `<slug>.<applet domain>`.
    pub slug: String,
    /// A metadata document of the group whose Git repository holds the source.
    pub repository: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct AppletInput {
    pub applet_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SourceInput {
    pub applet_id: String,
    /// A branch, tag or commit; main when absent.
    pub revision: Option<String>,
    /// A folder to list; the whole tree when absent.
    pub root: Option<String>,
    /// Whether to return each file's text.
    #[serde(default)]
    pub contents: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CommitInput {
    pub applet_id: String,
    /// The main commit the change starts from, as applet_source answered it.
    pub base: String,
    pub message: String,
    /// Paths and their new UTF-8 text.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
    /// Paths to remove.
    #[serde(default)]
    pub removed: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReleaseInput {
    pub applet_id: String,
    pub revision: Option<String>,
    /// The folder with the built HTML, CSS and JavaScript and an index.html.
    pub root: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ApprovalInput {
    pub applet_id: String,
    pub version: u32,
    /// private, group_members or public.
    pub access: String,
    /// Exact https origins that may frame the applet.
    #[serde(default)]
    pub origins: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct DraftInput {
    pub applet_id: String,
    /// The build id applet_build or applet_drafts answered.
    pub build_id: String,
}

fn payload<T: Serialize>(value: T) -> Result<Json<JsonPayload>, CallToolResult> {
    let value = serde_json::to_value(value).map_err(super::internal_error)?;
    Ok(Json(JsonPayload(value)))
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::applet_router()
}

#[tool_router(router = applet_router)]
impl McpServer {
    #[tool(
        description = "Create a private applet, a web page of HTML, CSS and JavaScript the group publishes on Aruna, hosted on this node. Name a metadata document of the group as repository to keep the source in its Git repository. The answer holds the applet and the library catalogue to build with first. Nothing goes public without a person's approval.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn applet_create(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<CreateInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let request: wire::CreateAppletRequest =
            serde_json::from_value(serde_json::json!(input)).map_err(super::bad_request)?;
        let created = records::create_applet(
            State(self.state.clone()),
            UnrestrictedAuth(auth.clone()),
            axum::Json(request),
        )
        .await
        .map_err(server_error)?;
        let settings = node::applet_settings(
            State(self.state.clone()),
            RealmAuth(auth),
            Query(node::SettingsQuery { group_id: None }),
        )
        .await
        .map_err(server_error)?;
        let catalogue = settings.0.building.map(|building| building.catalogue);
        payload(serde_json::json!({ "applet": created.1.0, "catalogue": catalogue }))
    }

    #[tool(
        description = "Read one applet: lifecycle, access, versions, approved grants, hosts and any pending approval. The preview of the newest private version is served at the applet host to its creator and approvers only.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn applet_get(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<AppletInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let applet = records::get_applet(
            State(self.state.clone()),
            RealmAuth(auth),
            Path(input.applet_id),
        )
        .await
        .map_err(server_error)?;
        payload(applet.0)
    }

    #[tool(
        description = "List the files of the applet's source at a revision, optionally with their text. Use the returned commit as base for applet_commit.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn applet_source(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<SourceInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let query = source::SourceQuery {
            revision: input.revision,
            root: input.root,
            contents: input.contents,
        };
        let tree = source::source_files(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.applet_id),
            Query(query),
        )
        .await
        .map_err(server_error)?;
        let mut value = serde_json::to_value(tree.0).map_err(super::internal_error)?;
        // Agents read text: decode the base64 contents where they are UTF-8.
        if let Some(files) = value
            .get_mut("files")
            .and_then(|files| files.as_array_mut())
        {
            for file in files {
                let text = (file.get("content").and_then(|content| content.as_str()))
                    .and_then(|content| STANDARD.decode(content).ok())
                    .and_then(|bytes| String::from_utf8(bytes).ok());
                if let Some(text) = text {
                    file["content"] = serde_json::Value::String(text);
                }
            }
        }
        Ok(Json(JsonPayload(value)))
    }

    #[tool(
        description = "Commit file changes to the applet source's main branch; the node writes the commit, so no Git client is needed. base must be the current main commit. A commit makes no version; call applet_release afterwards.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn applet_commit(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<CommitInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let mut files: BTreeMap<String, Option<String>> = (input.files.into_iter())
            .map(|(path, text)| (path, Some(STANDARD.encode(text))))
            .collect();
        files.extend(input.removed.into_iter().map(|path| (path, None)));
        let body = source::CommitRequest {
            base: input.base,
            message: input.message,
            files,
        };
        let commit = source::commit_source(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.applet_id),
            axum::Json(body),
        )
        .await
        .map_err(server_error)?;
        payload(commit.0)
    }

    #[tool(
        description = "Release a folder of the applet source that already holds built HTML, CSS and JavaScript with an index.html as a new private version. The version is pinned and served only as a preview until a person approves it.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn applet_release(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ReleaseInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let body = source::ReleaseRequest {
            revision: input.revision,
            root: input.root,
        };
        let released = source::release_version(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.applet_id),
            axum::Json(body),
        )
        .await
        .map_err(server_error)?;
        payload(released.1.0)
    }

    #[tool(
        description = "Build a folder of the applet source in the node's curated builder, without network or credentials, into a draft that only the creator and approvers can preview. Drafts are limited in number and expire; applet_drafts shows the room left. Call applet_drafts until the build is no longer building, then capture it there.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn applet_build(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ReleaseInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let bearer = super::compute::request_bearer(&parts);
        let body = source::ReleaseRequest {
            revision: input.revision,
            root: input.root,
        };
        let started = builds::start_build(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            axum::Extension(bearer),
            Path(input.applet_id),
            axum::Json(body),
        )
        .await
        .map_err(server_error)?;
        payload(started.1.0)
    }

    #[tool(
        description = "List the applet's drafts with their state, expiry and the number of builds that may still start. A draft that finished building is captured here, so its state reads ready or failed.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn applet_drafts(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<AppletInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let listed = builds::list_drafts(
            State(self.state.clone()),
            UnrestrictedAuth(auth.clone()),
            Path(input.applet_id.clone()),
        )
        .await
        .map_err(server_error)?
        .0;
        let building = (listed.drafts.iter())
            .filter(|draft| draft.state == builds::DraftState::Building)
            .map(|draft| draft.build_id.clone())
            .collect::<Vec<_>>();
        // A finished build is captured on the way; one still running stays building.
        for build_id in building {
            let body = builds::CaptureRequest { build_id };
            let captured = builds::capture_draft(
                State(self.state.clone()),
                UnrestrictedAuth(auth.clone()),
                Path(input.applet_id.clone()),
                axum::Json(body),
            )
            .await;
            match captured {
                Ok(_) | Err(crate::error::ServerError::Conflict(_)) => {}
                Err(error) => return Err(server_error(error)),
            }
        }
        let listed = builds::list_drafts(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.applet_id),
        )
        .await
        .map_err(server_error)?;
        payload(listed.0)
    }

    #[tool(
        description = "Keep a ready draft as the next private version with exactly the files the build made, without building again. Ask for approval of that version afterwards.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn applet_keep_draft(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<DraftInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let kept = builds::keep_draft(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path((input.applet_id, input.build_id)),
        )
        .await
        .map_err(server_error)?;
        payload(kept.1.0)
    }

    #[tool(
        description = "Delete a draft to free its room. A running build keeps running until its job ends.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true
        )
    )]
    pub async fn applet_delete_draft(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<DraftInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        builds::delete_draft(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path((input.applet_id, input.build_id)),
        )
        .await
        .map_err(server_error)?;
        payload(serde_json::json!({ "deleted": true }))
    }

    #[tool(
        description = "Ask a person to approve a private version with the access and embedding origins it should go live with. Agents can only ask; a group member with the right to publish the files approves in the portal.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn applet_request_approval(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ApprovalInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let body: wire::ApprovalRequestBody = serde_json::from_value(serde_json::json!({
            "version": input.version, "access": input.access, "origins": input.origins
        }))
        .map_err(super::bad_request)?;
        let applet = lifecycle::request_approval(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.applet_id),
            axum::Json(body),
        )
        .await
        .map_err(server_error)?;
        payload(applet.0)
    }

    #[tool(
        description = "List the libraries this node's applet builder ships, with pinned version, role and license. Use them first; other packages may still be added and are listed on the approval card.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn applet_catalogue(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let settings = node::applet_settings(
            State(self.state.clone()),
            RealmAuth(auth),
            Query(node::SettingsQuery { group_id: None }),
        )
        .await
        .map_err(server_error)?;
        let catalogue = settings.0.building.map(|building| building.catalogue);
        payload(serde_json::json!({ "catalogue": catalogue }))
    }
}
