//! Lists what each repository kind can do and needs, and checks a dataset against a repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::repository::capabilities;
use aruna_core::repository::rules::{Mapped, rules};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::jobs::repository::requirement_profiles;
use aruna_operations::metadata::builtin_shapes;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use super::repository_links::{metadata_json, parse_ulid, readable, requirements};
use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ProfileFindingResponse, ServerError, ServerResult};
use crate::metadata::ensure_metadata_scope;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_kinds))
        .routes(routes!(check_repository))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CapabilitiesResponse {
    pub drafts: bool,
    pub reserve_identifier: bool,
    pub versions: bool,
    pub review: bool,
    pub pull: bool,
    pub search: bool,
    pub release_date: bool,
    /// The identifier a published record receives, such as doi.
    pub identifier_kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KindProfileResponse {
    pub iri: String,
    pub name: String,
    /// The Profile's SHACL shapes as Turtle sources.
    pub shapes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RepositoryKindResponse {
    /// The connector kind, such as invenio.
    pub kind: String,
    pub capabilities: CapabilitiesResponse,
    pub profiles: Vec<KindProfileResponse>,
    /// The mapping rule targets: which crate entities become which repository objects.
    pub targets: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CheckRequest {
    pub group_id: String,
    pub connector_id: String,
    /// Native repository fields a link or export would add; they never satisfy a requirement.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CheckProfileResponse {
    pub iri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MappingResponse {
    /// The crate entity; the root is ./.
    pub entity_id: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

impl From<Mapped> for MappingResponse {
    fn from(mapped: Mapped) -> Self {
        Self {
            entity_id: mapped.entity_id,
            target: mapped.target,
            group: mapped.group,
            field: mapped.field,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CheckResponse {
    pub kind: String,
    pub profile: CheckProfileResponse,
    /// No finding is a violation.
    pub ready: bool,
    pub findings: Vec<ProfileFindingResponse>,
    pub mapping: Vec<MappingResponse>,
}

fn kind_view(kind: RepositoryConnectorKind) -> ServerResult<Option<RepositoryKindResponse>> {
    let Some(can) = capabilities(kind) else {
        return Ok(None);
    };
    let targets = match rules(kind) {
        Ok(Some(rules)) => serde_json::to_value(&rules.targets)
            .map_err(|error| ServerError::InternalError(error.to_string()))?,
        Ok(None) => serde_json::json!([]),
        Err(error) => return Err(ServerError::InternalError(error.to_string())),
    };
    let profiles = requirement_profiles(kind)
        .iter()
        .map(|(iri, name)| KindProfileResponse {
            iri: (*iri).to_string(),
            name: (*name).to_string(),
            shapes: builtin_shapes(iri)
                .unwrap_or_default()
                .iter()
                .map(|shapes| (*shapes).to_string())
                .collect(),
        })
        .collect();
    Ok(Some(RepositoryKindResponse {
        kind: kind.as_str().to_string(),
        capabilities: CapabilitiesResponse {
            drafts: can.drafts,
            reserve_identifier: can.reserve_identifier,
            versions: can.versions,
            review: can.review,
            pull: can.pull,
            search: can.search,
            release_date: can.release_date,
            identifier_kind: can.identifier_kind.to_string(),
        },
        profiles,
        targets,
    }))
}

#[utoipa::path(
    get, path = "/metadata/repository/kinds", tag = "metadata/repository",
    summary = "List the repository kinds that publish datasets",
    description = r#"Lists every repository kind this node can publish to, with what it can do and what it needs.

**Authentication**

Requires authentication.

**Behavior**

Returns a bare array. capabilities names the supported actions; other actions answer 400 with code not_supported. profiles lists the built-in requirement Profiles with their SHACL shapes as Turtle sources. A crate can name one of them in conformsTo to be validated like any Profile. targets are the mapping rules: which crate entities become which repository objects and fields.

Kinds that only harvest, such as oai_pmh, are not listed."#,
    responses(
        (status = 200, description = "Repository kinds", body = Vec<RepositoryKindResponse>, example = json!([{
            "kind": "invenio",
            "capabilities": {"drafts": true, "reserve_identifier": true, "versions": true,
                "review": true, "pull": true, "search": true, "release_date": false,
                "identifier_kind": "doi"},
            "profiles": [{"iri": "https://w3id.org/aruna/profiles/repository/zenodo",
                "name": "Zenodo record", "shapes": ["@prefix sh: <http://www.w3.org/ns/shacl#> ."]}],
            "targets": [{"name": "record", "select": {"root": true}, "min": 1}]
        }])),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn list_kinds(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<Vec<RepositoryKindResponse>>> {
    require_unrestricted_auth(&state, auth)?;
    let mut kinds = Vec::new();
    for kind in [
        RepositoryConnectorKind::Invenio,
        RepositoryConnectorKind::OaiPmh,
    ] {
        kinds.extend(kind_view(kind)?);
    }
    Ok(Json(kinds))
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/repository/check", tag = "metadata/repository",
    summary = "Check a dataset against a repository",
    description = r#"Checks the dataset crate against the requirement Profile and mapping rules of the connector's repository. Nothing is stored or sent.

**Authentication**

Requires READ on the dataset and READ on the metadata path of the connector group.

**Behavior**

profile names the requirement Profile the repository needs; it is one of the Profiles the kinds route lists. findings uses the Profile validation format. ready is true when no finding is a violation; warnings, such as a missing license, do not block. mapping says what each crate entity becomes.

Only the crate counts: metadata is checked to be an object but never satisfies a requirement.

**Errors**

Invalid input returns 400, with code not_supported for a repository kind that cannot publish. Denied access returns 403; an unknown dataset or connector returns 404."#,
    params(("document_id" = String, Path, description = "Metadata document identifier")),
    request_body(content = CheckRequest, example = json!({
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW"
    })),
    responses(
        (status = 200, description = "The check result", body = CheckResponse, example = json!({
            "kind": "invenio",
            "profile": {"iri": "https://w3id.org/aruna/profiles/repository/zenodo", "revision": "builtin"},
            "ready": false,
            "findings": [{"code": "constraint_violation", "severity": "violation", "focus_node": "./",
                "path": "(<http://schema.org/author> | <http://schema.org/creator>)",
                "rule": "http://www.w3.org/ns/shacl#minCount",
                "message": "The dataset needs creators; each person needs a name or family name and each organization a name.",
                "profile_revision": "builtin", "completeness": "complete"}],
            "mapping": [{"entity_id": "./", "target": "record"},
                {"entity_id": "data.csv", "target": "file"}]
        })),
        (status = 400, description = "Invalid input, or a repository kind that cannot publish (code not_supported)", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Dataset or connector access denied", body = ErrorResponse),
        (status = 404, description = "Dataset or repository connector not found", body = ErrorResponse),
        (status = 503, description = "The requirements could not be checked", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn check_repository(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Json(request): Json<CheckRequest>,
) -> ServerResult<Json<CheckResponse>> {
    let (auth, document_id) = readable(&state, auth, &document_id).await?;
    let group_id = parse_ulid(&request.group_id)?;
    let connector_id = parse_ulid(&request.connector_id)?;
    metadata_json(&state, request.metadata)?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::READ).await?;
    let checked = Box::pin(requirements(
        &state,
        &auth,
        document_id,
        group_id,
        connector_id,
    ))
    .await?;
    Ok(Json(CheckResponse {
        kind: checked.kind.as_str().to_string(),
        profile: CheckProfileResponse {
            iri: checked.profile_iri.to_string(),
            revision: checked.profile_revision,
        },
        ready: checked.ready,
        findings: checked.findings.into_iter().map(Into::into).collect(),
        mapping: checked.mapping.into_iter().map(Into::into).collect(),
    }))
}

#[cfg(test)]
#[path = "repository_check_tests.rs"]
mod tests;
