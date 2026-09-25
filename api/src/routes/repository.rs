//! Native repository transfer endpoints backed by durable crate jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::jobs::repository::Action;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{RepositoryExportRequest, SubmitExportRequest, SubmitExportResponse};
use crate::server::state::ServerState;

use super::rocrate_import::{
    ImportMetadataRequest, ImportSourceRequest, ImportTargetRequest, SubmitImportRequest,
    SubmitImportResponse,
};

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(import_record))
        .routes(routes!(export_record))
        .routes(routes!(search_records))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct RepositorySearch {
    /// Native repository query; an empty query lists records.
    #[serde(default)]
    pub q: String,
    /// Page number starting at 1.
    #[serde(default = "first_page")]
    pub page: u32,
    /// Number of results, from 1 through 25.
    #[serde(default = "page_size")]
    pub size: u8,
    /// Include older published versions.
    #[serde(default)]
    pub all_versions: bool,
}

fn first_page() -> u32 {
    1
}
fn page_size() -> u8 {
    25
}

#[utoipa::path(
    get, path = "/metadata/groups/{group_id}/repositories/{connector_id}/records",
    tag = "metadata/repository",
    summary = "Search published repository records",
    description = r#"Searches one page of published records in the configured repository.

**Authentication**

Requires authentication and READ on the metadata path of the repository connector group.

**Behavior**

Returns native hits, totals and links. Set all_versions to include older published versions. No data is imported.

**Limits**

Page starts at 1; size is 1 to 25 and query text is at most 4096 bytes. Repository result windows and concurrent changes can limit enumeration.

**Errors**

Invalid queries return 400, with code not_supported for a repository kind without search. Denied access returns 403, an unknown connector 404 and repository availability failures 503."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the repository connector"),
        ("connector_id" = String, Path, description = "Repository connector of the group"),
        RepositorySearch
    ),
    responses(
        (status = 200, description = "Native repository search page", body = serde_json::Value, example = json!({
            "hits": {"total": 1, "hits": [{"id": "1234567", "metadata": {"title": "Example dataset", "publication_date": "2026-09-22", "resource_type": {"id": "dataset"}, "creators": [{"person_or_org": {"type": "personal", "family_name": "Researcher"}}]}}]},
            "links": {"next": null}
        })),
        (status = 400, description = "Invalid query or repository response", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Connector access denied", body = ErrorResponse),
        (status = 404, description = "Connector not found", body = ErrorResponse),
        (status = 503, description = "Repository unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn search_records(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
    Query(query): Query<RepositorySearch>,
) -> ServerResult<Json<serde_json::Value>> {
    let auth = crate::auth::require_unrestricted_auth(&state, auth)?;
    let query = aruna_core::repository::RepositoryQuery {
        group_id: ulid::Ulid::from_string(&group_id).map_err(|_| ServerError::BadRequest)?,
        connector_id: ulid::Ulid::from_string(&connector_id)
            .map_err(|_| ServerError::BadRequest)?,
        q: query.q,
        page: query.page,
        size: query.size,
        all_versions: query.all_versions,
    };
    crate::metadata::ensure_metadata_scope(
        &state,
        &auth,
        query.group_id,
        aruna_core::structs::identity::auth::Permission::READ,
    )
    .await?;
    let kind =
        super::repository_links::connector_kind(&state, query.group_id, query.connector_id).await?;
    super::repository_links::ensure_capable(kind, Action::Search)?;
    aruna_operations::jobs::repository::search(
        kind,
        &state.get_ctx(),
        &auth,
        &query,
        state.rocrate_limits().metadata_bytes,
    )
    .await
    .map(Json)
    .map_err(|error| match error {
        aruna_operations::jobs::repository::TransferError::Permanent(message) => {
            ServerError::BadRequestReason(message)
        }
        error @ aruna_operations::jobs::repository::TransferError::Refused(_) => {
            ServerError::BadRequestReason(error.to_string())
        }
        _ => ServerError::ServiceUnavailableReason("repository search unavailable".into()),
    })
}

/// Names the record by exactly one of record_id, doi or url.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RepositoryImportRequest {
    pub group_id: String,
    pub connector_id: String,
    #[serde(default)]
    pub record_id: Option<String>,
    /// A version DOI names that version; a concept DOI names the latest version.
    #[serde(default)]
    pub doi: Option<String>,
    /// A record page or API URL on the connector's repository.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(flatten)]
    pub options: ImportOptionsRequest,
    /// Creates a pull link that keeps the new dataset updated from the record lineage.
    #[serde(default)]
    pub keep_updated: bool,
    /// With keep_updated, imports new versions without asking; default false.
    #[serde(default)]
    pub auto_update: Option<bool>,
    pub target: ImportTargetRequest,
    pub metadata: ImportMetadataRequest,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImportDataMode {
    #[default]
    Copy,
    Reference,
    Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ImportOptionsRequest {
    #[serde(default)]
    pub mode: ImportDataMode,
    #[serde(default = "include_versions")]
    pub all_versions: bool,
}

fn include_versions() -> bool {
    true
}

impl Default for ImportOptionsRequest {
    fn default() -> Self {
        Self {
            mode: ImportDataMode::Copy,
            all_versions: true,
        }
    }
}

impl From<ImportOptionsRequest> for aruna_core::repository::ImportOptions {
    fn from(value: ImportOptionsRequest) -> Self {
        use aruna_core::repository::ImportMode;
        Self {
            all_versions: value.all_versions,
            mode: match value.mode {
                ImportDataMode::Copy => ImportMode::Copy,
                ImportDataMode::Reference => ImportMode::Reference,
                ImportDataMode::Metadata => ImportMode::Metadata,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitRepositoryExport {
    pub repository: RepositoryExportRequest,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[utoipa::path(
    post, path = "/metadata/repository/imports", tag = "metadata/repository",
    summary = "Import a repository record",
    description = r#"Imports repository metadata and optional data through a durable crate job.

**Authentication**

Requires READ on the metadata path of the repository connector group and WRITE on destination data and metadata. keep_updated also requires WRITE on the connector group's metadata path, as managing a link does. The connector's kind must support imports; an Invenio connector's endpoint is the API root, such as https://zenodo.org/api/.

**Behavior**

Mode copy verifies and stores file bytes. Mode reference creates native object references and reads bytes on demand; target and connector must share a group. Mode metadata skips attached files. Source record JSON is retained in every mode.

Name the record by exactly one of record_id, doi or url. A version DOI selects that version and a concept DOI the latest one. A url is a record page or API URL on the connector's repository, such as https://zenodo.org/records/1234567.

All published versions are included by default. Set all_versions to false for the selected version. Source identifiers remain provenance. Partial dates use their earliest day for crate validation and retain their exact original value.

keep_updated creates a pull link on the new dataset once the import succeeded. It asks the repository once a day for a new version, reading with the connector's token if the connector has one. With auto_update new versions are imported into the dataset without asking; otherwise the link shows update_available and the pull route imports them.

**Limits**

Crate limits apply. Hidden edits and inaccessible or deleted versions cannot be recovered. References depend on repository availability and connector credentials.

**Errors**

None or several of record_id, doi and url, a record_id the kind does not accept, a DOI no published record has, a URL on another origin, or auto_update without keep_updated return 400.

A kind without imports, a doi or url on a kind without search, or keep_updated on a kind without pull links return 400 with code not_supported. An unknown connector returns 404. The returned job exposes progress, cancellation and failure details. Copy imports fail on missing data or checksum mismatches."#,
    request_body(content = RepositoryImportRequest, example = json!({
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "doi": "10.5281/zenodo.1234567", "target": {"bucket": "research", "prefix": "zenodo/1234567"},
        "metadata": {"group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "path": "datasets/zenodo", "public": false},
        "keep_updated": true, "auto_update": false
    })),
    responses(
        (status = 202, description = "Transfer accepted", body = SubmitImportResponse, example = json!({
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX", "created": true,
            "owner_node_url": "https://node.example/api/v1",
            "status_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "report_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX/report"
        })),
        (status = 400, description = "Invalid transfer request or record name, or no record with this DOI", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Connector or destination access denied", body = ErrorResponse),
        (status = 404, description = "Destination or repository connector not found", body = ErrorResponse),
        (status = 409, description = "Job conflict or quota refusal", body = ErrorResponse),
        (status = 503, description = "Transfer placement or repository unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn import_record(
    state: State<Arc<ServerState>>,
    auth: Extension<Option<AuthContext>>,
    Json(request): Json<RepositoryImportRequest>,
) -> ServerResult<(StatusCode, Json<SubmitImportResponse>)> {
    use aruna_operations::jobs::repository::{RecordReference, TransferError, resolve};
    let reference = match (request.record_id, request.doi, request.url) {
        (Some(id), None, None) => RecordReference::Id(id),
        (None, Some(doi), None) => RecordReference::Doi(doi),
        (None, None, Some(url)) => RecordReference::Url(url),
        _ => {
            return Err(ServerError::BadRequestReason(
                "give exactly one of record_id, doi or url".into(),
            ));
        }
    };
    let record_id = match reference {
        RecordReference::Id(id) => id,
        reference => {
            let caller = crate::auth::require_unrestricted_auth(&state, auth.0.clone())?;
            let group_id =
                ulid::Ulid::from_string(&request.group_id).map_err(|_| ServerError::BadRequest)?;
            let connector_id = ulid::Ulid::from_string(&request.connector_id)
                .map_err(|_| ServerError::BadRequest)?;
            crate::metadata::ensure_metadata_scope(
                &state,
                &caller,
                group_id,
                aruna_core::structs::identity::auth::Permission::READ,
            )
            .await?;
            let kind =
                super::repository_links::connector_kind(&state, group_id, connector_id).await?;
            super::repository_links::ensure_capable(kind, Action::Import)?;
            super::repository_links::ensure_capable(kind, Action::Search)?;
            resolve(
                kind,
                &state.get_ctx(),
                &caller,
                group_id,
                connector_id,
                &reference,
                state.rocrate_limits().metadata_bytes,
            )
            .await
            .map_err(|error| match error {
                TransferError::Permanent(message) => ServerError::BadRequestReason(message),
                error @ TransferError::Refused(_) => {
                    ServerError::BadRequestReason(error.to_string())
                }
                _ => ServerError::ServiceUnavailableReason("repository unavailable".into()),
            })?
        }
    };
    super::rocrate_import::submit_import(
        state,
        auth,
        Json(SubmitImportRequest {
            source: ImportSourceRequest::Repository {
                group_id: request.group_id,
                connector_id: request.connector_id,
                record_id,
                options: request.options,
                keep_updated: request.keep_updated,
                auto_update: request.auto_update,
            },
            target: request.target,
            metadata: request.metadata,
            idempotency_key: request.idempotency_key,
        }),
    )
    .await
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/repository/exports", tag = "metadata/repository",
    summary = "Export a dataset as a repository record",
    description = r#"Exports mapped crate metadata and individual files to a native repository record.

**Authentication**

Requires WRITE on the crate, WRITE on the metadata path of the repository connector group and the user's repository.access_token. The token is encrypted for the user, node, connector and endpoint, omitted from output, and never replaced by the connector token.

**Behavior**

Creates a draft or uses draft_id for recovery. Set published_id to a published record ID to continue its version lineage; a kind without versions refuses it. Native source metadata and custom fields survive import/export; repository.metadata overrides mapped fields. RO-Crate JSON remains a provenance file.

Publication and public_files both default to false; public_files also applies to existing drafts. New drafts reserve their DOI. With a connector community, publishing a first version submits it for review instead.

Draft metadata updates use revision preconditions; files and metadata are verified before and after publication. Results include record ID, parent ID, DOI, concept DOI, URL, publication and review state, and a warning when a check failed after publication.

**Limits**

Every referenced file must be readable; web data entities become references instead. A record holds at most 100 files. Reference imports fetch their bytes when exported. Repository vocabularies and publication permissions apply. The upstream publish endpoint has no atomic revision precondition.

**Errors**

A repository kind that cannot publish, or published_id on a kind without versions, returns 400 with code not_supported. draft_id and published_id must be record ids the kind accepts.

A crate that does not meet the repository's requirement Profile or mapping rules returns 400 with code requirements_unmet and at most 100 findings, violations first; omitted_findings counts the rest. repository.metadata does not satisfy them.

A record whose mapped fields still lack a required field after repository.metadata is applied, such as creators cleared by an override, fails the job before any repository write.

Incomplete files, conflicting revisions or rejected metadata fail the job. An ambiguous creation outcome requires inspecting the repository and supplying draft_id. Cancellation retains remote drafts."#,
    params(("document_id" = String, Path, description = "Aruna metadata document identifier")),
    request_body(content = SubmitRepositoryExport, example = json!({"repository": {
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "access_token": "<personal-access-token>", "publish": false
    }})),
    responses(
        (status = 202, description = "Transfer accepted", body = SubmitExportResponse, example = json!({
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX", "created": true,
            "owner_node_url": "https://node.example/api/v1",
            "status_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "report_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX/report",
            "artifact_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX/artifacts/rocrate"
        })),
        (status = 400, description = "Missing personal repository token, invalid metadata or draft identifier, a repository kind that cannot publish (code not_supported), or unmet repository requirements (code requirements_unmet with findings)", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Crate or connector access denied", body = ErrorResponse),
        (status = 404, description = "Crate or connector not found", body = ErrorResponse),
        (status = 409, description = "Job conflict or quota refusal", body = ErrorResponse),
        (status = 503, description = "Transfer placement unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn export_record(
    state: State<Arc<ServerState>>,
    auth: Extension<Option<AuthContext>>,
    path: Path<String>,
    Json(request): Json<SubmitRepositoryExport>,
) -> ServerResult<(StatusCode, Json<SubmitExportResponse>)> {
    super::metadata::rocrate::submit_rocrate_export(
        state,
        auth,
        path,
        Json(SubmitExportRequest {
            destination: Some(request.repository),
            idempotency_key: request.idempotency_key,
        }),
    )
    .await
}
