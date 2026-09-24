//! Native repository transfer endpoints backed by durable crate jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{InvenioExportRequest, SubmitExportRequest, SubmitExportResponse};
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
pub struct InvenioSearch {
    /// Group that owns the repository connector.
    pub group_id: String,
    /// Invenio repository connector of the group.
    pub connector_id: String,
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
    get, path = "/metadata/invenio/records", tag = "metadata/invenio",
    summary = "Search published Invenio or Zenodo records",
    description = r#"Searches one page of published records in the configured repository.

**Authentication**

Requires authentication and READ on the metadata path of the repository connector group.

**Behavior**

Returns native hits, totals and links. Set all_versions to include older published versions. No data is imported.

**Limits**

Page starts at 1; size is 1 to 25 and query text is at most 4096 bytes. Repository result windows and concurrent changes can limit enumeration.

**Errors**

Invalid queries return 400; denied access returns 403; repository availability failures return 503."#,
    params(InvenioSearch),
    responses(
        (status = 200, description = "Native repository search page", body = serde_json::Value, example = json!({
            "hits": {"total": 1, "hits": [{"id": "1234567", "metadata": {"title": "Example dataset", "publication_date": "2026-09-22", "resource_type": {"id": "dataset"}, "creators": [{"person_or_org": {"type": "personal", "family_name": "Researcher"}}]}}]},
            "links": {"next": null}
        })),
        (status = 400, description = "Invalid query or repository response", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Connector access denied", body = ErrorResponse),
        (status = 503, description = "Repository unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn search_records(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<InvenioSearch>,
) -> ServerResult<Json<serde_json::Value>> {
    let auth = crate::auth::require_unrestricted_auth(&state, auth)?;
    let query = aruna_core::invenio::InvenioQuery {
        group_id: ulid::Ulid::from_string(&query.group_id).map_err(|_| ServerError::BadRequest)?,
        connector_id: ulid::Ulid::from_string(&query.connector_id)
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
    aruna_operations::jobs::invenio::search_records(
        &state.get_ctx(),
        &auth,
        &query,
        state.rocrate_limits().metadata_bytes,
    )
    .await
    .map(Json)
    .map_err(|error| match error {
        aruna_operations::jobs::invenio::TransferError::Permanent(message) => {
            ServerError::BadRequestReason(message)
        }
        error @ aruna_operations::jobs::invenio::TransferError::Refused(_) => {
            ServerError::BadRequestReason(error.to_string())
        }
        _ => ServerError::ServiceUnavailableReason("repository search unavailable".into()),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InvenioImportRequest {
    pub group_id: String,
    pub connector_id: String,
    pub record_id: String,
    #[serde(flatten)]
    pub options: InvenioOptionsRequest,
    pub target: ImportTargetRequest,
    pub metadata: ImportMetadataRequest,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvenioDataMode {
    #[default]
    Copy,
    Reference,
    Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InvenioOptionsRequest {
    #[serde(default)]
    pub mode: InvenioDataMode,
    #[serde(default = "include_versions")]
    pub all_versions: bool,
}

fn include_versions() -> bool {
    true
}

impl Default for InvenioOptionsRequest {
    fn default() -> Self {
        Self {
            mode: InvenioDataMode::Copy,
            all_versions: true,
        }
    }
}

impl From<InvenioOptionsRequest> for aruna_core::invenio::InvenioOptions {
    fn from(value: InvenioOptionsRequest) -> Self {
        use aruna_core::invenio::InvenioMode;
        Self {
            all_versions: value.all_versions,
            mode: match value.mode {
                InvenioDataMode::Copy => InvenioMode::Copy,
                InvenioDataMode::Reference => InvenioMode::Reference,
                InvenioDataMode::Metadata => InvenioMode::Metadata,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitInvenioExport {
    pub repository: InvenioExportRequest,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[utoipa::path(
    post, path = "/metadata/invenio/imports", tag = "metadata/invenio",
    summary = "Import Invenio metadata and data",
    description = r#"Imports repository metadata and optional data through a durable crate job.

**Authentication**

Requires READ on the metadata path of the repository connector group and WRITE on destination data and metadata. The connector is an Invenio repository connector whose endpoint is the API root, such as https://zenodo.org/api/.

**Behavior**

Mode copy verifies and stores file bytes. Mode reference creates native object references and reads bytes on demand; target and connector must share a group. Mode metadata skips attached files. Source record JSON is retained in every mode.

All published versions are included by default. Set all_versions to false for the selected version. Source identifiers remain provenance. Partial dates use their earliest day for crate validation and retain their exact original value.

**Limits**

Crate limits apply. Hidden edits and inaccessible or deleted versions cannot be recovered. References depend on repository availability and connector credentials.

**Errors**

The returned job exposes progress, cancellation and failure details. Copy imports fail on missing data or checksum mismatches."#,
    request_body(content = InvenioImportRequest, example = json!({
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "record_id": "1234567", "target": {"bucket": "research", "prefix": "zenodo/1234567"},
        "metadata": {"group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "path": "datasets/zenodo", "public": false}
    })),
    responses(
        (status = 202, description = "Transfer accepted", body = SubmitImportResponse, example = json!({
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX", "created": true,
            "owner_node_url": "https://node.example/api/v1",
            "status_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "report_url": "https://node.example/api/v1/compute/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAX/report"
        })),
        (status = 400, description = "Invalid transfer request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Connector or destination access denied", body = ErrorResponse),
        (status = 404, description = "Destination not found", body = ErrorResponse),
        (status = 409, description = "Job conflict or quota refusal", body = ErrorResponse),
        (status = 503, description = "Transfer placement unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn import_record(
    state: State<Arc<ServerState>>,
    auth: Extension<Option<AuthContext>>,
    Json(request): Json<InvenioImportRequest>,
) -> ServerResult<(StatusCode, Json<SubmitImportResponse>)> {
    super::rocrate_import::submit_import(
        state,
        auth,
        Json(SubmitImportRequest {
            source: ImportSourceRequest::Invenio {
                group_id: request.group_id,
                connector_id: request.connector_id,
                record_id: request.record_id,
                options: request.options,
            },
            target: request.target,
            metadata: request.metadata,
            idempotency_key: request.idempotency_key,
        }),
    )
    .await
}

#[utoipa::path(
    post, path = "/metadata/{document_id}/invenio/exports", tag = "metadata/invenio",
    summary = "Export native Invenio metadata and files",
    description = r#"Exports mapped crate metadata and individual files to a native repository record.

**Authentication**

Requires READ on the crate, WRITE on the metadata path of the repository connector group and the user's repository.access_token. The token is encrypted for the user, node, connector and endpoint, omitted from output, and never replaced by the connector token.

**Behavior**

Creates a draft or uses draft_id for recovery. Set new_version to a published record ID to continue its version lineage. Native source metadata and custom fields survive import/export; repository.metadata overrides mapped fields. RO-Crate JSON remains a provenance file.

Publication and public_files both default to false; public_files also applies to existing drafts. New drafts reserve their DOI. With a connector community, publishing a first version submits it for review instead.

Draft metadata updates use revision preconditions; files and metadata are verified before and after publication. Results include record ID, parent ID, DOI, concept DOI, URL, publication and review state, and a warning when a check failed after publication.

**Limits**

Every referenced file must be readable; web data entities become references instead. A record holds at most 100 files. Reference imports fetch their bytes when exported. Repository vocabularies and publication permissions apply. The upstream publish endpoint has no atomic revision precondition.

**Errors**

Incomplete files, conflicting revisions or rejected metadata fail the job. An ambiguous creation outcome requires inspecting the repository and supplying draft_id. Cancellation retains remote drafts."#,
    params(("document_id" = String, Path, description = "Aruna metadata document identifier")),
    request_body(content = SubmitInvenioExport, example = json!({"repository": {
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
        (status = 400, description = "Missing personal repository token, invalid metadata or draft identifier, or missing required metadata listed in `missing`", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Crate or connector access denied", body = ErrorResponse),
        (status = 404, description = "Crate not found", body = ErrorResponse),
        (status = 409, description = "Job conflict or quota refusal", body = ErrorResponse),
        (status = 503, description = "Transfer placement unavailable", body = ErrorResponse)
    ), security(("bearer_auth" = []))
)]
pub async fn export_record(
    state: State<Arc<ServerState>>,
    auth: Extension<Option<AuthContext>>,
    path: Path<String>,
    Json(request): Json<SubmitInvenioExport>,
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
