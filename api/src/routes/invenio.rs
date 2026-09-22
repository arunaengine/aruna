//! Native repository transfer endpoints backed by durable crate jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::error::{ErrorResponse, ServerResult};
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InvenioImportRequest {
    pub group_id: String,
    pub connector_id: String,
    pub record_id: String,
    pub target: ImportTargetRequest,
    pub metadata: ImportMetadataRequest,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitInvenioExport {
    pub repository: InvenioExportRequest,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[utoipa::path(
    post, path = "/metadata/invenio/imports", tag = "metadata/invenio",
    summary = "Import an Invenio or Zenodo record with version history and data",
    description = "Uses an existing HTTP connector whose endpoint is the repository API root, such as https://zenodo.org/api/. Requires READ on the connector group and WRITE on the destination data and metadata. Imports every accessible published version with checked file bytes and complete source record/file JSON. Hidden metadata edits and inaccessible/deleted versions cannot be recovered. The selected record supplies the root metadata. The returned job exposes progress, cancellation and its completion report.",
    request_body(content = InvenioImportRequest, example = json!({
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "record_id": "1234567", "target": {"bucket": "research", "prefix": "zenodo/1234567"},
        "metadata": {"group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "path": "datasets/zenodo", "public": false}
    })),
    responses(
        (status = 202, description = "Transfer accepted", body = SubmitImportResponse),
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
    summary = "Export a crate and its data to Invenio or Zenodo",
    description = "Creates a native Invenio record with mapped RO-Crate metadata and separately uploaded data files. Optional repository.metadata fields override the mapping. The requesting user must supply repository.access_token from their own Invenio/Zenodo account. Connector credentials are never used for export; the token is encrypted for this user and node and omitted from responses and debug output. The RO-Crate JSON is retained as a provenance file. Requires READ on the crate and WRITE on the connector group. Every referenced file must be included. Set repository.publish to true to publish after checksum verification; false leaves an unpublished draft. Files stay restricted unless repository.public_files is explicitly true. Existing drafts retain their access settings. An existing unpublished draft_id can be supplied for recovery. The job result contains the repository record URL and publication state. Ambiguous draft creation is never repeated automatically.",
    params(("document_id" = String, Path, description = "Aruna metadata document identifier")),
    request_body(content = SubmitInvenioExport, example = json!({"repository": {
        "group_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "connector_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "access_token": "<personal-access-token>", "publish": false
    }})),
    responses(
        (status = 202, description = "Transfer accepted", body = SubmitExportResponse),
        (status = 400, description = "Missing personal repository token, invalid metadata or draft identifier", body = ErrorResponse),
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
