//! Serves the RO-Crate routes to export, replace and upsert entities of one document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{
    ValidatedBearer, ensure_permission_with, require_realm_auth, require_unrestricted_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{
    JsonLdObject, MetadataDocumentSummary, MetadataRoCrateResponse, MetadataRoCrateView,
    ReplaceRoCrateRequest, RoCrateExportParams, SubmitExportRequest, SubmitExportResponse,
    forwarded_auth_token, load_document_record, local_write_record, map_api_error,
    map_export_response, map_export_view, map_write_error, parse_document_id,
    serialize_jsonld_object,
};
use crate::routes::execution::jobs::{job_urls, map_submit_error};
use crate::server::state::ServerState;
use aruna_core::structs::execution::job::ExportRoCrateSpec;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::jobs::service::submit_export_job;
use aruna_operations::metadata::api::ExportMetadataRequest;
use aruna_operations::metadata::forward::{
    export_rocrate_routed as run_export_rocrate,
    route_metadata_update as run_update_metadata_document,
};
use aruna_operations::metadata::update_document::UpdateDocumentMutation;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde_json::Value;
use std::sync::Arc;

#[utoipa::path(
    get,
    path = "/metadata/{document_id}/rocrate",
    tag = "metadata/rocrate",
    summary = "Export a metadata document as RO-Crate",
    description = r#"Exports a metadata document as an RO-Crate in the requested view.

**Authentication**: optional bearer token; a document that is not public is only exported to a
caller whose identity may read it, and an unknown document is indistinguishable from an
unreadable one because both answer 404.

**Behavior**
- The export is routed to the document's current holders and answered by the first holder that
  succeeds.
- RO-Crate 1.2 and 1.3 documents retain their stored context and specification version in every
  export view.
- The projected views (`full`, `summary`, `page`) need the document's graph to be materialized. The
  `raw` view returns the displayed revision with the last event merged into it, the projection
  state (`pending`, `materialized` or `failed`) and digests, so it stays readable while a recently
  accepted write is still being projected.
- Metadata graphs merge as OR-Sets and only a valid crate is displayed: when the merged graph fails
  Profile validation the displayed revision stays at the last valid render and the `raw` view also
  carries `merged` with the invalid render and its finding count, which is what an editor opens.
  The projected views render the merged graph either way.
- The pagination fields are only populated for the `page` view, and in the `summary` and `page`
  views the crate's root identifier is rewritten to carry the requested view and cursor, so a
  partial crate is not mistaken for the full one.

**Limits**: an export larger than this node's configured metadata byte limit is refused rather than
truncated."#,
    params(
        ("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list"),
        ("view" = Option<MetadataRoCrateView>, Query, description = "Export view, default full: full for the whole projected crate, summary for the root entity, page for a window over root-linked data entities, raw for the last accepted revision"),
        ("limit" = Option<usize>, Query, description = "Maximum number of root-linked data entities for page view. Default 100, clamped to 1..1000. Ignored by the other views"),
        ("offset" = Option<usize>, Query, description = "Offset cursor for page view: number of root-linked data entities to skip. Mutually exclusive with after, and sending both is rejected with 400"),
        ("after" = Option<String>, Query, description = "Entity id cursor for page view: continue after this data entity id, taken from a previous response's next_cursor. Mutually exclusive with offset")
    ),
    responses(
        (
            status = 200,
            description = "RO-Crate export in the requested view: a projected rocrate object, or the stored revision for the raw view",
            body = MetadataRoCrateResponse,
            examples(
                (
                    "FullRoCrate" = (
                        summary = "Full RO-Crate export",
                        value = json!({
                            "rocrate": {
                                "@context": "https://w3id.org/ro/crate/1.2/context",
                                "@graph": [
                                    {
                                        "@id": "ro-crate-metadata.json",
                                        "@type": "CreativeWork",
                                        "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                                        "about": { "@id": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE" }
                                    },
                                    {
                                        "@id": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                        "@type": "Dataset",
                                        "name": "Proteomics Run 42",
                                        "description": "Metadata record for LC-MS run 42",
                                        "datePublished": "2026-04-09",
                                        "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" },
                                        "hasPart": [{ "@id": "./data/run-42.raw" }]
                                    },
                                    {
                                        "@id": "./data/run-42.raw",
                                        "@type": "File",
                                        "name": "run-42.raw"
                                    }
                                ]
                            },
                            "total_data_entities": null,
                            "returned_data_entities": null,
                            "next_offset": null,
                            "next_cursor": null
                        })
                    )
                ),
                (
                    "PagedRoCrate" = (
                        summary = "One page of root-linked data entities",
                        value = json!({
                            "rocrate": {
                                "@context": "https://w3id.org/ro/crate/1.2/context",
                                "@graph": [
                                    {
                                        "@id": "ro-crate-metadata.json",
                                        "@type": "CreativeWork",
                                        "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                                        "about": { "@id": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE?view=page&limit=1" }
                                    },
                                    {
                                        "@id": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE?view=page&limit=1",
                                        "@type": "Dataset",
                                        "name": "Proteomics Run 42",
                                        "hasPart": [{ "@id": "./data/run-42.raw" }]
                                    },
                                    {
                                        "@id": "./data/run-42.raw",
                                        "@type": "File",
                                        "name": "run-42.raw"
                                    }
                                ]
                            },
                            "total_data_entities": 128,
                            "returned_data_entities": 1,
                            "next_offset": 1,
                            "next_cursor": "./data/run-42.raw"
                        })
                    )
                ),
                (
                    "RawRevision" = (
                        summary = "Last accepted revision with its projection state",
                        value = json!({
                            "raw": {
                                "@context": "https://w3id.org/ro/crate/1.2/context",
                                "@graph": [
                                    {
                                        "@id": "ro-crate-metadata.json",
                                        "@type": "CreativeWork",
                                        "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                                        "about": { "@id": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE" }
                                    },
                                    {
                                        "@id": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                        "@type": "Dataset",
                                        "name": "Proteomics Run 42"
                                    }
                                ]
                            },
                            "winning_event_id": "01JEVENT00123456789ABCDEFG",
                            "projection_state": "pending",
                            "context_digest": "3f5a1c9d8e7b6a4f2d0c1b3a5e7f90813f5a1c9d8e7b6a4f2d0c1b3a5e7f9081",
                            "dataset_digest": "0b1c2d3e4f5a69788796a5b4c3d2e1f00b1c2d3e4f5a69788796a5b4c3d2e1f0"
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Document id that is not a structured metadata id, or both offset and after sent for the page view", body = ErrorResponse),
        (status = 401, description = "A holder rejected the forwarded credential as unauthenticated", body = ErrorResponse),
        (status = 403, description = "A holder denied READ for this caller on the document", body = ErrorResponse),
        (status = 404, description = "The document does not exist or is not readable by the caller; the two cases are deliberately indistinguishable", body = ErrorResponse),
        (status = 503, description = "The graph is not materialized yet for a projected view, which `view=raw` can read meanwhile, the export exceeds this node's metadata byte limit, or no holder answered", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn export_metadata_rocrate(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
    Query(params): Query<RoCrateExportParams>,
) -> ServerResult<(StatusCode, Json<MetadataRoCrateResponse>)> {
    let document_id = parse_document_id(&document_id)?;
    let view = params.view.clone().unwrap_or(MetadataRoCrateView::Full);
    let ctx = state.get_ctx();
    let export = run_export_rocrate(
        &ctx,
        state.get_realm_id(),
        ExportMetadataRequest {
            document_id,
            auth,
            view: map_export_view(&view),
            limit: params.limit,
            offset: params.offset,
            after: params.after.clone(),
        },
        forwarded_auth_token(bearer_token)?,
        state.rocrate_limits().metadata_bytes,
    )
    .await
    .map_err(map_api_error)?;
    let response = map_export_response(export, &params, view)?;
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/rocrate/exports",
    tag = "metadata/rocrate",
    summary = "Submit an RO-Crate export job",
    description = r#"Submits an asynchronous job that assembles the document's RO-Crate.

**Authentication**: realm bearer token with READ on the document, resolved from this node's
registry view. A path-restricted delegated token is refused even when it would pass that check.

**Behavior**
- The job is durably accepted and owned by this node; the crate is assembled asynchronously, so
  the artifact does not exist yet.
- The artifact preserves whether the source document is RO-Crate 1.2 or 1.3.
- `status_url`, `report_url` and `artifact_url` are absolute and point at the owning node, the only
  node that can serve them.
- Submissions are idempotent per caller when `idempotency_key` is set: replaying the same key
  returns the same job with `created` false, while reusing it for a different document conflicts."#,
    params(("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list")),
    request_body(
        content = SubmitExportRequest,
        description = "Optional idempotency key, scoped to the calling user. Rejects unknown fields; an empty object submits a new job unconditionally.",
        example = json!({
            "idempotency_key": "export-run-42-2026-04-09"
        })
    ),
    responses(
        (
            status = 202,
            description = "Export job durably accepted and queued on this node; `created` is false when an existing job was replayed",
            body = SubmitExportResponse,
            examples(
                (
                    "Accepted" = (
                        summary = "Newly queued export job",
                        value = json!({
                            "job_id": "01JJOB0123456789ABCDEFGHJK",
                            "created": true,
                            "owner_node_url": "https://node.example.test/api/v1",
                            "status_url": "https://node.example.test/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHJK",
                            "report_url": "https://node.example.test/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHJK/report",
                            "artifact_url": "https://node.example.test/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHJK/artifacts/rocrate"
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Malformed body, unknown fields, or a document id that is not a structured metadata id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "READ denied on the document, the token belongs to another realm, or the token is path-restricted", body = ErrorResponse),
        (status = 404, description = "This node's registry view has no such metadata document", body = ErrorResponse),
        (status = 409, description = "The idempotency key is already bound to a job with a different plan, or the caller's active RO-Crate job limit is reached", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_rocrate_export(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Json(request): Json<SubmitExportRequest>,
) -> ServerResult<(StatusCode, Json<SubmitExportResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let record = load_document_record(&state, document_id).await?;
    ensure_permission_with(
        &state,
        &auth,
        record.permission_path,
        Permission::READ,
        PolicyRequestExtras::operation("metadata.read"),
    )
    .await?;
    let mut access_token = None;
    let mut destination = request
        .destination
        .map(|destination| {
            access_token = Some(
                destination
                    .access_token
                    .filter(|token| !token.is_empty())
                    .ok_or_else(|| {
                        ServerError::BadRequestReason(
                            "a personal repository access_token is required".into(),
                        )
                    })?,
            );
            if destination.metadata.to_string().len() as u64 > state.rocrate_limits().metadata_bytes
            {
                return Err(ServerError::BadRequestReason(
                    "repository metadata exceeds limit".into(),
                ));
            }
            if !destination.metadata.is_null() && !destination.metadata.is_object() {
                return Err(ServerError::BadRequestReason(
                    "metadata overrides must be an object".into(),
                ));
            }
            for id in destination
                .draft_id
                .iter()
                .chain(destination.new_version.iter())
            {
                aruna_core::invenio::validate_id(id)
                    .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
            }
            Ok(aruna_core::invenio::InvenioDestination {
                group_id: ulid::Ulid::from_string(&destination.group_id)
                    .map_err(|_| ServerError::BadRequest)?,
                connector_id: ulid::Ulid::from_string(&destination.connector_id)
                    .map_err(|_| ServerError::BadRequest)?,
                draft_id: destination.draft_id,
                new_version: destination.new_version,
                metadata_json: destination.metadata.to_string(),
                publish: destination.publish,
                public_files: destination.public_files,
                credential: None,
                link: None,
            })
        })
        .transpose()?;
    if let Some(destination) = &mut destination {
        crate::metadata::ensure_metadata_scope(
            &state,
            &auth,
            destination.group_id,
            Permission::WRITE,
        )
        .await?;
        Box::pin(crate::routes::invenio_links::check_mapping(
            &state,
            &auth,
            document_id,
            &destination.metadata_json,
        ))
        .await?;
        destination.credential = Some(
            aruna_operations::jobs::invenio::seal_credential(
                &state.get_ctx(),
                &auth,
                destination,
                access_token.as_deref().unwrap_or_default(),
            )
            .await
            .map_err(|error| match error {
                aruna_operations::jobs::invenio::TransferError::Permanent(message) => {
                    ServerError::BadRequestReason(message)
                }
                _ => ServerError::ServiceUnavailableReason(
                    "repository login could not be prepared".into(),
                ),
            })?,
        );
    }
    let result = submit_export_job(
        &state.get_ctx(),
        ExportRoCrateSpec {
            destination,
            auth_context: auth,
            document_id,
            limits: state.rocrate_limits().clone(),
        },
        state.get_node_id(),
        request.idempotency_key,
    )
    .await
    .map_err(map_submit_error)?;
    let urls = job_urls(&state, result.job_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitExportResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
            owner_node_url: urls.owner_node_url,
            status_url: urls.status_url,
            report_url: urls.report_url,
            artifact_url: urls.artifact_url,
        }),
    ))
}

#[utoipa::path(
    put,
    path = "/metadata/{document_id}/rocrate",
    tag = "metadata/rocrate",
    summary = "Replace a document's RO-Crate",
    description = r#"Replaces a document's stored RO-Crate with the submitted crate.

**Authentication**: realm bearer token with WRITE on the document's permission path. A node that
does not hold the document forwards the write to a holder that re-runs that check under the
caller's own token.

**Behavior**
- The submitted crate replaces the stored one wholesale, so any entity omitted from it is
  dropped.
- Omitting `public` leaves the current visibility unchanged.
- Acceptance is durable but asynchronous: the revision may not be materialized, queryable,
  searchable or present on every replica yet."#,
    params(("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list")),
    request_body(
        content = ReplaceRoCrateRequest,
        description = "The full replacement RO-Crate with a 1.2 or 1.3 context and specification IRI; the submitted version is preserved. Use the entity routes for small incremental changes. Omitting public keeps the current visibility.",
        examples(
            (
                "ReplaceRoCrate" = (
                    summary = "Replace entire RO-Crate",
                    value = json!({
                        "public": true,
                        "rocrate": {
                            "@context": "https://w3id.org/ro/crate/1.2/context",
                            "@graph": [
                                {
                                    "@id": "ro-crate-metadata.json",
                                    "@type": "CreativeWork",
                                    "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                                    "about": { "@id": "urn:dataset:run-42" }
                                },
                                {
                                    "@id": "urn:dataset:run-42",
                                    "@type": "Dataset",
                                    "name": "Proteomics Run 42",
                                    "description": "Updated dataset description",
                                    "datePublished": "2026-04-09",
                                    "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" }
                                }
                            ]
                        }
                    })
                )
            )
        )
    ),
    responses(
        (
            status = 200,
            description = "Accepted into the durable event/projection pipeline; not necessarily fully materialized, queryable, searchable or replicated yet",
            body = MetadataDocumentSummary,
            examples(
                (
                    "UpdatedSummary" = (
                        summary = "Updated metadata summary",
                        value = json!({
                            "document_id": "01JMETADATA0123456789ABCDE",
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "document_path": "datasets/proteomics/run-42",
                            "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                            "public": true,
                            "replicas": 3,
                            "created_at": "2026-04-09T14:23:11.123Z",
                            "updated_at": "2026-04-09T14:25:54.221Z"
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Malformed body, a document id that is not a structured metadata id, a non-object RO-Crate, or RO-Crate validation violations, which are listed in the error body", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token, or a holder rejected the forwarded credential", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or WRITE is denied on the document", body = ErrorResponse),
        (status = 404, description = "No holder knows this document", body = ErrorResponse),
        (status = 503, description = "Realm placement view unreadable, the document has no usable holder, the revision exceeds the stored-document limit, or no holder accepted the forwarded write; the write was not accepted and may be retried", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_metadata_rocrate(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
    Json(request): Json<ReplaceRoCrateRequest>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let ctx = state.get_ctx();
    let record =
        local_write_record(&state, &auth, document_id, PolicyRequestExtras::rest()).await?;
    let updated = run_update_metadata_document(
        &ctx,
        Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        record.as_ref(),
        document_id,
        request.public,
        UpdateDocumentMutation::ReplaceRoCrate {
            jsonld: serialize_jsonld_object(&request.rocrate)?,
        },
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_write_error)?;

    Ok((
        StatusCode::OK,
        Json(MetadataDocumentSummary::from(&updated)),
    ))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/rocrate/entities/data",
    tag = "metadata/rocrate",
    summary = "Upsert an RO-Crate data entity",
    description = r#"Adds or replaces one root-linked data entity in a document's RO-Crate.

**Authentication**: realm bearer token with WRITE on the document's permission path. A node that
does not hold the document forwards the write to a holder that re-runs that check under the
caller's own token.

**Behavior**
- The entity is matched by its `@id`: an existing data entity with that id is replaced, otherwise
  it is added and linked from the crate's root dataset.
- The rest of the crate, including its RO-Crate 1.2 or 1.3 version, is left untouched, and the
  document's visibility is unchanged.
- Acceptance is durable but asynchronous: the revision may not be materialized, queryable,
  searchable or present on every replica yet."#,
    params(("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list")),
    request_body(
        content = inline(JsonLdObject),
        description = "One root-linked RO-Crate data entity as a single JSON-LD object carrying its own @id; a body containing @graph is rejected.",
        examples(
            (
                "DataEntity" = (
                    summary = "Add a file data entity",
                    value = json!({
                        "@id": "./data/run-42.raw",
                        "@type": "File",
                        "name": "run-42.raw",
                        "description": "Raw instrument output",
                        "encodingFormat": "application/octet-stream",
                        "creator": { "@id": "#person-ada" },
                        "keywords": ["proteomics", "orbitrap", "raw-data"],
                        "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" }
                    })
                )
            )
        )
    ),
    responses(
        (
            status = 200,
            description = "Accepted into the durable event/projection pipeline; not necessarily fully materialized, queryable, searchable or replicated yet",
            body = MetadataDocumentSummary,
            examples(
                (
                    "UpdatedSummary" = (
                        summary = "Metadata summary after data entity upsert",
                        value = json!({
                            "document_id": "01JMETADATA0123456789ABCDE",
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "document_path": "datasets/proteomics/run-42",
                            "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                            "public": true,
                            "replicas": 3,
                            "created_at": "2026-04-09T14:23:11.123Z",
                            "updated_at": "2026-04-09T14:26:37.904Z"
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Malformed body, a body that is not a single JSON-LD object or that contains @graph, a document id that is not a structured metadata id, or RO-Crate violations listed in the error body", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token, or a holder rejected the forwarded credential", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or WRITE is denied on the document", body = ErrorResponse),
        (status = 404, description = "No holder knows this document", body = ErrorResponse),
        (status = 503, description = "Realm placement view unreadable, the document has no usable holder, the revision exceeds the stored-document limit, or no holder accepted the forwarded write; the write was not accepted and may be retried", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_data_entity(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
    Json(entity): Json<Value>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let ctx = state.get_ctx();
    let record =
        local_write_record(&state, &auth, document_id, PolicyRequestExtras::rest()).await?;
    let updated = run_update_metadata_document(
        &ctx,
        Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        record.as_ref(),
        document_id,
        None,
        UpdateDocumentMutation::UpsertDataEntity {
            jsonld: serialize_jsonld_entity(&entity)?,
        },
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_write_error)?;

    Ok((
        StatusCode::OK,
        Json(MetadataDocumentSummary::from(&updated)),
    ))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/rocrate/entities/contextual",
    tag = "metadata/rocrate",
    summary = "Upsert an RO-Crate contextual entity",
    description = r#"Adds or replaces one contextual entity in a document's RO-Crate.

**Authentication**: realm bearer token with WRITE on the document's permission path. A node that
does not hold the document forwards the write to a holder that re-runs that check under the
caller's own token.

**Behavior**
- The entity is matched by its `@id`: an existing contextual entity with that id is replaced,
  otherwise it is added.
- Contextual entities describe people, organizations, licenses and the like and are not linked
  from the root dataset as parts.
- The surrounding crate retains its RO-Crate 1.2 or 1.3 version.
- Acceptance is durable but asynchronous: the revision may not be materialized, queryable,
  searchable or present on every replica yet."#,
    params(("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list")),
    request_body(
        content = inline(JsonLdObject),
        description = "One RO-Crate contextual entity as a single JSON-LD object carrying its own @id; a body containing @graph is rejected.",
        examples(
            (
                "ContextualEntity" = (
                    summary = "Add a person contextual entity",
                    value = json!({
                        "@id": "#person-ada",
                        "@type": "Person",
                        "name": "Ada Lovelace",
                        "affiliation": { "@id": "#org-aruna" }
                    })
                )
            )
        )
    ),
    responses(
        (
            status = 200,
            description = "Accepted into the durable event/projection pipeline; not necessarily fully materialized, queryable, searchable or replicated yet",
            body = MetadataDocumentSummary,
            examples(
                (
                    "UpdatedSummary" = (
                        summary = "Metadata summary after contextual entity upsert",
                        value = json!({
                            "document_id": "01JMETADATA0123456789ABCDE",
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "document_path": "datasets/proteomics/run-42",
                            "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                            "public": true,
                            "replicas": 3,
                            "created_at": "2026-04-09T14:23:11.123Z",
                            "updated_at": "2026-04-09T14:24:05.011Z"
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Malformed body, a body that is not a single JSON-LD object or that contains @graph, a document id that is not a structured metadata id, or RO-Crate violations listed in the error body", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token, or a holder rejected the forwarded credential", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or WRITE is denied on the document", body = ErrorResponse),
        (status = 404, description = "No holder knows this document", body = ErrorResponse),
        (status = 503, description = "Realm placement view unreadable, the document has no usable holder, the revision exceeds the stored-document limit, or no holder accepted the forwarded write; the write was not accepted and may be retried", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_contextual_entity(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
    Json(entity): Json<Value>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let ctx = state.get_ctx();
    let record =
        local_write_record(&state, &auth, document_id, PolicyRequestExtras::rest()).await?;
    let updated = run_update_metadata_document(
        &ctx,
        Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        record.as_ref(),
        document_id,
        None,
        UpdateDocumentMutation::UpsertContextualEntity {
            jsonld: serialize_jsonld_entity(&entity)?,
        },
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_write_error)?;

    Ok((
        StatusCode::OK,
        Json(MetadataDocumentSummary::from(&updated)),
    ))
}

fn serialize_jsonld_entity(value: &Value) -> ServerResult<String> {
    let Some(object) = value.as_object() else {
        return Err(ServerError::BadRequest);
    };
    if object.contains_key("@graph") || object.contains_key("graph") {
        return Err(ServerError::BadRequest);
    }
    serde_json::to_string(value).map_err(|_| ServerError::BadRequest)
}
