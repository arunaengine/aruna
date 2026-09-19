//! Serves the routes that list documents referencing an IRI and preflight destructive changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerResult};
use crate::metadata::{
    ExcludedFormResponse, MetadataReferencesParams, MetadataReferencesResponse, PreflightBody,
    PreflightCoverageResponse, PreflightFreshnessResponse, PreflightLocationResponse,
    PreflightResponse, PreflightStorageBody, PreflightTargetBody, PreflightTargetResponse,
    PreflightVisibleResponse, bearer_token_string, map_api_error, map_query_mode,
    map_references_response,
};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::metadata::api::{
    MetadataReferencesRequest, MetadataStorageOperation, ReferenceExecution, ReferenceRequest,
    ReferenceTarget, references_metadata as run_references_metadata,
    references_preflight as run_references_preflight,
};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::sync::Arc;

#[utoipa::path(
    get,
    path = "/metadata/references",
    tag = "metadata/references",
    summary = "List documents referencing an IRI",
    description = r#"Lists the visible metadata documents that reference an IRI.

**Authentication**: realm bearer token; there is no anonymous access. Each candidate document is
filtered by the caller's READ permission, so an unreadable referencing document is omitted.

**Behavior**
- The scan is answered from the local node's reference index only, without realm fan-out, so
  references held solely by other nodes are missing and a just-accepted write may not be indexed
  yet.
- When the scan is empty and `iri` is a known graph IRI, or when `resolve` is set, the matching
  document's summary is returned as a single predicate-less entry.

**Limits**
- Results are capped at `limit` and there is no continuation."#,
    params(
        ("iri" = String, Query, description = "Referenced object IRI to find backlinks for, such as a document graph IRI or any IRI appearing as a triple object. Must be a valid absolute IRI"),
        ("predicate" = Option<String>, Query, description = "Optional exact predicate IRI filter, such as http://schema.org/conformsTo. Must be a valid absolute IRI and is matched exactly"),
        ("limit" = Option<usize>, Query, description = "Page size (default 25, silently clamped to a maximum of 100)"),
        ("resolve" = Option<bool>, Query, description = "Resolve iri as a document graph IRI and return that document's summary as a single predicate-less entry, skipping the backlink scan. Default false")
    ),
    responses(
        (
            status = 200,
            description = "Documents referencing the IRI that the caller may read, as indexed on the answering node",
            body = MetadataReferencesResponse,
            examples(
                (
                    "Backlinks" = (
                        summary = "One document conforming to the queried profile IRI",
                        value = json!({
                            "references": [
                                {
                                    "document_id": "01JMETADATA0123456789ABCDE",
                                    "group_id": "01JABCDEF0123456789ABCDEFG",
                                    "document_path": "datasets/proteomics/run-42",
                                    "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                    "predicate": "http://purl.org/dc/terms/conformsTo",
                                    "subject_iris": ["https://w3id.org/aruna/01JMETADATA0123456789ABCDE"],
                                    "title": "Proteomics Run 42"
                                }
                            ]
                        })
                    )
                ),
                (
                    "ResolvedGraph" = (
                        summary = "Graph IRI resolved to its own document",
                        value = json!({
                            "references": [
                                {
                                    "document_id": "01JMETADATA9876543210ZYXWV",
                                    "group_id": "01JABCDEF0123456789ABCDEFG",
                                    "document_path": "profiles/proteomics-profile",
                                    "graph_iri": "https://w3id.org/aruna/01JMETADATA9876543210ZYXWV",
                                    "subject_iris": [],
                                    "title": "Proteomics Profile"
                                }
                            ]
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Missing iri, or an iri or predicate that is not a valid absolute IRI", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn metadata_references(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(params): Query<MetadataReferencesParams>,
) -> ServerResult<(StatusCode, Json<MetadataReferencesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let ctx = state.get_ctx();
    let execution = run_references_metadata(
        ctx.as_ref(),
        state.get_realm_id(),
        MetadataReferencesRequest {
            auth: Some(auth),
            iri: params.iri,
            predicate: params.predicate,
            limit: params.limit,
            resolve: params.resolve,
        },
    )
    .await
    .map_err(map_api_error)?;
    Ok((StatusCode::OK, Json(map_references_response(execution))))
}

#[utoipa::path(
    post,
    path = "/metadata/references/preflight",
    tag = "metadata/references",
    summary = "Preflight destructive content operations against metadata backlinks",
    description = r#"Reports the metadata backlinks a destructive content operation would break.

**Authentication**: realm bearer token. A `bucket_prefix` target additionally requires WRITE on the
bucket, on the prefix when one is given, and on every key the prefix resolves to; a `content_w3ids`
target names content identities directly and checks no storage permission.

**Behavior**
- The target set is mapped to canonical content identities, which are then queried as canonical
  and known legacy location IRIs.
- Local mode reports realm coverage as incomplete; distributed mode fans out to realm nodes and
  reports per-node index freshness, failed partitions and stable cursor pagination.
- Restricted referencing documents are represented only by `hidden_references_exist`; their count
  and identity are never returned."#,
    request_body(content = PreflightBody,
        example = json!({
            "target": {
                "kind": "bucket_prefix",
                "bucket": "results",
                "prefix": "run-42/",
                "operation": "latest_version_tombstone"
            },
            "allow_partial": true,
            "limit": 50
        })),
    responses(
        (status = 200, description = "Reference warnings, location impact, pagination, and explicit coverage metadata", body = PreflightResponse,
            example = json!({
                "targets": [
                    {
                        "content_w3id": "https://w3id.org/aruna/data/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "targeted_versions": [
                            {
                                "node_id": "node-a",
                                "bucket": "results",
                                "key": "run-42/output.csv",
                                "version_id": "01JVERSION0000000000000000"
                            }
                        ],
                        "visible_references": [
                            {
                                "document_id": "01JMETADATA0123456789ABCDE",
                                "title": "Run 42 results"
                            }
                        ],
                        "hidden_references_exist": false,
                        "would_remove_last_resolvable_aruna_location": true,
                        "location_impact_complete": true
                    }
                ],
                "next_cursor": null,
                "truncated": false,
                "nodes_queried": 3,
                "nodes_failed": 0,
                "complete": true,
                "failed_partitions": [],
                "coverage": {
                    "queried_scope": "realm",
                    "queried_forms": ["content_w3id", "aruna_s3_url"],
                    "excluded_forms": [
                        {
                            "form": "literal_content_url",
                            "reason": "plain string contentUrl values are structurally invisible"
                        }
                    ],
                    "node_freshness": [
                        {
                            "node_id": "node-a",
                            "index_state": "current",
                            "oldest_status_updated_at_ms": 1787000000000_u64
                        }
                    ],
                    "target_resolution_complete": true,
                    "path_style_endpoint_coverage_complete": true,
                    "realm_coverage_complete": true
                }
            })),
        (status = 400, description = "Malformed or oversized target set, unsupported content identity, or invalid cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or WRITE is missing on the bucket, prefix or a resolved key", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse),
        (status = 503, description = "With allow_partial=false a failed, stale or otherwise incomplete partition fails the request instead of silently downgrading it; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn metadata_reference_preflight(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Json(request): Json<PreflightBody>,
) -> ServerResult<(StatusCode, Json<PreflightResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let target = match request.target {
        PreflightTargetBody::ContentW3ids {
            content_w3ids,
            remove_resolvable_locations,
        } => ReferenceTarget::ContentW3ids {
            content_w3ids,
            remove_resolvable_locations,
        },
        PreflightTargetBody::BucketPrefix {
            bucket,
            prefix,
            operation,
        } => ReferenceTarget::BucketPrefix {
            bucket,
            prefix,
            operation: match operation {
                PreflightStorageBody::LatestVersionTombstone => {
                    MetadataStorageOperation::LatestVersionTombstone
                }
                PreflightStorageBody::AllVersionsPurge => {
                    MetadataStorageOperation::AllVersionsPurge
                }
            },
        },
    };
    let s3_endpoint = state
        .interface_state()
        .await
        .s3
        .map(|interface| interface.base_url);
    let execution = run_references_preflight(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        ReferenceRequest {
            auth,
            bearer_token: bearer_token_string(bearer_token),
            target,
            s3_endpoint,
            limit: request.limit,
            cursor: request.cursor,
            mode: map_query_mode(request.mode),
            target_nodes: None,
            allow_partial: request.allow_partial,
        },
    )
    .await
    .map_err(map_api_error)?;
    Ok((StatusCode::OK, Json(map_preflight_response(execution))))
}

fn map_preflight_response(execution: ReferenceExecution) -> PreflightResponse {
    PreflightResponse {
        targets: execution
            .targets
            .into_iter()
            .map(|target| PreflightTargetResponse {
                content_w3id: target.content_w3id,
                targeted_versions: target
                    .targeted_versions
                    .into_iter()
                    .map(|location| PreflightLocationResponse {
                        node_id: location.node_id.to_string(),
                        bucket: location.bucket,
                        key: location.key,
                        version_id: location.version_id.to_string(),
                    })
                    .collect(),
                visible_references: target
                    .visible_references
                    .into_iter()
                    .map(|reference| PreflightVisibleResponse {
                        document_id: reference.document_id,
                        title: reference.title,
                    })
                    .collect(),
                hidden_references_exist: target.hidden_references_exist,
                would_remove_location: target.would_remove_location,
                location_impact_complete: target.location_impact_complete,
            })
            .collect(),
        next_cursor: execution.next_cursor,
        truncated: execution.truncated,
        nodes_queried: execution.nodes_queried,
        nodes_failed: execution.nodes_failed,
        complete: execution.complete,
        failed_partitions: execution
            .failed_partitions
            .into_iter()
            .map(|node_id| node_id.to_string())
            .collect(),
        coverage: PreflightCoverageResponse {
            queried_scope: execution.coverage.queried_scope.to_string(),
            queried_forms: execution
                .coverage
                .queried_forms
                .into_iter()
                .map(str::to_string)
                .collect(),
            excluded_forms: execution
                .coverage
                .excluded_forms
                .into_iter()
                .map(|excluded| ExcludedFormResponse {
                    form: excluded.form.to_string(),
                    reason: excluded.reason.to_string(),
                })
                .collect(),
            node_freshness: execution
                .coverage
                .node_freshness
                .into_iter()
                .map(|freshness| PreflightFreshnessResponse {
                    node_id: freshness.node_id.to_string(),
                    index_state: match freshness.index_state {
                        aruna_operations::metadata::api::MetadataIndexState::Current => "current",
                        aruna_operations::metadata::api::MetadataIndexState::Pending => "pending",
                        aruna_operations::metadata::api::MetadataIndexState::Failed => "failed",
                        aruna_operations::metadata::api::MetadataIndexState::Mixed => "mixed",
                    }
                    .to_string(),
                    oldest_status_updated: freshness.oldest_status_updated,
                })
                .collect(),
            target_resolution_complete: execution.coverage.target_resolution_complete,
            path_style_complete: execution.coverage.path_style_complete,
            realm_coverage_complete: execution.coverage.realm_coverage_complete,
        },
    }
}
