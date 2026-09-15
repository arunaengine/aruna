//! Metadata document lifecycle routes: thin request-to-operation conversion
//! over the shared `crate::metadata` adapter.

use crate::auth::{ValidatedBearer, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerResult};
use crate::metadata::{
    CreateMetadataRequest, CreateMetadataResponse, ListMetadataQuery, ListMetadataResponse,
    MetadataDocumentSummary, MetadataPathQuery, MetadataPathResponse, forwarded_auth_token,
    local_write_record, map_api_error, map_write_error, parse_document_id, run_create_metadata,
    run_document_list, serialize_jsonld_object,
};
use crate::server_state::ServerState;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::metadata::api::{
    GetVisibleRequest, MetadataLookupRequest, lookup_metadata_path as run_lookup_metadata_path,
};
use aruna_operations::metadata::create_document::CreateDocumentPayload;
use aruna_operations::metadata::forward::{
    get_metadata_routed as run_get_visible_metadata_document,
    route_metadata_delete as run_delete_metadata_document,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::sync::Arc;

#[utoipa::path(
    post,
    path = "/metadata",
    tag = "metadata/documents",
    summary = "Create a metadata document",
    description = r#"Creates a metadata document from scaffold fields or a submitted RO-Crate.

**Authentication**: realm bearer token with WRITE on the group's metadata path and on the new
document's permission path. A user-kind node holds no metadata bucket, so it forwards the create to
a holder that re-runs both checks under the caller's own token.

**Behavior**
- Acceptance is durable but asynchronous: the graph may not be materialized, queryable, searchable
  or present on every replica yet, so a follow-up read can answer 404 or 503 for a moment.
- A write that can neither be applied locally nor delivered to a holder is refused rather than
  accepted.

**Limits**: the document path is normalized before use and must not be empty."#,
    request_body(
        content = CreateMetadataRequest,
        description = "Scaffold fields or a full RO-Crate JSON-LD object. Scaffold creation emits RO-Crate 1.3; the RO-Crate form accepts 1.2 and 1.3 contexts and specification IRIs and preserves the submitted version. Both forms reject unknown fields.",
        examples(
            (
                "ScaffoldCreate" = (
                    summary = "Create from scaffold fields",
                    value = json!({
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "path": "datasets/proteomics/run-42",
                        "name": "Proteomics Run 42",
                        "description": "Metadata record for LC-MS run 42",
                        "date_published": "2026-04-09",
                        "license": "https://creativecommons.org/licenses/by/4.0/",
                        "public": true
                    })
                )
            ),
            (
                "RoCrateCreate" = (
                    summary = "Create from inline RO-Crate",
                    value = json!({
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "path": "datasets/proteomics/run-42",
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
                                    "description": "Metadata record for LC-MS run 42",
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
            status = 201,
            description = "Accepted into the durable event/projection pipeline; not necessarily fully materialized, queryable, searchable or replicated yet",
            body = CreateMetadataResponse,
            examples(
                (
                    "Created" = (
                        summary = "Created metadata summary",
                        value = json!({
                            "document_id": "01JMETADATA0123456789ABCDE",
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "document_path": "datasets/proteomics/run-42",
                            "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                            "public": true,
                            "replicas": 3,
                            "created_at": "2026-04-09T14:23:11.123Z",
                            "updated_at": "2026-04-09T14:23:11.123Z"
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Malformed body, unknown fields, a group id that is not a ULID, an empty document path, a non-object RO-Crate, or RO-Crate validation violations, which are listed in the error body", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or WRITE is denied on the group's metadata path or the new document's path", body = ErrorResponse),
        (status = 409, description = "Concurrent create conflict; the create was not accepted and may be retried", body = ErrorResponse),
        (status = 503, description = "Placement binding unavailable or conflicted, realm configuration unreadable, local clock unhealthy, or no holder accepted the forwarded write; the create was not accepted and may be retried", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Json(request): Json<CreateMetadataRequest>,
) -> ServerResult<(StatusCode, Json<CreateMetadataResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let (group_id, path, public, payload) = match request {
        CreateMetadataRequest::Scaffold(request) => (
            parse_group_id(&request.group_id)?,
            request.path,
            request.public,
            CreateDocumentPayload::Scaffold {
                name: request.name,
                description: request.description,
                date_published: request.date_published,
                license: request.license,
            },
        ),
        CreateMetadataRequest::RoCrate(request) => (
            parse_group_id(&request.group_id)?,
            request.path,
            request.public,
            CreateDocumentPayload::RoCrate {
                jsonld: serialize_jsonld_object(&request.rocrate)?,
            },
        ),
    };
    let result = run_create_metadata(
        &state,
        &auth,
        PolicyRequestExtras::rest(),
        bearer_token,
        group_id,
        path,
        public,
        payload,
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateMetadataResponse {
            summary: MetadataDocumentSummary::from(&result),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/metadata",
    tag = "metadata/documents",
    summary = "List visible metadata documents across groups",
    description = r#"Lists metadata documents from every group the caller may see.

**Authentication**: optional bearer token; an authenticated caller sees every document their
identity may read, an anonymous caller only public documents.

**Behavior**
- Answered from this node's eventually consistent registry view, so a just-accepted create can be
  missing for a moment, and a document whose graph is not materialized yet is listed without its
  RO-Crate summary.
- Pagination is offset-based over the filtered, visibility-checked sequence, so a page is stable
  only as far as concurrent metadata churn allows.
- `limit` and `offset` echo the values actually applied after clamping.
- `total_estimate` is only present for browse-sized pages (an effective limit of 24 or more),
  where it counts the documents visible to the caller across all pages of the same filter."#,
    params(
        ("group_id" = Option<String>, Query, description = "Optional group ULID filter. Restricts the listing to that group; omitted lists every group visible to the caller"),
        ("path_prefix" = Option<String>, Query, description = "Normalized metadata path prefix, for example profiles/. Matches the prefix itself and any path directly below it, never a partial path segment"),
        ("include" = Option<String>, Query, description = "Comma-separated includes. Currently supports summary, which adds a compact RO-Crate summary per document. Any other value is rejected with 400"),
        ("limit" = Option<usize>, Query, description = "Maximum documents to return. Default 50, clamped to 1..1000 for an authenticated caller and to 1..100 for an anonymous one"),
        ("offset" = Option<usize>, Query, description = "Number of filtered, visible documents to skip. Default 0"),
        ("order" = Option<String>, Query, description = "Page order: created (default, ascending document id) or recent (descending updated_at, tie-broken by descending document id). Any other value is rejected with 400")
    ),
    responses(
        (
            status = 200,
            description = "Metadata documents visible to the caller, with the applied page size, offset and counts",
            body = ListMetadataResponse,
            examples(
                (
                    "SummaryPage" = (
                        summary = "First page with RO-Crate summaries",
                        value = json!({
                            "documents": [
                                {
                                    "document_id": "01JMETADATA0123456789ABCDE",
                                    "group_id": "01JABCDEF0123456789ABCDEFG",
                                    "document_path": "datasets/proteomics/run-42",
                                    "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                    "public": true,
                                    "replicas": 3,
                                    "created_at": "2026-04-09T14:23:11.123Z",
                                    "updated_at": "2026-04-09T14:25:54.221Z",
                                    "rocrate_summary": {
                                        "@context": "https://w3id.org/ro/crate/1.2/context",
                                        "@graph": [
                                            {
                                                "@id": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                                "@type": "Dataset",
                                                "name": "Proteomics Run 42",
                                                "datePublished": "2026-04-09"
                                            }
                                        ]
                                    }
                                }
                            ],
                            "limit": 50,
                            "offset": 0,
                            "total_returned": 1,
                            "total_estimate": 1
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Group id that is not a ULID, unknown include value, or unknown order value", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn list_all_documents(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListMetadataQuery>,
) -> ServerResult<(StatusCode, Json<ListMetadataResponse>)> {
    let group_id = query.group_id.as_deref().map(parse_group_id).transpose()?;
    Ok((
        StatusCode::OK,
        Json(run_document_list(&state, auth, query, group_id).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/metadata/groups/{group_id}",
    tag = "metadata/documents",
    summary = "List a group's visible metadata documents",
    description = r#"Lists the metadata documents of one group that the caller may see.

**Authentication**: optional bearer token; an authenticated caller sees every document of the
group their identity may read, an anonymous caller only public documents.

**Behavior**
- Answered from this node's eventually consistent registry view, so a just-accepted create can be
  missing for a moment, and a document whose graph is not materialized yet is listed without its
  RO-Crate summary.
- Pagination is offset-based over the filtered, visibility-checked sequence, and `limit` and
  `offset` echo the values actually applied after clamping.
- `total_estimate` is only present for browse-sized pages (an effective limit of 24 or more)."#,
    params(
        ("group_id" = String, Path, description = "Group ULID whose metadata documents are listed"),
        ("path_prefix" = Option<String>, Query, description = "Normalized metadata path prefix, for example profiles/. Matches the prefix itself and any path directly below it, never a partial path segment"),
        ("include" = Option<String>, Query, description = "Comma-separated includes. Currently supports summary, which adds a compact RO-Crate summary per document. Any other value is rejected with 400"),
        ("limit" = Option<usize>, Query, description = "Maximum documents to return. Default 50, clamped to 1..1000 for an authenticated caller and to 1..100 for an anonymous one"),
        ("offset" = Option<usize>, Query, description = "Number of filtered, visible documents to skip. Default 0"),
        ("order" = Option<String>, Query, description = "Page order: created (default, ascending document id) or recent (descending updated_at, tie-broken by descending document id). Any other value is rejected with 400")
    ),
    responses(
        (
            status = 200,
            description = "Metadata documents of the group visible to the caller, with the applied page size, offset and counts",
            body = ListMetadataResponse,
            examples(
                (
                    "GroupPage" = (
                        summary = "First page without summaries",
                        value = json!({
                            "documents": [
                                {
                                    "document_id": "01JMETADATA0123456789ABCDE",
                                    "group_id": "01JABCDEF0123456789ABCDEFG",
                                    "document_path": "datasets/proteomics/run-42",
                                    "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                    "public": true,
                                    "replicas": 3,
                                    "created_at": "2026-04-09T14:23:11.123Z",
                                    "updated_at": "2026-04-09T14:25:54.221Z"
                                }
                            ],
                            "limit": 50,
                            "offset": 0,
                            "total_returned": 1,
                            "total_estimate": 1
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Group id that is not a ULID, unknown include value, or unknown order value", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn list_metadata_documents(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Query(query): Query<ListMetadataQuery>,
) -> ServerResult<(StatusCode, Json<ListMetadataResponse>)> {
    let group_id = parse_group_id(&group_id)?;
    Ok((
        StatusCode::OK,
        Json(run_document_list(&state, auth, query, Some(group_id)).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/metadata/groups/{group_id}/path",
    tag = "metadata/documents",
    summary = "Resolve a metadata path to its winning document",
    description = r#"Resolves one normalized metadata path to its visible winning document.

**Authentication**: optional bearer token; only claims the caller may read are considered, so an
anonymous caller resolves the winner among public documents only.

**Behavior**
- The winner and its conflicts are the ones reported consistently by every current replica of the
  path's registry shards, under a bounded fan-out and a deadline of a few seconds.
- A path whose only claims are invisible answers 404 rather than disclosing that it is taken."#,
    params(
        ("group_id" = String, Path, description = "Group ULID that owns the path"),
        ("path" = String, Query, description = "Exact metadata path, normalized before lookup (surrounding slashes trimmed); an empty path is rejected with 400. Prefix matching is not applied")
    ),
    responses(
        (
            status = 200,
            description = "The winning document plus the ids of visible losing claims, which stay reachable by id",
            body = MetadataPathResponse,
            examples(
                (
                    "PathWinner" = (
                        summary = "Winner with one conflicting claim",
                        value = json!({
                            "winner": {
                                "document_id": "01JMETADATA0123456789ABCDE",
                                "group_id": "01JABCDEF0123456789ABCDEFG",
                                "document_path": "datasets/proteomics/run-42",
                                "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                "public": true,
                                "replicas": 3,
                                "created_at": "2026-04-09T14:23:11.123Z",
                                "updated_at": "2026-04-09T14:25:54.221Z"
                            },
                            "conflicts": ["01JMETADATA9876543210ZYXWV"]
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Group id that is not a ULID, an empty path, or an unusable bearer forwarding carrier", body = ErrorResponse),
        (status = 401, description = "A replica rejected the forwarded credential as unauthenticated", body = ErrorResponse),
        (status = 403, description = "A replica denied READ for this caller on the path's documents", body = ErrorResponse),
        (status = 404, description = "No claim for the path is visible to the caller, which is also the answer when the path is taken by claims the caller may not read", body = ErrorResponse),
        (status = 503, description = "The current-replica path view is unavailable, divergent, or past the lookup deadline, so the lookup fails closed instead of answering from a partial view; retryable", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_metadata_path(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(group_id): Path<String>,
    Query(query): Query<MetadataPathQuery>,
) -> ServerResult<(StatusCode, Json<MetadataPathResponse>)> {
    let group_id = parse_group_id(&group_id)?;
    let result = run_lookup_metadata_path(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        MetadataLookupRequest {
            group_id,
            document_path: query.path,
            auth,
        },
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_api_error)?;
    Ok((
        StatusCode::OK,
        Json(MetadataPathResponse {
            winner: MetadataDocumentSummary::from(&result.winner),
            conflicts: result.conflicts.iter().map(ToString::to_string).collect(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/metadata/{document_id}",
    tag = "metadata/documents",
    summary = "Get a metadata document summary",
    description = r#"Returns the registry summary of one metadata document.

**Authentication**: optional bearer token; a document that is not public is only returned to a
caller whose identity may read it, and existence is hidden, so an anonymous caller is never told
401 or 403 for a private document.

**Behavior**
- The read is routed to the document's current holders and answered by the first holder that
  returns a matching record.
- A record whose create was only just accepted may still answer 404 until it lands on a holder."#,
    params(("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list")),
    responses(
        (
            status = 200,
            description = "Registry summary; `replicas` counts the holders that currently carry the document",
            body = MetadataDocumentSummary,
            examples(
                (
                    "DocumentSummary" = (
                        summary = "Metadata document summary",
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
        (status = 400, description = "Document id that is not a structured metadata id, or an unusable bearer forwarding carrier", body = ErrorResponse),
        (status = 401, description = "A holder rejected the forwarded credential as unauthenticated", body = ErrorResponse),
        (status = 403, description = "A holder denied READ for this caller on the document", body = ErrorResponse),
        (status = 404, description = "The document does not exist or is not readable by the caller; the two cases are deliberately indistinguishable", body = ErrorResponse),
        (status = 503, description = "Placement view unreadable, no holder answered, or holders disagreed about the document; retryable", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let document_id = parse_document_id(&document_id)?;
    let ctx = state.get_ctx();
    let record = run_get_visible_metadata_document(
        &ctx,
        state.get_realm_id(),
        GetVisibleRequest { document_id, auth },
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_api_error)?;
    Ok((StatusCode::OK, Json(MetadataDocumentSummary::from(&record))))
}

#[utoipa::path(
    delete,
    path = "/metadata/{document_id}",
    tag = "metadata/documents",
    summary = "Delete a metadata document",
    description = r#"Deletes a metadata document and tombstones its RO-Crate graph.

**Authentication**: realm bearer token with WRITE on the document's permission path. A node that
does not hold the document forwards the delete to a holder that re-runs that check under the
caller's own token.

**Behavior**
- The delete itself is applied by the document's persistent-id authority.
- Acceptance is durable but asynchronous: the graph tombstone, the removal from listings, search
  and query results, and the pruning of remote replicas follow, so the document can still answer
  reads for a short window."#,
    params(("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list")),
    responses(
        (status = 204, description = "Delete durably accepted; tombstoning, listing and query visibility, and replica pruning may still be catching up"),
        (status = 400, description = "Document id that is not a structured metadata id, or an unusable bearer forwarding carrier", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token, or a holder rejected the forwarded credential", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or WRITE is denied on the document", body = ErrorResponse),
        (status = 404, description = "No holder knows this document", body = ErrorResponse),
        (status = 503, description = "Realm placement view unreadable, the document has no usable holder, or no holder accepted the forwarded delete; the delete was not accepted and may be retried", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let ctx = state.get_ctx();
    let record =
        local_write_record(&state, &auth, document_id, PolicyRequestExtras::rest()).await?;
    run_delete_metadata_document(
        &ctx,
        Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        record.as_ref(),
        document_id,
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_write_error)?;

    Ok(StatusCode::NO_CONTENT)
}
