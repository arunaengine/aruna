//! Metadata query and search routes: thin request-to-operation conversion over
//! the shared `crate::metadata` adapter.

use crate::auth::{ValidatedBearer, parse_group_id};
use crate::error::{ErrorResponse, ServerResult};
use crate::metadata::{
    MetadataQueryMode, MetadataQueryResponse, MetadataSearchParams, SearchResultsResponse,
    SparqlQueryRequest, bearer_token_string, map_api_error, map_query_mode, map_query_results,
    map_search_hit, parse_document_id,
};
use crate::server_state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::metadata::api::{
    DocumentQueryRequest, MetadataQueryRequest, MetadataSearchRequest,
    query_metadata as run_query_metadata, query_metadata_document as run_query_metadata_document,
    search_metadata as run_search_metadata,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::sync::Arc;
use std::time::Instant;

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/sparql/query",
    tag = "metadata/query",
    summary = "Run a SPARQL query against one metadata document",
    description = r#"Runs a SPARQL query against the graph of one metadata document.

**Authentication**: optional bearer token; the document is only queried for a caller whose
identity may read it, and an unknown document is indistinguishable from an unreadable one because
both answer 404.

**Behavior**
- In distributed mode the document's replicas are tried one after another until one returns a
  complete result, so a successful answer is never a merge of partial replies.
- `nodes_queried` counts the replica attempts made and `nodes_failed` only those that failed
  before a success.
- The queried graph reflects the last materialized revision, so a write that was only just
  accepted may not be visible yet.
- `kind` is `Solutions` with one object per row (variable name to lexical value, unbound
  variables omitted) for a `SELECT`, or `Boolean` for an `ASK`.
- `complete` is false when a selected partition is missing, and `failed_partitions` names the node
  ids that failed, plus `partition-discovery` when the realm view itself was unavailable.

**Limits**
- Only `SELECT` and `ASK` are accepted.
- The query text is limited to 64 KiB.
- A `LIMIT` above 10000 rows is rejected.
- `SERVICE` clauses are refused."#,
    params(("document_id" = String, Path, description = "Metadata document id, a structured document ULID as returned by create or list")),
    request_body(
        content = SparqlQueryRequest,
        description = "A SPARQL `SELECT` or `ASK` query over one metadata document. `mode=local` queries only the current node, while `mode=distributed` tries the document's registry replicas until one returns a complete result. `allow_partial` only permits local fallback when holder discovery is unavailable.",
        examples(
            (
                "DocumentAsk" = (
                    summary = "Check whether the document contains a dataset name",
                    value = json!({
                        "query": "ASK WHERE { ?dataset <http://schema.org/name> \"Public Dataset\" }"
                    })
                )
            ),
            (
                "DocumentSelectLocal" = (
                    summary = "Run a document-scoped query only on the current node",
                    value = json!({
                        "query": "SELECT ?file ?name WHERE { ?file a <http://schema.org/File> ; <http://schema.org/name> ?name . } LIMIT 10",
                        "mode": "local"
                    })
                )
            )
        )
    ),
    responses(
        (
            status = 200,
            description = "SPARQL query result for the document, with replica attempt counters and completeness flags",
            body = MetadataQueryResponse,
            examples(
                (
                    "Solutions" = (
                        summary = "SELECT answered by the first replica",
                        value = json!({
                            "kind": "Solutions",
                            "value": [
                                {
                                    "file": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE#run-42.raw",
                                    "name": "run-42.raw"
                                }
                            ],
                            "nodes_queried": 1,
                            "nodes_failed": 0,
                            "complete": true,
                            "failed_partitions": []
                        })
                    )
                ),
                (
                    "Boolean" = (
                        summary = "ASK answered after one replica failed",
                        value = json!({
                            "kind": "Boolean",
                            "value": true,
                            "nodes_queried": 2,
                            "nodes_failed": 0,
                            "complete": true,
                            "failed_partitions": []
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Malformed body, a document id that is not a structured metadata id, a query that is not SELECT or ASK, a query over the size or row limits, a SERVICE clause, or an unusable bearer forwarding carrier", body = ErrorResponse),
        (status = 401, description = "A replica rejected the forwarded credential as unauthenticated", body = ErrorResponse),
        (status = 403, description = "A replica denied READ for this caller on the document", body = ErrorResponse),
        (status = 404, description = "The document does not exist or is not readable by the caller; the two cases are deliberately indistinguishable", body = ErrorResponse),
        (status = 503, description = "The graph is not materialized yet, holder discovery failed while allow_partial was false, or every document replica failed; retryable", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn query_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
    Json(request): Json<SparqlQueryRequest>,
) -> ServerResult<(StatusCode, Json<MetadataQueryResponse>)> {
    let document_id = parse_document_id(&document_id)?;
    let ctx = state.get_ctx();
    let result = run_query_metadata_document(
        ctx.as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        DocumentQueryRequest {
            document_id,
            auth,
            bearer_token: bearer_token_string(bearer_token),
            query: request.query,
            mode: map_query_mode(request.mode),
            allow_partial: request.allow_partial,
        },
    )
    .await
    .map_err(map_api_error)?;
    let serialize_started = Instant::now();
    let response = map_query_results(result.results, result.fanout_stats)?;
    aruna_core::telemetry::record_stage("serialize", serialize_started.elapsed());
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    post,
    path = "/metadata/sparql/query",
    tag = "metadata/query",
    summary = "Run a SPARQL query across visible metadata",
    description = r#"Runs a SPARQL query over the metadata graph union the caller may read.

**Authentication**: optional bearer token; the queried graph union only contains metadata the
caller may read, so an anonymous request sees the public documents alone.

**Behavior**
- Distributed mode fans out to at most 32 realm node partitions, at most 8 of them concurrently,
  under an overall deadline of a few seconds.
- Partitions beyond that cap, partitions that fail, and a failed realm node discovery all count
  towards `nodes_failed` and make `complete` false; `failed_partitions` is then non-empty and the
  rows are a partial view of the realm.
- With `allow_partial=false` any missing partition turns into 503 instead of a partial answer.
- Results may be served from a short-lived cache keyed to the caller's credential, so a
  just-accepted write can be missing for a moment.
- `kind` is `Solutions` with one object per row (variable name to lexical value, unbound
  variables omitted) for a `SELECT`, or `Boolean` for an `ASK`.

**Limits**
- Only `SELECT` and `ASK` are accepted.
- The query text is limited to 64 KiB.
- A `LIMIT` above 10000 rows is rejected.
- `SERVICE` clauses are refused.
- A distributed query must be union-safe: only an `ASK` or a `SELECT DISTINCT` over a single
  pattern, without `OFFSET`, can be merged across partitions."#,
    request_body(
        content = SparqlQueryRequest,
        description = "A SPARQL `SELECT` or `ASK` query across all visible metadata. `mode=local` evaluates the current node's authorized graph union. `mode=distributed` accepts only union-safe `ASK` or `SELECT DISTINCT` single-pattern queries, merges partition sets, and is best-effort by default; set `allow_partial=false` to fail when a partition is unavailable.",
        examples(
            (
                "SelectDatasets" = (
                    summary = "List dataset names across visible metadata graphs",
                    value = json!({
                        "query": "SELECT DISTINCT ?dataset WHERE { ?dataset a <http://schema.org/Dataset> } LIMIT 25"
                    })
                )
            ),
            (
                "SelectDatasetsLocal" = (
                    summary = "List dataset names from the current node only",
                    value = json!({
                        "query": "SELECT ?dataset ?name WHERE { ?dataset a <http://schema.org/Dataset> ; <http://schema.org/name> ?name . } LIMIT 25",
                        "mode": "local"
                    })
                )
            )
        )
    ),
    responses(
        (
            status = 200,
            description = "SPARQL query result over the visible graph union, with partition counters and completeness flags",
            body = MetadataQueryResponse,
            examples(
                (
                    "CompleteSolutions" = (
                        summary = "Every partition answered",
                        value = json!({
                            "kind": "Solutions",
                            "value": [
                                { "dataset": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE" },
                                { "dataset": "https://w3id.org/aruna/01JMETADATA9876543210ZYXWV" }
                            ],
                            "nodes_queried": 3,
                            "nodes_failed": 0,
                            "complete": true,
                            "failed_partitions": []
                        })
                    )
                ),
                (
                    "PartialSolutions" = (
                        summary = "One partition failed, partial result returned",
                        value = json!({
                            "kind": "Solutions",
                            "value": [
                                { "dataset": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE" }
                            ],
                            "nodes_queried": 3,
                            "nodes_failed": 1,
                            "complete": false,
                            "failed_partitions": [
                                "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978"
                            ]
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Malformed body, a query that is not SELECT or ASK, a query over the size or row limits, a SERVICE clause, or a distributed query that is not union-safe", body = ErrorResponse),
        (status = 501, description = "Unsupported query mode; not reachable today because both local and distributed are supported", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn query_all_metadata(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Json(request): Json<SparqlQueryRequest>,
) -> ServerResult<(StatusCode, Json<MetadataQueryResponse>)> {
    let ctx = state.get_ctx();
    let result = run_query_metadata(
        ctx.as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        MetadataQueryRequest {
            auth,
            bearer_token: bearer_token_string(bearer_token),
            graph_iris: None,
            query: request.query,
            mode: map_query_mode(request.mode),
            target_nodes: None,
            allow_partial: request.allow_partial,
        },
    )
    .await
    .map_err(map_api_error)?;
    let serialize_started = Instant::now();
    let response = map_query_results(result.results, result.fanout_stats)?;
    aruna_core::telemetry::record_stage("serialize", serialize_started.elapsed());
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    get,
    path = "/metadata/search",
    tag = "metadata/query",
    summary = "Search visible metadata documents",
    description = r#"Searches the metadata documents the caller may read, by text or Profile.

**Authentication**: optional bearer token; hits are restricted to metadata the caller may read,
so an anonymous request returns fewer documents.

**Behavior**
- Distributed searches fan out to at most 32 realm node partitions, at most 8 of them
  concurrently, under an overall deadline of a few seconds.
- `nodes_failed` counts the partitions that failed, timed out or were dropped by that cap, and
  any non-zero value means the page is a partial view of the realm.
- Pagination is cursor-based and stops at a server-side depth of 1000 hits per node, which is
  reported as `truncated`; `next_cursor` is null once the results are exhausted.
- Hits come from each node's own index, so a document whose write was only just accepted may not
  be findable yet.
- One hit is one matched RDF subject, so a crate's root dataset and its file entities match
  separately; `subject_types` carries that subject's `rdf:type` IRIs to tell them apart.
- `conforms_to` matches the RO-Crate 1.2 and 1.3 specification IRIs and the RO-Crate community
  profiles under `https://w3id.org/ro/wfrun/` and
  `https://w3id.org/workflowhub/workflow-ro-crate/` exactly, without treating them as registered
  Profiles.
- A registered Profile uses only `https://w3id.org/aruna/profile/{id}`; other absolute IRIs are
  matched exactly, never as a prefix.

**Limits**
- Either `q` or `conforms_to` must be given.
- A cursor is bound to the original query.
- Hits are ordered by descending score and the page size is silently clamped to at most 100."#,
    params(
        ("q" = Option<String>, Query, description = "Free-text search query, matched against indexed literals. Optional when conforms_to is set"),
        ("conforms_to" = Option<String>, Query, description = "RO-Crate conformsTo specification or Profile IRI, matched exactly and never as a prefix"),
        ("group_id" = Option<String>, Query, description = "Restrict hits to a single group ULID"),
        ("limit" = Option<usize>, Query, description = "Page size (default 25, silently clamped to a maximum of 100)"),
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from a previous response's next_cursor. Paging is best-effort: results may shift under concurrent metadata churn or node failures"),
        ("mode" = Option<MetadataQueryMode>, Query, description = "Search mode: local or distributed. Distributed mode is best-effort and may return partial results if realm node discovery or remote requests fail")
    ),
    responses(
        (
            status = 200,
            description = "Search hits ordered by descending score, with the paging and partition flags",
            body = SearchResultsResponse,
            examples(
                (
                    "SearchPage" = (
                        summary = "First page with a continuation cursor",
                        value = json!({
                            "hits": [
                                {
                                    "document_id": "01JMETADATA0123456789ABCDE",
                                    "group_id": "01JABCDEF0123456789ABCDEFG",
                                    "document_path": "datasets/proteomics/run-42",
                                    "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                    "subject_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                    "score": 0.87,
                                    "title": "Proteomics Run 42",
                                    "snippet": "Metadata record for LC-MS run 42",
                                    "subject_types": ["http://schema.org/Dataset"]
                                },
                                {
                                    "document_id": "01JMETADATA0123456789ABCDE",
                                    "group_id": "01JABCDEF0123456789ABCDEFG",
                                    "document_path": "datasets/proteomics/run-42",
                                    "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                    "subject_iri": "./raw/run-42.mzML",
                                    "score": 0.52,
                                    "title": "run-42.mzML",
                                    "snippet": "LC-MS run 42 raw spectra",
                                    "subject_types": ["http://schema.org/MediaObject"]
                                }
                            ],
                            "next_cursor": "eyJmIjoiMDFKIn0.c2lnbmF0dXJl",
                            "nodes_queried": 3,
                            "nodes_failed": 0,
                            "truncated": false
                        })
                    )
                ),
                (
                    "PartialPage" = (
                        summary = "Last page with one unreachable partition",
                        value = json!({
                            "hits": [
                                {
                                    "document_id": "01JMETADATA9876543210ZYXWV",
                                    "group_id": "01JABCDEF0123456789ABCDEFG",
                                    "document_path": "profiles/proteomics-profile",
                                    "graph_iri": "https://w3id.org/aruna/01JMETADATA9876543210ZYXWV",
                                    "subject_iri": "https://w3id.org/aruna/01JMETADATA9876543210ZYXWV",
                                    "score": 0.41,
                                    "title": "Proteomics Profile",
                                    "snippet": null,
                                    "subject_types": ["http://schema.org/Dataset"]
                                }
                            ],
                            "next_cursor": null,
                            "nodes_queried": 3,
                            "nodes_failed": 1,
                            "truncated": false
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Neither q nor conforms_to given, a conforms_to or group_id that is not a valid IRI or ULID, or a cursor that is unreadable, unsigned by a realm node, or bound to a different query", body = ErrorResponse),
        (status = 501, description = "Unsupported query mode; not reachable today because both local and distributed are supported", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn search_metadata(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Query(params): Query<MetadataSearchParams>,
) -> ServerResult<(StatusCode, Json<SearchResultsResponse>)> {
    let group_id = params.group_id.as_deref().map(parse_group_id).transpose()?;
    let ctx = state.get_ctx();
    let result = run_search_metadata(
        ctx.as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        MetadataSearchRequest {
            auth,
            bearer_token: bearer_token_string(bearer_token),
            graph_iris: None,
            query: params.q,
            conforms_to: params.conforms_to,
            group_id,
            limit: params.limit,
            cursor: params.cursor,
            mode: map_query_mode(params.mode),
            target_nodes: None,
        },
    )
    .await
    .map_err(map_api_error)?;
    Ok((
        StatusCode::OK,
        Json(SearchResultsResponse {
            hits: result.hits.into_iter().map(map_search_hit).collect(),
            next_cursor: result.next_cursor,
            nodes_queried: result.fanout_stats.nodes_queried,
            nodes_failed: result.fanout_stats.nodes_failed,
            truncated: result.truncated,
        }),
    ))
}
