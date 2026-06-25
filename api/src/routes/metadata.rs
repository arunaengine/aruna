use crate::auth::{ValidatedArunaBearerTokenCarrier, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::errors::AuthorizationError;
use aruna_core::id::short_display_id;
use aruna_core::metadata::{
    MetadataError, MetadataQueryResults, MetadataRoCratePage, MetadataSearchHit,
};
use aruna_core::structs::{Actor, AuthContext, MetadataRegistryRecord, Permission};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, create_metadata_document as run_create_metadata_document,
};
use aruna_operations::delete_metadata_document::{
    DeleteMetadataDocumentOperation, delete_metadata_document as run_delete_metadata_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::{
    is_metadata_record_materialized_for_graph_read,
    load_metadata_record_by_document as load_metadata_record_by_document_from_operations,
};
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::list_metadata_documents::ListMetadataDocumentsOperation;
use aruna_operations::metadata::MetadataAuthToken;
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentError, UpdateMetadataDocumentMutation,
    UpdateMetadataDocumentOperation, update_metadata_document as run_update_metadata_document,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{Instrument, Span, debug_span, field, warn};
use ulid::Ulid;
use url::form_urlencoded::Serializer;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "metadata", description = "Metadata RO-Crate and SPARQL operations")),
    components(schemas(MetadataRoCrateView)),
    paths(
        create_metadata_document,
        list_all_metadata_documents,
        list_metadata_documents,
        get_metadata_document,
        delete_metadata_document,
        search_metadata,
        export_metadata_rocrate,
        replace_metadata_rocrate,
        add_metadata_data_entity,
        add_metadata_contextual_entity,
        query_metadata_document,
        query_all_metadata
    )
)]
pub struct MetadataApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route(
            "/metadata",
            get(list_all_metadata_documents).post(create_metadata_document),
        )
        .route("/metadata/search", get(search_metadata))
        .route("/metadata/sparql/query", post(query_all_metadata))
        .route("/groups/{group_id}/metadata", get(list_metadata_documents))
        .route(
            "/metadata/{document_id}",
            get(get_metadata_document).delete(delete_metadata_document),
        )
        .route(
            "/metadata/{document_id}/rocrate",
            get(export_metadata_rocrate).put(replace_metadata_rocrate),
        )
        .route(
            "/metadata/{document_id}/rocrate/data-entities",
            post(add_metadata_data_entity),
        )
        .route(
            "/metadata/{document_id}/rocrate/contextual-entities",
            post(add_metadata_contextual_entity),
        )
        .route(
            "/metadata/{document_id}/sparql/query",
            post(query_metadata_document),
        )
}

/// Public metadata registry summary.
///
/// When returned by create or update endpoints, this summary reflects a write
/// accepted into the durable event/projection pipeline. The graph, query/search
/// visibility, and remote replicas may still be catching up.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataDocumentSummary {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub public: bool,
    pub replicas: usize,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateMetadataScaffoldRequest {
    pub group_id: String,
    pub path: String,
    pub name: String,
    pub description: String,
    pub date_published: String,
    pub license: String,
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateMetadataRoCrateRequest {
    pub group_id: String,
    pub path: String,
    #[serde(default)]
    pub public: bool,
    #[schema(value_type = Object)]
    pub rocrate: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum CreateMetadataRequest {
    Scaffold(CreateMetadataScaffoldRequest),
    RoCrate(CreateMetadataRoCrateRequest),
}

/// Response for a metadata create request accepted into the durable
/// event/projection pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateMetadataResponse {
    /// Accepted registry summary. It does not guarantee the graph is fully
    /// materialized, queryable, searchable, or replicated yet.
    #[serde(flatten)]
    pub summary: MetadataDocumentSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListMetadataResponse {
    pub documents: Vec<MetadataDocumentListItem>,
    pub limit: usize,
    pub offset: usize,
    pub total_returned: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataDocumentListItem {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub public: bool,
    pub replicas: usize,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub rocrate_summary: Option<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListMetadataQuery {
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub include: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceMetadataRoCrateRequest {
    #[schema(value_type = Object)]
    pub rocrate: Value,
    #[serde(default)]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataRoCrateResponse {
    #[schema(value_type = Object)]
    pub rocrate: Value,
    pub total_data_entities: Option<usize>,
    pub returned_data_entities: Option<usize>,
    pub next_offset: Option<usize>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(value_type = Object)]
pub struct JsonLdObject(pub Value);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MetadataRoCrateView {
    Full,
    Summary,
    Page,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct MetadataRoCrateExportParams {
    #[serde(default)]
    pub view: Option<MetadataRoCrateView>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub after: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct MetadataSearchParams {
    pub q: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataSearchHitResponse {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub subject_iri: String,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataSearchResponse {
    pub hits: Vec<MetadataSearchHitResponse>,
    /// Number of node partitions this search was executed against.
    pub nodes_queried: usize,
    /// Number of node partitions that failed or timed out; a non-zero value
    /// means the result is partial.
    pub nodes_failed: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MetadataIncludeFlags {
    summary: bool,
}

const DEFAULT_LIST_METADATA_LIMIT: usize = 50;
const MAX_LIST_METADATA_LIMIT: usize = 1_000;
const METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT: usize = 8;
const METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SparqlQueryRequest {
    /// SPARQL query string. Only `SELECT` and `ASK` queries are supported.
    pub query: String,
    /// Query execution scope. Omit to use `distributed`.
    ///
    /// `local` runs only against metadata indexed on the current node.
    /// `distributed` fans out to all known realm nodes for all-metadata queries,
    /// or to the document's registry replica nodes for document-scoped queries,
    /// and merges the results.
    /// Distributed mode is best-effort and may return partial results if realm
    /// node discovery or remote requests fail.
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MetadataQueryMode {
    /// Run the query only on the current node.
    Local,
    /// Run the query across all known realm nodes and merge the results.
    /// This is best-effort and may return partial results if discovery or
    /// remote requests fail.
    Distributed,
}

#[derive(Debug)]
struct MetadataFanoutScope {
    mode: Option<MetadataQueryMode>,
    target_nodes: Option<Vec<aruna_core::NodeId>>,
}

impl MetadataFanoutScope {
    fn new(mode: Option<MetadataQueryMode>, target_nodes: Option<Vec<aruna_core::NodeId>>) -> Self {
        Self { mode, target_nodes }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataQueryResponse {
    #[serde(flatten)]
    pub result: MetadataQueryResult,
    /// Number of node partitions this query was executed against.
    pub nodes_queried: usize,
    /// Number of node partitions that failed or timed out; a non-zero value
    /// means the result is partial.
    pub nodes_failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", content = "value")]
pub enum MetadataQueryResult {
    Solutions(Vec<HashMap<String, String>>),
    Boolean(bool),
}

impl From<&MetadataRegistryRecord> for MetadataDocumentSummary {
    fn from(record: &MetadataRegistryRecord) -> Self {
        Self {
            document_id: record.document_id.to_string(),
            group_id: record.group_id.to_string(),
            document_path: record.document_path.clone(),
            graph_iri: record.graph_iri.clone(),
            public: record.public,
            replicas: record.holder_node_ids.len(),
            created_at: format_timestamp_ms(record.created_at_ms),
            updated_at: format_timestamp_ms(record.updated_at_ms),
        }
    }
}

impl MetadataDocumentListItem {
    fn from_record(record: &MetadataRegistryRecord, rocrate_summary: Option<Value>) -> Self {
        Self {
            document_id: record.document_id.to_string(),
            group_id: record.group_id.to_string(),
            document_path: record.document_path.clone(),
            graph_iri: record.graph_iri.clone(),
            public: record.public,
            replicas: record.holder_node_ids.len(),
            created_at: format_timestamp_ms(record.created_at_ms),
            updated_at: format_timestamp_ms(record.updated_at_ms),
            rocrate_summary,
        }
    }
}

#[utoipa::path(
    post,
    path = "/metadata",
    tag = "metadata",
    request_body(
        content = CreateMetadataRequest,
        description = "Create metadata either from scaffold fields or from a full RO-Crate JSON-LD object.",
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
            description = "Metadata create accepted into the durable event/projection pipeline. This does not guarantee the graph is fully materialized, queryable, searchable, or replicated yet.",
            body = CreateMetadataResponse,
            examples(
                (
                    "Created" = (
                        summary = "Created metadata summary",
                        value = json!({
                            "summary": {
                                "document_id": "01JMETADATA0123456789ABCDE",
                                "group_id": "01JABCDEF0123456789ABCDEFG",
                                "document_path": "datasets/proteomics/run-42",
                                "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                "public": true,
                                "replicas": 3,
                                "created_at": "2026-04-09T14:23:11.123Z",
                                "updated_at": "2026-04-09T14:23:11.123Z"
                            }
                        })
                    )
                )
            )
        ),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateMetadataRequest>,
) -> ServerResult<(StatusCode, Json<CreateMetadataResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let (group_id, path, public, payload) = match request {
        CreateMetadataRequest::Scaffold(request) => (
            parse_group_id(&request.group_id)?,
            request.path,
            request.public,
            CreateMetadataDocumentPayload::Scaffold {
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
            CreateMetadataDocumentPayload::RoCrate {
                jsonld: serialize_jsonld_object(&request.rocrate)?,
            },
        ),
    };
    if MetadataRegistryRecord::normalize_document_path(&path).is_empty() {
        return Err(ServerError::BadRequest);
    }
    ensure_metadata_write_scope(&state, &auth, group_id).await?;

    let ctx = state.get_ctx();
    let created = run_create_metadata_document(
        CreateMetadataDocumentOperation::new_for_generated_document_id(
            CreateMetadataDocumentConfig {
                actor: Actor {
                    node_id: state.get_node_id(),
                    user_id: auth.user_id,
                    realm_id: state.get_realm_id(),
                },
                group_id,
                document_id: Ulid::new(),
                document_path: path,
                public,
                payload,
            },
        ),
        ctx,
    )
    .await
    .map_err(map_create_metadata_error)?;
    let result = created.record;

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
    tag = "metadata",
    params(
        ("group_id" = Option<String>, Query, description = "Optional group id filter"),
        ("path_prefix" = Option<String>, Query, description = "Normalized metadata path prefix, for example profiles/"),
        ("include" = Option<String>, Query, description = "Comma-separated includes. Currently supports summary"),
        ("limit" = Option<usize>, Query, description = "Maximum documents to return"),
        ("offset" = Option<usize>, Query, description = "Number of filtered documents to skip")
    ),
    responses(
        (status = 200, description = "Visible metadata documents", body = ListMetadataResponse),
        (status = 400, description = "Invalid query", body = ErrorResponse)
    )
)]
pub async fn list_all_metadata_documents(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListMetadataQuery>,
) -> ServerResult<(StatusCode, Json<ListMetadataResponse>)> {
    if let Some(group_id) = query.group_id.as_deref() {
        let group_id = parse_group_id(group_id)?;
        let records = load_group_metadata_records(&state, group_id).await?;
        return Ok((
            StatusCode::OK,
            Json(build_metadata_list_response(&state, auth.as_ref(), records, &query).await?),
        ));
    }

    let groups = drive(ListGroupOperation::new(), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let mut records = Vec::new();
    for group in groups {
        records.extend(load_group_metadata_records(&state, group.group_id).await?);
    }

    Ok((
        StatusCode::OK,
        Json(build_metadata_list_response(&state, auth.as_ref(), records, &query).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/metadata",
    tag = "metadata",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("path_prefix" = Option<String>, Query, description = "Normalized metadata path prefix, for example profiles/"),
        ("include" = Option<String>, Query, description = "Comma-separated includes. Currently supports summary"),
        ("limit" = Option<usize>, Query, description = "Maximum documents to return"),
        ("offset" = Option<usize>, Query, description = "Number of filtered documents to skip")
    ),
    responses(
        (status = 200, description = "Visible metadata documents", body = ListMetadataResponse),
        (status = 400, description = "Invalid group id", body = ErrorResponse)
    )
)]
pub async fn list_metadata_documents(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Query(query): Query<ListMetadataQuery>,
) -> ServerResult<(StatusCode, Json<ListMetadataResponse>)> {
    let group_id = parse_group_id(&group_id)?;
    let records = load_group_metadata_records(&state, group_id).await?;

    Ok((
        StatusCode::OK,
        Json(build_metadata_list_response(&state, auth.as_ref(), records, &query).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/metadata/{document_id}",
    tag = "metadata",
    params(("document_id" = String, Path, description = "Metadata document id")),
    responses(
        (status = 200, description = "Metadata document summary", body = MetadataDocumentSummary),
        (status = 400, description = "Invalid id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    )
)]
pub async fn get_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let document_id = parse_document_id(&document_id)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_readable(&state, auth.as_ref(), &record).await?;
    ensure_record_materialized_for_graph_read(&state, &record).await?;
    Ok((StatusCode::OK, Json(MetadataDocumentSummary::from(&record))))
}

#[utoipa::path(
    delete,
    path = "/metadata/{document_id}",
    tag = "metadata",
    params(("document_id" = String, Path, description = "Metadata document id")),
    responses(
        (status = 204, description = "Metadata document deleted"),
        (status = 400, description = "Invalid id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_writable(&state, &auth, &record).await?;

    let ctx = state.get_ctx();
    run_delete_metadata_document(
        DeleteMetadataDocumentOperation::new(
            Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            record.group_id,
            document_id,
        ),
        ctx.as_ref(),
        document_id,
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/metadata/{document_id}/rocrate",
    tag = "metadata",
    params(
        ("document_id" = String, Path, description = "Metadata document id"),
        ("view" = Option<MetadataRoCrateView>, Query, description = "Export view: full, summary, or page"),
        ("limit" = Option<usize>, Query, description = "Maximum number of root-linked data entities for page view"),
        ("offset" = Option<usize>, Query, description = "Offset cursor for page view"),
        ("after" = Option<String>, Query, description = "Entity id cursor for page view")
    ),
    responses(
        (
            status = 200,
            description = "RO-Crate export",
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
                )
            )
        ),
        (status = 400, description = "Invalid id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    )
)]
pub async fn export_metadata_rocrate(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Query(params): Query<MetadataRoCrateExportParams>,
) -> ServerResult<(StatusCode, Json<MetadataRoCrateResponse>)> {
    let document_id = parse_document_id(&document_id)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_readable(&state, auth.as_ref(), &record).await?;
    ensure_record_materialized_for_graph_read(&state, &record).await?;
    let response = match params.view.clone().unwrap_or(MetadataRoCrateView::Full) {
        MetadataRoCrateView::Full => MetadataRoCrateResponse {
            rocrate: export_rocrate_jsonld(&state, &record.graph_iri).await?,
            total_data_entities: None,
            returned_data_entities: None,
            next_offset: None,
            next_cursor: None,
        },
        MetadataRoCrateView::Summary => MetadataRoCrateResponse {
            rocrate: rewrite_view_jsonld(
                export_rocrate_summary_jsonld(&state, &record.graph_iri).await?,
                &record.graph_iri,
                &build_view_id(&record.graph_iri, &params, MetadataRoCrateView::Summary),
            )?,
            total_data_entities: None,
            returned_data_entities: None,
            next_offset: None,
            next_cursor: None,
        },
        MetadataRoCrateView::Page => map_page_response(
            export_rocrate_page(&state, &record.graph_iri, &params).await?,
            &record.graph_iri,
            &build_view_id(&record.graph_iri, &params, MetadataRoCrateView::Page),
        )?,
    };
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    put,
    path = "/metadata/{document_id}/rocrate",
    tag = "metadata",
    params(("document_id" = String, Path, description = "Metadata document id")),
    request_body(
        content = ReplaceMetadataRoCrateRequest,
        description = "Replace the full RO-Crate document. Use the entity endpoints for small incremental changes.",
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
            description = "Metadata update accepted into the durable event/projection pipeline. This does not guarantee the graph is fully materialized, queryable, searchable, or replicated yet.",
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
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_metadata_rocrate(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Json(request): Json<ReplaceMetadataRoCrateRequest>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_writable(&state, &auth, &record).await?;

    let ctx = state.get_ctx();
    let updated = run_update_metadata_document(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            group_id: record.group_id,
            document_id,
            public: request.public.unwrap_or(record.public),
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: serialize_jsonld_object(&request.rocrate)?,
            },
        }),
        ctx.as_ref(),
    )
    .await
    .map_err(map_update_metadata_error)?;

    Ok((
        StatusCode::OK,
        Json(MetadataDocumentSummary::from(&updated)),
    ))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/rocrate/data-entities",
    tag = "metadata",
    params(("document_id" = String, Path, description = "Metadata document id")),
    request_body(
        content = inline(JsonLdObject),
        description = "Upsert one root-linked RO-Crate data entity as a JSON-LD object.",
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
            description = "Data entity upsert accepted into the durable event/projection pipeline. This does not guarantee the graph is fully materialized, queryable, searchable, or replicated yet.",
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
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_metadata_data_entity(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Json(entity): Json<Value>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_writable(&state, &auth, &record).await?;

    let ctx = state.get_ctx();
    let updated = run_update_metadata_document(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            group_id: record.group_id,
            document_id,
            public: record.public,
            mutation: UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: serialize_jsonld_entity(&entity)?,
            },
        }),
        ctx.as_ref(),
    )
    .await
    .map_err(map_update_metadata_error)?;

    Ok((
        StatusCode::OK,
        Json(MetadataDocumentSummary::from(&updated)),
    ))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/rocrate/contextual-entities",
    tag = "metadata",
    params(("document_id" = String, Path, description = "Metadata document id")),
    request_body(
        content = inline(JsonLdObject),
        description = "Upsert one RO-Crate contextual entity as a JSON-LD object.",
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
            description = "Contextual entity upsert accepted into the durable event/projection pipeline. This does not guarantee the graph is fully materialized, queryable, searchable, or replicated yet.",
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
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_metadata_contextual_entity(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Json(entity): Json<Value>,
) -> ServerResult<(StatusCode, Json<MetadataDocumentSummary>)> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_writable(&state, &auth, &record).await?;

    let ctx = state.get_ctx();
    let updated = run_update_metadata_document(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            group_id: record.group_id,
            document_id,
            public: record.public,
            mutation: UpdateMetadataDocumentMutation::UpsertContextualEntity {
                jsonld: serialize_jsonld_entity(&entity)?,
            },
        }),
        ctx.as_ref(),
    )
    .await
    .map_err(map_update_metadata_error)?;

    Ok((
        StatusCode::OK,
        Json(MetadataDocumentSummary::from(&updated)),
    ))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/sparql/query",
    tag = "metadata",
    params(("document_id" = String, Path, description = "Metadata document id")),
    request_body(
        content = SparqlQueryRequest,
        description = "Run a SPARQL `SELECT` or `ASK` query against one metadata document. `mode=local` only queries the current node, while `mode=distributed` queries the document's registry replica nodes and merges the results. Distributed mode is best-effort and may return partial results if replica requests fail. Omitting `mode` defaults to `distributed`.",
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
        (status = 200, description = "SPARQL query result", body = MetadataQueryResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    )
)]
pub async fn query_metadata_document(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(document_id): Path<String>,
    Json(request): Json<SparqlQueryRequest>,
) -> ServerResult<(StatusCode, Json<MetadataQueryResponse>)> {
    let document_id = parse_document_id(&document_id)?;
    ensure_supported_query_form(&request.query)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_readable(&state, auth.as_ref(), &record).await?;
    ensure_record_materialized_for_graph_read(&state, &record).await?;
    let (results, fanout_stats) = run_query_distributed(
        &state,
        auth,
        bearer_token,
        Some(vec![record.graph_iri.clone()]),
        request.query,
        MetadataFanoutScope::new(
            request.mode,
            Some(document_replica_query_nodes(&record, state.get_node_id())),
        ),
    )
    .await?;
    let serialize_started = Instant::now();
    let response = map_query_results(results, fanout_stats)?;
    aruna_core::telemetry::record_stage("serialize", serialize_started.elapsed());
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    post,
    path = "/metadata/sparql/query",
    tag = "metadata",
    request_body(
        content = SparqlQueryRequest,
        description = "Run a SPARQL `SELECT` or `ASK` query across all visible metadata. `mode=local` only queries the current node, while `mode=distributed` queries all known realm nodes and merges the results. Distributed mode is best-effort and may return partial results if realm node discovery or remote requests fail. Omitting `mode` defaults to `distributed`.",
        examples(
            (
                "SelectDatasets" = (
                    summary = "List dataset names across visible metadata graphs",
                    value = json!({
                        "query": "SELECT ?dataset ?name WHERE { ?dataset a <http://schema.org/Dataset> ; <http://schema.org/name> ?name . } LIMIT 25"
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
        (status = 200, description = "SPARQL query result", body = MetadataQueryResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 501, description = "Unsupported query mode", body = ErrorResponse)
    )
)]
pub async fn query_all_metadata(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<SparqlQueryRequest>,
) -> ServerResult<(StatusCode, Json<MetadataQueryResponse>)> {
    ensure_supported_query_form(&request.query)?;
    let (results, fanout_stats) = run_query_distributed(
        &state,
        auth,
        bearer_token,
        None,
        request.query,
        MetadataFanoutScope::new(request.mode, None),
    )
    .await?;
    let serialize_started = Instant::now();
    let response = map_query_results(results, fanout_stats)?;
    aruna_core::telemetry::record_stage("serialize", serialize_started.elapsed());
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    get,
    path = "/metadata/search",
    tag = "metadata",
    params(
        ("q" = String, Query, description = "Search query"),
        ("limit" = Option<usize>, Query, description = "Maximum number of hits"),
        ("mode" = Option<MetadataQueryMode>, Query, description = "Search mode: local or distributed. Distributed mode is best-effort and may return partial results if realm node discovery or remote requests fail")
    ),
    responses(
        (status = 200, description = "Metadata search hits", body = MetadataSearchResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 501, description = "Unsupported query mode", body = ErrorResponse)
    )
)]
pub async fn search_metadata(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Query(params): Query<MetadataSearchParams>,
) -> ServerResult<(StatusCode, Json<MetadataSearchResponse>)> {
    if params.q.trim().is_empty() {
        return Err(ServerError::BadRequest);
    }
    let limit = params.limit.unwrap_or(25).clamp(1, 250);
    let (hits, fanout_stats) = run_search_distributed(
        &state,
        auth,
        bearer_token,
        None,
        params.q,
        limit,
        MetadataFanoutScope::new(params.mode, None),
    )
    .await?;
    Ok((
        StatusCode::OK,
        Json(MetadataSearchResponse {
            hits: hits.into_iter().map(map_search_hit).collect(),
            nodes_queried: fanout_stats.nodes_queried,
            nodes_failed: fanout_stats.nodes_failed,
        }),
    ))
}

fn parse_document_id(document_id: &str) -> ServerResult<Ulid> {
    Ulid::from_string(document_id).map_err(|_| ServerError::BadRequest)
}

async fn load_group_metadata_records(
    state: &ServerState,
    group_id: Ulid,
) -> ServerResult<Vec<MetadataRegistryRecord>> {
    let ctx = state.get_ctx();
    // Listing is eventually consistent by design: the handle-owned visibility
    // cache is incrementally updated on local writes, serves stale snapshots
    // while a background refill runs, and is keyed per group so a small
    // group's listing stays independent of the realm-wide corpus size.
    if let Some(metadata_handle) = ctx.metadata_handle.as_ref() {
        match metadata_handle
            .list_cached_registry_records_for_group(group_id)
            .await
        {
            Ok(group_records) => return Ok(group_records.as_ref().clone()),
            Err(error) => {
                warn!(
                    error = %error,
                    "metadata registry cache fill failed, falling back to registry scan"
                );
            }
        }
    }

    drive(ListMetadataDocumentsOperation::new(group_id), &ctx)
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))
}

async fn build_metadata_list_response(
    state: &ServerState,
    auth: Option<&AuthContext>,
    records: Vec<MetadataRegistryRecord>,
    query: &ListMetadataQuery,
) -> ServerResult<ListMetadataResponse> {
    let include = parse_metadata_include_flags(query.include.as_deref())?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_METADATA_LIMIT)
        .clamp(1, MAX_LIST_METADATA_LIMIT);
    let offset = query.offset.unwrap_or(0);

    let needed = offset.saturating_add(limit);
    let mut selected = Vec::with_capacity(limit.min(records.len()));
    let mut visible_count = 0usize;
    for record in records {
        if !metadata_record_matches_filters(&record, query) {
            continue;
        }
        if !can_read_record(state, auth, &record).await? {
            continue;
        }
        visible_count += 1;
        if visible_count > offset {
            selected.push(record);
            if visible_count >= needed {
                break;
            }
        }
    }

    let mut documents = Vec::with_capacity(selected.len());
    if include.summary {
        let summaries = futures_util::future::join_all(
            selected
                .iter()
                .map(|record| export_rocrate_summary_jsonld(state, &record.graph_iri)),
        )
        .await;
        for (record, summary) in selected.iter().zip(summaries) {
            documents.push(MetadataDocumentListItem::from_record(
                record,
                Some(summary?),
            ));
        }
    } else {
        for record in &selected {
            documents.push(MetadataDocumentListItem::from_record(record, None));
        }
    }
    let total_returned = documents.len();

    Ok(ListMetadataResponse {
        documents,
        limit,
        offset,
        total_returned,
    })
}

fn metadata_record_matches_filters(
    record: &MetadataRegistryRecord,
    query: &ListMetadataQuery,
) -> bool {
    query
        .path_prefix
        .as_deref()
        .map(|path_prefix| metadata_path_matches_prefix(&record.document_path, path_prefix))
        .unwrap_or(true)
}

fn metadata_path_matches_prefix(document_path: &str, path_prefix: &str) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(document_path);
    let normalized_prefix = MetadataRegistryRecord::normalize_document_path(path_prefix);
    normalized_prefix.is_empty()
        || normalized_path == normalized_prefix
        || normalized_path
            .strip_prefix(&normalized_prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn parse_metadata_include_flags(include: Option<&str>) -> ServerResult<MetadataIncludeFlags> {
    let mut flags = MetadataIncludeFlags::default();
    let Some(include) = include else {
        return Ok(flags);
    };
    for value in include.split(',').map(str::trim) {
        if value.is_empty() {
            continue;
        }
        match value {
            "summary" => flags.summary = true,
            _ => return Err(ServerError::BadRequest),
        }
    }
    Ok(flags)
}

fn format_timestamp_ms(timestamp_ms: u64) -> String {
    i64::try_from(timestamp_ms)
        .ok()
        .and_then(|timestamp_ms| Utc.timestamp_millis_opt(timestamp_ms).single())
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

fn serialize_jsonld_object(value: &Value) -> ServerResult<String> {
    if !value.is_object() {
        return Err(ServerError::BadRequest);
    }
    serde_json::to_string(value).map_err(|_| ServerError::BadRequest)
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

fn map_create_metadata_error(error: CreateMetadataDocumentError) -> ServerError {
    match error {
        CreateMetadataDocumentError::MetadataError(metadata_error) => {
            map_metadata_error(metadata_error)
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_update_metadata_error(error: UpdateMetadataDocumentError) -> ServerError {
    match error {
        UpdateMetadataDocumentError::DocumentNotFound => ServerError::NotFound,
        UpdateMetadataDocumentError::MetadataError(metadata_error) => {
            map_metadata_error(metadata_error)
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_metadata_error(error: MetadataError) -> ServerError {
    match error {
        MetadataError::InvalidInput(_) => ServerError::BadRequest,
        MetadataError::GraphNotFound => ServerError::ServiceUnavailable,
        other => ServerError::InternalError(other.to_string()),
    }
}

// Pending graph materialization surfaces as GraphNotFound on read paths; the
// document is known to exist, so signal retry instead of failure.
fn map_metadata_event_error(error: MetadataError) -> ServerError {
    match error {
        MetadataError::GraphNotFound => ServerError::ServiceUnavailable,
        other => ServerError::InternalError(other.to_string()),
    }
}

async fn ensure_metadata_write_scope(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<()> {
    let path = format!("/{}/g/{group_id}/meta/**", state.get_realm_id());
    ensure_permission(state, auth.clone(), path, Permission::WRITE).await
}

async fn ensure_record_readable(
    state: &ServerState,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
) -> ServerResult<()> {
    if record.public {
        return Ok(());
    }
    let Some(auth) = auth.cloned() else {
        return Err(ServerError::Unauthorized);
    };
    ensure_permission(
        state,
        auth,
        record.permission_path.clone(),
        Permission::READ,
    )
    .await
}

async fn ensure_record_writable(
    state: &ServerState,
    auth: &AuthContext,
    record: &MetadataRegistryRecord,
) -> ServerResult<()> {
    ensure_permission(
        state,
        auth.clone(),
        record.permission_path.clone(),
        Permission::WRITE,
    )
    .await
}

async fn can_read_record(
    state: &ServerState,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
) -> ServerResult<bool> {
    if record.public {
        return Ok(true);
    }
    let Some(auth) = auth.cloned() else {
        return Ok(false);
    };
    if auth.realm_id != state.get_realm_id() {
        return Ok(false);
    }

    match aruna_core::telemetry::time_stage(
        "permission",
        drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth,
                path: record.permission_path.clone(),
                required_permission: Permission::READ,
            }),
            &state.get_ctx(),
        ),
    )
    .await
    {
        Ok(allowed) => Ok(allowed),
        Err(_) => Ok(false),
    }
}

async fn ensure_permission(
    state: &ServerState,
    auth: AuthContext,
    path: String,
    required_permission: Permission,
) -> ServerResult<()> {
    if auth.realm_id != state.get_realm_id() {
        return Err(ServerError::Forbidden);
    }
    let allowed = aruna_core::telemetry::time_stage(
        "permission",
        drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth,
                path,
                required_permission,
            }),
            &state.get_ctx(),
        ),
    )
    .await
    .map_err(|err| match err {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => ServerError::Forbidden,
        _ => ServerError::InternalError(err.to_string()),
    })?;
    if allowed {
        Ok(())
    } else {
        Err(ServerError::Forbidden)
    }
}

async fn load_metadata_record_by_document(
    state: &ServerState,
    document_id: Ulid,
) -> ServerResult<MetadataRegistryRecord> {
    let ctx = state.get_ctx();
    match load_metadata_record_by_document_from_operations(ctx.as_ref(), document_id).await {
        Ok(Some(record)) => Ok(record),
        Ok(None) => Err(ServerError::NotFound),
        Err(crate::routes::metadata::ReadError::Storage(error)) => {
            Err(ServerError::InternalError(error.to_string()))
        }
        Err(crate::routes::metadata::ReadError::Conversion(error)) => {
            Err(ServerError::InternalError(error.to_string()))
        }
    }
}

type ReadError = aruna_operations::metadata::repository::StorageReadError;

async fn ensure_record_materialized_for_graph_read(
    state: &ServerState,
    record: &MetadataRegistryRecord,
) -> ServerResult<()> {
    let ctx = state.get_ctx();
    match is_metadata_record_materialized_for_graph_read(ctx.as_ref(), record).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(ServerError::ServiceUnavailable),
        Err(ReadError::Storage(error)) => Err(ServerError::InternalError(error.to_string())),
        Err(ReadError::Conversion(error)) => Err(ServerError::InternalError(error.to_string())),
    }
}

async fn export_rocrate_jsonld(state: &ServerState, graph_iri: &str) -> ServerResult<Value> {
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_jsonld(graph_iri.to_string())
        .await
        .map_err(map_metadata_event_error)
        .and_then(parse_jsonld)
}

async fn export_rocrate_summary_jsonld(
    state: &ServerState,
    graph_iri: &str,
) -> ServerResult<Value> {
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_summary_jsonld(graph_iri.to_string())
        .await
        .map_err(map_metadata_event_error)
        .and_then(parse_jsonld)
}

async fn export_rocrate_page(
    state: &ServerState,
    graph_iri: &str,
    params: &MetadataRoCrateExportParams,
) -> ServerResult<MetadataRoCratePage> {
    if params.offset.is_some() && params.after.is_some() {
        return Err(ServerError::BadRequest);
    }
    let limit = params.limit.unwrap_or(100).clamp(1, 1_000);
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_page(
            graph_iri.to_string(),
            limit,
            params.offset,
            params.after.clone(),
        )
        .await
        .map_err(map_metadata_event_error)
}

fn map_page_response(
    page: MetadataRoCratePage,
    graph_iri: &str,
    view_id: &str,
) -> ServerResult<MetadataRoCrateResponse> {
    Ok(MetadataRoCrateResponse {
        rocrate: rewrite_view_jsonld(parse_jsonld(page.jsonld)?, graph_iri, view_id)?,
        total_data_entities: Some(page.total_data_entities),
        returned_data_entities: Some(page.returned_data_entities),
        next_offset: page.next_offset,
        next_cursor: page.next_cursor,
    })
}

fn build_view_id(
    graph_iri: &str,
    params: &MetadataRoCrateExportParams,
    view: MetadataRoCrateView,
) -> String {
    let mut serializer = Serializer::new(String::new());
    let view = match view {
        MetadataRoCrateView::Full => "full",
        MetadataRoCrateView::Summary => "summary",
        MetadataRoCrateView::Page => "page",
    };
    serializer.append_pair("view", view);
    if let Some(limit) = params.limit {
        serializer.append_pair("limit", &limit.to_string());
    }
    if let Some(offset) = params.offset {
        serializer.append_pair("offset", &offset.to_string());
    }
    if let Some(after) = params.after.as_deref() {
        serializer.append_pair("after", after);
    }
    let query = serializer.finish();
    if query.is_empty() {
        graph_iri.to_string()
    } else {
        format!("{graph_iri}?{query}")
    }
}

fn parse_jsonld(jsonld: String) -> ServerResult<Value> {
    serde_json::from_str(&jsonld)
        .map_err(|_| ServerError::InternalError("invalid jsonld export".to_string()))
}

fn rewrite_view_jsonld(mut value: Value, graph_iri: &str, view_id: &str) -> ServerResult<Value> {
    rewrite_identifier_value(&mut value, None, graph_iri, view_id);
    Ok(value)
}

fn rewrite_identifier_value(
    value: &mut serde_json::Value,
    key: Option<&str>,
    canonical_id: &str,
    replacement_id: &str,
) {
    match value {
        serde_json::Value::String(current)
            if matches!(key, Some("@id") | Some("id") | Some("about"))
                && current == canonical_id =>
        {
            *current = replacement_id.to_string();
        }
        serde_json::Value::Array(values) => {
            for entry in values {
                rewrite_identifier_value(entry, key, canonical_id, replacement_id);
            }
        }
        serde_json::Value::Object(object) => {
            for (child_key, child_value) in object.iter_mut() {
                rewrite_identifier_value(
                    child_value,
                    Some(child_key),
                    canonical_id,
                    replacement_id,
                );
            }
        }
        _ => {}
    }
}

fn ensure_supported_query_mode(mode: &Option<MetadataQueryMode>) -> ServerResult<()> {
    match mode {
        None | Some(MetadataQueryMode::Local) | Some(MetadataQueryMode::Distributed) => Ok(()),
    }
}

fn ensure_supported_query_form(query: &str) -> ServerResult<()> {
    match query_form(query) {
        Some(QueryForm::Select | QueryForm::Ask) => Ok(()),
        _ => Err(ServerError::BadRequest),
    }
}

fn map_query_results(
    results: MetadataQueryResults,
    fanout_stats: MetadataFanoutStats,
) -> ServerResult<MetadataQueryResponse> {
    let result = match results {
        MetadataQueryResults::Solutions(rows) => MetadataQueryResult::Solutions(
            rows.into_iter()
                .map(|row| row.into_iter().collect::<HashMap<_, _>>())
                .collect(),
        ),
        MetadataQueryResults::Boolean(value) => MetadataQueryResult::Boolean(value),
        MetadataQueryResults::Graph(_) => return Err(ServerError::BadRequest),
    };
    Ok(MetadataQueryResponse {
        result,
        nodes_queried: fanout_stats.nodes_queried,
        nodes_failed: fanout_stats.nodes_failed,
    })
}

fn metadata_query_result_kind(results: &MetadataQueryResults) -> &'static str {
    match results {
        MetadataQueryResults::Solutions(_) => "solutions",
        MetadataQueryResults::Boolean(_) => "boolean",
        MetadataQueryResults::Graph(_) => "graph",
    }
}

async fn load_realm_nodes(state: &ServerState) -> ServerResult<Vec<aruna_core::NodeId>> {
    Ok(state.load_metadata_realm_nodes().await)
}

fn document_replica_query_nodes(
    record: &MetadataRegistryRecord,
    local_node_id: aruna_core::NodeId,
) -> Vec<aruna_core::NodeId> {
    let nodes = deduplicate_fanout_nodes(record.holder_node_ids.clone());
    if nodes.is_empty() {
        vec![local_node_id]
    } else {
        nodes
    }
}

fn deduplicate_fanout_nodes(nodes: Vec<aruna_core::NodeId>) -> Vec<aruna_core::NodeId> {
    let mut seen = HashSet::with_capacity(nodes.len());
    nodes
        .into_iter()
        .filter(|node_id| seen.insert(*node_id))
        .collect()
}

fn metadata_auth_token_from_carrier(
    carrier: Option<&ValidatedArunaBearerTokenCarrier>,
) -> Option<MetadataAuthToken> {
    carrier.and_then(|carrier| MetadataAuthToken::bearer(carrier.as_str()).ok())
}

type MetadataNodeCall<T> =
    Arc<dyn Fn(aruna_core::NodeId) -> BoxFuture<'static, Result<T, MetadataError>> + Send + Sync>;

fn metadata_node_call<C, T, F, Fut>(context: C, call: F) -> MetadataNodeCall<T>
where
    C: Clone + Send + Sync + 'static,
    T: Send + 'static,
    F: Fn(C, aruna_core::NodeId) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, MetadataError>> + Send + 'static,
{
    Arc::new(move |node_id| {
        let context = context.clone();
        call(context, node_id).boxed()
    })
}

#[derive(Clone, Copy)]
enum MetadataFanoutOperation {
    Query,
    Search,
}

impl MetadataFanoutOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Search => "search",
        }
    }

    fn timeout_error(self) -> MetadataError {
        MetadataError::Backend(format!(
            "distributed metadata {} node timed out after {}ms",
            self.label(),
            METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT.as_millis()
        ))
    }
}

fn metadata_fanout_node_span(
    operation: MetadataFanoutOperation,
    node_id: aruna_core::NodeId,
    local: bool,
) -> Span {
    match operation {
        MetadataFanoutOperation::Query => debug_span!(
            "metadata.api.query_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::Search => debug_span!(
            "metadata.api.search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
    }
}

async fn run_metadata_fanout_node<T>(
    operation: MetadataFanoutOperation,
    node_id: aruna_core::NodeId,
    local: bool,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataError>),
    record_stage_detail: bool,
) -> Result<T, MetadataError> {
    let node_span = metadata_fanout_node_span(operation, node_id, local);
    let node_started = Instant::now();
    // The coordinator's own partition runs in-process like mode=local; only
    // remote partitions go over the wire and carry the per-node timeout.
    let result = if local {
        local_call(node_id).instrument(node_span.clone()).await
    } else {
        match tokio::time::timeout(
            METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT,
            remote_call(node_id).instrument(node_span.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(operation.timeout_error()),
        }
    };
    let elapsed = record_elapsed_ms(&node_span, "elapsed_ms", node_started);
    if record_stage_detail {
        aruna_core::telemetry::record_stage_detail(
            "fanout_node",
            || short_display_id(node_id),
            elapsed,
        );
    }
    record_result(&node_span, &result);
    result
}

async fn metadata_fanout_nodes(
    state: &ServerState,
    span: &Span,
    target_nodes: Option<Vec<aruna_core::NodeId>>,
) -> ServerResult<Vec<aruna_core::NodeId>> {
    match target_nodes {
        Some(nodes) => {
            span.record("discovery_ms", 0u64);
            Ok(deduplicate_fanout_nodes(nodes))
        }
        None => {
            let discovery_started = Instant::now();
            let nodes =
                aruna_core::telemetry::time_stage("discovery", load_realm_nodes(state)).await?;
            record_elapsed_ms(span, "discovery_ms", discovery_started);
            Ok(nodes)
        }
    }
}

async fn run_metadata_fanout<T>(
    state: &ServerState,
    scope: MetadataFanoutScope,
    operation: MetadataFanoutOperation,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataError>),
    map_local_error: fn(MetadataError) -> ServerError,
) -> ServerResult<(Vec<T>, MetadataFanoutStats)>
where
    T: Send + 'static,
{
    let span = Span::current();
    let MetadataFanoutScope { mode, target_nodes } = scope;
    ensure_supported_query_mode(&mode)?;
    match mode.unwrap_or(MetadataQueryMode::Distributed) {
        MetadataQueryMode::Local => {
            let local_node_id = state.get_node_id();
            let result = run_metadata_fanout_node(
                operation,
                local_node_id,
                true,
                local_call,
                remote_call,
                record_result,
                false,
            )
            .await;
            let fanout_stats = MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 0,
            };
            match result {
                Ok(result) => Ok((vec![result], fanout_stats)),
                Err(error) => Err(map_local_error(error)),
            }
        }
        MetadataQueryMode::Distributed => {
            let nodes = metadata_fanout_nodes(state, &span, target_nodes).await?;
            span.record("node_count", nodes.len() as u64);
            let mut fanout_stats = MetadataFanoutStats {
                nodes_queried: nodes.len(),
                nodes_failed: 0,
            };
            let fanout_started = Instant::now();
            let local_node_id = state.get_node_id();
            let mut node_parts = Vec::new();

            let pending =
                stream::iter(nodes.into_iter().enumerate().map(|(node_index, node_id)| {
                    let local_call = local_call.clone();
                    let remote_call = remote_call.clone();
                    async move {
                        let result = run_metadata_fanout_node(
                            operation,
                            node_id,
                            node_id == local_node_id,
                            local_call,
                            remote_call,
                            record_result,
                            true,
                        )
                        .await;
                        (node_index, node_id, result)
                    }
                }))
                .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT);
            futures_util::pin_mut!(pending);

            while let Some((node_index, node_id, result)) = pending.next().await {
                match result {
                    Ok(result) => node_parts.push((node_index, result)),
                    Err(error) => {
                        fanout_stats.nodes_failed += 1;
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            error = %error,
                            "distributed metadata skipped failed node result"
                        );
                    }
                }
            }

            node_parts.sort_by_key(|(node_index, _)| *node_index);
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
            Ok((
                node_parts.into_iter().map(|(_, result)| result).collect(),
                fanout_stats,
            ))
        }
    }
}

fn record_query_node_result(span: &Span, result: &Result<MetadataQueryResults, MetadataError>) {
    match result {
        Ok(result) => {
            span.record("result", metadata_query_result_kind(result));
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_search_node_result(span: &Span, result: &Result<Vec<MetadataSearchHit>, MetadataError>) {
    match result {
        Ok(hits) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn map_metadata_internal_error(error: MetadataError) -> ServerError {
    ServerError::InternalError(error.to_string())
}

#[tracing::instrument(
    name = "metadata.api.query_distributed",
    level = "debug",
    skip(state, auth, query, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
    )
)]
async fn run_query_distributed(
    state: &ServerState,
    auth: Option<AuthContext>,
    bearer_token: Option<ValidatedArunaBearerTokenCarrier>,
    graph_iris: Option<Vec<String>>,
    query: String,
    scope: MetadataFanoutScope,
) -> ServerResult<(MetadataQueryResults, MetadataFanoutStats)> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    let query_form = query_form(&query).ok_or(ServerError::BadRequest)?;
    let select_limit = match query_form {
        QueryForm::Select => query_select_limit(&query),
        QueryForm::Ask => None,
    };
    let remote_auth_token = metadata_auth_token_from_carrier(bearer_token.as_ref());

    let local_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
        ),
        |(handle, auth, graph_iris, query), _| async move {
            handle.query_authorized_local(auth, graph_iris, query).await
        },
    );
    let remote_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
        ),
        |(handle, auth_token, graph_iris, query), node_id| async move {
            handle
                .request_remote_query_graphs(node_id, auth_token, graph_iris, query)
                .await
        },
    );
    let (parts, fanout_stats) = run_metadata_fanout(
        state,
        scope,
        MetadataFanoutOperation::Query,
        local_call,
        remote_call,
        record_query_node_result,
        map_metadata_event_error,
    )
    .await?;

    let result = aggregate_query_results(parts, query_form, select_limit);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    match &result {
        Ok(results) => {
            span.record("result", metadata_query_result_kind(results));
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
    result.map(|results| (results, fanout_stats))
}

#[tracing::instrument(
    name = "metadata.api.search_distributed",
    level = "debug",
    skip(state, auth, query, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        limit = limit as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        hit_count = field::Empty,
    )
)]
async fn run_search_distributed(
    state: &ServerState,
    auth: Option<AuthContext>,
    bearer_token: Option<ValidatedArunaBearerTokenCarrier>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    scope: MetadataFanoutScope,
) -> ServerResult<(Vec<MetadataSearchHit>, MetadataFanoutStats)> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    let remote_auth_token = metadata_auth_token_from_carrier(bearer_token.as_ref());

    let local_call: MetadataNodeCall<Vec<MetadataSearchHit>> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
            limit,
        ),
        |(handle, auth, graph_iris, query, limit), _| async move {
            handle
                .search_authorized_local(auth, graph_iris, query, limit)
                .await
        },
    );
    let remote_call: MetadataNodeCall<Vec<MetadataSearchHit>> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
            limit,
        ),
        |(handle, auth_token, graph_iris, query, limit), node_id| async move {
            handle
                .request_remote_search_graphs(node_id, auth_token, graph_iris, query, limit)
                .await
        },
    );
    let (node_hits, fanout_stats) = run_metadata_fanout(
        state,
        scope,
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        record_search_node_result,
        map_metadata_internal_error,
    )
    .await?;

    let hits = deduplicate_search_hits(node_hits.into_iter().flatten().collect(), limit);
    span.record("hit_count", hits.len() as u64);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok((hits, fanout_stats))
}

fn aggregate_query_results(
    results: Vec<MetadataQueryResults>,
    query_form: QueryForm,
    select_limit: Option<usize>,
) -> ServerResult<MetadataQueryResults> {
    match query_form {
        QueryForm::Ask => {
            Ok(MetadataQueryResults::Boolean(results.into_iter().any(
                |result| matches!(result, MetadataQueryResults::Boolean(true)),
            )))
        }
        QueryForm::Select => {
            let mut seen = HashSet::new();
            let mut merged = Vec::new();
            for result in results {
                let MetadataQueryResults::Solutions(rows) = result else {
                    continue;
                };
                for row in rows {
                    let key = serde_json::to_string(&row)
                        .map_err(|err| ServerError::InternalError(err.to_string()))?;
                    if seen.insert(key) {
                        merged.push(row);
                    }
                }
            }
            // Each node applies the query LIMIT independently, so the merged
            // set can hold up to nodes x LIMIT rows; re-apply it after dedup.
            if let Some(limit) = select_limit {
                merged.truncate(limit);
            }
            Ok(MetadataQueryResults::Solutions(merged))
        }
    }
}

// Reads the outermost LIMIT of a SELECT query so distributed aggregation can
// re-apply it; sub-select slices sit deeper in the algebra and are not picked
// up here.
fn query_select_limit(query: &str) -> Option<usize> {
    let parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select { pattern, .. } = parsed else {
        return None;
    };
    let spargebra::algebra::GraphPattern::Slice { length, .. } = pattern else {
        return None;
    };
    length
}

fn deduplicate_search_hits(hits: Vec<MetadataSearchHit>, limit: usize) -> Vec<MetadataSearchHit> {
    let mut deduped = HashMap::new();
    for hit in hits {
        let key = (hit.graph_iri.clone(), hit.subject_iri.clone());
        deduped
            .entry(key)
            .and_modify(|existing: &mut MetadataSearchHit| {
                if hit.score > existing.score {
                    *existing = hit.clone();
                }
            })
            .or_insert(hit);
    }
    let mut hits = deduped.into_values().collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.graph_iri.cmp(&right.graph_iri))
            .then_with(|| left.subject_iri.cmp(&right.subject_iri))
    });
    hits.truncate(limit);
    hits
}

fn map_search_hit(hit: MetadataSearchHit) -> MetadataSearchHitResponse {
    MetadataSearchHitResponse {
        document_id: hit.document_id,
        group_id: hit.group_id,
        document_path: hit.document_path,
        graph_iri: hit.graph_iri,
        subject_iri: hit.subject_iri,
        score: hit.score,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryForm {
    Select,
    Ask,
}

#[derive(Debug, Clone, Copy, Default)]
struct MetadataFanoutStats {
    nodes_queried: usize,
    nodes_failed: usize,
}

fn query_form(query: &str) -> Option<QueryForm> {
    let mut remaining = query.trim_start();
    loop {
        let trimmed = remaining.trim_start();
        if trimmed.is_empty() {
            return None;
        }
        if let Some(rest) = trimmed.strip_prefix('#') {
            remaining = rest.split_once('\n').map(|(_, tail)| tail).unwrap_or("");
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("PREFIX ") || upper.starts_with("BASE ") {
            remaining = trimmed.split_once('\n').map(|(_, tail)| tail).unwrap_or("");
            continue;
        }
        if upper.starts_with("SELECT") {
            return Some(QueryForm::Select);
        }
        if upper.starts_with("ASK") {
            return Some(QueryForm::Ask);
        }
        return None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, TASK_TIMER_KEYSPACE,
    };
    use aruna_core::metadata::{
        MetadataDocumentDeleteRecord, MetadataDocumentLifecycleRecord, MetadataGraphLifecycleRecord,
    };
    use aruna_core::storage_entries::metadata_registry_delete_entries;
    use aruna_core::structs::{
        Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument,
        RealmConfigDocument, RealmId, RealmNodeKind, TokenClaims,
    };
    use aruna_core::task::{PersistedTaskTimer, TaskKey};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::announce_realm_presence::{
        AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::incoming::initialize_net_incoming;
    use aruna_operations::metadata::MetadataHandle;
    use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
    use aruna_operations::metadata::projector::{
        drain_pending_metadata_projection_queue, replay_metadata_event_log,
        schedule_pending_metadata_projection_drain,
    };
    use aruna_operations::metadata::prune_queue::{
        metadata_graph_prune_jobs_exist, process_metadata_graph_tombstones,
    };
    use aruna_operations::metadata::repository::{
        write_document_lifecycle_effect, write_graph_lifecycle_effect,
    };
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde_json::json;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    struct TestState {
        _storage_dir: TempDir,
        _metadata_dir: TempDir,
        auth: AuthContext,
        group_id: Ulid,
        state: Arc<ServerState>,
    }

    #[tokio::test]
    async fn public_metadata_routes_support_create_list_export_and_query() {
        let test = setup_state().await;

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/public-dataset".to_string(),
                    name: "Public Dataset".to_string(),
                    description: "Visible metadata".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();

        let document_id = created.summary.document_id.clone();

        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
            Query(ListMetadataQuery::default()),
        )
        .await
        .unwrap();
        assert!(listed.documents.is_empty());

        let fetched = get_metadata_document(
            State(test.state.clone()),
            Extension(None),
            Path(document_id.clone()),
        )
        .await;
        assert!(matches!(fetched, Err(ServerError::NotFound)));

        drain_metadata_background(test.state.as_ref()).await;
        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
            Query(ListMetadataQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(listed.documents.len(), 1);
        assert_eq!(listed.documents[0].document_id, created.summary.document_id);

        let paged_jsonld = format!(
            r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "Public Dataset",
      "description": "Visible metadata",
      "datePublished": "2026-01-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}},
      "hasPart": [
        {{"@id": "./data/file-0.txt"}},
        {{"@id": "./data/file-1.txt"}},
        {{"@id": "./data/file-2.txt"}}
      ]
    }},
    {{
      "@id": "./data/file-0.txt",
      "@type": "File",
      "name": "file-0"
    }},
    {{
      "@id": "./data/file-1.txt",
      "@type": "File",
      "name": "file-1"
    }},
    {{
      "@id": "./data/file-2.txt",
      "@type": "File",
      "name": "file-2"
    }}
  ]
}}"#
        );

        let _ = replace_metadata_rocrate(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(document_id.clone()),
            Json(ReplaceMetadataRoCrateRequest {
                rocrate: serde_json::from_str(&paged_jsonld).unwrap(),
                public: Some(true),
            }),
        )
        .await
        .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        let (_, Json(response)) = export_metadata_rocrate(
            State(test.state.clone()),
            Extension(None),
            Path(document_id.clone()),
            Query(MetadataRoCrateExportParams::default()),
        )
        .await
        .unwrap();
        assert!(
            response
                .rocrate
                .to_string()
                .contains(&format!("https://w3id.org/aruna/{document_id}"))
        );

        let (_, Json(summary)) = export_metadata_rocrate(
            State(test.state.clone()),
            Extension(None),
            Path(document_id.clone()),
            Query(MetadataRoCrateExportParams {
                view: Some(MetadataRoCrateView::Summary),
                limit: None,
                offset: None,
                after: None,
            }),
        )
        .await
        .unwrap();
        assert!(summary.rocrate.to_string().contains(&format!(
            "https://w3id.org/aruna/{document_id}?view=summary"
        )));
        assert!(!summary.rocrate.to_string().contains("file-0.txt"));

        let (_, Json(page)) = export_metadata_rocrate(
            State(test.state.clone()),
            Extension(None),
            Path(document_id.clone()),
            Query(MetadataRoCrateExportParams {
                view: Some(MetadataRoCrateView::Page),
                limit: Some(2),
                offset: Some(0),
                after: None,
            }),
        )
        .await
        .unwrap();
        assert!(page.rocrate.to_string().contains(&format!(
            "https://w3id.org/aruna/{document_id}?view=page&limit=2&offset=0"
        )));
        assert_eq!(page.total_data_entities, Some(3));
        assert_eq!(page.returned_data_entities, Some(2));
        assert_eq!(page.next_offset, Some(2));
        assert!(page.next_cursor.is_some());
        assert!(
            page.rocrate.to_string().contains("file-0.txt")
                || page.rocrate.to_string().contains("file-1.txt")
        );

        let (_, Json(result)) = query_metadata_document(
            State(test.state.clone()),
            Extension(None),
            Extension(None),
            Path(document_id.clone()),
            Json(SparqlQueryRequest {
                query: "ASK WHERE { ?s <http://schema.org/name> \"Public Dataset\" }".to_string(),
                mode: None,
            }),
        )
        .await
        .unwrap();
        assert!(matches!(result.result, MetadataQueryResult::Boolean(true)));
        assert_eq!(result.nodes_queried, 1);
        assert_eq!(result.nodes_failed, 0);

        test.state
            .get_ctx()
            .metadata_handle
            .as_ref()
            .unwrap()
            .flush_search_updates()
            .await
            .unwrap();

        let (_, Json(search)) = search_metadata(
            State(test.state.clone()),
            Extension(None),
            Extension(None),
            Query(MetadataSearchParams {
                q: "Public".to_string(),
                limit: Some(10),
                mode: None,
            }),
        )
        .await
        .unwrap();
        assert!(!search.hits.is_empty());
        assert_eq!(search.nodes_queried, 1);
        assert_eq!(search.nodes_failed, 0);
    }

    #[tokio::test]
    async fn metadata_routes_support_rocrate_create_and_entity_upserts() {
        let test = setup_state().await;

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::RoCrate(
                CreateMetadataRoCrateRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/rocrate-dataset".to_string(),
                    public: true,
                    rocrate: json!({
                        "@context": "https://w3id.org/ro/crate/1.2/context",
                        "@graph": [
                            {
                                "@id": "ro-crate-metadata.json",
                                "@type": "CreativeWork",
                                "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                                "about": { "@id": "urn:dataset:rocrate-create" }
                            },
                            {
                                "@id": "urn:dataset:rocrate-create",
                                "@type": "Dataset",
                                "name": "Created From RO-Crate",
                                "description": "Created from inline JSON-LD",
                                "datePublished": "2026-01-01",
                                "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" }
                            }
                        ]
                    }),
                },
            )),
        )
        .await
        .unwrap();

        let document_id = created.summary.document_id.clone();
        assert_eq!(created.summary.document_path, "datasets/rocrate-dataset");
        assert!(created.summary.created_at.ends_with('Z'));
        assert!(created.summary.updated_at.ends_with('Z'));
        drain_metadata_background(test.state.as_ref()).await;

        let _ = add_metadata_contextual_entity(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(document_id.clone()),
            Json(json!({
                "@id": "#person-ada",
                "@type": "Person",
                "name": "Ada Lovelace"
            })),
        )
        .await
        .unwrap();

        let _ = add_metadata_data_entity(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(document_id.clone()),
            Json(json!({
                "@id": "./data/run-42.raw",
                "@type": "File",
                "name": "run-42.raw",
                "creator": { "@id": "#person-ada" }
            })),
        )
        .await
        .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        let (_, Json(exported)) = export_metadata_rocrate(
            State(test.state),
            Extension(None),
            Path(document_id.clone()),
            Query(MetadataRoCrateExportParams::default()),
        )
        .await
        .unwrap();

        let json = exported.rocrate.to_string();
        assert!(json.contains(&format!("https://w3id.org/aruna/{document_id}")));
        assert!(json.contains("Created From RO-Crate"));
        assert!(json.contains("Ada Lovelace"));
        assert!(json.contains("run-42.raw"));
    }

    #[tokio::test]
    async fn list_metadata_documents_serves_records_from_handle_registry_cache() {
        let test = setup_state().await;
        let ctx = test.state.get_ctx();
        ctx.metadata_handle
            .as_ref()
            .expect("metadata handle installed")
            .expire_visibility_caches();

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/cache-served".to_string(),
                    name: "Cache Served Dataset".to_string(),
                    description: "Served from the handle registry cache".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        ctx.metadata_handle
            .as_ref()
            .expect("metadata handle installed")
            .expire_visibility_caches();

        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
            Query(ListMetadataQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(listed.documents.len(), 1);
        assert_eq!(listed.documents[0].document_id, created.summary.document_id);

        let status = delete_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(created.summary.document_id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
            Query(ListMetadataQuery::default()),
        )
        .await
        .unwrap();
        assert!(listed.documents.is_empty());
    }

    #[tokio::test]
    async fn inbound_document_lifecycle_tombstone_hides_stale_registry_listing() {
        let test = setup_state().await;
        let ctx = test.state.get_ctx();
        ctx.metadata_handle
            .as_ref()
            .expect("metadata handle installed")
            .expire_visibility_caches();

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/inbound-tombstone".to_string(),
                    name: "Inbound Tombstone Dataset".to_string(),
                    description: "Deleted by document lifecycle only".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        let document_id = parse_document_id(&created.summary.document_id).unwrap();
        let record = load_metadata_record_by_document(test.state.as_ref(), document_id)
            .await
            .unwrap();
        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
            Query(ListMetadataQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(listed.documents.len(), 1);

        let tombstone = MetadataGraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        );
        let lifecycle = MetadataDocumentLifecycleRecord::Delete {
            event: MetadataDocumentDeleteRecord {
                event_id: Ulid::new(),
                tombstone: tombstone.clone(),
                deleted_after_event_id: record.last_event_id,
            },
        };
        for effect in [
            write_graph_lifecycle_effect(&tombstone, None).unwrap(),
            write_document_lifecycle_effect(&lifecycle, None).unwrap(),
        ] {
            match ctx.storage_handle.send_effect(effect).await {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected lifecycle write event: {other:?}"),
            }
        }
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes: metadata_registry_delete_entries(record.group_id, record.document_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            other => panic!("unexpected registry delete event: {other:?}"),
        }

        let processed = process_metadata_graph_tombstones(ctx.as_ref(), vec![tombstone]).await;

        assert_eq!(processed.enqueued, 1);
        assert!(
            metadata_graph_prune_jobs_exist(&ctx.storage_handle)
                .await
                .unwrap()
        );
        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
            Query(ListMetadataQuery::default()),
        )
        .await
        .unwrap();
        assert!(listed.documents.is_empty());
        let fetched = get_metadata_document(
            State(test.state.clone()),
            Extension(None),
            Path(created.summary.document_id),
        )
        .await;
        assert!(matches!(fetched, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn pending_projection_drain_timer_is_persisted() {
        let test = setup_state().await;
        let ctx = test.state.get_ctx();

        schedule_pending_metadata_projection_drain(ctx.as_ref(), Duration::ZERO)
            .await
            .expect("projection drain scheduled");

        let timer = read_persisted_task_timer(ctx.as_ref(), &TaskKey::DrainMetadataProjectionQueue)
            .await
            .expect("projection drain timer persisted");
        assert_eq!(timer.key, TaskKey::DrainMetadataProjectionQueue);
    }

    #[tokio::test]
    async fn private_metadata_is_hidden_without_auth() {
        let test = setup_state().await;

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/private-dataset".to_string(),
                    name: "Private Dataset".to_string(),
                    description: "Private metadata".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: false,
                },
            )),
        )
        .await
        .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
            Query(ListMetadataQuery::default()),
        )
        .await
        .unwrap();
        assert!(listed.documents.is_empty());

        let result = export_metadata_rocrate(
            State(test.state),
            Extension(None),
            Path(created.summary.document_id),
            Query(MetadataRoCrateExportParams::default()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Unauthorized)));
    }

    #[tokio::test]
    async fn query_metadata_document_returns_not_found_for_missing_document() {
        let test = setup_state().await;

        let result = query_metadata_document(
            State(test.state),
            Extension(None),
            Extension(None),
            Path(Ulid::new().to_string()),
            Json(SparqlQueryRequest {
                query: "ASK WHERE { ?s ?p ?o }".to_string(),
                mode: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn load_realm_nodes_falls_back_to_local_node_on_discovery_failure() {
        let state = setup_state_with_closed_storage().await;

        let nodes = load_realm_nodes(state.as_ref()).await.unwrap();

        assert_eq!(nodes, vec![state.get_node_id()]);
    }

    #[tokio::test]
    async fn load_realm_nodes_reflects_new_presence_without_stale_cache() {
        let realm_id = test_realm_id(31);
        let coordinator = spawn_distributed_metadata_node(realm_id).await;
        let remote = spawn_distributed_metadata_node(realm_id).await;

        coordinator
            .net
            .add_peer_addr(remote.net.endpoint_addr())
            .await;
        remote
            .net
            .add_peer_addr(coordinator.net.endpoint_addr())
            .await;

        let initial = load_realm_nodes(coordinator.state.as_ref()).await.unwrap();
        assert_eq!(initial, vec![coordinator.net.node_id()]);

        let remote_ctx = remote.state.get_ctx();
        let mut announced = false;
        let mut last_announce_error = None;
        for _ in 0..10 {
            match drive(
                AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                    realm_id,
                    node_id: remote.net.node_id(),
                    schedule_refresh: false,
                }),
                remote_ctx.as_ref(),
            )
            .await
            {
                Ok(()) => {
                    announced = true;
                    break;
                }
                Err(error) => {
                    last_announce_error = Some(format!("{error:?}"));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        if !announced {
            coordinator.net.shutdown().await;
            remote.net.shutdown().await;
            panic!(
                "remote realm presence was not announced: {}",
                last_announce_error.unwrap_or_else(|| "no attempts".to_string())
            );
        }

        let mut discovered = Vec::new();
        for _ in 0..10 {
            discovered = load_realm_nodes(coordinator.state.as_ref()).await.unwrap();
            if discovered.contains(&remote.net.node_id()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(discovered.contains(&coordinator.net.node_id()));
        assert!(discovered.contains(&remote.net.node_id()));

        coordinator.net.shutdown().await;
        remote.net.shutdown().await;
    }

    #[test]
    fn document_replica_query_nodes_use_deduplicated_replicas() {
        let local_node_id = iroh::SecretKey::from_bytes(&[21u8; 32]).public();
        let remote_node_id = iroh::SecretKey::from_bytes(&[22u8; 32]).public();
        let document_id = Ulid::new();
        let record = MetadataRegistryRecord {
            realm_id: RealmId([3u8; 32]),
            group_id: Ulid::new(),
            document_id,
            document_path: "datasets/query-targets".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: "/metadata/query-targets".to_string(),
            holder_node_ids: vec![remote_node_id, local_node_id, remote_node_id],
            created_at_ms: 0,
            updated_at_ms: 0,
            last_event_id: Ulid::nil(),
        };

        assert_eq!(
            document_replica_query_nodes(&record, local_node_id),
            vec![remote_node_id, local_node_id]
        );

        let mut empty_replicas = record;
        empty_replicas.holder_node_ids.clear();
        assert_eq!(
            document_replica_query_nodes(&empty_replicas, local_node_id),
            vec![local_node_id]
        );
    }

    #[test]
    fn deduplicate_fanout_nodes_preserves_first_seen_order() {
        let first = iroh::SecretKey::from_bytes(&[31u8; 32]).public();
        let second = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
        let third = iroh::SecretKey::from_bytes(&[33u8; 32]).public();

        assert_eq!(
            deduplicate_fanout_nodes(vec![first, second, first, third, second]),
            vec![first, second, third]
        );
    }

    #[test]
    fn metadata_auth_token_helper_uses_validated_carrier_only() {
        let carrier = ValidatedArunaBearerTokenCarrier::new_for_test("raw-aruna-token");

        assert_eq!(
            metadata_auth_token_from_carrier(Some(&carrier)),
            Some(MetadataAuthToken::bearer("raw-aruna-token").unwrap())
        );
        assert_eq!(metadata_auth_token_from_carrier(None), None);
    }

    #[tokio::test]
    async fn load_metadata_record_by_document_returns_internal_error_on_storage_failure() {
        let state = setup_state_with_closed_storage().await;

        let result = load_metadata_record_by_document(state.as_ref(), Ulid::new()).await;

        assert!(matches!(
            result,
            Err(ServerError::InternalError(message)) if message == "Channel closed"
        ));
    }

    #[tokio::test]
    async fn ensure_permission_returns_forbidden_for_nonexistent_group() {
        let test = setup_state().await;
        let missing_group = Ulid::new();
        let path = format!("/{}/g/{missing_group}/meta/**", test.state.get_realm_id());

        let result = ensure_permission(
            test.state.as_ref(),
            test.auth.clone(),
            path,
            Permission::WRITE,
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn export_returns_service_unavailable_while_materialization_pending() {
        let test = setup_state().await;
        let ctx = test.state.get_ctx();

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/pending-dataset".to_string(),
                    name: "Pending Dataset".to_string(),
                    description: "Not yet materialized".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();

        let result = export_metadata_rocrate(
            State(test.state.clone()),
            Extension(None),
            Path(created.summary.document_id.clone()),
            Query(MetadataRoCrateExportParams::default()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::NotFound)));

        let projected = drain_pending_metadata_projection_queue(ctx.as_ref())
            .await
            .unwrap();
        assert_eq!(projected.markers_examined, 1);
        assert_eq!(projected.projected, 1);
        let result = export_metadata_rocrate(
            State(test.state.clone()),
            Extension(None),
            Path(created.summary.document_id.clone()),
            Query(MetadataRoCrateExportParams::default()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::ServiceUnavailable)));

        let materialized = process_metadata_materialization_batch(ctx.as_ref())
            .await
            .unwrap();
        assert_eq!(materialized.processed, 1);
        let result = export_metadata_rocrate(
            State(test.state.clone()),
            Extension(None),
            Path(created.summary.document_id),
            Query(MetadataRoCrateExportParams::default()),
        )
        .await;
        assert!(result.is_ok());
    }

    #[test]
    fn metadata_openapi_includes_examples_and_public_field_names() {
        let openapi = serde_json::to_value(MetadataApiDoc::openapi()).unwrap();

        assert_eq!(
            openapi["components"]["schemas"]["MetadataRoCrateView"]["type"],
            json!("string")
        );

        let export_params =
            openapi["paths"]["/metadata/{document_id}/rocrate"]["get"]["parameters"]
                .as_array()
                .unwrap();
        let view_param = export_params
            .iter()
            .find(|param| param["name"] == "view")
            .unwrap();
        assert_eq!(
            view_param["schema"]["$ref"],
            json!("#/components/schemas/MetadataRoCrateView")
        );

        let create_examples = openapi["paths"]["/metadata"]["post"]["requestBody"]["content"]
            ["application/json"]["examples"]
            .as_object()
            .unwrap();
        assert!(create_examples.contains_key("ScaffoldCreate"));
        assert!(create_examples.contains_key("RoCrateCreate"));

        let create_response_description = openapi["paths"]["/metadata"]["post"]["responses"]["201"]
            ["description"]
            .as_str()
            .unwrap();
        assert!(create_response_description.contains("durable event/projection pipeline"));
        assert!(create_response_description.contains("fully materialized"));
        assert!(create_response_description.contains("replicated yet"));

        for (path, method) in [
            ("/metadata/{document_id}/rocrate", "put"),
            ("/metadata/{document_id}/rocrate/data-entities", "post"),
            (
                "/metadata/{document_id}/rocrate/contextual-entities",
                "post",
            ),
        ] {
            let description = openapi["paths"][path][method]["responses"]["200"]["description"]
                .as_str()
                .unwrap();
            assert!(description.contains("durable event/projection pipeline"));
            assert!(description.contains("fully materialized"));
            assert!(description.contains("replicated yet"));
        }

        let document_query_request =
            &openapi["paths"]["/metadata/{document_id}/sparql/query"]["post"]["requestBody"];
        assert!(
            document_query_request["description"]
                .as_str()
                .unwrap()
                .contains("best-effort")
        );

        let query_all_request = &openapi["paths"]["/metadata/sparql/query"]["post"]["requestBody"];
        assert!(
            query_all_request["description"]
                .as_str()
                .unwrap()
                .contains("best-effort")
        );

        let search_params = openapi["paths"]["/metadata/search"]["get"]["parameters"]
            .as_array()
            .unwrap();
        let search_mode_param = search_params
            .iter()
            .find(|param| param["name"] == "mode")
            .unwrap();
        assert!(
            search_mode_param["description"]
                .as_str()
                .unwrap()
                .contains("best-effort")
        );

        let data_entity_examples = openapi["paths"]["/metadata/{document_id}/rocrate/data-entities"]
            ["post"]["requestBody"]["content"]["application/json"]["examples"]
            .as_object()
            .unwrap();
        assert!(data_entity_examples.contains_key("DataEntity"));

        let contextual_examples = openapi["paths"]
            ["/metadata/{document_id}/rocrate/contextual-entities"]["post"]["requestBody"]
            ["content"]["application/json"]["examples"]
            .as_object()
            .unwrap();
        assert!(contextual_examples.contains_key("ContextualEntity"));

        let summary_properties =
            openapi["components"]["schemas"]["MetadataDocumentSummary"]["properties"]
                .as_object()
                .unwrap();
        assert!(summary_properties.contains_key("replicas"));
        assert!(summary_properties.contains_key("created_at"));
        assert!(summary_properties.contains_key("updated_at"));
        assert!(!summary_properties.contains_key("holder_count"));
        assert!(!summary_properties.contains_key("created_at_ms"));
        assert!(!summary_properties.contains_key("updated_at_ms"));
    }

    #[test]
    fn deduplicates_select_rows_from_multiple_nodes() {
        let results = aggregate_query_results(
            vec![
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
                ]),
                MetadataQueryResults::Solutions(vec![BTreeMap::from([(
                    String::from("s"),
                    String::from("<urn:a>"),
                )])]),
            ],
            QueryForm::Select,
            None,
        )
        .unwrap();

        let MetadataQueryResults::Solutions(rows) = results else {
            panic!("expected solutions");
        };
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn reapplies_select_limit_after_distributed_merge() {
        let results = aggregate_query_results(
            vec![
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
                ]),
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:c>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:d>"))]),
                ]),
            ],
            QueryForm::Select,
            Some(3),
        )
        .unwrap();

        let MetadataQueryResults::Solutions(rows) = results else {
            panic!("expected solutions");
        };
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn query_select_limit_reads_outermost_limit_only() {
        assert_eq!(
            query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 5"),
            Some(5)
        );
        assert_eq!(
            query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 7 OFFSET 3"),
            Some(7)
        );
        assert_eq!(query_select_limit("SELECT ?s WHERE { ?s ?p ?o }"), None);
        assert_eq!(
            query_select_limit(
                "SELECT ?s WHERE { { SELECT ?s WHERE { ?s ?p ?o } LIMIT 5 } ?s ?p ?o }"
            ),
            None
        );
        assert_eq!(query_select_limit("ASK WHERE { ?s ?p ?o }"), None);
        assert_eq!(query_select_limit("not sparql"), None);
    }

    #[test]
    fn query_response_serializes_envelope_with_partiality_fields() {
        let response = MetadataQueryResponse {
            result: MetadataQueryResult::Boolean(true),
            nodes_queried: 3,
            nodes_failed: 1,
        };
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["kind"], json!("Boolean"));
        assert_eq!(value["value"], json!(true));
        assert_eq!(value["nodes_queried"], json!(3));
        assert_eq!(value["nodes_failed"], json!(1));

        let roundtrip: MetadataQueryResponse = serde_json::from_value(value).unwrap();
        assert!(matches!(
            roundtrip.result,
            MetadataQueryResult::Boolean(true)
        ));
        assert_eq!(roundtrip.nodes_queried, 3);
        assert_eq!(roundtrip.nodes_failed, 1);
    }

    #[tokio::test]
    async fn distributed_query_executes_local_partition_in_process() {
        let test = setup_state().await;

        let _ = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/local-partition".to_string(),
                    name: "Local Partition Dataset".to_string(),
                    description: "Coordinator partition".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        // The test state has no remote realm nodes and no net handle, so a
        // distributed query only succeeds if the coordinator partition runs
        // in-process instead of going over the wire.
        let (_, Json(result)) = query_all_metadata(
            State(test.state.clone()),
            Extension(None),
            Extension(None),
            Json(SparqlQueryRequest {
                query: "SELECT ?name WHERE { ?s <http://schema.org/name> ?name } LIMIT 10"
                    .to_string(),
                mode: Some(MetadataQueryMode::Distributed),
            }),
        )
        .await
        .unwrap();

        assert_eq!(result.nodes_queried, 1);
        assert_eq!(result.nodes_failed, 0);
        let MetadataQueryResult::Solutions(rows) = result.result else {
            panic!("expected solutions");
        };
        assert!(rows.iter().any(|row| {
            row.values()
                .any(|value| value.contains("Local Partition Dataset"))
        }));
    }

    #[tokio::test]
    async fn query_all_metadata_applies_lazy_per_caller_visibility() {
        let test = setup_state().await;

        for (path, name, public) in [
            ("datasets/lazy-public", "Lazy Public Dataset", true),
            ("datasets/lazy-private", "Lazy Private Dataset", false),
        ] {
            let _ = create_metadata_document(
                State(test.state.clone()),
                Extension(Some(test.auth.clone())),
                Json(CreateMetadataRequest::Scaffold(
                    CreateMetadataScaffoldRequest {
                        group_id: test.group_id.to_string(),
                        path: path.to_string(),
                        name: name.to_string(),
                        description: "Lazy visibility".to_string(),
                        date_published: "2026-01-01".to_string(),
                        license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                        public,
                    },
                )),
            )
            .await
            .unwrap();
        }
        drain_metadata_background(test.state.as_ref()).await;

        let query_names = async |auth: Option<AuthContext>| {
            let (_, Json(result)) = query_all_metadata(
                State(test.state.clone()),
                Extension(auth),
                Extension(None),
                Json(SparqlQueryRequest {
                    query: "SELECT ?name WHERE { ?s <http://schema.org/name> ?name }".to_string(),
                    mode: Some(MetadataQueryMode::Local),
                }),
            )
            .await
            .unwrap();
            let MetadataQueryResult::Solutions(rows) = result.result else {
                panic!("expected solutions");
            };
            rows.into_iter()
                .flat_map(|row| row.into_values())
                .collect::<Vec<_>>()
        };

        let anonymous = query_names(None).await;
        assert!(anonymous.iter().any(|name| name.contains("Lazy Public")));
        assert!(!anonymous.iter().any(|name| name.contains("Lazy Private")));

        let authorized = query_names(Some(test.auth.clone())).await;
        assert!(authorized.iter().any(|name| name.contains("Lazy Public")));
        assert!(authorized.iter().any(|name| name.contains("Lazy Private")));
    }

    #[tokio::test]
    async fn distributed_query_forwards_validated_bearer_token_for_remote_private_metadata() {
        let test = setup_distributed_metadata_access_state().await;
        let token_auth: AuthContext =
            crate::auth::handle_token(test.coordinator.state.as_ref(), &test.valid_bearer_token)
                .await
                .unwrap()
                .try_into()
                .unwrap();
        assert_eq!(token_auth, test.auth);

        let authorized = query_remote_metadata_names(
            &test,
            Some(token_auth.clone()),
            Some(ValidatedArunaBearerTokenCarrier::new_for_test(
                test.valid_bearer_token.clone(),
            )),
        )
        .await;
        assert_eq!(authorized.nodes_queried, 1);
        assert_eq!(authorized.nodes_failed, 0);
        assert_contains_dataset_name(&authorized.names, "Remote Public Dataset");
        assert_contains_dataset_name(&authorized.names, "Remote Private Dataset");

        let anonymous = query_remote_metadata_names(&test, None, None).await;
        assert_eq!(anonymous.nodes_failed, 0);
        assert_contains_dataset_name(&anonymous.names, "Remote Public Dataset");
        assert_excludes_dataset_name(&anonymous.names, "Remote Private Dataset");

        let authenticated_without_forwardable_token =
            query_remote_metadata_names(&test, Some(token_auth.clone()), None).await;
        assert_eq!(authenticated_without_forwardable_token.nodes_failed, 0);
        assert_contains_dataset_name(
            &authenticated_without_forwardable_token.names,
            "Remote Public Dataset",
        );
        assert_excludes_dataset_name(
            &authenticated_without_forwardable_token.names,
            "Remote Private Dataset",
        );

        let oversized_non_forwardable_token = query_remote_metadata_names(
            &test,
            Some(token_auth.clone()),
            Some(ValidatedArunaBearerTokenCarrier::new_for_test(
                "x".repeat(4097),
            )),
        )
        .await;
        assert_eq!(oversized_non_forwardable_token.nodes_failed, 0);
        assert_contains_dataset_name(
            &oversized_non_forwardable_token.names,
            "Remote Public Dataset",
        );
        assert_excludes_dataset_name(
            &oversized_non_forwardable_token.names,
            "Remote Private Dataset",
        );

        assert!(
            crate::auth::handle_token(test.coordinator.state.as_ref(), "not-a-jwt")
                .await
                .is_err()
        );
        let invalid_forwarded_token = query_remote_metadata_names(
            &test,
            Some(token_auth),
            Some(ValidatedArunaBearerTokenCarrier::new_for_test("not-a-jwt")),
        )
        .await;
        assert_eq!(invalid_forwarded_token.nodes_queried, 1);
        assert_eq!(invalid_forwarded_token.nodes_failed, 1);
        assert_excludes_dataset_name(&invalid_forwarded_token.names, "Remote Public Dataset");
        assert_excludes_dataset_name(&invalid_forwarded_token.names, "Remote Private Dataset");

        test.shutdown().await;
    }

    #[tokio::test]
    async fn distributed_search_without_forwarded_token_reads_remote_public_metadata_only() {
        let test = setup_distributed_metadata_access_state().await;
        test.remote
            .state
            .get_ctx()
            .metadata_handle
            .as_ref()
            .unwrap()
            .flush_search_updates()
            .await
            .unwrap();

        let anonymous = search_remote_metadata_paths(&test, None, None).await;
        assert_eq!(anonymous.nodes_queried, 1);
        assert_eq!(anonymous.nodes_failed, 0);
        assert_contains_search_path(&anonymous.paths, "datasets/remote-public");
        assert_excludes_search_path(&anonymous.paths, "datasets/remote-private");

        test.shutdown().await;
    }

    #[test]
    fn deduplicates_search_hits_across_replicas() {
        let hits = deduplicate_search_hits(
            vec![
                MetadataSearchHit {
                    document_id: "01A".to_string(),
                    group_id: "01G".to_string(),
                    document_path: "datasets/a".to_string(),
                    graph_iri: "https://w3id.org/aruna/01A".to_string(),
                    subject_iri: "./file.txt".to_string(),
                    score: 0.5,
                },
                MetadataSearchHit {
                    document_id: "01A".to_string(),
                    group_id: "01G".to_string(),
                    document_path: "datasets/a".to_string(),
                    graph_iri: "https://w3id.org/aruna/01A".to_string(),
                    subject_iri: "./file.txt".to_string(),
                    score: 0.8,
                },
                MetadataSearchHit {
                    document_id: "01B".to_string(),
                    group_id: "01G".to_string(),
                    document_path: "datasets/b".to_string(),
                    graph_iri: "https://w3id.org/aruna/01B".to_string(),
                    subject_iri: "./file.txt".to_string(),
                    score: 0.7,
                },
            ],
            2,
        );

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].graph_iri, "https://w3id.org/aruna/01A");
        assert_eq!(hits[0].score, 0.8);
        assert_eq!(hits[1].graph_iri, "https://w3id.org/aruna/01B");
    }

    #[test]
    fn search_hit_dedup_orders_equal_scores_stably() {
        let hit = |document_id: &str, subject_iri: &str| MetadataSearchHit {
            document_id: document_id.to_string(),
            group_id: "01G".to_string(),
            document_path: format!("datasets/{document_id}"),
            graph_iri: format!("https://w3id.org/aruna/{document_id}"),
            subject_iri: subject_iri.to_string(),
            score: 0.7,
        };

        let hits = deduplicate_search_hits(
            vec![
                hit("01B", "./file-b.txt"),
                hit("01A", "./file-b.txt"),
                hit("01A", "./file-a.txt"),
            ],
            10,
        );

        let keys = hits
            .iter()
            .map(|hit| (hit.graph_iri.as_str(), hit.subject_iri.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                ("https://w3id.org/aruna/01A", "./file-a.txt"),
                ("https://w3id.org/aruna/01A", "./file-b.txt"),
                ("https://w3id.org/aruna/01B", "./file-b.txt"),
            ]
        );
    }

    struct DistributedMetadataAccessState {
        auth: AuthContext,
        valid_bearer_token: String,
        coordinator: DistributedMetadataNode,
        remote: DistributedMetadataNode,
    }

    impl DistributedMetadataAccessState {
        async fn shutdown(self) {
            self.coordinator.net.shutdown().await;
            self.remote.net.shutdown().await;
        }
    }

    struct DistributedMetadataNode {
        _node_dir: TempDir,
        net: NetHandle,
        state: Arc<ServerState>,
    }

    struct QueryNamesResult {
        names: Vec<String>,
        nodes_queried: usize,
        nodes_failed: usize,
    }

    struct SearchPathsResult {
        paths: Vec<String>,
        nodes_queried: usize,
        nodes_failed: usize,
    }

    async fn setup_distributed_metadata_access_state() -> DistributedMetadataAccessState {
        let realm_signing_key = test_realm_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let user_id = aruna_core::UserId::local(Ulid::new(), realm_id);
        let group_id = Ulid::new();
        let coordinator = spawn_distributed_metadata_node(realm_id).await;
        let remote = spawn_distributed_metadata_node(realm_id).await;
        let nodes = [&coordinator, &remote];

        coordinator
            .net
            .add_peer_addr(remote.net.endpoint_addr())
            .await;
        remote
            .net
            .add_peer_addr(coordinator.net.endpoint_addr())
            .await;
        install_distributed_realm_config(&nodes, realm_id).await;
        for node in nodes {
            install_metadata_auth_documents(node, realm_id, user_id, group_id).await;
        }

        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };
        create_test_metadata_document(
            remote.state.clone(),
            auth.clone(),
            group_id,
            "datasets/remote-public",
            "Remote Public Dataset",
            true,
        )
        .await;
        create_test_metadata_document(
            remote.state.clone(),
            auth.clone(),
            group_id,
            "datasets/remote-private",
            "Remote Private Dataset",
            false,
        )
        .await;
        drain_metadata_background(remote.state.as_ref()).await;

        let valid_bearer_token =
            sign_test_token(&realm_signing_key, &test_token_claims(realm_id, user_id));

        DistributedMetadataAccessState {
            auth,
            valid_bearer_token,
            coordinator,
            remote,
        }
    }

    async fn spawn_distributed_metadata_node(realm_id: RealmId) -> DistributedMetadataNode {
        let node_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(node_dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let metadata_handle = MetadataHandle::new(
            node_dir.path().join("metadata"),
            net.node_id(),
            storage_handle.clone(),
            Some(net.clone()),
            Some(net.document_sync_node()),
            Some(net.document_sync_database()),
        )
        .unwrap();
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: Some(TaskHandle::new()),
        });
        initialize_net_incoming(context.clone());
        let state = Arc::new(
            ServerState::new(
                context,
                realm_id,
                net.node_id(),
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
            )
            .await,
        );

        DistributedMetadataNode {
            _node_dir: node_dir,
            net,
            state,
        }
    }

    async fn install_distributed_realm_config(
        nodes: &[&DistributedMetadataNode],
        realm_id: RealmId,
    ) {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        for node in nodes {
            config.ensure_node(node.net.node_id(), RealmNodeKind::Server);
        }

        for node in nodes {
            let actor = Actor {
                node_id: node.net.node_id(),
                user_id: aruna_core::UserId::nil(realm_id),
                realm_id,
            };
            write_doc(
                &node.state.get_ctx(),
                REALM_CONFIG_KEYSPACE,
                (*realm_id.as_bytes()).into(),
                config.to_bytes(&actor).unwrap().into(),
            )
            .await;
            node.net
                .refresh_realm_peers_from_document(&config)
                .await
                .unwrap();
        }
    }

    async fn install_metadata_auth_documents(
        node: &DistributedMetadataNode,
        realm_id: RealmId,
        user_id: aruna_core::UserId,
        group_id: Ulid,
    ) {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id,
            realm_id,
        };
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "distributed-metadata-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let context = node.state.get_ctx();

        write_doc(
            &context,
            AUTH_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &context,
            AUTH_KEYSPACE,
            group_id.to_bytes().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &context,
            GROUP_KEYSPACE,
            group_id.to_bytes().into(),
            group.to_bytes(&actor).unwrap().into(),
        )
        .await;
    }

    async fn create_test_metadata_document(
        state: Arc<ServerState>,
        auth: AuthContext,
        group_id: Ulid,
        path: &str,
        name: &str,
        public: bool,
    ) {
        let _ = create_metadata_document(
            State(state),
            Extension(Some(auth)),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: group_id.to_string(),
                    path: path.to_string(),
                    name: name.to_string(),
                    description: "Remote metadata access fixture".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public,
                },
            )),
        )
        .await
        .unwrap();
    }

    async fn query_remote_metadata_names(
        test: &DistributedMetadataAccessState,
        auth: Option<AuthContext>,
        bearer_token: Option<ValidatedArunaBearerTokenCarrier>,
    ) -> QueryNamesResult {
        let (results, fanout_stats) = run_query_distributed(
            test.coordinator.state.as_ref(),
            auth,
            bearer_token,
            None,
            "SELECT ?name WHERE { ?s <http://schema.org/name> ?name }".to_string(),
            MetadataFanoutScope::new(
                Some(MetadataQueryMode::Distributed),
                Some(vec![test.remote.net.node_id()]),
            ),
        )
        .await
        .unwrap();
        let MetadataQueryResults::Solutions(rows) = results else {
            panic!("expected SELECT solutions");
        };
        QueryNamesResult {
            names: rows.into_iter().flat_map(|row| row.into_values()).collect(),
            nodes_queried: fanout_stats.nodes_queried,
            nodes_failed: fanout_stats.nodes_failed,
        }
    }

    async fn search_remote_metadata_paths(
        test: &DistributedMetadataAccessState,
        auth: Option<AuthContext>,
        bearer_token: Option<ValidatedArunaBearerTokenCarrier>,
    ) -> SearchPathsResult {
        let (hits, fanout) = run_search_distributed(
            test.coordinator.state.as_ref(),
            auth,
            bearer_token,
            None,
            "Remote".to_string(),
            10,
            MetadataFanoutScope::new(
                Some(MetadataQueryMode::Distributed),
                Some(vec![test.remote.net.node_id()]),
            ),
        )
        .await
        .unwrap();
        SearchPathsResult {
            paths: hits.into_iter().map(|hit| hit.document_path).collect(),
            nodes_queried: fanout.nodes_queried,
            nodes_failed: fanout.nodes_failed,
        }
    }

    fn assert_contains_dataset_name(names: &[String], expected: &str) {
        assert!(
            names.iter().any(|name| name.contains(expected)),
            "expected {names:?} to contain {expected:?}"
        );
    }

    fn assert_excludes_dataset_name(names: &[String], unexpected: &str) {
        assert!(
            !names.iter().any(|name| name.contains(unexpected)),
            "expected {names:?} not to contain {unexpected:?}"
        );
    }

    fn assert_contains_search_path(paths: &[String], expected: &str) {
        assert!(
            paths.iter().any(|path| path == expected),
            "expected {paths:?} to contain {expected:?}"
        );
    }

    fn assert_excludes_search_path(paths: &[String], unexpected: &str) {
        assert!(
            !paths.iter().any(|path| path == unexpected),
            "expected {paths:?} not to contain {unexpected:?}"
        );
    }

    fn test_realm_signing_key() -> SigningKey {
        let mut rng = jsonwebtoken::signature::rand_core::OsRng;
        SigningKey::generate(&mut rng)
    }

    fn test_realm_id(seed: u8) -> RealmId {
        RealmId::from_bytes(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    fn test_token_claims(realm_id: RealmId, user_id: aruna_core::UserId) -> TokenClaims {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        TokenClaims {
            sub: user_id.to_string(),
            iss: realm_id.to_string(),
            iat: now,
            exp: now + 600,
            jti: Ulid::new().to_string(),
            restrictions: None,
            issuer_pubkey: None,
            delegation_signature: None,
        }
    }

    fn sign_test_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
        let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        encode(
            &Header::new(Algorithm::EdDSA),
            claims,
            &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let metadata_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let realm_id = test_realm_id(3);
        let user_id = aruna_core::UserId::local(Ulid::new(), realm_id);
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            node_id,
            storage_handle.clone(),
            None,
            None,
            None,
        )
        .unwrap();
        let task_handle = TaskHandle::new();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: Some(task_handle),
        });
        let group_id = Ulid::new();
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "metadata-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);

        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            group_id.to_bytes().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            GROUP_KEYSPACE,
            group_id.to_bytes().into(),
            group.to_bytes(&actor).unwrap().into(),
        )
        .await;

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
            )
            .await,
        );

        TestState {
            _storage_dir: storage_dir,
            _metadata_dir: metadata_dir,
            auth: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            },
            group_id,
            state,
        }
    }

    async fn drain_metadata_background(state: &ServerState) {
        let ctx = state.get_ctx();
        let drained = drain_pending_metadata_projection_queue(ctx.as_ref())
            .await
            .unwrap();
        if drained.markers_examined == 0 {
            replay_metadata_event_log(ctx.as_ref()).await.unwrap();
        }
        process_metadata_materialization_batch(ctx.as_ref())
            .await
            .unwrap();
    }

    async fn setup_state_with_closed_storage() -> Arc<ServerState> {
        let (storage_handle, receiver) = storage::StorageHandle::new();
        drop(receiver);

        let realm_id = test_realm_id(3);
        let node_id = iroh::SecretKey::from_bytes(&[14u8; 32]).public();
        Arc::new(
            ServerState::new(
                Arc::new(DriverContext {
                    storage_handle,
                    net_handle: None,
                    blob_handle: None,
                    metadata_handle: None,
                    task_handle: None,
                }),
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
            )
            .await,
        )
    }

    async fn write_doc(
        driver_ctx: &Arc<DriverContext>,
        key_space: &str,
        key: byteview::ByteView,
        value: byteview::ByteView,
    ) {
        let event = driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read_persisted_task_timer(
        ctx: &DriverContext,
        key: &TaskKey,
    ) -> Option<PersistedTaskTimer> {
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                key: postcard::to_allocvec(key).unwrap().into(),
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                value.map(|value| postcard::from_bytes(&value).expect("timer decodes"))
            }
            other => panic!("unexpected task timer read event: {other:?}"),
        }
    }
}
