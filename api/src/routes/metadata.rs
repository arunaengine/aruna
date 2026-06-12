use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::effects::Effect;
use aruna_core::errors::AuthorizationError;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::{
    MetadataEffect, MetadataError, MetadataEvent, MetadataQueryResults, MetadataRoCratePage,
    MetadataSearchHit,
};
use aruna_core::structs::{Actor, AuthContext, MetadataRegistryRecord, Permission};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::list_metadata_documents::ListMetadataDocumentsOperation;
use aruna_operations::metadata::projector::project_metadata_create_events_from_log;
use aruna_operations::metadata::repository::{
    parse_registry_read, read_registry_by_document_effect,
};
use aruna_operations::metadata::visible_registry;
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentError, UpdateMetadataDocumentMutation,
    UpdateMetadataDocumentOperation,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, Weak};
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateMetadataResponse {
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
const METADATA_REALM_NODES_CACHE_TTL: Duration = Duration::from_secs(5);
const METADATA_PROJECTION_DEBOUNCE_AFTER: Duration = Duration::from_millis(100);

static METADATA_REALM_NODES_CACHE: OnceLock<
    Mutex<HashMap<RealmNodesCacheKey, RealmNodesCacheEntry>>,
> = OnceLock::new();
static METADATA_PROJECTION_BATCHES: OnceLock<Mutex<HashMap<[u8; 32], MetadataProjectionBatch>>> =
    OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct RealmNodesCacheKey {
    realm_id: [u8; 32],
    local_node_id: [u8; 32],
}

struct RealmNodesCacheEntry {
    nodes: Vec<aruna_core::NodeId>,
    expires_at: Instant,
}

struct MetadataProjectionBatch {
    ctx: Weak<DriverContext>,
    pending: Vec<(Ulid, Ulid)>,
    scheduled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SparqlQueryRequest {
    /// SPARQL query string. Only `SELECT` and `ASK` queries are supported.
    pub query: String,
    /// Query execution scope. Omit to use `distributed`.
    ///
    /// `local` runs only against metadata indexed on the current node.
    /// `distributed` fans out to all known realm nodes for all-metadata queries,
    /// or to the document's registry holder nodes for document-scoped queries,
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
            description = "Metadata document created",
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
    let created = drive(
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
        ctx.as_ref(),
    )
    .await
    .map_err(map_create_metadata_error)?;
    let result = created.record;
    if let Some(metadata_handle) = ctx.metadata_handle.as_ref() {
        metadata_handle.cache_accepted_create(result.clone());
    }
    wake_metadata_create_projection(ctx, result.document_id, created.event_id);

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
    drive(
        DeleteMetadataDocumentOperation::new(
            Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            record.group_id,
            document_id,
        ),
        &ctx,
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    if let Some(metadata_handle) = ctx.metadata_handle.as_ref() {
        metadata_handle.remove_cached_accepted_create(document_id);
    }
    visible_registry::remove_visible_registry_record(ctx.as_ref(), record.group_id, document_id);

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
            description = "Metadata document updated",
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

    let updated = drive(
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
        &state.get_ctx(),
    )
    .await
    .map_err(map_update_metadata_error)?;
    upsert_visible_registry_record(&state, &updated);

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
            description = "Data entity upserted",
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

    let updated = drive(
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
        &state.get_ctx(),
    )
    .await
    .map_err(map_update_metadata_error)?;
    upsert_visible_registry_record(&state, &updated);

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
            description = "Contextual entity upserted",
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

    let updated = drive(
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
        &state.get_ctx(),
    )
    .await
    .map_err(map_update_metadata_error)?;
    upsert_visible_registry_record(&state, &updated);

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
        description = "Run a SPARQL `SELECT` or `ASK` query against one metadata document. `mode=local` only queries the current node, while `mode=distributed` queries the document's registry holder nodes and merges the results. Distributed mode is best-effort and may return partial results if holder requests fail. Omitting `mode` defaults to `distributed`.",
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
    Path(document_id): Path<String>,
    Json(request): Json<SparqlQueryRequest>,
) -> ServerResult<(StatusCode, Json<MetadataQueryResponse>)> {
    let document_id = parse_document_id(&document_id)?;
    ensure_supported_query_form(&request.query)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_readable(&state, auth.as_ref(), &record).await?;
    let (results, fanout) = run_query_distributed(
        &state,
        auth,
        Some(vec![record.graph_iri.clone()]),
        request.query,
        request.mode,
        Some(document_query_target_nodes(&record, state.get_node_id())),
    )
    .await?;
    let serialize_started = Instant::now();
    let response = map_query_results(results, fanout)?;
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
    Json(request): Json<SparqlQueryRequest>,
) -> ServerResult<(StatusCode, Json<MetadataQueryResponse>)> {
    ensure_supported_query_form(&request.query)?;
    let (results, fanout) =
        run_query_distributed(&state, auth, None, request.query, request.mode, None).await?;
    let serialize_started = Instant::now();
    let response = map_query_results(results, fanout)?;
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
    Query(params): Query<MetadataSearchParams>,
) -> ServerResult<(StatusCode, Json<MetadataSearchResponse>)> {
    if params.q.trim().is_empty() {
        return Err(ServerError::BadRequest);
    }
    let limit = params.limit.unwrap_or(25).clamp(1, 250);
    let (hits, fanout) =
        run_search_distributed(&state, auth, None, params.q, limit, params.mode).await?;
    Ok((
        StatusCode::OK,
        Json(MetadataSearchResponse {
            hits: hits.into_iter().map(map_search_hit).collect(),
            nodes_queried: fanout.nodes_queried,
            nodes_failed: fanout.nodes_failed,
        }),
    ))
}

fn parse_document_id(document_id: &str) -> ServerResult<Ulid> {
    Ulid::from_string(document_id).map_err(|_| ServerError::BadRequest)
}

fn metadata_projection_batch_key(ctx: &Arc<DriverContext>) -> [u8; 32] {
    match ctx.net_handle.as_ref() {
        Some(net) => *net.node_id().as_bytes(),
        None => {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(Arc::as_ptr(ctx) as usize as u64).to_be_bytes());
            key
        }
    }
}

fn wake_metadata_create_projection(ctx: Arc<DriverContext>, document_id: Ulid, event_id: Ulid) {
    let key = metadata_projection_batch_key(&ctx);
    let should_spawn = {
        let batches = METADATA_PROJECTION_BATCHES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut batches = batches
            .lock()
            .expect("metadata projection batch mutex poisoned");
        let batch = batches
            .entry(key)
            .or_insert_with(|| MetadataProjectionBatch {
                ctx: Arc::downgrade(&ctx),
                pending: Vec::new(),
                scheduled: false,
            });
        batch.ctx = Arc::downgrade(&ctx);
        batch.pending.push((document_id, event_id));
        if batch.scheduled {
            false
        } else {
            batch.scheduled = true;
            true
        }
    };
    if should_spawn {
        tokio::spawn(async move {
            tokio::time::sleep(METADATA_PROJECTION_DEBOUNCE_AFTER).await;
            drain_metadata_projection_batch(key).await;
        });
    }
}

async fn drain_metadata_projection_batch(key: [u8; 32]) {
    let Some((ctx, targets)) = take_metadata_projection_batch(key) else {
        return;
    };
    if targets.is_empty() {
        return;
    }
    if let Err(error) = project_metadata_create_events_from_log(ctx.as_ref(), targets).await {
        warn!(error = ?error, "Failed to project metadata create event batch after create");
    }
}

fn take_metadata_projection_batch(
    key: [u8; 32],
) -> Option<(Arc<DriverContext>, Vec<(Ulid, Ulid)>)> {
    let batches = METADATA_PROJECTION_BATCHES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut batches = batches
        .lock()
        .expect("metadata projection batch mutex poisoned");
    let mut remove = false;
    let result = batches.get_mut(&key).and_then(|batch| {
        batch.scheduled = false;
        let ctx = match batch.ctx.upgrade() {
            Some(ctx) => ctx,
            None => {
                remove = true;
                return None;
            }
        };
        let targets = std::mem::take(&mut batch.pending);
        Some((ctx, targets))
    });
    if remove {
        batches.remove(&key);
    }
    result
}

async fn load_group_metadata_records(
    state: &ServerState,
    group_id: Ulid,
) -> ServerResult<Vec<MetadataRegistryRecord>> {
    let ctx = state.get_ctx();
    // Listing is eventually consistent by design: the visible-registry cache is
    // incrementally updated on local writes (projection upserts, updates,
    // deletes), serves stale snapshots while a background refill runs, and is
    // keyed per group so a small group's listing stays independent of the
    // realm-wide corpus size; the registry scan only runs as a cold-cache
    // fallback when the fill fails.
    let mut records = match visible_registry::list_visible_registry_records_for_group(
        ctx.as_ref(),
        group_id,
    )
    .await
    {
        Ok(group_records) => group_records.as_ref().clone(),
        Err(error) => {
            warn!(
                error = %error,
                "visible registry cache fill failed, falling back to registry scan"
            );
            drive(ListMetadataDocumentsOperation::new(group_id), &ctx)
                .await
                .map_err(|err| ServerError::InternalError(err.to_string()))?
        }
    };
    if let Some(metadata_handle) = ctx.metadata_handle.as_ref() {
        merge_cached_metadata_records(
            &mut records,
            metadata_handle.cached_accepted_creates_for_group(group_id),
        );
    }
    Ok(records)
}

fn merge_cached_metadata_records(
    records: &mut Vec<MetadataRegistryRecord>,
    cached: Vec<MetadataRegistryRecord>,
) {
    let existing = records
        .iter()
        .map(|record| record.document_id)
        .collect::<HashSet<_>>();
    records.extend(
        cached
            .into_iter()
            .filter(|record| !existing.contains(&record.document_id)),
    );
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
    let event = state
        .get_ctx()
        .storage_handle
        .send_effect(read_registry_by_document_effect(document_id, None))
        .await;
    match parse_registry_read(event) {
        Ok(Some(record)) => Ok(record),
        Ok(None) => state
            .get_ctx()
            .metadata_handle
            .as_ref()
            .and_then(|metadata_handle| metadata_handle.cached_accepted_create(document_id))
            .ok_or(ServerError::NotFound),
        Err(crate::routes::metadata::ReadError::Storage(error)) => {
            Err(ServerError::InternalError(error.to_string()))
        }
        Err(crate::routes::metadata::ReadError::Conversion(error)) => {
            Err(ServerError::InternalError(error.to_string()))
        }
    }
}

type ReadError = aruna_operations::metadata::repository::StorageReadError;

fn upsert_visible_registry_record(state: &ServerState, record: &MetadataRegistryRecord) {
    let ctx = state.get_ctx();
    if let Some(metadata_handle) = ctx.metadata_handle.as_ref() {
        metadata_handle.upsert_visible_registry_record(record.clone());
    }
    visible_registry::upsert_visible_registry_records(ctx.as_ref(), std::slice::from_ref(record));
}

async fn export_rocrate_jsonld(state: &ServerState, graph_iri: &str) -> ServerResult<Value> {
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    match handle
        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCrate {
            graph_iri: graph_iri.to_string(),
        }))
        .await
    {
        Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => parse_jsonld(jsonld),
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(map_metadata_event_error(error)),
        other => Err(ServerError::InternalError(format!(
            "unexpected metadata export event: {other:?}"
        ))),
    }
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
    match handle
        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCrateSummary {
            graph_iri: graph_iri.to_string(),
        }))
        .await
    {
        Event::Metadata(MetadataEvent::RoCrateSummaryResult { jsonld, .. }) => parse_jsonld(jsonld),
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(map_metadata_event_error(error)),
        other => Err(ServerError::InternalError(format!(
            "unexpected metadata summary event: {other:?}"
        ))),
    }
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
    match handle
        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCratePage {
            graph_iri: graph_iri.to_string(),
            offset: params.offset,
            after: params.after.clone(),
            limit,
        }))
        .await
    {
        Event::Metadata(MetadataEvent::RoCratePageResult { page, .. }) => Ok(page),
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(map_metadata_event_error(error)),
        other => Err(ServerError::InternalError(format!(
            "unexpected metadata page event: {other:?}"
        ))),
    }
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
    fanout: DistributedFanout,
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
        nodes_queried: fanout.nodes_queried,
        nodes_failed: fanout.nodes_failed,
    })
}

fn api_duration_ms(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn record_api_elapsed(span: &Span, field: &'static str, started: Instant) {
    span.record(field, api_duration_ms(started.elapsed()));
}

fn metadata_query_result_kind(results: &MetadataQueryResults) -> &'static str {
    match results {
        MetadataQueryResults::Solutions(_) => "solutions",
        MetadataQueryResults::Boolean(_) => "boolean",
        MetadataQueryResults::Graph(_) => "graph",
    }
}

async fn load_realm_nodes(state: &ServerState) -> ServerResult<Vec<aruna_core::NodeId>> {
    let cache_key = RealmNodesCacheKey {
        realm_id: *state.get_realm_id().as_bytes(),
        local_node_id: *state.get_node_id().as_bytes(),
    };
    let cache = METADATA_REALM_NODES_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let now = Instant::now();
    if let Some(nodes) = {
        let mut cache = cache.lock().unwrap_or_else(|lock| lock.into_inner());
        match cache.get(&cache_key) {
            Some(entry) if entry.expires_at > now => Some(entry.nodes.clone()),
            Some(_) => {
                cache.remove(&cache_key);
                None
            }
            None => None,
        }
    } {
        return Ok(nodes);
    }

    let mut discovery_succeeded = true;
    let nodes = match drive(
        GetRealmNodesOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(nodes) => nodes,
        Err(error) => {
            discovery_succeeded = false;
            warn!(
                error = %error,
                "realm node discovery failed, using best-effort local-only metadata results"
            );
            HashSet::new()
        }
    };
    let mut nodes = nodes.into_iter().collect::<Vec<_>>();
    if !nodes.contains(&state.get_node_id()) {
        nodes.push(state.get_node_id());
    }
    if discovery_succeeded {
        let mut cache = cache.lock().unwrap_or_else(|lock| lock.into_inner());
        cache.insert(
            cache_key,
            RealmNodesCacheEntry {
                nodes: nodes.clone(),
                expires_at: Instant::now() + METADATA_REALM_NODES_CACHE_TTL,
            },
        );
    }
    Ok(nodes)
}

fn document_query_target_nodes(
    record: &MetadataRegistryRecord,
    local_node_id: aruna_core::NodeId,
) -> Vec<aruna_core::NodeId> {
    let nodes = deduplicate_node_ids(record.holder_node_ids.clone());
    if nodes.is_empty() {
        vec![local_node_id]
    } else {
        nodes
    }
}

fn deduplicate_node_ids(nodes: Vec<aruna_core::NodeId>) -> Vec<aruna_core::NodeId> {
    let mut seen = HashSet::with_capacity(nodes.len());
    nodes
        .into_iter()
        .filter(|node_id| seen.insert(*node_id))
        .collect()
}

fn short_node_id(node_id: aruna_core::NodeId) -> String {
    let mut id = node_id.to_string();
    id.truncate(8);
    id
}

#[tracing::instrument(
    name = "metadata.api.query_distributed",
    level = "debug",
    skip(state, auth, query, target_nodes),
    fields(
        mode = ?mode,
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
    graph_iris: Option<Vec<String>>,
    query: String,
    mode: Option<MetadataQueryMode>,
    target_nodes: Option<Vec<aruna_core::NodeId>>,
) -> ServerResult<(MetadataQueryResults, DistributedFanout)> {
    let span = Span::current();
    let total_started = Instant::now();
    ensure_supported_query_mode(&mode)?;
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

    let mut parts = Vec::new();
    let mut fanout = DistributedFanout::default();
    match mode.unwrap_or(MetadataQueryMode::Distributed) {
        MetadataQueryMode::Local => {
            let node_span = debug_span!(
                "metadata.api.query_node",
                peer = ?state.get_node_id(),
                local = true,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let node_started = Instant::now();
            let result = handle
                .query_authorized_local(auth, graph_iris, query)
                .instrument(node_span.clone())
                .await;
            record_api_elapsed(&node_span, "elapsed_ms", node_started);
            fanout.nodes_queried = 1;
            match result {
                Ok(result) => {
                    node_span.record("result", metadata_query_result_kind(&result));
                    parts.push(result);
                }
                Err(error) => {
                    node_span.record("result", "error");
                    return Err(map_metadata_event_error(error));
                }
            }
        }
        MetadataQueryMode::Distributed => {
            let nodes = match target_nodes {
                Some(nodes) => {
                    span.record("discovery_ms", 0u64);
                    deduplicate_node_ids(nodes)
                }
                None => {
                    let discovery_started = Instant::now();
                    let nodes =
                        aruna_core::telemetry::time_stage("discovery", load_realm_nodes(state))
                            .await?;
                    record_api_elapsed(&span, "discovery_ms", discovery_started);
                    nodes
                }
            };
            span.record("node_count", nodes.len() as u64);
            fanout.nodes_queried = nodes.len();
            let fanout_started = Instant::now();
            let local_node_id = state.get_node_id();
            let mut node_iter = nodes.into_iter().enumerate();
            let mut pending = FuturesUnordered::new();
            let mut node_parts = Vec::new();

            loop {
                while pending.len() < METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT {
                    let Some((node_index, node_id)) = node_iter.next() else {
                        break;
                    };
                    let handle = handle.clone();
                    let auth = auth.clone();
                    let graph_iris = graph_iris.clone();
                    let query = query.clone();
                    let local = node_id == local_node_id;
                    pending.push(async move {
                        let node_span = debug_span!(
                            "metadata.api.query_node",
                            peer = ?node_id,
                            local,
                            elapsed_ms = field::Empty,
                            result = field::Empty,
                        );
                        let node_started = Instant::now();
                        // The coordinator's own partition runs in-process like
                        // mode=local; only remote partitions go over the wire
                        // and carry the per-node timeout.
                        let result = if local {
                            handle
                                .query_authorized_local(auth, graph_iris, query)
                                .instrument(node_span.clone())
                                .await
                        } else {
                            match tokio::time::timeout(
                                METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT,
                                handle
                                    .request_remote_query_graphs(node_id, auth, graph_iris, query)
                                    .instrument(node_span.clone()),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(MetadataError::Backend(format!(
                                    "distributed metadata query node timed out after {}ms",
                                    METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT.as_millis()
                                ))),
                            }
                        };
                        record_api_elapsed(&node_span, "elapsed_ms", node_started);
                        aruna_core::telemetry::record_stage_detail(
                            "fanout_node",
                            || short_node_id(node_id),
                            node_started.elapsed(),
                        );
                        match &result {
                            Ok(result) => {
                                node_span.record("result", metadata_query_result_kind(result));
                            }
                            Err(_) => {
                                node_span.record("result", "error");
                            }
                        }
                        (node_index, node_id, result)
                    });
                }

                let Some((node_index, node_id, result)) = pending.next().await else {
                    break;
                };
                match result {
                    Ok(result) => node_parts.push((node_index, result)),
                    Err(error) => {
                        fanout.nodes_failed += 1;
                        warn!(
                            node_id = ?node_id,
                            error = %error,
                            "distributed metadata query skipped failed node result"
                        );
                    }
                }
            }

            node_parts.sort_by_key(|(node_index, _)| *node_index);
            parts.extend(node_parts.into_iter().map(|(_, result)| result));
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
        }
    }

    let result = aggregate_query_results(parts, query_form, select_limit);
    record_api_elapsed(&span, "elapsed_ms", total_started);
    match &result {
        Ok(results) => {
            span.record("result", metadata_query_result_kind(results));
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
    result.map(|results| (results, fanout))
}

#[tracing::instrument(
    name = "metadata.api.search_distributed",
    level = "debug",
    skip(state, auth, query),
    fields(
        mode = ?mode,
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
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    mode: Option<MetadataQueryMode>,
) -> ServerResult<(Vec<MetadataSearchHit>, DistributedFanout)> {
    let span = Span::current();
    let total_started = Instant::now();
    ensure_supported_query_mode(&mode)?;
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;

    let mut hits = Vec::new();
    let mut fanout = DistributedFanout::default();
    match mode.unwrap_or(MetadataQueryMode::Distributed) {
        MetadataQueryMode::Local => {
            let node_span = debug_span!(
                "metadata.api.search_node",
                peer = ?state.get_node_id(),
                local = true,
                elapsed_ms = field::Empty,
                hit_count = field::Empty,
            );
            let node_started = Instant::now();
            let result = handle
                .search_authorized_local(auth, graph_iris, query, limit)
                .instrument(node_span.clone())
                .await;
            record_api_elapsed(&node_span, "elapsed_ms", node_started);
            fanout.nodes_queried = 1;
            match result {
                Ok(result) => {
                    node_span.record("hit_count", result.len() as u64);
                    hits.extend(result);
                }
                Err(error) => return Err(ServerError::InternalError(error.to_string())),
            }
        }
        MetadataQueryMode::Distributed => {
            let discovery_started = Instant::now();
            let nodes =
                aruna_core::telemetry::time_stage("discovery", load_realm_nodes(state)).await?;
            record_api_elapsed(&span, "discovery_ms", discovery_started);
            span.record("node_count", nodes.len() as u64);
            fanout.nodes_queried = nodes.len();
            let fanout_started = Instant::now();
            let local_node_id = state.get_node_id();
            let mut node_iter = nodes.into_iter();
            let mut pending = FuturesUnordered::new();

            loop {
                while pending.len() < METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT {
                    let Some(node_id) = node_iter.next() else {
                        break;
                    };
                    let handle = handle.clone();
                    let auth = auth.clone();
                    let graph_iris = graph_iris.clone();
                    let query = query.clone();
                    let local = node_id == local_node_id;
                    pending.push(async move {
                        let node_span = debug_span!(
                            "metadata.api.search_node",
                            peer = ?node_id,
                            local,
                            elapsed_ms = field::Empty,
                            hit_count = field::Empty,
                            result = field::Empty,
                        );
                        let node_started = Instant::now();
                        // The coordinator's own partition runs in-process like
                        // mode=local; only remote partitions go over the wire
                        // and carry the per-node timeout.
                        let result = if local {
                            handle
                                .search_authorized_local(auth, graph_iris, query, limit)
                                .instrument(node_span.clone())
                                .await
                        } else {
                            match tokio::time::timeout(
                                METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT,
                                handle
                                    .request_remote_search_graphs(
                                        node_id, auth, graph_iris, query, limit,
                                    )
                                    .instrument(node_span.clone()),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(MetadataError::Backend(format!(
                                    "distributed metadata search node timed out after {}ms",
                                    METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT.as_millis()
                                ))),
                            }
                        };
                        record_api_elapsed(&node_span, "elapsed_ms", node_started);
                        aruna_core::telemetry::record_stage_detail(
                            "fanout_node",
                            || short_node_id(node_id),
                            node_started.elapsed(),
                        );
                        match &result {
                            Ok(result) => {
                                node_span.record("result", "ok");
                                node_span.record("hit_count", result.len() as u64);
                            }
                            Err(_) => {
                                node_span.record("result", "error");
                            }
                        }
                        (node_id, result)
                    });
                }

                let Some((node_id, result)) = pending.next().await else {
                    break;
                };
                match result {
                    Ok(result) => hits.extend(result),
                    Err(error) => {
                        fanout.nodes_failed += 1;
                        warn!(
                            node_id = ?node_id,
                            error = %error,
                            "distributed metadata search skipped failed node result"
                        );
                    }
                }
            }
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
        }
    }

    let hits = deduplicate_search_hits(hits, limit);
    span.record("hit_count", hits.len() as u64);
    record_api_elapsed(&span, "elapsed_ms", total_started);
    Ok((hits, fanout))
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
struct DistributedFanout {
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
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::structs::{
        Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument, RealmId,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::metadata::MetadataHandle;
    use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
    use aruna_operations::metadata::projector::replay_metadata_event_log;
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
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

        let document_id = created.summary.document_id.clone();
        drain_metadata_background(test.state.as_ref()).await;
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
    async fn list_metadata_documents_serves_records_from_visible_registry_cache() {
        let test = setup_state().await;
        let ctx = test.state.get_ctx();
        visible_registry::invalidate_visible_registry(ctx.as_ref());

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: test.group_id.to_string(),
                    path: "datasets/cache-served".to_string(),
                    name: "Cache Served Dataset".to_string(),
                    description: "Served from the visible registry cache".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        // Drop the accepted-create overlay so the listing below can only come
        // from the visible-registry cache fill.
        let document_id = parse_document_id(&created.summary.document_id).unwrap();
        ctx.metadata_handle
            .as_ref()
            .unwrap()
            .remove_cached_accepted_create(document_id);
        visible_registry::invalidate_visible_registry(ctx.as_ref());

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

    #[test]
    fn document_query_target_nodes_use_deduplicated_holders() {
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
            document_query_target_nodes(&record, local_node_id),
            vec![remote_node_id, local_node_id]
        );

        let mut empty_holders = record;
        empty_holders.holder_node_ids.clear();
        assert_eq!(
            document_query_target_nodes(&empty_holders, local_node_id),
            vec![local_node_id]
        );
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

        // No drain_metadata_background: the craqle graph does not exist yet.
        let result = export_metadata_rocrate(
            State(test.state.clone()),
            Extension(None),
            Path(created.summary.document_id.clone()),
            Query(MetadataRoCrateExportParams::default()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::ServiceUnavailable)));

        drain_metadata_background(test.state.as_ref()).await;
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
        assert!(matches!(roundtrip.result, MetadataQueryResult::Boolean(true)));
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
            10,
        );

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].graph_iri, "https://w3id.org/aruna/01A");
        assert_eq!(hits[0].score, 0.8);
    }

    async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let metadata_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let realm_id = aruna_core::structs::RealmId([3u8; 32]);
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
        replay_metadata_event_log(ctx.as_ref()).await.unwrap();
        process_metadata_materialization_batch(ctx.as_ref())
            .await
            .unwrap();
    }

    async fn setup_state_with_closed_storage() -> Arc<ServerState> {
        let (storage_handle, receiver) = storage::StorageHandle::new();
        drop(receiver);

        let realm_id = RealmId([3u8; 32]);
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
}
