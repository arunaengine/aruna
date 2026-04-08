use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::{
    MetadataEffect, MetadataEvent, MetadataQueryResults, MetadataRoCratePage, MetadataSearchHit,
};
use aruna_core::structs::{Actor, AuthContext, MetadataRegistryRecord, Permission};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::driver::drive;
use aruna_operations::list_metadata_documents::ListMetadataDocumentsOperation;
use aruna_operations::metadata::repository::{
    iter_all_registry_effect, parse_registry_iter, parse_registry_read, read_registry_by_document_effect,
};
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentOperation,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use ulid::Ulid;
use url::form_urlencoded::Serializer;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "metadata", description = "Metadata RO-Crate and SPARQL operations")),
    paths(
        create_metadata_document,
        list_metadata_documents,
        get_metadata_document,
        delete_metadata_document,
        search_metadata,
        export_metadata_rocrate,
        replace_metadata_rocrate,
        query_metadata_document,
        query_all_metadata
    )
)]
pub struct MetadataApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/metadata", post(create_metadata_document))
        .route("/metadata/search", get(search_metadata))
        .route("/metadata/sparql/query", post(query_all_metadata))
        .route("/groups/{group_id}/metadata", get(list_metadata_documents))
        .route("/metadata/{document_id}", get(get_metadata_document).delete(delete_metadata_document))
        .route(
            "/metadata/{document_id}/rocrate",
            get(export_metadata_rocrate).put(replace_metadata_rocrate),
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
    pub holder_count: usize,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateMetadataRequest {
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
pub struct CreateMetadataResponse {
    #[serde(flatten)]
    pub summary: MetadataDocumentSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListMetadataResponse {
    pub documents: Vec<MetadataDocumentSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceMetadataRoCrateRequest {
    pub jsonld: String,
    #[serde(default)]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataRoCrateResponse {
    pub jsonld: String,
    pub total_data_entities: Option<usize>,
    pub returned_data_entities: Option<usize>,
    pub next_offset: Option<usize>,
    pub next_cursor: Option<String>,
}

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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SparqlQueryRequest {
    pub query: String,
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MetadataQueryMode {
    Local,
    Distributed,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", content = "value")]
pub enum MetadataQueryResponse {
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
            holder_count: record.holder_node_ids.len(),
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
        }
    }
}

#[utoipa::path(
    post,
    path = "/metadata",
    tag = "metadata",
    request_body = CreateMetadataRequest,
    responses(
        (status = 201, description = "Metadata document created", body = CreateMetadataResponse),
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
    let group_id = parse_group_id(&request.group_id)?;
    if MetadataRegistryRecord::normalize_document_path(&request.path).is_empty() {
        return Err(ServerError::BadRequest);
    }
    ensure_metadata_write_scope(&state, &auth, group_id).await?;

    let result = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            group_id,
            document_id: Ulid::new(),
            document_path: request.path,
            name: request.name,
            description: request.description,
            date_published: request.date_published,
            license: request.license,
            public: request.public,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(CreateMetadataResponse {
            summary: MetadataDocumentSummary::from(&result),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/metadata",
    tag = "metadata",
    params(("group_id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "Visible metadata documents", body = ListMetadataResponse),
        (status = 400, description = "Invalid group id", body = ErrorResponse)
    )
)]
pub async fn list_metadata_documents(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ListMetadataResponse>)> {
    let group_id = parse_group_id(&group_id)?;
    let records = drive(ListMetadataDocumentsOperation::new(group_id), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    let mut visible = Vec::new();
    for record in records {
        if can_read_record(&state, auth.as_ref(), &record).await? {
            visible.push(MetadataDocumentSummary::from(&record));
        }
    }

    Ok((StatusCode::OK, Json(ListMetadataResponse { documents: visible })))
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
        &state.get_ctx(),
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
        (status = 200, description = "RO-Crate export", body = MetadataRoCrateResponse),
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
            jsonld: export_rocrate_jsonld(&state, &record.graph_iri).await?,
            total_data_entities: None,
            returned_data_entities: None,
            next_offset: None,
            next_cursor: None,
        },
        MetadataRoCrateView::Summary => MetadataRoCrateResponse {
            jsonld: rewrite_view_jsonld(
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
    request_body = ReplaceMetadataRoCrateRequest,
    responses(
        (status = 200, description = "Metadata document updated", body = MetadataDocumentSummary),
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
            jsonld: request.jsonld,
            public: request.public.unwrap_or(record.public),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((StatusCode::OK, Json(MetadataDocumentSummary::from(&updated))))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/sparql/query",
    tag = "metadata",
    params(("document_id" = String, Path, description = "Metadata document id")),
    request_body = SparqlQueryRequest,
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
    ensure_supported_query_mode(&request.mode)?;
    ensure_supported_query_form(&request.query)?;

    let document_id = parse_document_id(&document_id)?;
    let record = load_metadata_record_by_document(&state, document_id).await?;
    ensure_record_readable(&state, auth.as_ref(), &record).await?;

    let results = run_query(&state, vec![record.graph_iri.clone()], request.query).await?;
    Ok((StatusCode::OK, Json(map_query_results(results)?)))
}

#[utoipa::path(
    post,
    path = "/metadata/sparql/query",
    tag = "metadata",
    request_body = SparqlQueryRequest,
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
    ensure_supported_query_mode(&request.mode)?;
    ensure_supported_query_form(&request.query)?;

    let records = load_all_metadata_records(&state).await?;

    let mut graph_iris = Vec::new();
    for record in records {
        if can_read_record(&state, auth.as_ref(), &record).await? {
            graph_iris.push(record.graph_iri);
        }
    }

    let results = run_query(&state, graph_iris, request.query).await?;
    Ok((StatusCode::OK, Json(map_query_results(results)?)))
}

#[utoipa::path(
    get,
    path = "/metadata/search",
    tag = "metadata",
    params(
        ("q" = String, Query, description = "Search query"),
        ("limit" = Option<usize>, Query, description = "Maximum number of hits")
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
    let records = load_all_metadata_records(&state).await?;
    let mut visible = Vec::new();
    let mut by_graph = HashMap::new();
    for record in records {
        if can_read_record(&state, auth.as_ref(), &record).await? {
            by_graph.insert(record.graph_iri.clone(), record.clone());
            visible.push(record.graph_iri);
        }
    }
    let hits = run_search(&state, visible, params.q, limit).await?;
    Ok((
        StatusCode::OK,
        Json(MetadataSearchResponse {
            hits: hits
                .into_iter()
                .filter_map(|hit| {
                    by_graph.get(&hit.graph_iri).map(|record| MetadataSearchHitResponse {
                        document_id: record.document_id.to_string(),
                        group_id: record.group_id.to_string(),
                        document_path: record.document_path.clone(),
                        graph_iri: hit.graph_iri,
                        subject_iri: hit.subject_iri,
                        score: hit.score,
                    })
                })
                .collect(),
        }),
    ))
}

fn parse_group_id(group_id: &str) -> ServerResult<Ulid> {
    Ulid::from_string(group_id).map_err(|_| ServerError::BadRequest)
}

fn parse_document_id(document_id: &str) -> ServerResult<Ulid> {
    Ulid::from_string(document_id).map_err(|_| ServerError::BadRequest)
}

fn require_realm_auth(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.realm_id != state.get_realm_id() {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
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
    ensure_permission(state, auth, record.permission_path.clone(), Permission::READ).await
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

    match drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth,
            path: record.permission_path.clone(),
            required_permission: Permission::READ,
        }),
        &state.get_ctx(),
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
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth,
            path,
            required_permission,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
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
        Ok(None) => Err(ServerError::NotFound),
        Err(crate::routes::metadata::ReadError::Storage(_)) => unreachable!(),
        Err(_) => Err(ServerError::InternalError(
            "failed to read metadata record".to_string(),
        )),
    }
}

async fn load_all_metadata_records(state: &ServerState) -> ServerResult<Vec<MetadataRegistryRecord>> {
    let mut records = Vec::new();
    let mut start_after = None;
    loop {
        let event = state
            .get_ctx()
            .storage_handle
            .send_effect(iter_all_registry_effect(start_after.clone(), None))
            .await;
        match parse_registry_iter(event) {
            Ok((mut page, next_start_after)) => {
                records.append(&mut page);
                if let Some(cursor) = next_start_after {
                    start_after = Some(cursor);
                } else {
                    return Ok(records);
                }
            }
            Err(_) => {
                return Err(ServerError::InternalError(
                    "failed to iterate metadata records".to_string(),
                ));
            }
        }
    }
}

type ReadError = aruna_operations::metadata::repository::StorageReadError;

async fn export_rocrate_jsonld(state: &ServerState, graph_iri: &str) -> ServerResult<String> {
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
        Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => Ok(jsonld),
        Event::Metadata(MetadataEvent::Error { error, .. }) => {
            Err(ServerError::InternalError(error.to_string()))
        }
        other => Err(ServerError::InternalError(format!(
            "unexpected metadata export event: {other:?}"
        ))),
    }
}

async fn export_rocrate_summary_jsonld(
    state: &ServerState,
    graph_iri: &str,
) -> ServerResult<String> {
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
        Event::Metadata(MetadataEvent::RoCrateSummaryResult { jsonld, .. }) => Ok(jsonld),
        Event::Metadata(MetadataEvent::Error { error, .. }) => {
            Err(ServerError::InternalError(error.to_string()))
        }
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
        Event::Metadata(MetadataEvent::Error { error, .. }) => {
            Err(ServerError::InternalError(error.to_string()))
        }
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
        jsonld: rewrite_view_jsonld(page.jsonld, graph_iri, view_id)?,
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

fn rewrite_view_jsonld(jsonld: String, graph_iri: &str, view_id: &str) -> ServerResult<String> {
    let mut value: serde_json::Value =
        serde_json::from_str(&jsonld).map_err(|_| ServerError::InternalError("invalid jsonld export".to_string()))?;
    rewrite_identifier_value(&mut value, None, graph_iri, view_id);
    serde_json::to_string(&value)
        .map_err(|err| ServerError::InternalError(err.to_string()))
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
                rewrite_identifier_value(child_value, Some(child_key), canonical_id, replacement_id);
            }
        }
        _ => {}
    }
}

async fn run_query(
    state: &ServerState,
    graph_iris: Vec<String>,
    query: String,
) -> ServerResult<MetadataQueryResults> {
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    match handle
        .send_effect(Effect::Metadata(MetadataEffect::QueryGraphs {
            graph_iris,
            sparql: query,
        }))
        .await
    {
        Event::Metadata(MetadataEvent::QueryResult { results }) => Ok(results),
        Event::Metadata(MetadataEvent::Error { error, .. }) => {
            Err(ServerError::InternalError(error.to_string()))
        }
        other => Err(ServerError::InternalError(format!(
            "unexpected metadata query event: {other:?}"
        ))),
    }
}

async fn run_search(
    state: &ServerState,
    graph_iris: Vec<String>,
    query: String,
    limit: usize,
) -> ServerResult<Vec<MetadataSearchHit>> {
    let handle = state
        .get_ctx()
        .metadata_handle
        .clone()
        .ok_or_else(|| ServerError::InternalError("metadata handle unavailable".to_string()))?;
    match handle
        .send_effect(Effect::Metadata(MetadataEffect::SearchGraphs {
            graph_iris,
            query,
            limit,
        }))
        .await
    {
        Event::Metadata(MetadataEvent::SearchResult { hits }) => Ok(hits),
        Event::Metadata(MetadataEvent::Error { error, .. }) => {
            Err(ServerError::InternalError(error.to_string()))
        }
        other => Err(ServerError::InternalError(format!(
            "unexpected metadata search event: {other:?}"
        ))),
    }
}

fn ensure_supported_query_mode(mode: &Option<MetadataQueryMode>) -> ServerResult<()> {
    match mode {
        None | Some(MetadataQueryMode::Local) => Ok(()),
        Some(MetadataQueryMode::Distributed) => Err(ServerError::Unimplemented),
    }
}

fn ensure_supported_query_form(query: &str) -> ServerResult<()> {
    match query_form(query) {
        Some(QueryForm::Select | QueryForm::Ask) => Ok(()),
        _ => Err(ServerError::BadRequest),
    }
}

fn map_query_results(results: MetadataQueryResults) -> ServerResult<MetadataQueryResponse> {
    match results {
        MetadataQueryResults::Solutions(rows) => Ok(MetadataQueryResponse::Solutions(
            rows.into_iter()
                .map(|row| row.into_iter().collect::<HashMap<_, _>>())
                .collect(),
        )),
        MetadataQueryResults::Boolean(value) => Ok(MetadataQueryResponse::Boolean(value)),
        MetadataQueryResults::Graph(_) => Err(ServerError::BadRequest),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryForm {
    Select,
    Ask,
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
            remaining = trimmed
                .split_once('\n')
                .map(|(_, tail)| tail)
                .unwrap_or("");
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
    use aruna_core::structs::{Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::metadata::MetadataHandle;
    use aruna_storage::storage;
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
            Json(CreateMetadataRequest {
                group_id: test.group_id.to_string(),
                path: "datasets/public-dataset".to_string(),
                name: "Public Dataset".to_string(),
                description: "Visible metadata".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                public: true,
            }),
        )
        .await
        .unwrap();

        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(listed.documents.len(), 1);
        assert_eq!(listed.documents[0].document_id, created.summary.document_id);

        let document_id = created.summary.document_id.clone();
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
                jsonld: paged_jsonld,
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
        assert!(response
            .jsonld
            .contains(&format!("https://w3id.org/aruna/{document_id}")));

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
        assert!(summary.jsonld.contains(&format!(
            "https://w3id.org/aruna/{document_id}?view=summary"
        )));
        assert!(!summary.jsonld.contains("file-0.txt"));

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
        assert!(page.jsonld.contains(&format!(
            "https://w3id.org/aruna/{document_id}?view=page&limit=2&offset=0"
        )));
        assert_eq!(page.total_data_entities, Some(3));
        assert_eq!(page.returned_data_entities, Some(2));
        assert_eq!(page.next_offset, Some(2));
        assert!(page.next_cursor.is_some());
        assert!(page.jsonld.contains("file-0.txt") || page.jsonld.contains("file-1.txt"));

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
        assert!(matches!(result, MetadataQueryResponse::Boolean(true)));

        let (_, Json(search)) = search_metadata(
            State(test.state.clone()),
            Extension(None),
            Query(MetadataSearchParams {
                q: "Public".to_string(),
                limit: Some(10),
            }),
        )
        .await
        .unwrap();
        assert!(!search.hits.is_empty());
    }

    #[tokio::test]
    async fn private_metadata_is_hidden_without_auth() {
        let test = setup_state().await;

        let (_, Json(created)) = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(CreateMetadataRequest {
                group_id: test.group_id.to_string(),
                path: "datasets/private-dataset".to_string(),
                name: "Private Dataset".to_string(),
                description: "Private metadata".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                public: false,
            }),
        )
        .await
        .unwrap();

        let (_, Json(listed)) = list_metadata_documents(
            State(test.state.clone()),
            Extension(None),
            Path(test.group_id.to_string()),
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

    async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let metadata_dir = tempfile::tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let realm_id = aruna_core::structs::RealmId([3u8; 32]);
        let user_id = Ulid::new();
        let actor = Actor {
            node_id,
            user_id,
            realm_id: realm_id.clone(),
        };
        let metadata_handle = MetadataHandle::new(metadata_dir.path(), node_id).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: None,
        });
        let group_id = Ulid::new();
        let group_auth = GroupAuthorizationDocument::new_default_group_doc(
            user_id,
            realm_id.clone(),
            group_id,
        );
        let group = Group {
            display_name: "metadata-group".to_string(),
            group_id,
            realm_id: realm_id.clone(),
            roles: group_auth.roles.keys().copied().collect(),
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id.clone());

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
                realm_id.clone(),
                node_id,
                NodeCapabilities::local_node(realm_id.clone()).unwrap(),
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
