use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::short_display_id;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataError, MetadataGraphLifecycleRecord, MetadataQueryResults,
    MetadataRoCratePage, MetadataSearchHit,
};
use aruna_core::storage_entries::{
    metadata_event_log_key, metadata_graph_lifecycle_key, metadata_pending_projection_target,
};
use aruna_core::structs::{
    AuthContext, MetadataRegistryRecord, Permission, RealmConfigDocument, RealmId,
};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::GroupId;
use futures_util::StreamExt;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream;
use thiserror::Error;
use tracing::{Instrument, Span, debug_span, field, warn};
use ulid::Ulid;

use super::MetadataAuthToken;
use super::search_cursor::{
    METADATA_SEARCH_DEFAULT_PAGE_SIZE, METADATA_SEARCH_MAX_PAGE_SIZE,
    METADATA_SEARCH_MAX_PAGINATION_DEPTH, NodeSearchResult, SearchCursor, SearchCursorError,
    SearchPageCursor, SearchWatermark, paginate, query_fingerprint, resume_fetch_limit,
};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::get_metadata_document::{
    is_metadata_record_materialized_for_graph_read, load_metadata_record_by_document,
};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::get_realm_nodes::GetRealmNodesOperation;
use crate::list_groups::ListGroupOperation;
use crate::list_metadata_documents::ListMetadataDocumentsOperation;
use crate::metadata::repository::{LIST_METADATA_PAGE_SIZE, StorageReadError};
use crate::placement::resolve_shard_holders;

const DEFAULT_LIST_METADATA_LIMIT: usize = 50;
const MAX_LIST_METADATA_LIMIT: usize = 1_000;
const METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT: usize = 8;
const METADATA_DISTRIBUTED_QUERY_NODE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum MetadataApiError {
    #[error("bad request")]
    BadRequest,
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("not found")]
    NotFound,
    #[error("service unavailable")]
    ServiceUnavailable,
    #[error("{0}")]
    InvalidCursor(String),
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Clone)]
pub struct ListVisibleMetadataDocumentsRequest {
    pub group_id: Option<GroupId>,
    pub path_prefix: Option<String>,
    pub include_summary: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone)]
pub struct ListedMetadataDocument {
    pub record: MetadataRegistryRecord,
    pub rocrate_summary_jsonld: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListVisibleMetadataDocumentsResult {
    pub documents: Vec<ListedMetadataDocument>,
    pub limit: usize,
    pub offset: usize,
    pub total_returned: usize,
}

#[derive(Debug, Clone)]
pub struct GetVisibleMetadataDocumentRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataRoCrateExportView {
    Full,
    Summary,
    Page,
}

#[derive(Debug, Clone)]
pub struct ExportMetadataRoCrateRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub view: MetadataRoCrateExportView,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub after: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ExportMetadataRoCrateResult {
    Full {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Summary {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Page {
        record: MetadataRegistryRecord,
        page: MetadataRoCratePage,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataApiQueryMode {
    Local,
    Distributed,
}

#[derive(Debug, Clone)]
pub struct MetadataDocumentQueryRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct MetadataSearchRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryExecution {
    pub results: MetadataQueryResults,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct MetadataSearchExecution {
    pub hits: Vec<MetadataSearchHit>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetadataFanoutStats {
    pub nodes_queried: usize,
    pub nodes_failed: usize,
}

#[derive(Debug, Clone)]
struct MetadataRealmNodeDiscovery {
    nodes: Vec<NodeId>,
    failed: bool,
}

#[derive(Debug)]
struct MetadataFanoutScope {
    mode: Option<MetadataApiQueryMode>,
    target_nodes: Option<Vec<NodeId>>,
    discovery_failed: bool,
}

impl MetadataFanoutScope {
    fn new(mode: Option<MetadataApiQueryMode>, target_nodes: Option<Vec<NodeId>>) -> Self {
        Self {
            mode,
            target_nodes,
            discovery_failed: false,
        }
    }

    fn with_discovery_failed(mut self, discovery_failed: bool) -> Self {
        self.discovery_failed = discovery_failed;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataQueryForm {
    Select,
    Ask,
}

pub async fn list_visible_metadata_documents(
    context: &DriverContext,
    realm_id: RealmId,
    request: ListVisibleMetadataDocumentsRequest,
) -> Result<ListVisibleMetadataDocumentsResult, MetadataApiError> {
    let limit = request
        .limit
        .unwrap_or(DEFAULT_LIST_METADATA_LIMIT)
        .clamp(1, MAX_LIST_METADATA_LIMIT);
    let offset = request.offset.unwrap_or(0);

    let mut records = Vec::new();
    let include_pending_projection = request.include_summary;
    if let Some(group_id) = request.group_id {
        records.extend(
            load_group_metadata_records(context, group_id, include_pending_projection).await?,
        );
    } else {
        let groups = drive(ListGroupOperation::new(), context)
            .await
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        for group in groups {
            records.extend(
                load_group_metadata_records(context, group.group_id, include_pending_projection)
                    .await?,
            );
        }
    }

    let needed = offset.saturating_add(limit);
    let mut selected = Vec::with_capacity(limit.min(records.len()));
    let mut visible_count = 0usize;
    for record in records {
        if !metadata_record_matches_filters(&record, request.path_prefix.as_deref()) {
            continue;
        }
        if !can_read_record(context, realm_id, request.auth.as_ref(), &record).await? {
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
    if request.include_summary {
        let summaries = futures_util::future::join_all(
            selected
                .iter()
                .map(|record| export_rocrate_summary_jsonld(context, &record.graph_iri)),
        )
        .await;
        for (record, summary) in selected.into_iter().zip(summaries) {
            let rocrate_summary_jsonld = match summary {
                Ok(summary) => Some(summary),
                Err(MetadataApiError::ServiceUnavailable) => None,
                Err(error) => return Err(error),
            };
            documents.push(ListedMetadataDocument {
                record,
                rocrate_summary_jsonld,
            });
        }
    } else {
        documents.extend(selected.into_iter().map(|record| ListedMetadataDocument {
            record,
            rocrate_summary_jsonld: None,
        }));
    }

    let total_returned = documents.len();
    Ok(ListVisibleMetadataDocumentsResult {
        documents,
        limit,
        offset,
        total_returned,
    })
}

pub async fn get_visible_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    request: GetVisibleMetadataDocumentRequest,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record).await?;
    ensure_record_materialized_for_graph_read(context, &record).await?;
    Ok(record)
}

pub async fn export_metadata_rocrate(
    context: &DriverContext,
    realm_id: RealmId,
    request: ExportMetadataRoCrateRequest,
) -> Result<ExportMetadataRoCrateResult, MetadataApiError> {
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record).await?;
    ensure_record_materialized_for_graph_read(context, &record).await?;

    match request.view {
        MetadataRoCrateExportView::Full => Ok(ExportMetadataRoCrateResult::Full {
            jsonld: export_rocrate_jsonld(context, &record.graph_iri).await?,
            record,
        }),
        MetadataRoCrateExportView::Summary => Ok(ExportMetadataRoCrateResult::Summary {
            jsonld: export_rocrate_summary_jsonld(context, &record.graph_iri).await?,
            record,
        }),
        MetadataRoCrateExportView::Page => Ok(ExportMetadataRoCrateResult::Page {
            page: export_rocrate_page(
                context,
                &record.graph_iri,
                request.limit,
                request.offset,
                request.after,
            )
            .await?,
            record,
        }),
    }
}

pub async fn query_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataDocumentQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    ensure_supported_query_form(&request.query)?;
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record).await?;
    ensure_record_materialized_for_graph_read(context, &record).await?;
    let config = load_realm_config(context, realm_id).await;

    query_metadata(
        context,
        realm_id,
        local_node_id,
        MetadataQueryRequest {
            auth: request.auth,
            bearer_token: request.bearer_token,
            graph_iris: Some(vec![record.graph_iri.clone()]),
            query: request.query,
            mode: request.mode,
            target_nodes: Some(document_replica_query_nodes(
                config.as_ref(),
                &record,
                local_node_id,
            )),
        },
    )
    .await
}

pub async fn query_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    ensure_supported_query_form(&request.query)?;
    let (results, fanout_stats) = run_query_distributed(
        context,
        realm_id,
        local_node_id,
        request.auth,
        request.bearer_token,
        request.graph_iris,
        request.query,
        MetadataFanoutScope::new(request.mode, request.target_nodes),
    )
    .await?;
    Ok(MetadataQueryExecution {
        results,
        fanout_stats,
    })
}

pub async fn search_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataSearchRequest,
) -> Result<MetadataSearchExecution, MetadataApiError> {
    if request.query.trim().is_empty() {
        return Err(MetadataApiError::BadRequest);
    }
    let page_size = request
        .limit
        .unwrap_or(METADATA_SEARCH_DEFAULT_PAGE_SIZE)
        .clamp(1, METADATA_SEARCH_MAX_PAGE_SIZE);

    let fingerprint =
        query_fingerprint(&request.query, request.graph_iris.as_deref(), request.mode);
    let mut cursor_discovery = None;
    let (watermark, resume) = match request.cursor.as_deref() {
        Some(raw) => {
            let signer_nodes = match request.mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => match request.target_nodes.clone() {
                    Some(nodes) => fanout_nodes_with_local(nodes, local_node_id),
                    None => {
                        let discovery =
                            load_metadata_realm_nodes_with_status(context, realm_id, local_node_id)
                                .await;
                        let nodes = discovery.nodes.clone();
                        cursor_discovery = Some(discovery);
                        nodes
                    }
                },
            };
            let cursor = SearchCursor::decode(raw, &signer_nodes)
                .map_err(|error| MetadataApiError::InvalidCursor(error.to_string()))?;
            if cursor.fingerprint != fingerprint {
                return Err(MetadataApiError::InvalidCursor(
                    SearchCursorError::QueryMismatch.to_string(),
                ));
            }
            (Some(cursor.watermark.clone()), cursor.resume_positions())
        }
        None => (None, HashMap::new()),
    };

    // On a continuation, attempt every node in the resume map even if realm
    // discovery no longer reports it, so its remaining hits are not skipped.
    let (target_nodes, discovery_failed) = if request.cursor.is_some() {
        let mut nodes = match request.target_nodes.clone() {
            Some(nodes) => nodes,
            None => match request.mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => cursor_discovery
                    .as_ref()
                    .map(|discovery| discovery.nodes.clone())
                    .unwrap_or_else(|| vec![local_node_id]),
            },
        };
        for node_id in resume.keys() {
            if !nodes.contains(node_id) {
                nodes.push(*node_id);
            }
        }
        (
            Some(deduplicate_fanout_nodes(nodes)),
            cursor_discovery
                .as_ref()
                .is_some_and(|discovery| discovery.failed),
        )
    } else {
        (request.target_nodes.clone(), false)
    };

    let (hits, next, truncated, fanout_stats) = run_search_distributed(
        context,
        realm_id,
        local_node_id,
        request.auth,
        request.bearer_token,
        request.graph_iris,
        request.query,
        resume,
        watermark,
        page_size,
        MetadataFanoutScope::new(request.mode, target_nodes)
            .with_discovery_failed(discovery_failed),
    )
    .await?;
    let next_cursor = match next {
        Some(cursor) => {
            let net = context.net_handle.as_ref().ok_or_else(|| {
                MetadataApiError::Internal(
                    "net handle unavailable for search cursor signing".to_string(),
                )
            })?;
            Some(
                SearchCursor::new_signed(
                    fingerprint,
                    cursor.watermark,
                    cursor.resume,
                    net.node_id(),
                    |bytes| net.sign(bytes),
                )
                .encode(),
            )
        }
        None => None,
    };
    Ok(MetadataSearchExecution {
        hits,
        next_cursor,
        truncated,
        fanout_stats,
    })
}

pub async fn load_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    match drive(GetRealmConfigOperation::new(realm_id), context).await {
        Ok(config) => Some(config),
        Err(error) => {
            warn!(error = %error, "realm config unavailable; querying the local replica only");
            None
        }
    }
}

pub async fn load_metadata_realm_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> Vec<NodeId> {
    load_metadata_realm_nodes_with_status(context, realm_id, local_node_id)
        .await
        .nodes
}

async fn load_metadata_realm_nodes_with_status(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> MetadataRealmNodeDiscovery {
    let nodes = match drive(GetRealmNodesOperation::new(realm_id), context).await {
        Ok(nodes) => {
            let mut nodes = nodes.into_iter().collect::<Vec<_>>();
            if !nodes.contains(&local_node_id) {
                nodes.push(local_node_id);
            }
            nodes.sort_by_key(|node_id| node_id.to_string());
            return MetadataRealmNodeDiscovery {
                nodes,
                failed: false,
            };
        }
        Err(error) => {
            warn!(
                error = %error,
                "realm node discovery failed, using best-effort local-only metadata results"
            );
            HashSet::new()
        }
    };
    let mut nodes = nodes.into_iter().collect::<Vec<_>>();
    if !nodes.contains(&local_node_id) {
        nodes.push(local_node_id);
    }
    nodes.sort_by_key(|node_id| node_id.to_string());
    MetadataRealmNodeDiscovery {
        nodes,
        failed: true,
    }
}

async fn load_group_metadata_records(
    context: &DriverContext,
    group_id: GroupId,
    include_pending_projection: bool,
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    // Listing remains eventually consistent: the handle-owned visibility cache
    // serves stale snapshots while one refill updates the operation-owned read path.
    if !include_pending_projection && let Some(metadata_handle) = context.metadata_handle.as_ref() {
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

    let mut records = drive(ListMetadataDocumentsOperation::new(group_id), context)
        .await
        .map_err(|err| MetadataApiError::Internal(err.to_string()))?;

    if include_pending_projection {
        merge_pending_metadata_records(
            &mut records,
            load_pending_group_metadata_records(context, group_id).await?,
        );
        records.sort_by_key(|record| record.document_id);
    }

    Ok(records)
}

async fn load_pending_group_metadata_records(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    let mut records = Vec::new();
    let mut start_after = None;

    loop {
        let page = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: LIST_METADATA_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match page {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataApiError::Internal(error.to_string()));
            }
            other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
        };

        for (key, _) in values {
            let Some((document_id, event_id)) = metadata_pending_projection_target(key.as_ref())
            else {
                continue;
            };
            let Some(event) = read_metadata_create_event(context, document_id, event_id).await?
            else {
                continue;
            };
            let record = event.record;
            if record.group_id != group_id {
                continue;
            }
            if metadata_graph_is_deleted(context, &record.graph_iri).await? {
                continue;
            }
            records.push(record);
        }

        if next_start_after.is_none() {
            break;
        }
        start_after = next_start_after;
    }

    Ok(records)
}

async fn read_metadata_create_event(
    context: &DriverContext,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<Option<MetadataCreateEventRecord>, MetadataApiError> {
    let value = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
            key: metadata_event_log_key(document_id, event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataApiError::Internal(error.to_string()));
        }
        other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
    };
    let Some(value) = value else {
        return Ok(None);
    };

    let event: MetadataCreateEventRecord = postcard::from_bytes(&value)
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    if event.record.document_id != document_id || event.event_id != event_id {
        return Err(MetadataApiError::Internal(format!(
            "metadata create event log target {document_id}/{event_id} did not match payload {}/{}",
            event.record.document_id, event.event_id
        )));
    }
    Ok(Some(event))
}

async fn metadata_graph_is_deleted(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<bool, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(graph_iri),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&value)
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
            Ok(record.is_deleted())
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
        other => Err(MetadataApiError::Internal(format!("{other:?}"))),
    }
}

fn merge_pending_metadata_records(
    records: &mut Vec<MetadataRegistryRecord>,
    pending_records: Vec<MetadataRegistryRecord>,
) {
    let mut positions = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.document_id, index))
        .collect::<HashMap<_, _>>();

    for pending_record in pending_records {
        if let Some(&index) = positions.get(&pending_record.document_id) {
            let existing_record = &records[index];
            if (pending_record.updated_at_ms, pending_record.last_event_id)
                > (existing_record.updated_at_ms, existing_record.last_event_id)
            {
                records[index] = pending_record;
            }
        } else {
            positions.insert(pending_record.document_id, records.len());
            records.push(pending_record);
        }
    }
}

async fn load_record_by_document(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    match load_metadata_record_by_document(context, document_id).await {
        Ok(Some(record)) => Ok(record),
        Ok(None) => Err(MetadataApiError::NotFound),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

async fn ensure_record_materialized_for_graph_read(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<(), MetadataApiError> {
    match is_metadata_record_materialized_for_graph_read(context, record).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(MetadataApiError::ServiceUnavailable),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

async fn ensure_record_readable(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
) -> Result<(), MetadataApiError> {
    if record.public {
        return Ok(());
    }
    let Some(auth) = auth.cloned() else {
        return Err(MetadataApiError::Unauthorized);
    };
    ensure_permission(
        context,
        realm_id,
        auth,
        record.permission_path.clone(),
        Permission::READ,
    )
    .await
}

async fn can_read_record(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
) -> Result<bool, MetadataApiError> {
    if record.public {
        return Ok(true);
    }
    let Some(auth) = auth.cloned() else {
        return Ok(false);
    };
    if auth.realm_id != realm_id {
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
            context,
        ),
    )
    .await
    {
        Ok(allowed) => Ok(allowed),
        Err(_) => Ok(false),
    }
}

async fn ensure_permission(
    context: &DriverContext,
    realm_id: RealmId,
    auth: AuthContext,
    path: String,
    required_permission: Permission,
) -> Result<(), MetadataApiError> {
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let allowed = aruna_core::telemetry::time_stage(
        "permission",
        drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth,
                path,
                required_permission,
            }),
            context,
        ),
    )
    .await
    .map_err(|err| match err {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => MetadataApiError::Forbidden,
        _ => MetadataApiError::Internal(err.to_string()),
    })?;
    if allowed {
        Ok(())
    } else {
        Err(MetadataApiError::Forbidden)
    }
}

fn metadata_record_matches_filters(
    record: &MetadataRegistryRecord,
    path_prefix: Option<&str>,
) -> bool {
    path_prefix
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

async fn export_rocrate_jsonld(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<String, MetadataApiError> {
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_jsonld(graph_iri.to_string())
        .await
        .map_err(map_metadata_event_error)
}

async fn export_rocrate_summary_jsonld(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<String, MetadataApiError> {
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_summary_jsonld(graph_iri.to_string())
        .await
        .map_err(map_metadata_event_error)
}

async fn export_rocrate_page(
    context: &DriverContext,
    graph_iri: &str,
    limit: Option<usize>,
    offset: Option<usize>,
    after: Option<String>,
) -> Result<MetadataRoCratePage, MetadataApiError> {
    if offset.is_some() && after.is_some() {
        return Err(MetadataApiError::BadRequest);
    }
    let limit = limit.unwrap_or(100).clamp(1, 1_000);
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    handle
        .export_rocrate_page(graph_iri.to_string(), limit, offset, after)
        .await
        .map_err(map_metadata_event_error)
}

fn map_metadata_event_error(error: MetadataError) -> MetadataApiError {
    match error {
        MetadataError::GraphNotFound => MetadataApiError::ServiceUnavailable,
        other => MetadataApiError::Internal(other.to_string()),
    }
}

fn map_metadata_internal_error(error: MetadataError) -> MetadataApiError {
    MetadataApiError::Internal(error.to_string())
}

fn ensure_supported_query_mode(mode: &Option<MetadataApiQueryMode>) {
    match mode {
        None | Some(MetadataApiQueryMode::Local) | Some(MetadataApiQueryMode::Distributed) => {}
    }
}

fn ensure_supported_query_form(query: &str) -> Result<(), MetadataApiError> {
    match query_form(query) {
        Some(MetadataQueryForm::Select | MetadataQueryForm::Ask) => Ok(()),
        _ => Err(MetadataApiError::BadRequest),
    }
}

/// Nodes a document query fans out to: the live holders of the bucket the
/// document was created into, not the holder set stamped at event time (which a
/// rebalance leaves stale).
pub fn document_replica_query_nodes(
    config: Option<&RealmConfigDocument>,
    record: &MetadataRegistryRecord,
    local_node_id: NodeId,
) -> Vec<NodeId> {
    let holders = config
        .map(|config| resolve_shard_holders(config, &record.placement))
        .unwrap_or_default();
    let nodes = deduplicate_fanout_nodes(holders);
    if nodes.is_empty() {
        vec![local_node_id]
    } else {
        nodes
    }
}

pub fn deduplicate_fanout_nodes(nodes: Vec<NodeId>) -> Vec<NodeId> {
    let mut seen = HashSet::with_capacity(nodes.len());
    nodes
        .into_iter()
        .filter(|node_id| seen.insert(*node_id))
        .collect()
}

fn fanout_nodes_with_local(mut nodes: Vec<NodeId>, local_node_id: NodeId) -> Vec<NodeId> {
    if !nodes.contains(&local_node_id) {
        nodes.push(local_node_id);
    }
    deduplicate_fanout_nodes(nodes)
}

pub fn metadata_auth_token_from_bearer(token: Option<&str>) -> Option<MetadataAuthToken> {
    token.and_then(|token| MetadataAuthToken::bearer(token).ok())
}

type MetadataNodeCall<T> =
    Arc<dyn Fn(NodeId) -> BoxFuture<'static, Result<T, MetadataError>> + Send + Sync>;

fn metadata_node_call<C, T, F, Fut>(context: C, call: F) -> MetadataNodeCall<T>
where
    C: Clone + Send + Sync + 'static,
    T: Send + 'static,
    F: Fn(C, NodeId) -> Fut + Send + Sync + 'static,
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
    node_id: NodeId,
    local: bool,
) -> Span {
    match operation {
        MetadataFanoutOperation::Query => debug_span!(
            "metadata.operation.query_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::Search => debug_span!(
            "metadata.operation.search_node",
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
    node_id: NodeId,
    local: bool,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataError>),
    record_stage_detail: bool,
) -> Result<T, MetadataError> {
    let node_span = metadata_fanout_node_span(operation, node_id, local);
    let node_started = Instant::now();
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
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    span: &Span,
    target_nodes: Option<Vec<NodeId>>,
) -> MetadataRealmNodeDiscovery {
    match target_nodes {
        Some(nodes) => {
            span.record("discovery_ms", 0u64);
            MetadataRealmNodeDiscovery {
                nodes: deduplicate_fanout_nodes(nodes),
                failed: false,
            }
        }
        None => {
            let discovery_started = Instant::now();
            let discovery = aruna_core::telemetry::time_stage(
                "discovery",
                load_metadata_realm_nodes_with_status(context, realm_id, local_node_id),
            )
            .await;
            record_elapsed_ms(span, "discovery_ms", discovery_started);
            discovery
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_metadata_fanout<T>(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    scope: MetadataFanoutScope,
    operation: MetadataFanoutOperation,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataError>),
    map_local_error: fn(MetadataError) -> MetadataApiError,
) -> Result<(Vec<(NodeId, T)>, MetadataFanoutStats), MetadataApiError>
where
    T: Send + 'static,
{
    let span = Span::current();
    let MetadataFanoutScope {
        mode,
        target_nodes,
        discovery_failed,
    } = scope;
    ensure_supported_query_mode(&mode);
    match mode.unwrap_or(MetadataApiQueryMode::Distributed) {
        MetadataApiQueryMode::Local => {
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
                Ok(result) => Ok((vec![(local_node_id, result)], fanout_stats)),
                Err(error) => Err(map_local_error(error)),
            }
        }
        MetadataApiQueryMode::Distributed => {
            let discovery =
                metadata_fanout_nodes(context, realm_id, local_node_id, &span, target_nodes).await;
            let discovery_failed = discovery_failed || discovery.failed;
            let nodes = discovery.nodes;
            span.record("node_count", nodes.len() as u64);
            let mut fanout_stats = MetadataFanoutStats {
                nodes_queried: nodes.len(),
                nodes_failed: usize::from(discovery_failed),
            };
            let fanout_started = Instant::now();
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
                    Ok(result) => node_parts.push((node_index, node_id, result)),
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

            node_parts.sort_by_key(|(node_index, _, _)| *node_index);
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
            Ok((
                node_parts
                    .into_iter()
                    .map(|(_, node_id, result)| (node_id, result))
                    .collect(),
                fanout_stats,
            ))
        }
    }
}

fn record_query_node_result(span: &Span, result: &Result<MetadataQueryResults, MetadataError>) {
    match result {
        Ok(result) => {
            span.record("result", result.kind());
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_search_node_result(
    span: &Span,
    result: &Result<(Vec<MetadataSearchHit>, usize), MetadataError>,
) {
    match result {
        Ok((hits, _)) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

#[tracing::instrument(
    name = "metadata.operation.query_distributed",
    level = "debug",
    skip(context, auth, query, scope),
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
#[allow(clippy::too_many_arguments)]
async fn run_query_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    scope: MetadataFanoutScope,
) -> Result<(MetadataQueryResults, MetadataFanoutStats), MetadataApiError> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let query_form = query_form(&query).ok_or(MetadataApiError::BadRequest)?;
    let select_limit = match query_form {
        MetadataQueryForm::Select => query_select_limit(&query),
        MetadataQueryForm::Ask => None,
    };
    let remote_auth_token = metadata_auth_token_from_bearer(bearer_token.as_deref());

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
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Query,
        local_call,
        remote_call,
        record_query_node_result,
        map_metadata_event_error,
    )
    .await?;

    let parts = parts.into_iter().map(|(_, result)| result).collect();
    let result = aggregate_query_results(parts, query_form, select_limit);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    match &result {
        Ok(results) => {
            span.record("result", results.kind());
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
    result.map(|results| (results, fanout_stats))
}

#[tracing::instrument(
    name = "metadata.operation.search_distributed",
    level = "debug",
    skip(context, auth, query, resume, watermark, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        page_size = page_size as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        hit_count = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
async fn run_search_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    resume: HashMap<NodeId, u32>,
    watermark: Option<SearchWatermark>,
    page_size: usize,
    scope: MetadataFanoutScope,
) -> Result<
    (
        Vec<MetadataSearchHit>,
        Option<SearchPageCursor>,
        bool,
        MetadataFanoutStats,
    ),
    MetadataApiError,
> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = metadata_auth_token_from_bearer(bearer_token.as_deref());
    let resume = Arc::new(resume);

    let local_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
            resume.clone(),
            page_size,
        ),
        |(handle, auth, graph_iris, query, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = handle
                .search_authorized_local(auth, graph_iris, query, limit)
                .await?;
            Ok((hits, limit))
        },
    );
    let remote_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
            resume.clone(),
            page_size,
        ),
        |(handle, auth_token, graph_iris, query, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = handle
                .request_remote_search_graphs(node_id, auth_token, graph_iris, query, limit)
                .await?;
            Ok((hits, limit))
        },
    );
    let (node_parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        record_search_node_result,
        map_metadata_internal_error,
    )
    .await?;

    let node_results = node_parts
        .into_iter()
        .map(|(node_id, (hits, requested))| NodeSearchResult {
            node_id,
            saturated: hits.len() >= requested,
            hits,
        })
        .collect();
    let page = paginate(
        node_results,
        watermark,
        page_size,
        METADATA_SEARCH_MAX_PAGINATION_DEPTH,
    );
    span.record("hit_count", page.hits.len() as u64);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok((page.hits, page.next, page.truncated, fanout_stats))
}

pub fn aggregate_query_results(
    results: Vec<MetadataQueryResults>,
    query_form: MetadataQueryForm,
    select_limit: Option<usize>,
) -> Result<MetadataQueryResults, MetadataApiError> {
    match query_form {
        MetadataQueryForm::Ask => {
            Ok(MetadataQueryResults::Boolean(results.into_iter().any(
                |result| matches!(result, MetadataQueryResults::Boolean(true)),
            )))
        }
        MetadataQueryForm::Select => {
            let mut seen = HashSet::new();
            let mut merged = Vec::new();
            for result in results {
                let MetadataQueryResults::Solutions(rows) = result else {
                    continue;
                };
                for row in rows {
                    let key = serde_json::to_string(&row)
                        .map_err(|err| MetadataApiError::Internal(err.to_string()))?;
                    if seen.insert(key) {
                        merged.push(row);
                    }
                }
            }
            if let Some(limit) = select_limit {
                merged.truncate(limit);
            }
            Ok(MetadataQueryResults::Solutions(merged))
        }
    }
}

pub fn query_select_limit(query: &str) -> Option<usize> {
    let parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select { pattern, .. } = parsed else {
        return None;
    };
    let spargebra::algebra::GraphPattern::Slice { length, .. } = pattern else {
        return None;
    };
    length
}

pub fn query_form(query: &str) -> Option<MetadataQueryForm> {
    match spargebra::SparqlParser::new().parse_query(query).ok()? {
        spargebra::Query::Select { .. } => Some(MetadataQueryForm::Select),
        spargebra::Query::Ask { .. } => Some(MetadataQueryForm::Ask),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

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
            MetadataQueryForm::Select,
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
            MetadataQueryForm::Select,
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
    fn query_form_accepts_single_line_declarations() {
        assert_eq!(
            query_form("PREFIX ex: <https://example.org/> SELECT ?s WHERE { ?s ?p ?o }").unwrap(),
            MetadataQueryForm::Select
        );
        assert_eq!(
            query_form("BASE <https://example.org/> ASK WHERE { ?s ?p ?o }").unwrap(),
            MetadataQueryForm::Ask
        );
        assert_eq!(query_form("CONSTRUCT WHERE { ?s ?p ?o }"), None);
    }

    // Fan-out follows the live holders of the stored bucket; the event-time
    // holder stamp on the record is ignored, and no config means local only.
    #[test]
    fn query_fans_out_to_holders() {
        let local_node_id = iroh::SecretKey::from_bytes(&[21u8; 32]).public();
        let remote_node_id = iroh::SecretKey::from_bytes(&[22u8; 32]).public();
        let stale_node_id = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
        let realm_id = RealmId([3u8; 32]);
        let document_id = Ulid::r#gen();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.seed_default_placement();
        config.ensure_node(local_node_id, aruna_core::structs::RealmNodeKind::Server);
        config.ensure_node(remote_node_id, aruna_core::structs::RealmNodeKind::Server);
        let strategy = config
            .strategy(&config.default_strategy_id.expect("default strategy"))
            .expect("default strategy resolves");
        let placement = crate::placement::choose_origin_bucket(
            &config,
            strategy,
            local_node_id,
            &document_id.to_bytes(),
        )
        .expect("origin holds a bucket");

        let record = MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::r#gen(),
            document_id,
            document_path: "datasets/query-targets".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: "/metadata/query-targets".to_string(),
            placement,
            holder_node_ids: vec![stale_node_id],
            created_at_ms: 0,
            updated_at_ms: 0,
            last_event_id: Ulid::nil(),
        };

        let nodes = document_replica_query_nodes(Some(&config), &record, local_node_id);
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&local_node_id) && nodes.contains(&remote_node_id));
        assert!(!nodes.contains(&stale_node_id));

        assert_eq!(
            document_replica_query_nodes(None, &record, local_node_id),
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
        assert_eq!(
            metadata_auth_token_from_bearer(Some("raw-aruna-token")),
            Some(MetadataAuthToken::bearer("raw-aruna-token").unwrap())
        );
        assert_eq!(metadata_auth_token_from_bearer(None), None);
    }
}
