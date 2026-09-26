//! Shared metadata adapter that both the REST and MCP transports call for metadata work.
//! It maps operation errors, finds the local write target, and builds the response bodies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod model;

pub use model::*;

use crate::auth::ValidatedBearer;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::metadata::{
    MetadataError, MetadataQueryResults, MetadataRoCratePage, MetadataSearchHit,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::{MetaResourceId, StructuredId};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::forward::routing::origin_holds_document as run_origin_holds_document;
use aruna_operations::forward::transport::MetadataWriteError;
use aruna_operations::metadata::api::{
    ApiQueryMode, ExportMetadataResult, ListVisibleRequest, MetadataApiError, MetadataFanoutStats,
    MetadataListOrder, MetadataReferenceEntry, MetadataReferencesExecution,
    RoCrateExportView as OperationMetadataRoCrateExportView, forwarded_bearer,
    list_visible_documents as run_list_visible_metadata_documents,
};
use aruna_operations::metadata::create_document::{CreateDocumentError, CreateDocumentPayload};
use aruna_operations::metadata::forward::{CreateAuthorizedError, create_metadata_authorized};
use aruna_operations::metadata::get_document::load_document_record as load_metadata_record_by_document_from_operations;
use aruna_operations::metadata::update_document::UpdateDocumentError;
use chrono::{TimeZone, Utc};
use serde_json::Value;
use std::collections::HashMap;
use ulid::Ulid;
use url::form_urlencoded::Serializer;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_create_metadata(
    state: &ServerState,
    auth: &AuthContext,
    extras: PolicyRequestExtras,
    bearer_token: Option<ValidatedBearer>,
    group_id: Ulid,
    path: String,
    public: bool,
    payload: CreateDocumentPayload,
) -> ServerResult<MetadataRegistryRecord> {
    let ctx = state.get_ctx();
    create_metadata_authorized(
        &ctx,
        state.get_realm_id(),
        state.get_node_id(),
        auth,
        extras,
        forwarded_auth_token(bearer_token)?,
        group_id,
        path,
        public,
        payload,
    )
    .await
    .map_err(|error| match error {
        CreateAuthorizedError::EmptyPath => ServerError::BadRequest,
        CreateAuthorizedError::Forbidden => ServerError::Forbidden,
        CreateAuthorizedError::Api(error) => map_api_error(error),
        CreateAuthorizedError::Create(error) => map_create_error(error),
        CreateAuthorizedError::Authorize(error) => crate::auth::map_authorize_error(error),
        CreateAuthorizedError::Write(error) => map_write_error(error),
    })
}

pub(crate) fn map_references_response(
    execution: MetadataReferencesExecution,
) -> MetadataReferencesResponse {
    MetadataReferencesResponse {
        references: execution
            .references
            .into_iter()
            .map(map_reference_entry)
            .collect(),
    }
}

fn map_reference_entry(entry: MetadataReferenceEntry) -> MetadataReferenceItem {
    MetadataReferenceItem {
        document_id: entry.document_id,
        group_id: entry.group_id,
        document_path: entry.document_path,
        graph_iri: entry.graph_iri,
        predicate: entry.predicate,
        subject_iris: entry.subject_iris,
        title: entry.title,
    }
}

pub(crate) fn parse_document_id(document_id: &str) -> ServerResult<Ulid> {
    MetaResourceId::parse(document_id)
        .map(|id| id.as_ulid())
        .map_err(|_| ServerError::BadRequest)
}

pub(crate) async fn run_document_list(
    state: &ServerState,
    auth: Option<AuthContext>,
    query: ListMetadataQuery,
    group_id: Option<Ulid>,
) -> ServerResult<ListMetadataResponse> {
    let include = parse_include_flags(query.include.as_deref())?;
    let order = parse_metadata_order(query.order.as_deref())?;
    let ctx = state.get_ctx();
    let result = run_list_visible_metadata_documents(
        ctx.as_ref(),
        state.get_realm_id(),
        ListVisibleRequest {
            group_id,
            path_prefix: query.path_prefix,
            include_summary: include.summary,
            limit: query.limit,
            offset: query.offset,
            order,
            auth,
        },
    )
    .await
    .map_err(map_api_error)?;

    let mut documents = Vec::with_capacity(result.documents.len());
    for document in result.documents {
        let rocrate_summary = document
            .rocrate_summary_jsonld
            .map(parse_jsonld)
            .transpose()?;
        documents.push(DocumentListItem::from_record(
            &document.record,
            rocrate_summary,
        ));
    }
    Ok(ListMetadataResponse {
        documents,
        limit: result.limit,
        offset: result.offset,
        total_returned: result.total_returned,
        total_estimate: result.total_estimate,
    })
}

fn parse_include_flags(include: Option<&str>) -> ServerResult<MetadataIncludeFlags> {
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

fn parse_metadata_order(order: Option<&str>) -> ServerResult<MetadataListOrder> {
    match order.map(str::trim) {
        None | Some("") | Some("created") => Ok(MetadataListOrder::Created),
        Some("recent") => Ok(MetadataListOrder::Recent),
        Some(_) => Err(ServerError::BadRequestMessage(
            "order must be created or recent".to_string(),
        )),
    }
}

fn format_timestamp_ms(timestamp_ms: u64) -> String {
    i64::try_from(timestamp_ms)
        .ok()
        .and_then(|timestamp_ms| Utc.timestamp_millis_opt(timestamp_ms).single())
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

pub(crate) fn serialize_jsonld_object(value: &Value) -> ServerResult<String> {
    if !value.is_object() {
        return Err(ServerError::BadRequest);
    }
    serde_json::to_string(value).map_err(|_| ServerError::BadRequest)
}
/// A write that could neither be applied locally nor forwarded to a holder is a
/// loud failure: the caller is told it was not accepted, rather than being
/// answered `201` for a record that can never replicate.
pub(crate) fn map_write_error(error: MetadataWriteError) -> ServerError {
    match error {
        MetadataWriteError::Unauthorized => ServerError::Unauthorized,
        MetadataWriteError::Forbidden => ServerError::Forbidden,
        MetadataWriteError::NotFound => ServerError::NotFound,
        MetadataWriteError::Create(error) => map_create_error(error),
        MetadataWriteError::Update(error) => map_update_error(error),
        MetadataWriteError::Delete(error) => ServerError::InternalError(error.to_string()),
        MetadataWriteError::Undeliverable(error) => ServerError::ServiceUnavailableReason(format!(
            "metadata write is undeliverable: {error}"
        )),
    }
}

pub(crate) fn map_create_error(error: CreateDocumentError) -> ServerError {
    match error {
        CreateDocumentError::MetadataError(metadata_error) => map_metadata_error(metadata_error),
        CreateDocumentError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent metadata create conflict; retry".to_string())
        }
        CreateDocumentError::PlacementBinding(
            aruna_core::structs::placement::binding_directory::BindingError::Conflicted(_),
        ) => ServerError::ServiceUnavailableReason("placement_binding_conflict".to_string()),
        CreateDocumentError::PlacementBinding(_)
        | CreateDocumentError::PlacementBindingUnavailable(_) => {
            ServerError::ServiceUnavailableReason("placement_binding_unavailable".to_string())
        }
        CreateDocumentError::ClockHealth(_) => {
            ServerError::ServiceUnavailableReason("structured_id_clock_unhealthy".to_string())
        }
        CreateDocumentError::RawLimit => ServerError::ServiceUnavailable,
        other => ServerError::InternalError(other.to_string()),
    }
}

pub(crate) fn map_update_error(error: UpdateDocumentError) -> ServerError {
    match error {
        UpdateDocumentError::DocumentNotFound => ServerError::NotFound,
        UpdateDocumentError::RawLimit => ServerError::ServiceUnavailable,
        UpdateDocumentError::MetadataError(metadata_error) => map_metadata_error(metadata_error),
        UpdateDocumentError::RevisionConflict { .. } => {
            ServerError::PreconditionFailed(error.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}
/// The caller's own bearer token, carried to the holder a write is forwarded to
/// so it re-runs the same permission checks under the same authority.
pub(crate) fn forwarded_auth_token(
    bearer_token: Option<ValidatedBearer>,
) -> ServerResult<Option<aruna_operations::metadata::AuthToken>> {
    forwarded_bearer(bearer_token_string(bearer_token).as_deref()).map_err(map_api_error)
}

pub(crate) fn map_metadata_error(error: MetadataError) -> ServerError {
    match error {
        MetadataError::InvalidInput(reason) => ServerError::BadRequestReason(reason),
        MetadataError::Validation(violations) => ServerError::MetadataValidation(violations),
        MetadataError::ProfileValidation(findings) => {
            ServerError::MetadataProfileValidation(findings)
        }
        MetadataError::GraphNotFound => ServerError::ServiceUnavailable,
        other => ServerError::InternalError(other.to_string()),
    }
}

pub(crate) fn map_api_error(error: MetadataApiError) -> ServerError {
    match error {
        MetadataApiError::BadRequest => ServerError::BadRequest,
        MetadataApiError::Unauthorized => ServerError::Unauthorized,
        MetadataApiError::Forbidden => ServerError::Forbidden,
        MetadataApiError::NotFound => ServerError::NotFound,
        MetadataApiError::ServiceUnavailable => ServerError::ServiceUnavailable,
        MetadataApiError::PlacementUnavailable(
            aruna_operations::placement::PlacementResolveError::ActivationConflicted(_),
        ) => ServerError::ServiceUnavailableReason("placement_activation_conflict".to_string()),
        MetadataApiError::PlacementUnavailable(_) => {
            ServerError::ServiceUnavailableReason("placement_activation_unavailable".to_string())
        }
        MetadataApiError::InvalidCursor(message) => ServerError::BadRequestMessage(message),
        MetadataApiError::Internal(message) => ServerError::InternalError(message),
    }
}

pub(crate) fn map_query_mode(mode: Option<MetadataQueryMode>) -> Option<ApiQueryMode> {
    mode.map(|mode| match mode {
        MetadataQueryMode::Local => ApiQueryMode::Local,
        MetadataQueryMode::Distributed => ApiQueryMode::Distributed,
    })
}

pub(crate) fn bearer_token_string(bearer_token: Option<ValidatedBearer>) -> Option<String> {
    bearer_token.map(|carrier| carrier.as_str().to_string())
}

pub(crate) async fn ensure_metadata_scope(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    permission: Permission,
) -> ServerResult<()> {
    let path = format!("/{}/g/{group_id}/meta/**", state.get_realm_id());
    ensure_permission(state, auth.clone(), path, permission).await
}

async fn ensure_record_writable(
    state: &ServerState,
    auth: &AuthContext,
    record: &MetadataRegistryRecord,
    extras: PolicyRequestExtras,
) -> ServerResult<()> {
    crate::auth::ensure_permission_with(
        state,
        auth,
        record.permission_path.clone(),
        Permission::WRITE,
        extras,
    )
    .await
}

pub(crate) async fn local_write_record(
    state: &ServerState,
    auth: &AuthContext,
    document_id: Ulid,
    extras: PolicyRequestExtras,
) -> ServerResult<Option<MetadataRegistryRecord>> {
    let context = state.get_ctx();
    if !run_origin_holds_document(
        &context,
        state.get_realm_id(),
        state.get_node_id(),
        document_id,
    )
    .await
    .map_err(map_api_error)?
    {
        return Ok(None);
    }
    let record =
        match load_metadata_record_by_document_from_operations(context.as_ref(), document_id).await
        {
            Ok(Some(record)) => record,
            Ok(None) => return Ok(None),
            Err(ReadError::Storage(error)) => {
                return Err(ServerError::InternalError(error.to_string()));
            }
            Err(ReadError::Conversion(error)) => {
                return Err(ServerError::InternalError(error.to_string()));
            }
        };
    ensure_record_writable(state, auth, &record, extras).await?;
    Ok(Some(record))
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
    // Route metadata writes through the single authorization boundary so realm
    // and group request policies apply here as on every other REST path.
    crate::auth::ensure_permission(state, &auth, path, required_permission).await
}

pub(crate) async fn load_document_record(
    state: &ServerState,
    document_id: Ulid,
) -> ServerResult<MetadataRegistryRecord> {
    let ctx = state.get_ctx();
    match load_metadata_record_by_document_from_operations(ctx.as_ref(), document_id).await {
        Ok(Some(record)) => Ok(record),
        Ok(None) => Err(ServerError::NotFound),
        Err(ReadError::Storage(error)) => Err(ServerError::InternalError(error.to_string())),
        Err(ReadError::Conversion(error)) => Err(ServerError::InternalError(error.to_string())),
    }
}
type ReadError = aruna_operations::metadata::repository::StorageReadError;

pub(crate) fn map_export_view(view: &MetadataRoCrateView) -> OperationMetadataRoCrateExportView {
    match view {
        MetadataRoCrateView::Full => OperationMetadataRoCrateExportView::Full,
        MetadataRoCrateView::Summary => OperationMetadataRoCrateExportView::Summary,
        MetadataRoCrateView::Page => OperationMetadataRoCrateExportView::Page,
        MetadataRoCrateView::Raw => OperationMetadataRoCrateExportView::Raw,
    }
}

pub(crate) fn map_export_response(
    export: ExportMetadataResult,
    params: &RoCrateExportParams,
    view: MetadataRoCrateView,
) -> ServerResult<MetadataRoCrateResponse> {
    match export {
        ExportMetadataResult::Full { jsonld, .. } => Ok(MetadataRoCrateResponse::Projected(
            ProjectedRoCrateResponse {
                rocrate: parse_jsonld(jsonld)?,
                total_data_entities: None,
                returned_data_entities: None,
                next_offset: None,
                next_cursor: None,
            },
        )),
        ExportMetadataResult::Summary { record, jsonld } => Ok(MetadataRoCrateResponse::Projected(
            ProjectedRoCrateResponse {
                rocrate: rewrite_view_jsonld(
                    parse_jsonld(jsonld)?,
                    &record.graph_iri,
                    &build_view_id(&record.graph_iri, params, view),
                )?,
                total_data_entities: None,
                returned_data_entities: None,
                next_offset: None,
                next_cursor: None,
            },
        )),
        ExportMetadataResult::Page { record, page } => map_page_response(
            page,
            &record.graph_iri,
            &build_view_id(&record.graph_iri, params, view),
        ),
        ExportMetadataResult::Raw {
            raw,
            dataset_digest,
            ..
        } => Ok(MetadataRoCrateResponse::Raw(RawRoCrateResponse {
            raw: parse_jsonld(raw.revision.jsonld)?,
            winning_event_id: raw.revision.winning_event_id.to_string(),
            projection_state: match raw.projection_state {
                aruna_core::metadata::MaterializationState::Pending => "pending",
                aruna_core::metadata::MaterializationState::Materialized => "materialized",
                aruna_core::metadata::MaterializationState::Failed => "failed",
            }
            .to_string(),
            projected_event_id: raw.projected_event_id.map(|event_id| event_id.to_string()),
            context_digest: hex::encode(raw.revision.context_digest),
            dataset_digest: dataset_digest.map(hex::encode),
            merged: raw
                .revision
                .merged
                .map(|merged| {
                    Ok::<_, ServerError>(MergedRoCrateResponse {
                        rocrate: parse_jsonld(merged.jsonld)?,
                        findings: merged.findings,
                    })
                })
                .transpose()?,
        })),
    }
}

fn map_page_response(
    page: MetadataRoCratePage,
    graph_iri: &str,
    view_id: &str,
) -> ServerResult<MetadataRoCrateResponse> {
    Ok(MetadataRoCrateResponse::Projected(
        ProjectedRoCrateResponse {
            rocrate: rewrite_view_jsonld(parse_jsonld(page.jsonld)?, graph_iri, view_id)?,
            total_data_entities: Some(page.total_data_entities),
            returned_data_entities: Some(page.returned_data_entities),
            next_offset: page.next_offset,
            next_cursor: page.next_cursor,
        },
    ))
}

fn build_view_id(
    graph_iri: &str,
    params: &RoCrateExportParams,
    view: MetadataRoCrateView,
) -> String {
    let mut serializer = Serializer::new(String::new());
    let view = match view {
        MetadataRoCrateView::Full => "full",
        MetadataRoCrateView::Summary => "summary",
        MetadataRoCrateView::Page => "page",
        MetadataRoCrateView::Raw => "raw",
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

pub(crate) fn map_query_results(
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
    let mut failed_partitions = fanout_stats
        .failed_partitions
        .into_iter()
        .map(|node_id| node_id.to_string())
        .collect::<Vec<_>>();
    if fanout_stats.discovery_failed {
        failed_partitions.push("partition-discovery".to_string());
    }
    Ok(MetadataQueryResponse {
        result,
        nodes_queried: fanout_stats.nodes_queried,
        nodes_failed: fanout_stats.nodes_failed,
        complete: fanout_stats.nodes_failed == 0,
        failed_partitions,
    })
}

pub(crate) fn map_search_hit(hit: MetadataSearchHit) -> SearchHitResponse {
    SearchHitResponse {
        document_id: hit.document_id,
        group_id: hit.group_id,
        document_path: hit.document_path,
        graph_iri: hit.graph_iri,
        subject_iri: hit.subject_iri,
        score: hit.score,
        title: hit.title,
        snippet: hit.snippet,
        subject_types: hit.subject_types,
    }
}

#[cfg(test)]
mod pure_tests;
#[cfg(test)]
mod tests;
