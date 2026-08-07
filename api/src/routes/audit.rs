use crate::auth::{
    ValidatedArunaBearerTokenCarrier, ensure_permission, parse_group_id, require_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::map_metadata_api_error;
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, MetadataAuditOperation, Permission};
use aruna_operations::metadata::api::forwarded_bearer;
use aruna_operations::metadata::audit::{
    ListAuditError, ListAuditRequest, list_audit as gather_audit,
};
use aruna_operations::metadata::forward::is_user_origin;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "audit", description = "Audit trail reads")),
    paths(list_audit)
)]
pub struct AuditApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/audit", get(list_audit))
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct AuditQuery {
    /// Group whose audit trail is read; User origins forward the bearer.
    pub group_id: String,
    /// Optional narrowing to one metadata document.
    #[serde(default)]
    pub document_id: Option<String>,
    /// Opaque continuation token from a previous page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Page size (default 50, clamped to 1..=200).
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuditRecordResponse {
    pub group_id: String,
    pub document_id: String,
    pub graph_iri: String,
    pub user_id: String,
    pub node_id: String,
    pub operation: String,
    pub occurred_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuditPageResponse {
    pub records: Vec<AuditRecordResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// True when records may be missing or conflicting; partial pages have no cursor.
    pub partial: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub missing_nodes: Vec<String>,
    /// Number of omitted missing nodes beyond the bounded node list.
    pub missing_overflow: usize,
}

fn operation_name(operation: &MetadataAuditOperation) -> &'static str {
    match operation {
        MetadataAuditOperation::Create => "create",
        MetadataAuditOperation::ReplaceRoCrate => "replace_rocrate",
        MetadataAuditOperation::UpsertDataEntity => "upsert_data_entity",
        MetadataAuditOperation::UpsertContextualEntity => "upsert_contextual_entity",
        MetadataAuditOperation::Delete => "delete",
        MetadataAuditOperation::SetVisibility => "set_visibility",
        MetadataAuditOperation::PlaceReplicas => "place_replicas",
    }
}

#[utoipa::path(
    get,
    path = "/audit",
    tag = "audit",
    params(
        ("group_id" = String, Query, description = "Group id"),
        ("document_id" = Option<String>, Query, description = "Optional document id"),
        ("cursor" = Option<String>, Query, description = "Continuation token"),
        ("limit" = Option<usize>, Query, description = "Page size (max 200)")
    ),
    responses(
        (status = 200, description = "Audit records", body = AuditPageResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_audit(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Query(query): Query<AuditQuery>,
) -> ServerResult<(StatusCode, Json<AuditPageResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&query.group_id)?;
    let document_id = query
        .document_id
        .as_deref()
        .map(|id| Ulid::from_str(id).map_err(|_| ServerError::BadRequest))
        .transpose()?;
    let ctx = state.get_ctx();
    let user_origin = is_user_origin(&ctx, state.get_realm_id(), state.get_node_id())
        .await
        .map_err(map_metadata_api_error)?;
    if !user_origin {
        ensure_permission(
            &state,
            &auth,
            format!("/{}/g/{group_id}/admin", state.get_realm_id()),
            Permission::WRITE,
        )
        .await?;
    }

    // Peers re-check the same group-admin authority, so carry the caller's token.
    let forward_token = if user_origin {
        let carrier = bearer_token.as_ref().ok_or(ServerError::Unauthorized)?;
        forwarded_bearer(Some(carrier.as_str()))
            .map_err(map_metadata_api_error)?
            .ok_or(ServerError::Unauthorized)?
    } else {
        forwarded_bearer(bearer_token.as_ref().map(|carrier| carrier.as_str()))
            .map_err(map_metadata_api_error)?
    };
    let page = gather_audit(
        ctx.as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        forward_token,
        ListAuditRequest {
            group_id,
            document_id,
            cursor: query.cursor,
            limit: query.limit,
            local_authorized: !user_origin,
        },
    )
    .await
    .map_err(|error| match error {
        ListAuditError::Unavailable => ServerError::ServiceUnavailable,
        ListAuditError::Unauthorized => ServerError::Unauthorized,
        ListAuditError::InvalidCursor => ServerError::BadRequest,
        ListAuditError::Storage(message) => ServerError::InternalError(message),
    })?;

    Ok((
        StatusCode::OK,
        Json(AuditPageResponse {
            records: page
                .records
                .iter()
                .map(|record| AuditRecordResponse {
                    group_id: record.group_id.to_string(),
                    document_id: record.document_id.to_string(),
                    graph_iri: record.graph_iri.clone(),
                    user_id: record.user_id.to_string(),
                    node_id: record.node_id.to_string(),
                    operation: operation_name(&record.operation).to_string(),
                    occurred_at_ms: record.occurred_at_ms,
                    details: record.details.clone(),
                })
                .collect(),
            next_cursor: page.next_cursor,
            partial: page.partial,
            missing_nodes: page
                .missing_nodes
                .iter()
                .map(|node| node.to_string())
                .collect(),
            missing_overflow: page.missing_overflow,
        }),
    ))
}
