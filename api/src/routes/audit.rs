use crate::auth::{
    ValidatedArunaBearerTokenCarrier, ensure_permission, parse_group_id, require_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::map_metadata_api_error;
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, MetadataAuditOperation, Permission};
use aruna_operations::metadata::api::forwarded_bearer;
use aruna_operations::metadata::audit::{
    AUDIT_DEADLINE_SECS, ListAuditError, ListAuditRequest, list_audit as gather_audit,
};
use aruna_operations::metadata::forward::is_user_origin;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "metadata/audit", description = "Audit trail reads"))
)]
pub struct AuditApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(AuditApiDoc::openapi()).routes(routes!(list_audit))
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
        MetadataAuditOperation::WithdrawPersistentId => "withdraw_persistent_id",
    }
}

#[utoipa::path(
    get,
    path = "/metadata/audit",
    tag = "metadata/audit",
    summary = "List a group's metadata audit trail",
    description = r#"Returns a group's metadata audit trail as a realm-wide merged page, oldest first.

**Authentication**: realm bearer token with WRITE on the group's admin path. A user-kind node
forwards the read under the caller's own token and every peer re-checks that same authority.

**Behavior**
- Audit rows are node-local, so a page merges a slice from every sync-eligible realm node under a
  30 second deadline.
- `partial` is true when a node did not answer in time, realm membership or its digest changed
  under the read, or a peer's slice was rejected.
- A partial page never carries `next_cursor`, so an absent cursor on a complete page is the end of
  the trail.
- `missing_nodes` names up to 64 nodes that did not contribute; `missing_overflow` counts the rest.
- A `cursor` is bound to this realm, the membership digest, the group and the document filter."#,
    params(
        ("group_id" = String, Query, description = "ULID of the group whose audit trail is read"),
        ("document_id" = Option<String>, Query, description = "Narrows the trail to one metadata document ULID; the default is the whole group trail"),
        ("cursor" = Option<String>, Query, description = "Continuation token from a previous page's `next_cursor`; absent starts at the oldest record"),
        ("limit" = Option<usize>, Query, description = "Maximum records in one page. Default 50, clamped to 1..=200")
    ),
    responses(
        (
            status = 200,
            description = "Merged audit page, oldest first",
            body = AuditPageResponse,
            examples(
                ("Complete page" = (
                    summary = "Every node answered; the cursor continues the trail",
                    value = json!({
                        "records": [
                            {
                                "group_id": "01JABCDEF0123456789ABCDEFG",
                                "document_id": "01JMETADATA0123456789ABCDE",
                                "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                "user_id": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                                "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                                "operation": "upsert_data_entity",
                                "occurred_at_ms": 1775744591123_i64,
                                "details": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE#data/reads.fastq"
                            }
                        ],
                        "next_cursor": "AQECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "partial": false,
                        "missing_overflow": 0
                    })
                )),
                ("Partial page" = (
                    summary = "One node did not answer, so the page is incomplete and has no cursor",
                    value = json!({
                        "records": [
                            {
                                "group_id": "01JABCDEF0123456789ABCDEFG",
                                "document_id": "01JMETADATA0123456789ABCDE",
                                "graph_iri": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                                "user_id": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                                "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                                "operation": "create",
                                "occurred_at_ms": 1775744591123_i64
                            }
                        ],
                        "partial": true,
                        "missing_nodes": [
                            "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a"
                        ],
                        "missing_overflow": 0
                    })
                ))
            )
        ),
        (status = 400, description = "Malformed group or document id, or a cursor that no longer matches this realm, membership digest, group or document filter", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token, or a forwarded token the realm peers rejected", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller lacks WRITE on the group's admin path", body = ErrorResponse),
        (status = 503, description = "Concurrent audit reads are admission-limited and this node is saturated, the realm configuration is unreadable, or the merge deadline expired; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_audit(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Query(query): Query<AuditQuery>,
) -> ServerResult<(StatusCode, Json<AuditPageResponse>)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(AUDIT_DEADLINE_SECS);
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&query.group_id)?;
    let document_id = query
        .document_id
        .as_deref()
        .map(|id| Ulid::from_str(id).map_err(|_| ServerError::BadRequest))
        .transpose()?;
    let ctx = state.get_ctx();
    let user_origin = tokio::time::timeout_at(
        deadline,
        is_user_origin(&ctx, state.get_realm_id(), state.get_node_id()),
    )
    .await
    .map_err(|_| ServerError::ServiceUnavailable)?
    .map_err(map_metadata_api_error)?;
    // A device holds no audit rows of its own, so this read is pure fan-out and
    // every peer re-checks the same group-admin authority on the caller's token.
    if !user_origin {
        tokio::time::timeout_at(
            deadline,
            ensure_permission(
                &state,
                &auth,
                format!("/{}/g/{group_id}/admin", state.get_realm_id()),
                Permission::WRITE,
            ),
        )
        .await
        .map_err(|_| ServerError::ServiceUnavailable)??;
    }

    // Peers re-check the same group-admin authority, so carry the caller's token.
    let forward_token = if user_origin {
        let carrier = bearer_token.as_ref().ok_or(ServerError::Unauthorized)?;
        Some(
            forwarded_bearer(Some(carrier.as_str()))
                .map_err(map_metadata_api_error)?
                .ok_or(ServerError::Unauthorized)?,
        )
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
        deadline,
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
