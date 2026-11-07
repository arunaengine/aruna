//! Route that reads this node's retained administrative audit rows for the realm or a group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::document::policy_authority_path;
use aruna_operations::driver::drive;
use aruna_operations::users::admin_audit::{AuditPageInput, AuditPageOperation};
use axum::extract::{Query, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, parse_group_id, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

const DEFAULT_LIMIT: usize = 50;

#[derive(OpenApi)]
#[openapi(tags((name = "access/audit", description = "Retained administrative audit rows")))]
pub struct AdminAuditDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(AdminAuditDoc::openapi()).routes(routes!(list_admin_audit))
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AdminAuditQuery {
    /// Reads only the rows of this group; without it the realm rows are read.
    pub group_id: Option<String>,
    /// The `next_cursor` of the previous page.
    pub cursor: Option<String>,
    /// Rows per page, 1 to 200, default 50.
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct AdminAuditRow {
    pub audit_id: String,
    /// `account_deactivated`, `account_reactivated`, `service_account_created` or
    /// `restricted_token_issued`.
    pub action: String,
    pub actor: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub result: String,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AdminAuditPage {
    pub records: Vec<AdminAuditRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Always true: rows cover only the changes this node made and recorded.
    pub partial: bool,
}

#[utoipa::path(
    get,
    path = "/access/audit",
    tag = "access/audit",
    summary = "Read retained administrative audit rows",
    description = r#"Lists the administrative changes this node made, oldest first, one page at a time.

**Authentication**: realm bearer token without path restrictions. The realm rows need WRITE on
`/{realm}/admin/config`; the rows of one group need WRITE on `/{realm}/g/{group}/admin`.

**Behavior**
- Recorded changes are account deactivation and reactivation, service account creation and the
  issuance of a token with path restrictions. Each row names the actor, the target account, the
  owning group when there is one, the session of an issued token and the result `succeeded`.
- A row is written only after its change succeeded on this node; denied or failed attempts and
  changes made on other nodes are absent, so every page reports `partial` as true.
- The realm rows hold every recorded change. The rows of a group hold the changes to its service
  accounts and their tokens, never other targets.
- Tokens and path restrictions are never recorded.

**Limits**
- At most 200 rows per page, 50 by default; follow `next_cursor` for more.

**Errors**
- 403 when the caller may not read the requested scope; no row of it is returned."#,
    params(AdminAuditQuery),
    responses(
        (status = 200, description = "One page of retained audit rows", body = AdminAuditPage, example = json!({
            "records": [{
                "audit_id": "01JAUDT00123456789ABCDEFGH",
                "action": "service_account_created",
                "actor": "01JUSER00123456789ABCDEFGH@01JREALM0123456789ABCDEFG",
                "target": "01JSERV00123456789ABCDEFGH@01JREALM0123456789ABCDEFG",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "result": "succeeded",
                "recorded_at_ms": 1762160400000u64
            }],
            "next_cursor": "01JAUDT00123456789ABCDEFGH",
            "partial": true
        })),
        (status = 400, description = "Invalid group id or cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or no administration grant for the scope", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_admin_audit(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<AdminAuditQuery>,
) -> ServerResult<Json<AdminAuditPage>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = query.group_id.as_deref().map(parse_group_id).transpose()?;
    let start_after = query
        .cursor
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequestReason("cursor is not a ULID".to_string()))?;
    let path = policy_authority_path(state.get_realm_id(), group_id);
    ensure_permission(&state, &auth, path, Permission::WRITE).await?;
    let page = drive(
        AuditPageOperation::new(AuditPageInput {
            group_id,
            start_after,
            limit: query.limit.unwrap_or(DEFAULT_LIMIT),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(Json(AdminAuditPage {
        records: page.records.into_iter().map(row).collect(),
        next_cursor: page.next.map(|id| id.to_string()),
        partial: true,
    }))
}

fn row(record: AdminAuditRecord) -> AdminAuditRow {
    let action = match record.action {
        AdminAuditAction::AccountDeactivated => "account_deactivated",
        AdminAuditAction::AccountReactivated => "account_reactivated",
        AdminAuditAction::ServiceAccountCreated => "service_account_created",
        AdminAuditAction::RestrictedTokenIssued => "restricted_token_issued",
    };
    AdminAuditRow {
        audit_id: record.audit_id.to_string(),
        action: action.to_string(),
        actor: record.actor.to_string(),
        target: record.target.to_string(),
        group_id: record.group_id.map(|id| id.to_string()),
        session_id: record.session_id,
        result: "succeeded".to_string(),
        recorded_at_ms: record.audit_id.timestamp_ms(),
    }
}
