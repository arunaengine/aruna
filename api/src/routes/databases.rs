//! Serves the realm's database list: listing for readers, marking and removal for curators,
//! and the owner consent flow for the admins of a bucket's group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::realm::database_list::{Caller, DatabaseServiceError, list_databases};
use aruna_operations::realm::database_publish::PublishDatabaseError;
use aruna_operations::realm::databases::{
    MarkInput, MarkOutcome, approve_request, list_requests, mark_database, reject_request,
    remove_database,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use ulid::Ulid;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::bearer_token_string;
use crate::server::state::ServerState;

pub use super::database_types::{
    DatabaseEntryView, DatabaseListParams, DatabaseListResponse, DatabaseRequestList,
    DatabaseRequestView, DatabaseTargetBody, DatabaseView, MarkDatabaseRequest,
    MarkDatabaseResponse,
};

#[derive(OpenApi)]
#[openapi(tags((name = "data/databases", description = "Buckets marked as databases, each described by a dataset")))]
pub struct DatabasesApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DatabasesApiDoc::openapi())
        .routes(routes!(get_databases))
        .routes(routes!(mark))
        .routes(routes!(remove))
        .routes(routes!(get_requests))
        .routes(routes!(approve))
        .routes(routes!(reject))
}

fn caller(auth: AuthContext, bearer: Option<ValidatedBearer>) -> Caller {
    Caller {
        auth,
        bearer: bearer_token_string(bearer),
    }
}

fn parse_id(value: &str, what: &str) -> ServerResult<Ulid> {
    Ulid::from_string(value).map_err(|_| ServerError::BadRequestReason(format!("invalid {what}")))
}

fn map_error(error: DatabaseServiceError) -> ServerError {
    match error {
        DatabaseServiceError::Forbidden => ServerError::Forbidden,
        DatabaseServiceError::NotFound(_) => ServerError::NotFound,
        DatabaseServiceError::NotLinked(_) | DatabaseServiceError::BadRequest(_) => {
            ServerError::BadRequestReason(error.to_string())
        }
        DatabaseServiceError::Unavailable(reason) => ServerError::ServiceUnavailableReason(reason),
        DatabaseServiceError::Publish(error) => match error {
            PublishDatabaseError::Unauthorized => ServerError::Forbidden,
            PublishDatabaseError::NotManagementNode => ServerError::NoManagementNode,
            PublishDatabaseError::NotFound(_) => ServerError::NotFound,
            PublishDatabaseError::PredecessorChanged(_) => ServerError::Conflict(error.to_string()),
            PublishDatabaseError::Database(_) => ServerError::BadRequestReason(error.to_string()),
            PublishDatabaseError::Storage(StorageError::TransactionConflict) => {
                ServerError::Conflict("concurrent realm configuration update; retry".to_string())
            }
            other => ServerError::InternalError(other.to_string()),
        },
        DatabaseServiceError::Internal(reason) => ServerError::InternalError(reason),
    }
}

fn entry_example() -> serde_json::Value {
    serde_json::json!({
        "entry_id": "01K8Z0DB0SP1RE000000000003",
        "dataset_id": "01K8Z0DB0SP1RE000000000004",
        "target": {"kind": "distributed", "set_id": "01K8Y2B6D8F0H2K4M6P8R0T2V4"},
        "owner_group": "01K8XW4C2T6B9Q1ZHDNE5A7RMF"
    })
}

fn list_example() -> serde_json::Value {
    let mut view = entry_example();
    view["bucket"] = "spire-db".into();
    view["state"] = "active".into();
    view["title"] = "SPIRE database".into();
    view["graph_iri"] = "https://w3id.org/aruna/01K8Z0DB0SP1RE000000000004".into();
    view["document_path"] = "databases/spire".into();
    serde_json::json!({"databases": [view], "next_after": null})
}

#[utoipa::path(
    get,
    path = "/data/databases",
    tag = "data/databases",
    summary = "List the databases the caller may use",
    description = r#"Lists buckets marked as databases, each with the dataset that describes it.

**Authentication**: realm bearer token. The curator view needs WRITE on
`/{realm-id}/admin/databases`.

**Behavior**
- An entry is listed only when the caller may read its bucket on a node that holds it and
  may read its dataset, and the dataset's root still names the bucket root as a
  Dataset-typed part. Entries grant nothing.
- When not every metadata node answered, the entry is listed with state
  `description_unavailable` instead of being dropped.
- The curator view also lists `dataset_hidden` and `no_longer_linked` entries.

**Limits**
- At most 100 entries per page, and at most four times the page size examined; continue with
  `after`."#,
    params(DatabaseListParams),
    responses(
        (status = 200, description = "Databases the caller may see", body = DatabaseListResponse, example = json!(list_example())),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Curator view without WRITE on the databases path", body = ErrorResponse),
        (status = 503, description = "Realm configuration or authorization unreadable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_databases(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Query(params): Query<DatabaseListParams>,
) -> ServerResult<Json<DatabaseListResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let (listed, next) = list_databases(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        &caller(auth, bearer),
        params.after,
        params.limit.unwrap_or(25),
        params.curation,
    )
    .await
    .map_err(map_error)?;
    Ok(Json(DatabaseListResponse {
        databases: listed.into_iter().map(Into::into).collect(),
        next_after: next.map(|id| id.to_string()),
    }))
}

#[utoipa::path(
    post,
    path = "/system/realm/databases",
    tag = "data/databases",
    summary = "Mark a bucket as a database",
    description = r#"Marks a bucket as a database described by a dataset, or retargets an entry.

**Authentication**: realm bearer token with WRITE on `/{realm-id}/admin/databases`. A
management node serves the call and every other node relays it to one.

**Behavior**
- The dataset's live root must already name the bucket root as a Dataset-typed `hasPart`:
  that existing part is the dataset owner's approval.
- When the caller also holds admin WRITE on the group that owns the bucket, the entry is
  published at once (`published`). Otherwise the request waits on the management node for an
  admin of that group (`pending_consent`).

**Limits**
- At most 1024 databases per realm."#,
    request_body(content = MarkDatabaseRequest, example = json!({
        "dataset_id": "01K8Z0DB0SP1RE000000000004",
        "target": {"kind": "distributed", "set_id": "01K8Y2B6D8F0H2K4M6P8R0T2V4"}
    })),
    responses(
        (status = 200, description = "Published or waiting for owner consent", body = MarkDatabaseResponse, example = json!({
            "status": "pending_consent",
            "entry": entry_example(),
            "request_id": "01K8Z1REQ0000000000000000A"
        })),
        (status = 400, description = "Malformed ids, or the dataset does not link the bucket", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the databases path", body = ErrorResponse),
        (status = 404, description = "The bucket or set is not known", body = ErrorResponse),
        (status = 409, description = "A concurrent realm configuration update won; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Not every metadata node answered, or no management node was reachable; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn mark(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<MarkDatabaseRequest>,
) -> ServerResult<Json<MarkDatabaseResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let input = MarkInput {
        entry_id: request
            .entry_id
            .as_deref()
            .map(|id| parse_id(id, "entry_id"))
            .transpose()?,
        dataset_id: parse_id(&request.dataset_id, "dataset_id")?,
        target: request.target.try_into()?,
    };
    let outcome = mark_database(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        &caller(auth, bearer),
        input,
    )
    .await
    .map_err(map_error)?;
    Ok(Json(match outcome {
        MarkOutcome::Published(entry) => MarkDatabaseResponse {
            status: "published".to_string(),
            entry: (&entry).into(),
            request_id: None,
        },
        MarkOutcome::Pending(request) => MarkDatabaseResponse {
            status: "pending_consent".to_string(),
            entry: (&request.entry).into(),
            request_id: Some(request.request_id.to_string()),
        },
    }))
}

#[utoipa::path(
    delete,
    path = "/system/realm/databases/{entry_id}",
    tag = "data/databases",
    summary = "Remove a database mark",
    description = r#"Removes a database entry. The bucket and the dataset stay as they are.

**Authentication**: realm bearer token with WRITE on `/{realm-id}/admin/databases`, or admin
WRITE on the group that owns the bucket. A management node serves the call and every other
node relays it to one.

**Behavior**
- After removal an earlier consent cannot be reused: it names the replaced register version.

**Limits**
- One entry per call."#,
    params(("entry_id" = String, Path, description = "Database entry id")),
    responses(
        (status = 204, description = "Entry removed"),
        (status = 400, description = "Malformed entry id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Neither curator nor admin of the owning group", body = ErrorResponse),
        (status = 404, description = "No such entry", body = ErrorResponse),
        (status = 409, description = "A concurrent realm configuration update won; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "No management node was reachable; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(entry_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    remove_database(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        &caller(auth, bearer),
        parse_id(&entry_id, "entry_id")?,
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/system/realm/databases/requests",
    tag = "data/databases",
    summary = "List database marks waiting for owner consent",
    description = r#"Lists the pending database requests the caller made or may approve.

**Authentication**: realm bearer token. A management node serves the call and every other node
relays it to one.

**Behavior**
- A request is listed to its curator and to users with admin WRITE on the owning group.
- Requests stay on the management node that took them; they are never broadcast.

**Limits**
- At most 1024 requests are read."#,
    responses(
        (status = 200, description = "Visible pending requests", body = DatabaseRequestList, example = json!({
            "requests": [{
                "request_id": "01K8Z1REQ0000000000000000A",
                "entry": entry_example(),
                "curator": "01K8Z1C0RAT0R0000000000000",
                "created_at_ms": 1761300000000u64
            }]
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 502, description = "A relayed call failed; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "No management node was reachable; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_requests(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
) -> ServerResult<Json<DatabaseRequestList>> {
    let auth = require_realm_auth(&state, auth)?;
    let requests = list_requests(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        &caller(auth, bearer),
    )
    .await
    .map_err(map_error)?;
    Ok(Json(DatabaseRequestList {
        requests: requests.iter().map(Into::into).collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/system/realm/databases/requests/{request_id}/approve",
    tag = "data/databases",
    summary = "Approve a database mark as owner",
    description = r#"An admin of the owning group approves a pending database request.

**Authentication**: realm bearer token with admin WRITE on the group that owns the bucket. A
management node serves the call and every other node relays it to one.

**Behavior**
- The management node signs a consent for this authenticated request, bound to the realm, the
  entry, the replaced register version, the bucket, its group and the dataset, and publishes
  the curator's mark with it. Every node checks both people's current rights again.
- A consent is valid for one publication; a changed entry needs a new request.

**Limits**
- One request per call."#,
    params(("request_id" = String, Path, description = "Pending request id")),
    responses(
        (status = 200, description = "The published entry", body = DatabaseEntryView, example = json!(entry_example())),
        (status = 400, description = "Malformed request id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No admin WRITE on the owning group, or the curator lost the right", body = ErrorResponse),
        (status = 404, description = "No such request", body = ErrorResponse),
        (status = 409, description = "The entry changed since the request; ask again", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "No management node was reachable; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn approve(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(request_id): Path<String>,
) -> ServerResult<Json<DatabaseEntryView>> {
    let auth = require_realm_auth(&state, auth)?;
    let entry = approve_request(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        &caller(auth, bearer),
        parse_id(&request_id, "request_id")?,
    )
    .await
    .map_err(map_error)?;
    Ok(Json((&entry).into()))
}

#[utoipa::path(
    delete,
    path = "/system/realm/databases/requests/{request_id}",
    tag = "data/databases",
    summary = "Withdraw or decline a database mark",
    description = r#"Drops a pending database request.

**Authentication**: realm bearer token of the curator who asked, or of a user with admin WRITE
on the owning group. A management node serves the call and every other node relays it to one.

**Behavior**
- Nothing is published; the curator may ask again.

**Limits**
- One request per call."#,
    params(("request_id" = String, Path, description = "Pending request id")),
    responses(
        (status = 204, description = "Request dropped"),
        (status = 400, description = "Malformed request id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Neither the curator nor an admin of the owning group", body = ErrorResponse),
        (status = 404, description = "No such request", body = ErrorResponse),
        (status = 502, description = "A relayed call failed; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "No management node was reachable; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn reject(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(request_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    reject_request(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        &caller(auth, bearer),
        parse_id(&request_id, "request_id")?,
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}
