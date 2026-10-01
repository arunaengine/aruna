//! Routes for the caller's placed vault, which holders keep as immutable revisions.
//! Saves and reads reach the vault holders from any node; the node never opens a payload.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ValidatedBearer, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::forwarded_auth_token;
use crate::routes::access::sessions::unix_rfc3339;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::VaultQuery;
use aruna_core::errors::StorageError;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::user::vault::{
    MAX_PREDECESSORS, MAX_VAULT_BYTES, VaultRecordError, VaultRecords, VaultRevision,
};
use aruna_operations::driver::drive;
use aruna_operations::forward::transport::MetadataWriteError;
use aruna_operations::users::vault_read::{ReadVaultConfig, ReadVaultError, ReadVaultOperation};
use aruna_operations::users::vault_route::{VaultRefusal, VaultRouteError, append_vault_routed};
use aruna_operations::users::vault_write::{
    AppendVaultConfig, AppendVaultError, VaultAppended, VaultChange,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Time a read may spend asking the vault holders.
const FETCH_DEADLINE: Duration = Duration::from_secs(10);

/// One current head of the caller's vault.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct VaultHead {
    /// ULID of this save; pass it in `predecessors` with the next save.
    pub revision: String,
    /// The heads this save replaced.
    pub predecessors: Vec<String>,
    /// The sealed vault as the portal encodes it.
    pub payload: String,
    pub updated_at: String,
}

/// The current heads of the caller's vault. More than one head means two saves
/// started from the same state; the portal merges them with its next save.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct VaultResponse {
    /// Empty before the first save and after a delete.
    pub heads: Vec<VaultHead>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SaveVaultRequest {
    /// The sealed vault as the portal encodes it, at most 64 KiB.
    pub payload: String,
    /// Revisions of every head the save replaces; empty for the first save.
    #[serde(default)]
    pub predecessors: Vec<String>,
}

fn vault_response(heads: Vec<VaultRevision>) -> VaultResponse {
    let heads = heads
        .into_iter()
        .filter_map(|head| {
            Some(VaultHead {
                revision: head.revision_id.to_string(),
                predecessors: head.predecessors.iter().map(Ulid::to_string).collect(),
                payload: head.payload?,
                updated_at: unix_rfc3339(head.created_at_ms / 1000),
            })
        })
        .collect();
    VaultResponse { heads }
}

fn map_read_error(error: ReadVaultError) -> ServerError {
    match error {
        ReadVaultError::Denied => ServerError::Forbidden,
        ReadVaultError::Unavailable(_)
        | ReadVaultError::PlacementResolve(_)
        | ReadVaultError::PlacementUnavailable
        | ReadVaultError::RealmConfigMissing => {
            ServerError::ServiceUnavailableReason("vault_unavailable".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    }
}

fn map_route_error(error: VaultRouteError) -> ServerError {
    match error {
        VaultRouteError::Append(AppendVaultError::Record(VaultRecordError::TooLarge)) => {
            ServerError::PayloadTooLarge("a vault payload may hold at most 64 KiB".to_string())
        }
        VaultRouteError::Append(AppendVaultError::Record(error)) => {
            ServerError::BadRequestReason(error.to_string())
        }
        VaultRouteError::Refused(VaultRefusal::Invalid) => {
            ServerError::BadRequestReason("the vault holder refused the record".to_string())
        }
        VaultRouteError::Append(AppendVaultError::TooManyKeys)
        | VaultRouteError::Refused(VaultRefusal::TooManyKeys) => {
            ServerError::Conflict("the user has published the most key records allowed".to_string())
        }
        VaultRouteError::Append(AppendVaultError::Storage(StorageError::TransactionConflict)) => {
            ServerError::Conflict("the vault is being written concurrently; retry".to_string())
        }
        VaultRouteError::Forward(MetadataWriteError::Unauthorized) => ServerError::Unauthorized,
        VaultRouteError::Forward(MetadataWriteError::Forbidden) => ServerError::Forbidden,
        VaultRouteError::Forward(_)
        | VaultRouteError::Append(
            AppendVaultError::PlacementResolve(_)
            | AppendVaultError::PlacementUnavailable
            | AppendVaultError::PlacementFenced
            | AppendVaultError::RealmConfigMissing,
        ) => ServerError::ServiceUnavailableReason("vault_unavailable".to_string()),
        error => ServerError::InternalError(error.to_string()),
    }
}

fn parse_predecessors(predecessors: &[String]) -> ServerResult<Vec<Ulid>> {
    if predecessors.len() > MAX_PREDECESSORS {
        return Err(ServerError::BadRequestReason(format!(
            "a save may name at most {MAX_PREDECESSORS} predecessors"
        )));
    }
    predecessors
        .iter()
        .map(|revision| Ulid::from_str(revision).map_err(|_| ServerError::BadRequest))
        .collect()
}

pub(super) async fn append(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    auth_token: Option<AuthToken>,
    change: VaultChange,
) -> ServerResult<VaultAppended> {
    let config = AppendVaultConfig {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        record_id: Ulid::generate(),
        change,
        now_ms: aruna_core::time::unix_timestamp_millis(),
    };
    append_vault_routed(&state.get_ctx(), config, auth_token)
        .await
        .map_err(map_route_error)
}

pub(super) async fn read(
    state: &Arc<ServerState>,
    user_id: UserId,
    query: VaultQuery,
) -> ServerResult<VaultRecords> {
    let config = ReadVaultConfig {
        node_id: state.get_node_id(),
        user_id,
        query,
        deadline: FETCH_DEADLINE,
    };
    drive(ReadVaultOperation::new(config), &state.get_ctx())
        .await
        .map_err(map_read_error)
}

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(get_vault, put_vault, delete_vault))
}

const HEAD_EXAMPLE: &str = "{\"version\":1,\"kdf\":{\"name\":\"pbkdf2-sha256\",\"iterations\":600000,\"salt\":\"c2FsdA==\"},\"keys\":[]}";

#[utoipa::path(
    get,
    path = "/access/users/me/vault",
    tag = "access/users",
    summary = "Read the caller's vault",
    description = r#"Returns the current heads of the calling user's passphrase-sealed vault.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. The vault is self-scoped, so a caller reaches only their own.

**Behavior**
- The vault is stored on the holders of the user's vault placement, not on every node. A node that
  is no holder asks the holders with the caller's token, and each holder checks it again.
- Every head is returned. Two heads mean two saves started from the same state; the portal merges
  them and names both in `predecessors` of its next save. Nodes never merge payloads.
- The payload is the portal's own ciphertext, returned unchanged.
- An empty list means there is no vault yet, or it was deleted.
- 503 means no holder answered; it never means the vault is absent."#,
    responses(
        (status = 200, description = "The heads of the caller's vault", body = VaultResponse,
            example = json!({
                "heads": [{
                    "revision": "01K6A5Q3T6Z7B0V9P8N2M4K1J0",
                    "predecessors": ["01K6A5P1R2S3T4V5W6X7Y8Z9A0"],
                    "payload": HEAD_EXAMPLE,
                    "updated_at": "2026-10-01T12:30:00Z"
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 503, description = "No vault holder answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_vault(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
) -> ServerResult<(StatusCode, Json<VaultResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let auth_token = forwarded_auth_token(bearer_token)?;
    let heads = match read(&state, auth.user_id, VaultQuery::Heads { auth_token }).await? {
        VaultRecords::Heads(heads) => heads,
        VaultRecords::Keys(_) => {
            return Err(ServerError::InternalError(
                "vault read answered key records".to_string(),
            ));
        }
    };
    Ok((StatusCode::OK, Json(vault_response(heads))))
}

#[utoipa::path(
    put,
    path = "/access/users/me/vault",
    tag = "access/users",
    summary = "Save the caller's vault",
    description = r#"Stores a new head of the calling user's passphrase-sealed vault.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. The vault is self-scoped, so a caller writes only their own.

**Behavior**
- Each save is an immutable revision on the holders of the user's vault placement. A node that is
  no holder forwards the save to a holder with the caller's token; when no holder accepts it the
  save fails with 503 and nothing is kept.
- `predecessors` names the heads the save replaces. Pass every head the last read returned.
- A save never fails because another browser saved first: both saves stay as heads, and the next
  read returns both for the portal to merge.
- The answer lists the heads the accepting holder knows after the save.

**Limits**
- The payload holds at most 64 KiB; a larger one is refused with 413.
- At most 32 predecessors."#,
    request_body(
        content = SaveVaultRequest,
        description = "The sealed vault and the heads it replaces",
        example = json!({
            "payload": HEAD_EXAMPLE,
            "predecessors": ["01K6A5P1R2S3T4V5W6X7Y8Z9A0"]
        })
    ),
    responses(
        (status = 200, description = "The heads after the save", body = VaultResponse,
            example = json!({
                "heads": [{
                    "revision": "01K6A5Q3T6Z7B0V9P8N2M4K1J0",
                    "predecessors": ["01K6A5P1R2S3T4V5W6X7Y8Z9A0"],
                    "payload": HEAD_EXAMPLE,
                    "updated_at": "2026-10-01T12:30:00Z"
                }]
            })),
        (status = 400, description = "A predecessor is not a revision id, or there are too many", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 409, description = "The holder lost a concurrent write; retry", body = ErrorResponse),
        (status = 413, description = "The payload exceeds 64 KiB", body = ErrorResponse),
        (status = 503, description = "No vault holder accepted the save", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_vault(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Json(request): Json<SaveVaultRequest>,
) -> ServerResult<(StatusCode, Json<VaultResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if request.payload.len() > MAX_VAULT_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "a vault payload may hold at most 64 KiB".to_string(),
        ));
    }
    let change = VaultChange::Save {
        payload: request.payload,
        predecessors: parse_predecessors(&request.predecessors)?,
    };
    let auth_token = forwarded_auth_token(bearer_token)?;
    match append(&state, &auth, auth_token, change).await? {
        VaultAppended::Heads(heads) => Ok((StatusCode::OK, Json(vault_response(heads)))),
        VaultAppended::Key(_) => Err(ServerError::InternalError(
            "vault save answered a key record".to_string(),
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/access/users/me/vault",
    tag = "access/users",
    summary = "Delete the caller's vault",
    description = r#"Replaces every head of the calling user's vault with a delete marker.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. The vault is self-scoped, so a caller deletes only their own.

**Behavior**
- The delete reaches the holders like a save. The next read returns no heads.
- A save made concurrently on another holder stays as a head and is returned by later reads.
- Deleting a vault that is not there, or one already deleted, answers 204 without a write."#,
    responses(
        (status = 204, description = "The vault is deleted or was never there"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 503, description = "No vault holder accepted the delete", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_vault(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let auth_token = forwarded_auth_token(bearer_token)?;
    append(&state, &auth, auth_token, VaultChange::Delete).await?;
    Ok(StatusCode::NO_CONTENT)
}
