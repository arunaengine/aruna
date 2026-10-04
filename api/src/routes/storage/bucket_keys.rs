//! Serves the bucket key routes: unlock, extend, lock, holders, the caller's sealed copies,
//! rotation and the key audit of an encrypted bucket on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::encryption::{
    EncryptionStatus, RecoveryView, UnlockView, blob_refusal, bucket_group, holder_report,
    is_holder, key_refusal, not_encrypted, read_snapshot, refused, settings_refusal, unlock_view,
};
use super::routing::ensure_group_admin;
use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::encryption::{BucketKeyRef, HolderOrigin, UnlockStatus};
use aruna_core::structs::storage::holders::{HolderEntry, HolderState, KeyLookup, holder_revision};
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::s3::bucket::holders::lookup_keys;
use aruna_operations::s3::bucket::key_extend::{ExtendBucketOperation, ExtendError, ExtendInput};
use aruna_operations::s3::bucket::key_grant::{GrantError, GrantHolderOperation, GrantInput};
use aruna_operations::s3::bucket::key_lock::{LockBucketOperation, LockError, LockInput};
use aruna_operations::s3::bucket::key_removal::{
    RemovalError, RemovalInput, RemoveHolderOperation,
};
use aruna_operations::s3::bucket::key_unlock::{UnlockBucketOperation, UnlockError, UnlockInput};
use aruna_operations::s3::key_status::{AuditPageOperation, KeySnapshot};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Length of a raw bucket private key in an unlock body.
const KEY_BYTES: usize = 32;
const AUDIT_PAGE: usize = 50;
const AUDIT_MAX: usize = 500;

#[derive(OpenApi)]
#[openapi()]
pub struct BucketKeysDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BucketKeysDoc::openapi())
        .routes(routes!(unlock_bucket))
        .routes(routes!(extend_unlock))
        .routes(routes!(lock_bucket))
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UnlockQuery {
    /// The bucket id the key belongs to, as shown in the status.
    pub bucket_id: String,
    pub generation: u64,
    /// Unlock length; without it the session lasts until the bucket maximum.
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ExtendRequest {
    pub generation: u64,
    pub session_id: String,
    #[serde(default)]
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct HolderView {
    pub user_id: String,
    pub name: Option<String>,
    /// `creator`, `admin` or `explicit`.
    #[schema(value_type = String)]
    pub origin: HolderOrigin,
    /// `ready`, `pending`, `missing_key` or `unavailable`.
    #[schema(value_type = String)]
    pub state: HolderState,
    pub has_recovery: Option<bool>,
    pub granted_by: Option<String>,
    pub granted_at_ms: Option<u64>,
}

impl From<&HolderEntry> for HolderView {
    fn from(entry: &HolderEntry) -> Self {
        Self {
            user_id: entry.user_id.to_string(),
            name: None,
            origin: entry.origin,
            state: entry.state,
            has_recovery: entry.has_recovery,
            granted_by: entry
                .grant
                .as_ref()
                .map(|grant| grant.granted_by.to_string()),
            granted_at_ms: entry.grant.as_ref().map(|grant| grant.granted_at_ms),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct HoldersResponse {
    pub holders: Vec<HolderView>,
    /// False when a key directory lookup failed; the list is then partial.
    pub complete: bool,
    pub unresolved: usize,
    pub recovery: RecoveryView,
    /// Lowercase hex digest of the grants and copies; a removal names it.
    pub revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GrantRequest {
    pub user_id: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RemovalQuery {
    /// The `revision` of the holder list the removal was decided on.
    pub revision: String,
    /// Accepts that the removal breaks the recovery rule.
    #[serde(default)]
    pub confirm_recovery: bool,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct CopiesQuery {
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CopyView {
    pub bucket_id: String,
    pub generation: u64,
    /// The key directory record the copy is sealed to.
    pub key_record: String,
    /// The vault slot of that user key.
    pub key_id: String,
    /// Standard base64 of the encapsulated key.
    pub enc: String,
    /// Standard base64 of the sealed private key.
    pub ciphertext: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CopiesResponse {
    pub copies: Vec<CopyView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RotateRequest {
    pub expected_generation: u64,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AuditQuery {
    /// Events per page, 1 to 500; 50 by default.
    pub limit: Option<usize>,
    /// The `next_cursor` of the previous page.
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct AuditEventView {
    pub event_id: String,
    pub at_ms: u64,
    pub action: String,
    pub actor_user_id: Option<String>,
    pub node_id: String,
    pub generation: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub reason: Option<String>,
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct AuditResponse {
    pub events: Vec<AuditEventView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

fn parse_ulid(value: &str, field: &str) -> ServerResult<Ulid> {
    Ulid::from_string(value).map_err(|_| ServerError::BadRequestReason(format!("invalid {field}")))
}

fn parse_user(value: &str) -> ServerResult<UserId> {
    UserId::from_string(value).map_err(|_| ServerError::BadRequestReason("invalid user".into()))
}

fn wire_name(value: impl Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn forbidden_holder() -> ServerError {
    refused(
        StatusCode::FORBIDDEN,
        "not_holder",
        "the caller holds no key of this bucket",
    )
}

/// A bucket's snapshot after the caller proved holder or group-admin authority.
async fn holder_snapshot(
    state: &ServerState,
    auth: &AuthContext,
    bucket: &str,
) -> ServerResult<KeySnapshot> {
    let group_id = bucket_group(state, bucket).await?;
    let snapshot = read_snapshot(state, bucket, group_id).await?;
    if !is_holder(&snapshot, auth.user_id) {
        return Err(forbidden_holder());
    }
    Ok(snapshot)
}

/// The secret is taken out of the request body at once and never logged or returned.
fn unlock_key(body: Bytes) -> ServerResult<SharedSecret> {
    if body.len() != KEY_BYTES {
        return Err(refused(
            StatusCode::BAD_REQUEST,
            "wrong_key",
            "the body must be exactly 32 key bytes",
        ));
    }
    Ok(SharedSecret::new(SecretBytes::new(body.to_vec())))
}

fn status_view(status: &UnlockStatus) -> UnlockView {
    unlock_view(Some(status), now_ms())
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/encryption/unlock",
    tag = "data/storage",
    summary = "Unlock a bucket key generation",
    description = r#"Installs a holder's opened bucket private key in this node's memory.

**Authentication**: realm bearer token without path restrictions of a current key holder.

**Behavior**
- The body is exactly 32 raw key bytes as `application/octet-stream`. The node checks them
  against the generation's public key, keeps them only in memory and never returns them.
- An audit intent is stored before the key becomes usable.
- Any 4xx answer means the key was not applied. A 5xx answer, a network error or a timeout is
  an unknown outcome: read the status before trying again."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        UnlockQuery
    ),
    request_body(content = Vec<u8>, content_type = "application/octet-stream", description = "The 32-byte private key"),
    responses(
        (status = 200, description = "The unlock state", body = UnlockView),
        (status = 400, description = "`wrong_key` or `invalid_duration`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller holds no key of this bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`stale_generation` or `not_encrypted`", body = ErrorResponse),
        (status = 503, description = "`unlock_capacity`: no room for another unlocked key", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn unlock_bucket(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<UnlockQuery>,
    body: Bytes,
) -> ServerResult<Json<UnlockView>> {
    let private_key = unlock_key(body)?;
    let auth = require_unrestricted_auth(&state, auth)?;
    let bucket_id = parse_ulid(&query.bucket_id, "bucket_id")?;
    let group_id = bucket_group(&state, &bucket).await?;
    let input = UnlockInput {
        bucket,
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        caller: auth.user_id,
        key: BucketKeyRef::new(bucket_id, query.generation),
        duration: query.duration_ms.map(Duration::from_millis),
        now_ms: now_ms(),
    };
    let operation = UnlockBucketOperation::new(input, private_key);
    let status = drive(operation, &state.get_ctx())
        .await
        .map_err(|error| match error {
            UnlockError::Settings(error) => settings_refusal(error),
            UnlockError::Key(error) => key_refusal(&error),
            UnlockError::Blob(error) => blob_refusal(error),
            UnlockError::NotHolder => forbidden_holder(),
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok(Json(status_view(&status)))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/encryption/extend",
    tag = "data/storage",
    summary = "Extend an unlock session",
    description = r#"Moves the deadline of a running unlock session of one generation.

**Authentication**: realm bearer token without path restrictions of a current key holder.

**Behavior**
- The new deadline counts from now and never passes the session maximum.
- `session_id` must belong to `generation`, otherwise 409 `session_mismatch`."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = ExtendRequest,
        example = json!({ "generation": 1, "session_id": "01JAMXR0C8M7T2D4WQ3V9KX6EZ", "duration_ms": 900000 })
    ),
    responses(
        (status = 200, description = "The unlock state", body = UnlockView),
        (status = 400, description = "`invalid_duration` or an invalid session id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller holds no key of this bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`session_mismatch`, `bucket_locked` or `not_encrypted`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn extend_unlock(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<ExtendRequest>,
) -> ServerResult<Json<UnlockView>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let session_id = parse_ulid(&request.session_id, "session_id")?;
    let group_id = bucket_group(&state, &bucket).await?;
    let input = ExtendInput {
        bucket,
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        caller: auth.user_id,
        generation: request.generation,
        session_id,
        duration: request.duration_ms.map(Duration::from_millis),
        now_ms: now_ms(),
    };
    let status = drive(ExtendBucketOperation::new(input), &state.get_ctx())
        .await
        .map_err(|error| match error {
            ExtendError::Settings(error) => settings_refusal(error),
            ExtendError::Blob(error) => blob_refusal(error),
            ExtendError::NotEncrypted => not_encrypted(),
            ExtendError::NotHolder => forbidden_holder(),
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok(Json(status_view(&status)))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/encryption/lock",
    tag = "data/storage",
    summary = "Lock a bucket",
    description = r#"Removes every unlocked key generation of a bucket from this node's memory.

**Authentication**: realm bearer token without path restrictions of a key holder or group admin.

**Behavior**
- Locks every generation, including transition sources. Takes no body and is idempotent.
- Reads already admitted finish; new plaintext reads are refused."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (status = 200, description = "The locked state of the active generation", body = UnlockView),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Neither a key holder nor a group admin", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`not_encrypted`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn lock_bucket(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<UnlockView>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = bucket_group(&state, &bucket).await?;
    let now_ms = now_ms();
    let input = LockInput {
        bucket,
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        caller: Some(auth.user_id),
        session: None,
        now_ms,
    };
    let result = drive(LockBucketOperation::new(input), &state.get_ctx())
        .await
        .map_err(|error| match error {
            LockError::Settings(error) => settings_refusal(error),
            LockError::Blob(error) => blob_refusal(error),
            LockError::NotEncrypted => not_encrypted(),
            LockError::NotHolder => forbidden_holder(),
            other => ServerError::InternalError(other.to_string()),
        })?;
    let mut view = unlock_view(None, now_ms);
    if !result.locked.is_empty() {
        view.lock_reason = Some("manual".to_string());
        view.locked_at_ms = Some(now_ms);
    }
    Ok(Json(view))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_body_exact() {
        assert!(unlock_key(Bytes::from(vec![1; KEY_BYTES])).is_ok());
        for length in [0, 31, 33, 64] {
            let refusal = unlock_key(Bytes::from(vec![1; length])).unwrap_err();
            assert_eq!(refusal.status_code(), StatusCode::BAD_REQUEST);
            assert_eq!(refusal.response_body().code.as_deref(), Some("wrong_key"));
        }
    }

    #[test]
    fn key_never_printed() {
        let secret = unlock_key(Bytes::from(vec![0xab; KEY_BYTES])).unwrap();
        let printed = format!("{secret:?}");
        assert!(!printed.contains("171") && !printed.to_lowercase().contains("ab, "));
    }

    #[test]
    fn audit_wire_names() {
        use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome};
        assert_eq!(wire_name(AuditAction::RestartLock), "restart_lock");
        assert_eq!(wire_name(AuditOutcome::Intent), "intent");
    }
}
