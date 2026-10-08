//! Serves the bucket key routes: unlock, extend, lock, holders, the caller's sealed copies,
//! rotation and the key audit of an encrypted bucket on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::encryption::{
    EncryptionStatus, RecoveryView, UnlockView, blob_refusal, bucket_group, change_bucket,
    current_status, holder_report, is_holder, key_refusal, not_encrypted, read_snapshot, refused,
    settings_refusal, unlock_view,
};
use super::super::routing::ensure_group_admin;
use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::encryption::{BucketKeyRef, HolderOrigin, UnlockStatus};
use aruna_core::structs::storage::holders::{
    HolderEntry, HolderState, KeyLookup, holder_revision, revision_with_facts,
};
use aruna_core::types::GroupId;
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::s3::bucket::holders::lookup_keys;
use aruna_operations::s3::bucket::key_extend::{ExtendBucketOperation, ExtendError, ExtendInput};
use aruna_operations::s3::bucket::key_grant::{GrantError, GrantHolderOperation, GrantInput};
use aruna_operations::s3::bucket::key_lock::{LockBucketOperation, LockError, LockInput};
use aruna_operations::s3::bucket::key_removal::{
    RemovalError, RemovalInput, RemoveHolderOperation,
};
use aruna_operations::s3::bucket::key_unlock::{
    UnlockBucketOperation, UnlockError, UnlockInput, unlock_and_wake,
};
use aruna_operations::s3::bucket::rotate::KeyChange;
use aruna_operations::s3::bucket::seal_missing::{SealMissingInput, SealMissingOperation};
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
        .routes(routes!(list_holders, grant_holder))
        .routes(routes!(remove_holder))
        .routes(routes!(my_copies))
        .routes(routes!(rotate_key))
        .routes(routes!(bucket_audit))
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UnlockQuery {
    /// The bucket id the key belongs to, as shown in the status.
    pub bucket_id: String,
    /// The key generation the key belongs to.
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
    /// The key generation whose copies to return.
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
        (status = 200, description = "The unlock state", body = UnlockView, example = json!({ "state": "unlocked", "lock_reason": null, "locked_at_ms": null, "session_id": "01JAMXR0C8M7T2D4WQ3V9KX6EZ", "unlocked_at_ms": 1790000000000_u64, "deadline_ms": 1790003600000_u64, "max_deadline_ms": 1790007200000_u64 })),
        (status = 400, description = "`wrong_key` or `invalid_duration`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller holds no key of this bucket, or is a federated user (code `foreign_encryption_keys_unsupported`)", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`stale_generation`, `not_encrypted`, or `no_copy` when the caller has no sealed copy of the generation", body = ErrorResponse),
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
    crate::routes::storage::abe::refuse_foreign_keys(&auth)?;
    let bucket_id = parse_ulid(&query.bucket_id, "bucket_id")?;
    let group_id = bucket_group(&state, &bucket).await?;
    let input = UnlockInput {
        bucket: bucket.clone(),
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        caller: auth.user_id,
        key: BucketKeyRef::new(bucket_id, query.generation),
        duration: query.duration_ms.map(Duration::from_millis),
        now_ms: now_ms(),
    };
    let operation = UnlockBucketOperation::new(input, private_key);
    let origin = (state.get_realm_id(), state.get_node_id());
    let context = state.get_ctx();
    let status = unlock_and_wake(&context, operation, origin, state.rocrate_limits())
        .await
        .map_err(|error| match error {
            UnlockError::Settings(error) => settings_refusal(error),
            UnlockError::Key(error) => key_refusal(&error),
            UnlockError::Blob(error) => blob_refusal(error),
            UnlockError::NotHolder => forbidden_holder(),
            UnlockError::NoCopy => refused(
                StatusCode::CONFLICT,
                "no_copy",
                "the caller has no sealed copy of this key generation",
            ),
            other => ServerError::InternalError(other.to_string()),
        })?;
    seal_missing(&state, &bucket, group_id, status.key).await;
    // An unlocked node is an issuer: a due raise happens now and its requests are issued.
    if let Err(error) = crate::routes::storage::abe::epoch_run(&state, &auth, &bucket, false).await
    {
        tracing::warn!(event = "abe.epoch_raise.failed", error = %error);
    }
    Ok(Json(status_view(&status)))
}

/// Seals copies for holders that still lack one; a failure leaves them pending for later.
async fn seal_missing(state: &ServerState, bucket: &str, group_id: GroupId, key: BucketKeyRef) {
    let snapshot = match read_snapshot(state, bucket, group_id).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(%bucket, %error, "could not read holders to seal missing copies");
            return;
        }
    };
    let users = snapshot
        .info
        .as_ref()
        .map(|info| info.created_by)
        .into_iter()
        .chain(snapshot.admins.iter().copied())
        .chain(snapshot.grants.iter().map(|grant| grant.user_id))
        .collect::<Vec<_>>();
    let lookups = lookup_keys(&state.get_ctx(), state.get_node_id(), users).await;
    let input = SealMissingInput {
        bucket: bucket.to_string(),
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        key,
        lookups,
    };
    if let Err(error) = drive(SealMissingOperation::new(input), &state.get_ctx()).await {
        tracing::warn!(%bucket, ?error, "missing holder copies stay pending");
    }
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
        (status = 200, description = "The unlock state", body = UnlockView, example = json!({ "state": "unlocked", "lock_reason": null, "locked_at_ms": null, "session_id": "01JAMXR0C8M7T2D4WQ3V9KX6EZ", "unlocked_at_ms": 1790000000000_u64, "deadline_ms": 1790003600000_u64, "max_deadline_ms": 1790007200000_u64 })),
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
        (status = 200, description = "The locked state of the active generation", body = UnlockView, example = json!({ "state": "locked", "lock_reason": "manual", "locked_at_ms": 1790000000000_u64, "session_id": null, "unlocked_at_ms": null, "deadline_ms": null, "max_deadline_ms": null })),
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

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/storage/encryption/holders",
    tag = "data/storage",
    summary = "List a bucket's key holders",
    description = r#"Returns the holders of the active key generation and their readiness.

**Authentication**: realm bearer token without path restrictions of a key holder or group admin.

**Behavior**
- `complete` is false when a key directory lookup failed; the list is then partial and
  `recovery` reads `unknown` unless resolved holders already meet it.
- `revision` names this list in a later removal."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (status = 200, description = "The holders", body = HoldersResponse, example = json!({ "holders": [{ "user_id": "01JAMXS1P3T9V6Q8W2Y4Z7B5CD@realm", "name": null, "origin": "explicit", "state": "ready", "has_recovery": true, "granted_by": "01JAMXS1P3T9V6Q8W2Y4Z7B5CE@realm", "granted_at_ms": 1790000000000_u64 }], "complete": true, "unresolved": 0, "recovery": { "state": "met", "ready_holders": 2, "ready_with_recovery": 1 }, "revision": "5d1c0a6f9e1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5" })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Neither a key holder nor a group admin", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`not_encrypted`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_holders(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<HoldersResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let snapshot = holder_snapshot(&state, &auth, &bucket).await?;
    let report = holder_report(&state, &snapshot)
        .await
        .ok_or_else(not_encrypted)?;
    let creator = snapshot
        .info
        .as_ref()
        .map(|info| info.created_by)
        .unwrap_or_default();
    let rows = holder_revision(&snapshot.grants, &snapshot.copies);
    let revision = revision_with_facts(rows, creator, &snapshot.admins, &report);
    Ok(Json(HoldersResponse {
        holders: report.holders.iter().map(HolderView::from).collect(),
        complete: report.complete,
        unresolved: report.unresolved,
        recovery: RecoveryView::from(&report.recovery),
        revision: hex::encode(revision),
    }))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/encryption/holders",
    tag = "data/storage",
    summary = "Grant a bucket key to a user",
    description = r#"Adds an explicit key holder and seals a key copy to the user's published keys.

**Authentication**: realm bearer token without path restrictions, with WRITE on the owning
group's admin path.

**Behavior**
- While the active generation is locked, or the user has no published key, the holder is
  `pending` and receives a copy at the next unlock."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(content = GrantRequest, example = json!({ "user_id": "01JAMXS1P3T9V6Q8W2Y4Z7B5CD@realm" })),
    responses(
        (status = 200, description = "The new holder", body = HolderView, example = json!({ "user_id": "01JAMXS1P3T9V6Q8W2Y4Z7B5CD@realm", "name": null, "origin": "explicit", "state": "ready", "has_recovery": true, "granted_by": "01JAMXS1P3T9V6Q8W2Y4Z7B5CE@realm", "granted_at_ms": 1790000000000_u64 })),
        (status = 400, description = "Invalid user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`not_encrypted`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn grant_holder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<GrantRequest>,
) -> ServerResult<Json<HolderView>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let user_id = parse_user(&request.user_id)?;
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let mut lookups = lookup_keys(&state.get_ctx(), state.get_node_id(), [user_id]).await;
    let lookup = lookups.remove(&user_id).unwrap_or(KeyLookup::Unavailable);
    let has_recovery = match &lookup {
        KeyLookup::Keys(keys) => Some(keys.iter().any(|key| key.has_recovery)),
        KeyLookup::Missing => Some(false),
        KeyLookup::Unavailable => None,
    };
    let input = GrantInput {
        bucket,
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        user_id,
        granted_by: auth.user_id,
        lookup,
        now_ms: now_ms(),
    };
    let result = drive(GrantHolderOperation::new(input), &state.get_ctx())
        .await
        .map_err(|error| match error {
            GrantError::Settings(error) => settings_refusal(error),
            GrantError::Blob(error) => blob_refusal(error),
            GrantError::NotEncrypted => not_encrypted(),
            GrantError::NotAdmin => ServerError::Forbidden,
            other => ServerError::InternalError(other.to_string()),
        })?;
    let grant = result.grant;
    Ok(Json(HolderView {
        user_id: grant.user_id.to_string(),
        name: None,
        origin: grant.origin,
        state: result.state,
        has_recovery,
        granted_by: Some(grant.granted_by.to_string()),
        granted_at_ms: Some(grant.granted_at_ms),
    }))
}

#[utoipa::path(
    delete,
    path = "/data/buckets/{bucket}/storage/encryption/holders/{user}",
    tag = "data/storage",
    summary = "Remove an explicit key holder",
    description = r#"Removes an explicit grant and the key copies it gave.

**Authentication**: realm bearer token without path restrictions, with WRITE on the owning
group's admin path.

**Behavior**
- `revision` must match the current holder list, otherwise 409 `stale_holders`.
- A removal that breaks the recovery rule needs `confirm_recovery=true`, otherwise 409
  `recovery_confirmation_required`.
- The removal does not lock the bucket and does not rotate its key."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        ("user" = String, Path, description = "User id as `<ULID>@<realm>`"),
        RemovalQuery
    ),
    responses(
        (status = 204, description = "The grant was removed"),
        (status = 400, description = "Invalid user id or revision", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket or explicit grant not found", body = ErrorResponse),
        (status = 409, description = "`stale_holders`, `recovery_confirmation_required` or `not_encrypted`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove_holder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((bucket, user)): Path<(String, String)>,
    Query(query): Query<RemovalQuery>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let user_id = parse_user(&user)?;
    let revision: [u8; 32] = hex::decode(&query.revision)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| ServerError::BadRequestReason("invalid revision".into()))?;
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let snapshot = read_snapshot(&state, &bucket, group_id).await?;
    let creator = snapshot.info.as_ref().map(|info| info.created_by);
    let users = creator
        .into_iter()
        .chain(snapshot.admins.iter().copied())
        .chain(snapshot.grants.iter().map(|grant| grant.user_id))
        .collect::<Vec<_>>();
    let lookups = lookup_keys(&state.get_ctx(), state.get_node_id(), users).await;
    let input = RemovalInput {
        bucket,
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        user_id,
        removed_by: auth.user_id,
        revision,
        confirm_recovery: query.confirm_recovery,
        lookups,
        now_ms: now_ms(),
    };
    drive(RemoveHolderOperation::new(input), &state.get_ctx())
        .await
        .map_err(|error| match error {
            RemovalError::Settings(error) => settings_refusal(error),
            RemovalError::NotEncrypted => not_encrypted(),
            RemovalError::NoSuchGrant => ServerError::NotFound,
            RemovalError::NotAdmin => ServerError::Forbidden,
            RemovalError::StaleHolders => refused(
                StatusCode::CONFLICT,
                "stale_holders",
                "the holders changed since they were read",
            ),
            RemovalError::RecoveryConfirmationRequired => refused(
                StatusCode::CONFLICT,
                "recovery_confirmation_required",
                "removing this holder breaks the recovery rule",
            ),
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/storage/encryption/copies/me",
    tag = "data/storage",
    summary = "Read the caller's sealed key copies",
    description = r#"Returns the bucket private key copies sealed to the caller's user keys.

**Authentication**: realm bearer token without path restrictions of a current key holder.

**Behavior**
- Only copies of the requested `generation`. A generation without a copy for the caller
  returns an empty list.
- The copies are sealed; only the caller's vault opens them."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        CopiesQuery
    ),
    responses(
        (status = 200, description = "The caller's copies", body = CopiesResponse, example = json!({ "copies": [{ "bucket_id": "01JAMXQ7B1D7Q8E7Q2F3R8Z9KC", "generation": 1, "key_record": "01JAMXT2V4W6X8Y0Z2A4B6C8DE", "key_id": "laptop", "enc": "qL3UuCZ0XkWbQZ2yZ8m1qL3UuCZ0XkWbQZ2yZ8m1qL0=", "ciphertext": "c2VhbGVkIGtleSBieXRlcw==", "created_at_ms": 1790000000000_u64 }] })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller holds no key of this bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn my_copies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<CopiesQuery>,
) -> ServerResult<Json<CopiesResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let snapshot = holder_snapshot(&state, &auth, &bucket).await?;
    let copies = snapshot
        .copies
        .iter()
        .filter(|copy| copy.user_id == auth.user_id && copy.key.generation == query.generation)
        .map(|copy| CopyView {
            bucket_id: copy.key.bucket_id.to_string(),
            generation: copy.key.generation,
            key_record: copy.key_record.to_string(),
            key_id: copy.key_id.clone(),
            enc: BASE64_STANDARD.encode(copy.enc),
            ciphertext: BASE64_STANDARD.encode(&copy.ciphertext),
            created_at_ms: copy.created_at_ms,
        })
        .collect();
    Ok(Json(CopiesResponse { copies }))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/storage/encryption/rotate",
    tag = "data/storage",
    summary = "Rotate a bucket key",
    description = r#"Starts a new key generation and moves this node's copies to it.

**Authentication**: realm bearer token without path restrictions, with WRITE on the owning
group's admin path.

**Behavior**
- Creates the next key generation, sealed to every holder with a published key, and starts a
  transition that grants this node's archives to it. Needs the current key unlocked."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(content = RotateRequest, example = json!({ "expected_generation": 1 })),
    responses(
        (status = 200, description = "The status after the rotation started", body = EncryptionStatus, example = json!({ "bucket": "research-raw", "mode": "node_managed", "bucket_id": "01JAMXQ7B1D7Q8E7Q2F3R8Z9KC", "storage_generation": 1, "key_generation": 1, "public_key": "qL3UuCZ0XkWbQZ2yZ8m1qL3UuCZ0XkWbQZ2yZ8m1qL0=", "fingerprint": "5d1c0a6f9e1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5", "cipher": "chacha20_poly1305", "block_keys": "content_derived", "max_unlock_ms": null, "unlock": { "state": "unlocked", "lock_reason": null, "locked_at_ms": null, "session_id": "01JAMXR0C8M7T2D4WQ3V9KX6EZ", "unlocked_at_ms": 1790000000000_u64, "deadline_ms": null, "max_deadline_ms": null }, "generations": [], "holders": { "ready": 2, "pending": 0, "missing_key": 0 }, "recovery": { "state": "met", "ready_holders": 2, "ready_with_recovery": 1 }, "transition": null, "abe": null, "caller": { "holder": true, "ready_copy": true, "admin": true } })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`stale_generation`, `bucket_locked`, `open_uploads`, `recovery_unmet` or `transition_running`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn rotate_key(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<RotateRequest>,
) -> ServerResult<Json<EncryptionStatus>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let snapshot = read_snapshot(&state, &bucket, group_id).await?;
    let change = KeyChange::Rotate;
    let expected = request.expected_generation;
    let target = (group_id, auth.user_id);
    change_bucket(&state, &bucket, target, &snapshot, change, (expected, None)).await?;
    let status = current_status(&state, bucket, group_id, auth.user_id).await?;
    Ok(Json(status))
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/storage/encryption/audit",
    tag = "data/storage",
    summary = "Read a bucket's key audit",
    description = r#"Returns the bucket's key state events on this node, oldest first.

**Authentication**: realm bearer token without path restrictions of a key holder or group admin.

**Behavior**
- Events never contain key bytes, grants or vault payloads.
- An `intent` without a later `applied` event is not a successful change.
- Pass `next_cursor` as `cursor` to read the next page."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        AuditQuery
    ),
    responses(
        (status = 200, description = "One page of events", body = AuditResponse, example = json!({ "events": [{ "event_id": "01JAMXV3W5X7Y9Z1A3B5C7D9EF", "at_ms": 1790000000000_u64, "action": "unlock", "actor_user_id": "01JAMXS1P3T9V6Q8W2Y4Z7B5CD@realm", "node_id": "b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c", "generation": 1, "deadline_ms": 1790003600000_u64, "reason": null, "outcome": "applied" }], "next_cursor": "01JAMXV3W5X7Y9Z1A3B5C7D9EF" })),
        (status = 400, description = "Invalid cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Neither a key holder nor a group admin", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn bucket_audit(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<AuditQuery>,
) -> ServerResult<Json<AuditResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(|cursor| parse_ulid(cursor, "cursor"))
        .transpose()?;
    let limit = query.limit.unwrap_or(AUDIT_PAGE).clamp(1, AUDIT_MAX);
    let snapshot = holder_snapshot(&state, &auth, &bucket).await?;
    let Some(bucket_id) = snapshot.settings.bucket_id else {
        return Ok(Json(AuditResponse {
            events: Vec::new(),
            next_cursor: None,
        }));
    };
    let operation = AuditPageOperation::new(bucket_id, cursor, limit);
    let (events, next) = drive(operation, &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let events = events
        .into_iter()
        .map(|event| AuditEventView {
            event_id: event.event_id.to_string(),
            at_ms: event.at_ms,
            action: wire_name(event.action),
            actor_user_id: event.actor.map(|actor| actor.to_string()),
            node_id: event.node_id.to_string(),
            generation: event.generation,
            deadline_ms: event.deadline_ms,
            reason: event.reason,
            outcome: wire_name(event.outcome),
        })
        .collect();
    Ok(Json(AuditResponse {
        events,
        next_cursor: next.map(|next| next.to_string()),
    }))
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

#[cfg(test)]
#[path = "keys_tests.rs"]
mod route_tests;
