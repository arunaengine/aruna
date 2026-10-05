//! Serves the bucket encryption settings and status: mode, key generations, unlock state,
//! holder readiness and recovery of a bucket on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::routing::ensure_group_admin;
use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::errors::BlobError;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketKeyError, EncryptionMode, KeyState, UnlockStatus,
};
use aruna_core::structs::storage::holders::{
    HolderReport, HolderState, Recovery, RecoveryState, resolve_holders,
};
use aruna_core::structs::storage::key_audit::AuditAction;
use aruna_core::structs::storage::transition::{EncryptionTransition, TransitionState};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_operations::driver::{DriverContext, drive, now_ms};
use aruna_operations::s3::bucket::encryption::{
    EnableEncryptionOperation, EnableError, EnableInput,
};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::bucket::holders::lookup_keys;
use aruna_operations::s3::bucket::key_install::{InstallInput, InstallKeyOperation};
use aruna_operations::s3::bucket::key_rows::SettingsError;
use aruna_operations::s3::bucket::rotate::{
    ChangeEncryptionOperation, ChangeError, ChangeInput, KeyChange,
};
use aruna_operations::s3::key_status::{KeySnapshot, KeyStatusError, KeyStatusOperation};
use aruna_operations::s3::unlock_limit::{
    UnlockLimitError, UnlockLimitInput, UnlockLimitOperation,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi()]
pub struct StorageEncryptionDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageEncryptionDoc::openapi())
        .routes(routes!(get_bucket_encryption, put_bucket_encryption))
}

/// Unlock state of one key generation on this node.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct UnlockView {
    /// `locked` or `unlocked`.
    pub state: String,
    /// `manual`, `timed` or `restart` when known; null while unlocked.
    pub lock_reason: Option<String>,
    pub locked_at_ms: Option<u64>,
    pub session_id: Option<String>,
    pub unlocked_at_ms: Option<u64>,
    /// When the session locks unless extended; null without a deadline.
    pub deadline_ms: Option<u64>,
    /// The latest deadline any extension can reach.
    pub max_deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct HolderCounts {
    pub ready: Option<usize>,
    pub pending: Option<usize>,
    pub missing_key: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RecoveryView {
    /// `met`, `degraded` or `unknown`.
    #[schema(value_type = String)]
    pub state: RecoveryState,
    pub ready_holders: Option<usize>,
    pub ready_with_recovery: Option<usize>,
}

impl From<&Recovery> for RecoveryView {
    fn from(recovery: &Recovery) -> Self {
        Self {
            state: recovery.state,
            ready_holders: Some(recovery.ready_holders),
            ready_with_recovery: Some(recovery.ready_with_recovery),
        }
    }
}

/// Progress of a mode change or rotation of this node's stored copies.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct TransitionView {
    /// `encrypt`, `decrypt`, `reencode` or `rotate`.
    pub kind: String,
    /// `running`, `awaiting_key`, `cleanup`, `blocked` or `finished`.
    pub state: String,
    pub source_generation: Option<u64>,
    pub target_generation: Option<u64>,
    pub done: u64,
    pub remaining: Option<u64>,
    pub failed: u64,
    pub cleanup_remaining: Option<u64>,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub blocked_reason: Option<String>,
}

impl From<&EncryptionTransition> for TransitionView {
    fn from(transition: &EncryptionTransition) -> Self {
        let name = |value: serde_json::Value| value.as_str().unwrap_or_default().to_string();
        Self {
            kind: name(serde_json::json!(transition.kind)),
            state: name(serde_json::json!(transition.reported_state())),
            source_generation: transition.source.map(|source| source.generation),
            target_generation: transition
                .target
                .plan
                .as_ref()
                .map(|plan| plan.key.generation),
            done: transition.done,
            remaining: Some(transition.remaining),
            failed: transition.failed,
            cleanup_remaining: Some(transition.cleanup_remaining),
            started_at_ms: transition.started_at_ms,
            finished_at_ms: transition.finished_at_ms,
            blocked_reason: transition.blocked_reason.clone(),
        }
    }
}

/// What the caller may do; display only, every route checks again.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CallerView {
    pub holder: bool,
    pub ready_copy: bool,
    pub admin: bool,
}

/// A key generation this node still needs: the active one and transition sources.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GenerationView {
    pub generation: u64,
    /// `active` or `source`.
    pub role: String,
    /// Standard base64 of the 32-byte public key.
    pub public_key: String,
    /// Lowercase hex SHA-256 of the public key.
    pub fingerprint: String,
    pub unlock: UnlockView,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct EncryptionStatus {
    pub bucket: String,
    #[schema(value_type = String, example = "node_managed")]
    pub mode: EncryptionMode,
    pub bucket_id: Option<String>,
    pub storage_generation: u64,
    pub key_generation: u64,
    pub public_key: Option<String>,
    pub fingerprint: Option<String>,
    #[schema(value_type = String, example = "chacha20_poly1305")]
    pub cipher: BlockCipher,
    #[schema(value_type = String, example = "content_derived")]
    pub block_keys: BlockKeys,
    pub max_unlock_ms: Option<u64>,
    /// Unlock state of the active generation; null for a plain bucket.
    pub unlock: Option<UnlockView>,
    pub generations: Vec<GenerationView>,
    pub holders: Option<HolderCounts>,
    pub recovery: Option<RecoveryView>,
    pub transition: Option<TransitionView>,
    pub caller: CallerView,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct EncryptionRequest {
    #[schema(value_type = String, example = "node_managed")]
    pub mode: EncryptionMode,
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub cipher: Option<BlockCipher>,
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub block_keys: Option<BlockKeys>,
    /// Absent keeps the current maximum; null lets an unlock last until lock or restart.
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<u64>)]
    pub max_unlock_ms: Option<Option<u64>>,
    /// The `storage_generation` the caller read; another value is refused as stale.
    pub expected_generation: u64,
}

/// A present field, which may be null, as opposed to an absent one.
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<u64>>, D::Error> {
    Option::<u64>::deserialize(deserializer).map(Some)
}

pub(crate) fn refused(status: StatusCode, code: &'static str, message: &str) -> ServerError {
    ServerError::Refused(status, code, message.to_string())
}

/// The REST answer to a typed key failure; no key bytes reach the message.
pub(crate) fn key_refusal(error: &BucketKeyError) -> ServerError {
    let message = error.to_string();
    let (status, code) = match error {
        BucketKeyError::Locked(_) => (StatusCode::CONFLICT, "bucket_locked"),
        BucketKeyError::WrongKey | BucketKeyError::Fingerprint => {
            (StatusCode::BAD_REQUEST, "wrong_key")
        }
        BucketKeyError::StaleGeneration { .. } => (StatusCode::CONFLICT, "stale_generation"),
        BucketKeyError::SessionMismatch => (StatusCode::CONFLICT, "session_mismatch"),
        BucketKeyError::Capacity => (StatusCode::SERVICE_UNAVAILABLE, "unlock_capacity"),
        BucketKeyError::InvalidDuration => (StatusCode::BAD_REQUEST, "invalid_duration"),
        BucketKeyError::Seal => return ServerError::InternalError(message),
        BucketKeyError::Unsupported => (StatusCode::NOT_IMPLEMENTED, "not_supported"),
    };
    ServerError::Refused(status, code, message)
}

pub(crate) fn blob_refusal(error: BlobError) -> ServerError {
    match error {
        BlobError::BucketKey(error) => key_refusal(&error),
        BlobError::SizeLimitExceeded { limit } => {
            refused(StatusCode::CONFLICT, "object_too_large", &too_large(limit))
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

/// The refusal text for an object above the encryption ceiling, in bytes and TiB.
pub(crate) fn too_large(limit: u64) -> String {
    let tib = limit as f64 / (1u64 << 40) as f64;
    format!("an object is larger than the {limit} byte ({tib:.2} TiB) limit of encrypted objects")
}

pub(crate) fn settings_refusal(error: SettingsError) -> ServerError {
    match error {
        SettingsError::NoSuchBucket | SettingsError::GroupMismatch => ServerError::NotFound,
        SettingsError::MissingAuthority => ServerError::ServiceUnavailable,
        other => ServerError::InternalError(other.to_string()),
    }
}

pub(crate) fn not_encrypted() -> ServerError {
    refused(
        StatusCode::CONFLICT,
        "not_encrypted",
        "the bucket has no keys",
    )
}

pub(crate) async fn bucket_group(state: &ServerState, bucket: &str) -> ServerResult<GroupId> {
    drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .map(|info| info.group_id)
    .map_err(|error| match error {
        GetBucketError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })
}

pub(crate) async fn ensure_bucket_read(
    state: &ServerState,
    auth: &AuthContext,
    group_id: GroupId,
    bucket: &str,
) -> ServerResult<()> {
    let path = bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket);
    crate::auth::ensure_permission(state, auth, path, Permission::READ).await
}

pub(crate) async fn read_snapshot(
    state: &ServerState,
    bucket: &str,
    group_id: GroupId,
) -> ServerResult<KeySnapshot> {
    let operation = KeyStatusOperation::new(bucket.to_string(), state.get_realm_id(), group_id);
    drive(operation, &state.get_ctx())
        .await
        .map_err(|error| match error {
            KeyStatusError::Settings(error) => settings_refusal(error),
            KeyStatusError::Blob(error) => blob_refusal(error),
            other => ServerError::InternalError(other.to_string()),
        })
}

/// Holder states of the active generation; key directory failures read as unavailable.
pub(crate) async fn holder_report(
    state: &ServerState,
    snapshot: &KeySnapshot,
) -> Option<HolderReport> {
    let creator = snapshot.info.as_ref()?.created_by;
    let active = snapshot.settings.active_key()?;
    let users = std::iter::once(creator)
        .chain(snapshot.admins.iter().copied())
        .chain(snapshot.grants.iter().map(|grant| grant.user_id))
        .collect::<Vec<_>>();
    let lookups = lookup_keys(&state.get_ctx(), state.get_node_id(), users).await;
    let copies: Vec<_> = snapshot
        .copies
        .iter()
        .filter(|copy| copy.key == active)
        .cloned()
        .collect();
    Some(resolve_holders(
        creator,
        &snapshot.admins,
        &snapshot.grants,
        &lookups,
        &copies,
    ))
}

/// Whether `user` holds implicit or explicit key authority of the bucket now.
pub(crate) fn is_holder(snapshot: &KeySnapshot, user: UserId) -> bool {
    let creator = snapshot.info.as_ref().map(|info| info.created_by);
    creator == Some(user)
        || snapshot.admins.contains(&user)
        || snapshot.grants.iter().any(|grant| {
            grant.user_id == user
                && grant.origin == aruna_core::structs::storage::encryption::HolderOrigin::Explicit
        })
}

fn system_ms(time: std::time::SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|since| since.as_millis() as u64)
}

fn after(now_ms: u64, left: Option<Duration>) -> Option<u64> {
    left.map(|left| now_ms.saturating_add(left.as_millis() as u64))
}

pub(crate) fn unlock_view(status: Option<&UnlockStatus>, now_ms: u64) -> UnlockView {
    match status.filter(|status| status.active) {
        Some(status) => UnlockView {
            state: "unlocked".to_string(),
            lock_reason: None,
            locked_at_ms: None,
            session_id: Some(status.session_id.to_string()),
            unlocked_at_ms: system_ms(status.unlocked_at),
            deadline_ms: after(now_ms, status.remaining),
            max_deadline_ms: after(now_ms, status.max_remaining),
        },
        None => UnlockView {
            state: "locked".to_string(),
            lock_reason: None,
            locked_at_ms: None,
            session_id: None,
            unlocked_at_ms: None,
            deadline_ms: None,
            max_deadline_ms: None,
        },
    }
}

fn lock_reason(action: AuditAction) -> Option<&'static str> {
    match action {
        AuditAction::Lock => Some("manual"),
        AuditAction::TimedLock => Some("timed"),
        AuditAction::RestartLock => Some("restart"),
        _ => None,
    }
}

fn count(report: &HolderReport, state: HolderState) -> Option<usize> {
    Some(
        report
            .holders
            .iter()
            .filter(|holder| holder.state == state)
            .count(),
    )
}

pub(crate) fn build_status(
    bucket: String,
    snapshot: &KeySnapshot,
    report: Option<&HolderReport>,
    caller: UserId,
    now_ms: u64,
) -> EncryptionStatus {
    let settings = &snapshot.settings;
    let unlock_of = |generation: u64| {
        let status = snapshot
            .unlocks
            .iter()
            .find(|status| status.key.generation == generation);
        let mut view = unlock_view(status, now_ms);
        if let (None, Some((action, at_ms))) = (&view.session_id, snapshot.locks.get(&generation)) {
            view.lock_reason = lock_reason(*action).map(str::to_string);
            view.locked_at_ms = Some(*at_ms);
        }
        view
    };
    // A transition source stays listed until its transition settles.
    let source = snapshot
        .transition
        .as_ref()
        .filter(|transition| transition.reported_state() != TransitionState::Finished)
        .and_then(|transition| transition.source);
    let mut records: Vec<_> = snapshot
        .records
        .iter()
        .filter(|record| record.state != KeyState::Retired)
        .filter(|record| Some(record.key) == settings.active_key() || Some(record.key) == source)
        .collect();
    records.sort_by_key(|record| record.key.generation);
    let active = settings.active_key();
    let active_record = records
        .iter()
        .find(|record| Some(record.key) == active)
        .copied();
    let generations = records
        .iter()
        .map(|record| GenerationView {
            generation: record.key.generation,
            role: match Some(record.key) == active {
                true => "active",
                false => "source",
            }
            .to_string(),
            public_key: BASE64_STANDARD.encode(record.public_key),
            fingerprint: hex::encode(record.fingerprint),
            unlock: unlock_of(record.key.generation),
        })
        .collect();
    let ready_copy = active.is_some_and(|active| {
        snapshot
            .copies
            .iter()
            .any(|copy| copy.key == active && copy.user_id == caller)
    });
    EncryptionStatus {
        bucket,
        mode: settings.mode,
        bucket_id: settings.bucket_id.map(|id| id.to_string()),
        storage_generation: settings.storage_generation,
        key_generation: settings.key_generation,
        public_key: active_record.map(|record| BASE64_STANDARD.encode(record.public_key)),
        fingerprint: active_record.map(|record| hex::encode(record.fingerprint)),
        cipher: settings.cipher,
        block_keys: settings.block_keys,
        max_unlock_ms: settings.max_unlock_ms,
        unlock: active.map(|active| unlock_of(active.generation)),
        generations,
        holders: report.map(|report| HolderCounts {
            ready: count(report, HolderState::Ready),
            pending: count(report, HolderState::Pending),
            missing_key: count(report, HolderState::MissingKey),
        }),
        recovery: report.map(|report| RecoveryView::from(&report.recovery)),
        transition: snapshot.transition.as_ref().map(TransitionView::from),
        caller: CallerView {
            holder: is_holder(snapshot, caller),
            ready_copy,
            admin: snapshot.admins.contains(&caller),
        },
    }
}

pub(crate) async fn current_status(
    state: &ServerState,
    bucket: String,
    group_id: GroupId,
    caller: UserId,
) -> ServerResult<EncryptionStatus> {
    let snapshot = read_snapshot(state, &bucket, group_id).await?;
    let report = holder_report(state, &snapshot).await;
    Ok(build_status(
        bucket,
        &snapshot,
        report.as_ref(),
        caller,
        now_ms(),
    ))
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/storage/encryption",
    tag = "data/storage",
    summary = "Read a bucket's encryption status",
    description = r#"Returns how this node encrypts a bucket, its key generations and their unlock state.

**Authentication**: realm bearer token with READ on the bucket.

**Behavior**
- `mode` is `off`, `node_managed` or `vault_locked`; a plain bucket reports `off` with null keys.
- `generations` lists every key generation this node still needs; `unlock`, `public_key` and
  `fingerprint` describe the active one.
- `holders` and `recovery` come from the key directory; a failed lookup reads as unknown.
- `caller` is for display only; every key route checks the caller again."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (
            status = 200,
            description = "The bucket's encryption status",
            body = EncryptionStatus,
            example = json!({
                "bucket": "research-raw",
                "mode": "vault_locked",
                "bucket_id": "01JAMXQ7B1D7Q8E7Q2F3R8Z9KC",
                "storage_generation": 1,
                "key_generation": 1,
                "public_key": "qL3UuCZ0XkWbQZ2yZ8m1qL3UuCZ0XkWbQZ2yZ8m1qL0=",
                "fingerprint": "5d1c0a6f9e1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5",
                "cipher": "chacha20_poly1305",
                "block_keys": "content_derived",
                "max_unlock_ms": 3600000,
                "unlock": {
                    "state": "locked", "lock_reason": null, "locked_at_ms": null,
                    "session_id": null, "unlocked_at_ms": null, "deadline_ms": null,
                    "max_deadline_ms": null
                },
                "generations": [],
                "holders": { "ready": 2, "pending": 0, "missing_key": 0 },
                "recovery": { "state": "met", "ready_holders": 2, "ready_with_recovery": 1 },
                "transition": null,
                "caller": { "holder": true, "ready_copy": true, "admin": true }
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 503, description = "An authorization document of the bucket is missing", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_encryption(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<EncryptionStatus>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_bucket_read(&state, &auth, group_id, &bucket).await?;
    let status = current_status(&state, bucket, group_id, auth.user_id).await?;
    Ok(Json(status))
}

#[utoipa::path(
    put,
    path = "/data/buckets/{bucket}/storage/encryption",
    tag = "data/storage",
    summary = "Change a bucket's encryption mode",
    description = r#"Enables encryption of new writes of a bucket on this node.

**Authentication**: realm bearer token with WRITE on the owning group's admin path.

**Behavior**
- `node_managed` or `vault_locked` on a plain bucket creates key generation 1 and seals a copy of
  its private key to every holder with a published user key. The new key starts unlocked.
- `expected_generation` must equal the current `storage_generation`.
- On an encrypted bucket, another mode, `off`, cipher or block-key mode starts a transition of
  this node's stored copies; `transition` in the status reports its progress.
- `max_unlock_ms` changes the longest unlock of an encrypted bucket from the next unlock on;
  an absent field keeps it, `null` removes the limit.
- A change that needs the old key answers 409 `bucket_locked` while that key is locked.
- While stored copies still move, a further change answers 409 `transition_running`; a bucket
  set to `off` enables again once its decryption has finished."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = EncryptionRequest,
        description = "The new mode with optional cipher, block keys and unlock maximum",
        example = json!({ "mode": "vault_locked", "max_unlock_ms": 3600000, "expected_generation": 0 })
    ),
    responses(
        (status = 200, description = "The status after the change", body = EncryptionStatus, example = json!({ "bucket": "research-raw", "mode": "node_managed", "bucket_id": "01JAMXQ7B1D7Q8E7Q2F3R8Z9KC", "storage_generation": 1, "key_generation": 1, "public_key": "qL3UuCZ0XkWbQZ2yZ8m1qL3UuCZ0XkWbQZ2yZ8m1qL0=", "fingerprint": "5d1c0a6f9e1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5", "cipher": "chacha20_poly1305", "block_keys": "content_derived", "max_unlock_ms": null, "unlock": { "state": "unlocked", "lock_reason": null, "locked_at_ms": null, "session_id": "01JAMXR0C8M7T2D4WQ3V9KX6EZ", "unlocked_at_ms": 1790000000000_u64, "deadline_ms": null, "max_deadline_ms": null }, "generations": [], "holders": { "ready": 2, "pending": 0, "missing_key": 0 }, "recovery": { "state": "met", "ready_holders": 2, "ready_with_recovery": 1 }, "transition": null, "caller": { "holder": true, "ready_copy": true, "admin": true } })),
        (status = 400, description = "An invalid mode, cipher or unlock maximum", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "`stale_generation`, `open_uploads`, `recovery_unmet`, `bucket_locked`, `transition_running`, `object_too_large` when a stored object exceeds the encryption size limit, or `unchanged` when the settings already apply", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_bucket_encryption(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<EncryptionRequest>,
) -> ServerResult<Json<EncryptionStatus>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = bucket_group(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let snapshot = read_snapshot(&state, &bucket, group_id).await?;
    let current = &snapshot.settings;
    if current.storage_generation != request.expected_generation {
        return Err(key_refusal(&BucketKeyError::StaleGeneration {
            requested: request.expected_generation,
            current: current.storage_generation,
        }));
    }
    let route = put_route(
        current.is_encrypted(),
        snapshot.transition.as_ref(),
        request.mode,
    );
    if route == PutRoute::Busy {
        return Err(transition_running());
    }
    if route == PutRoute::Change {
        let (cipher, block_keys) = (
            request.cipher.unwrap_or(current.cipher),
            request.block_keys.unwrap_or(current.block_keys),
        );
        let same = (request.mode, cipher, block_keys)
            == (current.mode, current.cipher, current.block_keys);
        let expected = request.expected_generation;
        if !same {
            let change = KeyChange::Settings {
                mode: request.mode,
                cipher,
                block_keys,
            };
            let target = (group_id, auth.user_id);
            change_bucket(
                &state,
                &bucket,
                target,
                &snapshot,
                change,
                (expected, request.max_unlock_ms),
            )
            .await?;
        } else if let Some(max) = request
            .max_unlock_ms
            .filter(|max| *max != current.max_unlock_ms)
        {
            set_unlock_limit(&state, &bucket, group_id, auth.user_id, max, expected).await?;
        }
        let status = current_status(&state, bucket, group_id, auth.user_id).await?;
        return Ok(Json(status));
    }
    if route == PutRoute::Keep {
        let status = current_status(&state, bucket, group_id, auth.user_id).await?;
        return Ok(Json(status));
    }
    let context = state.get_ctx();
    let target = (state.get_realm_id(), state.get_node_id(), group_id);
    enable_bucket(&context, target, auth.user_id, &bucket, &snapshot, &request)
        .await
        .map_err(enable_refusal)?;
    let status = current_status(&state, bucket, group_id, auth.user_id).await?;
    Ok(Json(status))
}

/// What a PUT does with a bucket in its current state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutRoute {
    Change,
    Enable,
    Keep,
    Busy,
}

/// An encrypted bucket changes through a transition. A plain or settled `off` bucket enables
/// again, but not while a decryption still needs its source key.
fn put_route(
    encrypted: bool,
    transition: Option<&EncryptionTransition>,
    mode: EncryptionMode,
) -> PutRoute {
    if encrypted {
        return PutRoute::Change;
    }
    if mode == EncryptionMode::Off {
        return PutRoute::Keep;
    }
    match transition.is_some_and(|transition| transition.finished_at_ms.is_none()) {
        true => PutRoute::Busy,
        false => PutRoute::Enable,
    }
}

fn transition_running() -> ServerError {
    refused(
        StatusCode::CONFLICT,
        "transition_running",
        "the bucket's stored copies are still moving to a new encryption",
    )
}

/// Stores a new unlock maximum of an encrypted bucket; the next unlock uses it.
async fn set_unlock_limit(
    state: &ServerState,
    bucket: &str,
    group_id: GroupId,
    caller: UserId,
    max_unlock_ms: Option<u64>,
    expected_generation: u64,
) -> ServerResult<()> {
    let input = UnlockLimitInput {
        bucket: bucket.to_string(),
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        caller,
        max_unlock_ms,
        expected_generation,
        now_ms: now_ms(),
    };
    drive(UnlockLimitOperation::new(input), &state.get_ctx())
        .await
        .map(|_| ())
        .map_err(|error| match error {
            UnlockLimitError::Settings(error) => settings_refusal(error),
            UnlockLimitError::Key(error) => key_refusal(&error),
            UnlockLimitError::NotEncrypted => not_encrypted(),
            UnlockLimitError::NotAdmin => ServerError::Forbidden,
            other => ServerError::InternalError(other.to_string()),
        })
}

/// Applies a mode change or rotation, then installs a new key generation if it made one.
pub(crate) async fn change_bucket(
    state: &ServerState,
    bucket: &str,
    (group_id, caller): (GroupId, UserId),
    snapshot: &KeySnapshot,
    change: KeyChange,
    (expected_generation, max_unlock_ms): (u64, Option<Option<u64>>),
) -> ServerResult<()> {
    let creator = snapshot.info.as_ref().map(|info| info.created_by);
    let users = creator
        .into_iter()
        .chain(snapshot.admins.iter().copied())
        .chain(snapshot.grants.iter().map(|grant| grant.user_id))
        .collect::<Vec<_>>();
    let context = state.get_ctx();
    let lookups = lookup_keys(&context, state.get_node_id(), users).await;
    let input = ChangeInput {
        bucket: bucket.to_string(),
        group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        caller,
        change,
        max_unlock_ms,
        expected_generation,
        lookups,
        now_ms: now_ms(),
    };
    let changed = drive(ChangeEncryptionOperation::new(input), &context)
        .await
        .map_err(change_refusal)?;
    if let Some((record, private_key)) = changed.key {
        let install = InstallInput {
            key: record.key,
            public_key: record.public_key,
            private_key,
            duration: None,
            max: changed.settings.max_unlock_ms.map(Duration::from_millis),
        };
        // The change is committed either way; a failed install only leaves the key locked.
        let node_id = state.get_node_id();
        let install = InstallKeyOperation::new(install).audited(node_id, Some(caller), now_ms());
        if let Err(error) = drive(install, &context).await {
            tracing::warn!(%bucket, ?error, "new bucket key stays locked");
        }
    }
    Ok(())
}

fn change_refusal(error: ChangeError) -> ServerError {
    match error {
        ChangeError::Settings(error) => settings_refusal(error),
        ChangeError::Key(error) => key_refusal(&error),
        ChangeError::Blob(error) => blob_refusal(error),
        ChangeError::NotEncrypted => not_encrypted(),
        ChangeError::NotAdmin => ServerError::Forbidden,
        ChangeError::Unchanged => refused(
            StatusCode::CONFLICT,
            "unchanged",
            "the bucket already uses these settings",
        ),
        ChangeError::OpenUploads => refused(
            StatusCode::CONFLICT,
            "open_uploads",
            "the bucket has open multipart uploads",
        ),
        ChangeError::RecoveryUnmet => refused(
            StatusCode::CONFLICT,
            "recovery_unmet",
            "the key holders do not meet the recovery rule",
        ),
        ChangeError::TransitionRunning => transition_running(),
        other => ServerError::InternalError(other.to_string()),
    }
}

/// Creates key generation 1 sealed to the creator and admins, then installs the new key.
/// `caller` must still be a group admin inside the enabling transaction.
pub(crate) async fn enable_bucket(
    context: &DriverContext,
    (realm_id, node_id, group_id): (RealmId, NodeId, GroupId),
    caller: UserId,
    bucket: &str,
    snapshot: &KeySnapshot,
    request: &EncryptionRequest,
) -> Result<(), EnableError> {
    let creator = snapshot.info.as_ref().map(|info| info.created_by);
    let users = creator
        .into_iter()
        .chain(snapshot.admins.iter().copied())
        .collect::<Vec<_>>();
    let lookups = lookup_keys(context, node_id, users).await;
    let input = EnableInput {
        bucket: bucket.to_string(),
        group_id,
        realm_id,
        node_id,
        caller,
        mode: request.mode,
        cipher: request.cipher.unwrap_or_default(),
        block_keys: request.block_keys.unwrap_or_default(),
        max_unlock_ms: request.max_unlock_ms.flatten(),
        expected_generation: request.expected_generation,
        lookups,
        now_ms: now_ms(),
    };
    let enabled = drive(EnableEncryptionOperation::new(input), context).await?;
    let install = InstallInput {
        key: enabled.key.key,
        public_key: enabled.key.public_key,
        private_key: enabled.private_key,
        duration: None,
        max: enabled.settings.max_unlock_ms.map(Duration::from_millis),
    };
    // The bucket is encrypted either way; a failed install only leaves it locked.
    let install = InstallKeyOperation::new(install).audited(node_id, Some(caller), now_ms());
    if let Err(error) = drive(install, context).await {
        tracing::warn!(%bucket, ?error, "new bucket key stays locked");
    }
    Ok(())
}

pub(crate) fn enable_refusal(error: EnableError) -> ServerError {
    match error {
        EnableError::Settings(error) => settings_refusal(error),
        EnableError::Key(error) => key_refusal(&error),
        EnableError::Blob(error) => blob_refusal(error),
        EnableError::NotAdmin => ServerError::Forbidden,
        EnableError::TransitionRunning => transition_running(),
        EnableError::AlreadyEncrypted => refused(
            StatusCode::CONFLICT,
            "stale_generation",
            "the bucket already encrypts its writes",
        ),
        EnableError::InvalidMode | EnableError::Conversion(_) => {
            ServerError::BadRequestReason(error.to_string())
        }
        EnableError::OpenUploads => refused(
            StatusCode::CONFLICT,
            "open_uploads",
            "the bucket has open multipart uploads",
        ),
        EnableError::RecoveryUnmet => refused(
            StatusCode::CONFLICT,
            "recovery_unmet",
            "the key holders do not meet the recovery rule",
        ),
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyRecord, BucketKeyRef,
    };
    use std::time::SystemTime;
    use ulid::Ulid;

    const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn snapshot() -> KeySnapshot {
        let record = |generation, state| BucketKeyRecord {
            state,
            ..BucketKeyRecord::new(
                BucketKeyRef::new(BUCKET_ID, generation),
                BUCKET_ID,
                [7; 32],
                5,
            )
        };
        KeySnapshot {
            settings: BucketEncryption {
                mode: EncryptionMode::VaultLocked,
                bucket_id: Some(BUCKET_ID),
                key_generation: 2,
                storage_generation: 3,
                ..Default::default()
            },
            records: vec![
                record(2, KeyState::Active),
                record(1, KeyState::Retiring),
                record(0, KeyState::Retired),
            ],
            locks: [
                (2, (AuditAction::RestartLock, 40)),
                (1, (AuditAction::Lock, 30)),
            ]
            .into(),
            unlocks: vec![UnlockStatus {
                key: BucketKeyRef::new(BUCKET_ID, 1),
                session_id: Ulid::from_bytes([9; 16]),
                sequence: ulid::Ulid::from_parts(1, 1),
                deadline_ms: Some(60),
                active: true,
                unlocked_at: SystemTime::UNIX_EPOCH + Duration::from_millis(50),
                remaining: Some(Duration::from_millis(10)),
                max_remaining: None,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn reports_retained_generations() {
        let alone = build_status("bucket".to_string(), &snapshot(), None, user(1), 100);
        assert_eq!(alone.generations.len(), 1);
        let mut moving = snapshot();
        moving.transition = Some(EncryptionTransition::new(
            aruna_core::structs::storage::transition::TransitionKind::Rotate,
            Some(BucketKeyRef::new(BUCKET_ID, 1)),
            aruna_core::structs::storage::transition::TransitionTarget {
                compression: Default::default(),
                plan: None,
            },
            3,
            9,
        ));
        let status = build_status("bucket".to_string(), &moving, None, user(1), 100);
        let roles: Vec<_> = status
            .generations
            .iter()
            .map(|generation| (generation.generation, generation.role.as_str()))
            .collect();
        assert_eq!(roles, [(1, "source"), (2, "active")]);
        let active = status.unlock.as_ref().unwrap();
        assert_eq!(active.state, "locked");
        assert_eq!(active.lock_reason.as_deref(), Some("restart"));
        assert_eq!(active.locked_at_ms, Some(40));
        let source = &status.generations[0].unlock;
        assert_eq!(source.state, "unlocked");
        assert_eq!(source.deadline_ms, Some(110));
        assert_eq!(source.unlocked_at_ms, Some(50));
        assert_eq!(status.fingerprint.as_deref().map(str::len), Some(64));
        assert!(status.holders.is_none());
        assert!(!status.caller.holder);
    }

    #[test]
    fn wire_names_match() {
        let status = build_status("bucket".to_string(), &snapshot(), None, user(1), 100);
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["mode"], "vault_locked");
        assert_eq!(json["cipher"], "chacha20_poly1305");
        assert_eq!(json["block_keys"], "content_derived");
        assert!(json["transition"].is_null());
        let mut moving = snapshot();
        let mut transition = EncryptionTransition::new(
            aruna_core::structs::storage::transition::TransitionKind::Rotate,
            Some(BucketKeyRef::new(BUCKET_ID, 1)),
            aruna_core::structs::storage::transition::TransitionTarget {
                compression: Default::default(),
                plan: None,
            },
            3,
            9,
        );
        transition.remaining = 2;
        moving.transition = Some(transition);
        let status = build_status("bucket".to_string(), &moving, None, user(1), 100);
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["transition"]["kind"], "rotate");
        assert_eq!(json["transition"]["state"], "running");
        assert_eq!(json["transition"]["source_generation"], 1);
        assert_eq!(json["transition"]["remaining"], 2);
        assert_eq!(json["unlock"]["lock_reason"], "restart");
        let plain = build_status("b".to_string(), &KeySnapshot::default(), None, user(1), 0);
        let json = serde_json::to_value(&plain).unwrap();
        assert_eq!(json["mode"], "off");
        assert!(json["unlock"].is_null() && json["public_key"].is_null());
        assert_eq!(json["generations"], serde_json::json!([]));
    }

    #[test]
    fn unlock_maximum_presence() {
        let parse = |body: &str| {
            serde_json::from_str::<EncryptionRequest>(body)
                .unwrap()
                .max_unlock_ms
        };
        let base = r#""mode":"vault_locked","expected_generation":1"#;
        assert_eq!(parse(&format!("{{{base}}}")), None);
        assert_eq!(
            parse(&format!(r#"{{{base},"max_unlock_ms":null}}"#)),
            Some(None)
        );
        let limited = parse(&format!(r#"{{{base},"max_unlock_ms":60000}}"#));
        assert_eq!(limited, Some(Some(60_000)));
    }

    #[test]
    fn maps_change_codes() {
        let code = |error| change_refusal(error).response_body().code.unwrap();
        assert_eq!(code(ChangeError::OpenUploads), "open_uploads");
        assert_eq!(code(ChangeError::RecoveryUnmet), "recovery_unmet");
        assert_eq!(code(ChangeError::Unchanged), "unchanged");
        let stale = BucketKeyError::StaleGeneration {
            requested: 1,
            current: 2,
        };
        assert_eq!(code(ChangeError::Key(stale)), "stale_generation");
        let locked = ChangeError::Key(BucketKeyError::Locked(BUCKET_ID));
        assert_eq!(code(locked), "bucket_locked");
    }

    #[test]
    fn put_round_trips() {
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::transition::{TransitionKind, TransitionTarget};
        let (locked, off) = (EncryptionMode::VaultLocked, EncryptionMode::Off);
        // Encrypted to off and back: a settled decryption enables again.
        assert_eq!(put_route(true, None, off), PutRoute::Change);
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let mut decrypt = EncryptionTransition::new(TransitionKind::Decrypt, None, target, 2, 1);
        assert_eq!(put_route(false, Some(&decrypt), locked), PutRoute::Busy);
        decrypt.finished_at_ms = Some(5);
        assert_eq!(put_route(false, Some(&decrypt), locked), PutRoute::Enable);
        assert_eq!(
            put_route(false, None, EncryptionMode::NodeManaged),
            PutRoute::Enable
        );
        assert_eq!(put_route(false, Some(&decrypt), off), PutRoute::Keep);
        let code = change_refusal(ChangeError::TransitionRunning)
            .response_body()
            .code;
        assert_eq!(code.as_deref(), Some("transition_running"));
        let code = enable_refusal(EnableError::TransitionRunning)
            .response_body()
            .code;
        assert_eq!(code.as_deref(), Some("transition_running"));
    }

    #[test]
    fn oversized_object_conflicts() {
        let limit = 1u64 << 40;
        let error = EnableError::Blob(BlobError::SizeLimitExceeded { limit });
        let refusal = enable_refusal(error);
        assert_eq!(refusal.status_code(), StatusCode::CONFLICT);
        let body = refusal.response_body();
        assert_eq!(body.code.as_deref(), Some("object_too_large"));
        assert!(body.error.contains("1099511627776 byte (1.00 TiB)"));
    }

    #[test]
    fn maps_key_codes() {
        let code = |error: BucketKeyError| {
            let refusal = key_refusal(&error);
            (refusal.status_code(), refusal.response_body().code.unwrap())
        };
        assert_eq!(
            code(BucketKeyError::Locked(BUCKET_ID)),
            (StatusCode::CONFLICT, "bucket_locked".to_string())
        );
        assert_eq!(
            code(BucketKeyError::WrongKey),
            (StatusCode::BAD_REQUEST, "wrong_key".to_string())
        );
        assert_eq!(
            code(BucketKeyError::InvalidDuration),
            (StatusCode::BAD_REQUEST, "invalid_duration".to_string())
        );
        assert_eq!(
            code(BucketKeyError::SessionMismatch).1,
            "session_mismatch".to_string()
        );
        assert_eq!(code(BucketKeyError::Capacity).1, "unlock_capacity");
    }
}
