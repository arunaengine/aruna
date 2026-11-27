//! REST administration of the embedded NFS server: host enrollment with proof of possession,
//! mount bindings of each host, and bucket-admin recovery of retained stages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::UnrestrictedAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_clock::unix_timestamp_millis;
use aruna_core::bucket_read::GetBucketOperation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::Hasher;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::vfs::{
    DetachReason, EnrolledHostRecord, NfsMountBinding, NfsMountTarget, NfsStageRecord,
    StageDisposition,
};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::VfsProtocol;
use aruna_ops_data::replication::live_queue::{CompletePutInput, CompletePutOperation};
use aruna_ops_data::s3::access::get::{GetAccessError, GetAccessOperation};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput};
use aruna_ops_data::vfs::admin::{
    AdminPage, EnrollInput, HostChange, MountInput, PublishStageInput, StageAdmin,
};
use aruna_ops_data::vfs::{
    BucketScopeOperation, CHUNK_BYTES, CreateMountOperation, DeleteMountOperation,
    DeleteStageOperation, EnrollHostOperation, GetHostOperation, GetMountOperation,
    GetStageOperation, ListHostsOperation, ListMountsOperation, ListStagesOperation,
    PublishStageOperation, ReadCheckpointOperation, RevokeHostOperation, RotateHostOperation,
    STATE_PAGE, VfsAccessContext, VfsError,
};
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_ops_work::jobs::workflow::input_view::{view_bucket, view_entries, view_job};
use aruna_runner::driver::{drive, drive_retry, node_routing};
use axum::extract::{Path, Query, State};
use axum::{Json, http::StatusCode};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use ed25519_dalek::pkcs8::DecodePublicKey;
use ed25519_dalek::{Signature, VerifyingKey};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, SubjectPublicKeyInfoDer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

/// The prefix of the challenge a host key signs; the caller and the key's fingerprint follow.
const ENROLL_CHALLENGE: &str = "aruna-nfs-host-enroll-v1";

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(enroll_host, list_hosts))
        .routes(routes!(get_host, rotate_host, revoke_host))
        .routes(routes!(create_mount, list_mounts))
        .routes(routes!(get_mount, delete_mount))
        .routes(routes!(list_stages))
        .routes(routes!(publish_stage))
        .routes(routes!(delete_stage))
}

/// A presented host key and its signature over the enrollment challenge.
#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
pub struct KeyProof {
    /// The Ed25519 public key as an SPKI PEM block; send this or `certificate_pem`.
    pub public_key_pem: Option<String>,
    /// The client certificate PEM whose Ed25519 key signed the challenge.
    pub certificate_pem: Option<String>,
    /// Base64 Ed25519 signature over `aruna-nfs-host-enroll-v1:<user_id>:<fingerprint hex>`.
    pub proof: String,
}

#[derive(Deserialize, ToSchema)]
pub struct HostEnrollRequest {
    /// Hex SHA-256 digests of the accepted client certificate SPKIs.
    pub fingerprints: Vec<String>,
    #[serde(flatten)]
    pub key: KeyProof,
    /// Source networks in CIDR notation; empty admits no network.
    #[serde(default)]
    pub allowed_networks: Vec<String>,
    #[serde(default)]
    pub worker_pool: bool,
}

#[derive(Deserialize, ToSchema)]
pub struct HostRotateRequest {
    /// The new hex SHA-256 SPKI digests; they replace the old ones.
    pub fingerprints: Vec<String>,
    #[serde(flatten)]
    pub key: KeyProof,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HostView {
    pub host_id: String,
    pub fingerprints: Vec<String>,
    pub allowed_networks: Vec<String>,
    pub worker_pool: bool,
    pub created_at_ms: u64,
    pub revoked_at_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct HostPage {
    pub items: Vec<HostView>,
    pub next_cursor: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub struct MountCreateRequest {
    pub host_id: String,
    /// The caller's S3 access key id whose live grants admit every request of the mount.
    pub access_key: String,
    pub group_id: String,
    /// A local ordinary bucket of this node; omit when binding a job input view.
    pub bucket: Option<String>,
    /// A folder prefix inside the bucket; absent exports the whole bucket.
    pub prefix: Option<String>,
    /// A nonterminal job id to export as a read-only input view; mutually exclusive with `bucket`.
    pub job_id: Option<String>,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub allowed_networks: Vec<String>,
    pub display_uid: u32,
    pub display_gid: u32,
    /// Permission bits shown to the client, at most `0o7777`.
    pub mode: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MountView {
    pub mount_id: String,
    pub host_id: String,
    pub access_key: String,
    pub group_id: String,
    /// `bucket`, `folder` or `job_inputs`.
    pub kind: String,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub job_id: Option<String>,
    pub read_only: bool,
    pub allowed_networks: Vec<String>,
    pub display_uid: u32,
    pub display_gid: u32,
    pub mode: u32,
    pub created_at_ms: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct MountPage {
    pub items: Vec<MountView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StageView {
    pub stage_id: String,
    pub key: String,
    /// The checkpoint generation; publish and delete name it as their fence.
    pub generation: u64,
    pub logical_size: u64,
    pub owner: String,
    /// Why the stage left its name: `unlinked`, `displaced` or `lost_life`.
    pub reason: Option<String>,
    /// The life that took the name after the stage was detached.
    pub foreign_life: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct StagePage {
    pub items: Vec<StageView>,
    pub next_cursor: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub struct StagePublishRequest {
    /// The bucket admin's S3 access key id; it must belong to the caller.
    pub access_key: String,
    /// The generation the admin saw; another one refuses the publication.
    pub generation: u64,
    /// The head version the admin saw at the destination; absent expects no live head.
    pub expected_head: Option<String>,
    /// The object life the admin saw at the destination, when fencing against a newer life.
    pub expected_life: Option<String>,
    /// A separate recovery key, so no newer life at the stage's own name is overwritten.
    pub destination_key: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct StagePublished {
    pub version_id: String,
    pub key: String,
}

#[derive(Deserialize)]
pub struct PageQuery {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub host_id: Option<String>,
}

#[derive(Deserialize)]
pub struct StageQuery {
    pub access_key: String,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct StageDelete {
    pub access_key: String,
    pub generation: u64,
}

/// The REST answer to a file server refusal; storage faults stay unavailable.
pub(crate) fn nfs_refusal(error: VfsError) -> ServerError {
    let message = error.to_string();
    let (status, code) = match &error {
        VfsError::InvalidName => (StatusCode::BAD_REQUEST, "invalid_name"),
        VfsError::BadCookie => (StatusCode::BAD_REQUEST, "bad_cookie"),
        VfsError::Denied => return ServerError::Forbidden,
        VfsError::VaultLocked => (StatusCode::FORBIDDEN, "vault_locked"),
        VfsError::KeyRequired => (StatusCode::FORBIDDEN, "object_key_required"),
        VfsError::Quarantined => (StatusCode::FORBIDDEN, "quarantined"),
        VfsError::ReadOnly => (StatusCode::FORBIDDEN, "read_only"),
        VfsError::NotFound | VfsError::StaleHandle | VfsError::ExpiredHandle => {
            return ServerError::NotFound;
        }
        VfsError::Exists => (StatusCode::CONFLICT, "exists"),
        VfsError::Conflict => (StatusCode::CONFLICT, "conflict"),
        VfsError::NotEmpty => (StatusCode::CONFLICT, "not_empty"),
        VfsError::IsDirectory => (StatusCode::CONFLICT, "is_directory"),
        VfsError::NotDirectory => (StatusCode::CONFLICT, "not_directory"),
        VfsError::ShareViolation => (StatusCode::CONFLICT, "share_violation"),
        VfsError::LockConflict => (StatusCode::CONFLICT, "lock_conflict"),
        VfsError::DeletePending => (StatusCode::CONFLICT, "delete_pending"),
        VfsError::QuotaExceeded => (StatusCode::PAYLOAD_TOO_LARGE, "quota_exceeded"),
        VfsError::NoSpace => (StatusCode::INSUFFICIENT_STORAGE, "no_space"),
        VfsError::Distributed => (StatusCode::UNPROCESSABLE_ENTITY, "distributed_bucket"),
        VfsError::CrossExport => (StatusCode::UNPROCESSABLE_ENTITY, "cross_export"),
        VfsError::CorruptStage => return ServerError::InternalError(message.into()),
        VfsError::Unavailable(_) | VfsError::Storage(_) | VfsError::Vfs(_) => {
            return ServerError::ServiceUnavailable;
        }
    };
    ServerError::Refused(status, code, message)
}

fn parse_id(text: &str) -> ServerResult<Ulid> {
    Ulid::from_string(text).map_err(|_| ServerError::BadRequestReason(format!("{text:?} is no id")))
}

fn parse_cursor(cursor: Option<&str>) -> ServerResult<Option<Ulid>> {
    cursor.map(parse_id).transpose()
}

fn parse_fingerprints(fingerprints: &[String]) -> ServerResult<Vec<[u8; 32]>> {
    let parse = |text: &String| {
        let bytes = hex::decode(text.trim()).ok()?;
        <[u8; 32]>::try_from(bytes).ok()
    };
    let parsed = fingerprints.iter().map(parse).collect::<Option<Vec<_>>>();
    match parsed {
        Some(parsed) if !parsed.is_empty() => Ok(parsed),
        _ => Err(ServerError::BadRequestReason(
            "fingerprints must be one or more hex SHA-256 digests".to_string(),
        )),
    }
}

/// The challenge a host key signs to prove possession for `user`.
pub(crate) fn enroll_challenge(user: &UserId, fingerprint: &[u8; 32]) -> String {
    format!("{ENROLL_CHALLENGE}:{user}:{}", hex::encode(fingerprint))
}

/// The DER SPKI of the presented public key or certificate.
fn presented_key(key: &KeyProof) -> ServerResult<Vec<u8>> {
    let refused = |reason: &str| ServerError::BadRequestReason(reason.to_string());
    match (&key.public_key_pem, &key.certificate_pem) {
        (Some(pem), None) => SubjectPublicKeyInfoDer::from_pem_slice(pem.as_bytes())
            .map(|spki| spki.as_ref().to_vec())
            .map_err(|_| refused("public_key_pem is no PEM public key")),
        (None, Some(pem)) => {
            let der = CertificateDer::from_pem_slice(pem.as_bytes())
                .map_err(|_| refused("certificate_pem is no PEM certificate"))?;
            let certificate = webpki::EndEntityCert::try_from(&der)
                .map_err(|_| refused("certificate_pem is no valid certificate"))?;
            Ok(certificate.subject_public_key_info().as_ref().to_vec())
        }
        _ => Err(refused(
            "send exactly one of public_key_pem and certificate_pem",
        )),
    }
}

/// Verifies the signed challenge and returns the fingerprint of the key that signed it.
pub(crate) fn proven_key(user: &UserId, key: &KeyProof) -> ServerResult<[u8; 32]> {
    let spki = presented_key(key)?;
    let fingerprint = Hasher::new_with_bytes(&spki).finalize().sha256;
    let verifying = VerifyingKey::from_public_key_der(&spki)
        .map_err(|_| ServerError::BadRequestReason("the key is no Ed25519 key".to_string()))?;
    let signature = STANDARD
        .decode(key.proof.trim())
        .ok()
        .and_then(|bytes| Signature::from_slice(&bytes).ok())
        .ok_or_else(|| {
            ServerError::BadRequestReason("proof is no base64 Ed25519 signature".to_string())
        })?;
    let challenge = enroll_challenge(user, &fingerprint);
    verifying
        .verify_strict(challenge.as_bytes(), &signature)
        .map_err(|_| ServerError::Forbidden)?;
    Ok(fingerprint)
}

fn host_view(host: EnrolledHostRecord) -> HostView {
    HostView {
        host_id: host.host_id.to_string(),
        fingerprints: host.fingerprints.iter().map(hex::encode).collect(),
        allowed_networks: host.allowed_networks,
        worker_pool: host.worker_pool,
        created_at_ms: host.created_at_ms,
        revoked_at_ms: host.revoked_at_ms,
    }
}

fn mount_view(mount: NfsMountBinding) -> MountView {
    let (kind, bucket, prefix, job_id) = match mount.target {
        NfsMountTarget::Bucket { bucket, .. } => ("bucket", Some(bucket), None, None),
        NfsMountTarget::Folder { bucket, prefix, .. } => {
            ("folder", Some(bucket), Some(prefix), None)
        }
        NfsMountTarget::JobInputView { job_id } => {
            ("job_inputs", None, None, Some(job_id.to_string()))
        }
    };
    MountView {
        mount_id: mount.mount_id.to_string(),
        host_id: mount.host_id.to_string(),
        access_key: mount.access_key,
        group_id: mount.group_id.to_string(),
        kind: kind.to_string(),
        bucket,
        prefix,
        job_id,
        read_only: mount.read_only,
        allowed_networks: mount.allowed_networks,
        display_uid: mount.display_uid,
        display_gid: mount.display_gid,
        mode: mount.mode,
        created_at_ms: mount.created_at_ms,
    }
}

fn stage_view(stage: NfsStageRecord) -> StageView {
    let reason = match stage.disposition {
        StageDisposition::Retained { reason } => Some(match reason {
            DetachReason::Unlinked => "unlinked",
            DetachReason::Displaced => "displaced",
            DetachReason::LostLife => "lost_life",
        }),
        StageDisposition::Live | StageDisposition::DeletePending => None,
    };
    StageView {
        stage_id: stage.stage_id.to_string(),
        key: stage.key,
        generation: stage.generation,
        logical_size: stage.logical_size,
        owner: stage.owner.user.to_string(),
        reason: reason.map(str::to_string),
        foreign_life: stage.foreign_life.map(|life| life.to_string()),
        updated_at_ms: stage.updated_at_ms,
    }
}

fn page_of<T, V>(page: AdminPage<T>, view: fn(T) -> V) -> (Vec<V>, Option<String>) {
    let items = page.items.into_iter().map(view).collect();
    (items, page.next.map(|next| next.to_string()))
}

/// The bucket export of `bucket` at its live incarnation, admitted through `access_key`.
async fn access_context(
    state: &ServerState,
    bucket: &str,
    access_key: String,
) -> ServerResult<VfsAccessContext> {
    let scope = drive(BucketScopeOperation::new(bucket), &state.get_ctx())
        .await
        .map_err(nfs_refusal)?;
    let target = NfsMountTarget::for_bucket(&scope.bucket, scope.incarnation)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    Ok(VfsAccessContext {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        access_key,
        protocol: VfsProtocol::Nfs,
        target,
        read_only: false,
        now_ms: unix_timestamp_millis(),
    })
}

/// A stage admin acting through one of the caller's own access keys.
async fn stage_admin(
    state: &ServerState,
    auth: &AuthContext,
    bucket: &str,
    access_key: String,
) -> ServerResult<StageAdmin> {
    let read = GetAccessOperation::new(access_key.clone());
    match drive(read, &state.get_ctx()).await {
        Ok(access) if access.user_identity == auth.user_id => {}
        Ok(_) | Err(GetAccessError::NotFound) => return Err(ServerError::Forbidden),
        Err(_) => return Err(ServerError::ServiceUnavailable),
    }
    let context = access_context(state, bucket, access_key).await?;
    Ok(StageAdmin {
        context,
        bucket: bucket.to_string(),
    })
}

fn retry_seed() -> [u8; 16] {
    Ulid::generate().to_bytes()
}

#[utoipa::path(post, path = "/data/nfs/hosts", tag = "data/nfs",
    summary = "Enroll an NFS client host",
    description = r#"Enrolls a host whose client certificate keys may connect to the embedded NFS server.

**Authentication**: any caller without path restrictions; the caller owns the host.

**Behavior**
- `proof` is a base64 Ed25519 signature over `aruna-nfs-host-enroll-v1:<user_id>:<fingerprint>`
  by the key in `public_key_pem` or `certificate_pem`; its fingerprint must be listed.
- Fingerprints are hex SHA-256 digests of the DER SubjectPublicKeyInfo.
- Each connection's source address must lie in `allowed_networks` (CIDR)."#,
    request_body(content = HostEnrollRequest, example = json!({
        "fingerprints": ["9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"],
        "public_key_pem": "<PEM-encoded Ed25519 SubjectPublicKeyInfo>",
        "proof": "c2lnbmF0dXJl", "allowed_networks": ["10.0.0.0/8"], "worker_pool": false})),
    responses((status = 201, body = HostView, description = "The enrolled host", example = json!({
            "host_id": "01JABCDEF0123456789ABCDEFG",
            "fingerprints": ["9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"],
            "allowed_networks": ["10.0.0.0/8"], "worker_pool": false,
            "created_at_ms": 1764000000000u64, "revoked_at_ms": null})),
        (status = 400, body = ErrorResponse, description = "Malformed key, fingerprint, proof or network"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "The proof does not verify or names no listed key"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn enroll_host(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Json(request): Json<HostEnrollRequest>,
) -> ServerResult<(StatusCode, Json<HostView>)> {
    let fingerprints = parse_fingerprints(&request.fingerprints)?;
    let proven = proven_key(&auth.user_id, &request.key)?;
    let input = EnrollInput {
        owner: auth.user_id,
        fingerprints,
        proven,
        allowed_networks: request.allowed_networks,
        worker_pool: request.worker_pool,
    };
    let build = || EnrollHostOperation::new(input.clone());
    let host = drive_retry(build, &state.get_ctx(), &retry_seed())
        .await
        .map_err(nfs_refusal)?;
    Ok((StatusCode::CREATED, Json(host_view(host))))
}

#[utoipa::path(get, path = "/data/nfs/hosts", tag = "data/nfs",
    summary = "List enrolled NFS hosts",
    description = r#"Lists the caller's enrolled hosts, revoked ones included, a page at a time.

**Authentication**: any caller without path restrictions; only own hosts are listed.

**Behavior**
- Pass `next_cursor` as `cursor` for the next page; `limit` is bounded by the server page size."#,
    params(("cursor" = Option<String>, Query, description = "next_cursor of the previous page"),
        ("limit" = Option<usize>, Query, description = "Hosts per page")),
    responses((status = 200, body = HostPage, description = "One page of hosts", example = json!({
            "items": [{"host_id": "01JABCDEF0123456789ABCDEFG",
                "fingerprints": ["9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"],
                "allowed_networks": ["10.0.0.0/8"], "worker_pool": false,
                "created_at_ms": 1764000000000u64, "revoked_at_ms": null}],
            "next_cursor": null})),
        (status = 400, body = ErrorResponse, description = "Malformed cursor"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn list_hosts(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Query(query): Query<PageQuery>,
) -> ServerResult<Json<HostPage>> {
    let after = parse_cursor(query.cursor.as_deref())?;
    let limit = query.limit.unwrap_or(STATE_PAGE);
    let list = ListHostsOperation::new(auth.user_id, None, after, limit);
    let page = drive(list, &state.get_ctx()).await.map_err(nfs_refusal)?;
    let (items, next_cursor) = page_of(page, host_view);
    Ok(Json(HostPage { items, next_cursor }))
}

#[utoipa::path(get, path = "/data/nfs/hosts/{host_id}", tag = "data/nfs",
    summary = "Read an enrolled NFS host",
    description = r#"Returns one host the caller enrolled.

**Authentication**: any caller without path restrictions; another owner's host answers 404."#,
    params(("host_id" = String, Path, description = "Host id")),
    responses((status = 200, body = HostView, description = "The host", example = json!({
            "host_id": "01JABCDEF0123456789ABCDEFG",
            "fingerprints": ["9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"],
            "allowed_networks": ["10.0.0.0/8"], "worker_pool": false,
            "created_at_ms": 1764000000000u64, "revoked_at_ms": null})),
        (status = 400, body = ErrorResponse, description = "Malformed host id"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 404, body = ErrorResponse, description = "No such host of the caller"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn get_host(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(host_id): Path<String>,
) -> ServerResult<Json<HostView>> {
    let read = GetHostOperation::new(parse_id(&host_id)?, auth.user_id);
    let host = drive(read, &state.get_ctx()).await.map_err(nfs_refusal)?;
    Ok(Json(host_view(host)))
}

#[utoipa::path(put, path = "/data/nfs/hosts/{host_id}", tag = "data/nfs",
    summary = "Rotate an NFS host's keys",
    description = r#"Replaces the accepted certificate keys of a live host.

**Authentication**: the owner who enrolled the host, without path restrictions.

**Behavior**
- `proof` signs the enrollment challenge with one of the new keys.
- Every established connection of the host is closed; clients reconnect with a new key."#,
    params(("host_id" = String, Path, description = "Host id")),
    request_body(content = HostRotateRequest, example = json!({
        "fingerprints": ["9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"],
        "certificate_pem": "<PEM-encoded X.509 certificate>",
        "proof": "c2lnbmF0dXJl"})),
    responses((status = 200, body = HostView, description = "The rotated host", example = json!({
            "host_id": "01JABCDEF0123456789ABCDEFG",
            "fingerprints": ["9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"],
            "allowed_networks": ["10.0.0.0/8"], "worker_pool": false,
            "created_at_ms": 1764000000000u64, "revoked_at_ms": null})),
        (status = 400, body = ErrorResponse, description = "Malformed id, key, fingerprint or proof"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "Another owner's host, or the proof does not verify"),
        (status = 404, body = ErrorResponse, description = "No such live host"),
        (status = 409, body = ErrorResponse, description = "A concurrent change conflicted"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn rotate_host(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(host_id): Path<String>,
    Json(request): Json<HostRotateRequest>,
) -> ServerResult<Json<HostView>> {
    let host_id = parse_id(&host_id)?;
    let fingerprints = parse_fingerprints(&request.fingerprints)?;
    let proven = proven_key(&auth.user_id, &request.key)?;
    let change = HostChange::Rotate {
        fingerprints,
        proven,
    };
    let build = || RotateHostOperation::new(host_id, auth.user_id, change.clone());
    let host = drive_retry(build, &state.get_ctx(), &retry_seed())
        .await
        .map_err(nfs_refusal)?;
    Ok(Json(host_view(host)))
}

#[utoipa::path(delete, path = "/data/nfs/hosts/{host_id}", tag = "data/nfs",
    summary = "Revoke an NFS host",
    description = r#"Revokes a live host and closes every connection and grant it holds.

**Authentication**: the owner who enrolled the host, without path restrictions."#,
    params(("host_id" = String, Path, description = "Host id")),
    responses((status = 204, description = "The host is revoked"),
        (status = 400, body = ErrorResponse, description = "Malformed host id"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "Another owner's host"),
        (status = 404, body = ErrorResponse, description = "No such live host"),
        (status = 409, body = ErrorResponse, description = "A concurrent change conflicted"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn revoke_host(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(host_id): Path<String>,
) -> ServerResult<StatusCode> {
    let host_id = parse_id(&host_id)?;
    let build = || RevokeHostOperation::new(host_id, auth.user_id, HostChange::Revoke);
    drive_retry(build, &state.get_ctx(), &retry_seed())
        .await
        .map_err(nfs_refusal)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/data/nfs/mounts", tag = "data/nfs",
    summary = "Create an NFS mount binding",
    description = r#"Binds an immutable export of a local ordinary bucket or folder to one of the caller's hosts.

**Authentication**: the host owner; `access_key` must be the caller's live local S3 key with
LIST on the bucket or folder, checked like the S3 request.

**Behavior**
- `prefix` is canonicalized like a dataset folder; absent exports the whole bucket.
- Distributed buckets are refused with 422.
- The displayed ids and mode only describe files to the client; the key admits each request."#,
    request_body(content = MountCreateRequest, example = json!({
        "host_id": "01JABCDEF0123456789ABCDEFG", "access_key": "aruna_access_key_example",
        "group_id": "01JABCDEF0123456789ABCDEFH", "bucket": "lab-data", "prefix": "raw/",
        "read_only": false, "allowed_networks": ["10.0.0.0/8"],
        "display_uid": 1000, "display_gid": 1000, "mode": 493})),
    responses((status = 201, body = MountView, description = "The binding", example = json!({
            "mount_id": "01JABCDEF0123456789ABCDEFJ", "host_id": "01JABCDEF0123456789ABCDEFG",
            "access_key": "aruna_access_key_example", "group_id": "01JABCDEF0123456789ABCDEFH",
            "kind": "folder", "bucket": "lab-data", "prefix": "raw/", "job_id": null,
            "read_only": false, "allowed_networks": ["10.0.0.0/8"],
            "display_uid": 1000, "display_gid": 1000, "mode": 493,
            "created_at_ms": 1764000000000u64})),
        (status = 400, body = ErrorResponse, description = "Malformed id, prefix, network or mode"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "Key, group or host not the caller's, or LIST refused"),
        (status = 404, body = ErrorResponse, description = "No such bucket or live host"),
        (status = 409, body = ErrorResponse, description = "A concurrent change conflicted"),
        (status = 422, body = ErrorResponse, description = "Distributed bucket"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn create_mount(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Json(request): Json<MountCreateRequest>,
) -> ServerResult<(StatusCode, Json<MountView>)> {
    if request.mode > 0o7777 {
        return Err(ServerError::BadRequestReason(
            "mode holds permission bits only, at most 0o7777".to_string(),
        ));
    }
    let host_id = parse_id(&request.host_id)?;
    let group_id = parse_id(&request.group_id)?;
    let (bucket, prefix, job_id, read_only, context) = match (request.bucket, request.job_id) {
        (Some(bucket), None) => {
            let context = access_context(&state, &bucket, request.access_key).await?;
            (bucket, request.prefix, None, request.read_only, context)
        }
        (None, Some(raw_job)) => {
            if request.prefix.is_some() {
                return Err(ServerError::BadRequestReason(
                    "a job input view mount takes no folder prefix".to_string(),
                ));
            }
            let ulid = parse_id(&raw_job)?;
            let job_id = JobId::try_from_bytes(ulid.to_bytes())
                .map_err(|_| ServerError::BadRequestReason("malformed job id".to_string()))?;
            if view_job(&request.access_key, &view_bucket(job_id)) != Some(job_id) {
                return Err(ServerError::Forbidden);
            }
            let record = crate::job_service::read_job_record(&state.get_ctx(), job_id)
                .await
                .map_err(|error| ServerError::InternalError(error.to_string().into()))?
                .ok_or(ServerError::NotFound)?;
            if view_entries(&record).is_err() {
                return Err(ServerError::NotFound);
            }
            let context = VfsAccessContext {
                realm_id: state.get_realm_id(),
                node_id: state.get_node_id(),
                access_key: request.access_key,
                protocol: VfsProtocol::Nfs,
                target: NfsMountTarget::JobInputView { job_id },
                read_only: true,
                now_ms: unix_timestamp_millis(),
            };
            (String::new(), None, Some(job_id), true, context)
        }
        _ => {
            return Err(ServerError::BadRequestReason(
                "specify exactly one of bucket or job_id".to_string(),
            ));
        }
    };
    let input = MountInput {
        context,
        caller: auth.user_id,
        host_id,
        group_id,
        bucket,
        prefix,
        job_id,
        read_only,
        allowed_networks: request.allowed_networks,
        display_uid: request.display_uid,
        display_gid: request.display_gid,
        mode: request.mode,
    };
    let build = || CreateMountOperation::new(input.clone());
    let mount = drive_retry(build, &state.get_ctx(), &retry_seed())
        .await
        .map_err(nfs_refusal)?;
    Ok((StatusCode::CREATED, Json(mount_view(mount))))
}

#[utoipa::path(get, path = "/data/nfs/mounts", tag = "data/nfs",
    summary = "List NFS mount bindings",
    description = r#"Lists the caller's mount bindings, optionally of one host, a page at a time.

**Authentication**: any caller without path restrictions; only own bindings are listed.

**Behavior**
- Pass `next_cursor` as `cursor` for the next page; `limit` is bounded by the server page size."#,
    params(("host_id" = Option<String>, Query, description = "Only bindings of this host"),
        ("cursor" = Option<String>, Query, description = "next_cursor of the previous page"),
        ("limit" = Option<usize>, Query, description = "Bindings per page")),
    responses((status = 200, body = MountPage, description = "One page of bindings", example = json!({
            "items": [{"mount_id": "01JABCDEF0123456789ABCDEFJ",
                "host_id": "01JABCDEF0123456789ABCDEFG", "access_key": "aruna_access_key_example",
                "group_id": "01JABCDEF0123456789ABCDEFH", "kind": "bucket",
                "bucket": "lab-data", "prefix": null, "job_id": null, "read_only": true,
                "allowed_networks": [], "display_uid": 1000, "display_gid": 1000,
                "mode": 365, "created_at_ms": 1764000000000u64}],
            "next_cursor": null})),
        (status = 400, body = ErrorResponse, description = "Malformed host id or cursor"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn list_mounts(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Query(query): Query<PageQuery>,
) -> ServerResult<Json<MountPage>> {
    let after = parse_cursor(query.cursor.as_deref())?;
    let host_id = query.host_id.as_deref().map(parse_id).transpose()?;
    let limit = query.limit.unwrap_or(STATE_PAGE);
    let list = ListMountsOperation::new(auth.user_id, host_id, after, limit);
    let page = drive(list, &state.get_ctx()).await.map_err(nfs_refusal)?;
    let (items, next_cursor) = page_of(page, mount_view);
    Ok(Json(MountPage { items, next_cursor }))
}

#[utoipa::path(get, path = "/data/nfs/mounts/{mount_id}", tag = "data/nfs",
    summary = "Read an NFS mount binding",
    description = r#"Returns one binding the caller created.

**Authentication**: any caller without path restrictions; another owner's binding answers 404."#,
    params(("mount_id" = String, Path, description = "Mount binding id")),
    responses((status = 200, body = MountView, description = "The binding", example = json!({
            "mount_id": "01JABCDEF0123456789ABCDEFJ", "host_id": "01JABCDEF0123456789ABCDEFG",
            "access_key": "aruna_access_key_example", "group_id": "01JABCDEF0123456789ABCDEFH",
            "kind": "bucket", "bucket": "lab-data", "prefix": null, "job_id": null,
            "read_only": true, "allowed_networks": [], "display_uid": 1000,
            "display_gid": 1000, "mode": 365, "created_at_ms": 1764000000000u64})),
        (status = 400, body = ErrorResponse, description = "Malformed mount id"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 404, body = ErrorResponse, description = "No such binding of the caller"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn get_mount(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(mount_id): Path<String>,
) -> ServerResult<Json<MountView>> {
    let read = GetMountOperation::get(parse_id(&mount_id)?, auth.user_id);
    let mount = drive(read, &state.get_ctx()).await.map_err(nfs_refusal)?;
    Ok(Json(mount_view(mount)))
}

#[utoipa::path(delete, path = "/data/nfs/mounts/{mount_id}", tag = "data/nfs",
    summary = "Delete an NFS mount binding",
    description = r#"Deletes one binding and revokes the live state of that binding only.

**Authentication**: the binding's owner, without path restrictions."#,
    params(("mount_id" = String, Path, description = "Mount binding id")),
    responses((status = 204, description = "The binding is deleted"),
        (status = 400, body = ErrorResponse, description = "Malformed mount id"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 404, body = ErrorResponse, description = "No such binding of the caller"),
        (status = 409, body = ErrorResponse, description = "A concurrent change conflicted"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn delete_mount(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(mount_id): Path<String>,
) -> ServerResult<StatusCode> {
    let mount_id = parse_id(&mount_id)?;
    let build = || DeleteMountOperation::delete(mount_id, auth.user_id);
    drive_retry(build, &state.get_ctx(), &retry_seed())
        .await
        .map_err(nfs_refusal)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path = "/data/buckets/{bucket}/nfs/stages", tag = "data/nfs",
    summary = "List retained NFS stages",
    description = r#"Lists the retained file server stages of the bucket's current incarnation.

**Authentication**: a bucket admin acting through `access_key`, one of the caller's own live
local S3 keys with COPY access on the bucket.

**Behavior**
- Pass `next_cursor` as `cursor` for the next page; `limit` is bounded by the server page size.
- `generation` is the fence that publish and delete name."#,
    params(("bucket" = String, Path, description = "Node-local S3 bucket name"),
        ("access_key" = String, Query, description = "The caller's S3 access key id"),
        ("cursor" = Option<String>, Query, description = "next_cursor of the previous page"),
        ("limit" = Option<usize>, Query, description = "Stages per page")),
    responses((status = 200, body = StagePage, description = "One page of retained stages", example = json!({
            "items": [{"stage_id": "01JABCDEF0123456789ABCDEFK", "key": "raw/run-1.csv",
                "generation": 3, "logical_size": 1048576,
                "owner": "01JABCDEF0123456789ABCDEFM@realm", "reason": "lost_life",
                "foreign_life": "01JABCDEF0123456789ABCDEFN",
                "updated_at_ms": 1764000000000u64}],
            "next_cursor": null})),
        (status = 400, body = ErrorResponse, description = "Malformed cursor"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "Not the caller's key, or no bucket admin"),
        (status = 404, body = ErrorResponse, description = "No such bucket"),
        (status = 422, body = ErrorResponse, description = "Distributed bucket"),
        (status = 503, body = ErrorResponse, description = "Storage unavailable")),
    security(("bearer_auth" = [])))]
pub async fn list_stages(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(bucket): Path<String>,
    Query(query): Query<StageQuery>,
) -> ServerResult<Json<StagePage>> {
    let after = parse_cursor(query.cursor.as_deref())?;
    let admin = stage_admin(&state, &auth, &bucket, query.access_key).await?;
    let limit = query.limit.unwrap_or(STATE_PAGE);
    let list = ListStagesOperation::new(admin, after, limit);
    let page = drive(list, &state.get_ctx()).await.map_err(nfs_refusal)?;
    let (items, next_cursor) = page_of(page, stage_view);
    Ok(Json(StagePage { items, next_cursor }))
}

/// Reads the checkpoint chunk by chunk into `sender` until `size` bytes or the first failure.
async fn feed_checkpoint(
    state: Arc<ServerState>,
    (stage_id, generation, size): (Ulid, u64, u64),
    sender: Sender<Result<Bytes, std::io::Error>>,
) {
    let mut offset = 0;
    while offset < size {
        let length = u32::try_from((size - offset).min(CHUNK_BYTES)).unwrap_or(u32::MAX);
        let read = ReadCheckpointOperation::new(stage_id, generation, offset, length);
        let chunk = match drive(read, &state.get_ctx()).await {
            Ok(chunk) if chunk.data.is_empty() => {
                Err(std::io::Error::other("the checkpoint ended early"))
            }
            Ok(chunk) => Ok(chunk.data),
            Err(error) => Err(std::io::Error::other(error.to_string())),
        };
        let failed = chunk.is_err();
        if let Ok(data) = &chunk {
            offset += data.len() as u64;
        }
        if sender.send(chunk.map(Bytes::from)).await.is_err() || failed {
            return;
        }
    }
}

/// The checkpoint image as a body; at most two chunks wait in memory.
fn checkpoint_body(
    state: Arc<ServerState>,
    stage: &NfsStageRecord,
) -> BackendStream<Result<Bytes, StreamError>> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let image = (stage.stage_id, stage.generation, stage.logical_size);
    tokio::spawn(feed_checkpoint(state, image, sender));
    BackendStream::new(futures_util::stream::poll_fn(move |context| {
        receiver.poll_recv(context)
    }))
}

#[utoipa::path(post, path = "/data/buckets/{bucket}/nfs/stages/{stage_id}/publish", tag = "data/nfs",
    summary = "Publish a retained NFS stage",
    description = r#"Publishes a retained stage as a new object version and removes the retained row.

**Authentication**: a bucket admin acting through `access_key`, one of the caller's own live
local S3 keys with COPY access on the bucket and PUT on the destination key.

**Behavior**
- `generation` and `expected_head` are the explicit conflict fence; a changed stage or head
  answers 409 and nothing is written.
- `destination_key` publishes to a separate recovery key instead of the stage's own name.
- Quota, placement and encryption apply as to any other write."#,
    params(("bucket" = String, Path, description = "Node-local S3 bucket name"),
        ("stage_id" = String, Path, description = "Retained stage id")),
    request_body(content = StagePublishRequest, example = json!({
        "access_key": "aruna_access_key_example", "generation": 3,
        "expected_head": "01JABCDEF0123456789ABCDEFN",
        "destination_key": "recovered/raw/run-1.csv"})),
    responses((status = 200, body = StagePublished, description = "The published version", example = json!({
            "version_id": "01JABCDEF0123456789ABCDEFK", "key": "recovered/raw/run-1.csv"})),
        (status = 400, body = ErrorResponse, description = "Malformed id or destination key"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "Not the caller's key, no bucket admin, or PUT refused"),
        (status = 404, body = ErrorResponse, description = "No such bucket or retained stage"),
        (status = 409, body = ErrorResponse, description = "The stage generation or destination head changed"),
        (status = 413, body = ErrorResponse, description = "Group quota exceeded"),
        (status = 422, body = ErrorResponse, description = "Distributed bucket"),
        (status = 503, body = ErrorResponse, description = "Storage or file server unavailable")),
    security(("bearer_auth" = [])))]
pub async fn publish_stage(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path((bucket, stage_id)): Path<(String, String)>,
    Json(request): Json<StagePublishRequest>,
) -> ServerResult<Json<StagePublished>> {
    let stage_id = parse_id(&stage_id)?;
    let expected_head = request.expected_head.as_deref().map(parse_id).transpose()?;
    let expected_life = request.expected_life.as_deref().map(parse_id).transpose()?;
    let admin = stage_admin(&state, &auth, &bucket, request.access_key).await?;
    let read = GetStageOperation::new(admin.clone(), stage_id);
    let stage = drive(read, &state.get_ctx()).await.map_err(nfs_refusal)?;
    if stage.generation != request.generation {
        return Err(nfs_refusal(VfsError::Conflict));
    }
    let key = request
        .destination_key
        .clone()
        .unwrap_or_else(|| stage.key.clone());
    let body = checkpoint_body(state.clone(), &stage);
    let mut put =
        publish_config(&state, &auth, (&bucket, &key), (body, stage.logical_size)).await?;
    put.exists = expected_head.is_some();
    let group_id = put.group_id;
    let input = PublishStageInput {
        admin,
        stage_id,
        generation: request.generation,
        expected_head,
        expected_life,
        destination: request.destination_key,
    };
    let operation = PublishStageOperation::new(input, put);
    let version_id = drive(operation, &state.get_ctx())
        .await
        .map_err(nfs_refusal)?;
    let complete = CompletePutOperation::new(CompletePutInput {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        auth: auth.clone(),
        group_id,
        bucket: bucket.clone(),
        key: key.clone(),
        version_id,
        size_bytes: stage.logical_size,
    });
    // Replication is queued like after every write; a failed queue entry is retried later.
    drive(complete, &state.get_ctx()).await.ok();
    Ok(Json(StagePublished {
        version_id: version_id.to_string(),
        key,
    }))
}

/// The ordinary write of a published stage: quota ceiling and routing of the bucket's group.
async fn publish_config(
    state: &ServerState,
    auth: &AuthContext,
    (bucket, key): (&str, &str),
    (body, size): (BackendStream<Result<Bytes, StreamError>>, u64),
) -> ServerResult<PutObjectConfig> {
    let context = state.get_ctx();
    let info = drive(GetBucketOperation::new(bucket.to_string()), &context)
        .await
        .map_err(|_| ServerError::NotFound)?;
    let realm = drive(GetConfigOperation::new(state.get_realm_id()), &context)
        .await
        .map_err(|_| ServerError::ServiceUnavailable)?;
    let rules = BucketRules::of(&info);
    let snapshot = SnapshotOperation::new(&node_routing(&context), info.group_id, rules);
    let routing = drive(snapshot, &context)
        .await
        .map_err(|_| ServerError::ServiceUnavailable)?;
    Ok(PutObjectConfig {
        user_id: auth.user_id,
        group_id: info.group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        request: PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_length: Some(size),
            body: Some(body),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: realm.quota.effective_group_ceiling(&info.group_id),
        routing,
    })
}

#[utoipa::path(delete, path = "/data/buckets/{bucket}/nfs/stages/{stage_id}", tag = "data/nfs",
    summary = "Delete a retained NFS stage",
    description = r#"Deletes a retained stage and its checkpoint file without publishing it.

**Authentication**: a bucket admin acting through `access_key`, one of the caller's own live
local S3 keys with COPY access on the bucket.

**Behavior**
- `generation` is the explicit conflict fence; a changed stage answers 409 and is kept."#,
    params(("bucket" = String, Path, description = "Node-local S3 bucket name"),
        ("stage_id" = String, Path, description = "Retained stage id"),
        ("access_key" = String, Query, description = "The caller's S3 access key id"),
        ("generation" = u64, Query, description = "The stage generation the admin saw")),
    responses((status = 204, description = "The stage is deleted"),
        (status = 400, body = ErrorResponse, description = "Malformed stage id"),
        (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "Not the caller's key, or no bucket admin"),
        (status = 404, body = ErrorResponse, description = "No such bucket or retained stage"),
        (status = 409, body = ErrorResponse, description = "The stage generation changed"),
        (status = 422, body = ErrorResponse, description = "Distributed bucket"),
        (status = 503, body = ErrorResponse, description = "Storage or file server unavailable")),
    security(("bearer_auth" = [])))]
pub async fn delete_stage(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path((bucket, stage_id)): Path<(String, String)>,
    Query(query): Query<StageDelete>,
) -> ServerResult<StatusCode> {
    let stage_id = parse_id(&stage_id)?;
    let admin = stage_admin(&state, &auth, &bucket, query.access_key).await?;
    let delete = DeleteStageOperation::new(admin, stage_id, query.generation);
    drive(delete, &state.get_ctx()).await.map_err(nfs_refusal)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
#[path = "nfs_tests.rs"]
mod tests;
