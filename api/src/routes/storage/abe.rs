//! REST contracts for scoped keys, per-version envelopes and pinned downloads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod content;
mod records;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::abe::{AbeError, AbeParameters, EnvelopeArchive, ObjectEnvelope};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_operations::abe::KeyError;
use aruna_operations::abe::envelope::EnvelopeOperation;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::get::GetBucketOperation;
use axum::extract::{Query, State};
use axum::{Extension, Json};
use base64::{Engine, engine::general_purpose::STANDARD};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(envelope))
        .routes(routes!(content::content))
        .merge(records::router())
}

#[derive(Serialize, ToSchema)]
pub struct EnvelopeView {
    pub version_id: String,
    pub context: Value,
    pub parameters: Value,
    pub envelope: Value,
}
#[derive(Deserialize, ToSchema)]
pub struct VersionQuery {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
}

fn abe_error(error: AbeError) -> ServerError {
    let (status, code) = match error {
        AbeError::WrongKey => (StatusCode::FORBIDDEN, "wrong_object_key"),
        AbeError::Pending => (StatusCode::CONFLICT, "envelope_pending"),
        AbeError::Required => (StatusCode::LOCKED, "object_key_required"),
        AbeError::Parameters => (StatusCode::CONFLICT, "parameter_mismatch"),
        AbeError::Epoch => (StatusCode::CONFLICT, "stale_epoch"),
        AbeError::Context => (StatusCode::CONFLICT, "stale_version"),
        AbeError::Stale => (StatusCode::CONFLICT, "stale_request"),
        AbeError::Scope => (StatusCode::UNPROCESSABLE_ENTITY, "scope_unsupported"),
        AbeError::Limit => (StatusCode::PAYLOAD_TOO_LARGE, "encryption_limit"),
        AbeError::Crypto => (StatusCode::INTERNAL_SERVER_ERROR, "encryption_failed"),
        AbeError::Missing => return ServerError::NotFound,
    };
    ServerError::Refused(status, code, error.to_string())
}
fn key_error(error: KeyError) -> ServerError {
    match error {
        KeyError::Abe(e) => abe_error(e),
        KeyError::Missing => ServerError::NotFound,
        KeyError::Denied => ServerError::Forbidden,
        KeyError::Storage => ServerError::ServiceUnavailable,
    }
}
fn parameter_view(p: &AbeParameters, epoch: u64) -> ServerResult<Value> {
    Ok(
        json!({"realm_id":p.realm_id.to_string(), "node_id":p.node_id.to_string(),
        "bucket_id":p.key.bucket_id.to_string(), "generation":p.key.generation,
        "fingerprint":STANDARD.encode(p.fingerprint), "parameters":STANDARD.encode(&p.parameters),
        "epoch":epoch, "context":STANDARD.encode(p.context().map_err(abe_error)?)}),
    )
}
async fn authorize_version(
    state: &ServerState,
    auth: &AuthContext,
    query: &VersionQuery,
) -> ServerResult<(Ulid, BucketInfo)> {
    let ctx = state.get_ctx();
    let info = drive(GetBucketOperation::new(query.bucket.clone()), &ctx)
        .await
        .map_err(|_| ServerError::NotFound)?;
    let path = aruna_core::structs::storage::blob::object_permission_path(
        auth.realm_id,
        info.group_id,
        state.get_node_id(),
        &query.bucket,
        &query.key,
    );
    crate::auth::ensure_permission(state, auth, path, Permission::READ).await?;
    let version = Ulid::from_string(&query.version_id).map_err(|_| ServerError::BadRequest)?;
    Ok((version, info))
}
async fn read_envelope(
    state: &ServerState,
    query: &VersionQuery,
    version: Ulid,
) -> ServerResult<(ObjectEnvelope, EnvelopeArchive)> {
    let operation = EnvelopeOperation::new(query.bucket.clone(), query.key.clone(), version);
    drive(operation, &state.get_ctx()).await.map_err(abe_error)
}

#[utoipa::path(get, path = "/data/blobs/envelope", tag = "data/blobs",
    summary = "Read a version envelope",
    description = r#"Returns the envelope of one pinned object version.

**Authentication**: READ on the exact object, including realm and group request policies.

**Behavior**
- Binary fields use padded base64; version ids use ULIDs.
- Multipart and copied versions have no envelope and require bucket unlock."#,
    params(("bucket" = String, Query, description = "Node-local S3 bucket name"), ("key" = String, Query, description = "Literal object key"), ("version_id" = String, Query, description = "Pinned version ULID")),
    responses((status = 200, body = EnvelopeView, description = "Envelope of the exact authorized version", example = json!({"version_id":"01JABCDEF0123456789ABCDEFG","context":{},"parameters":{},"envelope":{}})),
        (status = 400, body = ErrorResponse, description = "Malformed version"), (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "READ refused"), (status = 404, body = ErrorResponse, description = "Version missing"), (status = 409, body = ErrorResponse, description = "Envelope pending or stale version")), security(("bearer_auth" = [])))]
pub async fn envelope(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<VersionQuery>,
) -> ServerResult<Json<EnvelopeView>> {
    let auth = crate::auth::require_realm_auth(&state, auth)?;
    let (version, _) = authorize_version(&state, &auth, &query).await?;
    let (envelope, _) = read_envelope(&state, &query, version).await?;
    let c = &envelope.context;
    let context = json!({"realm_id":c.parameters.realm_id.to_string(), "node_id":c.parameters.node_id.to_string(),
        "bucket_id":c.parameters.key.bucket_id.to_string(), "generation":c.parameters.key.generation,
        "fingerprint":STANDARD.encode(c.parameters.fingerprint), "epoch":c.epoch, "object_key":c.object_key,
        "write_id":c.write_id.to_string(), "public_key":STANDARD.encode(c.public_key),
        "bytes":STANDARD.encode(c.bytes().map_err(abe_error)?)});
    Ok(Json(EnvelopeView {
        version_id: query.version_id,
        parameters: parameter_view(&c.parameters, c.epoch)?,
        context,
        envelope: json!({"abe":STANDARD.encode(&envelope.abe),"recovery_enc":STANDARD.encode(envelope.recovery_enc),
            "recovery_ciphertext":STANDARD.encode(&envelope.recovery_ciphertext)}),
    }))
}
