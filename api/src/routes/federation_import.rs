//! Destination realm routes of an export from another realm: sign an import intent, then admit
//! the source's grant, pull or accept the pushed artifact, and start the upload-backed import.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::federation_export::{
    GRANT_HEADER, INTENT_HEADER, decode_header, encode_header, grant_header, transfer_refused,
};
use crate::routes::rocrate_import::{
    ImportMetadataRequest, ImportSourceRequest, ImportTargetRequest, SubmitImportRequest,
    SubmitImportResponse, map_upload_error, parse_import_metadata, parse_import_target,
    submit_import,
};
use crate::server::state::ServerState;
use aruna_core::federation::{RealmDescriptor, Signed};
use aruna_core::handoff::descriptor_digest;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{RoCrateMediaType, RoCrateUploadRecord};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::{unix_timestamp_millis, unix_timestamp_secs};
use aruna_core::transfer::{
    ExportGrant, ImportDestination, ImportIntent, MAX_TRANSFER_SECS, TransferError, check_grant,
    check_intent, import_key,
};
use aruna_operations::driver::drive;
use aruna_operations::federation::import::{
    ImportError, ImportRecord, authorize_import, reusable_upload, write_import,
};
use aruna_operations::jobs::import::{
    CreateRoCrateConfig, CreateRoCrateOperation, load_rocrate_upload, write_rocrate_upload,
};
use aruna_operations::realm::get_config::GetConfigOperation;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use futures_util::{Stream, StreamExt, stream};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const SECRET_LEN: usize = 32;
const PULL_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(OpenApi)]
#[openapi(tags((name = "federation", description = "Native login across realms")))]
pub struct FederationImportApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(FederationImportApiDoc::openapi())
        .routes(routes!(create_intent))
        .routes(routes!(create_import))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct ImportIntentRequest {
    pub group_id: String,
    pub bucket: String,
    pub prefix: String,
    pub metadata_path: String,
    /// Largest artifact accepted; at most and by default the node's direct-upload limit.
    #[serde(default)]
    pub max_bytes: Option<u64>,
    /// Hex SHA-256 of the secret this realm's portal keeps.
    pub nonce: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct FederatedImportRequest {
    #[schema(value_type = Object)]
    pub intent: Signed<ImportIntent>,
    #[schema(value_type = Object)]
    pub grant: Signed<ExportGrant>,
    /// Hex of the 32 byte secret whose SHA-256 is the intent nonce.
    pub secret: String,
}

/// An admitted push of another realm's artifact into the upload route.
pub(crate) struct Push {
    pub intent: Signed<ImportIntent>,
    pub grant: Signed<ExportGrant>,
    pub key: String,
}

fn import_refused(error: ImportError) -> ServerError {
    let message = error.to_string();
    match error {
        ImportError::Denied => {
            ServerError::Refused(StatusCode::FORBIDDEN, "import_denied", message)
        }
        ImportError::Unbound => {
            ServerError::Refused(StatusCode::FORBIDDEN, "import_denied", message)
        }
        ImportError::NoBucket => ServerError::NotFound,
        ImportError::Conflict => {
            ServerError::Refused(StatusCode::CONFLICT, "import_conflict", message)
        }
        ImportError::Storage(_) => ServerError::ServiceUnavailableReason(message),
    }
}

fn gateway(code: &'static str, message: String) -> ServerError {
    ServerError::Refused(StatusCode::BAD_GATEWAY, code, message)
}

/// This realm's current signed descriptor.
async fn current_descriptor(state: &ServerState) -> ServerResult<Signed<RealmDescriptor>> {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    let settings = config.federation.ok_or_else(|| {
        ServerError::Refused(
            StatusCode::FORBIDDEN,
            "federation_disabled",
            "this realm has no federation settings".to_string(),
        )
    })?;
    Ok(settings.descriptor)
}

fn principal(state: &ServerState, intent: &ImportIntent) -> AuthContext {
    AuthContext {
        user_id: intent.principal,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: None,
    }
}

/// Admits the intent and grant against the current descriptor, then the import checks of the
/// intent's principal. `secret` is present when the browser posts it.
async fn admit(
    state: &ServerState,
    intent: &Signed<ImportIntent>,
    grant: &Signed<ExportGrant>,
    secret: Option<&[u8]>,
) -> ServerResult<String> {
    let (local, now) = (state.get_realm_id(), unix_timestamp_secs());
    let descriptor = current_descriptor(state).await?;
    check_intent(intent, &local, &descriptor, secret, now).map_err(transfer_refused)?;
    check_grant(grant, intent, now).map_err(transfer_refused)?;
    let (payload, source) = (&intent.payload, Some(grant.payload.source));
    let auth = principal(state, payload);
    let context = state.get_ctx();
    authorize_import(
        &context,
        &auth,
        &payload.destination,
        source,
        state.get_node_id(),
    )
    .await
    .map_err(import_refused)?;
    import_key(&grant.payload, &payload.destination)
        .map_err(|error| transfer_refused(TransferError::Signature(error)))
}

/// Admits a push from the source realm; `None` without intent and grant headers.
pub(crate) async fn admit_push(
    state: &ServerState,
    headers: &HeaderMap,
) -> ServerResult<Option<Push>> {
    let intent = decode_header::<Signed<ImportIntent>>(headers, INTENT_HEADER)?;
    let grant = grant_header(headers)?;
    let (intent, grant) = match (intent, grant) {
        (Some(intent), Some(grant)) => (intent, grant),
        (None, None) => return Ok(None),
        _ => return Err(ServerError::Unauthorized),
    };
    let key = admit(state, &intent, &grant, None).await?;
    Ok(Some(Push { intent, grant, key }))
}

/// The upload an earlier transfer of the same import key left on this node.
pub(crate) async fn pushed_upload(
    state: &ServerState,
    push: &Push,
) -> ServerResult<Option<RoCrateUploadRecord>> {
    let context = state.get_ctx();
    let (intent, grant) = (&push.intent.payload, &push.grant.payload);
    let Some(upload_id) = reusable_upload(&context, intent, grant, &push.key)
        .await
        .map_err(import_refused)?
    else {
        return Ok(None);
    };
    load_rocrate_upload(&context, upload_id)
        .await
        .map_err(ServerError::InternalError)
}

/// Checks a transferred upload against the grant and binds it to the intent and import key.
/// When a racing transfer bound its upload first, that one is kept and this spool discarded.
pub(crate) async fn bind_upload(
    state: &ServerState,
    intent: &Signed<ImportIntent>,
    grant: &Signed<ExportGrant>,
    key: &str,
    record: &RoCrateUploadRecord,
) -> ServerResult<RoCrateUploadRecord> {
    let expected = &grant.payload;
    if hex::encode(record.blake3) != expected.artifact_blake3
        || record.size != expected.artifact_size
    {
        return Err(gateway(
            "artifact_mismatch",
            "the artifact does not match its grant".to_string(),
        ));
    }
    let binding = ImportRecord {
        intent: intent.clone(),
        grant: grant.clone(),
    };
    let context = state.get_ctx();
    let bound = write_import(&context, key, record.upload_id, &binding)
        .await
        .map_err(import_refused)?;
    if bound == record.upload_id {
        return Ok(record.clone());
    }
    // An expired upload is removed by the next hidden sweep.
    let losing = RoCrateUploadRecord {
        expires_at_ms: 0,
        ..record.clone()
    };
    write_rocrate_upload(&context.storage_handle, &losing)
        .await
        .map_err(ServerError::ServiceUnavailableReason)?;
    load_rocrate_upload(&context, bound)
        .await
        .map_err(ServerError::InternalError)?
        .ok_or(ServerError::NotFound)
}

/// A `Sync` byte stream over a response body, as the upload writer needs.
fn shared_stream(
    body: impl Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static,
) -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
    let body = Mutex::new(Box::pin(
        body.map(|item| item.map_err(std::io::Error::other)),
    ));
    BackendStream::new(stream::poll_fn(move |cx| {
        let mut body = body.lock().unwrap_or_else(|error| error.into_inner());
        body.as_mut().poll_next(cx)
    }))
}

/// Pulls the artifact from the source realm with the grant into a new upload of `intent`'s
/// principal on this node.
async fn pull_artifact(
    state: &ServerState,
    intent: &Signed<ImportIntent>,
    grant: &Signed<ExportGrant>,
) -> ServerResult<RoCrateUploadRecord> {
    let context = state.get_ctx();
    let blob = context
        .blob_handle
        .as_ref()
        .ok_or(ServerError::ServiceUnavailable)?;
    let unreachable = |error: String| gateway("pull_unreachable", error);
    let response = blob
        .repository_request(reqwest::Method::GET, grant.payload.artifact_url.clone())
        .map_err(|error| unreachable(error.to_string()))?
        .header(GRANT_HEADER, encode_header(grant)?)
        .timeout(PULL_TIMEOUT)
        .send()
        .await
        .map_err(|error| unreachable(error.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        let message = format!("source realm answered {status}");
        return Err(ServerError::Refused(
            StatusCode::FORBIDDEN,
            "source_refused",
            message,
        ));
    }
    let expires_at_ms =
        unix_timestamp_millis().saturating_add(state.rocrate_limits().upload_retention_ms);
    let config = CreateRoCrateConfig {
        upload_id: Ulid::generate(),
        owner: intent.payload.principal,
        media_type: RoCrateMediaType::Zip,
        expires_at_ms,
        max_bytes: grant.payload.artifact_size,
        deadline: None,
        blob: shared_stream(response.bytes_stream()),
    };
    drive(CreateRoCrateOperation::new(config), &context)
        .await
        .map_err(map_upload_error)
}

#[utoipa::path(
    post,
    path = "/federation/import-intents",
    tag = "federation",
    summary = "Sign an import intent for another realm's export",
    description = r#"Signs the intent that lets the caller import from another realm into one destination.

**Authentication**: unrestricted bearer token of this realm, also of a federated session.

**Behavior**
- The caller needs WRITE on the destination bucket and metadata path, and the deny-only policies
  of operation `federation.import` must allow it.
- The intent names this realm's current descriptor digest, the caller, the destination, the size
  limit and the nonce, and lives 24 hours. It is not renewable."#,
    request_body(
        content = ImportIntentRequest,
        example = json!({ "group_id": "01JGROUP0123456789ABCDEFGH", "bucket": "lab", "prefix": "imports", "metadata_path": "datasets/run", "max_bytes": 1073741824, "nonce": "<hex sha256 of the browser secret>" })
    ),
    responses(
        (status = 200, description = "The signed import intent", body = serde_json::Value,
            example = json!({ "payload": { "realm_id": "<this realm id>", "descriptor_digest": "<hex>", "principal": "<user id>", "destination": { "group_id": "01JGROUP0123456789ABCDEFGH", "bucket": "lab", "prefix": "imports", "metadata_path": "datasets/run" }, "max_bytes": 1073741824, "nonce": "<hex>", "issued_at": 1791000000, "expires_at": 1791086400, "intent_id": "01JINTENT0123456789ABCDEFG" }, "signer": "Realm", "signature": "<hex>" })),
        (status = 400, description = "A malformed nonce, id, bucket, prefix or path", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "WRITE or a policy denied the import (code `import_denied`), or federation is not set up (code `federation_disabled`)", body = ErrorResponse),
        (status = 404, description = "The destination bucket does not exist", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_intent(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ImportIntentRequest>,
) -> ServerResult<Json<Signed<ImportIntent>>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if !hex::decode(&request.nonce).is_ok_and(|bytes| bytes.len() == SECRET_LEN) {
        return Err(ServerError::BadRequestReason(
            "nonce must be a hex SHA-256 digest".into(),
        ));
    }
    let descriptor = current_descriptor(&state).await?;
    let limits = state.rocrate_limits();
    let target = ImportTargetRequest {
        bucket: request.bucket,
        prefix: request.prefix,
    };
    let target = parse_import_target(target, limits.key_bytes)?;
    let metadata = ImportMetadataRequest {
        group_id: request.group_id,
        path: request.metadata_path,
        public: false,
    };
    let metadata = parse_import_metadata(metadata, limits.key_bytes)?;
    let destination = ImportDestination {
        group_id: metadata.group_id,
        bucket: target.bucket,
        prefix: target.prefix,
        metadata_path: metadata.path,
    };
    let context = state.get_ctx();
    authorize_import(&context, &auth, &destination, None, state.get_node_id())
        .await
        .map_err(import_refused)?;
    let now = unix_timestamp_secs();
    let limit = limits.direct_upload_bytes;
    let intent = ImportIntent {
        realm_id: state.get_realm_id(),
        descriptor_digest: descriptor_digest(&descriptor)
            .map_err(|error| ServerError::InternalError(error.to_string()))?,
        principal: auth.user_id,
        destination,
        max_bytes: request.max_bytes.unwrap_or(limit).min(limit),
        nonce: request.nonce.to_ascii_lowercase(),
        issued_at: now,
        expires_at: now.saturating_add(MAX_TRANSFER_SECS),
        intent_id: Ulid::generate(),
    };
    let signed = Signed::sign(intent, state.node_capabilities())
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(Json(signed))
}

#[utoipa::path(
    post,
    path = "/federation/imports",
    tag = "federation",
    summary = "Import another realm's export",
    description = r#"Admits the source realm's grant for the caller's intent and starts the upload-backed import.

**Authentication**: unrestricted bearer token of the intent's principal; the browser secret
binds the call to the portal that requested the intent.

**Behavior**
- The intent must name this realm's current descriptor and the secret; the grant must be signed
  by its source realm for this intent. WRITE and the `federation.import` policies are checked
  again, and on every step of the import job.
- When an earlier push or pull of the same import key left an upload on this node, it is used.
  Otherwise this node pulls the artifact from the grant's artifact URL through its egress guard.
  If that fails, the answer is 502 with code `pull_unreachable` and the source realm may push.
- The import key covers source realm, document, dataset and selection digests, destination
  bucket, prefix and metadata path: a retry returns the same job with `created` false, a
  different plan for the same key is a 409. Existing keys are never overwritten."#,
    request_body(
        content = FederatedImportRequest,
        example = json!({ "intent": { "payload": { "realm_id": "<this realm id>" }, "signer": "Realm", "signature": "<hex>" }, "grant": { "payload": { "source": "<source realm id>" }, "signer": "Realm", "signature": "<hex>" }, "secret": "<hex of the 32 byte browser secret>" })
    ),
    responses(
        (status = 202, description = "The import job is recorded", body = SubmitImportResponse,
            example = json!({ "job_id": "01JJOB0123456789ABCDEFGHIJ", "created": true, "owner_node_url": "https://b.example.org/api/v1", "status_url": "https://b.example.org/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHIJ", "report_url": "https://b.example.org/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHIJ/report" })),
        (status = 400, description = "A malformed secret", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the intent's principal, the intent or grant was refused (code `transfer_rejected`), WRITE or a policy denied the import (code `import_denied`), or the source refused the pull (code `source_refused`)", body = ErrorResponse),
        (status = 409, description = "The import key is bound to a different plan, or to another transfer, owner or artifact (code `import_conflict`)", body = ErrorResponse),
        (status = 502, description = "The source realm could not be reached (code `pull_unreachable`) or sent another artifact (code `artifact_mismatch`)", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<FederatedImportRequest>,
) -> ServerResult<(StatusCode, Json<SubmitImportResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (intent, grant) = (request.intent, request.grant);
    if auth.user_id != intent.payload.principal {
        return Err(ServerError::Forbidden);
    }
    let secret = hex::decode(&request.secret)
        .ok()
        .filter(|secret| secret.len() == SECRET_LEN)
        .ok_or_else(|| ServerError::BadRequestReason("secret must be 32 hex bytes".into()))?;
    let key = admit(&state, &intent, &grant, Some(&secret)).await?;
    let (payload, granted) = (&intent.payload, &grant.payload);
    let bound = reusable_upload(&state.get_ctx(), payload, granted, &key)
        .await
        .map_err(import_refused)?;
    let upload_id = match bound {
        Some(upload_id) => upload_id,
        None => {
            let record = pull_artifact(&state, &intent, &grant).await?;
            bind_upload(&state, &intent, &grant, &key, &record)
                .await?
                .upload_id
        }
    };
    let destination = intent.payload.destination;
    let submit = SubmitImportRequest {
        source: ImportSourceRequest::Upload {
            upload_id: upload_id.to_string(),
        },
        target: ImportTargetRequest {
            bucket: destination.bucket,
            prefix: destination.prefix,
        },
        metadata: ImportMetadataRequest {
            group_id: destination.group_id.to_string(),
            path: destination.metadata_path,
            public: false,
        },
        idempotency_key: Some(key),
    };
    // Without the session, a retry from a later session keeps the same plan digest.
    let auth = AuthContext {
        session: None,
        ..auth
    };
    submit_import(State(state), Extension(Some(auth)), Json(submit)).await
}

#[cfg(test)]
#[path = "federation_import_tests.rs"]
mod tests;
