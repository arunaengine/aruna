//! Source realm routes of an export into another realm: start the export for a verified import
//! intent, issue and revoke its grant, and push the artifact when the destination cannot pull.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};

use crate::metadata::{SubmitExportResponse, load_document_record, parse_document_id};
use crate::routes::execution::jobs::{job_urls, map_submit_error};
use crate::server::state::ServerState;
use aruna_core::federation::{RealmDescriptor, Signed};
use aruna_core::structs::execution::job::{
    ExportRoCrateSpec, ExportSelection, JobId, JobPayload, JobState,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_secs;
use aruna_core::transfer::{ExportGrant, ImportIntent, TransferError, check_remote, intent_digest};
use aruna_operations::federation::export::{
    GrantError, GrantRequest, admit_grant, authorize_export, issue_grant, read_record, revoke_grant,
};
use aruna_operations::jobs::service::{read_artifact_routed, read_owned_job, submit_export_job};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(tags((name = "federation", description = "Native login across realms")))]
pub struct FederationExportApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(FederationExportApiDoc::openapi())
        .routes(routes!(create_export))
        .routes(routes!(create_grant, delete_grant))
        .routes(routes!(push_export))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct PushExportRequest {
    /// The destination realm's signed descriptor; the artifact goes to its API URL.
    #[schema(value_type = Object)]
    pub descriptor: Signed<RealmDescriptor>,
    /// The import intent the export's grant was issued for.
    #[schema(value_type = Object)]
    pub intent: Signed<ImportIntent>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct FederatedExportRequest {
    /// The destination realm's signed descriptor as its portal sent it.
    #[schema(value_type = Object)]
    pub descriptor: Signed<RealmDescriptor>,
    /// The destination realm's signed import intent.
    #[schema(value_type = Object)]
    pub intent: Signed<ImportIntent>,
    pub document_id: String,
    /// Crate `@id`s of the File entities whose bytes travel; empty exports metadata only.
    #[serde(default)]
    pub files: Vec<String>,
}

/// Header carrying a signed export grant as unpadded base64url JSON.
pub(crate) const GRANT_HEADER: &str = "x-aruna-export-grant";
/// Header carrying a signed import intent as unpadded base64url JSON.
pub(crate) const INTENT_HEADER: &str = "x-aruna-import-intent";
const PUSH_TIMEOUT: Duration = Duration::from_secs(30 * 60);

pub(crate) fn encode_header<T: Serialize>(value: &T) -> ServerResult<String> {
    let json = serde_json::to_vec(value).map_err(|e| ServerError::InternalError(e.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

pub(crate) fn decode_header<T: DeserializeOwned>(
    headers: &HeaderMap,
    name: &str,
) -> ServerResult<Option<T>> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let invalid = || ServerError::BadRequestReason(format!("{name} is malformed"));
    let bytes = URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|_| invalid())?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| invalid())
}

pub(crate) fn grant_header(headers: &HeaderMap) -> ServerResult<Option<Signed<ExportGrant>>> {
    decode_header(headers, GRANT_HEADER)
}

pub(crate) fn transfer_refused(error: TransferError) -> ServerError {
    ServerError::Refused(
        StatusCode::FORBIDDEN,
        "transfer_rejected",
        error.to_string(),
    )
}

pub(crate) fn grant_refused(error: GrantError) -> ServerError {
    let message = error.to_string();
    match error {
        GrantError::Denied => ServerError::Refused(StatusCode::FORBIDDEN, "export_denied", message),
        GrantError::Revoked => ServerError::Refused(StatusCode::GONE, "grant_revoked", message),
        GrantError::Missing => ServerError::NotFound,
        GrantError::Unfinished => {
            ServerError::Refused(StatusCode::CONFLICT, "export_unfinished", message)
        }
        GrantError::Transfer(error) => transfer_refused(error),
        GrantError::Storage(_) => ServerError::ServiceUnavailableReason(message),
        GrantError::Sign(_) => ServerError::InternalError(message),
    }
}

#[utoipa::path(
    post,
    path = "/federation/exports",
    tag = "federation",
    summary = "Export a dataset into another realm",
    description = r#"Starts an RO-Crate export of a document for the realm that signed `intent`.

**Authentication**: unrestricted bearer token of this realm.

**Behavior**
- The descriptor must be signed by the realm it names, and the intent by that realm for this
  descriptor; both must be current and unexpired.
- The caller needs READ on the document, and the deny-only policies of operation
  `federation.export` see `destination_realm` and `with_files`.
- Only the File entities named in `files` travel; every other one stays a reference by its web
  identifier with the original ARN as `identifier`. Encrypted files need an unlocked bucket key
  that the caller holds. Selected files must be stored on this node.
- Repeating the call for the same intent returns the same job with `created` false."#,
    request_body(
        content = FederatedExportRequest,
        example = json!({
            "descriptor": { "payload": { "realm_id": "<destination realm id>", "name": "B", "description": "", "api_url": "https://b.example.org/api/v1", "portal_url": "https://b.example.org/", "issued_at": 1791000000 }, "signer": "Realm", "signature": "<hex>" },
            "intent": { "payload": { "realm_id": "<destination realm id>", "descriptor_digest": "<hex>", "principal": "<user id>", "destination": { "group_id": "<group id>", "bucket": "lab", "prefix": "imports", "metadata_path": "datasets/run" }, "max_bytes": 1073741824, "nonce": "<hex>", "issued_at": 1791000000, "expires_at": 1791086400, "intent_id": "<ulid>" }, "signer": "Realm", "signature": "<hex>" },
            "document_id": "01JDOC0123456789ABCDEFGHJK",
            "files": ["data/reads.fastq"]
        })
    ),
    responses(
        (status = 202, description = "The export job is accepted on this node", body = SubmitExportResponse,
            example = json!({
                "job_id": "01JJOB0123456789ABCDEFGHJK",
                "created": true,
                "owner_node_url": "https://a.example.org/api/v1",
                "status_url": "https://a.example.org/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHJK",
                "report_url": "https://a.example.org/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHJK/report",
                "artifact_url": "https://a.example.org/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHJK/artifacts/rocrate"
            })),
        (status = 400, description = "A malformed document id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The intent or descriptor was refused (code `transfer_rejected`), or READ or a policy denied the export (code `export_denied`)", body = ErrorResponse),
        (status = 404, description = "No such document on this node", body = ErrorResponse),
        (status = 409, description = "The caller's active RO-Crate job limit is reached", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_export(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<FederatedExportRequest>,
) -> ServerResult<(StatusCode, Json<SubmitExportResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let local = state.get_realm_id();
    check_remote(
        &request.intent,
        &request.descriptor,
        &local,
        unix_timestamp_secs(),
    )
    .map_err(transfer_refused)?;
    let document_id = parse_document_id(&request.document_id)?;
    let record = load_document_record(&state, document_id).await?;
    let audience = request.descriptor.payload.realm_id;
    let with_files = !request.files.is_empty();
    let path = &record.permission_path;
    authorize_export(&state.get_ctx(), &auth, path, audience, with_files)
        .await
        .map_err(grant_refused)?;
    let digest = intent_digest(&request.intent)
        .map_err(|error| transfer_refused(TransferError::Signature(error)))?;
    let key = format!("federation-{digest}");
    // Without the session, a retry from a later session keeps the same plan digest.
    let spec = ExportRoCrateSpec {
        destination: None,
        auth_context: AuthContext {
            session: None,
            ..auth
        },
        document_id,
        limits: state.rocrate_limits().clone(),
        selection: Some(ExportSelection {
            files: request.files,
            audience,
            intent_digest: digest,
        }),
    };
    let result = submit_export_job(&state.get_ctx(), spec, state.get_node_id(), Some(key))
        .await
        .map_err(map_submit_error)?;
    let urls = job_urls(&state, result.job_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitExportResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
            owner_node_url: urls.owner_node_url,
            status_url: urls.status_url,
            report_url: urls.report_url,
            artifact_url: urls.artifact_url,
        }),
    ))
}

/// The caller's finished export into another realm on this node, with its selection.
pub(crate) async fn owned_export(
    state: &ServerState,
    auth: &AuthContext,
    job_id: &str,
) -> ServerResult<(JobId, ExportRoCrateSpec)> {
    let job_id = crate::jobs::parse_job_id(job_id).map_err(|_| ServerError::NotFound)?;
    let record = read_owned_job(&state.get_ctx(), auth.user_id, job_id)
        .await
        .map_err(ServerError::InternalError)?
        .ok_or(ServerError::NotFound)?;
    let JobPayload::ExportRoCrate(spec) = record.payload else {
        return Err(ServerError::NotFound);
    };
    if spec.selection.is_none() {
        return Err(ServerError::NotFound);
    }
    if record.state != JobState::Succeeded {
        return Err(grant_refused(GrantError::Unfinished));
    }
    Ok((job_id, spec))
}

#[utoipa::path(
    post,
    path = "/federation/exports/{job_id}/grant",
    tag = "federation",
    summary = "Issue the grant of a finished export",
    description = r#"Signs the grant the destination realm uses to fetch this export's artifact.

**Authentication**: unrestricted bearer token of the user who started the export, at the node
that owns the job.

**Behavior**
- The export must have succeeded with every selected file included.
- READ, the `federation.export` policies and, for encrypted files, an unlocked key the caller
  holds are checked again now and on every artifact read with the grant. A credential cutoff of
  the caller ends every grant issued before it.
- The grant names the artifact's BLAKE3 and size, the dataset and selection digests and the
  artifact URL; it lives 24 hours and is not renewable. Repeating the call returns the stored
  grant while it is valid and not revoked. An expired grant is refused with code
  `transfer_rejected`; a new grant needs a new intent and export."#,
    params(("job_id" = String, Path, description = "Export job id")),
    responses(
        (status = 200, description = "The signed export grant", body = serde_json::Value,
            example = json!({ "payload": { "source": "<source realm id>", "audience": "<destination realm id>", "intent_digest": "<hex>", "export_job_id": "<ulid>", "document_id": "<ulid>", "source_revision": "<ulid>", "dataset_digest": "<hex>", "selection_digest": "<hex>", "artifact_url": "https://a.example.org/api/v1/compute/jobs/<job id>/artifacts/rocrate", "artifact_blake3": "<hex>", "artifact_size": 2048, "issued_at": 1791000000, "expires_at": 1791086400 }, "signer": "Realm", "signature": "<hex>" })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The export is no longer allowed (code `export_denied`) or its grant expired (code `transfer_rejected`)", body = ErrorResponse),
        (status = 404, description = "No such export into another realm of the caller on this node", body = ErrorResponse),
        (status = 409, description = "The export has not succeeded or left out a selected file (code `export_unfinished`)", body = ErrorResponse),
        (status = 410, description = "The grant was revoked (code `grant_revoked`)", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_grant(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<Json<Signed<ExportGrant>>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (job_id, spec) = owned_export(&state, &auth, &job_id).await?;
    let Some(selection) = spec.selection.as_ref() else {
        return Err(ServerError::NotFound);
    };
    let record = load_document_record(&state, spec.document_id).await?;
    let urls = job_urls(&state, job_id).await?;
    let artifact_url = urls
        .artifact_url
        .parse()
        .map_err(|_| ServerError::InternalError("artifact URL is invalid".to_string()))?;
    let request = GrantRequest {
        auth: &auth,
        job_id,
        document_id: spec.document_id,
        document_path: record.permission_path,
        selection,
        artifact_url,
        capabilities: state.node_capabilities(),
        now: unix_timestamp_secs(),
    };
    let grant = issue_grant(&state.get_ctx(), request)
        .await
        .map_err(grant_refused)?;
    Ok(Json(grant))
}

#[utoipa::path(
    delete,
    path = "/federation/exports/{job_id}/grant",
    tag = "federation",
    summary = "Revoke the grant of an export",
    description = r#"Marks the export's grant as revoked, so every later artifact read with it is refused.

**Authentication**: unrestricted bearer token of the user who started the export, at the node
that owns the job.

**Behavior**
- Copies the destination already made stay there; nothing is deleted at either realm.
- Before the export finishes, cancel the job instead."#,
    params(("job_id" = String, Path, description = "Export job id")),
    responses(
        (status = 204, description = "The grant is revoked"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "No grant for an export of the caller on this node", body = ErrorResponse),
        (status = 409, description = "The export has not succeeded (code `export_unfinished`)", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_grant(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (job_id, _) = owned_export(&state, &auth, &job_id).await?;
    match revoke_grant(&state.get_ctx(), job_id).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Err(ServerError::NotFound),
        Err(error) => Err(grant_refused(error)),
    }
}

fn unreachable(code: &'static str, message: String) -> ServerError {
    ServerError::Refused(StatusCode::BAD_GATEWAY, code, message)
}

#[utoipa::path(
    post,
    path = "/federation/exports/{job_id}/push",
    tag = "federation",
    summary = "Push an export's artifact to the destination realm",
    description = r#"Sends the finished artifact to the destination realm when its server cannot pull.

**Authentication**: unrestricted bearer token of the user who started the export, at the node
that owns the job.

**Behavior**
- The grant must be issued and not revoked; the export checks of every artifact read apply.
- The artifact goes to `/metadata/rocrate/uploads` below the API URL of the destination's
  verified descriptor, through this node's egress guard, authenticated by the intent and grant.
- The destination's answer (`upload_id`, `owner_node_url`, hash and size) is returned as it came
  and is not trusted here; the destination portal checks `owner_node_url` itself."#,
    params(("job_id" = String, Path, description = "Export job id")),
    request_body(
        content = PushExportRequest,
        example = json!({
            "descriptor": { "payload": { "realm_id": "<destination realm id>", "name": "B", "description": "", "api_url": "https://b.example.org/api/v1", "portal_url": "https://b.example.org/", "issued_at": 1791000000 }, "signer": "Realm", "signature": "<hex>" },
            "intent": { "payload": { "realm_id": "<destination realm id>", "descriptor_digest": "<hex>", "principal": "<user id>", "destination": { "group_id": "<group id>", "bucket": "lab", "prefix": "imports", "metadata_path": "datasets/run" }, "max_bytes": 1073741824, "nonce": "<hex>", "issued_at": 1791000000, "expires_at": 1791086400, "intent_id": "<ulid>" }, "signer": "Realm", "signature": "<hex>" }
        })
    ),
    responses(
        (status = 200, description = "The destination's upload answer, unverified", body = serde_json::Value,
            example = json!({ "upload_id": "01JABCDEF0123456789ABCDEFG", "blake3": "<hex>", "size": 2048, "expires_at": "2026-04-10T14:23:11.123+00:00", "owner_node_url": "https://b.example.org/api/v1" })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The intent was refused (code `transfer_rejected`) or the export is no longer allowed (code `export_denied`)", body = ErrorResponse),
        (status = 404, description = "No grant for an export of the caller on this node", body = ErrorResponse),
        (status = 410, description = "The grant was revoked (code `grant_revoked`)", body = ErrorResponse),
        (status = 502, description = "The destination could not be reached (code `push_unreachable`) or refused the upload (code `push_refused`)", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn push_export(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Json(request): Json<PushExportRequest>,
) -> ServerResult<Json<serde_json::Value>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (job_id, _) = owned_export(&state, &auth, &job_id).await?;
    let (local, now) = (state.get_realm_id(), unix_timestamp_secs());
    let context = state.get_ctx();
    check_remote(&request.intent, &request.descriptor, &local, now).map_err(transfer_refused)?;
    let grant = read_record(&context, job_id)
        .await
        .map_err(grant_refused)?
        .ok_or(ServerError::NotFound)?
        .grant;
    admit_grant(&context, local, job_id, &grant, now)
        .await
        .map_err(grant_refused)?;
    let digest = intent_digest(&request.intent)
        .map_err(|error| transfer_refused(TransferError::Signature(error)))?;
    if grant.payload.intent_digest != digest {
        return Err(transfer_refused(TransferError::Unbound));
    }
    let size = grant.payload.artifact_size;
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let (_, read) =
        read_artifact_routed(&context, auth.user_id, job_id, now_ms, Some(0..size), None)
            .await
            .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    let read = read.ok_or_else(|| grant_refused(GrantError::Unfinished))?;
    let mut url = request.descriptor.payload.api_url.clone();
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    let url = url
        .join("metadata/rocrate/uploads")
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let blob = context
        .blob_handle
        .as_ref()
        .ok_or(ServerError::ServiceUnavailable)?;
    let response = blob
        .repository_request(reqwest::Method::POST, url)
        .map_err(|error| unreachable("push_unreachable", error.to_string()))?
        .header(reqwest::header::CONTENT_TYPE, "application/zip")
        .header(reqwest::header::CONTENT_LENGTH, size)
        .header(GRANT_HEADER, encode_header(&grant)?)
        .header(INTENT_HEADER, encode_header(&request.intent)?)
        .body(reqwest::Body::wrap_stream(read.blob))
        .timeout(PUSH_TIMEOUT)
        .send()
        .await
        .map_err(|error| unreachable("push_unreachable", error.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(unreachable(
            "push_refused",
            format!("destination answered {status}"),
        ));
    }
    let answer = response
        .json()
        .await
        .map_err(|error| unreachable("push_refused", error.to_string()))?;
    Ok(Json(answer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::execution::jobs::get_job_artifact;
    use crate::tests::routes::{test_context, test_state, test_storage};
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::transfer::MAX_TRANSFER_SECS;
    use axum::http::HeaderValue;
    use axum::response::IntoResponse;
    use ed25519_dalek::SigningKey;
    use ulid::Ulid;
    use url::Url;

    #[tokio::test]
    async fn artifact_needs_record() {
        // Without a bearer only a grant with its node-local record reads the artifact.
        let (_dir, storage) = test_storage();
        let key = SigningKey::from_bytes(&[31; 32]);
        let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
        let capabilities = NodeCapabilities::management_node(key).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[32; 32]).public();
        let context = Arc::new(test_context(storage));
        let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
        let job_id = JobId::from_bytes([5; 16]);
        let now = unix_timestamp_secs();
        let grant = ExportGrant {
            source: realm_id,
            audience: RealmId::from_bytes([9; 32]),
            intent_digest: String::new(),
            export_job_id: job_id.as_ulid(),
            document_id: Ulid::from_bytes([6; 16]),
            source_revision: Ulid::from_bytes([7; 16]),
            dataset_digest: String::new(),
            selection_digest: String::new(),
            artifact_url: Url::parse("https://a.example.org/artifact").unwrap(),
            artifact_blake3: String::new(),
            artifact_size: 1,
            issued_at: now,
            expires_at: now + MAX_TRANSFER_SECS,
        };
        let grant = Signed::sign(grant, state.node_capabilities()).unwrap();
        let read = |headers: HeaderMap| {
            let state = State(state.clone());
            let path = Path(job_id.as_ulid().to_string());
            get_job_artifact(state, Extension(None), Extension(None), path, headers)
        };
        let error = read(HeaderMap::new()).await.unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::UNAUTHORIZED);
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_str(&encode_header(&grant).unwrap()).unwrap();
        headers.insert(GRANT_HEADER, value);
        let error = read(headers).await.unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);
        let mut headers = HeaderMap::new();
        headers.insert(GRANT_HEADER, HeaderValue::from_static("not-a-grant"));
        let error = read(headers).await.unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }
}
