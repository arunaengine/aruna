use crate::error::{ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, CreateOnboardingSecretRequest,
    CreateOnboardingSecretResponse, OnboardingMode, OnboardingPurpose, OnboardingSecret,
    OnboardingSecretRecord, OnboardingSecretState, bootstrap_issuer_proof_message,
    bootstrap_node_proof_message,
};
use aruna_core::structs::{AuthContext, NodeCapabilities, Permission};
use aruna_operations::bootstrap_onboarding_finalize::{
    BootstrapOnboardingFinalizeError, BootstrapOnboardingFinalizeInput,
    bootstrap_onboarding_finalize,
};
use aruna_operations::consume_onboarding_secret::ConsumeOnboardingSecretError;
use aruna_operations::create_onboarding_secret::{
    CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::delete_onboarding_secret::{
    DeleteOnboardingSecretError, DeleteOnboardingSecretInput, DeleteOnboardingSecretOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::ensure_realm_config::EnsureRealmConfigError;
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretError, InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::list_onboarding_secrets::ListOnboardingSecretsOperation;
use aruna_operations::reserve_onboarding_secret::ReserveOnboardingSecretError;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use crypto_box::{
    PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey,
    aead::{Aead, AeadCore, OsRng as CryptoOsRng},
};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rand::Rng;
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const DEFAULT_ONBOARDING_SECRET_TTL_SECS: u64 = 3600;

#[derive(OpenApi)]
#[openapi(
    tags((name = "onboarding", description = "Node onboarding and bootstrap operations"))
)]
pub struct OnboardingApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(OnboardingApiDoc::openapi())
        .routes(routes!(bootstrap_onboarding))
        .routes(routes!(create_onboarding_secret, list_onboarding_secrets))
        .routes(routes!(revoke_onboarding_secret))
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct BootstrapEndpointDoc {
    pub id: String,
    pub addrs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CreateOnboardingSecretRequestDoc {
    pub seed_url: String,
    pub mode: String,
    pub expires_in_seconds: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CreateOnboardingSecretResponseDoc {
    pub onboarding_secret: String,
    pub mode: String,
    pub expires_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct BootstrapOnboardingRequestDoc {
    pub onboarding_secret: String,
    pub node_id: String,
    pub node_proof: String,
    pub transport_public_key: Option<String>,
    pub issuer_public_key: Option<String>,
    pub issuer_proof: Option<String>,
    pub node_location: Option<String>,
    pub node_weight: Option<u32>,
    pub node_labels: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct BootstrapOnboardingResponseDoc {
    pub realm_id: String,
    pub mode: String,
    pub temporary_bootstrap_endpoint: BootstrapEndpointDoc,
    pub wrapped_realm_private_key: Option<String>,
    pub wrapped_realm_private_key_nonce: Option<String>,
    pub wrapping_public_key: Option<String>,
    pub delegation_signature: Option<String>,
    pub onboarding_sync_ticket: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct ListOnboardingSecretsResponse {
    pub secrets: Vec<OnboardingSecretSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct OnboardingSecretSummary {
    pub enrollment_id: String,
    pub mode: String,
    pub expires_at: u64,
    pub claimed_node_id: Option<String>,
}

impl From<OnboardingSecretRecord> for OnboardingSecretSummary {
    fn from(record: OnboardingSecretRecord) -> Self {
        Self {
            enrollment_id: record.enrollment_id.to_string(),
            mode: format!("{:?}", record.mode),
            expires_at: record.expires_at,
            claimed_node_id: record.claimed_node_id,
        }
    }
}

async fn authorize_onboarding_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id || !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }

    // Route through the single authorization boundary so realm and group
    // request policies also constrain onboarding administration.
    crate::auth::ensure_permission(
        state,
        &auth,
        format!("/{realm_id}/admin/onboarding"),
        Permission::WRITE,
    )
    .await?;

    Ok(auth)
}

async fn prune_stale_onboarding_secrets(state: &Arc<ServerState>) -> ServerResult<()> {
    let now = now_timestamp();
    let secrets = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    for secret in secrets {
        if secret.record.expires_at < now
            && !matches!(&secret.state, OnboardingSecretState::Finalizing { .. })
        {
            drive(
                DeleteOnboardingSecretOperation::new(DeleteOnboardingSecretInput {
                    enrollment_id: secret.record.enrollment_id,
                }),
                &state.get_ctx(),
            )
            .await
            .map_err(map_delete_error)?;
        }
    }

    Ok(())
}

#[utoipa::path(
    post,
    path = "/admin/onboarding/secrets",
    tag = "onboarding",
    summary = "Mint a node enrollment secret",
    description = "Requires a bearer token of this realm with WRITE on the realm's onboarding admin path, and only a management node serves it; any other node answers 403. The response carries the enrollment secret exactly once: the node stores only its hash, so a secret that is lost cannot be recovered and must be revoked and minted again. Treat the value like a credential, hand it to exactly one joining node, and expect it to be single-use. `mode` fixes what the joiner may become and is one of `Management`, `Server` or `Local`; a Management secret later lets the joiner receive the realm private key wrapped to its transport key, so it is the most sensitive of the three. `expires_in_seconds` defaults to 3600 and is clamped to 60..=86400, and `expires_at` is the resulting absolute expiry in Unix seconds. Every expired secret that is not already mid-enrollment is discarded before the new one is created.",
    request_body(
        content = CreateOnboardingSecretRequestDoc,
        description = "Seed URL the joiner calls back, the mode it is enrolled as, and an optional lifetime",
        example = json!({
            "seed_url": "https://node.example.test/api/v1",
            "mode": "Server",
            "expires_in_seconds": 3600
        })
    ),
    responses(
        (
            status = 201,
            description = "Secret created; `onboarding_secret` is shown here and never again",
            body = CreateOnboardingSecretResponseDoc,
            example = json!({
                "onboarding_secret": "<onboarding-secret-shown-once>",
                "mode": "Server",
                "expires_at": 1775748191
            })
        ),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Token belongs to another realm, this is not a management node, or the caller lacks WRITE on the realm's onboarding admin path", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_onboarding_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateOnboardingSecretRequest>,
) -> ServerResult<(StatusCode, Json<CreateOnboardingSecretResponse>)> {
    let _auth = authorize_onboarding_admin(&state, auth).await?;
    prune_stale_onboarding_secrets(&state).await?;

    let ttl = request
        .expires_in_seconds
        .unwrap_or(DEFAULT_ONBOARDING_SECRET_TTL_SECS)
        .clamp(60, 86_400);
    let expires_at = now_timestamp().saturating_add(ttl);

    let mut secret_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut secret_bytes);

    let onboarding_secret = OnboardingSecret {
        seed_url: request.seed_url,
        enrollment_id: ulid::Ulid::generate(),
        secret: secret_bytes,
        mode: request.mode,
        realm_id: state.get_realm_id(),
        purpose: OnboardingPurpose::NodeEnrollment,
    };
    let encoded_secret = onboarding_secret
        .encode()
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let record = OnboardingSecretRecord {
        enrollment_id: onboarding_secret.enrollment_id,
        secret_hash: onboarding_secret.secret_hash(),
        mode: onboarding_secret.mode,
        purpose: OnboardingPurpose::NodeEnrollment,
        expires_at,
        claimed_node_id: None,
    };

    drive(
        CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput { record }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(CreateOnboardingSecretResponse {
            onboarding_secret: encoded_secret,
            mode: onboarding_secret.mode,
            expires_at,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/admin/onboarding/secrets",
    tag = "onboarding",
    summary = "List outstanding node enrollment secrets",
    description = "Requires a bearer token of this realm with WRITE on the realm's onboarding admin path, and only a management node serves it; any other node answers 403. Returns bookkeeping only: the enrollment id, the mode, the absolute expiry in Unix seconds and, once a joiner has claimed the secret, the node id it was claimed by. The secret value itself is never returned here, because only its hash was kept when it was minted. Expired secrets that are not mid-enrollment are discarded before the list is built, so the list holds live and in-flight enrollments; entries are ordered by expiry, soonest first. The list is this management node's local state, not a realm-wide fan-out.",
    responses(
        (
            status = 200,
            description = "Live and in-flight enrollment secrets, soonest expiry first",
            body = ListOnboardingSecretsResponse,
            example = json!({
                "secrets": [
                    {
                        "enrollment_id": "01JABCDEF0123456789ABCDEFG",
                        "mode": "Server",
                        "expires_at": 1775748191,
                        "claimed_node_id": null
                    },
                    {
                        "enrollment_id": "01JMETADATA0123456789ABCDE",
                        "mode": "Local",
                        "expires_at": 1775751791,
                        "claimed_node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978"
                    }
                ]
            })
        ),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Token belongs to another realm, this is not a management node, or the caller lacks WRITE on the realm's onboarding admin path", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_onboarding_secrets(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ListOnboardingSecretsResponse>)> {
    let _auth = authorize_onboarding_admin(&state, auth).await?;
    prune_stale_onboarding_secrets(&state).await?;
    let mut secrets = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    secrets.sort_by_key(|entry| entry.record.expires_at);

    Ok((
        StatusCode::OK,
        Json(ListOnboardingSecretsResponse {
            secrets: secrets
                .into_iter()
                .map(|entry| OnboardingSecretSummary::from(entry.record))
                .collect(),
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/admin/onboarding/secrets/{id}",
    tag = "onboarding",
    summary = "Revoke a pending node enrollment secret",
    description = "Requires a bearer token of this realm with WRITE on the realm's onboarding admin path, and only a management node serves it; any other node answers 403. Deletes the enrollment record on this node, which makes the secret unredeemable from here on. This is the remedy for a secret that leaked or was never used, and it is the only remedy, since the secret value itself was never stored. Revocation does not undo an enrollment that already completed: a node that finished bootstrapping stays a member of the realm and has to be removed through the realm configuration instead. A secret that is already gone, expired and pruned or revoked by an earlier call, answers 404.",
    params(("id" = String, Path, description = "Enrollment id of the secret, the ULID reported when it was minted and by the list endpoint")),
    responses(
        (status = 204, description = "Secret deleted and no longer redeemable; no response body"),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Token belongs to another realm, this is not a management node, or the caller lacks WRITE on the realm's onboarding admin path", body = crate::error::ErrorResponse),
        (status = 404, description = "No enrollment secret with this id on this node", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_onboarding_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(enrollment_id): Path<String>,
) -> ServerResult<StatusCode> {
    let _auth = authorize_onboarding_admin(&state, auth).await?;
    let enrollment_id = Ulid::from_string(&enrollment_id).map_err(|_| ServerError::BadRequest)?;

    drive(
        DeleteOnboardingSecretOperation::new(DeleteOnboardingSecretInput { enrollment_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_delete_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/onboarding/bootstrap",
    tag = "onboarding",
    summary = "Redeem an enrollment secret and join the realm",
    description = "Deliberately unauthenticated: a joining node has no realm token yet. The enrollment secret plus a signature by the joiner's own node key are the credentials, so an unknown, expired, already claimed or unmatched secret and any signature that does not verify are refused with 401, without saying which of the two failed. Only a management node serves this; any other node answers 403. The secret is single-use and is consumed when enrollment finalizes, which also adds the joiner to the realm configuration; a rejected attempt leaves the secret usable so an operator does not have to mint a new one after a typo. What must be sent depends on the mode the secret was minted for: a Server secret additionally requires `issuer_public_key` and a matching `issuer_proof` and returns a delegation signature; a Management secret requires `transport_public_key` and returns the realm private key encrypted to it, along with the nonce and the ephemeral public key needed to open it; a Local secret needs neither. The response always carries the realm id, the temporary endpoint to dial and a one-time sync ticket the joiner uses to fetch the realm's core documents. `node_location`, `node_weight` and `node_labels` seed the joiner's placement entry and are optional. Everything returned here is one-time joining material and must never be logged or reused.",
    request_body(
        content = BootstrapOnboardingRequestDoc,
        description = "The enrollment secret, the joiner's node id, its proof of possession of the node key, and any mode-specific key material",
        example = json!({
            "onboarding_secret": "<onboarding-secret-shown-once>",
            "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
            "node_proof": "<node-key-proof-signature>",
            "issuer_public_key": "<issuer-public-key>",
            "issuer_proof": "<issuer-proof-signature>",
            "node_location": "dc-a",
            "node_weight": 100,
            "node_labels": {"zone": "dc-a"}
        })
    ),
    responses(
        (
            status = 200,
            description = "Enrollment finalized; joining material for the mode the secret was minted for. `addrs` entries are tagged transport addresses, either `Ip` or `Relay`",
            body = BootstrapOnboardingResponseDoc,
            example = json!({
                "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "mode": "Server",
                "temporary_bootstrap_endpoint": {
                    "id": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a",
                    "addrs": [
                        {"Ip": "192.0.2.10:4433"},
                        {"Relay": "https://relay.example.test/"}
                    ]
                },
                "wrapped_realm_private_key": null,
                "wrapped_realm_private_key_nonce": null,
                "wrapping_public_key": null,
                "delegation_signature": "<realm-delegation-signature>",
                "onboarding_sync_ticket": "<one-time-onboarding-sync-ticket>"
            })
        ),
        (status = 400, description = "Malformed node id, key material or proof, or key material missing for the mode the secret was minted for", body = crate::error::ErrorResponse),
        (status = 401, description = "Unknown, expired, already claimed or non-matching enrollment secret, or a proof that does not verify", body = crate::error::ErrorResponse),
        (status = 403, description = "This node does not serve enrollment, or the secret was not minted for node enrollment", body = crate::error::ErrorResponse)
    )
)]
pub async fn bootstrap_onboarding(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<BootstrapOnboardingRequest>,
) -> ServerResult<(StatusCode, Json<BootstrapOnboardingResponse>)> {
    if !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }

    let onboarding_secret = OnboardingSecret::decode(&request.onboarding_secret)
        .map_err(|_| ServerError::Unauthorized)?;
    let node_id = NodeId::from_str(&request.node_id).map_err(|_| ServerError::BadRequest)?;
    verify_node_proof(&request, node_id)?;

    let record = drive(
        InspectOnboardingSecretOperation::new(InspectOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: onboarding_secret.secret_hash(),
            node_id: request.node_id.clone(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_inspect_error)?;

    // Only node-enrollment secrets may bootstrap a node; an initial-admin
    // secret is rejected here before any reserve or consume.
    if record.purpose != OnboardingPurpose::NodeEnrollment {
        return Err(ServerError::Forbidden);
    }

    match record.mode {
        OnboardingMode::Server => {
            let issuer_public_key = request
                .issuer_public_key
                .as_deref()
                .ok_or(ServerError::BadRequest)?;
            verify_issuer_proof(&request, issuer_public_key)?;
        }
        OnboardingMode::Management | OnboardingMode::Local => {}
    }

    let bootstrap_endpoint = state
        .bootstrap_endpoint()
        .ok_or_else(|| ServerError::InternalError("net handle unavailable".to_string()))?;
    let wrapped_management_key = if matches!(record.mode, OnboardingMode::Management) {
        Some(wrap_realm_private_key(
            &state,
            request
                .transport_public_key
                .as_deref()
                .ok_or(ServerError::BadRequest)?,
        )?)
    } else {
        None
    };
    let delegation_signature = if matches!(record.mode, OnboardingMode::Server) {
        let issuer_public_key = request
            .issuer_public_key
            .as_deref()
            .ok_or(ServerError::BadRequest)?;
        Some(
            state
                .sign_server_delegation(issuer_public_key)
                .ok_or(ServerError::Forbidden)?,
        )
    } else {
        None
    };
    let realm_signing_key = match state.node_capabilities() {
        NodeCapabilities::Management {
            realm_signing_key, ..
        } => realm_signing_key.clone(),
        _ => return Err(ServerError::Forbidden),
    };

    let finalized = bootstrap_onboarding_finalize(
        BootstrapOnboardingFinalizeInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: onboarding_secret.secret_hash(),
            node_id,
            realm_id: state.get_realm_id(),
            local_node_id: state.get_node_id(),
            realm_signing_key,
            now: now_timestamp(),
            node_location: request.node_location.clone(),
            node_weight: request.node_weight,
            node_labels: request.node_labels.clone(),
        },
        state.get_ctx(),
    )
    .await
    .map_err(map_finalize_error)?;

    let response = match finalized.mode {
        OnboardingMode::Management => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Management,
            temporary_bootstrap_endpoint: bootstrap_endpoint,
            wrapped_realm_private_key: wrapped_management_key.as_ref().map(|value| value.0.clone()),
            wrapped_realm_private_key_nonce: wrapped_management_key
                .as_ref()
                .map(|value| value.1.clone()),
            wrapping_public_key: wrapped_management_key.as_ref().map(|value| value.2.clone()),
            delegation_signature: None,
            onboarding_sync_ticket: finalized.onboarding_sync_ticket,
        },
        OnboardingMode::Server => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Server,
            temporary_bootstrap_endpoint: bootstrap_endpoint,
            wrapped_realm_private_key: None,
            wrapped_realm_private_key_nonce: None,
            wrapping_public_key: None,
            delegation_signature,
            onboarding_sync_ticket: finalized.onboarding_sync_ticket,
        },
        OnboardingMode::Local => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Local,
            temporary_bootstrap_endpoint: bootstrap_endpoint,
            wrapped_realm_private_key: None,
            wrapped_realm_private_key_nonce: None,
            wrapping_public_key: None,
            delegation_signature: None,
            onboarding_sync_ticket: finalized.onboarding_sync_ticket,
        },
    };

    Ok((StatusCode::OK, Json(response)))
}

fn now_timestamp() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn map_consume_error(error: ConsumeOnboardingSecretError) -> ServerError {
    match error {
        ConsumeOnboardingSecretError::NotFound
        | ConsumeOnboardingSecretError::Expired
        | ConsumeOnboardingSecretError::AlreadyClaimed
        | ConsumeOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_finalize_error(error: BootstrapOnboardingFinalizeError) -> ServerError {
    match error {
        BootstrapOnboardingFinalizeError::Reserve(
            ReserveOnboardingSecretError::NotFound
            | ReserveOnboardingSecretError::Expired
            | ReserveOnboardingSecretError::AlreadyClaimed
            | ReserveOnboardingSecretError::InvalidSecret,
        ) => ServerError::Unauthorized,
        BootstrapOnboardingFinalizeError::Consume(error) => map_consume_error(error),
        BootstrapOnboardingFinalizeError::EnsureRealmConfig(
            EnsureRealmConfigError::NodeKindMismatch { .. },
        ) => ServerError::BadRequest,
        BootstrapOnboardingFinalizeError::EnsureRealmConfig(
            EnsureRealmConfigError::HandleSpaceExhausted,
        ) => ServerError::Conflict("realm handle space is exhausted".to_string()),
        BootstrapOnboardingFinalizeError::ReservedNodeLabel(_)
        | BootstrapOnboardingFinalizeError::NodeLocationTooLong => ServerError::BadRequest,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_inspect_error(error: InspectOnboardingSecretError) -> ServerError {
    match error {
        InspectOnboardingSecretError::NotFound
        | InspectOnboardingSecretError::Expired
        | InspectOnboardingSecretError::AlreadyClaimed
        | InspectOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_delete_error(error: DeleteOnboardingSecretError) -> ServerError {
    match error {
        DeleteOnboardingSecretError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn verify_node_proof(request: &BootstrapOnboardingRequest, node_id: NodeId) -> ServerResult<()> {
    let signature =
        Signature::from_str(&request.node_proof).map_err(|_| ServerError::Unauthorized)?;
    let verifying_key =
        VerifyingKey::from_bytes(node_id.as_bytes()).map_err(|_| ServerError::BadRequest)?;
    verifying_key
        .verify(
            &bootstrap_node_proof_message(
                &request.onboarding_secret,
                &request.node_id,
                request.transport_public_key.as_deref(),
            ),
            &signature,
        )
        .map_err(|_| ServerError::Unauthorized)
}

fn verify_issuer_proof(
    request: &BootstrapOnboardingRequest,
    issuer_public_key: &str,
) -> ServerResult<()> {
    let issuer_proof = request
        .issuer_proof
        .as_ref()
        .ok_or(ServerError::BadRequest)?;
    let signature = Signature::from_str(issuer_proof).map_err(|_| ServerError::Unauthorized)?;
    let issuer_public_key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(issuer_public_key)
        .map_err(|_| ServerError::BadRequest)?;
    let verifying_key = VerifyingKey::from_bytes(
        issuer_public_key_bytes
            .as_slice()
            .try_into()
            .map_err(|_| ServerError::BadRequest)?,
    )
    .map_err(|_| ServerError::BadRequest)?;
    verifying_key
        .verify(
            &bootstrap_issuer_proof_message(
                &request.onboarding_secret,
                &request.node_id,
                request
                    .issuer_public_key
                    .as_deref()
                    .ok_or(ServerError::BadRequest)?,
            ),
            &signature,
        )
        .map_err(|_| ServerError::Unauthorized)
}

fn wrap_realm_private_key(
    state: &Arc<ServerState>,
    transport_public_key: &str,
) -> ServerResult<(String, String, String)> {
    let realm_private_key_pem = state
        .realm_private_key_pem()
        .ok_or(ServerError::Forbidden)?;
    let transport_public_key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(transport_public_key)
        .map_err(|_| ServerError::BadRequest)?;
    let transport_public_key = TransportPublicKey::from(
        <[u8; 32]>::try_from(transport_public_key_bytes.as_slice())
            .map_err(|_| ServerError::BadRequest)?,
    );
    let wrapping_secret_key = TransportSecretKey::generate(&mut CryptoOsRng);
    let wrapping_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(wrapping_secret_key.public_key().as_bytes());
    let cipher = SalsaBox::new(&transport_public_key, &wrapping_secret_key);
    let nonce = SalsaBox::generate_nonce(&mut CryptoOsRng);
    let ciphertext = cipher
        .encrypt(&nonce, realm_private_key_pem.as_bytes())
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(ciphertext),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(nonce),
        wrapping_public_key,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        ServerError, bootstrap_onboarding, create_onboarding_secret, list_onboarding_secrets,
        map_finalize_error, revoke_onboarding_secret,
    };
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::AdminDocumentReducerState;
    use aruna_core::admin_documents::AdminDocumentTarget;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::onboarding::{
        BootstrapOnboardingRequest, CreateOnboardingSecretRequest, OnboardingMode,
        OnboardingPurpose, OnboardingSecret, OnboardingSecretRecord, OnboardingSecretState,
        bootstrap_issuer_proof_message, bootstrap_node_proof_message,
    };
    use aruna_core::storage_entries::admin_document_reducer_state_key;
    use aruna_core::structs::{
        Actor, AuthContext, NodeCapabilities, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::bootstrap_onboarding_finalize::BootstrapOnboardingFinalizeError;
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::list_onboarding_secrets::ListOnboardingSecretsOperation;
    use aruna_operations::reserve_onboarding_secret::{
        ReserveOnboardingSecretInput, ReserveOnboardingSecretOperation,
    };
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use axum::Extension;
    use axum::Json;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use base64::Engine;
    use crypto_box::{
        PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey, aead::Aead,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn setup_management_state() -> (
        Arc<ServerState>,
        RealmId,
        iroh::PublicKey,
        UserId,
        NetHandle,
        TempDir,
    ) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });

        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = net_handle.node_id();

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: vec![],
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::management_node(realm_signing_key).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        (state, realm_id, node_id, user_id, net_handle, tempdir)
    }

    #[test]
    fn placement_validation_errors_map_to_bad_request() {
        assert!(matches!(
            map_finalize_error(BootstrapOnboardingFinalizeError::ReservedNodeLabel(
                String::new()
            )),
            ServerError::BadRequest
        ));
        assert!(matches!(
            map_finalize_error(BootstrapOnboardingFinalizeError::NodeLocationTooLong),
            ServerError::BadRequest
        ));
    }

    #[tokio::test]
    async fn create_and_consume_server_onboarding_secret() {
        let (state, realm_id, seed_node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth)),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: OnboardingMode::Server,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let issuer_key = generate_signing_key();
        let issuer_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_key.verifying_key().to_bytes());
        let onboarding_secret = created.onboarding_secret;
        let node_proof = SigningKey::from_bytes(&[9u8; 32]);
        let bootstrap_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
        let node_id = bootstrap_node_id.to_string();
        let node_signature = node_proof
            .sign(&bootstrap_node_proof_message(
                &onboarding_secret,
                &node_id,
                None,
            ))
            .to_string();
        let issuer_signature = issuer_key
            .sign(&bootstrap_issuer_proof_message(
                &onboarding_secret,
                &node_id,
                &issuer_public_key,
            ))
            .to_string();

        let (_, Json(bootstrap)) = bootstrap_onboarding(
            State(state.clone()),
            Json(BootstrapOnboardingRequest {
                onboarding_secret,
                node_id,
                node_proof: node_signature,
                transport_public_key: None,
                issuer_public_key: Some(issuer_public_key.clone()),
                issuer_proof: Some(issuer_signature),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(bootstrap.mode, OnboardingMode::Server);
        assert_eq!(bootstrap.realm_id, realm_id.to_string());
        assert_eq!(bootstrap.temporary_bootstrap_endpoint.id, seed_node_id);
        assert!(bootstrap.wrapped_realm_private_key.is_none());
        assert!(bootstrap.delegation_signature.is_some());
        assert!(!bootstrap.onboarding_sync_ticket.is_empty());

        let config = match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: byteview::ByteView::from(*realm_id.as_bytes()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected realm config read result: {other:?}"),
        };
        assert!(config.has_node(bootstrap_node_id));

        let reducer_state = match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                key: admin_document_reducer_state_key(&AdminDocumentTarget::RealmConfig {
                    realm_id,
                }),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => postcard::from_bytes::<AdminDocumentReducerState>(&bytes).unwrap(),
            other => panic!("unexpected realm config reducer state read result: {other:?}"),
        };
        assert_eq!(
            reducer_state.materialized_realm_config_nodes()[&bootstrap_node_id],
            RealmNodeKind::Server
        );

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_rejects_secret() {
        // An initial-administrator secret must not onboard a node or be consumed.
        let (state, realm_id, _seed, _user_id, net_handle, _tempdir) =
            setup_management_state().await;

        let enrollment_id = Ulid::generate();
        let secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id,
            secret: [11u8; 32],
            mode: OnboardingMode::Local,
            realm_id,
            purpose: OnboardingPurpose::InitialAdministrator,
        };
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: secret.secret_hash(),
                    mode: OnboardingMode::Local,
                    purpose: OnboardingPurpose::InitialAdministrator,
                    expires_at: u64::MAX,
                    claimed_node_id: None,
                },
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let encoded = secret.encode().unwrap();
        let node_proof = SigningKey::from_bytes(&[9u8; 32]);
        let bootstrap_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
        let node_id = bootstrap_node_id.to_string();
        let node_signature = node_proof
            .sign(&bootstrap_node_proof_message(&encoded, &node_id, None))
            .to_string();

        let result = bootstrap_onboarding(
            State(state.clone()),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: encoded,
                node_id,
                node_proof: node_signature,
                transport_public_key: None,
                issuer_public_key: None,
                issuer_proof: None,
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));

        let entries = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
            .await
            .unwrap();
        let entry = entries
            .iter()
            .find(|entry| entry.record.enrollment_id == enrollment_id)
            .expect("secret still present");
        assert!(matches!(entry.state, OnboardingSecretState::Available));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn list_and_revoke_onboarding_secrets() {
        let (state, realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: OnboardingMode::Local,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let (_, Json(listed)) =
            list_onboarding_secrets(State(state.clone()), Extension(Some(auth.clone())))
                .await
                .unwrap();
        assert_eq!(listed.secrets.len(), 1);

        let secret =
            aruna_core::onboarding::OnboardingSecret::decode(&created.onboarding_secret).unwrap();
        let status = revoke_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth)),
            Path(secret.enrollment_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (_, Json(listed)) = list_onboarding_secrets(
            State(state),
            Extension(Some(AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            })),
        )
        .await
        .unwrap();
        assert!(listed.secrets.is_empty());

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn list_prunes_expired_available_but_keeps_expired_finalizing_secret() {
        let (state, realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let finalizing_id = Ulid::generate();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: finalizing_id,
                    secret_hash: "finalizing".to_string(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 1,
                    claimed_node_id: None,
                },
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        drive(
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
                enrollment_id: finalizing_id,
                secret_hash: "finalizing".to_string(),
                node_id: "node-a".to_string(),
                now: 1,
                reservation_expires_at: 2,
                finalizing: true,
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let stale_id = Ulid::generate();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: stale_id,
                    secret_hash: "stale".to_string(),
                    mode: OnboardingMode::Local,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 1,
                    claimed_node_id: None,
                },
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let (_, Json(listed)) =
            list_onboarding_secrets(State(state.clone()), Extension(Some(auth)))
                .await
                .unwrap();
        assert_eq!(listed.secrets.len(), 1);
        assert_eq!(listed.secrets[0].enrollment_id, finalizing_id.to_string());
        assert_eq!(listed.secrets[0].claimed_node_id.as_deref(), Some("node-a"));

        let entries = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].record.enrollment_id, finalizing_id);

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_issuer_proof_does_not_consume_secret() {
        let (state, realm_id, _seed_node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: OnboardingMode::Server,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let node_proof = SigningKey::from_bytes(&[5u8; 32]);
        let joiner_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
        let joiner_node_id_string = joiner_node_id.to_string();
        let node_signature = node_proof
            .sign(&bootstrap_node_proof_message(
                &created.onboarding_secret,
                &joiner_node_id_string,
                None,
            ))
            .to_string();

        let issuer_key = generate_signing_key();
        let issuer_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_key.verifying_key().to_bytes());
        let onboarding_secret = created.onboarding_secret;

        let result = bootstrap_onboarding(
            State(state.clone()),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: onboarding_secret.clone(),
                node_id: joiner_node_id_string.clone(),
                node_proof: node_signature.clone(),
                transport_public_key: None,
                issuer_public_key: Some(issuer_public_key.clone()),
                issuer_proof: Some("invalid-signature".to_string()),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Unauthorized)));

        let (_, Json(listed)) =
            list_onboarding_secrets(State(state.clone()), Extension(Some(auth)))
                .await
                .unwrap();
        assert_eq!(listed.secrets.len(), 1);

        let issuer_signature = issuer_key
            .sign(&bootstrap_issuer_proof_message(
                &onboarding_secret,
                &joiner_node_id_string,
                &issuer_public_key,
            ))
            .to_string();
        let result = bootstrap_onboarding(
            State(state),
            Json(BootstrapOnboardingRequest {
                onboarding_secret,
                node_id: joiner_node_id_string,
                node_proof: node_signature,
                transport_public_key: None,
                issuer_public_key: Some(issuer_public_key),
                issuer_proof: Some(issuer_signature),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await;
        assert!(result.is_ok());

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn management_bootstrap_wraps_realm_key() {
        let (state, realm_id, _seed_node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth)),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: OnboardingMode::Management,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let joiner_node_key = SigningKey::from_bytes(&[11u8; 32]);
        let joiner_node_id = iroh::SecretKey::from_bytes(&joiner_node_key.to_bytes()).public();
        let joiner_node_id_string = joiner_node_id.to_string();
        let transport_secret_key = TransportSecretKey::generate(&mut crypto_box::aead::OsRng);
        let transport_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(transport_secret_key.public_key().as_bytes());
        let node_signature = joiner_node_key
            .sign(&bootstrap_node_proof_message(
                &created.onboarding_secret,
                &joiner_node_id_string,
                Some(&transport_public_key),
            ))
            .to_string();

        let (_, Json(bootstrap)) = bootstrap_onboarding(
            State(state),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: created.onboarding_secret,
                node_id: joiner_node_id_string,
                node_proof: node_signature,
                transport_public_key: Some(transport_public_key),
                issuer_public_key: None,
                issuer_proof: None,
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await
        .unwrap();

        let sender_public_key = TransportPublicKey::from(
            <[u8; 32]>::try_from(
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(bootstrap.wrapping_public_key.unwrap())
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
        );
        let cipher = SalsaBox::new(&sender_public_key, &transport_secret_key);
        let nonce_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(bootstrap.wrapped_realm_private_key_nonce.unwrap())
            .unwrap();
        let ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(bootstrap.wrapped_realm_private_key.unwrap())
            .unwrap();
        let nonce = crypto_box::Nonce::from(<[u8; 24]>::try_from(nonce_bytes.as_slice()).unwrap());
        let plaintext = cipher.decrypt(&nonce, ciphertext.as_ref()).unwrap();
        let pem = String::from_utf8(plaintext).unwrap();
        assert!(pem.contains("BEGIN PRIVATE KEY"));

        net_handle.shutdown().await;
    }
}
