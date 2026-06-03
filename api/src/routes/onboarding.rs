use crate::error::{ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, CreateOnboardingSecretRequest,
    CreateOnboardingSecretResponse, OnboardingMode, OnboardingSecret, OnboardingSecretRecord,
    bootstrap_issuer_proof_message, bootstrap_node_proof_message,
};
use aruna_core::structs::{Actor, AuthContext, Permission, RealmConfigDocument, RealmNodeKind};
use aruna_core::types::UserId;
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::consume_onboarding_secret::{
    ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use aruna_operations::create_onboarding_secret::{
    CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::delete_onboarding_secret::{
    DeleteOnboardingSecretError, DeleteOnboardingSecretInput, DeleteOnboardingSecretOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretError, InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::list_onboarding_secrets::ListOnboardingSecretsOperation;
use aruna_operations::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use aruna_operations::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsOperation,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use byteview::ByteView;
use crypto_box::{
    PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey,
    aead::{Aead, AeadCore, OsRng as CryptoOsRng},
};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rand::Rng;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

const DEFAULT_ONBOARDING_SECRET_TTL_SECS: u64 = 3600;
const REALM_NODE_UPDATE_RETRIES: usize = 5;

#[derive(OpenApi)]
#[openapi(
    tags((name = "onboarding", description = "Node onboarding and bootstrap operations")),
    paths(
        create_onboarding_secret,
        list_onboarding_secrets,
        revoke_onboarding_secret,
        bootstrap_onboarding
    )
)]
pub struct OnboardingApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/onboarding/bootstrap", post(bootstrap_onboarding))
        .route("/admin/onboarding/secrets", post(create_onboarding_secret))
        .route("/admin/onboarding/secrets", get(list_onboarding_secrets))
        .route(
            "/admin/onboarding/secrets/{id}",
            delete(revoke_onboarding_secret),
        )
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

    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{realm_id}/admin/onboarding"),
            required_permission: Permission::WRITE,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    if !allowed {
        return Err(ServerError::Forbidden);
    }

    Ok(auth)
}

async fn prune_stale_onboarding_secrets(state: &Arc<ServerState>) -> ServerResult<()> {
    let now = now_timestamp();
    let secrets = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    for secret in secrets {
        if secret.expires_at < now {
            drive(
                DeleteOnboardingSecretOperation::new(DeleteOnboardingSecretInput {
                    enrollment_id: secret.enrollment_id,
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
    request_body = CreateOnboardingSecretRequestDoc,
    responses(
        (status = 201, description = "Onboarding secret created", body = CreateOnboardingSecretResponseDoc),
        (status = 401, description = "Unauthorized", body = crate::error::ErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::ErrorResponse)
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
        enrollment_id: ulid::Ulid::new(),
        secret: secret_bytes,
        mode: request.mode,
    };
    let encoded_secret = onboarding_secret
        .encode()
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let record = OnboardingSecretRecord {
        enrollment_id: onboarding_secret.enrollment_id,
        secret_hash: blake3::hash(&onboarding_secret.secret).to_string(),
        mode: onboarding_secret.mode,
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
    responses(
        (status = 200, description = "List onboarding secrets", body = ListOnboardingSecretsResponse),
        (status = 401, description = "Unauthorized", body = crate::error::ErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::ErrorResponse)
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
    secrets.sort_by_key(|record| record.expires_at);

    Ok((
        StatusCode::OK,
        Json(ListOnboardingSecretsResponse {
            secrets: secrets
                .into_iter()
                .map(OnboardingSecretSummary::from)
                .collect(),
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/admin/onboarding/secrets/{id}",
    tag = "onboarding",
    params(("id" = String, Path, description = "Onboarding secret enrollment id")),
    responses(
        (status = 204, description = "Secret revoked"),
        (status = 401, description = "Unauthorized", body = crate::error::ErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::ErrorResponse),
        (status = 404, description = "Secret not found", body = crate::error::ErrorResponse)
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
    request_body = BootstrapOnboardingRequestDoc,
    responses(
        (status = 200, description = "Bootstrap material for joiner", body = BootstrapOnboardingResponseDoc),
        (status = 400, description = "Invalid request", body = crate::error::ErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::ErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::ErrorResponse)
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
            secret_hash: blake3::hash(&onboarding_secret.secret).to_string(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_inspect_error)?;

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

    let record = drive(
        ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: blake3::hash(&onboarding_secret.secret).to_string(),
            node_id: node_id.to_string(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_consume_error)?;

    let bootstrap_endpoint = state
        .bootstrap_endpoint()
        .ok_or_else(|| ServerError::InternalError("net handle unavailable".to_string()))?;
    ensure_realm_node(&state, node_id, record.mode).await?;
    let ctx = state.get_ctx();
    if let Some(net_handle) = ctx.net_handle.as_ref() {
        net_handle
            .reload_realm_peers()
            .await
            .map_err(|error| ServerError::InternalError(error.to_string()))?;
    }
    let replication_ctx = ctx.clone();
    let realm_id = state.get_realm_id();
    let local_node_id = state.get_node_id();
    tokio::spawn(async move {
        if let Err(error) = drive(
            ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
                realm_id,
                local_node_id,
                excluded_peers: vec![node_id],
                documents: vec![DocumentSyncTarget::RealmConfig { realm_id }],
            }),
            replication_ctx.as_ref(),
        )
        .await
        {
            warn!(error = ?error, "Failed to queue realm config replication during onboarding");
        }
    });

    let placement_ctx = ctx.clone();
    let realm_id = state.get_realm_id();
    let local_node_id = state.get_node_id();
    tokio::spawn(async move {
        if let Err(error) = drive(
            ProcessPlacementsOperation::new(PlacementConfig {
                realm_id,
                local_node_id,
            }),
            placement_ctx.as_ref(),
        )
        .await
        {
            warn!(error = ?error, "Failed to process pending topic placements during onboarding");
        }
    });

    let onboarding_sync_ticket = state
        .issue_onboarding_sync_ticket(node_id)
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?
        .encode()
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
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
    let response = match record.mode {
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
            onboarding_sync_ticket,
        },
        OnboardingMode::Server => {
            let issuer_public_key = request
                .issuer_public_key
                .as_deref()
                .ok_or(ServerError::BadRequest)?;
            let delegation_signature = state
                .sign_server_delegation(issuer_public_key)
                .ok_or(ServerError::Forbidden)?;
            BootstrapOnboardingResponse {
                realm_id: state.get_realm_id().to_string(),
                mode: OnboardingMode::Server,
                temporary_bootstrap_endpoint: bootstrap_endpoint,
                wrapped_realm_private_key: None,
                wrapped_realm_private_key_nonce: None,
                wrapping_public_key: None,
                delegation_signature: Some(delegation_signature),
                onboarding_sync_ticket,
            }
        }
        OnboardingMode::Local => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Local,
            temporary_bootstrap_endpoint: bootstrap_endpoint,
            wrapped_realm_private_key: None,
            wrapped_realm_private_key_nonce: None,
            wrapping_public_key: None,
            delegation_signature: None,
            onboarding_sync_ticket,
        },
    };

    Ok((StatusCode::OK, Json(response)))
}

async fn ensure_realm_node(
    state: &Arc<ServerState>,
    node_id: NodeId,
    mode: OnboardingMode,
) -> ServerResult<()> {
    let mut last_conflict = None;
    for _ in 0..REALM_NODE_UPDATE_RETRIES {
        match ensure_realm_node_once(state, node_id, mode).await {
            Ok(()) => return Ok(()),
            Err(ServerError::InternalError(message))
                if message == StorageError::TransactionConflict.to_string() =>
            {
                last_conflict = Some(message);
                continue;
            }
            Err(error) => return Err(error),
        }
    }

    Err(ServerError::InternalError(last_conflict.unwrap_or_else(
        || "realm config update conflicted".to_string(),
    )))
}

async fn ensure_realm_node_once(
    state: &Arc<ServerState>,
    node_id: NodeId,
    mode: OnboardingMode,
) -> ServerResult<()> {
    let realm_id = state.get_realm_id();
    let key = ByteView::from(*realm_id.as_bytes());
    let ctx = state.get_ctx();
    let txn_id = match ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        }))
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(ServerError::InternalError(error.to_string()));
        }
        other => {
            return Err(ServerError::InternalError(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let current = match ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => value,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_realm_node_update(&ctx.storage_handle, txn_id).await;
            return Err(ServerError::InternalError(
                "realm config document missing".to_string(),
            ));
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_realm_node_update(&ctx.storage_handle, txn_id).await;
            return Err(ServerError::InternalError(error.to_string()));
        }
        other => {
            abort_realm_node_update(&ctx.storage_handle, txn_id).await;
            return Err(ServerError::InternalError(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let mut document = match RealmConfigDocument::from_bytes(&current) {
        Ok(document) => document,
        Err(error) => {
            abort_realm_node_update(&ctx.storage_handle, txn_id).await;
            return Err(ServerError::InternalError(error.to_string()));
        }
    };
    let kind = match mode {
        OnboardingMode::Management => RealmNodeKind::Management,
        OnboardingMode::Server => RealmNodeKind::Server,
        OnboardingMode::Local => RealmNodeKind::Local,
    };
    let node_id_string = node_id.to_string();
    if let Some(existing) = document
        .nodes
        .iter()
        .find(|node| node.node_id == node_id_string)
        && existing.kind != kind
    {
        abort_realm_node_update(&ctx.storage_handle, txn_id).await;
        return Err(ServerError::BadRequest);
    }
    document.ensure_node(node_id, kind);

    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    let value = match document.reconcile_bytes(Some(&current), &actor) {
        Ok(value) => value,
        Err(error) => {
            abort_realm_node_update(&ctx.storage_handle, txn_id).await;
            return Err(ServerError::InternalError(error.to_string()));
        }
    };

    match ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key,
            value: ByteView::from(value),
            txn_id: Some(txn_id),
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            abort_realm_node_update(&ctx.storage_handle, txn_id).await;
            return Err(ServerError::InternalError(error.to_string()));
        }
        other => {
            abort_realm_node_update(&ctx.storage_handle, txn_id).await;
            return Err(ServerError::InternalError(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    }

    match ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(ServerError::InternalError(error.to_string()))
        }
        other => Err(ServerError::InternalError(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

async fn abort_realm_node_update<H: Handle>(storage: &H, txn_id: Ulid) {
    let _ = storage
        .send_effect(Effect::Storage(StorageEffect::AbortTransaction { txn_id }))
        .await;
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
        revoke_onboarding_secret,
    };
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::onboarding::{
        BootstrapOnboardingRequest, CreateOnboardingSecretRequest, OnboardingMode,
        bootstrap_issuer_proof_message, bootstrap_node_proof_message,
    };
    use aruna_core::structs::{Actor, AuthContext, NodeCapabilities, RealmConfigDocument, RealmId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
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
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::new(), realm_id);
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
            )
            .await,
        );

        (state, realm_id, node_id, user_id, net_handle, tempdir)
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

        let issuer_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
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

        let issuer_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
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
