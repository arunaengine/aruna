use crate::error::{ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, CreateOnboardingSecretRequest,
    CreateOnboardingSecretResponse, OnboardingMode, OnboardingSecret, OnboardingSecretRecord,
};
use aruna_core::structs::{AuthContext, Permission};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::consume_onboarding_secret::{
    ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use aruna_operations::create_onboarding_secret::{
    CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::driver::drive;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use rand::RngCore;
use std::str::FromStr;
use std::sync::Arc;

const DEFAULT_ONBOARDING_SECRET_TTL_SECS: u64 = 3600;

pub async fn create_onboarding_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateOnboardingSecretRequest>,
) -> ServerResult<(StatusCode, Json<CreateOnboardingSecretResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id || !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }

    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth,
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
        consumed: false,
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

pub async fn bootstrap_onboarding(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<BootstrapOnboardingRequest>,
) -> ServerResult<(StatusCode, Json<BootstrapOnboardingResponse>)> {
    if !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }

    let onboarding_secret = OnboardingSecret::decode(&request.onboarding_secret)
        .map_err(|_| ServerError::Unauthorized)?;
    let _node_id = NodeId::from_str(&request.node_id).map_err(|_| ServerError::BadRequest)?;

    let record = drive(
        ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: blake3::hash(&onboarding_secret.secret).to_string(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_consume_error)?;

    let bootstrap_endpoint = state
        .bootstrap_endpoint()
        .ok_or_else(|| ServerError::InternalError("net handle unavailable".to_string()))?;
    let response = match record.mode {
        OnboardingMode::Management => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Management,
            bootstrap_endpoints: vec![bootstrap_endpoint],
            realm_private_key_pem: Some(
                state
                    .realm_private_key_pem()
                    .ok_or(ServerError::Forbidden)?,
            ),
            delegation_signature: None,
        },
        OnboardingMode::Server => {
            let issuer_public_key = request
                .issuer_public_key
                .ok_or(ServerError::BadRequest)?;
            let delegation_signature = state
                .sign_server_delegation(&issuer_public_key)
                .ok_or(ServerError::Forbidden)?;
            BootstrapOnboardingResponse {
                realm_id: state.get_realm_id().to_string(),
                mode: OnboardingMode::Server,
                bootstrap_endpoints: vec![bootstrap_endpoint],
                realm_private_key_pem: None,
                delegation_signature: Some(delegation_signature),
            }
        }
        OnboardingMode::Local => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Local,
            bootstrap_endpoints: vec![bootstrap_endpoint],
            realm_private_key_pem: None,
            delegation_signature: None,
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
        | ConsumeOnboardingSecretError::AlreadyConsumed
        | ConsumeOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{bootstrap_onboarding, create_onboarding_secret};
    use crate::server_state::ServerState;
    use aruna_core::onboarding::{
        BootstrapOnboardingRequest, CreateOnboardingSecretRequest, OnboardingMode,
    };
    use aruna_core::structs::{Actor, AuthContext, NodeCapabilities, RealmId};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use axum::Extension;
    use axum::Json;
    use axum::extract::State;
    use base64::Engine;
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn setup_management_state(
    ) -> (Arc<ServerState>, RealmId, iroh::PublicKey, Ulid, NetHandle, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                use_dns_discovery: false,
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
            automerge_handle: None,
            task_handle: Some(TaskHandle::new()),
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let user_id = Ulid::new();
        let node_id = net_handle.node_id();

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id: realm_id.clone(),
                },
                realm_description: "Realm".to_string(),
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
                    realm_id: realm_id.clone(),
                },
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id.clone(),
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
        let (state, realm_id, node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id: realm_id.clone(),
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

        let (_, Json(bootstrap)) = bootstrap_onboarding(
            State(state),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: created.onboarding_secret,
                node_id: node_id.to_string(),
                issuer_public_key: Some(issuer_public_key.clone()),
            }),
        )
        .await
        .unwrap();

        assert_eq!(bootstrap.mode, OnboardingMode::Server);
        assert_eq!(bootstrap.realm_id, realm_id.to_string());
        assert_eq!(bootstrap.bootstrap_endpoints.len(), 1);
        assert_eq!(bootstrap.bootstrap_endpoints[0].id, node_id);
        assert!(bootstrap.realm_private_key_pem.is_none());
        assert!(bootstrap.delegation_signature.is_some());

        net_handle.shutdown().await;
    }
}
