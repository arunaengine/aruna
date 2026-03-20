use crate::auth::OidcValidator;
use crate::error::TokenError;
use crate::openapi::ApiDoc;
use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::API_STATE_KEYSPACE;
use aruna_core::structs::{Actor, AuthContext, NodeCapabilities, RealmId};
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminError, ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    ClaimInitialRealmAdminResult,
};
use aruna_operations::driver::{DriverContext, drive};
use base64::Engine;
use byteview::ByteView;
use ed25519_dalek::Signer;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use iroh::EndpointAddr;
use jsonwebtoken::DecodingKey;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

const TOKEN_REVOCATION_LIST_KEY: &[u8] = b"token_revocation_list";
const TRUSTED_REALMS_LIST_KEY: &[u8] = b"trusted_realms_list";
const INITIAL_REALM_ADMIN_CLAIMED_KEY: &[u8] = b"initial_realm_admin_claimed";

#[derive(Clone, Debug)]
pub struct ServerState {
    // Contains neccessary drivers for request handling
    driver_ctx: Arc<DriverContext>,
    // Capabilities defined as in spec: Membership, Server and Local node capabilities
    node_capabilities: NodeCapabilities,
    // Base64 encoded issuer pubkeys and jsonwebtoken serialized DecodingKeys
    issuer_keys: Arc<RwLock<HashMap<String, DecodingKey, ahash::RandomState>>>,
    // Contains token id as a string, so also invalid ids get banned
    token_revocation_list: Arc<RwLock<HashSet<String, ahash::RandomState>>>,
    // Contains trusted realms
    trusted_realms_list: Arc<RwLock<HashSet<RealmId, ahash::RandomState>>>,
    initial_admin_claim: Option<Arc<AtomicBool>>,
    // Realm membership
    realm_id: RealmId,
    // Realm membership
    node_id: NodeId,
    // TODO: OIDC handling
    _oidc_validator: Option<Arc<OidcValidator>>,
}

impl ServerState {
    pub async fn new(
        driver_ctx: Arc<DriverContext>,
        realm_id: RealmId,
        node_id: NodeId,
        node_capabilities: NodeCapabilities,
        claim_initial_admin_enabled: bool,
        oidc_validator: Option<Arc<OidcValidator>>,
    ) -> Self {
        let token_revocation_list = load_persisted_state::<HashSet<String, ahash::RandomState>>(
            driver_ctx.as_ref(),
            TOKEN_REVOCATION_LIST_KEY,
        )
        .await
        .unwrap_or_default();
        let mut trusted_realms = load_persisted_state::<HashSet<RealmId, ahash::RandomState>>(
            driver_ctx.as_ref(),
            TRUSTED_REALMS_LIST_KEY,
        )
        .await
        .unwrap_or_default();
        let initial_admin_claim = if claim_initial_admin_enabled {
            Some(Arc::new(AtomicBool::new(
                load_persisted_state::<bool>(driver_ctx.as_ref(), INITIAL_REALM_ADMIN_CLAIMED_KEY)
                    .await
                    .unwrap_or(false),
            )))
        } else {
            None
        };
        trusted_realms.insert(realm_id.clone());
        let state = Self {
            driver_ctx,
            realm_id,
            node_id,
            _oidc_validator: oidc_validator,
            node_capabilities,
            token_revocation_list: Arc::new(RwLock::new(token_revocation_list)),
            trusted_realms_list: Arc::new(RwLock::new(trusted_realms)),
            issuer_keys: Arc::new(RwLock::new(HashMap::default())),
            initial_admin_claim,
        };
        state.persist_trusted_realms().await;
        state
    }
    pub fn get_ctx(&self) -> Arc<DriverContext> {
        self.driver_ctx.clone()
    }
    pub fn get_pubkey(&self) -> [u8; 113] {
        match self.node_capabilities {
            NodeCapabilities::Management {
                realm_verifying_key,
                ..
            } => realm_verifying_key,
            NodeCapabilities::Server {
                realm_verifying_key,
                ..
            } => realm_verifying_key,
            NodeCapabilities::Local {
                realm_verifying_key,
            } => realm_verifying_key,
        }
    }

    pub fn get_realm_id(&self) -> RealmId {
        self.realm_id.clone()
    }

    pub fn get_node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn is_management_node(&self) -> bool {
        matches!(self.node_capabilities, NodeCapabilities::Management { .. })
    }

    pub fn bootstrap_endpoint(&self) -> Option<EndpointAddr> {
        self.driver_ctx
            .net_handle
            .as_ref()
            .map(|net_handle| net_handle.endpoint_addr())
    }

    pub fn realm_private_key_pem(&self) -> Option<String> {
        match &self.node_capabilities {
            NodeCapabilities::Management {
                realm_signing_key, ..
            } => realm_signing_key
                .to_pkcs8_pem(LineEnding::default())
                .ok()
                .map(|pem| pem.to_string()),
            _ => None,
        }
    }

    pub fn sign_server_delegation(&self, issuer_public_key: &str) -> Option<String> {
        match &self.node_capabilities {
            NodeCapabilities::Management {
                realm_signing_key, ..
            } => Some(realm_signing_key.sign(issuer_public_key.as_bytes()).to_string()),
            _ => None,
        }
    }

    pub async fn get_cached_pubkey(&self, pubkey: String) -> Result<DecodingKey, TokenError> {
        // Just to be double sure this is not producing deadlocks
        let read_lock = self.issuer_keys.read().await;
        let key = read_lock.get(&pubkey).cloned();
        drop(read_lock);
        if let Some(key) = key {
            return Ok(key);
        }

        let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(pubkey.clone())?
            .try_into()
            .map_err(|_| TokenError::InvalidIssuerKey)?;
        let pub_pem_key =
            VerifyingKey::from_bytes(&issuer_pubkey)?.to_public_key_pem(LineEnding::default())?;
        let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;
        self.issuer_keys
            .write()
            .await
            .insert(pubkey, decoding_key.clone());
        Ok(decoding_key)
    }

    pub async fn add_token_to_blacklist(&self, token: &str) {
        let hash = blake3::hash(token.as_bytes()).to_string();
        self.token_revocation_list.write().await.insert(hash);
        self.persist_token_revocation_list().await;
    }
    pub async fn add_trusted_realm(&self, realm_id: RealmId) {
        self.trusted_realms_list.write().await.insert(realm_id);
        self.persist_trusted_realms().await;
    }

    pub async fn is_token_blacklisted(&self, token: &str) -> bool {
        let hash = blake3::hash(token.as_bytes()).to_string();
        self.token_revocation_list.read().await.get(&hash).is_some()
    }

    pub async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.trusted_realms_list
            .read()
            .await
            .get(realm_id)
            .is_some()
    }

    pub async fn claim_initial_realm_admin(
        &self,
        auth: &AuthContext,
    ) -> Result<(), ClaimInitialRealmAdminError> {
        let Some(initial_admin_claim) = &self.initial_admin_claim else {
            return Ok(());
        };

        if auth.realm_id != self.realm_id {
            return Ok(());
        }

        if initial_admin_claim.load(Ordering::Acquire) {
            return Ok(());
        }

        for _ in 0..3 {
            let result = drive(
                ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                    actor: Actor {
                        node_id: self.node_id,
                        user_id: auth.user_id,
                        realm_id: auth.realm_id.clone(),
                    },
                }),
                &self.driver_ctx,
            )
            .await;

            match result {
                Ok(ClaimInitialRealmAdminResult::Claimed(_))
                | Ok(ClaimInitialRealmAdminResult::AlreadyClaimed) => {
                    initial_admin_claim.store(true, Ordering::Release);
                    self.persist_initial_admin_claimed().await;
                    return Ok(());
                }
                Err(ClaimInitialRealmAdminError::StorageError(StorageError::TransactionConflict)) => {
                    if initial_admin_claim.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        Err(ClaimInitialRealmAdminError::StorageError(
            StorageError::TransactionConflict,
        ))
    }

    async fn persist_token_revocation_list(&self) {
        let blacklist = self.token_revocation_list.read().await.clone();
        persist_state(
            self.driver_ctx.as_ref(),
            TOKEN_REVOCATION_LIST_KEY,
            &blacklist,
        )
        .await;
    }

    async fn persist_trusted_realms(&self) {
        let trusted_realms = self.trusted_realms_list.read().await.clone();
        persist_state(
            self.driver_ctx.as_ref(),
            TRUSTED_REALMS_LIST_KEY,
            &trusted_realms,
        )
        .await;
    }

    async fn persist_initial_admin_claimed(&self) {
        let Some(initial_admin_claim) = &self.initial_admin_claim else {
            return;
        };
        let claimed = initial_admin_claim.load(Ordering::Acquire);
        persist_state(
            self.driver_ctx.as_ref(),
            INITIAL_REALM_ADMIN_CLAIMED_KEY,
            &claimed,
        )
        .await;
    }
}

async fn load_persisted_state<T>(driver_ctx: &DriverContext, key: &[u8]) -> Option<T>
where
    T: DeserializeOwned,
{
    match driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => match postcard::from_bytes(&bytes) {
            Ok(value) => Some(value),
            Err(error) => {
                warn!(error = %error, "Failed to decode persisted API state");
                None
            }
        },
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to load persisted API state");
            None
        }
        _ => None,
    }
}

async fn persist_state<T>(driver_ctx: &DriverContext, key: &[u8], value: &T)
where
    T: Serialize,
{
    let Ok(bytes) = postcard::to_allocvec(value) else {
        warn!("Failed to serialize API state for persistence");
        return;
    };

    if let Event::Storage(StorageEvent::Error { error }) = driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(bytes),
            txn_id: None,
        }))
        .await
    {
        warn!(error = %error, "Failed to persist API state");
    }
}

/// Create the SwaggerUI router for API documentation.
///
/// Provides two separate OpenAPI specs:
/// - `/api-docs/openapi.json` - REST & Admin API
/// - `/api-docs/s3-openapi.json` - S3-compatible API
pub fn swagger_ui() -> SwaggerUi {
    SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi())
}
