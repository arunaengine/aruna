use crate::auth::{OidcTokenSelector, OidcValidator};
use crate::error::{OidcError, TokenError};
use crate::openapi::ApiDoc;
use aruna_core::NodeId;
use aruna_core::auth::{TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY, bearer_token_hash};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, USER_KEYSPACE};
use aruna_core::onboarding::{OnboardingSecretError, OnboardingSyncTicket};
use aruna_core::structs::{Actor, AuthContext, NodeCapabilities, OidcProviderConfig, RealmId};
use aruna_operations::auth::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, decoding_key_from_base64_public_key,
};
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminError, ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    ClaimInitialRealmAdminResult,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::issue_onboarding_sync_ticket::{
    IssueOnboardingSyncTicketInput, IssueOnboardingSyncTicketOperation,
    ONBOARDING_SYNC_TICKET_TTL_SECS,
};
use async_trait::async_trait;
use byteview::ByteView;
use ed25519_dalek::Signer;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use iroh::EndpointAddr;
use jsonwebtoken::DecodingKey;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tracing::warn;
use utoipa::ToSchema;
use utoipa_swagger_ui::SwaggerUi;

pub const INITIAL_REALM_ADMIN_CLAIMED_KEY: &[u8] = b"initial_realm_admin_claimed";
pub const INITIAL_LOCAL_ONBOARDING_SECRET_KEY: &[u8] = b"initial_local_onboarding_secret";
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
    // Contains OIDC config and Client
    oidc_validator: Option<Arc<OidcValidator>>,
    interface_state: Arc<RwLock<InterfaceRuntimeState>>,
    portal: Arc<RwLock<PortalRuntimeState>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PortalStatus {
    pub installed: bool,
    pub mode: String,
    pub version: Option<String>,
    pub source: Option<String>,
    pub url: Option<String>,
    pub checksum: Option<String>,
    pub fetched_at: Option<String>,
    pub last_error: Option<String>,
}

impl Default for PortalStatus {
    fn default() -> Self {
        Self {
            installed: false,
            mode: "disabled".to_string(),
            version: None,
            source: None,
            url: None,
            checksum: None,
            fetched_at: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PortalRuntimeState {
    pub status: PortalStatus,
    pub portal_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub struct InterfaceRuntimeState {
    pub rest: Option<RestInterfaceRuntime>,
    pub s3: Option<S3InterfaceRuntime>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestInterfaceRuntime {
    pub bind_address: SocketAddr,
    pub base_url: String,
    pub api_base_url: String,
    pub info_url: String,
    pub swagger_ui_url: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3InterfaceRuntime {
    pub bind_address: SocketAddr,
    pub base_url: String,
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
        trusted_realms.insert(realm_id);
        let state = Self {
            driver_ctx,
            realm_id,
            node_id,
            oidc_validator,
            node_capabilities,
            token_revocation_list: Arc::new(RwLock::new(token_revocation_list)),
            trusted_realms_list: Arc::new(RwLock::new(trusted_realms)),
            issuer_keys: Arc::new(RwLock::new(HashMap::default())),
            initial_admin_claim,
            interface_state: Arc::new(RwLock::new(InterfaceRuntimeState::default())),
            portal: Arc::new(RwLock::new(PortalRuntimeState::default())),
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
        self.realm_id
    }

    pub fn get_node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn node_capabilities(&self) -> &NodeCapabilities {
        &self.node_capabilities
    }

    pub fn oidc_validator(&self) -> Result<&OidcValidator, OidcError> {
        self.oidc_validator
            .as_deref()
            .ok_or(OidcError::NotConfigured)
    }

    pub async fn register_rest_interface(&self, bind_address: SocketAddr) {
        let mut interface_state = self.interface_state.write().await;
        interface_state.rest = Some(RestInterfaceRuntime::from_bind_address(bind_address));
    }

    pub async fn register_s3_interface(&self, bind_address: SocketAddr, advertised_host: &str) {
        let mut interface_state = self.interface_state.write().await;
        interface_state.s3 = Some(S3InterfaceRuntime {
            bind_address,
            base_url: client_base_url_from_advertised_host(advertised_host, bind_address),
        });
    }

    pub async fn interface_state(&self) -> InterfaceRuntimeState {
        self.interface_state.read().await.clone()
    }

    pub async fn portal_status(&self) -> PortalStatus {
        self.portal.read().await.status.clone()
    }

    pub async fn portal_runtime_state(&self) -> PortalRuntimeState {
        self.portal.read().await.clone()
    }

    pub async fn set_portal_status(&self, status: PortalStatus) {
        let mut portal = self.portal.write().await;
        if !status.installed {
            portal.portal_dir = None;
        }
        portal.status = status;
    }

    pub async fn set_portal_dir(&self, status: PortalStatus, portal_dir: PathBuf) {
        let mut portal = self.portal.write().await;
        portal.portal_dir = status.installed.then_some(portal_dir);
        portal.status = status;
    }

    pub async fn load_metadata_realm_nodes(&self) -> Vec<NodeId> {
        aruna_operations::metadata::api::load_metadata_realm_nodes(
            self.driver_ctx.as_ref(),
            self.realm_id,
            self.node_id,
        )
        .await
    }

    pub async fn get_oidc_provider_by_token(
        &self,
        selector: &OidcTokenSelector,
    ) -> Result<OidcProviderConfig, OidcError> {
        let config = drive(
            GetRealmConfigOperation::new(self.realm_id),
            &self.driver_ctx,
        )
        .await
        .map_err(|error| OidcError::Internal(error.to_string()))?;
        config
            .oidc_providers
            .into_iter()
            .find(|provider| {
                provider.issuer == selector.issuer && selector.matches_audience(&provider.audience)
            })
            .ok_or(OidcError::ProviderNotFound)
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
            } => Some(
                realm_signing_key
                    .sign(issuer_public_key.as_bytes())
                    .to_string(),
            ),
            _ => None,
        }
    }

    pub async fn issue_onboarding_sync_ticket(
        &self,
        node_id: NodeId,
    ) -> Result<OnboardingSyncTicket, OnboardingSecretError> {
        match &self.node_capabilities {
            NodeCapabilities::Management {
                realm_signing_key, ..
            } => drive(
                IssueOnboardingSyncTicketOperation::new(IssueOnboardingSyncTicketInput {
                    realm_signing_key: realm_signing_key.clone(),
                    realm_id: self.realm_id,
                    node_id,
                    issuer_node_id: self.node_id,
                    now: chrono::Utc::now().timestamp().max(0) as u64,
                    ttl_secs: ONBOARDING_SYNC_TICKET_TTL_SECS,
                }),
                &self.driver_ctx,
            )
            .await
            .map_err(|_| OnboardingSecretError::InvalidSecret),
            _ => Err(OnboardingSecretError::InvalidSecret),
        }
    }

    pub async fn get_cached_pubkey(&self, pubkey: String) -> Result<DecodingKey, TokenError> {
        <Self as ArunaBearerTokenValidationState>::decoding_key_for_issuer(self, &pubkey)
            .await
            .map_err(Into::into)
    }

    pub async fn add_token_to_blacklist(&self, token: &str) {
        let hash = bearer_token_hash(token);
        self.token_revocation_list.write().await.insert(hash);
        self.persist_token_revocation_list().await;
    }
    pub async fn add_trusted_realm(&self, realm_id: RealmId) {
        self.trusted_realms_list.write().await.insert(realm_id);
        self.persist_trusted_realms().await;
    }

    pub async fn is_token_blacklisted(&self, token: &str) -> bool {
        let hash = bearer_token_hash(token);
        self.token_revocation_list.read().await.get(&hash).is_some()
    }

    pub async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.trusted_realms_list
            .read()
            .await
            .get(realm_id)
            .is_some()
    }

    pub async fn user_exists(&self, user_id: aruna_core::UserId) -> Result<bool, StorageError> {
        match self
            .driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: USER_KEYSPACE.to_string(),
                key: ByteView::from(user_id.to_bytes()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
            Event::Storage(StorageEvent::Error { error }) => Err(error),
            _ => Err(StorageError::InvalidEffect),
        }
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
                        realm_id: auth.realm_id,
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
                Err(ClaimInitialRealmAdminError::StorageError(
                    StorageError::TransactionConflict,
                )) => {
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

#[async_trait]
impl ArunaBearerTokenValidationState for ServerState {
    async fn is_bearer_token_revoked(&self, token_hash: &str) -> bool {
        self.token_revocation_list.read().await.contains(token_hash)
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.trusted_realms_list.read().await.contains(realm_id)
    }

    async fn decoding_key_for_issuer(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        let read_lock = self.issuer_keys.read().await;
        let key = read_lock.get(issuer_pubkey).cloned();
        drop(read_lock);
        if let Some(key) = key {
            return Ok(key);
        }

        let decoding_key = decoding_key_from_base64_public_key(issuer_pubkey)?;
        self.issuer_keys
            .write()
            .await
            .insert(issuer_pubkey.to_string(), decoding_key.clone());
        Ok(decoding_key)
    }
}

pub async fn load_persisted_state<T>(driver_ctx: &DriverContext, key: &[u8]) -> Option<T>
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

pub async fn persist_state<T>(driver_ctx: &DriverContext, key: &[u8], value: &T)
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

impl RestInterfaceRuntime {
    pub fn from_bind_address(bind_address: SocketAddr) -> Self {
        let base_url = client_base_url_from_bind_address(bind_address);
        Self {
            bind_address,
            api_base_url: format!("{base_url}/api/v1"),
            info_url: format!("{base_url}/api/v1/info"),
            swagger_ui_url: format!("{base_url}/swagger-ui"),
            base_url,
        }
    }
}

pub fn client_base_url_from_bind_address(bind_address: SocketAddr) -> String {
    format!(
        "http://{}:{}",
        client_host_from_ip(bind_address.ip()),
        bind_address.port()
    )
}

pub fn client_base_url_from_advertised_host(
    advertised_host: &str,
    bind_address: SocketAddr,
) -> String {
    let host = match advertised_host.trim() {
        "" => return client_base_url_from_bind_address(bind_address),
        host => {
            if host.contains("://") {
                return host.trim_end_matches('/').to_string();
            }

            if let Ok(addr) = host.parse::<SocketAddr>() {
                return format!("http://{}:{}", client_host_from_ip(addr.ip()), addr.port());
            }

            if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                return format!("http://{}:{}", client_host_from_ip(ip), bind_address.port());
            }

            host
        }
    };

    format!("http://{host}")
}

fn client_host_from_ip(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            std::net::Ipv4Addr::LOCALHOST.to_string()
        }
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => {
            format!("[{}]", std::net::Ipv6Addr::LOCALHOST)
        }
        std::net::IpAddr::V6(ip) => format!("[{ip}]"),
        std::net::IpAddr::V4(ip) => ip.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{client_base_url_from_advertised_host, client_base_url_from_bind_address};

    #[test]
    fn client_base_url_rewrites_unspecified_ipv6() {
        assert_eq!(
            client_base_url_from_bind_address("[::]:3000".parse().unwrap()),
            "http://[::1]:3000"
        );
    }

    #[test]
    fn s3_base_url_normalizes_advertised_wildcards() {
        assert_eq!(
            client_base_url_from_advertised_host("0.0.0.0", "0.0.0.0:1337".parse().unwrap()),
            "http://127.0.0.1:1337"
        );
        assert_eq!(
            client_base_url_from_advertised_host("::", "[::]:1337".parse().unwrap()),
            "http://[::1]:1337"
        );
    }

    #[test]
    fn s3_base_url_preserves_explicit_authority() {
        assert_eq!(
            client_base_url_from_advertised_host("127.0.0.1:1337", "0.0.0.0:9999".parse().unwrap()),
            "http://127.0.0.1:1337"
        );
        assert_eq!(
            client_base_url_from_advertised_host(
                "s3.node-1.v3.aruna-engine.org",
                "0.0.0.0:1337".parse().unwrap()
            ),
            "http://s3.node-1.v3.aruna-engine.org"
        );
    }

    #[test]
    fn s3_base_url_preserves_explicit_scheme() {
        assert_eq!(
            client_base_url_from_advertised_host(
                "https://s3.node-1.v3.aruna-engine.org",
                "0.0.0.0:1337".parse().unwrap()
            ),
            "https://s3.node-1.v3.aruna-engine.org"
        );
        assert_eq!(
            client_base_url_from_advertised_host(
                "https://s3.node-1.v3.aruna-engine.org/",
                "0.0.0.0:1337".parse().unwrap()
            ),
            "https://s3.node-1.v3.aruna-engine.org"
        );
    }
}
