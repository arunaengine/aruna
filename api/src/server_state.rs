use crate::auth::{OidcTokenSelector, OidcValidator};
use crate::error::OidcError;
use crate::openapi::ApiDoc;
use crate::routes::management_relay::ManagementUrlCache;
use aruna_core::NodeId;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::API_STATE_KEYSPACE;
use aruna_core::metrics::NodeMetrics;
use aruna_core::onboarding::{OnboardingSecretError, OnboardingTicket};
use aruna_core::structs::{
    Actor, AuthContext, NodeCapabilities, OidcProviderConfig, RealmId, RoCrateLimits,
};
use aruna_operations::auth::bearer_token::{
    ArunaBearerError, ArunaValidationState, IssuerKeyCache, realm_token_revoked,
};
use aruna_operations::device::wipe::DeviceWipe;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::onboarding::issue_ticket::{
    IssueSyncInput, IssueSyncOperation, ONBOARDING_SYNC_TICKET_TTL_SECS,
};
use aruna_operations::realm::claim_admin::{
    ClaimInitialError, ClaimInitialInput, ClaimInitialOperation, ClaimInitialResult,
};
use aruna_operations::realm::get_config::GetConfigOperation;
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use utoipa::ToSchema;
use utoipa_swagger_ui::SwaggerUi;

pub const INITIAL_REALM_ADMIN_CLAIMED_KEY: &[u8] = b"initial_realm_admin_claimed";
pub const INITIAL_LOCAL_ONBOARDING_SECRET_KEY: &[u8] = b"initial_local_onboarding_secret";
pub(crate) const ROCRATE_UPLOAD_SLOTS: usize = 32;
pub(crate) const DOWNLOAD_SLOTS: usize = 256;

/// Identity, realm trust roots, and the shared issuer-key cache. The `Arc`
/// fields are the node's shared owners: cloning the state clones these handles
/// and never recreates the guarded contents.
#[derive(Clone, Debug)]
struct IdentityState {
    // Realm membership.
    realm_id: RealmId,
    // Realm membership.
    node_id: NodeId,
    // Capabilities defined as in spec: Management, Server and User node capabilities.
    node_capabilities: NodeCapabilities,
    // Issuer-local key that encrypts S3 credential secrets at rest, derived from
    // this node's secret so it matches the S3 verifier on the same node.
    credential_encryption_key: CredentialEncryptionKey,
    // Contains OIDC config and Client.
    oidc_validator: Option<Arc<OidcValidator>>,
    // Bounded TTL + LRU cache of trusted issuer decoding keys. One cache per
    // node: every state clone must observe the same cache identity.
    issuer_keys: Arc<IssuerKeyCache>,
    // Contains trusted realms. A mutation inserts under the write guard and
    // persists the snapshot after the guard drops, so no lock crosses I/O.
    trusted_realms_list: Arc<RwLock<HashSet<RealmId, ahash::RandomState>>>,
    // One-time initial-admin latch. The claim is persisted after the flag is
    // set, so a durable claim always implies a latched flag.
    initial_admin_claim: Option<Arc<AtomicBool>>,
}

impl IdentityState {
    async fn add_trusted_realm(&self, driver_ctx: &DriverContext, realm_id: RealmId) {
        self.trusted_realms_list.write().await.insert(realm_id);
        self.persist_trusted_realms(driver_ctx).await;
    }

    async fn persist_trusted_realms(&self, driver_ctx: &DriverContext) {
        let trusted_realms = self.trusted_realms_list.read().await.clone();
        persist_state(driver_ctx, TRUSTED_REALMS_LIST_KEY, &trusted_realms).await;
    }

    async fn claim_initial_admin(
        &self,
        driver_ctx: &DriverContext,
        auth: &AuthContext,
    ) -> Result<(), ClaimInitialError> {
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
                ClaimInitialOperation::new(ClaimInitialInput {
                    actor: Actor {
                        node_id: self.node_id,
                        user_id: auth.user_id,
                        realm_id: auth.realm_id,
                    },
                }),
                driver_ctx,
            )
            .await;

            match result {
                Ok(ClaimInitialResult::Claimed(_)) | Ok(ClaimInitialResult::AlreadyClaimed) => {
                    initial_admin_claim.store(true, Ordering::Release);
                    self.persist_admin_claim(driver_ctx).await;
                    return Ok(());
                }
                Err(ClaimInitialError::StorageError(StorageError::TransactionConflict)) => {
                    if initial_admin_claim.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        Err(ClaimInitialError::StorageError(
            StorageError::TransactionConflict,
        ))
    }

    async fn persist_admin_claim(&self, driver_ctx: &DriverContext) {
        let Some(initial_admin_claim) = &self.initial_admin_claim else {
            return;
        };
        let claimed = initial_admin_claim.load(Ordering::Acquire);
        persist_state(driver_ctx, INITIAL_REALM_ADMIN_CLAIMED_KEY, &claimed).await;
    }
}

/// Request admission limits: bounded operation slots, proxy trust, and the
/// operator-configured rate limiter.
#[derive(Clone, Debug)]
struct RequestLimits {
    rocrate_limits: RoCrateLimits,
    // Peers allowed to set `x-forwarded-*`; empty means no proxy is trusted.
    trusted_proxies: Vec<ipnet::IpNet>,
    rate_limits: Arc<crate::rate_limit::ApiRateLimits>,
    // Semaphore owners are moved into the state, never recreated per clone.
    rocrate_upload_slots: Arc<Semaphore>,
    download_slots: Arc<Semaphore>,
}

/// Interface and portal runtime state, the process shutdown token, and the
/// user-node wipe latch.
#[derive(Clone, Debug)]
struct Interfaces {
    // One lock guards the rest/s3/mcp tuple: the MCP entry is derived from the
    // registered REST entry, so the three must change together.
    interface_state: Arc<RwLock<InterfaceRuntimeState>>,
    // One lock guards `status` and `portal_dir` together: a non-installed
    // status always clears the directory.
    portal: Arc<RwLock<PortalRuntimeState>>,
    // Cached management urls the management-route relay re-issues against.
    management_urls: Arc<RwLock<ManagementUrlCache>>,
    // Long-lived response streams end when this fires, so the ingress drain
    // does not have to wait for client disconnects.
    shutdown_token: CancellationToken,
    // Present only on a user node: the owner's local wipe latch.
    device_wipe: Option<Arc<DeviceWipe>>,
}

/// Outbound assistant provider connections. The client is built once with its
/// egress policy by the route that uses it and shared by every state clone.
#[derive(Clone, Debug)]
struct AssistantConnections {
    proxy_enabled: bool,
    // `None` when the platform client cannot be built; the proxy answers 500.
    client: Option<reqwest::Client>,
    // Guarded map of live per-provider refresh locks. The guard protects only
    // the map; a refresh runs under the per-provider lock, never under it.
    chatgpt_refresh_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    chatgpt_issuer: String,
    chatgpt_base_url: String,
}

/// The one server-state entry point. Cloning it shares every underlying owner
/// (driver context, semaphores, locks, caches) rather than duplicating them.
#[derive(Clone, Debug)]
pub struct ServerState {
    // Contains necessary drivers for request handling.
    driver_ctx: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    // Per-node Prometheus registry shared with the S3 server and ops listener.
    metrics: Arc<NodeMetrics>,
    identity: IdentityState,
    limits: RequestLimits,
    interfaces: Interfaces,
    assistant: AssistantConnections,
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
    pub mcp: Option<McpInterfaceRuntime>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpInterfaceRuntime {
    pub bind_address: SocketAddr,
    pub url: String,
}

impl ServerState {
    pub async fn new(
        driver_ctx: Arc<DriverContext>,
        realm_id: RealmId,
        node_id: NodeId,
        node_capabilities: NodeCapabilities,
        claim_initial_admin_enabled: bool,
        oidc_validator: Option<Arc<OidcValidator>>,
        jobs_runtime: Arc<JobsRuntime>,
    ) -> Self {
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
        let credential_encryption_key = driver_ctx
            .net_handle
            .as_ref()
            .map(|net| net.credential_encryption_key())
            .unwrap_or_else(CredentialEncryptionKey::random);
        let assistant_client =
            crate::routes::assistant::egress::outbound_client(&node_capabilities);
        let state = Self {
            driver_ctx,
            jobs_runtime,
            metrics: Arc::new(NodeMetrics::new()),
            identity: IdentityState {
                realm_id,
                node_id,
                node_capabilities,
                credential_encryption_key,
                oidc_validator,
                issuer_keys: Arc::new(IssuerKeyCache::new()),
                trusted_realms_list: Arc::new(RwLock::new(trusted_realms)),
                initial_admin_claim,
            },
            limits: RequestLimits {
                rocrate_limits: RoCrateLimits::default(),
                trusted_proxies: Vec::new(),
                rate_limits: Arc::new(crate::rate_limit::ApiRateLimits::default()),
                rocrate_upload_slots: Arc::new(Semaphore::new(ROCRATE_UPLOAD_SLOTS)),
                download_slots: Arc::new(Semaphore::new(DOWNLOAD_SLOTS)),
            },
            interfaces: Interfaces {
                interface_state: Arc::new(RwLock::new(InterfaceRuntimeState::default())),
                portal: Arc::new(RwLock::new(PortalRuntimeState::default())),
                management_urls: Arc::new(RwLock::new(ManagementUrlCache::default())),
                shutdown_token: CancellationToken::new(),
                device_wipe: None,
            },
            assistant: AssistantConnections {
                proxy_enabled: true,
                client: assistant_client,
                chatgpt_refresh_locks: Arc::new(Mutex::new(HashMap::new())),
                chatgpt_issuer: "https://auth.openai.com".to_string(),
                chatgpt_base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            },
        };
        state
            .identity
            .persist_trusted_realms(state.driver_ctx.as_ref())
            .await;
        state
    }

    pub fn with_shutdown_token(mut self, token: CancellationToken) -> Self {
        self.interfaces.shutdown_token = token;
        self
    }

    /// Hands the device plane the wipe latch the process erases through. Only a
    /// user node is given one; without it `POST /device/wipe` is unavailable.
    pub fn with_device_wipe(mut self, wipe: Arc<DeviceWipe>) -> Self {
        self.interfaces.device_wipe = Some(wipe);
        self
    }

    pub fn device_wipe(&self) -> Option<&Arc<DeviceWipe>> {
        self.interfaces.device_wipe.as_ref()
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.interfaces.shutdown_token.clone()
    }

    pub fn get_ctx(&self) -> Arc<DriverContext> {
        self.driver_ctx.clone()
    }

    pub fn metrics(&self) -> Arc<NodeMetrics> {
        self.metrics.clone()
    }

    /// Replaces the metrics registry so the REST interface, the S3 server and
    /// the ops listener share one per-node instance. Call before serving.
    pub fn with_metrics(mut self, metrics: Arc<NodeMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.limits.rocrate_limits = limits;
        self
    }

    pub fn rocrate_limits(&self) -> &RoCrateLimits {
        &self.limits.rocrate_limits
    }

    pub fn with_trusted_proxies(mut self, proxies: Vec<ipnet::IpNet>) -> Self {
        self.limits.trusted_proxies = proxies;
        self
    }

    pub fn trusted_proxies(&self) -> &[ipnet::IpNet] {
        &self.limits.trusted_proxies
    }

    /// Installs operator-configured request limiters. Call before serving.
    pub fn with_rate_limits(mut self, limits: crate::rate_limit::ApiRateLimits) -> Self {
        self.limits.rate_limits = Arc::new(limits);
        self
    }

    pub fn rate_limits(&self) -> &crate::rate_limit::ApiRateLimits {
        &self.limits.rate_limits
    }

    pub(crate) fn try_rocrate_slot(&self) -> Option<OwnedSemaphorePermit> {
        self.limits
            .rocrate_upload_slots
            .clone()
            .try_acquire_owned()
            .ok()
    }

    pub(crate) fn try_acquire_download(&self) -> Option<OwnedSemaphorePermit> {
        self.limits.download_slots.clone().try_acquire_owned().ok()
    }

    pub fn jobs_runtime(&self) -> Arc<JobsRuntime> {
        self.jobs_runtime.clone()
    }
    pub fn get_realm_id(&self) -> RealmId {
        self.identity.realm_id
    }

    pub fn get_node_id(&self) -> NodeId {
        self.identity.node_id
    }

    pub fn credential_encryption_key(&self) -> &CredentialEncryptionKey {
        &self.identity.credential_encryption_key
    }

    pub fn with_assistant_proxy(mut self, enabled: bool) -> Self {
        self.assistant.proxy_enabled = enabled;
        self
    }

    pub fn assistant_proxy(&self) -> bool {
        self.assistant.proxy_enabled
    }

    pub fn assistant_client(&self) -> Option<&reqwest::Client> {
        self.assistant.client.as_ref()
    }

    pub(crate) async fn chatgpt_lock(&self, provider_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.assistant.chatgpt_refresh_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(provider_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(provider_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    pub fn chatgpt_issuer(&self) -> &str {
        &self.assistant.chatgpt_issuer
    }

    pub fn chatgpt_base_url(&self) -> &str {
        &self.assistant.chatgpt_base_url
    }

    #[cfg(test)]
    pub(crate) fn with_chatgpt_urls(mut self, issuer: String, base_url: String) -> Self {
        self.assistant.chatgpt_issuer = issuer;
        self.assistant.chatgpt_base_url = base_url;
        self
    }

    pub fn node_capabilities(&self) -> &NodeCapabilities {
        &self.identity.node_capabilities
    }

    pub fn oidc_validator(&self) -> Result<&OidcValidator, OidcError> {
        self.identity
            .oidc_validator
            .as_deref()
            .ok_or(OidcError::NotConfigured)
    }

    pub async fn register_rest_interface(&self, bind_address: SocketAddr) {
        self.register_rest_public(bind_address, None).await;
    }

    pub async fn register_rest_public(&self, bind_address: SocketAddr, public_url: Option<&str>) {
        let mut interface_state = self.interfaces.interface_state.write().await;
        interface_state.rest = Some(RestInterfaceRuntime::from_bind_address(
            bind_address,
            public_url,
        ));
    }

    pub async fn register_s3_interface(&self, bind_address: SocketAddr, advertised_host: &str) {
        let mut interface_state = self.interfaces.interface_state.write().await;
        interface_state.s3 = Some(S3InterfaceRuntime {
            bind_address,
            base_url: client_host_url(advertised_host, bind_address),
        });
    }

    pub async fn register_mcp_interface(&self) {
        let mut interface_state = self.interfaces.interface_state.write().await;
        interface_state.mcp = interface_state
            .rest
            .as_ref()
            .map(|rest| McpInterfaceRuntime {
                bind_address: rest.bind_address,
                url: format!("{}/mcp", rest.base_url),
            });
    }

    pub async fn interface_state(&self) -> InterfaceRuntimeState {
        self.interfaces.interface_state.read().await.clone()
    }

    pub async fn portal_status(&self) -> PortalStatus {
        self.interfaces.portal.read().await.status.clone()
    }

    pub async fn portal_runtime_state(&self) -> PortalRuntimeState {
        self.interfaces.portal.read().await.clone()
    }

    pub async fn set_portal_status(&self, status: PortalStatus) {
        let mut portal = self.interfaces.portal.write().await;
        if !status.installed {
            portal.portal_dir = None;
        }
        portal.status = status;
    }

    pub async fn set_portal_dir(&self, status: PortalStatus, portal_dir: PathBuf) {
        let mut portal = self.interfaces.portal.write().await;
        portal.portal_dir = status.installed.then_some(portal_dir);
        portal.status = status;
    }

    pub async fn load_realm_nodes(&self) -> Vec<NodeId> {
        aruna_operations::metadata::api::load_realm_nodes(
            self.driver_ctx.as_ref(),
            self.identity.realm_id,
            self.identity.node_id,
        )
        .await
    }

    pub async fn get_oidc_provider(
        &self,
        selector: &OidcTokenSelector,
    ) -> Result<OidcProviderConfig, OidcError> {
        let config = drive(
            GetConfigOperation::new(self.identity.realm_id),
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

    /// Peer contacts from the metadata plane; empty while it is not wired.
    pub fn peer_contacts(&self) -> aruna_operations::metadata::PeerContacts {
        self.get_ctx()
            .metadata_handle
            .as_ref()
            .map(aruna_operations::metadata::MetadataHandle::peer_contacts)
            .unwrap_or_default()
    }

    pub fn is_management_node(&self) -> bool {
        matches!(
            self.identity.node_capabilities,
            NodeCapabilities::Management { .. }
        )
    }

    pub fn is_user_node(&self) -> bool {
        matches!(
            self.identity.node_capabilities,
            NodeCapabilities::User { .. }
        )
    }

    pub(crate) fn management_url_cache(&self) -> &Arc<RwLock<ManagementUrlCache>> {
        &self.interfaces.management_urls
    }

    pub fn bootstrap_endpoint(&self) -> Option<EndpointAddr> {
        self.driver_ctx
            .net_handle
            .as_ref()
            .map(|net_handle| net_handle.endpoint_addr())
    }

    pub fn realm_key_pem(&self) -> Option<String> {
        match &self.identity.node_capabilities {
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
        match &self.identity.node_capabilities {
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

    pub async fn issue_sync_ticket(
        &self,
        node_id: NodeId,
    ) -> Result<OnboardingTicket, OnboardingSecretError> {
        match &self.identity.node_capabilities {
            NodeCapabilities::Management {
                realm_signing_key, ..
            } => drive(
                IssueSyncOperation::new(IssueSyncInput {
                    realm_signing_key: realm_signing_key.clone(),
                    realm_id: self.identity.realm_id,
                    node_id,
                    issuer_node_id: self.identity.node_id,
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

    pub async fn issuer_cache_len(&self) -> usize {
        self.identity.issuer_keys.len().await
    }

    pub async fn add_trusted_realm(&self, realm_id: RealmId) {
        self.identity
            .add_trusted_realm(self.driver_ctx.as_ref(), realm_id)
            .await;
    }

    pub async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.identity
            .trusted_realms_list
            .read()
            .await
            .get(realm_id)
            .is_some()
    }

    pub async fn claim_initial_admin(&self, auth: &AuthContext) -> Result<(), ClaimInitialError> {
        self.identity
            .claim_initial_admin(self.driver_ctx.as_ref(), auth)
            .await
    }
}

#[async_trait]
impl ArunaValidationState for ServerState {
    async fn is_token_revoked(
        &self,
        realm_id: &RealmId,
        token_hash: &str,
    ) -> Result<bool, ArunaBearerError> {
        // The issuing realm's replicated config is the only revocation
        // authority; it is expiry-bounded, so the durable set stays limited.
        realm_token_revoked(&self.driver_ctx.storage_handle, *realm_id, token_hash).await
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.identity
            .trusted_realms_list
            .read()
            .await
            .contains(realm_id)
    }

    async fn issuer_decoding_key(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerError> {
        self.identity.issuer_keys.get_or_insert(issuer_pubkey).await
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

/// Creates Swagger UI for the REST/Admin and S3 OpenAPI specifications.
/// Serves them at `/api-docs/openapi.json` and `/api-docs/s3-openapi.json`.
pub fn swagger_ui() -> SwaggerUi {
    SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi())
}

impl RestInterfaceRuntime {
    pub fn from_bind_address(bind_address: SocketAddr, public_url: Option<&str>) -> Self {
        let base_url = client_host_url(public_url.unwrap_or_default(), bind_address);
        Self {
            bind_address,
            api_base_url: format!("{base_url}/api/v1"),
            info_url: format!("{base_url}/api/v1/system/info"),
            swagger_ui_url: format!("{base_url}/swagger-ui"),
            base_url,
        }
    }
}

pub fn client_bind_url(bind_address: SocketAddr) -> String {
    format!(
        "http://{}:{}",
        host_for_ip(bind_address.ip()),
        bind_address.port()
    )
}

pub fn client_host_url(advertised_host: &str, bind_address: SocketAddr) -> String {
    let host = match advertised_host.trim() {
        "" => return client_bind_url(bind_address),
        host => {
            if host.contains("://") {
                return host.trim_end_matches('/').to_string();
            }

            if let Ok(addr) = host.parse::<SocketAddr>() {
                return format!("http://{}:{}", host_for_ip(addr.ip()), addr.port());
            }

            if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                return format!("http://{}:{}", host_for_ip(ip), bind_address.port());
            }

            host
        }
    };

    format!("http://{host}")
}

fn host_for_ip(ip: std::net::IpAddr) -> String {
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
#[path = "server_state_tests.rs"]
mod pure_tests;
