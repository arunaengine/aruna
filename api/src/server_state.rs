use crate::auth::{OidcTokenSelector, OidcValidator};
use crate::error::OidcError;
use crate::openapi::ApiDoc;
use crate::routes::management_relay::ManagementUrlCache;
use aruna_core::NodeId;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::credential_seal::CredentialSealKey;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, USER_KEYSPACE};
use aruna_core::metrics::NodeMetrics;
use aruna_core::onboarding::{OnboardingSecretError, OnboardingSyncTicket};
use aruna_core::structs::{
    Actor, AuthContext, NodeCapabilities, OidcProviderConfig, RealmId, RoCrateLimits,
};
use aruna_operations::auth::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, IssuerKeyCache, realm_token_revoked,
};
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminError, ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    ClaimInitialRealmAdminResult,
};
use aruna_operations::device::wipe::DeviceWipe;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::issue_onboarding_sync_ticket::{
    IssueOnboardingSyncTicketInput, IssueOnboardingSyncTicketOperation,
    ONBOARDING_SYNC_TICKET_TTL_SECS,
};
use aruna_operations::jobs::runtime::JobsRuntime;
use async_trait::async_trait;
use byteview::ByteView;
use ed25519_dalek::Signer;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use iroh::EndpointAddr;
use jsonwebtoken::DecodingKey;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use utoipa::ToSchema;
use utoipa_swagger_ui::SwaggerUi;

pub const INITIAL_REALM_ADMIN_CLAIMED_KEY: &[u8] = b"initial_realm_admin_claimed";
pub const INITIAL_LOCAL_ONBOARDING_SECRET_KEY: &[u8] = b"initial_local_onboarding_secret";
pub(crate) const ROCRATE_UPLOAD_SLOTS: usize = 32;
pub(crate) const DOWNLOAD_SLOTS: usize = 256;

#[derive(Debug)]
struct PublicDns;

impl Resolve for PublicDns {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?
                .collect::<Vec<_>>();
            if addresses.is_empty()
                || addresses
                    .iter()
                    .any(|address| !public_address(address.ip()))
            {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "assistant provider DNS resolved to a non-public address",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

pub(crate) fn public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => public_ipv4(address),
        IpAddr::V6(address) => public_ipv6(address),
    }
}

fn public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, d] = address.octets();
    !(a == 0
        || address.is_private()
        || (a == 100 && b & 0xc0 == 0x40)
        || address.is_loopback()
        || address.is_link_local()
        || (a == 192 && b == 0 && c == 0 && d != 9 && d != 10)
        || address.is_documentation()
        || (a == 198 && b & 0xfe == 18)
        || address.is_multicast()
        || a & 0xf0 == 0xf0)
}

fn public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    // Server-side providers use currently assigned global unicast space only.
    segments[0] & 0xe000 == 0x2000
        && !matches!(segments, [0x2001, 0xdb8, ..] | [0x3fff, 0..=0x0fff, ..])
        && !matches!(segments, [0x2002, ..])
        && !(matches!(segments, [0x2001, b, ..] if b < 0x200)
            && !(u128::from_be_bytes(address.octets())
                == 0x2001_0001_0000_0000_0000_0000_0000_0001
                || u128::from_be_bytes(address.octets())
                    == 0x2001_0001_0000_0000_0000_0000_0000_0002
                || matches!(segments, [0x2001, 3, ..] | [0x2001, 4, 0x112, ..])
                || matches!(segments, [0x2001, b, ..] if (0x20..=0x3f).contains(&b))))
}

#[derive(Clone, Debug)]
pub struct ServerState {
    // Contains neccessary drivers for request handling
    driver_ctx: Arc<DriverContext>,
    // Capabilities defined as in spec: Management, Server and User node capabilities
    node_capabilities: NodeCapabilities,
    // Bounded TTL + LRU cache of trusted issuer decoding keys
    issuer_keys: Arc<IssuerKeyCache>,
    // Contains trusted realms
    trusted_realms_list: Arc<RwLock<HashSet<RealmId, ahash::RandomState>>>,
    initial_admin_claim: Option<Arc<AtomicBool>>,
    // Realm membership
    realm_id: RealmId,
    // Realm membership
    node_id: NodeId,
    // Issuer-local key that seals S3 credential secrets at rest, derived from
    // this node's secret so it matches the S3 verifier on the same node.
    credential_seal_key: CredentialSealKey,
    // Contains OIDC config and Client
    oidc_validator: Option<Arc<OidcValidator>>,
    jobs_runtime: Arc<JobsRuntime>,
    interface_state: Arc<RwLock<InterfaceRuntimeState>>,
    portal: Arc<RwLock<PortalRuntimeState>>,
    // Per-node Prometheus registry shared with the S3 server and ops listener.
    metrics: Arc<NodeMetrics>,
    // True when this node can mount S3 inputs (Kubernetes with a CSI driver).
    s3_mounts_available: bool,
    rocrate_limits: RoCrateLimits,
    // Peers allowed to set `x-forwarded-*`; empty means no proxy is trusted.
    trusted_proxies: Vec<ipnet::IpNet>,
    rate_limits: Arc<crate::rate_limit::ApiRateLimits>,
    rocrate_upload_slots: Arc<Semaphore>,
    download_slots: Arc<Semaphore>,
    // Node shutdown token: long-lived response streams end when it fires, so
    // the ingress drain does not have to wait for client disconnects.
    shutdown_token: CancellationToken,
    // Present only on a user node: the owner's local wipe latch.
    device_wipe: Option<Arc<DeviceWipe>>,
    // Management api urls the management-route relay re-issues against.
    management_urls: Arc<RwLock<ManagementUrlCache>>,
    assistant_proxy: bool,
    assistant_client: Option<reqwest::Client>,
    chatgpt_refresh_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    chatgpt_issuer: String,
    chatgpt_base_url: String,
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
        let credential_seal_key = driver_ctx
            .net_handle
            .as_ref()
            .map(|net| net.credential_seal_key())
            .unwrap_or_else(CredentialSealKey::random);
        let assistant_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none());
        let assistant_client = if matches!(&node_capabilities, NodeCapabilities::User { .. }) {
            assistant_client
        } else {
            assistant_client.no_proxy().dns_resolver(PublicDns)
        };
        let state = Self {
            driver_ctx,
            realm_id,
            node_id,
            credential_seal_key,
            oidc_validator,
            jobs_runtime,
            node_capabilities,
            trusted_realms_list: Arc::new(RwLock::new(trusted_realms)),
            issuer_keys: Arc::new(IssuerKeyCache::new()),
            initial_admin_claim,
            interface_state: Arc::new(RwLock::new(InterfaceRuntimeState::default())),
            portal: Arc::new(RwLock::new(PortalRuntimeState::default())),
            metrics: Arc::new(NodeMetrics::new()),
            s3_mounts_available: false,
            rocrate_limits: RoCrateLimits::default(),
            trusted_proxies: Vec::new(),
            rate_limits: Arc::new(crate::rate_limit::ApiRateLimits::default()),
            rocrate_upload_slots: Arc::new(Semaphore::new(ROCRATE_UPLOAD_SLOTS)),
            download_slots: Arc::new(Semaphore::new(DOWNLOAD_SLOTS)),
            shutdown_token: CancellationToken::new(),
            device_wipe: None,
            management_urls: Arc::new(RwLock::new(ManagementUrlCache::default())),
            assistant_proxy: true,
            assistant_client: assistant_client.build().ok(),
            chatgpt_refresh_locks: Arc::new(Mutex::new(HashMap::new())),
            chatgpt_issuer: "https://auth.openai.com".to_string(),
            chatgpt_base_url: "https://chatgpt.com/backend-api/codex".to_string(),
        };
        state.persist_trusted_realms().await;
        state
    }

    pub fn with_shutdown_token(mut self, token: CancellationToken) -> Self {
        self.shutdown_token = token;
        self
    }

    /// Hands the device plane the wipe latch the process erases through. Only a
    /// user node is given one; without it `POST /device/wipe` is unavailable.
    pub fn with_device_wipe(mut self, wipe: Arc<DeviceWipe>) -> Self {
        self.device_wipe = Some(wipe);
        self
    }

    pub fn device_wipe(&self) -> Option<&Arc<DeviceWipe>> {
        self.device_wipe.as_ref()
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
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

    /// Records whether this node can mount S3 inputs, gating TES between mounted
    /// and snapshot staging. Call before serving.
    pub fn with_s3_mounts(mut self, available: bool) -> Self {
        self.s3_mounts_available = available;
        self
    }

    pub fn s3_mounts_available(&self) -> bool {
        self.s3_mounts_available
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    pub fn rocrate_limits(&self) -> &RoCrateLimits {
        &self.rocrate_limits
    }

    pub fn with_trusted_proxies(mut self, proxies: Vec<ipnet::IpNet>) -> Self {
        self.trusted_proxies = proxies;
        self
    }

    pub fn trusted_proxies(&self) -> &[ipnet::IpNet] {
        &self.trusted_proxies
    }

    /// Installs operator-configured request limiters. Call before serving.
    pub fn with_rate_limits(mut self, limits: crate::rate_limit::ApiRateLimits) -> Self {
        self.rate_limits = Arc::new(limits);
        self
    }

    pub fn rate_limits(&self) -> &crate::rate_limit::ApiRateLimits {
        &self.rate_limits
    }

    pub(crate) fn try_rocrate_slot(&self) -> Option<OwnedSemaphorePermit> {
        self.rocrate_upload_slots.clone().try_acquire_owned().ok()
    }

    pub(crate) fn try_acquire_download(&self) -> Option<OwnedSemaphorePermit> {
        self.download_slots.clone().try_acquire_owned().ok()
    }

    pub fn jobs_runtime(&self) -> Arc<JobsRuntime> {
        self.jobs_runtime.clone()
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
            NodeCapabilities::User {
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

    pub fn credential_seal_key(&self) -> &CredentialSealKey {
        &self.credential_seal_key
    }

    pub fn with_assistant_proxy(mut self, enabled: bool) -> Self {
        self.assistant_proxy = enabled;
        self
    }

    pub fn assistant_proxy(&self) -> bool {
        self.assistant_proxy
    }

    pub fn assistant_client(&self) -> Option<&reqwest::Client> {
        self.assistant_client.as_ref()
    }

    pub(crate) async fn chatgpt_lock(&self, provider_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.chatgpt_refresh_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(provider_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(provider_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    pub fn chatgpt_issuer(&self) -> &str {
        &self.chatgpt_issuer
    }

    pub fn chatgpt_base_url(&self) -> &str {
        &self.chatgpt_base_url
    }

    #[cfg(test)]
    pub(crate) fn with_chatgpt_urls(mut self, issuer: String, base_url: String) -> Self {
        self.chatgpt_issuer = issuer;
        self.chatgpt_base_url = base_url;
        self
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
        self.register_rest_interface_with_public_url(bind_address, None)
            .await;
    }

    pub async fn register_rest_interface_with_public_url(
        &self,
        bind_address: SocketAddr,
        public_url: Option<&str>,
    ) {
        let mut interface_state = self.interface_state.write().await;
        interface_state.rest = Some(RestInterfaceRuntime::from_bind_address(
            bind_address,
            public_url,
        ));
    }

    pub async fn register_s3_interface(&self, bind_address: SocketAddr, advertised_host: &str) {
        let mut interface_state = self.interface_state.write().await;
        interface_state.s3 = Some(S3InterfaceRuntime {
            bind_address,
            base_url: client_base_url_from_advertised_host(advertised_host, bind_address),
        });
    }

    pub async fn register_mcp_interface(&self) {
        let mut interface_state = self.interface_state.write().await;
        interface_state.mcp = interface_state
            .rest
            .as_ref()
            .map(|rest| McpInterfaceRuntime {
                bind_address: rest.bind_address,
                url: format!("{}/mcp", rest.base_url),
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

    /// Peer contacts from the metadata plane; empty while it is not wired.
    pub fn peer_contacts(&self) -> aruna_operations::metadata::PeerContacts {
        self.get_ctx()
            .metadata_handle
            .as_ref()
            .map(aruna_operations::metadata::MetadataHandle::peer_contacts)
            .unwrap_or_default()
    }

    pub fn is_management_node(&self) -> bool {
        matches!(self.node_capabilities, NodeCapabilities::Management { .. })
    }

    pub fn is_user_node(&self) -> bool {
        matches!(self.node_capabilities, NodeCapabilities::User { .. })
    }

    pub(crate) fn management_url_cache(&self) -> &Arc<RwLock<ManagementUrlCache>> {
        &self.management_urls
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

    pub async fn issuer_key_cache_len(&self) -> usize {
        self.issuer_keys.len().await
    }

    pub async fn add_trusted_realm(&self, realm_id: RealmId) {
        self.trusted_realms_list.write().await.insert(realm_id);
        self.persist_trusted_realms().await;
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
    async fn is_token_revoked(
        &self,
        realm_id: &RealmId,
        token_hash: &str,
    ) -> Result<bool, ArunaBearerTokenError> {
        // The issuing realm's replicated config is the only revocation
        // authority; it is expiry-bounded, so the durable set stays limited.
        realm_token_revoked(&self.driver_ctx.storage_handle, *realm_id, token_hash).await
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.trusted_realms_list.read().await.contains(realm_id)
    }

    async fn issuer_decoding_key(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        self.issuer_keys.get_or_insert(issuer_pubkey).await
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
    pub fn from_bind_address(bind_address: SocketAddr, public_url: Option<&str>) -> Self {
        let base_url =
            client_base_url_from_advertised_host(public_url.unwrap_or_default(), bind_address);
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
    use super::{
        PublicDns, RestInterfaceRuntime, client_base_url_from_advertised_host,
        client_base_url_from_bind_address, public_address,
    };
    use reqwest::dns::Resolve;

    #[test]
    fn rest_runtime_uses_public_url() {
        let runtime = RestInterfaceRuntime::from_bind_address(
            "0.0.0.0:3000".parse().unwrap(),
            Some("https://api.node-1.v3.aruna-engine.org/"),
        );
        assert_eq!(
            runtime.api_base_url,
            "https://api.node-1.v3.aruna-engine.org/api/v1"
        );
    }

    #[test]
    fn classifies_public_addresses() {
        assert!(public_address("8.8.8.8".parse().unwrap()));
        assert!(public_address("2001:4860:4860::8888".parse().unwrap()));
        for address in [
            "127.0.0.1",
            "100.64.0.1",
            "198.18.0.1",
            "::1",
            "fc00::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!public_address(address.parse().unwrap()), "{address}");
        }
    }

    #[tokio::test]
    async fn dns_rejects_localhost() {
        assert!(
            PublicDns
                .resolve("localhost".parse().unwrap())
                .await
                .is_err()
        );
    }

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
