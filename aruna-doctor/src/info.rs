use crate::error::CliError;
use aruna::config::{BootOrigin, Config, PersistedNodeIdentity, PersistedNodeStatus, StartupMode, load};
use aruna_api::routes::info::InfoResponse;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::NodeCapabilities;
use reqwest::Client;
use serde::Serialize;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Serialize)]
struct DoctorInfoOutput {
    config: ConfigView,
    endpoint_info: InfoResponse,
}

#[derive(Debug, Serialize)]
struct ConfigView {
    storage_path: String,
    metadata_storage_path: String,
    blob_root: String,
    blob_bucket_prefix: Option<String>,
    blob_max_bucket_size: Option<u64>,
    blob_multipart_bucket: Option<String>,
    blob_control_plane_connect_timeout_secs: u64,
    blob_control_plane_io_timeout_secs: u64,
    blob_transfer_idle_timeout_secs: u64,
    http_socket_addr: String,
    http_base_url: String,
    p2p_socket_addr: String,
    max_concurrent_uni_streams: Option<u64>,
    max_concurrent_bidi_streams: Option<u64>,
    node_capabilities: NodeCapabilitiesView,
    realm_id: String,
    node_id: String,
    bootstrap_nodes: Vec<String>,
    bootstrap_endpoints: Vec<String>,
    default_metadata_replication_factor: u32,
    s3_port: u16,
    s3_host: String,
    s3_address: String,
    onboarding_secret_present: bool,
    oidc_providers: Vec<OidcProviderView>,
    startup_mode: StartupModeView,
    node_state: PersistedNodeStateView,
}

#[derive(Debug, Serialize)]
struct OidcProviderView {
    id: String,
    issuer: String,
    audience: String,
    discovery_url: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind")]
enum NodeCapabilitiesView {
    Management,
    Server {
        delegation_signature: String,
    },
    Local,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind")]
enum StartupModeView {
    InitializeRealm {
        realm_description: String,
    },
    JoinRealm {
        phase: OnboardingPhaseView,
    },
    Provisioned,
}

#[derive(Debug, Serialize)]
struct PersistedNodeStateView {
    boot_origin: BootOriginView,
    status: PersistedNodeStatusView,
    realm_id: String,
    bootstrap_endpoints: Vec<String>,
    onboarding_phase: Option<OnboardingPhaseView>,
    onboarding_sync_ticket_present: bool,
    identity: PersistedNodeIdentityView,
}

#[derive(Debug, Serialize)]
enum BootOriginView {
    InitializedRealm,
    Onboarded,
}

#[derive(Debug, Serialize)]
enum PersistedNodeStatusView {
    PendingInitialization,
    PendingOnboarding,
    Complete,
}

#[derive(Debug, Serialize)]
enum OnboardingPhaseView {
    Bootstrapped,
    CoreDocumentsFetched,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind")]
enum PersistedNodeIdentityView {
    Management,
    Server {
        delegation_signature: String,
    },
    Local,
}

pub async fn print_info() -> Result<(), CliError> {
    let _ = dotenvy::dotenv();
    let (config, _storage) = load().await.map_err(Box::new)?;
    let endpoint_info = fetch_info(&config).await?;
    let output = DoctorInfoOutput {
        config: ConfigView::from_config(&config),
        endpoint_info,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn fetch_info(config: &Config) -> Result<InfoResponse, CliError> {
    let base_url = http_base_url(config.http_socket_addr);
    let response = Client::new()
        .get(format!("{base_url}/api/v1/info"))
        .send()
        .await?
        .error_for_status()?;

    Ok(response.json::<InfoResponse>().await?)
}

fn http_base_url(addr: SocketAddr) -> String {
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => Ipv4Addr::LOCALHOST.to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "::1".to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
        IpAddr::V4(ip) => ip.to_string(),
    };
    format!("http://{host}:{}", addr.port())
}

impl ConfigView {
    fn from_config(config: &Config) -> Self {
        Self {
            storage_path: config.storage_path.clone(),
            metadata_storage_path: config.metadata_storage_path.clone(),
            blob_root: config.blob_root.clone(),
            blob_bucket_prefix: config.blob_bucket_prefix.clone(),
            blob_max_bucket_size: config.blob_max_bucket_size,
            blob_multipart_bucket: config.blob_multipart_bucket.clone(),
            blob_control_plane_connect_timeout_secs: config.blob_control_plane_connect_timeout_secs,
            blob_control_plane_io_timeout_secs: config.blob_control_plane_io_timeout_secs,
            blob_transfer_idle_timeout_secs: config.blob_transfer_idle_timeout_secs,
            http_socket_addr: config.http_socket_addr.to_string(),
            http_base_url: http_base_url(config.http_socket_addr),
            p2p_socket_addr: config.p2p_socket_addr.to_string(),
            max_concurrent_uni_streams: config.max_concurrent_uni_streams,
            max_concurrent_bidi_streams: config.max_concurrent_bidi_streams,
            node_capabilities: NodeCapabilitiesView::from_capabilities(&config.node_capabilities),
            realm_id: config.realm_id.to_string(),
            node_id: config.node_id.to_string(),
            bootstrap_nodes: config.bootstrap_nodes.iter().map(ToString::to_string).collect(),
            bootstrap_endpoints: config
                .bootstrap_endpoints
                .iter()
                .map(|endpoint| serde_json::to_string(endpoint).expect("endpoint addr must serialize"))
                .collect(),
            default_metadata_replication_factor: config.default_metadata_replication_factor,
            s3_port: config.s3_port,
            s3_host: config.s3_host.clone(),
            s3_address: config.s3_address.clone(),
            onboarding_secret_present: config.onboarding_secret.is_some(),
            oidc_providers: config
                .oidc_providers
                .iter()
                .map(|provider| OidcProviderView {
                    id: provider.id.clone(),
                    issuer: provider.issuer.clone(),
                    audience: provider.audience.clone(),
                    discovery_url: provider.discovery_url.clone(),
                })
                .collect(),
            startup_mode: StartupModeView::from_startup_mode(&config.startup_mode),
            node_state: PersistedNodeStateView::from_config(config),
        }
    }
}

impl NodeCapabilitiesView {
    fn from_capabilities(capabilities: &NodeCapabilities) -> Self {
        match capabilities {
            NodeCapabilities::Management { .. } => Self::Management,
            NodeCapabilities::Server {
                delegation_signature,
                ..
            } => Self::Server {
                delegation_signature: delegation_signature.clone(),
            },
            NodeCapabilities::Local { .. } => Self::Local,
        }
    }
}

impl StartupModeView {
    fn from_startup_mode(mode: &StartupMode) -> Self {
        match mode {
            StartupMode::InitializeRealm { realm_description } => Self::InitializeRealm {
                realm_description: realm_description.clone(),
            },
            StartupMode::JoinRealm { phase } => Self::JoinRealm {
                phase: OnboardingPhaseView::from_phase(*phase),
            },
            StartupMode::Provisioned => Self::Provisioned,
        }
    }
}

impl PersistedNodeStateView {
    fn from_config(config: &Config) -> Self {
        Self {
            boot_origin: BootOriginView::from_origin(config.node_state.boot_origin.clone()),
            status: PersistedNodeStatusView::from_status(config.node_state.status.clone()),
            realm_id: config.node_state.realm_id.to_string(),
            bootstrap_endpoints: config
                .node_state
                .bootstrap_endpoints
                .iter()
                .map(|endpoint| serde_json::to_string(endpoint).expect("endpoint addr must serialize"))
                .collect(),
            onboarding_phase: config
                .node_state
                .onboarding_phase
                .map(OnboardingPhaseView::from_phase),
            onboarding_sync_ticket_present: config.node_state.onboarding_sync_ticket.is_some(),
            identity: PersistedNodeIdentityView::from_identity(&config.node_state.identity),
        }
    }
}

impl BootOriginView {
    fn from_origin(origin: BootOrigin) -> Self {
        match origin {
            BootOrigin::InitializedRealm => Self::InitializedRealm,
            BootOrigin::Onboarded => Self::Onboarded,
        }
    }
}

impl PersistedNodeStatusView {
    fn from_status(status: PersistedNodeStatus) -> Self {
        match status {
            PersistedNodeStatus::PendingInitialization => Self::PendingInitialization,
            PersistedNodeStatus::PendingOnboarding => Self::PendingOnboarding,
            PersistedNodeStatus::Complete => Self::Complete,
        }
    }
}

impl OnboardingPhaseView {
    fn from_phase(phase: OnboardingPhase) -> Self {
        match phase {
            OnboardingPhase::Bootstrapped => Self::Bootstrapped,
            OnboardingPhase::CoreDocumentsFetched => Self::CoreDocumentsFetched,
        }
    }
}

impl PersistedNodeIdentityView {
    fn from_identity(identity: &PersistedNodeIdentity) -> Self {
        match identity {
            PersistedNodeIdentity::Management { .. } => Self::Management,
            PersistedNodeIdentity::Server {
                delegation_signature,
                ..
            } => Self::Server {
                delegation_signature: delegation_signature.clone(),
            },
            PersistedNodeIdentity::Local => Self::Local,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigView, fetch_info, http_base_url};
    use crate::test_support::env_lock;
    use aruna::config::{BootOrigin, PersistedNodeIdentity, PersistedNodeStatus, load};
    use aruna_api::server::{Server, ServerConfig};
    use aruna_api::server_state::ServerState;
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::announce_realm_presence::{
        AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::incoming::initialize_net_incoming;
    use aruna_operations::task_incoming::initialize_task_incoming;
    use aruna_tasks::TaskHandle;
    use axum::Router;
    use ed25519_dalek::SigningKey;
    use std::str::FromStr;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use ulid::Ulid;

    struct TestEnvGuard {
        previous: Vec<(String, Option<String>)>,
    }

    impl TestEnvGuard {
        fn set(vars: &[(&str, String)]) -> Self {
            let previous = vars
                .iter()
                .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
                .collect::<Vec<_>>();
            for (key, value) in vars {
                unsafe { std::env::set_var(key, value) };
            }
            Self { previous }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.previous.drain(..) {
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    struct TestNode {
        _temp_dir: tempfile::TempDir,
        _env_guard: TestEnvGuard,
        http_addr: std::net::SocketAddr,
        server_task: tokio::task::JoinHandle<()>,
        net: NetHandle,
    }

    impl TestNode {
        async fn shutdown(self) {
            self.server_task.abort();
            let _ = self.server_task.await;
            self.net.shutdown().await;
        }
    }

    async fn spawn_test_node() -> TestNode {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("storage");
        let blob_root = temp_dir.path().join("blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();

        let env_guard = TestEnvGuard::set(&[
            (
                "STORAGE_PATH",
                storage_path.to_str().unwrap().to_string(),
            ),
            ("BLOB_ROOT", blob_root.to_str().unwrap().to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("S3_PORT", "0".to_string()),
            ("S3_HOST", "localhost".to_string()),
            ("S3_ADDRESS", "127.0.0.1".to_string()),
            ("OIDC_PROVIDER_IDS", "".to_string()),
        ]);

        let (config, storage_handle) = load().await.unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(config.net_secret_key.clone()),
                realm_id: config.realm_id,
                bootstrap_nodes: Vec::new(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..Default::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net.clone()),
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
        });
        initialize_net_incoming(context.clone());
        initialize_task_incoming(context.clone(), task_handle).await;

        let realm_signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
        let bootstrap_user = aruna_core::UserId::local(Ulid::new(), realm_id);
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: aruna_core::structs::Actor {
                    node_id: net.node_id(),
                    user_id: bootstrap_user,
                    realm_id,
                },
                realm_description: "Doctor Test Realm".to_string(),
                oidc_providers: Vec::new(),
            }),
            context.as_ref(),
        )
        .await
        .unwrap();
        drive(
            AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                realm_id,
                node_id: net.node_id(),
                schedule_refresh: false,
            }),
            context.as_ref(),
        )
        .await
        .unwrap();

        let state = Arc::new(
            ServerState::new(context, realm_id, net.node_id(), capabilities, false, None).await,
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router: Router = Server::new(state, ServerConfig { http_addr: addr }).build_router();
        let server_task = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });

        TestNode {
            _temp_dir: temp_dir,
            _env_guard: env_guard,
            http_addr: addr,
            server_task,
            net,
        }
    }

    #[test]
    fn http_base_url_rewrites_unspecified_ipv4() {
        let addr: std::net::SocketAddr = "0.0.0.0:3000".parse().unwrap();
        assert_eq!(http_base_url(addr), "http://127.0.0.1:3000");
    }

    #[test]
    fn config_view_marks_onboarding_secret_presence_without_exposing_secret() {
        let config = aruna::config::Config {
            storage_path: "/tmp/storage".to_string(),
            metadata_storage_path: "/tmp/storage/craqle".to_string(),
            blob_root: "/tmp/blob".to_string(),
            blob_bucket_prefix: Some("aruna-".to_string()),
            blob_max_bucket_size: Some(123),
            blob_multipart_bucket: Some("parts".to_string()),
            blob_control_plane_connect_timeout_secs: 11,
            blob_control_plane_io_timeout_secs: 12,
            blob_transfer_idle_timeout_secs: 13,
            http_socket_addr: "0.0.0.0:3000".parse().unwrap(),
            p2p_socket_addr: "127.0.0.1:3001".parse().unwrap(),
            max_concurrent_uni_streams: Some(10),
            max_concurrent_bidi_streams: Some(20),
            node_capabilities: NodeCapabilities::Local {
                realm_verifying_key: RealmId::from_bytes(SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng).verifying_key().to_bytes())
                    .to_pkcs8_pem_bytes()
                    .unwrap(),
            },
            realm_id: RealmId([7u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            net_secret_key: iroh::SecretKey::from_bytes(&[9u8; 32]),
            bootstrap_nodes: Vec::new(),
            bootstrap_endpoints: Vec::new(),
            default_metadata_replication_factor: 3,
            s3_port: 1337,
            s3_host: "localhost".to_string(),
            s3_address: "127.0.0.1".to_string(),
            onboarding_secret: Some("secret".to_string()),
            oidc_providers: Vec::new(),
            startup_mode: aruna::config::StartupMode::Provisioned,
            node_state: aruna::config::PersistedNodeState {
                boot_origin: BootOrigin::InitializedRealm,
                status: PersistedNodeStatus::Complete,
                realm_id: RealmId([7u8; 32]),
                net_secret_key: [9u8; 32],
                bootstrap_endpoints: Vec::new(),
                onboarding_phase: None,
                onboarding_sync_ticket: None,
                identity: PersistedNodeIdentity::Local,
            },
        };

        let view = ConfigView::from_config(&config);

        assert_eq!(view.http_base_url, "http://127.0.0.1:3000");
        assert!(view.onboarding_secret_present);
    }

    #[tokio::test]
    async fn fetch_info_reads_live_info_endpoint() {
        let _guard = env_lock().lock().await;
        let node = spawn_test_node().await;
        let storage_path = dotenvy::var("STORAGE_PATH").unwrap();
        let blob_root = dotenvy::var("BLOB_ROOT").unwrap();
        let p2p_socket_addr = std::net::SocketAddr::from_str(&dotenvy::var("P2P_SOCKET_ADDRESS").unwrap()).unwrap();
        let s3_port = dotenvy::var("S3_PORT").unwrap().parse::<u16>().unwrap();
        let s3_host = dotenvy::var("S3_HOST").unwrap();
        let s3_address = dotenvy::var("S3_ADDRESS").unwrap();
        let temp_storage = tempdir().unwrap();
        let temp_storage_handle = aruna_storage::FjallStorage::open(temp_storage.path().to_str().unwrap()).unwrap();
        let config = aruna::config::Config {
            storage_path,
            metadata_storage_path: format!("{}/craqle", temp_storage.path().display()),
            blob_root,
            blob_bucket_prefix: None,
            blob_max_bucket_size: Some(100_000),
            blob_multipart_bucket: Some("uploaded-parts".to_string()),
            blob_control_plane_connect_timeout_secs: 30,
            blob_control_plane_io_timeout_secs: 30,
            blob_transfer_idle_timeout_secs: 30 * 60,
            http_socket_addr: node.http_addr,
            p2p_socket_addr,
            max_concurrent_uni_streams: None,
            max_concurrent_bidi_streams: None,
            node_capabilities: NodeCapabilities::Local {
                realm_verifying_key: RealmId::from_bytes(SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng).verifying_key().to_bytes())
                    .to_pkcs8_pem_bytes()
                    .unwrap(),
            },
            realm_id: RealmId([1u8; 32]),
            node_id: iroh::SecretKey::generate().public(),
            net_secret_key: iroh::SecretKey::generate(),
            bootstrap_nodes: Vec::new(),
            bootstrap_endpoints: Vec::new(),
            default_metadata_replication_factor: 3,
            s3_port,
            s3_host,
            s3_address,
            onboarding_secret: None,
            oidc_providers: Vec::new(),
            startup_mode: aruna::config::StartupMode::Provisioned,
            node_state: aruna::config::PersistedNodeState {
                boot_origin: BootOrigin::InitializedRealm,
                status: PersistedNodeStatus::Complete,
                realm_id: RealmId([1u8; 32]),
                net_secret_key: iroh::SecretKey::generate().to_bytes(),
                bootstrap_endpoints: Vec::new(),
                onboarding_phase: None,
                onboarding_sync_ticket: None,
                identity: PersistedNodeIdentity::Local,
            },
        };
        drop(temp_storage_handle);

        let info = fetch_info(&config).await.unwrap();

        assert!(matches!(info.net_state, aruna_api::routes::info::NetStatus::Available { .. }));
        node.shutdown().await;
    }
}
