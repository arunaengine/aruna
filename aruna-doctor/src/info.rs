use crate::error::CliError;
use aruna_api::routes::info::InfoResponse;
use aruna_api::server_state::client_base_url_from_bind_address;
use reqwest::Client;
use serde::Serialize;
use std::net::SocketAddr;
use std::time::Duration;

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
    p2p_additional_relay_urls: Vec<String>,
    default_metadata_replication_factor: u32,
    s3_host: String,
    api_public_url: Option<String>,
    s3_public_url: Option<String>,
    s3_address: String,
    onboarding_secret_present: bool,
    oidc_providers: Vec<OidcProviderView>,
}

#[derive(Debug, Serialize)]
struct OidcProviderView {
    id: String,
    issuer: String,
    audience: String,
    discovery_url: String,
}

pub async fn print_info(token: Option<String>) -> Result<(), CliError> {
    let _ = dotenvy::dotenv();
    let http_socket_addr: SocketAddr = dotenvy::var("SOCKET_ADDRESS")?.parse()?;
    let endpoint_info = fetch_info(http_socket_addr, resolve_token(token).as_deref()).await?;
    let output = DoctorInfoOutput {
        config: ConfigView::from_env(http_socket_addr)?,
        endpoint_info,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

/// `/info` only reports topology to a realm token and backend detail to a realm
/// admin, so the operator passes one via `--token` or `ARUNA_TOKEN`.
pub(crate) fn resolve_token(token: Option<String>) -> Option<String> {
    token
        .or_else(|| dotenvy::var("ARUNA_TOKEN").ok())
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
}

pub(crate) fn default_info_url_from_env() -> Result<String, CliError> {
    let http_socket_addr: SocketAddr = dotenvy::var("SOCKET_ADDRESS")?.parse()?;
    Ok(info_url(http_socket_addr))
}

pub(crate) async fn fetch_info(
    http_socket_addr: SocketAddr,
    token: Option<&str>,
) -> Result<InfoResponse, CliError> {
    fetch_info_url_with_timeout(&info_url(http_socket_addr), Duration::from_secs(10), token).await
}

pub(crate) async fn fetch_info_url_with_timeout(
    url: &str,
    timeout: Duration,
    token: Option<&str>,
) -> Result<InfoResponse, CliError> {
    let timeout = nonzero_timeout(timeout);
    let connect_timeout = nonzero_timeout(timeout.min(Duration::from_secs(2)));
    let mut request = Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(timeout)
        .build()?
        .get(url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|source| CliError::InfoRequest {
            url: url.to_string(),
            source,
        })?
        .error_for_status()
        .map_err(|source| CliError::InfoRequest {
            url: url.to_string(),
            source,
        })?;

    response
        .json::<InfoResponse>()
        .await
        .map_err(|source| CliError::InfoRequest {
            url: url.to_string(),
            source,
        })
}

fn http_base_url(addr: SocketAddr) -> String {
    client_base_url_from_bind_address(addr)
}

fn info_url(addr: SocketAddr) -> String {
    format!("{}/api/v1/info", http_base_url(addr))
}

fn nonzero_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout
    }
}

impl ConfigView {
    fn from_env(http_socket_addr: SocketAddr) -> Result<Self, CliError> {
        let storage_path = dotenvy::var("STORAGE_PATH").ok();
        let metadata_storage_path = dotenvy::var("CRAQLE_STORAGE_PATH")
            .ok()
            .or_else(|| storage_path.as_ref().map(|path| format!("{path}/craqle")));
        let blob_root = dotenvy::var("BLOB_ROOT").ok().or_else(|| {
            storage_path
                .as_ref()
                .map(|path| format!("{path}/blobstore"))
        });
        let oidc_provider_ids = dotenvy::var("OIDC_PROVIDER_IDS").unwrap_or_default();

        Ok(Self {
            storage_path: storage_path.unwrap_or_default(),
            metadata_storage_path: metadata_storage_path.unwrap_or_default(),
            blob_root: blob_root.unwrap_or_default(),
            blob_bucket_prefix: dotenvy::var("BLOB_BUCKET_PREFIX").ok(),
            blob_max_bucket_size: parse_optional_env("BLOB_MAX_BUCKET_SIZE")?,
            blob_multipart_bucket: dotenvy::var("BLOB_MULTIPART_BUCKET")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            blob_control_plane_connect_timeout_secs: parse_optional_env(
                "BLOB_CONTROL_PLANE_CONNECT_TIMEOUT_SECS",
            )?
            .unwrap_or(30),
            blob_control_plane_io_timeout_secs: parse_optional_env(
                "BLOB_CONTROL_PLANE_IO_TIMEOUT_SECS",
            )?
            .unwrap_or(30),
            blob_transfer_idle_timeout_secs: parse_optional_env("BLOB_TRANSFER_IDLE_TIMEOUT_SECS")?
                .unwrap_or(30 * 60),
            http_socket_addr: http_socket_addr.to_string(),
            http_base_url: http_base_url(http_socket_addr),
            p2p_socket_addr: dotenvy::var("P2P_SOCKET_ADDRESS")
                .unwrap_or_else(|_| http_socket_addr.to_string()),
            max_concurrent_uni_streams: parse_optional_env("MAX_CONCURRENT_UNI_STREAMS")?,
            max_concurrent_bidi_streams: parse_optional_env("MAX_CONCURRENT_BIDI_STREAMS")?,
            p2p_additional_relay_urls: parse_list_env("P2P_ADDITIONAL_RELAY_URLS"),
            default_metadata_replication_factor: parse_optional_env("METADATA_REPLICATION_FACTOR")?
                .unwrap_or(3),
            s3_host: dotenvy::var("S3_HOST").unwrap_or_default(),
            api_public_url: optional_nonempty_env("API_PUBLIC_URL"),
            s3_public_url: optional_nonempty_env("S3_PUBLIC_URL"),
            s3_address: dotenvy::var("S3_ADDRESS").unwrap_or_default(),
            onboarding_secret_present: dotenvy::var("ONBOARDING_SECRET")
                .ok()
                .is_some_and(|value| !value.trim().is_empty()),
            oidc_providers: oidc_provider_ids
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(|id| OidcProviderView {
                    id: id.to_string(),
                    issuer: dotenvy::var(format!("OIDC_{}_ISSUER", env_key(id)))
                        .unwrap_or_default(),
                    audience: dotenvy::var(format!("OIDC_{}_AUDIENCE", env_key(id)))
                        .unwrap_or_default(),
                    discovery_url: dotenvy::var(format!("OIDC_{}_DISCOVERY_URL", env_key(id)))
                        .unwrap_or_default(),
                })
                .collect(),
        })
    }
}

fn parse_optional_env<T>(key: &str) -> Result<Option<T>, CliError>
where
    T: std::str::FromStr,
    CliError: From<T::Err>,
{
    Ok(dotenvy::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.parse::<T>())
        .transpose()?)
}

fn optional_nonempty_env(key: &str) -> Option<String> {
    dotenvy::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn parse_list_env(key: &str) -> Vec<String> {
    dotenvy::var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn env_key(id: &str) -> String {
    id.to_ascii_uppercase().replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::{ConfigView, fetch_info, http_base_url};
    use crate::test_support::env_lock;
    use aruna::config::load;
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
    use ed25519_dalek::SigningKey;
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
            ("STORAGE_PATH", storage_path.to_str().unwrap().to_string()),
            ("BLOB_ROOT", blob_root.to_str().unwrap().to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("S3_HOST", "127.0.0.1:0".to_string()),
            ("S3_ADDRESS", "127.0.0.1:0".to_string()),
            ("OIDC_PROVIDER_IDS", "".to_string()),
            ("ONBOARDING_SECRET", "".to_string()),
        ]);

        let (config, storage_handle) = load().await.unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(config.net_secret_key.clone()),
                realm_id: config.realm_id,
                peer_nodes: Vec::new(),
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
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
        });
        initialize_net_incoming(context.clone());
        initialize_task_incoming(context.clone(), task_handle).await;

        let realm_signing_key =
            SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
        let bootstrap_user = aruna_core::UserId::local(Ulid::r#gen(), realm_id);
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: aruna_core::structs::Actor {
                    node_id: net.node_id(),
                    user_id: bootstrap_user,
                    realm_id,
                },
                realm_description: "Doctor Test Realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
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
        let server = Server::new(
            state,
            ServerConfig {
                http_addr: addr,
                max_http_body_size: aruna_api::server::DEFAULT_MAX_HTTP_BODY_SIZE,
                cors: aruna_api::cors::CorsConfig::default(),
                portal_csp: aruna_api::csp::PortalCspConfig::default(),
            },
        );
        let server_task = tokio::spawn(async move {
            server.run_with_listener(listener).await.unwrap();
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
    fn ipv4_base_url() {
        let addr: std::net::SocketAddr = "0.0.0.0:3000".parse().unwrap();
        assert_eq!(http_base_url(addr), "http://127.0.0.1:3000");
    }

    #[test]
    fn ipv6_base_url() {
        let addr: std::net::SocketAddr = "[::]:3000".parse().unwrap();
        assert_eq!(http_base_url(addr), "http://[::1]:3000");
    }

    #[tokio::test]
    async fn onboarding_secret() {
        let _env_lock = env_lock().lock().await;
        let _guard = TestEnvGuard::set(&[
            ("STORAGE_PATH", "/tmp/storage".to_string()),
            ("ONBOARDING_SECRET", "secret".to_string()),
        ]);
        let view = ConfigView::from_env("0.0.0.0:3000".parse().unwrap()).unwrap();

        assert_eq!(view.http_base_url, "http://127.0.0.1:3000");
        assert!(view.onboarding_secret_present);
    }

    #[tokio::test]
    async fn doctor_relays() {
        let _env_lock = env_lock().lock().await;
        let _guard = TestEnvGuard::set(&[(
            "P2P_ADDITIONAL_RELAY_URLS",
            "https://relay-a.example,https://relay-b.example".to_string(),
        )]);

        let view = ConfigView::from_env("0.0.0.0:3000".parse().unwrap()).unwrap();

        assert_eq!(
            view.p2p_additional_relay_urls,
            vec![
                "https://relay-a.example".to_string(),
                "https://relay-b.example".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn doctor_reports_public_urls_without_fallbacks() {
        let _env_lock = env_lock().lock().await;
        let _guard = TestEnvGuard::set(&[
            ("API_PUBLIC_URL", "https://api.example.test".to_string()),
            ("S3_PUBLIC_URL", "https://s3.example.test".to_string()),
        ]);

        let view = ConfigView::from_env("0.0.0.0:3000".parse().unwrap()).unwrap();

        assert_eq!(
            view.api_public_url.as_deref(),
            Some("https://api.example.test")
        );
        assert_eq!(
            view.s3_public_url.as_deref(),
            Some("https://s3.example.test")
        );
    }

    /// Without a token the node reports health and version only.
    #[tokio::test]
    async fn live_info() {
        let _guard = env_lock().lock().await;
        let node = spawn_test_node().await;

        let info = fetch_info(node.http_addr, None).await.unwrap();

        assert_eq!(
            info.status,
            aruna_api::routes::info::ServiceStatus::Available
        );
        assert!(!info.api_version.is_empty());
        assert!(info.topology.is_none());
        assert!(info.operations.is_none());
        node.shutdown().await;
    }
}
