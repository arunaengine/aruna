#![allow(dead_code)]

use aruna::bootstrap::{
    announce_core_documents, fetch_core_onboarding_documents, realm_bootstrap_exists,
};
use aruna::config::{Config, load, mark_node_state_complete, mark_onboarding_phase};
use aruna_api::routes::credentials::{
    CreateS3CredentialsRequest, CreateS3CredentialsResponse, CreateS3PathRestriction,
};
use aruna_api::routes::groups::{CreateGroupRequest, CreateGroupResponse, GroupInfoResponse};
use aruna_api::s3::s3_server::S3Server;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::onboarding::{
    CreateOnboardingSecretRequest, CreateOnboardingSecretResponse, OnboardingMode, OnboardingPhase,
};
use aruna_core::structs::{
    Actor, ArunaArn, Backend, BackendConfig, BlobTimeoutConfig, NodeCapabilities, PathRestriction,
    RealmId, TokenClaims, UserAccess,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::automerge::AutomergeHandle;
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
};
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::s3::get_user_access::GetUserAccessOperation;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use ed25519_dalek::SigningKey;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::StatusCode;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

pub(crate) type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
pub(crate) const AWS_REGION: &str = "eu-central-1";

#[allow(dead_code)]
pub(crate) struct S3Endpoint {
    pub(crate) endpoint_url: String,
    pub(crate) host: String,
}

pub(crate) struct S3Credentials {
    pub(crate) access_key_id: String,
    pub(crate) access_secret: String,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NodeServiceMode {
    Minimal,
    Full,
}

#[derive(Clone, Debug)]
struct FullNodeStorageConfig {
    metadata_storage_path: String,
    blob_root: String,
    blob_bucket_prefix: Option<String>,
    blob_max_bucket_size: Option<u64>,
    blob_multipart_bucket: Option<String>,
    blob_timeouts: BlobTimeoutConfig,
}

impl FullNodeStorageConfig {
    fn for_temp_dir(temp_dir: &TempDir) -> Self {
        let root = temp_dir.path();
        Self {
            metadata_storage_path: root.join("craqle").display().to_string(),
            blob_root: root.join("blobstore").display().to_string(),
            blob_bucket_prefix: None,
            blob_max_bucket_size: Some(100_000),
            blob_multipart_bucket: Some("uploaded-parts".to_string()),
            blob_timeouts: BlobTimeoutConfig::default(),
        }
    }

    fn from_config(config: &Config) -> Self {
        Self {
            metadata_storage_path: config.metadata_storage_path.clone(),
            blob_root: config.blob_root.clone(),
            blob_bucket_prefix: config.blob_bucket_prefix.clone(),
            blob_max_bucket_size: config.blob_max_bucket_size,
            blob_multipart_bucket: config.blob_multipart_bucket.clone(),
            blob_timeouts: config.blob_timeout_config(),
        }
    }

    fn blob_backend_config(&self) -> BackendConfig {
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: self.blob_root.clone(),
            service_config: HashMap::new(),
            bucket_prefix: self.blob_bucket_prefix.clone(),
            max_bucket_size: self.blob_max_bucket_size,
            multipart_bucket: self.blob_multipart_bucket.clone(),
            timeouts: self.blob_timeouts,
        }
    }

    fn ensure_directories(&self) -> TestResult<()> {
        std::fs::create_dir_all(PathBuf::from(&self.metadata_storage_path))?;
        std::fs::create_dir_all(PathBuf::from(&self.blob_root))?;
        Ok(())
    }
}

#[allow(dead_code)]
pub(crate) struct SeedNode {
    _temp_dir: TempDir,
    pub(crate) net: NetHandle,
    pub(crate) context: Arc<DriverContext>,
    pub(crate) realm_id: RealmId,
    pub(crate) user_id: UserId,
    pub(crate) capabilities: NodeCapabilities,
    pub(crate) base_url: String,
    pub(crate) s3: Option<S3Endpoint>,
    server_task: JoinHandle<()>,
    s3_task: Option<JoinHandle<()>>,
}

impl SeedNode {
    pub(crate) async fn shutdown(self) {
        self.server_task.abort();
        let _ = self.server_task.await;

        if let Some(s3_task) = self.s3_task {
            s3_task.abort();
            let _ = s3_task.await;
        }

        self.net.shutdown().await;
    }
}

#[allow(dead_code)]
pub(crate) struct JoinerNode {
    _temp_dir: TempDir,
    pub(crate) net: NetHandle,
    pub(crate) context: Arc<DriverContext>,
    pub(crate) config: Config,
    pub(crate) base_url: String,
    pub(crate) s3: Option<S3Endpoint>,
    server_task: JoinHandle<()>,
    s3_task: Option<JoinHandle<()>>,
}

impl JoinerNode {
    pub(crate) async fn shutdown(self) {
        self.server_task.abort();
        let _ = self.server_task.await;

        if let Some(s3_task) = self.s3_task {
            s3_task.abort();
            let _ = s3_task.await;
        }

        self.net.shutdown().await;
    }
}

struct EnvVarGuard {
    previous: Vec<(String, Option<String>)>,
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        for (key, value) in self.previous.drain(..) {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

pub(crate) fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) async fn wait_until<F, Fut>(
    description: &str,
    timeout: Duration,
    interval: Duration,
    mut condition: F,
) -> TestResult<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                std::io::Error::other(format!("timed out waiting for {description}")).into(),
            );
        }
        sleep(interval).await;
    }
}

pub(crate) async fn wait_for_realm_nodes(
    contexts: &[&DriverContext],
    realm_id: &RealmId,
    expected: usize,
) -> TestResult<()> {
    wait_until(
        "realm node convergence",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || async {
            for context in contexts {
                match drive(GetRealmNodesOperation::new(*realm_id), context).await {
                    Ok(nodes) if nodes.len() == expected => {}
                    _ => return false,
                }
            }
            true
        },
    )
    .await
}

pub(crate) async fn create_bearer_token(
    context: &DriverContext,
    user_id: UserId,
    realm_id: RealmId,
    node_capabilities: NodeCapabilities,
) -> TestResult<String> {
    Ok(drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            expiry: None,
            user_id,
            realm_id,
            node_capabilities,
        })?,
        context,
    )
    .await?)
}

pub(crate) fn sign_scoped_bearer_token(
    seed: &SeedNode,
    user_id: UserId,
    path_restrictions: Vec<PathRestriction>,
) -> TestResult<String> {
    let now = chrono::Utc::now().timestamp() as u64;
    let claims = TokenClaims {
        sub: user_id.to_string(),
        iss: seed.realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::new().to_string(),
        restrictions: Some(path_restrictions),
        issuer_pubkey: None,
        delegation_signature: None,
    };
    let NodeCapabilities::Management {
        realm_encoding_key, ..
    } = &seed.capabilities
    else {
        return Err(std::io::Error::other("seed node must use management capabilities").into());
    };

    Ok(encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(realm_encoding_key)?,
    )?)
}

pub(crate) async fn get_user_access(
    context: &DriverContext,
    access_key_id: &str,
) -> TestResult<UserAccess> {
    let access = drive(
        GetUserAccessOperation::new(access_key_id.to_string()),
        context,
    )
    .await?
    .ok_or_else(|| std::io::Error::other("user access not found"))?
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(access)
}

pub(crate) async fn create_group_via_http(
    base_url: &str,
    bearer_token: &str,
    name: &str,
) -> TestResult<CreateGroupResponse> {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/api/v1/groups"))
        .bearer_auth(bearer_token)
        .json(&CreateGroupRequest {
            name: name.to_string(),
        })
        .send()
        .await?;

    if response.status() != StatusCode::CREATED {
        return Err(std::io::Error::other(format!(
            "unexpected create group status: {}",
            response.status()
        ))
        .into());
    }

    Ok(response.json().await?)
}

pub(crate) async fn get_group_via_http(
    base_url: &str,
    bearer_token: &str,
    group_id: &str,
) -> TestResult<GroupInfoResponse> {
    let response = reqwest::Client::new()
        .get(format!("{base_url}/api/v1/groups/{group_id}"))
        .bearer_auth(bearer_token)
        .send()
        .await?;

    if response.status() != StatusCode::OK {
        return Err(std::io::Error::other(format!(
            "unexpected get group status: {}",
            response.status()
        ))
        .into());
    }

    Ok(response.json().await?)
}

pub(crate) async fn wait_for_group_via_http(
    base_url: &str,
    bearer_token: &str,
    group_id: &str,
) -> TestResult<GroupInfoResponse> {
    wait_until(
        "group visibility over REST",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || async {
            get_group_via_http(base_url, bearer_token, group_id)
                .await
                .is_ok()
        },
    )
    .await?;

    get_group_via_http(base_url, bearer_token, group_id).await
}

pub(crate) async fn create_s3_credentials_via_http(
    base_url: &str,
    bearer_token: &str,
    group_id: &str,
) -> TestResult<S3Credentials> {
    create_s3_credentials_with_restrictions_via_http(base_url, bearer_token, group_id, None).await
}

pub(crate) async fn create_s3_credentials_with_restrictions_via_http(
    base_url: &str,
    bearer_token: &str,
    group_id: &str,
    path_restrictions: Option<Vec<CreateS3PathRestriction>>,
) -> TestResult<S3Credentials> {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/api/v1/users/credentials"))
        .bearer_auth(bearer_token)
        .json(&CreateS3CredentialsRequest {
            group_id: group_id.to_string(),
            expires_in_seconds: Some(600),
            path_restrictions,
        })
        .send()
        .await?;

    if response.status() != StatusCode::CREATED {
        return Err(std::io::Error::other(format!(
            "unexpected create credentials status: {}",
            response.status()
        ))
        .into());
    }

    let response: CreateS3CredentialsResponse = response.json().await?;
    Ok(S3Credentials {
        access_key_id: response.access_key_id,
        access_secret: response.access_secret,
    })
}

pub(crate) fn s3_client(endpoint: &S3Endpoint, credentials: &S3Credentials) -> S3Client {
    let credentials = Credentials::new(
        credentials.access_key_id.clone(),
        credentials.access_secret.clone(),
        None,
        None,
        "aruna-e2e-test",
    );
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(AWS_REGION))
        .credentials_provider(credentials)
        .endpoint_url(endpoint.endpoint_url.clone())
        .force_path_style(true)
        .build();
    S3Client::from_conf(config)
}

pub(crate) fn bucket_arn(realm_id: &RealmId, node_id: iroh::PublicKey, bucket: &str) -> String {
    ArunaArn::s3_bucket(*realm_id, node_id, bucket.to_string())
        .expect("bucket arn should be valid")
        .to_string()
}

#[allow(dead_code)]
pub(crate) async fn spawn_seed_node() -> TestResult<SeedNode> {
    spawn_seed_node_with_mode(NodeServiceMode::Minimal).await
}

#[allow(dead_code)]
pub(crate) async fn spawn_full_seed_node() -> TestResult<SeedNode> {
    spawn_seed_node_with_mode(NodeServiceMode::Full).await
}

pub(crate) async fn create_onboarding_secret_via_http(
    seed: &SeedNode,
    mode: OnboardingMode,
) -> TestResult<String> {
    let token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;

    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/admin/onboarding/secrets", seed.base_url))
        .bearer_auth(token)
        .json(&CreateOnboardingSecretRequest {
            seed_url: seed.base_url.clone(),
            mode,
            expires_in_seconds: Some(600),
        })
        .send()
        .await?;
    if response.status() != StatusCode::CREATED {
        return Err(std::io::Error::other(format!(
            "unexpected onboarding secret status: {}",
            response.status()
        ))
        .into());
    }
    let response: CreateOnboardingSecretResponse = response.json().await?;
    Ok(response.onboarding_secret)
}

#[allow(dead_code)]
pub(crate) async fn spawn_joiner_node(
    seed: &SeedNode,
    onboarding_secret: String,
) -> TestResult<JoinerNode> {
    spawn_joiner_node_with_mode(seed, onboarding_secret, NodeServiceMode::Minimal).await
}

#[allow(dead_code)]
pub(crate) async fn spawn_full_joiner_node(
    seed: &SeedNode,
    onboarding_secret: String,
) -> TestResult<JoinerNode> {
    spawn_joiner_node_with_mode(seed, onboarding_secret, NodeServiceMode::Full).await
}

async fn spawn_seed_node_with_mode(mode: NodeServiceMode) -> TestResult<SeedNode> {
    let temp_dir = tempfile::tempdir()?;
    let storage_path = temp_dir
        .path()
        .to_str()
        .ok_or_else(|| std::io::Error::other("invalid temp path"))?;
    let storage = FjallStorage::open(storage_path)?;
    let realm_signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = UserId::new(Ulid::new(), realm_id);
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let full_storage_config =
        (mode == NodeServiceMode::Full).then(|| FullNodeStorageConfig::for_temp_dir(&temp_dir));
    let context = initialize_context(storage, net.clone(), full_storage_config.as_ref()).await?;

    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                node_id: net.node_id(),
                user_id,
                realm_id,
            },
            realm_description: "Test Realm".to_string(),
            oidc_providers: Vec::new(),
        }),
        context.as_ref(),
    )
    .await?;
    announce_core_documents(context.as_ref(), net.node_id(), &realm_id).await?;
    drive(
        ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: Actor {
                node_id: net.node_id(),
                user_id,
                realm_id,
            },
        }),
        context.as_ref(),
    )
    .await?;
    announce_realm_presence(context.as_ref(), &realm_id, net.node_id()).await?;

    let capabilities = NodeCapabilities::management_node(realm_signing_key)?;
    let (base_url, server_task) = spawn_rest_server(
        context.clone(),
        realm_id,
        net.node_id(),
        capabilities.clone(),
    )
    .await?;
    let (s3, s3_task) =
        spawn_optional_s3_server(mode, context.clone(), realm_id, net.node_id()).await?;

    Ok(SeedNode {
        _temp_dir: temp_dir,
        net,
        context,
        realm_id,
        user_id,
        capabilities,
        base_url,
        s3,
        server_task,
        s3_task,
    })
}

async fn spawn_joiner_node_with_mode(
    seed: &SeedNode,
    onboarding_secret: String,
    mode: NodeServiceMode,
) -> TestResult<JoinerNode> {
    let joiner_dir = tempfile::tempdir()?;
    let (config, storage_handle) = load_config_with_env(&joiner_dir, onboarding_secret).await?;

    let joiner_net = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_socket_addr,
            secret_key: Some(config.net_secret_key.clone()),
            realm_id: config.realm_id,
            peer_nodes: config.peer_nodes.clone(),
            peer_endpoints: config.peer_endpoints.clone(),
            temporary_bootstrap_active: config.temporary_bootstrap_active,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            max_concurrent_uni_streams: config.max_concurrent_uni_streams,
            max_concurrent_bidi_streams: config.max_concurrent_bidi_streams,
        },
        storage_handle.clone(),
    )
    .await?;
    let full_storage_config =
        (mode == NodeServiceMode::Full).then(|| FullNodeStorageConfig::from_config(&config));
    let joiner_context = initialize_context(
        storage_handle,
        joiner_net.clone(),
        full_storage_config.as_ref(),
    )
    .await?;
    seed.net.add_peer_addr(joiner_net.endpoint_addr()).await;
    announce_realm_presence(seed.context.as_ref(), &seed.realm_id, seed.net.node_id()).await?;

    fetch_core_onboarding_documents(
        joiner_context.as_ref(),
        &config.node_state,
        &config.realm_id,
        config.peer_endpoints.first().map(|endpoint| endpoint.id),
    )
    .await?;
    assert!(realm_bootstrap_exists(joiner_context.as_ref(), &config.realm_id).await?);
    mark_onboarding_phase(
        &joiner_context.storage_handle,
        &config.node_state,
        OnboardingPhase::CoreDocumentsFetched,
    )
    .await?;
    announce_core_documents(joiner_context.as_ref(), config.node_id, &config.realm_id).await?;
    mark_node_state_complete(&joiner_context.storage_handle, &config.node_state).await?;
    announce_realm_presence(joiner_context.as_ref(), &config.realm_id, config.node_id).await?;

    let (base_url, server_task) = spawn_rest_server(
        joiner_context.clone(),
        config.realm_id,
        config.node_id,
        config.node_capabilities.clone(),
    )
    .await?;
    let (s3, s3_task) = spawn_optional_s3_server(
        mode,
        joiner_context.clone(),
        config.realm_id,
        config.node_id,
    )
    .await?;

    Ok(JoinerNode {
        _temp_dir: joiner_dir,
        net: joiner_net,
        context: joiner_context,
        config,
        base_url,
        s3,
        server_task,
        s3_task,
    })
}

async fn initialize_context(
    storage_handle: StorageHandle,
    net: NetHandle,
    full_storage_config: Option<&FullNodeStorageConfig>,
) -> TestResult<Arc<DriverContext>> {
    let task_handle = TaskHandle::new();
    let automerge_handle = AutomergeHandle::new(Some(net.clone()));
    let metadata_handle = if let Some(config) = full_storage_config {
        config.ensure_directories()?;
        Some(MetadataHandle::new(
            &config.metadata_storage_path,
            net.node_id(),
            storage_handle.clone(),
            Some(net.clone()),
        )?)
    } else {
        None
    };
    let blob_handle = if let Some(config) = full_storage_config {
        Some(
            BlobHandler::new(
                config.blob_backend_config(),
                storage_handle.clone(),
                net.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net),
        blob_handle,
        automerge_handle: Some(automerge_handle),
        metadata_handle,
        task_handle: Some(task_handle.clone()),
    });
    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;
    Ok(context)
}

async fn announce_realm_presence(
    context: &DriverContext,
    realm_id: &RealmId,
    node_id: iroh::PublicKey,
) -> TestResult<()> {
    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: *realm_id,
            node_id,
            schedule_refresh: false,
        }),
        context,
    )
    .await?;
    Ok(())
}

async fn spawn_rest_server(
    context: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: iroh::PublicKey,
    capabilities: NodeCapabilities,
) -> TestResult<(String, JoinHandle<()>)> {
    let state =
        Arc::new(ServerState::new(context, realm_id, node_id, capabilities, false, None).await);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = Server::new(state, ServerConfig { http_addr: addr });
    let router = server.build_router();
    let server_task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Ok((format!("http://{}", addr), server_task))
}

async fn spawn_optional_s3_server(
    mode: NodeServiceMode,
    context: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: iroh::PublicKey,
) -> TestResult<(Option<S3Endpoint>, Option<JoinHandle<()>>)> {
    if mode != NodeServiceMode::Full {
        return Ok((None, None));
    }

    let (s3, task) = spawn_s3_server(context, realm_id, node_id).await?;
    Ok((Some(s3), Some(task)))
}

async fn spawn_s3_server(
    context: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: iroh::PublicKey,
) -> TestResult<(S3Endpoint, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let bind_addr = listener.local_addr()?;
    let address = bind_addr.to_string();
    let host = format!("localhost:{}", bind_addr.port());
    let s3_server =
        S3Server::new(address.as_str(), host.clone(), context, realm_id, node_id).await?;
    let (_addr, task) = s3_server.run_with_listener(listener)?;
    Ok((
        S3Endpoint {
            endpoint_url: format!("http://{host}"),
            host,
        },
        task,
    ))
}

async fn load_config_with_env(
    joiner_dir: &TempDir,
    onboarding_secret: String,
) -> TestResult<(Config, StorageHandle)> {
    let vars = [
        (
            "STORAGE_PATH",
            joiner_dir.path().to_str().unwrap().to_string(),
        ),
        ("SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
        ("P2P_SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
        ("S3_HOST", "127.0.0.1:0".to_string()),
        ("S3_ADDRESS", "127.0.0.1:0".to_string()),
        ("ONBOARDING_SECRET", onboarding_secret),
    ];

    let _lock = env_lock().lock().await;
    let _guard = set_env_vars(&vars);
    Ok(load().await?)
}

fn set_env_vars(vars: &[(&str, String)]) -> EnvVarGuard {
    let previous = vars
        .iter()
        .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
        .collect::<Vec<_>>();
    for (key, value) in vars {
        unsafe { std::env::set_var(key, value) };
    }
    EnvVarGuard { previous }
}
