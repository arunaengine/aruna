use aruna::bootstrap::{
    announce_core_documents, fetch_core_onboarding_documents, realm_bootstrap_exists,
};
use aruna::config::{StartupMode, load, mark_node_state_complete, mark_onboarding_phase};
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_core::onboarding::{
    CreateOnboardingSecretRequest, CreateOnboardingSecretResponse, OnboardingMode, OnboardingPhase,
};
use aruna_core::structs::{Actor, NodeCapabilities, RealmId};
use aruna_net::{NetConfig, NetHandle};
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
use aruna_operations::register_user::{RegisterUserInput, RegisterUserOperation};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use reqwest::StatusCode;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

struct SeedNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
    realm_id: RealmId,
    user_id: Ulid,
    capabilities: NodeCapabilities,
    base_url: String,
    server_task: JoinHandle<()>,
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn spawn_seed_node() -> Result<SeedNode, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            use_dns_discovery: false,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let automerge_handle = AutomergeHandle::new(Some(net.clone()));
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_handle),
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
    });
    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;

    let realm_signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = Ulid::new();
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                node_id: net.node_id(),
                user_id,
                realm_id: realm_id.clone(),
            },
            realm_description: "Test Realm".to_string(),
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
                realm_id: realm_id.clone(),
            },
        }),
        context.as_ref(),
    )
    .await?;
    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: realm_id.clone(),
            node_id: net.node_id(),
            schedule_refresh: false,
        }),
        context.as_ref(),
    )
    .await?;

    let capabilities = NodeCapabilities::management_node(realm_signing_key)?;
    let state = Arc::new(
        ServerState::new(
            context.clone(),
            realm_id.clone(),
            net.node_id(),
            capabilities.clone(),
            false,
            None,
        )
        .await,
    );
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

    Ok(SeedNode {
        _temp_dir: temp_dir,
        net,
        context,
        realm_id,
        user_id,
        capabilities,
        base_url: format!("http://{}", addr),
        server_task,
    })
}

async fn create_onboarding_secret_via_http(
    seed: &SeedNode,
    mode: OnboardingMode,
) -> Result<String, Box<dyn std::error::Error>> {
    let _user = drive(
        RegisterUserOperation::new(RegisterUserInput {
            actor: Actor {
                node_id: seed.net.node_id(),
                user_id: seed.user_id,
                realm_id: seed.realm_id.clone(),
            },
            user_id: seed.user_id,
            name: format!("{}-ADMIN", seed.base_url),
            subject_ids: Vec::new(),
        }),
        &seed.context,
    )
    .await?;
    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            expiry: None,
            user_id: seed.user_id,
            realm_id: seed.realm_id.clone(),
            node_capabilities: seed.capabilities.clone(),
        })?,
        seed.context.as_ref(),
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
    assert_eq!(response.status(), StatusCode::CREATED);
    let response: CreateOnboardingSecretResponse = response.json().await?;
    Ok(response.onboarding_secret)
}

async fn wait_for_realm_nodes(
    contexts: &[&DriverContext],
    realm_id: &RealmId,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut done = true;
        for context in contexts {
            match drive(GetRealmNodesOperation::new(realm_id.clone()), context).await {
                Ok(nodes) if nodes.len() == expected => {}
                _ => {
                    done = false;
                    break;
                }
            }
        }
        if done {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for realm node convergence".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn onboarding_bootstraps_joiner_over_http_and_syncs_core_documents()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = env_lock().lock().await;
    let seed = spawn_seed_node().await?;
    sleep(Duration::from_millis(50)).await;
    let onboarding_secret = create_onboarding_secret_via_http(&seed, OnboardingMode::Local).await?;

    let joiner_dir = tempfile::tempdir()?;
    let vars = [
        (
            "STORAGE_PATH",
            joiner_dir.path().to_str().unwrap().to_string(),
        ),
        ("SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
        ("P2P_SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
        ("S3_PORT", "1337".to_string()),
        ("S3_HOST", "localhost".to_string()),
        ("S3_ADDRESS", "127.0.0.1".to_string()),
        ("ONBOARDING_SECRET", onboarding_secret),
    ];
    let previous: Vec<_> = vars
        .iter()
        .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
        .collect();
    for (key, value) in &vars {
        unsafe { std::env::set_var(key, value) };
    }

    let (config, storage_handle) = load().await?;

    for (key, value) in previous {
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    assert!(matches!(
        config.startup_mode,
        StartupMode::JoinRealm {
            phase: OnboardingPhase::Bootstrapped
        }
    ));

    let joiner_net = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_socket_addr,
            secret_key: Some(config.net_secret_key.clone()),
            realm_id: config.realm_id.clone(),
            bootstrap_nodes: config.bootstrap_nodes.clone(),
            use_dns_discovery: false,
        },
        storage_handle.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let automerge_handle = AutomergeHandle::new(Some(joiner_net.clone()));
    let joiner_context = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(joiner_net.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_handle),
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
    });
    initialize_net_incoming(joiner_context.clone());
    initialize_task_incoming(joiner_context.clone(), task_handle).await;
    for endpoint in &config.bootstrap_endpoints {
        joiner_net.add_peer_addr(endpoint.clone()).await;
    }
    seed.net.add_peer_addr(joiner_net.endpoint_addr()).await;
    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: seed.realm_id.clone(),
            node_id: seed.net.node_id(),
            schedule_refresh: false,
        }),
        seed.context.as_ref(),
    )
    .await?;

    fetch_core_onboarding_documents(
        joiner_context.as_ref(),
        &config.node_state,
        &config.realm_id,
        config
            .bootstrap_endpoints
            .first()
            .map(|endpoint| endpoint.id),
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
    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: config.realm_id.clone(),
            node_id: config.node_id,
            schedule_refresh: false,
        }),
        joiner_context.as_ref(),
    )
    .await?;

    wait_for_realm_nodes(
        &[seed.context.as_ref(), joiner_context.as_ref()],
        &config.realm_id,
        2,
    )
    .await?;

    seed.server_task.abort();
    joiner_net.shutdown().await;
    seed.net.shutdown().await;
    Ok(())
}
