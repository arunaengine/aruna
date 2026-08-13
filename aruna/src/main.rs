#![allow(clippy::result_large_err)]
// The tracked recovery child overflows the default query depth in a fresh build.
#![recursion_limit = "256"]

use aruna::bootstrap::{
    ensure_initial_local_onboarding_secret, fetch_core_onboarding_documents,
    prepare_core_documents, publish_core_documents, realm_bootstrap_exists,
    wait_for_onboarding_placement,
};
use aruna::config::{
    Config, PortalConfig, StartupMode, load, mark_node_state_complete, mark_onboarding_phase,
};
use aruna::portal;
use aruna::shutdown::{NodeShutdown, arm_signal_exit, shutdown_grace_env, wait_for_signal};
use aruna::telemetry::{init_tracing, shutdown_tracing};
use aruna_api::auth::OidcValidator;
use aruna_api::cors::CorsConfig;
use aruna_api::csp::PortalCspConfig;
use aruna_api::ops::{OpsState, Readiness, serve_ops};
use aruna_api::s3::s3_server::S3Server;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::{BackendRegistry, BlobHandler};
use aruna_core::UserId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::egress::EgressPolicy;
use aruna_core::metrics::NodeMetrics;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::NodeCapabilities;
use aruna_core::structs::{Actor, NodeUrls, RealmNodeKind};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::ensure_realm_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use aruna_operations::incoming::initialize_net_holder;
use aruna_operations::jobs::drain::restore_job_queue_timer;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions, spawn_metadata_warmup};
use aruna_operations::replication::migration::migrate_legacy_sync;
#[cfg(debug_assertions)]
use aruna_operations::startup::RecoveryState;
use aruna_operations::startup::{
    RecoveryConfig, RecoveryStatus, prepare_shard_policy, run_recovery,
};
use aruna_operations::task_incoming::{TaskQueues, initialize_task_holder};
use aruna_tasks::TaskHandle;
#[cfg(debug_assertions)]
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[cfg(debug_assertions)]
struct CoreBarrier {
    path: PathBuf,
}

#[cfg(debug_assertions)]
impl Drop for CoreBarrier {
    fn drop(&mut self) {
        info!(
            event = "test.core_publication.joined",
            "Core publication joined"
        );
        if let Err(error) = std::fs::write(&self.path, b"joined") {
            warn!(error = %error, "Failed to record core publication join");
        }
    }
}

#[cfg(debug_assertions)]
async fn core_barrier() {
    let Ok(path) = std::env::var("ARUNA_TEST_CORE_PUBLICATION_BARRIER") else {
        return;
    };
    let path = PathBuf::from(path);
    if let Err(error) = std::fs::write(&path, b"active") {
        warn!(error = %error, "Failed to arm core publication test barrier");
        return;
    }
    let _barrier = CoreBarrier { path };
    std::future::pending::<()>().await;
}

async fn publish_core(
    core_ctx: Arc<DriverContext>,
    node_id: iroh::PublicKey,
    realm_id: aruna_core::structs::RealmId,
    allow_genesis: bool,
    documents: Vec<DocumentSyncTarget>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(debug_assertions)]
    {
        if documents.is_empty() {
            return Ok(());
        }
        core_barrier().await;
    }
    publish_core_documents(
        core_ctx.as_ref(),
        node_id,
        realm_id,
        allow_genesis,
        documents,
    )
    .await
}

#[cfg(debug_assertions)]
struct RecoveryBarrier {
    path: PathBuf,
}

#[cfg(debug_assertions)]
impl Drop for RecoveryBarrier {
    fn drop(&mut self) {
        info!(event = "test.recovery.joined", "Recovery child joined");
        if let Err(error) = std::fs::write(&self.path, b"joined") {
            warn!(error = %error, "Failed to record recovery join");
        }
    }
}

#[cfg(debug_assertions)]
async fn watch_recovery(
    barrier: &RecoveryBarrier,
    status: RecoveryStatus,
    cancelled: CancellationToken,
) {
    loop {
        if cancelled.is_cancelled() {
            return;
        }
        match status.snapshot().state {
            RecoveryState::Degraded => {
                if let Err(error) = std::fs::write(&barrier.path, b"active") {
                    warn!(error = %error, "Failed to record recovery activity");
                }
                cancelled.cancelled().await;
                return;
            }
            RecoveryState::Converged => return,
            RecoveryState::Pending | RecoveryState::Running => {}
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(debug_assertions)]
async fn recover_child(
    context: Arc<DriverContext>,
    config: RecoveryConfig,
    status: RecoveryStatus,
    cancelled: CancellationToken,
) {
    let Ok(path) = std::env::var("ARUNA_TEST_RECOVERY_BARRIER") else {
        run_recovery(context, config, status, cancelled).await;
        return;
    };
    let barrier = RecoveryBarrier {
        path: PathBuf::from(path),
    };
    tokio::join!(
        run_recovery(context, config, status.clone(), cancelled.clone()),
        watch_recovery(&barrier, status, cancelled),
    );
}

#[cfg(not(debug_assertions))]
async fn recover_child(
    context: Arc<DriverContext>,
    config: RecoveryConfig,
    status: RecoveryStatus,
    cancelled: CancellationToken,
) {
    run_recovery(context, config, status, cancelled).await;
}

fn main() {
    // Both ring and aws-lc-rs are in the graph; rustls needs one picked before any TLS init.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls ring crypto provider");
    if let Some(code) = aruna_compute::dispatch_helper() {
        std::process::exit(code);
    }
    async_main();
}

#[tokio::main]
async fn async_main() {
    dotenvy::dotenv().expect("Failed to load .env file");
    init_tracing();

    let result = run().await;
    shutdown_tracing();

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

struct Runtime {
    config: Config,
    driver_ctx: Arc<DriverContext>,
    net_handle: NetHandle,
    s3_mounts_available: bool,
    shutdown: Shutdown,
    metrics: Arc<NodeMetrics>,
    readiness: Readiness,
    recovery: RecoveryStatus,
    jobs_runtime: Arc<JobsRuntime>,
    task_handle: TaskHandle,
    task_queues: TaskQueues,
    usage_counters_rebuilt: bool,
    ops_handle: tokio::task::JoinHandle<()>,
}

async fn setup_runtime() -> Result<Runtime, Box<dyn std::error::Error>> {
    let (config, storage_handle) = load().await?;
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_socket_addr,
            secret_key: Some(config.net_secret_key.clone()),
            realm_id: config.realm_id,
            peer_nodes: config.peer_nodes.clone(),
            peer_endpoints: config.peer_endpoints.clone(),
            temporary_bootstrap_active: config.temporary_bootstrap_active,
            discovery_method: config.discovery_method.clone(),
            relay_method: config.relay_method.clone(),
            max_concurrent_uni_streams: config.max_concurrent_uni_streams,
            max_concurrent_bidi_streams: config.max_concurrent_bidi_streams,
            document_sync_storage_path: Some(config.document_sync_storage_path.clone()),
            document_sync_runtime: Some(config.document_sync_runtime),
            fjall_persist_policy: config.fjall_persist_policy,
        },
        storage_handle.clone(),
    )
    .await?;
    if let Err(error) = net_handle.reload_realm_peers().await {
        warn!(error = %error, "Failed to refresh realm peers from persisted config during startup");
    }
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new_with_options(
        &config.metadata_storage_path,
        config.node_id,
        storage_handle.clone(),
        Some(net_handle.clone()),
        Some(net_handle.document_sync_node()),
        Some(net_handle.document_sync_database()),
        MetadataHandleOptions::default()
            .with_search_storage(config.metadata_search_storage)
            .with_document_sync_persist_policy(config.fjall_persist_policy),
    )?;
    let blob_handle = BlobHandler::with_registry(
        BackendRegistry::from_config(&config.blob_backends).map_err(std::io::Error::other)?,
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::strict().with_deny(config.blob_backends.extra_deny.clone()),
    )
    .await?;

    let (compute_handle, s3_mounts_available) = build_compute_registry(&config)
        .await
        .map_err(std::io::Error::other)?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle.clone()),
        blob_handle: Some(blob_handle),
        metadata_handle: Some(metadata_handle),
        task_handle: Some(task_handle.clone()),
        compute_handle: compute_handle.clone(),
    });

    // One cancellation path for the whole node: background children register
    // here so an ordered shutdown can drain them before storage is sealed.
    let shutdown = Shutdown::new();

    // Start ops before realm bootstrap so readiness reports startup failure.
    let metrics = Arc::new(NodeMetrics::new());
    let readiness = Readiness::new();
    let recovery = RecoveryStatus::new();
    let ops_state = OpsState::with_recovery(
        driver_ctx.clone(),
        metrics.clone(),
        readiness.clone(),
        recovery.clone(),
    )
    .await;
    let ops_listener = TcpListener::bind(config.ops_socket_addr).await?;
    let bound = ops_listener.local_addr()?;
    let ops_handle = tokio::spawn(async move {
        if let Err(error) = serve_ops(ops_listener, ops_state).await {
            error!(error = %error, "Ops server stopped");
        }
    });
    info!(ops_address = %bound, "Ops server listening");

    // A rebuild is the only local evidence that counters were not carried over.
    let usage_counters_rebuilt = ensure_usage_counters(driver_ctx.as_ref()).await?;

    // Bind compute reconciliation before startup recovery.
    let jobs_runtime = JobsRuntime::new_paused();
    initialize_net_holder(
        driver_ctx.clone(),
        config.rocrate_limits.clone(),
        jobs_runtime.clone(),
        &shutdown,
    );
    let task_queues = initialize_task_holder(
        driver_ctx.clone(),
        task_handle.clone(),
        jobs_runtime.clone(),
        config.rocrate_limits.clone(),
    )
    .await;

    Ok(Runtime {
        config,
        driver_ctx,
        net_handle,
        s3_mounts_available,
        shutdown,
        metrics,
        readiness,
        recovery,
        jobs_runtime,
        task_handle,
        task_queues,
        usage_counters_rebuilt,
        ops_handle,
    })
}

struct CoreAnnouncement {
    documents: Vec<DocumentSyncTarget>,
    allow_genesis: bool,
}

async fn prepare_startup(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    net_handle: &NetHandle,
) -> Result<CoreAnnouncement, Box<dyn std::error::Error>> {
    let replayed_metadata_events = replay_metadata_event_log(driver_ctx.as_ref()).await?;
    if replayed_metadata_events > 0 {
        info!(
            replayed_metadata_events,
            "Replayed metadata event log during startup"
        );
    }

    let announcement = prepare_mode(config, driver_ctx, net_handle).await?;
    match migrate_legacy_sync(driver_ctx.as_ref(), config.node_id, config.realm_id).await {
        Ok(summary) if summary.failed > 0 => warn!(
            migrated = summary.migrated,
            skipped = summary.skipped,
            failed = summary.failed,
            "Legacy S3 replication migration incomplete; startup will retry"
        ),
        Ok(summary) if !summary.already_complete => info!(
            migrated = summary.migrated,
            skipped = summary.skipped,
            "Migrated legacy S3 replication configs"
        ),
        Ok(_) => {}
        Err(error) => warn!(%error, "Failed to migrate legacy S3 replication configs"),
    }

    // Prepare local topics before binding; remote convergence stays behind the gate.
    prepare_shard_policy(driver_ctx, config.node_id, config.realm_id).await;
    Ok(announcement)
}

async fn prepare_mode(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    net_handle: &NetHandle,
) -> Result<CoreAnnouncement, Box<dyn std::error::Error>> {
    match &config.startup_mode {
        StartupMode::InitializeRealm { realm_description } => {
            init_realm(config, driver_ctx, realm_description).await
        }
        StartupMode::JoinRealm { phase } => join_realm(config, driver_ctx, net_handle, phase).await,
        StartupMode::Provisioned => provision_realm(config, driver_ctx).await,
    }
}

async fn init_realm(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    realm_description: &str,
) -> Result<CoreAnnouncement, Box<dyn std::error::Error>> {
    if !realm_bootstrap_exists(driver_ctx.as_ref(), &config.realm_id).await? {
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id: config.node_id,
                    user_id: UserId::nil(config.realm_id),
                    realm_id: config.realm_id,
                },
                realm_description: realm_description.to_string(),
                oidc_providers: config.oidc_providers.clone(),
                node_location: config.node_location.clone(),
                node_weight: config.node_weight,
                node_labels: config.node_labels.clone(),
            }),
            driver_ctx.as_ref(),
        )
        .await?;
    }
    seed_local_node_info(driver_ctx.as_ref(), config).await?;
    let documents = prepare_core_documents(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        true,
        true,
    )
    .await?;

    if config.is_initial_node() {
        match ensure_initial_local_onboarding_secret(
            driver_ctx.as_ref(),
            format!("http://{}", config.http_socket_addr),
            &config.node_state.net_secret_key,
            config.realm_id,
        )
        .await
        {
            Ok(_) => info!("Created initial local onboarding secret for first user registration"),
            Err(error) => {
                return Err(
                    format!("failed to create initial local onboarding secret: {error}").into(),
                );
            }
        }
    }

    mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
    Ok(CoreAnnouncement {
        documents,
        allow_genesis: true,
    })
}

async fn join_realm(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    net_handle: &NetHandle,
    phase: &OnboardingPhase,
) -> Result<CoreAnnouncement, Box<dyn std::error::Error>> {
    let bootstrap_peer = config
        .peer_endpoints
        .first()
        .map(|endpoint| endpoint.id)
        .or_else(|| config.peer_nodes.first().copied());
    if matches!(phase, OnboardingPhase::Bootstrapped) {
        fetch_core_onboarding_documents(
            driver_ctx.as_ref(),
            &config.node_state,
            &config.realm_id,
            bootstrap_peer,
        )
        .await?;
    }
    wait_for_onboarding_placement(
        driver_ctx.as_ref(),
        config.realm_id,
        config.node_id,
        bootstrap_peer,
    )
    .await?;
    if matches!(phase, OnboardingPhase::Bootstrapped) {
        mark_onboarding_phase(
            &driver_ctx.storage_handle,
            &config.node_state,
            OnboardingPhase::CoreDocumentsFetched,
        )
        .await?;
        if let Err(error) = net_handle.reload_realm_peers().await {
            warn!(error = %error, "Failed to refresh realm peers after onboarding document fetch");
        }
    }
    seed_local_node_info(driver_ctx.as_ref(), config).await?;
    let documents = prepare_core_documents(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        false,
        true,
    )
    .await?;
    mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
    Ok(CoreAnnouncement {
        documents,
        allow_genesis: false,
    })
}

async fn provision_realm(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
) -> Result<CoreAnnouncement, Box<dyn std::error::Error>> {
    if matches!(
        &config.node_capabilities,
        NodeCapabilities::Management { .. }
    ) {
        drive(
            EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
                actor: Actor {
                    node_id: config.node_id,
                    user_id: UserId::nil(config.realm_id),
                    realm_id: config.realm_id,
                },
                target_node_id: config.node_id,
                target_node_kind: RealmNodeKind::Management,
                default_metadata_replication_factor: config.default_metadata_replication_factor,
                realm_description: config.realm_description.clone(),
                create_if_missing: true,
                reject_kind_mismatch: false,
            }),
            driver_ctx.as_ref(),
        )
        .await?;
    }

    seed_local_node_info(driver_ctx.as_ref(), config).await?;
    let allow_genesis = config.is_initial_node();
    let documents = prepare_core_documents(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        allow_genesis,
        false,
    )
    .await?;
    Ok(CoreAnnouncement {
        documents,
        allow_genesis,
    })
}

struct ServerBindings {
    rest_handle: tokio::task::JoinHandle<Result<(), aruna_api::error::ServerSetupError>>,
    s3_handle: tokio::task::JoinHandle<()>,
    portal_handle: Option<tokio::task::JoinHandle<()>>,
    realm_id: aruna_core::structs::RealmId,
    node_id: iroh::PublicKey,
    is_initial_boot: bool,
}

async fn bind_servers(
    config: Config,
    driver_ctx: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    metrics: Arc<NodeMetrics>,
    s3_mounts_available: bool,
    shutdown: &Shutdown,
) -> Result<ServerBindings, Box<dyn std::error::Error>> {
    let is_initial_node = config.is_initial_node();
    let is_initial_boot = !matches!(config.startup_mode, StartupMode::Provisioned);
    let state = Arc::new(
        ServerState::new(
            driver_ctx.clone(),
            config.realm_id,
            config.node_id,
            config.node_capabilities,
            is_initial_node,
            Some(Arc::new(OidcValidator::new()?)),
            jobs_runtime,
        )
        .await
        .with_metrics(metrics.clone())
        .with_rocrate_limits(config.rocrate_limits.clone())
        .with_s3_mounts(s3_mounts_available)
        .with_trusted_proxies(config.trusted_proxies.clone())
        .with_rate_limits(aruna_api::rate_limit::ApiRateLimits::new(
            config.rate_limits.ip_per_minute,
            config.rate_limits.ip_burst,
            config.rate_limits.principal_per_minute,
            config.rate_limits.principal_burst,
        ))
        .with_shutdown_token(shutdown.token()),
    );
    portal::initialize(config.portal.clone(), state.clone()).await;

    let cors = CorsConfig::new(config.cors_allowed_origins.clone());
    let server_config = ServerConfig {
        http_addr: config.http_socket_addr,
        max_http_body_size: config.max_http_body_size,
        cors: cors.clone(),
    };
    let server = Server::new(state.clone(), server_config)
        .with_api_public_url(config.api_public_url.clone());

    let portal_handle = bind_portal(
        &config.portal,
        config.api_public_url.as_deref(),
        PortalCspConfig::new(config.portal_csp_extra_origins.clone()),
        state.clone(),
        shutdown,
    )
    .await?;

    let s3_server = S3Server::new(
        &config.s3_address,
        &config.s3_host,
        driver_ctx,
        config.realm_id,
        config.node_id,
        aruna_core::credential_seal::CredentialSealKey::derive(&config.node_state.net_secret_key),
        config.rocrate_limits.clone(),
        cors,
        metrics,
    )
    .await
    .unwrap()
    .with_concurrency_limits(
        config.rate_limits.s3_max_connections as usize,
        config.rate_limits.s3_max_requests as usize,
    )
    .with_trusted_proxies(config.trusted_proxies.clone())
    .with_rate_limits(aruna_api::rate_limit::ApiRateLimits::new(
        config.rate_limits.ip_per_minute,
        config.rate_limits.ip_burst,
        config.rate_limits.principal_per_minute,
        config.rate_limits.principal_burst,
    ))
    .unwrap();

    let s3_listener = TcpListener::bind(&config.s3_address).await.unwrap();
    let s3_bound_addr = s3_listener.local_addr().unwrap();
    state
        .register_s3_interface(
            s3_bound_addr,
            config.s3_public_url.as_deref().unwrap_or(&config.s3_host),
        )
        .await;
    let (_s3_addr, s3_handle) = s3_server
        .run_with_listener(s3_listener, shutdown.token())
        .unwrap();

    let rest_listener = TcpListener::bind(config.http_socket_addr).await?;
    let rest_handle = tokio::spawn(server.run_with_listener(rest_listener, shutdown.token()));

    Ok(ServerBindings {
        rest_handle,
        s3_handle,
        portal_handle,
        realm_id: config.realm_id,
        node_id: config.node_id,
        is_initial_boot,
    })
}

/// Binds the portal SPA listener when a portal is configured. `API_PUBLIC_URL`
/// is required alongside it, so the served config always carries an absolute
/// API base.
async fn bind_portal(
    portal: &PortalConfig,
    api_public_url: Option<&str>,
    csp: PortalCspConfig,
    state: Arc<ServerState>,
    shutdown: &Shutdown,
) -> Result<Option<tokio::task::JoinHandle<()>>, Box<dyn std::error::Error>> {
    let PortalConfig::Artifact { socket_addr, .. } = portal else {
        return Ok(None);
    };
    let Some(api_public_url) = api_public_url else {
        return Ok(None);
    };

    let listener = TcpListener::bind(socket_addr).await?;
    let bound = listener.local_addr()?;
    let portal_config = aruna_api::portal::PortalConfig {
        api_public_url: api_public_url.to_string(),
        csp,
    };
    let token = shutdown.token();
    let handle = tokio::spawn(async move {
        if let Err(error) = aruna_api::portal::serve(listener, state, portal_config, token).await {
            error!(error = %error, "Portal server stopped");
        }
    });
    info!(portal_address = %bound, "Portal server listening");
    Ok(Some(handle))
}

/// Resolves when a configured portal listener exits, and never without one, so
/// a portal is supervised like the other ingress listeners while an unconfigured
/// portal never fails the node.
async fn portal_exit(handle: Option<&mut tokio::task::JoinHandle<()>>) -> String {
    match handle {
        Some(handle) => match handle.await {
            Ok(()) => "Portal server stopped unexpectedly".to_string(),
            Err(error) => format!("Portal server panicked: {error}"),
        },
        None => std::future::pending().await,
    }
}

struct Background {
    realm_id: aruna_core::structs::RealmId,
    node_id: iroh::PublicKey,
    is_initial_boot: bool,
    driver_ctx: Arc<DriverContext>,
    shutdown: Shutdown,
    readiness: Readiness,
    recovery: RecoveryStatus,
    jobs_runtime: Arc<JobsRuntime>,
    task_handle: TaskHandle,
    task_queues: TaskQueues,
    usage_counters_rebuilt: bool,
    core_announcement: CoreAnnouncement,
}

async fn start_background(background: Background) {
    let Background {
        realm_id,
        node_id,
        is_initial_boot,
        driver_ctx,
        shutdown,
        readiness,
        recovery,
        jobs_runtime,
        task_handle,
        task_queues,
        usage_counters_rebuilt,
        core_announcement,
    } = background;
    // Both listeners are bound and the local safety gate is satisfied.
    readiness.set_ready();

    let core_ctx = driver_ctx.clone();
    let core_cancelled = shutdown.token();
    let CoreAnnouncement {
        documents: core_documents,
        allow_genesis: allow_core_genesis,
    } = core_announcement;
    let core_publish = publish_core(
        core_ctx,
        node_id,
        realm_id,
        allow_core_genesis,
        core_documents,
    );
    shutdown.spawn(async move {
        tokio::select! {
            result = core_publish => {
                if let Err(error) = result {
                    warn!(error = ?error, "Failed to queue core document replication");
                }
            }
            _ = core_cancelled.cancelled() => {}
        }
    });

    // Durable background queues run after admission opens.
    if let Err(error) = jobs_runtime
        .recover_stale_jobs(&driver_ctx.storage_handle)
        .await
    {
        warn!(error = %error, "Failed to recover stale jobs at startup");
    }
    jobs_runtime.start();
    task_queues.start(&shutdown).await;
    restore_job_queue_timer(&driver_ctx.storage_handle, &task_handle).await;
    spawn_metadata_warmup(driver_ctx.clone(), &shutdown);

    let recovery_ctx = driver_ctx.clone();
    let recovery_config = RecoveryConfig {
        realm_id,
        node_id,
        // An unchanged restart republishes nothing: accepted outbox work and
        // document sync history already carry convergence.
        publish_full_usage: usage_counters_rebuilt || is_initial_boot,
    };
    let cancelled = shutdown.token();
    shutdown.spawn(async move {
        recover_child(recovery_ctx, recovery_config, recovery, cancelled).await;
    });
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Before any startup work: readiness opens and background children start
    // long before this point, and an uninstalled handler would let the signal
    // kill the node instead of draining it.
    let mut signal = tokio::spawn(wait_for_signal());

    let Runtime {
        config,
        driver_ctx,
        net_handle,
        s3_mounts_available,
        shutdown,
        metrics,
        readiness,
        recovery,
        jobs_runtime,
        task_handle,
        task_queues,
        usage_counters_rebuilt,
        ops_handle,
    } = setup_runtime().await?;

    let core_announcement = prepare_startup(&config, &driver_ctx, &net_handle).await?;

    let ServerBindings {
        rest_handle,
        s3_handle,
        mut portal_handle,
        realm_id,
        node_id,
        is_initial_boot,
    } = bind_servers(
        config,
        driver_ctx.clone(),
        jobs_runtime.clone(),
        metrics.clone(),
        s3_mounts_available,
        &shutdown,
    )
    .await?;

    start_background(Background {
        realm_id,
        node_id,
        is_initial_boot,
        driver_ctx: driver_ctx.clone(),
        shutdown: shutdown.clone(),
        readiness: readiness.clone(),
        recovery: recovery.clone(),
        jobs_runtime: jobs_runtime.clone(),
        task_handle: task_handle.clone(),
        task_queues,
        usage_counters_rebuilt,
        core_announcement,
    })
    .await;

    let mut rest_handle = Some(rest_handle);
    let mut s3_handle = Some(s3_handle);

    // A server that returns before shutdown was requested has failed: the node
    // is no longer serving, so it must not exit as success.
    let mut failure: Option<String> = None;
    tokio::select! {
        result = s3_handle.as_mut().expect("s3 server handle is present") => {
            s3_handle = None;
            failure = Some(match result {
                Ok(()) => "S3 server stopped unexpectedly".to_string(),
                Err(error) => format!("S3 server panicked: {error}"),
            });
        }
        result = rest_handle.as_mut().expect("rest server handle is present") => {
            rest_handle = None;
            failure = Some(match result {
                Ok(Ok(())) => "REST server stopped unexpectedly".to_string(),
                Ok(Err(error)) => format!("REST server failed: {error}"),
                Err(error) => format!("REST server panicked: {error}"),
            });
        }
        message = portal_exit(portal_handle.as_mut()) => {
            portal_handle = None;
            failure = Some(message);
        }
        _ = &mut signal => {}
    }

    if let Some(failure) = failure.as_ref() {
        error!(error = %failure, "Shutting down after a server failure");
    }

    // A second termination signal means "stop now". After a server failure no
    // signal has arrived yet, so wait for the first before arming escalation.
    if failure.is_some() {
        tokio::spawn(async move {
            let _ = signal.await;
            let _ = arm_signal_exit().await;
        });
    } else {
        arm_signal_exit();
    }

    NodeShutdown {
        shutdown,
        readiness,
        rest: rest_handle,
        s3: s3_handle,
        portal: portal_handle,
        task_handle,
        jobs_runtime,
        net_handle: driver_ctx.net_handle.clone(),
        metadata_handle: driver_ctx.metadata_handle.clone(),
        blob_handle: driver_ctx.blob_handle.clone(),
        storage_handle: driver_ctx.storage_handle.clone(),
        ops: Some(ops_handle),
        grace: shutdown_grace_env(),
    }
    .run()
    .await;

    match failure {
        Some(failure) => Err(failure.into()),
        None => Ok(()),
    }
}

async fn build_compute_registry(
    config: &Config,
) -> Result<(Option<Arc<aruna_compute::ExecutorRegistry>>, bool), String> {
    let selected = dotenvy::var("ARUNA_COMPUTE_EXECUTOR").unwrap_or_else(|_| "none".to_string());
    let result = match selected.as_str() {
        "none" => return Ok((None, false)),
        "docker" => build_docker(config).await,
        "apptainer" => build_apptainer(config).await,
        "kubernetes" => build_kubernetes(config).await,
        other => Err(ComputeBuildError::Config(format!(
            "unknown ARUNA_COMPUTE_EXECUTOR `{other}`"
        ))),
    };
    let registry = match result {
        Ok(registry) => Some(Arc::new(registry)),
        Err(ComputeBuildError::Unavailable(error)) if env_true("ARUNA_COMPUTE_OPTIONAL") => {
            warn!(executor = %selected, reason = %error, "Compute executor unavailable; running without compute");
            None
        }
        Err(ComputeBuildError::Config(error) | ComputeBuildError::Unavailable(error)) => {
            return Err(error);
        }
    };
    // S3 mounts are a local deployment property: Kubernetes with a CSI driver.
    let s3_mounts_available =
        registry.is_some() && selected == "kubernetes" && read_mount_driver().is_some();
    Ok((registry, s3_mounts_available))
}

fn read_mount_driver() -> Option<String> {
    dotenvy::var("ARUNA_COMPUTE_K8S_S3_MOUNT_DRIVER")
        .ok()
        .filter(|driver| !driver.is_empty())
}

enum ComputeBuildError {
    Config(String),
    Unavailable(String),
}

impl From<String> for ComputeBuildError {
    fn from(error: String) -> Self {
        Self::Config(error)
    }
}

impl From<&'static str> for ComputeBuildError {
    fn from(error: &'static str) -> Self {
        Self::Config(error.to_string())
    }
}

#[cfg(feature = "docker")]
async fn build_docker(
    config: &Config,
) -> Result<aruna_compute::ExecutorRegistry, ComputeBuildError> {
    let disk_bytes = parse_disk_limit(
        dotenvy::var("ARUNA_COMPUTE_DOCKER_DISK_BYTES")
            .ok()
            .as_deref(),
    )?;
    let endpoint = compute_s3_endpoint(config).ok_or_else(|| {
        "Docker executor requires ARUNA_COMPUTE_S3_URL or S3_PUBLIC_URL".to_string()
    })?;
    if container_local_endpoint(&endpoint) {
        return Err(
            "Docker executor requires a container-reachable S3_PUBLIC_URL"
                .to_string()
                .into(),
        );
    }
    if !config
        .s3_address
        .parse::<std::net::SocketAddr>()
        .is_ok_and(|address| !address.ip().is_loopback())
    {
        return Err("Docker executor requires a non-loopback S3_ADDRESS"
            .to_string()
            .into());
    }
    let docker_config = aruna_compute::DockerConfig {
        default_disk_bytes: disk_bytes,
        pull_deadline: env_duration("ARUNA_COMPUTE_DOCKER_PULL_DEADLINE", 300)?,
        ..aruna_compute::DockerConfig::default()
    };
    let backend = aruna_compute::executor::docker::DockerBackend::with_config(docker_config)
        .map_err(|error| error.to_string())?;
    aruna_compute::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!("Docker executor backend enabled");
    Ok(aruna_compute::ExecutorRegistry::new()
        .with_backend(Arc::new(backend))
        .with_workspace_endpoint(Some(endpoint), "eu-central-1".to_string()))
}

#[cfg(not(feature = "docker"))]
async fn build_docker(
    _config: &Config,
) -> Result<aruna_compute::ExecutorRegistry, ComputeBuildError> {
    Err("Docker executor feature is not compiled".to_string().into())
}

#[cfg(feature = "apptainer")]
async fn build_apptainer(
    config: &Config,
) -> Result<aruna_compute::ExecutorRegistry, ComputeBuildError> {
    let cgroup_root = dotenvy::var("ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT")
        .map(std::path::PathBuf::from)
        .map_err(|_| {
            "Apptainer executor requires ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT".to_string()
        })?;
    let state_root = dotenvy::var("ARUNA_COMPUTE_APPTAINER_STATE_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./compute-state/apptainer"));
    let sif_cache = dotenvy::var("ARUNA_COMPUTE_APPTAINER_SIF_CACHE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./compute-state/sif"));
    let backend = aruna_compute::executor::apptainer::ApptainerBackend::with_config(
        aruna_compute::ApptainerConfig {
            state_root,
            sif_cache,
            cgroup_root,
            stop_grace: env_duration("ARUNA_COMPUTE_STOP_GRACE", 10)?,
            pull_deadline: env_duration("ARUNA_COMPUTE_APPTAINER_PULL_DEADLINE", 300)?,
        },
    )
    .map_err(|error| error.to_string())?;
    aruna_compute::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!("Apptainer executor backend enabled");
    Ok(aruna_compute::ExecutorRegistry::new()
        .with_backend(Arc::new(backend))
        .with_workspace_endpoint(compute_s3_endpoint(config), "eu-central-1".to_string()))
}

#[cfg(not(feature = "apptainer"))]
async fn build_apptainer(
    _config: &Config,
) -> Result<aruna_compute::ExecutorRegistry, ComputeBuildError> {
    Err("Apptainer executor feature is not compiled"
        .to_string()
        .into())
}

#[cfg(feature = "kubernetes")]
async fn build_kubernetes(
    config: &Config,
) -> Result<aruna_compute::ExecutorRegistry, ComputeBuildError> {
    let storage_class = dotenvy::var("ARUNA_COMPUTE_K8S_STORAGE_CLASS")
        .map_err(|_| "Kubernetes executor requires ARUNA_COMPUTE_K8S_STORAGE_CLASS".to_string())?;
    let helper_image = dotenvy::var("ARUNA_COMPUTE_K8S_HELPER_IMAGE")
        .map_err(|_| "Kubernetes executor requires ARUNA_COMPUTE_K8S_HELPER_IMAGE".to_string())?;
    let s3_cidrs = dotenvy::var("ARUNA_COMPUTE_K8S_S3_CIDRS")
        .ok()
        .map(|value| parse_s3_cidrs(&value))
        .transpose()?
        .unwrap_or_default();
    let s3_port = dotenvy::var("ARUNA_COMPUTE_K8S_S3_PORT")
        .map(|value| value.parse::<u16>())
        .unwrap_or(Ok(443))
        .map_err(|_| "ARUNA_COMPUTE_K8S_S3_PORT must be a valid port".to_string())?;
    let s3_mount_driver = read_mount_driver();
    let backend = aruna_compute::executor::kubernetes::KubernetesBackend::with_config(
        aruna_compute::KubernetesConfig {
            namespace: dotenvy::var("ARUNA_COMPUTE_K8S_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            storage_class,
            helper_image,
            pull_deadline: env_duration("ARUNA_COMPUTE_K8S_PULL_DEADLINE", 300)?,
            s3_cidrs,
            s3_port,
            s3_mount_driver,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    aruna_compute::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!("Kubernetes executor backend enabled");
    Ok(aruna_compute::ExecutorRegistry::new()
        .with_backend(Arc::new(backend))
        .with_workspace_endpoint(compute_s3_endpoint(config), "eu-central-1".to_string()))
}

#[cfg(not(feature = "kubernetes"))]
async fn build_kubernetes(
    _config: &Config,
) -> Result<aruna_compute::ExecutorRegistry, ComputeBuildError> {
    Err("Kubernetes executor feature is not compiled"
        .to_string()
        .into())
}

fn env_true(name: &str) -> bool {
    dotenvy::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

#[cfg(feature = "kubernetes")]
fn parse_s3_cidrs(value: &str) -> Result<Vec<String>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|cidr| !cidr.is_empty())
        .map(|cidr| {
            let (address, prefix) = cidr
                .split_once('/')
                .ok_or_else(|| format!("invalid Kubernetes S3 CIDR `{cidr}`"))?;
            let address = address
                .parse::<std::net::IpAddr>()
                .map_err(|_| format!("invalid Kubernetes S3 CIDR `{cidr}`"))?;
            let prefix = prefix
                .parse::<u8>()
                .map_err(|_| format!("invalid Kubernetes S3 CIDR `{cidr}`"))?;
            let max_prefix = if address.is_ipv4() { 32 } else { 128 };
            if prefix > max_prefix {
                return Err(format!("invalid Kubernetes S3 CIDR `{cidr}`"));
            }
            Ok(cidr.to_string())
        })
        .collect()
}

#[cfg(any(feature = "docker", feature = "apptainer", feature = "kubernetes"))]
fn env_duration(name: &str, default: u64) -> Result<std::time::Duration, String> {
    let seconds = dotenvy::var(name)
        .map(|value| value.parse::<u64>())
        .unwrap_or(Ok(default))
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if seconds == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(std::time::Duration::from_secs(seconds))
}

#[cfg(any(feature = "docker", test))]
fn parse_disk_limit(value: Option<&str>) -> Result<Option<u64>, &'static str> {
    let Some(value) = value else {
        return Ok(None);
    };
    let bytes = value
        .parse::<u64>()
        .map_err(|_| "disk ceiling must be an integer byte count")?;
    if bytes == 0 {
        return Err("disk ceiling must be greater than zero");
    }
    Ok(Some(bytes))
}

async fn seed_local_node_info(ctx: &DriverContext, config: &Config) -> Result<(), String> {
    aruna_operations::node_info::seed_node_info_document(
        ctx,
        config.node_id,
        config.realm_id,
        NodeUrls {
            api: config.api_public_url.clone(),
            s3: config.s3_public_url.clone(),
        },
        ctx.compute_handle
            .as_ref()
            .map(|registry| registry.capabilities())
            .unwrap_or_default(),
    )
    .await
}

#[cfg(any(feature = "docker", feature = "apptainer", feature = "kubernetes"))]
/// Containers may need a different S3 endpoint than browsers: the override
/// keeps the portal-facing url on loopback (strict CSP) while container
/// workloads get a host-reachable one.
fn compute_s3_endpoint(config: &Config) -> Option<String> {
    dotenvy::var("ARUNA_COMPUTE_S3_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| config.s3_public_url.clone())
}

#[cfg(feature = "docker")]
fn container_local_endpoint(endpoint: &str) -> bool {
    let Some(host) = reqwest::Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
    else {
        return true;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback() || address.is_unspecified())
}

/// Ensures the maintained usage counter shards exist before background writes
/// start, and reports whether that required a rebuild.
async fn ensure_usage_counters(
    driver_ctx: &DriverContext,
) -> Result<bool, Box<dyn std::error::Error>> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::usage_global_shard_keys;
    use aruna_operations::usage_stats::RebuildUsageStatsOperation;

    let shard_keys = usage_global_shard_keys();
    let event = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads: shard_keys
                .iter()
                .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.clone().into()))
                .collect(),
            txn_id: None,
        })
        .await;

    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            if values.len() != shard_keys.len() {
                return Err(format!(
                    "usage counter probe returned {} values for {} shards",
                    values.len(),
                    shard_keys.len()
                )
                .into());
            }
            if values.iter().any(|(_, value)| value.is_none()) {
                drive(RebuildUsageStatsOperation::new(), driver_ctx).await?;
                return Ok(true);
            }
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(format!("usage counter probe failed: {error}").into());
        }
        other => {
            return Err(format!("usage counter probe received unexpected event: {other:?}").into());
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::{UsageCounters, usage_global_shard_keys};
    use aruna_storage::StorageHandle;
    use std::thread;
    use tempfile::tempdir;

    fn test_driver_ctx(storage_handle: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    #[test]
    fn accepts_disk_limit() {
        assert_eq!(
            parse_disk_limit(Some("10737418240")),
            Ok(Some(10_737_418_240))
        );
    }

    #[test]
    fn rejects_disk_limit() {
        assert_eq!(parse_disk_limit(None), Ok(None));
        assert!(parse_disk_limit(Some("invalid")).is_err());
        assert!(parse_disk_limit(Some("0")).is_err());
    }

    #[tokio::test]
    async fn portal_exit_reports() {
        // A dead portal is a node failure, and a panic must not lose its error.
        let mut stopped = tokio::spawn(async {});
        assert_eq!(
            portal_exit(Some(&mut stopped)).await,
            "Portal server stopped unexpectedly"
        );

        let mut panicked = tokio::spawn(async { panic!("portal panicked") });
        assert!(portal_exit(Some(&mut panicked)).await.contains("panicked"));
    }

    #[tokio::test(start_paused = true)]
    async fn portal_exit_pends() {
        // Without a configured portal the failure select must never fire for it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(60), portal_exit(None))
                .await
                .is_err()
        );
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn validates_k8s_cidrs() {
        assert_eq!(
            parse_s3_cidrs(" 10.0.0.0/8, 2001:db8::/32 ").unwrap(),
            ["10.0.0.0/8", "2001:db8::/32"]
        );
        assert!(parse_s3_cidrs("10.0.0.0/33").is_err());
        assert!(parse_s3_cidrs("2001:db8::/129").is_err());
        assert!(parse_s3_cidrs("invalid/8").is_err());
    }

    #[tokio::test]
    async fn ensure_usage_counters_rebuilds_missing_shards() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = aruna_storage::FjallStorage::open(
            temp.path().to_str().expect("temp path should be utf8"),
        )
        .expect("storage opens");
        let driver_ctx = test_driver_ctx(storage_handle.clone());

        assert!(
            ensure_usage_counters(&driver_ctx).await.unwrap(),
            "missing shards must report a rebuild"
        );
        assert!(
            !ensure_usage_counters(&driver_ctx).await.unwrap(),
            "intact shards must report no rebuild"
        );

        for key in usage_global_shard_keys() {
            let event = storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: USAGE_STATS_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: None,
                })
                .await;
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) = event
            else {
                panic!("expected rebuilt usage shard, got {event:?}");
            };
            assert_eq!(
                UsageCounters::from_bytes(bytes.as_ref()).unwrap(),
                UsageCounters::default()
            );
        }
    }

    #[tokio::test]
    async fn ensure_usage_counters_returns_probe_errors() {
        let (storage_handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at, _in_flight) = receiver
                .recv()
                .expect("usage counter ensure should probe storage");
            assert!(matches!(effect, StorageEffect::BatchRead { .. }));
            response_tx.send(StorageEvent::Error {
                error: StorageError::ReadError,
            });
        });
        let driver_ctx = test_driver_ctx(storage_handle);

        let error = ensure_usage_counters(&driver_ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("usage counter probe failed"));
        worker.join().expect("storage responder should finish");
    }

    #[tokio::test]
    async fn ensure_usage_counters_rejects_unexpected_probe_events() {
        let (storage_handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at, _in_flight) = receiver
                .recv()
                .expect("usage counter ensure should probe storage");
            assert!(matches!(effect, StorageEffect::BatchRead { .. }));
            response_tx.send(StorageEvent::SyncAllFinished);
        });
        let driver_ctx = test_driver_ctx(storage_handle);

        let error = ensure_usage_counters(&driver_ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("usage counter probe received unexpected event"));
        worker.join().expect("storage responder should finish");
    }
}
