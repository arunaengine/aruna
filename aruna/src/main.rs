#![allow(clippy::result_large_err)]

use aruna::bootstrap::{
    announce_core_documents, ensure_initial_local_onboarding_secret,
    fetch_core_onboarding_documents, realm_bootstrap_exists, wait_for_onboarding_placement,
};
use aruna::config::{Config, StartupMode, load, mark_node_state_complete, mark_onboarding_phase};
use aruna::portal;
use aruna::telemetry::{init_tracing, shutdown_tracing};
use aruna_api::auth::OidcValidator;
use aruna_api::cors::CorsConfig;
use aruna_api::csp::PortalCspConfig;
use aruna_api::ops::{OpsState, Readiness, serve_ops};
use aruna_api::s3::s3_server::S3Server;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::metrics::NodeMetrics;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::Backend::FileSystem;
use aruna_core::structs::BackendConfig;
use aruna_core::structs::NodeCapabilities;
use aruna_core::structs::{Actor, NodeUrls, RealmNodeKind};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::ensure_realm_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::jobs::JOB_SHUTDOWN_GRACE;
use aruna_operations::jobs::drain::restore_job_queue_timer;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions, spawn_metadata_warmup};
use aruna_operations::startup::restore_shard_subscriptions;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

fn main() {
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

async fn run() -> Result<(), Box<dyn std::error::Error>> {
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
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: FileSystem,
            root: config.blob_root.clone(),
            service_config: HashMap::new(),
            bucket_prefix: config.blob_bucket_prefix.clone(),
            max_bucket_size: config.blob_max_bucket_size,
            multipart_bucket: config.blob_multipart_bucket.clone(),
            timeouts: config.blob_timeout_config(),
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
    .await?;

    let compute_handle = build_compute_registry(&config)
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

    // Start the ops listener before realm bootstrap so `/readyz` reports 503
    // (startup) and `/metrics` is scrapable while the node is still coming up.
    let metrics = Arc::new(NodeMetrics::new());
    let readiness = Readiness::new();
    {
        let ops_state = OpsState::new(driver_ctx.clone(), metrics.clone(), readiness.clone()).await;
        let ops_listener = TcpListener::bind(config.ops_socket_addr).await?;
        let bound = ops_listener.local_addr()?;
        tokio::spawn(async move {
            if let Err(error) = serve_ops(ops_listener, ops_state).await {
                error!(error = %error, "Ops server stopped");
            }
        });
        info!(ops_address = %bound, "Ops server listening");
    }

    ensure_usage_counters(driver_ctx.as_ref()).await?;

    // Task initialization binds the compute reconciler before startup recovery.
    let jobs_runtime = JobsRuntime::new_paused();
    initialize_net_incoming(driver_ctx.clone());
    initialize_task_incoming(
        driver_ctx.clone(),
        task_handle.clone(),
        jobs_runtime.clone(),
    )
    .await;

    // Republish a full set of node usage snapshots at startup so realm peers see
    // this node's totals again after a restart, dirty-marker loss, or a counter
    // rebuild. Best-effort: failures are retried by the debounced publisher.
    if let Err(error) = aruna_operations::usage_stats::publish_and_refresh_usage_snapshots(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        true,
    )
    .await
    {
        warn!(error = %error, "Failed to publish initial node usage snapshots");
    }

    let replayed_metadata_events = replay_metadata_event_log(driver_ctx.as_ref()).await?;
    if replayed_metadata_events > 0 {
        info!(
            replayed_metadata_events,
            "Replayed metadata event log during startup"
        );
    }
    spawn_metadata_warmup(driver_ctx.clone());

    match &config.startup_mode {
        StartupMode::InitializeRealm { realm_description } => {
            if !realm_bootstrap_exists(driver_ctx.as_ref(), &config.realm_id).await? {
                drive(
                    CreateRealmOperation::new(CreateRealmConfig {
                        actor: Actor {
                            node_id: config.node_id,
                            user_id: UserId::nil(config.realm_id),
                            realm_id: config.realm_id,
                        },
                        realm_description: realm_description.clone(),
                        oidc_providers: config.oidc_providers.clone(),
                        node_location: config.node_location.clone(),
                        node_weight: config.node_weight,
                        node_labels: config.node_labels.clone(),
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }
            seed_local_node_info(driver_ctx.as_ref(), &config).await?;
            // CreateRealm mints the realm-auth/realm-config genesis via its admin
            // operation outbox records, but not the shared realm topics. Seed
            // NodeInfo first so its stored document is included in bootstrap.
            announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id, true)
                .await?;

            if config.is_initial_node() {
                match ensure_initial_local_onboarding_secret(
                    driver_ctx.as_ref(),
                    format!("http://{}", config.http_socket_addr),
                )
                .await
                {
                    Ok(secret) => warn!(
                        onboarding_secret = %secret
                            .encode()
                            .expect("initial onboarding secret encoding should succeed"),
                        "Created initial local onboarding secret for first user registration"
                    ),
                    Err(error) => {
                        return Err(format!(
                            "failed to create initial local onboarding secret: {error}"
                        )
                        .into());
                    }
                }
            }

            mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
        }
        StartupMode::JoinRealm { phase } => {
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
            seed_local_node_info(driver_ctx.as_ref(), &config).await?;
            // A joining node is never the realm-bootstrap node: it announces the
            // shared topics with allow_genesis=false after onboarding fetched a
            // representative target for each.
            announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id, false)
                .await?;
            mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
        }
        StartupMode::Provisioned => {
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
                        default_metadata_replication_factor: config
                            .default_metadata_replication_factor,
                        realm_description: config.realm_description.clone(),
                        create_if_missing: true,
                        reject_kind_mismatch: false,
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }

            seed_local_node_info(driver_ctx.as_ref(), &config).await?;
            // The realm-bootstrap node retains genesis authority so a missing
            // shared topic can be repaired after a partial first boot. Onboarded
            // nodes still wait for that authoritative genesis.
            announce_core_documents(
                driver_ctx.as_ref(),
                config.node_id,
                &config.realm_id,
                config.is_initial_node(),
            )
            .await?;
        }
    }

    // Republish a full set of node usage snapshots after startup-mode core
    // document announcement has had a chance to queue the shared node-usage topic
    // genesis. Best-effort: failures are retried by the debounced publisher.
    if let Err(error) = aruna_operations::usage_stats::publish_and_refresh_usage_snapshots(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        true,
    )
    .await
    {
        warn!(error = %error, "Failed to publish initial node usage snapshots");
    }

    // All startup modes: join the held shard topics (a freshly onboarded node
    // pulls existing shard data from its co-holders here), then create the
    // geneses of the shards this node is rank-0 holder of.
    let restore_summary =
        restore_shard_subscriptions(&driver_ctx, config.node_id, config.realm_id).await;
    tracing::info!(
        held_shards = restore_summary.held_shards,
        shard_topics = restore_summary.shard_topics,
        shared_topics = restore_summary.shared_topics,
        "Restored held shard subscriptions",
    );
    aruna_operations::process_placements::process_shard_placements(
        &driver_ctx,
        config.realm_id,
        config.node_id,
    )
    .await;

    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: config.realm_id,
            node_id: config.node_id,
            schedule_refresh: true,
        }),
        driver_ctx.as_ref(),
    )
    .await?;

    // REST Server
    let is_initial_node = config.is_initial_node();
    let state = Arc::new(
        ServerState::new(
            driver_ctx.clone(),
            config.realm_id,
            config.node_id,
            config.node_capabilities,
            is_initial_node,
            Some(Arc::new(OidcValidator::new()?)),
            jobs_runtime.clone(),
        )
        .await
        .with_metrics(metrics.clone()),
    );
    portal::initialize(config.portal.clone(), state.clone()).await;

    let cors = CorsConfig::new(config.cors_allowed_origins.clone());
    let server_config = ServerConfig {
        http_addr: config.http_socket_addr,
        max_http_body_size: config.max_http_body_size,
        cors: cors.clone(),
        portal_csp: PortalCspConfig::new(config.portal_csp_extra_origins.clone()),
    };
    let server = Server::new(state.clone(), server_config);

    // S3 Server
    let s3_server = S3Server::new(
        &config.s3_address,
        &config.s3_host,
        driver_ctx.clone(),
        config.realm_id,
        config.node_id,
        cors,
        metrics.clone(),
    )
    .await
    .unwrap();

    let s3_listener = TcpListener::bind(&config.s3_address).await.unwrap();
    let s3_bound_addr = s3_listener.local_addr().unwrap();
    state
        .register_s3_interface(
            s3_bound_addr,
            config.s3_public_url.as_deref().unwrap_or(&config.s3_host),
        )
        .await;
    let (_s3_addr, server_handle) = s3_server.run_with_listener(s3_listener).unwrap();
    if let Err(error) = jobs_runtime
        .recover_stale_jobs(&driver_ctx.storage_handle)
        .await
    {
        warn!(error = %error, "Failed to recover stale jobs at startup");
    }
    jobs_runtime.start();
    restore_job_queue_timer(&driver_ctx.storage_handle, &task_handle).await;

    let rest_listener = TcpListener::bind(config.http_socket_addr).await?;

    // Both request listeners are bound and bootstrap has completed: the node is
    // ready to serve, so `/readyz` may now return 200.
    readiness.set_ready();

    tokio::select! {
        res = server_handle => {
            match res {
                Ok(_) => info!("S3 Server stopped normally"),
                Err(e) => error!("S3 Server panicked: {:?}", e),
            }
        }
        res = server.run_with_listener(rest_listener) => {
            match res {
                Ok(_) => info!("REST Server stopped normally"),
                Err(e) => error!("REST Server panicked: {:?}", e),
            }
        }
        // You can add other signals here, e.g., Ctrl+C
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down S3 interface");
        }
    }

    shutdown_runtime(
        &readiness,
        driver_ctx.net_handle.as_ref(),
        driver_ctx.metadata_handle.as_ref(),
        &driver_ctx.storage_handle,
        jobs_runtime.clone(),
    )
    .await;

    Ok(())
}

async fn build_compute_registry(
    config: &Config,
) -> Result<Option<Arc<aruna_compute::ExecutorRegistry>>, String> {
    let selected = dotenvy::var("ARUNA_COMPUTE_EXECUTOR").unwrap_or_else(|_| "none".to_string());
    let result = match selected.as_str() {
        "none" => return Ok(None),
        "docker" => build_docker(config).await,
        "apptainer" => build_apptainer(config).await,
        "kubernetes" => build_kubernetes(config).await,
        other => Err(ComputeBuildError::Config(format!(
            "unknown ARUNA_COMPUTE_EXECUTOR `{other}`"
        ))),
    };
    match result {
        Ok(registry) => Ok(Some(Arc::new(registry))),
        Err(ComputeBuildError::Unavailable(error)) if env_true("ARUNA_COMPUTE_OPTIONAL") => {
            warn!(executor = %selected, reason = %error, "Compute executor unavailable; running without compute");
            Ok(None)
        }
        Err(ComputeBuildError::Config(error) | ComputeBuildError::Unavailable(error)) => Err(error),
    }
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
    let backend = aruna_compute::executor::kubernetes::KubernetesBackend::with_config(
        aruna_compute::KubernetesConfig {
            namespace: dotenvy::var("ARUNA_COMPUTE_K8S_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            storage_class,
            helper_image,
            pull_deadline: env_duration("ARUNA_COMPUTE_K8S_PULL_DEADLINE", 300)?,
            s3_cidrs,
            s3_port,
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

async fn shutdown_runtime(
    readiness: &Readiness,
    net_handle: Option<&NetHandle>,
    metadata_handle: Option<&MetadataHandle>,
    storage_handle: &StorageHandle,
    jobs_runtime: Arc<JobsRuntime>,
) {
    readiness.begin_drain();

    jobs_runtime
        .shutdown(storage_handle, JOB_SHUTDOWN_GRACE)
        .await;

    if let Some(net_handle) = net_handle {
        info!("Shutting down network services");
        net_handle.shutdown().await;
    }

    if let Some(metadata_handle) = metadata_handle {
        info!("Flushing metadata persistence");
        if let Err(error) = metadata_handle.flush_persistence().await {
            error!(error = %error, "Failed to flush metadata persistence during shutdown");
        }
    }

    if let Err(error) = storage_handle.sync_all().await {
        error!(error = %error, "Failed to sync storage during shutdown");
    }
}

#[cfg(feature = "docker")]
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

/// Ensures the maintained usage counter shards exist before background writes start.
async fn ensure_usage_counters(
    driver_ctx: &DriverContext,
) -> Result<(), Box<dyn std::error::Error>> {
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
            }
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(format!("usage counter probe failed: {error}").into());
        }
        other => {
            return Err(format!("usage counter probe received unexpected event: {other:?}").into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::{UsageCounters, usage_global_shard_keys};
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
    async fn shutdown_drains_syncs() {
        let readiness = Readiness::new();
        readiness.set_ready();
        let observed_readiness = readiness.clone();
        let (storage_handle, receiver) = StorageHandle::new();
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at, _in_flight) = receiver
                .recv()
                .expect("shutdown should request storage sync_all");
            assert!(
                observed_readiness.is_draining(),
                "readiness should drain before storage sync"
            );
            assert!(matches!(effect, StorageEffect::SyncAll));
            response_tx.send(StorageEvent::SyncAllFinished);
        });

        shutdown_runtime(&readiness, None, None, &storage_handle, JobsRuntime::new()).await;

        assert!(readiness.is_draining());
        worker.join().expect("storage responder should finish");
    }

    #[tokio::test]
    async fn ensure_usage_counters_rebuilds_missing_shards() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = aruna_storage::FjallStorage::open(
            temp.path().to_str().expect("temp path should be utf8"),
        )
        .expect("storage opens");
        let driver_ctx = test_driver_ctx(storage_handle.clone());

        ensure_usage_counters(&driver_ctx).await.unwrap();

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
        let (storage_handle, receiver) = StorageHandle::new();
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
        let (storage_handle, receiver) = StorageHandle::new();
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
