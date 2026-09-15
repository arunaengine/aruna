//! Binding the node's ingress listeners and reporting their exit.

use std::sync::Arc;

use aruna_api::auth::OidcValidator;
use aruna_api::cors::CorsConfig;
use aruna_api::csp::PortalCspConfig;
use aruna_api::s3::server::{S3Server, S3ServerHandle, S3ServerTimeouts};
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_core::metrics::NodeMetrics;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_operations::device::wipe::DeviceWipe;
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::config::{Config, PortalConfig, StartupMode};
use crate::portal;

pub(crate) struct ServerBindings {
    pub(crate) rest_handle: tokio::task::JoinHandle<Result<(), aruna_api::error::ServerSetupError>>,
    /// S3 and session S3 carry their own connection-completion boundary, so a
    /// forced abort can still await every child's release.
    pub(crate) s3_handle: Option<S3ServerHandle>,
    pub(crate) portal_handle: Option<tokio::task::JoinHandle<()>>,
    /// The optional session bridge listener, joined with the other ingress
    /// listeners instead of being left to its cancellation token.
    pub(crate) session_s3_handle: Option<S3ServerHandle>,
    pub(crate) realm_id: aruna_core::structs::identity::realm::RealmId,
    pub(crate) node_id: iroh::PublicKey,
    pub(crate) is_initial_boot: bool,
    /// Present on a user node only: the owner's local wipe latch.
    pub(crate) device_wipe: Option<Arc<DeviceWipe>>,
}

/// The listener tasks started so far. `bind` registers each one here before the
/// next fallible step, so a partial bind aborts and awaits exactly the tasks it
/// started instead of leaking them into a failed startup.
#[derive(Default)]
struct StartedListeners {
    portal: Option<tokio::task::JoinHandle<()>>,
    s3: Option<S3ServerHandle>,
    session_s3: Option<S3ServerHandle>,
}

impl StartedListeners {
    /// Aborts and awaits every started listener in reverse start order. The S3
    /// handles await their connection children after the abort, so a partial
    /// bind releases every accepted connection, not just the accept loop.
    async fn abort_all(&mut self) {
        if let Some(handle) = self.session_s3.take() {
            handle.abort();
            handle.wait().await;
        }
        if let Some(handle) = self.s3.take() {
            handle.abort();
            handle.wait().await;
        }
        if let Some(handle) = self.portal.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// Everything a wipe erases: the store root, every derived root, and every
/// filesystem backend. A backend this process cannot erase is named instead, so
/// the wipe reports an incomplete erasure rather than claiming a complete one.
fn wipe_plan(config: &Config) -> (Vec<std::path::PathBuf>, Vec<String>) {
    let (mut roots, unsupported) = backend_wipe(&config.blob_backends);
    roots.extend([
        std::path::PathBuf::from(&config.storage_path),
        std::path::PathBuf::from(&config.metadata_storage_path),
        config.sync_storage_path.clone(),
        std::path::PathBuf::from(&config.blob_root),
    ]);
    (crate::config::outermost_roots(&roots), unsupported)
}

/// The filesystem roots a wipe has to visit, and the backends it cannot erase.
fn backend_wipe(
    backends: &aruna_core::structs::storage::backends::NodeBackendsConfig,
) -> (Vec<std::path::PathBuf>, Vec<String>) {
    let mut roots = Vec::new();
    let mut unsupported = Vec::new();
    for entry in &backends.backends {
        match entry.config.backend_type {
            aruna_core::structs::storage::blob::Backend::FileSystem => {
                roots.push(std::path::PathBuf::from(&entry.config.root));
            }
            _ => unsupported.push(entry.name.clone()),
        }
    }
    unsupported.sort();
    unsupported.dedup();
    (roots, unsupported)
}

/// Pends forever when this node serves no device plane, so the failure select
/// never fires for it.
pub(crate) async fn device_wipe_armed(wipe: Option<&Arc<DeviceWipe>>) {
    match wipe {
        Some(wipe) => wipe.wait().await,
        None => std::future::pending().await,
    }
}

/// What a second S3 listener on the session bridge needs, taken before the node
/// configuration is consumed.
struct SessionS3 {
    address: std::net::SocketAddr,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    node_id: iroh::PublicKey,
    key: aruna_core::credential_encryption::CredentialEncryptionKey,
    rocrate_limits: aruna_core::structs::execution::job::RoCrateLimits,
}

/// Serves the node's S3 plane on the session bridge gateway too. A bind failure
/// is not fatal (only sessions lose their endpoint); the returned handle is
/// joined by the caller's ingress owner, which the token only asks to stop.
async fn bind_session_s3(
    session: Option<SessionS3>,
    s3_host: &str,
    driver_ctx: Arc<DriverContext>,
    cors: CorsConfig,
    metrics: Arc<NodeMetrics>,
    s3_timeouts: S3ServerTimeouts,
    shutdown: &Shutdown,
) -> Option<S3ServerHandle> {
    let session = session?;
    let address = session.address;
    let server = match S3Server::new(
        &address.to_string(),
        s3_host,
        driver_ctx,
        session.realm_id,
        session.node_id,
        session.key,
        session.rocrate_limits,
        cors,
        metrics,
    )
    .await
    {
        Ok(server) => server.with_timeouts(s3_timeouts),
        Err(error) => {
            warn!(address = %address, error = %error, "Session S3 endpoint unavailable");
            return None;
        }
    };
    let listener = match TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            warn!(address = %address, error = %error, "Session S3 endpoint could not bind");
            return None;
        }
    };
    match server.run_with_listener(listener, shutdown.token()) {
        Ok((_, handle)) => {
            info!(address = %address, "Session S3 endpoint listening");
            Some(handle)
        }
        Err(error) => {
            warn!(address = %address, error = %error, "Session S3 endpoint failed");
            None
        }
    }
}

pub(crate) async fn bind(
    config: &Config,
    session_s3: Option<std::net::SocketAddr>,
    driver_ctx: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    metrics: Arc<NodeMetrics>,
    shutdown: &Shutdown,
) -> Result<ServerBindings, Box<dyn std::error::Error>> {
    let mut started = StartedListeners::default();
    match bind_all(
        config,
        session_s3,
        driver_ctx,
        jobs_runtime,
        metrics,
        shutdown,
        &mut started,
    )
    .await
    {
        Ok(bindings) => Ok(bindings),
        Err(error) => {
            started.abort_all().await;
            Err(error)
        }
    }
}

/// Binds every configured listener in order, registering each started task on
/// `started` before the next fallible step.
async fn bind_all(
    config: &Config,
    session_s3: Option<std::net::SocketAddr>,
    driver_ctx: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    metrics: Arc<NodeMetrics>,
    shutdown: &Shutdown,
    started: &mut StartedListeners,
) -> Result<ServerBindings, Box<dyn std::error::Error>> {
    let is_initial_node = config.is_initial_node();
    let is_initial_boot = !matches!(config.startup_mode, StartupMode::Provisioned);
    let s3_timeouts = config.s3_timeouts();
    // A wildcard S3 bind already answers on the gateway, so it needs no second
    // listener. Session containers still target the gateway address.
    let wildcard_s3 = config
        .s3_address
        .as_deref()
        .and_then(|address| address.parse::<std::net::SocketAddr>().ok())
        .is_some_and(|address| address.ip().is_unspecified());
    let session_s3 = session_s3
        .filter(|_| !wildcard_s3)
        .map(|address| SessionS3 {
            address,
            realm_id: config.realm_id,
            node_id: config.node_id,
            key: aruna_core::credential_encryption::CredentialEncryptionKey::derive(
                &config.node_state.net_secret_key,
            ),
            rocrate_limits: config.rocrate_limits.clone(),
        });
    let device_wipe = match matches!(config.node_capabilities, NodeCapabilities::User { .. }) {
        true => {
            let (roots, unsupported) = wipe_plan(config);
            // Reject unsafe wipe roots before startup and erase only their normalized paths.
            let roots = crate::config::validate_wipe_roots(&roots, dirs::home_dir().as_deref())?;
            Some(Arc::new(DeviceWipe::new(roots, unsupported)))
        }
        false => None,
    };
    let mut state = ServerState::new(
        driver_ctx.clone(),
        config.realm_id,
        config.node_id,
        config.node_capabilities.clone(),
        is_initial_node,
        Some(Arc::new(OidcValidator::new()?)),
        jobs_runtime,
    )
    .await
    .with_metrics(metrics.clone())
    .with_rocrate_limits(config.rocrate_limits.clone())
    .with_assistant_proxy(config.assistant_proxy)
    .with_trusted_proxies(config.trusted_proxies.clone())
    .with_rate_limits(aruna_api::rate_limit::ApiRateLimits::new(
        config.rate_limits.ip_per_minute,
        config.rate_limits.ip_burst,
        config.rate_limits.principal_per_minute,
        config.rate_limits.principal_burst,
    ))
    .with_shutdown_token(shutdown.token());
    if let Some(wipe) = device_wipe.clone() {
        state = state.with_device_wipe(wipe);
    }
    let state = Arc::new(state);
    portal::initialize(config.portal.clone(), state.clone()).await;

    let cors =
        CorsConfig::new(config.cors_allowed_origins.clone()).with_desktop(config.desktop_cors);
    let server_config = ServerConfig {
        http_addr: config.http_socket_addr,
        max_body_size: config.max_body_size,
        cors: cors.clone(),
    };
    let server = Server::new(state.clone(), server_config)
        .with_public_url(config.api_public_url.clone())
        .with_mcp_enabled(config.mcp_enabled);

    started.portal = bind_portal(
        &config.portal,
        config.api_public_url.as_deref(),
        PortalCspConfig::new(config.portal_csp_origins.clone()),
        state.clone(),
        shutdown,
    )
    .await?;

    // A device serves S3 only where S3_HOST and S3_ADDRESS are configured; the
    // desktop shell sets both to loopback by default, and the pair stays whole.
    let driver_ctx_for_sessions = driver_ctx.clone();
    let cors_for_sessions = cors.clone();
    let metrics_for_sessions = metrics.clone();
    if let (Some(s3_address), Some(s3_host)) =
        (config.s3_address.as_deref(), config.s3_host.as_deref())
    {
        let s3_server = S3Server::new(
            s3_address,
            s3_host,
            driver_ctx,
            config.realm_id,
            config.node_id,
            aruna_core::credential_encryption::CredentialEncryptionKey::derive(
                &config.node_state.net_secret_key,
            ),
            config.rocrate_limits.clone(),
            cors,
            metrics,
        )
        .await?
        .with_concurrency_limits(
            config.rate_limits.s3_max_connections as usize,
            config.rate_limits.s3_max_requests as usize,
        )
        .with_timeouts(s3_timeouts)
        .with_trusted_proxies(config.trusted_proxies.clone())
        .with_rate_limits(aruna_api::rate_limit::ApiRateLimits::new(
            config.rate_limits.ip_per_minute,
            config.rate_limits.ip_burst,
            config.rate_limits.principal_per_minute,
            config.rate_limits.principal_burst,
        ))?;

        let s3_listener = TcpListener::bind(s3_address).await?;
        let s3_bound_addr = s3_listener.local_addr()?;
        state
            .register_s3_interface(
                s3_bound_addr,
                config.s3_public_url.as_deref().unwrap_or(s3_host),
            )
            .await;
        let (_s3_addr, s3_handle) = s3_server.run_with_listener(s3_listener, shutdown.token())?;
        started.s3 = Some(s3_handle);
        started.session_s3 = bind_session_s3(
            session_s3,
            s3_host,
            driver_ctx_for_sessions,
            cors_for_sessions,
            metrics_for_sessions,
            s3_timeouts,
            shutdown,
        )
        .await;
    }

    let rest_listener = TcpListener::bind(config.http_socket_addr).await?;
    let rest_handle = tokio::spawn(server.run_with_listener(rest_listener, shutdown.token()));

    Ok(ServerBindings {
        rest_handle,
        s3_handle: started.s3.take(),
        portal_handle: started.portal.take(),
        session_s3_handle: started.session_s3.take(),
        realm_id: config.realm_id,
        node_id: config.node_id,
        is_initial_boot,
        device_wipe,
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
pub(crate) async fn portal_exit(handle: Option<&mut tokio::task::JoinHandle<()>>) -> String {
    match handle {
        Some(handle) => match handle.await {
            Ok(()) => "Portal server stopped unexpectedly".to_string(),
            Err(error) => format!("Portal server panicked: {error}"),
        },
        None => std::future::pending().await,
    }
}

pub(crate) async fn s3_exit(handle: Option<&mut S3ServerHandle>) -> String {
    match handle {
        Some(handle) => handle.exit("S3").await,
        None => std::future::pending().await,
    }
}

/// Resolves when the optional session bridge listener exits, and never without
/// one, so its exit is reported without stopping the node.
pub(crate) async fn session_s3_exit(handle: Option<&mut S3ServerHandle>) -> String {
    match handle {
        Some(handle) => handle.exit("Session S3").await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn s3_exit_pends() {
        // Without an S3 listener the failure select must never fire for it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(60), s3_exit(None))
                .await
                .is_err()
        );
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

    #[tokio::test]
    async fn session_exit_reports() {
        use aruna_core::metrics::NodeMetrics;
        use aruna_core::shutdown::Shutdown;
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::execution::job::RoCrateLimits;

        // A real session listener that stopped on its shutdown token is
        // reported, never treated as a node failure.
        let temp = tempfile::tempdir().expect("temp dir");
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().expect("utf8 path"))
            .expect("storage opens");
        let driver_ctx = std::sync::Arc::new(aruna_operations::driver::DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let secret = iroh::SecretKey::from_bytes(&[0x46; 32]);
        let shutdown = Shutdown::new();
        let mut handle = bind_session_s3(
            Some(SessionS3 {
                address: "127.0.0.1:0".parse().expect("loopback address"),
                realm_id: RealmId::from_bytes([7u8; 32]),
                node_id: secret.public(),
                key: aruna_core::credential_encryption::CredentialEncryptionKey::derive(
                    &secret.to_bytes(),
                ),
                rocrate_limits: RoCrateLimits::default(),
            }),
            "127.0.0.1",
            driver_ctx,
            CorsConfig::default(),
            std::sync::Arc::new(NodeMetrics::new()),
            S3ServerTimeouts::default(),
            &shutdown,
        )
        .await
        .expect("the session S3 listener binds on an ephemeral loopback port");

        shutdown.trigger();
        let message = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            session_s3_exit(Some(&mut handle)),
        )
        .await
        .expect("the session listener must stop on the shutdown token");
        assert_eq!(message, "Session S3 server stopped unexpectedly");
    }

    #[tokio::test(start_paused = true)]
    async fn session_exit_pends() {
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(60), session_s3_exit(None))
                .await
                .is_err()
        );
    }

    /// Increments on drop, so the test can observe that an aborted start task
    /// really stopped and that `abort_all` waited for it.
    struct DropCounter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn partial_bind_cleanup() {
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut started = StartedListeners::default();
        let counter = DropCounter(dropped.clone());
        started.portal = Some(tokio::spawn(async move {
            let _counter = counter;
            std::future::pending::<()>().await;
        }));

        started.abort_all().await;

        assert_eq!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the started listener task must be aborted and awaited"
        );
        assert!(started.portal.is_none());
        assert!(started.s3.is_none());
        assert!(started.session_s3.is_none());
    }

    // A failure after the S3 listener started but before the REST listener did
    // must release the S3 port instead of leaving a half-bound server behind.
    #[tokio::test]
    async fn bind_failure_releases() {
        use aruna_core::metrics::NodeMetrics;
        use aruna_core::shutdown::Shutdown;
        use aruna_operations::jobs::runtime::JobsRuntime;

        let temp = tempfile::tempdir().expect("temp dir");
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").expect("occupied port");
        let rest_addr = occupied.local_addr().expect("occupied addr");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("s3 port probe");
        let s3_addr = probe.local_addr().expect("s3 addr");
        drop(probe);

        let map: std::collections::BTreeMap<String, String> = [
            (
                "STORAGE_PATH".to_string(),
                temp.path().to_str().expect("utf8 path").to_string(),
            ),
            ("SOCKET_ADDRESS".to_string(), rest_addr.to_string()),
            ("P2P_SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("S3_HOST".to_string(), "127.0.0.1".to_string()),
            ("S3_ADDRESS".to_string(), s3_addr.to_string()),
            ("PORTAL_MODE".to_string(), "disabled".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE".to_string(), "buffer".to_string()),
        ]
        .into_iter()
        .collect();
        let (config, storage) = crate::config::resolve_settings(
            crate::settings::read_settings_from(&map).expect("settings parse"),
        )
        .await
        .expect("settings resolve");
        let driver_ctx = std::sync::Arc::new(aruna_operations::driver::DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });

        let error = match bind(
            &config,
            None,
            driver_ctx,
            JobsRuntime::new_paused(),
            std::sync::Arc::new(NodeMetrics::new()),
            &Shutdown::new(),
        )
        .await
        {
            Ok(_) => panic!("the occupied REST port must fail the bind"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("Address already in use")
                || error.to_string().contains("address already in use"),
            "expected a bind failure, got {error}"
        );
        // The aborted S3 listener dropped its socket, so the port binds again.
        let rebound =
            std::net::TcpListener::bind(s3_addr).expect("the failed bind must release the port");
        drop(rebound);
        drop(occupied);
    }

    #[tokio::test]
    async fn absent_session_unbound() {
        let started = bind_session_s3(
            None,
            "127.0.0.1",
            std::sync::Arc::new(aruna_operations::driver::DriverContext {
                storage_handle: aruna_storage::StorageHandle::new().0,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            CorsConfig::default(),
            std::sync::Arc::new(aruna_core::metrics::NodeMetrics::new()),
            S3ServerTimeouts::default(),
            &aruna_core::shutdown::Shutdown::new(),
        )
        .await;

        assert!(started.is_none());
    }

    // A successful optional bind returns a retained owner whose task completes
    // once the shutdown token fires, so a later drain can await it.
    #[tokio::test]
    async fn session_listener_completes() {
        use aruna_core::metrics::NodeMetrics;
        use aruna_core::shutdown::Shutdown;
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::execution::job::RoCrateLimits;

        let temp = tempfile::tempdir().expect("temp dir");
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().expect("utf8 path"))
            .expect("storage opens");
        let driver_ctx = std::sync::Arc::new(aruna_operations::driver::DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let secret = iroh::SecretKey::from_bytes(&[0x44; 32]);
        let session = SessionS3 {
            address: "127.0.0.1:0".parse().expect("loopback address"),
            realm_id: RealmId::from_bytes([7u8; 32]),
            node_id: secret.public(),
            key: aruna_core::credential_encryption::CredentialEncryptionKey::derive(
                &secret.to_bytes(),
            ),
            rocrate_limits: RoCrateLimits::default(),
        };
        let shutdown = Shutdown::new();
        let handle = bind_session_s3(
            Some(session),
            "127.0.0.1",
            driver_ctx,
            CorsConfig::default(),
            std::sync::Arc::new(NodeMetrics::new()),
            S3ServerTimeouts::default(),
            &shutdown,
        )
        .await
        .expect("the session S3 listener binds on an ephemeral loopback port");

        shutdown.trigger();
        tokio::time::timeout(std::time::Duration::from_secs(10), handle.wait())
            .await
            .expect("the retained session listener must stop on the shutdown token");
    }

    // A forced abort of the session listener still awaits its children; the
    // port releases only once `wait` returned.
    #[tokio::test]
    async fn abort_awaits_children() {
        use aruna_core::metrics::NodeMetrics;
        use aruna_core::shutdown::Shutdown;
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::execution::job::RoCrateLimits;

        let temp = tempfile::tempdir().expect("temp dir");
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().expect("utf8 path"))
            .expect("storage opens");
        let driver_ctx = std::sync::Arc::new(aruna_operations::driver::DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let secret = iroh::SecretKey::from_bytes(&[0x45; 32]);
        let session = SessionS3 {
            address: "127.0.0.1:0".parse().expect("loopback address"),
            realm_id: RealmId::from_bytes([7u8; 32]),
            node_id: secret.public(),
            key: aruna_core::credential_encryption::CredentialEncryptionKey::derive(
                &secret.to_bytes(),
            ),
            rocrate_limits: RoCrateLimits::default(),
        };
        let handle = bind_session_s3(
            Some(session),
            "127.0.0.1",
            driver_ctx,
            CorsConfig::default(),
            std::sync::Arc::new(NodeMetrics::new()),
            S3ServerTimeouts::default(),
            &Shutdown::new(),
        )
        .await
        .expect("the session S3 listener binds on an ephemeral loopback port");

        handle.abort();
        tokio::time::timeout(std::time::Duration::from_secs(10), handle.wait())
            .await
            .expect("a forced abort must still await every connection child");
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn backend(
        name: &str,
        backend_type: aruna_core::structs::storage::blob::Backend,
        root: &str,
    ) -> aruna_core::structs::storage::backends::NodeBackendEntry {
        aruna_core::structs::storage::backends::NodeBackendEntry {
            name: name.to_string(),
            config: aruna_core::structs::storage::blob::BackendConfig {
                backend_type,
                root: root.to_string(),
                service_config: std::collections::HashMap::new(),
                bucket_prefix: None,
                max_bucket_size: None,
                multipart_bucket: None,
                timeouts: aruna_core::structs::storage::blob::BlobTimeoutConfig::default(),
            },
            class: None,
            allow_tenants: true,
            quota_bytes: None,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::node_default(),
        }
    }

    // A relocated filesystem backend must be erased, and one this process
    // cannot erase must be named instead of quietly reported as wiped.
    #[test]
    fn wipe_covers_backends() {
        let backends = aruna_core::structs::storage::backends::NodeBackendsConfig {
            backends: vec![
                backend("hot", aruna_core::structs::storage::blob::Backend::FileSystem, "/srv/hot"),
                backend("cold", aruna_core::structs::storage::blob::Backend::S3, ""),
            ],
            default_name: "hot".to_string(),
            rules: Vec::new(),
            serve_group_backends: true,
            extra_deny: Vec::new(),
        };

        let (roots, unsupported) = backend_wipe(&backends);
        assert_eq!(roots, vec![std::path::PathBuf::from("/srv/hot")]);
        assert_eq!(unsupported, vec!["cold".to_string()]);
    }
}
