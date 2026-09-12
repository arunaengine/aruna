//! Binding the node's ingress listeners and reporting their exit.

use std::sync::Arc;

use aruna_api::auth::OidcValidator;
use aruna_api::cors::CorsConfig;
use aruna_api::csp::PortalCspConfig;
use aruna_api::s3::server::{S3Server, S3ServerTimeouts};
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_core::metrics::NodeMetrics;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::NodeCapabilities;
use aruna_operations::device::wipe::DeviceWipe;
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::compute_setup::{session_s3_address, session_subnet};
use crate::config::{Config, PortalConfig, StartupMode};
use crate::portal;

pub(crate) struct ServerBindings {
    pub(crate) rest_handle: tokio::task::JoinHandle<Result<(), aruna_api::error::ServerSetupError>>,
    pub(crate) s3_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) portal_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) realm_id: aruna_core::structs::RealmId,
    pub(crate) node_id: iroh::PublicKey,
    pub(crate) is_initial_boot: bool,
    /// Present on a user node only: the owner's local wipe latch.
    pub(crate) device_wipe: Option<Arc<DeviceWipe>>,
}

/// Everything a wipe erases: the store root, every derived root, and every
/// filesystem backend. A backend this process cannot erase is named instead, so
/// the wipe reports an incomplete erasure rather than claiming a complete one.
fn wipe_plan(config: &Config) -> (Vec<std::path::PathBuf>, Vec<String>) {
    let (mut roots, unsupported) = backend_wipe(&config.blob_backends);
    roots.extend([
        std::path::PathBuf::from(&config.storage_path),
        std::path::PathBuf::from(&config.metadata_storage_path),
        config.document_sync_storage_path.clone(),
        std::path::PathBuf::from(&config.blob_root),
    ]);
    (crate::config::outermost_roots(&roots), unsupported)
}

/// The filesystem roots a wipe has to visit, and the backends it cannot erase.
fn backend_wipe(
    backends: &aruna_core::structs::NodeBackendsConfig,
) -> (Vec<std::path::PathBuf>, Vec<String>) {
    let mut roots = Vec::new();
    let mut unsupported = Vec::new();
    for entry in &backends.backends {
        match entry.config.backend_type {
            aruna_core::structs::Backend::FileSystem => {
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
    realm_id: aruna_core::structs::RealmId,
    node_id: iroh::PublicKey,
    key: aruna_core::credential_encryption::CredentialEncryptionKey,
    rocrate_limits: aruna_core::structs::RoCrateLimits,
}

/// Serves the node's S3 plane on the session bridge gateway too. A bind failure
/// is not fatal: only sessions lose their endpoint, the node keeps serving.
async fn bind_session_s3(
    session: Option<SessionS3>,
    s3_host: &str,
    driver_ctx: Arc<DriverContext>,
    cors: CorsConfig,
    metrics: Arc<NodeMetrics>,
    s3_timeouts: S3ServerTimeouts,
    shutdown: &Shutdown,
) {
    let Some(session) = session else {
        return;
    };
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
            return;
        }
    };
    let listener = match TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            warn!(address = %address, error = %error, "Session S3 endpoint could not bind");
            return;
        }
    };
    // The shutdown token stops it, so the handle needs no separate join.
    match server.run_with_listener(listener, shutdown.token()) {
        Ok(_) => info!(address = %address, "Session S3 endpoint listening"),
        Err(error) => warn!(address = %address, error = %error, "Session S3 endpoint failed"),
    }
}

pub(crate) async fn bind(
    config: Config,
    driver_ctx: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    metrics: Arc<NodeMetrics>,
    shutdown: &Shutdown,
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
    let mut session_s3 = session_s3_address(&config, &session_subnet())
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
            let (roots, unsupported) = wipe_plan(&config);
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
        config.node_capabilities,
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
        max_http_body_size: config.max_http_body_size,
        cors: cors.clone(),
    };
    let server = Server::new(state.clone(), server_config)
        .with_public_url(config.api_public_url.clone())
        .with_mcp_enabled(config.mcp_enabled);

    let portal_handle = bind_portal(
        &config.portal,
        config.api_public_url.as_deref(),
        PortalCspConfig::new(config.portal_csp_extra_origins.clone()),
        state.clone(),
        shutdown,
    )
    .await?;

    // A device serves S3 only where S3_HOST and S3_ADDRESS are configured; the
    // desktop shell sets both to loopback by default, and the pair stays whole.
    let driver_ctx_for_sessions = driver_ctx.clone();
    let cors_for_sessions = cors.clone();
    let metrics_for_sessions = metrics.clone();
    let session_s3 = session_s3.take();
    let s3_handle = match (config.s3_address.as_deref(), config.s3_host.as_deref()) {
        (Some(s3_address), Some(s3_host)) => {
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
            let (_s3_addr, s3_handle) =
                s3_server.run_with_listener(s3_listener, shutdown.token())?;
            bind_session_s3(
                session_s3,
                s3_host,
                driver_ctx_for_sessions,
                cors_for_sessions,
                metrics_for_sessions,
                s3_timeouts,
                shutdown,
            )
            .await;
            Some(s3_handle)
        }
        _ => None,
    };

    let rest_listener = TcpListener::bind(config.http_socket_addr).await?;
    let rest_handle = tokio::spawn(server.run_with_listener(rest_listener, shutdown.token()));

    Ok(ServerBindings {
        rest_handle,
        s3_handle,
        portal_handle,
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

pub(crate) async fn s3_exit(handle: Option<&mut tokio::task::JoinHandle<()>>) -> String {
    match handle {
        Some(handle) => match handle.await {
            Ok(()) => "S3 server stopped unexpectedly".to_string(),
            Err(error) => format!("S3 server panicked: {error}"),
        },
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
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn backend(
        name: &str,
        backend_type: aruna_core::structs::Backend,
        root: &str,
    ) -> aruna_core::structs::NodeBackendEntry {
        aruna_core::structs::NodeBackendEntry {
            name: name.to_string(),
            config: aruna_core::structs::BackendConfig {
                backend_type,
                root: root.to_string(),
                service_config: std::collections::HashMap::new(),
                bucket_prefix: None,
                max_bucket_size: None,
                multipart_bucket: None,
                timeouts: aruna_core::structs::BlobTimeoutConfig::default(),
            },
            class: None,
            allow_tenants: true,
            quota_bytes: None,
            cleanup: aruna_core::structs::CleanupStrategy::node_default(),
        }
    }

    // A relocated filesystem backend must be erased, and one this process
    // cannot erase must be named instead of quietly reported as wiped.
    #[test]
    fn wipe_covers_backends() {
        let backends = aruna_core::structs::NodeBackendsConfig {
            backends: vec![
                backend("hot", aruna_core::structs::Backend::FileSystem, "/srv/hot"),
                backend("cold", aruna_core::structs::Backend::S3, ""),
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
