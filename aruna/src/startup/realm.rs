//! Realm preparation during startup: metadata replay, realm bootstrap or
//! joining, placement, and node-info seeding.

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::storage::node_info::NodeUrls;
use aruna_net::NetHandle;
use aruna_operations::device::realm_documents::fetch_realm_documents;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::projector::replay_until_stopped;
use aruna_operations::node::startup::prepare_shard_policy;
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::realm::ensure_config::{EnsureConfigOperation, EnsureConfigParams};
use tracing::{info, warn};

use crate::bootstrap::{
    ensure_onboarding_secret, fetch_core_documents, prepare_core_documents, realm_bootstrap_exists,
    wait_for_placement,
};
use crate::config::{Config, StartupMode};
use crate::identity::{mark_onboarding_phase, mark_state_complete};

pub(crate) struct CoreAnnouncement {
    pub(crate) documents: Vec<DocumentTarget>,
    pub(crate) allow_genesis: bool,
}

/// How long a device waits for the realm documents before it serves anyway.
const STARTUP_DOCUMENT_FETCH: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) async fn prepare(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    net_handle: &NetHandle,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<Option<CoreAnnouncement>, Box<dyn std::error::Error>> {
    if stop.is_cancelled() {
        return Ok(None);
    }
    let replayed_metadata_events =
        replay_until_stopped(driver_ctx.as_ref(), || !stop.is_cancelled()).await?;
    if replayed_metadata_events > 0 {
        info!(
            replayed_metadata_events,
            "Replayed metadata event log during startup"
        );
    }
    // A replay can take a while on a large log; a stop accepted during it must
    // not proceed into realm bootstrap.
    if stop.is_cancelled() {
        return Ok(None);
    }

    let announcement = prepare_mode(config, driver_ctx, net_handle, stop).await?;
    let Some(announcement) = announcement else {
        return Ok(None);
    };

    // Prepare local topics before binding; remote convergence stays behind the gate.
    prepare_shard_policy(driver_ctx, config.node_id, config.realm_id).await;
    if stop.is_cancelled() {
        return Ok(None);
    }
    // Devices fetch governing realm documents before serving because they run no sync.
    // A short attempt lets stored copies serve while heartbeat retries an unreachable realm.
    if matches!(config.node_capabilities, NodeCapabilities::User { .. })
        && !fetch_realm_documents(driver_ctx, STARTUP_DOCUMENT_FETCH).await
    {
        warn!("Serving this device from its stored realm documents for now");
    }
    if stop.is_cancelled() {
        return Ok(None);
    }
    Ok(Some(announcement))
}

async fn prepare_mode(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    net_handle: &NetHandle,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<Option<CoreAnnouncement>, Box<dyn std::error::Error>> {
    match &config.startup_mode {
        StartupMode::InitializeRealm { realm_description } => {
            init_realm(config, driver_ctx, realm_description, stop).await
        }
        StartupMode::JoinRealm { phase } => {
            join_realm(config, driver_ctx, net_handle, phase, stop).await
        }
        StartupMode::Provisioned => provision_realm(config, driver_ctx, stop).await,
    }
}

async fn init_realm(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    realm_description: &str,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<Option<CoreAnnouncement>, Box<dyn std::error::Error>> {
    if stop.is_cancelled() {
        return Ok(None);
    }
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
    if stop.is_cancelled() {
        return Ok(None);
    }
    // The subject comes first: the advertisement built from it carries no
    // execution target while this node has no placement subject yet.
    sync_placement_subject(driver_ctx.as_ref(), config).await?;
    seed_node_info(driver_ctx.as_ref(), config).await?;
    let documents = prepare_core_documents(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        true,
        true,
    )
    .await?;

    if config.is_initial_node() {
        match ensure_onboarding_secret(
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

    if stop.is_cancelled() {
        return Ok(None);
    }
    mark_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
    Ok(Some(CoreAnnouncement {
        documents,
        allow_genesis: true,
    }))
}

async fn join_realm(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    net_handle: &NetHandle,
    phase: &OnboardingPhase,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<Option<CoreAnnouncement>, Box<dyn std::error::Error>> {
    if stop.is_cancelled() {
        return Ok(None);
    }
    let bootstrap_peer = config
        .peer_endpoints
        .first()
        .map(|endpoint| endpoint.id)
        .or_else(|| config.peer_nodes.first().copied());
    if matches!(phase, OnboardingPhase::Bootstrapped) {
        fetch_core_documents(
            driver_ctx,
            &config.node_state,
            &config.realm_id,
            bootstrap_peer,
            config.onboarding_sync_timeout(),
        )
        .await?;
    }
    if stop.is_cancelled() {
        return Ok(None);
    }
    wait_for_placement(
        driver_ctx,
        config.realm_id,
        config.node_id,
        config.device_owner(),
        bootstrap_peer,
        config.onboarding_sync_timeout(),
    )
    .await?;
    if stop.is_cancelled() {
        return Ok(None);
    }
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
    sync_placement_subject(driver_ctx.as_ref(), config).await?;
    seed_node_info(driver_ctx.as_ref(), config).await?;
    let documents = match is_device(config) {
        true => Vec::new(),
        false => {
            prepare_core_documents(
                driver_ctx.as_ref(),
                config.node_id,
                config.realm_id,
                false,
                true,
            )
            .await?
        }
    };
    if stop.is_cancelled() {
        return Ok(None);
    }
    mark_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
    Ok(Some(CoreAnnouncement {
        documents,
        allow_genesis: false,
    }))
}

/// A device reads the realm's documents over metadata and publishes none of
/// its own over sync, so it never joins or announces a sync topic.
fn is_device(config: &Config) -> bool {
    matches!(config.node_capabilities, NodeCapabilities::User { .. })
}

async fn provision_realm(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<Option<CoreAnnouncement>, Box<dyn std::error::Error>> {
    if stop.is_cancelled() {
        return Ok(None);
    }
    if matches!(
        &config.node_capabilities,
        NodeCapabilities::Management { .. }
    ) {
        drive(
            EnsureConfigOperation::new(EnsureConfigParams {
                actor: Actor {
                    node_id: config.node_id,
                    user_id: UserId::nil(config.realm_id),
                    realm_id: config.realm_id,
                },
                target_node_id: config.node_id,
                target_node_kind: RealmNodeKind::Management,
                metadata_replication_factor: config.metadata_replication_factor,
                realm_description: config.realm_description.clone(),
                create_if_missing: true,
                reject_kind_mismatch: false,
            }),
            driver_ctx.as_ref(),
        )
        .await?;
    }

    sync_placement_subject(driver_ctx.as_ref(), config).await?;
    seed_node_info(driver_ctx.as_ref(), config).await?;
    let allow_genesis = config.is_initial_node();
    let documents = match is_device(config) {
        true => Vec::new(),
        false => {
            prepare_core_documents(
                driver_ctx.as_ref(),
                config.node_id,
                config.realm_id,
                allow_genesis,
                false,
            )
            .await?
        }
    };
    if stop.is_cancelled() {
        return Ok(None);
    }
    Ok(Some(CoreAnnouncement {
        documents,
        allow_genesis,
    }))
}

/// Reconciles this node's advertised placement subject with the realm's
/// placement map before it serves anything. A changed subject blocks governed
/// serving until the local inventory has been revalidated under it.
async fn sync_placement_subject(ctx: &DriverContext, config: &Config) -> Result<(), String> {
    aruna_operations::placement::policy::sync_subject(
        ctx,
        config.realm_id,
        config.node_id,
        aruna_operations::placement::policy::SubjectScanMode::Revalidate(
            aruna_core::structs::storage::blob::ManagedCopyQuarantine::Rejoin,
        ),
        aruna_operations::driver::now_ms(),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

async fn seed_node_info(ctx: &DriverContext, config: &Config) -> Result<(), String> {
    aruna_operations::node::node_info::seed_info_document(
        ctx,
        config.node_id,
        config.realm_id,
        NodeUrls {
            api: config.api_public_url.clone(),
            s3: config.s3_public_url.clone(),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};

    // A stop accepted before preparation starts must return cleanly without
    // touching storage or the network, so the caller can release the acquired
    // resources and report a startup cancellation.
    #[tokio::test]
    async fn cancel_stops_preparation() {
        let temp = tempfile::tempdir().expect("temp dir");
        let map: std::collections::BTreeMap<String, String> = [
            (
                "STORAGE_PATH".to_string(),
                temp.path().to_str().expect("utf8 path").to_string(),
            ),
            ("SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("P2P_SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("S3_HOST".to_string(), "127.0.0.1:0".to_string()),
            ("S3_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
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
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind address"),
                secret_key: Some(iroh::SecretKey::from_bytes(&[9u8; 32])),
                realm_id: config.realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let stop = tokio_util::sync::CancellationToken::new();
        stop.cancel();

        let result = prepare(&config, &driver_ctx, &net, &stop).await;

        assert!(matches!(result, Ok(None)));
        net.shutdown().await;
    }
}
