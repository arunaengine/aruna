//! Realm preparation during startup: metadata replay, realm bootstrap or
//! joining, placement, and node-info seeding.

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::{Actor, NodeCapabilities, NodeUrls, RealmNodeKind};
use aruna_net::NetHandle;
use aruna_operations::device::realm_documents::fetch_realm_documents;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::projector::replay_event_log;
use aruna_operations::node::startup::prepare_shard_policy;
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::realm::ensure_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use tracing::{info, warn};

use crate::bootstrap::{
    ensure_onboarding_secret, fetch_core_documents, prepare_core_documents, realm_bootstrap_exists,
    wait_for_placement,
};
use crate::config::{Config, StartupMode, mark_onboarding_phase, mark_state_complete};

pub(crate) struct CoreAnnouncement {
    pub(crate) documents: Vec<DocumentSyncTarget>,
    pub(crate) allow_genesis: bool,
}

/// How long a device waits for the realm documents before it serves anyway.
const STARTUP_DOCUMENT_FETCH: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) async fn prepare(
    config: &Config,
    driver_ctx: &Arc<DriverContext>,
    net_handle: &NetHandle,
) -> Result<CoreAnnouncement, Box<dyn std::error::Error>> {
    let replayed_metadata_events = replay_event_log(driver_ctx.as_ref()).await?;
    if replayed_metadata_events > 0 {
        info!(
            replayed_metadata_events,
            "Replayed metadata event log during startup"
        );
    }

    let announcement = prepare_mode(config, driver_ctx, net_handle).await?;

    // Prepare local topics before binding; remote convergence stays behind the gate.
    prepare_shard_policy(driver_ctx, config.node_id, config.realm_id).await;
    // Devices fetch governing realm documents before serving because they run no sync.
    // A short attempt lets stored copies serve while heartbeat retries an unreachable realm.
    if matches!(config.node_capabilities, NodeCapabilities::User { .. })
        && !fetch_realm_documents(driver_ctx, STARTUP_DOCUMENT_FETCH).await
    {
        warn!("Serving this device from its stored realm documents for now");
    }
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

    mark_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
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
        fetch_core_documents(
            driver_ctx,
            &config.node_state,
            &config.realm_id,
            bootstrap_peer,
            config.onboarding_sync_timeout(),
        )
        .await?;
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
    mark_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
    Ok(CoreAnnouncement {
        documents,
        allow_genesis: false,
    })
}

/// A device reads the realm's documents over metadata and publishes none of
/// its own over sync, so it never joins or announces a sync topic.
fn is_device(config: &Config) -> bool {
    matches!(config.node_capabilities, NodeCapabilities::User { .. })
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
    Ok(CoreAnnouncement {
        documents,
        allow_genesis,
    })
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
            aruna_core::structs::ManagedCopyQuarantine::Rejoin,
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
