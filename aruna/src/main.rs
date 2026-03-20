#![allow(clippy::result_large_err)]

use aruna::config::{StartupMode, load, mark_node_state_complete, mark_onboarding_phase};
use aruna_api::s3::s3_server::S3Server;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::BlobHandler;
use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, GossipEffect, NetEffect, StorageEffect};
use aruna_core::errors::GossipError;
use aruna_core::events::{Event, GossipEvent, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{OnboardingPhase, OnboardingSyncTicket};
use aruna_core::TopicId;
use aruna_core::structs::Actor;
use aruna_core::structs::Backend::FileSystem;
use aruna_core::structs::BackendConfig;
use aruna_core::structs::NodeCapabilities;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::automerge_announce::AnnounceAutomergeDocumentOperation;
use aruna_operations::automerge::AutomergeHandle;
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::ensure_realm_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::outgoing_automerge::OutgoingAutomergeOperation;
use aruna_operations::startup::RestoreAutomergeSubscriptionsOperation;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

#[tokio::main]
async fn main() {
    init_tracing();

    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;
    let (config, storage_handle) = load().await?;
    let net_handle = NetHandle::new(
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
    let automerge_handle = AutomergeHandle::new(Some(net_handle.clone()));
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: FileSystem,
            root: config.blob_root.clone(),
            service_config: HashMap::new(),
            bucket_prefix: config.blob_bucket_prefix.clone(),
            max_bucket_size: config.blob_max_bucket_size,
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
    .await?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle.clone()),
        blob_handle: Some(blob_handle),
        automerge_handle: Some(automerge_handle),
        task_handle: Some(task_handle.clone()),
    });

    initialize_net_incoming(driver_ctx.clone());
    initialize_task_incoming(driver_ctx.clone(), task_handle).await;

    for endpoint in &config.bootstrap_endpoints {
        net_handle.add_peer_addr(endpoint.clone()).await;
    }

    match &config.startup_mode {
        StartupMode::InitializeRealm { realm_description } => {
            if realm_bootstrap_exists(driver_ctx.as_ref(), &config.realm_id).await? {
                announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id).await?;
            } else {
                drive(
                    CreateRealmOperation::new(CreateRealmConfig {
                        actor: Actor {
                            node_id: config.node_id,
                            user_id: Ulid::from_bytes([0u8; 16]),
                            realm_id: config.realm_id.clone(),
                        },
                        realm_description: realm_description.clone(),
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }

            mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
        }
        StartupMode::JoinRealm { phase } => {
            if matches!(phase, OnboardingPhase::Bootstrapped) {
                fetch_core_onboarding_documents(
                    driver_ctx.as_ref(),
                    &config.node_state,
                    &config.realm_id,
                    config.bootstrap_endpoints.first().map(|endpoint| endpoint.id),
                )
                .await?;
                mark_onboarding_phase(
                    &driver_ctx.storage_handle,
                    &config.node_state,
                    OnboardingPhase::CoreDocumentsFetched,
                )
                .await?;
            }
            announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id).await?;
            mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
        }
        StartupMode::Provisioned => {
            drive(
                RestoreAutomergeSubscriptionsOperation::new(config.node_id),
                driver_ctx.as_ref(),
            )
            .await?;

            if matches!(&config.node_capabilities, NodeCapabilities::Management { .. }) {
                drive(
                    EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
                        actor: Actor {
                            node_id: config.node_id,
                            user_id: Ulid::from_bytes([0u8; 16]),
                            realm_id: config.realm_id.clone(),
                        },
                        bootstrap_peers: config.bootstrap_nodes.clone(),
                        default_metadata_replication_factor: config.default_metadata_replication_factor,
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }
        }
    }

    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: config.realm_id.clone(),
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
            config.realm_id.clone(),
            config.node_id,
            config.node_capabilities,
            is_initial_node,
            None,
        )
        .await,
    );

    let server_config = ServerConfig {
        http_addr: config.http_socket_addr,
    };
    let server = Server::new(state, server_config);

    // S3 Server
    let s3_address = format!("{}:{}", config.s3_address, config.s3_port);
    let s3_host = format!("{}:{}", config.s3_host, config.s3_port);
    let s3_server = S3Server::new(
        &s3_address,
        &s3_host,
        driver_ctx,
        config.realm_id.clone(),
        config.node_id,
    )
    .await
    .unwrap();

    let server_handle = s3_server.run().await.unwrap();

    tokio::select! {
        res = server_handle => {
            match res {
                Ok(_) => info!("S3 Server stopped normally"),
                Err(e) => error!("S3 Server panicked: {:?}", e),
            }
        }
        res = server.run() => {
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

    Ok(())
}

async fn realm_bootstrap_exists(
    driver_ctx: &DriverContext,
    realm_id: &aruna_core::structs::RealmId,
) -> Result<bool, Box<dyn std::error::Error>> {
    let key = ByteView::from(*realm_id.as_bytes());

    for key_space in [AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE] {
        match driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.clone(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {}
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(false),
            Event::Storage(StorageEvent::Error { error }) => return Err(Box::new(error)),
            other => return Err(format!("unexpected storage event: {other:?}").into()),
        }
    }

    Ok(true)
}

async fn announce_core_documents(
    driver_ctx: &DriverContext,
    node_id: aruna_core::NodeId,
    realm_id: &aruna_core::structs::RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    for document in [
        AutomergeDocumentVariant::RealmAuthorization {
            realm_id: realm_id.clone(),
        },
        AutomergeDocumentVariant::RealmConfig {
            realm_id: realm_id.clone(),
        },
    ] {
        drive(
            AnnounceAutomergeDocumentOperation::new(document, node_id),
            driver_ctx,
        )
        .await?;
    }

    Ok(())
}

async fn fetch_core_onboarding_documents(
    driver_ctx: &DriverContext,
    node_state: &aruna::config::PersistedNodeState,
    realm_id: &aruna_core::structs::RealmId,
    bootstrap_peer: Option<aruna_core::NodeId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(net_handle) = driver_ctx.net_handle.as_ref() else {
        return Err("net handle unavailable".into());
    };
    let bootstrap_peer = bootstrap_peer.ok_or("missing bootstrap peer")?;
    let onboarding_sync_ticket = node_state
        .onboarding_sync_ticket
        .as_deref()
        .ok_or("missing onboarding sync ticket")?;
    let onboarding_sync_ticket = OnboardingSyncTicket::decode(onboarding_sync_ticket)?;

    for topic in [
        TopicId::automerge_document(aruna_core::AutomergeTopicId::RealmAuthorization {
            realm_id: realm_id.clone(),
        }),
        TopicId::automerge_document(aruna_core::AutomergeTopicId::RealmConfig {
            realm_id: realm_id.clone(),
        }),
    ] {
        match net_handle
            .send_effect(Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
                topic: topic.clone(),
            })))
            .await
        {
            Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. })) => {}
            Event::Net(NetEvent::Gossip(GossipEvent::Error {
                error: GossipError::AlreadySubscribed,
            })) => {}
            Event::Net(NetEvent::Gossip(GossipEvent::Error { error })) => {
                return Err(error.to_string().into());
            }
            Event::Net(NetEvent::Error(error)) => return Err(format!("{error:?}").into()),
            other => return Err(format!("unexpected gossip subscribe result: {other:?}").into()),
        }
    }

    for document in [
        AutomergeDocumentVariant::RealmAuthorization {
            realm_id: realm_id.clone(),
        },
        AutomergeDocumentVariant::RealmConfig {
            realm_id: realm_id.clone(),
        },
    ] {
        drive(
            OutgoingAutomergeOperation::new_with_auth(
                bootstrap_peer,
                document,
                Some(onboarding_sync_ticket.clone().into_auth_proof()),
            ),
            driver_ctx,
        )
        .await?;
    }

    Ok(())
}
