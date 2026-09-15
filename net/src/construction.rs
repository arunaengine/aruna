//! Network construction in named, ordered stages: bind the endpoint and freeze
//! the configuration, load persisted peer and admission state, start services,
//! then the loops. The socket is reachable exactly at `NetworkEndpoint::bind`.

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::id::NodeId;
use aruna_core::metrics::NotificationWatchMetrics;
use aruna_core::structs::NetworkDiagnosticsState;
use aruna_core::structs::execution::notification_watch::WatchInterestTable;
use aruna_storage::StorageHandle;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::address_lookup::{DnsAddressLookup, PkarrPublisher};
use iroh::endpoint::{QuicTransportConfig, VarInt, presets};
use iroh::{Endpoint, RelayMap, RelayMode};
use parking_lot::RwLock;
use tokio::sync::{Mutex, Notify, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;
use ulid::Ulid;

use crate::config::{DiscoveryMethod, NetConfig, RelayMethod};
use crate::connection_pool::{ConnectionPool, ConnectionPoolOptions};
use crate::connectivity::{
    PeerEvent, PeerManagerState, run_connectivity_manager, send_connectivity_event,
};
use crate::dht::DhtHandle;
use crate::dht::handle::DhtSpawnResources;
use crate::discovery::{local_endpoint_addr, read_persisted_peers, spawn_signed_publisher};
use crate::document_sync::DocumentSyncService;
use crate::effect_handlers::{self, NetEffectContext};
use crate::error::{NetError, Result};
use crate::eviction::spawn_eviction_maintenance;
use crate::streams::{self, InboundAdmission, StreamsService};
use crate::tasks::{
    BackgroundTasks, MAX_STREAM_HANDLERS, spawn_accept_loop, spawn_dht_forwarder,
    spawn_effect_dispatch, spawn_stream_dispatch,
};
use crate::{
    EffectHandle, InboundEventHandler, NetHandle, NetInner, unique_endpoint_addrs,
    unique_peer_nodes,
};

impl NetHandle {
    pub async fn new(config: NetConfig, storage: StorageHandle) -> Result<Self> {
        // 1. Configuration and the bound endpoint.
        let runtime = NetworkEndpoint::bind(&config).await?;
        // 2. Persisted peer and admission state.
        let (mut peers, handle_state) =
            PeerAdmissionState::load(&runtime, &config, &storage).await?;
        // 3. Long-lived services: connection pool, DHT, document sync, streams.
        let (services, dht_resources) =
            NetworkServices::start(&config, &runtime, &peers, &storage).await?;
        // 4. Background loops; all fallible work is behind us by here.
        let background =
            BackgroundRuntime::start(&config, &runtime, &mut peers, &services, dht_resources);
        Ok(Self {
            inner: Arc::new(NetInner {
                effect_tx: background.effect_tx,
                storage,
                node_id: runtime.node_id,
                realm_id: config.realm_id,
                endpoint: runtime.endpoint,
                address_lookup: runtime.address_lookup,
                discovery_method: runtime.discovery_method,
                relay_method: runtime.relay_method,
                realm_peers: peers.realm_peers,
                inbound_admission: peers.inbound_admission,
                watch_interest: handle_state.watch_interest,
                notification_watch_metrics: NotificationWatchMetrics::default(),
                notification_wakes: handle_state.notification_wakes,
                dashboard_epoch: handle_state.dashboard_epoch,
                dashboard_changes: handle_state.dashboard_changes,
                signed_authorized_nodes: peers.signed_authorized_nodes,
                dht: services.dht,
                document_sync: services.document_sync,
                streams: services.streams,
                connection_pool: services.connection_pool,
                peer_connectivity: peers.peer_connectivity,
                network_diagnostics: peers.network_diagnostics,
                peer_connectivity_tx: peers.peer_connectivity_tx,
                inbound_handler: background.inbound_handler,
                inbound_handler_registered: background.inbound_handler_registered,
                inbound_tasks: background.inbound_tasks,
                loopback_streams: background.loopback_streams,
                effect_tasks: background.effect_tasks,
                eviction_shutdown: background.eviction_shutdown,
                accept_shutdown: background.accept_shutdown,
                shutdown: runtime.shutdown,
                teardown: tokio::sync::OnceCell::new(),
                tasks: Mutex::new(background.tasks),
            }),
            monitor: runtime.monitor,
        })
    }
}

/// Configuration-derived transport state bound before any service starts.
struct NetworkEndpoint {
    endpoint: Endpoint,
    address_lookup: MemoryLookup,
    monitor: crate::connection_pool::Monitor,
    discovery_method: DiscoveryMethod,
    relay_method: RelayMethod,
    configured_relay_urls: Vec<String>,
    node_id: NodeId,
    peer_hints: Vec<NodeId>,
    app_alpns: Vec<Vec<u8>>,
    shutdown: CancellationToken,
}

impl NetworkEndpoint {
    async fn bind(config: &NetConfig) -> Result<Self> {
        let secret_key = config
            .secret_key
            .clone()
            .unwrap_or_else(iroh::SecretKey::generate);

        let address_lookup = MemoryLookup::new();
        let discovery_method = config.discovery_method.clone();
        let relay_method = config.relay_method.clone();
        let configured_relay_urls = relay_method.relay_urls();

        let mut transport_config = QuicTransportConfig::builder();
        if let Some(max_uni) = config.max_uni_streams {
            transport_config =
                transport_config.max_concurrent_uni_streams(VarInt::from_u64(max_uni)?);
        }
        if let Some(max_bidi) = config.max_bidi_streams {
            transport_config =
                transport_config.max_concurrent_bidi_streams(VarInt::from_u64(max_bidi)?);
        }

        let monitor = crate::connection_pool::Monitor::new();

        let app_alpns = vec![
            Alpn::Dht.as_bytes().to_vec(),
            Alpn::Bao.as_bytes().to_vec(),
            Alpn::DocumentSync.as_bytes().to_vec(),
            Alpn::Metadata.as_bytes().to_vec(),
            Alpn::NativeReference.as_bytes().to_vec(),
            Alpn::Notification.as_bytes().to_vec(),
            Alpn::Shard.as_bytes().to_vec(),
            Alpn::JobControl.as_bytes().to_vec(),
        ];

        let mut endpoint_builder = Endpoint::builder(presets::Minimal)
            .hooks(monitor.clone())
            .transport_config(transport_config.build())
            .secret_key(secret_key)
            .address_lookup(address_lookup.clone())
            .alpns(app_alpns.clone());

        match &config.relay_method {
            RelayMethod::None => {
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Disabled);
            }
            RelayMethod::N0 => {
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Default);
            }
            RelayMethod::Custom(relays) => {
                let relays = RelayMap::try_from_iter(relays.iter().map(|s| s.as_ref()))
                    .map_err(|e| NetError::Bootstrap(format!("Invalid relay URL: {}", e)))?;
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Custom(relays));
            }
            RelayMethod::N0WithCustom(relays) => {
                let relay_map = iroh::defaults::prod::default_relay_map();
                let custom = RelayMap::try_from_iter(relays.iter().map(|s| s.as_ref()))
                    .map_err(|e| NetError::Bootstrap(format!("Invalid relay URL: {}", e)))?;
                relay_map.extend(&custom);
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Custom(relay_map));
            }
        }

        for method in config.discovery_method.leaf_methods() {
            match method {
                DiscoveryMethod::None | DiscoveryMethod::DhtSigned { .. } => {
                    // No endpoint builder setup needed for these methods.
                }
                DiscoveryMethod::N0Dns => {
                    endpoint_builder = endpoint_builder.address_lookup(PkarrPublisher::n0_dns());
                    endpoint_builder = endpoint_builder.address_lookup(DnsAddressLookup::n0_dns());
                }
                DiscoveryMethod::CustomDns(servers) => {
                    for server in servers {
                        endpoint_builder = endpoint_builder
                            .address_lookup(DnsAddressLookup::builder(server.clone()));
                    }
                }
                DiscoveryMethod::Ordered(_) => {}
            }
        }

        let endpoint_builder = endpoint_builder
            .bind_addr(config.bind_addr)
            .map_err(|e| NetError::Bootstrap(e.to_string()))?;

        let endpoint = endpoint_builder
            .bind()
            .await
            .map_err(|e| NetError::Bootstrap(e.to_string()))?;
        address_lookup.set_endpoint_info(local_endpoint_addr(&endpoint, &configured_relay_urls));

        let node_id = endpoint.id();
        let peer_endpoints = unique_endpoint_addrs(config.peer_endpoints.clone(), node_id);
        for endpoint_addr in &peer_endpoints {
            address_lookup.set_endpoint_info(endpoint_addr.clone());
        }
        let mut peer_hints = config.peer_nodes.clone();
        peer_hints.extend(peer_endpoints.iter().map(|endpoint| endpoint.id));
        let peer_hints = unique_peer_nodes(peer_hints, node_id);

        Ok(Self {
            endpoint,
            address_lookup,
            monitor,
            discovery_method,
            relay_method,
            configured_relay_urls,
            node_id,
            peer_hints,
            app_alpns,
            shutdown: CancellationToken::new(),
        })
    }
}

/// Persisted realm peers plus the admission and connectivity state derived from
/// them. The peer set is read once here; later refreshes go through the handle.
struct PeerAdmissionState {
    realm_peers: Arc<RwLock<Vec<NodeId>>>,
    inbound_admission: InboundAdmission,
    signed_authorized_nodes: Arc<RwLock<Vec<NodeId>>>,
    peer_connectivity: Arc<Mutex<PeerManagerState>>,
    network_diagnostics: Arc<Mutex<NetworkDiagnosticsState>>,
    peer_connectivity_tx: mpsc::Sender<PeerEvent>,
    peer_connectivity_rx: Option<mpsc::Receiver<PeerEvent>>,
}

impl PeerAdmissionState {
    async fn load(
        runtime: &NetworkEndpoint,
        config: &NetConfig,
        storage: &StorageHandle,
    ) -> Result<(Self, HandleState)> {
        let persisted_realm_peers =
            read_persisted_peers(storage, config.realm_id, runtime.node_id).await?;
        let realm_peer_nodes = persisted_realm_peers.clone().unwrap_or_default();
        let realm_peers = Arc::new(RwLock::new(realm_peer_nodes.clone()));
        let inbound_admission = streams::InboundAdmission::new(
            realm_peers.clone(),
            runtime
                .peer_hints
                .iter()
                .chain(realm_peer_nodes.iter())
                .copied(),
        );
        if persisted_realm_peers.is_some() {
            inbound_admission.mark_materialized();
        }
        let signed_authorized_nodes = Arc::new(RwLock::new(realm_peer_nodes.clone()));
        let peer_connectivity = Arc::new(Mutex::new(PeerManagerState::new(
            &realm_peer_nodes,
            "realm_config",
        )));
        let network_diagnostics = Arc::new(Mutex::new(NetworkDiagnosticsState::default()));
        let (peer_connectivity_tx, peer_connectivity_rx) = mpsc::channel(256);
        for node_id in &runtime.peer_hints {
            send_connectivity_event(
                &peer_connectivity_tx,
                PeerEvent::ManagePeer {
                    node_id: *node_id,
                    source: "configured_peer".to_string(),
                    immediate: true,
                },
            );
        }

        Ok((
            Self {
                realm_peers,
                inbound_admission,
                signed_authorized_nodes,
                peer_connectivity,
                network_diagnostics,
                peer_connectivity_tx,
                peer_connectivity_rx: Some(peer_connectivity_rx),
            },
            HandleState::new(),
        ))
    }
}

/// Handle-owned caches and buses with no peer state and no background loop.
struct HandleState {
    watch_interest: Arc<RwLock<WatchInterestTable>>,
    notification_wakes: broadcast::Sender<UserId>,
    dashboard_epoch: Ulid,
    dashboard_changes: watch::Sender<u64>,
}

impl HandleState {
    fn new() -> Self {
        let (notification_wakes, _) = broadcast::channel(crate::NOTIFICATION_WAKE_CAPACITY);
        let (dashboard_changes, _) = watch::channel(0);
        Self {
            watch_interest: Arc::new(RwLock::new(WatchInterestTable::default())),
            notification_wakes,
            dashboard_epoch: Ulid::generate(),
            dashboard_changes,
        }
    }
}

/// Long-lived services started before any background loop. Starting the DHT
/// driver is fallible; a later document-sync failure cancels that driver and
/// joins it before returning, so no task outlives a failed construction.
struct NetworkServices {
    connection_pool: ConnectionPool,
    dht: Arc<DhtHandle>,
    document_sync: Arc<DocumentSyncService>,
    streams: Arc<StreamsService>,
}

impl NetworkServices {
    async fn start(
        config: &NetConfig,
        runtime: &NetworkEndpoint,
        peers: &PeerAdmissionState,
        storage: &StorageHandle,
    ) -> Result<(Self, DhtSpawnResources)> {
        let connection_pool =
            ConnectionPool::new(runtime.endpoint.clone(), ConnectionPoolOptions::default());

        let (dht_handle, dht_resources) = DhtHandle::spawn(
            runtime.endpoint.clone(),
            storage.clone(),
            connection_pool.clone(),
            peers.inbound_admission.peer_kinds(),
            runtime.shutdown.child_token(),
        )?;
        let dht = Arc::new(dht_handle);
        let realm_peer_nodes = peers.realm_peers.read().clone();
        for node_id in runtime.peer_hints.iter().chain(realm_peer_nodes.iter()) {
            if let Err(err) = dht.add_peer(*node_id) {
                warn!(
                    node_id = %node_id,
                    error = %err,
                    "Failed to add configured peer to DHT routing queue"
                );
            }
        }

        let document_sync_path = config
            .sync_storage_path
            .clone()
            .unwrap_or_else(|| {
                std::env::temp_dir().join(format!("aruna-document-sync-{}", ulid::Ulid::generate()))
            });
        // Configured bootstrap peers join the persisted realm peers so a fresh
        // joiner admits its seed's pushes before the first realm config applies.
        let mut document_sync_peers = realm_peer_nodes;
        document_sync_peers.extend(runtime.peer_hints.iter().copied());
        let mut document_sync = match DocumentSyncService::open_with_policy(
            runtime.endpoint.clone(),
            storage.clone(),
            document_sync_path,
            &document_sync_peers,
            runtime.app_alpns.clone(),
            config.document_sync_runtime.unwrap_or_default(),
            config.fjall_persist_policy,
            config.realm_id,
        ) {
            Ok(document_sync) => document_sync,
            Err(error) => {
                runtime.shutdown.cancel();
                let mut tasks = dht_resources.tasks;
                while let Some(task) = tasks.pop() {
                    let _ = task.await;
                }
                runtime.endpoint.close().await;
                return Err(error);
            }
        };
        // Dial-side half of the ALPN x kind matrix: sync never targets a device.
        document_sync.set_peer_kinds(peers.inbound_admission.peer_kinds());
        let document_sync = Arc::new(document_sync);

        let streams = Arc::new(StreamsService::new(connection_pool.clone()));

        Ok((
            Self {
                connection_pool,
                dht,
                document_sync,
                streams,
            },
            dht_resources,
        ))
    }
}

/// Background loops started last, owned together for shutdown.
struct BackgroundRuntime {
    effect_tx: mpsc::Sender<EffectHandle>,
    inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>>,
    inbound_handler_registered: Arc<Notify>,
    inbound_tasks: TaskTracker,
    // Released with admission, or the inbound stream task never sees the channel close.
    loopback_streams: parking_lot::Mutex<Option<mpsc::Sender<(Alpn, streams::BiStream, NodeId)>>>,
    /// Accepted effect futures, tracked so shutdown waits for them too.
    effect_tasks: TaskTracker,
    eviction_shutdown: CancellationToken,
    accept_shutdown: CancellationToken,
    tasks: BackgroundTasks,
}

impl BackgroundRuntime {
    fn start(
        config: &NetConfig,
        runtime: &NetworkEndpoint,
        peers: &mut PeerAdmissionState,
        services: &NetworkServices,
        dht_resources: DhtSpawnResources,
    ) -> Self {
        let inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>> =
            Arc::new(RwLock::new(None));
        let inbound_handler_registered = Arc::new(Notify::new());

        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(256);
        // Accepted effect futures run outside the dispatcher task itself, so
        // their completion is tracked here and awaited by shutdown.
        let effect_tasks = TaskTracker::new();

        // Inbound handlers and presence refreshes write to storage, so shutdown
        // joins them instead of leaving them detached behind the final sync.
        let inbound_tasks = TaskTracker::new();
        let effect_context = Arc::new(NetEffectContext {
            dht: services.dht.clone(),
            document_sync: services.document_sync.clone(),
            presence: effect_handlers::RealmPresenceCache::default(),
            tasks: inbound_tasks.clone(),
            shutdown: runtime.shutdown.clone(),
            #[cfg(test)]
            refresh_probe: None,
        });
        let mut tasks = BackgroundTasks::new();
        tasks.push(spawn_effect_dispatch(
            effect_rx,
            effect_context,
            effect_tasks.clone(),
            runtime.shutdown.clone(),
        ));

        let (dht_tx, dht_rx) = mpsc::channel(64);
        let (stream_tx, stream_rx) = mpsc::channel(64);
        let loopback_streams = parking_lot::Mutex::new(Some(stream_tx.clone()));
        let dht_inbound_tx = dht_resources.inbound_stream_tx.clone();
        tasks.push(spawn_dht_forwarder(dht_rx, dht_inbound_tx));

        let inbound_stream_handlers =
            Arc::new(tokio::sync::Semaphore::new(MAX_STREAM_HANDLERS));
        tasks.push(spawn_stream_dispatch(
            stream_rx,
            services.dht.clone(),
            inbound_handler.clone(),
            peers.inbound_admission.clone(),
            inbound_tasks.clone(),
            inbound_stream_handlers,
        ));

        // Admission has its own token so shutdown can stop accepting new peers
        // while the handlers already running still have a working network.
        let accept_shutdown = CancellationToken::new();
        tasks.push(spawn_accept_loop(
            runtime.endpoint.clone(),
            dht_tx,
            stream_tx,
            services.document_sync.clone(),
            peers.inbound_admission.clone(),
            accept_shutdown.clone(),
        ));

        let eviction_shutdown = CancellationToken::new();
        tasks.push(spawn_eviction_maintenance(
            services.document_sync.clone(),
            inbound_handler.clone(),
            inbound_handler_registered.clone(),
            eviction_shutdown.clone(),
        ));

        tasks.push(tokio::spawn(run_connectivity_manager(
            services.dht.clone(),
            runtime.address_lookup.clone(),
            services.connection_pool.clone(),
            runtime.discovery_method.clone(),
            config.realm_id,
            peers.signed_authorized_nodes.clone(),
            peers.peer_connectivity.clone(),
            peers.network_diagnostics.clone(),
            peers
                .peer_connectivity_rx
                .take()
                .expect("connectivity receiver is taken exactly once"),
            runtime.shutdown.child_token(),
        )));

        if let Some((ttl, refresh_after)) = runtime.discovery_method.dht_signed_config() {
            tasks.push(spawn_signed_publisher(
                services.dht.clone(),
                runtime.endpoint.clone(),
                config.realm_id,
                runtime.configured_relay_urls.clone(),
                ttl,
                refresh_after,
                peers.network_diagnostics.clone(),
                runtime.shutdown.child_token(),
            ));
        }

        // The DHT driver task joins through the same owner as every other loop.
        for task in dht_resources.tasks {
            tasks.push(task);
        }

        Self {
            effect_tx,
            inbound_handler,
            inbound_handler_registered,
            inbound_tasks,
            loopback_streams,
            effect_tasks,
            eviction_shutdown,
            accept_shutdown,
            tasks,
        }
    }
}
