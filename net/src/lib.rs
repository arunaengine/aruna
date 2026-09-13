#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unsafe_code)]
#![recursion_limit = "256"]

//! The node networking layer: peer state, DHT, document sync, and the
//! network handle the rest of the node drives through effects.
//!
//! The module map is short on purpose. `config` owns operator settings;
//! `construction` binds the endpoint and starts services and background loops
//! in order; `connectivity` maintains peer refresh; `discovery` resolves
//! DHT-signed endpoints and persisted peers; `eviction` maintains the
//! document-sync eviction journal; `tasks` owns every spawned loop; and the
//! handle's public operations live in this file.

mod config;
mod connection_pool;
mod connectivity;
mod construction;
pub mod device_limits;
pub mod dht;
mod discovery;
pub mod document_sync;
mod effect_handlers;
pub mod error;
mod eviction;
pub mod streams;
mod tasks;
mod telemetry;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::document::{
    DocumentSyncEvictedDocument, DocumentSyncReconcileResult, DocumentSyncTarget,
};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetError as CoreNetError, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_core::metrics::NotificationWatchMetrics;
use aruna_core::structs::{
    NetState, NetworkDiagnosticsState, PlacementRef, RealmConfigDocument, RealmId,
    WatchInterestEntry, WatchInterestTable,
};
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use parking_lot::RwLock;
use tokio::sync::{Mutex, Notify, broadcast, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Span, debug, warn};
use ulid::Ulid;

pub use ::irokle::net::IrohRuntimeConfig;
pub use config::{
    DiscoveryMethod, NetConfig, RelayMethod, format_endpoint_config, parse_endpoint_config,
};
pub use connection_pool::{Monitor, PoolCounts};
pub use dht::DhtHandle;
pub use document_sync::{DocumentSyncService, PendingEviction, ShardGenesisProbe};
pub use error::{NetError, Result};
pub use streams::StreamsService;

use connection_pool::ConnectionPool;
use connectivity::{
    PeerConnectivityEvent, PeerConnectivityManagerState, net_warnings, peer_connection_states,
    peer_connectivity_status, send_connectivity_event,
};
use discovery::{
    authorize_signed_node, install_signed_endpoint, local_endpoint_addr, replace_authorized_nodes,
    resolve_signed_endpoint,
};
use tasks::BackgroundTasks;

/// Drain budget for inbound stream handlers when `shutdown` is called without one.
const DEFAULT_INBOUND_DRAIN: Duration = Duration::from_secs(5);
/// Grace for the same handlers once the endpoint is closed under them.
pub const FORCED_INBOUND_DRAIN: Duration = Duration::from_secs(5);
const NOTIFICATION_WAKE_CAPACITY: usize = 256;
// Overall open_stream budget: first open attempt, DHT-signed endpoint
// re-resolution, and the retry combined.
const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) type EffectHandle = (NetEffect, oneshot::Sender<NetEvent>, Span);

#[async_trait]
pub trait InboundEventHandler: Send + Sync {
    async fn handle_incoming_stream(&self, alpn: Alpn, stream: streams::BiStream, node_id: NodeId);

    /// Re-emits documents recovered from a genesis tie-break eviction: the loser's
    /// payloads replayed onto the winning genesis via durable outbox records.
    /// Returns whether every replacement committed; otherwise the journal stays.
    async fn handle_evicted_documents(&self, _documents: Vec<DocumentSyncEvictedDocument>) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct NetHandle {
    inner: Arc<NetInner>,
    monitor: Monitor,
}

struct NetInner {
    effect_tx: mpsc::Sender<EffectHandle>,
    storage: StorageHandle,
    node_id: NodeId,
    realm_id: RealmId,
    endpoint: Endpoint,
    address_lookup: MemoryLookup,
    discovery_method: DiscoveryMethod,
    relay_method: RelayMethod,
    realm_peers: Arc<RwLock<Vec<NodeId>>>,
    inbound_admission: streams::InboundAdmission,
    watch_interest: Arc<RwLock<WatchInterestTable>>,
    notification_watch_metrics: NotificationWatchMetrics,
    notification_wakes: broadcast::Sender<UserId>,
    dashboard_epoch: Ulid,
    dashboard_changes: watch::Sender<u64>,
    dht_signed_authorized_nodes: Arc<RwLock<Vec<NodeId>>>,
    dht: Arc<DhtHandle>,
    document_sync: Arc<DocumentSyncService>,
    streams: Arc<StreamsService>,
    connection_pool: ConnectionPool,
    peer_connectivity: Arc<Mutex<PeerConnectivityManagerState>>,
    network_diagnostics: Arc<Mutex<NetworkDiagnosticsState>>,
    peer_connectivity_tx: mpsc::Sender<PeerConnectivityEvent>,
    inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>>,
    inbound_handler_registered: Arc<Notify>,
    inbound_tasks: TaskTracker,
    eviction_shutdown: CancellationToken,
    accept_shutdown: CancellationToken,
    shutdown: CancellationToken,
    tasks: Mutex<BackgroundTasks>,
}

impl std::fmt::Debug for NetHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetHandle")
            .field("node_id", &self.inner.node_id)
            .finish()
    }
}

impl NetHandle {
    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub fn sign(&self, message: &[u8]) -> iroh::Signature {
        self.inner.endpoint.secret_key().sign(message)
    }

    /// Issuer-local key that encrypts S3 credential secrets at rest. Derived
    /// from this node's secret, so only this node can decrypt what it wrote.
    pub fn credential_encryption_key(
        &self,
    ) -> aruna_core::credential_encryption::CredentialEncryptionKey {
        aruna_core::credential_encryption::CredentialEncryptionKey::derive(
            &self.inner.endpoint.secret_key().to_bytes(),
        )
    }

    pub fn realm_id(&self) -> &RealmId {
        &self.inner.realm_id
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        local_endpoint_addr(&self.inner.endpoint, &self.inner.relay_method.relay_urls())
    }

    pub fn document_sync_node(&self) -> ::irokle::Irokle<::irokle::FjallStorage> {
        self.inner.document_sync.node()
    }

    /// Whether an unreleased eviction journal entry may still enqueue outbox
    /// rows for `placement`. A placement drain must not report the bucket clear
    /// while one is outstanding.
    pub fn eviction_pending(&self, placement: &PlacementRef) -> bool {
        self.inner.document_sync.eviction_pending(placement)
    }

    /// Closes a shard topic before a departing holder checks its eviction
    /// journal and outbox, ordering those checks after any reset transaction.
    pub fn close_sync_topic(&self, topic_id: ::irokle::TopicId) -> Result<bool> {
        self.inner.document_sync.close_topic(topic_id)
    }

    pub fn reopen_sync_topic(&self, topic_id: ::irokle::TopicId) -> Result<()> {
        self.inner.document_sync.reopen_topic(topic_id)
    }

    /// Takes one genesis tie-break eviction into the re-emission hand-off. The
    /// journal entry stays registered against the buckets it targets until the
    /// replacement outbox rows commit.
    pub async fn consume_eviction(
        &self,
        eviction: ::irokle::TopicEviction,
    ) -> Option<PendingEviction> {
        self.inner.document_sync.consume_eviction(eviction).await
    }

    pub fn document_sync_database(&self) -> fjall::OptimisticTxDatabase {
        self.inner.document_sync.database()
    }

    pub async fn sync_topic_peers(
        &self,
        topic_id: ::irokle::TopicId,
        peers: Vec<NodeId>,
    ) -> Result<()> {
        self.inner
            .document_sync
            .sync_with_peers(topic_id, peers)
            .await
    }

    pub fn allow_topic_peers(
        &self,
        topics: &[::irokle::TopicId],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        self.inner.document_sync.allow_topic_peers(topics, peers)
    }

    /// Reconciles shard-only topics to their exact sync membership (delivery)
    /// and accepted publisher set (authority). Shared topic membership and
    /// default peers are unchanged.
    pub async fn reconcile_shard_membership(
        &self,
        topics: &[::irokle::TopicId],
        members: Vec<NodeId>,
        publishers: Vec<NodeId>,
        retained: &std::collections::BTreeSet<NodeId>,
        verified_topics: &std::collections::BTreeSet<::irokle::TopicId>,
    ) -> Result<()> {
        self.inner
            .document_sync
            .reconcile_shard_membership(topics, members, publishers, retained, verified_topics)
            .await
    }

    pub fn ensure_sync_topics(
        &self,
        topics: &[::irokle::TopicId],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        self.inner.document_sync.ensure_sync_topics(topics, peers)
    }

    /// Ensures topics this node is the only holder of. No peer is added to
    /// their membership, so a device's own topics stay entirely local.
    pub fn ensure_local_topics(&self, topics: &[::irokle::TopicId]) -> Result<()> {
        self.inner.document_sync.ensure_local_topics(topics)
    }

    /// Whether a document sync topic's genesis is known locally.
    pub fn sync_topic_exists(&self, topic: ::irokle::TopicId) -> Result<bool> {
        self.inner.document_sync.topic_exists(topic)
    }

    /// Probes a shard's co-holders for an existing genesis of `topics` (see
    /// [`ShardGenesisProbe`]). A rank-0 holder uses the result to create a fresh
    /// genesis only when every co-holder was reached and none had the topic.
    pub async fn probe_shard_geneses(
        &self,
        topics: Vec<::irokle::TopicId>,
        co_holders: Vec<NodeId>,
    ) -> ShardGenesisProbe {
        self.inner
            .document_sync
            .probe_shard_geneses(topics, co_holders)
            .await
    }

    /// Shard-topic anti-entropy for the startup restore and placement
    /// reconciler: ensures the topics locally, syncs them with `peers`, and
    /// reconciles the applied events.
    pub async fn sync_document_topics(
        &self,
        topics: Vec<::irokle::TopicId>,
        peers: Vec<NodeId>,
    ) -> aruna_core::document::DocumentSyncNetEvent {
        self.inner
            .document_sync
            .sync_documents_event(topics, peers)
            .await
    }

    pub async fn handle_sync_stream(
        &self,
        stream: streams::BiStream,
        peer: NodeId,
    ) -> Result<Vec<::irokle::TopicId>> {
        self.inner
            .document_sync
            .handle_inbound_stream(stream, peer)
            .await
    }

    pub async fn reconcile_sync_topics(
        &self,
        topic_ids: Vec<::irokle::TopicId>,
    ) -> Result<DocumentSyncReconcileResult> {
        let applied = self
            .inner
            .document_sync
            .reconcile_sync_topics(topic_ids)
            .await?;
        if applied
            .targets
            .iter()
            .any(|target| matches!(target, DocumentSyncTarget::RealmConfig { .. }))
        {
            self.reload_realm_peers().await?;
        }
        Ok(applied)
    }

    /// Connection-attempt counters behind the structured connection
    /// diagnostics; fixed outcome classes with no peer dimension.
    pub fn pool_counts(&self) -> PoolCounts {
        self.inner.connection_pool.counts()
    }

    #[doc(hidden)]
    pub fn pool_counts_for(&self, node_id: NodeId, alpn: Alpn) -> PoolCounts {
        self.inner.connection_pool.counts_for(node_id, alpn)
    }

    pub async fn add_peer_addr(&self, endpoint_addr: EndpointAddr) {
        if endpoint_addr.id == self.inner.node_id {
            return;
        }

        self.inner.inbound_admission.add_bootstrap(endpoint_addr.id);
        self.inner
            .address_lookup
            .set_endpoint_info(endpoint_addr.clone());
        // A newly installed address invalidates the failures recorded against
        // the previous one.
        if let Err(error) = self
            .inner
            .connection_pool
            .clear_failures(endpoint_addr.id)
            .await
        {
            debug!(node_id = %endpoint_addr.id, %error, "Connection pool stopped during address installation");
        }
        send_connectivity_event(
            &self.inner.peer_connectivity_tx,
            PeerConnectivityEvent::ManagePeer {
                node_id: endpoint_addr.id,
                source: "endpoint_addr".to_string(),
                immediate: true,
            },
        );
        if let Err(err) = self.inner.dht.add_peer(endpoint_addr.id) {
            warn!(
                node_id = %endpoint_addr.id,
                error = %err,
                "Failed to add endpoint address peer to DHT"
            );
        }
    }

    pub async fn refresh_document_peers(
        &self,
        document: &RealmConfigDocument,
    ) -> Result<Vec<NodeId>> {
        if document.realm_id != self.inner.realm_id {
            return Err(NetError::Bootstrap(format!(
                "realm config {} does not match net realm {}",
                document.realm_id, self.inner.realm_id
            )));
        }
        // Node kind decides which protocols each side may speak and is published
        // first: a peer admitted before its kind is known would pass as unconfigured.
        let mut peer_kinds = BTreeMap::new();
        let mut local_kind = None;
        for node in &document.nodes {
            let Ok(node_id) = NodeId::from_str(&node.node_id) else {
                continue;
            };
            if node_id == self.inner.node_id {
                local_kind = Some(node.kind.clone());
            } else {
                peer_kinds.insert(node_id, node.kind.clone());
            }
        }
        self.inner
            .inbound_admission
            .set_kinds(local_kind, peer_kinds);
        // Device limits are keyed by the kinds just published, so they follow the
        // same refresh rather than a second source of truth.
        self.inner
            .inbound_admission
            .set_device_limits(device_limits::DeviceLimits {
                requests_per_minute: document.quota.device_requests_per_minute,
                concurrent: document.quota.device_concurrent_pulls,
            });
        // Connection admission covers every registered realm node, User kind
        // included: user nodes forward metadata and job-control requests.
        let admitted = unique_peer_nodes(
            document
                .node_ids()
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            self.inner.node_id,
        );
        self.inner.inbound_admission.set_admitted(admitted);
        // Sync fan-out and DHT trust stay restricted to sync-eligible nodes.
        let peers = unique_peer_nodes(
            document
                .sync_eligible_nodes()
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            self.inner.node_id,
        );
        self.refresh_realm_peers(peers.clone()).await;
        Ok(peers)
    }

    pub async fn refresh_encoded_peers(&self, bytes: &[u8]) -> Result<Vec<NodeId>> {
        let document = RealmConfigDocument::from_bytes(bytes)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.refresh_document_peers(&document).await
    }

    pub async fn reload_realm_peers(&self) -> Result<Option<Vec<NodeId>>> {
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: self.inner.realm_id,
        };
        let Some(bytes) = self
            .read_storage(target.storage_keyspace().to_string(), target.storage_key())
            .await?
        else {
            return Ok(None);
        };
        self.refresh_encoded_peers(&bytes).await.map(Some)
    }

    pub async fn realm_peers(&self) -> Vec<NodeId> {
        self.inner.realm_peers.read().clone()
    }

    /// Cheap clone of the origin-side watch interest cache. Consumers match
    /// events against this table without any per-event storage read.
    pub fn watch_interest_snapshot(&self) -> WatchInterestTable {
        self.inner.watch_interest.read().clone()
    }

    pub fn notification_watch_metrics(&self) -> &NotificationWatchMetrics {
        &self.inner.notification_watch_metrics
    }

    /// Replaces the whole watch interest cache (startup and full rebuilds).
    pub fn replace_watch_interest(&self, table: WatchInterestTable) {
        *self.inner.watch_interest.write() = table;
    }

    /// Subscribes to the per-node notification wake bus. Each subscriber gets an
    /// independent receiver carrying the `UserId` whose inbox just changed; live
    /// streams filter by recipient and refetch the unread count on wake.
    pub fn subscribe_notification_wakes(&self) -> broadcast::Receiver<UserId> {
        self.inner.notification_wakes.subscribe()
    }

    /// Best-effort wake fired after a successful inbox mutation (new record or a
    /// mark-read that changed the unread count). A send with no subscribers is a
    /// no-op, never an error; nothing may fail through this path.
    pub fn notify_inbox_activity(&self, recipient: UserId) {
        let _ = self.inner.notification_wakes.send(recipient);
    }

    /// Subscribes to the retained dashboard revision for this node.
    pub fn subscribe_dashboard_changes(&self) -> watch::Receiver<u64> {
        self.inner.dashboard_changes.subscribe()
    }

    pub fn dashboard_epoch(&self) -> Ulid {
        self.inner.dashboard_epoch
    }

    /// Advances the retained dashboard revision and wakes every subscriber.
    pub fn notify_dashboard_change(&self) {
        self.inner
            .dashboard_changes
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    /// Replaces one realm's node map after a reconcile touched its digests,
    /// leaving every other realm's cached interest untouched.
    pub fn update_realm_interest(
        &self,
        realm_id: RealmId,
        nodes: HashMap<NodeId, Vec<WatchInterestEntry>>,
    ) {
        self.inner.watch_interest.write().set_realm(realm_id, nodes);
    }

    /// Records a locally-known holder for a freshly created watch so matching
    /// events route to it before its digest replicates back. Retracted by
    /// [`Self::retract_local_interest`].
    pub fn register_local_interest(
        &self,
        watch_id: Ulid,
        realm_id: RealmId,
        holder: NodeId,
        entry: WatchInterestEntry,
    ) {
        self.inner
            .watch_interest
            .write()
            .register_local(watch_id, realm_id, holder, entry);
    }

    /// Drops the local watch-interest registration for a deleted watch.
    pub fn retract_local_interest(&self, watch_id: Ulid) {
        self.inner.watch_interest.write().retract_local(watch_id);
    }

    async fn refresh_realm_peers(&self, peers: Vec<NodeId>) {
        *self.inner.realm_peers.write() = peers.clone();
        self.inner.inbound_admission.mark_materialized();
        replace_authorized_nodes(
            &self.inner.dht_signed_authorized_nodes,
            &peers,
            self.inner.node_id,
        );
        if let Err(err) = self
            .inner
            .document_sync
            .refresh_peer_candidates(peers.clone())
        {
            warn!(
                error = %err,
                "Failed to refresh document sync potential peers from realm config"
            );
        }
        for node_id in peers {
            self.register_realm_peer(node_id, true).await;
        }
    }

    async fn register_realm_peer(&self, node_id: NodeId, immediate: bool) {
        if node_id == self.inner.node_id {
            return;
        }

        authorize_signed_node(
            &self.inner.dht_signed_authorized_nodes,
            node_id,
            self.inner.node_id,
        );
        send_connectivity_event(
            &self.inner.peer_connectivity_tx,
            PeerConnectivityEvent::ManagePeer {
                node_id,
                source: "realm_config".to_string(),
                immediate,
            },
        );
        if let Err(err) = self.inner.dht.add_peer(node_id) {
            warn!(
                node_id = %node_id,
                error = %err,
                "Failed to add realm peer to DHT routing queue"
            );
        }
    }

    async fn read_storage(
        &self,
        key_space: String,
        key: aruna_core::types::Key,
    ) -> Result<Option<aruna_core::types::Value>> {
        match self
            .inner
            .storage
            .send_storage_effect(StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while reading realm config: {other:?}"
            ))),
        }
    }

    pub async fn open_stream(&self, node_id: NodeId, alpn: Alpn) -> Result<streams::BiStream> {
        if matches!(alpn, Alpn::Dht) {
            return Err(NetError::Stream(format!(
                "{alpn} is an internal network protocol"
            )));
        }
        // Outbound half of the accept matrix: a device must not even dial a
        // protocol its own node kind is refused on.
        if !self.inner.inbound_admission.local_dials(alpn) {
            return Err(NetError::Stream(format!(
                "{alpn} is not permitted for this node kind"
            )));
        }

        // An offline peer must fail within a bounded window; the inner path is
        // otherwise open (10s) + unbounded DHT resolve + open (10s).
        match tokio::time::timeout(OPEN_STREAM_TIMEOUT, self.open_stream_inner(node_id, alpn)).await
        {
            Ok(result) => result,
            Err(_) => {
                if node_id != self.inner.node_id {
                    send_connectivity_event(
                        &self.inner.peer_connectivity_tx,
                        PeerConnectivityEvent::ConnectionFailure {
                            node_id,
                            source: "stream_open".to_string(),
                            error: "stream open timed out".to_string(),
                        },
                    );
                }
                Err(NetError::Stream(format!(
                    "stream open to {node_id} timed out after {}s",
                    OPEN_STREAM_TIMEOUT.as_secs()
                )))
            }
        }
    }

    async fn open_stream_inner(&self, node_id: NodeId, alpn: Alpn) -> Result<streams::BiStream> {
        if node_id != self.inner.node_id {
            if let Err(err) = self.inner.dht.add_peer(node_id) {
                warn!(
                    node_id = %node_id,
                    error = %err,
                    "Failed to add stream target peer to DHT"
                );
            }
            send_connectivity_event(
                &self.inner.peer_connectivity_tx,
                PeerConnectivityEvent::ManagePeer {
                    node_id,
                    source: "stream_target".to_string(),
                    immediate: false,
                },
            );
        }

        match self.inner.streams.open(node_id, alpn).await {
            Ok(stream) => {
                if node_id != self.inner.node_id {
                    send_connectivity_event(
                        &self.inner.peer_connectivity_tx,
                        PeerConnectivityEvent::ConnectionSuccess {
                            node_id,
                            source: "stream_open".to_string(),
                        },
                    );
                }
                Ok(stream)
            }
            Err(mut err) => {
                if node_id != self.inner.node_id {
                    let authorized_nodes = self.inner.dht_signed_authorized_nodes.read().clone();
                    match resolve_signed_endpoint(
                        &self.inner.dht,
                        self.inner.realm_id,
                        &authorized_nodes,
                        node_id,
                        self.inner.discovery_method.dht_signed_config(),
                        &self.inner.network_diagnostics,
                    )
                    .await
                    {
                        Ok(Some(endpoint_addr)) => {
                            install_signed_endpoint(
                                &self.inner.address_lookup,
                                &self.inner.dht,
                                &self.inner.connection_pool,
                                endpoint_addr,
                            )
                            .await;
                            debug!(
                                node_id = %node_id,
                                "Retrying stream after DHT-signed endpoint resolution"
                            );
                            match self.inner.streams.open(node_id, alpn).await {
                                Ok(stream) => {
                                    send_connectivity_event(
                                        &self.inner.peer_connectivity_tx,
                                        PeerConnectivityEvent::ConnectionSuccess {
                                            node_id,
                                            source: "stream_open_dht_signed".to_string(),
                                        },
                                    );
                                    return Ok(stream);
                                }
                                Err(retry_err) => {
                                    err = retry_err;
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(resolve_err) => {
                            debug!(
                                node_id = %node_id,
                                error = %resolve_err,
                                "DHT-signed endpoint resolution failed"
                            );
                        }
                    }
                }

                if node_id != self.inner.node_id {
                    send_connectivity_event(
                        &self.inner.peer_connectivity_tx,
                        PeerConnectivityEvent::ConnectionFailure {
                            node_id,
                            source: "stream_open".to_string(),
                            error: err.to_string(),
                        },
                    );
                }
                Err(err)
            }
        }
    }

    pub fn set_inbound_handler(&self, handler: Arc<dyn InboundEventHandler>) {
        *self.inner.inbound_handler.write() = Some(handler);
        self.inner.inbound_handler_registered.notify_one();
    }

    pub fn clear_inbound_handler(&self) {
        self.inner.inbound_handler.write().take();
    }

    pub async fn shutdown(&self) {
        self.shutdown_with_drain(DEFAULT_INBOUND_DRAIN).await;
    }

    /// Stops accepting inbound streams and rejects further inbound handlers,
    /// without waiting for the ones in flight or tearing the endpoint down.
    pub fn close_admission(&self) {
        self.inner.accept_shutdown.cancel();
        self.inner.inbound_tasks.close();
    }

    /// Stops inbound admission, gives handlers that are already running up to
    /// `drain` to finish while the endpoint is still usable, then tears the
    /// network down and joins every child.
    pub async fn shutdown_with_drain(&self, drain: Duration) -> bool {
        if self.inner.shutdown.is_cancelled() {
            return false;
        }

        self.close_admission();
        if tokio::time::timeout(drain, self.inner.inbound_tasks.wait())
            .await
            .is_err()
        {
            warn!(
                pending = self.inner.inbound_tasks.len(),
                drain_ms = drain.as_millis(),
                "Inbound stream handlers outlived the drain deadline; closing the endpoint under them"
            );
        }

        self.inner.shutdown.cancel();
        if let Err(err) = self.inner.dht.shutdown().await {
            warn!(error = %err, "DHT shutdown returned error");
        }
        self.inner.document_sync.shutdown().await;
        self.inner.eviction_shutdown.cancel();
        if let Err(err) = self.inner.connection_pool.shutdown().await {
            warn!(error = %err, "Connection pool shutdown returned error");
        }
        self.inner.endpoint.close().await;

        let mut tasks = self.inner.tasks.lock().await;
        tasks.join_all().await;
        drop(tasks);

        // The endpoint is closed, so surviving handlers now fail their stream IO
        // instead of blocking; join them before the caller closes storage.
        let inbound_drained =
            tokio::time::timeout(FORCED_INBOUND_DRAIN, self.inner.inbound_tasks.wait())
                .await
                .is_ok();
        if !inbound_drained {
            warn!(
                pending = self.inner.inbound_tasks.len(),
                "Gave up joining inbound stream handlers during shutdown"
            );
        }
        inbound_drained
    }

    pub async fn get_status(&self) -> NetState {
        let peer_nodes = self.inner.realm_peers.read().clone();
        let configured_relay_urls = self.inner.relay_method.relay_urls();
        let monitor = self.monitor.get_status().await;
        let mut diagnostics = self.inner.network_diagnostics.lock().await.clone();
        if let Ok(size) = self.inner.dht.routing_table_size().await {
            diagnostics.routing_table_size = Some(size);
        }
        let peer_connectivity = peer_connectivity_status(&self.inner.peer_connectivity).await;
        let connections = peer_connection_states(
            &self.inner.endpoint,
            &monitor,
            &peer_connectivity,
            &peer_nodes,
            self.inner.node_id,
        )
        .await;
        let warnings = net_warnings(&peer_nodes, &connections, diagnostics.routing_table_size);

        NetState {
            endpoint_addr: local_endpoint_addr(&self.inner.endpoint, &configured_relay_urls),
            realm_id: *self.realm_id(),
            node_id: self.node_id(),
            discovery_methods: self.inner.discovery_method.enabled_methods(),
            relay_method: self.inner.relay_method.method_name().to_string(),
            relay_urls: configured_relay_urls,
            connections,
            requests: diagnostics.requests,
            routing_table_size: diagnostics.routing_table_size,
            warnings,
        }
    }
}

#[async_trait]
impl Handle for NetHandle {
    #[tracing::instrument(
        name = "net.handle.send_effect",
        level = "debug",
        skip(self, effect),
        fields(effect = effect_kind(&effect))
    )]
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Net(net_effect) => {
                let (tx, rx) = oneshot::channel();
                if self
                    .inner
                    .effect_tx
                    .send((net_effect, tx, Span::current()))
                    .await
                    .is_err()
                {
                    return Event::Net(NetEvent::Error(CoreNetError::ChannelClosed));
                }

                match rx.await {
                    Ok(event) => Event::Net(event),
                    Err(_) => Event::Net(NetEvent::Error(CoreNetError::ChannelClosed)),
                }
            }
            _ => Event::Net(NetEvent::Error(CoreNetError::InvalidEffect)),
        }
    }
}

fn effect_kind(effect: &Effect) -> &'static str {
    match effect {
        Effect::Net(NetEffect::Dht(_)) => "dht",
        Effect::Net(NetEffect::DocumentSync(_)) => "document_sync",
        Effect::Net(NetEffect::Stream(_)) => "stream",
        Effect::Net(NetEffect::JobControl(_)) => "job_control",
        Effect::Net(NetEffect::AuditPage(_)) => "audit_page",
        Effect::Net(NetEffect::PolicyFetch(_)) => "policy_fetch",
        Effect::Net(NetEffect::JobRecord(_)) => "job_record",
        Effect::Net(NetEffect::LaunchOffer(_)) => "launch_offer",
        Effect::Net(NetEffect::PolicySign(_)) => "policy_sign",
        Effect::Blob(_) => "blob",
        Effect::StagingSource(_) => "staging_source",
        Effect::LocalFile(_) => "local_file",
        Effect::Storage(_) => "storage",
        Effect::Metadata(_) => "metadata",
        Effect::SubOperation(_) => "suboperation",
        Effect::Task(_) => "task",
        Effect::Search() => "search",
        Effect::Stream() => "stream",
    }
}
pub(crate) fn unique_endpoint_addrs(
    mut endpoint_addrs: Vec<EndpointAddr>,
    local_id: NodeId,
) -> Vec<EndpointAddr> {
    let mut unique = Vec::<EndpointAddr>::new();
    for endpoint_addr in endpoint_addrs.drain(..) {
        if endpoint_addr.id == local_id {
            continue;
        }
        if let Some(existing) = unique
            .iter_mut()
            .find(|existing| existing.id == endpoint_addr.id)
        {
            *existing = endpoint_addr;
        } else {
            unique.push(endpoint_addr);
        }
    }
    unique.sort_unstable_by(|a, b| a.id.as_bytes().cmp(b.id.as_bytes()));
    unique
}

pub(crate) fn unique_peer_nodes(mut nodes: Vec<NodeId>, local_id: NodeId) -> Vec<NodeId> {
    nodes.retain(|node| *node != local_id);
    nodes.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    nodes.dedup();
    nodes
}
