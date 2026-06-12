use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::document::{
    DocumentSyncEvent, DocumentSyncPublish, DocumentSyncReconcileResult, DocumentSyncTarget,
    IrokleEvent,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::IROKLE_APPLIED_OPS_KEYSPACE;
use aruna_core::metadata::{MetadataCreateEventRecord, MetadataGraphLifecycleRecord};
use aruna_core::storage_entries::{
    metadata_graph_lifecycle_key, metadata_graph_lifecycle_write_entry,
    metadata_registry_delete_entries, metadata_registry_write_entries, stale_subject_index_deletes,
    subject_index_writes,
};
use aruna_core::structs::{MetadataRegistryRecord, User};
use aruna_core::types::Value;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use irokle_crate::Event as _;
use irokle_crate::Storage as _;
use irokle_crate::TopicControl;
use irokle_crate::history::HistoryOrder;
use irokle_crate::net::{decode_sync_message, encode_frame, encode_sync_message};
use irokle_crate::oplog::Oplog;
use irokle_crate::sync::{SyncData, SyncMessage, SyncRequest};
use irokle_crate::{EventEnvelope, PeerId, ReplicationPolicy, TopicGenesis, TopicPayload};
use aruna_core::telemetry::duration_ms;
use parking_lot::RwLock;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::error::{NetError, Result};
use crate::streams::BiStream;

use ::irokle as irokle_crate;

const IROKLE_PEER_SYNC_TIMEOUT: Duration = Duration::from_secs(30);
// Matches irokle's 1024-topic wire batches; the worst-case data stream sends
// three messages per topic, staying under the peer's 4096-message stream cap.
pub const IROKLE_BATCH_SYNC_TOPIC_LIMIT: usize = 1_024;
const IROKLE_INBOUND_SYNC_MESSAGE_LIMIT: usize = 4_096;
const IROKLE_INBOUND_SYNC_STREAM_BYTES: usize = 256 * 1024 * 1024;
const IROKLE_SYNC_FRAME_LEN_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Debug)]
struct PendingMetadataCreateApply {
    target: DocumentSyncTarget,
    record: MetadataCreateEventRecord,
    bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct IrokleService {
    node: irokle_crate::Irokle<irokle_crate::FjallStorage>,
    net: Arc<irokle_crate::net::IrohNet<irokle_crate::FjallStorage>>,
    db: fjall::OptimisticTxDatabase,
    storage: StorageHandle,
    default_peers: Arc<RwLock<BTreeSet<PeerId>>>,
    storage_path: PathBuf,
}

impl std::fmt::Debug for IrokleService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrokleService")
            .field("peer_id", &self.node.peer_id())
            .field("storage_path", &self.storage_path)
            .finish()
    }
}

impl IrokleService {
    pub fn open(
        endpoint: iroh::Endpoint,
        storage: StorageHandle,
        storage_path: impl AsRef<Path>,
        peer_nodes: &[NodeId],
        alpns: Vec<Vec<u8>>,
        runtime: irokle_crate::net::IrohRuntimeConfig,
    ) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let default_peers: BTreeSet<PeerId> = peer_nodes.iter().map(node_id_to_peer_id).collect();
        let db = fjall::OptimisticTxDatabase::builder(&storage_path)
            .manual_journal_persist(true)
            .open()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let node = irokle_crate::Irokle::builder()
            .with_iroh_secret_key(endpoint.secret_key())
            .with_peer_whitelist(default_peers.clone())
            .with_fjall_database_and_persist_mode(db.clone(), fjall::PersistMode::Buffer)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .build()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let net = Arc::new(
            irokle_crate::net::IrohNet::new_with_alpns_and_config(
                endpoint,
                node.clone(),
                alpns,
                runtime,
            )
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
        net.start_configured_resync_loop()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;

        Ok(Self {
            node,
            net,
            db,
            storage,
            default_peers: Arc::new(RwLock::new(default_peers)),
            storage_path,
        })
    }

    pub fn node(&self) -> irokle_crate::Irokle<irokle_crate::FjallStorage> {
        self.node.clone()
    }

    pub fn database(&self) -> fjall::OptimisticTxDatabase {
        self.db.clone()
    }

    pub fn allow_peer_node(&self, node_id: NodeId) -> Result<()> {
        let peer_id = node_id_to_peer_id(&node_id);
        if peer_id == self.node.peer_id() {
            return Ok(());
        }
        self.node
            .add_peer_to_whitelist(peer_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.flush_database()
    }

    pub fn add_potential_peer_node(&self, node_id: NodeId) -> Result<()> {
        let peer_id = node_id_to_peer_id(&node_id);
        if peer_id == self.node.peer_id() {
            return Ok(());
        }
        self.allow_peer_node(node_id)?;
        self.default_peers.write().insert(peer_id);
        Ok(())
    }

    pub fn add_potential_peer_nodes(&self, nodes: impl IntoIterator<Item = NodeId>) -> Result<()> {
        for node_id in nodes {
            self.add_potential_peer_node(node_id)?;
        }
        Ok(())
    }

    pub fn refresh_potential_peer_nodes(
        &self,
        nodes: impl IntoIterator<Item = NodeId>,
    ) -> Result<()> {
        let mut peers = BTreeSet::new();
        for node_id in nodes {
            let peer_id = node_id_to_peer_id(&node_id);
            if peer_id == self.node.peer_id() {
                continue;
            }
            peers.insert(peer_id);
        }
        self.node
            .add_peers_to_whitelist(peers.iter().copied())
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        *self.default_peers.write() = peers;
        self.flush_database()?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.net.shutdown().await;
        if let Err(error) = self.db.persist(fjall::PersistMode::SyncAll) {
            warn!(error = %error, "Failed to persist Irokle database on shutdown");
        }
    }

    pub async fn sync_topic_with_peers(
        &self,
        topic_id: irokle_crate::TopicId,
        peers: Vec<NodeId>,
    ) -> Result<()> {
        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;
        self.sync_topic(topic_id, sync_peers).await?;
        self.flush_database()
    }

    /// Notes a live inbound Irokle connection so the resync scheduler retries
    /// the peer immediately. The connection itself is not pooled for outbound
    /// reuse: streams opened over it toward the original dialer would never be
    /// accepted, because only connections accepted by our accept loop serve
    /// inbound streams.
    pub fn register_inbound_connection(&self, connection: &iroh::endpoint::Connection) {
        self.net
            .note_peer_reachable(node_id_to_peer_id(&connection.remote_id()));
    }

    pub async fn handle_inbound_stream(
        &self,
        stream: BiStream,
        peer: NodeId,
    ) -> Result<Vec<irokle_crate::TopicId>> {
        let stream_started = Instant::now();
        self.net.note_peer_reachable(node_id_to_peer_id(&peer));
        let BiStream(mut send, mut recv, _) = stream;
        let (messages, touched_topics) = read_inbound_sync_messages(&mut recv).await?;
        let read_elapsed = stream_started.elapsed();
        let message_count = messages.len();
        let handle_started = Instant::now();
        let net = self.net.clone();
        let responses = tokio::task::spawn_blocking(move || net.handle_messages(peer, messages))
            .await
            .map_err(|error| NetError::Stream(error.to_string()))?
            .map_err(|error| NetError::Stream(error.to_string()))?;
        let handle_elapsed = handle_started.elapsed();
        let write_started = Instant::now();
        write_inbound_sync_messages(&mut send, &responses).await?;
        let write_elapsed = write_started.elapsed();
        let flush_started = Instant::now();
        self.flush_database()?;
        info!(
            event = "pipeline.inbound_sync.summary",
            peer = %node_id_to_peer_id(&peer),
            messages = message_count,
            responses = responses.len(),
            topics = touched_topics.len(),
            read_ms = duration_ms(read_elapsed),
            handle_ms = duration_ms(handle_elapsed),
            write_ms = duration_ms(write_elapsed),
            flush_ms = duration_ms(flush_started.elapsed()),
            total_ms = duration_ms(stream_started.elapsed()),
            "Inbound Irokle sync stream summary"
        );
        Ok(touched_topics)
    }

    pub async fn reconcile_irokle_topics(
        &self,
        topic_ids: Vec<irokle_crate::TopicId>,
    ) -> Result<DocumentSyncReconcileResult> {
        self.reconcile_document_topics(topic_ids).await
    }

    pub async fn publish_document(
        &self,
        event_id: Ulid,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let event = DocumentSyncEvent::Upsert {
            event_id,
            target: target.clone(),
            bytes,
        };
        match self.publish_event(event, peers).await {
            Ok(()) => IrokleEvent::DocumentPublished { target },
            Err(error) => IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            },
        }
    }

    pub async fn delete_document(
        &self,
        event_id: Ulid,
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let event = DocumentSyncEvent::Delete {
            event_id,
            target: target.clone(),
        };
        match self.publish_event(event, peers).await {
            Ok(()) => IrokleEvent::DocumentDeleted { target },
            Err(error) => IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            },
        }
    }

    pub async fn publish_documents(
        &self,
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let targets = documents
            .iter()
            .map(|document| document.target().clone())
            .collect::<Vec<_>>();
        match self.publish_events(documents, peers).await {
            Ok(()) => IrokleEvent::DocumentsPublished { targets },
            Err(error) => IrokleEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn reconcile_documents_event(&self) -> IrokleEvent {
        match self.reconcile_documents().await {
            Ok(result) => IrokleEvent::DocumentsReconciled {
                applied: result.applied(),
                targets: result.targets,
                metadata_create_events: result.metadata_create_events,
            },
            Err(error) => IrokleEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_document_event(
        &self,
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let topic_id = target.irokle_topic_id();
        let sync_peers = self.sync_peers(peers);
        if let Err(error) = self.allow_sync_peers(&sync_peers) {
            return IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            };
        }
        match self.has_topic(topic_id) {
            Ok(true) => {
                if let Err(error) = self.sync_topic(topic_id, sync_peers).await {
                    return IrokleEvent::Error {
                        target: Some(target),
                        error: error.to_string(),
                    };
                }
            }
            Ok(false) => {
                if let Err(error) = self.bootstrap_topic_from_peers(topic_id, &sync_peers).await {
                    return IrokleEvent::Error {
                        target: Some(target),
                        error: error.to_string(),
                    };
                }
            }
            Err(error) => {
                return IrokleEvent::Error {
                    target: Some(target),
                    error: error.to_string(),
                };
            }
        }
        if let Err(error) = self.flush_database() {
            return IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            };
        }
        match self.reconcile_document_topics([topic_id]).await {
            Ok(result) => IrokleEvent::DocumentsReconciled {
                applied: result.applied(),
                targets: result.targets,
                metadata_create_events: result.metadata_create_events,
            },
            Err(error) => IrokleEvent::Error {
                target: Some(target),
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_documents_event(
        &self,
        targets: Vec<DocumentSyncTarget>,
        peers: Vec<NodeId>,
    ) -> IrokleEvent {
        let sync_started = Instant::now();
        let target_count = targets.len();
        let sync_peers = self.sync_peers(peers);
        if let Err(error) = self.allow_sync_peers(&sync_peers) {
            return IrokleEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }

        let mut seen_topics = BTreeSet::new();
        let mut topics: Vec<(irokle_crate::TopicId, DocumentSyncTarget)> = Vec::new();
        for target in targets {
            let topic_id = target.irokle_topic_id();
            if !seen_topics.insert(topic_id) {
                continue;
            }
            match self.has_topic(topic_id) {
                Ok(true) => topics.push((topic_id, target)),
                Ok(false) => {
                    if let Err(error) = self.bootstrap_topic_from_peers(topic_id, &sync_peers).await
                    {
                        return IrokleEvent::Error {
                            target: Some(target),
                            error: error.to_string(),
                        };
                    }
                    topics.push((topic_id, target));
                }
                Err(error) => {
                    return IrokleEvent::Error {
                        target: Some(target),
                        error: error.to_string(),
                    };
                }
            }
        }

        let bootstrap_elapsed = sync_started.elapsed();
        let topic_ids = topics
            .iter()
            .map(|(topic_id, _)| *topic_id)
            .collect::<Vec<_>>();
        let peer_sync_started = Instant::now();
        if let Err(error) = self.sync_topics(topic_ids.clone(), sync_peers).await {
            return IrokleEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }
        let peer_sync_elapsed = peer_sync_started.elapsed();

        let flush_started = Instant::now();
        if let Err(error) = self.flush_database() {
            return IrokleEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }
        let flush_elapsed = flush_started.elapsed();
        let reconcile_started = Instant::now();
        match self.reconcile_document_topics(topic_ids).await {
            Ok(result) => {
                info!(
                    event = "pipeline.sync.summary",
                    targets = target_count,
                    applied = result.applied(),
                    bootstrap_ms = duration_ms(bootstrap_elapsed),
                    peer_sync_ms = duration_ms(peer_sync_elapsed),
                    flush_ms = duration_ms(flush_elapsed),
                    reconcile_ms = duration_ms(reconcile_started.elapsed()),
                    total_ms = duration_ms(sync_started.elapsed()),
                    "Document sync batch summary"
                );
                IrokleEvent::DocumentsReconciled {
                    applied: result.applied(),
                    targets: result.targets,
                    metadata_create_events: result.metadata_create_events,
                }
            }
            Err(error) => IrokleEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    async fn publish_event(&self, event: DocumentSyncEvent, peers: Vec<NodeId>) -> Result<()> {
        let topic_id = event.target().irokle_topic_id();
        let document = match event {
            DocumentSyncEvent::Upsert {
                event_id,
                target,
                bytes,
            } => DocumentSyncPublish::Upsert {
                event_id,
                target,
                bytes,
            },
            DocumentSyncEvent::Delete { event_id, target } => {
                DocumentSyncPublish::Delete { event_id, target }
            }
        };
        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;
        let service = self.clone();
        let published = tokio::task::spawn_blocking(move || {
            service.publish_events_blocking(vec![document], &sync_peers)
        })
        .await
        .map_err(|error| NetError::Bootstrap(error.to_string()))??;
        self.advance_topic_cursors(published).await?;
        self.net.schedule_topic_recheck(topic_id)?;
        self.flush_database()?;
        Ok(())
    }

    async fn publish_events(
        &self,
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;
        let service = self.clone();
        let published = tokio::task::spawn_blocking(move || {
            service.publish_events_blocking(documents, &sync_peers)
        })
        .await
        .map_err(|error| NetError::Bootstrap(error.to_string()))??;
        self.advance_topic_cursors(published).await?;
        self.flush_database()
    }

    fn publish_events_blocking(
        &self,
        documents: Vec<DocumentSyncPublish>,
        sync_peers: &BTreeSet<PeerId>,
    ) -> Result<BTreeMap<irokle_crate::TopicId, irokle_crate::ActorClock>> {
        let publish_started = Instant::now();
        let document_count = documents.len();
        let mut fast_path = 0usize;
        let mut fallback = 0usize;
        let oplog = Oplog::with_storage(self.node.storage().clone());
        let mut published: BTreeMap<irokle_crate::TopicId, irokle_crate::ActorClock> =
            BTreeMap::new();
        for document in documents {
            let event = match document {
                DocumentSyncPublish::Upsert {
                    event_id,
                    target,
                    bytes,
                } => DocumentSyncEvent::Upsert {
                    event_id,
                    target,
                    bytes,
                },
                DocumentSyncPublish::Delete { event_id, target } => {
                    DocumentSyncEvent::Delete { event_id, target }
                }
            };
            let target = event.target().clone();
            let topic_id = target.irokle_topic_id();
            let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
            let envelope = EventEnvelope::encode_event(&event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let op = self.publish_event_op(
                &oplog,
                &target,
                topic_id,
                actor_id,
                envelope,
                sync_peers,
                &mut fast_path,
                &mut fallback,
            )?;
            published
                .entry(topic_id)
                .or_default()
                .observe(op.signed.body.actor_id, op.signed.body.actor_seq);
        }
        info!(
            event = "pipeline.publish.summary",
            documents = document_count,
            fast_path,
            fallback,
            existing = document_count - fast_path - fallback,
            total_ms = duration_ms(publish_started.elapsed()),
            "Irokle publish batch breakdown"
        );
        Ok(published)
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_event_op(
        &self,
        oplog: &Oplog<irokle_crate::FjallStorage>,
        target: &DocumentSyncTarget,
        topic_id: irokle_crate::TopicId,
        actor_id: irokle_crate::ActorId,
        envelope: EventEnvelope,
        sync_peers: &BTreeSet<PeerId>,
        fast_path: &mut usize,
        fallback: &mut usize,
    ) -> Result<irokle_crate::Op> {
        // Fast path for brand-new topics: genesis + first event admitted in a
        // single storage transaction. Any failure (e.g. a concurrent admission
        // won the genesis race) falls back to the existing two-step flow.
        let topic_missing = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .is_none();
        if topic_missing {
            let genesis = TopicGenesis {
                event_type_id: DocumentSyncEvent::TYPE_ID.to_string(),
                initial_peers: sync_peers.clone(),
                replication_policy: ReplicationPolicy::all(),
            };
            match oplog.create_topic_genesis_with_event(
                topic_id,
                actor_id,
                genesis,
                envelope.clone(),
                self.node.signer(),
            ) {
                Ok((_, event_op)) => {
                    *fast_path += 1;
                    self.net.schedule_topic_recheck(topic_id)?;
                    return Ok(event_op);
                }
                Err(error) => {
                    *fallback += 1;
                    debug!(%topic_id, error = %error, "genesis+event fast path failed, falling back");
                }
            }
        }
        let topic_id = self.ensure_topic(target, sync_peers)?;
        oplog
            .create_event_op(topic_id, actor_id, envelope, self.node.signer())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    /// Marks locally published ops as applied by advancing the per-topic
    /// cursor, so the origin's own reconcile does not re-emit them. Their
    /// effects are always applied locally before the outbox publish runs.
    async fn advance_topic_cursors(
        &self,
        published: BTreeMap<irokle_crate::TopicId, irokle_crate::ActorClock>,
    ) -> Result<()> {
        if published.is_empty() {
            return Ok(());
        }
        let mut writes = Vec::with_capacity(published.len());
        for (topic_id, clock) in published {
            let cursor_key = topic_cursor_key(topic_id);
            let mut cursor: irokle_crate::ActorClock = match self
                .storage_read(IROKLE_APPLIED_OPS_KEYSPACE.to_string(), cursor_key.clone())
                .await?
            {
                Some(value) => postcard::from_bytes(value.as_ref()).unwrap_or_default(),
                None => irokle_crate::ActorClock::default(),
            };
            cursor.merge(&clock);
            let value = ByteView::from(
                postcard::to_allocvec(&cursor)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            );
            writes.push((IROKLE_APPLIED_OPS_KEYSPACE.to_string(), cursor_key, value));
        }
        self.storage_batch_write(writes).await
    }

    fn flush_database(&self) -> Result<()> {
        self.db
            .persist(fjall::PersistMode::Buffer)
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    fn ensure_topic(
        &self,
        target: &DocumentSyncTarget,
        peers: &BTreeSet<PeerId>,
    ) -> Result<irokle_crate::TopicId> {
        let topic_id = target.irokle_topic_id();
        let mut genesis_error = None;
        for _ in 0..2 {
            if let Some(state) = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
            {
                if state.event_type_id != DocumentSyncEvent::TYPE_ID {
                    return Err(NetError::Bootstrap(format!(
                        "Irokle topic {topic_id} has event type {}, expected {}",
                        state.event_type_id,
                        DocumentSyncEvent::TYPE_ID
                    )));
                }
                let missing_peers = peers
                    .iter()
                    .copied()
                    .filter(|peer| !state.members.contains(peer))
                    .collect::<Vec<_>>();
                if !missing_peers.is_empty() {
                    let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
                    let oplog = Oplog::with_storage(self.node.storage().clone());
                    for peer in missing_peers {
                        oplog
                            .create_control_op(
                                topic_id,
                                actor_id,
                                TopicControl::AddPeer { peer },
                                self.node.signer(),
                            )
                            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                    }
                    self.net.schedule_topic_recheck(topic_id)?;
                }
                return Ok(topic_id);
            }

            let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
            let genesis = TopicGenesis {
                event_type_id: DocumentSyncEvent::TYPE_ID.to_string(),
                initial_peers: peers.clone(),
                replication_policy: ReplicationPolicy::all(),
            };
            let oplog = Oplog::with_storage(self.node.storage().clone());
            match oplog.create_topic_genesis(topic_id, actor_id, genesis, self.node.signer()) {
                Ok(_) => {
                    self.net.schedule_topic_recheck(topic_id)?;
                    return Ok(topic_id);
                }
                // A concurrent admission may have created the topic between the
                // state read and the genesis commit; re-check and reuse it.
                Err(error) => genesis_error = Some(error),
            }
        }
        Err(NetError::Bootstrap(
            genesis_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| format!("failed to ensure Irokle topic {topic_id}")),
        ))
    }

    fn has_topic(&self, topic_id: irokle_crate::TopicId) -> Result<bool> {
        Ok(self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .is_some())
    }

    fn sync_peers(&self, peers: Vec<NodeId>) -> BTreeSet<PeerId> {
        let mut sync_peers = if peers.is_empty() {
            self.default_peers.read().clone()
        } else {
            peers
                .into_iter()
                .map(|node_id| node_id_to_peer_id(&node_id))
                .collect()
        };
        sync_peers.remove(&self.node.peer_id());
        sync_peers
    }

    fn allow_sync_peers(&self, peers: &BTreeSet<PeerId>) -> Result<()> {
        self.node
            .add_peers_to_whitelist(peers.iter().copied())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    async fn fan_out_peer_syncs<F, Fut>(
        &self,
        peers: BTreeSet<PeerId>,
        context: String,
        run: F,
    ) -> Result<()>
    where
        F: Fn(PeerId) -> Fut,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let attempted = peers.len();
        if attempted == 0 {
            return Ok(());
        }

        let fanout_started = Instant::now();
        let mut syncs = JoinSet::new();
        for peer in peers {
            let future = run(peer);
            syncs.spawn(async move {
                let peer_started = Instant::now();
                let result = future.await;
                (peer, result, peer_started.elapsed())
            });
        }
        let mut successes = 0usize;
        let mut first_error = None;
        let mut per_peer = Vec::with_capacity(attempted);
        while let Some(result) = syncs.join_next().await {
            match result {
                Ok((peer, Ok(()), elapsed)) => {
                    successes += 1;
                    per_peer.push(format!("{}={}ms", short_peer(peer), duration_ms(elapsed)));
                    debug!(%peer, context = %context, "Synced Irokle document peer")
                }
                Ok((peer, Err(error), elapsed)) => {
                    per_peer.push(format!(
                        "{}={}ms(err)",
                        short_peer(peer),
                        duration_ms(elapsed)
                    ));
                    warn!(%peer, context = %context, error = %error, "Irokle peer sync failed; deferring to resync scheduler");
                    if first_error.is_none() {
                        first_error = Some(error.to_string());
                    }
                }
                Err(error) => {
                    warn!(context = %context, error = %error, "Irokle peer sync task failed");
                    if first_error.is_none() {
                        first_error = Some(error.to_string());
                    }
                }
            }
        }
        info!(
            event = "pipeline.fanout.summary",
            context = %context,
            peers = attempted,
            ok = successes,
            failed = attempted - successes,
            total_ms = duration_ms(fanout_started.elapsed()),
            per_peer = %per_peer.join(","),
            "Irokle peer fan-out summary"
        );
        if successes == 0 {
            let detail = first_error.unwrap_or_else(|| "unknown sync error".to_string());
            return Err(NetError::Bootstrap(format!(
                "{context}: all {attempted} peers failed; {detail}"
            )));
        }
        Ok(())
    }

    async fn sync_topic(
        &self,
        topic_id: irokle_crate::TopicId,
        peers: BTreeSet<PeerId>,
    ) -> Result<()> {
        let net = self.net.clone();
        self.fan_out_peer_syncs(peers, format!("Irokle topic {topic_id}"), move |peer| {
            let net = net.clone();
            async move {
                match timeout(IROKLE_PEER_SYNC_TIMEOUT, net.sync_peer_now(peer, topic_id)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(NetError::Bootstrap(error.to_string())),
                    Err(_) => Err(NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT)),
                }
            }
        })
        .await
    }

    async fn sync_topics(
        &self,
        topic_ids: Vec<irokle_crate::TopicId>,
        peers: BTreeSet<PeerId>,
    ) -> Result<()> {
        if topic_ids.is_empty() || peers.is_empty() {
            return Ok(());
        }
        for chunk in topic_ids.chunks(IROKLE_BATCH_SYNC_TOPIC_LIMIT) {
            self.sync_topic_batch(chunk, peers.clone()).await?;
        }
        Ok(())
    }

    async fn sync_topic_batch(
        &self,
        topic_ids: &[irokle_crate::TopicId],
        peers: BTreeSet<PeerId>,
    ) -> Result<()> {
        if topic_ids.is_empty() {
            return Ok(());
        }
        let service = self.clone();
        let topic_ids = topic_ids.to_vec();
        self.fan_out_peer_syncs(
            peers,
            format!("Irokle topic batch of {} topics", topic_ids.len()),
            move |peer| {
                let service = service.clone();
                let topic_ids = topic_ids.clone();
                async move { service.sync_topic_batch_with_peer(peer, topic_ids).await }
            },
        )
        .await
    }

    async fn sync_topic_batch_with_peer(
        &self,
        peer: PeerId,
        topic_ids: Vec<irokle_crate::TopicId>,
    ) -> Result<()> {
        let batch_started = Instant::now();
        let topic_count = topic_ids.len();
        let peer_addr = peer_id_to_endpoint_addr(peer)?;
        let mut known_topics = BTreeSet::new();
        let mut local_fingerprints = BTreeMap::new();
        let mut initial_messages = Vec::with_capacity(topic_ids.len().saturating_mul(2));
        for topic_id in topic_ids {
            let fingerprint = self
                .node
                .sync_fingerprint(topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            known_topics.insert(topic_id);
            local_fingerprints.insert(topic_id, fingerprint.fingerprint);
            initial_messages.push(SyncMessage::Open(self.node.sync_open(topic_id)));
            initial_messages.push(SyncMessage::Fingerprint(fingerprint));
        }
        let r1_build = batch_started.elapsed();

        let r1_io_started = Instant::now();
        let responses = timeout(
            IROKLE_PEER_SYNC_TIMEOUT,
            self.net.sync_with(peer_addr.clone(), &initial_messages),
        )
        .await
        .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;
        let r1_io = r1_io_started.elapsed();
        let r1_process_started = Instant::now();
        let node = self.node.clone();
        let summary_known = known_topics.clone();
        let (responded_topics, failed_topics, sync_messages) =
            tokio::task::spawn_blocking(move || {
                process_batch_summary_responses(
                    &node,
                    peer,
                    &summary_known,
                    &local_fingerprints,
                    responses,
                )
            })
            .await
            .map_err(|error| NetError::Bootstrap(error.to_string()))??;
        let r1_process = r1_process_started.elapsed();
        if responded_topics.len() != known_topics.len() {
            return Err(NetError::Bootstrap(format!(
                "peer {peer} responded for {}/{} Irokle batch topics",
                responded_topics.len(),
                known_topics.len()
            )));
        }
        if sync_messages.is_empty() {
            log_peer_batch_summary(
                peer,
                topic_count,
                r1_build,
                r1_io,
                r1_process,
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
                0,
                batch_started.elapsed(),
            );
            return finish_batch_sync(peer, &known_topics, &failed_topics);
        }

        let r2_message_count = sync_messages.len();
        let r2_io_started = Instant::now();
        let responses = timeout(
            IROKLE_PEER_SYNC_TIMEOUT,
            self.net.sync_with(peer_addr.clone(), &sync_messages),
        )
        .await
        .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;
        let r2_io = r2_io_started.elapsed();
        let r2_process_started = Instant::now();
        let node = self.node.clone();
        let net = self.net.clone();
        let data_known = known_topics.clone();
        let (failed_topics, followup) = tokio::task::spawn_blocking(move || {
            process_batch_data_responses(&node, &net, peer, &data_known, failed_topics, responses)
        })
        .await
        .map_err(|error| NetError::Bootstrap(error.to_string()))??;
        let r2_process = r2_process_started.elapsed();
        let fu_io_started = Instant::now();
        if !followup.is_empty() {
            let responses = timeout(
                IROKLE_PEER_SYNC_TIMEOUT,
                self.net.sync_with(peer_addr, &followup),
            )
            .await
            .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
            .map_err(NetError::from)?;
            for response in responses {
                match response {
                    SyncMessage::Summary(summary) if known_topics.contains(&summary.topic_id) => {}
                    other => {
                        return Err(NetError::Bootstrap(format!(
                            "unexpected Irokle batch ack response from {peer}: {other:?}"
                        )));
                    }
                }
            }
        }
        log_peer_batch_summary(
            peer,
            topic_count,
            r1_build,
            r1_io,
            r1_process,
            r2_io,
            r2_process,
            fu_io_started.elapsed(),
            r2_message_count,
            batch_started.elapsed(),
        );
        finish_batch_sync(peer, &known_topics, &failed_topics)
    }

    async fn bootstrap_topic_from_peers(
        &self,
        topic_id: irokle_crate::TopicId,
        peers: &BTreeSet<PeerId>,
    ) -> Result<()> {
        let mut first_error = None;
        for peer in peers {
            match self.bootstrap_topic_from_peer(topic_id, *peer).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    warn!(%peer, %topic_id, error = %error, "Irokle document bootstrap attempt failed");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        Err(first_error.unwrap_or_else(|| {
            NetError::Bootstrap(format!(
                "no peers available to bootstrap Irokle topic {topic_id}"
            ))
        }))
    }

    async fn bootstrap_topic_from_peer(
        &self,
        topic_id: irokle_crate::TopicId,
        peer: PeerId,
    ) -> Result<()> {
        let peer_addr = peer_id_to_endpoint_addr(peer)?;
        let responses = timeout(
            IROKLE_PEER_SYNC_TIMEOUT,
            self.net.sync_with(
                peer_addr.clone(),
                &[SyncMessage::Open(self.node.sync_open(topic_id))],
            ),
        )
        .await
        .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;
        let summary = responses
            .into_iter()
            .find_map(|response| match response {
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => Some(summary),
                _ => None,
            })
            .ok_or_else(|| {
                NetError::Bootstrap(format!(
                    "peer {peer} did not return an Irokle summary for topic {topic_id}"
                ))
            })?;
        if summary.event_type_id.as_deref() != Some(DocumentSyncEvent::TYPE_ID) {
            return Err(NetError::Bootstrap(format!(
                "peer {peer} advertised Irokle topic {topic_id} with unexpected event type {:?}",
                summary.event_type_id
            )));
        }

        let request = SyncRequest {
            topic_id,
            known: BTreeSet::new(),
            wants: summary.heads,
            actor_range_hints: Vec::new(),
        };
        let responses = timeout(
            IROKLE_PEER_SYNC_TIMEOUT,
            self.net.sync_with(
                peer_addr.clone(),
                &[
                    SyncMessage::Open(self.node.sync_open(topic_id)),
                    SyncMessage::Request(request),
                ],
            ),
        )
        .await
        .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;

        let mut followup = vec![SyncMessage::Open(self.node.sync_open(topic_id))];
        let mut received_data = false;
        for response in responses {
            match response {
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                SyncMessage::Data(data) if data.topic_id == topic_id => {
                    let ack = self
                        .node
                        .receive_sync_data_from(peer, data)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                    received_data = true;
                    followup.push(SyncMessage::Ack(ack));
                }
                other => {
                    return Err(NetError::Bootstrap(format!(
                        "unexpected Irokle bootstrap response: {other:?}"
                    )));
                }
            }
        }
        if received_data {
            self.net.schedule_topic_recheck(topic_id)?;
        }
        if followup.len() > 1 {
            let responses = timeout(
                IROKLE_PEER_SYNC_TIMEOUT,
                self.net.sync_with(peer_addr, &followup),
            )
            .await
            .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
            .map_err(NetError::from)?;
            for response in responses {
                match response {
                    SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                    other => {
                        return Err(NetError::Bootstrap(format!(
                            "unexpected Irokle bootstrap ack response: {other:?}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    async fn reconcile_documents(&self) -> Result<DocumentSyncReconcileResult> {
        let topics = self.document_topic_ids()?;
        self.reconcile_document_topics(topics).await
    }

    fn document_topic_ids(&self) -> Result<Vec<irokle_crate::TopicId>> {
        let topics = self
            .node
            .list_topics()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        Ok(topics
            .into_iter()
            .filter(|topic| topic.event_type_id == DocumentSyncEvent::TYPE_ID)
            .map(|topic| topic.topic_id)
            .collect())
    }

    async fn reconcile_document_topics(
        &self,
        topic_ids: impl IntoIterator<Item = irokle_crate::TopicId>,
    ) -> Result<DocumentSyncReconcileResult> {
        let mut seen_topics = BTreeSet::new();
        let mut applied_targets = Vec::new();
        let mut metadata_create_events = Vec::new();
        let mut pending_metadata_creates = Vec::new();
        let mut deferred_cursor_writes = Vec::new();
        for topic_id in topic_ids {
            if !seen_topics.insert(topic_id) {
                continue;
            }
            let Some(topic) = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
            else {
                continue;
            };
            if topic.event_type_id != DocumentSyncEvent::TYPE_ID {
                continue;
            }
            let cursor_key = topic_cursor_key(topic_id);
            let mut cursor: irokle_crate::ActorClock = match self
                .storage_read(IROKLE_APPLIED_OPS_KEYSPACE.to_string(), cursor_key.clone())
                .await?
            {
                Some(value) => postcard::from_bytes(value.as_ref()).unwrap_or_default(),
                None => irokle_crate::ActorClock::default(),
            };
            let topic_clock = self
                .node
                .storage()
                .actor_clock(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if cursor.dominates(&topic_clock) {
                continue;
            }
            let events = self.document_events_after(topic_id, &cursor)?;
            // Every admitted op counted in `topic_clock` is either one of
            // `events`, an already-applied event, or a control op, so the
            // merged clock is the new applied watermark.
            cursor.merge(&topic_clock);
            let mut deferred_creates = false;
            for event in events {
                let target_topic_id = event.target().irokle_topic_id();
                if target_topic_id != topic_id {
                    warn!(
                        %topic_id,
                        %target_topic_id,
                        "Skipping Irokle document event whose target does not match its topic"
                    );
                    continue;
                }
                if matches!(
                    event,
                    DocumentSyncEvent::Upsert {
                        target: DocumentSyncTarget::MetadataCreateEvent { .. },
                        ..
                    }
                ) {
                    let pending = self.pending_metadata_create_apply(event)?;
                    pending_metadata_creates.push(pending);
                    deferred_creates = true;
                    continue;
                }
                let target = event.target().clone();
                self.apply_document_event(event).await?;
                applied_targets.push(target);
            }
            let value = ByteView::from(
                postcard::to_allocvec(&cursor)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            );
            if deferred_creates {
                deferred_cursor_writes.push((
                    IROKLE_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key,
                    value,
                ));
            } else {
                self.storage_write(IROKLE_APPLIED_OPS_KEYSPACE.to_string(), cursor_key, value)
                    .await?;
            }
        }
        self.apply_metadata_create_batch(
            pending_metadata_creates,
            deferred_cursor_writes,
            &mut applied_targets,
            &mut metadata_create_events,
        )
        .await?;
        Ok(DocumentSyncReconcileResult {
            targets: applied_targets,
            metadata_create_events,
        })
    }

    /// Returns the decoded document events above the applied cursor, reading
    /// only the unapplied portion of the topic history where possible.
    fn document_events_after(
        &self,
        topic_id: irokle_crate::TopicId,
        cursor: &irokle_crate::ActorClock,
    ) -> Result<Vec<DocumentSyncEvent>> {
        match self.node.open_topic::<DocumentSyncEvent>(topic_id) {
            Ok(topic) => Ok(topic
                .history_after(cursor, HistoryOrder::OldestFirst)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into_iter()
                .map(|record| record.event)
                .collect()),
            // Topics we hold ops for without being a listed member still
            // reconcile via the full history.
            Err(irokle_crate::Error::NotTopicMember) => {
                let raw = self
                    .node
                    .raw_topic(topic_id)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                let ops = raw
                    .history()
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                let mut events = Vec::new();
                for op in ops {
                    if cursor.get(&op.signed.body.actor_id) >= op.signed.body.actor_seq {
                        continue;
                    }
                    let TopicPayload::Event(envelope) = op.signed.body.payload else {
                        continue;
                    };
                    events.push(
                        envelope
                            .decode_event::<DocumentSyncEvent>()
                            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                    );
                }
                Ok(events)
            }
            Err(error) => Err(NetError::Bootstrap(error.to_string())),
        }
    }

    fn pending_metadata_create_apply(
        &self,
        event: DocumentSyncEvent,
    ) -> Result<PendingMetadataCreateApply> {
        let DocumentSyncEvent::Upsert {
            target:
                DocumentSyncTarget::MetadataCreateEvent {
                    document_id,
                    event_id: target_event_id,
                },
            bytes,
            ..
        } = event
        else {
            unreachable!("metadata create apply helper is only called for metadata create upserts");
        };
        let record: MetadataCreateEventRecord =
            postcard::from_bytes(&bytes).map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if record.record.document_id != document_id || record.event_id != target_event_id {
            return Err(NetError::Bootstrap(format!(
                "replicated metadata create-event target {document_id}/{target_event_id} does not match payload {}/{}",
                record.record.document_id, record.event_id
            )));
        }
        Ok(PendingMetadataCreateApply {
            target: DocumentSyncTarget::MetadataCreateEvent {
                document_id,
                event_id: target_event_id,
            },
            record,
            bytes,
        })
    }

    async fn apply_metadata_create_batch(
        &self,
        pending: Vec<PendingMetadataCreateApply>,
        cursor_writes: Vec<(String, ByteView, Value)>,
        applied_targets: &mut Vec<DocumentSyncTarget>,
        metadata_create_events: &mut Vec<MetadataCreateEventRecord>,
    ) -> Result<()> {
        if pending.is_empty() && cursor_writes.is_empty() {
            return Ok(());
        }
        let mut writes = Vec::with_capacity(pending.len() + cursor_writes.len());
        for apply in &pending {
            writes.push((
                apply.target.storage_keyspace().to_string(),
                apply.target.storage_key(),
                ByteView::from(apply.bytes.clone()),
            ));
        }
        writes.extend(cursor_writes);
        self.storage_batch_write(writes).await?;
        for apply in pending {
            applied_targets.push(apply.target);
            metadata_create_events.push(apply.record);
        }
        Ok(())
    }

    async fn apply_document_event(&self, event: DocumentSyncEvent) -> Result<()> {
        match event {
            DocumentSyncEvent::Upsert { target, bytes, .. } => {
                self.apply_upsert(target, bytes).await
            }
            DocumentSyncEvent::Delete { target, .. } => self.apply_delete(target).await,
        }
    }

    async fn apply_upsert(&self, target: DocumentSyncTarget, bytes: Vec<u8>) -> Result<()> {
        if let DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
        } = target
        {
            let record: MetadataCreateEventRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.record.document_id != document_id || record.event_id != event_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata create-event target {document_id}/{event_id} does not match payload {}/{}",
                    record.record.document_id, record.event_id
                )));
            }
            return self
                .storage_write(
                    DocumentSyncTarget::MetadataCreateEvent {
                        document_id,
                        event_id,
                    }
                    .storage_keyspace()
                    .to_string(),
                    DocumentSyncTarget::MetadataCreateEvent {
                        document_id,
                        event_id,
                    }
                    .storage_key(),
                    bytes.into(),
                )
                .await;
        }
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            let record: MetadataRegistryRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.group_id != group_id || record.document_id != document_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata registry target {group_id}/{document_id} does not match payload {}/{}",
                    record.group_id, record.document_id
                )));
            }
            return self.apply_metadata_registry_upsert(record, bytes).await;
        }
        if let DocumentSyncTarget::MetadataGraphLifecycle { graph_iri } = target {
            let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.graph_iri != graph_iri {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                    record.graph_iri
                )));
            }
            return self.apply_metadata_graph_lifecycle(record, bytes).await;
        }
        if let DocumentSyncTarget::User { user_id } = target {
            let user =
                User::from_bytes(&bytes).map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if user.user_id != user_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated user document id {} does not match payload user id {}",
                    user_id, user.user_id
                )));
            }
            return self.apply_user_upsert(user, bytes).await;
        }
        self.storage_write(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.into(),
        )
        .await
    }

    async fn apply_user_upsert(&self, user: User, primary_bytes: Vec<u8>) -> Result<()> {
        let target = DocumentSyncTarget::User {
            user_id: user.user_id,
        };
        let previous = self
            .storage_read(target.storage_keyspace().to_string(), target.storage_key())
            .await?
            .map(|bytes| User::from_bytes(&bytes))
            .transpose()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;

        let deletes = stale_subject_index_deletes(previous.as_ref(), Some(&user));
        if !deletes.is_empty() {
            self.storage_batch_delete(deletes).await?;
        }

        let mut writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            primary_bytes.into(),
        )];
        writes.extend(subject_index_writes(&user));
        self.storage_batch_write(writes).await?;
        Ok(())
    }

    async fn apply_metadata_registry_upsert(
        &self,
        record: MetadataRegistryRecord,
        primary_bytes: Vec<u8>,
    ) -> Result<()> {
        if self.metadata_graph_deleted(&record.graph_iri).await? {
            return self
                .storage_batch_delete(metadata_registry_delete_entries(
                    record.group_id,
                    record.document_id,
                ))
                .await;
        }
        let mut entries = metadata_registry_write_entries(&record)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if let Some((_, _, value)) = entries.first_mut() {
            *value = primary_bytes.into();
        }
        self.storage_batch_write(entries).await
    }

    async fn apply_metadata_graph_lifecycle(
        &self,
        record: MetadataGraphLifecycleRecord,
        primary_bytes: Vec<u8>,
    ) -> Result<()> {
        let (key_space, key, _) = metadata_graph_lifecycle_write_entry(&record)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.storage_write(key_space, key, primary_bytes.into())
            .await?;
        if record.is_deleted() {
            self.storage_batch_delete(metadata_registry_delete_entries(
                record.group_id,
                record.document_id,
            ))
            .await?;
        }
        Ok(())
    }

    async fn metadata_graph_deleted(&self, graph_iri: &str) -> Result<bool> {
        let value = self
            .storage_read(
                aruna_core::keyspaces::METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                metadata_graph_lifecycle_key(graph_iri),
            )
            .await?;
        let Some(value) = value else {
            return Ok(false);
        };
        let record: MetadataGraphLifecycleRecord =
            postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
        Ok(record.is_deleted())
    }

    async fn apply_delete(&self, target: DocumentSyncTarget) -> Result<()> {
        if let DocumentSyncTarget::MetadataGraphLifecycle { .. } = target {
            return Ok(());
        }
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            return self
                .storage_batch_delete(metadata_registry_delete_entries(group_id, document_id))
                .await;
        }
        if let DocumentSyncTarget::User { user_id } = target {
            let target = DocumentSyncTarget::User { user_id };
            let previous = self
                .storage_read(target.storage_keyspace().to_string(), target.storage_key())
                .await?
                .map(|bytes| User::from_bytes(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let mut deletes = vec![(target.storage_keyspace().to_string(), target.storage_key())];
            if let Some(previous) = previous {
                deletes.extend(stale_subject_index_deletes(Some(&previous), None));
            }
            return self.storage_batch_delete(deletes).await;
        }
        self.storage_delete(target.storage_keyspace().to_string(), target.storage_key())
            .await
    }

    async fn storage_read(&self, key_space: String, key: ByteView) -> Result<Option<Value>> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle read: {other:?}"
            ))),
        }
    }

    async fn storage_write(&self, key_space: String, key: ByteView, value: Value) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle write: {other:?}"
            ))),
        }
    }

    async fn storage_batch_write(&self, writes: Vec<(String, ByteView, Value)>) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle batch write: {other:?}"
            ))),
        }
    }

    async fn storage_delete(&self, key_space: String, key: ByteView) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle delete: {other:?}"
            ))),
        }
    }

    async fn storage_batch_delete(&self, deletes: Vec<(String, ByteView)>) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle batch delete: {other:?}"
            ))),
        }
    }
}

fn node_id_to_peer_id(node_id: &NodeId) -> PeerId {
    PeerId::from_bytes(*node_id.as_bytes())
}

fn short_peer(peer: PeerId) -> String {
    let mut id = peer.to_string();
    id.truncate(8);
    id
}

fn topic_cursor_key(topic_id: irokle_crate::TopicId) -> ByteView {
    let mut key = b"topic-cursor/".to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

async fn read_inbound_sync_messages(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(Vec<SyncMessage>, Vec<irokle_crate::TopicId>)> {
    let mut messages = Vec::new();
    let mut topics = BTreeSet::new();
    let mut bytes_read = 0usize;
    let mut frame_index = 0usize;
    while let Some(frame) = read_next_inbound_sync_frame(recv, &mut bytes_read).await? {
        frame_index = frame_index.saturating_add(1);
        if messages.len() >= IROKLE_INBOUND_SYNC_MESSAGE_LIMIT {
            return Err(NetError::Stream(format!(
                "Irokle sync stream exceeded {IROKLE_INBOUND_SYNC_MESSAGE_LIMIT} messages"
            )));
        }
        let message = decode_sync_message(&frame).map_err(|error| {
            NetError::Stream(format!(
                "invalid Irokle sync message frame {frame_index} ({} bytes): {error}",
                frame.len()
            ))
        })?;
        topics.insert(sync_message_topic_id(&message));
        messages.push(message);
    }
    Ok((messages, topics.into_iter().collect()))
}

async fn read_next_inbound_sync_frame(
    recv: &mut iroh::endpoint::RecvStream,
    bytes_read: &mut usize,
) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    let Some(first_read) = read_some_inbound_sync(recv, &mut len_buf[..1]).await? else {
        return Ok(None);
    };
    if first_read == 0 {
        return Ok(None);
    }

    let mut read = first_read;
    while read < len_buf.len() {
        let Some(n) = read_some_inbound_sync(recv, &mut len_buf[read..]).await? else {
            return Err(NetError::Stream(
                "incomplete Irokle sync frame length".to_string(),
            ));
        };
        if n == 0 {
            return Err(NetError::Stream(
                "incomplete Irokle sync frame length".to_string(),
            ));
        }
        read += n;
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > IROKLE_SYNC_FRAME_LEN_LIMIT {
        return Err(NetError::Stream(
            "Irokle sync frame exceeds maximum length".to_string(),
        ));
    }
    *bytes_read = bytes_read.saturating_add(4).saturating_add(len);
    if *bytes_read > IROKLE_INBOUND_SYNC_STREAM_BYTES {
        return Err(NetError::Stream(format!(
            "Irokle sync stream exceeded {IROKLE_INBOUND_SYNC_STREAM_BYTES} bytes"
        )));
    }

    let mut payload = vec![0u8; len];
    let mut payload_read = 0usize;
    while payload_read < payload.len() {
        let Some(n) = read_some_inbound_sync(recv, &mut payload[payload_read..]).await? else {
            return Err(NetError::Stream(
                "incomplete Irokle sync frame payload".to_string(),
            ));
        };
        if n == 0 {
            return Err(NetError::Stream(
                "incomplete Irokle sync frame payload".to_string(),
            ));
        }
        payload_read += n;
    }
    Ok(Some(payload))
}

async fn read_some_inbound_sync(
    recv: &mut iroh::endpoint::RecvStream,
    buf: &mut [u8],
) -> Result<Option<usize>> {
    timeout(IROKLE_PEER_SYNC_TIMEOUT, recv.read(buf))
        .await
        .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
        .map_err(|error| NetError::Stream(error.to_string()))
}

async fn write_inbound_sync_messages(
    send: &mut iroh::endpoint::SendStream,
    messages: &[SyncMessage],
) -> Result<()> {
    for message in messages {
        let payload =
            encode_sync_message(message).map_err(|error| NetError::Stream(error.to_string()))?;
        let frame = encode_frame(&payload).map_err(|error| NetError::Stream(error.to_string()))?;
        timeout(IROKLE_PEER_SYNC_TIMEOUT, send.write_all(&frame))
            .await
            .map_err(|_| NetError::Timeout(IROKLE_PEER_SYNC_TIMEOUT))?
            .map_err(|error| NetError::Stream(error.to_string()))?;
    }
    send.finish()
        .map_err(|error| NetError::Stream(error.to_string()))
}

type BatchSummaryOutcome = (
    BTreeSet<irokle_crate::TopicId>,
    BTreeSet<irokle_crate::TopicId>,
    Vec<SyncMessage>,
);

fn process_batch_summary_responses(
    node: &irokle_crate::Irokle<irokle_crate::FjallStorage>,
    peer: PeerId,
    known_topics: &BTreeSet<irokle_crate::TopicId>,
    local_fingerprints: &BTreeMap<irokle_crate::TopicId, [u8; 32]>,
    responses: Vec<SyncMessage>,
) -> Result<BatchSummaryOutcome> {
    let mut responded_topics = BTreeSet::new();
    let mut failed_topics = BTreeSet::new();
    let mut sync_messages = Vec::new();
    for response in responses {
        match response {
            SyncMessage::Fingerprint(remote) if known_topics.contains(&remote.topic_id) => {
                responded_topics.insert(remote.topic_id);
                if local_fingerprints.get(&remote.topic_id) != Some(&remote.fingerprint) {
                    warn!(
                        %peer,
                        topic_id = %remote.topic_id,
                        "Skipping Irokle batch topic: peer returned mismatched fingerprint"
                    );
                    failed_topics.insert(remote.topic_id);
                }
            }
            SyncMessage::Summary(summary) if known_topics.contains(&summary.topic_id) => {
                responded_topics.insert(summary.topic_id);
                if let Some(event_type_id) = summary.event_type_id.as_deref()
                    && event_type_id != DocumentSyncEvent::TYPE_ID
                {
                    warn!(
                        %peer,
                        topic_id = %summary.topic_id,
                        event_type_id,
                        "Skipping Irokle batch topic: peer advertised unexpected event type"
                    );
                    failed_topics.insert(summary.topic_id);
                    continue;
                }
                let plan = match node.negotiate_sync(peer, &summary) {
                    Ok(plan) => plan,
                    Err(error) => {
                        warn!(
                            %peer,
                            topic_id = %summary.topic_id,
                            error = %error,
                            "Skipping Irokle batch topic: sync negotiation failed"
                        );
                        failed_topics.insert(summary.topic_id);
                        continue;
                    }
                };
                let wants_remote_data = !plan.need.is_empty() || !plan.actor_range_hints.is_empty();
                if !plan.send.is_empty() || wants_remote_data {
                    sync_messages.push(SyncMessage::Open(node.sync_open(plan.topic_id)));
                    if !plan.send.is_empty() {
                        sync_messages.push(SyncMessage::Data(SyncData {
                            topic_id: plan.topic_id,
                            ops: plan.send,
                        }));
                    }
                    if wants_remote_data {
                        sync_messages.push(SyncMessage::Request(SyncRequest {
                            topic_id: plan.topic_id,
                            known: plan.common,
                            wants: plan.need,
                            actor_range_hints: plan.actor_range_hints,
                        }));
                    }
                }
            }
            other => {
                return Err(NetError::Bootstrap(format!(
                    "unexpected Irokle batch sync response from {peer}: {other:?}"
                )));
            }
        }
    }
    Ok((responded_topics, failed_topics, sync_messages))
}

fn process_batch_data_responses(
    node: &irokle_crate::Irokle<irokle_crate::FjallStorage>,
    net: &irokle_crate::net::IrohNet<irokle_crate::FjallStorage>,
    peer: PeerId,
    known_topics: &BTreeSet<irokle_crate::TopicId>,
    mut failed_topics: BTreeSet<irokle_crate::TopicId>,
    responses: Vec<SyncMessage>,
) -> Result<(BTreeSet<irokle_crate::TopicId>, Vec<SyncMessage>)> {
    let mut followup = Vec::new();
    let mut acks = Vec::new();
    for response in responses {
        match response {
            SyncMessage::Ack(ack) if ack.peer_id == peer && known_topics.contains(&ack.topic_id) => {
                acks.push(ack);
            }
            SyncMessage::Summary(summary) if known_topics.contains(&summary.topic_id) => {}
            SyncMessage::Data(data) if known_topics.contains(&data.topic_id) => {
                let topic_id = data.topic_id;
                let ack = match node.receive_sync_data_from(peer, data) {
                    Ok(ack) => ack,
                    Err(error) => {
                        warn!(
                            %peer,
                            topic_id = %topic_id,
                            error = %error,
                            "Skipping Irokle batch topic: receiving sync data failed"
                        );
                        failed_topics.insert(topic_id);
                        continue;
                    }
                };
                net.schedule_topic_recheck(topic_id)?;
                followup.push(SyncMessage::Open(node.sync_open(topic_id)));
                followup.push(SyncMessage::Ack(ack));
            }
            other => {
                return Err(NetError::Bootstrap(format!(
                    "unexpected Irokle batch data response from {peer}: {other:?}"
                )));
            }
        }
    }
    for (ack, result) in acks.iter().zip(node.apply_sync_acks(&acks)) {
        if let Err(error) = result {
            warn!(
                %peer,
                topic_id = %ack.topic_id,
                error = %error,
                "Skipping Irokle batch topic: applying sync ack failed"
            );
            failed_topics.insert(ack.topic_id);
        }
    }
    Ok((failed_topics, followup))
}

#[allow(clippy::too_many_arguments)]
fn log_peer_batch_summary(
    peer: PeerId,
    topics: usize,
    r1_build: Duration,
    r1_io: Duration,
    r1_process: Duration,
    r2_io: Duration,
    r2_process: Duration,
    fu_io: Duration,
    r2_messages: usize,
    total: Duration,
) {
    info!(
        event = "pipeline.peer_batch.summary",
        peer = %peer,
        topics,
        r1_build_ms = duration_ms(r1_build),
        r1_io_ms = duration_ms(r1_io),
        r1_process_ms = duration_ms(r1_process),
        r2_io_ms = duration_ms(r2_io),
        r2_process_ms = duration_ms(r2_process),
        fu_io_ms = duration_ms(fu_io),
        r2_messages,
        total_ms = duration_ms(total),
        "Irokle peer batch sync round breakdown"
    );
}

fn finish_batch_sync(
    peer: PeerId,
    known_topics: &BTreeSet<irokle_crate::TopicId>,
    failed_topics: &BTreeSet<irokle_crate::TopicId>,
) -> Result<()> {
    if !failed_topics.is_empty() {
        if failed_topics.len() == known_topics.len() {
            return Err(NetError::Bootstrap(format!(
                "peer {peer}: all {} Irokle batch topics failed to sync",
                known_topics.len()
            )));
        }
        warn!(
            %peer,
            failed = failed_topics.len(),
            total = known_topics.len(),
            "Irokle batch sync completed with per-topic failures"
        );
    }
    Ok(())
}

fn sync_message_topic_id(message: &SyncMessage) -> irokle_crate::TopicId {
    match message {
        SyncMessage::Open(open) => open.topic_id,
        SyncMessage::Fingerprint(fingerprint) => fingerprint.topic_id,
        SyncMessage::Summary(summary) => summary.topic_id,
        SyncMessage::Request(request) => request.topic_id,
        SyncMessage::Data(data) => data.topic_id,
        SyncMessage::Ack(ack) => ack.topic_id,
    }
}

fn peer_id_to_endpoint_addr(peer_id: PeerId) -> Result<iroh::EndpointAddr> {
    let endpoint_id = iroh::EndpointId::from_bytes(peer_id.as_bytes())
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(iroh::EndpointAddr::from(endpoint_id))
}
