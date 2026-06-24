use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{AdminDocumentApplyStatus, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncEvent, DocumentSyncPublish, DocumentSyncReconcileResult,
    DocumentSyncTarget, IrokleEvent,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_STATE_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, IROKLE_APPLIED_OPS_KEYSPACE,
    METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataDocumentDeleteRecord, MetadataDocumentLifecycleRecord,
    MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord,
};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, document_sync_revision_key,
    document_sync_revision_write_entry, metadata_create_event_and_pending_projection_write_entries,
    metadata_document_lifecycle_key, metadata_document_lifecycle_revision_change,
    metadata_document_lifecycle_write_entry, metadata_graph_lifecycle_key,
    metadata_graph_lifecycle_write_entry, metadata_graph_prune_job_write_entry,
    metadata_registry_delete_entries, metadata_registry_write_entries,
    stale_admin_document_conflict_delete_entries, stale_subject_index_deletes,
    subject_index_writes,
};
use aruna_core::structs::{
    GroupAuthorizationDocument, MetadataRegistryRecord, RealmAuthorizationDocument, User,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{TxnId, UserId, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use byteview::ByteView;
use irokle_crate::Event as _;
use irokle_crate::Storage as _;
use irokle_crate::TopicControl;
use irokle_crate::history::HistoryOrder;
use irokle_crate::net::{decode_sync_message, encode_frame, encode_sync_message};
use irokle_crate::oplog::Oplog;
use irokle_crate::sync::{SyncData, SyncMessage, SyncRequest};
use irokle_crate::{EventEnvelope, PeerId, ReplicationPolicy, TopicGenesis, TopicPayload};
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
    lifecycle_revision: Option<DocumentSyncChange>,
}

#[derive(Clone)]
pub struct IrokleService {
    node: irokle_crate::Irokle<irokle_crate::FjallStorage>,
    net: Arc<irokle_crate::net::IrohNet<irokle_crate::FjallStorage>>,
    db: fjall::OptimisticTxDatabase,
    persist_policy: FjallPersistPolicy,
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
        Self::open_with_persist_policy(
            endpoint,
            storage,
            storage_path,
            peer_nodes,
            alpns,
            runtime,
            FjallPersistPolicy::default(),
        )
    }

    pub fn open_with_persist_policy(
        endpoint: iroh::Endpoint,
        storage: StorageHandle,
        storage_path: impl AsRef<Path>,
        peer_nodes: &[NodeId],
        alpns: Vec<Vec<u8>>,
        runtime: irokle_crate::net::IrohRuntimeConfig,
        persist_policy: FjallPersistPolicy,
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
            .with_fjall_database_and_persist_mode(db.clone(), persist_policy.as_fjall())
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
            persist_policy,
            storage,
            default_peers: Arc::new(RwLock::new(default_peers)),
            storage_path,
        })
    }

    pub fn node(&self) -> irokle_crate::Irokle<irokle_crate::FjallStorage> {
        self.node.clone()
    }

    fn local_node_id(&self) -> Result<NodeId> {
        NodeId::from_bytes(self.node.peer_id().as_bytes())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
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
                metadata_graph_tombstones: result.metadata_graph_tombstones,
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
                metadata_graph_tombstones: result.metadata_graph_tombstones,
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
                    metadata_graph_tombstones: result.metadata_graph_tombstones,
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
            DocumentSyncEvent::AdminOperation { target, event } => {
                DocumentSyncPublish::AdminOperation { target, event }
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
                DocumentSyncPublish::AdminOperation { target, event } => {
                    DocumentSyncEvent::AdminOperation { target, event }
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
            .persist(self.persist_policy.as_fjall())
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
        if successes != attempted {
            let detail = first_error.unwrap_or_else(|| "unknown sync error".to_string());
            return Err(NetError::Bootstrap(format!(
                "{context}: only {successes}/{attempted} peers synced; {detail}"
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
        Self::fan_out_peer_syncs(peers, format!("Irokle topic {topic_id}"), move |peer| {
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
        Self::fan_out_peer_syncs(
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
        if remote_summary_is_empty(&summary) {
            return Ok(());
        }
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
        let mut metadata_graph_tombstones = Vec::new();
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
                match event {
                    event @ DocumentSyncEvent::Upsert {
                        target: DocumentSyncTarget::MetadataCreateEvent { .. },
                        ..
                    } => {
                        let pending = self.pending_metadata_create_apply(event)?;
                        pending_metadata_creates.push(pending);
                        deferred_creates = true;
                    }
                    DocumentSyncEvent::Upsert {
                        target: DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
                        bytes,
                        ..
                    } => {
                        let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
                        let lifecycle: MetadataDocumentLifecycleRecord =
                            postcard::from_bytes(&bytes)
                                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                        if lifecycle.document_id() != document_id {
                            return Err(NetError::Bootstrap(format!(
                                "replicated metadata document lifecycle target {document_id} does not match payload document {}",
                                lifecycle.document_id()
                            )));
                        }
                        match lifecycle {
                            MetadataDocumentLifecycleRecord::Upsert { event: record } => {
                                let record = *record;
                                let bytes = postcard::to_allocvec(&record)
                                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                                let lifecycle = MetadataDocumentLifecycleRecord::Upsert {
                                    event: Box::new(record.clone()),
                                };
                                pending_metadata_creates.push(PendingMetadataCreateApply {
                                    target,
                                    lifecycle_revision: Some(
                                        metadata_document_lifecycle_revision_change(
                                            &lifecycle,
                                            self.local_node_id()?,
                                        ),
                                    ),
                                    record,
                                    bytes,
                                });
                                deferred_creates = true;
                            }
                            MetadataDocumentLifecycleRecord::Delete { event } => {
                                let tombstone = event.tombstone.clone();
                                let accepted = self
                                    .apply_metadata_document_lifecycle(
                                        MetadataDocumentLifecycleRecord::Delete { event },
                                    )
                                    .await?;
                                if accepted && tombstone.is_deleted() {
                                    metadata_graph_tombstones.push(tombstone);
                                }
                                if accepted {
                                    applied_targets.push(target);
                                }
                            }
                        }
                    }
                    DocumentSyncEvent::Upsert {
                        target: DocumentSyncTarget::MetadataGraphLifecycle { graph_iri },
                        bytes,
                        ..
                    } => {
                        let target = DocumentSyncTarget::MetadataGraphLifecycle {
                            graph_iri: graph_iri.clone(),
                        };
                        let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&bytes)
                            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                        if record.graph_iri != graph_iri {
                            return Err(NetError::Bootstrap(format!(
                                "replicated metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                                record.graph_iri
                            )));
                        }
                        self.apply_metadata_graph_lifecycle(record.clone(), bytes)
                            .await?;
                        if record.is_deleted() {
                            metadata_graph_tombstones.push(record);
                        }
                        applied_targets.push(target);
                    }
                    event => {
                        let target = event.target().clone();
                        self.apply_document_event(event).await?;
                        applied_targets.push(target);
                    }
                }
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
            metadata_graph_tombstones,
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
            lifecycle_revision: None,
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
        let mut accepted = Vec::with_capacity(pending.len());
        for apply in pending {
            if self.metadata_create_fenced(&apply.record).await? {
                continue;
            }
            let target = DocumentSyncTarget::MetadataCreateEvent {
                document_id: apply.record.record.document_id,
                event_id: apply.record.event_id,
            };
            if let Some(revision) = &apply.lifecycle_revision {
                if incoming_metadata_document_lifecycle_stale_or_equal(
                    &self.storage,
                    &apply.target,
                    revision,
                )
                .await?
                {
                    continue;
                }
                writes.push(
                    document_sync_revision_write_entry(&apply.target, revision)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                );
            }
            writes.push((
                target.storage_keyspace().to_string(),
                target.storage_key(),
                ByteView::from(apply.bytes.clone()),
            ));
            accepted.push(apply);
        }
        writes.extend(cursor_writes);
        if !writes.is_empty() {
            self.storage_batch_write(writes).await?;
        }
        for apply in accepted {
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
            DocumentSyncEvent::AdminOperation { target, event } => {
                apply_admin_document_operation_to_storage(&self.storage, target, *event).await
            }
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
            if self.metadata_create_fenced(&record).await? {
                return Ok(());
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
        if let DocumentSyncTarget::MetadataDocumentLifecycle { document_id } = target {
            let record: MetadataDocumentLifecycleRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.document_id() != document_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata document lifecycle target {document_id} does not match payload document {}",
                    record.document_id()
                )));
            }
            return self
                .apply_metadata_document_lifecycle(record)
                .await
                .map(|_| ());
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
        apply_metadata_registry_upsert_to_storage(&self.storage, record, primary_bytes).await
    }

    async fn apply_metadata_document_lifecycle(
        &self,
        record: MetadataDocumentLifecycleRecord,
    ) -> Result<bool> {
        apply_metadata_document_lifecycle_to_storage(&self.storage, &record, self.local_node_id()?)
            .await
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

    async fn metadata_create_fenced(&self, event: &MetadataCreateEventRecord) -> Result<bool> {
        metadata_create_fenced_in_storage(&self.storage, event).await
    }

    async fn apply_delete(&self, target: DocumentSyncTarget) -> Result<()> {
        if let DocumentSyncTarget::MetadataGraphLifecycle { .. } = target {
            return Ok(());
        }
        if let DocumentSyncTarget::MetadataDocumentLifecycle { .. } = target {
            return Ok(());
        }
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            return apply_metadata_registry_delete_to_storage(&self.storage, group_id, document_id)
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
        storage_read_from(&self.storage, key_space, key).await
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
        storage_batch_write_to(&self.storage, writes).await
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
        storage_batch_delete_to(&self.storage, deletes).await
    }
}

async fn apply_metadata_registry_upsert_to_storage(
    storage: &StorageHandle,
    record: MetadataRegistryRecord,
    primary_bytes: Vec<u8>,
) -> Result<()> {
    if metadata_graph_deleted_in_storage(storage, &record.graph_iri).await? {
        return storage_batch_delete_to(
            storage,
            metadata_registry_delete_entries(record.group_id, record.document_id),
        )
        .await;
    }

    let target = DocumentSyncTarget::MetadataRegistry {
        group_id: record.group_id,
        document_id: record.document_id,
    };
    let existing = storage_read_from(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
    )
    .await?
    .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if existing
        .as_ref()
        .is_some_and(|existing| incoming_metadata_registry_stale_or_equal(existing, &record))
    {
        return Ok(());
    }

    let mut entries = metadata_registry_write_entries(&record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if let Some((_, _, value)) = entries.first_mut() {
        *value = primary_bytes.into();
    }
    storage_batch_write_to(storage, entries).await
}

async fn apply_metadata_document_lifecycle_to_storage(
    storage: &StorageHandle,
    record: &MetadataDocumentLifecycleRecord,
    delete_actor: NodeId,
) -> Result<bool> {
    let Some(writes) =
        metadata_document_lifecycle_write_entries_if_current(storage, record, delete_actor).await?
    else {
        return Ok(false);
    };
    storage_batch_write_to(storage, writes).await?;
    if let MetadataDocumentLifecycleRecord::Delete { event } = record
        && event.tombstone.is_deleted()
    {
        apply_metadata_registry_delete_to_storage(
            storage,
            event.tombstone.group_id,
            event.tombstone.document_id,
        )
        .await?;
    }
    Ok(true)
}

async fn apply_metadata_registry_delete_to_storage(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
) -> Result<()> {
    let Some(delete) = metadata_document_delete_in_storage(storage, document_id).await? else {
        return Ok(());
    };
    if delete.tombstone.is_deleted()
        && delete.tombstone.group_id == group_id
        && delete.tombstone.document_id == document_id
    {
        storage_batch_delete_to(
            storage,
            metadata_registry_delete_entries(group_id, document_id),
        )
        .await?;
    }
    Ok(())
}

async fn apply_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    match (&document_target, &event.target) {
        (DocumentSyncTarget::User { .. }, AdminDocumentTarget::User { .. }) => {
            apply_user_admin_document_operation_to_storage(storage, document_target, event).await
        }
        (DocumentSyncTarget::GroupAuthorization { .. }, AdminDocumentTarget::Group { .. }) => {
            apply_group_authorization_admin_document_operation_to_storage(
                storage,
                document_target,
                event,
            )
            .await
        }
        (DocumentSyncTarget::RealmAuthorization { .. }, AdminDocumentTarget::Realm { .. }) => {
            apply_realm_authorization_admin_document_operation_to_storage(
                storage,
                document_target,
                event,
            )
            .await
        }
        _ => Err(NetError::Bootstrap(
            "admin document operation target does not match document sync target".to_string(),
        )),
    }
}

async fn apply_user_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::User { user_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "admin document operation sync only supports user targets".to_string(),
        ));
    };
    let AdminDocumentTarget::User {
        user_id: event_user_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a user".to_string(),
        ));
    };
    if event_user_id != user_id {
        return Err(NetError::Bootstrap(format!(
            "replicated user admin operation target {user_id} does not match payload user id {event_user_id}"
        )));
    }
    if !matches!(
        event.op,
        AdminDocumentOperation::UserNameSet { .. }
            | AdminDocumentOperation::UserSubjectIdAdded { .. }
            | AdminDocumentOperation::UserSubjectIdRemoved { .. }
            | AdminDocumentOperation::UserAttributeSet { .. }
            | AdminDocumentOperation::UserAttributeRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "admin document operation sync only supports user name, subject, and attribute updates"
                .to_string(),
        ));
    }

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| postcard::from_bytes::<AdminDocumentReducerState>(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if apply_status != AdminDocumentApplyStatus::Applied {
        return Ok(());
    }

    let previous_user = storage_read_from(
        storage,
        document_target.storage_keyspace().to_string(),
        document_target.storage_key(),
    )
    .await?
    .map(|bytes| User::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let user = materialize_user_admin_document_operation(
        user_id,
        previous_user.as_ref(),
        &reducer_state,
        &event,
    );

    let mut writes = vec![
        (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            user.to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        admin_document_reducer_state_write_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(subject_index_writes(&user));
    writes.extend(
        admin_document_conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let mut deletes =
        stale_admin_document_conflict_delete_entries(previous_state.as_ref(), Some(&reducer_state));
    deletes.extend(stale_subject_index_deletes(
        previous_user.as_ref(),
        Some(&user),
    ));
    storage_batch_delete_and_write_transactionally(storage, deletes, writes).await
}

async fn apply_group_authorization_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::GroupAuthorization { group_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "group admin operation sync only supports group authorization targets".to_string(),
        ));
    };
    let AdminDocumentTarget::Group {
        group_id: event_group_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a group".to_string(),
        ));
    };
    if event_group_id != group_id {
        return Err(NetError::Bootstrap(format!(
            "replicated group admin operation target {group_id} does not match payload group id {event_group_id}"
        )));
    }
    if !matches!(
        event.op,
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { .. }
            | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "group admin operation sync only supports role user assignment updates".to_string(),
        ));
    }

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| postcard::from_bytes::<AdminDocumentReducerState>(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if apply_status != AdminDocumentApplyStatus::Applied {
        return Ok(());
    }

    let previous_auth_doc = storage_read_from(
        storage,
        document_target.storage_keyspace().to_string(),
        document_target.storage_key(),
    )
    .await?
    .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut auth_doc = previous_auth_doc.unwrap_or_else(|| GroupAuthorizationDocument {
        group_id,
        roles: Default::default(),
    });
    materialize_group_authorization_admin_document_operation(&mut auth_doc, &reducer_state, &event);

    let mut writes = vec![
        (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            auth_doc
                .to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        admin_document_reducer_state_write_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(
        admin_document_conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let stale_conflict_deletes =
        stale_admin_document_conflict_delete_entries(previous_state.as_ref(), Some(&reducer_state));
    storage_batch_delete_and_write_transactionally(storage, stale_conflict_deletes, writes).await
}

async fn apply_realm_authorization_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::RealmAuthorization { realm_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "realm admin operation sync only supports realm authorization targets".to_string(),
        ));
    };
    let AdminDocumentTarget::Realm {
        realm_id: event_realm_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a realm".to_string(),
        ));
    };
    if event_realm_id != realm_id {
        return Err(NetError::Bootstrap(format!(
            "replicated realm admin operation target {realm_id} does not match payload realm id {event_realm_id}"
        )));
    }
    if !matches!(
        event.op,
        AdminDocumentOperation::RealmRoleUserAssignmentAdded { .. }
            | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm admin operation sync only supports role user assignment updates".to_string(),
        ));
    }

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| postcard::from_bytes::<AdminDocumentReducerState>(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if apply_status != AdminDocumentApplyStatus::Applied {
        return Ok(());
    }

    let previous_auth_doc = storage_read_from(
        storage,
        document_target.storage_keyspace().to_string(),
        document_target.storage_key(),
    )
    .await?
    .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut auth_doc = previous_auth_doc.unwrap_or_else(|| RealmAuthorizationDocument {
        realm_id,
        roles: Default::default(),
        operation_restrictions: Default::default(),
    });
    materialize_realm_authorization_admin_document_operation(&mut auth_doc, &reducer_state, &event);

    let mut writes = vec![
        (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            auth_doc
                .to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        admin_document_reducer_state_write_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(
        admin_document_conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let stale_conflict_deletes =
        stale_admin_document_conflict_delete_entries(previous_state.as_ref(), Some(&reducer_state));
    storage_batch_delete_and_write_transactionally(storage, stale_conflict_deletes, writes).await
}

fn materialize_group_authorization_admin_document_operation(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) {
    let (role_id, user_id) = match &event.op {
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id } => {
            (role_id, user_id)
        }
        _ => return,
    };
    let path = group_role_user_assignment_path(role_id, user_id);
    if reducer_state.conflicts.contains_key(&path) {
        return;
    }
    let Some(role) = auth_doc.roles.get_mut(role_id) else {
        return;
    };
    let assigned = reducer_state
        .user_subject_ids
        .get(&path)
        .and_then(|version| version.value.as_deref())
        .and_then(|value| UserId::from_string(value).ok())
        .is_some_and(|materialized_user_id| materialized_user_id == *user_id);
    if assigned {
        role.assigned_users.insert(*user_id);
    } else {
        role.assigned_users.remove(user_id);
    }
}

fn materialize_realm_authorization_admin_document_operation(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) {
    let (role_id, user_id) = match &event.op {
        AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id } => {
            (role_id, user_id)
        }
        _ => return,
    };
    let path = realm_role_user_assignment_path(role_id, user_id);
    if reducer_state.conflicts.contains_key(&path) {
        return;
    }
    let Some(role) = auth_doc.roles.get_mut(role_id) else {
        return;
    };
    let assigned = reducer_state
        .user_subject_ids
        .get(&path)
        .and_then(|version| version.value.as_deref())
        .and_then(|value| UserId::from_string(value).ok())
        .is_some_and(|materialized_user_id| materialized_user_id == *user_id);
    if assigned {
        role.assigned_users.insert(*user_id);
    } else {
        role.assigned_users.remove(user_id);
    }
}

fn group_role_user_assignment_path(
    role_id: &aruna_core::types::RoleId,
    user_id: &UserId,
) -> String {
    format!("group.roles.{role_id}.assigned_users.{user_id}")
}

fn realm_role_user_assignment_path(
    role_id: &aruna_core::types::RoleId,
    user_id: &UserId,
) -> String {
    format!("realm.roles.{role_id}.assigned_users.{user_id}")
}

fn materialize_user_admin_document_operation(
    user_id: UserId,
    previous_user: Option<&User>,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) -> User {
    let mut user = previous_user.cloned().unwrap_or_else(|| User {
        user_id,
        name: String::new(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    });

    match &event.op {
        AdminDocumentOperation::UserNameSet { .. } => {
            if !reducer_state.conflicts.contains_key("user.name")
                && let Some(name) = reducer_state.materialized_user_name()
            {
                user.name = name;
            }
        }
        AdminDocumentOperation::UserSubjectIdAdded { subject_id }
        | AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => {
            let path = format!("user.subject_ids.{subject_id}");
            let materialized_subject_id = if reducer_state.conflicts.contains_key(&path) {
                None
            } else {
                reducer_state
                    .user_subject_ids
                    .get(subject_id)
                    .and_then(|version| version.value.clone())
            };

            user.subject_ids.retain(|candidate| candidate != subject_id);
            if let Some(materialized_subject_id) = materialized_subject_id
                && !user.subject_ids.contains(&materialized_subject_id)
            {
                user.subject_ids.push(materialized_subject_id);
            }
        }
        AdminDocumentOperation::UserAttributeSet { key, .. }
        | AdminDocumentOperation::UserAttributeRemoved { key } => {
            let path = format!("user.attributes.{key}");
            if !reducer_state.conflicts.contains_key(&path) {
                match reducer_state
                    .user_attributes
                    .get(key)
                    .and_then(|version| version.value.clone())
                {
                    Some(value) => {
                        user.attributes.insert(key.clone(), value);
                    }
                    None => {
                        user.attributes.remove(key);
                    }
                }
            }
        }
        _ => {}
    }

    user
}

async fn metadata_document_lifecycle_write_entries_if_current(
    storage: &StorageHandle,
    record: &MetadataDocumentLifecycleRecord,
    delete_actor: NodeId,
) -> Result<Option<Vec<(String, ByteView, Value)>>> {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: record.document_id(),
    };
    let revision = metadata_document_lifecycle_revision_change(record, delete_actor);
    if incoming_metadata_document_lifecycle_stale_or_equal(storage, &target, &revision).await? {
        return Ok(None);
    }
    if let MetadataDocumentLifecycleRecord::Upsert { event } = record
        && metadata_create_fenced_in_storage(storage, event).await?
    {
        return Ok(None);
    }

    let mut entries = match record {
        MetadataDocumentLifecycleRecord::Upsert { event } => {
            metadata_create_event_and_pending_projection_write_entries(event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
        }
        MetadataDocumentLifecycleRecord::Delete { event } => {
            metadata_document_delete_write_entries(event)?
        }
    };
    entries.push(
        document_sync_revision_write_entry(&target, &revision)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );
    Ok(Some(entries))
}

async fn incoming_metadata_document_lifecycle_stale_or_equal(
    storage: &StorageHandle,
    target: &DocumentSyncTarget,
    incoming: &DocumentSyncChange,
) -> Result<bool> {
    let value = storage_read_from(
        storage,
        DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
        document_sync_revision_key(target),
    )
    .await?;
    let Some(value) = value else {
        return Ok(false);
    };
    let local: DocumentSyncChange =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(incoming.current <= local.current)
}

fn incoming_metadata_registry_stale_or_equal(
    existing: &MetadataRegistryRecord,
    incoming: &MetadataRegistryRecord,
) -> bool {
    metadata_registry_freshness(incoming) <= metadata_registry_freshness(existing)
}

fn metadata_registry_freshness(record: &MetadataRegistryRecord) -> (u64, Ulid) {
    (record.updated_at_ms, record.last_event_id)
}

async fn metadata_graph_deleted_in_storage(
    storage: &StorageHandle,
    graph_iri: &str,
) -> Result<bool> {
    let value = storage_read_from(
        storage,
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

async fn metadata_document_delete_in_storage(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<Option<MetadataDocumentDeleteRecord>> {
    let value = storage_read_from(
        storage,
        METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
        metadata_document_lifecycle_key(document_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(None);
    };
    let record: MetadataDocumentLifecycleRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    match record {
        MetadataDocumentLifecycleRecord::Delete { event } => Ok(Some(event)),
        MetadataDocumentLifecycleRecord::Upsert { .. } => Ok(None),
    }
}

async fn metadata_create_fenced_in_storage(
    storage: &StorageHandle,
    event: &MetadataCreateEventRecord,
) -> Result<bool> {
    if let Some(delete) =
        metadata_document_delete_in_storage(storage, event.record.document_id).await?
    {
        return Ok(event.event_id <= delete.deleted_after_event_id);
    }
    metadata_graph_deleted_in_storage(storage, &event.record.graph_iri).await
}

async fn storage_read_from(
    storage: &StorageHandle,
    key_space: String,
    key: ByteView,
) -> Result<Option<Value>> {
    match storage
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

async fn storage_batch_write_to(
    storage: &StorageHandle,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    match storage
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

async fn storage_batch_delete_and_write_transactionally(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(NetError::Dht(error.to_string()));
        }
        other => {
            return Err(NetError::Dht(format!(
                "unexpected storage event while starting irokle apply transaction: {other:?}"
            )));
        }
    };

    if let Err(error) =
        storage_batch_delete_and_write_in_transaction(storage, txn_id, deletes, writes).await
    {
        let _ = storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Err(error);
    }

    Ok(())
}

async fn storage_batch_delete_and_write_in_transaction(
    storage: &StorageHandle,
    txn_id: TxnId,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    if !deletes.is_empty() {
        match storage
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(NetError::Dht(error.to_string()));
            }
            other => {
                return Err(NetError::Dht(format!(
                    "unexpected storage event while applying irokle transactional batch delete: {other:?}"
                )));
            }
        }
    }

    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(NetError::Dht(error.to_string()));
        }
        other => {
            return Err(NetError::Dht(format!(
                "unexpected storage event while applying irokle transactional batch write: {other:?}"
            )));
        }
    }

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while committing irokle apply transaction: {other:?}"
        ))),
    }
}

async fn storage_batch_delete_to(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
) -> Result<()> {
    match storage
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

fn metadata_document_delete_write_entries(
    record: &MetadataDocumentDeleteRecord,
) -> Result<Vec<(String, ByteView, Value)>> {
    let lifecycle = MetadataDocumentLifecycleRecord::Delete {
        event: record.clone(),
    };
    let mut entries = vec![
        metadata_document_lifecycle_write_entry(&lifecycle)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        metadata_graph_lifecycle_write_entry(&record.tombstone)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    if record.tombstone.is_deleted() {
        let job = MetadataGraphPruneJobRecord::new(
            record.tombstone.graph_iri.clone(),
            unix_timestamp_millis(),
        );
        entries.push(
            metadata_graph_prune_job_write_entry(&job)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
    }
    Ok(entries)
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
            SyncMessage::Ack(ack)
                if ack.peer_id == peer && known_topics.contains(&ack.topic_id) =>
            {
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
        warn!(
            %peer,
            failed = failed_topics.len(),
            total = known_topics.len(),
            "Irokle batch sync failed for one or more topics"
        );
        return Err(NetError::Bootstrap(format!(
            "peer {peer}: {}/{} Irokle batch topics failed to sync",
            failed_topics.len(),
            known_topics.len()
        )));
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

fn remote_summary_is_empty(summary: &irokle_crate::sync::SyncSummary) -> bool {
    summary.event_type_id.is_none() && summary.heads.is_empty()
}

fn peer_id_to_endpoint_addr(peer_id: PeerId) -> Result<iroh::EndpointAddr> {
    let endpoint_id = iroh::EndpointId::from_bytes(peer_id.as_bytes())
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(iroh::EndpointAddr::from(endpoint_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::alpn::Alpn;
    use aruna_core::document::DocumentSyncChangeKind;
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
        DOCUMENT_SYNC_REVISION_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
        METADATA_DOCUMENT_LIFECYCLE_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE,
        METADATA_GRAPH_PRUNE_JOB_KEYSPACE, METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE,
        USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
    };
    use aruna_core::metadata::MetadataCreateEventPayload;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
        metadata_document_key, metadata_event_log_key, metadata_registry_key, subject_index_key,
        subject_index_value,
    };
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, Permission, RealmAuthorizationDocument, RealmId, Role,
    };
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::{env, process::Command};
    use tempfile::TempDir;

    const IROKLE_RESTART_CHILD_PATH_ENV: &str = "ARUNA_NET_IROKLE_RESTART_CHILD_PATH";
    const IROKLE_RESTART_CHILD_TEST: &str =
        "irokle::tests::buffered_irokle_publish_restart_child_process";

    fn peer(seed: u8) -> PeerId {
        node_id_to_peer_id(&iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn topic(seed: u8) -> irokle_crate::TopicId {
        DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([seed; 32]),
        }
        .irokle_topic_id()
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn test_storage() -> (TempDir, StorageHandle) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        (dir, storage)
    }

    fn storage_at(path: &Path) -> StorageHandle {
        aruna_storage::FjallStorage::open(path.to_str().expect("utf-8 storage path"))
            .expect("storage opens")
    }

    fn restart_target() -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([99; 32]),
        }
    }

    fn restart_event_id() -> Ulid {
        Ulid::from_parts(1_727_000_000_000, 42)
    }

    fn restart_payload() -> Vec<u8> {
        b"buffered restart contract payload".to_vec()
    }

    async fn restart_endpoint() -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(iroh::SecretKey::from_bytes(&[91; 32]))
            .relay_mode(iroh::RelayMode::Disabled)
            .alpns(vec![Alpn::Irokle.as_bytes().to_vec()])
            .bind_addr(
                "127.0.0.1:0"
                    .parse::<std::net::SocketAddr>()
                    .expect("valid bind address"),
            )
            .expect("endpoint bind address configures")
            .bind()
            .await
            .expect("endpoint binds")
    }

    async fn open_restart_service(root: &Path, storage_name: &str) -> IrokleService {
        IrokleService::open_with_persist_policy(
            restart_endpoint().await,
            storage_at(&root.join(storage_name)),
            root.join("irokle"),
            &[],
            vec![Alpn::Irokle.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("Irokle service opens")
    }

    fn run_irokle_restart_child(root: &Path) {
        let status = Command::new(env::current_exe().expect("test binary path"))
            .arg(IROKLE_RESTART_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env(IROKLE_RESTART_CHILD_PATH_ENV, root)
            .status()
            .expect("restart child process should run");

        assert!(status.success(), "restart child process failed: {status}");
    }

    fn registry_record(
        group_id: Ulid,
        document_id: Ulid,
        document_path: &str,
        updated_at_ms: u64,
        last_event_id: Ulid,
    ) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([42; 32]);
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                document_path,
                document_id,
            ),
            holder_node_ids: vec![node(1)],
            created_at_ms: 1,
            updated_at_ms,
            last_event_id,
        }
    }

    async fn write_registry_record(storage: &StorageHandle, record: &MetadataRegistryRecord) {
        let event = storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: metadata_registry_write_entries(record).expect("registry entries build"),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    async fn read_storage_value(
        storage: &StorageHandle,
        key_space: &str,
        key: ByteView,
    ) -> Option<Value> {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected storage read event: {other:?}"),
        }
    }

    async fn read_registry_record(
        storage: &StorageHandle,
        key_space: &str,
        key: ByteView,
    ) -> MetadataRegistryRecord {
        let value = read_storage_value(storage, key_space, key)
            .await
            .expect("registry record exists");
        postcard::from_bytes(&value).expect("registry record decodes")
    }

    fn metadata_create_event(
        group_id: Ulid,
        document_id: Ulid,
        updated_at_ms: u64,
        event_id: Ulid,
        actor_seed: u8,
    ) -> MetadataCreateEventRecord {
        let realm_id = RealmId::from_bytes([42; 32]);
        MetadataCreateEventRecord {
            event_id,
            record: registry_record(
                group_id,
                document_id,
                "datasets/lifecycle",
                updated_at_ms,
                event_id,
            ),
            user_id: UserId::local(Ulid::from_parts(90, 1), realm_id),
            node_id: node(actor_seed),
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Lifecycle".to_string(),
                description: "Lifecycle event".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
            occurred_at_ms: updated_at_ms,
        }
    }

    fn metadata_delete_lifecycle(
        group_id: Ulid,
        document_id: Ulid,
        updated_at_ms: u64,
        event_id: Ulid,
        deleted_after_event_id: Ulid,
    ) -> MetadataDocumentLifecycleRecord {
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        MetadataDocumentLifecycleRecord::Delete {
            event: MetadataDocumentDeleteRecord {
                event_id,
                tombstone: MetadataGraphLifecycleRecord::deleted(
                    graph_iri,
                    RealmId::from_bytes([42; 32]),
                    group_id,
                    document_id,
                    updated_at_ms,
                ),
                deleted_after_event_id,
            },
        }
    }

    #[tokio::test]
    async fn user_admin_operation_applies_reducer_state_and_materializes_user() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([7; 32]);
        let user_id = UserId::local(Ulid::from_parts(1, 1), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id,
            realm_id,
        };
        let original = User {
            user_id,
            name: "Alice".to_string(),
            subject_ids: vec!["subject-1".to_string()],
            alias_user_ids: Default::default(),
            attributes: HashMap::from([("department".to_string(), "physics".to_string())]),
        };
        storage_batch_write_to(
            &storage,
            vec![(
                USER_KEYSPACE.to_string(),
                user_id.to_bytes().into(),
                original.to_bytes(&actor).expect("user serializes").into(),
            )],
        )
        .await
        .expect("original user writes");

        let event = AdminDocumentEvent {
            event_id: Ulid::from_parts(2, 1),
            target: AdminDocumentTarget::User { user_id },
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor,
            op: AdminDocumentOperation::UserNameSet {
                name: "Alice Updated".to_string(),
            },
        };
        apply_user_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::User { user_id },
            event,
        )
        .await
        .expect("admin operation applies");

        let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
            .await
            .expect("user exists");
        let user = User::from_bytes(&stored_user).expect("user decodes");
        assert_eq!(user.name, "Alice Updated");
        assert_eq!(user.attributes["department"], "physics");
        let reducer_state = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&AdminDocumentTarget::User { user_id }),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&reducer_state).expect("reducer state decodes");
        assert_eq!(
            reducer_state.materialized_user_name().as_deref(),
            Some("Alice Updated")
        );
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("subject-1")
            )
            .await,
            Some(subject_index_value(user_id))
        );
    }

    #[tokio::test]
    async fn user_subject_add_admin_operation_creates_user_and_subject_index() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([7; 32]);
        let user_id = UserId::local(Ulid::from_parts(10, 1), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id,
            realm_id,
        };
        let event = AdminDocumentEvent {
            event_id: Ulid::from_parts(11, 1),
            target: AdminDocumentTarget::User { user_id },
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor,
            op: AdminDocumentOperation::UserSubjectIdAdded {
                subject_id: "subject-created".to_string(),
            },
        };

        apply_user_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::User { user_id },
            event,
        )
        .await
        .expect("subject add applies");

        let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
            .await
            .expect("user exists");
        let user = User::from_bytes(&stored_user).expect("user decodes");
        assert_eq!(user.subject_ids, vec!["subject-created".to_string()]);
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("subject-created")
            )
            .await,
            Some(subject_index_value(user_id))
        );
    }

    #[tokio::test]
    async fn user_subject_remove_admin_operation_deletes_stale_subject_index() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([7; 32]);
        let user_id = UserId::local(Ulid::from_parts(12, 1), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id,
            realm_id,
        };
        let original = User {
            user_id,
            name: "Alice".to_string(),
            subject_ids: vec!["subject-removed".to_string()],
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        storage_batch_write_to(
            &storage,
            vec![
                (
                    USER_KEYSPACE.to_string(),
                    user_id.to_bytes().into(),
                    original.to_bytes(&actor).expect("user serializes").into(),
                ),
                (
                    USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                    subject_index_key("subject-removed"),
                    subject_index_value(user_id),
                ),
            ],
        )
        .await
        .expect("original user and index write");

        let event = AdminDocumentEvent {
            event_id: Ulid::from_parts(13, 1),
            target: AdminDocumentTarget::User { user_id },
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor,
            op: AdminDocumentOperation::UserSubjectIdRemoved {
                subject_id: "subject-removed".to_string(),
            },
        };
        apply_user_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::User { user_id },
            event,
        )
        .await
        .expect("subject remove applies");

        let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
            .await
            .expect("user exists");
        let user = User::from_bytes(&stored_user).expect("user decodes");
        assert!(user.subject_ids.is_empty());
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("subject-removed")
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn group_assignment_admin_operation_materializes_auth_doc_and_reducer_state() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([9; 32]);
        let group_id = Ulid::from_parts(1, 1);
        let role_id = Ulid::from_parts(2, 2);
        let assigned_user_id = UserId::local(Ulid::from_parts(3, 3), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id: UserId::local(Ulid::from_parts(4, 4), realm_id),
            realm_id,
        };
        let auth_doc = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: "member".to_string(),
                    permissions: HashMap::from([("/datasets".to_string(), Permission::READ)]),
                    assigned_users: HashSet::new(),
                },
            )]),
        };
        storage_batch_write_to(
            &storage,
            vec![(
                AUTH_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                auth_doc
                    .to_bytes(&actor)
                    .expect("auth doc serializes")
                    .into(),
            )],
        )
        .await
        .expect("auth doc writes");

        let add_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(5, 5),
            target: AdminDocumentTarget::Group { group_id },
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                role_id,
                user_id: assigned_user_id,
            },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            add_event,
        )
        .await
        .expect("add assignment applies");

        let stored_auth_doc =
            read_storage_value(&storage, AUTH_KEYSPACE, group_id.to_bytes().into())
                .await
                .expect("auth doc exists");
        let stored_auth_doc =
            GroupAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
        assert!(
            stored_auth_doc.roles[&role_id]
                .assigned_users
                .contains(&assigned_user_id)
        );
        let target = AdminDocumentTarget::Group { group_id };
        let reducer_state = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&reducer_state).expect("reducer state decodes");
        assert!(reducer_state.conflicts.is_empty());
        let assignment_path = group_role_user_assignment_path(&role_id, &assigned_user_id);
        assert_eq!(
            reducer_state
                .user_subject_ids
                .get(&assignment_path)
                .and_then(|version| version.value.clone()),
            Some(assigned_user_id.to_string())
        );

        let remove_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(6, 6),
            target,
            origin_node_id: actor.node_id,
            origin_seq: 2,
            observed: AdminDocumentClock::default(),
            actor,
            op: AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
                role_id,
                user_id: assigned_user_id,
            },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            remove_event,
        )
        .await
        .expect("remove assignment applies");

        let stored_auth_doc =
            read_storage_value(&storage, AUTH_KEYSPACE, group_id.to_bytes().into())
                .await
                .expect("auth doc exists");
        let stored_auth_doc =
            GroupAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
        assert!(
            !stored_auth_doc.roles[&role_id]
                .assigned_users
                .contains(&assigned_user_id)
        );
        let reducer_state = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&AdminDocumentTarget::Group { group_id }),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&reducer_state).expect("reducer state decodes");
        assert_eq!(
            reducer_state
                .user_subject_ids
                .get(&assignment_path)
                .and_then(|version| version.value.clone()),
            None
        );
    }

    #[tokio::test]
    async fn group_assignment_conflict_resolution_deletes_stale_conflict_and_materializes() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([11; 32]);
        let group_id = Ulid::from_parts(21, 1);
        let role_id = Ulid::from_parts(22, 2);
        let assigned_user_id = UserId::local(Ulid::from_parts(23, 3), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id: UserId::local(Ulid::from_parts(24, 4), realm_id),
            realm_id,
        };
        let auth_doc = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: "member".to_string(),
                    permissions: HashMap::from([("/datasets".to_string(), Permission::READ)]),
                    assigned_users: HashSet::new(),
                },
            )]),
        };
        storage_batch_write_to(
            &storage,
            vec![(
                AUTH_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                auth_doc
                    .to_bytes(&actor)
                    .expect("auth doc serializes")
                    .into(),
            )],
        )
        .await
        .expect("auth doc writes");

        let target = AdminDocumentTarget::Group { group_id };
        let assignment_path = group_role_user_assignment_path(&role_id, &assigned_user_id);
        let conflict_key = admin_document_reducer_conflict_key(&target, &assignment_path);
        let add_origin = node(9);
        let remove_origin = node(10);
        let add_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(25, 5),
            target: target.clone(),
            origin_node_id: add_origin,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                role_id,
                user_id: assigned_user_id,
            },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            add_event,
        )
        .await
        .expect("add assignment applies");
        let conflicting_remove_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(26, 6),
            target: target.clone(),
            origin_node_id: remove_origin,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
                role_id,
                user_id: assigned_user_id,
            },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            conflicting_remove_event,
        )
        .await
        .expect("conflicting remove applies");

        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                conflict_key.clone(),
            )
            .await
            .is_some()
        );

        let resolving_remove_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(27, 7),
            target: target.clone(),
            origin_node_id: node(11),
            origin_seq: 1,
            observed: AdminDocumentClock::default()
                .with_observed(add_origin, 1)
                .with_observed(remove_origin, 1),
            actor,
            op: AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
                role_id,
                user_id: assigned_user_id,
            },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            resolving_remove_event,
        )
        .await
        .expect("resolving remove applies");

        assert_eq!(
            read_storage_value(&storage, ADMIN_DOCUMENT_CONFLICT_KEYSPACE, conflict_key).await,
            None
        );
        let stored_auth_doc =
            read_storage_value(&storage, AUTH_KEYSPACE, group_id.to_bytes().into())
                .await
                .expect("auth doc exists");
        let stored_auth_doc =
            GroupAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
        assert!(
            !stored_auth_doc.roles[&role_id]
                .assigned_users
                .contains(&assigned_user_id)
        );
        let reducer_state = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&reducer_state).expect("reducer state decodes");
        assert!(reducer_state.conflicts.is_empty());
    }

    #[tokio::test]
    async fn realm_assignment_admin_operation_materializes_auth_doc_and_reducer_state() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([10; 32]);
        let role_id = Ulid::from_parts(2, 2);
        let assigned_user_id = UserId::local(Ulid::from_parts(3, 3), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id: UserId::local(Ulid::from_parts(4, 4), realm_id),
            realm_id,
        };
        let auth_doc = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: "realm_member".to_string(),
                    permissions: HashMap::from([("/datasets".to_string(), Permission::READ)]),
                    assigned_users: HashSet::new(),
                },
            )]),
            operation_restrictions: HashMap::new(),
        };
        storage_batch_write_to(
            &storage,
            vec![(
                AUTH_KEYSPACE.to_string(),
                (*realm_id.as_bytes()).into(),
                auth_doc
                    .to_bytes(&actor)
                    .expect("auth doc serializes")
                    .into(),
            )],
        )
        .await
        .expect("auth doc writes");

        let add_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(5, 5),
            target: AdminDocumentTarget::Realm { realm_id },
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                role_id,
                user_id: assigned_user_id,
            },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::RealmAuthorization { realm_id },
            add_event,
        )
        .await
        .expect("add assignment applies");

        let stored_auth_doc =
            read_storage_value(&storage, AUTH_KEYSPACE, (*realm_id.as_bytes()).into())
                .await
                .expect("auth doc exists");
        let stored_auth_doc =
            RealmAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
        assert!(
            stored_auth_doc.roles[&role_id]
                .assigned_users
                .contains(&assigned_user_id)
        );
        let target = AdminDocumentTarget::Realm { realm_id };
        let reducer_state = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&reducer_state).expect("reducer state decodes");
        assert!(reducer_state.conflicts.is_empty());
        let assignment_path = realm_role_user_assignment_path(&role_id, &assigned_user_id);
        assert_eq!(
            reducer_state
                .user_subject_ids
                .get(&assignment_path)
                .and_then(|version| version.value.clone()),
            Some(assigned_user_id.to_string())
        );

        let remove_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(6, 6),
            target,
            origin_node_id: actor.node_id,
            origin_seq: 2,
            observed: AdminDocumentClock::default(),
            actor,
            op: AdminDocumentOperation::RealmRoleUserAssignmentRemoved {
                role_id,
                user_id: assigned_user_id,
            },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::RealmAuthorization { realm_id },
            remove_event,
        )
        .await
        .expect("remove assignment applies");

        let stored_auth_doc =
            read_storage_value(&storage, AUTH_KEYSPACE, (*realm_id.as_bytes()).into())
                .await
                .expect("auth doc exists");
        let stored_auth_doc =
            RealmAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
        assert!(
            !stored_auth_doc.roles[&role_id]
                .assigned_users
                .contains(&assigned_user_id)
        );
        let reducer_state = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&AdminDocumentTarget::Realm { realm_id }),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&reducer_state).expect("reducer state decodes");
        assert_eq!(
            reducer_state
                .user_subject_ids
                .get(&assignment_path)
                .and_then(|version| version.value.clone()),
            None
        );
    }

    async fn read_document_lifecycle_record(
        storage: &StorageHandle,
        document_id: Ulid,
    ) -> MetadataDocumentLifecycleRecord {
        let value = read_storage_value(
            storage,
            METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
            metadata_document_lifecycle_key(document_id),
        )
        .await
        .expect("lifecycle record exists");
        postcard::from_bytes(&value).expect("lifecycle record decodes")
    }

    async fn read_lifecycle_revision(
        storage: &StorageHandle,
        document_id: Ulid,
    ) -> DocumentSyncChange {
        let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let value = read_storage_value(
            storage,
            DOCUMENT_SYNC_REVISION_KEYSPACE,
            document_sync_revision_key(&target),
        )
        .await
        .expect("lifecycle revision exists");
        postcard::from_bytes(&value).expect("lifecycle revision decodes")
    }

    fn sync_summary(
        event_type_id: Option<String>,
        heads: BTreeSet<irokle_crate::OpId>,
    ) -> irokle_crate::sync::SyncSummary {
        irokle_crate::sync::SyncSummary {
            topic_id: topic(9),
            event_type_id,
            fingerprint: [0; 32],
            heads,
            actor_clock: irokle_crate::ActorClock::default(),
            actor_tips: BTreeMap::new(),
        }
    }

    #[test]
    fn buffered_irokle_publish_restart_child_process() {
        let Ok(root) = env::var(IROKLE_RESTART_CHILD_PATH_ENV) else {
            return;
        };
        let root = PathBuf::from(root);
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

        runtime.block_on(async {
            let service = open_restart_service(&root, "child-storage").await;
            let target = restart_target();
            let event = service
                .publish_document(
                    restart_event_id(),
                    target.clone(),
                    restart_payload(),
                    Vec::new(),
                )
                .await;

            assert_eq!(event, IrokleEvent::DocumentPublished { target });
        });

        // Skip Rust destructors so the parent verifies the restart contract, not shutdown cleanup.
        std::process::exit(0);
    }

    #[tokio::test]
    async fn acknowledged_irokle_publish_survives_buffered_process_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let target = restart_target();

        run_irokle_restart_child(root);

        let service = open_restart_service(root, "parent-storage").await;
        let topic = service
            .node()
            .open_topic::<DocumentSyncEvent>(target.irokle_topic_id())
            .expect("published topic reopens after restart");
        let history = topic
            .history(HistoryOrder::OldestFirst)
            .expect("published history reads after restart");

        assert_eq!(history.len(), 1);
        assert_eq!(
            history[0].event,
            DocumentSyncEvent::Upsert {
                event_id: restart_event_id(),
                target,
                bytes: restart_payload(),
            }
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn fan_out_peer_syncs_fails_when_any_peer_failed() {
        let ok_peer = peer(1);
        let failed_peer = peer(2);
        let peers = BTreeSet::from([ok_peer, failed_peer]);

        let error = IrokleService::fan_out_peer_syncs(
            peers,
            "test document sync".to_string(),
            move |peer| async move {
                if peer == ok_peer {
                    Ok(())
                } else {
                    Err(NetError::Bootstrap("offline peer".to_string()))
                }
            },
        )
        .await
        .expect_err("partial peer fan-out must fail");

        let NetError::Bootstrap(message) = error else {
            panic!("unexpected error: {error:?}");
        };
        assert!(message.contains("only 1/2 peers synced"));
        assert!(message.contains("offline peer"));
    }

    #[test]
    fn finish_batch_sync_fails_when_any_known_topic_failed() {
        let known_topics = BTreeSet::from([topic(3), topic(4)]);
        let failed_topics = BTreeSet::from([topic(4)]);

        let error = finish_batch_sync(peer(5), &known_topics, &failed_topics)
            .expect_err("partial topic failure must fail the batch");

        let NetError::Bootstrap(message) = error else {
            panic!("unexpected error: {error:?}");
        };
        assert!(message.contains("1/2 Irokle batch topics failed"));
    }

    #[test]
    fn finish_batch_sync_succeeds_when_no_topics_failed() {
        let known_topics = BTreeSet::from([topic(6), topic(7)]);
        let failed_topics = BTreeSet::new();

        finish_batch_sync(peer(8), &known_topics, &failed_topics)
            .expect("batch with no failed topics should succeed");
    }

    #[test]
    fn remote_summary_is_empty_only_for_untyped_headless_topics() {
        assert!(remote_summary_is_empty(&sync_summary(
            None,
            BTreeSet::new()
        )));
        assert!(!remote_summary_is_empty(&sync_summary(
            Some(DocumentSyncEvent::TYPE_ID.to_string()),
            BTreeSet::new()
        )));
        assert!(!remote_summary_is_empty(&sync_summary(
            None,
            BTreeSet::from([irokle_crate::OpId::hash(b"head")])
        )));
    }

    #[tokio::test]
    async fn metadata_registry_upsert_skips_stale_local_record() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(1, 1);
        let document_id = Ulid::from_parts(2, 2);
        let local = registry_record(
            group_id,
            document_id,
            "datasets/fresh",
            200,
            Ulid::from_parts(200, 2),
        );
        write_registry_record(&storage, &local).await;

        let mut stale = registry_record(
            group_id,
            document_id,
            "datasets/stale",
            100,
            Ulid::from_parts(100, 1),
        );
        stale.public = false;
        stale.holder_node_ids = vec![node(2)];
        let stale_bytes = postcard::to_allocvec(&stale).expect("stale registry serializes");

        apply_metadata_registry_upsert_to_storage(&storage, stale, stale_bytes)
            .await
            .expect("stale registry upsert succeeds idempotently");

        let primary = read_registry_record(
            &storage,
            METADATA_INDEX_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await;
        let document_index = read_registry_record(
            &storage,
            METADATA_DOCUMENT_INDEX_KEYSPACE,
            metadata_document_key(document_id),
        )
        .await;
        let holder_value = read_storage_value(
            &storage,
            METADATA_HOLDERS_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await
        .expect("holder index exists");
        let holders: Vec<NodeId> = postcard::from_bytes(&holder_value).expect("holders decode");

        assert_eq!(primary, local);
        assert_eq!(document_index, local);
        assert_eq!(holders, local.holder_node_ids);
    }

    #[tokio::test]
    async fn metadata_registry_delete_skips_without_lifecycle_tombstone() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(30, 1);
        let document_id = Ulid::from_parts(31, 1);
        let record = registry_record(
            group_id,
            document_id,
            "datasets/kept",
            100,
            Ulid::from_parts(32, 1),
        );
        write_registry_record(&storage, &record).await;

        apply_metadata_registry_delete_to_storage(&storage, group_id, document_id)
            .await
            .expect("registry delete skips without tombstone");

        let primary = read_registry_record(
            &storage,
            METADATA_INDEX_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await;
        let document_index = read_registry_record(
            &storage,
            METADATA_DOCUMENT_INDEX_KEYSPACE,
            metadata_document_key(document_id),
        )
        .await;
        let holder_value = read_storage_value(
            &storage,
            METADATA_HOLDERS_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await
        .expect("holder index exists");
        let holders: Vec<NodeId> = postcard::from_bytes(&holder_value).expect("holders decode");

        assert_eq!(primary, record);
        assert_eq!(document_index, record);
        assert_eq!(holders, record.holder_node_ids);
    }

    #[tokio::test]
    async fn metadata_registry_delete_with_matching_lifecycle_tombstone_deletes_indexes() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(40, 1);
        let document_id = Ulid::from_parts(41, 1);
        let record = registry_record(
            group_id,
            document_id,
            "datasets/deleted",
            100,
            Ulid::from_parts(42, 1),
        );
        write_registry_record(&storage, &record).await;
        let lifecycle = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(43, 1),
            record.last_event_id,
        );
        storage_batch_write_to(
            &storage,
            vec![
                metadata_document_lifecycle_write_entry(&lifecycle)
                    .expect("lifecycle entry builds"),
            ],
        )
        .await
        .expect("lifecycle tombstone writes");

        apply_metadata_registry_delete_to_storage(&storage, group_id, document_id)
            .await
            .expect("registry delete applies with tombstone");

        assert!(
            read_storage_value(
                &storage,
                METADATA_INDEX_KEYSPACE,
                metadata_registry_key(group_id, document_id),
            )
            .await
            .is_none()
        );
        assert!(
            read_storage_value(
                &storage,
                METADATA_DOCUMENT_INDEX_KEYSPACE,
                metadata_document_key(document_id),
            )
            .await
            .is_none()
        );
        assert!(
            read_storage_value(
                &storage,
                METADATA_HOLDERS_KEYSPACE,
                metadata_registry_key(group_id, document_id),
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn metadata_document_lifecycle_upsert_stamps_revision_and_replays_idempotently() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(1, 1);
        let document_id = Ulid::from_parts(2, 2);
        let event_id = Ulid::from_parts(3, 3);
        let event = metadata_create_event(group_id, document_id, 100, event_id, 7);
        let lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(event.clone()),
        };

        assert!(
            apply_metadata_document_lifecycle_to_storage(&storage, &lifecycle, node(9))
                .await
                .expect("upsert lifecycle applies")
        );
        assert!(
            !apply_metadata_document_lifecycle_to_storage(&storage, &lifecycle, node(9))
                .await
                .expect("equal upsert lifecycle is idempotent")
        );

        let stored_event = read_storage_value(
            &storage,
            METADATA_EVENT_LOG_KEYSPACE,
            metadata_event_log_key(document_id, event_id),
        )
        .await
        .expect("event log record exists");
        assert_eq!(
            postcard::from_bytes::<MetadataCreateEventRecord>(&stored_event)
                .expect("event log record decodes"),
            event
        );
        let revision = read_lifecycle_revision(&storage, document_id).await;
        assert_eq!(revision.current.event_id, event_id);
        assert_eq!(revision.current.actor, node(7));
        assert_eq!(revision.current.generation, 100);
        assert_eq!(revision.kind, DocumentSyncChangeKind::Upsert);
    }

    #[tokio::test]
    async fn metadata_document_lifecycle_upsert_skips_newer_delete_sidecar() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(10, 1);
        let document_id = Ulid::from_parts(11, 1);
        let stale_event_id = Ulid::from_parts(12, 1);
        let delete_event_id = Ulid::from_parts(13, 1);
        let delete_lifecycle =
            metadata_delete_lifecycle(group_id, document_id, 200, delete_event_id, stale_event_id);
        assert!(
            apply_metadata_document_lifecycle_to_storage(&storage, &delete_lifecycle, node(8))
                .await
                .expect("delete lifecycle applies")
        );

        let stale_event = metadata_create_event(group_id, document_id, 100, stale_event_id, 7);
        let stale_lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(stale_event),
        };
        assert!(
            !apply_metadata_document_lifecycle_to_storage(&storage, &stale_lifecycle, node(8))
                .await
                .expect("stale upsert lifecycle is fenced")
        );

        assert_eq!(
            read_document_lifecycle_record(&storage, document_id).await,
            delete_lifecycle
        );
        assert!(
            read_storage_value(
                &storage,
                METADATA_EVENT_LOG_KEYSPACE,
                metadata_event_log_key(document_id, stale_event_id),
            )
            .await
            .is_none()
        );
        let revision = read_lifecycle_revision(&storage, document_id).await;
        assert_eq!(revision.current.event_id, delete_event_id);
        assert_eq!(revision.kind, DocumentSyncChangeKind::Delete);
    }

    #[tokio::test]
    async fn metadata_document_lifecycle_delete_skips_stale_and_equal_sidecars() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(20, 1);
        let document_id = Ulid::from_parts(21, 1);
        let deleted_after_event_id = Ulid::from_parts(22, 1);
        let local_delete = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(23, 1),
            deleted_after_event_id,
        );
        assert!(
            apply_metadata_document_lifecycle_to_storage(&storage, &local_delete, node(8))
                .await
                .expect("delete lifecycle applies")
        );
        assert!(
            !apply_metadata_document_lifecycle_to_storage(&storage, &local_delete, node(8))
                .await
                .expect("equal delete lifecycle is idempotent")
        );

        let stale_delete = metadata_delete_lifecycle(
            group_id,
            document_id,
            100,
            Ulid::from_parts(24, 1),
            deleted_after_event_id,
        );
        assert!(
            !apply_metadata_document_lifecycle_to_storage(&storage, &stale_delete, node(8))
                .await
                .expect("stale delete lifecycle is fenced")
        );

        assert_eq!(
            read_document_lifecycle_record(&storage, document_id).await,
            local_delete
        );
        let revision = read_lifecycle_revision(&storage, document_id).await;
        assert_eq!(revision.current.generation, 200);
        assert_eq!(revision.kind, DocumentSyncChangeKind::Delete);
    }

    #[test]
    fn metadata_document_delete_write_entries_include_prune_job() {
        let document_id = Ulid::from_parts(10, 1);
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            "urn:graph:deleted".to_string(),
            RealmId::from_bytes([1; 32]),
            Ulid::from_parts(11, 1),
            document_id,
            12,
        );
        let record = MetadataDocumentDeleteRecord {
            event_id: Ulid::from_parts(13, 1),
            tombstone: tombstone.clone(),
            deleted_after_event_id: Ulid::from_parts(9, 1),
        };

        let entries = metadata_document_delete_write_entries(&record).expect("entries build");

        let prune_jobs = entries
            .iter()
            .filter(|(keyspace, _, _)| keyspace == METADATA_GRAPH_PRUNE_JOB_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<MetadataGraphPruneJobRecord>(value.as_ref())
                    .expect("prune job decodes")
            })
            .collect::<Vec<_>>();
        assert_eq!(prune_jobs.len(), 1);
        assert_eq!(prune_jobs[0].graph_iri, tombstone.graph_iri);
        assert_eq!(prune_jobs[0].attempts, 0);
        assert!(prune_jobs[0].last_error.is_none());
    }
}
