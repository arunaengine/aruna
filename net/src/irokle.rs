use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{AdminDocumentApplyStatus, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use aruna_core::document::{
    DocumentSyncApplyDecision, DocumentSyncChange, DocumentSyncConflict, DocumentSyncEvent,
    DocumentSyncPublish, DocumentSyncReconcileResult, DocumentSyncTarget, IrokleEvent,
    document_sync_apply_decision,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_STATE_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, GROUP_KEYSPACE,
    IROKLE_APPLIED_OPS_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataDocumentDeleteRecord, MetadataDocumentLifecycleRecord,
    MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord,
};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, document_sync_conflict_write_entry,
    document_sync_revision_key, document_sync_revision_write_entry,
    metadata_create_event_and_pending_projection_write_entries, metadata_document_lifecycle_key,
    metadata_document_lifecycle_revision_change, metadata_document_lifecycle_write_entry,
    metadata_graph_lifecycle_key, metadata_graph_lifecycle_write_entry,
    metadata_graph_prune_job_write_entry, metadata_registry_delete_entries,
    metadata_registry_write_entries, stale_admin_document_conflict_delete_entries,
    stale_subject_index_deletes, subject_index_writes,
};
use aruna_core::structs::{
    Group, GroupAuthorizationDocument, MetadataRegistryRecord, RealmAuthorizationDocument,
    RealmConfigDocument, Role, User,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{RoleId, TxnId, UserId, Value};
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
const REALM_CONFIG_METADATA_REPLICATION_PATH: &str = "realm_config.settings.metadata_replication";
const REALM_CONFIG_DISCOVERY_PATH: &str = "realm_config.settings.discovery";

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
            DocumentSyncEvent::UpsertWithRevision {
                event_id,
                target,
                bytes,
                change,
            } => DocumentSyncPublish::UpsertWithRevision {
                event_id,
                target,
                bytes,
                change,
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
                DocumentSyncPublish::UpsertWithRevision {
                    event_id,
                    target,
                    bytes,
                    change,
                } => DocumentSyncEvent::UpsertWithRevision {
                    event_id,
                    target,
                    bytes,
                    change,
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
                    event @ (DocumentSyncEvent::Upsert {
                        target: DocumentSyncTarget::MetadataCreateEvent { .. },
                        ..
                    }
                    | DocumentSyncEvent::UpsertWithRevision {
                        target: DocumentSyncTarget::MetadataCreateEvent { .. },
                        ..
                    }) => {
                        let pending = self.pending_metadata_create_apply(event)?;
                        pending_metadata_creates.push(pending);
                        deferred_creates = true;
                    }
                    DocumentSyncEvent::Upsert {
                        target: DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
                        bytes,
                        ..
                    }
                    | DocumentSyncEvent::UpsertWithRevision {
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
                    }
                    | DocumentSyncEvent::UpsertWithRevision {
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
                        let accepted = self
                            .apply_metadata_graph_lifecycle(record.clone(), bytes)
                            .await?;
                        if accepted {
                            if record.is_deleted() {
                                metadata_graph_tombstones.push(record);
                            }
                            applied_targets.push(target);
                        }
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
        let (document_id, target_event_id, bytes) = match event {
            DocumentSyncEvent::Upsert {
                target:
                    DocumentSyncTarget::MetadataCreateEvent {
                        document_id,
                        event_id: target_event_id,
                    },
                bytes,
                ..
            }
            | DocumentSyncEvent::UpsertWithRevision {
                target:
                    DocumentSyncTarget::MetadataCreateEvent {
                        document_id,
                        event_id: target_event_id,
                    },
                bytes,
                ..
            } => (document_id, target_event_id, bytes),
            _ => unreachable!(
                "metadata create apply helper is only called for metadata create upserts"
            ),
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
            DocumentSyncEvent::UpsertWithRevision {
                target,
                bytes,
                change,
                ..
            } => self.apply_upsert_with_revision(target, bytes, change).await,
            DocumentSyncEvent::Delete { target, .. } => self.apply_delete(target).await,
            DocumentSyncEvent::AdminOperation { target, event } => {
                apply_admin_document_operation_to_storage(&self.storage, target, *event).await
            }
        }
    }

    async fn apply_upsert_with_revision(
        &self,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        change: DocumentSyncChange,
    ) -> Result<()> {
        if let DocumentSyncTarget::User { user_id } = target {
            return apply_revisioned_user_upsert_to_storage(&self.storage, user_id, bytes, change)
                .await;
        }
        self.apply_upsert(target, bytes).await
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
            return self
                .apply_metadata_graph_lifecycle(record, bytes)
                .await
                .map(|_| ());
        }
        if matches!(
            target,
            DocumentSyncTarget::Group { .. }
                | DocumentSyncTarget::User { .. }
                | DocumentSyncTarget::GroupAuthorization { .. }
                | DocumentSyncTarget::RealmAuthorization { .. }
                | DocumentSyncTarget::RealmConfig { .. }
        ) {
            return apply_legacy_admin_document_upsert_to_storage(&self.storage, target, bytes)
                .await;
        }
        self.storage_write(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.into(),
        )
        .await
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
    ) -> Result<bool> {
        apply_metadata_graph_lifecycle_to_storage(&self.storage, &record, primary_bytes).await
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

async fn apply_legacy_admin_document_upsert_to_storage(
    storage: &StorageHandle,
    target: DocumentSyncTarget,
    bytes: Vec<u8>,
) -> Result<()> {
    let admin_target = admin_document_target_for_legacy_upsert(&target).ok_or_else(|| {
        NetError::Bootstrap("legacy admin upsert target is not admin-reduced".to_string())
    })?;
    let reducer_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&admin_target),
    )
    .await?
    .map(|bytes| postcard::from_bytes::<AdminDocumentReducerState>(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if let Some(reducer_state) = reducer_state.as_ref()
        && reducer_state.target != admin_target
    {
        return Err(NetError::Bootstrap(
            "legacy admin upsert reducer state target mismatch".to_string(),
        ));
    }

    match target {
        DocumentSyncTarget::Group { group_id } => {
            let target = DocumentSyncTarget::Group { group_id };
            let mut group = Group::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if group.group_id != group_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated group document id {group_id} does not match payload group id {}",
                    group.group_id
                )));
            }
            let bytes = if let Some(reducer_state) = reducer_state.as_ref() {
                overlay_group_role_set_reducer_materialization(&mut group, reducer_state);
                postcard::to_allocvec(&group)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
            } else {
                bytes
            };
            storage_batch_write_to(storage, vec![target_write_entry(target, bytes.into())]).await
        }
        DocumentSyncTarget::User { user_id } => {
            let mut user =
                User::from_bytes(&bytes).map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if user.user_id != user_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated user document id {} does not match payload user id {}",
                    user_id, user.user_id
                )));
            }
            if let Some(reducer_state) = reducer_state.as_ref() {
                let target = DocumentSyncTarget::User { user_id };
                let previous_user = storage_read_from(
                    storage,
                    target.storage_keyspace().to_string(),
                    target.storage_key(),
                )
                .await?
                .map(|bytes| User::from_bytes(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                overlay_user_reducer_materialization(&mut user, reducer_state);
                return apply_merged_user_upsert_to_storage(storage, previous_user.as_ref(), user)
                    .await;
            }
            apply_user_upsert_to_storage(storage, user, bytes).await
        }
        DocumentSyncTarget::GroupAuthorization { group_id } => {
            let target = DocumentSyncTarget::GroupAuthorization { group_id };
            let bytes = if let Some(reducer_state) = reducer_state.as_ref() {
                let mut auth_doc = GroupAuthorizationDocument::from_bytes(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                if auth_doc.group_id != group_id {
                    return Err(NetError::Bootstrap(format!(
                        "replicated group authorization document id {group_id} does not match payload group id {}",
                        auth_doc.group_id
                    )));
                }
                overlay_group_authorization_reducer_materialization(&mut auth_doc, reducer_state);
                postcard::to_allocvec(&auth_doc)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
            } else {
                bytes
            };
            storage_batch_write_to(storage, vec![target_write_entry(target, bytes.into())]).await
        }
        DocumentSyncTarget::RealmAuthorization { realm_id } => {
            let target = DocumentSyncTarget::RealmAuthorization { realm_id };
            let bytes = if let Some(reducer_state) = reducer_state.as_ref() {
                let mut auth_doc = RealmAuthorizationDocument::from_bytes(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                if auth_doc.realm_id != realm_id {
                    return Err(NetError::Bootstrap(format!(
                        "replicated realm authorization document id {realm_id} does not match payload realm id {}",
                        auth_doc.realm_id
                    )));
                }
                overlay_realm_authorization_reducer_materialization(&mut auth_doc, reducer_state);
                postcard::to_allocvec(&auth_doc)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
            } else {
                bytes
            };
            storage_batch_write_to(storage, vec![target_write_entry(target, bytes.into())]).await
        }
        DocumentSyncTarget::RealmConfig { realm_id } => {
            let target = DocumentSyncTarget::RealmConfig { realm_id };
            let bytes = if let Some(reducer_state) = reducer_state.as_ref() {
                let mut config = RealmConfigDocument::from_bytes(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                if config.realm_id != realm_id {
                    return Err(NetError::Bootstrap(format!(
                        "replicated realm config document id {realm_id} does not match payload realm id {}",
                        config.realm_id
                    )));
                }
                overlay_realm_config_reducer_materialization(&mut config, reducer_state);
                postcard::to_allocvec(&config)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
            } else {
                bytes
            };
            storage_batch_write_to(storage, vec![target_write_entry(target, bytes.into())]).await
        }
        _ => Err(NetError::Bootstrap(
            "legacy admin upsert target is not admin-reduced".to_string(),
        )),
    }
}

fn target_write_entry(target: DocumentSyncTarget, value: Value) -> (String, ByteView, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        value,
    )
}

async fn apply_merged_user_upsert_to_storage(
    storage: &StorageHandle,
    previous_user: Option<&User>,
    user: User,
) -> Result<()> {
    let target = DocumentSyncTarget::User {
        user_id: user.user_id,
    };
    let mut writes = vec![(
        target.storage_keyspace().to_string(),
        target.storage_key(),
        postcard::to_allocvec(&user)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .into(),
    )];
    writes.extend(subject_index_writes(&user));
    storage_batch_delete_and_write_transactionally(
        storage,
        stale_subject_index_deletes(previous_user, Some(&user)),
        writes,
    )
    .await
}

async fn apply_revisioned_user_upsert_to_storage(
    storage: &StorageHandle,
    user_id: UserId,
    bytes: Vec<u8>,
    change: DocumentSyncChange,
) -> Result<()> {
    if change.kind != aruna_core::document::DocumentSyncChangeKind::Upsert {
        return Err(NetError::Bootstrap(
            "revisioned user upsert must carry an upsert change".to_string(),
        ));
    }

    let target = DocumentSyncTarget::User { user_id };
    let mut user =
        User::from_bytes(&bytes).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if user.user_id != user_id {
        return Err(NetError::Bootstrap(format!(
            "replicated user document id {user_id} does not match payload user id {}",
            user.user_id
        )));
    }

    let local_change = storage_read_from(
        storage,
        DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
        document_sync_revision_key(&target),
    )
    .await?
    .map(|bytes| postcard::from_bytes::<DocumentSyncChange>(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let previous_user_bytes = storage_read_from(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
    )
    .await?;
    let previous_user = previous_user_bytes
        .as_ref()
        .map(|bytes| User::from_bytes(bytes.as_ref()))
        .transpose()
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let admin_target = AdminDocumentTarget::User { user_id };
    let reducer_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&admin_target),
    )
    .await?
    .map(|bytes| postcard::from_bytes::<AdminDocumentReducerState>(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if let Some(reducer_state) = reducer_state.as_ref()
        && reducer_state.target != admin_target
    {
        return Err(NetError::Bootstrap(
            "revisioned user upsert reducer state target mismatch".to_string(),
        ));
    }

    match document_sync_apply_decision(local_change.as_ref(), &change) {
        DocumentSyncApplyDecision::Apply => {}
        DocumentSyncApplyDecision::SkipStale | DocumentSyncApplyDecision::SkipTombstoned => {
            return Ok(());
        }
        DocumentSyncApplyDecision::Conflict => {
            let conflict = DocumentSyncConflict {
                target: target.clone(),
                local_change,
                local_bytes: previous_user_bytes.map(|bytes| bytes.as_ref().to_vec()),
                incoming_change: change,
                incoming_bytes: bytes,
            };
            return storage_batch_write_to(
                storage,
                vec![
                    document_sync_conflict_write_entry(&target, &conflict)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                ],
            )
            .await;
        }
    }

    let primary_bytes = if let Some(reducer_state) = reducer_state.as_ref() {
        overlay_user_reducer_materialization(&mut user, reducer_state);
        postcard::to_allocvec(&user).map_err(|error| NetError::Bootstrap(error.to_string()))?
    } else {
        bytes
    };
    let mut writes = vec![target_write_entry(target.clone(), primary_bytes.into())];
    writes.extend(subject_index_writes(&user));
    writes.push(
        document_sync_revision_write_entry(&target, &change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );
    storage_batch_delete_and_write_transactionally(
        storage,
        stale_subject_index_deletes(previous_user.as_ref(), Some(&user)),
        writes,
    )
    .await
}

fn overlay_user_reducer_materialization(
    user: &mut User,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state.conflicts.contains_key("user.name")
        && let Some(name) = reducer_state.materialized_user_name()
    {
        user.name = name;
    }

    for path in reducer_state.conflicts.keys() {
        if let Some(subject_id) = path.strip_prefix("user.subject_ids.") {
            user.subject_ids.retain(|candidate| candidate != subject_id);
        }
        if let Some(key) = path.strip_prefix("user.attributes.") {
            user.attributes.remove(key);
        }
    }

    for (subject_id, version) in &reducer_state.user_subject_ids {
        let path = format!("user.subject_ids.{subject_id}");
        user.subject_ids.retain(|candidate| candidate != subject_id);
        if reducer_state.conflicts.contains_key(&path) {
            continue;
        }
        if let Some(materialized_subject_id) = version.value.as_ref()
            && !user.subject_ids.contains(materialized_subject_id)
        {
            user.subject_ids.push(materialized_subject_id.clone());
        }
    }

    for (key, version) in &reducer_state.user_attributes {
        let path = format!("user.attributes.{key}");
        if reducer_state.conflicts.contains_key(&path) {
            user.attributes.remove(key);
            continue;
        }
        match version.value.as_ref() {
            Some(value) => {
                user.attributes.insert(key.clone(), value.clone());
            }
            None => {
                user.attributes.remove(key);
            }
        }
    }
}

fn overlay_group_role_set_reducer_materialization(
    group: &mut Group,
    reducer_state: &AdminDocumentReducerState,
) {
    for role_id in reducer_state.materialized_group_roles() {
        group.roles.insert(role_id);
    }

    for path in reducer_state.conflicts.keys() {
        if let Some(role_id) = group_role_from_path(path) {
            group.roles.remove(&role_id);
        }
    }
}

fn overlay_group_authorization_reducer_materialization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    overlay_group_authorization_role_reducer_materialization(auth_doc, reducer_state);
    overlay_group_authorization_assignment_reducer_materialization(auth_doc, reducer_state, None);
}

fn overlay_group_authorization_role_reducer_materialization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some(role_id) = group_role_from_path(path) {
            auth_doc.roles.remove(&role_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some(role_id) = group_role_from_path(path) else {
            continue;
        };
        if reducer_state.conflicts.contains_key(path) {
            auth_doc.roles.remove(&role_id);
            continue;
        }
        let Some(role) = version
            .value
            .as_deref()
            .and_then(|value| reducer_role_definition(value, role_id))
        else {
            continue;
        };
        let assigned_users = auth_doc
            .roles
            .get(&role_id)
            .map(|role| role.assigned_users.clone())
            .unwrap_or_default();
        auth_doc.roles.insert(
            role_id,
            Role {
                role_id,
                name: role.name,
                permissions: role.permissions.into_iter().collect(),
                assigned_users,
            },
        );
    }
}

fn overlay_group_authorization_role_assignment_reducer_materialization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role_id: RoleId,
) {
    overlay_group_authorization_assignment_reducer_materialization(
        auth_doc,
        reducer_state,
        Some(role_id),
    );
}

fn overlay_group_authorization_assignment_reducer_materialization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    only_role_id: Option<RoleId>,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some((role_id, user_id)) = group_role_user_assignment_from_path(path)
            && only_role_id.is_none_or(|only_role_id| only_role_id == role_id)
            && let Some(role) = auth_doc.roles.get_mut(&role_id)
        {
            role.assigned_users.remove(&user_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some((role_id, user_id)) = group_role_user_assignment_from_path(path) else {
            continue;
        };
        if only_role_id.is_some_and(|only_role_id| only_role_id != role_id) {
            continue;
        }
        let Some(role) = auth_doc.roles.get_mut(&role_id) else {
            continue;
        };
        role.assigned_users.remove(&user_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if version
            .value
            .as_deref()
            .and_then(|value| UserId::from_string(value).ok())
            .is_some_and(|materialized_user_id| materialized_user_id == user_id)
        {
            role.assigned_users.insert(user_id);
        }
    }
}

fn overlay_realm_authorization_reducer_materialization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    overlay_realm_authorization_role_reducer_materialization(auth_doc, reducer_state);
    overlay_realm_authorization_assignment_reducer_materialization(auth_doc, reducer_state, None);
}

fn overlay_realm_authorization_role_reducer_materialization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some(role_id) = realm_role_from_path(path) {
            auth_doc.roles.remove(&role_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some(role_id) = realm_role_from_path(path) else {
            continue;
        };
        if reducer_state.conflicts.contains_key(path) {
            auth_doc.roles.remove(&role_id);
            continue;
        }
        let Some(role) = version
            .value
            .as_deref()
            .and_then(|value| reducer_role_definition(value, role_id))
        else {
            continue;
        };
        let assigned_users = auth_doc
            .roles
            .get(&role_id)
            .map(|role| role.assigned_users.clone())
            .unwrap_or_default();
        auth_doc.roles.insert(
            role_id,
            Role {
                role_id,
                name: role.name,
                permissions: role.permissions.into_iter().collect(),
                assigned_users,
            },
        );
    }
}

fn overlay_realm_authorization_role_assignment_reducer_materialization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role_id: RoleId,
) {
    overlay_realm_authorization_assignment_reducer_materialization(
        auth_doc,
        reducer_state,
        Some(role_id),
    );
}

fn overlay_realm_authorization_assignment_reducer_materialization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    only_role_id: Option<RoleId>,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some((role_id, user_id)) = realm_role_user_assignment_from_path(path)
            && only_role_id.is_none_or(|only_role_id| only_role_id == role_id)
            && let Some(role) = auth_doc.roles.get_mut(&role_id)
        {
            role.assigned_users.remove(&user_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some((role_id, user_id)) = realm_role_user_assignment_from_path(path) else {
            continue;
        };
        if only_role_id.is_some_and(|only_role_id| only_role_id != role_id) {
            continue;
        }
        let Some(role) = auth_doc.roles.get_mut(&role_id) else {
            continue;
        };
        role.assigned_users.remove(&user_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if version
            .value
            .as_deref()
            .and_then(|value| UserId::from_string(value).ok())
            .is_some_and(|materialized_user_id| materialized_user_id == user_id)
        {
            role.assigned_users.insert(user_id);
        }
    }
}

fn overlay_realm_config_reducer_materialization(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_METADATA_REPLICATION_PATH)
        && let Some(metadata_replication) =
            reducer_state.materialized_realm_config_metadata_replication()
    {
        config.metadata_replication = metadata_replication;
    }

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_DISCOVERY_PATH)
        && let Some(discovery) = reducer_state.materialized_realm_config_discovery()
    {
        config.discovery = discovery;
    }

    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = realm_config_node_from_path(path) {
            remove_realm_config_node(config, &node_id);
        }
    }

    for (node_id, kind) in reducer_state.materialized_realm_config_nodes() {
        let path = realm_config_node_path(&node_id);
        if reducer_state.conflicts.contains_key(&path) {
            remove_realm_config_node(config, &node_id);
            continue;
        }
        config.ensure_node(node_id, kind);
    }

    let materialized_providers = reducer_state.materialized_realm_config_oidc_providers();
    for path in reducer_state.conflicts.keys() {
        if let Some(provider_id) = realm_config_oidc_provider_from_path(path) {
            remove_realm_config_oidc_provider(config, provider_id);
        }
    }

    for path in reducer_state.user_subject_ids.keys() {
        let Some(provider_id) = realm_config_oidc_provider_from_path(path) else {
            continue;
        };
        remove_realm_config_oidc_provider(config, provider_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(provider) = materialized_providers.get(provider_id) {
            config.oidc_providers.push(provider.clone());
        }
    }
}

fn remove_realm_config_node(config: &mut RealmConfigDocument, node_id: &NodeId) {
    let node_id = node_id.to_string();
    config.nodes.retain(|node| node.node_id != node_id);
}

fn remove_realm_config_oidc_provider(config: &mut RealmConfigDocument, provider_id: &str) {
    config
        .oidc_providers
        .retain(|provider| provider.id != provider_id);
}

fn group_role_user_assignment_from_path(path: &str) -> Option<(RoleId, UserId)> {
    let path = path.strip_prefix("group.roles.")?;
    let (role_id, user_id) = path.split_once(".assigned_users.")?;
    Some((
        Ulid::from_string(role_id).ok()?,
        UserId::from_string(user_id).ok()?,
    ))
}

fn realm_role_user_assignment_from_path(path: &str) -> Option<(RoleId, UserId)> {
    let path = path.strip_prefix("realm.roles.")?;
    let (role_id, user_id) = path.split_once(".assigned_users.")?;
    Some((
        Ulid::from_string(role_id).ok()?,
        UserId::from_string(user_id).ok()?,
    ))
}

fn group_role_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("group.roles.")?;
    if role_id.contains(".assigned_users.") {
        return None;
    }
    Ulid::from_string(role_id).ok()
}

fn realm_role_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("realm.roles.")?;
    if role_id.contains(".assigned_users.") {
        return None;
    }
    Ulid::from_string(role_id).ok()
}

fn realm_config_node_from_path(path: &str) -> Option<NodeId> {
    let node_id = path.strip_prefix("realm_config.nodes.")?;
    NodeId::from_str(node_id).ok()
}

fn realm_config_oidc_provider_from_path(path: &str) -> Option<&str> {
    path.strip_prefix("realm_config.oidc_providers.")
}

fn reducer_role_definition(value: &str, role_id: RoleId) -> Option<AdminDocumentRoleDefinition> {
    let role = serde_json::from_str::<AdminDocumentRoleDefinition>(value).ok()?;
    (role.role_id == role_id).then_some(role)
}

fn admin_document_target_for_legacy_upsert(
    target: &DocumentSyncTarget,
) -> Option<AdminDocumentTarget> {
    match target {
        DocumentSyncTarget::User { user_id } => {
            Some(AdminDocumentTarget::User { user_id: *user_id })
        }
        DocumentSyncTarget::Group { group_id } => Some(AdminDocumentTarget::Group {
            group_id: *group_id,
        }),
        DocumentSyncTarget::GroupAuthorization { group_id } => Some(AdminDocumentTarget::Group {
            group_id: *group_id,
        }),
        DocumentSyncTarget::RealmAuthorization { realm_id } => Some(AdminDocumentTarget::Realm {
            realm_id: *realm_id,
        }),
        DocumentSyncTarget::RealmConfig { realm_id } => Some(AdminDocumentTarget::RealmConfig {
            realm_id: *realm_id,
        }),
        _ => None,
    }
}

async fn apply_user_upsert_to_storage(
    storage: &StorageHandle,
    user: User,
    primary_bytes: Vec<u8>,
) -> Result<()> {
    let target = DocumentSyncTarget::User {
        user_id: user.user_id,
    };
    let previous = storage_read_from(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
    )
    .await?
    .map(|bytes| User::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;

    let deletes = stale_subject_index_deletes(previous.as_ref(), Some(&user));
    if !deletes.is_empty() {
        storage_batch_delete_to(storage, deletes).await?;
    }

    let mut writes = vec![(
        target.storage_keyspace().to_string(),
        target.storage_key(),
        primary_bytes.into(),
    )];
    writes.extend(subject_index_writes(&user));
    storage_batch_write_to(storage, writes).await
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

async fn apply_metadata_graph_lifecycle_to_storage(
    storage: &StorageHandle,
    record: &MetadataGraphLifecycleRecord,
    primary_bytes: Vec<u8>,
) -> Result<bool> {
    if record.is_deleted() && !metadata_graph_lifecycle_delete_current(storage, record).await? {
        return Ok(false);
    }

    let (key_space, key, _) = metadata_graph_lifecycle_write_entry(record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    storage_batch_write_to(storage, vec![(key_space, key, primary_bytes.into())]).await?;
    if record.is_deleted() {
        apply_metadata_registry_delete_to_storage(storage, record.group_id, record.document_id)
            .await?;
    }
    Ok(true)
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
    if metadata_document_delete_matches_registry(&delete, group_id, document_id)
        && !metadata_registry_live_after_delete(storage, group_id, document_id, &delete).await?
    {
        storage_batch_delete_to(
            storage,
            metadata_registry_delete_entries(group_id, document_id),
        )
        .await?;
    }
    Ok(())
}

async fn metadata_graph_lifecycle_delete_current(
    storage: &StorageHandle,
    record: &MetadataGraphLifecycleRecord,
) -> Result<bool> {
    let Some(delete) = metadata_document_delete_in_storage(storage, record.document_id).await?
    else {
        return Ok(false);
    };
    if !metadata_document_delete_matches_graph_lifecycle(&delete, record) {
        return Ok(false);
    }
    Ok(
        !metadata_registry_live_after_delete(storage, record.group_id, record.document_id, &delete)
            .await?,
    )
}

fn metadata_document_delete_matches_graph_lifecycle(
    delete: &MetadataDocumentDeleteRecord,
    record: &MetadataGraphLifecycleRecord,
) -> bool {
    metadata_document_delete_matches_registry(delete, record.group_id, record.document_id)
        && delete.tombstone.graph_iri == record.graph_iri
        && delete.tombstone.updated_at_ms >= record.updated_at_ms
}

fn metadata_document_delete_matches_registry(
    delete: &MetadataDocumentDeleteRecord,
    group_id: Ulid,
    document_id: Ulid,
) -> bool {
    delete.tombstone.is_deleted()
        && delete.tombstone.group_id == group_id
        && delete.tombstone.document_id == document_id
}

async fn metadata_registry_live_after_delete(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
    delete: &MetadataDocumentDeleteRecord,
) -> Result<bool> {
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    let Some(value) = storage_read_from(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
    )
    .await?
    else {
        return Ok(false);
    };
    let record: MetadataRegistryRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(record.updated_at_ms > delete.tombstone.updated_at_ms
        || record.last_event_id > delete.deleted_after_event_id)
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
        (DocumentSyncTarget::RealmConfig { .. }, AdminDocumentTarget::RealmConfig { .. }) => {
            apply_realm_config_admin_document_operation_to_storage(storage, document_target, event)
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
        &event.op,
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

async fn group_role_set_write_entry_for_existing_group(
    storage: &StorageHandle,
    group_id: Ulid,
    reducer_state: &AdminDocumentReducerState,
) -> Result<Option<(String, ByteView, Value)>> {
    let target = DocumentSyncTarget::Group { group_id };
    let Some(bytes) =
        storage_read_from(storage, GROUP_KEYSPACE.to_string(), target.storage_key()).await?
    else {
        return Ok(None);
    };
    let mut group =
        Group::from_bytes(&bytes).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if group.group_id != group_id {
        return Err(NetError::Bootstrap(format!(
            "stored group document id {group_id} does not match payload group id {}",
            group.group_id
        )));
    }

    overlay_group_role_set_reducer_materialization(&mut group, reducer_state);
    Ok(Some(target_write_entry(
        target,
        postcard::to_allocvec(&group)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .into(),
    )))
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
        &event.op,
        AdminDocumentOperation::GroupRoleAdded { .. }
            | AdminDocumentOperation::GroupRoleCreated { .. }
            | AdminDocumentOperation::GroupRoleUserAssignmentAdded { .. }
            | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "group admin operation sync only supports role seeds, role creation, and role user assignment updates"
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
    let group_write = if matches!(&event.op, AdminDocumentOperation::GroupRoleCreated { .. }) {
        group_role_set_write_entry_for_existing_group(storage, group_id, &reducer_state).await?
    } else {
        None
    };

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
    if let Some(group_write) = group_write {
        writes.push(group_write);
    }
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
        &event.op,
        AdminDocumentOperation::RealmRoleAdded { .. }
            | AdminDocumentOperation::RealmRoleCreated { .. }
            | AdminDocumentOperation::RealmRoleUserAssignmentAdded { .. }
            | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm admin operation sync only supports role seeds, role creation, and role user assignment updates"
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

async fn apply_realm_config_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::RealmConfig { realm_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports realm config targets".to_string(),
        ));
    };
    let AdminDocumentTarget::RealmConfig {
        realm_id: event_realm_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a realm config".to_string(),
        ));
    };
    if event_realm_id != realm_id {
        return Err(NetError::Bootstrap(format!(
            "replicated realm config admin operation target {realm_id} does not match payload realm id {event_realm_id}"
        )));
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigNodeEnsured { .. }
            | AdminDocumentOperation::RealmConfigOidcProviderUpserted { .. }
            | AdminDocumentOperation::RealmConfigOidcProviderRemoved { .. }
            | AdminDocumentOperation::RealmConfigSettingsSet { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports node ensure, OIDC provider updates, and settings updates"
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

    let previous_config = storage_read_from(
        storage,
        document_target.storage_keyspace().to_string(),
        document_target.storage_key(),
    )
    .await?
    .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut writes = Vec::new();
    if let Some(mut config) = previous_config {
        if config.realm_id != realm_id {
            return Err(NetError::Bootstrap(format!(
                "stored realm config document id {realm_id} does not match payload realm id {}",
                config.realm_id
            )));
        }
        overlay_realm_config_reducer_materialization(&mut config, &reducer_state);
        writes.push((
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            config
                .to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ));
    }
    writes.push(
        admin_document_reducer_state_write_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );
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
    if let AdminDocumentOperation::GroupRoleCreated { role } = &event.op {
        materialize_group_authorization_role(auth_doc, reducer_state, role);
        return;
    }

    let (role_id, user_id) = match &event.op {
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id } => {
            (role_id, user_id)
        }
        _ => return,
    };
    let path = group_role_user_assignment_path(role_id, user_id);
    if reducer_state.conflicts.contains_key(&path) {
        if let Some(role) = auth_doc.roles.get_mut(role_id) {
            role.assigned_users.remove(user_id);
        }
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

fn materialize_group_authorization_role(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role: &AdminDocumentRoleDefinition,
) {
    let role_path = group_role_path(&role.role_id);
    if reducer_state.conflicts.contains_key(&role_path)
        || !reducer_state
            .materialized_group_roles()
            .contains(&role.role_id)
    {
        auth_doc.roles.remove(&role.role_id);
        return;
    }

    let assigned_users = auth_doc
        .roles
        .get(&role.role_id)
        .map(|role| role.assigned_users.clone())
        .unwrap_or_default();
    auth_doc.roles.insert(
        role.role_id,
        Role {
            role_id: role.role_id,
            name: role.name.clone(),
            permissions: role
                .permissions
                .iter()
                .map(|(path, permission)| (path.clone(), permission.clone()))
                .collect(),
            assigned_users,
        },
    );
    overlay_group_authorization_role_assignment_reducer_materialization(
        auth_doc,
        reducer_state,
        role.role_id,
    );
}

fn materialize_realm_authorization_admin_document_operation(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) {
    if let AdminDocumentOperation::RealmRoleCreated { role } = &event.op {
        materialize_realm_authorization_role(auth_doc, reducer_state, role);
        return;
    }

    let (role_id, user_id) = match &event.op {
        AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id } => {
            (role_id, user_id)
        }
        _ => return,
    };
    let path = realm_role_user_assignment_path(role_id, user_id);
    if reducer_state.conflicts.contains_key(&path) {
        if let Some(role) = auth_doc.roles.get_mut(role_id) {
            role.assigned_users.remove(user_id);
        }
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

fn materialize_realm_authorization_role(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role: &AdminDocumentRoleDefinition,
) {
    let role_path = realm_role_path(&role.role_id);
    if reducer_state.conflicts.contains_key(&role_path)
        || !reducer_state
            .materialized_realm_roles()
            .contains(&role.role_id)
    {
        auth_doc.roles.remove(&role.role_id);
        return;
    }

    let assigned_users = auth_doc
        .roles
        .get(&role.role_id)
        .map(|role| role.assigned_users.clone())
        .unwrap_or_default();
    auth_doc.roles.insert(
        role.role_id,
        Role {
            role_id: role.role_id,
            name: role.name.clone(),
            permissions: role
                .permissions
                .iter()
                .map(|(path, permission)| (path.clone(), permission.clone()))
                .collect(),
            assigned_users,
        },
    );
    overlay_realm_authorization_role_assignment_reducer_materialization(
        auth_doc,
        reducer_state,
        role.role_id,
    );
}

fn group_role_path(role_id: &RoleId) -> String {
    format!("group.roles.{role_id}")
}

fn realm_role_path(role_id: &RoleId) -> String {
    format!("realm.roles.{role_id}")
}

fn realm_config_node_path(node_id: &NodeId) -> String {
    format!("realm_config.nodes.{node_id}")
}

fn group_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
    format!("group.roles.{role_id}.assigned_users.{user_id}")
}

fn realm_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
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
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use aruna_core::alpn::Alpn;
    use aruna_core::document::{
        DocumentSyncChangeKind, DocumentSyncConflict, DocumentSyncRevision,
    };
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
        DOCUMENT_SYNC_CONFLICT_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, GROUP_KEYSPACE,
        METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
        METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
        METADATA_GRAPH_PRUNE_JOB_KEYSPACE, METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE,
        USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
    };
    use aruna_core::metadata::MetadataCreateEventPayload;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
        document_sync_conflict_key, metadata_document_key, metadata_event_log_key,
        metadata_registry_key, subject_index_key, subject_index_value,
    };
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, MetadataReplicationConfig, OidcProviderConfig,
        Permission, RealmAuthorizationDocument, RealmConfigDocument, RealmDiscoveryConfig, RealmId,
        RealmNodeKind, Role, StaticRealmEndpoint,
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

    fn revision_change_at(
        base: Option<DocumentSyncRevision>,
        generation: u64,
        seq: u128,
    ) -> DocumentSyncChange {
        DocumentSyncChange {
            base,
            current: DocumentSyncRevision {
                generation,
                event_id: Ulid::from_parts(1_727_000_000_000 + generation, seq),
                actor: node((seq % 200) as u8 + 1),
                updated_at_ms: 1_727_000_000_100 + generation,
            },
            kind: DocumentSyncChangeKind::Upsert,
        }
    }

    fn revision_change() -> DocumentSyncChange {
        revision_change_at(None, 1, 43)
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

    fn test_actor(seed: u8, user_id: UserId, realm_id: RealmId) -> Actor {
        Actor {
            node_id: node(seed),
            user_id,
            realm_id,
        }
    }

    fn test_role(role_id: Ulid, assigned_users: impl IntoIterator<Item = UserId>) -> Role {
        Role {
            role_id,
            name: "member".to_string(),
            permissions: HashMap::from([("/datasets".to_string(), Permission::READ)]),
            assigned_users: assigned_users.into_iter().collect(),
        }
    }

    fn test_admin_role_definition(
        role_id: Ulid,
        name: &str,
        path: &str,
        permission: Permission,
    ) -> AdminDocumentRoleDefinition {
        AdminDocumentRoleDefinition {
            role_id,
            name: name.to_string(),
            permissions: BTreeMap::from([(path.to_string(), permission)]),
        }
    }

    fn test_admin_event(
        event_id: Ulid,
        target: AdminDocumentTarget,
        actor: &Actor,
        origin_seq: u64,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        AdminDocumentEvent {
            event_id,
            target,
            origin_node_id: actor.node_id,
            origin_seq,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op,
        }
    }

    async fn read_user_doc(storage: &StorageHandle, user_id: UserId) -> User {
        let target = DocumentSyncTarget::User { user_id };
        let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("user exists");
        User::from_bytes(&value).expect("user decodes")
    }

    async fn read_user_revision(storage: &StorageHandle, user_id: UserId) -> DocumentSyncChange {
        let target = DocumentSyncTarget::User { user_id };
        let value = read_storage_value(
            storage,
            DOCUMENT_SYNC_REVISION_KEYSPACE,
            document_sync_revision_key(&target),
        )
        .await
        .expect("user revision exists");
        postcard::from_bytes(&value).expect("user revision decodes")
    }

    async fn read_user_conflict(
        storage: &StorageHandle,
        user_id: UserId,
    ) -> Option<DocumentSyncConflict> {
        let target = DocumentSyncTarget::User { user_id };
        read_storage_value(
            storage,
            DOCUMENT_SYNC_CONFLICT_KEYSPACE,
            document_sync_conflict_key(&target),
        )
        .await
        .map(|value| postcard::from_bytes(&value).expect("user conflict decodes"))
    }

    fn test_user_doc(user_id: UserId, name: &str, subjects: &[&str]) -> User {
        User {
            user_id,
            name: name.to_string(),
            subject_ids: subjects.iter().map(|subject| subject.to_string()).collect(),
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        }
    }

    async fn write_user_with_revision(
        storage: &StorageHandle,
        user: &User,
        actor: &Actor,
        change: &DocumentSyncChange,
    ) {
        let target = DocumentSyncTarget::User {
            user_id: user.user_id,
        };
        let mut writes = vec![target_write_entry(
            target.clone(),
            user.to_bytes(actor).expect("user serializes").into(),
        )];
        writes.extend(subject_index_writes(user));
        writes.push(document_sync_revision_write_entry(&target, change).expect("revision writes"));
        storage_batch_write_to(storage, writes)
            .await
            .expect("user and revision write");
    }

    async fn read_group_doc(storage: &StorageHandle, group_id: Ulid) -> Group {
        let target = DocumentSyncTarget::Group { group_id };
        let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("group exists");
        Group::from_bytes(&value).expect("group decodes")
    }

    async fn read_group_auth_doc(
        storage: &StorageHandle,
        group_id: Ulid,
    ) -> GroupAuthorizationDocument {
        let target = DocumentSyncTarget::GroupAuthorization { group_id };
        let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("group auth doc exists");
        GroupAuthorizationDocument::from_bytes(&value).expect("group auth doc decodes")
    }

    async fn read_realm_auth_doc(
        storage: &StorageHandle,
        realm_id: RealmId,
    ) -> RealmAuthorizationDocument {
        let target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("realm auth doc exists");
        RealmAuthorizationDocument::from_bytes(&value).expect("realm auth doc decodes")
    }

    async fn read_realm_config_doc(
        storage: &StorageHandle,
        realm_id: RealmId,
    ) -> RealmConfigDocument {
        let target = DocumentSyncTarget::RealmConfig { realm_id };
        let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("realm config doc exists");
        RealmConfigDocument::from_bytes(&value).expect("realm config doc decodes")
    }

    fn realm_config_nodes(config: &RealmConfigDocument) -> BTreeMap<String, RealmNodeKind> {
        config
            .nodes
            .iter()
            .map(|node| (node.node_id.clone(), node.kind.clone()))
            .collect()
    }

    fn realm_config_oidc_providers(
        config: &RealmConfigDocument,
    ) -> BTreeMap<String, OidcProviderConfig> {
        config
            .oidc_providers
            .iter()
            .map(|provider| (provider.id.clone(), provider.clone()))
            .collect()
    }

    fn test_oidc_provider(id: &str, issuer_suffix: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            issuer: format!("https://issuer.example/{issuer_suffix}"),
            audience: "aruna".to_string(),
            discovery_url: format!(
                "https://issuer.example/{issuer_suffix}/.well-known/openid-configuration"
            ),
        }
    }

    fn test_discovery(node_seed: u8, endpoint_addr: &str) -> RealmDiscoveryConfig {
        RealmDiscoveryConfig::Static {
            endpoints: vec![StaticRealmEndpoint {
                node_id: node(node_seed).to_string(),
                endpoint_addr: endpoint_addr.to_string(),
            }],
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

    async fn read_graph_lifecycle_record(
        storage: &StorageHandle,
        graph_iri: &str,
    ) -> Option<MetadataGraphLifecycleRecord> {
        read_storage_value(
            storage,
            METADATA_GRAPH_LIFECYCLE_KEYSPACE,
            metadata_graph_lifecycle_key(graph_iri),
        )
        .await
        .map(|value| postcard::from_bytes(&value).expect("graph lifecycle record decodes"))
    }

    async fn write_document_lifecycle_record(
        storage: &StorageHandle,
        lifecycle: &MetadataDocumentLifecycleRecord,
    ) {
        storage_batch_write_to(
            storage,
            vec![
                metadata_document_lifecycle_write_entry(lifecycle)
                    .expect("document lifecycle entry builds"),
            ],
        )
        .await
        .expect("document lifecycle writes");
    }

    async fn assert_registry_record_present(
        storage: &StorageHandle,
        record: &MetadataRegistryRecord,
    ) {
        let primary = read_registry_record(
            storage,
            METADATA_INDEX_KEYSPACE,
            metadata_registry_key(record.group_id, record.document_id),
        )
        .await;
        let document_index = read_registry_record(
            storage,
            METADATA_DOCUMENT_INDEX_KEYSPACE,
            metadata_document_key(record.document_id),
        )
        .await;
        let holder_value = read_storage_value(
            storage,
            METADATA_HOLDERS_KEYSPACE,
            metadata_registry_key(record.group_id, record.document_id),
        )
        .await
        .expect("holder index exists");
        let holders: Vec<NodeId> = postcard::from_bytes(&holder_value).expect("holders decode");

        assert_eq!(primary, *record);
        assert_eq!(document_index, *record);
        assert_eq!(holders, record.holder_node_ids);
    }

    async fn assert_registry_record_deleted(
        storage: &StorageHandle,
        group_id: Ulid,
        document_id: Ulid,
    ) {
        assert!(
            read_storage_value(
                storage,
                METADATA_INDEX_KEYSPACE,
                metadata_registry_key(group_id, document_id),
            )
            .await
            .is_none()
        );
        assert!(
            read_storage_value(
                storage,
                METADATA_DOCUMENT_INDEX_KEYSPACE,
                metadata_document_key(document_id),
            )
            .await
            .is_none()
        );
        assert!(
            read_storage_value(
                storage,
                METADATA_HOLDERS_KEYSPACE,
                metadata_registry_key(group_id, document_id),
            )
            .await
            .is_none()
        );
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
    async fn missing_realm_config_admin_op_stores_reducer_state_without_config_doc() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([40; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_290, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let reducer_node = node(10);

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_291, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: reducer_node,
                    kind: RealmNodeKind::Management,
                },
            ),
        )
        .await
        .expect("realm config node ensure applies without config doc");

        assert!(
            read_storage_value(
                &storage,
                document_target.storage_keyspace(),
                document_target.storage_key(),
            )
            .await
            .is_none()
        );
        let state_value = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&state_value).expect("reducer state decodes");
        assert_eq!(
            reducer_state.materialized_realm_config_nodes()[&reducer_node],
            RealmNodeKind::Management
        );
    }

    #[tokio::test]
    async fn realm_config_settings_admin_op_materializes_existing_config() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([48; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_370, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let legacy_provider = test_oidc_provider("legacy", "legacy-settings");
        let seed_node = node(20);
        let mut seed_config = RealmConfigDocument::new(realm_id, vec![legacy_provider.clone()], 3);
        seed_config.discovery = test_discovery(21, "https://legacy-settings.example:443");
        seed_config.ensure_node(seed_node, RealmNodeKind::Server);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&actor)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        let metadata_replication = MetadataReplicationConfig::new(9);
        let discovery = test_discovery(22, "https://reducer-settings.example:443");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_371, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: metadata_replication.clone(),
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("realm config settings apply");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.metadata_replication, metadata_replication);
        assert_eq!(config.discovery, discovery);
        assert_eq!(config.oidc_providers, vec![legacy_provider]);
        assert_eq!(
            realm_config_nodes(&config),
            realm_config_nodes(&seed_config)
        );
        let state_value = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&state_value).expect("reducer state decodes");
        assert_eq!(
            reducer_state.materialized_realm_config_metadata_replication(),
            Some(metadata_replication)
        );
        assert_eq!(
            reducer_state.materialized_realm_config_discovery(),
            Some(discovery)
        );
    }

    #[tokio::test]
    async fn missing_realm_config_settings_ops_store_state_and_conflicts_without_config_doc() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([49; 32]);
        let actor_a = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_380, 1), realm_id),
            realm_id,
        );
        let actor_b = test_actor(
            9,
            UserId::local(Ulid::from_parts(1_381, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let first_metadata = MetadataReplicationConfig::new(5);
        let second_metadata = MetadataReplicationConfig::new(7);
        let discovery = test_discovery(23, "https://missing-settings.example:443");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_382, 1),
                target.clone(),
                &actor_a,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: first_metadata,
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("first realm config settings op applies without config doc");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_383, 1),
                target.clone(),
                &actor_b,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: second_metadata,
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("conflicting realm config settings op applies without config doc");

        assert!(
            read_storage_value(
                &storage,
                document_target.storage_keyspace(),
                document_target.storage_key(),
            )
            .await
            .is_none()
        );
        let state_value = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&state_value).expect("reducer state decodes");
        assert_eq!(
            reducer_state.materialized_realm_config_metadata_replication(),
            None
        );
        assert_eq!(
            reducer_state.materialized_realm_config_discovery(),
            Some(discovery)
        );
        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                admin_document_reducer_conflict_key(
                    &target,
                    REALM_CONFIG_METADATA_REPLICATION_PATH,
                ),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn late_legacy_realm_config_upsert_preserves_reducer_settings() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([50; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_390, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let metadata_replication = MetadataReplicationConfig::new(11);
        let discovery = test_discovery(24, "https://reducer-late-settings.example:443");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_391, 1),
                target,
                &actor,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: metadata_replication.clone(),
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("realm config settings apply without config doc");

        let legacy_provider = test_oidc_provider("legacy", "legacy-late-settings");
        let legacy_node = node(25);
        let mut late_legacy = RealmConfigDocument::new(realm_id, vec![legacy_provider.clone()], 3);
        late_legacy.discovery = test_discovery(26, "https://legacy-late-settings.example:443");
        late_legacy.ensure_node(legacy_node, RealmNodeKind::Server);
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            document_target,
            late_legacy
                .to_bytes(&actor)
                .expect("legacy realm config serializes"),
        )
        .await
        .expect("late legacy realm config upsert overlays reducer settings");

        let merged = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(merged.metadata_replication, metadata_replication);
        assert_eq!(merged.discovery, discovery);
        assert_eq!(merged.oidc_providers, vec![legacy_provider]);
        assert_eq!(
            realm_config_nodes(&merged),
            BTreeMap::from([(legacy_node.to_string(), RealmNodeKind::Server)])
        );
    }

    #[tokio::test]
    async fn concurrent_realm_config_settings_conflict_withholds_conflicted_metadata() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([51; 32]);
        let actor_a = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_400, 1), realm_id),
            realm_id,
        );
        let actor_b = test_actor(
            9,
            UserId::local(Ulid::from_parts(1_401, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&actor_a)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        let first_metadata = MetadataReplicationConfig::new(5);
        let second_metadata = MetadataReplicationConfig::new(7);
        let discovery = test_discovery(27, "https://conflict-settings.example:443");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_402, 1),
                target.clone(),
                &actor_a,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: first_metadata.clone(),
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("first realm config settings op applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(1_403, 1),
                target.clone(),
                &actor_b,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: second_metadata.clone(),
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("conflicting realm config settings op applies");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.metadata_replication, first_metadata);
        assert_ne!(config.metadata_replication, second_metadata);
        assert_eq!(config.discovery, discovery);
        let state_value = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&state_value).expect("reducer state decodes");
        assert_eq!(
            reducer_state.materialized_realm_config_metadata_replication(),
            None
        );
        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                admin_document_reducer_conflict_key(
                    &target,
                    REALM_CONFIG_METADATA_REPLICATION_PATH,
                ),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn realm_config_node_ensure_admin_ops_merge_nodes() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([41; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(300, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let first_node = node(11);
        let second_node = node(12);

        let seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&actor)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        for (seq, node_id, kind) in [
            (1, first_node, RealmNodeKind::Management),
            (2, second_node, RealmNodeKind::Server),
        ] {
            apply_admin_document_operation_to_storage(
                &storage,
                document_target.clone(),
                test_admin_event(
                    Ulid::from_parts(1_300 + seq, 1),
                    target.clone(),
                    &actor,
                    seq,
                    AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
                ),
            )
            .await
            .expect("realm config node ensure applies");
        }

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(
            realm_config_nodes(&config),
            BTreeMap::from([
                (first_node.to_string(), RealmNodeKind::Management),
                (second_node.to_string(), RealmNodeKind::Server),
            ])
        );
    }

    #[tokio::test]
    async fn realm_config_oidc_provider_admin_ops_merge_disjoint_updates() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([44; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(330, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let legacy = test_oidc_provider("legacy", "legacy");
        let removed = test_oidc_provider("removed", "removed");
        let first = test_oidc_provider("default", "one");
        let second = test_oidc_provider("partner", "two");
        let seed_node = node(16);
        let mut seed_config =
            RealmConfigDocument::new(realm_id, vec![legacy.clone(), removed.clone()], 5);
        seed_config.discovery = RealmDiscoveryConfig::Static {
            endpoints: vec![StaticRealmEndpoint {
                node_id: seed_node.to_string(),
                endpoint_addr: "https://seed.example:443".to_string(),
            }],
        };
        seed_config.ensure_node(seed_node, RealmNodeKind::Server);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&actor)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        for (seq, op) in [
            (
                1,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                    provider: first.clone(),
                },
            ),
            (
                2,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                    provider: second.clone(),
                },
            ),
            (
                3,
                AdminDocumentOperation::RealmConfigOidcProviderRemoved {
                    provider_id: removed.id.clone(),
                },
            ),
        ] {
            apply_admin_document_operation_to_storage(
                &storage,
                document_target.clone(),
                test_admin_event(
                    Ulid::from_parts(1_330 + seq, 1),
                    target.clone(),
                    &actor,
                    seq,
                    op,
                ),
            )
            .await
            .expect("realm config OIDC provider op applies");
        }

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(
            config.metadata_replication,
            seed_config.metadata_replication
        );
        assert_eq!(config.discovery, seed_config.discovery);
        assert_eq!(
            realm_config_nodes(&config),
            realm_config_nodes(&seed_config)
        );
        assert_eq!(
            realm_config_oidc_providers(&config),
            BTreeMap::from([
                ("default".to_string(), first),
                ("legacy".to_string(), legacy),
                ("partner".to_string(), second),
            ])
        );
    }

    #[tokio::test]
    async fn concurrent_realm_config_oidc_provider_conflict_withholds_provider() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([45; 32]);
        let actor_a = test_actor(
            8,
            UserId::local(Ulid::from_parts(340, 1), realm_id),
            realm_id,
        );
        let actor_b = test_actor(
            9,
            UserId::local(Ulid::from_parts(341, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let untouched = test_oidc_provider("untouched", "untouched");
        let first = test_oidc_provider("default", "one");
        let second = test_oidc_provider("default", "two");
        let seed_config = RealmConfigDocument::new(
            realm_id,
            vec![test_oidc_provider("default", "seed"), untouched.clone()],
            3,
        );
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&actor_a)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_340, 1),
                target.clone(),
                &actor_a,
                1,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider: first },
            ),
        )
        .await
        .expect("first realm config OIDC provider upsert applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(1_341, 1),
                target.clone(),
                &actor_b,
                1,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider: second },
            ),
        )
        .await
        .expect("conflicting realm config OIDC provider upsert applies");

        let config = read_realm_config_doc(&storage, realm_id).await;
        let providers = realm_config_oidc_providers(&config);
        assert!(!providers.contains_key("default"));
        assert_eq!(providers.get("untouched"), Some(&untouched));
        let path = "realm_config.oidc_providers.default".to_string();
        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                admin_document_reducer_conflict_key(&target, &path),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn late_legacy_realm_config_upsert_preserves_reducer_oidc_provider() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([46; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(350, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let reducer_provider = test_oidc_provider("reducer", "reducer");
        let legacy_provider = test_oidc_provider("legacy", "legacy");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_350, 1),
                target,
                &actor,
                1,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                    provider: reducer_provider.clone(),
                },
            ),
        )
        .await
        .expect("realm config OIDC provider upsert applies");

        let late_legacy = RealmConfigDocument::new(realm_id, vec![legacy_provider.clone()], 7);
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            document_target,
            late_legacy
                .to_bytes(&actor)
                .expect("legacy realm config serializes"),
        )
        .await
        .expect("late legacy realm config upsert overlays reducer state");

        let merged = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(
            merged.metadata_replication,
            late_legacy.metadata_replication
        );
        assert_eq!(merged.discovery, late_legacy.discovery);
        assert_eq!(
            realm_config_nodes(&merged),
            realm_config_nodes(&late_legacy)
        );
        assert_eq!(
            realm_config_oidc_providers(&merged),
            BTreeMap::from([
                ("legacy".to_string(), legacy_provider),
                ("reducer".to_string(), reducer_provider),
            ])
        );
    }

    #[tokio::test]
    async fn missing_realm_config_oidc_provider_op_stores_state_without_config_doc() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([47; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(360, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let provider = test_oidc_provider("default", "missing");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_360, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                    provider: provider.clone(),
                },
            ),
        )
        .await
        .expect("realm config OIDC provider upsert applies without config doc");

        assert!(
            read_storage_value(
                &storage,
                document_target.storage_keyspace(),
                document_target.storage_key(),
            )
            .await
            .is_none()
        );
        let state_value = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&state_value).expect("reducer state decodes");
        assert_eq!(
            reducer_state.materialized_realm_config_oidc_providers(),
            BTreeMap::from([("default".to_string(), provider)])
        );
    }

    #[tokio::test]
    async fn late_legacy_realm_config_upsert_preserves_reducer_node() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([42; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(310, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let reducer_node = node(13);
        let legacy_node = node(14);

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_400, 1),
                target,
                &actor,
                1,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: reducer_node,
                    kind: RealmNodeKind::Management,
                },
            ),
        )
        .await
        .expect("realm config node ensure applies");

        let mut late_legacy = RealmConfigDocument::new(
            realm_id,
            vec![OidcProviderConfig {
                id: "legacy-provider".to_string(),
                issuer: "https://issuer.example".to_string(),
                audience: "legacy-audience".to_string(),
                discovery_url: "https://issuer.example/.well-known/openid-configuration"
                    .to_string(),
            }],
            7,
        );
        late_legacy.discovery = RealmDiscoveryConfig::Static {
            endpoints: vec![StaticRealmEndpoint {
                node_id: legacy_node.to_string(),
                endpoint_addr: "https://legacy.example:443".to_string(),
            }],
        };
        late_legacy.ensure_node(legacy_node, RealmNodeKind::Server);
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            document_target,
            late_legacy
                .to_bytes(&actor)
                .expect("legacy realm config serializes"),
        )
        .await
        .expect("late legacy realm config upsert overlays reducer state");

        let merged = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(
            merged.metadata_replication,
            late_legacy.metadata_replication
        );
        assert_eq!(merged.oidc_providers, late_legacy.oidc_providers);
        assert_eq!(merged.discovery, late_legacy.discovery);
        assert_eq!(
            realm_config_nodes(&merged),
            BTreeMap::from([
                (legacy_node.to_string(), RealmNodeKind::Server),
                (reducer_node.to_string(), RealmNodeKind::Management),
            ])
        );
    }

    #[tokio::test]
    async fn concurrent_realm_config_node_kind_conflict_withholds_node() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([43; 32]);
        let actor_a = test_actor(
            8,
            UserId::local(Ulid::from_parts(320, 1), realm_id),
            realm_id,
        );
        let actor_b = test_actor(
            9,
            UserId::local(Ulid::from_parts(321, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let conflicted_node = node(15);

        let seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&actor_a)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_500, 1),
                target.clone(),
                &actor_a,
                1,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: conflicted_node,
                    kind: RealmNodeKind::Management,
                },
            ),
        )
        .await
        .expect("first realm config node ensure applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(1_501, 1),
                target.clone(),
                &actor_b,
                1,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: conflicted_node,
                    kind: RealmNodeKind::Server,
                },
            ),
        )
        .await
        .expect("conflicting realm config node ensure applies");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert!(!realm_config_nodes(&config).contains_key(&conflicted_node.to_string()));
        let path = realm_config_node_path(&conflicted_node);
        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                admin_document_reducer_conflict_key(&target, &path),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn legacy_user_upsert_merges_with_reducer_state_and_bootstraps_without_state() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([21; 32]);
        let user_id = UserId::local(Ulid::from_parts(100, 1), realm_id);
        let actor = test_actor(8, user_id, realm_id);
        let target = AdminDocumentTarget::User { user_id };
        for (seq, op) in [
            (
                1,
                AdminDocumentOperation::UserNameSet {
                    name: "Reduced".to_string(),
                },
            ),
            (
                2,
                AdminDocumentOperation::UserSubjectIdAdded {
                    subject_id: "reduced-subject".to_string(),
                },
            ),
            (
                3,
                AdminDocumentOperation::UserSubjectIdRemoved {
                    subject_id: "removed-subject".to_string(),
                },
            ),
            (
                4,
                AdminDocumentOperation::UserAttributeSet {
                    key: "source".to_string(),
                    value: "reducer".to_string(),
                },
            ),
            (
                5,
                AdminDocumentOperation::UserAttributeRemoved {
                    key: "removed".to_string(),
                },
            ),
        ] {
            apply_admin_document_operation_to_storage(
                &storage,
                DocumentSyncTarget::User { user_id },
                test_admin_event(
                    Ulid::from_parts(1_000 + seq, 1),
                    target.clone(),
                    &actor,
                    seq,
                    op,
                ),
            )
            .await
            .expect("admin user operation applies");
        }

        let alias_user_id = UserId::local(Ulid::from_parts(102, 1), realm_id);
        let late_legacy = User {
            user_id,
            name: "Legacy".to_string(),
            subject_ids: vec!["legacy-subject".to_string(), "removed-subject".to_string()],
            alias_user_ids: HashSet::from([alias_user_id]),
            attributes: HashMap::from([
                ("legacy-only".to_string(), "preserved".to_string()),
                ("removed".to_string(), "legacy".to_string()),
                ("source".to_string(), "legacy".to_string()),
            ]),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            DocumentSyncTarget::User { user_id },
            late_legacy.to_bytes(&actor).expect("legacy serializes"),
        )
        .await
        .expect("late legacy upsert merges with reducer state");

        let merged = read_user_doc(&storage, user_id).await;
        assert_eq!(merged.name, "Reduced");
        assert_eq!(merged.alias_user_ids, HashSet::from([alias_user_id]));
        assert_eq!(
            merged.subject_ids.iter().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from(["legacy-subject".to_string(), "reduced-subject".to_string()])
        );
        assert_eq!(merged.attributes["legacy-only"], "preserved");
        assert_eq!(merged.attributes["source"], "reducer");
        assert!(!merged.attributes.contains_key("removed"));
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("legacy-subject")
            )
            .await,
            Some(subject_index_value(user_id))
        );
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("reduced-subject")
            )
            .await,
            Some(subject_index_value(user_id))
        );
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("removed-subject")
            )
            .await,
            None
        );

        let bootstrap_user_id = UserId::local(Ulid::from_parts(101, 1), realm_id);
        let bootstrap_actor = test_actor(9, bootstrap_user_id, realm_id);
        let bootstrap = User {
            user_id: bootstrap_user_id,
            name: "Bootstrap".to_string(),
            subject_ids: vec!["bootstrap-subject".to_string()],
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            DocumentSyncTarget::User {
                user_id: bootstrap_user_id,
            },
            bootstrap
                .to_bytes(&bootstrap_actor)
                .expect("bootstrap serializes"),
        )
        .await
        .expect("bootstrap legacy user applies");

        assert_eq!(read_user_doc(&storage, bootstrap_user_id).await, bootstrap);
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("bootstrap-subject")
            )
            .await,
            Some(subject_index_value(bootstrap_user_id))
        );
    }

    #[tokio::test]
    async fn revisioned_user_successor_upsert_applies_and_writes_sidecar() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([31; 32]);
        let user_id = UserId::local(Ulid::from_parts(201, 1), realm_id);
        let actor = test_actor(8, user_id, realm_id);
        let local_change = revision_change_at(None, 1, 201);
        let incoming_change = revision_change_at(Some(local_change.current), 2, 202);
        let local = test_user_doc(user_id, "Local", &["old-subject"]);
        write_user_with_revision(&storage, &local, &actor, &local_change).await;

        let incoming = test_user_doc(user_id, "Incoming", &["new-subject"]);
        apply_revisioned_user_upsert_to_storage(
            &storage,
            user_id,
            incoming.to_bytes(&actor).expect("incoming serializes"),
            incoming_change,
        )
        .await
        .expect("successor revision applies");

        assert_eq!(read_user_doc(&storage, user_id).await, incoming);
        assert_eq!(read_user_revision(&storage, user_id).await, incoming_change);
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("old-subject")
            )
            .await,
            None
        );
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("new-subject")
            )
            .await,
            Some(subject_index_value(user_id))
        );
    }

    #[tokio::test]
    async fn revisioned_user_stale_upsert_skips_without_overwrite() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([32; 32]);
        let user_id = UserId::local(Ulid::from_parts(202, 1), realm_id);
        let actor = test_actor(8, user_id, realm_id);
        let local_change = revision_change_at(None, 2, 203);
        let stale_change = revision_change_at(None, 1, 204);
        let local = test_user_doc(user_id, "Local", &["local-subject"]);
        write_user_with_revision(&storage, &local, &actor, &local_change).await;

        let stale = test_user_doc(user_id, "Stale", &["stale-subject"]);
        apply_revisioned_user_upsert_to_storage(
            &storage,
            user_id,
            stale.to_bytes(&actor).expect("stale serializes"),
            stale_change,
        )
        .await
        .expect("stale revision is a successful skip");

        assert_eq!(read_user_doc(&storage, user_id).await, local);
        assert_eq!(read_user_revision(&storage, user_id).await, local_change);
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("stale-subject")
            )
            .await,
            None
        );
        assert_eq!(read_user_conflict(&storage, user_id).await, None);
    }

    #[tokio::test]
    async fn revisioned_user_unbased_concurrent_upsert_records_conflict_without_overwrite() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([33; 32]);
        let user_id = UserId::local(Ulid::from_parts(203, 1), realm_id);
        let actor = test_actor(8, user_id, realm_id);
        let local_change = revision_change_at(None, 1, 205);
        let concurrent_change = revision_change_at(None, 2, 206);
        let local = test_user_doc(user_id, "Local", &["local-subject"]);
        write_user_with_revision(&storage, &local, &actor, &local_change).await;

        let concurrent = test_user_doc(user_id, "Concurrent", &["concurrent-subject"]);
        apply_revisioned_user_upsert_to_storage(
            &storage,
            user_id,
            concurrent.to_bytes(&actor).expect("concurrent serializes"),
            concurrent_change,
        )
        .await
        .expect("conflicting revision is a successful skip");

        assert_eq!(read_user_doc(&storage, user_id).await, local);
        assert_eq!(read_user_revision(&storage, user_id).await, local_change);
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("concurrent-subject")
            )
            .await,
            None
        );
        let conflict = read_user_conflict(&storage, user_id)
            .await
            .expect("conflict sidecar exists");
        assert_eq!(conflict.target, DocumentSyncTarget::User { user_id });
        assert_eq!(conflict.local_change, Some(local_change));
        assert_eq!(
            conflict.local_bytes,
            Some(local.to_bytes(&actor).expect("local serializes"))
        );
        assert_eq!(conflict.incoming_change, concurrent_change);
        assert_eq!(
            conflict.incoming_bytes,
            concurrent.to_bytes(&actor).expect("concurrent serializes")
        );
    }

    #[tokio::test]
    async fn revisioned_user_upsert_preserves_reducer_overlay() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([34; 32]);
        let user_id = UserId::local(Ulid::from_parts(204, 1), realm_id);
        let actor = test_actor(8, user_id, realm_id);
        let target = AdminDocumentTarget::User { user_id };
        for (seq, op) in [
            (
                1,
                AdminDocumentOperation::UserNameSet {
                    name: "Reduced".to_string(),
                },
            ),
            (
                2,
                AdminDocumentOperation::UserSubjectIdAdded {
                    subject_id: "reduced-subject".to_string(),
                },
            ),
            (
                3,
                AdminDocumentOperation::UserAttributeSet {
                    key: "source".to_string(),
                    value: "reducer".to_string(),
                },
            ),
        ] {
            apply_admin_document_operation_to_storage(
                &storage,
                DocumentSyncTarget::User { user_id },
                test_admin_event(
                    Ulid::from_parts(2_000 + seq, 1),
                    target.clone(),
                    &actor,
                    seq,
                    op,
                ),
            )
            .await
            .expect("admin user operation applies");
        }

        let mut incoming = test_user_doc(user_id, "Remote", &["remote-subject"]);
        incoming.attributes = HashMap::from([
            ("legacy-only".to_string(), "preserved".to_string()),
            ("source".to_string(), "remote".to_string()),
        ]);
        let incoming_change = revision_change_at(None, 1, 207);
        apply_revisioned_user_upsert_to_storage(
            &storage,
            user_id,
            incoming.to_bytes(&actor).expect("incoming serializes"),
            incoming_change,
        )
        .await
        .expect("revisioned user upsert applies with reducer overlay");

        let merged = read_user_doc(&storage, user_id).await;
        assert_eq!(merged.name, "Reduced");
        assert_eq!(merged.attributes["legacy-only"], "preserved");
        assert_eq!(merged.attributes["source"], "reducer");
        assert_eq!(
            merged.subject_ids.iter().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from(["remote-subject".to_string(), "reduced-subject".to_string()])
        );
        assert_eq!(read_user_revision(&storage, user_id).await, incoming_change);
        assert_eq!(
            read_storage_value(
                &storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key("reduced-subject")
            )
            .await,
            Some(subject_index_value(user_id))
        );
    }

    #[tokio::test]
    async fn legacy_group_auth_upsert_merges_with_reducer_state_and_bootstraps_without_state() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([22; 32]);
        let group_id = Ulid::from_parts(110, 1);
        let role_id = Ulid::from_parts(111, 1);
        let assigned_user_id = UserId::local(Ulid::from_parts(112, 1), realm_id);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(113, 1), realm_id),
            realm_id,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            test_admin_event(
                Ulid::from_parts(117, 1),
                AdminDocumentTarget::Group { group_id },
                &actor,
                1,
                AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                    role_id,
                    user_id: assigned_user_id,
                },
            ),
        )
        .await
        .expect("group assignment reducer state writes");

        let legacy_user_id = UserId::local(Ulid::from_parts(118, 1), realm_id);
        let mut legacy_role = test_role(role_id, [legacy_user_id]);
        legacy_role.name = "legacy-member".to_string();
        legacy_role.permissions = HashMap::from([("/legacy".to_string(), Permission::WRITE)]);
        let late_legacy = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(role_id, legacy_role.clone())]),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            late_legacy
                .to_bytes(&actor)
                .expect("legacy group auth serializes"),
        )
        .await
        .expect("late legacy group auth upsert merges with reducer state");

        let merged = read_group_auth_doc(&storage, group_id).await;
        let merged_role = &merged.roles[&role_id];
        assert_eq!(merged_role.name, legacy_role.name);
        assert_eq!(merged_role.permissions, legacy_role.permissions);
        assert!(merged_role.assigned_users.contains(&assigned_user_id));
        assert!(merged_role.assigned_users.contains(&legacy_user_id));

        let bootstrap_group_id = Ulid::from_parts(114, 1);
        let bootstrap_role_id = Ulid::from_parts(115, 1);
        let bootstrap_user_id = UserId::local(Ulid::from_parts(116, 1), realm_id);
        let bootstrap = GroupAuthorizationDocument {
            group_id: bootstrap_group_id,
            roles: HashMap::from([(
                bootstrap_role_id,
                test_role(bootstrap_role_id, [bootstrap_user_id]),
            )]),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization {
                group_id: bootstrap_group_id,
            },
            bootstrap
                .to_bytes(&actor)
                .expect("bootstrap group auth serializes"),
        )
        .await
        .expect("bootstrap legacy group auth applies");

        assert_eq!(
            read_group_auth_doc(&storage, bootstrap_group_id).await,
            bootstrap
        );
    }

    #[tokio::test]
    async fn group_role_create_admin_operation_updates_existing_group_roles() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([27; 32]);
        let group_id = Ulid::from_parts(160, 1);
        let existing_role_id = Ulid::from_parts(161, 1);
        let role_id = Ulid::from_parts(162, 1);
        let conflicted_role_id = Ulid::from_parts(163, 1);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(164, 1), realm_id),
            realm_id,
        );
        let group = Group {
            display_name: "Durable group".to_string(),
            group_id,
            realm_id,
            roles: HashSet::from([existing_role_id, conflicted_role_id]),
        };
        storage_batch_write_to(
            &storage,
            vec![(
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                group.to_bytes(&actor).expect("group serializes").into(),
            )],
        )
        .await
        .expect("group writes");

        let target = AdminDocumentTarget::Group { group_id };
        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(165, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        role_id,
                        "Reduced group role",
                        "/group/reduced/**",
                        Permission::WRITE,
                    ),
                },
            ),
        )
        .await
        .expect("role create applies");

        let conflict_actor_a = test_actor(
            9,
            UserId::local(Ulid::from_parts(166, 1), realm_id),
            realm_id,
        );
        let conflict_actor_b = test_actor(
            10,
            UserId::local(Ulid::from_parts(167, 1), realm_id),
            realm_id,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(168, 1),
                target.clone(),
                &conflict_actor_a,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "First conflicted role",
                        "/group/conflict-a/**",
                        Permission::READ,
                    ),
                },
            ),
        )
        .await
        .expect("first conflict role applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(169, 1),
                target,
                &conflict_actor_b,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "Second conflicted role",
                        "/group/conflict-b/**",
                        Permission::WRITE,
                    ),
                },
            ),
        )
        .await
        .expect("second conflict role applies");

        let stored_group = read_group_doc(&storage, group_id).await;
        assert_eq!(stored_group.display_name, group.display_name);
        assert_eq!(stored_group.realm_id, realm_id);
        assert_eq!(
            stored_group.roles,
            HashSet::from([existing_role_id, role_id])
        );
    }

    #[tokio::test]
    async fn group_role_create_admin_operation_does_not_create_missing_group() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([28; 32]);
        let group_id = Ulid::from_parts(170, 1);
        let role_id = Ulid::from_parts(171, 1);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(172, 1), realm_id),
            realm_id,
        );

        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            test_admin_event(
                Ulid::from_parts(173, 1),
                AdminDocumentTarget::Group { group_id },
                &actor,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        role_id,
                        "Reduced group role",
                        "/group/reduced/**",
                        Permission::WRITE,
                    ),
                },
            ),
        )
        .await
        .expect("role create applies");

        assert_eq!(
            read_storage_value(&storage, GROUP_KEYSPACE, group_id.to_bytes().into()).await,
            None
        );
        assert!(
            read_group_auth_doc(&storage, group_id)
                .await
                .roles
                .contains_key(&role_id)
        );
    }

    #[tokio::test]
    async fn late_legacy_group_upsert_overlays_reducer_role_set_and_conflicts() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([29; 32]);
        let group_id = Ulid::from_parts(180, 1);
        let role_id = Ulid::from_parts(181, 1);
        let conflicted_role_id = Ulid::from_parts(182, 1);
        let legacy_role_id = Ulid::from_parts(183, 1);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(184, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::Group { group_id };
        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(185, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        role_id,
                        "Reduced group role",
                        "/group/reduced/**",
                        Permission::WRITE,
                    ),
                },
            ),
        )
        .await
        .expect("role create applies");

        let conflict_actor_a = test_actor(
            9,
            UserId::local(Ulid::from_parts(186, 1), realm_id),
            realm_id,
        );
        let conflict_actor_b = test_actor(
            10,
            UserId::local(Ulid::from_parts(187, 1), realm_id),
            realm_id,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(188, 1),
                target.clone(),
                &conflict_actor_a,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "First conflicted role",
                        "/group/conflict-a/**",
                        Permission::READ,
                    ),
                },
            ),
        )
        .await
        .expect("first conflict role applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(189, 1),
                target,
                &conflict_actor_b,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "Second conflicted role",
                        "/group/conflict-b/**",
                        Permission::WRITE,
                    ),
                },
            ),
        )
        .await
        .expect("second conflict role applies");

        let late_legacy = Group {
            display_name: "Late legacy group".to_string(),
            group_id,
            realm_id,
            roles: HashSet::from([legacy_role_id, conflicted_role_id]),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            DocumentSyncTarget::Group { group_id },
            late_legacy
                .to_bytes(&actor)
                .expect("legacy group serializes"),
        )
        .await
        .expect("late legacy group upsert overlays reducer state");

        let merged = read_group_doc(&storage, group_id).await;
        assert_eq!(merged.display_name, late_legacy.display_name);
        assert_eq!(merged.realm_id, realm_id);
        assert_eq!(merged.roles, HashSet::from([legacy_role_id, role_id]));
    }

    #[tokio::test]
    async fn legacy_realm_auth_upsert_merges_with_reducer_state_and_bootstraps_without_state() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([23; 32]);
        let role_id = Ulid::from_parts(120, 1);
        let assigned_user_id = UserId::local(Ulid::from_parts(121, 1), realm_id);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(122, 1), realm_id),
            realm_id,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::RealmAuthorization { realm_id },
            test_admin_event(
                Ulid::from_parts(125, 1),
                AdminDocumentTarget::Realm { realm_id },
                &actor,
                1,
                AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                    role_id,
                    user_id: assigned_user_id,
                },
            ),
        )
        .await
        .expect("realm assignment reducer state writes");

        let legacy_user_id = UserId::local(Ulid::from_parts(126, 1), realm_id);
        let mut legacy_role = test_role(role_id, [legacy_user_id]);
        legacy_role.name = "legacy-realm-member".to_string();
        legacy_role.permissions = HashMap::from([("/realm-legacy".to_string(), Permission::WRITE)]);
        let late_legacy = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(role_id, legacy_role.clone())]),
            operation_restrictions: Default::default(),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            DocumentSyncTarget::RealmAuthorization { realm_id },
            late_legacy
                .to_bytes(&actor)
                .expect("legacy realm auth serializes"),
        )
        .await
        .expect("late legacy realm auth upsert merges with reducer state");

        let merged = read_realm_auth_doc(&storage, realm_id).await;
        let merged_role = &merged.roles[&role_id];
        assert_eq!(merged_role.name, legacy_role.name);
        assert_eq!(merged_role.permissions, legacy_role.permissions);
        assert!(merged_role.assigned_users.contains(&assigned_user_id));
        assert!(merged_role.assigned_users.contains(&legacy_user_id));

        let bootstrap_realm_id = RealmId::from_bytes([24; 32]);
        let bootstrap_role_id = Ulid::from_parts(123, 1);
        let bootstrap_user_id = UserId::local(Ulid::from_parts(124, 1), bootstrap_realm_id);
        let bootstrap = RealmAuthorizationDocument {
            realm_id: bootstrap_realm_id,
            roles: HashMap::from([(
                bootstrap_role_id,
                test_role(bootstrap_role_id, [bootstrap_user_id]),
            )]),
            operation_restrictions: Default::default(),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            DocumentSyncTarget::RealmAuthorization {
                realm_id: bootstrap_realm_id,
            },
            bootstrap
                .to_bytes(&actor)
                .expect("bootstrap realm auth serializes"),
        )
        .await
        .expect("bootstrap legacy realm auth applies");

        assert_eq!(
            read_realm_auth_doc(&storage, bootstrap_realm_id).await,
            bootstrap
        );
    }

    #[tokio::test]
    async fn late_legacy_group_auth_upsert_keeps_reducer_role_body_and_drops_conflicted_role() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([25; 32]);
        let group_id = Ulid::from_parts(130, 1);
        let role_id = Ulid::from_parts(131, 1);
        let conflicted_role_id = Ulid::from_parts(132, 1);
        let assigned_user_id = UserId::local(Ulid::from_parts(133, 1), realm_id);
        let legacy_user_id = UserId::local(Ulid::from_parts(134, 1), realm_id);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(135, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::Group { group_id };
        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

        let reducer_role = test_admin_role_definition(
            role_id,
            "Reduced group role",
            "/group/reduced/**",
            Permission::WRITE,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(136, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::GroupRoleCreated { role: reducer_role },
            ),
        )
        .await
        .expect("group role create applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(137, 1),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                    role_id,
                    user_id: assigned_user_id,
                },
            ),
        )
        .await
        .expect("group assignment applies");

        let conflict_actor_a = test_actor(
            9,
            UserId::local(Ulid::from_parts(138, 1), realm_id),
            realm_id,
        );
        let conflict_actor_b = test_actor(
            10,
            UserId::local(Ulid::from_parts(139, 1), realm_id),
            realm_id,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(140, 1),
                target.clone(),
                &conflict_actor_a,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "First conflicted group role",
                        "/group/conflict-a/**",
                        Permission::READ,
                    ),
                },
            ),
        )
        .await
        .expect("first group conflict role applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(141, 1),
                target.clone(),
                &conflict_actor_b,
                1,
                AdminDocumentOperation::GroupRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "Second conflicted group role",
                        "/group/conflict-b/**",
                        Permission::WRITE,
                    ),
                },
            ),
        )
        .await
        .expect("second group conflict role applies");

        let mut legacy_role = test_role(role_id, [legacy_user_id]);
        legacy_role.name = "legacy group role".to_string();
        legacy_role.permissions =
            HashMap::from([("/group/legacy/**".to_string(), Permission::READ)]);
        let mut legacy_conflicted_role = test_role(conflicted_role_id, [legacy_user_id]);
        legacy_conflicted_role.name = "legacy conflicted group role".to_string();
        let late_legacy = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([
                (role_id, legacy_role),
                (conflicted_role_id, legacy_conflicted_role),
            ]),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            document_target,
            late_legacy
                .to_bytes(&actor)
                .expect("legacy group auth serializes"),
        )
        .await
        .expect("late legacy group auth upsert merges with reducer state");

        let merged = read_group_auth_doc(&storage, group_id).await;
        let merged_role = &merged.roles[&role_id];
        assert_eq!(merged_role.name, "Reduced group role");
        assert_eq!(
            merged_role.permissions,
            HashMap::from([("/group/reduced/**".to_string(), Permission::WRITE)])
        );
        assert_eq!(
            merged_role.assigned_users,
            HashSet::from([legacy_user_id, assigned_user_id])
        );
        assert!(!merged.roles.contains_key(&conflicted_role_id));
    }

    #[tokio::test]
    async fn late_legacy_realm_auth_upsert_keeps_reducer_role_body_and_drops_conflicted_role() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([26; 32]);
        let role_id = Ulid::from_parts(142, 1);
        let conflicted_role_id = Ulid::from_parts(143, 1);
        let assigned_user_id = UserId::local(Ulid::from_parts(144, 1), realm_id);
        let legacy_user_id = UserId::local(Ulid::from_parts(145, 1), realm_id);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(146, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::Realm { realm_id };
        let document_target = DocumentSyncTarget::RealmAuthorization { realm_id };

        let reducer_role = test_admin_role_definition(
            role_id,
            "Reduced realm role",
            "/realm/reduced/**",
            Permission::WRITE,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(147, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmRoleCreated { role: reducer_role },
            ),
        )
        .await
        .expect("realm role create applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(148, 1),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                    role_id,
                    user_id: assigned_user_id,
                },
            ),
        )
        .await
        .expect("realm assignment applies");

        let conflict_actor_a = test_actor(
            9,
            UserId::local(Ulid::from_parts(149, 1), realm_id),
            realm_id,
        );
        let conflict_actor_b = test_actor(
            10,
            UserId::local(Ulid::from_parts(150, 1), realm_id),
            realm_id,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(151, 1),
                target.clone(),
                &conflict_actor_a,
                1,
                AdminDocumentOperation::RealmRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "First conflicted realm role",
                        "/realm/conflict-a/**",
                        Permission::READ,
                    ),
                },
            ),
        )
        .await
        .expect("first realm conflict role applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(152, 1),
                target.clone(),
                &conflict_actor_b,
                1,
                AdminDocumentOperation::RealmRoleCreated {
                    role: test_admin_role_definition(
                        conflicted_role_id,
                        "Second conflicted realm role",
                        "/realm/conflict-b/**",
                        Permission::WRITE,
                    ),
                },
            ),
        )
        .await
        .expect("second realm conflict role applies");

        let mut legacy_role = test_role(role_id, [legacy_user_id]);
        legacy_role.name = "legacy realm role".to_string();
        legacy_role.permissions =
            HashMap::from([("/realm/legacy/**".to_string(), Permission::READ)]);
        let mut legacy_conflicted_role = test_role(conflicted_role_id, [legacy_user_id]);
        legacy_conflicted_role.name = "legacy conflicted realm role".to_string();
        let late_legacy = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([
                (role_id, legacy_role),
                (conflicted_role_id, legacy_conflicted_role),
            ]),
            operation_restrictions: Default::default(),
        };
        apply_legacy_admin_document_upsert_to_storage(
            &storage,
            document_target,
            late_legacy
                .to_bytes(&actor)
                .expect("legacy realm auth serializes"),
        )
        .await
        .expect("late legacy realm auth upsert merges with reducer state");

        let merged = read_realm_auth_doc(&storage, realm_id).await;
        let merged_role = &merged.roles[&role_id];
        assert_eq!(merged_role.name, "Reduced realm role");
        assert_eq!(
            merged_role.permissions,
            HashMap::from([("/realm/reduced/**".to_string(), Permission::WRITE)])
        );
        assert_eq!(
            merged_role.assigned_users,
            HashSet::from([legacy_user_id, assigned_user_id])
        );
        assert!(!merged.roles.contains_key(&conflicted_role_id));
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
    async fn group_role_seed_then_assignment_admin_operations_materialize_existing_auth_doc() {
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

        let target = AdminDocumentTarget::Group { group_id };
        let seed_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(5, 5),
            target: target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::GroupRoleAdded { role_id },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            seed_event,
        )
        .await
        .expect("role seed applies");

        let add_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(6, 6),
            target: target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: 2,
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
        assert_eq!(
            reducer_state.materialized_group_roles(),
            BTreeSet::from([role_id])
        );
        let assignment_path = group_role_user_assignment_path(&role_id, &assigned_user_id);
        assert_eq!(
            reducer_state
                .user_subject_ids
                .get(&assignment_path)
                .and_then(|version| version.value.clone()),
            Some(assigned_user_id.to_string())
        );

        let remove_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(7, 7),
            target,
            origin_node_id: actor.node_id,
            origin_seq: 3,
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
    async fn group_role_create_admin_operation_bootstraps_auth_doc_and_overlays_assignments() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([12; 32]);
        let group_id = Ulid::from_parts(31, 1);
        let role_id = Ulid::from_parts(32, 2);
        let assigned_user_id = UserId::local(Ulid::from_parts(33, 3), realm_id);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(34, 4), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::Group { group_id };
        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(35, 5),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                    role_id,
                    user_id: assigned_user_id,
                },
            ),
        )
        .await
        .expect("assignment state applies before role exists");
        storage_batch_delete_to(
            &storage,
            vec![(
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
            )],
        )
        .await
        .expect("transient empty auth doc deletes");

        let role = test_admin_role_definition(
            role_id,
            "Group data steward",
            "/datasets/**",
            Permission::WRITE,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(36, 6),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::GroupRoleCreated { role },
            ),
        )
        .await
        .expect("role create applies without pre-existing auth doc");

        let auth_doc = read_group_auth_doc(&storage, group_id).await;
        let auth_role = &auth_doc.roles[&role_id];
        assert_eq!(auth_role.name, "Group data steward");
        assert_eq!(
            auth_role.permissions,
            HashMap::from([("/datasets/**".to_string(), Permission::WRITE)])
        );
        assert_eq!(auth_role.assigned_users, HashSet::from([assigned_user_id]));

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
        assert_eq!(
            reducer_state.materialized_group_roles(),
            BTreeSet::from([role_id])
        );
        assert_eq!(
            reducer_state.materialized_group_role_user_assignments(),
            BTreeMap::from([(role_id, BTreeSet::from([assigned_user_id]))])
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
        assert!(
            !read_group_auth_doc(&storage, group_id).await.roles[&role_id]
                .assigned_users
                .contains(&assigned_user_id)
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
    async fn realm_assignment_conflicting_add_removes_existing_grant() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([14; 32]);
        let role_id = Ulid::from_parts(51, 1);
        let assigned_user_id = UserId::local(Ulid::from_parts(52, 2), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id: UserId::local(Ulid::from_parts(53, 3), realm_id),
            realm_id,
        };
        let target = AdminDocumentTarget::Realm { realm_id };
        let document_target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let auth_doc = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(role_id, test_role(role_id, [assigned_user_id]))]),
            operation_restrictions: Default::default(),
        };
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                auth_doc
                    .to_bytes(&actor)
                    .expect("auth doc serializes")
                    .into(),
            )],
        )
        .await
        .expect("auth doc writes");

        let remove_origin = node(9);
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            AdminDocumentEvent {
                event_id: Ulid::from_parts(54, 4),
                target: target.clone(),
                origin_node_id: remove_origin,
                origin_seq: 1,
                observed: AdminDocumentClock::default(),
                actor: actor.clone(),
                op: AdminDocumentOperation::RealmRoleUserAssignmentRemoved {
                    role_id,
                    user_id: assigned_user_id,
                },
            },
        )
        .await
        .expect("remove assignment applies");
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                auth_doc
                    .to_bytes(&actor)
                    .expect("auth doc serializes")
                    .into(),
            )],
        )
        .await
        .expect("stale auth doc grant rewrites");

        let add_origin = node(10);
        let assignment_path = realm_role_user_assignment_path(&role_id, &assigned_user_id);
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            AdminDocumentEvent {
                event_id: Ulid::from_parts(55, 5),
                target: target.clone(),
                origin_node_id: add_origin,
                origin_seq: 1,
                observed: AdminDocumentClock::default(),
                actor,
                op: AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                    role_id,
                    user_id: assigned_user_id,
                },
            },
        )
        .await
        .expect("conflicting add applies");

        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                admin_document_reducer_conflict_key(&target, &assignment_path),
            )
            .await
            .is_some()
        );
        assert!(
            !read_realm_auth_doc(&storage, realm_id).await.roles[&role_id]
                .assigned_users
                .contains(&assigned_user_id)
        );
    }

    #[tokio::test]
    async fn realm_role_seed_then_assignment_admin_operations_materialize_existing_auth_doc() {
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

        let target = AdminDocumentTarget::Realm { realm_id };
        let seed_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(5, 5),
            target: target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::RealmRoleAdded { role_id },
        };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::RealmAuthorization { realm_id },
            seed_event,
        )
        .await
        .expect("role seed applies");

        let add_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(6, 6),
            target: target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: 2,
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
        assert_eq!(
            reducer_state.materialized_realm_roles(),
            BTreeSet::from([role_id])
        );
        let assignment_path = realm_role_user_assignment_path(&role_id, &assigned_user_id);
        assert_eq!(
            reducer_state
                .user_subject_ids
                .get(&assignment_path)
                .and_then(|version| version.value.clone()),
            Some(assigned_user_id.to_string())
        );

        let remove_event = AdminDocumentEvent {
            event_id: Ulid::from_parts(7, 7),
            target,
            origin_node_id: actor.node_id,
            origin_seq: 3,
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

    #[tokio::test]
    async fn realm_role_create_admin_operation_bootstraps_auth_doc_and_overlays_assignments() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([13; 32]);
        let role_id = Ulid::from_parts(41, 1);
        let assigned_user_id = UserId::local(Ulid::from_parts(42, 2), realm_id);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(43, 3), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::Realm { realm_id };
        let document_target = DocumentSyncTarget::RealmAuthorization { realm_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(44, 4),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                    role_id,
                    user_id: assigned_user_id,
                },
            ),
        )
        .await
        .expect("assignment state applies before role exists");
        storage_batch_delete_to(
            &storage,
            vec![(
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
            )],
        )
        .await
        .expect("transient empty auth doc deletes");

        let role = test_admin_role_definition(
            role_id,
            "Realm operator",
            "/realm/admin/**",
            Permission::WRITE,
        );
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(45, 5),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::RealmRoleCreated { role },
            ),
        )
        .await
        .expect("role create applies without pre-existing auth doc");

        let auth_doc = read_realm_auth_doc(&storage, realm_id).await;
        let auth_role = &auth_doc.roles[&role_id];
        assert_eq!(auth_role.name, "Realm operator");
        assert_eq!(
            auth_role.permissions,
            HashMap::from([("/realm/admin/**".to_string(), Permission::WRITE)])
        );
        assert_eq!(auth_role.assigned_users, HashSet::from([assigned_user_id]));

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
        assert_eq!(
            reducer_state.materialized_realm_roles(),
            BTreeSet::from([role_id])
        );
        assert_eq!(
            reducer_state.materialized_realm_role_user_assignments(),
            BTreeMap::from([(role_id, BTreeSet::from([assigned_user_id]))])
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

    #[test]
    fn upsert_with_revision_event_envelope_round_trips() {
        let event = DocumentSyncEvent::UpsertWithRevision {
            event_id: restart_event_id(),
            target: restart_target(),
            bytes: restart_payload(),
            change: revision_change(),
        };
        let envelope = EventEnvelope::encode_event(&event).expect("event encodes");
        let decoded = envelope
            .decode_event::<DocumentSyncEvent>()
            .expect("event decodes");

        assert_eq!(decoded, event);
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
    async fn metadata_graph_lifecycle_delete_skips_without_document_lifecycle_tombstone() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(50, 1);
        let document_id = Ulid::from_parts(51, 1);
        let record = registry_record(
            group_id,
            document_id,
            "datasets/graph-kept",
            100,
            Ulid::from_parts(52, 1),
        );
        write_registry_record(&storage, &record).await;
        let graph = MetadataGraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            group_id,
            document_id,
            200,
        );

        assert!(
            !apply_metadata_graph_lifecycle_to_storage(
                &storage,
                &graph,
                postcard::to_allocvec(&graph).expect("graph lifecycle serializes"),
            )
            .await
            .expect("graph lifecycle delete is fenced")
        );

        assert_registry_record_present(&storage, &record).await;
        assert!(
            read_graph_lifecycle_record(&storage, &graph.graph_iri)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn metadata_graph_lifecycle_delete_skips_when_document_lifecycle_is_live() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(60, 1);
        let document_id = Ulid::from_parts(61, 1);
        let live_event_id = Ulid::from_parts(62, 1);
        let record = registry_record(
            group_id,
            document_id,
            "datasets/graph-live",
            300,
            live_event_id,
        );
        write_registry_record(&storage, &record).await;
        let live_lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(metadata_create_event(
                group_id,
                document_id,
                300,
                live_event_id,
                7,
            )),
        };
        write_document_lifecycle_record(&storage, &live_lifecycle).await;
        let graph = MetadataGraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            group_id,
            document_id,
            200,
        );

        assert!(
            !apply_metadata_graph_lifecycle_to_storage(
                &storage,
                &graph,
                postcard::to_allocvec(&graph).expect("graph lifecycle serializes"),
            )
            .await
            .expect("stale graph lifecycle delete is fenced")
        );

        assert_registry_record_present(&storage, &record).await;
        assert!(
            read_graph_lifecycle_record(&storage, &graph.graph_iri)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn metadata_graph_lifecycle_delete_skips_newer_live_registry_record() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(70, 1);
        let document_id = Ulid::from_parts(71, 1);
        let live_event_id = Ulid::from_parts(74, 1);
        let record = registry_record(
            group_id,
            document_id,
            "datasets/graph-newer-live",
            300,
            live_event_id,
        );
        write_registry_record(&storage, &record).await;
        let delete_lifecycle = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(72, 1),
            Ulid::from_parts(73, 1),
        );
        write_document_lifecycle_record(&storage, &delete_lifecycle).await;
        let MetadataDocumentLifecycleRecord::Delete { event } = delete_lifecycle else {
            unreachable!("delete lifecycle helper returns delete records")
        };
        let graph = event.tombstone;

        assert!(
            !apply_metadata_graph_lifecycle_to_storage(
                &storage,
                &graph,
                postcard::to_allocvec(&graph).expect("graph lifecycle serializes"),
            )
            .await
            .expect("stale graph lifecycle delete is fenced")
        );

        assert_registry_record_present(&storage, &record).await;
        assert!(
            read_graph_lifecycle_record(&storage, &graph.graph_iri)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn metadata_graph_lifecycle_delete_applies_with_matching_document_lifecycle_tombstone() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(80, 1);
        let document_id = Ulid::from_parts(81, 1);
        let record = registry_record(
            group_id,
            document_id,
            "datasets/graph-deleted",
            100,
            Ulid::from_parts(82, 1),
        );
        write_registry_record(&storage, &record).await;
        let delete_lifecycle = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(83, 1),
            record.last_event_id,
        );
        write_document_lifecycle_record(&storage, &delete_lifecycle).await;
        let MetadataDocumentLifecycleRecord::Delete { event } = delete_lifecycle else {
            unreachable!("delete lifecycle helper returns delete records")
        };
        let graph = event.tombstone;

        assert!(
            apply_metadata_graph_lifecycle_to_storage(
                &storage,
                &graph,
                postcard::to_allocvec(&graph).expect("graph lifecycle serializes"),
            )
            .await
            .expect("graph lifecycle delete applies")
        );

        assert_eq!(
            read_graph_lifecycle_record(&storage, &graph.graph_iri).await,
            Some(graph.clone())
        );
        assert_registry_record_deleted(&storage, group_id, document_id).await;
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
