use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{
    AdminDocumentApplyStatus, AdminDocumentReducerState, GROUP_DISPLAY_NAME_PATH, GROUP_OWNER_PATH,
    GROUP_REALM_ID_PATH, REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
    REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_QUOTA_PATH, USER_NAME_PATH,
    decode_admin_document_reducer_state, group_role_id_from_path, group_role_path,
    group_role_user_assignment_from_path, group_role_user_assignment_path,
    overlay_realm_config_placement_reducer_materialization, realm_config_node_id_from_path,
    realm_config_node_path, realm_config_oidc_provider_id_from_path, realm_role_path,
    realm_role_user_assignment_from_path, realm_role_user_assignment_path, user_attribute_path,
    user_subject_id_path,
};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncEvent, DocumentSyncEvictedDocument,
    DocumentSyncNetEvent, DocumentSyncOutboxEvent, DocumentSyncPublish,
    DocumentSyncReconcileResult, DocumentSyncTarget,
};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::short_display_id;
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_STATE_KEYSPACE, DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
    DOCUMENT_SYNC_REVISION_KEYSPACE, GROUP_KEYSPACE, GROUP_OWNER_INDEX_KEYSPACE,
    METADATA_DOCUMENT_LIFECYCLE_KEYSPACE, NOTIFICATION_WATCH_INTEREST_KEYSPACE,
    USER_SUBJECT_CLAIMS_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataDocumentDeleteRecord, MetadataDocumentLifecycleRecord,
    MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord,
};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, document_placement_delete_entry,
    document_sync_revision_key, document_sync_revision_write_entry,
    metadata_create_event_and_pending_projection_write_entries, metadata_document_lifecycle_key,
    metadata_document_lifecycle_write_entry, metadata_graph_lifecycle_key,
    metadata_graph_lifecycle_write_entry, metadata_graph_prune_job_write_entry,
    metadata_registry_delete_entries, metadata_registry_write_entries,
    stale_admin_document_conflict_delete_entries, subject_index_key, subject_index_value,
};
use aruna_core::structs::{
    Group, GroupAuthorizationDocument, KIND_LABEL_KEY, MetadataRegistryRecord,
    NOTIFICATION_WATCH_MAX_PREFIX_LEN, NodeInfoDocument, NodeUsageSnapshot,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind, Role, User,
    WatchEventMask, WatchInterestDigest, WatchSubscription, group_owner_index_key,
    node_usage_key_node_id, watch_interest_dirty_key, watch_interest_key_node_id,
    watch_interest_key_realm_id,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{RoleId, TxnId, UserId, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use byteview::ByteView;
use globset::Glob;
use irokle_crate::Event as _;
use irokle_crate::Storage as _;
use irokle_crate::TopicControl;
use irokle_crate::history::HistoryOrder;
use irokle_crate::net::{decode_sync_message, encode_frame, encode_sync_message};
use irokle_crate::oplog::Oplog;
use irokle_crate::sync::{SyncData, SyncMessage, SyncRequest};
use irokle_crate::{
    EventEnvelope, PeerId, ReplicationPolicy, TopicEviction, TopicGenesis, TopicPayload,
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::error::{NetError, Result};
use crate::streams::BiStream;

use ::irokle as irokle_crate;

const DOCUMENT_SYNC_PEER_SYNC_TIMEOUT: Duration = Duration::from_secs(30);
// Matches irokle's 1024-topic wire batches; the worst-case data stream sends
// three messages per topic, staying under the peer's 4096-message stream cap.
pub const DOCUMENT_SYNC_BATCH_SYNC_TOPIC_LIMIT: usize = 1_024;
const DOCUMENT_SYNC_INBOUND_SYNC_MESSAGE_LIMIT: usize = 4_096;
const DOCUMENT_SYNC_INBOUND_SYNC_STREAM_BYTES: usize = 256 * 1024 * 1024;
const DOCUMENT_SYNC_FRAME_LEN_LIMIT: usize = 16 * 1024 * 1024;
const MAX_DEFERRED_ADMIN_TOPICS: usize = 1_024;
const MAX_DEFERRED_ADMIN_TOPICS_PER_DEPENDENCY: usize = 256;

#[derive(Debug)]
struct PendingMetadataCreateApply {
    target: DocumentSyncTarget,
    record: MetadataCreateEventRecord,
    bytes: Vec<u8>,
    lifecycle_revision: Option<DocumentSyncChange>,
}

#[derive(Default)]
struct PublishEventsOutcome {
    published: BTreeMap<irokle_crate::TopicId, irokle_crate::ActorClock>,
    published_indices: Vec<usize>,
    retry_indices: Vec<usize>,
    retry_error: Option<String>,
}

#[derive(Clone)]
pub struct DocumentSyncService {
    node: irokle_crate::Irokle<irokle_crate::FjallStorage>,
    net: Arc<irokle_crate::net::IrohNet<irokle_crate::FjallStorage>>,
    db: fjall::OptimisticTxDatabase,
    persist_policy: FjallPersistPolicy,
    storage: StorageHandle,
    default_peers: Arc<RwLock<BTreeSet<PeerId>>>,
    storage_path: PathBuf,
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
    // Genesis tie-break evictions from every admission path (irokle's own
    // accept/resync loops via the net sink, plus this service's bootstrap and
    // batch-sync paths) funnel into this sender; the embedder drains the
    // receiver once via `take_eviction_receiver` and re-emits the payloads.
    eviction_tx: tokio::sync::mpsc::UnboundedSender<TopicEviction>,
    eviction_rx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<TopicEviction>>>>,
}

impl std::fmt::Debug for DocumentSyncService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentSyncService")
            .field("peer_id", &self.node.peer_id())
            .field("storage_path", &self.storage_path)
            .finish()
    }
}

impl DocumentSyncService {
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
        let (eviction_tx, eviction_rx) = tokio::sync::mpsc::unbounded_channel();
        let net = Arc::new(
            irokle_crate::net::IrohNet::new_with_alpns_config_and_sink(
                endpoint,
                node.clone(),
                alpns,
                runtime,
                Some(eviction_tx.clone()),
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
            reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
            eviction_tx,
            eviction_rx: Arc::new(Mutex::new(Some(eviction_rx))),
        })
    }

    pub fn node(&self) -> irokle_crate::Irokle<irokle_crate::FjallStorage> {
        self.node.clone()
    }

    /// Takes the genesis tie-break eviction receiver. The embedder calls this
    /// once to drive the re-emission consumer; later calls return `None`.
    pub fn take_eviction_receiver(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<TopicEviction>> {
        self.eviction_rx.lock().take()
    }

    /// Decodes a genesis tie-break eviction into the document outbox events that
    /// must be re-emitted onto the winning chain. Every item sets
    /// `allow_genesis: false` so the loser replays through the normal outbox
    /// drain instead of minting a rival genesis, and original ids are preserved
    /// where the outbox format can carry them. Control ops are skipped,
    /// whole-document admin poison is dropped with a warning (mirroring the
    /// reconcile skip arm), and ops not authored by the local node are rejected
    /// since an eviction is by construction the local node's own chain.
    pub fn decode_eviction(&self, eviction: TopicEviction) -> Vec<DocumentSyncEvictedDocument> {
        let local_peer = self.node.peer_id();
        let mut documents = Vec::new();
        for evicted in eviction.evicted {
            if evicted.author != local_peer {
                warn!(
                    topic_id = %eviction.topic_id,
                    author = %evicted.author,
                    "Skipping evicted op not authored by the local node"
                );
                continue;
            }
            let TopicPayload::Event(envelope) = evicted.payload else {
                // Non-event control op (e.g. AddPeer/RemovePeer): nothing to re-emit.
                continue;
            };
            let event = match envelope.decode_event::<DocumentSyncEvent>() {
                Ok(event) => event,
                Err(error) => {
                    warn!(
                        topic_id = %eviction.topic_id,
                        op_id = %evicted.op_id,
                        %error,
                        "Skipping evicted op that is not a document sync event"
                    );
                    continue;
                }
            };
            match event {
                DocumentSyncEvent::AdminOperation { target, event } => {
                    documents.push(DocumentSyncEvictedDocument {
                        event_id: None,
                        target,
                        event: DocumentSyncOutboxEvent::AdminOperation { event },
                        allow_genesis: false,
                    });
                }
                DocumentSyncEvent::Upsert {
                    event_id,
                    target,
                    bytes,
                    change,
                } => {
                    if admin_document_target_for_reduced_document(&target).is_some() {
                        warn!(
                            topic_id = %eviction.topic_id,
                            ?target,
                            "Dropping evicted whole-document admin upsert"
                        );
                        continue;
                    }
                    documents.push(DocumentSyncEvictedDocument {
                        event_id: Some(event_id),
                        target,
                        event: DocumentSyncOutboxEvent::Upsert { bytes, change },
                        allow_genesis: false,
                    });
                }
                DocumentSyncEvent::Delete {
                    event_id,
                    target,
                    change,
                } => {
                    if admin_document_target_for_reduced_document(&target).is_some() {
                        warn!(
                            topic_id = %eviction.topic_id,
                            ?target,
                            "Dropping evicted whole-document admin delete"
                        );
                        continue;
                    }
                    documents.push(DocumentSyncEvictedDocument {
                        event_id: Some(event_id),
                        target,
                        event: DocumentSyncOutboxEvent::Delete { change },
                        allow_genesis: false,
                    });
                }
            }
        }
        documents
    }

    /// Forwards evictions produced by this service's own admission paths into
    /// the shared eviction sink.
    fn forward_evictions(&self, evictions: Vec<TopicEviction>) {
        forward_evictions_to(&self.eviction_tx, evictions);
    }

    #[cfg(test)]
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
        // The underlying sync node exposes additive whitelist updates only; this
        // refresh replaces the default fan-out set while retaining previously
        // allowed peers until process restart.
        *self.default_peers.write() = peers;
        self.flush_database()?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.net.shutdown().await;
        if let Err(error) = self.db.persist(fjall::PersistMode::SyncAll) {
            warn!(error = %error, "Failed to persist document sync database on shutdown");
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

    pub fn allow_document_sync_peers(
        &self,
        targets: &[DocumentSyncTarget],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        if targets.is_empty() {
            return Ok(());
        }

        let sync_peers = self.sync_peers(peers);
        if sync_peers.is_empty() {
            return Ok(());
        }
        self.allow_sync_peers(&sync_peers)?;

        let mut seen_topics = BTreeSet::new();
        for target in targets {
            let topic_id = target.sync_topic_id();
            if !seen_topics.insert(topic_id) {
                continue;
            }

            let state = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| {
                    NetError::Bootstrap(format!(
                        "document sync topic {topic_id} for target {target:?} is missing"
                    ))
                })?;

            if state.event_type_id != DocumentSyncEvent::TYPE_ID {
                return Err(NetError::Bootstrap(format!(
                    "Document sync topic {topic_id} has event type {}, expected {}",
                    state.event_type_id,
                    DocumentSyncEvent::TYPE_ID
                )));
            }

            let missing_peers = sync_peers
                .iter()
                .copied()
                .filter(|peer| !state.members.contains(peer))
                .collect::<Vec<_>>();
            if missing_peers.is_empty() {
                continue;
            }

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

        self.flush_database()
    }

    pub fn ensure_document_sync_topics(
        &self,
        targets: &[DocumentSyncTarget],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        if targets.is_empty() {
            return Ok(());
        }

        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;

        let mut seen_topics = BTreeSet::new();
        for target in targets {
            if seen_topics.insert(target.sync_topic_id()) {
                // Called on the realm node that already holds these documents to
                // admit an onboarding peer, so it may create any still-missing
                // topic genesis for documents it originated/holds.
                self.ensure_topic(target, &sync_peers, true)?;
            }
        }

        self.flush_database()
    }

    /// Notes a live inbound document sync connection so the resync scheduler retries
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
            "Inbound document sync stream summary"
        );
        Ok(touched_topics)
    }

    pub async fn reconcile_document_sync_topics(
        &self,
        topic_ids: Vec<irokle_crate::TopicId>,
    ) -> Result<DocumentSyncReconcileResult> {
        self.reconcile_document_topics(topic_ids).await
    }

    pub async fn publish_documents(
        &self,
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        let targets = documents
            .iter()
            .map(|document| document.target().clone())
            .collect::<Vec<_>>();
        match self.publish_events(documents, peers).await {
            Ok(outcome) if outcome.retry_indices.is_empty() => {
                DocumentSyncNetEvent::DocumentsPublished { targets }
            }
            Ok(outcome) if outcome.published_indices.is_empty() => DocumentSyncNetEvent::Error {
                target: outcome
                    .retry_indices
                    .first()
                    .and_then(|index| targets.get(*index).cloned()),
                error: outcome
                    .retry_error
                    .unwrap_or_else(|| "Document sync topic not ready".to_string()),
            },
            Ok(outcome) => DocumentSyncNetEvent::DocumentsPartiallyPublished {
                published_indices: outcome.published_indices,
                retry_indices: outcome.retry_indices,
                error: outcome
                    .retry_error
                    .unwrap_or_else(|| "Document sync topic not ready".to_string()),
            },
            Err(error) => DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn reconcile_documents_event(&self) -> DocumentSyncNetEvent {
        match self.reconcile_documents().await {
            Ok(result) => DocumentSyncNetEvent::DocumentsReconciled {
                applied: result.applied(),
                targets: result.targets,
                metadata_create_events: result.metadata_create_events,
                metadata_graph_tombstones: result.metadata_graph_tombstones,
            },
            Err(error) => DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_document_event(
        &self,
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        let topic_id = target.sync_topic_id();
        let sync_peers = self.sync_peers(peers);
        if let Err(error) = self.allow_sync_peers(&sync_peers) {
            return DocumentSyncNetEvent::Error {
                target: Some(target),
                error: error.to_string(),
            };
        }
        match self.has_topic(topic_id) {
            Ok(true) => {
                if let Err(error) = self.sync_topic(topic_id, sync_peers).await {
                    return DocumentSyncNetEvent::Error {
                        target: Some(target),
                        error: error.to_string(),
                    };
                }
            }
            Ok(false) => {
                if let Err(error) = self.bootstrap_topic_from_peers(topic_id, &sync_peers).await {
                    return DocumentSyncNetEvent::Error {
                        target: Some(target),
                        error: error.to_string(),
                    };
                }
            }
            Err(error) => {
                return DocumentSyncNetEvent::Error {
                    target: Some(target),
                    error: error.to_string(),
                };
            }
        }
        if let Err(error) = self.flush_database() {
            return DocumentSyncNetEvent::Error {
                target: Some(target),
                error: error.to_string(),
            };
        }
        match self.reconcile_document_topics([topic_id]).await {
            Ok(result) => DocumentSyncNetEvent::DocumentsReconciled {
                applied: result.applied(),
                targets: result.targets,
                metadata_create_events: result.metadata_create_events,
                metadata_graph_tombstones: result.metadata_graph_tombstones,
            },
            Err(error) => DocumentSyncNetEvent::Error {
                target: Some(target),
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_documents_event(
        &self,
        targets: Vec<DocumentSyncTarget>,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        let sync_started = Instant::now();
        let target_count = targets.len();
        let sync_peers = self.sync_peers(peers);
        if let Err(error) = self.allow_sync_peers(&sync_peers) {
            return DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }

        let mut seen_topics = BTreeSet::new();
        let mut topics: Vec<(irokle_crate::TopicId, DocumentSyncTarget)> = Vec::new();
        for target in targets {
            let topic_id = target.sync_topic_id();
            if !seen_topics.insert(topic_id) {
                continue;
            }
            match self.has_topic(topic_id) {
                Ok(true) => topics.push((topic_id, target)),
                Ok(false) => {
                    if let Err(error) = self.bootstrap_topic_from_peers(topic_id, &sync_peers).await
                    {
                        return DocumentSyncNetEvent::Error {
                            target: Some(target),
                            error: error.to_string(),
                        };
                    }
                    topics.push((topic_id, target));
                }
                Err(error) => {
                    return DocumentSyncNetEvent::Error {
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
            return DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }
        let peer_sync_elapsed = peer_sync_started.elapsed();

        let flush_started = Instant::now();
        if let Err(error) = self.flush_database() {
            return DocumentSyncNetEvent::Error {
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
                DocumentSyncNetEvent::DocumentsReconciled {
                    applied: result.applied(),
                    targets: result.targets,
                    metadata_create_events: result.metadata_create_events,
                    metadata_graph_tombstones: result.metadata_graph_tombstones,
                }
            }
            Err(error) => DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    async fn publish_events(
        &self,
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    ) -> Result<PublishEventsOutcome> {
        if documents.is_empty() {
            return Ok(PublishEventsOutcome::default());
        }
        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;
        let service = self.clone();
        let mut outcome = tokio::task::spawn_blocking(move || {
            service.publish_events_blocking(documents, &sync_peers)
        })
        .await
        .map_err(|error| NetError::Bootstrap(error.to_string()))??;
        let published = std::mem::take(&mut outcome.published);
        self.advance_topic_cursors(published).await?;
        self.flush_database()?;
        Ok(outcome)
    }

    fn publish_events_blocking(
        &self,
        documents: Vec<DocumentSyncPublish>,
        sync_peers: &BTreeSet<PeerId>,
    ) -> Result<PublishEventsOutcome> {
        let publish_started = Instant::now();
        let document_count = documents.len();
        let mut fast_path = 0usize;
        let mut fallback = 0usize;
        let oplog = Oplog::with_storage(self.node.storage().clone());
        let mut outcome = PublishEventsOutcome::default();
        for (index, document) in documents.into_iter().enumerate() {
            let allow_genesis = document.allow_genesis();
            let event = match document {
                DocumentSyncPublish::Upsert {
                    event_id,
                    target,
                    bytes,
                    change,
                    ..
                } => DocumentSyncEvent::Upsert {
                    event_id,
                    target,
                    bytes,
                    change,
                },
                DocumentSyncPublish::Delete {
                    event_id,
                    target,
                    change,
                    ..
                } => DocumentSyncEvent::Delete {
                    event_id,
                    target,
                    change,
                },
                DocumentSyncPublish::AdminOperation { target, event, .. } => {
                    DocumentSyncEvent::AdminOperation { target, event }
                }
            };
            let target = event.target().clone();
            let topic_id = target.sync_topic_id();
            let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
            let envelope = EventEnvelope::encode_event(&event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let op = match self.publish_event_op(
                &oplog,
                &target,
                topic_id,
                actor_id,
                envelope,
                sync_peers,
                allow_genesis,
                &mut fast_path,
                &mut fallback,
            ) {
                Ok(op) => op,
                Err(NetError::TopicNotReady(topic)) => {
                    outcome.retry_indices.push(index);
                    outcome
                        .retry_error
                        .get_or_insert_with(|| NetError::TopicNotReady(topic).to_string());
                    continue;
                }
                Err(error) => return Err(error),
            };
            outcome.published_indices.push(index);
            outcome
                .published
                .entry(topic_id)
                .or_default()
                .observe(op.signed.body.actor_id, op.signed.body.actor_seq);
        }
        let published_count = outcome.published_indices.len();
        info!(
            event = "pipeline.publish.summary",
            documents = document_count,
            published = published_count,
            retry = outcome.retry_indices.len(),
            fast_path,
            fallback,
            existing = published_count.saturating_sub(fast_path + fallback),
            total_ms = duration_ms(publish_started.elapsed()),
            "Document sync publish batch breakdown"
        );
        Ok(outcome)
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
        allow_genesis: bool,
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
            // Only the document's origin may mint its topic genesis. Any other
            // publisher waits (retryable) for that genesis to replicate in.
            if !allow_genesis {
                return Err(NetError::TopicNotReady(topic_id.to_string()));
            }
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
        let topic_id = self.ensure_topic(target, sync_peers, allow_genesis)?;
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
                .storage_read(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key.clone(),
                )
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
            writes.push((
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                cursor_key,
                value,
            ));
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
        allow_genesis: bool,
    ) -> Result<irokle_crate::TopicId> {
        let topic_id = target.sync_topic_id();
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
                        "Document sync topic {topic_id} has event type {}, expected {}",
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

            // Only the document's origin may mint the genesis; other publishers
            // wait (retryable) for it to replicate in.
            if !allow_genesis {
                return Err(NetError::TopicNotReady(topic_id.to_string()));
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
                .unwrap_or_else(|| format!("failed to ensure document sync topic {topic_id}")),
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
                    per_peer.push(format!(
                        "{}={}ms",
                        short_display_id(peer),
                        duration_ms(elapsed)
                    ));
                    debug!(%peer, context = %context, "Synced document peer")
                }
                Ok((peer, Err(error), elapsed)) => {
                    per_peer.push(format!(
                        "{}={}ms(err)",
                        short_display_id(peer),
                        duration_ms(elapsed)
                    ));
                    warn!(%peer, context = %context, error = %error, "Document sync peer sync failed; deferring to resync scheduler");
                    if first_error.is_none() {
                        first_error = Some(error.to_string());
                    }
                }
                Err(error) => {
                    warn!(context = %context, error = %error, "Document sync peer sync task failed");
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
            "Document sync peer fan-out summary"
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
        Self::fan_out_peer_syncs(
            peers,
            format!("document sync topic {topic_id}"),
            move |peer| {
                let net = net.clone();
                async move {
                    match timeout(
                        DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
                        net.sync_peer_now(peer, topic_id),
                    )
                    .await
                    {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(error)) => Err(NetError::Bootstrap(error.to_string())),
                        Err(_) => Err(NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT)),
                    }
                }
            },
        )
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
        for chunk in topic_ids.chunks(DOCUMENT_SYNC_BATCH_SYNC_TOPIC_LIMIT) {
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
            format!("document sync topic batch of {} topics", topic_ids.len()),
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
            DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
            self.net.sync_with(peer_addr.clone(), &initial_messages),
        )
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
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
                "peer {peer} responded for {}/{} document sync batch topics",
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
            DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
            self.net.sync_with(peer_addr.clone(), &sync_messages),
        )
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;
        let r2_io = r2_io_started.elapsed();
        let r2_process_started = Instant::now();
        let node = self.node.clone();
        let net = self.net.clone();
        let data_known = known_topics.clone();
        let eviction_tx = self.eviction_tx.clone();
        let (failed_topics, followup) = tokio::task::spawn_blocking(move || {
            process_batch_data_responses(
                &node,
                &net,
                peer,
                &data_known,
                failed_topics,
                responses,
                &eviction_tx,
            )
        })
        .await
        .map_err(|error| NetError::Bootstrap(error.to_string()))??;
        let r2_process = r2_process_started.elapsed();
        let fu_io_started = Instant::now();
        if !followup.is_empty() {
            let responses = timeout(
                DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
                self.net.sync_with(peer_addr, &followup),
            )
            .await
            .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
            .map_err(NetError::from)?;
            for response in responses {
                match response {
                    SyncMessage::Summary(summary) if known_topics.contains(&summary.topic_id) => {}
                    other => {
                        return Err(NetError::Bootstrap(format!(
                            "unexpected document sync batch ack response from {peer}: {other:?}"
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
                    warn!(%peer, %topic_id, error = %error, "Document sync bootstrap attempt failed");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        Err(first_error.unwrap_or_else(|| {
            NetError::Bootstrap(format!(
                "no peers available to bootstrap document sync topic {topic_id}"
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
            DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
            self.net.sync_with(
                peer_addr.clone(),
                &[SyncMessage::Open(self.node.sync_open(topic_id))],
            ),
        )
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;
        let summary = responses
            .into_iter()
            .find_map(|response| match response {
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => Some(summary),
                _ => None,
            })
            .ok_or_else(|| {
                NetError::Bootstrap(format!(
                    "peer {peer} did not return a document sync summary for topic {topic_id}"
                ))
            })?;
        if remote_summary_is_empty(&summary) {
            return Ok(());
        }
        if summary.event_type_id.as_deref() != Some(DocumentSyncEvent::TYPE_ID) {
            return Err(NetError::Bootstrap(format!(
                "peer {peer} advertised document sync topic {topic_id} with unexpected event type {:?}",
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
            DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
            self.net.sync_with(
                peer_addr.clone(),
                &[
                    SyncMessage::Open(self.node.sync_open(topic_id)),
                    SyncMessage::Request(request),
                ],
            ),
        )
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;

        let mut followup = vec![SyncMessage::Open(self.node.sync_open(topic_id))];
        let mut received_data = false;
        for response in responses {
            match response {
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                SyncMessage::Data(data) if data.topic_id == topic_id => {
                    let (ack, evictions) = self
                        .node
                        .receive_sync_data_from_evicting(peer, data)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                    self.forward_evictions(evictions);
                    received_data = true;
                    followup.push(SyncMessage::Ack(ack));
                }
                other => {
                    return Err(NetError::Bootstrap(format!(
                        "unexpected document sync bootstrap response: {other:?}"
                    )));
                }
            }
        }
        if received_data {
            self.net.schedule_topic_recheck(topic_id)?;
        }
        if followup.len() > 1 {
            let responses = timeout(
                DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
                self.net.sync_with(peer_addr, &followup),
            )
            .await
            .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
            .map_err(NetError::from)?;
            for response in responses {
                match response {
                    SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                    other => {
                        return Err(NetError::Bootstrap(format!(
                            "unexpected document sync bootstrap ack response: {other:?}"
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
        let _reconcile_guard = self.reconcile_lock.lock().await;
        let mut deferred_admin_topics: BTreeMap<
            AdminEventDependency,
            BTreeSet<irokle_crate::TopicId>,
        > = self
            .storage_read(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                deferred_admin_topics_key(),
            )
            .await?
            .map(|bytes| postcard::from_bytes(&bytes))
            .transpose()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .unwrap_or_default();
        let mut queued_topics = BTreeSet::new();
        let mut topic_queue = VecDeque::new();
        for topic_id in topic_ids {
            if queued_topics.insert(topic_id) {
                topic_queue.push_back(topic_id);
            }
        }
        let mut satisfied_persisted_dependencies = Vec::new();
        for dependency in deferred_admin_topics.keys().copied().collect::<Vec<_>>() {
            let available = match dependency {
                AdminEventDependency::RealmConfig(realm_id) => {
                    read_admin_realm_config(&self.storage, realm_id)
                        .await?
                        .is_some()
                }
                AdminEventDependency::RealmAuthorization(realm_id) => {
                    read_admin_realm_authorization(&self.storage, realm_id)
                        .await?
                        .is_some()
                }
            };
            if available {
                satisfied_persisted_dependencies.push(dependency);
            }
        }
        for dependency in satisfied_persisted_dependencies {
            if let Some(topics) = deferred_admin_topics.remove(&dependency) {
                for topic_id in topics {
                    if queued_topics.insert(topic_id) {
                        topic_queue.push_back(topic_id);
                    }
                }
            }
        }
        let mut applied_targets = Vec::new();
        let mut metadata_create_events = Vec::new();
        let mut metadata_graph_tombstones = Vec::new();
        let mut pending_metadata_creates = Vec::new();
        let mut deferred_cursor_writes = Vec::new();
        while let Some(topic_id) = topic_queue.pop_front() {
            queued_topics.remove(&topic_id);
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
                .storage_read(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key.clone(),
                )
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
            let mut deferred_admin_events = Vec::new();
            let mut satisfied_admin_dependencies = BTreeSet::new();
            for (event, actor_id) in events {
                let target_topic_id = event.target().sync_topic_id();
                if target_topic_id != topic_id {
                    warn!(
                        %topic_id,
                        %target_topic_id,
                        "Skipping document sync event whose target does not match its topic"
                    );
                    continue;
                }
                match event {
                    DocumentSyncEvent::Upsert {
                        target: target @ DocumentSyncTarget::WatchSubscription { owner, watch_id },
                        bytes,
                        change,
                        ..
                    } => {
                        let expected_actor = irokle_crate::actor_id_for(
                            topic_id,
                            node_id_to_peer_id(&change.current.actor),
                        );
                        if actor_id != expected_actor {
                            warn!(
                                %topic_id,
                                %owner,
                                %watch_id,
                                "Rejecting watch subscription whose revision actor is not its publisher"
                            );
                            continue;
                        }
                        if let Err(reason) =
                            validate_watch_subscription_upsert(&target, &bytes, &change)
                        {
                            warn!(%topic_id, %owner, %watch_id, %reason, "Rejecting invalid watch subscription");
                            continue;
                        }
                        if self
                            .apply_watch_subscription_change(target.clone(), Some(bytes), change)
                            .await?
                        {
                            applied_targets.push(target);
                        }
                    }
                    DocumentSyncEvent::Upsert {
                        target:
                            target @ DocumentSyncTarget::NodeUsage {
                                node_id: snapshot_node,
                                ..
                            },
                        bytes,
                        change,
                        ..
                    } => {
                        // Node-usage snapshots ride a single shared realm topic
                        // that every realm publisher can write, so validate that
                        // the signed publisher owns the claimed node and that the
                        // payload's own node id matches its target before applying.
                        // A rejected event is skipped (never `?`) so the cursor
                        // still advances past it and a forgery cannot wedge the
                        // topic's reconcile loop.
                        let expected_actor = irokle_crate::actor_id_for(
                            topic_id,
                            node_id_to_peer_id(&snapshot_node),
                        );
                        if actor_id != expected_actor {
                            warn!(
                                %topic_id,
                                node_id = %snapshot_node,
                                "Rejecting node usage snapshot: publisher is not the owning node"
                            );
                            continue;
                        }
                        if let Err(reason) = validate_node_usage_upsert(&target, &bytes) {
                            warn!(
                                %topic_id,
                                node_id = %snapshot_node,
                                %reason,
                                "Rejecting invalid node usage snapshot"
                            );
                            continue;
                        }
                        self.apply_upsert(target.clone(), bytes, change).await?;
                        applied_targets.push(target);
                    }
                    DocumentSyncEvent::Upsert {
                        target:
                            target @ DocumentSyncTarget::WatchInterest {
                                realm_id,
                                node_id: interest_node,
                            },
                        bytes,
                        change,
                        ..
                    } => {
                        // Watch-interest digests ride a single shared realm topic
                        // that every realm publisher can write, so validate that
                        // the signed publisher owns the claimed node and that the
                        // payload's own node id matches its target before applying.
                        // A rejected event is skipped (never `?`) so the cursor
                        // still advances past it and a forgery cannot wedge the
                        // topic's reconcile loop.
                        let expected_actor = irokle_crate::actor_id_for(
                            topic_id,
                            node_id_to_peer_id(&interest_node),
                        );
                        if actor_id != expected_actor {
                            warn!(
                                %topic_id,
                                realm_id = %realm_id,
                                node_id = %interest_node,
                                "Rejecting watch interest digest: publisher is not the owning node"
                            );
                            continue;
                        }
                        if let Err(reason) = validate_watch_interest_upsert(&target, &bytes) {
                            warn!(
                                %topic_id,
                                realm_id = %realm_id,
                                node_id = %interest_node,
                                %reason,
                                "Rejecting invalid watch interest digest"
                            );
                            continue;
                        }
                        self.apply_upsert(target.clone(), bytes, change).await?;
                        applied_targets.push(target);
                    }
                    DocumentSyncEvent::Upsert {
                        target:
                            target @ DocumentSyncTarget::NodeInfo {
                                node_id: info_node, ..
                            },
                        bytes,
                        change,
                        ..
                    } => {
                        // Node info documents ride a single shared realm topic that
                        // every realm publisher can write, so validate that the
                        // signed publisher owns the claimed node and that the
                        // payload's own node id matches its target before applying.
                        // A rejected event is skipped (never `?`) so the cursor
                        // advances past it and a forgery cannot wedge the topic.
                        let expected_actor =
                            irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&info_node));
                        if actor_id != expected_actor {
                            warn!(
                                %topic_id,
                                node_id = %info_node,
                                "Rejecting node info document: publisher is not the owning node"
                            );
                            continue;
                        }
                        if let Err(reason) = validate_node_info_upsert(&target, &bytes) {
                            warn!(
                                %topic_id,
                                node_id = %info_node,
                                %reason,
                                "Rejecting invalid node info document"
                            );
                            continue;
                        }
                        self.apply_upsert(target.clone(), bytes, change).await?;
                        applied_targets.push(target);
                    }
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
                        change,
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
                                pending_metadata_creates.push(PendingMetadataCreateApply {
                                    target,
                                    lifecycle_revision: Some(change),
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
                                        change,
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
                    DocumentSyncEvent::Delete {
                        target: target @ DocumentSyncTarget::WatchSubscription { owner, watch_id },
                        change,
                        ..
                    } => {
                        let expected_actor = irokle_crate::actor_id_for(
                            topic_id,
                            node_id_to_peer_id(&change.current.actor),
                        );
                        if actor_id != expected_actor {
                            warn!(
                                %topic_id,
                                %owner,
                                %watch_id,
                                "Rejecting watch subscription delete whose revision actor is not its publisher"
                            );
                            continue;
                        }
                        if let Err(reason) = validate_watch_subscription_delete(&target, &change) {
                            warn!(%topic_id, %owner, %watch_id, %reason, "Rejecting invalid watch subscription delete");
                            continue;
                        }
                        if self
                            .apply_watch_subscription_change(target.clone(), None, change)
                            .await?
                        {
                            applied_targets.push(target);
                        }
                    }
                    DocumentSyncEvent::Delete {
                        target:
                            target @ (DocumentSyncTarget::NodeUsage { .. }
                            | DocumentSyncTarget::WatchInterest { .. }),
                        ..
                    }
                    | DocumentSyncEvent::AdminOperation {
                        target:
                            target @ (DocumentSyncTarget::NodeUsage { .. }
                            | DocumentSyncTarget::WatchInterest { .. }),
                        ..
                    } => {
                        // Shared realm snapshots only ever sync as owner-validated
                        // upserts. A signed Delete or AdminOperation on the shared
                        // realm topic would otherwise `?`-propagate through the
                        // generic arm and wedge every peer's reconcile forever, so
                        // skip it and let the cursor advance past the hostile op.
                        warn!(
                            %topic_id,
                            ?target,
                            "Skipping unsupported non-upsert shared document event"
                        );
                        continue;
                    }
                    DocumentSyncEvent::Delete {
                        target: target @ DocumentSyncTarget::NodeInfo { .. },
                        ..
                    }
                    | DocumentSyncEvent::AdminOperation {
                        target: target @ DocumentSyncTarget::NodeInfo { .. },
                        ..
                    } => {
                        // Node info documents only ever sync as owner-validated
                        // upserts; skip any signed Delete/AdminOperation on the
                        // shared realm topic so it cannot wedge the reconcile loop.
                        warn!(
                            %topic_id,
                            ?target,
                            "Skipping unsupported non-upsert node info document event"
                        );
                        continue;
                    }
                    DocumentSyncEvent::Upsert { ref target, .. }
                    | DocumentSyncEvent::Delete { ref target, .. }
                        if admin_document_target_for_reduced_document(target).is_some() =>
                    {
                        // apply_upsert/apply_delete refuse whole-document admin sync, so
                        // skip it here to let the cursor advance instead of wedging reconcile.
                        warn!(
                            %topic_id,
                            ?target,
                            "Skipping unsupported whole-document admin sync event"
                        );
                        continue;
                    }
                    DocumentSyncEvent::AdminOperation { target, event } => {
                        match validate_replicated_admin_event(
                            &self.storage,
                            topic_id,
                            actor_id,
                            &target,
                            &event,
                        )
                        .await?
                        {
                            AdminEventValidation::Accepted => {}
                            AdminEventValidation::Rejected(reason) => {
                                warn!(
                                    %topic_id,
                                    event_id = %event.event_id,
                                    origin_node_id = %event.origin_node_id,
                                    %reason,
                                    "Rejecting invalid or unauthorized admin operation"
                                );
                                continue;
                            }
                            AdminEventValidation::Deferred { dependency, reason } => {
                                warn!(
                                    %topic_id,
                                    event_id = %event.event_id,
                                    origin_node_id = %event.origin_node_id,
                                    %reason,
                                    "Deferring admin operation until prerequisite state is available"
                                );
                                deferred_admin_events
                                    .push((target, *event, actor_id, dependency, reason));
                                continue;
                            }
                        }

                        apply_admin_document_operation_to_storage(
                            &self.storage,
                            target.clone(),
                            *event,
                        )
                        .await?;
                        if let Some(dependency) = satisfied_admin_dependency(&target) {
                            satisfied_admin_dependencies.insert(dependency);
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
            let mut cross_topic_dependencies = BTreeSet::new();
            let mut pending = deferred_admin_events;
            loop {
                let mut progressed = false;
                let mut retry = Vec::new();
                for (target, event, actor_id, _dependency, _previous_reason) in pending {
                    match validate_replicated_admin_event(
                        &self.storage,
                        topic_id,
                        actor_id,
                        &target,
                        &event,
                    )
                    .await?
                    {
                        AdminEventValidation::Accepted => {
                            apply_admin_document_operation_to_storage(
                                &self.storage,
                                target.clone(),
                                event,
                            )
                            .await?;
                            if let Some(dependency) = satisfied_admin_dependency(&target) {
                                satisfied_admin_dependencies.insert(dependency);
                            }
                            applied_targets.push(target);
                            progressed = true;
                        }
                        AdminEventValidation::Rejected(reason) => warn!(
                            %topic_id,
                            event_id = %event.event_id,
                            %reason,
                            "Rejecting deferred admin operation after prerequisite replay"
                        ),
                        AdminEventValidation::Deferred { dependency, reason } => {
                            retry.push((target, event, actor_id, dependency, reason))
                        }
                    }
                }
                if !progressed {
                    for (_, event, _, dependency, reason) in retry {
                        if let Some(dependency) = dependency {
                            cross_topic_dependencies.insert(dependency);
                        } else {
                            warn!(
                                %topic_id,
                                event_id = %event.event_id,
                                reason = %reason,
                                "Rejecting admin operation whose same-topic prerequisite is absent"
                            );
                        }
                    }
                    break;
                }
                pending = retry;
                if pending.is_empty() {
                    break;
                }
            }
            if !cross_topic_dependencies.is_empty() {
                remove_deferred_admin_topic(&mut deferred_admin_topics, topic_id);
                let mut registered_dependency = false;
                for dependency in cross_topic_dependencies {
                    let total_topics = deferred_admin_topics
                        .values()
                        .map(BTreeSet::len)
                        .sum::<usize>();
                    let dependency_topics = deferred_admin_topics
                        .get(&dependency)
                        .map(BTreeSet::len)
                        .unwrap_or_default();
                    if total_topics < MAX_DEFERRED_ADMIN_TOPICS
                        && dependency_topics < MAX_DEFERRED_ADMIN_TOPICS_PER_DEPENDENCY
                    {
                        deferred_admin_topics
                            .entry(dependency)
                            .or_default()
                            .insert(topic_id);
                        registered_dependency = true;
                    } else {
                        warn!(
                            %topic_id,
                            ?dependency,
                            "Dropping admin dependency registration because the deferred-topic registry is full"
                        );
                    }
                }
                if registered_dependency {
                    continue;
                }
            }
            let value = ByteView::from(
                postcard::to_allocvec(&cursor)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            );
            if deferred_creates {
                deferred_cursor_writes.push((
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key,
                    value,
                ));
            } else {
                self.storage_write(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key,
                    value,
                )
                .await?;
            }
            let retry_topics = {
                remove_deferred_admin_topic(&mut deferred_admin_topics, topic_id);
                satisfied_admin_dependencies
                    .into_iter()
                    .filter_map(|dependency| deferred_admin_topics.remove(&dependency))
                    .flatten()
                    .collect::<Vec<_>>()
            };
            for retry_topic in retry_topics {
                if queued_topics.insert(retry_topic) {
                    topic_queue.push_back(retry_topic);
                }
            }
        }
        self.storage_write(
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            deferred_admin_topics_key(),
            postcard::to_allocvec(&deferred_admin_topics)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        )
        .await?;
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
    /// only the unapplied portion of the topic history where possible. Each
    /// event is paired with the authenticated `actor_id` of the op that carried
    /// it: irokle admits an op only after verifying its signature against
    /// `body.author` and enforcing `actor_id == actor_id_for(topic, author)`, so
    /// the returned actor id is a trustworthy proxy for the signed publisher.
    fn document_events_after(
        &self,
        topic_id: irokle_crate::TopicId,
        cursor: &irokle_crate::ActorClock,
    ) -> Result<Vec<(DocumentSyncEvent, irokle_crate::ActorId)>> {
        match self.node.open_topic::<DocumentSyncEvent>(topic_id) {
            Ok(topic) => Ok(topic
                .history_after(cursor, HistoryOrder::OldestFirst)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into_iter()
                .map(|record| (record.event, record.meta.actor_id))
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
                    let actor_id = op.signed.body.actor_id;
                    let TopicPayload::Event(envelope) = op.signed.body.payload else {
                        continue;
                    };
                    events.push((
                        envelope
                            .decode_event::<DocumentSyncEvent>()
                            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                        actor_id,
                    ));
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
            DocumentSyncEvent::Upsert {
                target,
                bytes,
                change,
                ..
            } => self.apply_upsert(target, bytes, change).await,
            DocumentSyncEvent::Delete { target, change, .. } => {
                self.apply_delete(target, change).await
            }
            DocumentSyncEvent::AdminOperation { target, event } => {
                apply_admin_document_operation_to_storage(&self.storage, target, *event).await
            }
        }
    }

    async fn apply_upsert(
        &self,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        change: DocumentSyncChange,
    ) -> Result<()> {
        if admin_document_target_for_reduced_document(&target).is_some() {
            return Err(NetError::Bootstrap(
                "whole-document admin sync is unsupported; admin documents must sync as operations"
                    .to_string(),
            ));
        }
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
                .apply_metadata_document_lifecycle(record, change)
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
        if let DocumentSyncTarget::NodeUsage { .. } = target {
            // Structural guard for the shared node-usage keyspace. The reconcile
            // loop already validated the signed publisher and this payload, but
            // re-check the snapshot's self-consistency here so the generic
            // storage write below can never persist an unvalidated snapshot.
            validate_node_usage_upsert(&target, &bytes).map_err(NetError::Bootstrap)?;
        }
        if let DocumentSyncTarget::WatchInterest { .. } = target {
            // Structural guard for the shared watch-interest keyspace. The
            // reconcile loop already validated the signed publisher and this
            // payload, but re-check the digest's self-consistency here so the
            // generic storage write below can never persist an unvalidated digest.
            validate_watch_interest_upsert(&target, &bytes).map_err(NetError::Bootstrap)?;
        }
        if let DocumentSyncTarget::NodeInfo { .. } = target {
            // Structural guard for the shared node-info keyspace, mirroring the
            // node-usage guard above so the generic storage write can never
            // persist an unvalidated node info document.
            validate_node_info_upsert(&target, &bytes).map_err(NetError::Bootstrap)?;
        }
        self.storage_write(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.into(),
        )
        .await
    }

    async fn apply_watch_subscription_change(
        &self,
        target: DocumentSyncTarget,
        bytes: Option<Vec<u8>>,
        change: DocumentSyncChange,
    ) -> Result<bool> {
        apply_watch_subscription_change_to_storage(&self.storage, target, bytes, change).await
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
        change: DocumentSyncChange,
    ) -> Result<bool> {
        apply_metadata_document_lifecycle_to_storage(&self.storage, &record, change).await
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

    async fn apply_delete(
        &self,
        target: DocumentSyncTarget,
        change: DocumentSyncChange,
    ) -> Result<()> {
        if change.kind != DocumentSyncChangeKind::Delete {
            return Err(NetError::Bootstrap(
                "document sync delete must carry a delete change".to_string(),
            ));
        }
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
        if admin_document_target_for_reduced_document(&target).is_some() {
            return Err(NetError::Bootstrap(
                "whole-document admin sync is unsupported; admin documents must sync as operations"
                    .to_string(),
            ));
        }
        Err(NetError::Bootstrap(
            "document sync delete target is unsupported".to_string(),
        ))
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
                "unexpected storage event while applying document sync write: {other:?}"
            ))),
        }
    }

    async fn storage_batch_write(&self, writes: Vec<(String, ByteView, Value)>) -> Result<()> {
        storage_batch_write_to(&self.storage, writes).await
    }
}

fn target_write_entry(target: DocumentSyncTarget, value: Value) -> (String, ByteView, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        value,
    )
}

fn overlay_group_reducer_materialization(
    group: &mut Group,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .conflicts
        .contains_key(GROUP_DISPLAY_NAME_PATH)
        && let Some(display_name) = reducer_state.materialized_group_display_name()
    {
        group.display_name = display_name;
    }

    if !reducer_state.conflicts.contains_key(GROUP_REALM_ID_PATH)
        && let Some(realm_id) = reducer_state.materialized_group_realm_id()
    {
        group.realm_id = realm_id;
    }

    if !reducer_state.conflicts.contains_key(GROUP_OWNER_PATH)
        && let Some(owner) = reducer_state.materialized_group_owner()
    {
        group.owner = owner;
    }

    overlay_group_role_set_reducer_materialization(group, reducer_state);
}

fn overlay_group_role_set_reducer_materialization(
    group: &mut Group,
    reducer_state: &AdminDocumentReducerState,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some(role_id) = group_role_id_from_path(path) {
            group.roles.remove(&role_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some(role_id) = group_role_id_from_path(path) else {
            continue;
        };
        group.roles.remove(&role_id);
        if version.value.is_some() && !reducer_state.conflicts.contains_key(path) {
            group.roles.insert(role_id);
        }
    }
}

fn group_metadata_conflicted(reducer_state: &AdminDocumentReducerState) -> bool {
    reducer_state
        .conflicts
        .contains_key(GROUP_DISPLAY_NAME_PATH)
        || reducer_state.conflicts.contains_key(GROUP_REALM_ID_PATH)
        || reducer_state.conflicts.contains_key(GROUP_OWNER_PATH)
}

fn group_reducer_materialized_group(
    group_id: Ulid,
    reducer_state: &AdminDocumentReducerState,
) -> Option<Group> {
    if group_metadata_conflicted(reducer_state) {
        return None;
    }

    Some(Group {
        display_name: reducer_state.materialized_group_display_name()?,
        group_id,
        realm_id: reducer_state.materialized_group_realm_id()?,
        owner: reducer_state.materialized_group_owner()?,
        roles: reducer_state
            .materialized_group_roles()
            .into_iter()
            .collect(),
    })
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

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_DESCRIPTION_PATH)
        && let Some(description) = reducer_state.materialized_realm_config_description()
    {
        config.description = description;
    }

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_QUOTA_PATH)
        && let Some(quota) = reducer_state.materialized_realm_config_quota()
    {
        config.quota = quota;
    }

    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = realm_config_node_id_from_path(path) {
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
        if let Some(provider_id) = realm_config_oidc_provider_id_from_path(path) {
            remove_realm_config_oidc_provider(config, provider_id);
        }
    }

    for path in reducer_state.user_subject_ids.keys() {
        let Some(provider_id) = realm_config_oidc_provider_id_from_path(path) else {
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

    overlay_realm_config_placement_reducer_materialization(config, reducer_state);
}

fn realm_config_from_reducer_materialization(
    realm_id: RealmId,
    reducer_state: &AdminDocumentReducerState,
) -> Option<RealmConfigDocument> {
    let metadata_replication = reducer_state.materialized_realm_config_metadata_replication()?;
    let discovery = reducer_state.materialized_realm_config_discovery()?;
    let mut config = RealmConfigDocument {
        realm_id,
        metadata_replication,
        oidc_providers: Vec::new(),
        discovery,
        nodes: Vec::new(),
        quota: reducer_state
            .materialized_realm_config_quota()
            .unwrap_or_default(),
        description: String::new(),
        placement_map: Vec::new(),
        strategies: Vec::new(),
        default_strategy_id: None,
        strategy_bindings: Vec::new(),
        placement_overrides: Vec::new(),
    };
    overlay_realm_config_reducer_materialization(&mut config, reducer_state);
    Some(config)
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

fn admin_document_target_for_reduced_document(
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
    change: DocumentSyncChange,
) -> Result<bool> {
    let Some(writes) =
        metadata_document_lifecycle_write_entries_if_current(storage, record, &change).await?
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
        let mut deletes = metadata_registry_delete_entries(group_id, document_id);
        deletes.push(document_placement_delete_entry(
            delete.tombstone.realm_id,
            &DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
        ));
        storage_batch_delete_to(storage, deletes).await?;
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

async fn persist_stale_admin_document_event(
    storage: &StorageHandle,
    apply_status: AdminDocumentApplyStatus,
    reducer_state: &AdminDocumentReducerState,
) -> Result<bool> {
    match apply_status {
        AdminDocumentApplyStatus::Applied => Ok(false),
        AdminDocumentApplyStatus::Duplicate => Ok(true),
        AdminDocumentApplyStatus::StaleOriginSequence => {
            storage_batch_write_to(
                storage,
                vec![
                    admin_document_reducer_state_write_entry(reducer_state)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                ],
            )
            .await?;
            Ok(true)
        }
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
    let changed_subject_id = match &event.op {
        AdminDocumentOperation::UserSubjectIdAdded { subject_id }
        | AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => Some(subject_id.clone()),
        _ => None,
    };

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| decode_admin_document_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_admin_document_event(storage, apply_status, &reducer_state).await? {
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
    writes.extend(
        admin_document_conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let deletes =
        stale_admin_document_conflict_delete_entries(previous_state.as_ref(), Some(&reducer_state));
    let subject_ids = changed_subject_id
        .map(|subject_id| vec![subject_id])
        .unwrap_or_else(|| user.subject_ids.clone());
    if subject_ids.is_empty() {
        return storage_batch_delete_and_write_transactionally(storage, deletes, writes).await;
    }

    for _ in 0..3 {
        let txn_id = start_storage_transaction(storage).await?;
        let mut attempt_writes = writes.clone();
        let mut attempt_deletes = deletes.clone();
        for subject_id in &subject_ids {
            let subject_key = subject_index_key(subject_id);
            let mut claims = match storage_read_from_transaction(
                storage,
                USER_SUBJECT_CLAIMS_KEYSPACE.to_string(),
                subject_key.clone(),
                Some(txn_id),
            )
            .await?
            {
                Some(bytes) => postcard::from_bytes::<BTreeSet<UserId>>(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                None => {
                    let mut claims = BTreeSet::new();
                    if let Some(bytes) = storage_read_from_transaction(
                        storage,
                        USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                        subject_key.clone(),
                        Some(txn_id),
                    )
                    .await?
                    {
                        claims.insert(
                            UserId::from_storage_key(&bytes)
                                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                        );
                    }
                    claims
                }
            };
            if user.subject_ids.contains(subject_id) {
                claims.insert(user_id);
            } else {
                claims.remove(&user_id);
            }

            if let Some(canonical_user_id) = claims.first().copied() {
                attempt_writes.push((
                    USER_SUBJECT_CLAIMS_KEYSPACE.to_string(),
                    subject_key.clone(),
                    postcard::to_allocvec(&claims)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?
                        .into(),
                ));
                attempt_writes.push((
                    USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                    subject_key,
                    subject_index_value(canonical_user_id),
                ));
            } else {
                attempt_deletes.push((
                    USER_SUBJECT_CLAIMS_KEYSPACE.to_string(),
                    subject_key.clone(),
                ));
                attempt_deletes.push((USER_SUBJECT_INDEX_KEYSPACE.to_string(), subject_key));
            }
        }
        match storage_batch_delete_and_write_in_transaction(
            storage,
            txn_id,
            attempt_deletes,
            attempt_writes,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(NetError::Dht(message))
                if message == StorageError::TransactionConflict.to_string() =>
            {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht(
        "user subject claim apply conflicted three times".to_string(),
    ))
}

async fn group_write_entries_from_reducer(
    storage: &StorageHandle,
    group_id: Ulid,
    reducer_state: &AdminDocumentReducerState,
) -> Result<Vec<(String, ByteView, Value)>> {
    let target = DocumentSyncTarget::Group { group_id };
    let group =
        match storage_read_from(storage, GROUP_KEYSPACE.to_string(), target.storage_key()).await? {
            Some(bytes) => {
                let mut group = Group::from_bytes(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                if group.group_id != group_id {
                    return Err(NetError::Bootstrap(format!(
                        "stored group document id {group_id} does not match payload group id {}",
                        group.group_id
                    )));
                }
                overlay_group_reducer_materialization(&mut group, reducer_state);
                group
            }
            None => {
                let Some(group) = group_reducer_materialized_group(group_id, reducer_state) else {
                    return Ok(Vec::new());
                };
                group
            }
        };

    Ok(vec![
        target_write_entry(
            target,
            postcard::to_allocvec(&group)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        (
            GROUP_OWNER_INDEX_KEYSPACE.to_string(),
            group_owner_index_key(group.owner, group.group_id).into(),
            ByteView::from(Vec::new()),
        ),
    ])
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
        AdminDocumentOperation::GroupCreated { .. }
            | AdminDocumentOperation::GroupRoleAdded { .. }
            | AdminDocumentOperation::GroupRoleCreated { .. }
            | AdminDocumentOperation::GroupRoleRemoved { .. }
            | AdminDocumentOperation::GroupRoleUserAssignmentAdded { .. }
            | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "group admin operation sync only supports group creation, role seeds, role creation/removal, and role user assignment updates"
                .to_string(),
        ));
    }

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| decode_admin_document_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_admin_document_event(storage, apply_status, &reducer_state).await? {
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
    let group_writes = group_write_entries_from_reducer(storage, group_id, &reducer_state).await?;

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
    writes.extend(group_writes);
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
    .map(|bytes| decode_admin_document_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_admin_document_event(storage, apply_status, &reducer_state).await? {
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
            | AdminDocumentOperation::RealmConfigDescriptionSet { .. }
            | AdminDocumentOperation::RealmConfigQuotaSet { .. }
            | AdminDocumentOperation::RealmConfigNodePlacementSet { .. }
            | AdminDocumentOperation::RealmConfigNodePlacementRemoved { .. }
            | AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { .. }
            | AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { .. }
            | AdminDocumentOperation::RealmConfigDefaultStrategySet { .. }
            | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
            | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
            | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
            | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports node ensure, OIDC provider updates, settings updates, description updates, quota updates, and placement updates"
                .to_string(),
        ));
    }

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| decode_admin_document_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_admin_document_event(storage, apply_status, &reducer_state).await? {
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
    let config = match previous_config {
        Some(mut config) => {
            if config.realm_id != realm_id {
                return Err(NetError::Bootstrap(format!(
                    "stored realm config document id {realm_id} does not match payload realm id {}",
                    config.realm_id
                )));
            }
            overlay_realm_config_reducer_materialization(&mut config, &reducer_state);
            Some(config)
        }
        None => realm_config_from_reducer_materialization(realm_id, &reducer_state),
    };
    if let Some(config) = config {
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

    if let AdminDocumentOperation::GroupRoleRemoved { role_id } = &event.op {
        auth_doc.roles.remove(role_id);
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
            if reducer_state.conflicts.contains_key(USER_NAME_PATH) {
                user.name.clear();
            } else if let Some(name) = reducer_state.materialized_user_name() {
                user.name = name;
            }
        }
        AdminDocumentOperation::UserSubjectIdAdded { subject_id }
        | AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => {
            let path = user_subject_id_path(subject_id);
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
            let path = user_attribute_path(key);
            if reducer_state.conflicts.contains_key(&path) {
                user.attributes.remove(key);
            } else {
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
    change: &DocumentSyncChange,
) -> Result<Option<Vec<(String, ByteView, Value)>>> {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: record.document_id(),
    };
    if incoming_metadata_document_lifecycle_stale_or_equal(storage, &target, change).await? {
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
        document_sync_revision_write_entry(&target, change)
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
    storage_read_from_transaction(storage, key_space, key, None).await
}

async fn storage_read_from_transaction(
    storage: &StorageHandle,
    key_space: String,
    key: ByteView,
    txn_id: Option<TxnId>,
) -> Result<Option<Value>> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space,
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while applying document sync read: {other:?}"
        ))),
    }
}

async fn start_storage_transaction(storage: &StorageHandle) -> Result<TxnId> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while starting document sync transaction: {other:?}"
        ))),
    }
}

async fn apply_watch_subscription_change_to_storage(
    storage: &StorageHandle,
    target: DocumentSyncTarget,
    bytes: Option<Vec<u8>>,
    change: DocumentSyncChange,
) -> Result<bool> {
    for _ in 0..2 {
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
                    "unexpected transaction start while applying watch subscription: {other:?}"
                )));
            }
        };

        let current = match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
                key: document_sync_revision_key(&target),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => match value
                .map(|value| postcard::from_bytes::<DocumentSyncChange>(value.as_ref()))
                .transpose()
            {
                Ok(current) => current,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(NetError::Bootstrap(error.to_string()));
                }
            },
            Event::Storage(StorageEvent::Error { error }) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(NetError::Dht(error.to_string()));
            }
            other => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(NetError::Dht(format!(
                    "unexpected revision read while applying watch subscription: {other:?}"
                )));
            }
        };

        // Watch ids are immutable and never reused. The first valid upsert wins,
        // and any delete permanently fences delayed/replayed creates.
        let apply = match (current.as_ref().map(|local| local.kind), change.kind) {
            (None, _) => true,
            (Some(DocumentSyncChangeKind::Upsert), DocumentSyncChangeKind::Delete) => true,
            (Some(DocumentSyncChangeKind::Upsert), DocumentSyncChangeKind::Upsert)
            | (Some(DocumentSyncChangeKind::Delete), _) => false,
        };
        if !apply {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        }

        let realm_id = match &target {
            DocumentSyncTarget::WatchSubscription { owner, .. } => owner.realm_id,
            _ => unreachable!("watch subscription apply requires a subscription target"),
        };
        let revision_entry = match document_sync_revision_write_entry(&target, &change) {
            Ok(entry) => entry,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(NetError::Bootstrap(error.to_string()));
            }
        };
        let mut writes = vec![
            revision_entry,
            (
                NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                ByteView::from(watch_interest_dirty_key(realm_id)),
                ByteView::from(Ulid::new().to_bytes().to_vec()),
            ),
        ];
        let deletes = if let Some(bytes) = bytes.as_ref() {
            writes.push((
                target.storage_keyspace().to_string(),
                target.storage_key(),
                ByteView::from(bytes.clone()),
            ));
            Vec::new()
        } else {
            vec![(target.storage_keyspace().to_string(), target.storage_key())]
        };

        match storage_batch_delete_and_write_in_transaction(storage, txn_id, deletes, writes).await
        {
            Ok(()) => return Ok(true),
            Err(NetError::Dht(message))
                if message == StorageError::TransactionConflict.to_string() =>
            {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht(
        "watch subscription apply conflicted twice".to_string(),
    ))
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
            "unexpected storage event while applying document sync batch write: {other:?}"
        ))),
    }
}

async fn storage_batch_delete_and_write_transactionally(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    let txn_id = start_storage_transaction(storage).await?;

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
                    "unexpected storage event while applying document sync transactional batch delete: {other:?}"
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
                "unexpected storage event while applying document sync transactional batch write: {other:?}"
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
            "unexpected storage event while committing document sync apply transaction: {other:?}"
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
            "unexpected storage event while applying document sync batch delete: {other:?}"
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdminOperationFamily {
    Group,
    RealmAuthorization,
    User,
    RealmConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum AdminEventDependency {
    RealmConfig(RealmId),
    RealmAuthorization(RealmId),
}

#[derive(Debug, PartialEq, Eq)]
enum AdminEventValidation {
    Accepted,
    Rejected(String),
    Deferred {
        dependency: Option<AdminEventDependency>,
        reason: String,
    },
}

fn satisfied_admin_dependency(target: &DocumentSyncTarget) -> Option<AdminEventDependency> {
    match target {
        DocumentSyncTarget::RealmConfig { realm_id } => {
            Some(AdminEventDependency::RealmConfig(*realm_id))
        }
        DocumentSyncTarget::RealmAuthorization { realm_id } => {
            Some(AdminEventDependency::RealmAuthorization(*realm_id))
        }
        _ => None,
    }
}

fn remove_deferred_admin_topic(
    deferred_topics: &mut BTreeMap<AdminEventDependency, BTreeSet<irokle_crate::TopicId>>,
    topic_id: irokle_crate::TopicId,
) {
    for topics in deferred_topics.values_mut() {
        topics.remove(&topic_id);
    }
    deferred_topics.retain(|_, topics| !topics.is_empty());
}

async fn validate_replicated_admin_event(
    storage: &StorageHandle,
    topic_id: irokle_crate::TopicId,
    authenticated_actor_id: irokle_crate::ActorId,
    target: &DocumentSyncTarget,
    event: &AdminDocumentEvent,
) -> Result<AdminEventValidation> {
    let reject = |reason: &str| Ok(AdminEventValidation::Rejected(reason.to_string()));

    if target.sync_topic_id() != topic_id {
        return reject("document sync target does not belong to the reconciled topic");
    }
    if event.origin_node_id != event.actor.node_id {
        return reject("event origin node does not match its actor node");
    }
    let expected_actor_id =
        irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&event.origin_node_id));
    if authenticated_actor_id != expected_actor_id {
        return reject("signed publisher does not match the event origin node");
    }
    if event.actor.user_id.realm_id != event.actor.realm_id {
        return reject("actor user and actor realm do not match");
    }
    if event.event_id.is_nil() || event.origin_seq == 0 {
        return reject("event id and origin sequence must be non-zero");
    }
    if event
        .observed
        .sequence_for(&event.origin_node_id)
        .checked_add(1)
        != Some(event.origin_seq)
    {
        return reject("event origin sequence does not follow its observed clock");
    }

    // This match is deliberately exhaustive. Adding an operation requires an
    // explicit inbound authorization decision here before it can reach storage.
    let family = match &event.op {
        AdminDocumentOperation::GroupRoleAdded { .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentAdded { .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { .. }
        | AdminDocumentOperation::GroupRoleCreated { .. }
        | AdminDocumentOperation::GroupRoleRemoved { .. }
        | AdminDocumentOperation::GroupCreated { .. } => AdminOperationFamily::Group,
        AdminDocumentOperation::RealmRoleAdded { .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentAdded { .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { .. }
        | AdminDocumentOperation::RealmRoleCreated { .. } => {
            AdminOperationFamily::RealmAuthorization
        }
        AdminDocumentOperation::UserAttributeSet { .. }
        | AdminDocumentOperation::UserAttributeRemoved { .. }
        | AdminDocumentOperation::UserNameSet { .. }
        | AdminDocumentOperation::UserSubjectIdAdded { .. }
        | AdminDocumentOperation::UserSubjectIdRemoved { .. } => AdminOperationFamily::User,
        AdminDocumentOperation::RealmConfigNodeEnsured { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderUpserted { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderRemoved { .. }
        | AdminDocumentOperation::RealmConfigSettingsSet { .. }
        | AdminDocumentOperation::RealmConfigDescriptionSet { .. }
        | AdminDocumentOperation::RealmConfigQuotaSet { .. }
        | AdminDocumentOperation::RealmConfigNodePlacementSet { .. }
        | AdminDocumentOperation::RealmConfigNodePlacementRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { .. }
        | AdminDocumentOperation::RealmConfigDefaultStrategySet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. } => {
            AdminOperationFamily::RealmConfig
        }
    };

    let target_matches = matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::Group,
            DocumentSyncTarget::GroupAuthorization { group_id },
            AdminDocumentTarget::Group { group_id: event_group_id }
        ) if group_id == event_group_id
    ) || matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::RealmAuthorization,
            DocumentSyncTarget::RealmAuthorization { realm_id },
            AdminDocumentTarget::Realm { realm_id: event_realm_id }
        ) if realm_id == event_realm_id
    ) || matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::User,
            DocumentSyncTarget::User { user_id },
            AdminDocumentTarget::User { user_id: event_user_id }
        ) if user_id == event_user_id
    ) || matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::RealmConfig,
            DocumentSyncTarget::RealmConfig { realm_id },
            AdminDocumentTarget::RealmConfig { realm_id: event_realm_id }
        ) if realm_id == event_realm_id
    );
    if !target_matches {
        return reject("operation, sync target, and admin event target do not match");
    }

    let target_realm = match &event.target {
        AdminDocumentTarget::Realm { realm_id } | AdminDocumentTarget::RealmConfig { realm_id } => {
            Some(*realm_id)
        }
        AdminDocumentTarget::User { user_id } => Some(user_id.realm_id),
        AdminDocumentTarget::Group { .. } => None,
    };
    if target_realm.is_some_and(|realm_id| realm_id != event.actor.realm_id) {
        return reject("admin event target and actor realms do not match");
    }

    match &event.op {
        AdminDocumentOperation::GroupCreated {
            realm_id, owner, ..
        } => {
            if *realm_id != event.actor.realm_id
                || owner.realm_id != *realm_id
                || *owner != event.actor.user_id
                || owner.is_nil()
            {
                return reject("group creation realm and owner must match the actor");
            }
        }
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { user_id, .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { user_id, .. } => {
            if user_id.realm_id != event.actor.realm_id {
                return reject("role assignment user belongs to a different realm");
            }
        }
        AdminDocumentOperation::GroupRoleAdded { .. }
        | AdminDocumentOperation::GroupRoleCreated { .. }
        | AdminDocumentOperation::GroupRoleRemoved { .. }
        | AdminDocumentOperation::RealmRoleAdded { .. }
        | AdminDocumentOperation::RealmRoleCreated { .. }
        | AdminDocumentOperation::UserAttributeSet { .. }
        | AdminDocumentOperation::UserAttributeRemoved { .. }
        | AdminDocumentOperation::UserNameSet { .. }
        | AdminDocumentOperation::UserSubjectIdAdded { .. }
        | AdminDocumentOperation::UserSubjectIdRemoved { .. }
        | AdminDocumentOperation::RealmConfigNodeEnsured { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderUpserted { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderRemoved { .. }
        | AdminDocumentOperation::RealmConfigSettingsSet { .. }
        | AdminDocumentOperation::RealmConfigDescriptionSet { .. }
        | AdminDocumentOperation::RealmConfigQuotaSet { .. }
        | AdminDocumentOperation::RealmConfigNodePlacementRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { .. }
        | AdminDocumentOperation::RealmConfigDefaultStrategySet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. } => {}
        AdminDocumentOperation::RealmConfigNodePlacementSet { entry }
            if entry.labels.contains_key(KIND_LABEL_KEY) =>
        {
            return reject(&format!(
                "placement entry must not set reserved label {KIND_LABEL_KEY}"
            ));
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy }
            if strategy.replica_count == Some(0) =>
        {
            return reject("placement strategy replica count must be greater than zero");
        }
        AdminDocumentOperation::RealmConfigNodePlacementSet { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { .. } => {}
    }

    let previous_state = read_admin_reducer_state(storage, &event.target).await?;
    let authorized = match family {
        AdminOperationFamily::RealmConfig => {
            validate_realm_config_admin_authority(storage, event, previous_state.as_ref()).await?
        }
        AdminOperationFamily::RealmAuthorization => {
            validate_realm_authorization_admin_authority(storage, event, previous_state.as_ref())
                .await?
        }
        AdminOperationFamily::Group => validate_group_admin_authority(storage, event).await?,
        AdminOperationFamily::User => {
            validate_user_admin_authority(storage, event, previous_state.as_ref()).await?
        }
    };
    if !matches!(authorized, AdminEventValidation::Accepted) {
        return Ok(authorized);
    }

    let mut reducer_state =
        previous_state.unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    if reducer_state.target != event.target {
        return reject("stored admin reducer state has the wrong target");
    }
    if let Err(error) = reducer_state.apply(event) {
        return Ok(AdminEventValidation::Rejected(format!(
            "admin operation is malformed: {error}"
        )));
    }

    Ok(AdminEventValidation::Accepted)
}

async fn read_admin_reducer_state(
    storage: &StorageHandle,
    target: &AdminDocumentTarget,
) -> Result<Option<AdminDocumentReducerState>> {
    storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(target),
    )
    .await?
    .map(|bytes| decode_admin_document_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

async fn read_admin_realm_config(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<Option<RealmConfigDocument>> {
    storage_read_from(
        storage,
        DocumentSyncTarget::RealmConfig { realm_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::RealmConfig { realm_id }.storage_key(),
    )
    .await?
    .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

async fn read_admin_realm_authorization(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<Option<RealmAuthorizationDocument>> {
    storage_read_from(
        storage,
        DocumentSyncTarget::RealmAuthorization { realm_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::RealmAuthorization { realm_id }.storage_key(),
    )
    .await?
    .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

fn configured_node_kind<'a>(
    config: &'a RealmConfigDocument,
    node_id: &NodeId,
) -> Option<&'a RealmNodeKind> {
    let node_id = node_id.to_string();
    config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .map(|node| &node.kind)
}

async fn validate_realm_config_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::RealmConfig { realm_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a realm config".to_string(),
        ));
    };
    let current_config = read_admin_realm_config(storage, realm_id).await?;
    if let Some(config) = current_config.as_ref() {
        if config.realm_id != realm_id {
            return Ok(AdminEventValidation::Rejected(
                "stored realm config has the wrong realm".to_string(),
            ));
        }
        return Ok(
            if matches!(
                configured_node_kind(config, &event.origin_node_id),
                Some(RealmNodeKind::Management)
            ) {
                AdminEventValidation::Accepted
            } else {
                AdminEventValidation::Rejected(
                    "event origin is not a current management node".to_string(),
                )
            },
        );
    }

    let bootstrap = previous_state.is_none()
        && matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id,
                kind: RealmNodeKind::Management,
            } if *node_id == event.origin_node_id
        );
    let continuing_bootstrap = previous_state.is_some_and(|state| {
        state
            .materialized_realm_config_nodes()
            .get(&event.origin_node_id)
            .is_some_and(|kind| matches!(kind, RealmNodeKind::Management))
    });
    Ok(if bootstrap || continuing_bootstrap {
        AdminEventValidation::Accepted
    } else {
        AdminEventValidation::Deferred {
            dependency: None,
            reason: "realm config bootstrap must begin with a management node ensuring itself"
                .to_string(),
        }
    })
}

async fn validate_realm_authorization_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::Realm { realm_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a realm authorization document".to_string(),
        ));
    };
    let current_config = read_admin_realm_config(storage, realm_id).await?;
    if let Some(config) = current_config.as_ref() {
        if config.realm_id != realm_id {
            return Ok(AdminEventValidation::Rejected(
                "stored realm config has the wrong realm".to_string(),
            ));
        }
        return Ok(
            if matches!(
                configured_node_kind(config, &event.origin_node_id),
                Some(RealmNodeKind::Management)
            ) {
                AdminEventValidation::Accepted
            } else {
                AdminEventValidation::Rejected(
                    "event origin is not a current management node".to_string(),
                )
            },
        );
    }

    let current_auth = read_admin_realm_authorization(storage, realm_id).await?;
    let bootstrap = previous_state.is_none()
        && current_auth.is_none()
        && event.actor.user_id.is_nil_in(realm_id)
        && matches!(
            &event.op,
            AdminDocumentOperation::RealmRoleCreated { role }
                if !role.role_id.is_nil()
                    && role.name == "realm_admin"
                    && role.permissions == BTreeMap::from([(
                        format!("/{realm_id}/admin/**"),
                        aruna_core::structs::Permission::WRITE,
                    )])
        );
    Ok(if bootstrap {
        AdminEventValidation::Accepted
    } else {
        AdminEventValidation::Deferred {
            dependency: Some(AdminEventDependency::RealmConfig(realm_id)),
            reason: "current realm config is unavailable".to_string(),
        }
    })
}

async fn validate_group_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::Group { group_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a group".to_string(),
        ));
    };
    let realm_id = event.actor.realm_id;
    let Some(config) = read_admin_realm_config(storage, realm_id).await? else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(AdminEventDependency::RealmConfig(realm_id)),
            reason: "current realm config is unavailable".to_string(),
        });
    };
    if config.realm_id != realm_id || configured_node_kind(&config, &event.origin_node_id).is_none()
    {
        return Ok(AdminEventValidation::Rejected(
            "group admin event origin is not a current realm node".to_string(),
        ));
    }

    let group_value = storage_read_from(
        storage,
        GROUP_KEYSPACE.to_string(),
        group_id.to_bytes().into(),
    )
    .await?;
    let group = group_value
        .map(|bytes| Group::from_bytes(&bytes))
        .transpose()
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;

    if let AdminDocumentOperation::GroupCreated {
        realm_id: event_realm_id,
        display_name,
        owner,
    } = &event.op
    {
        return Ok(match group {
            None => AdminEventValidation::Accepted,
            Some(group)
                if group.group_id == group_id
                    && group.realm_id == *event_realm_id
                    && group.display_name == *display_name
                    && group.owner == *owner =>
            {
                AdminEventValidation::Accepted
            }
            Some(_) => AdminEventValidation::Rejected(
                "group creation conflicts with the current group".to_string(),
            ),
        });
    }

    let Some(group) = group else {
        return Ok(AdminEventValidation::Deferred {
            dependency: None,
            reason: "group does not exist".to_string(),
        });
    };
    if group.group_id != group_id || group.realm_id != realm_id || group.owner.realm_id != realm_id
    {
        return Ok(AdminEventValidation::Rejected(
            "stored group identity does not match the event".to_string(),
        ));
    }
    if group.owner == event.actor.user_id {
        return Ok(AdminEventValidation::Accepted);
    }
    if matches!(
        &event.op,
        AdminDocumentOperation::GroupRoleUserAssignmentRemoved { user_id, .. }
            if *user_id == event.actor.user_id
    ) {
        return Ok(AdminEventValidation::Accepted);
    }

    let realm_auth = read_admin_realm_authorization(storage, realm_id).await?;
    let group_auth = storage_read_from(
        storage,
        DocumentSyncTarget::GroupAuthorization { group_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::GroupAuthorization { group_id }.storage_key(),
    )
    .await?
    .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let Some(group_auth) = group_auth else {
        return Ok(AdminEventValidation::Rejected(
            "group authorization state is unavailable".to_string(),
        ));
    };
    let Some(realm_auth) = realm_auth else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(AdminEventDependency::RealmAuthorization(realm_id)),
            reason: "realm authorization state is unavailable".to_string(),
        });
    };
    if realm_auth.realm_id != realm_id || group_auth.group_id != group_id {
        return Ok(AdminEventValidation::Rejected(
            "stored authorization identity does not match the group".to_string(),
        ));
    }
    let path = match &event.op {
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { user_id, .. } => {
            format!("/{realm_id}/g/{group_id}/admin/users/{user_id}")
        }
        AdminDocumentOperation::GroupRoleAdded { .. }
        | AdminDocumentOperation::GroupRoleCreated { .. }
        | AdminDocumentOperation::GroupRoleRemoved { .. } => {
            format!("/{realm_id}/g/{group_id}/admin")
        }
        AdminDocumentOperation::GroupCreated { .. } => unreachable!(),
        _ => unreachable!("group authority only receives group operations"),
    };
    let allowed = has_current_write_permission(
        event.actor.user_id,
        &path,
        realm_auth.roles.values().chain(group_auth.roles.values()),
    );
    Ok(if allowed {
        AdminEventValidation::Accepted
    } else {
        AdminEventValidation::Rejected("actor lacks current group write authority".to_string())
    })
}

async fn validate_user_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::User { user_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a user".to_string(),
        ));
    };
    let realm_id = user_id.realm_id;
    let Some(config) = read_admin_realm_config(storage, realm_id).await? else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(AdminEventDependency::RealmConfig(realm_id)),
            reason: "current realm config is unavailable".to_string(),
        });
    };
    if config.realm_id != realm_id {
        return Ok(AdminEventValidation::Rejected(
            "stored realm config has the wrong realm".to_string(),
        ));
    }
    let Some(origin_kind) = configured_node_kind(&config, &event.origin_node_id) else {
        return Ok(AdminEventValidation::Rejected(
            "user admin event origin is not a current realm node".to_string(),
        ));
    };

    let current_user = storage_read_from(
        storage,
        DocumentSyncTarget::User { user_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::User { user_id }.storage_key(),
    )
    .await?
    .map(|bytes| User::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if current_user
        .as_ref()
        .is_some_and(|user| user.user_id != user_id)
    {
        return Ok(AdminEventValidation::Rejected(
            "stored user identity does not match the event".to_string(),
        ));
    }

    let self_service = event.actor.user_id == user_id;
    let management_bootstrap = event.actor.user_id.is_nil_in(realm_id)
        && matches!(origin_kind, RealmNodeKind::Management)
        && event.origin_seq <= 2
        && previous_state.is_none_or(|state| {
            state
                .clock
                .origins
                .keys()
                .all(|origin| *origin == event.origin_node_id)
        });
    let realm_admin = if self_service || management_bootstrap {
        false
    } else {
        let Some(auth) = read_admin_realm_authorization(storage, realm_id).await? else {
            return Ok(AdminEventValidation::Deferred {
                dependency: Some(AdminEventDependency::RealmAuthorization(realm_id)),
                reason: "realm authorization state is unavailable".to_string(),
            });
        };
        if auth.realm_id != realm_id {
            return Ok(AdminEventValidation::Rejected(
                "stored realm authorization has the wrong realm".to_string(),
            ));
        }
        has_current_write_permission(
            event.actor.user_id,
            &format!("/{realm_id}/admin/u/{user_id}"),
            auth.roles.values(),
        )
    };
    if !self_service && !management_bootstrap && !realm_admin {
        return Ok(AdminEventValidation::Rejected(
            "actor lacks current user write authority".to_string(),
        ));
    }

    Ok(AdminEventValidation::Accepted)
}

fn has_current_write_permission<'a>(
    user_id: UserId,
    path: &str,
    roles: impl IntoIterator<Item = &'a Role>,
) -> bool {
    let mut allowed = false;
    for role in roles {
        if user_id.is_nil() || !role.assigned_users.contains(&user_id) {
            continue;
        }
        for (pattern, permission) in &role.permissions {
            let Ok(glob) = Glob::new(pattern) else {
                return false;
            };
            if !glob.compile_matcher().is_match(path) {
                continue;
            }
            match permission {
                aruna_core::structs::Permission::DENY => return false,
                aruna_core::structs::Permission::WRITE => allowed = true,
                aruna_core::structs::Permission::READ => {}
            }
        }
    }
    allowed
}

/// Validates the self-consistency of a replicated node-usage snapshot against
/// its sync target: the payload must decode, its embedded `node_id` must match
/// the target's node, and the target's derived storage key must attribute back
/// to that same node. Returns a human-readable reason on rejection. Does not
/// check the publisher's identity (the caller enforces that against the signed
/// actor). A zero-counter snapshot is valid: stale-group cleanup publishes them.
fn validate_node_usage_upsert(
    target: &DocumentSyncTarget,
    bytes: &[u8],
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::NodeUsage { node_id, .. } = target else {
        return Err("target is not a node usage snapshot".to_string());
    };
    let snapshot = NodeUsageSnapshot::from_bytes(bytes)
        .map_err(|error| format!("undecodable node usage snapshot: {error}"))?;
    if snapshot.node_id != *node_id {
        return Err(format!(
            "snapshot node id {} does not match target node id {node_id}",
            snapshot.node_id
        ));
    }
    let key = target.storage_key();
    if node_usage_key_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "node usage storage key does not attribute to target node id {node_id}"
        ));
    }
    Ok(())
}

/// Validates the self-consistency of a replicated watch-interest digest against
/// its sync target. Does not check the publisher's identity (the caller enforces
/// that against the signed actor). Empty digests are valid: they clear a node's
/// interest for the realm while preserving single-writer ownership.
fn validate_watch_interest_upsert(
    target: &DocumentSyncTarget,
    bytes: &[u8],
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::WatchInterest { realm_id, node_id } = target else {
        return Err("target is not a watch interest digest".to_string());
    };
    let digest = WatchInterestDigest::from_bytes(bytes)
        .map_err(|error| format!("undecodable watch interest digest: {error}"))?;
    if digest.node_id != *node_id {
        return Err(format!(
            "digest node id {} does not match target node id {node_id}",
            digest.node_id
        ));
    }
    let key = target.storage_key();
    if watch_interest_key_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target node id {node_id}"
        ));
    }
    if watch_interest_key_realm_id(key.as_ref()) != Some(*realm_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target realm id {realm_id}"
        ));
    }
    Ok(())
}

fn validate_watch_subscription_upsert(
    target: &DocumentSyncTarget,
    bytes: &[u8],
    change: &DocumentSyncChange,
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::WatchSubscription { owner, watch_id } = target else {
        return Err("target is not a watch subscription".to_string());
    };
    validate_watch_subscription_target(*owner, *watch_id)?;
    if change.kind != DocumentSyncChangeKind::Upsert || change.current.generation != 1 {
        return Err(
            "watch subscription upsert must carry generation 1 upsert revision".to_string(),
        );
    }
    let subscription = WatchSubscription::from_bytes(bytes)
        .map_err(|error| format!("undecodable watch subscription: {error}"))?;
    if subscription.owner != *owner || subscription.watch_id != *watch_id {
        return Err("watch subscription payload does not match its target".to_string());
    }
    if subscription.path_prefix.is_empty()
        || subscription.path_prefix.starts_with('/')
        || subscription.path_prefix.len() > NOTIFICATION_WATCH_MAX_PREFIX_LEN
    {
        return Err("watch subscription path prefix is invalid".to_string());
    }
    let known_mask = WatchEventMask::METADATA_CREATED | WatchEventMask::DATA_UPLOADED;
    if subscription.event_mask.is_empty() || subscription.event_mask.bits() & !known_mask != 0 {
        return Err("watch subscription event mask is invalid".to_string());
    }
    Ok(())
}

fn validate_watch_subscription_delete(
    target: &DocumentSyncTarget,
    change: &DocumentSyncChange,
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::WatchSubscription { owner, watch_id } = target else {
        return Err("target is not a watch subscription".to_string());
    };
    validate_watch_subscription_target(*owner, *watch_id)?;
    if change.kind != DocumentSyncChangeKind::Delete || change.current.generation != 2 {
        return Err(
            "watch subscription delete must carry generation 2 delete revision".to_string(),
        );
    }
    Ok(())
}

fn validate_watch_subscription_target(
    owner: UserId,
    watch_id: Ulid,
) -> std::result::Result<(), String> {
    if owner.is_nil() {
        return Err("watch subscription owner must not be nil".to_string());
    }
    if watch_id.is_nil() {
        return Err("watch subscription id must not be nil".to_string());
    }
    Ok(())
}

/// Validates the self-consistency of a replicated node-info document against its
/// sync target: the payload must decode and its embedded `node_id` must match the
/// target's node. Does not check the publisher's identity (the caller enforces
/// that against the signed actor).
fn validate_node_info_upsert(
    target: &DocumentSyncTarget,
    bytes: &[u8],
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::NodeInfo { node_id, .. } = target else {
        return Err("target is not a node info document".to_string());
    };
    let document = NodeInfoDocument::from_bytes(bytes)
        .map_err(|error| format!("undecodable node info document: {error}"))?;
    if document.node_id != *node_id {
        return Err(format!(
            "node info document node id {} does not match target node id {node_id}",
            document.node_id
        ));
    }
    Ok(())
}

fn topic_cursor_key(topic_id: irokle_crate::TopicId) -> ByteView {
    let mut key = b"topic-cursor/".to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

fn deferred_admin_topics_key() -> ByteView {
    ByteView::from(b"deferred-admin-topics".to_vec())
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
        if messages.len() >= DOCUMENT_SYNC_INBOUND_SYNC_MESSAGE_LIMIT {
            return Err(NetError::Stream(format!(
                "document sync stream exceeded {DOCUMENT_SYNC_INBOUND_SYNC_MESSAGE_LIMIT} messages"
            )));
        }
        let message = decode_sync_message(&frame).map_err(|error| {
            NetError::Stream(format!(
                "invalid document sync message frame {frame_index} ({} bytes): {error}",
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
                "incomplete document sync frame length".to_string(),
            ));
        };
        if n == 0 {
            return Err(NetError::Stream(
                "incomplete document sync frame length".to_string(),
            ));
        }
        read += n;
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > DOCUMENT_SYNC_FRAME_LEN_LIMIT {
        return Err(NetError::Stream(
            "document sync frame exceeds maximum length".to_string(),
        ));
    }
    *bytes_read = bytes_read.saturating_add(4).saturating_add(len);
    if *bytes_read > DOCUMENT_SYNC_INBOUND_SYNC_STREAM_BYTES {
        return Err(NetError::Stream(format!(
            "document sync stream exceeded {DOCUMENT_SYNC_INBOUND_SYNC_STREAM_BYTES} bytes"
        )));
    }

    let mut payload = vec![0u8; len];
    let mut payload_read = 0usize;
    while payload_read < payload.len() {
        let Some(n) = read_some_inbound_sync(recv, &mut payload[payload_read..]).await? else {
            return Err(NetError::Stream(
                "incomplete document sync frame payload".to_string(),
            ));
        };
        if n == 0 {
            return Err(NetError::Stream(
                "incomplete document sync frame payload".to_string(),
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
    timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT, recv.read(buf))
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
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
        timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT, send.write_all(&frame))
            .await
            .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
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
                        "Skipping document sync batch topic: peer returned mismatched fingerprint"
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
                        "Skipping document sync batch topic: peer advertised unexpected event type"
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
                            "Skipping document sync batch topic: sync negotiation failed"
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
                    "unexpected document sync batch response from {peer}: {other:?}"
                )));
            }
        }
    }
    Ok((responded_topics, failed_topics, sync_messages))
}

fn forward_evictions_to(
    sink: &tokio::sync::mpsc::UnboundedSender<TopicEviction>,
    evictions: Vec<TopicEviction>,
) {
    for eviction in evictions {
        if sink.send(eviction).is_err() {
            warn!("Document sync eviction consumer closed; dropping re-emitted payloads");
        }
    }
}

fn process_batch_data_responses(
    node: &irokle_crate::Irokle<irokle_crate::FjallStorage>,
    net: &irokle_crate::net::IrohNet<irokle_crate::FjallStorage>,
    peer: PeerId,
    known_topics: &BTreeSet<irokle_crate::TopicId>,
    mut failed_topics: BTreeSet<irokle_crate::TopicId>,
    responses: Vec<SyncMessage>,
    eviction_tx: &tokio::sync::mpsc::UnboundedSender<TopicEviction>,
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
                let ack = match node.receive_sync_data_from_evicting(peer, data) {
                    Ok((ack, evictions)) => {
                        forward_evictions_to(eviction_tx, evictions);
                        ack
                    }
                    Err(error) => {
                        warn!(
                            %peer,
                            topic_id = %topic_id,
                            error = %error,
                            "Skipping document sync batch topic: receiving sync data failed"
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
                    "unexpected document sync batch data response from {peer}: {other:?}"
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
                "Skipping document sync batch topic: applying sync ack failed"
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
        "Document sync peer batch sync round breakdown"
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
            "Document sync batch sync failed for one or more topics"
        );
        return Err(NetError::Bootstrap(format!(
            "peer {peer}: {}/{} document sync batch topics failed to sync",
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
    use aruna_core::admin_document_reducer::REALM_CONFIG_DEFAULT_STRATEGY_PATH;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use aruna_core::alpn::Alpn;
    use aruna_core::document::{DocumentSyncChangeKind, DocumentSyncRevision};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
        DOCUMENT_SYNC_REVISION_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
        METADATA_DOCUMENT_LIFECYCLE_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE,
        METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_GRAPH_PRUNE_JOB_KEYSPACE,
        METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE, USER_KEYSPACE,
        USER_SUBJECT_CLAIMS_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
    };
    use aruna_core::metadata::MetadataCreateEventPayload;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
        metadata_document_key, metadata_event_log_key, metadata_registry_key, subject_index_key,
        subject_index_value,
    };
    use aruna_core::structs::{
        Actor, BindingScope, DocumentClass, Group, GroupAuthorizationDocument, GroupQuotaOverride,
        MetadataReplicationConfig, NodePlacementEntry, OidcProviderConfig, Permission,
        PlacementOverride, PlacementStrategy, QuotaConfig, RealmAuthorizationDocument,
        RealmConfigDocument, RealmDiscoveryConfig, RealmId, RealmNodeKind, Role,
        StaticRealmEndpoint, StrategyBinding, UserGroupCapOverride,
    };
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::{env, process::Command};
    use tempfile::TempDir;

    const DOCUMENT_SYNC_RESTART_CHILD_PATH_ENV: &str = "ARUNA_NET_DOCUMENT_SYNC_RESTART_CHILD_PATH";
    const DOCUMENT_SYNC_RESTART_CHILD_TEST: &str =
        "document_sync::tests::buffered_document_sync_publish_restart_child_process";

    fn peer(seed: u8) -> PeerId {
        node_id_to_peer_id(&iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn topic(seed: u8) -> irokle_crate::TopicId {
        DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([seed; 32]),
        }
        .sync_topic_id()
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
        DocumentSyncTarget::MetadataGraphLifecycle {
            graph_iri: "urn:aruna:restart-contract".to_string(),
        }
    }

    fn restart_event_id() -> Ulid {
        Ulid::from_parts(1_727_000_000_000, 42)
    }

    fn restart_payload() -> Vec<u8> {
        postcard::to_allocvec(&MetadataGraphLifecycleRecord::deleted(
            "urn:aruna:restart-contract".to_string(),
            RealmId::from_bytes([99; 32]),
            Ulid::from_parts(99, 1),
            Ulid::from_parts(99, 2),
            1,
        ))
        .expect("restart payload serializes")
    }

    fn revision_change() -> DocumentSyncChange {
        DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: restart_event_id(),
                actor: node(43),
                updated_at_ms: 1_727_000_000_101,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: aruna_core::structs::PlacementRef::NIL,
        }
    }

    async fn test_endpoint(seed: u8) -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .relay_mode(iroh::RelayMode::Disabled)
            .alpns(vec![Alpn::DocumentSync.as_bytes().to_vec()])
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

    async fn restart_endpoint() -> iroh::Endpoint {
        test_endpoint(91).await
    }

    async fn open_restart_service(root: &Path, storage_name: &str) -> DocumentSyncService {
        DocumentSyncService::open_with_persist_policy(
            restart_endpoint().await,
            storage_at(&root.join(storage_name)),
            root.join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens")
    }

    fn run_document_sync_restart_child(root: &Path) {
        let status = Command::new(env::current_exe().expect("test binary path"))
            .arg(DOCUMENT_SYNC_RESTART_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env(DOCUMENT_SYNC_RESTART_CHILD_PATH_ENV, root)
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

    async fn apply_conflicting_user_name_and_attribute(
        storage: &StorageHandle,
        user_id: UserId,
        realm_id: RealmId,
    ) -> Actor {
        let actor_a = test_actor(8, user_id, realm_id);
        let actor_b = test_actor(9, user_id, realm_id);
        let target = AdminDocumentTarget::User { user_id };
        for (seq, actor, origin_seq, op) in [
            (
                1,
                &actor_a,
                1,
                AdminDocumentOperation::UserNameSet {
                    name: "Alice".to_string(),
                },
            ),
            (
                2,
                &actor_b,
                1,
                AdminDocumentOperation::UserNameSet {
                    name: "Mallory".to_string(),
                },
            ),
            (
                3,
                &actor_a,
                2,
                AdminDocumentOperation::UserAttributeSet {
                    key: "department".to_string(),
                    value: "physics".to_string(),
                },
            ),
            (
                4,
                &actor_b,
                2,
                AdminDocumentOperation::UserAttributeSet {
                    key: "department".to_string(),
                    value: "malware".to_string(),
                },
            ),
        ] {
            apply_admin_document_operation_to_storage(
                storage,
                DocumentSyncTarget::User { user_id },
                test_admin_event(
                    Ulid::from_parts(2_500 + seq, 1),
                    target.clone(),
                    actor,
                    origin_seq,
                    op,
                ),
            )
            .await
            .expect("conflicting user admin operation applies");
        }
        actor_a
    }

    async fn read_user_doc(storage: &StorageHandle, user_id: UserId) -> User {
        let target = DocumentSyncTarget::User { user_id };
        let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("user exists");
        User::from_bytes(&value).expect("user decodes")
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

    fn metadata_lifecycle_change(
        lifecycle: &MetadataDocumentLifecycleRecord,
        actor: NodeId,
    ) -> DocumentSyncChange {
        aruna_core::storage_entries::metadata_document_lifecycle_revision_change(
            lifecycle,
            actor,
            aruna_core::structs::PlacementRef::NIL,
        )
    }

    #[tokio::test]
    async fn realm_config_node_op_alone_stores_reducer_state_without_config_doc() {
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
        let existing_provider = test_oidc_provider("existing", "existing-settings");
        let seed_node = node(20);
        let mut seed_config =
            RealmConfigDocument::new(realm_id, vec![existing_provider.clone()], 3);
        seed_config.discovery = test_discovery(21, "https://existing-settings.example:443");
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
        assert_eq!(config.oidc_providers, vec![existing_provider]);
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
    async fn realm_config_description_admin_op_materializes_existing_config() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([53; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_415, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let mut seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        seed_config.description = "Old Realm".to_string();
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

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_416, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigDescriptionSet {
                    description: "Replicated Realm".to_string(),
                },
            ),
        )
        .await
        .expect("realm config description applies");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.description, "Replicated Realm");
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
            reducer_state
                .materialized_realm_config_description()
                .as_deref(),
            Some("Replicated Realm")
        );
    }

    #[tokio::test]
    async fn realm_config_placement_admin_ops_materialize_existing_config() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([71; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_500, 1), realm_id),
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
                    .to_bytes(&actor)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        let entry = NodePlacementEntry {
            node_id: actor.node_id,
            location: "eu-west".to_string(),
            weight: 250,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        };
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_parts(1_501, 1),
            name: "default".to_string(),
            replica_count: Some(3),
            distinct_locations: false,
            affinity: Vec::new(),
        };
        let binding = StrategyBinding {
            scope: BindingScope::Class(DocumentClass::MetadataRegistry),
            strategy_id: strategy.strategy_id,
        };
        let record = PlacementOverride {
            subject: b"document-subject".to_vec(),
            pinned: vec![actor.node_id],
            excluded: Vec::new(),
            strategy_id: Some(strategy.strategy_id),
        };

        for (index, op) in [
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: entry.clone(),
            },
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: strategy.clone(),
            },
            AdminDocumentOperation::RealmConfigDefaultStrategySet {
                strategy_id: strategy.strategy_id,
            },
            AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: binding.clone(),
            },
            AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: record.clone(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let seq = index as u64 + 1;
            apply_admin_document_operation_to_storage(
                &storage,
                document_target.clone(),
                test_admin_event(
                    Ulid::from_parts(1_502 + seq, 1),
                    target.clone(),
                    &actor,
                    seq,
                    op,
                ),
            )
            .await
            .expect("placement op applies");
        }

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.placement_map, vec![entry]);
        assert_eq!(config.strategies, vec![strategy.clone()]);
        assert_eq!(config.default_strategy_id, Some(strategy.strategy_id));
        assert_eq!(config.strategy_bindings, vec![binding]);
        assert_eq!(config.placement_overrides, vec![record]);

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
            reducer_state.materialized_realm_config_default_strategy(),
            Some(strategy.strategy_id)
        );
    }

    #[test]
    fn realm_config_overlay_clears_prior_default_strategy_on_reducer_conflict() {
        let realm_id = RealmId::from_bytes([73; 32]);
        let user_id = UserId::local(Ulid::from_parts(1_520, 1), realm_id);
        let actor_a = test_actor(35, user_id, realm_id);
        let actor_b = test_actor(36, user_id, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut state = AdminDocumentReducerState::new(target.clone());
        let prior_default = Ulid::from_parts(1_521, 1);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.default_strategy_id = Some(prior_default);

        overlay_realm_config_reducer_materialization(&mut config, &state);
        assert_eq!(config.default_strategy_id, None);

        for (event_id, actor, strategy_id) in [
            (
                Ulid::from_parts(1_522, 1),
                &actor_a,
                Ulid::from_parts(1_523, 1),
            ),
            (
                Ulid::from_parts(1_524, 1),
                &actor_b,
                Ulid::from_parts(1_525, 1),
            ),
        ] {
            state
                .apply(&test_admin_event(
                    event_id,
                    target.clone(),
                    actor,
                    1,
                    AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
                ))
                .unwrap();
        }

        assert!(
            state
                .conflicts
                .contains_key(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
        );
        assert_eq!(state.materialized_realm_config_default_strategy(), None);

        overlay_realm_config_reducer_materialization(&mut config, &state);
        assert_eq!(config.default_strategy_id, None);
    }

    #[tokio::test]
    async fn dangling_strategy_references_materialize_through_storage() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([72; 32]);
        let user_id = UserId::local(Ulid::from_parts(1_510, 1), realm_id);
        let strategy_actor = test_actor(30, user_id, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&strategy_actor)
                    .expect("seed realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed realm config writes");

        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_parts(1_511, 1),
            name: "removed".to_string(),
            replica_count: Some(3),
            distinct_locations: false,
            affinity: Vec::new(),
        };
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_512, 1),
                target.clone(),
                &strategy_actor,
                1,
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: strategy.clone(),
                },
            ),
        )
        .await
        .expect("placement strategy applies");

        let binding = StrategyBinding {
            scope: BindingScope::Class(DocumentClass::MetadataRegistry),
            strategy_id: strategy.strategy_id,
        };
        let record = PlacementOverride {
            subject: b"dangling-document-subject".to_vec(),
            pinned: vec![strategy_actor.node_id],
            excluded: Vec::new(),
            strategy_id: Some(strategy.strategy_id),
        };
        let observed_strategy =
            AdminDocumentClock::default().with_observed(strategy_actor.node_id, 1);
        for (index, (actor, op)) in [
            (
                test_actor(31, user_id, realm_id),
                AdminDocumentOperation::RealmConfigPlacementStrategyRemoved {
                    strategy_id: strategy.strategy_id,
                },
            ),
            (
                test_actor(32, user_id, realm_id),
                AdminDocumentOperation::RealmConfigDefaultStrategySet {
                    strategy_id: strategy.strategy_id,
                },
            ),
            (
                test_actor(33, user_id, realm_id),
                AdminDocumentOperation::RealmConfigStrategyBindingSet {
                    binding: binding.clone(),
                },
            ),
            (
                test_actor(34, user_id, realm_id),
                AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                    record: record.clone(),
                },
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let mut event = test_admin_event(
                Ulid::from_parts(1_513 + index as u64, 1),
                target.clone(),
                &actor,
                1,
                op,
            );
            event.observed = observed_strategy.clone();
            apply_admin_document_operation_to_storage(&storage, document_target.clone(), event)
                .await
                .expect("concurrent placement operation applies");
        }

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert!(config.strategies.is_empty());
        assert_eq!(config.default_strategy_id, None);
        assert!(config.strategy_bindings.is_empty());
        assert_eq!(config.placement_overrides.len(), 1);
        assert_eq!(config.placement_overrides[0].subject, record.subject);
        assert_eq!(config.placement_overrides[0].pinned, record.pinned);
        assert_eq!(config.placement_overrides[0].excluded, record.excluded);
        assert_eq!(config.placement_overrides[0].strategy_id, None);

        let state_value = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&state_value).expect("reducer state decodes");
        assert!(
            reducer_state
                .materialized_realm_config_placement_strategies()
                .is_empty()
        );
        assert_eq!(
            reducer_state.materialized_realm_config_default_strategy(),
            Some(strategy.strategy_id)
        );
    }

    #[tokio::test]
    async fn realm_config_settings_op_alone_creates_config_doc() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([49; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_380, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let metadata_replication = MetadataReplicationConfig::new(5);
        let discovery = test_discovery(23, "https://missing-settings.example:443");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_382, 1),
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
        .expect("realm config settings op bootstraps config doc");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.metadata_replication, metadata_replication);
        assert_eq!(config.discovery, discovery);
        assert!(config.nodes.is_empty());
        assert!(config.oidc_providers.is_empty());
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
    async fn quota_survives_reducer_materialization_without_existing_config_doc() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([61; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_384, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let quota = QuotaConfig {
            default_group_quota_bytes: Some(9_000),
            grace_factor_percent: 130,
            warn_threshold_percent: 70,
            group_overrides: vec![GroupQuotaOverride {
                group_id: Ulid::from_parts(1_385, 1),
                quota_bytes: Some(4_500),
                grace_factor_percent: Some(140),
            }],
            max_groups_per_user: Some(7),
            user_group_cap_overrides: vec![UserGroupCapOverride {
                user_id: UserId::local(Ulid::from_parts(1_386, 1), realm_id),
                max_groups: Some(2),
            }],
            max_devices_per_user: Some(6),
        };
        let expected_quota = QuotaConfig {
            max_devices_per_user: None,
            ..quota.clone()
        };

        // Quota lands before any config doc exists; it must be recorded in the
        // reducer and later carried through realm_config_from_reducer_materialization.
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_387, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigQuotaSet {
                    quota: quota.clone(),
                },
            ),
        )
        .await
        .expect("realm config quota op applies");

        let metadata_replication = MetadataReplicationConfig::new(5);
        let discovery = test_discovery(23, "https://quota-materialization.example:443");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_388, 1),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: metadata_replication.clone(),
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("realm config settings op bootstraps config doc");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.quota, expected_quota);
        assert_eq!(config.metadata_replication, metadata_replication);
    }

    #[tokio::test]
    async fn realm_config_settings_after_node_and_oidc_bootstraps_full_config() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([52; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_410, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let reducer_node = node(28);
        let provider = test_oidc_provider("default", "bootstrap-after-reducer");
        let metadata_replication = MetadataReplicationConfig::new(8);
        let discovery = test_discovery(29, "https://bootstrap-settings.example:443");

        for (seq, op) in [
            (
                1,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: reducer_node,
                    kind: RealmNodeKind::Management,
                },
            ),
            (
                2,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                    provider: provider.clone(),
                },
            ),
        ] {
            apply_admin_document_operation_to_storage(
                &storage,
                document_target.clone(),
                test_admin_event(
                    Ulid::from_parts(1_410 + seq, 1),
                    target.clone(),
                    &actor,
                    seq,
                    op,
                ),
            )
            .await
            .expect("pre-settings realm config reducer op applies");
        }
        assert!(
            read_storage_value(
                &storage,
                document_target.storage_keyspace(),
                document_target.storage_key(),
            )
            .await
            .is_none()
        );

        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(1_413, 1),
                target,
                &actor,
                3,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: metadata_replication.clone(),
                    discovery: discovery.clone(),
                },
            ),
        )
        .await
        .expect("realm config settings op bootstraps full config doc");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.metadata_replication, metadata_replication);
        assert_eq!(config.discovery, discovery);
        assert_eq!(
            realm_config_nodes(&config),
            BTreeMap::from([(reducer_node.to_string(), RealmNodeKind::Management)])
        );
        assert_eq!(
            realm_config_oidc_providers(&config),
            BTreeMap::from([("default".to_string(), provider)])
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
        let existing = test_oidc_provider("existing", "existing");
        let removed = test_oidc_provider("removed", "removed");
        let first = test_oidc_provider("default", "one");
        let second = test_oidc_provider("partner", "two");
        let seed_node = node(16);
        let mut seed_config =
            RealmConfigDocument::new(realm_id, vec![existing.clone(), removed.clone()], 5);
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
                ("existing".to_string(), existing),
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
        let path = aruna_core::admin_document_reducer::realm_config_oidc_provider_path("default");
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
    async fn user_name_and_attribute_conflicts_fail_closed_incrementally() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([44; 32]);
        let user_id = UserId::local(Ulid::from_parts(210, 1), realm_id);
        let target = AdminDocumentTarget::User { user_id };

        apply_conflicting_user_name_and_attribute(&storage, user_id, realm_id).await;

        let user = read_user_doc(&storage, user_id).await;
        assert_eq!(user.name, "");
        assert!(!user.attributes.contains_key("department"));
        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                admin_document_reducer_conflict_key(&target, USER_NAME_PATH),
            )
            .await
            .is_some()
        );
        assert!(
            read_storage_value(
                &storage,
                ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
                admin_document_reducer_conflict_key(&target, &user_attribute_path("department")),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn group_created_admin_operation_bootstraps_missing_group_doc() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([35; 32]);
        let group_id = Ulid::from_parts(190, 1);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(191, 1), realm_id),
            realm_id,
        );

        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            test_admin_event(
                Ulid::from_parts(192, 1),
                AdminDocumentTarget::Group { group_id },
                &actor,
                1,
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name: "Reduced group".to_string(),
                    owner: actor.user_id,
                },
            ),
        )
        .await
        .expect("group create applies");

        assert_eq!(
            read_group_doc(&storage, group_id).await,
            Group {
                display_name: "Reduced group".to_string(),
                group_id,
                realm_id,
                owner: actor.user_id,
                roles: HashSet::new(),
            }
        );
        assert!(
            read_storage_value(
                &storage,
                GROUP_OWNER_INDEX_KEYSPACE,
                group_owner_index_key(actor.user_id, group_id).into(),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn group_role_create_admin_operation_after_group_created_updates_group_roles() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([36; 32]);
        let group_id = Ulid::from_parts(193, 1);
        let role_id = Ulid::from_parts(194, 1);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(195, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::Group { group_id };
        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(196, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name: "Reduced group".to_string(),
                    owner: actor.user_id,
                },
            ),
        )
        .await
        .expect("group create applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(197, 1),
                target,
                &actor,
                2,
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
            read_group_doc(&storage, group_id).await.roles,
            HashSet::from([role_id])
        );
        assert!(
            read_group_auth_doc(&storage, group_id)
                .await
                .roles
                .contains_key(&role_id)
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
            owner: actor.user_id,
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
    async fn group_role_remove_admin_operation_updates_group_and_auth_docs() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([37; 32]);
        let group_id = Ulid::from_parts(198, 1);
        let role_id = Ulid::from_parts(199, 1);
        let assigned_user_id = UserId::local(Ulid::from_parts(200, 1), realm_id);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(201, 1), realm_id),
            realm_id,
        );
        let group = Group {
            display_name: "Durable group".to_string(),
            group_id,
            realm_id,
            owner: actor.user_id,
            roles: HashSet::from([role_id]),
        };
        let auth_doc = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: "custom_role".to_string(),
                    permissions: HashMap::from([(
                        "/group/custom/**".to_string(),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::from([assigned_user_id]),
                },
            )]),
        };
        storage_batch_write_to(
            &storage,
            vec![
                (
                    GROUP_KEYSPACE.to_string(),
                    group_id.to_bytes().into(),
                    group.to_bytes(&actor).expect("group serializes").into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    group_id.to_bytes().into(),
                    auth_doc
                        .to_bytes(&actor)
                        .expect("auth doc serializes")
                        .into(),
                ),
            ],
        )
        .await
        .expect("group and auth docs write");

        let target = AdminDocumentTarget::Group { group_id };
        apply_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::GroupAuthorization { group_id },
            test_admin_event(
                Ulid::from_parts(202, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::GroupRoleRemoved { role_id },
            ),
        )
        .await
        .expect("role remove applies");

        assert!(
            !read_group_doc(&storage, group_id)
                .await
                .roles
                .contains(&role_id)
        );
        assert!(
            !read_group_auth_doc(&storage, group_id)
                .await
                .roles
                .contains_key(&role_id)
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
        assert!(!reducer_state.materialized_group_roles().contains(&role_id));
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
    async fn stale_user_admin_operation_is_recorded_without_rematerializing_older_value() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([7; 32]);
        let user_id = UserId::local(Ulid::from_parts(3, 1), realm_id);
        let actor = Actor {
            node_id: node(8),
            user_id,
            realm_id,
        };
        let target = AdminDocumentTarget::User { user_id };
        let newer = AdminDocumentEvent {
            event_id: Ulid::from_parts(4, 2),
            target: target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: 2,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::UserNameSet {
                name: "newer".to_string(),
            },
        };
        let older = AdminDocumentEvent {
            event_id: Ulid::from_parts(4, 1),
            target: target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor,
            op: AdminDocumentOperation::UserNameSet {
                name: "older".to_string(),
            },
        };

        for event in [newer, older.clone(), older.clone()] {
            apply_user_admin_document_operation_to_storage(
                &storage,
                DocumentSyncTarget::User { user_id },
                event,
            )
            .await
            .expect("out-of-order admin operation applies");
        }

        let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
            .await
            .expect("user exists");
        let user = User::from_bytes(&stored_user).expect("user decodes");
        assert_eq!(user.name, "newer");
        let reducer_state = read_storage_value(
            &storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE,
            admin_document_reducer_state_key(&target),
        )
        .await
        .expect("reducer state exists");
        let reducer_state: AdminDocumentReducerState =
            postcard::from_bytes(&reducer_state).expect("reducer state decodes");
        assert_eq!(reducer_state.applied_event_ids.len(), 2);
        assert!(reducer_state.applied_event_ids.contains(&older.event_id));
        assert_eq!(reducer_state.clock.sequence_for(&older.origin_node_id), 2);
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
        let claims = read_storage_value(
            &storage,
            USER_SUBJECT_CLAIMS_KEYSPACE,
            subject_index_key("subject-created"),
        )
        .await
        .expect("subject claims exist");
        assert_eq!(
            postcard::from_bytes::<BTreeSet<UserId>>(&claims).expect("claims decode"),
            BTreeSet::from([user_id])
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
    async fn concurrent_user_subject_claims_converge_and_promote_on_removal() {
        let (_left_dir, left) = test_storage();
        let (_right_dir, right) = test_storage();
        let realm_id = RealmId::from_bytes([8; 32]);
        let mut user_ids = [
            UserId::local(Ulid::from_parts(20, 1), realm_id),
            UserId::local(Ulid::from_parts(21, 1), realm_id),
        ];
        user_ids.sort();
        let subject_id = "shared-subject".to_string();
        let actors = [
            test_actor(20, user_ids[0], realm_id),
            test_actor(21, user_ids[1], realm_id),
        ];
        let additions = actors.each_ref().map(|actor| {
            test_admin_event(
                Ulid::new(),
                AdminDocumentTarget::User {
                    user_id: actor.user_id,
                },
                actor,
                1,
                AdminDocumentOperation::UserSubjectIdAdded {
                    subject_id: subject_id.clone(),
                },
            )
        });

        for (storage, order) in [(&left, [0, 1]), (&right, [1, 0])] {
            for index in order {
                apply_user_admin_document_operation_to_storage(
                    storage,
                    DocumentSyncTarget::User {
                        user_id: user_ids[index],
                    },
                    additions[index].clone(),
                )
                .await
                .expect("subject claim applies");
            }
        }

        for storage in [&left, &right] {
            let claims = read_storage_value(
                storage,
                USER_SUBJECT_CLAIMS_KEYSPACE,
                subject_index_key(&subject_id),
            )
            .await
            .expect("subject claims exist");
            assert_eq!(
                postcard::from_bytes::<BTreeSet<UserId>>(&claims).expect("claims decode"),
                BTreeSet::from(user_ids)
            );
            assert_eq!(
                read_storage_value(
                    storage,
                    USER_SUBJECT_INDEX_KEYSPACE,
                    subject_index_key(&subject_id),
                )
                .await,
                Some(subject_index_value(user_ids[0]))
            );
        }

        let mut removal = test_admin_event(
            Ulid::new(),
            AdminDocumentTarget::User {
                user_id: user_ids[0],
            },
            &actors[0],
            2,
            AdminDocumentOperation::UserSubjectIdRemoved {
                subject_id: subject_id.clone(),
            },
        );
        removal.observed.advance(actors[0].node_id, 1);
        for storage in [&left, &right] {
            apply_user_admin_document_operation_to_storage(
                storage,
                DocumentSyncTarget::User {
                    user_id: user_ids[0],
                },
                removal.clone(),
            )
            .await
            .expect("canonical subject removal applies");
            assert_eq!(
                read_storage_value(
                    storage,
                    USER_SUBJECT_INDEX_KEYSPACE,
                    subject_index_key(&subject_id),
                )
                .await,
                Some(subject_index_value(user_ids[1]))
            );
        }
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
    fn buffered_document_sync_publish_restart_child_process() {
        let Ok(root) = env::var(DOCUMENT_SYNC_RESTART_CHILD_PATH_ENV) else {
            return;
        };
        let root = PathBuf::from(root);
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

        runtime.block_on(async {
            let service = open_restart_service(&root, "child-storage").await;
            let target = restart_target();
            let event = service
                .publish_documents(
                    vec![DocumentSyncPublish::Upsert {
                        event_id: restart_event_id(),
                        target: target.clone(),
                        bytes: restart_payload(),
                        change: revision_change(),
                        allow_genesis: true,
                    }],
                    Vec::new(),
                )
                .await;

            assert_eq!(
                event,
                DocumentSyncNetEvent::DocumentsPublished {
                    targets: vec![target]
                }
            );
        });

        // Skip Rust destructors so the parent verifies the restart contract, not shutdown cleanup.
        std::process::exit(0);
    }

    #[test]
    fn upsert_event_envelope_round_trips() {
        let event = DocumentSyncEvent::Upsert {
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
    async fn acknowledged_document_sync_publish_survives_buffered_process_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let target = restart_target();

        run_document_sync_restart_child(root);

        let service = open_restart_service(root, "parent-storage").await;
        let topic = service
            .node()
            .open_topic::<DocumentSyncEvent>(target.sync_topic_id())
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
                change: revision_change(),
            }
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn fan_out_peer_syncs_fails_when_any_peer_failed() {
        let ok_peer = peer(1);
        let failed_peer = peer(2);
        let peers = BTreeSet::from([ok_peer, failed_peer]);

        let error = DocumentSyncService::fan_out_peer_syncs(
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
        assert!(message.contains("1/2 document sync batch topics failed"));
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
    async fn document_sync_fencing_metadata_registry_stale_delete_preserves_newer_live_indexes() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(1_560, 1);
        let document_id = Ulid::from_parts(1_561, 1);
        let live_event_id = Ulid::from_parts(1_564, 1);
        let live = registry_record(
            group_id,
            document_id,
            "datasets/live-after-delete",
            300,
            live_event_id,
        );
        write_registry_record(&storage, &live).await;
        let stale_delete = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(1_562, 1),
            Ulid::from_parts(1_563, 1),
        );
        write_document_lifecycle_record(&storage, &stale_delete).await;

        apply_metadata_registry_delete_to_storage(&storage, group_id, document_id)
            .await
            .expect("stale registry delete is fenced");

        assert_registry_record_present(&storage, &live).await;
    }

    #[tokio::test]
    async fn document_sync_fencing_tombstone_wins_over_late_metadata_registry_upsert() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(1_570, 1);
        let document_id = Ulid::from_parts(1_571, 1);
        let stale_event_id = Ulid::from_parts(1_572, 1);
        let delete_lifecycle = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(1_573, 1),
            stale_event_id,
        );
        assert!(
            apply_metadata_document_lifecycle_to_storage(
                &storage,
                &delete_lifecycle,
                metadata_lifecycle_change(&delete_lifecycle, node(8)),
            )
            .await
            .expect("document tombstone applies")
        );
        let stale = registry_record(
            group_id,
            document_id,
            "datasets/stale-after-tombstone",
            100,
            stale_event_id,
        );

        apply_metadata_registry_upsert_to_storage(
            &storage,
            stale.clone(),
            postcard::to_allocvec(&stale).expect("stale registry serializes"),
        )
        .await
        .expect("late registry upsert is fenced by tombstone");

        assert_registry_record_deleted(&storage, group_id, document_id).await;
        assert_eq!(
            read_document_lifecycle_record(&storage, document_id).await,
            delete_lifecycle
        );
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
        let realm_id = RealmId::from_bytes([42; 32]);
        let placement_target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let placement = aruna_core::document::PendingDocumentPlacement {
            realm_id,
            target: placement_target.clone(),
            group_id: Some(group_id),
            metadata_path: Some(record.document_path.clone()),
            desired_holder_count: 1,
            selected_holders: record.holder_node_ids.clone(),
            updated_at: 1,
            origin_node_id: node(1),
            placement: aruna_core::structs::PlacementRef::NIL,
        };
        storage_batch_write_to(
            &storage,
            vec![
                metadata_document_lifecycle_write_entry(&lifecycle)
                    .expect("lifecycle entry builds"),
                aruna_core::storage_entries::document_placement_write_entry(&placement)
                    .expect("placement entry builds"),
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
        assert!(
            read_storage_value(
                &storage,
                aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE,
                aruna_core::storage_entries::document_placement_key(realm_id, &placement_target,),
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn metadata_lifecycle_upsert_preserves_revision_and_replays_idempotently() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(1, 1);
        let document_id = Ulid::from_parts(2, 2);
        let event_id = Ulid::from_parts(3, 3);
        let event = metadata_create_event(group_id, document_id, 100, event_id, 7);
        let lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(event.clone()),
        };
        let placement = aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_parts(4, 4),
            epoch: 7,
        };
        let change = aruna_core::storage_entries::metadata_document_lifecycle_revision_change(
            &lifecycle,
            node(9),
            placement,
        );

        assert!(
            apply_metadata_document_lifecycle_to_storage(&storage, &lifecycle, change)
                .await
                .expect("upsert lifecycle applies")
        );
        assert!(
            !apply_metadata_document_lifecycle_to_storage(&storage, &lifecycle, change)
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
        assert_eq!(revision, change);
    }

    #[tokio::test]
    async fn metadata_lifecycle_upsert_skips_newer_delete_sidecar() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(10, 1);
        let document_id = Ulid::from_parts(11, 1);
        let stale_event_id = Ulid::from_parts(12, 1);
        let delete_event_id = Ulid::from_parts(13, 1);
        let delete_lifecycle =
            metadata_delete_lifecycle(group_id, document_id, 200, delete_event_id, stale_event_id);
        assert!(
            apply_metadata_document_lifecycle_to_storage(
                &storage,
                &delete_lifecycle,
                metadata_lifecycle_change(&delete_lifecycle, node(8)),
            )
            .await
            .expect("delete lifecycle applies")
        );

        let stale_event = metadata_create_event(group_id, document_id, 100, stale_event_id, 7);
        let stale_lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(stale_event),
        };
        assert!(
            !apply_metadata_document_lifecycle_to_storage(
                &storage,
                &stale_lifecycle,
                metadata_lifecycle_change(&stale_lifecycle, node(8)),
            )
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
    async fn metadata_lifecycle_delete_skips_stale_and_equal_sidecars() {
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
        let mut local_change = metadata_lifecycle_change(&local_delete, node(8));
        local_change.placement = aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_parts(25, 1),
            epoch: 9,
        };
        assert!(
            apply_metadata_document_lifecycle_to_storage(&storage, &local_delete, local_change,)
                .await
                .expect("delete lifecycle applies")
        );
        assert!(
            !apply_metadata_document_lifecycle_to_storage(&storage, &local_delete, local_change,)
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
            !apply_metadata_document_lifecycle_to_storage(
                &storage,
                &stale_delete,
                metadata_lifecycle_change(&stale_delete, node(8)),
            )
            .await
            .expect("stale delete lifecycle is fenced")
        );

        assert_eq!(
            read_document_lifecycle_record(&storage, document_id).await,
            local_delete
        );
        let revision = read_lifecycle_revision(&storage, document_id).await;
        assert_eq!(revision, local_change);
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

    // A publisher that is not the document's origin (allow_genesis=false) must not
    // mint a missing topic's genesis: it gets a retryable error and no topic is
    // created. The origin (allow_genesis=true) creates the topic and publishes.
    #[tokio::test]
    async fn missing_topic_publish_requires_allow_genesis() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(61).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::new(),
        };
        let topic_id = target.sync_topic_id();
        let change = DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::new(),
                actor: local_node,
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: aruna_core::structs::PlacementRef::NIL,
        };

        let blocked = service
            .publish_documents(
                vec![DocumentSyncPublish::Upsert {
                    event_id: Ulid::new(),
                    target: target.clone(),
                    bytes: b"blocked".to_vec(),
                    change,
                    allow_genesis: false,
                }],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(blocked, DocumentSyncNetEvent::Error { .. }),
            "non-origin publish must fail retryably: {blocked:?}"
        );
        assert!(
            !service.has_topic(topic_id).expect("topic lookup"),
            "no genesis may be minted without allow_genesis"
        );

        let published = service
            .publish_documents(
                vec![DocumentSyncPublish::Upsert {
                    event_id: Ulid::new(),
                    target: target.clone(),
                    bytes: b"origin".to_vec(),
                    change,
                    allow_genesis: true,
                }],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "origin publish must succeed: {published:?}"
        );
        assert!(
            service.has_topic(topic_id).expect("topic lookup"),
            "origin publish must create the topic genesis"
        );
    }

    #[tokio::test]
    async fn topic_not_ready_publish_does_not_block_ready_batch_record() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(62).await,
            storage,
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let blocked_target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_parts(62, 1),
        };
        let ready_target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_parts(62, 2),
        };
        let blocked_topic = blocked_target.sync_topic_id();
        let ready_topic = ready_target.sync_topic_id();
        let change = DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(62, 3),
                actor: local_node,
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: aruna_core::structs::PlacementRef::NIL,
        };

        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::from_parts(62, 4),
                        target: blocked_target.clone(),
                        bytes: b"blocked".to_vec(),
                        change,
                        allow_genesis: false,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::from_parts(62, 5),
                        target: ready_target.clone(),
                        bytes: b"ready".to_vec(),
                        change,
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;

        match published {
            DocumentSyncNetEvent::DocumentsPartiallyPublished {
                published_indices,
                retry_indices,
                error,
            } => {
                assert_eq!(published_indices, vec![1]);
                assert_eq!(retry_indices, vec![0]);
                assert!(error.contains(&blocked_topic.to_string()));
            }
            other => panic!("expected partial publish, got {other:?}"),
        }
        assert!(
            !service
                .has_topic(blocked_topic)
                .expect("blocked topic lookup"),
            "not-ready record must not mint genesis"
        );
        assert!(
            service.has_topic(ready_topic).expect("ready topic lookup"),
            "ready record behind not-ready record must publish"
        );
    }

    // Two services fork one admin topic (each mints its own genesis carrying a
    // unique admin event). The genesis tie-break resets exactly the losing side,
    // whose admin event is evicted and decodes back into a re-emittable outbox
    // publish that preserves the original embedded event id (the applier dedup
    // key) and refuses to mint a rival genesis.
    #[tokio::test]
    async fn forked_admin_topic_eviction_reemits_with_preserved_event_id() {
        let (_dir_a, storage_a) = test_storage();
        let (_dir_b, storage_b) = test_storage();
        let doc_a = tempfile::tempdir().expect("doc a");
        let doc_b = tempfile::tempdir().expect("doc b");
        let service_a = DocumentSyncService::open_with_persist_policy(
            test_endpoint(71).await,
            storage_a,
            doc_a.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("service a opens");
        let service_b = DocumentSyncService::open_with_persist_policy(
            test_endpoint(72).await,
            storage_b,
            doc_b.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("service b opens");

        let node_a = service_a.local_node_id().expect("node a id");
        let node_b = service_b.local_node_id().expect("node b id");

        let realm_id = RealmId::from_bytes([71; 32]);
        let user_id = UserId::local(Ulid::from_parts(7, 1), realm_id);
        let target = DocumentSyncTarget::User { user_id };
        let admin_target = AdminDocumentTarget::User { user_id };
        let topic_id = target.sync_topic_id();

        let event_a_id = Ulid::from_parts(0xA1, 1);
        let event_b_id = Ulid::from_parts(0xB2, 2);
        let admin_a = test_admin_event(
            event_a_id,
            admin_target.clone(),
            &test_actor(1, user_id, realm_id),
            1,
            AdminDocumentOperation::UserNameSet {
                name: "from-a".into(),
            },
        );
        let admin_b = test_admin_event(
            event_b_id,
            admin_target.clone(),
            &test_actor(2, user_id, realm_id),
            1,
            AdminDocumentOperation::UserNameSet {
                name: "from-b".into(),
            },
        );

        // Each side lists the other in its genesis peer set, so the loser is a
        // member of the winner's topic after the reset.
        let published_a = service_a
            .publish_documents(
                vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(admin_a),
                    allow_genesis: true,
                }],
                vec![node_b],
            )
            .await;
        assert!(
            matches!(published_a, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "service a publish: {published_a:?}"
        );
        let published_b = service_b
            .publish_documents(
                vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(admin_b),
                    allow_genesis: true,
                }],
                vec![node_a],
            )
            .await;
        assert!(
            matches!(published_b, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "service b publish: {published_b:?}"
        );

        let node_a_handle = service_a.node();
        let node_b_handle = service_b.node();
        let genesis_a = node_a_handle
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis;
        let genesis_b = node_b_handle
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis;
        assert_ne!(
            genesis_a, genesis_b,
            "the two nodes forked distinct genesis"
        );

        // The smaller genesis wins; the side holding the larger genesis loses.
        let (
            loser,
            loser_node,
            winner_node,
            loser_event_id,
            winner_genesis,
            winner_peer,
            loser_peer,
        ) = if genesis_a > genesis_b {
            (
                &service_a,
                &node_a_handle,
                &node_b_handle,
                event_a_id,
                genesis_b,
                node_id_to_peer_id(&node_b),
                node_id_to_peer_id(&node_a),
            )
        } else {
            (
                &service_b,
                &node_b_handle,
                &node_a_handle,
                event_b_id,
                genesis_a,
                node_id_to_peer_id(&node_a),
                node_id_to_peer_id(&node_b),
            )
        };

        let winner_ops =
            irokle_crate::oplog::topological(winner_node.storage(), &topic_id).unwrap();
        let loser_ops = irokle_crate::oplog::topological(loser_node.storage(), &topic_id).unwrap();

        // The winner keeps its genesis and produces no eviction.
        let (_winner_ack, winner_side) = winner_node
            .receive_sync_data_from_evicting(
                loser_peer,
                SyncData {
                    topic_id,
                    ops: loser_ops,
                },
            )
            .unwrap();
        assert!(
            winner_side.is_empty(),
            "winner keeps its genesis; no eviction"
        );

        // The loser resets and evicts its own admin chain.
        let (_loser_ack, evictions) = loser_node
            .receive_sync_data_from_evicting(
                winner_peer,
                SyncData {
                    topic_id,
                    ops: winner_ops,
                },
            )
            .unwrap();
        assert_eq!(evictions.len(), 1, "exactly the loser side resets");

        // Both sides now agree on the winning genesis.
        assert_eq!(
            loser_node
                .storage()
                .topic_state(&topic_id)
                .unwrap()
                .unwrap()
                .genesis,
            winner_genesis
        );
        assert_eq!(
            winner_node
                .storage()
                .topic_state(&topic_id)
                .unwrap()
                .unwrap()
                .genesis,
            winner_genesis
        );

        // The evicted admin event re-emits with its original event id preserved
        // and allow_genesis cleared.
        let reemitted = loser.decode_eviction(evictions.into_iter().next().unwrap());
        assert_eq!(reemitted.len(), 1);
        let reemitted = &reemitted[0];
        assert_eq!(&reemitted.target, &target);
        assert!(
            !reemitted.allow_genesis,
            "re-emission must not mint a rival genesis"
        );
        assert!(
            reemitted.event_id.is_none(),
            "admin outbox re-emission uses the embedded admin event id"
        );
        match &reemitted.event {
            DocumentSyncOutboxEvent::AdminOperation { event } => {
                assert_eq!(
                    event.event_id, loser_event_id,
                    "embedded admin event id must survive for applier dedup"
                );
            }
            other => panic!("expected an AdminOperation re-emission, got {other:?}"),
        }
    }

    // Whole-document admin sync is refused by apply_upsert/apply_delete. If reconcile
    // `?`-propagated that refusal the applied-ops cursor would never advance, so each
    // reconcile would re-materialize the whole post-cursor history. The upsert/delete
    // must be skipped while the admin operation on the same topic still applies.
    #[tokio::test]
    async fn reconcile_skips_whole_document_admin_sync_events() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(54).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let realm_id = RealmId::from_bytes([54u8; 32]);
        let user_id = UserId::local(Ulid::from_parts(1_400, 1), realm_id);
        let target = DocumentSyncTarget::User { user_id };
        let topic_id = target.sync_topic_id();

        let change = |kind| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::new(),
                actor: service.local_node_id().expect("local node id"),
                updated_at_ms: 1,
            },
            kind,
            placement: aruna_core::structs::PlacementRef::NIL,
        };
        let actor = test_actor(54, user_id, realm_id);
        assert_eq!(
            actor.node_id,
            service.local_node_id().expect("local node id")
        );
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config.to_bytes(&actor).expect("config serializes").into(),
            )],
        )
        .await
        .expect("config writes");
        let admin_event = test_admin_event(
            Ulid::from_parts(1_401, 1),
            AdminDocumentTarget::User { user_id },
            &actor,
            1,
            AdminDocumentOperation::UserNameSet {
                name: "Skip Survivor".to_string(),
            },
        );

        // Two hostile whole-document ops (upsert then delete) precede a legitimate
        // owner-authored admin operation on the same topic.
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::new(),
                        target: target.clone(),
                        bytes: b"whole-document-admin-upsert".to_vec(),
                        change: change(DocumentSyncChangeKind::Upsert),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Delete {
                        event_id: Ulid::new(),
                        target: target.clone(),
                        change: change(DocumentSyncChangeKind::Delete),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(admin_event),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        // Reset the cursor so reconcile reprocesses every op as a fresh peer would.
        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
                ByteView::from(
                    postcard::to_allocvec(&irokle_crate::ActorClock::default())
                        .expect("clock serializes"),
                ),
            )
            .await
            .expect("cursor reset");

        // Reconcile completes despite the hostile whole-document ops.
        let result = service
            .reconcile_document_topics([topic_id])
            .await
            .expect("reconcile skips whole-document admin sync instead of wedging");

        // The admin operation on the same topic still applied.
        assert!(result.targets.contains(&target));
        let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
            .await
            .expect("user materialized by the admin operation");
        assert_eq!(
            User::from_bytes(&stored_user).expect("user decodes").name,
            "Skip Survivor"
        );

        // The cursor advanced past the hostile ops.
        let cursor_bytes = read_storage_value(
            &storage,
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
            topic_cursor_key(topic_id),
        )
        .await
        .expect("cursor persisted");
        let cursor: irokle_crate::ActorClock =
            postcard::from_bytes(&cursor_bytes).expect("cursor decodes");
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock");
        assert!(
            cursor.dominates(&topic_clock),
            "cursor must advance past the hostile ops"
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn inbound_admin_validation_rejects_publisher_impersonation_for_every_family() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([63; 32]);
        let user_id = UserId::local(Ulid::from_parts(1_600, 1), realm_id);
        let actor = test_actor(63, user_id, realm_id);
        let group_id = Ulid::from_parts(1_601, 1);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Management);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config.to_bytes(&actor).expect("config serializes").into(),
            )],
        )
        .await
        .expect("config writes");

        let cases = [
            (
                DocumentSyncTarget::RealmConfig { realm_id },
                test_admin_event(
                    Ulid::from_parts(1_602, 1),
                    AdminDocumentTarget::RealmConfig { realm_id },
                    &actor,
                    1,
                    AdminDocumentOperation::RealmConfigDescriptionSet {
                        description: "forged".to_string(),
                    },
                ),
            ),
            (
                DocumentSyncTarget::RealmAuthorization { realm_id },
                test_admin_event(
                    Ulid::from_parts(1_603, 1),
                    AdminDocumentTarget::Realm { realm_id },
                    &actor,
                    1,
                    AdminDocumentOperation::RealmRoleAdded {
                        role_id: Ulid::from_parts(1_604, 1),
                    },
                ),
            ),
            (
                DocumentSyncTarget::User { user_id },
                test_admin_event(
                    Ulid::from_parts(1_605, 1),
                    AdminDocumentTarget::User { user_id },
                    &actor,
                    1,
                    AdminDocumentOperation::UserNameSet {
                        name: "forged".to_string(),
                    },
                ),
            ),
            (
                DocumentSyncTarget::GroupAuthorization { group_id },
                test_admin_event(
                    Ulid::from_parts(1_606, 1),
                    AdminDocumentTarget::Group { group_id },
                    &actor,
                    1,
                    AdminDocumentOperation::GroupCreated {
                        realm_id,
                        display_name: "forged".to_string(),
                        owner: user_id,
                    },
                ),
            ),
        ];

        for (target, event) in cases {
            let topic_id = target.sync_topic_id();
            let impersonator = irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&node(64)));
            assert!(
                matches!(
                    validate_replicated_admin_event(
                        &storage,
                        topic_id,
                        impersonator,
                        &target,
                        &event,
                    )
                    .await
                    .expect("storage succeeds"),
                    AdminEventValidation::Rejected(_)
                ),
                "publisher impersonation must be rejected for {target:?}"
            );
        }
    }

    #[tokio::test]
    async fn inbound_admin_validation_enforces_management_and_preserves_genesis() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([65; 32]);
        let nil_actor = test_actor(65, UserId::nil(realm_id), realm_id);
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let config_topic = config_target.sync_topic_id();
        let publisher =
            irokle_crate::actor_id_for(config_topic, node_id_to_peer_id(&nil_actor.node_id));
        let ensure = test_admin_event(
            Ulid::from_parts(1_610, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &nil_actor,
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: nil_actor.node_id,
                kind: RealmNodeKind::Management,
            },
        );
        assert_eq!(
            validate_replicated_admin_event(
                &storage,
                config_topic,
                publisher,
                &config_target,
                &ensure,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Accepted
        );
        apply_admin_document_operation_to_storage(&storage, config_target.clone(), ensure)
            .await
            .expect("bootstrap node applies");

        let mut description = test_admin_event(
            Ulid::from_parts(1_611, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &nil_actor,
            2,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "genesis".to_string(),
            },
        );
        description.observed.advance(nil_actor.node_id, 1);
        assert_eq!(
            validate_replicated_admin_event(
                &storage,
                config_topic,
                publisher,
                &config_target,
                &description,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Accepted,
            "the management self-ensure must authorize the rest of config genesis"
        );

        let (_auth_dir, auth_storage) = test_storage();
        let auth_target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let auth_topic = auth_target.sync_topic_id();
        let auth_publisher =
            irokle_crate::actor_id_for(auth_topic, node_id_to_peer_id(&nil_actor.node_id));
        let genesis_role = test_admin_event(
            Ulid::from_parts(1_612, 1),
            AdminDocumentTarget::Realm { realm_id },
            &nil_actor,
            1,
            AdminDocumentOperation::RealmRoleCreated {
                role: test_admin_role_definition(
                    Ulid::from_parts(1_613, 1),
                    "realm_admin",
                    &format!("/{realm_id}/admin/**"),
                    Permission::WRITE,
                ),
            },
        );
        assert_eq!(
            validate_replicated_admin_event(
                &auth_storage,
                auth_topic,
                auth_publisher,
                &auth_target,
                &genesis_role,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Accepted
        );

        let server_actor = test_actor(66, UserId::local(Ulid::new(), realm_id), realm_id);
        let mut server_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        server_config.ensure_node(server_actor.node_id, RealmNodeKind::Server);
        storage_batch_write_to(
            &auth_storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                server_config
                    .to_bytes(&server_actor)
                    .expect("config serializes")
                    .into(),
            )],
        )
        .await
        .expect("config writes");
        for (target, event) in [
            (
                DocumentSyncTarget::RealmConfig { realm_id },
                test_admin_event(
                    Ulid::from_parts(1_614, 1),
                    AdminDocumentTarget::RealmConfig { realm_id },
                    &server_actor,
                    1,
                    AdminDocumentOperation::RealmConfigQuotaSet {
                        quota: QuotaConfig::default(),
                    },
                ),
            ),
            (
                DocumentSyncTarget::RealmAuthorization { realm_id },
                test_admin_event(
                    Ulid::from_parts(1_615, 1),
                    AdminDocumentTarget::Realm { realm_id },
                    &server_actor,
                    1,
                    AdminDocumentOperation::RealmRoleAdded {
                        role_id: Ulid::new(),
                    },
                ),
            ),
        ] {
            let topic_id = target.sync_topic_id();
            let publisher =
                irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&server_actor.node_id));
            assert!(matches!(
                validate_replicated_admin_event(
                    &auth_storage,
                    topic_id,
                    publisher,
                    &target,
                    &event,
                )
                .await
                .expect("storage succeeds"),
                AdminEventValidation::Rejected(_)
            ));
        }
    }

    #[tokio::test]
    async fn inbound_admin_validation_rejects_target_and_malformed_events() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([67; 32]);
        let actor = test_actor(67, UserId::local(Ulid::new(), realm_id), realm_id);
        let user_id = actor.user_id;
        let other_user = UserId::local(Ulid::new(), realm_id);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config.to_bytes(&actor).expect("config serializes").into(),
            )],
        )
        .await
        .expect("config writes");
        let target = DocumentSyncTarget::User { user_id };
        let topic_id = target.sync_topic_id();
        let publisher = irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&actor.node_id));

        let wrong_target = test_admin_event(
            Ulid::from_parts(1_620, 1),
            AdminDocumentTarget::User {
                user_id: other_user,
            },
            &actor,
            1,
            AdminDocumentOperation::UserNameSet {
                name: "wrong".to_string(),
            },
        );
        assert!(matches!(
            validate_replicated_admin_event(&storage, topic_id, publisher, &target, &wrong_target,)
                .await
                .expect("storage succeeds"),
            AdminEventValidation::Rejected(_)
        ));

        let malformed = test_admin_event(
            Ulid::from_parts(1_621, 1),
            AdminDocumentTarget::User { user_id },
            &actor,
            1,
            AdminDocumentOperation::UserAttributeSet {
                key: "display name".to_string(),
                value: "invalid".to_string(),
            },
        );
        assert!(matches!(
            validate_replicated_admin_event(&storage, topic_id, publisher, &target, &malformed,)
                .await
                .expect("storage succeeds"),
            AdminEventValidation::Rejected(_)
        ));
        assert_eq!(
            read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into()).await,
            None,
            "rejected validation must not mutate storage"
        );
    }

    #[tokio::test]
    async fn reconcile_retries_admin_events_deferred_before_realm_config() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(68).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let realm_id = RealmId::from_bytes([68; 32]);
        let admin_user_id = UserId::local(Ulid::from_parts(1_625, 1), realm_id);
        let bootstrap_actor = test_actor(68, UserId::nil(realm_id), realm_id);
        let admin_actor = test_actor(68, admin_user_id, realm_id);
        let role_id = Ulid::from_parts(1_626, 1);
        let auth_target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let auth_topic = auth_target.sync_topic_id();
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let config_topic = config_target.sync_topic_id();
        assert_ne!(auth_topic, config_topic);

        let role = test_admin_event(
            Ulid::from_parts(1_627, 1),
            AdminDocumentTarget::Realm { realm_id },
            &bootstrap_actor,
            1,
            AdminDocumentOperation::RealmRoleCreated {
                role: test_admin_role_definition(
                    role_id,
                    "realm_admin",
                    &format!("/{realm_id}/admin/**"),
                    Permission::WRITE,
                ),
            },
        );
        let mut assignment = test_admin_event(
            Ulid::from_parts(1_628, 1),
            AdminDocumentTarget::Realm { realm_id },
            &admin_actor,
            2,
            AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                role_id,
                user_id: admin_user_id,
            },
        );
        assignment.observed.advance(admin_actor.node_id, 1);
        let ensure = test_admin_event(
            Ulid::from_parts(1_629, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &bootstrap_actor,
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: bootstrap_actor.node_id,
                kind: RealmNodeKind::Management,
            },
        );
        let mut settings = test_admin_event(
            Ulid::from_parts(1_630, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &bootstrap_actor,
            2,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: test_discovery(68, "https://management.example:443"),
            },
        );
        settings.observed.advance(bootstrap_actor.node_id, 1);

        for (target, events) in [
            (
                auth_target.clone(),
                vec![Box::new(role), Box::new(assignment)],
            ),
            (
                config_target.clone(),
                vec![Box::new(settings), Box::new(ensure)],
            ),
        ] {
            let documents = events
                .into_iter()
                .map(|event| DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event,
                    allow_genesis: true,
                })
                .collect();
            assert!(matches!(
                service.publish_documents(documents, Vec::new()).await,
                DocumentSyncNetEvent::DocumentsPublished { .. }
            ));
            service
                .storage_write(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    topic_cursor_key(target.sync_topic_id()),
                    ByteView::from(
                        postcard::to_allocvec(&irokle_crate::ActorClock::default())
                            .expect("clock serializes"),
                    ),
                )
                .await
                .expect("cursor resets");
        }

        service
            .reconcile_document_topics([auth_topic])
            .await
            .expect("realm authorization reconciliation defers the assignment");
        let auth_before_config = read_realm_auth_doc(&storage, realm_id).await;
        assert!(
            auth_before_config
                .roles
                .get(&role_id)
                .expect("bootstrap role exists")
                .assigned_users
                .is_empty(),
            "the assignment must wait for realm configuration"
        );
        let deferred_cursor: irokle_crate::ActorClock = postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                topic_cursor_key(auth_topic),
            )
            .await
            .expect("deferred cursor remains stored"),
        )
        .expect("cursor decodes");
        let auth_clock = service
            .node()
            .storage()
            .actor_clock(&auth_topic)
            .expect("auth topic clock");
        assert!(!deferred_cursor.dominates(&auth_clock));
        let deferred_topics: BTreeMap<AdminEventDependency, BTreeSet<irokle_crate::TopicId>> =
            postcard::from_bytes(
                &read_storage_value(
                    &storage,
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                    deferred_admin_topics_key(),
                )
                .await
                .expect("deferred topic registry is persisted"),
            )
            .expect("deferred topic registry decodes");
        assert_eq!(
            deferred_topics
                .get(&AdminEventDependency::RealmConfig(realm_id))
                .and_then(|topics| topics.get(&auth_topic)),
            Some(&auth_topic)
        );
        assert!(
            service
                .document_topic_ids()
                .expect("document topics list")
                .contains(&auth_topic),
            "deferred auth topic must remain discoverable"
        );

        let result = service
            .reconcile_document_topics([config_topic])
            .await
            .expect("realm config reconciliation retries deferred admin topics");
        assert!(result.targets.contains(&config_target));
        assert!(
            result.targets.contains(&auth_target),
            "realm authorization target was not retried: {:?}",
            result.targets
        );
        let auth = read_realm_auth_doc(&storage, realm_id).await;
        assert!(
            auth.roles
                .get(&role_id)
                .expect("realm admin role exists")
                .assigned_users
                .contains(&admin_user_id),
            "the deferred assignment must apply after realm configuration"
        );
        let applied_cursor: irokle_crate::ActorClock = postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                topic_cursor_key(auth_topic),
            )
            .await
            .expect("applied cursor is stored"),
        )
        .expect("cursor decodes");
        assert!(applied_cursor.dominates(&auth_clock));

        let group_id = Ulid::from_parts(1_631, 1);
        let group_target = DocumentSyncTarget::GroupAuthorization { group_id };
        let group_role_id = Ulid::from_parts(1_632, 1);
        let mut group_role = test_admin_event(
            Ulid::from_parts(1_633, 1),
            AdminDocumentTarget::Group { group_id },
            &admin_actor,
            2,
            AdminDocumentOperation::GroupRoleCreated {
                role: test_admin_role_definition(
                    group_role_id,
                    "member",
                    &format!("/{realm_id}/g/{group_id}/**"),
                    Permission::WRITE,
                ),
            },
        );
        group_role.observed.advance(admin_actor.node_id, 1);
        let group_create = test_admin_event(
            Ulid::from_parts(1_634, 1),
            AdminDocumentTarget::Group { group_id },
            &admin_actor,
            1,
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: "reordered".to_string(),
                owner: admin_user_id,
            },
        );
        assert!(matches!(
            service
                .publish_documents(
                    vec![
                        DocumentSyncPublish::AdminOperation {
                            target: group_target.clone(),
                            event: Box::new(group_role),
                            allow_genesis: true,
                        },
                        DocumentSyncPublish::AdminOperation {
                            target: group_target.clone(),
                            event: Box::new(group_create),
                            allow_genesis: true,
                        },
                    ],
                    Vec::new(),
                )
                .await,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));
        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(group_target.sync_topic_id()),
                postcard::to_allocvec(&irokle_crate::ActorClock::default())
                    .expect("clock serializes")
                    .into(),
            )
            .await
            .expect("group cursor resets");
        service
            .reconcile_document_topics([group_target.sync_topic_id()])
            .await
            .expect("same-topic group prerequisite is retried");
        assert!(
            read_group_auth_doc(&storage, group_id)
                .await
                .roles
                .contains_key(&group_role_id)
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn reconcile_skips_rejected_admin_ops_and_advances_cursor() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(63).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let realm_id = RealmId::from_bytes([63; 32]);
        let other_realm_id = RealmId::from_bytes([64; 32]);
        let user_id = UserId::local(Ulid::from_parts(1_630, 1), realm_id);
        let local_actor = test_actor(63, user_id, realm_id);
        let claimed_actor = test_actor(64, user_id, realm_id);
        assert_eq!(
            service.local_node_id().expect("local node id"),
            local_actor.node_id
        );

        let target = DocumentSyncTarget::RealmConfig { realm_id };
        let topic_id = target.sync_topic_id();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(local_actor.node_id, RealmNodeKind::Management);
        config.ensure_node(claimed_actor.node_id, RealmNodeKind::Management);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                target.clone(),
                config
                    .to_bytes(&local_actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");

        let placement_entry = |node_id| NodePlacementEntry {
            node_id,
            location: "eu".to_string(),
            weight: 250,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        };
        let wrong_realm_event = test_admin_event(
            Ulid::from_parts(1_631, 1),
            AdminDocumentTarget::RealmConfig {
                realm_id: other_realm_id,
            },
            &local_actor,
            1,
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: placement_entry(node(90)),
            },
        );
        let impersonated_event = test_admin_event(
            Ulid::from_parts(1_632, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &claimed_actor,
            1,
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: placement_entry(node(91)),
            },
        );
        let mut reducer_invalid_event = test_admin_event(
            Ulid::from_parts(1_633, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &local_actor,
            2,
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: PlacementStrategy {
                    strategy_id: Ulid::from_parts(1_633, 2),
                    name: "invalid".to_string(),
                    replica_count: Some(0),
                    distinct_locations: false,
                    affinity: Vec::new(),
                },
            },
        );
        reducer_invalid_event
            .observed
            .advance(local_actor.node_id, 1);
        let mut valid_event = test_admin_event(
            Ulid::from_parts(1_634, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &local_actor,
            3,
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: placement_entry(local_actor.node_id),
            },
        );
        valid_event.observed.advance(local_actor.node_id, 2);
        // Non-placement admin operations use the same publisher binding and must
        // not retain the old generic bypass.
        let unrelated_event = test_admin_event(
            Ulid::from_parts(1_635, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &claimed_actor,
            2,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "unrelated operation applied".to_string(),
            },
        );

        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(wrong_realm_event),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(impersonated_event),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(reducer_invalid_event),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(valid_event),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(unrelated_event),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
                ByteView::from(
                    postcard::to_allocvec(&irokle_crate::ActorClock::default())
                        .expect("clock serializes"),
                ),
            )
            .await
            .expect("cursor reset");

        let result = service
            .reconcile_document_topics([topic_id])
            .await
            .expect("unauthorized placement events are skipped");
        assert!(result.targets.contains(&target));

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.description, "");
        assert_eq!(
            config.placement_map,
            vec![placement_entry(local_actor.node_id)]
        );

        let cursor_bytes = read_storage_value(
            &storage,
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
            topic_cursor_key(topic_id),
        )
        .await
        .expect("cursor persisted");
        let cursor: irokle_crate::ActorClock =
            postcard::from_bytes(&cursor_bytes).expect("cursor decodes");
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock");
        assert!(
            cursor.dominates(&topic_clock),
            "cursor must advance past rejected admin events"
        );

        service.shutdown().await;
    }

    #[test]
    fn validate_node_usage_upsert_accepts_owner_and_rejects_forgeries() {
        use aruna_core::structs::UsageCounters;

        let node_id = node(7);
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let global = DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        };
        let group = DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: Some(group_id),
        };

        // The owning node's own snapshot validates as global and per-group.
        let owned = NodeUsageSnapshot {
            node_id,
            counters: UsageCounters {
                buckets: 3,
                ..Default::default()
            },
        };
        let owned_bytes = owned.to_bytes().unwrap();
        assert!(validate_node_usage_upsert(&global, &owned_bytes).is_ok());
        assert!(validate_node_usage_upsert(&group, &owned_bytes).is_ok());

        // Zero-counter snapshots (stale-group cleanup) are legitimate upserts.
        let zero = NodeUsageSnapshot {
            node_id,
            counters: UsageCounters::default(),
        };
        assert!(validate_node_usage_upsert(&global, &zero.to_bytes().unwrap()).is_ok());

        // A snapshot whose embedded node id is a different node is rejected.
        let misattributed = NodeUsageSnapshot {
            node_id: node(9),
            counters: UsageCounters {
                buckets: 99,
                ..Default::default()
            },
        };
        assert!(validate_node_usage_upsert(&global, &misattributed.to_bytes().unwrap()).is_err());

        // Undecodable payloads and non node-usage targets are rejected.
        assert!(validate_node_usage_upsert(&global, b"not-a-snapshot").is_err());
        assert!(
            validate_node_usage_upsert(&DocumentSyncTarget::RealmConfig { realm_id }, &owned_bytes)
                .is_err()
        );
    }

    #[test]
    fn validate_watch_interest_upsert_accepts_owner_and_rejects_forgeries() {
        use aruna_core::structs::{WatchEventKind, WatchEventMask};

        let node_id = node(7);
        let realm_id = RealmId::from_bytes([12u8; 32]);
        let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };

        // The owning node's own digest validates.
        let owned = WatchInterestDigest::from_subscriptions(
            node_id,
            [(
                "/owned/**".to_string(),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            )],
        );
        let owned_bytes = owned.to_bytes().unwrap();
        assert!(validate_watch_interest_upsert(&target, &owned_bytes).is_ok());

        // Empty digests are legitimate upserts that clear a node's interest.
        let empty = WatchInterestDigest {
            node_id,
            entries: Vec::new(),
        };
        assert!(validate_watch_interest_upsert(&target, &empty.to_bytes().unwrap()).is_ok());

        // A digest whose embedded node id is a different node is rejected.
        let misattributed = WatchInterestDigest::from_subscriptions(
            node(9),
            [(
                "/forged/**".to_string(),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            )],
        );
        assert!(
            validate_watch_interest_upsert(&target, &misattributed.to_bytes().unwrap()).is_err()
        );

        // Undecodable payloads and non watch-interest targets are rejected.
        assert!(validate_watch_interest_upsert(&target, b"not-a-digest").is_err());
        assert!(
            validate_watch_interest_upsert(
                &DocumentSyncTarget::RealmConfig { realm_id },
                &owned_bytes,
            )
            .is_err()
        );
    }

    #[test]
    fn validate_node_info_upsert_accepts_owner_and_rejects_forgeries() {
        use aruna_core::structs::{NodeInfoDocument, NodeUrls, NodeUtilization};

        let node_id = node(7);
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let target = DocumentSyncTarget::NodeInfo { realm_id, node_id };

        let owned = NodeInfoDocument {
            node_id,
            labels: std::collections::BTreeMap::new(),
            urls: NodeUrls {
                api: None,
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 1,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: 5,
            },
            updated_at_ms: 5,
        };
        assert!(validate_node_info_upsert(&target, &owned.to_bytes().unwrap()).is_ok());

        // A document whose embedded node id is a different node is rejected.
        let misattributed = NodeInfoDocument {
            node_id: node(9),
            ..owned.clone()
        };
        assert!(validate_node_info_upsert(&target, &misattributed.to_bytes().unwrap()).is_err());

        // Undecodable payloads and non node-info targets are rejected.
        assert!(validate_node_info_upsert(&target, b"not-a-document").is_err());
        assert!(
            validate_node_info_upsert(
                &DocumentSyncTarget::RealmConfig { realm_id },
                &owned.to_bytes().unwrap()
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn reconcile_skips_forged_non_owner_watch_interest_upsert() {
        use aruna_core::structs::{WatchEventKind, WatchEventMask};

        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(55).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let forged_node = node(88);
        assert_ne!(local_node, forged_node);
        let realm_id = RealmId::from_bytes([55u8; 32]);
        let target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: local_node,
        };
        let forged_target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: forged_node,
        };
        let topic_id = target.sync_topic_id();
        assert_eq!(topic_id, forged_target.sync_topic_id());

        let change = || DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::new(),
                actor: local_node,
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: aruna_core::structs::PlacementRef::NIL,
        };
        let forged_digest = WatchInterestDigest::from_subscriptions(
            forged_node,
            [(
                "/forged/**".to_string(),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            )],
        );
        let owned_digest = WatchInterestDigest::from_subscriptions(
            local_node,
            [(
                "/owned/**".to_string(),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            )],
        );
        let owned_bytes = owned_digest.to_bytes().expect("digest serializes");

        // The forged upsert claims another node's key but is signed by this
        // service's local node. The legitimate upsert is signed by its owner on
        // the same shared realm topic.
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::new(),
                        target: forged_target.clone(),
                        bytes: forged_digest.to_bytes().expect("digest serializes"),
                        change: change(),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::new(),
                        target: target.clone(),
                        bytes: owned_bytes.clone(),
                        change: change(),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
                ByteView::from(
                    postcard::to_allocvec(&irokle_crate::ActorClock::default())
                        .expect("clock serializes"),
                ),
            )
            .await
            .expect("cursor reset");

        let result = service
            .reconcile_document_topics([topic_id])
            .await
            .expect("reconcile skips forged watch-interest upsert");

        assert!(result.targets.contains(&target));
        assert!(!result.targets.contains(&forged_target));
        let stored = read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("watch interest digest applied");
        assert_eq!(
            WatchInterestDigest::from_bytes(&stored).expect("digest decodes"),
            owned_digest
        );
        assert!(
            read_storage_value(
                &storage,
                forged_target.storage_keyspace(),
                forged_target.storage_key(),
            )
            .await
            .is_none()
        );

        let cursor_bytes = read_storage_value(
            &storage,
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
            topic_cursor_key(topic_id),
        )
        .await
        .expect("cursor persisted");
        let cursor: irokle_crate::ActorClock =
            postcard::from_bytes(&cursor_bytes).expect("cursor decodes");
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock");
        assert!(
            cursor.dominates(&topic_clock),
            "cursor must advance past the forged upsert"
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn reconcile_skips_forged_non_upsert_watch_interest_events() {
        use aruna_core::structs::{WatchEventKind, WatchEventMask};

        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(56).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let forged_node = node(89);
        assert_ne!(local_node, forged_node);
        let realm_id = RealmId::from_bytes([56u8; 32]);
        let target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: local_node,
        };
        let forged_target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: forged_node,
        };
        let topic_id = target.sync_topic_id();
        assert_eq!(topic_id, forged_target.sync_topic_id());

        let change = |kind| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::new(),
                actor: local_node,
                updated_at_ms: 1,
            },
            kind,
            placement: aruna_core::structs::PlacementRef::NIL,
        };
        let digest = WatchInterestDigest::from_subscriptions(
            local_node,
            [(
                "/owned/**".to_string(),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            )],
        );
        let digest_bytes = digest.to_bytes().expect("digest serializes");
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_560, 1), realm_id),
            realm_id,
        );
        let admin_event = test_admin_event(
            Ulid::from_parts(1_561, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &actor,
            1,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "forged watch-interest admin op".to_string(),
            },
        );

        // Hostile non-upserts precede a legitimate owner-signed digest on the
        // same shared realm topic. Reconcile must skip both non-upserts and
        // continue to the valid upsert.
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Delete {
                        event_id: Ulid::new(),
                        target: forged_target.clone(),
                        change: change(DocumentSyncChangeKind::Delete),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: forged_target.clone(),
                        event: Box::new(admin_event),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::new(),
                        target: target.clone(),
                        bytes: digest_bytes.clone(),
                        change: change(DocumentSyncChangeKind::Upsert),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
                ByteView::from(
                    postcard::to_allocvec(&irokle_crate::ActorClock::default())
                        .expect("clock serializes"),
                ),
            )
            .await
            .expect("cursor reset");

        let result = service
            .reconcile_document_topics([topic_id])
            .await
            .expect("reconcile skips forged watch-interest non-upserts");

        assert!(result.targets.contains(&target));
        assert!(!result.targets.contains(&forged_target));
        let stored = read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("watch interest digest applied");
        assert_eq!(
            WatchInterestDigest::from_bytes(&stored).expect("digest decodes"),
            digest
        );
        assert!(
            read_storage_value(
                &storage,
                forged_target.storage_keyspace(),
                forged_target.storage_key(),
            )
            .await
            .is_none()
        );

        let cursor_bytes = read_storage_value(
            &storage,
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
            topic_cursor_key(topic_id),
        )
        .await
        .expect("cursor persisted");
        let cursor: irokle_crate::ActorClock =
            postcard::from_bytes(&cursor_bytes).expect("cursor decodes");
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock");
        assert!(
            cursor.dominates(&topic_clock),
            "cursor must advance past the forged non-upserts"
        );

        service.shutdown().await;
    }

    // A forged non-upsert node-usage event (here a signed Delete) on the shared
    // realm topic must be skipped, not `?`-propagated: otherwise every peer's
    // reconcile of that topic errors at the op forever and realm usage freezes.
    #[tokio::test]
    async fn reconcile_skips_forged_non_upsert_node_usage_event() {
        use aruna_core::structs::UsageCounters;

        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(53).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let realm_id = RealmId::from_bytes([53u8; 32]);
        let target = DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id: local_node,
            group_id: None,
        };
        let topic_id = target.sync_topic_id();

        let change = |kind| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::new(),
                actor: local_node,
                updated_at_ms: 1,
            },
            kind,
            placement: aruna_core::structs::PlacementRef::NIL,
        };

        let snapshot = NodeUsageSnapshot {
            node_id: local_node,
            counters: UsageCounters {
                buckets: 5,
                ..Default::default()
            },
        };
        let snapshot_bytes = snapshot.to_bytes().expect("snapshot serializes");

        // Hostile Delete first, then a legitimate owner-signed Upsert on the same
        // topic. Publishing appends both ops and advances the local cursor.
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Delete {
                        event_id: Ulid::new(),
                        target: target.clone(),
                        change: change(DocumentSyncChangeKind::Delete),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::new(),
                        target: target.clone(),
                        bytes: snapshot_bytes.clone(),
                        change: change(DocumentSyncChangeKind::Upsert),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        // Reset the cursor so reconcile reprocesses both ops exactly as a fresh
        // peer receiving them via sync would (its cursor has not yet advanced).
        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
                ByteView::from(
                    postcard::to_allocvec(&irokle_crate::ActorClock::default())
                        .expect("clock serializes"),
                ),
            )
            .await
            .expect("cursor reset");

        // (a) Reconcile completes without error despite the hostile Delete.
        let result = service
            .reconcile_document_topics([topic_id])
            .await
            .expect("reconcile skips the forged delete instead of wedging");

        // (c) The legitimate upsert on the same topic still applied.
        assert!(result.targets.contains(&target));
        let stored = read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
            .await
            .expect("node usage snapshot applied");
        assert_eq!(
            NodeUsageSnapshot::from_bytes(&stored).expect("snapshot decodes"),
            snapshot
        );

        // (b) The cursor advanced past the hostile op.
        let cursor_bytes = read_storage_value(
            &storage,
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
            topic_cursor_key(topic_id),
        )
        .await
        .expect("cursor persisted");
        let cursor: irokle_crate::ActorClock =
            postcard::from_bytes(&cursor_bytes).expect("cursor decodes");
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock");
        assert!(
            cursor.dominates(&topic_clock),
            "cursor must advance past the hostile op"
        );

        service.shutdown().await;
    }
}
