use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use aruna_core::DhtKeyId;
use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{
    AdminDocumentApplyStatus, AdminDocumentReducerState, GROUP_DISPLAY_NAME_PATH, GROUP_OWNER_PATH,
    GROUP_REALM_ID_PATH, MAX_LIVE_REVOCATIONS_PER_ORIGIN, REALM_CONFIG_DESCRIPTION_PATH,
    REALM_CONFIG_DISCOVERY_PATH, REALM_CONFIG_METADATA_REPLICATION_PATH,
    REALM_CONFIG_POLICIES_PATH, REALM_CONFIG_QUOTA_PATH, RevocationIndex, USER_NAME_PATH,
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
use aruna_core::auth::valid_revocation_expiry;
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
    METADATA_CREATE_ACCEPTANCE_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_GRAPH_LIFECYCLE_KEYSPACE, NOTIFICATION_WATCH_INTEREST_KEYSPACE,
    PERSISTENT_ID_MAPPING_KEYSPACE, REALM_CONFIG_KEYSPACE, SYNC_QUARANTINE_KEYSPACE,
    SYNC_QUARANTINE_USAGE_KEYSPACE, USER_SUBJECT_CLAIMS_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataDocumentDeleteRecord, MetadataDocumentLifecycleRecord,
    MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord,
};
use aruna_core::permission_path::compile_permission_matcher;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, document_sync_revision_key,
    document_sync_revision_write_entry, metadata_create_acceptance_key,
    metadata_create_acceptance_write_entry,
    metadata_create_event_and_pending_projection_write_entries, metadata_document_lifecycle_key,
    metadata_document_lifecycle_write_entry, metadata_graph_lifecycle_key,
    metadata_graph_lifecycle_write_entry, metadata_graph_prune_job_write_entry,
    metadata_registry_delete_entries, metadata_registry_write_entries, shard_manifest_write_entry,
    stale_admin_document_conflict_delete_entries, subject_index_key, subject_index_value,
};
use aruna_core::structs::{
    BindingError, DocumentClass, FIRST_GRANTABLE_HANDLE, Group, GroupAuthorizationDocument,
    HANDLE_RANGE_SIZE, MetadataRegistryRecord, NOTIFICATION_WATCH_INTEREST_BYTES_CAP,
    NOTIFICATION_WATCH_INTEREST_ENTRY_CAP, NOTIFICATION_WATCH_MAX_PREFIX_LEN, NodeInfoDocument,
    NodeUsageSnapshot, PersistentIdKind, PersistentIdMapping, PersistentIdStatus, PlacementRef,
    PlacementScope, PoolAdmission, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    RealmNodeKind, Role, SYNC_QUARANTINE_USAGE_KEY, SyncQuarantineCapacity, SyncQuarantineError,
    SyncQuarantineEvidence, SyncQuarantineIdentity, SyncQuarantineInput, SyncQuarantineUsage, User,
    WatchEventMask, WatchInterestDigest, WatchSubscription, admit_band_pool,
    build_quarantine_entries, coordinator_spans, group_owner_index_key, node_usage_key_node_id,
    persistent_id_change, persistent_id_key, persistent_id_target, quarantine_usage_entry,
    reserved_label, watch_interest_dirty_key, watch_interest_key_node_id,
    watch_interest_key_realm_id,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{RoleId, TxnId, UserId, Value};
use aruna_core::util::{unix_timestamp_millis, unix_timestamp_secs};
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use byteview::ByteView;
use irokle_crate::Event as _;
use irokle_crate::Storage as _;
use irokle_crate::TopicControl;
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
const DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT: usize = 8;
const DOCUMENT_SYNC_FANOUT_DOMAIN: &[u8] = b"aruna-document-sync-fanout-v1";
const DOCUMENT_SYNC_FANOUT_KEYSPACE: &str = "document-sync-fanout";
const DOCUMENT_SYNC_INBOUND_SYNC_MESSAGE_LIMIT: usize = 4_096;
const DOCUMENT_SYNC_INBOUND_SYNC_STREAM_BYTES: usize = 256 * 1024 * 1024;
// A frame is meaningful progress; a byte trickle cannot retain a permit forever.
const DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const DOCUMENT_SYNC_INBOUND_STREAM_TIMEOUT: Duration = Duration::from_secs(30 * 60);
// Admission budgets: worst-case inbound cost is bounded before any stream is
// drained, per pushing peer and for the node as a whole.
const DOCUMENT_SYNC_INBOUND_PEER_STREAMS: usize = 8;
const DOCUMENT_SYNC_INBOUND_GLOBAL_STREAMS: usize = 64;
// Aggregate buffered-byte ceilings, well below 64 independent 256 MiB streams,
// so concurrent streams cannot pin more memory than the node can absorb.
const DOCUMENT_SYNC_INBOUND_PEER_BYTES: usize = 512 * 1024 * 1024;
const DOCUMENT_SYNC_INBOUND_GLOBAL_BYTES: usize = 2 * 1024 * 1024 * 1024;
const DOCUMENT_SYNC_FRAME_LEN_LIMIT: usize = 16 * 1024 * 1024;
const DOCUMENT_SYNC_REPLAY_BATCH_LIMIT: usize = 1_024;
const MAX_DEFERRED_TOPICS: usize = 1_024;
const MAX_DEFERRED_TOPICS_PER_DEPENDENCY: usize = 256;
/// Bounds concurrent co-holder genesis probes so a large holder set cannot open
/// an unbounded number of simultaneous sync streams.
const SHARD_GENESIS_PROBE_CONCURRENCY: usize = 8;

#[derive(Debug)]
struct PendingMetadataCreateApply {
    identity: SyncQuarantineIdentity,
    /// The event exactly as received, so a reject in the create batch keeps the
    /// genuine envelope instead of the payload the batch reconstructed.
    event: DocumentSyncEvent,
    target: DocumentSyncTarget,
    record: MetadataCreateEventRecord,
    bytes: Vec<u8>,
    lifecycle_revision: Option<DocumentSyncChange>,
}

/// A permanently rejected sync operation awaiting durable evidence. Evidence is
/// materialized only when the topic cursor is about to advance past it, so both
/// land in one transaction. The identity is transport-derived, so an operation
/// whose payload never decoded is still keyed and retained.
struct SyncRejection {
    identity: SyncQuarantineIdentity,
    evidence: SyncQuarantineEvidence,
    reason: String,
}

impl SyncRejection {
    fn new(
        identity: SyncQuarantineIdentity,
        event: DocumentSyncEvent,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            identity,
            evidence: SyncQuarantineEvidence::from_event(&event),
            reason: reason.into(),
        }
    }

    /// Evidence for a payload that could not be decoded into an event at all.
    fn raw(identity: SyncQuarantineIdentity, bytes: Vec<u8>, reason: impl Into<String>) -> Self {
        Self {
            identity,
            evidence: SyncQuarantineEvidence::raw(bytes),
            reason: reason.into(),
        }
    }

    fn topic_id(&self) -> irokle_crate::TopicId {
        self.identity.topic
    }
}

struct DocumentEventBatch {
    cursor: irokle_crate::ActorClock,
    events: Vec<(DocumentSyncEvent, irokle_crate::ActorId, u64)>,
    /// Ops whose transport payload never decoded into an event. They are
    /// permanent by construction: no redelivery can make the bytes valid.
    rejections: Vec<SyncRejection>,
}

/// Placement fence outcome. The transactional read of the realm config is the
/// whole fence: a concurrent config mutation conflicts the commit. The config
/// row is never written back, which would only conflict its readers.
struct MetadataPlacementFence;

enum MetadataPlacementOutcome<T> {
    Accepted(T),
    Deferred(DocumentSyncDependency),
    Rejected,
}

#[derive(Default)]
struct PublishEventsOutcome {
    published: BTreeMap<irokle_crate::TopicId, irokle_crate::ActorClock>,
    published_indices: Vec<usize>,
    retry_indices: Vec<usize>,
    retry_error: Option<String>,
}

#[derive(Debug)]
struct PeerSelection {
    peers: BTreeSet<PeerId>,
    truncated: bool,
    round: u64,
}

#[derive(Clone, Debug)]
struct ShardPublisherPolicy {
    current: BTreeSet<irokle_crate::ActorId>,
    // Existing history remains applicable across holder replacement. Once a
    // topic is local, only former-holder ops above this cutover clock are stale.
    history_cutoff: Option<irokle_crate::ActorClock>,
}

impl ShardPublisherPolicy {
    fn allows(&self, actor_id: &irokle_crate::ActorId, actor_seq: u64) -> bool {
        self.current.contains(actor_id)
            || self
                .history_cutoff
                .as_ref()
                .is_none_or(|cutoff| actor_seq <= cutoff.get(actor_id))
    }
}

/// Outcome of probing a shard's co-holders for an existing genesis before a
/// rank-0 holder considers creating a fresh one. A genesis may be created only
/// when every co-holder was reached (`unreachable` empty), none advertised the
/// topic (`known_by_co_holder`), and every reached co-holder positively
/// confirmed it unknown (not in `unconfirmed`). An unreachable co-holder — or a
/// reached one that refused the topic (holds it but the prober may not open it
/// yet) — might hold a genesis, so creation must be withheld to avoid a fork.
#[derive(Clone, Debug, Default)]
pub struct ShardGenesisProbe {
    /// Probed topics at least one reached co-holder already has a genesis for.
    pub known_by_co_holder: BTreeSet<irokle_crate::TopicId>,
    /// Probed topics a reached co-holder neither advertised nor positively
    /// confirmed unknown: it holds the topic but the prober may not open it yet,
    /// so a fresh genesis would fork. Possibly-existing ⇒ creation withheld.
    pub unconfirmed: BTreeSet<irokle_crate::TopicId>,
    /// Co-holders that could not be reached (empty ⇒ every co-holder answered).
    pub unreachable: Vec<NodeId>,
}

/// Concurrent inbound sync stream counters. A stream takes its permit before
/// any byte of the payload is read, so an abusive peer costs one table entry
/// instead of a 256 MiB drain per stream.
#[derive(Debug, Default)]
struct InboundSyncBudget {
    state: Mutex<InboundSyncCounters>,
}

#[derive(Debug, Default)]
struct InboundSyncCounters {
    global: usize,
    per_peer: BTreeMap<PeerId, usize>,
    global_bytes: usize,
    per_peer_bytes: BTreeMap<PeerId, usize>,
}

impl InboundSyncBudget {
    fn acquire(self: &Arc<Self>, peer: PeerId) -> Option<InboundSyncPermit> {
        let mut state = self.state.lock();
        let held = state.per_peer.get(&peer).copied().unwrap_or(0);
        if state.global >= DOCUMENT_SYNC_INBOUND_GLOBAL_STREAMS
            || held >= DOCUMENT_SYNC_INBOUND_PEER_STREAMS
        {
            return None;
        }
        state.global += 1;
        *state.per_peer.entry(peer).or_insert(0) += 1;
        Some(InboundSyncPermit {
            budget: self.clone(),
            peer,
        })
    }

    // Reserves declared frame bytes against the per-peer and global ceilings
    // before the payload is allocated; false leaves both counters untouched.
    fn reserve_bytes(&self, peer: PeerId, bytes: usize) -> bool {
        let mut state = self.state.lock();
        let held = state.per_peer_bytes.get(&peer).copied().unwrap_or(0);
        if state.global_bytes.saturating_add(bytes) > DOCUMENT_SYNC_INBOUND_GLOBAL_BYTES
            || held.saturating_add(bytes) > DOCUMENT_SYNC_INBOUND_PEER_BYTES
        {
            return false;
        }
        state.global_bytes = state.global_bytes.saturating_add(bytes);
        *state.per_peer_bytes.entry(peer).or_insert(0) += bytes;
        true
    }

    fn release_bytes(&self, peer: PeerId, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut state = self.state.lock();
        state.global_bytes = state.global_bytes.saturating_sub(bytes);
        if let Some(held) = state.per_peer_bytes.get_mut(&peer) {
            *held = held.saturating_sub(bytes);
            if *held == 0 {
                state.per_peer_bytes.remove(&peer);
            }
        }
    }
}

struct InboundSyncPermit {
    budget: Arc<InboundSyncBudget>,
    peer: PeerId,
}

impl Drop for InboundSyncPermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.global = state.global.saturating_sub(1);
        if let Some(held) = state.per_peer.get_mut(&self.peer) {
            *held = held.saturating_sub(1);
            if *held == 0 {
                state.per_peer.remove(&self.peer);
            }
        }
    }
}

/// Holds a peer's buffered-byte reservation for the lifetime of one inbound
/// stream, releasing it on success, decode failure, cancellation, or drop.
struct InboundByteReservation {
    budget: Arc<InboundSyncBudget>,
    peer: PeerId,
    reserved: usize,
}

impl InboundByteReservation {
    fn new(budget: Arc<InboundSyncBudget>, peer: PeerId) -> Self {
        Self {
            budget,
            peer,
            reserved: 0,
        }
    }

    fn reserve(&mut self, bytes: usize) -> Result<()> {
        if !self.budget.reserve_bytes(self.peer, bytes) {
            return Err(NetError::AdmissionRejected(format!(
                "document sync byte budget exhausted for peer {}",
                self.peer
            )));
        }
        self.reserved = self.reserved.saturating_add(bytes);
        Ok(())
    }
}

impl Drop for InboundByteReservation {
    fn drop(&mut self) {
        self.budget.release_bytes(self.peer, self.reserved);
    }
}

#[derive(Clone)]
pub struct DocumentSyncService {
    node: irokle_crate::Irokle<irokle_crate::FjallStorage>,
    net: Arc<irokle_crate::net::IrohNet<irokle_crate::FjallStorage>>,
    db: fjall::OptimisticTxDatabase,
    fanout_cursors: fjall::OptimisticTxKeyspace,
    persist_policy: FjallPersistPolicy,
    storage: StorageHandle,
    default_peers: Arc<RwLock<BTreeSet<PeerId>>>,
    shard_publishers: Arc<RwLock<BTreeMap<irokle_crate::TopicId, ShardPublisherPolicy>>>,
    storage_path: PathBuf,
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
    // Genesis tie-break evictions from every admission path (irokle's own
    // accept/resync loops via the net sink, plus this service's bootstrap and
    // batch-sync paths) funnel into this sender; the embedder drains the
    // receiver once via `take_eviction_receiver` and re-emits the payloads.
    eviction_tx: tokio::sync::mpsc::UnboundedSender<TopicEviction>,
    eviction_rx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<TopicEviction>>>>,
    // Realm this service serves; shard-classed targets carry no realm id of
    // their own, so their topic derivation reads it from here.
    realm_id: RealmId,
    inbound_budget: Arc<InboundSyncBudget>,
    // Peers configured at open. They admit inbound sync only during the
    // bootstrap window; once realm config materializes the current
    // sync-eligible `default_peers` set is authoritative.
    configured_peers: BTreeSet<PeerId>,
    // Flips true on the first realm-config-driven peer refresh, after which
    // `configured_peers` no longer grant admission.
    realm_config_materialized: Arc<AtomicBool>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        endpoint: iroh::Endpoint,
        storage: StorageHandle,
        storage_path: impl AsRef<Path>,
        peer_nodes: &[NodeId],
        alpns: Vec<Vec<u8>>,
        runtime: irokle_crate::net::IrohRuntimeConfig,
        realm_id: RealmId,
    ) -> Result<Self> {
        Self::open_with_persist_policy(
            endpoint,
            storage,
            storage_path,
            peer_nodes,
            alpns,
            runtime,
            FjallPersistPolicy::default(),
            realm_id,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_with_persist_policy(
        endpoint: iroh::Endpoint,
        storage: StorageHandle,
        storage_path: impl AsRef<Path>,
        peer_nodes: &[NodeId],
        alpns: Vec<Vec<u8>>,
        runtime: irokle_crate::net::IrohRuntimeConfig,
        persist_policy: FjallPersistPolicy,
        realm_id: RealmId,
    ) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let default_peers: BTreeSet<PeerId> = peer_nodes.iter().map(node_id_to_peer_id).collect();
        let db = fjall::OptimisticTxDatabase::builder(&storage_path)
            .manual_journal_persist(true)
            .open()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let fanout_cursors = db
            .keyspace(
                DOCUMENT_SYNC_FANOUT_KEYSPACE,
                fjall::KeyspaceCreateOptions::default,
            )
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
            fanout_cursors,
            persist_policy,
            storage,
            default_peers: Arc::new(RwLock::new(default_peers.clone())),
            shard_publishers: Arc::new(RwLock::new(BTreeMap::new())),
            storage_path,
            reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
            eviction_tx,
            eviction_rx: Arc::new(Mutex::new(Some(eviction_rx))),
            realm_id,
            inbound_budget: Arc::new(InboundSyncBudget::default()),
            configured_peers: default_peers,
            realm_config_materialized: Arc::new(AtomicBool::new(false)),
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
        self.clear_cursor(eviction.topic_id);
        if let Err(error) = self.flush_database() {
            warn!(%error, topic_id = %eviction.topic_id, "Failed to persist document sync fan-out cursor reset");
        }
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
                DocumentSyncEvent::AdminOperation {
                    target,
                    event,
                    placement,
                } => {
                    documents.push(DocumentSyncEvictedDocument {
                        event_id: None,
                        target,
                        event: DocumentSyncOutboxEvent::AdminOperation { event },
                        placement,
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
                        placement: change.placement,
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
                        placement: change.placement,
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
        // Realm config is now authoritative: replace the fan-out/admission set
        // and stop honoring the bootstrap `configured_peers`. The transport
        // whitelist is additive, but `admit_inbound` gates before any read.
        *self.default_peers.write() = peers;
        self.realm_config_materialized
            .store(true, Ordering::Release);
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
        let selection = self.sync_peer_selection(&peers, &topic_id)?;
        self.log_peer_selection(topic_id, &selection);
        self.allow_sync_peers(&selection.peers)?;
        let round = selection.round;
        let result = self.sync_topic(topic_id, selection).await;
        self.advance_cursor(topic_id, round)?;
        self.flush_database()?;
        result
    }

    pub fn allow_document_sync_peers(
        &self,
        topics: &[irokle_crate::TopicId],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        let sync_peers = self.sync_peers(peers);
        if sync_peers.is_empty() {
            return Ok(());
        }
        self.allow_sync_peers(&sync_peers)?;

        let mut seen_topics = BTreeSet::new();
        for topic_id in topics.iter().copied() {
            if !seen_topics.insert(topic_id) {
                continue;
            }

            let state = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| {
                    NetError::Bootstrap(format!("document sync topic {topic_id} is missing"))
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

    /// Reconciles shard-only topic membership to the authoritative current
    /// holder set. The publisher cutover is installed before controls are
    /// emitted, so later events from a removed holder cannot apply while
    /// pre-cutover replacement history remains usable. Shared topics and the
    /// default peer set are intentionally untouched.
    ///
    /// A `history_cutoff` is only frozen for topics in `verified_topics`: until a
    /// node has durably verified a shard, its local clock is not a trustworthy
    /// cutover boundary, so former-holder history is left admissible ([BR-030]).
    ///
    /// `retained` peers are draining former-holders that must keep publishing onto
    /// the shard until they have flushed (flush-then-leave): they stay members and
    /// accepted publishers even though they are not canonical holders, so a
    /// removal never cuts off an in-flight flush. They rejoin neither the missing
    /// nor the local-holder computation — only a canonical holder may mint or top
    /// up membership — so a true non-holder is never added (DECISIONS D11).
    pub async fn reconcile_shard_membership(
        &self,
        topics: &[irokle_crate::TopicId],
        holders: Vec<NodeId>,
        retained: &BTreeSet<NodeId>,
        verified_topics: &BTreeSet<irokle_crate::TopicId>,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        let _reconcile_guard = self.reconcile_lock.lock().await;
        let holder_peers: BTreeSet<PeerId> = holders
            .into_iter()
            .map(|node_id| node_id_to_peer_id(&node_id))
            .collect();
        let local_peer = self.node.peer_id();
        if !holder_peers.contains(&local_peer) {
            return Err(NetError::Bootstrap(
                "local node is not an authoritative shard holder".to_string(),
            ));
        }
        // Canonical holders plus draining former-holders still flushing: the set
        // that may publish onto and stay in the shard topic. Only canonical
        // holders drive membership top-up and the local-holder guard above.
        let member_peers: BTreeSet<PeerId> = holder_peers
            .iter()
            .copied()
            .chain(retained.iter().map(node_id_to_peer_id))
            .collect();

        let mut seen_topics = BTreeSet::new();
        let topics: Vec<irokle_crate::TopicId> = topics
            .iter()
            .copied()
            .filter(|topic_id| seen_topics.insert(*topic_id))
            .collect();
        let mut states = Vec::with_capacity(topics.len());
        let mut policies = Vec::with_capacity(topics.len());
        let mut missing_topic = None;
        for topic_id in topics {
            let state = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let current = member_peers
                .iter()
                .copied()
                .map(|peer| irokle_crate::actor_id_for(topic_id, peer))
                .collect();
            match state {
                Some(state) => {
                    if state.event_type_id != DocumentSyncEvent::TYPE_ID {
                        return Err(NetError::Bootstrap(format!(
                            "Document sync topic {topic_id} has event type {}, expected {}",
                            state.event_type_id,
                            DocumentSyncEvent::TYPE_ID
                        )));
                    }
                    let history_cutoff = if verified_topics.contains(&topic_id) {
                        Some(
                            self.node
                                .storage()
                                .actor_clock(&topic_id)
                                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                        )
                    } else {
                        None
                    };
                    policies.push((
                        topic_id,
                        ShardPublisherPolicy {
                            current,
                            history_cutoff,
                        },
                    ));
                    states.push((topic_id, state));
                }
                None => {
                    missing_topic.get_or_insert(topic_id);
                    policies.push((
                        topic_id,
                        ShardPublisherPolicy {
                            current,
                            history_cutoff: None,
                        },
                    ));
                }
            }
        }
        self.shard_publishers.write().extend(policies);
        let sync_peers: BTreeSet<PeerId> = member_peers
            .iter()
            .copied()
            .filter(|peer| *peer != local_peer)
            .collect();
        self.allow_sync_peers(&sync_peers)?;
        if let Some(topic_id) = missing_topic {
            return Err(NetError::Bootstrap(format!(
                "document sync topic {topic_id} is missing"
            )));
        }

        let oplog = Oplog::with_storage(self.node.storage().clone());
        for (topic_id, state) in states {
            let missing_peers = member_peers
                .iter()
                .copied()
                .filter(|peer| *peer != local_peer && !state.members.contains(peer))
                .collect::<Vec<_>>();
            let stale_peers = state
                .members
                .iter()
                .copied()
                .filter(|peer| !member_peers.contains(peer))
                .collect::<Vec<_>>();
            if missing_peers.is_empty() && stale_peers.is_empty() {
                continue;
            }

            let actor_id = irokle_crate::actor_id_for(topic_id, local_peer);
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
            for peer in stale_peers {
                oplog
                    .create_control_op(
                        topic_id,
                        actor_id,
                        TopicControl::RemovePeer { peer },
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
        topics: &[irokle_crate::TopicId],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;

        let mut seen_topics = BTreeSet::new();
        for topic_id in topics.iter().copied() {
            if seen_topics.insert(topic_id) {
                self.ensure_topic(topic_id, &sync_peers, true)?;
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

    /// Admission for one inbound sync stream, decided before any payload byte
    /// is read: the pusher must be a configured realm peer and within the
    /// per-peer and global stream budgets.
    fn admit_inbound(&self, peer: NodeId) -> Result<InboundSyncPermit> {
        let peer_id = node_id_to_peer_id(&peer);
        // The bootstrap peers admit only until realm config materializes; after
        // that the current sync-eligible set is the sole authority, so a removed
        // startup peer fails here without a restart.
        let bootstrap_window = !self.realm_config_materialized.load(Ordering::Acquire);
        let admitted = self.default_peers.read().contains(&peer_id)
            || (bootstrap_window && self.configured_peers.contains(&peer_id));
        if !admitted {
            return Err(NetError::AdmissionRejected(format!(
                "document sync peer {peer_id} is not a current realm peer"
            )));
        }
        self.inbound_budget.acquire(peer_id).ok_or_else(|| {
            NetError::AdmissionRejected(format!(
                "document sync stream budget exhausted for peer {peer_id}"
            ))
        })
    }

    pub async fn handle_inbound_stream(
        &self,
        stream: BiStream,
        peer: NodeId,
    ) -> Result<Vec<irokle_crate::TopicId>> {
        let stream_started = Instant::now();
        let _permit = self.admit_inbound(peer)?;
        self.net.note_peer_reachable(node_id_to_peer_id(&peer));
        let BiStream(mut send, mut recv, _) = stream;
        let mut byte_reservation =
            InboundByteReservation::new(self.inbound_budget.clone(), node_id_to_peer_id(&peer));
        let (messages, touched_topics) = timeout(
            DOCUMENT_SYNC_INBOUND_STREAM_TIMEOUT,
            read_inbound_sync_messages(&mut recv, &mut byte_reservation),
        )
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_INBOUND_STREAM_TIMEOUT))??;
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
        topic_id: irokle_crate::TopicId,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        match self.has_topic(topic_id) {
            Ok(true) => {
                let selection = match self.sync_peer_selection(&peers, &topic_id) {
                    Ok(selection) => selection,
                    Err(error) => {
                        return DocumentSyncNetEvent::Error {
                            target: None,
                            error: error.to_string(),
                        };
                    }
                };
                self.log_peer_selection(topic_id, &selection);
                if let Err(error) = self.allow_sync_peers(&selection.peers) {
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
                let round = selection.round;
                let result = self.sync_topic(topic_id, selection).await;
                if let Err(error) = self.advance_cursor(topic_id, round) {
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
                if let Err(error) = result {
                    if let Err(persist_error) = self.flush_database() {
                        return DocumentSyncNetEvent::Error {
                            target: None,
                            error: persist_error.to_string(),
                        };
                    }
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
            }
            Ok(false) => {
                if let Err(error) = self.bootstrap_topic_from_peers(topic_id, &peers).await {
                    if let Err(persist_error) = self.flush_database() {
                        warn!(%persist_error, %topic_id, "Failed to persist document sync bootstrap cleanup");
                    }
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
            }
            Err(error) => {
                return DocumentSyncNetEvent::Error {
                    target: None,
                    error: error.to_string(),
                };
            }
        }
        if let Err(error) = self.flush_database() {
            return DocumentSyncNetEvent::Error {
                target: None,
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
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_documents_event(
        &self,
        topic_ids: Vec<irokle_crate::TopicId>,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        let sync_started = Instant::now();
        let target_count = topic_ids.len();

        let mut seen_topics = BTreeSet::new();
        let mut topic_ids_out: Vec<irokle_crate::TopicId> = Vec::new();
        let mut bootstrap_cursor_dirty = false;
        for topic_id in topic_ids {
            if !seen_topics.insert(topic_id) {
                continue;
            }
            match self.has_topic(topic_id) {
                Ok(true) => topic_ids_out.push(topic_id),
                Ok(false) => {
                    bootstrap_cursor_dirty = true;
                    // Join-only: an unknown topic whose genesis is nowhere to be
                    // found yet (e.g. an empty shard whose rank-0 holder has not
                    // created it) is skipped, not fatal — it arrives via gossip
                    // or a later anti-entropy pass.
                    match self.bootstrap_topic_from_peers(topic_id, &peers).await {
                        Ok(()) => topic_ids_out.push(topic_id),
                        Err(error) => {
                            debug!(%topic_id, error = %error, "skipping unbootstrappable document sync topic");
                        }
                    }
                }
                Err(error) => {
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
            }
        }

        if bootstrap_cursor_dirty && let Err(error) = self.flush_database() {
            return DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }

        let bootstrap_elapsed = sync_started.elapsed();
        let topic_ids = topic_ids_out;
        let peer_sync_started = Instant::now();
        if let Err(error) = self.sync_topics(topic_ids.clone(), &peers).await {
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
                DocumentSyncPublish::AdminOperation {
                    target,
                    event,
                    placement,
                    ..
                } => DocumentSyncEvent::AdminOperation {
                    target,
                    event,
                    placement,
                },
            };
            let target = event.target().clone();
            let topic_id = target.sync_topic_id(self.realm_id, &event.placement());
            // Shard topics are join-only here: only the shard's rank-0 holder
            // creates the genesis (eagerly, via the placement reconciler), so a
            // publish onto a genesis-less shard topic fails and the outbox
            // drain defers the record instead.
            let may_create_topic = !target.uses_shard_topic();
            let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
            let envelope = EventEnvelope::encode_event(&event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let op = match self.publish_event_op(
                &oplog,
                topic_id,
                actor_id,
                envelope,
                sync_peers,
                allow_genesis,
                may_create_topic,
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
        // Member fan-out for publishes without an explicit peer set (admin
        // operations): the drain's sync stage only pushes to record peers, so
        // on an already-existing topic these ops would otherwise wait for an
        // incidental recheck. Peer-addressed publishes keep their drain sync.
        if sync_peers.is_empty() {
            for topic_id in outcome.published.keys() {
                self.net.schedule_topic_recheck(*topic_id)?;
            }
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
        topic_id: irokle_crate::TopicId,
        actor_id: irokle_crate::ActorId,
        envelope: EventEnvelope,
        sync_peers: &BTreeSet<PeerId>,
        allow_genesis: bool,
        may_create_topic: bool,
        fast_path: &mut usize,
        fallback: &mut usize,
    ) -> Result<irokle_crate::Op> {
        let topic_missing = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .is_none();
        if topic_missing && !may_create_topic {
            return Err(NetError::Bootstrap(format!(
                "shard topic {topic_id} has no genesis yet; only its rank-0 holder creates it"
            )));
        }
        if topic_missing {
            // Only the document's origin may mint its topic genesis. Any other
            // publisher waits (retryable) for that genesis to replicate in.
            if !allow_genesis {
                return Err(NetError::TopicNotReady(topic_id.to_string()));
            }
            // Fast path for brand-new topics: genesis + first event admitted in
            // a single storage transaction. Any failure (e.g. a concurrent
            // admission won the genesis race) falls back to the two-step flow.
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
        if may_create_topic {
            self.ensure_topic(topic_id, sync_peers, allow_genesis)?;
        }
        oplog
            .create_event_op(topic_id, actor_id, envelope, self.node.signer())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    /// Signs one op carrying `payload` under the document-sync event type
    /// without encoding an event, so tests can deliver a payload no peer can
    /// decode. Returns the op's transport identity.
    #[cfg(test)]
    pub(crate) fn publish_raw_event(
        &self,
        topic_id: irokle_crate::TopicId,
        payload: Vec<u8>,
    ) -> Result<SyncQuarantineIdentity> {
        let oplog = Oplog::with_storage(self.node.storage().clone());
        let actor_id = irokle_crate::actor_id_for(topic_id, self.node.peer_id());
        let envelope = EventEnvelope {
            type_id: DocumentSyncEvent::TYPE_ID.to_string(),
            payload: payload.into(),
        };
        let op = self.publish_event_op(
            &oplog,
            topic_id,
            actor_id,
            envelope,
            &BTreeSet::new(),
            true,
            true,
            &mut 0,
            &mut 0,
        )?;
        self.flush_database()?;
        Ok(SyncQuarantineIdentity {
            topic: topic_id,
            actor: op.signed.body.actor_id,
            actor_seq: op.signed.body.actor_seq,
        })
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
        topic_id: irokle_crate::TopicId,
        peers: &BTreeSet<PeerId>,
        allow_genesis: bool,
    ) -> Result<irokle_crate::TopicId> {
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

    /// Whether the topic's genesis is known locally. The outbox drain uses this
    /// to defer shard-topic records until the rank-0 holder's genesis arrives.
    pub fn topic_exists(&self, topic_id: irokle_crate::TopicId) -> Result<bool> {
        self.has_topic(topic_id)
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

    fn next_sync_round(&self, topic_id: irokle_crate::TopicId) -> Result<u64> {
        current_cursor(&self.fanout_cursors, topic_id)
    }

    fn advance_cursor(&self, topic_id: irokle_crate::TopicId, round: u64) -> Result<()> {
        advance_cursor(&self.fanout_cursors, topic_id, round)
    }

    fn clear_cursor(&self, topic_id: irokle_crate::TopicId) {
        if let Err(error) = remove_cursor(&self.fanout_cursors, topic_id) {
            warn!(%error, %topic_id, "Failed to clear document sync fan-out cursor");
        }
    }

    fn sync_peer_selection(
        &self,
        peers: &[NodeId],
        topic_id: &irokle_crate::TopicId,
    ) -> Result<PeerSelection> {
        let round = self.next_sync_round(*topic_id)?;
        let mut subject = [0u8; 64];
        subject[..32].copy_from_slice(topic_id.as_ref());
        subject[32..].copy_from_slice(self.node.peer_id().as_bytes());
        if peers.is_empty() {
            let defaults = self.default_peers.read();
            Ok(select_sync_peers(
                defaults.iter().copied(),
                self.node.peer_id(),
                &subject,
                round,
            ))
        } else {
            Ok(select_sync_peers(
                peers
                    .iter()
                    .copied()
                    .map(|node_id| node_id_to_peer_id(&node_id)),
                self.node.peer_id(),
                &subject,
                round,
            ))
        }
    }

    fn log_peer_selection(&self, topic_id: irokle_crate::TopicId, selection: &PeerSelection) {
        if selection.truncated {
            debug!(
                %topic_id,
                selected = selection.peers.len(),
                "Document sync fan-out bounded; omitted peers remain anti-entropy work"
            );
        }
    }

    fn allow_sync_peers(&self, peers: &BTreeSet<PeerId>) -> Result<()> {
        self.node
            .add_peers_to_whitelist(peers.iter().copied())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    async fn fan_out_peer_syncs<F, Fut>(
        selection: PeerSelection,
        context: String,
        run: F,
    ) -> Result<()>
    where
        F: Fn(PeerId) -> Fut,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let omitted = selection.truncated;
        let attempted = selection.peers.len();
        if attempted == 0 {
            return Ok(());
        }

        let fanout_started = Instant::now();
        let mut syncs = JoinSet::new();
        for peer in selection.peers {
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
            omitted,
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
        selection: PeerSelection,
    ) -> Result<()> {
        let net = self.net.clone();
        Self::fan_out_peer_syncs(
            selection,
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
        peers: &[NodeId],
    ) -> Result<()> {
        if topic_ids.is_empty() {
            return Ok(());
        }
        type SyncGroups =
            BTreeMap<BTreeSet<PeerId>, (PeerSelection, Vec<(irokle_crate::TopicId, u64)>)>;
        for chunk in topic_ids.chunks(DOCUMENT_SYNC_BATCH_SYNC_TOPIC_LIMIT) {
            let mut groups: SyncGroups = BTreeMap::new();
            for topic_id in chunk.iter().copied() {
                let selection = self.sync_peer_selection(peers, &topic_id)?;
                let round = selection.round;
                let selected = selection.peers.clone();
                if let Some((group, topics)) = groups.get_mut(&selected) {
                    group.truncated |= selection.truncated;
                    topics.push((topic_id, round));
                } else {
                    groups.insert(selected, (selection, vec![(topic_id, round)]));
                }
            }
            for (_, (selection, topics)) in groups {
                let Some((topic_id, _)) = topics.first() else {
                    continue;
                };
                self.log_peer_selection(*topic_id, &selection);
                self.allow_sync_peers(&selection.peers)?;
                let topic_ids = topics
                    .iter()
                    .map(|(topic_id, _)| *topic_id)
                    .collect::<Vec<_>>();
                let result = self.sync_topic_batch(&topic_ids, selection).await;
                for (topic_id, round) in topics {
                    self.advance_cursor(topic_id, round)?;
                }
                self.flush_database()?;
                result?;
            }
        }
        Ok(())
    }

    async fn sync_topic_batch(
        &self,
        topic_ids: &[irokle_crate::TopicId],
        selection: PeerSelection,
    ) -> Result<()> {
        if topic_ids.is_empty() {
            return Ok(());
        }
        let service = self.clone();
        let topic_ids = topic_ids.to_vec();
        Self::fan_out_peer_syncs(
            selection,
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
        peers: &[NodeId],
    ) -> Result<()> {
        let selection = self.sync_peer_selection(peers, &topic_id)?;
        self.log_peer_selection(topic_id, &selection);
        self.allow_sync_peers(&selection.peers)?;
        let mut first_error = None;
        for peer in selection.peers {
            match self.bootstrap_topic_from_peer(topic_id, peer).await {
                Ok(()) => match self.has_topic(topic_id) {
                    Ok(true) => {
                        self.advance_cursor(topic_id, selection.round)?;
                        return Ok(());
                    }
                    Ok(false) => {
                        let error = NetError::TopicNotReady(topic_id.to_string());
                        warn!(%peer, %topic_id, "Document sync bootstrap peer has no topic");
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    Err(error) => {
                        warn!(%peer, %topic_id, error = %error, "Document sync bootstrap topic check failed");
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                },
                Err(error) => {
                    warn!(%peer, %topic_id, error = %error, "Document sync bootstrap attempt failed");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        // Advance after attempted peers so omitted candidates rotate into the next retry.
        self.advance_cursor(topic_id, selection.round)?;
        Err(first_error.unwrap_or_else(|| {
            NetError::Bootstrap(format!(
                "no peers available to bootstrap document sync topic {topic_id}"
            ))
        }))
    }

    /// Probes each co-holder for an existing genesis of `topics` without
    /// adopting anything: a rank-0 holder uses this to decide whether creating a
    /// fresh genesis is safe (see [`ShardGenesisProbe`]). A co-holder that could
    /// not be reached is recorded so the caller withholds creation for it.
    pub async fn probe_shard_topic_geneses(
        &self,
        topics: Vec<irokle_crate::TopicId>,
        co_holders: Vec<NodeId>,
    ) -> ShardGenesisProbe {
        use futures::StreamExt as _;

        let mut probe = ShardGenesisProbe::default();
        if topics.is_empty() {
            return probe;
        }
        let wanted: BTreeSet<irokle_crate::TopicId> = topics.iter().copied().collect();
        // Callers pass co-holders with the local node already excluded.
        let topic_ids = &topics;
        let probes = futures::stream::iter(co_holders.iter().copied().map(|node_id| async move {
            let peer = node_id_to_peer_id(&node_id);
            (node_id, self.probe_topics_on_peer(topic_ids, peer).await)
        }))
        .buffer_unordered(SHARD_GENESIS_PROBE_CONCURRENCY);
        // Poll a bounded number of peer probes together; aggregation is order
        // independent (set unions plus an unreachable list).
        let probe_results: Vec<_> = probes.collect().await;
        for (node_id, result) in probe_results {
            match result {
                Ok(peer_probe) => {
                    probe
                        .known_by_co_holder
                        .extend(peer_probe.known.iter().copied());
                    // A reached co-holder that neither advertised a topic nor
                    // positively confirmed it unknown refused it: it holds the
                    // genesis but the prober may not open it yet. Withhold, never
                    // treat as topic-unknown — a fresh genesis would fork.
                    for topic in &wanted {
                        if !peer_probe.known.contains(topic)
                            && !peer_probe.confirmed_unknown.contains(topic)
                        {
                            probe.unconfirmed.insert(*topic);
                        }
                    }
                }
                Err(error) => {
                    debug!(%node_id, error = %error, "co-holder unreachable while probing shard genesis");
                    probe.unreachable.push(node_id);
                }
            }
        }
        probe
    }

    async fn probe_topics_on_peer(
        &self,
        topics: &[irokle_crate::TopicId],
        peer: PeerId,
    ) -> Result<PeerTopicProbe> {
        let peer_addr = peer_id_to_endpoint_addr(peer)?;
        let wanted: BTreeSet<irokle_crate::TopicId> = topics.iter().copied().collect();
        let mut probe = PeerTopicProbe::default();
        for chunk in topics.chunks(DOCUMENT_SYNC_BATCH_SYNC_TOPIC_LIMIT) {
            let opens: Vec<SyncMessage> = chunk
                .iter()
                .map(|topic| SyncMessage::Open(self.node.sync_open(*topic)))
                .collect();
            let responses = timeout(
                DOCUMENT_SYNC_PEER_SYNC_TIMEOUT,
                self.net.sync_with(peer_addr.clone(), &opens),
            )
            .await
            .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
            .map_err(NetError::from)?;
            probe.merge(classify_probe_responses(&wanted, responses));
        }
        Ok(probe)
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
        let mut deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>> =
            self.storage_read(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                deferred_topics_key(),
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
        for dependency in deferred_topics.keys().copied().collect::<Vec<_>>() {
            if document_sync_dependency_available(&self.storage, dependency).await? {
                satisfied_persisted_dependencies.push(dependency);
            }
        }
        for dependency in satisfied_persisted_dependencies {
            if let Some(topics) = deferred_topics.remove(&dependency) {
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
        let mut pending_metadata_creates: Vec<PendingMetadataCreateApply> = Vec::new();
        let mut deferred_cursor_writes: Vec<(irokle_crate::TopicId, (String, ByteView, Value))> =
            Vec::new();
        let mut deferred_rejections: Vec<SyncRejection> = Vec::new();
        while let Some(topic_id) = topic_queue.pop_front() {
            queued_topics.remove(&topic_id);
            pending_metadata_creates.retain(|pending| pending.identity.topic != topic_id);
            deferred_cursor_writes.retain(|(pending_topic_id, _)| *pending_topic_id != topic_id);
            deferred_rejections.retain(|rejection| rejection.topic_id() != topic_id);
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
            let batch =
                self.document_event_batch(topic_id, &cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)?;
            if batch.cursor == cursor {
                continue;
            }
            // Persist only this causal batch; anti-entropy will revisit the topic.
            cursor = batch.cursor;
            let mut deferred_creates = false;
            let mut rejections: Vec<SyncRejection> = batch.rejections;
            let mut deferred_admin_events = Vec::new();
            let mut satisfied_admin_dependencies = BTreeSet::new();
            let mut cross_topic_dependencies = BTreeSet::new();
            for (event, actor_id, actor_seq) in batch.events {
                let identity = SyncQuarantineIdentity {
                    topic: topic_id,
                    actor: actor_id,
                    actor_seq,
                };
                if self
                    .shard_publishers
                    .read()
                    .get(&topic_id)
                    .is_some_and(|policy| !policy.allows(&actor_id, actor_seq))
                {
                    warn!(
                        %topic_id,
                        %actor_id,
                        "Rejecting shard event from a publisher outside the current holder set"
                    );
                    rejections.push(SyncRejection::new(
                        identity,
                        event,
                        "shard publisher is outside the current holder set",
                    ));
                    continue;
                }
                let target_topic_id = event
                    .target()
                    .sync_topic_id(self.realm_id, &event.placement());
                if target_topic_id != topic_id {
                    warn!(
                        %topic_id,
                        %target_topic_id,
                        "Skipping document sync event whose target does not match its topic"
                    );
                    rejections.push(SyncRejection::new(
                        identity,
                        event,
                        "event target does not match its topic",
                    ));
                    continue;
                }
                match event {
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target: target @ DocumentSyncTarget::WatchSubscription { owner, watch_id },
                        bytes,
                        change,
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
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                "watch subscription revision actor is not its publisher",
                            ));
                            continue;
                        }
                        if let Err(reason) =
                            validate_watch_subscription_upsert(&target, &bytes, &change)
                        {
                            warn!(%topic_id, %owner, %watch_id, %reason, "Rejecting invalid watch subscription");
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                format!("invalid watch subscription: {reason}"),
                            ));
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
                        event_id,
                        target:
                            target @ DocumentSyncTarget::NodeUsage {
                                node_id: snapshot_node,
                                ..
                            },
                        bytes,
                        change,
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
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                "node usage publisher is not the owning node",
                            ));
                            continue;
                        }
                        if let Err(reason) = validate_node_usage_upsert(&target, &bytes) {
                            warn!(
                                %topic_id,
                                node_id = %snapshot_node,
                                %reason,
                                "Rejecting invalid node usage snapshot"
                            );
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                format!("invalid node usage snapshot: {reason}"),
                            ));
                            continue;
                        }
                        self.apply_upsert(target.clone(), bytes, change).await?;
                        applied_targets.push(target);
                    }
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target:
                            target @ DocumentSyncTarget::WatchInterest {
                                realm_id,
                                node_id: interest_node,
                            },
                        bytes,
                        change,
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
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                "watch interest publisher is not the owning node",
                            ));
                            continue;
                        }
                        if let Err(reason) = validate_watch_interest(&target, &bytes) {
                            warn!(
                                %topic_id,
                                realm_id = %realm_id,
                                node_id = %interest_node,
                                %reason,
                                "Rejecting invalid watch interest digest"
                            );
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                format!("invalid watch interest digest: {reason}"),
                            ));
                            continue;
                        }
                        self.apply_upsert(target.clone(), bytes, change).await?;
                        applied_targets.push(target);
                    }
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target:
                            target @ DocumentSyncTarget::NodeInfo {
                                node_id: info_node, ..
                            },
                        bytes,
                        change,
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
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                "node info publisher is not the owning node",
                            ));
                            continue;
                        }
                        if let Err(reason) = validate_node_info_upsert(&target, &bytes) {
                            warn!(
                                %topic_id,
                                node_id = %info_node,
                                %reason,
                                "Rejecting invalid node info document"
                            );
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target,
                                    bytes,
                                    change,
                                },
                                format!("invalid node info document: {reason}"),
                            ));
                            continue;
                        }
                        self.apply_upsert(target.clone(), bytes, change).await?;
                        applied_targets.push(target);
                    }
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target:
                            target @ DocumentSyncTarget::MetadataRegistry {
                                group_id,
                                document_id,
                            },
                        bytes,
                        change,
                    } => {
                        let reject = |reason: String| {
                            SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target: DocumentSyncTarget::MetadataRegistry {
                                        group_id,
                                        document_id,
                                    },
                                    bytes: bytes.clone(),
                                    change,
                                },
                                reason,
                            )
                        };
                        let record = match postcard::from_bytes::<MetadataRegistryRecord>(&bytes) {
                            Ok(record) => record,
                            Err(error) => {
                                warn!(%topic_id, %document_id, %error, "Rejecting undecodable metadata registry record");
                                rejections.push(reject(format!(
                                    "undecodable metadata registry record: {error}"
                                )));
                                continue;
                            }
                        };
                        if record.group_id != group_id || record.document_id != document_id {
                            warn!(%topic_id, %document_id, "Rejecting metadata registry record whose payload does not match its target");
                            rejections.push(reject(format!(
                                "metadata registry target {group_id}/{document_id} does not match payload {}/{}",
                                record.group_id, record.document_id
                            )));
                            continue;
                        }
                        let realm_id = record.realm_id;
                        let strategy_id = record.placement.strategy_id;
                        let event_bytes = bytes.clone();
                        match self.apply_metadata_registry_upsert(record, bytes).await? {
                            MetadataPlacementOutcome::Accepted(()) => applied_targets.push(target),
                            MetadataPlacementOutcome::Deferred(dependency) => {
                                warn!(
                                    %topic_id,
                                    %realm_id,
                                    %document_id,
                                    %strategy_id,
                                    "Deferring metadata registry record until its placement strategy is available"
                                );
                                cross_topic_dependencies.insert(dependency);
                            }
                            MetadataPlacementOutcome::Rejected => {
                                warn!(
                                    %topic_id,
                                    %realm_id,
                                    %document_id,
                                    %strategy_id,
                                    "Rejecting metadata registry record with mismatched placement configuration"
                                );
                                rejections.push(SyncRejection::new(
                                    identity,
                                    DocumentSyncEvent::Upsert {
                                        event_id,
                                        target,
                                        bytes: event_bytes,
                                        change,
                                    },
                                    "metadata registry record has a mismatched placement configuration",
                                ));
                            }
                        }
                    }
                    event @ DocumentSyncEvent::Upsert {
                        target: DocumentSyncTarget::MetadataCreateEvent { .. },
                        ..
                    } => match self.pending_metadata_create_apply(identity, event) {
                        Ok(pending) => {
                            pending_metadata_creates.push(pending);
                            deferred_creates = true;
                        }
                        Err(rejection) => {
                            warn!(%topic_id, reason = %rejection.reason, "Rejecting malformed metadata create event");
                            rejections.push(*rejection);
                            continue;
                        }
                    },
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target: DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
                        bytes,
                        change,
                    } => {
                        let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
                        let reject = |reason: String| {
                            SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target: DocumentSyncTarget::MetadataDocumentLifecycle {
                                        document_id,
                                    },
                                    bytes: bytes.clone(),
                                    change,
                                },
                                reason,
                            )
                        };
                        let lifecycle = match postcard::from_bytes::<MetadataDocumentLifecycleRecord>(
                            &bytes,
                        ) {
                            Ok(lifecycle) => lifecycle,
                            Err(error) => {
                                warn!(%topic_id, %document_id, %error, "Rejecting undecodable metadata document lifecycle record");
                                rejections.push(reject(format!(
                                    "undecodable metadata document lifecycle record: {error}"
                                )));
                                continue;
                            }
                        };
                        if lifecycle.document_id() != document_id {
                            warn!(%topic_id, %document_id, "Rejecting metadata document lifecycle record whose payload does not match its target");
                            rejections.push(reject(format!(
                                "metadata document lifecycle target {document_id} does not match payload document {}",
                                lifecycle.document_id()
                            )));
                            continue;
                        }
                        match lifecycle {
                            MetadataDocumentLifecycleRecord::Upsert { event: record } => {
                                let record = *record;
                                let inner_bytes = postcard::to_allocvec(&record)
                                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                                pending_metadata_creates.push(PendingMetadataCreateApply {
                                    identity,
                                    event: DocumentSyncEvent::Upsert {
                                        event_id,
                                        target: target.clone(),
                                        bytes,
                                        change,
                                    },
                                    target,
                                    lifecycle_revision: Some(change),
                                    record,
                                    bytes: inner_bytes,
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
                        event_id,
                        target: DocumentSyncTarget::MetadataGraphLifecycle { graph_iri },
                        bytes,
                        change,
                    } => {
                        let target = DocumentSyncTarget::MetadataGraphLifecycle {
                            graph_iri: graph_iri.clone(),
                        };
                        let reject = |reason: String| {
                            SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target: DocumentSyncTarget::MetadataGraphLifecycle {
                                        graph_iri: graph_iri.clone(),
                                    },
                                    bytes: bytes.clone(),
                                    change,
                                },
                                reason,
                            )
                        };
                        let record = match postcard::from_bytes::<MetadataGraphLifecycleRecord>(
                            &bytes,
                        ) {
                            Ok(record) => record,
                            Err(error) => {
                                warn!(%topic_id, %graph_iri, %error, "Rejecting undecodable metadata graph lifecycle record");
                                rejections.push(reject(format!(
                                    "undecodable metadata graph lifecycle record: {error}"
                                )));
                                continue;
                            }
                        };
                        if record.graph_iri != graph_iri {
                            warn!(%topic_id, %graph_iri, "Rejecting metadata graph lifecycle record whose payload does not match its target");
                            rejections.push(reject(format!(
                                "metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                                record.graph_iri
                            )));
                            continue;
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
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target: DocumentSyncTarget::PersistentIdMapping { document_id },
                        bytes,
                        change,
                    } => {
                        let target = DocumentSyncTarget::PersistentIdMapping { document_id };
                        let reject = |reason: String| {
                            SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target: DocumentSyncTarget::PersistentIdMapping { document_id },
                                    bytes: bytes.clone(),
                                    change,
                                },
                                reason,
                            )
                        };
                        let mapping = match postcard::from_bytes::<PersistentIdMapping>(&bytes) {
                            Ok(mapping) => mapping,
                            Err(error) => {
                                warn!(%topic_id, %document_id, %error, "Rejecting undecodable persistent id mapping");
                                rejections.push(reject(format!(
                                    "undecodable persistent id mapping: {error}"
                                )));
                                continue;
                            }
                        };
                        if let Err(reason) = validate_pid_mapping(document_id, &mapping, &change) {
                            warn!(%topic_id, %document_id, %reason, "Rejecting invalid persistent id mapping");
                            rejections
                                .push(reject(format!("invalid persistent id mapping: {reason}")));
                            continue;
                        }
                        // The revision actor is the node that took the transition
                        // and the only node that publishes it, so a mapping signed
                        // by anyone else is a forgery.
                        let expected_actor = irokle_crate::actor_id_for(
                            topic_id,
                            node_id_to_peer_id(&mapping.revision.actor),
                        );
                        if actor_id != expected_actor {
                            warn!(
                                %topic_id,
                                %document_id,
                                "Rejecting persistent id mapping whose revision actor is not its publisher"
                            );
                            rejections.push(reject(
                                "persistent id mapping revision actor is not its publisher"
                                    .to_string(),
                            ));
                            continue;
                        }
                        match self.apply_pid_mapping(&mapping, change.placement).await? {
                            MetadataPlacementOutcome::Accepted(true) => {
                                applied_targets.push(target)
                            }
                            MetadataPlacementOutcome::Accepted(false) => {}
                            MetadataPlacementOutcome::Deferred(dependency) => {
                                warn!(
                                    %topic_id,
                                    %document_id,
                                    "Deferring persistent id mapping until its placement configuration is available"
                                );
                                cross_topic_dependencies.insert(dependency);
                            }
                            MetadataPlacementOutcome::Rejected => {
                                warn!(
                                    %topic_id,
                                    %document_id,
                                    "Rejecting persistent id mapping stamped with a placement its document id does not decode to"
                                );
                                rejections.push(reject(
                                    "persistent id mapping has a mismatched placement configuration"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    event @ (DocumentSyncEvent::Delete {
                        target: DocumentSyncTarget::PersistentIdMapping { .. },
                        ..
                    }
                    | DocumentSyncEvent::AdminOperation {
                        target: DocumentSyncTarget::PersistentIdMapping { .. },
                        ..
                    }) => {
                        // The mapping row is a permanent tombstone once written, so
                        // it only ever syncs as a monotone upsert. Skip rather than
                        // `?`-propagate so a hostile op cannot wedge the topic.
                        warn!(
                            %topic_id,
                            target = ?event.target(),
                            "Skipping unsupported non-upsert persistent id mapping event"
                        );
                        rejections.push(SyncRejection::new(
                            identity,
                            event,
                            "unsupported non-upsert persistent id mapping event",
                        ));
                        continue;
                    }
                    DocumentSyncEvent::Delete {
                        event_id,
                        target: target @ DocumentSyncTarget::WatchSubscription { owner, watch_id },
                        change,
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
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Delete {
                                    event_id,
                                    target,
                                    change,
                                },
                                "watch subscription delete actor is not its publisher",
                            ));
                            continue;
                        }
                        if let Err(reason) = validate_watch_subscription_delete(&target, &change) {
                            warn!(%topic_id, %owner, %watch_id, %reason, "Rejecting invalid watch subscription delete");
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Delete {
                                    event_id,
                                    target,
                                    change,
                                },
                                format!("invalid watch subscription delete: {reason}"),
                            ));
                            continue;
                        }
                        if self
                            .apply_watch_subscription_change(target.clone(), None, change)
                            .await?
                        {
                            applied_targets.push(target);
                        }
                    }
                    event @ (DocumentSyncEvent::Delete {
                        target:
                            DocumentSyncTarget::NodeUsage { .. }
                            | DocumentSyncTarget::WatchInterest { .. },
                        ..
                    }
                    | DocumentSyncEvent::AdminOperation {
                        target:
                            DocumentSyncTarget::NodeUsage { .. }
                            | DocumentSyncTarget::WatchInterest { .. },
                        ..
                    }) => {
                        // Shared realm snapshots only ever sync as owner-validated
                        // upserts. A signed Delete or AdminOperation on the shared
                        // realm topic would otherwise `?`-propagate through the
                        // generic arm and wedge every peer's reconcile forever, so
                        // skip it and let the cursor advance past the hostile op.
                        warn!(
                            %topic_id,
                            target = ?event.target(),
                            "Skipping unsupported non-upsert shared document event"
                        );
                        rejections.push(SyncRejection::new(
                            identity,
                            event,
                            "unsupported non-upsert shared realm document event",
                        ));
                        continue;
                    }
                    event @ (DocumentSyncEvent::Delete {
                        target: DocumentSyncTarget::NodeInfo { .. },
                        ..
                    }
                    | DocumentSyncEvent::AdminOperation {
                        target: DocumentSyncTarget::NodeInfo { .. },
                        ..
                    }) => {
                        // Node info documents only ever sync as owner-validated
                        // upserts; skip any signed Delete/AdminOperation on the
                        // shared realm topic so it cannot wedge the reconcile loop.
                        warn!(
                            %topic_id,
                            target = ?event.target(),
                            "Skipping unsupported non-upsert node info document event"
                        );
                        rejections.push(SyncRejection::new(
                            identity,
                            event,
                            "unsupported non-upsert node info document event",
                        ));
                        continue;
                    }
                    event @ (DocumentSyncEvent::Upsert { .. }
                    | DocumentSyncEvent::Delete { .. })
                        if admin_document_target_for_reduced_document(event.target()).is_some() =>
                    {
                        // apply_upsert/apply_delete refuse whole-document admin sync, so
                        // skip it here to let the cursor advance instead of wedging reconcile.
                        warn!(
                            %topic_id,
                            target = ?event.target(),
                            "Skipping unsupported whole-document admin sync event"
                        );
                        rejections.push(SyncRejection::new(
                            identity,
                            event,
                            "unsupported whole-document admin sync event",
                        ));
                        continue;
                    }
                    DocumentSyncEvent::AdminOperation {
                        target,
                        event,
                        placement,
                    } => {
                        match validate_replicated_admin_event(
                            &self.storage,
                            topic_id,
                            actor_id,
                            &target,
                            &event,
                            self.realm_id,
                            &placement,
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
                                rejections.push(SyncRejection::new(
                                    identity,
                                    DocumentSyncEvent::AdminOperation {
                                        target,
                                        event,
                                        placement,
                                    },
                                    reason,
                                ));
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
                                deferred_admin_events.push((
                                    target, *event, placement, identity, dependency, reason,
                                ));
                                continue;
                            }
                        }

                        let dependencies =
                            satisfied_document_sync_dependencies(&target, event.as_ref());
                        apply_admin_document_operation_to_storage(
                            &self.storage,
                            target.clone(),
                            *event,
                        )
                        .await?;
                        satisfied_admin_dependencies.extend(dependencies);
                        applied_targets.push(target);
                    }
                    event => {
                        let target = event.target().clone();
                        match self.apply_document_event(event.clone()).await {
                            Ok(()) => applied_targets.push(target),
                            // The apply paths raise `Bootstrap` only for decode
                            // and shape failures, which no redelivery can fix;
                            // storage failures stay transient and propagate.
                            Err(NetError::Bootstrap(reason)) => {
                                warn!(
                                    %topic_id,
                                    ?target,
                                    %reason,
                                    "Quarantining a malformed or unsupported sync event"
                                );
                                rejections.push(SyncRejection::new(identity, event, reason));
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            let mut pending = deferred_admin_events;
            loop {
                let mut progressed = false;
                let mut retry = Vec::new();
                for (target, event, placement, identity, _dependency, _previous_reason) in pending {
                    match validate_replicated_admin_event(
                        &self.storage,
                        topic_id,
                        identity.actor,
                        &target,
                        &event,
                        self.realm_id,
                        &placement,
                    )
                    .await?
                    {
                        AdminEventValidation::Accepted => {
                            let dependencies =
                                satisfied_document_sync_dependencies(&target, &event);
                            apply_admin_document_operation_to_storage(
                                &self.storage,
                                target.clone(),
                                event,
                            )
                            .await?;
                            satisfied_admin_dependencies.extend(dependencies);
                            applied_targets.push(target);
                            progressed = true;
                        }
                        AdminEventValidation::Rejected(reason) => {
                            warn!(
                                %topic_id,
                                event_id = %event.event_id,
                                %reason,
                                "Rejecting deferred admin operation after prerequisite replay"
                            );
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::AdminOperation {
                                    target,
                                    event: Box::new(event),
                                    placement,
                                },
                                reason,
                            ));
                        }
                        AdminEventValidation::Deferred { dependency, reason } => {
                            retry.push((target, event, placement, identity, dependency, reason))
                        }
                    }
                }
                if !progressed {
                    for (target, event, placement, identity, dependency, reason) in retry {
                        if let Some(dependency) = dependency {
                            cross_topic_dependencies.insert(dependency);
                        } else {
                            warn!(
                                %topic_id,
                                event_id = %event.event_id,
                                reason = %reason,
                                "Rejecting admin operation whose same-topic prerequisite is absent"
                            );
                            rejections.push(SyncRejection::new(
                                identity,
                                DocumentSyncEvent::AdminOperation {
                                    target,
                                    event: Box::new(event),
                                    placement,
                                },
                                reason,
                            ));
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
                remove_deferred_topic(&mut deferred_topics, topic_id);
                for dependency in cross_topic_dependencies {
                    if matches!(
                        register_deferred_topic(&mut deferred_topics, dependency, topic_id),
                        DeferredTopicRegistrationOutcome::CapacityExceeded
                    ) {
                        warn!(
                            %topic_id,
                            ?dependency,
                            "Dropping document dependency registration because the deferred-topic registry is full"
                        );
                    }
                }
                // Registry capacity limits retry discovery, not whether an
                // unresolved topic is safe to mark as applied.
                continue;
            }
            let value = ByteView::from(
                postcard::to_allocvec(&cursor)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            );
            if deferred_creates {
                deferred_rejections.append(&mut rejections);
                deferred_cursor_writes.push((
                    topic_id,
                    (
                        DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                        cursor_key,
                        value,
                    ),
                ));
            } else if rejections.is_empty() {
                self.storage_write(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key,
                    value,
                )
                .await?;
            } else if !self
                .commit_cursor_evidence(
                    &rejections,
                    (
                        DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                        cursor_key,
                        value,
                    ),
                )
                .await?
            {
                // Fail closed: the events redeliver once evidence fits again.
                continue;
            }
            let retry_topics = {
                remove_deferred_topic(&mut deferred_topics, topic_id);
                satisfied_admin_dependencies
                    .into_iter()
                    .filter_map(|dependency| deferred_topics.remove(&dependency))
                    .flatten()
                    .collect::<Vec<_>>()
            };
            for retry_topic in retry_topics {
                if queued_topics.insert(retry_topic) {
                    topic_queue.push_back(retry_topic);
                }
            }
        }
        let persisted_deferred_topics = postcard::to_allocvec(&deferred_topics)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.storage_write(
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            deferred_topics_key(),
            persisted_deferred_topics.clone().into(),
        )
        .await?;
        self.apply_metadata_create_batch(
            pending_metadata_creates,
            deferred_cursor_writes,
            deferred_rejections,
            &mut deferred_topics,
            &mut applied_targets,
            &mut metadata_create_events,
        )
        .await?;
        let updated_deferred_topics = postcard::to_allocvec(&deferred_topics)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if updated_deferred_topics != persisted_deferred_topics {
            self.storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                deferred_topics_key(),
                updated_deferred_topics.into(),
            )
            .await?;
        }
        Ok(DocumentSyncReconcileResult {
            targets: applied_targets,
            metadata_create_events,
            metadata_graph_tombstones,
        })
    }

    /// Returns one bounded causal batch above a component-wise cursor.
    fn document_event_batch(
        &self,
        topic_id: irokle_crate::TopicId,
        cursor: &irokle_crate::ActorClock,
        byte_limit: usize,
    ) -> Result<DocumentEventBatch> {
        let storage = self.node.storage();
        let topic_clock = storage
            .actor_clock(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let mut working_cursor = cursor.clone();
        let mut queued = BTreeSet::new();
        for (actor_id, actor_tip) in topic_clock.iter() {
            if working_cursor.get(actor_id) < *actor_tip {
                queued.insert(*actor_id);
            }
        }

        // A candidate can wait on a dependency actor's next contiguous op. Wake
        // it only after that actor reaches the required sequence.
        let mut blocked_by: BTreeMap<
            irokle_crate::ActorId,
            BTreeMap<u64, BTreeSet<irokle_crate::ActorId>>,
        > = BTreeMap::new();
        let mut events = Vec::with_capacity(DOCUMENT_SYNC_REPLAY_BATCH_LIMIT);
        let mut rejections = Vec::new();
        let mut processed = 0usize;
        let mut batch_bytes = 0usize;
        while processed < DOCUMENT_SYNC_REPLAY_BATCH_LIMIT {
            let Some(actor_id) = queued.pop_first() else {
                break;
            };
            let actor_seq = working_cursor
                .get(&actor_id)
                .checked_add(1)
                .ok_or_else(|| {
                    NetError::Bootstrap("document sync actor sequence overflow".into())
                })?;
            if actor_seq > topic_clock.get(&actor_id) {
                continue;
            }
            let op_id = storage
                .actor_index(&topic_id, &actor_id, actor_seq)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| {
                    NetError::Bootstrap(format!(
                        "missing document sync op for actor {actor_id} sequence {actor_seq}"
                    ))
                })?;
            let meta = storage
                .get_meta(&op_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| {
                    NetError::Bootstrap(format!("missing document sync op meta {op_id}"))
                })?;
            if meta.topic_id != topic_id || meta.actor_id != actor_id || meta.actor_seq != actor_seq
            {
                return Err(NetError::Bootstrap(format!(
                    "document sync actor index mismatch for actor {actor_id} sequence {actor_seq}"
                )));
            }

            let mut missing = BTreeMap::new();
            for dependency in &meta.deps {
                let dependency_meta = storage
                    .get_meta(dependency)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
                    .ok_or_else(|| {
                        NetError::Bootstrap(format!(
                            "missing document sync dependency meta {dependency}"
                        ))
                    })?;
                if dependency_meta.topic_id != topic_id {
                    return Err(NetError::Bootstrap(
                        "document sync dependency belongs to another topic".into(),
                    ));
                }
                if dependency_meta.actor_seq > working_cursor.get(&dependency_meta.actor_id) {
                    missing
                        .entry(dependency_meta.actor_id)
                        .and_modify(|sequence: &mut u64| {
                            *sequence = (*sequence).max(dependency_meta.actor_seq)
                        })
                        .or_insert(dependency_meta.actor_seq);
                }
            }
            if !missing.is_empty() {
                for (dependency_actor, dependency_seq) in missing {
                    blocked_by
                        .entry(dependency_actor)
                        .or_default()
                        .entry(dependency_seq)
                        .or_default()
                        .insert(actor_id);
                }
                continue;
            }

            let op = storage
                .get_op(&op_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| NetError::Bootstrap(format!("missing document sync op {op_id}")))?;
            let op_bytes = postcard::experimental::serialized_size(&op)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if op_bytes > DOCUMENT_SYNC_FRAME_LEN_LIMIT {
                return Err(NetError::Bootstrap(
                    "document sync operation exceeds replay frame limit".into(),
                ));
            }
            if processed > 0 && batch_bytes.saturating_add(op_bytes) > byte_limit {
                break;
            }
            let actor_id = op.signed.body.actor_id;
            let actor_seq = op.signed.body.actor_seq;
            match op.signed.body.payload {
                // An undecodable payload is permanent: no redelivery can make
                // the same signed bytes valid, so it becomes raw evidence and
                // the cursor advances past it instead of replaying forever.
                TopicPayload::Event(envelope) => {
                    match envelope.decode_event::<DocumentSyncEvent>() {
                        Ok(event) => events.push((event, actor_id, actor_seq)),
                        Err(error) => rejections.push(SyncRejection::raw(
                            SyncQuarantineIdentity {
                                topic: topic_id,
                                actor: actor_id,
                                actor_seq,
                            },
                            envelope.payload.to_vec(),
                            format!(
                                "undecodable sync payload of type `{}`: {error}",
                                envelope.type_id
                            ),
                        )),
                    }
                }
                TopicPayload::Genesis(_) | TopicPayload::Control(_) => {}
            }
            working_cursor.observe(actor_id, actor_seq);
            processed += 1;
            batch_bytes = batch_bytes.saturating_add(op_bytes);

            let mut wake = BTreeSet::new();
            if let Some(waiters) = blocked_by.get_mut(&actor_id) {
                let ready = waiters
                    .range(..=actor_seq)
                    .map(|(sequence, _)| *sequence)
                    .collect::<Vec<_>>();
                for sequence in ready {
                    if let Some(waiters) = waiters.remove(&sequence) {
                        wake.extend(waiters);
                    }
                }
            }
            if blocked_by
                .get(&actor_id)
                .is_some_and(|waiters| waiters.is_empty())
            {
                blocked_by.remove(&actor_id);
            }
            queued.extend(wake);
            if working_cursor.get(&actor_id) < topic_clock.get(&actor_id) {
                queued.insert(actor_id);
            }
        }

        if processed == 0 {
            if !cursor.dominates(&topic_clock) {
                return Err(NetError::Bootstrap(
                    "document sync causal replay made no progress".into(),
                ));
            }
            return Ok(DocumentEventBatch {
                cursor: working_cursor,
                events: Vec::new(),
                rejections,
            });
        }

        Ok(DocumentEventBatch {
            cursor: working_cursor,
            events,
            rejections,
        })
    }

    #[cfg(test)]
    fn document_events_after(
        &self,
        topic_id: irokle_crate::TopicId,
        cursor: &irokle_crate::ActorClock,
    ) -> Result<Vec<(DocumentSyncEvent, irokle_crate::ActorId, u64)>> {
        Ok(self
            .document_event_batch(topic_id, cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)?
            .events)
    }

    /// `Err` is permanent evidence: a create event whose payload does not decode
    /// or does not name its own target can never become valid.
    fn pending_metadata_create_apply(
        &self,
        identity: SyncQuarantineIdentity,
        event: DocumentSyncEvent,
    ) -> std::result::Result<PendingMetadataCreateApply, Box<SyncRejection>> {
        let (document_id, target_event_id, bytes) = match &event {
            DocumentSyncEvent::Upsert {
                target:
                    DocumentSyncTarget::MetadataCreateEvent {
                        document_id,
                        event_id: target_event_id,
                    },
                bytes,
                ..
            } => (*document_id, *target_event_id, bytes.clone()),
            _ => unreachable!(
                "metadata create apply helper is only called for metadata create upserts"
            ),
        };
        let record = match postcard::from_bytes::<MetadataCreateEventRecord>(&bytes) {
            Ok(record) => record,
            Err(error) => {
                return Err(Box::new(SyncRejection::new(
                    identity,
                    event,
                    format!("undecodable metadata create event: {error}"),
                )));
            }
        };
        if record.record.document_id != document_id || record.event_id != target_event_id {
            let reason = format!(
                "metadata create-event target {document_id}/{target_event_id} does not match payload {}/{}",
                record.record.document_id, record.event_id
            );
            return Err(Box::new(SyncRejection::new(identity, event, reason)));
        }
        Ok(PendingMetadataCreateApply {
            identity,
            event,
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
        cursor_writes: Vec<(irokle_crate::TopicId, (String, ByteView, Value))>,
        mut rejections: Vec<SyncRejection>,
        deferred_topics: &mut BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>>,
        applied_targets: &mut Vec<DocumentSyncTarget>,
        metadata_create_events: &mut Vec<MetadataCreateEventRecord>,
    ) -> Result<()> {
        if pending.is_empty() && cursor_writes.is_empty() && rejections.is_empty() {
            return Ok(());
        }
        let mut candidates = Vec::with_capacity(pending.len());
        for apply in pending {
            if let Err(error) = validate_metadata_event(&apply.record) {
                warn!(
                    topic_id = %apply.identity.topic,
                    document_id = %apply.record.record.document_id,
                    %error,
                    "Rejecting replicated metadata event with inconsistent identity"
                );
                rejections.push(SyncRejection::new(
                    apply.identity,
                    apply.event,
                    format!("replicated metadata event has an inconsistent identity: {error}"),
                ));
                continue;
            }
            let mut entries = Vec::new();
            if let Some(revision) = &apply.lifecycle_revision {
                entries.push(
                    document_sync_revision_write_entry(&apply.target, revision)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                );
                if let Some(manifest) = shard_manifest_write_entry(&apply.target, revision)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
                {
                    entries.push(manifest);
                }
            }
            let mut event_entries =
                metadata_create_event_and_pending_projection_write_entries(&apply.record)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if let Some((_, _, value)) = event_entries.first_mut() {
                *value = ByteView::from(apply.bytes.clone());
            }
            entries.extend(event_entries);
            if event_is_create(&apply.record) {
                entries.push(
                    metadata_create_acceptance_write_entry(&apply.record)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                );
            }
            candidates.push((apply, entries));
        }

        let txn_id = start_storage_transaction(&self.storage).await?;
        let mut writes = Vec::with_capacity(candidates.len() * 3 + cursor_writes.len());
        let mut accepted = Vec::with_capacity(candidates.len());
        let mut accepted_candidates = Vec::with_capacity(candidates.len());
        let mut create_acceptances: BTreeMap<Ulid, MetadataCreateEventRecord> = BTreeMap::new();
        let mut deferred_cursor_topics = BTreeSet::new();
        for (apply, entries) in candidates {
            let fenced = match create_fence_txn(&self.storage, &apply.record, txn_id).await {
                Ok(fenced) => fenced,
                Err(error) => {
                    let _ = self
                        .storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            if fenced {
                continue;
            }
            if let Some(revision) = &apply.lifecycle_revision {
                let stale =
                    match lifecycle_stale_txn(&self.storage, &apply.target, revision, txn_id).await
                    {
                        Ok(stale) => stale,
                        Err(error) => {
                            let _ = self
                                .storage
                                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                                .await;
                            return Err(error);
                        }
                    };
                if stale {
                    continue;
                }
            }
            match metadata_placement_fence_in_transaction(
                &self.storage,
                &apply.record.record,
                txn_id,
            )
            .await
            {
                Ok(MetadataPlacementOutcome::Accepted(MetadataPlacementFence)) => {}
                Ok(MetadataPlacementOutcome::Deferred(dependency)) => {
                    warn!(
                        topic_id = %apply.identity.topic,
                        realm_id = %apply.record.record.realm_id,
                        document_id = %apply.record.record.document_id,
                        strategy_id = %apply.record.record.placement.strategy_id,
                        "Deferring replicated metadata create until its placement strategy is available"
                    );
                    deferred_cursor_topics.insert(apply.identity.topic);
                    if matches!(
                        register_deferred_topic(deferred_topics, dependency, apply.identity.topic),
                        DeferredTopicRegistrationOutcome::CapacityExceeded
                    ) {
                        warn!(
                            topic_id = %apply.identity.topic,
                            ?dependency,
                            "Dropping metadata placement dependency because the deferred-topic registry is full"
                        );
                    }
                    continue;
                }
                Ok(MetadataPlacementOutcome::Rejected) => {
                    warn!(
                        topic_id = %apply.identity.topic,
                        realm_id = %apply.record.record.realm_id,
                        document_id = %apply.record.record.document_id,
                        strategy_id = %apply.record.record.placement.strategy_id,
                        "Rejecting replicated metadata create with mismatched placement configuration"
                    );
                    rejections.push(SyncRejection::new(
                        apply.identity,
                        apply.event,
                        "replicated metadata create has a mismatched placement configuration",
                    ));
                    continue;
                }
                Err(error) => {
                    let _ = self
                        .storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            let document_id = apply.record.record.document_id;
            let accepted_create = if let Some(event) = create_acceptances.get(&document_id) {
                Some(event.clone())
            } else {
                let value = match storage_read_from_transaction(
                    &self.storage,
                    METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                    metadata_create_acceptance_key(document_id),
                    Some(txn_id),
                )
                .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                };
                let event = match value
                    .as_deref()
                    .map(postcard::from_bytes::<MetadataCreateEventRecord>)
                    .transpose()
                {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(NetError::Bootstrap(error.to_string()));
                    }
                };
                if let Some(event) = &event {
                    if !event_is_create(event) {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(NetError::Bootstrap(
                            "metadata create acceptance contains a non-create event".to_string(),
                        ));
                    }
                    if let Err(error) = validate_metadata_event(event) {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                    create_acceptances.insert(document_id, event.clone());
                }
                event
            };
            if event_is_create(&apply.record) {
                if accepted_create
                    .as_ref()
                    .is_some_and(|accepted| !same_create_event(accepted, &apply.record))
                {
                    warn!(
                        topic_id = %apply.identity.topic,
                        %document_id,
                        "Rejecting divergent replicated metadata create"
                    );
                    rejections.push(SyncRejection::new(
                        apply.identity,
                        apply.event,
                        "divergent replicated metadata create",
                    ));
                    continue;
                }
                create_acceptances
                    .entry(document_id)
                    .or_insert_with(|| apply.record.clone());
            } else if accepted_create.is_none() {
                warn!(
                    topic_id = %apply.identity.topic,
                    %document_id,
                    "Deferring replicated metadata update until its create is accepted"
                );
                deferred_cursor_topics.insert(apply.identity.topic);
                continue;
            } else if accepted_create.as_ref().is_some_and(|accepted| {
                !registry_identity_matches(&accepted.record, &apply.record.record)
            }) {
                warn!(
                    topic_id = %apply.identity.topic,
                    %document_id,
                    "Rejecting replicated metadata update with mismatched accepted create"
                );
                rejections.push(SyncRejection::new(
                    apply.identity,
                    apply.event,
                    "replicated metadata update has a mismatched accepted create",
                ));
                continue;
            }
            accepted_candidates.push((apply, entries));
        }
        for (apply, entries) in accepted_candidates {
            writes.extend(entries);
            accepted.push(apply);
        }
        match self.quarantine_entries(&rejections, txn_id).await {
            Ok(Some(entries)) => writes.extend(entries),
            Ok(None) => {
                // Fail closed: no topic may advance past evidence that does not fit.
                deferred_cursor_topics.extend(rejections.iter().map(|reject| reject.topic_id()));
            }
            Err(error) => {
                self.abort_transaction(txn_id).await;
                return Err(error);
            }
        }
        writes.extend(cursor_writes.into_iter().filter_map(|(topic_id, write)| {
            (!deferred_cursor_topics.contains(&topic_id)).then_some(write)
        }));
        if let Err(error) =
            storage_batch_delete_and_write_in_transaction(&self.storage, txn_id, Vec::new(), writes)
                .await
        {
            let _ = self
                .storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(error);
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
            DocumentSyncEvent::AdminOperation { target, event, .. } => {
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
            validate_metadata_event(&record)?;
            return apply_create_event(
                &self.storage,
                &record,
                DocumentSyncTarget::MetadataCreateEvent {
                    document_id,
                    event_id,
                },
                bytes,
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
            self.apply_metadata_registry_upsert(record, bytes).await?;
            return Ok(());
        }
        if let DocumentSyncTarget::PersistentIdMapping { document_id } = target {
            let mapping: PersistentIdMapping = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            validate_pid_mapping(document_id, &mapping, &change).map_err(NetError::Bootstrap)?;
            // The generic write below would clobber a local tombstone with a
            // replayed Active row, so the mapping always goes through its merge.
            return match self.apply_pid_mapping(&mapping, change.placement).await? {
                MetadataPlacementOutcome::Accepted(_) => Ok(()),
                MetadataPlacementOutcome::Deferred(_) => Err(NetError::Dht(
                    "persistent id mapping placement configuration is unavailable".to_string(),
                )),
                MetadataPlacementOutcome::Rejected => Err(NetError::Bootstrap(
                    "persistent id mapping has a mismatched placement configuration".to_string(),
                )),
            };
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
            validate_watch_interest(&target, &bytes).map_err(NetError::Bootstrap)?;
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
    ) -> Result<MetadataPlacementOutcome<()>> {
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

    async fn apply_pid_mapping(
        &self,
        mapping: &PersistentIdMapping,
        placement: PlacementRef,
    ) -> Result<MetadataPlacementOutcome<bool>> {
        store_pid_mapping(&self.storage, self.realm_id, mapping, placement).await
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
        // A minted PID is a permanent identity: the row is never removed, only
        // flipped to Withdrawn, so a delete for it is a no-op rather than an error.
        if let DocumentSyncTarget::PersistentIdMapping { .. } = target {
            return Ok(());
        }
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            return delete_registry_record(&self.storage, group_id, document_id).await;
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

    /// Evidence rows for permanently rejected operations, chained through the
    /// usage total so the batch commits its final accounting once. Rows are
    /// keyed by transport identity and deduplicated within the batch: a key that
    /// recurs swaps its pending bytes instead of counting a second record that
    /// the batch would then collapse into one row. `None` means the store is at
    /// capacity: the caller must leave the affected cursors unwritten so the
    /// operations are redelivered.
    async fn quarantine_entries(
        &self,
        rejections: &[SyncRejection],
        txn_id: TxnId,
    ) -> Result<Option<Vec<(String, ByteView, Value)>>> {
        if rejections.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut usage = match storage_read_from_transaction(
            &self.storage,
            SYNC_QUARANTINE_USAGE_KEYSPACE.to_string(),
            ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
            Some(txn_id),
        )
        .await?
        {
            Some(value) => SyncQuarantineUsage::from_bytes(value.as_ref())
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            None => SyncQuarantineUsage::default(),
        };
        let quarantined_at_ms = unix_timestamp_millis();
        let mut pending: BTreeMap<Vec<u8>, (String, ByteView, Value)> = BTreeMap::new();
        for rejection in rejections {
            let key = rejection.identity.storage_key();
            // A redelivered operation replaces its own row, so its bytes are
            // swapped rather than added to the usage total. A key already
            // pending in this batch is swapped against that pending value,
            // which is what the batch will actually store.
            let replaced_bytes = match pending.get(&key) {
                Some((_, _, value)) => Some(value.len() as u64),
                None => storage_read_from_transaction(
                    &self.storage,
                    SYNC_QUARANTINE_KEYSPACE.to_string(),
                    ByteView::from(key.clone()),
                    Some(txn_id),
                )
                .await?
                .map(|value| value.len() as u64),
            };
            match build_quarantine_entries(
                SyncQuarantineInput {
                    identity: rejection.identity,
                    evidence: rejection.evidence.clone(),
                    reason: &rejection.reason,
                    quarantined_at_ms,
                    replaced_bytes,
                },
                usage,
                SyncQuarantineCapacity::default(),
            ) {
                Ok(write) => {
                    usage = write.usage;
                    pending.insert(key, write.row);
                }
                Err(SyncQuarantineError::AtCapacity { .. }) => {
                    warn!(
                        topic_id = %rejection.identity.topic,
                        actor_id = %rejection.identity.actor,
                        actor_seq = rejection.identity.actor_seq,
                        records = usage.records,
                        bytes = usage.bytes,
                        "Holding the sync cursor: the quarantine store is at capacity"
                    );
                    return Ok(None);
                }
                Err(error) => return Err(NetError::Bootstrap(error.to_string())),
            }
        }
        let mut entries = pending.into_values().collect::<Vec<_>>();
        entries.push(
            quarantine_usage_entry(usage)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
        Ok(Some(entries))
    }

    /// Commit rejection evidence and the topic cursor in one transaction so the
    /// cursor can never move past evidence that is not durable. `false` means
    /// the store is at capacity and nothing was written.
    async fn commit_cursor_evidence(
        &self,
        rejections: &[SyncRejection],
        cursor_write: (String, ByteView, Value),
    ) -> Result<bool> {
        let txn_id = start_storage_transaction(&self.storage).await?;
        let mut writes = match self.quarantine_entries(rejections, txn_id).await {
            Ok(Some(entries)) => entries,
            Ok(None) => {
                self.abort_transaction(txn_id).await;
                return Ok(false);
            }
            Err(error) => {
                self.abort_transaction(txn_id).await;
                return Err(error);
            }
        };
        writes.push(cursor_write);
        if let Err(error) =
            storage_batch_delete_and_write_in_transaction(&self.storage, txn_id, Vec::new(), writes)
                .await
        {
            self.abort_transaction(txn_id).await;
            return Err(error);
        }
        Ok(true)
    }

    async fn abort_transaction(&self, txn_id: TxnId) {
        let _ = self
            .storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }
}

fn target_write_entry(target: DocumentSyncTarget, value: Value) -> (String, ByteView, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        value,
    )
}

async fn apply_create_event(
    storage: &StorageHandle,
    event: &MetadataCreateEventRecord,
    target: DocumentSyncTarget,
    bytes: Vec<u8>,
) -> Result<()> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        match create_fence_txn(storage, event, txn_id).await {
            Ok(true) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
        let writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            ByteView::from(bytes.clone()),
        )];
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), writes)
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
        "metadata create admission conflicted twice".to_string(),
    ))
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
    now: u64,
    revocation_index: Option<&RevocationIndex>,
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

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_POLICIES_PATH)
        && let Some(request_policies) = reducer_state.materialized_realm_policies()
    {
        config.request_policies = request_policies;
    }

    config.revocation_floor = config.revocation_floor.max(reducer_state.revocation_floor);
    // Without an index, the existing set remains fail-closed until each entry's expiry.
    if let Some(revocation_index) = revocation_index {
        // Union, so a locally accepted revocation is never dropped because the
        // replicated set has not carried it yet.
        config.merge_revocation_index(revocation_index, now);
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
    now: u64,
    revocation_index: Option<&RevocationIndex>,
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
        request_policies: reducer_state
            .materialized_realm_policies()
            .unwrap_or_default(),
        description: String::new(),
        placement_map: Vec::new(),
        strategies: Vec::new(),
        default_strategy_id: None,
        strategy_bindings: Vec::new(),
        placement_overrides: Vec::new(),
        placement_bindings: Vec::new(),
        placement_handle_ranges: Vec::new(),
        band_pools: Vec::new(),
        candidate_maps: Vec::new(),
        placement_activations: Vec::new(),
        placement_transitions: Vec::new(),
        revoked_tokens: Vec::new(),
        revocation_floor: reducer_state.revocation_floor,
    };
    overlay_realm_config_reducer_materialization(&mut config, reducer_state, now, revocation_index);
    Some(config)
}

fn needs_revocation_index(
    is_revocation: bool,
    config_present: bool,
    reducer_state: &AdminDocumentReducerState,
    now: u64,
) -> bool {
    is_revocation || !config_present || reducer_state.revocation_compaction_due(now)
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
) -> Result<MetadataPlacementOutcome<()>> {
    if !registry_identity_valid(&record) {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    let mut base_entries = metadata_registry_write_entries(&record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if let Some((_, _, value)) = base_entries.first_mut() {
        *value = primary_bytes.into();
    }
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id: record.group_id,
        document_id: record.document_id,
    };
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let delete_present = match delete_record_txn(storage, record.document_id, txn_id).await {
            Ok(delete) => delete.is_some(),
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        match record_fenced_txn(storage, &record, txn_id).await {
            Ok(true) => {
                // The stale local row carries the timestamp half of its index
                // key; the fenced incoming record may be stamped differently.
                let stale = match storage_read_from_transaction(
                    storage,
                    target.storage_keyspace().to_string(),
                    target.storage_key(),
                    Some(txn_id),
                )
                .await
                {
                    Ok(value) => value.and_then(|value| {
                        postcard::from_bytes::<MetadataRegistryRecord>(&value).ok()
                    }),
                    Err(error) => {
                        let _ = storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                };
                let deletes = metadata_registry_delete_entries(stale.as_ref().unwrap_or(&record));
                match storage_batch_delete_and_write_in_transaction(
                    storage,
                    txn_id,
                    deletes,
                    Vec::new(),
                )
                .await
                {
                    Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(())),
                    Err(NetError::Dht(message))
                        if message == StorageError::TransactionConflict.to_string() =>
                    {
                        let _ = storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                }
            }
            Ok(false) => {}
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
        match metadata_placement_fence_in_transaction(storage, &record, txn_id).await {
            Ok(MetadataPlacementOutcome::Accepted(MetadataPlacementFence)) => {}
            Ok(MetadataPlacementOutcome::Deferred(dependency)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Deferred(dependency));
            }
            Ok(MetadataPlacementOutcome::Rejected) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Rejected);
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let existing_value = match storage_read_from_transaction(
            storage,
            target.storage_keyspace().to_string(),
            target.storage_key(),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let existing = match existing_value
            .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
            .transpose()
        {
            Ok(existing) => existing,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(NetError::Bootstrap(error.to_string()));
            }
        };
        if existing
            .as_ref()
            .is_some_and(|existing| !registry_identity_matches(existing, &record))
        {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(MetadataPlacementOutcome::Rejected);
        }
        if let Some(existing) = existing.as_ref().filter(|existing| {
            delete_present || incoming_metadata_registry_stale_or_equal(existing, &record)
        }) {
            let repairs = match registry_sidecar_repairs(storage, existing, txn_id).await {
                Ok(repairs) => repairs,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            if repairs.is_empty() {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Accepted(()));
            }
            match storage_batch_delete_and_write_in_transaction(
                storage,
                txn_id,
                Vec::new(),
                repairs,
            )
            .await
            {
                Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(())),
                Err(NetError::Dht(message))
                    if message == StorageError::TransactionConflict.to_string() =>
                {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    continue;
                }
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            }
        }

        let entries = base_entries.clone();
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), entries)
            .await
        {
            Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(())),
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
        "metadata registry admission conflicted twice".to_string(),
    ))
}

async fn apply_metadata_graph_lifecycle_to_storage(
    storage: &StorageHandle,
    record: &MetadataGraphLifecycleRecord,
    primary_bytes: Vec<u8>,
) -> Result<bool> {
    if !record.is_deleted() {
        return Ok(false);
    }
    let (key_space, key, _) = metadata_graph_lifecycle_write_entry(record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let delete = match delete_record_txn(storage, record.document_id, txn_id).await {
            Ok(delete) => delete,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let Some(delete) = delete else {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        };
        let (registry_live, registry_row) = match registry_live_txn(
            storage,
            record.group_id,
            record.document_id,
            &delete,
            txn_id,
        )
        .await
        {
            Ok(live) => live,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        if !metadata_document_delete_matches_graph_lifecycle(&delete, record) || registry_live {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        }
        let deletes = registry_row
            .as_ref()
            .map(metadata_registry_delete_entries)
            .unwrap_or_default();
        let writes = vec![(key_space.clone(), key.clone(), primary_bytes.clone().into())];
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
        "metadata graph lifecycle conflicted twice".to_string(),
    ))
}

async fn apply_metadata_document_lifecycle_to_storage(
    storage: &StorageHandle,
    record: &MetadataDocumentLifecycleRecord,
    change: DocumentSyncChange,
) -> Result<bool> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let writes = match metadata_document_lifecycle_write_entries_if_current(
            storage, record, &change, txn_id,
        )
        .await
        {
            Ok(writes) => writes,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let cleanup_delete = if let MetadataDocumentLifecycleRecord::Delete { event } = record
            && event.tombstone.is_deleted()
        {
            let current = match delete_record_txn(storage, record.document_id(), txn_id).await {
                Ok(current) => current,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            if writes.is_some() {
                Some(event.clone())
            } else {
                current
            }
        } else {
            None
        };
        let deletes = if let Some(delete) = cleanup_delete.as_ref() {
            match registry_cleanup_txn(
                storage,
                delete.tombstone.group_id,
                delete.tombstone.document_id,
                delete,
                txn_id,
            )
            .await
            {
                Ok(deletes) => deletes,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            }
        } else {
            Vec::new()
        };
        if writes.is_none() && deletes.is_empty() {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        }
        match storage_batch_delete_and_write_in_transaction(
            storage,
            txn_id,
            deletes,
            writes.unwrap_or_default(),
        )
        .await
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
        "metadata document lifecycle conflicted twice".to_string(),
    ))
}

/// Fold a replicated PID mapping into the local row inside one transaction, with
/// its sync sidecar and shard-manifest entry. The merge is monotone and derived
/// entirely from the two rows, so replay, reordering, and a frozen holder catching
/// up all converge on the same state and the same manifest revision — and an
/// Active row can never overwrite a local Withdrawn tombstone.
///
/// The same transaction fences the stamped placement against the one the
/// document id decodes to, so a publisher authorized for one shard can neither
/// stamp a document belonging to another shard nor write that shard's manifest.
async fn store_pid_mapping(
    storage: &StorageHandle,
    realm_id: RealmId,
    incoming: &PersistentIdMapping,
    placement: PlacementRef,
) -> Result<MetadataPlacementOutcome<bool>> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        match derive_placement_txn(storage, realm_id, None, incoming.target, placement, txn_id)
            .await
        {
            Ok(MetadataPlacementOutcome::Accepted(_)) => {}
            Ok(MetadataPlacementOutcome::Deferred(dependency)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Deferred(dependency));
            }
            Ok(MetadataPlacementOutcome::Rejected) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Rejected);
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
        let merged = match pid_merge_txn(storage, incoming, txn_id).await {
            Ok(Some(merged)) => merged,
            Ok(None) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Accepted(false));
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let target = persistent_id_target(merged.target);
        let change = persistent_id_change(&merged, placement);
        let mut writes = vec![(
            PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
            ByteView::from(persistent_id_key(merged.target)),
            Value::from(
                merged
                    .to_bytes()
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            ),
        )];
        writes.push(
            document_sync_revision_write_entry(&target, &change)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
        if let Some(entry) = shard_manifest_write_entry(&target, &change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        {
            writes.push(entry);
        }
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), writes)
            .await
        {
            Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(true)),
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
        "persistent id mapping conflicted twice".to_string(),
    ))
}

/// `Ok(None)` when the local row already absorbs the incoming one.
async fn pid_merge_txn(
    storage: &StorageHandle,
    incoming: &PersistentIdMapping,
    txn_id: TxnId,
) -> Result<Option<PersistentIdMapping>> {
    let local = storage_read_from_transaction(
        storage,
        PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
        ByteView::from(persistent_id_key(incoming.target)),
        Some(txn_id),
    )
    .await?;
    let Some(local) = local else {
        return Ok(Some(incoming.clone()));
    };
    let mut local = PersistentIdMapping::from_bytes(&local)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if !local.merge(incoming) {
        return Ok(None);
    }
    Ok(Some(local))
}

async fn delete_registry_record(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
) -> Result<()> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let delete = match delete_record_txn(storage, document_id, txn_id).await {
            Ok(delete) => delete,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let Some(delete) = delete else {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(());
        };
        let deletes =
            match registry_cleanup_txn(storage, group_id, document_id, &delete, txn_id).await {
                Ok(deletes) => deletes,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
        if deletes.is_empty() {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(());
        }
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, deletes, Vec::new())
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
        "metadata registry cleanup conflicted twice".to_string(),
    ))
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
        AdminDocumentApplyStatus::Redundant | AdminDocumentApplyStatus::StaleOriginSequence => {
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

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) -> Result<()> {
    match storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted })
            if aborted == txn_id =>
        {
            Ok(())
        }
        // A conflicted commit consumes the transaction; the abort's goal is met.
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionNotFound,
        }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(NetError::Dht(error.to_string())),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while aborting transaction: {other:?}"
        ))),
    }
}

async fn abort_error(storage: &StorageHandle, txn_id: TxnId, error: NetError) -> NetError {
    match abort_txn(storage, txn_id).await {
        Ok(()) => error,
        Err(abort) => NetError::Dht(format!("{error}; transaction abort failed: {abort}")),
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
            | AdminDocumentOperation::GroupPoliciesSet { .. }
    ) {
        return Err(NetError::Bootstrap(
            "group admin operation sync only supports group creation, role seeds, role creation/removal, role user assignment updates, and policy updates"
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
        policies: Default::default(),
    });
    materialize_group_authorization(&mut auth_doc, &reducer_state, &event);
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
            | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
            | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
            | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
            | AdminDocumentOperation::RealmConfigPoliciesSet { .. }
            | AdminDocumentOperation::RealmConfigTokenRevoked { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports node ensure, OIDC provider updates, settings updates, description updates, quota updates, placement updates, policy updates, and token revocations"
                .to_string(),
        ));
    }

    let is_revocation = matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigTokenRevoked { .. }
    );
    if event.origin_node_id != event.actor.node_id
        || event.actor.realm_id != realm_id
        || event.actor.user_id.realm_id != realm_id
    {
        return Err(NetError::Bootstrap(
            "realm config event actor and origin do not match the target realm".to_string(),
        ));
    }

    for _ in 0..3 {
        let raw_now = unix_timestamp_secs();
        let txn_id = start_storage_transaction(storage).await?;
        let previous_state = match storage_read_from_transaction(
            storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
            admin_document_reducer_state_key(&event.target),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => match value
                .map(|bytes| decode_admin_document_reducer_state(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))
            {
                Ok(value) => value,
                Err(error) => return Err(abort_error(storage, txn_id, error).await),
            },
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };
        let previous_config = match storage_read_from_transaction(
            storage,
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => match value
                .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))
            {
                Ok(value) => value,
                Err(error) => return Err(abort_error(storage, txn_id, error).await),
            },
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };

        if is_revocation {
            let valid = revocation_origin_known(
                previous_config.as_ref(),
                previous_state.as_ref(),
                &event,
                realm_id,
            );
            if !valid {
                return Err(
                    abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(
                            "revocation origin is not an onboarded realm node in the transaction snapshot"
                                .to_string(),
                        ),
                    )
                    .await,
                );
            }
            if let AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash,
                expires_at,
                token_owner,
            } = &event.op
                && (!aruna_core::auth::valid_token_hash(token_hash)
                    || !valid_revocation_expiry(*expires_at, raw_now)
                    || token_owner.is_nil()
                    || token_owner.realm_id != realm_id)
            {
                return Err(abort_error(
                    storage,
                    txn_id,
                    NetError::Bootstrap(
                        "replicated revocation has invalid hash, expiry, or owner".to_string(),
                    ),
                )
                .await);
            }
        }

        let effective_now = previous_state
            .as_ref()
            .map_or(raw_now, |state| state.revocation_floor.max(raw_now));
        let mut reducer_state = previous_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
        let needs_index = needs_revocation_index(
            is_revocation,
            previous_config.is_some(),
            &reducer_state,
            effective_now,
        );
        let mut revocation_index =
            needs_index.then(|| reducer_state.revocation_index(effective_now));
        if is_revocation {
            let Some(index) = revocation_index.as_mut() else {
                return Err(abort_error(
                    storage,
                    txn_id,
                    NetError::Bootstrap("revocation index was not admitted".to_string()),
                )
                .await);
            };
            if let Err(error) = reducer_state.apply_revocation_event(&event, index) {
                return Err(
                    abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await,
                );
            }
        } else if let Err(error) = reducer_state.apply(&event) {
            return Err(abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await);
        }
        reducer_state.advance_revocation_floor(effective_now);
        if let Some(index) = revocation_index.as_mut() {
            index.compact(&mut reducer_state);
        }

        let (config, config_changed) = match previous_config {
            Some(mut config) => {
                if config.realm_id != realm_id {
                    return Err(
                        abort_error(
                            storage,
                            txn_id,
                            NetError::Bootstrap(format!(
                                "stored realm config document id {realm_id} does not match payload realm id {}",
                                config.realm_id
                            )),
                        )
                        .await,
                    );
                }
                let before = config.clone();
                overlay_realm_config_reducer_materialization(
                    &mut config,
                    &reducer_state,
                    effective_now,
                    revocation_index.as_ref(),
                );
                let changed = config != before;
                (Some(config), changed)
            }
            None => {
                let config = realm_config_from_reducer_materialization(
                    realm_id,
                    &reducer_state,
                    effective_now,
                    revocation_index.as_ref(),
                );
                let changed = config.is_some();
                (config, changed)
            }
        };
        if previous_state
            .as_ref()
            .is_some_and(|previous| previous == &reducer_state)
            && !config_changed
        {
            abort_txn(storage, txn_id).await?;
            return Ok(());
        }

        let mut writes = Vec::new();
        if config_changed && let Some(config) = config {
            let bytes = match config.to_bytes(&event.actor) {
                Ok(bytes) => bytes,
                Err(error) => {
                    return Err(abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(error.to_string()),
                    )
                    .await);
                }
            };
            writes.push((
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                bytes.into(),
            ));
        }
        let reducer_write = match admin_document_reducer_state_write_entry(&reducer_state) {
            Ok(write) => write,
            Err(error) => {
                return Err(
                    abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await,
                );
            }
        };
        writes.push(reducer_write);
        if previous_state
            .as_ref()
            .is_none_or(|previous| previous.conflicts != reducer_state.conflicts)
        {
            let conflict_writes = match admin_document_conflict_write_entries(&reducer_state) {
                Ok(writes) => writes,
                Err(error) => {
                    return Err(abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(error.to_string()),
                    )
                    .await);
                }
            };
            writes.extend(conflict_writes);
        }

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_state.as_ref(),
            Some(&reducer_state),
        );
        match storage_batch_delete_and_write_in_transaction(
            storage,
            txn_id,
            stale_conflict_deletes,
            writes,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(NetError::Dht(message))
                if message == StorageError::TransactionConflict.to_string() =>
            {
                abort_txn(storage, txn_id).await?;
            }
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        }
    }
    Err(NetError::Dht(
        "realm config admin operation conflicted three times".to_string(),
    ))
}

fn materialize_group_authorization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) {
    if let AdminDocumentOperation::GroupPoliciesSet { .. } = &event.op {
        if !reducer_state
            .conflicts
            .contains_key(aruna_core::admin_document_reducer::GROUP_POLICIES_PATH)
            && let Some(policies) = reducer_state.materialized_group_policies()
        {
            auth_doc.policies = policies;
        }
        return;
    }

    if let AdminDocumentOperation::GroupRoleCreated { role } = &event.op {
        materialize_group_role(auth_doc, reducer_state, role);
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

fn materialize_group_role(
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
    txn_id: TxnId,
) -> Result<Option<Vec<(String, ByteView, Value)>>> {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: record.document_id(),
    };
    if lifecycle_stale_txn(storage, &target, change, txn_id).await? {
        return Ok(None);
    }
    let mut acceptance_to_write = None;
    if let MetadataDocumentLifecycleRecord::Upsert { event } = record {
        validate_metadata_event(event)?;
        if create_fence_txn(storage, event, txn_id).await? {
            return Ok(None);
        }

        let accepted = storage_read_from_transaction(
            storage,
            METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
            metadata_create_acceptance_key(event.record.document_id),
            Some(txn_id),
        )
        .await?
        .map(|value| {
            postcard::from_bytes::<MetadataCreateEventRecord>(&value)
                .map_err(|error| NetError::Bootstrap(error.to_string()))
        })
        .transpose()?;
        if let Some(accepted) = accepted.as_ref() {
            validate_metadata_event(accepted)?;
        }
        if event_is_create(event) {
            if accepted
                .as_ref()
                .is_some_and(|accepted| !same_create_event(accepted, event))
            {
                return Ok(None);
            }
            if accepted.is_none() {
                acceptance_to_write = Some(event.as_ref());
            }
        } else if accepted.as_ref().is_none_or(|accepted| {
            !event_is_create(accepted)
                || !registry_identity_matches(&accepted.record, &event.record)
        }) {
            return Ok(None);
        }
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
    if let Some(event) = acceptance_to_write {
        entries.push(
            metadata_create_acceptance_write_entry(event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
    }
    entries.push(
        document_sync_revision_write_entry(&target, change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );
    if let Some(manifest) = shard_manifest_write_entry(&target, change)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?
    {
        entries.push(manifest);
    }
    Ok(Some(entries))
}

async fn lifecycle_stale_txn(
    storage: &StorageHandle,
    target: &DocumentSyncTarget,
    incoming: &DocumentSyncChange,
    txn_id: TxnId,
) -> Result<bool> {
    let value = storage_read_from_transaction(
        storage,
        DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
        document_sync_revision_key(target),
        Some(txn_id),
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

fn registry_identity_matches(
    existing: &MetadataRegistryRecord,
    incoming: &MetadataRegistryRecord,
) -> bool {
    existing.realm_id == incoming.realm_id
        && existing.group_id == incoming.group_id
        && existing.document_id == incoming.document_id
        && existing.document_path == incoming.document_path
        && existing.graph_iri == incoming.graph_iri
        && existing.permission_path == incoming.permission_path
        && existing.placement == incoming.placement
        && existing.created_at_ms == incoming.created_at_ms
        && existing.establishing_event_id == incoming.establishing_event_id
}

fn registry_identity_valid(record: &MetadataRegistryRecord) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(&record.document_path);
    record.establishing_event_id != Ulid::nil()
        && record.document_path == normalized_path
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(record.document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &record.realm_id,
                record.group_id,
                &normalized_path,
                record.document_id,
            )
}

fn validate_metadata_event(event: &MetadataCreateEventRecord) -> Result<()> {
    if !registry_identity_valid(&event.record)
        || event.record.last_event_id != event.event_id
        || event_is_create(event) && event.record.establishing_event_id != event.event_id
    {
        return Err(NetError::Bootstrap(
            "replicated metadata event has inconsistent event identity".to_string(),
        ));
    }
    Ok(())
}

fn event_is_create(event: &MetadataCreateEventRecord) -> bool {
    matches!(
        &event.payload,
        aruna_core::metadata::MetadataCreateEventPayload::Scaffold { .. }
            | aruna_core::metadata::MetadataCreateEventPayload::RoCrate { .. }
    )
}

fn same_create_event(
    accepted: &MetadataCreateEventRecord,
    incoming: &MetadataCreateEventRecord,
) -> bool {
    accepted.event_id == incoming.event_id
        && registry_identity_matches(&accepted.record, &incoming.record)
        && accepted.record.public == incoming.record.public
        && accepted.record.updated_at_ms == incoming.record.updated_at_ms
        && accepted.record.last_event_id == incoming.record.last_event_id
        && accepted.user_id == incoming.user_id
        && accepted.node_id == incoming.node_id
        && accepted.payload == incoming.payload
        && accepted.occurred_at_ms == incoming.occurred_at_ms
}

fn metadata_registry_freshness(record: &MetadataRegistryRecord) -> (u64, Ulid) {
    (record.updated_at_ms, record.last_event_id)
}

async fn registry_sidecar_repairs(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<Vec<(String, ByteView, Value)>> {
    let entries = metadata_registry_write_entries(record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut repairs = Vec::new();
    for (key_space, key, value) in entries.into_iter().skip(1) {
        let current =
            storage_read_from_transaction(storage, key_space.clone(), key.clone(), Some(txn_id))
                .await?;
        if current.as_ref() != Some(&value) {
            repairs.push((key_space, key, value));
        }
    }
    Ok(repairs)
}

async fn graph_record_txn(
    storage: &StorageHandle,
    graph_iri: &str,
    txn_id: TxnId,
) -> Result<Option<MetadataGraphLifecycleRecord>> {
    let value = storage_read_from_transaction(
        storage,
        METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
        metadata_graph_lifecycle_key(graph_iri),
        Some(txn_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(None);
    };
    let record: MetadataGraphLifecycleRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(Some(record))
}

async fn delete_record_txn(
    storage: &StorageHandle,
    document_id: Ulid,
    txn_id: TxnId,
) -> Result<Option<MetadataDocumentDeleteRecord>> {
    let value = storage_read_from_transaction(
        storage,
        METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
        metadata_document_lifecycle_key(document_id),
        Some(txn_id),
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

async fn create_fence_txn(
    storage: &StorageHandle,
    event: &MetadataCreateEventRecord,
    txn_id: TxnId,
) -> Result<bool> {
    if let Some(delete) = delete_record_txn(storage, event.record.document_id, txn_id).await? {
        return Ok(event.event_id <= delete.deleted_after_event_id);
    }
    Ok(graph_record_txn(storage, &event.record.graph_iri, txn_id)
        .await?
        .is_some_and(|record| record.is_deleted()))
}

async fn record_fenced_txn(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<bool> {
    if let Some(delete) = delete_record_txn(storage, record.document_id, txn_id).await? {
        return Ok(!registry_live_txn(
            storage,
            record.group_id,
            record.document_id,
            &delete,
            txn_id,
        )
        .await?
        .0);
    }
    Ok(graph_record_txn(storage, &record.graph_iri, txn_id)
        .await?
        .is_some_and(|record| record.is_deleted()))
}

async fn registry_live_txn(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
    delete: &MetadataDocumentDeleteRecord,
    txn_id: TxnId,
) -> Result<(bool, Option<MetadataRegistryRecord>)> {
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    let Some(value) = storage_read_from_transaction(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok((false, None));
    };
    let record: MetadataRegistryRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let live = record.updated_at_ms > delete.tombstone.updated_at_ms
        || record.last_event_id > delete.deleted_after_event_id;
    Ok((live, Some(record)))
}

async fn registry_cleanup_txn(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
    delete: &MetadataDocumentDeleteRecord,
    txn_id: TxnId,
) -> Result<Vec<(String, ByteView)>> {
    if !metadata_document_delete_matches_registry(delete, group_id, document_id) {
        return Ok(Vec::new());
    }
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    // A missing registry row means cleanup already ran; an equal delete
    // replay must stay a no-op.
    let Some(value) = storage_read_from_transaction(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    let record: MetadataRegistryRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if record.updated_at_ms > delete.tombstone.updated_at_ms
        || record.last_event_id > delete.deleted_after_event_id
    {
        return Ok(Vec::new());
    }
    Ok(metadata_registry_delete_entries(&record))
}

async fn metadata_placement_fence_in_transaction(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<MetadataPlacementOutcome<MetadataPlacementFence>> {
    Ok(
        match derive_placement_txn(
            storage,
            record.realm_id,
            Some(record.group_id),
            record.document_id,
            record.placement,
            txn_id,
        )
        .await?
        {
            MetadataPlacementOutcome::Accepted(_) => {
                MetadataPlacementOutcome::Accepted(MetadataPlacementFence)
            }
            MetadataPlacementOutcome::Deferred(dependency) => {
                MetadataPlacementOutcome::Deferred(dependency)
            }
            MetadataPlacementOutcome::Rejected => MetadataPlacementOutcome::Rejected,
        },
    )
}

/// The placement a structured metadata id must ride, derived from the realm
/// config inside the caller's transaction and compared against the `placement`
/// the publisher stamped. The transactional config read is the whole fence: a
/// concurrent config mutation conflicts the commit. `group_id` is compared only
/// when the caller knows it; a PID mapping target carries no group.
async fn derive_placement_txn(
    storage: &StorageHandle,
    realm_id: RealmId,
    group_id: Option<Ulid>,
    document_id: Ulid,
    placement: PlacementRef,
    txn_id: TxnId,
) -> Result<MetadataPlacementOutcome<PlacementRef>> {
    let dependency = DocumentSyncDependency::PlacementStrategy {
        realm_id,
        strategy_id: placement.strategy_id,
    };
    if placement == PlacementRef::NIL || placement.strategy_id.is_nil() {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let value = storage_read_from_transaction(
        storage,
        REALM_CONFIG_KEYSPACE.to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(MetadataPlacementOutcome::Deferred(
            DocumentSyncDependency::RealmConfig(realm_id),
        ));
    };
    let config = RealmConfigDocument::from_bytes(&value)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if config.realm_id != realm_id {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    let id = match MetaResourceId::from_bytes(document_id.to_bytes()) {
        Ok(id) => id,
        Err(_) => return Ok(MetadataPlacementOutcome::Rejected),
    };
    let resolved = match config.binding_directory().resolve_id(&id, |strategy_id| {
        config
            .strategy(&strategy_id)
            .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
    }) {
        Ok(resolved) => resolved,
        Err(BindingError::UnknownStrategy(_)) => {
            return Ok(MetadataPlacementOutcome::Deferred(dependency));
        }
        Err(BindingError::Unknown(_)) => {
            return Ok(MetadataPlacementOutcome::Deferred(
                DocumentSyncDependency::RealmConfig(realm_id),
            ));
        }
        Err(BindingError::Conflicted(_) | BindingError::BucketOutOfRange(_)) => {
            return Ok(MetadataPlacementOutcome::Rejected);
        }
    };
    let scope_matches = match resolved.scope {
        PlacementScope::Realm(scope_realm) => scope_realm == realm_id,
        PlacementScope::Group(scope_group) => group_id.is_none_or(|group| scope_group == group),
    };
    let derived = PlacementRef {
        strategy_id: resolved.strategy_id,
        shard: u32::from(resolved.bucket.get()),
    };
    if resolved.document_class != DocumentClass::Metadata || !scope_matches || derived != placement
    {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    Ok(MetadataPlacementOutcome::Accepted(derived))
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
                ByteView::from(Ulid::generate().to_bytes().to_vec()),
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

#[cfg(test)]
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

#[cfg(test)]
fn group_sync_topics<F>(
    topic_ids: &[irokle_crate::TopicId],
    mut select: F,
) -> BTreeMap<BTreeSet<PeerId>, (PeerSelection, Vec<irokle_crate::TopicId>)>
where
    F: FnMut(irokle_crate::TopicId) -> PeerSelection,
{
    let mut groups: BTreeMap<BTreeSet<PeerId>, (PeerSelection, Vec<irokle_crate::TopicId>)> =
        BTreeMap::new();
    for topic_id in topic_ids.iter().copied() {
        let selection = select(topic_id);
        let selected = selection.peers.clone();
        if let Some((group, topics)) = groups.get_mut(&selected) {
            group.truncated |= selection.truncated;
            topics.push(topic_id);
        } else {
            groups.insert(selected, (selection, vec![topic_id]));
        }
    }
    groups
}

fn select_sync_peers(
    candidates: impl IntoIterator<Item = PeerId>,
    local_peer: PeerId,
    subject: &[u8],
    round: u64,
) -> PeerSelection {
    let mut ranked = candidates
        .into_iter()
        .filter(|peer| *peer != local_peer)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|peer| (peer, peer_score(subject, peer)))
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|(left_peer, left_score), (right_peer, right_score)| {
        left_score
            .cmp(right_score)
            .then_with(|| left_peer.as_bytes().cmp(right_peer.as_bytes()))
    });
    let candidate_count = ranked.len();
    let selected = DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT.min(candidate_count);
    let start = if candidate_count == 0 {
        0
    } else {
        ((round as u128 * DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT as u128) % candidate_count as u128)
            as usize
    };
    let peers = (0..selected)
        .map(|offset| ranked[(start + offset) % candidate_count].0)
        .collect();
    PeerSelection {
        peers,
        truncated: candidate_count > selected,
        round,
    }
}

fn peer_score(subject: &[u8], peer: PeerId) -> [u8; 32] {
    let mut input = Vec::with_capacity(DOCUMENT_SYNC_FANOUT_DOMAIN.len() + subject.len() + 32);
    input.extend_from_slice(DOCUMENT_SYNC_FANOUT_DOMAIN);
    input.extend_from_slice(subject);
    input.extend_from_slice(peer.as_bytes());
    *DhtKeyId::from_data(&input).as_bytes()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdminOperationFamily {
    Group,
    RealmAuthorization,
    User,
    RealmConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum DocumentSyncDependency {
    RealmConfig(RealmId),
    RealmAuthorization(RealmId),
    PlacementStrategy {
        realm_id: RealmId,
        strategy_id: Ulid,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeferredTopicRegistrationOutcome {
    Inserted,
    AlreadyRegistered,
    CapacityExceeded,
}

#[derive(Debug, PartialEq, Eq)]
enum AdminEventValidation {
    Accepted,
    Rejected(String),
    Deferred {
        dependency: Option<DocumentSyncDependency>,
        reason: String,
    },
}

fn satisfied_document_sync_dependencies(
    target: &DocumentSyncTarget,
    event: &AdminDocumentEvent,
) -> Vec<DocumentSyncDependency> {
    let mut dependencies = Vec::new();
    match target {
        DocumentSyncTarget::RealmConfig { realm_id } => {
            dependencies.push(DocumentSyncDependency::RealmConfig(*realm_id));
            if let AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy } =
                &event.op
            {
                dependencies.push(DocumentSyncDependency::PlacementStrategy {
                    realm_id: *realm_id,
                    strategy_id: strategy.strategy_id,
                });
            }
        }
        DocumentSyncTarget::RealmAuthorization { realm_id } => {
            dependencies.push(DocumentSyncDependency::RealmAuthorization(*realm_id));
        }
        _ => {}
    }
    dependencies
}

async fn document_sync_dependency_available(
    storage: &StorageHandle,
    dependency: DocumentSyncDependency,
) -> Result<bool> {
    match dependency {
        DocumentSyncDependency::RealmConfig(realm_id) => {
            Ok(read_admin_realm_config(storage, realm_id).await?.is_some())
        }
        DocumentSyncDependency::RealmAuthorization(realm_id) => {
            Ok(read_admin_realm_authorization(storage, realm_id)
                .await?
                .is_some())
        }
        DocumentSyncDependency::PlacementStrategy {
            realm_id,
            strategy_id,
        } => Ok(read_admin_realm_config(storage, realm_id)
            .await?
            .is_some_and(|config| {
                config.realm_id == realm_id && config.strategy(&strategy_id).is_some()
            })),
    }
}

fn register_deferred_topic(
    deferred_topics: &mut BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>>,
    dependency: DocumentSyncDependency,
    topic_id: irokle_crate::TopicId,
) -> DeferredTopicRegistrationOutcome {
    if deferred_topics
        .get(&dependency)
        .is_some_and(|topics| topics.contains(&topic_id))
    {
        return DeferredTopicRegistrationOutcome::AlreadyRegistered;
    }
    let total_topics = deferred_topics.values().map(BTreeSet::len).sum::<usize>();
    let dependency_topics = deferred_topics
        .get(&dependency)
        .map(BTreeSet::len)
        .unwrap_or_default();
    if total_topics >= MAX_DEFERRED_TOPICS
        || dependency_topics >= MAX_DEFERRED_TOPICS_PER_DEPENDENCY
    {
        return DeferredTopicRegistrationOutcome::CapacityExceeded;
    }
    deferred_topics
        .entry(dependency)
        .or_default()
        .insert(topic_id);
    DeferredTopicRegistrationOutcome::Inserted
}

fn remove_deferred_topic(
    deferred_topics: &mut BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>>,
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
    realm_id: RealmId,
    placement: &PlacementRef,
) -> Result<AdminEventValidation> {
    let reject = |reason: &str| Ok(AdminEventValidation::Rejected(reason.to_string()));

    if target.sync_topic_id(realm_id, placement) != topic_id {
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
        | AdminDocumentOperation::GroupCreated { .. }
        | AdminDocumentOperation::GroupPoliciesSet { .. } => AdminOperationFamily::Group,
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
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
        | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
        | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
        | AdminDocumentOperation::RealmConfigPoliciesSet { .. }
        | AdminDocumentOperation::RealmConfigTokenRevoked { .. } => {
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
    if matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding }
            if matches!(
                binding.scope,
                aruna_core::structs::PlacementScope::Realm(binding_realm_id)
                    if binding_realm_id != event.actor.realm_id
            )
    ) {
        return reject("placement binding realm does not match the admin event target");
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
        AdminDocumentOperation::GroupRoleCreated { role } => {
            // Distributed events must enforce the same subtree confinement as
            // local issuance; the publisher is not trusted to have done so.
            let AdminDocumentTarget::Group { group_id } = &event.target else {
                return reject("group role event target must be a group");
            };
            let subtree_root =
                aruna_core::permission_path::role_subtree_root(event.actor.realm_id, group_id);
            if role.permissions.keys().any(|pattern| {
                !aruna_core::permission_path::role_path_confined(pattern, &subtree_root)
            }) {
                return reject("group role grants outside its group subtree");
            }
        }
        AdminDocumentOperation::GroupRoleAdded { .. }
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
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. } => {}
        AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
        | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. } => {}
        AdminDocumentOperation::RealmConfigNodePlacementSet { entry } => {
            if let Some(label) = reserved_label(&entry.labels) {
                return reject(&format!(
                    "placement entry must not set derived label {label}"
                ));
            }
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy }
            if strategy.replica_count == Some(0) =>
        {
            return reject("placement strategy replica count must be greater than zero");
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { .. } => {}
        AdminDocumentOperation::RealmConfigPoliciesSet { policies }
        | AdminDocumentOperation::GroupPoliciesSet { policies } => {
            if let Err(error) = aruna_core::request_policy::validate_policy_set(policies) {
                return reject(&format!("invalid policy set: {error}"));
            }
        }
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash,
            expires_at,
            token_owner,
            ..
        } => {
            if !aruna_core::auth::valid_token_hash(token_hash) {
                return reject("revoked bearer token hash is malformed");
            }
            if !valid_revocation_expiry(*expires_at, unix_timestamp_secs()) {
                return reject("revoked bearer token expiry exceeds the admission window");
            }
            if token_owner.is_nil() || token_owner.realm_id != event.actor.realm_id {
                return reject("revoked bearer token owner is malformed");
            }
        }
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

    if previous_state
        .as_ref()
        .is_some_and(|state| state.target != event.target)
    {
        return reject("stored admin reducer state has the wrong target");
    }
    if let AdminDocumentOperation::RealmConfigTokenRevoked { token_hash, .. } = &event.op {
        if revocation_origin_full(previous_state.as_ref(), event, token_hash) {
            return reject("revocation origin reached its live revocation cap");
        }
        return Ok(AdminEventValidation::Accepted);
    }

    let mut reducer_state =
        previous_state.unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
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

/// Whether this origin already holds the per-origin bound a local mint obeys.
/// Replacing its own entry stays allowed; the flooding origin is rejected rather
/// than trimmed, so a valid revocation is never discarded to make room.
fn revocation_origin_full(
    state: Option<&AdminDocumentReducerState>,
    event: &AdminDocumentEvent,
    token_hash: &str,
) -> bool {
    let Some(state) = state else {
        return false;
    };
    let index = state.revocation_index(state.revocation_floor.max(unix_timestamp_secs()));
    index.origin(token_hash) != Some(event.origin_node_id)
        && index.count(&event.origin_node_id) >= MAX_LIVE_REVOCATIONS_PER_ORIGIN
}

fn revocation_origin_known(
    config: Option<&RealmConfigDocument>,
    state: Option<&AdminDocumentReducerState>,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
) -> bool {
    if config
        .is_some_and(|config| config.realm_id == realm_id && config.has_node(event.origin_node_id))
    {
        return true;
    }

    let Some(state) = state else {
        return false;
    };
    let path = realm_config_node_path(&event.origin_node_id);
    state
        .user_subject_ids
        .get(&path)
        .is_some_and(|version| event.observed.observes(&version.dot))
        || state.conflicts.get(&path).is_some_and(|conflict| {
            conflict
                .values
                .iter()
                .any(|value| event.observed.observes(&value.dot))
        })
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
    if current_config
        .as_ref()
        .is_some_and(|config| config.realm_id != realm_id)
    {
        return Ok(AdminEventValidation::Rejected(
            "stored realm config has the wrong realm".to_string(),
        ));
    }
    if matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigTokenRevoked { .. }
    ) {
        if !revocation_origin_known(current_config.as_ref(), previous_state, event, realm_id) {
            return Ok(AdminEventValidation::Deferred {
                dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
                reason: if current_config.is_some() {
                    "revocation event origin onboarding is not yet materialized"
                } else {
                    "current realm config is unavailable"
                }
                .to_string(),
            });
        }
        return Ok(AdminEventValidation::Accepted);
    }
    // Placement reducer state precedes full config materialization at bootstrap.
    let mut placement_config = current_config
        .clone()
        .unwrap_or_else(|| RealmConfigDocument::default_for_realm(realm_id, Vec::new()));
    if let Some(state) = previous_state {
        overlay_realm_config_placement_reducer_materialization(&mut placement_config, state);
    }
    // Band pools form a causal delegation tree; reject a forged or
    // non-owning issuer, and defer a child until its parent replicates.
    if let AdminDocumentOperation::RealmConfigBandPoolAssigned { pool } = &event.op {
        match admit_band_pool(&placement_config.band_pools, pool, &event.origin_node_id) {
            PoolAdmission::Reject => {
                return Ok(AdminEventValidation::Rejected(
                    "band pool lineage is invalid".to_string(),
                ));
            }
            PoolAdmission::MissingParent => {
                return Ok(AdminEventValidation::Deferred {
                    dependency: None,
                    reason: "band pool parent is not yet replicated".to_string(),
                });
            }
            PoolAdmission::Accept => {}
        }
    }
    if let AdminDocumentOperation::RealmConfigHandleRangeGranted { range } = &event.op {
        let canonical = range.len() == HANDLE_RANGE_SIZE
            && range
                .start
                .checked_sub(FIRST_GRANTABLE_HANDLE)
                .is_some_and(|offset| offset % HANDLE_RANGE_SIZE == 0)
            && range.start.checked_add(HANDLE_RANGE_SIZE) == Some(range.end);
        if !canonical {
            return Ok(AdminEventValidation::Rejected(
                "handle grant is not one canonical band".to_string(),
            ));
        }
        let spans = coordinator_spans(&placement_config.band_pools, &event.origin_node_id);
        if spans.is_empty() {
            return Ok(AdminEventValidation::Deferred {
                dependency: None,
                reason: "coordinator band pool is not yet replicated".to_string(),
            });
        }
        if !spans
            .iter()
            .any(|(start, end)| *start <= range.start && range.end <= *end)
        {
            return Ok(AdminEventValidation::Rejected(
                "handle grant lies outside the coordinator band pool".to_string(),
            ));
        }
    }
    if let Some(config) = current_config.as_ref() {
        let server_binding = match (
            configured_node_kind(config, &event.origin_node_id),
            &event.op,
        ) {
            (
                Some(RealmNodeKind::Server),
                AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding },
            ) => {
                binding.allocated_by == Some(event.origin_node_id)
                    && binding.has_valid_provenance(&config.handle_range_directory())
            }
            _ => false,
        };
        if matches!(
            configured_node_kind(config, &event.origin_node_id),
            Some(RealmNodeKind::Management)
        ) && matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding }
                if !binding.has_valid_provenance(&config.handle_range_directory())
        ) {
            // The granting range event may still be in flight in the same batch
            // (onboarding writes grant + JobControl binding back to back).
            return Ok(AdminEventValidation::Deferred {
                dependency: None,
                reason: "placement binding provenance is not yet valid".to_string(),
            });
        }
        return Ok(
            if matches!(
                configured_node_kind(config, &event.origin_node_id),
                Some(RealmNodeKind::Management)
            ) || server_binding
            {
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
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
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
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
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
            dependency: Some(DocumentSyncDependency::RealmAuthorization(realm_id)),
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
        AdminDocumentOperation::GroupPoliciesSet { .. } => {
            format!("/{realm_id}/g/{group_id}/admin/config")
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
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
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
                dependency: Some(DocumentSyncDependency::RealmAuthorization(realm_id)),
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
            let Ok(glob) = compile_permission_matcher(pattern) else {
                return false;
            };
            if !glob.is_match(path) {
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
fn validate_watch_interest(
    target: &DocumentSyncTarget,
    bytes: &[u8],
) -> std::result::Result<(), String> {
    if bytes.len() > NOTIFICATION_WATCH_INTEREST_BYTES_CAP {
        return Err(format!(
            "watch interest digest exceeds serialized byte cap {}",
            NOTIFICATION_WATCH_INTEREST_BYTES_CAP
        ));
    }
    let DocumentSyncTarget::WatchInterest { realm_id, node_id } = target else {
        return Err("target is not a watch interest digest".to_string());
    };
    let digest = WatchInterestDigest::from_bytes(bytes)
        .map_err(|error| format!("undecodable watch interest digest: {error}"))?;
    if digest.entries.len() > NOTIFICATION_WATCH_INTEREST_ENTRY_CAP {
        return Err(format!(
            "watch interest digest exceeds entry cap {}",
            NOTIFICATION_WATCH_INTEREST_ENTRY_CAP
        ));
    }
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
    let known_mask = WatchEventMask::METADATA_CREATED
        | WatchEventMask::DATA_UPLOADED
        | WatchEventMask::SYNC_COMPLETED
        | WatchEventMask::SYNC_FAILED;
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

/// Validates a replicated PID mapping against its sync target and change: the
/// canonical PID for the target document, a consistent kind/status/provenance
/// triple, and a change that is exactly the one the mapping row derives. The
/// caller enforces the publisher identity against the signed actor.
fn validate_pid_mapping(
    document_id: Ulid,
    mapping: &PersistentIdMapping,
    change: &DocumentSyncChange,
) -> std::result::Result<(), String> {
    if mapping.target != document_id {
        return Err(format!(
            "mapping target {} does not match target document {document_id}",
            mapping.target
        ));
    }
    if mapping.pid != MetadataRegistryRecord::graph_iri_for(document_id) {
        return Err(format!("mapping pid `{}` is not canonical", mapping.pid));
    }
    if !matches!(mapping.kind, PersistentIdKind::Conceptual) {
        return Err("mapping kind is unsupported".to_string());
    }
    if mapping.minted_at_ms.is_some() != mapping.minted_by.is_some() {
        return Err("mapping mint provenance is incomplete".to_string());
    }
    match mapping.status {
        PersistentIdStatus::Active => {
            if mapping.minted_at_ms.is_none() || mapping.withdrawn_at_ms.is_some() {
                return Err("active mapping has inconsistent transition fields".to_string());
            }
        }
        PersistentIdStatus::Withdrawn => {
            if mapping.withdrawn_at_ms.is_none() {
                return Err("withdrawn mapping has no withdrawal timestamp".to_string());
            }
        }
    }
    if change.kind != DocumentSyncChangeKind::Upsert {
        return Err("mapping event is not an upsert".to_string());
    }
    if *change != persistent_id_change(mapping, change.placement) {
        return Err("mapping revision does not match its sync change".to_string());
    }
    Ok(())
}

fn topic_cursor_key(topic_id: irokle_crate::TopicId) -> ByteView {
    let mut key = b"topic-cursor/".to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

fn current_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: irokle_crate::TopicId,
) -> Result<u64> {
    let Some(value) = cursors
        .get(topic_cursor_key(topic_id))
        .map_err(|error| NetError::Bootstrap(error.to_string()))?
    else {
        return Ok(0);
    };
    if value.len() != std::mem::size_of::<u64>() {
        return Ok(0);
    }
    let mut bytes = [0u8; std::mem::size_of::<u64>()];
    bytes.copy_from_slice(value.as_ref());
    Ok(u64::from_be_bytes(bytes))
}

fn advance_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: irokle_crate::TopicId,
    round: u64,
) -> Result<()> {
    cursors
        .update_fetch(topic_cursor_key(topic_id), |value| {
            let stored = value
                .filter(|value| value.len() == std::mem::size_of::<u64>())
                .map(|value| {
                    let mut bytes = [0u8; std::mem::size_of::<u64>()];
                    bytes.copy_from_slice(value.as_ref());
                    u64::from_be_bytes(bytes)
                })
                .unwrap_or_default();
            if stored == round {
                Some(fjall::Slice::from(round.wrapping_add(1).to_be_bytes()))
            } else {
                value.cloned()
            }
        })
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    // Advance only after network attempts; a crash before persistence safely repeats them.
    Ok(())
}

fn remove_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: irokle_crate::TopicId,
) -> Result<()> {
    cursors
        .remove(topic_cursor_key(topic_id))
        .map_err(|error| NetError::Bootstrap(error.to_string()))
}

fn deferred_topics_key() -> ByteView {
    // Retain the original key so previously persisted admin dependencies decode.
    ByteView::from(b"deferred-admin-topics".to_vec())
}

async fn read_inbound_sync_messages(
    recv: &mut iroh::endpoint::RecvStream,
    reservation: &mut InboundByteReservation,
) -> Result<(Vec<SyncMessage>, Vec<irokle_crate::TopicId>)> {
    let mut messages = Vec::new();
    let mut topics = BTreeSet::new();
    let mut bytes_read = 0usize;
    let mut frame_index = 0usize;
    while let Some(frame) = timeout(
        DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT,
        read_next_inbound_sync_frame(recv, &mut bytes_read, reservation),
    )
    .await
    .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT))??
    {
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
    reservation: &mut InboundByteReservation,
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
    reservation.reserve(len)?;

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

/// Per-peer outcome of probing shard topics: which topics the peer holds a
/// genesis for (`known`) and which it positively confirmed it has none of
/// (`confirmed_unknown`). A probed topic in neither was refused — the peer holds
/// it but the prober may not open it yet, so it must not be treated as unknown.
#[derive(Debug, Default, PartialEq, Eq)]
struct PeerTopicProbe {
    known: BTreeSet<irokle_crate::TopicId>,
    confirmed_unknown: BTreeSet<irokle_crate::TopicId>,
}

impl PeerTopicProbe {
    fn merge(&mut self, other: PeerTopicProbe) {
        self.known.extend(other.known);
        self.confirmed_unknown.extend(other.confirmed_unknown);
    }
}

/// Buckets a peer's Open responses for the `wanted` topics: a non-empty summary
/// ⇒ the peer holds a genesis; an empty summary (untyped, headless) ⇒ positive
/// confirmation the peer has none; a topic with no summary is left out of both,
/// meaning the peer refused it (holds it but the prober may not open it yet).
fn classify_probe_responses(
    wanted: &BTreeSet<irokle_crate::TopicId>,
    responses: Vec<SyncMessage>,
) -> PeerTopicProbe {
    let mut probe = PeerTopicProbe::default();
    for response in responses {
        if let SyncMessage::Summary(summary) = response
            && wanted.contains(&summary.topic_id)
        {
            if remote_summary_is_empty(&summary) {
                probe.confirmed_unknown.insert(summary.topic_id);
            } else {
                probe.known.insert(summary.topic_id);
            }
        }
    }
    probe
}

fn peer_id_to_endpoint_addr(peer_id: PeerId) -> Result<iroh::EndpointAddr> {
    let endpoint_id = iroh::EndpointId::from_bytes(peer_id.as_bytes())
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(iroh::EndpointAddr::from(endpoint_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::admin_document_reducer::REALM_CONFIG_DEFAULT_STRATEGY_PATH;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use aruna_core::alpn::Alpn;
    use aruna_core::auth::{MAX_BEARER_TOKEN_LIFETIME_SECS, REVOCATION_GRACE_SECS};
    use aruna_core::document::{DocumentSyncChangeKind, DocumentSyncRevision};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
        DOCUMENT_SYNC_REVISION_KEYSPACE, GROUP_KEYSPACE, METADATA_CREATE_ACCEPTANCE_KEYSPACE,
        METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
        METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
        METADATA_GRAPH_PRUNE_JOB_KEYSPACE, METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE,
        USER_KEYSPACE, USER_SUBJECT_CLAIMS_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
    };
    use aruna_core::metadata::MetadataCreateEventPayload;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
        metadata_create_acceptance_key, metadata_document_key, metadata_event_log_key,
        metadata_registry_key, subject_index_key, subject_index_value,
    };
    use aruna_core::structs::{
        Actor, BandPool, BindingScope, DocumentClass, FIRST_GRANTABLE_HANDLE, Group,
        GroupAuthorizationDocument, GroupQuotaOverride, HANDLE_BANDS, HandleRange, METADATA_HANDLE,
        MetadataReplicationConfig, NodePlacementEntry, OidcProviderConfig, Permission,
        PlacementBinding, PlacementOverride, PlacementRef, PlacementStrategy, QuotaConfig,
        RealmAuthorizationDocument, RealmConfigDocument, RealmDiscoveryConfig, RealmId,
        RealmNodeKind, Role, SYNC_QUARANTINE_MAX_RECORDS, StaticRealmEndpoint, StrategyBinding,
        SyncQuarantineFamily, SyncQuarantineRecord, UserGroupCapOverride, band_start,
    };
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::{MetaResourceId, StructuredId, UserId};
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::{env, process::Command};
    use tempfile::TempDir;

    const DOCUMENT_SYNC_RESTART_CHILD_PATH_ENV: &str = "ARUNA_NET_DOCUMENT_SYNC_RESTART_CHILD_PATH";
    const DOCUMENT_SYNC_RESTART_CHILD_TEST: &str =
        "document_sync::tests::buffered_document_sync_publish_restart_child_process";

    fn peer(seed: u8) -> PeerId {
        node_id_to_peer_id(&iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    #[test]
    fn budget_caps_streams() {
        // Per-peer and global caps hold, and dropping a permit restores both.
        let budget = Arc::new(InboundSyncBudget::default());
        let mut held = Vec::new();
        for _ in 0..DOCUMENT_SYNC_INBOUND_PEER_STREAMS {
            held.push(budget.acquire(peer(1)).expect("within per-peer budget"));
        }
        assert!(budget.acquire(peer(1)).is_none());
        assert!(budget.acquire(peer(2)).is_some());

        held.pop();
        assert!(budget.acquire(peer(1)).is_some());

        let mut fill = Vec::new();
        for seed in 10..u8::MAX {
            match budget.acquire(peer(seed)) {
                Some(permit) => fill.push(permit),
                None => break,
            }
        }
        assert!(budget.acquire(peer(3)).is_none());
        fill.clear();
        assert!(budget.acquire(peer(3)).is_some());
    }

    #[test]
    fn inbound_timeout_order() {
        assert!(DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT < DOCUMENT_SYNC_INBOUND_STREAM_TIMEOUT);
    }

    #[test]
    fn sync_peers_bounded() {
        let selection =
            select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 0);

        assert_eq!(selection.peers.len(), DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
        assert!(selection.truncated);
        assert!(!selection.peers.contains(&peer(0)));
    }

    #[test]
    fn sync_peers_dedup() {
        let candidates = (1u8..=9).map(peer).collect::<Vec<_>>();
        let selection = select_sync_peers(
            candidates.iter().copied().chain(candidates.iter().copied()),
            peer(0),
            b"document-sync-subject",
            0,
        );

        assert_eq!(selection.peers.len(), DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
        assert!(selection.truncated);
    }

    #[test]
    fn sync_peers_cover() {
        let candidates = (1u8..=17).map(peer).collect::<BTreeSet<_>>();
        let mut seen = BTreeSet::new();
        let rounds = candidates.len().div_ceil(DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
        for round in 0..rounds as u64 {
            seen.extend(
                select_sync_peers(
                    candidates.iter().copied(),
                    peer(0),
                    b"document-sync-subject",
                    round,
                )
                .peers,
            );
        }

        assert_eq!(seen, candidates);
    }

    #[test]
    fn fanout_cursor_restart() {
        let root = TempDir::new().expect("fanout cursor tempdir");
        let topic_id = topic(42);
        {
            let db = fjall::OptimisticTxDatabase::builder(root.path())
                .manual_journal_persist(true)
                .open()
                .expect("fanout cursor database");
            let cursors = db
                .keyspace(
                    DOCUMENT_SYNC_FANOUT_KEYSPACE,
                    fjall::KeyspaceCreateOptions::default,
                )
                .expect("fanout cursor keyspace");
            assert_eq!(current_cursor(&cursors, topic_id).expect("first cursor"), 0);
            advance_cursor(&cursors, topic_id, 0).expect("advance cursor");
            db.persist(fjall::PersistMode::SyncAll)
                .expect("persist fanout cursor");
        }
        let db = fjall::OptimisticTxDatabase::builder(root.path())
            .manual_journal_persist(true)
            .open()
            .expect("reopen fanout cursor database");
        let cursors = db
            .keyspace(
                DOCUMENT_SYNC_FANOUT_KEYSPACE,
                fjall::KeyspaceCreateOptions::default,
            )
            .expect("reopen fanout cursor keyspace");
        assert_eq!(
            current_cursor(&cursors, topic_id).expect("restarted cursor"),
            1
        );
    }

    #[test]
    fn fanout_cursor_clear() {
        // A topic reset can remove stale fan-out progress before re-emission.
        let selection = select_sync_peers((1u8..=9).map(peer), peer(0), b"missing-topic", 0);
        assert_eq!(selection.peers.len(), DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
        assert!(selection.truncated);
        let root = TempDir::new().expect("fanout cursor clear tempdir");
        let topic_id = topic(43);
        {
            let db = fjall::OptimisticTxDatabase::builder(root.path())
                .manual_journal_persist(true)
                .open()
                .expect("fanout cursor clear database");
            let cursors = db
                .keyspace(
                    DOCUMENT_SYNC_FANOUT_KEYSPACE,
                    fjall::KeyspaceCreateOptions::default,
                )
                .expect("fanout cursor clear keyspace");
            advance_cursor(&cursors, topic_id, 0).expect("create fanout cursor");
            assert!(
                cursors
                    .contains_key(topic_cursor_key(topic_id))
                    .expect("find fanout cursor")
            );
            remove_cursor(&cursors, topic_id).expect("clear fanout cursor");
            db.persist(fjall::PersistMode::SyncAll)
                .expect("persist cleared fanout cursor");
        }
        let db = fjall::OptimisticTxDatabase::builder(root.path())
            .manual_journal_persist(true)
            .open()
            .expect("reopen fanout cursor clear database");
        let cursors = db
            .keyspace(
                DOCUMENT_SYNC_FANOUT_KEYSPACE,
                fjall::KeyspaceCreateOptions::default,
            )
            .expect("reopen fanout cursor clear keyspace");
        assert!(
            !cursors
                .contains_key(topic_cursor_key(topic_id))
                .expect("find cleared fanout cursor")
        );
    }

    #[test]
    fn sync_peers_permutation() {
        let forward = select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 0);
        let reverse = select_sync_peers(
            (1u8..=32).rev().map(peer),
            peer(0),
            b"document-sync-subject",
            0,
        );

        assert_eq!(forward.peers, reverse.peers);
        assert_ne!(forward.peers, (1u8..=8).map(peer).collect::<BTreeSet<_>>());
    }

    #[test]
    fn sync_peers_rotate() {
        let first = select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 0);
        let second = select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 1);

        assert!(second.peers.iter().any(|peer| !first.peers.contains(peer)));
    }

    #[test]
    fn sync_topics_grouped() {
        let topics = [topic(1), topic(2), topic(3)];
        let groups = group_sync_topics(&topics, |topic_id| {
            let selected = if topic_id == topics[1] {
                peer(2)
            } else {
                peer(1)
            };
            PeerSelection {
                peers: BTreeSet::from([selected]),
                truncated: false,
                round: 0,
            }
        });

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .values()
                .map(|(_, topics)| topics.len())
                .sum::<usize>(),
            topics.len()
        );
        assert!(
            groups
                .values()
                .any(|(_, grouped)| grouped == &vec![topics[1]])
        );
        assert!(
            groups
                .values()
                .any(|(_, grouped)| grouped == &vec![topics[0], topics[2]])
        );
    }

    #[test]
    fn write_permission_scope() {
        let realm_id = RealmId::from_bytes([74; 32]);
        let group_id = Ulid::from_parts(1_700, 1);
        let user_id = UserId::local(Ulid::from_parts(1_701, 1), realm_id);
        let role = Role {
            role_id: Ulid::from_parts(1_702, 1),
            name: "group-admin".to_string(),
            permissions: HashMap::from([(
                format!("/{realm_id}/g/{group_id}/*"),
                Permission::WRITE,
            )]),
            assigned_users: HashSet::from([user_id]),
        };

        assert!(has_current_write_permission(
            user_id,
            &format!("/{realm_id}/g/{group_id}/admin"),
            [&role],
        ));
        assert!(!has_current_write_permission(
            user_id,
            &format!("/{realm_id}/g/{group_id}/admin/config"),
            [&role],
        ));
    }

    #[test]
    fn revocation_expiry_bound() {
        // The shared admission window bounds replicated reducer retention.
        let now = 1_000;
        let bound = now + MAX_BEARER_TOKEN_LIFETIME_SECS + REVOCATION_GRACE_SECS;

        assert!(valid_revocation_expiry(bound, now));
        assert!(!valid_revocation_expiry(bound + 1, now));
        assert!(!valid_revocation_expiry(u64::MAX, now));
    }

    #[test]
    fn index_skip_expiry() {
        let realm_id = RealmId::from_bytes([76; 32]);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let actor = test_actor(
            45,
            UserId::local(Ulid::from_parts(1_810, 1), realm_id),
            realm_id,
        );
        let mut state = AdminDocumentReducerState::new(target.clone());
        state
            .apply(&test_admin_event(
                Ulid::from_parts(1_811, 1),
                target,
                &actor,
                1,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: aruna_core::auth::bearer_token_hash("scheduled"),
                    expires_at: 2_000,
                    token_owner: actor.user_id,
                },
            ))
            .expect("revocation applies");

        assert!(!needs_revocation_index(false, true, &state, 2_000));
        assert!(needs_revocation_index(
            false,
            true,
            &state,
            2_000 + REVOCATION_GRACE_SECS + 1
        ));
        assert!(needs_revocation_index(true, true, &state, 2_000));
        assert!(needs_revocation_index(false, false, &state, 2_000));
    }

    #[test]
    fn floor_stays_monotonic() {
        // A clock rollback must not resurrect compacted revocation paths.
        let realm_id = RealmId::from_bytes([75; 32]);
        let actor = test_actor(
            44,
            UserId::local(Ulid::from_parts(1_800, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let token_hash = aruna_core::auth::bearer_token_hash("floor-token");
        let mut state = AdminDocumentReducerState::new(target.clone());
        state
            .apply(&test_admin_event(
                Ulid::from_parts(1_801, 1),
                target,
                &actor,
                1,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: token_hash.clone(),
                    expires_at: 10_000,
                    token_owner: actor.user_id,
                },
            ))
            .expect("revocation applies");

        state.compact_revocations(10_000);
        state.compact_revocations(9_999);

        assert_eq!(state.revocation_floor, 10_000);
        assert!(
            state
                .materialized_revoked_tokens()
                .contains_key(&token_hash)
        );
    }

    #[tokio::test]
    async fn malformed_state_aborts() {
        // A decode failure after transaction start must release the snapshot.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([76; 32]);
        let actor = test_actor(
            45,
            UserId::local(Ulid::from_parts(1_810, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        storage_batch_write_to(
            &storage,
            vec![(
                ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                admin_document_reducer_state_key(&target),
                vec![0xff].into(),
            )],
        )
        .await
        .expect("malformed state writes");

        assert!(
            apply_admin_document_operation_to_storage(
                &storage,
                document_target.clone(),
                test_admin_event(
                    Ulid::from_parts(1_811, 1),
                    target.clone(),
                    &actor,
                    1,
                    AdminDocumentOperation::RealmConfigSettingsSet {
                        metadata_replication: MetadataReplicationConfig::new(3),
                        discovery: test_discovery(27, "https://abort.example:443"),
                    },
                ),
            )
            .await
            .is_err()
        );
        storage_batch_delete_to(
            &storage,
            vec![(
                ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                admin_document_reducer_state_key(&target),
            )],
        )
        .await
        .expect("malformed state deletes");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target,
            test_admin_event(
                Ulid::from_parts(1_812, 1),
                target,
                &actor,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: MetadataReplicationConfig::new(3),
                    discovery: test_discovery(27, "https://abort.example:443"),
                },
            ),
        )
        .await
        .expect("valid state applies after abort");
    }

    #[tokio::test]
    async fn admits_known_peers() {
        // An inbound sync stream is refused before any read unless the pusher
        // is a configured realm peer.
        let root = TempDir::new().expect("tempdir");
        let service = open_restart_service(root.path(), "storage").await;
        let stranger = iroh::SecretKey::from_bytes(&[41u8; 32]).public();

        assert!(service.admit_inbound(stranger).is_err());
        service
            .add_potential_peer_node(stranger)
            .expect("peer added");
        let permit = service.admit_inbound(stranger);
        assert!(permit.is_ok());
    }

    #[test]
    fn budget_caps_bytes() {
        // Per-peer and global byte ceilings hold, and release restores both.
        let budget = Arc::new(InboundSyncBudget::default());
        assert!(budget.reserve_bytes(peer(1), DOCUMENT_SYNC_INBOUND_PEER_BYTES));
        assert!(!budget.reserve_bytes(peer(1), 1));
        budget.release_bytes(peer(1), DOCUMENT_SYNC_INBOUND_PEER_BYTES);
        assert!(budget.reserve_bytes(peer(1), 1));
        budget.release_bytes(peer(1), 1);

        let mut reserved = 0usize;
        for seed in 10..u8::MAX {
            if budget.reserve_bytes(peer(seed), DOCUMENT_SYNC_INBOUND_PEER_BYTES) {
                reserved = reserved.saturating_add(DOCUMENT_SYNC_INBOUND_PEER_BYTES);
            } else {
                break;
            }
        }
        assert!(reserved <= DOCUMENT_SYNC_INBOUND_GLOBAL_BYTES);
        assert!(!budget.reserve_bytes(peer(9), DOCUMENT_SYNC_INBOUND_PEER_BYTES));
    }

    #[test]
    fn drop_releases_reservation() {
        // A stream's reservation is returned to the budget when it drops.
        let budget = Arc::new(InboundSyncBudget::default());
        {
            let mut reservation = InboundByteReservation::new(budget.clone(), peer(1));
            reservation
                .reserve(DOCUMENT_SYNC_INBOUND_PEER_BYTES)
                .expect("first reservation fits");
            assert!(reservation.reserve(1).is_err());
        }
        assert!(budget.reserve_bytes(peer(1), DOCUMENT_SYNC_INBOUND_PEER_BYTES));
    }

    #[tokio::test]
    async fn removed_peer_denied() {
        // A startup peer admits during bootstrap, then loses admission once
        // realm config materializes without it, with no restart.
        let root = TempDir::new().expect("tempdir");
        let startup = node(51);
        let current = node(52);
        let service = DocumentSyncService::open_with_persist_policy(
            restart_endpoint().await,
            storage_at(&root.path().join("storage")),
            root.path().join("document-sync"),
            &[startup],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            restart_realm(),
        )
        .expect("service opens");

        assert!(service.admit_inbound(startup).is_ok());

        service
            .refresh_potential_peer_nodes([current])
            .expect("refresh applies");

        assert!(matches!(
            service.admit_inbound(startup),
            Err(NetError::AdmissionRejected(_))
        ));
        assert!(service.admit_inbound(current).is_ok());
    }

    fn topic(seed: u8) -> irokle_crate::TopicId {
        DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([seed; 32]),
        }
        .sync_topic_id(RealmId::from_bytes([seed; 32]), &PlacementRef::NIL)
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

    fn restart_realm() -> RealmId {
        RealmId::from_bytes([99; 32])
    }

    fn restart_placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_parts(99, 7),
            shard: 11,
        }
    }

    fn restart_topic() -> irokle_crate::TopicId {
        restart_target().sync_topic_id(restart_realm(), &restart_placement())
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
            placement: restart_placement(),
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
            restart_realm(),
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
            placement: PlacementRef::NIL,
            holder_node_ids: vec![node(1)],
            created_at_ms: 1,
            updated_at_ms,
            establishing_event_id: last_event_id,
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

    fn admin_test_placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_parts(9_990, 1),
            shard: 0,
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
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
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
            shard_count: 64,
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

        let now = unix_timestamp_secs();
        let index = state.revocation_index(now);
        overlay_realm_config_reducer_materialization(&mut config, &state, now, Some(&index));
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

        let index = state.revocation_index(now);
        overlay_realm_config_reducer_materialization(&mut config, &state, now, Some(&index));
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
            shard_count: 64,
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
    async fn realm_policies_replicate() {
        // S2: a policy event replicated from another node must pass the
        // realm-config storage-apply whitelist and materialize here.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([57; 32]);
        let actor = test_actor(
            9,
            UserId::local(Ulid::from_parts(1_610, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_611, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: MetadataReplicationConfig::new(3),
                    discovery: test_discovery(24, "https://policies.example:443"),
                },
            ),
        )
        .await
        .expect("settings bootstrap the config doc");

        let policies = vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::from_bytes([2; 16]),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'write'".to_string(),
            enabled: true,
        }];
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_612, 1),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::RealmConfigPoliciesSet {
                    policies: policies.clone(),
                },
            ),
        )
        .await
        .expect("policy event replicates and applies");

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert_eq!(config.request_policies, policies);
    }

    #[tokio::test]
    async fn replicated_revocation_applies() {
        // A revocation replicated from another node must pass the realm-config
        // storage-apply whitelist and deny the token on this node.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([59; 32]);
        let actor = test_actor(
            11,
            UserId::local(Ulid::from_parts(1_630, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_631, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: MetadataReplicationConfig::new(3),
                    discovery: test_discovery(25, "https://revocation.example:443"),
                },
            ),
        )
        .await
        .expect("settings bootstrap the config doc");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_631, 2),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: actor.node_id,
                    kind: RealmNodeKind::Server,
                },
            ),
        )
        .await
        .expect("realm node bootstraps revocation authority");

        let token_hash = aruna_core::auth::bearer_token_hash("replicated-token");
        let expires_at = unix_timestamp_secs() + 600;
        for (index, seq) in [(1_632u64, 3u64), (1_633, 4)] {
            apply_admin_document_operation_to_storage(
                &storage,
                document_target.clone(),
                test_admin_event(
                    Ulid::from_parts(index, 1),
                    target.clone(),
                    &actor,
                    seq,
                    AdminDocumentOperation::RealmConfigTokenRevoked {
                        token_hash: token_hash.clone(),
                        expires_at,
                        token_owner: actor.user_id,
                    },
                ),
            )
            .await
            .expect("revocation replicates and applies");
        }

        let config = read_realm_config_doc(&storage, realm_id).await;
        assert!(config.token_revoked(&token_hash, unix_timestamp_secs()));
        assert_eq!(config.revoked_tokens.len(), 1);
    }

    #[tokio::test]
    async fn accepts_user_origin() {
        // Onboarded node kind changes must not make event arrival order diverge.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([61; 32]);
        let attacker = test_actor(
            13,
            UserId::local(Ulid::from_parts(1_650, 1), realm_id),
            realm_id,
        );
        let token_owner = UserId::local(Ulid::from_parts(1_651, 1), realm_id);
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(attacker.node_id, RealmNodeKind::Server);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config
                    .to_bytes(&attacker)
                    .expect("config serializes")
                    .into(),
            )],
        )
        .await
        .expect("config writes");

        let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let event = test_admin_event(
            Ulid::from_parts(1_652, 1),
            admin_target,
            &attacker,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash("observed-token"),
                expires_at: unix_timestamp_secs() + 600,
                token_owner,
            },
        );
        let publisher = irokle_crate::actor_id_for(topic, node_id_to_peer_id(&attacker.node_id));

        assert_eq!(
            validate_replicated_admin_event(
                &storage,
                topic,
                publisher,
                &config_target,
                &event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("validation runs"),
            AdminEventValidation::Accepted
        );

        config.nodes.clear();
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config
                    .to_bytes(&attacker)
                    .expect("unonboarded config serializes")
                    .into(),
            )],
        )
        .await
        .expect("unonboarded config writes");
        assert!(matches!(
            validate_replicated_admin_event(
                &storage,
                topic,
                publisher,
                &config_target,
                &event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("unonboarded origin validation runs"),
            AdminEventValidation::Deferred {
                dependency: Some(DocumentSyncDependency::RealmConfig(id)),
                ..
            } if id == realm_id
        ));

        config.ensure_node(attacker.node_id, RealmNodeKind::User);
        let long_event = test_admin_event(
            Ulid::from_parts(1_654, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &attacker,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash("long-token"),
                expires_at: unix_timestamp_secs()
                    + MAX_BEARER_TOKEN_LIFETIME_SECS
                    + REVOCATION_GRACE_SECS
                    + 1,
                token_owner: attacker.user_id,
            },
        );
        assert!(matches!(
            validate_replicated_admin_event(
                &storage,
                topic,
                publisher,
                &config_target,
                &long_event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("long expiry validation runs"),
            AdminEventValidation::Rejected(reason)
                if reason == "revoked bearer token expiry exceeds the admission window"
        ));

        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config
                    .to_bytes(&attacker)
                    .expect("config serializes")
                    .into(),
            )],
        )
        .await
        .expect("updated config writes");
        let user_event = test_admin_event(
            Ulid::from_parts(1_653, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &attacker,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash("owned-token"),
                expires_at: unix_timestamp_secs() + 600,
                token_owner: attacker.user_id,
            },
        );
        assert_eq!(
            validate_replicated_admin_event(
                &storage,
                topic,
                publisher,
                &config_target,
                &user_event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("onboarded user origin validation runs"),
            AdminEventValidation::Accepted
        );
        apply_admin_document_operation_to_storage(&storage, config_target.clone(), user_event)
            .await
            .expect("onboarded user revocation applies");
        let config = read_realm_config_doc(&storage, realm_id).await;
        assert!(config.token_revoked(
            &aruna_core::auth::bearer_token_hash("owned-token"),
            unix_timestamp_secs()
        ));
    }

    #[tokio::test]
    async fn caps_flooding_origin() {
        // Past its per-origin bound the flooding node is rejected, while another
        // origin's revocation still applies instead of being trimmed away.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([65; 32]);
        let flooder = test_actor(
            22,
            UserId::local(Ulid::from_parts(1_670, 1), realm_id),
            realm_id,
        );
        let neighbour = test_actor(
            23,
            UserId::local(Ulid::from_parts(1_671, 1), realm_id),
            realm_id,
        );
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(flooder.node_id, RealmNodeKind::Server);
        config.ensure_node(neighbour.node_id, RealmNodeKind::Server);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config.to_bytes(&flooder).expect("config serializes").into(),
            )],
        )
        .await
        .expect("config writes");

        let expires_at = unix_timestamp_secs() + 600;
        let mut state = AdminDocumentReducerState::new(admin_target.clone());
        let mut index = state.revocation_index(expires_at);
        for seed in 0..MAX_LIVE_REVOCATIONS_PER_ORIGIN {
            state
                .apply_revocation_operation(
                    &flooder,
                    AdminDocumentOperation::RealmConfigTokenRevoked {
                        token_hash: aruna_core::auth::bearer_token_hash(&format!("flood-{seed}")),
                        expires_at,
                        token_owner: flooder.user_id,
                    },
                    &mut index,
                )
                .expect("seeded revocation applies");
        }
        storage_batch_write_to(
            &storage,
            vec![admin_document_reducer_state_write_entry(&state).expect("state serializes")],
        )
        .await
        .expect("reducer state writes");

        let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let flood_event = test_admin_event(
            Ulid::from_parts(1_672, 1),
            admin_target.clone(),
            &flooder,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash("flood-extra"),
                expires_at,
                token_owner: flooder.user_id,
            },
        );
        assert!(matches!(
            validate_replicated_admin_event(
                &storage,
                topic,
                irokle_crate::actor_id_for(topic, node_id_to_peer_id(&flooder.node_id)),
                &config_target,
                &flood_event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("flood validation runs"),
            AdminEventValidation::Rejected(reason)
                if reason == "revocation origin reached its live revocation cap"
        ));

        let neighbour_event = test_admin_event(
            Ulid::from_parts(1_673, 1),
            admin_target,
            &neighbour,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash("neighbour-token"),
                expires_at,
                token_owner: neighbour.user_id,
            },
        );
        assert_eq!(
            validate_replicated_admin_event(
                &storage,
                topic,
                irokle_crate::actor_id_for(topic, node_id_to_peer_id(&neighbour.node_id)),
                &config_target,
                &neighbour_event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("neighbour validation runs"),
            AdminEventValidation::Accepted
        );
    }

    #[test]
    fn accepts_historical_origin() {
        let realm_id = RealmId::from_bytes([62; 32]);
        let origin = test_actor(
            14,
            UserId::local(Ulid::from_parts(1_660, 1), realm_id),
            realm_id,
        );
        let other = test_actor(
            15,
            UserId::local(Ulid::from_parts(1_661, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let ensure = test_admin_event(
            Ulid::from_parts(1_662, 1),
            target.clone(),
            &origin,
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: origin.node_id,
                kind: RealmNodeKind::Server,
            },
        );
        let conflict = test_admin_event(
            Ulid::from_parts(1_663, 1),
            target.clone(),
            &other,
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: origin.node_id,
                kind: RealmNodeKind::User,
            },
        );
        let mut state = AdminDocumentReducerState::new(target.clone());
        state.apply(&ensure).expect("onboarding applies");
        state
            .apply(&conflict)
            .expect("conflicting onboarding applies");
        assert!(
            state
                .conflicts
                .contains_key(&realm_config_node_path(&origin.node_id))
        );

        let event = AdminDocumentEvent {
            event_id: Ulid::from_parts(1_664, 1),
            target,
            origin_node_id: origin.node_id,
            origin_seq: 2,
            observed: AdminDocumentClock::default().with_observed(origin.node_id, 1),
            actor: origin,
            op: AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash("historical-token"),
                expires_at: unix_timestamp_secs() + 600,
                token_owner: other.user_id,
            },
        };
        assert!(revocation_origin_known(
            None,
            Some(&state),
            &event,
            realm_id
        ));
    }

    #[tokio::test]
    async fn replicated_revocation_compacts() {
        // A replicated revocation whose token has expired must leave no entry
        // behind in the receiving node's reducer state.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([60; 32]);
        let actor = test_actor(
            12,
            UserId::local(Ulid::from_parts(1_640, 1), realm_id),
            realm_id,
        );
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_641, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: MetadataReplicationConfig::new(3),
                    discovery: test_discovery(26, "https://compaction.example:443"),
                },
            ),
        )
        .await
        .expect("settings bootstrap the config doc");

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_641, 2),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: actor.node_id,
                    kind: RealmNodeKind::Server,
                },
            ),
        )
        .await
        .expect("realm node bootstraps revocation authority");

        let expired = aruna_core::auth::bearer_token_hash("expired-token");
        let live = aruna_core::auth::bearer_token_hash("live-token");
        let now = unix_timestamp_secs();
        for (index, seq, token_hash, expires_at) in [
            (1_642u64, 3u64, expired.clone(), now - 1),
            (1_643, 4, live.clone(), now + 600),
        ] {
            apply_admin_document_operation_to_storage(
                &storage,
                document_target.clone(),
                test_admin_event(
                    Ulid::from_parts(index, 1),
                    target.clone(),
                    &actor,
                    seq,
                    AdminDocumentOperation::RealmConfigTokenRevoked {
                        token_hash,
                        expires_at,
                        token_owner: actor.user_id,
                    },
                ),
            )
            .await
            .expect("revocation replicates and applies");
        }

        let mut state = read_admin_reducer_state(&storage, &target)
            .await
            .expect("reducer state reads")
            .expect("reducer state exists");
        assert!(state.materialized_revoked_tokens().contains_key(&live));
        assert!(
            state
                .user_subject_ids
                .keys()
                .any(|path| path.contains(&expired))
        );
        let config = read_realm_config_doc(&storage, realm_id).await;
        assert!(!config.token_revoked(&expired, now));
        assert_eq!(config.revoked_tokens.len(), 1);

        let future = now + REVOCATION_GRACE_SECS + 1;
        state.compact_revocations(future);
        assert!(
            !state
                .user_subject_ids
                .keys()
                .any(|path| path.contains(&expired))
        );
        let mut future_config = config;
        let index = state.revocation_index(future);
        overlay_realm_config_reducer_materialization(
            &mut future_config,
            &state,
            future,
            Some(&index),
        );
        assert_eq!(future_config.revoked_tokens.len(), 1);
    }

    #[tokio::test]
    async fn redundant_persists_clock() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([64; 32]);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let document_target = DocumentSyncTarget::RealmConfig { realm_id };
        let assert_stable = |before: &[u8], after: &[u8]| {
            let mut before = RealmConfigDocument::from_bytes(before).expect("decode prior config");
            let after = RealmConfigDocument::from_bytes(after).expect("decode current config");
            assert!(after.revocation_floor >= before.revocation_floor);
            before.revocation_floor = after.revocation_floor;
            assert_eq!(before, after);
        };
        let actor_a = test_actor(
            18,
            UserId::local(Ulid::from_parts(1_690, 1), realm_id),
            realm_id,
        );
        let actor_b = test_actor(
            19,
            UserId::local(Ulid::from_parts(1_691, 1), realm_id),
            realm_id,
        );
        let token_hash = aruna_core::auth::bearer_token_hash("redundant-token");
        let expires_at = unix_timestamp_secs() + 600;
        let mut seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        seed_config.ensure_node(actor_a.node_id, RealmNodeKind::Server);
        seed_config.ensure_node(actor_b.node_id, RealmNodeKind::Server);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                document_target.clone(),
                seed_config
                    .to_bytes(&actor_a)
                    .expect("seed config serializes")
                    .into(),
            )],
        )
        .await
        .expect("seed config writes");
        let first = test_admin_event(
            Ulid::from_parts(1_692, 1),
            target.clone(),
            &actor_a,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: token_hash.clone(),
                expires_at,
                token_owner: actor_a.user_id,
            },
        );
        apply_admin_document_operation_to_storage(&storage, document_target.clone(), first.clone())
            .await
            .expect("first revocation applies");
        let config_key = document_target.storage_key();
        let before = read_storage_value(
            &storage,
            document_target.storage_keyspace(),
            config_key.clone(),
        )
        .await
        .expect("config exists after first revocation");
        apply_admin_document_operation_to_storage(&storage, document_target.clone(), first)
            .await
            .expect("duplicate revocation applies");
        let duplicate = read_storage_value(
            &storage,
            document_target.storage_keyspace(),
            document_target.storage_key(),
        )
        .await
        .expect("config exists after duplicate revocation");
        assert_stable(&before, &duplicate);

        let second = test_admin_event(
            Ulid::from_parts(1_691, 1),
            target.clone(),
            &actor_b,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: token_hash.clone(),
                expires_at,
                token_owner: actor_b.user_id,
            },
        );
        apply_admin_document_operation_to_storage(&storage, document_target.clone(), second)
            .await
            .expect("redundant revocation applies");

        let after = read_storage_value(&storage, document_target.storage_keyspace(), config_key)
            .await
            .expect("config remains after redundant revocation");
        assert_stable(&before, &after);
        let state = read_admin_reducer_state(&storage, &target)
            .await
            .expect("reducer state reads")
            .expect("reducer state exists");
        assert_eq!(state.clock.sequence_for(&actor_a.node_id), 1);
        assert_eq!(state.clock.sequence_for(&actor_b.node_id), 1);
        assert_eq!(state.applied_event_ids.len(), 1);
        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(token_hash, expires_at)])
        );
    }

    #[tokio::test]
    async fn group_policies_replicate() {
        // GroupPoliciesSet must pass the group storage-apply whitelist and land
        // on the receiving node's group authorization document.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([58; 32]);
        let group_id = Ulid::from_parts(1_620, 1);
        let owner = UserId::local(Ulid::from_parts(1_621, 1), realm_id);
        let actor = test_actor(10, owner, realm_id);
        let target = AdminDocumentTarget::Group { group_id };
        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_622, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name: "Engineering".to_string(),
                    owner,
                },
            ),
        )
        .await
        .expect("group creation bootstraps the auth doc");

        let policies = vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::from_bytes([3; 16]),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'write'".to_string(),
            enabled: true,
        }];
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_623, 1),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::GroupPoliciesSet {
                    policies: policies.clone(),
                },
            ),
        )
        .await
        .expect("group policy event replicates and applies");

        let auth_doc = read_group_auth_doc(&storage, group_id).await;
        assert_eq!(auth_doc.policies, policies);
    }

    #[tokio::test]
    async fn policies_authority_gate() {
        // GroupPoliciesSet must reach the config-path check, not the unreachable
        // arm: a non-owner config admin is accepted, one without config write is
        // rejected, and neither path panics.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([59; 32]);
        let group_id = Ulid::from_parts(1_630, 1);
        let owner = UserId::local(Ulid::from_parts(1_631, 1), realm_id);
        let admin_user = UserId::local(Ulid::from_parts(1_632, 1), realm_id);
        let role_id = Ulid::from_parts(1_633, 1);
        let actor = test_actor(11, admin_user, realm_id);

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Management);
        let group = Group {
            display_name: "Engineering".to_string(),
            group_id,
            realm_id,
            owner,
            roles: HashSet::from([role_id]),
        };
        let realm_auth = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::new(),
            operation_restrictions: Default::default(),
        };
        storage_batch_write_to(
            &storage,
            vec![
                target_write_entry(
                    DocumentSyncTarget::RealmConfig { realm_id },
                    config.to_bytes(&actor).expect("config serializes").into(),
                ),
                target_write_entry(
                    DocumentSyncTarget::RealmAuthorization { realm_id },
                    realm_auth
                        .to_bytes(&actor)
                        .expect("realm auth serializes")
                        .into(),
                ),
                (
                    GROUP_KEYSPACE.to_string(),
                    group_id.to_bytes().into(),
                    group.to_bytes(&actor).expect("group serializes").into(),
                ),
            ],
        )
        .await
        .expect("realm and group state writes");

        let config_path = format!("/{realm_id}/g/{group_id}/admin/config");
        let policies = vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::from_bytes([7; 16]),
            name: "deny-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'write'".to_string(),
            enabled: true,
        }];
        let event = test_admin_event(
            Ulid::from_parts(1_634, 1),
            AdminDocumentTarget::Group { group_id },
            &actor,
            1,
            AdminDocumentOperation::GroupPoliciesSet { policies },
        );

        for (permission, expect_accept) in [(Permission::WRITE, true), (Permission::READ, false)] {
            let auth_doc = GroupAuthorizationDocument {
                group_id,
                policies: Vec::new(),
                roles: HashMap::from([(
                    role_id,
                    Role {
                        role_id,
                        name: "config_admin".to_string(),
                        permissions: HashMap::from([(config_path.clone(), permission)]),
                        assigned_users: HashSet::from([admin_user]),
                    },
                )]),
            };
            storage_batch_write_to(
                &storage,
                vec![target_write_entry(
                    DocumentSyncTarget::GroupAuthorization { group_id },
                    auth_doc
                        .to_bytes(&actor)
                        .expect("auth doc serializes")
                        .into(),
                )],
            )
            .await
            .expect("group auth doc writes");

            let validation = validate_group_admin_authority(&storage, &event)
                .await
                .expect("validation runs without panic");
            assert_eq!(
                matches!(validation, AdminEventValidation::Accepted),
                expect_accept
            );
        }
    }

    #[tokio::test]
    async fn quota_survives_materialization() {
        // Quota remains when the reducer materializes without a stored config.
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
    async fn retries_config_conflict() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([52; 32]);
        let actor_a = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_410, 1), realm_id),
            realm_id,
        );
        let actor_b = test_actor(
            9,
            UserId::local(Ulid::from_parts(1_411, 1), realm_id),
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

        let first = test_admin_event(
            Ulid::from_parts(1_412, 1),
            target.clone(),
            &actor_a,
            1,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "first".to_string(),
            },
        );
        let second = test_admin_event(
            Ulid::from_parts(1_413, 1),
            target.clone(),
            &actor_b,
            1,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "second".to_string(),
            },
        );
        let (first_result, second_result) = tokio::join!(
            apply_admin_document_operation_to_storage(&storage, document_target.clone(), first),
            apply_admin_document_operation_to_storage(&storage, document_target.clone(), second),
        );
        first_result.expect("first concurrent config operation applies");
        second_result.expect("second concurrent config operation retries");

        let config = read_realm_config_doc(&storage, realm_id).await;
        let state = read_admin_reducer_state(&storage, &target)
            .await
            .expect("reducer state reads")
            .expect("reducer state exists");
        assert!(matches!(config.description.as_str(), "first" | "second"));
        let path = REALM_CONFIG_DESCRIPTION_PATH;
        assert!(state.conflicts.contains_key(path));
        assert_eq!(
            state
                .conflicts
                .get(path)
                .expect("description conflict exists")
                .values
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn keeps_stale_config() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([53; 32]);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(1_420, 1), realm_id),
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

        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_421, 1),
                target.clone(),
                &actor,
                2,
                AdminDocumentOperation::RealmConfigDescriptionSet {
                    description: "new".to_string(),
                },
            ),
        )
        .await
        .expect("new config operation applies");
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_422, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigDescriptionSet {
                    description: "stale".to_string(),
                },
            ),
        )
        .await
        .expect("stale config operation persists clock");

        assert_eq!(
            read_realm_config_doc(&storage, realm_id).await.description,
            "new"
        );
        let state = read_admin_reducer_state(&storage, &target)
            .await
            .expect("reducer state reads")
            .expect("reducer state exists");
        assert_eq!(state.clock.sequence_for(&actor.node_id), 2);
        assert_eq!(
            state.materialized_realm_config_description().as_deref(),
            Some("new")
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
    async fn replicated_role_confined() {
        // An allowed publisher cannot replicate a role granting outside its group.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([71; 32]);
        let group_id = Ulid::from_parts(210, 1);
        let role_id = Ulid::from_parts(211, 1);
        let actor = test_actor(
            8,
            UserId::local(Ulid::from_parts(212, 1), realm_id),
            realm_id,
        );
        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };
        let placement = admin_test_placement();
        let topic_id = document_target.sync_topic_id(realm_id, &placement);
        let actor_id = irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&actor.node_id));

        let event = test_admin_event(
            Ulid::from_parts(213, 1),
            AdminDocumentTarget::Group { group_id },
            &actor,
            1,
            AdminDocumentOperation::GroupRoleCreated {
                role: test_admin_role_definition(role_id, "escalated", "/**", Permission::WRITE),
            },
        );

        let outcome = validate_replicated_admin_event(
            &storage,
            topic_id,
            actor_id,
            &document_target,
            &event,
            realm_id,
            &placement,
        )
        .await
        .expect("validation runs");
        assert!(matches!(outcome, AdminEventValidation::Rejected(_)));
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
            policies: Vec::new(),
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
                Ulid::generate(),
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
            Ulid::generate(),
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
            policies: Vec::new(),
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
            policies: Vec::new(),
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
        summary_for(topic(9), event_type_id, heads)
    }

    fn summary_for(
        topic_id: irokle_crate::TopicId,
        event_type_id: Option<String>,
        heads: BTreeSet<irokle_crate::OpId>,
    ) -> irokle_crate::sync::SyncSummary {
        irokle_crate::sync::SyncSummary {
            topic_id,
            event_type_id,
            fingerprint: [0; 32],
            heads,
            actor_clock: irokle_crate::ActorClock::default(),
            actor_tips: BTreeMap::new(),
        }
    }

    #[test]
    fn classify_probe_buckets_empty_summary_as_confirmed_unknown() {
        let wanted = BTreeSet::from([topic(1), topic(2), topic(3)]);
        let responses = vec![
            // Empty summary: positive confirmation the peer has no genesis.
            SyncMessage::Summary(summary_for(topic(1), None, BTreeSet::new())),
            // Typed summary: the peer holds a genesis.
            SyncMessage::Summary(summary_for(
                topic(2),
                Some(DocumentSyncEvent::TYPE_ID.to_string()),
                BTreeSet::new(),
            )),
            // topic(3) omitted entirely: refused (held, prober not a member).
        ];
        let probe = classify_probe_responses(&wanted, responses);
        assert_eq!(probe.confirmed_unknown, BTreeSet::from([topic(1)]));
        assert_eq!(probe.known, BTreeSet::from([topic(2)]));
        assert!(!probe.confirmed_unknown.contains(&topic(3)));
        assert!(!probe.known.contains(&topic(3)));
    }

    #[test]
    fn classify_probe_ignores_summaries_for_unwanted_topics() {
        let wanted = BTreeSet::from([topic(1)]);
        let responses = vec![SyncMessage::Summary(summary_for(
            topic(5),
            None,
            BTreeSet::new(),
        ))];
        assert_eq!(
            classify_probe_responses(&wanted, responses),
            PeerTopicProbe::default()
        );
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
            // Shard topics are join-only at publish time; this node plays the
            // shard's rank-0 holder and creates the genesis eagerly first.
            service
                .ensure_document_sync_topics(&[restart_topic()], Vec::new())
                .expect("restart shard topic genesis");
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
            .open_topic::<DocumentSyncEvent>(restart_topic())
            .expect("published topic reopens after restart");
        let history = topic
            .history(irokle_crate::history::HistoryOrder::OldestFirst)
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
        let selection = PeerSelection {
            peers: BTreeSet::from([ok_peer, failed_peer]),
            truncated: false,
            round: 0,
        };

        let error = DocumentSyncService::fan_out_peer_syncs(
            selection,
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
    async fn registry_replay_repairs() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([42; 32]);
        let group_id = Ulid::from_parts(2_110, 1);
        let actor = test_actor(1, UserId::nil(realm_id), realm_id);
        let strategy_id = Ulid::from_parts(2_113, 1);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.default_strategy_id = Some(strategy_id);
        config.strategies.push(PlacementStrategy {
            strategy_id,
            name: "test".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        });
        config.placement_bindings.push(PlacementBinding {
            handle: PlacementHandle::new(METADATA_HANDLE).unwrap(),
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        });
        let placement = PlacementRef {
            strategy_id,
            shard: 4,
        };
        let document_id = MetaResourceId::from_parts(
            2_111,
            PlacementHandle::new(METADATA_HANDLE).unwrap(),
            BucketId::new(placement.shard as u16).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let mut record = registry_record(
            group_id,
            document_id,
            "datasets/replay-repair",
            100,
            Ulid::from_parts(2_112, 1),
        );
        record.placement = placement;
        storage_batch_write_to(
            &storage,
            vec![(
                DocumentSyncTarget::RealmConfig { realm_id }
                    .storage_keyspace()
                    .to_string(),
                DocumentSyncTarget::RealmConfig { realm_id }.storage_key(),
                config
                    .to_bytes(&actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");
        let mut entries = metadata_registry_write_entries(&record).expect("registry entries build");
        let primary = entries.remove(0);
        storage_batch_write_to(&storage, vec![primary])
            .await
            .expect("registry primary writes");

        apply_metadata_registry_upsert_to_storage(
            &storage,
            record.clone(),
            postcard::to_allocvec(&record).expect("registry serializes"),
        )
        .await
        .expect("equal registry replay repairs sidecars");

        assert_registry_record_present(&storage, &record).await;
    }

    #[tokio::test]
    async fn registry_fence_ulid() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(2_120, 1);
        let document_id = Ulid::from_parts(2_121, 1);
        let boundary = Ulid::from_parts(2_122, 1);
        let delete = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(2_123, 1),
            boundary,
        );
        write_document_lifecycle_record(&storage, &delete).await;
        let record = registry_record(
            group_id,
            document_id,
            "datasets/post-delete",
            100,
            Ulid::from_parts(2_124, 1),
        );
        let txn_id = start_storage_transaction(&storage)
            .await
            .expect("fence transaction starts");
        assert!(
            record_fenced_txn(&storage, &record, txn_id)
                .await
                .expect("newer ULID fence checks")
        );
        match storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => {}
            other => panic!("unexpected fence transaction result: {other:?}"),
        }

        let live = registry_record(group_id, document_id, "datasets/post-delete", 300, boundary);
        write_registry_record(&storage, &live).await;
        let stale = registry_record(
            group_id,
            document_id,
            "datasets/post-delete",
            100,
            Ulid::from_parts(2_124, 1),
        );
        let txn_id = start_storage_transaction(&storage)
            .await
            .expect("stale fence transaction starts");
        assert!(
            !record_fenced_txn(&storage, &stale, txn_id)
                .await
                .expect("stale registry fence checks live primary")
        );
        match storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => {}
            other => panic!("unexpected stale fence transaction result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn registry_strategy_fenced() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(2_100, 1);
        let document_id = Ulid::from_parts(2_101, 1);
        let mut record = registry_record(
            group_id,
            document_id,
            "datasets/missing-strategy",
            100,
            Ulid::from_parts(2_102, 1),
        );
        record.placement = PlacementRef {
            strategy_id: Ulid::from_parts(2_103, 1),
            shard: 1,
        };
        let mut config = RealmConfigDocument::default_for_realm(record.realm_id, Vec::new());
        config.seed_default_placement();
        let config_target = DocumentSyncTarget::RealmConfig {
            realm_id: record.realm_id,
        };
        storage_batch_write_to(
            &storage,
            vec![(
                config_target.storage_keyspace().to_string(),
                config_target.storage_key(),
                config
                    .to_bytes(&test_actor(
                        1,
                        UserId::nil(record.realm_id),
                        record.realm_id,
                    ))
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");

        apply_metadata_registry_upsert_to_storage(
            &storage,
            record.clone(),
            postcard::to_allocvec(&record).expect("registry serializes"),
        )
        .await
        .expect("invalid strategy is rejected without wedging reconciliation");

        assert!(
            read_storage_value(
                &storage,
                METADATA_INDEX_KEYSPACE,
                metadata_registry_key(group_id, document_id),
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn upsert_keeps_config() {
        // The placement fence reads the realm config inside its transaction and
        // must not write it back: an identical write only conflicts the config's
        // readers, which is how inbound sync used to stall concurrent creates.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([42; 32]);
        let group_id = Ulid::from_parts(2_130, 1);
        let actor = test_actor(1, UserId::nil(realm_id), realm_id);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        let placement = PlacementRef {
            strategy_id: config.default_strategy_id.unwrap(),
            shard: 4,
        };
        let document_id = MetaResourceId::from_parts(
            2_131,
            PlacementHandle::new(METADATA_HANDLE).unwrap(),
            BucketId::new(placement.shard as u16).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let mut record = registry_record(
            group_id,
            document_id,
            "datasets/config-untouched",
            100,
            Ulid::from_parts(2_132, 1),
        );
        record.placement = placement;
        let config_target = DocumentSyncTarget::RealmConfig {
            realm_id: record.realm_id,
        };
        storage_batch_write_to(
            &storage,
            vec![(
                config_target.storage_keyspace().to_string(),
                config_target.storage_key(),
                config
                    .to_bytes(&actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");
        let before = storage.snapshot_metrics().requests_total;

        apply_metadata_registry_upsert_to_storage(
            &storage,
            record.clone(),
            postcard::to_allocvec(&record).expect("registry serializes"),
        )
        .await
        .expect("registry upsert applies");

        assert!(
            read_storage_value(
                &storage,
                METADATA_INDEX_KEYSPACE,
                metadata_registry_key(group_id, document_id),
            )
            .await
            .is_some(),
            "the upsert still lands"
        );
        assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
        assert!(storage.snapshot_metrics().requests_total > before);
    }

    #[tokio::test]
    async fn capacity_retains_cursors() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([42; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(77).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");
        let actor = test_actor(
            77,
            UserId::local(Ulid::from_parts(2_120, 1), realm_id),
            realm_id,
        );
        assert_eq!(actor.node_id, local_node);

        let strategy_id = Ulid::from_parts(2_121, 1);
        let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(local_node, RealmNodeKind::Management);
        config.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        });
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config
                    .to_bytes(&actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");

        assert!(config.strategy(&strategy_id).is_none());
        let registry_placement = PlacementRef {
            strategy_id,
            shard: 4,
        };
        let create_placement = PlacementRef {
            strategy_id,
            shard: 5,
        };
        let registry_group_id = Ulid::from_parts(2_122, 1);
        let registry_document_id = MetaResourceId::from_parts(
            2_123,
            handle,
            BucketId::new(registry_placement.shard as u16).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let registry_event_id = Ulid::from_parts(2_124, 1);
        let mut registry = registry_record(
            registry_group_id,
            registry_document_id,
            "datasets/capacity-registry",
            100,
            registry_event_id,
        );
        registry.placement = registry_placement;
        let registry_target = DocumentSyncTarget::MetadataRegistry {
            group_id: registry_group_id,
            document_id: registry_document_id,
        };

        let create_group_id = Ulid::from_parts(2_125, 1);
        let create_document_id = MetaResourceId::from_parts(
            2_126,
            handle,
            BucketId::new(create_placement.shard as u16).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let create_event_id = Ulid::from_parts(2_127, 1);
        let mut create = metadata_create_event(
            create_group_id,
            create_document_id,
            100,
            create_event_id,
            77,
        );
        create.record.placement = create_placement;
        let create_target = DocumentSyncTarget::MetadataCreateEvent {
            document_id: create_document_id,
            event_id: create_event_id,
        };
        let registry_topic = registry_target.sync_topic_id(realm_id, &registry_placement);
        let create_topic = create_target.sync_topic_id(realm_id, &create_placement);
        service
            .ensure_document_sync_topics(&[registry_topic, create_topic], Vec::new())
            .expect("metadata shard topic genesis");
        let change = |event_id, placement| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id,
                actor: local_node,
                updated_at_ms: 100,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        };
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Upsert {
                        event_id: registry_event_id,
                        target: registry_target,
                        bytes: postcard::to_allocvec(&registry).expect("registry serializes"),
                        change: change(registry_event_id, registry_placement),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: create_event_id,
                        target: create_target,
                        bytes: postcard::to_allocvec(&create).expect("create serializes"),
                        change: change(create_event_id, create_placement),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "metadata publish failed: {published:?}"
        );
        for topic_id in [registry_topic, create_topic] {
            assert_ne!(
                service
                    .node()
                    .storage()
                    .actor_clock(&topic_id)
                    .expect("metadata topic clock"),
                irokle_crate::ActorClock::default(),
                "metadata topic must contain its published event"
            );
            service
                .storage_write(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    topic_cursor_key(topic_id),
                    postcard::to_allocvec(&irokle_crate::ActorClock::default())
                        .expect("clock serializes")
                        .into(),
                )
                .await
                .expect("metadata cursor resets");
        }

        let dependency = DocumentSyncDependency::PlacementStrategy {
            realm_id,
            strategy_id,
        };
        assert!(
            !document_sync_dependency_available(&storage, dependency)
                .await
                .expect("placement dependency checks")
        );
        let mut filler_topics = BTreeSet::new();
        for index in 0..MAX_DEFERRED_TOPICS_PER_DEPENDENCY {
            let mut filler_realm = [0xA5; 32];
            filler_realm[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let filler_realm = RealmId::from_bytes(filler_realm);
            filler_topics.insert(
                DocumentSyncTarget::RealmConfig {
                    realm_id: filler_realm,
                }
                .sync_topic_id(filler_realm, &PlacementRef::NIL),
            );
        }
        assert_eq!(filler_topics.len(), MAX_DEFERRED_TOPICS_PER_DEPENDENCY);
        assert!(!filler_topics.contains(&registry_topic));
        assert!(!filler_topics.contains(&create_topic));
        let deferred_topics = BTreeMap::from([(dependency, filler_topics)]);
        let mut capacity_probe = deferred_topics.clone();
        let existing_topic = *capacity_probe
            .get(&dependency)
            .and_then(BTreeSet::first)
            .expect("full dependency has a topic");
        assert_eq!(
            register_deferred_topic(&mut capacity_probe, dependency, existing_topic),
            DeferredTopicRegistrationOutcome::AlreadyRegistered
        );
        assert_eq!(
            register_deferred_topic(&mut capacity_probe, dependency, registry_topic),
            DeferredTopicRegistrationOutcome::CapacityExceeded
        );
        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                deferred_topics_key(),
                postcard::to_allocvec(&deferred_topics)
                    .expect("deferred topics serialize")
                    .into(),
            )
            .await
            .expect("full deferred topic registry writes");

        service
            .reconcile_document_topics([registry_topic, create_topic])
            .await
            .expect("metadata reconciliation remains retryable at capacity");
        let deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>> =
            postcard::from_bytes(
                &read_storage_value(
                    &storage,
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                    deferred_topics_key(),
                )
                .await
                .expect("deferred topic registry remains stored"),
            )
            .expect("deferred topic registry decodes");
        assert_eq!(
            deferred_topics.get(&dependency).map(BTreeSet::len),
            Some(MAX_DEFERRED_TOPICS_PER_DEPENDENCY)
        );
        let registered_dependencies = deferred_topics
            .iter()
            .filter(|(_, topics)| {
                topics.contains(&registry_topic) || topics.contains(&create_topic)
            })
            .map(|(dependency, _)| *dependency)
            .collect::<Vec<_>>();
        assert!(
            registered_dependencies.is_empty(),
            "expected full {dependency:?}; topics registered under {registered_dependencies:?}"
        );
        for topic_id in [registry_topic, create_topic] {
            let cursor: irokle_crate::ActorClock = postcard::from_bytes(
                &read_storage_value(
                    &storage,
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                    topic_cursor_key(topic_id),
                )
                .await
                .expect("deferred cursor remains stored"),
            )
            .expect("cursor decodes");
            assert_eq!(
                cursor,
                irokle_crate::ActorClock::default(),
                "capacity-blocked metadata dependency changed cursor for {topic_id}"
            );
            let topic_clock = service
                .node()
                .storage()
                .actor_clock(&topic_id)
                .expect("metadata topic clock");
            assert!(
                !cursor.dominates(&topic_clock),
                "capacity-blocked metadata dependency advanced cursor for {topic_id}"
            );
        }

        service.shutdown().await;
    }

    #[tokio::test]
    async fn metadata_placement_defers() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([42; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(76).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");
        let actor = test_actor(
            76,
            UserId::local(Ulid::from_parts(2_110, 1), realm_id),
            realm_id,
        );
        assert_eq!(actor.node_id, local_node);

        let strategy_id = Ulid::from_parts(2_111, 1);
        let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(local_node, RealmNodeKind::Management);
        config.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        });
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config
                    .to_bytes(&actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");

        let strategy = PlacementStrategy {
            strategy_id,
            name: "deferred".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: 4,
        };
        let group_id = Ulid::from_parts(2_112, 1);
        let document_id = MetaResourceId::from_parts(
            2_113,
            handle,
            BucketId::new(placement.shard as u16).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let create_event_id = Ulid::from_parts(2_114, 1);
        let mut record = registry_record(
            group_id,
            document_id,
            "datasets/deferred",
            100,
            create_event_id,
        );
        record.placement = placement;
        let mut create = metadata_create_event(group_id, document_id, 100, create_event_id, 76);
        create.record = record.clone();
        let registry_target = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        };
        let create_target = DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id: create_event_id,
        };
        let update_event_id = Ulid::from_parts(2_118, 1);
        let mut update = create.clone();
        update.event_id = update_event_id;
        update.record.updated_at_ms = 200;
        update.record.last_event_id = update_event_id;
        update.payload = MetadataCreateEventPayload::ReplaceRoCrate {
            jsonld: "{}".to_string(),
        };
        update.occurred_at_ms = 200;
        let update_target = DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id: update_event_id,
        };
        let metadata_topic = registry_target.sync_topic_id(realm_id, &placement);
        service
            .ensure_document_sync_topics(&[metadata_topic], Vec::new())
            .expect("metadata shard topic genesis");
        let change = |event_id| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id,
                actor: local_node,
                updated_at_ms: 100,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        };
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::from_parts(2_115, 1),
                        target: registry_target.clone(),
                        bytes: postcard::to_allocvec(&record).expect("registry serializes"),
                        change: change(Ulid::from_parts(2_115, 1)),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::from_parts(2_117, 1),
                        target: update_target.clone(),
                        bytes: postcard::to_allocvec(&update).expect("update serializes"),
                        change: change(Ulid::from_parts(2_117, 1)),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::from_parts(2_116, 1),
                        target: create_target.clone(),
                        bytes: postcard::to_allocvec(&create).expect("create serializes"),
                        change: change(Ulid::from_parts(2_116, 1)),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "metadata publish failed: {published:?}"
        );
        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(metadata_topic),
                postcard::to_allocvec(&irokle_crate::ActorClock::default())
                    .expect("clock serializes")
                    .into(),
            )
            .await
            .expect("metadata cursor resets");

        let deferred = service
            .reconcile_document_topics([metadata_topic])
            .await
            .expect("metadata reconciliation defers");
        assert!(deferred.metadata_create_events.is_empty());
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
                METADATA_EVENT_LOG_KEYSPACE,
                metadata_event_log_key(document_id, create_event_id),
            )
            .await
            .is_none()
        );
        let deferred_cursor: irokle_crate::ActorClock = postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                topic_cursor_key(metadata_topic),
            )
            .await
            .expect("deferred cursor remains stored"),
        )
        .expect("cursor decodes");
        let metadata_clock = service
            .node()
            .storage()
            .actor_clock(&metadata_topic)
            .expect("metadata topic clock");
        assert!(!deferred_cursor.dominates(&metadata_clock));
        let deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>> =
            postcard::from_bytes(
                &read_storage_value(
                    &storage,
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                    deferred_topics_key(),
                )
                .await
                .expect("deferred topic registry is persisted"),
            )
            .expect("deferred topic registry decodes");
        assert_eq!(
            deferred_topics
                .get(&DocumentSyncDependency::PlacementStrategy {
                    realm_id,
                    strategy_id: strategy.strategy_id,
                })
                .and_then(|topics| topics.get(&metadata_topic)),
            Some(&metadata_topic)
        );

        let config_topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let strategy_event = test_admin_event(
            Ulid::from_parts(2_120, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &actor,
            1,
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: strategy.clone(),
            },
        );
        let published = service
            .publish_documents(
                vec![DocumentSyncPublish::AdminOperation {
                    target: config_target.clone(),
                    event: Box::new(strategy_event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                }],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "strategy publish failed: {published:?}"
        );
        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(config_topic),
                postcard::to_allocvec(&irokle_crate::ActorClock::default())
                    .expect("clock serializes")
                    .into(),
            )
            .await
            .expect("config cursor resets");

        let applied = service
            .reconcile_document_topics([config_topic])
            .await
            .expect("strategy reconciliation retries metadata");
        assert!(applied.targets.contains(&registry_target));
        assert!(applied.targets.contains(&create_target));
        assert_eq!(applied.metadata_create_events, vec![create.clone()]);
        assert_registry_record_present(&storage, &record).await;
        let stored_create = read_storage_value(
            &storage,
            METADATA_EVENT_LOG_KEYSPACE,
            metadata_event_log_key(document_id, create_event_id),
        )
        .await
        .expect("create event exists");
        assert_eq!(
            postcard::from_bytes::<MetadataCreateEventRecord>(&stored_create)
                .expect("create event decodes"),
            create
        );
        let acceptance = read_storage_value(
            &storage,
            METADATA_CREATE_ACCEPTANCE_KEYSPACE,
            metadata_create_acceptance_key(document_id),
        )
        .await
        .expect("create acceptance exists");
        assert_eq!(
            postcard::from_bytes::<MetadataCreateEventRecord>(&acceptance)
                .expect("create acceptance decodes"),
            create
        );
        let applied_cursor: irokle_crate::ActorClock = postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                topic_cursor_key(metadata_topic),
            )
            .await
            .expect("applied cursor is stored"),
        )
        .expect("cursor decodes");
        assert!(!applied_cursor.dominates(&metadata_clock));
        let replayed = service
            .reconcile_document_topics([metadata_topic])
            .await
            .expect("metadata update reconciles");
        assert!(replayed.targets.contains(&update_target));
        assert!(replayed.metadata_create_events.contains(&update));
        let applied_cursor: irokle_crate::ActorClock = postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                topic_cursor_key(metadata_topic),
            )
            .await
            .expect("replayed cursor is stored"),
        )
        .expect("cursor decodes");
        assert!(applied_cursor.dominates(&metadata_clock));
        let acceptance = read_storage_value(
            &storage,
            METADATA_CREATE_ACCEPTANCE_KEYSPACE,
            metadata_create_acceptance_key(document_id),
        )
        .await
        .expect("create acceptance remains");
        assert_eq!(
            postcard::from_bytes::<MetadataCreateEventRecord>(&acceptance).unwrap(),
            create
        );

        let divergent_id = Ulid::from_parts(2_119, 1);
        let mut divergent = create.clone();
        divergent.event_id = divergent_id;
        divergent.record.establishing_event_id = divergent_id;
        divergent.record.last_event_id = divergent_id;
        let divergent_target = DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id: divergent_id,
        };
        service
            .publish_documents(
                vec![DocumentSyncPublish::Upsert {
                    event_id: divergent_id,
                    target: divergent_target,
                    bytes: postcard::to_allocvec(&divergent).expect("create serializes"),
                    change: change(divergent_id),
                    allow_genesis: true,
                }],
                Vec::new(),
            )
            .await;
        let rejected = service
            .reconcile_document_topics([metadata_topic])
            .await
            .expect("divergent create is rejected");
        assert!(rejected.metadata_create_events.is_empty());
        let acceptance = read_storage_value(
            &storage,
            METADATA_CREATE_ACCEPTANCE_KEYSPACE,
            metadata_create_acceptance_key(document_id),
        )
        .await
        .unwrap();
        assert_eq!(
            postcard::from_bytes::<MetadataCreateEventRecord>(&acceptance).unwrap(),
            create
        );
        let cursor: irokle_crate::ActorClock = postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                topic_cursor_key(metadata_topic),
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert!(
            cursor.dominates(
                &service
                    .node()
                    .storage()
                    .actor_clock(&metadata_topic)
                    .unwrap()
            )
        );

        service.shutdown().await;
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

        delete_registry_record(&storage, group_id, document_id)
            .await
            .expect("stale registry delete is fenced");

        assert_registry_record_present(&storage, &live).await;
    }

    #[tokio::test]
    async fn document_sync_fencing_tombstone_wins_over_late_metadata_registry_upsert() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(1_570, 1);
        let document_id = Ulid::from_parts(1_571, 1);
        let deleted_after_event_id = Ulid::from_parts(1_572, 1);
        let newer_event_id = Ulid::from_parts(1_574, 1);
        let delete_lifecycle = metadata_delete_lifecycle(
            group_id,
            document_id,
            200,
            Ulid::from_parts(1_573, 1),
            deleted_after_event_id,
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
            newer_event_id,
        );

        let outcome = apply_metadata_registry_upsert_to_storage(
            &storage,
            stale.clone(),
            postcard::to_allocvec(&stale).expect("stale registry serializes"),
        )
        .await
        .expect("late registry upsert is fenced by tombstone");
        assert!(matches!(outcome, MetadataPlacementOutcome::Accepted(())));

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

        delete_registry_record(&storage, group_id, document_id)
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
            shard: 3,
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
        let acceptance = read_storage_value(
            &storage,
            METADATA_CREATE_ACCEPTANCE_KEYSPACE,
            metadata_create_acceptance_key(document_id),
        )
        .await
        .expect("create acceptance exists");
        assert_eq!(
            postcard::from_bytes::<MetadataCreateEventRecord>(&acceptance).unwrap(),
            event
        );
    }

    #[tokio::test]
    async fn lifecycle_acceptance_fence() {
        let (_dir, storage) = test_storage();
        let group_id = Ulid::from_parts(11, 1);
        let document_id = Ulid::from_parts(12, 1);
        let event_id = Ulid::from_parts(13, 1);
        let accepted = metadata_create_event(group_id, document_id, 100, event_id, 7);
        let accepted_lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(accepted.clone()),
        };
        assert!(
            apply_metadata_document_lifecycle_to_storage(
                &storage,
                &accepted_lifecycle,
                metadata_lifecycle_change(&accepted_lifecycle, node(8)),
            )
            .await
            .expect("accepted lifecycle create applies")
        );

        let mut divergent = accepted.clone();
        divergent.event_id = Ulid::from_parts(14, 1);
        divergent.record.establishing_event_id = divergent.event_id;
        divergent.record.last_event_id = divergent.event_id;
        let divergent_lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(divergent),
        };
        assert!(
            !apply_metadata_document_lifecycle_to_storage(
                &storage,
                &divergent_lifecycle,
                metadata_lifecycle_change(&divergent_lifecycle, node(8)),
            )
            .await
            .expect("divergent lifecycle create is fenced")
        );
        let acceptance = read_storage_value(
            &storage,
            METADATA_CREATE_ACCEPTANCE_KEYSPACE,
            metadata_create_acceptance_key(document_id),
        )
        .await
        .expect("create acceptance remains");
        assert_eq!(
            postcard::from_bytes::<MetadataCreateEventRecord>(&acceptance).unwrap(),
            accepted
        );

        let orphan_id = Ulid::from_parts(15, 1);
        let mut orphan = metadata_create_event(group_id, orphan_id, 200, orphan_id, 7);
        orphan.payload = MetadataCreateEventPayload::ReplaceRoCrate {
            jsonld: "{}".to_string(),
        };
        let orphan_lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(orphan),
        };
        assert!(
            !apply_metadata_document_lifecycle_to_storage(
                &storage,
                &orphan_lifecycle,
                metadata_lifecycle_change(&orphan_lifecycle, node(8)),
            )
            .await
            .expect("lifecycle update without acceptance is fenced")
        );
        assert!(
            read_storage_value(
                &storage,
                METADATA_CREATE_ACCEPTANCE_KEYSPACE,
                metadata_create_acceptance_key(orphan_id),
            )
            .await
            .is_none()
        );

        let mut mismatched = accepted.clone();
        mismatched.event_id = Ulid::from_parts(16, 1);
        mismatched.record.updated_at_ms = 200;
        mismatched.record.last_event_id = mismatched.event_id;
        mismatched.record.placement = PlacementRef {
            strategy_id: Ulid::from_parts(17, 1),
            shard: 1,
        };
        mismatched.payload = MetadataCreateEventPayload::ReplaceRoCrate {
            jsonld: "{}".to_string(),
        };
        mismatched.occurred_at_ms = 200;
        let mismatched_lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(mismatched),
        };
        assert!(
            !apply_metadata_document_lifecycle_to_storage(
                &storage,
                &mismatched_lifecycle,
                metadata_lifecycle_change(&mismatched_lifecycle, node(8)),
            )
            .await
            .expect("mismatched lifecycle update is fenced")
        );
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
            shard: 5,
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
        let realm_id = RealmId::from_bytes([61u8; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(61).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let target = DocumentSyncTarget::NodeInfo {
            realm_id,
            node_id: local_node,
        };
        let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let change = DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::generate(),
                actor: local_node,
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: aruna_core::structs::PlacementRef::NIL,
        };

        let blocked = service
            .publish_documents(
                vec![DocumentSyncPublish::Upsert {
                    event_id: Ulid::generate(),
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
                    event_id: Ulid::generate(),
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
        let realm_id = RealmId::from_bytes([62; 32]);
        let placement = aruna_core::structs::PlacementRef::NIL;
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(62).await,
            storage,
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let blocked_target = DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id: local_node,
            group_id: None,
        };
        let ready_target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: local_node,
        };
        let blocked_topic = blocked_target.sync_topic_id(realm_id, &placement);
        let ready_topic = ready_target.sync_topic_id(realm_id, &placement);
        let change = DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(62, 3),
                actor: local_node,
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement,
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
        let realm_id = RealmId::from_bytes([71; 32]);
        let service_a = DocumentSyncService::open_with_persist_policy(
            test_endpoint(71).await,
            storage_a,
            doc_a.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
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
            realm_id,
        )
        .expect("service b opens");

        let node_a = service_a.local_node_id().expect("node a id");
        let node_b = service_b.local_node_id().expect("node b id");

        let user_id = UserId::local(Ulid::from_parts(7, 1), realm_id);
        let target = DocumentSyncTarget::User { user_id };
        let admin_target = AdminDocumentTarget::User { user_id };
        let placement = PlacementRef {
            strategy_id: Ulid::from_parts(71, 7),
            shard: 1,
        };
        let topic_id = target.sync_topic_id(realm_id, &placement);

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

        // Shard-classed admin topics are created eagerly; each side lists the
        // other in its genesis peer set so the loser is a member after reset.
        service_a
            .ensure_document_sync_topics(&[topic_id], vec![node_b])
            .expect("service a topic genesis");
        service_b
            .ensure_document_sync_topics(&[topic_id], vec![node_a])
            .expect("service b topic genesis");

        let published_a = service_a
            .publish_documents(
                vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(admin_a),
                    placement,
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
                    placement,
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

        // The evicted admin event re-emits with its original event id and
        // placement preserved, and allow_genesis cleared.
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
        assert_eq!(reemitted.placement, placement);
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
        let realm_id = RealmId::from_bytes([54u8; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(54).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let user_id = UserId::local(Ulid::from_parts(1_400, 1), realm_id);
        let target = DocumentSyncTarget::User { user_id };
        let placement = PlacementRef {
            strategy_id: Ulid::from_parts(54, 7),
            shard: 1,
        };
        let topic_id = target.sync_topic_id(realm_id, &placement);
        service
            .ensure_document_sync_topics(&[topic_id], Vec::new())
            .expect("admin shard topic genesis");

        let change = |kind| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::generate(),
                actor: service.local_node_id().expect("local node id"),
                updated_at_ms: 1,
            },
            kind,
            placement,
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
                        event_id: Ulid::generate(),
                        target: target.clone(),
                        bytes: b"whole-document-admin-upsert".to_vec(),
                        change: change(DocumentSyncChangeKind::Upsert),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Delete {
                        event_id: Ulid::generate(),
                        target: target.clone(),
                        change: change(DocumentSyncChangeKind::Delete),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(admin_event),
                        placement,
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

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 2, "{records:?}");
        for record in &records {
            assert_eq!(record.reason, "unsupported whole-document admin sync event");
            assert_eq!(record.target(), Some(&target));
            assert_eq!(
                record.decoded_event().expect("event decodes").placement(),
                placement
            );
        }

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
            let placement = admin_test_placement();
            let topic_id = target.sync_topic_id(realm_id, &placement);
            let impersonator = irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&node(64)));
            assert!(
                matches!(
                    validate_replicated_admin_event(
                        &storage,
                        topic_id,
                        impersonator,
                        &target,
                        &event,
                        realm_id,
                        &placement,
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
        let config_topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
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
                realm_id,
                &PlacementRef::NIL,
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
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Accepted,
            "the management self-ensure must authorize the rest of config genesis"
        );

        let (_auth_dir, auth_storage) = test_storage();
        let auth_target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let auth_topic = auth_target.sync_topic_id(realm_id, &PlacementRef::NIL);
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
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Accepted
        );

        let server_actor = test_actor(66, UserId::local(Ulid::generate(), realm_id), realm_id);
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
                        role_id: Ulid::generate(),
                    },
                ),
            ),
        ] {
            let placement = admin_test_placement();
            let topic_id = target.sync_topic_id(realm_id, &placement);
            let publisher =
                irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&server_actor.node_id));
            assert!(matches!(
                validate_replicated_admin_event(
                    &auth_storage,
                    topic_id,
                    publisher,
                    &target,
                    &event,
                    realm_id,
                    &placement,
                )
                .await
                .expect("storage succeeds"),
                AdminEventValidation::Rejected(_)
            ));
        }
    }

    #[tokio::test]
    async fn inbound_rejects_pool() {
        // A child band pool whose issuer does not own its parent is rejected;
        // one whose parent has not replicated yet is deferred.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([73; 32]);
        let coordinator = test_actor(73, UserId::local(Ulid::generate(), realm_id), realm_id);
        let attacker = test_actor(74, UserId::local(Ulid::generate(), realm_id), realm_id);
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
        let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);

        let root = BandPool {
            pool_id: Ulid::from_bytes([90; 16]),
            parent: None,
            issuer: coordinator.node_id,
            owner: coordinator.node_id,
            start: FIRST_GRANTABLE_HANDLE,
            end: band_start(HANDLE_BANDS),
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(coordinator.node_id, RealmNodeKind::Management);
        config.band_pools.push(root);
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config
                    .to_bytes(&coordinator)
                    .expect("config serializes")
                    .into(),
            )],
        )
        .await
        .expect("config writes");

        let forged = BandPool {
            pool_id: Ulid::from_bytes([91; 16]),
            parent: Some(root.pool_id),
            issuer: attacker.node_id,
            owner: attacker.node_id,
            start: band_start(1),
            end: band_start(2),
        };
        let forged_event = test_admin_event(
            Ulid::from_parts(1_702, 1),
            admin_target.clone(),
            &attacker,
            1,
            AdminDocumentOperation::RealmConfigBandPoolAssigned { pool: forged },
        );
        let forged_publisher =
            irokle_crate::actor_id_for(topic, node_id_to_peer_id(&attacker.node_id));
        assert!(matches!(
            validate_replicated_admin_event(
                &storage,
                topic,
                forged_publisher,
                &config_target,
                &forged_event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Rejected(_)
        ));

        let orphan = BandPool {
            pool_id: Ulid::from_bytes([92; 16]),
            parent: Some(Ulid::from_bytes([99; 16])),
            issuer: coordinator.node_id,
            owner: coordinator.node_id,
            start: band_start(1),
            end: band_start(2),
        };
        let orphan_event = test_admin_event(
            Ulid::from_parts(1_703, 1),
            admin_target,
            &coordinator,
            1,
            AdminDocumentOperation::RealmConfigBandPoolAssigned { pool: orphan },
        );
        let publisher = irokle_crate::actor_id_for(topic, node_id_to_peer_id(&coordinator.node_id));
        assert!(matches!(
            validate_replicated_admin_event(
                &storage,
                topic,
                publisher,
                &config_target,
                &orphan_event,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Deferred { .. }
        ));
    }

    #[tokio::test]
    async fn inbound_checks_grants() {
        // Replicated grants outside or misaligned with an issuer pool are
        // rejected; a canonical grant waits until its pool replicates.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([75; 32]);
        let coordinator = test_actor(75, UserId::local(Ulid::generate(), realm_id), realm_id);
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
        let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let publisher = irokle_crate::actor_id_for(topic, node_id_to_peer_id(&coordinator.node_id));

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(coordinator.node_id, RealmNodeKind::Management);
        config.band_pools.push(BandPool {
            pool_id: Ulid::from_bytes([93; 16]),
            parent: None,
            issuer: coordinator.node_id,
            owner: coordinator.node_id,
            start: band_start(0),
            end: band_start(2),
        });
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config
                    .to_bytes(&coordinator)
                    .expect("config serializes")
                    .into(),
            )],
        )
        .await
        .expect("config writes");

        for (event_id, range, expected) in [
            (
                Ulid::from_parts(1_704, 1),
                HandleRange {
                    range_id: Ulid::from_bytes([94; 16]),
                    owner: node(76),
                    start: band_start(3),
                    end: band_start(4),
                },
                "handle grant lies outside the coordinator band pool",
            ),
            (
                Ulid::from_parts(1_705, 1),
                HandleRange {
                    range_id: Ulid::from_bytes([95; 16]),
                    owner: node(76),
                    start: band_start(0) + 1,
                    end: band_start(0) + 1 + HANDLE_RANGE_SIZE,
                },
                "handle grant is not one canonical band",
            ),
        ] {
            let event = test_admin_event(
                event_id,
                admin_target.clone(),
                &coordinator,
                1,
                AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
            );
            assert_eq!(
                validate_replicated_admin_event(
                    &storage,
                    topic,
                    publisher,
                    &config_target,
                    &event,
                    realm_id,
                    &PlacementRef::NIL,
                )
                .await
                .expect("storage succeeds"),
                AdminEventValidation::Rejected(expected.to_string())
            );
        }

        config.band_pools.clear();
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                config_target.clone(),
                config
                    .to_bytes(&coordinator)
                    .expect("config serializes")
                    .into(),
            )],
        )
        .await
        .expect("config writes");
        let waiting = test_admin_event(
            Ulid::from_parts(1_706, 1),
            admin_target,
            &coordinator,
            1,
            AdminDocumentOperation::RealmConfigHandleRangeGranted {
                range: HandleRange {
                    range_id: Ulid::from_bytes([96; 16]),
                    owner: node(76),
                    start: band_start(0),
                    end: band_start(1),
                },
            },
        );
        assert_eq!(
            validate_replicated_admin_event(
                &storage,
                topic,
                publisher,
                &config_target,
                &waiting,
                realm_id,
                &PlacementRef::NIL,
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Deferred {
                dependency: None,
                reason: "coordinator band pool is not yet replicated".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn inbound_admin_validation_rejects_target_and_malformed_events() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([67; 32]);
        let actor = test_actor(67, UserId::local(Ulid::generate(), realm_id), realm_id);
        let user_id = actor.user_id;
        let other_user = UserId::local(Ulid::generate(), realm_id);
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
        let placement = admin_test_placement();
        let topic_id = target.sync_topic_id(realm_id, &placement);
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
            validate_replicated_admin_event(
                &storage,
                topic_id,
                publisher,
                &target,
                &wrong_target,
                realm_id,
                &placement,
            )
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
            validate_replicated_admin_event(
                &storage, topic_id, publisher, &target, &malformed, realm_id, &placement,
            )
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
        let realm_id = RealmId::from_bytes([68; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(68).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let admin_user_id = UserId::local(Ulid::from_parts(1_625, 1), realm_id);
        let bootstrap_actor = test_actor(68, UserId::nil(realm_id), realm_id);
        let admin_actor = test_actor(68, admin_user_id, realm_id);
        let role_id = Ulid::from_parts(1_626, 1);
        let auth_target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let auth_topic = auth_target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let config_topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
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
                    placement: PlacementRef::NIL,
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
                    topic_cursor_key(target.sync_topic_id(realm_id, &PlacementRef::NIL)),
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
        let deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>> =
            postcard::from_bytes(
                &read_storage_value(
                    &storage,
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                    deferred_topics_key(),
                )
                .await
                .expect("deferred topic registry is persisted"),
            )
            .expect("deferred topic registry decodes");
        assert_eq!(
            deferred_topics
                .get(&DocumentSyncDependency::RealmConfig(realm_id))
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
        let group_placement = admin_test_placement();
        // Shard topics are join-only at publish; create the genesis eagerly.
        service
            .ensure_document_sync_topics(
                &[group_target.sync_topic_id(realm_id, &group_placement)],
                Vec::new(),
            )
            .expect("group shard topic genesis");
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
                            placement: group_placement,
                            allow_genesis: true,
                        },
                        DocumentSyncPublish::AdminOperation {
                            target: group_target.clone(),
                            event: Box::new(group_create),
                            placement: group_placement,
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
                topic_cursor_key(group_target.sync_topic_id(realm_id, &group_placement)),
                postcard::to_allocvec(&irokle_crate::ActorClock::default())
                    .expect("clock serializes")
                    .into(),
            )
            .await
            .expect("group cursor resets");
        service
            .reconcile_document_topics([group_target.sync_topic_id(realm_id, &group_placement)])
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
        let realm_id = RealmId::from_bytes([63; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(63).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let other_realm_id = RealmId::from_bytes([64; 32]);
        let user_id = UserId::local(Ulid::from_parts(1_630, 1), realm_id);
        let local_actor = test_actor(63, user_id, realm_id);
        let claimed_actor = test_actor(64, user_id, realm_id);
        assert_eq!(
            service.local_node_id().expect("local node id"),
            local_actor.node_id
        );

        let target = DocumentSyncTarget::RealmConfig { realm_id };
        let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
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
                    shard_count: 64,
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
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(impersonated_event),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(reducer_invalid_event),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(valid_event),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(unrelated_event),
                        placement: PlacementRef::NIL,
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

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 4, "{records:?}");
        for record in &records {
            assert_eq!(record.family(), Some(SyncQuarantineFamily::AdminOperation));
            assert_eq!(record.target(), Some(&target));
            assert!(!record.reason.is_empty());
            assert!(matches!(
                record.decoded_event().expect("event decodes"),
                DocumentSyncEvent::AdminOperation { .. }
            ));
        }
        assert_eq!(quarantine_usage(&storage).await.records, 4);

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
    fn watch_interest_validation() {
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
        assert!(validate_watch_interest(&target, &owned_bytes).is_ok());

        // Empty digests are legitimate upserts that clear a node's interest.
        let empty = WatchInterestDigest {
            node_id,
            entries: Vec::new(),
        };
        assert!(validate_watch_interest(&target, &empty.to_bytes().unwrap()).is_ok());

        let too_many = WatchInterestDigest::from_subscriptions(
            node_id,
            (0..=NOTIFICATION_WATCH_INTEREST_ENTRY_CAP).map(|index| {
                (
                    format!("/entry/{index}"),
                    WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
                )
            }),
        );
        assert!(validate_watch_interest(&target, &too_many.to_bytes().unwrap()).is_err());
        assert!(
            validate_watch_interest(&target, &vec![0; NOTIFICATION_WATCH_INTEREST_BYTES_CAP + 1],)
                .is_err()
        );

        // A digest whose embedded node id is a different node is rejected.
        let misattributed = WatchInterestDigest::from_subscriptions(
            node(9),
            [(
                "/forged/**".to_string(),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            )],
        );
        assert!(validate_watch_interest(&target, &misattributed.to_bytes().unwrap()).is_err());

        // Undecodable payloads and non watch-interest targets are rejected.
        assert!(validate_watch_interest(&target, b"not-a-digest").is_err());
        assert!(
            validate_watch_interest(&DocumentSyncTarget::RealmConfig { realm_id }, &owned_bytes,)
                .is_err()
        );
    }

    #[tokio::test]
    async fn watch_origins_converge() {
        // Independent origins must not lose a replica to arrival-order admission.
        use aruna_core::structs::{WatchEventKind, WatchEventMask};

        let (_left_dir, left) = test_storage();
        let (_right_dir, right) = test_storage();
        let realm_id = RealmId::from_bytes([57u8; 32]);
        let make = |seed: u8, event_mask: WatchEventMask| {
            let owner = UserId::local(Ulid::from_parts(seed as u64, 1), realm_id);
            let watch_id = Ulid::from_parts(seed as u64, 2);
            let mut subscription =
                WatchSubscription::new(owner, format!("watch/{seed}"), event_mask, 1);
            subscription.watch_id = watch_id;
            let change = DocumentSyncChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::from_parts(seed as u64, 3),
                    actor: node(seed),
                    updated_at_ms: 1,
                },
                kind: DocumentSyncChangeKind::Upsert,
                placement: PlacementRef::NIL,
            };
            (
                DocumentSyncTarget::WatchSubscription { owner, watch_id },
                subscription.to_bytes().expect("subscription serializes"),
                change,
            )
        };
        let first = make(
            1,
            WatchEventMask::from_kinds([WatchEventKind::SyncCompleted]),
        );
        let second = make(2, WatchEventMask::from_kinds([WatchEventKind::SyncFailed]));

        assert!(validate_watch_subscription_upsert(&first.0, &first.1, &first.2).is_ok());
        assert!(
            apply_watch_subscription_change_to_storage(
                &left,
                first.0.clone(),
                Some(first.1.clone()),
                first.2,
            )
            .await
            .expect("first origin applies")
        );
        assert!(validate_watch_subscription_upsert(&second.0, &second.1, &second.2).is_ok());
        assert!(
            apply_watch_subscription_change_to_storage(
                &left,
                second.0.clone(),
                Some(second.1.clone()),
                second.2,
            )
            .await
            .expect("second origin applies")
        );
        assert!(validate_watch_subscription_upsert(&second.0, &second.1, &second.2).is_ok());
        assert!(
            apply_watch_subscription_change_to_storage(
                &right,
                second.0.clone(),
                Some(second.1.clone()),
                second.2,
            )
            .await
            .expect("second origin applies in reverse order")
        );
        assert!(validate_watch_subscription_upsert(&first.0, &first.1, &first.2).is_ok());
        assert!(
            apply_watch_subscription_change_to_storage(
                &right,
                first.0.clone(),
                Some(first.1.clone()),
                first.2,
            )
            .await
            .expect("first origin applies in reverse order")
        );

        for storage in [&left, &right] {
            assert!(
                read_storage_value(storage, first.0.storage_keyspace(), first.0.storage_key(),)
                    .await
                    .is_some()
            );
            assert!(
                read_storage_value(storage, second.0.storage_keyspace(), second.0.storage_key(),)
                    .await
                    .is_some()
            );
        }
        let unknown_mask: WatchEventMask =
            postcard::from_bytes(&postcard::to_allocvec(&16u32).expect("unknown mask serializes"))
                .expect("unknown mask decodes");
        let (target, bytes, change) = make(3, unknown_mask);
        assert!(validate_watch_subscription_upsert(&target, &bytes, &change).is_err());
    }

    #[test]
    fn validate_node_info_upsert_accepts_owner_and_rejects_forgeries() {
        use aruna_core::structs::{NodeInfoDocument, NodeUrls, NodeUtilization};

        let node_id = node(7);
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let target = DocumentSyncTarget::NodeInfo { realm_id, node_id };

        let owned = NodeInfoDocument {
            node_id,
            executors: Vec::new(),
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
        let realm_id = RealmId::from_bytes([55u8; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(55).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let forged_node = node(88);
        assert_ne!(local_node, forged_node);
        let target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: local_node,
        };
        let forged_target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: forged_node,
        };
        let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        assert_eq!(
            topic_id,
            forged_target.sync_topic_id(realm_id, &PlacementRef::NIL)
        );

        let change = || DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::generate(),
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
                        event_id: Ulid::generate(),
                        target: forged_target.clone(),
                        bytes: forged_digest.to_bytes().expect("digest serializes"),
                        change: change(),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::generate(),
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

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].reason,
            "watch interest publisher is not the owning node"
        );
        assert_eq!(records[0].target(), Some(&forged_target));

        service.shutdown().await;
    }

    #[tokio::test]
    async fn reconcile_skips_forged_non_upsert_watch_interest_events() {
        use aruna_core::structs::{WatchEventKind, WatchEventMask};

        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([56u8; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(56).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let forged_node = node(89);
        assert_ne!(local_node, forged_node);
        let target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: local_node,
        };
        let forged_target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: forged_node,
        };
        let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        assert_eq!(
            topic_id,
            forged_target.sync_topic_id(realm_id, &PlacementRef::NIL)
        );

        let change = |kind| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::generate(),
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
                        event_id: Ulid::generate(),
                        target: forged_target.clone(),
                        change: change(DocumentSyncChangeKind::Delete),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: forged_target.clone(),
                        event: Box::new(admin_event),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::generate(),
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

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 2, "{records:?}");
        for record in &records {
            assert_eq!(
                record.reason,
                "unsupported non-upsert shared realm document event"
            );
        }

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
            RealmId::from_bytes([53u8; 32]),
        )
        .expect("document sync service opens");

        let local_node = service.local_node_id().expect("local node id");
        let realm_id = RealmId::from_bytes([53u8; 32]);
        let target = DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id: local_node,
            group_id: None,
        };
        let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);

        let change = |kind| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::generate(),
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
                        event_id: Ulid::generate(),
                        target: target.clone(),
                        change: change(DocumentSyncChangeKind::Delete),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: Ulid::generate(),
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

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].reason,
            "unsupported non-upsert shared realm document event"
        );
        assert_eq!(records[0].family(), Some(SyncQuarantineFamily::Delete));

        service.shutdown().await;
    }

    async fn quarantine_rows(storage: &StorageHandle) -> Vec<SyncQuarantineRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: SYNC_QUARANTINE_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 256,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| {
                    SyncQuarantineRecord::from_bytes(value.as_ref()).expect("record decodes")
                })
                .collect(),
            other => panic!("unexpected storage iteration event: {other:?}"),
        }
    }

    async fn quarantine_usage(storage: &StorageHandle) -> SyncQuarantineUsage {
        match read_storage_value(
            storage,
            SYNC_QUARANTINE_USAGE_KEYSPACE,
            ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
        )
        .await
        {
            Some(bytes) => SyncQuarantineUsage::from_bytes(bytes.as_ref()).expect("usage decodes"),
            None => SyncQuarantineUsage::default(),
        }
    }

    async fn write_usage(storage: &StorageHandle, usage: SyncQuarantineUsage) {
        storage_batch_write_to(
            storage,
            vec![(
                SYNC_QUARANTINE_USAGE_KEYSPACE.to_string(),
                ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
                ByteView::from(usage.to_bytes().expect("usage serializes")),
            )],
        )
        .await
        .expect("usage row writes");
    }

    async fn reset_cursor(service: &DocumentSyncService, topic_id: irokle_crate::TopicId) {
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
    }

    async fn cursor_advanced(
        service: &DocumentSyncService,
        storage: &StorageHandle,
        topic_id: irokle_crate::TopicId,
    ) -> bool {
        let Some(bytes) = read_storage_value(
            storage,
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
            topic_cursor_key(topic_id),
        )
        .await
        else {
            return false;
        };
        let cursor: irokle_crate::ActorClock =
            postcard::from_bytes(&bytes).expect("cursor decodes");
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock");
        cursor.dominates(&topic_clock)
    }

    fn quarantined_reason(records: &[SyncQuarantineRecord], event_id: Ulid) -> String {
        records
            .iter()
            .find(|record| record.event_id() == Some(event_id))
            .unwrap_or_else(|| panic!("event {event_id} is quarantined"))
            .reason
            .clone()
    }

    fn node_info_bytes(node_id: NodeId, updated_at_ms: u64) -> Vec<u8> {
        use aruna_core::structs::{NodeInfoDocument, NodeUrls, NodeUtilization};

        NodeInfoDocument {
            node_id,
            executors: Vec::new(),
            labels: BTreeMap::new(),
            urls: NodeUrls {
                api: None,
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 1,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: updated_at_ms,
            },
            updated_at_ms,
        }
        .to_bytes()
        .expect("node info serializes")
    }

    #[tokio::test]
    async fn quarantine_retains_families() {
        use aruna_core::structs::{WatchEventKind, WatchEventMask};

        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([70u8; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(70).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");
        let forged_node = node(71);
        let owner = UserId::local(Ulid::from_bytes([3; 16]), realm_id);

        let change = |actor, generation, kind| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation,
                event_id: Ulid::generate(),
                actor,
                updated_at_ms: 1,
            },
            kind,
            placement: PlacementRef::NIL,
        };

        // Same family, invalid then valid: an unusable path prefix, a delete
        // signed by a node that is not the revision's actor, then a good upsert.
        let invalid_watch = WatchSubscription::new(
            owner,
            "/leading-slash".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            1,
        );
        let deleted_watch = WatchSubscription::new(
            owner,
            "deleted".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            1,
        );
        let valid_watch = WatchSubscription::new(
            owner,
            "documents".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            1,
        );
        let watch_target = |watch: &WatchSubscription| DocumentSyncTarget::WatchSubscription {
            owner,
            watch_id: watch.watch_id,
        };
        let watch_topic = watch_target(&valid_watch).sync_topic_id(realm_id, &PlacementRef::NIL);

        let info_target = DocumentSyncTarget::NodeInfo {
            realm_id,
            node_id: local_node,
        };
        let info_topic = info_target.sync_topic_id(realm_id, &PlacementRef::NIL);

        let invalid_watch_event = Ulid::generate();
        let forged_delete_event = Ulid::generate();
        let valid_watch_event = Ulid::generate();
        let invalid_info_event = Ulid::generate();
        let valid_info_event = Ulid::generate();
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Upsert {
                        event_id: invalid_watch_event,
                        target: watch_target(&invalid_watch),
                        bytes: invalid_watch.to_bytes().expect("subscription serializes"),
                        change: change(local_node, 1, DocumentSyncChangeKind::Upsert),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Delete {
                        event_id: forged_delete_event,
                        target: watch_target(&deleted_watch),
                        change: change(forged_node, 2, DocumentSyncChangeKind::Delete),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: valid_watch_event,
                        target: watch_target(&valid_watch),
                        bytes: valid_watch.to_bytes().expect("subscription serializes"),
                        change: change(local_node, 1, DocumentSyncChangeKind::Upsert),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: invalid_info_event,
                        target: info_target.clone(),
                        bytes: node_info_bytes(forged_node, 5),
                        change: change(local_node, 1, DocumentSyncChangeKind::Upsert),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: valid_info_event,
                        target: info_target.clone(),
                        bytes: node_info_bytes(local_node, 6),
                        change: change(local_node, 2, DocumentSyncChangeKind::Upsert),
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

        reset_cursor(&service, watch_topic).await;
        reset_cursor(&service, info_topic).await;
        service
            .reconcile_document_topics([watch_topic, info_topic])
            .await
            .expect("rejected events are quarantined");

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 3, "{records:?}");
        assert!(
            quarantined_reason(&records, invalid_watch_event)
                .starts_with("invalid watch subscription:")
        );
        assert_eq!(
            quarantined_reason(&records, forged_delete_event),
            "watch subscription delete actor is not its publisher"
        );
        assert!(
            quarantined_reason(&records, invalid_info_event)
                .starts_with("invalid node info document:")
        );
        // The complete received envelope survives, target included.
        let invalid_watch_record = records
            .iter()
            .find(|record| record.event_id() == Some(invalid_watch_event))
            .expect("watch evidence");
        assert_eq!(
            invalid_watch_record.family(),
            Some(SyncQuarantineFamily::Upsert)
        );
        assert_eq!(
            invalid_watch_record.target(),
            Some(&watch_target(&invalid_watch)),
            "evidence keeps the real target, not a placeholder"
        );
        assert_eq!(invalid_watch_record.origin(), Some(local_node));
        assert_eq!(invalid_watch_record.identity.topic, watch_topic);
        match invalid_watch_record.decoded_event().expect("event decodes") {
            DocumentSyncEvent::Upsert {
                event_id, bytes, ..
            } => {
                assert_eq!(event_id, invalid_watch_event);
                assert_eq!(
                    WatchSubscription::from_bytes(&bytes).expect("subscription decodes"),
                    invalid_watch
                );
            }
            other => panic!("unexpected quarantined event: {other:?}"),
        }
        let forged_delete_record = records
            .iter()
            .find(|record| record.event_id() == Some(forged_delete_event))
            .expect("delete evidence");
        assert_eq!(
            forged_delete_record.family(),
            Some(SyncQuarantineFamily::Delete)
        );
        assert_eq!(forged_delete_record.origin(), Some(forged_node));

        // The valid successors of both families applied.
        assert!(
            read_storage_value(
                &storage,
                watch_target(&valid_watch).storage_keyspace(),
                watch_target(&valid_watch).storage_key(),
            )
            .await
            .is_some()
        );
        assert_eq!(
            read_storage_value(
                &storage,
                info_target.storage_keyspace(),
                info_target.storage_key(),
            )
            .await
            .expect("node info applied")
            .as_ref(),
            node_info_bytes(local_node, 6).as_slice()
        );
        assert!(cursor_advanced(&service, &storage, watch_topic).await);
        assert!(cursor_advanced(&service, &storage, info_topic).await);

        let usage = quarantine_usage(&storage).await;
        assert_eq!(usage.records, 3);
        assert_eq!(
            usage.bytes,
            records
                .iter()
                .map(|record| record.to_bytes().expect("record serializes").len() as u64)
                .sum::<u64>()
        );

        // Replay: the same evidence, no duplicate rows and no usage growth.
        reset_cursor(&service, watch_topic).await;
        reset_cursor(&service, info_topic).await;
        service
            .reconcile_document_topics([watch_topic, info_topic])
            .await
            .expect("replay reconciles");
        let replayed = quarantine_rows(&storage).await;
        assert_eq!(replayed.len(), 3);
        assert_eq!(quarantine_usage(&storage).await, usage);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn quarantine_keeps_placement() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([72u8; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(72).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");

        let placement = PlacementRef {
            strategy_id: Ulid::from_parts(7_200, 1),
            epoch: 0,
            shard: 5,
        };
        let target = DocumentSyncTarget::PersistentIdMapping {
            document_id: Ulid::from_parts(7_201, 1),
        };
        let topic_id = target.sync_topic_id(realm_id, &placement);
        service
            .ensure_document_sync_topics(&[topic_id], Vec::new())
            .expect("mapping shard topic genesis");
        let event_id = Ulid::generate();
        let published = service
            .publish_documents(
                vec![DocumentSyncPublish::Delete {
                    event_id,
                    target: target.clone(),
                    change: DocumentSyncChange {
                        base: None,
                        current: DocumentSyncRevision {
                            generation: 1,
                            event_id: Ulid::generate(),
                            actor: local_node,
                            updated_at_ms: 1,
                        },
                        kind: DocumentSyncChangeKind::Delete,
                        placement,
                    },
                    allow_genesis: true,
                }],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        reset_cursor(&service, topic_id).await;
        service
            .reconcile_document_topics([topic_id])
            .await
            .expect("unsupported mapping delete is quarantined");

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].reason,
            "unsupported non-upsert persistent id mapping event"
        );
        assert_eq!(records[0].target(), Some(&target));
        // The stopgap re-wrapped rejects with a NIL placement; the real one rides along.
        assert_eq!(
            records[0]
                .decoded_event()
                .expect("event decodes")
                .placement(),
            placement
        );
        assert!(cursor_advanced(&service, &storage, topic_id).await);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn quarantine_rejects_placement() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([42; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(75).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");
        let actor = test_actor(
            75,
            UserId::local(Ulid::from_parts(2_150, 1), realm_id),
            realm_id,
        );
        assert_eq!(actor.node_id, local_node);

        let strategy_id = Ulid::from_parts(2_151, 1);
        let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(local_node, RealmNodeKind::Management);
        config.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        });
        config.strategies.push(PlacementStrategy {
            strategy_id,
            name: "placed".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        });
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config
                    .to_bytes(&actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");

        // A document whose stamped shard is not the one its id decodes to fails
        // the placement fence permanently, on both the registry and create paths.
        let document = |bucket: u16, seed: u64| {
            MetaResourceId::from_parts(seed, handle, BucketId::new(bucket).unwrap(), 1)
                .unwrap()
                .as_ulid()
        };
        let group_id = Ulid::from_parts(2_152, 1);
        let mismatched = PlacementRef {
            strategy_id,
            epoch: 0,
            shard: 9,
        };
        let matching = PlacementRef {
            strategy_id,
            epoch: 0,
            shard: 4,
        };
        let bad_document = document(4, 2_153);
        let good_document = document(matching.shard as u16, 2_154);
        let bad_event_id = Ulid::from_parts(2_155, 1);
        let good_event_id = Ulid::from_parts(2_156, 1);

        let mut bad_record = registry_record(
            group_id,
            bad_document,
            "datasets/mismatched",
            100,
            bad_event_id,
        );
        bad_record.placement = mismatched;
        let mut bad_create = metadata_create_event(group_id, bad_document, 100, bad_event_id, 75);
        bad_create.record = bad_record.clone();
        let mut good_record = registry_record(
            group_id,
            good_document,
            "datasets/placed",
            100,
            good_event_id,
        );
        good_record.placement = matching;
        let mut good_create =
            metadata_create_event(group_id, good_document, 100, good_event_id, 75);
        good_create.record = good_record.clone();

        let registry_target = |document_id| DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        };
        let create_target = |document_id, event_id| DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
        };
        let bad_topic = registry_target(bad_document).sync_topic_id(realm_id, &mismatched);
        let good_topic = registry_target(good_document).sync_topic_id(realm_id, &matching);
        service
            .ensure_document_sync_topics(&[bad_topic, good_topic], Vec::new())
            .expect("metadata shard topic genesis");
        let change = |event_id, placement| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id,
                actor: local_node,
                updated_at_ms: 100,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        };
        let bad_registry_event = Ulid::from_parts(2_157, 1);
        let good_registry_event = Ulid::from_parts(2_158, 1);
        let published = service
            .publish_documents(
                vec![
                    DocumentSyncPublish::Upsert {
                        event_id: bad_registry_event,
                        target: registry_target(bad_document),
                        bytes: postcard::to_allocvec(&bad_record).expect("registry serializes"),
                        change: change(bad_registry_event, mismatched),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: bad_event_id,
                        target: create_target(bad_document, bad_event_id),
                        bytes: postcard::to_allocvec(&bad_create).expect("create serializes"),
                        change: change(bad_event_id, mismatched),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: good_registry_event,
                        target: registry_target(good_document),
                        bytes: postcard::to_allocvec(&good_record).expect("registry serializes"),
                        change: change(good_registry_event, matching),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: good_event_id,
                        target: create_target(good_document, good_event_id),
                        bytes: postcard::to_allocvec(&good_create).expect("create serializes"),
                        change: change(good_event_id, matching),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "metadata publish failed: {published:?}"
        );

        reset_cursor(&service, bad_topic).await;
        reset_cursor(&service, good_topic).await;
        let applied = service
            .reconcile_document_topics([bad_topic, good_topic])
            .await
            .expect("mismatched placements are quarantined");

        assert!(applied.targets.contains(&registry_target(good_document)));
        assert!(
            applied
                .targets
                .contains(&create_target(good_document, good_event_id))
        );
        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 2, "{records:?}");
        assert_eq!(
            quarantined_reason(&records, bad_registry_event),
            "metadata registry record has a mismatched placement configuration"
        );
        assert_eq!(
            quarantined_reason(&records, bad_event_id),
            "replicated metadata create has a mismatched placement configuration"
        );
        // The create-batch reject rides that function's own transaction.
        assert!(cursor_advanced(&service, &storage, bad_topic).await);
        assert!(cursor_advanced(&service, &storage, good_topic).await);
        assert_eq!(quarantine_usage(&storage).await.records, 2);

        service.shutdown().await;
    }

    /// `event_id` is payload-controlled, so it may repeat across publishers,
    /// operations, and batches. Rows and usage follow transport identity: one
    /// row per identity, counted exactly once however often it is rejected.
    #[tokio::test]
    async fn quarantine_batch_accounting() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([79; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(79).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");

        let topic_id = topic(79);
        let shared_event_id = Ulid::from_parts(3_300, 1);
        let event = || DocumentSyncEvent::Upsert {
            event_id: shared_event_id,
            target: DocumentSyncTarget::RealmConfig { realm_id },
            bytes: vec![3; 8],
            change: DocumentSyncChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: shared_event_id,
                    actor: node(79),
                    updated_at_ms: 1,
                },
                kind: DocumentSyncChangeKind::Upsert,
                placement: PlacementRef::NIL,
            },
        };
        let identity = |actor: u8, actor_seq: u64| SyncQuarantineIdentity {
            topic: topic_id,
            actor: irokle_crate::ActorId::from_bytes([actor; 32]),
            actor_seq,
        };
        let commit = |rejections: Vec<SyncRejection>| {
            let service = service.clone();
            async move {
                let txn_id = start_storage_transaction(&service.storage)
                    .await
                    .expect("transaction starts");
                let entries = service
                    .quarantine_entries(&rejections, txn_id)
                    .await
                    .expect("evidence builds")
                    .expect("capacity is available");
                storage_batch_delete_and_write_in_transaction(
                    &service.storage,
                    txn_id,
                    Vec::new(),
                    entries,
                )
                .await
                .expect("evidence commits");
            }
        };

        // One batch: one publisher's two operations, a second publisher reusing
        // the same event id, and a repeat of the first key.
        commit(vec![
            SyncRejection::new(identity(1, 1), event(), "first"),
            SyncRejection::new(identity(1, 2), event(), "second"),
            SyncRejection::new(identity(2, 1), event(), "other publisher"),
            SyncRejection::new(identity(1, 1), event(), "repeat in batch"),
        ])
        .await;
        let stored_bytes = |records: &[SyncQuarantineRecord]| {
            records
                .iter()
                .map(|record| record.to_bytes().expect("record serializes").len() as u64)
                .sum::<u64>()
        };
        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 3, "{records:?}");
        let usage = quarantine_usage(&storage).await;
        assert_eq!(usage.records, 3);
        assert_eq!(usage.bytes, stored_bytes(&records));
        assert_eq!(
            records
                .iter()
                .find(|record| record.identity == identity(1, 1))
                .expect("first identity")
                .reason,
            "repeat in batch",
            "the batch's last write for a key is the stored one"
        );

        // Several batches: the same three identities redeliver, and a fourth
        // operation reusing the same event id is the only new row.
        commit(vec![
            SyncRejection::new(identity(1, 1), event(), "redelivered"),
            SyncRejection::new(identity(2, 1), event(), "redelivered"),
        ])
        .await;
        commit(vec![
            SyncRejection::new(identity(1, 2), event(), "redelivered"),
            SyncRejection::new(identity(2, 2), event(), "new operation"),
        ])
        .await;
        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 4, "{records:?}");
        let usage = quarantine_usage(&storage).await;
        assert_eq!(usage.records, 4);
        assert_eq!(usage.bytes, stored_bytes(&records));

        service.shutdown().await;
    }

    /// A malformed envelope or payload from any metadata/PID family is
    /// permanent evidence, never a replayed error: the topic keeps moving and
    /// the valid successor behind the poison applies. While evidence cannot be
    /// persisted, nothing advances.
    #[tokio::test]
    async fn quarantine_malformed_payloads() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        // `registry_record` binds its permission path to this realm.
        let realm_id = RealmId::from_bytes([42; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(78).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");
        let actor = test_actor(
            78,
            UserId::local(Ulid::from_parts(3_200, 1), realm_id),
            realm_id,
        );
        assert_eq!(actor.node_id, local_node);

        let strategy_id = Ulid::from_parts(3_201, 1);
        let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
        let group_id = Ulid::from_parts(3_202, 1);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(local_node, RealmNodeKind::Management);
        config.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        });
        config.strategies.push(PlacementStrategy {
            strategy_id,
            name: "placed".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        });
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config
                    .to_bytes(&actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");

        let placement = PlacementRef {
            strategy_id,
            epoch: 0,
            shard: 4,
        };
        let document = |seed: u64| {
            MetaResourceId::from_parts(seed, handle, BucketId::new(4).unwrap(), 1)
                .unwrap()
                .as_ulid()
        };
        let topic_id = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id: document(3_210),
        }
        .sync_topic_id(realm_id, &placement);
        service
            .ensure_document_sync_topics(&[topic_id], Vec::new())
            .expect("metadata shard topic genesis");

        // A payload no peer can decode into an event at all.
        let raw_payload = vec![0xffu8; 24];
        let raw_identity = service
            .publish_raw_event(topic_id, raw_payload.clone())
            .expect("raw op publishes");

        let change = |event_id| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id,
                actor: local_node,
                updated_at_ms: 100,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        };
        let poison = |event_id: u64, target: DocumentSyncTarget| {
            let event_id = Ulid::from_parts(event_id, 1);
            DocumentSyncPublish::Upsert {
                event_id,
                target,
                bytes: vec![0xfe; 12],
                change: change(event_id),
                allow_genesis: true,
            }
        };
        // A registry payload that decodes but names another document.
        let mismatched_event = Ulid::from_parts(3_226, 1);
        let mut mismatched = registry_record(
            group_id,
            document(3_211),
            "datasets/mismatched",
            100,
            mismatched_event,
        );
        mismatched.placement = placement;
        let valid_event = Ulid::from_parts(3_227, 1);
        let valid_document = document(3_212);
        let mut valid =
            registry_record(group_id, valid_document, "datasets/valid", 100, valid_event);
        valid.placement = placement;
        let valid_target = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id: valid_document,
        };

        let published = service
            .publish_documents(
                vec![
                    poison(
                        3_220,
                        DocumentSyncTarget::MetadataRegistry {
                            group_id,
                            document_id: document(3_213),
                        },
                    ),
                    poison(
                        3_221,
                        DocumentSyncTarget::MetadataCreateEvent {
                            document_id: document(3_214),
                            event_id: Ulid::from_parts(3_221, 1),
                        },
                    ),
                    poison(
                        3_222,
                        DocumentSyncTarget::MetadataDocumentLifecycle {
                            document_id: document(3_215),
                        },
                    ),
                    poison(
                        3_223,
                        DocumentSyncTarget::MetadataGraphLifecycle {
                            graph_iri: MetadataRegistryRecord::graph_iri_for(document(3_216)),
                        },
                    ),
                    poison(
                        3_224,
                        DocumentSyncTarget::PersistentIdMapping {
                            document_id: document(3_217),
                        },
                    ),
                    DocumentSyncPublish::Upsert {
                        event_id: mismatched_event,
                        target: DocumentSyncTarget::MetadataRegistry {
                            group_id,
                            document_id: document(3_218),
                        },
                        bytes: postcard::to_allocvec(&mismatched).expect("registry serializes"),
                        change: change(mismatched_event),
                        allow_genesis: true,
                    },
                    DocumentSyncPublish::Upsert {
                        event_id: valid_event,
                        target: valid_target.clone(),
                        bytes: postcard::to_allocvec(&valid).expect("registry serializes"),
                        change: change(valid_event),
                        allow_genesis: true,
                    },
                ],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "poison publish failed: {published:?}"
        );

        // While evidence cannot be persisted, neither the poison nor its valid
        // successor may pass: the cursor stays before both.
        write_usage(
            &storage,
            SyncQuarantineUsage {
                records: SYNC_QUARANTINE_MAX_RECORDS,
                bytes: 0,
            },
        )
        .await;
        reset_cursor(&service, topic_id).await;
        service
            .reconcile_document_topics([topic_id])
            .await
            .expect("a full quarantine store is not a reconcile failure");
        assert!(quarantine_rows(&storage).await.is_empty());
        // Applies are idempotent and monotone; the cursor is what may not move
        // past evidence that is not durable, so the whole batch redelivers.
        assert!(!cursor_advanced(&service, &storage, topic_id).await);

        write_usage(&storage, SyncQuarantineUsage::default()).await;
        let applied = service
            .reconcile_document_topics([topic_id])
            .await
            .expect("malformed payloads are quarantined");
        assert!(
            applied.targets.contains(&valid_target),
            "{:?}",
            applied.targets
        );
        assert!(
            read_storage_value(
                &storage,
                valid_target.storage_keyspace(),
                valid_target.storage_key(),
            )
            .await
            .is_some(),
            "the valid successor applies"
        );
        assert!(cursor_advanced(&service, &storage, topic_id).await);

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 7, "{records:?}");
        let raw = records
            .iter()
            .find(|record| record.identity == raw_identity)
            .expect("raw evidence");
        assert_eq!(raw.event_id(), None);
        assert_eq!(raw.evidence.bytes(), raw_payload.as_slice());
        assert!(raw.reason.starts_with("undecodable sync payload of type"));
        for (event_id, reason) in [
            (3_220, "undecodable metadata registry record"),
            (3_221, "undecodable metadata create event"),
            (3_222, "undecodable metadata document lifecycle record"),
            (3_223, "undecodable metadata graph lifecycle record"),
            (3_224, "undecodable persistent id mapping"),
        ] {
            let quarantined = quarantined_reason(&records, Ulid::from_parts(event_id, 1));
            assert!(quarantined.starts_with(reason), "{quarantined}");
        }
        assert!(
            quarantined_reason(&records, mismatched_event).starts_with("metadata registry target")
        );
        let usage = quarantine_usage(&storage).await;
        assert_eq!(usage.records, 7);

        // Redelivery is the same evidence under the same transport identity.
        reset_cursor(&service, topic_id).await;
        service
            .reconcile_document_topics([topic_id])
            .await
            .expect("replay reconciles");
        assert_eq!(quarantine_rows(&storage).await.len(), 7);
        assert_eq!(quarantine_usage(&storage).await, usage);

        service.shutdown().await;
    }

    /// Only a correctly placed, structurally consistent, publisher-bound mapping
    /// reaches the mapping row or the shard manifest: a holder of one shard may
    /// not stamp a document that decodes to another, even when both shards share
    /// this holder.
    #[tokio::test]
    async fn pid_placement_fence() {
        use aruna_core::keyspaces::{PERSISTENT_ID_MAPPING_KEYSPACE, SHARD_MANIFEST_KEYSPACE};
        use aruna_core::storage_entries::shard_manifest_key;
        use aruna_core::structs::PersistentIdRevision;

        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([76; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(76).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");
        let minted_by = UserId::local(Ulid::from_parts(3_100, 1), realm_id);
        let actor = test_actor(76, minted_by, realm_id);
        assert_eq!(actor.node_id, local_node);

        let strategy_id = Ulid::from_parts(3_101, 1);
        let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(local_node, RealmNodeKind::Management);
        config.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        });
        config.strategies.push(PlacementStrategy {
            strategy_id,
            name: "placed".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        });
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config
                    .to_bytes(&actor)
                    .expect("realm config serializes")
                    .into(),
            )],
        )
        .await
        .expect("realm config writes");

        let document = |bucket: u16, seed: u64| {
            MetaResourceId::from_parts(seed, handle, BucketId::new(bucket).unwrap(), 1)
                .unwrap()
                .as_ulid()
        };
        let placed = |shard: u32| PlacementRef {
            strategy_id,
            epoch: 0,
            shard,
        };
        let mapping = |document_id, actor, seed: u64| {
            PersistentIdMapping::conceptual(
                document_id,
                minted_by,
                PersistentIdRevision {
                    event_id: Ulid::from_parts(seed, 1),
                    actor,
                    occurred_at_ms: 100 + seed,
                },
            )
        };

        let valid_document = document(4, 3_110);
        let forged_document = document(4, 3_111);
        let uncanonical_document = document(6, 3_112);
        let forged_actor_document = document(6, 3_113);
        let valid = mapping(valid_document, local_node, 3_120);
        let forged = mapping(forged_document, local_node, 3_121);
        let mut uncanonical = mapping(uncanonical_document, local_node, 3_122);
        uncanonical.pid = "https://w3id.org/aruna/not-this-document".to_string();
        let forged_actor = mapping(forged_actor_document, node(77), 3_123);

        let valid_topic = persistent_id_target(valid_document).sync_topic_id(realm_id, &placed(4));
        let forged_topic =
            persistent_id_target(forged_document).sync_topic_id(realm_id, &placed(9));
        let other_topic =
            persistent_id_target(uncanonical_document).sync_topic_id(realm_id, &placed(6));
        service
            .ensure_document_sync_topics(&[valid_topic, forged_topic, other_topic], Vec::new())
            .expect("mapping shard topic genesis");

        let publish =
            |mapping: &PersistentIdMapping, placement, event_id: u64| DocumentSyncPublish::Upsert {
                event_id: Ulid::from_parts(event_id, 1),
                target: persistent_id_target(mapping.target),
                bytes: mapping.to_bytes().expect("mapping serializes"),
                change: persistent_id_change(mapping, placement),
                allow_genesis: true,
            };
        let published = service
            .publish_documents(
                vec![
                    publish(&valid, placed(4), 3_130),
                    publish(&forged, placed(9), 3_131),
                    publish(&uncanonical, placed(6), 3_132),
                    publish(&forged_actor, placed(6), 3_133),
                ],
                Vec::new(),
            )
            .await;
        assert!(
            matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
            "mapping publish failed: {published:?}"
        );

        for topic_id in [valid_topic, forged_topic, other_topic] {
            reset_cursor(&service, topic_id).await;
        }
        let applied = service
            .reconcile_document_topics([valid_topic, forged_topic, other_topic])
            .await
            .expect("forged mappings are quarantined");

        assert_eq!(
            applied.targets,
            vec![persistent_id_target(valid_document)],
            "only the valid mapping applies"
        );
        assert!(
            read_storage_value(
                &storage,
                PERSISTENT_ID_MAPPING_KEYSPACE,
                ByteView::from(persistent_id_key(valid_document)),
            )
            .await
            .is_some()
        );
        assert!(
            read_storage_value(
                &storage,
                SHARD_MANIFEST_KEYSPACE,
                shard_manifest_key(&placed(4), &persistent_id_target(valid_document)),
            )
            .await
            .is_some()
        );
        for (document_id, placement) in [
            (forged_document, placed(9)),
            (uncanonical_document, placed(6)),
            (forged_actor_document, placed(6)),
        ] {
            assert!(
                read_storage_value(
                    &storage,
                    PERSISTENT_ID_MAPPING_KEYSPACE,
                    ByteView::from(persistent_id_key(document_id)),
                )
                .await
                .is_none(),
                "rejected mapping {document_id} reached storage"
            );
            assert!(
                read_storage_value(
                    &storage,
                    SHARD_MANIFEST_KEYSPACE,
                    shard_manifest_key(&placement, &persistent_id_target(document_id)),
                )
                .await
                .is_none(),
                "rejected mapping {document_id} reached the shard manifest"
            );
        }

        let records = quarantine_rows(&storage).await;
        assert_eq!(records.len(), 3, "{records:?}");
        assert_eq!(
            quarantined_reason(&records, Ulid::from_parts(3_131, 1)),
            "persistent id mapping has a mismatched placement configuration"
        );
        assert_eq!(
            quarantined_reason(&records, Ulid::from_parts(3_132, 1)),
            "invalid persistent id mapping: mapping pid \
             `https://w3id.org/aruna/not-this-document` is not canonical"
        );
        assert_eq!(
            quarantined_reason(&records, Ulid::from_parts(3_133, 1)),
            "persistent id mapping revision actor is not its publisher"
        );
        for topic_id in [valid_topic, forged_topic, other_topic] {
            assert!(cursor_advanced(&service, &storage, topic_id).await);
        }

        service.shutdown().await;
    }

    #[tokio::test]
    async fn capacity_holds_cursor() {
        let (_storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([73u8; 32]);
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(73).await,
            storage.clone(),
            doc_dir.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");

        let target = DocumentSyncTarget::NodeInfo {
            realm_id,
            node_id: local_node,
        };
        let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let published = service
            .publish_documents(
                vec![DocumentSyncPublish::Upsert {
                    event_id: Ulid::generate(),
                    target: target.clone(),
                    bytes: node_info_bytes(node(74), 5),
                    change: DocumentSyncChange {
                        base: None,
                        current: DocumentSyncRevision {
                            generation: 1,
                            event_id: Ulid::generate(),
                            actor: local_node,
                            updated_at_ms: 1,
                        },
                        kind: DocumentSyncChangeKind::Upsert,
                        placement: PlacementRef::NIL,
                    },
                    allow_genesis: true,
                }],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        // A full store fails the write closed: no evidence, no cursor movement.
        let full = SyncQuarantineUsage {
            records: SYNC_QUARANTINE_MAX_RECORDS,
            bytes: 0,
        };
        write_usage(&storage, full).await;
        reset_cursor(&service, topic_id).await;
        service
            .reconcile_document_topics([topic_id])
            .await
            .expect("a full quarantine store is not a reconcile failure");
        assert!(quarantine_rows(&storage).await.is_empty());
        assert_eq!(quarantine_usage(&storage).await, full);
        assert!(!cursor_advanced(&service, &storage, topic_id).await);

        // Reclaimed capacity lets the redelivered event persist and advance.
        write_usage(&storage, SyncQuarantineUsage::default()).await;
        service
            .reconcile_document_topics([topic_id])
            .await
            .expect("the redelivered event is quarantined");
        assert_eq!(quarantine_rows(&storage).await.len(), 1);
        assert_eq!(quarantine_usage(&storage).await.records, 1);
        assert!(cursor_advanced(&service, &storage, topic_id).await);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn shard_membership_exact() {
        let (_dir, storage) = test_storage();
        let doc = tempfile::tempdir().expect("document sync dir");
        let realm_id = restart_realm();
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(80).await,
            storage,
            doc.path().join("document-sync"),
            &[node(81), node(82)],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("document sync service opens");
        let local_node = service.local_node_id().expect("local node id");
        let current_node = node(81);
        let stale_node = node(82);
        let shard_topic = restart_topic();
        let shared_topic = DocumentSyncTarget::RealmConfig { realm_id }
            .sync_topic_id(realm_id, &PlacementRef::NIL);

        service
            .ensure_document_sync_topics(&[shard_topic], vec![current_node, stale_node])
            .expect("shard topic exists");
        service
            .ensure_document_sync_topics(&[shared_topic], vec![stale_node])
            .expect("shared topic exists");

        service
            .reconcile_shard_membership(
                &[shard_topic],
                vec![local_node, current_node],
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .await
            .expect("shard membership reconciles");

        let shard_state = service
            .node()
            .storage()
            .topic_state(&shard_topic)
            .expect("shard state reads")
            .expect("shard state exists");
        assert!(
            shard_state
                .members
                .contains(&node_id_to_peer_id(&local_node))
        );
        assert!(
            shard_state
                .members
                .contains(&node_id_to_peer_id(&current_node))
        );
        assert!(
            !shard_state
                .members
                .contains(&node_id_to_peer_id(&stale_node))
        );

        let shared_state = service
            .node()
            .storage()
            .topic_state(&shared_topic)
            .expect("shared state reads")
            .expect("shared state exists");
        assert!(
            shared_state
                .members
                .contains(&node_id_to_peer_id(&stale_node)),
            "exact shard reconciliation must not alter shared topics"
        );
        assert!(
            service
                .default_peers
                .read()
                .contains(&node_id_to_peer_id(&stale_node)),
            "former shard holders remain available as default network peers"
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn document_events_after_keeps_unapplied_dependency_of_covered_head() {
        use irokle_crate::{Ed25519Signer, Signer as _};

        let root = tempfile::tempdir().expect("temp dir");
        let service = open_restart_service(root.path(), "causal-cursor-storage").await;
        let local_node = service.local_node_id().expect("local node id");
        let remote_node = node(88);
        let topic_id = restart_topic();
        service
            .ensure_document_sync_topics(&[topic_id], vec![remote_node])
            .expect("shard topic exists");
        let oplog = Oplog::with_storage(service.node().storage().clone());
        let remote_signer = Ed25519Signer::from_bytes(&[88; 32]);
        let remote_event_id = Ulid::from_parts(1_727_000_000_000, 43);
        let local_event_id = Ulid::from_parts(1_727_000_000_000, 44);
        let change = |event_id, actor| DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id,
                actor,
                updated_at_ms: 1_727_000_000_101,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: restart_placement(),
        };
        let publish = |event_id, actor| DocumentSyncEvent::Upsert {
            event_id,
            target: restart_target(),
            bytes: restart_payload(),
            change: change(event_id, actor),
        };
        let remote_actor = irokle_crate::actor_id_for(topic_id, remote_signer.peer_id());
        oplog
            .create_event_op(
                topic_id,
                remote_actor,
                EventEnvelope::encode_event(&publish(remote_event_id, remote_node))
                    .expect("remote event encodes"),
                &remote_signer,
            )
            .expect("remote event publishes");
        let local_op = oplog
            .create_event_op(
                topic_id,
                irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&local_node)),
                EventEnvelope::encode_event(&publish(local_event_id, local_node))
                    .expect("local event encodes"),
                service.node().signer(),
            )
            .expect("local event publishes above remote head");
        let mut cursor = irokle_crate::ActorClock::default();
        cursor.observe(
            local_op.signed.body.actor_id,
            local_op.signed.body.actor_seq,
        );

        let events = service
            .document_events_after(topic_id, &cursor)
            .expect("unapplied document events read");
        let event_ids = events
            .into_iter()
            .filter_map(|(event, _, _)| match event {
                DocumentSyncEvent::Upsert { event_id, .. } => Some(event_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(event_ids, vec![remote_event_id]);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn replay_backlog() {
        let root = tempfile::tempdir().expect("temp dir");
        let service = open_restart_service(root.path(), "replay-batch-storage").await;
        let topic_id = restart_topic();
        let target = restart_target();
        service
            .ensure_document_sync_topics(&[topic_id], Vec::new())
            .expect("shard topic exists");

        let documents = (0..(DOCUMENT_SYNC_REPLAY_BATCH_LIMIT + 1))
            .map(|index| {
                let event_id = Ulid::from_parts(1_800_000_000_000 + index as u64, 1);
                DocumentSyncPublish::Upsert {
                    event_id,
                    target: target.clone(),
                    bytes: restart_payload(),
                    change: DocumentSyncChange {
                        base: None,
                        current: DocumentSyncRevision {
                            generation: 1,
                            event_id,
                            actor: service.local_node_id().expect("local node id"),
                            updated_at_ms: 1_800_000_000_000 + index as u64,
                        },
                        kind: DocumentSyncChangeKind::Upsert,
                        placement: restart_placement(),
                    },
                    allow_genesis: false,
                }
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            service.publish_documents(documents, Vec::new()).await,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));

        let cursor = irokle_crate::ActorClock::default();
        service
            .storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
                postcard::to_allocvec(&cursor)
                    .expect("cursor serializes")
                    .into(),
            )
            .await
            .expect("cursor resets");

        let first = service
            .document_event_batch(topic_id, &cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)
            .expect("first replay batch");
        assert_eq!(
            first.events.len(),
            DOCUMENT_SYNC_REPLAY_BATCH_LIMIT - 1,
            "genesis consumes one bounded replay slot"
        );
        let actor = irokle_crate::actor_id_for(topic_id, service.node().peer_id());
        assert_eq!(
            first.cursor.get(&actor),
            DOCUMENT_SYNC_REPLAY_BATCH_LIMIT as u64
        );
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock");
        assert!(!first.cursor.dominates(&topic_clock));

        // An interrupted run before the cursor write retries the same batch.
        let retry = service
            .document_event_batch(topic_id, &cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)
            .expect("retry replay batch");
        assert_eq!(retry.cursor, first.cursor);
        let remaining = service
            .document_event_batch(topic_id, &first.cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)
            .expect("remaining replay batch");
        assert!(remaining.cursor.dominates(&topic_clock));

        let byte_first = service
            .document_event_batch(topic_id, &cursor, 0)
            .expect("single operation byte batch");
        assert_eq!(byte_first.cursor.get(&actor), 1);
        let byte_second = service
            .document_event_batch(topic_id, &byte_first.cursor, 0)
            .expect("second operation byte batch");
        assert_eq!(byte_second.cursor.get(&actor), 2);
        assert!(!byte_second.cursor.dominates(&topic_clock));

        service.shutdown().await;
    }

    #[tokio::test]
    async fn stale_publisher_rejected() {
        let (_receiver_dir, receiver_storage) = test_storage();
        let (_publisher_dir, publisher_storage) = test_storage();
        let receiver_doc = tempfile::tempdir().expect("receiver document sync dir");
        let publisher_doc = tempfile::tempdir().expect("publisher document sync dir");
        let realm_id = restart_realm();
        let receiver = DocumentSyncService::open_with_persist_policy(
            test_endpoint(83).await,
            receiver_storage.clone(),
            receiver_doc.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("receiver opens");
        let publisher = DocumentSyncService::open_with_persist_policy(
            test_endpoint(84).await,
            publisher_storage,
            publisher_doc.path().join("document-sync"),
            &[],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("publisher opens");
        let receiver_node = receiver.local_node_id().expect("receiver node id");
        let publisher_node = publisher.local_node_id().expect("publisher node id");
        let topic_id = restart_topic();

        receiver
            .ensure_document_sync_topics(&[topic_id], vec![publisher_node])
            .expect("receiver creates shard topic");
        let receiver_ops = irokle_crate::oplog::topological(receiver.node().storage(), &topic_id)
            .expect("receiver history reads");
        publisher
            .node()
            .receive_sync_data_from_evicting(
                node_id_to_peer_id(&receiver_node),
                SyncData {
                    topic_id,
                    ops: receiver_ops,
                },
            )
            .expect("publisher adopts shard genesis");

        let published = publisher
            .publish_documents(
                vec![DocumentSyncPublish::Upsert {
                    event_id: restart_event_id(),
                    target: restart_target(),
                    bytes: restart_payload(),
                    change: revision_change(),
                    allow_genesis: false,
                }],
                vec![receiver_node],
            )
            .await;
        assert!(matches!(
            published,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));
        let publisher_ops = irokle_crate::oplog::topological(publisher.node().storage(), &topic_id)
            .expect("publisher history reads");
        receiver
            .reconcile_shard_membership(
                &[topic_id],
                vec![receiver_node],
                &BTreeSet::new(),
                &BTreeSet::from([topic_id]),
            )
            .await
            .expect("former holder is removed");
        receiver
            .node()
            .receive_sync_data_from_evicting(
                node_id_to_peer_id(&publisher_node),
                SyncData {
                    topic_id,
                    ops: publisher_ops,
                },
            )
            .expect("receiver admits the publisher's pre-removal causal history");
        let result = receiver
            .reconcile_document_topics([topic_id])
            .await
            .expect("stale publisher event is skipped");
        assert!(
            result.targets.is_empty(),
            "a former holder's shard event must not apply"
        );
        assert!(
            read_storage_value(
                &receiver_storage,
                restart_target().storage_keyspace(),
                restart_target().storage_key(),
            )
            .await
            .is_none(),
            "rejected event must not mutate document storage"
        );

        let cursor_bytes = read_storage_value(
            &receiver_storage,
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
            topic_cursor_key(topic_id),
        )
        .await
        .expect("cursor persisted");
        let cursor: irokle_crate::ActorClock =
            postcard::from_bytes(&cursor_bytes).expect("cursor decodes");
        let topic_clock = receiver
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("topic clock reads");
        assert!(
            cursor.dominates(&topic_clock),
            "rejected stale-holder event must not wedge reconciliation"
        );
        let retained_events =
            irokle_crate::oplog::topological(receiver.node().storage(), &topic_id)
                .expect("retained history reads")
                .into_iter()
                .filter(|op| matches!(&op.signed.body.payload, TopicPayload::Event(_)))
                .count();
        assert_eq!(
            retained_events, 1,
            "membership changes retain event history"
        );

        let records = quarantine_rows(&receiver_storage).await;
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].reason,
            "shard publisher is outside the current holder set"
        );
        assert_eq!(records[0].target(), Some(&restart_target()));
        assert_eq!(
            records[0]
                .decoded_event()
                .expect("event decodes")
                .placement(),
            restart_placement()
        );
        assert_eq!(quarantine_usage(&receiver_storage).await.records, 1);

        publisher.shutdown().await;
        receiver.shutdown().await;
    }

    #[tokio::test]
    async fn cutoff_gated_by_verification() {
        // An unverified shard freezes no former-holder history cutoff; a verified
        // shard does. The local clock is only a trustworthy cutover boundary once
        // the shard is durably verified.
        let (_dir, storage) = test_storage();
        let doc = tempfile::tempdir().expect("document sync dir");
        let realm_id = restart_realm();
        let service = DocumentSyncService::open_with_persist_policy(
            test_endpoint(85).await,
            storage,
            doc.path().join("document-sync"),
            &[node(86)],
            vec![Alpn::DocumentSync.as_bytes().to_vec()],
            irokle_crate::net::IrohRuntimeConfig::default(),
            FjallPersistPolicy::Buffer,
            realm_id,
        )
        .expect("service opens");
        let local_node = service.local_node_id().expect("local node id");
        let co_holder = node(86);
        let topic = restart_topic();
        service
            .ensure_document_sync_topics(&[topic], vec![co_holder])
            .expect("shard topic exists");

        service
            .reconcile_shard_membership(
                &[topic],
                vec![local_node, co_holder],
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .await
            .expect("membership reconciles");
        assert!(
            service
                .shard_publishers
                .read()
                .get(&topic)
                .expect("publisher policy installed")
                .history_cutoff
                .is_none(),
            "an unverified shard must not freeze a history cutoff"
        );

        service
            .reconcile_shard_membership(
                &[topic],
                vec![local_node, co_holder],
                &BTreeSet::new(),
                &BTreeSet::from([topic]),
            )
            .await
            .expect("membership reconciles");
        assert!(
            service
                .shard_publishers
                .read()
                .get(&topic)
                .expect("publisher policy installed")
                .history_cutoff
                .is_some(),
            "a verified shard must freeze a history cutoff"
        );

        service.shutdown().await;
    }
}
