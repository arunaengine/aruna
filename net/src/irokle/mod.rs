use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use aruna_core::DhtKeyId;
use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{
    AdminDocumentApplyStatus, AdminDocumentReducerState, GROUP_DISPLAY_NAME_PATH, GROUP_OWNER_PATH,
    GROUP_REALM_ID_PATH, MAX_LIVE_REVOCATIONS_PER_ORIGIN, REALM_CONFIG_COMPUTE_PATH,
    REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
    REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_POLICIES_PATH, REALM_CONFIG_QUOTA_PATH,
    RevocationIndex, USER_NAME_PATH, decode_admin_document_reducer_state, group_role_id_from_path,
    group_role_path, group_role_user_assignment_from_path, group_role_user_assignment_path,
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
    NodeUsageSnapshot, PersistentIdKind, PersistentIdMapping, PersistentIdProvider,
    PersistentIdStatus, PlacementPolicyDocument, PlacementRef, PlacementScope, PoolAdmission,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind, Role,
    SYNC_QUARANTINE_USAGE_KEY, SyncQuarantineCapacity, SyncQuarantineError, SyncQuarantineEvidence,
    SyncQuarantineIdentity, SyncQuarantineInput, SyncQuarantineUsage, User, WatchEventMask,
    WatchInterestDigest, WatchSubscription, admit_band_pool, build_quarantine_entries,
    coordinator_spans, group_owner_index_key, node_usage_key_node_id, persistent_id_change,
    persistent_id_key, persistent_id_target, placement_policy_change, placement_policy_target,
    quarantine_usage_entry, reserved_label, verify_policy_authority, watch_interest_dirty_key,
    watch_interest_key_node_id, watch_interest_key_realm_id,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{GroupId, RoleId, TxnId, UserId, Value};
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
use tracing::{debug, error, info, warn};
use ulid::Ulid;

use crate::error::{NetError, Result};
use crate::streams::{BiStream, PeerKinds};

use ::irokle as irokle_crate;

mod reconcile;
mod storage;

use self::reconcile::*;
use self::storage::*;

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

/// One journalled eviction: the payloads Irokle removed with the losing chain,
/// plus the key that releases them once their outbox rows are committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingEviction {
    pub key: irokle_crate::EvictionKey,
    pub documents: Vec<DocumentSyncEvictedDocument>,
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
    // Buckets each unreleased journal entry may still re-emit onto, so a
    // placement drain can wait on its own bucket without rescanning the
    // journal. `None` marks an entry recovered at open but not yet decoded.
    eviction_buckets: Arc<RwLock<BTreeMap<irokle_crate::EvictionKey, Option<Vec<PlacementRef>>>>>,
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
    // Realm-config kind table shared with the accept loop, so the dial side
    // applies the same node-kind boundary. Empty until the embedder attaches it.
    peer_kinds: PeerKinds,
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
        // Seeded before any drain can observe the service: an entry left by an
        // interrupted handoff must block its bucket from the first tick on.
        let eviction_buckets: BTreeMap<irokle_crate::EvictionKey, Option<Vec<PlacementRef>>> = node
            .pending_evictions()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .iter()
            .map(|eviction| (eviction.key(), None))
            .collect();

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
            eviction_buckets: Arc::new(RwLock::new(eviction_buckets)),
            realm_id,
            inbound_budget: Arc::new(InboundSyncBudget::default()),
            configured_peers: default_peers,
            realm_config_materialized: Arc::new(AtomicBool::new(false)),
            peer_kinds: PeerKinds::default(),
        })
    }

    /// Shares the realm-config kind table so topic membership and sync fan-out
    /// apply the same node-kind boundary as the accept loop.
    pub(crate) fn set_peer_kinds(&mut self, peer_kinds: PeerKinds) {
        self.peer_kinds = peer_kinds;
    }

    /// Whether a peer may hold sync state: a user device never joins a topic and
    /// is never dialed for one. A peer whose kind is unknown stays eligible; the
    /// table is empty until realm config materializes.
    fn peer_is_eligible(&self, peer: &PeerId) -> bool {
        let Ok(node_id) = NodeId::from_bytes(peer.as_bytes()) else {
            return true;
        };
        self.peer_kinds
            .read()
            .get(&node_id)
            .is_none_or(RealmNodeKind::is_sync_eligible)
    }

    /// Dial-side mirror of the accept matrix: drops peers whose configured kind
    /// carries no sync responsibility.
    fn eligible_peers(
        &self,
        peers: impl IntoIterator<Item = PeerId>,
        topic_id: Option<irokle_crate::TopicId>,
    ) -> BTreeSet<PeerId> {
        peers
            .into_iter()
            .filter(|peer| {
                let eligible = self.peer_is_eligible(peer);
                if !eligible {
                    debug!(node_id = %peer, ?topic_id, "Skipping a document sync peer that is not sync eligible");
                }
                eligible
            })
            .collect()
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
    /// where the outbox format can carry them.
    ///
    /// Irokle's journal entry is the last local copy of these payloads, so each
    /// dropped class needs its own reason not to be re-emitted:
    ///
    /// * Control ops carry nothing to replay.
    /// * A whole-document admin payload cannot exist: `announce` refuses to
    ///   build one and `apply_upsert`/`apply_delete` refuse to apply one, so
    ///   re-emitting would only add an outbox row no peer can ever accept.
    pub fn decode_eviction(&self, eviction: TopicEviction) -> Vec<DocumentSyncEvictedDocument> {
        self.clear_cursor(eviction.topic_id);
        if let Err(error) = self.flush_database() {
            warn!(%error, topic_id = %eviction.topic_id, "Failed to persist document sync fan-out cursor reset");
        }
        let mut documents = Vec::new();
        for evicted in eviction.evicted {
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
                    origin_signature,
                } => {
                    documents.push(DocumentSyncEvictedDocument {
                        event_id: event.event_id,
                        target,
                        event: DocumentSyncOutboxEvent::AdminOperation {
                            event,
                            origin_signature: Some(origin_signature),
                        },
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
                        event_id,
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
                        event_id,
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

    /// Ensures topics this node holds alone: it mints what is missing and no
    /// peer joins their membership, so nothing about them is ever exchanged.
    pub fn ensure_local_topics(&self, topics: &[irokle_crate::TopicId]) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        let mut seen_topics = BTreeSet::new();
        for topic_id in topics.iter().copied() {
            if seen_topics.insert(topic_id) {
                self.ensure_topic(topic_id, &BTreeSet::new(), true)?;
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
                    origin_signature,
                    ..
                } => {
                    let origin_signature = match origin_signature {
                        Some(signature) => signature,
                        None => match self.sign_admin_event(&event, &placement) {
                            Ok(signature) => signature,
                            Err(error) => {
                                // Retained, never counted as published: deleting
                                // the outbox row would lose the mutation.
                                error!(
                                    event = "pipeline.publish.unsigned_admin",
                                    origin = %event.origin_node_id,
                                    %error,
                                    "Refusing to publish an admin event this node cannot sign"
                                );
                                outcome.retry_indices.push(index);
                                outcome.retry_error.get_or_insert_with(|| error.to_string());
                                continue;
                            }
                        },
                    };
                    DocumentSyncEvent::AdminOperation {
                        target,
                        event,
                        placement,
                        origin_signature,
                    }
                }
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

    /// Signs an admin envelope this node originated. A record carrying another
    /// origin must arrive already signed: re-signing here would substitute the
    /// relay's identity for the origin's.
    fn sign_admin_event(
        &self,
        event: &AdminDocumentEvent,
        placement: &PlacementRef,
    ) -> Result<iroh::Signature> {
        if node_id_to_peer_id(&event.origin_node_id) != self.node.peer_id() {
            return Err(NetError::PublisherUnauthorized(format!(
                "admin event originated by {} arrived unsigned",
                event.origin_node_id
            )));
        }
        let bytes = event
            .signing_bytes(placement)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let signature = irokle_crate::Signer::sign(self.node.signer(), &bytes)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        Ok(iroh::Signature::from_bytes(&signature.to_bytes()))
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
            // A tie-break between the publish and this write leaves the ops on a
            // chain that no longer exists; the next reconcile replays the winner.
            let Some(genesis) = self.topic_genesis(topic_id)? else {
                continue;
            };
            let cursor_key = topic_cursor_key(topic_id);
            let mut cursor = applied_cursor_clock(
                self.node.storage(),
                topic_id,
                genesis,
                self.storage_read(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key.clone(),
                )
                .await?,
            )?;
            cursor.merge(&clock);
            let value = applied_cursor_value(self.node.storage(), topic_id, genesis, &cursor)?;
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
                let missing_peers = self
                    .eligible_peers(peers.iter().copied(), Some(topic_id))
                    .into_iter()
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
                initial_peers: self.eligible_peers(peers.iter().copied(), Some(topic_id)),
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
        let candidates = if peers.is_empty() {
            self.default_peers.read().clone()
        } else {
            peers
                .into_iter()
                .map(|node_id| node_id_to_peer_id(&node_id))
                .collect()
        };
        let mut sync_peers = self.eligible_peers(candidates, None);
        sync_peers.remove(&self.node.peer_id());
        sync_peers
    }

    fn topic_genesis(&self, topic_id: irokle_crate::TopicId) -> Result<Option<irokle_crate::OpId>> {
        Ok(self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .map(|state| state.genesis))
    }

    fn next_sync_round(&self, topic_id: irokle_crate::TopicId) -> Result<u64> {
        let Some(genesis) = self.topic_genesis(topic_id)? else {
            return Ok(0);
        };
        current_cursor(&self.fanout_cursors, topic_id, genesis)
    }

    fn advance_cursor(&self, topic_id: irokle_crate::TopicId, round: u64) -> Result<()> {
        let Some(genesis) = self.topic_genesis(topic_id)? else {
            return Ok(());
        };
        advance_cursor(&self.fanout_cursors, topic_id, genesis, round)
    }

    fn clear_cursor(&self, topic_id: irokle_crate::TopicId) {
        if let Err(error) = remove_cursor(&self.fanout_cursors, topic_id) {
            warn!(%error, %topic_id, "Failed to clear document sync fan-out cursor");
        }
    }

    /// Closes a topic before a departing holder scans its journal and outbox.
    /// The result reports whether a journal entry still exists afterwards.
    pub fn close_topic(&self, topic_id: irokle_crate::TopicId) -> Result<bool> {
        self.node
            .seal_topic(topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.flush_database()?;
        Ok(self
            .node
            .pending_evictions()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .into_iter()
            .any(|eviction| eviction.topic_id == topic_id))
    }

    pub fn reopen_topic(&self, topic_id: irokle_crate::TopicId) -> Result<()> {
        let removed = self
            .node
            .unseal_topic(topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if removed {
            self.flush_database()?;
        }
        Ok(())
    }

    /// Decodes an eviction Irokle already journalled with its reset and drops
    /// the replaced chain's cursors. The journal remains until every replacement
    /// outbox row is committed.
    pub async fn consume_eviction(&self, eviction: TopicEviction) -> Option<PendingEviction> {
        let topic_id = eviction.topic_id;
        let key = eviction.key();
        // Irokle journals nothing for an eviction with no payloads, so treating
        // one as pending would arm the retry timer against a phantom entry.
        let journalled = !eviction.evicted.is_empty();
        let documents = self.decode_eviction(eviction);
        self.reset_applied_cursor(topic_id).await;
        if !journalled {
            return None;
        }
        self.eviction_buckets.write().insert(
            key,
            Some(
                documents
                    .iter()
                    .map(|document| document.placement)
                    .collect(),
            ),
        );
        Some(PendingEviction { key, documents })
    }

    /// Irokle journal entries left by an interrupted eviction handoff.
    pub async fn pending_evictions(&self) -> Result<Vec<PendingEviction>> {
        let evictions = self
            .node
            .pending_evictions()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let mut pending = Vec::with_capacity(evictions.len());
        for eviction in evictions {
            pending.extend(self.consume_eviction(eviction).await);
        }
        Ok(pending)
    }

    /// Releases a journal entry once every replacement outbox row is durable.
    pub async fn clear_eviction(&self, key: irokle_crate::EvictionKey) -> Result<()> {
        self.node
            .clear_eviction(&key)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.eviction_buckets.write().remove(&key);
        Ok(())
    }

    /// Whether a journalled eviction may still write replacement outbox rows for
    /// `placement`. An entry recovered at open blocks every bucket until the
    /// consumer decodes the buckets it actually targets.
    pub fn eviction_pending(&self, placement: &PlacementRef) -> bool {
        self.eviction_buckets
            .read()
            .values()
            .any(|buckets| buckets.as_ref().is_none_or(|list| list.contains(placement)))
    }

    /// Drops the applied-ops cursor of a topic whose chain was replaced by a
    /// genesis tie-break. The winning chain renumbers every actor sequence, so
    /// a cursor from the losing chain silently skips the winner's first ops.
    async fn reset_applied_cursor(&self, topic_id: irokle_crate::TopicId) {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        debug!(%topic_id, "Resetting document sync applied-ops cursor after a genesis tie-break");
        match self
            .storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                key: topic_cursor_key(topic_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {}
            other => {
                warn!(%topic_id, ?other, "Failed to reset document sync applied-ops cursor");
            }
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
        let candidates = if peers.is_empty() {
            self.default_peers.read().clone()
        } else {
            peers
                .iter()
                .copied()
                .map(|node_id| node_id_to_peer_id(&node_id))
                .collect()
        };
        Ok(select_sync_peers(
            self.eligible_peers(candidates, Some(*topic_id)),
            self.node.peer_id(),
            &subject,
            round,
        ))
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
            let refused: Vec<String> = known_topics
                .iter()
                .filter(|topic| !responded_topics.contains(*topic))
                .map(|topic| topic.to_string())
                .collect();
            return Err(NetError::Bootstrap(format!(
                "peer {peer} responded for {}/{} document sync batch topics (refused: {refused:?})",
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
        let (mut failed_topics, followup) = tokio::task::spawn_blocking(move || {
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
                    SyncMessage::Failure(failure) if known_topics.contains(&failure.topic_id) => {
                        failed_topics.insert(failure.topic_id);
                        warn!(
                            %peer,
                            topic_id = %failure.topic_id,
                            code = ?failure.code,
                            "Skipping document sync batch topic: peer rejected the sync ack"
                        );
                    }
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
                        .map_err(|error| {
                            report_journal_full(topic_id, &error);
                            NetError::Bootstrap(error.to_string())
                        })?;
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
            // The cursor self-heals here: an eviction callback that was lost or
            // failed cannot make a rebuilt chain's early ops look already
            // applied, because every recorded position is re-checked.
            let genesis = topic.genesis;
            let cursor_key = topic_cursor_key(topic_id);
            let mut cursor = applied_cursor_clock(
                self.node.storage(),
                topic_id,
                genesis,
                self.storage_read(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key.clone(),
                )
                .await?,
            )?;
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
            let mut config_run: Option<(DocumentSyncTarget, Vec<AdminDocumentEvent>)> = None;
            let mut validation_cache = ConfigValidationCache::default();
            for (event, actor_id, actor_seq) in batch.events {
                let identity = SyncQuarantineIdentity {
                    topic: topic_id,
                    actor: actor_id,
                    actor_seq,
                };
                // Any event outside the run must observe the run's state, so
                // the buffer flushes before anything else applies.
                let run_candidate = matches!(
                    &event,
                    DocumentSyncEvent::AdminOperation { target, event, .. }
                        if matches!(target, DocumentSyncTarget::RealmConfig { .. })
                            && coalescible_config_op(&event.op)
                );
                if !run_candidate {
                    flush_config_run(&self.storage, &mut config_run, &mut validation_cache).await?;
                }
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
                        let mapping = match PersistentIdMapping::from_bytes(&bytes) {
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
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target: DocumentSyncTarget::PlacementPolicy { policy_id },
                        bytes,
                        change,
                    } => {
                        let target = DocumentSyncTarget::PlacementPolicy { policy_id };
                        let reject = |reason: String| {
                            SyncRejection::new(
                                identity,
                                DocumentSyncEvent::Upsert {
                                    event_id,
                                    target: DocumentSyncTarget::PlacementPolicy { policy_id },
                                    bytes: bytes.clone(),
                                    change,
                                },
                                reason,
                            )
                        };
                        let document = match postcard::from_bytes::<PlacementPolicyDocument>(&bytes)
                        {
                            Ok(document) => document,
                            Err(error) => {
                                warn!(%topic_id, %policy_id, %error, "Rejecting undecodable placement policy document");
                                rejections.push(reject(format!(
                                    "undecodable placement policy document: {error}"
                                )));
                                continue;
                            }
                        };
                        if let Err(reason) =
                            validate_policy_document(policy_id, self.realm_id, &document, &change)
                        {
                            warn!(%topic_id, %policy_id, %reason, "Rejecting invalid placement policy document");
                            rejections.push(reject(format!(
                                "invalid placement policy document: {reason}"
                            )));
                            continue;
                        }
                        // The stored actor is the original publisher, so a relay
                        // that restates the document is not its author.
                        let expected_actor = irokle_crate::actor_id_for(
                            topic_id,
                            node_id_to_peer_id(&document.publication.publisher),
                        );
                        if actor_id != expected_actor {
                            warn!(
                                %topic_id,
                                %policy_id,
                                "Rejecting placement policy document whose actor is not its publisher"
                            );
                            rejections.push(reject(
                                "placement policy actor is not its publisher".to_string(),
                            ));
                            continue;
                        }
                        match self
                            .apply_policy_document(&document, change.placement)
                            .await?
                        {
                            MetadataPlacementOutcome::Accepted(true) => {
                                applied_targets.push(target)
                            }
                            MetadataPlacementOutcome::Accepted(false) => {}
                            MetadataPlacementOutcome::Deferred(dependency) => {
                                warn!(
                                    %topic_id,
                                    %policy_id,
                                    "Deferring placement policy document until its placement configuration is available"
                                );
                                cross_topic_dependencies.insert(dependency);
                            }
                            MetadataPlacementOutcome::Rejected => {
                                warn!(
                                    %topic_id,
                                    %policy_id,
                                    "Rejecting placement policy document with a mismatched placement or reused id"
                                );
                                rejections.push(reject(
                                    "placement policy document has a mismatched placement or reuses its id"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    event @ (DocumentSyncEvent::Delete {
                        target: DocumentSyncTarget::PlacementPolicy { .. },
                        ..
                    }
                    | DocumentSyncEvent::AdminOperation {
                        target: DocumentSyncTarget::PlacementPolicy { .. },
                        ..
                    }) => {
                        // A policy document is immutable, so it only ever syncs as
                        // an upsert; anything else is refused without wedging the
                        // topic.
                        warn!(
                            %topic_id,
                            target = ?event.target(),
                            "Skipping unsupported non-upsert placement policy event"
                        );
                        rejections.push(SyncRejection::new(
                            identity,
                            event,
                            "unsupported non-upsert placement policy event",
                        ));
                        continue;
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
                        origin_signature,
                    } => {
                        match validate_replicated_admin_event(
                            &self.storage,
                            topic_id,
                            actor_id,
                            &target,
                            &event,
                            self.realm_id,
                            &placement,
                            &origin_signature,
                            &mut validation_cache,
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
                                        origin_signature,
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
                                    target,
                                    *event,
                                    placement,
                                    identity,
                                    origin_signature,
                                    dependency,
                                    reason,
                                ));
                                continue;
                            }
                        }

                        let dependencies =
                            satisfied_document_sync_dependencies(&target, event.as_ref());
                        if matches!(target, DocumentSyncTarget::RealmConfig { .. })
                            && coalescible_config_op(&event.op)
                        {
                            match &mut config_run {
                                Some((run_target, events)) if *run_target == target => {
                                    events.push(*event);
                                }
                                run => {
                                    flush_config_run(&self.storage, run, &mut validation_cache)
                                        .await?;
                                    *run = Some((target.clone(), vec![*event]));
                                }
                            }
                        } else {
                            apply_admin_document_operation_to_storage(
                                &self.storage,
                                target.clone(),
                                *event,
                            )
                            .await?;
                            validation_cache.invalidate();
                        }
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
            flush_config_run(&self.storage, &mut config_run, &mut validation_cache).await?;
            let mut pending = deferred_admin_events;
            loop {
                let mut progressed = false;
                let mut retry = Vec::new();
                for (
                    target,
                    event,
                    placement,
                    identity,
                    signature,
                    _dependency,
                    _previous_reason,
                ) in pending
                {
                    match validate_replicated_admin_event(
                        &self.storage,
                        topic_id,
                        identity.actor,
                        &target,
                        &event,
                        self.realm_id,
                        &placement,
                        &signature,
                        &mut validation_cache,
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
                            validation_cache.invalidate();
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
                                    origin_signature: signature,
                                },
                                reason,
                            ));
                        }
                        AdminEventValidation::Deferred { dependency, reason } => retry.push((
                            target, event, placement, identity, signature, dependency, reason,
                        )),
                    }
                }
                if !progressed {
                    for (target, event, placement, identity, signature, dependency, reason) in retry
                    {
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
                                    origin_signature: signature,
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
            let value = applied_cursor_value(self.node.storage(), topic_id, genesis, &cursor)?;
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
}

const APPLY_CONFLICT_ATTEMPTS: usize = 64;

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
            | AdminDocumentOperation::RealmConfigNodeRemoved { .. }
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
            | AdminDocumentOperation::RealmConfigJobFamilySet { .. }
            | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
            | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
            | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
            | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
            | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
            | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
            | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
            | AdminDocumentOperation::RealmConfigPoliciesSet { .. }
            | AdminDocumentOperation::RealmConfigCandidateMapPublished { .. }
            | AdminDocumentOperation::RealmConfigActivationsInitialized { .. }
            | AdminDocumentOperation::RealmConfigTransitionStarted { .. }
            | AdminDocumentOperation::RealmConfigTransitionBarrierReported { .. }
            | AdminDocumentOperation::RealmConfigTransitionProofSubmitted { .. }
            | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
            | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. }
            | AdminDocumentOperation::RealmConfigTransitionStallReported { .. }
            | AdminDocumentOperation::RealmConfigTransitionDrainReported { .. }
            | AdminDocumentOperation::RealmConfigComputeSet { .. }
            | AdminDocumentOperation::RealmConfigTokenRevoked { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports node membership updates, OIDC provider updates, settings updates, description updates, quota updates, placement updates, transition updates, policy updates, compute updates, and token revocations"
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

    // A transient SSI conflict must never become stream-fatal: an aborted
    // inbound apply leaves ops without meta and wedges the topic. Local
    // interleavings are finite, so retry with yields; the bound stays a
    // safety valve against a genuine livelock.
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
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
                    unix_timestamp_millis(),
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
                    unix_timestamp_millis(),
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
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                abort_txn(storage, txn_id).await?;
            }
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        }
    }
    Err(NetError::Dht(
        "realm config admin operation conflict retries exhausted".to_string(),
    ))
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
    /// Roles of the group that owns a placement policy; without them its
    /// publication authority cannot be decided here.
    GroupAuthorization(GroupId),
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

/// Per-batch snapshot of the realm-config document and reducer state that
/// admin-event validation consults. Decoding both per event is quadratic over
/// a transition batch; a buffered run applies nothing, so the snapshot holds
/// until the caller applies events and invalidates it.
#[derive(Default)]
struct ConfigValidationCache {
    entry: Option<(
        RealmId,
        Option<RealmConfigDocument>,
        Option<AdminDocumentReducerState>,
    )>,
}

async fn read_group_authorization(
    storage: &StorageHandle,
    group_id: GroupId,
) -> Result<Option<GroupAuthorizationDocument>> {
    let target = DocumentSyncTarget::GroupAuthorization { group_id };
    storage_read_from(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
    )
    .await?
    .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

/// Validates one replicated administrative event. Authority comes from the
/// origin's signature over the envelope, never from the transport publisher: a
/// sync-eligible relay may carry another origin's event, but cannot forge,
/// re-target, or re-actor it. The transport publisher is still an authenticated
/// realm peer, checked by the caller's admission path.
#[allow(clippy::too_many_arguments)]
async fn validate_replicated_admin_event(
    storage: &StorageHandle,
    topic_id: irokle_crate::TopicId,
    authenticated_actor_id: irokle_crate::ActorId,
    target: &DocumentSyncTarget,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    placement: &PlacementRef,
    origin_signature: &iroh::Signature,
    config_cache: &mut ConfigValidationCache,
) -> Result<AdminEventValidation> {
    let reject = |reason: &str| Ok(AdminEventValidation::Rejected(reason.to_string()));

    if target.sync_topic_id(realm_id, placement) != topic_id {
        return reject("document sync target does not belong to the reconciled topic");
    }
    if event.origin_node_id != event.actor.node_id {
        return reject("event origin node does not match its actor node");
    }
    if !event.origin_signed(placement, origin_signature) {
        return reject("admin event is not signed by its origin node");
    }
    // A relay hop must itself be a realm node that may carry administrative
    // traffic. Before the config materializes only the origin may publish,
    // which is exactly the bootstrap case.
    let self_published = authenticated_actor_id
        == irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&event.origin_node_id));
    if !self_published
        && !relay_publisher_allowed(storage, topic_id, authenticated_actor_id, realm_id).await?
    {
        return reject("relayed admin event publisher is not a realm relay node");
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
        | AdminDocumentOperation::GroupDisplayNameSet { .. }
        | AdminDocumentOperation::GroupPoliciesSet { .. }
        | AdminDocumentOperation::GroupJoinRequested { .. }
        | AdminDocumentOperation::GroupJoinDecided { .. } => AdminOperationFamily::Group,
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
        | AdminDocumentOperation::RealmConfigNodeRemoved { .. }
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
        | AdminDocumentOperation::RealmConfigJobFamilySet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
        | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
        | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
        | AdminDocumentOperation::RealmConfigPoliciesSet { .. }
        | AdminDocumentOperation::RealmConfigCandidateMapPublished { .. }
        | AdminDocumentOperation::RealmConfigActivationsInitialized { .. }
        | AdminDocumentOperation::RealmConfigTransitionStarted { .. }
        | AdminDocumentOperation::RealmConfigTransitionBarrierReported { .. }
        | AdminDocumentOperation::RealmConfigTransitionProofSubmitted { .. }
        | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
        | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. }
        | AdminDocumentOperation::RealmConfigTransitionStallReported { .. }
        | AdminDocumentOperation::RealmConfigTransitionDrainReported { .. }
        | AdminDocumentOperation::RealmConfigComputeSet { .. }
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
        AdminDocumentOperation::GroupJoinRequested { .. }
        | AdminDocumentOperation::GroupJoinDecided { .. } => {}
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
        | AdminDocumentOperation::GroupDisplayNameSet { .. }
        | AdminDocumentOperation::RealmRoleAdded { .. }
        | AdminDocumentOperation::RealmRoleCreated { .. }
        | AdminDocumentOperation::UserAttributeSet { .. }
        | AdminDocumentOperation::UserAttributeRemoved { .. }
        | AdminDocumentOperation::UserNameSet { .. }
        | AdminDocumentOperation::UserSubjectIdAdded { .. }
        | AdminDocumentOperation::UserSubjectIdRemoved { .. }
        | AdminDocumentOperation::RealmConfigNodeEnsured { .. }
        | AdminDocumentOperation::RealmConfigNodeRemoved { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderUpserted { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderRemoved { .. }
        | AdminDocumentOperation::RealmConfigSettingsSet { .. }
        | AdminDocumentOperation::RealmConfigDescriptionSet { .. }
        | AdminDocumentOperation::RealmConfigQuotaSet { .. }
        | AdminDocumentOperation::RealmConfigNodePlacementRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { .. }
        | AdminDocumentOperation::RealmConfigDefaultStrategySet { .. }
        | AdminDocumentOperation::RealmConfigJobFamilySet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. } => {}
        AdminDocumentOperation::RealmConfigComputeSet { compute } => {
            // A malformed link or quota set would make every planner estimate
            // meaningless, so it is refused before it reaches storage.
            if compute.validate().is_err() {
                return reject("realm compute configuration is malformed");
            }
        }
        AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
        | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
        | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
        | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. } => {}
        AdminDocumentOperation::RealmConfigCandidateMapPublished { map } => {
            let mut seen = std::collections::BTreeSet::new();
            if map.epoch == 0 || !map.nodes.iter().all(|node| seen.insert(node.node_id)) {
                return reject("candidate placement map is malformed");
            }
        }
        AdminDocumentOperation::RealmConfigActivationsInitialized {
            candidate_map_epoch,
            ..
        } => {
            if *candidate_map_epoch == 0 {
                return reject("activation names no candidate map epoch");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionStarted { plan } => {
            let mut seen = std::collections::BTreeSet::new();
            let well_formed = plan.limits.max_incomplete_buckets >= 1
                && plan.target_map_epoch > 0
                && !plan.buckets.is_empty()
                && plan
                    .buckets
                    .iter()
                    .all(|bucket| seen.insert(bucket.bucket) && !bucket.target_holders.is_empty());
            if !well_formed {
                return reject("placement transition plan is malformed");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionBarrierReported {
            reported_by,
            frontier,
            ..
        } => {
            if *reported_by != event.origin_node_id {
                return reject("transition report does not come from the node it names");
            }
            if frontier.len() > aruna_core::structs::MAX_BARRIER_FRONTIER_BYTES {
                return reject("transition barrier frontier exceeds its size bound");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionStallReported {
            reported_by,
            reason,
            ..
        } => {
            if *reported_by != event.origin_node_id {
                return reject("transition report does not come from the node it names");
            }
            if reason.len() > aruna_core::structs::MAX_STALL_REASON_BYTES {
                return reject("transition stall reason exceeds its size bound");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionDrainReported { reported_by, .. } => {
            if *reported_by != event.origin_node_id {
                return reject("transition report does not come from the node it names");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
            transition_id,
            strategy_id,
            proof,
        } => {
            // Verified here as well as in the reducer: a forged proof must never
            // reach storage, and the publisher binding already fixed the origin.
            if proof.holder != event.origin_node_id
                || !proof.verify(event.actor.realm_id, *transition_id, *strategy_id)
            {
                return reject("transition completion proof does not verify");
            }
        }
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

    let previous_state = match family {
        AdminOperationFamily::RealmConfig => {
            let AdminDocumentTarget::RealmConfig {
                realm_id: event_realm_id,
            } = event.target
            else {
                return reject("admin event target is not a realm config");
            };
            let (current_config, cached_state) = config_cache.load(storage, event_realm_id).await?;
            let authorized = validate_config_authority(current_config, event, cached_state)?;
            if !matches!(authorized, AdminEventValidation::Accepted) {
                return Ok(authorized);
            }
            cached_state.cloned()
        }
        family => {
            let previous_state = read_admin_reducer_state(storage, &event.target).await?;
            let authorized = match family {
                AdminOperationFamily::RealmAuthorization => {
                    validate_realm_authorization_admin_authority(
                        storage,
                        event,
                        previous_state.as_ref(),
                    )
                    .await?
                }
                AdminOperationFamily::Group => {
                    validate_group_admin_authority(storage, event, previous_state.as_ref()).await?
                }
                _ => validate_user_admin_authority(storage, event, previous_state.as_ref()).await?,
            };
            if !matches!(authorized, AdminEventValidation::Accepted) {
                return Ok(authorized);
            }
            previous_state
        }
    };

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

/// Who a transition report claims to be, against the named plan's roles.
enum ReportParticipation {
    NotReport,
    Participant,
    UnknownPlan,
    Foreign,
}

/// An applied-ops cursor together with the history it describes: the genesis it
/// was built from, and the op that occupied each actor position when it was
/// written. A genesis tie-break replaces the chain and an orphan quarantine
/// rebuilds it in place under the same genesis, but both renumber actor
/// sequences, so the position alone is never evidence that its op still stands.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct AppliedCursor {
    lineage: irokle_crate::OpId,
    clock: irokle_crate::ActorClock,
    marks: BTreeMap<irokle_crate::ActorId, irokle_crate::OpId>,
}

const FANOUT_CURSOR_LEN: usize = irokle_crate::OpId::LEN + std::mem::size_of::<u64>();

/// Per-peer outcome of probing shard topics: which topics the peer holds a
/// genesis for (`known`) and which it positively confirmed it has none of
/// (`confirmed_unknown`). A probed topic in neither was refused — the peer holds
/// it but the prober may not open it yet, so it must not be treated as unknown.
#[derive(Debug, Default, PartialEq, Eq)]
struct PeerTopicProbe {
    known: BTreeSet<irokle_crate::TopicId>,
    confirmed_unknown: BTreeSet<irokle_crate::TopicId>,
}

#[cfg(test)]
mod tests;
