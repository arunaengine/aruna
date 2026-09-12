use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ::irokle::Event as _;
use ::irokle::Storage as _;
use ::irokle::TopicControl;
use ::irokle::net::{decode_sync_message, encode_frame, encode_sync_message};
use ::irokle::oplog::Oplog;
use ::irokle::sync::{SyncData, SyncMessage, SyncRequest};
use ::irokle::{
    EventEnvelope, PeerId, ReplicationPolicy, TopicEviction, TopicGenesis, TopicPayload,
};
use aruna_core::DhtKeyId;
use aruna_core::MetaResourceId;
use aruna_core::NodeId;
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
use aruna_core::reducer::{
    AdminDocumentApplyStatus, AdminDocumentReducerState, GROUP_DISPLAY_NAME_PATH, GROUP_OWNER_PATH,
    GROUP_REALM_ID_PATH, MAX_LIVE_REVOCATIONS_PER_ORIGIN, REALM_CONFIG_COMPUTE_PATH,
    REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
    REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_POLICIES_PATH, REALM_CONFIG_QUOTA_PATH,
    RevocationIndex, USER_NAME_PATH, config_node_from_path, config_node_path,
    config_oidc_from_path, decode_reducer_state, group_assignment_from_path, group_role_from_path,
    group_role_path, group_user_path, overlay_placement, realm_assignment_from_path,
    realm_role_path, realm_user_path, user_attribute_path, user_subject_path,
};
use aruna_core::storage_entries::{
    conflict_write_entries, create_acceptance_entry, create_acceptance_key,
    create_projection_entries, document_lifecycle_entry, document_lifecycle_key,
    graph_lifecycle_entry, graph_lifecycle_key, graph_prune_entry, reducer_state_entry,
    reducer_state_key, registry_delete_entries, registry_write_entries, shard_manifest_entry,
    stale_conflict_deletes, subject_index_key, subject_index_value, sync_revision_entry,
    sync_revision_key,
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
    coordinator_spans, interest_dirty_key, interest_node_id, interest_realm_id, owner_index_key,
    persistent_id_change, persistent_id_key, persistent_id_target, placement_policy_change,
    placement_policy_target, quarantine_usage_entry, reserved_label, usage_node_id,
    verify_policy_authority,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::time::{unix_timestamp_millis, unix_timestamp_secs};
use aruna_core::types::{GroupId, RoleId, TxnId, UserId, Value};
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use byteview::ByteView;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

use crate::error::{NetError, Result};
use crate::streams::{BiStream, PeerKinds};

mod eviction;
mod inbound;
mod peers;
mod publish;
mod reconcile;
mod storage;
mod sync;
mod topics;

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
/// materialized only as the cursor advances past it, so both land in one
/// transaction; the identity is transport-derived, so undecodable payloads stay keyed.
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

    fn topic_id(&self) -> ::irokle::TopicId {
        self.identity.topic
    }
}

struct DocumentEventBatch {
    cursor: ::irokle::ActorClock,
    events: Vec<(DocumentSyncEvent, ::irokle::ActorId, u64)>,
    /// Ops whose transport payload never decoded into an event. They are
    /// permanent by construction: no redelivery can make the bytes valid.
    rejections: Vec<SyncRejection>,
}

/// One journalled eviction: the payloads Irokle removed with the losing chain,
/// plus the key that releases them once their outbox rows are committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingEviction {
    pub key: ::irokle::EvictionKey,
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
    published: BTreeMap<::irokle::TopicId, ::irokle::ActorClock>,
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
    current: BTreeSet<::irokle::ActorId>,
    // Existing history remains applicable across holder replacement. Once a
    // topic is local, only former-holder ops above this cutover clock are stale.
    history_cutoff: Option<::irokle::ActorClock>,
}

impl ShardPublisherPolicy {
    fn allows(&self, actor_id: &::irokle::ActorId, actor_seq: u64) -> bool {
        self.current.contains(actor_id)
            || self
                .history_cutoff
                .as_ref()
                .is_none_or(|cutoff| actor_seq <= cutoff.get(actor_id))
    }
}

/// Shard genesis probe outcome. A rank-0 holder may create a genesis only when
/// every co-holder was reached (`unreachable` empty), none advertised it, and
/// every reached co-holder confirmed unknown; otherwise creation is withheld.
#[derive(Clone, Debug, Default)]
pub struct ShardGenesisProbe {
    /// Probed topics at least one reached co-holder already has a genesis for.
    pub known_by_co_holder: BTreeSet<::irokle::TopicId>,
    /// Probed topics a reached co-holder neither advertised nor positively
    /// confirmed unknown: it holds the topic but the prober may not open it yet,
    /// so a fresh genesis would fork. Possibly-existing ⇒ creation withheld.
    pub unconfirmed: BTreeSet<::irokle::TopicId>,
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
    node: ::irokle::Irokle<::irokle::FjallStorage>,
    net: Arc<::irokle::net::IrohNet<::irokle::FjallStorage>>,
    db: fjall::OptimisticTxDatabase,
    fanout_cursors: fjall::OptimisticTxKeyspace,
    persist_policy: FjallPersistPolicy,
    storage: StorageHandle,
    default_peers: Arc<RwLock<BTreeSet<PeerId>>>,
    shard_publishers: Arc<RwLock<BTreeMap<::irokle::TopicId, ShardPublisherPolicy>>>,
    storage_path: PathBuf,
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
    // Genesis tie-break evictions from all admission paths funnel into this
    // sender; the embedder drains it once and re-emits the payloads.
    eviction_tx: tokio::sync::mpsc::UnboundedSender<TopicEviction>,
    eviction_rx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<TopicEviction>>>>,
    // Buckets each unreleased journal entry may re-emit onto, so a bucket's
    // drain need not rescan the journal; `None` marks an undecoded recovery.
    eviction_buckets: Arc<RwLock<BTreeMap<::irokle::EvictionKey, Option<Vec<PlacementRef>>>>>,
    // Realm this service serves; shard-classed targets carry no realm id of
    // their own, so their topic derivation reads it from here.
    realm_id: RealmId,
    inbound_budget: Arc<InboundSyncBudget>,
    // Peers configured at open; they admit inbound sync only during bootstrap,
    // after which only the current sync-eligible set is authoritative.
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
        runtime: ::irokle::net::IrohRuntimeConfig,
        realm_id: RealmId,
    ) -> Result<Self> {
        Self::open_with_policy(
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
    pub fn open_with_policy(
        endpoint: iroh::Endpoint,
        storage: StorageHandle,
        storage_path: impl AsRef<Path>,
        peer_nodes: &[NodeId],
        alpns: Vec<Vec<u8>>,
        runtime: ::irokle::net::IrohRuntimeConfig,
        persist_policy: FjallPersistPolicy,
        realm_id: RealmId,
    ) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let default_peers: BTreeSet<PeerId> = peer_nodes.iter().map(node_to_peer).collect();
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
        let node = ::irokle::Irokle::builder()
            .with_iroh_secret_key(endpoint.secret_key())
            .with_peer_whitelist(default_peers.clone())
            .with_fjall_database_and_persist_mode(db.clone(), persist_policy.as_fjall())
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .build()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let (eviction_tx, eviction_rx) = tokio::sync::mpsc::unbounded_channel();
        let net = Arc::new(
            ::irokle::net::IrohNet::new_with_alpns_config_and_sink(
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
        let eviction_buckets: BTreeMap<::irokle::EvictionKey, Option<Vec<PlacementRef>>> = node
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

    pub fn node(&self) -> ::irokle::Irokle<::irokle::FjallStorage> {
        self.node.clone()
    }

    #[cfg(test)]
    fn local_node_id(&self) -> Result<NodeId> {
        NodeId::from_bytes(self.node.peer_id().as_bytes())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    pub fn database(&self) -> fjall::OptimisticTxDatabase {
        self.db.clone()
    }

    pub async fn shutdown(&self) {
        self.net.shutdown().await;
        if let Err(error) = self.db.persist(fjall::PersistMode::SyncAll) {
            warn!(error = %error, "Failed to persist document sync database on shutdown");
        }
    }
}

fn node_to_peer(node_id: &NodeId) -> PeerId {
    PeerId::from_bytes(*node_id.as_bytes())
}

#[cfg(test)]
fn group_sync_topics<F>(
    topic_ids: &[::irokle::TopicId],
    mut select: F,
) -> BTreeMap<BTreeSet<PeerId>, (PeerSelection, Vec<::irokle::TopicId>)>
where
    F: FnMut(::irokle::TopicId) -> PeerSelection,
{
    let mut groups: BTreeMap<BTreeSet<PeerId>, (PeerSelection, Vec<::irokle::TopicId>)> =
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

#[cfg(test)]
mod tests;
