use std::cmp::Ordering;

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::admin_documents::AdminDocumentEvent;
use crate::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, DOCUMENT_LIFECYCLE_KEYSPACE,
    EVENT_LOG_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE,
    NODE_INFO_KEYSPACE, WATCH_INTEREST_KEYSPACE,
    WATCH_SUBSCRIPTIONS_KEYSPACE, ID_MAPPING_KEYSPACE,
    PLACEMENT_POLICY_KEYSPACE, REALM_CONFIG_KEYSPACE, NODE_STATS_KEYSPACE, USER_KEYSPACE,
};
use crate::metadata::{GraphLifecycleRecord, MetadataEventRecord};
use crate::storage_entries::{document_lifecycle_key, event_log_key, graph_lifecycle_key};
use crate::structs::placement::placement_record::{PLACEMENT_EPOCH_PAD, PlacementRef};
use crate::structs::identity::realm::RealmId;
use crate::structs::execution::notification_watch::{interest_node_key, watch_subscription_key};
use crate::structs::storage::node_info::node_info_key;
use crate::structs::persistent_id_key;
use crate::structs::placement::policy_document::placement_policy_key;
use crate::structs::storage::usage::{usage_global_key, usage_snapshot_key};
use crate::types::{GroupId, Key};
use crate::{NodeId, TopicId};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DocumentTarget {
    Group {
        group_id: GroupId,
    },
    GroupAuthorization {
        group_id: GroupId,
    },
    RealmAuthorization {
        realm_id: RealmId,
    },
    RealmConfig {
        realm_id: RealmId,
    },
    User {
        user_id: UserId,
    },
    MetadataRegistry {
        group_id: GroupId,
        document_id: Ulid,
    },
    MetadataCreateEvent {
        document_id: Ulid,
        event_id: Ulid,
    },
    MetadataDocumentLifecycle {
        document_id: Ulid,
    },
    MetadataGraphLifecycle {
        graph_iri: String,
    },
    /// The document's w3id PID mapping. Rides the document-lifecycle placement so the PID authority is
    /// co-located with the document's holders, but keeps its own keyspace: the registry row is deleted with
    /// the document while the mapping must survive it to serve a permanent 410.
    PersistentIdMapping {
        document_id: Ulid,
    },
    NodeUsage {
        realm_id: RealmId,
        node_id: NodeId,
        group_id: Option<GroupId>,
    },
    WatchInterest {
        realm_id: RealmId,
        node_id: NodeId,
    },
    WatchSubscription {
        owner: UserId,
        watch_id: Ulid,
    },
    NodeInfo {
        realm_id: RealmId,
        node_id: NodeId,
    },
    /// One immutable placement-policy document. Placed by its policy id alone,
    /// so a reader resolves the rule's holders from a ref without any catalog.
    PlacementPolicy {
        policy_id: Ulid,
    },
}

/// A shard whose sync topic the local node is an authoritative holder of and whose co-holder membership
/// is still being topped up. Keyed by realm ‖ strategy ‖ pad ‖ shard(be); one record per shard, not per
/// document (every document in the shard rides the same topic).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingShardPlacement {
    pub realm_id: RealmId,
    pub placement: PlacementRef,
    pub selected_peers: Vec<NodeId>,
    pub updated_at: u64,
    pub authoritative_node_id: NodeId,
}

/// One held shard document and revision. Deletes remain as tombstone rows matching lifecycle sidecars.
/// Per-entry keys avoid a read-modify-write blob on the hot write path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardManifestEntry {
    pub target: DocumentTarget,
    pub revision: DocumentSyncRevision,
}

/// Authoritative shard inventory, topic digest, cursor, and provenance assembled from local state.
/// It is fetched from a co-holder over shard ALPN and is never document-synchronized.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardManifest {
    pub placement: PlacementRef,
    pub holder: NodeId,
    pub entries: Vec<ShardManifestEntry>,
    pub cursor: Vec<u8>,
    pub digest: [u8; 32],
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentOutboxRecord {
    pub outbox_id: Ulid,
    pub node_id: NodeId,
    pub target: DocumentTarget,
    pub peers: Vec<NodeId>,
    pub event: DocumentOutboxEvent,
    /// Placement reference this record rides under: for `Upsert`/`Delete` the
    /// envelope change's ref, for `AdminOperation` the target's resolved ref.
    /// Does not affect the outbox FIFO key.
    pub placement: PlacementRef,
    /// Activation generation the write was admitted at, so a departing holder
    /// drains exactly the finite set its closed fence bounds. `0` means the
    /// writer took no fence and the row always counts toward the drain.
    pub generation: u64,
    pub updated_at: u64,
    /// Whether the publisher may mint this document's sync topic genesis when it
    /// is missing. Only the node that originated the document sets this; every
    /// other publisher waits (retryable) for the origin's genesis to replicate.
    pub allow_genesis: bool,
}

impl DocumentOutboxRecord {
    /// Stamps the generation the bucket's write fence admitted this row at.
    pub fn fenced_at(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentOutboxEvent {
    Upsert {
        bytes: Vec<u8>,
        change: DocumentChange,
    },
    AdminOperation {
        event: Box<AdminDocumentEvent>,
        /// Set only on a relayed record, where this node is not the origin and
        /// forwards the origin's signature verbatim. `None` means the local node
        /// is the origin and signs the envelope when it publishes.
        origin_signature: Option<iroh::Signature>,
    },
    Delete {
        change: DocumentChange,
    },
}

impl DocumentOutboxEvent {
    /// Admin record originated by this node; the publisher signs the envelope.
    pub fn admin(event: AdminDocumentEvent) -> Self {
        Self::AdminOperation {
            event: Box::new(event),
            origin_signature: None,
        }
    }

    /// Admin record accepted from another origin, kept exactly as signed.
    pub fn relayed_admin(event: AdminDocumentEvent, origin_signature: iroh::Signature) -> Self {
        Self::AdminOperation {
            event: Box::new(event),
            origin_signature: Some(origin_signature),
        }
    }
}

/// A payload recovered from a genesis tie-break, journalled until its replacement outbox row is
/// durable. `event_id` is the evicted event's own id, so repeating the recovery rewrites one stable
/// outbox row instead of adding a duplicate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentEvictedDocument {
    pub event_id: Ulid,
    pub target: DocumentTarget,
    pub event: DocumentOutboxEvent,
    pub placement: PlacementRef,
    pub allow_genesis: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DocumentSyncRevision {
    pub generation: u64,
    pub event_id: Ulid,
    pub actor: NodeId,
    pub updated_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentChange {
    pub base: Option<DocumentSyncRevision>,
    pub current: DocumentSyncRevision,
    pub kind: DocumentChangeKind,
    pub placement: PlacementRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSyncConflict {
    pub target: DocumentTarget,
    pub local_change: Option<DocumentChange>,
    pub local_bytes: Option<Vec<u8>>,
    pub incoming_change: DocumentChange,
    pub incoming_bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentChangeKind {
    Upsert,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentApplyDecision {
    Apply,
    SkipStale,
    SkipTombstoned,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentSyncPublish {
    Upsert {
        event_id: Ulid,
        target: DocumentTarget,
        bytes: Vec<u8>,
        change: DocumentChange,
        allow_genesis: bool,
    },
    AdminOperation {
        target: DocumentTarget,
        event: Box<AdminDocumentEvent>,
        placement: PlacementRef,
        allow_genesis: bool,
        /// `None` when the local node is the origin and signs at publish time.
        origin_signature: Option<iroh::Signature>,
    },
    Delete {
        event_id: Ulid,
        target: DocumentTarget,
        change: DocumentChange,
        allow_genesis: bool,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentReconcileResult {
    pub targets: Vec<DocumentTarget>,
    pub metadata_create_events: Vec<MetadataEventRecord>,
    pub metadata_graph_tombstones: Vec<GraphLifecycleRecord>,
}

impl DocumentReconcileResult {
    pub fn applied(&self) -> usize {
        self.targets.len()
    }
}

impl DocumentSyncPublish {
    pub fn target(&self) -> &DocumentTarget {
        match self {
            Self::Upsert { target, .. }
            | Self::Delete { target, .. }
            | Self::AdminOperation { target, .. } => target,
        }
    }

    pub fn event_id(&self) -> Ulid {
        match self {
            Self::Upsert { event_id, .. } | Self::Delete { event_id, .. } => *event_id,
            Self::AdminOperation { event, .. } => event.event_id,
        }
    }

    pub fn allow_genesis(&self) -> bool {
        match self {
            Self::Upsert { allow_genesis, .. }
            | Self::Delete { allow_genesis, .. }
            | Self::AdminOperation { allow_genesis, .. } => *allow_genesis,
        }
    }
}

impl DocumentOutboxEvent {
    pub fn kind(&self) -> &'static [u8] {
        match self {
            Self::Upsert { .. } => b"upsert",
            Self::Delete { .. } => b"delete",
            Self::AdminOperation { .. } => b"admin-operation",
        }
    }
}

pub fn compare_sync_revisions(
    local: &DocumentSyncRevision,
    remote: &DocumentSyncRevision,
) -> Ordering {
    local.cmp(remote)
}

pub fn sync_apply_decision(
    local: Option<&DocumentChange>,
    incoming: &DocumentChange,
) -> DocumentApplyDecision {
    let Some(local) = local else {
        return DocumentApplyDecision::Apply;
    };

    if incoming.current == local.current {
        return if incoming.kind == local.kind {
            DocumentApplyDecision::Apply
        } else {
            DocumentApplyDecision::Conflict
        };
    }

    if local.kind == DocumentChangeKind::Delete
        && incoming.kind == DocumentChangeKind::Upsert
        && incoming.base.as_ref() != Some(&local.current)
    {
        return DocumentApplyDecision::SkipTombstoned;
    }

    match incoming.current.generation.cmp(&local.current.generation) {
        Ordering::Less => DocumentApplyDecision::SkipStale,
        Ordering::Equal => DocumentApplyDecision::Conflict,
        Ordering::Greater if incoming.base.as_ref() == Some(&local.current) => {
            DocumentApplyDecision::Apply
        }
        Ordering::Greater => DocumentApplyDecision::Conflict,
    }
}

impl DocumentTarget {
    /// Admin documents (user, group, and realm authorization/config) replicate
    /// only as `AdminOperation` events over their shared topic; they never take
    /// placements or sync as whole documents.
    pub fn is_admin_document(&self) -> bool {
        matches!(
            self,
            Self::Group { .. }
                | Self::GroupAuthorization { .. }
                | Self::RealmAuthorization { .. }
                | Self::RealmConfig { .. }
                | Self::User { .. }
        )
    }

    pub fn topic_id(&self) -> TopicId {
        match self {
            Self::Group { group_id } | Self::GroupAuthorization { group_id } => {
                TopicId::group(*group_id)
            }
            Self::RealmAuthorization { realm_id } | Self::RealmConfig { realm_id } => {
                TopicId::realm(*realm_id)
            }
            Self::User { user_id } => TopicId::users(user_id.realm_id),
            Self::MetadataRegistry { document_id, .. }
            | Self::MetadataCreateEvent { document_id, .. }
            | Self::MetadataDocumentLifecycle { document_id }
            | Self::PersistentIdMapping { document_id } => TopicId::metadata(*document_id),
            Self::MetadataGraphLifecycle { graph_iri } => {
                TopicId::metadata(graph_lifecycle_topic(graph_iri))
            }
            Self::NodeUsage { realm_id, .. } => TopicId::realm(*realm_id),
            Self::WatchInterest { realm_id, .. } => TopicId::realm(*realm_id),
            Self::WatchSubscription { owner, .. } => TopicId::realm(owner.realm_id),
            Self::NodeInfo { realm_id, .. } => TopicId::realm(*realm_id),
            Self::PlacementPolicy { policy_id } => TopicId::metadata(*policy_id),
        }
    }

    pub fn storage_keyspace(&self) -> &'static str {
        match self {
            Self::Group { .. } => GROUP_KEYSPACE,
            Self::GroupAuthorization { .. } | Self::RealmAuthorization { .. } => AUTH_KEYSPACE,
            Self::RealmConfig { .. } => REALM_CONFIG_KEYSPACE,
            Self::User { .. } => USER_KEYSPACE,
            Self::MetadataRegistry { .. } => METADATA_INDEX_KEYSPACE,
            Self::MetadataCreateEvent { .. } => EVENT_LOG_KEYSPACE,
            Self::MetadataDocumentLifecycle { .. } => DOCUMENT_LIFECYCLE_KEYSPACE,
            Self::MetadataGraphLifecycle { .. } => GRAPH_LIFECYCLE_KEYSPACE,
            Self::PersistentIdMapping { .. } => ID_MAPPING_KEYSPACE,
            Self::NodeUsage { .. } => NODE_STATS_KEYSPACE,
            Self::WatchInterest { .. } => WATCH_INTEREST_KEYSPACE,
            Self::WatchSubscription { .. } => WATCH_SUBSCRIPTIONS_KEYSPACE,
            Self::NodeInfo { .. } => NODE_INFO_KEYSPACE,
            Self::PlacementPolicy { .. } => PLACEMENT_POLICY_KEYSPACE,
        }
    }

    pub fn storage_key(&self) -> Key {
        match self {
            Self::Group { group_id } | Self::GroupAuthorization { group_id } => {
                ByteView::from(group_id.to_bytes().to_vec())
            }
            Self::RealmAuthorization { realm_id } | Self::RealmConfig { realm_id } => {
                ByteView::from(realm_id.as_bytes().to_vec())
            }
            Self::User { user_id } => ByteView::from(user_id.to_bytes()),
            Self::MetadataRegistry {
                group_id,
                document_id,
            } => {
                let mut bytes = Vec::with_capacity(32);
                bytes.extend_from_slice(&group_id.to_bytes());
                bytes.extend_from_slice(&document_id.to_bytes());
                ByteView::from(bytes)
            }
            Self::MetadataCreateEvent {
                document_id,
                event_id,
            } => event_log_key(*document_id, *event_id),
            Self::MetadataDocumentLifecycle { document_id } => document_lifecycle_key(*document_id),
            Self::MetadataGraphLifecycle { graph_iri } => graph_lifecycle_key(graph_iri),
            Self::PersistentIdMapping { document_id } => {
                ByteView::from(persistent_id_key(*document_id))
            }
            Self::NodeUsage {
                node_id, group_id, ..
            } => match group_id {
                Some(group_id) => ByteView::from(usage_snapshot_key(*group_id, *node_id)),
                None => ByteView::from(usage_global_key(*node_id)),
            },
            Self::WatchInterest { realm_id, node_id } => {
                ByteView::from(interest_node_key(*realm_id, *node_id))
            }
            Self::WatchSubscription { owner, watch_id } => {
                watch_subscription_key(*owner, *watch_id)
            }
            Self::NodeInfo { node_id, .. } => ByteView::from(node_info_key(*node_id)),
            Self::PlacementPolicy { policy_id } => ByteView::from(placement_policy_key(*policy_id)),
        }
    }

    /// Whether this target's records ride a shard topic (group, user, metadata classes) rather than a
    /// shared realm-scoped domain topic. Shard topics are join-only for everyone but the shard's rank-0
    /// holder, which creates the genesis eagerly.
    pub fn uses_shard_topic(&self) -> bool {
        matches!(
            self,
            Self::Group { .. }
                | Self::GroupAuthorization { .. }
                | Self::User { .. }
                | Self::MetadataRegistry { .. }
                | Self::MetadataCreateEvent { .. }
                | Self::MetadataDocumentLifecycle { .. }
                | Self::MetadataGraphLifecycle { .. }
                | Self::PersistentIdMapping { .. }
                | Self::PlacementPolicy { .. }
        )
    }

    /// Returns a placement-derived topic for shard-classed targets and a shared domain topic otherwise.
    /// A missing shard placement is an emitter bug: debug builds assert and release builds warn.
    pub fn sync_topic_id(&self, realm_id: RealmId, placement: &PlacementRef) -> irokle::TopicId {
        if self.uses_shard_topic() {
            if *placement == PlacementRef::NIL {
                debug_assert!(
                    false,
                    "shard-classed target {self:?} derived a topic from a NIL placement"
                );
                tracing::warn!(
                    target = ?self,
                    "shard-classed target has a NIL placement; deriving a NIL shard topic"
                );
            }
            shard_topic_id(realm_id, placement)
        } else {
            self.shared_topic_id()
        }
    }

    fn shared_topic_id(&self) -> irokle::TopicId {
        let mut bytes = b"aruna-document-topic-v1".to_vec();
        bytes.extend_from_slice(&self.topic_id().to_bytes());
        match self {
            Self::RealmAuthorization { .. } => bytes.extend_from_slice(b"/realm-auth"),
            Self::RealmConfig { .. } => bytes.extend_from_slice(b"/realm-config"),
            // No node id in the suffix: every node's usage snapshot flows over a
            // single shared realm-scoped topic that all realm nodes subscribe to.
            Self::NodeUsage { .. } => bytes.extend_from_slice(b"/node-usage"),
            // Likewise realm-shared: every node's watch-interest digest rides one
            // topic so origin nodes receive all holders' interest.
            Self::WatchInterest { .. } | Self::WatchSubscription { .. } => {
                bytes.extend_from_slice(b"/watch-interest")
            }
            // Realm-shared: every node's info/heartbeat document rides one topic
            // (no node id in the suffix) so all realm nodes receive every peer's.
            Self::NodeInfo { .. } => bytes.extend_from_slice(b"/node-info"),
            // Realm-shared: every node subscribes so an access key created on any
            // node replicates to all, making the credential valid realm-wide.
            other => {
                debug_assert!(false, "shared_topic_id on shard-classed target {other:?}");
                bytes.extend_from_slice(b"/shard-misroute");
            }
        }
        irokle::TopicId::hash(bytes)
    }
}

/// Derives a stable topic from realm, strategy, and shard without network-layer config lookup.
/// Holder or epoch turnover does not change it; all shard-classed targets use this derivation.
pub fn shard_topic_id(realm_id: RealmId, placement: &PlacementRef) -> irokle::TopicId {
    let mut bytes = b"aruna-shard-topic-v1".to_vec();
    bytes.extend_from_slice(realm_id.as_bytes());
    bytes.extend_from_slice(&placement.strategy_id.to_bytes());
    bytes.extend_from_slice(&PLACEMENT_EPOCH_PAD);
    bytes.extend_from_slice(&placement.shard.to_be_bytes());
    irokle::TopicId::hash(bytes)
}

fn graph_lifecycle_topic(graph_iri: &str) -> Ulid {
    let hash = blake3::hash(graph_iri.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, irokle::Event)]
#[irokle(type_id = "aruna.document.v3")]
pub enum DocumentEvent {
    Upsert {
        event_id: Ulid,
        target: DocumentTarget,
        bytes: Vec<u8>,
        change: DocumentChange,
    },
    AdminOperation {
        target: DocumentTarget,
        event: Box<AdminDocumentEvent>,
        placement: PlacementRef,
        /// The origin's signature over the envelope. Carried on the wire so a
        /// relay hop preserves origin authority instead of substituting its own.
        origin_signature: iroh::Signature,
    },
    Delete {
        event_id: Ulid,
        target: DocumentTarget,
        change: DocumentChange,
    },
}

impl DocumentEvent {
    pub fn target(&self) -> &DocumentTarget {
        match self {
            Self::Upsert { target, .. }
            | Self::Delete { target, .. }
            | Self::AdminOperation { target, .. } => target,
        }
    }

    pub fn event_id(&self) -> Ulid {
        match self {
            Self::Upsert { event_id, .. } | Self::Delete { event_id, .. } => *event_id,
            Self::AdminOperation { event, .. } => event.event_id,
        }
    }

    /// Placement the event rides under: the envelope change's ref for
    /// `Upsert`/`Delete`, the stamped admin ref for `AdminOperation`. Feeds
    /// [`DocumentTarget::sync_topic_id`] on both publish and reconcile.
    pub fn placement(&self) -> PlacementRef {
        match self {
            Self::Upsert { change, .. } | Self::Delete { change, .. } => change.placement,
            Self::AdminOperation { placement, .. } => *placement,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentEffect {
    PublishDocuments {
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    },
    SyncDocument {
        topic: irokle::TopicId,
        peers: Vec<NodeId>,
    },
    SyncDocuments {
        topics: Vec<irokle::TopicId>,
        peers: Vec<NodeId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentNetEvent {
    DocumentsPublished {
        targets: Vec<DocumentTarget>,
    },
    DocumentsPartiallyPublished {
        published_indices: Vec<usize>,
        retry_indices: Vec<usize>,
        error: String,
    },
    DocumentsReconciled {
        applied: usize,
        targets: Vec<DocumentTarget>,
        metadata_create_events: Vec<MetadataEventRecord>,
        metadata_graph_tombstones: Vec<GraphLifecycleRecord>,
    },
    Error {
        target: Option<DocumentTarget>,
        error: String,
    },
}

#[cfg(test)]
#[path = "document_tests.rs"]
mod tests;
