use std::cmp::Ordering;

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::admin_documents::AdminDocumentEvent;
use crate::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE,
    NODE_INFO_KEYSPACE, NOTIFICATION_WATCH_INTEREST_KEYSPACE, REALM_CONFIG_KEYSPACE,
    USAGE_NODE_STATS_KEYSPACE, USER_KEYSPACE,
};
use crate::metadata::{MetadataCreateEventRecord, MetadataGraphLifecycleRecord};
use crate::storage_entries::{
    metadata_document_lifecycle_key, metadata_event_log_key, metadata_graph_lifecycle_key,
};
use crate::structs::{
    PlacementRef, RealmId, node_info_storage_key, node_usage_global_key, node_usage_group_key,
    watch_interest_node_key,
};
use crate::types::{GroupId, Key, UserId};
use crate::{NodeId, TopicId};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DocumentSyncTarget {
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
    NodeUsage {
        realm_id: RealmId,
        node_id: NodeId,
        group_id: Option<GroupId>,
    },
    WatchInterest {
        realm_id: RealmId,
        node_id: NodeId,
    },
    NodeInfo {
        realm_id: RealmId,
        node_id: NodeId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingDocumentPlacement {
    pub realm_id: RealmId,
    pub target: DocumentSyncTarget,
    pub desired_peer_count: usize,
    pub selected_peers: Vec<NodeId>,
    pub updated_at: u64,
    pub authoritative_node_id: NodeId,
    pub placement: PlacementRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSyncOutboxRecord {
    pub outbox_id: Ulid,
    pub node_id: NodeId,
    pub target: DocumentSyncTarget,
    pub peers: Vec<NodeId>,
    pub event: DocumentSyncOutboxEvent,
    pub updated_at: u64,
    /// Whether the publisher may mint this document's sync topic genesis when it
    /// is missing. Only the node that originated the document sets this; every
    /// other publisher waits (retryable) for the origin's genesis to replicate.
    pub allow_genesis: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentSyncOutboxEvent {
    Upsert {
        bytes: Vec<u8>,
        change: DocumentSyncChange,
    },
    AdminOperation {
        event: Box<AdminDocumentEvent>,
    },
    Delete {
        change: DocumentSyncChange,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentSyncEvictedDocument {
    pub event_id: Option<Ulid>,
    pub target: DocumentSyncTarget,
    pub event: DocumentSyncOutboxEvent,
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
pub struct DocumentSyncChange {
    pub base: Option<DocumentSyncRevision>,
    pub current: DocumentSyncRevision,
    pub kind: DocumentSyncChangeKind,
    pub placement: PlacementRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSyncConflict {
    pub target: DocumentSyncTarget,
    pub local_change: Option<DocumentSyncChange>,
    pub local_bytes: Option<Vec<u8>>,
    pub incoming_change: DocumentSyncChange,
    pub incoming_bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentSyncChangeKind {
    Upsert,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentSyncApplyDecision {
    Apply,
    SkipStale,
    SkipTombstoned,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentSyncPublish {
    Upsert {
        event_id: Ulid,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        change: DocumentSyncChange,
        allow_genesis: bool,
    },
    AdminOperation {
        target: DocumentSyncTarget,
        event: Box<AdminDocumentEvent>,
        allow_genesis: bool,
    },
    Delete {
        event_id: Ulid,
        target: DocumentSyncTarget,
        change: DocumentSyncChange,
        allow_genesis: bool,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentSyncReconcileResult {
    pub targets: Vec<DocumentSyncTarget>,
    pub metadata_create_events: Vec<MetadataCreateEventRecord>,
    pub metadata_graph_tombstones: Vec<MetadataGraphLifecycleRecord>,
}

impl DocumentSyncReconcileResult {
    pub fn applied(&self) -> usize {
        self.targets.len()
    }
}

impl DocumentSyncPublish {
    pub fn target(&self) -> &DocumentSyncTarget {
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

impl DocumentSyncOutboxEvent {
    pub fn kind(&self) -> &'static [u8] {
        match self {
            Self::Upsert { .. } => b"upsert",
            Self::Delete { .. } => b"delete",
            Self::AdminOperation { .. } => b"admin-operation",
        }
    }
}

pub fn compare_document_sync_revisions(
    local: &DocumentSyncRevision,
    remote: &DocumentSyncRevision,
) -> Ordering {
    local.cmp(remote)
}

pub fn document_sync_apply_decision(
    local: Option<&DocumentSyncChange>,
    incoming: &DocumentSyncChange,
) -> DocumentSyncApplyDecision {
    let Some(local) = local else {
        return DocumentSyncApplyDecision::Apply;
    };

    if incoming.current == local.current {
        return if incoming.kind == local.kind {
            DocumentSyncApplyDecision::Apply
        } else {
            DocumentSyncApplyDecision::Conflict
        };
    }

    if local.kind == DocumentSyncChangeKind::Delete
        && incoming.kind == DocumentSyncChangeKind::Upsert
        && incoming.base.as_ref() != Some(&local.current)
    {
        return DocumentSyncApplyDecision::SkipTombstoned;
    }

    match incoming.current.generation.cmp(&local.current.generation) {
        Ordering::Less => DocumentSyncApplyDecision::SkipStale,
        Ordering::Equal => DocumentSyncApplyDecision::Conflict,
        Ordering::Greater if incoming.base.as_ref() == Some(&local.current) => {
            DocumentSyncApplyDecision::Apply
        }
        Ordering::Greater => DocumentSyncApplyDecision::Conflict,
    }
}

impl DocumentSyncTarget {
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
            | Self::MetadataDocumentLifecycle { document_id } => TopicId::metadata(*document_id),
            Self::MetadataGraphLifecycle { graph_iri } => {
                TopicId::metadata(metadata_graph_lifecycle_topic_id(graph_iri))
            }
            Self::NodeUsage { realm_id, .. } => TopicId::realm(*realm_id),
            Self::WatchInterest { realm_id, .. } => TopicId::realm(*realm_id),
            Self::NodeInfo { realm_id, .. } => TopicId::realm(*realm_id),
        }
    }

    pub fn storage_keyspace(&self) -> &'static str {
        match self {
            Self::Group { .. } => GROUP_KEYSPACE,
            Self::GroupAuthorization { .. } | Self::RealmAuthorization { .. } => AUTH_KEYSPACE,
            Self::RealmConfig { .. } => REALM_CONFIG_KEYSPACE,
            Self::User { .. } => USER_KEYSPACE,
            Self::MetadataRegistry { .. } => METADATA_INDEX_KEYSPACE,
            Self::MetadataCreateEvent { .. } => METADATA_EVENT_LOG_KEYSPACE,
            Self::MetadataDocumentLifecycle { .. } => METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
            Self::MetadataGraphLifecycle { .. } => METADATA_GRAPH_LIFECYCLE_KEYSPACE,
            Self::NodeUsage { .. } => USAGE_NODE_STATS_KEYSPACE,
            Self::WatchInterest { .. } => NOTIFICATION_WATCH_INTEREST_KEYSPACE,
            Self::NodeInfo { .. } => NODE_INFO_KEYSPACE,
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
            } => metadata_event_log_key(*document_id, *event_id),
            Self::MetadataDocumentLifecycle { document_id } => {
                metadata_document_lifecycle_key(*document_id)
            }
            Self::MetadataGraphLifecycle { graph_iri } => metadata_graph_lifecycle_key(graph_iri),
            Self::NodeUsage {
                node_id, group_id, ..
            } => match group_id {
                Some(group_id) => ByteView::from(node_usage_group_key(*group_id, *node_id)),
                None => ByteView::from(node_usage_global_key(*node_id)),
            },
            Self::WatchInterest { realm_id, node_id } => {
                ByteView::from(watch_interest_node_key(*realm_id, *node_id))
            }
            Self::NodeInfo { node_id, .. } => ByteView::from(node_info_storage_key(*node_id)),
        }
    }

    pub fn sync_topic_id(&self) -> irokle::TopicId {
        let mut bytes = b"aruna-document-topic-v1".to_vec();
        bytes.extend_from_slice(&self.topic_id().to_bytes());
        match self {
            Self::Group { .. } => bytes.extend_from_slice(b"/group"),
            Self::GroupAuthorization { .. } => bytes.extend_from_slice(b"/group-auth"),
            Self::RealmAuthorization { .. } => bytes.extend_from_slice(b"/realm-auth"),
            Self::RealmConfig { .. } => bytes.extend_from_slice(b"/realm-config"),
            Self::User { user_id } => {
                bytes.extend_from_slice(b"/user/");
                bytes.extend_from_slice(&user_id.to_bytes());
            }
            Self::MetadataRegistry { document_id, .. } => {
                bytes.extend_from_slice(b"/metadata/");
                bytes.extend_from_slice(&document_id.to_bytes());
            }
            Self::MetadataCreateEvent { document_id, .. } => {
                bytes.extend_from_slice(b"/metadata-create-event/");
                bytes.extend_from_slice(&document_id.to_bytes());
            }
            Self::MetadataDocumentLifecycle { document_id } => {
                bytes.extend_from_slice(b"/metadata-document-lifecycle/");
                bytes.extend_from_slice(&document_id.to_bytes());
            }
            Self::MetadataGraphLifecycle { graph_iri } => {
                bytes.extend_from_slice(b"/metadata-graph-lifecycle/");
                bytes.extend_from_slice(graph_iri.as_bytes());
            }
            // No node id in the suffix: every node's usage snapshot flows over a
            // single shared realm-scoped topic that all realm nodes subscribe to.
            Self::NodeUsage { .. } => bytes.extend_from_slice(b"/node-usage"),
            // Likewise realm-shared: every node's watch-interest digest rides one
            // topic so origin nodes receive all holders' interest.
            Self::WatchInterest { .. } => bytes.extend_from_slice(b"/watch-interest"),
            // Realm-shared: every node's info/heartbeat document rides one topic
            // (no node id in the suffix) so all realm nodes receive every peer's.
            Self::NodeInfo { .. } => bytes.extend_from_slice(b"/node-info"),
        }
        irokle::TopicId::hash(bytes)
    }
}

/// Sync topic a bucket's records ride, derived purely from the placement
/// reference (no config lookup at the net layer). Mirrors the `TopicId::hash`
/// idiom [`DocumentSyncTarget::sync_topic_id`] uses. Not yet wired into
/// `sync_topic_id`: stage 2 flips the bucketed targets over to this in a later
/// commit.
pub fn bucket_topic_id(realm_id: RealmId, placement: &PlacementRef) -> irokle::TopicId {
    let mut bytes = b"aruna-bucket-topic-v1".to_vec();
    bytes.extend_from_slice(realm_id.as_bytes());
    bytes.extend_from_slice(&placement.strategy_id.to_bytes());
    bytes.extend_from_slice(&placement.epoch.to_le_bytes());
    bytes.extend_from_slice(&placement.bucket.to_be_bytes());
    irokle::TopicId::hash(bytes)
}

fn metadata_graph_lifecycle_topic_id(graph_iri: &str) -> Ulid {
    let hash = blake3::hash(graph_iri.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, irokle::Event)]
#[irokle(type_id = "aruna.document.v2")]
pub enum DocumentSyncEvent {
    Upsert {
        event_id: Ulid,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        change: DocumentSyncChange,
    },
    AdminOperation {
        target: DocumentSyncTarget,
        event: Box<AdminDocumentEvent>,
    },
    Delete {
        event_id: Ulid,
        target: DocumentSyncTarget,
        change: DocumentSyncChange,
    },
}

impl DocumentSyncEvent {
    pub fn target(&self) -> &DocumentSyncTarget {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentSyncEffect {
    PublishDocuments {
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    },
    SyncDocument {
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    },
    SyncDocuments {
        targets: Vec<DocumentSyncTarget>,
        peers: Vec<NodeId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentSyncNetEvent {
    DocumentsPublished {
        targets: Vec<DocumentSyncTarget>,
    },
    DocumentsReconciled {
        applied: usize,
        targets: Vec<DocumentSyncTarget>,
        metadata_create_events: Vec<MetadataCreateEventRecord>,
        metadata_graph_tombstones: Vec<MetadataGraphLifecycleRecord>,
    },
    Error {
        target: Option<DocumentSyncTarget>,
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{
        DocumentSyncApplyDecision, DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncEvent,
        DocumentSyncOutboxEvent, DocumentSyncPublish, DocumentSyncRevision, DocumentSyncTarget,
        bucket_topic_id, compare_document_sync_revisions, document_sync_apply_decision,
    };
    use crate::NodeId;
    use crate::TopicId;
    use crate::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
        METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE,
        REALM_CONFIG_KEYSPACE, USER_KEYSPACE,
    };
    use crate::structs::PlacementRef;
    use crate::structs::RealmId;
    use crate::types::UserId;
    use irokle::Event as _;
    use ulid::Ulid;

    fn test_ulid(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    fn test_node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn revision(generation: u64, event_seed: u8, actor_seed: u8) -> DocumentSyncRevision {
        DocumentSyncRevision {
            generation,
            event_id: test_ulid(event_seed),
            actor: test_node(actor_seed),
            updated_at_ms: u64::from(event_seed),
        }
    }

    fn change(
        kind: DocumentSyncChangeKind,
        base: Option<DocumentSyncRevision>,
        generation: u64,
        event_seed: u8,
        actor_seed: u8,
    ) -> DocumentSyncChange {
        DocumentSyncChange {
            base,
            current: revision(generation, event_seed, actor_seed),
            kind,
            placement: crate::structs::PlacementRef::NIL,
        }
    }

    fn test_realm(seed: u8) -> RealmId {
        RealmId::from_bytes([seed; 32])
    }

    fn graph_topic_ulid(graph_iri: &str) -> Ulid {
        let hash = blake3::hash(graph_iri.as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash.as_bytes()[..16]);
        Ulid::from_bytes(bytes)
    }

    fn graph_lifecycle_key(graph_iri: &str) -> Vec<u8> {
        blake3::hash(graph_iri.as_bytes()).as_bytes().to_vec()
    }

    #[test]
    fn document_sync_target_domain_topic_mapping_is_stable() {
        let group_id = test_ulid(1);
        let realm_id = test_realm(2);
        let user_id = UserId::new(test_ulid(3), realm_id);
        let document_id = test_ulid(4);
        let event_id = test_ulid(5);
        let graph_iri = "https://example.com/graphs/stable";

        let cases = [
            (
                DocumentSyncTarget::Group { group_id },
                TopicId::group(group_id),
            ),
            (
                DocumentSyncTarget::GroupAuthorization { group_id },
                TopicId::group(group_id),
            ),
            (
                DocumentSyncTarget::RealmAuthorization { realm_id },
                TopicId::realm(realm_id),
            ),
            (
                DocumentSyncTarget::RealmConfig { realm_id },
                TopicId::realm(realm_id),
            ),
            (
                DocumentSyncTarget::User { user_id },
                TopicId::users(realm_id),
            ),
            (
                DocumentSyncTarget::MetadataRegistry {
                    group_id,
                    document_id,
                },
                TopicId::metadata(document_id),
            ),
            (
                DocumentSyncTarget::MetadataCreateEvent {
                    document_id,
                    event_id,
                },
                TopicId::metadata(document_id),
            ),
            (
                DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
                TopicId::metadata(document_id),
            ),
            (
                DocumentSyncTarget::MetadataGraphLifecycle {
                    graph_iri: graph_iri.to_string(),
                },
                TopicId::metadata(graph_topic_ulid(graph_iri)),
            ),
        ];

        for (target, expected_topic) in cases {
            assert_eq!(target.topic_id(), expected_topic, "{target:?}");
        }
    }

    #[test]
    fn document_sync_target_storage_mapping_is_stable() {
        let group_id = test_ulid(1);
        let realm_id = test_realm(2);
        let user_id = UserId::new(test_ulid(3), realm_id);
        let document_id = test_ulid(4);
        let event_id = test_ulid(5);
        let graph_iri = "https://example.com/graphs/stable";

        let mut user_key = Vec::with_capacity(48);
        user_key.extend_from_slice(realm_id.as_bytes());
        user_key.extend_from_slice(&user_id.user_ulid.to_bytes());

        let mut registry_key = Vec::with_capacity(32);
        registry_key.extend_from_slice(&group_id.to_bytes());
        registry_key.extend_from_slice(&document_id.to_bytes());

        let mut event_key = Vec::with_capacity(32);
        event_key.extend_from_slice(&document_id.to_bytes());
        event_key.extend_from_slice(&event_id.to_bytes());

        let cases = [
            (
                DocumentSyncTarget::Group { group_id },
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
            ),
            (
                DocumentSyncTarget::GroupAuthorization { group_id },
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
            ),
            (
                DocumentSyncTarget::RealmAuthorization { realm_id },
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
            ),
            (
                DocumentSyncTarget::RealmConfig { realm_id },
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
            ),
            (
                DocumentSyncTarget::User { user_id },
                USER_KEYSPACE,
                user_key,
            ),
            (
                DocumentSyncTarget::MetadataRegistry {
                    group_id,
                    document_id,
                },
                METADATA_INDEX_KEYSPACE,
                registry_key,
            ),
            (
                DocumentSyncTarget::MetadataCreateEvent {
                    document_id,
                    event_id,
                },
                METADATA_EVENT_LOG_KEYSPACE,
                event_key,
            ),
            (
                DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
                METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
                document_id.to_bytes().to_vec(),
            ),
            (
                DocumentSyncTarget::MetadataGraphLifecycle {
                    graph_iri: graph_iri.to_string(),
                },
                METADATA_GRAPH_LIFECYCLE_KEYSPACE,
                graph_lifecycle_key(graph_iri),
            ),
        ];

        for (target, expected_keyspace, expected_key) in cases {
            assert_eq!(target.storage_keyspace(), expected_keyspace, "{target:?}");
            assert_eq!(
                target.storage_key().as_ref(),
                expected_key.as_slice(),
                "{target:?}"
            );
        }
    }

    #[test]
    fn document_sync_topic_mapping_scopes_variants_under_shared_domain_topics() {
        let group_id = test_ulid(1);
        let realm_id = test_realm(2);
        let document_id = test_ulid(4);
        let event_id = test_ulid(5);
        let group = DocumentSyncTarget::Group { group_id };
        let group_auth = DocumentSyncTarget::GroupAuthorization { group_id };
        let realm_auth = DocumentSyncTarget::RealmAuthorization { realm_id };
        let realm_config = DocumentSyncTarget::RealmConfig { realm_id };
        let registry = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        };
        let create = DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
        };
        let lifecycle = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let user_a = DocumentSyncTarget::User {
            user_id: UserId::new(test_ulid(6), realm_id),
        };
        let user_b = DocumentSyncTarget::User {
            user_id: UserId::new(test_ulid(7), realm_id),
        };

        assert_eq!(group.topic_id(), group_auth.topic_id());
        assert_ne!(group.sync_topic_id(), group_auth.sync_topic_id());

        assert_eq!(realm_auth.topic_id(), realm_config.topic_id());
        assert_ne!(realm_auth.sync_topic_id(), realm_config.sync_topic_id());

        assert_eq!(registry.topic_id(), create.topic_id());
        assert_eq!(registry.topic_id(), lifecycle.topic_id());
        assert_ne!(registry.sync_topic_id(), create.sync_topic_id());
        assert_ne!(registry.sync_topic_id(), lifecycle.sync_topic_id());
        assert_ne!(create.sync_topic_id(), lifecycle.sync_topic_id());

        assert_eq!(user_a.topic_id(), user_b.topic_id());
        assert_ne!(user_a.sync_topic_id(), user_b.sync_topic_id());
    }

    #[test]
    fn document_sync_event_type_id_is_stable() {
        assert_eq!(DocumentSyncEvent::TYPE_ID, "aruna.document.v2");
    }

    #[test]
    fn bucket_topic_id_matches_golden_and_separates_dimensions() {
        let realm_id = test_realm(2);
        let placement = PlacementRef {
            strategy_id: test_ulid(4),
            epoch: 0,
            bucket: 5,
        };
        // Fixed inputs → fixed topic id: the stage-2 cross-node canary. A change
        // here means co-holders would derive different bucket topics.
        assert_eq!(
            bucket_topic_id(realm_id, &placement).to_string(),
            "9ea950b394474cd820cfce7d0a840232de8cbd34216aeb19a11eb39f21f99048"
        );

        // Each hashed dimension moves the topic.
        let other_bucket = PlacementRef {
            bucket: 6,
            ..placement
        };
        let other_epoch = PlacementRef {
            epoch: 1,
            ..placement
        };
        let other_strategy = PlacementRef {
            strategy_id: test_ulid(5),
            ..placement
        };
        assert_ne!(
            bucket_topic_id(realm_id, &placement),
            bucket_topic_id(realm_id, &other_bucket)
        );
        assert_ne!(
            bucket_topic_id(realm_id, &placement),
            bucket_topic_id(realm_id, &other_epoch)
        );
        assert_ne!(
            bucket_topic_id(realm_id, &placement),
            bucket_topic_id(realm_id, &other_strategy)
        );
        assert_ne!(
            bucket_topic_id(realm_id, &placement),
            bucket_topic_id(test_realm(3), &placement)
        );
    }

    #[test]
    fn node_usage_targets_share_one_realm_topic_and_map_to_snapshot_keys() {
        use crate::keyspaces::USAGE_NODE_STATS_KEYSPACE;
        use crate::structs::{node_usage_global_key, node_usage_group_key};

        let realm_id = test_realm(2);
        let node_id = test_node(1);
        let group_id = test_ulid(4);

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

        // Both map onto the realm domain topic and the single shared sync topic.
        assert_eq!(global.topic_id(), TopicId::realm(realm_id));
        assert_eq!(global.topic_id(), group.topic_id());
        assert_eq!(global.sync_topic_id(), group.sync_topic_id());

        // A different node's usage rides the very same shared topic.
        let other = DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id: test_node(9),
            group_id: None,
        };
        assert_eq!(global.sync_topic_id(), other.sync_topic_id());
        // But is distinct from the realm-config topic on the same domain.
        assert_ne!(
            global.sync_topic_id(),
            DocumentSyncTarget::RealmConfig { realm_id }.sync_topic_id()
        );

        assert_eq!(global.storage_keyspace(), USAGE_NODE_STATS_KEYSPACE);
        assert_eq!(
            global.storage_key().as_ref(),
            node_usage_global_key(node_id).as_slice()
        );
        assert_eq!(
            group.storage_key().as_ref(),
            node_usage_group_key(group_id, node_id).as_slice()
        );
    }

    #[test]
    fn node_info_targets_share_one_realm_topic_and_map_to_node_keys() {
        use crate::keyspaces::NODE_INFO_KEYSPACE;
        use crate::structs::node_info_storage_key;

        let realm_id = test_realm(2);
        let node_id = test_node(1);
        let target = DocumentSyncTarget::NodeInfo { realm_id, node_id };

        // Rides the realm domain topic and one shared sync topic across nodes.
        assert_eq!(target.topic_id(), TopicId::realm(realm_id));
        let other = DocumentSyncTarget::NodeInfo {
            realm_id,
            node_id: test_node(9),
        };
        assert_eq!(target.sync_topic_id(), other.sync_topic_id());
        // Distinct from the node-usage and watch-interest topics on the same realm.
        assert_ne!(
            target.sync_topic_id(),
            DocumentSyncTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            }
            .sync_topic_id()
        );
        assert_ne!(
            target.sync_topic_id(),
            DocumentSyncTarget::WatchInterest { realm_id, node_id }.sync_topic_id()
        );

        assert_eq!(target.storage_keyspace(), NODE_INFO_KEYSPACE);
        assert_eq!(
            target.storage_key().as_ref(),
            node_info_storage_key(node_id).as_slice()
        );
    }

    #[test]
    fn watch_interest_targets_share_one_realm_topic_and_map_to_digest_keys() {
        use crate::keyspaces::NOTIFICATION_WATCH_INTEREST_KEYSPACE;
        use crate::structs::watch_interest_node_key;

        let realm_id = test_realm(2);
        let node_id = test_node(1);
        let other = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: test_node(9),
        };
        let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };

        // Rides the realm domain topic and one shared sync topic across nodes.
        assert_eq!(target.topic_id(), TopicId::realm(realm_id));
        assert_eq!(target.sync_topic_id(), other.sync_topic_id());
        // Distinct from the node-usage topic that shares the same realm domain.
        assert_ne!(
            target.sync_topic_id(),
            DocumentSyncTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            }
            .sync_topic_id()
        );

        assert_eq!(
            target.storage_keyspace(),
            NOTIFICATION_WATCH_INTEREST_KEYSPACE
        );
        assert_eq!(
            target.storage_key().as_ref(),
            watch_interest_node_key(realm_id, node_id).as_slice()
        );
    }

    #[test]
    fn upsert_uses_upsert_kind_and_helpers() {
        let event_id = test_ulid(10);
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: test_realm(11),
        };
        let change = change(DocumentSyncChangeKind::Upsert, None, 1, 12, 1);
        let outbox = DocumentSyncOutboxEvent::Upsert {
            bytes: vec![1, 2],
            change,
        };
        let publish = DocumentSyncPublish::Upsert {
            event_id,
            target: target.clone(),
            bytes: vec![1, 2],
            change,
            allow_genesis: true,
        };
        let event = DocumentSyncEvent::Upsert {
            event_id,
            target: target.clone(),
            bytes: vec![1, 2],
            change,
        };

        assert_eq!(outbox.kind(), b"upsert");
        assert_eq!(publish.target(), &target);
        assert_eq!(publish.event_id(), event_id);
        assert!(publish.allow_genesis());
        assert_eq!(event.target(), &target);
        assert_eq!(event.event_id(), event_id);
    }

    #[test]
    fn delete_uses_delete_kind_and_helpers() {
        let event_id = test_ulid(13);
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: test_realm(14),
        };
        let change = change(DocumentSyncChangeKind::Delete, None, 2, 15, 1);
        let outbox = DocumentSyncOutboxEvent::Delete { change };
        let publish = DocumentSyncPublish::Delete {
            event_id,
            target: target.clone(),
            change,
            allow_genesis: false,
        };
        let event = DocumentSyncEvent::Delete {
            event_id,
            target: target.clone(),
            change,
        };

        assert_eq!(outbox.kind(), b"delete");
        assert_eq!(publish.target(), &target);
        assert_eq!(publish.event_id(), event_id);
        assert!(!publish.allow_genesis());
        assert_eq!(event.target(), &target);
        assert_eq!(event.event_id(), event_id);
    }

    #[test]
    fn document_sync_revision_comparator_is_stable() {
        let older = revision(1, 9, 1);
        let newer = revision(2, 1, 1);
        let same_generation_a = revision(2, 1, 1);
        let same_generation_b = revision(2, 2, 1);

        assert_eq!(
            compare_document_sync_revisions(&older, &newer),
            Ordering::Less
        );
        assert_eq!(
            compare_document_sync_revisions(&newer, &older),
            Ordering::Greater
        );
        assert_eq!(
            compare_document_sync_revisions(&newer, &same_generation_a),
            Ordering::Equal
        );
        assert_eq!(
            compare_document_sync_revisions(&same_generation_a, &same_generation_b),
            Ordering::Less
        );
    }

    #[test]
    fn document_sync_apply_decision_applies_new_and_successor_changes() {
        let local = change(DocumentSyncChangeKind::Upsert, None, 1, 1, 1);
        let incoming = change(DocumentSyncChangeKind::Upsert, Some(local.current), 2, 2, 1);

        assert_eq!(
            document_sync_apply_decision(None, &local),
            DocumentSyncApplyDecision::Apply
        );
        assert_eq!(
            document_sync_apply_decision(Some(&local), &incoming),
            DocumentSyncApplyDecision::Apply
        );
        assert_eq!(
            document_sync_apply_decision(Some(&local), &local),
            DocumentSyncApplyDecision::Apply
        );
    }

    #[test]
    fn document_sync_apply_decision_skips_stale_and_tombstoned_changes() {
        let stale = change(DocumentSyncChangeKind::Upsert, None, 1, 1, 1);
        let newer = change(DocumentSyncChangeKind::Upsert, Some(stale.current), 2, 2, 1);
        let tombstone = change(DocumentSyncChangeKind::Delete, Some(stale.current), 2, 3, 1);

        assert_eq!(
            document_sync_apply_decision(Some(&newer), &stale),
            DocumentSyncApplyDecision::SkipStale
        );
        assert_eq!(
            document_sync_apply_decision(Some(&tombstone), &stale),
            DocumentSyncApplyDecision::SkipTombstoned
        );
    }

    #[test]
    fn document_sync_apply_decision_conflicts_on_unobserved_changes() {
        let local = change(DocumentSyncChangeKind::Upsert, None, 1, 1, 1);
        let same_generation = change(DocumentSyncChangeKind::Upsert, None, 1, 2, 2);
        let unobserved_newer = change(DocumentSyncChangeKind::Delete, None, 2, 3, 2);

        assert_eq!(
            document_sync_apply_decision(Some(&local), &same_generation),
            DocumentSyncApplyDecision::Conflict
        );
        assert_eq!(
            document_sync_apply_decision(Some(&local), &unobserved_newer),
            DocumentSyncApplyDecision::Conflict
        );
    }

    #[test]
    fn metadata_document_lifecycle_target_is_document_scoped() {
        let document_id = test_ulid(4);
        let lifecycle = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let create = DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id: test_ulid(5),
        };

        assert_eq!(lifecycle.topic_id(), create.topic_id());
        assert_eq!(lifecycle.storage_key().as_ref(), document_id.to_bytes());
    }

    #[test]
    fn metadata_document_lifecycle_topic_is_shared_by_upsert_and_delete() {
        let document_id = test_ulid(4);
        let upsert_target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let delete_target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };

        assert_eq!(upsert_target.topic_id(), delete_target.topic_id());
        assert_eq!(upsert_target.sync_topic_id(), delete_target.sync_topic_id());
    }
}
