use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_KEYSPACE,
};
use crate::metadata::MetadataCreateEventRecord;
use crate::storage_entries::{metadata_event_log_key, metadata_graph_lifecycle_key};
use crate::structs::RealmId;
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
    MetadataGraphLifecycle {
        graph_iri: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTopicPlacement {
    pub realm_id: RealmId,
    pub target: DocumentSyncTarget,
    pub desired_peer_count: usize,
    pub selected_peers: Vec<NodeId>,
    pub updated_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSyncOutboxRecord {
    pub outbox_id: Ulid,
    pub node_id: NodeId,
    pub target: DocumentSyncTarget,
    pub peers: Vec<NodeId>,
    pub event: DocumentSyncOutboxEvent,
    pub updated_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentSyncOutboxEvent {
    Upsert { bytes: Vec<u8> },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentSyncPublish {
    Upsert {
        event_id: Ulid,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
    },
    Delete {
        event_id: Ulid,
        target: DocumentSyncTarget,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentSyncReconcileResult {
    pub targets: Vec<DocumentSyncTarget>,
    pub metadata_create_events: Vec<MetadataCreateEventRecord>,
}

impl DocumentSyncReconcileResult {
    pub fn applied(&self) -> usize {
        self.targets.len()
    }
}

impl DocumentSyncPublish {
    pub fn target(&self) -> &DocumentSyncTarget {
        match self {
            Self::Upsert { target, .. } | Self::Delete { target, .. } => target,
        }
    }
}

impl DocumentSyncOutboxEvent {
    pub fn kind(&self) -> &'static [u8] {
        match self {
            Self::Upsert { .. } => b"upsert",
            Self::Delete => b"delete",
        }
    }
}

impl DocumentSyncTarget {
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
            | Self::MetadataCreateEvent { document_id, .. } => TopicId::metadata(*document_id),
            Self::MetadataGraphLifecycle { graph_iri } => {
                TopicId::metadata(metadata_graph_lifecycle_topic_id(graph_iri))
            }
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
            Self::MetadataGraphLifecycle { .. } => METADATA_GRAPH_LIFECYCLE_KEYSPACE,
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
            Self::MetadataGraphLifecycle { graph_iri } => metadata_graph_lifecycle_key(graph_iri),
        }
    }

    pub fn irokle_topic_id(&self) -> irokle::TopicId {
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
            Self::MetadataGraphLifecycle { graph_iri } => {
                bytes.extend_from_slice(b"/metadata-graph-lifecycle/");
                bytes.extend_from_slice(graph_iri.as_bytes());
            }
        }
        irokle::TopicId::hash(bytes)
    }
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
    },
    Delete {
        event_id: Ulid,
        target: DocumentSyncTarget,
    },
}

impl DocumentSyncEvent {
    pub fn target(&self) -> &DocumentSyncTarget {
        match self {
            Self::Upsert { target, .. } | Self::Delete { target, .. } => target,
        }
    }

    pub fn event_id(&self) -> Ulid {
        match self {
            Self::Upsert { event_id, .. } | Self::Delete { event_id, .. } => *event_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IrokleEffect {
    PublishDocument {
        event_id: Ulid,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        peers: Vec<NodeId>,
    },
    PublishDocuments {
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    },
    DeleteDocument {
        event_id: Ulid,
        target: DocumentSyncTarget,
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
pub enum IrokleEvent {
    DocumentPublished {
        target: DocumentSyncTarget,
    },
    DocumentsPublished {
        targets: Vec<DocumentSyncTarget>,
    },
    DocumentDeleted {
        target: DocumentSyncTarget,
    },
    DocumentsReconciled {
        applied: usize,
        targets: Vec<DocumentSyncTarget>,
        metadata_create_events: Vec<MetadataCreateEventRecord>,
    },
    Error {
        target: Option<DocumentSyncTarget>,
        error: String,
    },
}
