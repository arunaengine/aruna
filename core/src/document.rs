use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE, USER_KEYSPACE,
};
use crate::storage_entries::metadata_graph_lifecycle_key;
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
            Self::MetadataRegistry { document_id, .. } => TopicId::metadata(*document_id),
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
#[irokle(type_id = "aruna.document.v1")]
pub enum DocumentSyncEvent {
    Upsert {
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
    },
    Delete {
        target: DocumentSyncTarget,
    },
}

impl DocumentSyncEvent {
    pub fn target(&self) -> &DocumentSyncTarget {
        match self {
            Self::Upsert { target, .. } | Self::Delete { target } => target,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IrokleEffect {
    PublishDocument {
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        peers: Vec<NodeId>,
    },
    DeleteDocument {
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    },
    SyncDocument {
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IrokleEvent {
    DocumentPublished {
        target: DocumentSyncTarget,
    },
    DocumentDeleted {
        target: DocumentSyncTarget,
    },
    DocumentsReconciled {
        applied: usize,
    },
    Error {
        target: Option<DocumentSyncTarget>,
        error: String,
    },
}
