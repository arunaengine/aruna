use automerge::ChangeHash;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::id::{DhtKeyId, NodeId, TopicId};
use crate::structs::RealmId;
use crate::task::TaskKey;
use crate::types::GroupId;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AutomergeDocumentVariant {
    Metadata {
        group_id: GroupId,
        document_id: Ulid,
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
}

impl AutomergeDocumentVariant {
    pub fn metadata(group_id: GroupId, document_id: Ulid) -> Self {
        Self::Metadata {
            group_id,
            document_id,
        }
    }

    pub fn topic_id(&self) -> TopicId {
        TopicId::automerge_document(self.topic_key())
    }

    pub fn topic_key(&self) -> DhtKeyId {
        DhtKeyId::from_data(&self.holder_lookup_bytes())
    }

    pub fn holder_lookup_bytes(&self) -> Vec<u8> {
        match self {
            Self::Metadata {
                group_id,
                document_id,
            } => {
                let mut bytes = Vec::with_capacity(32);
                bytes.extend_from_slice(&group_id.to_bytes());
                bytes.extend_from_slice(&document_id.to_bytes());
                bytes
            }
            Self::GroupAuthorization { group_id } => format!("perm_{group_id}").into_bytes(),
            Self::RealmAuthorization { realm_id } => format!("realm_perm_{realm_id}").into_bytes(),
            Self::RealmConfig { realm_id } => format!("realm_config_{realm_id}").into_bytes(),
        }
    }

    pub fn announce_timer_key(&self) -> TaskKey {
        TaskKey::AutomergeAnnounce(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::AutomergeDocumentVariant;
    use ulid::Ulid;

    #[test]
    fn metadata_documents_with_different_groups_have_distinct_topic_keys() {
        let document_id = Ulid::from_bytes([7u8; 16]);
        let left = AutomergeDocumentVariant::Metadata {
            group_id: Ulid::from_bytes([1u8; 16]),
            document_id,
        };
        let right = AutomergeDocumentVariant::Metadata {
            group_id: Ulid::from_bytes([2u8; 16]),
            document_id,
        };

        assert_ne!(left.topic_key(), right.topic_key());
        assert_ne!(left.topic_id(), right.topic_id());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AutomergeSyncFeature {
    MessageV1,
    #[default]
    MessageV2,
    InitAuthProof,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InitAuthProof {
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomergeInit {
    pub document: AutomergeDocumentVariant,
    pub heads: Vec<ChangeHash>,
    pub capabilities: Vec<AutomergeSyncFeature>,
    pub auth: Option<InitAuthProof>,
}

impl AutomergeInit {
    pub fn new(document: AutomergeDocumentVariant, heads: Vec<ChangeHash>) -> Self {
        Self {
            document,
            heads,
            capabilities: vec![
                AutomergeSyncFeature::MessageV1,
                AutomergeSyncFeature::MessageV2,
            ],
            auth: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomergeState {
    pub document: AutomergeDocumentVariant,
    pub heads: Vec<ChangeHash>,
}

impl AutomergeState {
    pub fn new(document: AutomergeDocumentVariant, heads: Vec<ChangeHash>) -> Self {
        Self { document, heads }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutomergeRejectReason {
    Unauthorized,
    DocumentNotFound,
    InvalidDocument,
    InvalidInit,
    InternalError,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutomergeSyncError {
    Unauthorized,
    DocumentNotFound,
    InvalidInit,
    InvalidFrame,
    InvalidDocument,
    Protocol(String),
    Network(String),
    Storage(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutomergeEffect {
    StartOutboundSync {
        peer: NodeId,
        init: AutomergeInit,
    },
    StartInboundSync {
        sync_id: Ulid,
    },
    RunSync {
        sync_id: Ulid,
        local_document: Vec<u8>,
        response_init: Option<AutomergeInit>,
    },
    RejectSync {
        sync_id: Ulid,
        reason: AutomergeRejectReason,
    },
    CloseSync {
        sync_id: Ulid,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutomergeEvent {
    SyncInitialized {
        sync_id: Ulid,
        peer: NodeId,
        remote_init: AutomergeInit,
    },
    SyncFinished {
        sync_id: Ulid,
        document: AutomergeDocumentVariant,
        before_heads: Vec<ChangeHash>,
        after_heads: Vec<ChangeHash>,
        updated_document: Vec<u8>,
        changed: bool,
    },
    SyncRejected {
        sync_id: Ulid,
        document: Option<AutomergeDocumentVariant>,
        error: AutomergeSyncError,
    },
    SyncClosed {
        sync_id: Ulid,
    },
}
