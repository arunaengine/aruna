use automerge::ChangeHash;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::gossip::TopicMessageKind;
use crate::id::{NodeId, TopicId};
use crate::structs::RealmId;
use crate::task::TaskKey;
use crate::trace_context::DistributedTraceContext;
use crate::types::{GroupId, UserId};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AutomergeDocumentVariant {
    Group { group_id: GroupId },
    GroupAuthorization { group_id: GroupId },
    RealmAuthorization { realm_id: RealmId },
    RealmConfig { realm_id: RealmId },
    User { user_id: UserId },
}

impl AutomergeDocumentVariant {
    pub fn topic_id(&self) -> TopicId {
        match self {
            Self::Group { group_id } | Self::GroupAuthorization { group_id } => {
                TopicId::group(*group_id)
            }
            Self::RealmAuthorization { realm_id } | Self::RealmConfig { realm_id } => {
                TopicId::realm(*realm_id)
            }
            Self::User { user_id } => TopicId::users(user_id.realm_id),
        }
    }

    pub fn message_kind(&self) -> TopicMessageKind {
        match self {
            Self::Group { .. } => TopicMessageKind::Group,
            Self::GroupAuthorization { .. } => TopicMessageKind::GroupAuthorization,
            Self::RealmAuthorization { .. } => TopicMessageKind::RealmAuthorization,
            Self::RealmConfig { .. } => TopicMessageKind::RealmConfig,
            Self::User { user_id } => TopicMessageKind::User { user_id: *user_id },
        }
    }

    pub fn from_topic_message(topic: &TopicId, kind: &TopicMessageKind) -> Option<Self> {
        match (topic, kind) {
            (TopicId::Group(group_id), TopicMessageKind::Group) => Some(Self::Group {
                group_id: *group_id,
            }),
            (TopicId::Group(group_id), TopicMessageKind::GroupAuthorization) => {
                Some(Self::GroupAuthorization {
                    group_id: *group_id,
                })
            }
            (TopicId::Realm(realm_id), TopicMessageKind::RealmAuthorization) => {
                Some(Self::RealmAuthorization {
                    realm_id: *realm_id,
                })
            }
            (TopicId::Realm(realm_id), TopicMessageKind::RealmConfig) => Some(Self::RealmConfig {
                realm_id: *realm_id,
            }),
            (TopicId::Users(realm_id), TopicMessageKind::User { user_id })
                if user_id.realm_id == *realm_id =>
            {
                Some(Self::User { user_id: *user_id })
            }
            _ => None,
        }
    }

    pub fn announce_timer_key(&self) -> TaskKey {
        TaskKey::TopicAnnounce(self.topic_id())
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
    pub trace_context: Option<DistributedTraceContext>,
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
            trace_context: None,
        }
    }

    pub fn with_trace_context(mut self, trace_context: Option<DistributedTraceContext>) -> Self {
        self.trace_context = trace_context;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomergeClock {
    pub heads: Vec<ChangeHash>,
    pub change_count: u64,
}

impl AutomergeClock {
    pub fn new(heads: Vec<ChangeHash>, change_count: u64) -> Self {
        Self {
            heads,
            change_count,
        }
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

#[cfg(test)]
mod tests {
    use super::AutomergeDocumentVariant;
    use crate::gossip::TopicMessageKind;
    use crate::id::TopicId;
    use crate::structs::RealmId;
    use crate::types::{GroupId, UserId};
    use ulid::Ulid;

    #[test]
    fn resolves_group_message_variant() {
        let group_id = GroupId::from_bytes([1u8; 16]);
        let topic = TopicId::group(group_id);
        let document =
            AutomergeDocumentVariant::from_topic_message(&topic, &TopicMessageKind::Group)
                .expect("group document resolves");
        assert_eq!(document, AutomergeDocumentVariant::Group { group_id });
    }

    #[test]
    fn resolves_realm_message_variant() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let topic = TopicId::realm(realm_id);
        let document = AutomergeDocumentVariant::from_topic_message(
            &topic,
            &TopicMessageKind::RealmAuthorization,
        )
        .expect("realm document resolves");
        assert_eq!(
            document,
            AutomergeDocumentVariant::RealmAuthorization { realm_id }
        );
    }

    #[test]
    fn resolves_user_message_variant() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let user_id = UserId::new(Ulid::from_bytes([4u8; 16]), realm_id);
        let topic = TopicId::users(realm_id);
        let document = AutomergeDocumentVariant::from_topic_message(
            &topic,
            &TopicMessageKind::User { user_id },
        )
        .expect("user document resolves");
        assert_eq!(document, AutomergeDocumentVariant::User { user_id });
        assert_eq!(document.topic_id(), topic);
        assert!(
            AutomergeDocumentVariant::from_topic_message(
                &TopicId::realm(realm_id),
                &TopicMessageKind::User { user_id },
            )
            .is_none()
        );
    }
}
