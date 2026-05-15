use automerge::ChangeHash;
use craqle::VectorClock;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::id::{NodeId, TopicId};
use crate::types::UserId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMessage {
    pub kind: TopicMessageKind,
    pub message_id: Ulid,
    pub node_id: NodeId,
    pub version: TopicMessageVersion,
}

impl TopicMessage {
    pub fn new(
        kind: TopicMessageKind,
        message_id: Ulid,
        node_id: NodeId,
        version: TopicMessageVersion,
    ) -> Self {
        Self {
            kind,
            message_id,
            node_id,
            version,
        }
    }

    pub fn is_valid_for(&self, topic: &TopicId) -> bool {
        self.kind.allowed_in_topic(topic) && self.version.matches_kind(&self.kind)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicMessageKind {
    RealmAuthorization,
    RealmConfig,
    User { user_id: UserId },
    Group,
    GroupAuthorization,
    Metadata,
}

impl TopicMessageKind {
    pub fn allowed_in_topic(&self, topic: &TopicId) -> bool {
        match (topic, self) {
            (TopicId::Realm(_), Self::RealmAuthorization | Self::RealmConfig) => true,
            (TopicId::Users(realm_id), Self::User { user_id }) => user_id.realm_id == *realm_id,
            (TopicId::Group(_), Self::Group | Self::GroupAuthorization) => true,
            (TopicId::Metadata(_), Self::Metadata) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicMessageVersion {
    Automerge {
        heads: Vec<ChangeHash>,
        change_count: u64,
    },
    Metadata {
        clock: VectorClock,
    },
}

impl TopicMessageVersion {
    pub fn matches_kind(&self, kind: &TopicMessageKind) -> bool {
        matches!(
            (kind, self),
            (
                TopicMessageKind::RealmAuthorization
                    | TopicMessageKind::RealmConfig
                    | TopicMessageKind::User { .. }
                    | TopicMessageKind::Group
                    | TopicMessageKind::GroupAuthorization,
                Self::Automerge { .. }
            ) | (TopicMessageKind::Metadata, Self::Metadata { .. })
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use craqle::{ActorId, VectorClock};

    use super::*;
    use crate::structs::RealmId;
    use crate::types::{GroupId, UserId};

    fn make_node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn validates_topic_and_version_combinations() {
        let realm = TopicId::realm(RealmId::from_bytes([1u8; 32]));
        let users = TopicId::users(RealmId::from_bytes([1u8; 32]));
        let other_users = TopicId::users(RealmId::from_bytes([8u8; 32]));
        let group = TopicId::group(GroupId::from_bytes([2u8; 16]));
        let metadata = TopicId::metadata(Ulid::from_bytes([3u8; 16]));

        let automerge = TopicMessage::new(
            TopicMessageKind::RealmConfig,
            Ulid::new(),
            make_node(4),
            TopicMessageVersion::Automerge {
                heads: Vec::new(),
                change_count: 0,
            },
        );
        assert!(automerge.is_valid_for(&realm));
        assert!(!automerge.is_valid_for(&group));

        let user_message = TopicMessage::new(
            TopicMessageKind::User {
                user_id: UserId::new(Ulid::from_bytes([6u8; 16]), RealmId::from_bytes([1u8; 32])),
            },
            Ulid::new(),
            make_node(6),
            TopicMessageVersion::Automerge {
                heads: Vec::new(),
                change_count: 0,
            },
        );
        assert!(user_message.is_valid_for(&users));
        assert!(!user_message.is_valid_for(&realm));
        assert!(!user_message.is_valid_for(&other_users));
        assert!(!user_message.is_valid_for(&group));

        let metadata_message = TopicMessage::new(
            TopicMessageKind::Metadata,
            Ulid::new(),
            make_node(5),
            TopicMessageVersion::Metadata {
                clock: VectorClock(BTreeMap::from([(ActorId::from_bytes([7u8; 32]), 1)])),
            },
        );
        assert!(metadata_message.is_valid_for(&metadata));
        assert!(!metadata_message.is_valid_for(&realm));
    }
}
