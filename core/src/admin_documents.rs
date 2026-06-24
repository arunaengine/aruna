use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{Actor, RealmId};
use crate::types::{GroupId, RoleId, UserId};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentClock {
    pub origins: BTreeMap<NodeId, u64>,
}

impl AdminDocumentClock {
    pub fn sequence_for(&self, origin_node_id: &NodeId) -> u64 {
        self.origins
            .get(origin_node_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn observes(&self, dot: &AdminDocumentDot) -> bool {
        self.sequence_for(&dot.origin_node_id) >= dot.origin_seq
    }

    pub fn advance(&mut self, origin_node_id: NodeId, origin_seq: u64) {
        let current = self.origins.entry(origin_node_id).or_default();
        *current = (*current).max(origin_seq);
    }

    pub fn with_observed(mut self, origin_node_id: NodeId, origin_seq: u64) -> Self {
        self.advance(origin_node_id, origin_seq);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AdminDocumentDot {
    pub event_id: Ulid,
    pub origin_node_id: NodeId,
    pub origin_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminDocumentTarget {
    Group { group_id: GroupId },
    Realm { realm_id: RealmId },
    User { user_id: UserId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminDocumentOperation {
    GroupRoleAdded { role_id: RoleId },
    GroupRoleUserAssignmentAdded { role_id: RoleId, user_id: UserId },
    GroupRoleUserAssignmentRemoved { role_id: RoleId, user_id: UserId },
    UserAttributeSet { key: String, value: String },
    UserAttributeRemoved { key: String },
    UserNameSet { name: String },
    UserSubjectIdAdded { subject_id: String },
    UserSubjectIdRemoved { subject_id: String },
    RealmRoleAdded { role_id: RoleId },
    RealmRoleUserAssignmentAdded { role_id: RoleId, user_id: UserId },
    RealmRoleUserAssignmentRemoved { role_id: RoleId, user_id: UserId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentEvent {
    pub event_id: Ulid,
    pub target: AdminDocumentTarget,
    pub origin_node_id: NodeId,
    pub origin_seq: u64,
    pub observed: AdminDocumentClock,
    pub actor: Actor,
    pub op: AdminDocumentOperation,
}

impl AdminDocumentEvent {
    pub fn dot(&self) -> AdminDocumentDot {
        AdminDocumentDot {
            event_id: self.event_id,
            origin_node_id: self.origin_node_id,
            origin_seq: self.origin_seq,
        }
    }
}
