//! Durable group deletion plans, node confirmations and terminal decisions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::errors::{ConversionError, StorageError};
use crate::structs::identity::realm::{RealmConfigDocument, RealmId};
use crate::{NodeId, UserId};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipFence {
    pub pending: BTreeSet<Ulid>,
}

impl MembershipFence {
    pub fn key(realm_id: RealmId) -> crate::types::Key {
        let mut key = b"realm".to_vec();
        key.extend(realm_id.as_bytes());
        key.into()
    }

    pub fn from_value(value: Option<&[u8]>) -> Result<Self, ConversionError> {
        value
            .map(postcard::from_bytes)
            .transpose()
            .map(Option::unwrap_or_default)
            .map_err(Into::into)
    }

    pub fn entry(
        &self,
        realm_id: RealmId,
    ) -> Result<(String, crate::types::Key, crate::types::Value), ConversionError> {
        Ok((
            crate::keyspaces::GROUP_DELETE_KEYSPACE.to_string(),
            Self::key(realm_id),
            postcard::to_allocvec(self)?.into(),
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupDeletePlan {
    pub request_id: Ulid,
    pub group_id: Ulid,
    pub realm_id: RealmId,
    pub owner: UserId,
    pub requested_by: UserId,
    pub coordinator: NodeId,
    pub nodes: BTreeSet<NodeId>,
}

impl GroupDeletePlan {
    pub fn matches_config(&self, config: &RealmConfigDocument) -> bool {
        let nodes = config
            .nodes
            .iter()
            .map(|node| node.node_id.parse::<NodeId>())
            .collect::<Result<BTreeSet<_>, _>>();
        !self.request_id.is_nil()
            && !self.group_id.is_nil()
            && self.realm_id == config.realm_id
            && self.owner.realm_id == self.realm_id
            && !self.owner.is_nil()
            && self.requested_by.realm_id == self.realm_id
            && !self.requested_by.is_nil()
            && self.nodes.contains(&self.coordinator)
            && nodes.is_ok_and(|nodes| nodes.len() == config.nodes.len() && nodes == self.nodes)
    }

    pub fn signing_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        let mut bytes = b"aruna-group-empty-v1\0".to_vec();
        bytes.extend(postcard::to_allocvec(self)?);
        Ok(bytes)
    }

    pub fn cancelled_key(&self) -> Vec<u8> {
        let mut key = self.group_id.to_bytes().to_vec();
        key.extend(self.request_id.to_bytes());
        key
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupDeleteProof {
    pub node_id: NodeId,
    pub signature: iroh::Signature,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupDeleteCertificate {
    pub plan: GroupDeletePlan,
    pub proofs: Vec<GroupDeleteProof>,
}

impl GroupDeleteCertificate {
    pub fn verify(&self) -> bool {
        let Ok(bytes) = self.plan.signing_bytes() else {
            return false;
        };
        let mut nodes = BTreeSet::new();
        !self.plan.request_id.is_nil()
            && !self.plan.group_id.is_nil()
            && !self.plan.owner.is_nil()
            && !self.plan.requested_by.is_nil()
            && self.plan.owner.realm_id == self.plan.realm_id
            && self.plan.requested_by.realm_id == self.plan.realm_id
            && self.plan.nodes.contains(&self.plan.coordinator)
            && self.proofs.iter().all(|proof| {
                nodes.insert(proof.node_id)
                    && proof.node_id.verify(&bytes, &proof.signature).is_ok()
            })
            && nodes == self.plan.nodes
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupDeletePhase {
    Preparing,
    Aborting,
    Cancelled,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupDeleteAction {
    Start {
        group_id: Ulid,
    },
    Prepare {
        plan: Box<GroupDeletePlan>,
    },
    Abort {
        plan: Box<GroupDeletePlan>,
    },
    Commit {
        event: Box<crate::admin_documents::AdminDocumentEvent>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum GroupDeletionError {
    #[error("group deletion is not authorized")]
    Unauthorized,
    #[error("group not found")]
    NotFound,
    #[error("group is not empty: {0}")]
    NotEmpty(String),
    #[error("group deletion unavailable: {0}")]
    Unavailable(String),
    #[error("group deletion conflict: {0}")]
    Conflict(String),
    #[error("invalid group deletion: {0}")]
    Invalid(String),
}

impl From<StorageError> for GroupDeletionError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::TransactionConflict => Self::Conflict("retry the request".into()),
            error => Self::Unavailable(error.to_string()),
        }
    }
}

impl From<ConversionError> for GroupDeletionError {
    fn from(error: ConversionError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupDeleteRecord {
    pub plan: GroupDeletePlan,
    pub phase: GroupDeletePhase,
    pub deleted_by: Option<UserId>,
    pub event: Option<Box<crate::admin_documents::AdminDocumentEvent>>,
}

impl GroupDeleteRecord {
    pub fn blocks_writes(&self) -> bool {
        matches!(
            self.phase,
            GroupDeletePhase::Preparing | GroupDeletePhase::Deleted
        )
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GroupWriteError {
    #[error("group deletion is pending or complete; group resources cannot be created")]
    Frozen,
    #[error("group has been deleted")]
    Deleted,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected group write event")]
    Unexpected,
}

pub fn check_group_write(value: Option<&[u8]>) -> Result<(), GroupWriteError> {
    if let Some(value) = value {
        return match GroupDeleteRecord::from_bytes(value)?.phase {
            GroupDeletePhase::Preparing => Err(GroupWriteError::Frozen),
            GroupDeletePhase::Deleted => Err(GroupWriteError::Deleted),
            GroupDeletePhase::Aborting | GroupDeletePhase::Cancelled => Ok(()),
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::{RealmNode, RealmNodeKind};

    fn certificate() -> GroupDeleteCertificate {
        let keys = [
            iroh::SecretKey::from_bytes(&[1; 32]),
            iroh::SecretKey::from_bytes(&[2; 32]),
        ];
        let realm_id = RealmId::from_bytes([9; 32]);
        let owner = UserId::local(Ulid::from_bytes([3; 16]), realm_id);
        let plan = GroupDeletePlan {
            request_id: Ulid::from_bytes([4; 16]),
            group_id: Ulid::from_bytes([5; 16]),
            realm_id,
            owner,
            requested_by: owner,
            coordinator: keys[0].public(),
            nodes: keys.iter().map(iroh::SecretKey::public).collect(),
        };
        let bytes = plan.signing_bytes().unwrap();
        let proofs = keys
            .iter()
            .map(|key| GroupDeleteProof {
                node_id: key.public(),
                signature: key.sign(&bytes),
            })
            .collect();
        GroupDeleteCertificate { plan, proofs }
    }

    #[test]
    fn confirmations_bind_plan() {
        let valid = certificate();
        assert!(valid.verify());
        let encoded = postcard::to_allocvec(&valid).unwrap();
        assert!(
            postcard::from_bytes::<GroupDeleteCertificate>(&encoded)
                .unwrap()
                .verify()
        );
        let mut missing = valid.clone();
        missing.proofs.pop();
        assert!(!missing.verify());
        let mut duplicate = valid.clone();
        duplicate.proofs[1] = duplicate.proofs[0].clone();
        assert!(!duplicate.verify());
        let mut changed = valid;
        changed.plan.request_id = Ulid::from_bytes([6; 16]);
        assert!(!changed.verify());
    }

    #[test]
    fn membership_includes_devices() {
        let plan = certificate().plan;
        let mut config = RealmConfigDocument::new(plan.realm_id, Vec::new(), 1);
        config.nodes = plan
            .nodes
            .iter()
            .map(|node| RealmNode {
                node_id: node.to_string(),
                kind: if *node == plan.coordinator {
                    RealmNodeKind::Management
                } else {
                    RealmNodeKind::User { owner: plan.owner }
                },
            })
            .collect();
        assert!(plan.matches_config(&config));
        config.nodes.pop();
        assert!(!plan.matches_config(&config));
    }

    #[test]
    fn aborted_decisions_reopen() {
        let mut record = GroupDeleteRecord {
            plan: certificate().plan,
            phase: GroupDeletePhase::Preparing,
            deleted_by: None,
            event: None,
        };
        assert!(check_group_write(None).is_ok());
        for (phase, error) in [
            (GroupDeletePhase::Preparing, GroupWriteError::Frozen),
            (GroupDeletePhase::Deleted, GroupWriteError::Deleted),
        ] {
            record.phase = phase;
            assert_eq!(
                check_group_write(Some(&record.to_bytes().unwrap())),
                Err(error)
            );
        }
        for phase in [GroupDeletePhase::Aborting, GroupDeletePhase::Cancelled] {
            record.phase = phase;
            assert!(check_group_write(Some(&record.to_bytes().unwrap())).is_ok());
        }
        assert!(check_group_write(Some(&[255])).is_err());
    }
}
