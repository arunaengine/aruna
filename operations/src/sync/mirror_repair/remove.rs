//! Removes the local relationship records and the mirror of the other endpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::MetadataError;
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRelationship;

use super::ensure::{creator_token, peer_failure};
use super::{MirrorPeer, SyncMirrorError, delete_peer};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::metadata::mirror_request::MirrorRequestOperation;
use crate::sync::sync_relationship::{
    DeleteRelationshipOperation, RemoveOutgoingOperation, SyncRelationshipDirection,
    SyncRelationshipError,
};

/// Removes this node's records of `relationship`: the outgoing one on the source, keeping a
/// stub when it serves references, and the incoming one on the target.
#[derive(Debug, PartialEq)]
pub(super) struct RemoveLocalOperation {
    relationship: SyncRelationship,
    outgoing: bool,
    incoming: bool,
}

impl RemoveLocalOperation {
    pub(super) fn new(local_node: NodeId, relationship: SyncRelationship) -> Self {
        Self {
            outgoing: relationship.source.node_id == local_node,
            incoming: relationship.target.node_id == local_node,
            relationship,
        }
    }

    fn step(&mut self) -> Next<()> {
        if std::mem::take(&mut self.outgoing) {
            let remove = RemoveOutgoingOperation::new(self.relationship.clone());
            return Next::Call(IoCall::child(remove));
        }
        if std::mem::take(&mut self.incoming) {
            return Next::Call(incoming_delete(&self.relationship));
        }
        Next::Done(())
    }
}

impl Operation for RemoveLocalOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<()>, SyncMirrorError> {
        Ok(self.step())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncMirrorError> {
        result.child::<(), SyncRelationshipError>()??;
        Ok(self.step())
    }
}

#[derive(Debug, PartialEq)]
enum Phase {
    Incoming,
    Local,
    Requesting,
}

/// Deletes the remote or local mirror. A rollback removes only the target mirror of a create
/// that never activated, without the creator's WRITE. A peer without the mirror counts as done.
#[derive(Debug, PartialEq)]
pub struct RemoveMirrorOperation {
    local_node: NodeId,
    relationship: SyncRelationship,
    rollback: bool,
    phase: Phase,
}

impl RemoveMirrorOperation {
    /// The ordinary delete of the mirror of `relationship`.
    pub fn new(local_node: NodeId, relationship: SyncRelationship) -> Self {
        Self::with(local_node, relationship, false)
    }

    pub(super) fn rollback(local_node: NodeId, relationship: SyncRelationship) -> Self {
        Self::with(local_node, relationship, true)
    }

    fn with(local_node: NodeId, relationship: SyncRelationship, rollback: bool) -> Self {
        Self {
            local_node,
            relationship,
            rollback,
            phase: Phase::Requesting,
        }
    }
}

impl Operation for RemoveMirrorOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<()>, SyncMirrorError> {
        self.relationship.validate()?;
        let call = match delete_peer(&self.relationship, self.local_node, self.rollback)? {
            MirrorPeer::Local if self.rollback => {
                self.phase = Phase::Incoming;
                incoming_delete(&self.relationship)
            }
            MirrorPeer::Local => {
                self.phase = Phase::Local;
                let relationship = self.relationship.clone();
                IoCall::child(RemoveLocalOperation::new(self.local_node, relationship))
            }
            MirrorPeer::Remote(peer) => {
                self.phase = Phase::Requesting;
                IoCall::child(MirrorRequestOperation::delete(
                    peer,
                    creator_token(&self.relationship),
                    self.relationship.clone(),
                    PolicyRequestExtras::operation("s3.DeleteBucketReplication"),
                    self.rollback,
                ))
            }
        };
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncMirrorError> {
        match self.phase {
            Phase::Incoming => result.child::<(), SyncRelationshipError>()??,
            Phase::Local => result.child::<(), SyncMirrorError>()??,
            Phase::Requesting => match result.child::<(), MetadataError>()? {
                Ok(()) => {}
                Err(MetadataError::Backend(message)) if message == "not_found" => {}
                Err(error) => return Err(peer_failure(error)),
            },
        }
        Ok(Next::Done(()))
    }
}

fn incoming_delete(relationship: &SyncRelationship) -> IoCall {
    IoCall::child(DeleteRelationshipOperation::new(
        relationship.clone(),
        SyncRelationshipDirection::Incoming,
    ))
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn answer<E>(result: Result<(), E>) -> IoResult
    where
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn is_child(next: Result<Next<()>, SyncMirrorError>) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    fn both_local() -> SyncRelationship {
        let mut relationship = relationship();
        relationship.target.node_id = relationship.source.node_id;
        relationship
    }

    #[test]
    fn removes_local_records() {
        let local = both_local();
        let mut operation = RemoveLocalOperation::new(local.source.node_id, local);
        assert!(is_child(operation.first()));
        assert!(is_child(
            operation.next(answer::<SyncRelationshipError>(Ok(())))
        ));
        assert_eq!(
            operation.next(answer::<SyncRelationshipError>(Ok(()))),
            Ok(Next::Done(()))
        );
        let other = relationship();
        let stranger = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let mut nothing = RemoveLocalOperation::new(stranger, other);
        assert_eq!(nothing.first(), Ok(Next::Done(())));
    }

    #[test]
    fn missing_mirror_counts() {
        let node = relationship().source.node_id;
        let mut operation = RemoveMirrorOperation::new(node, relationship());
        assert!(is_child(operation.first()));
        let gone = answer(Err(MetadataError::Backend("not_found".to_string())));
        assert_eq!(operation.next(gone), Ok(Next::Done(())));
    }

    #[test]
    fn reports_peer_failure() {
        let node = relationship().source.node_id;
        let mut operation = RemoveMirrorOperation::rollback(node, relationship());
        assert!(is_child(operation.first()));
        assert_eq!(
            operation.next(answer(Err(MetadataError::HandleMissing))),
            Err(SyncMirrorError::Mirror("target unreachable".to_string()))
        );
    }

    #[test]
    fn rollback_keeps_outgoing() {
        let local = both_local();
        let mut operation = RemoveMirrorOperation::rollback(local.source.node_id, local);
        assert!(is_child(operation.first()));
        assert_eq!(operation.phase, Phase::Incoming);
        let deleted = answer::<SyncRelationshipError>(Ok(()));
        assert_eq!(operation.next(deleted), Ok(Next::Done(())));
    }

    #[test]
    fn refuses_foreign_node() {
        let stranger = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let mut operation = RemoveMirrorOperation::new(stranger, relationship());
        assert!(matches!(operation.first(), Err(SyncMirrorError::Mirror(_))));
    }
}
