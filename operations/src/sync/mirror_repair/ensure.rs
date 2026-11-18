//! Brings up the target mirror of an active relationship under its creator's current rights.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value};
use aruna_core::structs::SyncRelationship;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::types::GroupId;

use super::{MirrorPeer, SyncMirrorError, create_peer};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::metadata::mirror_request::MirrorRequestOperation;
use crate::sync::sync_relationship::{
    StoreRelationshipOperation, SyncRelationshipDirection, SyncRelationshipError,
};

#[derive(Debug, PartialEq)]
enum Phase {
    SourceBucket,
    SourceAccess(GroupId),
    TargetBucket,
    TargetAccess,
    Storing,
    Requesting,
}

/// Checks that the creator may still read the source, then stores the mirror here or asks the
/// remote target for it. A local target also needs the creator's write access.
#[derive(Debug, PartialEq)]
pub(super) struct EnsureMirrorOperation {
    local_node: NodeId,
    relationship: SyncRelationship,
    phase: Phase,
}

impl EnsureMirrorOperation {
    pub(super) fn new(local_node: NodeId, relationship: SyncRelationship) -> Self {
        Self {
            local_node,
            relationship,
            phase: Phase::SourceBucket,
        }
    }

    fn source_access(&mut self, group_id: GroupId) -> Next<()> {
        self.phase = Phase::SourceAccess(group_id);
        let source = &self.relationship.source;
        let path = source.bucket().map_or_else(String::new, |bucket| {
            bucket_permission_path(source.realm_id, group_id, source.node_id, bucket)
        });
        self.access(source.realm_id, path, Permission::READ, "s3.GetObject")
    }

    fn target_bucket(&mut self) -> Result<Next<()>, SyncMirrorError> {
        let bucket = self
            .relationship
            .target
            .bucket()
            .ok_or_else(|| SyncMirrorError::Mirror("invalid target ARN".to_string()))?;
        let read = bucket_read(bucket);
        self.phase = Phase::TargetBucket;
        Ok(Next::Call(read))
    }

    fn target_access(&mut self, group_id: GroupId) -> Next<()> {
        self.phase = Phase::TargetAccess;
        let target = &self.relationship.target;
        let path = target.bucket().map_or_else(String::new, |bucket| {
            bucket_permission_path(target.realm_id, group_id, target.node_id, bucket)
        });
        let operation = "s3.PutBucketReplication";
        self.access(target.realm_id, path, Permission::WRITE, operation)
    }

    fn access(
        &self,
        realm_id: RealmId,
        path: String,
        permission: Permission,
        operation: &'static str,
    ) -> Next<()> {
        let auth = AuthContext {
            user_id: self.relationship.created_by,
            realm_id: self.relationship.created_by.realm_id,
            path_restrictions: None,
            session: None,
        };
        let extras = PolicyRequestExtras::operation(operation);
        let check = AuthorizeOperation::new(realm_id, &auth, &path, &permission, extras);
        Next::Call(IoCall::child(check))
    }

    fn source_allowed(&mut self, group_id: GroupId) -> Result<Next<()>, SyncMirrorError> {
        match create_peer(&self.relationship, self.local_node)? {
            MirrorPeer::Local => self.target_bucket(),
            MirrorPeer::Remote(target) => {
                self.phase = Phase::Requesting;
                Ok(Next::Call(IoCall::child(MirrorRequestOperation::create(
                    target,
                    creator_token(&self.relationship),
                    group_id,
                    self.relationship.clone(),
                    PolicyRequestExtras::operation("s3.PutBucketReplication"),
                ))))
            }
        }
    }
}

impl Operation for EnsureMirrorOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<()>, SyncMirrorError> {
        self.relationship.validate()?;
        create_peer(&self.relationship, self.local_node)?;
        let bucket = self
            .relationship
            .source
            .bucket()
            .ok_or_else(|| SyncMirrorError::Mirror("invalid source ARN".to_string()))?;
        Ok(Next::Call(bucket_read(bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncMirrorError> {
        match self.phase {
            Phase::SourceBucket => {
                let group_id = bucket_group(result, "source")?;
                Ok(self.source_access(group_id))
            }
            Phase::SourceAccess(group_id) => {
                allowed(result)?;
                self.source_allowed(group_id)
            }
            Phase::TargetBucket => {
                let group_id = bucket_group(result, "target")?;
                Ok(self.target_access(group_id))
            }
            Phase::TargetAccess => {
                allowed(result)?;
                self.phase = Phase::Storing;
                let incoming = SyncRelationshipDirection::Incoming;
                let store = StoreRelationshipOperation::new(self.relationship.clone(), incoming);
                Ok(Next::Call(IoCall::child(store)))
            }
            Phase::Storing => {
                result.child::<SyncRelationship, SyncRelationshipError>()??;
                Ok(Next::Done(()))
            }
            Phase::Requesting => {
                result.child::<(), MetadataError>()?.map_err(peer_failure)?;
                Ok(Next::Done(()))
            }
        }
    }
}

fn bucket_read(bucket: &str) -> IoCall {
    read_row(S3_BUCKET_KEYSPACE, bucket.as_bytes().to_vec())
}

/// The owning group of a local endpoint bucket; `side` names the endpoint in the error.
fn bucket_group(result: IoResult, side: &str) -> Result<GroupId, SyncMirrorError> {
    let bytes = value(result)?
        .ok_or_else(|| SyncMirrorError::Mirror(format!("{side} bucket not found")))?;
    Ok(BucketInfo::from_bytes(&bytes)?.group_id)
}

/// A denial is recorded as `access_denied`; any other failure keeps its text.
fn allowed(result: IoResult) -> Result<(), SyncMirrorError> {
    result
        .child::<(), AuthorizeError>()?
        .map_err(|error| match error {
            AuthorizeError::PermissionDenied | AuthorizeError::Policy(_) => {
                SyncMirrorError::Mirror("access_denied".to_string())
            }
            AuthorizeError::CheckFailed(error) => SyncMirrorError::Mirror(error),
            AuthorizeError::Storage(error) => SyncMirrorError::Mirror(error.to_string()),
        })
}

/// The creator's token, which the peer checks against its own rights.
pub(super) fn creator_token(relationship: &SyncRelationship) -> AuthToken {
    AuthToken::internal(AuthContext {
        user_id: relationship.created_by,
        realm_id: relationship.source.realm_id,
        path_restrictions: None,
        session: None,
    })
}

/// A node without the peer transport cannot reach the other endpoint.
pub(super) fn peer_failure(error: MetadataError) -> SyncMirrorError {
    match error {
        MetadataError::HandleMissing => SyncMirrorError::Mirror("target unreachable".to_string()),
        other => SyncMirrorError::Mirror(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::format::Compression;
    use byteview::ByteView;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn group() -> GroupId {
        Ulid::from_bytes([6; 16])
    }

    fn bucket(created_by: UserId) -> IoResult {
        let info = BucketInfo {
            group_id: group(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        IoResult::Values(vec![Some(ByteView::from(info.to_bytes().unwrap()))])
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn granted() -> IoResult {
        child::<(), AuthorizeError>(Ok(()))
    }

    fn local() -> SyncRelationship {
        let mut relationship = relationship();
        relationship.target.node_id = relationship.source.node_id;
        relationship
    }

    fn checked_source(relationship: SyncRelationship) -> EnsureMirrorOperation {
        let node = relationship.source.node_id;
        let created_by = relationship.created_by;
        let mut operation = EnsureMirrorOperation::new(node, relationship);
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        let next = operation.next(bucket(created_by));
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        operation
    }

    #[test]
    fn asks_remote_target() {
        let mut operation = checked_source(relationship());
        let next = operation.next(granted());
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(operation.phase, Phase::Requesting);
        let done = child::<(), MetadataError>(Ok(()));
        assert_eq!(operation.next(done), Ok(Next::Done(())));
    }

    #[test]
    fn stores_local_mirror() {
        let local = local();
        let created_by = local.created_by;
        let mut operation = checked_source(local.clone());
        let next = operation.next(granted());
        assert!(matches!(next, Ok(Next::Call(IoCall::Read(_)))));
        let next = operation.next(bucket(created_by));
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        let next = operation.next(granted());
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        let stored = child::<_, SyncRelationshipError>(Ok(local));
        assert_eq!(operation.next(stored), Ok(Next::Done(())));
    }

    #[test]
    fn names_each_failure() {
        let mirror = |message: &str| Err(SyncMirrorError::Mirror(message.to_string()));
        let node = relationship().source.node_id;
        let mut missing = EnsureMirrorOperation::new(node, relationship());
        assert_eq!(
            missing.next(IoResult::Values(vec![None])),
            mirror("source bucket not found")
        );
        let denied = child::<(), _>(Err(AuthorizeError::PermissionDenied));
        assert_eq!(
            checked_source(relationship()).next(denied),
            mirror("access_denied")
        );
        let mut requesting = checked_source(relationship());
        requesting.next(granted()).unwrap();
        let unreachable = child::<(), _>(Err(MetadataError::HandleMissing));
        assert_eq!(requesting.next(unreachable), mirror("target unreachable"));
    }

    #[test]
    fn refuses_foreign_node() {
        let mut operation =
            EnsureMirrorOperation::new(relationship().target.node_id, relationship());
        assert!(operation.first().is_err());
    }
}
