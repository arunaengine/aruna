//! Serves the sync mirror a source node creates or removes on this node, under the caller's
//! WRITE on the local bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::events::PeerAuthRefusal;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRelationship;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::format::Compression;
use aruna_core::types::GroupId;

use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::write_peer::WritePeerOperation;
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::staging::bucket_guard::{BucketGuardError, BucketGuardOperation};
use crate::sync::sync_relationship::{
    DeleteRelationshipOperation, GetRelationshipOperation, RemoveOutgoingOperation,
    StoreRelationshipOperation, SyncRelationshipDirection, SyncRelationshipError,
};
use aruna_core::request_policy::PolicyRequestExtras;

pub(crate) fn sync_identity_matches(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.id == right.id
        && left.source == right.source
        && left.target == right.target
        && left.mode == right.mode
        && left.replicate_deletes == right.replicate_deletes
        && left.created_by == right.created_by
        && left.created_at == right.created_at
}

/// The sender of a mirror rollback, as the transport authenticated it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RollbackRequest {
    pub(crate) peer: NodeId,
    pub(crate) local_node: NodeId,
    /// The token was node-vouched, never a user bearer.
    pub(crate) vouched: bool,
}

impl RollbackRequest {
    /// Only the source peer of this exact incoming mirror may roll it back without WRITE.
    pub(crate) fn allows(&self, stored: &SyncRelationship, request: &SyncRelationship) -> bool {
        self.vouched
            && request.source.node_id == self.peer
            && request.target.node_id == self.local_node
            && sync_identity_matches(stored, request)
    }
}

pub(crate) fn valid_sync_request(
    relationship: &SyncRelationship,
    (source_bucket, target_bucket): (&str, &str),
    realm_id: RealmId,
    user_id: UserId,
    delete: bool,
) -> bool {
    !source_bucket.is_empty()
        && !target_bucket.is_empty()
        && relationship.source.key_prefix() != Some("")
        && relationship.target.key_prefix() != Some("")
        && relationship.source.realm_id == realm_id
        && relationship.target.realm_id == realm_id
        && relationship.created_by == user_id
        && (delete || (!source_bucket.starts_with("ws-") && !target_bucket.starts_with("ws-")))
}

/// The bucket a mirror creates on its target when the target holds none yet.
pub(crate) fn sync_bucket(group_id: GroupId, relationship: &SyncRelationship) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: relationship.created_at,
        created_by: relationship.created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

/// A sync mirror request: a create names the source group for a bucket this node lacks.
#[derive(Debug, PartialEq)]
pub(crate) struct MirrorRequest {
    pub(crate) token: Option<AuthToken>,
    pub(crate) relationship: SyncRelationship,
    pub(crate) source_group: Option<GroupId>,
    pub(crate) extras: Option<PolicyRequestExtras>,
    pub(crate) delete: bool,
    pub(crate) rollback: bool,
}

#[derive(Debug, PartialEq)]
enum MirrorStage {
    Caller,
    Guard,
    Bucket,
    /// Authorizes the create, then creates the bucket under this group when it is missing.
    Authorize(Option<GroupId>),
    Create,
    Store,
    Stored,
    Owner(Box<SyncRelationship>),
    Allow(Box<SyncRelationship>),
    Remove,
}

/// Creates or removes the sync mirror `peer` asks for and answers its outcome or the refusal.
#[derive(Debug, PartialEq)]
pub(crate) struct MirrorServeOperation {
    peer: NodeId,
    local: Option<(NodeId, RealmId)>,
    request: MirrorRequest,
    /// The token was node-vouched, which only a rollback needs.
    vouched: bool,
    auth: Option<AuthContext>,
    bucket: String,
    direction: SyncRelationshipDirection,
    stage: MirrorStage,
}

type MirrorTurn = Result<Next<MetadataTransportMessage>, IoError>;

fn reject(reason: &str) -> MirrorTurn {
    Ok(Next::Done(MetadataTransportMessage::Reject(
        reason.to_string(),
    )))
}

fn done(reply: MetadataTransportMessage) -> MirrorTurn {
    Ok(Next::Done(reply))
}

impl MirrorServeOperation {
    pub(crate) fn new(
        peer: NodeId,
        local: Option<(NodeId, RealmId)>,
        request: MirrorRequest,
    ) -> Self {
        Self {
            peer,
            local,
            vouched: matches!(request.token, Some(AuthToken::Internal(_))),
            request,
            auth: None,
            bucket: String::new(),
            direction: SyncRelationshipDirection::Incoming,
            stage: MirrorStage::Caller,
        }
    }

    fn call(&mut self, stage: MirrorStage, call: IoCall) -> MirrorTurn {
        self.stage = stage;
        Ok(Next::Call(call))
    }

    // Checks the relationship against the caller and finds this node's side of it.
    fn accepted(&mut self, (node, realm_id): (NodeId, RealmId), auth: AuthContext) -> MirrorTurn {
        let relationship = &self.request.relationship;
        let delete = self.request.delete;
        let (Some(source), Some(target)) =
            (relationship.source.bucket(), relationship.target.bucket())
        else {
            return reject("invalid_relationship");
        };
        if !valid_sync_request(
            relationship,
            (source, target),
            realm_id,
            auth.user_id,
            delete,
        ) {
            return reject("invalid_relationship");
        }
        let (bucket, direction) =
            if relationship.source.node_id == self.peer && relationship.target.node_id == node {
                (target, SyncRelationshipDirection::Incoming)
            } else if delete
                && relationship.target.node_id == self.peer
                && relationship.source.node_id == node
            {
                (source, SyncRelationshipDirection::Outgoing)
            } else {
                return reject("invalid_relationship");
            };
        self.bucket = bucket.to_string();
        self.direction = direction;
        self.auth = Some(auth);
        match delete {
            true => {
                let read = GetRelationshipOperation::new(relationship.id, direction);
                self.call(MirrorStage::Stored, IoCall::child(read))
            }
            // A read-only bucket refuses a mirror like a denied write, before any bucket exists.
            false => {
                let guard = BucketGuardOperation::new(self.bucket.clone());
                self.call(MirrorStage::Guard, IoCall::child(guard))
            }
        }
    }

    fn authorize(
        &mut self,
        stage: MirrorStage,
        (node, realm_id): (NodeId, RealmId),
        group: GroupId,
    ) -> MirrorTurn {
        let path = bucket_permission_path(realm_id, group, node, &self.bucket);
        let (Some(auth), Some(extras)) = (self.auth.as_ref(), self.request.extras.take()) else {
            return reject("mirror_internal");
        };
        let check = AuthorizeOperation::new(realm_id, auth, &path, &Permission::WRITE, extras);
        self.call(stage, IoCall::child(check))
    }

    fn store(&mut self) -> MirrorTurn {
        let store =
            StoreRelationshipOperation::new(self.request.relationship.clone(), self.direction);
        self.call(MirrorStage::Store, IoCall::child(store))
    }

    // Retains the peer's readable reference records when an outgoing mirror is detached.
    fn remove(&mut self, stored: SyncRelationship) -> MirrorTurn {
        let call = match self.direction {
            SyncRelationshipDirection::Outgoing => {
                IoCall::child(RemoveOutgoingOperation::new(stored))
            }
            SyncRelationshipDirection::Incoming => {
                IoCall::child(DeleteRelationshipOperation::new(stored, self.direction))
            }
        };
        self.call(MirrorStage::Remove, call)
    }
}

// The refusal of a failed WRITE check; a fault deciding it is internal.
fn denied(error: AuthorizeError) -> MirrorTurn {
    match error {
        AuthorizeError::CheckFailed(_) | AuthorizeError::Storage(_) => reject("mirror_internal"),
        _ => reject("access_denied"),
    }
}

impl Operation for MirrorServeOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> MirrorTurn {
        if self.local.is_none() {
            return reject("mirror_internal");
        }
        let Some(token) = self.request.token.take() else {
            return reject("access_denied");
        };
        let check = WritePeerOperation::mirror(self.peer, token);
        self.call(MirrorStage::Caller, IoCall::child(check))
    }

    fn next(&mut self, result: IoResult) -> MirrorTurn {
        let local = self.local.ok_or(IoError::Unexpected {
            expected: "this node's realm",
            got: "none",
        })?;
        match std::mem::replace(&mut self.stage, MirrorStage::Remove) {
            MirrorStage::Caller => match result.child::<AuthContext, PeerAuthRefusal>()? {
                Ok(auth) => self.accepted(local, auth),
                Err(_) => reject("access_denied"),
            },
            MirrorStage::Guard => match result.child::<(), BucketGuardError>()? {
                Ok(()) => {
                    let read = GetBucketOperation::new(self.bucket.clone());
                    self.call(MirrorStage::Bucket, IoCall::child(read))
                }
                Err(BucketGuardError::ReadOnly(_)) => reject("access_denied"),
                Err(_) => reject("mirror_internal"),
            },
            MirrorStage::Bucket => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => self.authorize(MirrorStage::Authorize(None), local, info.group_id),
                Err(GetBucketError::NotFound) => match self.request.source_group {
                    Some(group) => {
                        self.authorize(MirrorStage::Authorize(Some(group)), local, group)
                    }
                    None => reject("invalid_relationship"),
                },
                Err(_) => reject("mirror_internal"),
            },
            MirrorStage::Authorize(create) => match result.child::<(), AuthorizeError>()? {
                Ok(()) => match create {
                    Some(group) => {
                        let info = sync_bucket(group, &self.request.relationship);
                        let create = CreateBucketOperation::new(self.bucket.clone(), info);
                        self.call(MirrorStage::Create, IoCall::child(create))
                    }
                    None => self.store(),
                },
                Err(error) => denied(error),
            },
            MirrorStage::Create => match result.child::<BucketInfo, CreateBucketError>()? {
                Ok(_) => self.store(),
                Err(_) => reject("mirror_internal"),
            },
            MirrorStage::Store => {
                match result.child::<SyncRelationship, SyncRelationshipError>()? {
                    Ok(_) => done(MetadataTransportMessage::SyncMirrorCreated),
                    Err(_) => reject("mirror_internal"),
                }
            }
            MirrorStage::Stored => {
                let stored = match result.child::<SyncRelationship, SyncRelationshipError>()? {
                    Ok(stored) => stored,
                    Err(SyncRelationshipError::NotFound) => {
                        return done(MetadataTransportMessage::SyncMirrorDeleted);
                    }
                    Err(_) => return reject("mirror_internal"),
                };
                let relationship = &self.request.relationship;
                if !sync_identity_matches(&stored, relationship) {
                    return reject("invalid_relationship");
                }
                if !self.request.rollback {
                    let read = GetBucketOperation::new(self.bucket.clone());
                    return self.call(MirrorStage::Owner(Box::new(stored)), IoCall::child(read));
                }
                // A rollback never falls back to the WRITE-checked delete.
                let rollback = RollbackRequest {
                    peer: self.peer,
                    local_node: local.0,
                    vouched: self.vouched,
                };
                match rollback.allows(&stored, relationship) {
                    true => self.remove(stored),
                    false => reject("invalid_relationship"),
                }
            }
            MirrorStage::Owner(stored) => match result.child::<BucketInfo, GetBucketError>()? {
                // A present bucket must pass the origin's authorization boundary.
                Ok(info) => self.authorize(MirrorStage::Allow(stored), local, info.group_id),
                Err(GetBucketError::NotFound) => self.remove(*stored),
                Err(_) => reject("mirror_internal"),
            },
            MirrorStage::Allow(stored) => match result.child::<(), AuthorizeError>()? {
                Ok(()) => self.remove(*stored),
                Err(error) => denied(error),
            },
            MirrorStage::Remove => match result.child::<(), SyncRelationshipError>()? {
                Ok(()) => done(MetadataTransportMessage::SyncMirrorDeleted),
                Err(_) => reject("mirror_internal"),
            },
        }
    }
}
