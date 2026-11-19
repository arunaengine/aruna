//! Carries distributed bucket configuration between nodes: a member creates and admits its
//! bucket for a forwarded bearer token (D12); admin requests and statements reach the publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Only the publisher accepts them (D33). Nothing is queued.

use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::events::PeerAuthRefusal;
use aruna_core::heartbeat::Here;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{NodeRealm, RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::distribution::capacity::RangeSize;
use aruna_core::structs::storage::distribution::request::PublishRefusal;
use aruna_core::structs::storage::distribution::snapshot::{DistributionSnapshot, SignedSnapshot};
use aruna_core::structs::storage::distribution::statement::SignedStatement;
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionMember};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::synced_folder::SyncRefusal;
use tracing::warn;
use ulid::Ulid;

use crate::auth::error::{AuthorizeError, pull_refusal};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::distribution::admit::{AdmitConfig, AdmitError, AdmitOperation};
use crate::distribution::publish::PublishError;
use crate::document_repository::{config_of, realm_config};
use crate::forward::transport::reject;
use crate::forward::write_peer::WritePeerOperation;
use crate::realm::peer_trust::{is_sync_eligible, peer_acts_for};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::delete::{DeleteBucketError, DeleteBucketOperation};
use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::metadata::protocol::MetadataTransportMessage;
use aruna_core::request_policy::PolicyRequestExtras;

impl From<PublishError> for PublishRefusal {
    fn from(error: PublishError) -> Self {
        use aruna_core::structs::storage::distribution::validate::DistributionError;
        match error {
            PublishError::Unauthorized => Self::Forbidden,
            PublishError::Distribution(DistributionError::Replayed(_)) => Self::Replayed,
            PublishError::Storage(_) | PublishError::SignUnavailable(_) => {
                Self::Unavailable("the publisher could not commit the change".to_string())
            }
            other => Self::Rejected(other.to_string()),
        }
    }
}

fn bearer_only(auth_token: &AuthToken) -> bool {
    matches!(auth_token, AuthToken::Bearer(_))
}

/// The caller of a forwarded configuration request. Only the caller's own bearer token
/// counts; a peer-asserted identity never acts as a group admin (D12, K1).
#[derive(Debug, PartialEq)]
pub(crate) struct PeerCallerOperation {
    peer: NodeId,
    token: Option<AuthToken>,
    auth: Option<AuthContext>,
}

impl PeerCallerOperation {
    pub(crate) fn new(peer: NodeId, token: AuthToken) -> Self {
        Self {
            peer,
            token: Some(token),
            auth: None,
        }
    }
}

impl Operation for PeerCallerOperation {
    type Output = AuthContext;
    type Error = SyncRefusal;

    fn first(&mut self) -> Result<Next<AuthContext>, SyncRefusal> {
        let token = self.token.take().ok_or(IoError::Unfinished)?;
        if !bearer_only(&token) {
            return Err(SyncRefusal::Unauthorized);
        }
        let check = WritePeerOperation::new(self.peer, token);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuthContext>, SyncRefusal> {
        let Some(auth) = self.auth.take() else {
            let auth =
                (result.child::<AuthContext, PeerAuthRefusal>()?).map_err(|error| match error {
                    PeerAuthRefusal::Unauthorized => SyncRefusal::Unauthorized,
                    PeerAuthRefusal::Unavailable(_) => SyncRefusal::Unavailable,
                })?;
            let read = realm_config(auth.realm_id);
            self.auth = Some(auth);
            return Ok(Next::Call(read));
        };
        let config = config_of(result)?.ok_or(SyncRefusal::Unavailable)?;
        match peer_acts_for(&config, self.peer, auth.user_id) {
            true => Ok(Next::Done(auth)),
            false => Err(SyncRefusal::Unauthorized),
        }
    }
}

/// The member's side of a forwarded admission; see [`ServeAdmissionOperation`].
pub(crate) fn serve_admission(
    node: Here,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> ServeAdmissionOperation {
    ServeAdmissionOperation::new((node.ids, node.capacity), peer, message)
}

/// The member's side of a forwarded admission: the bearer token is checked again here.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeAdmissionOperation {
    ids: NodeRealm,
    capacity: Option<u64>,
    peer: NodeId,
    message: Option<MetadataTransportMessage>,
    admission: Option<(Box<SignedSnapshot>, Ulid, Vec<RangeSize>)>,
    member_id: Option<Ulid>,
}

impl ServeAdmissionOperation {
    pub(crate) fn new(
        (ids, capacity): (NodeRealm, Option<u64>),
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> Self {
        Self {
            ids,
            capacity,
            peer,
            message: Some(message),
            admission: None,
            member_id: None,
        }
    }

    fn answer(&self, result: Result<SignedStatement, SyncRefusal>) -> MetadataTransportMessage {
        if let (Err(refusal), Some(member_id)) = (&result, self.member_id) {
            let peer = self.peer;
            warn!(%peer, %member_id, ?refusal, "Refused a distributed bucket admission");
        }
        MetadataTransportMessage::AdmittedMember {
            result: result.map(Box::new),
        }
    }
}

type Served = Result<Next<MetadataTransportMessage>, IoError>;

impl Operation for ServeAdmissionOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> Served {
        let MetadataTransportMessage::ForwardAdmitMember {
            auth_token,
            snapshot,
            member_id,
            sizes,
        } = self.message.take().ok_or(IoError::Unfinished)?
        else {
            return Ok(Next::Done(reject("unexpected admission message")));
        };
        self.admission = Some((snapshot, member_id, sizes));
        self.member_id = Some(member_id);
        let caller = PeerCallerOperation::new(self.peer, auth_token);
        Ok(Next::Call(IoCall::child(caller)))
    }

    fn next(&mut self, result: IoResult) -> Served {
        let Some((snapshot, member_id, sizes)) = self.admission.take() else {
            let admitted = result.child::<SignedStatement, SyncRefusal>()?;
            return Ok(Next::Done(self.answer(admitted)));
        };
        match result.child::<AuthContext, SyncRefusal>()? {
            Ok(auth) => {
                let here = (self.ids, self.capacity);
                let admit = AdmitMemberOperation::new(here, auth, *snapshot, member_id, sizes);
                Ok(Next::Call(IoCall::child(admit)))
            }
            Err(refused) => Ok(Next::Done(self.answer(Err(refused)))),
        }
    }
}

/// What the pending call of a member admission answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmitStep {
    Config,
    Authorize,
    Create,
    Existing,
    Admit,
    Remove,
}

/// Creates this node's member bucket for the caller, or finds it again on a retry, then
/// admits it against this node's capacity with the ranges' expected `sizes`. Any other bucket
/// of the name fails.
#[derive(Debug, PartialEq)]
pub(crate) struct AdmitMemberOperation {
    ids: NodeRealm,
    capacity: Option<u64>,
    auth: AuthContext,
    signed: SignedSnapshot,
    member_id: Ulid,
    sizes: Vec<RangeSize>,
    step: AdmitStep,
    refused: Option<SyncRefusal>,
    now_ms: u64,
}

type Admitting = Result<Next<SignedStatement>, SyncRefusal>;

impl AdmitMemberOperation {
    pub(crate) fn new(
        (ids, capacity): (NodeRealm, Option<u64>),
        auth: AuthContext,
        signed: SignedSnapshot,
        member_id: Ulid,
        sizes: Vec<RangeSize>,
    ) -> Self {
        Self {
            now_ms: 0,
            ids,
            capacity,
            auth,
            signed,
            member_id,
            sizes,
            step: AdmitStep::Config,
            refused: None,
        }
    }

    fn node(&self) -> Result<(NodeId, RealmId), SyncRefusal> {
        self.ids.ok_or(SyncRefusal::Unavailable)
    }

    fn member(&self) -> Result<&DistributionMember, SyncRefusal> {
        let member = self.signed.snapshot.member(self.member_id);
        member.ok_or(SyncRefusal::NotFound)
    }

    /// Checks the nodes and the member, then the caller's WRITE on the bucket path.
    fn authorize(&mut self, config: RealmConfigDocument) -> Admitting {
        let (node_id, realm_id) = self.node()?;
        let snapshot = &self.signed.snapshot;
        if !is_sync_eligible(&config, snapshot.root.publisher)
            || !is_sync_eligible(&config, node_id)
        {
            return Err(SyncRefusal::Invalid(
                "publisher and members must be realm infrastructure nodes".to_string(),
            ));
        }
        if self.member()?.node_id != node_id {
            return Err(SyncRefusal::Invalid(
                "the member belongs to another node".to_string(),
            ));
        }
        let group_id = snapshot.config.owner_group;
        let path = bucket_permission_path(realm_id, group_id, node_id, &snapshot.config.bucket);
        let check = AuthorizeOperation::new(
            self.auth.realm_id,
            &self.auth,
            &path,
            &Permission::WRITE,
            PolicyRequestExtras::operation("s3.CreateBucket"),
        );
        self.step = AdmitStep::Authorize;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn create(&mut self) -> Admitting {
        let member = self.member()?;
        let snapshot = &self.signed.snapshot;
        let info = BucketInfo {
            group_id: snapshot.config.owner_group,
            created_at: SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms),
            created_by: self.auth.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: Some(DistributionBinding {
                set_id: snapshot.set_id(),
                member_id: member.member_id,
                // The creation fills in the bucket's own incarnation.
                incarnation: 0,
                revision: snapshot.revision(),
            }),
        };
        let create = CreateBucketOperation::new(snapshot.config.bucket.clone(), info).mapped();
        self.step = AdmitStep::Create;
        Ok(Next::Call(IoCall::child(create)))
    }

    fn admit(&mut self) -> Admitting {
        let (node_id, realm_id) = self.node()?;
        let member = self.member()?;
        let snapshot = &self.signed.snapshot;
        let admit = AdmitConfig {
            realm_id,
            node_id,
            bucket: snapshot.config.bucket.clone(),
            set_id: snapshot.set_id(),
            member_id: self.member_id,
            intent: member.intent,
            ranges: (snapshot.held_ranges(self.member_id).into_iter())
                .cloned()
                .collect(),
            strategy: member.strategy,
            coverage: member.coverage,
            sizes: self.sizes.clone(),
            capacity: self.capacity,
            fills: (snapshot.lifecycle.fills.iter())
                .filter(|fill| fill.member_id == self.member_id && !fill.admitted)
                .cloned()
                .collect(),
        };
        self.step = AdmitStep::Admit;
        Ok(Next::Call(IoCall::child(AdmitOperation::new(admit))))
    }

    fn admitted(&mut self, result: Result<SignedStatement, AdmitError>) -> Admitting {
        let error = match result {
            Ok(statement) => return Ok(Next::Done(statement)),
            Err(error) => error,
        };
        let capacity_refused = matches!(
            error,
            AdmitError::CapacityUnknown
                | AdmitError::InsufficientCapacity { .. }
                | AdmitError::SizeUnknown(_)
        );
        let refusal = match error {
            AdmitError::BindingChanged(_)
            | AdmitError::CapacityUnknown
            | AdmitError::InsufficientCapacity { .. }
            | AdmitError::SizeUnknown(_) => SyncRefusal::Invalid(error.to_string()),
            _ => SyncRefusal::Unavailable,
        };
        if !capacity_refused || self.member()?.incarnation.is_some() {
            return Err(refusal);
        }
        // A member never admitted holds nothing, so its bucket goes and a new create may use
        // the name.
        let snapshot = &self.signed.snapshot;
        let remove =
            RemoveMemberOperation::new(&snapshot.config.bucket, snapshot.set_id(), self.member_id);
        self.refused = Some(refusal);
        self.step = AdmitStep::Remove;
        Ok(Next::Call(IoCall::child(remove)))
    }
}

impl Operation for AdmitMemberOperation {
    type Output = SignedStatement;
    type Error = SyncRefusal;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Admitting {
        let (_, realm_id) = self.node()?;
        if self.auth.realm_id != realm_id {
            return Err(SyncRefusal::Unauthorized);
        }
        (self.signed.verify(realm_id)).map_err(|error| SyncRefusal::Invalid(error.to_string()))?;
        Ok(Next::Call(realm_config(realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Admitting {
        match self.step {
            AdmitStep::Config => {
                let config = config_of(result)?.ok_or(SyncRefusal::Unavailable)?;
                self.authorize(config)
            }
            AdmitStep::Authorize => {
                (result.child::<(), AuthorizeError>()?).map_err(pull_refusal)?;
                self.create()
            }
            AdmitStep::Create => match result.child::<BucketInfo, CreateBucketError>()? {
                Ok(_) => self.admit(),
                Err(error @ CreateBucketError::ProviderBucket(_)) => {
                    Err(SyncRefusal::Invalid(error.to_string()))
                }
                Err(CreateBucketError::BucketAlreadyExists) => {
                    let bucket = self.signed.snapshot.config.bucket.clone();
                    self.step = AdmitStep::Existing;
                    Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))))
                }
                Err(_) => Err(SyncRefusal::Unavailable),
            },
            AdmitStep::Existing => {
                let info = (result.child::<BucketInfo, GetBucketError>()?)
                    .map_err(|_| SyncRefusal::Unavailable)?;
                let snapshot = &self.signed.snapshot;
                same_member(&info, snapshot, self.member()?, &snapshot.config.bucket)?;
                self.admit()
            }
            AdmitStep::Admit => self.admitted(result.child::<SignedStatement, AdmitError>()?),
            AdmitStep::Remove => {
                result.child::<(), StorageError>()?.ok();
                Err(self.refused.take().ok_or(IoError::Unfinished)?)
            }
        }
    }
}

/// Deletes the bucket of the name while it still carries this member's binding; a bucket that
/// cannot be read or deleted stays.
#[derive(Debug, PartialEq)]
pub(crate) struct RemoveMemberOperation {
    bucket: String,
    set_id: Ulid,
    member_id: Ulid,
    deleting: bool,
}

impl RemoveMemberOperation {
    pub(crate) fn new(bucket: &str, set_id: Ulid, member_id: Ulid) -> Self {
        Self {
            bucket: bucket.to_string(),
            set_id,
            member_id,
            deleting: false,
        }
    }
}

impl Operation for RemoveMemberOperation {
    type Output = ();
    type Error = StorageError;

    fn first(&mut self) -> Result<Next<()>, StorageError> {
        let read = GetBucketOperation::new(self.bucket.clone());
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, StorageError> {
        if self.deleting {
            if let Err(error) = result.child::<(), DeleteBucketError>()? {
                warn!(%error, bucket = %self.bucket, "The bucket of a refused member stayed");
            }
            return Ok(Next::Done(()));
        }
        let Ok(info) = result.child::<BucketInfo, GetBucketError>()? else {
            return Ok(Next::Done(()));
        };
        let member = (self.set_id, self.member_id);
        let bound = (info.distribution).filter(|bound| (bound.set_id, bound.member_id) == member);
        let Some(binding) = bound else {
            return Ok(Next::Done(()));
        };
        let delete = DeleteBucketOperation::new(self.bucket.clone()).bound_to(binding);
        self.deleting = true;
        Ok(Next::Call(IoCall::child(delete)))
    }
}

/// A retry succeeds only on the bucket this member created: same set, member and group, and
/// the incarnation the record admitted, if any.
fn same_member(
    info: &BucketInfo,
    snapshot: &DistributionSnapshot,
    member: &DistributionMember,
    bucket: &str,
) -> Result<(), SyncRefusal> {
    let same = info.group_id == snapshot.config.owner_group
        && info.distribution.as_ref().is_some_and(|binding| {
            binding.set_id == snapshot.set_id()
                && binding.member_id == member.member_id
                && member
                    .incarnation
                    .is_none_or(|admitted| admitted == binding.incarnation)
        });
    match same {
        true => Ok(()),
        false => Err(SyncRefusal::Invalid(format!(
            "bucket \"{bucket}\" already exists on this node and is not this member"
        ))),
    }
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
