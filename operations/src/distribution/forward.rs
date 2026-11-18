//! Carries distributed bucket configuration between nodes: a member creates and admits its
//! bucket for a forwarded bearer token (D12); admin requests and statements reach the publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Only the publisher accepts them (D33). Nothing is queued.

use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::events::PeerAuthRefusal;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::dataset_description::{DatasetDescription, DescriptionError};
use aruna_core::structs::storage::distribution::capacity::RangeSize;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionMember, DistributionRequest, DistributionRevision,
    DistributionSnapshot, SignedSnapshot, SignedStatement,
};
use aruna_core::structs::storage::format::Compression;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::admit::{AdmitConfig, AdmitError, AdmitOperation};
use super::here::{NodeRealm, here};
use super::publish::PublishError;
use super::submit::PublishLocalOperation;
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::DriverContext;
use crate::forward::transport::reject;
use crate::metadata::device_pull::pull_refusal;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::write_peer::WritePeerOperation;
use crate::realm::peer_trust::{is_sync_eligible, peer_acts_for};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::dataset::DatasetSummaryOperation;
use crate::s3::bucket::delete::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Why the publisher refused or could not take a request.
#[derive(Clone, Debug, Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublishRefusal {
    #[error("the bearer token is missing or invalid")]
    Unauthorized,
    #[error("caller may not administer the distributed bucket's owner group")]
    Forbidden,
    #[error("{0}")]
    Rejected(String),
    #[error("the statement is already part of the record")]
    Replayed,
    #[error("configuration changes unavailable: {0}")]
    Unavailable(String),
}

impl From<IoError> for PublishRefusal {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

impl From<PublishError> for PublishRefusal {
    fn from(error: PublishError) -> Self {
        use aruna_core::structs::storage::distribution::DistributionError;
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

/// A member node's refusal, as the error of its operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberRefusal(pub SyncRefusal);

impl From<IoError> for MemberRefusal {
    fn from(_: IoError) -> Self {
        Self(SyncRefusal::Unavailable)
    }
}

impl From<SyncRefusal> for MemberRefusal {
    fn from(refusal: SyncRefusal) -> Self {
        Self(refusal)
    }
}

pub(crate) fn realm_config(realm_id: RealmId) -> IoCall {
    IoCall::child(GetConfigOperation::new(realm_id))
}

/// The realm configuration a config read found; an unreadable one counts as none.
pub(crate) fn config_of(result: IoResult) -> Result<Option<RealmConfigDocument>, IoError> {
    Ok(result.child::<RealmConfigDocument, GetConfigError>()?.ok())
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
    type Error = MemberRefusal;

    fn first(&mut self) -> Result<Next<AuthContext>, MemberRefusal> {
        let token = self.token.take().ok_or(IoError::Unfinished)?;
        if !bearer_only(&token) {
            return Err(SyncRefusal::Unauthorized.into());
        }
        let check = WritePeerOperation::new(self.peer, token);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuthContext>, MemberRefusal> {
        let Some(auth) = self.auth.take() else {
            let auth = (result.child::<AuthContext, PeerAuthRefusal>()?).map_err(|error| {
                match error {
                    PeerAuthRefusal::Unauthorized => SyncRefusal::Unauthorized,
                    PeerAuthRefusal::Unavailable(_) => SyncRefusal::Unavailable,
                }
            })?;
            let read = realm_config(auth.realm_id);
            self.auth = Some(auth);
            return Ok(Next::Call(read));
        };
        let config = config_of(result)?.ok_or(SyncRefusal::Unavailable)?;
        match peer_acts_for(&config, self.peer, auth.user_id) {
            true => Ok(Next::Done(auth)),
            false => Err(SyncRefusal::Unauthorized.into()),
        }
    }
}

/// The dataset a metadata change links, if any.
fn linked_dataset(request: &DistributionRequest) -> Option<Ulid> {
    match request {
        DistributionRequest::ChangeMetadata {
            metadata: Some(metadata),
            ..
        } => metadata.dataset,
        _ => None,
    }
}

/// Refuses a metadata change that links a dataset the caller cannot read or that is no Dataset,
/// as the metadata route does.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadableLinkOperation {
    auth: AuthContext,
    token: AuthToken,
    dataset: Option<Ulid>,
}

impl ReadableLinkOperation {
    pub(crate) fn new(auth: AuthContext, token: AuthToken, request: &DistributionRequest) -> Self {
        Self {
            auth,
            token,
            dataset: linked_dataset(request),
        }
    }
}

impl Operation for ReadableLinkOperation {
    type Output = ();
    type Error = PublishRefusal;

    fn first(&mut self) -> Result<Next<()>, PublishRefusal> {
        let Some(dataset) = self.dataset else {
            return Ok(Next::Done(()));
        };
        let caller = (Some(&self.auth), Some(self.token.clone()));
        let read = DatasetSummaryOperation::new(self.auth.realm_id, dataset, caller);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PublishRefusal> {
        let dataset = self.dataset.ok_or(IoError::Unfinished)?;
        match result.child::<DatasetDescription, DescriptionError>()? {
            Ok(_) => Ok(Next::Done(())),
            Err(DescriptionError::Unreadable) => Err(PublishRefusal::Rejected(format!(
                "dataset {dataset} does not exist or you may not read it"
            ))),
            Err(DescriptionError::Invalid(reason)) => Err(PublishRefusal::Rejected(reason)),
            Err(DescriptionError::Unavailable) => Err(PublishRefusal::Unavailable(format!(
                "dataset {dataset} could not be read now"
            ))),
        }
    }
}

/// The publisher's side of a forwarded request; see [`ServePublishOperation`].
pub(crate) fn serve_publish(
    context: &DriverContext,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> ServePublishOperation {
    ServePublishOperation::new(context.node_and_realm(), peer, message)
}

/// The member's side of a forwarded admission; see [`ServeAdmissionOperation`].
pub(crate) fn serve_admission(
    context: &DriverContext,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> ServeAdmissionOperation {
    let node = here(context);
    ServeAdmissionOperation::new((node.ids, node.capacity), peer, message)
}

/// What the pending call of a served publish answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServeStep {
    Caller,
    Link,
    Peer,
    Publish,
}

/// A request waiting for its checks on the publisher.
#[derive(Debug, PartialEq)]
struct Waiting {
    ids: (Ulid, Ulid),
    request: DistributionRequest,
    auth: Option<AuthContext>,
    token: Option<AuthToken>,
}

/// The publisher's side of a forwarded admin request or member statement.
#[derive(Debug, PartialEq)]
pub(crate) struct ServePublishOperation {
    ids: NodeRealm,
    peer: NodeId,
    message: Option<MetadataTransportMessage>,
    waiting: Option<Waiting>,
    step: ServeStep,
}

type Served = Result<Next<MetadataTransportMessage>, IoError>;

fn accepted(result: Result<DistributionRevision, PublishRefusal>) -> Served {
    let message = MetadataTransportMessage::DistributionAccepted { result };
    Ok(Next::Done(message))
}

impl ServePublishOperation {
    pub(crate) fn new(ids: NodeRealm, peer: NodeId, message: MetadataTransportMessage) -> Self {
        Self {
            ids,
            peer,
            message: Some(message),
            waiting: None,
            step: ServeStep::Caller,
        }
    }

    fn publish(&mut self) -> Served {
        let waiting = self.waiting.take().ok_or(IoError::Unfinished)?;
        let publish =
            PublishLocalOperation::new(self.ids, waiting.ids, waiting.request, waiting.auth);
        self.step = ServeStep::Publish;
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn caller_known(&mut self, caller: Result<AuthContext, MemberRefusal>) -> Served {
        let waiting = self.waiting.as_mut().ok_or(IoError::Unfinished)?;
        let auth = match caller {
            Ok(auth) => auth,
            Err(MemberRefusal(SyncRefusal::Unavailable)) => {
                return accepted(Err(PublishRefusal::Unavailable(
                    "the caller could not be checked".to_string(),
                )));
            }
            Err(_) => return accepted(Err(PublishRefusal::Unauthorized)),
        };
        // Statements carry their signer's authority, never a forwarded caller's.
        if !waiting.request.admin() {
            return accepted(Err(PublishRefusal::Rejected(
                "statements are submitted without a caller".to_string(),
            )));
        }
        let token = waiting.token.take().ok_or(IoError::Unfinished)?;
        let link = ReadableLinkOperation::new(auth.clone(), token, &waiting.request);
        waiting.auth = Some(auth);
        self.step = ServeStep::Link;
        Ok(Next::Call(IoCall::child(link)))
    }
}

impl Operation for ServePublishOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> Served {
        match self.message.take().ok_or(IoError::Unfinished)? {
            MetadataTransportMessage::ForwardDistributionRequest {
                auth_token,
                set_id,
                request_id,
                request,
            } => {
                let caller = PeerCallerOperation::new(self.peer, auth_token.clone());
                self.waiting = Some(Waiting {
                    ids: (set_id, request_id),
                    request: *request,
                    auth: None,
                    token: Some(auth_token),
                });
                Ok(Next::Call(IoCall::child(caller)))
            }
            MetadataTransportMessage::SubmitDistributionStatement { statement } => {
                // Only configured realm infrastructure nodes relay statements.
                let Some((_, realm_id)) = self.ids else {
                    return accepted(Err(PublishRefusal::Unavailable(
                        "net transport unavailable".to_string(),
                    )));
                };
                self.waiting = Some(Waiting {
                    ids: (statement.statement.set_id, statement.statement.statement_id),
                    request: DistributionRequest::Statement(statement),
                    auth: None,
                    token: None,
                });
                self.step = ServeStep::Peer;
                Ok(Next::Call(realm_config(realm_id)))
            }
            _ => Ok(Next::Done(reject("unexpected distribution message"))),
        }
    }

    fn next(&mut self, result: IoResult) -> Served {
        match self.step {
            ServeStep::Caller => self.caller_known(result.child::<AuthContext, MemberRefusal>()?),
            ServeStep::Link => match result.child::<(), PublishRefusal>()? {
                Ok(()) => self.publish(),
                Err(refusal) => accepted(Err(refusal)),
            },
            ServeStep::Peer => match config_of(result)? {
                None => accepted(Err(PublishRefusal::Unavailable(
                    "realm config missing".to_string(),
                ))),
                Some(config) if is_sync_eligible(&config, self.peer) => self.publish(),
                Some(_) => accepted(Err(PublishRefusal::Unauthorized)),
            },
            ServeStep::Publish => {
                accepted(result.child::<DistributionRevision, PublishRefusal>()?)
            }
        }
    }
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
            let admitted = result.child::<SignedStatement, MemberRefusal>()?;
            return Ok(Next::Done(self.answer(admitted.map_err(|refused| refused.0))));
        };
        match result.child::<AuthContext, MemberRefusal>()? {
            Ok(auth) => {
                let here = (self.ids, self.capacity);
                let admit = AdmitMemberOperation::new(here, auth, *snapshot, member_id, sizes);
                Ok(Next::Call(IoCall::child(admit)))
            }
            Err(refused) => Ok(Next::Done(self.answer(Err(refused.0)))),
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
}

type Admitting = Result<Next<SignedStatement>, MemberRefusal>;

impl AdmitMemberOperation {
    pub(crate) fn new(
        (ids, capacity): (NodeRealm, Option<u64>),
        auth: AuthContext,
        signed: SignedSnapshot,
        member_id: Ulid,
        sizes: Vec<RangeSize>,
    ) -> Self {
        Self {
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
            )
            .into());
        }
        if self.member()?.node_id != node_id {
            return Err(
                SyncRefusal::Invalid("the member belongs to another node".to_string()).into(),
            );
        }
        let group_id = snapshot.config.owner_group;
        let path = bucket_permission_path(realm_id, group_id, node_id, &snapshot.config.bucket);
        let check = AuthorizeOperation::new(
            self.auth.realm_id,
            self.auth.clone(),
            path,
            Permission::WRITE,
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
            created_at: SystemTime::now(),
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
            return Err(refusal.into());
        }
        // A member never admitted holds nothing, so its bucket goes and a new create may use
        // the name.
        let snapshot = &self.signed.snapshot;
        let remove = RemoveMemberOperation::new(
            &snapshot.config.bucket,
            snapshot.set_id(),
            self.member_id,
        );
        self.refused = Some(refusal);
        self.step = AdmitStep::Remove;
        Ok(Next::Call(IoCall::child(remove)))
    }
}

impl Operation for AdmitMemberOperation {
    type Output = SignedStatement;
    type Error = MemberRefusal;

    fn first(&mut self) -> Admitting {
        let (_, realm_id) = self.node()?;
        if self.auth.realm_id != realm_id {
            return Err(SyncRefusal::Unauthorized.into());
        }
        (self.signed.verify(realm_id))
            .map_err(|error| SyncRefusal::Invalid(error.to_string()))?;
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
                    Err(SyncRefusal::Invalid(error.to_string()).into())
                }
                Err(CreateBucketError::BucketAlreadyExists) => {
                    let bucket = self.signed.snapshot.config.bucket.clone();
                    self.step = AdmitStep::Existing;
                    Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))))
                }
                Err(_) => Err(SyncRefusal::Unavailable.into()),
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
                Err(self.refused.take().ok_or(IoError::Unfinished)?.into())
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
