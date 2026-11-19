//! Requests and serves object bytes and metadata between nodes over the native source ALPN.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::{Event, NetEvent};
use aruna_core::native_reference::{
    DelegatedBody, DelegatedReadEffect, NativeAuthority, NativeRange, NativeReferenceReject,
    NativeReferenceRequest,
};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::delegation::SignedDelegation;
use aruna_core::structs::storage::read_grant::{
    self, GrantScope, HEAD_OPERATION, MAX_REQUEST_SECS, READ_OPERATION, SignedRequest,
};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::{SyncRelationship, SyncState};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use bytes::Bytes;
use tracing::warn;
use ulid::Ulid;

use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::delegate::{DelegationInput, unsigned_delegation};
use crate::distribution::nonce::{NonceError, SpendNonceOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::realm::peer_trust::is_sync_eligible;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult, ObjectRangeRequest,
};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::sync::mirror_repair::{KickRepairOperation, StoreStatusOperation, SyncMirrorError};
use crate::sync::sync_relationship::{
    GetRelationshipOperation, SyncRelationshipDirection, SyncRelationshipError,
};

/// This node and its realm.
type Ids = (NodeId, RealmId);

type Reject = NativeReferenceReject;

/// What a peer's request prepared: the answer's metadata and, for a read, its bytes.
#[derive(Debug, PartialEq)]
pub struct PreparedReference {
    pub(crate) metadata: SourceMetadata,
    pub(crate) group_id: Option<GroupId>,
    pub(crate) body: Option<BackendStream<Result<Bytes, StreamError>>>,
}

fn request_operation(request: &NativeReferenceRequest) -> &'static str {
    match request.head {
        true => HEAD_OPERATION,
        false => READ_OPERATION,
    }
}

/// One exact version read from the holder for a caller this node verified itself, bound by a
/// D33 delegation to this operation, version and range.
pub(crate) fn delegated_read(
    ids: Option<Ids>,
    caller: DelegationInput,
    arn: &VersionedObjectArn,
    range: Option<Range<u64>>,
    head: bool,
) -> DelegatedReadOperation {
    DelegatedReadOperation {
        ids,
        caller: Some(caller),
        arn: arn.clone(),
        range,
        head,
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct DelegatedReadOperation {
    ids: Option<Ids>,
    caller: Option<DelegationInput>,
    arn: VersionedObjectArn,
    range: Option<Range<u64>>,
    head: bool,
}

impl Operation for DelegatedReadOperation {
    type Output = DelegatedBody;
    type Error = StagingSourceError;

    fn first(&mut self) -> Result<Next<DelegatedBody>, StagingSourceError> {
        let range = NativeRange::checked(self.range.clone())?;
        let operation = match self.head {
            true => HEAD_OPERATION,
            false => READ_OPERATION,
        };
        let arn = &self.arn;
        let scope = GrantScope::Version(arn.clone());
        let digest = read_grant::request_digest(
            operation,
            &scope,
            range.map(|range| (range.start, range.end)),
        )
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
        let ids = self.ids.ok_or(StagingSourceError::HandleMissing)?;
        let caller = self.caller.take().ok_or(IoError::Unfinished)?;
        let input = DelegationInput {
            target: arn.node_id,
            operation: operation.to_string(),
            bucket: arn.bucket.clone(),
            key: arn.key.clone(),
            request_digest: digest,
            ..caller
        };
        let now = unix_timestamp_millis() / 1000;
        let read = DelegatedReadEffect {
            delegation: unsigned_delegation(ids, input, now),
            version_id: arn.version,
            head: self.head,
            range,
        };
        let effect = Effect::Net(NetEffect::DelegatedRead(Box::new(read)));
        Ok(Next::Call(IoCall::from(effect)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DelegatedBody>, StagingSourceError> {
        match result.event()? {
            Event::Net(NetEvent::DelegatedRead(answer)) => Ok(Next::Done(*answer?)),
            other => Err(IoError::Unexpected {
                expected: "Event::Net(NetEvent::DelegatedRead)",
                got: other.label(),
            }
            .into()),
        }
    }
}

/// What a signed grant or delegation vouches for, once its signature and scope are checked.
#[derive(Debug, PartialEq)]
struct VouchedRead {
    issuer: NodeId,
    nonce: [u8; 16],
    expires: u64,
    now: u64,
    auth: AuthContext,
}

fn version_scope((node_id, realm_id): Ids, request: &NativeReferenceRequest) -> GrantScope {
    GrantScope::Version(VersionedObjectArn {
        realm_id,
        node_id,
        bucket: request.bucket.clone(),
        key: request.key.clone(),
        version: request.version_id,
    })
}

fn request_digest(
    scope: &GrantScope,
    request: &NativeReferenceRequest,
) -> Result<[u8; 32], Reject> {
    let range = request.range.map(|range| (range.start, range.end));
    read_grant::request_digest(request_operation(request), scope, range)
        .map_err(|_| Reject::AccessDenied)
}

/// A read a grant of this node covers: signed by the reading node, for exactly the version and
/// range the grant and request name.
fn vouched_grant(
    ids: Ids,
    request: &NativeReferenceRequest,
    signed: &SignedRequest,
    now: u64,
) -> Result<VouchedRead, Reject> {
    let granted = (signed.verify(ids.1, ids.0, now)).map_err(|_| Reject::AccessDenied)?;
    let grant = &granted.grant.grant;
    let scope = version_scope(ids, request);
    let digest = request_digest(&scope, request)?;
    if grant.scope != scope
        || granted.operation != request_operation(request)
        || granted.request_digest != digest
    {
        return Err(Reject::AccessDenied);
    }
    Ok(VouchedRead {
        issuer: granted.issuer,
        nonce: granted.nonce,
        expires: granted.issued_at.saturating_add(2 * MAX_REQUEST_SECS),
        now,
        auth: AuthContext {
            user_id: grant.user_id,
            realm_id: grant.realm_id,
            path_restrictions: grant.restrictions.clone(),
            session: None,
        },
    })
}

/// A read the reading node vouches for in one D33 delegation naming this exact version,
/// operation and range.
fn vouched_delegation(
    ids: Ids,
    request: &NativeReferenceRequest,
    signed: &SignedDelegation,
    now: u64,
) -> Result<VouchedRead, Reject> {
    let delegation = (signed.verify(ids.1, ids.0, now)).map_err(|_| Reject::AccessDenied)?;
    let digest = request_digest(&version_scope(ids, request), request)?;
    if delegation.operation != request_operation(request)
        || delegation.bucket != request.bucket
        || delegation.key != request.key
        || delegation.request_digest != digest
    {
        return Err(Reject::AccessDenied);
    }
    Ok(VouchedRead {
        issuer: delegation.issuer,
        nonce: delegation.nonce,
        expires: delegation.issued_at.saturating_add(2 * MAX_REQUEST_SECS),
        now,
        auth: AuthContext {
            user_id: delegation.user_id,
            realm_id: delegation.realm_id,
            path_restrictions: delegation.restrictions.clone(),
            session: None,
        },
    })
}

/// What the pending call of preparing a peer's read answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrepareStep {
    Relationship,
    RelationshipRead,
    Denied,
    Config,
    Nonce,
    Governed,
    VouchedRead,
}

/// Checks a peer's read under its relationship, grant or delegation, then prepares the answer.
#[derive(Debug, PartialEq)]
pub struct PrepareReferenceOperation {
    ids: Option<Ids>,
    peer: NodeId,
    request: NativeReferenceRequest,
    step: PrepareStep,
    vouched: Option<VouchedRead>,
    relationship: Option<SyncRelationship>,
}

type PrepareTurn = Result<Next<PreparedReference>, Reject>;

impl PrepareReferenceOperation {
    pub fn new(ids: Option<Ids>, peer: NodeId, request: NativeReferenceRequest) -> Self {
        Self {
            ids,
            peer,
            request,
            step: PrepareStep::Relationship,
            vouched: None,
            relationship: None,
        }
    }

    fn ids(&self) -> Result<Ids, Reject> {
        (self.ids).ok_or_else(|| Reject::Unavailable("network handle unavailable".to_string()))
    }

    fn head_call(&self) -> IoCall {
        IoCall::child(HeadObjectOperation::new(HeadObjectInput {
            bucket: self.request.bucket.clone(),
            key: self.request.key.clone(),
            version_id: Some(self.request.version_id),
        }))
    }

    fn read_call(&self, auth: AuthContext, node_id: NodeId) -> IoCall {
        IoCall::child(ReadExactOperation::new(auth, node_id, self.request.clone()))
    }

    fn vouched(&mut self, vouched: VouchedRead) -> PrepareTurn {
        let realm_id = self.ids()?.1;
        self.vouched = Some(vouched);
        self.step = PrepareStep::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn vouched_read(&self) -> Result<&VouchedRead, IoError> {
        self.vouched.as_ref().ok_or(IoError::Unfinished)
    }

    fn configured(&mut self, config: Result<RealmConfigDocument, GetConfigError>) -> PrepareTurn {
        let config = config.map_err(|error| {
            warn!(%error, "realm config unavailable; querying the local replica");
            Reject::Unavailable("realm config unavailable".to_string())
        })?;
        let vouched = self.vouched_read()?;
        if vouched.issuer != self.peer || !is_sync_eligible(&config, self.peer) {
            return Err(Reject::AccessDenied);
        }
        let spend = SpendNonceOperation::new(vouched.nonce, vouched.expires, vouched.now);
        self.step = PrepareStep::Nonce;
        Ok(Next::Call(IoCall::child(spend)))
    }
}

impl Operation for PrepareReferenceOperation {
    type Output = PreparedReference;
    type Error = Reject;

    fn first(&mut self) -> PrepareTurn {
        let ids = self.ids()?;
        let now = unix_timestamp_millis() / 1000;
        match &self.request.authority {
            NativeAuthority::Relationship(relationship_id) => {
                let direction = SyncRelationshipDirection::Outgoing;
                let get = GetRelationshipOperation::new(*relationship_id, direction);
                Ok(Next::Call(IoCall::child(get)))
            }
            NativeAuthority::Granted(signed) => {
                let vouched = vouched_grant(ids, &self.request, signed, now)?;
                self.vouched(vouched)
            }
            NativeAuthority::Delegated(signed) => {
                let vouched = vouched_delegation(ids, &self.request, signed, now)?;
                self.vouched(vouched)
            }
        }
    }

    fn next(&mut self, result: IoResult) -> PrepareTurn {
        match self.step {
            PrepareStep::Relationship => {
                let relationship = result.child::<SyncRelationship, SyncRelationshipError>()?;
                let relationship = relationship.map_err(map_relationship_error)?;
                let ids = self.ids()?;
                validate_relationship(ids, self.peer, &relationship, &self.request)?;
                let auth = AuthContext {
                    user_id: relationship.created_by,
                    realm_id: relationship.source.realm_id,
                    path_restrictions: None,
                    session: None,
                };
                let call = self.read_call(auth, relationship.source.node_id);
                self.relationship = Some(relationship);
                self.step = PrepareStep::RelationshipRead;
                Ok(Next::Call(call))
            }
            PrepareStep::RelationshipRead => match result.child::<PreparedReference, Reject>()? {
                Err(Reject::AccessDenied) => {
                    let relationship = self.relationship.take().ok_or(IoError::Unfinished)?;
                    self.step = PrepareStep::Denied;
                    let mark = MarkDeniedOperation::new(relationship);
                    Ok(Next::Call(IoCall::child(mark)))
                }
                read => Ok(Next::Done(read?)),
            },
            PrepareStep::Denied => {
                result.child::<(), IoError>()?.ok();
                Err(Reject::AccessDenied)
            }
            PrepareStep::Config => {
                self.configured(result.child::<RealmConfigDocument, GetConfigError>()?)
            }
            PrepareStep::Nonce => {
                let spent = result.child::<(), NonceError>()?;
                spent.map_err(|_| Reject::AccessDenied)?;
                self.step = PrepareStep::Governed;
                Ok(Next::Call(self.head_call()))
            }
            // Governed content answers only the managed read's destination challenge.
            PrepareStep::Governed => {
                let head = result.child::<HeadObjectResult, HeadObjectError>()?;
                if !head.map_err(map_head_error)?.source_policies.is_empty() {
                    return Err(Reject::AccessDenied);
                }
                let node_id = self.ids()?.0;
                let auth = self.vouched_read()?.auth.clone();
                self.step = PrepareStep::VouchedRead;
                Ok(Next::Call(self.read_call(auth, node_id)))
            }
            PrepareStep::VouchedRead => {
                Ok(Next::Done(result.child::<PreparedReference, Reject>()??))
            }
        }
    }
}

/// What the pending call of an authorized read answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadStep {
    Bucket,
    Authorize,
    Head,
    Get,
}

/// Authorizes the caller like an S3 read of the exact version, then reads it.
#[derive(Debug, PartialEq)]
struct ReadExactOperation {
    auth: AuthContext,
    node_id: NodeId,
    request: NativeReferenceRequest,
    step: ReadStep,
    group_id: Option<GroupId>,
}

type ReadTurn = Result<Next<PreparedReference>, Reject>;

impl ReadExactOperation {
    fn new(auth: AuthContext, node_id: NodeId, request: NativeReferenceRequest) -> Self {
        Self {
            auth,
            node_id,
            request,
            step: ReadStep::Bucket,
            group_id: None,
        }
    }

    fn group(&self) -> Result<GroupId, IoError> {
        self.group_id.ok_or(IoError::Unfinished)
    }

    fn authorize(&mut self, bucket: BucketInfo) -> ReadTurn {
        self.group_id = Some(bucket.group_id);
        let request = &self.request;
        let path = object_permission_path(
            self.auth.realm_id,
            bucket.group_id,
            self.node_id,
            &request.bucket,
            &request.key,
        );
        let extras = PolicyRequestExtras::operation(request_operation(request));
        let authorize = AuthorizeOperation::new(
            self.auth.realm_id,
            &self.auth,
            &path,
            &Permission::READ,
            extras,
        );
        self.step = ReadStep::Authorize;
        Ok(Next::Call(IoCall::child(authorize)))
    }

    fn read(&mut self) -> ReadTurn {
        let request = &self.request;
        if request.head {
            self.step = ReadStep::Head;
            let head = HeadObjectOperation::new(HeadObjectInput {
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                version_id: Some(request.version_id),
            });
            return Ok(Next::Call(IoCall::child(head)));
        }
        let range = request.range.map(|range| ObjectRangeRequest::StartEnd {
            start: range.start,
            end: range.end.saturating_sub(1),
        });
        let get = GetObjectOperation::new(GetObjectInput {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            version_id: Some(request.version_id),
            range,
            group_id: self.group()?,
            user_identity: self.auth.user_id,
            realm_id: self.auth.realm_id,
            node_id: self.node_id,
        });
        self.step = ReadStep::Get;
        Ok(Next::Call(IoCall::child(get)))
    }
}

impl Operation for ReadExactOperation {
    type Output = PreparedReference;
    type Error = Reject;

    fn first(&mut self) -> ReadTurn {
        let bucket = GetBucketOperation::new(self.request.bucket.clone());
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        let version_id = self.request.version_id;
        match self.step {
            ReadStep::Bucket => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(bucket) => self.authorize(bucket),
                Err(GetBucketError::NotFound) => Err(Reject::NotFound),
                Err(error) => Err(Reject::Unavailable(error.to_string())),
            },
            ReadStep::Authorize => {
                let allowed = result.child::<(), AuthorizeError>()?;
                allowed.map_err(map_authorize_error)?;
                self.read()
            }
            ReadStep::Head => {
                let head = result.child::<HeadObjectResult, HeadObjectError>()?;
                let head = head.map_err(map_head_error)?;
                Ok(Next::Done(PreparedReference {
                    metadata: head_metadata(&head, version_id)?,
                    group_id: Some(self.group()?),
                    body: None,
                }))
            }
            ReadStep::Get => {
                let get = result.child::<GetObjectResult, GetObjectError>()?;
                let get = get.map_err(map_get_error)?;
                Ok(Next::Done(PreparedReference {
                    metadata: result_metadata(&get, version_id)?,
                    group_id: Some(self.group()?),
                    body: Some(get.blob),
                }))
            }
        }
    }
}

/// Fails a relationship whose creator lost read access. A detached stub has no visible status
/// entry, and marking it failed would stop retained reads, so only the request reports denial.
#[derive(Debug, PartialEq)]
pub(crate) struct MarkDeniedOperation {
    relationship: SyncRelationship,
    kicking: bool,
}

impl MarkDeniedOperation {
    fn new(relationship: SyncRelationship) -> Self {
        Self {
            relationship,
            kicking: false,
        }
    }
}

impl Operation for MarkDeniedOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let relationship = &mut self.relationship;
        if relationship.state == SyncState::Detached {
            return Ok(Next::Done(()));
        }
        relationship.state = SyncState::Failed {
            reason: "access_denied".to_string(),
        };
        relationship.status.last_error = Some("access_denied".to_string());
        let counters = &mut relationship.status.counters;
        counters.failures = counters.failures.saturating_add(1);
        counters.consecutive_failures = counters.consecutive_failures.saturating_add(1);
        let store = StoreStatusOperation::new(relationship.clone());
        Ok(Next::Call(IoCall::child(store)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.kicking {
            result.child::<(), SyncMirrorError>()?.ok();
            return Ok(Next::Done(()));
        }
        match result.child::<bool, SyncMirrorError>()? {
            Ok(true) => {
                self.kicking = true;
                Ok(Next::Call(IoCall::child(KickRepairOperation::new())))
            }
            Ok(false) => Ok(Next::Done(())),
            Err(error) => {
                warn!(%error, "Failed to persist native reference denial");
                Ok(Next::Done(()))
            }
        }
    }
}

fn map_authorize_error(error: AuthorizeError) -> Reject {
    match error {
        AuthorizeError::PermissionDenied
        | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => Reject::AccessDenied,
        AuthorizeError::Policy(PolicyEnforcementError::Unavailable(error))
        | AuthorizeError::CheckFailed(error) => Reject::Unavailable(error),
        AuthorizeError::Storage(error) => Reject::Unavailable(error.to_string()),
    }
}

fn validate_relationship(
    (node_id, realm_id): Ids,
    peer: NodeId,
    relationship: &SyncRelationship,
    request: &NativeReferenceRequest,
) -> Result<(), NativeReferenceReject> {
    let valid_prefix = relationship
        .source
        .key_prefix()
        .is_none_or(|prefix| request.key.starts_with(prefix));
    // Detached stubs keep serving retained references after relationship deletion.
    // Other disabled states refuse access.
    if !relationship.serves_references()
        || !matches!(relationship.state, SyncState::Enabled | SyncState::Detached)
        || relationship.source.realm_id != realm_id
        || relationship.source.node_id != node_id
        || relationship.target.node_id != peer
        || relationship.source.bucket() != Some(request.bucket.as_str())
        || !valid_prefix
    {
        return Err(NativeReferenceReject::AccessDenied);
    }
    Ok(())
}

fn map_relationship_error(error: SyncRelationshipError) -> NativeReferenceReject {
    match error {
        SyncRelationshipError::NotFound => NativeReferenceReject::AccessDenied,
        error => NativeReferenceReject::Unavailable(error.to_string()),
    }
}

fn map_get_error(error: GetObjectError) -> NativeReferenceReject {
    match error {
        GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker
        // The pinned observation no longer has bytes to relay.
        // Drift and exhausted bindings remain retryable through rebinding.
        | GetObjectError::HistoricalReferenceUnavailable
        | GetObjectError::ReferenceChecksumMismatch => NativeReferenceReject::NotFound,
        GetObjectError::ReferenceSourceChanged | GetObjectError::ReferenceAdvanceExhausted => {
            NativeReferenceReject::Unavailable(error.to_string())
        }
        GetObjectError::StagingSourceError(StagingSourceError::NotFound) => {
            NativeReferenceReject::NotFound
        }
        GetObjectError::StagingSourceError(StagingSourceError::AccessDenied) => {
            NativeReferenceReject::AccessDenied
        }
        error => NativeReferenceReject::Unavailable(error.to_string()),
    }
}

fn map_head_error(error: HeadObjectError) -> NativeReferenceReject {
    match error {
        HeadObjectError::NoSuchKey
        | HeadObjectError::NoSuchVersion
        | HeadObjectError::DeleteMarker => NativeReferenceReject::NotFound,
        HeadObjectError::StagingSourceError(StagingSourceError::NotFound) => {
            NativeReferenceReject::NotFound
        }
        HeadObjectError::StagingSourceError(StagingSourceError::AccessDenied) => {
            NativeReferenceReject::AccessDenied
        }
        error => NativeReferenceReject::Unavailable(error.to_string()),
    }
}

fn result_metadata(
    result: &GetObjectResult,
    version_id: Ulid,
) -> Result<SourceMetadata, NativeReferenceReject> {
    let mut metadata = if let Some(metadata) = result.source_metadata.clone() {
        metadata
    } else if let Some(location) = result.location.as_ref() {
        SourceMetadata {
            content_length: location.blob_size,
            content_type: None,
            etag: None,
            last_modified: result.info.version_created_at,
            source_version: None,
            digests: Default::default(),
        }
    } else {
        return Err(NativeReferenceReject::Unavailable(
            "source metadata unavailable".to_string(),
        ));
    };
    metadata.source_version = Some(version_id.to_string());
    Ok(metadata)
}

fn head_metadata(
    result: &HeadObjectResult,
    version_id: Ulid,
) -> Result<SourceMetadata, NativeReferenceReject> {
    let mut metadata = if let Some(metadata) = result.source_metadata.clone() {
        metadata
    } else if let Some(location) = result.location.as_ref() {
        SourceMetadata {
            content_length: location.blob_size,
            content_type: None,
            etag: None,
            last_modified: result.version_created_at,
            source_version: None,
            digests: Default::default(),
        }
    } else {
        return Err(NativeReferenceReject::Unavailable(
            "source metadata unavailable".to_string(),
        ));
    };
    metadata.source_version = Some(version_id.to_string());
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::replication::ArunaArn;
    use aruna_core::structs::{ReferenceHandling, SyncMode, SyncStatusSnapshot};
    use aruna_storage::storage::FjallStorage;

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn ids() -> Ids {
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        (node, RealmId::from_bytes([2; 32]))
    }

    fn read_request(authority: NativeAuthority) -> NativeReferenceRequest {
        NativeReferenceRequest {
            authority,
            bucket: "source".to_string(),
            key: "data.txt".to_string(),
            version_id: Ulid::from_bytes([3; 16]),
            head: false,
            range: None,
        }
    }

    // Without a network there is no identity to check a peer's read against.
    #[test]
    fn needs_network_ids() {
        let peer = ids().0;
        let authority = NativeAuthority::Relationship(Ulid::from_bytes([4; 16]));
        let mut prepare = PrepareReferenceOperation::new(None, peer, read_request(authority));
        assert!(matches!(prepare.first(), Err(Reject::Unavailable(_))));
    }

    // An unknown relationship is denied, so its existence stays hidden.
    #[test]
    fn unknown_relationship_denied() {
        let peer = ids().0;
        let authority = NativeAuthority::Relationship(Ulid::from_bytes([4; 16]));
        let mut prepare =
            PrepareReferenceOperation::new(Some(ids()), peer, read_request(authority));
        prepare.first().unwrap();
        let answer = prepare.next(child(Err::<SyncRelationship, _>(
            SyncRelationshipError::NotFound,
        )));
        assert_eq!(answer, Err(Reject::AccessDenied));
    }

    // A missing bucket answers not found before any permission is checked.
    #[test]
    fn missing_bucket_found() {
        let authority = NativeAuthority::Relationship(Ulid::from_bytes([4; 16]));
        let auth = AuthContext {
            user_id: aruna_core::UserId::local(Ulid::from_bytes([5; 16]), ids().1),
            realm_id: ids().1,
            path_restrictions: None,
            session: None,
        };
        let mut read = ReadExactOperation::new(auth, ids().0, read_request(authority));
        read.first().unwrap();
        let answer = read.next(child(Err::<BucketInfo, _>(GetBucketError::NotFound)));
        assert_eq!(answer, Err(Reject::NotFound));
    }

    // A denied policy is a denial; a failed check stays retryable.
    #[test]
    fn maps_authorize_errors() {
        assert_eq!(
            map_authorize_error(AuthorizeError::PermissionDenied),
            Reject::AccessDenied
        );
        let failed = map_authorize_error(AuthorizeError::CheckFailed("down".to_string()));
        assert_eq!(failed, Reject::Unavailable("down".to_string()));
    }

    // The relaying peer must learn that a pinned observation is gone (NotFound)
    // rather than treating it as a transient origin failure.
    #[test]
    fn maps_reference_rejects() {
        assert_eq!(
            map_get_error(GetObjectError::HistoricalReferenceUnavailable),
            NativeReferenceReject::NotFound
        );
        for error in [
            GetObjectError::ReferenceSourceChanged,
            GetObjectError::ReferenceAdvanceExhausted,
        ] {
            assert!(matches!(
                map_get_error(error),
                NativeReferenceReject::Unavailable(_)
            ));
        }
    }

    #[tokio::test]
    async fn denial_journals_failure() {
        // A native read whose creator lost READ fails the relationship and journals its
        // `sync_failed` with that status write, with no replication job queued.
        use crate::sync::sync_relationship::{
            StoreRelationshipOperation, SyncRelationshipDirection,
        };
        use aruna_core::structs::storage::blob::BucketInfo;
        use aruna_core::structs::storage::format::Compression;
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        let source = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let target = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let creator = aruna_core::UserId::local(Ulid::from_parts(5, 5), realm_id);
        let relationship = SyncRelationship {
            id: Ulid::from_parts(2, 2),
            source: ArunaArn::s3_bucket(realm_id, source, "source").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, target, "target").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: ReferenceHandling::Preserve,
            reference_serving: true,
            replicate_deletes: true,
            created_by: creator,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        };
        let direction = SyncRelationshipDirection::Outgoing;
        let store = StoreRelationshipOperation::new(relationship.clone(), direction);
        drive(store, &context).await.unwrap();
        let info = BucketInfo {
            group_id: Ulid::from_parts(6, 6),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: creator,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let write = aruna_core::effects::StorageEffect::Write {
            key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
            key: b"source".to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;

        drive(MarkDeniedOperation::new(relationship.clone()), &context)
            .await
            .unwrap();

        let pending = aruna_core::plugin_runs::PENDING_ROW;
        let page = crate::plugins::journal::journal_page(pending, None, 10);
        let (rows, _) = crate::driver::drive(page, &context).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.resource, relationship.id.to_string());
        let transition = format!("sync_failed/{source}/{}/", relationship.id);
        assert!(rows[0].1.source.starts_with(&transition));
    }

    #[test]
    fn preserve_allows_native() {
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let realm_id = RealmId::from_bytes([8; 32]);
        let peer = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let mut relationship = SyncRelationship {
            id: Ulid::generate(),
            source: ArunaArn::s3_bucket(realm_id, node, "source").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, peer, "target").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: ReferenceHandling::Materialize,
            reference_serving: false,
            replicate_deletes: true,
            created_by: aruna_core::UserId::local(Ulid::generate(), realm_id),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        };
        let request = NativeReferenceRequest {
            authority: NativeAuthority::Relationship(relationship.id),
            bucket: "source".to_string(),
            key: "data.txt".to_string(),
            version_id: Ulid::generate(),
            head: true,
            range: None,
        };

        assert_eq!(
            validate_relationship((node, realm_id), peer, &relationship, &request),
            Err(NativeReferenceReject::AccessDenied)
        );
        relationship.set_reference_handling(ReferenceHandling::Preserve);
        assert!(validate_relationship((node, realm_id), peer, &relationship, &request).is_ok());
        relationship.set_reference_handling(ReferenceHandling::Materialize);
        assert!(validate_relationship((node, realm_id), peer, &relationship, &request).is_ok());
    }
}
