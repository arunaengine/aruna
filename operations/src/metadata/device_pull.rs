//! Serves a device's sync: lists local object versions and pulls one exact version on request.
//! The pull commits through the ordinary object write, so all write side effects stay intact.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::SYNC_RECEIPT_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{
    BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, KeyRange, LocalFence, RouteError, WriteRoute,
};
use aruna_core::structs::storage::origin::{UploadClient, VersionOrigin};
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::structs::{
    ORIGIN_DEVICE_TAG, RemoteHead, SYNC_VERSION_TAG, SyncListCursor, SyncPageLimit, SyncPull,
    SyncPullAck, SyncRefusal, SyncVersionPage,
};
use aruna_core::types::{GroupId, Key, Value};
use futures_util::StreamExt;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::guard::GuardError;
use crate::distribution::routing::{bound_view, local_fence, route_write};
use crate::distribution::versions::{
    InventoryVersionsError, InventoryVersionsInput, InventoryVersionsOperation,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::{ForwardAuthError, ForwardedCallerOperation};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};
use crate::realm::mutate_placement::node_kind;
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::live_queue::{CompletePutInput, CompletePutOperation, QueueLiveOperation};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use crate::replication::queue::LiveVersionInput;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use crate::s3::object::versions::{
    ListVersionsError, ListVersionsInput, ListVersionsItem, ListVersionsOperation,
    ListVersionsResult,
};
use crate::staging::bucket_guard::{BucketGuardError, BucketGuardOperation};
use crate::staging::offered_directory::OfferedDirectoryError;

/// How long a forwarded listing waits for the holder of a range.
const HOLDER_TIMEOUT: Duration = Duration::from_secs(15);

/// Why a pull or listing operation ended without its answer: the refusal for the device, or a
/// lost storage conflict the runner may retry.
#[derive(Debug, PartialEq)]
pub enum PullError {
    Refused(SyncRefusal),
    Conflict,
}

impl PullError {
    /// The refusal the device is told; a conflict that outlived its retries is unavailable.
    pub fn refusal(self) -> SyncRefusal {
        match self {
            Self::Refused(refusal) => refusal,
            Self::Conflict => SyncRefusal::Unavailable,
        }
    }
}

impl From<SyncRefusal> for PullError {
    fn from(refusal: SyncRefusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<IoError> for PullError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Conflict,
            _ => Self::Refused(SyncRefusal::Unavailable),
        }
    }
}

/// The node and realm a pull or listing is served on.
pub type ServingNode = (NodeId, RealmId);

/// A failed step whose cause the device need not know.
fn unavailable<E>(_: E) -> SyncRefusal {
    SyncRefusal::Unavailable
}

fn forward_refusal(error: ForwardAuthError) -> SyncRefusal {
    match error {
        ForwardAuthError::Unauthorized | ForwardAuthError::Forbidden => SyncRefusal::Unauthorized,
        ForwardAuthError::Unavailable(_) => SyncRefusal::Unavailable,
    }
}

/// The owner the device forwards for. A User peer is owner-bound, so the binding is checked
/// rather than trusted; a pull carries no binding proof, so the owner's WRITE permission is the
/// authority, exactly as for S3.
fn caller(peer: NodeId, realm_id: RealmId, auth_token: AuthToken) -> IoCall {
    IoCall::child(ForwardedCallerOperation::owner_token(
        peer, realm_id, auth_token,
    ))
}

fn called(result: IoResult) -> Result<AuthContext, SyncRefusal> {
    let caller = result.child::<AuthContext, ForwardAuthError>();
    caller
        .map_err(|_| SyncRefusal::Unavailable)?
        .map_err(forward_refusal)
}

/// The device may only ask for a version it serves itself: a peer must not make this node read
/// another node's object under its own authority.
fn own_source(peer: NodeId, request: &SyncPull) -> Result<(), SyncRefusal> {
    match request.source.node_id == peer {
        true => Ok(()),
        false => Err(SyncRefusal::Invalid(
            "a sync pull must name the requesting device".to_string(),
        )),
    }
}

/// The owner of a pull `peer` forwards, checked to name `peer` as its source.
#[derive(Debug, PartialEq)]
pub struct PullCallerOperation {
    peer: NodeId,
    realm_id: RealmId,
    auth_token: Option<AuthToken>,
    request: SyncPull,
}

impl PullCallerOperation {
    pub fn new(
        (peer, realm_id): (NodeId, RealmId),
        auth_token: AuthToken,
        request: SyncPull,
    ) -> Self {
        Self {
            peer,
            realm_id,
            auth_token: Some(auth_token),
            request,
        }
    }
}

impl Operation for PullCallerOperation {
    type Output = AuthContext;
    type Error = PullError;

    fn first(&mut self) -> Result<Next<AuthContext>, PullError> {
        let token = self.auth_token.take().ok_or(SyncRefusal::Unauthorized)?;
        Ok(Next::Call(caller(self.peer, self.realm_id, token)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuthContext>, PullError> {
        let auth = called(result)?;
        own_source(self.peer, &self.request)?;
        Ok(Next::Done(auth))
    }
}

/// The version this device's exact source already produced here, if any. Its receipt commits
/// with it, so a later unrelated write cannot make a replay mint a second copy.
#[derive(Debug, PartialEq)]
pub struct ReceiptOperation {
    key: Option<Key>,
}

impl ReceiptOperation {
    pub fn new(request: &SyncPull) -> Self {
        Self {
            key: receipt_of(request).ok(),
        }
    }
}

impl Operation for ReceiptOperation {
    type Output = Option<Ulid>;
    type Error = PullError;

    fn first(&mut self) -> Result<Next<Option<Ulid>>, PullError> {
        let Some(key) = self.key.take() else {
            return Ok(Next::Done(None));
        };
        Ok(Next::Call(IoCall::Read(vec![(
            SYNC_RECEIPT_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Ulid>>, PullError> {
        let value = result.value().ok().flatten();
        let version = value.and_then(|value| <[u8; 16]>::try_from(value.as_ref()).ok());
        Ok(Next::Done(version.map(Ulid::from_bytes)))
    }
}

fn replay_ack(version_id: Ulid) -> SyncPullAck {
    SyncPullAck {
        version_id,
        already_applied: true,
    }
}

/// Where an accepted pull stands before it runs.
#[derive(Debug, PartialEq)]
pub enum Admission {
    /// The device version already committed here.
    Replayed(SyncPullAck),
    /// The pull may write the target bucket.
    Admitted(BucketInfo),
}

#[derive(Debug, PartialEq)]
enum AdmitStage {
    Receipt,
    Bucket,
    Guard(BucketInfo),
    Route(BucketInfo),
    Write(BucketInfo),
}

/// Admits one pull of the authorized owner: a replay answers with its receipt; otherwise the
/// target bucket must exist, accept client writes, be coordinated here and be writable.
#[derive(Debug, PartialEq)]
pub struct AdmitPullOperation {
    node_id: NodeId,
    auth: AuthContext,
    request: SyncPull,
    stage: AdmitStage,
}

impl AdmitPullOperation {
    pub fn new(node_id: NodeId, auth: AuthContext, request: SyncPull) -> Self {
        Self {
            node_id,
            auth,
            request,
            stage: AdmitStage::Receipt,
        }
    }
}

impl Operation for AdmitPullOperation {
    type Output = Admission;
    type Error = PullError;

    fn first(&mut self) -> Result<Next<Admission>, PullError> {
        Ok(Next::Call(IoCall::child(ReceiptOperation::new(
            &self.request,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Admission>, PullError> {
        match std::mem::replace(&mut self.stage, AdmitStage::Receipt) {
            AdmitStage::Receipt => {
                // A replay answers with the version it produced, even after its range moved away.
                let receipt = result
                    .child::<Option<Ulid>, PullError>()
                    .map_err(unavailable)??;
                if let Some(version_id) = receipt {
                    return Ok(Next::Done(Admission::Replayed(replay_ack(version_id))));
                }
                self.stage = AdmitStage::Bucket;
                let bucket = self.request.target_bucket.clone();
                Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))))
            }
            AdmitStage::Bucket => {
                let bucket = read_bucket(result, &self.request.target_bucket)?;
                self.stage = AdmitStage::Guard(bucket);
                let guard = BucketGuardOperation::new(self.request.target_bucket.as_str());
                Ok(Next::Call(IoCall::child(guard)))
            }
            AdmitStage::Guard(bucket) => {
                let guarded = result
                    .child::<(), BucketGuardError>()
                    .map_err(unavailable)?;
                guarded.map_err(|error| guard_refusal(error, &self.request.target_bucket))?;
                let Some(binding) = bucket.distribution.as_ref() else {
                    return self.authorize(bucket);
                };
                let route = route_write(binding, &self.request.target_key);
                self.stage = AdmitStage::Route(bucket);
                Ok(Next::Call(IoCall::child(route)))
            }
            AdmitStage::Route(bucket) => {
                let route = result
                    .child::<WriteRoute, GuardError>()
                    .map_err(unavailable)?;
                route_refusal(route)?;
                self.authorize(bucket)
            }
            AdmitStage::Write(bucket) => {
                let checked = result.child::<(), AuthorizeError>().map_err(unavailable)?;
                checked.map_err(pull_refusal)?;
                Ok(Next::Done(Admission::Admitted(bucket)))
            }
        }
    }
}

impl AdmitPullOperation {
    fn authorize(&mut self, bucket: BucketInfo) -> Result<Next<Admission>, PullError> {
        let check = write_authorization(&self.auth, self.node_id, &bucket, &self.request);
        self.stage = AdmitStage::Write(bucket);
        Ok(Next::Call(IoCall::child(check)))
    }
}

/// Reads the target bucket. Only a bucket that is really absent is `NotFound`: the drain parks
/// that permanently, so a storage or decoding failure has to stay retryable instead.
fn read_bucket(result: IoResult, bucket: &str) -> Result<BucketInfo, SyncRefusal> {
    let read = result
        .child::<BucketInfo, GetBucketError>()
        .map_err(|_| SyncRefusal::Unavailable)?;
    read.map_err(|error| {
        debug!(error = %error, bucket = %bucket, "A sync pull could not read its target bucket");
        bucket_refusal(error)
    })
}

/// A pull writes like a client, so a bucket that is read-only for clients refuses it too.
fn guard_refusal(error: BucketGuardError, bucket: &str) -> SyncRefusal {
    match OfferedDirectoryError::from(error) {
        OfferedDirectoryError::ReadOnly(_) => SyncRefusal::Forbidden,
        error => {
            debug!(error = %error, bucket = %bucket, "A sync pull could not check its target bucket");
            SyncRefusal::Unavailable
        }
    }
}

/// Only the coordinator of the key's range writes a distributed bucket; the device takes the
/// pull there, since the pull reads the device's bytes under its own peer identity.
fn route_refusal(route: Result<WriteRoute, GuardError>) -> Result<(), SyncRefusal> {
    match route {
        Ok(WriteRoute::Local { .. }) => Ok(()),
        Ok(WriteRoute::Forward { node_id, .. }) => Err(SyncRefusal::Coordinator(node_id)),
        Err(
            GuardError::RecordMissing(_)
            | GuardError::Unreadable(_)
            | GuardError::Route(
                RouteError::Pending(_) | RouteError::Moving(_) | RouteError::StaleRange(_),
            ),
        ) => Err(SyncRefusal::Unavailable),
        Err(error) => Err(SyncRefusal::Invalid(error.to_string())),
    }
}

/// A bucket that is not there is permanent; everything else is this node's problem and must not
/// park the device's upload.
fn bucket_refusal(error: GetBucketError) -> SyncRefusal {
    match error {
        GetBucketError::NotFound => SyncRefusal::NotFound,
        _ => SyncRefusal::Unavailable,
    }
}

/// The WRITE check of a pull, judged as the put or delete it performs.
fn write_authorization(
    auth: &AuthContext,
    node_id: NodeId,
    bucket: &BucketInfo,
    request: &SyncPull,
) -> AuthorizeOperation {
    let operation = match request.deleted {
        true => "s3.DeleteObject",
        false => "s3.PutObject",
    };
    let (bucket_name, key) = (&request.target_bucket, &request.target_key);
    let path = object_permission_path(auth.realm_id, bucket.group_id, node_id, bucket_name, key);
    authorization(auth, path, Permission::WRITE, operation)
}

/// The READ check of a listing, judged as the version listing an S3 client would make.
fn read_authorization(
    auth: &AuthContext,
    node_id: NodeId,
    group_id: GroupId,
    bucket: &str,
) -> AuthorizeOperation {
    let path = bucket_permission_path(auth.realm_id, group_id, node_id, bucket);
    authorization(auth, path, Permission::READ, "s3.ListObjectVersions")
}

/// The same boundary the equivalent S3 request passes: RBAC first, then the realm and group
/// request policies. A pull is an ordinary write by the owner, so a policy that denies their
/// put or delete denies this too.
fn authorization(
    auth: &AuthContext,
    path: String,
    permission: Permission,
    operation: &'static str,
) -> AuthorizeOperation {
    let extras = PolicyRequestExtras::operation(operation);
    AuthorizeOperation::new(auth.realm_id, auth, &path, &permission, extras)
}

/// Only a real denial refuses the caller; an unreadable policy state or a failed check is this
/// node's problem and stays retryable.
pub(crate) fn pull_refusal(error: AuthorizeError) -> SyncRefusal {
    match error {
        AuthorizeError::PermissionDenied
        | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => SyncRefusal::Forbidden,
        AuthorizeError::Policy(PolicyEnforcementError::Unavailable(_))
        | AuthorizeError::Storage(_)
        | AuthorizeError::CheckFailed(_) => SyncRefusal::Unavailable,
    }
}

type PullBody = BackendStream<Result<bytes::Bytes, StreamError>>;

/// The device's bytes as the write consumes them, adding each chunk to `bytes_done`.
fn counted(blob: PullBody, bytes_done: Arc<AtomicU64>) -> PullBody {
    let chunks = blob.0.inspect(move |chunk| {
        if let Ok(chunk) = chunk {
            bytes_done.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }
    });
    BackendStream(Box::pin(chunks))
}

/// Why a device read failed. Bytes that do not answer for the hash the upload named are the
/// request's own fault: retrying re-reads the same mismatch, so the row is parked for the owner
/// instead. Everything else is infrastructure.
fn read_refusal(error: &BaoReadError) -> SyncRefusal {
    match error {
        BaoReadError::Refused(BaoReadRefusal::HashMismatch) => SyncRefusal::Invalid(
            "the device version no longer carries the bytes this pull named".to_string(),
        ),
        _ => SyncRefusal::Unavailable,
    }
}

/// The device version a realm version came from and the device that published it. The local
/// path stays on the device.
fn pull_tags(request: &SyncPull) -> HashMap<String, String> {
    HashMap::from([
        (
            SYNC_VERSION_TAG.to_string(),
            request.source.version.to_string(),
        ),
        (
            ORIGIN_DEVICE_TAG.to_string(),
            request.source.node_id.to_string(),
        ),
    ])
}

/// An upload this node received from the authenticated device that published the version.
fn pull_origin(node_id: NodeId, request: &SyncPull) -> VersionOrigin {
    VersionOrigin::Upload {
        node_id,
        device: Some(request.source.node_id.to_string()),
        client: Some(UploadClient::Device),
    }
}

/// The receipt row of this device version: absent until the version or marker commits.
pub(crate) fn receipt_of(request: &SyncPull) -> Result<Key, SyncRefusal> {
    request
        .receipt_key()
        .map(Key::from)
        .map_err(|error| SyncRefusal::Invalid(error.to_string()))
}

/// Guard rows and receipt keys of one fenced write.
type FenceRows = (Vec<(String, Key, Option<Value>)>, Vec<(String, Key)>);

/// The receipt must still be absent in the write's transaction, which then records it.
fn pull_fence(request: &SyncPull) -> Result<FenceRows, SyncRefusal> {
    let key = receipt_of(request)?;
    let space = SYNC_RECEIPT_KEYSPACE.to_string();
    Ok((vec![(space.clone(), key.clone(), None)], vec![(space, key)]))
}

/// The bytes of a pull that arrived so far, shared with the poll that reports them.
#[derive(Clone, Debug, Default)]
pub struct PullProgress(pub Arc<AtomicU64>);

/// Two progress counters are the same when they count the same pull.
impl PartialEq for PullProgress {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Debug, PartialEq)]
enum CommitStage {
    Start,
    Replay,
    Deleted,
    Read,
    Routing(Box<PullBody>),
    Quota(Box<PullBody>, RoutingSnapshot),
    Written,
    /// A step failed; an overlapping pull of the same version may have committed first.
    Failed(SyncRefusal),
}

/// Commits one admitted pull as the owner's own write: a delete marker, or the device's exact
/// version read from the device, counting the bytes that arrive into `bytes_done`. A failure
/// whose device version committed anyway answers with that receipt.
#[derive(Debug, PartialEq)]
pub struct CommitPullOperation {
    node: (NodeId, NodeRouting),
    auth: AuthContext,
    bucket: BucketInfo,
    request: SyncPull,
    bytes_done: PullProgress,
    stage: CommitStage,
}

type CommitTurn = Result<Next<SyncPullAck>, PullError>;

impl CommitPullOperation {
    /// `routing` is this node's storage routing, which the write is placed by.
    pub fn new(
        node: (NodeId, NodeRouting),
        (auth, bucket): (AuthContext, BucketInfo),
        request: SyncPull,
        bytes_done: PullProgress,
    ) -> Self {
        Self {
            node,
            auth,
            bucket,
            request,
            bytes_done,
            stage: CommitStage::Start,
        }
    }

    fn failed(&mut self, refusal: SyncRefusal) -> CommitTurn {
        self.stage = CommitStage::Failed(refusal);
        Ok(Next::Call(IoCall::child(ReceiptOperation::new(
            &self.request,
        ))))
    }

    fn delete(&mut self) -> CommitTurn {
        let (guards, receipts) = match pull_fence(&self.request) {
            Ok(fence) => fence,
            Err(refusal) => return self.failed(refusal),
        };
        let input = DeleteObjectInput {
            bucket: self.request.target_bucket.clone(),
            key: self.request.target_key.clone(),
            version_id: None,
            group_id: self.bucket.group_id,
            realm_id: self.auth.realm_id,
            node_id: self.node.0,
            deleted_by: self.auth.user_id,
        };
        // The marker carries the device version it came from, and the receipt committed with
        // it makes a retried delete the same one.
        let delete = DeleteObjectOperation::new(input)
            .with_metadata(pull_tags(&self.request))
            .with_fence(guards, receipts);
        self.stage = CommitStage::Deleted;
        Ok(Next::Call(IoCall::child(delete)))
    }

    fn read(&mut self) -> CommitTurn {
        let read = BaoReadRequest {
            auth_context: self.auth.clone(),
            realm_id: self.auth.realm_id,
            target: BaoReadTarget::ExactVersion(self.request.source.clone()),
            expected_blake3: self.request.blake3,
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        self.stage = CommitStage::Read;
        let read = ManagedReadOperation::new(self.request.source.node_id, read);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn put(&mut self, blob: PullBody, routing: RoutingSnapshot, quota: Option<u64>) -> CommitTurn {
        let (guards, receipts) = match pull_fence(&self.request) {
            Ok(fence) => fence,
            Err(refusal) => return self.failed(refusal),
        };
        let (auth, request) = (&self.auth, &self.request);
        let node_id = self.node.0;
        let put = PutObjectOperation::new(PutObjectConfig {
            user_id: auth.user_id,
            group_id: self.bucket.group_id,
            realm_id: auth.realm_id,
            node_id,
            request: PutObjectInput {
                bucket: request.target_bucket.clone(),
                key: request.target_key.clone(),
                content_length: Some(request.size),
                body: Some(blob),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: quota,
            routing,
        })
        .with_bucket_guard(self.bucket.clone())
        .with_metadata(pull_tags(request))
        .with_version_origin(Some(pull_origin(node_id, request)))
        .with_fence(guards, receipts);
        self.stage = CommitStage::Written;
        Ok(Next::Call(IoCall::child(put)))
    }
}

impl Operation for CommitPullOperation {
    type Output = SyncPullAck;
    type Error = PullError;

    fn first(&mut self) -> CommitTurn {
        self.stage = CommitStage::Replay;
        Ok(Next::Call(IoCall::child(ReceiptOperation::new(
            &self.request,
        ))))
    }

    fn next(&mut self, result: IoResult) -> CommitTurn {
        match std::mem::replace(&mut self.stage, CommitStage::Start) {
            CommitStage::Start => Err(SyncRefusal::Unavailable.into()),
            CommitStage::Replay => {
                let receipt = result
                    .child::<Option<Ulid>, PullError>()
                    .map_err(unavailable)??;
                if let Some(version_id) = receipt {
                    return Ok(Next::Done(replay_ack(version_id)));
                }
                match self.request.deleted {
                    true => self.delete(),
                    false => self.read(),
                }
            }
            CommitStage::Deleted => {
                let deleted = result.child::<DeleteObjectResult, DeleteObjectError>();
                let deleted = match deleted.map_err(unavailable).and_then(|deleted| {
                    deleted.map_err(|error| {
                        debug!(error = %error, "A forwarded sync delete failed");
                        SyncRefusal::Unavailable
                    })
                }) {
                    Ok(deleted) => deleted,
                    Err(refusal) => return self.failed(refusal),
                };
                let live = QueueLiveOperation::new(LiveVersionInput {
                    local_node_id: self.node.0,
                    auth_context: self.auth.clone(),
                    bucket: self.request.target_bucket.clone(),
                    key: self.request.target_key.clone(),
                    version_id: deleted.version_id,
                    delete_marker: deleted.delete_marker,
                });
                Ok(Next::Then {
                    output: SyncPullAck {
                        version_id: deleted.version_id,
                        already_applied: false,
                    },
                    calls: vec![IoCall::child(live)],
                })
            }
            CommitStage::Read => {
                let read = result
                    .child::<BaoReadOutput, BaoReadError>()
                    .map_err(unavailable);
                let blob = match read {
                    Ok(Ok(BaoReadOutput::Stream { blob, .. })) => {
                        counted(blob, self.bytes_done.0.clone())
                    }
                    // A metadata-only answer to a full read is a broken exchange, not an absent
                    // object: parking the upload on it would lose the file.
                    Ok(Ok(BaoReadOutput::Metadata { .. })) | Err(_) => {
                        return self.failed(SyncRefusal::Unavailable);
                    }
                    Ok(Err(error)) => {
                        debug!(error = %error, "Could not read a device version for a sync pull");
                        return self.failed(read_refusal(&error));
                    }
                };
                let rules = BucketRules::Named(self.request.target_bucket.clone());
                let routing = SnapshotOperation::new(&self.node.1, self.bucket.group_id, rules);
                self.stage = CommitStage::Routing(Box::new(blob));
                Ok(Next::Call(IoCall::child(routing)))
            }
            CommitStage::Routing(blob) => {
                let routing = result.child::<RoutingSnapshot, RoutingInputsError>();
                let Ok(Ok(routing)) = routing else {
                    return self.failed(SyncRefusal::Unavailable);
                };
                self.stage = CommitStage::Quota(blob, routing);
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.auth.realm_id,
                ))))
            }
            CommitStage::Quota(blob, routing) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>();
                let group_id = self.bucket.group_id;
                let quota = match config {
                    Ok(Ok(config)) => config.quota.effective_group_ceiling(&group_id),
                    Ok(Err(error)) => {
                        crate::realm::load_config::config_unavailable(&error);
                        None
                    }
                    Err(_) => None,
                };
                self.put(*blob, routing, quota)
            }
            CommitStage::Written => {
                let written = result.child::<PutObjectResult, PutObjectError>();
                let written = match written {
                    Ok(Ok(written)) => written,
                    Ok(Err(error)) => {
                        debug!(error = %error, "A forwarded sync pull could not be written");
                        return self.failed(SyncRefusal::Unavailable);
                    }
                    Err(_) => return self.failed(SyncRefusal::Unavailable),
                };
                let complete = CompletePutOperation::new(CompletePutInput {
                    realm_id: self.auth.realm_id,
                    node_id: self.node.0,
                    auth: self.auth.clone(),
                    group_id: self.bucket.group_id,
                    bucket: self.request.target_bucket.clone(),
                    key: self.request.target_key.clone(),
                    version_id: written.version_id,
                    size_bytes: written.location.blob_size,
                });
                Ok(Next::Then {
                    output: SyncPullAck {
                        version_id: written.version_id,
                        already_applied: false,
                    },
                    calls: vec![IoCall::child(complete)],
                })
            }
            CommitStage::Failed(refusal) => {
                // An overlapping pull of the same version committed first; its receipt is the
                // answer.
                let receipt = result.child::<Option<Ulid>, PullError>();
                match receipt {
                    Ok(Ok(Some(version_id))) => Ok(Next::Done(replay_ack(version_id))),
                    _ => Err(refusal.into()),
                }
            }
        }
    }
}

pub(crate) fn refusal_kind(refusal: &SyncRefusal) -> &'static str {
    match refusal {
        SyncRefusal::Unauthorized => "unauthorized",
        SyncRefusal::Forbidden => "forbidden",
        SyncRefusal::NotFound => "not_found",
        SyncRefusal::Invalid(_) => "invalid",
        SyncRefusal::Unavailable => "unavailable",
        SyncRefusal::Coordinator(_) => "coordinator",
    }
}

/// Logs a refused pull and answers with the refusal.
pub(crate) fn refused(peer: NodeId, request: &SyncPull, refusal: SyncRefusal) -> SyncRefusal {
    let bucket = &request.target_bucket;
    warn!(peer = %peer, bucket = %bucket, refusal = refusal_kind(&refusal), "Refused a synced-folder pull");
    refusal
}

/// Ranges one page of a distributed bucket's heads visits at most.
const MAX_PAGE_RANGES: usize = 64;

/// One device listing of a bucket's heads.
#[derive(Clone, Debug, PartialEq)]
pub struct SetListing {
    pub bucket: String,
    pub prefix: String,
    pub cursor: Option<SyncListCursor>,
    pub limit: SyncPageLimit,
}

#[derive(Debug, PartialEq)]
enum HeadsStage {
    Caller,
    Bucket,
    Read(BucketInfo),
    Plain,
    Set,
}

/// One bounded listing of the current heads under a bucket prefix for the owner `peer` forwards.
#[derive(Debug, PartialEq)]
pub struct ListHeadsOperation {
    peer: NodeId,
    node: ServingNode,
    auth_token: Option<AuthToken>,
    auth: Option<AuthContext>,
    bucket: String,
    prefix: String,
    listing: Option<SetListing>,
    stage: HeadsStage,
}

impl ListHeadsOperation {
    pub fn new(
        peer: NodeId,
        node: ServingNode,
        auth_token: AuthToken,
        listing: SetListing,
    ) -> Self {
        Self {
            peer,
            node,
            auth_token: Some(auth_token),
            auth: None,
            bucket: listing.bucket.clone(),
            prefix: listing.prefix.clone(),
            listing: Some(listing),
            stage: HeadsStage::Caller,
        }
    }

    fn listing(&mut self) -> Result<SetListing, SyncRefusal> {
        self.listing.take().ok_or(SyncRefusal::Unavailable)
    }
}

type HeadsTurn = Result<Next<SyncVersionPage>, PullError>;

impl Operation for ListHeadsOperation {
    type Output = SyncVersionPage;
    type Error = PullError;

    fn first(&mut self) -> HeadsTurn {
        let token = self.auth_token.take().ok_or(SyncRefusal::Unauthorized)?;
        Ok(Next::Call(caller(self.peer, self.node.1, token)))
    }

    fn next(&mut self, result: IoResult) -> HeadsTurn {
        match std::mem::replace(&mut self.stage, HeadsStage::Plain) {
            HeadsStage::Caller => {
                self.auth = Some(called(result)?);
                self.stage = HeadsStage::Bucket;
                let read = GetBucketOperation::new(self.bucket.clone());
                Ok(Next::Call(IoCall::child(read)))
            }
            HeadsStage::Bucket => {
                let info = read_bucket(result, &self.bucket)?;
                let auth = self.auth.as_ref().ok_or(SyncRefusal::Unavailable)?;
                let check = read_authorization(auth, self.node.0, info.group_id, &self.bucket);
                self.stage = HeadsStage::Read(info);
                Ok(Next::Call(IoCall::child(check)))
            }
            HeadsStage::Read(info) => {
                let checked = result.child::<(), AuthorizeError>().map_err(unavailable)?;
                checked.map_err(pull_refusal)?;
                let listing = self.listing()?;
                let auth = self.auth.take().ok_or(SyncRefusal::Unavailable)?;
                if let Some(binding) = info.distribution {
                    let set =
                        SetHeadsOperation::new((self.peer, self.node), auth, binding, listing);
                    self.stage = HeadsStage::Set;
                    return Ok(Next::Call(IoCall::child(set)));
                }
                let (key_marker, version_id_marker) = match listing.cursor {
                    Some(cursor) => (Some(cursor.key), cursor.version_id),
                    None => (None, None),
                };
                let prefix = (!listing.prefix.is_empty()).then(|| listing_prefix(&listing.prefix));
                let list = ListVersionsOperation::new(ListVersionsInput {
                    bucket: listing.bucket,
                    prefix,
                    delimiter: None,
                    key_marker,
                    version_id_marker,
                    max_keys: Some(listing.limit.get()),
                });
                self.stage = HeadsStage::Plain;
                Ok(Next::Call(IoCall::child(list)))
            }
            HeadsStage::Plain => {
                let listed = result.child::<ListVersionsResult, ListVersionsError>();
                let listed = listed.map_err(unavailable)?.map_err(unavailable)?;
                let heads = (listed.items.iter())
                    .filter_map(|item| head_of(item, &self.prefix))
                    .collect();
                let next = listed.is_truncated.then(|| SyncListCursor {
                    key: listed.next_key_marker.unwrap_or_default(),
                    version_id: listed.next_version_marker,
                });
                let page = SyncVersionPage::new(heads, next);
                Ok(Next::Done(page.map_err(|error| {
                    SyncRefusal::Invalid(error.to_string())
                })?))
            }
            HeadsStage::Set => {
                let page = result
                    .child::<SyncVersionPage, PullError>()
                    .map_err(unavailable)??;
                Ok(Next::Done(page))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum SetStage {
    View,
    Fence(DistributionView),
    Range,
    Holder,
    Forwarded(NodeId),
}

/// One page of a distributed bucket's heads in key order, range by range from this member's
/// inventory. A range it does not list ends the page there; a page that starts there is read
/// from a holder of the range for a device, and refused for a forwarded listing.
#[derive(Debug, PartialEq)]
pub struct SetHeadsOperation {
    peer: NodeId,
    node: ServingNode,
    auth: AuthContext,
    binding: DistributionBinding,
    listing: SetListing,
    view: Option<DistributionView>,
    fence: LocalFence,
    ranges: Vec<KeyRange>,
    visited: usize,
    marker: Option<String>,
    version_marker: Option<Ulid>,
    heads: Vec<RemoteHead>,
    stage: SetStage,
}

impl SetHeadsOperation {
    pub fn new(
        (peer, node): (NodeId, ServingNode),
        auth: AuthContext,
        binding: DistributionBinding,
        listing: SetListing,
    ) -> Self {
        let (marker, version_marker) = match listing.cursor.clone() {
            Some(cursor) => (Some(cursor.key), cursor.version_id),
            None => (None, None),
        };
        Self {
            peer,
            node,
            auth,
            binding,
            listing,
            view: None,
            fence: LocalFence::default(),
            ranges: Vec::new(),
            visited: 0,
            marker,
            version_marker,
            heads: Vec::new(),
            stage: SetStage::View,
        }
    }

    fn prefix(&self) -> Option<String> {
        (!self.listing.prefix.is_empty()).then(|| listing_prefix(&self.listing.prefix))
    }

    fn page(&mut self, next: Option<SyncListCursor>) -> HeadsTurn {
        let heads = std::mem::take(&mut self.heads);
        let page = SyncVersionPage::new(heads, next);
        Ok(Next::Done(page.map_err(|error| {
            SyncRefusal::Invalid(error.to_string())
        })?))
    }

    /// Lists the next range here, ends the page, or asks a holder for a first range kept
    /// elsewhere.
    fn range(&mut self) -> HeadsTurn {
        let Some(range) = self.ranges.get(self.visited).cloned() else {
            return self.page(None);
        };
        let snapshot = self
            .view
            .as_ref()
            .ok_or(SyncRefusal::Unavailable)?
            .snapshot();
        let remaining = self.listing.limit.get().saturating_sub(self.heads.len());
        let here = snapshot.lists_locally(self.binding.member_id, &range, &self.fence);
        if remaining == 0 || self.visited == MAX_PAGE_RANGES || (!here && self.visited > 0) {
            // The nil version id starts the next page at the first key of this range.
            let next = SyncListCursor {
                key: range.lower.clone(),
                version_id: Some(Ulid::nil()),
            };
            return self.page(Some(next));
        }
        if !here {
            self.stage = SetStage::Holder;
            return Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                self.auth.realm_id,
            ))));
        }
        self.visited += 1;
        let list = InventoryVersionsOperation::new(InventoryVersionsInput {
            set_id: self.binding.set_id,
            bucket: self.listing.bucket.clone(),
            prefix: self.prefix(),
            delimiter: None,
            key_marker: self.marker.take(),
            version_id_marker: self.version_marker.take(),
            max_keys: remaining,
            lower: range.lower.clone(),
            upper: range.upper.clone(),
        });
        self.stage = SetStage::Range;
        Ok(Next::Call(IoCall::child(list)))
    }

    /// The page a ready holder of the first range answers; only a device's listing is passed
    /// on, once.
    fn holder(&mut self, config: RealmConfigDocument) -> HeadsTurn {
        let range = self
            .ranges
            .get(self.visited)
            .cloned()
            .ok_or(SyncRefusal::Unavailable)?;
        let snapshot = self
            .view
            .as_ref()
            .ok_or(SyncRefusal::Unavailable)?
            .snapshot();
        let device = node_kind(&config, self.peer).is_some_and(|kind| kind.owner().is_some());
        let (local, peer) = (self.node.0, self.peer);
        let holder = (snapshot.list_holder(&range).into_iter())
            .map(|member| member.node_id)
            .find(|node_id| *node_id != local && *node_id != peer);
        let (Some(holder), true) = (holder, device) else {
            return Err(SyncRefusal::Unavailable.into());
        };
        let message = MetadataTransportMessage::ForwardListVersions {
            auth_token: AuthToken::internal(self.auth.clone()),
            bucket: self.listing.bucket.clone(),
            prefix: self.listing.prefix.clone(),
            cursor: self.listing.cursor.clone(),
            limit: self.listing.limit,
        };
        let frame = encode_message(&message).map_err(|_| SyncRefusal::Unavailable)?;
        self.stage = SetStage::Forwarded(holder);
        Ok(Next::Call(peer_call(holder, frame, HOLDER_TIMEOUT)))
    }
}

impl Operation for SetHeadsOperation {
    type Output = SyncVersionPage;
    type Error = PullError;

    fn first(&mut self) -> HeadsTurn {
        Ok(Next::Call(IoCall::child(bound_view(&self.binding))))
    }

    fn next(&mut self, result: IoResult) -> HeadsTurn {
        match std::mem::replace(&mut self.stage, SetStage::Range) {
            SetStage::View => {
                let view = result
                    .child::<DistributionView, GuardError>()
                    .map_err(unavailable)?;
                let view = view.map_err(unavailable)?;
                self.stage = SetStage::Fence(view);
                Ok(Next::Call(IoCall::child(local_fence(self.binding.set_id))))
            }
            SetStage::Fence(view) => {
                let fence = result
                    .child::<LocalFence, GuardError>()
                    .map_err(unavailable)?;
                self.fence = fence.map_err(unavailable)?;
                let start = self.marker.clone().unwrap_or_default();
                let prefix = self.prefix().unwrap_or_default();
                let listed = view.snapshot().listed_ranges(&prefix).into_iter();
                self.ranges = listed
                    .filter(|range| {
                        range
                            .upper
                            .as_deref()
                            .is_none_or(|upper| start.as_str() < upper)
                    })
                    .cloned()
                    .collect();
                self.view = Some(view);
                self.range()
            }
            SetStage::Range => {
                let listed = result.child::<ListVersionsResult, InventoryVersionsError>();
                let listed = listed.map_err(unavailable)?.map_err(unavailable)?;
                let prefix = self.listing.prefix.clone();
                let heads = listed
                    .items
                    .iter()
                    .filter_map(|item| head_of(item, &prefix));
                self.heads.extend(heads);
                if listed.is_truncated {
                    let next = SyncListCursor {
                        key: listed.next_key_marker.unwrap_or_default(),
                        version_id: listed.next_version_marker,
                    };
                    return self.page(Some(next));
                }
                self.range()
            }
            SetStage::Holder => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()
                    .map_err(unavailable)?;
                let config = config
                    .inspect_err(crate::realm::load_config::config_unavailable)
                    .map_err(unavailable)?;
                self.holder(config)
            }
            SetStage::Forwarded(holder) => match peer_reply(holder, result).map_err(unavailable)? {
                Ok(MetadataTransportMessage::ForwardedVersions { result }) => {
                    Ok(Next::Done(result?))
                }
                _ => Err(SyncRefusal::Unavailable.into()),
            },
        }
    }
}

/// The listing prefix a bound folder maps to, always ending in a separator so a sibling
/// directory with the same leading characters is not listed too.
fn listing_prefix(prefix: &str) -> String {
    format!("{}/", prefix.trim_end_matches('/'))
}

/// One current head, named relative to the requested prefix. Only the latest
/// version of a key is a head; older versions are not the folder's state.
fn head_of(item: &ListVersionsItem, prefix: &str) -> Option<RemoteHead> {
    let (key, version_id, deleted, location, metadata, created_at) = match item {
        ListVersionsItem::Version {
            key,
            version_id,
            is_latest: true,
            location,
            source_metadata,
            created_at,
            ..
        } => (
            key,
            version_id,
            false,
            location.as_ref(),
            source_metadata,
            created_at,
        ),
        ListVersionsItem::DeleteMarker {
            key,
            version_id,
            is_latest: true,
            created_at,
        } => (key, version_id, true, None, &None, created_at),
        _ => return None,
    };
    Some(RemoteHead {
        relative: relative_key(key, prefix)?,
        version_id: *version_id,
        size: location
            .map(|location| location.blob_size)
            .or_else(|| metadata.as_ref().map(|metadata| metadata.content_length))
            .unwrap_or_default(),
        blake3: location
            .and_then(|location| location.get_blake3())
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok()),
        modified_at_ms: created_at
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|since| u64::try_from(since.as_millis()).ok()),
        deleted,
    })
}

fn relative_key(key: &str, prefix: &str) -> Option<String> {
    match prefix.is_empty() {
        true => Some(key.to_string()),
        false => key
            .strip_prefix(&listing_prefix(prefix))
            .map(ToOwned::to_owned),
    }
}

#[cfg(test)]
#[path = "device_pull_tests.rs"]
mod call_tests;

#[cfg(test)]
mod tests {
    //! Tests for the realm side of a device's synced folders.
    use aruna_core::structs::storage::format::Compression;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::{ConversionError, StorageError};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };
    use aruna_core::structs::storage::replication::VersionedObjectArn;
    use aruna_storage::FjallStorage;
    use std::time::SystemTime;

    async fn guard_target(context: &DriverContext, bucket: &str) -> Result<(), SyncRefusal> {
        let guard = drive(BucketGuardOperation::new(bucket), context).await;
        guard.map_err(|error| guard_refusal(error, bucket))
    }

    async fn read_bucket(context: &DriverContext, bucket: &str) -> Result<BucketInfo, SyncRefusal> {
        let read = drive(GetBucketOperation::new(bucket.to_string()), context).await;
        read.map_err(bucket_refusal)
    }

    async fn coordinates(
        context: &DriverContext,
        bucket: &BucketInfo,
        key: &str,
    ) -> Result<(), SyncRefusal> {
        let Some(binding) = bucket.distribution.as_ref() else {
            return Ok(());
        };
        route_refusal(drive(route_write(binding, key), context).await)
    }

    async fn ensure_write(
        context: &DriverContext,
        auth: &AuthContext,
        node_id: NodeId,
        bucket: &BucketInfo,
        request: &SyncPull,
    ) -> Result<(), SyncRefusal> {
        let check = write_authorization(auth, node_id, bucket, request);
        drive(check, context).await.map_err(pull_refusal)
    }

    async fn ensure_read(
        context: &DriverContext,
        auth: &AuthContext,
        node_id: NodeId,
        group_id: GroupId,
        bucket: &str,
    ) -> Result<(), SyncRefusal> {
        let check = read_authorization(auth, node_id, group_id, bucket);
        drive(check, context).await.map_err(pull_refusal)
    }

    async fn applied_version(context: &DriverContext, request: &SyncPull) -> Option<Ulid> {
        drive(ReceiptOperation::new(request), context)
            .await
            .ok()
            .flatten()
    }

    async fn replayed(context: &DriverContext, request: &SyncPull) -> Option<SyncPullAck> {
        applied_version(context, request).await.map(replay_ack)
    }

    async fn set_heads(
        context: &DriverContext,
        (peer, auth): (NodeId, &AuthContext),
        binding: &DistributionBinding,
        listing: SetListing,
    ) -> Result<SyncVersionPage, SyncRefusal> {
        let node = (peer, (peer, auth.realm_id));
        let set = SetHeadsOperation::new(node, auth.clone(), binding.clone(), listing);
        drive(set, context).await.map_err(PullError::refusal)
    }

    fn deny_policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([7; 16]),
            name: "sync-pull-test".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    struct PolicyFixture {
        context: Arc<DriverContext>,
        auth: AuthContext,
        node_id: NodeId,
        group_id: GroupId,
        _dir: tempfile::TempDir,
    }

    /// A realm whose owner may write everywhere, optionally under one deny
    /// policy: the same state an S3 request of the same shape is judged against.
    async fn policy_fixture(expression: Option<&str>) -> PolicyFixture {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([5; 16]), realm_id);
        let group_id = Ulid::from_bytes([6; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let auth_doc = GroupAuthorizationDocument::default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "sync".to_string(),
            group_id,
            realm_id,
            roles: auth_doc.roles.keys().copied().collect(),
            owner: user_id,
        };
        let mut realm = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        realm.request_policies = expression.map(deny_policy).into_iter().collect();
        let _ = storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![
                    (
                        REALM_CONFIG_KEYSPACE.to_string(),
                        realm_id.as_bytes().to_vec().into(),
                        realm.to_bytes(&actor).unwrap().into(),
                    ),
                    (
                        GROUP_KEYSPACE.to_string(),
                        group_id.to_bytes().to_vec().into(),
                        group.to_bytes(&actor).unwrap().into(),
                    ),
                    (
                        AUTH_KEYSPACE.to_string(),
                        realm_id.as_bytes().to_vec().into(),
                        RealmAuthorizationDocument::default_realm_doc(realm_id)
                            .to_bytes(&actor)
                            .unwrap()
                            .into(),
                    ),
                    (
                        AUTH_KEYSPACE.to_string(),
                        group_id.to_bytes().to_vec().into(),
                        auth_doc.to_bytes(&actor).unwrap().into(),
                    ),
                ],
                txn_id: None,
            })
            .await;
        PolicyFixture {
            context: Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                graph_handle: None,
                task_handle: None,
                compute_handle: None,
                outbound_handle: None,
            }),
            auth: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            node_id,
            group_id,
            _dir: dir,
        }
    }

    fn bucket_info(group_id: GroupId, user_id: UserId) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn pull_request(deleted: bool) -> SyncPull {
        SyncPull {
            source: VersionedObjectArn::new(
                RealmId::from_bytes([4u8; 32]),
                iroh::SecretKey::from_bytes(&[9; 32]).public(),
                "folder-x".to_string(),
                "note.txt".to_string(),
                Ulid::from_bytes([2; 16]),
            )
            .unwrap(),
            blake3: None,
            size: 4,
            target_bucket: "lab".to_string(),
            target_key: "note.txt".to_string(),
            deleted,
        }
    }

    #[tokio::test]
    async fn policy_denies_pull() {
        // A deny policy that would refuse the owner's own S3 put must refuse the
        // forwarded pull that performs the same write.
        let fixture = policy_fixture(Some("operation == 's3.PutObject'")).await;
        let bucket = bucket_info(fixture.group_id, fixture.auth.user_id);
        assert_eq!(
            ensure_write(
                &fixture.context,
                &fixture.auth,
                fixture.node_id,
                &bucket,
                &pull_request(false),
            )
            .await,
            Err(SyncRefusal::Forbidden)
        );
        // The same request passes once the policy is gone.
        let allowed = policy_fixture(None).await;
        assert!(
            ensure_write(
                &allowed.context,
                &allowed.auth,
                allowed.node_id,
                &bucket_info(allowed.group_id, allowed.auth.user_id),
                &pull_request(false),
            )
            .await
            .is_ok()
        );
    }

    async fn store_receipt(context: &Arc<DriverContext>, request: &SyncPull, version_id: Ulid) {
        let stored = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: SYNC_RECEIPT_KEYSPACE.to_string(),
                key: receipt_of(request).unwrap(),
                value: version_id.to_bytes().to_vec().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            stored,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    // A pull into an offered folder bucket is refused, and so is one into the bucket of a folder
    // being unbound after its offer is gone. The cleanup on this node still deletes the bucket.
    #[tokio::test]
    async fn guards_folder_pull() {
        use crate::device::sync::folders::finish_unbind;
        use crate::device::sync::repository::folder_bucket;
        use crate::staging::offered_directory::{
            OfferDirectoryInput, WithdrawOfferInput, offer_directory, withdraw_offer,
        };
        use crate::tests::device::store_folder;
        use aruna_core::structs::{FolderMode, FolderState, RemoteBinding, SyncedFolder};

        let fixture = crate::tests::staging::setup_driver_context().await;
        let context = Arc::new(fixture.driver_context);
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("note.txt"), b"note").unwrap();
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let folder_id = Ulid::generate();
        let bucket = folder_bucket(folder_id);
        let offer = OfferDirectoryInput {
            bucket: bucket.clone(),
            root: root.path().to_string_lossy().to_string(),
            group_id: Ulid::generate(),
            realm_id,
            node_id,
            user_id,
        };
        drive(offer_directory(offer.clone()), &context)
            .await
            .unwrap();
        assert_eq!(
            guard_target(&context, &bucket).await,
            Err(SyncRefusal::Forbidden)
        );

        let folder = SyncedFolder {
            folder_id,
            root: offer.root.clone(),
            local_bucket: bucket.clone(),
            group_id: offer.group_id,
            remote: RemoteBinding {
                node_id,
                bucket: "lab".to_string(),
                prefix: String::new(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            state: FolderState::Deleting,
            created_by: user_id,
            created_at_ms: 1,
            last_reconcile_ms: None,
            last_error: None,
            last_error_ms: None,
            observed_files: 0,
            list_cursor: None,
        };
        store_folder(&context, &folder).await.unwrap();
        let withdrawal = WithdrawOfferInput {
            bucket: bucket.clone(),
            realm_id,
            node_id,
            user_id,
        };
        assert_eq!(drive(withdraw_offer(withdrawal), &context).await, Ok(1));
        assert_eq!(
            guard_target(&context, &bucket).await,
            Err(SyncRefusal::Forbidden)
        );

        assert_eq!(
            drive(
                finish_unbind(context.node_and_realm(), folder_id, node_id),
                &context
            )
            .await,
            Ok(())
        );
        assert!(matches!(
            read_bucket(&context, &bucket).await,
            Err(SyncRefusal::NotFound)
        ));
        assert_eq!(guard_target(&context, &bucket).await, Ok(()));
    }

    #[tokio::test]
    async fn delete_follows_upload() {
        // A delete must not treat the preceding upload's receipt as its own replay.
        let fixture = policy_fixture(None).await;
        let upload = Ulid::from_bytes([6; 16]);
        store_receipt(&fixture.context, &pull_request(false), upload).await;

        assert_eq!(
            applied_version(&fixture.context, &pull_request(true)).await,
            None
        );
        // The upload itself still replays as the version it produced.
        assert_eq!(
            applied_version(&fixture.context, &pull_request(false)).await,
            Some(upload)
        );
    }

    #[tokio::test]
    async fn replayed_delete_settles() {
        // A retried delete answers with the marker its receipt names, and only for the
        // device version that receipt was written for.
        let fixture = policy_fixture(None).await;
        let request = pull_request(true);
        let marker = Ulid::from_bytes([7; 16]);
        store_receipt(&fixture.context, &request, marker).await;

        assert_eq!(
            replayed(&fixture.context, &request).await,
            Some(SyncPullAck {
                version_id: marker,
                already_applied: true,
            })
        );
        let newer = SyncPull {
            source: VersionedObjectArn::new(
                request.source.realm_id,
                request.source.node_id,
                request.source.bucket.clone(),
                request.source.key.clone(),
                Ulid::from_bytes([8; 16]),
            )
            .unwrap(),
            ..pull_request(true)
        };
        assert_eq!(applied_version(&fixture.context, &newer).await, None);
    }

    #[tokio::test]
    async fn policy_denies_listing() {
        // The listing runs the boundary too, under the operation an S3 client
        // would be judged by.
        let fixture = policy_fixture(Some("operation == 's3.ListObjectVersions'")).await;
        assert_eq!(
            ensure_read(
                &fixture.context,
                &fixture.auth,
                fixture.node_id,
                fixture.group_id,
                "lab",
            )
            .await,
            Err(SyncRefusal::Forbidden)
        );
        let allowed = policy_fixture(None).await;
        assert!(
            ensure_read(
                &allowed.context,
                &allowed.auth,
                allowed.node_id,
                allowed.group_id,
                "lab",
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn policy_denies_delete() {
        // A delete pull is judged as the delete it performs, not as a put.
        let fixture = policy_fixture(Some("operation == 's3.DeleteObject'")).await;
        assert_eq!(
            ensure_write(
                &fixture.context,
                &fixture.auth,
                fixture.node_id,
                &bucket_info(fixture.group_id, fixture.auth.user_id),
                &pull_request(true),
            )
            .await,
            Err(SyncRefusal::Forbidden)
        );
    }

    fn version(key: &str, latest: bool) -> ListVersionsItem {
        ListVersionsItem::Version {
            key: key.to_string(),
            version_id: Ulid::from_bytes([1u8; 16]),
            is_latest: latest,
            location: None,
            source_metadata: Some(SourceMetadata {
                content_length: 7,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            }),
            created_at: SystemTime::UNIX_EPOCH,
            link: None,
        }
    }

    // A pulled version names the device it came from in its origin, within the size budget.
    #[test]
    fn origin_names_device() {
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let request = pull_request(false);
        let origin = pull_origin(node_id, &request);
        let device = Some(request.source.node_id.to_string());
        let client = Some(UploadClient::Device);
        let expected = VersionOrigin::Upload {
            node_id,
            device,
            client,
        };
        assert_eq!(origin, expected);
        assert!(origin.bounded());
    }

    #[test]
    fn names_heads_relatively() {
        // The device binds a prefix, so the answer is relative to that prefix.
        let head = head_of(&version("notes/a/b.txt", true), "notes").expect("a head");
        assert_eq!(head.relative, "a/b.txt");
        assert_eq!(head.size, 7);
        assert!(!head.deleted);
        assert!(head_of(&version("notes/a/b.txt", false), "notes").is_none());
        assert!(head_of(&version("other/b.txt", true), "notes").is_none());
    }

    #[test]
    fn parks_hash_mismatch() {
        // A missing source file parks the row, while transport failures remain retryable.
        assert_eq!(
            read_refusal(&BaoReadError::Refused(BaoReadRefusal::HashMismatch)),
            SyncRefusal::Invalid(
                "the device version no longer carries the bytes this pull named".to_string()
            )
        );
        assert_eq!(
            read_refusal(&BaoReadError::Refused(BaoReadRefusal::BackendFailure)),
            SyncRefusal::Unavailable
        );
        assert_eq!(
            read_refusal(&BaoReadError::NotFinished),
            SyncRefusal::Unavailable
        );
    }

    #[test]
    fn keeps_reads_retryable() {
        // The drain parks a NotFound for good, so only a bucket that is really
        // absent may answer it; a storage fault must stay retryable.
        assert_eq!(
            bucket_refusal(GetBucketError::NotFound),
            SyncRefusal::NotFound
        );
        assert_eq!(
            bucket_refusal(GetBucketError::StorageError(StorageError::KeyspaceError(
                "unavailable".to_string()
            ))),
            SyncRefusal::Unavailable
        );
        assert_eq!(
            bucket_refusal(GetBucketError::ConversionError(
                ConversionError::FromStrError("broken".to_string())
            )),
            SyncRefusal::Unavailable
        );
        assert_eq!(
            bucket_refusal(GetBucketError::Incomplete),
            SyncRefusal::Unavailable
        );
    }

    #[test]
    fn bounds_listing_prefix() {
        // `notes` must not list `notesbook`.
        assert_eq!(listing_prefix("notes"), "notes/");
        assert_eq!(listing_prefix("notes/"), "notes/");
        assert_eq!(relative_key("notesbook/x", "notes"), None);
        assert_eq!(relative_key("x", "").as_deref(), Some("x"));
    }

    // A distributed bucket lists range by range: the page ends where a range this member keeps
    // no records of begins, and a forwarded listing that starts there is refused, not passed on.
    #[tokio::test]
    async fn lists_set_ranges() {
        use crate::distribution::test_record::{binding, record, store_record};
        let fixture = policy_fixture(None).await;
        let (snapshot, bytes) = record(true);
        store_record(&fixture.context.storage_handle, snapshot.set_id(), bytes).await;
        let binding = binding(&snapshot, 1, 5);
        let listing = |cursor| SetListing {
            bucket: "spire".to_string(),
            prefix: String::new(),
            cursor,
            limit: SyncPageLimit::default(),
        };
        let peer = (fixture.node_id, &fixture.auth);
        let page = set_heads(&fixture.context, peer, &binding, listing(None))
            .await
            .unwrap();
        let (heads, next) = page.into_parts();
        assert!(heads.is_empty());
        let next = next.expect("the upper range follows");
        assert_eq!(
            (next.key.as_str(), next.version_id),
            ("m", Some(Ulid::nil()))
        );
        let forwarded = set_heads(&fixture.context, peer, &binding, listing(Some(next))).await;
        assert_eq!(forwarded, Err(SyncRefusal::Unavailable));
    }

    #[tokio::test]
    async fn pulls_follow_coordinator() {
        use crate::distribution::test_record::{binding, record, secret, store_record};
        let fixture = policy_fixture(None).await;
        let (snapshot, bytes) = record(true);
        store_record(&fixture.context.storage_handle, snapshot.set_id(), bytes).await;
        let mut bucket = bucket_info(fixture.group_id, fixture.auth.user_id);
        bucket.distribution = Some(binding(&snapshot, 1, 5));
        assert_eq!(coordinates(&fixture.context, &bucket, "a").await, Ok(()));
        assert_eq!(
            coordinates(&fixture.context, &bucket, "x").await,
            Err(SyncRefusal::Coordinator(secret(2).public())),
            "member 2 coordinates the upper range"
        );
        let ordinary = bucket_info(fixture.group_id, fixture.auth.user_id);
        assert_eq!(coordinates(&fixture.context, &ordinary, "x").await, Ok(()));
    }
}
