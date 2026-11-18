//! Reads objects through the GetObject operation: routed to the realm's holders, by token, by
//! key or as object info.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use futures_util::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::time::SystemTime;
use ulid::Ulid;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, NetEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{ABE_GRANT_KEYSPACE, OBJECT_METADATA_KEYSPACE};
use aruna_core::operation::{Operation, Runnable, boxed_suboperation};
use aruna_core::rows::read_row;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::abe::{AbeEffect, AbeError, EnvelopeArchive, ObjectEnvelope};
use aruna_core::structs::storage::abe_access::{KeyGrant, MAX_REQUESTS};
use aruna_core::structs::storage::blob::{ArchiveKey, BackendLocation, HiddenBlobKey};
use aruna_core::structs::storage::distribution::on_use::{ATTACHMENT_NAMESPACE, CACHE_NAMESPACE};
use aruna_core::structs::storage::encryption::{BucketKeyError, ReadLease, TokenCredential};
use aruna_core::structs::storage::multipart::{
    MultipartChecksumType, MultipartObjectKey, MultipartObjectSummary,
};
use aruna_core::types::{Key, Value};
use bytes::Bytes;
use tracing::warn;

use super::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult, ObjectInfo, locked,
};
use crate::abe::envelope::EnvelopeOperation;
use crate::blob::holders::{GetHoldersError, GetHoldersOperation};
use crate::blob::promote::{PromoteError, PromotePendingOperation, Promotion};
use crate::blob::unlocked::DropReceivedOperation;
use crate::distribution::held::{HeldElsewhere, PullRequest};
use crate::distribution::pull::{PullThroughOperation, Pulled, pinned_blob};
use crate::distribution::source_keep::{KeepFirstOperation, KeepStart};
use crate::distribution::source_plan::SourceKeep;
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::{LocalUserOperation, ManagedReadOperation};
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::lookup::{LookupError, summary_from_value};

type Turn<T> = Result<Next<T>, GetObjectError>;
type Read = Result<GetObjectResult, GetObjectError>;

/// Starts `operation` beside the caller while a background slot is free.
fn background<O, K>(operation: O) -> IoCall
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: Send + 'static,
    O::Error: Send + 'static,
{
    let child = boxed_suboperation(operation, |_| Event::Delayed);
    Effect::Background(child).into()
}

/// The token credential of one S3 request and the limits of a promotion it starts.
#[derive(Clone, Debug, PartialEq)]
pub struct TokenRead {
    pub credential: TokenCredential,
    pub limits: RoCrateLimits,
}

#[derive(Debug, PartialEq)]
enum RoutedStep {
    Local,
    Keep,
    Again,
    Pull,
    User(Box<RoutedRead>),
    Holders,
}

/// Reads an object, continuing at the realm's holders when a User node has the record but not
/// the bytes; a routed range streams the whole blob and skips to it. A token admits a read of a
/// locked bucket key, and a pending version is promoted first.
#[derive(Debug, PartialEq)]
pub struct GetRoutedOperation {
    input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
    token: Option<TokenRead>,
    /// The first read, while a member keeps the reference it names.
    held: Option<Read>,
    step: RoutedStep,
}

impl GetRoutedOperation {
    pub fn new(
        input: GetObjectInput,
        restrictions: Option<Vec<PathRestriction>>,
        token: Option<TokenRead>,
    ) -> Self {
        Self {
            input,
            restrictions,
            token,
            held: None,
            step: RoutedStep::Local,
        }
    }

    fn read_local(&self) -> IoCall {
        IoCall::child(ReadLocalOperation::new(
            self.input.clone(),
            self.restrictions.clone(),
            self.token.clone(),
        ))
    }

    fn call(&mut self, step: RoutedStep, call: IoCall) -> Turn<GetObjectResult> {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// A member of a distributed bucket receives references without a connector id; the one
    /// it read from the source may be kept, and a member that fills first serves from it.
    fn first_read(&mut self, result: Read) -> Turn<GetObjectResult> {
        let input = &self.input;
        let keep = match &result {
            Ok(read)
                if read.source_metadata.is_some()
                    && read.location.is_none()
                    && (read.source_binding.as_ref())
                        .is_some_and(|source| source.connector_id.is_none()) =>
            {
                (read.resolved_version_id.or(read.version_id)).map(|version_id| SourceKeep {
                    bucket: input.bucket.clone(),
                    key: input.key.clone(),
                    version_id,
                    group_id: input.group_id,
                    user_id: input.user_identity,
                    ranged: input.range.is_some(),
                })
            }
            _ => None,
        };
        match keep {
            Some(keep) => {
                self.held = Some(result);
                let keep = KeepReadOperation::new(keep);
                self.call(RoutedStep::Keep, IoCall::child(keep))
            }
            None => self.finish_read(result),
        }
    }

    fn finish_read(&mut self, result: Read) -> Turn<GetObjectResult> {
        let mut read = match result {
            Ok(read) => read,
            Err(GetObjectError::HeldElsewhere(held)) => {
                let request = pull_request(&self.input, self.restrictions.take(), *held);
                let pull = PullReadOperation::new(request);
                return self.call(RoutedStep::Pull, IoCall::child(pull));
            }
            Err(GetObjectError::BlobNotLocal {
                blake3,
                version_id,
                metadata,
                version_created_at,
                source_policies,
            }) => {
                let routed = RoutedRead {
                    user_id: self.input.user_identity,
                    realm_id: self.input.realm_id,
                    blake3,
                    version_id,
                    metadata,
                    version_created_at,
                    source_policies,
                    restrictions: self.restrictions.take(),
                };
                let user = LocalUserOperation::new(self.input.realm_id);
                return self.call(RoutedStep::User(Box::new(routed)), IoCall::child(user));
            }
            Err(error) => return Err(error),
        };
        if let Some(location) = (read.location.as_ref()).filter(|location| kept_copy(location)) {
            let empty = BackendStream(Box::pin(futures_util::stream::empty()));
            read.blob = pinned_blob(location, std::mem::replace(&mut read.blob, empty));
        }
        Ok(Next::Done(read))
    }

    fn routed(&mut self, found: HolderRead) -> Turn<GetObjectResult> {
        let HolderRead::Blob(result) = found else {
            return Err(GetObjectError::GetObjectFailed);
        };
        let mut result = *result;
        if let Some(range) = self.input.range.clone() {
            let resolved = range.resolve(result.info.size)?;
            result.blob = slice_blob(result.blob, resolved.range.clone());
            result.resolved_range = Some(resolved);
        }
        Ok(Next::Done(result))
    }
}

fn pull_request(
    input: &GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
    held: HeldElsewhere,
) -> PullRequest {
    PullRequest {
        bucket: input.bucket.clone(),
        key: input.key.clone(),
        range: input.range.clone(),
        auth: AuthContext {
            user_id: input.user_identity,
            realm_id: input.realm_id,
            path_restrictions: restrictions,
            session: None,
        },
        held,
    }
}

impl Operation for GetRoutedOperation {
    type Output = GetObjectResult;
    type Error = GetObjectError;

    fn first(&mut self) -> Turn<GetObjectResult> {
        Ok(Next::Call(self.read_local()))
    }

    fn next(&mut self, result: IoResult) -> Turn<GetObjectResult> {
        match std::mem::replace(&mut self.step, RoutedStep::Pull) {
            RoutedStep::Local => self.first_read(result.child()?),
            RoutedStep::Keep => {
                let kept = result.child::<bool, IoError>()?.unwrap_or(false);
                let earlier = self.held.take().ok_or(IoError::Unfinished)?;
                if !kept {
                    return self.finish_read(earlier);
                }
                let again = self.read_local();
                self.call(RoutedStep::Again, again)
            }
            RoutedStep::Again => self.finish_read(result.child()?),
            RoutedStep::Pull => Ok(Next::Done(
                result.child::<GetObjectResult, GetObjectError>()??,
            )),
            RoutedStep::User(read) => {
                if !result.child::<bool, IoError>()?.unwrap_or(false) {
                    return Err(GetObjectError::GetObjectFailed);
                }
                let holders = HolderReadOperation::new(*read, false);
                self.call(RoutedStep::Holders, IoCall::child(holders))
            }
            RoutedStep::Holders => {
                let found = result.child::<HolderRead, GetObjectError>()??;
                self.routed(found)
            }
        }
    }
}

/// Whether `location` is a copy a member keeps on use, which eviction may remove.
fn kept_copy(location: &BackendLocation) -> bool {
    HiddenBlobKey::try_from(location)
        .and_then(|key| key.namespace())
        .is_ok_and(|namespace| namespace == ATTACHMENT_NAMESPACE || namespace == CACHE_NAMESPACE)
}

/// The bytes of `range` from a stream of the whole object; the rest is skipped.
pub(super) fn slice_blob(
    blob: BackendStream<Result<Bytes, StreamError>>,
    range: Range<u64>,
) -> BackendStream<Result<Bytes, StreamError>> {
    let sliced = blob
        .0
        .scan(0u64, move |offset, chunk| {
            let start = *offset;
            let item = match chunk {
                Ok(_) if start >= range.end => return futures_util::future::ready(None),
                Ok(chunk) => {
                    let len = chunk.len() as u64;
                    *offset = start + len;
                    let from = range.start.saturating_sub(start).min(len) as usize;
                    let to = (range.end - start).min(len) as usize;
                    (from < to).then(|| Ok(chunk.slice(from..to)))
                }
                Err(error) => Some(Err(error)),
            };
            futures_util::future::ready(Some(item))
        })
        .filter_map(futures_util::future::ready);
    BackendStream(Box::pin(sliced))
}

/// Keeps a reference's bytes. Outputs `true` once they are kept before the read is served.
#[derive(Debug, PartialEq)]
struct KeepReadOperation {
    keep: Option<SourceKeep>,
    starting: bool,
}

impl KeepReadOperation {
    fn new(keep: SourceKeep) -> Self {
        Self {
            keep: Some(keep),
            starting: false,
        }
    }
}

impl Operation for KeepReadOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let keep = self.keep.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(KeepFirstOperation::new(keep))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.starting {
            result.event()?;
            return Ok(Next::Done(false));
        }
        match result.child::<KeepStart, IoError>()? {
            Ok(KeepStart::Settled(kept)) => Ok(Next::Done(kept)),
            Ok(KeepStart::Background(source)) => {
                self.starting = true;
                Ok(Next::Call(background(source)))
            }
            Err(_) => Ok(Next::Done(false)),
        }
    }
}

/// Keeps a reference's bytes; `true` once they are kept before the read is served.
async fn keep_first(context: &DriverContext, keep: SourceKeep) -> bool {
    match drive(KeepFirstOperation::new(keep), context).await {
        Ok(KeepStart::Settled(kept)) => kept,
        Ok(KeepStart::Background(source)) => {
            if let Some(slot) = fill_slot() {
                let context = context.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    let _ = drive(source, &context).await;
                });
            }
            false
        }
        Err(_) => false,
    }
}

/// Reads the local copy; with a token, a locked or pending version is read with the object key
/// that a grant of the credential opens.
pub(crate) async fn read_local(
    context: &DriverContext,
    input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
    token: Option<&TokenRead>,
) -> Result<GetObjectResult, GetObjectError> {
    let operation = GetObjectOperation::new(input.clone())
        .with_restrictions(restrictions.clone())
        .with_token(token.is_some());
    let result = drive(operation, context).await;
    let Some(token) = token else {
        return result;
    };
    match result {
        Err(GetObjectError::PendingContent(_)) => {}
        Err(GetObjectError::ConversionError(ConversionError::BucketKey(
            BucketKeyError::Locked(_),
        ))) => {}
        result => return result,
    }
    let (input, private) = token_object(context, input, &token.credential).await?;
    read_object(context, input, restrictions, private, &token.limits).await
}

/// Reads a version with the object key of a request header; without a version id, the
/// current version is pinned first.
pub async fn get_object_keyed(
    context: &DriverContext,
    mut input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
    private: SharedSecret,
    limits: &RoCrateLimits,
) -> Result<GetObjectResult, GetObjectError> {
    pin_version(context, &mut input).await?;
    read_object(context, input, restrictions, private, limits).await
}

/// Sets the version id of `input` to the current version when none was named.
async fn pin_version(
    context: &DriverContext,
    input: &mut GetObjectInput,
) -> Result<Ulid, GetObjectError> {
    if let Some(version) = input.version_id {
        return Ok(version);
    }
    let head = HeadObjectOperation::new(HeadObjectInput {
        bucket: input.bucket.clone(),
        key: input.key.clone(),
        version_id: None,
    });
    let head = drive(head, context)
        .await
        .map_err(|_| GetObjectError::GetObjectFailed)?;
    let version = head
        .resolved_version_id
        .or(head.version_id)
        .ok_or(GetObjectError::NoSuchKey)?;
    input.version_id = Some(version);
    Ok(version)
}

/// Pins the version and opens its object key with a grant of `credential` that covers it.
async fn token_object(
    context: &DriverContext,
    mut input: GetObjectInput,
    credential: &TokenCredential,
) -> Result<(GetObjectInput, SharedSecret), GetObjectError> {
    let version = pin_version(context, &mut input).await?;
    let read = EnvelopeOperation::new(input.bucket.clone(), input.key.clone(), version);
    let (envelope, _) = drive(read, context).await?;
    let key = envelope.context.parameters.key;
    let user = input.user_identity.to_storage_key();
    let scan = StorageEffect::Iter {
        key_space: ABE_GRANT_KEYSPACE.to_string(),
        prefix: Some([&key.bucket_id.to_bytes()[..], &user].concat().into()),
        start: None,
        limit: MAX_REQUESTS + 1,
        txn_id: None,
    };
    let values = match context.storage_handle.send_storage_effect(scan).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(GetObjectError::GetObjectFailed),
    };
    let mut error = locked(BucketKeyError::Locked(key.bucket_id));
    for (_, value) in values {
        let Ok(grant) = KeyGrant::from_bytes(&value) else {
            continue;
        };
        let request = &grant.context.request;
        if request.credential_id.as_deref() != Some(credential.access_key.as_str())
            || request.parameters != envelope.context.parameters
            || !request.epochs.contains(&envelope.context.epoch)
            || !request.covers(&input.key)
        {
            continue;
        }
        match grant.open_object(credential.token.bytes(), &envelope) {
            Ok(private) => return Ok((input, private)),
            Err(AbeError::WrongKey) => error = locked(BucketKeyError::InvalidToken),
            Err(other) => error = other.into(),
        }
    }
    Err(error)
}

/// Reads a pinned version with an object key; a pending version is promoted under that key once.
pub async fn read_object(
    context: &DriverContext,
    input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
    private: SharedSecret,
    limits: &RoCrateLimits,
) -> Result<GetObjectResult, GetObjectError> {
    let version = input.version_id.ok_or(GetObjectError::NoSuchVersion)?;
    let mut promoted = false;
    loop {
        let read = EnvelopeOperation::new(input.bucket.clone(), input.key.clone(), version);
        let (envelope, mapping) = drive(read, context).await?;
        let operation = GetObjectOperation::new(input.clone())
            .with_restrictions(restrictions.clone())
            .with_object(envelope.clone(), mapping.clone(), private.clone());
        let archive = match drive(operation, context).await {
            Err(GetObjectError::PendingContent(archive))
                if !promoted && archive == mapping.archive =>
            {
                archive
            }
            result => return result,
        };
        promoted = true;
        let blob = context
            .blob_handle
            .as_ref()
            .ok_or(GetObjectError::GetObjectFailed)?;
        let admit = AbeEffect::Admit {
            envelope,
            archive: archive.clone(),
            private: private.clone(),
        };
        let lease = match blob
            .send_blob_effect(BlobEffect::Abe(Box::new(admit)))
            .await
        {
            Event::Blob(BlobEvent::ReadAdmitted { lease }) => lease,
            Event::Blob(BlobEvent::Error(BlobError::Abe(error))) => return Err(error.into()),
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(error))) => {
                return Err(locked(error));
            }
            _ => return Err(GetObjectError::GetObjectFailed),
        };
        promote_leased(context, &input, archive, lease, limits).await?;
    }
}

/// Promotes the pending `archive` under `lease`, as an unlock would.
pub(super) async fn promote_leased(
    context: &DriverContext,
    input: &GetObjectInput,
    archive: ArchiveKey,
    lease: ReadLease,
    limits: &RoCrateLimits,
) -> Result<(), GetObjectError> {
    let (realm_id, node_id) = (input.realm_id, input.node_id);
    let promote = PromotePendingOperation::new(archive.clone(), realm_id, node_id, limits.clone());
    let outcome = match drive(promote.with_lease(lease), context).await {
        Err(PromoteError::Blob(BlobError::IntegrityCheckFailed(reason))) => {
            let drop = DropReceivedOperation::rejected(archive, (realm_id, node_id), reason);
            drive(drop, context)
                .await
                .map_err(|_| GetObjectError::GetObjectFailed)?;
            return Err(GetObjectError::GetObjectFailed);
        }
        result => result.map_err(|error| {
            warn!(error = %error, "Token promotion of a pending version failed");
            GetObjectError::GetObjectFailed
        })?,
    };
    match outcome {
        Promotion::Promoted { .. } | Promotion::Gone => Ok(()),
        Promotion::AwaitingKey(_) => Err(GetObjectError::GetObjectFailed),
        Promotion::Mismatch { claimed } => {
            let drop = DropReceivedOperation::mismatched(archive, claimed, (realm_id, node_id));
            drive(drop, context)
                .await
                .map_err(|_| GetObjectError::GetObjectFailed)?;
            Err(GetObjectError::GetObjectFailed)
        }
    }
}

/// Resolves complete object facts without transferring holder bytes.
pub async fn get_object_info(
    context: &DriverContext,
    input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
) -> Result<ObjectInfo, GetObjectError> {
    if input.range.is_some() {
        return Err(GetObjectError::InvalidRange);
    }
    let (user_id, realm_id) = (input.user_identity, input.realm_id);
    let operation = GetObjectOperation::new(input).with_restrictions(restrictions.clone());
    match drive(operation, context).await {
        Ok(result) => Ok(result.info),
        Err(GetObjectError::HeldElsewhere(held)) => Ok(held.info()),
        Err(GetObjectError::BlobNotLocal {
            blake3,
            version_id,
            metadata,
            version_created_at,
            source_policies,
        }) if drive(LocalUserOperation::new(realm_id), context)
            .await
            .unwrap_or(false) =>
        {
            let read = RoutedRead {
                user_id,
                realm_id,
                blake3,
                version_id,
                metadata,
                version_created_at,
                source_policies,
                restrictions,
            };
            routed_metadata(context, read).await
        }
        Err(error) => Err(error),
    }
}

/// One holder-backed read: the local version facts plus the caller's scoped
/// credential, so the routed result is never less constrained than the record.
pub(super) struct RoutedRead {
    pub(super) user_id: UserId,
    pub(super) realm_id: RealmId,
    pub(super) blake3: [u8; 32],
    pub(super) version_id: Option<Ulid>,
    pub(super) metadata: HashMap<String, String>,
    pub(super) version_created_at: Option<SystemTime>,
    pub(super) source_policies: Vec<PlacementPolicyRef>,
    pub(super) restrictions: Option<Vec<PathRestriction>>,
}

/// How the consulted holders failed, folded so the final answer keeps the most
/// informative cause. A read where no holder served is absence only when every
/// holder confirmed absence.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct HolderFailures {
    /// Whether at least one holder answered at all, so zero holders stay
    /// distinguishable from every holder confirming absence.
    pub(super) contacted: bool,
    governed: bool,
    denied: bool,
    integrity: bool,
    unavailable: bool,
    pub(super) metadata_only: bool,
    /// A holder refused because its copy's bucket is locked there.
    locked: Option<Ulid>,
}

impl HolderFailures {
    pub(super) fn record(&mut self, error: BaoReadError) {
        self.contacted = true;
        match error {
            BaoReadError::Refused(BaoReadRefusal::NotFound) => {}
            BaoReadError::Refused(BaoReadRefusal::ReadDenied) => self.denied = true,
            BaoReadError::Refused(BaoReadRefusal::HashMismatch) => self.integrity = true,
            BaoReadError::Refused(BaoReadRefusal::BucketLocked(bucket_id)) => {
                self.locked = Some(bucket_id);
            }
            // A member keeping only the record proves no absence: the bytes are elsewhere.
            BaoReadError::Refused(
                BaoReadRefusal::BackendFailure
                | BaoReadRefusal::RealmPeerDenied
                | BaoReadRefusal::InvalidTarget
                | BaoReadRefusal::HeldBy { .. },
            ) => self.unavailable = true,
            BaoReadError::GovernedUnavailable
            | BaoReadError::PolicyRequired { .. }
            | BaoReadError::PolicyDenied { .. }
            | BaoReadError::Gate(_)
            | BaoReadError::NoDestination => self.governed = true,
            BaoReadError::Blob(_)
            | BaoReadError::Conversion(_)
            | BaoReadError::Unexpected { .. }
            | BaoReadError::NotFinished
            | BaoReadError::Io(_)
            | BaoReadError::ManagedCopy(_) => self.unavailable = true,
        }
    }

    pub(crate) fn into_error(self) -> GetObjectError {
        if let Some(bucket_id) = self.locked {
            return locked(BucketKeyError::Locked(bucket_id));
        }
        if self.governed {
            GetObjectError::GovernedUnavailable
        } else if self.integrity {
            GetObjectError::HolderIntegrityFailure
        } else if self.denied {
            GetObjectError::HolderAccessDenied
        } else if self.unavailable || self.metadata_only || !self.contacted {
            GetObjectError::HoldersUnavailable
        } else {
            GetObjectError::NoSuchKey
        }
    }

    pub(crate) fn consider(
        &mut self,
        result: Result<BaoReadOutput, BaoReadError>,
    ) -> Option<BaoReadOutput> {
        match result {
            Ok(output @ BaoReadOutput::Stream { .. }) => Some(output),
            Ok(BaoReadOutput::Metadata { .. }) => {
                self.contacted = true;
                self.metadata_only = true;
                None
            }
            Err(error) => {
                self.record(error);
                None
            }
        }
    }
}

async fn routed_blob(
    context: &DriverContext,
    read: RoutedRead,
) -> Result<GetObjectResult, GetObjectError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(GetObjectError::GetObjectFailed)?;
    let realm_id = read.realm_id;
    let holders = drive(
        GetHoldersOperation::new(read.blake3, realm_id, net_handle.node_id()),
        context,
    )
    .await
    .map_err(|_| GetObjectError::GetObjectFailed)?;
    let summary = local_multipart_summary(context, read.version_id).await?;

    let mut failures = HolderFailures::default();
    for holder in holders {
        let request = BaoReadRequest {
            auth_context: AuthContext {
                user_id: read.user_id,
                realm_id,
                path_restrictions: read.restrictions.clone(),
                session: None,
            },
            realm_id,
            target: BaoReadTarget::Blake3(read.blake3),
            expected_blake3: Some(read.blake3),
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        match failures.consider(drive(ManagedReadOperation::new(holder, request), context).await) {
            Some(BaoReadOutput::Stream {
                blob,
                size,
                etag,
                hashes,
                ..
            }) => {
                return Ok(routed_result(read, blob, size, etag, hashes, summary));
            }
            None => {}
            Some(BaoReadOutput::Metadata { .. }) => unreachable!("metadata does not serve bytes"),
        }
    }
    Err(failures.into_error())
}

async fn routed_metadata(
    context: &DriverContext,
    read: RoutedRead,
) -> Result<ObjectInfo, GetObjectError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(GetObjectError::GetObjectFailed)?;
    let realm_id = read.realm_id;
    let holders = drive(
        GetHoldersOperation::new(read.blake3, realm_id, net_handle.node_id()),
        context,
    )
    .await
    .map_err(|_| GetObjectError::GetObjectFailed)?;
    let summary = local_multipart_summary(context, read.version_id).await?;
    let mut failures = HolderFailures::default();

    for holder in holders {
        let request = BaoReadRequest {
            auth_context: AuthContext {
                user_id: read.user_id,
                realm_id,
                path_restrictions: read.restrictions.clone(),
                session: None,
            },
            realm_id,
            target: BaoReadTarget::Blake3(read.blake3),
            expected_blake3: Some(read.blake3),
            metadata_only: true,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        match drive(ManagedReadOperation::new(holder, request), context).await {
            Ok(BaoReadOutput::Metadata {
                size, etag, hashes, ..
            }) => {
                return Ok(routed_info(&read, size, etag, hashes, summary));
            }
            Ok(BaoReadOutput::Stream { .. }) => failures.unavailable = true,
            Err(error) => failures.record(error),
        }
    }
    Err(failures.into_error())
}

/// Holder hashes describe the bytes; the local version describes logical multipart facts.
fn routed_result(
    read: RoutedRead,
    blob: BackendStream<Result<Bytes, StreamError>>,
    size: u64,
    etag: Option<String>,
    hashes: HashMap<String, Vec<u8>>,
    summary: Option<MultipartObjectSummary>,
) -> GetObjectResult {
    let info = routed_info(&read, size, etag, hashes, summary);
    GetObjectResult {
        blob,
        location: None,
        metadata: read.metadata,
        info,
        source_metadata: None,
        source_binding: None,
        last_refresh: None,
        version_id: read.version_id,
        resolved_version_id: read.version_id,
        resolved_range: None,
        source_policies: read.source_policies,
        served_source: None,
    }
}

pub(super) fn routed_info(
    read: &RoutedRead,
    size: u64,
    etag: Option<String>,
    hashes: HashMap<String, Vec<u8>>,
    summary: Option<MultipartObjectSummary>,
) -> ObjectInfo {
    let (checksum_type, composite_hashes, part_count) = match summary {
        Some(summary) => (
            summary.checksum_type,
            summary.composite_hashes,
            Some(summary.part_count),
        ),
        None => (MultipartChecksumType::FullObject, HashMap::new(), None),
    };
    ObjectInfo {
        size,
        version_created_at: read.version_created_at,
        etag,
        checksum_type,
        hashes,
        composite_hashes,
        part_count,
    }
}

/// The local version owns multipart facts even when another holder supplies bytes.
async fn local_multipart_summary(
    context: &DriverContext,
    version_id: Option<Ulid>,
) -> Result<Option<MultipartObjectSummary>, GetObjectError> {
    let Some(version_id) = version_id else {
        return Ok(None);
    };
    let key = MultipartObjectKey::summary(version_id)
        .to_bytes()
        .map_err(GetObjectError::ConversionError)?;
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: OBJECT_METADATA_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        Event::Storage(StorageEvent::ReadResult { value, .. }) => summary_from_value(value)
            .map_err(|error| match error {
                LookupError::Conversion(error) => GetObjectError::ConversionError(error),
                _ => GetObjectError::GetObjectFailed,
            }),
        _ => Err(GetObjectError::GetObjectFailed),
    }
}
