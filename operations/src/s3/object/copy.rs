//! Copies an object to another key, checking the source conditions and handling references.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod sealed;

use crate::distribution::held::record_only;
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::fence_store::FenceCheckOperation;
use crate::s3::object::copy::sealed::{
    SealedCopyError, SealedCopyInput, SealedCopyOperation, SealedCopyResult,
};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectResult, reference_archive};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use crate::s3::object::read::{ReadSourceOperation, TokenRead};
use crate::s3::purge_fence::PurgeFenceError;
use crate::staging::reference::{
    MaterializeReferenceError, ReferenceWrite, WriteReferenceOperation,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BackendLocation, BucketInfo};
use aruna_core::structs::storage::bucket_metadata::strip_release_keys;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{OriginVersion, VersionOrigin};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot, resolve_backend};
use aruna_core::types::GroupId;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CopySourceConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<SystemTime>,
    pub if_unmodified_since: Option<SystemTime>,
}

/// What a copy does with a source whose bytes sit behind a reference.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CopyReferences {
    /// The copy is a reference of its own; no byte moves and the connector is
    /// not contacted.
    #[default]
    Preserve,
    /// The bytes are pulled and stored as a snapshot.
    Materialize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyObjectInput {
    pub source_bucket: String,
    pub source_key: String,
    pub source_version_id: Option<Ulid>,
    pub source_group_id: GroupId,
    pub source_auth_context: AuthContext,
    pub dest_bucket: String,
    pub dest_key: String,
    pub user_id: UserId,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub quota_ceiling: Option<u64>,
    pub conditions: CopySourceConditions,
    pub metadata: Option<HashMap<String, String>>,
    pub restrictions: Option<Vec<PathRestriction>>,
    pub references: CopyReferences,
    /// The system job that runs the copy, recorded in the copy's origin.
    pub job_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct CopyResultData {
    /// The stored bytes; `None` for a copy that stayed a reference.
    pub location: Option<BackendLocation>,
    pub size: u64,
    /// The observation a kept reference carries.
    pub source_metadata: Option<SourceMetadata>,
    pub version_id: Ulid,
    pub created_at: SystemTime,
    pub source_version_id: Option<Ulid>,
    pub source_last_modified: Option<SystemTime>,
}

#[derive(Debug, Error, PartialEq)]
pub enum CopyObjectError {
    #[error(transparent)]
    Get(#[from] GetObjectError),
    #[error(transparent)]
    Put(#[from] PutObjectError),
    #[error(transparent)]
    Routing(#[from] RoutingInputsError),
    #[error(transparent)]
    Gate(#[from] GateContextError),
    #[error(transparent)]
    Reference(#[from] MaterializeReferenceError),
    #[error("At least one of the preconditions you specified did not hold.")]
    PreconditionFailed,
}

impl From<IoError> for CopyObjectError {
    fn from(error: IoError) -> Self {
        Self::Get(error.into())
    }
}

type CopyTurn<T> = Result<Next<T>, CopyObjectError>;

/// A sealed copy fails like the write it stands in for, or like a missing source.
fn sealed_error(error: SealedCopyError) -> CopyObjectError {
    let put = |error| CopyObjectError::Put(error);
    match error {
        SealedCopyError::Storage(error) => put(PutObjectError::StorageError(error)),
        SealedCopyError::Conversion(error) => put(PutObjectError::ConversionError(error)),
        SealedCopyError::PurgeFence(error) => put(PutObjectError::PurgeFence(error)),
        SealedCopyError::Quota(error) => put(PutObjectError::QuotaGateError(error)),
        SealedCopyError::QuotaExceeded { limit, usage } => {
            put(PutObjectError::QuotaExceeded { limit, usage })
        }
        SealedCopyError::UsageRebuilding => put(PutObjectError::UsageRebuilding),
        SealedCopyError::PolicyGate(error) => put(PutObjectError::PolicyGate(error)),
        SealedCopyError::Distribution(error) => put(PutObjectError::Distribution(error)),
        SealedCopyError::Usage(error) => put(PutObjectError::UsageUpdateError(error)),
        SealedCopyError::NoSuchVersion => CopyObjectError::Get(GetObjectError::NoSuchVersion),
        SealedCopyError::Blob(error) => put(PutObjectError::BlobWriteFailed(error)),
        error => put(PutObjectError::WriteFailed(error.to_string())),
    }
}

/// A description failure reads like the read it stands in for.
fn head_error(error: HeadObjectError) -> CopyObjectError {
    CopyObjectError::Get(match error {
        HeadObjectError::StorageError(error) => GetObjectError::StorageError(error),
        HeadObjectError::ConversionError(error) => GetObjectError::ConversionError(error),
        HeadObjectError::NoSuchKey => GetObjectError::NoSuchKey,
        HeadObjectError::NoSuchVersion => GetObjectError::NoSuchVersion,
        HeadObjectError::DeleteMarker => GetObjectError::DeleteMarker,
        HeadObjectError::ResolveReferenceError(error) => {
            GetObjectError::ResolveReferenceError(error)
        }
        HeadObjectError::StagingSourceError(error) => GetObjectError::StagingSourceError(error),
        HeadObjectError::ManagedCopyError(error) => GetObjectError::ManagedCopyError(error),
        _ => GetObjectError::GetObjectFailed,
    })
}

fn normalize_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}

fn etag_matches(expected: &str, actual: Option<&str>, source_exists: bool) -> bool {
    if expected == "*" {
        return source_exists;
    }
    actual.is_some_and(|actual| normalize_etag(actual) == normalize_etag(expected))
}

pub(crate) fn evaluate_source_conditions(
    conditions: &CopySourceConditions,
    etag: Option<&str>,
    last_modified: Option<SystemTime>,
    source_exists: bool,
) -> Result<(), CopyObjectError> {
    let last_modified = last_modified.map(|last_modified| {
        last_modified
            .duration_since(UNIX_EPOCH)
            .map(|duration| UNIX_EPOCH + Duration::from_secs(duration.as_secs()))
            .unwrap_or(last_modified)
    });

    // AWS precedence: x-amz-copy-source-if-match overrides if-unmodified-since.
    match conditions.if_match.as_deref() {
        Some(expected) => {
            if !etag_matches(expected, etag, source_exists) {
                return Err(CopyObjectError::PreconditionFailed);
            }
        }
        None => {
            if let Some(threshold) = conditions.if_unmodified_since
                && last_modified.is_some_and(|last_modified| last_modified > threshold)
            {
                return Err(CopyObjectError::PreconditionFailed);
            }
        }
    }

    // AWS precedence: x-amz-copy-source-if-none-match overrides if-modified-since.
    match conditions.if_none_match.as_deref() {
        Some(expected) => {
            if etag_matches(expected, etag, source_exists) {
                return Err(CopyObjectError::PreconditionFailed);
            }
        }
        None => {
            if let Some(threshold) = conditions.if_modified_since
                && last_modified.is_some_and(|last_modified| last_modified <= threshold)
            {
                return Err(CopyObjectError::PreconditionFailed);
            }
        }
    }

    Ok(())
}

/// The origin of a copy of `version_id`: its global id in a distributed bucket, else its ARN.
pub fn source_origin(
    (realm_id, node_id): (RealmId, NodeId),
    (bucket, key): (&str, &str),
    (version_id, job_id): (Ulid, Option<Ulid>),
    distributed: bool,
) -> Option<VersionOrigin> {
    let from = match distributed {
        true => OriginVersion::Global(GlobalId::Version {
            realm_id,
            version_id,
        }),
        false => OriginVersion::Node(
            VersionedObjectArn::new(realm_id, node_id, bucket, key, version_id).ok()?,
        ),
    };
    Some(VersionOrigin::Copy { from, job_id })
}

/// When a source was last modified, as far as its version record or observation tells.
pub(crate) fn source_modified(
    created_at: Option<SystemTime>,
    location: Option<&BackendLocation>,
    metadata: Option<&SourceMetadata>,
) -> Option<SystemTime> {
    created_at
        .or_else(|| location.map(|location| location.created_at))
        .or_else(|| metadata.and_then(|metadata| metadata.last_modified))
}

pub(crate) fn source_etag(
    location: Option<&BackendLocation>,
    metadata: Option<&SourceMetadata>,
) -> Option<String> {
    (location.and_then(|location| location.hashes.get(HASH_MD5)))
        .map(hex::encode)
        .or_else(|| metadata.and_then(|metadata| metadata.etag.clone()))
}

fn version_time(version_id: Ulid) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(version_id.timestamp_ms())
}

/// The bytes a copy pulled so far, shared with whoever reports them.
#[derive(Clone, Debug)]
struct PulledBytes(Arc<AtomicU64>);

impl PartialEq for PulledBytes {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The version a copy writes and the clock it writes under.
#[derive(Clone, Copy, Debug, PartialEq)]
struct CopyStamp {
    version: Option<Ulid>,
    fresh: Ulid,
    now_ms: u64,
}

/// Reads a copy source a member streams to the destination's coordinator; the conditions are
/// checked before any byte moves.
#[derive(Debug, PartialEq)]
pub struct ReadCopyOperation {
    read: Option<ReadSourceOperation>,
    conditions: CopySourceConditions,
}

impl ReadCopyOperation {
    pub fn new(
        input: GetObjectInput,
        restrictions: Option<Vec<PathRestriction>>,
        conditions: &CopySourceConditions,
        token: Option<TokenRead>,
    ) -> Self {
        Self {
            read: Some(ReadSourceOperation::new(input, restrictions, token)),
            conditions: conditions.clone(),
        }
    }
}

impl Operation for ReadCopyOperation {
    type Output = GetObjectResult;
    type Error = CopyObjectError;

    fn first(&mut self) -> CopyTurn<GetObjectResult> {
        let read = self.read.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<GetObjectResult> {
        let source = result.child::<GetObjectResult, GetObjectError>()??;
        let (location, metadata) = (source.location.as_ref(), source.source_metadata.as_ref());
        let modified = source_modified(source.info.version_created_at, location, metadata);
        let etag = source_etag(location, metadata);
        evaluate_source_conditions(&self.conditions, etag.as_deref(), modified, true)?;
        Ok(Next::Done(source))
    }
}

#[derive(Debug, PartialEq)]
enum CopyStep {
    Committed,
    Fence,
    Head,
    Finish,
}

/// Copies an object to another key. The source's description decides how: a reference stays
/// one, a sealed source in the same bucket is aliased, anything else moves its bytes.
#[derive(Debug, PartialEq)]
pub struct CopyObjectOperation {
    input: Option<CopyObjectInput>,
    routing: NodeRouting,
    progress: Option<PulledBytes>,
    token: Option<TokenRead>,
    stamp: CopyStamp,
    step: CopyStep,
}

impl CopyObjectOperation {
    pub fn new(input: CopyObjectInput, routing: NodeRouting) -> Self {
        Self {
            input: Some(input),
            routing,
            progress: None,
            token: None,
            stamp: CopyStamp {
                version: None,
                fresh: Ulid::generate(),
                now_ms: 0,
            },
            step: CopyStep::Fence,
        }
    }

    /// Publishes the bytes pulled so far, so a job can report a long source read.
    pub fn with_progress(mut self, pulled: Arc<AtomicU64>) -> Self {
        self.progress = Some(PulledBytes(pulled));
        self
    }

    /// Writes `version_id`, minted once by the caller, so a retry after a lost answer replays
    /// that version instead of writing another one.
    pub fn at_version(mut self, version_id: Ulid) -> Self {
        self.stamp.version = Some(version_id);
        self
    }

    /// Lets `token` admit the source read while its bucket key is locked.
    pub fn with_token(mut self, token: Option<TokenRead>) -> Self {
        self.token = token;
        self
    }

    fn input(&self) -> Result<&CopyObjectInput, IoError> {
        self.input.as_ref().ok_or(IoError::Unfinished)
    }

    fn check_fence(&mut self) -> CopyTurn<CopyResultData> {
        let input = self.input()?;
        let fence = FenceCheckOperation::new(&input.dest_bucket, &input.dest_key);
        self.step = CopyStep::Fence;
        Ok(Next::Call(IoCall::child(fence)))
    }

    /// A given version that already exists was written by an earlier attempt whose answer was
    /// lost; it is answered without the source, which may be gone by now.
    fn committed(
        &mut self,
        version_id: Ulid,
        head: Result<HeadObjectResult, HeadObjectError>,
    ) -> CopyTurn<CopyResultData> {
        let head = match head {
            Ok(head) => head,
            Err(HeadObjectError::NoSuchKey | HeadObjectError::NoSuchVersion) => {
                return self.check_fence();
            }
            Err(error) => return Err(head_error(error)),
        };
        let size = (head.location.as_ref().map(|location| location.blob_size))
            .or_else(|| (head.source_metadata.as_ref()).map(|metadata| metadata.content_length))
            .unwrap_or_default();
        Ok(Next::Done(CopyResultData {
            location: head.location,
            size,
            source_metadata: head.source_metadata,
            version_id,
            created_at: version_time(version_id),
            source_version_id: None,
            source_last_modified: None,
        }))
    }

    /// The description touches no source, so a reference is kept without contacting its
    /// connector.
    fn described(&mut self, head: HeadObjectResult) -> CopyTurn<CopyResultData> {
        let input = self.input.take().ok_or(IoError::Unfinished)?;
        let (location, metadata) = (head.location.as_ref(), head.source_metadata.as_ref());
        let modified = source_modified(head.version_created_at, location, metadata);
        let etag = source_etag(location, metadata);
        evaluate_source_conditions(&input.conditions, etag.as_deref(), modified, true)?;
        let stamp = self.stamp;
        self.step = CopyStep::Finish;
        // A record-only version of a distributed bucket is read through a holder, never kept.
        let record_only = (head.source_binding.as_ref()).is_some_and(record_only);
        if head.location.is_none() && input.references == CopyReferences::Preserve && !record_only {
            let keep = KeepReferenceOperation::new(input, head, modified, stamp);
            return Ok(Next::Call(IoCall::child(keep)));
        }
        // A sealed source in the same bucket is aliased, never read, so it copies while locked.
        // Its version cannot be given, so a copy with a given version reads the source instead.
        let sealed = head.location.clone().filter(|location| {
            location.format.bucket_key().is_some()
                && input.source_bucket == input.dest_bucket
                && stamp.version.is_none()
        });
        if let Some(location) = sealed {
            let alias = AliasSealedOperation::new(input, head, location, modified, stamp.now_ms)?;
            return Ok(Next::Call(IoCall::child(alias)));
        }
        let bytes = CopyBytesOperation {
            input,
            routing: self.routing.clone(),
            progress: self.progress.take(),
            token: self.token.take(),
            modified,
            stamp,
            source: None,
            snapshot: None,
            gate: None,
            source_version: None,
            step: BytesStep::Source,
        };
        Ok(Next::Call(IoCall::child(bytes)))
    }
}

impl Operation for CopyObjectOperation {
    type Output = CopyResultData;
    type Error = CopyObjectError;

    fn stamp(&mut self, now_ms: u64) {
        self.stamp.now_ms = now_ms;
    }

    fn first(&mut self) -> CopyTurn<CopyResultData> {
        let Some(version_id) = self.stamp.version else {
            return self.check_fence();
        };
        let input = self.input()?;
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: input.dest_bucket.clone(),
            key: input.dest_key.clone(),
            version_id: Some(version_id),
        });
        self.step = CopyStep::Committed;
        Ok(Next::Call(IoCall::child(head)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<CopyResultData> {
        match self.step {
            CopyStep::Committed => {
                let version_id = self.stamp.version.ok_or(IoError::Unfinished)?;
                self.committed(version_id, result.child()?)
            }
            CopyStep::Fence => {
                (result.child::<(), PurgeFenceError>()?)
                    .map_err(|error| CopyObjectError::Put(PutObjectError::PurgeFence(error)))?;
                let input = self.input()?;
                let head = HeadObjectOperation::new(HeadObjectInput {
                    bucket: input.source_bucket.clone(),
                    key: input.source_key.clone(),
                    version_id: input.source_version_id,
                });
                self.step = CopyStep::Head;
                Ok(Next::Call(IoCall::child(head)))
            }
            CopyStep::Head => {
                let head = result.child::<HeadObjectResult, HeadObjectError>()?;
                self.described(head.map_err(head_error)?)
            }
            CopyStep::Finish => Ok(Next::Done(
                result.child::<CopyResultData, CopyObjectError>()??,
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
enum BytesStep {
    Source,
    Routing,
    Gate,
    Origin,
    Put,
}

/// Reads the source and writes its bytes at the destination, or adopts them where the
/// destination's backend already holds them.
#[derive(Debug, PartialEq)]
struct CopyBytesOperation {
    input: CopyObjectInput,
    routing: NodeRouting,
    progress: Option<PulledBytes>,
    token: Option<TokenRead>,
    modified: Option<SystemTime>,
    stamp: CopyStamp,
    source: Option<GetObjectResult>,
    snapshot: Option<RoutingSnapshot>,
    gate: Option<GateContext>,
    source_version: Option<Ulid>,
    step: BytesStep,
}

impl CopyBytesOperation {
    /// A copy's origin names its exact source version, so the source bucket is read for it.
    fn read_origin(&mut self) -> CopyTurn<CopyResultData> {
        if self.source_version.is_none() {
            return self.write(None);
        }
        let bucket = GetBucketOperation::new(self.input.source_bucket.clone());
        self.step = BytesStep::Origin;
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn origin(&self, distributed: bool) -> Option<VersionOrigin> {
        let input = &self.input;
        source_origin(
            (input.realm_id, input.node_id),
            (&input.source_bucket, &input.source_key),
            (self.source_version?, input.job_id),
            distributed,
        )
    }

    fn write(&mut self, origin: Option<VersionOrigin>) -> CopyTurn<CopyResultData> {
        let source = self.source.take().ok_or(IoError::Unfinished)?;
        let routing = self.snapshot.take().ok_or(IoError::Unfinished)?;
        let input = &mut self.input;
        let content_length = source.location.as_ref().map(|location| location.blob_size);
        let version_source = match source.location.is_some() {
            true => source.source_binding.clone(),
            false => (source.source_binding.clone()).map(|binding| VersionSourceBinding {
                strategy: StagingStrategy::Snapshot,
                ..binding
            }),
        };
        let mut metadata = input.metadata.take().unwrap_or(source.metadata);
        // A copy is never a release, even of a release crate.
        strip_release_keys(&mut metadata);
        // Bytes the destination's backend already holds in the destination's encoding are
        // adopted; another encoding gets its own copy. A key under a backend import folder
        // streams, so its file appears at the folder's path.
        let (bucket, key) = (&input.dest_bucket, &input.dest_key);
        let plain = routing.plain_target(bucket, key, Ulid::nil()).is_some();
        let adopt = source.location.clone().filter(|location| {
            !plain
                && resolve_backend(&routing, bucket, key).is_ok_and(|resolved| {
                    resolved.backend == location.backend
                        && EncodingClass::from(resolved.compression) == location.format.encoding()
                })
        });
        let body = match (adopt.is_some(), self.progress.take()) {
            (true, _) => None,
            (false, Some(PulledBytes(pulled))) => {
                Some(BackendStream(Box::pin(source.blob.inspect(move |chunk| {
                    if let Ok(bytes) = chunk {
                        pulled.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    }
                }))))
            }
            (false, None) => Some(source.blob),
        };
        // Materialized source bytes are kept only when they match every recorded digest.
        let expected_checksums = (source.source_metadata.as_ref())
            .map(SourceMetadata::enforced_checksums)
            .unwrap_or_default();
        let mut put = PutObjectOperation::new(PutObjectConfig {
            user_id: input.user_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            request: PutObjectInput {
                bucket: input.dest_bucket.clone(),
                key: input.dest_key.clone(),
                content_length,
                body,
            },
            expected_checksums,
            checksum_type: None,
            exists: false,
            version_source,
            preassigned_version_id: self.stamp.version,
            quota_ceiling: input.quota_ceiling,
            routing,
        })
        .without_envelope()
        .with_metadata(metadata)
        .with_version_origin(origin)
        .with_inherited_policies(source.source_policies)
        .with_restrictions(input.restrictions.clone());
        if let Some(location) = adopt {
            put = put.with_adopted(location);
        }
        if let Some(gate) = self.gate.take() {
            put = put.with_gate(gate);
        }
        self.step = BytesStep::Put;
        Ok(Next::Call(IoCall::child(put)))
    }
}

impl Operation for CopyBytesOperation {
    type Output = CopyResultData;
    type Error = CopyObjectError;

    fn first(&mut self) -> CopyTurn<CopyResultData> {
        let input = &self.input;
        let source = GetObjectInput {
            bucket: input.source_bucket.clone(),
            key: input.source_key.clone(),
            version_id: input.source_version_id,
            range: None,
            group_id: input.source_group_id,
            user_identity: input.source_auth_context.user_id,
            realm_id: input.source_auth_context.realm_id,
            node_id: input.node_id,
        };
        let restrictions = input.source_auth_context.path_restrictions.clone();
        let read = ReadSourceOperation::new(source, restrictions, self.token.take());
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<CopyResultData> {
        match self.step {
            BytesStep::Source => {
                let source = result.child::<GetObjectResult, GetObjectError>()??;
                self.source_version = source.version_id;
                self.source = Some(source);
                let input = &self.input;
                let rules = BucketRules::Named(input.dest_bucket.clone());
                let snapshot = SnapshotOperation::new(&self.routing, input.group_id, rules);
                self.step = BytesStep::Routing;
                Ok(Next::Call(IoCall::child(snapshot)))
            }
            BytesStep::Routing => {
                self.snapshot = Some(result.child::<RoutingSnapshot, RoutingInputsError>()??);
                let gate = GateOperation::new(self.input.realm_id, self.stamp.now_ms);
                self.step = BytesStep::Gate;
                Ok(Next::Call(IoCall::child(gate)))
            }
            BytesStep::Gate => {
                self.gate = result.child::<Option<GateContext>, GateContextError>()??;
                self.read_origin()
            }
            BytesStep::Origin => {
                // An unreadable source bucket leaves the copy without an origin.
                let origin = (result.child::<BucketInfo, GetBucketError>()?)
                    .ok()
                    .and_then(|bucket| self.origin(bucket.distribution.is_some()));
                self.write(origin)
            }
            BytesStep::Put => {
                let put = result.child::<PutObjectResult, PutObjectError>()??;
                // The new version time owns copy time when dedup reuses an older location.
                Ok(Next::Done(CopyResultData {
                    size: put.location.blob_size,
                    location: Some(put.location),
                    source_metadata: None,
                    version_id: put.version_id,
                    created_at: version_time(put.version_id),
                    source_version_id: self.source_version,
                    source_last_modified: self.modified,
                }))
            }
        }
    }
}

/// Publishes the destination as a new version of the source's archive and grants.
#[derive(Debug, PartialEq)]
struct AliasSealedOperation {
    realm_id: RealmId,
    now_ms: u64,
    copy: Option<SealedCopyOperation>,
    location: BackendLocation,
    source_version_id: Ulid,
    modified: Option<SystemTime>,
}

impl AliasSealedOperation {
    fn new(
        input: CopyObjectInput,
        head: HeadObjectResult,
        location: BackendLocation,
        modified: Option<SystemTime>,
        now_ms: u64,
    ) -> Result<Self, CopyObjectError> {
        let source_version_id = head
            .version_id
            .ok_or(CopyObjectError::Get(GetObjectError::NoSuchVersion))?;
        let copy = SealedCopyOperation::new(SealedCopyInput {
            bucket: input.dest_bucket,
            source_key: input.source_key,
            source_version_id,
            location: location.clone(),
            source_policies: head.source_policies,
            size: location.blob_size,
            dest_key: input.dest_key,
            metadata: input.metadata,
            user_id: input.user_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            quota_ceiling: input.quota_ceiling,
            job_id: input.job_id,
        });
        Ok(Self {
            realm_id: input.realm_id,
            now_ms,
            copy: Some(copy),
            location,
            source_version_id,
            modified,
        })
    }
}

impl Operation for AliasSealedOperation {
    type Output = CopyResultData;
    type Error = CopyObjectError;

    fn first(&mut self) -> CopyTurn<CopyResultData> {
        let gate = GateOperation::new(self.realm_id, self.now_ms);
        Ok(Next::Call(IoCall::child(gate)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<CopyResultData> {
        if let Some(copy) = self.copy.take() {
            let copy = match result.child::<Option<GateContext>, GateContextError>()?? {
                Some(gate) => copy.with_gate(gate),
                None => copy,
            };
            return Ok(Next::Call(IoCall::child(copy)));
        }
        let copied = result.child::<SealedCopyResult, SealedCopyError>()?;
        let version_id = copied.map_err(sealed_error)?.version_id;
        Ok(Next::Done(CopyResultData {
            size: self.location.blob_size,
            location: Some(self.location.clone()),
            source_metadata: None,
            version_id,
            created_at: version_time(version_id),
            source_version_id: Some(self.source_version_id),
            source_last_modified: self.modified,
        }))
    }
}

/// Admits a plaintext read of the source bucket the way GET does: an encrypting bucket must be
/// unlocked, since a token opens single objects only. No bytes move, so the lease ends at once.
#[derive(Debug, PartialEq)]
struct AdmitSourceOperation {
    bucket: String,
    admitting: bool,
}

impl Operation for AdmitSourceOperation {
    type Output = ();
    type Error = CopyObjectError;

    fn first(&mut self) -> CopyTurn<()> {
        let key = self.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(BUCKET_ENCRYPTION_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<()> {
        if self.admitting {
            return match result.event()? {
                Event::Blob(BlobEvent::ReadAdmitted { .. }) => Ok(Next::Done(())),
                Event::Blob(BlobEvent::Error(BlobError::BucketKey(error))) => {
                    Err(CopyObjectError::Get(GetObjectError::ConversionError(
                        ConversionError::BucketKey(error),
                    )))
                }
                _ => Err(CopyObjectError::Get(GetObjectError::GetObjectFailed)),
            };
        }
        let value = result.value()?;
        let settings = BucketEncryption::from_row(value.as_deref())
            .map_err(|error| CopyObjectError::Get(GetObjectError::ConversionError(error)))?;
        let Some(key) = settings.active_key() else {
            return Ok(Next::Done(()));
        };
        let archive = reference_archive(key);
        self.admitting = true;
        let admit = BlobEffect::AdmitRead { key, archive };
        Ok(Next::Call(Effect::Blob(admit).into()))
    }
}

/// Records a reference of the source's binding at the destination, carrying the source's refs.
/// No byte moves; another bucket reads the reference without the source's lock, so it is
/// admitted there first.
#[derive(Debug, PartialEq)]
struct KeepReferenceOperation {
    input: Option<CopyObjectInput>,
    head: Option<HeadObjectResult>,
    modified: Option<SystemTime>,
    stamp: CopyStamp,
    metadata: Option<SourceMetadata>,
    source_version_id: Option<Ulid>,
    admitting: bool,
}

impl KeepReferenceOperation {
    fn new(
        input: CopyObjectInput,
        head: HeadObjectResult,
        modified: Option<SystemTime>,
        stamp: CopyStamp,
    ) -> Self {
        Self {
            input: Some(input),
            source_version_id: head.version_id,
            head: Some(head),
            modified,
            stamp,
            metadata: None,
            admitting: false,
        }
    }

    fn write(&mut self) -> CopyTurn<CopyResultData> {
        let input = self.input.take().ok_or(IoError::Unfinished)?;
        let head = self.head.take().ok_or(IoError::Unfinished)?;
        let (Some(binding), Some(metadata)) = (head.source_binding, head.source_metadata) else {
            return Err(CopyObjectError::Get(GetObjectError::GetObjectFailed));
        };
        self.metadata = Some(metadata.clone());
        self.admitting = false;
        let write = ReferenceWrite {
            preassigned_version_id: self.stamp.version,
            group_id: input.group_id,
            user_id: input.user_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            bucket: input.dest_bucket,
            key: input.dest_key,
            expected_bucket: None,
            version_source: VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                ..binding
            },
            metadata,
            inherited_policies: head.source_policies,
            connector_guard: None,
            mirror_guards: Vec::new(),
            restrictions: input.restrictions,
            job_id: input.job_id,
        };
        let now = UNIX_EPOCH + Duration::from_millis(self.stamp.now_ms);
        let write = WriteReferenceOperation::new(write, now, self.stamp.fresh);
        Ok(Next::Call(IoCall::child(write)))
    }
}

impl Operation for KeepReferenceOperation {
    type Output = CopyResultData;
    type Error = CopyObjectError;

    fn first(&mut self) -> CopyTurn<CopyResultData> {
        let input = self.input.as_ref().ok_or(IoError::Unfinished)?;
        if input.source_bucket == input.dest_bucket {
            return self.write();
        }
        let admit = AdmitSourceOperation {
            bucket: input.source_bucket.clone(),
            admitting: false,
        };
        self.admitting = true;
        Ok(Next::Call(IoCall::child(admit)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<CopyResultData> {
        if self.admitting {
            result.child::<(), CopyObjectError>()??;
            return self.write();
        }
        let written = result.child::<(Ulid, bool), MaterializeReferenceError>()?;
        let (version_id, _changed) = written?;
        let metadata = self.metadata.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Done(CopyResultData {
            location: None,
            size: metadata.content_length,
            source_metadata: Some(metadata),
            version_id,
            created_at: version_time(version_id),
            source_version_id: self.source_version_id,
            source_last_modified: self.modified,
        }))
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::driver::{DriverContext, drive, node_routing};
    use crate::s3::object::get::{GetObjectOperation, MAX_AUTO_ADVANCES};
    #[allow(unused_imports)]
    pub(crate) use crate::testing_objects::{put_config, seed_bucket, with_blob};
    use crate::tests::policy::{seed_gate, subject};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::egress::EgressPolicy;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::PortableSourceDescriptor;
    use aruna_core::structs::storage::blob::{
        Backend, BackendConfig, BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_net::NetHandle;
    use aruna_storage::storage;
    use axum::{Router, routing::get};
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::{TempDir, tempdir};
    use tokio::net::TcpListener;

    fn test_node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[7; 32]).public()
    }

    async fn copy_object(
        context: &DriverContext,
        input: CopyObjectInput,
    ) -> Result<CopyResultData, CopyObjectError> {
        drive(
            CopyObjectOperation::new(input, node_routing(context)),
            context,
        )
        .await
    }

    async fn copy_object_at(
        context: &DriverContext,
        input: CopyObjectInput,
        version_id: Ulid,
    ) -> Result<CopyResultData, CopyObjectError> {
        let copy = CopyObjectOperation::new(input, node_routing(context)).at_version(version_id);
        drive(copy, context).await
    }

    async fn copy_object_tracked(
        context: &DriverContext,
        input: CopyObjectInput,
        pulled: Option<Arc<AtomicU64>>,
    ) -> Result<CopyResultData, CopyObjectError> {
        let copy = CopyObjectOperation::new(input, node_routing(context));
        match pulled {
            Some(pulled) => drive(copy.with_progress(pulled), context).await,
            None => drive(copy, context).await,
        }
    }

    fn step_input(conditions: CopySourceConditions) -> CopyObjectInput {
        let realm_id = RealmId::from_bytes([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        CopyObjectInput {
            source_bucket: "source".to_string(),
            source_key: "a".to_string(),
            source_version_id: None,
            source_group_id: Ulid::from_bytes([3; 16]),
            source_auth_context: auth_context(user_id),
            dest_bucket: "dest".to_string(),
            dest_key: "b".to_string(),
            user_id,
            group_id: Ulid::from_bytes([3; 16]),
            realm_id,
            node_id: test_node_id(),
            quota_ceiling: None,
            conditions,
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        }
    }

    fn step_head() -> HeadObjectResult {
        HeadObjectResult {
            location: None,
            metadata: HashMap::new(),
            source_metadata: None,
            last_refresh: None,
            version_created_at: None,
            version_id: None,
            resolved_version_id: None,
            checksum_type:
                aruna_core::structs::storage::multipart::MultipartChecksumType::FullObject,
            composite_hashes: HashMap::new(),
            part_count: None,
            source_policies: Vec::new(),
            source_binding: None,
        }
    }

    fn step_copy(conditions: CopySourceConditions) -> CopyObjectOperation {
        CopyObjectOperation::new(step_input(conditions), NodeRouting::default())
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: std::fmt::Debug + PartialEq + Send + 'static,
        E: std::fmt::Debug + PartialEq + Send + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(outcome))
    }

    #[test]
    fn failed_condition_stops() {
        let mut copy = step_copy(CopySourceConditions {
            if_match: Some("other".to_string()),
            ..Default::default()
        });
        assert!(matches!(copy.first(), Ok(Next::Call(IoCall::Child(_)))));
        let fenced = copy.next(child::<(), PurgeFenceError>(Ok(())));
        assert!(matches!(fenced, Ok(Next::Call(IoCall::Child(_)))));
        let answer = copy.next(child::<_, HeadObjectError>(Ok(step_head())));
        assert_eq!(answer, Err(CopyObjectError::PreconditionFailed));
    }

    #[test]
    fn missing_source_fails() {
        let mut copy = step_copy(CopySourceConditions::default());
        copy.first().unwrap();
        copy.next(child::<(), PurgeFenceError>(Ok(()))).unwrap();
        let missing = child::<HeadObjectResult, _>(Err(HeadObjectError::NoSuchKey));
        let answer = copy.next(missing);
        assert_eq!(answer, Err(CopyObjectError::Get(GetObjectError::NoSuchKey)));
    }

    #[test]
    fn committed_version_answers() {
        let version = Ulid::from_bytes([4; 16]);
        let mut copy = step_copy(CopySourceConditions::default()).at_version(version);
        copy.first().unwrap();
        let answer = copy.next(child::<_, HeadObjectError>(Ok(step_head())));
        let Ok(Next::Done(done)) = answer else {
            panic!("a committed version answers the copy");
        };
        assert_eq!(done.version_id, version);
        assert_eq!(done.source_version_id, None);
    }

    #[test]
    fn absent_version_copies() {
        let version = Ulid::from_bytes([4; 16]);
        let mut copy = step_copy(CopySourceConditions::default()).at_version(version);
        copy.first().unwrap();
        let missing = child::<HeadObjectResult, _>(Err(HeadObjectError::NoSuchVersion));
        assert!(matches!(
            copy.next(missing),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(copy.step, CopyStep::Fence);
    }

    #[test]
    fn unbound_reference_fails() {
        let input = step_input(CopySourceConditions::default());
        let stamp = CopyStamp {
            version: None,
            fresh: Ulid::from_bytes([5; 16]),
            now_ms: 1,
        };
        let mut keep = KeepReferenceOperation::new(input, step_head(), None, stamp);
        assert!(matches!(keep.first(), Ok(Next::Call(IoCall::Child(_)))));
        let answer = keep.next(child::<(), CopyObjectError>(Ok(())));
        assert_eq!(
            answer,
            Err(CopyObjectError::Get(GetObjectError::GetObjectFailed))
        );
    }

    #[test]
    fn plain_source_admitted() {
        let mut admit = AdmitSourceOperation {
            bucket: "source".to_string(),
            admitting: false,
        };
        assert!(matches!(admit.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = admit.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(())));
    }

    #[test]
    fn locked_source_refused() {
        let settings = BucketEncryption {
            mode: aruna_core::structs::storage::encryption::EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([6; 16])),
            key_generation: 1,
            ..Default::default()
        };
        let mut admit = AdmitSourceOperation {
            bucket: "source".to_string(),
            admitting: false,
        };
        admit.first().unwrap();
        let row = settings.to_bytes().unwrap();
        let read = admit.next(IoResult::Values(vec![Some(row.into())]));
        assert!(matches!(read, Ok(Next::Call(IoCall::Effect(_)))));
        let error = aruna_core::structs::storage::encryption::BucketKeyError::InvalidToken;
        let event = Event::Blob(BlobEvent::Error(BlobError::BucketKey(error)));
        let answer = admit.next(IoResult::Event(Box::new(event)));
        assert!(matches!(
            answer,
            Err(CopyObjectError::Get(GetObjectError::ConversionError(
                ConversionError::BucketKey(_)
            )))
        ));
    }

    fn auth_context(user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: user_id.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    pub(crate) async fn full_context() -> (TempDir, DriverContext) {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
        let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::with_egress(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
            EgressPolicy::loopback(),
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (temp_handle, context)
    }

    pub(crate) async fn spawn_reference_server(
        body: &'static [u8],
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/folder/file.txt",
            get(
                move || async move { ([("content-type", "text/plain"), ("etag", "etag-1")], body) },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    pub(crate) async fn write_version(
        context: &DriverContext,
        bucket: &str,
        key: &str,
        version: BlobVersion,
    ) {
        let version_id = Ulid::generate();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
    }

    async fn read_dest_version(
        context: &DriverContext,
        bucket: &str,
        key: &str,
        version_id: Ulid,
    ) -> BlobVersion {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob version entry");
        };
        BlobVersion::from_bytes(value.expect("missing blob version").as_ref()).unwrap()
    }

    /// A rule that admits exactly this node, so a governed write is allowed.
    pub(crate) fn admits(
        node_id: NodeId,
        seed: u8,
    ) -> aruna_core::structs::placement::policy::VerifiedPolicy {
        let policy = aruna_core::structs::placement::policy::PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
            vec![aruna_core::structs::placement::policy::PlacementSelector {
                node_id: Some(node_id),
                location: None,
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        aruna_core::structs::placement::policy::VerifiedPolicy::verify(policy)
            .expect("policy verifies")
    }

    #[tokio::test]
    async fn copy_unions_refs() {
        // Union tightens: the copy carries both its source's and its
        // destination's refs.
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let source = admits(node_id, 1);
        let dest = admits(node_id, 2);
        let (source_ref, dest_ref) = (source.policy_ref(), dest.policy_ref());
        seed_gate(
            &context,
            realm_id,
            user_id,
            subject(node_id, "eu-west"),
            &[source.clone(), dest.clone()],
        )
        .await;
        seed_bucket(&context, "source", group_id, user_id, vec![source_ref]).await;
        seed_bucket(&context, "dest", group_id, user_id, vec![dest_ref]).await;
        let gate = drive(GateOperation::new(realm_id, 1_000), &context)
            .await
            .expect("subject reads")
            .expect("gate present");
        drive(
            PutObjectOperation::new(put_config(
                realm_id,
                group_id,
                node_id,
                "source",
                "object.txt",
                b"payload",
            ))
            .with_gate(gate),
            &context,
        )
        .await
        .unwrap();

        let result = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "source".to_string(),
                source_key: "object.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "dest".to_string(),
                dest_key: "copy.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
        )
        .await
        .unwrap();

        let copied = read_dest_version(&context, "dest", "copy.txt", result.version_id).await;
        assert_eq!(copied.placement_policies, vec![source_ref, dest_ref]);
    }

    #[tokio::test]
    async fn materialized_copy_deduplicates() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);

        let source = drive(
            PutObjectOperation::new(put_config(
                realm_id,
                group_id,
                node_id,
                "bucket",
                "source.txt",
                b"hello, world!",
            )),
            &context,
        )
        .await
        .unwrap();

        let copy_started_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let result = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
        )
        .await
        .unwrap();

        let source_version =
            read_dest_version(&context, "bucket", "source.txt", source.version_id).await;
        assert_eq!(result.location, Some(source.location));
        assert_eq!(result.source_version_id, Some(source.version_id));
        assert_eq!(result.source_last_modified, Some(source_version.created_at));
        // The copy dedups onto the source location, but its last-modified must
        // come from the fresh destination version, not the source's timestamp.
        assert_eq!(
            result.created_at,
            UNIX_EPOCH + Duration::from_millis(result.version_id.timestamp_ms())
        );
        assert!(result.version_id.timestamp_ms() >= copy_started_ms);

        let dest_version =
            read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
        assert!(dest_version.is_materialized());
        assert_eq!(dest_version.created_at, result.created_at);
    }

    #[tokio::test]
    async fn given_version_replays() {
        // A retry with the minted version answers that version instead of writing another.
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let config = put_config(realm_id, group_id, node_id, "bucket", "a.txt", b"bytes");
        drive(PutObjectOperation::new(config), &context)
            .await
            .unwrap();
        let input = || CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "a.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "b.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        };
        let version = Ulid::generate();
        let first = copy_object_at(&context, input(), version).await.unwrap();
        let again = copy_object_at(&context, input(), version).await.unwrap();
        assert_eq!((first.version_id, again.version_id), (version, version));
        let head = HeadObjectInput {
            bucket: "bucket".to_string(),
            key: "b.txt".to_string(),
            version_id: None,
        };
        let head = drive(HeadObjectOperation::new(head), &context)
            .await
            .unwrap();
        assert_eq!(head.resolved_version_id, Some(version));
    }

    #[tokio::test]
    async fn replays_without_source() {
        // The copy committed and its answer was lost; the source was deleted since, yet the
        // retry answers the committed version instead of failing on the missing source.
        use crate::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let config = put_config(realm_id, group_id, node_id, "bucket", "a.txt", b"bytes");
        drive(PutObjectOperation::new(config), &context)
            .await
            .unwrap();
        let input = || CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "a.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "b.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        };
        let version = Ulid::generate();
        let first = copy_object_at(&context, input(), version).await;
        assert_eq!(first.unwrap().size, 5);
        let delete = DeleteObjectInput {
            bucket: "bucket".to_string(),
            key: "a.txt".to_string(),
            version_id: None,
            group_id,
            realm_id,
            node_id,
            deleted_by: user_id,
        };
        drive(DeleteObjectOperation::new(delete), &context)
            .await
            .unwrap();
        let fresh = copy_object_at(&context, input(), Ulid::generate()).await;
        assert!(fresh.is_err(), "a new copy of the deleted source must fail");
        let again = copy_object_at(&context, input(), version).await.unwrap();
        assert_eq!((again.version_id, again.size), (version, 5));
    }

    #[tokio::test]
    async fn reference_copy_materializes() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);

        let (endpoint, server) = spawn_reference_server(b"reference-bytes").await;
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::generate()),
            mirrors: Vec::new(),
        };
        write_version(
            &context,
            "bucket",
            "ref.txt",
            BlobVersion::reference(
                source,
                SourceMetadata {
                    content_length: 15,
                    content_type: Some("text/plain".to_string()),
                    etag: Some("etag-1".to_string()),
                    last_modified: Some(SystemTime::UNIX_EPOCH),
                    source_version: None,
                    digests: Default::default(),
                },
                SystemTime::UNIX_EPOCH,
                UserId::local(Ulid::generate(), realm_id),
                SystemTime::UNIX_EPOCH,
            ),
        )
        .await;

        let result = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "ref.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
        )
        .await
        .unwrap();

        let dest_version =
            read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
        assert!(dest_version.is_materialized());
        assert_eq!(
            dest_version
                .source_binding()
                .map(|binding| binding.strategy.clone()),
            Some(StagingStrategy::Snapshot)
        );

        server.abort();
        let _ = server.await;

        let mut blob = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "bucket".to_string(),
                key: "dest.txt".to_string(),
                version_id: None,
                range: None,
                group_id,
                user_identity: UserId::local(Ulid::generate(), realm_id),
                realm_id,
                node_id: test_node_id(),
            }),
            &context,
        )
        .await
        .unwrap()
        .blob;
        let mut read_buffer = Vec::new();
        while let Some(Ok(bytes)) = blob.next().await {
            read_buffer.extend_from_slice(&bytes);
        }
        assert_eq!(read_buffer, b"reference-bytes");
    }

    // A copy reading a capped reference must surface the exact variant, so the
    // caller learns that only an explicit rebind heals it.
    #[tokio::test]
    async fn copy_preserves_reference() {
        // A kept reference is cloned from the source version alone: the
        // connector is never contacted, so the source may be unreachable.
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        seed_bucket(&context, "bucket", group_id, user_id, Vec::new()).await;
        let (endpoint, server) = spawn_reference_server(b"reference-bytes").await;
        server.abort();
        let _ = server.await;
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        write_version(
            &context,
            "bucket",
            "ref.txt",
            BlobVersion::reference(
                source.clone(),
                SourceMetadata {
                    content_length: 15,
                    content_type: Some("text/plain".to_string()),
                    etag: Some("etag-1".to_string()),
                    last_modified: Some(SystemTime::UNIX_EPOCH),
                    source_version: None,
                    digests: Default::default(),
                },
                SystemTime::UNIX_EPOCH,
                user_id,
                SystemTime::UNIX_EPOCH,
            ),
        )
        .await;

        let result = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "ref.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Preserve,
                job_id: None,
            },
        )
        .await
        .unwrap();
        assert!(result.location.is_none());
        assert_eq!(result.size, 15);
        assert!(result.source_version_id.is_some());

        let dest_version =
            read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
        assert!(!dest_version.is_materialized());
        let binding = dest_version.source_binding().expect("a reference binding");
        assert_eq!(binding.strategy, StagingStrategy::Reference);
        assert_eq!(binding.descriptor, source.descriptor);
    }

    #[tokio::test]
    async fn locked_reference_refused() {
        // A reference of a locked encrypting bucket is not preserved into another bucket, where
        // its external bytes would be readable without the source's key.
        use aruna_core::compute::{SecretBytes, SharedSecret};
        use aruna_core::structs::execution::job::RoCrateLimits;
        use aruna_core::structs::storage::encryption::{
            BucketKeyError, EncryptionMode, TokenCredential,
        };
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        seed_bucket(&context, "locked", group_id, user_id, Vec::new()).await;
        seed_bucket(&context, "plain", group_id, user_id, Vec::new()).await;
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::generate()),
            key_generation: 1,
            ..Default::default()
        };
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                key: b"locked".to_vec().into(),
                value: settings.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "http://127.0.0.1:9".to_string(),
                )]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let metadata = SourceMetadata {
            content_length: 15,
            content_type: None,
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        };
        let version = BlobVersion::reference(
            source,
            metadata,
            SystemTime::UNIX_EPOCH,
            user_id,
            SystemTime::UNIX_EPOCH,
        );
        write_version(&context, "locked", "ref.txt", version).await;
        let request = |dest_bucket: &str| CopyObjectInput {
            source_bucket: "locked".to_string(),
            source_key: "ref.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: dest_bucket.to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Preserve,
            job_id: None,
        };

        let refused = copy_object(&context, request("plain")).await;

        assert!(matches!(
            refused,
            Err(CopyObjectError::Get(GetObjectError::ConversionError(
                ConversionError::BucketKey(_)
            )))
        ));
        let head = BlobHeadKey::new("plain", "dest.txt").to_bytes().unwrap();
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: head.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event");
        };
        assert!(value.is_none(), "nothing is published in the plain bucket");
        // Within its own bucket the reference stays behind the same lock, so it is kept.
        copy_object(&context, request("locked")).await.unwrap();

        // A token opens single objects only, so it admits no reference copy of a locked bucket.
        let token = Some(TokenRead {
            credential: TokenCredential {
                access_key: "HOLDER".to_string(),
                token: SharedSecret::new(SecretBytes::new(vec![9; 32])),
            },
            limits: RoCrateLimits::default(),
        });
        let copy = CopyObjectOperation::new(request("plain"), node_routing(&context));
        let refused = drive(copy.with_token(token), &context).await;
        assert!(matches!(
            refused,
            Err(CopyObjectError::Get(GetObjectError::ConversionError(
                ConversionError::BucketKey(BucketKeyError::Locked(_))
            )))
        ));
    }

    /// Stores the realm and group documents, so `owner` holds the group's roles.
    pub(crate) async fn seed_authority(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: GroupId,
        owner: UserId,
    ) {
        use aruna_core::keyspaces::AUTH_KEYSPACE;
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::identity::group::GroupAuthorizationDocument;
        use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
        let realm = RealmAuthorizationDocument {
            realm_id,
            roles: Default::default(),
            operation_restrictions: Default::default(),
        };
        let group = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            user_id: owner,
            realm_id,
        };
        for (key, value) in [
            (
                realm_id.as_bytes().to_vec(),
                realm.to_bytes(&actor).unwrap(),
            ),
            (
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            let write = StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(write).await;
        }
    }

    #[tokio::test]
    async fn backend_copy_adopts() {
        // Bytes the destination's backend already holds are adopted without a
        // stream: nothing passes the progress counter and the location is shared.
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        seed_bucket(&context, "bucket", group_id, user_id, Vec::new()).await;
        let data: &'static [u8] = b"adopt these bytes";
        let source = drive(
            PutObjectOperation::new(put_config(
                realm_id, group_id, node_id, "bucket", "src.txt", data,
            )),
            &context,
        )
        .await
        .unwrap();

        let pulled = Arc::new(AtomicU64::new(0));
        let result = copy_object_tracked(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "src.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
            Some(pulled.clone()),
        )
        .await
        .unwrap();
        assert_eq!(pulled.load(Ordering::Relaxed), 0, "no byte streamed");
        assert_eq!(result.location, Some(source.location.clone()));
        assert_eq!(result.size, data.len() as u64);
        let dest_version =
            read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
        assert!(dest_version.is_materialized());
    }

    #[tokio::test]
    async fn copy_keeps_encodings() {
        // A destination that compresses never adopts the source's raw copy.
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        seed_bucket(&context, "bucket", group_id, user_id, Vec::new()).await;
        let packed = aruna_core::structs::storage::blob::BucketInfo {
            group_id,
            created_at: std::time::UNIX_EPOCH,
            created_by: user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 1,
            compression: Compression::Zstd { level: 3 },
            distribution: None,
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
                key: b"packed".to_vec().into(),
                value: packed.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let data: &'static [u8] = b"bytes that need their own copy";
        let source = drive(
            PutObjectOperation::new(put_config(
                realm_id, group_id, node_id, "bucket", "src.txt", data,
            )),
            &context,
        )
        .await
        .unwrap();

        let pulled = Arc::new(AtomicU64::new(0));
        let result = copy_object_tracked(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "src.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "packed".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
            Some(pulled.clone()),
        )
        .await
        .unwrap();

        assert_eq!(pulled.load(Ordering::Relaxed), data.len() as u64);
        let location = result.location.expect("a materialized copy");
        assert_ne!(location, source.location);
        assert_eq!(location.format.encoding(), EncodingClass::Zstd { level: 3 });
    }

    #[tokio::test]
    async fn copy_reports_exhausted() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);

        let (endpoint, server) = spawn_reference_server(b"reference-bytes").await;
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::generate()),
            mirrors: Vec::new(),
        };
        write_version(
            &context,
            "bucket",
            "capped.txt",
            BlobVersion::reference(
                source,
                SourceMetadata {
                    content_length: 1,
                    content_type: Some("text/plain".to_string()),
                    etag: Some("stale-etag".to_string()),
                    last_modified: Some(SystemTime::UNIX_EPOCH),
                    source_version: None,
                    digests: Default::default(),
                },
                SystemTime::UNIX_EPOCH,
                UserId::local(Ulid::generate(), realm_id),
                SystemTime::UNIX_EPOCH,
            )
            .with_advance_count(MAX_AUTO_ADVANCES),
        )
        .await;

        let error = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "capped.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
        )
        .await
        .unwrap_err();

        server.abort();
        let _ = server.await;
        assert_eq!(
            error,
            CopyObjectError::Get(GetObjectError::ReferenceAdvanceExhausted)
        );
    }

    #[tokio::test]
    async fn delete_marker_errors() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);

        let version_id = Ulid::generate();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", "gone.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::deleted(
                    SystemTime::now(),
                    UserId::local(Ulid::generate(), realm_id),
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let error = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "gone.txt".to_string(),
                source_version_id: Some(version_id),
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, CopyObjectError::Get(GetObjectError::DeleteMarker));
    }

    #[test]
    fn matching_etag_required() {
        let conditions = CopySourceConditions {
            if_match: Some("abc".to_string()),
            ..Default::default()
        };
        assert!(evaluate_source_conditions(&conditions, Some("abc"), None, true).is_ok());
        assert!(evaluate_source_conditions(&conditions, Some("\"abc\""), None, true).is_ok());
        assert_eq!(
            evaluate_source_conditions(&conditions, Some("def"), None, true),
            Err(CopyObjectError::PreconditionFailed)
        );
        assert_eq!(
            evaluate_source_conditions(&conditions, None, None, true),
            Err(CopyObjectError::PreconditionFailed)
        );
    }

    #[test]
    fn wildcard_uses_existence() {
        let if_match = CopySourceConditions {
            if_match: Some("*".to_string()),
            ..Default::default()
        };
        assert!(evaluate_source_conditions(&if_match, None, None, true).is_ok());
        assert_eq!(
            evaluate_source_conditions(&if_match, None, None, false),
            Err(CopyObjectError::PreconditionFailed)
        );

        let if_none_match = CopySourceConditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        };
        assert_eq!(
            evaluate_source_conditions(&if_none_match, None, None, true),
            Err(CopyObjectError::PreconditionFailed)
        );
        assert!(evaluate_source_conditions(&if_none_match, None, None, false).is_ok());
    }

    #[test]
    fn matching_etag_rejected() {
        let conditions = CopySourceConditions {
            if_none_match: Some("abc".to_string()),
            ..Default::default()
        };
        assert_eq!(
            evaluate_source_conditions(&conditions, Some("abc"), None, true),
            Err(CopyObjectError::PreconditionFailed)
        );
        assert!(evaluate_source_conditions(&conditions, Some("def"), None, true).is_ok());
    }

    #[test]
    fn checks_unmodified_precision() {
        let threshold = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let conditions = CopySourceConditions {
            if_unmodified_since: Some(threshold),
            ..Default::default()
        };
        assert_eq!(
            evaluate_source_conditions(
                &conditions,
                None,
                Some(threshold + Duration::from_secs(1)),
                true,
            ),
            Err(CopyObjectError::PreconditionFailed)
        );
        assert!(evaluate_source_conditions(&conditions, None, Some(threshold), true).is_ok());
        assert!(
            evaluate_source_conditions(
                &conditions,
                None,
                Some(threshold + Duration::from_millis(500)),
                true,
            )
            .is_ok()
        );
    }

    #[test]
    fn checks_modified_precision() {
        let threshold = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let conditions = CopySourceConditions {
            if_modified_since: Some(threshold),
            ..Default::default()
        };
        assert_eq!(
            evaluate_source_conditions(&conditions, None, Some(threshold), true),
            Err(CopyObjectError::PreconditionFailed)
        );
        assert_eq!(
            evaluate_source_conditions(
                &conditions,
                None,
                Some(threshold + Duration::from_millis(500)),
                true,
            ),
            Err(CopyObjectError::PreconditionFailed)
        );
        assert!(
            evaluate_source_conditions(
                &conditions,
                None,
                Some(threshold + Duration::from_secs(1)),
                true,
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn precondition_preserves_versions() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::generate(), realm_id);

        drive(
            PutObjectOperation::new(put_config(
                realm_id,
                group_id,
                node_id,
                "bucket",
                "source.txt",
                b"payload",
            )),
            &context,
        )
        .await
        .unwrap();

        let error = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                source_auth_context: auth_context(user_id),
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id,
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions {
                    if_none_match: Some("*".to_string()),
                    ..Default::default()
                },
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
                job_id: None,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, CopyObjectError::PreconditionFailed);

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("bucket", "dest.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        assert!(value.is_none());
    }
}
