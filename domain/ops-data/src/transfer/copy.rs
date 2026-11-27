//! Copies an object to another key, checking the source conditions and handling references.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod sealed;

use crate::distribution::held::record_only;
use crate::s3::fence_store::FenceCheckOperation;
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectResult, reference_archive};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use crate::s3::object::reference::{
    MaterializeReferenceError, ReferenceWrite, WriteReferenceOperation,
};
use crate::s3::purge_fence::PurgeFenceError;
use crate::transfer::copy::sealed::{
    SealedCopyError, SealedCopyInput, SealedCopyOperation, SealedCopyResult,
};
use crate::transfer::read::{ReadSourceOperation, TokenRead};
use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::BlobEffect;
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::BlobEvent;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::operation::Operation;
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
use aruna_core::user::id::UserId;
use aruna_ops_identity::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use aruna_ops_identity::placement::policy::gate::{GateContext, GateContextError, GateOperation};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
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
    #[error("copying from an encrypted bucket into an unencrypted bucket is not allowed")]
    EncryptionDowngrade,
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

pub fn evaluate_source_conditions(
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
pub fn source_modified(
    created_at: Option<SystemTime>,
    location: Option<&BackendLocation>,
    metadata: Option<&SourceMetadata>,
) -> Option<SystemTime> {
    created_at
        .or_else(|| location.map(|location| location.created_at))
        .or_else(|| metadata.and_then(|metadata| metadata.last_modified))
}

pub fn source_etag(
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
    ids: IdMint,
    input: Option<CopyObjectInput>,
    routing: NodeRouting,
    progress: Option<PulledBytes>,
    token: Option<TokenRead>,
    allow_downgrade: bool,
    stamp: CopyStamp,
    step: CopyStep,
}

impl CopyObjectOperation {
    pub fn new(input: CopyObjectInput, routing: NodeRouting) -> Self {
        Self {
            ids: IdMint::default(),
            input: Some(input),
            routing,
            progress: None,
            token: None,
            allow_downgrade: false,
            stamp: CopyStamp {
                version: None,
                fresh: Ulid::nil(),
                now_ms: 0,
            },
            step: CopyStep::Fence,
        }
    }

    /// Permits copying from an encrypted source bucket into an unencrypted destination bucket.
    pub fn allow_downgrade(mut self, allow: bool) -> Self {
        self.allow_downgrade = allow;
        self
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
            let keep =
                KeepReferenceOperation::new(input, head, modified, stamp, self.allow_downgrade);
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
            allow_downgrade: self.allow_downgrade,
            dest_encrypted: false,
            modified,
            stamp,
            source: None,
            snapshot: None,
            gate: None,
            source_version: None,
            step: BytesStep::Encryption,
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
        self.stamp.fresh = self.ids.mint(self.stamp.now_ms);
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

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

#[derive(Debug, PartialEq)]
enum BytesStep {
    Encryption,
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
    allow_downgrade: bool,
    dest_encrypted: bool,
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
                && !self.dest_encrypted
                && location.format.bucket_key().is_none()
                && resolve_backend(&routing, bucket, key).is_ok_and(|resolved| {
                    resolved.backend == location.backend
                        && EncodingClass::from(resolved.compression) == location.format.encoding()
                })
        });
        let body = match (adopt.is_some(), self.progress.take()) {
            (true, _) => None,
            (false, Some(PulledBytes(pulled))) => Some(source.blob.counted(pulled)),
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
        let mut reads = vec![(
            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            self.input.dest_bucket.as_bytes().to_vec().into(),
        )];
        if self.input.source_bucket != self.input.dest_bucket {
            reads.push((
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                self.input.source_bucket.as_bytes().to_vec().into(),
            ));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<CopyResultData> {
        match self.step {
            BytesStep::Encryption => {
                let mut rows = result.values()?.into_iter();
                let dest_row = rows.next().flatten();
                let dest = BucketEncryption::from_row(dest_row.as_deref()).map_err(|error| {
                    CopyObjectError::Put(PutObjectError::ConversionError(error))
                })?;
                self.dest_encrypted = dest.is_encrypted();
                if self.input.source_bucket != self.input.dest_bucket {
                    let source_row = rows.next().flatten();
                    let source =
                        BucketEncryption::from_row(source_row.as_deref()).map_err(|error| {
                            CopyObjectError::Get(GetObjectError::ConversionError(error))
                        })?;
                    if source.is_encrypted() && !self.dest_encrypted && !self.allow_downgrade {
                        return Err(CopyObjectError::EncryptionDowngrade);
                    }
                }
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
                self.step = BytesStep::Source;
                Ok(Next::Call(IoCall::child(read)))
            }
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
    dest_bucket: Option<String>,
    allow_downgrade: bool,
    dest_encrypted: bool,
    admitting: bool,
}

impl Operation for AdmitSourceOperation {
    type Output = ();
    type Error = CopyObjectError;

    fn first(&mut self) -> CopyTurn<()> {
        let mut reads = vec![(
            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            self.bucket.as_bytes().to_vec().into(),
        )];
        if let Some(dest) = &self.dest_bucket {
            reads.push((
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                dest.as_bytes().to_vec().into(),
            ));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn<()> {
        if self.admitting {
            return match result.blob()? {
                BlobEvent::ReadAdmitted { .. } => {
                    if self.dest_bucket.is_some() && !self.dest_encrypted && !self.allow_downgrade {
                        return Err(CopyObjectError::EncryptionDowngrade);
                    }
                    Ok(Next::Done(()))
                }
                BlobEvent::Error(BlobError::BucketKey(error)) => Err(CopyObjectError::Get(
                    GetObjectError::ConversionError(ConversionError::BucketKey(error)),
                )),
                _ => Err(CopyObjectError::Get(GetObjectError::GetObjectFailed)),
            };
        }
        let mut rows = result.values()?.into_iter();
        let value = rows.next().flatten();
        let settings = BucketEncryption::from_row(value.as_deref())
            .map_err(|error| CopyObjectError::Get(GetObjectError::ConversionError(error)))?;
        if self.dest_bucket.is_some() {
            let dest_row = rows.next().flatten();
            let dest = BucketEncryption::from_row(dest_row.as_deref())
                .map_err(|error| CopyObjectError::Put(PutObjectError::ConversionError(error)))?;
            self.dest_encrypted = dest.is_encrypted();
        }
        let Some(key) = settings.active_key() else {
            return Ok(Next::Done(()));
        };
        let archive = reference_archive(key);
        self.admitting = true;
        let admit = BlobEffect::AdmitRead { key, archive };
        Ok(Next::Call(IoCall::blob(admit)))
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
    allow_downgrade: bool,
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
        allow_downgrade: bool,
    ) -> Self {
        Self {
            input: Some(input),
            source_version_id: head.version_id,
            head: Some(head),
            modified,
            stamp,
            allow_downgrade,
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
            dest_bucket: Some(input.dest_bucket.clone()),
            allow_downgrade: self.allow_downgrade,
            dest_encrypted: false,
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
#[path = "copy_tests.rs"]
pub(crate) mod test;
