//! Decides what a keeping member keeps of one reference version a read served from its source,
//! then spools the source bytes into a hidden copy verified against the record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect};
use aruna_core::errors::SourceResolutionError;
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, DISTRIBUTION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::{ExpectedChecksum, unmatched_checksum};
use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobVersion, BlobVersionState, BucketInfo, HiddenBlobKey, VersionKey,
};
use aruna_core::structs::storage::distribution::on_use::{KeptKind, PullPlan};
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionView};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::types::{GroupId, Value};
use bytes::Bytes;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::held::{HeldElsewhere, admitted};
use crate::connectors::{ResolveBindingInput, ResolveBindingOperation};
use crate::s3::object::get::strong_validator;

/// One reference a read served from its source.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceKeep {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    /// The bucket's group, whose connector of the source root reads the bytes.
    pub group_id: GroupId,
    pub user_id: UserId,
    /// The read asked for a byte range, so the member's range behaviour applies.
    pub ranged: bool,
}

/// What a keeping member keeps of one reference version.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Plan {
    pub(super) kind: KeptKind,
    pub(super) pull: PullPlan,
    pub(super) set_id: Ulid,
    pub(super) member_id: Ulid,
    pub(super) identity: [u8; 32],
    pub(super) version: BlobVersion,
}

#[derive(Debug, Error, PartialEq)]
#[error("the keep plan could not be read: {0}")]
pub(super) struct PlanError(String);

impl From<IoError> for PlanError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn plan_fault(error: impl std::fmt::Display) -> PlanError {
    PlanError(error.to_string())
}

/// The row the pending read asks for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanState {
    Version,
    Bucket,
    Encryption,
    Record,
}

type PlanNext = Result<Next<Option<Plan>>, PlanError>;

/// Reads the version and this member's strategy; outputs `None` when nothing is to be kept.
#[derive(Debug, PartialEq)]
pub(super) struct KeepPlanOperation {
    keep: SourceKeep,
    state: PlanState,
    version: Option<(BlobVersion, [u8; 32])>,
    binding: Option<DistributionBinding>,
}

impl KeepPlanOperation {
    pub(super) fn new(keep: SourceKeep) -> Self {
        Self {
            keep,
            state: PlanState::Version,
            version: None,
            binding: None,
        }
    }

    fn read(&mut self, state: PlanState, key_space: &str, key: Vec<u8>) -> PlanNext {
        self.state = state;
        Ok(Next::Call(read_row(key_space, key)))
    }

    /// The next read, or the plan; `None` when nothing is kept.
    fn decide(&mut self, value: Option<Value>) -> PlanNext {
        let nothing = Ok(Next::Done(None));
        match self.state {
            PlanState::Version => {
                let Some(value) = value else {
                    return nothing;
                };
                let version = BlobVersion::from_bytes(&value).map_err(plan_fault)?;
                let BlobVersionState::Reference { source, .. } = &version.state else {
                    return nothing;
                };
                // Governed versions are served only from registered copies, never kept ones.
                if !version.placement_policies.is_empty() {
                    return nothing;
                }
                let identity = version
                    .identity
                    .filter(|_| HeldElsewhere::of(source, version.identity).is_none());
                let Some(identity) = identity else {
                    return nothing;
                };
                self.version = Some((version, identity));
                let bucket = self.keep.bucket.as_bytes().to_vec();
                self.read(PlanState::Bucket, S3_BUCKET_KEYSPACE, bucket)
            }
            PlanState::Bucket => {
                let binding = (value.as_deref())
                    .and_then(|bucket| BucketInfo::from_bytes(bucket).ok())
                    .and_then(|bucket| bucket.distribution);
                let Some(binding) = binding else {
                    return nothing;
                };
                self.binding = Some(binding);
                let bucket = self.keep.bucket.as_bytes().to_vec();
                self.read(PlanState::Encryption, BUCKET_ENCRYPTION_KEYSPACE, bucket)
            }
            PlanState::Encryption => {
                let encryption = BucketEncryption::from_row(value.as_deref()).map_err(plan_fault)?;
                let binding = self.binding.as_ref();
                let set_id = binding.ok_or(plan_fault("no binding was read"))?.set_id;
                if encryption.active_key().is_some() {
                    return nothing;
                }
                let record = set_id.to_bytes().to_vec();
                self.read(PlanState::Record, DISTRIBUTION_KEYSPACE, record)
            }
            PlanState::Record => {
                let Some(value) = value else {
                    return nothing;
                };
                let view = DistributionView::from_bytes(&value).map_err(plan_fault)?;
                let binding = self.binding.take().ok_or(plan_fault("no binding was read"))?;
                let version = self.version.take().ok_or(plan_fault("no version was read"))?;
                let (version, identity) = version;
                Ok(Next::Done(plan(&self.keep, &view, &binding, version, identity)))
            }
        }
    }
}

/// The plan of a member that keeps what a read through it served, if its strategy keeps it.
fn plan(
    keep: &SourceKeep,
    view: &DistributionView,
    binding: &DistributionBinding,
    version: BlobVersion,
    identity: [u8; 32],
) -> Option<Plan> {
    let snapshot = view.snapshot();
    let member = snapshot.member(binding.member_id)?;
    let behaviour = snapshot.config.range_behaviour(member);
    // A passthrough range read keeps nothing, like a read through a holder.
    let (kind, pull) = match PullPlan::choose(member.strategy, behaviour, keep.ranged, true) {
        pull @ (PullPlan::FillFirst(kind) | PullPlan::ServeAndFill(kind)) => (kind, pull),
        PullPlan::Stream | PullPlan::SpoolFirst => return None,
    };
    let BlobVersionState::Reference {
        cached_metadata, ..
    } = &version.state
    else {
        return None;
    };
    if !admitted(kind, cached_metadata.content_length) {
        return None;
    }
    Some(Plan {
        kind,
        pull,
        set_id: binding.set_id,
        member_id: binding.member_id,
        identity,
        version,
    })
}

impl Operation for KeepPlanOperation {
    type Output = Option<Plan>;
    type Error = PlanError;

    fn first(&mut self) -> PlanNext {
        let keep = &self.keep;
        let key = VersionKey::new(&keep.bucket, &keep.key, keep.version_id).to_bytes();
        self.read(
            PlanState::Version,
            BLOB_VERSIONS_KEYSPACE,
            key.map_err(plan_fault)?,
        )
    }

    fn next(&mut self, result: IoResult) -> PlanNext {
        self.decide(value(result)?)
    }
}

#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub(super) struct SpoolError(String);

impl From<IoError> for SpoolError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn spool_error(reason: impl Into<String>) -> SpoolError {
    SpoolError(reason.into())
}

/// A verified hidden copy: its location, BLAKE3 digest and size.
pub(super) type SpooledCopy = (BackendLocation, [u8; 32], u64);

/// What the pending call of a source copy answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpoolStep {
    Resolve,
    Read,
    Spool,
    Drop,
}

type SpoolNext = Result<Next<SpooledCopy>, SpoolError>;

const MISMATCH: &str = "the source bytes do not match the record";
const NO_REFERENCE: &str = "the version is no reference";

/// Reads the whole source object into a hidden copy and checks it against the record: the
/// recorded ETag binds the read, or the recorded digests must match the copy. A copy that does
/// not match is deleted again.
#[derive(Debug, PartialEq)]
pub(super) struct SpoolSourceOperation {
    keep: SourceKeep,
    plan: Plan,
    step: SpoolStep,
    enforced: Vec<ExpectedChecksum>,
}

impl SpoolSourceOperation {
    pub(super) fn new(keep: SourceKeep, plan: Plan) -> Self {
        Self {
            keep,
            plan,
            step: SpoolStep::Resolve,
            enforced: Vec::new(),
        }
    }

    fn call(&mut self, step: SpoolStep, effect: Effect) -> SpoolNext {
        self.step = step;
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    /// The recorded source metadata of the reference.
    fn cached(&self) -> Result<&SourceMetadata, SpoolError> {
        match &self.plan.version.state {
            BlobVersionState::Reference {
                cached_metadata, ..
            } => Ok(cached_metadata),
            _ => Err(spool_error(NO_REFERENCE)),
        }
    }

    fn read(&mut self, access: ResolvedSourceAccess) -> SpoolNext {
        let cached = self.cached()?.clone();
        let ResolvedSourceAccess::OpenDal { kind, .. } = &access;
        let bound = kind.enforces_if_match() && strong_validator(cached.etag.as_deref());
        self.enforced = cached.enforced_checksums();
        if !bound && self.enforced.is_empty() {
            return Err(spool_error("the source allows no verified read"));
        }
        let read = StagingSourceEffect::Read {
            access,
            range: None,
            expected: bound.then_some(cached),
        };
        self.call(SpoolStep::Read, Effect::StagingSource(read))
    }

    fn spool(
        &mut self,
        metadata: SourceMetadata,
        stream: BackendStream<Result<Bytes, StreamError>>,
    ) -> SpoolNext {
        let length = self.cached()?.content_length;
        if metadata.content_length != length {
            return Err(spool_error("the source object changed its length"));
        }
        let name = match self.plan.kind {
            KeptKind::Attachment => format!("{}-{}", self.plan.set_id, self.keep.version_id),
            KeptKind::Cache => hex::encode(self.plan.identity),
        };
        let spool = BlobEffect::SpoolHidden {
            namespace: self.plan.kind.namespace(),
            name,
            created_by: self.keep.user_id,
            max_bytes: Some(length),
            deadline: None,
            blob: stream,
        };
        self.call(SpoolStep::Spool, Effect::Blob(spool))
    }

    fn check(&mut self, copy: SpooledCopy) -> SpoolNext {
        let length = self.cached()?.content_length;
        let (location, _, size) = &copy;
        if *size == length && unmatched_checksum(&self.enforced, &location.hashes).is_none() {
            return Ok(Next::Done(copy));
        }
        let key = HiddenBlobKey::try_from(location).map_err(|_| spool_error(MISMATCH))?;
        let drop = BlobEffect::DeleteHidden { key };
        self.call(SpoolStep::Drop, Effect::Blob(drop))
    }
}

impl Operation for SpoolSourceOperation {
    type Output = SpooledCopy;
    type Error = SpoolError;

    fn first(&mut self) -> SpoolNext {
        let BlobVersionState::Reference { source, .. } = &self.plan.version.state else {
            return Err(spool_error(NO_REFERENCE));
        };
        let resolve = ResolveBindingOperation::new(ResolveBindingInput {
            source: source.clone(),
            group_id: Some(self.keep.group_id),
        });
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn next(&mut self, result: IoResult) -> SpoolNext {
        match self.step {
            SpoolStep::Resolve => {
                let resolved = result.child::<ResolvedSourceAccess, SourceResolutionError>()?;
                self.read(resolved.map_err(|error| spool_error(error.to_string()))?)
            }
            SpoolStep::Read => match result.event()? {
                Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream }) => {
                    self.spool(metadata, stream)
                }
                Event::StagingSource(StagingSourceEvent::Error { error }) => {
                    Err(spool_error(error.to_string()))
                }
                _ => Err(spool_error("unexpected answer to the source read")),
            },
            SpoolStep::Spool => match result.event()? {
                Event::Blob(BlobEvent::HiddenSpooled {
                    location,
                    blake3,
                    size,
                }) => self.check((location, blake3, size)),
                Event::Blob(BlobEvent::Error(error)) => Err(spool_error(error.to_string())),
                _ => Err(spool_error("unexpected answer to the copy write")),
            },
            SpoolStep::Drop => {
                if let Event::Blob(BlobEvent::Error(error)) = result.event()? {
                    warn!(%error, "A dropped kept copy is left to the hidden sweep");
                }
                Err(spool_error(MISMATCH))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;

    fn keep() -> SourceKeep {
        let realm_id = RealmId::from_bytes([1; 32]);
        SourceKeep {
            bucket: "spire".to_string(),
            key: "a".to_string(),
            version_id: Ulid::from_bytes([2; 16]),
            group_id: Ulid::from_bytes([3; 16]),
            user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            ranged: false,
        }
    }

    fn version_read() -> IoCall {
        let key = VersionKey::new("spire", "a", Ulid::from_bytes([2; 16]));
        read_row(BLOB_VERSIONS_KEYSPACE, key.to_bytes().unwrap())
    }

    // A version this node lacks keeps nothing.
    #[test]
    fn missing_version_ignored() {
        let mut operation = KeepPlanOperation::new(keep());
        assert_eq!(operation.first(), Ok(Next::Call(version_read())));
        let missing = IoResult::Values(vec![None]);
        assert_eq!(operation.next(missing), Ok(Next::Done(None)));
    }

    // An undecodable version or an answer of another kind fails the plan.
    #[test]
    fn failures_fail() {
        let mut operation = KeepPlanOperation::new(keep());
        operation.first().unwrap();
        assert!(operation.next(IoResult::Done).is_err());
        let mut operation = KeepPlanOperation::new(keep());
        operation.first().unwrap();
        let garbage = IoResult::Values(vec![Some(vec![0xFF].into())]);
        assert!(operation.next(garbage).is_err());
    }

    fn metadata(length: u64) -> SourceMetadata {
        SourceMetadata {
            content_length: length,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }
    }

    fn spool_plan(reference: bool) -> SpoolSourceOperation {
        use aruna_core::structs::execution::staging::StagingStrategy;
        use aruna_core::structs::storage::blob::BackendRef;
        use aruna_core::structs::storage::format::EncodingClass;
        use std::time::UNIX_EPOCH;
        let user = keep().user_id;
        let version = match reference {
            true => {
                let source = crate::connectors::descriptor::build_source_binding(
                    StagingStrategy::Snapshot,
                    &crate::tests::staging::sample_connector(),
                    &crate::tests::staging::sample_metadata(),
                    "folder/file.txt".to_string(),
                    None,
                    None,
                );
                BlobVersion::reference(source, metadata(4), UNIX_EPOCH, user, UNIX_EPOCH)
            }
            false => {
                let backend = BackendRef::node_default();
                BlobVersion::materialized(
                    [1; 32],
                    backend,
                    EncodingClass::Raw,
                    UNIX_EPOCH,
                    user,
                    None,
                )
            }
        };
        let plan = Plan {
            kind: KeptKind::Cache,
            pull: PullPlan::FillFirst(KeptKind::Cache),
            set_id: Ulid::from_bytes([5; 16]),
            member_id: Ulid::from_bytes([6; 16]),
            identity: [7; 32],
            version,
        };
        SpoolSourceOperation::new(keep(), plan)
    }

    fn spooled(size: u64) -> (BackendLocation, [u8; 32], u64) {
        use aruna_core::structs::storage::blob::{BackendRef, HIDDEN_BLOB_PREFIX};
        use aruna_core::structs::storage::format::StoredFormat;
        let namespace = Ulid::from_bytes([5; 16]);
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: format!("{HIDDEN_BLOB_PREFIX}/{namespace}/one"),
            ulid: Ulid::from_bytes([8; 16]),
            format: StoredFormat::default(),
            created_by: keep().user_id,
            created_at: std::time::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes: Default::default(),
        };
        (location, [9; 32], size)
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn at(step: SpoolStep) -> SpoolSourceOperation {
        let mut spool = spool_plan(true);
        spool.step = step;
        spool
    }

    // A version that is no reference has no source to read.
    #[test]
    fn non_reference_refused() {
        let refused = Err(spool_error(NO_REFERENCE));
        assert_eq!(spool_plan(false).first(), refused);
    }

    // The source binding resolves as a child; a failed resolution ends the copy.
    #[test]
    fn resolves_binding_first() {
        let mut spool = spool_plan(true);
        assert!(matches!(spool.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed: Result<ResolvedSourceAccess, SourceResolutionError> =
            Err(SourceResolutionError::NotFound);
        let error = SourceResolutionError::NotFound.to_string();
        let failed = IoResult::Child(ChildOutcome::new(failed));
        assert_eq!(spool.next(failed), Err(spool_error(error)));
    }

    // A source read of the recorded length is spooled; another length is refused.
    #[test]
    fn source_read_spooled() {
        let read = |length: u64| {
            let stream = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
            event(Event::StagingSource(StagingSourceEvent::ReadResult {
                metadata: metadata(length),
                stream: BackendStream::new(stream),
            }))
        };
        let Ok(Next::Call(IoCall::Effect(spool))) = at(SpoolStep::Read).next(read(4)) else {
            panic!("expected the spool");
        };
        assert!(matches!(
            *spool,
            Effect::Blob(BlobEffect::SpoolHidden {
                max_bytes: Some(4),
                ..
            })
        ));
        let changed = Err(spool_error("the source object changed its length"));
        assert_eq!(at(SpoolStep::Read).next(read(5)), changed);
    }

    // A copy of the recorded size is kept; another copy is deleted and refused.
    #[test]
    fn mismatched_copy_dropped() {
        let spooled_event = |size: u64| {
            let (location, blake3, size) = spooled(size);
            event(Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            }))
        };
        let kept = at(SpoolStep::Spool).next(spooled_event(4));
        assert_eq!(kept, Ok(Next::Done(spooled(4))));

        let mut spool = at(SpoolStep::Spool);
        let key = HiddenBlobKey::try_from(&spooled(3).0).unwrap();
        let delete = IoCall::Effect(Box::new(Effect::Blob(BlobEffect::DeleteHidden { key })));
        assert_eq!(spool.next(spooled_event(3)), Ok(Next::Call(delete)));
        let deleted = event(Event::Blob(BlobEvent::HiddenDeleted));
        assert_eq!(spool.next(deleted), Err(spool_error(MISMATCH)));
    }

    // An answer of another family than the pending call fails the copy.
    #[test]
    fn spool_rejects_unexpected() {
        let refused = at(SpoolStep::Read).next(event(Event::Search()));
        assert_eq!(
            refused,
            Err(spool_error("unexpected answer to the source read"))
        );
        assert!(at(SpoolStep::Resolve).next(IoResult::Done).is_err());
    }
}
