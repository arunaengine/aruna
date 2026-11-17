//! Decides what a keeping member keeps of one reference version a read served from its source,
//! then spools the source bytes into a hidden copy verified against the record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, DISTRIBUTION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::{ExpectedChecksum, unmatched_checksum};
use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobVersion, BlobVersionState, BucketInfo, HiddenBlobKey, VersionKey,
};
use aruna_core::structs::storage::distribution::on_use::{KeptKind, PullPlan};
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionView};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::types::{Effects, GroupId, Value};
use bytes::Bytes;
use smallvec::smallvec;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanState {
    Init,
    Version,
    Bucket,
    Encryption,
    Record,
    Finish,
    Error,
}

/// Reads the version and this member's strategy; outputs `None` when nothing is to be kept.
#[derive(Debug, PartialEq)]
pub(super) struct KeepPlanOperation {
    keep: SourceKeep,
    state: PlanState,
    version: Option<(BlobVersion, [u8; 32])>,
    binding: Option<DistributionBinding>,
    output: Option<Result<Option<Plan>, PlanError>>,
}

impl KeepPlanOperation {
    pub(super) fn new(keep: SourceKeep) -> Self {
        Self {
            keep,
            state: PlanState::Init,
            version: None,
            binding: None,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<Option<Plan>, PlanError>) -> Effects {
        self.state = match output {
            Ok(_) => PlanState::Finish,
            Err(_) => PlanState::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn read(&mut self, state: PlanState, key_space: &str, key: Vec<u8>) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    /// The next read, `Ok(None)` when nothing is kept, or the plan.
    fn decide(&mut self, value: Option<Value>) -> Result<Result<Effects, Option<Plan>>, String> {
        let fault = |error: &dyn std::fmt::Display| error.to_string();
        match self.state {
            PlanState::Version => {
                let Some(value) = value else {
                    return Ok(Err(None));
                };
                let version = BlobVersion::from_bytes(&value).map_err(|error| fault(&error))?;
                let BlobVersionState::Reference { source, .. } = &version.state else {
                    return Ok(Err(None));
                };
                // Governed versions are served only from registered copies, never kept ones.
                if !version.placement_policies.is_empty() {
                    return Ok(Err(None));
                }
                let identity = version
                    .identity
                    .filter(|_| HeldElsewhere::of(source, version.identity).is_none());
                let Some(identity) = identity else {
                    return Ok(Err(None));
                };
                self.version = Some((version, identity));
                let bucket = self.keep.bucket.as_bytes().to_vec();
                Ok(Ok(self.read(PlanState::Bucket, S3_BUCKET_KEYSPACE, bucket)))
            }
            PlanState::Bucket => {
                let binding = (value.as_deref())
                    .and_then(|bucket| BucketInfo::from_bytes(bucket).ok())
                    .and_then(|bucket| bucket.distribution);
                let Some(binding) = binding else {
                    return Ok(Err(None));
                };
                self.binding = Some(binding);
                let bucket = self.keep.bucket.as_bytes().to_vec();
                Ok(Ok(self.read(
                    PlanState::Encryption,
                    BUCKET_ENCRYPTION_KEYSPACE,
                    bucket,
                )))
            }
            PlanState::Encryption => {
                let encryption =
                    BucketEncryption::from_row(value.as_deref()).map_err(|error| fault(&error))?;
                let set_id = self.binding.as_ref().ok_or("no binding was read")?.set_id;
                if encryption.active_key().is_some() {
                    return Ok(Err(None));
                }
                let record = set_id.to_bytes().to_vec();
                Ok(Ok(self.read(
                    PlanState::Record,
                    DISTRIBUTION_KEYSPACE,
                    record,
                )))
            }
            PlanState::Record => {
                let Some(value) = value else {
                    return Ok(Err(None));
                };
                let view = DistributionView::from_bytes(&value).map_err(|error| fault(&error))?;
                let binding = self.binding.take().ok_or("no binding was read")?;
                let (version, identity) = self.version.take().ok_or("no version was read")?;
                Ok(Err(plan(&self.keep, &view, &binding, version, identity)))
            }
            _ => Err("no read was asked".to_string()),
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

    fn start(&mut self) -> Effects {
        let keep = &self.keep;
        match VersionKey::new(&keep.bucket, &keep.key, keep.version_id).to_bytes() {
            Ok(key) => self.read(PlanState::Version, BLOB_VERSIONS_KEYSPACE, key),
            Err(error) => self.finish(Err(PlanError(error.to_string()))),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. })
                if !matches!(
                    self.state,
                    PlanState::Init | PlanState::Finish | PlanState::Error
                ) =>
            {
                value
            }
            Event::Storage(StorageEvent::Error { error }) => {
                return self.finish(Err(PlanError(error.to_string())));
            }
            event => {
                let reason = format!("unexpected {} while planning a keep", event.label());
                return self.finish(Err(PlanError(reason)));
            }
        };
        match self.decide(value) {
            Ok(Ok(effects)) => effects,
            Ok(Err(plan)) => self.finish(Ok(plan)),
            Err(reason) => self.finish(Err(PlanError(reason))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PlanState::Finish | PlanState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(PlanError("the keep plan did not finish".to_string())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub(super) struct SpoolError(String);

fn spool_error(reason: impl Into<String>) -> SpoolError {
    SpoolError(reason.into())
}

/// A verified hidden copy: its location, BLAKE3 digest and size.
pub(super) type SpooledCopy = (BackendLocation, [u8; 32], u64);

#[derive(Debug, PartialEq)]
enum SpoolStep {
    Init,
    Resolve(ResolveBindingOperation),
    Read,
    Spool,
    Drop,
    Finish,
}

/// Reads the whole source object into a hidden copy and checks it against the record: the
/// recorded ETag binds the read, or the recorded digests must match the copy. A copy that does
/// not match is deleted again.
#[derive(Debug, PartialEq)]
pub(super) struct SpoolSourceOperation {
    keep: SourceKeep,
    plan: Plan,
    step: SpoolStep,
    enforced: Vec<ExpectedChecksum>,
    output: Option<Result<SpooledCopy, SpoolError>>,
}

impl SpoolSourceOperation {
    pub(super) fn new(keep: SourceKeep, plan: Plan) -> Self {
        Self {
            keep,
            plan,
            step: SpoolStep::Init,
            enforced: Vec::new(),
            output: None,
        }
    }

    fn finish(&mut self, output: Result<SpooledCopy, SpoolError>) -> Effects {
        self.step = SpoolStep::Finish;
        self.output = Some(output);
        smallvec![]
    }

    /// The recorded source metadata of the reference.
    fn cached(&self) -> Option<&SourceMetadata> {
        match &self.plan.version.state {
            BlobVersionState::Reference {
                cached_metadata, ..
            } => Some(cached_metadata),
            _ => None,
        }
    }

    fn read(&mut self, access: ResolvedSourceAccess) -> Effects {
        let Some(cached) = self.cached().cloned() else {
            return self.finish(Err(spool_error("the version is no reference")));
        };
        let ResolvedSourceAccess::OpenDal { kind, .. } = &access;
        let bound = kind.enforces_if_match() && strong_validator(cached.etag.as_deref());
        self.enforced = cached.enforced_checksums();
        if !bound && self.enforced.is_empty() {
            return self.finish(Err(spool_error("the source allows no verified read")));
        }
        self.step = SpoolStep::Read;
        smallvec![Effect::StagingSource(StagingSourceEffect::Read {
            access,
            range: None,
            expected: bound.then_some(cached),
        })]
    }

    fn spool(
        &mut self,
        metadata: SourceMetadata,
        stream: BackendStream<Result<Bytes, StreamError>>,
    ) -> Effects {
        let Some(length) = self.cached().map(|cached| cached.content_length) else {
            return self.finish(Err(spool_error("the version is no reference")));
        };
        if metadata.content_length != length {
            return self.finish(Err(spool_error("the source object changed its length")));
        }
        let name = match self.plan.kind {
            KeptKind::Attachment => format!("{}-{}", self.plan.set_id, self.keep.version_id),
            KeptKind::Cache => hex::encode(self.plan.identity),
        };
        self.step = SpoolStep::Spool;
        smallvec![Effect::Blob(BlobEffect::SpoolHidden {
            namespace: self.plan.kind.namespace(),
            name,
            created_by: self.keep.user_id,
            max_bytes: Some(length),
            deadline: None,
            blob: stream,
        })]
    }

    fn check(&mut self, copy: SpooledCopy) -> Effects {
        let length = self.cached().map(|cached| cached.content_length);
        let (location, _, size) = &copy;
        if Some(*size) == length && unmatched_checksum(&self.enforced, &location.hashes).is_none() {
            return self.finish(Ok(copy));
        }
        let Ok(key) = HiddenBlobKey::try_from(location) else {
            return self.finish(Err(spool_error(MISMATCH)));
        };
        self.step = SpoolStep::Drop;
        smallvec![Effect::Blob(BlobEffect::DeleteHidden { key })]
    }
}

const MISMATCH: &str = "the source bytes do not match the record";

impl Operation for SpoolSourceOperation {
    type Output = SpooledCopy;
    type Error = SpoolError;

    fn start(&mut self) -> Effects {
        let BlobVersionState::Reference { source, .. } = &self.plan.version.state else {
            return self.finish(Err(spool_error("the version is no reference")));
        };
        let mut resolve = ResolveBindingOperation::new(ResolveBindingInput {
            source: source.clone(),
            group_id: Some(self.keep.group_id),
        });
        let effects = resolve.start();
        self.step = SpoolStep::Resolve(resolve);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.step, SpoolStep::Finish), event) {
            (SpoolStep::Resolve(mut resolve), event) => {
                let effects = resolve.step(event);
                if !resolve.is_complete() {
                    self.step = SpoolStep::Resolve(resolve);
                    return effects;
                }
                match resolve.finalize() {
                    Ok(access) => self.read(access),
                    Err(error) => self.finish(Err(spool_error(error.to_string()))),
                }
            }
            (
                SpoolStep::Read,
                Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream }),
            ) => self.spool(metadata, stream),
            (SpoolStep::Read, Event::StagingSource(StagingSourceEvent::Error { error })) => {
                self.finish(Err(spool_error(error.to_string())))
            }
            (SpoolStep::Read, _) => {
                self.finish(Err(spool_error("unexpected answer to the source read")))
            }
            (
                SpoolStep::Spool,
                Event::Blob(BlobEvent::HiddenSpooled {
                    location,
                    blake3,
                    size,
                }),
            ) => self.check((location, blake3, size)),
            (SpoolStep::Spool, Event::Blob(BlobEvent::Error(error))) => {
                self.finish(Err(spool_error(error.to_string())))
            }
            (SpoolStep::Spool, _) => {
                self.finish(Err(spool_error("unexpected answer to the copy write")))
            }
            (SpoolStep::Drop, event) => {
                if let Event::Blob(BlobEvent::Error(error)) = event {
                    warn!(%error, "A dropped kept copy is left to the hidden sweep");
                }
                self.finish(Err(spool_error(MISMATCH)))
            }
            (SpoolStep::Init | SpoolStep::Finish, event) => {
                let label = event.label();
                self.finish(Err(spool_error(format!("unexpected {label}"))))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let unfinished = spool_error("the source copy did not finish");
        self.output.unwrap_or(Err(unfinished))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.step {
            SpoolStep::Resolve(resolve) => resolve.abort(),
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
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

    fn version_read() -> Effect {
        let key = VersionKey::new("spire", "a", Ulid::from_bytes([2; 16]));
        Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.to_bytes().unwrap().into(),
            txn_id: None,
        })
    }

    fn missing() -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        })
    }

    // A version this node lacks keeps nothing.
    #[test]
    fn missing_version_ignored() {
        let mut operation = KeepPlanOperation::new(keep());
        check_trace(
            &mut operation,
            &[version_read()],
            vec![(missing(), Vec::new())],
        );
        assert_eq!(operation.finalize(), Ok(None));
    }

    // A failed read or an undecodable version fails the plan.
    #[test]
    fn failures_fail() {
        let mut operation = KeepPlanOperation::new(keep());
        operation.start();
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert!(operation.finalize().is_err());
        let mut operation = KeepPlanOperation::new(keep());
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(vec![0xFF].into()),
        }));
        assert!(operation.finalize().is_err());
    }

    // Unstarted plans refuse every event, abort nothing and never finish early.
    #[test]
    fn rejects_unexpected() {
        let fresh = || KeepPlanOperation::new(keep());
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
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
        use aruna_core::structs::storage::blob::BackendRef;
        use aruna_core::structs::storage::format::StoredFormat;
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: "hidden/one".to_string(),
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

    // A version that is no reference has no source to read.
    #[test]
    fn non_reference_refused() {
        let mut spool = spool_plan(false);
        assert!(spool.start().is_empty());
        let refused = Err(spool_error("the version is no reference"));
        assert_eq!(spool.finalize(), refused);
    }

    // A source read of the recorded length is spooled; another length is refused.
    #[test]
    fn source_read_spooled() {
        let read = |length: u64| {
            let stream = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
            Event::StagingSource(StagingSourceEvent::ReadResult {
                metadata: metadata(length),
                stream: BackendStream::new(stream),
            })
        };
        let mut spool = spool_plan(true);
        spool.step = SpoolStep::Read;
        let effects = spool.step(read(4));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::SpoolHidden {
                max_bytes: Some(4),
                ..
            })]
        ));
        let mut spool = spool_plan(true);
        spool.step = SpoolStep::Read;
        assert!(spool.step(read(5)).is_empty());
        let changed = Err(spool_error("the source object changed its length"));
        assert_eq!(spool.finalize(), changed);
    }

    // A copy of the recorded size is kept; another copy is deleted and refused.
    #[test]
    fn mismatched_copy_dropped() {
        let event = |size: u64| {
            let (location, blake3, size) = spooled(size);
            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            })
        };
        let mut spool = spool_plan(true);
        spool.step = SpoolStep::Spool;
        assert!(spool.step(event(4)).is_empty());
        assert_eq!(spool.finalize(), Ok(spooled(4)));

        let mut spool = spool_plan(true);
        spool.step = SpoolStep::Spool;
        let key = HiddenBlobKey::try_from(&spooled(3).0).unwrap();
        let delete = Effect::Blob(BlobEffect::DeleteHidden { key });
        assert_eq!(spool.step(event(3)).into_vec(), vec![delete]);
        assert!(spool.step(Event::Blob(BlobEvent::HiddenDeleted)).is_empty());
        assert_eq!(spool.finalize(), Err(spool_error(MISMATCH)));
    }

    // An unstarted copy refuses every event, aborts nothing and never finishes early.
    #[test]
    fn spool_rejects_unexpected() {
        let fresh = || spool_plan(true);
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }
}
