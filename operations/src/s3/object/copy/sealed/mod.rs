//! Copies an encrypted object within its bucket by publishing a new version of the same
//! archive. No content is read, so the copy also works while the bucket is locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::records::{
    HeadAliasContext, add_index_effect, owner_write_effect, write_head_effect, write_version_effect,
};
use crate::distribution::guard::{GuardError, WriteAuthority, record_authority, record_read};
use crate::distribution::inventory::{
    AddedVersion, InventoryChange, StagedCall, entry_of, outside_answer, stamp_head, stamp_version,
    version_written,
};
use crate::placement::policy::read::ReadPolicyError;
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOutcome, drift_reads, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::s3::abe_copies::{copy_row, still_active};
use crate::s3::abe_rekey::RekeyProgress;
use crate::s3::object::put::abe::{abe_reads, keyed, located_rows, parse_abe};
use crate::s3::purge_fence::{PurgeFenceError, check_published, publication_rows};
use crate::s3::usage::{
    QuotaGate, QuotaGateError, RowChange, UsageCounterUpdate, UsageUpdateError,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_REKEY_KEYSPACE, ABE_VERSION_KEYSPACE,
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::managed_copy::{CopyRegistration, ManagedCopyError, register_effect};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::plugin_runs::journaled;
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::abe::{
    AbeEffect, AbeError, AbeEvent, AbeParameters, ObjectEnvelope, PendingCopy,
};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo,
    CopyOrigin, CopyOwner, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_core::structs::storage::distribution::hint::with_hint;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{OriginVersion, VersionOrigin};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum SealedCopyError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Quota(#[from] QuotaGateError),
    #[error("group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("the source version no longer uses the archive the copy was asked for")]
    SourceChanged,
    #[error(transparent)]
    PolicyGate(#[from] PolicyGateError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
    #[error(transparent)]
    ManagedCopy(#[from] ManagedCopyError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("Invalid operation state")]
    InvalidState,
    #[error("operation did not finish")]
    NotFinished,
    #[error("usage counters are being rebuilt; retry later")]
    UsageRebuilding,
}

impl From<IoError> for SealedCopyError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// One same-bucket copy of a sealed version, described by what HEAD returned for it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedCopyInput {
    pub bucket: String,
    pub source_key: String,
    pub source_version_id: Ulid,
    /// The archive HEAD described; the source version must still use exactly it.
    pub location: BackendLocation,
    /// Refs of the source version; the copy carries them with the bucket default.
    pub source_policies: Vec<PlacementPolicyRef>,
    /// Original size of the object, for logical usage and quota.
    pub size: u64,
    pub dest_key: String,
    /// Replacement metadata; `None` keeps the source's.
    pub metadata: Option<HashMap<String, String>>,
    pub user_id: UserId,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub quota_ceiling: Option<u64>,
    /// The system job that runs the copy, recorded in the copy's origin.
    pub job_id: Option<Ulid>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedCopyResult {
    pub version_id: Ulid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    ReadBucket,
    Gate,
    Record,
    ReadAbe,
    /// Reads the complete envelope with this write id.
    ReadEnvelope([u8; 16]),
    CreateEnvelope,
    Fence,
    Drift,
    ReadSource,
    ReadLiveness,
    WriteHead,
    WriteIndex,
    WriteVersion,
    WriteOwner,
    FenceAbe,
    WriteEnvelope,
    ReadRekey,
    ResetRekey,
    Register,
    Quota,
    Usage,
}

type Turn = Result<Next<SealedCopyResult>, SealedCopyError>;

/// A read before the transaction, which stays closed until the publication fence.
fn outside(call: IoCall) -> IoCall {
    StagedCall::Outside(call).into_call()
}

/// A write effect or write call finished.
fn written(result: IoResult) -> Result<(), SealedCopyError> {
    match result {
        IoResult::Done => Ok(()),
        IoResult::Event(event) => match *event {
            Event::Storage(
                StorageEvent::WriteResult { .. } | StorageEvent::BatchWriteResult { .. },
            ) => Ok(()),
            _ => Err(SealedCopyError::InvalidState),
        },
        _ => Err(SealedCopyError::InvalidState),
    }
}

#[derive(Debug, PartialEq)]
pub struct SealedCopyOperation {
    input: SealedCopyInput,
    step: Step,
    /// Destination of this node; absent fails every governed copy closed.
    gate_context: Option<GateContext>,
    /// What the gate decided on, read again inside the transaction.
    gated: Option<GatedBucket>,
    /// The bucket default joined with the source refs, stored on the copy.
    refs: Vec<PlacementPolicyRef>,
    version_id: Ulid,
    version: Option<BlobVersion>,
    existing: Option<CurrentVersionPointer>,
    /// The bucket's distribution binding and the head the copy installs, for the inventory.
    binding: Option<DistributionBinding>,
    /// The coordinator epoch the copy's version carries in a distributed bucket.
    epoch: aruna_core::structs::storage::distribution::OriginalEpoch,
    /// The coordinator authority the copy was routed under; its transaction rechecks it.
    authority: Option<WriteAuthority>,
    head: Option<CurrentVersionPointer>,
    was_live: bool,
    /// Offered directory bytes the replaced head stops charging.
    released_bytes: u64,
    quota: Option<QuotaGate>,
    usage: Option<UsageCounterUpdate>,
    /// Admitted parameters and epoch, the complete envelope the copy reuses and its own one.
    admitted: Option<(AbeParameters, u64)>,
    source_envelope: Option<ObjectEnvelope>,
    envelope: Option<ObjectEnvelope>,
    /// Written instead of an envelope while the bucket is locked.
    pending: Option<PendingCopy>,
    envelope_bytes: u64,
}

impl SealedCopyOperation {
    pub fn new(input: SealedCopyInput) -> Self {
        Self {
            input,
            step: Step::ReadBucket,
            gate_context: None,
            gated: None,
            refs: Vec::new(),
            version_id: Ulid::generate(),
            version: None,
            existing: None,
            binding: None,
            epoch: Default::default(),
            authority: None,
            head: None,
            was_live: false,
            released_bytes: 0,
            quota: None,
            usage: None,
            admitted: None,
            source_envelope: None,
            envelope: None,
            pending: None,
            envelope_bytes: 0,
        }
    }

    pub fn with_gate(mut self, context: GateContext) -> Self {
        self.gate_context = Some(context);
        self
    }

    fn archive(&self) -> ArchiveKey {
        ArchiveKey::of(&self.input.location)
    }

    /// Gates the bucket default joined with the source refs before the transaction, like a write.
    fn bucket_read(&mut self, value: Option<Value>) -> Turn {
        let bucket = value
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        self.binding = bucket
            .as_ref()
            .and_then(|bucket| bucket.distribution.clone());
        if let Some(binding) = self.binding.as_ref() {
            self.version_id = with_hint(self.version_id, binding.hint());
        }
        let observed = GatedBucket::observe(bucket.as_ref());
        self.refs = union_refs(&observed.policies, &self.input.source_policies)?;
        let governed = !self.refs.is_empty();
        self.gated = Some(observed.stored_under(self.gate_context.as_ref(), governed));
        let group_id = Some(self.input.group_id);
        match write_gate(self.gate_context.as_ref(), &self.refs, group_id)? {
            None => self.read_abe(),
            Some(gate) => {
                self.step = Step::Gate;
                Ok(Next::Call(IoCall::child(gate)))
            }
        }
    }

    fn gate_checked(&mut self, result: IoResult) -> Turn {
        let outcome = result.child::<PolicyGateOutcome, ReadPolicyError>()?;
        gate_decision(outcome.map_err(PolicyGateError::from)?)?;
        self.read_abe()
    }

    /// The copy names its exact source version: by global id in a distributed bucket.
    fn copy_origin(&self) -> Option<VersionOrigin> {
        let from = match self.binding {
            Some(_) => OriginVersion::Global(GlobalId::Version {
                realm_id: self.input.realm_id,
                version_id: self.input.source_version_id,
            }),
            None => OriginVersion::Node(
                VersionedObjectArn::new(
                    self.input.realm_id,
                    self.input.node_id,
                    &self.input.bucket,
                    &self.input.source_key,
                    self.input.source_version_id,
                )
                .ok()?,
            ),
        };
        let job_id = self.input.job_id;
        Some(VersionOrigin::Copy { from, job_id })
    }

    fn source_version(&self) -> VersionKey {
        VersionKey::new(
            &self.input.bucket,
            &self.input.source_key,
            self.input.source_version_id,
        )
    }

    /// Reads the source's envelope or pending copy row and the admitted parameters and epoch.
    /// Only the coordinator of the destination key's range writes the copy.
    fn read_abe(&mut self) -> Turn {
        match self.binding.as_ref() {
            Some(binding) => {
                let read = outside(record_read(binding).into());
                self.step = Step::Record;
                Ok(Next::Call(read))
            }
            None => self.read_abe_rows(),
        }
    }

    fn record_checked(&mut self, event: Event) -> Turn {
        let binding = self.binding.as_ref().ok_or(SealedCopyError::InvalidState)?;
        let authority = record_authority(binding, &self.input.dest_key, event)?;
        self.epoch = authority.epoch;
        self.authority = Some(authority);
        self.read_abe_rows()
    }

    fn read_abe_rows(&mut self) -> Turn {
        let Some(key) = self.input.location.format.bucket_key() else {
            return Ok(self.begin());
        };
        let source = self.source_version();
        let mut reads = vec![
            (ABE_VERSION_KEYSPACE.to_string(), source.to_bytes()?.into()),
            (
                ABE_COPY_KEYSPACE.to_string(),
                copy_row(key.bucket_id, &source)?.into(),
            ),
        ];
        reads.extend(abe_reads(key));
        self.step = Step::ReadAbe;
        Ok(Next::Call(outside(IoCall::Read(reads))))
    }

    /// A source without any envelope leaves the copy without one, as before.
    fn abe_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Turn {
        let [(_, id), (_, pending), parameters @ ..] = values.as_slice() else {
            return Err(SealedCopyError::InvalidState);
        };
        if id.is_none() && pending.is_none() {
            return Ok(self.begin());
        }
        let key = self.input.location.format.bucket_key();
        let (parameters, epoch) =
            parse_abe(parameters, key.ok_or(SealedCopyError::InvalidState)?).map_err(abe)?;
        self.admitted = Some((parameters, epoch));
        if let Some(id) = id {
            let id = <[u8; 16]>::try_from(id.as_ref()).map_err(|_| abe(AbeError::Context))?;
            self.step = Step::ReadEnvelope(id);
            let read = read_row(ABE_ENVELOPE_KEYSPACE, id.to_vec());
            return Ok(Next::Call(outside(read)));
        }
        // A pending source passes on the complete envelope it names.
        let pending = pending.as_deref().ok_or(SealedCopyError::InvalidState)?;
        self.create_envelope(PendingCopy::from_bytes(pending).map_err(abe)?.source)
    }

    fn envelope_read(&mut self, id: [u8; 16], value: Option<Value>) -> Turn {
        let envelope = ObjectEnvelope::from_bytes(value.as_deref().ok_or(abe(AbeError::Pending))?)
            .map_err(abe)?;
        if envelope.context.write_id.to_bytes() != id
            || envelope.context.object_key != self.input.source_key
        {
            return Err(abe(AbeError::Context));
        }
        self.create_envelope(envelope)
    }

    /// Asks for the copy's own envelope; a locked bucket leaves the copy pending on `source`.
    fn create_envelope(&mut self, source: ObjectEnvelope) -> Turn {
        let (parameters, epoch) = self.admitted.clone().ok_or(SealedCopyError::InvalidState)?;
        if source.context.parameters != parameters {
            return Err(abe(AbeError::Parameters));
        }
        self.step = Step::CreateEnvelope;
        self.source_envelope = Some(source.clone());
        let copy = AbeEffect::Copy {
            source,
            epoch,
            write_id: Ulid::generate(),
            object_key: self.input.dest_key.clone(),
        };
        Ok(Next::Call(
            Effect::Blob(BlobEffect::Abe(Box::new(copy))).into(),
        ))
    }

    fn envelope_created(&mut self, event: Event) -> Turn {
        let Event::Blob(event) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        match event {
            BlobEvent::Abe(event) => match *event {
                AbeEvent::Envelope(envelope) => self.envelope = Some(envelope),
                _ => return Err(SealedCopyError::InvalidState),
            },
            BlobEvent::Error(BlobError::Abe(AbeError::Required)) => {
                let source = self.source_envelope.take();
                let source = source.ok_or(SealedCopyError::InvalidState)?;
                let archive = self.archive();
                self.pending = Some(PendingCopy { source, archive });
            }
            BlobEvent::Error(error) => return Err(error.into()),
            _ => return Err(SealedCopyError::InvalidState),
        }
        Ok(self.begin())
    }

    /// The publication fence is the first read of the transaction.
    fn begin(&mut self) -> Next<SealedCopyResult> {
        self.step = Step::Fence;
        let rows = publication_rows(&self.input.bucket, self.authority.as_ref());
        Next::Call(IoCall::Read(rows))
    }

    /// The default and subject the gate admitted must still hold when the copy commits.
    fn drift_read(&mut self, event: Event) -> Turn {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        let (bucket, subject) = split_drift_reads(values)?;
        let gated = self.gated.as_ref().ok_or(SealedCopyError::InvalidState)?;
        if !gated.matches(&GatedBucket::observe(bucket.as_ref())) {
            return Err(PolicyGateError::Drift.into());
        }
        gated.check_subject(subject.as_ref())?;
        self.read_source()
    }

    fn dest_version(&self) -> VersionKey {
        VersionKey::new(&self.input.bucket, &self.input.dest_key, self.version_id)
    }

    fn alias_context(&self) -> HeadAliasContext {
        HeadAliasContext::new(
            self.input.realm_id,
            self.input.group_id,
            self.input.node_id,
            self.input.bucket.clone(),
            self.input.dest_key.clone(),
        )
    }

    /// Reads the exact source version and the destination head in the transaction.
    fn read_source(&mut self) -> Turn {
        let source = VersionKey::new(
            &self.input.bucket,
            &self.input.source_key,
            self.input.source_version_id,
        );
        let head = BlobHeadKey::new(&self.input.bucket, &self.input.dest_key);
        self.step = Step::ReadSource;
        Ok(Next::Call(IoCall::Read(vec![
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                source.to_bytes()?.into(),
            ),
            (BLOB_HEAD_KEYSPACE.to_string(), head.to_bytes()?.into()),
        ])))
    }

    /// The new version keeps the source's content state, known or pending, on the same archive.
    fn source_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [source, head] =
            <[_; 2]>::try_from(values).map_err(|_| SealedCopyError::InvalidState)?;
        let source = source.ok_or(SealedCopyError::NoSuchVersion)?;
        let source = BlobVersion::from_bytes(source.as_ref())?;
        if source.placement_policies
            != PlacementPolicyRef::canonical_set(&self.input.source_policies)?
        {
            return Err(SealedCopyError::SourceChanged);
        }
        let archive = self.archive();
        if let Some(key) = source.location_key()
            && key != self.input.location.location_key()?
        {
            return Err(SealedCopyError::SourceChanged);
        }
        let state = match source.state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding: encoding @ EncodingClass::Pithos { .. },
                ..
            } if backend == archive.backend => BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                source: None,
            },
            BlobVersionState::PendingContent { archive: used, .. } if used == archive => {
                BlobVersionState::PendingContent {
                    archive,
                    source: None,
                }
            }
            _ => return Err(SealedCopyError::SourceChanged),
        };
        let mut metadata = self.input.metadata.clone().unwrap_or(source.metadata);
        // A copy is never a release, even of a release crate.
        aruna_core::structs::storage::bucket_metadata::strip_release_keys(&mut metadata);
        let mut version = BlobVersion::deleted(SystemTime::now(), self.input.user_id);
        version.state = state;
        self.version = Some(
            version
                .with_metadata(metadata)
                .with_origin(self.copy_origin())
                .with_policies(self.refs.clone())?,
        );
        self.existing = head
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        let Some(pointer) = self.existing.as_ref() else {
            return self.write_head();
        };
        let key = VersionKey::new(&self.input.bucket, &self.input.dest_key, pointer.version_id);
        self.step = Step::ReadLiveness;
        Ok(Next::Call(read_row(
            BLOB_VERSIONS_KEYSPACE,
            key.to_bytes()?,
        )))
    }

    fn liveness_read(&mut self, value: Option<Value>) -> Turn {
        let head = value
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()?;
        self.was_live = head.as_ref().is_some_and(|version| !version.is_deleted());
        self.released_bytes = head
            .and_then(|version| version.offered_size(&self.input.bucket, &self.input.dest_key))
            .unwrap_or(0);
        self.write_head()
    }

    fn write_head(&mut self) -> Turn {
        let pointer = CurrentVersionPointer::next_for(self.existing.as_ref(), self.version_id)?;
        let pointer = stamp_head(pointer, self.binding.as_ref(), self.epoch);
        self.head = Some(pointer.clone());
        self.step = Step::WriteHead;
        let write = write_head_effect(&self.alias_context(), pointer)?;
        Ok(Next::Call(write.into()))
    }

    /// A known content hash gets its path index; a pending archive has none yet.
    fn write_index(&mut self) -> Turn {
        let version = self.version.as_ref().ok_or(SealedCopyError::InvalidState)?;
        let Some(hash) = version.state.blob_hash().copied() else {
            return self.write_version();
        };
        self.step = Step::WriteIndex;
        let context = self.alias_context();
        let write = add_index_effect(&context, hash, self.version_id)?;
        Ok(Next::Call(write.into()))
    }

    fn write_version(&mut self) -> Turn {
        let version = self.version.take().ok_or(SealedCopyError::InvalidState)?;
        let generation = self.head.as_ref().map_or(0, |head| head.generation);
        let version = stamp_version(version, self.binding.as_ref(), generation, self.epoch);
        let input = &self.input;
        let uploaded = SourceEvent::data_uploaded(
            (input.realm_id, input.group_id, input.node_id),
            (&input.bucket, &input.dest_key),
            self.version_id,
            unix_timestamp_millis(),
        );
        let effect = write_version_effect(&self.dest_version(), &version)?;
        let effect = journaled(effect, &uploaded)?;
        self.version = Some(version);
        self.step = Step::WriteVersion;
        Ok(Next::Call(effect.into()))
    }

    /// The new version owns the archive too, so neither alias frees it while the other lives.
    fn write_owner(&mut self) -> Turn {
        let owner = CopyOwner::new(self.archive(), self.dest_version());
        self.step = Step::WriteOwner;
        Ok(Next::Call(owner_write_effect(&owner)?.into()))
    }

    /// Fences the copy's own envelope or writes its pending row; a source without either skips.
    fn publish_envelope(&mut self) -> Turn {
        if let Some(pending) = &self.pending {
            let key = copy_row(
                pending.source.context.parameters.key.bucket_id,
                &self.dest_version(),
            )?;
            let value = pending.to_bytes().map_err(abe)?;
            self.envelope_bytes = value.len() as u64;
            self.step = Step::WriteEnvelope;
            let row = (ABE_COPY_KEYSPACE.to_string(), key.into(), value.into());
            return Ok(Next::Call(IoCall::Write(vec![row])));
        }
        let Some(envelope) = &self.envelope else {
            return self.register();
        };
        let bucket = self.input.bucket.as_bytes().to_vec();
        let mut reads = vec![(BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into())];
        reads.extend(abe_reads(envelope.context.parameters.key));
        self.step = Step::FenceAbe;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// A raise or rotation since the envelope was made refuses the copy, like a stale write.
    fn abe_fenced(&mut self, values: Vec<(Key, Option<Value>)>) -> Turn {
        let [(_, settings), anchors @ ..] = values.as_slice() else {
            return Err(SealedCopyError::InvalidState);
        };
        let envelope = self
            .envelope
            .as_ref()
            .ok_or(SealedCopyError::InvalidState)?;
        let key = envelope.context.parameters.key;
        still_active(settings.as_deref(), key).map_err(abe)?;
        let (parameters, epoch) = parse_abe(anchors, key).map_err(abe)?;
        envelope.anchored(&parameters, epoch).map_err(abe)?;
        let version = self.version.as_ref().ok_or(SealedCopyError::InvalidState)?;
        let limit = RoCrateLimits::default().metadata_bytes;
        let rows = (&version.metadata, limit);
        let (writes, charge) =
            located_rows(envelope, &self.dest_version(), &self.input.location, rows)
                .map_err(abe)?;
        self.envelope_bytes = charge;
        self.step = Step::WriteEnvelope;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// The alias reuses the old object key, so a re-key pass of its prefix starts again.
    fn rekey_key(&self) -> Option<Key> {
        let key = self.input.location.format.bucket_key()?;
        Some(key.bucket_id.to_bytes().to_vec().into())
    }

    fn read_rekey(&mut self) -> Turn {
        let Some(key) = self.rekey_key() else {
            return self.register();
        };
        self.step = Step::ReadRekey;
        Ok(Next::Call(read_row(ABE_REKEY_KEYSPACE, key)))
    }

    fn rekey_read(&mut self, value: Option<Value>) -> Turn {
        let (Some(key), Some(value)) = (self.rekey_key(), value) else {
            return self.register();
        };
        let mut progress: RekeyProgress =
            postcard::from_bytes(&value).map_err(|_| abe(AbeError::Context))?;
        if !self.input.dest_key.starts_with(&progress.prefix) {
            return self.register();
        }
        progress.cursor.clear();
        progress.rekeyed = 0;
        let value = postcard::to_allocvec(&progress).map_err(|_| abe(AbeError::Context))?;
        self.step = Step::ResetRekey;
        let row = (ABE_REKEY_KEYSPACE.to_string(), key, value.into());
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    /// Registers the copy like any write, so governed reads find this node's copy of it.
    fn register(&mut self) -> Turn {
        let subject_generation = self
            .gated
            .as_ref()
            .and_then(|gated| gated.subject_generation);
        let registration = CopyRegistration {
            version: self.dest_version(),
            node_id: self.input.node_id,
            location: &self.input.location,
            policies: &self.refs,
            origin: CopyOrigin::Write,
            subject_generation: subject_generation.unwrap_or_default(),
            registered_at_ms: self.version_id.timestamp_ms(),
        };
        let effect = register_effect(registration, None)?;
        self.step = Step::Register;
        Ok(Next::Call(effect.into()))
    }

    /// The copy adds a logical object but no physical bytes: the archive is already credited.
    fn start_quota(&mut self) -> Turn {
        let added = self.input.size.saturating_add(self.envelope_bytes);
        let delta = UsageDelta {
            objects: i128::from(!self.was_live),
            logical_bytes: i128::from(added),
            referenced_bytes: -i128::from(self.released_bytes),
            ..Default::default()
        };
        let usage = UsageCounterUpdate::for_group(self.input.group_id, delta);
        let usage = usage.in_bucket(&self.input.bucket, RowChange::AddedVersion);
        self.usage = Some(usage.with_inventory(self.inventory_change()));
        match self.input.quota_ceiling.filter(|_| added > 0) {
            Some(ceiling) => {
                let mut gate = QuotaGate::new_for_realm(
                    ceiling,
                    added,
                    self.input.group_id,
                    self.input.node_id,
                    self.input.realm_id,
                );
                self.step = Step::Quota;
                let call = gate.start_calls();
                self.quota = Some(gate);
                Ok(Next::Call(call))
            }
            None => self.start_usage(),
        }
    }

    /// The copied version and the head it becomes in a distributed bucket.
    fn inventory_change(&self) -> Option<InventoryChange> {
        let (version, head) = (self.version.as_ref()?, self.head.as_ref()?);
        let added = AddedVersion {
            version_id: self.version_id,
            epoch: version.original_epoch,
            generation: head.generation,
            entry: entry_of(version, self.input.size),
        };
        version_written(self.binding.as_ref(), &self.input.dest_key, added, true)
    }

    fn quota_step(&mut self, result: IoResult) -> Turn {
        let gate = self.quota.as_mut().ok_or(SealedCopyError::InvalidState)?;
        if let Some(call) = gate.step_calls(result)? {
            return Ok(Next::Call(call));
        }
        if gate.is_rebuilding() {
            return Err(SealedCopyError::UsageRebuilding);
        }
        if gate.is_exceeded() {
            let (limit, usage) = (gate.ceiling(), gate.projected_usage());
            return Err(SealedCopyError::QuotaExceeded { limit, usage });
        }
        self.start_usage()
    }

    fn copied(&self) -> Turn {
        Ok(Next::Done(SealedCopyResult {
            version_id: self.version_id,
        }))
    }

    fn start_usage(&mut self) -> Turn {
        let update = self.usage.as_mut().ok_or(SealedCopyError::InvalidState)?;
        if update.is_noop() {
            return self.copied();
        }
        self.step = Step::Usage;
        Ok(Next::Call(update.start_calls().into_call()))
    }

    fn usage_step(&mut self, result: IoResult) -> Turn {
        let update = self.usage.as_mut().ok_or(SealedCopyError::InvalidState)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.copied(),
        }
    }
}

fn abe(error: AbeError) -> SealedCopyError {
    SealedCopyError::Blob(error.into())
}

impl Operation for SealedCopyOperation {
    type Output = SealedCopyResult;
    type Error = SealedCopyError;

    fn first(&mut self) -> Turn {
        self.step = Step::ReadBucket;
        let read = read_row(S3_BUCKET_KEYSPACE, self.input.bucket.as_bytes().to_vec());
        Ok(Next::Call(outside(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.step {
            Step::ReadBucket => self.bucket_read(outside_answer(result)?.value()?),
            Step::Gate => self.gate_checked(result),
            Step::Record => self.record_checked(outside_answer(result)?.event()?),
            Step::ReadAbe => self.abe_read(keyed(&outside_answer(result)?.values()?)),
            Step::ReadEnvelope(id) => self.envelope_read(id, outside_answer(result)?.value()?),
            Step::CreateEnvelope => self.envelope_created(result.event()?),
            Step::Fence => {
                let (bucket, key) = (&self.input.bucket, &self.input.dest_key);
                check_published(result.values()?, bucket, key, self.authority.as_ref())?;
                self.step = Step::Drift;
                Ok(Next::Call(drift_reads(&self.input.bucket, None).into()))
            }
            Step::Drift => self.drift_read(result.event()?),
            Step::ReadSource => self.source_read(result.values()?),
            Step::ReadLiveness => self.liveness_read(result.value()?),
            Step::WriteHead => {
                written(result)?;
                self.write_index()
            }
            Step::WriteIndex => {
                written(result)?;
                self.write_version()
            }
            // The version commits with its journal rows in one batch.
            Step::WriteVersion => {
                written(result)?;
                self.write_owner()
            }
            Step::WriteOwner => {
                written(result)?;
                self.publish_envelope()
            }
            Step::FenceAbe => self.abe_fenced(keyed(&result.values()?)),
            Step::WriteEnvelope => {
                written(result)?;
                self.read_rekey()
            }
            Step::ReadRekey => self.rekey_read(result.value()?),
            Step::ResetRekey => {
                written(result)?;
                self.register()
            }
            Step::Register => {
                written(result)?;
                self.start_quota()
            }
            Step::Quota => self.quota_step(result),
            Step::Usage => self.usage_step(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
