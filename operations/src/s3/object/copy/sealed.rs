//! Copies an encrypted object within its bucket by publishing a new version of the same
//! archive. No content is read, so the copy also works while the bucket is locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::abe::copies::still_active;
use crate::blob::managed_copy::{CopyRegistration, ManagedCopyError, register_effect};
use crate::blob::records::{
    HeadAliasContext, add_index_effect, owner_write_effect, write_head_effect, write_version_effect,
};
use crate::node::usage_stats::{QuotaGate, QuotaGateError, UsageCounterUpdate, UsageUpdateError};
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, drift_reads, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::s3::object::put::abe::{abe_reads, envelope_write, parse_abe};
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE, BLOB_HEAD_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
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
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
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
    ManagedCopy(#[from] ManagedCopyError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("Invalid operation state")]
    InvalidState,
    #[error("operation did not finish")]
    NotFinished,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedCopyResult {
    pub version_id: Ulid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    Init,
    ReadBucket,
    Gate,
    ReadAbe,
    ReadEnvelope,
    CreateEnvelope,
    Start,
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
    Register,
    Quota,
    Usage,
    Commit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct SealedCopyOperation {
    input: SealedCopyInput,
    step: Step,
    /// Destination of this node; absent fails every governed copy closed.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// What the gate decided on, read again inside the transaction.
    gated: Option<GatedBucket>,
    /// The bucket default joined with the source refs, stored on the copy.
    refs: Vec<PlacementPolicyRef>,
    txn_id: Option<TxnId>,
    version_id: Ulid,
    version: Option<BlobVersion>,
    existing: Option<CurrentVersionPointer>,
    was_live: bool,
    quota: Option<QuotaGate>,
    usage: Option<UsageCounterUpdate>,
    /// Admitted parameters and epoch, the complete envelope the copy reuses and its own one.
    admitted: Option<(AbeParameters, u64)>,
    source_envelope: Option<ObjectEnvelope>,
    envelope: Option<ObjectEnvelope>,
    /// Written instead of an envelope while the bucket is locked.
    pending: Option<PendingCopy>,
    envelope_bytes: u64,
    output: Option<Result<SealedCopyResult, SealedCopyError>>,
}

impl SealedCopyOperation {
    pub fn new(input: SealedCopyInput) -> Self {
        Self {
            input,
            step: Step::Init,
            gate_context: None,
            gate: None,
            gated: None,
            refs: Vec::new(),
            txn_id: None,
            version_id: Ulid::generate(),
            version: None,
            existing: None,
            was_live: false,
            quota: None,
            usage: None,
            admitted: None,
            source_envelope: None,
            envelope: None,
            pending: None,
            envelope_bytes: 0,
            output: None,
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
    fn bucket_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        let bucket = value
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let observed = GatedBucket::observe(bucket.as_ref());
        self.refs = union_refs(&observed.policies, &self.input.source_policies)?;
        let governed = !self.refs.is_empty();
        self.gated = Some(observed.stored_under(self.gate_context.as_ref(), governed));
        let group_id = Some(self.input.group_id);
        match write_gate(self.gate_context.as_ref(), &self.refs, group_id)? {
            None => self.read_abe(),
            Some(mut gate) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.step = Step::Gate;
                match complete {
                    true => self.finish_gate(),
                    false => Ok(effects),
                }
            }
        }
    }

    fn gate_step(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let gate = self.gate.as_mut().ok_or(SealedCopyError::InvalidState)?;
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_gate(),
            false => Ok(effects),
        }
    }

    fn finish_gate(&mut self) -> Result<Effects, SealedCopyError> {
        let gate = self.gate.take().ok_or(SealedCopyError::InvalidState)?;
        let outcome = gate.finalize().map_err(PolicyGateError::from)?;
        gate_decision(outcome)?;
        self.read_abe()
    }

    fn source_version(&self) -> Result<Vec<u8>, ConversionError> {
        VersionKey::new(
            &self.input.bucket,
            &self.input.source_key,
            self.input.source_version_id,
        )
        .to_bytes()
    }

    /// Reads the source's envelope or pending copy row and the admitted parameters and epoch.
    fn read_abe(&mut self) -> Result<Effects, SealedCopyError> {
        let Some(key) = self.input.location.format.bucket_key() else {
            return Ok(self.begin());
        };
        let source = self.source_version()?;
        let mut reads = vec![
            (ABE_VERSION_KEYSPACE.to_string(), source.clone().into()),
            (ABE_COPY_KEYSPACE.to_string(), source.into()),
        ];
        reads.extend(abe_reads(key));
        self.step = Step::ReadAbe;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None
        })])
    }

    /// A source without any envelope leaves the copy without one, as before.
    fn abe_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
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
            self.step = Step::ReadEnvelope;
            return Ok(smallvec![Effect::Storage(StorageEffect::Read {
                key_space: ABE_ENVELOPE_KEYSPACE.to_string(),
                key: id.to_vec().into(),
                txn_id: None,
            })]);
        }
        // A pending source passes on the complete envelope it names.
        let pending = pending.as_deref().ok_or(SealedCopyError::InvalidState)?;
        self.create_envelope(PendingCopy::from_bytes(pending).map_err(abe)?.source)
    }

    fn envelope_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::ReadResult { key, value }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        let envelope = ObjectEnvelope::from_bytes(value.as_deref().ok_or(abe(AbeError::Pending))?)
            .map_err(abe)?;
        if envelope.context.write_id.to_bytes().as_slice() != key.as_ref()
            || envelope.context.object_key != self.input.source_key
        {
            return Err(abe(AbeError::Context));
        }
        self.create_envelope(envelope)
    }

    /// Asks for the copy's own envelope; a locked bucket leaves the copy pending on `source`.
    fn create_envelope(&mut self, source: ObjectEnvelope) -> Result<Effects, SealedCopyError> {
        let (parameters, epoch) = self.admitted.clone().ok_or(SealedCopyError::InvalidState)?;
        if source.context.parameters != parameters {
            return Err(abe(AbeError::Parameters));
        }
        self.step = Step::CreateEnvelope;
        self.source_envelope = Some(source.clone());
        Ok(smallvec![Effect::Blob(BlobEffect::Abe(Box::new(
            AbeEffect::Copy {
                source,
                epoch,
                write_id: Ulid::generate(),
                object_key: self.input.dest_key.clone(),
            }
        )))])
    }

    fn envelope_created(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
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

    fn begin(&mut self) -> Effects {
        self.step = Step::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// The default and subject the gate admitted must still hold when the copy commits.
    fn drift_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
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

    fn fail(&mut self, error: impl Into<SealedCopyError>) -> Effects {
        self.step = Step::Error;
        self.output = Some(Err(error.into()));
        self.abort()
    }

    fn txn(&self) -> Result<TxnId, SealedCopyError> {
        self.txn_id.ok_or(SealedCopyError::InvalidState)
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
    fn read_source(&mut self) -> Result<Effects, SealedCopyError> {
        let source = VersionKey::new(
            &self.input.bucket,
            &self.input.source_key,
            self.input.source_version_id,
        );
        let head = BlobHeadKey::new(&self.input.bucket, &self.input.dest_key);
        self.step = Step::ReadSource;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    source.to_bytes()?.into()
                ),
                (BLOB_HEAD_KEYSPACE.to_string(), head.to_bytes()?.into()),
            ],
            txn_id: self.txn_id,
        })])
    }

    /// The new version keeps the source's content state, known or pending, on the same archive.
    fn source_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        let [(_, source), (_, head)] =
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
        let metadata = self.input.metadata.clone().unwrap_or(source.metadata);
        let mut version = BlobVersion::deleted(SystemTime::now(), self.input.user_id);
        version.state = state;
        self.version = Some(
            version
                .with_metadata(metadata)
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
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.to_bytes()?.into(),
            txn_id: self.txn_id,
        })])
    }

    fn liveness_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        self.was_live = value
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()?
            .is_some_and(|version| !version.is_deleted());
        self.write_head()
    }

    fn write_head(&mut self) -> Result<Effects, SealedCopyError> {
        let pointer = CurrentVersionPointer::next_for(self.existing.as_ref(), self.version_id)?;
        self.step = Step::WriteHead;
        Ok(smallvec![write_head_effect(
            &self.alias_context(),
            pointer,
            self.txn_id
        )?])
    }

    /// A known content hash gets its path index; a pending archive has none yet.
    fn write_index(&mut self) -> Result<Effects, SealedCopyError> {
        let version = self.version.as_ref().ok_or(SealedCopyError::InvalidState)?;
        let Some(hash) = version.state.blob_hash().copied() else {
            return self.write_version();
        };
        self.step = Step::WriteIndex;
        let context = self.alias_context();
        Ok(smallvec![add_index_effect(
            &context,
            hash,
            self.version_id,
            self.txn_id
        )?])
    }

    fn write_version(&mut self) -> Result<Effects, SealedCopyError> {
        let version = self.version.as_ref().ok_or(SealedCopyError::InvalidState)?;
        let effect = write_version_effect(&self.dest_version(), version, self.txn_id)?;
        self.step = Step::WriteVersion;
        Ok(smallvec![effect])
    }

    /// The new version owns the archive too, so neither alias frees it while the other lives.
    fn write_owner(&mut self) -> Result<Effects, SealedCopyError> {
        let owner = CopyOwner::new(self.archive(), self.dest_version());
        self.step = Step::WriteOwner;
        Ok(smallvec![owner_write_effect(&owner, self.txn_id)?])
    }

    /// Fences the copy's own envelope or writes its pending row; a source without either skips.
    fn publish_envelope(&mut self) -> Result<Effects, SealedCopyError> {
        if let Some(pending) = &self.pending {
            let key = self.dest_version().to_bytes()?;
            let value = pending.to_bytes().map_err(abe)?;
            self.step = Step::WriteEnvelope;
            return Ok(smallvec![Effect::Storage(StorageEffect::Write {
                key_space: ABE_COPY_KEYSPACE.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: self.txn_id,
            })]);
        }
        let Some(envelope) = &self.envelope else {
            return self.register();
        };
        let bucket = self.input.bucket.as_bytes().to_vec();
        let mut reads = vec![(BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into())];
        reads.extend(abe_reads(envelope.context.parameters.key));
        self.step = Step::FenceAbe;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })])
    }

    /// A raise or rotation since the envelope was made refuses the copy, like a stale write.
    fn abe_fenced(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
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
        let rows = (&version.metadata, limit, self.txn_id);
        let (effect, charge) =
            envelope_write(envelope, &self.dest_version(), &self.input.location, rows)
                .map_err(abe)?;
        self.envelope_bytes = charge;
        self.step = Step::WriteEnvelope;
        Ok(smallvec![effect])
    }

    /// Registers the copy like any write, so governed reads find this node's copy of it.
    fn register(&mut self) -> Result<Effects, SealedCopyError> {
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
        let effect = register_effect(registration, self.txn_id)?;
        self.step = Step::Register;
        Ok(smallvec![effect])
    }

    /// The copy adds a logical object but no physical bytes: the archive is already credited.
    fn start_quota(&mut self) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let delta = UsageDelta {
            objects: i128::from(!self.was_live),
            logical_bytes: i128::from(self.input.size) + i128::from(self.envelope_bytes),
            ..Default::default()
        };
        self.usage = Some(UsageCounterUpdate::for_group(self.input.group_id, delta));
        match self.input.quota_ceiling.filter(|_| self.input.size > 0) {
            Some(ceiling) => {
                let mut gate = QuotaGate::new_for_realm(
                    ceiling,
                    self.input.size,
                    self.input.group_id,
                    self.input.node_id,
                    self.input.realm_id,
                );
                self.step = Step::Quota;
                let effects = gate.start(txn_id);
                self.quota = Some(gate);
                Ok(effects)
            }
            None => self.start_usage(),
        }
    }

    fn quota_step(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let gate = self.quota.as_mut().ok_or(SealedCopyError::InvalidState)?;
        if let Some(effects) = gate.step(event, txn_id)? {
            return Ok(effects);
        }
        if gate.is_exceeded() {
            let (limit, usage) = (gate.ceiling(), gate.projected_usage());
            return Err(SealedCopyError::QuotaExceeded { limit, usage });
        }
        self.start_usage()
    }

    fn start_usage(&mut self) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let update = self.usage.as_mut().ok_or(SealedCopyError::InvalidState)?;
        if update.is_noop() {
            return self.commit();
        }
        self.step = Step::Usage;
        Ok(update.start(txn_id))
    }

    fn usage_step(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let update = self.usage.as_mut().ok_or(SealedCopyError::InvalidState)?;
        match update.step(event, txn_id)? {
            Some(effects) => Ok(effects),
            None => self.commit(),
        }
    }

    fn commit(&mut self) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        self.step = Step::Commit;
        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn advance(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let written = matches!(event, Event::Storage(StorageEvent::WriteResult { .. }));
        match (self.step, event) {
            (Step::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.step = Step::Fence;
                Ok(smallvec![write_fence_read(&self.input.bucket, self.txn_id)])
            }
            (Step::ReadBucket, event) => self.bucket_read(event),
            (Step::Gate, event) => self.gate_step(event),
            (Step::ReadAbe, event) => self.abe_read(event),
            (Step::ReadEnvelope, event) => self.envelope_read(event),
            (Step::CreateEnvelope, event) => self.envelope_created(event),
            (Step::Fence, event) => {
                check_write_fence(event, &self.input.bucket, &self.input.dest_key)?;
                self.step = Step::Drift;
                Ok(smallvec![drift_reads(&self.input.bucket, self.txn_id)])
            }
            (Step::Drift, event) => self.drift_read(event),
            (Step::ReadSource, event) => self.source_read(event),
            (Step::ReadLiveness, event) => self.liveness_read(event),
            (Step::WriteHead, _) if written => self.write_index(),
            (Step::WriteIndex, _) if written => self.write_version(),
            (Step::WriteVersion, _) if written => self.write_owner(),
            (Step::WriteOwner, _) if written => self.publish_envelope(),
            (Step::FenceAbe, event) => self.abe_fenced(event),
            (Step::WriteEnvelope, Event::Storage(StorageEvent::WriteResult { .. }))
            | (Step::WriteEnvelope, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.register()
            }
            (Step::Register, _) if written => self.start_quota(),
            (Step::Quota, event) => self.quota_step(event),
            (Step::Usage, event) => self.usage_step(event),
            (Step::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.step = Step::Finish;
                self.output = Some(Ok(SealedCopyResult {
                    version_id: self.version_id,
                }));
                Ok(smallvec![])
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            _ => Err(SealedCopyError::InvalidState),
        }
    }
}

fn abe(error: AbeError) -> SealedCopyError {
    SealedCopyError::Blob(error.into())
}

impl Operation for SealedCopyOperation {
    type Output = SealedCopyResult;
    type Error = SealedCopyError;

    fn start(&mut self) -> Effects {
        self.step = Step::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.advance(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, Step::Finish | Step::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SealedCopyError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "sealed_tests.rs"]
mod tests;
